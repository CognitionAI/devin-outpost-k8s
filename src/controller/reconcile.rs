//! The `OutpostPool` reconcile loop.

use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::core::v1::{Pod, Secret};
use kube::ResourceExt;
use kube::api::{Api, DeleteParams, ListParams as KubeListParams, Patch, PatchParams, PostParams};
use kube::runtime::Controller;
use kube::runtime::controller::Action as RequeueAction;
use kube::runtime::finalizer::{Event as FinalizerEvent, finalizer};
use kube::runtime::watcher;
use tracing::{debug, info, warn};

use crate::api::{ListParams, OutpostsClient};
use crate::config::OperatorConfig;
use crate::crd::{Condition, OutpostPool, OutpostPoolStatus};
use crate::error::Error;
use crate::metrics::{Metrics, PoolLabels};
use crate::snapshot::{RestoreVerdict, SnapshotOutcome, SnapshotProvider, provider_for};

use super::context::Context;
use super::plan::{Action, Observed, next_claim_deadline, plan};
use super::pod::{
    LABEL_MANAGED_BY, LABEL_POOL, WorkerPodParams, build_session_token_secret, build_worker_pod,
    session_token_secret_name, worker_pod_name,
};
use super::queue_watch::PoolWatchers;

/// Requeue delay while a snapshot is in progress or right after failures.
const REQUEUE_SOON: Duration = Duration::from_secs(10);

/// Run the controller until the process is asked to shut down.
///
/// Wires a `kube::runtime::Controller` over [`OutpostPool`] resources and
/// their owned worker `Pod`s, edge-triggered by the per-pool queue watchers
/// (see [`super::queue_watch`]), and drives [`reconcile`].
pub async fn run(
    client: kube::Client,
    config: Arc<OperatorConfig>,
    metrics: Arc<Metrics>,
) -> crate::Result<()> {
    let (trigger_tx, trigger_rx) = futures::channel::mpsc::unbounded::<()>();
    let watchers = Arc::new(PoolWatchers::new(trigger_tx));
    let ctx = Arc::new(Context::new(client.clone(), config, metrics, watchers));

    let pools: Api<OutpostPool> = match &ctx.config.watch_namespace {
        Some(ns) => Api::namespaced(client.clone(), ns),
        None => Api::all(client.clone()),
    };
    let pods: Api<Pod> = match &ctx.config.watch_namespace {
        Some(ns) => Api::namespaced(client.clone(), ns),
        None => Api::all(client.clone()),
    };

    info!("starting OutpostPool controller");

    Controller::new(pools, watcher::Config::default())
        .owns(
            pods,
            watcher::Config::default()
                .labels(&format!("{LABEL_MANAGED_BY}={}", crate::MANAGER_NAME)),
        )
        .reconcile_all_on(trigger_rx)
        .run(reconcile, error_policy, ctx)
        .for_each(|res| async move {
            match res {
                Ok((obj, _action)) => debug!(?obj, "reconciled"),
                Err(err) => warn!(%err, "reconcile error"),
            }
        })
        .await;

    Ok(())
}

/// Reconcile a single [`OutpostPool`], routing through the
/// [`super::POOL_FINALIZER`] so pool deletion releases in-flight claims
/// first.
pub async fn reconcile(pool: Arc<OutpostPool>, ctx: Arc<Context>) -> crate::Result<RequeueAction> {
    let ns = pool.namespace().unwrap_or_else(|| "default".to_string());
    let pools: Api<OutpostPool> = Api::namespaced(ctx.client.clone(), &ns);
    finalizer(&pools, super::POOL_FINALIZER, pool, |event| async {
        match event {
            FinalizerEvent::Apply(pool) => apply(pool, &ctx).await,
            FinalizerEvent::Cleanup(pool) => cleanup(pool, &ctx).await,
        }
    })
    .await
    .map_err(|e| Error::Other(anyhow::Error::new(e)))
}

/// Decide how to back off when [`reconcile`] returns an error.
pub fn error_policy(pool: Arc<OutpostPool>, err: &Error, ctx: Arc<Context>) -> RequeueAction {
    ctx.metrics
        .reconcile_failures
        .get_or_create(&pool_labels(&pool))
        .inc();
    warn!(pool = %pool.spec.pool_id, %err, "reconcile failed; requeueing");
    RequeueAction::requeue(Duration::from_secs(30))
}

fn pool_labels(pool: &OutpostPool) -> PoolLabels {
    PoolLabels {
        pool_id: pool.spec.pool_id.clone(),
    }
}

/// Read the pool's PAT from its referenced `Secret`.
async fn pool_token(pool: &OutpostPool, ctx: &Context, ns: &str) -> crate::Result<String> {
    let secret_ref = &pool.spec.token_secret_ref;
    let secrets: Api<Secret> = Api::namespaced(ctx.client.clone(), ns);
    let secret = secrets.get(&secret_ref.name).await?;
    let value = secret
        .data
        .as_ref()
        .and_then(|d| d.get(&secret_ref.key))
        .ok_or_else(|| Error::MissingSecretKey {
            name: secret_ref.name.clone(),
            key: secret_ref.key.clone(),
        })?;
    String::from_utf8(value.0.clone()).map_err(|_| Error::MissingSecretKey {
        name: secret_ref.name.clone(),
        key: secret_ref.key.clone(),
    })
}

/// State shared by the action executors for one reconcile pass.
struct PoolSync<'a> {
    pool: &'a Arc<OutpostPool>,
    ctx: &'a Context,
    outposts: &'a OutpostsClient,
    provider: &'a dyn SnapshotProvider,
    pods: Api<Pod>,
    secrets: Api<Secret>,
    pods_by_session: BTreeMap<String, Pod>,
    /// Set when a snapshot reported [`SnapshotOutcome::InProgress`].
    snapshot_pending: bool,
}

async fn apply(pool: Arc<OutpostPool>, ctx: &Context) -> crate::Result<RequeueAction> {
    let labels = pool_labels(&pool);
    ctx.metrics.reconciliations.get_or_create(&labels).inc();

    let ns = pool.namespace().unwrap_or_else(|| "default".to_string());
    let api_url = pool
        .spec
        .api_url
        .clone()
        .unwrap_or_else(|| ctx.config.default_api_url.clone());

    let token = match pool_token(&pool, ctx, &ns).await {
        Ok(token) => token,
        Err(err) => {
            update_status(&pool, ctx, degraded_status(&pool, ctx, &err), &ns).await?;
            return Err(err);
        }
    };
    let acceptor_id = ctx.acceptor_id().await?.to_string();
    let outposts = OutpostsClient::new(&api_url, &token, &acceptor_id)?;

    // Keep the edge-trigger watcher running (replaced if the token/URL
    // changed) and seed a fresh one from the persisted cursor.
    let fingerprint = {
        let mut h = std::hash::DefaultHasher::new();
        (&pool.spec.pool_id, &api_url, &token).hash(&mut h);
        h.finish()
    };
    ctx.watchers.ensure(
        &pool.uid().unwrap_or_default(),
        fingerprint,
        outposts.clone(),
        pool.spec.pool_id.clone(),
        pool.status.as_ref().and_then(|s| s.watch_cursor.clone()),
    );

    let sessions = match outposts
        .list_all(&pool.spec.pool_id, &ListParams::default())
        .await
    {
        Ok(sessions) => sessions,
        Err(err) => {
            update_status(&pool, ctx, degraded_status(&pool, ctx, &err), &ns).await?;
            return Err(err);
        }
    };

    // Owned worker pods, matched to sessions by their deterministic names
    // (labels are overridable by the pool, names are not).
    let pods: Api<Pod> = Api::namespaced(ctx.client.clone(), &ns);
    let pool_uid = pool.uid().unwrap_or_default();
    let owned_pods: Vec<Pod> = pods
        .list(&KubeListParams::default())
        .await?
        .items
        .into_iter()
        .filter(|p| p.owner_references().iter().any(|r| r.uid == pool_uid))
        .collect();
    let expected_pod_names: BTreeMap<String, String> = sessions
        .iter()
        .map(|s| {
            (
                worker_pod_name(&s.metadata.session_id),
                s.metadata.session_id.clone(),
            )
        })
        .collect();
    let mut pods_by_session = BTreeMap::new();
    let mut orphan_pods = Vec::new();
    for pod in owned_pods {
        match expected_pod_names.get(&pod.name_any()) {
            Some(session_id) => {
                pods_by_session.insert(session_id.clone(), pod);
            }
            None => orphan_pods.push(pod.name_any()),
        }
    }

    let provider = provider_for(ctx.client.clone(), pool.clone());
    if let Err(err) = provider.gc().await {
        warn!(pool = %pool.spec.pool_id, %err, "snapshot GC failed");
    }

    let now = chrono::Utc::now().timestamp();
    let actions = plan(&Observed {
        sessions: &sessions,
        pods_by_session: &pods_by_session,
        orphan_pods: &orphan_pods,
        acceptor_id: &acceptor_id,
        max_concurrent: pool.spec.max_concurrent_sessions,
        restart_limit: ctx.config.worker_restart_limit,
        renew_margin_secs: ctx.config.claim_renew_margin.as_secs() as i64,
        now,
    });

    let mut sync = PoolSync {
        pool: &pool,
        ctx,
        outposts: &outposts,
        provider: provider.as_ref(),
        pods,
        secrets: Api::namespaced(ctx.client.clone(), &ns),
        pods_by_session,
        snapshot_pending: false,
    };

    let mut claimed_now: u32 = 0;
    let mut first_failure: Option<Error> = None;
    for action in actions {
        debug!(pool = %pool.spec.pool_id, ?action, "executing");
        let result = execute(&mut sync, &action).await;
        match result {
            Ok(()) => {
                if matches!(action, Action::Claim { .. }) {
                    claimed_now += 1;
                    sync.ctx
                        .metrics
                        .sessions_claimed
                        .get_or_create(&labels)
                        .inc();
                }
            }
            Err(Error::ClaimConflict { session_id }) => {
                debug!(pool = %pool.spec.pool_id, session = %session_id, "lost claim race");
                sync.ctx
                    .metrics
                    .claim_conflicts
                    .get_or_create(&labels)
                    .inc();
            }
            Err(err) => {
                warn!(pool = %pool.spec.pool_id, ?action, %err, "action failed");
                first_failure.get_or_insert(err);
            }
        }
    }
    // Recycle worker pods that should have been restored from a snapshot but
    // cold-started (see `GkeSnapshotProvider::verify_restore` for the race
    // this covers). One retry per snapshot, tracked on the token secret so
    // the marker survives the pod swap; a second cold start is accepted.
    for (session_id, worker_pod) in sync.pods_by_session.clone() {
        let running = worker_pod
            .status
            .as_ref()
            .and_then(|s| s.phase.as_deref())
            .is_some_and(|phase| phase == "Running")
            && worker_pod.metadata.deletion_timestamp.is_none();
        if !running || sync.provider.verify_restore(&worker_pod) != RestoreVerdict::ColdStarted {
            continue;
        }
        match recycle_cold_started_pod(&mut sync, &session_id).await {
            Ok(true) => {
                warn!(session = %session_id, "worker cold-started instead of restoring; recycling pod");
            }
            Ok(false) => {}
            Err(err) => {
                warn!(session = %session_id, %err, "failed to recycle cold-started worker");
                first_failure.get_or_insert(err);
            }
        }
    }
    let snapshot_pending = sync.snapshot_pending;

    // Cleanup: token secrets whose session no longer exists in the queue.
    let secrets: Api<Secret> = Api::namespaced(ctx.client.clone(), &ns);
    let selector = format!("{LABEL_POOL}={}", pool.name_any());
    let expected_secrets: Vec<String> = sessions
        .iter()
        .map(|s| session_token_secret_name(&s.metadata.session_id))
        .collect();
    for secret in secrets
        .list(&KubeListParams::default().labels(&selector))
        .await?
    {
        let name = secret.name_any();
        if !expected_secrets.contains(&name) {
            delete_ignoring_missing(&secrets, &name).await?;
            // A session can leave the queue (tombstoned) without ever being
            // observed as terminated — e.g. suspended sessions the sweeper
            // reaps — so the terminate-time snapshot cleanup runs here too.
            if let Some(session_id) = secret.labels().get(super::LABEL_SESSION_ID)
                && let Err(err) = provider.on_terminate(session_id).await
            {
                warn!(session = %session_id, %err, "snapshot cleanup for vanished session failed");
            }
        }
    }

    let ours_active = sessions
        .iter()
        .filter(|s| {
            s.status.phase == crate::api::Phase::Claimed
                && s.status.acceptor_id.as_deref() == Some(acceptor_id.as_str())
                && s.status.session_status != crate::api::SessionStatus::Terminated
        })
        .count() as u32
        + claimed_now;
    ctx.metrics
        .active_workers
        .get_or_create(&labels)
        .set(ours_active as i64);

    let (phase, condition) = match &first_failure {
        None => ("Ready", ready_condition(true, None)),
        Some(err) if err.is_unauthorized() => ("Unauthorized", ready_condition(false, Some(err))),
        Some(err) => ("Degraded", ready_condition(false, Some(err))),
    };
    update_status(
        &pool,
        ctx,
        OutpostPoolStatus {
            phase: Some(phase.to_string()),
            claimed_sessions: ours_active,
            last_synced: Some(chrono::Utc::now().to_rfc3339()),
            watch_cursor: ctx.watchers.cursor(&pool_uid),
            conditions: vec![condition],
        },
        &ns,
    )
    .await?;

    if let Some(err) = first_failure {
        return Err(err);
    }

    // Requeue in time to renew the earliest claim, immediately-ish while a
    // snapshot is pending, and at the reconcile interval otherwise.
    let mut requeue = ctx.config.reconcile_interval;
    if snapshot_pending {
        requeue = requeue.min(REQUEUE_SOON);
    }
    if let Some(deadline) = next_claim_deadline(&sessions, &acceptor_id) {
        let until_renew = deadline - ctx.config.claim_renew_margin.as_secs() as i64 - now;
        requeue = requeue.min(Duration::from_secs(until_renew.clamp(5, i64::MAX) as u64));
    }
    Ok(RequeueAction::requeue(requeue))
}

async fn execute(sync: &mut PoolSync<'_>, action: &Action) -> crate::Result<()> {
    match action {
        Action::Claim { session_id } | Action::StartWorker { session_id } => {
            start_worker(sync, session_id).await
        }
        Action::Renew { session_id } => {
            // The renewal returns a fresh connect token, but the secret keeps
            // the claim-time one: tokens outlive the maximum session
            // lifetime, so rewriting it would only churn the pod's env.
            sync.outposts.claim(session_id).await.map(drop)
        }
        Action::Suspend { session_id } => {
            match sync.pods_by_session.get(session_id) {
                Some(pod) => match sync.provider.on_suspend(session_id, pod).await? {
                    SnapshotOutcome::InProgress => {
                        sync.snapshot_pending = true;
                        return Ok(());
                    }
                    SnapshotOutcome::Ready => {
                        teardown_session(sync, session_id).await?;
                    }
                },
                None => teardown_session(sync, session_id).await?,
            }
            release_ignoring_conflict(sync.outposts, session_id).await
        }
        Action::Terminate { session_id } => {
            release_ignoring_conflict(sync.outposts, session_id).await?;
            teardown_session(sync, session_id).await?;
            sync.provider.on_terminate(session_id).await
        }
        Action::GiveUp {
            session_id,
            restarts,
        } => {
            warn!(
                session = %session_id,
                restarts,
                limit = sync.ctx.config.worker_restart_limit,
                "worker exceeded restart limit; giving the session back"
            );
            sync.ctx
                .metrics
                .workers_given_up
                .get_or_create(&pool_labels(sync.pool))
                .inc();
            release_ignoring_conflict(sync.outposts, session_id).await?;
            teardown_session(sync, session_id).await
        }
        Action::ReplaceSucceededPod { session_id } => {
            delete_ignoring_missing(&sync.pods, &worker_pod_name(session_id)).await
        }
        Action::DeleteOrphanPod { pod_name } => delete_ignoring_missing(&sync.pods, pod_name).await,
    }
}

/// Claim (or renew) the session for a fresh connect token, then create its
/// token secret and worker pod.
async fn start_worker(sync: &mut PoolSync<'_>, session_id: &str) -> crate::Result<()> {
    let claimed = sync.outposts.claim(session_id).await?;
    let connect_token = claimed.status.connect_token.as_deref().ok_or_else(|| {
        Error::Config(
            "claim response carried no connect_token (gateway not configured upstream)".into(),
        )
    })?;
    let gateway_url = claimed.status.gateway_url.as_deref().ok_or_else(|| {
        Error::Config(
            "claim response carried no gateway_url (gateway not configured upstream)".into(),
        )
    })?;

    let prepared = sync.provider.prepare(session_id).await?;

    let secret = build_session_token_secret(sync.pool, &claimed, connect_token)?;
    sync.secrets
        .patch(
            &secret.name_any(),
            &PatchParams::apply(crate::MANAGER_NAME).force(),
            &Patch::Apply(&secret),
        )
        .await?;

    let pod = build_worker_pod(WorkerPodParams {
        pool: sync.pool,
        session: &claimed,
        gateway_url,
        token_secret_name: &secret.name_any(),
        default_image: &sync.ctx.config.default_worker_image,
        state_pvc_name: prepared.state_pvc_name.as_deref(),
        provider_annotations: &prepared.pod_annotations,
    })?;
    match sync.pods.create(&PostParams::default(), &pod).await {
        Ok(created) => {
            info!(session = %session_id, pod = %created.name_any(), "started worker");
            sync.pods_by_session.insert(session_id.to_string(), created);
            Ok(())
        }
        Err(kube::Error::Api(e)) if e.code == 409 => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// Annotation on the token secret marking that the session's worker was
/// already recycled once after a cold start (see [`recycle_cold_started_pod`]).
const ANNOTATION_RESTORE_RETRIED: &str = "outposts.cognition.com/restore-retried";

/// Delete a worker pod that cold-started instead of restoring, so the next
/// pass recreates it — by which point the node's snapshot agent is up and the
/// restore succeeds. At most once per session (the marker outlives the pod on
/// the token secret); returns whether the pod was recycled.
async fn recycle_cold_started_pod(
    sync: &mut PoolSync<'_>,
    session_id: &str,
) -> crate::Result<bool> {
    let secret_name = session_token_secret_name(session_id);
    let Some(secret) = sync.secrets.get_opt(&secret_name).await? else {
        return Ok(false);
    };
    if secret
        .annotations()
        .contains_key(ANNOTATION_RESTORE_RETRIED)
    {
        return Ok(false);
    }
    let patch = serde_json::json!({
        "metadata": {"annotations": {ANNOTATION_RESTORE_RETRIED: chrono::Utc::now().to_rfc3339()}}
    });
    sync.secrets
        .patch(&secret_name, &PatchParams::default(), &Patch::Merge(&patch))
        .await?;
    delete_ignoring_missing(&sync.pods, &worker_pod_name(session_id)).await?;
    sync.pods_by_session.remove(session_id);
    sync.snapshot_pending = true;
    Ok(true)
}

/// Delete the session's worker pod and token secret.
async fn teardown_session(sync: &mut PoolSync<'_>, session_id: &str) -> crate::Result<()> {
    delete_ignoring_missing(&sync.pods, &worker_pod_name(session_id)).await?;
    delete_ignoring_missing(&sync.secrets, &session_token_secret_name(session_id)).await?;
    sync.pods_by_session.remove(session_id);
    Ok(())
}

/// Release a claim, treating "not claimed by us" and "gone" as released: the
/// queue's lazy expiry and tombstoning race the operator, and both outcomes
/// mean the session is no longer ours.
async fn release_ignoring_conflict(
    outposts: &OutpostsClient,
    session_id: &str,
) -> crate::Result<()> {
    match outposts.release(session_id).await {
        Ok(()) => Ok(()),
        Err(Error::ClaimConflict { .. }) => Ok(()),
        Err(e) if e.is_not_found() => Ok(()),
        Err(e) => Err(e),
    }
}

async fn delete_ignoring_missing<K>(api: &Api<K>, name: &str) -> crate::Result<()>
where
    K: kube::Resource + Clone + serde::de::DeserializeOwned + std::fmt::Debug,
{
    match api.delete(name, &DeleteParams::default()).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(e)) if e.code == 404 => Ok(()),
        Err(e) => Err(e.into()),
    }
}

fn ready_condition(ready: bool, err: Option<&Error>) -> Condition {
    Condition {
        type_: "Ready".to_string(),
        status: if ready { "True" } else { "False" }.to_string(),
        reason: Some(if ready { "Synced" } else { "SyncFailed" }.to_string()),
        message: err.map(|e| e.to_string()),
        last_transition_time: Some(chrono::Utc::now().to_rfc3339()),
    }
}

fn degraded_status(pool: &OutpostPool, ctx: &Context, err: &Error) -> OutpostPoolStatus {
    let phase = if err.is_unauthorized() {
        "Unauthorized"
    } else {
        "Degraded"
    };
    OutpostPoolStatus {
        phase: Some(phase.to_string()),
        claimed_sessions: pool
            .status
            .as_ref()
            .map(|s| s.claimed_sessions)
            .unwrap_or_default(),
        last_synced: pool.status.as_ref().and_then(|s| s.last_synced.clone()),
        watch_cursor: ctx
            .watchers
            .cursor(&pool.uid().unwrap_or_default())
            .or_else(|| pool.status.as_ref().and_then(|s| s.watch_cursor.clone())),
        conditions: vec![ready_condition(false, Some(err))],
    }
}

async fn update_status(
    pool: &OutpostPool,
    ctx: &Context,
    status: OutpostPoolStatus,
    ns: &str,
) -> crate::Result<()> {
    let pools: Api<OutpostPool> = Api::namespaced(ctx.client.clone(), ns);
    pools
        .patch_status(
            &pool.name_any(),
            &PatchParams::default(),
            &Patch::Merge(serde_json::json!({ "status": status })),
        )
        .await?;
    Ok(())
}

/// Pool deletion: stop the queue watcher and release every claim we hold, so
/// sessions return to the queue immediately instead of waiting out the claim
/// TTL. Owned Kubernetes objects (pods, secrets, PVCs, snapshot CRs) are
/// garbage-collected through their owner references.
async fn cleanup(pool: Arc<OutpostPool>, ctx: &Context) -> crate::Result<RequeueAction> {
    let ns = pool.namespace().unwrap_or_else(|| "default".to_string());
    ctx.watchers.remove(&pool.uid().unwrap_or_default());

    match pool_token(&pool, ctx, &ns).await {
        Ok(token) => {
            let api_url = pool
                .spec
                .api_url
                .clone()
                .unwrap_or_else(|| ctx.config.default_api_url.clone());
            let acceptor_id = ctx.acceptor_id().await?.to_string();
            let outposts = OutpostsClient::new(&api_url, &token, &acceptor_id)?;
            let ours = outposts
                .list_all(
                    &pool.spec.pool_id,
                    &ListParams {
                        acceptor_id: Some(&acceptor_id),
                        ..Default::default()
                    },
                )
                .await;
            match ours {
                Ok(sessions) => {
                    for session in sessions {
                        if let Err(err) =
                            release_ignoring_conflict(&outposts, &session.metadata.session_id).await
                        {
                            warn!(
                                session = %session.metadata.session_id,
                                %err,
                                "failed to release claim during pool deletion"
                            );
                        }
                    }
                }
                Err(err) => {
                    warn!(pool = %pool.spec.pool_id, %err, "could not list claims during pool deletion");
                }
            }
        }
        Err(err) => {
            // The token secret may already be gone; deletion must not wedge.
            warn!(pool = %pool.spec.pool_id, %err, "no pool token during deletion; claims will expire on their own");
        }
    }

    info!(pool = %pool.spec.pool_id, "pool cleaned up");
    Ok(RequeueAction::await_change())
}
