//! The `OutpostPool` reconcile loop.

use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::core::v1::{Pod, Secret};
use kube::api::{Api, DeleteParams, ListParams as KubeListParams, Patch, PatchParams, PostParams};
use kube::runtime::Controller;
use kube::runtime::controller::Action as RequeueAction;
use kube::runtime::finalizer::{Event as FinalizerEvent, finalizer};
use kube::runtime::watcher;
use kube::{Resource, ResourceExt};
use tracing::{debug, info, warn};

use crate::api::{ListParams, OutpostsClient};
use crate::config::OperatorConfig;
use crate::crd::{Condition, OutpostPool, OutpostPoolStatus};
use crate::error::Error;
use crate::metrics::{Metrics, PoolLabels};
use crate::network_policy::{Enforcement, SessionPolicy, worker_infrastructure};
use crate::snapshot::{RestoreVerdict, SnapshotOutcome, SnapshotProvider, provider_for};

use super::context::Context;
use super::plan::{Action, Observed, next_claim_deadline, plan};
use super::pod::{
    LABEL_MANAGED_BY, LABEL_POOL, WorkerPodParams, build_session_token_secret, build_worker_pod,
    network_policy_name, pool_owner_ref, session_token_secret_name, validate_session_image,
    worker_pod_name, worker_static_base_url,
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
        startup_timeout_secs: ctx.config.worker_startup_timeout.as_secs() as i64,
        renew_margin_secs: ctx.config.claim_renew_margin.as_secs() as i64,
        container_name: &pool.spec.worker.container_name,
        desired_image: pool
            .spec
            .worker
            .overrides
            .image
            .as_deref()
            .unwrap_or(&ctx.config.default_worker_image),
        build_image: pool
            .spec
            .image_build
            .as_ref()
            .and_then(|config| config.base_image.as_deref()),
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
        match recycle_unrestored_pod(&mut sync, &session_id, false).await {
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

    // Cleanup: token secrets whose session left the queue; its state stays
    // for a resume (`on_vanish`).
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
            let result = match secret.labels().get(super::LABEL_SESSION_ID) {
                Some(session_id) => match secret.annotations().get(ANNOTATION_CLEANUP_INTENT) {
                    Some(raw) => match serde_json::from_str::<CleanupIntent>(raw) {
                        Ok(intent) => finish_cleanup(&mut sync, session_id, intent).await,
                        Err(err) => Err(err.into()),
                    },
                    None => {
                        retire_vanished_session(provider.as_ref(), &secrets, session_id, &name)
                            .await
                    }
                },
                None => delete_ignoring_missing(&secrets, &name).await,
            };
            if let Err(err) = result {
                warn!(secret = %name, %err, "cleanup for vanished session failed");
                first_failure.get_or_insert(err);
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
            mark_cleanup_intent(sync, session_id, &CleanupIntent::Terminate).await?;
            finish_cleanup(sync, session_id, CleanupIntent::Terminate).await
        }
        Action::GiveUp {
            session_id,
            restarts,
        } => {
            // A worker crash-looping on a snapshot pin (e.g. `OCI runtime
            // restore failed` from a CPU-feature mismatch) gets one unpinned
            // retry before the session is given back.
            if sync
                .pods_by_session
                .get(session_id)
                .is_some_and(|pod| sync.provider.verify_restore(pod) == RestoreVerdict::ColdStarted)
                && recycle_unrestored_pod(sync, session_id, true).await?
            {
                warn!(
                    session = %session_id,
                    restarts,
                    "worker failing under a snapshot pin; retrying without it"
                );
                return Ok(());
            }
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
        Action::StartupFailed {
            session_id,
            failure,
        } => report_startup_failure(sync, session_id, failure).await,
        Action::ReplaceSucceededPod { session_id } => {
            delete_ignoring_missing(&sync.pods, &worker_pod_name(session_id)).await
        }
        Action::ReplaceDriftedPod { session_id } => {
            info!(
                session = %session_id,
                "crash-looping worker's image is stale; recreating with the current spec"
            );
            delete_ignoring_missing(&sync.pods, &worker_pod_name(session_id)).await?;
            sync.pods_by_session.remove(session_id);
            Ok(())
        }
        Action::DeleteOrphanPod { pod_name } => delete_ignoring_missing(&sync.pods, pod_name).await,
    }
}

async fn report_startup_failure(
    sync: &mut PoolSync<'_>,
    session_id: &str,
    failure: &crate::api::StartupFailure,
) -> crate::Result<()> {
    warn!(
        session = %session_id,
        reason = failure.reason.as_str(),
        image = failure.image.as_deref().unwrap_or(""),
        message = %failure.message,
        "worker cannot start; reporting the failure and giving the session up"
    );
    let intent = CleanupIntent::StartupFailure(failure.clone());
    mark_cleanup_intent(sync, session_id, &intent).await?;
    finish_cleanup(sync, session_id, intent).await
}

/// Claim (or renew) the session for a fresh connect token, then create its
/// token secret, network policy (see [`crate::network_policy`]) and worker
/// pod.
async fn start_worker(sync: &mut PoolSync<'_>, session_id: &str) -> crate::Result<()> {
    let claimed = sync.outposts.claim(session_id).await?;
    match validate_session_image(
        sync.pool,
        &claimed,
        sync.ctx.config.default_image_build_repository.as_deref(),
    ) {
        Ok(()) => {}
        Err(Error::SessionImageRejected(failure)) => {
            return report_startup_failure(sync, session_id, &failure).await;
        }
        Err(error) => return Err(error),
    }
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

    let secret_name = session_token_secret_name(session_id);
    let existing = sync.secrets.get_opt(&secret_name).await?;
    if existing
        .as_ref()
        .is_some_and(|s| s.annotations().contains_key(ANNOTATION_CLEANUP_INTENT))
    {
        clear_cleanup_intent(&sync.secrets, &secret_name).await?;
    }
    let mut prepared = sync.provider.prepare(&claimed).await?;
    // See `recycle_unrestored_pod`: a crashed restore's replacement starts
    // without the provider's restore annotations so it deliberately
    // cold-starts instead of looping.
    if let Some(existing) = existing
        && existing.annotations().get(ANNOTATION_RESTORE_RETRIED)
            == Some(&RETRY_WITHOUT_PIN.to_string())
    {
        prepared.pod_annotations.clear();
    }

    let secret = build_session_token_secret(sync.pool, &claimed, connect_token)?;
    sync.secrets
        .patch(
            &secret.name_any(),
            &PatchParams::apply(crate::MANAGER_NAME).force(),
            &Patch::Apply(&secret),
        )
        .await?;

    let network_policy = &claimed.spec.network_policy;
    let enforced_through = match network_policy.enabled {
        true => sync.ctx.network_policy_provider().await?,
        false => None,
    };
    if network_policy.enabled
        && enforced_through.is_none()
        && sync.ctx.config.network_policy_enforcement != Enforcement::Off
    {
        warn!(
            session = %session_id,
            "session has a network policy but the cluster has no provider to enforce it; \
             starting its worker unrestricted"
        );
    }
    let policy_name = enforced_through.map(|_| network_policy_name(session_id));

    let pod = build_worker_pod(WorkerPodParams {
        pool: sync.pool,
        session: &claimed,
        gateway_url,
        token_secret_name: &secret.name_any(),
        default_image: &sync.ctx.config.default_worker_image,
        state_pvc_name: prepared.state_pvc_name.as_deref(),
        seed_home: prepared.seed_home,
        provider_annotations: &prepared.pod_annotations,
        network_policy_name: policy_name.as_deref(),
        operator_image: sync.ctx.config.operator_image.as_deref(),
        default_image_build_repository: sync.ctx.config.default_image_build_repository.as_deref(),
    })?;

    let allow = match enforced_through {
        Some(_) => {
            let static_base_url =
                worker_static_base_url(&pod, &sync.pool.spec.worker.container_name)?;
            [
                network_policy.allow.clone(),
                worker_infrastructure(gateway_url, static_base_url),
            ]
            .concat()
        }
        None => Vec::new(),
    };
    let session_policy = enforced_through.map(|provider| SessionPolicy {
        provider,
        pool: sync.pool,
        session_id,
        allow: &allow,
    });
    if let Some(policy) = &session_policy {
        policy
            .apply(&sync.ctx.client, &pool_owner_ref(sync.pool)?)
            .await?;
    }

    match sync.pods.create(&PostParams::default(), &pod).await {
        Ok(created) => {
            info!(session = %session_id, pod = %created.name_any(), "started worker");
            let pod_owner = created.owner_ref(&());
            sync.pods_by_session.insert(session_id.to_string(), created);
            if let Some(policy) = &session_policy {
                let pod_owner = pod_owner.ok_or_else(|| {
                    Error::Config("created worker pod has no metadata.name/uid".into())
                })?;
                policy.apply(&sync.ctx.client, &pod_owner).await?;
            }
            Ok(())
        }
        Err(kube::Error::Api(e)) if e.code == 409 => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// Annotation on the token secret marking that the session's worker was
/// already recycled once this claim cycle (see [`recycle_unrestored_pod`]).
/// The value records the retry flavor; [`RETRY_WITHOUT_PIN`] additionally
/// makes the replacement start without the provider's restore annotations.
const ANNOTATION_RESTORE_RETRIED: &str = "outposts.cognition.com/restore-retried";
/// Marker value for the restore-failure flavor of the retry.
const RETRY_WITHOUT_PIN: &str = "without-pin";
/// Marker value for the agent-race flavor of the retry (the replacement
/// keeps its pin and is expected to restore on the now-warm node).
const RETRY_WITH_PIN: &str = "with-pin";

/// Delete a worker pod whose expected restore did not happen, so the next
/// pass recreates it. Two flavors:
///
/// - `drop_pin = false` — the pod *ran* without restoring (the node's
///   snapshot agent lost the scale-up race): the replacement keeps its pin,
///   and by the time it starts the agent is up, so it restores.
/// - `drop_pin = true` — the pod *crashed* on its pin (e.g. a CPU-feature
///   mismatch): the replacement starts unpinned and deliberately cold.
///
/// At most one recycle per claim cycle (the marker outlives the pod on the
/// token secret, which is deleted on suspend/terminate); returns whether the
/// pod was recycled.
async fn recycle_unrestored_pod(
    sync: &mut PoolSync<'_>,
    session_id: &str,
    drop_pin: bool,
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
    let value = if drop_pin {
        RETRY_WITHOUT_PIN
    } else {
        RETRY_WITH_PIN
    };
    let patch = serde_json::json!({
        "metadata": {"annotations": {ANNOTATION_RESTORE_RETRIED: value}}
    });
    sync.secrets
        .patch(&secret_name, &PatchParams::default(), &Patch::Merge(&patch))
        .await?;
    delete_ignoring_missing(&sync.pods, &worker_pod_name(session_id)).await?;
    sync.pods_by_session.remove(session_id);
    sync.snapshot_pending = true;
    Ok(true)
}

/// Keep the token secret as the retry handle until snapshot cleanup succeeds.
async fn delete_session_artifacts(
    provider: &dyn SnapshotProvider,
    secrets: &Api<Secret>,
    session_id: &str,
    secret_name: &str,
) -> crate::Result<()> {
    provider.on_terminate(session_id).await?;
    delete_ignoring_missing(secrets, secret_name).await
}

/// The secret is the retry handle: it goes only once the state is kept.
async fn retire_vanished_session(
    provider: &dyn SnapshotProvider,
    secrets: &Api<Secret>,
    session_id: &str,
    secret_name: &str,
) -> crate::Result<()> {
    provider.on_vanish(session_id).await?;
    delete_ignoring_missing(secrets, secret_name).await
}

const ANNOTATION_CLEANUP_INTENT: &str = "outposts.cognition.com/cleanup-intent";

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", content = "failure", rename_all = "snake_case")]
enum CleanupIntent {
    Terminate,
    StartupFailure(crate::api::StartupFailure),
    Delete,
}

async fn mark_cleanup_intent(
    sync: &PoolSync<'_>,
    session_id: &str,
    intent: &CleanupIntent,
) -> crate::Result<()> {
    let secret = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": session_token_secret_name(session_id),
            "namespace": sync.pool.namespace().unwrap_or_else(|| "default".to_string()),
            "labels": {
                LABEL_POOL: sync.pool.name_any(),
                super::LABEL_SESSION_ID: session_id,
            },
            "ownerReferences": [pool_owner_ref(sync.pool)?],
            "annotations": {
                ANNOTATION_CLEANUP_INTENT: serde_json::to_string(intent)?,
            },
        },
    });
    sync.secrets
        .patch(
            &session_token_secret_name(session_id),
            &PatchParams::apply("devin-outposts-k8s-cleanup"),
            &Patch::Apply(&secret),
        )
        .await?;
    Ok(())
}

async fn clear_cleanup_intent(secrets: &Api<Secret>, secret_name: &str) -> crate::Result<()> {
    let patch = serde_json::json!({
        "metadata": {"annotations": {ANNOTATION_CLEANUP_INTENT: null}}
    });
    match secrets
        .patch(secret_name, &PatchParams::default(), &Patch::Merge(&patch))
        .await
    {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(e)) if e.code == 404 => Ok(()),
        Err(e) => Err(e.into()),
    }
}

async fn finish_cleanup(
    sync: &mut PoolSync<'_>,
    session_id: &str,
    intent: CleanupIntent,
) -> crate::Result<()> {
    let release = matches!(&intent, CleanupIntent::Terminate);
    if let CleanupIntent::StartupFailure(failure) = &intent {
        match sync
            .outposts
            .report_startup_failure(session_id, failure)
            .await
        {
            Ok(()) => {
                mark_cleanup_intent(sync, session_id, &CleanupIntent::Delete).await?;
            }
            // Not ours any more (claim expired and moved on): the pod is an
            // orphan either way.
            Err(Error::ClaimConflict { .. }) => {
                clear_cleanup_intent(&sync.secrets, &session_token_secret_name(session_id)).await?;
                return teardown_session(sync, session_id).await;
            }
            Err(e) if e.is_not_found() => {
                return Err(anyhow::anyhow!(
                    "session {session_id}: upstream has no startup-failure operation \
                     (404); keeping the claim until it can take the report"
                )
                .into());
            }
            Err(e) => return Err(e),
        }
    }
    delete_ignoring_missing(&sync.pods, &worker_pod_name(session_id)).await?;
    sync.pods_by_session.remove(session_id);
    if release {
        sync.provider.on_terminate(session_id).await?;
        release_ignoring_conflict(sync.outposts, session_id).await?;
        delete_ignoring_missing(&sync.secrets, &session_token_secret_name(session_id)).await
    } else {
        delete_session_artifacts(
            sync.provider,
            &sync.secrets,
            session_id,
            &session_token_secret_name(session_id),
        )
        .await
    }
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

/// Prepare the status to patch, or `None` when patching would be pure churn.
///
/// Every status patch fires the controller's own pool watch and immediately
/// re-triggers reconcile, so unconditional writes self-trigger in a loop —
/// on a persistently failing pool (e.g. a bad token) that loop is hot,
/// hammering both the cluster and the upstream API instead of honoring
/// [`error_policy`]'s backoff. Two measures keep the patch a no-op unless
/// something meaningful changed:
///
/// - a condition equal to the current one (up to its timestamp) keeps its
///   previous `lastTransitionTime`;
/// - when only `lastSynced` moved, it is refreshed only after it has gone
///   at least `min_last_synced_refresh` stale.
fn prepare_status_patch(
    old: Option<&OutpostPoolStatus>,
    mut status: OutpostPoolStatus,
    min_last_synced_refresh: Duration,
) -> Option<OutpostPoolStatus> {
    let Some(old) = old else { return Some(status) };
    for condition in &mut status.conditions {
        if let Some(previous) = old.conditions.iter().find(|c| {
            c.type_ == condition.type_
                && c.status == condition.status
                && c.reason == condition.reason
                && c.message == condition.message
        }) {
            condition.last_transition_time = previous.last_transition_time.clone();
        }
    }
    let mut comparable = status.clone();
    comparable.last_synced = old.last_synced.clone();
    if &comparable != old {
        return Some(status);
    }
    if status.last_synced == old.last_synced {
        return None;
    }
    let stale = old
        .last_synced
        .as_deref()
        .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
        .is_none_or(|t| {
            chrono::Utc::now() - t.with_timezone(&chrono::Utc)
                >= chrono::Duration::from_std(min_last_synced_refresh).unwrap_or_default()
        });
    stale.then_some(status)
}

async fn update_status(
    pool: &OutpostPool,
    ctx: &Context,
    status: OutpostPoolStatus,
    ns: &str,
) -> crate::Result<()> {
    let Some(status) =
        prepare_status_patch(pool.status.as_ref(), status, ctx.config.reconcile_interval)
    else {
        return Ok(());
    };
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

#[cfg(test)]
mod tests {
    use super::*;

    fn status(phase: &str, message: Option<&str>, last_synced: Option<&str>) -> OutpostPoolStatus {
        OutpostPoolStatus {
            phase: Some(phase.to_string()),
            claimed_sessions: 0,
            last_synced: last_synced.map(str::to_string),
            watch_cursor: None,
            conditions: vec![Condition {
                type_: "Ready".to_string(),
                status: (phase == "Ready").to_string(),
                reason: Some(
                    if phase == "Ready" {
                        "Synced"
                    } else {
                        "SyncFailed"
                    }
                    .to_string(),
                ),
                message: message.map(str::to_string),
                last_transition_time: Some(chrono::Utc::now().to_rfc3339()),
            }],
        }
    }

    #[test]
    fn unchanged_failing_status_is_not_patched() {
        let old = status("Unauthorized", Some("403"), Some("2026-01-01T00:00:00Z"));
        let new = status("Unauthorized", Some("403"), Some("2026-01-01T00:00:00Z"));
        assert_eq!(
            prepare_status_patch(Some(&old), new, Duration::from_secs(30)),
            None
        );
    }

    #[test]
    fn a_transition_is_patched_and_stamps_a_new_transition_time() {
        let old = status("Unauthorized", Some("403"), Some("2026-01-01T00:00:00Z"));
        let new = status("Ready", None, Some("2026-01-01T00:00:30Z"));
        let patched = prepare_status_patch(Some(&old), new.clone(), Duration::from_secs(30));
        assert_eq!(patched, Some(new));
    }

    #[test]
    fn unchanged_condition_keeps_its_transition_time() {
        let old = status("Ready", None, Some("2026-01-01T00:00:00Z"));
        let new = status("Ready", None, None);
        let patched = prepare_status_patch(Some(&old), new, Duration::from_secs(30))
            .expect("stale lastSynced refresh");
        assert_eq!(
            patched.conditions[0].last_transition_time,
            old.conditions[0].last_transition_time
        );
    }

    #[test]
    fn fresh_last_synced_only_change_is_skipped() {
        let just_now = chrono::Utc::now().to_rfc3339();
        let old = status("Ready", None, Some(&just_now));
        let new = status("Ready", None, Some(&chrono::Utc::now().to_rfc3339()));
        assert_eq!(
            prepare_status_patch(Some(&old), new, Duration::from_secs(30)),
            None
        );
    }
    #[tokio::test]
    async fn startup_failure_cleanup_keeps_the_secret_as_the_retry_handle() {
        use std::sync::atomic::Ordering;

        let (server, client, pool, ctx, http) = retention_fixture(Some(3600)).await;
        let action = Action::StartupFailed {
            session_id: "s".to_string(),
            failure: crate::api::StartupFailure {
                reason: crate::api::StartupFailureReason::ImagePullError,
                image: Some("img:x".to_string()),
                message: "pull failed".to_string(),
            },
        };
        let (outposts, provider) = sync_deps(&pool, &client);
        let mut sync = PoolSync {
            pool: &pool,
            ctx: &ctx,
            outposts: &outposts,
            provider: &provider,
            pods: Api::namespaced(client.clone(), "default"),
            secrets: Api::namespaced(client.clone(), "default"),
            pods_by_session: BTreeMap::new(),
            snapshot_pending: false,
        };

        // Report refused upstream: nothing is torn down.
        server.report_status.store(500, Ordering::SeqCst);
        assert!(execute(&mut sync, &action).await.is_err());
        assert!(*server.secret.lock().unwrap());
        assert!(*server.pod.lock().unwrap());
        assert!(server.pvc.lock().unwrap().is_some());

        // Report conflict (claimed elsewhere): pod and secret go, artifacts
        // stay — the PVC may belong to a new owner now.
        server.report_status.store(409, Ordering::SeqCst);
        execute(&mut sync, &action).await.unwrap();
        assert!(!*server.pod.lock().unwrap());
        assert!(!*server.secret.lock().unwrap());
        assert!(server.pvc.lock().unwrap().is_some());
        assert!(
            !server
                .secret_annotations
                .lock()
                .unwrap()
                .contains_key(ANNOTATION_CLEANUP_INTENT)
        );

        // Report accepted but PVC cleanup fails: pod is gone, the secret is
        // kept, and the error propagates.
        *server.pod.lock().unwrap() = true;
        *server.secret.lock().unwrap() = true;
        server.report_status.store(200, Ordering::SeqCst);
        server.fail_pvc_delete.store(true, Ordering::SeqCst);
        assert!(execute(&mut sync, &action).await.is_err());
        assert!(!*server.pod.lock().unwrap());
        assert!(*server.secret.lock().unwrap());
        assert!(server.pvc.lock().unwrap().is_some());
        assert_eq!(intent_kind(&server), "delete");

        // The retained secret drives a retry later: still failing PVC means
        // still no secret deletion.
        assert!(apply(pool.clone(), &ctx).await.is_err());
        assert!(*server.secret.lock().unwrap());
        assert!(server.pvc.lock().unwrap().is_some());

        // Once cleanup succeeds the secret goes too.
        server.fail_pvc_delete.store(false, Ordering::SeqCst);
        apply(pool.clone(), &ctx).await.unwrap();
        assert!(!*server.secret.lock().unwrap());
        assert!(server.pvc.lock().unwrap().is_none());

        retention_teardown(&pool, &ctx, http);
    }

    #[derive(Clone)]
    struct RetentionServer {
        pvc: Arc<std::sync::Mutex<Option<serde_json::Value>>>,
        secret: Arc<std::sync::Mutex<bool>>,
        secret_annotations: Arc<std::sync::Mutex<BTreeMap<String, String>>>,
        pod: Arc<std::sync::Mutex<bool>>,
        sessions: Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
        claimed_image: Arc<std::sync::Mutex<Option<String>>>,
        report_bodies: Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
        pod_create_count: Arc<std::sync::atomic::AtomicUsize>,
        fail_pvc_patch: Arc<std::sync::atomic::AtomicBool>,
        fail_pvc_delete: Arc<std::sync::atomic::AtomicBool>,
        fail_pod_delete: Arc<std::sync::atomic::AtomicBool>,
        fail_secret_patch: Arc<std::sync::atomic::AtomicBool>,
        report_status: Arc<std::sync::atomic::AtomicU16>,
        release_status: Arc<std::sync::atomic::AtomicU16>,
        report_count: Arc<std::sync::atomic::AtomicUsize>,
        release_count: Arc<std::sync::atomic::AtomicUsize>,
    }

    fn retained_pvc() -> serde_json::Value {
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {
                "name": crate::controller::state_pvc_name("s"),
                "namespace": "default",
                "uid": "original-pvc",
                "labels": {
                    LABEL_POOL: "p",
                    super::super::LABEL_SESSION_ID: "s",
                },
                "annotations": {},
            },
            "spec": {
                "volumeName": "original-pv",
                "dataSource": {
                    "apiGroup": "",
                    "kind": "PersistentVolumeClaim",
                    "name": "golden-home",
                },
            },
        })
    }

    async fn retention_fixture(
        snapshot_ttl_seconds: Option<u64>,
    ) -> (
        RetentionServer,
        kube::Client,
        Arc<OutpostPool>,
        Context,
        tokio::task::JoinHandle<()>,
    ) {
        use std::sync::Mutex;
        use std::sync::atomic::{AtomicBool, AtomicU16, AtomicUsize, Ordering};

        use axum::extract::{Request, State};
        use axum::http::StatusCode;
        use axum::response::{IntoResponse, Json, Response};
        use axum::routing::any;
        use axum::{Router, serve};

        fn status(code: u16, reason: &str) -> Response {
            (
                StatusCode::from_u16(code).unwrap(),
                Json(serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Status",
                    "status": "Failure",
                    "reason": reason,
                    "code": code,
                })),
            )
                .into_response()
        }

        fn success() -> Response {
            Json(serde_json::json!({
                "apiVersion": "v1", "kind": "Status", "status": "Success"
            }))
            .into_response()
        }

        fn session_secret_json(server: &RetentionServer) -> serde_json::Value {
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {
                    "name": session_token_secret_name("s"),
                    "namespace": "default",
                    "labels": {
                        LABEL_POOL: "p",
                        super::super::LABEL_SESSION_ID: "s",
                    },
                    "annotations": server.secret_annotations.lock().unwrap().clone(),
                },
            })
        }

        async fn handle(
            State((server, pool_json)): State<(RetentionServer, serde_json::Value)>,
            req: Request,
        ) -> Response {
            let method = req.method().clone();
            let path = req.uri().path().to_string();
            let watch = req.uri().query().unwrap_or("").contains("watch=true");
            let body = axum::body::to_bytes(req.into_body(), usize::MAX)
                .await
                .unwrap();
            let ns = "/api/v1/namespaces/default";

            if path == "/opbeta/outposts/devins" {
                if watch {
                    return (
                        StatusCode::OK,
                        [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                        "",
                    )
                        .into_response();
                }
                let items = server.sessions.lock().unwrap().clone();
                return Json(serde_json::json!({
                    "items": items, "cursor": null, "has_next_page": false, "total": 0
                }))
                .into_response();
            }
            if let Some(rest) = path.strip_prefix("/opbeta/outposts/devins/") {
                match (method.as_str(), rest) {
                    ("POST", "s/claim") => {
                        return Json(serde_json::json!({
                            "metadata": {"session_id": "s", "outpost_id": "pool_x"},
                            "spec": {
                                "kind": "resume",
                                "platform": "linux",
                                "image": server.claimed_image.lock().unwrap().clone(),
                            },
                            "status": {
                                "phase": "claimed",
                                "acceptor_id": "us",
                                "session_status": "running",
                                "connect_token": "ct",
                                "gateway_url": "ws://gw",
                            },
                        }))
                        .into_response();
                    }
                    ("POST", "s/release") => {
                        server.release_count.fetch_add(1, Ordering::SeqCst);
                        let code = server.release_status.load(Ordering::SeqCst);
                        return match code {
                            200 => success(),
                            _ => status(code, "Failure"),
                        };
                    }
                    ("POST", "s/startup-failure") => {
                        server.report_count.fetch_add(1, Ordering::SeqCst);
                        server
                            .report_bodies
                            .lock()
                            .unwrap()
                            .push(serde_json::from_slice(&body).unwrap());
                        let code = server.report_status.load(Ordering::SeqCst);
                        return match code {
                            200 => success(),
                            _ => status(code, "Failure"),
                        };
                    }
                    _ => return status(404, "NotFound"),
                }
            }

            if path == format!("{ns}/pods") && method.as_str() == "GET" {
                let items = if *server.pod.lock().unwrap() {
                    vec![serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "metadata": {
                            "name": worker_pod_name("s"),
                            "namespace": "default",
                            "labels": {
                                LABEL_POOL: "p",
                                super::super::LABEL_SESSION_ID: "s",
                            },
                            "ownerReferences": [{
                                "apiVersion": "outposts.cognition.com/v1alpha1",
                                "kind": "OutpostPool",
                                "name": "p",
                                "uid": "pool-uid",
                            }],
                        },
                        "status": {"phase": "Running"},
                    })]
                } else {
                    vec![]
                };
                return Json(serde_json::json!({
                    "apiVersion": "v1", "kind": "PodList", "items": items
                }))
                .into_response();
            }
            if path == format!("{ns}/pods") && method.as_str() == "POST" {
                let pod: serde_json::Value = serde_json::from_slice(&body).unwrap();
                server.pod_create_count.fetch_add(1, Ordering::SeqCst);
                *server.pod.lock().unwrap() = true;
                return Json(pod).into_response();
            }
            if path.starts_with(&format!("{ns}/pods/")) {
                if method.as_str() == "DELETE" {
                    if server.fail_pod_delete.load(Ordering::SeqCst) {
                        return status(500, "InternalError");
                    }
                    let mut pod = server.pod.lock().unwrap();
                    if !*pod {
                        return status(404, "NotFound");
                    }
                    *pod = false;
                    return success();
                }
                return status(500, "InternalError");
            }

            if path == format!("{ns}/secrets") && method.as_str() == "GET" {
                let items = if *server.secret.lock().unwrap() {
                    vec![session_secret_json(&server)]
                } else {
                    vec![]
                };
                return Json(serde_json::json!({
                    "apiVersion": "v1", "kind": "SecretList", "items": items
                }))
                .into_response();
            }
            if let Some(name) = path.strip_prefix(&format!("{ns}/secrets/")) {
                let session_secret = name == session_token_secret_name("s");
                match method.as_str() {
                    "GET" if name == "t" => {
                        return Json(serde_json::json!({
                            "apiVersion": "v1",
                            "kind": "Secret",
                            "metadata": {"name": "t", "namespace": "default"},
                            "data": {"token": "dG9r"},
                        }))
                        .into_response();
                    }
                    "GET" if session_secret && *server.secret.lock().unwrap() => {
                        return Json(session_secret_json(&server)).into_response();
                    }
                    "PATCH" if session_secret => {
                        if server.fail_secret_patch.load(Ordering::SeqCst) {
                            return status(500, "InternalError");
                        }
                        let patch: serde_json::Value = serde_json::from_slice(&body).unwrap();
                        if !*server.secret.lock().unwrap()
                            && patch.pointer("/metadata/name").is_none()
                        {
                            return status(404, "NotFound");
                        }
                        if let Some(annotations) = patch
                            .pointer("/metadata/annotations")
                            .and_then(|a| a.as_object())
                        {
                            let mut stored = server.secret_annotations.lock().unwrap();
                            for (key, value) in annotations {
                                if value.is_null() {
                                    stored.remove(key);
                                } else {
                                    stored.insert(
                                        key.clone(),
                                        value
                                            .as_str()
                                            .map(str::to_string)
                                            .unwrap_or_else(|| value.to_string()),
                                    );
                                }
                            }
                        }
                        *server.secret.lock().unwrap() = true;
                        return Json(session_secret_json(&server)).into_response();
                    }
                    "DELETE" if session_secret => {
                        let mut secret = server.secret.lock().unwrap();
                        if !*secret {
                            return status(404, "NotFound");
                        }
                        *secret = false;
                        server.secret_annotations.lock().unwrap().clear();
                        return success();
                    }
                    _ => return status(404, "NotFound"),
                }
            }

            if path == format!("{ns}/persistentvolumeclaims") && method.as_str() == "GET" {
                let items = server
                    .pvc
                    .lock()
                    .unwrap()
                    .clone()
                    .into_iter()
                    .collect::<Vec<_>>();
                return Json(serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "PersistentVolumeClaimList",
                    "items": items,
                }))
                .into_response();
            }
            if path.starts_with(&format!("{ns}/persistentvolumeclaims/")) {
                match method.as_str() {
                    "GET" => {
                        return match server.pvc.lock().unwrap().clone() {
                            Some(pvc) => Json(pvc).into_response(),
                            None => status(404, "NotFound"),
                        };
                    }
                    "PATCH" => {
                        if server.fail_pvc_patch.load(Ordering::SeqCst) {
                            return status(500, "InternalError");
                        }
                        let patch: serde_json::Value = serde_json::from_slice(&body).unwrap();
                        let mut stored = server.pvc.lock().unwrap();
                        let Some(pvc) = stored.as_mut() else {
                            return status(404, "NotFound");
                        };
                        if let Some(annotations) = patch
                            .pointer("/metadata/annotations")
                            .and_then(|a| a.as_object())
                        {
                            let target = pvc
                                .pointer_mut("/metadata/annotations")
                                .and_then(|a| a.as_object_mut())
                                .expect("fixture PVC keeps an annotations map");
                            for (key, value) in annotations {
                                if value.is_null() {
                                    target.remove(key);
                                } else {
                                    target.insert(key.clone(), value.clone());
                                }
                            }
                        }
                        return Json(pvc.clone()).into_response();
                    }
                    "DELETE" => {
                        if server.fail_pvc_delete.load(Ordering::SeqCst) {
                            return status(500, "InternalError");
                        }
                        let mut stored = server.pvc.lock().unwrap();
                        if stored.is_none() {
                            return status(404, "NotFound");
                        }
                        *stored = None;
                        return success();
                    }
                    _ => return status(500, "InternalError"),
                }
            }

            if path.ends_with("/status")
                && path.contains("/outpostpools/")
                && method.as_str() == "PATCH"
            {
                let patch: serde_json::Value = serde_json::from_slice(&body).unwrap();
                let mut pool = pool_json.clone();
                pool["status"] = patch["status"].clone();
                return Json(pool).into_response();
            }

            status(500, "InternalError")
        }

        let server = RetentionServer {
            pvc: Arc::new(Mutex::new(Some(retained_pvc()))),
            secret: Arc::new(Mutex::new(true)),
            secret_annotations: Arc::new(Mutex::new(BTreeMap::new())),
            pod: Arc::new(Mutex::new(true)),
            sessions: Arc::new(Mutex::new(Vec::new())),
            claimed_image: Arc::new(Mutex::new(None)),
            report_bodies: Arc::new(Mutex::new(Vec::new())),
            pod_create_count: Arc::new(AtomicUsize::new(0)),
            fail_pvc_patch: Arc::new(AtomicBool::new(false)),
            fail_pvc_delete: Arc::new(AtomicBool::new(false)),
            fail_pod_delete: Arc::new(AtomicBool::new(false)),
            fail_secret_patch: Arc::new(AtomicBool::new(false)),
            report_status: Arc::new(AtomicU16::new(200)),
            release_status: Arc::new(AtomicU16::new(200)),
            report_count: Arc::new(AtomicUsize::new(0)),
            release_count: Arc::new(AtomicUsize::new(0)),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        let mut pool = OutpostPool::new(
            "p",
            crate::crd::OutpostPoolSpec {
                pool_id: "pool_x".to_string(),
                token_secret_ref: crate::crd::SecretKeyRef {
                    name: "t".to_string(),
                    key: "token".to_string(),
                },
                api_url: Some(format!("http://{address}")),
                max_concurrent_sessions: 1,
                worker: crate::crd::WorkerTemplate {
                    template: Default::default(),
                    container_name: "devin-worker".to_string(),
                    overrides: Default::default(),
                },
                resume: crate::crd::ResumeConfig {
                    policy: crate::crd::ResumePolicy::FilesystemSnapshot,
                    snapshot_ttl_seconds,
                    ..Default::default()
                },
                image_build: None,
            },
        );
        pool.metadata.namespace = Some("default".to_string());
        pool.metadata.uid = Some("pool-uid".to_string());
        let pool = Arc::new(pool);
        let pool_json = serde_json::to_value(&*pool).unwrap();

        let app = Router::new()
            .route("/{*path}", any(handle))
            .with_state((server.clone(), pool_json));
        let http = tokio::spawn(async move { serve(listener, app).await.unwrap() });
        let _ = rustls::crypto::ring::default_provider().install_default();

        let client = kube::Client::try_from(kube::Config::new(
            format!("http://{address}").parse().unwrap(),
        ))
        .unwrap();
        let (trigger_tx, _trigger_rx) = futures::channel::mpsc::unbounded::<()>();
        let config = OperatorConfig {
            acceptor_id: Some("us".to_string()),
            ..Default::default()
        };
        let ctx = Context::new(
            client.clone(),
            Arc::new(config),
            Arc::new(Metrics::default()),
            Arc::new(PoolWatchers::new(trigger_tx)),
        );
        (server, client, pool, ctx, http)
    }

    fn retention_teardown(pool: &OutpostPool, ctx: &Context, http: tokio::task::JoinHandle<()>) {
        ctx.watchers.remove(&pool.uid().unwrap_or_default());
        http.abort();
    }

    fn sync_deps(
        pool: &Arc<OutpostPool>,
        client: &kube::Client,
    ) -> (OutpostsClient, crate::snapshot::FilesystemSnapshotProvider) {
        (
            OutpostsClient::new(pool.spec.api_url.clone().unwrap(), "tok", "us").unwrap(),
            crate::snapshot::FilesystemSnapshotProvider::new(client.clone(), pool.clone()),
        )
    }

    fn restart_context(client: &kube::Client, ctx: &Context) -> Context {
        let (trigger_tx, _trigger_rx) = futures::channel::mpsc::unbounded::<()>();
        Context::new(
            client.clone(),
            ctx.config.clone(),
            ctx.metrics.clone(),
            Arc::new(PoolWatchers::new(trigger_tx)),
        )
    }

    fn intent_kind(server: &RetentionServer) -> serde_json::Value {
        let raw = server.secret_annotations.lock().unwrap()[ANNOTATION_CLEANUP_INTENT].clone();
        serde_json::from_str::<serde_json::Value>(&raw).unwrap()["kind"].clone()
    }

    #[tokio::test]
    async fn terminated_session_cleanup_retries_after_queue_disappears() {
        use std::sync::atomic::Ordering;

        let (server, client, pool, ctx, http) = retention_fixture(Some(3600)).await;
        let (outposts, provider) = sync_deps(&pool, &client);
        let mut sync = PoolSync {
            pool: &pool,
            ctx: &ctx,
            outposts: &outposts,
            provider: &provider,
            pods: Api::namespaced(client.clone(), "default"),
            secrets: Api::namespaced(client.clone(), "default"),
            pods_by_session: BTreeMap::new(),
            snapshot_pending: false,
        };
        let action = Action::Terminate {
            session_id: "s".to_string(),
        };

        server.fail_pvc_delete.store(true, Ordering::SeqCst);
        assert!(execute(&mut sync, &action).await.is_err());
        assert_eq!(server.release_count.load(Ordering::SeqCst), 0);
        assert!(*server.secret.lock().unwrap());
        assert!(server.pvc.lock().unwrap().is_some());
        assert_eq!(intent_kind(&server), "terminate");

        let ctx = restart_context(&client, &ctx);
        assert!(apply(pool.clone(), &ctx).await.is_err());
        assert!(*server.secret.lock().unwrap());
        assert!(server.pvc.lock().unwrap().is_some());
        assert_eq!(server.release_count.load(Ordering::SeqCst), 0);

        server.fail_pvc_delete.store(false, Ordering::SeqCst);
        apply(pool.clone(), &ctx).await.unwrap();
        assert!(server.pvc.lock().unwrap().is_none());
        assert!(!*server.secret.lock().unwrap());
        assert_eq!(server.release_count.load(Ordering::SeqCst), 1);

        *server.pvc.lock().unwrap() = Some(retained_pvc());
        *server.secret.lock().unwrap() = true;
        server.secret_annotations.lock().unwrap().insert(
            ANNOTATION_CLEANUP_INTENT.to_string(),
            "{\"kind\":\"terminate\"}".to_string(),
        );
        server.release_status.store(500, Ordering::SeqCst);
        assert!(apply(pool.clone(), &ctx).await.is_err());
        assert!(*server.secret.lock().unwrap());
        assert!(server.pvc.lock().unwrap().is_none());
        server.release_status.store(200, Ordering::SeqCst);
        apply(pool.clone(), &ctx).await.unwrap();
        assert!(!*server.secret.lock().unwrap());

        *server.pvc.lock().unwrap() = Some(retained_pvc());
        *server.secret.lock().unwrap() = true;
        server.secret_annotations.lock().unwrap().insert(
            ANNOTATION_CLEANUP_INTENT.to_string(),
            "{\"kind\":\"terminate\"}".to_string(),
        );
        server.release_status.store(404, Ordering::SeqCst);
        apply(pool.clone(), &ctx).await.unwrap();
        assert!(server.pvc.lock().unwrap().is_none());
        assert!(!*server.secret.lock().unwrap());

        retention_teardown(&pool, &ctx, http);
    }

    #[tokio::test]
    async fn cleanup_intent_is_durable_before_backend_acknowledgement() {
        use std::sync::atomic::Ordering;

        let (server, client, pool, ctx, http) = retention_fixture(Some(3600)).await;
        let action = Action::StartupFailed {
            session_id: "s".to_string(),
            failure: crate::api::StartupFailure {
                reason: crate::api::StartupFailureReason::ImagePullError,
                image: Some("img:x".to_string()),
                message: "pull failed".to_string(),
            },
        };
        let (outposts, provider) = sync_deps(&pool, &client);
        let mut sync = PoolSync {
            pool: &pool,
            ctx: &ctx,
            outposts: &outposts,
            provider: &provider,
            pods: Api::namespaced(client.clone(), "default"),
            secrets: Api::namespaced(client.clone(), "default"),
            pods_by_session: BTreeMap::new(),
            snapshot_pending: false,
        };

        server.fail_secret_patch.store(true, Ordering::SeqCst);
        assert!(execute(&mut sync, &action).await.is_err());
        assert_eq!(server.report_count.load(Ordering::SeqCst), 0);
        assert!(*server.secret.lock().unwrap());
        assert!(*server.pod.lock().unwrap());
        assert!(server.pvc.lock().unwrap().is_some());
        server.fail_secret_patch.store(false, Ordering::SeqCst);

        server.report_status.store(500, Ordering::SeqCst);
        assert!(execute(&mut sync, &action).await.is_err());
        assert_eq!(server.report_count.load(Ordering::SeqCst), 1);
        assert_eq!(intent_kind(&server), "startup_failure");

        server.report_status.store(200, Ordering::SeqCst);
        server.fail_secret_patch.store(true, Ordering::SeqCst);
        assert!(apply(pool.clone(), &ctx).await.is_err());
        assert_eq!(server.report_count.load(Ordering::SeqCst), 2);
        assert!(*server.secret.lock().unwrap());
        assert!(server.pvc.lock().unwrap().is_some());
        assert_eq!(intent_kind(&server), "startup_failure");

        server.fail_secret_patch.store(false, Ordering::SeqCst);
        apply(pool.clone(), &ctx).await.unwrap();
        assert_eq!(server.report_count.load(Ordering::SeqCst), 3);
        assert!(!*server.secret.lock().unwrap());
        assert!(!*server.pod.lock().unwrap());
        assert!(server.pvc.lock().unwrap().is_none());

        retention_teardown(&pool, &ctx, http);
    }

    #[tokio::test]
    async fn rejected_session_image_reports_startup_failure_before_creating_a_pod() {
        use std::sync::atomic::Ordering;

        let (server, client, pool, ctx, http) = retention_fixture(Some(3600)).await;
        let mut pool = pool;
        Arc::make_mut(&mut pool).spec.image_build = Some(crate::crd::ImageBuildConfig {
            repository: Some("reg.example/drs".to_string()),
            ..Default::default()
        });
        let evil = format!("evil.example/drs/org@sha256:{}", "a".repeat(64));
        *server.claimed_image.lock().unwrap() = Some(evil.clone());
        *server.pod.lock().unwrap() = false;
        *server.secret.lock().unwrap() = false;
        *server.pvc.lock().unwrap() = None;
        let (outposts, provider) = sync_deps(&pool, &client);
        let mut sync = PoolSync {
            pool: &pool,
            ctx: &ctx,
            outposts: &outposts,
            provider: &provider,
            pods: Api::namespaced(client.clone(), "default"),
            secrets: Api::namespaced(client.clone(), "default"),
            pods_by_session: BTreeMap::new(),
            snapshot_pending: false,
        };

        let expected_body = serde_json::json!({
            "acceptor_id": "us",
            "reason": "image_pull_error",
            "image": evil,
            "message": "session image must be pinned by digest under the pool's image repository",
        });
        server.report_status.store(500, Ordering::SeqCst);
        assert!(start_worker(&mut sync, "s").await.is_err());
        assert_eq!(server.report_count.load(Ordering::SeqCst), 1);
        assert_eq!(server.report_bodies.lock().unwrap()[0], expected_body);
        assert_eq!(server.pod_create_count.load(Ordering::SeqCst), 0);
        assert!(!*server.pod.lock().unwrap());
        assert!(server.pvc.lock().unwrap().is_none());
        assert!(*server.secret.lock().unwrap());
        assert_eq!(intent_kind(&server), "startup_failure");
        assert_eq!(server.release_count.load(Ordering::SeqCst), 0);

        server.report_status.store(200, Ordering::SeqCst);
        let ctx = restart_context(&client, &ctx);
        apply(pool.clone(), &ctx).await.unwrap();
        assert_eq!(server.report_count.load(Ordering::SeqCst), 2);
        assert_eq!(server.report_bodies.lock().unwrap()[1], expected_body);
        assert_eq!(server.pod_create_count.load(Ordering::SeqCst), 0);
        assert!(!*server.pod.lock().unwrap());
        assert!(server.pvc.lock().unwrap().is_none());
        assert!(!*server.secret.lock().unwrap());
        assert_eq!(server.release_count.load(Ordering::SeqCst), 0);

        retention_teardown(&pool, &ctx, http);
    }

    #[tokio::test]
    async fn live_or_reclaimed_session_outlives_stale_cleanup_intent() {
        use std::sync::atomic::Ordering;

        let (server, _client, pool, ctx, http) = retention_fixture(Some(3600)).await;

        *server.pod.lock().unwrap() = false;
        server.secret_annotations.lock().unwrap().insert(
            ANNOTATION_CLEANUP_INTENT.to_string(),
            "{\"kind\":\"delete\"}".to_string(),
        );
        server.sessions.lock().unwrap().push(serde_json::json!({
            "metadata": {"session_id": "s", "outpost_id": "pool_x"},
            "spec": {"kind": "resume", "platform": "linux"},
            "status": {"phase": "pending", "session_status": "pending"},
        }));

        apply(pool.clone(), &ctx).await.unwrap();

        assert!(*server.pod.lock().unwrap());
        assert!(*server.secret.lock().unwrap());
        assert!(
            !server
                .secret_annotations
                .lock()
                .unwrap()
                .contains_key(ANNOTATION_CLEANUP_INTENT)
        );
        let pvc = server.pvc.lock().unwrap().clone().unwrap();
        assert_eq!(pvc["metadata"]["uid"], "original-pvc");
        assert_eq!(pvc["spec"]["volumeName"], "original-pv");

        *server.sessions.lock().unwrap() = vec![serde_json::json!({
            "metadata": {"session_id": "s", "outpost_id": "pool_x"},
            "spec": {"kind": "resume", "platform": "linux"},
            "status": {
                "phase": "claimed",
                "acceptor_id": "us",
                "claim_deadline": chrono::Utc::now().timestamp() + 86_400,
                "session_status": "running",
            },
        })];
        server.secret_annotations.lock().unwrap().insert(
            ANNOTATION_CLEANUP_INTENT.to_string(),
            "{\"kind\":\"delete\"}".to_string(),
        );

        apply(pool.clone(), &ctx).await.unwrap();

        assert!(*server.secret.lock().unwrap());
        assert!(
            server
                .secret_annotations
                .lock()
                .unwrap()
                .contains_key(ANNOTATION_CLEANUP_INTENT)
        );
        assert!(server.pvc.lock().unwrap().is_some());
        assert_eq!(server.release_count.load(Ordering::SeqCst), 0);

        retention_teardown(&pool, &ctx, http);
    }

    #[tokio::test]
    async fn vanished_session_reconcile_preserves_pvc_for_resume() {
        let (server, client, pool, ctx, http) = retention_fixture(Some(3600)).await;

        apply(pool.clone(), &ctx).await.unwrap();

        assert!(!*server.pod.lock().unwrap());
        assert!(!*server.secret.lock().unwrap());
        let retained = server.pvc.lock().unwrap().clone().unwrap();
        assert_eq!(retained["metadata"]["uid"], "original-pvc");
        assert_eq!(retained["spec"]["volumeName"], "original-pv");
        assert!(
            chrono::DateTime::parse_from_rfc3339(
                retained["metadata"]["annotations"][super::super::ANNOTATION_SUSPENDED_AT]
                    .as_str()
                    .unwrap()
            )
            .is_ok()
        );

        let resume_session: crate::api::OutpostDevin = serde_json::from_value(serde_json::json!({
            "metadata": {"session_id": "s", "outpost_id": "pool_x"},
            "spec": {"kind": "resume", "platform": "linux"},
            "status": {"phase": "pending", "session_status": "pending"},
        }))
        .unwrap();
        let provider =
            crate::snapshot::FilesystemSnapshotProvider::new(client.clone(), pool.clone());
        provider.prepare(&resume_session).await.unwrap();

        let resumed = server.pvc.lock().unwrap().clone().unwrap();
        assert_eq!(resumed["metadata"]["uid"], "original-pvc");
        assert_eq!(resumed["spec"]["volumeName"], "original-pv");
        assert!(
            resumed["metadata"]["annotations"][super::super::ANNOTATION_SUSPENDED_AT].is_null()
        );

        provider.gc().await.unwrap();
        assert!(server.pvc.lock().unwrap().is_some());

        retention_teardown(&pool, &ctx, http);
    }

    #[tokio::test]
    async fn vanished_session_retention_retries_and_keeps_existing_deadline() {
        use std::sync::atomic::Ordering;

        let (server, _client, pool, ctx, http) = retention_fixture(Some(3600)).await;

        server.fail_pvc_patch.store(true, Ordering::SeqCst);
        assert!(apply(pool.clone(), &ctx).await.is_err());
        assert!(*server.secret.lock().unwrap());
        assert!(server.pvc.lock().unwrap().is_some());

        server.fail_pvc_patch.store(false, Ordering::SeqCst);
        apply(pool.clone(), &ctx).await.unwrap();
        assert!(!*server.secret.lock().unwrap());
        assert!(
            chrono::DateTime::parse_from_rfc3339(
                server.pvc.lock().unwrap().clone().unwrap()["metadata"]["annotations"]
                    [super::super::ANNOTATION_SUSPENDED_AT]
                    .as_str()
                    .unwrap()
            )
            .is_ok()
        );

        let deadline = (chrono::Utc::now() - chrono::Duration::minutes(5)).to_rfc3339();
        {
            let mut guard = server.pvc.lock().unwrap();
            let pvc = guard.as_mut().unwrap();
            pvc["metadata"]["annotations"][super::super::ANNOTATION_SUSPENDED_AT] =
                serde_json::Value::String(deadline.clone());
        }
        *server.secret.lock().unwrap() = true;
        apply(pool.clone(), &ctx).await.unwrap();
        assert!(!*server.secret.lock().unwrap());
        assert_eq!(
            server.pvc.lock().unwrap().clone().unwrap()["metadata"]["annotations"]
                [super::super::ANNOTATION_SUSPENDED_AT],
            serde_json::Value::String(deadline)
        );

        retention_teardown(&pool, &ctx, http);
    }

    #[tokio::test]
    async fn vanished_session_retention_expires_only_after_ttl() {
        let (server, client, pool, ctx, http) = retention_fixture(Some(3600)).await;
        let provider =
            crate::snapshot::FilesystemSnapshotProvider::new(client.clone(), pool.clone());

        apply(pool.clone(), &ctx).await.unwrap();
        provider.gc().await.unwrap();
        assert!(server.pvc.lock().unwrap().is_some());

        {
            let mut guard = server.pvc.lock().unwrap();
            let pvc = guard.as_mut().unwrap();
            pvc["metadata"]["annotations"][super::super::ANNOTATION_SUSPENDED_AT] =
                serde_json::Value::String(
                    (chrono::Utc::now() - chrono::Duration::hours(2)).to_rfc3339(),
                );
        }
        provider.gc().await.unwrap();
        assert!(server.pvc.lock().unwrap().is_none());

        *server.pvc.lock().unwrap() = Some(retained_pvc());
        {
            let mut guard = server.pvc.lock().unwrap();
            let pvc = guard.as_mut().unwrap();
            pvc["metadata"]["annotations"][super::super::ANNOTATION_SUSPENDED_AT] =
                serde_json::Value::String(
                    (chrono::Utc::now() - chrono::Duration::hours(2)).to_rfc3339(),
                );
        }
        let mut unlimited = (*pool).clone();
        unlimited.spec.resume.snapshot_ttl_seconds = None;
        let unlimited =
            crate::snapshot::FilesystemSnapshotProvider::new(client.clone(), Arc::new(unlimited));
        unlimited.gc().await.unwrap();
        assert!(server.pvc.lock().unwrap().is_some());
        unlimited.on_terminate("s").await.unwrap();
        assert!(server.pvc.lock().unwrap().is_none());

        retention_teardown(&pool, &ctx, http);
    }
}
