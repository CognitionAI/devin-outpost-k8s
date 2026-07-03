//! GKE pod-snapshot provider for the `GkeSnapshot` resume policy.
//!
//! Backed by GKE Pod snapshots
//! (<https://docs.cloud.google.com/kubernetes-engine/docs/concepts/pod-snapshots>),
//! which checkpoint the *entire* pod — memory included — to Cloud Storage via
//! CRIU, and restore new pods from the checkpoint. Everything is driven
//! through the `podsnapshot.gke.io/v1` CRDs (via kube's dynamic API, since no
//! typed bindings exist):
//!
//! - [`SnapshotProvider::prepare`] applies one `PodSnapshotPolicy` per pool,
//!   selecting the pool's worker pods, with a `manual` trigger and snapshots
//!   grouped by the session-ID label (max one snapshot per session). When a
//!   `Ready` snapshot exists for the session, the recreated pod pins it via
//!   the `podsnapshot.gke.io/ps-name` annotation; GKE then restores the pod
//!   on creation (the pod's distilled spec must still match — env *values*
//!   like the rotated connect token are not part of it, so re-claimed tokens
//!   don't break matching). Restore is best-effort with a silent cold-start
//!   fallback, so [`SnapshotProvider::verify_restore`] checks the pod's
//!   `PodRestored` condition afterwards and the reconciler recycles a
//!   cold-started pod once.
//! - [`SnapshotProvider::on_suspend`] creates a `PodSnapshotManualTrigger`
//!   for the worker pod and reports [`SnapshotOutcome::InProgress`] until the
//!   resulting `PodSnapshot` goes `Ready` (the pod must stay alive while the
//!   checkpoint is captured). Snapshots stuck longer than
//!   [`SNAPSHOT_GIVE_UP`] are abandoned so a broken snapshot pipeline cannot
//!   hold a suspended session's pod (and its claim) hostage forever.
//!
//! Cluster prerequisites (admin-provided, see the how-to doc): GKE ≥ 1.35 with
//! the pod-snapshot feature enabled, worker pods sandboxed with gVisor
//! (`runtimeClassName: gvisor` in the pool's worker template), Workload
//! Identity, and a `PodSnapshotStorageConfig` referenced by
//! `resume.gkeStorageConfigName`.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{
    Api, ApiResource, DeleteParams, DynamicObject, GroupVersionKind, ListParams, Patch,
    PatchParams, PostParams,
};
use kube::{Client, Resource, ResourceExt};

use crate::controller::{LABEL_MANAGED_BY, LABEL_POOL, LABEL_SESSION_ID, session_labels};
use crate::crd::OutpostPool;
use crate::error::{Error, Result};

use super::{PreparedSession, RestoreVerdict, SnapshotOutcome, SnapshotProvider};

/// API group of the GKE pod-snapshot CRDs.
const GKE_SNAPSHOT_GROUP: &str = "podsnapshot.gke.io";
/// API version of the GKE pod-snapshot CRDs.
const GKE_SNAPSHOT_VERSION: &str = "v1";

/// Pod annotation pinning the exact `PodSnapshot` to restore from.
const ANNOTATION_PIN_SNAPSHOT: &str = "podsnapshot.gke.io/ps-name";
/// Pod condition GKE sets on pods it restored (the message carries the
/// snapshot name).
const CONDITION_POD_RESTORED: &str = "PodRestored";

/// Abandon a snapshot that has not gone `Ready` after this long and proceed
/// with pod teardown.
const SNAPSHOT_GIVE_UP: Duration = Duration::from_secs(30 * 60);

/// Snapshot provider backed by GKE Pod snapshots.
#[derive(Clone)]
pub struct GkeSnapshotProvider {
    client: Client,
    pool: Arc<OutpostPool>,
}

impl GkeSnapshotProvider {
    /// Build a provider for one pool.
    pub fn new(client: Client, pool: Arc<OutpostPool>) -> Self {
        Self { client, pool }
    }

    fn namespace(&self) -> &str {
        self.pool.meta().namespace.as_deref().unwrap_or("default")
    }

    fn dynamic_api(&self, kind: &str, plural: &str) -> Api<DynamicObject> {
        let gvk = GroupVersionKind::gvk(GKE_SNAPSHOT_GROUP, GKE_SNAPSHOT_VERSION, kind);
        let resource = ApiResource::from_gvk_with_plural(&gvk, plural);
        Api::namespaced_with(self.client.clone(), self.namespace(), &resource)
    }

    fn policies(&self) -> Api<DynamicObject> {
        self.dynamic_api("PodSnapshotPolicy", "podsnapshotpolicies")
    }

    fn triggers(&self) -> Api<DynamicObject> {
        self.dynamic_api("PodSnapshotManualTrigger", "podsnapshotmanualtriggers")
    }

    fn pod_snapshots(&self) -> Api<DynamicObject> {
        self.dynamic_api("PodSnapshot", "podsnapshots")
    }

    fn policy_name(&self) -> String {
        format!("outpost-{}", self.pool.name_any())
    }

    /// Name of the manual trigger for one suspend cycle. Includes the pod
    /// UID: each resume recreates the (deterministically named) pod, and a
    /// later suspend must fire a fresh trigger rather than reuse the spent
    /// one from the previous cycle.
    fn trigger_name(session_id: &str, pod: &Pod) -> String {
        let uid = pod.uid().unwrap_or_default();
        let uid_suffix: String = uid.chars().filter(|c| *c != '-').take(8).collect();
        let base = crate::controller::worker_pod_name(session_id);
        let base = &base[..base.len().min(63 - 1 - uid_suffix.len())];
        format!("{}-{uid_suffix}", base.trim_end_matches('-'))
    }

    /// Ready = the object has a `Ready` condition with status `"True"`.
    fn is_ready(obj: &DynamicObject) -> bool {
        obj.data["status"]["conditions"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|c| c["type"] == "Ready" && c["status"] == "True")
    }

    /// A trigger whose checkpoint failed reports `Triggered: "False"` (e.g.
    /// reason `Failed`, "context deadline exceeded"). Spent: it never fires
    /// again.
    fn trigger_failed(trigger: &DynamicObject) -> bool {
        trigger.data["status"]["conditions"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|c| c["type"] == "Triggered" && c["status"] == "False")
    }

    fn older_than_give_up(obj: &DynamicObject) -> bool {
        obj.creation_timestamp().is_some_and(|created| {
            k8s_openapi::jiff::Timestamp::now().as_second() - created.0.as_second()
                >= SNAPSHOT_GIVE_UP.as_secs() as i64
        })
    }

    /// Most recent `Ready` snapshot labeled with the session, if any.
    async fn latest_ready_snapshot(&self, session_id: &str) -> Result<Option<String>> {
        let selector = format!("{LABEL_SESSION_ID}={session_id}");
        let mut ready: Vec<DynamicObject> = self
            .pod_snapshots()
            .list(&ListParams::default().labels(&selector))
            .await?
            .items
            .into_iter()
            .filter(Self::is_ready)
            .collect();
        ready.sort_by_key(|s| s.creation_timestamp());
        Ok(ready.pop().map(|s| s.name_any()))
    }

    async fn delete_labeled(&self, api: &Api<DynamicObject>, session_id: &str) -> Result<()> {
        let selector = format!("{LABEL_SESSION_ID}={session_id}");
        for obj in api.list(&ListParams::default().labels(&selector)).await? {
            match api.delete(&obj.name_any(), &DeleteParams::default()).await {
                Ok(_) => {}
                Err(kube::Error::Api(e)) if e.code == 404 => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }
}

#[async_trait]
impl SnapshotProvider for GkeSnapshotProvider {
    fn name(&self) -> &'static str {
        "gke-pod-snapshot"
    }

    /// Apply the pool's `PodSnapshotPolicy` and, when the session already has
    /// a `Ready` snapshot, pin it onto the pod via the
    /// `podsnapshot.gke.io/ps-name` annotation. Pinning (rather than relying
    /// on latest-in-group matching) makes the expected restore explicit so
    /// [`Self::verify_restore`] can tell a cold start from success. No
    /// per-session state volume is needed — the checkpoint carries the
    /// filesystem.
    async fn prepare(&self, session_id: &str) -> Result<PreparedSession> {
        let storage_config = self
            .pool
            .spec
            .resume
            .gke_storage_config_name
            .as_ref()
            .ok_or_else(|| {
                Error::Config(
                    "resume.gkeStorageConfigName is required for the GkeSnapshot policy"
                        .to_string(),
                )
            })?;

        let mut spec = serde_json::json!({
            "storageConfigName": storage_config,
            "selector": {
                "matchLabels": {
                    LABEL_MANAGED_BY: crate::MANAGER_NAME,
                    LABEL_POOL: self.pool.name_any(),
                }
            },
            // "stop" ends the pod once the checkpoint is captured; the
            // operator deletes it right after anyway, and "resume" would let
            // the workload keep mutating state past the snapshot.
            "triggerConfig": {"type": "manual", "postCheckpoint": "stop"},
            "snapshotGroupingRules": {
                "groupByLabelValue": {
                    "labels": [LABEL_SESSION_ID],
                    "groupRetentionPolicy": {"maxSnapshotCountPerGroup": 1}
                }
            }
        });
        if let Some(ttl) = self.pool.spec.resume.snapshot_ttl_seconds {
            spec["retentionConfig"] =
                serde_json::json!({"lastAccessTimeout": format!("{}min", ttl.div_ceil(60))});
        }

        let gvk = GroupVersionKind::gvk(
            GKE_SNAPSHOT_GROUP,
            GKE_SNAPSHOT_VERSION,
            "PodSnapshotPolicy",
        );
        let resource = ApiResource::from_gvk_with_plural(&gvk, "podsnapshotpolicies");
        let mut policy = DynamicObject::new(&self.policy_name(), &resource)
            .data(serde_json::json!({"spec": spec}));
        policy.metadata.namespace = Some(self.namespace().to_string());
        policy.metadata.owner_references = self.pool.controller_owner_ref(&()).map(|r| vec![r]);

        self.policies()
            .patch(
                &self.policy_name(),
                &PatchParams::apply(crate::MANAGER_NAME).force(),
                &Patch::Apply(&policy),
            )
            .await?;

        let mut prepared = PreparedSession::default();
        if let Some(snapshot) = self.latest_ready_snapshot(session_id).await? {
            prepared
                .pod_annotations
                .insert(ANNOTATION_PIN_SNAPSHOT.to_string(), snapshot);
        }
        Ok(prepared)
    }

    /// The pod-snapshot agent is a DaemonSet that races workload pods onto
    /// freshly scaled-up nodes; a pod that starts before the agent silently
    /// cold-starts (no restore is attempted, and the platform offers no
    /// barrier — Google's own agent-sandbox client verifies restores the
    /// same way). The reconciler recycles such pods once; by then the node's
    /// agent is up and the retry restores.
    fn verify_restore(&self, pod: &Pod) -> RestoreVerdict {
        let Some(pinned) = pod.annotations().get(ANNOTATION_PIN_SNAPSHOT) else {
            return RestoreVerdict::NotApplicable;
        };
        let restored = pod
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .into_iter()
            .flatten()
            .any(|c| {
                c.type_ == CONDITION_POD_RESTORED
                    && c.status == "True"
                    && c.message.as_deref().is_some_and(|m| m.contains(pinned))
            });
        if restored {
            RestoreVerdict::Restored
        } else {
            RestoreVerdict::ColdStarted
        }
    }

    async fn on_suspend(&self, session_id: &str, pod: &Pod) -> Result<SnapshotOutcome> {
        // A ready snapshot for this session (from this or an earlier pod
        // incarnation) means the state is already durable. Checking first
        // keeps retried teardowns and recreated pods from firing spurious
        // triggers — with `maxSnapshotCountPerGroup: 1`, a new snapshot
        // attempt would prune the good one.
        if self.latest_ready_snapshot(session_id).await?.is_some() {
            return Ok(SnapshotOutcome::Ready);
        }

        let pod_running = pod
            .status
            .as_ref()
            .and_then(|s| s.phase.as_deref())
            .is_some_and(|phase| phase == "Running")
            && pod.meta().deletion_timestamp.is_none();
        let trigger_name = Self::trigger_name(session_id, pod);
        let triggers = self.triggers();

        let trigger = match triggers.get_opt(&trigger_name).await? {
            Some(trigger) => trigger,
            None if pod_running => {
                let gvk = GroupVersionKind::gvk(
                    GKE_SNAPSHOT_GROUP,
                    GKE_SNAPSHOT_VERSION,
                    "PodSnapshotManualTrigger",
                );
                let resource = ApiResource::from_gvk_with_plural(&gvk, "podsnapshotmanualtriggers");
                let mut trigger = DynamicObject::new(&trigger_name, &resource)
                    .data(serde_json::json!({"spec": {"targetPod": pod.name_any()}}));
                trigger.metadata.namespace = Some(self.namespace().to_string());
                trigger.metadata.labels = Some(session_labels(&self.pool, session_id));
                trigger.metadata.owner_references =
                    self.pool.controller_owner_ref(&()).map(|r| vec![r]);
                triggers.create(&PostParams::default(), &trigger).await?;
                return Ok(SnapshotOutcome::InProgress);
            }
            None => {
                // Nothing to checkpoint: the worker already stopped or is
                // being deleted. Proceeding without a snapshot beats holding
                // the suspended session's claim hostage; the resume starts
                // fresh (the brain owns resume semantics regardless).
                tracing::warn!(
                    session = session_id,
                    pod = %pod.name_any(),
                    "worker pod is not running; suspending without a snapshot"
                );
                return Ok(SnapshotOutcome::Ready);
            }
        };

        let snapshot_name = trigger.data["status"]["snapshotCreated"]["name"]
            .as_str()
            .map(str::to_string);
        let snapshot = match &snapshot_name {
            Some(name) => self.pod_snapshots().get_opt(name).await?,
            None => None,
        };

        if let Some(snapshot) = &snapshot
            && Self::is_ready(snapshot)
        {
            // Label the snapshot with the session so on_terminate can find
            // and delete it (the grouping label is only applied by GKE when
            // grouping is configured).
            let patch = serde_json::json!({
                "metadata": {"labels": session_labels(&self.pool, session_id)}
            });
            self.pod_snapshots()
                .patch(
                    &snapshot.name_any(),
                    &PatchParams::default(),
                    &Patch::Merge(&patch),
                )
                .await?;
            return Ok(SnapshotOutcome::Ready);
        }

        if Self::trigger_failed(&trigger) {
            if pod_running {
                // Retry: the failure may have been transient (the trigger
                // object is spent, so a fresh one is needed).
                tracing::warn!(
                    session = session_id,
                    trigger = %trigger_name,
                    "snapshot trigger failed; retrying"
                );
                match triggers
                    .delete(&trigger_name, &DeleteParams::default())
                    .await
                {
                    Ok(_) => {}
                    Err(kube::Error::Api(e)) if e.code == 404 => {}
                    Err(e) => return Err(e.into()),
                }
                return Ok(SnapshotOutcome::InProgress);
            }
            tracing::warn!(
                session = session_id,
                trigger = %trigger_name,
                "snapshot trigger failed and the worker pod is gone; suspending without a snapshot"
            );
            return Ok(SnapshotOutcome::Ready);
        }

        if Self::older_than_give_up(&trigger) {
            tracing::warn!(
                session = session_id,
                trigger = %trigger_name,
                snapshot = snapshot_name.as_deref(),
                "GKE pod snapshot not ready after {SNAPSHOT_GIVE_UP:?}; abandoning it"
            );
            return Ok(SnapshotOutcome::Ready);
        }
        Ok(SnapshotOutcome::InProgress)
    }

    /// Delete the session's triggers and (labeled) snapshots. Snapshots that
    /// were never labeled (e.g. operator crashed mid-suspend) fall back to
    /// the policy's retention config.
    async fn on_terminate(&self, session_id: &str) -> Result<()> {
        self.delete_labeled(&self.triggers(), session_id).await?;
        self.delete_labeled(&self.pod_snapshots(), session_id).await
    }

    /// TTL enforcement is server-side (`retentionConfig.lastAccessTimeout`,
    /// set in [`Self::prepare`] from `resume.snapshotTtlSeconds`).
    async fn gc(&self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use k8s_openapi::api::core::v1::{PodCondition, PodStatus};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    use crate::crd::{OutpostPoolSpec, SecretKeyRef};

    use super::*;

    fn provider() -> GkeSnapshotProvider {
        // The kube client is never dialed by the pure methods under test,
        // but constructing it initializes rustls.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let config = kube::Config::new("http://localhost:1".parse().unwrap());
        let pool = OutpostPool::new(
            "p",
            OutpostPoolSpec {
                pool_id: "pool_x".to_string(),
                token_secret_ref: SecretKeyRef {
                    name: "t".to_string(),
                    key: "token".to_string(),
                },
                api_url: None,
                max_concurrent_sessions: 1,
                worker: crate::crd::WorkerTemplate {
                    template: Default::default(),
                    container_name: "devin-worker".to_string(),
                    overrides: Default::default(),
                },
                resume: Default::default(),
            },
        );
        GkeSnapshotProvider::new(
            kube::Client::try_from(config).unwrap(),
            std::sync::Arc::new(pool),
        )
    }

    fn pod(pin: Option<&str>, condition: Option<(&str, &str, &str)>) -> Pod {
        Pod {
            metadata: ObjectMeta {
                annotations: pin.map(|p| {
                    std::collections::BTreeMap::from([(
                        ANNOTATION_PIN_SNAPSHOT.to_string(),
                        p.to_string(),
                    )])
                }),
                ..Default::default()
            },
            status: Some(PodStatus {
                conditions: condition.map(|(type_, status, message)| {
                    vec![PodCondition {
                        type_: type_.to_string(),
                        status: status.to_string(),
                        message: Some(message.to_string()),
                        ..Default::default()
                    }]
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    // tokio: constructing the (never-dialed) kube client spawns its buffer task.
    #[tokio::test]
    async fn verify_restore_judges_the_pod_restored_condition() {
        let provider = provider();
        // Unpinned pods expect no restore.
        assert_eq!(
            provider.verify_restore(&pod(None, None)),
            RestoreVerdict::NotApplicable
        );
        // Pinned + PodRestored=True naming the snapshot => restored.
        assert_eq!(
            provider.verify_restore(&pod(
                Some("snap-1"),
                Some((CONDITION_POD_RESTORED, "True", "restored from snap-1"))
            )),
            RestoreVerdict::Restored
        );
        // Pinned but no condition, a failed restore, or the wrong snapshot
        // => cold start.
        assert_eq!(
            provider.verify_restore(&pod(Some("snap-1"), None)),
            RestoreVerdict::ColdStarted
        );
        assert_eq!(
            provider.verify_restore(&pod(
                Some("snap-1"),
                Some((CONDITION_POD_RESTORED, "False", ""))
            )),
            RestoreVerdict::ColdStarted
        );
        assert_eq!(
            provider.verify_restore(&pod(
                Some("snap-1"),
                Some((CONDITION_POD_RESTORED, "True", "restored from snap-2"))
            )),
            RestoreVerdict::ColdStarted
        );
    }
}
