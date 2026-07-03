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
//!   grouped by the session-ID label (max one snapshot per session).
//!   Restores need no operator action beyond recreating the pod: GKE matches
//!   the new pod's distilled spec + snapshot group and restores the latest
//!   snapshot automatically. Env *values* (like the rotated connect token)
//!   are not part of the distilled spec, so re-claimed tokens don't break
//!   matching.
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

use super::{SnapshotOutcome, SnapshotProvider};

/// API group of the GKE pod-snapshot CRDs.
const GKE_SNAPSHOT_GROUP: &str = "podsnapshot.gke.io";
/// API version of the GKE pod-snapshot CRDs.
const GKE_SNAPSHOT_VERSION: &str = "v1";

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

    /// Apply the pool's `PodSnapshotPolicy`. No per-session state volume is
    /// needed — the checkpoint carries the filesystem.
    async fn prepare(&self, _session_id: &str) -> Result<Option<String>> {
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
        Ok(None)
    }

    async fn on_suspend(&self, session_id: &str, pod: &Pod) -> Result<SnapshotOutcome> {
        // A ready snapshot for this session (from this or an earlier pod
        // incarnation) means the state is already durable. Checking first
        // keeps retried teardowns and recreated pods from firing spurious
        // triggers — with `maxSnapshotCountPerGroup: 1`, a new snapshot
        // attempt would prune the good one.
        let selector = format!("{LABEL_SESSION_ID}={session_id}");
        if self
            .pod_snapshots()
            .list(&ListParams::default().labels(&selector))
            .await?
            .iter()
            .any(Self::is_ready)
        {
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
