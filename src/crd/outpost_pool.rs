//! The [`OutpostPool`] custom resource.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{LocalObjectReference, ResourceRequirements, Toleration};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// `OutpostPool` binds one account-scoped Devin Outposts queue (identified by
/// `poolId` + an account PAT) to a worker `Pod` template. The operator watches
/// the queue for this pool, claims pending sessions, and runs the `devin` worker
/// for each claimed session using the settings below.
///
/// One operator deployment reconciles many `OutpostPool` resources.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "outposts.cognition.ai",
    version = "v1alpha1",
    kind = "OutpostPool",
    plural = "outpostpools",
    shortname = "opool",
    namespaced,
    status = "OutpostPoolStatus",
    printcolumn = r#"{"name":"Pool","type":"string","jsonPath":".spec.poolId"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Claimed","type":"integer","jsonPath":".status.claimedSessions"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct OutpostPoolSpec {
    /// The upstream Outposts pool ID this resource serves.
    pub pool_id: String,

    /// Reference to the `Secret` holding the account PAT (with the
    /// `UseOutpostsMachine` permission) used to authenticate to the queue API.
    pub token_secret_ref: SecretKeyRef,

    /// Override for the upstream API base URL. Defaults to the operator's
    /// configured default when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_url: Option<String>,

    /// Maximum number of sessions this pool will run concurrently. Pending
    /// sessions beyond this are left in the queue for other workers.
    #[serde(default = "default_max_concurrent_sessions")]
    pub max_concurrent_sessions: u32,

    /// The worker `Pod` template applied to every claimed session.
    pub worker: WorkerTemplate,

    /// How `resume`-kind sessions are handled for this pool.
    #[serde(default)]
    pub resume_policy: ResumePolicy,

    /// Snapshot policy used to back resumes / idle cost savings. Disabled by
    /// default.
    #[serde(default)]
    pub snapshot: SnapshotPolicy,
}

/// A reference to a single key within a `Secret` in the same namespace.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SecretKeyRef {
    /// Name of the `Secret`.
    pub name: String,
    /// Key within the `Secret`'s data holding the token.
    #[serde(default = "default_token_key")]
    pub key: String,
}

/// The per-session worker `Pod` template.
///
/// These fields shape the `Pod` the operator creates for each claimed session.
/// Cloud-specific knobs (GKE Autopilot annotations/labels, runtime class for
/// gVisor/Kata) live here so a single operator can serve heterogeneous pools.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkerTemplate {
    /// Container image bundling the `devin` CLI / worker.
    pub image: String,

    /// Image pull policy (`Always` | `IfNotPresent` | `Never`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_pull_policy: Option<String>,

    /// Image pull secrets for private registries.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub image_pull_secrets: Vec<LocalObjectReference>,

    /// CPU/memory (and other) resource requests + limits for the worker
    /// container. Global per pool for now.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirements>,

    /// Node selector applied to worker pods.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub node_selector: BTreeMap<String, String>,

    /// Tolerations applied to worker pods.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tolerations: Vec<Toleration>,

    /// RuntimeClass name for sandboxing (e.g. `gvisor`, `kata`). When unset the
    /// cluster default runtime is used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_class_name: Option<String>,

    /// ServiceAccount for worker pods. Defaults to the namespace default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_account_name: Option<String>,

    /// Extra annotations merged onto every worker pod. Useful for GKE Autopilot
    /// (e.g. compute-class / scheduling hints) and similar cloud knobs.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub annotations: BTreeMap<String, String>,

    /// Extra labels merged onto every worker pod.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
}

/// Strategy for serving `resume`-kind sessions.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq, JsonSchema)]
pub enum ResumePolicy {
    /// Ignore prior state and start the worker fresh. The simplest, fully
    /// portable option, and the default.
    #[default]
    StartFresh,
    /// Restore the pod from a GKE pod snapshot taken on suspend. GKE only.
    GkeSnapshot,
    /// Restore session state from a persistent filesystem snapshot (e.g. a
    /// retained `PersistentVolume`). Portable across providers.
    FilesystemSnapshot,
}

/// Snapshot policy controlling whether/when worker state is snapshotted.
#[derive(Serialize, Deserialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotPolicy {
    /// Whether snapshotting is enabled at all. Defaults to `false`.
    #[serde(default)]
    pub enabled: bool,

    /// How long a snapshot is retained before being garbage collected, in
    /// seconds. `None` => keep until the session is terminated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_seconds: Option<u64>,
}

/// Observed state of an [`OutpostPool`].
#[derive(Serialize, Deserialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OutpostPoolStatus {
    /// High-level reconcile phase (e.g. `Ready`, `Degraded`, `Unauthorized`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,

    /// Number of sessions currently claimed and running for this pool.
    #[serde(default)]
    pub claimed_sessions: u32,

    /// Last time the operator successfully synced this pool's queue (RFC3339).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_synced: Option<String>,

    /// Opaque cursor for resuming the queue watch after a restart.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watch_cursor: Option<String>,

    /// Standard Kubernetes-style conditions.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// A minimal Kubernetes-style status condition.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Condition {
    /// Condition type, e.g. `Ready`.
    pub type_: String,
    /// `True` | `False` | `Unknown`.
    pub status: String,
    /// Machine-readable reason.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Human-readable message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// When the condition last transitioned (RFC3339).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<String>,
}

fn default_max_concurrent_sessions() -> u32 {
    10
}

fn default_token_key() -> String {
    "token".to_string()
}
