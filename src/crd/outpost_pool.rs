//! The [`OutpostPool`] custom resource.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::PodTemplateSpec;
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
    group = "outposts.cognition.com",
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

    /// How `resume`-kind sessions are served for this pool.
    #[serde(default)]
    pub resume: ResumeConfig,
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
/// The pod is assembled in three layers, applied in order:
///
/// 1. **`template`** — your base [`PodTemplateSpec`]. Put anything
///    pod/container-level you need here (resources, env, volumes,
///    `nodeSelector`, `tolerations`, `runtimeClassName`, `serviceAccountName`,
///    `securityContext`, `affinity`, sidecars, pod metadata, …). A single
///    operator can serve heterogeneous pools because every cloud-specific knob
///    is expressible here.
/// 2. **operator vars** — the operator merges the bits it must own onto the
///    worker container (`containerName`): the worker image + command, the
///    per-claim `DEVIN_OUTPOST_*` env, a non-restarting pod policy, and
///    identifying pod metadata + an owner reference for GC.
/// 3. **`overrides`** — your final say. Each `Some` field in [`WorkerOverrides`]
///    wins over the operator var from layer 2, so a pool can pin the image, swap
///    the entrypoint, or stop the operator's labels/annotations from being
///    applied. The pod name and owner reference are always operator-owned and
///    cannot be overridden.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkerTemplate {
    /// Layer 1: the base pod template the operator overlays the worker container
    /// onto.
    #[serde(default)]
    pub template: PodTemplateSpec,

    /// Name of the container in `template` the operator treats as the worker: it
    /// merges its image/command/env onto this container, creating it if the
    /// template doesn't define one by this name. Other containers in the
    /// template are left as-is and run as sidecars.
    #[serde(default = "default_worker_container_name")]
    pub container_name: String,

    /// Layer 3: final-say overrides over the fields the operator otherwise owns.
    #[serde(default)]
    pub overrides: WorkerOverrides,
}

/// Final-say overrides over the fields the operator otherwise owns (layer 3 in
/// [`WorkerTemplate`]).
///
/// Every field is optional: a `Some` value takes precedence over the operator's
/// own value for that field, while `None` leaves the operator in control. This
/// keeps the set of operator-owned knobs explicit and lets a pool opt out of any
/// one of them individually instead of accepting all of them wholesale.
#[derive(Serialize, Deserialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkerOverrides {
    /// Worker container image. When unset the operator uses its configured
    /// default worker image (pinned to the operator release).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,

    /// Entrypoint for the worker container, replacing the operator's default
    /// worker command.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<Vec<String>>,

    /// Args for the worker container, replacing the operator's default args.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args: Option<Vec<String>>,

    /// Pod `restartPolicy`. The operator defaults to `OnFailure` (see
    /// [`crate::controller`] for the lifecycle mapping); override only if you
    /// understand the implications — the operator deletes pods as sessions
    /// terminate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restart_policy: Option<String>,

    /// Pod labels. When set, these *replace* the labels the operator would
    /// otherwise merge (use `{}` to apply none of them). The owner reference is
    /// always added regardless, so garbage collection still works.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub labels: Option<BTreeMap<String, String>>,

    /// Pod annotations. When set, these *replace* the annotations the operator
    /// would otherwise merge (use `{}` to apply none of them).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub annotations: Option<BTreeMap<String, String>>,
}

/// How `resume`-kind sessions are served, and the snapshotting that backs
/// them.
///
/// A single block (rather than separate policy + `snapshot.enabled` knobs) so
/// that contradictory configurations — a snapshotting policy with snapshots
/// disabled — are unrepresentable. The [`ResumePolicy`] alone decides whether
/// snapshots are taken.
#[derive(Serialize, Deserialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResumeConfig {
    /// Strategy for serving resume-kind sessions.
    #[serde(default)]
    pub policy: ResumePolicy,

    /// How long a snapshot is retained before being garbage collected, in
    /// seconds. `None` => keep until the session is terminated. Only
    /// meaningful for policies that snapshot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_ttl_seconds: Option<u64>,
}

/// Strategy for serving `resume`-kind sessions. Policies other than
/// [`ResumePolicy::StartFresh`] snapshot the worker when its session is
/// suspended and restore from that snapshot on resume (see
/// [`crate::snapshot`]).
#[derive(Serialize, Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq, JsonSchema)]
pub enum ResumePolicy {
    /// Ignore prior state and start the worker fresh. The simplest, fully
    /// portable option, and the default. Resumes still function, but the prior
    /// filesystem state is gone.
    #[default]
    StartFresh,
    /// Restore the pod from a GKE pod snapshot taken on suspend. GKE only.
    GkeSnapshot,
    /// Restore session state from a persistent filesystem snapshot (e.g. a
    /// retained `PersistentVolume`). Portable across providers.
    FilesystemSnapshot,
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

fn default_worker_container_name() -> String {
    "devin-worker".to_string()
}

#[cfg(test)]
mod tests {
    use super::OutpostPool;
    use kube::Resource;

    /// The `#[kube(group = ..., version = ...)]` derive attributes must be
    /// string literals, so they can't reference [`crate::API_GROUP`] /
    /// [`crate::API_VERSION`] directly. Assert they stay in sync instead.
    #[test]
    fn crd_identity_matches_crate_constants() {
        assert_eq!(OutpostPool::group(&()), crate::API_GROUP);
        assert_eq!(OutpostPool::version(&()), crate::API_VERSION);
    }
}
