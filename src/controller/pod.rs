//! Per-session worker `Pod` template builder (scaffold).
//!
//! For each claimed session the operator creates one owned `Pod` running the
//! `devin` CLI worker. The worker dials the outpost gateway's public leg using
//! the connect token from the claim, and the brain attaches over the gateway's
//! internal leg.
//!
//! ## Operator ↔ worker-image contract
//!
//! This module is the single source of truth for the contract between the
//! operator and the worker image; the image (a lightweight Dockerfile bundling
//! the `devin` CLI, published from the CLI's regular release flow) must stay
//! in sync with it.
//!
//! - The operator runs [`WORKER_COMMAND`] with args
//!   `["worker", "--session", <session_id>, "--once"]` — `--once` means the
//!   process serves exactly one session and exits `0` when it completes.
//! - [`ENV_SESSION_TOKEN`] carries the gateway connect token, injected via
//!   `secretKeyRef` from a per-session `Secret` (see
//!   [`build_session_token_secret`]) rather than inline in the pod spec, so it
//!   isn't readable by everyone with `pods get`.
//! - [`ENV_GATEWAY_URL`] carries the gateway public websocket URL from the
//!   claim.
//! - [`ENV_REMOTE_BINARY_SHA`] carries `spec.remote_binary_sha` when the queue
//!   item pins one; unset otherwise.

use k8s_openapi::api::core::v1::{Pod, Secret};

use crate::api::OutpostDevin;
use crate::crd::OutpostPool;
use crate::error::{Error, Result};

/// Default worker image used when a pool's `worker.overrides.image` is unset.
///
/// CR-soon nikhil: "there's probably a better tag to use later"; the operator
/// deployment should eventually pin this (via config) to the image matching its
/// own release.
pub const DEFAULT_WORKER_IMAGE: &str = "public.ecr.aws/e0h8a4b6/devin-cli:3000.1.1016";

/// Entrypoint the operator sets on the worker container. Kept separate from
/// the args so `worker.overrides.args` can tweak flags without repeating the
/// binary (and `worker.overrides.command` can swap the binary itself).
pub const WORKER_COMMAND: &str = "devin";

/// Env var carrying the outpost gateway public websocket URL.
pub const ENV_GATEWAY_URL: &str = "DEVIN_REMOTE_GATEWAY_URL";
/// Env var carrying the gateway connect token (via the per-session `Secret`).
pub const ENV_SESSION_TOKEN: &str = "DEVIN_REMOTE_SESSION_TOKEN";
/// Env var carrying `spec.remote_binary_sha`, when set.
pub const ENV_REMOTE_BINARY_SHA: &str = "DEVIN_REMOTE_BINARY_SHA";

/// Key under which the connect token is stored in the per-session `Secret`.
pub const SESSION_TOKEN_SECRET_KEY: &str = "token";

/// Inputs needed to render a worker pod for one claimed session.
#[derive(Debug, Clone)]
pub struct WorkerPodParams<'a> {
    /// The owning pool (provides the pod template + ownership reference).
    pub pool: &'a OutpostPool,
    /// The claimed session this pod will serve.
    pub session: &'a OutpostDevin,
    /// Gateway public URL returned by the claim.
    pub gateway_url: &'a str,
    /// Name of the per-session `Secret` (from [`build_session_token_secret`])
    /// holding the connect token under [`SESSION_TOKEN_SECRET_KEY`].
    pub token_secret_name: &'a str,
    /// Operator-wide default worker image, used when the pool's
    /// `worker.overrides.image` is unset (see [`DEFAULT_WORKER_IMAGE`] /
    /// [`crate::config::OperatorConfig`]).
    pub default_image: &'a str,
}

/// Build the per-session `Secret` holding the gateway connect token under
/// [`SESSION_TOKEN_SECRET_KEY`].
///
/// Labeled with the session ID and owner-referenced to the pool; the operator
/// deletes it together with the worker pod.
pub fn build_session_token_secret(
    _pool: &OutpostPool,
    _session: &OutpostDevin,
    _connect_token: &str,
) -> Result<Secret> {
    Err(Error::todo("controller::build_session_token_secret"))
}

/// Build the worker `Pod` for one claimed session.
///
/// The pool's [`crate::crd::WorkerTemplate`] is assembled in three layers (see
/// its docs). The intended algorithm:
///
/// 1. **Base:** start from `worker.template`
///    ([`k8s_openapi::api::core::v1::PodTemplateSpec`]) — carry over its `spec`,
///    `nodeSelector`, `tolerations`, `runtimeClassName`, `serviceAccountName`,
///    volumes, sidecars, pod metadata, etc.
/// 2. **Operator vars:** find the container named `worker.container_name`
///    (default `devin-worker`), inserting an empty one if absent. Set its
///    `image` to `default_image`, its command/args and env per the module-level
///    contract. Set `restartPolicy = OnFailure` (see the lifecycle mapping in
///    [`crate::controller`]), and attach a deterministic pod name, identifying
///    labels/annotations, and an owner reference to the pool for GC.
/// 3. **Overrides:** apply `worker.overrides`. Each `Some` field wins over the
///    layer-2 value: `image`, `command`, `args`, `restart_policy`, and the pod
///    `labels`/`annotations` (which *replace* the operator's merged set). The
///    pod name and owner reference stay operator-owned and are never overridden.
pub fn build_worker_pod(_params: WorkerPodParams<'_>) -> Result<Pod> {
    Err(Error::todo("controller::build_worker_pod"))
}
