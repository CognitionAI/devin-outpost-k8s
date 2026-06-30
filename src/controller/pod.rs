//! Per-session worker `Pod` template builder (scaffold).
//!
//! For each claimed session the operator creates one owned `Pod` running the
//! `devin` worker image. The worker spawns `devin-remote` in *connect-back*
//! mode: it dials the outpost gateway's public leg using the short-lived connect
//! token from the claim, and the brain attaches over the gateway's internal leg.
//!
//! The container env the worker/remote expects mirrors the reference worker in
//! `devin-webapp/apps/chisel/chisel/src/worker/runner.rs`. The relevant values
//! (gateway URL, connect token, session id) come from the claim response, not
//! from static config — so this builder takes them as inputs.

use k8s_openapi::api::core::v1::Pod;

use crate::crd::OutpostPool;
use crate::error::{Error, Result};
use crate::api::OutpostDevin;

/// Default worker image used when a pool's `worker.image` is unset.
///
/// PLACEHOLDER until the lightweight `devin` CLI/worker image is published; the
/// operator deployment should pin this (via config) to the image matching its
/// own release. See `docs/ARCHITECTURE.md`.
pub const DEFAULT_WORKER_IMAGE: &str = "ghcr.io/usacognition/devin-worker:latest";

/// Env var carrying the outpost gateway public websocket URL.
pub const ENV_GATEWAY_URL: &str = "DEVIN_OUTPOST_GATEWAY_URL";
/// Env var carrying the short-lived gateway connect token.
pub const ENV_CONNECT_TOKEN: &str = "DEVIN_OUTPOST_CONNECT_TOKEN";
/// Env var carrying the session (devin) ID.
pub const ENV_SESSION_ID: &str = "DEVIN_OUTPOST_SESSION_ID";

/// Inputs needed to render a worker pod for one claimed session.
#[derive(Debug, Clone)]
pub struct WorkerPodParams<'a> {
    /// The owning pool (provides the pod template + ownership reference).
    pub pool: &'a OutpostPool,
    /// The claimed session this pod will serve.
    pub session: &'a OutpostDevin,
    /// Gateway public URL returned by the claim.
    pub gateway_url: &'a str,
    /// Connect token returned by the claim.
    pub connect_token: &'a str,
    /// Operator-wide default worker image, used when the pool's `worker.image`
    /// is unset (see [`DEFAULT_WORKER_IMAGE`] / [`crate::config::OperatorConfig`]).
    pub default_image: &'a str,
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
///    `image` to `default_image`, set the worker command/args, and inject the
///    env contract above (`ENV_GATEWAY_URL`/`ENV_CONNECT_TOKEN`/`ENV_SESSION_ID`
///    from the claim). Set `restartPolicy = Never`, and attach a deterministic
///    pod name, identifying labels/annotations, and an owner reference to the
///    pool for GC.
/// 3. **Overrides:** apply `worker.overrides`. Each `Some` field wins over the
///    layer-2 value: `image`, `command`, `args`, `restart_policy`, and the pod
///    `labels`/`annotations` (which *replace* the operator's merged set). The
///    pod name and owner reference stay operator-owned and are never overridden.
pub fn build_worker_pod(_params: WorkerPodParams<'_>) -> Result<Pod> {
    Err(Error::todo("controller::build_worker_pod"))
}
