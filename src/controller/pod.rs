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
use crate::opbeta::OutpostDevin;

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
}

/// Build the worker `Pod` for one claimed session.
///
/// TODO: assemble metadata (name/labels/annotations, owner reference to the
/// pool), the worker container (image, resources, env contract above), and
/// scheduling (`nodeSelector`, `tolerations`, `runtimeClassName`,
/// `serviceAccountName`, GKE Autopilot annotations) from
/// [`crate::crd::WorkerTemplate`].
pub fn build_worker_pod(_params: WorkerPodParams<'_>) -> Result<Pod> {
    Err(Error::todo("controller::build_worker_pod"))
}
