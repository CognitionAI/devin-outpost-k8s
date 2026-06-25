//! Wire models for the `/opbeta/outposts` API.
//!
//! These mirror the Pydantic response models in
//! `devin-webapp/apps/webserver/app/routers/opbeta/outposts_router.py` and the
//! reference worker's deserialization structs. Unknown enum variants degrade to
//! `Unknown` so that a server-side addition never breaks the operator.

use serde::{Deserialize, Serialize};

/// Queue phase of a session as tracked by the control plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Phase {
    /// Waiting in the queue, available to be claimed.
    Pending,
    /// Currently claimed by some worker.
    Claimed,
    /// A variant the server added that this client does not know about.
    #[serde(other)]
    Unknown,
}

/// Whether a queued item is a brand new session or a resume of an existing one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    /// A fresh session; start the worker from scratch.
    New,
    /// Resume a previously-suspended session (see [`crate::snapshot`]).
    Resume,
    /// A variant the server added that this client does not know about.
    #[serde(other)]
    Unknown,
}

/// Coarse status of the underlying Devin session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SessionStatus {
    /// Not yet started.
    Pending,
    /// Actively running.
    Running,
    /// Suspended; a resume may be enqueued later.
    Suspended,
    /// Finished; the worker pod can be torn down.
    Terminated,
    /// A variant the server added that this client does not know about.
    #[serde(other)]
    Unknown,
}

/// Identifying metadata for a queued session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    /// The session (devin) ID.
    pub session_id: String,
    /// The outpost pool the session is queued on.
    pub pool_id: String,
    /// When the session was enqueued (unix seconds).
    #[serde(default)]
    pub created_at: Option<f64>,
    /// When this object last changed (unix seconds).
    #[serde(default)]
    pub updated_at: Option<f64>,
}

/// Desired state of a queued session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Spec {
    /// Whether this is a new session or a resume.
    pub kind: Kind,
    /// Machine platform, e.g. `"linux"`.
    pub platform: String,
    /// Short commit SHA of the `devin-remote` binary the worker should run;
    /// the worker's default is used when `None`.
    #[serde(default)]
    pub remote_binary_sha: Option<String>,
}

/// Observed state of a queued session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Status {
    /// Queue phase of the session.
    pub phase: Phase,
    /// Worker that currently holds the claim, if claimed.
    #[serde(default)]
    pub acceptor_id: Option<String>,
    /// When the current claim expires and the session returns to the queue
    /// (unix seconds).
    #[serde(default)]
    pub claim_deadline: Option<f64>,
    /// Coarse status of the underlying Devin session.
    pub session_status: SessionStatus,
    /// Gateway connect token for the claimed session; only returned from a
    /// successful claim.
    #[serde(default)]
    pub connect_token: Option<String>,
    /// Public websocket URL of the outpost gateway; only returned from a
    /// successful claim.
    #[serde(default)]
    pub gateway_url: Option<String>,
}

/// A single queued Devin session — the unit the operator reconciles.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutpostDevin {
    /// Identifying metadata.
    pub metadata: Metadata,
    /// Desired state.
    pub spec: Spec,
    /// Observed state.
    pub status: Status,
}

/// A page of [`OutpostDevin`] items plus a resume cursor for the next page /
/// watch position.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DevinList {
    /// The items in this page.
    #[serde(default)]
    pub items: Vec<OutpostDevin>,
    /// Opaque cursor to resume listing/watching from.
    #[serde(default)]
    pub cursor: Option<String>,
}

/// The kind of change carried by a watch event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum WatchEventKind {
    /// Item added or updated.
    Modified,
    /// Item removed from the queue.
    Deleted,
    /// A variant the server added that this client does not know about.
    #[serde(other)]
    Unknown,
}

/// A single server-sent watch event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchEvent {
    /// What happened to the object.
    #[serde(rename = "type")]
    pub kind: WatchEventKind,
    /// The affected object.
    pub object: OutpostDevin,
    /// Cursor to resume from after this event.
    #[serde(default)]
    pub cursor: Option<String>,
}

/// Result of a successful claim: everything the worker needs to dial back to the
/// gateway.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimResponse {
    /// The (renewed) status, including `connect_token` and `gateway_url`.
    pub status: Status,
}

/// An outpost pool (account-scoped queue) as returned by the pools endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pool {
    /// Stable pool identifier.
    pub pool_id: String,
    /// Human-readable name, if set.
    #[serde(default)]
    pub name: Option<String>,
}
