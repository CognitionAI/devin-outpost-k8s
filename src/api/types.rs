//! Wire models for the `outposts` API.
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

/// The human who started a queued session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Requester {
    /// Stable ID of the user.
    pub user_id: String,
    /// Email of the user; `None` when their account has none.
    #[serde(default)]
    pub email: Option<String>,
}

/// Identifying metadata for a queued session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    /// The session (devin) ID.
    pub session_id: String,
    /// The outpost the session is queued on.
    pub outpost_id: String,
    /// When the session was enqueued (unix seconds).
    #[serde(default)]
    pub created_at: Option<i64>,
    /// When this object last changed (unix seconds).
    #[serde(default)]
    pub updated_at: Option<i64>,
    /// The human who started the session, so a worker can run it as that
    /// developer. `None` when no human did (automations, service-user API
    /// keys, chat senders without a linked Devin account) and when the
    /// server predates the field.
    #[serde(default)]
    pub requester: Option<Requester>,
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
    /// The session's effective network policy (see [`crate::network_policy`]
    /// for how it is enforced).
    #[serde(default)]
    pub network_policy: NetworkPolicy,
    /// Container image, by digest, the worker must run for this session: the
    /// DRS build of the session's org, which already holds its repos and
    /// dependencies. Fixed for the life of the session, so a resume runs the
    /// image it started on. `None` => the pool's own worker image.
    #[serde(default)]
    pub image: Option<String>,
    /// The session exists to build the pool's next image rather than to run
    /// an agent, so it alone gets the pool's `imageBuild` settings.
    #[serde(default)]
    pub image_build: bool,
    /// For an image build: `repository/path:tag` the built image is pushed
    /// to. Set by the control plane so the build box, where the customer's
    /// blueprints run, cannot choose where its image goes.
    #[serde(default)]
    pub image_target: Option<String>,
}

/// The egress restrictions of a session.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkPolicy {
    /// Whether the session's network access is restricted at all. When
    /// `false` the session is unrestricted and `allow` is empty.
    #[serde(default)]
    pub enabled: bool,
    /// The only destinations a restricted session may reach; an empty list
    /// denies everything.
    #[serde(default)]
    pub allow: Vec<NetDestination>,
}

/// One destination a [`NetworkPolicy`] allows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum NetDestination {
    /// A DNS name, optionally a glob in which `*` matches any run of
    /// characters, dots included (`*.example.com` covers `a.b.example.com`
    /// but not `example.com`).
    Hostname {
        /// The name or glob.
        hostname: String,
    },
    /// An IPv4 address or CIDR range.
    Ipv4 {
        /// The address or range.
        ipv4: String,
    },
    /// An IPv6 address or CIDR range.
    Ipv6 {
        /// The address or range.
        ipv6: String,
    },
    /// A shape the server added that this client does not know about.
    Unknown(serde_json::Value),
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
    pub claim_deadline: Option<i64>,
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
/// watch position. List and watch cursors are interchangeable
/// (Kubernetes-style list-then-watch); paging shares the watch's
/// at-least-once semantics, so boundary duplicates are expected.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DevinList {
    /// The items in this page.
    #[serde(default)]
    pub items: Vec<OutpostDevin>,
    /// Opaque cursor to resume listing/watching from.
    #[serde(default)]
    pub cursor: Option<String>,
    /// Whether the returned page was full; if true, more rows may be
    /// available after `cursor`.
    #[serde(default)]
    pub has_next_page: bool,
    /// Total number of matching rows.
    #[serde(default)]
    pub total: Option<u64>,
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

/// Identifying metadata of an outpost.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutpostMetadata {
    /// Stable outpost identifier.
    pub outpost_id: String,
    /// Account that owns the outpost.
    #[serde(default)]
    pub account_id: Option<String>,
    /// When the outpost was created (unix seconds).
    #[serde(default)]
    pub created_at: Option<i64>,
}

/// Desired state of an outpost.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutpostSpec {
    /// Unique (per account) outpost name.
    pub name: String,
    /// Machine platform; `None` means the default platform.
    #[serde(default)]
    pub platform: Option<String>,
    /// Human-readable description.
    #[serde(default)]
    pub description: Option<String>,
}

/// Observed state of an outpost.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutpostStatus {
    /// Number of pending (unclaimed) sessions in the queue.
    #[serde(default)]
    pub queue_depth: u64,
    /// Number of unexpired claims held by workers.
    #[serde(default)]
    pub active_claims: u64,
}

/// An outpost (account-scoped queue) as returned by the outposts endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Outpost {
    /// Identifying metadata.
    pub metadata: OutpostMetadata,
    /// Desired state.
    pub spec: OutpostSpec,
    /// Observed state.
    pub status: OutpostStatus,
}

/// A page of the standard cursor-paginated list envelope used by the outposts
/// endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutpostList {
    /// The items in this page.
    #[serde(default)]
    pub items: Vec<Outpost>,
    /// Cursor for the next page, when `has_next_page` is true.
    #[serde(default)]
    pub end_cursor: Option<String>,
    /// Whether more pages are available after `end_cursor`.
    #[serde(default)]
    pub has_next_page: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_from_a_server_without_requester_deserializes() {
        let metadata: Metadata =
            serde_json::from_str(r#"{"session_id": "devin-1", "outpost_id": "pool_1"}"#).unwrap();
        assert_eq!(metadata.requester, None);
    }

    #[test]
    fn requester_email_is_optional() {
        let metadata: Metadata = serde_json::from_str(
            r#"{"session_id": "devin-1", "outpost_id": "pool_1", "requester": {"user_id": "user-1"}}"#,
        )
        .unwrap();
        assert_eq!(
            metadata.requester,
            Some(Requester {
                user_id: "user-1".to_string(),
                email: None,
            })
        );
    }
}
