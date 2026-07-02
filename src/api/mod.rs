//! Typed client and models for the upstream Devin outposts queue API.
//!
//! The API is served by `devin-webapp` under the `/.../outposts` prefix and
//! is intentionally Kubernetes-shaped:
//!
//! - `GET    /.../devins`               — list, or `?watch=true` to
//!   stream `MODIFIED`/`DELETED` events with a resumable opaque `cursor`
//!   (at-least-once delivery).
//! - `POST   /.../devins/{id}/claim`    — atomic compare-and-set
//!   claim. Returns a `connect_token` + `gateway_url`. Re-claiming with the same
//!   `acceptor_id` **renews** the claim (TTL ~5 min). `409` on conflict.
//! - `POST   /.../devins/{id}/release`  — return a session to the queue.
//! - `GET/POST/DELETE /.../outposts/pools`       — pool CRUD.
//!
//! Auth is a bearer **personal access token** with the `UseOutpostsMachine`
//! account permission; the whole surface is account-scoped and gated behind the
//! `outposts-enabled` flag.

mod client;
mod types;

pub use client::{ListParams, OutpostsClient};
pub use types::*;

/// Default upstream API base URL (overridable per pool / via env).
pub const DEFAULT_API_URL: &str = "https://api.devin.ai";

/// Path prefix for all outpost API endpoints.
pub const API_VERSION_PREFIX: &str = "/opbeta/outposts";
