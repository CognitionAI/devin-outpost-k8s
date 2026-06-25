//! HTTP client for the `/opbeta/outposts` queue API.
//!
//! Scaffold only: method signatures and the request shapes are defined, but the
//! bodies return [`Error::NotImplemented`]. The real implementation will use the
//! `reqwest` client held here (list/claim/release as JSON requests, watch as an
//! SSE stream decoded into [`WatchEvent`]s).

use futures::stream::{self, BoxStream};

use crate::error::{Error, Result};

use super::types::{ClaimResponse, DevinList, Pool, WatchEvent};

/// A client bound to a single account token + API base URL.
#[derive(Clone)]
pub struct OutpostsClient {
    http: reqwest::Client,
    base_url: String,
    /// PAT with the `UseOutpostsMachine` permission. Never logged.
    token: String,
    /// Stable identity used when claiming/renewing; one per worker identity.
    acceptor_id: String,
}

impl std::fmt::Debug for OutpostsClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OutpostsClient")
            .field("base_url", &self.base_url)
            .field("acceptor_id", &self.acceptor_id)
            .field("token", &"<redacted>")
            .finish()
    }
}

impl OutpostsClient {
    /// Create a new client for the given base URL, token and acceptor identity.
    pub fn new(
        base_url: impl Into<String>,
        token: impl Into<String>,
        acceptor_id: impl Into<String>,
    ) -> Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder()
                .user_agent(concat!("outposts-operator/", env!("CARGO_PKG_VERSION")))
                .build()?,
            base_url: base_url.into(),
            token: token.into(),
            acceptor_id: acceptor_id.into(),
        })
    }

    /// The acceptor identity this client claims sessions as.
    pub fn acceptor_id(&self) -> &str {
        &self.acceptor_id
    }

    /// List a page of queued sessions for `pool_id`, optionally resuming from
    /// `cursor`.
    ///
    /// TODO: `GET {base}/opbeta/outposts/devins?pool={pool}&cursor={cursor}`.
    pub async fn list(&self, _pool_id: &str, _cursor: Option<&str>) -> Result<DevinList> {
        Err(Error::todo("OutpostsClient::list"))
    }

    /// Open a watch stream for `pool_id` starting from `cursor`.
    ///
    /// TODO: `GET .../devins?pool={pool}&watch=true&cursor={cursor}` as SSE,
    /// decoded into [`WatchEvent`]s, with reconnect-on-cursor semantics.
    pub fn watch(
        &self,
        _pool_id: &str,
        _cursor: Option<&str>,
    ) -> BoxStream<'static, Result<WatchEvent>> {
        Box::pin(stream::once(async {
            Err(Error::todo("OutpostsClient::watch"))
        }))
    }

    /// Claim (or renew, if already held by this `acceptor_id`) a session.
    ///
    /// TODO: `POST .../devins/{id}/claim`; map HTTP 409 to
    /// [`Error::ClaimConflict`].
    pub async fn claim(&self, session_id: &str) -> Result<ClaimResponse> {
        let _ = session_id;
        Err(Error::todo("OutpostsClient::claim"))
    }

    /// Release a session back to the queue.
    ///
    /// TODO: `POST .../devins/{id}/release`.
    pub async fn release(&self, _session_id: &str) -> Result<()> {
        Err(Error::todo("OutpostsClient::release"))
    }

    /// List the pools visible to this token.
    ///
    /// TODO: `GET .../pools`.
    pub async fn list_pools(&self) -> Result<Vec<Pool>> {
        Err(Error::todo("OutpostsClient::list_pools"))
    }
}
