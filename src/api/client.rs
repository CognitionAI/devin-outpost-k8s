//! HTTP client for the `/opbeta/outposts` queue API.

use std::time::Duration;

use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use serde::Serialize;

use crate::error::{Error, Result};

use super::types::{DevinList, OutpostDevin, Phase, Pool, PoolList, WatchEvent};

/// Timeout for plain JSON requests. Watch streams are long-lived and are not
/// subject to it.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// A client bound to a single account token + API base URL.
#[derive(Clone)]
pub struct OutpostsClient {
    http: reqwest::Client,
    base_url: String,
    /// PAT with the `UseOutpostsMachine` permission. Never logged.
    token: String,
    /// Identity used when claiming/renewing; see
    /// [`crate::config::OperatorConfig::acceptor_id`] for its stability and
    /// uniqueness requirements.
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

/// Filters for [`OutpostsClient::list`]. `phase` and `acceptor_id` are
/// list-only filters; the watch stream ignores them (clients filter events
/// themselves, as in Kubernetes watches).
#[derive(Debug, Clone, Default, Serialize)]
pub struct ListParams<'a> {
    /// Filter by queue phase.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<Phase>,
    /// Filter by claiming worker.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acceptor_id: Option<&'a str>,
}

#[derive(Serialize)]
struct AcceptorBody<'a> {
    acceptor_id: &'a str,
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
                .user_agent(concat!("devin-outposts-k8s/", env!("CARGO_PKG_VERSION")))
                .connect_timeout(Duration::from_secs(10))
                .build()?,
            base_url: base_url.into().trim_end_matches('/').to_string(),
            token: token.into(),
            acceptor_id: acceptor_id.into(),
        })
    }

    /// The acceptor identity this client claims sessions as.
    pub fn acceptor_id(&self) -> &str {
        &self.acceptor_id
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}{path}", self.base_url, super::API_VERSION_PREFIX)
    }

    async fn check(resp: reqwest::Response) -> Result<reqwest::Response> {
        let status = resp.status();
        if status.is_success() {
            return Ok(resp);
        }
        let message = resp.text().await.unwrap_or_default();
        Err(Error::ApiStatus { status, message })
    }

    /// List a page of queued sessions for `pool_id`, optionally resuming from
    /// `cursor`.
    pub async fn list(
        &self,
        pool_id: &str,
        cursor: Option<&str>,
        params: &ListParams<'_>,
    ) -> Result<DevinList> {
        let mut req = self
            .http
            .get(self.url("/devins"))
            .bearer_auth(&self.token)
            .timeout(REQUEST_TIMEOUT)
            .query(&[("pool", pool_id)])
            .query(params);
        if let Some(cursor) = cursor {
            req = req.query(&[("cursor", cursor)]);
        }
        let resp = Self::check(req.send().await?).await?;
        Ok(resp.json().await?)
    }

    /// List *all* queued sessions for `pool_id`, following pagination.
    ///
    /// Paging shares the watch's at-least-once semantics, so rows on a page
    /// boundary may repeat; later pages win the dedup since they carry the
    /// fresher row.
    pub async fn list_all(
        &self,
        pool_id: &str,
        params: &ListParams<'_>,
    ) -> Result<Vec<OutpostDevin>> {
        let mut items: Vec<OutpostDevin> = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let page = self.list(pool_id, cursor.as_deref(), params).await?;
            for item in page.items {
                if let Some(existing) = items
                    .iter_mut()
                    .find(|i| i.metadata.session_id == item.metadata.session_id)
                {
                    *existing = item;
                } else {
                    items.push(item);
                }
            }
            if !page.has_next_page {
                return Ok(items);
            }
            cursor = page.cursor;
        }
    }

    /// Open a watch stream for `pool_id` starting from `cursor`.
    ///
    /// Decodes the server's SSE stream into [`WatchEvent`]s. The server ends
    /// every stream after a few minutes (mirroring Kubernetes watch
    /// semantics); the returned stream simply terminates and the caller
    /// reconnects with the last event's `cursor`.
    pub fn watch(
        &self,
        pool_id: &str,
        cursor: Option<&str>,
    ) -> BoxStream<'static, Result<WatchEvent>> {
        let mut req = self
            .http
            .get(self.url("/devins"))
            .bearer_auth(&self.token)
            .query(&[("pool", pool_id), ("watch", "true")]);
        if let Some(cursor) = cursor {
            req = req.query(&[("cursor", cursor)]);
        }

        Box::pin(
            futures::stream::once(async move {
                let resp = Self::check(req.send().await?).await?;
                let events = sse_data_lines(resp.bytes_stream()).filter_map(|line| async {
                    match line {
                        Ok(data) => {
                            Some(serde_json::from_str::<WatchEvent>(&data).map_err(Error::from))
                        }
                        Err(e) => Some(Err(e)),
                    }
                });
                Ok::<_, Error>(events)
            })
            .try_flatten(),
        )
    }

    /// Claim (or renew, if already held by this `acceptor_id`) a session.
    ///
    /// A successful claim's `status` carries the `connect_token` +
    /// `gateway_url` the worker needs. HTTP 409 maps to
    /// [`Error::ClaimConflict`].
    pub async fn claim(&self, session_id: &str) -> Result<OutpostDevin> {
        let resp = self
            .http
            .post(self.url(&format!("/devins/{session_id}/claim")))
            .bearer_auth(&self.token)
            .timeout(REQUEST_TIMEOUT)
            .json(&AcceptorBody {
                acceptor_id: &self.acceptor_id,
            })
            .send()
            .await?;
        if resp.status() == reqwest::StatusCode::CONFLICT {
            return Err(Error::ClaimConflict {
                session_id: session_id.to_string(),
            });
        }
        Ok(Self::check(resp).await?.json().await?)
    }

    /// Release a session back to the queue.
    ///
    /// HTTP 409 (not claimed by this acceptor) maps to
    /// [`Error::ClaimConflict`]: the claim already expired or moved on, which
    /// callers usually treat as already-released.
    pub async fn release(&self, session_id: &str) -> Result<()> {
        let resp = self
            .http
            .post(self.url(&format!("/devins/{session_id}/release")))
            .bearer_auth(&self.token)
            .timeout(REQUEST_TIMEOUT)
            .json(&AcceptorBody {
                acceptor_id: &self.acceptor_id,
            })
            .send()
            .await?;
        if resp.status() == reqwest::StatusCode::CONFLICT {
            return Err(Error::ClaimConflict {
                session_id: session_id.to_string(),
            });
        }
        Self::check(resp).await?;
        Ok(())
    }

    /// List the pools visible to this token, following pagination.
    pub async fn list_pools(&self) -> Result<Vec<Pool>> {
        let mut pools = Vec::new();
        let mut after: Option<String> = None;
        loop {
            let mut req = self
                .http
                .get(self.url("/pools"))
                .bearer_auth(&self.token)
                .timeout(REQUEST_TIMEOUT);
            if let Some(after) = &after {
                req = req.query(&[("after", after.as_str())]);
            }
            let page: PoolList = Self::check(req.send().await?).await?.json().await?;
            pools.extend(page.items);
            if !page.has_next_page {
                return Ok(pools);
            }
            after = page.end_cursor;
        }
    }
}

/// Decode an SSE byte stream into the payloads of its `data:` lines.
///
/// Only the subset of SSE the outposts server emits is handled: single-line
/// `data:` events separated by blank lines, and comment lines (`: keepalive`)
/// which are dropped.
fn sse_data_lines(
    bytes: impl futures::Stream<Item = reqwest::Result<bytes::Bytes>> + Send + 'static,
) -> BoxStream<'static, Result<String>> {
    let lines = futures::stream::unfold(
        (Box::pin(bytes), Vec::<u8>::new(), false),
        |(mut bytes, mut buf, mut done)| async move {
            loop {
                if let Some(pos) = buf.iter().position(|&b| b == b'\n') {
                    let mut line: Vec<u8> = buf.drain(..=pos).collect();
                    line.pop();
                    if line.last() == Some(&b'\r') {
                        line.pop();
                    }
                    let line = String::from_utf8_lossy(&line).into_owned();
                    return Some((Ok(line), (bytes, buf, done)));
                }
                if done {
                    return None;
                }
                match bytes.next().await {
                    Some(Ok(chunk)) => buf.extend_from_slice(&chunk),
                    Some(Err(e)) => return Some((Err(Error::from(e)), (bytes, buf, done))),
                    None => done = true,
                }
            }
        },
    );
    Box::pin(lines.filter_map(|line| async move {
        match line {
            Ok(line) => line
                .strip_prefix("data:")
                .map(|data| Ok(data.trim_start().to_string())),
            Err(e) => Some(Err(e)),
        }
    }))
}
