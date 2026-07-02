//! In-memory mock of the `/opbeta/outposts` queue API, mirroring the
//! semantics of `devin-webapp`'s `outposts_router.py` closely enough to test
//! the client and controller against realistic wire behavior:
//!
//! - list: keyset pagination over `(updated_at, id)` with an opaque cursor,
//!   `>=` boundary overlap (at-least-once), lazy claim expiry on read.
//! - watch: SSE stream that replays rows at/after the cursor, dedups by
//!   `(session, updated_at)`, emits `: keepalive` comments when idle, and
//!   ends after a max duration (clients must reconnect with their cursor).
//! - claim: CAS with 409 on conflict, renewal-by-same-acceptor extending the
//!   deadline, expired claims reclaimable unless the session is live, and a
//!   fresh connect token per (re)claim.
//! - release: 409 unless currently claimed by the releasing acceptor.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde_json::json;

const CURSOR_PREFIX: &str = "v1:";
const WATCH_POLL: Duration = Duration::from_millis(20);
const WATCH_CURSOR_EPSILON: f64 = 0.05;

#[derive(Clone, Debug)]
pub struct Row {
    pub id: i64,
    pub session_id: String,
    pub pool_id: String,
    pub kind: &'static str,
    pub session_status: String,
    pub phase: String,
    pub acceptor_id: Option<String>,
    pub claim_deadline: Option<f64>,
    pub remote_binary_sha: Option<String>,
    pub created_at: f64,
    pub updated_at: f64,
    pub deleted_at: Option<f64>,
    pub claim_count: u64,
}

struct QueueState {
    rows: Vec<Row>,
    next_id: i64,
    token: String,
    claim_ttl: f64,
    page_size: usize,
    watch_max_duration: Duration,
}

/// Handle to the running mock server and its state.
pub struct MockQueue {
    state: Arc<Mutex<QueueState>>,
    pub base_url: String,
}

/// Strictly monotonic wall clock. Real `updated_at`s come from the database
/// with microsecond precision and never collide in practice; back-to-back
/// mutations here can, which would wedge `(updated_at, id) >= cursor` keyset
/// pagination.
fn now() -> f64 {
    static LAST_MICROS: Mutex<u64> = Mutex::new(0);
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64;
    let mut last = LAST_MICROS.lock().unwrap();
    *last = micros.max(*last + 1);
    *last as f64 / 1e6
}

fn encode_cursor(ts: f64) -> String {
    format!("{CURSOR_PREFIX}{ts}")
}

fn decode_cursor(raw: &str) -> Option<f64> {
    raw.strip_prefix(CURSOR_PREFIX)?.parse().ok()
}

impl MockQueue {
    pub async fn start(token: &str) -> Self {
        let state = Arc::new(Mutex::new(QueueState {
            rows: Vec::new(),
            next_id: 1,
            token: token.to_string(),
            claim_ttl: 300.0,
            page_size: 100,
            watch_max_duration: Duration::from_secs(300),
        }));
        let app = Router::new()
            .route("/opbeta/outposts/devins", get(list_or_watch))
            .route("/opbeta/outposts/devins/{id}/claim", post(claim))
            .route("/opbeta/outposts/devins/{id}/release", post(release))
            .route("/opbeta/outposts/pools", get(pools))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self {
            state,
            base_url: format!("http://{addr}"),
        }
    }

    pub fn set_claim_ttl(&self, secs: f64) {
        self.state.lock().unwrap().claim_ttl = secs;
    }

    pub fn set_page_size(&self, n: usize) {
        self.state.lock().unwrap().page_size = n;
    }

    pub fn set_watch_max_duration(&self, d: Duration) {
        self.state.lock().unwrap().watch_max_duration = d;
    }

    pub fn enqueue(&self, session_id: &str, pool_id: &str) {
        self.enqueue_raw(session_id, pool_id, "new", "pending", "pending");
    }

    /// Insert a row with arbitrary status strings (e.g. values this client
    /// version does not know about).
    pub fn enqueue_raw(
        &self,
        session_id: &str,
        pool_id: &str,
        kind: &'static str,
        session_status: &str,
        phase: &str,
    ) {
        let mut state = self.state.lock().unwrap();
        let ts = now();
        let id = state.next_id;
        state.next_id += 1;
        state.rows.push(Row {
            id,
            session_id: session_id.to_string(),
            pool_id: pool_id.to_string(),
            kind,
            session_status: session_status.to_string(),
            phase: phase.to_string(),
            acceptor_id: None,
            claim_deadline: None,
            remote_binary_sha: None,
            created_at: ts,
            updated_at: ts,
            deleted_at: None,
            claim_count: 0,
        });
    }

    pub fn set_session_status(&self, session_id: &str, status: &str) {
        let mut state = self.state.lock().unwrap();
        let ts = now();
        let row = state
            .rows
            .iter_mut()
            .find(|r| r.session_id == session_id)
            .expect("unknown session");
        row.session_status = status.to_string();
        row.updated_at = ts;
    }

    /// Soft-delete (tombstone) a row; watchers emit it as DELETED.
    pub fn tombstone(&self, session_id: &str) {
        let mut state = self.state.lock().unwrap();
        let ts = now();
        let row = state
            .rows
            .iter_mut()
            .find(|r| r.session_id == session_id)
            .expect("unknown session");
        row.deleted_at = Some(ts);
        row.updated_at = ts;
    }

    pub fn row(&self, session_id: &str) -> Row {
        self.state
            .lock()
            .unwrap()
            .rows
            .iter()
            .find(|r| r.session_id == session_id)
            .expect("unknown session")
            .clone()
    }
}

fn check_auth(state: &QueueState, headers: &HeaderMap) -> Result<(), Box<Response>> {
    let ok = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v == format!("Bearer {}", state.token));
    if ok {
        Ok(())
    } else {
        Err(Box::new(
            (
                StatusCode::UNAUTHORIZED,
                json_body(json!({"detail": "Invalid token"})),
            )
                .into_response(),
        ))
    }
}

fn json_body(value: serde_json::Value) -> String {
    value.to_string()
}

/// Mirrors `_expire_stale_claims`: expired claims flip back to pending unless
/// the underlying session is live.
fn expire_stale_claims(state: &mut QueueState) {
    let ts = now();
    for row in &mut state.rows {
        if row.phase == "claimed"
            && row.deleted_at.is_none()
            && row.claim_deadline.is_some_and(|d| d < ts)
            && row.session_status != "running"
        {
            row.phase = "pending".to_string();
            row.acceptor_id = None;
            row.claim_deadline = None;
            row.updated_at = ts;
        }
    }
}

fn row_json(row: &Row) -> serde_json::Value {
    json!({
        "metadata": {
            "session_id": row.session_id,
            "pool_id": row.pool_id,
            "created_at": row.created_at as i64,
            "updated_at": row.updated_at as i64,
        },
        "spec": {
            "kind": row.kind,
            "platform": "linux",
            "remote_binary_sha": row.remote_binary_sha,
        },
        "status": {
            "phase": row.phase,
            "acceptor_id": row.acceptor_id,
            "claim_deadline": row.claim_deadline.map(|d| d as i64),
            "session_status": row.session_status,
            "connect_token": null,
            "gateway_url": null,
        },
    })
}

async fn list_or_watch(
    State(state): State<Arc<Mutex<QueueState>>>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    let watch = params.get("watch").is_some_and(|w| w == "true");
    if watch {
        return watch_stream(state, params, headers).await;
    }

    let mut guard = state.lock().unwrap();
    if let Err(resp) = check_auth(&guard, &headers) {
        return *resp;
    }
    expire_stale_claims(&mut guard);

    let since = params.get("cursor").and_then(|c| decode_cursor(c));
    let mut rows: Vec<&Row> = guard
        .rows
        .iter()
        .filter(|r| r.deleted_at.is_none())
        .filter(|r| params.get("pool").is_none_or(|p| &r.pool_id == p))
        .filter(|r| params.get("phase").is_none_or(|p| &r.phase == p))
        .filter(|r| {
            params
                .get("acceptor_id")
                .is_none_or(|a| r.acceptor_id.as_ref() == Some(a))
        })
        .filter(|r| since.is_none_or(|s| (r.updated_at, r.id) >= (s, 0)))
        .collect();
    rows.sort_by(|a, b| {
        (a.updated_at, a.id)
            .partial_cmp(&(b.updated_at, b.id))
            .unwrap()
    });
    let total = rows.len();
    let first = params
        .get("first")
        .and_then(|f| f.parse().ok())
        .unwrap_or(guard.page_size);
    let page: Vec<&&Row> = rows.iter().take(first).collect();
    let has_next_page = page.len() == first;
    let cursor = if has_next_page {
        encode_cursor(page.last().map(|r| r.updated_at).unwrap_or_else(now))
    } else {
        encode_cursor(now())
    };
    let body = json!({
        "items": page.iter().map(|r| row_json(r)).collect::<Vec<_>>(),
        "cursor": cursor,
        "has_next_page": has_next_page,
        "total": total,
    });
    (StatusCode::OK, json_body(body)).into_response()
}

async fn watch_stream(
    state: Arc<Mutex<QueueState>>,
    params: HashMap<String, String>,
    headers: HeaderMap,
) -> Response {
    {
        let guard = state.lock().unwrap();
        if let Err(resp) = check_auth(&guard, &headers) {
            return *resp;
        }
    }
    let pool = params.get("pool").cloned();
    let since = params
        .get("cursor")
        .and_then(|c| decode_cursor(c))
        .map(|s| s - WATCH_CURSOR_EPSILON);
    let max_duration = state.lock().unwrap().watch_max_duration;
    let deadline = tokio::time::Instant::now() + max_duration;

    struct StreamState {
        state: Arc<Mutex<QueueState>>,
        pool: Option<String>,
        cursor: Option<(f64, i64)>,
        emitted: HashMap<String, f64>,
        pending: Vec<String>,
        deadline: tokio::time::Instant,
    }

    let stream = futures::stream::unfold(
        StreamState {
            state,
            pool,
            cursor: since.map(|s| (s, 0)),
            emitted: HashMap::new(),
            pending: Vec::new(),
            deadline,
        },
        |mut st| async move {
            loop {
                if let Some(chunk) = st.pending.pop() {
                    return Some((Ok::<_, Infallible>(chunk), st));
                }
                if tokio::time::Instant::now() >= st.deadline {
                    return None;
                }
                let mut chunks = Vec::new();
                {
                    let mut guard = st.state.lock().unwrap();
                    expire_stale_claims(&mut guard);
                    let mut rows: Vec<Row> = guard
                        .rows
                        .iter()
                        .filter(|r| st.pool.as_ref().is_none_or(|p| &r.pool_id == p))
                        .filter(|r| st.cursor.is_none_or(|c| (r.updated_at, r.id) >= c))
                        .cloned()
                        .collect();
                    rows.sort_by(|a, b| {
                        (a.updated_at, a.id)
                            .partial_cmp(&(b.updated_at, b.id))
                            .unwrap()
                    });
                    for row in rows {
                        st.cursor = Some((row.updated_at, row.id));
                        if st.emitted.get(&row.session_id) == Some(&row.updated_at) {
                            continue;
                        }
                        st.emitted.insert(row.session_id.clone(), row.updated_at);
                        let event = json!({
                            "type": if row.deleted_at.is_some() { "DELETED" } else { "MODIFIED" },
                            "object": row_json(&row),
                            "cursor": encode_cursor(row.updated_at),
                        });
                        chunks.push(format!("data: {event}\n\n"));
                    }
                }
                if chunks.is_empty() {
                    st.pending.push(": keepalive\n\n".to_string());
                    tokio::time::sleep(WATCH_POLL).await;
                } else {
                    chunks.reverse();
                    st.pending = chunks;
                }
            }
        },
    );

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .body(Body::from_stream(stream))
        .unwrap()
}

async fn claim(
    State(state): State<Arc<Mutex<QueueState>>>,
    Path(session_id): Path<String>,
    headers: HeaderMap,
    body: String,
) -> Response {
    let mut guard = state.lock().unwrap();
    if let Err(resp) = check_auth(&guard, &headers) {
        return *resp;
    }
    let acceptor: String = serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|v| v["acceptor_id"].as_str().map(str::to_string))
        .unwrap_or_default();
    let ts = now();
    let claim_ttl = guard.claim_ttl;
    let Some(row) = guard
        .rows
        .iter_mut()
        .find(|r| r.session_id == session_id && r.deleted_at.is_none())
    else {
        return (
            StatusCode::NOT_FOUND,
            json_body(json!({"detail": "Session not found in queue"})),
        )
            .into_response();
    };

    let renewal = row.phase == "claimed" && row.acceptor_id.as_deref() == Some(&acceptor);
    let mut claim_expired = row.phase == "claimed" && row.claim_deadline.is_some_and(|d| d <= ts);
    // A claim whose session is live is held by a worker actively serving it.
    if claim_expired && !renewal && row.session_status == "running" {
        claim_expired = false;
    }
    if row.phase != "pending" && !claim_expired && !renewal {
        return (
            StatusCode::CONFLICT,
            json_body(json!({"detail": "Session is already claimed"})),
        )
            .into_response();
    }

    row.phase = "claimed".to_string();
    row.acceptor_id = Some(acceptor.clone());
    row.claim_deadline = Some(ts + claim_ttl);
    row.updated_at = ts;
    row.claim_count += 1;

    let mut response = row_json(row);
    response["status"]["connect_token"] =
        json!(format!("tok-{}-{}", row.session_id, row.claim_count));
    response["status"]["gateway_url"] = json!("wss://outpost-gateway.mock");
    (StatusCode::OK, json_body(response)).into_response()
}

async fn release(
    State(state): State<Arc<Mutex<QueueState>>>,
    Path(session_id): Path<String>,
    headers: HeaderMap,
    body: String,
) -> Response {
    let mut guard = state.lock().unwrap();
    if let Err(resp) = check_auth(&guard, &headers) {
        return *resp;
    }
    let acceptor: String = serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|v| v["acceptor_id"].as_str().map(str::to_string))
        .unwrap_or_default();
    let ts = now();
    let Some(row) = guard
        .rows
        .iter_mut()
        .find(|r| r.session_id == session_id && r.deleted_at.is_none())
    else {
        return (
            StatusCode::NOT_FOUND,
            json_body(json!({"detail": "Session not found in queue"})),
        )
            .into_response();
    };
    if row.phase != "claimed" || row.acceptor_id.as_deref() != Some(&acceptor) {
        return (
            StatusCode::CONFLICT,
            json_body(json!({"detail": "Session is not claimed by this acceptor"})),
        )
            .into_response();
    }
    row.phase = "pending".to_string();
    row.acceptor_id = None;
    row.claim_deadline = None;
    row.updated_at = ts;
    (StatusCode::OK, json_body(row_json(row))).into_response()
}

async fn pools(State(state): State<Arc<Mutex<QueueState>>>, headers: HeaderMap) -> Response {
    let guard = state.lock().unwrap();
    if let Err(resp) = check_auth(&guard, &headers) {
        return *resp;
    }
    let mut pool_ids: Vec<String> = guard.rows.iter().map(|r| r.pool_id.clone()).collect();
    pool_ids.sort();
    pool_ids.dedup();
    let items: Vec<serde_json::Value> = pool_ids
        .iter()
        .map(|pool_id| {
            let queue_depth = guard
                .rows
                .iter()
                .filter(|r| &r.pool_id == pool_id && r.phase == "pending" && r.deleted_at.is_none())
                .count();
            let active_claims = guard
                .rows
                .iter()
                .filter(|r| &r.pool_id == pool_id && r.phase == "claimed" && r.deleted_at.is_none())
                .count();
            json!({
                "metadata": {"pool_id": pool_id, "account_id": "acc_mock", "created_at": 0},
                "spec": {"name": format!("{pool_id}-name"), "platform": "linux", "description": null},
                "status": {"queue_depth": queue_depth, "active_claims": active_claims},
            })
        })
        .collect();
    let body = json!({
        "items": items,
        "end_cursor": null,
        "has_next_page": false,
        "total": items.len(),
    });
    (StatusCode::OK, json_body(body)).into_response()
}
