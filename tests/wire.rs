//! Wire-level tests of [`OutpostsClient`] against the in-memory mock of the
//! `/opbeta/outposts` API (see [`mock_server`]).

mod mock_server;

use std::time::Duration;

use devin_outposts_k8s::Error;
use devin_outposts_k8s::api::{
    Kind, ListParams, OutpostsClient, Phase, SessionStatus, WatchEventKind,
};
use futures::StreamExt;
use mock_server::MockQueue;

const TOKEN: &str = "cog_testtoken";
const POOL: &str = "pool_test";

async fn setup() -> (MockQueue, OutpostsClient) {
    let queue = MockQueue::start(TOKEN).await;
    let client = OutpostsClient::new(&queue.base_url, TOKEN, "acceptor-a").unwrap();
    (queue, client)
}

#[tokio::test]
async fn list_paginates_and_dedups_boundary_overlap() {
    let (queue, client) = setup().await;
    for i in 0..5 {
        queue.enqueue(&format!("devin-{i}"), POOL);
    }
    // Page size 2 forces pagination; the `>=` keyset boundary re-serves the
    // last row of each page, which list_all must dedup.
    queue.set_page_size(2);
    let sessions = client.list_all(POOL, &ListParams::default()).await.unwrap();
    let mut ids: Vec<&str> = sessions
        .iter()
        .map(|s| s.metadata.session_id.as_str())
        .collect();
    ids.sort();
    assert_eq!(ids, ["devin-0", "devin-1", "devin-2", "devin-3", "devin-4"]);
    assert!(sessions.iter().all(|s| s.spec.kind == Kind::New));
    assert!(sessions.iter().all(|s| s.status.phase == Phase::Pending));
}

#[tokio::test]
async fn list_filters_by_phase_and_acceptor() {
    let (queue, client) = setup().await;
    queue.enqueue("devin-1", POOL);
    queue.enqueue("devin-2", POOL);
    client.claim("devin-1").await.unwrap();

    let claimed = client
        .list_all(
            POOL,
            &ListParams {
                acceptor_id: Some("acceptor-a"),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].metadata.session_id, "devin-1");

    let pending = client
        .list_all(
            POOL,
            &ListParams {
                phase: Some(Phase::Pending),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].metadata.session_id, "devin-2");
}

#[tokio::test]
async fn claim_is_cas_and_renewal_returns_fresh_token() {
    let (queue, client_a) = setup().await;
    let client_b = OutpostsClient::new(&queue.base_url, TOKEN, "acceptor-b").unwrap();
    queue.enqueue("devin-1", POOL);

    let first = client_a.claim("devin-1").await.unwrap();
    assert_eq!(first.status.connect_token.as_deref(), Some("tok-devin-1-1"));
    assert_eq!(
        first.status.gateway_url.as_deref(),
        Some("wss://outpost-gateway.mock")
    );
    let deadline = first.status.claim_deadline.unwrap();

    // Someone else's claim conflicts...
    let conflict = client_b.claim("devin-1").await.unwrap_err();
    assert!(matches!(conflict, Error::ClaimConflict { session_id } if session_id == "devin-1"));

    // ...but re-claiming with the same acceptor renews (fresh token, same or
    // later deadline).
    let renewed = client_a.claim("devin-1").await.unwrap();
    assert_eq!(
        renewed.status.connect_token.as_deref(),
        Some("tok-devin-1-2")
    );
    assert!(renewed.status.claim_deadline.unwrap() >= deadline);
}

#[tokio::test]
async fn expired_claims_return_to_queue_unless_session_is_live() {
    let (queue, client_a) = setup().await;
    let client_b = OutpostsClient::new(&queue.base_url, TOKEN, "acceptor-b").unwrap();
    queue.set_claim_ttl(0.0);

    // An expired claim on a live session stays with its worker.
    queue.enqueue("devin-live", POOL);
    client_a.claim("devin-live").await.unwrap();
    queue.set_session_status("devin-live", "running");
    let err = client_b.claim("devin-live").await.unwrap_err();
    assert!(matches!(err, Error::ClaimConflict { .. }));

    // An expired claim on a non-live session is reclaimable.
    queue.enqueue("devin-idle", POOL);
    client_a.claim("devin-idle").await.unwrap();
    client_b.claim("devin-idle").await.unwrap();
    assert_eq!(
        queue.row("devin-idle").acceptor_id.as_deref(),
        Some("acceptor-b")
    );
}

#[tokio::test]
async fn release_requires_holding_the_claim() {
    let (queue, client) = setup().await;
    queue.enqueue("devin-1", POOL);

    // Releasing an unclaimed session conflicts.
    let err = client.release("devin-1").await.unwrap_err();
    assert!(matches!(err, Error::ClaimConflict { .. }));

    client.claim("devin-1").await.unwrap();
    client.release("devin-1").await.unwrap();
    assert_eq!(queue.row("devin-1").phase, "pending");

    // Releasing a tombstoned session is a 404 the caller treats as released.
    queue.tombstone("devin-1");
    let err = client.release("devin-1").await.unwrap_err();
    assert!(err.is_not_found());
}

#[tokio::test]
async fn watch_replays_then_streams_live_events() {
    let (queue, client) = setup().await;
    queue.enqueue("devin-1", POOL);

    let mut stream = client.watch(POOL, None);

    // Replay of the existing row.
    let replayed = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(replayed.kind, WatchEventKind::Modified);
    assert_eq!(replayed.object.metadata.session_id, "devin-1");
    assert!(replayed.cursor.is_some());

    // Live change.
    queue.set_session_status("devin-1", "terminated");
    let live = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(live.kind, WatchEventKind::Modified);
    assert_eq!(live.object.status.session_status, SessionStatus::Terminated);

    // Tombstones arrive as DELETED.
    queue.tombstone("devin-1");
    let deleted = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(deleted.kind, WatchEventKind::Deleted);
}

#[tokio::test]
async fn watch_resumes_from_cursor_with_at_least_once_delivery() {
    let (queue, client) = setup().await;
    queue.enqueue("devin-1", POOL);

    let mut stream = client.watch(POOL, None);
    let first = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let cursor = first.cursor.clone().unwrap();
    drop(stream);

    queue.enqueue("devin-2", POOL);

    // Reconnecting with the cursor sees the new row; the old row may repeat
    // (at-least-once), but must not be the only thing delivered.
    let mut stream = client.watch(POOL, Some(&cursor));
    let mut seen = Vec::new();
    for _ in 0..2 {
        match tokio::time::timeout(Duration::from_secs(2), stream.next()).await {
            Ok(Some(Ok(event))) => seen.push(event.object.metadata.session_id.clone()),
            _ => break,
        }
        if seen.contains(&"devin-2".to_string()) {
            break;
        }
    }
    assert!(seen.contains(&"devin-2".to_string()), "saw {seen:?}");
}

#[tokio::test]
async fn watch_stream_ends_after_server_max_duration() {
    let (queue, client) = setup().await;
    queue.set_watch_max_duration(Duration::from_millis(300));

    let mut stream = client.watch(POOL, None);
    // Keepalive comments are filtered out, so an idle stream yields nothing
    // and terminates when the server ends it; the caller then reconnects.
    let next = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("stream should end, not hang");
    assert!(next.is_none());
}

#[tokio::test]
async fn unknown_enum_variants_degrade_to_unknown() {
    let (queue, client) = setup().await;
    queue.enqueue_raw("devin-1", POOL, "hibernate", "defragmenting", "pending");

    let sessions = client.list_all(POOL, &ListParams::default()).await.unwrap();
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].spec.kind, Kind::Unknown);
    assert_eq!(sessions[0].status.session_status, SessionStatus::Unknown);
}

#[tokio::test]
async fn bad_token_is_unauthorized() {
    let (queue, _) = setup().await;
    let client = OutpostsClient::new(&queue.base_url, "cog_wrong", "acceptor-a").unwrap();
    let err = client
        .list_all(POOL, &ListParams::default())
        .await
        .unwrap_err();
    assert!(err.is_unauthorized());
}

#[tokio::test]
async fn list_pools_follows_the_paginated_envelope() {
    let (queue, client) = setup().await;
    queue.enqueue("devin-1", "pool_a");
    queue.enqueue("devin-2", "pool_b");
    client.claim("devin-1").await.unwrap();

    let mut pools = client.list_pools().await.unwrap();
    pools.sort_by(|a, b| a.metadata.pool_id.cmp(&b.metadata.pool_id));
    assert_eq!(pools.len(), 2);
    assert_eq!(pools[0].metadata.pool_id, "pool_a");
    assert_eq!(pools[0].status.active_claims, 1);
    assert_eq!(pools[1].status.queue_depth, 1);
}
