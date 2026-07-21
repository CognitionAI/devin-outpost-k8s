//! Long-lived per-pool watchers of the upstream queue.
//!
//! The reconciler is level-based: every pass re-lists the pool's queue and
//! converges the cluster to it. These watchers add edge-triggering on top —
//! each holds an SSE watch on its pool's queue and nudges the controller
//! (via [`kube::runtime::Controller::reconcile_all_on`]) whenever anything
//! changes, so claims start within a second of enqueue instead of a poll
//! interval later. Events carry no data into the reconciler; the list is the
//! source of truth.
//!
//! Watchers also track the last seen cursor, which the reconciler persists
//! in the pool's status so a restarted operator resumes the watch without a
//! full replay.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use futures::channel::mpsc::UnboundedSender;
use tracing::debug;

use crate::api::OutpostsClient;

/// Registry of running pool watchers, keyed by the pool object's UID.
pub struct PoolWatchers {
    trigger: UnboundedSender<()>,
    entries: Mutex<HashMap<String, Entry>>,
}

struct Entry {
    /// Hash of the watcher's inputs (pool ID, API URL, token); a change
    /// replaces the watcher.
    fingerprint: u64,
    cursor: Arc<Mutex<Option<String>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Entry {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Reconnect backoff bounds for a failing watch connection.
const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(60);
/// Connections living at least this long reset the backoff (the server ends
/// healthy streams every few minutes by design).
const HEALTHY_CONNECTION: Duration = Duration::from_secs(5);

impl PoolWatchers {
    /// Create a registry whose watchers nudge `trigger` on every queue event.
    pub fn new(trigger: UnboundedSender<()>) -> Self {
        Self {
            trigger,
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// Ensure a watcher runs for this pool, replacing one whose inputs
    /// changed (e.g. a rotated token). `initial_cursor` seeds a brand new
    /// watcher's position (from the pool's persisted status).
    pub fn ensure(
        &self,
        pool_uid: &str,
        fingerprint: u64,
        client: OutpostsClient,
        pool_id: String,
        initial_cursor: Option<String>,
    ) {
        let mut entries = self.entries.lock().expect("watcher registry poisoned");
        if let Some(entry) = entries.get(pool_uid)
            && entry.fingerprint == fingerprint
            && !entry.task.is_finished()
        {
            return;
        }
        let cursor = Arc::new(Mutex::new(initial_cursor));
        let task = tokio::spawn(watch_loop(
            client,
            pool_id,
            cursor.clone(),
            self.trigger.clone(),
        ));
        entries.insert(
            pool_uid.to_string(),
            Entry {
                fingerprint,
                cursor,
                task,
            },
        );
    }

    /// Last cursor seen by the pool's watcher, if any.
    pub fn cursor(&self, pool_uid: &str) -> Option<String> {
        let entries = self.entries.lock().expect("watcher registry poisoned");
        entries
            .get(pool_uid)
            .and_then(|e| e.cursor.lock().expect("cursor poisoned").clone())
    }

    /// Stop and forget the pool's watcher (pool deleted).
    pub fn remove(&self, pool_uid: &str) {
        self.entries
            .lock()
            .expect("watcher registry poisoned")
            .remove(pool_uid);
    }
}

async fn watch_loop(
    client: OutpostsClient,
    pool_id: String,
    cursor: Arc<Mutex<Option<String>>>,
    trigger: UnboundedSender<()>,
) {
    let mut backoff = BACKOFF_MIN;
    loop {
        let started = std::time::Instant::now();
        let from = cursor.lock().expect("cursor poisoned").clone();
        let mut stream = client.watch(&pool_id, from.as_deref());
        while let Some(item) = stream.next().await {
            match item {
                Ok(event) => {
                    if let Some(next) = event.cursor {
                        *cursor.lock().expect("cursor poisoned") = Some(next);
                    }
                    if trigger.unbounded_send(()).is_err() {
                        return;
                    }
                }
                Err(err) => {
                    debug!(pool = %pool_id, %err, "queue watch stream failed; reconnecting");
                    break;
                }
            }
        }
        if started.elapsed() >= HEALTHY_CONNECTION {
            backoff = BACKOFF_MIN;
        } else {
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(BACKOFF_MAX);
        }
    }
}
