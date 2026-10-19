//! Shared reconcile context handed to every reconcile invocation.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use kube::api::{Api, ObjectMeta, Patch, PatchParams};
use kube::{Client, ResourceExt};

use crate::config::OperatorConfig;
use crate::crd::OutpostPool;
use crate::metrics::Metrics;

use super::queue_watch::PoolWatchers;

/// Long-lived state shared across reconciles.
#[derive(Clone)]
pub struct Context {
    /// Kubernetes API client.
    pub client: Client,
    /// Operator-wide configuration.
    pub config: Arc<OperatorConfig>,
    /// Prometheus metrics handles.
    pub metrics: Arc<Metrics>,
    /// Registry of per-pool upstream queue watchers.
    pub watchers: Arc<PoolWatchers>,
    /// Lazily resolved acceptor identity (see [`Context::acceptor_id`]).
    acceptor_id: Arc<tokio::sync::OnceCell<String>>,
    /// Pools backing off after a failed reconcile.
    pub backoffs: Arc<PoolBackoffs>,
}

/// When failed pools may be reconciled again.
///
/// The controller's own backoff only delays the requeue it schedules; queue
/// events still trigger reconciles immediately. A failure after a claim
/// therefore loops hot, since the claim itself is a queue event. So a pool
/// stays untouched until its backoff ends or its spec changes (a changed
/// spec may be the fix).
#[derive(Default)]
pub struct PoolBackoffs(Mutex<HashMap<String, (Option<i64>, Instant)>>);

impl PoolBackoffs {
    /// Back `pool` off until `delay` after `now`.
    pub fn start(&self, pool: &OutpostPool, now: Instant, delay: Duration) {
        let mut backoffs = self.0.lock().expect("not poisoned");
        backoffs.insert(
            pool.uid().unwrap_or_default(),
            (pool.metadata.generation, now + delay),
        );
    }

    /// How long `pool` is still backing off for at `now`, if at all.
    pub fn remaining(&self, pool: &OutpostPool, now: Instant) -> Option<Duration> {
        let backoffs = self.0.lock().expect("not poisoned");
        let (generation, until) = backoffs.get(&pool.uid().unwrap_or_default())?;
        (*generation == pool.metadata.generation)
            .then(|| until.saturating_duration_since(now))
            .filter(|remaining| !remaining.is_zero())
    }

    /// End `pool`'s backoff.
    pub fn clear(&self, pool: &OutpostPool) {
        let mut backoffs = self.0.lock().expect("not poisoned");
        backoffs.remove(&pool.uid().unwrap_or_default());
    }
}

/// Name of the `ConfigMap` persisting the generated acceptor ID.
const ACCEPTOR_ID_CONFIGMAP: &str = "devin-outposts-k8s-acceptor-id";
/// Key of the acceptor ID within [`ACCEPTOR_ID_CONFIGMAP`].
const ACCEPTOR_ID_KEY: &str = "acceptor-id";

impl Context {
    /// Build a new reconcile context.
    pub fn new(
        client: Client,
        config: Arc<OperatorConfig>,
        metrics: Arc<Metrics>,
        watchers: Arc<PoolWatchers>,
    ) -> Self {
        Self {
            client,
            config,
            metrics,
            watchers,
            acceptor_id: Arc::default(),
            backoffs: Arc::default(),
        }
    }

    /// The acceptor identity this operator claims sessions as.
    ///
    /// Uses [`OperatorConfig::acceptor_id`] when set; otherwise
    /// read-or-creates a `ConfigMap` in the operator's namespace holding a
    /// generated value, so the identity survives restarts and upgrades (see
    /// the config field's docs for why the chart can't generate it).
    pub async fn acceptor_id(&self) -> crate::Result<&str> {
        let id = self
            .acceptor_id
            .get_or_try_init(|| async {
                if let Some(id) = &self.config.acceptor_id {
                    return Ok::<_, crate::Error>(id.clone());
                }
                let api: Api<k8s_openapi::api::core::v1::ConfigMap> =
                    Api::namespaced(self.client.clone(), &self.config.operator_namespace);
                if let Some(existing) = api.get_opt(ACCEPTOR_ID_CONFIGMAP).await?
                    && let Some(id) = existing
                        .data
                        .as_ref()
                        .and_then(|d| d.get(ACCEPTOR_ID_KEY))
                        .filter(|id| !id.is_empty())
                {
                    return Ok(id.clone());
                }
                let generated = format!(
                    "k8s-{}-{}",
                    self.config.operator_namespace,
                    &uuid::Uuid::new_v4().simple().to_string()[..8]
                );
                let cm = k8s_openapi::api::core::v1::ConfigMap {
                    metadata: ObjectMeta {
                        name: Some(ACCEPTOR_ID_CONFIGMAP.to_string()),
                        namespace: Some(self.config.operator_namespace.clone()),
                        ..Default::default()
                    },
                    data: Some(std::collections::BTreeMap::from([(
                        ACCEPTOR_ID_KEY.to_string(),
                        generated.clone(),
                    )])),
                    ..Default::default()
                };
                // Server-side apply without force: if another replica created
                // the map concurrently, the conflict falls through to its
                // value on the next call.
                api.patch(
                    ACCEPTOR_ID_CONFIGMAP,
                    &PatchParams::apply(crate::MANAGER_NAME),
                    &Patch::Apply(&cm),
                )
                .await?;
                Ok(generated)
            })
            .await?;
        Ok(id)
    }
}

#[cfg(test)]
mod tests {
    use crate::crd::{OutpostPoolSpec, SecretKeyRef};

    use super::*;

    fn pool(uid: &str, generation: i64) -> OutpostPool {
        let mut pool = OutpostPool::new(
            "my-pool",
            OutpostPoolSpec {
                pool_id: "pool_x".to_string(),
                token_secret_ref: SecretKeyRef {
                    name: "tok".to_string(),
                    key: "token".to_string(),
                },
                api_url: None,
                max_concurrent_sessions: 10,
                worker: serde_json::from_value(serde_json::json!({})).unwrap(),
                resume: Default::default(),
            },
        );
        pool.metadata.uid = Some(uid.to_string());
        pool.metadata.generation = Some(generation);
        pool
    }

    #[test]
    fn backoff_holds_until_it_ends_the_spec_changes_or_it_is_cleared() {
        let backoffs = PoolBackoffs::default();
        let now = Instant::now();
        let delay = Duration::from_secs(30);
        assert_eq!(backoffs.remaining(&pool("a", 1), now), None);

        backoffs.start(&pool("a", 1), now, delay);
        let later = now + Duration::from_secs(10);
        assert_eq!(
            backoffs.remaining(&pool("a", 1), later),
            Some(Duration::from_secs(20))
        );
        assert_eq!(backoffs.remaining(&pool("b", 1), later), None);
        assert_eq!(backoffs.remaining(&pool("a", 2), later), None);
        assert_eq!(backoffs.remaining(&pool("a", 1), now + delay), None);

        backoffs.clear(&pool("a", 1));
        assert_eq!(backoffs.remaining(&pool("a", 1), later), None);
    }
}
