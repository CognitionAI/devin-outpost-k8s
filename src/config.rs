//! Process-level runtime configuration for the operator.
//!
//! Per-pool behaviour (token, VM size, runtime class, snapshot policy, ...)
//! lives on the [`crate::crd::OutpostPool`] custom resource. This struct only
//! holds settings that apply to the operator process as a whole.

use std::time::Duration;

/// Operator-wide runtime configuration, typically sourced from environment
/// variables / flags at startup.
#[derive(Debug, Clone)]
pub struct OperatorConfig {
    /// Address the Prometheus `/metrics` + health server binds to.
    pub metrics_addr: std::net::SocketAddr,

    /// Optional namespace to restrict the operator to. `None` => cluster-wide.
    pub watch_namespace: Option<String>,

    /// Default upstream API base URL when an [`crate::crd::OutpostPool`] does
    /// not override it.
    pub default_api_url: String,

    /// Default worker image used when a pool's `worker.overrides.image` is
    /// unset. Pinned to the operator release (typically set via the Helm chart).
    pub default_worker_image: String,

    /// How long to wait between full re-list/reconcile passes of a pool's queue.
    pub reconcile_interval: Duration,

    /// Safety margin to renew a claim before its deadline expires.
    pub claim_renew_margin: Duration,

    /// Identity used when claiming sessions upstream. Must be unique within the
    /// Devin account and stable for this install (re-claiming with the same
    /// acceptor renews, so instability orphans claims for one TTL). `None` =>
    /// the operator read-or-creates a `ConfigMap` in its own namespace holding
    /// a generated `k8s-<namespace>-<random>` value. Generated at runtime
    /// rather than by the chart because template-time randomness regenerates on
    /// every `helm upgrade` (and the `lookup` workaround breaks `helm
    /// template`/GitOps).
    pub acceptor_id: Option<String>,

    /// Restarts of a worker pod (non-zero exits, restarted in place by the
    /// kubelet) after which the operator gives up on it: release the claim +
    /// delete the pod. See the lifecycle mapping in [`crate::controller`].
    pub worker_restart_limit: u32,
}

impl Default for OperatorConfig {
    fn default() -> Self {
        Self {
            metrics_addr: ([0, 0, 0, 0], 8080).into(),
            watch_namespace: None,
            default_api_url: crate::api::DEFAULT_API_URL.to_string(),
            default_worker_image: crate::controller::DEFAULT_WORKER_IMAGE.to_string(),
            reconcile_interval: Duration::from_secs(30),
            claim_renew_margin: Duration::from_secs(60),
            acceptor_id: None,
            worker_restart_limit: 5,
        }
    }
}

impl OperatorConfig {
    /// Build configuration from the process environment.
    ///
    /// CR-soon nikhil: parse `METRICS_ADDR`, `WATCH_NAMESPACE`, `DEVIN_API_URL`,
    /// `DEVIN_WORKER_IMAGE`, `ACCEPTOR_ID`, etc.
    pub fn from_env() -> crate::Result<Self> {
        // Scaffold: real env parsing comes with the runtime implementation.
        Ok(Self::default())
    }
}
