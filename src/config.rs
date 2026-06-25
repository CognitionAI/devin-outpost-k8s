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

    /// Default upstream `/opbeta` API base URL when a pool does not override it.
    pub default_api_url: String,

    /// How long to wait between full re-list/reconcile passes of a pool's queue.
    pub reconcile_interval: Duration,

    /// Safety margin to renew a claim before its deadline expires.
    pub claim_renew_margin: Duration,
}

impl Default for OperatorConfig {
    fn default() -> Self {
        Self {
            metrics_addr: ([0, 0, 0, 0], 8080).into(),
            watch_namespace: None,
            default_api_url: crate::opbeta::DEFAULT_API_URL.to_string(),
            reconcile_interval: Duration::from_secs(30),
            claim_renew_margin: Duration::from_secs(60),
        }
    }
}

impl OperatorConfig {
    /// Build configuration from the process environment.
    ///
    /// TODO: parse `METRICS_ADDR`, `WATCH_NAMESPACE`, `DEVIN_API_URL`, etc.
    pub fn from_env() -> crate::Result<Self> {
        // Scaffold: real env parsing comes with the runtime implementation.
        Ok(Self::default())
    }
}
