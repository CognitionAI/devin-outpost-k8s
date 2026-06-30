//! Prometheus metrics and the metrics/health HTTP server.
//!
//! Uses `prometheus-client` and exposes an OpenMetrics endpoint. The Helm chart
//! ships an optional `ServiceMonitor`.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::Router;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use tokio::sync::Mutex;

/// Label set distinguishing per-pool metrics.
#[derive(Clone, Debug, Hash, PartialEq, Eq, prometheus_client::encoding::EncodeLabelSet)]
pub struct PoolLabels {
    /// The upstream pool ID.
    pub pool_id: String,
}

/// Handles to all operator metrics, plus the registry used to encode them.
#[derive(Debug)]
pub struct Metrics {
    /// The registry backing [`Self::encode`].
    pub registry: Registry,
    /// Total reconcile invocations, by pool.
    pub reconciliations: Family<PoolLabels, Counter>,
    /// Total reconcile failures, by pool.
    pub reconcile_failures: Family<PoolLabels, Counter>,
    /// Total sessions successfully claimed, by pool.
    pub sessions_claimed: Family<PoolLabels, Counter>,
    /// Total claim conflicts (lost races), by pool.
    pub claim_conflicts: Family<PoolLabels, Counter>,
    /// Currently running worker pods, by pool.
    pub active_workers: Family<PoolLabels, Gauge>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    /// Build the metric families and register them.
    pub fn new() -> Self {
        let mut registry = Registry::with_prefix("outposts");

        let reconciliations = Family::<PoolLabels, Counter>::default();
        let reconcile_failures = Family::<PoolLabels, Counter>::default();
        let sessions_claimed = Family::<PoolLabels, Counter>::default();
        let claim_conflicts = Family::<PoolLabels, Counter>::default();
        let active_workers = Family::<PoolLabels, Gauge>::default();

        registry.register(
            "reconciliations",
            "Total OutpostPool reconcile invocations",
            reconciliations.clone(),
        );
        registry.register(
            "reconcile_failures",
            "Total OutpostPool reconcile failures",
            reconcile_failures.clone(),
        );
        registry.register(
            "sessions_claimed",
            "Total sessions claimed from the upstream queue",
            sessions_claimed.clone(),
        );
        registry.register(
            "claim_conflicts",
            "Total claim conflicts (races lost to other workers)",
            claim_conflicts.clone(),
        );
        registry.register(
            "active_workers",
            "Worker pods currently running",
            active_workers.clone(),
        );

        Self {
            registry,
            reconciliations,
            reconcile_failures,
            sessions_claimed,
            claim_conflicts,
            active_workers,
        }
    }

    /// Encode the registry into the OpenMetrics text exposition format.
    pub fn encode(&self) -> Result<String, std::fmt::Error> {
        let mut buf = String::new();
        encode(&mut buf, &self.registry)?;
        Ok(buf)
    }
}

/// Shared state for the metrics/health server.
#[derive(Clone)]
struct ServerState {
    metrics: Arc<Mutex<Arc<Metrics>>>,
}

/// Serve `/metrics`, `/healthz` and `/readyz` on `addr` until cancelled.
///
/// The metrics handle is shared with the controller so counters increment as it
/// runs.
pub async fn serve(addr: std::net::SocketAddr, metrics: Arc<Metrics>) -> crate::Result<()> {
    let state = ServerState {
        metrics: Arc::new(Mutex::new(metrics)),
    };

    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/healthz", get(|| async { StatusCode::OK }))
        .route("/readyz", get(|| async { StatusCode::OK }))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| crate::Error::Config(format!("bind metrics addr {addr}: {e}")))?;
    tracing::info!(%addr, "metrics server listening");
    axum::serve(listener, app)
        .await
        .map_err(|e| crate::Error::Config(format!("metrics server: {e}")))?;
    Ok(())
}

async fn metrics_handler(State(state): State<ServerState>) -> (StatusCode, String) {
    let metrics = state.metrics.lock().await;
    match metrics.encode() {
        Ok(body) => (StatusCode::OK, body),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}
