//! Shared reconcile context handed to every reconcile invocation.

use std::sync::Arc;

use kube::Client;

use crate::config::OperatorConfig;
use crate::metrics::Metrics;

/// Long-lived state shared across reconciles.
#[derive(Clone)]
pub struct Context {
    /// Kubernetes API client.
    pub client: Client,
    /// Operator-wide configuration.
    pub config: Arc<OperatorConfig>,
    /// Prometheus metrics handles.
    pub metrics: Arc<Metrics>,
}

impl Context {
    /// Build a new reconcile context.
    pub fn new(client: Client, config: OperatorConfig, metrics: Metrics) -> Self {
        Self {
            client,
            config: Arc::new(config),
            metrics: Arc::new(metrics),
        }
    }
}
