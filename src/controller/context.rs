//! Shared reconcile context handed to every reconcile invocation.

use std::sync::Arc;

use kube::Client;
use kube::api::{Api, ObjectMeta, Patch, PatchParams};

use crate::config::OperatorConfig;
use crate::metrics::Metrics;
use crate::network_policy::{Enforcement, Provider};

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
    /// Detected provider (see [`Context::network_policy_provider`]).
    detected_network_policy_provider: Arc<std::sync::OnceLock<Provider>>,
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
            detected_network_policy_provider: Arc::default(),
        }
    }

    /// The provider to enforce session network policies through, if any, per
    /// [`OperatorConfig::network_policy_enforcement`]. Only a successful
    /// detection is remembered, so enabling a provider on the cluster later
    /// takes effect without a restart.
    pub async fn network_policy_provider(&self) -> crate::Result<Option<Provider>> {
        match self.config.network_policy_enforcement {
            Enforcement::Off => return Ok(None),
            Enforcement::Through(provider) => return Ok(Some(provider)),
            Enforcement::Auto => {}
        }
        if let Some(provider) = self.detected_network_policy_provider.get() {
            return Ok(Some(*provider));
        }
        let detected = Provider::detect(&self.client).await?;
        if let Some(provider) = detected {
            tracing::info!(?provider, "detected a network policy provider");
            let _ = self.detected_network_policy_provider.set(provider);
        }
        Ok(detected)
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
