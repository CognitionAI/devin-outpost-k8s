//! Entry point for the `devin-outposts-k8s` binary.
//!
//! Wires together telemetry, the metrics server and the `OutpostPool`
//! controller. The controller logic itself is a scaffold (see
//! [`devin_outposts_k8s::controller`]).

use std::sync::Arc;

use devin_outposts_k8s::config::OperatorConfig;
use devin_outposts_k8s::controller::{self, Context};
use devin_outposts_k8s::metrics::{self, Metrics};
use devin_outposts_k8s::telemetry;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry::init();

    let config = OperatorConfig::from_env()?;
    tracing::info!(?config, "starting devin-outposts-k8s");

    let client = kube::Client::try_default().await?;

    let metrics = Arc::new(Metrics::new());
    let metrics_addr = config.metrics_addr;

    let metrics_server = {
        let metrics = metrics.clone();
        tokio::spawn(async move { metrics::serve(metrics_addr, metrics).await })
    };

    let ctx = Context {
        client,
        config: Arc::new(config),
        metrics,
    };

    tokio::select! {
        res = controller::run(ctx) => {
            res?;
        }
        res = metrics_server => {
            res??;
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("received ctrl-c, shutting down");
        }
    }

    Ok(())
}
