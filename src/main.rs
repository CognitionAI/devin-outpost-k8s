//! Entry point for the `outposts-operator` binary.
//!
//! Wires together telemetry, the metrics server and the `OutpostPool`
//! controller. The controller logic itself is a scaffold (see
//! [`outposts_operator::controller`]).

use std::sync::Arc;

use outposts_operator::config::OperatorConfig;
use outposts_operator::controller::{self, Context};
use outposts_operator::metrics::{self, Metrics};
use outposts_operator::telemetry;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry::init();

    let config = OperatorConfig::from_env()?;
    tracing::info!(?config, "starting outposts-operator");

    let client = kube::Client::try_default().await?;

    let metrics = Arc::new(Metrics::new());
    let metrics_addr = config.metrics_addr;

    // Metrics/health server runs alongside the controller.
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
