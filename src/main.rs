//! Entry point for the `devin-outposts-k8s` binary.
//!
//! Wires together telemetry, the metrics server, leader election and the
//! `OutpostPool` controller (see [`devin_outposts_k8s::controller`]).

use std::sync::Arc;

use devin_outposts_k8s::config::OperatorConfig;
use devin_outposts_k8s::metrics::{self, Metrics};
use devin_outposts_k8s::{controller, elector, telemetry};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry::init();

    // Both `ring` (via kube) and `aws-lc-rs` (via reqwest) are in the
    // dependency graph, so rustls cannot pick a process-level crypto
    // provider on its own.
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("no other crypto provider is installed before this");

    let config = Arc::new(OperatorConfig::from_env()?);
    tracing::info!(?config, "starting devin-outposts-k8s");

    let client = kube::Client::try_default().await?;

    // The metrics/health server runs on every replica (leader or not) so
    // probes and scrapes keep working while a replica waits in standby.
    let metrics = Arc::new(Metrics::new());
    let metrics_server = {
        let metrics = metrics.clone();
        let addr = config.metrics_addr;
        tokio::spawn(async move { metrics::serve(addr, metrics).await })
    };

    let leadership = if config.leader_election {
        let lease =
            elector::become_leader(client.clone(), &config.operator_namespace, &config.identity)
                .await?;
        futures::future::Either::Left(lease.hold())
    } else {
        futures::future::Either::Right(futures::future::pending())
    };

    tokio::select! {
        res = controller::run(client, config, metrics) => {
            res?;
        }
        res = metrics_server => {
            res??;
        }
        err = leadership => {
            // Exit so Kubernetes restarts this replica as a candidate; the
            // controller must not keep acting without the lease (see
            // [`devin_outposts_k8s::elector`]).
            return Err(err.into());
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("received ctrl-c, shutting down");
        }
    }

    Ok(())
}
