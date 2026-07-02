//! The `OutpostPool` reconcile loop (scaffold).

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use kube::api::Api;
use kube::runtime::Controller;
use kube::runtime::controller::Action;
use kube::runtime::watcher;
use tracing::{info, warn};

use crate::crd::OutpostPool;
use crate::error::Error;

use super::context::Context;

/// Run the controller until the process is asked to shut down.
///
/// Wires a `kube::runtime::Controller` over [`OutpostPool`] resources (and, once
/// implemented, owned worker `Pod`s) and drives [`reconcile`].
pub async fn run(ctx: Context) -> crate::Result<()> {
    let pools: Api<OutpostPool> = match &ctx.config.watch_namespace {
        Some(ns) => Api::namespaced(ctx.client.clone(), ns),
        None => Api::all(ctx.client.clone()),
    };

    info!("starting OutpostPool controller");

    let ctx = Arc::new(ctx);
    Controller::new(pools, watcher::Config::default())
        // CR-soon nikhil: `.owns::<Pod>(...)` once worker pods are created.
        .run(reconcile, error_policy, ctx)
        .for_each(|res| async move {
            match res {
                Ok((obj, _action)) => info!(?obj, "reconciled"),
                Err(err) => warn!(%err, "reconcile error"),
            }
        })
        .await;

    Ok(())
}

/// Reconcile a single [`OutpostPool`].
///
/// CR-soon nikhil: authenticate with the pool PAT, list/watch the upstream queue, claim up
/// to `maxConcurrentSessions`, create/renew/tear down worker pods per the
/// lifecycle mapping in [`crate::controller`], handle
/// [`crate::controller::POOL_FINALIZER`], update status.
pub async fn reconcile(pool: Arc<OutpostPool>, _ctx: Arc<Context>) -> crate::Result<Action> {
    warn!(
        pool = %pool.spec.pool_id,
        "reconcile is not implemented yet (scaffold)"
    );
    // Requeue so the loop is well-behaved even though it currently no-ops.
    Ok(Action::requeue(Duration::from_secs(300)))
}

/// Decide how to back off when [`reconcile`] returns an error.
pub fn error_policy(_pool: Arc<OutpostPool>, err: &Error, _ctx: Arc<Context>) -> Action {
    warn!(%err, "reconcile failed; requeueing");
    Action::requeue(Duration::from_secs(30))
}
