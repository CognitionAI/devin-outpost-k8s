//! Cloud-portable snapshot/restore abstraction used to back `resume` sessions.
//!
//! The [`crate::crd::ResumePolicy`] alone selects which provider a pool uses
//! and whether snapshots are taken at all. When and why providers are invoked
//! is documented in the lifecycle mapping in [`crate::controller`]:
//!
//! - [`SnapshotProvider::prepare`] runs before every worker pod is created
//!   (new *and* resume sessions), setting up whatever per-session state the
//!   policy needs — a state volume, a snapshot policy object, or nothing.
//!   Restores are driven from here too: providers arrange for the recreated
//!   pod to come back with the suspended session's state.
//! - [`SnapshotProvider::on_suspend`] runs when a session reports
//!   `session_status = suspended`, while the worker pod still exists. The pod
//!   is only deleted once it reports [`SnapshotOutcome::Ready`], so slow
//!   snapshots hold the pod alive across reconciles instead of blocking one.
//! - [`SnapshotProvider::on_terminate`] runs when the session ends for good
//!   and deletes any per-session artifacts.
//! - [`SnapshotProvider::gc`] runs on every pool reconcile and enforces
//!   `resume.snapshotTtlSeconds` on suspended-session artifacts.

mod filesystem;
mod gke;
mod noop;

pub use filesystem::FilesystemSnapshotProvider;
pub use gke::GkeSnapshotProvider;
pub use noop::NoopSnapshotProvider;

use std::sync::Arc;

use async_trait::async_trait;
use kube::Client;

use crate::crd::{OutpostPool, ResumePolicy};
use crate::error::Result;

/// Result of a snapshot attempt (see [`SnapshotProvider::on_suspend`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotOutcome {
    /// The snapshot is durable; the worker pod may be deleted.
    Ready,
    /// The snapshot is still being taken; keep the pod and retry shortly.
    InProgress,
}

/// Abstraction over a cloud/cluster's snapshot mechanism.
///
/// Implementors persist and restore the state of a worker pod so a `resume`
/// session can pick up where a suspended one left off.
#[async_trait]
pub trait SnapshotProvider: Send + Sync {
    /// Human-readable provider name (for logs/metrics).
    fn name(&self) -> &'static str;

    /// Set up per-session state before the worker pod is created. Returns the
    /// name of a `PersistentVolumeClaim` the pod must mount at
    /// [`crate::controller::WORKER_DATA_DIR`], if the policy keeps one.
    async fn prepare(&self, session_id: &str) -> Result<Option<String>>;

    /// Take (or continue taking) a snapshot of the worker for `session_id`.
    /// Invoked on every reconcile while the session is suspended and its pod
    /// still exists; must be idempotent.
    async fn on_suspend(
        &self,
        session_id: &str,
        pod: &k8s_openapi::api::core::v1::Pod,
    ) -> Result<SnapshotOutcome>;

    /// Delete per-session artifacts once the session is terminated.
    async fn on_terminate(&self, session_id: &str) -> Result<()>;

    /// Garbage-collect artifacts of suspended sessions past the pool's
    /// `resume.snapshotTtlSeconds`.
    async fn gc(&self) -> Result<()>;
}

/// Construct the snapshot provider implied by a pool's [`ResumePolicy`].
pub fn provider_for(client: Client, pool: Arc<OutpostPool>) -> Box<dyn SnapshotProvider> {
    match pool.spec.resume.policy {
        ResumePolicy::StartFresh => Box::new(NoopSnapshotProvider),
        ResumePolicy::GkeSnapshot => Box::new(GkeSnapshotProvider::new(client, pool)),
        ResumePolicy::FilesystemSnapshot => Box::new(FilesystemSnapshotProvider::new(client, pool)),
    }
}
