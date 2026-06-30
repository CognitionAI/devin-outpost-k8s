//! Cloud-portable snapshot/restore abstraction used to back `resume` sessions.
//!
//! The [`crate::crd::ResumePolicy`] selects which provider a pool uses:

mod filesystem;
mod gke;
mod noop;

pub use filesystem::FilesystemSnapshotProvider;
pub use gke::GkeSnapshotProvider;
pub use noop::NoopSnapshotProvider;

use async_trait::async_trait;

use crate::crd::{OutpostPool, ResumePolicy};
use crate::error::Result;

/// An opaque handle identifying a persisted snapshot for a session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotHandle(pub String);

/// Abstraction over a cloud/cluster's snapshot mechanism.
///
/// Implementors persist and restore the state of a worker pod so a `resume`
/// session can pick up where a suspended one left off.
#[async_trait]
pub trait SnapshotProvider: Send + Sync {
    /// Human-readable provider name (for logs/metrics).
    fn name(&self) -> &'static str;

    /// Take a snapshot of the worker for `session_id`, returning its handle.
    async fn snapshot(&self, session_id: &str) -> Result<SnapshotHandle>;

    /// Restore a previously taken snapshot, if one exists for `session_id`.
    async fn restore(&self, session_id: &str) -> Result<Option<SnapshotHandle>>;

    /// Delete a snapshot (e.g. on TTL expiry or session termination).
    async fn delete(&self, handle: &SnapshotHandle) -> Result<()>;
}

/// Construct the snapshot provider implied by a pool's [`ResumePolicy`].
pub fn provider_for(pool: &OutpostPool) -> Box<dyn SnapshotProvider> {
    match pool.spec.resume_policy {
        ResumePolicy::StartFresh => Box::new(NoopSnapshotProvider),
        ResumePolicy::GkeSnapshot => Box::new(GkeSnapshotProvider),
        ResumePolicy::FilesystemSnapshot => Box::new(FilesystemSnapshotProvider),
    }
}
