//! No-op snapshot provider used by the `StartFresh` resume policy.

use async_trait::async_trait;

use crate::error::Result;

use super::{PreparedSession, SnapshotOutcome, SnapshotProvider};

/// Does nothing: resumes always start fresh. Fully portable. The session
/// still resumes (the cloud brain owns resume semantics); only the worker's
/// filesystem state is gone.
#[derive(Debug, Default, Clone)]
pub struct NoopSnapshotProvider;

#[async_trait]
impl SnapshotProvider for NoopSnapshotProvider {
    fn name(&self) -> &'static str {
        "noop"
    }

    async fn prepare(&self, _session_id: &str) -> Result<PreparedSession> {
        Ok(PreparedSession::default())
    }

    async fn on_suspend(
        &self,
        _session_id: &str,
        _pod: &k8s_openapi::api::core::v1::Pod,
    ) -> Result<SnapshotOutcome> {
        Ok(SnapshotOutcome::Ready)
    }

    async fn on_terminate(&self, _session_id: &str) -> Result<()> {
        Ok(())
    }

    async fn gc(&self) -> Result<()> {
        Ok(())
    }
}
