//! No-op snapshot provider used by the `StartFresh` resume policy.

use async_trait::async_trait;

use crate::error::Result;

use super::{SnapshotHandle, SnapshotProvider};

/// Does nothing: resumes always start fresh. Fully portable.
#[derive(Debug, Default, Clone)]
pub struct NoopSnapshotProvider;

#[async_trait]
impl SnapshotProvider for NoopSnapshotProvider {
    fn name(&self) -> &'static str {
        "noop"
    }

    async fn snapshot(&self, _session_id: &str) -> Result<SnapshotHandle> {
        Ok(SnapshotHandle(String::new()))
    }

    async fn restore(&self, _session_id: &str) -> Result<Option<SnapshotHandle>> {
        Ok(None)
    }

    async fn delete(&self, _handle: &SnapshotHandle) -> Result<()> {
        Ok(())
    }
}
