//! Filesystem snapshot provider (scaffold) for the `FilesystemSnapshot` policy.
//!
//! Portable fallback for non-GKE clusters: persist the worker's state directory
//! to a retained volume / `VolumeSnapshot` and re-mount it on resume.
//!
//! TODO: implement using the CSI `VolumeSnapshot` API (or a retained
//! `PersistentVolumeClaim`), honouring the pool's snapshot TTL.

use async_trait::async_trait;

use crate::error::{Error, Result};

use super::{SnapshotHandle, SnapshotProvider};

/// Snapshot provider backed by a persistent filesystem / CSI VolumeSnapshot.
#[derive(Debug, Default, Clone)]
pub struct FilesystemSnapshotProvider;

#[async_trait]
impl SnapshotProvider for FilesystemSnapshotProvider {
    fn name(&self) -> &'static str {
        "filesystem"
    }

    async fn snapshot(&self, _session_id: &str) -> Result<SnapshotHandle> {
        Err(Error::todo("FilesystemSnapshotProvider::snapshot"))
    }

    async fn restore(&self, _session_id: &str) -> Result<Option<SnapshotHandle>> {
        Err(Error::todo("FilesystemSnapshotProvider::restore"))
    }

    async fn delete(&self, _handle: &SnapshotHandle) -> Result<()> {
        Err(Error::todo("FilesystemSnapshotProvider::delete"))
    }
}
