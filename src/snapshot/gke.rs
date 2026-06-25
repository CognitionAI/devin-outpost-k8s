//! GKE pod-snapshot provider (scaffold) for the `GkeSnapshot` resume policy.
//!
//! Backed by GKE Pod snapshots:
//! <https://docs.cloud.google.com/kubernetes-engine/docs/concepts/pod-snapshots>
//!
//! TODO: snapshot a suspended worker pod and restore it on resume, honouring the
//! pool's [`crate::crd::SnapshotPolicy`] (enablement + TTL). GKE only.

use async_trait::async_trait;

use crate::error::{Error, Result};

use super::{SnapshotHandle, SnapshotProvider};

/// Snapshot provider backed by GKE Pod snapshots.
#[derive(Debug, Default, Clone)]
pub struct GkeSnapshotProvider;

#[async_trait]
impl SnapshotProvider for GkeSnapshotProvider {
    fn name(&self) -> &'static str {
        "gke-pod-snapshot"
    }

    async fn snapshot(&self, _session_id: &str) -> Result<SnapshotHandle> {
        Err(Error::todo("GkeSnapshotProvider::snapshot"))
    }

    async fn restore(&self, _session_id: &str) -> Result<Option<SnapshotHandle>> {
        Err(Error::todo("GkeSnapshotProvider::restore"))
    }

    async fn delete(&self, _handle: &SnapshotHandle) -> Result<()> {
        Err(Error::todo("GkeSnapshotProvider::delete"))
    }
}
