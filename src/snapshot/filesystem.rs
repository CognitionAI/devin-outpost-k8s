//! Filesystem snapshot provider for the `FilesystemSnapshot` policy.
//!
//! Portable fallback for non-GKE clusters: the worker's data directory lives
//! on a per-session `PersistentVolumeClaim` that is *retained* when the
//! session suspends and re-mounted by the recreated pod on resume — the
//! volume itself is the snapshot. Unlike GKE pod snapshots, only filesystem
//! state survives (process memory does not); the worker restarts and the
//! brain replays the session over the preserved state dir.

use std::sync::Arc;

use async_trait::async_trait;
use k8s_openapi::api::core::v1::PersistentVolumeClaim;
use kube::api::{Api, DeleteParams, ListParams, Patch, PatchParams};
use kube::{Client, Resource, ResourceExt};

use crate::controller::{
    ANNOTATION_SUSPENDED_AT, LABEL_POOL, LABEL_SESSION_ID, build_state_pvc, state_pvc_name,
};
use crate::crd::OutpostPool;
use crate::error::Result;

use super::{SnapshotOutcome, SnapshotProvider};

/// Snapshot provider backed by retained per-session `PersistentVolumeClaim`s.
#[derive(Clone)]
pub struct FilesystemSnapshotProvider {
    client: Client,
    pool: Arc<OutpostPool>,
}

impl FilesystemSnapshotProvider {
    /// Build a provider for one pool.
    pub fn new(client: Client, pool: Arc<OutpostPool>) -> Self {
        Self { client, pool }
    }

    fn pvcs(&self) -> Api<PersistentVolumeClaim> {
        Api::namespaced(
            self.client.clone(),
            self.pool.meta().namespace.as_deref().unwrap_or("default"),
        )
    }
}

#[async_trait]
impl SnapshotProvider for FilesystemSnapshotProvider {
    fn name(&self) -> &'static str {
        "filesystem"
    }

    /// Ensure the per-session state PVC exists and is not marked suspended
    /// (the session is live again, so TTL GC must leave it alone).
    ///
    /// An existing PVC is reused as-is — its spec is immutable, and changed
    /// pool volume settings only affect volumes created afterwards.
    async fn prepare(&self, session_id: &str) -> Result<Option<String>> {
        let name = state_pvc_name(session_id);
        match self.pvcs().get_opt(&name).await? {
            Some(existing) => {
                if existing.annotations().contains_key(ANNOTATION_SUSPENDED_AT) {
                    let patch = serde_json::json!({
                        "metadata": {"annotations": {ANNOTATION_SUSPENDED_AT: null}}
                    });
                    self.pvcs()
                        .patch(&name, &PatchParams::default(), &Patch::Merge(&patch))
                        .await?;
                }
            }
            None => {
                let pvc = build_state_pvc(&self.pool, session_id, &self.pool.spec.resume)?;
                match self.pvcs().create(&Default::default(), &pvc).await {
                    Ok(_) => {}
                    Err(kube::Error::Api(e)) if e.code == 409 => {}
                    Err(e) => return Err(e.into()),
                }
            }
        }
        Ok(Some(name))
    }

    /// Data is already durable on the PVC; just timestamp it for TTL GC.
    async fn on_suspend(
        &self,
        session_id: &str,
        _pod: &k8s_openapi::api::core::v1::Pod,
    ) -> Result<SnapshotOutcome> {
        let patch = serde_json::json!({
            "metadata": {
                "annotations": {
                    ANNOTATION_SUSPENDED_AT: chrono::Utc::now().to_rfc3339(),
                }
            }
        });
        self.pvcs()
            .patch(
                &state_pvc_name(session_id),
                &PatchParams::default(),
                &Patch::Merge(&patch),
            )
            .await?;
        Ok(SnapshotOutcome::Ready)
    }

    async fn on_terminate(&self, session_id: &str) -> Result<()> {
        match self
            .pvcs()
            .delete(&state_pvc_name(session_id), &DeleteParams::default())
            .await
        {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(e)) if e.code == 404 => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Delete suspended-session PVCs older than `resume.snapshotTtlSeconds`.
    async fn gc(&self) -> Result<()> {
        let Some(ttl) = self.pool.spec.resume.snapshot_ttl_seconds else {
            return Ok(());
        };
        let selector = format!("{LABEL_POOL}={}", self.pool.name_any());
        let pvcs = self
            .pvcs()
            .list(&ListParams::default().labels(&selector))
            .await?;
        let now = chrono::Utc::now();
        for pvc in pvcs {
            let Some(suspended_at) = pvc
                .annotations()
                .get(ANNOTATION_SUSPENDED_AT)
                .and_then(|raw| chrono::DateTime::parse_from_rfc3339(raw).ok())
            else {
                continue;
            };
            if (now - suspended_at.with_timezone(&chrono::Utc)).num_seconds() < ttl as i64 {
                continue;
            }
            let name = pvc.name_any();
            tracing::info!(
                pvc = %name,
                session = pvc.labels().get(LABEL_SESSION_ID).map(String::as_str),
                "state volume past snapshot TTL; deleting"
            );
            match self.pvcs().delete(&name, &DeleteParams::default()).await {
                Ok(_) => {}
                Err(kube::Error::Api(e)) if e.code == 404 => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }
}
