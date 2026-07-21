//! `coordination.k8s.io/Lease`-based leader election.
//!
//! The operator renews upstream claims centrally, so only one replica may act
//! at a time — but downtime longer than the claim TTL (~5 min) hands sessions
//! to other workers while their pods still run. Running several replicas with
//! Lease failover keeps takeover well inside the TTL.
//!
//! Semantics follow client-go's leaderelection: candidates poll the lease and
//! take it over once the previous holder's `renewTime` is older than
//! [`LEASE_DURATION`]; the holder renews every [`RENEW_INTERVAL`] and, if it
//! cannot renew for a full lease duration, *exits the process* so Kubernetes
//! restarts it as a candidate. Exiting is deliberate: the controller has no
//! way to un-observe state mid-flight, so a clean restart is the safe way to
//! drop leadership.

use std::time::Duration;

use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime;
use kube::Client;
use kube::api::{Api, ObjectMeta, PostParams};
use tracing::{debug, info, warn};

use crate::error::{Error, Result};

/// Name of the operator's leader-election lease.
pub const LEASE_NAME: &str = "devin-outposts-k8s-leader";

/// A holder's claim on the lease expires this long after its last renewal.
const LEASE_DURATION: Duration = Duration::from_secs(15);
/// How often the holder renews (and candidates poll) the lease.
const RENEW_INTERVAL: Duration = Duration::from_secs(5);

/// Wait until this process holds the leadership lease.
///
/// Returns a [`Leadership`] whose [`Leadership::hold`] future must be driven
/// for as long as the process acts as leader.
pub async fn become_leader(client: Client, namespace: &str, identity: &str) -> Result<Leadership> {
    let leases: Api<Lease> = Api::namespaced(client, namespace);
    loop {
        match try_acquire(&leases, identity).await {
            Ok(true) => {
                info!(identity, "acquired leadership");
                return Ok(Leadership {
                    leases,
                    identity: identity.to_string(),
                });
            }
            Ok(false) => debug!(identity, "leadership held elsewhere; waiting"),
            Err(err) => warn!(%err, "leader election attempt failed"),
        }
        tokio::time::sleep(RENEW_INTERVAL).await;
    }
}

/// A held leadership lease.
pub struct Leadership {
    leases: Api<Lease>,
    identity: String,
}

impl Leadership {
    /// Renew the lease until renewal fails for a full [`LEASE_DURATION`] or
    /// another holder takes it, then return. The caller is expected to exit
    /// the process (see the module docs).
    pub async fn hold(self) -> Error {
        let mut last_renewed = std::time::Instant::now();
        loop {
            tokio::time::sleep(RENEW_INTERVAL).await;
            match try_acquire(&self.leases, &self.identity).await {
                Ok(true) => last_renewed = std::time::Instant::now(),
                Ok(false) => {
                    return Error::Config(format!(
                        "lost leadership: lease {LEASE_NAME} taken by another holder"
                    ));
                }
                Err(err) => {
                    warn!(%err, "lease renewal failed");
                    if last_renewed.elapsed() >= LEASE_DURATION {
                        return err;
                    }
                }
            }
        }
    }
}

/// Acquire or renew the lease for `identity`. Returns `false` when another
/// holder's claim is still fresh. Optimistic concurrency via
/// `resourceVersion` makes concurrent takeovers race safely.
async fn try_acquire(leases: &Api<Lease>, identity: &str) -> Result<bool> {
    let now = MicroTime(k8s_openapi::jiff::Timestamp::now());
    let Some(mut lease) = leases.get_opt(LEASE_NAME).await? else {
        let lease = Lease {
            metadata: ObjectMeta {
                name: Some(LEASE_NAME.to_string()),
                ..Default::default()
            },
            spec: Some(LeaseSpec {
                holder_identity: Some(identity.to_string()),
                lease_duration_seconds: Some(LEASE_DURATION.as_secs() as i32),
                acquire_time: Some(now.clone()),
                renew_time: Some(now),
                lease_transitions: Some(0),
                ..Default::default()
            }),
        };
        return match leases.create(&PostParams::default(), &lease).await {
            Ok(_) => Ok(true),
            Err(kube::Error::Api(e)) if e.code == 409 => Ok(false),
            Err(e) => Err(e.into()),
        };
    };

    let spec = lease.spec.get_or_insert_default();
    let held_by_us = spec.holder_identity.as_deref() == Some(identity);
    let expired = spec.renew_time.as_ref().is_none_or(|t| {
        k8s_openapi::jiff::Timestamp::now().as_millisecond() - t.0.as_millisecond()
            >= LEASE_DURATION.as_millis() as i64
    });
    if !held_by_us && !expired {
        return Ok(false);
    }

    if !held_by_us {
        spec.holder_identity = Some(identity.to_string());
        spec.acquire_time = Some(now.clone());
        spec.lease_transitions = Some(spec.lease_transitions.unwrap_or(0) + 1);
    }
    spec.lease_duration_seconds = Some(LEASE_DURATION.as_secs() as i32);
    spec.renew_time = Some(now);

    match leases
        .replace(LEASE_NAME, &PostParams::default(), &lease)
        .await
    {
        Ok(_) => Ok(true),
        // Lost the optimistic-concurrency race to another candidate.
        Err(kube::Error::Api(e)) if e.code == 409 => Ok(false),
        Err(e) => Err(e.into()),
    }
}
