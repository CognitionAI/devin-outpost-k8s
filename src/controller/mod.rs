//! The reconcile loop and per-session `Pod` template builder.
//!
//! 1. A `kube::runtime::Controller` watches [`crate::crd::OutpostPool`]
//!    resources (and owned worker `Pod`s).
//! 2. For each pool, the operator authenticates to the upstream `/opbeta` queue
//!    with the pool's PAT and lists/watches pending sessions.
//! 3. Up to `maxConcurrentSessions`, it claims pending sessions; for each claim
//!    it creates a per-session token `Secret` and an owned worker `Pod` (see
//!    [`pod`] for the worker contract).
//! 4. The operator renews claims centrally before their deadline (claim TTL
//!    ~5 min); worker pods never hold the pool PAT. Operator downtime longer
//!    than the TTL returns sessions to the queue while their pods still run, so
//!    the deployment is meant to run leader-elected replicas (a
//!    `coordination.k8s.io/Lease`) with failover well inside the TTL.
//!    CR-soon nikhil: leader election is not implemented yet; until then the
//!    chart enforces `replicaCount: 1`.
//!
//! ## Session ↔ pod lifecycle mapping
//!
//! - Worker exits `0` (`--once` = session complete) → release the claim, delete
//!   the pod + token secret.
//! - Non-zero exit → the kubelet restarts it in place (`restartPolicy:
//!   OnFailure`, built-in backoff, no re-scheduling latency, and the token
//!   doesn't expire so the same `Secret` keeps working). Past
//!   [`crate::config::OperatorConfig::worker_restart_limit`] the operator gives
//!   up: release the claim + delete the pod so the session isn't held hostage
//!   by a broken node/image.
//! - `session_status = suspended` → snapshot per the pool's
//!   [`crate::crd::ResumePolicy`] (no-op for `StartFresh`), then delete the pod.
//! - Pool deleted → [`POOL_FINALIZER`] releases in-flight claims and drains
//!   worker pods before the resource is removed.
//!
//! ## Operator state
//!
//! Deliberately no `Session`-like CRD: everything the operator needs is
//! reconstructible from list calls, so a cache of it would only add sync bugs.
//! Which sessions are ours comes from the upstream queue filtered by our
//! acceptor ID; session↔pod/secret/snapshot mappings come from labels + owner
//! references on those objects. Revisit if per-session state with no natural
//! Kubernetes home shows up.

mod context;
mod pod;
mod reconcile;

pub use context::Context;
pub use pod::{DEFAULT_WORKER_IMAGE, build_session_token_secret, build_worker_pod};
pub use reconcile::{error_policy, reconcile, run};

/// Finalizer the operator adds to every [`crate::crd::OutpostPool`]; see the
/// lifecycle mapping above for what removal entails.
pub const POOL_FINALIZER: &str = "outposts.cognition.com/pool-cleanup";
