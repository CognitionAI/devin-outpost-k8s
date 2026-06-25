//! The reconcile loop and per-session `Pod` template builder.
//!
//! High level design (see `docs/ARCHITECTURE.md`):
//!
//! 1. A `kube::runtime::Controller` watches [`crate::crd::OutpostPool`]
//!    resources (and owned worker `Pod`s).
//! 2. For each pool, the operator authenticates to the upstream `/opbeta` queue
//!    with the pool's PAT and lists/watches pending sessions.
//! 3. Up to `maxConcurrentSessions`, it claims pending sessions and creates an
//!    owned worker `Pod` (built by [`pod`]) per claimed session.
//! 4. It renews claims before their deadline, and releases / tears down pods as
//!    sessions terminate.
//!
//! Everything here is a scaffold; reconcile bodies return early / TODO.

mod context;
mod pod;
mod reconcile;

pub use context::Context;
pub use pod::build_worker_pod;
pub use reconcile::{error_policy, reconcile, run};
