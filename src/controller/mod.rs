//! The reconcile loop and per-session `Pod` template builder.
//!
//! 1. A `kube::runtime::Controller` watches [`crate::crd::OutpostPool`]
//!    resources and owned worker `Pod`s. Each pool also gets a long-lived
//!    upstream queue watcher ([`queue_watch`]) that edge-triggers reconciles;
//!    the reconcile itself is level-based (re-list and converge), so watch
//!    hiccups degrade latency, never correctness.
//! 2. For each pool, the operator authenticates to the upstream `/opbeta` queue
//!    with the pool's PAT and lists pending sessions.
//! 3. Up to `maxConcurrentSessions`, it claims pending sessions; for each claim
//!    it creates a per-session token `Secret` and an owned worker `Pod` (see
//!    [`pod`] for the worker contract). What to do on each pass is decided by
//!    the pure planner in [`plan`].
//! 4. The operator renews claims centrally before their deadline (claim TTL
//!    ~5 min); worker pods never hold the pool PAT. Operator downtime longer
//!    than the TTL returns sessions to the queue while their pods still run, so
//!    the deployment runs leader-elected replicas (see [`crate::elector`]) with
//!    failover well inside the TTL.
//!
//! ## Session ↔ pod lifecycle mapping
//!
//! The worker serves in direct mode and never talks to the queue API, so the
//! *operator* drives every transition off the queue object's
//! `status.session_status`:
//!
//! - `terminated` → release the claim, delete the pod + token secret, delete
//!   snapshot artifacts.
//! - `suspended` → snapshot per the pool's [`crate::crd::ResumePolicy`]
//!   (no-op for `StartFresh`; the pod is kept alive until the snapshot is
//!   durable), then delete the pod + secret and release the claim. A later
//!   `resume`-kind claim recreates the pod restored per the policy.
//! - Worker exits non-zero → the kubelet restarts it in place
//!   (`restartPolicy: OnFailure`, built-in backoff, no re-scheduling latency,
//!   and the connect token outlives the session so the same `Secret` keeps
//!   working). Past [`crate::config::OperatorConfig::worker_restart_limit`]
//!   the operator gives up: release the claim + delete the pod so the session
//!   isn't held hostage by a broken node/image.
//! - Worker exits zero while the session is live → delete the pod; the next
//!   pass recreates it with a freshly renewed connect token.
//! - Pool deleted → [`POOL_FINALIZER`] releases in-flight claims (so sessions
//!   requeue immediately instead of waiting out the claim TTL); owned pods,
//!   secrets, volumes and snapshot resources are garbage-collected via owner
//!   references.
//!
//! ## Operator state
//!
//! Deliberately no `Session`-like CRD: everything the operator needs is
//! reconstructible from list calls, so a cache of it would only add sync bugs.
//! Which sessions are ours comes from the upstream queue filtered by our
//! acceptor ID; session↔pod/secret/volume mappings come from deterministic
//! object names + owner references on those objects. Revisit if per-session
//! state with no natural Kubernetes home shows up.

mod context;
mod plan;
mod pod;
mod queue_watch;
mod reconcile;

pub use context::Context;
pub use plan::{Action, Observed, next_claim_deadline, plan, pod_restart_count};
pub use pod::{
    ANNOTATION_POOL_ID, ANNOTATION_SUSPENDED_AT, DEFAULT_WORKER_IMAGE, ENV_GATEWAY_URL,
    ENV_REMOTE_BINARY_SHA, ENV_SESSION_TOKEN, ENV_WORKER_CACHE_DIR, LABEL_MANAGED_BY, LABEL_POOL,
    LABEL_SESSION_ID, SESSION_TOKEN_SECRET_KEY, WORKER_COMMAND, WORKER_DATA_DIR, WorkerPodParams,
    build_session_token_secret, build_state_pvc, build_worker_pod, session_labels,
    session_token_secret_name, state_pvc_name, worker_pod_name,
};
pub use queue_watch::PoolWatchers;
pub use reconcile::{error_policy, reconcile, run};

/// Finalizer the operator adds to every [`crate::crd::OutpostPool`]; see the
/// lifecycle mapping above for what removal entails.
pub const POOL_FINALIZER: &str = "outposts.cognition.com/pool-cleanup";
