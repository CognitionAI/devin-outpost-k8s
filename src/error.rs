//! The crate-wide error type.

use thiserror::Error;

/// Result alias used throughout the operator.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors produced by the operator.
#[derive(Debug, Error)]
pub enum Error {
    /// A feature that is scaffolded but not yet implemented was invoked.
    #[error("not implemented yet: {0}")]
    NotImplemented(&'static str),

    /// Error talking to the Kubernetes API server.
    #[error("k8s api error: {0}")]
    Kube(#[from] kube::Error),

    /// Error talking to the upstream `/opbeta` Outposts queue API.
    #[error("api error: {0}")]
    Api(#[from] reqwest::Error),

    /// A claim conflicted with another worker (HTTP 409).
    ///
    /// The session was claimed by someone else between list and claim; the
    /// reconciler should drop it and move on.
    #[error("claim conflict for session {session_id}")]
    ClaimConflict {
        /// The session that could not be claimed.
        session_id: String,
    },

    /// Invalid or missing operator configuration.
    #[error("configuration error: {0}")]
    Config(String),

    /// A referenced Kubernetes `Secret` (e.g. the pool PAT) was missing a key.
    #[error("secret {name:?} missing key {key:?}")]
    MissingSecretKey {
        /// Secret name.
        name: String,
        /// Expected key.
        key: String,
    },

    /// JSON (de)serialization failure.
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    /// Any other error.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl Error {
    /// Convenience constructor for [`Error::NotImplemented`].
    pub fn todo(what: &'static str) -> Self {
        Error::NotImplemented(what)
    }
}
