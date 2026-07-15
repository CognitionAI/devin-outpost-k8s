//! The crate-wide error type.

use thiserror::Error;

/// Result alias used throughout the operator.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors produced by the operator.
#[derive(Debug, Error)]
pub enum Error {
    /// Error talking to the Kubernetes API server.
    #[error("k8s api error: {0}")]
    Kube(#[from] kube::Error),

    /// Error talking to the upstream `/opbeta` Outposts queue API.
    #[error("api error: {0}")]
    Api(#[from] reqwest::Error),

    /// The upstream API returned a non-success HTTP status.
    #[error("api returned {status}: {message}")]
    ApiStatus {
        /// The HTTP status code.
        status: reqwest::StatusCode,
        /// The response body (usually a JSON `detail` message).
        message: String,
    },

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

    /// A pool's `spec.apiUrl` names a URL the operator does not trust. See
    /// [`crate::config::OperatorConfig::api_url_allowlist`].
    #[error("api url {url:?} is not the operator default and is not in the allowlist")]
    ApiUrlNotAllowed {
        /// The rejected URL.
        url: String,
    },

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
    /// Whether the upstream API rejected our credentials (401/403). Used to
    /// surface an `Unauthorized` pool phase instead of a generic failure.
    pub fn is_unauthorized(&self) -> bool {
        matches!(
            self,
            Error::ApiStatus { status, .. }
                if *status == reqwest::StatusCode::UNAUTHORIZED
                    || *status == reqwest::StatusCode::FORBIDDEN
        )
    }

    /// Whether the upstream API says the resource is gone (404). Claims and
    /// releases race with queue tombstoning, so callers often treat this as
    /// success.
    pub fn is_not_found(&self) -> bool {
        matches!(
            self,
            Error::ApiStatus { status, .. } if *status == reqwest::StatusCode::NOT_FOUND
        )
    }
}
