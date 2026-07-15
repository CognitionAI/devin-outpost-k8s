//! Process-level runtime configuration for the operator.
//!
//! Per-pool behaviour (token, VM size, runtime class, snapshot policy, ...)
//! lives on the [`crate::crd::OutpostPool`] custom resource. This struct only
//! holds settings that apply to the operator process as a whole.

use std::time::Duration;

/// Operator-wide runtime configuration, typically sourced from environment
/// variables / flags at startup.
#[derive(Debug, Clone)]
pub struct OperatorConfig {
    /// Address the Prometheus `/metrics` + health server binds to.
    pub metrics_addr: std::net::SocketAddr,

    /// Optional namespace to restrict the operator to. `None` => cluster-wide.
    pub watch_namespace: Option<String>,

    /// Default upstream API base URL when an [`crate::crd::OutpostPool`] does
    /// not override it.
    pub default_api_url: String,

    /// The URLs, besides [`Self::default_api_url`], a pool may select with
    /// `spec.apiUrl`. Anything else is rejected by [`Self::resolve_api_url`].
    ///
    /// A pool's `spec.apiUrl` is untrusted input (whoever can create an
    /// `OutpostPool` sets it) and the pool's token `Secret` is sent to that
    /// URL as a bearer credential, so accepting it verbatim makes the operator
    /// a confused deputy: it reads a namespace `Secret` with its own
    /// cluster-wide-read ServiceAccount and ships the plaintext value to an
    /// attacker-chosen host (and issues attacker-directed requests from inside
    /// the cluster network).
    pub api_url_allowlist: Vec<String>,

    /// Default worker image used when a pool's `worker.overrides.image` is
    /// unset. Pinned to the operator release (typically set via the Helm chart).
    pub default_worker_image: String,

    /// How long to wait between full re-list/reconcile passes of a pool's queue.
    pub reconcile_interval: Duration,

    /// Safety margin to renew a claim before its deadline expires.
    pub claim_renew_margin: Duration,

    /// Identity used when claiming sessions upstream. Must be unique within the
    /// Devin account and stable for this install (re-claiming with the same
    /// acceptor renews, so instability orphans claims for one TTL). `None` =>
    /// the operator read-or-creates a `ConfigMap` in its own namespace holding
    /// a generated `k8s-<namespace>-<random>` value. Generated at runtime
    /// rather than by the chart because template-time randomness regenerates on
    /// every `helm upgrade` (and the `lookup` workaround breaks `helm
    /// template`/GitOps).
    pub acceptor_id: Option<String>,

    /// Restarts of a worker pod (non-zero exits, restarted in place by the
    /// kubelet) after which the operator gives up on it: release the claim +
    /// delete the pod. See the lifecycle mapping in [`crate::controller`].
    pub worker_restart_limit: u32,

    /// Namespace this operator pod runs in. Holds operator-owned singletons:
    /// the acceptor-ID `ConfigMap` and the leader-election `Lease`.
    pub operator_namespace: String,

    /// Identity used for leader election (the pod name in-cluster).
    pub identity: String,

    /// Whether to run Lease-based leader election before serving. Disable
    /// only for local development against a cluster you exclusively own.
    pub leader_election: bool,
}

impl Default for OperatorConfig {
    fn default() -> Self {
        Self {
            metrics_addr: ([0, 0, 0, 0], 8080).into(),
            watch_namespace: None,
            default_api_url: crate::api::DEFAULT_API_URL.to_string(),
            api_url_allowlist: Vec::new(),
            default_worker_image: crate::controller::DEFAULT_WORKER_IMAGE.to_string(),
            reconcile_interval: Duration::from_secs(30),
            claim_renew_margin: Duration::from_secs(60),
            acceptor_id: None,
            worker_restart_limit: 5,
            operator_namespace: "default".to_string(),
            identity: "devin-outposts-k8s".to_string(),
            leader_election: true,
        }
    }
}

/// Path the service account admission controller mounts the pod's own
/// namespace at; used when `POD_NAMESPACE` isn't set explicitly.
const SERVICEACCOUNT_NAMESPACE_PATH: &str =
    "/var/run/secrets/kubernetes.io/serviceaccount/namespace";

impl OperatorConfig {
    /// Build configuration from the process environment.
    ///
    /// | variable | field |
    /// |---|---|
    /// | `METRICS_ADDR` (`":8080"` or `"host:port"`) | [`Self::metrics_addr`] |
    /// | `WATCH_NAMESPACE` | [`Self::watch_namespace`] |
    /// | `DEVIN_API_URL` | [`Self::default_api_url`] |
    /// | `DEVIN_API_URL_ALLOWLIST` (comma-separated) | [`Self::api_url_allowlist`] |
    /// | `DEVIN_WORKER_IMAGE` | [`Self::default_worker_image`] |
    /// | `RECONCILE_INTERVAL_SECONDS` | [`Self::reconcile_interval`] |
    /// | `CLAIM_RENEW_MARGIN_SECONDS` | [`Self::claim_renew_margin`] |
    /// | `ACCEPTOR_ID` | [`Self::acceptor_id`] |
    /// | `WORKER_RESTART_LIMIT` | [`Self::worker_restart_limit`] |
    /// | `POD_NAMESPACE` (downward API) | [`Self::operator_namespace`] |
    /// | `POD_NAME` (downward API) | [`Self::identity`] |
    /// | `LEADER_ELECTION` (`"false"` to disable) | [`Self::leader_election`] |
    ///
    /// Empty values are treated as unset.
    pub fn from_env() -> crate::Result<Self> {
        let defaults = Self::default();
        let get = |key: &str| std::env::var(key).ok().filter(|v| !v.is_empty());

        let metrics_addr = match get("METRICS_ADDR") {
            Some(raw) => {
                // Allow the common ":8080" shorthand for "bind all interfaces".
                let candidate = if raw.starts_with(':') {
                    format!("0.0.0.0{raw}")
                } else {
                    raw.clone()
                };
                candidate
                    .parse()
                    .map_err(|e| crate::Error::Config(format!("METRICS_ADDR {raw:?}: {e}")))?
            }
            None => defaults.metrics_addr,
        };

        let parse_secs = |key: &str, default: Duration| -> crate::Result<Duration> {
            match get(key) {
                Some(raw) => raw
                    .parse::<u64>()
                    .map(Duration::from_secs)
                    .map_err(|e| crate::Error::Config(format!("{key} {raw:?}: {e}"))),
                None => Ok(default),
            }
        };

        let worker_restart_limit = match get("WORKER_RESTART_LIMIT") {
            Some(raw) => raw
                .parse()
                .map_err(|e| crate::Error::Config(format!("WORKER_RESTART_LIMIT {raw:?}: {e}")))?,
            None => defaults.worker_restart_limit,
        };

        let operator_namespace = get("POD_NAMESPACE")
            .or_else(|| {
                std::fs::read_to_string(SERVICEACCOUNT_NAMESPACE_PATH)
                    .ok()
                    .map(|ns| ns.trim().to_string())
                    .filter(|ns| !ns.is_empty())
            })
            .unwrap_or(defaults.operator_namespace);

        Ok(Self {
            metrics_addr,
            watch_namespace: get("WATCH_NAMESPACE"),
            default_api_url: get("DEVIN_API_URL").unwrap_or(defaults.default_api_url),
            api_url_allowlist: get("DEVIN_API_URL_ALLOWLIST")
                .map(|raw| {
                    raw.split(',')
                        .map(str::trim)
                        .filter(|url| !url.is_empty())
                        .map(String::from)
                        .collect()
                })
                .unwrap_or(defaults.api_url_allowlist),
            default_worker_image: get("DEVIN_WORKER_IMAGE")
                .unwrap_or(defaults.default_worker_image),
            reconcile_interval: parse_secs(
                "RECONCILE_INTERVAL_SECONDS",
                defaults.reconcile_interval,
            )?,
            claim_renew_margin: parse_secs(
                "CLAIM_RENEW_MARGIN_SECONDS",
                defaults.claim_renew_margin,
            )?,
            acceptor_id: get("ACCEPTOR_ID"),
            worker_restart_limit,
            operator_namespace,
            identity: get("POD_NAME").unwrap_or(defaults.identity),
            leader_election: get("LEADER_ELECTION").as_deref() != Some("false"),
        })
    }

    /// The upstream API base URL to use for a pool whose `spec.apiUrl` is
    /// `pool_api_url`, or [`crate::Error::ApiUrlNotAllowed`] when the pool
    /// names a URL the operator does not trust (see
    /// [`Self::api_url_allowlist`]).
    ///
    /// Callers must resolve the URL *before* reading the pool's token
    /// `Secret`, so an untrusted URL never causes a secret read.
    pub fn resolve_api_url(&self, pool_api_url: Option<&str>) -> crate::Result<String> {
        let Some(requested) = pool_api_url else {
            return Ok(normalize_api_url(&self.default_api_url).to_string());
        };
        let requested = normalize_api_url(requested);
        std::iter::once(&self.default_api_url)
            .chain(&self.api_url_allowlist)
            .any(|allowed| normalize_api_url(allowed) == requested)
            .then(|| requested.to_string())
            .ok_or_else(|| crate::Error::ApiUrlNotAllowed {
                url: requested.to_string(),
            })
    }
}

/// The canonical form of a base URL, matching what [`crate::api::OutpostsClient`]
/// requests against, so allowlist comparisons can't be evaded by formatting.
fn normalize_api_url(url: &str) -> &str {
    url.trim().trim_end_matches('/')
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One test covers all env parsing: the environment is process-global, so
    /// splitting into parallel `#[test]`s would race.
    #[test]
    fn from_env_parses_and_defaults() {
        let vars = [
            ("METRICS_ADDR", ":9090"),
            ("WATCH_NAMESPACE", "outposts"),
            ("DEVIN_API_URL", "https://api.example.com"),
            (
                "DEVIN_API_URL_ALLOWLIST",
                "https://api.eu.example.com/ , https://staging.example.com",
            ),
            ("DEVIN_WORKER_IMAGE", "img:tag"),
            ("RECONCILE_INTERVAL_SECONDS", "12"),
            ("CLAIM_RENEW_MARGIN_SECONDS", "34"),
            ("ACCEPTOR_ID", "my-acceptor"),
            ("WORKER_RESTART_LIMIT", "7"),
            ("POD_NAMESPACE", "op-ns"),
            ("POD_NAME", "op-pod-0"),
            ("LEADER_ELECTION", "false"),
        ];
        // SAFETY: nothing else in this test binary reads or writes the
        // environment concurrently.
        unsafe {
            for (key, value) in vars {
                std::env::set_var(key, value);
            }
        }
        let config = OperatorConfig::from_env().unwrap();
        assert_eq!(config.metrics_addr.to_string(), "0.0.0.0:9090");
        assert_eq!(config.watch_namespace.as_deref(), Some("outposts"));
        assert_eq!(config.default_api_url, "https://api.example.com");
        assert_eq!(
            config.api_url_allowlist,
            ["https://api.eu.example.com/", "https://staging.example.com"]
        );
        assert_eq!(config.default_worker_image, "img:tag");
        assert_eq!(config.reconcile_interval, Duration::from_secs(12));
        assert_eq!(config.claim_renew_margin, Duration::from_secs(34));
        assert_eq!(config.acceptor_id.as_deref(), Some("my-acceptor"));
        assert_eq!(config.worker_restart_limit, 7);
        assert_eq!(config.operator_namespace, "op-ns");
        assert_eq!(config.identity, "op-pod-0");
        assert!(!config.leader_election);

        // Empty values mean unset (the chart always renders the env vars).
        // SAFETY: as above.
        unsafe {
            for (key, _) in vars {
                std::env::set_var(key, "");
            }
        }
        let config = OperatorConfig::from_env().unwrap();
        let defaults = OperatorConfig::default();
        assert_eq!(config.metrics_addr, defaults.metrics_addr);
        assert_eq!(config.watch_namespace, None);
        assert_eq!(config.acceptor_id, None);
        assert_eq!(config.default_api_url, defaults.default_api_url);
        assert!(config.api_url_allowlist.is_empty());
        assert!(config.leader_election);

        // SAFETY: as above.
        unsafe {
            std::env::set_var("METRICS_ADDR", "not-an-addr");
        }
        assert!(OperatorConfig::from_env().is_err());
        // SAFETY: as above.
        unsafe {
            std::env::remove_var("METRICS_ADDR");
        }
    }

    #[test]
    fn resolve_api_url_accepts_only_trusted_urls() {
        let config = OperatorConfig {
            default_api_url: "https://api.devin.ai".to_string(),
            api_url_allowlist: vec!["https://api.eu.devin.ai/".to_string()],
            ..Default::default()
        };

        let resolve = |url: &str| config.resolve_api_url(Some(url));

        assert_eq!(
            config.resolve_api_url(None).unwrap(),
            "https://api.devin.ai"
        );
        assert_eq!(
            resolve("https://api.devin.ai/").unwrap(),
            "https://api.devin.ai"
        );
        assert_eq!(
            resolve("https://api.eu.devin.ai").unwrap(),
            "https://api.eu.devin.ai"
        );

        for untrusted in [
            "https://attacker.tld",
            "http://api.devin.ai",
            "https://api.devin.ai.attacker.tld",
            "https://attacker.tld/https://api.devin.ai",
            "http://169.254.169.254",
            "https://api.devin.ai@attacker.tld",
        ] {
            assert!(
                matches!(
                    resolve(untrusted),
                    Err(crate::Error::ApiUrlNotAllowed { .. })
                ),
                "{untrusted} must be rejected"
            );
        }
    }
}
