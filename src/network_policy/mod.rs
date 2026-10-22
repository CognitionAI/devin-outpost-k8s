//! Enforcement of a session's network policy on its worker pod.
//!
//! A queued session carries the egress allow list it must run under
//! ([`crate::api::NetworkPolicy`]). Most of that list is hostnames, which a
//! Kubernetes `NetworkPolicy` cannot match, so enforcement goes through a
//! CNI-specific [`Provider`].
//!
//! ## Which provider
//!
//! [`crate::config::OperatorConfig::network_policy_enforcement`] decides:
//!
//! - [`Enforcement::Auto`] leaves it to [`Provider::detect`]: "if those
//!   aren't available then don't create" — a restricted session still gets
//!   its worker, unrestricted, with a warning logged.
//! - [`Enforcement::Through`] makes the named provider a requirement, so a
//!   session whose policy cannot be applied gets no worker.
//! - [`Enforcement::Off`] never creates anything: every session runs
//!   unrestricted, for clusters that restrict egress by other means.
//!
//! ## What a worker may reach
//!
//! - the session's allow list, on any port;
//! - [`worker_infrastructure`], without which no restricted session could
//!   start;
//! - cluster DNS.
//!
//! Everything else leaving the pod is denied; traffic into it is untouched.
//! A destination that cannot be enforced as written fails closed: it is
//! dropped (with a warning) or enforced narrower, never wider. Each provider
//! documents where its hostname globs fall short of
//! [`crate::api::NetDestination::Hostname`]'s.
//!
//! ## Lifetime
//!
//! The policy objects have to exist before the worker pod starts and until
//! it is completely gone (a terminating pod still runs the session's
//! processes). So [`SessionPolicy::apply`] runs twice per pod: owned by the
//! pool before the pod is created, then handed to the created pod, whose
//! garbage collection removes them whichever way the pod is deleted. The
//! operator never deletes them itself; a crash between the two applies only
//! defers their cleanup to the pool's deletion.

mod cilium;
mod gke;

use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::str::FromStr;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::api::{Api, ApiResource, DynamicObject, Patch, PatchParams};
use kube::{Client, ResourceExt};
use serde_json::{Value, json};
use tracing::warn;

use crate::api::NetDestination;
use crate::controller::{LABEL_NETWORK_POLICY, network_policy_name, session_labels};
use crate::crd::OutpostPool;
use crate::error::Result;

/// Whether, and through what, session network policies are enforced (see
/// the module docs).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Enforcement {
    /// Never.
    Off,
    /// Through whichever provider the cluster supports, if any.
    #[default]
    Auto,
    /// Through this provider.
    Through(Provider),
}

/// A CNI that can enforce hostname-based egress rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Provider {
    /// See [`cilium`].
    Cilium,
    /// See [`gke`].
    Gke,
}

impl Provider {
    /// The provider this cluster can enforce through, if any.
    pub async fn detect(client: &Client) -> Result<Option<Self>> {
        // GKE's dataplane is Cilium underneath, but GKE only supports
        // hostname rules through its own kind, so that one is asked for
        // first. Each kind is served only once its provider enforces it:
        // GKE installs its kind when FQDN network policy is enabled, and
        // Dataplane V2 doesn't serve Cilium's.
        for (provider, required) in [
            (Self::Gke, gke::fqdn_policy()),
            (Self::Cilium, cilium::policy()),
        ] {
            let served = match client.list_api_group_resources(&required.api_version).await {
                Ok(list) => list.resources,
                Err(kube::Error::Api(e)) if e.code == 404 => continue,
                Err(e) => return Err(e.into()),
            };
            if served.iter().any(|r| r.kind == required.kind) {
                return Ok(Some(provider));
            }
        }
        Ok(None)
    }

    /// The kinds, and their `spec`s, that restrict the pods `selector`
    /// matches to `egress`.
    fn manifests(self, selector: &Value, egress: &Egress) -> Vec<(ApiResource, Value)> {
        match self {
            Self::Cilium => cilium::manifests(selector, egress),
            Self::Gke => gke::manifests(selector, egress),
        }
    }
}

/// An allow list reduced to what every provider can express.
#[derive(Debug, Default, PartialEq, Eq)]
struct Egress {
    /// Names and globs, in [`NetDestination::Hostname`]'s syntax.
    hostnames: BTreeSet<String>,
    cidrs: BTreeSet<String>,
}

impl Egress {
    fn new(allow: &[NetDestination]) -> Self {
        let mut egress = Self::default();
        for destination in allow {
            let kept = match destination {
                NetDestination::Hostname { hostname } => {
                    hostname_glob(hostname).map(|h| egress.hostnames.insert(h))
                }
                NetDestination::Ipv4 { ipv4 } => {
                    cidr::<Ipv4Addr>(ipv4, 32).map(|c| egress.cidrs.insert(c))
                }
                NetDestination::Ipv6 { ipv6 } => {
                    cidr::<Ipv6Addr>(ipv6, 128).map(|c| egress.cidrs.insert(c))
                }
                NetDestination::Unknown(_) => None,
            };
            if kept.is_none() {
                warn!(
                    ?destination,
                    "network policy destination cannot be enforced; it stays blocked"
                );
            }
        }
        egress
    }
}

/// The providers reject an object outright over one malformed name, which
/// would cost the session its worker rather than one destination.
fn hostname_glob(hostname: &str) -> Option<String> {
    let hostname = hostname.strip_suffix('.').unwrap_or(hostname);
    hostname
        .split('.')
        .all(|label| {
            !label.is_empty()
                && label
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || "-_*".contains(c))
        })
        .then(|| hostname.to_ascii_lowercase())
}

fn cidr<A: FromStr>(entry: &str, max_prefix: u8) -> Option<String> {
    let (address, prefix) = match entry.split_once('/') {
        Some((address, prefix)) => (address, prefix.parse().ok().filter(|p| *p <= max_prefix)?),
        None => (entry, max_prefix),
    };
    address.parse::<A>().ok()?;
    Some(format!("{address}/{prefix}"))
}

/// The destinations a worker needs whatever its session's policy says: the
/// gateway it dials and the host it downloads `devin-remote` from (see the
/// worker contract in [`crate::controller`]'s pod module).
pub fn worker_infrastructure(gateway_url: &str, static_base_url: &str) -> Vec<NetDestination> {
    [gateway_url, static_base_url]
        .into_iter()
        .filter_map(|url| {
            let url = reqwest::Url::parse(url).ok()?;
            let host = url.host_str()?;
            Some(match host.trim_matches(['[', ']']).parse() {
                Ok(IpAddr::V4(ip)) => NetDestination::Ipv4 {
                    ipv4: ip.to_string(),
                },
                Ok(IpAddr::V6(ip)) => NetDestination::Ipv6 {
                    ipv6: ip.to_string(),
                },
                Err(_) => NetDestination::Hostname {
                    hostname: host.to_string(),
                },
            })
        })
        .collect()
}

/// One session's network policy, as enforced through a [`Provider`].
#[derive(Debug, Clone, Copy)]
pub struct SessionPolicy<'a> {
    /// The provider to enforce through.
    pub provider: Provider,
    /// The pool whose worker pod serves the session.
    pub pool: &'a OutpostPool,
    /// The session being restricted.
    pub session_id: &'a str,
    /// Everything the worker pod may reach.
    pub allow: &'a [NetDestination],
}

impl SessionPolicy<'_> {
    fn objects(&self, owner: &OwnerReference) -> Vec<(ApiResource, DynamicObject)> {
        let name = network_policy_name(self.session_id);
        let selector = json!({"matchLabels": {LABEL_NETWORK_POLICY: name}});
        self.provider
            .manifests(&selector, &Egress::new(self.allow))
            .into_iter()
            .map(|(resource, spec)| {
                let mut object = DynamicObject::new(&name, &resource).data(json!({"spec": spec}));
                object.metadata.namespace = self.pool.namespace();
                object.metadata.labels = Some(session_labels(self.pool, self.session_id));
                object.metadata.owner_references = Some(vec![owner.clone()]);
                (resource, object)
            })
            .collect()
    }

    /// Create or update the policy objects, owned by `owner` alone (see the
    /// module docs for who that is when).
    pub async fn apply(&self, client: &Client, owner: &OwnerReference) -> Result<()> {
        let namespace = self.pool.namespace().unwrap_or_else(|| "default".into());
        for (resource, object) in self.objects(owner) {
            Api::<DynamicObject>::namespaced_with(client.clone(), &namespace, &resource)
                .patch(
                    &object.name_any(),
                    &PatchParams::apply(crate::MANAGER_NAME).force(),
                    &Patch::Apply(&object),
                )
                .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use kube::Resource;

    use crate::crd::{OutpostPoolSpec, SecretKeyRef};

    use super::*;

    fn hostname(hostname: &str) -> NetDestination {
        NetDestination::Hostname {
            hostname: hostname.to_string(),
        }
    }

    fn ipv4(ipv4: &str) -> NetDestination {
        NetDestination::Ipv4 {
            ipv4: ipv4.to_string(),
        }
    }

    fn ipv6(ipv6: &str) -> NetDestination {
        NetDestination::Ipv6 {
            ipv6: ipv6.to_string(),
        }
    }

    fn egress(allow: &[NetDestination]) -> Egress {
        Egress::new(allow)
    }

    fn selector() -> Value {
        json!({"matchLabels": {"k": "v"}})
    }

    #[test]
    fn addresses_become_cidrs_and_duplicates_collapse() {
        let egress = egress(&[
            hostname("GitHub.com"),
            hostname("github.com."),
            ipv4("10.0.0.0/8"),
            ipv4("192.0.2.7"),
            ipv6("2001:db8::/32"),
            ipv6("2001:db8::1"),
        ]);
        assert_eq!(egress.hostnames, BTreeSet::from(["github.com".to_string()]));
        assert_eq!(
            egress.cidrs,
            BTreeSet::from([
                "10.0.0.0/8".to_string(),
                "192.0.2.7/32".to_string(),
                "2001:db8::/32".to_string(),
                "2001:db8::1/128".to_string(),
            ])
        );
    }

    #[test]
    fn unenforceable_destinations_stay_blocked() {
        let dropped = egress(&[
            hostname(""),
            hostname("exa mple.com"),
            hostname("a..b"),
            ipv4("10.0.0.0/33"),
            ipv4("2001:db8::1"),
            ipv6("10.0.0.1"),
            ipv6("2001:db8::/129"),
            NetDestination::Unknown(json!({"asn": 64496})),
        ]);
        assert_eq!(dropped, Egress::default());
    }

    #[test]
    fn worker_infrastructure_is_the_hosts_of_both_urls() {
        assert_eq!(
            worker_infrastructure("wss://gw.example:8443/connect", "https://static.example/x"),
            [hostname("gw.example"), hostname("static.example")]
        );
        assert_eq!(
            worker_infrastructure("ws://192.0.2.7:80", "https://[2001:db8::1]/x"),
            [ipv4("192.0.2.7"), ipv6("2001:db8::1")]
        );
    }

    #[test]
    fn cilium_allows_dns_then_only_the_allow_list() {
        let manifests = Provider::Cilium.manifests(
            &selector(),
            &egress(&[
                hostname("github.com"),
                hostname("*.example.com"),
                hostname("s3-*.amazonaws.com"),
                ipv4("10.0.0.0/8"),
            ]),
        );
        let [(resource, spec)] = manifests.as_slice() else {
            panic!("expected one object, got {manifests:?}");
        };
        assert_eq!(resource.kind, "CiliumNetworkPolicy");
        assert_eq!(spec["endpointSelector"], selector());
        let rules = spec["egress"].as_array().unwrap();
        assert_eq!(rules.len(), 3);
        assert_eq!(rules[0]["toPorts"][0]["ports"][0]["port"], "53");
        assert_eq!(
            rules[1]["toFQDNs"],
            json!([
                {"matchPattern": "**.example.com"},
                {"matchName": "github.com"},
                {"matchPattern": "s3-*.amazonaws.com"},
            ])
        );
        assert_eq!(rules[2]["toCIDR"], json!(["10.0.0.0/8"]));
    }

    #[test]
    fn cilium_empty_allow_list_denies_everything_but_dns() {
        let manifests = Provider::Cilium.manifests(&selector(), &Egress::default());
        let rules = manifests[0].1["egress"].as_array().unwrap();
        assert_eq!(rules.len(), 1);
        let dns_servers: Vec<&Value> = rules[0]["toEndpoints"]
            .as_array()
            .unwrap()
            .iter()
            .map(|selector| &selector["matchLabels"]["k8s:k8s-app"])
            .collect();
        assert_eq!(dns_servers, ["kube-dns", "node-local-dns"]);
    }

    #[test]
    fn gke_splits_hostnames_and_cidrs_across_two_kinds() {
        let manifests = Provider::Gke.manifests(
            &selector(),
            &egress(&[
                hostname("github.com"),
                hostname("*.example.com"),
                ipv4("10.0.0.0/8"),
                ipv6("2001:db8::/32"),
            ]),
        );
        let [(ip_resource, ip_spec), (fqdn_resource, fqdn_spec)] = manifests.as_slice() else {
            panic!("expected two objects, got {manifests:?}");
        };
        assert_eq!(ip_resource.kind, "NetworkPolicy");
        assert_eq!(ip_resource.api_version, "networking.k8s.io/v1");
        assert_eq!(ip_spec["podSelector"], selector());
        assert_eq!(ip_spec["policyTypes"], json!(["Egress"]));
        assert_eq!(
            ip_spec["egress"],
            json!([{"to": [
                {"ipBlock": {"cidr": "10.0.0.0/8"}},
                {"ipBlock": {"cidr": "2001:db8::/32"}},
            ]}])
        );
        assert_eq!(fqdn_resource.kind, "FQDNNetworkPolicy");
        assert_eq!(fqdn_spec["podSelector"], selector());
        assert_eq!(
            fqdn_spec["egress"],
            json!([{"matches": [{"pattern": "*.example.com"}, {"name": "github.com"}]}])
        );
    }

    #[test]
    fn gke_empty_allow_list_denies_everything() {
        let manifests = Provider::Gke.manifests(&selector(), &Egress::default());
        let [(resource, spec)] = manifests.as_slice() else {
            panic!("expected one object, got {manifests:?}");
        };
        assert_eq!(resource.kind, "NetworkPolicy");
        assert_eq!(spec["policyTypes"], json!(["Egress"]));
        assert_eq!(spec["egress"], json!([]));
    }

    #[test]
    fn objects_select_the_labeled_pod_and_belong_to_the_given_owner() {
        let mut pool = OutpostPool::new(
            "my-pool",
            OutpostPoolSpec {
                pool_id: "pool_x".to_string(),
                token_secret_ref: SecretKeyRef {
                    name: "tok".to_string(),
                    key: "token".to_string(),
                },
                api_url: None,
                max_concurrent_sessions: 10,
                worker: serde_json::from_value(json!({})).unwrap(),
                resume: Default::default(),
                image_build: None,
            },
        );
        pool.metadata.namespace = Some("ns".to_string());
        pool.metadata.uid = Some("uid-123".to_string());
        let owner = pool.controller_owner_ref(&()).unwrap();
        let allow = [hostname("github.com")];

        for provider in [Provider::Cilium, Provider::Gke] {
            let policy = SessionPolicy {
                provider,
                pool: &pool,
                session_id: "devin-1",
                allow: &allow,
            };
            for (_, object) in policy.objects(&owner) {
                let name = network_policy_name("devin-1");
                assert_eq!(object.metadata.name.as_deref(), Some(&*name));
                assert_eq!(object.metadata.namespace.as_deref(), Some("ns"));
                assert_eq!(object.metadata.owner_references, Some(vec![owner.clone()]));
                let selector = ["endpointSelector", "podSelector"]
                    .iter()
                    .find_map(|key| object.data["spec"].get(key))
                    .unwrap();
                assert_eq!(selector["matchLabels"][LABEL_NETWORK_POLICY], name);
            }
        }
    }
}
