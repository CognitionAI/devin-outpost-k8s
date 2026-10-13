//! GKE Dataplane V2 with FQDN network policy enabled
//! (<https://cloud.google.com/kubernetes-engine/docs/how-to/fqdn-network-policies>):
//! an `FQDNNetworkPolicy` for the hostnames next to a Kubernetes
//! `NetworkPolicy` for the IP ranges. GKE allows what either one allows, and
//! keeps DNS open to the pods an `FQDNNetworkPolicy` selects.
//!
//! Hostname globs: GKE's `*` stops at a dot, so `*.example.com` covers
//! `a.example.com` but, unlike in the session's policy, not
//! `a.b.example.com`.

use k8s_openapi::api::networking::v1::NetworkPolicy;
use kube::api::{ApiResource, GroupVersionKind};
use serde_json::{Value, json};

use super::Egress;

/// API group only GKE clusters serve.
pub(super) const GROUP: &str = "networking.gke.io";

pub(super) fn fqdn_policy() -> ApiResource {
    let gvk = GroupVersionKind::gvk(GROUP, "v1alpha1", "FQDNNetworkPolicy");
    ApiResource::from_gvk_with_plural(&gvk, "fqdnnetworkpolicies")
}

pub(super) fn manifests(selector: &Value, egress: &Egress) -> Vec<(ApiResource, Value)> {
    // A rule without `to` allows every destination, so "no IP ranges" has to
    // be no rule at all.
    let ip_rules = if egress.cidrs.is_empty() {
        json!([])
    } else {
        let blocks: Vec<Value> = egress
            .cidrs
            .iter()
            .map(|cidr| json!({"ipBlock": {"cidr": cidr}}))
            .collect();
        json!([{"to": blocks}])
    };
    let mut manifests = vec![(
        ApiResource::erase::<NetworkPolicy>(&()),
        json!({"podSelector": selector, "policyTypes": ["Egress"], "egress": ip_rules}),
    )];
    if !egress.hostnames.is_empty() {
        let matches: Vec<Value> = egress
            .hostnames
            .iter()
            .map(|hostname| match hostname.contains('*') {
                true => json!({"pattern": hostname}),
                false => json!({"name": hostname}),
            })
            .collect();
        manifests.push((
            fqdn_policy(),
            json!({"podSelector": selector, "egress": [{"matches": matches}]}),
        ));
    }
    manifests
}
