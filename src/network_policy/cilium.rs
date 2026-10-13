//! Cilium: one `CiliumNetworkPolicy` per session
//! (<https://docs.cilium.io/en/stable/security/policy/language/#dns-based>).
//!
//! Hostname globs: Cilium's `*` stops at a dot, and only a leading `**.`
//! spans subdomain levels. A glob of the form `*.example.com` therefore keeps
//! its full reach; any other glob matches within a single label only.

use kube::api::{ApiResource, GroupVersionKind};
use serde_json::{Value, json};

use super::Egress;

pub(super) fn policy() -> ApiResource {
    let gvk = GroupVersionKind::gvk("cilium.io", "v2", "CiliumNetworkPolicy");
    ApiResource::from_gvk_with_plural(&gvk, "ciliumnetworkpolicies")
}

pub(super) fn manifests(selector: &Value, egress: &Egress) -> Vec<(ApiResource, Value)> {
    // Routing lookups through Cilium's DNS proxy is also how it learns which
    // addresses the `toFQDNs` names currently stand for.
    let mut rules = vec![json!({
        "toEndpoints": [{"matchLabels": {
            "k8s:io.kubernetes.pod.namespace": "kube-system",
            "k8s:k8s-app": "kube-dns",
        }}],
        "toPorts": [{
            "ports": [{"port": "53", "protocol": "ANY"}],
            "rules": {"dns": [{"matchPattern": "*"}]},
        }],
    })];
    if !egress.hostnames.is_empty() {
        let fqdns: Vec<Value> = egress.hostnames.iter().map(|h| fqdn(h)).collect();
        rules.push(json!({"toFQDNs": fqdns}));
    }
    if !egress.cidrs.is_empty() {
        rules.push(json!({"toCIDR": egress.cidrs}));
    }
    vec![(
        policy(),
        json!({"endpointSelector": selector, "egress": rules}),
    )]
}

fn fqdn(hostname: &str) -> Value {
    if !hostname.contains('*') {
        return json!({"matchName": hostname});
    }
    match hostname.strip_prefix("*.") {
        Some(parent) if !parent.contains('*') => json!({"matchPattern": format!("**.{parent}")}),
        _ => json!({"matchPattern": hostname}),
    }
}
