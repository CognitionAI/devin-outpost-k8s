//! Per-session worker `Pod` template builder.
//!
//! For each claimed session the operator creates one owned `Pod` running the
//! `devin` CLI worker in *direct-serve* mode. The worker dials the outpost
//! gateway's public leg using the connect token from the claim, and the brain
//! attaches over the gateway's internal leg.
//!
//! ## Operator ↔ worker-image contract
//!
//! This module is the single source of truth for the contract between the
//! operator and the worker image; the image (a lightweight Dockerfile bundling
//! the `devin` CLI, published from the CLI's regular release flow) must stay
//! in sync with it.
//!
//! - The operator runs [`WORKER_COMMAND`] with args
//!   `["worker", "start", "--session", <session_id>]`.
//! - [`ENV_SESSION_TOKEN`] carries the gateway connect token, injected via
//!   `secretKeyRef` from a per-session `Secret` (see
//!   [`build_session_token_secret`]) rather than inline in the pod spec, so it
//!   isn't readable by everyone with `pods get`. Its presence switches
//!   `devin worker start` into direct-serve mode: the worker runs the remote
//!   for exactly this session and never contacts the queue API, so worker
//!   pods hold no account credentials — claim, renewal and release all happen
//!   centrally in the operator.
//! - [`ENV_GATEWAY_URL`] carries the gateway public websocket URL from the
//!   claim.
//! - [`ENV_REMOTE_BINARY_SHA`] carries `spec.remote_binary_sha` when the queue
//!   item pins one; unset otherwise (the worker then uses the latest published
//!   binary).
//! - For pools with the `FilesystemSnapshot` resume policy, the worker's data
//!   directory is redirected onto a per-session volume by setting
//!   [`ENV_WORKER_CACHE_DIR`] to `<`[`WORKER_DATA_DIR`]`>/cache` (the worker
//!   derives its per-session state dir from the cache dir's parent).

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{
    Container, EnvVar, EnvVarSource, PersistentVolumeClaim, PersistentVolumeClaimSpec,
    PersistentVolumeClaimVolumeSource, Pod, PodSpec, Secret, SecretKeySelector, Volume,
    VolumeMount, VolumeResourceRequirements,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use kube::Resource;

use crate::api::OutpostDevin;
use crate::crd::{OutpostPool, ResumeConfig};
use crate::error::{Error, Result};

/// Default worker image used when a pool's `worker.overrides.image` is unset.
///
/// CR-soon nikhil: "there's probably a better tag to use later"; the operator
/// deployment should eventually pin this (via config) to the image matching its
/// own release.
pub const DEFAULT_WORKER_IMAGE: &str = "public.ecr.aws/e0h8a4b6/devin-cli:3000.1.1016";

/// Entrypoint the operator sets on the worker container. Kept separate from
/// the args so `worker.overrides.args` can tweak flags without repeating the
/// binary (and `worker.overrides.command` can swap the binary itself).
pub const WORKER_COMMAND: &str = "devin";

/// Env var carrying the outpost gateway public websocket URL.
pub const ENV_GATEWAY_URL: &str = "DEVIN_OUTPOST_GATEWAY_URL";
/// Env var carrying the gateway connect token (via the per-session `Secret`).
pub const ENV_SESSION_TOKEN: &str = "DEVIN_REMOTE_SESSION_TOKEN";
/// Env var carrying `spec.remote_binary_sha`, when set.
pub const ENV_REMOTE_BINARY_SHA: &str = "DEVIN_WORKER_REMOTE_SHA";
/// Env var redirecting the worker's binary cache (and, via its parent, the
/// per-session state dir) onto [`WORKER_DATA_DIR`].
pub const ENV_WORKER_CACHE_DIR: &str = "DEVIN_WORKER_CACHE_DIR";

/// Mount point of the per-session state volume inside the worker container.
pub const WORKER_DATA_DIR: &str = "/var/lib/devin-worker";

/// Name of the per-session state volume in the pod spec.
const STATE_VOLUME_NAME: &str = "outpost-state";

/// Key under which the connect token is stored in the per-session `Secret`.
pub const SESSION_TOKEN_SECRET_KEY: &str = "token";

/// Label identifying every object this operator manages.
pub const LABEL_MANAGED_BY: &str = "app.kubernetes.io/managed-by";
/// Label carrying the session ID on per-session objects.
pub const LABEL_SESSION_ID: &str = "outposts.cognition.com/session-id";
/// Label carrying the owning `OutpostPool`'s name on per-session objects.
pub const LABEL_POOL: &str = "outposts.cognition.com/pool";
/// Annotation carrying the upstream pool ID on per-session objects.
pub const ANNOTATION_POOL_ID: &str = "outposts.cognition.com/pool-id";
/// Annotation recording when a per-session state volume was last suspended
/// (RFC3339); used to garbage-collect volumes past `resume.snapshotTtlSeconds`.
pub const ANNOTATION_SUSPENDED_AT: &str = "outposts.cognition.com/suspended-at";

/// Deterministic name of the worker `Pod` for a session.
pub fn worker_pod_name(session_id: &str) -> String {
    session_object_name("outpost-worker", session_id)
}

/// Deterministic name of the per-session connect-token `Secret`.
pub fn session_token_secret_name(session_id: &str) -> String {
    session_object_name("outpost-token", session_id)
}

/// Deterministic name of the per-session state `PersistentVolumeClaim`
/// (`FilesystemSnapshot` resume policy only).
pub fn state_pvc_name(session_id: &str) -> String {
    session_object_name("outpost-state", session_id)
}

/// Derive a DNS-1123 object name from a session ID, deterministically.
///
/// Session IDs are generally already DNS-safe, but the name must stay valid
/// (and collision-free) even for hostile inputs, so the sanitized ID is
/// paired with a short stable hash of the raw ID.
fn session_object_name(prefix: &str, session_id: &str) -> String {
    let sanitized: String = session_id
        .to_ascii_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    let hash = fnv1a(session_id.as_bytes());
    let budget = 63 - prefix.len() - "--########".len();
    let sanitized = sanitized
        .get(..budget.min(sanitized.len()))
        .unwrap_or_default()
        .trim_matches('-');
    format!("{prefix}-{sanitized}-{hash:08x}")
}

/// FNV-1a (32-bit). `DefaultHasher` isn't stable across Rust releases, and
/// these hashes end up in object names that must survive operator upgrades.
fn fnv1a(bytes: &[u8]) -> u32 {
    let mut hash: u32 = 0x811c9dc5;
    for &b in bytes {
        hash ^= u32::from(b);
        hash = hash.wrapping_mul(0x01000193);
    }
    hash
}

/// The labels the operator merges onto every per-session object.
pub fn session_labels(pool: &OutpostPool, session_id: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            LABEL_MANAGED_BY.to_string(),
            crate::MANAGER_NAME.to_string(),
        ),
        (LABEL_SESSION_ID.to_string(), session_id.to_string()),
        (
            LABEL_POOL.to_string(),
            pool.meta().name.clone().unwrap_or_default(),
        ),
    ])
}

fn session_annotations(pool: &OutpostPool) -> BTreeMap<String, String> {
    BTreeMap::from([(ANNOTATION_POOL_ID.to_string(), pool.spec.pool_id.clone())])
}

fn pool_owner_ref(pool: &OutpostPool) -> Result<OwnerReference> {
    pool.controller_owner_ref(&()).ok_or_else(|| {
        Error::Config("OutpostPool has no metadata.name/uid; cannot own objects".to_string())
    })
}

/// Inputs needed to render a worker pod for one claimed session.
#[derive(Debug, Clone)]
pub struct WorkerPodParams<'a> {
    /// The owning pool (provides the pod template + ownership reference).
    pub pool: &'a OutpostPool,
    /// The claimed session this pod will serve.
    pub session: &'a OutpostDevin,
    /// Gateway public URL returned by the claim.
    pub gateway_url: &'a str,
    /// Name of the per-session `Secret` (from [`build_session_token_secret`])
    /// holding the connect token under [`SESSION_TOKEN_SECRET_KEY`].
    pub token_secret_name: &'a str,
    /// Operator-wide default worker image, used when the pool's
    /// `worker.overrides.image` is unset (see [`DEFAULT_WORKER_IMAGE`] /
    /// [`crate::config::OperatorConfig`]).
    pub default_image: &'a str,
    /// Name of the per-session state `PersistentVolumeClaim` to mount at
    /// [`WORKER_DATA_DIR`], when the pool's resume policy keeps one (see
    /// [`build_state_pvc`]).
    pub state_pvc_name: Option<&'a str>,
}

/// Build the per-session `Secret` holding the gateway connect token under
/// [`SESSION_TOKEN_SECRET_KEY`].
///
/// Labeled with the session ID and owner-referenced to the pool; the operator
/// deletes it together with the worker pod. Connect tokens are issued with a
/// TTL covering the maximum session lifetime, so the secret is written once
/// at claim time and never refreshed.
pub fn build_session_token_secret(
    pool: &OutpostPool,
    session: &OutpostDevin,
    connect_token: &str,
) -> Result<Secret> {
    let session_id = &session.metadata.session_id;
    Ok(Secret {
        metadata: ObjectMeta {
            name: Some(session_token_secret_name(session_id)),
            namespace: pool.meta().namespace.clone(),
            labels: Some(session_labels(pool, session_id)),
            annotations: Some(session_annotations(pool)),
            owner_references: Some(vec![pool_owner_ref(pool)?]),
            ..Default::default()
        },
        type_: Some("Opaque".to_string()),
        string_data: Some(BTreeMap::from([(
            SESSION_TOKEN_SECRET_KEY.to_string(),
            connect_token.to_string(),
        )])),
        ..Default::default()
    })
}

/// Build the per-session state `PersistentVolumeClaim` backing the
/// `FilesystemSnapshot` resume policy (see [`crate::snapshot`] for when it is
/// created, retained and deleted).
pub fn build_state_pvc(
    pool: &OutpostPool,
    session_id: &str,
    resume: &ResumeConfig,
) -> Result<PersistentVolumeClaim> {
    let size = resume
        .volume_size
        .clone()
        .unwrap_or_else(|| ResumeConfig::DEFAULT_VOLUME_SIZE.to_string());
    Ok(PersistentVolumeClaim {
        metadata: ObjectMeta {
            name: Some(state_pvc_name(session_id)),
            namespace: pool.meta().namespace.clone(),
            labels: Some(session_labels(pool, session_id)),
            annotations: Some(session_annotations(pool)),
            owner_references: Some(vec![pool_owner_ref(pool)?]),
            ..Default::default()
        },
        spec: Some(PersistentVolumeClaimSpec {
            access_modes: Some(vec!["ReadWriteOnce".to_string()]),
            storage_class_name: resume.storage_class_name.clone(),
            resources: Some(VolumeResourceRequirements {
                requests: Some(BTreeMap::from([("storage".to_string(), Quantity(size))])),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    })
}

/// Build the worker `Pod` for one claimed session.
///
/// The pool's [`crate::crd::WorkerTemplate`] is assembled in three layers (see
/// its docs):
///
/// 1. **Base:** start from `worker.template`
///    ([`k8s_openapi::api::core::v1::PodTemplateSpec`]) — carry over its `spec`,
///    `nodeSelector`, `tolerations`, `runtimeClassName`, `serviceAccountName`,
///    volumes, sidecars, pod metadata, etc.
/// 2. **Operator vars:** find the container named `worker.container_name`
///    (default `devin-worker`), inserting an empty one if absent. Set its
///    `image` to `default_image`, its command/args and env per the module-level
///    contract. Set `restartPolicy = OnFailure` (see the lifecycle mapping in
///    [`crate::controller`]), and attach a deterministic pod name, identifying
///    labels/annotations, and an owner reference to the pool for GC.
/// 3. **Overrides:** apply `worker.overrides`. Each `Some` field wins over the
///    layer-2 value: `image`, `command`, `args`, `restart_policy`, and the pod
///    `labels`/`annotations` (which *replace* the operator's merged set). The
///    pod name and owner reference stay operator-owned and are never overridden.
pub fn build_worker_pod(params: WorkerPodParams<'_>) -> Result<Pod> {
    let WorkerPodParams {
        pool,
        session,
        gateway_url,
        token_secret_name,
        default_image,
        state_pvc_name,
    } = params;
    let session_id = &session.metadata.session_id;
    let worker = &pool.spec.worker;
    let overrides = &worker.overrides;

    // Layer 1: the pool's base template.
    let template = worker.template.clone();
    let mut spec = template.spec.unwrap_or_default();
    let template_meta = template.metadata.unwrap_or_default();

    // Layer 2: operator vars on the worker container.
    let container = match spec
        .containers
        .iter_mut()
        .find(|c| c.name == worker.container_name)
    {
        Some(c) => c,
        None => {
            spec.containers.push(Container {
                name: worker.container_name.clone(),
                ..Default::default()
            });
            spec.containers.last_mut().expect("just pushed")
        }
    };

    container.image = Some(default_image.to_string());
    container.command = Some(vec![WORKER_COMMAND.to_string()]);
    container.args = Some(vec![
        "worker".to_string(),
        "start".to_string(),
        "--session".to_string(),
        session_id.clone(),
    ]);

    let mut operator_env = vec![
        EnvVar {
            name: ENV_SESSION_TOKEN.to_string(),
            value_from: Some(EnvVarSource {
                secret_key_ref: Some(SecretKeySelector {
                    name: token_secret_name.to_string(),
                    key: SESSION_TOKEN_SECRET_KEY.to_string(),
                    optional: Some(false),
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
        EnvVar {
            name: ENV_GATEWAY_URL.to_string(),
            value: Some(gateway_url.to_string()),
            ..Default::default()
        },
    ];
    if let Some(sha) = &session.spec.remote_binary_sha {
        operator_env.push(EnvVar {
            name: ENV_REMOTE_BINARY_SHA.to_string(),
            value: Some(sha.clone()),
            ..Default::default()
        });
    }
    if let Some(pvc_name) = state_pvc_name {
        operator_env.push(EnvVar {
            name: ENV_WORKER_CACHE_DIR.to_string(),
            value: Some(format!("{WORKER_DATA_DIR}/cache")),
            ..Default::default()
        });
        container
            .volume_mounts
            .get_or_insert_default()
            .push(VolumeMount {
                name: STATE_VOLUME_NAME.to_string(),
                mount_path: WORKER_DATA_DIR.to_string(),
                ..Default::default()
            });
        spec.volumes.get_or_insert_default().push(Volume {
            name: STATE_VOLUME_NAME.to_string(),
            persistent_volume_claim: Some(PersistentVolumeClaimVolumeSource {
                claim_name: pvc_name.to_string(),
                ..Default::default()
            }),
            ..Default::default()
        });
    }
    let env = container.env.get_or_insert_default();
    env.retain(|e| operator_env.iter().all(|o| o.name != e.name));
    env.extend(operator_env);

    spec.restart_policy = Some("OnFailure".to_string());

    // Layer 3: pool overrides win over layer 2 (but never over the pod name
    // or owner reference).
    if let Some(image) = &overrides.image {
        container.image = Some(image.clone());
    }
    if let Some(command) = &overrides.command {
        container.command = Some(command.clone());
    }
    if let Some(args) = &overrides.args {
        container.args = Some(args.clone());
    }
    if let Some(restart_policy) = &overrides.restart_policy {
        spec.restart_policy = Some(restart_policy.clone());
    }

    let operator_labels = overrides
        .labels
        .clone()
        .unwrap_or_else(|| session_labels(pool, session_id));
    let operator_annotations = overrides
        .annotations
        .clone()
        .unwrap_or_else(|| session_annotations(pool));

    let mut labels = template_meta.labels.unwrap_or_default();
    labels.extend(operator_labels);
    let mut annotations = template_meta.annotations.unwrap_or_default();
    annotations.extend(operator_annotations);

    Ok(Pod {
        metadata: ObjectMeta {
            name: Some(worker_pod_name(session_id)),
            namespace: pool.meta().namespace.clone(),
            labels: Some(labels),
            annotations: Some(annotations),
            owner_references: Some(vec![pool_owner_ref(pool)?]),
            ..Default::default()
        },
        spec: Some(PodSpec { ..spec }),
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use k8s_openapi::api::core::v1::PodTemplateSpec;

    use crate::api::{Kind, Metadata, OutpostDevin, Phase, SessionStatus, Spec, Status};
    use crate::crd::{OutpostPoolSpec, SecretKeyRef, WorkerOverrides, WorkerTemplate};

    use super::*;

    fn pool() -> OutpostPool {
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
                worker: WorkerTemplate {
                    template: serde_json::from_value::<PodTemplateSpec>(serde_json::json!({
                        "metadata": {
                            "labels": {"team": "outposts"},
                            "annotations": {"autopilot.gke.io/compute-class": "Performance"},
                        },
                        "spec": {
                            "runtimeClassName": "gvisor",
                            "nodeSelector": {"pool": "workers"},
                            "containers": [
                                {
                                    "name": "devin-worker",
                                    "imagePullPolicy": "IfNotPresent",
                                    "resources": {"requests": {"cpu": "1"}},
                                },
                                {"name": "sidecar", "image": "envoy"},
                            ],
                        },
                    }))
                    .unwrap(),
                    container_name: "devin-worker".to_string(),
                    overrides: WorkerOverrides::default(),
                },
                resume: Default::default(),
            },
        );
        pool.metadata.namespace = Some("ns".to_string());
        pool.metadata.uid = Some("uid-123".to_string());
        pool
    }

    fn session(id: &str) -> OutpostDevin {
        OutpostDevin {
            metadata: Metadata {
                session_id: id.to_string(),
                pool_id: "pool_x".to_string(),
                created_at: Some(1),
                updated_at: Some(1),
            },
            spec: Spec {
                kind: Kind::New,
                platform: "linux".to_string(),
                remote_binary_sha: Some("abc123".to_string()),
            },
            status: Status {
                phase: Phase::Claimed,
                acceptor_id: Some("us".to_string()),
                claim_deadline: Some(1_000),
                session_status: SessionStatus::Pending,
                connect_token: Some("tok".to_string()),
                gateway_url: Some("wss://gw".to_string()),
            },
        }
    }

    fn params<'a>(pool: &'a OutpostPool, session: &'a OutpostDevin) -> WorkerPodParams<'a> {
        WorkerPodParams {
            pool,
            session,
            gateway_url: "wss://gw",
            token_secret_name: "outpost-token-x",
            default_image: "img:default",
            state_pvc_name: None,
        }
    }

    #[test]
    fn object_names_are_deterministic_and_dns_safe() {
        assert_eq!(worker_pod_name("devin-abc"), worker_pod_name("devin-abc"));
        assert_ne!(worker_pod_name("devin-abc"), worker_pod_name("devin-abd"));
        for hostile in ["UPPER_case!", "a".repeat(200).as_str(), "..--.."] {
            for name in [
                worker_pod_name(hostile),
                session_token_secret_name(hostile),
                state_pvc_name(hostile),
            ] {
                assert!(name.len() <= 63, "{name}");
                assert!(
                    name.chars()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'),
                    "{name}"
                );
                assert!(!name.starts_with('-') && !name.ends_with('-'), "{name}");
            }
        }
    }

    #[test]
    fn layer2_overlays_operator_vars_onto_the_template() {
        let (pool, session) = (pool(), session("devin-1"));
        let pod = build_worker_pod(params(&pool, &session)).unwrap();

        // Layer 1 survives: pod-level knobs, sidecars, container extras.
        let spec = pod.spec.as_ref().unwrap();
        assert_eq!(spec.runtime_class_name.as_deref(), Some("gvisor"));
        assert_eq!(spec.node_selector.as_ref().unwrap()["pool"], "workers");
        assert_eq!(spec.containers.len(), 2);
        let worker = spec
            .containers
            .iter()
            .find(|c| c.name == "devin-worker")
            .unwrap();
        assert_eq!(worker.image_pull_policy.as_deref(), Some("IfNotPresent"));
        assert!(worker.resources.is_some());
        let labels = pod.metadata.labels.as_ref().unwrap();
        assert_eq!(labels["team"], "outposts");

        // Layer 2: operator-owned fields.
        assert_eq!(worker.image.as_deref(), Some("img:default"));
        assert_eq!(worker.command.as_ref().unwrap(), &vec!["devin".to_string()]);
        assert_eq!(
            worker.args.as_ref().unwrap(),
            &vec!["worker", "start", "--session", "devin-1"]
        );
        assert_eq!(spec.restart_policy.as_deref(), Some("OnFailure"));
        assert_eq!(labels[LABEL_SESSION_ID], "devin-1");
        assert_eq!(labels[LABEL_POOL], "my-pool");
        let env = worker.env.as_ref().unwrap();
        let token = env.iter().find(|e| e.name == ENV_SESSION_TOKEN).unwrap();
        let key_ref = token
            .value_from
            .as_ref()
            .unwrap()
            .secret_key_ref
            .as_ref()
            .unwrap();
        assert_eq!(key_ref.name, "outpost-token-x");
        assert_eq!(key_ref.key, SESSION_TOKEN_SECRET_KEY);
        let gateway = env.iter().find(|e| e.name == ENV_GATEWAY_URL).unwrap();
        assert_eq!(gateway.value.as_deref(), Some("wss://gw"));
        let sha = env
            .iter()
            .find(|e| e.name == ENV_REMOTE_BINARY_SHA)
            .unwrap();
        assert_eq!(sha.value.as_deref(), Some("abc123"));

        let owner = &pod.metadata.owner_references.as_ref().unwrap()[0];
        assert_eq!(owner.uid, "uid-123");
        assert_eq!(owner.controller, Some(true));
    }

    #[test]
    fn layer3_overrides_win_but_name_and_owner_stay() {
        let mut pool = pool();
        pool.spec.worker.overrides = WorkerOverrides {
            image: Some("img:pinned".to_string()),
            command: Some(vec!["/custom".to_string()]),
            args: Some(vec!["--flag".to_string()]),
            restart_policy: Some("Never".to_string()),
            labels: Some(BTreeMap::new()),
            annotations: Some(BTreeMap::from([("note".to_string(), "x".to_string())])),
        };
        let session = session("devin-1");
        let pod = build_worker_pod(params(&pool, &session)).unwrap();

        let spec = pod.spec.as_ref().unwrap();
        let worker = spec
            .containers
            .iter()
            .find(|c| c.name == "devin-worker")
            .unwrap();
        assert_eq!(worker.image.as_deref(), Some("img:pinned"));
        assert_eq!(
            worker.command.as_ref().unwrap(),
            &vec!["/custom".to_string()]
        );
        assert_eq!(worker.args.as_ref().unwrap(), &vec!["--flag".to_string()]);
        assert_eq!(spec.restart_policy.as_deref(), Some("Never"));

        // `labels: {}` drops the operator's labels but keeps the template's.
        let labels = pod.metadata.labels.as_ref().unwrap();
        assert_eq!(labels["team"], "outposts");
        assert!(!labels.contains_key(LABEL_SESSION_ID));
        let annotations = pod.metadata.annotations.as_ref().unwrap();
        assert_eq!(annotations["note"], "x");
        assert!(!annotations.contains_key(ANNOTATION_POOL_ID));

        // Name and owner reference are never overridable.
        assert_eq!(
            pod.metadata.name.as_deref(),
            Some(&*worker_pod_name("devin-1"))
        );
        assert_eq!(
            pod.metadata.owner_references.as_ref().unwrap()[0].uid,
            "uid-123"
        );
    }

    #[test]
    fn state_volume_is_mounted_when_the_policy_keeps_one() {
        let (pool, session) = (pool(), session("devin-1"));
        let mut p = params(&pool, &session);
        let pvc_name = state_pvc_name("devin-1");
        p.state_pvc_name = Some(&pvc_name);
        let pod = build_worker_pod(p).unwrap();

        let spec = pod.spec.as_ref().unwrap();
        let volume = spec
            .volumes
            .as_ref()
            .unwrap()
            .iter()
            .find(|v| v.name == STATE_VOLUME_NAME)
            .unwrap();
        assert_eq!(
            volume.persistent_volume_claim.as_ref().unwrap().claim_name,
            pvc_name
        );
        let worker = spec
            .containers
            .iter()
            .find(|c| c.name == "devin-worker")
            .unwrap();
        let mount = worker
            .volume_mounts
            .as_ref()
            .unwrap()
            .iter()
            .find(|m| m.name == STATE_VOLUME_NAME)
            .unwrap();
        assert_eq!(mount.mount_path, WORKER_DATA_DIR);
        let env = worker.env.as_ref().unwrap();
        let cache_dir = env.iter().find(|e| e.name == ENV_WORKER_CACHE_DIR).unwrap();
        assert_eq!(
            cache_dir.value.as_deref(),
            Some(format!("{WORKER_DATA_DIR}/cache").as_str())
        );
    }

    #[test]
    fn missing_container_is_created() {
        let mut pool = pool();
        pool.spec.worker.container_name = "not-in-template".to_string();
        let session = session("devin-1");
        let pod = build_worker_pod(params(&pool, &session)).unwrap();
        let spec = pod.spec.as_ref().unwrap();
        assert_eq!(spec.containers.len(), 3);
        let worker = spec
            .containers
            .iter()
            .find(|c| c.name == "not-in-template")
            .unwrap();
        assert_eq!(worker.image.as_deref(), Some("img:default"));
    }

    #[test]
    fn token_secret_carries_the_connect_token_and_ownership() {
        let (pool, session) = (pool(), session("devin-1"));
        let secret = build_session_token_secret(&pool, &session, "tok-value").unwrap();
        assert_eq!(
            secret.metadata.name.as_deref(),
            Some(&*session_token_secret_name("devin-1"))
        );
        assert_eq!(
            secret.string_data.as_ref().unwrap()[SESSION_TOKEN_SECRET_KEY],
            "tok-value"
        );
        assert_eq!(
            secret.metadata.labels.as_ref().unwrap()[LABEL_SESSION_ID],
            "devin-1"
        );
        assert_eq!(
            secret.metadata.owner_references.as_ref().unwrap()[0].uid,
            "uid-123"
        );
    }

    #[test]
    fn state_pvc_uses_configured_size_and_class() {
        let pool = pool();
        let pvc = build_state_pvc(&pool, "devin-1", &Default::default()).unwrap();
        let spec = pvc.spec.as_ref().unwrap();
        assert_eq!(
            spec.resources.as_ref().unwrap().requests.as_ref().unwrap()["storage"].0,
            ResumeConfig::DEFAULT_VOLUME_SIZE
        );
        assert_eq!(spec.storage_class_name, None);

        let resume = ResumeConfig {
            volume_size: Some("100Gi".to_string()),
            storage_class_name: Some("fast".to_string()),
            ..Default::default()
        };
        let pvc = build_state_pvc(&pool, "devin-1", &resume).unwrap();
        let spec = pvc.spec.as_ref().unwrap();
        assert_eq!(
            spec.resources.as_ref().unwrap().requests.as_ref().unwrap()["storage"].0,
            "100Gi"
        );
        assert_eq!(spec.storage_class_name.as_deref(), Some("fast"));
    }
}
