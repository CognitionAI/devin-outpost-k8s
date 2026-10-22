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
//! - For pools with the `FilesystemSnapshot` resume policy, the per-session
//!   volume is mounted as the worker container's home directory
//!   (`resume.homeDir`, also exported as [`ENV_HOME`]) so repos and anything
//!   Devin installs under it survive suspend/resume. The worker's data
//!   directory is redirected under it by setting [`ENV_WORKER_CACHE_DIR`] to
//!   `<homeDir>/`[`WORKER_DATA_SUBDIR`]`/cache` (the worker derives its
//!   per-session state dir from the cache dir's parent). The mount hides
//!   whatever the image ships in that directory, so new volumes are cloned
//!   from `resume.volumeDataSource` (see [`build_state_pvc`]).
//! - The worker downloads the `devin-remote` binary from
//!   [`ENV_STATIC_BASE_URL`], falling back to [`DEFAULT_STATIC_BASE_URL`].
//!   The operator never sets it, but a pool's template may; either way the
//!   worker must be able to reach that host and the gateway even when its
//!   session's network policy doesn't list them (see
//!   [`crate::network_policy`]). The operator can only allow a host it can
//!   read, so a template must set it as a literal `env` `value`: a
//!   `valueFrom` fails restricted sessions' workers, and an `envFrom` isn't
//!   seen at all.
//! - When the queue item carries `spec.image` the worker container runs that
//!   image instead of the pool's: it is the session's DRS build, so it wins
//!   even over `worker.overrides.image`. Such an image must honour the same
//!   contract as the pool's worker image. With a state volume, the volume
//!   starts empty and [`SEED_HOME_CONTAINER_NAME`] fills it from the image's
//!   own home directory, so `resume.volumeDataSource` is not used.
//! - When the queue item carries `spec.image_build` the pod is a DRS build:
//!   the worker gets `allowPrivilegeEscalation` (the capture reads the whole
//!   filesystem through `sudo`) and a directory shared with
//!   [`PUSH_SIDECAR_CONTAINER_NAME`], a native sidecar running this
//!   operator's image (`push-layer`, see [`crate::image_push`]) as root,
//!   with the pool's registry credential, the base image and
//!   `spec.image_target`; it pushes what the capture puts in the directory.
//!   The worker, where the customer's blueprints run with sudo, never sees
//!   the credential or the target.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{
    Container, EmptyDirVolumeSource, EnvVar, EnvVarSource, PersistentVolumeClaim,
    PersistentVolumeClaimSpec, PersistentVolumeClaimVolumeSource, Pod, PodSpec, Secret,
    SecretKeySelector, SecurityContext, Volume, VolumeMount, VolumeResourceRequirements,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use kube::Resource;

use crate::api::OutpostDevin;
use crate::crd::{OutpostPool, ResumeConfig};
use crate::error::{Error, Result};
use crate::image_push;

/// Default worker image used when a pool's `worker.overrides.image` is unset.
///
/// Only the `stable` tag is supported; never pin a versioned tag.
pub const DEFAULT_WORKER_IMAGE: &str = "public.ecr.aws/e0h8a4b6/devin-cli:stable";

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
/// per-session state dir) under the home volume.
pub const ENV_WORKER_CACHE_DIR: &str = "DEVIN_WORKER_CACHE_DIR";
/// Env var the worker reads the `devin-remote` download base URL from.
pub const ENV_STATIC_BASE_URL: &str = "DEVIN_WORKER_STATIC_BASE_URL";
/// The worker's built-in default for [`ENV_STATIC_BASE_URL`].
pub const DEFAULT_STATIC_BASE_URL: &str = "https://static.devin.ai/devin-rs/remote";
/// Env var naming the worker container's home directory.
pub const ENV_HOME: &str = "HOME";
/// Push sidecar of an image-build pod (see [`crate::image_push`]).
pub const PUSH_SIDECAR_CONTAINER_NAME: &str = "devin-image-push";
const LAYER_VOLUME_NAME: &str = "drs-layer";

/// Subdirectory of the home volume holding the worker's binary cache and
/// per-session state dir.
pub const WORKER_DATA_SUBDIR: &str = ".devin-worker";

/// Name of the per-session state volume in the pod spec.
const STATE_VOLUME_NAME: &str = "outpost-state";
/// Init container that copies the session image's home directory onto an
/// empty state volume.
pub const SEED_HOME_CONTAINER_NAME: &str = "devin-seed-home";
/// Where [`SEED_HOME_CONTAINER_NAME`] mounts the state volume: not the home
/// directory, which it has to read from the image.
const SEED_HOME_MOUNT_PATH: &str = "/mnt/outpost-state";
/// File on the state volume, relative to its root, recording that the copy
/// finished. A resume, or a retry after a crash mid-copy, must not overwrite a
/// home the session has already changed.
const SEED_HOME_MARKER: &str = ".devin-worker/home-seeded";

/// Key under which the connect token is stored in the per-session `Secret`.
pub const SESSION_TOKEN_SECRET_KEY: &str = "token";

/// Label identifying every object this operator manages.
pub const LABEL_MANAGED_BY: &str = "app.kubernetes.io/managed-by";
/// Label carrying the session ID on per-session objects.
pub const LABEL_SESSION_ID: &str = "outposts.cognition.com/session-id";
/// Label carrying the owning `OutpostPool`'s name on per-session objects.
pub const LABEL_POOL: &str = "outposts.cognition.com/pool";
/// Label naming the network policy objects (see [`network_policy_name`])
/// that govern a worker pod; they select the pod by it. Only present on pods
/// whose session's network policy is enforced.
pub const LABEL_NETWORK_POLICY: &str = "outposts.cognition.com/network-policy";
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

/// Deterministic name of the objects enforcing a session's network policy
/// (see [`crate::network_policy`]).
pub fn network_policy_name(session_id: &str) -> String {
    session_object_name("outpost-netpol", session_id)
}

/// The base URL the pod's worker will download `devin-remote` from (see the
/// module-level contract).
pub fn worker_static_base_url<'a>(pod: &'a Pod, container_name: &str) -> Result<&'a str> {
    let entry = pod
        .spec
        .iter()
        .flat_map(|spec| &spec.containers)
        .filter(|c| c.name == container_name)
        .flat_map(|c| c.env.iter().flatten())
        .find(|e| e.name == ENV_STATIC_BASE_URL);
    match entry {
        Some(EnvVar {
            value_from: Some(_),
            ..
        }) => Err(Error::Config(format!(
            "{ENV_STATIC_BASE_URL} must be a literal value for the network policy to allow its host"
        ))),
        Some(EnvVar {
            value: Some(url), ..
        }) => Ok(url),
        _ => Ok(DEFAULT_STATIC_BASE_URL),
    }
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

pub(super) fn pool_owner_ref(pool: &OutpostPool) -> Result<OwnerReference> {
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
    /// Name of the per-session state `PersistentVolumeClaim` to mount as the
    /// worker's home directory, when the pool's resume policy keeps one (see
    /// [`build_state_pvc`]).
    pub state_pvc_name: Option<&'a str>,
    /// Annotations required by the pool's snapshot provider (e.g. the GKE
    /// restore pin). Applied after overrides: like the pod name and owner
    /// reference, they are never overridable.
    pub provider_annotations: &'a BTreeMap<String, String>,
    /// [`network_policy_name`] of the session when its network policy is
    /// enforced; becomes the pod's [`LABEL_NETWORK_POLICY`]. Never
    /// overridable either: dropping the label would lift the restrictions.
    pub network_policy_name: Option<&'a str>,
    /// This operator's image, run as the push sidecar of image-build pods
    /// (see [`crate::config::OperatorConfig::operator_image`]).
    pub operator_image: Option<&'a str>,
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
/// created, retained and deleted). It is cloned from `resume.volumeDataSource`
/// rather than created empty: the volume becomes the worker's home directory,
/// and an empty one would hide everything the image ships there. A session
/// with its own image (`spec.image`) is the exception: its volume is created
/// empty and seeded from that image by the worker pod, because the pool's
/// data source holds the home of a different image.
pub fn build_state_pvc(
    pool: &OutpostPool,
    session: &OutpostDevin,
    resume: &ResumeConfig,
) -> Result<PersistentVolumeClaim> {
    let session_id = &session.metadata.session_id;
    let data_source = match &session.spec.image {
        Some(_) => None,
        None => Some(resume.volume_data_source.clone().ok_or_else(|| {
            Error::Config(
                "resume.volumeDataSource is required for the FilesystemSnapshot policy \
                 when the session carries no image"
                    .to_string(),
            )
        })?),
    };
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
            data_source,
            resources: Some(VolumeResourceRequirements {
                requests: Some(BTreeMap::from([("storage".to_string(), Quantity(size))])),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    })
}

/// Init container that fills an empty state volume with the home directory
/// `image` ships. It runs as root so the copy keeps the image's ownership on
/// a freshly provisioned, root-owned volume.
fn seed_home_container(image: &str, home_dir: &str) -> Container {
    let script = format!(
        "[ -e \"$SEED/{SEED_HOME_MARKER}\" ] || {{ cp -a \"$IMAGE_HOME/.\" \"$SEED/\" \
         && mkdir -p \"$(dirname \"$SEED/{SEED_HOME_MARKER}\")\" \
         && touch \"$SEED/{SEED_HOME_MARKER}\"; }}"
    );
    Container {
        name: SEED_HOME_CONTAINER_NAME.to_string(),
        image: Some(image.to_string()),
        command: Some(vec!["sh".to_string(), "-c".to_string(), script]),
        env: Some(vec![
            EnvVar {
                name: "IMAGE_HOME".to_string(),
                value: Some(home_dir.to_string()),
                ..Default::default()
            },
            EnvVar {
                name: "SEED".to_string(),
                value: Some(SEED_HOME_MOUNT_PATH.to_string()),
                ..Default::default()
            },
        ]),
        security_context: Some(SecurityContext {
            run_as_user: Some(0),
            ..Default::default()
        }),
        volume_mounts: Some(vec![VolumeMount {
            name: STATE_VOLUME_NAME.to_string(),
            mount_path: SEED_HOME_MOUNT_PATH.to_string(),
            ..Default::default()
        }]),
        ..Default::default()
    }
}

fn layer_volume_mount() -> VolumeMount {
    VolumeMount {
        name: LAYER_VOLUME_NAME.to_string(),
        mount_path: image_push::LAYER_DIR.to_string(),
        ..Default::default()
    }
}

/// The push sidecar of an image-build pod: the operator image running
/// [`image_push::SUBCOMMAND`] with the registry settings the worker must not
/// have. A native sidecar (`restartPolicy: Always` on an init container,
/// Kubernetes 1.29+): up before the worker starts, stopped when the worker
/// exits, so it never holds the pod open. Root, to close the shared
/// directory to the worker's box user and still read what the capture, also
/// root, writes.
fn push_sidecar_container(
    config: &crate::crd::ImageBuildConfig,
    session: &OutpostDevin,
    operator_image: Option<&str>,
    base_image: &str,
) -> Result<Container> {
    let operator_image = operator_image.ok_or_else(|| {
        Error::Config(
            "OPERATOR_IMAGE is not set on the operator; image-build sessions need it \
             for their push sidecar"
                .to_string(),
        )
    })?;
    let target = session.spec.image_target.as_deref().ok_or_else(|| {
        Error::Config(format!(
            "image-build session {} has no image_target",
            session.metadata.session_id
        ))
    })?;
    let value = |name: &str, value: &str| EnvVar {
        name: name.to_string(),
        value: Some(value.to_string()),
        ..Default::default()
    };
    let mut env = vec![
        value(image_push::ENV_LAYER_DIR, image_push::LAYER_DIR),
        value(image_push::ENV_BASE_IMAGE, base_image),
        value(image_push::ENV_TARGET, target),
    ];
    if config.insecure_registry {
        env.push(value(image_push::ENV_REGISTRY_INSECURE, "1"));
    }
    if let Some(secret) = &config.registry_secret_name {
        for (name, key) in [
            (image_push::ENV_REGISTRY_USERNAME, "username"),
            (image_push::ENV_REGISTRY_PASSWORD, "password"),
        ] {
            env.push(EnvVar {
                name: name.to_string(),
                value_from: Some(EnvVarSource {
                    secret_key_ref: Some(SecretKeySelector {
                        name: secret.clone(),
                        key: key.to_string(),
                        optional: Some(false),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            });
        }
    }
    Ok(Container {
        name: PUSH_SIDECAR_CONTAINER_NAME.to_string(),
        image: Some(operator_image.to_string()),
        args: Some(vec![image_push::SUBCOMMAND.to_string()]),
        restart_policy: Some("Always".to_string()),
        env: Some(env),
        security_context: Some(SecurityContext {
            run_as_user: Some(0),
            run_as_non_root: Some(false),
            ..Default::default()
        }),
        volume_mounts: Some(vec![layer_volume_mount()]),
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
///    labels/annotations, and an owner reference to the pool for GC. A
///    session image (`spec.image`) replaces `default_image`.
/// 3. **Overrides:** apply `worker.overrides`. Each `Some` field wins over the
///    layer-2 value: `image` (unless the session carries its own), `command`, `args`, `restart_policy`, and the pod
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
        provider_annotations,
        network_policy_name,
        operator_image,
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

    let image = session.spec.image.as_deref().unwrap_or(default_image);
    container.image = Some(image.to_string());
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
        let home_dir = pool.spec.resume.home_dir();
        operator_env.push(EnvVar {
            name: ENV_HOME.to_string(),
            value: Some(home_dir.to_string()),
            ..Default::default()
        });
        operator_env.push(EnvVar {
            name: ENV_WORKER_CACHE_DIR.to_string(),
            value: Some(format!("{home_dir}/{WORKER_DATA_SUBDIR}/cache")),
            ..Default::default()
        });
        container
            .volume_mounts
            .get_or_insert_default()
            .push(VolumeMount {
                name: STATE_VOLUME_NAME.to_string(),
                mount_path: home_dir.to_string(),
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
    if session.spec.image_build {
        let config = pool.spec.image_build.clone().unwrap_or_default();
        let base_image = config
            .base_image
            .as_deref()
            .or(overrides.image.as_deref())
            .unwrap_or(default_image);
        let sidecar = push_sidecar_container(&config, session, operator_image, base_image)?;
        operator_env.push(EnvVar {
            name: image_push::ENV_LAYER_DIR.to_string(),
            value: Some(image_push::LAYER_DIR.to_string()),
            ..Default::default()
        });
        container
            .volume_mounts
            .get_or_insert_default()
            .push(layer_volume_mount());
        // The capture reads the whole filesystem, which takes sudo.
        container
            .security_context
            .get_or_insert_default()
            .allow_privilege_escalation = Some(true);
        spec.volumes.get_or_insert_default().push(Volume {
            name: LAYER_VOLUME_NAME.to_string(),
            empty_dir: Some(EmptyDirVolumeSource::default()),
            ..Default::default()
        });
        spec.init_containers.get_or_insert_default().push(sidecar);
    }
    let env = container.env.get_or_insert_default();
    env.retain(|e| operator_env.iter().all(|o| o.name != e.name));
    env.extend(operator_env);

    if let (Some(image), Some(_)) = (&session.spec.image, state_pvc_name) {
        spec.init_containers
            .get_or_insert_default()
            .push(seed_home_container(image, pool.spec.resume.home_dir()));
    }

    spec.restart_policy = Some("OnFailure".to_string());

    // Layer 3: pool overrides win over layer 2 (but never over the pod name
    // or owner reference).
    if let Some(image) = &overrides.image
        && session.spec.image.is_none()
    {
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
    labels.remove(LABEL_NETWORK_POLICY);
    labels.extend(
        network_policy_name.map(|name| (LABEL_NETWORK_POLICY.to_string(), name.to_string())),
    );
    let mut annotations = template_meta.annotations.unwrap_or_default();
    annotations.extend(operator_annotations);
    annotations.extend(provider_annotations.clone());

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
    use k8s_openapi::api::core::v1::{PodTemplateSpec, TypedLocalObjectReference};

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
                image_build: None,
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
                outpost_id: "pool_x".to_string(),
                created_at: Some(1),
                updated_at: Some(1),
            },
            spec: Spec {
                kind: Kind::New,
                platform: "linux".to_string(),
                remote_binary_sha: Some("abc123".to_string()),
                network_policy: Default::default(),
                image: None,
                image_build: false,
                image_target: None,
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
        static NO_ANNOTATIONS: BTreeMap<String, String> = BTreeMap::new();
        WorkerPodParams {
            pool,
            session,
            gateway_url: "wss://gw",
            token_secret_name: "outpost-token-x",
            default_image: "img:default",
            state_pvc_name: None,
            provider_annotations: &NO_ANNOTATIONS,
            network_policy_name: None,
            operator_image: Some("ghcr.io/cognitionai/devin-outposts-k8s:sha-1"),
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
                network_policy_name(hostile),
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
    fn only_an_image_build_pod_gets_a_push_sidecar_and_the_worker_never_the_credential() {
        fn drs_env(container: &Container) -> BTreeMap<&str, &EnvVar> {
            container
                .env
                .iter()
                .flatten()
                .filter(|e| e.name.starts_with("DEVIN_DRS_"))
                .map(|e| (e.name.as_str(), e))
                .collect()
        }
        let mut pool = pool();
        pool.spec.image_build = Some(crate::crd::ImageBuildConfig {
            registry_secret_name: Some("registry-push".to_string()),
            ..Default::default()
        });
        let pod = build_worker_pod(params(&pool, &session("devin-agent"))).unwrap();
        let agent = pod.spec.unwrap();
        assert!(agent.init_containers.is_none());
        assert!(drs_env(&agent.containers[0]).is_empty());

        let mut build = session("devin-build");
        build.spec.image_build = true;
        // The target is the control plane's; without it the pod is refused.
        let err = build_worker_pod(params(&pool, &build)).unwrap_err();
        assert!(matches!(err, Error::Config(_)), "{err}");
        build.spec.image_target = Some("reg.example/devin/drs/org-1:job-1".to_string());
        let pod = build_worker_pod(params(&pool, &build)).unwrap();
        let spec = pod.spec.unwrap();
        let worker = &spec.containers[0];
        let worker_env = drs_env(worker);
        // The worker knows the shared directory and nothing else.
        assert_eq!(
            worker_env.keys().copied().collect::<Vec<_>>(),
            [image_push::ENV_LAYER_DIR]
        );

        // The sidecar is the operator image, as root, with everything else.
        let sidecar = &spec.init_containers.as_ref().unwrap()[0];
        let operator_image = params(&pool, &build).operator_image;
        assert_eq!(sidecar.image.as_deref(), operator_image);
        assert_eq!(sidecar.restart_policy.as_deref(), Some("Always"));
        let sidecar_env = drs_env(sidecar);
        assert_eq!(
            sidecar_env[image_push::ENV_TARGET].value,
            build.spec.image_target
        );
        for (name, key) in [
            (image_push::ENV_REGISTRY_USERNAME, "username"),
            (image_push::ENV_REGISTRY_PASSWORD, "password"),
        ] {
            let source = sidecar_env[name].value_from.as_ref().unwrap();
            let secret = source.secret_key_ref.as_ref().unwrap();
            assert_eq!(
                (secret.name.as_str(), secret.key.as_str()),
                ("registry-push", key)
            );
        }

        // Both mount the same emptyDir at the path the env var names.
        let shared = &sidecar.volume_mounts.as_ref().unwrap()[0];
        assert!(worker.volume_mounts.as_ref().unwrap().contains(shared));
        for env in [&worker_env, &sidecar_env] {
            assert_eq!(
                env[image_push::ENV_LAYER_DIR].value.as_deref(),
                Some(shared.mount_path.as_str())
            );
        }
        let volumes = spec.volumes.as_ref().unwrap();
        assert!(
            volumes
                .iter()
                .any(|v| v.name == shared.name && v.empty_dir.is_some())
        );

        // No operator image, no sidecar, no build.
        let err = build_worker_pod(WorkerPodParams {
            operator_image: None,
            ..params(&pool, &build)
        })
        .unwrap_err();
        assert!(matches!(err, Error::Config(_)), "{err}");
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
        assert_eq!(mount.mount_path, ResumeConfig::DEFAULT_HOME_DIR);
        let env = worker.env.as_ref().unwrap();
        let home = env.iter().find(|e| e.name == ENV_HOME).unwrap();
        assert_eq!(home.value.as_deref(), Some(ResumeConfig::DEFAULT_HOME_DIR));
        let cache_dir = env.iter().find(|e| e.name == ENV_WORKER_CACHE_DIR).unwrap();
        assert_eq!(
            cache_dir.value.as_deref(),
            Some("/home/ubuntu/.devin-worker/cache")
        );
    }

    #[test]
    fn home_dir_override_moves_the_mount() {
        let mut pool = pool();
        pool.spec.resume.home_dir = Some("/workspace".to_string());
        let session = session("devin-1");
        let mut p = params(&pool, &session);
        let pvc_name = state_pvc_name("devin-1");
        p.state_pvc_name = Some(&pvc_name);
        let pod = build_worker_pod(p).unwrap();

        let spec = pod.spec.as_ref().unwrap();
        let worker = &spec.containers[0];
        let mount = worker.volume_mounts.as_ref().unwrap().last().unwrap();
        assert_eq!(mount.mount_path, "/workspace");
        let env = worker.env.as_ref().unwrap();
        assert_eq!(
            env.iter()
                .find(|e| e.name == ENV_HOME)
                .unwrap()
                .value
                .as_deref(),
            Some("/workspace")
        );
    }

    #[test]
    fn provider_annotations_survive_override_replacement() {
        let mut pool = pool();
        pool.spec.worker.overrides.annotations = Some(BTreeMap::new());
        let session = session("devin-1");
        let mut p = params(&pool, &session);
        let pin = BTreeMap::from([(
            "podsnapshot.gke.io/ps-name".to_string(),
            "snap-uuid".to_string(),
        )]);
        p.provider_annotations = &pin;
        let pod = build_worker_pod(p).unwrap();
        assert_eq!(
            pod.metadata.annotations.as_ref().unwrap()["podsnapshot.gke.io/ps-name"],
            "snap-uuid"
        );
    }

    #[test]
    fn network_policy_label_is_operator_owned() {
        let mut pool = pool();
        let session = session("devin-1");
        fn labels(p: WorkerPodParams<'_>) -> BTreeMap<String, String> {
            build_worker_pod(p).unwrap().metadata.labels.unwrap()
        }
        let other_session = network_policy_name("devin-2");
        let name = network_policy_name("devin-1");

        for (template_labels, override_labels) in [
            (BTreeMap::new(), Some(BTreeMap::new())),
            (
                BTreeMap::from([(LABEL_NETWORK_POLICY.to_string(), other_session.clone())]),
                None,
            ),
            (
                BTreeMap::new(),
                Some(BTreeMap::from([(
                    LABEL_NETWORK_POLICY.to_string(),
                    other_session.clone(),
                )])),
            ),
        ] {
            let template = pool.spec.worker.template.metadata.get_or_insert_default();
            template.labels = Some(template_labels);
            pool.spec.worker.overrides.labels = override_labels;

            assert!(!labels(params(&pool, &session)).contains_key(LABEL_NETWORK_POLICY));

            let mut p = params(&pool, &session);
            p.network_policy_name = Some(&name);
            assert_eq!(labels(p)[LABEL_NETWORK_POLICY], name);
        }
    }

    #[test]
    fn static_base_url_follows_the_template_env() {
        let mut pool = pool();
        let session = session("devin-1");
        let static_base_url = |pool: &OutpostPool| {
            let pod = build_worker_pod(params(pool, &session)).unwrap();
            worker_static_base_url(&pod, "devin-worker").map(str::to_string)
        };
        assert_eq!(static_base_url(&pool).unwrap(), DEFAULT_STATIC_BASE_URL);

        let env = &mut pool.spec.worker.template.spec.as_mut().unwrap().containers[0].env;
        *env = Some(vec![EnvVar {
            name: ENV_STATIC_BASE_URL.to_string(),
            value: Some("https://static.example/remote".to_string()),
            ..Default::default()
        }]);
        assert_eq!(
            static_base_url(&pool).unwrap(),
            "https://static.example/remote"
        );

        let env = &mut pool.spec.worker.template.spec.as_mut().unwrap().containers[0].env;
        *env = Some(vec![EnvVar {
            name: ENV_STATIC_BASE_URL.to_string(),
            value_from: Some(EnvVarSource::default()),
            ..Default::default()
        }]);
        assert!(matches!(static_base_url(&pool), Err(Error::Config(_))));
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
        let pvc = build_state_pvc(&pool, &session("devin-1"), &golden_resume()).unwrap();
        let spec = pvc.spec.as_ref().unwrap();
        assert_eq!(
            spec.resources.as_ref().unwrap().requests.as_ref().unwrap()["storage"].0,
            ResumeConfig::DEFAULT_VOLUME_SIZE
        );
        assert_eq!(spec.storage_class_name, None);

        let resume = ResumeConfig {
            volume_size: Some("100Gi".to_string()),
            storage_class_name: Some("fast".to_string()),
            ..golden_resume()
        };
        let pvc = build_state_pvc(&pool, &session("devin-1"), &resume).unwrap();
        let spec = pvc.spec.as_ref().unwrap();
        assert_eq!(
            spec.resources.as_ref().unwrap().requests.as_ref().unwrap()["storage"].0,
            "100Gi"
        );
        assert_eq!(spec.storage_class_name.as_deref(), Some("fast"));
    }

    fn golden_resume() -> ResumeConfig {
        ResumeConfig {
            volume_data_source: Some(TypedLocalObjectReference {
                api_group: Some("snapshot.storage.k8s.io".to_string()),
                kind: "VolumeSnapshot".to_string(),
                name: "golden-home".to_string(),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn state_pvc_requires_and_clones_the_data_source() {
        let pool = pool();
        let err = build_state_pvc(&pool, &session("devin-1"), &Default::default()).unwrap_err();
        assert!(matches!(err, Error::Config(_)), "{err}");

        let resume = golden_resume();
        let pvc = build_state_pvc(&pool, &session("devin-1"), &resume).unwrap();
        assert_eq!(pvc.spec.unwrap().data_source, resume.volume_data_source);
    }

    #[test]
    fn session_image_wins_over_the_pool_image_and_seeds_its_own_home() {
        let mut pool = pool();
        pool.spec.worker.overrides.image = Some("img:pool".to_string());
        let mut session = session("devin-1");
        session.spec.image = Some("reg/drs@sha256:abc".to_string());

        // No golden volume: the state volume starts empty.
        let pvc = build_state_pvc(&pool, &session, &Default::default()).unwrap();
        assert_eq!(pvc.spec.unwrap().data_source, None);

        let pod = build_worker_pod(WorkerPodParams {
            state_pvc_name: Some("pvc-x"),
            ..params(&pool, &session)
        })
        .unwrap();
        let spec = pod.spec.unwrap();
        let worker = spec
            .containers
            .iter()
            .find(|c| c.name == "devin-worker")
            .unwrap();
        assert_eq!(worker.image.as_deref(), Some("reg/drs@sha256:abc"));

        // The seed runs the session image and writes to the volume the worker
        // mounts as home, at a path that leaves the image's home readable.
        let seed = &spec.init_containers.as_ref().unwrap()[0];
        assert_eq!(seed.image.as_deref(), Some("reg/drs@sha256:abc"));
        let seed_mount = &seed.volume_mounts.as_ref().unwrap()[0];
        let home_mount = worker
            .volume_mounts
            .as_ref()
            .unwrap()
            .iter()
            .find(|m| m.mount_path == "/home/ubuntu")
            .unwrap();
        assert_eq!(seed_mount.name, home_mount.name);
        assert_ne!(seed_mount.mount_path, home_mount.mount_path);

        // Without a session image the pool's own image and no seed.
        session.spec.image = None;
        let pod = build_worker_pod(params(&pool, &session)).unwrap();
        let spec = pod.spec.unwrap();
        assert_eq!(
            spec.containers
                .iter()
                .find(|c| c.name == "devin-worker")
                .unwrap()
                .image
                .as_deref(),
            Some("img:pool")
        );
        assert!(spec.init_containers.is_none());
    }
}
