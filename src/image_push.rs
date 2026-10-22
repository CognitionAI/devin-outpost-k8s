//! `push-layer`: the push sidecar of an image-build pod.
//!
//! A DRS build session on an outpost runs the customer's blueprints in the
//! worker container, with sudo. The image of the result must be pushed with
//! a registry credential, and that credential must not be readable from the
//! worker. So the worker never has it: the capture (root in the worker)
//! writes the layer it built into a directory shared with this sidecar, and
//! the sidecar, which runs the operator image and no customer code, holds
//! the credential and talks to the registry (through `oci-client`).
//!
//! ## Protocol, over the shared `emptyDir`
//!
//! 1. The sidecar makes the directory `0700` at startup (it is a native
//!    sidecar, so this happens before the worker starts); the capture does
//!    the same before any blueprint runs. That keeps the box user out, not
//!    root in the worker (blueprints have sudo), so the sidecar trusts
//!    nothing it finds in the directory (see `open_layer`). Files, not a port:
//!    the containers of a pod share `localhost`, and anything listening
//!    there would be reachable by blueprint code.
//! 2. The capture writes [`LAYER_FILE`], a gzip tar layer with whiteouts,
//!    whole (renamed into place).
//! 3. The sidecar appends it to the base image ([`ENV_BASE_IMAGE`]), pushes
//!    the result to the target the queue entry named ([`ENV_TARGET`], from
//!    `spec.image_target`) and writes [`RESULT_FILE`] atomically:
//!    `{"image": "<repository>@sha256:..."}` or `{"error": "..."}`.
//! 4. The build job reads the result through the worker.
//!
//! The target is the control plane's, not the worker's: anything root in the
//! worker could write would otherwise aim the push at another org's path on
//! a shared pool. The worker is told nothing but the directory.

use std::io::{Read, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::Duration;

use oci_client::client::{ClientConfig, ClientProtocol};
use oci_client::config::{ConfigFile, History};
use oci_client::manifest::{
    IMAGE_DOCKER_LAYER_GZIP_MEDIA_TYPE, IMAGE_LAYER_GZIP_MEDIA_TYPE, OciDescriptor,
    OciImageManifest, OciManifest,
};
use oci_client::secrets::RegistryAuth;
use oci_client::{Client, Reference};
use serde::Serialize;
use sha2::{Digest, Sha256};
use tracing::{error, info, warn};

use crate::{Error, Result};

/// Env vars of the sidecar container. [`ENV_LAYER_DIR`] is also set on the
/// worker container; the others never are.
pub const ENV_LAYER_DIR: &str = "DEVIN_DRS_LAYER_DIR";
pub const ENV_BASE_IMAGE: &str = "DEVIN_DRS_BASE_IMAGE";
pub const ENV_TARGET: &str = "DEVIN_DRS_TARGET";
pub const ENV_REGISTRY_USERNAME: &str = "DEVIN_DRS_REGISTRY_USERNAME";
pub const ENV_REGISTRY_PASSWORD: &str = "DEVIN_DRS_REGISTRY_PASSWORD";
pub const ENV_REGISTRY_INSECURE: &str = "DEVIN_DRS_REGISTRY_INSECURE";

/// Where both containers mount the shared directory.
pub const LAYER_DIR: &str = "/var/lib/devin-drs";
pub const LAYER_FILE: &str = "layer.tar.gz";
pub const RESULT_FILE: &str = "result.json";

/// Subcommand of the operator binary that runs this sidecar.
pub const SUBCOMMAND: &str = "push-layer";

const POLL_INTERVAL: Duration = Duration::from_secs(2);

/// What the capture gets back. Exactly one field is set.
#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum Response {
    Image(String),
    Error(String),
}

#[derive(Debug, Clone)]
struct PushConfig {
    layer_dir: PathBuf,
    base_image: Reference,
    target: Reference,
    auth: RegistryAuth,
    insecure: bool,
}

impl PushConfig {
    fn from_env() -> Result<Self> {
        let get = |key: &str| std::env::var(key).ok().filter(|v| !v.is_empty());
        let reference = |key: &str| -> Result<Reference> {
            let raw = get(key).ok_or_else(|| Error::Config(format!("{key} is not set")))?;
            raw.parse()
                .map_err(|e| Error::Config(format!("{key} {raw:?}: {e}")))
        };
        let base_image = reference(ENV_BASE_IMAGE)?;
        let target = reference(ENV_TARGET)?;
        if base_image.resolve_registry() != target.resolve_registry() {
            return Err(Error::Config(format!(
                "base image {base_image} and target {target} are not in the same registry"
            )));
        }
        if target.tag().is_none() || target.digest().is_some() {
            return Err(Error::Config(format!("target {target} must be a tag")));
        }
        let auth = match get(ENV_REGISTRY_USERNAME) {
            Some(user) => RegistryAuth::Basic(user, get(ENV_REGISTRY_PASSWORD).unwrap_or_default()),
            None => RegistryAuth::Anonymous,
        };
        Ok(Self {
            layer_dir: get(ENV_LAYER_DIR)
                .map(PathBuf::from)
                .unwrap_or(LAYER_DIR.into()),
            base_image,
            target,
            auth,
            insecure: get(ENV_REGISTRY_INSECURE).is_some(),
        })
    }
}

/// Entry point of the `push-layer` subcommand: serve until the pod stops the
/// sidecar.
pub async fn main() -> anyhow::Result<()> {
    let config = PushConfig::from_env()?;
    info!(dir = %config.layer_dir.display(), base = %config.base_image, target = %config.target, "starting push-layer");
    std::fs::set_permissions(&config.layer_dir, std::fs::Permissions::from_mode(0o700))?;
    let layer_path = config.layer_dir.join(LAYER_FILE);
    loop {
        if std::fs::symlink_metadata(&layer_path).is_ok() {
            let response = match push(&config, &layer_path).await {
                Ok(image) => {
                    info!(%image, "pushed");
                    Response::Image(image)
                }
                Err(e) => {
                    error!(error = %e, "push failed");
                    Response::Error(e.to_string())
                }
            };
            let result_path = config.layer_dir.join(RESULT_FILE);
            let tmp = result_path.with_extension("tmp");
            write_new(&tmp, &serde_json::to_vec(&response)?)?;
            std::fs::rename(&tmp, &result_path)?;
            if let Err(e) = std::fs::remove_file(&layer_path) {
                warn!(error = %e, "could not remove the layer");
            }
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Digests of the layer file as the manifest (`blob`) and the image config
/// (`diff_id`, of the uncompressed tar) describe it, in one pass.
struct LayerDigests {
    diff_id: String,
    blob: String,
    size: u64,
}

/// Files in the shared directory are opened without following symlinks and
/// must be regular, and the sidecar only ever creates its own files
/// ([`write_new`]): a symlink planted by a blueprint would otherwise make the
/// sidecar read, or overwrite, a path of its own filesystem.
fn open_layer(path: &Path) -> Result<std::fs::File> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|e| Error::Config(format!("open {}: {e}", path.display())))?;
    let meta = file
        .metadata()
        .map_err(|e| Error::Config(format!("stat {}: {e}", path.display())))?;
    if !meta.is_file() {
        return Err(Error::Config(format!(
            "{} is not a regular file",
            path.display()
        )));
    }
    Ok(file)
}

fn write_new(path: &Path, contents: &[u8]) -> std::io::Result<()> {
    match std::fs::remove_file(path) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e),
        _ => {}
    }
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    file.write_all(contents)?;
    file.sync_all()
}

fn digest_layer(path: &Path) -> Result<LayerDigests> {
    let mut file = open_layer(path)?;
    let (mut compressed, mut plain, mut size) = (Sha256::new(), Sha256::new(), 0u64);
    let mut gunzip = flate2::write::GzDecoder::new(HashSink(&mut plain));
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = file
            .read(&mut buf)
            .map_err(|e| Error::Config(format!("read layer: {e}")))?;
        if n == 0 {
            break;
        }
        compressed.update(&buf[..n]);
        gunzip
            .write_all(&buf[..n])
            .map_err(|e| Error::Config(format!("layer is not gzip: {e}")))?;
        size += n as u64;
    }
    gunzip
        .finish()
        .map_err(|e| Error::Config(format!("layer is not gzip: {e}")))?;
    Ok(LayerDigests {
        diff_id: format!("sha256:{:x}", plain.finalize()),
        blob: format!("sha256:{:x}", compressed.finalize()),
        size,
    })
}

struct HashSink<'a>(&'a mut Sha256);

impl Write for HashSink<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.update(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn append_layer(
    manifest: &mut OciImageManifest,
    config: &mut ConfigFile,
    layer: &LayerDigests,
) -> Result<Vec<u8>> {
    config.rootfs.diff_ids.push(layer.diff_id.clone());
    config.history.get_or_insert_default().push(History {
        created: Some(chrono::Utc::now()),
        created_by: Some("devin drs build capture".to_string()),
        ..Default::default()
    });
    let config_bytes = serde_json::to_vec(config)?;
    manifest.config.digest = format!("sha256:{:x}", Sha256::digest(&config_bytes));
    manifest.config.size = config_bytes.len() as i64;
    let oci = manifest
        .media_type
        .as_deref()
        .unwrap_or(&manifest.config.media_type)
        .contains("oci");
    manifest.layers.push(OciDescriptor {
        media_type: (if oci {
            IMAGE_LAYER_GZIP_MEDIA_TYPE
        } else {
            IMAGE_DOCKER_LAYER_GZIP_MEDIA_TYPE
        })
        .to_string(),
        digest: layer.blob.clone(),
        size: layer.size as i64,
        ..Default::default()
    });
    Ok(config_bytes)
}

fn manifest_payload(manifest: &OciManifest, target: &Reference) -> Result<(Vec<u8>, Reference)> {
    let body = serde_json::to_vec(manifest)?;
    let digest = format!("sha256:{:x}", Sha256::digest(&body));
    Ok((body, target.clone_with_digest(digest)))
}

/// Append the layer to the base image and push it to the target; returns
/// `<target repository>@sha256:<digest>`.
async fn push(config: &PushConfig, layer_path: &Path) -> Result<String> {
    let layer = tokio::task::spawn_blocking({
        let path = layer_path.to_path_buf();
        move || digest_layer(&path)
    })
    .await
    .map_err(|e| Error::Config(format!("digest task: {e}")))??;
    info!(size = layer.size, digest = %layer.blob, "layer");

    let client = Client::new(ClientConfig {
        protocol: if config.insecure {
            ClientProtocol::Http
        } else {
            ClientProtocol::Https
        },
        // The layer is streamed once; a chunked upload would need a replay.
        use_monolithic_push: true,
        ..Default::default()
    });
    let registry =
        |e: oci_client::errors::OciDistributionError| Error::Config(format!("registry: {e}"));

    // The base's manifest for this platform, and its config.
    let (mut manifest, _, config_json) = client
        .pull_manifest_and_config(&config.base_image, &config.auth)
        .await
        .map_err(registry)?;
    let mut image_config: ConfigFile = serde_json::from_str(&config_json)?;

    // Base blobs are mounted into the target, not re-uploaded. A registry
    // that declines a mount answers 202 with an upload session instead of
    // 201 (`oci-client` reports it as a spec violation); then the blob is
    // copied through the sidecar.
    let (mut mounted, mut copied) = (0, 0);
    if config.base_image.repository() != config.target.repository() {
        for blob in &manifest.layers {
            match client
                .mount_blob(&config.target, &config.base_image, &blob.digest)
                .await
            {
                Ok(()) => mounted += 1,
                Err(e) => {
                    warn!(digest = %blob.digest, error = %e, "mount declined, copying");
                    let pulled = client
                        .pull_blob_stream(&config.base_image, blob)
                        .await
                        .map_err(registry)?;
                    let size = pulled.content_length.unwrap_or(blob.size as u64);
                    client
                        .push_blob_stream(
                            &config.target,
                            io_stream(pulled),
                            &blob.digest,
                            Some(size),
                        )
                        .await
                        .map_err(registry)?;
                    copied += 1;
                }
            }
        }
    }

    let config_bytes = append_layer(&mut manifest, &mut image_config, &layer)?;
    client
        .push_blob(&config.target, config_bytes, &manifest.config.digest)
        .await
        .map_err(registry)?;

    let file = tokio::fs::File::from_std(open_layer(layer_path)?);
    let stream = io_stream(tokio_util::io::ReaderStream::with_capacity(file, 1 << 20));
    client
        .push_blob_stream(&config.target, stream, &layer.blob, Some(layer.size))
        .await
        .map_err(registry)?;

    // `push_manifest` returns the manifest's URL, not its digest, and the
    // registry may not echo a digest at all; so the bytes are pushed as is and
    // digested here, then the digest is checked to be pullable.
    let manifest = OciManifest::Image(manifest);
    let (body, by_digest) = manifest_payload(&manifest, &config.target)?;
    client
        .push_manifest_raw(
            &config.target,
            body,
            manifest.content_type().parse().expect("media type header"),
        )
        .await
        .map_err(registry)?;
    let served = client
        .fetch_manifest_digest(&by_digest, &config.auth)
        .await
        .map_err(registry)?;
    if Some(served.as_str()) != by_digest.digest() {
        return Err(Error::Config(format!(
            "registry serves {by_digest} as {served}"
        )));
    }
    info!(mounted, copied, "pushed layer");
    Ok(by_digest.to_string())
}

fn io_stream<S>(
    stream: S,
) -> impl futures::Stream<Item = oci_client::errors::Result<bytes::Bytes>> + Send + 'static
where
    S: futures::Stream<Item = std::io::Result<bytes::Bytes>> + Send + 'static,
{
    futures::StreamExt::map(stream, |r| {
        r.map_err(|e| oci_client::errors::OciDistributionError::GenericError(Some(e.to_string())))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digests_the_blob_and_what_it_decompresses_to() {
        let dir = std::env::temp_dir().join(format!("drs-layer-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let path = dir.join("layer.tar.gz");
        let plain = b"not really a tar, but gzip does not mind".repeat(1000);
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        gz.write_all(&plain).unwrap();
        let blob = gz.finish().unwrap();
        std::fs::write(&path, &blob).unwrap();

        let layer = digest_layer(&path).unwrap();
        assert_eq!(layer.size, blob.len() as u64);
        assert_eq!(layer.blob, format!("sha256:{:x}", Sha256::digest(&blob)));
        assert_eq!(
            layer.diff_id,
            format!("sha256:{:x}", Sha256::digest(&plain))
        );

        std::fs::write(&path, b"plain text").unwrap();
        assert!(digest_layer(&path).is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn response_has_exactly_the_set_field() {
        assert_eq!(
            serde_json::to_string(&Response::Image("r@sha256:0".into())).unwrap(),
            r#"{"image":"r@sha256:0"}"#
        );
        assert_eq!(
            serde_json::to_string(&Response::Error("x".into())).unwrap(),
            r#"{"error":"x"}"#
        );
    }

    #[test]
    fn layer_must_be_a_regular_file_and_result_never_follows_a_link() {
        let dir = std::env::temp_dir().join(format!("drs-link-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let elsewhere = dir.join("elsewhere");
        std::fs::write(&elsewhere, b"owned by the sidecar").unwrap();
        let link = dir.join(LAYER_FILE);
        std::os::unix::fs::symlink(&elsewhere, &link).unwrap();
        assert!(open_layer(&link).is_err());

        write_new(&link, b"{}").unwrap();
        assert_eq!(std::fs::read(&elsewhere).unwrap(), b"owned by the sidecar");
        assert_eq!(std::fs::read(&link).unwrap(), b"{}");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn assembled_manifest_preserves_base_layers_and_names_its_exact_bytes() {
        for (manifest_type, layer_type) in [
            (
                "application/vnd.oci.image.manifest.v1+json",
                IMAGE_LAYER_GZIP_MEDIA_TYPE,
            ),
            (
                "application/vnd.docker.distribution.manifest.v2+json",
                IMAGE_DOCKER_LAYER_GZIP_MEDIA_TYPE,
            ),
        ] {
            let mut manifest: OciImageManifest = serde_json::from_value(serde_json::json!({
                "schemaVersion": 2, "mediaType": manifest_type,
                "config": {"mediaType": "application/vnd.oci.image.config.v1+json", "digest": "sha256:old", "size": 1},
                "layers": [{"mediaType": layer_type, "digest": "sha256:base", "size": 7}]
            }))
            .unwrap();
            let base_layer = manifest.layers[0].clone();
            let mut config: ConfigFile = serde_json::from_value(serde_json::json!({
                "architecture": "amd64", "os": "linux", "config": {"User": "ubuntu"},
                "rootfs": {"type": "layers", "diff_ids": ["sha256:base"]}
            }))
            .unwrap();
            let layer = LayerDigests {
                diff_id: "sha256:plain".into(),
                blob: "sha256:compressed".into(),
                size: 12,
            };
            let config_bytes = append_layer(&mut manifest, &mut config, &layer).unwrap();
            assert_eq!(manifest.layers.len(), 2);
            assert_eq!(
                serde_json::to_value(&manifest.layers[0]).unwrap(),
                serde_json::to_value(&base_layer).unwrap()
            );
            assert_eq!(manifest.layers[1].media_type, layer_type);
            assert_eq!(manifest.layers[1].digest, layer.blob);
            assert_eq!(manifest.layers[1].size, 12);
            assert_eq!(
                manifest.config.digest,
                format!("sha256:{:x}", Sha256::digest(&config_bytes))
            );
            assert_eq!(manifest.config.size, config_bytes.len() as i64);
            let config_json: serde_json::Value = serde_json::from_slice(&config_bytes).unwrap();
            assert_eq!(config_json["config"]["User"], "ubuntu");
            assert_eq!(
                config_json["rootfs"]["diff_ids"],
                serde_json::json!(["sha256:base", "sha256:plain"])
            );
            let target: Reference = "registry.example/team/image:build".parse().unwrap();
            let (body, reference) =
                manifest_payload(&OciManifest::Image(manifest), &target).unwrap();
            let expected_digest = format!("sha256:{:x}", Sha256::digest(&body));
            assert_eq!(reference.digest(), Some(expected_digest.as_str()));
            assert_eq!(reference.tag(), None);
            assert_eq!(
                reference.to_string(),
                format!("registry.example/team/image@{expected_digest}")
            );
            assert_eq!(
                reference.to_string().parse::<Reference>().unwrap(),
                reference
            );
        }
    }
}
