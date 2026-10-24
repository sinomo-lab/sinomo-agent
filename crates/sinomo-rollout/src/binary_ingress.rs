//! Durable streamed-binary ingress for daemon rollout activation.
//!
//! The rollout owner validates manifests, receives and verifies one artifact,
//! preserves upload identity across retries, and records the terminal response.
//! A process host only sees a fully verified activation manifest.

use crate::{
    ClusterRolloutBinaryManifest, ClusterRolloutBinaryRequest, ClusterRolloutBinaryResponse,
    ClusterRolloutNativeDylibModuleUploadSpec, ClusterRolloutUploadArtifactSpec,
    LocalRolloutNativeDylibModuleArtifact, RolloutError, SelfRolloutPaths, ensure_rollout_layout,
    read_object, write_object,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::io::Read;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

const ROLLOUT_BODY_BUFFER_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RolloutUploadStatus {
    pub activation_started: bool,
    pub receipt: Option<ClusterRolloutBinaryResponse>,
    #[serde(default)]
    pub pending_activation: Option<bool>,
}

/// Read-only reconciliation by the complete immutable manifest, including the
/// upload ID. An unsupported route on an older daemon is not absence evidence.
pub fn read_rollout_upload_status(
    paths: &SelfRolloutPaths,
    manifest: &ClusterRolloutBinaryManifest,
) -> Result<RolloutUploadStatus, RolloutError> {
    validate_rollout_binary_manifest(manifest)?;
    let path = rollout_upload_manifest_path(paths, &manifest.upload_id)?;
    if path.exists() {
        let existing: ClusterRolloutBinaryManifest = read_object(&path)?;
        if existing != *manifest {
            return Err(RolloutError::InvalidArtifact {
                detail: "upload status manifest mismatch".into(),
            });
        }
    }
    Ok(RolloutUploadStatus {
        pending_activation: Some(
            crate::rollout_main_activation_path(paths).exists()
                || crate::rollout_shadow_finalize_path(paths).exists(),
        ),
        activation_started: rollout_upload_dir(paths, &manifest.upload_id)?
            .join("activation-started.sobj")
            .exists(),
        receipt: read_rollout_binary_receipt(paths, manifest)?.map(|receipt| receipt.response),
    })
}

impl RolloutUploadStatus {
    pub fn can_retire(&self) -> bool {
        self.pending_activation == Some(false)
            && self.receipt.as_ref().is_some_and(|receipt| {
                matches!(receipt.status.as_str(), "accepted" | "rejected" | "failed")
            })
    }
}

#[derive(Debug)]
pub struct PreparedRolloutBinaryActivation {
    pub rollout_paths: SelfRolloutPaths,
    pub manifest: ClusterRolloutBinaryManifest,
    // Serializes ingress and activation for this upload across daemon processes.
    _upload_lock: RolloutUploadLock,
}

// A duplicated descriptor (for example across fork before exec) must not retain
// admission after the actual ingress/activation owner has ended. No alias is
// exposed to callers; the guard moves with PreparedRolloutBinaryActivation.
#[derive(Debug)]
struct RolloutUploadLock(fs::File);

impl Drop for RolloutUploadLock {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(&self.0);
    }
}

#[derive(Debug)]
pub enum RolloutBinaryIngress {
    Complete(ClusterRolloutBinaryResponse),
    Activate(PreparedRolloutBinaryActivation),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ClusterRolloutBinaryReceipt {
    manifest: ClusterRolloutBinaryManifest,
    response: ClusterRolloutBinaryResponse,
}

pub async fn receive_rollout_binary_ingress<R>(
    data_dir: &Path,
    request: ClusterRolloutBinaryRequest,
    body: &mut R,
) -> Result<RolloutBinaryIngress, RolloutError>
where
    R: AsyncRead + Unpin,
{
    validate_rollout_binary_manifest(&request.manifest)?;
    let artifact = request
        .manifest
        .artifact(&request.artifact_id)
        .cloned()
        .ok_or_else(|| RolloutError::InvalidArtifact {
            detail: format!(
                "rollout manifest does not contain artifact {}",
                request.artifact_id
            ),
        })?;
    let rollout_paths = SelfRolloutPaths::new(data_dir);
    ensure_rollout_layout(&rollout_paths)?;
    if let Err(error) = crate::reclaim_rollout_transfers(&rollout_paths) {
        tracing::warn!(%error, "could not reclaim obsolete rollout transfers");
    }
    let upload_dir = rollout_upload_dir(&rollout_paths, &request.manifest.upload_id)?;
    fs::create_dir_all(&upload_dir).map_err(|error| RolloutError::Io {
        path: upload_dir.display().to_string(),
        detail: error.to_string(),
    })?;
    let lock_path = upload_dir.join("ingress.lock");
    let upload_lock = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .map_err(|error| RolloutError::Io {
            path: lock_path.display().to_string(),
            detail: error.to_string(),
        })?;
    fs2::FileExt::try_lock_exclusive(&upload_lock).map_err(|error| RolloutError::Io {
        path: lock_path.display().to_string(),
        detail: format!("upload is already being received or activated: {error}"),
    })?;
    let upload_lock = RolloutUploadLock(upload_lock);
    ensure_rollout_upload_manifest(&rollout_paths, &request.manifest)?;
    let receipt = read_rollout_binary_receipt(&rollout_paths, &request.manifest)?;
    let activation_claim = upload_dir.join("activation-started.sobj");
    let claimed = activation_claim.exists();
    if claimed {
        let claimed_manifest: ClusterRolloutBinaryManifest = read_object(&activation_claim)?;
        if claimed_manifest != request.manifest {
            return Err(RolloutError::InvalidArtifact {
                detail: "activation claim manifest mismatch".to_string(),
            });
        }
    }
    receive_rollout_binary_artifact(
        &rollout_paths,
        &request.manifest,
        &artifact,
        body,
        receipt.is_none() && !claimed,
    )
    .await?;

    if let Some(receipt) = receipt {
        remove_rollout_uploaded_artifacts(&rollout_paths, &request.manifest);
        return Ok(RolloutBinaryIngress::Complete(receipt.response));
    }

    if claimed {
        return Ok(RolloutBinaryIngress::Complete(
            ClusterRolloutBinaryResponse {
                status: "in-progress".to_string(),
                detail: "this upload already started activation; reconcile its durable progress"
                    .to_string(),
                main_binary: rollout_paths.main_binary,
                shadow_binary: rollout_paths.shadow_binary,
                source_head: None,
            },
        ));
    }

    if !request.activate {
        return Ok(RolloutBinaryIngress::Complete(
            ClusterRolloutBinaryResponse {
                status: "staged".to_string(),
                detail: format!(
                    "received and verified rollout artifact {} ({} bytes)",
                    artifact.artifact_id, artifact.total_bytes
                ),
                main_binary: rollout_paths.main_binary,
                shadow_binary: rollout_paths.shadow_binary,
                source_head: None,
            },
        ));
    }

    // Validate the complete upload before claiming its one activation. The
    // durable claim survives a lost reply or process restart. An unresolved
    // claim must be observed/recovered, never executed a second time.
    verify_rollout_uploaded_artifact(
        &rollout_paths,
        &request.manifest.upload_id,
        &request.manifest.binary,
    )?;
    for module in &request.manifest.native_dylib_modules {
        verify_rollout_uploaded_artifact(
            &rollout_paths,
            &request.manifest.upload_id,
            &module.artifact,
        )?;
    }
    write_object(&activation_claim, &request.manifest)?;
    Ok(RolloutBinaryIngress::Activate(
        PreparedRolloutBinaryActivation {
            rollout_paths,
            manifest: request.manifest,
            _upload_lock: upload_lock,
        },
    ))
}

pub fn complete_rollout_binary_activation(
    prepared: &PreparedRolloutBinaryActivation,
    response: &ClusterRolloutBinaryResponse,
) -> Result<(), RolloutError> {
    write_rollout_binary_receipt(&prepared.rollout_paths, &prepared.manifest, response)?;
    remove_rollout_uploaded_artifacts(&prepared.rollout_paths, &prepared.manifest);
    Ok(())
}

pub fn validate_rollout_binary_manifest(
    manifest: &ClusterRolloutBinaryManifest,
) -> Result<(), RolloutError> {
    validate_rollout_upload_component("upload id", &manifest.upload_id)?;
    let mut artifact_ids = BTreeSet::new();
    for artifact in manifest.artifacts() {
        validate_rollout_upload_component("artifact id", &artifact.artifact_id)?;
        if artifact.total_bytes == 0 {
            return Err(RolloutError::InvalidArtifact {
                detail: format!(
                    "rollout artifact {} must contain at least one byte",
                    artifact.artifact_id
                ),
            });
        }
        if artifact.blake3.len() != blake3::OUT_LEN {
            return Err(RolloutError::InvalidArtifact {
                detail: format!(
                    "rollout artifact {} has a {}-byte blake3 digest; expected {}",
                    artifact.artifact_id,
                    artifact.blake3.len(),
                    blake3::OUT_LEN
                ),
            });
        }
        if !artifact_ids.insert(artifact.artifact_id.as_str()) {
            return Err(RolloutError::InvalidArtifact {
                detail: format!(
                    "rollout manifest contains duplicate artifact id {}",
                    artifact.artifact_id
                ),
            });
        }
    }
    manifest
        .total_bytes()
        .ok_or_else(|| RolloutError::InvalidArtifact {
            detail: "rollout manifest total byte count overflowed u64".to_string(),
        })?;
    Ok(())
}

pub(crate) fn reclaim_upload_payloads(
    paths: &SelfRolloutPaths,
    grace: std::time::Duration,
) -> Result<(), RolloutError> {
    let directory = paths.root.join("staged/uploads");
    let entries = match fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(RolloutError::Io {
                path: directory.display().to_string(),
                detail: error.to_string(),
            });
        }
    };
    for entry in entries {
        let entry = entry.map_err(|e| RolloutError::Io {
            path: directory.display().to_string(),
            detail: e.to_string(),
        })?;
        if !entry.file_type().is_ok_and(|t| t.is_dir()) {
            continue;
        }
        let upload = entry.path();
        let lock_path = upload.join("ingress.lock");
        if fs::symlink_metadata(&lock_path).is_ok_and(|m| m.file_type().is_symlink()) {
            continue;
        }
        let lock = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)
            .map_err(|e| RolloutError::Io {
                path: lock_path.display().to_string(),
                detail: e.to_string(),
            })?;
        if fs2::FileExt::try_lock_exclusive(&lock).is_err() {
            continue;
        }
        let _lock = RolloutUploadLock(lock);
        let manifest: ClusterRolloutBinaryManifest =
            match read_object(&upload.join("manifest.sobj")) {
                Ok(m) => m,
                Err(_) => continue,
            };
        if validate_rollout_binary_manifest(&manifest).is_err()
            || upload.file_name().and_then(|s| s.to_str()) != Some(manifest.upload_id.as_str())
        {
            continue;
        }
        let terminal = read_rollout_binary_receipt(paths, &manifest)?.is_some_and(|r| {
            matches!(
                r.response.status.as_str(),
                "accepted" | "rejected" | "failed" | "rolled-back"
            )
        });
        // Unresolved claims are recovery evidence, never inferred terminal by age.
        if !terminal && upload.join("activation-started.sobj").exists() {
            continue;
        }
        for artifact in manifest.artifacts() {
            let path =
                rollout_upload_artifact_path(paths, &manifest.upload_id, &artifact.artifact_id)?;
            let Ok(metadata) = fs::symlink_metadata(&path) else {
                continue;
            };
            if !metadata.file_type().is_file() {
                continue;
            }
            let expired = metadata
                .modified()
                .ok()
                .and_then(|t| std::time::SystemTime::now().duration_since(t).ok())
                .is_some_and(|age| age >= grace);
            if terminal || expired {
                sinomo_core::storage::scratch::remove_owned_scratch_path(&path).map_err(|e| {
                    RolloutError::Io {
                        path: path.display().to_string(),
                        detail: e.to_string(),
                    }
                })?;
            }
        }
    }
    Ok(())
}

pub fn rollout_upload_manifest_path(
    rollout_paths: &SelfRolloutPaths,
    upload_id: &str,
) -> Result<PathBuf, RolloutError> {
    Ok(rollout_upload_dir(rollout_paths, upload_id)?.join("manifest.sobj"))
}

fn rollout_upload_receipt_path(
    rollout_paths: &SelfRolloutPaths,
    upload_id: &str,
) -> Result<PathBuf, RolloutError> {
    Ok(rollout_upload_dir(rollout_paths, upload_id)?.join("result.sobj"))
}

fn ensure_rollout_upload_manifest(
    rollout_paths: &SelfRolloutPaths,
    manifest: &ClusterRolloutBinaryManifest,
) -> Result<(), RolloutError> {
    let path = rollout_upload_manifest_path(rollout_paths, &manifest.upload_id)?;
    if path.exists() {
        let existing: ClusterRolloutBinaryManifest = read_object(&path)?;
        if existing != *manifest {
            return Err(RolloutError::InvalidArtifact {
                detail: format!(
                    "rollout upload id {} was already used with a different manifest",
                    manifest.upload_id
                ),
            });
        }
        return Ok(());
    }
    write_object(&path, manifest)?;
    sync_rollout_control_file(&path)
}

fn read_rollout_binary_receipt(
    rollout_paths: &SelfRolloutPaths,
    manifest: &ClusterRolloutBinaryManifest,
) -> Result<Option<ClusterRolloutBinaryReceipt>, RolloutError> {
    let path = rollout_upload_receipt_path(rollout_paths, &manifest.upload_id)?;
    if !path.exists() {
        return Ok(None);
    }
    let receipt: ClusterRolloutBinaryReceipt = read_object(&path)?;
    if receipt.manifest != *manifest {
        return Err(RolloutError::InvalidArtifact {
            detail: format!(
                "rollout upload id {} has a result for a different manifest",
                manifest.upload_id
            ),
        });
    }
    Ok(Some(receipt))
}

fn write_rollout_binary_receipt(
    rollout_paths: &SelfRolloutPaths,
    manifest: &ClusterRolloutBinaryManifest,
    response: &ClusterRolloutBinaryResponse,
) -> Result<(), RolloutError> {
    let path = rollout_upload_receipt_path(rollout_paths, &manifest.upload_id)?;
    write_object(
        &path,
        &ClusterRolloutBinaryReceipt {
            manifest: manifest.clone(),
            response: response.clone(),
        },
    )?;
    sync_rollout_control_file(&path)
}

fn sync_rollout_control_file(path: &Path) -> Result<(), RolloutError> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .and_then(|file| file.sync_all())
        .map_err(|source| RolloutError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })
}

async fn receive_rollout_binary_artifact<R>(
    rollout_paths: &SelfRolloutPaths,
    manifest: &ClusterRolloutBinaryManifest,
    artifact: &ClusterRolloutUploadArtifactSpec,
    body: &mut R,
    persist: bool,
) -> Result<(), RolloutError>
where
    R: AsyncRead + Unpin,
{
    let path =
        rollout_upload_artifact_path(rollout_paths, &manifest.upload_id, &artifact.artifact_id)?;
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|source| RolloutError::Io {
                path: parent.display().to_string(),
                detail: source.to_string(),
            })?;
    }
    let result = async {
        // Declared before the file so cancellation closes the writer first.
        let mut cleanup =
            persist.then(|| sinomo_core::storage::scratch::ScratchPath::new(path.clone()));
        let mut file = if persist {
            Some(
                tokio::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&path)
                    .await
                    .map_err(|source| RolloutError::Io {
                        path: path.display().to_string(),
                        detail: source.to_string(),
                    })?,
            )
        } else {
            None
        };
        let mut remaining = artifact.total_bytes;
        let mut hasher = blake3::Hasher::new();
        let mut buffer = vec![0_u8; ROLLOUT_BODY_BUFFER_BYTES];
        while remaining != 0 {
            let wanted = usize::try_from(remaining.min(buffer.len() as u64))
                .expect("bounded rollout read size fits usize");
            let read =
                body.read(&mut buffer[..wanted])
                    .await
                    .map_err(|source| RolloutError::Io {
                        path: format!("request body for {}", artifact.artifact_id),
                        detail: source.to_string(),
                    })?;
            if read == 0 {
                return Err(RolloutError::InvalidArtifact {
                    detail: format!(
                        "rollout artifact {} ended {} bytes early",
                        artifact.artifact_id, remaining
                    ),
                });
            }
            if let Some(file) = file.as_mut() {
                file.write_all(&buffer[..read])
                    .await
                    .map_err(|source| RolloutError::Io {
                        path: path.display().to_string(),
                        detail: source.to_string(),
                    })?;
            }
            hasher.update(&buffer[..read]);
            remaining -= u64::try_from(read).expect("read length fits u64");
        }
        let mut extra = [0_u8; 1];
        if body
            .read(&mut extra)
            .await
            .map_err(|source| RolloutError::Io {
                path: format!("request body for {}", artifact.artifact_id),
                detail: source.to_string(),
            })?
            != 0
        {
            return Err(RolloutError::InvalidArtifact {
                detail: format!(
                    "rollout artifact {} exceeded its declared {} bytes",
                    artifact.artifact_id, artifact.total_bytes
                ),
            });
        }
        if let Some(file) = file.as_mut() {
            file.flush().await.map_err(|source| RolloutError::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?;
            file.sync_all().await.map_err(|source| RolloutError::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?;
        }
        if hasher.finalize().as_bytes().as_slice() != artifact.blake3.as_slice() {
            return Err(RolloutError::InvalidArtifact {
                detail: format!("rollout artifact {} hash mismatch", artifact.artifact_id),
            });
        }
        if let Some(cleanup) = cleanup.as_mut() {
            cleanup.disarm();
        }
        Ok(())
    }
    .await;
    if persist && result.is_err() {
        let _ = tokio::fs::remove_file(&path).await;
    }
    result
}

fn remove_rollout_uploaded_artifacts(
    rollout_paths: &SelfRolloutPaths,
    manifest: &ClusterRolloutBinaryManifest,
) {
    for artifact in manifest.artifacts() {
        if let Ok(path) =
            rollout_upload_artifact_path(rollout_paths, &manifest.upload_id, &artifact.artifact_id)
        {
            let _ = fs::remove_file(path);
        }
    }
}

fn rollout_upload_dir(
    rollout_paths: &SelfRolloutPaths,
    upload_id: &str,
) -> Result<PathBuf, RolloutError> {
    validate_rollout_upload_component("upload id", upload_id)?;
    Ok(rollout_paths
        .root
        .join("staged")
        .join("uploads")
        .join(upload_id))
}

pub fn rollout_upload_artifact_path(
    rollout_paths: &SelfRolloutPaths,
    upload_id: &str,
    artifact_id: &str,
) -> Result<PathBuf, RolloutError> {
    validate_rollout_upload_component("artifact id", artifact_id)?;
    Ok(rollout_upload_dir(rollout_paths, upload_id)?.join(format!("{artifact_id}.part")))
}

pub fn validate_rollout_upload_component(label: &str, value: &str) -> Result<(), RolloutError> {
    if value.is_empty()
        || value == "."
        || value == ".."
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(RolloutError::InvalidArtifact {
            detail: format!("invalid rollout upload {label}: `{value}`"),
        });
    }
    Ok(())
}

pub fn verify_rollout_uploaded_module(
    rollout_paths: &SelfRolloutPaths,
    upload_id: &str,
    module: &ClusterRolloutNativeDylibModuleUploadSpec,
) -> Result<LocalRolloutNativeDylibModuleArtifact, RolloutError> {
    let built_library =
        verify_rollout_uploaded_artifact(rollout_paths, upload_id, &module.artifact)?;
    Ok(LocalRolloutNativeDylibModuleArtifact {
        module_id: module.module_id.clone(),
        interface_version: module.interface_version.clone(),
        cache_key: module.cache_key.clone(),
        library_name: module.library_name.clone(),
        built_library,
        source: module.source.clone(),
    })
}

pub fn verify_rollout_uploaded_artifact(
    rollout_paths: &SelfRolloutPaths,
    upload_id: &str,
    spec: &ClusterRolloutUploadArtifactSpec,
) -> Result<PathBuf, RolloutError> {
    validate_rollout_upload_component("artifact id", &spec.artifact_id)?;
    let path = rollout_upload_artifact_path(rollout_paths, upload_id, &spec.artifact_id)?;
    let metadata = path.metadata().map_err(|source| RolloutError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    if metadata.len() != spec.total_bytes {
        return Err(RolloutError::InvalidArtifact {
            detail: format!(
                "rollout upload artifact {} is {} bytes, expected {}",
                spec.artifact_id,
                metadata.len(),
                spec.total_bytes
            ),
        });
    }
    if rollout_file_blake3(&path)? != spec.blake3 {
        return Err(RolloutError::InvalidArtifact {
            detail: format!("rollout upload artifact {} hash mismatch", spec.artifact_id),
        });
    }
    Ok(path)
}

pub(crate) fn rollout_file_blake3(path: &Path) -> Result<Vec<u8>, RolloutError> {
    let mut file = fs::File::open(path).map_err(|source| RolloutError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(|source| RolloutError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().as_bytes().to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest(bytes: &[u8]) -> ClusterRolloutBinaryManifest {
        ClusterRolloutBinaryManifest {
            upload_id: "upload-test".to_string(),
            source: "test".to_string(),
            target_os: std::env::consts::OS.to_string(),
            target_arch: std::env::consts::ARCH.to_string(),
            host: "127.0.0.1".to_string(),
            host_override: None,
            port: 21_353,
            shadow_port: 21_354,
            requested_at: "now".to_string(),
            binary: ClusterRolloutUploadArtifactSpec {
                artifact_id: "binary".to_string(),
                total_bytes: bytes.len() as u64,
                blake3: blake3::hash(bytes).as_bytes().to_vec(),
            },
            native_dylib_modules: Vec::new(),
        }
    }

    fn request(bytes: &[u8]) -> ClusterRolloutBinaryRequest {
        ClusterRolloutBinaryRequest {
            manifest: manifest(bytes),
            artifact_id: "binary".to_string(),
            activate: false,
        }
    }

    #[tokio::test]
    async fn cancelled_upload_removes_partial_without_losing_replay_fences() {
        let root = sinomo_testdir::TestDir::new("rollout-upload-cancel");
        let bytes = b"candidate";
        let upload = request(bytes);
        let paths = SelfRolloutPaths::new(&root);
        let partial =
            rollout_upload_artifact_path(&paths, &upload.manifest.upload_id, "binary").unwrap();
        let (mut writer, mut reader) = tokio::io::duplex(64);
        writer.write_all(b"cand").await.unwrap();
        let data = root.to_path_buf();
        let task = tokio::spawn(async move {
            receive_rollout_binary_ingress(&data, upload, &mut reader).await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !partial.metadata().is_ok_and(|m| m.len() == 4) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        task.abort();
        let _ = task.await;
        assert!(!partial.exists());
        assert!(
            rollout_upload_manifest_path(&paths, "upload-test")
                .unwrap()
                .exists()
        );
        let replay = receive_rollout_binary_ingress(&root, request(bytes), &mut bytes.as_slice())
            .await
            .unwrap();
        assert!(matches!(replay, RolloutBinaryIngress::Complete(_)));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn replay_waits_for_activation_owner_but_not_a_surviving_descriptor_alias() {
        let root = sinomo_testdir::TestDir::new("rollout-activation-descriptor-lifetime");
        let bytes = b"candidate";
        let mut upload = request(bytes);
        upload.activate = true;
        let first = receive_rollout_binary_ingress(&root, upload.clone(), &mut bytes.as_slice())
            .await
            .expect("actual prepared activation owner");
        let alias = match &first {
            RolloutBinaryIngress::Activate(prepared) => {
                prepared._upload_lock.0.try_clone().unwrap()
            }
            RolloutBinaryIngress::Complete(_) => panic!("first upload must own activation"),
        };
        let busy = receive_rollout_binary_ingress(&root, upload.clone(), &mut bytes.as_slice())
            .await
            .expect_err("live activation owner must exclude another receiver");
        assert!(matches!(busy, RolloutError::Io { detail, .. }
            if detail.contains("already being received or activated")));
        drop(first);
        let paths = SelfRolloutPaths::new(&root);
        let partial = rollout_upload_artifact_path(&paths, &upload.manifest.upload_id, "binary")
            .expect("original payload path");
        fs::remove_file(&partial).expect("simulate payload moved by interrupted activation");
        let replay = receive_rollout_binary_ingress(&root, upload.clone(), &mut bytes.as_slice())
            .await
            .expect("ended activation owner releases its lock despite a descriptor alias");
        let RolloutBinaryIngress::Complete(response) = replay else {
            panic!("uncertain activation must not activate again");
        };
        assert_eq!(response.status, "in-progress");
        assert!(
            !partial.exists(),
            "same-identity replay must not recreate payloads"
        );
        let status = read_rollout_upload_status(&paths, &upload.manifest).unwrap();
        assert!(status.activation_started);
        assert!(
            status.receipt.is_none(),
            "uncertainty is not a fabricated terminal receipt"
        );
        assert!(
            alias.metadata().is_ok(),
            "descriptor alias really remains open"
        );
    }

    #[tokio::test]
    async fn replay_of_interrupted_activation_does_not_recreate_payloads() {
        let root = sinomo_testdir::TestDir::new("rollout-claim-replay-no-garbage");
        let bytes = b"candidate";
        let mut upload = request(bytes);
        upload.activate = true;
        let first = receive_rollout_binary_ingress(&root, upload.clone(), &mut bytes.as_slice())
            .await
            .unwrap();
        drop(first);
        let paths = SelfRolloutPaths::new(&root);
        let partial =
            rollout_upload_artifact_path(&paths, &upload.manifest.upload_id, "binary").unwrap();
        fs::remove_file(&partial).unwrap();
        for _ in 0..8 {
            let replay =
                receive_rollout_binary_ingress(&root, upload.clone(), &mut bytes.as_slice())
                    .await
                    .unwrap();
            assert!(matches!(replay, RolloutBinaryIngress::Complete(_)));
            assert!(!partial.exists());
        }
        assert!(
            read_rollout_upload_status(&paths, &upload.manifest)
                .unwrap()
                .activation_started
        );
    }

    #[tokio::test]
    async fn reclaim_transfers_keeps_active_receivers_and_unresolved_claims() {
        let root = sinomo_testdir::TestDir::new("rollout-payload-retention");
        let bytes = b"candidate";
        let paths = SelfRolloutPaths::new(&root);
        let mut upload = request(bytes);
        upload.activate = true;
        let RolloutBinaryIngress::Activate(prepared) =
            receive_rollout_binary_ingress(&root, upload.clone(), &mut bytes.as_slice())
                .await
                .unwrap()
        else {
            panic!("activation")
        };
        let partial =
            rollout_upload_artifact_path(&paths, &upload.manifest.upload_id, "binary").unwrap();
        reclaim_upload_payloads(&paths, std::time::Duration::ZERO).unwrap();
        assert!(partial.exists()); // live per-upload lease
        drop(prepared);
        reclaim_upload_payloads(&paths, std::time::Duration::ZERO).unwrap();
        assert!(partial.exists()); // unresolved durable activation claim
        let response = ClusterRolloutBinaryResponse {
            status: "accepted".into(),
            detail: "recovered".into(),
            main_binary: PathBuf::new(),
            shadow_binary: PathBuf::new(),
            source_head: None,
        };
        write_rollout_binary_receipt(&paths, &upload.manifest, &response).unwrap();
        reclaim_upload_payloads(&paths, std::time::Duration::ZERO).unwrap();
        assert!(!partial.exists());
        assert!(
            read_rollout_upload_status(&paths, &upload.manifest)
                .unwrap()
                .receipt
                .is_some()
        );
        let mut abandoned = request(bytes);
        abandoned.manifest.upload_id = "abandoned".into();
        receive_rollout_binary_ingress(&root, abandoned.clone(), &mut bytes.as_slice())
            .await
            .unwrap();
        reclaim_upload_payloads(&paths, std::time::Duration::ZERO).unwrap();
        assert!(
            !rollout_upload_artifact_path(&paths, "abandoned", "binary")
                .unwrap()
                .exists()
        );
    }

    #[tokio::test]
    async fn interrupted_activation_claim_prevents_reactivation_after_process_restart() {
        let root = sinomo_testdir::TestDir::new("rollout-interrupted-activation");
        let bytes = b"candidate";
        let mut upload = request(bytes);
        upload.activate = true;
        let first = receive_rollout_binary_ingress(&root, upload.clone(), &mut bytes.as_slice())
            .await
            .expect("first upload");
        assert!(matches!(&first, RolloutBinaryIngress::Activate(_)));
        // Drop the process-owned lock without a receipt, as if activation lost
        // its process. The durable claim still forbids another activation.
        drop(first);
        let replay = receive_rollout_binary_ingress(&root, upload.clone(), &mut bytes.as_slice())
            .await
            .expect("replay");
        let RolloutBinaryIngress::Complete(response) = replay else {
            panic!("must not reactivate")
        };
        assert_eq!(response.status, "in-progress");
        let status = read_rollout_upload_status(&SelfRolloutPaths::new(&root), &upload.manifest)
            .expect("status");
        assert!(status.activation_started);
        assert!(status.receipt.is_none());
        upload.manifest.requested_at = "another request".into();
        assert!(
            read_rollout_upload_status(&SelfRolloutPaths::new(&root), &upload.manifest).is_err()
        );
    }

    #[tokio::test]
    async fn completed_upload_replays_receipt_without_activation() {
        let root = sinomo_testdir::TestDir::new("rollout-completed-replay");
        let bytes = b"candidate";
        let mut upload = request(bytes);
        upload.activate = true;
        let RolloutBinaryIngress::Activate(first) =
            receive_rollout_binary_ingress(&root, upload.clone(), &mut bytes.as_slice())
                .await
                .expect("upload")
        else {
            panic!("activation")
        };
        let expected = ClusterRolloutBinaryResponse {
            status: "accepted".into(),
            detail: "staged".into(),
            main_binary: PathBuf::new(),
            shadow_binary: PathBuf::new(),
            source_head: None,
        };
        complete_rollout_binary_activation(&first, &expected).expect("receipt");
        drop(first);
        let RolloutBinaryIngress::Complete(replayed) =
            receive_rollout_binary_ingress(&root, upload, &mut bytes.as_slice())
                .await
                .expect("replay")
        else {
            panic!("must not activate")
        };
        assert_eq!(replayed, expected);
    }

    #[test]
    fn plan_retirement_requires_terminal_receipt_and_explicit_no_pending_proof() {
        let receipt = ClusterRolloutBinaryResponse {
            status: "failed".into(),
            detail: "candidate rejected".into(),
            main_binary: PathBuf::new(),
            shadow_binary: PathBuf::new(),
            source_head: None,
        };
        let mut status = RolloutUploadStatus {
            activation_started: true,
            receipt: Some(receipt),
            pending_activation: None,
        };
        assert!(
            !status.can_retire(),
            "older replies cannot prove absence of pending replacements"
        );
        status.pending_activation = Some(true);
        assert!(!status.can_retire());
        status.pending_activation = Some(false);
        assert!(status.can_retire());
        status.receipt = None;
        assert!(
            !status.can_retire(),
            "an unknown operation may still activate"
        );
    }

    #[tokio::test]
    async fn body_streams_to_a_durable_verified_artifact() {
        let root = sinomo_testdir::TestDir::new("sinomo-rollout-binary-body-stream");
        let bytes = b"hello world";
        let mut body = bytes.as_slice();

        let ingress = receive_rollout_binary_ingress(&root, request(bytes), &mut body)
            .await
            .expect("streamed upload");
        let RolloutBinaryIngress::Complete(response) = ingress else {
            panic!("staging-only ingress must be complete");
        };
        assert_eq!(response.status, "staged");
        let rollout_paths = SelfRolloutPaths::new(&root);
        let uploaded = rollout_upload_artifact_path(&rollout_paths, "upload-test", "binary")
            .expect("upload path");
        assert_eq!(fs::read(uploaded).expect("uploaded bytes"), bytes);
        assert!(
            rollout_upload_manifest_path(&rollout_paths, "upload-test")
                .expect("manifest path")
                .exists()
        );
    }

    #[tokio::test]
    async fn truncated_body_removes_partial_file() {
        let root = sinomo_testdir::TestDir::new("sinomo-rollout-binary-body-truncated");
        let expected = b"hello world";
        let mut body = b"hello".as_slice();

        let error = receive_rollout_binary_ingress(&root, request(expected), &mut body)
            .await
            .expect_err("truncated upload");
        assert!(error.to_string().contains("ended 6 bytes early"));
        let rollout_paths = SelfRolloutPaths::new(&root);
        let uploaded = rollout_upload_artifact_path(&rollout_paths, "upload-test", "binary")
            .expect("upload path");
        assert!(!uploaded.exists());
    }

    #[tokio::test]
    async fn body_hash_mismatch_is_rejected() {
        let root = sinomo_testdir::TestDir::new("sinomo-rollout-binary-body-hash-mismatch");
        let expected = b"hello world";
        let mut body = b"HELLO WORLD".as_slice();

        let error = receive_rollout_binary_ingress(&root, request(expected), &mut body)
            .await
            .expect_err("hash mismatch");
        assert!(error.to_string().contains("hash mismatch"));
    }
}
