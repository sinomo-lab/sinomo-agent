//! Durable coordinator intent. The target still owns activation and authority;
//! this journal only preserves the exact request across CLI interruptions.
use crate::{
    ClusterRolloutBinaryManifest, RolloutError, SelfRolloutPaths, read_object, write_object,
};
use serde::{Deserialize, Serialize};
use sinomo_core::{ClusterEndpointRecord, Id, node::NodeId};
use std::{
    fs,
    path::{Path, PathBuf},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RolloutArtifactSource {
    pub artifact_id: String,
    pub path: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RolloutCoordinatorTarget {
    pub node_id: NodeId,
    pub address: String,
    pub endpoint: Option<ClusterEndpointRecord>,
    pub restart_endpoints: Vec<ClusterEndpointRecord>,
    pub was_voter: bool,
    pub manifest: ClusterRolloutBinaryManifest,
    pub artifacts: Vec<RolloutArtifactSource>,
    pub submitted: bool,
    pub verified: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RolloutCoordinatorPlan {
    pub schema_version: u32,
    #[serde(default)]
    pub cancelled: bool,
    pub id: String,
    pub cluster_id: Id,
    pub source_repo: PathBuf,
    pub source_revision: Option<String>,
    pub targets: Vec<RolloutCoordinatorTarget>,
}

impl RolloutCoordinatorPlan {
    pub fn complete(&self) -> bool {
        !self.targets.is_empty() && self.targets.iter().all(|target| target.verified)
    }

    pub fn validate(&self) -> Result<(), RolloutError> {
        crate::validate_rollout_upload_component("coordinator id", &self.id)?;
        if self.schema_version != 1 || self.targets.is_empty() {
            return Err(RolloutError::InvalidArtifact {
                detail: "unsupported or empty rollout coordinator plan".into(),
            });
        }
        let mut nodes = std::collections::BTreeSet::new();
        for target in &self.targets {
            crate::validate_rollout_binary_manifest(&target.manifest)?;
            if !nodes.insert(&target.node_id)
                || target
                    .endpoint
                    .as_ref()
                    .is_some_and(|endpoint| endpoint.node_id != target.node_id)
            {
                return Err(RolloutError::InvalidArtifact {
                    detail: "duplicate or mismatched rollout target identity".into(),
                });
            }
            let expected = std::iter::once(&target.manifest.binary)
                .chain(
                    target
                        .manifest
                        .native_dylib_modules
                        .iter()
                        .map(|module| &module.artifact),
                )
                .map(|artifact| artifact.artifact_id.as_str())
                .collect::<std::collections::BTreeSet<_>>();
            let actual = target
                .artifacts
                .iter()
                .map(|artifact| artifact.artifact_id.as_str())
                .collect::<std::collections::BTreeSet<_>>();
            if expected != actual || actual.len() != target.artifacts.len() {
                return Err(RolloutError::InvalidArtifact {
                    detail: "coordinator artifact sources do not match manifest".into(),
                });
            }
        }
        Ok(())
    }
}

pub fn rollout_coordinator_plan_path(
    paths: &SelfRolloutPaths,
    id: &str,
) -> Result<PathBuf, RolloutError> {
    crate::validate_rollout_upload_component("coordinator id", id)?;
    Ok(paths.root.join("coordinator").join(format!("{id}.sobj")))
}

/// Keep replay inputs independent of a mutable operator build directory.
/// Source-build caches may already be immutable, but prebuilt paths need the
/// same guarantee when a coordinator is resumed after another local build.
pub fn cache_rollout_coordinator_artifact(
    paths: &SelfRolloutPaths,
    source: &Path,
    spec: &crate::ClusterRolloutUploadArtifactSpec,
) -> Result<PathBuf, RolloutError> {
    let digest: [u8; 32] =
        spec.blake3
            .as_slice()
            .try_into()
            .map_err(|_| RolloutError::InvalidArtifact {
                detail: "invalid coordinator artifact digest".into(),
            })?;
    let directory = paths.root.join("coordinator/artifacts");
    fs::create_dir_all(&directory).map_err(|error| io_error(&directory, error))?;
    let destination = directory.join(blake3::Hash::from_bytes(digest).to_hex().as_str());
    let validate = |path: &Path| -> Result<(), RolloutError> {
        if fs::metadata(path)
            .map_err(|error| io_error(path, error))?
            .len()
            != spec.total_bytes
            || crate::binary_ingress::rollout_file_blake3(path)? != spec.blake3
        {
            return Err(RolloutError::InvalidArtifact {
                detail: "coordinator artifact changed after manifest preparation".into(),
            });
        }
        Ok(())
    };
    if !destination.exists() {
        let temporary = destination.with_extension(format!("{}.part", Id::generate()));
        let _cleanup = sinomo_core::storage::scratch::ScratchPath::new(temporary.clone());
        let mut input = fs::File::open(source).map_err(|error| io_error(source, error))?;
        let mut output = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .map_err(|error| io_error(&temporary, error))?;
        std::io::copy(&mut input, &mut output).map_err(|error| io_error(&temporary, error))?;
        output
            .sync_all()
            .map_err(|error| io_error(&temporary, error))?;
        drop(output);
        if let Err(error) = validate(&temporary) {
            let _ = fs::remove_file(&temporary);
            return Err(error);
        }
        fs::rename(&temporary, &destination).map_err(|error| io_error(&destination, error))?;
    }
    validate(&destination)?;
    fs::canonicalize(&destination).map_err(|error| io_error(&destination, error))
}

pub fn save_rollout_coordinator_plan(
    paths: &SelfRolloutPaths,
    plan: &RolloutCoordinatorPlan,
) -> Result<(), RolloutError> {
    plan.validate()?;
    write_object(&rollout_coordinator_plan_path(paths, &plan.id)?, plan)?;
    write_object(&paths.root.join("coordinator/latest.sobj"), &plan.id)?;
    if (plan.complete() || plan.cancelled)
        && let Err(error) = crate::prune_rollout_coordinator_artifacts(paths)
    {
        tracing::warn!(%error, "could not reclaim settled rollout artifacts");
    }
    Ok(())
}

pub fn load_rollout_coordinator_plan(
    paths: &SelfRolloutPaths,
    id: Option<&str>,
) -> Result<RolloutCoordinatorPlan, RolloutError> {
    let id = match id {
        Some(id) => id.to_string(),
        None => read_object(&paths.root.join("coordinator/latest.sobj"))?,
    };
    let plan: RolloutCoordinatorPlan = read_object(&rollout_coordinator_plan_path(paths, &id)?)?;
    plan.validate()?;
    if plan.id != id {
        return Err(RolloutError::InvalidArtifact {
            detail: "coordinator plan identity mismatch".into(),
        });
    }
    Ok(plan)
}

pub fn unfinished_rollout_coordinator_plan_ids(
    paths: &SelfRolloutPaths,
) -> Result<Vec<String>, RolloutError> {
    let directory = paths.root.join("coordinator");
    let mut ids = Vec::new();
    for entry in fs::read_dir(&directory).map_err(|error| io_error(&directory, error))? {
        let path = entry.map_err(|error| io_error(&directory, error))?.path();
        if path
            .extension()
            .is_some_and(|extension| extension == "sobj")
            && path.file_name().is_some_and(|name| name != "latest.sobj")
        {
            let id = path.file_stem().and_then(|id| id.to_str()).ok_or_else(|| {
                RolloutError::InvalidArtifact {
                    detail: "invalid coordinator plan filename".into(),
                }
            })?;
            let plan = load_rollout_coordinator_plan(paths, Some(id))?;
            if !plan.complete() && !plan.cancelled {
                ids.push(id.to_string());
            }
        }
    }
    ids.sort();
    Ok(ids)
}

/// Held by every activating coordinator, including resume. A second CLI may
/// inspect state but cannot race a live coordinator's target sequence.
pub fn lock_rollout_coordinator(paths: &SelfRolloutPaths) -> Result<fs::File, RolloutError> {
    let dir = paths.root.join("coordinator");
    fs::create_dir_all(&dir).map_err(|error| io_error(&dir, error))?;
    let path = dir.join("coordinator.lock");
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&path)
        .map_err(|error| io_error(&path, error))?;
    fs2::FileExt::try_lock_exclusive(&file).map_err(|error| RolloutError::Io {
        path: path.display().to_string(),
        detail: format!("another rollout coordinator is active: {error}"),
    })?;
    Ok(file)
}

fn io_error(path: &Path, error: std::io::Error) -> RolloutError {
    RolloutError::Io {
        path: path.display().to_string(),
        detail: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn coordinator_checkpoint_survives_interrupt_with_original_operation_identity() {
        let root = sinomo_testdir::TestDir::new("rollout-coordinator-checkpoint");
        let paths = SelfRolloutPaths::new(&root);
        let binary = root.join("candidate");
        fs::write(&binary, b"candidate").unwrap();
        let manifest = crate::prepare_cluster_rollout_binary_manifest_for_target(
            "source",
            &binary,
            &[],
            "linux",
            "x86_64",
            "0.0.0.0",
            24500,
            24501,
            "request-a",
        )
        .unwrap();
        let original_upload_id = manifest.upload_id.clone();
        let retained =
            cache_rollout_coordinator_artifact(&paths, &binary, &manifest.binary).unwrap();
        fs::write(&binary, b"rebuilt output").unwrap();
        assert_eq!(fs::read(&retained).unwrap(), b"candidate");
        let mut plan = RolloutCoordinatorPlan {
            schema_version: 1,
            cancelled: false,
            id: Id::generate().to_string(),
            cluster_id: Id::generate(),
            source_repo: root.to_path_buf(),
            source_revision: None,
            targets: vec![RolloutCoordinatorTarget {
                node_id: Id::generate(),
                address: "node:24500".into(),
                endpoint: None,
                restart_endpoints: vec![],
                was_voter: true,
                artifacts: vec![RolloutArtifactSource {
                    artifact_id: manifest.binary.artifact_id.clone(),
                    path: retained,
                }],
                manifest,
                submitted: true,
                verified: false,
            }],
        };
        let lock = lock_rollout_coordinator(&paths).unwrap();
        assert!(lock_rollout_coordinator(&paths).is_err());
        save_rollout_coordinator_plan(&paths, &plan).unwrap();
        drop(lock);
        let _resumed_lock = lock_rollout_coordinator(&paths).unwrap();
        let recovered = load_rollout_coordinator_plan(&paths, None).unwrap();
        assert!(recovered.targets[0].submitted);
        assert!(!recovered.complete());
        assert_eq!(recovered.targets[0].manifest.upload_id, original_upload_id);
        assert_eq!(recovered.targets[0].manifest.port, 24500);
        plan.targets[0].verified = true;
        save_rollout_coordinator_plan(&paths, &plan).unwrap();
        assert!(
            load_rollout_coordinator_plan(&paths, Some(&plan.id))
                .unwrap()
                .complete()
        );
        assert!(load_rollout_coordinator_plan(&paths, Some("../escape")).is_err());
        let mut later = plan.clone();
        later.id = Id::generate().to_string();
        later.targets[0].verified = false;
        save_rollout_coordinator_plan(&paths, &later).unwrap();
        plan.cancelled = true;
        save_rollout_coordinator_plan(&paths, &plan).unwrap();
        assert_eq!(
            unfinished_rollout_coordinator_plan_ids(&paths).unwrap(),
            vec![later.id],
            "retiring an older plan must not hide another unfinished plan behind the latest pointer"
        );
    }
}
