//! Node-local DFS state access.
//!
//! This module owns filesystem paths under [`SinomoDataDir`], local expose/pin
//! store wrappers, metadata backend openers, and operator inspection entry
//! points. The networked service loop remains in the root module for now.

use std::path::{Path, PathBuf};

use sinomo_local::{LocalDataError, SinomoDataDir};
use sinomo_network::ClusterPaths;
use sinomo_node::NodeId;

use crate::{
    DaemonDfsInspectionRequest, DaemonDfsInspectionResponse, DfsConflictRecord, DfsError,
    DfsExposeAddRequest, DfsExposeConfig, DfsExposeStore, DfsExposeStoreError, DfsFileSystemId,
    DfsMetadataBackend, DfsMetadataShardConfig, DfsMetadataShardId, DfsObjectId,
    DfsOperatorFileSystemView, DfsOperatorInspector, DfsOperatorObjectView, DfsOperatorRepairView,
    DfsOperatorStatus, DfsPinAddRequest, DfsPinMode, DfsPinRecord, DfsPinStoreError,
    LocalDfsPinStore, LocalShardedMetadataBackend, LocalSobjMetadataBackend, MetadataBackendError,
    default_dfs_repair_attempt_snapshot_path, fallback_dfs_node_id, metadata_shards_prefix,
    read_dfs_repair_attempt_snapshot, resolve_dfs_conflicts, sifs_file_system_id,
    use_dfs_conflict_version,
};

/// Metadata backend root for the legacy single-shard local store.
pub fn default_dfs_metadata_root(data_dir: &SinomoDataDir) -> PathBuf {
    data_dir.dfs_dir().join("metadata").join("root")
}

/// Metadata backend root for sharded local metadata.
pub fn default_dfs_metadata_shards_root(data_dir: &SinomoDataDir) -> PathBuf {
    data_dir.dfs_dir().join("metadata")
}

pub(crate) fn dfs_path_safe_segment(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

pub fn local_dfs_metadata_shard_path(
    data_dir: &SinomoDataDir,
    shard_id: &DfsMetadataShardId,
) -> PathBuf {
    default_dfs_metadata_shards_root(data_dir).join(dfs_path_safe_segment(&shard_id.0))
}

pub fn open_local_metadata_shard_backend(
    data_dir: &SinomoDataDir,
    shard_id: DfsMetadataShardId,
) -> Result<LocalSobjMetadataBackend, DfsError> {
    let path = local_dfs_metadata_shard_path(data_dir, &shard_id);
    Ok(LocalSobjMetadataBackend::new(shard_id, path)?)
}

pub const DFS_IMPORTS_NAMESPACE_ROOT: &str = "/sinomo/imports";

pub fn dfs_import_namespace_root(source_cluster_id: impl std::fmt::Display) -> String {
    format!("{DFS_IMPORTS_NAMESPACE_ROOT}/{source_cluster_id}")
}

pub fn dfs_import_root(
    data_dir: &SinomoDataDir,
    source_cluster_id: impl std::fmt::Display,
) -> PathBuf {
    data_dir
        .dfs_dir()
        .join("imports")
        .join(source_cluster_id.to_string())
}

pub fn dfs_import_metadata_root(
    data_dir: &SinomoDataDir,
    source_cluster_id: impl std::fmt::Display,
) -> PathBuf {
    dfs_import_root(data_dir, source_cluster_id).join("metadata")
}

pub fn resolve_dfs_metadata_root(explicit: Option<PathBuf>) -> Result<PathBuf, LocalDataError> {
    if let Some(path) = explicit {
        return Ok(path);
    }
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    Ok(default_dfs_metadata_root(&data_dir))
}

pub fn default_dfs_pin_root(data_dir: &SinomoDataDir) -> PathBuf {
    data_dir.dfs_dir().join("pins")
}

pub fn resolve_dfs_pin_root(explicit: Option<PathBuf>) -> Result<PathBuf, LocalDataError> {
    if let Some(path) = explicit {
        return Ok(path);
    }
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    Ok(default_dfs_pin_root(&data_dir))
}

pub fn default_dfs_expose_config_path(data_dir: &SinomoDataDir) -> PathBuf {
    data_dir.dfs_expose_config_file()
}

pub fn dfs_expose_config_path(explicit: Option<PathBuf>, data_dir: &SinomoDataDir) -> PathBuf {
    explicit.unwrap_or_else(|| default_dfs_expose_config_path(data_dir))
}

pub fn ensure_default_dfs_expose_config(
    config_path: impl AsRef<Path>,
    data_dir: impl AsRef<Path>,
    node_id: NodeId,
) -> Result<DfsExposeConfig, DfsExposeStoreError> {
    DfsExposeStore::new(config_path.as_ref().to_path_buf())
        .ensure_default(data_dir.as_ref(), node_id)
}

pub fn ensure_default_cluster_aware_dfs_expose_config(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
) -> Result<DfsExposeConfig, DfsExposeStoreError> {
    let node_id = if cluster_paths.state.exists() {
        sinomo_network::read_cluster_state_without_kv(cluster_paths)
            .map(|state| state.local_node_id)
            .unwrap_or_else(|_| fallback_dfs_node_id())
    } else {
        fallback_dfs_node_id()
    };
    ensure_default_dfs_expose_config(data_dir.dfs_expose_config_file(), data_dir.root(), node_id)
}

pub fn load_dfs_expose_config(
    config_path: impl AsRef<Path>,
) -> Result<DfsExposeConfig, DfsExposeStoreError> {
    DfsExposeStore::new(config_path.as_ref().to_path_buf()).load()
}

pub fn add_dfs_expose_entry(
    config_path: impl AsRef<Path>,
    request: DfsExposeAddRequest,
) -> Result<DfsExposeConfig, DfsExposeStoreError> {
    DfsExposeStore::new(config_path.as_ref().to_path_buf()).add_entry(request)
}

pub fn remove_dfs_expose_entry(
    config_path: impl AsRef<Path>,
    id: &str,
) -> Result<(DfsExposeConfig, bool), DfsExposeStoreError> {
    DfsExposeStore::new(config_path.as_ref().to_path_buf()).remove_entry(id)
}

pub fn list_local_dfs_pins(
    root: impl Into<PathBuf>,
    file_system_id: Option<&DfsFileSystemId>,
) -> Result<Vec<DfsPinRecord>, DfsPinStoreError> {
    LocalDfsPinStore::new(root).list_pins(file_system_id)
}

pub fn add_local_dfs_pin(
    root: impl Into<PathBuf>,
    request: DfsPinAddRequest,
) -> Result<DfsPinRecord, DfsPinStoreError> {
    LocalDfsPinStore::new(root).add_pin(request)
}

pub fn remove_local_dfs_pin(
    root: impl Into<PathBuf>,
    file_system_id: &DfsFileSystemId,
    path: &str,
    node_id: Option<&NodeId>,
    mode: Option<&DfsPinMode>,
) -> Result<bool, DfsPinStoreError> {
    LocalDfsPinStore::new(root).remove_pin(file_system_id, path, node_id, mode)
}

pub fn open_root_local_metadata_backend(
    root: impl Into<PathBuf>,
) -> Result<LocalSobjMetadataBackend, MetadataBackendError> {
    LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), root.into())
}

pub fn open_local_sharded_metadata_backend(
    root: impl Into<PathBuf>,
    shards: Vec<DfsMetadataShardConfig>,
) -> Result<LocalShardedMetadataBackend, MetadataBackendError> {
    LocalShardedMetadataBackend::new(root, shards)
}

pub fn inspect_dfs_status(
    root: impl Into<PathBuf>,
) -> Result<DfsOperatorStatus, MetadataBackendError> {
    let backend = open_root_local_metadata_backend(root)?;
    DfsOperatorInspector::new(&backend).status()
}

pub fn inspect_dfs_file_system(
    root: impl Into<PathBuf>,
    file_system_id: &DfsFileSystemId,
) -> Result<Option<DfsOperatorFileSystemView>, MetadataBackendError> {
    let backend = open_root_local_metadata_backend(root)?;
    DfsOperatorInspector::new(&backend).file_system(file_system_id)
}

pub fn inspect_dfs_object(
    root: impl Into<PathBuf>,
    object_id: &DfsObjectId,
) -> Result<Option<DfsOperatorObjectView>, MetadataBackendError> {
    let backend = open_root_local_metadata_backend(root)?;
    DfsOperatorInspector::new(&backend).object(object_id)
}

pub fn inspect_dfs_path(
    root: impl Into<PathBuf>,
    file_system_id: &DfsFileSystemId,
    path: &str,
) -> Result<Option<DfsOperatorObjectView>, MetadataBackendError> {
    let backend = open_root_local_metadata_backend(root)?;
    DfsOperatorInspector::new(&backend).object_by_path(file_system_id, path)
}

pub fn inspect_dfs_conflicts(
    root: impl Into<PathBuf>,
    file_system_id: Option<&DfsFileSystemId>,
    object_id: Option<&DfsObjectId>,
) -> Result<Vec<DfsConflictRecord>, MetadataBackendError> {
    let backend = open_root_local_metadata_backend(root)?;
    DfsOperatorInspector::new(&backend).conflicts(file_system_id, object_id)
}

pub fn inspect_dfs_repairs(
    root: impl Into<PathBuf>,
    file_system_id: Option<&DfsFileSystemId>,
    object_id: Option<&DfsObjectId>,
) -> Result<Vec<DfsOperatorRepairView>, MetadataBackendError> {
    let backend = open_root_local_metadata_backend(root)?;
    DfsOperatorInspector::new(&backend).repairs(file_system_id, object_id)
}

pub fn open_cluster_aware_dfs_metadata_backend(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
) -> Result<Box<dyn DfsMetadataBackend>, DfsError> {
    if cluster_paths.state.exists()
        && let Some(metadata) = open_cluster_sharded_dfs_metadata_backend(data_dir, cluster_paths)?
    {
        return Ok(Box::new(metadata));
    }
    open_root_local_metadata_backend(default_dfs_metadata_root(data_dir))
        .map(|backend| Box::new(backend) as Box<dyn DfsMetadataBackend>)
        .map_err(Into::into)
}

pub fn open_cluster_sharded_dfs_metadata_backend(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
) -> Result<Option<LocalShardedMetadataBackend>, DfsError> {
    let file_system_id = sifs_file_system_id();
    let shards = cluster_dfs_metadata_shards(cluster_paths, &file_system_id)?;
    if shards.is_empty() {
        return Ok(None);
    }
    open_local_sharded_metadata_backend(default_dfs_metadata_shards_root(data_dir), shards)
        .map(Some)
        .map_err(Into::into)
}

pub fn cluster_dfs_metadata_shards(
    cluster_paths: &ClusterPaths,
    file_system_id: &DfsFileSystemId,
) -> Result<Vec<DfsMetadataShardConfig>, DfsError> {
    let mut shards = sinomo_kv::read_cluster_kv_prefix_entries(
        &cluster_paths.kv_paths(),
        &format!("{}/", metadata_shards_prefix(file_system_id)),
    )
    .map_err(DfsError::from)?
    .into_iter()
    .map(|entry| entry.decode::<DfsMetadataShardConfig>())
    .collect::<Result<Vec<_>, _>>()?;
    shards.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
    Ok(shards)
}

pub fn inspect_cluster_aware_dfs_status(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
) -> Result<DfsOperatorStatus, DfsError> {
    let metadata = open_cluster_aware_dfs_metadata_backend(data_dir, cluster_paths)?;
    let mut status = DfsOperatorInspector::new(metadata.as_ref()).status()?;
    attach_cluster_aware_dfs_maintenance_observations(&mut status, data_dir)?;
    if let Ok(state) = sinomo_network::read_cluster_state_without_kv(cluster_paths) {
        for shard in &mut status.metadata_shards {
            if shard.primary_node_id.as_ref() == Some(&state.local_node_id) {
                shard.replay_cursors.clear();
            } else {
                shard.replica_lag.clear();
            }
        }
    }
    Ok(status)
}

fn attach_cluster_aware_dfs_maintenance_observations(
    status: &mut DfsOperatorStatus,
    data_dir: &SinomoDataDir,
) -> Result<(), DfsError> {
    status.repair_attempts =
        read_dfs_repair_attempt_snapshot(&default_dfs_repair_attempt_snapshot_path(data_dir))?
            .map(|snapshot| snapshot.records)
            .unwrap_or_default();
    Ok(())
}

pub fn inspect_cluster_aware_dfs(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    request: DaemonDfsInspectionRequest,
) -> Result<DaemonDfsInspectionResponse, DfsError> {
    let mut metadata = open_cluster_aware_dfs_metadata_backend(data_dir, cluster_paths)?;
    let inspector = DfsOperatorInspector::new(metadata.as_ref());
    Ok(match request {
        DaemonDfsInspectionRequest::Status => {
            let mut status = inspector.status()?;
            attach_cluster_aware_dfs_maintenance_observations(&mut status, data_dir)?;
            DaemonDfsInspectionResponse::Status { status }
        }
        DaemonDfsInspectionRequest::FileSystem { file_system_id } => {
            DaemonDfsInspectionResponse::FileSystem {
                file_system: inspector.file_system(&file_system_id)?,
            }
        }
        DaemonDfsInspectionRequest::Path {
            file_system_id,
            path,
        } => DaemonDfsInspectionResponse::Path {
            object: inspector.object_by_path(&file_system_id, &path)?,
        },
        DaemonDfsInspectionRequest::Object { object_id } => DaemonDfsInspectionResponse::Object {
            object: inspector.object(&object_id)?,
        },
        DaemonDfsInspectionRequest::Conflicts {
            file_system_id,
            object_id,
            limit,
            cursor,
        } => {
            let page = inspector.conflicts_page(
                file_system_id.as_ref(),
                object_id.as_ref(),
                limit,
                cursor.as_ref(),
            )?;
            DaemonDfsInspectionResponse::Conflicts {
                conflicts: page.conflicts,
                next_cursor: page.next_cursor,
            }
        }
        DaemonDfsInspectionRequest::ResolveConflicts {
            file_system_id,
            object_id,
            conflict_ids,
        } => DaemonDfsInspectionResponse::ResolveConflicts {
            report: resolve_dfs_conflicts(
                metadata.as_mut(),
                file_system_id.as_ref(),
                object_id.as_ref(),
                &conflict_ids,
            )?,
        },
        DaemonDfsInspectionRequest::ConflictVersions {
            object_id,
            conflict_ids,
        } => DaemonDfsInspectionResponse::ConflictVersions {
            report: inspector.conflict_versions(&object_id, &conflict_ids)?,
        },
        DaemonDfsInspectionRequest::UseConflictVersion {
            object_id,
            version,
            conflict_ids,
        } => DaemonDfsInspectionResponse::UseConflictVersion {
            report: use_dfs_conflict_version(
                metadata.as_mut(),
                &object_id,
                &version,
                &conflict_ids,
            )?,
        },
        DaemonDfsInspectionRequest::Repair {
            file_system_id,
            object_id,
            limit,
            cursor,
        } => {
            let page = inspector.repairs_page(
                file_system_id.as_ref(),
                object_id.as_ref(),
                limit,
                cursor.as_ref(),
            )?;
            DaemonDfsInspectionResponse::Repair {
                repairs: page.repairs,
                next_cursor: page.next_cursor,
            }
        }
    })
}
