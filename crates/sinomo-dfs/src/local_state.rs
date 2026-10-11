//! Node-local DFS state access.
//!
//! This module owns filesystem paths under [`SinomoDataDir`], local pin
//! store wrappers, metadata backend openers, and operator inspection entry
//! points. The networked service loop remains in the root module for now.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::UNIX_EPOCH;

use sinomo_core::cluster::ClusterKvPathsExt;
use sinomo_core::local::{LocalDataError, SinomoDataDir};
use sinomo_core::network::ClusterPaths;
use sinomo_core::node::NodeId;

use crate::metadata_sharded_backend::{
    DfsResidentMetadataStatusSnapshot, metadata_shard_topology_status,
    routing_topology_history_path,
};
use crate::{
    DaemonDfsInspectionRequest, DaemonDfsInspectionResponse, DfsControlPlaneKvClient, DfsError,
    DfsFileSystemConfig, DfsFileSystemId, DfsMetadataBackend, DfsMetadataShardConfig,
    DfsMetadataShardHeadWatermark, DfsMetadataShardId, DfsMetadataShardMembershipHandoff,
    DfsMetadataShardWriteFreeze, DfsMetadataSummary, DfsObjectId, DfsOperatorFileSystemView,
    DfsOperatorInspector, DfsOperatorObjectView, DfsOperatorRepairView, DfsOperatorStatus,
    DfsOperatorStatusOmission, DfsPinAddRequest, DfsPinMode, DfsPinRecord, DfsPinStoreError,
    DfsRepairQueueBoundedStatus, LocalDfsPinStore, LocalDfsRepairQueueStore,
    LocalShardedMetadataBackend, LocalSobjMetadataBackend, MetadataBackendError,
    default_dfs_repair_attempt_snapshot_path, metadata_shards_prefix, sifs_file_system_id,
};

const DFS_STATUS_REPAIR_ATTEMPT_SAMPLE_LIMIT: usize = 32;
const DFS_METADATA_INSPECTION_CACHE_CAPACITY: usize = 4;

#[derive(Clone, Debug, Eq, PartialEq)]
enum DfsMetadataInspectionIdentity {
    ExactShard {
        metadata_root: PathBuf,
        shard_id: DfsMetadataShardId,
    },
    Root {
        metadata_root: PathBuf,
    },
    Sharded {
        metadata_root: PathBuf,
        local_node_id: NodeId,
        shards: Vec<DfsMetadataShardConfig>,
        routing_topology_generation: Option<DfsMetadataInspectionFileGeneration>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DfsMetadataInspectionFileGeneration {
    is_file: bool,
    len: u64,
    modified_unix_nanos: Option<u128>,
    created_unix_nanos: Option<u128>,
    platform_identity: Option<String>,
}

fn dfs_metadata_inspection_file_generation(
    path: &Path,
) -> Result<Option<DfsMetadataInspectionFileGeneration>, DfsError> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(metadata_inspection_cache_error(format!(
                "cannot inspect DFS metadata topology generation {}: {source}",
                path.display()
            )));
        }
    };
    let modified_unix_nanos = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos());
    let created_unix_nanos = metadata
        .created()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos());
    #[cfg(unix)]
    let platform_identity = {
        use std::os::unix::fs::MetadataExt as _;
        Some(format!(
            "unix:{}:{}:{}:{}",
            metadata.dev(),
            metadata.ino(),
            metadata.ctime(),
            metadata.ctime_nsec()
        ))
    };
    #[cfg(windows)]
    let platform_identity = crate::metadata_io::windows_path_identity(path);
    #[cfg(not(any(unix, windows)))]
    let platform_identity = None;
    Ok(Some(DfsMetadataInspectionFileGeneration {
        is_file: metadata.is_file(),
        len: metadata.len(),
        modified_unix_nanos,
        created_unix_nanos,
        platform_identity,
    }))
}

#[cfg(test)]
impl DfsMetadataInspectionIdentity {
    fn metadata_root(&self) -> &Path {
        match self {
            Self::Root { metadata_root }
            | Self::Sharded { metadata_root, .. }
            | Self::ExactShard { metadata_root, .. } => metadata_root,
        }
    }
}

#[derive(Debug)]
enum CachedDfsMetadataInspectionBackend {
    Root(Box<LocalSobjMetadataBackend>),
    Sharded(LocalShardedMetadataBackend),
}

impl CachedDfsMetadataInspectionBackend {
    fn open(identity: &DfsMetadataInspectionIdentity) -> Result<Self, DfsError> {
        match identity {
            DfsMetadataInspectionIdentity::ExactShard {
                metadata_root,
                shard_id,
            } => LocalSobjMetadataBackend::open_for_inspection(
                shard_id.clone(),
                metadata_root.clone(),
            )
            .map(Box::new)
            .map(Self::Root)
            .map_err(Into::into),
            DfsMetadataInspectionIdentity::Root { metadata_root } => {
                open_root_local_metadata_backend_for_inspection(metadata_root.clone())
                    .map(Box::new)
                    .map(Self::Root)
                    .map_err(Into::into)
            }
            DfsMetadataInspectionIdentity::Sharded {
                metadata_root,
                local_node_id,
                shards,
                ..
            } => open_local_sharded_metadata_backend_for_inspection_at_node(
                metadata_root.clone(),
                shards.clone(),
                local_node_id,
            )
            .map(Self::Sharded)
            .map_err(Into::into),
        }
    }

    fn begin_inspection_snapshot(&mut self) -> Result<(), DfsError> {
        match self {
            Self::Root(backend) => backend.begin_inspection_snapshot(),
            Self::Sharded(backend) => backend.begin_inspection_snapshot(),
        }
        .map_err(Into::into)
    }

    fn end_inspection_snapshot(&mut self) {
        match self {
            Self::Root(backend) => backend.end_inspection_snapshot(),
            Self::Sharded(backend) => backend.end_inspection_snapshot(),
        }
    }

    fn as_metadata_backend(&self) -> &dyn DfsMetadataBackend {
        match self {
            Self::Root(backend) => backend.as_ref(),
            Self::Sharded(backend) => backend,
        }
    }
}

#[derive(Debug)]
struct CachedDfsMetadataInspection {
    identity: DfsMetadataInspectionIdentity,
    backend: CachedDfsMetadataInspectionBackend,
}

type DfsMetadataInspectionCacheSlot = Arc<Mutex<Option<CachedDfsMetadataInspection>>>;

#[derive(Default)]
struct DfsMetadataInspectionCache {
    slots: BTreeMap<PathBuf, DfsMetadataInspectionCacheSlot>,
    recency: VecDeque<PathBuf>,
}

impl DfsMetadataInspectionCache {
    fn slot(&mut self, metadata_root: &Path) -> DfsMetadataInspectionCacheSlot {
        let key = metadata_root.to_path_buf();
        self.recency.retain(|existing| existing != &key);
        self.recency.push_back(key.clone());
        if let Some(slot) = self.slots.get(&key) {
            return Arc::clone(slot);
        }

        while self.slots.len() >= DFS_METADATA_INSPECTION_CACHE_CAPACITY {
            let Some(oldest) = self.recency.pop_front() else {
                break;
            };
            self.slots.remove(&oldest);
        }
        let slot = Arc::new(Mutex::new(None));
        self.slots.insert(key, Arc::clone(&slot));
        slot
    }

    fn evict(&mut self, metadata_root: &Path) {
        self.slots.remove(metadata_root);
        self.recency.retain(|existing| existing != metadata_root);
    }
}

fn dfs_metadata_inspection_cache() -> &'static Mutex<DfsMetadataInspectionCache> {
    static CACHE: OnceLock<Mutex<DfsMetadataInspectionCache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(DfsMetadataInspectionCache::default()))
}

fn metadata_inspection_cache_error(detail: impl Into<String>) -> DfsError {
    DfsError::Metadata {
        message: detail.into(),
    }
}

/// Default root shard metadata path.
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

/// Read only the bounded durable authority marker for one local metadata
/// shard without opening or recovering its metadata backend.
pub fn read_local_dfs_metadata_shard_write_freeze(
    data_dir: &SinomoDataDir,
    shard_id: &DfsMetadataShardId,
) -> Result<Option<DfsMetadataShardWriteFreeze>, DfsError> {
    let path = local_dfs_metadata_shard_path(data_dir, shard_id)
        .join("journal")
        .join("membership-handoff-write-freeze.sobj");
    crate::metadata_io::read_sobj_optional(&path).map_err(Into::into)
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

pub fn open_root_local_metadata_backend_for_inspection(
    root: impl Into<PathBuf>,
) -> Result<LocalSobjMetadataBackend, MetadataBackendError> {
    LocalSobjMetadataBackend::open_for_inspection(DfsMetadataShardId::new("root"), root.into())
}

pub fn open_local_sharded_metadata_backend(
    root: impl Into<PathBuf>,
    shards: Vec<DfsMetadataShardConfig>,
) -> Result<LocalShardedMetadataBackend, MetadataBackendError> {
    LocalShardedMetadataBackend::new(root, shards)
}

pub fn open_local_sharded_metadata_backend_for_replica_repair(
    root: impl Into<PathBuf>,
    shards: Vec<DfsMetadataShardConfig>,
    local_node_id: &NodeId,
) -> Result<LocalShardedMetadataBackend, MetadataBackendError> {
    LocalShardedMetadataBackend::new_with_replica_repair(root, shards, local_node_id)
}

pub fn open_local_sharded_metadata_backend_for_inspection_at_node(
    root: impl Into<PathBuf>,
    shards: Vec<DfsMetadataShardConfig>,
    local_node_id: &NodeId,
) -> Result<LocalShardedMetadataBackend, MetadataBackendError> {
    LocalShardedMetadataBackend::open_for_inspection_at_node(root, shards, local_node_id)
}

pub fn inspect_dfs_status(
    root: impl Into<PathBuf>,
) -> Result<DfsOperatorStatus, MetadataBackendError> {
    let backend = open_root_local_metadata_backend_for_inspection(root)?;
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

pub fn open_cluster_aware_dfs_metadata_backend_for_inspection(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
) -> Result<Box<dyn DfsMetadataBackend>, DfsError> {
    if cluster_paths.state.exists()
        && let Some(metadata) =
            open_cluster_sharded_dfs_metadata_backend_for_inspection(data_dir, cluster_paths)?
    {
        return Ok(Box::new(metadata));
    }
    open_root_local_metadata_backend_for_inspection(default_dfs_metadata_root(data_dir))
        .map(|backend| Box::new(backend) as Box<dyn DfsMetadataBackend>)
        .map_err(Into::into)
}

/// Open a read-only metadata image from topology which the caller already
/// obtained through its own authority boundary.
///
/// Application processes use this seam after asking the resident daemon for
/// a bounded authoritative shard projection. Keeping the projection explicit
/// prevents an R1 DFS helper from consulting the process-local resident-Raft
/// registry, which exists only in the daemon process.
pub fn open_cluster_aware_dfs_metadata_backend_for_inspection_with_topology(
    data_dir: &SinomoDataDir,
    local_node_id: &NodeId,
    topology: crate::DfsAuthoritativePrefixProjection,
) -> Result<Box<dyn DfsMetadataBackend>, DfsError> {
    let shards = sifs_metadata_shards_from_authoritative_topology(topology)?;
    open_cluster_aware_dfs_metadata_backend_for_inspection_with_shards(
        data_dir,
        local_node_id,
        shards,
    )
}

fn open_cluster_aware_dfs_metadata_backend_for_inspection_with_shards(
    data_dir: &SinomoDataDir,
    local_node_id: &NodeId,
    shards: Vec<DfsMetadataShardConfig>,
) -> Result<Box<dyn DfsMetadataBackend>, DfsError> {
    if shards.is_empty() {
        return open_root_local_metadata_backend_for_inspection(default_dfs_metadata_root(
            data_dir,
        ))
        .map(|backend| Box::new(backend) as Box<dyn DfsMetadataBackend>)
        .map_err(Into::into);
    }
    open_local_sharded_metadata_backend_for_inspection_at_node(
        default_dfs_metadata_shards_root(data_dir),
        shards,
        local_node_id,
    )
    .map(|backend| Box::new(backend) as Box<dyn DfsMetadataBackend>)
    .map_err(Into::into)
}

fn cluster_aware_dfs_metadata_inspection_identity_with_topology(
    data_dir: &SinomoDataDir,
    local_node_id: NodeId,
    shards: Vec<DfsMetadataShardConfig>,
) -> Result<DfsMetadataInspectionIdentity, DfsError> {
    if shards.is_empty() {
        return Ok(DfsMetadataInspectionIdentity::Root {
            metadata_root: default_dfs_metadata_root(data_dir),
        });
    }
    Ok(DfsMetadataInspectionIdentity::Sharded {
        metadata_root: default_dfs_metadata_shards_root(data_dir),
        local_node_id,
        shards,
        routing_topology_generation: dfs_metadata_inspection_file_generation(
            &routing_topology_history_path(&default_dfs_metadata_shards_root(data_dir)),
        )?,
    })
}

fn cluster_aware_dfs_metadata_inspection_identity(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
) -> Result<DfsMetadataInspectionIdentity, DfsError> {
    if cluster_paths.state.exists() {
        let shards = cluster_dfs_metadata_shards(cluster_paths, &sifs_file_system_id())?;
        if !shards.is_empty() {
            let state = sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths)
                .map_err(DfsError::from_network)?;
            return cluster_aware_dfs_metadata_inspection_identity_with_topology(
                data_dir,
                state.local_node_id,
                shards,
            );
        }
    }
    Ok(DfsMetadataInspectionIdentity::Root {
        metadata_root: default_dfs_metadata_root(data_dir),
    })
}

#[cfg(test)]
fn with_cached_dfs_metadata_inspection_identity<T>(
    identity: DfsMetadataInspectionIdentity,
    inspect: impl FnOnce(&dyn DfsMetadataBackend) -> Result<T, DfsError>,
) -> Result<T, DfsError> {
    let resolved_identity = identity.clone();
    with_cached_dfs_metadata_inspection_identity_refreshing(
        identity,
        move || Ok(resolved_identity),
        inspect,
    )
}

#[cfg(test)]
fn with_cached_dfs_metadata_inspection_identity_refreshing<T>(
    initial_identity: DfsMetadataInspectionIdentity,
    resolve_identity: impl FnOnce() -> Result<DfsMetadataInspectionIdentity, DfsError>,
    inspect: impl FnOnce(&dyn DfsMetadataBackend) -> Result<T, DfsError>,
) -> Result<T, DfsError> {
    with_cached_dfs_metadata_inspection(
        initial_identity.metadata_root().to_path_buf(),
        resolve_identity,
        inspect,
    )
}

fn with_cached_dfs_metadata_inspection<T>(
    cache_key: PathBuf,
    resolve_identity: impl FnOnce() -> Result<DfsMetadataInspectionIdentity, DfsError>,
    inspect: impl FnOnce(&dyn DfsMetadataBackend) -> Result<T, DfsError>,
) -> Result<T, DfsError> {
    let slot = {
        let mut cache = dfs_metadata_inspection_cache().lock().map_err(|_| {
            metadata_inspection_cache_error("DFS metadata inspection cache lock is poisoned")
        })?;
        cache.slot(&cache_key)
    };
    let mut slot = slot.lock().map_err(|_| {
        metadata_inspection_cache_error("DFS metadata inspection backend lock is poisoned")
    })?;
    // Resolve topology only after serializing this data root. A request that
    // waited behind a newer topology must not overwrite that newer cached
    // generation with an identity it captured before waiting.
    let identity = resolve_identity()?;

    if slot
        .as_ref()
        .is_none_or(|cached| cached.identity != identity)
    {
        // Drop the old decoded image before opening a replacement so topology
        // changes do not transiently retain two complete metadata generations.
        *slot = None;
        *slot = Some(CachedDfsMetadataInspection {
            backend: CachedDfsMetadataInspectionBackend::open(&identity)?,
            identity: identity.clone(),
        });
    }

    let begin_result = slot
        .as_mut()
        .expect("cached DFS metadata inspection was initialized")
        .backend
        .begin_inspection_snapshot();
    if let Err(refresh_error) = begin_result {
        // A replaced snapshot or other cross-process generation race can make
        // an incremental refresh fail. Discard the image and retry one cold,
        // lenient inspection open. A second failure is authoritative to the
        // caller; it must never be projected as an empty metadata store.
        *slot = None;
        let mut replacement = CachedDfsMetadataInspection {
            backend: CachedDfsMetadataInspectionBackend::open(&identity).map_err(|open_error| {
                metadata_inspection_cache_error(format!(
                    "cannot refresh cached DFS metadata inspection ({refresh_error}); cold inspection reopen failed: {open_error}"
                ))
            })?,
            identity,
        };
        replacement
            .backend
            .begin_inspection_snapshot()
            .map_err(|retry_error| {
                metadata_inspection_cache_error(format!(
                    "cannot refresh cached DFS metadata inspection ({refresh_error}); cold inspection refresh failed: {retry_error}"
                ))
            })?;
        *slot = Some(replacement);
    }

    let cached = slot
        .as_mut()
        .expect("cached DFS metadata inspection was initialized");
    let result = inspect(cached.backend.as_metadata_backend());
    cached.backend.end_inspection_snapshot();
    result
}

/// Run one coherent, read-only inspection against a retained cluster-aware
/// metadata image.
///
/// The image is refreshed incrementally from materialized table state and is
/// invalidated by any exact shard topology or local-node change. The closure
/// prevents callers from retaining or mutating the cached backend.
pub(crate) fn with_current_owned_dfs_shard_inspection<T>(
    data_dir: &SinomoDataDir,
    shard_id: &DfsMetadataShardId,
    inspect: impl FnOnce(&dyn DfsMetadataBackend) -> Result<T, DfsError>,
) -> Result<Option<T>, DfsError> {
    let root = default_dfs_metadata_shards_root(data_dir)
        .join(crate::metadata_keys::key_segment(&shard_id.0));
    let Some(_guard) = crate::metadata_lock::try_metadata_inspection_lock(
        root.clone(),
        crate::metadata_cursor::journal_write_lock_path(&root),
    )?
    else {
        return Ok(None);
    };
    let Some(prefix) =
        crate::metadata_journal::admission::current_owned_materialized_prefix(&root, shard_id)?
    else {
        return Ok(None);
    };
    // This exact physical shard never follows or refreshes the global object
    // index. The writer fence remains held through authorization and opening
    // the reader, so the observational image cannot grant a stale permission.
    with_cached_dfs_metadata_inspection(
        root.clone(),
        || {
            Ok(DfsMetadataInspectionIdentity::ExactShard {
                metadata_root: root,
                shard_id: shard_id.clone(),
            })
        },
        |backend| {
            if backend.metadata_prefix_proof(shard_id)?.as_ref() != Some(&prefix) {
                return Ok(None);
            }
            inspect(backend).map(Some)
        },
    )
}

pub fn with_cached_cluster_aware_dfs_metadata_inspection<T>(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    inspect: impl FnOnce(&dyn DfsMetadataBackend) -> Result<T, DfsError>,
) -> Result<T, DfsError> {
    with_cached_dfs_metadata_inspection(
        data_dir.root(),
        || cluster_aware_dfs_metadata_inspection_identity(data_dir, cluster_paths),
        inspect,
    )
}

/// Run one coherent read-only inspection using topology already certified by
/// the caller's authority provider (normally the resident daemon).
pub fn with_cached_cluster_aware_dfs_metadata_inspection_with_topology<T>(
    data_dir: &SinomoDataDir,
    local_node_id: NodeId,
    topology: crate::DfsAuthoritativePrefixProjection,
    inspect: impl FnOnce(&dyn DfsMetadataBackend) -> Result<T, DfsError>,
) -> Result<T, DfsError> {
    let shards = sifs_metadata_shards_from_authoritative_topology(topology)?;
    with_cached_dfs_metadata_inspection(
        data_dir.root(),
        || {
            cluster_aware_dfs_metadata_inspection_identity_with_topology(
                data_dir,
                local_node_id,
                shards,
            )
        },
        inspect,
    )
}

/// Release the retained inspection image for this node's metadata root.
///
/// Writable maintenance may call this before opening a full backend
/// generation when bounding peak memory matters more than the next warm
/// observation.
pub fn evict_cluster_aware_dfs_metadata_inspection_cache(data_dir: &SinomoDataDir) {
    let Ok(mut cache) = dfs_metadata_inspection_cache().lock() else {
        return;
    };
    cache.evict(&data_dir.root());
    cache.evict(&default_dfs_metadata_root(data_dir));
    cache.evict(&default_dfs_metadata_shards_root(data_dir));
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
    let state = sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths)
        .map_err(DfsError::from_network)?;
    open_local_sharded_metadata_backend_for_replica_repair(
        default_dfs_metadata_shards_root(data_dir),
        shards,
        &state.local_node_id,
    )
    .map(Some)
    .map_err(Into::into)
}

pub fn open_cluster_sharded_dfs_metadata_backend_for_inspection(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
) -> Result<Option<LocalShardedMetadataBackend>, DfsError> {
    let file_system_id = sifs_file_system_id();
    let shards = cluster_dfs_metadata_shards(cluster_paths, &file_system_id)?;
    if shards.is_empty() {
        return Ok(None);
    }
    let state = sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths)
        .map_err(DfsError::from_network)?;
    open_local_sharded_metadata_backend_for_inspection_at_node(
        default_dfs_metadata_shards_root(data_dir),
        shards,
        &state.local_node_id,
    )
    .map(Some)
    .map_err(Into::into)
}

pub fn cluster_dfs_metadata_shards(
    cluster_paths: &ClusterPaths,
    file_system_id: &DfsFileSystemId,
) -> Result<Vec<DfsMetadataShardConfig>, DfsError> {
    let projection = DfsClusterTopologyProjection::observed_local_prefix(
        cluster_paths,
        &format!("{}/", metadata_shards_prefix(file_system_id)),
    )?;
    projection.metadata_shards(file_system_id)
}

/// One complete bounded `/dfs/filesystems/` prefix certified by cluster KV
/// authority for a single operation generation.
///
/// This is the only local-state topology view which may fence destructive
/// content collection or handoff activation. The ordinary `cluster_dfs_*`
/// helpers below are intentionally observational/recovery-only and fail fast
/// when the daemon-owned resident view is checked out.
#[derive(Clone, Debug)]
pub(crate) struct DfsClusterTopologyProjection {
    entries: Vec<sinomo_core::kv::KvEntry>,
    revision: sinomo_core::kv::KvRevision,
    complete: bool,
    authoritative: bool,
}

impl DfsClusterTopologyProjection {
    pub(crate) fn read_authoritative(
        control_plane: &mut dyn DfsControlPlaneKvClient,
    ) -> Result<Self, DfsError> {
        let prefix = "/dfs/filesystems/";
        let projection = control_plane
            .range_prefix_authoritative(prefix)
            .map_err(DfsError::from)?;
        Self::from_authoritative_prefix(projection)
    }

    pub(crate) fn from_authoritative_prefix(
        projection: crate::DfsAuthoritativePrefixProjection,
    ) -> Result<Self, DfsError> {
        let (entries, revision, complete) = projection.into_parts();
        if !complete {
            return Err(DfsError::Metadata {
                message: "authoritative DFS topology prefix was truncated".to_string(),
            });
        }
        Ok(Self {
            entries,
            revision,
            complete: true,
            authoritative: true,
        })
    }

    /// sinomo:kv-configuration: Inspect resident configuration scopes in the existing cluster topology projection without an authoritative refresh.
    fn observed_local_prefix(cluster_paths: &ClusterPaths, prefix: &str) -> Result<Self, DfsError> {
        cluster_dfs_observed_local_prefix_entries(cluster_paths, prefix).map(|read| Self {
            entries: read.value,
            revision: read.revision,
            complete: true,
            authoritative: false,
        })
    }

    pub(crate) fn require_destructive_authority(&self) -> Result<(), DfsError> {
        if self.authoritative && self.complete {
            Ok(())
        } else {
            Err(DfsError::Metadata {
                message: format!(
                    "destructive DFS topology requires a complete authority proof; projection revision {} is only observational",
                    self.revision.0,
                ),
            })
        }
    }

    pub(crate) fn metadata_shards(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Vec<DfsMetadataShardConfig>, DfsError> {
        let prefix = format!("{}/", metadata_shards_prefix(file_system_id));
        let mut shards = Vec::new();
        for entry in self
            .entries
            .iter()
            .filter(|entry| entry.key.starts_with(&prefix))
        {
            let shard = entry.decode::<DfsMetadataShardConfig>()?;
            let expected_key = crate::metadata_shard_key(file_system_id, &shard.shard_id);
            if shard.file_system_id != *file_system_id || entry.key != expected_key {
                return Err(DfsError::Metadata {
                    message: format!(
                        "authoritative DFS metadata shard entry {:?} does not match SIFS shard identity {}/{}",
                        entry.key, shard.file_system_id, shard.shard_id,
                    ),
                });
            }
            shards.push(shard);
        }
        shards.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
        Ok(shards)
    }

    pub(crate) fn all_metadata_shards(&self) -> Result<Vec<DfsMetadataShardConfig>, DfsError> {
        let mut shards = self
            .entries
            .iter()
            .filter(|entry| dfs_metadata_shard_config_key_is_exact(&entry.key))
            .map(|entry| entry.decode::<DfsMetadataShardConfig>())
            .collect::<Result<Vec<_>, _>>()?;
        shards.sort_by(|left, right| {
            left.file_system_id
                .cmp(&right.file_system_id)
                .then_with(|| left.shard_id.cmp(&right.shard_id))
        });
        shards.dedup_by(|left, right| {
            left.file_system_id == right.file_system_id && left.shard_id == right.shard_id
        });
        Ok(shards)
    }

    pub(crate) fn all_file_system_configs(&self) -> Result<Vec<DfsFileSystemConfig>, DfsError> {
        let mut file_systems = self
            .entries
            .iter()
            .filter(|entry| dfs_file_system_config_key_is_exact(&entry.key))
            .map(|entry| entry.decode::<DfsFileSystemConfig>())
            .collect::<Result<Vec<_>, _>>()?;
        file_systems.sort_by(|left, right| left.file_system_id.cmp(&right.file_system_id));
        file_systems.dedup_by(|left, right| left.file_system_id == right.file_system_id);
        Ok(file_systems)
    }

    pub(crate) fn all_metadata_shard_membership_handoffs(
        &self,
    ) -> Result<Vec<DfsMetadataShardMembershipHandoff>, DfsError> {
        let mut handoffs = self
            .entries
            .iter()
            .filter(|entry| dfs_metadata_shard_membership_handoff_key_is_exact(&entry.key))
            .map(|entry| entry.decode::<DfsMetadataShardMembershipHandoff>())
            .collect::<Result<Vec<_>, _>>()?;
        handoffs.sort_by(|left, right| {
            left.retiring_shard
                .file_system_id
                .cmp(&right.retiring_shard.file_system_id)
                .then_with(|| {
                    left.retiring_shard
                        .shard_id
                        .cmp(&right.retiring_shard.shard_id)
                })
                .then_with(|| left.handoff_id.cmp(&right.handoff_id))
        });
        handoffs.dedup_by(|left, right| left.handoff_id == right.handoff_id);
        Ok(handoffs)
    }

    pub(crate) fn all_metadata_shard_head_watermarks(
        &self,
    ) -> Result<Vec<DfsMetadataShardHeadWatermark>, DfsError> {
        let mut watermarks = self
            .entries
            .iter()
            .filter(|entry| dfs_metadata_shard_head_watermark_key_is_exact(&entry.key))
            .map(|entry| entry.decode::<DfsMetadataShardHeadWatermark>())
            .collect::<Result<Vec<_>, _>>()?;
        watermarks.sort_by(|left, right| {
            left.file_system_id
                .cmp(&right.file_system_id)
                .then_with(|| left.shard_id.cmp(&right.shard_id))
                .then_with(|| left.epoch.cmp(&right.epoch))
        });
        watermarks.dedup_by(|left, right| {
            left.file_system_id == right.file_system_id
                && left.shard_id == right.shard_id
                && left.epoch == right.epoch
        });
        Ok(watermarks)
    }
}

pub(crate) fn sifs_metadata_shards_from_authoritative_topology(
    topology: crate::DfsAuthoritativePrefixProjection,
) -> Result<Vec<DfsMetadataShardConfig>, DfsError> {
    DfsClusterTopologyProjection::from_authoritative_prefix(topology)?
        .metadata_shards(&sifs_file_system_id())
}

/// Return every exact metadata-shard assignment whose namespace shares this
/// node's content store.
///
/// The cluster KV prefix also contains filesystem configs, handoff proofs,
/// watermarks, and placement records. Filtering the key shape before decoding
/// prevents those compact control records from being mistaken for shard
/// assignments while still making cross-filesystem catalog coverage explicit.
pub fn cluster_dfs_all_metadata_shards(
    cluster_paths: &ClusterPaths,
) -> Result<Vec<DfsMetadataShardConfig>, DfsError> {
    DfsClusterTopologyProjection::observed_local_prefix(cluster_paths, "/dfs/filesystems/")?
        .all_metadata_shards()
}

/// Return every configured filesystem whose namespace shares this node's
/// content store. Filesystem records are part of the positive catalog
/// coverage proof even while shard bootstrap is incomplete.
pub fn cluster_dfs_all_file_system_configs(
    cluster_paths: &ClusterPaths,
) -> Result<Vec<DfsFileSystemConfig>, DfsError> {
    DfsClusterTopologyProjection::observed_local_prefix(cluster_paths, "/dfs/filesystems/")?
        .all_file_system_configs()
}

/// Return every in-progress metadata membership handoff across the shared
/// content store. A non-empty result blocks destructive collection unless a
/// future catalog format explicitly unions both generations.
pub fn cluster_dfs_all_metadata_shard_membership_handoffs(
    cluster_paths: &ClusterPaths,
) -> Result<Vec<DfsMetadataShardMembershipHandoff>, DfsError> {
    DfsClusterTopologyProjection::observed_local_prefix(cluster_paths, "/dfs/filesystems/")?
        .all_metadata_shard_membership_handoffs()
}

pub fn cluster_dfs_all_metadata_shard_head_watermarks(
    cluster_paths: &ClusterPaths,
) -> Result<Vec<DfsMetadataShardHeadWatermark>, DfsError> {
    DfsClusterTopologyProjection::observed_local_prefix(cluster_paths, "/dfs/filesystems/")?
        .all_metadata_shard_head_watermarks()
}

/// Read an already-resident local Raft prefix for observation or explicit
/// recovery diagnostics. This never opens/replays durable Raft state, waits on
/// the mutable resident checkout, or falls back to the materialized checkpoint.
/// It is not quorum authority and must never fence destructive work.
fn cluster_dfs_observed_local_prefix_entries(
    cluster_paths: &ClusterPaths,
    prefix: &str,
) -> Result<sinomo_core::kv::KvLocalRead<Vec<sinomo_core::kv::KvEntry>>, DfsError> {
    let state = sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths)
        .map_err(DfsError::from_network)?;
    sinomo_core::kv::read_cluster_kv_resident_local_prefix(
        &cluster_paths.kv_paths(),
        &state.local_node_id,
        prefix,
    )
    .map_err(DfsError::from)
}

fn dfs_file_system_config_key_is_exact(key: &str) -> bool {
    let mut segments = key.split('/');
    matches!(
        (
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
        ),
        (
            Some(""),
            Some("dfs"),
            Some("filesystems"),
            Some(file_system_id),
            Some("config"),
            None,
        ) if !file_system_id.is_empty()
    )
}

fn dfs_metadata_shard_config_key_is_exact(key: &str) -> bool {
    let mut segments = key.split('/');
    matches!(
        (
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
        ),
        (
            Some(""),
            Some("dfs"),
            Some("filesystems"),
            Some(file_system_id),
            Some("metadata-shards"),
            Some(shard_id),
            None,
        ) if !file_system_id.is_empty() && !shard_id.is_empty()
    )
}

pub fn dfs_metadata_shard_membership_handoff_key_is_exact(key: &str) -> bool {
    let mut segments = key.split('/');
    matches!(
        (
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
        ),
        (
            Some(""),
            Some("dfs"),
            Some("filesystems"),
            Some(file_system_id),
            Some("metadata-membership-handoffs"),
            Some(shard_id),
            None,
        ) if !file_system_id.is_empty() && !shard_id.is_empty()
    )
}

fn dfs_metadata_shard_head_watermark_key_is_exact(key: &str) -> bool {
    let mut segments = key.split('/');
    matches!(
        (
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
        ),
        (
            Some(""),
            Some("dfs"),
            Some("filesystems"),
            Some(file_system_id),
            Some("metadata-head-watermarks"),
            Some(shard_id),
            None,
        ) if !file_system_id.is_empty() && !shard_id.is_empty()
    )
}

pub fn inspect_cluster_aware_dfs_status(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
) -> Result<DfsOperatorStatus, DfsError> {
    with_cached_cluster_aware_dfs_metadata_inspection(data_dir, cluster_paths, |metadata| {
        inspect_dfs_status_from_metadata(data_dir, cluster_paths, metadata)
    })
}

/// Project operator status from an already-owned metadata generation.
///
/// Long-lived daemon services use this seam to avoid opening and retaining a
/// second decoded inspection backend over the same shard tables. Standalone
/// callers continue through [`inspect_cluster_aware_dfs_status`], which owns
/// the cold inspection fallback.
pub fn inspect_dfs_status_from_metadata(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    metadata: &dyn DfsMetadataBackend,
) -> Result<DfsOperatorStatus, DfsError> {
    let (mut status, primary_summary) = match metadata.metadata_status_summaries() {
        Ok((summary, primary_summary)) => (
            DfsOperatorInspector::new(metadata).status_from_summary(summary)?,
            primary_summary,
        ),
        Err(MetadataBackendError::StatusUnavailable { reason }) => {
            let mut status = DfsOperatorInspector::new(metadata)
                .status_from_summary(DfsMetadataSummary::default())?;
            status.file_system_count = status
                .metadata_shards
                .iter()
                .filter_map(|shard| shard.file_system_id.clone())
                .collect::<BTreeSet<_>>()
                .len();
            status
                .incomplete_reasons
                .push(format!("metadata summary unavailable: {reason}"));
            (status, DfsMetadataSummary::default())
        }
        Err(MetadataBackendError::StatusOmitted { omission }) => {
            let mut status = DfsOperatorInspector::new(metadata)
                .status_from_summary(DfsMetadataSummary::default())?;
            status.file_system_count = status
                .metadata_shards
                .iter()
                .filter_map(|shard| shard.file_system_id.clone())
                .collect::<BTreeSet<_>>()
                .len();
            status.push_bounded_omission(omission);
            (status, DfsMetadataSummary::default())
        }
        Err(error) => return Err(error.into()),
    };
    finalize_dfs_status_projection(data_dir, cluster_paths, &mut status, &primary_summary)?;
    Ok(status)
}

/// Project operator status from a bounded image already published by the
/// daemon's resident metadata generation. This path performs no metadata open,
/// hydration, table snapshot, or exact scan.
pub fn inspect_dfs_status_from_resident_snapshot(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    snapshot: DfsResidentMetadataStatusSnapshot,
) -> Result<DfsOperatorStatus, DfsError> {
    let local_node_id =
        sinomo_core::cluster::read_cluster_state_without_kv_cached_shared(cluster_paths)
            .ok()
            .map(|state| state.local_node_id.clone());
    let mut status = project_dfs_status_from_resident_snapshot(local_node_id.as_ref(), snapshot);
    attach_cluster_aware_dfs_maintenance_observations(&mut status, data_dir)?;
    Ok(status)
}

/// Project an already-resident DFS generation into operator status without
/// reading cluster state, repair ledgers, or metadata files.
pub fn project_dfs_status_from_resident_snapshot(
    local_node_id: Option<&NodeId>,
    snapshot: DfsResidentMetadataStatusSnapshot,
) -> DfsOperatorStatus {
    let DfsResidentMetadataStatusSnapshot {
        summary,
        primary_summary,
        mut metadata_shards,
        incomplete_reasons,
        bounded_omissions,
    } = snapshot;
    let cross_shard_namespace_lock_count = (!bounded_omissions.iter().any(|omission| {
        omission
            .omitted_metrics()
            .contains(&crate::DfsOperatorStatusMetric::CrossShardNamespaceLockCount)
    }))
    .then_some(summary.cross_shard_namespace_lock_count);
    if let Some(local_node_id) = local_node_id {
        for shard in &mut metadata_shards {
            if shard.primary_node_id.as_ref() == Some(local_node_id) {
                shard.replay_cursors.clear();
            } else {
                shard.replica_lag.clear();
            }
        }
    }
    let mut status = DfsOperatorStatus {
        incomplete_reasons,
        bounded_omissions,
        file_system_count: summary.file_system_count,
        object_count: summary.object_count,
        version_count: summary.version_count,
        receipt_count: summary.receipt_count,
        intent_count: summary.intent_count,
        pending_write_count: summary.pending_write_count,

        pending_repair_count: summary.pending_repair_count,
        repair_attempt_count: 0,
        pending_namespace_job_count: summary.pending_namespace_job_count,
        failed_namespace_job_count: summary.failed_namespace_job_count,
        cross_shard_namespace_lock_count,
        metadata_shards,
        repair_attempts: Vec::new(),
        storage_migrations: Vec::new(),
    };
    attach_primary_owned_operational_status(&mut status, &primary_summary);
    status
}

/// Return a bounded topology-only status image when no complete resident
/// metadata generation can be borrowed.
///
/// This deliberately does not open or hydrate any metadata shard. Every
/// omitted counter is paired with an explicit incomplete reason, while shard
/// assignments remain visible so cold nodes are distinguishable from an
/// actually empty cluster.
pub fn inspect_incomplete_dfs_status(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    reason: impl Into<String>,
) -> Result<DfsOperatorStatus, DfsError> {
    let mut incomplete_reasons = vec![reason.into()];
    let shards = match cluster_dfs_all_metadata_shards(cluster_paths) {
        Ok(shards) => shards,
        Err(error) => {
            incomplete_reasons.push(format!(
                "metadata shard topology observation unavailable: {error}"
            ));
            Vec::new()
        }
    };
    let file_system_ids = shards
        .iter()
        .map(|shard| shard.file_system_id.clone())
        .collect::<BTreeSet<_>>();
    let local_node_id = sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths)
        .ok()
        .map(|state| state.local_node_id);
    let primary_file_system_ids = shards
        .iter()
        .filter(|shard| {
            local_node_id
                .as_ref()
                .is_some_and(|local_node_id| shard.primary_node_id == *local_node_id)
        })
        .map(|shard| shard.file_system_id.clone())
        .collect::<BTreeSet<_>>();
    inspect_dfs_status_from_resident_snapshot(
        data_dir,
        cluster_paths,
        DfsResidentMetadataStatusSnapshot {
            summary: DfsMetadataSummary {
                file_system_count: file_system_ids.len(),
                ..DfsMetadataSummary::default()
            },
            primary_summary: DfsMetadataSummary {
                file_system_count: primary_file_system_ids.len(),
                ..DfsMetadataSummary::default()
            },
            metadata_shards: shards.iter().map(metadata_shard_topology_status).collect(),
            incomplete_reasons,
            bounded_omissions: Vec::new(),
        },
    )
}

fn finalize_dfs_status_projection(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    status: &mut DfsOperatorStatus,
    primary_summary: &DfsMetadataSummary,
) -> Result<(), DfsError> {
    attach_primary_owned_operational_status(status, primary_summary);
    attach_cluster_aware_dfs_maintenance_observations(status, data_dir)?;
    if let Ok(state) = sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths) {
        for shard in &mut status.metadata_shards {
            if shard.primary_node_id.as_ref() == Some(&state.local_node_id) {
                shard.replay_cursors.clear();
            } else {
                shard.replica_lag.clear();
            }
        }
    }
    Ok(())
}

fn attach_primary_owned_operational_status(
    status: &mut DfsOperatorStatus,
    primary_summary: &DfsMetadataSummary,
) {
    status.pending_write_count = primary_summary.pending_write_count;

    status.pending_repair_count = primary_summary.pending_repair_count;
    status.pending_namespace_job_count = primary_summary.pending_namespace_job_count;
    status.failed_namespace_job_count = primary_summary.failed_namespace_job_count;
}

fn attach_cluster_aware_dfs_maintenance_observations(
    status: &mut DfsOperatorStatus,
    data_dir: &SinomoDataDir,
) -> Result<(), DfsError> {
    // Historical conversion inspection belongs to the migration component.
    match LocalDfsRepairQueueStore::new(default_dfs_repair_attempt_snapshot_path(data_dir))
        .bounded_status_view(DFS_STATUS_REPAIR_ATTEMPT_SAMPLE_LIMIT)
    {
        Ok(DfsRepairQueueBoundedStatus::Hydrated(view)) => {
            status.repair_attempt_count = view.record_count;
            status.repair_attempts = view.records;
        }
        Ok(DfsRepairQueueBoundedStatus::NotHydrated) => {
            status.push_bounded_omission(
                DfsOperatorStatusOmission::resident_repair_queue_not_hydrated(),
            );
            status.repair_attempt_count = 0;
            status.repair_attempts.clear();
        }
        Err(error) => {
            status
                .incomplete_reasons
                .push(format!("repair queue observation unavailable: {error}"));
            status.repair_attempt_count = 0;
            status.repair_attempts.clear();
        }
    }
    Ok(())
}

pub fn inspect_cluster_aware_dfs(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    request: DaemonDfsInspectionRequest,
) -> Result<DaemonDfsInspectionResponse, DfsError> {
    match &request {
        read_only_request => {
            with_cached_cluster_aware_dfs_metadata_inspection(data_dir, cluster_paths, |metadata| {
                inspect_cluster_aware_dfs_read_only_from_metadata(
                    data_dir,
                    cluster_paths,
                    metadata,
                    read_only_request.clone(),
                )
            })
        }
    }
}

/// Serve one inspection request from the metadata generation already owned by
/// the embedding daemon. Callers must fence writable requests with their
/// ordinary metadata mutation admission before borrowing this backend.
pub fn inspect_cluster_aware_dfs_from_metadata(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    metadata: &mut dyn DfsMetadataBackend,
    request: DaemonDfsInspectionRequest,
) -> Result<DaemonDfsInspectionResponse, DfsError> {
    match request {
        read_only_request => inspect_cluster_aware_dfs_read_only_from_metadata(
            data_dir,
            cluster_paths,
            metadata,
            read_only_request,
        ),
    }
}

fn inspect_cluster_aware_dfs_read_only_from_metadata(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    metadata: &dyn DfsMetadataBackend,
    request: DaemonDfsInspectionRequest,
) -> Result<DaemonDfsInspectionResponse, DfsError> {
    let inspector = DfsOperatorInspector::new(metadata);
    Ok(match request {
        DaemonDfsInspectionRequest::Status => DaemonDfsInspectionResponse::Status {
            status: inspect_dfs_status_from_metadata(data_dir, cluster_paths, metadata)?,
        },
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DfsFileSystem;
    use crate::control_plane::DfsMetadataShardAssignment;
    use sinomo_core::DfsObjectPolicy;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    fn test_node(value: &str) -> NodeId {
        NodeId::from_legacy_named(value).expect("node id")
    }

    fn empty_status() -> DfsOperatorStatus {
        DfsOperatorStatus {
            incomplete_reasons: Vec::new(),
            bounded_omissions: Vec::new(),
            file_system_count: 1,
            object_count: 0,
            version_count: 0,
            receipt_count: 0,
            intent_count: 0,
            pending_write_count: 1,

            pending_repair_count: 1,
            repair_attempt_count: 0,
            pending_namespace_job_count: 1,
            failed_namespace_job_count: 1,
            cross_shard_namespace_lock_count: Some(0),
            metadata_shards: Vec::new(),
            repair_attempts: Vec::new(),
            storage_migrations: Vec::new(),
        }
    }

    #[test]
    fn resident_status_snapshot_fails_closed_for_omitted_cross_shard_locks() {
        let root = sinomo_testdir::TestDir::new("dfs-status-omitted-cross-shard-locks");
        let data_dir = SinomoDataDir::new(root.path());
        data_dir.ensure_root_layout().expect("data directory");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let file_system_id = sifs_file_system_id();
        let shard_id = DfsMetadataShardId::new("root");
        let omissions = [
            DfsOperatorStatusOmission::historical_routing_topology(1),
            DfsOperatorStatusOmission::non_primary_resident_metadata_not_hydrated(
                file_system_id,
                shard_id,
            ),
        ];

        for omission in omissions {
            assert!(
                omission
                    .omitted_metrics()
                    .contains(&crate::DfsOperatorStatusMetric::CrossShardNamespaceLockCount)
            );
            let status = inspect_dfs_status_from_resident_snapshot(
                &data_dir,
                &cluster_paths,
                DfsResidentMetadataStatusSnapshot {
                    summary: DfsMetadataSummary {
                        cross_shard_namespace_lock_count: 2,
                        ..DfsMetadataSummary::default()
                    },
                    primary_summary: DfsMetadataSummary::default(),
                    metadata_shards: Vec::new(),
                    incomplete_reasons: vec![omission.reason()],
                    bounded_omissions: vec![omission],
                },
            )
            .expect("bounded resident status");
            assert_eq!(status.cross_shard_namespace_lock_count, None);
        }

        let zero_proof =
            DfsOperatorStatusOmission::historical_routing_topology_with_cross_shard_lock_zero_proof(
                1,
            );
        let status = inspect_dfs_status_from_resident_snapshot(
            &data_dir,
            &cluster_paths,
            DfsResidentMetadataStatusSnapshot {
                summary: DfsMetadataSummary::default(),
                primary_summary: DfsMetadataSummary::default(),
                metadata_shards: Vec::new(),
                incomplete_reasons: vec![zero_proof.reason()],
                bounded_omissions: vec![zero_proof],
            },
        )
        .expect("bounded historical zero proof");
        assert_eq!(status.cross_shard_namespace_lock_count, Some(0));

        let status = inspect_dfs_status_from_resident_snapshot(
            &data_dir,
            &cluster_paths,
            DfsResidentMetadataStatusSnapshot {
                summary: DfsMetadataSummary {
                    cross_shard_namespace_lock_count: 2,
                    ..DfsMetadataSummary::default()
                },
                primary_summary: DfsMetadataSummary::default(),
                metadata_shards: Vec::new(),
                incomplete_reasons: Vec::new(),
                bounded_omissions: Vec::new(),
            },
        )
        .expect("complete resident status");
        assert_eq!(status.cross_shard_namespace_lock_count, Some(2));
    }

    #[test]
    fn observed_local_topology_fails_under_100ms_while_resident_node_is_checked_out() {
        let root = sinomo_testdir::TestDir::new("dfs-topology-held-resident-node");
        let data_dir = SinomoDataDir::new(root.path());
        data_dir.ensure_root_layout().expect("data directory");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local = test_node("node_topology_stale_materialized_local");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named("cluster_topology_stale_materialized")
                .expect("cluster id"),
            "topology stale materialized",
            local.clone(),
            "local",
            "public-key",
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");

        let file_system_id = sifs_file_system_id();
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: local.clone(),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,

            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let kv_paths = cluster_paths.kv_paths();
        let store = sinomo_core::kv::ClusterKvRaftStore::new(
            kv_paths.clone(),
            sinomo_core::kv::ClusterKvRaftCompactionPolicy {
                max_snapshot_bytes: u64::MAX,
                max_snapshot_events: sinomo_core::kv::MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS,
            },
        );
        let mut command = sinomo_core::kv::ClusterKvRaftLocalCommandStorage::new(
            store,
            local,
            sinomo_core::kv::cluster_kv_key_can_defer_materialized_snapshot,
        );
        let entry = command
            .append_uncommitted(sinomo_core::kv::KvCommandRequest::generated(
                sinomo_core::kv::KvCommand::Op(sinomo_core::kv::KvOp::Put {
                    key: crate::metadata_shard_key(&file_system_id, &shard.shard_id),
                    value: sinomo_core::kv::encode_kv_value(&shard).expect("encode shard"),
                    lease_id: None,
                }),
            ))
            .expect("append authoritative topology");
        command
            .commit_index_compact(entry.index)
            .expect("commit authoritative topology");
        command.release_session_after_commit();

        let _held_resident = kv_paths
            .raft_store()
            .open_node(&state.local_node_id)
            .expect("hold resident Raft node checkout");
        let started = Instant::now();
        let error = cluster_dfs_all_metadata_shards(&cluster_paths)
            .expect_err("observational topology must not wait for or reopen a checked-out node");
        assert!(started.elapsed() < Duration::from_millis(100), "{error}");

        let authoritative_topology = || {
            let key = crate::metadata_shard_key(&file_system_id, &shard.shard_id);
            crate::DfsAuthoritativePrefixProjection::complete(
                &format!("{}/", metadata_shards_prefix(&file_system_id)),
                vec![sinomo_core::kv::KvEntry {
                    key,
                    value: sinomo_core::kv::encode_kv_value(&shard).expect("encode shard"),
                    create_revision: sinomo_core::kv::KvRevision(1),
                    modify_revision: sinomo_core::kv::KvRevision(1),
                    version: sinomo_core::kv::KvVersion(1),
                    lease_id: None,
                }],
                sinomo_core::kv::KvRevision(1),
            )
            .expect("complete SIFS topology projection")
        };

        let opened = open_cluster_aware_dfs_metadata_backend_for_inspection_with_topology(
            &data_dir,
            &state.local_node_id,
            authoritative_topology(),
        )
        .expect("daemon-certified topology must bypass process-local Raft lookup");
        assert_eq!(
            opened
                .metadata_shard_statuses()
                .expect("explicit-topology shard status")[0]
                .shard_id,
            DfsMetadataShardId::new("root")
        );
        drop(opened);

        let statuses = with_cached_cluster_aware_dfs_metadata_inspection_with_topology(
            &data_dir,
            state.local_node_id.clone(),
            authoritative_topology(),
            |metadata| Ok(metadata.metadata_shard_statuses()?),
        )
        .expect("cached explicit-topology inspection must bypass process-local Raft lookup");
        assert_eq!(statuses[0].shard_id, DfsMetadataShardId::new("root"));
    }

    #[test]
    fn explicit_authoritative_topology_rejects_mismatched_sifs_shard_identity() {
        let root = sinomo_testdir::TestDir::new("dfs-topology-mismatched-sifs-identity");
        let data_dir = SinomoDataDir::new(root.path());
        let local_node_id = test_node("node_topology_mismatched_sifs_local");
        let file_system_id = sifs_file_system_id();
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "other".to_string(),
            primary_node_id: local_node_id.clone(),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,

            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let key = crate::metadata_shard_key(&file_system_id, "root");
        let topology = crate::DfsAuthoritativePrefixProjection::complete(
            &format!("{}/", metadata_shards_prefix(&file_system_id)),
            vec![sinomo_core::kv::KvEntry {
                key,
                value: sinomo_core::kv::encode_kv_value(&shard).expect("encode mismatched shard"),
                create_revision: sinomo_core::kv::KvRevision(1),
                modify_revision: sinomo_core::kv::KvRevision(1),
                version: sinomo_core::kv::KvVersion(1),
                lease_id: None,
            }],
            sinomo_core::kv::KvRevision(1),
        )
        .expect("bounded mismatched topology projection");

        let error = open_cluster_aware_dfs_metadata_backend_for_inspection_with_topology(
            &data_dir,
            &local_node_id,
            topology,
        )
        .expect_err("KV key and decoded SIFS shard identity must match");
        assert!(
            error
                .to_string()
                .contains("does not match SIFS shard identity")
        );
    }

    #[test]
    fn authoritative_topology_uses_newer_remote_projection_not_stale_local_entries() {
        #[derive(Debug)]
        struct StaleLocalNewerAuthority {
            local_entries: Vec<sinomo_core::kv::KvEntry>,
            authoritative: Option<crate::DfsAuthoritativePrefixProjection>,
            authority_reads: usize,
        }

        impl DfsControlPlaneKvClient for StaleLocalNewerAuthority {
            fn get(
                &self,
                _key: &str,
            ) -> Result<Option<sinomo_core::kv::KvEntry>, sinomo_core::kv::KvError> {
                Ok(None)
            }

            fn range_prefix(
                &self,
                _prefix: &str,
            ) -> Result<Vec<sinomo_core::kv::KvEntry>, sinomo_core::kv::KvError> {
                Ok(self.local_entries.clone())
            }

            fn range_prefix_authoritative(
                &mut self,
                _prefix: &str,
            ) -> Result<crate::DfsAuthoritativePrefixProjection, sinomo_core::kv::KvError>
            {
                self.authority_reads = self.authority_reads.saturating_add(1);
                self.authoritative.clone().ok_or_else(|| {
                    sinomo_core::kv::KvError::AuthorityUnavailable {
                        detail: "missing test authority projection".to_string(),
                    }
                })
            }

            fn put(
                &mut self,
                _key: String,
                _value: Vec<u8>,
            ) -> Result<sinomo_core::kv::KvRevision, sinomo_core::kv::KvError> {
                unreachable!("test control plane does not mutate")
            }

            fn txn(
                &mut self,
                _txn: sinomo_core::kv::KvTxn,
            ) -> Result<sinomo_core::kv::KvTxnResponse, sinomo_core::kv::KvError> {
                unreachable!("test control plane does not mutate")
            }
        }

        let file_system_id = sifs_file_system_id();
        let primary = test_node("node_topology_remote_primary");
        let shard_at_epoch = |epoch| DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: primary.clone(),
            replica_node_ids: Vec::new(),
            epoch,
            assignment: DfsMetadataShardAssignment::Root,

            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let key = crate::metadata_shard_key(&file_system_id, "root");
        let entry_at_epoch = |epoch, revision| sinomo_core::kv::KvEntry {
            key: key.clone(),
            value: sinomo_core::kv::encode_kv_value(&shard_at_epoch(epoch)).expect("encode shard"),
            create_revision: sinomo_core::kv::KvRevision(1),
            modify_revision: sinomo_core::kv::KvRevision(revision),
            version: sinomo_core::kv::KvVersion(revision),
            lease_id: None,
        };
        let mut control_plane = StaleLocalNewerAuthority {
            local_entries: vec![entry_at_epoch(1, 1)],
            authoritative: Some(
                crate::DfsAuthoritativePrefixProjection::complete(
                    "/dfs/filesystems/",
                    vec![entry_at_epoch(3, 3)],
                    sinomo_core::kv::KvRevision(3),
                )
                .expect("complete authority projection"),
            ),
            authority_reads: 0,
        };

        let topology = DfsClusterTopologyProjection::read_authoritative(&mut control_plane)
            .expect("authoritative topology");
        assert_eq!(
            topology.metadata_shards(&file_system_id).unwrap()[0].epoch,
            3
        );
        assert_eq!(control_plane.authority_reads, 1);
    }

    #[test]
    fn cold_repair_queue_is_a_typed_bounded_omission() {
        let root = sinomo_testdir::TestDir::new("dfs-status-cold-repair-queue-omission");
        let data_dir = SinomoDataDir::new(root.path());
        let mut status = empty_status();

        attach_cluster_aware_dfs_maintenance_observations(&mut status, &data_dir)
            .expect("attach cold repair queue observation");

        assert_eq!(status.repair_attempt_count, 0);
        assert!(status.repair_attempts.is_empty());
        assert!(status.bounded_omissions.iter().any(|omission| matches!(
            omission,
            DfsOperatorStatusOmission::ResidentRepairQueueNotHydrated
        )));
        assert!(status.blocking_incomplete_reasons().is_empty());
        assert!(status.incomplete_reasons.is_empty());
    }

    #[test]
    fn global_content_catalog_key_filters_distinguish_configs_shards_handoffs_and_watermarks() {
        let base = "/dfs/filesystems/sifs";
        assert!(dfs_file_system_config_key_is_exact(&format!(
            "{base}/config"
        )));
        assert!(!dfs_file_system_config_key_is_exact(&format!(
            "{base}/metadata-shards/root"
        )));
        assert!(dfs_metadata_shard_config_key_is_exact(&format!(
            "{base}/metadata-shards/root"
        )));
        assert!(dfs_metadata_shard_membership_handoff_key_is_exact(
            &format!("{base}/metadata-membership-handoffs/root")
        ));
        assert!(dfs_metadata_shard_head_watermark_key_is_exact(&format!(
            "{base}/metadata-head-watermarks/root"
        )));
        assert!(!dfs_metadata_shard_membership_handoff_key_is_exact(
            &format!("{base}/metadata-membership-handoffs/root/proof")
        ));
    }

    #[test]
    fn cached_inspection_reuses_one_open_and_observes_external_table_suffix() {
        let owner =
            crate::test_runtime::DfsTestRuntime::ordered_node_ids("native-cached-inspection", 1)
                .remove(0);
        let mut runtime =
            crate::test_runtime::DfsTestRuntime::new("native-cached-inspection", owner.clone())
                .unwrap();
        runtime.start_nodes().unwrap();
        let shard = runtime.root_shard().unwrap();
        let identity = DfsMetadataInspectionIdentity::ExactShard {
            metadata_root: default_dfs_metadata_shards_root(runtime.data_dir(&owner))
                .join(&shard.shard_id),
            shard_id: DfsMetadataShardId::new(shard.shard_id.clone()),
        };
        let original = with_cached_dfs_metadata_inspection_identity(identity.clone(), |metadata| {
            Ok(metadata.get_file_system(&shard.file_system_id)?)
        })
        .unwrap()
        .unwrap();
        let mut changed = original.clone();
        changed.display_name = "native changed configuration".into();
        let mut writer = runtime.client(&owner).unwrap();
        writer.add_file_system(changed.clone()).unwrap();
        for _ in 0..2 {
            let actual =
                with_cached_dfs_metadata_inspection_identity(identity.clone(), |metadata| {
                    Ok(metadata.get_file_system(&shard.file_system_id)?)
                })
                .unwrap()
                .unwrap();
            assert_eq!(actual, changed);
        }
        evict_cluster_aware_dfs_metadata_inspection_cache(runtime.data_dir(&owner));
        drop(writer);
        runtime.close().unwrap();
    }

    #[test]
    fn cached_inspection_reopens_on_exact_topology_change() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "sinomo-dfs-cached-topology-{}-{}",
            std::process::id(),
            nanos
        ));
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("root layout");
        let local_node_id = test_node("cached_topology_local");
        let mut shard = DfsMetadataShardConfig {
            file_system_id: sifs_file_system_id(),
            shard_id: "root".to_string(),
            primary_node_id: local_node_id.clone(),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,

            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let metadata_root = default_dfs_metadata_shards_root(&data_dir);
        drop(
            open_local_sharded_metadata_backend(metadata_root.clone(), vec![shard.clone()])
                .expect("initialize shard store"),
        );

        crate::metadata_backend::reset_metadata_backend_open_calls();
        let routing_topology_generation =
            dfs_metadata_inspection_file_generation(&routing_topology_history_path(&metadata_root))
                .expect("routing topology generation");
        let initial_identity = DfsMetadataInspectionIdentity::Sharded {
            metadata_root: metadata_root.clone(),
            local_node_id: local_node_id.clone(),
            shards: vec![shard.clone()],
            routing_topology_generation: routing_topology_generation.clone(),
        };
        for _ in 0..2 {
            with_cached_dfs_metadata_inspection_identity(initial_identity.clone(), |metadata| {
                metadata.metadata_summary().map(|_| ()).map_err(Into::into)
            })
            .expect("warm topology inspection");
        }
        shard.epoch = 2;
        with_cached_dfs_metadata_inspection_identity(
            DfsMetadataInspectionIdentity::Sharded {
                metadata_root: metadata_root.clone(),
                local_node_id: local_node_id.clone(),
                shards: vec![shard.clone()],
                routing_topology_generation: routing_topology_generation.clone(),
            },
            |metadata| metadata.metadata_summary().map(|_| ()).map_err(Into::into),
        )
        .expect("changed topology inspection");

        let topology_path = routing_topology_history_path(&metadata_root);
        let replacement_path = topology_path.with_extension("replacement.sobj");
        fs::copy(&topology_path, &replacement_path).expect("copy topology generation");
        fs::remove_file(&topology_path).expect("remove old topology generation");
        fs::rename(&replacement_path, &topology_path).expect("replace topology generation");
        let replaced_generation = dfs_metadata_inspection_file_generation(&topology_path)
            .expect("replaced topology generation");
        assert_ne!(replaced_generation, routing_topology_generation);
        with_cached_dfs_metadata_inspection_identity(
            DfsMetadataInspectionIdentity::Sharded {
                metadata_root,
                local_node_id,
                shards: vec![shard],
                routing_topology_generation: replaced_generation,
            },
            |metadata| metadata.metadata_summary().map(|_| ()).map_err(Into::into),
        )
        .expect("replaced topology history inspection");
        assert_eq!(
            crate::metadata_backend::metadata_backend_open_calls(),
            3,
            "warm topology, epoch change, and history replacement must each have exact reuse/invalidation"
        );

        evict_cluster_aware_dfs_metadata_inspection_cache(&data_dir);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cached_inspection_older_waiter_cannot_replace_newer_identity() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "sinomo-dfs-cached-topology-waiter-{}-{}",
            std::process::id(),
            nanos
        ));
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("root layout");
        let local_node_id = test_node("cached_topology_waiter_local");
        let older_shard = DfsMetadataShardConfig {
            file_system_id: sifs_file_system_id(),
            shard_id: "root".to_string(),
            primary_node_id: local_node_id.clone(),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,

            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let metadata_root = default_dfs_metadata_shards_root(&data_dir);
        drop(
            open_local_sharded_metadata_backend(metadata_root.clone(), vec![older_shard.clone()])
                .expect("initialize shard store"),
        );
        let routing_topology_generation =
            dfs_metadata_inspection_file_generation(&routing_topology_history_path(&metadata_root))
                .expect("routing topology generation");
        let older_identity = DfsMetadataInspectionIdentity::Sharded {
            metadata_root: metadata_root.clone(),
            local_node_id: local_node_id.clone(),
            shards: vec![older_shard.clone()],
            routing_topology_generation: routing_topology_generation.clone(),
        };
        let newer_identity = DfsMetadataInspectionIdentity::Sharded {
            metadata_root: metadata_root.clone(),
            local_node_id,
            shards: vec![DfsMetadataShardConfig {
                epoch: 2,
                ..older_shard
            }],
            routing_topology_generation,
        };

        crate::metadata_backend::reset_metadata_backend_open_calls();
        with_cached_dfs_metadata_inspection_identity(newer_identity.clone(), |metadata| {
            metadata.metadata_summary().map(|_| ()).map_err(Into::into)
        })
        .expect("install newer cached topology");
        with_cached_dfs_metadata_inspection_identity_refreshing(
            older_identity,
            || Ok(newer_identity.clone()),
            |metadata| metadata.metadata_summary().map(|_| ()).map_err(Into::into),
        )
        .expect("older waiter re-resolves after acquiring the cache slot");
        assert_eq!(
            crate::metadata_backend::metadata_backend_open_calls(),
            1,
            "an older waiter must reuse, not replace, the newer cached backend"
        );
        let slot = {
            let mut cache = dfs_metadata_inspection_cache().lock().expect("cache lock");
            cache.slot(&metadata_root)
        };
        assert_eq!(
            slot.lock()
                .expect("cache slot")
                .as_ref()
                .map(|cached| cached.identity.clone()),
            Some(newer_identity),
            "the cache must remain on the topology resolved after slot admission"
        );

        evict_cluster_aware_dfs_metadata_inspection_cache(&data_dir);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn primary_owned_operational_status_ignores_replica_only_backlog() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "sinomo-dfs-primary-owned-status-{}-{}",
            std::process::id(),
            nanos
        ));
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("root layout");
        let local = test_node("node_primary_status_local");
        let remote = test_node("node_primary_status_remote");
        let file_system_id = sifs_file_system_id();
        let local_primary = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: local.clone(),
            replica_node_ids: vec![remote.clone()],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,

            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let local_replica = DfsMetadataShardConfig {
            file_system_id,
            shard_id: "services".to_string(),
            primary_node_id: remote,
            replica_node_ids: vec![local.clone()],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::PathPrefix {
                prefix: "/services".to_string(),
            },

            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let shards = vec![local_primary, local_replica];
        let metadata_root = default_dfs_metadata_shards_root(&data_dir);
        drop(
            open_local_sharded_metadata_backend(metadata_root.clone(), shards.clone())
                .expect("initialize shard stores"),
        );
        let metadata = open_local_sharded_metadata_backend_for_inspection_at_node(
            metadata_root,
            shards,
            &local,
        )
        .expect("open all shards once");
        crate::metadata_backend::reset_metadata_current_tables_snapshot_calls();
        let (_, primary_summary) = metadata
            .metadata_status_summaries()
            .expect("combined status summaries");
        assert_eq!(
            crate::metadata_backend::metadata_current_tables_snapshot_calls(),
            0,
            "combined status summaries must use the resident projection without scanning shards"
        );
        crate::metadata_backend::reset_metadata_current_tables_snapshot_calls();
        let standalone_primary_summary = metadata
            .metadata_summary_for_local_primary_shards()
            .expect("primary-owned summary");
        assert_eq!(
            crate::metadata_backend::metadata_current_tables_snapshot_calls(),
            1,
            "the primary-owned summary must not scan the replica-only shard"
        );
        assert_eq!(standalone_primary_summary, primary_summary);

        let mut status = empty_status();
        attach_primary_owned_operational_status(&mut status, &primary_summary);

        assert_eq!(status.pending_write_count, 0);

        assert_eq!(status.pending_repair_count, 0);
        assert_eq!(status.pending_namespace_job_count, 0);
        assert_eq!(status.failed_namespace_job_count, 0);
        std::fs::remove_dir_all(root).ok();
    }
}
