//! DFS metadata journal replication and default metadata materialization.
//!
//! This module owns the metadata publish pipeline: deciding when service events
//! require journal publication, routing journal/snapshot replication to peer
//! nodes, validating incoming replica updates, and materializing the default
//! SIFS metadata records. Durable metadata storage itself remains owned by
//! `metadata_backend`.

use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use sinomo_core::cluster::ClusterKvPathsExt;
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::{
    ClusterConnectivityGraph, ClusterConnectivityState, ClusterPaths, NetworkError, ObjectRpcCall,
    ObjectRpcResponse, ObjectRpcResult, cluster_directory_for,
    cluster_nodes_share_healthy_component,
};
use sinomo_core::node::NodeId;
use sinomo_core::{ClusterBootstrapState, ClusterEndpointRecord};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::task::Poll;
use std::time::{Duration, Instant};
use time::OffsetDateTime;

use crate::maintenance::{
    DfsMaintenanceExecutionExclusions, migrate_legacy_dfs_metadata_replication_cursor,
};
use crate::node_transport::value_response_from_rpc_result;
use crate::{
    AsyncDfsPeerTransport, DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
    DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT, DfsDistributedWriteResult, DfsError, DfsFileSystem,
    DfsFileSystemId, DfsMetadataBackend, DfsMetadataJournalBatch, DfsMetadataJournalBatchCommand,
    DfsMetadataJournalBatchResponse, DfsMetadataJournalBatchResult, DfsMetadataPrefixProbe,
    DfsMetadataReplicaIngestPermit, DfsMetadataReplicationSuppressionRecord,
    DfsMetadataShardConfig, DfsMetadataShardId, DfsMetadataShardMembershipHandoff,
    DfsMetadataShardMigrationReport, DfsMetadataSnapshotInstallCommand,
    DfsMetadataSnapshotInstallResponse, DfsMetadataSnapshotInstallResult, DfsNamespaceResult,
    DfsObjectKind, DfsObjectRpcSendOptions, DfsObjectVersionRecord, DfsPeerTransport,
    DfsPeerTransportFuture, DfsVersion, LocalShardedMetadataBackend, LocalSobjMetadataBackend,
    NetworkedDfsServiceEvent, cluster_dfs_metadata_shards, cluster_dfs_peer_node_ids,
    decode_dfs_metadata_journal_batch_rpc_response,
    decode_dfs_metadata_snapshot_install_rpc_response,
    default_dfs_metadata_replication_suppression_snapshot_path,
    dfs_metadata_journal_batch_rpc_call, dfs_metadata_snapshot_install_rpc_call,
    dfs_now_unix_nanos, distributed_sifs, distributed_sifs_bootstrap_objects_for_nodes,
    local_dfs_metadata_shard_path, metadata_shard_key, metadata_shard_membership_handoff_key,
    open_local_metadata_shard_backend, put_local_dfs_metadata_replication_suppression,
    read_dfs_metadata_replication_suppression_snapshot, sifs_file_system_id,
};
use crate::{metadata_cursor, metadata_lock};

type ClusterMetadataBackendCache =
    BTreeMap<(PathBuf, DfsMetadataShardId), Arc<Mutex<LocalSobjMetadataBackend>>>;
static DFS_METADATA_REPLICATION_SUPPRESSION_WRITE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
const DFS_METADATA_UNREADABLE_REPLICA_SNAPSHOT_REPAIR_REASON: &str =
    "metadata replica durable state is unreadable and requires authoritative snapshot repair";

enum CachedClusterMetadataBackendError {
    CachePoisoned,
    Open(crate::MetadataBackendError),
}

fn metadata_backend_open_error_allows_snapshot_repair(error: &crate::MetadataBackendError) -> bool {
    matches!(error, crate::MetadataBackendError::Serde(_))
}

/// A topology change may leave legacy objects on their former metadata shard.
/// Both catalog inspection and all logical object mutations are bounded by
/// this per-generation budget; a durable object-id cursor resumes the next
/// generation without rescanning a settled prefix.
const DEFAULT_SIFS_BOOTSTRAP_SHARD_MIGRATION_MUTATION_LIMIT: usize = 256;
const DEFAULT_SIFS_BOOTSTRAP_SHARD_MIGRATION_PAGE_SIZE: usize = 32;
const DEFAULT_SIFS_BOOTSTRAP_SHARD_MIGRATION_CURSOR_FORMAT_VERSION: u16 = 5;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct DfsDefaultSifsMigrationGeneration {
    file_system_id: DfsFileSystemId,
    local_node_id: NodeId,
    shards: Vec<DfsMetadataShardConfig>,
    active_primary_shard_ids: Vec<DfsMetadataShardId>,
    certified_replica_shard_ids: Vec<DfsMetadataShardId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct DfsDefaultSifsMigrationCursor {
    format_version: u16,
    generation: DfsDefaultSifsMigrationGeneration,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    after_object_id: Option<crate::DfsObjectId>,
    /// Durable source-journal positions already converted into dirty-object
    /// replay. They close the ordered-catalog race without restarting the
    /// catalog for migration's own canonicalization writes.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    // SOBJ map keys are strings. Store the shard identifier's canonical
    // string directly so a durable cursor round-trips without asking serde to
    // reconstruct a tuple-newtype from a map-key token.
    observed_journal_sequences: BTreeMap<String, u64>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    dirty_object_ids: BTreeSet<crate::DfsObjectId>,
    /// Progress inside one journal entry whose affected-object set is larger
    /// than the per-pass replay capacity. The entry itself remains the durable
    /// source of truth; this cursor records only the settled sorted-ID prefix.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pending_journal_replay: Option<DfsDefaultSifsPendingJournalReplay>,
    /// A large object family is copied in bounded pages. Source eviction is
    /// deferred until every dependent record and the live object are durable
    /// on the target.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pending_migration: Option<DfsDefaultSifsPendingMigration>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct DfsDefaultSifsPendingJournalReplay {
    shard_id: DfsMetadataShardId,
    sequence: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    after_object_id: Option<crate::DfsObjectId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct DfsDefaultSifsPendingMigration {
    object: crate::DfsObjectMetadata,
    source_shard_id: DfsMetadataShardId,
    target_shard_id: DfsMetadataShardId,
    /// Exact source generations which may be evicted after target durability.
    /// Keeping the records (rather than only shard IDs) fences a concurrent
    /// source update that lands between the target/index stages and a later
    /// bounded source-eviction pass.
    writable_sources: Vec<DfsDefaultSifsPendingSource>,
    /// Catalog items advance the ordered cursor only after every physical
    /// stage completes; dirty replays never move that cursor backwards or
    /// forwards.
    catalog_object: bool,
    counts_as_migration: bool,
    stage: DfsDefaultSifsPendingMigrationStage,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct DfsDefaultSifsPendingSource {
    shard_id: DfsMetadataShardId,
    object: crate::DfsObjectMetadata,
    /// Last source journal sequence whose object-family effects are durable on
    /// the target. Source eviction atomically fences this exact prefix.
    #[serde(default)]
    replayed_through_sequence: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
enum DfsDefaultSifsPendingMigrationStage {
    EnsureFileSystem,
    Versions {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        after_version: Option<crate::DfsObjectVersionPageCursor>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        active_version: Option<DfsVersion>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        after_receipt: Option<crate::DfsReplicaReceiptPageCursor>,
    },
    ClearPinTransfers {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        after_transfer_id: Option<String>,
    },
    ClearVersionPins {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        after: Option<crate::DfsMetadataVersionPinPageCursor>,
    },
    PinTransfers {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        after_transfer_id: Option<String>,
    },
    VersionPins {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        after: Option<crate::DfsMetadataVersionPinPageCursor>,
    },
    Intents {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        after: Option<crate::DfsWriteIntentPageCursor>,
    },
    Conflicts {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        after: Option<crate::DfsConflictPageCursor>,
    },
    ObjectAndLock,
    Index,
    SourceEvictions {
        next_source: usize,
    },
    Complete,
}

#[derive(Debug)]
struct DfsDefaultSifsMigrationPassBudget {
    remaining_family_records: usize,
    remaining_physical_mutations: usize,
    physical_mutations: usize,
}

struct DfsDefaultSifsMigrationBackendContinuation {
    generation: DfsDefaultSifsMigrationGeneration,
    primary_backends: BTreeMap<DfsMetadataShardId, LocalSobjMetadataBackend>,
    replica_backends: BTreeMap<DfsMetadataShardId, LocalSobjMetadataBackend>,
}

static DFS_DEFAULT_SIFS_MIGRATION_BACKEND_CONTINUATIONS: OnceLock<
    Mutex<BTreeMap<PathBuf, DfsDefaultSifsMigrationBackendContinuation>>,
> = OnceLock::new();

fn default_sifs_migration_generation_base_matches(
    generation: &DfsDefaultSifsMigrationGeneration,
    file_system_id: &DfsFileSystemId,
    local_node_id: &NodeId,
    all_shards: &[DfsMetadataShardConfig],
    active_primary_shard_ids: &BTreeSet<DfsMetadataShardId>,
) -> bool {
    let candidate = default_sifs_migration_generation(
        file_system_id,
        local_node_id,
        all_shards,
        active_primary_shard_ids,
        &BTreeSet::new(),
    );
    generation.file_system_id == candidate.file_system_id
        && generation.local_node_id == candidate.local_node_id
        && generation.shards == candidate.shards
        && generation.active_primary_shard_ids == candidate.active_primary_shard_ids
}

pub(crate) fn default_sifs_migration_has_backend_continuation(
    metadata_root: &std::path::Path,
    file_system_id: &DfsFileSystemId,
    local_node_id: &NodeId,
    all_shards: &[DfsMetadataShardConfig],
    active_primary_shard_ids: &BTreeSet<DfsMetadataShardId>,
) -> bool {
    DFS_DEFAULT_SIFS_MIGRATION_BACKEND_CONTINUATIONS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .is_ok_and(|continuations| {
            continuations
                .get(metadata_root)
                .is_some_and(|continuation| {
                    default_sifs_migration_generation_base_matches(
                        &continuation.generation,
                        file_system_id,
                        local_node_id,
                        all_shards,
                        active_primary_shard_ids,
                    )
                })
        })
}

fn take_default_sifs_migration_backend_continuation(
    metadata_root: &std::path::Path,
    file_system_id: &DfsFileSystemId,
    local_node_id: &NodeId,
    all_shards: &[DfsMetadataShardConfig],
    active_primary_shard_ids: &BTreeSet<DfsMetadataShardId>,
) -> Option<DfsDefaultSifsMigrationBackendContinuation> {
    let mut continuations = DFS_DEFAULT_SIFS_MIGRATION_BACKEND_CONTINUATIONS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .ok()?;
    let continuation = continuations.remove(metadata_root)?;
    default_sifs_migration_generation_base_matches(
        &continuation.generation,
        file_system_id,
        local_node_id,
        all_shards,
        active_primary_shard_ids,
    )
    .then_some(continuation)
}

fn put_default_sifs_migration_backend_continuation(
    metadata_root: PathBuf,
    continuation: DfsDefaultSifsMigrationBackendContinuation,
) {
    if let Ok(mut continuations) = DFS_DEFAULT_SIFS_MIGRATION_BACKEND_CONTINUATIONS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
    {
        continuations.insert(metadata_root, continuation);
    }
}

impl DfsDefaultSifsMigrationPassBudget {
    fn new(limit: usize) -> Self {
        Self {
            remaining_family_records: limit,
            remaining_physical_mutations: limit,
            physical_mutations: 0,
        }
    }

    fn can_read_family_record(&self) -> bool {
        self.remaining_family_records > 0
    }

    fn charge_family_record(&mut self) {
        self.remaining_family_records = self.remaining_family_records.saturating_sub(1);
    }

    fn can_mutate(&self) -> bool {
        self.remaining_physical_mutations > 0
    }

    fn charge_mutation(&mut self) {
        self.remaining_physical_mutations = self.remaining_physical_mutations.saturating_sub(1);
        self.physical_mutations = self.physical_mutations.saturating_add(1);
    }
}

fn default_sifs_migration_generation(
    file_system_id: &DfsFileSystemId,
    local_node_id: &NodeId,
    all_shards: &[DfsMetadataShardConfig],
    active_primary_shard_ids: &BTreeSet<DfsMetadataShardId>,
    certified_replica_shard_ids: &BTreeSet<DfsMetadataShardId>,
) -> DfsDefaultSifsMigrationGeneration {
    let mut shards = all_shards
        .iter()
        .filter(|shard| shard.file_system_id == *file_system_id)
        .cloned()
        .collect::<Vec<_>>();
    shards.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
    DfsDefaultSifsMigrationGeneration {
        file_system_id: file_system_id.clone(),
        local_node_id: local_node_id.clone(),
        shards,
        active_primary_shard_ids: active_primary_shard_ids.iter().cloned().collect(),
        certified_replica_shard_ids: certified_replica_shard_ids.iter().cloned().collect(),
    }
}

fn default_sifs_migration_cursor_path(
    metadata_root: &std::path::Path,
    file_system_id: &DfsFileSystemId,
) -> PathBuf {
    metadata_root
        .join("shard-index")
        .join("migration-cursors")
        .join(format!(
            "default-sifs-{}.sobj",
            crate::metadata_keys::key_segment(file_system_id)
        ))
}

fn read_default_sifs_migration_cursor(
    metadata_root: &std::path::Path,
    file_system_id: &DfsFileSystemId,
    generation: &DfsDefaultSifsMigrationGeneration,
) -> Result<Option<DfsDefaultSifsMigrationCursor>, DfsError> {
    let path = default_sifs_migration_cursor_path(metadata_root, file_system_id);
    let cursor = crate::metadata_io::read_sobj_optional::<DfsDefaultSifsMigrationCursor>(&path)?;
    match cursor {
        Some(cursor)
            if cursor.format_version
                == DEFAULT_SIFS_BOOTSTRAP_SHARD_MIGRATION_CURSOR_FORMAT_VERSION
                && cursor.generation == *generation =>
        {
            Ok(Some(cursor))
        }
        Some(_) => {
            // An obsolete generation must not leave a permanent journal-prune
            // fence after membership or shard configuration changes.
            crate::metadata_io::remove_file_if_exists(&path)?;
            Ok(None)
        }
        None => Ok(None),
    }
}

fn write_default_sifs_migration_cursor(
    metadata_root: &std::path::Path,
    cursor: &DfsDefaultSifsMigrationCursor,
) -> Result<(), DfsError> {
    let path = default_sifs_migration_cursor_path(metadata_root, &cursor.generation.file_system_id);
    if crate::metadata_io::read_sobj_optional::<DfsDefaultSifsMigrationCursor>(&path)?.as_ref()
        == Some(cursor)
    {
        return Ok(());
    }
    let parent = path.parent().ok_or_else(|| DfsError::Metadata {
        message: format!(
            "default SIFS migration cursor {} has no parent",
            path.display()
        ),
    })?;
    crate::metadata_io::create_dir_all(parent.to_path_buf())?;
    crate::metadata_io::write_sobj_atomic(&path, cursor)?;
    Ok(())
}

fn clear_default_sifs_migration_cursor(
    metadata_root: &std::path::Path,
    file_system_id: &DfsFileSystemId,
) -> Result<(), DfsError> {
    crate::metadata_io::remove_file_if_exists(&default_sifs_migration_cursor_path(
        metadata_root,
        file_system_id,
    ))?;
    Ok(())
}

#[cfg(test)]
thread_local! {
    static DEFAULT_SIFS_MIGRATION_MAX_BUFFERED_RECORDS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn reset_default_sifs_migration_max_buffered_records() {
    DEFAULT_SIFS_MIGRATION_MAX_BUFFERED_RECORDS.set(0);
}

#[cfg(test)]
fn default_sifs_migration_max_buffered_records() -> usize {
    DEFAULT_SIFS_MIGRATION_MAX_BUFFERED_RECORDS.get()
}

#[cfg(test)]
fn observe_default_sifs_migration_buffered_records(records: usize) {
    DEFAULT_SIFS_MIGRATION_MAX_BUFFERED_RECORDS.set(
        DEFAULT_SIFS_MIGRATION_MAX_BUFFERED_RECORDS
            .get()
            .max(records),
    );
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DfsDefaultSifsMaterializationReport {
    pub(crate) migration: DfsMetadataShardMigrationReport,
    /// A local authority still needs an exact replica generation before it can
    /// safely finish one side of a cross-node shard migration.
    pub(crate) awaiting_replica_progress: bool,
}

fn cached_cluster_metadata_backend(
    data_dir: &SinomoDataDir,
    shard_id: &DfsMetadataShardId,
) -> Result<Arc<Mutex<LocalSobjMetadataBackend>>, CachedClusterMetadataBackendError> {
    static BACKENDS: OnceLock<Mutex<ClusterMetadataBackendCache>> = OnceLock::new();
    let key = (data_dir.root(), shard_id.clone());
    let mut backends = BACKENDS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .map_err(|_| CachedClusterMetadataBackendError::CachePoisoned)?;
    if let Some(backend) = backends.get(&key) {
        return Ok(Arc::clone(backend));
    }
    let root = local_dfs_metadata_shard_path(data_dir, shard_id);
    let backend = Arc::new(Mutex::new(
        LocalSobjMetadataBackend::new(shard_id.clone(), root)
            .map_err(CachedClusterMetadataBackendError::Open)?,
    ));
    backends.insert(key, Arc::clone(&backend));
    Ok(backend)
}

pub const DEFAULT_DFS_METADATA_REPLICATION_MAX_BATCHES_PER_TARGET: usize = 64;
const DEFAULT_DFS_METADATA_REPLICATION_MAX_CONCURRENT_TARGET_NODES: usize = 8;

fn read_cluster_dfs_metadata_publish_state(
    cluster_paths: &ClusterPaths,
) -> Result<ClusterBootstrapState, DfsError> {
    // Metadata replica targets are cluster membership, not bootstrap state.
    // A joining node can retain a deliberately small bootstrap file while a
    // newer accepted KV snapshot already contains the full member set. Using
    // the bootstrap-only view here turns valid replica targets into
    // `MissingRoute` and parks the retained publisher until another mutation
    // happens to wake it.
    sinomo_core::cluster::read_cluster_state(cluster_paths).map_err(DfsError::from_network)
}

pub fn publish_cluster_dfs_metadata_journal(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    sender: DfsMetadataJournalBatchSender,
    snapshot_sender: DfsMetadataSnapshotInstallSender,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<DfsMetadataJournalReplicationReport, DfsError> {
    if !cluster_paths.state.exists() {
        return Ok(DfsMetadataJournalReplicationReport {
            outcomes: Vec::new(),
        });
    }

    let state = read_cluster_dfs_metadata_publish_state(cluster_paths)?;
    let file_system = distributed_sifs(state.local_node_id.clone());
    let shards = cluster_dfs_metadata_shards(cluster_paths, &file_system.file_system_id)?;

    let peer_node_ids = cluster_dfs_peer_node_ids(&state);
    let mut transport = DfsMetadataNodeTransport::new(peer_node_ids, sender, snapshot_sender);
    let mut host = LocalDfsMetadataJournalPublisherHost::new(data_dir.clone());
    publish_dfs_metadata_journal_with_host(
        file_system.file_system_id,
        &state.local_node_id,
        shards,
        &mut host,
        &mut transport,
        options,
    )
}

pub async fn publish_cluster_dfs_metadata_journal_async(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    sender: AsyncDfsMetadataJournalBatchSender,
    snapshot_sender: AsyncDfsMetadataSnapshotInstallSender,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<DfsMetadataJournalReplicationReport, DfsError> {
    publish_cluster_dfs_metadata_journal_async_with_execution_exclusions(
        data_dir,
        cluster_paths,
        sender,
        snapshot_sender,
        &DfsMaintenanceExecutionExclusions::default(),
        options,
    )
    .await
}

pub async fn publish_cluster_dfs_metadata_journal_async_with_execution_exclusions(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    sender: AsyncDfsMetadataJournalBatchSender,
    snapshot_sender: AsyncDfsMetadataSnapshotInstallSender,
    execution_exclusions: &DfsMaintenanceExecutionExclusions,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<DfsMetadataJournalReplicationReport, DfsError> {
    let Some(plan) = prepare_cluster_dfs_metadata_journal_publish_async(
        data_dir,
        cluster_paths,
        execution_exclusions,
        None,
        options,
    )
    .await?
    else {
        return Ok(DfsMetadataJournalReplicationReport {
            outcomes: Vec::new(),
        });
    };
    let mut transport =
        AsyncDfsMetadataNodeTransport::new(plan.peer_node_ids.clone(), sender, snapshot_sender);
    execute_prepared_dfs_metadata_journal_publish_async(plan, &mut transport, options).await
}

pub fn publish_cluster_dfs_metadata_journal_with_peer_transport(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    peer_transport: Arc<dyn DfsPeerTransport>,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<DfsMetadataJournalReplicationReport, DfsError> {
    refresh_cluster_dfs_kv_snapshot_before_metadata_publish_with_peer_transport(
        cluster_paths,
        peer_transport.as_ref(),
    )?;
    publish_cluster_dfs_metadata_journal(
        data_dir,
        cluster_paths,
        dfs_metadata_journal_batch_sender_from_peer_transport(Arc::clone(&peer_transport)),
        dfs_metadata_snapshot_install_sender_from_peer_transport(peer_transport),
        options,
    )
}

pub async fn publish_cluster_dfs_metadata_journal_with_async_peer_transport(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<DfsMetadataJournalReplicationReport, DfsError> {
    publish_cluster_dfs_metadata_journal_async(
        data_dir,
        cluster_paths,
        async_dfs_metadata_journal_batch_sender_from_peer_transport(Arc::clone(&peer_transport)),
        async_dfs_metadata_snapshot_install_sender_from_peer_transport(peer_transport),
        options,
    )
    .await
}

pub async fn publish_cluster_dfs_metadata_journal_with_peer_transports_async(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    sync_peer_transport: Arc<dyn DfsPeerTransport>,
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<DfsMetadataJournalReplicationReport, DfsError> {
    publish_cluster_dfs_metadata_journal_with_peer_transports_async_and_execution_exclusions(
        data_dir,
        cluster_paths,
        sync_peer_transport,
        peer_transport,
        &DfsMaintenanceExecutionExclusions::default(),
        options,
    )
    .await
}

pub async fn publish_cluster_dfs_metadata_journal_with_peer_transports_async_and_execution_exclusions(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    sync_peer_transport: Arc<dyn DfsPeerTransport>,
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    execution_exclusions: &DfsMaintenanceExecutionExclusions,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<DfsMetadataJournalReplicationReport, DfsError> {
    let refresh_cluster_paths = cluster_paths.clone();
    tokio::task::spawn_blocking(move || {
        refresh_cluster_dfs_kv_snapshot_before_metadata_publish_with_peer_transport(
            &refresh_cluster_paths,
            sync_peer_transport.as_ref(),
        )
    })
    .await
    .map_err(|source| DfsError::Metadata {
        message: format!("DFS metadata publish KV refresh task failed: {source}"),
    })??;

    publish_cluster_dfs_metadata_journal_async_with_execution_exclusions(
        data_dir,
        cluster_paths,
        async_dfs_metadata_journal_batch_sender_from_peer_transport(Arc::clone(&peer_transport)),
        async_dfs_metadata_snapshot_install_sender_from_peer_transport(peer_transport),
        execution_exclusions,
        options,
    )
    .await
}

/// Publish one locally authoritative metadata-shard lane.
///
/// Daemon reactors use this narrower entrypoint so an unavailable replica for
/// one shard cannot occupy the publication lane for unrelated local primaries.
/// The journal batch is still planned from the same durable shard backend and
/// the ordinary receiver-side authority checks remain unchanged.
#[allow(clippy::too_many_arguments)]
pub async fn publish_cluster_dfs_metadata_shard_with_peer_transports_async_and_execution_exclusions(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    sync_peer_transport: Arc<dyn DfsPeerTransport>,
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    file_system_id: DfsFileSystemId,
    shard_id: DfsMetadataShardId,
    execution_exclusions: &DfsMaintenanceExecutionExclusions,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<DfsMetadataJournalReplicationReport, DfsError> {
    let refresh_cluster_paths = cluster_paths.clone();
    tokio::task::spawn_blocking(move || {
        refresh_cluster_dfs_kv_snapshot_with_peer_transport(
            &refresh_cluster_paths,
            sync_peer_transport,
        )
    })
    .await
    .map_err(|source| DfsError::Metadata {
        message: format!("DFS metadata publish KV refresh task failed: {source}"),
    })??;

    let Some(plan) = prepare_cluster_dfs_metadata_journal_publish_async(
        data_dir,
        cluster_paths,
        execution_exclusions,
        Some((file_system_id, shard_id)),
        options,
    )
    .await?
    else {
        return Ok(DfsMetadataJournalReplicationReport {
            outcomes: Vec::new(),
        });
    };
    let mut transport = AsyncDfsMetadataNodeTransport::new(
        plan.peer_node_ids.clone(),
        async_dfs_metadata_journal_batch_sender_from_peer_transport(Arc::clone(&peer_transport)),
        async_dfs_metadata_snapshot_install_sender_from_peer_transport(peer_transport),
    );
    execute_prepared_dfs_metadata_journal_publish_async(plan, &mut transport, options).await
}

pub fn refresh_cluster_dfs_kv_snapshot_with_peer_transport(
    cluster_paths: &ClusterPaths,
    peer_transport: Arc<dyn DfsPeerTransport>,
) -> Result<(), DfsError> {
    if !cluster_paths.state.exists() {
        return Ok(());
    }
    let state = sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths)
        .map_err(DfsError::from_network)?;
    if sinomo_core::kv::cluster_kv_raft_local_is_voter(&state) {
        return Ok(());
    }
    refresh_cluster_dfs_kv_snapshot_with_peer_transport_for_state(
        cluster_paths,
        &state,
        peer_transport.as_ref(),
    )
}

pub(crate) fn refresh_cluster_dfs_kv_snapshot_before_metadata_publish_with_peer_transport(
    cluster_paths: &ClusterPaths,
    peer_transport: &dyn DfsPeerTransport,
) -> Result<(), DfsError> {
    if !cluster_paths.state.exists() {
        return Ok(());
    }
    let state = sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths)
        .map_err(DfsError::from_network)?;
    if sinomo_core::kv::cluster_kv_raft_local_is_voter(&state) {
        return Ok(());
    }
    let file_system = distributed_sifs(state.local_node_id.clone());
    let shards = cluster_dfs_metadata_shards(cluster_paths, &file_system.file_system_id)?;
    if !shards
        .iter()
        .any(|shard| shard.primary_node_id == state.local_node_id)
    {
        return Ok(());
    }
    refresh_cluster_dfs_kv_snapshot_with_peer_transport_for_state(
        cluster_paths,
        &state,
        peer_transport,
    )
}

fn refresh_cluster_dfs_kv_snapshot_with_peer_transport_for_state(
    cluster_paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    peer_transport: &dyn DfsPeerTransport,
) -> Result<(), DfsError> {
    let options = sinomo_core::cluster::ClusterControlPlaneTimeouts::default_daemon_transport()
        .kv_snapshot_refresh_options();
    sinomo_core::cluster::refresh_current_cluster_kv_store_with_local_storage_and_transport(
        cluster_paths,
        state,
        options,
        |target_node_id, timeout| {
            let call = ObjectRpcCall::encode_value(
                sinomo_core::kv::CLUSTER_KV_AUTHORITATIVE_SNAPSHOT_ROUTE,
                sinomo_core::kv::CLUSTER_KV_RPC_CATEGORY,
                sinomo_core::kv::ClusterKvAuthoritativeSnapshotRequest,
            )?;
            let response = peer_transport
                .call_object_rpc(
                    target_node_id,
                    call,
                    DfsObjectRpcSendOptions::default()
                        .with_overall_timeout(timeout)
                        .with_observe_connectivity(false),
                )
                .map_err(|source| NetworkError::Io {
                    operation: "DFS cluster KV snapshot refresh".to_string(),
                    detail: source.to_string(),
                })?;
            let response =
                value_response_from_rpc_result(response).map_err(|source| NetworkError::Io {
                    operation: "decode DFS cluster KV snapshot response".to_string(),
                    detail: source.to_string(),
                })?;
            response.error_detail()?;
            sinomo_core::kv::decode_cluster_kv_authoritative_snapshot_rpc_body(&response.body)
                .map_err(sinomo_core::network::NetworkError::directory)
        },
    )
    .map_err(DfsError::from_network)?;
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsMetadataReplicaShardContext {
    pub local_node_id: NodeId,
    pub shard: DfsMetadataShardConfig,
    pub pending_membership_handoff: Option<DfsMetadataShardMembershipHandoff>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DfsMetadataJournalReplicationOptions {
    pub batch_entry_limit: usize,
    pub batch_byte_limit: usize,
    pub max_batches_per_target: usize,
    pub attempt_timeout: Duration,
    pub target_timeout: Duration,
    /// Send one empty authenticated batch to a replica whose durable sender
    /// cursor is already at the primary head. Intended for periodic
    /// maintenance and explicit repair commands, not foreground operations.
    pub probe_caught_up_replicas: bool,
}

impl Default for DfsMetadataJournalReplicationOptions {
    fn default() -> Self {
        Self {
            batch_entry_limit: DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            batch_byte_limit: DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
            max_batches_per_target: DEFAULT_DFS_METADATA_REPLICATION_MAX_BATCHES_PER_TARGET,
            attempt_timeout: sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            target_timeout: sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            probe_caught_up_replicas: false,
        }
    }
}

fn dfs_metadata_replication_attempt_timeout(
    options: DfsMetadataJournalReplicationOptions,
    target_started_at: Instant,
) -> Option<Duration> {
    let remaining = options
        .target_timeout
        .checked_sub(target_started_at.elapsed())?;
    if remaining.is_zero() {
        return None;
    }
    Some(options.attempt_timeout.min(remaining))
}

#[cfg(test)]
mod replication_budget_tests {
    use super::*;

    #[test]
    fn metadata_replication_defaults_bound_attempt_and_target_time() {
        let options = DfsMetadataJournalReplicationOptions::default();
        assert_eq!(
            options.attempt_timeout,
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT
        );
        assert_eq!(
            options.target_timeout,
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT
        );
        assert!(!options.probe_caught_up_replicas);
    }

    #[test]
    fn metadata_replication_attempt_uses_remaining_target_budget() {
        let options = DfsMetadataJournalReplicationOptions {
            attempt_timeout: Duration::from_secs(10),
            target_timeout: Duration::from_secs(15),
            ..DfsMetadataJournalReplicationOptions::default()
        };

        let fresh = dfs_metadata_replication_attempt_timeout(options, Instant::now())
            .expect("fresh attempt budget");
        assert!(fresh > Duration::from_secs(9));
        assert!(fresh <= Duration::from_secs(10));

        let partly_spent = dfs_metadata_replication_attempt_timeout(
            options,
            Instant::now() - Duration::from_secs(9),
        )
        .expect("remaining target budget");
        assert!(partly_spent > Duration::from_secs(5));
        assert!(partly_spent <= Duration::from_secs(6));

        assert!(
            dfs_metadata_replication_attempt_timeout(
                options,
                Instant::now() - Duration::from_secs(15),
            )
            .is_none()
        );
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataJournalReplicationReport {
    pub outcomes: Vec<DfsMetadataJournalReplicationOutcome>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsMetadataJournalReplicationOutcome {
    MissingShard {
        file_system_id: DfsFileSystemId,
        shard_id: DfsMetadataShardId,
    },
    NotPrimary {
        file_system_id: DfsFileSystemId,
        shard_id: DfsMetadataShardId,
        local_node_id: NodeId,
        primary_node_id: NodeId,
        epoch: u64,
    },
    Applied {
        target_node_id: NodeId,
        shard_id: DfsMetadataShardId,
        entries: usize,
        next_after_sequence: u64,
    },
    NoChanges {
        target_node_id: NodeId,
        shard_id: DfsMetadataShardId,
        after_sequence: u64,
    },
    MissingRoute {
        target_node_id: NodeId,
        shard_id: DfsMetadataShardId,
        after_sequence: u64,
    },
    TopologyMismatchSuppressed {
        target_node_id: NodeId,
        shard_id: DfsMetadataShardId,
        epoch: u64,
        reason: String,
    },
    Rejected {
        target_node_id: NodeId,
        shard_id: DfsMetadataShardId,
        reason: String,
    },
    SnapshotInstalled {
        target_node_id: NodeId,
        shard_id: DfsMetadataShardId,
        next_after_sequence: u64,
        archived_existing: bool,
    },
    Failed {
        target_node_id: NodeId,
        shard_id: DfsMetadataShardId,
        reason: String,
    },
    BacklogRemaining {
        target_node_id: NodeId,
        shard_id: DfsMetadataShardId,
        after_sequence: u64,
        max_batches: usize,
    },
}

pub fn dfs_metadata_replication_sent_count(report: &DfsMetadataJournalReplicationReport) -> usize {
    report
        .outcomes
        .iter()
        .filter(|outcome| {
            matches!(
                outcome,
                DfsMetadataJournalReplicationOutcome::Applied { .. }
                    | DfsMetadataJournalReplicationOutcome::SnapshotInstalled { .. }
            )
        })
        .count()
}

pub fn dfs_metadata_replication_notice_lines(
    report: &DfsMetadataJournalReplicationReport,
) -> Vec<String> {
    report
        .outcomes
        .iter()
        .filter_map(|outcome| match outcome {
            DfsMetadataJournalReplicationOutcome::Applied {
                target_node_id,
                entries,
                ..
            } => Some(format!(
                "replicated {entries} DFS metadata journal entries to {target_node_id}"
            )),
            DfsMetadataJournalReplicationOutcome::SnapshotInstalled {
                target_node_id,
                next_after_sequence,
                ..
            } => Some(format!(
                "repaired DFS metadata replica {target_node_id} with snapshot through {next_after_sequence}"
            )),
            DfsMetadataJournalReplicationOutcome::Rejected {
                target_node_id,
                reason,
                ..
            }
            | DfsMetadataJournalReplicationOutcome::Failed {
                target_node_id,
                reason,
                ..
            } => Some(format!(
                "DFS metadata replication to {target_node_id} failed: {reason}"
            )),
            DfsMetadataJournalReplicationOutcome::BacklogRemaining {
                target_node_id,
                max_batches,
                ..
            } => Some(format!(
                "DFS metadata replication to {target_node_id} still has backlog after {max_batches} batches"
            )),
            DfsMetadataJournalReplicationOutcome::NoChanges { .. }
            | DfsMetadataJournalReplicationOutcome::MissingRoute { .. }
            | DfsMetadataJournalReplicationOutcome::TopologyMismatchSuppressed { .. }
            | DfsMetadataJournalReplicationOutcome::MissingShard { .. }
            | DfsMetadataJournalReplicationOutcome::NotPrimary { .. } => None,
        })
        .collect()
}

pub fn dfs_events_should_publish_metadata(events: &[NetworkedDfsServiceEvent]) -> bool {
    events.iter().any(|event| match event {
        NetworkedDfsServiceEvent::BlobStaged { .. }
        | NetworkedDfsServiceEvent::VersionCommitted { .. }
        | NetworkedDfsServiceEvent::ReplicaRepaired { .. }
        | NetworkedDfsServiceEvent::WriteConflictCreated { .. }
        | NetworkedDfsServiceEvent::OwnerHandoffCompleted { .. } => true,
        NetworkedDfsServiceEvent::RemoteWriteServed { result, .. } => {
            matches!(result, DfsDistributedWriteResult::Outcome { .. })
        }
        // The primary journals routed mutations and must publish them to
        // replicas. Requester-side completions only replay returned metadata
        // and, for committed writes, install a blob-only content manifest;
        // they never append the requester's metadata-shard journal.
        NetworkedDfsServiceEvent::RemoteWriteCompleted { .. }
        | NetworkedDfsServiceEvent::RemoteNamespaceCompleted { .. }
        | NetworkedDfsServiceEvent::RemoteResponseDiscarded { .. } => false,
        NetworkedDfsServiceEvent::RemoteNamespaceServed {
            result,
            mutates_metadata,
            ..
        } => *mutates_metadata && matches!(result, DfsNamespaceResult::Applied),
        NetworkedDfsServiceEvent::BlobStageRejected { .. }
        | NetworkedDfsServiceEvent::BlobFetched { .. }
        | NetworkedDfsServiceEvent::BlobFetchRejected { .. }
        | NetworkedDfsServiceEvent::RemoteReadServed { .. }
        | NetworkedDfsServiceEvent::RemoteBulkReadServed { .. }
        | NetworkedDfsServiceEvent::RemoteReadCompleted { .. }
        | NetworkedDfsServiceEvent::RemoteBulkReadCompleted { .. }
        | NetworkedDfsServiceEvent::RepairRejected { .. }
        | NetworkedDfsServiceEvent::RepairRetryFailed { .. }
        | NetworkedDfsServiceEvent::MetadataJournalBatchApplied { .. }
        | NetworkedDfsServiceEvent::MetadataJournalBatchRejected { .. }
        | NetworkedDfsServiceEvent::MetadataSnapshotInstalled { .. } => false,
    })
}

/// Returns true only when an accepted replica operation produced evidence that
/// the local metadata replica advanced. Rejected or stale-authority traffic is
/// deliberately excluded so it cannot churn higher-level reactive consumers.
pub fn dfs_events_advance_metadata_replica(events: &[NetworkedDfsServiceEvent]) -> bool {
    events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::MetadataJournalBatchApplied { .. }
                | NetworkedDfsServiceEvent::MetadataSnapshotInstalled { .. }
        )
    })
}

pub fn dfs_metadata_replication_report_needs_retry(
    report: &DfsMetadataJournalReplicationReport,
) -> bool {
    // These outcomes all leave a durable target cursor behind the primary.
    // Keep the exact shard lane dirty until it catches up instead of relying on
    // a process-wide maintenance scan to rediscover the work. Authority and
    // topology outcomes are terminal for this lane generation; their own
    // topology/change events schedule a fresh lane.
    dfs_metadata_publish_retry_class(report) != DfsMetadataPublishRetryClass::Complete
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DfsMetadataPublishRetryClass {
    Complete,
    /// A bounded publish round advanced normally but has more journal pages.
    Progress,
    /// A transport was attempted and failed; retry with exponential backoff.
    Failed,
    /// Current topology proves there is no route; retain the lane but wait for
    /// connectivity/new-input generation (plus one sparse safety reprobe).
    MissingRoute,
}

fn dfs_metadata_publish_retry_class(
    report: &DfsMetadataJournalReplicationReport,
) -> DfsMetadataPublishRetryClass {
    if report
        .outcomes
        .iter()
        .any(|outcome| matches!(outcome, DfsMetadataJournalReplicationOutcome::Failed { .. }))
    {
        return DfsMetadataPublishRetryClass::Failed;
    }
    let backlog = report
        .outcomes
        .iter()
        .filter_map(|outcome| match outcome {
            DfsMetadataJournalReplicationOutcome::BacklogRemaining {
                target_node_id,
                shard_id,
                ..
            } => Some((target_node_id, shard_id)),
            _ => None,
        })
        .collect::<Vec<_>>();
    if !backlog.is_empty() {
        let every_backlog_advanced = backlog.iter().all(|(target_node_id, shard_id)| {
            report.outcomes.iter().any(|outcome| match outcome {
                DfsMetadataJournalReplicationOutcome::Applied {
                    target_node_id: applied_target,
                    shard_id: applied_shard,
                    entries,
                    ..
                } => {
                    applied_target == *target_node_id && applied_shard == *shard_id && *entries > 0
                }
                DfsMetadataJournalReplicationOutcome::SnapshotInstalled {
                    target_node_id: installed_target,
                    shard_id: installed_shard,
                    ..
                } => installed_target == *target_node_id && installed_shard == *shard_id,
                _ => false,
            })
        });
        return if every_backlog_advanced {
            DfsMetadataPublishRetryClass::Progress
        } else {
            // A target budget can expire before any command is sent, and a
            // zero batch budget also reports backlog. Yield-retrying either
            // case would be a control-plane CPU loop, so retain the lane under
            // ordinary failure backoff until new input or its retry deadline.
            DfsMetadataPublishRetryClass::Failed
        };
    }
    if report.outcomes.iter().any(|outcome| {
        matches!(
            outcome,
            DfsMetadataJournalReplicationOutcome::MissingRoute { .. }
        )
    }) {
        return DfsMetadataPublishRetryClass::MissingRoute;
    }
    DfsMetadataPublishRetryClass::Complete
}

pub fn run_dfs_metadata_publish_after_events<P, R, E>(
    events: &[NetworkedDfsServiceEvent],
    mut publish: P,
    mut on_report: R,
    mut on_error: E,
) -> bool
where
    P: FnMut() -> Result<DfsMetadataJournalReplicationReport, String>,
    R: FnMut(DfsMetadataJournalReplicationReport),
    E: FnMut(String),
{
    if !dfs_events_should_publish_metadata(events) {
        return false;
    }
    match publish() {
        Ok(report) => on_report(report),
        Err(error) => on_error(error),
    }
    true
}

pub async fn run_dfs_metadata_publish_after_events_async<P, Fut, R, E>(
    events: &[NetworkedDfsServiceEvent],
    publish: P,
    mut on_report: R,
    mut on_error: E,
) -> bool
where
    P: FnOnce() -> Fut,
    Fut: Future<Output = Result<DfsMetadataJournalReplicationReport, String>>,
    R: FnMut(DfsMetadataJournalReplicationReport),
    E: FnMut(String),
{
    if !dfs_events_should_publish_metadata(events) {
        return false;
    }
    match publish().await {
        Ok(report) => on_report(report),
        Err(error) => on_error(error),
    }
    true
}

pub fn schedule_dfs_metadata_publish_async_on_runtime<P, Fut, R, E>(
    scheduler: Arc<DfsMetadataPublishScheduler>,
    runtime: tokio::runtime::Handle,
    options: DfsMetadataPublishScheduleOptions,
    publish: P,
    on_report: R,
    on_error: E,
) -> DfsMetadataPublishScheduleStart
where
    P: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = Result<DfsMetadataJournalReplicationReport, String>> + Send + 'static,
    R: FnMut(DfsMetadataJournalReplicationReport) + Send + 'static,
    E: FnMut(String) + Send + 'static,
{
    scheduler.request_publish_async_on_runtime(runtime, options, publish, on_report, on_error)
}

pub fn schedule_dfs_metadata_publish_after_events_async_on_runtime<P, Fut, R, E>(
    scheduler: Arc<DfsMetadataPublishScheduler>,
    runtime: tokio::runtime::Handle,
    events: &[NetworkedDfsServiceEvent],
    options: DfsMetadataPublishScheduleOptions,
    publish: P,
    on_report: R,
    on_error: E,
) -> Option<DfsMetadataPublishScheduleStart>
where
    P: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = Result<DfsMetadataJournalReplicationReport, String>> + Send + 'static,
    R: FnMut(DfsMetadataJournalReplicationReport) + Send + 'static,
    E: FnMut(String) + Send + 'static,
{
    dfs_events_should_publish_metadata(events).then(|| {
        schedule_dfs_metadata_publish_async_on_runtime(
            scheduler, runtime, options, publish, on_report, on_error,
        )
    })
}

fn default_dfs_metadata_publish_scheduler() -> Arc<DfsMetadataPublishScheduler> {
    // This compatibility API carries no data-root or shard identity, so it
    // cannot safely coalesce independent callers: the winning task would own
    // one caller's closure and discard the other's. Identity-aware production
    // paths use the keyed cluster/daemon registries; anonymous callers receive
    // a private lane and therefore never lose work through closure aliasing.
    Arc::new(DfsMetadataPublishScheduler::new())
}

fn cluster_dfs_metadata_publish_scheduler(
    data_dir: &SinomoDataDir,
) -> Arc<DfsMetadataPublishScheduler> {
    static SCHEDULERS: OnceLock<Mutex<BTreeMap<PathBuf, Weak<DfsMetadataPublishScheduler>>>> =
        OnceLock::new();
    let key = data_dir.root();
    let Ok(mut schedulers) = SCHEDULERS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
    else {
        // A poisoned registry must not alias an unrelated root. A private lane
        // preserves correctness and merely gives up coalescing for this call.
        return Arc::new(DfsMetadataPublishScheduler::new());
    };
    if let Some(scheduler) = schedulers.get(&key).and_then(Weak::upgrade) {
        return scheduler;
    }
    schedulers.retain(|_, scheduler| scheduler.strong_count() > 0);
    let scheduler = Arc::new(DfsMetadataPublishScheduler::new());
    schedulers.insert(key, Arc::downgrade(&scheduler));
    scheduler
}

pub fn schedule_default_dfs_metadata_publish_async_on_runtime<P, Fut, R, E>(
    runtime: tokio::runtime::Handle,
    options: DfsMetadataPublishScheduleOptions,
    publish: P,
    on_report: R,
    on_error: E,
) -> DfsMetadataPublishScheduleStart
where
    P: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = Result<DfsMetadataJournalReplicationReport, String>> + Send + 'static,
    R: FnMut(DfsMetadataJournalReplicationReport) + Send + 'static,
    E: FnMut(String) + Send + 'static,
{
    schedule_dfs_metadata_publish_async_on_runtime(
        default_dfs_metadata_publish_scheduler(),
        runtime,
        options,
        publish,
        on_report,
        on_error,
    )
}

pub fn schedule_default_dfs_metadata_publish_after_events_async_on_runtime<P, Fut, R, E>(
    runtime: tokio::runtime::Handle,
    events: &[NetworkedDfsServiceEvent],
    options: DfsMetadataPublishScheduleOptions,
    publish: P,
    on_report: R,
    on_error: E,
) -> Option<DfsMetadataPublishScheduleStart>
where
    P: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = Result<DfsMetadataJournalReplicationReport, String>> + Send + 'static,
    R: FnMut(DfsMetadataJournalReplicationReport) + Send + 'static,
    E: FnMut(String) + Send + 'static,
{
    dfs_events_should_publish_metadata(events).then(|| {
        schedule_default_dfs_metadata_publish_async_on_runtime(
            runtime, options, publish, on_report, on_error,
        )
    })
}

pub fn run_cluster_dfs_metadata_publish_after_events<R, E>(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    metadata_sender: DfsMetadataJournalBatchSender,
    snapshot_sender: DfsMetadataSnapshotInstallSender,
    events: &[NetworkedDfsServiceEvent],
    replication_options: DfsMetadataJournalReplicationOptions,
    on_report: R,
    on_error: E,
) -> bool
where
    R: FnMut(DfsMetadataJournalReplicationReport),
    E: FnMut(String),
{
    run_dfs_metadata_publish_after_events(
        events,
        || {
            publish_cluster_dfs_metadata_journal(
                data_dir,
                cluster_paths,
                Arc::clone(&metadata_sender),
                Arc::clone(&snapshot_sender),
                replication_options,
            )
            .map_err(|error| error.to_string())
        },
        on_report,
        on_error,
    )
}

pub fn run_cluster_dfs_metadata_publish_after_events_with_peer_transport<R, E>(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    peer_transport: Arc<dyn DfsPeerTransport>,
    events: &[NetworkedDfsServiceEvent],
    replication_options: DfsMetadataJournalReplicationOptions,
    on_report: R,
    on_error: E,
) -> bool
where
    R: FnMut(DfsMetadataJournalReplicationReport),
    E: FnMut(String),
{
    run_cluster_dfs_metadata_publish_after_events(
        data_dir,
        cluster_paths,
        dfs_metadata_journal_batch_sender_from_peer_transport(Arc::clone(&peer_transport)),
        dfs_metadata_snapshot_install_sender_from_peer_transport(peer_transport),
        events,
        replication_options,
        on_report,
        on_error,
    )
}

pub async fn run_cluster_dfs_metadata_publish_after_events_with_async_peer_transport<R, E>(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    events: &[NetworkedDfsServiceEvent],
    replication_options: DfsMetadataJournalReplicationOptions,
    on_report: R,
    on_error: E,
) -> bool
where
    R: FnMut(DfsMetadataJournalReplicationReport),
    E: FnMut(String),
{
    run_dfs_metadata_publish_after_events_async(
        events,
        || async {
            publish_cluster_dfs_metadata_journal_with_async_peer_transport(
                data_dir,
                cluster_paths,
                peer_transport,
                replication_options,
            )
            .await
            .map_err(|error| error.to_string())
        },
        on_report,
        on_error,
    )
    .await
}

pub fn schedule_cluster_dfs_metadata_publish_async_on_runtime<R, E>(
    runtime: tokio::runtime::Handle,
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    metadata_sender: AsyncDfsMetadataJournalBatchSender,
    snapshot_sender: AsyncDfsMetadataSnapshotInstallSender,
    schedule_options: DfsMetadataPublishScheduleOptions,
    replication_options: DfsMetadataJournalReplicationOptions,
    on_report: R,
    on_error: E,
) -> DfsMetadataPublishScheduleStart
where
    R: FnMut(DfsMetadataJournalReplicationReport) + Send + 'static,
    E: FnMut(String) + Send + 'static,
{
    let scheduler = cluster_dfs_metadata_publish_scheduler(&data_dir);
    schedule_dfs_metadata_publish_async_on_runtime(
        scheduler,
        runtime,
        schedule_options,
        move || {
            let data_dir = data_dir.clone();
            let cluster_paths = cluster_paths.clone();
            let metadata_sender = Arc::clone(&metadata_sender);
            let snapshot_sender = Arc::clone(&snapshot_sender);
            async move {
                publish_cluster_dfs_metadata_journal_async(
                    &data_dir,
                    &cluster_paths,
                    metadata_sender,
                    snapshot_sender,
                    replication_options,
                )
                .await
                .map_err(|error| error.to_string())
            }
        },
        on_report,
        on_error,
    )
}

pub fn schedule_cluster_dfs_metadata_publish_after_events_async_on_runtime<R, E>(
    runtime: tokio::runtime::Handle,
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    metadata_sender: AsyncDfsMetadataJournalBatchSender,
    snapshot_sender: AsyncDfsMetadataSnapshotInstallSender,
    events: &[NetworkedDfsServiceEvent],
    schedule_options: DfsMetadataPublishScheduleOptions,
    replication_options: DfsMetadataJournalReplicationOptions,
    on_report: R,
    on_error: E,
) -> Option<DfsMetadataPublishScheduleStart>
where
    R: FnMut(DfsMetadataJournalReplicationReport) + Send + 'static,
    E: FnMut(String) + Send + 'static,
{
    dfs_events_should_publish_metadata(events).then(|| {
        schedule_cluster_dfs_metadata_publish_async_on_runtime(
            runtime,
            data_dir,
            cluster_paths,
            metadata_sender,
            snapshot_sender,
            schedule_options,
            replication_options,
            on_report,
            on_error,
        )
    })
}

pub fn schedule_cluster_dfs_metadata_publish_with_async_peer_transport_on_runtime<R, E>(
    runtime: tokio::runtime::Handle,
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    schedule_options: DfsMetadataPublishScheduleOptions,
    replication_options: DfsMetadataJournalReplicationOptions,
    on_report: R,
    on_error: E,
) -> DfsMetadataPublishScheduleStart
where
    R: FnMut(DfsMetadataJournalReplicationReport) + Send + 'static,
    E: FnMut(String) + Send + 'static,
{
    schedule_cluster_dfs_metadata_publish_async_on_runtime(
        runtime,
        data_dir,
        cluster_paths,
        async_dfs_metadata_journal_batch_sender_from_peer_transport(Arc::clone(&peer_transport)),
        async_dfs_metadata_snapshot_install_sender_from_peer_transport(peer_transport),
        schedule_options,
        replication_options,
        on_report,
        on_error,
    )
}

pub fn schedule_cluster_dfs_metadata_publish_after_events_with_async_peer_transport_on_runtime<
    R,
    E,
>(
    runtime: tokio::runtime::Handle,
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    events: &[NetworkedDfsServiceEvent],
    schedule_options: DfsMetadataPublishScheduleOptions,
    replication_options: DfsMetadataJournalReplicationOptions,
    on_report: R,
    on_error: E,
) -> Option<DfsMetadataPublishScheduleStart>
where
    R: FnMut(DfsMetadataJournalReplicationReport) + Send + 'static,
    E: FnMut(String) + Send + 'static,
{
    dfs_events_should_publish_metadata(events).then(|| {
        schedule_cluster_dfs_metadata_publish_with_async_peer_transport_on_runtime(
            runtime,
            data_dir,
            cluster_paths,
            peer_transport,
            schedule_options,
            replication_options,
            on_report,
            on_error,
        )
    })
}

pub fn schedule_cluster_dfs_metadata_publish_with_peer_transports_on_runtime<R, E>(
    runtime: tokio::runtime::Handle,
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    sync_peer_transport: Arc<dyn DfsPeerTransport>,
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    schedule_options: DfsMetadataPublishScheduleOptions,
    replication_options: DfsMetadataJournalReplicationOptions,
    on_report: R,
    on_error: E,
) -> DfsMetadataPublishScheduleStart
where
    R: FnMut(DfsMetadataJournalReplicationReport) + Send + 'static,
    E: FnMut(String) + Send + 'static,
{
    let scheduler = cluster_dfs_metadata_publish_scheduler(&data_dir);
    schedule_dfs_metadata_publish_async_on_runtime(
        scheduler,
        runtime,
        schedule_options,
        move || {
            let data_dir = data_dir.clone();
            let cluster_paths = cluster_paths.clone();
            let sync_peer_transport = Arc::clone(&sync_peer_transport);
            let peer_transport = Arc::clone(&peer_transport);
            async move {
                publish_cluster_dfs_metadata_journal_with_peer_transports_async(
                    &data_dir,
                    &cluster_paths,
                    sync_peer_transport,
                    peer_transport,
                    replication_options,
                )
                .await
                .map_err(|error| error.to_string())
            }
        },
        on_report,
        on_error,
    )
}

pub fn schedule_cluster_dfs_metadata_publish_after_events_with_peer_transports_on_runtime<R, E>(
    runtime: tokio::runtime::Handle,
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    sync_peer_transport: Arc<dyn DfsPeerTransport>,
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    events: &[NetworkedDfsServiceEvent],
    schedule_options: DfsMetadataPublishScheduleOptions,
    replication_options: DfsMetadataJournalReplicationOptions,
    on_report: R,
    on_error: E,
) -> Option<DfsMetadataPublishScheduleStart>
where
    R: FnMut(DfsMetadataJournalReplicationReport) + Send + 'static,
    E: FnMut(String) + Send + 'static,
{
    dfs_events_should_publish_metadata(events).then(|| {
        schedule_cluster_dfs_metadata_publish_with_peer_transports_on_runtime(
            runtime,
            data_dir,
            cluster_paths,
            sync_peer_transport,
            peer_transport,
            schedule_options,
            replication_options,
            on_report,
            on_error,
        )
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsMetadataPublishScheduleOptions {
    /// Number of low-latency retries before the retained lane starts backing
    /// off. Zero disables autonomous retries while preserving coalescing.
    pub max_retry_rounds: usize,
    pub retry_delay: Duration,
    /// Upper bound for a retained dirty lane's exponential retry delay.
    pub max_retry_delay: Duration,
}

impl Default for DfsMetadataPublishScheduleOptions {
    fn default() -> Self {
        Self {
            max_retry_rounds: 3,
            retry_delay: Duration::from_millis(750),
            max_retry_delay: Duration::from_secs(5 * 60),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DfsMetadataPublishScheduleStart {
    Started,
    AlreadyRunning,
}

#[derive(Debug)]
pub struct DfsMetadataPublishScheduler {
    pending: AtomicBool,
    running: AtomicBool,
    wake: tokio::sync::Notify,
}

struct DfsMetadataPublishRunGuard {
    scheduler: Arc<DfsMetadataPublishScheduler>,
    owns_running: bool,
}

impl DfsMetadataPublishRunGuard {
    fn new(scheduler: Arc<DfsMetadataPublishScheduler>) -> Self {
        Self {
            scheduler,
            owns_running: true,
        }
    }

    fn release_or_reclaim_pending(&mut self) -> bool {
        debug_assert!(self.owns_running);
        // Disarm before publishing the idle state so this guard cannot clear a
        // concurrent request's newly acquired claim when the loop exits.
        self.owns_running = false;
        self.scheduler.running.store(false, Ordering::Release);
        if self.scheduler.pending.load(Ordering::Acquire)
            && self
                .scheduler
                .running
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            self.owns_running = true;
            true
        } else {
            false
        }
    }
}

impl Drop for DfsMetadataPublishRunGuard {
    fn drop(&mut self) {
        if self.owns_running {
            self.scheduler.running.store(false, Ordering::Release);
        }
    }
}

impl Default for DfsMetadataPublishScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl DfsMetadataPublishScheduler {
    pub const fn new() -> Self {
        Self {
            pending: AtomicBool::new(false),
            running: AtomicBool::new(false),
            wake: tokio::sync::Notify::const_new(),
        }
    }

    fn claim_publish_start(&self) -> DfsMetadataPublishScheduleStart {
        self.pending.store(true, Ordering::Release);
        if self
            .running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            // Wake a retained lane immediately for new journal or
            // connectivity input instead of waiting for its backoff timer.
            self.wake.notify_one();
            DfsMetadataPublishScheduleStart::AlreadyRunning
        } else {
            DfsMetadataPublishScheduleStart::Started
        }
    }

    pub fn request_publish_async_on_runtime<P, Fut, R, E>(
        self: Arc<Self>,
        runtime: tokio::runtime::Handle,
        options: DfsMetadataPublishScheduleOptions,
        publish: P,
        on_report: R,
        on_error: E,
    ) -> DfsMetadataPublishScheduleStart
    where
        P: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = Result<DfsMetadataJournalReplicationReport, String>> + Send + 'static,
        R: FnMut(DfsMetadataJournalReplicationReport) + Send + 'static,
        E: FnMut(String) + Send + 'static,
    {
        let start = self.claim_publish_start();
        if start == DfsMetadataPublishScheduleStart::Started {
            let run_guard = DfsMetadataPublishRunGuard::new(Arc::clone(&self));
            runtime.spawn(async move {
                self.run_publish_loop_async(options, publish, on_report, on_error, run_guard)
                    .await;
            });
        }
        start
    }

    async fn run_publish_loop_async<P, Fut, R, E>(
        self: &Arc<Self>,
        options: DfsMetadataPublishScheduleOptions,
        mut publish: P,
        mut on_report: R,
        mut on_error: E,
        mut run_guard: DfsMetadataPublishRunGuard,
    ) where
        P: FnMut() -> Fut,
        Fut: Future<Output = Result<DfsMetadataJournalReplicationReport, String>>,
        R: FnMut(DfsMetadataJournalReplicationReport),
        E: FnMut(String),
    {
        let mut retry_rounds = 0usize;
        loop {
            self.pending.store(false, Ordering::Release);
            let retry_class = match publish().await {
                Ok(report) => {
                    let retry_class = dfs_metadata_publish_retry_class(&report);
                    on_report(report);
                    retry_class
                }
                Err(error) => {
                    on_error(error);
                    DfsMetadataPublishRetryClass::Failed
                }
            };
            if self.pending.load(Ordering::Acquire) {
                retry_rounds = 0;
                continue;
            }
            match retry_class {
                DfsMetadataPublishRetryClass::Progress if options.max_retry_rounds > 0 => {
                    // Successful bounded progress is not a failure streak. Give
                    // other ready lanes one scheduler turn, then keep draining.
                    retry_rounds = 0;
                    tokio::task::yield_now().await;
                    continue;
                }
                DfsMetadataPublishRetryClass::Failed if options.max_retry_rounds > 0 => {
                    retry_rounds = retry_rounds.saturating_add(1);
                    let delay = dfs_metadata_publish_retry_delay(&options, retry_rounds);
                    self.wait_for_retry_or_input(delay).await;
                    continue;
                }
                DfsMetadataPublishRetryClass::MissingRoute if options.max_retry_rounds > 0 => {
                    retry_rounds = 0;
                    self.wait_for_retry_or_input(options.max_retry_delay).await;
                    continue;
                }
                DfsMetadataPublishRetryClass::Complete
                | DfsMetadataPublishRetryClass::Progress
                | DfsMetadataPublishRetryClass::Failed
                | DfsMetadataPublishRetryClass::MissingRoute => {}
            }
            if run_guard.release_or_reclaim_pending() {
                retry_rounds = 0;
                continue;
            }
            break;
        }
    }

    async fn wait_for_retry_or_input(&self, delay: Duration) {
        if self.pending.load(Ordering::Acquire) {
            return;
        }
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = self.wake.notified() => {}
        }
    }
}

fn dfs_metadata_publish_retry_delay(
    options: &DfsMetadataPublishScheduleOptions,
    retry_round: usize,
) -> Duration {
    if retry_round <= options.max_retry_rounds {
        return options.retry_delay;
    }
    let backoff_round = retry_round.saturating_sub(options.max_retry_rounds).min(31) as u32;
    let multiplier = 1u32 << backoff_round;
    options
        .retry_delay
        .saturating_mul(multiplier)
        .min(options.max_retry_delay.max(options.retry_delay))
}

#[cfg(test)]
mod publish_scheduler_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn failed_publish_report() -> DfsMetadataJournalReplicationReport {
        DfsMetadataJournalReplicationReport {
            outcomes: vec![DfsMetadataJournalReplicationOutcome::Failed {
                target_node_id: NodeId::from_legacy_named("node_metadata_publish_scheduler_failed")
                    .expect("target node"),
                shard_id: DfsMetadataShardId::new("services"),
                reason: "target unavailable".to_string(),
            }],
        }
    }

    fn backlog_publish_report() -> DfsMetadataJournalReplicationReport {
        let target_node_id = NodeId::from_legacy_named("node_metadata_publish_scheduler_backlog")
            .expect("target node");
        let shard_id = DfsMetadataShardId::new("services");
        DfsMetadataJournalReplicationReport {
            outcomes: vec![
                DfsMetadataJournalReplicationOutcome::Applied {
                    target_node_id: target_node_id.clone(),
                    shard_id: shard_id.clone(),
                    entries: 1,
                    next_after_sequence: 7,
                },
                DfsMetadataJournalReplicationOutcome::BacklogRemaining {
                    target_node_id,
                    shard_id,
                    after_sequence: 7,
                    max_batches: 1,
                },
            ],
        }
    }

    fn stalled_backlog_publish_report() -> DfsMetadataJournalReplicationReport {
        DfsMetadataJournalReplicationReport {
            outcomes: vec![DfsMetadataJournalReplicationOutcome::BacklogRemaining {
                target_node_id: NodeId::from_legacy_named(
                    "node_metadata_publish_scheduler_stalled_backlog",
                )
                .expect("target node"),
                shard_id: DfsMetadataShardId::new("services"),
                after_sequence: 7,
                max_batches: 0,
            }],
        }
    }

    #[test]
    fn only_accepted_replica_events_signal_progress() {
        let source = NodeId::from_legacy_named("node_metadata_replica_progress").expect("node");
        let shard_id = DfsMetadataShardId::new("services");
        let rejected = NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
            request_id: "request-rejected".to_string(),
            source_node_id: source.clone(),
            shard_id: shard_id.clone(),
            reason: "stale authority".to_string(),
        };
        assert!(!dfs_events_advance_metadata_replica(&[rejected]));

        let applied = NetworkedDfsServiceEvent::MetadataJournalBatchApplied {
            request_id: "request-applied".to_string(),
            source_node_id: source.clone(),
            shard_id: shard_id.clone(),
            applied_entries: 0,
            next_after_sequence: 7,
            verified_prefix_only: true,
        };
        assert!(dfs_events_advance_metadata_replica(&[applied]));

        let snapshot = NetworkedDfsServiceEvent::MetadataSnapshotInstalled {
            request_id: "request-snapshot".to_string(),
            source_node_id: source,
            shard_id,
            next_after_sequence: 11,
        };
        assert!(dfs_events_advance_metadata_replica(&[snapshot]));
    }

    #[test]
    fn cluster_publish_schedulers_are_isolated_per_data_root() {
        let first_root = sinomo_testdir::TestDir::new("dfs-publish-scheduler-first-root");
        let second_root = sinomo_testdir::TestDir::new("dfs-publish-scheduler-second-root");
        let first_data_dir = SinomoDataDir::new(first_root.join("data"));
        let second_data_dir = SinomoDataDir::new(second_root.join("data"));

        let first = cluster_dfs_metadata_publish_scheduler(&first_data_dir);
        let same = cluster_dfs_metadata_publish_scheduler(&first_data_dir);
        let second = cluster_dfs_metadata_publish_scheduler(&second_data_dir);

        assert!(Arc::ptr_eq(&first, &same));
        assert!(
            !Arc::ptr_eq(&first, &second),
            "one root's active closure must never discard another root's publish"
        );
    }

    #[test]
    fn anonymous_default_publish_callers_never_alias_closures() {
        let first = default_dfs_metadata_publish_scheduler();
        let second = default_dfs_metadata_publish_scheduler();
        assert!(
            !Arc::ptr_eq(&first, &second),
            "an identity-free compatibility API cannot safely coalesce independent closures"
        );
    }

    #[test]
    fn retry_classifier_retains_durable_target_failures() {
        assert_eq!(
            dfs_metadata_publish_retry_class(&backlog_publish_report()),
            DfsMetadataPublishRetryClass::Progress
        );
        assert_eq!(
            dfs_metadata_publish_retry_class(&failed_publish_report()),
            DfsMetadataPublishRetryClass::Failed
        );
        assert_eq!(
            dfs_metadata_publish_retry_class(&stalled_backlog_publish_report()),
            DfsMetadataPublishRetryClass::Failed,
            "backlog without a proven cursor advance must not yield-spin"
        );
        assert!(dfs_metadata_replication_report_needs_retry(
            &failed_publish_report()
        ));
        let missing_route = DfsMetadataJournalReplicationReport {
            outcomes: vec![DfsMetadataJournalReplicationOutcome::MissingRoute {
                target_node_id: NodeId::from_legacy_named(
                    "node_metadata_publish_scheduler_missing",
                )
                .expect("target node"),
                shard_id: DfsMetadataShardId::new("services"),
                after_sequence: 7,
            }],
        };
        assert_eq!(
            dfs_metadata_publish_retry_class(&missing_route),
            DfsMetadataPublishRetryClass::MissingRoute
        );
        assert!(dfs_metadata_replication_report_needs_retry(&missing_route));
        assert!(dfs_metadata_replication_report_needs_retry(
            &backlog_publish_report()
        ));
        let mut mixed = failed_publish_report();
        mixed.outcomes.extend(backlog_publish_report().outcomes);
        assert_eq!(
            dfs_metadata_publish_retry_class(&mixed),
            DfsMetadataPublishRetryClass::Failed,
            "a progressing target must not mask another target's failure"
        );
        assert!(
            dfs_metadata_replication_report_needs_retry(&mixed),
            "any durable lag keeps the exact shard lane active"
        );
    }

    #[test]
    fn failed_target_report_remains_scheduled_until_clean() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let scheduler = Arc::new(DfsMetadataPublishScheduler::new());
        scheduler.running.store(true, Ordering::Release);
        let calls = Arc::new(AtomicUsize::new(0));
        let publish_calls = Arc::clone(&calls);

        runtime.block_on(scheduler.run_publish_loop_async(
            DfsMetadataPublishScheduleOptions {
                max_retry_rounds: 3,
                retry_delay: Duration::ZERO,
                max_retry_delay: Duration::ZERO,
            },
            move || {
                let call = publish_calls.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Ok(if call < 4 {
                    failed_publish_report()
                } else {
                    DfsMetadataJournalReplicationReport {
                        outcomes: Vec::new(),
                    }
                }))
            },
            |_| {},
            |_| {},
            DfsMetadataPublishRunGuard::new(Arc::clone(&scheduler)),
        ));

        assert_eq!(calls.load(Ordering::SeqCst), 5);
        assert!(!scheduler.running.load(Ordering::Acquire));
    }

    #[test]
    fn backlog_report_remains_scheduled_until_clean() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let scheduler = Arc::new(DfsMetadataPublishScheduler::new());
        scheduler.running.store(true, Ordering::Release);
        let calls = Arc::new(AtomicUsize::new(0));
        let publish_calls = Arc::clone(&calls);

        runtime.block_on(scheduler.run_publish_loop_async(
            DfsMetadataPublishScheduleOptions {
                max_retry_rounds: 3,
                retry_delay: Duration::ZERO,
                max_retry_delay: Duration::ZERO,
            },
            move || {
                let call = publish_calls.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Ok(if call < 4 {
                    backlog_publish_report()
                } else {
                    DfsMetadataJournalReplicationReport {
                        outcomes: Vec::new(),
                    }
                }))
            },
            |_| {},
            |_| {},
            DfsMetadataPublishRunGuard::new(Arc::clone(&scheduler)),
        ));

        assert_eq!(calls.load(Ordering::SeqCst), 5);
        assert!(!scheduler.running.load(Ordering::Acquire));
    }

    #[test]
    fn stalled_backlog_waits_instead_of_spinning() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let scheduler = Arc::new(DfsMetadataPublishScheduler::new());
        scheduler.running.store(true, Ordering::Release);
        let calls = Arc::new(AtomicUsize::new(0));
        let publish_calls = Arc::clone(&calls);

        runtime.block_on(async {
            let result = tokio::time::timeout(
                Duration::from_millis(50),
                scheduler.run_publish_loop_async(
                    DfsMetadataPublishScheduleOptions {
                        max_retry_rounds: 3,
                        retry_delay: Duration::from_secs(60 * 60),
                        max_retry_delay: Duration::from_secs(60 * 60),
                    },
                    move || {
                        publish_calls.fetch_add(1, Ordering::SeqCst);
                        std::future::ready(Ok(stalled_backlog_publish_report()))
                    },
                    |_| {},
                    |_| {},
                    DfsMetadataPublishRunGuard::new(Arc::clone(&scheduler)),
                ),
            )
            .await;
            assert!(result.is_err(), "the retained lane should be sleeping");
        });

        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "no-progress backlog must not consume a worker in a yield loop"
        );
        assert!(!scheduler.running.load(Ordering::Acquire));
    }

    #[test]
    fn healthy_backlog_progress_never_enters_failure_backoff() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let scheduler = Arc::new(DfsMetadataPublishScheduler::new());
        scheduler.running.store(true, Ordering::Release);
        let calls = Arc::new(AtomicUsize::new(0));
        let publish_calls = Arc::clone(&calls);

        runtime.block_on(async {
            tokio::time::timeout(
                Duration::from_secs(1),
                scheduler.run_publish_loop_async(
                    DfsMetadataPublishScheduleOptions {
                        max_retry_rounds: 3,
                        retry_delay: Duration::from_secs(60 * 60),
                        max_retry_delay: Duration::from_secs(60 * 60),
                    },
                    move || {
                        let call = publish_calls.fetch_add(1, Ordering::SeqCst);
                        std::future::ready(Ok(if call < 128 {
                            backlog_publish_report()
                        } else {
                            DfsMetadataJournalReplicationReport {
                                outcomes: Vec::new(),
                            }
                        }))
                    },
                    |_| {},
                    |_| {},
                    DfsMetadataPublishRunGuard::new(Arc::clone(&scheduler)),
                ),
            )
            .await
            .expect("healthy progress must not sleep on failure backoff");
        });

        assert_eq!(calls.load(Ordering::SeqCst), 129);
        assert!(!scheduler.running.load(Ordering::Acquire));
    }

    #[test]
    fn retained_retry_delay_backs_off_to_a_hard_cap() {
        let options = DfsMetadataPublishScheduleOptions {
            max_retry_rounds: 2,
            retry_delay: Duration::from_millis(10),
            max_retry_delay: Duration::from_millis(50),
        };

        assert_eq!(
            dfs_metadata_publish_retry_delay(&options, 1),
            Duration::from_millis(10)
        );
        assert_eq!(
            dfs_metadata_publish_retry_delay(&options, 2),
            Duration::from_millis(10)
        );
        assert_eq!(
            dfs_metadata_publish_retry_delay(&options, 3),
            Duration::from_millis(20)
        );
        assert_eq!(
            dfs_metadata_publish_retry_delay(&options, 4),
            Duration::from_millis(40)
        );
        assert_eq!(
            dfs_metadata_publish_retry_delay(&options, 5),
            Duration::from_millis(50)
        );
        assert_eq!(
            dfs_metadata_publish_retry_delay(&options, usize::MAX),
            Duration::from_millis(50)
        );
    }

    #[test]
    fn pending_request_during_failure_runs_one_fresh_publish() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let scheduler = Arc::new(DfsMetadataPublishScheduler::new());
        scheduler.running.store(true, Ordering::Release);
        let calls = Arc::new(AtomicUsize::new(0));
        let publish_calls = Arc::clone(&calls);
        let request_scheduler = Arc::clone(&scheduler);

        runtime.block_on(scheduler.run_publish_loop_async(
            DfsMetadataPublishScheduleOptions {
                max_retry_rounds: 3,
                retry_delay: Duration::ZERO,
                max_retry_delay: Duration::ZERO,
            },
            move || {
                let call = publish_calls.fetch_add(1, Ordering::SeqCst);
                if call == 0 {
                    assert_eq!(
                        request_scheduler.claim_publish_start(),
                        DfsMetadataPublishScheduleStart::AlreadyRunning
                    );
                }
                std::future::ready(Ok(if call == 0 {
                    failed_publish_report()
                } else {
                    DfsMetadataJournalReplicationReport {
                        outcomes: Vec::new(),
                    }
                }))
            },
            |_| {},
            |_| {},
            DfsMetadataPublishRunGuard::new(Arc::clone(&scheduler)),
        ));

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(!scheduler.running.load(Ordering::Acquire));
    }

    #[test]
    fn unconditional_publish_runs_and_coalesces_pending_requests() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let scheduler = Arc::new(DfsMetadataPublishScheduler::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let discarded_calls = Arc::new(AtomicUsize::new(0));
        let first_started = Arc::new(tokio::sync::Notify::new());
        let release_first = Arc::new(tokio::sync::Notify::new());
        let (report_sender, report_receiver) = std::sync::mpsc::channel();

        let publish_calls = Arc::clone(&calls);
        let publish_started = Arc::clone(&first_started);
        let publish_release = Arc::clone(&release_first);
        let start = schedule_dfs_metadata_publish_async_on_runtime(
            Arc::clone(&scheduler),
            runtime.handle().clone(),
            DfsMetadataPublishScheduleOptions {
                max_retry_rounds: 0,
                retry_delay: Duration::ZERO,
                max_retry_delay: Duration::ZERO,
            },
            move || {
                let call = publish_calls.fetch_add(1, Ordering::SeqCst);
                let publish_started = Arc::clone(&publish_started);
                let publish_release = Arc::clone(&publish_release);
                async move {
                    publish_started.notify_one();
                    if call == 0 {
                        publish_release.notified().await;
                    }
                    Ok(DfsMetadataJournalReplicationReport {
                        outcomes: Vec::new(),
                    })
                }
            },
            move |report| {
                report_sender.send(report).expect("report receiver");
            },
            |_| {},
        );
        assert_eq!(start, DfsMetadataPublishScheduleStart::Started);
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(1), first_started.notified())
                .await
                .expect("first publish started");
        });

        let second_publish_calls = Arc::clone(&discarded_calls);
        let second_start = schedule_dfs_metadata_publish_async_on_runtime(
            Arc::clone(&scheduler),
            runtime.handle().clone(),
            DfsMetadataPublishScheduleOptions::default(),
            move || {
                second_publish_calls.fetch_add(1, Ordering::SeqCst);
                async {
                    Ok(DfsMetadataJournalReplicationReport {
                        outcomes: Vec::new(),
                    })
                }
            },
            |_| {},
            |_| {},
        );
        assert_eq!(
            second_start,
            DfsMetadataPublishScheduleStart::AlreadyRunning
        );

        release_first.notify_one();
        report_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("first report");
        report_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("coalesced report");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(discarded_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn cancelled_publish_task_releases_running_claim() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("runtime");
        let scheduler = Arc::new(DfsMetadataPublishScheduler::new());
        let (started_sender, started_receiver) = std::sync::mpsc::channel();

        let start = schedule_dfs_metadata_publish_async_on_runtime(
            Arc::clone(&scheduler),
            runtime.handle().clone(),
            DfsMetadataPublishScheduleOptions {
                max_retry_rounds: 0,
                retry_delay: Duration::ZERO,
                max_retry_delay: Duration::ZERO,
            },
            move || {
                let started_sender = started_sender.clone();
                async move {
                    started_sender.send(()).expect("start receiver");
                    std::future::pending::<Result<DfsMetadataJournalReplicationReport, String>>()
                        .await
                }
            },
            |_| {},
            |_| {},
        );
        assert_eq!(start, DfsMetadataPublishScheduleStart::Started);
        started_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("publish task started");

        drop(runtime);
        assert!(!scheduler.running.load(Ordering::Acquire));

        let recovery_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("recovery runtime");
        let (report_sender, report_receiver) = std::sync::mpsc::channel();
        let recovery_start = schedule_dfs_metadata_publish_async_on_runtime(
            Arc::clone(&scheduler),
            recovery_runtime.handle().clone(),
            DfsMetadataPublishScheduleOptions {
                max_retry_rounds: 0,
                retry_delay: Duration::ZERO,
                max_retry_delay: Duration::ZERO,
            },
            || async {
                Ok(DfsMetadataJournalReplicationReport {
                    outcomes: Vec::new(),
                })
            },
            move |report| report_sender.send(report).expect("report receiver"),
            |_| {},
        );
        assert_eq!(recovery_start, DfsMetadataPublishScheduleStart::Started);
        recovery_runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    if let Ok(report) = report_receiver.try_recv() {
                        break report;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("recovery publish report");
        });
    }

    #[test]
    fn event_gated_scheduler_skips_non_publishing_events() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let scheduler = Arc::new(DfsMetadataPublishScheduler::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let publish_calls = Arc::clone(&calls);

        let start = schedule_dfs_metadata_publish_after_events_async_on_runtime(
            scheduler,
            runtime.handle().clone(),
            &[],
            DfsMetadataPublishScheduleOptions::default(),
            move || {
                publish_calls.fetch_add(1, Ordering::SeqCst);
                async {
                    Ok(DfsMetadataJournalReplicationReport {
                        outcomes: Vec::new(),
                    })
                }
            },
            |_| {},
            |_| {},
        );

        assert_eq!(start, None);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DfsMetadataJournalReplicationTransportError {
    MissingRoute,
    Failed { reason: String },
}

pub trait DfsMetadataJournalReplicationTransport: Send {
    /// Returns an independent transport handle for one target-node lane.
    /// Implementations without a cheap, thread-safe fork retain serial
    /// execution through the default `None` result.
    fn fork_target_lane(&self) -> Option<Box<dyn DfsMetadataJournalReplicationTransport>> {
        None
    }

    fn has_metadata_replication_route(&self, target_node_id: &NodeId) -> bool;

    fn send_metadata_journal_batch(
        &mut self,
        target_node_id: &NodeId,
        command: &DfsMetadataJournalBatchCommand,
        timeout: Duration,
    ) -> Result<DfsMetadataJournalBatchResponse, DfsMetadataJournalReplicationTransportError>;

    fn send_metadata_snapshot_install(
        &mut self,
        target_node_id: &NodeId,
        command: &DfsMetadataSnapshotInstallCommand,
        timeout: Duration,
    ) -> Result<DfsMetadataSnapshotInstallResponse, DfsMetadataJournalReplicationTransportError>;
}

pub type DfsMetadataJournalReplicationTransportFuture<'a, T> =
    Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait AsyncDfsMetadataJournalReplicationTransport: Send + Sync {
    fn has_metadata_replication_route(&self, target_node_id: &NodeId) -> bool;

    fn send_metadata_journal_batch_async<'a>(
        &'a self,
        target_node_id: &'a NodeId,
        command: &'a DfsMetadataJournalBatchCommand,
        timeout: Duration,
    ) -> DfsMetadataJournalReplicationTransportFuture<
        'a,
        Result<DfsMetadataJournalBatchResponse, DfsMetadataJournalReplicationTransportError>,
    >;

    fn send_metadata_snapshot_install_async<'a>(
        &'a self,
        target_node_id: &'a NodeId,
        command: &'a DfsMetadataSnapshotInstallCommand,
        timeout: Duration,
    ) -> DfsMetadataJournalReplicationTransportFuture<
        'a,
        Result<DfsMetadataSnapshotInstallResponse, DfsMetadataJournalReplicationTransportError>,
    >;
}

pub const DFS_METADATA_REPLICATION_MAX_NODE_ROUTES_PER_TARGET: usize = 3;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsMetadataPeerTransportError {
    detail: String,
}

impl DfsMetadataPeerTransportError {
    pub fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }
}

impl std::fmt::Display for DfsMetadataPeerTransportError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl std::error::Error for DfsMetadataPeerTransportError {}

pub type DfsMetadataJournalBatchSender = Arc<
    dyn Fn(
            &NodeId,
            &DfsMetadataJournalBatchCommand,
            Duration,
        ) -> Result<DfsMetadataJournalBatchResponse, DfsMetadataPeerTransportError>
        + Send
        + Sync,
>;

pub type DfsMetadataSnapshotInstallSender = Arc<
    dyn Fn(
            &NodeId,
            &DfsMetadataSnapshotInstallCommand,
            Duration,
        ) -> Result<DfsMetadataSnapshotInstallResponse, DfsMetadataPeerTransportError>
        + Send
        + Sync,
>;

#[derive(Clone)]
pub struct DfsMetadataNodeTransport {
    peer_node_ids: BTreeSet<NodeId>,
    sender: DfsMetadataJournalBatchSender,
    snapshot_sender: DfsMetadataSnapshotInstallSender,
}

impl std::fmt::Debug for DfsMetadataNodeTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DfsMetadataNodeTransport")
            .field("peer_node_ids", &self.peer_node_ids)
            .finish_non_exhaustive()
    }
}

impl DfsMetadataNodeTransport {
    pub fn new(
        peer_node_ids: impl IntoIterator<Item = NodeId>,
        sender: DfsMetadataJournalBatchSender,
        snapshot_sender: DfsMetadataSnapshotInstallSender,
    ) -> Self {
        Self {
            peer_node_ids: peer_node_ids.into_iter().collect(),
            sender,
            snapshot_sender,
        }
    }

    fn has_peer_node(&self, target_node_id: &NodeId) -> bool {
        self.peer_node_ids.contains(target_node_id)
    }
}

fn validate_dfs_metadata_journal_batch_response(
    target_node_id: &NodeId,
    command: &DfsMetadataJournalBatchCommand,
    response: &DfsMetadataJournalBatchResponse,
) -> Result<(), String> {
    if response.request_id != command.request_id
        || response.shard_id != command.batch.shard_id
        || response.next_after_sequence != command.batch.next_after_sequence
    {
        return Err(format!(
            "{target_node_id} returned mismatched metadata journal response"
        ));
    }
    Ok(())
}

fn validate_dfs_metadata_journal_replication_response(
    command: &DfsMetadataJournalBatchCommand,
    shard_id: &DfsMetadataShardId,
    response: &DfsMetadataJournalBatchResponse,
) -> Result<(), String> {
    if response.request_id != command.request_id
        || response.shard_id != *shard_id
        || response.next_after_sequence != command.batch.next_after_sequence
    {
        return Err("metadata journal replication returned mismatched response".to_string());
    }
    Ok(())
}

fn validate_dfs_metadata_snapshot_install_response(
    target_node_id: &NodeId,
    command: &DfsMetadataSnapshotInstallCommand,
    response: &DfsMetadataSnapshotInstallResponse,
) -> Result<(), String> {
    if response.request_id != command.request_id
        || response.shard_id != command.shard_id
        || response.next_after_sequence != command.next_after_sequence
    {
        return Err(format!(
            "{target_node_id} returned mismatched metadata snapshot install response"
        ));
    }
    Ok(())
}

fn validate_dfs_metadata_snapshot_install_replication_response(
    command: &DfsMetadataSnapshotInstallCommand,
    response: &DfsMetadataSnapshotInstallResponse,
) -> Result<(), String> {
    if response.request_id != command.request_id
        || response.shard_id != command.shard_id
        || response.next_after_sequence != command.next_after_sequence
    {
        return Err("metadata snapshot install returned mismatched response".to_string());
    }
    Ok(())
}

impl DfsMetadataJournalReplicationTransport for DfsMetadataNodeTransport {
    fn fork_target_lane(&self) -> Option<Box<dyn DfsMetadataJournalReplicationTransport>> {
        Some(Box::new(self.clone()))
    }

    fn has_metadata_replication_route(&self, target_node_id: &NodeId) -> bool {
        self.has_peer_node(target_node_id)
    }

    fn send_metadata_journal_batch(
        &mut self,
        target_node_id: &NodeId,
        command: &DfsMetadataJournalBatchCommand,
        timeout: Duration,
    ) -> Result<DfsMetadataJournalBatchResponse, DfsMetadataJournalReplicationTransportError> {
        if !self.has_peer_node(target_node_id) {
            return Err(DfsMetadataJournalReplicationTransportError::MissingRoute);
        }
        match (self.sender)(target_node_id, command, timeout) {
            Ok(response) => {
                validate_dfs_metadata_journal_batch_response(target_node_id, command, &response)
                    .map_err(
                        |reason| DfsMetadataJournalReplicationTransportError::Failed { reason },
                    )?;
                Ok(response)
            }
            Err(error) => Err(DfsMetadataJournalReplicationTransportError::Failed {
                reason: error.to_string(),
            }),
        }
    }

    fn send_metadata_snapshot_install(
        &mut self,
        target_node_id: &NodeId,
        command: &DfsMetadataSnapshotInstallCommand,
        timeout: Duration,
    ) -> Result<DfsMetadataSnapshotInstallResponse, DfsMetadataJournalReplicationTransportError>
    {
        if !self.has_peer_node(target_node_id) {
            return Err(DfsMetadataJournalReplicationTransportError::MissingRoute);
        }
        match (self.snapshot_sender)(target_node_id, command, timeout) {
            Ok(response) => {
                validate_dfs_metadata_snapshot_install_response(target_node_id, command, &response)
                    .map_err(
                        |reason| DfsMetadataJournalReplicationTransportError::Failed { reason },
                    )?;
                Ok(response)
            }
            Err(error) => Err(DfsMetadataJournalReplicationTransportError::Failed {
                reason: error.to_string(),
            }),
        }
    }
}

#[derive(Clone)]
pub struct AsyncDfsMetadataNodeTransport {
    peer_node_ids: BTreeSet<NodeId>,
    sender: AsyncDfsMetadataJournalBatchSender,
    snapshot_sender: AsyncDfsMetadataSnapshotInstallSender,
}

impl std::fmt::Debug for AsyncDfsMetadataNodeTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AsyncDfsMetadataNodeTransport")
            .field("peer_node_ids", &self.peer_node_ids)
            .finish_non_exhaustive()
    }
}

impl AsyncDfsMetadataNodeTransport {
    pub fn new(
        peer_node_ids: impl IntoIterator<Item = NodeId>,
        sender: AsyncDfsMetadataJournalBatchSender,
        snapshot_sender: AsyncDfsMetadataSnapshotInstallSender,
    ) -> Self {
        Self {
            peer_node_ids: peer_node_ids.into_iter().collect(),
            sender,
            snapshot_sender,
        }
    }

    fn has_peer_node(&self, target_node_id: &NodeId) -> bool {
        self.peer_node_ids.contains(target_node_id)
    }
}

impl AsyncDfsMetadataJournalReplicationTransport for AsyncDfsMetadataNodeTransport {
    fn has_metadata_replication_route(&self, target_node_id: &NodeId) -> bool {
        self.has_peer_node(target_node_id)
    }

    fn send_metadata_journal_batch_async<'a>(
        &'a self,
        target_node_id: &'a NodeId,
        command: &'a DfsMetadataJournalBatchCommand,
        timeout: Duration,
    ) -> DfsMetadataJournalReplicationTransportFuture<
        'a,
        Result<DfsMetadataJournalBatchResponse, DfsMetadataJournalReplicationTransportError>,
    > {
        Box::pin(async move {
            if !self.has_peer_node(target_node_id) {
                return Err(DfsMetadataJournalReplicationTransportError::MissingRoute);
            }
            match (self.sender)(target_node_id, command, timeout).await {
                Ok(response) => {
                    validate_dfs_metadata_journal_batch_response(
                        target_node_id,
                        command,
                        &response,
                    )
                    .map_err(|reason| {
                        DfsMetadataJournalReplicationTransportError::Failed { reason }
                    })?;
                    Ok(response)
                }
                Err(error) => Err(DfsMetadataJournalReplicationTransportError::Failed {
                    reason: error.to_string(),
                }),
            }
        })
    }

    fn send_metadata_snapshot_install_async<'a>(
        &'a self,
        target_node_id: &'a NodeId,
        command: &'a DfsMetadataSnapshotInstallCommand,
        timeout: Duration,
    ) -> DfsMetadataJournalReplicationTransportFuture<
        'a,
        Result<DfsMetadataSnapshotInstallResponse, DfsMetadataJournalReplicationTransportError>,
    > {
        Box::pin(async move {
            if !self.has_peer_node(target_node_id) {
                return Err(DfsMetadataJournalReplicationTransportError::MissingRoute);
            }
            match (self.snapshot_sender)(target_node_id, command, timeout).await {
                Ok(response) => {
                    validate_dfs_metadata_snapshot_install_response(
                        target_node_id,
                        command,
                        &response,
                    )
                    .map_err(|reason| {
                        DfsMetadataJournalReplicationTransportError::Failed { reason }
                    })?;
                    Ok(response)
                }
                Err(error) => Err(DfsMetadataJournalReplicationTransportError::Failed {
                    reason: error.to_string(),
                }),
            }
        })
    }
}

pub trait DfsMetadataJournalPublisherHost: Send {
    fn local_data_dir(&self) -> Option<&SinomoDataDir> {
        None
    }

    fn open_primary_shard_backend(
        &mut self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Box<dyn DfsMetadataBackend>, DfsError>;

    fn metadata_replication_cursor(
        &mut self,
        backend: &mut dyn DfsMetadataBackend,
        target_node_id: &NodeId,
        shard_id: &DfsMetadataShardId,
    ) -> Result<u64, DfsError> {
        Ok(backend.metadata_replication_cursor(target_node_id, shard_id)?)
    }
}

#[derive(Clone, Debug)]
pub struct LocalDfsMetadataJournalPublisherHost {
    data_dir: SinomoDataDir,
}

impl LocalDfsMetadataJournalPublisherHost {
    pub fn new(data_dir: SinomoDataDir) -> Self {
        Self { data_dir }
    }
}

impl DfsMetadataJournalPublisherHost for LocalDfsMetadataJournalPublisherHost {
    fn local_data_dir(&self) -> Option<&SinomoDataDir> {
        Some(&self.data_dir)
    }

    fn open_primary_shard_backend(
        &mut self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Box<dyn DfsMetadataBackend>, DfsError> {
        Ok(Box::new(open_local_metadata_shard_backend(
            &self.data_dir,
            shard_id.clone(),
        )?))
    }

    fn metadata_replication_cursor(
        &mut self,
        backend: &mut dyn DfsMetadataBackend,
        target_node_id: &NodeId,
        shard_id: &DfsMetadataShardId,
    ) -> Result<u64, DfsError> {
        migrate_legacy_dfs_metadata_replication_cursor(
            backend,
            &self.data_dir,
            target_node_id,
            shard_id,
        )
    }
}

#[derive(Clone, Debug)]
struct DfsPreparedClusterMetadataJournalPublish {
    data_dir: SinomoDataDir,
    local_node_id: NodeId,
    peer_node_ids: Vec<NodeId>,
    outcomes: Vec<DfsMetadataJournalReplicationOutcome>,
    targets: Vec<DfsPreparedMetadataJournalTargetPublish>,
}

#[derive(Clone, Debug)]
pub(crate) struct DfsPreparedMetadataJournalTargetPublish {
    pub(crate) target_node_id: NodeId,
    pub(crate) file_system_id: DfsFileSystemId,
    pub(crate) shard_id: DfsMetadataShardId,
    pub(crate) shard_epoch: u64,
    pub(crate) after_sequence: u64,
    pub(crate) commands: Vec<DfsMetadataJournalBatchCommand>,
    pub(crate) backlog_after_sequence: Option<u64>,
    pub(crate) caught_up_probe: bool,
}

fn validate_prepared_caught_up_probe(
    target: &DfsPreparedMetadataJournalTargetPublish,
) -> Result<(), DfsError> {
    if !target.caught_up_probe {
        return Ok(());
    }
    let valid = target.commands.len() == 1 && {
        let command = &target.commands[0];
        command.batch.entries.is_empty()
            && command.batch.shard_id == target.shard_id
            && command.batch.next_after_sequence == target.after_sequence
            && command.verify_prefix_only
            && command.prefix_probe.as_ref().is_some_and(|probe| {
                probe.file_system_id.as_ref() == Some(&target.file_system_id)
                    && probe.authority_epoch == Some(target.shard_epoch)
                    && probe.shard_id == target.shard_id
                    && probe.journal_sequence == target.after_sequence
                    && !probe.barrier_id.is_empty()
                    && !probe.state_fingerprint.is_empty()
                    && probe.primary_handoff_id.is_none()
            })
            && command.authority_probe.is_none()
    };
    if valid {
        Ok(())
    } else {
        Err(DfsError::Metadata {
            message: format!(
                "caught-up metadata probe for shard {} must contain exactly one empty exact-prefix batch at sequence {}",
                target.shard_id.0, target.after_sequence
            ),
        })
    }
}

fn prepare_cluster_dfs_metadata_journal_publish(
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    execution_exclusions: &DfsMaintenanceExecutionExclusions,
    only_lane: Option<(DfsFileSystemId, DfsMetadataShardId)>,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<Option<DfsPreparedClusterMetadataJournalPublish>, DfsError> {
    if !cluster_paths.state.exists() && only_lane.is_none() {
        return Ok(None);
    }
    if !cluster_paths.state.exists() {
        return Err(DfsError::Metadata {
            message: "exact DFS metadata publish lacks cluster state".to_string(),
        });
    }

    let state = read_cluster_dfs_metadata_publish_state(&cluster_paths)?;
    let file_system_id = only_lane
        .as_ref()
        .map(|(file_system_id, _)| file_system_id.clone())
        .unwrap_or_else(|| distributed_sifs(state.local_node_id.clone()).file_system_id);
    let mut shards = cluster_dfs_metadata_shards(&cluster_paths, &file_system_id)?;
    if let Some((_, only_shard_id)) = only_lane.as_ref() {
        shards.retain(|shard| shard.shard_id == only_shard_id.0);
    }
    let peer_node_ids = cluster_dfs_peer_node_ids(&state);
    let metadata_suppressions = read_dfs_metadata_replication_suppression_snapshot(
        &default_dfs_metadata_replication_suppression_snapshot_path(&data_dir),
    )?
    .map(|snapshot| snapshot.records)
    .unwrap_or_default();
    let (outcomes, targets) =
        prepare_dfs_metadata_journal_publish_with_cached_backends_and_execution_exclusions(
            &data_dir,
            file_system_id,
            &state.local_node_id,
            shards,
            &metadata_suppressions,
            execution_exclusions,
            options,
        )?;
    Ok(Some(DfsPreparedClusterMetadataJournalPublish {
        data_dir,
        local_node_id: state.local_node_id,
        peer_node_ids,
        outcomes,
        targets,
    }))
}

#[cfg(test)]
mod metadata_publish_membership_tests {
    use super::*;

    #[test]
    fn metadata_publish_peers_use_latest_observed_cluster_membership() {
        let root = sinomo_testdir::TestDir::new("dfs-metadata-publish-latest-membership");
        let data_dir = SinomoDataDir::new(root.path());
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id = sinomo_core::node::ClusterId::from_legacy_named(
            "cluster_dfs_metadata_publish_latest_membership",
        )
        .expect("cluster id");
        let local_node_id =
            NodeId::from_legacy_named("node_dfs_metadata_publish_local").expect("local node");
        let joined_node_id =
            NodeId::from_legacy_named("node_dfs_metadata_publish_joined").expect("joined node");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "metadata publish latest membership",
            local_node_id.clone(),
            "local",
            "pk-local",
            now,
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("initial cluster KV snapshot");

        let stale_bootstrap = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
            .expect("bootstrap-only state");
        assert_eq!(
            cluster_dfs_peer_node_ids(&stale_bootstrap),
            vec![local_node_id.clone()],
            "the regression requires bootstrap membership to trail KV membership"
        );

        let mut joined_member = sinomo_core::ClusterMemberRecord::pending_learner(
            cluster_id,
            joined_node_id.clone(),
            "joined",
            "pk-joined",
            now,
        );
        joined_member.state = sinomo_core::ClusterMemberState::Learner;
        let mut accepted = sinomo_core::kv::InProcessKvStore::restore(
            sinomo_core::kv::read_cluster_kv_cached_snapshot(&cluster_paths.kv_paths())
                .expect("cached cluster KV snapshot"),
        )
        .expect("accepted KV store");
        accepted
            .put_serde(
                sinomo_core::node::cluster_member_key(&joined_node_id),
                &joined_member,
                None,
            )
            .expect("joined member");
        let accepted = accepted.try_snapshot().expect("accepted KV snapshot");
        sinomo_core::kv::write_imported_cluster_kv_snapshot_if_changed(
            &cluster_paths.kv_paths(),
            &accepted,
            |_| false,
        )
        .expect("import accepted KV snapshot");

        let publish_state = read_cluster_dfs_metadata_publish_state(&cluster_paths)
            .expect("metadata publish state");
        assert_eq!(
            cluster_dfs_peer_node_ids(&publish_state)
                .into_iter()
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([local_node_id, joined_node_id]),
            "metadata publishing must authorize peers from the latest accepted membership"
        );
    }

    #[test]
    fn metadata_replica_ingress_uses_latest_observed_shard_authority() {
        let root = sinomo_testdir::TestDir::new("dfs-metadata-ingress-latest-authority");
        let data_dir = SinomoDataDir::new(root.path());
        data_dir.ensure_root_layout().expect("data root layout");
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id = sinomo_core::node::ClusterId::from_legacy_named(
            "cluster_dfs_metadata_ingress_latest_authority",
        )
        .expect("cluster id");
        let receiver_node_id =
            NodeId::from_legacy_named("node_dfs_metadata_ingress_receiver").expect("receiver");
        let source_node_id =
            NodeId::from_legacy_named("node_dfs_metadata_ingress_source").expect("source");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "metadata ingress latest authority",
            receiver_node_id.clone(),
            "receiver",
            "pk-receiver",
            now,
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");
        let initial_snapshot =
            sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
                .expect("initial cluster KV snapshot");

        let file_system_id = sifs_file_system_id();
        let shard_key = metadata_shard_key(&file_system_id, "root");
        let current_shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: source_node_id.clone(),
            replica_node_ids: vec![receiver_node_id.clone()],
            epoch: 9,
            assignment: crate::DfsMetadataShardAssignment::Root,
            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        };
        let mut accepted = sinomo_core::kv::InProcessKvStore::restore(initial_snapshot)
            .expect("accepted KV store");
        accepted
            .put_serde(&shard_key, &current_shard, None)
            .expect("current shard authority");
        let accepted = accepted.try_snapshot().expect("accepted KV snapshot");
        sinomo_core::kv::write_imported_cluster_kv_snapshot_if_changed(
            &cluster_paths.kv_paths(),
            &accepted,
            |_| false,
        )
        .expect("import current KV snapshot");

        assert!(
            sinomo_core::cluster::read_cluster_kv_local_snapshot(&cluster_paths, &state)
                .expect("local Raft snapshot")
                .value
                .get(&shard_key)
                .is_none(),
            "the regression requires the local Raft view to trail the imported authority"
        );

        let context = cluster_dfs_metadata_replica_shard_context(
            &cluster_paths,
            &DfsMetadataShardId::new("root"),
        )
        .expect("replica shard context")
        .expect("configured replica shard");
        assert_eq!(context.shard, current_shard);

        let response = process_cluster_dfs_metadata_journal_batch(
            &data_dir,
            &cluster_paths,
            &source_node_id,
            DfsMetadataJournalBatchCommand {
                request_id: "latest-authority-empty-batch".to_string(),
                source_node_id: Some(source_node_id.clone()),
                file_system_id: Some(file_system_id),
                authority_epoch: Some(current_shard.epoch),
                verify_prefix_only: false,
                prefix_probe: None,
                authority_probe: None,
                batch: DfsMetadataJournalBatch {
                    shard_id: DfsMetadataShardId::new("root"),
                    entries: Vec::new(),
                    next_after_sequence: 0,
                },
            },
        )
        .expect("replica ingress response");
        assert!(matches!(
            response.result,
            DfsMetadataJournalBatchResult::Applied
        ));
    }
}

async fn prepare_cluster_dfs_metadata_journal_publish_async(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    execution_exclusions: &DfsMaintenanceExecutionExclusions,
    only_lane: Option<(DfsFileSystemId, DfsMetadataShardId)>,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<Option<DfsPreparedClusterMetadataJournalPublish>, DfsError> {
    let trace_started = Instant::now();
    let data_dir = data_dir.clone();
    let cluster_paths = cluster_paths.clone();
    let execution_exclusions = execution_exclusions.clone();
    let prepared = tokio::task::spawn_blocking(move || {
        prepare_cluster_dfs_metadata_journal_publish(
            data_dir,
            cluster_paths,
            &execution_exclusions,
            only_lane,
            options,
        )
    })
    .await
    .map_err(|source| DfsError::Metadata {
        message: format!("DFS metadata publish planning task failed: {source}"),
    })??;
    let (outcomes, targets) = prepared
        .as_ref()
        .map(|prepared| (prepared.outcomes.len(), prepared.targets.len()))
        .unwrap_or_default();
    let traceparent = crate::current_dfs_traceparent().unwrap_or_default();
    tracing::trace!(
        target: "sinomo.dfs.perf",
        traceparent = %traceparent,
        outcomes,
        targets,
        total_ms = trace_started.elapsed().as_millis(),
        "metadata-publish-plan"
    );
    Ok(prepared)
}

pub(crate) fn prepare_dfs_metadata_journal_publish_with_host(
    file_system_id: DfsFileSystemId,
    local_node_id: &NodeId,
    shards: Vec<DfsMetadataShardConfig>,
    host: &mut impl DfsMetadataJournalPublisherHost,
    metadata_suppressions: &[DfsMetadataReplicationSuppressionRecord],
    options: DfsMetadataJournalReplicationOptions,
) -> Result<
    (
        Vec<DfsMetadataJournalReplicationOutcome>,
        Vec<DfsPreparedMetadataJournalTargetPublish>,
    ),
    DfsError,
> {
    prepare_dfs_metadata_journal_publish_with_host_and_execution_exclusions(
        file_system_id,
        local_node_id,
        shards,
        host,
        metadata_suppressions,
        &DfsMaintenanceExecutionExclusions::default(),
        options,
    )
}

pub(crate) fn prepare_dfs_metadata_journal_publish_with_host_and_execution_exclusions(
    file_system_id: DfsFileSystemId,
    local_node_id: &NodeId,
    mut shards: Vec<DfsMetadataShardConfig>,
    host: &mut impl DfsMetadataJournalPublisherHost,
    metadata_suppressions: &[DfsMetadataReplicationSuppressionRecord],
    execution_exclusions: &DfsMaintenanceExecutionExclusions,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<
    (
        Vec<DfsMetadataJournalReplicationOutcome>,
        Vec<DfsPreparedMetadataJournalTargetPublish>,
    ),
    DfsError,
> {
    shards.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
    if shards.is_empty() {
        return Ok((
            vec![DfsMetadataJournalReplicationOutcome::MissingShard {
                file_system_id,
                shard_id: DfsMetadataShardId::new("root"),
            }],
            Vec::new(),
        ));
    }

    let mut outcomes = Vec::new();
    let mut targets = Vec::new();
    for shard in shards {
        let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
        if shard.primary_node_id != *local_node_id {
            outcomes.push(DfsMetadataJournalReplicationOutcome::NotPrimary {
                file_system_id: shard.file_system_id,
                shard_id,
                local_node_id: local_node_id.clone(),
                primary_node_id: shard.primary_node_id,
                epoch: shard.epoch,
            });
            continue;
        }

        let mut backend = host.open_primary_shard_backend(&shard_id)?;
        let mut payloads_by_after_sequence = BTreeMap::new();
        for target_node_id in shard
            .replica_node_ids
            .into_iter()
            .filter(|target_node_id| target_node_id != local_node_id)
        {
            if let Some(suppression) = dfs_metadata_replication_suppression_for(
                metadata_suppressions,
                &target_node_id,
                &shard_id,
                shard.epoch,
            ) {
                outcomes.push(
                    DfsMetadataJournalReplicationOutcome::TopologyMismatchSuppressed {
                        target_node_id,
                        shard_id: shard_id.clone(),
                        epoch: shard.epoch,
                        reason: suppression.reason.clone(),
                    },
                );
                continue;
            }
            let after_sequence =
                host.metadata_replication_cursor(&mut *backend, &target_node_id, &shard_id)?;
            if execution_exclusions.contains(&target_node_id) {
                outcomes.push(DfsMetadataJournalReplicationOutcome::MissingRoute {
                    target_node_id,
                    shard_id: shard_id.clone(),
                    after_sequence,
                });
                continue;
            }
            match prepare_dfs_metadata_journal_target_publish(
                &mut *backend,
                local_node_id,
                target_node_id,
                shard.file_system_id.clone(),
                shard_id.clone(),
                shard.epoch,
                after_sequence,
                options,
                &mut payloads_by_after_sequence,
            )? {
                DfsPreparedMetadataJournalTargetPublishResult::NoChanges { outcome } => {
                    outcomes.push(outcome);
                }
                DfsPreparedMetadataJournalTargetPublishResult::Publish { target } => {
                    targets.push(target);
                }
            }
        }
    }

    Ok((outcomes, targets))
}

fn prepare_dfs_metadata_journal_publish_with_cached_backends_and_execution_exclusions(
    data_dir: &SinomoDataDir,
    file_system_id: DfsFileSystemId,
    local_node_id: &NodeId,
    mut shards: Vec<DfsMetadataShardConfig>,
    metadata_suppressions: &[DfsMetadataReplicationSuppressionRecord],
    execution_exclusions: &DfsMaintenanceExecutionExclusions,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<
    (
        Vec<DfsMetadataJournalReplicationOutcome>,
        Vec<DfsPreparedMetadataJournalTargetPublish>,
    ),
    DfsError,
> {
    shards.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
    if shards.is_empty() {
        return Ok((
            vec![DfsMetadataJournalReplicationOutcome::MissingShard {
                file_system_id,
                shard_id: DfsMetadataShardId::new("root"),
            }],
            Vec::new(),
        ));
    }

    let mut outcomes = Vec::new();
    let mut targets = Vec::new();
    for shard in shards {
        let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
        if shard.primary_node_id != *local_node_id {
            outcomes.push(DfsMetadataJournalReplicationOutcome::NotPrimary {
                file_system_id: shard.file_system_id,
                shard_id,
                local_node_id: local_node_id.clone(),
                primary_node_id: shard.primary_node_id,
                epoch: shard.epoch,
            });
            continue;
        }

        let backend = match cached_cluster_metadata_backend(data_dir, &shard_id) {
            Ok(backend) => backend,
            Err(CachedClusterMetadataBackendError::CachePoisoned) => {
                return Err(DfsError::Metadata {
                    message: "cluster metadata backend cache lock is poisoned".to_string(),
                });
            }
            Err(CachedClusterMetadataBackendError::Open(error)) => return Err(error.into()),
        };
        let mut backend = backend.lock().map_err(|_| DfsError::Metadata {
            message: format!(
                "cluster metadata backend lock is poisoned for shard {}",
                shard_id.0
            ),
        })?;
        // The retained publisher backend is a durable read model, not a second
        // metadata authority. Refresh it under the shard's OS-backed write lock
        // so it observes the exact table/journal head committed by the request
        // lane without replaying the complete shard on every publication.
        backend.refresh_metadata_shard_durable_view(&shard_id)?;

        let mut payloads_by_after_sequence = BTreeMap::new();
        for target_node_id in shard
            .replica_node_ids
            .into_iter()
            .filter(|target_node_id| target_node_id != local_node_id)
        {
            if let Some(suppression) = dfs_metadata_replication_suppression_for(
                metadata_suppressions,
                &target_node_id,
                &shard_id,
                shard.epoch,
            ) {
                outcomes.push(
                    DfsMetadataJournalReplicationOutcome::TopologyMismatchSuppressed {
                        target_node_id,
                        shard_id: shard_id.clone(),
                        epoch: shard.epoch,
                        reason: suppression.reason.clone(),
                    },
                );
                continue;
            }
            let after_sequence = migrate_legacy_dfs_metadata_replication_cursor(
                &mut *backend,
                data_dir,
                &target_node_id,
                &shard_id,
            )?;
            if execution_exclusions.contains(&target_node_id) {
                outcomes.push(DfsMetadataJournalReplicationOutcome::MissingRoute {
                    target_node_id,
                    shard_id: shard_id.clone(),
                    after_sequence,
                });
                continue;
            }
            match prepare_dfs_metadata_journal_target_publish(
                &mut *backend,
                local_node_id,
                target_node_id,
                shard.file_system_id.clone(),
                shard_id.clone(),
                shard.epoch,
                after_sequence,
                options,
                &mut payloads_by_after_sequence,
            )? {
                DfsPreparedMetadataJournalTargetPublishResult::NoChanges { outcome } => {
                    outcomes.push(outcome);
                }
                DfsPreparedMetadataJournalTargetPublishResult::Publish { target } => {
                    targets.push(target);
                }
            }
        }
    }

    Ok((outcomes, targets))
}

enum DfsPreparedMetadataJournalTargetPublishResult {
    NoChanges {
        outcome: DfsMetadataJournalReplicationOutcome,
    },
    Publish {
        target: DfsPreparedMetadataJournalTargetPublish,
    },
}

#[derive(Clone, Debug)]
enum DfsPreparedMetadataJournalPayload {
    NoChanges,
    CaughtUp {
        state_fingerprint: String,
    },
    Batches {
        batches: Vec<DfsMetadataJournalBatch>,
        backlog_after_sequence: Option<u64>,
    },
}

fn prepare_dfs_metadata_journal_target_publish(
    backend: &mut dyn DfsMetadataBackend,
    local_node_id: &NodeId,
    target_node_id: NodeId,
    file_system_id: DfsFileSystemId,
    shard_id: DfsMetadataShardId,
    shard_epoch: u64,
    after_sequence: u64,
    options: DfsMetadataJournalReplicationOptions,
    payloads_by_after_sequence: &mut BTreeMap<u64, DfsPreparedMetadataJournalPayload>,
) -> Result<DfsPreparedMetadataJournalTargetPublishResult, DfsError> {
    let payload = match payloads_by_after_sequence.get(&after_sequence) {
        Some(payload) => payload.clone(),
        None => {
            let mut next_after_sequence = after_sequence;
            let mut batches = Vec::new();
            let mut backlog_after_sequence = None;
            let payload = loop {
                let Some(batch) = backend.journal_batch_for_shard_since_limited(
                    &shard_id,
                    next_after_sequence,
                    options.batch_entry_limit,
                    options.batch_byte_limit,
                )?
                else {
                    if batches.is_empty() {
                        if !options.probe_caught_up_replicas {
                            break DfsPreparedMetadataJournalPayload::NoChanges;
                        }
                        let state_fingerprint =
                            crate::service_sync::metadata_shard_state_fingerprint_at(
                                backend,
                                &shard_id,
                                after_sequence,
                            )?;
                        break DfsPreparedMetadataJournalPayload::CaughtUp { state_fingerprint };
                    }
                    break DfsPreparedMetadataJournalPayload::Batches {
                        batches,
                        backlog_after_sequence,
                    };
                };

                next_after_sequence = batch.next_after_sequence;
                batches.push(batch);
                if batches.len() >= options.max_batches_per_target {
                    if backend
                        .journal_batch_for_shard_since_limited(
                            &shard_id,
                            next_after_sequence,
                            options.batch_entry_limit,
                            options.batch_byte_limit,
                        )?
                        .is_some()
                    {
                        backlog_after_sequence = Some(next_after_sequence);
                    }
                    break DfsPreparedMetadataJournalPayload::Batches {
                        batches,
                        backlog_after_sequence,
                    };
                }
            };
            payloads_by_after_sequence.insert(after_sequence, payload.clone());
            payload
        }
    };

    match payload {
        DfsPreparedMetadataJournalPayload::NoChanges => {
            Ok(DfsPreparedMetadataJournalTargetPublishResult::NoChanges {
                outcome: DfsMetadataJournalReplicationOutcome::NoChanges {
                    target_node_id,
                    shard_id,
                    after_sequence,
                },
            })
        }
        DfsPreparedMetadataJournalPayload::CaughtUp { state_fingerprint } => {
            let command = dfs_metadata_caught_up_probe_command(
                local_node_id,
                &target_node_id,
                &file_system_id,
                &shard_id,
                shard_epoch,
                after_sequence,
                state_fingerprint,
            );
            Ok(DfsPreparedMetadataJournalTargetPublishResult::Publish {
                target: DfsPreparedMetadataJournalTargetPublish {
                    target_node_id,
                    file_system_id,
                    shard_id,
                    shard_epoch,
                    after_sequence,
                    commands: vec![command],
                    backlog_after_sequence: None,
                    caught_up_probe: true,
                },
            })
        }
        DfsPreparedMetadataJournalPayload::Batches {
            batches,
            backlog_after_sequence,
        } => {
            let commands = batches
                .into_iter()
                .map(|batch| {
                    dfs_metadata_journal_batch_command(
                        local_node_id,
                        &target_node_id,
                        &file_system_id,
                        shard_epoch,
                        batch,
                    )
                })
                .collect();
            Ok(DfsPreparedMetadataJournalTargetPublishResult::Publish {
                target: DfsPreparedMetadataJournalTargetPublish {
                    target_node_id,
                    file_system_id,
                    shard_id,
                    shard_epoch,
                    after_sequence,
                    commands,
                    backlog_after_sequence,
                    caught_up_probe: false,
                },
            })
        }
    }
}

fn dfs_metadata_replication_suppression_for<'a>(
    records: &'a [DfsMetadataReplicationSuppressionRecord],
    target_node_id: &NodeId,
    shard_id: &DfsMetadataShardId,
    shard_epoch: u64,
) -> Option<&'a DfsMetadataReplicationSuppressionRecord> {
    records.iter().find(|record| {
        record.target_node_id == *target_node_id
            && record.shard_id == *shard_id
            && record.shard_epoch == shard_epoch
    })
}

fn record_dfs_metadata_replication_topology_mismatch(
    data_dir: &SinomoDataDir,
    target_node_id: &NodeId,
    shard_id: &DfsMetadataShardId,
    shard_epoch: u64,
    reason: &str,
) -> Result<(), DfsError> {
    if !metadata_replication_rejection_is_topology_mismatch(reason) {
        return Ok(());
    }
    let _write_guard = DFS_METADATA_REPLICATION_SUPPRESSION_WRITE_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .map_err(|_| DfsError::Metadata {
            message: "DFS metadata replication suppression write lock is poisoned".to_string(),
        })?;
    put_local_dfs_metadata_replication_suppression(
        data_dir,
        DfsMetadataReplicationSuppressionRecord {
            target_node_id: target_node_id.clone(),
            shard_id: shard_id.clone(),
            shard_epoch,
            reason: reason.to_owned(),
            updated_at_unix_nanos: dfs_now_unix_nanos(),
        },
    )
}

async fn record_dfs_metadata_replication_topology_mismatch_async(
    data_dir: &SinomoDataDir,
    target_node_id: &NodeId,
    shard_id: &DfsMetadataShardId,
    shard_epoch: u64,
    reason: &str,
) -> Result<(), DfsError> {
    if !metadata_replication_rejection_is_topology_mismatch(reason) {
        return Ok(());
    }
    let data_dir = data_dir.clone();
    let target_node_id = target_node_id.clone();
    let shard_id = shard_id.clone();
    let reason = reason.to_owned();
    tokio::task::spawn_blocking(move || {
        record_dfs_metadata_replication_topology_mismatch(
            &data_dir,
            &target_node_id,
            &shard_id,
            shard_epoch,
            &reason,
        )
    })
    .await
    .map_err(|source| DfsError::Metadata {
        message: format!("DFS metadata replication suppression write task failed: {source}"),
    })?
}

fn prepared_dfs_metadata_target_lanes(
    targets: Vec<DfsPreparedMetadataJournalTargetPublish>,
) -> VecDeque<Vec<(usize, DfsPreparedMetadataJournalTargetPublish)>> {
    let mut target_lanes =
        BTreeMap::<NodeId, Vec<(usize, DfsPreparedMetadataJournalTargetPublish)>>::new();
    for (target_index, target) in targets.into_iter().enumerate() {
        target_lanes
            .entry(target.target_node_id.clone())
            .or_default()
            .push((target_index, target));
    }
    target_lanes.into_values().collect()
}

/// Treat a node as temporarily ineligible only when every endpoint currently
/// advertised by the cluster directory has an explicit local `Unreachable`
/// record and the connectivity graph has no relay path to it. An unobserved
/// directory-only or module endpoint therefore keeps the target eligible.
/// Re-reading the graph for each publish also makes a connectivity transition
/// immediately restore the target without waiting for a replication timer.
fn dfs_metadata_target_is_explicitly_unreachable(
    graph: &ClusterConnectivityGraph,
    advertised_endpoints: &[ClusterEndpointRecord],
    local_node_id: &NodeId,
    target_node_id: &NodeId,
) -> bool {
    if cluster_nodes_share_healthy_component(graph, local_node_id, target_node_id) {
        return false;
    }

    let mut target_endpoints = advertised_endpoints
        .iter()
        .filter(|endpoint| &endpoint.node_id == target_node_id)
        .peekable();
    target_endpoints.peek().is_some()
        && target_endpoints.all(|endpoint| {
            matches!(
                graph.state_for_endpoint(endpoint),
                Some(ClusterConnectivityState::Unreachable)
            )
        })
}

fn defer_explicitly_unreachable_dfs_metadata_targets(
    plan: &mut DfsPreparedClusterMetadataJournalPublish,
) {
    if plan.targets.is_empty() {
        return;
    }
    let cluster_paths = ClusterPaths::new(&plan.data_dir);
    if !cluster_paths.state.exists() {
        return;
    }
    let Ok(state) = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths) else {
        return;
    };
    if state.local_node_id != plan.local_node_id {
        return;
    }

    // Fail open when the directory is not installed (for example, an
    // embedded/pre-cluster caller). State-member endpoints alone are not
    // sufficient here because the directory may own additional module or
    // dynamically published endpoints that must remain routable.
    let Ok(advertised_endpoints) = cluster_directory_for(&cluster_paths).endpoints() else {
        return;
    };
    if advertised_endpoints.is_empty() {
        return;
    }
    let Ok(graph) = ClusterConnectivityGraph::try_read_for_local(&cluster_paths, &state) else {
        return;
    };

    defer_explicitly_unreachable_dfs_metadata_targets_with_connectivity(
        plan,
        &graph,
        &advertised_endpoints,
    );
}

fn defer_explicitly_unreachable_dfs_metadata_targets_with_connectivity(
    plan: &mut DfsPreparedClusterMetadataJournalPublish,
    graph: &ClusterConnectivityGraph,
    advertised_endpoints: &[ClusterEndpointRecord],
) {
    let target_node_ids = plan
        .targets
        .iter()
        .map(|target| target.target_node_id.clone())
        .collect::<BTreeSet<_>>();
    let deferred_node_ids = target_node_ids
        .into_iter()
        .filter(|target_node_id| {
            dfs_metadata_target_is_explicitly_unreachable(
                graph,
                advertised_endpoints,
                &plan.local_node_id,
                target_node_id,
            )
        })
        .collect::<BTreeSet<_>>();
    if deferred_node_ids.is_empty() {
        return;
    }

    let mut deferred_outcomes = Vec::new();
    plan.targets.retain(|target| {
        if !deferred_node_ids.contains(&target.target_node_id) {
            return true;
        }
        deferred_outcomes.push(DfsMetadataJournalReplicationOutcome::MissingRoute {
            target_node_id: target.target_node_id.clone(),
            shard_id: target.shard_id.clone(),
            after_sequence: target.after_sequence,
        });
        false
    });
    plan.outcomes.extend(deferred_outcomes);
}

#[cfg(test)]
mod explicitly_unreachable_target_tests {
    use super::*;
    use sinomo_core::network::ClusterConnectivityRecord;
    use sinomo_core::node::ClusterId;
    use sinomo_core::{ClusterEndpointPrivacy, ClusterEndpointSource, ClusterTransportKind, Id};

    fn test_node(name: &str) -> NodeId {
        NodeId::from_legacy_named(name).expect("node id")
    }

    fn test_endpoint(
        name: &str,
        node_id: &NodeId,
        transport: ClusterTransportKind,
        address: &str,
        source: ClusterEndpointSource,
    ) -> ClusterEndpointRecord {
        ClusterEndpointRecord {
            endpoint_id: Id::from_legacy_named(name).expect("endpoint id"),
            node_id: node_id.clone(),
            transport,
            address: address.to_string(),
            source,
            privacy: ClusterEndpointPrivacy::Lan,
            priority: 10,
            last_observed_latency_ms: None,
        }
    }

    fn connectivity_record(
        cluster_id: &ClusterId,
        source_node_id: &NodeId,
        endpoint: &ClusterEndpointRecord,
        state: ClusterConnectivityState,
    ) -> ClusterConnectivityRecord {
        let now = OffsetDateTime::now_utc();
        let is_healthy = state == ClusterConnectivityState::Healthy;
        ClusterConnectivityRecord {
            cluster_id: cluster_id.clone(),
            source_node_id: source_node_id.clone(),
            target_node_id: endpoint.node_id.clone(),
            endpoint_id: endpoint.endpoint_id.clone(),
            transport: endpoint.transport.clone(),
            address: endpoint.address.clone(),
            state,
            latency_ms: is_healthy.then_some(4),
            consecutive_failures: if is_healthy { 0 } else { 3 },
            last_success_at: None,
            last_failure_at: None,
            failure_reason: None,
            observed_at: now,
            expires_at: now + time::Duration::hours(1),
        }
    }

    fn graph(
        local_node_id: &NodeId,
        records: impl IntoIterator<Item = ClusterConnectivityRecord>,
    ) -> ClusterConnectivityGraph {
        ClusterConnectivityGraph::from_records(local_node_id.clone(), records)
    }

    #[test]
    fn explicitly_unreachable_requires_every_advertised_endpoint_to_be_unreachable() {
        let cluster_id =
            ClusterId::from_legacy_named("cluster_dfs_metadata_offline_all").expect("cluster id");
        let local_node_id = test_node("node_dfs_metadata_offline_all_local");
        let target_node_id = test_node("node_dfs_metadata_offline_all_target");
        let tcp_endpoint = test_endpoint(
            "endpoint_dfs_metadata_offline_all_tcp",
            &target_node_id,
            ClusterTransportKind::LanTcp,
            "10.90.20.2:21353",
            ClusterEndpointSource::KvMembership,
        );
        let iroh_endpoint = test_endpoint(
            "endpoint_dfs_metadata_offline_all_iroh",
            &target_node_id,
            ClusterTransportKind::Iroh,
            "iroh-offline-target",
            ClusterEndpointSource::Rendezvous,
        );
        let advertised_endpoints = vec![tcp_endpoint.clone(), iroh_endpoint.clone()];
        let graph = graph(
            &local_node_id,
            [
                connectivity_record(
                    &cluster_id,
                    &local_node_id,
                    &tcp_endpoint,
                    ClusterConnectivityState::Unreachable,
                ),
                connectivity_record(
                    &cluster_id,
                    &local_node_id,
                    &iroh_endpoint,
                    ClusterConnectivityState::Unreachable,
                ),
            ],
        );

        assert!(dfs_metadata_target_is_explicitly_unreachable(
            &graph,
            &advertised_endpoints,
            &local_node_id,
            &target_node_id,
        ));
    }

    #[test]
    fn explicitly_unreachable_fails_open_for_unobserved_module_endpoint() {
        let cluster_id = ClusterId::from_legacy_named("cluster_dfs_metadata_offline_unobserved")
            .expect("cluster id");
        let local_node_id = test_node("node_dfs_metadata_offline_unobserved_local");
        let target_node_id = test_node("node_dfs_metadata_offline_unobserved_target");
        let tcp_endpoint = test_endpoint(
            "endpoint_dfs_metadata_offline_unobserved_tcp",
            &target_node_id,
            ClusterTransportKind::LanTcp,
            "10.90.21.2:21353",
            ClusterEndpointSource::KvMembership,
        );
        let module_endpoint = test_endpoint(
            "endpoint_dfs_metadata_offline_unobserved_module",
            &target_node_id,
            ClusterTransportKind::Iroh,
            "iroh-module-target",
            ClusterEndpointSource::Rendezvous,
        );
        let graph = graph(
            &local_node_id,
            [connectivity_record(
                &cluster_id,
                &local_node_id,
                &tcp_endpoint,
                ClusterConnectivityState::Unreachable,
            )],
        );

        assert!(!dfs_metadata_target_is_explicitly_unreachable(
            &graph,
            &[tcp_endpoint, module_endpoint],
            &local_node_id,
            &target_node_id,
        ));
    }

    #[test]
    fn explicitly_unreachable_recovers_immediately_on_connectivity_transition() {
        let cluster_id = ClusterId::from_legacy_named("cluster_dfs_metadata_offline_recovery")
            .expect("cluster id");
        let local_node_id = test_node("node_dfs_metadata_offline_recovery_local");
        let target_node_id = test_node("node_dfs_metadata_offline_recovery_target");
        let endpoint = test_endpoint(
            "endpoint_dfs_metadata_offline_recovery",
            &target_node_id,
            ClusterTransportKind::LanTcp,
            "10.90.22.2:21353",
            ClusterEndpointSource::KvMembership,
        );
        let unreachable_graph = graph(
            &local_node_id,
            [connectivity_record(
                &cluster_id,
                &local_node_id,
                &endpoint,
                ClusterConnectivityState::Unreachable,
            )],
        );
        let healthy_graph = graph(
            &local_node_id,
            [connectivity_record(
                &cluster_id,
                &local_node_id,
                &endpoint,
                ClusterConnectivityState::Healthy,
            )],
        );

        assert!(dfs_metadata_target_is_explicitly_unreachable(
            &unreachable_graph,
            std::slice::from_ref(&endpoint),
            &local_node_id,
            &target_node_id,
        ));
        assert!(!dfs_metadata_target_is_explicitly_unreachable(
            &healthy_graph,
            std::slice::from_ref(&endpoint),
            &local_node_id,
            &target_node_id,
        ));
    }

    #[test]
    fn explicitly_unreachable_keeps_target_with_live_relay_path() {
        let cluster_id =
            ClusterId::from_legacy_named("cluster_dfs_metadata_offline_relay").expect("cluster id");
        let local_node_id = test_node("node_dfs_metadata_offline_relay_local");
        let relay_node_id = test_node("node_dfs_metadata_offline_relay_bridge");
        let target_node_id = test_node("node_dfs_metadata_offline_relay_target");
        let direct_endpoint = test_endpoint(
            "endpoint_dfs_metadata_offline_relay_direct",
            &target_node_id,
            ClusterTransportKind::LanTcp,
            "10.90.23.3:21353",
            ClusterEndpointSource::KvMembership,
        );
        let relay_endpoint = test_endpoint(
            "endpoint_dfs_metadata_offline_relay_bridge",
            &relay_node_id,
            ClusterTransportKind::LanTcp,
            "10.90.23.2:21353",
            ClusterEndpointSource::KvMembership,
        );
        let relay_target_endpoint = test_endpoint(
            "endpoint_dfs_metadata_offline_relay_bridge_target",
            &target_node_id,
            ClusterTransportKind::LanTcp,
            "10.90.23.3:21353",
            ClusterEndpointSource::Observed,
        );
        let graph = graph(
            &local_node_id,
            [
                connectivity_record(
                    &cluster_id,
                    &local_node_id,
                    &direct_endpoint,
                    ClusterConnectivityState::Unreachable,
                ),
                connectivity_record(
                    &cluster_id,
                    &local_node_id,
                    &relay_endpoint,
                    ClusterConnectivityState::Healthy,
                ),
                connectivity_record(
                    &cluster_id,
                    &relay_node_id,
                    &relay_target_endpoint,
                    ClusterConnectivityState::Healthy,
                ),
            ],
        );

        assert!(!dfs_metadata_target_is_explicitly_unreachable(
            &graph,
            &[direct_endpoint],
            &local_node_id,
            &target_node_id,
        ));
    }

    #[test]
    fn explicitly_unreachable_rejects_stale_degraded_relay_path() {
        let cluster_id =
            ClusterId::from_legacy_named("cluster_dfs_metadata_offline_degraded_relay")
                .expect("cluster id");
        let local_node_id = test_node("node_dfs_metadata_offline_degraded_relay_local");
        let relay_node_id = test_node("node_dfs_metadata_offline_degraded_relay_bridge");
        let target_node_id = test_node("node_dfs_metadata_offline_degraded_relay_target");
        let direct_endpoint = test_endpoint(
            "endpoint_dfs_metadata_offline_degraded_relay_direct",
            &target_node_id,
            ClusterTransportKind::LanTcp,
            "10.90.25.3:21353",
            ClusterEndpointSource::KvMembership,
        );
        let relay_endpoint = test_endpoint(
            "endpoint_dfs_metadata_offline_degraded_relay_bridge",
            &relay_node_id,
            ClusterTransportKind::LanTcp,
            "10.90.25.2:21353",
            ClusterEndpointSource::KvMembership,
        );
        let relay_target_endpoint = test_endpoint(
            "endpoint_dfs_metadata_offline_degraded_relay_bridge_target",
            &target_node_id,
            ClusterTransportKind::LanTcp,
            "10.90.25.3:21353",
            ClusterEndpointSource::Observed,
        );
        let graph = graph(
            &local_node_id,
            [
                connectivity_record(
                    &cluster_id,
                    &local_node_id,
                    &direct_endpoint,
                    ClusterConnectivityState::Unreachable,
                ),
                connectivity_record(
                    &cluster_id,
                    &local_node_id,
                    &relay_endpoint,
                    ClusterConnectivityState::Healthy,
                ),
                connectivity_record(
                    &cluster_id,
                    &relay_node_id,
                    &relay_target_endpoint,
                    ClusterConnectivityState::Degraded,
                ),
            ],
        );

        assert!(dfs_metadata_target_is_explicitly_unreachable(
            &graph,
            &[direct_endpoint],
            &local_node_id,
            &target_node_id,
        ));
    }

    #[test]
    fn explicitly_unreachable_target_is_deferred_without_dropping_healthy_target() {
        let cluster_id = ClusterId::from_legacy_named("cluster_dfs_metadata_offline_filter")
            .expect("cluster id");
        let local_node_id = test_node("node_dfs_metadata_offline_filter_local");
        let offline_node_id = test_node("node_dfs_metadata_offline_filter_offline");
        let healthy_node_id = test_node("node_dfs_metadata_offline_filter_healthy");
        let offline_endpoint = test_endpoint(
            "endpoint_dfs_metadata_offline_filter_offline",
            &offline_node_id,
            ClusterTransportKind::LanTcp,
            "10.90.24.2:21353",
            ClusterEndpointSource::KvMembership,
        );
        let healthy_endpoint = test_endpoint(
            "endpoint_dfs_metadata_offline_filter_healthy",
            &healthy_node_id,
            ClusterTransportKind::LanTcp,
            "10.90.24.3:21353",
            ClusterEndpointSource::KvMembership,
        );
        let graph = graph(
            &local_node_id,
            [
                connectivity_record(
                    &cluster_id,
                    &local_node_id,
                    &offline_endpoint,
                    ClusterConnectivityState::Unreachable,
                ),
                connectivity_record(
                    &cluster_id,
                    &local_node_id,
                    &healthy_endpoint,
                    ClusterConnectivityState::Healthy,
                ),
            ],
        );
        let file_system_id = DfsFileSystemId::from_legacy_named("fs_dfs_metadata_offline_filter")
            .expect("file system id");
        let shard_id = DfsMetadataShardId::new("services");
        let target = |target_node_id: NodeId| DfsPreparedMetadataJournalTargetPublish {
            target_node_id,
            file_system_id: file_system_id.clone(),
            shard_id: shard_id.clone(),
            shard_epoch: 4,
            after_sequence: 19,
            commands: Vec::new(),
            backlog_after_sequence: None,
            caught_up_probe: false,
        };
        let mut plan = DfsPreparedClusterMetadataJournalPublish {
            data_dir: SinomoDataDir::new(std::env::temp_dir()),
            local_node_id: local_node_id.clone(),
            peer_node_ids: vec![offline_node_id.clone(), healthy_node_id.clone()],
            outcomes: Vec::new(),
            targets: vec![
                target(offline_node_id.clone()),
                target(healthy_node_id.clone()),
            ],
        };

        defer_explicitly_unreachable_dfs_metadata_targets_with_connectivity(
            &mut plan,
            &graph,
            &[offline_endpoint, healthy_endpoint],
        );

        assert_eq!(plan.targets.len(), 1);
        assert_eq!(plan.targets[0].target_node_id, healthy_node_id);
        assert!(matches!(
            plan.outcomes.as_slice(),
            [DfsMetadataJournalReplicationOutcome::MissingRoute {
                target_node_id,
                shard_id: deferred_shard_id,
                after_sequence: 19,
            }] if target_node_id == &offline_node_id && deferred_shard_id == &shard_id
        ));
    }
}

fn execute_prepared_dfs_metadata_journal_publish(
    mut plan: DfsPreparedClusterMetadataJournalPublish,
    transport: &mut dyn DfsMetadataJournalReplicationTransport,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<DfsMetadataJournalReplicationReport, DfsError> {
    defer_explicitly_unreachable_dfs_metadata_targets(&mut plan);
    let DfsPreparedClusterMetadataJournalPublish {
        data_dir,
        local_node_id,
        peer_node_ids: _,
        mut outcomes,
        targets,
    } = plan;
    let Some(first_lane_transport) = transport.fork_target_lane() else {
        for target in targets {
            execute_prepared_dfs_metadata_journal_target_publish(
                &data_dir,
                &local_node_id,
                target,
                transport,
                options,
                &mut outcomes,
            )?;
        }
        return Ok(DfsMetadataJournalReplicationReport { outcomes });
    };

    let target_count = targets.len();
    let mut first_lane_transport = Some(first_lane_transport);
    let mut pending_lanes = prepared_dfs_metadata_target_lanes(targets)
        .into_iter()
        .map(|target_lane| {
            let lane_transport = first_lane_transport.take().unwrap_or_else(|| {
                transport
                    .fork_target_lane()
                    .expect("metadata transport stopped supporting target lane forks")
            });
            (target_lane, lane_transport)
        })
        .collect::<VecDeque<_>>();
    let mut ordered_target_results = std::iter::repeat_with(|| None)
        .take(target_count)
        .collect::<Vec<_>>();
    while !pending_lanes.is_empty() {
        let lane_count = pending_lanes
            .len()
            .min(DEFAULT_DFS_METADATA_REPLICATION_MAX_CONCURRENT_TARGET_NODES);
        let lanes = pending_lanes.drain(..lane_count).collect::<Vec<_>>();
        let completed_lanes = std::thread::scope(|scope| {
            lanes
                .into_iter()
                .map(|(target_lane, mut lane_transport)| {
                    let data_dir = &data_dir;
                    let local_node_id = &local_node_id;
                    scope.spawn(move || {
                        let mut lane_results = Vec::with_capacity(target_lane.len());
                        for (target_index, target) in target_lane {
                            let mut target_outcomes = Vec::new();
                            let result = execute_prepared_dfs_metadata_journal_target_publish(
                                &data_dir,
                                &local_node_id,
                                target,
                                lane_transport.as_mut(),
                                options,
                                &mut target_outcomes,
                            )
                            .map(|()| target_outcomes);
                            let failed = result.is_err();
                            lane_results.push((target_index, result));
                            if failed {
                                break;
                            }
                        }
                        lane_results
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|handle| handle.join().expect("metadata target lane panicked"))
                .collect::<Vec<_>>()
        });
        for completed in completed_lanes {
            for (target_index, result) in completed {
                ordered_target_results[target_index] = Some(result);
            }
        }
    }

    for result in ordered_target_results {
        let result = result.expect("metadata target result missing without an earlier error");
        match result {
            Ok(mut target_outcomes) => outcomes.append(&mut target_outcomes),
            Err(error) => return Err(error),
        }
    }
    Ok(DfsMetadataJournalReplicationReport { outcomes })
}

async fn execute_prepared_dfs_metadata_journal_publish_async(
    mut plan: DfsPreparedClusterMetadataJournalPublish,
    transport: &dyn AsyncDfsMetadataJournalReplicationTransport,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<DfsMetadataJournalReplicationReport, DfsError> {
    defer_explicitly_unreachable_dfs_metadata_targets(&mut plan);
    let DfsPreparedClusterMetadataJournalPublish {
        data_dir,
        local_node_id,
        peer_node_ids: _,
        mut outcomes,
        targets,
    } = plan;
    let target_count = targets.len();
    // One lane owns all shard plans for a target node, preserving the
    // established per-target batch/shard ordering. Independent node lanes run
    // concurrently so an offline replica cannot head-of-line block healthy
    // replicas. Results are reassembled by their original plan index below,
    // keeping report/error ordering deterministic rather than completion-timed.
    let mut pending_lanes = prepared_dfs_metadata_target_lanes(targets);
    let mut in_flight = Vec::<DfsMetadataTargetLaneFuture<'_>>::new();
    let mut ordered_target_results = std::iter::repeat_with(|| None)
        .take(target_count)
        .collect::<Vec<_>>();
    while !pending_lanes.is_empty() || !in_flight.is_empty() {
        while in_flight.len() < DEFAULT_DFS_METADATA_REPLICATION_MAX_CONCURRENT_TARGET_NODES {
            let Some(target_lane) = pending_lanes.pop_front() else {
                break;
            };
            let data_dir = &data_dir;
            let local_node_id = &local_node_id;
            in_flight.push(Box::pin(async move {
                let mut lane_results = Vec::with_capacity(target_lane.len());
                for (target_index, target) in target_lane {
                    let mut target_outcomes = Vec::new();
                    let result = execute_prepared_dfs_metadata_journal_target_publish_async(
                        &data_dir,
                        &local_node_id,
                        target,
                        transport,
                        options,
                        &mut target_outcomes,
                    )
                    .await
                    .map(|()| target_outcomes);
                    let failed = result.is_err();
                    lane_results.push((target_index, result));
                    if failed {
                        break;
                    }
                }
                lane_results
            }));
        }

        let completed = next_completed_dfs_metadata_target_lane(&mut in_flight).await;
        for (target_index, result) in completed {
            ordered_target_results[target_index] = Some(result);
        }
    }

    for result in ordered_target_results {
        let result = result.expect("metadata target result missing without an earlier error");
        match result {
            Ok(mut target_outcomes) => outcomes.append(&mut target_outcomes),
            Err(error) => return Err(error),
        }
    }
    Ok(DfsMetadataJournalReplicationReport { outcomes })
}

type DfsMetadataTargetPublishResult = Result<Vec<DfsMetadataJournalReplicationOutcome>, DfsError>;
type DfsMetadataTargetLaneResult = Vec<(usize, DfsMetadataTargetPublishResult)>;
type DfsMetadataTargetLaneFuture<'a> =
    Pin<Box<dyn Future<Output = DfsMetadataTargetLaneResult> + Send + 'a>>;

async fn next_completed_dfs_metadata_target_lane(
    in_flight: &mut Vec<DfsMetadataTargetLaneFuture<'_>>,
) -> DfsMetadataTargetLaneResult {
    let (completed_index, result) = std::future::poll_fn(|context| {
        for (index, future) in in_flight.iter_mut().enumerate() {
            if let Poll::Ready(result) = future.as_mut().poll(context) {
                return Poll::Ready((index, result));
            }
        }
        Poll::Pending
    })
    .await;
    drop(in_flight.swap_remove(completed_index));
    result
}

fn execute_prepared_dfs_metadata_journal_target_publish(
    data_dir: &SinomoDataDir,
    local_node_id: &NodeId,
    target: DfsPreparedMetadataJournalTargetPublish,
    transport: &mut dyn DfsMetadataJournalReplicationTransport,
    options: DfsMetadataJournalReplicationOptions,
    outcomes: &mut Vec<DfsMetadataJournalReplicationOutcome>,
) -> Result<(), DfsError> {
    validate_prepared_caught_up_probe(&target)?;
    let DfsPreparedMetadataJournalTargetPublish {
        target_node_id,
        file_system_id,
        shard_id,
        shard_epoch,
        mut after_sequence,
        commands,
        backlog_after_sequence,
        caught_up_probe,
    } = target;
    if !transport.has_metadata_replication_route(&target_node_id) {
        outcomes.push(DfsMetadataJournalReplicationOutcome::MissingRoute {
            target_node_id,
            shard_id,
            after_sequence,
        });
        return Ok(());
    }

    let target_started_at = Instant::now();
    let mut completed_plan = true;
    for command in commands {
        let Some(attempt_timeout) =
            dfs_metadata_replication_attempt_timeout(options, target_started_at)
        else {
            if caught_up_probe {
                outcomes.push(DfsMetadataJournalReplicationOutcome::Failed {
                    target_node_id: target_node_id.clone(),
                    shard_id: shard_id.clone(),
                    reason: "caught-up metadata probe exhausted its target budget".to_string(),
                });
            } else {
                outcomes.push(DfsMetadataJournalReplicationOutcome::BacklogRemaining {
                    target_node_id: target_node_id.clone(),
                    shard_id: shard_id.clone(),
                    after_sequence,
                    max_batches: options.max_batches_per_target,
                });
            }
            completed_plan = false;
            break;
        };
        let response =
            match transport.send_metadata_journal_batch(&target_node_id, &command, attempt_timeout)
            {
                Ok(response) => response,
                Err(error) => {
                    push_dfs_metadata_transport_error_outcome(
                        outcomes,
                        &target_node_id,
                        &shard_id,
                        after_sequence,
                        error,
                    );
                    completed_plan = false;
                    break;
                }
            };
        if let Err(reason) =
            validate_dfs_metadata_journal_replication_response(&command, &shard_id, &response)
        {
            outcomes.push(DfsMetadataJournalReplicationOutcome::Failed {
                target_node_id: target_node_id.clone(),
                shard_id: shard_id.clone(),
                reason,
            });
            completed_plan = false;
            break;
        }
        let verified_caught_up_probe = !caught_up_probe || response.verified_prefix_only;
        match response.result {
            DfsMetadataJournalBatchResult::Applied => {
                if !verified_caught_up_probe {
                    outcomes.push(DfsMetadataJournalReplicationOutcome::Failed {
                        target_node_id: target_node_id.clone(),
                        shard_id: shard_id.clone(),
                        reason: "metadata replica did not verify the exact caught-up prefix"
                            .to_string(),
                    });
                    completed_plan = false;
                    break;
                }
                let next_after_sequence = command.batch.next_after_sequence;
                put_local_dfs_metadata_replication_cursor(
                    data_dir,
                    &target_node_id,
                    &shard_id,
                    next_after_sequence,
                )?;
                after_sequence = next_after_sequence;
                if caught_up_probe {
                    outcomes.push(DfsMetadataJournalReplicationOutcome::NoChanges {
                        target_node_id: target_node_id.clone(),
                        shard_id: shard_id.clone(),
                        after_sequence: next_after_sequence,
                    });
                } else {
                    outcomes.push(DfsMetadataJournalReplicationOutcome::Applied {
                        target_node_id: target_node_id.clone(),
                        shard_id: shard_id.clone(),
                        entries: command.batch.entries.len(),
                        next_after_sequence,
                    });
                }
            }
            DfsMetadataJournalBatchResult::Rejected { reason } => {
                if metadata_journal_rejection_allows_snapshot_repair(&reason) {
                    execute_prepared_dfs_metadata_snapshot_repair(
                        data_dir,
                        local_node_id,
                        &target_node_id,
                        &shard_id,
                        shard_epoch,
                        &file_system_id,
                        after_sequence,
                        reason,
                        transport,
                        options,
                        target_started_at,
                        outcomes,
                    )?;
                } else {
                    record_dfs_metadata_replication_topology_mismatch(
                        data_dir,
                        &target_node_id,
                        &shard_id,
                        shard_epoch,
                        &reason,
                    )?;
                    outcomes.push(DfsMetadataJournalReplicationOutcome::Rejected {
                        target_node_id: target_node_id.clone(),
                        shard_id: shard_id.clone(),
                        reason,
                    });
                }
                completed_plan = false;
                break;
            }
        }
    }

    if completed_plan {
        if let Some(after_sequence) = backlog_after_sequence {
            outcomes.push(DfsMetadataJournalReplicationOutcome::BacklogRemaining {
                target_node_id,
                shard_id,
                after_sequence,
                max_batches: options.max_batches_per_target,
            });
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn execute_prepared_dfs_metadata_snapshot_repair(
    data_dir: &SinomoDataDir,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
    shard_id: &DfsMetadataShardId,
    shard_epoch: u64,
    file_system_id: &DfsFileSystemId,
    after_sequence: u64,
    reason: String,
    transport: &mut dyn DfsMetadataJournalReplicationTransport,
    options: DfsMetadataJournalReplicationOptions,
    target_started_at: Instant,
    outcomes: &mut Vec<DfsMetadataJournalReplicationOutcome>,
) -> Result<(), DfsError> {
    let command = build_local_dfs_metadata_snapshot_install_command(
        data_dir,
        local_node_id,
        target_node_id,
        file_system_id,
        shard_epoch,
        shard_id,
        reason.clone(),
    )?;
    let Some(attempt_timeout) =
        dfs_metadata_replication_attempt_timeout(options, target_started_at)
    else {
        outcomes.push(DfsMetadataJournalReplicationOutcome::BacklogRemaining {
            target_node_id: target_node_id.clone(),
            shard_id: shard_id.clone(),
            after_sequence,
            max_batches: options.max_batches_per_target,
        });
        return Ok(());
    };
    let response =
        match transport.send_metadata_snapshot_install(target_node_id, &command, attempt_timeout) {
            Ok(response) => response,
            Err(error) => {
                push_dfs_metadata_snapshot_transport_error_outcome(
                    outcomes,
                    target_node_id,
                    shard_id,
                    after_sequence,
                    reason,
                    error,
                );
                return Ok(());
            }
        };
    if let Err(reason) =
        validate_dfs_metadata_snapshot_install_replication_response(&command, &response)
    {
        outcomes.push(DfsMetadataJournalReplicationOutcome::Failed {
            target_node_id: target_node_id.clone(),
            shard_id: shard_id.clone(),
            reason,
        });
        return Ok(());
    }
    match response.result {
        DfsMetadataSnapshotInstallResult::Installed { archived_existing } => {
            put_local_dfs_metadata_replication_cursor(
                data_dir,
                target_node_id,
                shard_id,
                command.next_after_sequence,
            )?;
            outcomes.push(DfsMetadataJournalReplicationOutcome::SnapshotInstalled {
                target_node_id: target_node_id.clone(),
                shard_id: shard_id.clone(),
                next_after_sequence: command.next_after_sequence,
                archived_existing,
            });
        }
        DfsMetadataSnapshotInstallResult::Rejected {
            reason: snapshot_reason,
        } => {
            let reason = format!("{reason}; snapshot repair rejected: {snapshot_reason}");
            record_dfs_metadata_replication_topology_mismatch(
                data_dir,
                target_node_id,
                shard_id,
                shard_epoch,
                &reason,
            )?;
            outcomes.push(DfsMetadataJournalReplicationOutcome::Rejected {
                target_node_id: target_node_id.clone(),
                shard_id: shard_id.clone(),
                reason,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod concurrent_target_publish_tests {
    use super::*;

    fn prepared_target(
        local_node_id: &NodeId,
        target_node_id: NodeId,
        file_system_id: DfsFileSystemId,
        shard_id: &str,
    ) -> DfsPreparedMetadataJournalTargetPublish {
        let shard_id = DfsMetadataShardId::new(shard_id);
        DfsPreparedMetadataJournalTargetPublish {
            target_node_id: target_node_id.clone(),
            file_system_id: file_system_id.clone(),
            shard_id: shard_id.clone(),
            shard_epoch: 1,
            after_sequence: 0,
            commands: vec![DfsMetadataJournalBatchCommand {
                request_id: format!("publish-{target_node_id}-{}", shard_id.0),
                source_node_id: Some(local_node_id.clone()),
                file_system_id: Some(file_system_id),
                authority_epoch: Some(1),
                verify_prefix_only: false,
                prefix_probe: None,
                authority_probe: None,
                batch: DfsMetadataJournalBatch {
                    shard_id,
                    entries: Vec::new(),
                    next_after_sequence: 1,
                },
            }],
            backlog_after_sequence: None,
            caught_up_probe: false,
        }
    }

    fn snapshot_sender() -> AsyncDfsMetadataSnapshotInstallSender {
        Arc::new(move |_target_node_id, command, _timeout| {
            Box::pin(async move {
                Ok(DfsMetadataSnapshotInstallResponse {
                    request_id: command.request_id.clone(),
                    shard_id: command.shard_id.clone(),
                    next_after_sequence: command.next_after_sequence,
                    result: DfsMetadataSnapshotInstallResult::Installed {
                        archived_existing: false,
                    },
                })
            })
        })
    }

    fn sync_snapshot_sender() -> DfsMetadataSnapshotInstallSender {
        Arc::new(move |_target_node_id, command, _timeout| {
            Ok(DfsMetadataSnapshotInstallResponse {
                request_id: command.request_id.clone(),
                shard_id: command.shard_id.clone(),
                next_after_sequence: command.next_after_sequence,
                result: DfsMetadataSnapshotInstallResult::Installed {
                    archived_existing: false,
                },
            })
        })
    }

    #[test]
    fn sync_healthy_target_publish_does_not_wait_behind_offline_target() {
        let test_dir = sinomo_testdir::TestDir::new("dfs-metadata-sync-concurrent-targets");
        let data_dir = SinomoDataDir::new(test_dir.path());
        data_dir.ensure_root_layout().expect("root layout");
        let local_node_id =
            NodeId::from_legacy_named("node_metadata_sync_concurrent_local").expect("local node");
        let offline_node_id =
            NodeId::from_legacy_named("node_metadata_sync_concurrent_offline").expect("offline");
        let healthy_node_id =
            NodeId::from_legacy_named("node_metadata_sync_concurrent_healthy").expect("healthy");
        let file_system_id = distributed_sifs(local_node_id.clone()).file_system_id;
        let (offline_started_sender, offline_started_receiver) = std::sync::mpsc::channel();
        let (release_offline_sender, release_offline_receiver) = std::sync::mpsc::channel();
        let release_offline_receiver = Arc::new(Mutex::new(release_offline_receiver));
        let (healthy_completed_sender, healthy_completed_receiver) = std::sync::mpsc::channel();
        let sender: DfsMetadataJournalBatchSender = {
            let offline_node_id = offline_node_id.clone();
            let release_offline_receiver = Arc::clone(&release_offline_receiver);
            Arc::new(move |target_node_id, command, _timeout| {
                if *target_node_id == offline_node_id {
                    offline_started_sender.send(()).expect("offline started");
                    release_offline_receiver
                        .lock()
                        .expect("offline release lock")
                        .recv()
                        .expect("offline release");
                    return Err(DfsMetadataPeerTransportError::new("offline"));
                }
                healthy_completed_sender
                    .send(())
                    .expect("healthy completed");
                Ok(DfsMetadataJournalBatchResponse {
                    request_id: command.request_id.clone(),
                    shard_id: command.batch.shard_id.clone(),
                    next_after_sequence: command.batch.next_after_sequence,
                    applied_entries: command.batch.entries.len(),
                    verified_prefix_only: command.verify_prefix_only,
                    verified_authority_probe_id: None,
                    result: DfsMetadataJournalBatchResult::Applied,
                })
            })
        };
        let mut transport = DfsMetadataNodeTransport::new(
            vec![offline_node_id.clone(), healthy_node_id.clone()],
            sender,
            sync_snapshot_sender(),
        );
        let plan = DfsPreparedClusterMetadataJournalPublish {
            data_dir,
            local_node_id: local_node_id.clone(),
            peer_node_ids: vec![offline_node_id.clone(), healthy_node_id.clone()],
            outcomes: Vec::new(),
            targets: vec![
                prepared_target(
                    &local_node_id,
                    offline_node_id.clone(),
                    file_system_id.clone(),
                    "root",
                ),
                prepared_target(
                    &local_node_id,
                    healthy_node_id.clone(),
                    file_system_id,
                    "root",
                ),
            ],
        };

        let publish = std::thread::spawn(move || {
            execute_prepared_dfs_metadata_journal_publish(
                plan,
                &mut transport,
                DfsMetadataJournalReplicationOptions {
                    attempt_timeout: Duration::from_secs(5),
                    target_timeout: Duration::from_secs(5),
                    ..DfsMetadataJournalReplicationOptions::default()
                },
            )
        });
        offline_started_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("offline publish started");
        healthy_completed_receiver
            .recv_timeout(Duration::from_millis(250))
            .expect("healthy publish must complete while offline target is blocked");
        assert!(!publish.is_finished());

        release_offline_sender.send(()).expect("release offline");
        let report = publish
            .join()
            .expect("publish thread")
            .expect("publish report");
        assert!(matches!(
            &report.outcomes[0],
            DfsMetadataJournalReplicationOutcome::Failed { target_node_id, .. }
                if target_node_id == &offline_node_id
        ));
        assert!(matches!(
            &report.outcomes[1],
            DfsMetadataJournalReplicationOutcome::Applied { target_node_id, .. }
                if target_node_id == &healthy_node_id
        ));
    }

    #[test]
    fn healthy_target_publish_does_not_wait_behind_offline_target() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let test_dir = sinomo_testdir::TestDir::new("dfs-metadata-concurrent-targets");
            let data_dir = SinomoDataDir::new(test_dir.path());
            data_dir.ensure_root_layout().expect("root layout");
            let local_node_id =
                NodeId::from_legacy_named("node_metadata_concurrent_local").expect("local node");
            let offline_node_id =
                NodeId::from_legacy_named("node_metadata_concurrent_offline").expect("offline");
            let healthy_node_id =
                NodeId::from_legacy_named("node_metadata_concurrent_healthy").expect("healthy");
            let file_system_id = distributed_sifs(local_node_id.clone()).file_system_id;
            let offline_started = Arc::new(tokio::sync::Notify::new());
            let release_offline = Arc::new(tokio::sync::Notify::new());
            let healthy_completed = Arc::new(tokio::sync::Notify::new());
            let sender: AsyncDfsMetadataJournalBatchSender = {
                let offline_node_id = offline_node_id.clone();
                let offline_started = Arc::clone(&offline_started);
                let release_offline = Arc::clone(&release_offline);
                let healthy_completed = Arc::clone(&healthy_completed);
                Arc::new(move |target_node_id, command, _timeout| {
                    let is_offline = *target_node_id == offline_node_id;
                    let offline_started = Arc::clone(&offline_started);
                    let release_offline = Arc::clone(&release_offline);
                    let healthy_completed = Arc::clone(&healthy_completed);
                    Box::pin(async move {
                        if is_offline {
                            offline_started.notify_one();
                            release_offline.notified().await;
                            return Err(DfsMetadataPeerTransportError::new("offline"));
                        }
                        healthy_completed.notify_one();
                        Ok(DfsMetadataJournalBatchResponse {
                            request_id: command.request_id.clone(),
                            shard_id: command.batch.shard_id.clone(),
                            next_after_sequence: command.batch.next_after_sequence,
                            applied_entries: command.batch.entries.len(),
                            verified_prefix_only: command.verify_prefix_only,
                            verified_authority_probe_id: None,
                            result: DfsMetadataJournalBatchResult::Applied,
                        })
                    })
                })
            };
            let mut transport = AsyncDfsMetadataNodeTransport::new(
                vec![offline_node_id.clone(), healthy_node_id.clone()],
                sender,
                snapshot_sender(),
            );
            let plan = DfsPreparedClusterMetadataJournalPublish {
                data_dir,
                local_node_id: local_node_id.clone(),
                peer_node_ids: vec![offline_node_id.clone(), healthy_node_id.clone()],
                outcomes: Vec::new(),
                targets: vec![
                    prepared_target(
                        &local_node_id,
                        offline_node_id.clone(),
                        file_system_id.clone(),
                        "root",
                    ),
                    prepared_target(
                        &local_node_id,
                        healthy_node_id.clone(),
                        file_system_id,
                        "root",
                    ),
                ],
            };

            let publish = tokio::spawn(async move {
                execute_prepared_dfs_metadata_journal_publish_async(
                    plan,
                    &mut transport,
                    DfsMetadataJournalReplicationOptions {
                        attempt_timeout: Duration::from_secs(5),
                        target_timeout: Duration::from_secs(5),
                        ..DfsMetadataJournalReplicationOptions::default()
                    },
                )
                .await
            });
            tokio::time::timeout(Duration::from_secs(1), offline_started.notified())
                .await
                .expect("offline publish started");
            tokio::time::timeout(Duration::from_millis(250), healthy_completed.notified())
                .await
                .expect("healthy publish must complete while offline target is blocked");
            assert!(!publish.is_finished());

            release_offline.notify_one();
            let report = tokio::time::timeout(Duration::from_secs(1), publish)
                .await
                .expect("publish completed after offline release")
                .expect("publish task")
                .expect("publish report");
            assert!(matches!(
                &report.outcomes[0],
                DfsMetadataJournalReplicationOutcome::Failed { target_node_id, .. }
                    if target_node_id == &offline_node_id
            ));
            assert!(matches!(
                &report.outcomes[1],
                DfsMetadataJournalReplicationOutcome::Applied { target_node_id, .. }
                    if target_node_id == &healthy_node_id
            ));
        });
    }

    #[test]
    fn one_target_node_lane_preserves_shard_plan_order() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let test_dir = sinomo_testdir::TestDir::new("dfs-metadata-target-order");
            let data_dir = SinomoDataDir::new(test_dir.path());
            data_dir.ensure_root_layout().expect("root layout");
            let local_node_id =
                NodeId::from_legacy_named("node_metadata_order_local").expect("local node");
            let target_node_id =
                NodeId::from_legacy_named("node_metadata_order_target").expect("target node");
            let file_system_id = distributed_sifs(local_node_id.clone()).file_system_id;
            let first_started = Arc::new(tokio::sync::Notify::new());
            let release_first = Arc::new(tokio::sync::Notify::new());
            let second_started = Arc::new(tokio::sync::Notify::new());
            let sender: AsyncDfsMetadataJournalBatchSender = {
                let first_started = Arc::clone(&first_started);
                let release_first = Arc::clone(&release_first);
                let second_started = Arc::clone(&second_started);
                Arc::new(move |_target_node_id, command, _timeout| {
                    let is_first = command.batch.shard_id.0 == "root";
                    let first_started = Arc::clone(&first_started);
                    let release_first = Arc::clone(&release_first);
                    let second_started = Arc::clone(&second_started);
                    Box::pin(async move {
                        if is_first {
                            first_started.notify_one();
                            release_first.notified().await;
                        } else {
                            second_started.notify_one();
                        }
                        Ok(DfsMetadataJournalBatchResponse {
                            request_id: command.request_id.clone(),
                            shard_id: command.batch.shard_id.clone(),
                            next_after_sequence: command.batch.next_after_sequence,
                            applied_entries: command.batch.entries.len(),
                            verified_prefix_only: command.verify_prefix_only,
                            verified_authority_probe_id: None,
                            result: DfsMetadataJournalBatchResult::Applied,
                        })
                    })
                })
            };
            let mut transport = AsyncDfsMetadataNodeTransport::new(
                vec![target_node_id.clone()],
                sender,
                snapshot_sender(),
            );
            let plan = DfsPreparedClusterMetadataJournalPublish {
                data_dir,
                local_node_id: local_node_id.clone(),
                peer_node_ids: vec![target_node_id.clone()],
                outcomes: Vec::new(),
                targets: vec![
                    prepared_target(
                        &local_node_id,
                        target_node_id.clone(),
                        file_system_id.clone(),
                        "root",
                    ),
                    prepared_target(&local_node_id, target_node_id, file_system_id, "services"),
                ],
            };

            let publish = tokio::spawn(async move {
                execute_prepared_dfs_metadata_journal_publish_async(
                    plan,
                    &mut transport,
                    DfsMetadataJournalReplicationOptions::default(),
                )
                .await
            });
            tokio::time::timeout(Duration::from_secs(1), first_started.notified())
                .await
                .expect("first shard publish started");
            assert!(
                tokio::time::timeout(Duration::from_millis(100), second_started.notified())
                    .await
                    .is_err(),
                "the second shard must stay queued behind the first for the same node"
            );
            release_first.notify_one();
            tokio::time::timeout(Duration::from_secs(1), second_started.notified())
                .await
                .expect("second shard started after first completed");
            let report = tokio::time::timeout(Duration::from_secs(1), publish)
                .await
                .expect("ordered publish completed")
                .expect("publish task")
                .expect("publish report");
            assert_eq!(report.outcomes.len(), 2);
            assert!(matches!(
                &report.outcomes[0],
                DfsMetadataJournalReplicationOutcome::Applied { shard_id, .. }
                    if shard_id.0 == "root"
            ));
            assert!(matches!(
                &report.outcomes[1],
                DfsMetadataJournalReplicationOutcome::Applied { shard_id, .. }
                    if shard_id.0 == "services"
            ));
        });
    }
}

async fn execute_prepared_dfs_metadata_journal_target_publish_async(
    data_dir: &SinomoDataDir,
    local_node_id: &NodeId,
    target: DfsPreparedMetadataJournalTargetPublish,
    transport: &dyn AsyncDfsMetadataJournalReplicationTransport,
    options: DfsMetadataJournalReplicationOptions,
    outcomes: &mut Vec<DfsMetadataJournalReplicationOutcome>,
) -> Result<(), DfsError> {
    validate_prepared_caught_up_probe(&target)?;
    let DfsPreparedMetadataJournalTargetPublish {
        target_node_id,
        file_system_id,
        shard_id,
        shard_epoch,
        mut after_sequence,
        commands,
        backlog_after_sequence,
        caught_up_probe,
    } = target;
    if !transport.has_metadata_replication_route(&target_node_id) {
        outcomes.push(DfsMetadataJournalReplicationOutcome::MissingRoute {
            target_node_id,
            shard_id,
            after_sequence,
        });
        return Ok(());
    }

    let target_started_at = Instant::now();
    let mut completed_plan = true;
    for command in commands {
        let Some(attempt_timeout) =
            dfs_metadata_replication_attempt_timeout(options, target_started_at)
        else {
            if caught_up_probe {
                outcomes.push(DfsMetadataJournalReplicationOutcome::Failed {
                    target_node_id: target_node_id.clone(),
                    shard_id: shard_id.clone(),
                    reason: "caught-up metadata probe exhausted its target budget".to_string(),
                });
            } else {
                outcomes.push(DfsMetadataJournalReplicationOutcome::BacklogRemaining {
                    target_node_id: target_node_id.clone(),
                    shard_id: shard_id.clone(),
                    after_sequence,
                    max_batches: options.max_batches_per_target,
                });
            }
            completed_plan = false;
            break;
        };
        let response = match transport
            .send_metadata_journal_batch_async(&target_node_id, &command, attempt_timeout)
            .await
        {
            Ok(response) => response,
            Err(error) => {
                push_dfs_metadata_transport_error_outcome(
                    outcomes,
                    &target_node_id,
                    &shard_id,
                    after_sequence,
                    error,
                );
                completed_plan = false;
                break;
            }
        };
        if let Err(reason) =
            validate_dfs_metadata_journal_replication_response(&command, &shard_id, &response)
        {
            outcomes.push(DfsMetadataJournalReplicationOutcome::Failed {
                target_node_id: target_node_id.clone(),
                shard_id: shard_id.clone(),
                reason,
            });
            completed_plan = false;
            break;
        }
        let verified_caught_up_probe = !caught_up_probe || response.verified_prefix_only;
        match response.result {
            DfsMetadataJournalBatchResult::Applied => {
                if !verified_caught_up_probe {
                    outcomes.push(DfsMetadataJournalReplicationOutcome::Failed {
                        target_node_id: target_node_id.clone(),
                        shard_id: shard_id.clone(),
                        reason: "metadata replica did not verify the exact caught-up prefix"
                            .to_string(),
                    });
                    completed_plan = false;
                    break;
                }
                let next_after_sequence = command.batch.next_after_sequence;
                put_local_dfs_metadata_replication_cursor_async(
                    data_dir.clone(),
                    target_node_id.clone(),
                    shard_id.clone(),
                    next_after_sequence,
                )
                .await?;
                after_sequence = next_after_sequence;
                if caught_up_probe {
                    outcomes.push(DfsMetadataJournalReplicationOutcome::NoChanges {
                        target_node_id: target_node_id.clone(),
                        shard_id: shard_id.clone(),
                        after_sequence: next_after_sequence,
                    });
                } else {
                    outcomes.push(DfsMetadataJournalReplicationOutcome::Applied {
                        target_node_id: target_node_id.clone(),
                        shard_id: shard_id.clone(),
                        entries: command.batch.entries.len(),
                        next_after_sequence,
                    });
                }
            }
            DfsMetadataJournalBatchResult::Rejected { reason } => {
                if metadata_journal_rejection_allows_snapshot_repair(&reason) {
                    execute_prepared_dfs_metadata_snapshot_repair_async(
                        data_dir,
                        local_node_id,
                        &target_node_id,
                        &shard_id,
                        shard_epoch,
                        &file_system_id,
                        after_sequence,
                        reason,
                        transport,
                        options,
                        target_started_at,
                        outcomes,
                    )
                    .await?;
                } else {
                    record_dfs_metadata_replication_topology_mismatch_async(
                        data_dir,
                        &target_node_id,
                        &shard_id,
                        shard_epoch,
                        &reason,
                    )
                    .await?;
                    outcomes.push(DfsMetadataJournalReplicationOutcome::Rejected {
                        target_node_id: target_node_id.clone(),
                        shard_id: shard_id.clone(),
                        reason,
                    });
                }
                completed_plan = false;
                break;
            }
        }
    }

    if completed_plan {
        if let Some(after_sequence) = backlog_after_sequence {
            outcomes.push(DfsMetadataJournalReplicationOutcome::BacklogRemaining {
                target_node_id,
                shard_id,
                after_sequence,
                max_batches: options.max_batches_per_target,
            });
        }
    }
    Ok(())
}

async fn execute_prepared_dfs_metadata_snapshot_repair_async(
    data_dir: &SinomoDataDir,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
    shard_id: &DfsMetadataShardId,
    shard_epoch: u64,
    file_system_id: &DfsFileSystemId,
    after_sequence: u64,
    reason: String,
    transport: &dyn AsyncDfsMetadataJournalReplicationTransport,
    options: DfsMetadataJournalReplicationOptions,
    target_started_at: Instant,
    outcomes: &mut Vec<DfsMetadataJournalReplicationOutcome>,
) -> Result<(), DfsError> {
    let mut command = build_local_dfs_metadata_snapshot_install_command_async(
        data_dir.clone(),
        local_node_id.clone(),
        target_node_id.clone(),
        file_system_id.clone(),
        shard_epoch,
        shard_id.clone(),
        reason.clone(),
    )
    .await?;
    bind_pending_primary_handoff_snapshot_for_cluster_target(
        data_dir,
        target_node_id,
        &mut command,
    )?;
    let Some(attempt_timeout) =
        dfs_metadata_replication_attempt_timeout(options, target_started_at)
    else {
        outcomes.push(DfsMetadataJournalReplicationOutcome::BacklogRemaining {
            target_node_id: target_node_id.clone(),
            shard_id: shard_id.clone(),
            after_sequence,
            max_batches: options.max_batches_per_target,
        });
        return Ok(());
    };
    let response = match transport
        .send_metadata_snapshot_install_async(target_node_id, &command, attempt_timeout)
        .await
    {
        Ok(response) => response,
        Err(error) => {
            push_dfs_metadata_snapshot_transport_error_outcome(
                outcomes,
                target_node_id,
                shard_id,
                after_sequence,
                reason,
                error,
            );
            return Ok(());
        }
    };
    if let Err(reason) =
        validate_dfs_metadata_snapshot_install_replication_response(&command, &response)
    {
        outcomes.push(DfsMetadataJournalReplicationOutcome::Failed {
            target_node_id: target_node_id.clone(),
            shard_id: shard_id.clone(),
            reason,
        });
        return Ok(());
    }
    match response.result {
        DfsMetadataSnapshotInstallResult::Installed { archived_existing } => {
            put_local_dfs_metadata_replication_cursor_async(
                data_dir.clone(),
                target_node_id.clone(),
                shard_id.clone(),
                command.next_after_sequence,
            )
            .await?;
            outcomes.push(DfsMetadataJournalReplicationOutcome::SnapshotInstalled {
                target_node_id: target_node_id.clone(),
                shard_id: shard_id.clone(),
                next_after_sequence: command.next_after_sequence,
                archived_existing,
            });
        }
        DfsMetadataSnapshotInstallResult::Rejected {
            reason: snapshot_reason,
        } => {
            let reason = format!("{reason}; snapshot repair rejected: {snapshot_reason}");
            record_dfs_metadata_replication_topology_mismatch_async(
                data_dir,
                target_node_id,
                shard_id,
                shard_epoch,
                &reason,
            )
            .await?;
            outcomes.push(DfsMetadataJournalReplicationOutcome::Rejected {
                target_node_id: target_node_id.clone(),
                shard_id: shard_id.clone(),
                reason,
            });
        }
    }
    Ok(())
}

fn bind_pending_primary_handoff_snapshot_for_cluster_target(
    data_dir: &SinomoDataDir,
    target_node_id: &NodeId,
    command: &mut DfsMetadataSnapshotInstallCommand,
) -> Result<(), DfsError> {
    let cluster_paths = ClusterPaths::new(data_dir);
    let Some(context) =
        cluster_dfs_metadata_replica_shard_context(&cluster_paths, &command.shard_id)?
    else {
        return Ok(());
    };
    bind_pending_primary_handoff_snapshot_for_target(&context, target_node_id, command);
    Ok(())
}

fn bind_pending_primary_handoff_snapshot_for_target(
    sender_context: &DfsMetadataReplicaShardContext,
    target_node_id: &NodeId,
    command: &mut DfsMetadataSnapshotInstallCommand,
) {
    if sender_context.local_node_id != command.source_node_id
        || sender_context.shard.shard_id != command.shard_id.0
    {
        return;
    }
    let Some(handoff) = sender_context.pending_membership_handoff.as_ref() else {
        return;
    };

    command.primary_handoff_id = Some(handoff.handoff_id.clone());
    let receiver_context = DfsMetadataReplicaShardContext {
        local_node_id: target_node_id.clone(),
        shard: sender_context.shard.clone(),
        pending_membership_handoff: Some(handoff.clone()),
    };
    if !matches!(
        validated_pending_primary_handoff_snapshot_id(&receiver_context, command),
        Ok(Some(_))
    ) {
        command.primary_handoff_id = None;
    }
}

async fn build_local_dfs_metadata_snapshot_install_command_async(
    data_dir: SinomoDataDir,
    local_node_id: NodeId,
    target_node_id: NodeId,
    file_system_id: DfsFileSystemId,
    authority_epoch: u64,
    shard_id: DfsMetadataShardId,
    reason: String,
) -> Result<DfsMetadataSnapshotInstallCommand, DfsError> {
    tokio::task::spawn_blocking(move || {
        build_local_dfs_metadata_snapshot_install_command(
            &data_dir,
            &local_node_id,
            &target_node_id,
            &file_system_id,
            authority_epoch,
            &shard_id,
            reason,
        )
    })
    .await
    .map_err(|source| DfsError::Metadata {
        message: format!("DFS metadata snapshot planning task failed: {source}"),
    })?
}

fn build_local_dfs_metadata_snapshot_install_command(
    data_dir: &SinomoDataDir,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
    file_system_id: &DfsFileSystemId,
    authority_epoch: u64,
    shard_id: &DfsMetadataShardId,
    reason: String,
) -> Result<DfsMetadataSnapshotInstallCommand, DfsError> {
    let mut backend = open_local_metadata_shard_backend(data_dir, shard_id.clone())?;
    dfs_metadata_snapshot_install_command(
        &mut backend,
        local_node_id,
        target_node_id,
        file_system_id,
        authority_epoch,
        shard_id,
        reason,
    )
}

fn put_local_dfs_metadata_replication_cursor(
    data_dir: &SinomoDataDir,
    target_node_id: &NodeId,
    shard_id: &DfsMetadataShardId,
    next_after_sequence: u64,
) -> Result<(), DfsError> {
    // Cursor acknowledgement is independent of materialized metadata tables.
    // Keep the same shard write lock and durable monotonic cursor update
    // without loading unrelated table state.
    let root = local_dfs_metadata_shard_path(data_dir, shard_id);
    let _lock = metadata_lock::metadata_write_lock(
        root.clone(),
        metadata_cursor::journal_write_lock_path(&root),
    )?;
    Ok(metadata_cursor::put_metadata_replication_cursor(
        &root,
        target_node_id,
        shard_id,
        next_after_sequence,
    )?)
}

async fn put_local_dfs_metadata_replication_cursor_async(
    data_dir: SinomoDataDir,
    target_node_id: NodeId,
    shard_id: DfsMetadataShardId,
    next_after_sequence: u64,
) -> Result<(), DfsError> {
    tokio::task::spawn_blocking(move || {
        put_local_dfs_metadata_replication_cursor(
            &data_dir,
            &target_node_id,
            &shard_id,
            next_after_sequence,
        )
    })
    .await
    .map_err(|source| DfsError::Metadata {
        message: format!("DFS metadata cursor commit task failed: {source}"),
    })?
}

pub fn publish_dfs_metadata_journal_with_host(
    file_system_id: DfsFileSystemId,
    local_node_id: &NodeId,
    mut shards: Vec<DfsMetadataShardConfig>,
    host: &mut impl DfsMetadataJournalPublisherHost,
    transport: &mut dyn DfsMetadataJournalReplicationTransport,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<DfsMetadataJournalReplicationReport, DfsError> {
    if let Some(data_dir) = host.local_data_dir().cloned() {
        let metadata_suppressions = read_dfs_metadata_replication_suppression_snapshot(
            &default_dfs_metadata_replication_suppression_snapshot_path(&data_dir),
        )?
        .map(|snapshot| snapshot.records)
        .unwrap_or_default();
        let (outcomes, targets) = prepare_dfs_metadata_journal_publish_with_host(
            file_system_id,
            local_node_id,
            shards,
            host,
            &metadata_suppressions,
            options,
        )?;
        return execute_prepared_dfs_metadata_journal_publish(
            DfsPreparedClusterMetadataJournalPublish {
                data_dir,
                local_node_id: local_node_id.clone(),
                peer_node_ids: Vec::new(),
                outcomes,
                targets,
            },
            transport,
            options,
        );
    }

    shards.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
    if shards.is_empty() {
        return Ok(DfsMetadataJournalReplicationReport {
            outcomes: vec![DfsMetadataJournalReplicationOutcome::MissingShard {
                file_system_id,
                shard_id: DfsMetadataShardId::new("root"),
            }],
        });
    }

    let mut outcomes = Vec::new();
    for shard in shards {
        let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
        if shard.primary_node_id != *local_node_id {
            outcomes.push(DfsMetadataJournalReplicationOutcome::NotPrimary {
                file_system_id: shard.file_system_id,
                shard_id,
                local_node_id: local_node_id.clone(),
                primary_node_id: shard.primary_node_id,
                epoch: shard.epoch,
            });
            continue;
        }

        let mut backend = host.open_primary_shard_backend(&shard_id)?;
        for target_node_id in shard
            .replica_node_ids
            .into_iter()
            .filter(|target_node_id| target_node_id != local_node_id)
        {
            let after_sequence =
                host.metadata_replication_cursor(&mut *backend, &target_node_id, &shard_id)?;
            outcomes.extend(replicate_dfs_metadata_shard_to_target(
                &mut *backend,
                local_node_id,
                &target_node_id,
                &shard.file_system_id,
                shard.epoch,
                &shard_id,
                after_sequence,
                transport,
                options,
            )?);
        }
    }

    Ok(DfsMetadataJournalReplicationReport { outcomes })
}

pub async fn publish_dfs_metadata_journal_with_async_host(
    file_system_id: DfsFileSystemId,
    local_node_id: &NodeId,
    shards: Vec<DfsMetadataShardConfig>,
    data_dir: &SinomoDataDir,
    host: &mut impl DfsMetadataJournalPublisherHost,
    transport: &mut dyn AsyncDfsMetadataJournalReplicationTransport,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<DfsMetadataJournalReplicationReport, DfsError> {
    publish_dfs_metadata_journal_with_async_host_and_execution_exclusions(
        file_system_id,
        local_node_id,
        shards,
        data_dir,
        host,
        transport,
        &DfsMaintenanceExecutionExclusions::default(),
        options,
    )
    .await
}

pub async fn publish_dfs_metadata_journal_with_async_host_and_execution_exclusions(
    file_system_id: DfsFileSystemId,
    local_node_id: &NodeId,
    shards: Vec<DfsMetadataShardConfig>,
    data_dir: &SinomoDataDir,
    host: &mut impl DfsMetadataJournalPublisherHost,
    transport: &mut dyn AsyncDfsMetadataJournalReplicationTransport,
    execution_exclusions: &DfsMaintenanceExecutionExclusions,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<DfsMetadataJournalReplicationReport, DfsError> {
    let metadata_suppressions = read_dfs_metadata_replication_suppression_snapshot(
        &default_dfs_metadata_replication_suppression_snapshot_path(data_dir),
    )?
    .map(|snapshot| snapshot.records)
    .unwrap_or_default();
    let (outcomes, targets) =
        prepare_dfs_metadata_journal_publish_with_host_and_execution_exclusions(
            file_system_id,
            local_node_id,
            shards,
            host,
            &metadata_suppressions,
            execution_exclusions,
            options,
        )?;
    execute_prepared_dfs_metadata_journal_publish_async(
        DfsPreparedClusterMetadataJournalPublish {
            data_dir: data_dir.clone(),
            local_node_id: local_node_id.clone(),
            peer_node_ids: Vec::new(),
            outcomes,
            targets,
        },
        transport,
        options,
    )
    .await
}

pub fn process_dfs_metadata_journal_batch(
    backend: &mut dyn DfsMetadataBackend,
    context: Option<&DfsMetadataReplicaShardContext>,
    command: DfsMetadataJournalBatchCommand,
) -> DfsMetadataJournalBatchResponse {
    if let Some(response) = validate_dfs_metadata_journal_batch_source(context, &command) {
        return response;
    }
    let shard_id = command.batch.shard_id.clone();
    let next_after_sequence = command.batch.next_after_sequence;
    if command.verify_prefix_only {
        return match verify_dfs_metadata_journal_prefix(backend, &command) {
            Ok(()) => DfsMetadataJournalBatchResponse {
                request_id: command.request_id,
                shard_id,
                next_after_sequence,
                applied_entries: 0,
                verified_prefix_only: true,
                verified_authority_probe_id: None,
                result: DfsMetadataJournalBatchResult::Applied,
            },
            Err(reason) => DfsMetadataJournalBatchResponse {
                request_id: command.request_id,
                shard_id,
                next_after_sequence,
                applied_entries: 0,
                verified_prefix_only: false,
                verified_authority_probe_id: None,
                result: DfsMetadataJournalBatchResult::Rejected { reason },
            },
        };
    }
    if command.authority_probe.is_some()
        && let Err(reason) = validate_contiguous_metadata_authority_batch(backend, &command.batch)
    {
        return dfs_metadata_journal_rejection_response(&command, reason);
    }
    let context = context.expect("validated metadata replica ingest has a shard context");
    let permit = DfsMetadataReplicaIngestPermit::new_validated(
        context.shard.file_system_id.clone(),
        shard_id.clone(),
        context.shard.primary_node_id.clone(),
        context.shard.epoch,
    );
    match backend.apply_journal_batch_with_replica_permit(&permit, &command.batch) {
        Ok(applied_entries) => {
            let verified_authority_probe_id = if let Some(probe) = command.authority_probe.as_ref()
            {
                let confirmation_id =
                    match crate::service_replica::verify_persisted_metadata_authority_probe(
                        backend,
                        &context.shard,
                        probe,
                    ) {
                        Ok(confirmation_id) => confirmation_id,
                        Err(error) => {
                            return dfs_metadata_journal_rejection_response(
                                &command,
                                error.to_string(),
                            );
                        }
                    };
                if let Err(error) =
                    backend.put_metadata_authority_ack_floor(&crate::DfsMetadataAuthorityAckFloor {
                        shard_id: command.batch.shard_id.clone(),
                        journal_sequence: command.batch.next_after_sequence,
                    })
                {
                    return dfs_metadata_journal_rejection_response(
                        &command,
                        format!(
                            "persist metadata authority acknowledgement floor for shard {} sequence {}: {error}",
                            command.batch.shard_id.0, command.batch.next_after_sequence
                        ),
                    );
                }
                Some(confirmation_id)
            } else {
                None
            };
            DfsMetadataJournalBatchResponse {
                request_id: command.request_id,
                shard_id,
                next_after_sequence,
                applied_entries,
                verified_prefix_only: false,
                // Echo the prefix-bound confirmation only after the exact
                // hidden operation and complete primary history are durable.
                verified_authority_probe_id,
                result: DfsMetadataJournalBatchResult::Applied,
            }
        }
        Err(error) => DfsMetadataJournalBatchResponse {
            request_id: command.request_id,
            shard_id,
            next_after_sequence,
            applied_entries: 0,
            verified_prefix_only: false,
            verified_authority_probe_id: None,
            result: DfsMetadataJournalBatchResult::Rejected {
                reason: error.to_string(),
            },
        },
    }
}

fn validate_contiguous_metadata_authority_batch(
    backend: &dyn DfsMetadataBackend,
    batch: &DfsMetadataJournalBatch,
) -> Result<(), String> {
    let Some(last_entry) = batch.entries.last() else {
        return Err(format!(
            "metadata authority batch for shard {} must contain the exact hidden operation prepare",
            batch.shard_id.0
        ));
    };
    if last_entry.sequence != batch.next_after_sequence {
        return Err(format!(
            "metadata authority batch for shard {} ends at sequence {} but names sequence {}",
            batch.shard_id.0, last_entry.sequence, batch.next_after_sequence
        ));
    }
    let replayed_through_sequence =
        backend
            .metadata_replay_cursor(&batch.shard_id)
            .map_err(|error| {
                format!(
                    "read metadata replay cursor for authority batch shard {}: {error}",
                    batch.shard_id.0
                )
            })?;
    let mut previous_sequence: Option<u64> = None;
    let mut expected_unseen_sequence = replayed_through_sequence.saturating_add(1);
    for entry in &batch.entries {
        if entry.shard_id != batch.shard_id {
            return Err(format!(
                "metadata authority batch for shard {} contains entry from shard {}",
                batch.shard_id.0, entry.shard_id.0
            ));
        }
        if let Some(previous_sequence) = previous_sequence
            && entry.sequence != previous_sequence.saturating_add(1)
        {
            return Err(format!(
                "metadata authority batch for shard {} is not contiguous between sequences {} and {}",
                batch.shard_id.0, previous_sequence, entry.sequence
            ));
        }
        previous_sequence = Some(entry.sequence);
        if entry.sequence <= replayed_through_sequence {
            continue;
        }
        if entry.sequence != expected_unseen_sequence {
            return Err(format!(
                "metadata authority batch for shard {} starts unseen history at sequence {} but durable replay requires sequence {}",
                batch.shard_id.0, entry.sequence, expected_unseen_sequence
            ));
        }
        expected_unseen_sequence = expected_unseen_sequence.saturating_add(1);
    }
    Ok(())
}

fn verify_dfs_metadata_journal_prefix(
    backend: &mut dyn DfsMetadataBackend,
    command: &DfsMetadataJournalBatchCommand,
) -> Result<(), String> {
    let batch = &command.batch;
    let Some(probe) = command.prefix_probe.as_ref() else {
        return Err(
            "metadata journal prefix probe is missing its exact checkpoint identity".to_string(),
        );
    };
    if !batch.entries.is_empty() {
        return Err(
            "metadata journal prefix probe must carry an empty rolling-upgrade-safe batch"
                .to_string(),
        );
    }
    if probe.barrier_id.is_empty()
        || probe.state_fingerprint.is_empty()
        || probe.shard_id != batch.shard_id
        || probe.journal_sequence != batch.next_after_sequence
    {
        return Err(format!(
            "metadata journal prefix probe identity mismatch for shard {} sequence {}",
            batch.shard_id.0, batch.next_after_sequence
        ));
    }
    backend
        .refresh_metadata_shard_durable_view(&probe.shard_id)
        .map_err(|error| {
            format!(
                "refresh durable metadata view for prefix probe shard {} sequence {}: {error}",
                batch.shard_id.0, probe.journal_sequence
            )
        })?;
    let local_fingerprint = crate::service_sync::metadata_shard_state_fingerprint_at(
        backend,
        &probe.shard_id,
        probe.journal_sequence,
    )
    .map_err(|error| {
        format!(
            "metadata journal prefix probe state mismatch for shard {} sequence {}: {error}",
            batch.shard_id.0, probe.journal_sequence
        )
    })?;
    if local_fingerprint != probe.state_fingerprint {
        return Err(format!(
            "metadata journal prefix probe state mismatch for shard {} sequence {}",
            batch.shard_id.0, probe.journal_sequence
        ));
    }
    backend
        .put_metadata_prefix_ack_floor(&crate::DfsMetadataPrefixAckFloor {
            shard_id: probe.shard_id.clone(),
            journal_sequence: probe.journal_sequence,
            state_fingerprint: probe.state_fingerprint.clone(),
        })
        .map_err(|error| {
            format!(
                "persist metadata prefix acknowledgement floor for shard {} sequence {}: {error}",
                batch.shard_id.0, probe.journal_sequence
            )
        })?;
    Ok(())
}

pub fn cluster_dfs_metadata_replica_shard_context(
    cluster_paths: &ClusterPaths,
    shard_id: &DfsMetadataShardId,
) -> Result<Option<DfsMetadataReplicaShardContext>, DfsError> {
    if !cluster_paths.state.exists() {
        return Ok(None);
    }
    let state = sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths)
        .map_err(DfsError::from_network)?;
    // Replica ingress must validate against the newest durable view accepted
    // by this node. Non-voters can have a deliberately stale local Raft store
    // while their imported snapshot cache has already observed a newer shard
    // epoch; using only the local store here permanently rejects the current
    // primary and prevents snapshot repair from closing that gap.
    let shard = sinomo_core::cluster::read_cluster_kv_latest_observed_entry(
        cluster_paths,
        &metadata_shard_key(&sifs_file_system_id(), &shard_id.0),
    )
    .map_err(DfsError::from_network)?
    .map(|entry| entry.decode::<DfsMetadataShardConfig>())
    .transpose()?;
    let pending_membership_handoff = match shard.as_ref() {
        Some(shard) => sinomo_core::cluster::read_cluster_kv_latest_observed_entry(
            cluster_paths,
            &metadata_shard_membership_handoff_key(&shard.file_system_id, &shard.shard_id),
        )
        .map_err(DfsError::from_network)?
        .map(|entry| entry.decode::<DfsMetadataShardMembershipHandoff>())
        .transpose()?,
        None => None,
    };
    Ok(shard.map(|shard| DfsMetadataReplicaShardContext {
        local_node_id: state.local_node_id,
        pending_membership_handoff: pending_membership_handoff.filter(|handoff| {
            handoff.retiring_shard == shard
                && handoff.desired_shard.file_system_id == shard.file_system_id
                && handoff.desired_shard.shard_id == shard.shard_id
                && handoff.desired_shard.epoch == shard.epoch.saturating_add(1)
        }),
        shard,
    }))
}

pub fn process_cluster_dfs_metadata_journal_batch(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    authenticated_source_node_id: &NodeId,
    command: DfsMetadataJournalBatchCommand,
) -> Result<DfsMetadataJournalBatchResponse, DfsError> {
    let context =
        cluster_dfs_metadata_replica_shard_context(cluster_paths, &command.batch.shard_id)?;
    process_cluster_dfs_metadata_journal_batch_with_context(
        data_dir,
        authenticated_source_node_id,
        command,
        context,
    )
}

pub(crate) fn process_cluster_dfs_metadata_journal_batch_with_context(
    data_dir: &SinomoDataDir,
    authenticated_source_node_id: &NodeId,
    command: DfsMetadataJournalBatchCommand,
    context: Option<DfsMetadataReplicaShardContext>,
) -> Result<DfsMetadataJournalBatchResponse, DfsError> {
    if command.source_node_id.as_ref() != Some(authenticated_source_node_id) {
        return Ok(dfs_metadata_journal_rejection_response(
            &command,
            format!(
                "authenticated metadata journal caller {authenticated_source_node_id} does not match claimed source {}",
                command
                    .source_node_id
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "<missing>".to_string())
            ),
        ));
    }
    if let Some(response) = validate_dfs_metadata_journal_batch_source(context.as_ref(), &command) {
        return Ok(response);
    }
    data_dir
        .ensure_root_layout()
        .map_err(|source| DfsError::Metadata {
            message: source.to_string(),
        })?;
    let backend = match cached_cluster_metadata_backend(data_dir, &command.batch.shard_id) {
        Ok(backend) => backend,
        Err(CachedClusterMetadataBackendError::CachePoisoned) => {
            return Err(DfsError::Metadata {
                message: "cluster metadata backend cache lock is poisoned".to_string(),
            });
        }
        Err(CachedClusterMetadataBackendError::Open(error))
            if metadata_backend_open_error_allows_snapshot_repair(&error) =>
        {
            return Ok(dfs_metadata_journal_rejection_response(
                &command,
                format!(
                    "{DFS_METADATA_UNREADABLE_REPLICA_SNAPSHOT_REPAIR_REASON} for shard {}: {error}",
                    command.batch.shard_id.0
                ),
            ));
        }
        Err(CachedClusterMetadataBackendError::Open(error)) => return Err(error.into()),
    };
    let mut backend = backend.lock().map_err(|_| DfsError::Metadata {
        message: format!(
            "cluster metadata backend lock is poisoned for shard {}",
            command.batch.shard_id.0
        ),
    })?;
    Ok(process_dfs_metadata_journal_batch(
        &mut *backend,
        context.as_ref(),
        command,
    ))
}

pub fn process_dfs_metadata_snapshot_install(
    backend: &mut dyn DfsMetadataBackend,
    context: Option<&DfsMetadataReplicaShardContext>,
    command: DfsMetadataSnapshotInstallCommand,
) -> DfsMetadataSnapshotInstallResponse {
    if let Some(response) = validate_dfs_metadata_snapshot_install_source(context, &command) {
        return response;
    }
    let request_id = command.request_id.clone();
    let shard_id = command.shard_id.clone();
    let next_after_sequence = command.next_after_sequence;
    match validate_dfs_metadata_snapshot_install_state(backend, &command) {
        Ok(Some(reason)) => {
            return DfsMetadataSnapshotInstallResponse {
                request_id,
                shard_id,
                next_after_sequence,
                result: DfsMetadataSnapshotInstallResult::Rejected { reason },
            };
        }
        Err(error) => {
            return DfsMetadataSnapshotInstallResponse {
                request_id,
                shard_id,
                next_after_sequence,
                result: DfsMetadataSnapshotInstallResult::Rejected {
                    reason: error.to_string(),
                },
            };
        }
        Ok(None) => {}
    }
    let context = context.expect("validated metadata snapshot ingest has a shard context");
    let pending_primary_handoff_id =
        match validated_pending_primary_handoff_snapshot_id(context, &command) {
            Ok(handoff_id) => handoff_id,
            Err(reason) => {
                return DfsMetadataSnapshotInstallResponse {
                    request_id,
                    shard_id,
                    next_after_sequence,
                    result: DfsMetadataSnapshotInstallResult::Rejected { reason },
                };
            }
        };
    let permit = if let Some(handoff_id) = pending_primary_handoff_id {
        DfsMetadataReplicaIngestPermit::new_validated_pending_primary_handoff(
            context.shard.file_system_id.clone(),
            shard_id.clone(),
            context.shard.primary_node_id.clone(),
            context.shard.epoch,
            handoff_id,
        )
    } else {
        DfsMetadataReplicaIngestPermit::new_validated(
            context.shard.file_system_id.clone(),
            shard_id.clone(),
            context.shard.primary_node_id.clone(),
            context.shard.epoch,
        )
    };
    match backend.replace_with_replicated_snapshot_with_replica_permit(
        &permit,
        command.shard_id,
        command.next_after_sequence,
        command.snapshot,
    ) {
        Ok(archived_existing) => DfsMetadataSnapshotInstallResponse {
            request_id,
            shard_id,
            next_after_sequence,
            result: DfsMetadataSnapshotInstallResult::Installed { archived_existing },
        },
        Err(error) => DfsMetadataSnapshotInstallResponse {
            request_id,
            shard_id,
            next_after_sequence,
            result: DfsMetadataSnapshotInstallResult::Rejected {
                reason: error.to_string(),
            },
        },
    }
}

pub(crate) fn validate_dfs_metadata_snapshot_install_state(
    backend: &dyn DfsMetadataBackend,
    command: &DfsMetadataSnapshotInstallCommand,
) -> Result<Option<String>, DfsError> {
    if command.snapshot.materialized_through_sequence != Some(command.next_after_sequence) {
        return Ok(Some(format!(
            "metadata snapshot embedded sequence {:?} does not match install sequence {} for shard {}",
            command.snapshot.materialized_through_sequence,
            command.next_after_sequence,
            command.shard_id.0
        )));
    }
    if let Some(proof) = command.snapshot.prefix_proof.as_ref() {
        if !proof.is_well_formed() || proof.journal_sequence != command.next_after_sequence {
            return Ok(Some(format!(
                "metadata snapshot prefix proof does not name exact install sequence {} for shard {}",
                command.next_after_sequence, command.shard_id.0
            )));
        }
        let Some(expected_semantic_fingerprint) =
            command.snapshot.semantic_state_fingerprint.as_ref()
        else {
            return Ok(Some(format!(
                "metadata snapshot prefix proof for shard {} sequence {} is missing its semantic checksum",
                command.shard_id.0, command.next_after_sequence
            )));
        };
        let actual_semantic_fingerprint =
            crate::metadata_records::dfs_metadata_table_snapshot_state_fingerprint(
                &command.snapshot,
            )
            .map_err(|source| DfsError::Serialization {
                message: format!(
                    "encode metadata snapshot semantic checksum for {}: {source}",
                    command.shard_id.0
                ),
            })?;
        if &actual_semantic_fingerprint != expected_semantic_fingerprint {
            return Ok(Some(format!(
                "metadata snapshot semantic checksum does not match its prefix proof transfer for shard {} sequence {}",
                command.shard_id.0, command.next_after_sequence
            )));
        }
    }
    if let Some(authority_floor) = backend.metadata_authority_ack_floor(&command.shard_id)?
        && command.next_after_sequence <= authority_floor.journal_sequence
    {
        return Ok(Some(format!(
            "metadata snapshot sequence {} would replace shard {} at or behind locally acknowledged authority operation {}",
            command.next_after_sequence, command.shard_id.0, authority_floor.journal_sequence
        )));
    }
    let Some(floor) = backend.metadata_prefix_ack_floor(&command.shard_id)? else {
        return Ok(None);
    };
    if command.next_after_sequence < floor.journal_sequence {
        return Ok(Some(format!(
            "metadata snapshot sequence {} would roll shard {} behind locally acknowledged checkpoint {}",
            command.next_after_sequence, command.shard_id.0, floor.journal_sequence
        )));
    }
    if command.next_after_sequence == floor.journal_sequence {
        let matches = crate::metadata_records::dfs_metadata_table_snapshot_matches_checkpoint(
            &command.snapshot,
            command.next_after_sequence,
            &floor.state_fingerprint,
        )
        .map_err(|source| DfsError::Serialization {
            message: format!(
                "encode metadata snapshot local acknowledgement check for {}: {source}",
                command.shard_id.0
            ),
        })?;
        if !matches {
            return Ok(Some(format!(
                "metadata snapshot state does not match locally acknowledged checkpoint for shard {} sequence {}",
                command.shard_id.0, command.next_after_sequence
            )));
        }
    }
    Ok(None)
}

pub fn process_cluster_dfs_metadata_snapshot_install(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    authenticated_source_node_id: &NodeId,
    command: DfsMetadataSnapshotInstallCommand,
) -> Result<DfsMetadataSnapshotInstallResponse, DfsError> {
    let context = cluster_dfs_metadata_replica_shard_context(cluster_paths, &command.shard_id)?;
    process_cluster_dfs_metadata_snapshot_install_with_context(
        data_dir,
        authenticated_source_node_id,
        command,
        context,
    )
}

pub(crate) fn process_cluster_dfs_metadata_snapshot_install_with_context(
    data_dir: &SinomoDataDir,
    authenticated_source_node_id: &NodeId,
    command: DfsMetadataSnapshotInstallCommand,
    context: Option<DfsMetadataReplicaShardContext>,
) -> Result<DfsMetadataSnapshotInstallResponse, DfsError> {
    if command.source_node_id != *authenticated_source_node_id {
        return Ok(DfsMetadataSnapshotInstallResponse {
            request_id: command.request_id,
            shard_id: command.shard_id,
            next_after_sequence: command.next_after_sequence,
            result: DfsMetadataSnapshotInstallResult::Rejected {
                reason: format!(
                    "authenticated metadata snapshot caller {authenticated_source_node_id} does not match claimed source {}",
                    command.source_node_id
                ),
            },
        });
    }
    if let Some(response) =
        validate_dfs_metadata_snapshot_install_source(context.as_ref(), &command)
    {
        return Ok(response);
    }
    data_dir
        .ensure_root_layout()
        .map_err(|source| DfsError::Metadata {
            message: source.to_string(),
        })?;
    let root = local_dfs_metadata_shard_path(data_dir, &command.shard_id);
    match LocalSobjMetadataBackend::new(command.shard_id.clone(), &root) {
        Ok(mut backend) => Ok(process_dfs_metadata_snapshot_install(
            &mut backend,
            context.as_ref(),
            command,
        )),
        Err(open_error) if metadata_backend_open_error_allows_snapshot_repair(&open_error) => {
            let request_id = command.request_id.clone();
            let shard_id = command.shard_id.clone();
            let next_after_sequence = command.next_after_sequence;
            let context = context
                .as_ref()
                .expect("validated metadata snapshot ingest has a shard context");
            let permit = DfsMetadataReplicaIngestPermit::new_validated(
                context.shard.file_system_id.clone(),
                shard_id.clone(),
                context.shard.primary_node_id.clone(),
                context.shard.epoch,
            );
            let result = LocalSobjMetadataBackend::replace_unreadable_with_replicated_snapshot(
                &permit,
                shard_id.clone(),
                root,
                command.shard_id,
                command.next_after_sequence,
                command.snapshot,
            );
            let result = match result {
                Ok((_backend, archived_existing)) => {
                    DfsMetadataSnapshotInstallResult::Installed { archived_existing }
                }
                Err(repair_error) => DfsMetadataSnapshotInstallResult::Rejected {
                    reason: format!(
                        "cannot open metadata shard for snapshot install: {open_error}; unreadable-shard snapshot replacement failed: {repair_error}"
                    ),
                },
            };
            Ok(DfsMetadataSnapshotInstallResponse {
                request_id,
                shard_id,
                next_after_sequence,
                result,
            })
        }
        Err(open_error) => Err(open_error.into()),
    }
}

pub fn materialize_default_sifs_metadata(
    backend: &mut dyn DfsMetadataBackend,
    file_system: &DfsFileSystem,
    bootstrap_owner_node_ids: impl IntoIterator<Item = NodeId>,
    fallback_owner_node_id: NodeId,
    created_at: OffsetDateTime,
) -> Result<(), DfsError> {
    backend.ensure_file_system(file_system)?;
    materialize_default_sifs_metadata_objects(
        backend,
        distributed_sifs_bootstrap_objects_for_nodes(
            bootstrap_owner_node_ids,
            fallback_owner_node_id,
        ),
        created_at,
    )
}

fn materialize_default_sifs_metadata_objects(
    backend: &mut dyn DfsMetadataBackend,
    objects: impl IntoIterator<Item = crate::DfsObjectMetadata>,
    created_at: OffsetDateTime,
) -> Result<(), DfsError> {
    for object in objects {
        if backend
            .get_object_by_path(&object.file_system_id, object.path.trim_matches('/'))?
            .is_none()
        {
            backend.put_object(&object)?;
            backend.put_version(&DfsObjectVersionRecord {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                version: object.version.clone(),
                content_hash: object.content_hash.clone(),
                size_bytes: object.content_size_bytes,
                locations: object.current_locations.clone(),
                created_by_event_id: object.created_by_event_id.clone(),
                created_at,
                visible: true,
            })?;
        }
    }
    Ok(())
}

/// Materialize only bootstrap objects whose committed shard authority is a
/// currently writable local primary.
///
/// The full topology is used for routing, but only the selected local-primary
/// backends are opened. This prevents a node which happens to own one default
/// shard from appending bootstrap records to another node's primary journal.
/// Shards parked behind a membership handoff are omitted by the caller and are
/// revisited when the handoff generation becomes active.
#[cfg(test)]
pub(crate) fn materialize_default_sifs_metadata_on_active_primary_shards(
    metadata_root: impl Into<PathBuf>,
    file_system: &DfsFileSystem,
    all_shards: &[DfsMetadataShardConfig],
    active_primary_shard_ids: &BTreeSet<DfsMetadataShardId>,
    local_node_id: &NodeId,
    bootstrap_owner_node_ids: impl IntoIterator<Item = NodeId>,
    fallback_owner_node_id: NodeId,
    created_at: OffsetDateTime,
) -> Result<DfsDefaultSifsMaterializationReport, DfsError> {
    let metadata_root = metadata_root.into();
    let bootstrap_owner_node_ids = bootstrap_owner_node_ids.into_iter().collect::<Vec<_>>();
    let local_primary_shards = all_shards
        .iter()
        .filter(|shard| {
            shard.primary_node_id == *local_node_id
                && active_primary_shard_ids
                    .contains(&DfsMetadataShardId::new(shard.shard_id.clone()))
        })
        .cloned()
        .collect::<Vec<_>>();
    let has_continuation = default_sifs_migration_has_backend_continuation(
        &metadata_root,
        &file_system.file_system_id,
        local_node_id,
        all_shards,
        active_primary_shard_ids,
    );
    let bound_primary_backend = if local_primary_shards.is_empty() || has_continuation {
        None
    } else {
        Some(LocalShardedMetadataBackend::new(
            metadata_root.clone(),
            local_primary_shards,
        )?)
    };
    materialize_default_sifs_metadata_on_bound_primary_shards(
        metadata_root,
        bound_primary_backend,
        file_system,
        all_shards,
        active_primary_shard_ids,
        local_node_id,
        bootstrap_owner_node_ids,
        fallback_owner_node_id,
        created_at,
    )
}

/// Materialize default metadata through the exact shard backend instances
/// whose current authority generation was validated by the caller.
///
/// A completed membership handoff binds authority to one in-memory backend
/// instance. Durable activation markers deliberately do not grant authority
/// to another instance opened on the same path, so neither bootstrap writes
/// nor bounded cross-shard migration may reopen a validated primary.
pub(crate) fn materialize_default_sifs_metadata_on_bound_primary_shards(
    metadata_root: impl Into<PathBuf>,
    bound_primary_backend: Option<LocalShardedMetadataBackend>,
    file_system: &DfsFileSystem,
    all_shards: &[DfsMetadataShardConfig],
    active_primary_shard_ids: &BTreeSet<DfsMetadataShardId>,
    local_node_id: &NodeId,
    bootstrap_owner_node_ids: impl IntoIterator<Item = NodeId>,
    fallback_owner_node_id: NodeId,
    created_at: OffsetDateTime,
) -> Result<DfsDefaultSifsMaterializationReport, DfsError> {
    let metadata_root = metadata_root.into();
    let expected_bound_shard_ids = all_shards
        .iter()
        .filter(|shard| {
            shard.file_system_id == file_system.file_system_id
                && shard.primary_node_id == *local_node_id
                && active_primary_shard_ids
                    .contains(&DfsMetadataShardId::new(shard.shard_id.clone()))
        })
        .map(|shard| DfsMetadataShardId::new(shard.shard_id.clone()))
        .collect::<BTreeSet<_>>();
    // Taking the continuation is also the ownership revocation path. If this
    // node lost every active primary, drop the retained authority-bound
    // backends before returning instead of leaving their hydrated tables and
    // writer capabilities parked in the process registry.
    let continuation = take_default_sifs_migration_backend_continuation(
        &metadata_root,
        &file_system.file_system_id,
        local_node_id,
        all_shards,
        active_primary_shard_ids,
    );
    if expected_bound_shard_ids.is_empty() {
        if bound_primary_backend.is_some() {
            return Err(DfsError::Metadata {
                message: "default SIFS materialization received a writable backend without a locally active primary shard"
                    .to_string(),
            });
        }
        return Ok(DfsDefaultSifsMaterializationReport {
            migration: DfsMetadataShardMigrationReport {
                file_system_id: file_system.file_system_id.clone(),
                scanned_objects: 0,
                indexed_objects: 0,
                migrated_objects: 0,
                skipped_stale_sources: 0,
                reached_limit: false,
            },
            awaiting_replica_progress: false,
        });
    }
    let (bound_primary_backends, retained_replica_backends) = if let Some(continuation) =
        continuation
    {
        if bound_primary_backend.is_some() {
            return Err(DfsError::Metadata {
                message: "default SIFS materialization reopened primary backends while a bound continuation was available"
                    .to_string(),
            });
        }
        (continuation.primary_backends, continuation.replica_backends)
    } else {
        let mut backend = bound_primary_backend.ok_or_else(|| DfsError::Metadata {
            message: format!(
                "default SIFS materialization is missing bound backend capability for locally active shards {:?}",
                expected_bound_shard_ids
            ),
        })?;
        let bound_shard_ids = backend.shard_ids().into_iter().collect::<BTreeSet<_>>();
        if bound_shard_ids != expected_bound_shard_ids {
            return Err(DfsError::Metadata {
                message: format!(
                    "default SIFS materialization bound backend shards {:?} do not match locally active shards {:?}",
                    bound_shard_ids, expected_bound_shard_ids
                ),
            });
        }

        let objects = distributed_sifs_bootstrap_objects_for_nodes(
            bootstrap_owner_node_ids,
            fallback_owner_node_id,
        )
        .into_iter()
        .map(|object| {
            let selected = crate::control_plane::select_metadata_shard_config_for_path_strict_default_sifs_split(
                &object.file_system_id,
                all_shards,
                &object.path,
            )?
            .ok_or_else(|| sinomo_core::kv::KvError::Serialization {
                detail: format!(
                    "default SIFS bootstrap object {} has no committed metadata shard",
                    object.path
                ),
            })?;
            Ok((selected, object))
        })
        .collect::<Result<Vec<_>, sinomo_core::kv::KvError>>()?
        .into_iter()
        .filter_map(|(selected, object)| {
            let shard_id = DfsMetadataShardId::new(selected.shard_id.clone());
            (selected.primary_node_id == *local_node_id
                && active_primary_shard_ids.contains(&shard_id))
            .then_some(object)
        })
        .collect::<Vec<_>>();

        backend.ensure_file_system_on_all_shards(file_system)?;
        materialize_default_sifs_metadata_objects(&mut backend, objects, created_at)?;
        (backend.into_shard_backends(), BTreeMap::new())
    };

    let (report, continuation) = migrate_default_sifs_metadata_on_active_primary_shards(
        &metadata_root,
        bound_primary_backends,
        retained_replica_backends,
        file_system,
        all_shards,
        active_primary_shard_ids,
        local_node_id,
        DEFAULT_SIFS_BOOTSTRAP_SHARD_MIGRATION_MUTATION_LIMIT,
    )?;
    if report.migration.reached_limit {
        put_default_sifs_migration_backend_continuation(metadata_root, continuation);
    }
    Ok(report)
}

struct DfsDefaultSifsMigrationShardScan {
    shard_id: DfsMetadataShardId,
    buffered: VecDeque<crate::DfsObjectMetadata>,
    next_after: Option<crate::DfsObjectId>,
}

struct DfsDefaultSifsMigrationScanBatch {
    candidates_by_object:
        BTreeMap<crate::DfsObjectId, Vec<(DfsMetadataShardId, crate::DfsObjectMetadata)>>,
    scanned_object_ids: Vec<crate::DfsObjectId>,
    catalog_has_more: bool,
    awaiting_replica_progress: bool,
}

fn refill_default_sifs_migration_shard_scan(
    scan: &mut DfsDefaultSifsMigrationShardScan,
    backend: &LocalSobjMetadataBackend,
    file_system_id: &DfsFileSystemId,
    page_size: usize,
) -> Result<(), DfsError> {
    if !scan.buffered.is_empty() {
        return Ok(());
    }
    let Some(after_object_id) = scan.next_after.take() else {
        return Ok(());
    };
    let (objects, next_after) = backend.objects_for_file_system_by_id_page(
        file_system_id,
        Some(&after_object_id),
        page_size,
    )?;
    scan.buffered = objects.into();
    scan.next_after = next_after;
    Ok(())
}

fn collect_default_sifs_migration_scan_batch(
    shard_backends: &BTreeMap<DfsMetadataShardId, LocalSobjMetadataBackend>,
    file_system_id: &DfsFileSystemId,
    all_shards: &[DfsMetadataShardConfig],
    writable_shard_ids: &BTreeSet<DfsMetadataShardId>,
    certified_replica_shard_ids: &BTreeSet<DfsMetadataShardId>,
    local_replica_shard_ids: &BTreeSet<DfsMetadataShardId>,
    after_object_id: Option<&crate::DfsObjectId>,
    scan_limit: usize,
) -> Result<DfsDefaultSifsMigrationScanBatch, DfsError> {
    let page_size = DEFAULT_SIFS_BOOTSTRAP_SHARD_MIGRATION_PAGE_SIZE
        .min(scan_limit.saturating_add(1))
        .max(1);
    let mut scans = Vec::with_capacity(shard_backends.len());
    for (shard_id, backend) in shard_backends {
        let (objects, next_after) = backend.objects_for_file_system_by_id_page(
            file_system_id,
            after_object_id,
            page_size,
        )?;
        scans.push(DfsDefaultSifsMigrationShardScan {
            shard_id: shard_id.clone(),
            buffered: objects.into(),
            next_after,
        });
    }
    #[cfg(test)]
    observe_default_sifs_migration_buffered_records(
        scans.iter().map(|scan| scan.buffered.len()).sum(),
    );

    let mut candidates_by_object =
        BTreeMap::<crate::DfsObjectId, Vec<(DfsMetadataShardId, crate::DfsObjectMetadata)>>::new();
    let mut scanned_object_ids = Vec::with_capacity(scan_limit);
    let mut awaiting_replica_progress = false;
    while scanned_object_ids.len() < scan_limit {
        for scan in &mut scans {
            if scan.buffered.is_empty()
                && scan.next_after.is_some()
                && let Some(backend) = shard_backends.get(&scan.shard_id)
            {
                refill_default_sifs_migration_shard_scan(scan, backend, file_system_id, page_size)?;
            }
        }
        let Some(next_object_id) = scans
            .iter()
            .filter_map(|scan| scan.buffered.front().map(|object| object.object_id.clone()))
            .min()
        else {
            break;
        };
        scanned_object_ids.push(next_object_id.clone());
        for scan in &mut scans {
            if scan
                .buffered
                .front()
                .is_none_or(|object| object.object_id != next_object_id)
            {
                continue;
            }
            let object = scan
                .buffered
                .pop_front()
                .expect("front object was checked above");
            let source_usable = writable_shard_ids.contains(&scan.shard_id)
                || certified_replica_shard_ids.contains(&scan.shard_id);
            if !source_usable {
                // An assigned but uncertified replica is evidence only when
                // it contains a legacy object whose current route is one of
                // this node's writable authorities.
                if local_replica_shard_ids.contains(&scan.shard_id)
                    && object.kind != DfsObjectKind::Tombstone
                    && crate::control_plane::select_metadata_shard_config_for_path_strict_default_sifs_split(
                        &object.file_system_id,
                        all_shards,
                        &object.path,
                    )?
                    .is_some_and(|selected| {
                        let target_shard_id = DfsMetadataShardId::new(selected.shard_id);
                        target_shard_id != scan.shard_id
                            && writable_shard_ids.contains(&target_shard_id)
                    })
                {
                    awaiting_replica_progress = true;
                }
                continue;
            }
            candidates_by_object
                .entry(next_object_id.clone())
                .or_default()
                .push((scan.shard_id.clone(), object));
        }
        #[cfg(test)]
        observe_default_sifs_migration_buffered_records(
            scans.iter().map(|scan| scan.buffered.len()).sum::<usize>()
                + candidates_by_object.values().map(Vec::len).sum::<usize>(),
        );
    }
    let catalog_has_more = scans
        .iter()
        .any(|scan| !scan.buffered.is_empty() || scan.next_after.is_some());
    Ok(DfsDefaultSifsMigrationScanBatch {
        candidates_by_object,
        scanned_object_ids,
        catalog_has_more,
        awaiting_replica_progress,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DfsDefaultSifsPendingProgress {
    Complete,
    Incomplete,
    Restart,
}

fn default_sifs_migration_object_generation_supersedes(
    incoming: &crate::DfsObjectMetadata,
    existing: &crate::DfsObjectMetadata,
) -> bool {
    let incoming_generation = (incoming.changed_at_unix_nanos, &incoming.version);
    let existing_generation = (existing.changed_at_unix_nanos, &existing.version);
    incoming.file_system_id == existing.file_system_id
        && (incoming_generation > existing_generation
            || (incoming_generation == existing_generation
                && incoming.kind != DfsObjectKind::Tombstone
                && existing.kind == DfsObjectKind::Tombstone))
}

fn replay_default_sifs_object_family_entry(
    target: &mut LocalSobjMetadataBackend,
    target_shard_id: &DfsMetadataShardId,
    object: &crate::DfsObjectMetadata,
    entry: &crate::DfsMetadataJournalEntry,
    budget: &mut DfsDefaultSifsMigrationPassBudget,
) -> Result<DfsDefaultSifsPendingProgress, DfsError> {
    if !entry.mutation.affects_object_id(&object.object_id) {
        return Ok(DfsDefaultSifsPendingProgress::Complete);
    }

    match &entry.mutation {
        crate::DfsMetadataMutation::PutObject { .. }
        | crate::DfsMetadataMutation::CommitVisibleVersion { .. } => {
            // The object generation is the migration's authority fence. Plan
            // again rather than combining a new object with an old family.
            Ok(DfsDefaultSifsPendingProgress::Restart)
        }
        crate::DfsMetadataMutation::PutVersion {
            version_record: Some(version),
            ..
        } => {
            match target.get_version(&version.object_id, &version.version)? {
                Some(existing) if existing != *version => {
                    return Err(DfsError::Metadata {
                        message: format!(
                            "target metadata shard {} has conflicting late version {} for object {} during bounded migration",
                            target_shard_id.0, version.version, version.object_id
                        ),
                    });
                }
                Some(_) => {}
                None => {
                    if !budget.can_mutate() {
                        return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                    }
                    target.put_version(version)?;
                    budget.charge_mutation();
                }
            }
            Ok(DfsDefaultSifsPendingProgress::Complete)
        }
        crate::DfsMetadataMutation::ReconcileCrossShardPinTransfer {
            versions, transfer, ..
        } => {
            if !budget.can_mutate() {
                return Ok(DfsDefaultSifsPendingProgress::Incomplete);
            }
            target
                .install_cross_shard_pin_transfer_for_authoritative_migration(transfer, versions)?;
            budget.charge_mutation();
            Ok(DfsDefaultSifsPendingProgress::Complete)
        }
        crate::DfsMetadataMutation::RemoveCrossShardPinTransfer {
            file_system_id,
            object_id,
            transfer_id,
        } => {
            if !budget.can_mutate() {
                return Ok(DfsDefaultSifsPendingProgress::Incomplete);
            }
            target.remove_cross_shard_pin_transfer_for_authoritative_migration(
                file_system_id,
                object_id,
                transfer_id,
            )?;
            budget.charge_mutation();
            Ok(DfsDefaultSifsPendingProgress::Complete)
        }
        crate::DfsMetadataMutation::PutVersionPin { pin, .. } => {
            if !budget.can_mutate() {
                return Ok(DfsDefaultSifsPendingProgress::Incomplete);
            }
            if target.put_version_pin(pin)?.is_some() {
                budget.charge_mutation();
            }
            Ok(DfsDefaultSifsPendingProgress::Complete)
        }
        crate::DfsMetadataMutation::RemoveVersionPin {
            file_system_id,
            object_id,
            version,
            pin_id,
        } => {
            if !budget.can_mutate() {
                return Ok(DfsDefaultSifsPendingProgress::Incomplete);
            }
            if target
                .remove_version_pin(file_system_id, object_id, version, pin_id)?
                .is_some()
            {
                budget.charge_mutation();
            }
            Ok(DfsDefaultSifsPendingProgress::Complete)
        }
        crate::DfsMetadataMutation::RemoveVersion { .. }
        | crate::DfsMetadataMutation::PutIntentReceipt { .. }
        | crate::DfsMetadataMutation::RemoveIntentReceipt { .. }
        | crate::DfsMetadataMutation::MergeIntentReceiptRetention { .. }
        | crate::DfsMetadataMutation::RemoveVisibleCommit { .. } => {
            if !budget.can_mutate() {
                return Ok(DfsDefaultSifsPendingProgress::Incomplete);
            }
            target.apply_migrated_object_family_gc_mutation(&entry.mutation)?;
            budget.charge_mutation();
            Ok(DfsDefaultSifsPendingProgress::Complete)
        }
        crate::DfsMetadataMutation::PutReceipt {
            receipt: Some(receipt),
            ..
        } => {
            if target.receipt_for_replica(
                &receipt.receipt.object_id,
                &receipt.receipt.committed_version,
                &receipt.replica_node_id,
            )? != Some(receipt.clone())
            {
                if !budget.can_mutate() {
                    return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                }
                target.put_receipt(receipt)?;
                budget.charge_mutation();
            }
            Ok(DfsDefaultSifsPendingProgress::Complete)
        }
        crate::DfsMetadataMutation::RemoveReceipt {
            object_id,
            version,
            replica_node_id,
            ..
        } => {
            if target
                .receipt_for_replica(object_id, version, replica_node_id)?
                .is_some()
            {
                if !budget.can_mutate() {
                    return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                }
                target.remove_receipt(object_id, version, replica_node_id)?;
                budget.charge_mutation();
            }
            Ok(DfsDefaultSifsPendingProgress::Complete)
        }
        crate::DfsMetadataMutation::PutIntent {
            intent: Some(intent),
            ..
        } => {
            if target.get_intent(&intent.object_id, &intent.intent_id)? != Some(intent.clone()) {
                if !budget.can_mutate() {
                    return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                }
                target.put_migrated_intent_for_object(Some(object), intent)?;
                budget.charge_mutation();
            }
            Ok(DfsDefaultSifsPendingProgress::Complete)
        }
        crate::DfsMetadataMutation::PutConflict {
            object_id,
            conflict_id,
            conflict,
            ..
        } => {
            match conflict {
                Some(conflict)
                    if target.get_conflict(object_id, conflict_id)? != Some(conflict.clone()) =>
                {
                    if !budget.can_mutate() {
                        return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                    }
                    target.put_conflict(conflict)?;
                    budget.charge_mutation();
                }
                None if target.get_conflict(object_id, conflict_id)?.is_some() => {
                    if !budget.can_mutate() {
                        return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                    }
                    target.remove_conflict(object_id, conflict_id)?;
                    budget.charge_mutation();
                }
                _ => {}
            }
            Ok(DfsDefaultSifsPendingProgress::Complete)
        }
        crate::DfsMetadataMutation::PutAdvisoryLockState {
            state: Some(state), ..
        } => {
            let mut desired = state.clone();
            desired.authority_rebind_required = true;
            if target.get_advisory_lock_state(&desired.file_system_id, &desired.object_id)?
                != Some(desired.clone())
            {
                if !budget.can_mutate() {
                    return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                }
                target.apply_namespace_transaction(&crate::DfsNamespaceTransaction {
                    transaction_id: format!(
                        "migrate-lock:{}:{}:{}",
                        object.object_id, target_shard_id.0, entry.sequence
                    ),
                    file_system_id: object.file_system_id.clone(),
                    objects: Vec::new(),
                    versions: Vec::new(),
                    advisory_lock_states: vec![desired],
                    created_at: entry.created_at,
                })?;
                budget.charge_mutation();
            }
            Ok(DfsDefaultSifsPendingProgress::Complete)
        }
        crate::DfsMetadataMutation::ApplyNamespaceTransaction {
            transaction: Some(transaction),
            ..
        } => {
            if transaction
                .objects
                .iter()
                .any(|candidate| candidate.object_id == object.object_id)
            {
                return Ok(DfsDefaultSifsPendingProgress::Restart);
            }
            let mut missing_versions = Vec::new();
            for version in transaction
                .versions
                .iter()
                .filter(|version| version.object_id == object.object_id)
            {
                match target.get_version(&version.object_id, &version.version)? {
                    Some(existing) if existing != *version => {
                        return Err(DfsError::Metadata {
                            message: format!(
                                "target metadata shard {} has conflicting late transaction version {} for object {}",
                                target_shard_id.0, version.version, version.object_id
                            ),
                        });
                    }
                    Some(_) => {}
                    None => missing_versions.push(version.clone()),
                }
            }
            let mut changed_locks = Vec::new();
            for state in transaction
                .advisory_lock_states
                .iter()
                .filter(|state| state.object_id == object.object_id)
            {
                let mut desired = state.clone();
                desired.authority_rebind_required = true;
                if target.get_advisory_lock_state(&desired.file_system_id, &desired.object_id)?
                    != Some(desired.clone())
                {
                    changed_locks.push(desired);
                }
            }
            if !missing_versions.is_empty() || !changed_locks.is_empty() {
                if !budget.can_mutate() {
                    return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                }
                target.apply_namespace_transaction(&crate::DfsNamespaceTransaction {
                    transaction_id: format!(
                        "migrate-family:{}:{}:{}",
                        object.object_id, target_shard_id.0, entry.sequence
                    ),
                    file_system_id: object.file_system_id.clone(),
                    objects: Vec::new(),
                    versions: missing_versions,
                    advisory_lock_states: changed_locks,
                    created_at: entry.created_at,
                })?;
                budget.charge_mutation();
            }
            Ok(DfsDefaultSifsPendingProgress::Complete)
        }
        // A source-eviction record describes the source shard's physical
        // cleanup and must never be replayed onto the authoritative target.
        // Payload-less legacy mutations and namespace prepare/job records do
        // not change the migrated metadata family either.
        crate::DfsMetadataMutation::FinalizeCrossShardSourceEviction { .. }
        | crate::DfsMetadataMutation::PutVersion {
            version_record: None,
            ..
        }
        | crate::DfsMetadataMutation::PutReceipt { receipt: None, .. }
        | crate::DfsMetadataMutation::PutIntent { intent: None, .. }
        | crate::DfsMetadataMutation::PutAdvisoryLockState { state: None, .. }
        | crate::DfsMetadataMutation::ApplyNamespaceTransaction {
            transaction: None, ..
        } => Ok(DfsDefaultSifsPendingProgress::Complete),
        _ => Ok(DfsDefaultSifsPendingProgress::Complete),
    }
}

fn advance_default_sifs_pending_migration(
    metadata_root: &std::path::Path,
    shard_backends: &mut BTreeMap<DfsMetadataShardId, LocalSobjMetadataBackend>,
    file_system: &DfsFileSystem,
    pending: &mut DfsDefaultSifsPendingMigration,
    budget: &mut DfsDefaultSifsMigrationPassBudget,
    report: &mut DfsMetadataShardMigrationReport,
) -> Result<DfsDefaultSifsPendingProgress, DfsError> {
    // Before the target object becomes visible, every pass revalidates the
    // exact source generation. A concurrent mutation is replayed through the
    // durable dirty-ID lane instead of completing a mixed-generation bundle.
    if !matches!(
        pending.stage,
        DfsDefaultSifsPendingMigrationStage::SourceEvictions { .. }
            | DfsDefaultSifsPendingMigrationStage::Complete
    ) {
        let source = shard_backends
            .get(&pending.source_shard_id)
            .ok_or_else(|| DfsError::Metadata {
                message: format!(
                    "default SIFS migration source shard {} is unavailable",
                    pending.source_shard_id.0
                ),
            })?;
        if source
            .object_or_compacted_tombstone(&pending.object.object_id)?
            .as_ref()
            != Some(&pending.object)
        {
            return Ok(DfsDefaultSifsPendingProgress::Restart);
        }
    }

    loop {
        match &mut pending.stage {
            DfsDefaultSifsPendingMigrationStage::EnsureFileSystem => {
                let target = shard_backends
                    .get_mut(&pending.target_shard_id)
                    .expect("a pending migration target is a bound writable shard");
                match target.get_file_system(&file_system.file_system_id)? {
                    Some(existing)
                        if !crate::placement::dfs_file_system_semantically_eq(
                            &existing,
                            file_system,
                        ) =>
                    {
                        return Err(DfsError::Metadata {
                            message: format!(
                                "target metadata shard {} has conflicting filesystem {} during bounded migration",
                                pending.target_shard_id.0, file_system.file_system_id
                            ),
                        });
                    }
                    Some(_) => {}
                    None => {
                        if !budget.can_mutate() {
                            return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                        }
                        target.put_file_system(file_system)?;
                        budget.charge_mutation();
                    }
                }
                pending.stage = DfsDefaultSifsPendingMigrationStage::Versions {
                    after_version: None,
                    active_version: None,
                    after_receipt: None,
                };
            }
            DfsDefaultSifsPendingMigrationStage::Versions {
                after_version,
                active_version,
                after_receipt,
            } => {
                if let Some(version) = active_version.clone() {
                    if !budget.can_read_family_record() {
                        return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                    }
                    let page_size = DEFAULT_SIFS_BOOTSTRAP_SHARD_MIGRATION_PAGE_SIZE
                        .min(budget.remaining_family_records)
                        .max(1);
                    let receipt_page = shard_backends
                        .get(&pending.source_shard_id)
                        .expect("pending source exists")
                        .receipts_for_version_page(
                            &pending.object.object_id,
                            &version,
                            after_receipt.as_ref(),
                            page_size,
                        )?;
                    for receipt in receipt_page.receipts {
                        budget.charge_family_record();
                        let target = shard_backends
                            .get_mut(&pending.target_shard_id)
                            .expect("pending target exists");
                        match target.receipt_for_replica(
                            &receipt.receipt.object_id,
                            &receipt.receipt.committed_version,
                            &receipt.replica_node_id,
                        )? {
                            Some(existing) if existing != receipt => {
                                return Err(DfsError::Metadata {
                                    message: format!(
                                        "target metadata shard {} has conflicting receipt for object {} version {} replica {} during bounded migration",
                                        pending.target_shard_id.0,
                                        receipt.receipt.object_id,
                                        receipt.receipt.committed_version,
                                        receipt.replica_node_id
                                    ),
                                });
                            }
                            Some(_) => {}
                            None => {
                                if !budget.can_mutate() {
                                    return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                                }
                                target.put_receipt(&receipt)?;
                                budget.charge_mutation();
                            }
                        }
                        *after_receipt = Some(crate::DfsReplicaReceiptPageCursor {
                            replica_node_id: receipt.replica_node_id,
                            shard_id: None,
                        });
                    }
                    if receipt_page.next_cursor.is_some() {
                        return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                    }
                    *active_version = None;
                    *after_receipt = None;
                    *after_version = Some(crate::DfsObjectVersionPageCursor {
                        version,
                        shard_id: None,
                    });
                    continue;
                }

                if !budget.can_read_family_record() {
                    return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                }
                let version_page = shard_backends
                    .get(&pending.source_shard_id)
                    .expect("pending source exists")
                    .versions_for_object_page(
                        &pending.object.object_id,
                        after_version.as_ref(),
                        1,
                    )?;
                let Some(version) = version_page.versions.into_iter().next() else {
                    pending.stage = DfsDefaultSifsPendingMigrationStage::ClearPinTransfers {
                        after_transfer_id: None,
                    };
                    continue;
                };
                budget.charge_family_record();
                let target = shard_backends
                    .get_mut(&pending.target_shard_id)
                    .expect("pending target exists");
                match target.get_version(&version.object_id, &version.version)? {
                    Some(existing) if existing != version => {
                        return Err(DfsError::Metadata {
                            message: format!(
                                "target metadata shard {} has conflicting version {} for object {} during bounded migration",
                                pending.target_shard_id.0, version.version, version.object_id
                            ),
                        });
                    }
                    Some(_) => {}
                    None => {
                        if !budget.can_mutate() {
                            return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                        }
                        target.put_version(&version)?;
                        budget.charge_mutation();
                    }
                }
                *active_version = Some(version.version);
                *after_receipt = None;
            }
            DfsDefaultSifsPendingMigrationStage::ClearPinTransfers { after_transfer_id } => {
                if !budget.can_read_family_record() {
                    return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                }
                let mut transfers = shard_backends
                    .get(&pending.target_shard_id)
                    .expect("pending target exists")
                    .cross_shard_pin_transfers_for_object(&pending.object.object_id)?;
                transfers.sort_by(|left, right| left.transfer_id.cmp(&right.transfer_id));
                let next = transfers.into_iter().find(|transfer| {
                    after_transfer_id
                        .as_ref()
                        .is_none_or(|after| transfer.transfer_id > *after)
                });
                let Some(transfer) = next else {
                    pending.stage =
                        DfsDefaultSifsPendingMigrationStage::ClearVersionPins { after: None };
                    continue;
                };
                budget.charge_family_record();
                if !budget.can_mutate() {
                    return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                }
                shard_backends
                    .get_mut(&pending.target_shard_id)
                    .expect("pending target exists")
                    .remove_cross_shard_pin_transfer_for_authoritative_migration(
                        &transfer.file_system_id,
                        &transfer.object_id,
                        &transfer.transfer_id,
                    )?;
                budget.charge_mutation();
                *after_transfer_id = Some(transfer.transfer_id);
            }
            DfsDefaultSifsPendingMigrationStage::ClearVersionPins { after } => {
                if !budget.can_read_family_record() {
                    return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                }
                let page_size = DEFAULT_SIFS_BOOTSTRAP_SHARD_MIGRATION_PAGE_SIZE
                    .min(budget.remaining_family_records)
                    .max(1);
                let page = shard_backends
                    .get(&pending.target_shard_id)
                    .expect("pending target exists")
                    .version_pins_for_object_page(
                        &pending.object.object_id,
                        after.as_ref(),
                        page_size,
                    )?;
                for pin in page.pins {
                    budget.charge_family_record();
                    if !budget.can_mutate() {
                        return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                    }
                    if shard_backends
                        .get_mut(&pending.target_shard_id)
                        .expect("pending target exists")
                        .remove_version_pin(
                            &pin.file_system_id,
                            &pin.object_id,
                            &pin.version,
                            &pin.pin_id,
                        )?
                        .is_some()
                    {
                        budget.charge_mutation();
                    }
                    *after = Some(crate::DfsMetadataVersionPinPageCursor {
                        version: pin.version,
                        pin_id: pin.pin_id,
                        shard_id: None,
                    });
                }
                if page.next_cursor.is_some() {
                    return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                }
                pending.stage = DfsDefaultSifsPendingMigrationStage::PinTransfers {
                    after_transfer_id: None,
                };
            }
            DfsDefaultSifsPendingMigrationStage::PinTransfers { after_transfer_id } => {
                if !budget.can_read_family_record() {
                    return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                }
                let mut transfers = shard_backends
                    .get(&pending.source_shard_id)
                    .expect("pending source exists")
                    .cross_shard_pin_transfers_for_object(&pending.object.object_id)?;
                transfers.sort_by(|left, right| left.transfer_id.cmp(&right.transfer_id));
                let next = transfers.into_iter().find(|transfer| {
                    after_transfer_id
                        .as_ref()
                        .is_none_or(|after| transfer.transfer_id > *after)
                });
                let Some(transfer) = next else {
                    pending.stage =
                        DfsDefaultSifsPendingMigrationStage::VersionPins { after: None };
                    continue;
                };
                budget.charge_family_record();
                if !budget.can_mutate() {
                    return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                }

                // Versions were copied in the preceding stage. Include the
                // exact source records again in the provenance mutation so a
                // transfer appended concurrently with that scan can still
                // install newly pinned versions atomically.
                let mut transfer_version_ids = transfer
                    .source_pins
                    .iter()
                    .chain(&transfer.copied_pins)
                    .map(|pin| pin.version.clone())
                    .collect::<BTreeSet<_>>();
                transfer_version_ids.insert(transfer.target_version.clone());
                let source = shard_backends
                    .get(&pending.source_shard_id)
                    .expect("pending source exists");
                let mut versions = Vec::with_capacity(transfer_version_ids.len());
                for version_id in transfer_version_ids {
                    let version = source
                        .get_version(&pending.object.object_id, &version_id)?
                        .ok_or_else(|| DfsError::Metadata {
                            message: format!(
                                "default SIFS migration source shard {} pin transfer {} references missing version {}/{}",
                                pending.source_shard_id.0,
                                transfer.transfer_id,
                                pending.object.object_id,
                                version_id
                            ),
                        })?;
                    versions.push(version);
                }
                shard_backends
                    .get_mut(&pending.target_shard_id)
                    .expect("pending target exists")
                    .install_cross_shard_pin_transfer_for_authoritative_migration(
                        &transfer, &versions,
                    )?;
                budget.charge_mutation();
                *after_transfer_id = Some(transfer.transfer_id);
            }
            DfsDefaultSifsPendingMigrationStage::VersionPins { after } => {
                if !budget.can_read_family_record() {
                    return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                }
                let page_size = DEFAULT_SIFS_BOOTSTRAP_SHARD_MIGRATION_PAGE_SIZE
                    .min(budget.remaining_family_records)
                    .max(1);
                let page = shard_backends
                    .get(&pending.source_shard_id)
                    .expect("pending source exists")
                    .version_pins_for_object_page(
                        &pending.object.object_id,
                        after.as_ref(),
                        page_size,
                    )?;
                // The preceding stage installed the source's captured
                // provenance exactly. Skip every acknowledged key here,
                // including pre-existing target pins that the transfer fences
                // without owning. Replay of a later transfer mutation will
                // atomically reconcile any ownership change that raced the
                // base scan.
                let target_transfer_acknowledged_pins = shard_backends
                    .get(&pending.target_shard_id)
                    .expect("pending target exists")
                    .cross_shard_pin_transfers_for_object(&pending.object.object_id)?
                    .into_iter()
                    .flat_map(|transfer| transfer.source_pins)
                    .map(|pin| (pin.version, pin.pin_id))
                    .collect::<BTreeSet<_>>();
                for pin in page.pins {
                    budget.charge_family_record();
                    if !target_transfer_acknowledged_pins
                        .contains(&(pin.version.clone(), pin.pin_id.clone()))
                    {
                        if !budget.can_mutate() {
                            return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                        }
                        if shard_backends
                            .get_mut(&pending.target_shard_id)
                            .expect("pending target exists")
                            .put_version_pin(&pin)?
                            .is_some()
                        {
                            budget.charge_mutation();
                        }
                    }
                    *after = Some(crate::DfsMetadataVersionPinPageCursor {
                        version: pin.version,
                        pin_id: pin.pin_id,
                        shard_id: None,
                    });
                }
                if page.next_cursor.is_some() {
                    return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                }
                pending.stage = DfsDefaultSifsPendingMigrationStage::Intents { after: None };
            }
            DfsDefaultSifsPendingMigrationStage::Intents { after } => {
                if !budget.can_read_family_record() {
                    return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                }
                let page_size = DEFAULT_SIFS_BOOTSTRAP_SHARD_MIGRATION_PAGE_SIZE
                    .min(budget.remaining_family_records)
                    .max(1);
                let page = shard_backends
                    .get(&pending.source_shard_id)
                    .expect("pending source exists")
                    .intents_for_object_page(
                        &pending.object.object_id,
                        after.as_ref(),
                        page_size,
                    )?;
                for intent in page.intents {
                    budget.charge_family_record();
                    let source_receipt = shard_backends
                        .get(&pending.source_shard_id)
                        .expect("pending source exists")
                        .get_intent_receipt(&intent.object_id, &intent.intent_id)?;
                    let target = shard_backends
                        .get_mut(&pending.target_shard_id)
                        .expect("pending target exists");
                    if let Some(receipt) = source_receipt {
                        match target.get_intent_receipt(&receipt.object_id, &receipt.intent_id)? {
                            Some(existing) if existing != receipt => {
                                return Err(DfsError::Metadata {
                                    message: format!(
                                        "target metadata shard {} has conflicting compact intent receipt {} for object {} during bounded migration",
                                        pending.target_shard_id.0,
                                        receipt.intent_id,
                                        receipt.object_id
                                    ),
                                });
                            }
                            Some(_) => {}
                            None => {
                                if let Some(existing) =
                                    target.get_intent(&receipt.object_id, &receipt.intent_id)?
                                    && !receipt.matches_intent(&existing)
                                {
                                    return Err(DfsError::Metadata {
                                        message: format!(
                                            "target metadata shard {} has conflicting intent {} for object {} during bounded migration",
                                            pending.target_shard_id.0,
                                            receipt.intent_id,
                                            receipt.object_id
                                        ),
                                    });
                                }
                                if !budget.can_mutate() {
                                    return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                                }
                                target.put_intent_receipt_for_object(
                                    Some(&pending.object),
                                    &receipt,
                                )?;
                                budget.charge_mutation();
                            }
                        }
                    } else {
                        match target.get_intent_receipt(&intent.object_id, &intent.intent_id)? {
                            Some(receipt) if !receipt.matches_intent(&intent) => {
                                return Err(DfsError::Metadata {
                                    message: format!(
                                        "target metadata shard {} has conflicting compact intent receipt {} for object {} during bounded migration",
                                        pending.target_shard_id.0,
                                        intent.intent_id,
                                        intent.object_id
                                    ),
                                });
                            }
                            Some(_) => {}
                            None => {
                                match target.get_intent(&intent.object_id, &intent.intent_id)? {
                                    Some(existing) if existing != intent => {
                                        return Err(DfsError::Metadata {
                                            message: format!(
                                                "target metadata shard {} has conflicting intent {} for object {} during bounded migration",
                                                pending.target_shard_id.0,
                                                intent.intent_id,
                                                intent.object_id
                                            ),
                                        });
                                    }
                                    Some(_) => {}
                                    None => {
                                        if !budget.can_mutate() {
                                            return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                                        }
                                        target.put_migrated_intent_for_object(
                                            Some(&pending.object),
                                            &intent,
                                        )?;
                                        budget.charge_mutation();
                                    }
                                }
                            }
                        }
                    }
                    *after = Some(crate::DfsWriteIntentPageCursor {
                        intent_id: intent.intent_id,
                        shard_id: None,
                    });
                }
                if page.next_cursor.is_some() {
                    return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                }
                if !budget.can_read_family_record() {
                    return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                }
                let retention = shard_backends
                    .get(&pending.source_shard_id)
                    .expect("pending source exists")
                    .intent_receipt_retention_for_object(&pending.object.object_id)?;
                budget.charge_family_record();
                if !retention.is_empty() {
                    if !budget.can_mutate() {
                        return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                    }
                    shard_backends
                        .get_mut(&pending.target_shard_id)
                        .expect("pending target exists")
                        .merge_intent_receipt_retention_for_object(
                            Some(&pending.object),
                            &pending.object.file_system_id,
                            &pending.object.object_id,
                            &retention,
                        )?;
                    budget.charge_mutation();
                }
                pending.stage = DfsDefaultSifsPendingMigrationStage::Conflicts { after: None };
            }
            DfsDefaultSifsPendingMigrationStage::Conflicts { after } => {
                if !budget.can_read_family_record() {
                    return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                }
                let page_size = DEFAULT_SIFS_BOOTSTRAP_SHARD_MIGRATION_PAGE_SIZE
                    .min(budget.remaining_family_records)
                    .max(1);
                let page = shard_backends
                    .get(&pending.source_shard_id)
                    .expect("pending source exists")
                    .conflicts_for_object_page(
                        &pending.object.object_id,
                        after.as_ref(),
                        page_size,
                    )?;
                for conflict in page.conflicts {
                    budget.charge_family_record();
                    let target = shard_backends
                        .get_mut(&pending.target_shard_id)
                        .expect("pending target exists");
                    match target.get_conflict(&conflict.object_id, &conflict.conflict_id)? {
                        Some(existing) if existing != conflict => {
                            return Err(DfsError::Metadata {
                                message: format!(
                                    "target metadata shard {} has conflicting conflict {} for object {} during bounded migration",
                                    pending.target_shard_id.0,
                                    conflict.conflict_id,
                                    conflict.object_id
                                ),
                            });
                        }
                        Some(_) => {}
                        None => {
                            if !budget.can_mutate() {
                                return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                            }
                            target.put_conflict(&conflict)?;
                            budget.charge_mutation();
                        }
                    }
                    *after = Some(crate::DfsConflictPageCursor {
                        conflict_id: conflict.conflict_id,
                    });
                }
                if page.next_cursor.is_some() {
                    return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                }
                pending.stage = DfsDefaultSifsPendingMigrationStage::ObjectAndLock;
            }
            DfsDefaultSifsPendingMigrationStage::ObjectAndLock => {
                let source_lock = shard_backends
                    .get(&pending.source_shard_id)
                    .expect("pending source exists")
                    .get_advisory_lock_state(
                        &pending.object.file_system_id,
                        &pending.object.object_id,
                    )?;
                let desired_lock = source_lock.map(|mut state| {
                    state.authority_rebind_required = true;
                    state
                });
                let target = shard_backends
                    .get_mut(&pending.target_shard_id)
                    .expect("pending target exists");
                let target_object = target.get_object(&pending.object.object_id)?;
                if target_object.as_ref().is_some_and(|existing| {
                    existing != &pending.object
                        && !default_sifs_migration_object_generation_supersedes(
                            &pending.object,
                            existing,
                        )
                }) {
                    return Ok(DfsDefaultSifsPendingProgress::Restart);
                }
                let target_lock = target.get_advisory_lock_state(
                    &pending.object.file_system_id,
                    &pending.object.object_id,
                )?;
                if target_object.as_ref() != Some(&pending.object) || target_lock != desired_lock {
                    if !budget.can_mutate() {
                        return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                    }
                    target.apply_namespace_transaction(&crate::DfsNamespaceTransaction {
                        transaction_id: format!(
                            "migrate-object:{}:{}",
                            pending.object.object_id, pending.target_shard_id.0
                        ),
                        file_system_id: pending.object.file_system_id.clone(),
                        objects: vec![pending.object.clone()],
                        versions: Vec::new(),
                        advisory_lock_states: desired_lock.into_iter().collect(),
                        created_at: OffsetDateTime::UNIX_EPOCH,
                    })?;
                    budget.charge_mutation();
                }
                pending.stage = DfsDefaultSifsPendingMigrationStage::Index;
            }
            DfsDefaultSifsPendingMigrationStage::Index => {
                if !crate::metadata_shard_index::object_shard_index_matches(
                    metadata_root,
                    &pending.object,
                    &pending.target_shard_id,
                )? {
                    if !budget.can_mutate() {
                        return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                    }
                    crate::metadata_shard_index::write_object_shard_index(
                        metadata_root,
                        &pending.object,
                        &pending.target_shard_id,
                    )?;
                    report.indexed_objects = report.indexed_objects.saturating_add(1);
                    budget.charge_mutation();
                }
                pending.stage =
                    DfsDefaultSifsPendingMigrationStage::SourceEvictions { next_source: 0 };
            }
            DfsDefaultSifsPendingMigrationStage::SourceEvictions { next_source } => {
                while *next_source < pending.writable_sources.len() {
                    let source_index = *next_source;
                    let source_generation = pending.writable_sources[source_index].clone();
                    let source_shard_id = source_generation.shard_id;
                    let expected_source = source_generation.object;
                    let current_source = shard_backends
                        .get(&source_shard_id)
                        .expect("pending writable source exists")
                        .object_or_compacted_tombstone(&pending.object.object_id)?;
                    let already_evicted = current_source.as_ref().is_some_and(|current| {
                        matches!(
                            current.tombstone_provenance.as_ref(),
                            Some(crate::DfsTombstoneProvenance::SourceEviction {
                                source_shard_id: recorded_source,
                                target_shard_id: recorded_target,
                                ..
                            }) if recorded_source == &source_shard_id
                                && recorded_target == &pending.target_shard_id
                        )
                    });
                    if already_evicted {
                        *next_source = next_source.saturating_add(1);
                        continue;
                    }
                    if current_source.as_ref() != Some(&expected_source) {
                        return Ok(DfsDefaultSifsPendingProgress::Restart);
                    }

                    if !budget.can_read_family_record() {
                        return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                    }
                    let journal_page_size = budget.remaining_family_records.max(1);
                    let entries = shard_backends
                        .get(&source_shard_id)
                        .expect("pending writable source exists")
                        .durable_journal_since_limited(
                            source_generation.replayed_through_sequence,
                            journal_page_size,
                        )?;
                    let source_journal_head = shard_backends
                        .get(&source_shard_id)
                        .expect("pending writable source exists")
                        .journal_last_sequence();
                    let expected_sequence = source_generation
                        .replayed_through_sequence
                        .saturating_add(1);
                    if source_journal_head > source_generation.replayed_through_sequence
                        && entries.first().is_none_or(|entry| {
                            entry.shard_id != source_shard_id || entry.sequence != expected_sequence
                        })
                    {
                        return Err(DfsError::Metadata {
                            message: format!(
                                "default SIFS migration source shard {} lost journal sequence {} while object {} still references that prefix",
                                source_shard_id.0, expected_sequence, pending.object.object_id
                            ),
                        });
                    }
                    if !entries.is_empty() {
                        for entry in entries {
                            budget.charge_family_record();
                            let progress = if source_shard_id == pending.source_shard_id {
                                let target = shard_backends
                                    .get_mut(&pending.target_shard_id)
                                    .expect("pending target exists");
                                replay_default_sifs_object_family_entry(
                                    target,
                                    &pending.target_shard_id,
                                    &pending.object,
                                    &entry,
                                    budget,
                                )?
                            } else {
                                // A stale duplicate object is evicted, but its
                                // attached records are not authoritative input
                                // for the canonical target family.
                                DfsDefaultSifsPendingProgress::Complete
                            };
                            match progress {
                                DfsDefaultSifsPendingProgress::Complete => {
                                    pending.writable_sources[source_index]
                                        .replayed_through_sequence = entry.sequence;
                                }
                                DfsDefaultSifsPendingProgress::Incomplete => {
                                    return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                                }
                                DfsDefaultSifsPendingProgress::Restart => {
                                    return Ok(DfsDefaultSifsPendingProgress::Restart);
                                }
                            }
                        }
                        // If this page consumed the read allowance, yield
                        // before probing again. The source journal sequence is
                        // durable, so no already-settled family entry is lost.
                        if !budget.can_read_family_record() {
                            return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                        }
                        continue;
                    }

                    let needs_write = expected_source.kind != DfsObjectKind::Tombstone;
                    if needs_write && !budget.can_mutate() {
                        return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                    }
                    let eviction = shard_backends
                        .get_mut(&source_shard_id)
                        .expect("pending writable source exists")
                        .put_cross_shard_tombstone_if_unchanged(
                            &expected_source,
                            &pending.object,
                            &source_shard_id,
                            &pending.target_shard_id,
                            pending.writable_sources[source_index].replayed_through_sequence,
                            None,
                        )?;
                    match eviction {
                        crate::metadata_backend::DfsConditionalSourceEviction::Applied {
                            ..
                        } => {
                            budget.charge_mutation();
                            *next_source = next_source.saturating_add(1);
                        }
                        crate::metadata_backend::DfsConditionalSourceEviction::AlreadyApplied => {
                            *next_source = next_source.saturating_add(1);
                        }
                        crate::metadata_backend::DfsConditionalSourceEviction::JournalAdvanced => {
                            return Ok(DfsDefaultSifsPendingProgress::Incomplete);
                        }
                        crate::metadata_backend::DfsConditionalSourceEviction::SourceChanged => {
                            return Ok(DfsDefaultSifsPendingProgress::Restart);
                        }
                    }
                }
                pending.stage = DfsDefaultSifsPendingMigrationStage::Complete;
            }
            DfsDefaultSifsPendingMigrationStage::Complete => {
                return Ok(DfsDefaultSifsPendingProgress::Complete);
            }
        }
    }
}

fn default_sifs_is_legacy_source_eviction_marker(
    metadata_root: &std::path::Path,
    candidate_shard_id: &DfsMetadataShardId,
    candidate: &crate::DfsObjectMetadata,
    candidates: &[(DfsMetadataShardId, crate::DfsObjectMetadata)],
    all_shards: &[DfsMetadataShardConfig],
) -> Result<bool, DfsError> {
    if candidate.kind != DfsObjectKind::Tombstone || candidate.tombstone_provenance.is_some() {
        return Ok(false);
    }
    let Some(selected) =
        crate::control_plane::select_metadata_shard_config_for_path_strict_default_sifs_split(
            &candidate.file_system_id,
            all_shards,
            &candidate.path,
        )?
    else {
        return Ok(false);
    };
    let target_shard_id = DfsMetadataShardId::new(selected.shard_id);
    if target_shard_id == *candidate_shard_id {
        // An unmarked tombstone on its committed route remains authoritative
        // deletion evidence.
        return Ok(false);
    }
    let Some((_, live_target)) = candidates.iter().find(|(shard_id, target)| {
        shard_id == &target_shard_id
            && target.kind != DfsObjectKind::Tombstone
            && target.tombstone_provenance.is_none()
    }) else {
        return Ok(false);
    };
    let mut expected_legacy_marker = live_target.clone();
    expected_legacy_marker.kind = DfsObjectKind::Tombstone;
    expected_legacy_marker.changed_at_unix_nanos =
        live_target.changed_at_unix_nanos.saturating_add(1);
    Ok(candidate == &expected_legacy_marker
        && crate::metadata_shard_index::object_shard_index_matches(
            metadata_root,
            live_target,
            &target_shard_id,
        )?)
}

fn plan_default_sifs_pending_migration(
    metadata_root: &std::path::Path,
    shard_backends: &BTreeMap<DfsMetadataShardId, LocalSobjMetadataBackend>,
    mut candidates: Vec<(DfsMetadataShardId, crate::DfsObjectMetadata)>,
    all_shards: &[DfsMetadataShardConfig],
    writable_shard_ids: &BTreeSet<DfsMetadataShardId>,
    certified_replica_shard_ids: &BTreeSet<DfsMetadataShardId>,
    local_node_id: &NodeId,
    report: &mut DfsMetadataShardMigrationReport,
    awaiting_replica_progress: &mut bool,
) -> Result<Option<DfsDefaultSifsPendingMigration>, DfsError> {
    for (candidate_shard_id, candidate) in &candidates {
        if candidate.kind != DfsObjectKind::Tombstone && candidate.tombstone_provenance.is_some() {
            return Err(DfsError::Metadata {
                message: format!(
                    "live default SIFS object {} on shard {} carries tombstone provenance",
                    candidate.object_id, candidate_shard_id.0
                ),
            });
        }
        if let Some(crate::DfsTombstoneProvenance::SourceEviction {
            source_shard_id,
            target_shard_id,
            ..
        }) = candidate.tombstone_provenance.as_ref()
            && (source_shard_id != candidate_shard_id || target_shard_id == source_shard_id)
        {
            return Err(DfsError::Metadata {
                message: format!(
                    "default SIFS object {} has malformed source-eviction provenance {} -> {} on shard {}",
                    candidate.object_id, source_shard_id.0, target_shard_id.0, candidate_shard_id.0
                ),
            });
        }
    }
    let mut legacy_source_eviction_shards = BTreeSet::new();
    for (candidate_shard_id, candidate) in &candidates {
        if default_sifs_is_legacy_source_eviction_marker(
            metadata_root,
            candidate_shard_id,
            candidate,
            &candidates,
            all_shards,
        )? {
            legacy_source_eviction_shards.insert(candidate_shard_id.clone());
        }
    }
    candidates.retain(|(candidate_shard_id, candidate)| {
        !matches!(
            candidate.tombstone_provenance,
            Some(crate::DfsTombstoneProvenance::SourceEviction { .. })
        ) && !legacy_source_eviction_shards.contains(candidate_shard_id)
    });
    if candidates.is_empty() {
        return Ok(None);
    }
    candidates.sort_by(|(left_shard, left), (right_shard, right)| {
        (left.changed_at_unix_nanos, &left.version)
            .cmp(&(right.changed_at_unix_nanos, &right.version))
            .then_with(|| {
                let left_routed = crate::control_plane::select_metadata_shard_config_for_path_strict_default_sifs_split(
                    &left.file_system_id,
                    all_shards,
                    &left.path,
                )
                .ok()
                .flatten()
                .is_some_and(|selected| selected.shard_id == left_shard.0);
                let right_routed = crate::control_plane::select_metadata_shard_config_for_path_strict_default_sifs_split(
                    &right.file_system_id,
                    all_shards,
                    &right.path,
                )
                .ok()
                .flatten()
                .is_some_and(|selected| selected.shard_id == right_shard.0);
                left_routed.cmp(&right_routed)
            })
            .then_with(|| {
                writable_shard_ids
                    .contains(left_shard)
                    .cmp(&writable_shard_ids.contains(right_shard))
            })
            .then_with(|| {
                crate::metadata_keys::normalize_path(&left.path)
                    .cmp(&crate::metadata_keys::normalize_path(&right.path))
            })
            .then_with(|| left_shard.cmp(right_shard))
    });
    let (source_shard_id, object) = candidates
        .last()
        .cloned()
        .expect("nonempty candidates were checked above");
    report.skipped_stale_sources = report
        .skipped_stale_sources
        .saturating_add(candidates.len().saturating_sub(1));

    let target =
        crate::control_plane::select_metadata_shard_config_for_path_strict_default_sifs_split(
            &object.file_system_id,
            all_shards,
            &object.path,
        )?
        .ok_or_else(|| sinomo_core::kv::KvError::Serialization {
            detail: format!(
                "default SIFS object {} has no committed metadata shard",
                object.path
            ),
        })?;
    let target_shard_id = DfsMetadataShardId::new(target.shard_id.clone());
    let target_is_writable = writable_shard_ids.contains(&target_shard_id);
    let target_is_certified =
        target_is_writable || certified_replica_shard_ids.contains(&target_shard_id);
    let target_object = target_is_certified
        .then(|| {
            candidates
                .iter()
                .find(|(candidate_shard_id, _)| candidate_shard_id == &target_shard_id)
                .map(|(_, target_object)| target_object.clone())
        })
        .flatten();
    let target_preserves_canonical_object = match target_object.as_ref() {
        Some(target_object) if target_object.file_system_id == object.file_system_id => {
            let target_routes_here = crate::control_plane::select_metadata_shard_config_for_path_strict_default_sifs_split(
                &target_object.file_system_id,
                all_shards,
                &target_object.path,
            )?
            .is_some_and(|selected| selected.shard_id == target_shard_id.0);
            let target_generation = (target_object.changed_at_unix_nanos, &target_object.version);
            let source_generation = (object.changed_at_unix_nanos, &object.version);
            target_routes_here
                && (target_generation > source_generation
                    || (target_generation == source_generation
                        && target_object.kind == object.kind
                        && crate::metadata_keys::normalize_path(&target_object.path)
                            == crate::metadata_keys::normalize_path(&object.path)))
        }
        _ => false,
    };
    let needs_target_install = target_is_writable && !target_preserves_canonical_object;
    if !needs_target_install && !target_preserves_canonical_object {
        *awaiting_replica_progress |= target
            .replica_node_ids
            .iter()
            .any(|node_id| node_id == local_node_id);
        return Ok(None);
    }
    let writable_sources = candidates
        .iter()
        .filter(|(shard_id, _)| {
            *shard_id != target_shard_id && writable_shard_ids.contains(shard_id)
        })
        .map(|(shard_id, object)| DfsDefaultSifsPendingSource {
            shard_id: shard_id.clone(),
            object: object.clone(),
            replayed_through_sequence: shard_backends
                .get(shard_id)
                .map(LocalSobjMetadataBackend::journal_last_sequence)
                .unwrap_or_default(),
        })
        .collect::<Vec<_>>();
    let canonical_target_object = if needs_target_install {
        object
    } else {
        report.skipped_stale_sources = report.skipped_stale_sources.saturating_add(1);
        target_object.expect("accepted target object was checked above")
    };
    let needs_index_mutation = !crate::metadata_shard_index::object_shard_index_matches(
        metadata_root,
        &canonical_target_object,
        &target_shard_id,
    )?;
    if !needs_target_install && writable_sources.is_empty() && !needs_index_mutation {
        return Ok(None);
    }
    let counts_as_migration = needs_target_install || !writable_sources.is_empty();
    Ok(Some(DfsDefaultSifsPendingMigration {
        object: canonical_target_object,
        source_shard_id: if needs_target_install {
            source_shard_id
        } else {
            target_shard_id.clone()
        },
        target_shard_id,
        writable_sources,
        catalog_object: false,
        counts_as_migration,
        stage: if needs_target_install {
            DfsDefaultSifsPendingMigrationStage::EnsureFileSystem
        } else {
            DfsDefaultSifsPendingMigrationStage::Index
        },
    }))
}

fn collect_default_sifs_candidates_for_object(
    shard_backends: &BTreeMap<DfsMetadataShardId, LocalSobjMetadataBackend>,
    object_id: &crate::DfsObjectId,
    all_shards: &[DfsMetadataShardConfig],
    writable_shard_ids: &BTreeSet<DfsMetadataShardId>,
    certified_replica_shard_ids: &BTreeSet<DfsMetadataShardId>,
    local_replica_shard_ids: &BTreeSet<DfsMetadataShardId>,
    awaiting_replica_progress: &mut bool,
) -> Result<Vec<(DfsMetadataShardId, crate::DfsObjectMetadata)>, DfsError> {
    let mut candidates = Vec::new();
    for (shard_id, backend) in shard_backends {
        let Some(object) = backend.object_or_compacted_tombstone(object_id)? else {
            continue;
        };
        let source_usable =
            writable_shard_ids.contains(shard_id) || certified_replica_shard_ids.contains(shard_id);
        if !source_usable {
            if local_replica_shard_ids.contains(shard_id)
                && object.kind != DfsObjectKind::Tombstone
                && crate::control_plane::select_metadata_shard_config_for_path_strict_default_sifs_split(
                    &object.file_system_id,
                    all_shards,
                    &object.path,
                )?
                .is_some_and(|selected| {
                    let target_shard_id = DfsMetadataShardId::new(selected.shard_id);
                    target_shard_id != *shard_id
                        && writable_shard_ids.contains(&target_shard_id)
                })
            {
                *awaiting_replica_progress = true;
            }
            continue;
        }
        candidates.push((shard_id.clone(), object));
    }
    Ok(candidates)
}

fn stage_default_sifs_journal_entry_object_ids(
    cursor: &mut DfsDefaultSifsMigrationCursor,
    shard_id: &DfsMetadataShardId,
    entry: &crate::DfsMetadataJournalEntry,
    mut after_object_id: Option<crate::DfsObjectId>,
    replay_capacity: usize,
) -> Result<bool, DfsError> {
    let observed_sequence = cursor
        .observed_journal_sequences
        .get(&shard_id.0)
        .copied()
        .unwrap_or_default();
    let expected_sequence = observed_sequence.saturating_add(1);
    if entry.shard_id != *shard_id || entry.sequence != expected_sequence {
        return Err(DfsError::Metadata {
            message: format!(
                "default SIFS migration journal replay for shard {} expected sequence {}, got shard {} sequence {}",
                shard_id.0, expected_sequence, entry.shard_id.0, entry.sequence
            ),
        });
    }

    loop {
        let remaining = replay_capacity.saturating_sub(cursor.dirty_object_ids.len());
        if remaining == 0 {
            cursor.pending_journal_replay = Some(DfsDefaultSifsPendingJournalReplay {
                shard_id: shard_id.clone(),
                sequence: entry.sequence,
                after_object_id,
            });
            return Ok(false);
        }
        let (object_ids, has_more) = entry
            .mutation
            .affected_object_ids_page(after_object_id.as_ref(), remaining);
        if let Some(last) = object_ids.last() {
            after_object_id = Some(last.clone());
        }
        cursor.dirty_object_ids.extend(object_ids);
        if !has_more {
            cursor
                .observed_journal_sequences
                .insert(shard_id.0.clone(), entry.sequence);
            cursor.pending_journal_replay = None;
            return Ok(true);
        }
        if cursor.dirty_object_ids.len() >= replay_capacity {
            cursor.pending_journal_replay = Some(DfsDefaultSifsPendingJournalReplay {
                shard_id: shard_id.clone(),
                sequence: entry.sequence,
                after_object_id,
            });
            return Ok(false);
        }
    }
}

fn default_sifs_migration_cursor_has_journal_gap(
    cursor: &DfsDefaultSifsMigrationCursor,
    shard_backends: &BTreeMap<DfsMetadataShardId, LocalSobjMetadataBackend>,
) -> Result<bool, DfsError> {
    for (shard_id, backend) in shard_backends {
        let after_sequence = cursor
            .observed_journal_sequences
            .get(&shard_id.0)
            .copied()
            .unwrap_or_default();
        if backend.journal_last_sequence() <= after_sequence {
            continue;
        }
        let expected_sequence = after_sequence.saturating_add(1);
        let first = backend
            .durable_journal_since_limited(after_sequence, 1)?
            .into_iter()
            .next();
        if first
            .as_ref()
            .is_none_or(|entry| entry.shard_id != *shard_id || entry.sequence != expected_sequence)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn resume_default_sifs_pending_journal_replay(
    cursor: &mut DfsDefaultSifsMigrationCursor,
    shard_backends: &BTreeMap<DfsMetadataShardId, LocalSobjMetadataBackend>,
    replay_capacity: usize,
) -> Result<bool, DfsError> {
    let Some(pending) = cursor.pending_journal_replay.clone() else {
        return Ok(true);
    };
    let backend = shard_backends
        .get(&pending.shard_id)
        .ok_or_else(|| DfsError::Metadata {
            message: format!(
                "default SIFS migration pending journal shard {} is unavailable",
                pending.shard_id.0
            ),
        })?;
    let entries = backend.durable_journal_since_limited(pending.sequence.saturating_sub(1), 1)?;
    let entry = entries
        .into_iter()
        .next()
        .ok_or_else(|| DfsError::Metadata {
            message: format!(
                "default SIFS migration pending journal entry {} on shard {} disappeared",
                pending.sequence, pending.shard_id.0
            ),
        })?;
    stage_default_sifs_journal_entry_object_ids(
        cursor,
        &pending.shard_id,
        &entry,
        pending.after_object_id,
        replay_capacity,
    )
}

fn fence_default_sifs_journal_tail(
    cursor: &mut DfsDefaultSifsMigrationCursor,
    shard_backends: &BTreeMap<DfsMetadataShardId, LocalSobjMetadataBackend>,
    replay_capacity: usize,
    remaining_journal_entries: &mut usize,
) -> Result<bool, DfsError> {
    if cursor.pending_journal_replay.is_some()
        || cursor.dirty_object_ids.len() >= replay_capacity
        || *remaining_journal_entries == 0
    {
        return Ok(true);
    }

    for (shard_id, backend) in shard_backends {
        if cursor.pending_journal_replay.is_some()
            || cursor.dirty_object_ids.len() >= replay_capacity
            || *remaining_journal_entries == 0
        {
            return Ok(true);
        }
        let after_sequence = cursor
            .observed_journal_sequences
            .get(&shard_id.0)
            .copied()
            .unwrap_or_default();
        // One entry is admitted into this pass. The second is an exact
        // lookahead: a zero-object control mutation must not make the fence
        // look quiescent while an object mutation is already behind it.
        let entries = backend.durable_journal_since_limited(after_sequence, 2)?;
        let has_lookahead = entries.len() > 1;
        let Some(entry) = entries.into_iter().next() else {
            continue;
        };
        *remaining_journal_entries = remaining_journal_entries.saturating_sub(1);
        if !stage_default_sifs_journal_entry_object_ids(
            cursor,
            shard_id,
            &entry,
            None,
            replay_capacity,
        )? {
            return Ok(true);
        }
        if has_lookahead {
            return Ok(true);
        }
    }
    Ok(false)
}

fn migrate_default_sifs_metadata_on_active_primary_shards(
    metadata_root: &std::path::Path,
    bound_primary_backends: BTreeMap<DfsMetadataShardId, LocalSobjMetadataBackend>,
    retained_replica_backends: BTreeMap<DfsMetadataShardId, LocalSobjMetadataBackend>,
    file_system: &DfsFileSystem,
    all_shards: &[DfsMetadataShardConfig],
    active_primary_shard_ids: &BTreeSet<DfsMetadataShardId>,
    local_node_id: &NodeId,
    max_migrations: usize,
) -> Result<
    (
        DfsDefaultSifsMaterializationReport,
        DfsDefaultSifsMigrationBackendContinuation,
    ),
    DfsError,
> {
    let mut report = DfsMetadataShardMigrationReport {
        file_system_id: file_system.file_system_id.clone(),
        scanned_objects: 0,
        indexed_objects: 0,
        migrated_objects: 0,
        skipped_stale_sources: 0,
        reached_limit: false,
    };
    let writable_shard_ids = bound_primary_backends
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    let expected_writable_shard_ids = all_shards
        .iter()
        .filter(|shard| {
            shard.file_system_id == file_system.file_system_id
                && shard.primary_node_id == *local_node_id
                && active_primary_shard_ids
                    .contains(&DfsMetadataShardId::new(shard.shard_id.clone()))
        })
        .map(|shard| DfsMetadataShardId::new(shard.shard_id.clone()))
        .collect::<BTreeSet<_>>();
    if writable_shard_ids != expected_writable_shard_ids {
        return Err(DfsError::Metadata {
            message: format!(
                "default SIFS migration bound backend shards {:?} do not match locally active shards {:?}",
                writable_shard_ids, expected_writable_shard_ids
            ),
        });
    }
    let mut shard_backends = bound_primary_backends;
    for (shard_id, backend) in retained_replica_backends {
        if shard_backends.insert(shard_id.clone(), backend).is_some() {
            return Err(DfsError::Metadata {
                message: format!(
                    "default SIFS migration continuation duplicates bound primary shard {}",
                    shard_id.0
                ),
            });
        }
    }
    let mut certified_replica_shard_ids = BTreeSet::new();
    let mut local_replica_shard_ids = BTreeSet::new();
    let mut awaiting_replica_progress = false;

    for shard in all_shards
        .iter()
        .filter(|shard| shard.file_system_id == file_system.file_system_id)
    {
        let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
        let shard_root = metadata_root.join(crate::metadata_keys::key_segment(&shard_id.0));
        let local_replica_assigned = shard
            .replica_node_ids
            .iter()
            .any(|node_id| node_id == local_node_id);
        if local_replica_assigned {
            local_replica_shard_ids.insert(shard_id.clone());
        }
        if writable_shard_ids.contains(&shard_id) {
            continue;
        }
        if !shard_root.exists() {
            continue;
        }
        let backend = match shard_backends.remove(&shard_id) {
            Some(mut backend) => {
                backend.refresh_for_inspection()?;
                backend
            }
            None => LocalSobjMetadataBackend::open_for_inspection(shard_id.clone(), shard_root)?,
        };
        let journal_sequence = backend.journal_last_sequence();
        let authority_floor = backend.metadata_authority_ack_floor(&shard_id)?;
        if local_replica_assigned
            && (journal_sequence == 0
                || authority_floor
                    .as_ref()
                    .is_some_and(|floor| floor.journal_sequence >= journal_sequence))
        {
            certified_replica_shard_ids.insert(shard_id.clone());
        }
        shard_backends.insert(shard_id, backend);
    }

    let generation = default_sifs_migration_generation(
        &file_system.file_system_id,
        local_node_id,
        all_shards,
        active_primary_shard_ids,
        &certified_replica_shard_ids,
    );
    let scan_limit = max_migrations.max(1);
    let mut cursor = read_default_sifs_migration_cursor(
        metadata_root,
        &file_system.file_system_id,
        &generation,
    )?
    .unwrap_or_else(|| DfsDefaultSifsMigrationCursor {
        format_version: DEFAULT_SIFS_BOOTSTRAP_SHARD_MIGRATION_CURSOR_FORMAT_VERSION,
        generation: generation.clone(),
        after_object_id: None,
        observed_journal_sequences: shard_backends
            .iter()
            .map(|(shard_id, backend)| (shard_id.0.clone(), backend.journal_last_sequence()))
            .collect(),
        dirty_object_ids: BTreeSet::new(),
        pending_journal_replay: None,
        pending_migration: None,
    });
    for (shard_id, backend) in &shard_backends {
        cursor
            .observed_journal_sequences
            .entry(shard_id.0.clone())
            .or_insert_with(|| backend.journal_last_sequence());
    }
    if default_sifs_migration_cursor_has_journal_gap(&cursor, &shard_backends)? {
        return Err(DfsError::Metadata {
            message: "default SIFS migration cursor references pruned journal history".to_string(),
        });
    }

    // Convert a bounded durable journal suffix into dirty object IDs before
    // advancing the ordered catalog. A journal sequence is checkpointed only
    // after every affected ID from that entry has crossed the durable inner
    // cursor. One lookahead entry makes backlog detection exact.
    let mut journal_backlog = false;
    let mut remaining_journal_entries = scan_limit;
    if cursor.pending_journal_replay.is_some() {
        remaining_journal_entries = remaining_journal_entries.saturating_sub(1);
        if !resume_default_sifs_pending_journal_replay(&mut cursor, &shard_backends, scan_limit)? {
            journal_backlog = true;
        }
    }
    if cursor.pending_journal_replay.is_none() {
        'journal_shards: for (shard_id, backend) in &shard_backends {
            if remaining_journal_entries == 0 || cursor.dirty_object_ids.len() >= scan_limit {
                journal_backlog = true;
                break;
            }
            let after_sequence = cursor
                .observed_journal_sequences
                .get(&shard_id.0)
                .copied()
                .unwrap_or_default();
            let mut entries = backend.durable_journal_since_limited(
                after_sequence,
                remaining_journal_entries.saturating_add(1),
            )?;
            let has_lookahead = entries.len() > remaining_journal_entries;
            entries.truncate(remaining_journal_entries);
            for entry in entries {
                remaining_journal_entries = remaining_journal_entries.saturating_sub(1);
                if !stage_default_sifs_journal_entry_object_ids(
                    &mut cursor,
                    shard_id,
                    &entry,
                    None,
                    scan_limit,
                )? {
                    journal_backlog = true;
                    break 'journal_shards;
                }
            }
            if has_lookahead {
                journal_backlog = true;
                break;
            }
        }
    }

    let mut budget = DfsDefaultSifsMigrationPassBudget::new(max_migrations);
    let mut remaining_object_scans = scan_limit;
    let mut stopped_for_budget = false;

    if let Some(mut pending) = cursor.pending_migration.take() {
        let object_id = pending.object.object_id.clone();
        match advance_default_sifs_pending_migration(
            metadata_root,
            &mut shard_backends,
            file_system,
            &mut pending,
            &mut budget,
            &mut report,
        )? {
            DfsDefaultSifsPendingProgress::Complete => {
                if pending.counts_as_migration {
                    report.migrated_objects = report.migrated_objects.saturating_add(1);
                }
                if pending.catalog_object {
                    cursor.after_object_id = Some(object_id.clone());
                }
                cursor.dirty_object_ids.remove(&object_id);
            }
            DfsDefaultSifsPendingProgress::Incomplete => {
                cursor.pending_migration = Some(pending);
                stopped_for_budget = true;
            }
            DfsDefaultSifsPendingProgress::Restart => {
                cursor.dirty_object_ids.insert(object_id);
                stopped_for_budget = true;
            }
        }
    }

    if !stopped_for_budget {
        let dirty_ids = cursor
            .dirty_object_ids
            .iter()
            .take(remaining_object_scans)
            .cloned()
            .collect::<Vec<_>>();
        for object_id in dirty_ids {
            remaining_object_scans = remaining_object_scans.saturating_sub(1);
            report.scanned_objects = report.scanned_objects.saturating_add(1);
            let candidates = collect_default_sifs_candidates_for_object(
                &shard_backends,
                &object_id,
                all_shards,
                &writable_shard_ids,
                &certified_replica_shard_ids,
                &local_replica_shard_ids,
                &mut awaiting_replica_progress,
            )?;
            let Some(mut pending) = plan_default_sifs_pending_migration(
                metadata_root,
                &shard_backends,
                candidates,
                all_shards,
                &writable_shard_ids,
                &certified_replica_shard_ids,
                local_node_id,
                &mut report,
                &mut awaiting_replica_progress,
            )?
            else {
                cursor.dirty_object_ids.remove(&object_id);
                continue;
            };
            match advance_default_sifs_pending_migration(
                metadata_root,
                &mut shard_backends,
                file_system,
                &mut pending,
                &mut budget,
                &mut report,
            )? {
                DfsDefaultSifsPendingProgress::Complete => {
                    if pending.counts_as_migration {
                        report.migrated_objects = report.migrated_objects.saturating_add(1);
                    }
                    cursor.dirty_object_ids.remove(&object_id);
                }
                DfsDefaultSifsPendingProgress::Incomplete => {
                    cursor.pending_migration = Some(pending);
                    stopped_for_budget = true;
                    break;
                }
                DfsDefaultSifsPendingProgress::Restart => {
                    stopped_for_budget = true;
                    break;
                }
            }
        }
    }

    let mut catalog_has_more = true;
    if !stopped_for_budget && cursor.dirty_object_ids.is_empty() && remaining_object_scans > 0 {
        let scan = collect_default_sifs_migration_scan_batch(
            &shard_backends,
            &file_system.file_system_id,
            all_shards,
            &writable_shard_ids,
            &certified_replica_shard_ids,
            &local_replica_shard_ids,
            cursor.after_object_id.as_ref(),
            remaining_object_scans,
        )?;
        let DfsDefaultSifsMigrationScanBatch {
            mut candidates_by_object,
            scanned_object_ids,
            catalog_has_more: scan_has_more,
            awaiting_replica_progress: scan_awaiting_replica_progress,
            ..
        } = scan;
        catalog_has_more = scan_has_more;
        awaiting_replica_progress |= scan_awaiting_replica_progress;
        report.scanned_objects = report
            .scanned_objects
            .saturating_add(scanned_object_ids.len());

        let candidate_object_ids = candidates_by_object
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        for (evidence_shard_id, evidence_backend) in &shard_backends {
            if !writable_shard_ids.contains(evidence_shard_id)
                && !certified_replica_shard_ids.contains(evidence_shard_id)
            {
                continue;
            }
            for tombstone in
                evidence_backend.compacted_tombstones_for_object_ids(&candidate_object_ids)?
            {
                let candidates = candidates_by_object
                    .entry(tombstone.object_id.clone())
                    .or_default();
                if !candidates
                    .iter()
                    .any(|(candidate_shard_id, _)| candidate_shard_id == evidence_shard_id)
                {
                    candidates.push((evidence_shard_id.clone(), tombstone));
                }
            }
        }
        #[cfg(test)]
        observe_default_sifs_migration_buffered_records(
            candidates_by_object.values().map(Vec::len).sum(),
        );

        for object_id in scanned_object_ids {
            let candidates = candidates_by_object.remove(&object_id).unwrap_or_default();
            let Some(mut pending) = plan_default_sifs_pending_migration(
                metadata_root,
                &shard_backends,
                candidates,
                all_shards,
                &writable_shard_ids,
                &certified_replica_shard_ids,
                local_node_id,
                &mut report,
                &mut awaiting_replica_progress,
            )?
            else {
                cursor.after_object_id = Some(object_id);
                continue;
            };
            pending.catalog_object = true;
            match advance_default_sifs_pending_migration(
                metadata_root,
                &mut shard_backends,
                file_system,
                &mut pending,
                &mut budget,
                &mut report,
            )? {
                DfsDefaultSifsPendingProgress::Complete => {
                    if pending.counts_as_migration {
                        report.migrated_objects = report.migrated_objects.saturating_add(1);
                    }
                    cursor.after_object_id = Some(object_id);
                }
                DfsDefaultSifsPendingProgress::Incomplete => {
                    cursor.pending_migration = Some(pending);
                    stopped_for_budget = true;
                    break;
                }
                DfsDefaultSifsPendingProgress::Restart => {
                    cursor.dirty_object_ids.insert(object_id);
                    stopped_for_budget = true;
                    break;
                }
            }
        }
    }

    // End with a quiescent journal fence. This catches a mutation that lands
    // after the pass's initial dirty-tail sample, including one for an object
    // whose catalog ID is already behind the cursor. Migration's own writes
    // enter the same bounded replay lane; they never reset the catalog.
    journal_backlog |= fence_default_sifs_journal_tail(
        &mut cursor,
        &shard_backends,
        scan_limit,
        &mut remaining_journal_entries,
    )?;

    report.reached_limit = stopped_for_budget
        || journal_backlog
        || !cursor.dirty_object_ids.is_empty()
        || cursor.pending_journal_replay.is_some()
        || cursor.pending_migration.is_some()
        || catalog_has_more;
    if report.reached_limit {
        write_default_sifs_migration_cursor(metadata_root, &cursor)?;
    } else {
        clear_default_sifs_migration_cursor(metadata_root, &file_system.file_system_id)?;
    }

    let mut primary_backends = BTreeMap::new();
    let mut replica_backends = BTreeMap::new();
    for (shard_id, backend) in shard_backends {
        if writable_shard_ids.contains(&shard_id) {
            primary_backends.insert(shard_id, backend);
        } else {
            replica_backends.insert(shard_id, backend);
        }
    }
    Ok((
        DfsDefaultSifsMaterializationReport {
            migration: report,
            awaiting_replica_progress,
        },
        DfsDefaultSifsMigrationBackendContinuation {
            generation,
            primary_backends,
            replica_backends,
        },
    ))
}

pub fn materialize_default_sifs_metadata_on_shards(
    backend: &mut LocalShardedMetadataBackend,
    file_system: &DfsFileSystem,
    bootstrap_owner_node_ids: impl IntoIterator<Item = NodeId>,
    fallback_owner_node_id: NodeId,
    created_at: OffsetDateTime,
) -> Result<(), DfsError> {
    materialize_default_sifs_metadata_on_shards_with_report(
        backend,
        file_system,
        bootstrap_owner_node_ids,
        fallback_owner_node_id,
        created_at,
    )?;
    Ok(())
}

pub(crate) fn materialize_default_sifs_metadata_on_shards_with_report(
    backend: &mut LocalShardedMetadataBackend,
    file_system: &DfsFileSystem,
    bootstrap_owner_node_ids: impl IntoIterator<Item = NodeId>,
    fallback_owner_node_id: NodeId,
    created_at: OffsetDateTime,
) -> Result<DfsMetadataShardMigrationReport, DfsError> {
    backend.ensure_file_system_on_all_shards(file_system)?;
    materialize_default_sifs_metadata(
        backend,
        file_system,
        bootstrap_owner_node_ids,
        fallback_owner_node_id,
        created_at,
    )?;
    Ok(backend.migrate_objects_to_configured_shards(
        &file_system.file_system_id,
        DEFAULT_SIFS_BOOTSTRAP_SHARD_MIGRATION_MUTATION_LIMIT,
    )?)
}

pub fn validate_dfs_metadata_journal_batch_source(
    context: Option<&DfsMetadataReplicaShardContext>,
    command: &DfsMetadataJournalBatchCommand,
) -> Option<DfsMetadataJournalBatchResponse> {
    let Some(context) = context else {
        return Some(dfs_metadata_journal_rejection_response(
            command,
            "metadata journal ingest requires an exact committed cluster shard context".to_string(),
        ));
    };
    if context.shard.shard_id != command.batch.shard_id.0 {
        return Some(dfs_metadata_journal_rejection_response(
            command,
            "metadata journal batch has no local shard config".to_string(),
        ));
    }
    let ordinary_applied_batch = !command.verify_prefix_only && command.authority_probe.is_none();
    if ordinary_applied_batch
        && (command.file_system_id.as_ref() != Some(&context.shard.file_system_id)
            || command.authority_epoch != Some(context.shard.epoch))
    {
        return Some(dfs_metadata_journal_rejection_response(
            command,
            format!(
                "metadata journal authority identity does not match filesystem {} shard {} epoch {}",
                context.shard.file_system_id, context.shard.shard_id, context.shard.epoch
            ),
        ));
    }
    if let Some(probe) = command.authority_probe.as_ref()
        && (command.verify_prefix_only
            || command.prefix_probe.is_some()
            || command.batch.entries.is_empty()
            || probe.probe_id.is_empty()
            || probe.expected_shard != context.shard
            || !crate::service_replica::metadata_authority_operation_is_well_formed_for_shard(
                &probe.operation,
                &context.shard,
            )
            || crate::service_replica::metadata_authority_operation_prepare_sequence(
                &probe.operation,
            ) != command.batch.next_after_sequence)
    {
        return Some(dfs_metadata_journal_rejection_response(
            command,
            format!(
                "metadata authority probe does not match the exact committed assignment for shard {} epoch {}",
                context.shard.shard_id, context.shard.epoch
            ),
        ));
    }
    if command.verify_prefix_only
        && command.prefix_probe.as_ref().is_none_or(|probe| {
            probe.file_system_id.as_ref() != Some(&context.shard.file_system_id)
                || probe.authority_epoch != Some(context.shard.epoch)
        })
    {
        return Some(dfs_metadata_journal_rejection_response(
            command,
            format!(
                "metadata journal prefix probe authority does not match shard {} epoch {}",
                context.shard.shard_id, context.shard.epoch
            ),
        ));
    }
    let Some(source_node_id) = command.source_node_id.as_ref() else {
        return Some(dfs_metadata_journal_rejection_response(
            command,
            "metadata journal batch is missing source node id".to_string(),
        ));
    };
    if source_node_id != &context.shard.primary_node_id {
        return Some(dfs_metadata_journal_rejection_response(
            command,
            format!(
                "metadata journal source {source_node_id} is not shard {} primary {}",
                context.shard.shard_id, context.shard.primary_node_id
            ),
        ));
    }
    if context.shard.primary_node_id == context.local_node_id {
        return Some(dfs_metadata_journal_rejection_response(
            command,
            "metadata primary must not apply a replica journal batch".to_string(),
        ));
    }
    let configured_replica = context
        .shard
        .replica_node_ids
        .contains(&context.local_node_id);
    // A desired-only member can start behind the retiring prefix. The exact
    // pending handoff is a scoped replica-ingest permit for both the ordinary
    // contiguous suffix and its subsequent exact-prefix proof. Restricting
    // this to the proof alone deadlocks: the sender suppresses that proof
    // until its durable cursor says the suffix has landed.
    let handoff_catch_up_target = metadata_membership_handoff_allows_local_target(context)
        && command.authority_probe.is_none()
        && any_handoff_catch_up_command(command, ordinary_applied_batch);
    if !configured_replica && !handoff_catch_up_target {
        return Some(dfs_metadata_journal_rejection_response(
            command,
            format!(
                "local node {} is neither a configured replica nor an exact pending membership-handoff catch-up target for metadata shard {}",
                context.local_node_id, context.shard.shard_id
            ),
        ));
    }
    None
}

fn any_handoff_catch_up_command(
    command: &DfsMetadataJournalBatchCommand,
    ordinary_applied_batch: bool,
) -> bool {
    (ordinary_applied_batch && command.prefix_probe.is_none())
        || (command.verify_prefix_only && command.prefix_probe.is_some())
}

pub fn validate_dfs_metadata_snapshot_install_source(
    context: Option<&DfsMetadataReplicaShardContext>,
    command: &DfsMetadataSnapshotInstallCommand,
) -> Option<DfsMetadataSnapshotInstallResponse> {
    let reject = |reason: String| DfsMetadataSnapshotInstallResponse {
        request_id: command.request_id.clone(),
        shard_id: command.shard_id.clone(),
        next_after_sequence: command.next_after_sequence,
        result: DfsMetadataSnapshotInstallResult::Rejected { reason },
    };

    let Some(context) = context else {
        return Some(reject(
            "metadata snapshot install has no local shard config".to_string(),
        ));
    };
    if context.shard.shard_id != command.shard_id.0 {
        return Some(reject(
            "metadata snapshot install has no local shard config".to_string(),
        ));
    }
    if command.file_system_id.as_ref() != Some(&context.shard.file_system_id)
        || command.authority_epoch != Some(context.shard.epoch)
    {
        return Some(reject(format!(
            "metadata snapshot authority identity does not match filesystem {} shard {} epoch {}",
            context.shard.file_system_id, context.shard.shard_id, context.shard.epoch
        )));
    }
    if context.shard.primary_node_id != command.source_node_id {
        return Some(reject(format!(
            "metadata snapshot source {} is not shard {} primary {}",
            command.source_node_id, context.shard.shard_id, context.shard.primary_node_id
        )));
    }
    if context.shard.primary_node_id == context.local_node_id {
        return Some(reject(
            "metadata primary must not install a replica snapshot".to_string(),
        ));
    }
    if !context
        .shard
        .replica_node_ids
        .contains(&context.local_node_id)
        && !metadata_membership_handoff_allows_local_target(context)
    {
        return Some(reject(format!(
            "local node {} is neither a configured replica nor a pending membership-handoff catch-up target for metadata shard {}",
            context.local_node_id, context.shard.shard_id
        )));
    }
    if let Err(reason) = validated_pending_primary_handoff_snapshot_id(context, command) {
        return Some(reject(reason));
    }
    None
}

pub(crate) fn validated_pending_primary_handoff_snapshot_id(
    context: &DfsMetadataReplicaShardContext,
    command: &DfsMetadataSnapshotInstallCommand,
) -> Result<Option<String>, String> {
    let Some(primary_handoff_id) = command.primary_handoff_id.as_deref() else {
        return Ok(None);
    };
    let Some(handoff) = context.pending_membership_handoff.as_ref() else {
        return Err(format!(
            "metadata primary-handoff snapshot {primary_handoff_id} has no pending handoff"
        ));
    };
    let topology_preserving_replicas =
        crate::control_plane::topology_preserving_metadata_shard_replicas(
            &handoff.retiring_shard,
            &handoff.desired_shard.primary_node_id,
        );
    let exact_fence = if let Some(fence) = handoff.retiring_primary_fence.as_ref() {
        fence.handoff_id == primary_handoff_id
            && fence.primary_node_id == command.source_node_id
            && fence.journal_sequence == command.next_after_sequence
            && crate::metadata_records::dfs_metadata_table_snapshot_matches_checkpoint(
                &command.snapshot,
                fence.journal_sequence,
                &fence.state_fingerprint,
            )
            .map_err(|source| {
                format!(
                    "encode metadata primary-handoff snapshot checkpoint for shard {}: {source}",
                    command.shard_id.0
                )
            })?
    } else {
        false
    };
    let exact_role_rotation = handoff.handoff_id == primary_handoff_id
        && handoff.retiring_shard == context.shard
        && handoff.retiring_shard.primary_node_id == command.source_node_id
        && handoff
            .retiring_shard
            .replica_node_ids
            .contains(&context.local_node_id)
        && handoff.desired_shard.primary_node_id == context.local_node_id
        && handoff.desired_shard.epoch == handoff.retiring_shard.epoch.saturating_add(1)
        && topology_preserving_replicas == Some(handoff.desired_shard.replica_node_ids.clone())
        && command.file_system_id.as_ref() == Some(&handoff.retiring_shard.file_system_id)
        && command.authority_epoch == Some(handoff.retiring_shard.epoch)
        && exact_fence;
    if !exact_role_rotation {
        return Err(format!(
            "metadata primary-handoff snapshot {primary_handoff_id} is not bound to the exact fenced role rotation for shard {}",
            command.shard_id.0
        ));
    }
    Ok(Some(primary_handoff_id.to_string()))
}

fn metadata_membership_handoff_allows_local_target(
    context: &DfsMetadataReplicaShardContext,
) -> bool {
    context
        .pending_membership_handoff
        .as_ref()
        .is_some_and(|handoff| {
            !handoff.handoff_id.is_empty()
                && handoff.retiring_shard == context.shard
                && handoff.desired_shard.file_system_id == context.shard.file_system_id
                && handoff.desired_shard.shard_id == context.shard.shard_id
                && handoff.desired_shard.epoch == context.shard.epoch.saturating_add(1)
                && std::iter::once(&handoff.desired_shard.primary_node_id)
                    .chain(handoff.desired_shard.replica_node_ids.iter())
                    .any(|node_id| node_id == &context.local_node_id)
        })
}

fn dfs_metadata_journal_rejection_response(
    command: &DfsMetadataJournalBatchCommand,
    reason: String,
) -> DfsMetadataJournalBatchResponse {
    DfsMetadataJournalBatchResponse {
        request_id: command.request_id.clone(),
        shard_id: command.batch.shard_id.clone(),
        next_after_sequence: command.batch.next_after_sequence,
        applied_entries: 0,
        verified_prefix_only: false,
        verified_authority_probe_id: None,
        result: DfsMetadataJournalBatchResult::Rejected { reason },
    }
}

#[derive(Debug)]
enum DfsMetadataJournalReplicationStep {
    Continue {
        after_sequence: u64,
    },
    SnapshotRepair {
        reason: String,
        command: DfsMetadataSnapshotInstallCommand,
    },
    Terminal,
}

fn dfs_metadata_caught_up_probe_command(
    local_node_id: &NodeId,
    target_node_id: &NodeId,
    file_system_id: &DfsFileSystemId,
    shard_id: &DfsMetadataShardId,
    authority_epoch: u64,
    journal_sequence: u64,
    state_fingerprint: String,
) -> DfsMetadataJournalBatchCommand {
    let barrier_id = sinomo_core::Id::generate().to_string();
    DfsMetadataJournalBatchCommand {
        request_id: format!(
            "dfs_meta_probe_{}_{}_{}",
            local_node_id, target_node_id, barrier_id
        ),
        source_node_id: Some(local_node_id.clone()),
        file_system_id: Some(file_system_id.clone()),
        authority_epoch: Some(authority_epoch),
        verify_prefix_only: true,
        prefix_probe: Some(DfsMetadataPrefixProbe {
            barrier_id,
            file_system_id: Some(file_system_id.clone()),
            authority_epoch: Some(authority_epoch),
            shard_id: shard_id.clone(),
            journal_sequence,
            state_fingerprint,
            primary_handoff_id: None,
        }),
        authority_probe: None,
        batch: DfsMetadataJournalBatch {
            shard_id: shard_id.clone(),
            entries: Vec::new(),
            next_after_sequence: journal_sequence,
        },
    }
}

fn dfs_metadata_journal_batch_command(
    local_node_id: &NodeId,
    target_node_id: &NodeId,
    file_system_id: &DfsFileSystemId,
    authority_epoch: u64,
    batch: DfsMetadataJournalBatch,
) -> DfsMetadataJournalBatchCommand {
    let next_after_sequence = batch.next_after_sequence;
    DfsMetadataJournalBatchCommand {
        request_id: format!(
            "dfs_meta_{}_{}_{}",
            local_node_id, target_node_id, next_after_sequence
        ),
        source_node_id: Some(local_node_id.clone()),
        file_system_id: Some(file_system_id.clone()),
        authority_epoch: Some(authority_epoch),
        verify_prefix_only: false,
        prefix_probe: None,
        authority_probe: None,
        batch,
    }
}

fn dfs_metadata_snapshot_install_command(
    backend: &mut dyn DfsMetadataBackend,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
    file_system_id: &DfsFileSystemId,
    authority_epoch: u64,
    shard_id: &DfsMetadataShardId,
    reason: String,
) -> Result<DfsMetadataSnapshotInstallCommand, DfsError> {
    let next_after_sequence = backend.journal_last_sequence()?;
    Ok(DfsMetadataSnapshotInstallCommand {
        request_id: format!(
            "dfs_meta_snapshot_{}_{}_{}",
            local_node_id, target_node_id, next_after_sequence
        ),
        source_node_id: local_node_id.clone(),
        file_system_id: Some(file_system_id.clone()),
        authority_epoch: Some(authority_epoch),
        primary_handoff_id: None,
        shard_id: shard_id.clone(),
        next_after_sequence,
        snapshot: backend.export_table_snapshot()?,
        reason,
    })
}

fn push_dfs_metadata_transport_error_outcome(
    outcomes: &mut Vec<DfsMetadataJournalReplicationOutcome>,
    target_node_id: &NodeId,
    shard_id: &DfsMetadataShardId,
    after_sequence: u64,
    error: DfsMetadataJournalReplicationTransportError,
) {
    match error {
        DfsMetadataJournalReplicationTransportError::MissingRoute => {
            outcomes.push(DfsMetadataJournalReplicationOutcome::MissingRoute {
                target_node_id: target_node_id.clone(),
                shard_id: shard_id.clone(),
                after_sequence,
            });
        }
        DfsMetadataJournalReplicationTransportError::Failed { reason } => {
            outcomes.push(DfsMetadataJournalReplicationOutcome::Failed {
                target_node_id: target_node_id.clone(),
                shard_id: shard_id.clone(),
                reason,
            });
        }
    }
}

fn handle_dfs_metadata_journal_batch_response(
    backend: &mut dyn DfsMetadataBackend,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
    file_system_id: &DfsFileSystemId,
    authority_epoch: u64,
    shard_id: &DfsMetadataShardId,
    command: &DfsMetadataJournalBatchCommand,
    response: DfsMetadataJournalBatchResponse,
    outcomes: &mut Vec<DfsMetadataJournalReplicationOutcome>,
) -> Result<DfsMetadataJournalReplicationStep, DfsError> {
    if let Err(reason) =
        validate_dfs_metadata_journal_replication_response(command, shard_id, &response)
    {
        outcomes.push(DfsMetadataJournalReplicationOutcome::Failed {
            target_node_id: target_node_id.clone(),
            shard_id: shard_id.clone(),
            reason,
        });
        return Ok(DfsMetadataJournalReplicationStep::Terminal);
    }

    let next_after_sequence = command.batch.next_after_sequence;
    match response.result {
        DfsMetadataJournalBatchResult::Applied => {
            backend.put_metadata_replication_cursor(
                target_node_id,
                shard_id,
                next_after_sequence,
            )?;
            outcomes.push(DfsMetadataJournalReplicationOutcome::Applied {
                target_node_id: target_node_id.clone(),
                shard_id: shard_id.clone(),
                entries: command.batch.entries.len(),
                next_after_sequence,
            });
            Ok(DfsMetadataJournalReplicationStep::Continue {
                after_sequence: next_after_sequence,
            })
        }
        DfsMetadataJournalBatchResult::Rejected { reason } => {
            if metadata_journal_rejection_allows_snapshot_repair(&reason) {
                let command = dfs_metadata_snapshot_install_command(
                    backend,
                    local_node_id,
                    target_node_id,
                    file_system_id,
                    authority_epoch,
                    shard_id,
                    reason.clone(),
                )?;
                Ok(DfsMetadataJournalReplicationStep::SnapshotRepair { reason, command })
            } else {
                outcomes.push(DfsMetadataJournalReplicationOutcome::Rejected {
                    target_node_id: target_node_id.clone(),
                    shard_id: shard_id.clone(),
                    reason,
                });
                Ok(DfsMetadataJournalReplicationStep::Terminal)
            }
        }
    }
}

fn handle_dfs_metadata_snapshot_install_response(
    backend: &mut dyn DfsMetadataBackend,
    target_node_id: &NodeId,
    shard_id: &DfsMetadataShardId,
    repair_reason: String,
    command: &DfsMetadataSnapshotInstallCommand,
    response: DfsMetadataSnapshotInstallResponse,
    outcomes: &mut Vec<DfsMetadataJournalReplicationOutcome>,
) -> Result<DfsMetadataJournalReplicationStep, DfsError> {
    if let Err(reason) =
        validate_dfs_metadata_snapshot_install_replication_response(command, &response)
    {
        outcomes.push(DfsMetadataJournalReplicationOutcome::Failed {
            target_node_id: target_node_id.clone(),
            shard_id: shard_id.clone(),
            reason,
        });
        return Ok(DfsMetadataJournalReplicationStep::Terminal);
    }

    match response.result {
        DfsMetadataSnapshotInstallResult::Installed { archived_existing } => {
            backend.put_metadata_replication_cursor(
                target_node_id,
                shard_id,
                command.next_after_sequence,
            )?;
            outcomes.push(DfsMetadataJournalReplicationOutcome::SnapshotInstalled {
                target_node_id: target_node_id.clone(),
                shard_id: shard_id.clone(),
                next_after_sequence: command.next_after_sequence,
                archived_existing,
            });
            Ok(DfsMetadataJournalReplicationStep::Continue {
                after_sequence: command.next_after_sequence,
            })
        }
        DfsMetadataSnapshotInstallResult::Rejected {
            reason: snapshot_reason,
        } => {
            outcomes.push(DfsMetadataJournalReplicationOutcome::Rejected {
                target_node_id: target_node_id.clone(),
                shard_id: shard_id.clone(),
                reason: format!("{repair_reason}; snapshot repair rejected: {snapshot_reason}"),
            });
            Ok(DfsMetadataJournalReplicationStep::Terminal)
        }
    }
}

fn push_dfs_metadata_snapshot_transport_error_outcome(
    outcomes: &mut Vec<DfsMetadataJournalReplicationOutcome>,
    target_node_id: &NodeId,
    shard_id: &DfsMetadataShardId,
    after_sequence: u64,
    repair_reason: String,
    error: DfsMetadataJournalReplicationTransportError,
) {
    match error {
        DfsMetadataJournalReplicationTransportError::MissingRoute => {
            outcomes.push(DfsMetadataJournalReplicationOutcome::MissingRoute {
                target_node_id: target_node_id.clone(),
                shard_id: shard_id.clone(),
                after_sequence,
            });
        }
        DfsMetadataJournalReplicationTransportError::Failed { reason } => {
            outcomes.push(DfsMetadataJournalReplicationOutcome::Failed {
                target_node_id: target_node_id.clone(),
                shard_id: shard_id.clone(),
                reason: format!("{repair_reason}; snapshot repair failed: {reason}"),
            });
        }
    }
}

fn push_dfs_metadata_backlog_if_remaining(
    backend: &mut dyn DfsMetadataBackend,
    target_node_id: &NodeId,
    shard_id: &DfsMetadataShardId,
    after_sequence: u64,
    options: DfsMetadataJournalReplicationOptions,
    outcomes: &mut Vec<DfsMetadataJournalReplicationOutcome>,
) -> Result<(), DfsError> {
    if backend
        .journal_batch_since_limited(
            after_sequence,
            options.batch_entry_limit,
            options.batch_byte_limit,
        )?
        .is_some()
    {
        outcomes.push(DfsMetadataJournalReplicationOutcome::BacklogRemaining {
            target_node_id: target_node_id.clone(),
            shard_id: shard_id.clone(),
            after_sequence,
            max_batches: options.max_batches_per_target,
        });
    }
    Ok(())
}

pub fn replicate_dfs_metadata_shard_to_target(
    backend: &mut dyn DfsMetadataBackend,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
    file_system_id: &DfsFileSystemId,
    authority_epoch: u64,
    shard_id: &DfsMetadataShardId,
    mut after_sequence: u64,
    transport: &mut dyn DfsMetadataJournalReplicationTransport,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<Vec<DfsMetadataJournalReplicationOutcome>, DfsError> {
    let mut outcomes = Vec::new();
    let mut sent_batches = 0usize;
    let target_started_at = Instant::now();
    loop {
        let Some(batch) = backend.journal_batch_since_limited(
            after_sequence,
            options.batch_entry_limit,
            options.batch_byte_limit,
        )?
        else {
            if sent_batches == 0 {
                outcomes.push(DfsMetadataJournalReplicationOutcome::NoChanges {
                    target_node_id: target_node_id.clone(),
                    shard_id: shard_id.clone(),
                    after_sequence,
                });
            }
            break;
        };
        if !transport.has_metadata_replication_route(target_node_id) {
            outcomes.push(DfsMetadataJournalReplicationOutcome::MissingRoute {
                target_node_id: target_node_id.clone(),
                shard_id: shard_id.clone(),
                after_sequence,
            });
            break;
        }
        if sent_batches >= options.max_batches_per_target {
            push_dfs_metadata_backlog_if_remaining(
                backend,
                target_node_id,
                shard_id,
                after_sequence,
                options,
                &mut outcomes,
            )?;
            break;
        }

        let command = dfs_metadata_journal_batch_command(
            local_node_id,
            target_node_id,
            file_system_id,
            authority_epoch,
            batch,
        );
        let Some(attempt_timeout) =
            dfs_metadata_replication_attempt_timeout(options, target_started_at)
        else {
            outcomes.push(DfsMetadataJournalReplicationOutcome::BacklogRemaining {
                target_node_id: target_node_id.clone(),
                shard_id: shard_id.clone(),
                after_sequence,
                max_batches: options.max_batches_per_target,
            });
            break;
        };
        let response = match transport.send_metadata_journal_batch(
            target_node_id,
            &command,
            attempt_timeout,
        ) {
            Ok(response) => response,
            Err(error) => {
                push_dfs_metadata_transport_error_outcome(
                    &mut outcomes,
                    target_node_id,
                    shard_id,
                    after_sequence,
                    error,
                );
                break;
            }
        };
        match handle_dfs_metadata_journal_batch_response(
            backend,
            local_node_id,
            target_node_id,
            file_system_id,
            authority_epoch,
            shard_id,
            &command,
            response,
            &mut outcomes,
        )? {
            DfsMetadataJournalReplicationStep::Continue {
                after_sequence: next_after_sequence,
            } => {
                after_sequence = next_after_sequence;
                sent_batches += 1;
            }
            DfsMetadataJournalReplicationStep::SnapshotRepair {
                reason,
                command: snapshot_command,
            } => {
                let Some(snapshot_attempt_timeout) =
                    dfs_metadata_replication_attempt_timeout(options, target_started_at)
                else {
                    outcomes.push(DfsMetadataJournalReplicationOutcome::BacklogRemaining {
                        target_node_id: target_node_id.clone(),
                        shard_id: shard_id.clone(),
                        after_sequence,
                        max_batches: options.max_batches_per_target,
                    });
                    break;
                };
                match transport.send_metadata_snapshot_install(
                    target_node_id,
                    &snapshot_command,
                    snapshot_attempt_timeout,
                ) {
                    Ok(snapshot_response) => {
                        match handle_dfs_metadata_snapshot_install_response(
                            backend,
                            target_node_id,
                            shard_id,
                            reason,
                            &snapshot_command,
                            snapshot_response,
                            &mut outcomes,
                        )? {
                            DfsMetadataJournalReplicationStep::Continue {
                                after_sequence: next_after_sequence,
                            } => {
                                after_sequence = next_after_sequence;
                                sent_batches += 1;
                            }
                            DfsMetadataJournalReplicationStep::Terminal => break,
                            DfsMetadataJournalReplicationStep::SnapshotRepair { .. } => {
                                unreachable!("snapshot install response cannot request a snapshot")
                            }
                        }
                    }
                    Err(error) => {
                        push_dfs_metadata_snapshot_transport_error_outcome(
                            &mut outcomes,
                            target_node_id,
                            shard_id,
                            after_sequence,
                            reason,
                            error,
                        );
                        break;
                    }
                }
            }
            DfsMetadataJournalReplicationStep::Terminal => break,
        };
        if sent_batches >= options.max_batches_per_target {
            push_dfs_metadata_backlog_if_remaining(
                backend,
                target_node_id,
                shard_id,
                after_sequence,
                options,
                &mut outcomes,
            )?;
            break;
        }
    }
    Ok(outcomes)
}

pub(crate) fn metadata_journal_rejection_allows_snapshot_repair(reason: &str) -> bool {
    reason.contains(DFS_METADATA_UNREADABLE_REPLICA_SNAPSHOT_REPAIR_REASON)
        || (reason.contains("metadata table snapshot")
            && reason.contains("semantic checksum mismatch"))
        || (reason.contains("metadata journal sequence")
            && reason.contains("already exists with a different entry"))
        || (reason.contains("metadata journal sequence")
            && reason.contains("within durable prefix")
            && reason.contains("is missing while replaying shard"))
        || reason.contains("failed to decode segmented journal entry")
        || (reason.contains("metadata journal prefix probe") && reason.contains("state mismatch"))
        || ((reason.contains("metadata journal batch")
            || reason.contains("metadata authority batch"))
            && reason.contains("starts unseen history")
            && reason.contains("durable replay requires"))
        || (reason.contains("metadata shard")
            && reason.contains("cannot extend prefix")
            && reason.contains("with journal entry"))
}

pub(crate) fn metadata_replication_rejection_is_topology_mismatch(reason: &str) -> bool {
    reason.contains("metadata journal batch has no local shard config")
        || reason.contains("metadata snapshot install has no local shard config")
        || reason.contains("is not configured as a replica for metadata shard")
        || (reason.contains("metadata journal source")
            && reason.contains("is not shard")
            && reason.contains("primary"))
        || (reason.contains("metadata snapshot source")
            && reason.contains("is not shard")
            && reason.contains("primary"))
}

pub type AsyncDfsMetadataJournalBatchSender = Arc<
    dyn for<'a> Fn(
            &'a NodeId,
            &'a DfsMetadataJournalBatchCommand,
            Duration,
        ) -> DfsPeerTransportFuture<
            'a,
            Result<DfsMetadataJournalBatchResponse, DfsMetadataPeerTransportError>,
        > + Send
        + Sync,
>;

pub type AsyncDfsMetadataSnapshotInstallSender = Arc<
    dyn for<'a> Fn(
            &'a NodeId,
            &'a DfsMetadataSnapshotInstallCommand,
            Duration,
        ) -> DfsPeerTransportFuture<
            'a,
            Result<DfsMetadataSnapshotInstallResponse, DfsMetadataPeerTransportError>,
        > + Send
        + Sync,
>;

fn dfs_metadata_value_response(
    result: ObjectRpcResult,
) -> Result<ObjectRpcResponse, DfsMetadataPeerTransportError> {
    value_response_from_rpc_result(result)
        .map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))
}

pub(crate) fn async_dfs_metadata_journal_batch_sender_from_peer_transport(
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
) -> AsyncDfsMetadataJournalBatchSender {
    Arc::new(move |target_node_id, command, operation_timeout| {
        let peer_transport = Arc::clone(&peer_transport);
        Box::pin(async move {
            let call = dfs_metadata_journal_batch_rpc_call(command)
                .map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))?;
            let response = peer_transport
                .call_object_rpc_async(
                    target_node_id,
                    call,
                    DfsObjectRpcSendOptions::default().with_overall_timeout(operation_timeout),
                )
                .await
                .map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))?;
            response
                .decode_body::<DfsMetadataJournalBatchResponse>()
                .await
                .map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))
        })
    })
}

pub(crate) fn async_dfs_metadata_snapshot_install_sender_from_peer_transport(
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
) -> AsyncDfsMetadataSnapshotInstallSender {
    Arc::new(move |target_node_id, command, operation_timeout| {
        let peer_transport = Arc::clone(&peer_transport);
        Box::pin(async move {
            let call = dfs_metadata_snapshot_install_rpc_call(command)
                .map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))?;
            let response = peer_transport
                .call_object_rpc_async(
                    target_node_id,
                    call,
                    DfsObjectRpcSendOptions::default().with_overall_timeout(operation_timeout),
                )
                .await
                .map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))?;
            response
                .decode_body::<DfsMetadataSnapshotInstallResponse>()
                .await
                .map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))
        })
    })
}

pub(crate) fn dfs_metadata_journal_batch_sender_from_peer_transport(
    peer_transport: Arc<dyn DfsPeerTransport>,
) -> DfsMetadataJournalBatchSender {
    Arc::new(move |target_node_id, command, operation_timeout| {
        let call = dfs_metadata_journal_batch_rpc_call(command)
            .map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))?;
        let response = peer_transport
            .call_object_rpc(
                target_node_id,
                call,
                DfsObjectRpcSendOptions::default().with_overall_timeout(operation_timeout),
            )
            .map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))?;
        let response = dfs_metadata_value_response(response)?;
        decode_dfs_metadata_journal_batch_rpc_response(&response)
            .map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))
    })
}

pub(crate) fn dfs_metadata_snapshot_install_sender_from_peer_transport(
    peer_transport: Arc<dyn DfsPeerTransport>,
) -> DfsMetadataSnapshotInstallSender {
    Arc::new(move |target_node_id, command, operation_timeout| {
        let call = dfs_metadata_snapshot_install_rpc_call(command)
            .map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))?;
        let response = peer_transport
            .call_object_rpc(
                target_node_id,
                call,
                DfsObjectRpcSendOptions::default().with_overall_timeout(operation_timeout),
            )
            .map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))?;
        let response = dfs_metadata_value_response(response)?;
        decode_dfs_metadata_snapshot_install_rpc_response(&response)
            .map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))
    })
}

#[cfg(test)]
mod cursor_update_tests {
    use super::*;

    fn configure_replica_shard(
        data_dir: &SinomoDataDir,
        local_node_id: &NodeId,
        shard: &DfsMetadataShardConfig,
    ) {
        data_dir.ensure_root_layout().expect("replica root layout");
        let cluster_paths = ClusterPaths::new(data_dir);
        let cluster_id =
            sinomo_core::node::ClusterId::from_legacy_named("cluster_replica_recovery")
                .expect("cluster id");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "replica recovery",
            local_node_id.clone(),
            "replica",
            "pk-replica-recovery",
            OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("cluster KV state");

        let mut cluster_kv = sinomo_core::kv::InProcessKvStore::restore(
            sinomo_core::kv::read_cluster_kv_cached_snapshot(&cluster_paths.kv_paths())
                .expect("cluster KV snapshot"),
        )
        .expect("cluster KV store");
        cluster_kv
            .put_serde(
                metadata_shard_key(&shard.file_system_id, &shard.shard_id),
                shard,
                None,
            )
            .expect("metadata shard config");
        let snapshot = cluster_kv.snapshot().expect("updated cluster KV snapshot");
        sinomo_core::kv::write_cluster_kv_snapshot(&cluster_paths.kv_paths(), &snapshot)
            .expect("write metadata shard config");
    }

    struct LocalReplicaRecoveryTransport {
        receiver_data_dir: SinomoDataDir,
        source_node_id: NodeId,
        target_node_id: NodeId,
        journal_rejection_reason: Option<String>,
        snapshot_install_calls: usize,
    }

    impl DfsMetadataJournalReplicationTransport for LocalReplicaRecoveryTransport {
        fn has_metadata_replication_route(&self, target_node_id: &NodeId) -> bool {
            target_node_id == &self.target_node_id
        }

        fn send_metadata_journal_batch(
            &mut self,
            target_node_id: &NodeId,
            command: &DfsMetadataJournalBatchCommand,
            _timeout: Duration,
        ) -> Result<DfsMetadataJournalBatchResponse, DfsMetadataJournalReplicationTransportError>
        {
            assert_eq!(target_node_id, &self.target_node_id);
            let cluster_paths = ClusterPaths::new(&self.receiver_data_dir);
            let response = process_cluster_dfs_metadata_journal_batch(
                &self.receiver_data_dir,
                &cluster_paths,
                &self.source_node_id,
                command.clone(),
            )
            .map_err(
                |error| DfsMetadataJournalReplicationTransportError::Failed {
                    reason: error.to_string(),
                },
            )?;
            if let DfsMetadataJournalBatchResult::Rejected { reason } = &response.result {
                self.journal_rejection_reason = Some(reason.clone());
            }
            Ok(response)
        }

        fn send_metadata_snapshot_install(
            &mut self,
            target_node_id: &NodeId,
            command: &DfsMetadataSnapshotInstallCommand,
            _timeout: Duration,
        ) -> Result<DfsMetadataSnapshotInstallResponse, DfsMetadataJournalReplicationTransportError>
        {
            assert_eq!(target_node_id, &self.target_node_id);
            self.snapshot_install_calls = self.snapshot_install_calls.saturating_add(1);
            let cluster_paths = ClusterPaths::new(&self.receiver_data_dir);
            process_cluster_dfs_metadata_snapshot_install(
                &self.receiver_data_dir,
                &cluster_paths,
                &self.source_node_id,
                command.clone(),
            )
            .map_err(
                |error| DfsMetadataJournalReplicationTransportError::Failed {
                    reason: error.to_string(),
                },
            )
        }
    }

    #[test]
    fn default_sifs_bootstrap_shard_migration_is_bounded() {
        assert_eq!(DEFAULT_SIFS_BOOTSTRAP_SHARD_MIGRATION_MUTATION_LIMIT, 256);
        let source = include_str!("metadata_replication.rs");
        assert!(!source.contains(concat!(
            "migrate_objects_to_configured_shards(&file_system.file_system_id, ",
            "usize::MAX"
        )));
    }

    #[test]
    fn migration_end_fence_does_not_hide_object_write_behind_control_entry() {
        let root = sinomo_testdir::TestDir::new("dfs-default-migration-two-entry-fence");
        let local = NodeId::from_legacy_named("node_dfs_two_entry_fence").expect("node");
        let file_system = distributed_sifs(local.clone());
        let shard_id = DfsMetadataShardId::new("root");
        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), root.join("root"))
            .expect("root backend");
        backend.put_file_system(&file_system).expect("filesystem");
        let baseline_sequence = backend.journal_last_sequence();

        // This entry carries no object IDs. The following entry must still be
        // visible to the fence's lookahead in this same observation window.
        backend
            .put_file_system(&file_system)
            .expect("zero-object control mutation");
        let mut object = crate::distributed_sifs_bootstrap_objects(local.clone())
            .into_iter()
            .next()
            .expect("bootstrap object");
        object.object_id =
            crate::DfsObjectId::from_legacy_named("obj_two_entry_fence").expect("object id");
        object.path = "/two-entry-fence".to_string();
        backend.put_object(&object).expect("object mutation");
        let durable_tail = backend
            .durable_journal_since_limited(baseline_sequence, 3)
            .expect("durable two-entry tail");
        assert_eq!(
            durable_tail.len(),
            2,
            "test fixture must expose both consecutive journal entries: {durable_tail:?}"
        );

        let mut cursor = DfsDefaultSifsMigrationCursor {
            format_version: DEFAULT_SIFS_BOOTSTRAP_SHARD_MIGRATION_CURSOR_FORMAT_VERSION,
            generation: DfsDefaultSifsMigrationGeneration {
                file_system_id: file_system.file_system_id.clone(),
                local_node_id: local,
                shards: Vec::new(),
                active_primary_shard_ids: vec![shard_id.clone()],
                certified_replica_shard_ids: Vec::new(),
            },
            after_object_id: None,
            observed_journal_sequences: BTreeMap::from([(shard_id.0.clone(), baseline_sequence)]),
            dirty_object_ids: BTreeSet::new(),
            pending_journal_replay: None,
            pending_migration: None,
        };
        let backends = BTreeMap::from([(shard_id.clone(), backend)]);

        let mut remaining_entries = 1;
        assert!(
            fence_default_sifs_journal_tail(&mut cursor, &backends, 1, &mut remaining_entries,)
                .expect("first end fence"),
            "the second entry is exact backlog even when the first affects no object"
        );
        assert!(cursor.dirty_object_ids.is_empty());
        assert_eq!(
            cursor.observed_journal_sequences.get(&shard_id.0),
            Some(&baseline_sequence.saturating_add(1))
        );

        let mut remaining_entries = 1;
        assert!(
            !fence_default_sifs_journal_tail(&mut cursor, &backends, 1, &mut remaining_entries,)
                .expect("second end fence")
        );
        assert_eq!(cursor.dirty_object_ids, BTreeSet::from([object.object_id]));
    }

    #[test]
    fn migration_replay_checkpoints_inside_512_object_journal_transaction() {
        let local = NodeId::from_legacy_named("node_dfs_512_entry_replay").expect("node");
        let file_system = distributed_sifs(local.clone());
        let shard_id = DfsMetadataShardId::new("root");
        let mut objects = Vec::with_capacity(512);
        for index in (0..512).rev() {
            let mut object = crate::distributed_sifs_bootstrap_objects(local.clone())
                .into_iter()
                .next()
                .expect("bootstrap object");
            object.object_id =
                crate::DfsObjectId::from_legacy_named(format!("obj_512_entry_replay_{index:04}"))
                    .expect("object id");
            object.path = format!("/journal-512/{index:04}");
            objects.push(object);
        }
        let transaction = crate::DfsNamespaceTransaction {
            transaction_id: "transaction_512_entry_replay".to_string(),
            file_system_id: file_system.file_system_id.clone(),
            objects,
            versions: Vec::new(),
            advisory_lock_states: Vec::new(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        };
        let entry = crate::DfsMetadataJournalEntry {
            shard_id: shard_id.clone(),
            sequence: 1,
            created_at: OffsetDateTime::UNIX_EPOCH,
            mutation: crate::DfsMetadataMutation::ApplyNamespaceTransaction {
                file_system_id: transaction.file_system_id.clone(),
                transaction_id: transaction.transaction_id.clone(),
                object_count: transaction.objects.len(),
                transaction: Some(transaction),
            },
        };
        let mut cursor = DfsDefaultSifsMigrationCursor {
            format_version: DEFAULT_SIFS_BOOTSTRAP_SHARD_MIGRATION_CURSOR_FORMAT_VERSION,
            generation: DfsDefaultSifsMigrationGeneration {
                file_system_id: file_system.file_system_id,
                local_node_id: local,
                shards: Vec::new(),
                active_primary_shard_ids: vec![shard_id.clone()],
                certified_replica_shard_ids: Vec::new(),
            },
            after_object_id: None,
            observed_journal_sequences: BTreeMap::from([(shard_id.0.clone(), 0)]),
            dirty_object_ids: BTreeSet::new(),
            pending_journal_replay: None,
            pending_migration: None,
        };

        assert!(
            !stage_default_sifs_journal_entry_object_ids(
                &mut cursor,
                &shard_id,
                &entry,
                None,
                256,
            )
            .expect("first half")
        );
        assert_eq!(cursor.dirty_object_ids.len(), 256);
        assert_eq!(cursor.observed_journal_sequences.get(&shard_id.0), Some(&0));
        let pending = cursor
            .pending_journal_replay
            .clone()
            .expect("durable inner-entry cursor");

        cursor.dirty_object_ids.clear();
        assert!(
            stage_default_sifs_journal_entry_object_ids(
                &mut cursor,
                &shard_id,
                &entry,
                pending.after_object_id,
                256,
            )
            .expect("second half")
        );
        assert_eq!(cursor.dirty_object_ids.len(), 256);
        assert_eq!(cursor.observed_journal_sequences.get(&shard_id.0), Some(&1));
        assert!(cursor.pending_journal_replay.is_none());
    }

    #[test]
    fn authority_scoped_migration_stops_scanning_at_the_mutation_cap() {
        let root = sinomo_testdir::TestDir::new("dfs-authority-migration-bounded-scan");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let local =
            NodeId::from_legacy_named("node_dfs_bounded_authority_migration").expect("node");
        let file_system = distributed_sifs(local.clone());
        let shards = vec![
            DfsMetadataShardConfig {
                file_system_id: file_system.file_system_id.clone(),
                shard_id: "root".to_string(),
                primary_node_id: local.clone(),
                replica_node_ids: Vec::new(),
                epoch: 1,
                assignment: crate::DfsMetadataShardAssignment::Root,
                default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            },
            DfsMetadataShardConfig {
                file_system_id: file_system.file_system_id.clone(),
                shard_id: "services".to_string(),
                primary_node_id: local.clone(),
                replica_node_ids: Vec::new(),
                epoch: 1,
                assignment: crate::DfsMetadataShardAssignment::PathPrefix {
                    prefix: crate::SIFS_SERVICES_PATH.to_string(),
                },
                default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            },
            DfsMetadataShardConfig {
                file_system_id: file_system.file_system_id.clone(),
                shard_id: "users".to_string(),
                primary_node_id: local.clone(),
                replica_node_ids: Vec::new(),
                epoch: 1,
                assignment: crate::DfsMetadataShardAssignment::PathPrefix {
                    prefix: crate::SIFS_USERS_PATH.to_string(),
                },
                default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            },
        ];
        let mut source =
            open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("root"))
                .expect("root backend");
        source.put_file_system(&file_system).expect("filesystem");
        for index in 0..4 {
            let mut object = crate::distributed_sifs_bootstrap_objects(local.clone())
                .into_iter()
                .next()
                .expect("bootstrap object");
            object.object_id = crate::DfsObjectId::from_legacy_named(format!(
                "obj_bounded_authority_migration_{index}"
            ))
            .expect("object id");
            object.path = format!("/services/legacy-{index}.txt");
            source.put_object(&object).expect("legacy object");
        }
        drop(source);
        let bound_primary_backends = LocalShardedMetadataBackend::new(
            crate::default_dfs_metadata_shards_root(&data_dir),
            shards.clone(),
        )
        .expect("bound local primary backends")
        .into_shard_backends();
        crate::metadata_backend::reset_metadata_journal_write_count();
        crate::metadata_shard_index::reset_object_shard_index_write_count();

        let (report, _) = migrate_default_sifs_metadata_on_active_primary_shards(
            &crate::default_dfs_metadata_shards_root(&data_dir),
            bound_primary_backends,
            BTreeMap::new(),
            &file_system,
            &shards,
            &BTreeSet::from([
                DfsMetadataShardId::new("root"),
                DfsMetadataShardId::new("services"),
                DfsMetadataShardId::new("users"),
            ]),
            &local,
            1,
        )
        .expect("bounded authority migration");

        assert!(report.migration.reached_limit);
        assert_eq!(
            report.migration.migrated_objects, 0,
            "one physical mutation can establish the target filesystem but cannot complete an object bundle"
        );
        assert_eq!(
            crate::metadata_backend::metadata_journal_write_count()
                + crate::metadata_shard_index::object_shard_index_write_count(),
            1,
            "the pass cap applies to physical durable mutations, not only completed objects"
        );
        assert_eq!(
            report.migration.scanned_objects, 1,
            "catalog inspection must share the per-generation work bound"
        );
    }

    fn local_default_migration_shards(
        file_system_id: &DfsFileSystemId,
        local: &NodeId,
    ) -> Vec<DfsMetadataShardConfig> {
        vec![
            DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: "root".to_string(),
                primary_node_id: local.clone(),
                replica_node_ids: Vec::new(),
                epoch: 1,
                assignment: crate::DfsMetadataShardAssignment::Root,
                default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            },
            DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: "services".to_string(),
                primary_node_id: local.clone(),
                replica_node_ids: Vec::new(),
                epoch: 1,
                assignment: crate::DfsMetadataShardAssignment::PathPrefix {
                    prefix: crate::SIFS_SERVICES_PATH.to_string(),
                },
                default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            },
            DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: "users".to_string(),
                primary_node_id: local.clone(),
                replica_node_ids: Vec::new(),
                epoch: 1,
                assignment: crate::DfsMetadataShardAssignment::PathPrefix {
                    prefix: crate::SIFS_USERS_PATH.to_string(),
                },
                default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            },
        ]
    }

    fn run_test_default_migration_pass(
        data_dir: &SinomoDataDir,
        file_system: &DfsFileSystem,
        shards: &[DfsMetadataShardConfig],
        local: &NodeId,
        mutation_limit: usize,
    ) -> (
        DfsDefaultSifsMaterializationReport,
        usize,
        usize,
        usize,
        usize,
    ) {
        let metadata_root = crate::default_dfs_metadata_shards_root(data_dir);
        let bound_primary_backends =
            LocalShardedMetadataBackend::new(metadata_root.clone(), shards.to_vec())
                .expect("bound local primary backends")
                .into_shard_backends();
        reset_default_sifs_migration_max_buffered_records();
        crate::metadata_backend::reset_metadata_current_tables_snapshot_calls();
        crate::metadata_backend::reset_metadata_journal_write_count();
        crate::metadata_shard_index::reset_object_shard_index_write_count();
        let (report, _) = migrate_default_sifs_metadata_on_active_primary_shards(
            &metadata_root,
            bound_primary_backends,
            BTreeMap::new(),
            file_system,
            shards,
            &shards
                .iter()
                .map(|shard| DfsMetadataShardId::new(shard.shard_id.clone()))
                .collect(),
            local,
            mutation_limit,
        )
        .expect("bounded default migration pass");
        (
            report,
            crate::metadata_backend::metadata_current_tables_snapshot_calls(),
            default_sifs_migration_max_buffered_records(),
            crate::metadata_shard_index::object_shard_index_write_count(),
            crate::metadata_backend::metadata_journal_write_count(),
        )
    }

    fn run_active_default_materialization_pass(
        data_dir: &SinomoDataDir,
        file_system: &DfsFileSystem,
        shards: &[DfsMetadataShardConfig],
        local: &NodeId,
    ) -> DfsDefaultSifsMaterializationReport {
        materialize_default_sifs_metadata_on_active_primary_shards(
            crate::default_dfs_metadata_shards_root(data_dir),
            file_system,
            shards,
            &shards
                .iter()
                .map(|shard| DfsMetadataShardId::new(shard.shard_id.clone()))
                .collect(),
            local,
            [local.clone()],
            local.clone(),
            OffsetDateTime::UNIX_EPOCH,
        )
        .expect("active-primary default materialization")
    }

    #[test]
    fn reordered_policy_discovery_does_not_rejournal_default_filesystem_on_shards() {
        let root = sinomo_testdir::TestDir::new("dfs-default-policy-order-noop");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let local = NodeId::from_legacy_named("node_dfs_policy_order_noop").expect("node");
        let file_system = distributed_sifs(local.clone());
        let shards = local_default_migration_shards(&file_system.file_system_id, &local);

        let mut settled = false;
        for _ in 0..16 {
            let report =
                run_active_default_materialization_pass(&data_dir, &file_system, &shards, &local);
            if !report.migration.reached_limit {
                settled = true;
                break;
            }
        }
        assert!(settled, "initial bounded materialization must converge");

        let mut reordered = file_system.clone();
        reordered.subtree_policies.reverse();
        crate::metadata_backend::reset_metadata_journal_write_count();
        crate::metadata_shard_index::reset_object_shard_index_write_count();
        let verification =
            run_active_default_materialization_pass(&data_dir, &reordered, &shards, &local);

        assert!(!verification.migration.reached_limit);
        assert_eq!(verification.migration.migrated_objects, 0);
        assert_eq!(
            crate::metadata_backend::metadata_journal_write_count(),
            0,
            "a distinct-rule ordering change must not append PutFileSystem on any shard"
        );
        assert_eq!(
            crate::metadata_shard_index::object_shard_index_write_count(),
            0
        );

        let metadata_root = crate::default_dfs_metadata_shards_root(&data_dir);
        let mut sharded = LocalShardedMetadataBackend::new(metadata_root, shards.clone())
            .expect("reopen sharded backend");
        crate::metadata_backend::reset_metadata_journal_write_count();
        assert!(
            sharded
                .ensure_file_system(&reordered)
                .expect("ensure reordered filesystem")
                .is_none(),
            "the generic sharded ensure path must report a semantic no-op"
        );
        assert_eq!(
            crate::metadata_backend::metadata_journal_write_count(),
            0,
            "the generic sharded ensure path must not append PutFileSystem"
        );
    }

    #[test]
    fn exact_object_shard_index_is_a_noop() {
        let root = sinomo_testdir::TestDir::new("dfs-default-migration-exact-index-noop");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let local = NodeId::from_legacy_named("node_dfs_exact_index_noop").expect("node");
        let file_system = distributed_sifs(local.clone());
        let shards = local_default_migration_shards(&file_system.file_system_id, &local);
        let mut root_backend =
            open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("root"))
                .expect("root backend");
        root_backend
            .put_file_system(&file_system)
            .expect("filesystem");
        let mut object = crate::distributed_sifs_bootstrap_objects(local.clone())
            .into_iter()
            .next()
            .expect("bootstrap object");
        object.object_id =
            crate::DfsObjectId::from_legacy_named("obj_exact_index_noop").expect("object id");
        object.path = "/exact-index-noop".to_string();
        root_backend.put_object(&object).expect("root object");
        drop(root_backend);
        crate::metadata_shard_index::write_object_shard_index(
            &crate::default_dfs_metadata_shards_root(&data_dir),
            &object,
            &DfsMetadataShardId::new("root"),
        )
        .expect("seed exact index");

        let (report, snapshots, buffered, index_writes, _) =
            run_test_default_migration_pass(&data_dir, &file_system, &shards, &local, 8);
        assert!(!report.migration.reached_limit);
        assert_eq!(report.migration.scanned_objects, 1);
        assert_eq!(report.migration.migrated_objects, 0);
        assert_eq!(report.migration.indexed_objects, 0);
        assert_eq!(index_writes, 0, "an exact index must not be rewritten");
        assert!(snapshots <= 16, "unexpected snapshot work: {snapshots}");
        assert!(buffered <= 3, "unexpected buffered records: {buffered}");
    }

    #[test]
    fn newer_source_eviction_marker_never_overrides_live_target() {
        let root = sinomo_testdir::TestDir::new("dfs-default-migration-source-eviction-marker");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let local = NodeId::from_legacy_named("node_dfs_source_eviction_marker").expect("node");
        let file_system = distributed_sifs(local.clone());
        let shards = local_default_migration_shards(&file_system.file_system_id, &local);
        let metadata_root = crate::default_dfs_metadata_shards_root(&data_dir);

        let mut root_backend =
            open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("root"))
                .expect("root backend");
        root_backend
            .put_file_system(&file_system)
            .expect("filesystem");
        let mut object = crate::distributed_sifs_bootstrap_objects(local.clone())
            .into_iter()
            .next()
            .expect("bootstrap object");
        object.object_id =
            crate::DfsObjectId::from_legacy_named("obj_source_eviction_marker_canonical_target")
                .expect("object id");
        object.path = "/services/source-eviction-marker.txt".to_string();
        root_backend.put_object(&object).expect("legacy object");
        drop(root_backend);

        let mut settled = false;
        for _ in 0..16 {
            let report =
                run_active_default_materialization_pass(&data_dir, &file_system, &shards, &local);
            if !report.migration.reached_limit {
                settled = true;
                break;
            }
        }
        assert!(settled, "bounded materialization must converge");

        let source = open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("root"))
            .expect("source backend");
        let marker = source
            .get_object(&object.object_id)
            .expect("source lookup")
            .expect("source marker");
        let target =
            open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("services"))
                .expect("target backend");
        let live = target
            .get_object(&object.object_id)
            .expect("target lookup")
            .expect("live target");
        assert_ne!(live.kind, DfsObjectKind::Tombstone);
        assert_eq!(live.path, object.path);
        assert_eq!(marker.kind, DfsObjectKind::Tombstone);
        assert!(marker.changed_at_unix_nanos > live.changed_at_unix_nanos);
        assert_eq!(
            marker.tombstone_provenance,
            Some(crate::DfsTombstoneProvenance::SourceEviction {
                source_shard_id: DfsMetadataShardId::new("root"),
                target_shard_id: DfsMetadataShardId::new("services"),
                pin_transfer_id: None,
            })
        );
        drop(source);
        drop(target);

        crate::metadata_backend::reset_metadata_journal_write_count();
        crate::metadata_shard_index::reset_object_shard_index_write_count();
        let verification =
            run_active_default_materialization_pass(&data_dir, &file_system, &shards, &local);
        assert!(!verification.migration.reached_limit);
        assert_eq!(verification.migration.migrated_objects, 0);
        assert_eq!(crate::metadata_backend::metadata_journal_write_count(), 0);
        assert_eq!(
            crate::metadata_shard_index::object_shard_index_write_count(),
            0
        );

        let target =
            open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("services"))
                .expect("target backend after verification");
        assert_eq!(
            target
                .get_object(&object.object_id)
                .expect("target lookup after verification")
                .expect("live target after verification"),
            live,
            "a newer source-eviction marker is not authoritative deletion evidence"
        );
        assert!(
            crate::metadata_shard_index::object_shard_index_matches(
                &metadata_root,
                &live,
                &DfsMetadataShardId::new("services"),
            )
            .expect("exact target index")
        );

        // Cursors written before provenance was introduced contain the exact
        // historical marker shape. An exact target index plus the matching
        // live generation is sufficient to upgrade that old record without
        // mistaking an on-route unmarked tombstone for an eviction.
        let mut source =
            open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("root"))
                .expect("legacy source backend");
        let mut legacy_marker = source
            .get_object(&object.object_id)
            .expect("legacy source lookup")
            .expect("legacy source marker");
        legacy_marker.tombstone_provenance = None;
        source
            .put_object(&legacy_marker)
            .expect("seed pre-provenance source marker");
        drop(source);
        crate::metadata_backend::reset_metadata_journal_write_count();
        crate::metadata_shard_index::reset_object_shard_index_write_count();
        let legacy_verification =
            run_active_default_materialization_pass(&data_dir, &file_system, &shards, &local);
        assert!(!legacy_verification.migration.reached_limit);
        assert_eq!(legacy_verification.migration.migrated_objects, 0);
        assert_eq!(crate::metadata_backend::metadata_journal_write_count(), 0);
        assert_eq!(
            crate::metadata_shard_index::object_shard_index_write_count(),
            0
        );
        let target =
            open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("services"))
                .expect("target after legacy marker verification");
        assert_eq!(
            target
                .get_object(&object.object_id)
                .expect("target lookup after legacy marker verification")
                .expect("live target after legacy marker verification"),
            live
        );
    }

    #[test]
    fn migration_cursor_generation_tracks_replica_certification() {
        let local = NodeId::from_legacy_named("node_dfs_cursor_generation_local").expect("node");
        let remote = NodeId::from_legacy_named("node_dfs_cursor_generation_remote").expect("node");
        let file_system = distributed_sifs(local.clone());
        let mut shards = local_default_migration_shards(&file_system.file_system_id, &local);
        let services = shards
            .iter_mut()
            .find(|shard| shard.shard_id == "services")
            .expect("services shard");
        services.primary_node_id = remote.clone();
        services.replica_node_ids = vec![local.clone()];
        let active = BTreeSet::from([
            DfsMetadataShardId::new("root"),
            DfsMetadataShardId::new("users"),
        ]);
        let certified = BTreeSet::from([DfsMetadataShardId::new("services")]);
        let base = default_sifs_migration_generation(
            &file_system.file_system_id,
            &local,
            &shards,
            &active,
            &certified,
        );
        assert_ne!(
            default_sifs_migration_generation(
                &file_system.file_system_id,
                &local,
                &shards,
                &active,
                &BTreeSet::new(),
            ),
            base,
            "a replica certification transition must invalidate the cursor"
        );
    }

    #[test]
    fn large_default_migration_is_cursor_bounded_and_converges_without_prefix_rewrites() {
        const OBJECTS: usize = 300;
        const PASS_LIMIT: usize = 17;
        let root = sinomo_testdir::TestDir::new("dfs-default-migration-large-cursor");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let local = NodeId::from_legacy_named("node_dfs_large_cursor_migration").expect("node");
        let file_system = distributed_sifs(local.clone());
        let shards = local_default_migration_shards(&file_system.file_system_id, &local);
        let mut source =
            open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("root"))
                .expect("root source");
        source.put_file_system(&file_system).expect("filesystem");
        for index in 0..OBJECTS {
            let mut object = crate::distributed_sifs_bootstrap_objects(local.clone())
                .into_iter()
                .next()
                .expect("bootstrap object");
            object.object_id = crate::DfsObjectId::from_legacy_named(format!(
                "obj_large_cursor_migration_{index:04}"
            ))
            .expect("object id");
            object.path = format!("/services/legacy-{index:04}.txt");
            source.put_object(&object).expect("legacy object");
        }
        drop(source);

        let metadata_root = crate::default_dfs_metadata_shards_root(&data_dir);
        let cursor_path =
            default_sifs_migration_cursor_path(&metadata_root, &file_system.file_system_id);
        let mut previous_cursor = None;
        let mut total_migrated = 0usize;
        let mut total_indexed = 0usize;
        for pass in 0..64 {
            let (report, snapshots, buffered, index_writes, _) = run_test_default_migration_pass(
                &data_dir,
                &file_system,
                &shards,
                &local,
                PASS_LIMIT,
            );
            assert!(report.migration.scanned_objects <= PASS_LIMIT);
            assert!(report.migration.migrated_objects <= PASS_LIMIT);
            assert!(report.migration.indexed_objects <= PASS_LIMIT);
            assert_eq!(index_writes, report.migration.indexed_objects);
            assert!(
                snapshots <= PASS_LIMIT.saturating_mul(32).saturating_add(16),
                "pass {pass} performed unbounded snapshot work: {snapshots}"
            );
            assert!(
                buffered <= (PASS_LIMIT + 1).saturating_mul(shards.len()),
                "pass {pass} buffered too much catalog state: {buffered}"
            );
            total_migrated = total_migrated.saturating_add(report.migration.migrated_objects);
            total_indexed = total_indexed.saturating_add(report.migration.indexed_objects);
            if !report.migration.reached_limit {
                assert!(
                    !cursor_path.exists(),
                    "settled migration must clear its cursor"
                );
                break;
            }
            let cursor =
                crate::metadata_io::read_sobj::<DfsDefaultSifsMigrationCursor>(&cursor_path)
                    .expect("durable migration cursor");
            if let Some(after) = cursor.after_object_id {
                assert!(
                    previous_cursor
                        .as_ref()
                        .is_none_or(|previous| after >= *previous),
                    "pass {pass} regressed the durable object-id cursor"
                );
                previous_cursor = Some(after);
            } else {
                assert!(
                    cursor.pending_migration.is_some() || !cursor.dirty_object_ids.is_empty(),
                    "a partial pass without a catalog position must retain exact pending work"
                );
            }
        }
        assert_eq!(total_migrated, OBJECTS);
        assert_eq!(total_indexed, OBJECTS);

        // A complete verification sweep revisits every settled object after
        // the cursor was cleared. Exact indexes and target generations must
        // remain pure reads throughout the sweep.
        let mut verification_scanned = 0usize;
        for _ in 0..64 {
            let (report, _, _, index_writes, _) = run_test_default_migration_pass(
                &data_dir,
                &file_system,
                &shards,
                &local,
                PASS_LIMIT,
            );
            verification_scanned =
                verification_scanned.saturating_add(report.migration.scanned_objects);
            assert_eq!(report.migration.migrated_objects, 0);
            assert_eq!(report.migration.indexed_objects, 0);
            assert_eq!(index_writes, 0, "settled prefixes must not be rewritten");
            if !report.migration.reached_limit {
                break;
            }
        }
        assert_eq!(verification_scanned, OBJECTS);
    }

    #[test]
    fn bounded_materialization_retains_bound_backends_across_continuations() {
        const OBJECTS: usize = 120;
        let root = sinomo_testdir::TestDir::new("dfs-default-migration-retained-backends");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let local =
            NodeId::from_legacy_named("node_dfs_retained_migration_backends").expect("node");
        let file_system = distributed_sifs(local.clone());
        let shards = local_default_migration_shards(&file_system.file_system_id, &local);
        let mut source =
            open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("root"))
                .expect("root source");
        source.put_file_system(&file_system).expect("filesystem");
        for index in 0..OBJECTS {
            let mut object = crate::distributed_sifs_bootstrap_objects(local.clone())
                .into_iter()
                .next()
                .expect("bootstrap object");
            object.object_id = crate::DfsObjectId::from_legacy_named(format!(
                "obj_retained_migration_backend_{index:04}"
            ))
            .expect("object id");
            object.path = format!("/services/retained-{index:04}.txt");
            source.put_object(&object).expect("legacy object");
        }
        drop(source);

        crate::metadata_backend::reset_metadata_backend_open_calls();
        let mut report =
            run_active_default_materialization_pass(&data_dir, &file_system, &shards, &local);
        assert!(report.migration.reached_limit);
        assert_eq!(
            crate::metadata_backend::metadata_backend_open_calls(),
            shards.len(),
            "the first authority-bound pass opens each local primary exactly once"
        );

        for pass in 1..32 {
            crate::metadata_backend::reset_metadata_backend_open_calls();
            report =
                run_active_default_materialization_pass(&data_dir, &file_system, &shards, &local);
            assert_eq!(
                crate::metadata_backend::metadata_backend_open_calls(),
                0,
                "continuation pass {pass} reopened and rehydrated an authority-bound backend"
            );
            if !report.migration.reached_limit {
                break;
            }
        }
        assert!(
            !report.migration.reached_limit,
            "retained bounded migration must converge"
        );
    }

    #[test]
    fn durable_journal_replays_earlier_object_without_resetting_catalog_cursor() {
        const OBJECTS: usize = 40;
        const PASS_LIMIT: usize = 8;
        let root = sinomo_testdir::TestDir::new("dfs-default-migration-dirty-earlier-id");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let local = NodeId::from_legacy_named("node_dfs_dirty_earlier_id").expect("node");
        let file_system = distributed_sifs(local.clone());
        let shards = local_default_migration_shards(&file_system.file_system_id, &local);
        let metadata_root = crate::default_dfs_metadata_shards_root(&data_dir);
        let cursor_path =
            default_sifs_migration_cursor_path(&metadata_root, &file_system.file_system_id);
        let mut source =
            open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("root"))
                .expect("root source");
        source.put_file_system(&file_system).expect("filesystem");
        let mut objects = Vec::new();
        for index in 0..OBJECTS {
            let mut object = crate::distributed_sifs_bootstrap_objects(local.clone())
                .into_iter()
                .next()
                .expect("bootstrap object");
            object.object_id =
                crate::DfsObjectId::from_legacy_named(format!("obj_dirty_earlier_id_{index:04}"))
                    .expect("object id");
            object.path = format!("/services/dirty-earlier-{index:04}.txt");
            source.put_object(&object).expect("legacy object");
            objects.push(object);
        }
        drop(source);
        objects.sort_by(|left, right| left.object_id.cmp(&right.object_id));
        let early_object = objects.first().expect("early object").clone();

        // The direct helper deliberately drops every returned backend
        // continuation. Each pass therefore exercises recovery solely from
        // the durable cursor and journal checkpoints, as after a process
        // crash.
        let mut cursor_after_mutated_id = None;
        for _ in 0..32 {
            let _ = run_test_default_migration_pass(
                &data_dir,
                &file_system,
                &shards,
                &local,
                PASS_LIMIT,
            );
            if !cursor_path.exists() {
                continue;
            }
            let cursor =
                crate::metadata_io::read_sobj::<DfsDefaultSifsMigrationCursor>(&cursor_path)
                    .expect("durable migration cursor");
            let Some(after) = cursor.after_object_id else {
                continue;
            };
            if after <= early_object.object_id {
                continue;
            }
            let services =
                open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("services"))
                    .expect("services backend");
            if services
                .get_object(&early_object.object_id)
                .expect("services lookup")
                .is_some_and(|object| object.kind != DfsObjectKind::Tombstone)
            {
                cursor_after_mutated_id = Some(after);
                break;
            }
        }
        let cursor_after_mutated_id =
            cursor_after_mutated_id.expect("catalog cursor must advance past the early object");

        let mut services =
            open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("services"))
                .expect("services writer");
        let mut moved = services
            .get_object(&early_object.object_id)
            .expect("services lookup")
            .expect("migrated early object");
        moved.path = "/users/replayed-after-cursor.txt".to_string();
        moved.version =
            DfsVersion::from_legacy_named("version_replayed_after_cursor").expect("moved version");
        moved.changed_at_unix_nanos = moved.changed_at_unix_nanos.saturating_add(10_000);
        moved.tombstone_provenance = None;
        services
            .put_object(&moved)
            .expect("concurrent earlier-ID mutation");
        drop(services);

        let mut settled = false;
        for pass in 0..128 {
            let (report, _, _, _, _) = run_test_default_migration_pass(
                &data_dir,
                &file_system,
                &shards,
                &local,
                PASS_LIMIT,
            );
            if !report.migration.reached_limit {
                settled = true;
                break;
            }
            let cursor =
                crate::metadata_io::read_sobj::<DfsDefaultSifsMigrationCursor>(&cursor_path)
                    .expect("durable replay cursor");
            assert!(
                cursor
                    .after_object_id
                    .as_ref()
                    .is_some_and(|after| after >= &cursor_after_mutated_id),
                "pass {pass} reset the ordered catalog to replay an earlier dirty ID"
            );
        }
        assert!(settled, "dirty-ID replay must converge after restart");

        let users = open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("users"))
            .expect("users target");
        assert_eq!(
            users
                .get_object(&moved.object_id)
                .expect("users lookup")
                .expect("replayed target"),
            moved
        );
        assert!(
            crate::metadata_shard_index::object_shard_index_matches(
                &metadata_root,
                &moved,
                &DfsMetadataShardId::new("users"),
            )
            .expect("replayed target index")
        );
    }

    #[test]
    fn source_update_after_target_install_is_replayed_before_eviction() {
        const PASS_LIMIT: usize = 2;
        let root = sinomo_testdir::TestDir::new("dfs-default-migration-source-update-fence");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let local = NodeId::from_legacy_named("node_dfs_source_update_fence").expect("node");
        let file_system = distributed_sifs(local.clone());
        let shards = local_default_migration_shards(&file_system.file_system_id, &local);
        let metadata_root = crate::default_dfs_metadata_shards_root(&data_dir);
        let cursor_path =
            default_sifs_migration_cursor_path(&metadata_root, &file_system.file_system_id);
        let mut source =
            open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("root"))
                .expect("root source");
        source
            .put_file_system(&file_system)
            .expect("root filesystem");
        let mut object = crate::distributed_sifs_bootstrap_objects(local.clone())
            .into_iter()
            .next()
            .expect("bootstrap object");
        object.object_id =
            crate::DfsObjectId::from_legacy_named("obj_source_update_fence").expect("object id");
        object.path = "/services/source-update-fence.txt".to_string();
        source.put_object(&object).expect("legacy object");
        drop(source);
        let mut services =
            open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("services"))
                .expect("services target");
        services
            .put_file_system(&file_system)
            .expect("services filesystem");
        drop(services);

        let (first, _, _, _, _) =
            run_test_default_migration_pass(&data_dir, &file_system, &shards, &local, PASS_LIMIT);
        assert!(first.migration.reached_limit);
        let cursor = crate::metadata_io::read_sobj::<DfsDefaultSifsMigrationCursor>(&cursor_path)
            .expect("pending source-eviction cursor");
        assert!(cursor.pending_migration.as_ref().is_some_and(|pending| {
            matches!(
                pending.stage,
                DfsDefaultSifsPendingMigrationStage::SourceEvictions { .. }
            )
        }));
        let services =
            open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("services"))
                .expect("installed target");
        assert_eq!(
            services
                .get_object(&object.object_id)
                .expect("target lookup")
                .expect("installed target"),
            object
        );
        drop(services);

        let mut source =
            open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("root"))
                .expect("concurrent source writer");
        let mut moved = source
            .get_object(&object.object_id)
            .expect("source lookup")
            .expect("live source");
        moved.path = "/users/source-update-fence.txt".to_string();
        moved.version =
            DfsVersion::from_legacy_named("version_source_update_fence").expect("version");
        moved.changed_at_unix_nanos = moved.changed_at_unix_nanos.saturating_add(1_000);
        source.put_object(&moved).expect("concurrent source update");
        drop(source);

        let _ =
            run_test_default_migration_pass(&data_dir, &file_system, &shards, &local, PASS_LIMIT);
        let source = open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("root"))
            .expect("source after fenced retry");
        assert_eq!(
            source
                .get_object(&moved.object_id)
                .expect("source lookup after fenced retry")
                .expect("source update retained"),
            moved,
            "a source generation changed after target install must restart instead of being tombstoned"
        );
        drop(source);

        let mut settled = false;
        for _ in 0..64 {
            let (report, _, _, _, _) = run_test_default_migration_pass(
                &data_dir,
                &file_system,
                &shards,
                &local,
                PASS_LIMIT,
            );
            if !report.migration.reached_limit {
                settled = true;
                break;
            }
        }
        assert!(settled, "restarted exact-source migration must converge");
        let users = open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("users"))
            .expect("users target");
        assert_eq!(
            users
                .get_object(&moved.object_id)
                .expect("users lookup")
                .expect("moved target"),
            moved
        );
        assert!(
            crate::metadata_shard_index::object_shard_index_matches(
                &metadata_root,
                &moved,
                &DfsMetadataShardId::new("users"),
            )
            .expect("moved target index")
        );
    }

    #[test]
    fn late_source_family_records_reach_target_before_source_eviction() {
        const PASS_LIMIT: usize = 2;
        let root = sinomo_testdir::TestDir::new("dfs-default-migration-late-family-fence");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let local = NodeId::from_legacy_named("node_dfs_late_family_fence").expect("node");
        let file_system = distributed_sifs(local.clone());
        let shards = local_default_migration_shards(&file_system.file_system_id, &local);
        let metadata_root = crate::default_dfs_metadata_shards_root(&data_dir);
        let cursor_path =
            default_sifs_migration_cursor_path(&metadata_root, &file_system.file_system_id);
        let mut source =
            open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("root"))
                .expect("root source");
        source.put_file_system(&file_system).expect("filesystem");
        let mut object = crate::distributed_sifs_bootstrap_objects(local.clone())
            .into_iter()
            .next()
            .expect("bootstrap object");
        object.object_id =
            crate::DfsObjectId::from_legacy_named("obj_late_family_fence").expect("object id");
        object.path = "/services/late-family-fence.bin".to_string();
        source.put_object(&object).expect("legacy object");
        drop(source);
        let mut target =
            open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("services"))
                .expect("services target");
        target.put_file_system(&file_system).expect("filesystem");
        drop(target);

        let (first, _, _, _, _) =
            run_test_default_migration_pass(&data_dir, &file_system, &shards, &local, PASS_LIMIT);
        assert!(first.migration.reached_limit);
        let cursor = crate::metadata_io::read_sobj::<DfsDefaultSifsMigrationCursor>(&cursor_path)
            .expect("pending source eviction");
        assert!(cursor.pending_migration.as_ref().is_some_and(|pending| {
            matches!(
                pending.stage,
                DfsDefaultSifsPendingMigrationStage::SourceEvictions { .. }
            )
        }));

        let late_version = crate::DfsObjectVersionRecord {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: DfsVersion::from_legacy_named("version_late_family_fence").expect("version"),
            content_hash: Some("blake3:late-family-fence".to_string()),
            size_bytes: Some(17),
            locations: vec![local.clone()],
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        let late_intent = crate::DfsWriteIntentRecord {
            intent_id: "intent_late_family_fence".to_string(),
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            base_version: Some(object.version.clone()),
            target_version: late_version.version.clone(),
            coordinator_node_id: local.clone(),
            owner_epoch: 1,
            desired_replica_node_ids: vec![local.clone()],
            policy: crate::DfsReplicationPolicy::new(1, 1).expect("policy"),
            status: crate::DfsWriteIntentStatus::Staging,
            content_hash: late_version.content_hash.clone(),
            object_metadata: None,
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: None,
        };
        let mut source =
            open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("root"))
                .expect("late source writer");
        source.put_version(&late_version).expect("late version");
        source.put_intent(&late_intent).expect("late intent");
        drop(source);

        let mut settled = false;
        let mut observed_eviction = false;
        for pass in 0..32 {
            let (report, _, _, index_writes, journal_writes) = run_test_default_migration_pass(
                &data_dir,
                &file_system,
                &shards,
                &local,
                PASS_LIMIT,
            );
            assert!(
                index_writes.saturating_add(journal_writes) <= PASS_LIMIT,
                "pass {pass} exceeded the physical mutation cap"
            );
            let source =
                open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("root"))
                    .expect("source inspection");
            let target =
                open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("services"))
                    .expect("target inspection");
            let source_object = source
                .get_object(&object.object_id)
                .expect("source lookup")
                .expect("source object or marker");
            if source_object.kind == DfsObjectKind::Tombstone {
                observed_eviction = true;
                assert_eq!(
                    target
                        .get_version(&object.object_id, &late_version.version)
                        .expect("target version"),
                    Some(late_version.clone()),
                    "source eviction must be later than the late version install"
                );
                assert_eq!(
                    target
                        .get_intent(&object.object_id, &late_intent.intent_id)
                        .expect("target intent"),
                    Some(late_intent.clone()),
                    "source eviction must be later than the late intent install"
                );
            }
            if !report.migration.reached_limit {
                settled = true;
                break;
            }
        }
        assert!(settled, "late-family replay must converge");
        assert!(observed_eviction, "source must eventually be evicted");
    }

    #[test]
    fn dirty_replay_bounds_journal_entries_even_for_one_repeated_object() {
        const PASS_LIMIT: usize = 4;
        let root = sinomo_testdir::TestDir::new("dfs-default-migration-journal-entry-budget");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let local = NodeId::from_legacy_named("node_dfs_journal_entry_budget").expect("node");
        let file_system = distributed_sifs(local.clone());
        let shards = local_default_migration_shards(&file_system.file_system_id, &local);
        let metadata_root = crate::default_dfs_metadata_shards_root(&data_dir);
        let cursor_path =
            default_sifs_migration_cursor_path(&metadata_root, &file_system.file_system_id);
        let mut backend =
            open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("root"))
                .expect("root backend");
        backend.put_file_system(&file_system).expect("filesystem");
        let mut repeated = None;
        for index in 0..20 {
            let mut object = crate::distributed_sifs_bootstrap_objects(local.clone())
                .into_iter()
                .next()
                .expect("bootstrap object");
            object.object_id = crate::DfsObjectId::from_legacy_named(format!(
                "obj_journal_entry_budget_{index:02}"
            ))
            .expect("object id");
            object.path = format!("/journal-entry-budget-{index:02}");
            backend.put_object(&object).expect("object");
            repeated.get_or_insert(object);
        }
        drop(backend);

        let (first, _, _, _, _) =
            run_test_default_migration_pass(&data_dir, &file_system, &shards, &local, PASS_LIMIT);
        assert!(first.migration.reached_limit);
        let before = crate::metadata_io::read_sobj::<DfsDefaultSifsMigrationCursor>(&cursor_path)
            .expect("initial durable cursor");
        let before_sequence = before
            .observed_journal_sequences
            .get("root")
            .copied()
            .expect("root journal checkpoint");

        let mut backend =
            open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("root"))
                .expect("root writer");
        let mut repeated = repeated.expect("repeated object");
        for generation in 1..=20_u64 {
            repeated.changed_at_unix_nanos =
                repeated.changed_at_unix_nanos.saturating_add(generation);
            backend.put_object(&repeated).expect("repeated mutation");
        }
        drop(backend);

        let (second, _, _, _, _) =
            run_test_default_migration_pass(&data_dir, &file_system, &shards, &local, PASS_LIMIT);
        assert!(second.migration.reached_limit);
        let after = crate::metadata_io::read_sobj::<DfsDefaultSifsMigrationCursor>(&cursor_path)
            .expect("bounded replay cursor");
        let after_sequence = after
            .observed_journal_sequences
            .get("root")
            .copied()
            .expect("advanced root checkpoint");
        assert_eq!(
            after_sequence.saturating_sub(before_sequence),
            PASS_LIMIT as u64,
            "repeated mutations of one ID must not bypass the per-pass journal-entry budget"
        );
    }

    #[test]
    fn large_object_family_respects_physical_pass_cap_before_source_eviction() {
        const FAMILY_RECORDS: usize = 9;
        const PASS_LIMIT: usize = 5;
        let root = sinomo_testdir::TestDir::new("dfs-default-migration-large-family");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let local = NodeId::from_legacy_named("node_dfs_large_family").expect("node");
        let file_system = distributed_sifs(local.clone());
        let shards = local_default_migration_shards(&file_system.file_system_id, &local);
        let metadata_root = crate::default_dfs_metadata_shards_root(&data_dir);
        let mut source =
            open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("root"))
                .expect("root source");
        source.put_file_system(&file_system).expect("filesystem");
        let mut object = crate::distributed_sifs_bootstrap_objects(local.clone())
            .into_iter()
            .next()
            .expect("bootstrap object");
        object.object_id =
            crate::DfsObjectId::from_legacy_named("obj_large_migration_family").expect("object id");
        object.path = "/services/large-family.bin".to_string();
        source.put_object(&object).expect("legacy object");

        for index in 0..FAMILY_RECORDS {
            let version =
                DfsVersion::from_legacy_named(format!("version_large_migration_family_{index:02}"))
                    .expect("version");
            source
                .put_version(&crate::DfsObjectVersionRecord {
                    file_system_id: object.file_system_id.clone(),
                    object_id: object.object_id.clone(),
                    version: version.clone(),
                    content_hash: Some(format!("blake3:large-family-{index:02}")),
                    size_bytes: Some(index as u64),
                    locations: vec![local.clone()],
                    created_by_event_id: None,
                    created_at: OffsetDateTime::UNIX_EPOCH,
                    visible: true,
                })
                .expect("version record");
            source
                .put_receipt(&crate::DfsReplicaReceiptRecord {
                    receipt: sinomo_core::DfsWriteReceipt {
                        file_system_id: object.file_system_id.clone(),
                        object_id: object.object_id.clone(),
                        committed_version: version.clone(),
                        owner_node_id: object.owner_node_id.clone(),
                        event_id: sinomo_core::events::EventId::from_legacy_named(format!(
                            "evt_large_migration_family_{index:02}"
                        ))
                        .expect("receipt event"),
                    },
                    replica_node_id: NodeId::from_legacy_named(format!(
                        "node_large_migration_family_replica_{index:02}"
                    ))
                    .expect("replica node"),
                    content_hash: Some(format!("blake3:large-family-{index:02}")),
                    received_at: OffsetDateTime::UNIX_EPOCH,
                })
                .expect("receipt");
            source
                .put_intent(&crate::DfsWriteIntentRecord {
                    intent_id: format!("intent_large_migration_family_{index:02}"),
                    file_system_id: object.file_system_id.clone(),
                    object_id: object.object_id.clone(),
                    base_version: None,
                    target_version: version.clone(),
                    coordinator_node_id: local.clone(),
                    owner_epoch: 1,
                    desired_replica_node_ids: vec![local.clone()],
                    policy: crate::DfsReplicationPolicy::new(1, 1).expect("policy"),
                    status: crate::DfsWriteIntentStatus::Staging,
                    content_hash: None,
                    object_metadata: None,
                    touch_parent_on_create: false,
                    create_predecessor_object_id: None,
                    created_at: OffsetDateTime::UNIX_EPOCH,
                    updated_at: None,
                })
                .expect("intent");
            source
                .put_conflict(&crate::DfsConflictRecord {
                    conflict_id: format!("conflict_large_migration_family_{index:02}"),
                    file_system_id: object.file_system_id.clone(),
                    object_id: object.object_id.clone(),
                    path: object.path.clone(),
                    base_version: None,
                    local_version: version.clone(),
                    remote_version: object.version.clone(),
                    detected_by_node_id: local.clone(),
                    reason: "bounded family migration".to_string(),
                    local_content_hash: None,
                    remote_content_hash: None,
                    created_by_intent_id: None,
                    strategy: None,
                    status: crate::DfsConflictStatus::Open,
                    detected_at: OffsetDateTime::UNIX_EPOCH,
                })
                .expect("conflict");
        }
        drop(source);

        let mut settled = false;
        let mut source_evicted = false;
        for pass in 0..64 {
            let (report, _, buffered, index_writes, journal_writes) =
                run_test_default_migration_pass(
                    &data_dir,
                    &file_system,
                    &shards,
                    &local,
                    PASS_LIMIT,
                );
            assert!(
                index_writes.saturating_add(journal_writes) <= PASS_LIMIT,
                "pass {pass} exceeded the physical durable-mutation cap: {journal_writes} journal + {index_writes} index"
            );
            assert!(report.migration.scanned_objects <= PASS_LIMIT);
            assert!(
                buffered <= (PASS_LIMIT + 1).saturating_mul(shards.len()),
                "pass {pass} buffered an unbounded object family: {buffered}"
            );

            let target =
                open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("services"))
                    .expect("services target");
            let target_family = crate::metadata_shard_migration::collect_object_metadata_family(
                &target,
                &object.object_id,
                4,
                None,
            )
            .expect("target family");
            let source =
                open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("root"))
                    .expect("source inspection");
            let source_object = source
                .get_object(&object.object_id)
                .expect("source lookup")
                .expect("source object or marker");
            if source_object.kind == DfsObjectKind::Tombstone {
                source_evicted = true;
                assert_eq!(target_family.versions.len(), FAMILY_RECORDS);
                assert_eq!(target_family.receipts.len(), FAMILY_RECORDS);
                assert_eq!(target_family.intents.len(), FAMILY_RECORDS);
                assert_eq!(target_family.conflicts.len(), FAMILY_RECORDS);
                let target_object = target
                    .get_object(&object.object_id)
                    .expect("target lookup")
                    .expect("target object before eviction");
                assert_ne!(target_object.kind, DfsObjectKind::Tombstone);
                assert!(
                    crate::metadata_shard_index::object_shard_index_matches(
                        &metadata_root,
                        &target_object,
                        &DfsMetadataShardId::new("services"),
                    )
                    .expect("target index before eviction")
                );
            }
            if !report.migration.reached_limit {
                settled = true;
                break;
            }
        }
        assert!(settled, "bounded large-family migration must converge");
        assert!(
            source_evicted,
            "the source is evicted only after the complete family becomes durable"
        );
    }

    #[test]
    fn default_migration_pages_large_version_pin_sets_within_the_pass_budget() {
        const PIN_COUNT: usize = 40;
        const PASS_LIMIT: usize = 5;
        let root = sinomo_testdir::TestDir::new("dfs-default-migration-version-pin-pages");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let local = NodeId::from_legacy_named("node_dfs_version_pin_pages").expect("node");
        let file_system = distributed_sifs(local.clone());
        let shards = local_default_migration_shards(&file_system.file_system_id, &local);
        let metadata_root = crate::default_dfs_metadata_shards_root(&data_dir);
        let cursor_path =
            default_sifs_migration_cursor_path(&metadata_root, &file_system.file_system_id);
        let mut source =
            open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("root"))
                .expect("root source");
        source.put_file_system(&file_system).expect("filesystem");
        let mut object = crate::distributed_sifs_bootstrap_objects(local.clone())
            .into_iter()
            .next()
            .expect("bootstrap object");
        object.object_id =
            crate::DfsObjectId::from_legacy_named("obj_version_pin_pages").expect("object id");
        object.path = "/services/version-pin-pages.bin".to_string();
        source.put_object(&object).expect("legacy object");
        let version = crate::object_version_record(&object, object.content_size_bytes);
        source.put_version(&version).expect("pinned version");
        for index in 0..PIN_COUNT {
            source
                .put_version_pin(&crate::DfsMetadataVersionPinRecord {
                    file_system_id: object.file_system_id.clone(),
                    object_id: object.object_id.clone(),
                    version: version.version.clone(),
                    pin_id: format!("checkpoint-{index:03}"),
                    created_at: OffsetDateTime::UNIX_EPOCH,
                })
                .expect("put pin")
                .expect("new pin");
        }
        drop(source);

        let mut observed_partial_pin_page = false;
        for pass in 0..32 {
            let (report, _, buffered, index_writes, journal_writes) =
                run_test_default_migration_pass(
                    &data_dir,
                    &file_system,
                    &shards,
                    &local,
                    PASS_LIMIT,
                );
            assert!(
                index_writes.saturating_add(journal_writes) <= PASS_LIMIT,
                "pass {pass} exceeded the physical mutation cap"
            );
            assert!(
                buffered <= (PASS_LIMIT + 1).saturating_mul(shards.len()),
                "pass {pass} buffered an unbounded pin page"
            );
            let target =
                open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("services"))
                    .expect("target");
            let copied = target
                .version_pins_for_object(&object.object_id)
                .expect("target pins")
                .len();
            if copied > 0 && copied < PIN_COUNT {
                let cursor =
                    crate::metadata_io::read_sobj::<DfsDefaultSifsMigrationCursor>(&cursor_path)
                        .expect("durable migration cursor");
                assert!(matches!(
                    cursor.pending_migration.map(|pending| pending.stage),
                    Some(DfsDefaultSifsPendingMigrationStage::VersionPins { after: Some(_) })
                ));
                observed_partial_pin_page = true;
                break;
            }
            assert!(report.migration.reached_limit);
        }
        assert!(
            observed_partial_pin_page,
            "a large pin set must yield with a durable composite cursor before full materialization"
        );
    }

    #[test]
    fn default_migration_preserves_active_pin_transfer_provenance_exactly() {
        const PASS_LIMIT: usize = 4;
        let root = sinomo_testdir::TestDir::new("dfs-default-migration-pin-transfer");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let local = NodeId::from_legacy_named("node_dfs_pin_transfer_migration").expect("node");
        let file_system = distributed_sifs(local.clone());
        let shards = local_default_migration_shards(&file_system.file_system_id, &local);
        let mut source =
            open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("root"))
                .expect("root source");
        source.put_file_system(&file_system).expect("filesystem");
        let mut object = crate::distributed_sifs_bootstrap_objects(local)
            .into_iter()
            .next()
            .expect("bootstrap object");
        object.object_id =
            crate::DfsObjectId::from_legacy_named("obj_pin_transfer_migration").expect("object id");
        object.path = "/services/pin-transfer-migration.bin".to_string();
        source.put_object(&object).expect("legacy object");
        let version = crate::object_version_record(&object, object.content_size_bytes);
        source.put_version(&version).expect("version");
        let preexisting_pin = crate::DfsMetadataVersionPinRecord {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: version.version.clone(),
            pin_id: "checkpoint-preexisting-before-transfer".to_string(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        };
        source
            .put_version_pin(&preexisting_pin)
            .expect("preexisting pin")
            .expect("new preexisting pin");
        let copied_pin = crate::DfsMetadataVersionPinRecord {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: version.version.clone(),
            pin_id: "checkpoint-copied-by-transfer".to_string(),
            created_at: OffsetDateTime::UNIX_EPOCH,
        };
        let source_pins = vec![preexisting_pin.clone(), copied_pin.clone()];
        assert!(
            source
                .reconcile_cross_shard_pin_transfer(
                    "active-transfer-before-topology-change",
                    &object,
                    "/legacy/pin-transfer-migration.bin",
                    std::slice::from_ref(&version),
                    &source_pins,
                )
                .expect("install source transfer")
        );
        let expected_transfer = source
            .cross_shard_pin_transfers_for_object(&object.object_id)
            .expect("source transfer")
            .into_iter()
            .next()
            .expect("active source transfer");
        assert_eq!(expected_transfer.source_pins.len(), 2);
        assert_eq!(
            expected_transfer.copied_pins,
            vec![copied_pin.clone()],
            "the source transfer must distinguish its copied pin from an acknowledged pre-existing pin"
        );
        drop(source);

        let mut settled = false;
        for _ in 0..64 {
            let (report, _, _, _, _) = run_test_default_migration_pass(
                &data_dir,
                &file_system,
                &shards,
                &NodeId::from_legacy_named("node_dfs_pin_transfer_migration").expect("node"),
                PASS_LIMIT,
            );
            if !report.migration.reached_limit {
                settled = true;
                break;
            }
        }
        assert!(
            settled,
            "bounded migration with active transfer must converge"
        );

        let target =
            open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("services"))
                .expect("target");
        assert_eq!(
            target
                .cross_shard_pin_transfers_for_object(&object.object_id)
                .expect("target transfer"),
            vec![expected_transfer],
            "authoritative migration must preserve transfer identity, timestamps, and ownership"
        );
        let mut expected_pins = source_pins;
        expected_pins.sort();
        let mut target_pins = target
            .version_pins_for_object(&object.object_id)
            .expect("target pins");
        target_pins.sort();
        assert_eq!(
            target_pins, expected_pins,
            "both copied and independently owned acknowledged pins remain durable on the target"
        );

        let source = open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("root"))
            .expect("source after eviction");
        assert!(
            source
                .cross_shard_pin_transfers_for_object(&object.object_id)
                .expect("source transfers after eviction")
                .is_empty(),
            "source provenance is removed atomically with source eviction"
        );
        assert!(
            source
                .version_pins_for_object(&object.object_id)
                .expect("source pins after eviction")
                .is_empty(),
            "source-owned pins are removed atomically with source eviction"
        );
    }

    #[test]
    fn index_only_catalog_mutations_share_the_cursor_budget() {
        const OBJECTS: usize = 65;
        const PASS_LIMIT: usize = 7;
        let root = sinomo_testdir::TestDir::new("dfs-default-migration-index-budget");
        let data_dir = SinomoDataDir::new(root.join("data"));
        data_dir.ensure_root_layout().expect("data dir");
        let local = NodeId::from_legacy_named("node_dfs_index_budget").expect("node");
        let file_system = distributed_sifs(local.clone());
        let shards = local_default_migration_shards(&file_system.file_system_id, &local);
        let mut backend =
            open_local_metadata_shard_backend(&data_dir, DfsMetadataShardId::new("root"))
                .expect("root backend");
        backend.put_file_system(&file_system).expect("filesystem");
        for index in 0..OBJECTS {
            let mut object = crate::distributed_sifs_bootstrap_objects(local.clone())
                .into_iter()
                .next()
                .expect("bootstrap object");
            object.object_id =
                crate::DfsObjectId::from_legacy_named(format!("obj_index_budget_{index:04}"))
                    .expect("object id");
            object.path = format!("/index-budget-{index:04}");
            backend
                .put_object(&object)
                .expect("correctly routed object");
        }
        drop(backend);

        let mut total_indexed = 0usize;
        for _ in 0..32 {
            let (report, _, _, index_writes, _) = run_test_default_migration_pass(
                &data_dir,
                &file_system,
                &shards,
                &local,
                PASS_LIMIT,
            );
            assert!(report.migration.scanned_objects <= PASS_LIMIT);
            assert_eq!(report.migration.migrated_objects, 0);
            assert!(report.migration.indexed_objects <= PASS_LIMIT);
            assert_eq!(index_writes, report.migration.indexed_objects);
            total_indexed = total_indexed.saturating_add(report.migration.indexed_objects);
            if !report.migration.reached_limit {
                break;
            }
        }
        assert_eq!(total_indexed, OBJECTS);
    }

    #[test]
    fn journal_discontinuity_rejection_allows_snapshot_repair() {
        assert!(metadata_journal_rejection_allows_snapshot_repair(
            "metadata serialization error: metadata journal batch for shard services starts unseen history at sequence 7722 but durable replay requires 1"
        ));
        assert!(metadata_journal_rejection_allows_snapshot_repair(
            "metadata authority batch for shard root starts unseen history at sequence 9 but durable replay requires sequence 2"
        ));
        assert!(metadata_journal_rejection_allows_snapshot_repair(
            "metadata serialization error: metadata shard root cannot extend prefix 2216 with journal entry root:2216"
        ));
        assert!(metadata_journal_rejection_allows_snapshot_repair(
            "metadata serialization error: metadata journal sequence 14159 is within durable prefix 14162 but is missing while replaying shard root"
        ));
        assert!(metadata_journal_rejection_allows_snapshot_repair(
            "metadata serialization error: DFS metadata table snapshot semantic checksum mismatch"
        ));
        assert!(!metadata_journal_rejection_allows_snapshot_repair(
            "metadata journal batch for shard services contains entry from shard root"
        ));
    }

    #[test]
    fn semantic_checksum_corrupt_replica_recovers_via_authoritative_snapshot() {
        let target = sinomo_testdir::TestDir::new("sinomo-dfs-corrupt-replica-recovery");
        let source_data_dir = SinomoDataDir::new(target.path().join("source"));
        let receiver_data_dir = SinomoDataDir::new(target.path().join("receiver"));
        source_data_dir
            .ensure_root_layout()
            .expect("source root layout");
        receiver_data_dir
            .ensure_root_layout()
            .expect("receiver root layout");
        let source_node_id =
            NodeId::from_legacy_named("node_corrupt_replica_source").expect("source node");
        let receiver_node_id =
            NodeId::from_legacy_named("node_corrupt_replica_receiver").expect("receiver node");
        let shard_id = DfsMetadataShardId::new("root");
        let file_system = DfsFileSystem::sifs(source_node_id.clone());
        let authoritative_display_name = file_system.display_name.clone();

        let mut source_backend = LocalSobjMetadataBackend::new(
            shard_id.clone(),
            local_dfs_metadata_shard_path(&source_data_dir, &shard_id),
        )
        .expect("source backend");
        source_backend
            .put_file_system(&file_system)
            .expect("authoritative filesystem");
        let source_sequence = source_backend.journal_last_sequence();
        let authoritative_snapshot = source_backend.export_table_snapshot();

        let receiver_root = local_dfs_metadata_shard_path(&receiver_data_dir, &shard_id);
        let mut receiver_backend = LocalSobjMetadataBackend::new(shard_id.clone(), &receiver_root)
            .expect("receiver backend");
        receiver_backend
            .replace_with_replicated_snapshot(
                shard_id.clone(),
                source_sequence,
                authoritative_snapshot,
            )
            .expect("seed receiver snapshot");
        drop(receiver_backend);

        let shard = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: shard_id.0.clone(),
            primary_node_id: source_node_id.clone(),
            replica_node_ids: vec![receiver_node_id.clone()],
            epoch: 1,
            assignment: crate::DfsMetadataShardAssignment::Root,
            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        };
        configure_replica_shard(&receiver_data_dir, &receiver_node_id, &shard);

        let mut payloads_by_after_sequence = BTreeMap::new();
        let target_publish = match prepare_dfs_metadata_journal_target_publish(
            &mut source_backend,
            &source_node_id,
            receiver_node_id.clone(),
            file_system.file_system_id.clone(),
            shard_id.clone(),
            shard.epoch,
            source_sequence,
            DfsMetadataJournalReplicationOptions {
                probe_caught_up_replicas: true,
                ..DfsMetadataJournalReplicationOptions::default()
            },
            &mut payloads_by_after_sequence,
        )
        .expect("prepare caught-up replica probe")
        {
            DfsPreparedMetadataJournalTargetPublishResult::Publish { target } => target,
            DfsPreparedMetadataJournalTargetPublishResult::NoChanges { outcome } => {
                panic!("caught-up probe unexpectedly skipped: {outcome:?}")
            }
        };
        assert!(target_publish.caught_up_probe);
        assert_eq!(target_publish.commands.len(), 1);
        assert!(target_publish.commands[0].batch.entries.is_empty());
        assert_eq!(
            target_publish.commands[0].batch.next_after_sequence,
            source_sequence
        );

        let mut warm_transport = LocalReplicaRecoveryTransport {
            receiver_data_dir: receiver_data_dir.clone(),
            source_node_id: source_node_id.clone(),
            target_node_id: receiver_node_id.clone(),
            journal_rejection_reason: None,
            snapshot_install_calls: 0,
        };
        let mut warm_outcomes = Vec::new();
        execute_prepared_dfs_metadata_journal_target_publish(
            &source_data_dir,
            &source_node_id,
            target_publish.clone(),
            &mut warm_transport,
            DfsMetadataJournalReplicationOptions::default(),
            &mut warm_outcomes,
        )
        .expect("healthy caught-up replica probe");
        assert_eq!(
            warm_outcomes,
            vec![DfsMetadataJournalReplicationOutcome::NoChanges {
                target_node_id: receiver_node_id.clone(),
                shard_id: shard_id.clone(),
                after_sequence: source_sequence,
            }]
        );
        assert!(warm_transport.journal_rejection_reason.is_none());
        assert_eq!(warm_transport.snapshot_install_calls, 0);
        drop(source_backend);

        let snapshot_path =
            crate::metadata_table_store::metadata_table_snapshot_path(&receiver_root);
        let mut corrupt_snapshot: crate::metadata_records::DfsMetadataTableSnapshot =
            crate::metadata_io::read_sobj(&snapshot_path).expect("receiver snapshot");
        let corrupt_file_system = corrupt_snapshot
            .tables
            .as_mut()
            .expect("snapshot tables")
            .file_systems
            .iter_mut()
            .find(|candidate| candidate.file_system_id == file_system.file_system_id)
            .expect("snapshot filesystem");
        corrupt_file_system.display_name.push_str("-corrupt");
        crate::metadata_io::write_sobj_atomic(&snapshot_path, &corrupt_snapshot)
            .expect("write semantically corrupt snapshot");
        let open_error = LocalSobjMetadataBackend::new(shard_id.clone(), &receiver_root)
            .expect_err("semantic checksum corruption must prevent ordinary open");
        assert!(
            open_error
                .to_string()
                .contains("semantic checksum mismatch")
        );

        let mut transport = LocalReplicaRecoveryTransport {
            receiver_data_dir: receiver_data_dir.clone(),
            source_node_id: source_node_id.clone(),
            target_node_id: receiver_node_id.clone(),
            journal_rejection_reason: None,
            snapshot_install_calls: 0,
        };
        let mut outcomes = Vec::new();
        execute_prepared_dfs_metadata_journal_target_publish(
            &source_data_dir,
            &source_node_id,
            target_publish,
            &mut transport,
            DfsMetadataJournalReplicationOptions {
                attempt_timeout: Duration::from_secs(5),
                target_timeout: Duration::from_secs(5),
                ..DfsMetadataJournalReplicationOptions::default()
            },
            &mut outcomes,
        )
        .expect("publisher snapshot repair");

        let rejection_reason = transport
            .journal_rejection_reason
            .expect("warm-cache semantic checksum rejection");
        assert!(rejection_reason.contains("semantic checksum mismatch"));
        assert!(metadata_journal_rejection_allows_snapshot_repair(
            &rejection_reason
        ));
        assert_eq!(transport.snapshot_install_calls, 1);
        assert!(matches!(
            outcomes.as_slice(),
            [DfsMetadataJournalReplicationOutcome::SnapshotInstalled {
                target_node_id,
                shard_id: installed_shard_id,
                next_after_sequence,
                archived_existing: true,
            }] if target_node_id == &receiver_node_id
                && installed_shard_id == &shard_id
                && *next_after_sequence == source_sequence
        ));

        let repaired = LocalSobjMetadataBackend::new(shard_id, &receiver_root)
            .expect("reopen repaired receiver");
        let repaired_file_system = repaired
            .get_file_system(&file_system.file_system_id)
            .expect("read repaired filesystem")
            .expect("repaired filesystem");
        assert_eq!(
            repaired_file_system.display_name,
            authoritative_display_name
        );
        assert!(receiver_root.join("replica-repair-archives").is_dir());
    }

    #[test]
    fn valid_stale_replica_recovers_from_exact_caught_up_probe() {
        let target = sinomo_testdir::TestDir::new("sinomo-dfs-stale-replica-recovery");
        let source_data_dir = SinomoDataDir::new(target.path().join("source"));
        let receiver_data_dir = SinomoDataDir::new(target.path().join("receiver"));
        source_data_dir
            .ensure_root_layout()
            .expect("source root layout");
        receiver_data_dir
            .ensure_root_layout()
            .expect("receiver root layout");
        let source_node_id =
            NodeId::from_legacy_named("node_stale_replica_source").expect("source node");
        let receiver_node_id =
            NodeId::from_legacy_named("node_stale_replica_receiver").expect("receiver node");
        let shard_id = DfsMetadataShardId::new("root");
        let mut file_system = DfsFileSystem::sifs(source_node_id.clone());

        let mut source_backend = LocalSobjMetadataBackend::new(
            shard_id.clone(),
            local_dfs_metadata_shard_path(&source_data_dir, &shard_id),
        )
        .expect("source backend");
        source_backend
            .put_file_system(&file_system)
            .expect("initial filesystem");
        let stale_sequence = source_backend.journal_last_sequence();
        let stale_snapshot = source_backend.export_table_snapshot();
        file_system.display_name = "authoritative-after-stale-snapshot".to_string();
        source_backend
            .put_file_system(&file_system)
            .expect("updated filesystem");
        let source_sequence = source_backend.journal_last_sequence();
        assert!(source_sequence > stale_sequence);

        let receiver_root = local_dfs_metadata_shard_path(&receiver_data_dir, &shard_id);
        let mut receiver_backend = LocalSobjMetadataBackend::new(shard_id.clone(), &receiver_root)
            .expect("receiver backend");
        receiver_backend
            .replace_with_replicated_snapshot(shard_id.clone(), stale_sequence, stale_snapshot)
            .expect("seed valid stale receiver");
        drop(receiver_backend);

        let shard = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: shard_id.0.clone(),
            primary_node_id: source_node_id.clone(),
            replica_node_ids: vec![receiver_node_id.clone()],
            epoch: 1,
            assignment: crate::DfsMetadataShardAssignment::Root,
            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        };
        configure_replica_shard(&receiver_data_dir, &receiver_node_id, &shard);
        let mut payloads_by_after_sequence = BTreeMap::new();
        let target_publish = match prepare_dfs_metadata_journal_target_publish(
            &mut source_backend,
            &source_node_id,
            receiver_node_id.clone(),
            file_system.file_system_id.clone(),
            shard_id.clone(),
            shard.epoch,
            source_sequence,
            DfsMetadataJournalReplicationOptions {
                probe_caught_up_replicas: true,
                ..DfsMetadataJournalReplicationOptions::default()
            },
            &mut payloads_by_after_sequence,
        )
        .expect("prepare exact caught-up probe")
        {
            DfsPreparedMetadataJournalTargetPublishResult::Publish { target } => target,
            DfsPreparedMetadataJournalTargetPublishResult::NoChanges { outcome } => {
                panic!("exact caught-up probe unexpectedly skipped: {outcome:?}")
            }
        };
        assert!(target_publish.caught_up_probe);
        assert!(target_publish.commands[0].verify_prefix_only);
        drop(source_backend);

        let mut transport = LocalReplicaRecoveryTransport {
            receiver_data_dir: receiver_data_dir.clone(),
            source_node_id: source_node_id.clone(),
            target_node_id: receiver_node_id.clone(),
            journal_rejection_reason: None,
            snapshot_install_calls: 0,
        };
        let mut outcomes = Vec::new();
        execute_prepared_dfs_metadata_journal_target_publish(
            &source_data_dir,
            &source_node_id,
            target_publish,
            &mut transport,
            DfsMetadataJournalReplicationOptions::default(),
            &mut outcomes,
        )
        .expect("repair valid stale replica");

        let rejection_reason = transport
            .journal_rejection_reason
            .expect("exact-prefix mismatch rejection");
        assert!(rejection_reason.contains("metadata journal prefix probe state mismatch"));
        assert_eq!(transport.snapshot_install_calls, 1);
        assert!(matches!(
            outcomes.as_slice(),
            [DfsMetadataJournalReplicationOutcome::SnapshotInstalled {
                target_node_id,
                shard_id: installed_shard_id,
                next_after_sequence,
                ..
            }] if target_node_id == &receiver_node_id
                && installed_shard_id == &shard_id
                && *next_after_sequence == source_sequence
        ));

        let repaired = LocalSobjMetadataBackend::new(shard_id, receiver_root)
            .expect("reopen repaired receiver");
        assert_eq!(
            repaired
                .get_file_system(&file_system.file_system_id)
                .expect("read repaired filesystem")
                .expect("repaired filesystem")
                .display_name,
            file_system.display_name
        );
    }

    #[test]
    fn replica_backend_io_open_failure_is_not_snapshot_repair_rejection() {
        let target = sinomo_testdir::TestDir::new("sinomo-dfs-replica-io-open-error");
        let data_dir = SinomoDataDir::new(target.path());
        data_dir.ensure_root_layout().expect("root layout");
        let source_node_id =
            NodeId::from_legacy_named("node_replica_io_source").expect("source node");
        let receiver_node_id =
            NodeId::from_legacy_named("node_replica_io_receiver").expect("receiver node");
        let shard_id = DfsMetadataShardId::new("root");
        let file_system = DfsFileSystem::sifs(source_node_id.clone());
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: shard_id.0.clone(),
            primary_node_id: source_node_id.clone(),
            replica_node_ids: vec![receiver_node_id.clone()],
            epoch: 1,
            assignment: crate::DfsMetadataShardAssignment::Root,
            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        };
        configure_replica_shard(&data_dir, &receiver_node_id, &shard);

        let shard_root = local_dfs_metadata_shard_path(&data_dir, &shard_id);
        std::fs::create_dir_all(shard_root.parent().expect("metadata parent"))
            .expect("metadata parent");
        std::fs::write(&shard_root, b"not a metadata directory")
            .expect("non-directory shard fixture");
        let command = dfs_metadata_journal_batch_command(
            &source_node_id,
            &receiver_node_id,
            &file_system.file_system_id,
            shard.epoch,
            DfsMetadataJournalBatch {
                shard_id,
                entries: Vec::new(),
                next_after_sequence: 0,
            },
        );
        let error = process_cluster_dfs_metadata_journal_batch(
            &data_dir,
            &ClusterPaths::new(&data_dir),
            &source_node_id,
            command,
        )
        .expect_err("non-corruption open error must propagate");
        assert!(matches!(error, DfsError::Metadata { .. }));
        assert!(
            !error
                .to_string()
                .contains(DFS_METADATA_UNREADABLE_REPLICA_SNAPSHOT_REPAIR_REASON)
        );

        let mut snapshot_source = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("root"),
            target.path().join("snapshot-source"),
        )
        .expect("snapshot source");
        snapshot_source
            .put_file_system(&file_system)
            .expect("snapshot filesystem");
        let snapshot_sequence = snapshot_source.journal_last_sequence();
        let snapshot = snapshot_source.export_table_snapshot();
        let snapshot_error = process_cluster_dfs_metadata_snapshot_install(
            &data_dir,
            &ClusterPaths::new(&data_dir),
            &source_node_id,
            DfsMetadataSnapshotInstallCommand {
                request_id: "replica-io-snapshot".to_string(),
                source_node_id: source_node_id.clone(),
                file_system_id: Some(file_system.file_system_id),
                authority_epoch: Some(shard.epoch),
                primary_handoff_id: None,
                shard_id: DfsMetadataShardId::new("root"),
                next_after_sequence: snapshot_sequence,
                snapshot,
                reason: "IO errors must not archive replica state".to_string(),
            },
        )
        .expect_err("snapshot install must propagate non-corruption open error");
        assert!(matches!(snapshot_error, DfsError::Metadata { .. }));
        assert!(!shard_root.join("replica-repair-archives").exists());
    }

    #[test]
    fn unauthorized_cluster_replica_ingress_does_not_create_local_layout() {
        let target = sinomo_testdir::TestDir::new("sinomo-dfs-replica-ingress-no-layout");
        let data_dir = SinomoDataDir::new(target.path());
        let cluster_paths = ClusterPaths::new(&data_dir);
        let source_node_id =
            NodeId::from_legacy_named("replica-ingress-source").expect("source node id");
        let shard_id = DfsMetadataShardId::new("unconfigured");
        let file_system_id = sifs_file_system_id();
        assert!(!data_dir.dfs_dir().exists());

        let journal_response = process_cluster_dfs_metadata_journal_batch(
            &data_dir,
            &cluster_paths,
            &source_node_id,
            DfsMetadataJournalBatchCommand {
                request_id: "unauthorized-journal".to_string(),
                source_node_id: Some(source_node_id.clone()),
                file_system_id: Some(file_system_id.clone()),
                authority_epoch: Some(1),
                verify_prefix_only: false,
                prefix_probe: None,
                authority_probe: None,
                batch: DfsMetadataJournalBatch {
                    shard_id: shard_id.clone(),
                    entries: Vec::new(),
                    next_after_sequence: 1,
                },
            },
        )
        .expect("journal rejection response");
        assert!(matches!(
            journal_response.result,
            DfsMetadataJournalBatchResult::Rejected { .. }
        ));
        assert!(!data_dir.dfs_dir().exists());

        let snapshot_source =
            sinomo_testdir::TestDir::new("sinomo-dfs-replica-ingress-snapshot-source");
        let snapshot = LocalSobjMetadataBackend::new(shard_id.clone(), snapshot_source.path())
            .expect("snapshot source backend")
            .export_table_snapshot();
        let snapshot_response = process_cluster_dfs_metadata_snapshot_install(
            &data_dir,
            &cluster_paths,
            &source_node_id,
            DfsMetadataSnapshotInstallCommand {
                request_id: "unauthorized-snapshot".to_string(),
                source_node_id: source_node_id.clone(),
                file_system_id: Some(file_system_id),
                authority_epoch: Some(1),
                primary_handoff_id: None,
                shard_id,
                next_after_sequence: 1,
                snapshot,
                reason: "test unauthorized ingress".to_string(),
            },
        )
        .expect("snapshot rejection response");
        assert!(matches!(
            snapshot_response.result,
            DfsMetadataSnapshotInstallResult::Rejected { .. }
        ));
        assert!(!data_dir.dfs_dir().exists());
    }

    #[test]
    fn async_cursor_commit_does_not_open_materialized_tables() {
        let temp = sinomo_testdir::TestDir::new("sinomo-dfs-replication-cursor-direct");
        let data_dir = SinomoDataDir::new(temp.path());
        let shard_id = DfsMetadataShardId::new("services");
        let root = local_dfs_metadata_shard_path(&data_dir, &shard_id);
        let table_dir = root.join("records").join("tables");
        std::fs::create_dir_all(&table_dir).expect("table dir");
        std::fs::write(table_dir.join("materialized.slog"), b"not-a-table-log")
            .expect("corrupt table fixture");
        let target_node_id = NodeId::from_legacy_named("cursor-target").expect("node id");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        runtime
            .block_on(put_local_dfs_metadata_replication_cursor_async(
                data_dir.clone(),
                target_node_id.clone(),
                shard_id.clone(),
                42,
            ))
            .expect("cursor commit");
        runtime
            .block_on(put_local_dfs_metadata_replication_cursor_async(
                data_dir,
                target_node_id.clone(),
                shard_id.clone(),
                12,
            ))
            .expect("monotonic cursor commit");

        assert_eq!(
            metadata_cursor::metadata_replication_cursor(&root, &target_node_id, &shard_id)
                .expect("cursor read"),
            42
        );
    }

    #[test]
    fn exact_primary_handoff_snapshot_repairs_frozen_promoted_replica() {
        let target = sinomo_testdir::TestDir::new("sinomo-dfs-primary-handoff-snapshot-repair");
        let shard_id = DfsMetadataShardId::new("root");
        let source_node_id = NodeId::from_legacy_named("handoff-snapshot-source").unwrap();
        let promoted_node_id = NodeId::from_legacy_named("handoff-snapshot-promoted").unwrap();
        let other_node_id = NodeId::from_legacy_named("handoff-snapshot-other").unwrap();
        let mut file_system = DfsFileSystem::sifs(source_node_id.clone());
        file_system.display_name = "primary handoff snapshot repair".to_string();

        let mut source =
            LocalSobjMetadataBackend::new(shard_id.clone(), target.path().join("source"))
                .expect("source backend");
        source
            .put_file_system(&file_system)
            .expect("source filesystem");
        let sequence = source.journal_last_sequence();
        let snapshot = source.export_table_snapshot();
        let state_fingerprint =
            crate::service_sync::metadata_shard_state_fingerprint_at(&source, &shard_id, sequence)
                .expect("source checkpoint");

        let mut receiver =
            LocalSobjMetadataBackend::new(shard_id.clone(), target.path().join("receiver"))
                .expect("receiver backend");
        receiver
            .replace_with_replicated_snapshot(shard_id.clone(), sequence, snapshot.clone())
            .expect("seed promoted replica");

        let retiring = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: shard_id.0.clone(),
            primary_node_id: source_node_id.clone(),
            replica_node_ids: vec![promoted_node_id.clone(), other_node_id.clone()],
            epoch: 7,
            assignment: crate::DfsMetadataShardAssignment::Root,
            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        };
        let desired = DfsMetadataShardConfig {
            file_system_id: retiring.file_system_id.clone(),
            shard_id: retiring.shard_id.clone(),
            primary_node_id: promoted_node_id.clone(),
            replica_node_ids: vec![other_node_id, source_node_id.clone()],
            epoch: 8,
            assignment: retiring.assignment.clone(),
            default_policy: retiring.default_policy.clone(),
        };
        let handoff_id = "handoff-snapshot-repair".to_string();
        receiver
            .freeze_metadata_shard_for_membership_handoff(&shard_id, &handoff_id, retiring.epoch)
            .expect("freeze promoted replica")
            .expect("local shard");
        let context = DfsMetadataReplicaShardContext {
            local_node_id: promoted_node_id,
            shard: retiring.clone(),
            pending_membership_handoff: Some(DfsMetadataShardMembershipHandoff {
                handoff_id: handoff_id.clone(),
                retiring_shard: retiring.clone(),
                desired_shard: desired,
                required_prepared_sequence: 0,
                required_head_watermark: None,
                retiring_primary_fence: Some(crate::DfsMetadataShardMembershipHandoffFence {
                    handoff_id: handoff_id.clone(),
                    primary_node_id: source_node_id.clone(),
                    journal_sequence: sequence,
                    state_fingerprint,
                }),
            }),
        };
        let command = DfsMetadataSnapshotInstallCommand {
            request_id: "primary-handoff-snapshot-repair".to_string(),
            source_node_id,
            file_system_id: Some(retiring.file_system_id),
            authority_epoch: Some(retiring.epoch),
            primary_handoff_id: Some(handoff_id),
            shard_id: shard_id.clone(),
            next_after_sequence: sequence,
            snapshot,
            reason: "repair promoted replica to the exact frozen checkpoint".to_string(),
        };

        let mut ordinary = command.clone();
        ordinary.primary_handoff_id = None;
        let mut outbound = ordinary.clone();
        let mut sender_context = context.clone();
        sender_context.local_node_id = outbound.source_node_id.clone();
        bind_pending_primary_handoff_snapshot_for_target(
            &sender_context,
            &context.local_node_id,
            &mut outbound,
        );
        assert_eq!(
            outbound.primary_handoff_id.as_deref(),
            command.primary_handoff_id.as_deref(),
            "the direct publisher must bind the exact promoted-target handoff"
        );
        assert!(matches!(
            process_dfs_metadata_snapshot_install(&mut receiver, Some(&context), ordinary).result,
            DfsMetadataSnapshotInstallResult::Rejected { ref reason }
                if reason.contains("frozen for pending membership handoff")
        ));
        assert!(matches!(
            process_dfs_metadata_snapshot_install(&mut receiver, Some(&context), command).result,
            DfsMetadataSnapshotInstallResult::Installed { .. }
        ));
        assert!(
            receiver
                .metadata_shard_authoritative_writes_frozen(&shard_id)
                .expect("promoted replica remains fenced until activation")
        );
    }

    #[test]
    fn exact_primary_handoff_snapshot_preserves_authoritative_gc_after_promotion() {
        let target = sinomo_testdir::TestDir::new("sinomo-dfs-primary-handoff-snapshot-gc");
        let shard_id = DfsMetadataShardId::new("root");
        let source_node_id = NodeId::from_legacy_named("handoff-gc-source").unwrap();
        let promoted_node_id = NodeId::from_legacy_named("handoff-gc-promoted").unwrap();
        let other_node_id = NodeId::from_legacy_named("handoff-gc-other").unwrap();
        let mut file_system = DfsFileSystem::sifs(source_node_id.clone());
        file_system.display_name = "primary handoff snapshot GC".to_string();
        let object_id =
            crate::DfsObjectId::from_legacy_named("object_handoff_gc").expect("object id");
        let old_version =
            DfsVersion::from_legacy_named("version_handoff_gc_old").expect("old version");
        let live_version =
            DfsVersion::from_legacy_named("version_handoff_gc_live").expect("live version");
        let live = crate::DfsObjectMetadata {
            object_id: object_id.clone(),
            file_system_id: file_system.file_system_id.clone(),
            path: "/handoff-gc.txt".to_string(),
            kind: DfsObjectKind::File,
            tombstone_provenance: None,
            owner_node_id: source_node_id.clone(),
            current_locations: vec![source_node_id.clone(), promoted_node_id.clone()],
            content_hash: None,
            content_size_bytes: None,
            link_target_object_id: None,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 1,
            modified_at_unix_nanos: 2,
            created_at_unix_nanos: 1,
            changed_at_unix_nanos: 2,
            version: live_version.clone(),
            policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        };
        let old_version_record = DfsObjectVersionRecord {
            file_system_id: file_system.file_system_id.clone(),
            object_id: object_id.clone(),
            version: old_version.clone(),
            content_hash: None,
            size_bytes: None,
            locations: vec![source_node_id.clone()],
            created_by_event_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            visible: true,
        };
        let live_version_record = DfsObjectVersionRecord {
            file_system_id: file_system.file_system_id.clone(),
            object_id: object_id.clone(),
            version: live_version.clone(),
            content_hash: None,
            size_bytes: None,
            locations: live.current_locations.clone(),
            created_by_event_id: None,
            created_at: OffsetDateTime::from_unix_timestamp(1).expect("live version timestamp"),
            visible: true,
        };
        let terminal_intent = crate::DfsWriteIntentRecord {
            intent_id: "dfs_intent_handoff_gc_1".to_string(),
            file_system_id: file_system.file_system_id.clone(),
            object_id: object_id.clone(),
            base_version: Some(old_version.clone()),
            target_version: live_version.clone(),
            coordinator_node_id: source_node_id.clone(),
            owner_epoch: 7,
            desired_replica_node_ids: vec![source_node_id.clone(), promoted_node_id.clone()],
            policy: crate::DfsReplicationPolicy::confirmed(2).expect("replication policy"),
            status: crate::DfsWriteIntentStatus::Aborted,
            content_hash: None,
            object_metadata: Some(live.clone()),
            touch_parent_on_create: false,
            create_predecessor_object_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: Some(OffsetDateTime::UNIX_EPOCH),
        };

        let source_root = target.path().join("source");
        let mut source =
            LocalSobjMetadataBackend::new(shard_id.clone(), &source_root).expect("source backend");
        source
            .put_file_system(&file_system)
            .expect("source filesystem");
        source
            .put_version(&old_version_record)
            .expect("old source version");
        source
            .put_version(&live_version_record)
            .expect("live source version");
        source.put_object(&live).expect("live source object");
        source
            .put_intent(&terminal_intent)
            .expect("terminal source intent");

        let pre_gc_sequence = source.journal_last_sequence();
        let pre_gc_snapshot = source.export_table_snapshot();
        let receiver_root = target.path().join("receiver");
        let mut receiver = LocalSobjMetadataBackend::new(shard_id.clone(), &receiver_root)
            .expect("receiver backend");
        receiver
            .replace_with_replicated_snapshot(shard_id.clone(), pre_gc_sequence, pre_gc_snapshot)
            .expect("seed promoted replica with pre-GC history");
        assert!(
            receiver
                .get_version(&object_id, &old_version)
                .expect("seeded old version")
                .is_some()
        );
        assert!(
            receiver
                .export_table_snapshot()
                .tables
                .expect("seeded snapshot tables")
                .intents
                .iter()
                .any(|intent| intent.object_id == object_id
                    && intent.intent_id == terminal_intent.intent_id)
        );

        let gc = source
            .gc_metadata_history_authoritative(crate::DfsMetadataHistoryGcOptions::delete_eligible(
                Duration::ZERO,
                1,
            ))
            .expect("durable authoritative metadata GC");
        assert_eq!(gc.removed_version_count, 1);
        assert_eq!(gc.compacted_intent_count, 1);
        drop(source);

        let source =
            LocalSobjMetadataBackend::new(shard_id.clone(), &source_root).expect("reopen source");
        assert_eq!(
            source
                .get_version(&object_id, &old_version)
                .expect("reopened old version"),
            None,
            "durable GC must remove the old version before handoff"
        );
        assert!(
            source
                .export_table_snapshot()
                .tables
                .expect("reopened source tables")
                .intents
                .iter()
                .all(|intent| intent.object_id != object_id
                    || intent.intent_id != terminal_intent.intent_id),
            "durable GC must replace the full terminal body"
        );
        let expected_receipt = source
            .get_intent_receipt(&object_id, &terminal_intent.intent_id)
            .expect("reopened intent receipt")
            .expect("compact terminal receipt");
        assert_eq!(
            source.get_object(&object_id).expect("reopened live object"),
            Some(live.clone())
        );
        assert_eq!(
            source
                .get_version(&object_id, &live_version)
                .expect("reopened live version"),
            Some(live_version_record.clone())
        );

        let sequence = source.journal_last_sequence();
        let snapshot = source.export_table_snapshot();
        let state_fingerprint =
            crate::service_sync::metadata_shard_state_fingerprint_at(&source, &shard_id, sequence)
                .expect("source post-GC checkpoint");
        let retiring = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: shard_id.0.clone(),
            primary_node_id: source_node_id.clone(),
            replica_node_ids: vec![promoted_node_id.clone(), other_node_id.clone()],
            epoch: 7,
            assignment: crate::DfsMetadataShardAssignment::Root,
            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        };
        let desired = DfsMetadataShardConfig {
            file_system_id: retiring.file_system_id.clone(),
            shard_id: retiring.shard_id.clone(),
            primary_node_id: promoted_node_id.clone(),
            replica_node_ids: vec![other_node_id, source_node_id.clone()],
            epoch: 8,
            assignment: retiring.assignment.clone(),
            default_policy: retiring.default_policy.clone(),
        };
        let handoff_id = "handoff-snapshot-gc".to_string();
        receiver
            .freeze_metadata_shard_for_membership_handoff(&shard_id, &handoff_id, retiring.epoch)
            .expect("freeze promoted replica")
            .expect("local shard");
        let context = DfsMetadataReplicaShardContext {
            local_node_id: promoted_node_id,
            shard: retiring.clone(),
            pending_membership_handoff: Some(DfsMetadataShardMembershipHandoff {
                handoff_id: handoff_id.clone(),
                retiring_shard: retiring.clone(),
                desired_shard: desired.clone(),
                required_prepared_sequence: 0,
                required_head_watermark: None,
                retiring_primary_fence: Some(crate::DfsMetadataShardMembershipHandoffFence {
                    handoff_id: handoff_id.clone(),
                    primary_node_id: source_node_id.clone(),
                    journal_sequence: sequence,
                    state_fingerprint,
                }),
            }),
        };
        let command = DfsMetadataSnapshotInstallCommand {
            request_id: "primary-handoff-snapshot-gc".to_string(),
            source_node_id,
            file_system_id: Some(retiring.file_system_id),
            authority_epoch: Some(retiring.epoch),
            primary_handoff_id: Some(handoff_id.clone()),
            shard_id: shard_id.clone(),
            next_after_sequence: sequence,
            snapshot,
            reason: "install exact post-GC state before promoting replica".to_string(),
        };

        assert!(matches!(
            process_dfs_metadata_snapshot_install(&mut receiver, Some(&context), command).result,
            DfsMetadataSnapshotInstallResult::Installed { .. }
        ));
        assert!(
            receiver
                .metadata_shard_authoritative_writes_frozen(&shard_id)
                .expect("receiver remains frozen before activation")
        );
        assert_eq!(
            receiver
                .get_version(&object_id, &old_version)
                .expect("post-install old version"),
            None,
            "the exact post-GC snapshot must not resurrect pruned history"
        );
        assert!(
            receiver
                .export_table_snapshot()
                .tables
                .expect("post-install receiver tables")
                .intents
                .iter()
                .all(|intent| intent.object_id != object_id
                    || intent.intent_id != terminal_intent.intent_id)
        );
        assert_eq!(
            receiver
                .get_intent_receipt(&object_id, &terminal_intent.intent_id)
                .expect("post-install terminal receipt"),
            Some(expected_receipt.clone())
        );
        assert_eq!(
            receiver
                .get_object(&object_id)
                .expect("post-install live object"),
            Some(live.clone())
        );
        assert_eq!(
            receiver
                .get_version(&object_id, &live_version)
                .expect("post-install live version"),
            Some(live_version_record.clone())
        );

        assert!(
            receiver
                .clear_metadata_shard_membership_handoff_freeze(
                    &shard_id,
                    &handoff_id,
                    desired.epoch,
                )
                .expect("activate promoted metadata authority")
        );
        assert!(
            !receiver
                .metadata_shard_authoritative_writes_frozen(&shard_id)
                .expect("promoted metadata authority is writable")
        );
        receiver
            .put_intent(&terminal_intent)
            .expect("exact retry uses promoted compact receipt");
        assert!(
            receiver
                .export_table_snapshot()
                .tables
                .expect("promoted receiver tables")
                .intents
                .iter()
                .all(|intent| intent.object_id != object_id
                    || intent.intent_id != terminal_intent.intent_id),
            "an exact retry must not expand the compact terminal body"
        );
        assert_eq!(
            receiver
                .get_intent_receipt(&object_id, &terminal_intent.intent_id)
                .expect("promoted terminal receipt"),
            Some(expected_receipt)
        );
        assert_eq!(
            receiver
                .get_version(&object_id, &old_version)
                .expect("promoted old version"),
            None,
            "promotion and exact retry must not resurrect pruned history"
        );
        assert_eq!(
            receiver
                .get_object(&object_id)
                .expect("promoted live object"),
            Some(live)
        );
        assert_eq!(
            receiver
                .get_version(&object_id, &live_version)
                .expect("promoted live version"),
            Some(live_version_record)
        );
    }
}
