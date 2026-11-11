//! Sharded DFS metadata backend.
//!
//! This module owns routing across shard-local metadata backends: shard
//! selection, object-shard indexes, cross-shard moves, and merged scans.
//! Durable table and journal storage stays in `metadata_backend`.

// Negative scheduling seam only; no return value, DATA or authority is supplied.
#[cfg(test)]
thread_local! {
    pub(crate) static GENESIS_PREPARATION_OBSERVER: std::cell::RefCell<Option<Box<dyn Fn(u8)>>> =
        std::cell::RefCell::new(None);
}

use std::collections::{BTreeMap, BTreeSet};
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, OnceLock, RwLock, TryLockError};

use serde::{Deserialize, Serialize};
use sinomo_core::node::NodeId;

use crate::advisory_lock::DfsAdvisoryLockStateRecord;
use crate::content_root_catalog::{
    DfsContentRootCatalogIncompleteReason, DfsContentRootCatalogPage,
    DfsContentRootCatalogPageRequest, DfsContentRootCatalogShardIdentity,
};
use crate::control_plane::{
    DfsMetadataShardAssignment, DfsMetadataShardConfig, DfsMetadataShardRoutingIndex,
};
use crate::intent::write_intent_status_is_active;
use crate::metadata_api::{
    DfsMetadataBackend, DfsMetadataReplicaApplyReport, DfsMetadataReplicaIngestPermit,
    DfsMetadataShardWriteFreeze, DfsNamespaceRequestAdmissionSnapshot,
};
use crate::metadata_backend::{
    LocalSobjMetadataBackend, ResidentMetadataReplicationCursorProjectionObserver,
};
use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_hardlink::sort_hardlinks_by_path;
use crate::metadata_history_gc::{
    DfsMetadataContentProtection, DfsMetadataHistoryGcOptions, DfsMetadataHistoryGcReport,
};
use crate::metadata_io::{create_dir_all, read_sobj_optional, write_sobj_atomic};
use crate::metadata_keys::{
    directory_page_cursor_with_shard, key_segment, local_directory_page_cursor,
    local_object_page_cursor, local_object_version_page_cursor, local_replica_receipt_page_cursor,
    local_version_pin_page_cursor, local_write_intent_page_cursor, normalize_path,
    normalize_posix_path, object_page_cursor_with_shard, object_page_key,
    object_version_scan_cursor, object_version_scan_key, replica_receipt_scan_cursor,
    replica_receipt_scan_key, write_intent_page_cursor, write_intent_page_cursor_with_shard,
    write_intent_scan_cursor, write_intent_scan_order,
};
use crate::metadata_limits::OBJECT_LOCAL_METADATA_PAGE_SIZE;
use crate::metadata_lock::MetadataWriteLock;
use crate::metadata_pages::{
    collect_metadata_pages, finish_cursor_page, finish_sorted_cursor_page, walk_metadata_pages,
};
use crate::metadata_records::*;
use crate::metadata_shard_index::{
    DfsObjectShardIndexRecord, read_object_shard_index, read_object_shard_index_for_rebuild,
    remove_object_shard_index, write_new_object_shard_indexes, write_object_shard_index,
};
use crate::metadata_shard_migration::DfsObjectShardMigrationBundle;
use crate::metadata_shard_pages::{
    collect_resumed_sharded_page_entries, collect_sharded_scan_entries,
    object_version_page_with_shard_cursor, replica_receipt_page_with_shard_cursor,
    write_intent_page_with_shard_cursor,
};
use crate::metadata_summary::{
    accumulate_metadata_summary_detail_counts, metadata_summary_for_file_system_from_tables,
    metadata_summary_from_tables,
};
use crate::metadata_tombstone::{metadata_now_unix_nanos, plan_tombstone_compaction};
use crate::posix_path::posix_path_is_at_or_under_prefix;
use crate::{
    DfsFileSystem, DfsFileSystemId, DfsObjectId, DfsObjectKind, DfsObjectMetadata,
    DfsOperatorStatusOmission, DfsVersion,
};

fn is_source_eviction_tombstone(object: &DfsObjectMetadata) -> bool {
    object.kind == DfsObjectKind::Tombstone
        && matches!(
            object.tombstone_provenance.as_ref(),
            Some(crate::DfsTombstoneProvenance::SourceEviction { .. })
        )
}

const ROUTING_TOPOLOGY_HISTORY_LIMIT: usize = 4;

#[cfg(test)]
thread_local! {
    static SHARDED_METADATA_STATUS_QUERY_COUNT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static SHARDED_METADATA_OBJECT_ID_LOOKUP_COUNT: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static SHARDED_CONSISTENT_READ_SNAPSHOT_SHARDS: std::cell::RefCell<Vec<DfsMetadataShardId>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

#[cfg(test)]
pub(crate) fn take_sharded_metadata_status_query_count() -> usize {
    SHARDED_METADATA_STATUS_QUERY_COUNT.replace(0)
}

#[cfg(test)]
pub(crate) fn reset_sharded_consistent_read_snapshot_shards() {
    SHARDED_CONSISTENT_READ_SNAPSHOT_SHARDS.with(|shards| shards.borrow_mut().clear());
}

#[cfg(test)]
pub(crate) fn sharded_consistent_read_snapshot_shards() -> Vec<DfsMetadataShardId> {
    SHARDED_CONSISTENT_READ_SNAPSHOT_SHARDS.with(|shards| shards.borrow().clone())
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
struct DfsMetadataRoutingTopologyHistory {
    #[serde(default)]
    active: Vec<DfsMetadataShardConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    fallbacks: Vec<Vec<DfsMetadataShardConfig>>,
}

pub(crate) fn routing_topology_history_path(root: &std::path::Path) -> PathBuf {
    root.join("shard-index").join("routing-topologies.sobj")
}

fn canonicalize_routing_topology(shards: &mut [DfsMetadataShardConfig]) {
    shards.sort_by(|left, right| {
        left.file_system_id
            .cmp(&right.file_system_id)
            .then_with(|| left.shard_id.cmp(&right.shard_id))
    });
}

/// Fields that can change which shard receives a path or object lookup.
/// Authority, replication, codec, and policy changes do not create old routes.
#[derive(Clone, Debug, Eq, PartialEq)]
struct DfsMetadataRoutingShardIdentity {
    file_system_id: DfsFileSystemId,
    shard_id: String,
    assignment: DfsMetadataShardAssignment,
}

fn routing_topology_identity(
    topology: &[DfsMetadataShardConfig],
) -> Vec<DfsMetadataRoutingShardIdentity> {
    let mut identity = topology
        .iter()
        .map(|shard| DfsMetadataRoutingShardIdentity {
            file_system_id: shard.file_system_id.clone(),
            shard_id: shard.shard_id.clone(),
            assignment: shard.assignment.clone(),
        })
        .collect::<Vec<_>>();
    identity.sort_by(|left, right| {
        left.file_system_id
            .cmp(&right.file_system_id)
            .then_with(|| left.shard_id.cmp(&right.shard_id))
    });
    identity
}

fn compile_routing_indexes(
    shards: impl IntoIterator<Item = DfsMetadataShardConfig>,
) -> Result<BTreeMap<DfsFileSystemId, DfsMetadataShardRoutingIndex>> {
    let mut configs_by_file_system =
        BTreeMap::<DfsFileSystemId, Vec<DfsMetadataShardConfig>>::new();
    for shard in shards {
        configs_by_file_system
            .entry(shard.file_system_id.clone())
            .or_default()
            .push(shard);
    }
    configs_by_file_system
        .into_iter()
        .map(|(file_system_id, shards)| {
            DfsMetadataShardRoutingIndex::new(file_system_id.clone(), &shards)
                .map(|index| (file_system_id, index))
                .map_err(|error| MetadataBackendError::Serde(error.to_string()))
        })
        .collect()
}

/// Delete this node's local copies of `shard_ids` under the sharded metadata
/// root, together with the object-shard index rows routing into them. Each
/// shard root stays in place and its slot reopens empty; a node later added
/// back as a replica receives the shard again from its primary.
pub fn discard_local_metadata_shard_copies(
    root: &Path,
    shard_ids: &BTreeSet<DfsMetadataShardId>,
) -> Result<()> {
    for shard_id in shard_ids {
        let shard_root = root.join(key_segment(&shard_id.0));
        if shard_root.is_dir() {
            crate::metadata_table_store::discard_metadata_backend_state(&shard_root)?;
        }
    }
    if shard_ids.is_empty() {
        return Ok(());
    }
    crate::metadata_shard_index::remove_object_shard_index_rows_for_shards(root, shard_ids)
}

fn load_routing_topology_history(
    root: &std::path::Path,
    active: &[DfsMetadataShardConfig],
    persist: bool,
) -> Result<DfsMetadataRoutingTopologyHistory> {
    let path = routing_topology_history_path(root);
    let persisted = read_sobj_optional::<DfsMetadataRoutingTopologyHistory>(&path)?;
    let mut history = persisted.clone().unwrap_or_default();
    if history.active.is_empty() {
        history.active = active.to_vec();
    } else if history.active != active {
        let previous = std::mem::replace(&mut history.active, active.to_vec());
        if routing_topology_identity(&previous) != routing_topology_identity(active) {
            history.fallbacks.insert(0, previous);
        }
    }
    let active_identity = routing_topology_identity(&history.active);
    let mut fallback_identities = Vec::new();
    history.fallbacks.retain(|topology| {
        if topology.is_empty() {
            return false;
        }
        let identity = routing_topology_identity(topology);
        if identity == active_identity || fallback_identities.contains(&identity) {
            return false;
        }
        fallback_identities.push(identity);
        true
    });
    history.fallbacks.truncate(ROUTING_TOPOLOGY_HISTORY_LIMIT);
    if persist && persisted.as_ref() != Some(&history) {
        write_sobj_atomic(&path, &history)?;
    }
    Ok(history)
}

#[cfg(test)]
mod routing_topology_history_tests {
    use super::*;
    use sinomo_core::DfsObjectPolicy;

    fn node(value: &str) -> NodeId {
        NodeId::from_legacy_named(value).expect("valid node id")
    }

    fn root_config() -> DfsMetadataShardConfig {
        DfsMetadataShardConfig {
            file_system_id: crate::sifs_file_system_id(),
            shard_id: "root".to_string(),
            primary_node_id: node("routing-history-primary"),
            replica_node_ids: vec![node("routing-history-replica")],
            epoch: 1,
            assignment: DfsMetadataShardAssignment::Root,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        }
    }

    fn split_configs() -> Vec<DfsMetadataShardConfig> {
        let root = root_config();
        vec![
            root,
            DfsMetadataShardConfig {
                file_system_id: crate::sifs_file_system_id(),
                shard_id: "services".to_string(),
                primary_node_id: node("routing-history-services-primary"),
                replica_node_ids: vec![node("routing-history-services-replica")],
                epoch: 1,
                assignment: DfsMetadataShardAssignment::PathPrefix {
                    prefix: crate::SIFS_SERVICES_PATH.to_string(),
                },
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            },
        ]
    }

    fn change_non_routing_fields(
        topology: &[DfsMetadataShardConfig],
    ) -> Vec<DfsMetadataShardConfig> {
        topology
            .iter()
            .enumerate()
            .map(|(index, shard)| {
                let mut shard = shard.clone();
                shard.primary_node_id = node(&format!("routing-history-new-primary-{index}"));
                shard.replica_node_ids = vec![
                    node(&format!("routing-history-new-replica-{index}")),
                    node(&format!("routing-history-new-replica-{index}-b")),
                ];
                shard.epoch += 10;
                shard.default_policy = DfsObjectPolicy::AppendOnly;
                shard
            })
            .collect()
    }

    fn metadata_root(name: &str) -> (sinomo_testdir::TestDir, PathBuf) {
        let root = sinomo_testdir::TestDir::new(name);
        let metadata_root = root.path().join("metadata");
        create_dir_all(metadata_root.join("shard-index")).expect("create shard index root");
        (root, metadata_root)
    }

    #[test]
    fn routing_topology_history_does_not_fallback_for_non_routing_changes() {
        let (_root, metadata_root) = metadata_root("dfs-routing-history-non-routing-change");
        let initial = vec![root_config()];
        let initial_history = load_routing_topology_history(&metadata_root, &initial, true)
            .expect("persist initial routing history");
        assert!(initial_history.fallbacks.is_empty());

        let changed = change_non_routing_fields(&initial);
        let history = load_routing_topology_history(&metadata_root, &changed, true)
            .expect("refresh full active shard configs");

        assert_eq!(history.active, changed);
        assert!(
            history.fallbacks.is_empty(),
            "authority, replica, epoch, codec, and policy changes must not create routing fallbacks"
        );
        assert_eq!(
            read_sobj_optional::<DfsMetadataRoutingTopologyHistory>(
                &routing_topology_history_path(&metadata_root),
            )
            .expect("read refreshed routing history"),
            Some(history),
            "the complete active configs must still be refreshed on disk"
        );
    }

    #[test]
    fn routing_topology_history_compacts_existing_semantic_duplicates() {
        let (_root, metadata_root) = metadata_root("dfs-routing-history-semantic-dedup");
        let active = split_configs();
        let active_duplicate = change_non_routing_fields(&active);
        let root_fallback = vec![root_config()];
        let root_duplicate = change_non_routing_fields(&root_fallback);
        let persisted = DfsMetadataRoutingTopologyHistory {
            active: active.clone(),
            fallbacks: vec![active_duplicate, root_fallback.clone(), root_duplicate],
        };
        let path = routing_topology_history_path(&metadata_root);
        write_sobj_atomic(&path, &persisted).expect("persist duplicate routing history");

        let history = load_routing_topology_history(&metadata_root, &active, true)
            .expect("compact routing history");

        assert_eq!(history.fallbacks, vec![root_fallback]);
        assert_eq!(
            read_sobj_optional::<DfsMetadataRoutingTopologyHistory>(&path)
                .expect("read compacted routing history"),
            Some(history),
            "semantic compaction must be persisted"
        );
    }

    #[test]
    fn routing_topology_history_retains_genuine_root_to_split_change() {
        let (_root, metadata_root) = metadata_root("dfs-routing-history-root-to-split");
        let root_topology = vec![root_config()];
        load_routing_topology_history(&metadata_root, &root_topology, true)
            .expect("persist root routing topology");

        let split = split_configs();
        let history = load_routing_topology_history(&metadata_root, &split, true)
            .expect("persist split routing topology");

        assert_eq!(history.active, split);
        assert_eq!(
            history.fallbacks,
            vec![root_topology],
            "a genuine shard or assignment change must remain routable until migration retirement"
        );
    }
}

/// Local sharded metadata router.
///
/// This backend multiplexes multiple `LocalSobjMetadataBackend` shards using
/// control-plane shard configs and a local object-to-shard index. It owns
/// cross-shard moves, shard-index repair, and merged cursor scans; shard-local
/// durable table and journal storage remain in `LocalSobjMetadataBackend`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LocalMetadataShardOpenMode {
    Strict,
    ReplicaRepair,
    Inspection,
}

#[derive(Debug)]
struct LocalMetadataShardSlot {
    config: DfsMetadataShardConfig,
    root: PathBuf,
    open_mode: LocalMetadataShardOpenMode,
    selected_original_data_layout: Option<Arc<crate::DfsSelectedOriginalDataLayout>>,
    initialization: Mutex<()>,
    backend: OnceLock<Box<LocalSobjMetadataBackend>>,
}

/// One resident shard-local backend slot shared by scoped mutation sessions.
///
/// The slot owns exactly one [`LocalSobjMetadataBackend`] instance. Converting
/// a [`LocalShardedMetadataBackend`] moves any already-open instance here; a
/// deferred replica slot is still opened lazily, once, on its first lock.
/// Keeping the backend itself behind the shard lock is important: membership
/// handoff authority is deliberately bound to that exact in-memory instance.
#[derive(Debug)]
struct ResidentLocalMetadataShardSlot {
    config: DfsMetadataShardConfig,
    root: PathBuf,
    open_mode: LocalMetadataShardOpenMode,
    selected_original_data_layout: Option<Arc<crate::DfsSelectedOriginalDataLayout>>,
    state: Mutex<ResidentLocalMetadataShardState>,
    available: Condvar,
    async_available: tokio::sync::Notify,
    /// Last complete shard-local operator projection. It stays readable while
    /// the backend itself is checked out by a long mutation, so status never
    /// queues behind data-plane ownership. Its topology is mutable because a
    /// global membership-handoff lease can rebind the resident router while
    /// retaining this exact shard backend instance.
    operator_status: RwLock<ResidentLocalMetadataShardOperatorStatus>,
}

#[derive(Debug)]
enum ResidentLocalMetadataShardState {
    Unopened,
    Opening,
    Available(Box<LocalSobjMetadataBackend>),
    CheckedOut,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ResidentLocalMetadataShardStatusSnapshot {
    summary: DfsMetadataSummary,
    shard_status: DfsMetadataShardStatus,
    incomplete_reasons: Vec<String>,
}

#[derive(Debug)]
struct ResidentLocalMetadataShardOperatorStatus {
    config: DfsMetadataShardConfig,
    snapshot: Option<ResidentLocalMetadataShardStatusSnapshot>,
}

/// Post-commit bridge from an async replica ACK into bounded resident status.
///
/// It owns neither the shard backend nor its mutation lease. The durable
/// cursor remains authoritative; this observer advances only the monotonic
/// in-memory projection and the matching last-complete operator image.
#[derive(Clone, Debug)]
pub(crate) struct ResidentMetadataReplicationCursorStatusObserver {
    shard_id: DfsMetadataShardId,
    slot: Arc<ResidentLocalMetadataShardSlot>,
    projection: ResidentMetadataReplicationCursorProjectionObserver,
}

impl ResidentMetadataReplicationCursorStatusObserver {
    pub(crate) fn observe(
        &self,
        target_node_id: &NodeId,
        shard_id: &DfsMetadataShardId,
        sequence: u64,
    ) {
        if shard_id != &self.shard_id {
            debug_assert_eq!(shard_id, &self.shard_id);
            return;
        }

        // Lease restore takes these locks in the same order. That prevents a
        // restore which sampled the old projection from overwriting this
        // newer post-ACK status image.
        let mut operator_status = self
            .slot
            .operator_status
            .write()
            .unwrap_or_else(|error| error.into_inner());
        if !operator_status
            .config
            .replica_node_ids
            .contains(target_node_id)
        {
            // The durable ACK remains authoritative on disk, but a retained
            // predecessor plan cannot repopulate the current generation's
            // in-memory projection or bounded status with a removed target.
            return;
        }
        self.projection.observe(target_node_id, shard_id, sequence);
        let Some(snapshot) = operator_status.snapshot.as_mut() else {
            return;
        };
        let journal_last_sequence = snapshot.shard_status.journal_last_sequence;
        let Some(replica) = snapshot
            .shard_status
            .replica_lag
            .iter_mut()
            .find(|replica| replica.target_node_id == *target_node_id)
        else {
            // A retained plan may finish while a newer resident topology no
            // longer projects this target. The old plan still owns its
            // durable cursor ACK, but it must not invent a status row outside
            // the slot's retained replica set.
            return;
        };
        replica.replicated_through_sequence = replica.replicated_through_sequence.max(sequence);
        replica.lag_entries =
            journal_last_sequence.saturating_sub(replica.replicated_through_sequence);
    }
}

/// One bounded operator image from a resident sharded metadata generation.
///
/// The image is assembled from per-shard projections published when shard
/// leases complete. It never checks out or scans a metadata table generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsResidentMetadataStatusSnapshot {
    pub summary: DfsMetadataSummary,
    pub primary_summary: DfsMetadataSummary,
    pub metadata_shards: Vec<DfsMetadataShardStatus>,
    pub incomplete_reasons: Vec<String>,
    pub bounded_omissions: Vec<DfsOperatorStatusOmission>,
}

pub(crate) fn metadata_shard_topology_status(
    config: &DfsMetadataShardConfig,
) -> DfsMetadataShardStatus {
    DfsMetadataShardStatus {
        shard_id: DfsMetadataShardId::new(config.shard_id.clone()),
        file_system_id: Some(config.file_system_id.clone()),
        primary_node_id: Some(config.primary_node_id.clone()),
        replica_node_ids: config.replica_node_ids.clone(),
        epoch: Some(config.epoch),
        journal_next_sequence: 0,
        journal_last_sequence: 0,
        replica_lag: Vec::new(),
        replay_cursors: Vec::new(),
    }
}

fn non_primary_unhydrated_status_omission(
    local_node_id: Option<&NodeId>,
    config: &DfsMetadataShardConfig,
    shard_id: &DfsMetadataShardId,
) -> Option<DfsOperatorStatusOmission> {
    local_node_id
        .filter(|local_node_id| &config.primary_node_id != *local_node_id)
        .map(|_| {
            DfsOperatorStatusOmission::non_primary_resident_metadata_not_hydrated(
                config.file_system_id.clone(),
                shard_id.clone(),
            )
        })
}

struct ResidentMetadataShardOpening {
    slot: Arc<ResidentLocalMetadataShardSlot>,
    armed: bool,
}
impl Drop for ResidentMetadataShardOpening {
    fn drop(&mut self) {
        if self.armed {
            let mut state = self
                .slot
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if matches!(*state, ResidentLocalMetadataShardState::Opening) {
                *state = ResidentLocalMetadataShardState::Unopened;
                self.slot.available.notify_all();
                self.slot.async_available.notify_waiters();
            }
        }
    }
}

impl ResidentLocalMetadataShardSlot {
    fn from_local_slot(slot: LocalMetadataShardSlot) -> Self {
        let LocalMetadataShardSlot {
            config,
            root,
            open_mode,
            selected_original_data_layout,
            initialization: _,
            backend,
        } = slot;
        let backend = backend.into_inner();
        let status_snapshot = backend
            .as_deref()
            .map(|backend| initialize_resident_metadata_shard_status_snapshot(&config, backend));
        Self {
            config: config.clone(),
            root,
            open_mode,
            selected_original_data_layout,
            state: Mutex::new(match backend {
                Some(backend) => ResidentLocalMetadataShardState::Available(backend),
                None => ResidentLocalMetadataShardState::Unopened,
            }),
            available: Condvar::new(),
            async_available: tokio::sync::Notify::new(),
            operator_status: RwLock::new(ResidentLocalMetadataShardOperatorStatus {
                config,
                snapshot: status_snapshot,
            }),
        }
    }

    fn open_backend(&self) -> Result<Box<LocalSobjMetadataBackend>> {
        open_local_metadata_shard_backend(&self.config, &self.root, self.open_mode, self.selected_original_data_layout.clone()).map(Box::new)
    }

    fn try_checkout(self: &Arc<Self>) -> Result<Option<Box<LocalSobjMetadataBackend>>> {
        self.try_checkout_with_open(&mut || self.open_backend())
    }

    fn try_checkout_with_open(
        self: &Arc<Self>,
        open: &mut dyn FnMut() -> Result<Box<LocalSobjMetadataBackend>>,
    ) -> Result<Option<Box<LocalSobjMetadataBackend>>> {
        let mut state = self.state.lock().map_err(|error| {
            MetadataBackendError::Serde(format!(
                "resident metadata shard {} state lock failed: {error}",
                self.config.shard_id
            ))
        })?;
        tracing::trace!(
            target: "sinomo.dfs.perf",
            data_root = %self.root.display(),
            fs = %self.config.file_system_id,
            shard = %self.config.shard_id,
            slot_state = match &*state {
                ResidentLocalMetadataShardState::Unopened => "unopened",
                ResidentLocalMetadataShardState::Opening => "opening",
                ResidentLocalMetadataShardState::Available(_) => "available",
                ResidentLocalMetadataShardState::CheckedOut => "checked-out",
            },
            "resident metadata checkout state"
        );
        match &*state {
            ResidentLocalMetadataShardState::Unopened => {
                *state = ResidentLocalMetadataShardState::Opening;
                drop(state);
                let mut opening = ResidentMetadataShardOpening {
                    slot: Arc::clone(self),
                    armed: true,
                };
                let opened = open();
                let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                match opened {
                    Ok(backend) => {
                        let mut operator_status = self
                            .operator_status
                            .write()
                            .unwrap_or_else(|error| error.into_inner());
                        let status_config = operator_status.config.clone();
                        operator_status.snapshot =
                            Some(initialize_resident_metadata_shard_status_snapshot(
                                &status_config,
                                &backend,
                            ));
                        *state = ResidentLocalMetadataShardState::CheckedOut;
                        opening.armed = false;
                        Ok(Some(backend))
                    }
                    Err(error) => {
                        *state = ResidentLocalMetadataShardState::Unopened;
                        self.available.notify_all();
                        self.async_available.notify_waiters();
                        opening.armed = false;
                        Err(error)
                    }
                }
            }
            ResidentLocalMetadataShardState::Available(_) => {
                let ResidentLocalMetadataShardState::Available(backend) =
                    std::mem::replace(&mut *state, ResidentLocalMetadataShardState::CheckedOut)
                else {
                    unreachable!("resident metadata shard availability checked above")
                };
                Ok(Some(backend))
            }
            ResidentLocalMetadataShardState::Opening
            | ResidentLocalMetadataShardState::CheckedOut => Ok(None),
        }
    }

    fn checkout(self: &Arc<Self>) -> Result<Box<LocalSobjMetadataBackend>> {
        self.checkout_with_open(&mut || self.open_backend())
    }

    fn checkout_with_open(
        self: &Arc<Self>,
        open: &mut dyn FnMut() -> Result<Box<LocalSobjMetadataBackend>>,
    ) -> Result<Box<LocalSobjMetadataBackend>> {
        loop {
            if let Some(backend) = self.try_checkout_with_open(open)? {
                return Ok(backend);
            }
            let mut state = self.state.lock().map_err(|error| {
                MetadataBackendError::Serde(format!(
                    "resident metadata shard {} state lock failed: {error}",
                    self.config.shard_id
                ))
            })?;
            while matches!(
                *state,
                ResidentLocalMetadataShardState::Opening
                    | ResidentLocalMetadataShardState::CheckedOut
            ) {
                state = self.available.wait(state).map_err(|error| {
                    MetadataBackendError::Serde(format!(
                        "resident metadata shard {} availability wait failed: {error}",
                        self.config.shard_id
                    ))
                })?;
            }
        }
    }

    async fn wait_until_checkout_available(&self) -> Result<()> {
        loop {
            // Register before inspecting state so a restore between the check
            // and await cannot strand this waiter. Unlike the blocking
            // checkout, this owns no backend while it waits.
            let notified = self.async_available.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let state = self.state.lock().map_err(|error| {
                    MetadataBackendError::Serde(format!(
                        "resident metadata shard {} state lock failed: {error}",
                        self.config.shard_id
                    ))
                })?;
                if matches!(
                    *state,
                    ResidentLocalMetadataShardState::Unopened
                        | ResidentLocalMetadataShardState::Available(_)
                ) {
                    return Ok(());
                }
            }
            notified.await;
        }
    }

    fn restore(&self, backend: Box<LocalSobjMetadataBackend>) {
        self.restore_with_status_config(backend, None);
    }

    fn restore_with_status_config(
        &self,
        backend: Box<LocalSobjMetadataBackend>,
        status_config: Option<&DfsMetadataShardConfig>,
    ) {
        let mut operator_status = self
            .operator_status
            .write()
            .unwrap_or_else(|error| error.into_inner());
        let mut incomplete_reasons = Vec::new();
        if let Some(status_config) = status_config
            && operator_status.config != *status_config
        {
            if operator_status.config.replica_node_ids != status_config.replica_node_ids
                && let Err(error) = backend
                    .reconcile_resident_metadata_replication_cursor_status_projection(
                        &status_config.replica_node_ids,
                        &DfsMetadataShardId::new(status_config.shard_id.clone()),
                    )
            {
                incomplete_reasons.push(format!(
                    "metadata shard {} cursor projection could not be rebound: {error}",
                    status_config.shard_id
                ));
            }
            operator_status.config = status_config.clone();
        }
        let refreshed = resident_metadata_shard_status_snapshot_from_projection(
            &operator_status.config,
            &backend,
            incomplete_reasons,
        );
        operator_status.snapshot = Some(refreshed);
        drop(operator_status);
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        debug_assert!(matches!(
            *state,
            ResidentLocalMetadataShardState::CheckedOut
        ));
        *state = ResidentLocalMetadataShardState::Available(backend);
        self.available.notify_all();
        self.async_available.notify_waiters();
    }

    /// Populate the last completed operator projection for a deferred replica
    /// without turning the replica into a permanently resident backend.
    ///
    /// This is a startup-only operation in normal daemon use. The state wait
    /// keeps the helper correct if a future caller races it with a scoped
    /// checkout: an already-open shard retains its resident authority instance,
    /// while an unopened shard is inspected once and returned to `Unopened`.
    fn initialize_deferred_bounded_status_snapshot(&self) -> Result<()> {
        let mut state = self.state.lock().map_err(|error| {
            MetadataBackendError::Serde(format!(
                "resident metadata shard {} state lock failed: {error}",
                self.config.shard_id
            ))
        })?;
        loop {
            match &*state {
                ResidentLocalMetadataShardState::Unopened => {
                    *state = ResidentLocalMetadataShardState::Opening;
                    break;
                }
                ResidentLocalMetadataShardState::Available(_) => return Ok(()),
                ResidentLocalMetadataShardState::Opening
                | ResidentLocalMetadataShardState::CheckedOut => {
                    state = self.available.wait(state).map_err(|error| {
                        MetadataBackendError::Serde(format!(
                            "resident metadata shard {} availability wait failed: {error}",
                            self.config.shard_id
                        ))
                    })?;
                }
            }
        }
        drop(state);

        let opened = self.open_backend();
        match opened {
            Ok(backend) => {
                let snapshot =
                    initialize_resident_metadata_shard_status_snapshot(&self.config, &backend);
                self.operator_status
                    .write()
                    .unwrap_or_else(|error| error.into_inner())
                    .snapshot = Some(snapshot);
                let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                debug_assert!(matches!(*state, ResidentLocalMetadataShardState::Opening));
                *state = ResidentLocalMetadataShardState::Unopened;
                self.available.notify_all();
                self.async_available.notify_waiters();
                drop(state);
                drop(backend);
                Ok(())
            }
            Err(error) => {
                let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                *state = ResidentLocalMetadataShardState::Unopened;
                self.available.notify_all();
                self.async_available.notify_waiters();
                if self.open_mode == LocalMetadataShardOpenMode::ReplicaRepair
                    && crate::metadata_replication::metadata_backend_open_error_allows_snapshot_repair(&error)
                {
                    self.operator_status.write().unwrap_or_else(|error| error.into_inner()).snapshot = None;
                    tracing::warn!(target: "sinomo.dfs", shard = %self.config.shard_id,
                        error = %error, "replica status unavailable; authenticated snapshot repair required");
                    Ok(())
                } else {
                    Err(error)
                }
            }
        }
    }

    /// Return a bounded operator projection without waiting for ownership of
    /// the shard backend. A checked-out shard deliberately serves the last
    /// completed generation. An unopened shard serves a startup-inspected
    /// projection when present; otherwise it is reported as unavailable instead
    /// of being hydrated by an operator read.
    fn bounded_status_snapshot(&self) -> Result<ResidentLocalMetadataShardStatusSnapshot> {
        let state = match self.state.try_lock() {
            Ok(state) => state,
            Err(TryLockError::WouldBlock) => return self.last_status_snapshot(),
            Err(TryLockError::Poisoned(error)) => {
                return Err(MetadataBackendError::Serde(format!(
                    "resident metadata shard {} state lock failed: {error}",
                    self.config.shard_id
                )));
            }
        };
        match &*state {
            ResidentLocalMetadataShardState::Available(_) => self.last_status_snapshot(),
            ResidentLocalMetadataShardState::Opening
            | ResidentLocalMetadataShardState::CheckedOut => self.last_status_snapshot(),
            ResidentLocalMetadataShardState::Unopened => match self.last_status_snapshot() {
                Ok(snapshot) => Ok(snapshot),
                Err(MetadataBackendError::StatusUnavailable { .. }) => {
                    Err(MetadataBackendError::StatusUnhydrated {
                        shard_id: DfsMetadataShardId::new(self.config.shard_id.clone()),
                    })
                }
                Err(error) => Err(error),
            },
        }
    }

    fn last_status_snapshot(&self) -> Result<ResidentLocalMetadataShardStatusSnapshot> {
        self.operator_status
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .snapshot
            .clone()
            .ok_or_else(|| MetadataBackendError::StatusUnavailable {
                reason: format!(
                    "resident metadata shard {} has no completed generation for bounded status",
                    self.config.shard_id
                ),
            })
    }

    fn is_open(&self) -> bool {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        matches!(
            *state,
            ResidentLocalMetadataShardState::Available(_)
                | ResidentLocalMetadataShardState::CheckedOut
        )
    }
}

#[cfg(test)]
thread_local! {
    static RESIDENT_METADATA_STATUS_DURABLE_CURSOR_READS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

fn initialize_resident_metadata_shard_status_snapshot(
    config: &DfsMetadataShardConfig,
    backend: &LocalSobjMetadataBackend,
) -> ResidentLocalMetadataShardStatusSnapshot {
    #[cfg(test)]
    RESIDENT_METADATA_STATUS_DURABLE_CURSOR_READS.set(
        RESIDENT_METADATA_STATUS_DURABLE_CURSOR_READS
            .get()
            .saturating_add(1),
    );
    let incomplete_reasons = backend
        .initialize_resident_metadata_cursor_status_projection(
            &config.replica_node_ids,
            &DfsMetadataShardId::new(config.shard_id.clone()),
        )
        .err()
        .map(|error| {
            vec![format!(
                "metadata shard {} cursor projection could not be initialized: {error}",
                config.shard_id
            )]
        })
        .unwrap_or_default();
    resident_metadata_shard_status_snapshot_from_projection(config, backend, incomplete_reasons)
}

fn resident_metadata_shard_status_snapshot_from_projection(
    config: &DfsMetadataShardConfig,
    backend: &LocalSobjMetadataBackend,
    mut incomplete_reasons: Vec<String>,
) -> ResidentLocalMetadataShardStatusSnapshot {
    let shard_id = DfsMetadataShardId::new(config.shard_id.clone());
    let journal_last_sequence = backend.journal_last_sequence();
    let (replica_lag, replay_cursors) = match backend
        .resident_metadata_cursor_status_projection(&config.replica_node_ids, &shard_id)
    {
        Some((replication_cursors, replay_cursors)) => (
            replication_cursors
                .into_iter()
                .map(
                    |(target_node_id, replicated_through_sequence)| DfsMetadataReplicaLagStatus {
                        target_node_id,
                        replicated_through_sequence,
                        lag_entries: journal_last_sequence
                            .saturating_sub(replicated_through_sequence),
                    },
                )
                .collect(),
            replay_cursors,
        ),
        None => {
            if incomplete_reasons.is_empty() {
                incomplete_reasons.push(format!(
                    "metadata shard {} has no complete resident cursor projection",
                    config.shard_id
                ));
            }
            (Vec::new(), Vec::new())
        }
    };
    ResidentLocalMetadataShardStatusSnapshot {
        summary: backend.resident_metadata_status_summary(),
        shard_status: DfsMetadataShardStatus {
            shard_id,
            file_system_id: Some(config.file_system_id.clone()),
            primary_node_id: Some(config.primary_node_id.clone()),
            replica_node_ids: config.replica_node_ids.clone(),
            epoch: Some(config.epoch),
            journal_next_sequence: journal_last_sequence.saturating_add(1),
            journal_last_sequence,
            replica_lag,
            replay_cursors,
        },
        incomplete_reasons,
    }
}

/// A cheap, exact-shard handle into a resident local metadata generation.
///
/// Cloning this value does not clone or reopen metadata. Each clone points to
/// the same shard-local backend and therefore observes the same process-local
/// membership-handoff authority binding.
#[derive(Clone, Debug)]
pub struct ResidentLocalMetadataShardHandle {
    file_system_id: DfsFileSystemId,
    shard_id: DfsMetadataShardId,
    slot: Arc<ResidentLocalMetadataShardSlot>,
}

impl ResidentLocalMetadataShardHandle {
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn native_replica_observation_identity(&self) -> (PathBuf, String) {
        // Existing retained slot/root only: no lazy open, canonicalize, stat,
        // writer acquisition, or authority read.
        (
            self.slot.root.clone(),
            format!("{:p}", Arc::as_ptr(&self.slot)),
        )
    }
    pub fn file_system_id(&self) -> &DfsFileSystemId {
        &self.file_system_id
    }

    pub fn shard_id(&self) -> &DfsMetadataShardId {
        &self.shard_id
    }

    /// Required nonopening read through the SAME resident slot. Wait for its
    /// current checkout, but refuse a missing owner instead of opening storage.
    /// No replay/repair or initialization authority is supplied by this read.
    pub(crate) fn read_prepared_operation_record_without_open(
        &self,
        operation_id: &str,
    ) -> Result<Option<Option<DfsPreparedOperationRecord>>> {
        let backend = self.slot.checkout_with_open(&mut || {
            Err(MetadataBackendError::Serde(
                "resident prepared-record observation requires an already-open owner".into(),
            ))
        })?;
        let lease = ResidentLocalMetadataShardLease {
            file_system_id: self.file_system_id.clone(),
            shard_id: self.shard_id.clone(),
            slot: Arc::clone(&self.slot),
            backend: Some(backend),
        };
        lease.read_certified_prepared_record_for_ingress(operation_id)
    }

    /// Use the SAME strict primary slot to prepare storage for ingress, without
    /// entering a service lane or semantic prepare recovery. None means another
    /// owner is opening/using the slot; callers wait after releasing admission.
    pub(crate) fn try_prepare_genesis_record_for_ingress(
        &self,
        expected: &DfsMetadataShardConfig,
        operation_id: &str,
    ) -> Result<Option<Option<DfsPreparedOperationRecord>>> {
        if self.slot.config != *expected
            || self.slot.open_mode != LocalMetadataShardOpenMode::Strict
        {
            return Err(MetadataBackendError::Serde(
                "genesis ingress preparation requires its exact strict primary slot".into(),
            ));
        }
        let Some(mut lease) = self.try_checkout()? else {
            return Ok(None);
        };
        if let Some(record) = lease.try_read_certified_prepared_record_for_ingress(operation_id)? {
            return Ok(Some(record));
        }
        // Existing storage-owner preparation certifies/replays already durable
        // frames. It cannot author a CommitV7 or run Core namespace recovery.
        #[cfg(test)]
        GENESIS_PREPARATION_OBSERVER.with(|observer| {
            if let Some(observe) = observer.borrow().as_ref() { observe(0); }
        });
        lease.begin_consistent_read_snapshot()?;
        lease.end_consistent_read_snapshot();
        #[cfg(test)]
        GENESIS_PREPARATION_OBSERVER.with(|observer| {
            if let Some(observe) = observer.borrow().as_ref() { observe(1); }
        });
        let record = lease
            .read_certified_prepared_record_for_ingress(operation_id)?
            .ok_or_else(|| {
                MetadataBackendError::Serde(
                    "strict genesis ingress preparation has no current certified materialized cut"
                        .into(),
                )
            })?;
        Ok(Some(record))
    }

    /// Check out only this shard's resident backend for one bounded operation.
    /// A competing checkout waits until the returned lease is dropped.
    pub fn checkout(&self) -> Result<ResidentLocalMetadataShardLease> {
        let backend = self.slot.checkout()?;
        Ok(ResidentLocalMetadataShardLease {
            file_system_id: self.file_system_id.clone(),
            shard_id: self.shard_id.clone(),
            slot: Arc::clone(&self.slot),
            backend: Some(backend),
        })
    }

    /// Attempt to check out this shard without waiting for a competing owner.
    ///
    /// `Ok(None)` means the shard is opening or checked out. Callers that hold
    /// another admission or lock can release it before waiting for
    /// [`Self::wait_until_checkout_available`], avoiding cross-lock cycles.
    pub fn try_checkout(&self) -> Result<Option<ResidentLocalMetadataShardLease>> {
        self.slot.try_checkout().map(|backend| {
            backend.map(|backend| ResidentLocalMetadataShardLease {
                file_system_id: self.file_system_id.clone(),
                shard_id: self.shard_id.clone(),
                slot: Arc::clone(&self.slot),
                backend: Some(backend),
            })
        })
    }

    /// Wait asynchronously until a checkout can be retried without retaining
    /// a shard lease or another subsystem's admission guard.
    pub async fn wait_until_checkout_available(&self) -> Result<()> {
        self.slot.wait_until_checkout_available().await
    }

    /// Run one bounded operation against the exact resident backend instance.
    pub fn with_backend<R>(
        &self,
        operation: impl FnOnce(&mut LocalSobjMetadataBackend) -> Result<R>,
    ) -> Result<R> {
        let mut lease = self.checkout()?;
        operation(&mut lease)
    }

    /// Install through this same resident slot even when its replica materialization
    /// is unreadable. Source/context checks and durable floors still precede archive.
    pub(crate) fn install_replica_snapshot(
        &self,
        context: &crate::DfsMetadataReplicaShardContext,
        command: crate::DfsMetadataSnapshotInstallCommand,
    ) -> Result<crate::DfsMetadataSnapshotInstallResponse> {
        if let Some(response) =
            crate::metadata_replication::validate_dfs_metadata_snapshot_install_source(
                Some(context),
                &command,
            )
        {
            return Ok(response);
        }
        if self.file_system_id != context.shard.file_system_id || self.shard_id != command.shard_id
        {
            return Err(MetadataBackendError::Serde(
                "resident snapshot slot differs from validated context".into(),
            ));
        }
        let permit =
            crate::metadata_replication::metadata_snapshot_install_permit(context, &command)
                .map_err(MetadataBackendError::Serde)?;
        #[cfg(any(test, feature = "test-support"))]
        let mut native_boundary = crate::native_shard_trace::current_boundary_interest();
        #[cfg(any(test, feature = "test-support"))]
        let native_input = native_boundary.as_ref().map(|_| command.clone());
        let mut recovered = None;
        let mut open = || {
            match self.slot.open_backend() {
                Ok(backend) => Ok(backend),
                Err(error) if self.slot.open_mode == LocalMetadataShardOpenMode::ReplicaRepair
                    && crate::metadata_replication::metadata_backend_open_error_allows_snapshot_repair(&error) => {
                    let image = command.image()?.clone();
                    let (backend, archived_existing) = LocalSobjMetadataBackend::replace_unreadable_with_replicated_snapshot(
                        &permit, self.shard_id.clone(), self.slot.root.clone(),
                        command.shard_id.clone(), command.next_after_sequence, image,
                    )?;
                    recovered = Some(archived_existing);
                    Ok(Box::new(backend))
                }
                Err(error) => Err(error),
            }
        };
        // Opening remains exclusively owned across failure and replacement.
        // The existing lease publishes ONLY the real installed backend on drop.
        let backend = self.slot.checkout_with_open(&mut open)?;
        drop(open);
        let mut lease = ResidentLocalMetadataShardLease {
            file_system_id: self.file_system_id.clone(),
            shard_id: self.shard_id.clone(),
            slot: Arc::clone(&self.slot),
            backend: Some(backend),
        };
        let response = if let Some(archived_existing) = recovered {
            crate::DfsMetadataSnapshotInstallResponse {
                request_id: command.request_id,
                shard_id: command.shard_id,
                next_after_sequence: command.next_after_sequence,
                result: crate::DfsMetadataSnapshotInstallResult::Installed { archived_existing },
            }
        } else {
            crate::metadata_replication::process_dfs_metadata_snapshot_install(
                &mut *lease,
                Some(context),
                command,
            )
        };
        #[cfg(any(test, feature = "test-support"))]
        if recovered.is_some()
            && matches!(
                &response.result,
                crate::DfsMetadataSnapshotInstallResult::Installed { .. }
            )
            && let (Some(trace), Some(input)) = (native_boundary.as_mut(), native_input)
        {
            trace.observe(
                crate::native_shard_trace::NativeBoundaryFactsV1::ReplicaSnapshotAccepted {
                    local_node_id: context.local_node_id.clone(),
                    actual_source_config: context.shard.clone(),
                    actual_pending_handoff: context.pending_membership_handoff.clone(),
                    command: input,
                    response: response.clone(),
                    permit: crate::native_shard_trace::NativeReplicaPermitTupleV1::observed(
                        &permit,
                    ),
                },
                None,
                lease.native_held_snapshot_for_scope(
                    &context.shard.file_system_id,
                    &response.shard_id,
                ),
            );
        }
        Ok(response)
    }

    pub fn is_open(&self) -> bool {
        self.slot.is_open()
    }
}

/// Owned, exclusive access to one resident shard-local metadata backend.
///
/// Dropping the lease returns the same backend instance to its resident slot.
pub struct ResidentLocalMetadataShardLease {
    file_system_id: DfsFileSystemId,
    shard_id: DfsMetadataShardId,
    slot: Arc<ResidentLocalMetadataShardSlot>,
    backend: Option<Box<LocalSobjMetadataBackend>>,
}

impl std::fmt::Debug for ResidentLocalMetadataShardLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResidentLocalMetadataShardLease")
            .field("file_system_id", &self.file_system_id)
            .field("shard_id", &self.shard_id)
            .finish_non_exhaustive()
    }
}

impl ResidentLocalMetadataShardLease {
    pub fn file_system_id(&self) -> &DfsFileSystemId {
        &self.file_system_id
    }

    pub fn shard_id(&self) -> &DfsMetadataShardId {
        &self.shard_id
    }

    pub fn shard_root(&self) -> &Path {
        &self.slot.root
    }

    pub(crate) fn metadata_replication_cursor_status_observer(
        &self,
    ) -> ResidentMetadataReplicationCursorStatusObserver {
        ResidentMetadataReplicationCursorStatusObserver {
            shard_id: self.shard_id.clone(),
            slot: Arc::clone(&self.slot),
            projection: self
                .backend
                .as_ref()
                .expect("resident metadata shard lease retains backend until drop")
                .resident_metadata_replication_cursor_projection_observer(),
        }
    }
}

impl Deref for ResidentLocalMetadataShardLease {
    type Target = LocalSobjMetadataBackend;

    fn deref(&self) -> &Self::Target {
        self.backend
            .as_ref()
            .expect("resident metadata shard lease retains backend until drop")
    }
}

impl DerefMut for ResidentLocalMetadataShardLease {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.backend
            .as_mut()
            .expect("resident metadata shard lease retains backend until drop")
    }
}

impl Drop for ResidentLocalMetadataShardLease {
    fn drop(&mut self) {
        if let Some(backend) = self.backend.take() {
            self.slot.restore(backend);
        }
    }
}

#[derive(Debug)]
struct ResidentLocalShardedMetadataRouterState {
    root: PathBuf,
    local_node_id: Option<NodeId>,
    shard_configs: BTreeMap<String, DfsMetadataShardConfig>,
    routing_indexes: BTreeMap<DfsFileSystemId, DfsMetadataShardRoutingIndex>,
    fallback_routing_indexes: Vec<BTreeMap<DfsFileSystemId, DfsMetadataShardRoutingIndex>>,
}

#[derive(Debug)]
struct ResidentLocalShardedMetadataBackendInner {
    router: Mutex<ResidentLocalShardedMetadataRouterState>,
    shards: BTreeMap<DfsMetadataShardId, Arc<ResidentLocalMetadataShardSlot>>,
}

/// One resident sharded metadata generation with independent shard locks.
///
/// This is intentionally a pool of the original shard backend instances, not
/// an `Arc<Mutex<LocalShardedMetadataBackend>>`: unrelated shards can execute
/// concurrently, while same-shard calls retain the existing serialization and
/// authority semantics.
#[derive(Clone, Debug)]
pub struct ResidentLocalShardedMetadataBackend {
    inner: Arc<ResidentLocalShardedMetadataBackendInner>,
}

impl ResidentLocalShardedMetadataBackend {
    pub fn shard_ids(&self) -> Vec<DfsMetadataShardId> {
        self.inner.shards.keys().cloned().collect()
    }

    /// Rebuild the normal router while retaining only identical physical
    /// shard slots. The caller's current authority generation still owns
    /// admission; this DATA comparison creates no writer capability.
    /// Rebuild the router for `shards`. The local copies named by `discarded`
    /// are deleted before the changed slots reopen.
    pub(crate) fn into_reconfigured_for_replica_repair(
        self,
        root: PathBuf,
        mut shards: Vec<DfsMetadataShardConfig>,
        local_node_id: &NodeId,
        discarded: &BTreeSet<DfsMetadataShardId>,
    ) -> Result<Self> {
        canonicalize_routing_topology(&mut shards);
        let reusable = self.with_router(
            |router| -> Result<BTreeMap<DfsMetadataShardId, Arc<ResidentLocalMetadataShardSlot>>> {
                if router.root != root || router.local_node_id.as_ref() != Some(local_node_id) {
                    return Ok(BTreeMap::new());
                }
                let mut reusable = BTreeMap::new();
                for config in &shards {
                    if router.shard_configs.get(&config.shard_id) != Some(config) {
                        continue;
                    }
                    let shard_id = DfsMetadataShardId::new(config.shard_id.clone());
                    if discarded.contains(&shard_id) {
                        continue;
                    }
                    let Some(slot) = self.inner.shards.get(&shard_id) else {
                        continue;
                    };
                    let open_mode = if config.primary_node_id == *local_node_id {
                        LocalMetadataShardOpenMode::Strict
                    } else {
                        LocalMetadataShardOpenMode::ReplicaRepair
                    };
                    if slot.config != *config
                        || slot.root != root.join(key_segment(&shard_id.0))
                        || slot.open_mode != open_mode
                    {
                        continue;
                    }
                    let status = slot.operator_status.read().map_err(|error| {
                        MetadataBackendError::Serde(format!(
                            "resident metadata shard {} status lock failed: {error}",
                            shard_id.0
                        ))
                    })?;
                    // A global handoff can rebind a slot's status/config while
                    // retaining its immutable initial descriptor. Both views must
                    // match: never retain a predecessor epoch through that seam.
                    if status.config == *config {
                        reusable.insert(shard_id, Arc::clone(slot));
                    }
                }
                Ok(reusable)
            },
        )??;
        let selected_layout = self.inner.shards.values()
            .find_map(|slot| slot.selected_original_data_layout.clone());
        if let Some(layout) = &selected_layout {
            layout.check_routing_root(&root)?;
            if self.inner.shards.values().any(|slot| slot.selected_original_data_layout.as_ref()
                .is_none_or(|other| !Arc::ptr_eq(other, layout)))
            {
                return Err(MetadataBackendError::Rejected {
                    code: crate::DfsMetadataRejectionCode::Other,
                    message: "resident selected layout context is inconsistent".into(),
                });
            }
        }
        let retained_ids = reusable.keys().cloned().collect::<BTreeSet<_>>();
        // Release changed/removed slots and the predecessor router before
        // normal current opening. Retained exact slots keep their one shared
        // checkout state, status and instance; no second pool is opened.
        drop(self);
        discard_local_metadata_shard_copies(&root, discarded)?;
        let mut current = LocalShardedMetadataBackend::open_with_retained_shards_and_layout(
            root,
            shards,
            true,
            Some(local_node_id),
            &retained_ids,
            selected_layout,
            true,
        )?
        .into_resident();
        let inner = Arc::get_mut(&mut current.inner).ok_or_else(|| {
            MetadataBackendError::Serde("new resident router unexpectedly shared".into())
        })?;
        let current_configs = inner
            .router
            .get_mut()
            .map_err(|error| {
                MetadataBackendError::Serde(format!("new resident router lock failed: {error}"))
            })?
            .shard_configs
            .clone();
        for (shard_id, slot) in reusable {
            let status = slot.operator_status.read().map_err(|error| {
                MetadataBackendError::Serde(format!(
                    "resident metadata shard {} status lock failed: {error}",
                    shard_id.0
                ))
            })?;
            if current_configs.get(&shard_id.0) != Some(&status.config)
                || slot.config != status.config
            {
                return Err(MetadataBackendError::Serde(format!(
                    "resident metadata shard {} changed during router reconstruction",
                    shard_id.0
                )));
            }
            inner.shards.insert(shard_id, Arc::clone(&slot));
        }
        tracing::trace!(target: "sinomo.dfs.perf", detail = "resident-shard-retention",
            retained_shards = retained_ids.len(), configured_shards = current_configs.len());
        Ok(current)
    }

    /// Initialize every shard's compact operator projection during daemon
    /// startup, before the resident generation is published.
    ///
    /// Bounded status reads deliberately never open deferred replica shards.
    /// A restarted daemon must therefore own this one-time inspection so it can
    /// prove cluster-wide lock and job invariants even when a quiet non-primary
    /// shard receives no new replication after restart. Inspection leaves the
    /// replica deferred, preserving lazy repair and idle-runtime behavior.
    pub fn initialize_bounded_status_snapshots(&self) -> Result<()> {
        for slot in self.inner.shards.values() {
            slot.initialize_deferred_bounded_status_snapshot()?;
        }
        Ok(())
    }

    pub fn shard_config(&self, shard_id: &DfsMetadataShardId) -> Result<DfsMetadataShardConfig> {
        self.with_router(|router| router.shard_configs.get(&shard_id.0).cloned())?
            .ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "unknown resident DFS metadata shard: {}",
                    shard_id.0
                ))
            })
    }

    /// Verify that this resident generation is the exact durable router the
    /// caller intends to borrow. This is used at boundaries where cluster KV
    /// may have changed after a resident handle was captured.
    pub fn matches_topology(
        &self,
        metadata_root: &Path,
        shards: &[DfsMetadataShardConfig],
    ) -> Result<bool> {
        let expected = shards
            .iter()
            .map(|shard| (shard.shard_id.clone(), shard.clone()))
            .collect::<BTreeMap<_, _>>();
        if expected.len() != shards.len() {
            return Ok(false);
        }
        self.with_router(|router| router.root == metadata_root && router.shard_configs == expected)
    }

    /// Assemble an operator status image without checking out any shard or
    /// scanning a materialized metadata table. Shards currently owned by a
    /// mutation contribute their last completed generation.
    ///
    /// Historical routing fallbacks mean a namespace move can temporarily
    /// have live copies in both the source and destination shards. Determining
    /// which copy is authoritative requires the durable routing/index probes
    /// used by the exact scanner, so this bounded path omits visibility-based
    /// counters and marks the image incomplete until migration retires the
    /// fallbacks. Status itself must remain bounded and available.
    pub fn bounded_status_snapshot(&self) -> Result<DfsResidentMetadataStatusSnapshot> {
        // Retain the router generation while collecting every slot image.
        // Global handoff restore holds the same lock while rebinding slot
        // status, so an operator snapshot cannot pair predecessor topology
        // with successor lag rows (or vice versa).
        let router = self.inner.router.lock().map_err(|error| {
            MetadataBackendError::Serde(format!(
                "resident DFS metadata router lock failed: {error}"
            ))
        })?;
        let local_node_id = router.local_node_id.clone();
        let shard_configs = router.shard_configs.clone();
        let fallback_count = router.fallback_routing_indexes.len();
        let mut summary = DfsMetadataSummary::default();
        let mut primary_summary = DfsMetadataSummary::default();
        let mut file_system_ids = BTreeSet::new();
        let mut primary_file_system_ids = BTreeSet::new();
        let mut metadata_shards = Vec::with_capacity(shard_configs.len());
        let mut incomplete_reasons = Vec::new();
        let mut bounded_omissions = Vec::new();
        let mut every_configured_shard_summary_available = true;
        for config in shard_configs.values() {
            let shard_id = DfsMetadataShardId::new(config.shard_id.clone());
            let local_primary = local_node_id
                .as_ref()
                .is_none_or(|local_node_id| config.primary_node_id == *local_node_id);
            file_system_ids.insert(config.file_system_id.clone());
            if local_primary {
                primary_file_system_ids.insert(config.file_system_id.clone());
            }
            let Some(slot) = self.inner.shards.get(&shard_id) else {
                every_configured_shard_summary_available = false;
                incomplete_reasons.push(format!(
                    "metadata shard {} has no resident bounded-status slot",
                    shard_id.0
                ));
                metadata_shards.push(metadata_shard_topology_status(config));
                continue;
            };
            let mut snapshot = match slot.bounded_status_snapshot() {
                Ok(snapshot) => snapshot,
                Err(MetadataBackendError::StatusUnhydrated {
                    shard_id: unhydrated_shard_id,
                }) if unhydrated_shard_id == shard_id && !local_primary => {
                    every_configured_shard_summary_available = false;
                    let omission = non_primary_unhydrated_status_omission(
                        local_node_id.as_ref(),
                        config,
                        &shard_id,
                    )
                    .expect("non-primary shard must produce a bounded omission");
                    bounded_omissions.push(omission);
                    metadata_shards.push(metadata_shard_topology_status(config));
                    continue;
                }
                Err(error) => {
                    every_configured_shard_summary_available = false;
                    incomplete_reasons.push(format!(
                        "metadata shard {} status unavailable: {error}",
                        shard_id.0
                    ));
                    metadata_shards.push(metadata_shard_topology_status(config));
                    continue;
                }
            };
            incomplete_reasons.append(&mut snapshot.incomplete_reasons);
            accumulate_metadata_summary_detail_counts(&mut summary, &snapshot.summary);
            if local_primary {
                accumulate_metadata_summary_detail_counts(&mut primary_summary, &snapshot.summary);
            }

            // The router config is the generation-level authority for
            // topology fields. A checked-out shard snapshot can legitimately
            // predate a just-completed topology update.
            snapshot.shard_status.file_system_id = Some(config.file_system_id.clone());
            snapshot.shard_status.primary_node_id = Some(config.primary_node_id.clone());
            snapshot.shard_status.replica_node_ids = config.replica_node_ids.clone();
            snapshot.shard_status.epoch = Some(config.epoch);
            metadata_shards.push(snapshot.shard_status);
        }
        drop(router);
        summary.file_system_count = file_system_ids.len();
        primary_summary.file_system_count = primary_file_system_ids.len();
        if fallback_count > 0 {
            let cross_shard_namespace_locks_zero_proven = every_configured_shard_summary_available
                && summary.cross_shard_namespace_lock_count == 0;
            let file_system_count = summary.file_system_count;
            let primary_file_system_count = primary_summary.file_system_count;
            summary = DfsMetadataSummary {
                file_system_count,
                ..DfsMetadataSummary::default()
            };
            primary_summary = DfsMetadataSummary {
                file_system_count: primary_file_system_count,
                ..DfsMetadataSummary::default()
            };
            let omission = if cross_shard_namespace_locks_zero_proven {
                DfsOperatorStatusOmission::historical_routing_topology_with_cross_shard_lock_zero_proof(
                    fallback_count,
                )
            } else {
                DfsOperatorStatusOmission::historical_routing_topology(fallback_count)
            };
            bounded_omissions.push(omission);
        }
        incomplete_reasons.sort();
        incomplete_reasons.dedup();
        Ok(DfsResidentMetadataStatusSnapshot {
            summary,
            primary_summary,
            metadata_shards,
            incomplete_reasons,
            bounded_omissions,
        })
    }

    /// Scope a handle to an exact filesystem and shard identity.
    ///
    /// Requiring both identities prevents a wire-derived shard name from
    /// borrowing an unrelated filesystem's resident backend.
    pub fn scope_shard(
        &self,
        file_system_id: &DfsFileSystemId,
        shard_id: &DfsMetadataShardId,
    ) -> Result<ResidentLocalMetadataShardHandle> {
        let slot = self.inner.shards.get(shard_id).ok_or_else(|| {
            MetadataBackendError::Serde(format!(
                "unknown resident DFS metadata shard: {}",
                shard_id.0
            ))
        })?;
        let configured_file_system_id = self
            .with_router(|router| {
                router
                    .shard_configs
                    .get(&shard_id.0)
                    .map(|config| config.file_system_id.clone())
            })?
            .ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "resident DFS metadata shard {} has no routing config",
                    shard_id.0
                ))
            })?;
        if configured_file_system_id != *file_system_id {
            return Err(MetadataBackendError::Serde(format!(
                "resident DFS metadata shard {} belongs to filesystem {}, not {}",
                shard_id.0, configured_file_system_id, file_system_id
            )));
        }
        Ok(ResidentLocalMetadataShardHandle {
            file_system_id: file_system_id.clone(),
            shard_id: shard_id.clone(),
            slot: Arc::clone(slot),
        })
    }

    /// Check out every shard in canonical shard-id order and reconstruct the
    /// original router around those exact instances.
    pub fn checkout_global(&self) -> Result<ResidentLocalShardedMetadataBackendLease> {
        let mut leases = Vec::with_capacity(self.inner.shards.len());
        for (shard_id, slot) in &self.inner.shards {
            tracing::trace!(
                target: "sinomo.dfs.perf",
                data_root = %slot.root.display(),
                fs = %slot.config.file_system_id,
                shard = %shard_id.0,
                checkpoint = "before-checkout",
                "resident global metadata checkout"
            );
            let backend = slot.checkout()?;
            tracing::trace!(
                target: "sinomo.dfs.perf",
                data_root = %slot.root.display(),
                fs = %slot.config.file_system_id,
                shard = %shard_id.0,
                checkpoint = "after-checkout",
                "resident global metadata checkout"
            );
            leases.push((
                shard_id.clone(),
                ResidentLocalMetadataShardLease {
                    file_system_id: slot.config.file_system_id.clone(),
                    shard_id: shard_id.clone(),
                    slot: Arc::clone(slot),
                    backend: Some(backend),
                },
            ));
        }
        // The first canonical shard also serializes competing global
        // checkouts. Read router state only after every shard is owned so a
        // waiter cannot capture and later restore a predecessor generation.
        let router = self.with_router(|router| ResidentLocalShardedMetadataRouterState {
            root: router.root.clone(),
            local_node_id: router.local_node_id.clone(),
            shard_configs: router.shard_configs.clone(),
            routing_indexes: router.routing_indexes.clone(),
            fallback_routing_indexes: router.fallback_routing_indexes.clone(),
        })?;
        let mut shards = BTreeMap::new();
        for (shard_id, mut lease) in leases {
            let slot = Arc::clone(&lease.slot);
            let backend = lease
                .backend
                .take()
                .expect("new global resident lease owns checked-out shard");
            let config = router
                .shard_configs
                .get(&shard_id.0)
                .cloned()
                .unwrap_or_else(|| slot.config.clone());
            let local_slot = LocalMetadataShardSlot {
                config,
                root: slot.root.clone(),
                open_mode: slot.open_mode,
                selected_original_data_layout: slot.selected_original_data_layout.clone(),
                initialization: Mutex::new(()),
                backend: OnceLock::new(),
            };
            local_slot
                .backend
                .set(backend)
                .expect("global resident checkout installs exact shard backend");
            shards.insert(shard_id, local_slot);
        }
        Ok(ResidentLocalShardedMetadataBackendLease {
            resident: self.clone(),
            backend: Some(LocalShardedMetadataBackend {
                root: router.root,
                local_node_id: router.local_node_id,
                shard_configs: router.shard_configs,
                routing_indexes: router.routing_indexes,
                fallback_routing_indexes: router.fallback_routing_indexes,
                shards,
            }),
        })
    }

    fn with_router<R>(
        &self,
        operation: impl FnOnce(&ResidentLocalShardedMetadataRouterState) -> R,
    ) -> Result<R> {
        let router = self.inner.router.lock().map_err(|error| {
            MetadataBackendError::Serde(format!(
                "resident DFS metadata router lock failed: {error}"
            ))
        })?;
        Ok(operation(&router))
    }
}

/// Owned, exclusive access to every shard in one resident router generation.
/// Dropping this lease restores the same shard instances and any router state
/// changed by handoff or routing-fallback reconciliation.
pub struct ResidentLocalShardedMetadataBackendLease {
    resident: ResidentLocalShardedMetadataBackend,
    backend: Option<LocalShardedMetadataBackend>,
}

impl std::fmt::Debug for ResidentLocalShardedMetadataBackendLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResidentLocalShardedMetadataBackendLease")
            .field("shard_count", &self.resident.inner.shards.len())
            .finish_non_exhaustive()
    }
}

impl Deref for ResidentLocalShardedMetadataBackendLease {
    type Target = LocalShardedMetadataBackend;

    fn deref(&self) -> &Self::Target {
        self.backend
            .as_ref()
            .expect("resident global metadata lease retains router until drop")
    }
}

impl DerefMut for ResidentLocalShardedMetadataBackendLease {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.backend
            .as_mut()
            .expect("resident global metadata lease retains router until drop")
    }
}

impl ResidentLocalShardedMetadataBackendLease {
    pub(crate) fn try_begin_resident_read_snapshot_for_path(
        &mut self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<bool> {
        let router = &mut **self;
        let shard_ids = router.consistent_read_shard_ids_for_path(file_system_id, path)?;
        #[cfg(test)]
        SHARDED_CONSISTENT_READ_SNAPSHOT_SHARDS
            .with(|observed| observed.borrow_mut().extend_from_slice(&shard_ids));
        for (index, shard_id) in shard_ids.iter().enumerate() {
            // A global resident lease owns already-opened kernels. Do not
            // cold-open an unrelated root or borrow another owner's backend.
            let begin = router
                .shards
                .get_mut(shard_id)
                .expect("routed resident shard remains installed")
                .opened_backend_mut()
                .ok_or_else(|| {
                    MetadataBackendError::Serde(
                        "resident read snapshot lost its checked-out kernel".into(),
                    )
                })
                .and_then(LocalSobjMetadataBackend::try_begin_resident_read_snapshot);
            if !matches!(begin, Ok(true)) {
                router.end_consistent_read_snapshot_for_shards(&shard_ids[..index]);
                return begin;
            }
        }
        Ok(true)
    }
}

impl Drop for ResidentLocalShardedMetadataBackendLease {
    fn drop(&mut self) {
        let Some(backend) = self.backend.take() else {
            return;
        };
        let LocalShardedMetadataBackend {
            root,
            local_node_id,
            shard_configs,
            routing_indexes,
            fallback_routing_indexes,
            shards,
        } = backend;
        let mut router = self
            .resident
            .inner
            .router
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        for (shard_id, slot) in shards {
            let backend = slot
                .backend
                .into_inner()
                .expect("global resident lease returns every checked-out shard backend");
            let resident_slot = self
                .resident
                .inner
                .shards
                .get(&shard_id)
                .expect("global resident lease returns known shard");
            let status_config = shard_configs
                .get(&shard_id.0)
                .unwrap_or(&resident_slot.config);
            resident_slot.restore_with_status_config(backend, Some(status_config));
        }
        *router = ResidentLocalShardedMetadataRouterState {
            root,
            local_node_id,
            shard_configs,
            routing_indexes,
            fallback_routing_indexes,
        };
    }
}

impl LocalMetadataShardSlot {
    fn new(
        config: DfsMetadataShardConfig,
        root: PathBuf,
        open_mode: LocalMetadataShardOpenMode,
    ) -> Self {
        Self {
            config,
            root,
            open_mode,
            selected_original_data_layout: None,
            initialization: Mutex::new(()),
            backend: OnceLock::new(),
        }
    }

    fn should_open_eagerly(&self) -> bool {
        self.open_mode != LocalMetadataShardOpenMode::ReplicaRepair
    }

    fn open_backend(&self) -> Result<Box<LocalSobjMetadataBackend>> {
        open_local_metadata_shard_backend(&self.config, &self.root, self.open_mode, self.selected_original_data_layout.clone()).map(Box::new)
    }

    fn backend(&self) -> Result<&LocalSobjMetadataBackend> {
        if self.backend.get().is_none() {
            let _initialization = self.initialization.lock().map_err(|error| {
                MetadataBackendError::Serde(format!(
                    "metadata shard {} initialization lock failed: {error}",
                    self.config.shard_id
                ))
            })?;
            if self.backend.get().is_none() {
                let backend = self.open_backend()?;
                self.backend
                    .set(backend)
                    .expect("metadata shard initialization lock admits one backend");
            }
        }
        Ok(self
            .backend
            .get()
            .expect("metadata shard slot initialized above")
            .as_ref())
    }

    fn backend_mut(&mut self) -> Result<&mut LocalSobjMetadataBackend> {
        if self.backend.get().is_none() {
            let backend = self.open_backend()?;
            self.backend
                .set(backend)
                .expect("exclusive metadata shard slot initialization succeeds");
        }
        Ok(self
            .backend
            .get_mut()
            .expect("metadata shard slot initialized above")
            .as_mut())
    }

    fn opened_backend_mut(&mut self) -> Option<&mut LocalSobjMetadataBackend> {
        self.backend.get_mut().map(Box::as_mut)
    }
}

fn open_local_metadata_shard_backend(
    config: &DfsMetadataShardConfig,
    root: &Path,
    open_mode: LocalMetadataShardOpenMode,
    selected_layout: Option<Arc<crate::DfsSelectedOriginalDataLayout>>,
) -> Result<LocalSobjMetadataBackend> {
    let shard_id = DfsMetadataShardId::new(config.shard_id.clone());
    if let Some(layout) = selected_layout {
        if open_mode == LocalMetadataShardOpenMode::Inspection {
            return LocalSobjMetadataBackend::open_for_inspection_with_layout(
                shard_id, root.to_path_buf(), Some((config.file_system_id.clone(), layout)),
            );
        }
        // Representation failure is non-repairable. Actual normal replica
        // corruption keeps its existing repair-only fallback below.
        match LocalSobjMetadataBackend::new_with_selected_layout(
            config.file_system_id.clone(), shard_id.clone(), root.to_path_buf(), Arc::clone(&layout),
        ) {
            Ok(backend) => return Ok(backend),
            Err(error) if open_mode == LocalMetadataShardOpenMode::ReplicaRepair
                && crate::metadata_replication::metadata_backend_open_error_allows_snapshot_repair(&error) => {
                return LocalSobjMetadataBackend::open_for_replica_repair_with_layout(
                    shard_id, root.to_path_buf(), Some((config.file_system_id.clone(), layout)),
                );
            }
            Err(error) => return Err(error),
        }
    }
    let mut backend = match open_mode {
        LocalMetadataShardOpenMode::Strict => {
            LocalSobjMetadataBackend::new(shard_id, root.to_path_buf())?
        }
        LocalMetadataShardOpenMode::ReplicaRepair => {
            match LocalSobjMetadataBackend::new(shard_id.clone(), root.to_path_buf()) {
                Ok(backend) => backend,
                Err(open_error)
                    if !crate::metadata_replication::metadata_backend_open_error_allows_snapshot_repair(
                        &open_error,
                    ) =>
                {
                    return Err(open_error);
                }
                Err(open_error) => LocalSobjMetadataBackend::open_for_replica_repair(
                    shard_id,
                    root.to_path_buf(),
                )
                .map_err(|repair_error| {
                    MetadataBackendError::Serde(format!(
                        "cannot open replica metadata shard {}: {open_error}; repair-only open failed: {repair_error}",
                        config.shard_id
                    ))
                })?,
            }
        }
        LocalMetadataShardOpenMode::Inspection => {
            LocalSobjMetadataBackend::open_for_inspection(shard_id, root.to_path_buf())?
        }
    };
    Ok(backend)
}

#[derive(Debug)]
pub struct LocalShardedMetadataBackend {
    root: PathBuf,
    local_node_id: Option<NodeId>,
    shard_configs: BTreeMap<String, DfsMetadataShardConfig>,
    routing_indexes: BTreeMap<DfsFileSystemId, DfsMetadataShardRoutingIndex>,
    fallback_routing_indexes: Vec<BTreeMap<DfsFileSystemId, DfsMetadataShardRoutingIndex>>,
    shards: BTreeMap<DfsMetadataShardId, LocalMetadataShardSlot>,
}

impl LocalShardedMetadataBackend {
    /// Open or initialize a local router over the configured metadata shards.
    ///
    /// Each shard config gets its own `LocalSobjMetadataBackend` under `root`.
    /// Cross-shard object placement is tracked in this backend's shard-index
    /// directory, while shard-local tables and journals stay inside each local
    /// shard backend.
    pub fn new(root: impl Into<PathBuf>, shards: Vec<DfsMetadataShardConfig>) -> Result<Self> {
        let root = root.into();
        Self::open_with(root, shards, true, None, true)
    }

    /// Open configured shards while allowing unreadable non-primary mirrors
    /// to enter the snapshot-repair handshake. A shard primary always retains
    /// strict open/replay semantics.
    pub fn new_with_replica_repair(
        root: impl Into<PathBuf>,
        shards: Vec<DfsMetadataShardConfig>,
        local_node_id: &NodeId,
    ) -> Result<Self> {
        let root = root.into();
        Self::open_with(root, shards, true, Some(local_node_id), true)
    }

    pub(crate) fn new_with_replica_repair_and_layout(
        root: PathBuf,
        shards: Vec<DfsMetadataShardConfig>,
        local_node_id: &NodeId,
        layout: Arc<crate::DfsSelectedOriginalDataLayout>,
    ) -> Result<Self> {
        Self::open_with_retained_shards_and_layout(
            root,
            shards,
            true,
            Some(local_node_id),
            &BTreeSet::new(),
            Some(layout),
            true,
        )
    }

    /// Open a sharded backend for read-only inspection/status.
    ///
    /// Shard-local backends use their inspection open mode, so this path avoids
    /// recovery writes while still reading the durable materialized tables.
    pub fn open_for_inspection(
        root: impl Into<PathBuf>,
        shards: Vec<DfsMetadataShardConfig>,
    ) -> Result<Self> {
        let root = root.into();
        Self::open_with(root, shards, false, None, true)
    }

    /// Open read-only shard state while retaining the local node identity used
    /// to project primary-only maintenance work. This does not grant write
    /// authority; shard-local backends remain in inspection mode.
    pub fn open_for_inspection_at_node(
        root: impl Into<PathBuf>,
        shards: Vec<DfsMetadataShardConfig>,
        local_node_id: &NodeId,
    ) -> Result<Self> {
        let root = root.into();
        Self::open_with(root, shards, false, Some(local_node_id), true)
    }

    /// Inspect the exact owner-selected original layout without initializing or
    /// repairing it. The existing layout and per-shard checks still fence reads.
    #[cfg(test)]
    pub(crate) fn open_for_inspection_at_node_with_layout(
        root: PathBuf,
        shards: Vec<DfsMetadataShardConfig>,
        local_node_id: &NodeId,
        layout: Arc<crate::DfsSelectedOriginalDataLayout>,
    ) -> Result<Self> {
        Self::open_with_retained_shards_and_layout(
            root,
            shards,
            false,
            Some(local_node_id),
            &BTreeSet::new(),
            Some(layout),
            true,
        )
    }

    /// Move this router's exact shard backend instances into a resident pool.
    ///
    /// Already-open shards are moved without decoding or recovery. Deferred
    /// replica-repair slots remain lazy and are opened once by their first
    /// scoped handle. The consumed router cannot subsequently create a second
    /// process-local authority capability for the same pool generation.
    pub fn into_resident(self) -> ResidentLocalShardedMetadataBackend {
        let Self {
            root,
            local_node_id,
            shard_configs,
            routing_indexes,
            fallback_routing_indexes,
            shards,
        } = self;
        let shards = shards
            .into_iter()
            .map(|(shard_id, slot)| {
                (
                    shard_id,
                    Arc::new(ResidentLocalMetadataShardSlot::from_local_slot(slot)),
                )
            })
            .collect();
        ResidentLocalShardedMetadataBackend {
            inner: Arc::new(ResidentLocalShardedMetadataBackendInner {
                router: Mutex::new(ResidentLocalShardedMetadataRouterState {
                    root,
                    local_node_id,
                    shard_configs,
                    routing_indexes,
                    fallback_routing_indexes,
                }),
                shards,
            }),
        }
    }

    fn open_with(
        root: PathBuf,
        shards: Vec<DfsMetadataShardConfig>,
        initialize_layout: bool,
        replica_repair_local_node_id: Option<&NodeId>,
        eager_open: bool,
    ) -> Result<Self> {
        Self::open_with_retained_shards(
            root,
            shards,
            initialize_layout,
            replica_repair_local_node_id,
            &BTreeSet::new(),
            eager_open,
        )
    }

    fn open_with_retained_shards(
        root: PathBuf,
        shards: Vec<DfsMetadataShardConfig>,
        initialize_layout: bool,
        replica_repair_local_node_id: Option<&NodeId>,
        retained_ids: &BTreeSet<DfsMetadataShardId>,
        eager_open: bool,
    ) -> Result<Self> {
        Self::open_with_retained_shards_and_layout(
            root,
            shards,
            initialize_layout,
            replica_repair_local_node_id,
            retained_ids,
            None,
            eager_open,
        )
    }

    fn open_with_retained_shards_and_layout(
        root: PathBuf,
        mut shards: Vec<DfsMetadataShardConfig>,
        initialize_layout: bool,
        replica_repair_local_node_id: Option<&NodeId>,
        retained_ids: &BTreeSet<DfsMetadataShardId>,
        selected_layout: Option<Arc<crate::DfsSelectedOriginalDataLayout>>,
        eager_open: bool,
    ) -> Result<Self> {
        if let Some(layout) = &selected_layout {
            layout.check_routing_root(&root)?;
        } else {
            crate::repair_queue_store::SelectedOriginalFixtureIdentity::reject_default_root(&root)
                .map_err(|error| MetadataBackendError::Rejected {
                    code: crate::DfsMetadataRejectionCode::Other, message: error.to_string(),
                })?;
        }
        if shards.is_empty() {
            return Err(MetadataBackendError::Serde(
                "local sharded metadata backend requires at least one shard".to_string(),
            ));
        }
        if initialize_layout {
            crate::storage_preparation::prepare_storage_root_before_open(
                &root,
                &crate::DfsStoragePreparationScope::MetadataRouting,
            )?;
            if let Some(layout) = &selected_layout { layout.check_routing_root(&root)?; }
            create_dir_all(root.join("shard-index").join("objects"))?;
        }
        if let Some(layout) = &selected_layout { layout.check_routing_root(&root)?; }
        canonicalize_routing_topology(&mut shards);
        let routing_history = load_routing_topology_history(&root, &shards, initialize_layout)?;
        if let Some(layout) = &selected_layout { layout.check_routing_root(&root)?; }
        let mut shard_configs = BTreeMap::new();
        let mut shard_slots = Vec::with_capacity(shards.len());
        for shard in shards {
            let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
            if shard_configs
                .insert(shard.shard_id.clone(), shard.clone())
                .is_some()
            {
                return Err(MetadataBackendError::Serde(format!(
                    "duplicate DFS metadata shard config: {}",
                    shard.shard_id
                )));
            }
            let shard_root = root.join(key_segment(&shard_id.0));
            let open_mode = if !initialize_layout {
                LocalMetadataShardOpenMode::Inspection
            } else if replica_repair_local_node_id
                .is_some_and(|local_node_id| shard.primary_node_id != *local_node_id)
            {
                LocalMetadataShardOpenMode::ReplicaRepair
            } else {
                LocalMetadataShardOpenMode::Strict
            };
            if let Some(layout) = &selected_layout {
                #[cfg(any(test, feature = "test-support"))]
                if initialize_layout { layout.admit_stage(&shard.file_system_id, &shard_id, &shard_root)?; }
                layout.stage(&shard.file_system_id, &shard_id, &shard_root)?;
            }
            let mut slot = LocalMetadataShardSlot::new(shard, shard_root, open_mode);
            slot.selected_original_data_layout = selected_layout.clone();
            shard_slots.push((shard_id, slot));
        }
        let replica_repair_local_node_id = replica_repair_local_node_id.cloned();
        let (eager_slots, deferred_slots): (Vec<_>, Vec<_>) =
            shard_slots.into_iter().partition(|(shard_id, slot)| {
                eager_open && slot.should_open_eagerly() && !retained_ids.contains(shard_id)
            });
        #[cfg(test)]
        let eager_open_count = eager_slots.len();
        let mut opened = std::thread::scope(|scope| {
            let handles = eager_slots
                .into_iter()
                .map(|(shard_id, slot)| {
                    scope.spawn(sinomo_core::service::observed_blocking_task(
                        "dfs.metadata.open",
                        move || {
                            let backend = slot.open_backend()?;
                            slot.backend.set(backend).expect(
                                "new metadata shard slot accepts eager backend initialization",
                            );
                            Ok((shard_id, slot))
                        },
                    ))
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .map(|handle| {
                    handle.join().map_err(|_| {
                        MetadataBackendError::Serde(
                            "local DFS metadata shard open worker panicked".to_string(),
                        )
                    })?
                })
                .collect::<Result<Vec<_>>>()
        })?;
        opened.extend(deferred_slots);
        #[cfg(test)]
        crate::metadata_backend::record_metadata_backend_open_calls_from_workers(eager_open_count);
        let backends = opened.into_iter().collect::<BTreeMap<_, _>>();
        let routing_indexes = compile_routing_indexes(shard_configs.values().cloned())?;
        let fallback_routing_indexes = routing_history
            .fallbacks
            .iter()
            .map(|topology| compile_routing_indexes(topology.iter().cloned()))
            .collect::<Result<Vec<_>>>()?;
        if let Some(layout) = &selected_layout {
            layout.check_routing_root(&root)?;
            for (shard_id, config) in &shard_configs {
                layout.stage(&config.file_system_id, &DfsMetadataShardId::new(shard_id), &root.join(key_segment(shard_id)))?;
            }
        }

        Ok(Self {
            root,
            local_node_id: replica_repair_local_node_id,
            shard_configs,
            routing_indexes,
            fallback_routing_indexes,
            shards: backends,
        })
    }

    pub(crate) fn shard_ids(&self) -> Vec<DfsMetadataShardId> {
        self.shards.keys().cloned().collect()
    }

    /// Refresh and pin every inspection-only shard without taking writer locks
    /// or running authoritative recovery.
    pub(crate) fn begin_inspection_snapshot(&mut self) -> Result<()> {
        let shard_ids = self.shards.keys().cloned().collect::<Vec<_>>();
        for (index, shard_id) in shard_ids.iter().enumerate() {
            let begin = self
                .shards
                .get_mut(shard_id)
                .expect("collected shard id remains installed")
                .backend_mut()
                .and_then(LocalSobjMetadataBackend::begin_inspection_snapshot);
            if let Err(error) = begin {
                for pinned_shard_id in &shard_ids[..index] {
                    self.shards
                        .get_mut(pinned_shard_id)
                        .expect("pinned inspection shard remains installed")
                        .opened_backend_mut()
                        .expect("pinned inspection shard was opened")
                        .end_inspection_snapshot();
                }
                return Err(error);
            }
        }
        Ok(())
    }

    pub(crate) fn end_inspection_snapshot(&mut self) {
        for slot in self.shards.values_mut() {
            if let Some(backend) = slot.opened_backend_mut() {
                backend.end_inspection_snapshot();
            }
        }
    }

    fn shard_is_locally_replicated(&self, shard_id: &DfsMetadataShardId) -> bool {
        self.local_node_id.as_ref().is_none_or(|local_node_id| {
            self.shard_configs.get(&shard_id.0).is_some_and(|config| {
                config.primary_node_id == *local_node_id
                    || config.replica_node_ids.contains(local_node_id)
            })
        })
    }

    fn shard_is_local_authoritative_primary(&self, shard_id: &DfsMetadataShardId) -> bool {
        self.local_node_id.as_ref().is_none_or(|local_node_id| {
            self.shard_configs
                .get(&shard_id.0)
                .is_some_and(|config| config.primary_node_id == *local_node_id)
        })
    }

    pub(crate) fn read_object_shard_index(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<Option<DfsObjectShardIndexRecord>> {
        read_object_shard_index(&self.root, object_id)
    }

    pub(crate) fn write_object_shard_index(
        &self,
        object: &DfsObjectMetadata,
        shard_id: &DfsMetadataShardId,
    ) -> Result<()> {
        write_object_shard_index(&self.root, object, shard_id)
    }

    fn remove_object_shard_index(&self, object_id: &DfsObjectId) -> Result<()> {
        remove_object_shard_index(&self.root, object_id)
    }

    pub(crate) fn shard_backend(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<&LocalSobjMetadataBackend> {
        self.shards
            .get(shard_id)
            .ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "unknown local DFS metadata shard: {}",
                    shard_id.0
                ))
            })?
            .backend()
    }

    pub(crate) fn shard_backend_mut(
        &mut self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<&mut LocalSobjMetadataBackend> {
        self.shards
            .get_mut(shard_id)
            .ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "unknown local DFS metadata shard: {}",
                    shard_id.0
                ))
            })?
            .backend_mut()
    }

    #[cfg(test)]
    fn opened_shard_ids(&self) -> BTreeSet<DfsMetadataShardId> {
        self.shards
            .iter()
            .filter_map(|(shard_id, slot)| slot.backend.get().map(|_| shard_id.clone()))
            .collect()
    }

    fn require_replica_ingest_permit_for_shard(
        &self,
        permit: &DfsMetadataReplicaIngestPermit,
        shard_id: &DfsMetadataShardId,
    ) -> Result<()> {
        let shard = self.shard_configs.get(&shard_id.0).ok_or_else(|| {
            MetadataBackendError::Serde(format!(
                "unknown configured DFS metadata shard: {}",
                shard_id.0
            ))
        })?;
        if permit.file_system_id() != &shard.file_system_id || permit.shard_id() != shard_id {
            return Err(MetadataBackendError::Serde(format!(
                "metadata replica ingest permit for filesystem {} shard {} does not match configured filesystem {} shard {}",
                permit.file_system_id(),
                permit.shard_id().0,
                shard.file_system_id,
                shard.shard_id,
            )));
        }
        Ok(())
    }

    fn select_shard_id_for_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<DfsMetadataShardId> {
        let selected = self
            .routing_indexes
            .get(file_system_id)
            .ok_or_else(|| MetadataBackendError::UnknownFileSystem {
                file_system_id: file_system_id.clone(),
            })?
            .select_shard_id(path, false)
            .map_err(|error| MetadataBackendError::Serde(error.to_string()))?;
        selected.map(DfsMetadataShardId::new).ok_or_else(|| {
            MetadataBackendError::UnknownFileSystem {
                file_system_id: file_system_id.clone(),
            }
        })
    }

    fn fallback_shard_ids_for_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
        selected_shard_id: &DfsMetadataShardId,
    ) -> Result<Vec<DfsMetadataShardId>> {
        let mut shard_ids = Vec::new();
        let mut seen = BTreeSet::new();
        seen.insert(selected_shard_id.clone());
        for routing_indexes in &self.fallback_routing_indexes {
            let Some(index) = routing_indexes.get(file_system_id) else {
                continue;
            };
            let Some(shard_id) = index
                .select_shard_id(path, false)
                .map_err(|error| MetadataBackendError::Serde(error.to_string()))?
            else {
                continue;
            };
            let shard_id = DfsMetadataShardId::new(shard_id);
            if self.shards.contains_key(&shard_id) && seen.insert(shard_id.clone()) {
                shard_ids.push(shard_id);
            }
        }
        Ok(shard_ids)
    }

    fn consistent_read_shard_ids_for_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Vec<DfsMetadataShardId>> {
        let selected = self.select_shard_id_for_path(file_system_id, path)?;
        let mut shard_ids = vec![selected.clone()];
        shard_ids.extend(self.fallback_shard_ids_for_path(file_system_id, path, &selected)?);
        if let Some(missing) = shard_ids
            .iter()
            .find(|shard_id| !self.shards.contains_key(*shard_id))
        {
            return Err(MetadataBackendError::Serde(format!(
                "path {path} routed to unavailable local metadata shard {missing:?}"
            )));
        }
        Ok(shard_ids)
    }

    fn begin_consistent_read_snapshot_for_shards(
        &mut self,
        shard_ids: &[DfsMetadataShardId],
    ) -> Result<()> {
        #[cfg(test)]
        SHARDED_CONSISTENT_READ_SNAPSHOT_SHARDS
            .with(|observed| observed.borrow_mut().extend_from_slice(shard_ids));
        for (index, shard_id) in shard_ids.iter().enumerate() {
            let begin = self
                .shards
                .get_mut(shard_id)
                .expect("routed consistent-read shard remains installed")
                .backend_mut()
                .and_then(LocalSobjMetadataBackend::begin_consistent_read_snapshot);
            if let Err(error) = begin {
                for pinned_shard_id in &shard_ids[..index] {
                    self.shards
                        .get_mut(pinned_shard_id)
                        .expect("pinned consistent-read shard remains installed")
                        .opened_backend_mut()
                        .expect("pinned consistent-read shard was opened")
                        .end_consistent_read_snapshot();
                }
                return Err(error);
            }
        }
        Ok(())
    }

    fn end_consistent_read_snapshot_for_shards(&mut self, shard_ids: &[DfsMetadataShardId]) {
        for shard_id in shard_ids {
            self.shards
                .get_mut(shard_id)
                .expect("routed consistent-read shard remains installed")
                .opened_backend_mut()
                .expect("routed consistent-read shard was opened")
                .end_consistent_read_snapshot();
        }
    }

    fn shard_id_for_object(&self, object_id: &DfsObjectId) -> Result<Option<DfsMetadataShardId>> {
        if let Some(index) = self.read_object_shard_index(object_id)?
            && self.object_shard_index_is_corroborated(object_id, &index)?
        {
            return Ok(Some(index.shard_id));
        }
        // The object-to-shard record is a rebuildable accelerator. A replica
        // may have no record or may retain the source shard after ingesting a
        // cross-shard move. Fall back to physical/routing authority instead of
        // allowing that derived state to select the source eviction tombstone.
        Ok(self
            .authoritative_object_placement(object_id)?
            .map(|(shard_id, _)| shard_id))
    }

    fn object_shard_index_is_corroborated(
        &self,
        object_id: &DfsObjectId,
        index: &DfsObjectShardIndexRecord,
    ) -> Result<bool> {
        if !self.shards.contains_key(&index.shard_id) {
            return Ok(false);
        }
        let Some(object) = self.shard_backend(&index.shard_id)?.get_object(object_id)? else {
            return Ok(false);
        };
        if is_source_eviction_tombstone(&object) {
            return Ok(false);
        }
        Ok(object.file_system_id == index.file_system_id
            && normalize_path(&object.path) == index.path
            && self
                .select_shard_id_for_path(&object.file_system_id, &object.path)
                .is_ok_and(|selected| selected == index.shard_id))
    }

    /// Resolve the one shard whose receipt records may participate in restart
    /// recovery for an object/version pair.
    ///
    /// Visible objects use routed/generation-aware physical authority. A
    /// pending create has no visible object yet, so it uses the unique shard
    /// that carries its exact active intent, independently of any routing hint.
    /// Ambiguous physical copies fail closed rather than allowing an arbitrary shard's receipts to
    /// satisfy write quorum.
    fn shard_id_for_receipt_scan_record(
        &self,
        receipt: &DfsReplicaReceiptRecord,
    ) -> Result<Option<DfsMetadataShardId>> {
        let object_id = &receipt.receipt.object_id;
        let version = &receipt.receipt.committed_version;
        // A hint alone cannot select the receipt owner of an invisible create.
        // Its unsynced publication may be absent, torn or stale after restart.
        // Visible ownership comes from canonical physical records; otherwise
        // require the unique exact active-intent proof below.
        if let Some((shard_id, _)) = self.authoritative_object_placement(object_id)? {
            return Ok(Some(shard_id));
        }

        let mut active_intent_shard_id = None;
        for (shard_id, slot) in &self.shards {
            let backend = slot.backend()?;
            let mut has_active_intent = false;
            walk_metadata_pages(|cursor| {
                let page = backend.intents_for_object_version_page(
                    object_id,
                    version,
                    cursor,
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?;
                has_active_intent = page.intents.iter().any(|intent| {
                    write_intent_status_is_active(&intent.status)
                        && intent.file_system_id == receipt.receipt.file_system_id
                        && intent.object_id == *object_id
                        && intent.target_version == *version
                        && intent.content_hash == receipt.content_hash
                        && intent.object_metadata.as_ref().is_some_and(|object| {
                            object.object_id == intent.object_id
                                && object.file_system_id == intent.file_system_id
                                && self
                                    .select_shard_id_for_path(&object.file_system_id, &object.path)
                                    .is_ok_and(|selected| selected == *shard_id)
                        })
                });
                if has_active_intent {
                    Ok(None)
                } else {
                    Ok(page.next_cursor)
                }
            })?;
            if !has_active_intent {
                continue;
            }
            if active_intent_shard_id.is_some() {
                return Ok(None);
            }
            active_intent_shard_id = Some(shard_id.clone());
        }
        Ok(active_intent_shard_id)
    }

    /// Resolve the newest physical record for one object without trusting the
    /// derived shard index. Journal batches for different shards can arrive in
    /// either order, and a cross-shard move deliberately leaves a same-version
    /// eviction tombstone behind. The moved live record wins that exact tie;
    /// otherwise later generations win deterministically.
    fn authoritative_object_placement(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<Option<(DfsMetadataShardId, DfsObjectMetadata)>> {
        let index = self.read_object_shard_index(object_id)?;
        self.authoritative_object_placement_with_index(object_id, index.as_ref())
    }

    fn authoritative_object_placement_with_index(
        &self,
        object_id: &DfsObjectId,
        index: Option<&DfsObjectShardIndexRecord>,
    ) -> Result<Option<(DfsMetadataShardId, DfsObjectMetadata)>> {
        let mut candidates = Vec::new();
        for (shard_id, slot) in &self.shards {
            let backend = slot.backend()?;
            let Some(object) = backend.get_object(object_id)? else {
                continue;
            };
            // A source-eviction marker is physical cleanup evidence, never a
            // namespace deletion. This remains true if a later topology
            // generation routes the path back through the old source shard.
            if is_source_eviction_tombstone(&object) {
                continue;
            }
            let routed_here = self
                .select_shard_id_for_path(&object.file_system_id, &object.path)
                .is_ok_and(|selected| selected == *shard_id);
            candidates.push((shard_id.clone(), object, routed_here));
        }
        let require_routed_candidate = candidates.iter().any(|(_, _, routed_here)| *routed_here);
        let mut selected: Option<(DfsMetadataShardId, DfsObjectMetadata)> = None;
        for (shard_id, object, routed_here) in candidates {
            if require_routed_candidate && !routed_here {
                continue;
            }
            let replace = selected
                .as_ref()
                .is_none_or(|(selected_shard, selected_object)| {
                    let generation = (object.changed_at_unix_nanos, &object.version);
                    let selected_generation = (
                        selected_object.changed_at_unix_nanos,
                        &selected_object.version,
                    );
                    generation > selected_generation
                        || (generation == selected_generation
                            && object.kind != DfsObjectKind::Tombstone
                            && selected_object.kind == DfsObjectKind::Tombstone)
                        || (generation == selected_generation
                            && (object.kind == DfsObjectKind::Tombstone)
                                == (selected_object.kind == DfsObjectKind::Tombstone)
                            && index.is_some_and(|index| {
                                index.shard_id == shard_id && index.shard_id != *selected_shard
                            }))
                });
            if replace {
                selected = Some((shard_id, object));
            }
        }
        Ok(selected)
    }

    fn reconcile_object_shard_indexes<'a>(
        &mut self,
        object_ids: impl IntoIterator<Item = &'a DfsObjectId>,
    ) -> Result<()> {
        self.begin_consistent_read_snapshot()?;
        let result = (|| {
            let mut new_indexes = Vec::new();
            for object_id in object_ids {
                let (existing, rebuild_existing) =
                    read_object_shard_index_for_rebuild(&self.root, object_id)?;
                let Some((shard_id, object)) =
                    self.authoritative_object_placement_with_index(object_id, existing.as_ref())?
                else {
                    if existing.is_some() || rebuild_existing {
                        self.remove_object_shard_index(object_id)?;
                    }
                    continue;
                };
                if existing.as_ref().is_some_and(|index| {
                    index.file_system_id == object.file_system_id
                        && index.path == normalize_path(&object.path)
                        && index.shard_id == shard_id
                }) {
                    continue;
                }
                if existing.is_none() && !rebuild_existing {
                    new_indexes.push((object, shard_id));
                } else {
                    self.write_object_shard_index(&object, &shard_id)?;
                }
            }
            write_new_object_shard_indexes(
                &self.root,
                new_indexes
                    .iter()
                    .map(|(object, shard_id)| (object, shard_id)),
            )
        })();
        self.end_consistent_read_snapshot();
        result
    }

    fn refresh_metadata_shard_durable_view_and_reconcile(
        &mut self,
        shard_id: &DfsMetadataShardId,
        reconcile_unchanged: bool,
    ) -> Result<bool> {
        let generation_before = self.shard_backend(shard_id)?.cached_table_generation();
        let mut affected_object_ids = self
            .shard_backend(shard_id)?
            .cached_object_ids()
            .cloned()
            .collect::<BTreeSet<_>>();
        self.shard_backend_mut(shard_id)?
            .refresh_metadata_shard_durable_view(shard_id)?;
        let changed = self.shard_backend(shard_id)?.cached_table_generation() != generation_before;
        if !changed && !reconcile_unchanged {
            return Ok(false);
        }
        affected_object_ids.extend(self.shard_backend(shard_id)?.cached_object_ids().cloned());
        self.reconcile_object_shard_indexes(affected_object_ids.iter())?;
        Ok(changed)
    }

    fn object_is_visible_from_shard(
        &self,
        object: &DfsObjectMetadata,
        shard_id: &DfsMetadataShardId,
    ) -> Result<bool> {
        if is_source_eviction_tombstone(object) {
            return Ok(false);
        }
        let selected_shard_id =
            self.select_shard_id_for_path(&object.file_system_id, &object.path)?;
        if selected_shard_id == *shard_id {
            if object.kind == DfsObjectKind::Tombstone {
                return Ok(true);
            }
            // A failed source cleanup can leave the old path's live row behind
            // after a move to a different routed path. Hide that residue only
            // when the derived index is corroborated by the complete physical
            // destination record and current routing. A stale replica index
            // that points at a retired shard cannot veto this selected row.
            if let Some(index) = self.read_object_shard_index(&object.object_id)?
                && index.shard_id != *shard_id
                && self.object_shard_index_is_corroborated(&object.object_id, &index)?
            {
                return Ok(false);
            }
            return Ok(true);
        }

        // A current path-selected record is the namespace authority. A live
        // destination therefore hides the historical source record even when
        // the replica-local object index is stale or absent. If a newly
        // activated target has not received the object yet, preserve the old
        // source fallback until migration installs the routed copy.
        if self
            .shard_backend(&selected_shard_id)?
            .get_object_by_path(&object.file_system_id, &object.path)?
            .is_some_and(|selected| !is_source_eviction_tombstone(&selected))
        {
            return Ok(false);
        }
        Ok(self
            .authoritative_object_placement(&object.object_id)?
            .is_some_and(|(authoritative_shard_id, _)| authoritative_shard_id == *shard_id))
    }

    fn object_is_listable_from_shard(
        &self,
        object: &DfsObjectMetadata,
        shard_id: &DfsMetadataShardId,
    ) -> Result<bool> {
        self.object_is_visible_from_shard(object, shard_id)
    }

    /// One shard's objects under `subtree_path`, without rows another shard
    /// now owns. Cursors stay local to the shard.
    fn subtree_object_page_on_shard(
        &self,
        query: &crate::DfsMetadataShardQuery,
        subtree_path: &str,
        after: Option<&DfsObjectPageCursor>,
        limit: usize,
    ) -> Result<crate::DfsMetadataQueryResult> {
        let subtree_path = crate::posix_path::normalize_posix_path(subtree_path)
            .map_err(|error| MetadataBackendError::Serde(error.to_string()))?;
        let limit = limit.min(crate::file_history_query::DFS_SUBTREE_OBJECT_PAGE_LIMIT);
        let backend = self.shard_backend(&query.shard_id)?;
        let mut objects = Vec::new();
        let mut cursor = after.cloned();
        while objects.len() < limit {
            let page = backend.objects_for_subtree_page(
                &query.file_system_id,
                &subtree_path,
                cursor.as_ref(),
                limit - objects.len(),
            )?;
            for object in page.objects {
                cursor = Some(DfsObjectPageCursor {
                    path: object.path.clone(),
                    object_id: object.object_id.clone(),
                    shard_id: None,
                });
                if self.object_is_listable_from_shard(&object, &query.shard_id)? {
                    objects.push(object);
                }
            }
            if page.next_cursor.is_none() {
                return Ok(crate::DfsMetadataQueryResult::SubtreeObjectPageV1 {
                    objects,
                    next_cursor: None,
                });
            }
        }
        Ok(crate::DfsMetadataQueryResult::SubtreeObjectPageV1 {
            objects,
            next_cursor: cursor,
        })
    }

    fn intent_is_listable_from_shard(
        &self,
        intent: &DfsWriteIntentRecord,
        shard_id: &DfsMetadataShardId,
    ) -> Result<bool> {
        if write_intent_status_is_active(&intent.status) {
            return self.active_intent_is_listable_from_shard(intent, shard_id);
        }
        if let Some((owner_shard_id, _)) = self.authoritative_object_placement(&intent.object_id)? {
            return Ok(owner_shard_id == *shard_id);
        }
        // Hidden aborted creates and retained history can outlive their object.
        // A missing or misleading hint must not erase that canonical history.
        // Terminal records cannot supply the active-intent receipt proof.
        Ok(true)
    }

    fn active_intent_is_listable_from_shard(
        &self,
        intent: &DfsWriteIntentRecord,
        shard_id: &DfsMetadataShardId,
    ) -> Result<bool> {
        let Some((owner_shard_id, current_object)) =
            self.authoritative_object_placement(&intent.object_id)?
        else {
            return Ok(true);
        };
        if owner_shard_id == *shard_id {
            return Ok(true);
        }
        if current_object.kind == DfsObjectKind::Tombstone {
            return Ok(true);
        }
        Ok(intent
            .base_version
            .as_ref()
            .is_some_and(|base_version| *base_version == current_object.version)
            || intent.target_version == current_object.version)
    }

    fn require_shard_id_for_object(&self, object_id: &DfsObjectId) -> Result<DfsMetadataShardId> {
        self.shard_id_for_object(object_id)?
            .ok_or_else(|| MetadataBackendError::UnknownObject {
                object_id: object_id.clone(),
            })
    }

    fn first_hardlink_to_object_matching<P>(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
        mut matches: P,
    ) -> Result<Option<DfsObjectMetadata>>
    where
        P: FnMut(&DfsObjectMetadata) -> bool,
    {
        let mut best = None::<DfsObjectMetadata>;
        for (shard_id, slot) in &self.shards {
            let backend = slot.backend()?;
            walk_metadata_pages(|cursor| {
                let page = backend.hardlinks_to_object_page(
                    file_system_id,
                    target_object_id,
                    cursor,
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?;
                for object in page.objects {
                    if !self.object_is_listable_from_shard(&object, shard_id)? || !matches(&object)
                    {
                        continue;
                    }
                    if best
                        .as_ref()
                        .is_none_or(|current| object_page_key(&object) < object_page_key(current))
                    {
                        best = Some(object);
                    }
                }
                Ok(page.next_cursor)
            })?;
        }
        Ok(best)
    }

    fn object_shard_migration_bundle(
        &mut self,
        source_shard_id: &DfsMetadataShardId,
        object_id: &DfsObjectId,
    ) -> Result<(MetadataWriteLock, DfsObjectShardMigrationBundle)> {
        self.shard_backend_mut(source_shard_id)?
            .lock_and_capture_object_shard_migration_bundle(
                object_id,
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )
    }
}

#[cfg(test)]
mod lazy_replica_slot_tests {
    use super::*;

    use sinomo_core::DfsObjectPolicy;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    fn node(value: &str) -> NodeId {
        NodeId::from_legacy_named(value).expect("valid node id")
    }

    fn configs(local_node_id: &NodeId) -> Vec<DfsMetadataShardConfig> {
        vec![
            DfsMetadataShardConfig {
                file_system_id: crate::sifs_file_system_id(),
                shard_id: "root".to_string(),
                primary_node_id: local_node_id.clone(),
                replica_node_ids: Vec::new(),
                epoch: 1,
                assignment: DfsMetadataShardAssignment::Root,
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            },
            DfsMetadataShardConfig {
                file_system_id: crate::sifs_file_system_id(),
                shard_id: "services".to_string(),
                primary_node_id: node("lazy-slot-remote-primary"),
                replica_node_ids: vec![local_node_id.clone()],
                epoch: 1,
                assignment: DfsMetadataShardAssignment::PathPrefix {
                    prefix: crate::SIFS_SERVICES_PATH.to_string(),
                },
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            },
        ]
    }

    #[test]
    fn native_resident_opening_error_and_unwind_release_waiter_into_actual_backend() {
        let name = "native-resident-opening-negative";
        let nodes = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 3);
        let mut runtime = crate::test_runtime::DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
        for node in &nodes[1..] {
            runtime.define_node(node.clone()).unwrap();
        }
        runtime.start_nodes().unwrap();
        let cluster_paths = sinomo_core::network::ClusterPaths::new(runtime.data_dir(&nodes[0]));
        // This existing recovery-only projection supplies actual current topology;
        // opening a storage slot does not grant metadata publication authority.
        let shards =
            crate::cluster_dfs_metadata_shards(&cluster_paths, &crate::sifs_file_system_id())
                .unwrap();
        let deadline = Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        for (unwind, node) in [false, true].into_iter().zip(&nodes[1..]) {
            let data_dir = runtime.data_dir(node).clone();
            // Use each actual replica's normal storage opener before status
            // hydration. The daemon's status resident has already opened healthy
            // mirrors, and a recovered slot must never be reset to Unopened.
            let resident = crate::open_local_sharded_metadata_backend_for_replica_repair(
                crate::default_dfs_metadata_shards_root(&data_dir),
                shards.clone(),
                node,
            )
            .unwrap()
            .into_resident();
            // Borrow an actual deferred non-primary slot from the normal owner;
            // no slot state, metadata tables, role or admission is fabricated.
            let (id, slot) = resident
                .inner
                .shards
                .iter()
                .find(|(_, slot)| {
                    slot.open_mode == LocalMetadataShardOpenMode::ReplicaRepair
                        && matches!(
                            *slot.state.lock().unwrap(),
                            ResidentLocalMetadataShardState::Unopened
                        )
                })
                .map(|(id, slot)| (id.clone(), Arc::clone(slot)))
                .expect("normal actual replica storage starts deferred before status hydration");
            assert_ne!(slot.config.primary_node_id, *node);
            assert!(slot.config.replica_node_ids.contains(node));
            assert!(slot.root.starts_with(data_dir.dfs_dir()));
            let handle = resident
                .scope_shard(&slot.config.file_system_id, &id)
                .unwrap();
            assert!(Arc::ptr_eq(&handle.slot, &slot));
            std::thread::scope(|threads| {
                let (entered_tx, entered_rx) = mpsc::channel();
                let (release_tx, release_rx) = mpsc::channel();
                let opening_slot = Arc::clone(&slot);
                let opener = threads.spawn(move || {
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        opening_slot.checkout_with_open(&mut || {
                            entered_tx.send(()).unwrap();
                            release_rx
                                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                                .unwrap();
                            if unwind {
                                panic!("negative actual resident initializer unwind");
                            }
                            Err(MetadataBackendError::Serde(
                                "negative actual resident initializer error".into(),
                            ))
                        })
                    }))
                });
                entered_rx
                    .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                    .unwrap();
                assert!(matches!(
                    *slot.state.lock().unwrap(),
                    ResidentLocalMetadataShardState::Opening
                ));
                let (waiting_tx, waiting_rx) = mpsc::channel();
                let (available_tx, available_rx) = mpsc::channel();
                let (checkout_tx, checkout_rx) = mpsc::channel();
                let waiter_slot = Arc::clone(&slot);
                let waiter = threads.spawn(move || {
                    waiting_tx.send(()).unwrap();
                    crate::test_runtime::executor().block_on(async {
                        tokio::time::timeout_at(
                            tokio::time::Instant::from_std(deadline),
                            waiter_slot.wait_until_checkout_available(),
                        )
                        .await
                        .unwrap()
                        .unwrap();
                    });
                    assert!(matches!(
                        *waiter_slot.state.lock().unwrap(),
                        ResidentLocalMetadataShardState::Unopened
                    ));
                    available_tx.send(()).unwrap();
                    checkout_rx
                        .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                        .unwrap();
                    // Recover through the SAME normal production checkout/open,
                    // which reads the actual native physical replica source.
                    let lease = handle.checkout().unwrap();
                    let proof =
                        crate::DfsMetadataBackend::metadata_prefix_proof(&*lease, &id).unwrap();
                    if let Some(proof) = proof {
                        assert!(proof.is_well_formed());
                    }
                    drop(lease);
                });
                waiting_rx
                    .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                    .unwrap();
                assert!(Instant::now() < deadline);
                assert!(
                    matches!(
                        available_rx.recv_timeout(
                            Duration::from_millis(50)
                                .min(deadline.saturating_duration_since(Instant::now()))
                        ),
                        Err(mpsc::RecvTimeoutError::Timeout)
                    ),
                    "waiter cannot pass an actually owned Opening state"
                );
                release_tx.send(()).unwrap();
                let failed = opener.join().unwrap();
                if unwind {
                    let panic = failed.expect_err("the negative initializer must unwind");
                    assert_eq!(
                        panic.downcast_ref::<&str>().copied(),
                        Some("negative actual resident initializer unwind")
                    );
                } else {
                    assert!(
                        matches!(failed, Ok(Err(MetadataBackendError::Serde(reason)))
                        if reason == "negative actual resident initializer error")
                    );
                }
                available_rx
                    .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                    .unwrap();
                assert!(matches!(
                    *slot.state.lock().unwrap(),
                    ResidentLocalMetadataShardState::Unopened
                ));
                checkout_tx.send(()).unwrap();
                waiter.join().unwrap();
                assert!(matches!(
                    *slot.state.lock().unwrap(),
                    ResidentLocalMetadataShardState::Available(_)
                ));
                assert!(slot.operator_status.read().unwrap().snapshot.is_some());
            });
        }
        runtime.close().unwrap();
    }

    #[test]
    fn resident_shard_lease_moves_only_backend_ownership() {
        assert!(
            std::mem::size_of::<ResidentLocalMetadataShardLease>() <= 256,
            "resident shard lease grew to {} bytes",
            std::mem::size_of::<ResidentLocalMetadataShardLease>()
        );
    }

    #[test]
    fn replica_repair_router_defers_non_primary_shard_until_first_access() {
        let root = sinomo_testdir::TestDir::new("dfs-lazy-replica-shard-slot");
        let metadata_root = root.path().join("metadata");
        let local_node_id = node("lazy-slot-local");
        let root_shard_id = DfsMetadataShardId::new("root");
        let replica_shard_id = DfsMetadataShardId::new("services");
        let mut backend = LocalShardedMetadataBackend::new_with_replica_repair(
            &metadata_root,
            configs(&local_node_id),
            &local_node_id,
        )
        .expect("open node-aware sharded backend");

        assert_eq!(
            backend.opened_shard_ids(),
            BTreeSet::from([root_shard_id.clone()]),
            "only the local authoritative primary should be decoded eagerly"
        );
        backend
            .shard_backend_mut(&replica_shard_id)
            .expect("open lazy replica shard on first access");
        assert_eq!(
            backend.opened_shard_ids(),
            BTreeSet::from([root_shard_id, replica_shard_id])
        );
    }

    #[test]
    fn strict_and_inspection_routers_keep_eager_open_contract() {
        let root = sinomo_testdir::TestDir::new("dfs-eager-compatible-shard-slots");
        let metadata_root = root.path().join("metadata");
        let local_node_id = node("eager-slot-local");
        let expected = BTreeSet::from([
            DfsMetadataShardId::new("root"),
            DfsMetadataShardId::new("services"),
        ]);

        let strict = LocalShardedMetadataBackend::new(&metadata_root, configs(&local_node_id))
            .expect("open strict sharded backend");
        assert_eq!(strict.opened_shard_ids(), expected);
        drop(strict);

        let inspection = LocalShardedMetadataBackend::open_for_inspection(
            &metadata_root,
            configs(&local_node_id),
        )
        .expect("open inspection sharded backend");
        assert_eq!(inspection.opened_shard_ids(), expected);
    }

    #[test]
    fn lazy_slots_preserve_distinct_backend_instances_between_routers() {
        let root = sinomo_testdir::TestDir::new("dfs-distinct-lazy-shard-slots");
        let metadata_root = root.path().join("metadata");
        let local_node_id = node("distinct-slot-local");
        let replica_shard_id = DfsMetadataShardId::new("services");
        let first = LocalShardedMetadataBackend::new_with_replica_repair(
            &metadata_root,
            configs(&local_node_id),
            &local_node_id,
        )
        .expect("open first router");
        let second = LocalShardedMetadataBackend::new_with_replica_repair(
            &metadata_root,
            configs(&local_node_id),
            &local_node_id,
        )
        .expect("open second router");

        let first_backend = first
            .shard_backend(&replica_shard_id)
            .expect("open first replica slot");
        let second_backend = second
            .shard_backend(&replica_shard_id)
            .expect("open second replica slot");
        assert!(
            !std::ptr::eq(first_backend, second_backend),
            "lazy slots must not merge instance-local authority capabilities"
        );
    }

    #[test]
    fn resident_handles_move_open_backends_and_open_deferred_shards_once() {
        let root = sinomo_testdir::TestDir::new("dfs-resident-shard-open-once");
        let metadata_root = root.path().join("metadata");
        let local_node_id = node("resident-open-local");
        let file_system_id = crate::sifs_file_system_id();
        let root_shard_id = DfsMetadataShardId::new("root");
        let replica_shard_id = DfsMetadataShardId::new("services");
        let router = LocalShardedMetadataBackend::new_with_replica_repair(
            &metadata_root,
            configs(&local_node_id),
            &local_node_id,
        )
        .expect("open node-aware sharded backend");
        crate::metadata_backend::reset_metadata_backend_open_calls();

        let resident = router.into_resident();
        let root_handle = resident
            .scope_shard(&file_system_id, &root_shard_id)
            .expect("scope moved primary");
        assert!(root_handle.is_open());
        root_handle
            .with_backend(|backend| backend.metadata_summary().map(|_| ()))
            .expect("read moved primary backend");
        assert_eq!(
            crate::metadata_backend::metadata_backend_open_calls(),
            0,
            "converting and borrowing an eager shard must not reopen it"
        );

        let replica_handle = resident
            .scope_shard(&file_system_id, &replica_shard_id)
            .expect("scope deferred replica");
        assert!(!replica_handle.is_open());
        replica_handle
            .with_backend(|backend| backend.metadata_summary().map(|_| ()))
            .expect("open deferred replica once");
        assert_eq!(crate::metadata_backend::metadata_backend_open_calls(), 1);
        replica_handle
            .clone()
            .with_backend(|backend| backend.metadata_summary().map(|_| ()))
            .expect("reuse deferred replica instance");
        assert_eq!(
            crate::metadata_backend::metadata_backend_open_calls(),
            1,
            "cloned resident handles must reuse the first lazy open"
        );
    }

    #[test]
    fn resident_topology_match_requires_exact_root_and_config_generation() {
        let root = sinomo_testdir::TestDir::new("dfs-resident-topology-identity");
        let metadata_root = root.path().join("metadata");
        let local_node_id = node("resident-topology-local");
        let shards = configs(&local_node_id);
        let resident = LocalShardedMetadataBackend::new(&metadata_root, shards.clone())
            .expect("open resident topology")
            .into_resident();

        assert!(
            resident
                .matches_topology(&metadata_root, &shards)
                .expect("matching topology")
        );
        assert!(
            !resident
                .matches_topology(&root.path().join("other-metadata"), &shards)
                .expect("mismatched root")
        );
        let mut advanced = shards.clone();
        advanced[0].epoch = advanced[0].epoch.saturating_add(1);
        assert!(
            !resident
                .matches_topology(&metadata_root, &advanced)
                .expect("mismatched epoch")
        );
        assert!(
            !resident
                .matches_topology(&metadata_root, &shards[1..])
                .expect("missing shard")
        );
    }

    #[test]
    fn bounded_status_keeps_an_unopened_local_primary_blocking() {
        let root = sinomo_testdir::TestDir::new("dfs-bounded-status-unhydrated-primary");
        let metadata_root = root.path().join("metadata");
        let local_node_id = node("bounded-status-primary-local");
        let shard = configs(&local_node_id)[0].clone();
        let shard_id = DfsMetadataShardId::new(&shard.shard_id);
        let resident = LocalShardedMetadataBackend::new_with_replica_repair(
            &metadata_root,
            vec![shard],
            &local_node_id,
        )
        .expect("open resident primary")
        .into_resident();
        let slot = resident
            .inner
            .shards
            .get(&shard_id)
            .expect("resident primary slot");
        *slot.state.lock().unwrap_or_else(|error| error.into_inner()) =
            ResidentLocalMetadataShardState::Unopened;
        slot.operator_status
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .snapshot = None;
        crate::metadata_backend::reset_metadata_backend_open_calls();

        let snapshot = resident
            .bounded_status_snapshot()
            .expect("an unopened local primary returns blocking partial status");
        assert!(snapshot.bounded_omissions.is_empty());
        assert_eq!(snapshot.incomplete_reasons.len(), 1);
        assert!(snapshot.incomplete_reasons[0].contains("status unavailable"));
        assert!(snapshot.incomplete_reasons[0].contains("is not hydrated for bounded status"));
        assert_eq!(snapshot.metadata_shards.len(), 1);
        assert_eq!(snapshot.metadata_shards[0].shard_id, shard_id);
        assert_eq!(
            crate::metadata_backend::metadata_backend_open_calls(),
            0,
            "bounded status must fail closed without hydrating a local primary"
        );
    }

    #[test]
    fn unhydrated_status_is_omitted_only_for_a_non_primary_shard() {
        let local_node_id = node("bounded-status-role-local");
        let shard_id = DfsMetadataShardId::new("root");
        let local_primary = configs(&local_node_id)[0].clone();
        assert_eq!(
            non_primary_unhydrated_status_omission(Some(&local_node_id), &local_primary, &shard_id,),
            None,
            "an unopened local primary must remain a blocking incomplete status"
        );

        let mut non_primary = local_primary;
        non_primary.primary_node_id = node("bounded-status-role-primary");
        assert!(matches!(
            non_primary_unhydrated_status_omission(
                Some(&local_node_id),
                &non_primary,
                &shard_id,
            ),
            Some(DfsOperatorStatusOmission::NonPrimaryResidentMetadataNotHydrated {
                file_system_id,
                shard_id: omitted_shard_id,
            }) if file_system_id == non_primary.file_system_id && omitted_shard_id == shard_id
        ));
        assert_eq!(
            non_primary_unhydrated_status_omission(None, &non_primary, &shard_id),
            None,
            "missing local identity must fail closed"
        );
    }

    #[test]
    fn historical_routing_zero_lock_proof_requires_every_shard_summary() {
        let root = sinomo_testdir::TestDir::new("dfs-bounded-status-routing-zero-incomplete");
        let metadata_root = root.path().join("metadata");
        let local_node_id = node("bounded-status-routing-zero-incomplete-local");
        let root_shard = configs(&local_node_id)[0].clone();
        drop(
            LocalShardedMetadataBackend::new(&metadata_root, vec![root_shard])
                .expect("open predecessor topology"),
        );

        let resident = LocalShardedMetadataBackend::new_with_replica_repair(
            &metadata_root,
            configs(&local_node_id),
            &local_node_id,
        )
        .expect("open successor topology with deferred replica")
        .into_resident();
        crate::metadata_backend::reset_metadata_backend_open_calls();
        let snapshot = resident
            .bounded_status_snapshot()
            .expect("bounded incomplete historical status");
        let historical = snapshot
            .bounded_omissions
            .iter()
            .find(|omission| {
                matches!(
                    omission,
                    DfsOperatorStatusOmission::HistoricalRoutingTopology { .. }
                )
            })
            .expect("historical routing omission");

        assert!(
            historical
                .omitted_metrics()
                .contains(&crate::DfsOperatorStatusMetric::CrossShardNamespaceLockCount),
            "an unhydrated shard must keep the lock projection unknown"
        );
        assert!(snapshot.bounded_omissions.iter().any(|omission| matches!(
            omission,
            DfsOperatorStatusOmission::NonPrimaryResidentMetadataNotHydrated { .. }
        )));
        assert_eq!(
            crate::metadata_backend::metadata_backend_open_calls(),
            0,
            "zero-proof status must not hydrate a missing shard summary"
        );
    }

    #[test]
    fn explicit_startup_status_initialization_proves_zero_across_deferred_replicas() {
        let root = sinomo_testdir::TestDir::new("dfs-bounded-status-startup-initialization");
        let metadata_root = root.path().join("metadata");
        let local_node_id = node("bounded-status-startup-local");
        let root_shard = configs(&local_node_id)[0].clone();
        drop(
            LocalShardedMetadataBackend::new(&metadata_root, vec![root_shard])
                .expect("open predecessor topology"),
        );
        let resident = LocalShardedMetadataBackend::new_with_replica_repair(
            &metadata_root,
            configs(&local_node_id),
            &local_node_id,
        )
        .expect("open successor topology with deferred replicas")
        .into_resident();

        resident
            .initialize_bounded_status_snapshots()
            .expect("initialize startup status projections");
        for slot in resident.inner.shards.values().filter(|slot| {
            slot.config.primary_node_id != local_node_id
                && slot.config.replica_node_ids.contains(&local_node_id)
        }) {
            assert!(matches!(
                *slot.state.lock().expect("deferred slot state"),
                ResidentLocalMetadataShardState::Unopened
            ));
            assert!(
                slot.operator_status
                    .read()
                    .expect("deferred slot operator status")
                    .snapshot
                    .is_some(),
                "startup inspection must retain only the bounded projection"
            );
        }
        let snapshot = resident
            .bounded_status_snapshot()
            .expect("complete bounded startup status");

        assert!(!snapshot.bounded_omissions.iter().any(|omission| matches!(
            omission,
            DfsOperatorStatusOmission::NonPrimaryResidentMetadataNotHydrated { .. }
        )));
        let historical = snapshot
            .bounded_omissions
            .iter()
            .find(|omission| {
                matches!(
                    omission,
                    DfsOperatorStatusOmission::HistoricalRoutingTopology { .. }
                )
            })
            .expect("historical routing omission remains visible");
        assert!(
            !historical
                .omitted_metrics()
                .contains(&crate::DfsOperatorStatusMetric::CrossShardNamespaceLockCount),
            "startup initialization must preserve the exact zero-lock proof"
        );
        assert_eq!(snapshot.summary.cross_shard_namespace_lock_count, 0);
    }

    #[test]
    fn resident_handles_share_instance_local_handoff_authority() {
        let root = sinomo_testdir::TestDir::new("dfs-resident-shard-handoff-authority");
        let metadata_root = root.path().join("metadata");
        let local_node_id = node("resident-handoff-local");
        let file_system_id = crate::sifs_file_system_id();
        let shard_id = DfsMetadataShardId::new("root");
        let mut shard = configs(&local_node_id)
            .into_iter()
            .find(|config| config.shard_id == shard_id.0)
            .expect("root config");
        let resident = LocalShardedMetadataBackend::new(&metadata_root, vec![shard.clone()])
            .expect("open sharded backend")
            .into_resident();
        let first = resident
            .scope_shard(&file_system_id, &shard_id)
            .expect("scope first handle");
        let second = first.clone();

        first
            .with_backend(|backend| {
                backend.freeze_metadata_shard_for_membership_handoff(
                    &shard_id,
                    "resident-handoff",
                    shard.epoch,
                )?;
                shard.epoch += 1;
                let checked_prefix = DfsMetadataBackend::metadata_prefix_proof(backend, &shard_id)?
                    .ok_or_else(|| {
                        MetadataBackendError::Serde("resident rebind has no actual prefix".into())
                    })?;
                backend.clear_metadata_shard_membership_handoff_freeze(
                    &shard_id,
                    "resident-handoff",
                    &shard,
                    &checked_prefix,
                )?;
                Ok(())
            })
            .expect("bind activated handoff to resident instance");
        assert!(
            !second
                .with_backend(|backend| {
                    backend.metadata_shard_authoritative_writes_frozen(&shard_id)
                })
                .expect("observe authority through cloned handle"),
            "every handle must observe the authority bound to the moved instance"
        );
    }

    #[test]
    fn resident_global_checkout_returns_same_instances_and_router_updates() {
        let root = sinomo_testdir::TestDir::new("dfs-resident-global-checkout");
        let metadata_root = root.path().join("metadata");
        let local_node_id = node("resident-global-local");
        let file_system_id = crate::sifs_file_system_id();
        let shard_id = DfsMetadataShardId::new("root");
        let shard = configs(&local_node_id)
            .into_iter()
            .find(|config| config.shard_id == shard_id.0)
            .expect("root config");
        let resident = LocalShardedMetadataBackend::new(&metadata_root, vec![shard.clone()])
            .expect("open sharded backend")
            .into_resident();
        let handle = resident
            .scope_shard(&file_system_id, &shard_id)
            .expect("scope resident shard");
        crate::metadata_backend::reset_metadata_backend_open_calls();

        {
            let mut global = resident.checkout_global().expect("check out global router");
            global
                .freeze_metadata_shard_for_membership_handoff(
                    &shard_id,
                    "resident-global-handoff",
                    shard.epoch,
                )
                .expect("freeze resident shard");
            let mut active = shard.clone();
            active.epoch += 1;
            let checked_prefix = global
                .metadata_prefix_proof(&shard_id)
                .expect("actual resident prefix")
                .expect("resident rebind prefix exists");
            global
                .clear_metadata_shard_membership_handoff_freeze(
                    &shard_id,
                    "resident-global-handoff",
                    &active,
                    &checked_prefix,
                )
                .expect("activate resident shard");
        }
        assert_eq!(
            crate::metadata_backend::metadata_backend_open_calls(),
            0,
            "global checkout must reconstruct the router from resident instances"
        );

        assert_eq!(
            resident
                .shard_config(&shard_id)
                .expect("returned router config")
                .epoch,
            shard.epoch + 1,
            "global checkout must return router configuration updates"
        );
        handle
            .with_backend(|backend| {
                assert!(!backend.metadata_shard_authoritative_writes_frozen(&shard_id)?);
                Ok(())
            })
            .expect("returned exact backend authority");
    }

    #[test]
    fn resident_backend_serializes_one_shard_without_blocking_another() {
        let root = sinomo_testdir::TestDir::new("dfs-resident-disjoint-shard-locks");
        let metadata_root = root.path().join("metadata");
        let local_node_id = node("resident-lock-local");
        let file_system_id = crate::sifs_file_system_id();
        let resident = LocalShardedMetadataBackend::new(&metadata_root, configs(&local_node_id))
            .expect("open strict sharded backend")
            .into_resident();
        let root_handle = resident
            .scope_shard(&file_system_id, &DfsMetadataShardId::new("root"))
            .expect("scope root shard");
        let services_handle = resident
            .scope_shard(&file_system_id, &DfsMetadataShardId::new("services"))
            .expect("scope services shard");
        let root_guard = root_handle.checkout().expect("check out root shard");

        let (services_tx, services_rx) = mpsc::channel();
        let services_thread = std::thread::spawn(move || {
            let _guard = services_handle
                .checkout()
                .expect("check out disjoint shard");
            services_tx.send(()).expect("signal disjoint lock");
        });
        services_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("disjoint shard must not wait for root shard");
        services_thread.join().expect("disjoint lock thread");

        let (same_tx, same_rx) = mpsc::channel();
        let same_handle = root_handle.clone();
        let same_thread = std::thread::spawn(move || {
            let _guard = same_handle
                .checkout()
                .expect("check out same shard after release");
            same_tx.send(()).expect("signal same-shard lock");
        });
        assert!(
            same_rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "same-shard handle must remain serialized"
        );
        drop(root_guard);
        same_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("same shard must advance after release");
        same_thread.join().expect("same-shard lock thread");
    }
}

impl LocalShardedMetadataBackend {
    fn require_prepared_operation_shard(
        &self,
        file_system_id: &DfsFileSystemId,
        shard_id: &DfsMetadataShardId,
    ) -> Result<()> {
        if !self
            .shard_configs
            .get(&shard_id.0)
            .is_some_and(|config| &config.file_system_id == file_system_id)
        {
            return Err(MetadataBackendError::Serde(
                "v7 publication filesystem/shard routing mismatch".into(),
            ));
        }
        Ok(())
    }
}

impl DfsMetadataBackend for LocalShardedMetadataBackend {
    #[cfg(test)]
    fn retain_native_prefix_history(
        &mut self,round:&crate::node_transport::OwnedNativeMetadataPrefixRound,
        provider:&crate::publication_guard::DfsPublicationExecutionProvider,
        execution:&crate::publication_guard::DfsPublicationExecutionOwners,
        local:&NodeId,deadline:std::time::Instant,
    )->Result<crate::metadata_backend::native_prefix_history::NativePrefixHistoryIntake> {
        self.shard_backend_mut(&round.target().shard_id)?.retain_native_prefix_history(round,provider,execution,local,deadline)
    }
    #[cfg(test)]
    fn read_native_prefix_history(&self,shard:&DfsMetadataShardId)
        ->Result<Option<crate::metadata_backend::native_prefix_history::NativePrefixHistoryRecord>> {
        self.shard_backend(shard)?.read_native_prefix_history(shard)
    }

    #[cfg(test)]
    fn install_selected_local_recovery_fence(
        &mut self,
        reservation: &crate::control_plane::DfsMetadataShardRecoveryBallotRequest,
        context: &crate::metadata_api::DfsSelectedLocalRecoveryContext,
    ) -> Result<crate::metadata_api::DfsSelectedLocalRecoveryFenceData> {
        self.shard_backend_mut(&DfsMetadataShardId::new(&reservation.serving_shard.shard_id))?
            .install_selected_local_recovery_fence(reservation, context)
    }
    #[cfg(test)]
    fn selected_local_recovery_fence(&self, shard: &DfsMetadataShardId)
        -> Result<Option<crate::metadata_api::DfsSelectedLocalRecoveryFenceData>>
    {
        self.shard_backend(shard)?.selected_local_recovery_fence(shard)
    }
    fn selected_original_data_layout(
        &self,
        file_system_id: &DfsFileSystemId,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Option<Arc<crate::DfsSelectedOriginalDataLayout>>> {
        let Some(slot) = self.shards.get(shard_id) else { return Ok(None); };
        let Some(layout) = &slot.selected_original_data_layout else { return Ok(None); };
        if &slot.config.file_system_id != file_system_id {
            return Err(MetadataBackendError::Rejected {
                code: crate::DfsMetadataRejectionCode::Other,
                message: "selected router getter names another filesystem".into(),
            });
        }
        layout.stage(file_system_id, shard_id, &slot.root)?;
        Ok(Some(Arc::clone(layout)))
    }

    #[cfg(test)]
    fn test_receipt_scan_counts_by_shard(&self) -> BTreeMap<DfsMetadataShardId, (usize, usize)> {
        self.shards
            .iter()
            .filter_map(|(shard, slot)| {
                slot.backend
                    .get()
                    .map(|backend| (shard.clone(), backend.test_receipt_scan_counts()))
            })
            .collect()
    }
    #[cfg(test)]
    fn reset_test_receipt_scan_counts_by_shard(&self) {
        for slot in self.shards.values() {
            if let Some(backend) = slot.backend.get() {
                backend.reset_test_receipt_scan_counts();
            }
        }
    }

    fn prepared_content_reference_protection(
        &self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        max_prepares: usize,
    ) -> Result<(bool, bool)> {
        let mut remaining = max_prepares;
        let mut complete = true;
        for (shard, slot) in &self.shards {
            if slot.config.file_system_id != *file_system_id {
                continue;
            }
            // Charge every visited root as well as every inspected indexed
            // owner, so many empty lazy roots cannot escape the caller budget.
            if remaining == 0 {
                return Ok((false, false));
            }
            remaining -= 1;
            let tables = self.shard_backend(shard)?.current_tables_snapshot()?;
            let owners = tables.prepared_content_reference_owner_count(file_system_id, object_id);
            let (protected, root_complete) = tables.prepared_content_reference_protection(
                file_system_id,
                object_id,
                remaining,
            )?;
            if protected {
                return Ok((true, true));
            }
            remaining = remaining.saturating_sub(owners.min(remaining));
            complete &= root_complete;
        }
        Ok((false, complete))
    }
    fn plan_cross_shard_pin_transfer(
        &self,
        transfer_id: &str,
        moved_object: &DfsObjectMetadata,
        source_path: &str,
        incoming_versions: &[DfsObjectVersionRecord],
        incoming_pins: &[DfsMetadataVersionPinRecord],
    ) -> Result<crate::metadata_api::DfsCrossShardPinTransferPlan> {
        let target =
            self.select_shard_id_for_path(&moved_object.file_system_id, &moved_object.path)?;
        self.shard_backend(&target)?.plan_cross_shard_pin_transfer(
            transfer_id,
            moved_object,
            source_path,
            incoming_versions,
            incoming_pins,
        )
    }
    fn plan_foreign_shard_source_eviction(
        &self,
        moved_object: &DfsObjectMetadata,
        old_path: &str,
        captured_source_family: &DfsCrossShardSourceFamily,
        transfer_id: Option<&str>,
        target_shard_id: &DfsMetadataShardId,
    ) -> Result<crate::metadata_api::DfsForeignShardSourceEvictionPlan> {
        let target =
            self.select_shard_id_for_path(&moved_object.file_system_id, &moved_object.path)?;
        if &target != target_shard_id {
            return Err(MetadataBackendError::Serde(
                "source eviction target does not match current routing assignment".into(),
            ));
        }
        let source = self.select_shard_id_for_path(&moved_object.file_system_id, old_path)?;
        self.shard_backend(&source)?
            .plan_foreign_shard_source_eviction(
                moved_object,
                old_path,
                captured_source_family,
                transfer_id,
                target_shard_id,
            )
    }
    fn prepared_operation_record(
        &self,
        operation_id: &str,
    ) -> Result<Option<DfsPreparedOperationRecord>> {
        let Some(digest) = self.prepared_operation_digest(operation_id)? else {
            return Ok(None);
        };
        self.shard_backend(&digest.shard_id)?
            .prepared_operation_record(operation_id)
    }
    fn prepared_operation_read_context(
        &self,
        shard_id: &DfsMetadataShardId,
        file_system_id: &DfsFileSystemId,
        operation_id: &str,
    ) -> Result<(Option<DfsFileSystem>, Option<DfsPreparedOperationRecord>)> {
        self.shard_backend(shard_id)?
            .prepared_operation_read_context(shard_id, file_system_id, operation_id)
    }
    fn prepared_operation_reservations(
        &self,
        payload: &crate::DfsPreparedOperationPayload,
    ) -> Result<Vec<DfsPreparedOperationRecord>> {
        self.shard_backend(&payload.shard_id)?
            .prepared_operation_reservations(payload)
    }
    fn write_operation_admission_snapshot(
        &self,
        shard_id: &DfsMetadataShardId,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        intent_id: &str,
    ) -> Result<crate::DfsWriteOperationAdmissionSnapshot> {
        self.shard_backend(shard_id)?
            .write_operation_admission_snapshot(shard_id, file_system_id, object_id, intent_id)
    }
    fn list_prepared_operations(&self) -> Result<Vec<DfsPreparedOperationRecord>> {
        let mut records = Vec::new();
        for shard in self.shards.keys() {
            records.extend(self.shard_backend(shard)?.list_prepared_operations()?);
        }
        Ok(records)
    }
    fn prepared_operation_for_request(
        &self,
        requester: &NodeId,
        request_id: &str,
    ) -> Result<Option<DfsPreparedOperationRecord>> {
        let mut found = None;
        for shard in self.shards.keys() {
            if let Some(record) = self
                .shard_backend(shard)?
                .prepared_operation_for_request(requester, request_id)?
            {
                if found.is_some() {
                    return Err(MetadataBackendError::Serde(
                        "request has competing physical shard owners".into(),
                    ));
                }
                found = Some(record);
            }
        }
        Ok(found)
    }

    #[cfg(any(test, feature = "test-support"))]
    fn native_held_snapshot_for_scope(
        &self,
        fs: &DfsFileSystemId,
        shard: &DfsMetadataShardId,
    ) -> Option<crate::NativeHeldMetadataSnapshotV1> {
        let slot = self.shards.get(shard)?;
        if &slot.config.file_system_id != fs {
            return None;
        }
        // Deliberately avoid backend(), whose lazy initialization can issue IO.
        slot.backend
            .get()?
            .native_held_snapshot_for_scope(fs, shard)
    }

    fn prepare_operation(
        &mut self,
        payload: DfsPreparedOperationPayload,
        source: Option<&crate::source_adoption::DfsSourceAdoptionWitness>,
    ) -> Result<DfsPreparedOperationDigest> {
        let shard_id = payload.shard_id.clone();
        self.require_prepared_operation_shard(&payload.file_system_id, &shard_id)?;
        self.shard_backend_mut(&shard_id)?
            .prepare_operation(payload, source)
    }

    fn prepared_operation_digest(
        &self,
        operation_id: &str,
    ) -> Result<Option<DfsPreparedOperationDigest>> {
        let mut found = None;
        for shard_id in self.shards.keys() {
            if let Some(digest) = self
                .shard_backend(shard_id)?
                .prepared_operation_digest(operation_id)?
            {
                if found.is_some() {
                    return Err(MetadataBackendError::Serde(
                        "v7 operation identity occurs on multiple shards".into(),
                    ));
                }
                found = Some(digest);
            }
        }
        Ok(found)
    }

    fn prepared_operation_payload(
        &self,
        operation_id: &str,
    ) -> Result<Option<Arc<DfsPreparedOperationPayload>>> {
        let Some(digest) = self.prepared_operation_digest(operation_id)? else {
            return Ok(None);
        };
        self.shard_backend(&digest.shard_id)?
            .prepared_operation_payload(operation_id)
    }

    fn prepared_operation_payload_for_digest(
        &self,
        digest: &DfsPreparedOperationDigest,
    ) -> Result<Option<Arc<DfsPreparedOperationPayload>>> {
        self.require_prepared_operation_shard(&digest.file_system_id, &digest.shard_id)?;
        self.shard_backend(&digest.shard_id)?
            .prepared_operation_payload_for_digest(digest)
    }

    fn committed_prepared_operation(
        &self,
        digest: &DfsPreparedOperationDigest,
    ) -> Result<Option<DfsMetadataJournalEntry>> {
        self.require_prepared_operation_shard(&digest.file_system_id, &digest.shard_id)?;
        self.shard_backend(&digest.shard_id)?
            .committed_prepared_operation(digest)
    }

    fn publication_owner_scope(
        &self,
        file_system_id: &DfsFileSystemId,
        shard_id: &DfsMetadataShardId,
    ) -> Result<crate::publication_guard::DfsPublicationOwnerScope> {
        self.require_prepared_operation_shard(file_system_id, shard_id)?;
        self.shard_backend(shard_id)?
            .publication_owner_scope(file_system_id, shard_id)
    }

    fn publication_writer_session(
        &mut self,
        file_system_id: &DfsFileSystemId,
        shard_id: &DfsMetadataShardId,
        digest: &DfsPreparedOperationDigest,
        source: Option<&crate::DfsSourceAdoptionWitness>,
        provider: Option<&crate::publication_guard::DfsPublicationExecutionProvider>,
        execution: Option<&crate::publication_guard::DfsPublicationExecutionOwners>,
        clustered: Option<&crate::DfsMetadataShardConfig>,
    ) -> Result<crate::publication_guard::DfsPublicationWriterSession> {
        self.require_prepared_operation_shard(file_system_id, shard_id)?;
        self.shard_backend_mut(shard_id)?
            .publication_writer_session(
                file_system_id,
                shard_id,
                digest,
                source,
                provider,
                execution,
                clustered,
            )
    }

    fn publication_writer_file_system(
        &self,
        file_system_id: &DfsFileSystemId,
        session: &crate::publication_guard::DfsPublicationWriterSession,
    ) -> Result<Option<DfsFileSystem>> {
        let scope = session.owner_scope();
        self.require_prepared_operation_shard(file_system_id, scope.shard_id())?;
        self.shard_backend(scope.shard_id())?
            .publication_writer_file_system(file_system_id, session)
    }

    fn publication_writer_object(
        &self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        session: &crate::publication_guard::DfsPublicationWriterSession,
    ) -> Result<Option<DfsObjectMetadata>> {
        let scope = session.owner_scope();
        self.require_prepared_operation_shard(file_system_id, scope.shard_id())?;
        self.shard_backend(scope.shard_id())?
            .publication_writer_object(file_system_id, object_id, session)
    }

    fn commit_prepared_operation(
        &mut self,
        digest: &DfsPreparedOperationDigest,
        guard: crate::publication_guard::DfsPublicationGuard,
        enrollment: Option<&mut dyn FnMut(&[crate::DfsRepairAttemptRecord]) -> Result<()>>,
    ) -> Result<DfsPreparedOperationCommitResult> {
        self.require_prepared_operation_shard(&digest.file_system_id, &digest.shard_id)?;
        let payload = self
            .shard_backend(&digest.shard_id)?
            .held_prepared_operation_payload(&digest.operation_id)
            .ok_or_else(|| {
                MetadataBackendError::Serde("v7 commit has no captured shard-local body".into())
            })?;
        let entry = self
            .shard_backend_mut(&digest.shard_id)?
            .commit_prepared_operation(digest, guard, enrollment)?;
        let object_ids = payload
            .transaction
            .objects
            .iter()
            .map(|object| object.object_id.clone())
            .collect::<BTreeSet<_>>();
        if entry.published {
            self.reconcile_object_shard_indexes(object_ids.iter())?;
        }
        Ok(entry)
    }

    fn metadata_shard_id_for_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsMetadataShardId>> {
        self.select_shard_id_for_path(file_system_id, path)
            .map(Some)
    }

    fn begin_consistent_read_snapshot(&mut self) -> Result<()> {
        let shard_ids = self.shards.keys().cloned().collect::<Vec<_>>();
        self.begin_consistent_read_snapshot_for_shards(&shard_ids)
    }

    fn begin_stat_read_snapshot(&mut self) -> Result<()> {
        let ids = self.shards.keys().cloned().collect::<Vec<_>>();
        self.begin_stat_read_snapshot_on_shards(&ids)
    }
    fn end_stat_read_snapshot(&mut self) {
        let ids = self.shards.keys().cloned().collect::<Vec<_>>();
        self.end_stat_read_snapshot_on_shards(&ids);
    }
    fn begin_stat_read_snapshot_on_shard(&mut self, id: &DfsMetadataShardId) -> Result<()> {
        self.shard_backend_mut(id)?.pin_stat_read_snapshot()
    }
    fn end_stat_read_snapshot_on_shard(&mut self, id: &DfsMetadataShardId) {
        self.shards
            .get_mut(id)
            .expect("pinned stat shard remains installed")
            .opened_backend_mut()
            .expect("pinned stat shard remains open")
            .unpin_stat_read_snapshot();
    }
    fn begin_stat_read_snapshot_for_path(
        &mut self,
        fs: &DfsFileSystemId,
        path: &str,
    ) -> Result<()> {
        let ids = self.consistent_read_shard_ids_for_path(fs, path)?;
        self.begin_stat_read_snapshot_on_shards(&ids)
    }
    fn end_stat_read_snapshot_for_path(&mut self, fs: &DfsFileSystemId, path: &str) {
        let ids = self
            .consistent_read_shard_ids_for_path(fs, path)
            .expect("pinned stat path retains its routing");
        self.end_stat_read_snapshot_on_shards(&ids);
    }

    fn has_physical_read_scope(&self, shard_id: &DfsMetadataShardId) -> bool {
        self.shards.contains_key(shard_id)
    }

    fn begin_consistent_read_snapshot_on_shard(
        &mut self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<()> {
        if !self.shards.contains_key(shard_id) {
            return Err(MetadataBackendError::Serde(
                "read snapshot names an unavailable shard".into(),
            ));
        }
        self.begin_consistent_read_snapshot_for_shards(std::slice::from_ref(shard_id))
    }

    fn end_consistent_read_snapshot_on_shard(&mut self, shard_id: &DfsMetadataShardId) {
        self.end_consistent_read_snapshot_for_shards(std::slice::from_ref(shard_id));
    }

    fn end_consistent_read_snapshot(&mut self) {
        let shard_ids = self.shards.keys().cloned().collect::<Vec<_>>();
        self.end_consistent_read_snapshot_for_shards(&shard_ids);
    }

    fn begin_consistent_read_snapshot_for_path(
        &mut self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<()> {
        let shard_ids = self.consistent_read_shard_ids_for_path(file_system_id, path)?;
        self.begin_consistent_read_snapshot_for_shards(&shard_ids)
    }

    fn end_consistent_read_snapshot_for_path(
        &mut self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) {
        let Ok(shard_ids) = self.consistent_read_shard_ids_for_path(file_system_id, path) else {
            debug_assert!(false, "path-scoped metadata read lost its routed shard");
            return;
        };
        self.end_consistent_read_snapshot_for_shards(&shard_ids);
    }

    fn get_file_system(&self, file_system_id: &DfsFileSystemId) -> Result<Option<DfsFileSystem>> {
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            if let Some(file_system) = backend.get_file_system(file_system_id)? {
                return Ok(Some(file_system));
            }
        }
        Ok(None)
    }
    fn get_file_system_on_shard(
        &self,
        shard_id: &DfsMetadataShardId,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<DfsFileSystem>> {
        self.shard_backend(shard_id)?
            .get_file_system_on_shard(shard_id, file_system_id)
    }

    fn list_file_systems(&self) -> Result<Vec<DfsFileSystem>> {
        let mut file_systems = BTreeMap::<DfsFileSystemId, DfsFileSystem>::new();
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            for file_system in backend.list_file_systems()? {
                file_systems.insert(file_system.file_system_id.clone(), file_system);
            }
        }
        Ok(file_systems.into_values().collect())
    }

    fn get_object(&self, object_id: &DfsObjectId) -> Result<Option<DfsObjectMetadata>> {
        #[cfg(test)]
        SHARDED_METADATA_OBJECT_ID_LOOKUP_COUNT.set(
            SHARDED_METADATA_OBJECT_ID_LOOKUP_COUNT
                .get()
                .saturating_add(1),
        );
        if let Some(shard_id) = self.shard_id_for_object(object_id)? {
            return self.shard_backend(&shard_id)?.get_object(object_id);
        }
        Ok(None)
    }

    fn get_object_on_shard(
        &self,
        shard_id: &DfsMetadataShardId,
        object_id: &DfsObjectId,
    ) -> Result<Option<DfsObjectMetadata>> {
        self.shard_backend(shard_id)?.get_object(object_id)
    }

    fn get_advisory_lock_state(
        &self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
    ) -> Result<Option<DfsAdvisoryLockStateRecord>> {
        let Some(shard_id) = self.shard_id_for_object(object_id)? else {
            return Ok(None);
        };
        self.shard_backend(&shard_id)?
            .get_advisory_lock_state(file_system_id, object_id)
    }

    fn get_object_by_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        let shard_id = self.select_shard_id_for_path(file_system_id, path)?;
        let object = self
            .shard_backend(&shard_id)?
            .get_object_by_path(file_system_id, path)?;
        if let Some(object) = object
            && self.object_is_visible_from_shard(&object, &shard_id)?
        {
            return Ok(Some(object));
        }
        // A non-authoritative physical row on the currently selected
        // shard does not suppress a live fallback. In particular, a
        // source-eviction marker may become path-selected after a later
        // topology change, but it remains storage-only evidence.
        for fallback_shard_id in
            self.fallback_shard_ids_for_path(file_system_id, path, &shard_id)?
        {
            let Some(candidate) = self
                .shard_backend(&fallback_shard_id)?
                .get_object_by_path(file_system_id, path)?
            else {
                continue;
            };
            if self.object_is_visible_from_shard(&candidate, &fallback_shard_id)? {
                return Ok(Some(candidate));
            }
        }
        Ok(None)
    }

    fn get_object_by_path_on_shard(
        &self,
        shard_id: &DfsMetadataShardId,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        self.shard_backend(shard_id)?
            .get_object_by_path(file_system_id, path)
    }

    fn namespace_prepare_expectations(
        &self,
        shard_id: &DfsMetadataShardId,
        transaction: &DfsNamespaceTransaction,
    ) -> Result<(
        Vec<DfsNamespaceObjectVersionExpectation>,
        Vec<DfsNamespacePathOccupantExpectation>,
        BTreeSet<String>,
    )> {
        self.shard_backend(shard_id)?
            .namespace_prepare_expectations(shard_id, transaction)
    }

    fn objects_for_file_system(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Vec<DfsObjectMetadata>> {
        let mut objects = BTreeMap::<DfsObjectId, DfsObjectMetadata>::new();
        for (shard_id, slot) in &self.shards {
            let backend = slot.backend()?;
            walk_metadata_pages(|cursor| {
                let page = backend.objects_for_file_system_page(
                    file_system_id,
                    cursor,
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?;
                for object in page.objects {
                    if !self.object_is_listable_from_shard(&object, shard_id)? {
                        continue;
                    }
                    objects.insert(object.object_id.clone(), object);
                }
                Ok(page.next_cursor)
            })?;
        }
        let mut objects = objects.into_values().collect::<Vec<_>>();
        objects.sort_by(|left, right| {
            left.path
                .cmp(&right.path)
                .then_with(|| left.object_id.cmp(&right.object_id))
        });
        Ok(objects)
    }

    fn objects_for_file_system_page(
        &self,
        file_system_id: &DfsFileSystemId,
        after: Option<&DfsObjectPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectPage> {
        self.objects_for_subtree_page(file_system_id, "/", after, max_entries)
    }

    fn objects_for_subtree_page(
        &self,
        file_system_id: &DfsFileSystemId,
        subtree_path: &str,
        after: Option<&DfsObjectPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectPage> {
        if max_entries == 0 {
            return Ok(DfsObjectPage {
                objects: Vec::new(),
                next_cursor: None,
            });
        }
        let resume_shard_id = after.and_then(|cursor| cursor.shard_id.as_ref());
        let entries = collect_resumed_sharded_page_entries(
            &self.shards,
            after,
            resume_shard_id,
            max_entries,
            local_object_page_cursor,
            |_, slot, cursor, limit| {
                let backend = slot.backend()?;
                let page = backend.objects_for_subtree_page(
                    file_system_id,
                    subtree_path,
                    cursor,
                    limit,
                )?;
                Ok((page.objects, page.next_cursor))
            },
            |shard_id, _, object| {
                if !self.object_is_listable_from_shard(&object, shard_id)? {
                    return Ok(None);
                }
                let cursor = object_page_cursor_with_shard(&object, shard_id);
                Ok(Some((object, cursor)))
            },
        )?;
        let (objects, next_cursor) = finish_cursor_page(entries, max_entries);
        Ok(DfsObjectPage {
            objects,
            next_cursor,
        })
    }

    fn object_page_cursor_for_object(
        &self,
        object: &DfsObjectMetadata,
    ) -> Result<DfsObjectPageCursor> {
        Ok(DfsObjectPageCursor {
            path: object.path.clone(),
            object_id: object.object_id.clone(),
            shard_id: self.shard_id_for_object(&object.object_id)?,
        })
    }

    fn directory_page_cursor_for_object(
        &self,
        object: &DfsObjectMetadata,
    ) -> Result<DfsDirectoryPageCursor> {
        let shard_id = self.select_shard_id_for_path(&object.file_system_id, &object.path)?;
        let cursor = self
            .shard_backend(&shard_id)?
            .indexed_directory_page_cursor_for_object(object)?;
        Ok(directory_page_cursor_with_shard(cursor, &shard_id))
    }

    fn child_objects_for_directory_page_entries(
        &self,
        file_system_id: &DfsFileSystemId,
        directory_path: &str,
        after: Option<&DfsDirectoryPageCursor>,
        max_entries: usize,
    ) -> Result<(
        Vec<(DfsObjectMetadata, DfsDirectoryPageCursor)>,
        Option<DfsDirectoryPageCursor>,
    )> {
        if max_entries == 0 {
            return Ok((Vec::new(), None));
        }
        let resume_shard_id = after.and_then(|cursor| cursor.shard_id.as_ref());
        let mut entries = collect_resumed_sharded_page_entries(
            &self.shards,
            after,
            resume_shard_id,
            max_entries,
            local_directory_page_cursor,
            |_, slot, cursor, limit| {
                let backend = slot.backend()?;
                let page = backend.child_objects_for_directory_page_entries(
                    file_system_id,
                    directory_path,
                    cursor,
                    limit,
                )?;
                Ok((page.entries, page.next_cursor))
            },
            |shard_id, _, (object, local_cursor)| {
                if !self.object_is_listable_from_shard(&object, shard_id)? {
                    return Ok(None);
                }
                Ok(Some((
                    object,
                    directory_page_cursor_with_shard(local_cursor, shard_id),
                )))
            },
        )?;
        let has_more = entries.len() > max_entries;
        entries.truncate(max_entries);
        let next_cursor = has_more
            .then(|| entries.last().map(|(_, cursor)| cursor.clone()))
            .flatten();
        Ok((entries, next_cursor))
    }

    fn child_tombstones_for_directory(
        &self,
        file_system_id: &DfsFileSystemId,
        directory_path: &str,
    ) -> Result<Vec<DfsObjectMetadata>> {
        let mut candidate_paths = BTreeSet::new();
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            for tombstone in
                backend.child_tombstones_for_directory(file_system_id, directory_path)?
            {
                candidate_paths.insert(normalize_path(&tombstone.path));
            }
        }
        let mut tombstones = Vec::new();
        for path in candidate_paths {
            if let Some(object) = self.get_object_by_path(file_system_id, &path)?
                && object.kind == DfsObjectKind::Tombstone
            {
                tombstones.push(object);
            }
        }
        Ok(tombstones)
    }

    fn child_objects_for_directory_page(
        &self,
        file_system_id: &DfsFileSystemId,
        directory_path: &str,
        after: Option<&DfsDirectoryPageCursor>,
        max_entries: usize,
    ) -> Result<DfsDirectoryPage> {
        let (entries, next_cursor) = DfsMetadataBackend::child_objects_for_directory_page_entries(
            self,
            file_system_id,
            directory_path,
            after,
            max_entries,
        )?;
        Ok(DfsDirectoryPage {
            objects: entries.into_iter().map(|(object, _)| object).collect(),
            next_cursor,
        })
    }

    fn hardlinks_to_object(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
    ) -> Result<Vec<DfsObjectMetadata>> {
        let mut hardlinks = collect_metadata_pages(|cursor| {
            let page = self.hardlinks_to_object_page(
                file_system_id,
                target_object_id,
                cursor,
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            Ok((page.objects, page.next_cursor))
        })?;
        sort_hardlinks_by_path(&mut hardlinks);
        Ok(hardlinks)
    }

    fn hardlinks_to_object_page(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
        after: Option<&DfsObjectPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectPage> {
        if max_entries == 0 {
            return Ok(DfsObjectPage {
                objects: Vec::new(),
                next_cursor: None,
            });
        }
        let resume_shard_id = after.and_then(|cursor| cursor.shard_id.as_ref());
        let entries = collect_resumed_sharded_page_entries(
            &self.shards,
            after,
            resume_shard_id,
            max_entries,
            local_object_page_cursor,
            |_, slot, cursor, limit| {
                let backend = slot.backend()?;
                let page = backend.hardlinks_to_object_page(
                    file_system_id,
                    target_object_id,
                    cursor,
                    limit,
                )?;
                Ok((page.objects, page.next_cursor))
            },
            |shard_id, _, object| {
                if !self.object_is_listable_from_shard(&object, shard_id)? {
                    return Ok(None);
                }
                let cursor = object_page_cursor_with_shard(&object, shard_id);
                Ok(Some((object, cursor)))
            },
        )?;
        let (objects, next_cursor) = finish_cursor_page(entries, max_entries);
        Ok(DfsObjectPage {
            objects,
            next_cursor,
        })
    }

    fn hardlink_count_to_object(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
    ) -> Result<usize> {
        let mut object_ids = BTreeSet::<DfsObjectId>::new();
        for (shard_id, slot) in &self.shards {
            let backend = slot.backend()?;
            walk_metadata_pages(|cursor| {
                let page = backend.hardlinks_to_object_page(
                    file_system_id,
                    target_object_id,
                    cursor,
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?;
                for object in page.objects {
                    if self.object_is_listable_from_shard(&object, shard_id)? {
                        object_ids.insert(object.object_id);
                    }
                }
                Ok(page.next_cursor)
            })?;
        }
        Ok(object_ids.len())
    }

    fn first_hardlink_to_object(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
    ) -> Result<Option<DfsObjectMetadata>> {
        self.first_hardlink_to_object_matching(file_system_id, target_object_id, |_| true)
    }

    fn first_hardlink_to_object_outside_prefix(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
        prefix: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        let prefix = normalize_posix_path(prefix);
        self.first_hardlink_to_object_matching(file_system_id, target_object_id, |object| {
            !posix_path_is_at_or_under_prefix(&object.path, &prefix)
        })
    }

    fn version_pins_for_object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<Vec<DfsMetadataVersionPinRecord>> {
        let Some(shard_id) = self.shard_id_for_object(object_id)? else {
            return Ok(Vec::new());
        };
        self.shard_backend(&shard_id)?
            .version_pins_for_object(object_id)
    }

    fn version_pins_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsMetadataVersionPinPageCursor>,
        max_entries: usize,
    ) -> Result<DfsMetadataVersionPinPage> {
        if max_entries == 0 {
            return Ok(DfsMetadataVersionPinPage {
                pins: Vec::new(),
                next_cursor: None,
            });
        }
        let Some(shard_id) = self.shard_id_for_object(object_id)? else {
            return Ok(DfsMetadataVersionPinPage {
                pins: Vec::new(),
                next_cursor: None,
            });
        };
        let local_after = after.map(local_version_pin_page_cursor);
        let mut page = self
            .shard_backend(&shard_id)?
            .version_pins_for_object_page(object_id, local_after.as_ref(), max_entries)?;
        if let Some(cursor) = page.next_cursor.as_mut() {
            cursor.shard_id = Some(shard_id);
        }
        Ok(page)
    }

    fn cross_shard_pin_transfers_for_object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<Vec<DfsCrossShardPinTransferRecord>> {
        let shard_id = self.require_shard_id_for_object(object_id)?;
        self.shard_backend(&shard_id)?
            .cross_shard_pin_transfers_for_object(object_id)
    }

    fn capture_cross_shard_source_pin_fence(
        &mut self,
        object_id: &DfsObjectId,
    ) -> Result<DfsCrossShardVersionPinFence> {
        let shard_id = self.require_shard_id_for_object(object_id)?;
        self.shard_backend_mut(&shard_id)?
            .capture_cross_shard_source_pin_fence(object_id)
    }

    fn capture_source_adoption_source(
        &mut self,
        object_id: &DfsObjectId,
        source_path: &str,
    ) -> Result<crate::metadata_backend::DfsSourceAdoptionSourceCapture> {
        let actual_shard = self.require_shard_id_for_object(object_id)?;
        let planned = self
            .shard_backend(&actual_shard)?
            .get_object(object_id)?
            .ok_or_else(|| MetadataBackendError::UnknownObject {
                object_id: object_id.clone(),
            })?;
        let selected = self.select_shard_id_for_path(&planned.file_system_id, source_path)?;
        if selected != actual_shard || normalize_path(&planned.path) != normalize_path(source_path)
        {
            return Err(MetadataBackendError::Serde(
                "source capture object differs from actual current path routing".into(),
            ));
        }
        let captured = self
            .shard_backend_mut(&selected)?
            .capture_source_adoption_source_for_backend(object_id, source_path)?;
        if captured.family().source_object != planned
            || captured.scope().file_system_id() != &planned.file_system_id
            || captured.scope().shard_id() != &selected
        {
            return Err(MetadataBackendError::Serde(
                "source capture object or physical path routing changed while custody queued"
                    .into(),
            ));
        }
        Ok(captured)
    }

    fn capture_source_adoption_job(
        &mut self,
        job_id: &str,
    ) -> Result<crate::metadata_backend::DfsSourceAdoptionJobCapture> {
        let planned = self.get_namespace_job(job_id)?.ok_or_else(|| {
            MetadataBackendError::Serde("original coordinator has no actual running job".into())
        })?;
        if planned.status != DfsNamespaceJobStatus::Running {
            return Err(MetadataBackendError::Serde(
                "original coordinator job is not running".into(),
            ));
        }
        let shard = self.select_shard_id_for_path(&planned.file_system_id, &planned.root_path)?;
        let captured = self
            .shard_backend_mut(&shard)?
            .capture_source_adoption_job_for_backend(job_id)?;
        if captured.job() != &planned
            || captured.scope().file_system_id() != &planned.file_system_id
            || captured.scope().shard_id() != &shard
        {
            return Err(MetadataBackendError::Serde(
                "original coordinator job or physical path routing changed while custody queued"
                    .into(),
            ));
        }
        Ok(captured)
    }

    fn capture_source_adoption(
        &mut self,
        object_id: &DfsObjectId,
        source_path: &str,
        job_id: &str,
    ) -> Result<crate::metadata_backend::DfsSourceAdoptionCapture> {
        let authority_shard = self.require_shard_id_for_object(object_id)?;
        let fs = self
            .shard_backend(&authority_shard)?
            .get_object(object_id)?
            .ok_or_else(|| MetadataBackendError::UnknownObject {
                object_id: object_id.clone(),
            })?
            .file_system_id;
        let source_shard = self.select_shard_id_for_path(&fs, source_path)?;
        let planned_job = self.get_namespace_job(job_id)?.ok_or_else(|| {
            MetadataBackendError::Serde(
                "source adoption has no actual original coordinator job".into(),
            )
        })?;
        if planned_job.file_system_id != fs {
            return Err(MetadataBackendError::Serde(
                "source/job filesystem identity differs".into(),
            ));
        }
        let job_shard = self.select_shard_id_for_path(&fs, &planned_job.root_path)?;
        let source_scope = self
            .shard_backend(&source_shard)?
            .publication_owner_scope(&fs, &source_shard)?;
        let job_scope = self
            .shard_backend(&job_shard)?
            .publication_owner_scope(&fs, &job_shard)?;
        // All native physical roots use the same canonical owner-scope order.
        // Acquire each existing writer/fence once, including a distinct job root.
        let mut roots = BTreeMap::new();
        roots.insert(source_scope.clone(), source_shard.clone());
        roots.insert(job_scope.clone(), job_shard.clone());
        let mut held = BTreeMap::new();
        for (scope, shard) in roots {
            let custody = self
                .shard_backend_mut(&shard)?
                .acquire_source_adoption_writer(&fs)?;
            custody.validate_scope(&scope)?;
            held.insert(shard, custody);
        }
        let source_custody = held
            .remove(&source_shard)
            .expect("source physical root acquired");
        let family = self
            .shard_backend(&source_shard)?
            .source_adoption_family_while_held(&source_custody, object_id, source_path)?;
        let job_custody = if source_shard == job_shard {
            None
        } else {
            Some(
                held.remove(&job_shard)
                    .expect("original job physical root acquired"),
            )
        };
        let actual_job = self
            .shard_backend(&job_shard)?
            .source_adoption_job_while_held(
                job_custody.as_ref().unwrap_or(&source_custody),
                job_id,
            )?;
        if actual_job != planned_job {
            return Err(MetadataBackendError::Serde(
                "original coordinator job changed while custody queued".into(),
            ));
        }
        crate::metadata_backend::DfsSourceAdoptionCapture::from_held(
            source_custody,
            family,
            job_custody,
            actual_job,
        )
    }

    fn capture_cross_shard_source_family(
        &mut self,
        object_id: &DfsObjectId,
        source_path: &str,
    ) -> Result<DfsCrossShardSourceFamily> {
        let authority_shard_id = self.require_shard_id_for_object(object_id)?;
        let authority_object = self
            .shard_backend(&authority_shard_id)?
            .get_object(object_id)?
            .ok_or_else(|| MetadataBackendError::UnknownObject {
                object_id: object_id.clone(),
            })?;
        let source_shard_id =
            self.select_shard_id_for_path(&authority_object.file_system_id, source_path)?;
        let (_source_lock, bundle) =
            self.object_shard_migration_bundle(&source_shard_id, object_id)?;
        if bundle.object.file_system_id != authority_object.file_system_id
            || normalize_path(&bundle.object.path) != normalize_path(source_path)
        {
            return Err(MetadataBackendError::Serde(format!(
                "physical source shard {} does not contain object {} at requested path {}",
                source_shard_id.0, object_id, source_path
            )));
        }
        Ok(bundle.into())
    }

    fn get_version(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<Option<DfsObjectVersionRecord>> {
        let Some(shard_id) = self.shard_id_for_object(object_id)? else {
            return Ok(None);
        };
        self.shard_backend(&shard_id)?
            .get_version(object_id, version)
    }

    fn versions_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsObjectVersionRecord>> {
        collect_metadata_pages(|cursor| {
            let page =
                self.versions_for_object_page(object_id, cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            Ok((page.versions, page.next_cursor))
        })
    }

    fn versions_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsObjectVersionPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectVersionPage> {
        let Some(shard_id) = self.shard_id_for_object(object_id)? else {
            return Ok(DfsObjectVersionPage {
                versions: Vec::new(),
                next_cursor: None,
            });
        };
        let local_after = after.map(local_object_version_page_cursor);
        let page = self.shard_backend(&shard_id)?.versions_for_object_page(
            object_id,
            local_after.as_ref(),
            max_entries,
        )?;
        Ok(object_version_page_with_shard_cursor(page, &shard_id))
    }

    fn receipts_for_version(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<Vec<DfsReplicaReceiptRecord>> {
        collect_metadata_pages(|cursor| {
            let page = self.receipts_for_version_page(
                object_id,
                version,
                cursor,
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            Ok((page.receipts, page.next_cursor))
        })
    }

    fn receipts_for_version_page(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        after: Option<&DfsReplicaReceiptPageCursor>,
        max_entries: usize,
    ) -> Result<DfsReplicaReceiptPage> {
        let Some(shard_id) = self.shard_id_for_object(object_id)? else {
            return Ok(DfsReplicaReceiptPage {
                receipts: Vec::new(),
                next_cursor: None,
            });
        };
        let local_after = after.map(local_replica_receipt_page_cursor);
        let page = self.shard_backend(&shard_id)?.receipts_for_version_page(
            object_id,
            version,
            local_after.as_ref(),
            max_entries,
        )?;
        Ok(replica_receipt_page_with_shard_cursor(page, &shard_id))
    }

    fn receipt_slot_generation(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
    ) -> Result<u64> {
        let Some(shard_id) = self.shard_id_for_object(object_id)? else {
            return Ok(0);
        };
        self.shard_backend(&shard_id)?
            .receipt_slot_generation(object_id, version, replica_node_id)
    }

    fn has_current_placement_repair_owner(&self, object: &DfsObjectMetadata) -> Result<bool> {
        let shard_id = self.select_shard_id_for_path(&object.file_system_id, &object.path)?;
        self.shard_backend(&shard_id)?
            .has_current_placement_repair_owner(object)
    }

    fn object_write_intent_history(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<DfsObjectWriteIntentHistory> {
        if let Some(shard_id) = self.shard_id_for_object(object_id)? {
            return self
                .shard_backend(&shard_id)?
                .object_write_intent_history(object_id);
        }

        let mut intents = BTreeMap::<String, DfsWriteIntentRecord>::new();
        let mut receipts = BTreeMap::<String, DfsWriteIntentReceipt>::new();
        let mut retention = DfsWriteIntentReceiptRetentionState::default();
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            let history = backend.object_write_intent_history(object_id)?;
            retention.merge_conservative(history.retention);
            for intent in history.intents {
                if let Some(receipt) = receipts.get(&intent.intent_id) {
                    if !receipt.matches_intent(&intent) {
                        return Err(MetadataBackendError::Serde(format!(
                            "object {} has conflicting full and compact intent {} across metadata shards",
                            object_id, intent.intent_id
                        )));
                    }
                    continue;
                }
                if let Some(existing) = intents.get(&intent.intent_id)
                    && existing != &intent
                {
                    return Err(MetadataBackendError::Serde(format!(
                        "object {} has conflicting intent {} across metadata shards",
                        object_id, intent.intent_id
                    )));
                }
                intents.insert(intent.intent_id.clone(), intent);
            }
            for receipt in history.receipts {
                if let Some(intent) = intents.remove(&receipt.intent_id)
                    && !receipt.matches_intent(&intent)
                {
                    return Err(MetadataBackendError::Serde(format!(
                        "object {} has conflicting full and compact intent {} across metadata shards",
                        object_id, receipt.intent_id
                    )));
                }
                if let Some(existing) = receipts.get(&receipt.intent_id)
                    && existing != &receipt
                {
                    return Err(MetadataBackendError::Serde(format!(
                        "object {} has conflicting compact intent receipt {} across metadata shards",
                        object_id, receipt.intent_id
                    )));
                }
                receipts.insert(receipt.intent_id.clone(), receipt);
            }
        }
        Ok(DfsObjectWriteIntentHistory {
            intents: intents.into_values().collect(),
            receipts: receipts.into_values().collect(),
            retention,
        })
    }

    fn get_intent_receipt(
        &self,
        object_id: &DfsObjectId,
        intent_id: &str,
    ) -> Result<Option<DfsWriteIntentReceipt>> {
        if let Some(shard_id) = self.shard_id_for_object(object_id)? {
            return self
                .shard_backend(&shard_id)?
                .get_intent_receipt(object_id, intent_id);
        }
        let mut found = None;
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            let Some(receipt) = backend.get_intent_receipt(object_id, intent_id)? else {
                continue;
            };
            if found.as_ref().is_some_and(|existing| existing != &receipt) {
                return Err(MetadataBackendError::Serde(format!(
                    "object {} has conflicting compact intent receipt {} across metadata shards",
                    object_id, intent_id
                )));
            }
            found = Some(receipt);
        }
        Ok(found)
    }

    fn intent_receipt_retention_for_object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<DfsWriteIntentReceiptRetentionState> {
        if let Some(shard_id) = self.shard_id_for_object(object_id)? {
            return self
                .shard_backend(&shard_id)?
                .intent_receipt_retention_for_object(object_id);
        }
        let mut retention = DfsWriteIntentReceiptRetentionState::default();
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            retention.merge_conservative(backend.intent_receipt_retention_for_object(object_id)?);
        }
        Ok(retention)
    }

    fn get_intent(
        &self,
        object_id: &DfsObjectId,
        intent_id: &str,
    ) -> Result<Option<DfsWriteIntentRecord>> {
        if let Some(shard_id) = self.shard_id_for_object(object_id)? {
            return self
                .shard_backend(&shard_id)?
                .get_intent(object_id, intent_id);
        }
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            if let Some(intent) = backend.get_intent(object_id, intent_id)? {
                return Ok(Some(intent));
            }
        }
        Ok(None)
    }

    fn get_intent_on_shard(
        &self,
        shard_id: &DfsMetadataShardId,
        object_id: &DfsObjectId,
        intent_id: &str,
    ) -> Result<Option<DfsWriteIntentRecord>> {
        let Some(slot) = self.shards.get(shard_id) else {
            return Ok(None);
        };
        slot.backend()?
            .get_intent_on_shard(shard_id, object_id, intent_id)
    }

    fn get_receipt_on_shard(
        &self,
        shard_id: &DfsMetadataShardId,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
    ) -> Result<Option<DfsReplicaReceiptRecord>> {
        let Some(slot) = self.shards.get(shard_id) else {
            return Ok(None);
        };
        slot.backend()?
            .get_receipt_on_shard(shard_id, object_id, version, replica_node_id)
    }

    fn write_intent_counter_floor(&self) -> Result<u64> {
        self.shards.values().try_fold(0_u64, |floor, slot| {
            let backend = slot.backend()?;
            Ok(floor.max(backend.write_intent_counter_floor()?))
        })
    }

    fn intents_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsWriteIntentRecord>> {
        if let Some(shard_id) = self.shard_id_for_object(object_id)? {
            let backend = self.shard_backend(&shard_id)?;
            return collect_metadata_pages(|cursor| {
                let page = backend.intents_for_object_page(
                    object_id,
                    cursor,
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?;
                Ok((page.intents, page.next_cursor))
            });
        }
        let mut intents = BTreeMap::<String, DfsWriteIntentRecord>::new();
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            walk_metadata_pages(|cursor| {
                let page = backend.intents_for_object_page(
                    object_id,
                    cursor,
                    OBJECT_LOCAL_METADATA_PAGE_SIZE,
                )?;
                for intent in page.intents {
                    intents.entry(intent.intent_id.clone()).or_insert(intent);
                }
                Ok(page.next_cursor)
            })?;
        }
        Ok(intents.into_values().collect())
    }

    fn intents_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsWriteIntentPageCursor>,
        max_entries: usize,
    ) -> Result<DfsWriteIntentPage> {
        if max_entries == 0 {
            return Ok(DfsWriteIntentPage {
                intents: Vec::new(),
                next_cursor: None,
            });
        }
        if let Some(shard_id) = self.shard_id_for_object(object_id)? {
            let local_after = after.map(local_write_intent_page_cursor);
            let page = self.shard_backend(&shard_id)?.intents_for_object_page(
                object_id,
                local_after.as_ref(),
                max_entries,
            )?;
            return Ok(write_intent_page_with_shard_cursor(page, &shard_id));
        }

        let resume_shard_id = after.and_then(|cursor| cursor.shard_id.as_ref());
        let mut seen_intent_ids = BTreeSet::<String>::new();
        let entries = collect_resumed_sharded_page_entries(
            &self.shards,
            after,
            resume_shard_id,
            max_entries,
            local_write_intent_page_cursor,
            |_, slot, cursor, limit| {
                let backend = slot.backend()?;
                let page = backend.intents_for_object_page(object_id, cursor, limit)?;
                Ok((page.intents, page.next_cursor))
            },
            |shard_id, _, intent| {
                if !seen_intent_ids.insert(intent.intent_id.clone()) {
                    return Ok(None);
                }
                let cursor = write_intent_page_cursor_with_shard(
                    write_intent_page_cursor(&intent),
                    shard_id,
                );
                Ok(Some((intent, cursor)))
            },
        )?;
        let (intents, next_cursor) = finish_cursor_page(entries, max_entries);
        Ok(DfsWriteIntentPage {
            intents,
            next_cursor,
        })
    }

    fn intents_for_object_version_page(
        &self,
        object_id: &DfsObjectId,
        target_version: &DfsVersion,
        after: Option<&DfsWriteIntentPageCursor>,
        max_entries: usize,
    ) -> Result<DfsWriteIntentPage> {
        if max_entries == 0 {
            return Ok(DfsWriteIntentPage {
                intents: Vec::new(),
                next_cursor: None,
            });
        }
        if let Some(shard_id) = self.shard_id_for_object(object_id)? {
            let local_after = after.map(local_write_intent_page_cursor);
            let page = self
                .shard_backend(&shard_id)?
                .intents_for_object_version_page(
                    object_id,
                    target_version,
                    local_after.as_ref(),
                    max_entries,
                )?;
            return Ok(write_intent_page_with_shard_cursor(page, &shard_id));
        }

        let resume_shard_id = after.and_then(|cursor| cursor.shard_id.as_ref());
        let mut seen_intent_ids = BTreeSet::<String>::new();
        let entries = collect_resumed_sharded_page_entries(
            &self.shards,
            after,
            resume_shard_id,
            max_entries,
            local_write_intent_page_cursor,
            |_, slot, cursor, limit| {
                let backend = slot.backend()?;
                let page = backend.intents_for_object_version_page(
                    object_id,
                    target_version,
                    cursor,
                    limit,
                )?;
                Ok((page.intents, page.next_cursor))
            },
            |shard_id, _, intent| {
                if !self.intent_is_listable_from_shard(&intent, shard_id)? {
                    return Ok(None);
                }
                if !seen_intent_ids.insert(intent.intent_id.clone()) {
                    return Ok(None);
                }
                let cursor = write_intent_page_cursor_with_shard(
                    write_intent_page_cursor(&intent),
                    shard_id,
                );
                Ok(Some((intent, cursor)))
            },
        )?;
        let (intents, next_cursor) = finish_cursor_page(entries, max_entries);
        Ok(DfsWriteIntentPage {
            intents,
            next_cursor,
        })
    }

    fn write_intents_page(
        &self,
        after: Option<&DfsWriteIntentScanCursor>,
        max_entries: usize,
    ) -> Result<DfsWriteIntentScanPage> {
        if max_entries == 0 {
            return Ok(DfsWriteIntentScanPage {
                intents: Vec::new(),
                next_cursor: None,
            });
        }
        let mut seen = BTreeSet::<(DfsObjectId, String)>::new();
        let entries = collect_sharded_scan_entries(
            &self.shards,
            after,
            max_entries,
            |_, slot, cursor, limit| {
                let backend = slot.backend()?;
                let page = backend.write_intents_page(cursor, limit)?;
                Ok((page.intents, page.next_cursor))
            },
            |shard_id, _, intent| {
                if !self.intent_is_listable_from_shard(&intent, shard_id)? {
                    return Ok(None);
                }
                if !seen.insert((intent.object_id.clone(), intent.intent_id.clone())) {
                    return Ok(None);
                }
                let cursor = write_intent_scan_cursor(&intent);
                Ok(Some((intent, cursor)))
            },
        )?;
        let (intents, next_cursor) =
            finish_sorted_cursor_page(entries, max_entries, write_intent_scan_order);
        Ok(DfsWriteIntentScanPage {
            intents,
            next_cursor,
        })
    }

    fn write_intent_maintenance_page(
        &self,
        after: Option<&crate::DfsWriteIntentMaintenanceCursor>,
        max_records: usize,
    ) -> Result<crate::DfsWriteIntentMaintenancePage> {
        use std::ops::Bound::{Excluded, Included, Unbounded};
        if max_records == 0 {
            return Ok(crate::DfsWriteIntentMaintenancePage {
                next_cursor: after.cloned(),
                ..Default::default()
            });
        }
        let lower = after
            .map(|cursor| Included(cursor.shard_id.clone()))
            .unwrap_or(Unbounded);
        let Some((shard_id, slot)) = self.shards.range((lower, Unbounded)).next() else {
            return Ok(crate::DfsWriteIntentMaintenancePage::default());
        };
        let local_after = after.filter(|cursor| &cursor.shard_id == shard_id);
        let mut page = slot
            .backend()?
            .write_intent_maintenance_page(local_after, max_records)?;
        if page.next_cursor.is_none() {
            page.next_cursor = self
                .shards
                .range((Excluded(shard_id.clone()), Unbounded))
                .next()
                .map(|(next, _)| {
                    crate::DfsWriteIntentMaintenanceCursor::first_in_shard(next.clone())
                });
        }
        Ok(page)
    }

    fn write_intent_maintenance_candidate_is_listable(
        &self,
        shard_id: &DfsMetadataShardId,
        intent: &DfsWriteIntentRecord,
    ) -> Result<bool> {
        if !self.shards.contains_key(shard_id) || !write_intent_status_is_active(&intent.status) {
            return Ok(false);
        }
        self.active_intent_is_listable_from_shard(intent, shard_id)
    }

    fn write_intents_on_shard_page(
        &self,
        shard_id: &DfsMetadataShardId,
        after: Option<&DfsWriteIntentScanCursor>,
        max_entries: usize,
    ) -> Result<Option<DfsWriteIntentScanPage>> {
        let Some(backend) = self.shards.get(shard_id) else {
            return Ok(None);
        };
        backend
            .backend()?
            .write_intents_on_shard_page(shard_id, after, max_entries)
    }

    fn query_file_history_on_shard(
        &self,
        query: &crate::DfsMetadataShardQuery,
    ) -> Result<crate::DfsMetadataQueryResult> {
        if query.shard_id.0.is_empty() {
            return crate::file_history_query::query_backend_metadata(self, query);
        }
        if let crate::DfsMetadataSelection::SubtreeObjectPageV1 { path, after, limit } =
            &query.selection
        {
            return self.subtree_object_page_on_shard(query, path, after.as_ref(), *limit);
        }
        self.shard_backend(&query.shard_id)?
            .query_file_history_on_shard(query)
    }

    fn retained_revision_records_page(
        &self,
        namespace: Option<&str>,
        file_system_id: &DfsFileSystemId,
        object_id: Option<&DfsObjectId>,
        path_prefix: Option<&str>,
        after: Option<&DfsObjectVersionScanCursor>,
        max_records: usize,
    ) -> Result<DfsObjectVersionScanPage> {
        if max_records == 0 {
            return Ok(DfsObjectVersionScanPage {
                versions: Vec::new(),
                next_cursor: None,
            });
        }
        // Cursor advancement counts candidates, including an ineligible
        // physical source copy. Do not drain filtered pages to fill a result.
        let mut candidates = BTreeMap::<_, Option<DfsObjectVersionRecord>>::new();
        for (shard_id, slot) in &self.shards {
            let backend = slot.backend()?;
            let page = backend.retained_revision_records_page(
                namespace,
                file_system_id,
                object_id,
                path_prefix,
                after,
                max_records.saturating_add(1),
            )?;
            for version in page.versions {
                let selected = candidates
                    .entry((version.object_id.clone(), version.version.clone()))
                    .or_default();
                if selected.is_some() {
                    continue;
                }
                let Some(object) = backend.get_object(&version.object_id)? else {
                    continue;
                };
                if self.object_is_listable_from_shard(&object, shard_id)? {
                    *selected = Some(version);
                }
            }
        }
        let entries = candidates
            .into_iter()
            .take(max_records.saturating_add(1))
            .map(|((object_id, version), record)| {
                (record, DfsObjectVersionScanCursor { object_id, version })
            })
            .collect();
        let (versions, next_cursor) = finish_cursor_page(entries, max_records);
        Ok(DfsObjectVersionScanPage {
            versions: versions.into_iter().flatten().collect(),
            next_cursor,
        })
    }

    fn version_records_page(
        &self,
        after: Option<&DfsObjectVersionScanCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectVersionScanPage> {
        if max_entries == 0 {
            return Ok(DfsObjectVersionScanPage {
                versions: Vec::new(),
                next_cursor: None,
            });
        }
        let mut seen = BTreeSet::<(DfsObjectId, DfsVersion)>::new();
        let entries = collect_sharded_scan_entries(
            &self.shards,
            after,
            max_entries,
            |_, slot, cursor, limit| {
                let backend = slot.backend()?;
                let page = backend.version_records_page(cursor, limit)?;
                Ok((page.versions, page.next_cursor))
            },
            |shard_id, slot, version| {
                let backend = slot.backend()?;
                let Some(object) = backend.get_object(&version.object_id)? else {
                    return Ok(None);
                };
                if !self.object_is_listable_from_shard(&object, shard_id)? {
                    return Ok(None);
                }
                if !seen.insert((version.object_id.clone(), version.version.clone())) {
                    return Ok(None);
                }
                let cursor = object_version_scan_cursor(&version);
                Ok(Some((version, cursor)))
            },
        )?;
        let (versions, next_cursor) =
            finish_sorted_cursor_page(entries, max_entries, |left, right| {
                object_version_scan_key(left).cmp(&object_version_scan_key(right))
            });
        Ok(DfsObjectVersionScanPage {
            versions,
            next_cursor,
        })
    }

    fn receipt_records_page(
        &self,
        after: Option<&DfsReplicaReceiptScanCursor>,
        max_entries: usize,
    ) -> Result<DfsReplicaReceiptScanPage> {
        if max_entries == 0 {
            return Ok(DfsReplicaReceiptScanPage {
                receipts: Vec::new(),
                next_cursor: None,
            });
        }
        let mut seen = BTreeSet::<(DfsObjectId, DfsVersion, NodeId)>::new();
        let mut authoritative_shards = BTreeMap::<
            (DfsFileSystemId, DfsObjectId, DfsVersion, Option<String>),
            Option<DfsMetadataShardId>,
        >::new();
        let entries = collect_sharded_scan_entries(
            &self.shards,
            after,
            max_entries,
            |_, slot, cursor, limit| {
                let backend = slot.backend()?;
                let page = backend.receipt_records_page(cursor, limit)?;
                Ok((page.receipts, page.next_cursor))
            },
            |shard_id, _, receipt| {
                let receipt_provenance = (
                    receipt.receipt.file_system_id.clone(),
                    receipt.receipt.object_id.clone(),
                    receipt.receipt.committed_version.clone(),
                    receipt.content_hash.clone(),
                );
                let authoritative_shard_id =
                    if let Some(shard_id) = authoritative_shards.get(&receipt_provenance) {
                        shard_id.clone()
                    } else {
                        let shard_id = self.shard_id_for_receipt_scan_record(&receipt)?;
                        authoritative_shards.insert(receipt_provenance, shard_id.clone());
                        shard_id
                    };
                if authoritative_shard_id.as_ref() != Some(shard_id) {
                    return Ok(None);
                }
                let key = (
                    receipt.receipt.object_id.clone(),
                    receipt.receipt.committed_version.clone(),
                    receipt.replica_node_id.clone(),
                );
                if !seen.insert(key) {
                    return Ok(None);
                }
                let cursor = replica_receipt_scan_cursor(&receipt);
                Ok(Some((receipt, cursor)))
            },
        )?;
        let (receipts, next_cursor) =
            finish_sorted_cursor_page(entries, max_entries, |left, right| {
                replica_receipt_scan_key(left).cmp(&replica_receipt_scan_key(right))
            });
        Ok(DfsReplicaReceiptScanPage {
            receipts,
            next_cursor,
        })
    }

    fn tombstone_compaction_projection(
        &self,
        options: DfsTombstoneCompactionOptions,
    ) -> Result<DfsTombstoneCompactionReport> {
        let mut report = DfsTombstoneCompactionReport::for_options(&options);
        let now = options
            .now_unix_nanos
            .unwrap_or_else(metadata_now_unix_nanos);
        for (shard_id, slot) in &self.shards {
            if !self.shard_is_local_authoritative_primary(shard_id)
                || self.metadata_shard_authoritative_writes_frozen(shard_id)?
            {
                continue;
            }
            let backend = slot.backend()?;
            let tables = backend.current_tables_snapshot()?;
            report.merge(plan_tombstone_compaction(&tables, &options, now).report);
        }
        Ok(report)
    }

    fn metadata_history_gc_projection(
        &self,
        options: DfsMetadataHistoryGcOptions,
    ) -> Result<DfsMetadataHistoryGcReport> {
        let mut report = DfsMetadataHistoryGcReport::for_options(&options);
        for (shard_id, slot) in &self.shards {
            let backend = slot.backend()?;
            let mut shard_report = backend.metadata_history_gc_projection(options.clone())?;
            if self.shard_is_local_authoritative_primary(shard_id)
                && !self.metadata_shard_authoritative_writes_frozen(shard_id)?
            {
                report.merge(shard_report);
            } else {
                // Replica and handoff-frozen tables still consume local disk and
                // must remain visible in the per-shard inventory. Do not merge
                // their eligible/deadline counters into schedulable work:
                // authoritative deletion is deliberately fenced on these
                // shards and advertising it would create a reactive busy loop.
                for shard in &mut shard_report.shards {
                    shard.collection_authorized = false;
                }
                report.shards.extend(shard_report.shards);
            }
        }
        Ok(report)
    }

    fn plan_metadata_history_gc(
        &mut self,
        options: DfsMetadataHistoryGcOptions,
    ) -> Result<crate::metadata_api::DfsMetadataHistoryEffectsPlan> {
        let mut report = DfsMetadataHistoryGcReport::for_options(&options);
        let mut effects_by_scope = BTreeMap::new();
        // Only a shard's primary applies its history effects.
        let ids = self
            .shards
            .keys()
            .filter(|shard_id| self.shard_is_local_authoritative_primary(shard_id))
            .cloned()
            .collect::<Vec<_>>();
        for shard in ids {
            let plan = self
                .shard_backend_mut(&shard)?
                .plan_metadata_history_gc(options.clone())?;
            report.merge(plan.report);
            for (scope, effects) in plan.effects_by_scope {
                effects_by_scope
                    .entry(scope)
                    .or_insert_with(Vec::new)
                    .extend(effects);
            }
        }
        Ok(crate::metadata_api::DfsMetadataHistoryEffectsPlan {
            report,
            effects_by_scope,
        })
    }

    fn metadata_content_protection(&self) -> Result<DfsMetadataContentProtection> {
        let mut protection = DfsMetadataContentProtection::default();
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            protection.merge(backend.metadata_content_protection()?);
        }
        Ok(protection)
    }

    fn content_root_catalog_page(
        &mut self,
        request: &DfsContentRootCatalogPageRequest,
    ) -> Result<DfsContentRootCatalogPage> {
        request.validate()?;
        let observed_identity = self
            .shard_configs
            .get(&request.identity.shard_id.0)
            .map(DfsContentRootCatalogShardIdentity::from_config);
        let Some(observed_identity) = observed_identity else {
            return Ok(DfsContentRootCatalogPage::incomplete_for_request(
                request,
                DfsContentRootCatalogIncompleteReason::UnknownShard,
                metadata_now_unix_nanos(),
            ));
        };
        if observed_identity != request.identity {
            return Ok(DfsContentRootCatalogPage::incomplete_for_request(
                request,
                DfsContentRootCatalogIncompleteReason::AuthorityChanged {
                    observed: Some(observed_identity),
                },
                metadata_now_unix_nanos(),
            ));
        }
        self.shard_backend_mut(&request.identity.shard_id)?
            .content_root_catalog_page(request)
    }

    fn compact_tombstones(
        &mut self,
        options: DfsTombstoneCompactionOptions,
    ) -> Result<DfsTombstoneCompactionReport> {
        let mut report = DfsTombstoneCompactionReport::for_options(&options);
        let shard_ids = self.shard_ids();
        let mut compacted_object_ids = Vec::new();
        for shard_id in shard_ids {
            if !self.shard_is_local_authoritative_primary(&shard_id)
                || self.metadata_shard_authoritative_writes_frozen(&shard_id)?
            {
                // A retiring primary deliberately keeps its local shard
                // frozen after activating the successor generation. That
                // shard remains available for replica ingest, but optional
                // lifecycle mutation must leave it untouched. Independent
                // active shards must still reach metadata publication later
                // in the same maintenance tick.
                continue;
            }
            let mut shard_options = options.clone();
            if !options.dry_run
                && let Some(max_compactions) = options.max_compactions
            {
                let remaining = max_compactions.saturating_sub(report.compacted_tombstone_count);
                if remaining == 0 {
                    report.max_compactions_reached = true;
                    break;
                }
                shard_options.max_compactions = Some(remaining);
            }
            let compacted = self
                .shard_backend_mut(&shard_id)?
                .compact_tombstones_with_ids(shard_options);
            let (shard_report, shard_compacted_object_ids) = match compacted {
                Ok(compacted) => compacted,
                Err(MetadataBackendError::MembershipHandoffFrozen { .. }) => {
                    // A handoff can install its freeze after the read above.
                    // Preserve the same safe skip across that race.
                    continue;
                }
                Err(error) => return Err(error),
            };
            report.merge(shard_report);
            compacted_object_ids.extend(shard_compacted_object_ids);
        }
        for object_id in compacted_object_ids {
            self.remove_object_shard_index(&object_id)?;
        }
        Ok(report)
    }

    fn get_namespace_request(
        &self,
        requester_node_id: &NodeId,
        request_id: &str,
    ) -> Result<Option<DfsNamespaceRequestRecord>> {
        let mut found = None;
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            if let Some(request) = backend.get_namespace_request(requester_node_id, request_id)? {
                if found.is_some() {
                    return Err(MetadataBackendError::Serde(format!(
                        "namespace request {requester_node_id}/{request_id} exists on multiple local shards"
                    )));
                }
                found = Some(request);
            }
        }
        Ok(found)
    }

    fn get_namespace_request_receipt(
        &self,
        requester_node_id: &NodeId,
        request_id: &str,
    ) -> Result<Option<DfsNamespaceRequestReceipt>> {
        let mut found = None;
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            if let Some(receipt) =
                backend.get_namespace_request_receipt(requester_node_id, request_id)?
            {
                if found.is_some() {
                    return Err(MetadataBackendError::Serde(format!(
                        "namespace request receipt {requester_node_id}/{request_id} exists on multiple local shards"
                    )));
                }
                found = Some(receipt);
            }
        }
        Ok(found)
    }

    fn namespace_request_history_expired_on_shard(
        &self,
        shard_id: &DfsMetadataShardId,
        requester_node_id: &NodeId,
        request_id: &str,
    ) -> Result<bool> {
        // Validate the selected current authority, but consult every retained
        // local shard history. A prefix topology change can select a different
        // shard after an exact receipt was evicted; admitting against only the
        // new shard's floor would make the old request fresh again. Current
        // remote primaries are represented here by their local replica state,
        // and same-node topology changes retain both shard histories.
        self.shard_backend(shard_id)?;
        for (history_shard_id, slot) in &self.shards {
            let backend = slot.backend()?;
            if backend.namespace_request_history_expired_on_shard(
                history_shard_id,
                requester_node_id,
                request_id,
            )? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn namespace_request_admission_snapshot(
        &mut self,
        shard_id: &DfsMetadataShardId,
        requester_node_id: &NodeId,
        request_id: &str,
    ) -> Result<DfsNamespaceRequestAdmissionSnapshot> {
        self.shard_backend(shard_id)?;
        let mut request = None;
        let mut receipt = None;
        let mut prepared = None;
        let mut history_expired = false;
        for (history_shard_id, slot) in &mut self.shards {
            let backend = slot.backend_mut()?;
            let local = backend.namespace_request_admission_snapshot(
                history_shard_id,
                requester_node_id,
                request_id,
            )?;
            if let Some(found) = local.request {
                if request.is_some() {
                    return Err(MetadataBackendError::Serde(format!(
                        "namespace request {requester_node_id}/{request_id} exists on multiple local shards"
                    )));
                }
                request = Some(found);
            }
            if let Some(found) = local.receipt {
                if receipt.is_some() {
                    return Err(MetadataBackendError::Serde(format!(
                        "namespace request receipt {requester_node_id}/{request_id} exists on multiple local shards"
                    )));
                }
                receipt = Some(found);
            }
            if let Some(found) = local.prepared {
                if prepared.is_some() {
                    return Err(MetadataBackendError::Serde(format!(
                        "pending namespace request {requester_node_id}/{request_id} owns prepared transactions on multiple local shards"
                    )));
                }
                prepared = Some(found);
            }
            history_expired |= local.history_expired;
        }
        if request.is_some() || receipt.is_some() || prepared.is_some() {
            history_expired = false;
        }
        Ok(DfsNamespaceRequestAdmissionSnapshot {
            request,
            receipt,
            prepared,
            history_expired,
        })
    }

    fn list_namespace_requests(&self) -> Result<Vec<DfsNamespaceRequestRecord>> {
        let mut requests = BTreeMap::new();
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            for request in backend.list_namespace_requests()? {
                let key = (
                    request.requester_node_id.clone(),
                    request.request_id.clone(),
                );
                if requests.insert(key, request).is_some() {
                    return Err(MetadataBackendError::Serde(
                        "namespace request exists on multiple local shards".to_string(),
                    ));
                }
            }
        }
        Ok(requests.into_values().collect())
    }

    fn list_pending_namespace_requests_page(
        &self,
        after: Option<&DfsPendingNamespaceRequestPageCursor>,
        max_entries: usize,
    ) -> Result<DfsPendingNamespaceRequestPage> {
        if max_entries == 0 {
            return Ok(DfsPendingNamespaceRequestPage {
                requests: Vec::new(),
                next_cursor: None,
            });
        }
        let mut requests_by_identity =
            BTreeMap::<(NodeId, String), DfsNamespaceRequestRecord>::new();
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            let page = backend
                .list_pending_namespace_requests_page(after, max_entries.saturating_add(1))?;
            for request in page.requests {
                let request_key = (
                    request.requester_node_id.clone(),
                    request.request_id.clone(),
                );
                if requests_by_identity.insert(request_key, request).is_some() {
                    return Err(MetadataBackendError::Serde(
                        "pending namespace request exists on multiple local shards".to_string(),
                    ));
                }
            }
        }
        let entries = requests_by_identity
            .into_values()
            .map(|request| {
                let cursor = DfsPendingNamespaceRequestPageCursor {
                    updated_at: request.updated_at,
                    requester_node_id: request.requester_node_id.clone(),
                    request_id: request.request_id.clone(),
                };
                (request, cursor)
            })
            .collect::<Vec<_>>();
        let (requests, next_cursor) =
            finish_sorted_cursor_page(entries, max_entries, |left, right| {
                (left.updated_at, &left.requester_node_id, &left.request_id).cmp(&(
                    right.updated_at,
                    &right.requester_node_id,
                    &right.request_id,
                ))
            });
        Ok(DfsPendingNamespaceRequestPage {
            requests,
            next_cursor,
        })
    }

    fn get_namespace_transaction_receipt(
        &self,
        transaction_id: &str,
    ) -> Result<Option<DfsNamespaceTransactionReceipt>> {
        let mut found = None;
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            if let Some(receipt) = backend.get_namespace_transaction_receipt(transaction_id)? {
                if found
                    .as_ref()
                    .is_some_and(|existing: &DfsNamespaceTransactionReceipt| {
                        !existing.same_outcome_identity(&receipt)
                    })
                {
                    return Err(MetadataBackendError::Serde(
                        "transaction receipt has conflicting local shard identities".into(),
                    ));
                }
                found = Some(match found {
                    Some(mut existing) => {
                        existing.retained_at = existing.retained_at.min(receipt.retained_at);
                        existing
                    }
                    None => receipt,
                });
            }
        }
        Ok(found)
    }

    fn content_reference_outcomes_for_object_page(
        &self,
        fs: &DfsFileSystemId,
        object: &DfsObjectId,
        after: Option<&str>,
        limit: usize,
    ) -> Result<crate::DfsContentReferenceOutcomePage> {
        let shard = self.require_shard_id_for_object(object)?;
        self.shard_backend(&shard)?
            .content_reference_outcomes_for_object_page(fs, object, after, limit)
    }

    fn content_reference_outcome_retention(
        &self,
        fs: &DfsFileSystemId,
        object: &DfsObjectId,
    ) -> Result<Option<crate::DfsContentReferenceOutcomeRetention>> {
        let shard = self.require_shard_id_for_object(object)?;
        self.shard_backend(&shard)?
            .content_reference_outcome_retention(fs, object)
    }

    fn get_namespace_job(&self, job_id: &str) -> Result<Option<DfsNamespaceJobRecord>> {
        let mut found = None;
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            if let Some(job) = backend.get_namespace_job(job_id)? {
                if found.is_some() {
                    return Err(MetadataBackendError::Serde(format!(
                        "namespace job {job_id} exists on multiple local shards"
                    )));
                }
                found = Some(job);
            }
        }
        Ok(found)
    }

    fn get_namespace_job_receipt(&self, job_id: &str) -> Result<Option<DfsNamespaceJobReceipt>> {
        let mut found = None;
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            if let Some(receipt) = backend.get_namespace_job_receipt(job_id)? {
                if found.is_some() {
                    return Err(MetadataBackendError::Serde(format!(
                        "namespace job receipt {job_id} exists on multiple local shards"
                    )));
                }
                found = Some(receipt);
            }
        }
        Ok(found)
    }

    fn completed_namespace_job_receipt_for_paths_since(
        &self,
        file_system_id: &DfsFileSystemId,
        root_path: &str,
        target_path: &str,
        kinds: &[DfsNamespaceJobKind],
        created_at: time::OffsetDateTime,
        origin: &DfsNamespaceJobOrigin,
    ) -> Result<Option<DfsNamespaceJobReceipt>> {
        let mut found = None;
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            let Some(receipt) = backend.completed_namespace_job_receipt_for_paths_since(
                file_system_id,
                root_path,
                target_path,
                kinds,
                created_at,
                origin,
            )?
            else {
                continue;
            };
            if found.is_some() {
                return Err(MetadataBackendError::Serde(format!(
                    "completed namespace job receipt for {}/{}/{} exists on multiple local shards",
                    origin.requester_node_id, origin.request_id, origin.operation_id
                )));
            }
            found = Some(receipt);
        }
        Ok(found)
    }

    fn list_namespace_jobs(&self) -> Result<Vec<DfsNamespaceJobRecord>> {
        let mut jobs = BTreeMap::<String, DfsNamespaceJobRecord>::new();
        walk_metadata_pages(|cursor| {
            let page = self.list_namespace_jobs_page(cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            for job in page.jobs {
                jobs.insert(job.job_id.clone(), job);
            }
            Ok(page.next_cursor)
        })?;
        Ok(jobs.into_values().collect())
    }

    fn list_namespace_jobs_page(
        &self,
        after: Option<&DfsNamespaceJobPageCursor>,
        max_entries: usize,
    ) -> Result<DfsNamespaceJobPage> {
        if max_entries == 0 {
            return Ok(DfsNamespaceJobPage {
                jobs: Vec::new(),
                next_cursor: None,
            });
        }
        let mut merged = BTreeMap::<String, DfsNamespaceJobRecord>::new();
        let mut shard_has_more = false;
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            let page = backend.list_namespace_jobs_page(after, max_entries.saturating_add(1))?;
            shard_has_more |= page.next_cursor.is_some();
            for job in page.jobs {
                merged.insert(job.job_id.clone(), job);
            }
        }
        let mut jobs = merged.into_values().collect::<Vec<_>>();
        let has_more = jobs.len() > max_entries || shard_has_more;
        if jobs.len() > max_entries {
            jobs.truncate(max_entries);
        }
        let next_cursor = if has_more {
            jobs.last().map(|job| DfsNamespaceJobPageCursor {
                job_id: job.job_id.clone(),
            })
        } else {
            None
        };
        Ok(DfsNamespaceJobPage { jobs, next_cursor })
    }

    fn metadata_summary(&self) -> Result<DfsMetadataSummary> {
        let mut summary = DfsMetadataSummary::default();
        let mut file_system_ids = BTreeSet::new();
        for (shard_id, slot) in &self.shards {
            // A slot of a shard this node does not replicate is not kept
            // current and is not part of its inventory.
            if !self.shard_is_locally_replicated(shard_id) {
                continue;
            }
            let backend = slot.backend()?;
            let tables = backend.current_tables_snapshot()?;
            file_system_ids.extend(tables.file_systems.keys().cloned());
            let shard_summary = metadata_summary_from_tables(&tables, |object| {
                self.object_is_listable_from_shard(object, shard_id)
            })?;
            accumulate_metadata_summary_detail_counts(&mut summary, &shard_summary);
        }
        summary.file_system_count = file_system_ids.len();
        Ok(summary)
    }

    fn metadata_summary_for_local_primary_shards(&self) -> Result<DfsMetadataSummary> {
        let mut summary = DfsMetadataSummary::default();
        let mut file_system_ids = BTreeSet::new();
        for (shard_id, slot) in &self.shards {
            if !self.shard_is_local_authoritative_primary(shard_id) {
                continue;
            }
            if let Some(config) = self.shard_configs.get(&shard_id.0) {
                file_system_ids.insert(config.file_system_id.clone());
            }
            let backend = slot.backend()?;
            let tables = backend.current_tables_snapshot()?;
            let shard_summary = metadata_summary_from_tables(&tables, |object| {
                self.object_is_listable_from_shard(object, shard_id)
            })?;
            accumulate_metadata_summary_detail_counts(&mut summary, &shard_summary);
        }
        summary.file_system_count = file_system_ids.len();
        Ok(summary)
    }

    fn metadata_status_summaries(&self) -> Result<(DfsMetadataSummary, DfsMetadataSummary)> {
        let mut summary = DfsMetadataSummary::default();
        let mut primary_summary = DfsMetadataSummary::default();
        let mut file_system_ids = BTreeSet::new();
        let mut primary_file_system_ids = BTreeSet::new();
        for (shard_id, slot) in &self.shards {
            let backend = slot.backend()?;
            let shard_summary = backend.resident_metadata_status_summary();
            if shard_summary.file_system_count > 0
                && let Some(config) = self.shard_configs.get(&shard_id.0)
            {
                file_system_ids.insert(config.file_system_id.clone());
            }
            accumulate_metadata_summary_detail_counts(&mut summary, &shard_summary);
            if self.shard_is_local_authoritative_primary(shard_id) {
                if let Some(config) = self.shard_configs.get(&shard_id.0) {
                    // Primary-owned operational status counts the configured
                    // filesystem even before its first metadata object is
                    // materialized. Keep this identical to
                    // `metadata_summary_for_local_primary_shards` without
                    // reopening or scanning the shard tables.
                    primary_file_system_ids.insert(config.file_system_id.clone());
                }
                accumulate_metadata_summary_detail_counts(&mut primary_summary, &shard_summary);
            }
        }
        summary.file_system_count = file_system_ids.len();
        primary_summary.file_system_count = primary_file_system_ids.len();
        if !self.fallback_routing_indexes.is_empty()
            && (summary.object_count > 0
                || summary.pending_repair_count > 0
                || summary.cross_shard_namespace_lock_count > 0)
        {
            let generation_count = self.fallback_routing_indexes.len();
            let omission = if summary.cross_shard_namespace_lock_count == 0 {
                DfsOperatorStatusOmission::historical_routing_topology_with_cross_shard_lock_zero_proof(
                    generation_count,
                )
            } else {
                DfsOperatorStatusOmission::historical_routing_topology(generation_count)
            };
            return Err(MetadataBackendError::StatusOmitted { omission });
        }
        Ok((summary, primary_summary))
    }

    fn metadata_summary_for_file_system(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<DfsMetadataSummary>> {
        let Some(_) = self.get_file_system(file_system_id)? else {
            return Ok(None);
        };
        let mut summary = DfsMetadataSummary {
            file_system_count: 1,
            ..DfsMetadataSummary::default()
        };
        for (shard_id, slot) in &self.shards {
            let backend = slot.backend()?;
            let tables = backend.current_tables_snapshot()?;
            let Some(shard_summary) =
                metadata_summary_for_file_system_from_tables(&tables, file_system_id, |object| {
                    self.object_is_listable_from_shard(object, shard_id)
                })?
            else {
                continue;
            };
            accumulate_metadata_summary_detail_counts(&mut summary, &shard_summary);
        }
        Ok(Some(summary))
    }

    fn journal_since(&self, after_sequence: u64) -> Result<Vec<DfsMetadataJournalEntry>> {
        let mut entries = Vec::new();
        for slot in self.shards.values() {
            let backend = slot.backend()?;
            entries.extend(backend.journal_since(after_sequence)?);
        }
        entries.sort_by(|left, right| {
            left.sequence
                .cmp(&right.sequence)
                .then_with(|| left.shard_id.cmp(&right.shard_id))
        });
        Ok(entries)
    }

    fn journal_batch_for_shard_since_limited(
        &self,
        shard_id: &DfsMetadataShardId,
        after_sequence: u64,
        max_entries: usize,
        max_encoded_bytes: usize,
    ) -> Result<Option<DfsMetadataJournalBatch>> {
        self.shard_backend(shard_id)?.journal_batch_since_limited(
            after_sequence,
            max_entries,
            max_encoded_bytes,
        )
    }

    fn journal_frames_for_batch(
        &self,
        batch: &DfsMetadataJournalBatch,
    ) -> Result<Vec<DfsMetadataJournalFrameRecord>> {
        DfsMetadataBackend::journal_frames_for_batch(self.shard_backend(&batch.shard_id)?, batch)
    }

    fn export_shard_snapshot_image(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Option<crate::DfsMetadataSnapshotImage>> {
        Ok(Some(self.shard_backend(shard_id)?.export_snapshot_image()?))
    }

    fn export_shard_table_snapshot(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataTableSnapshot>> {
        Ok(Some(LocalSobjMetadataBackend::export_table_snapshot(
            self.shard_backend(shard_id)?,
        )))
    }

    fn metadata_prefix_proof(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataPrefixProof>> {
        self.shard_backend(shard_id)?
            .metadata_prefix_proof()
            .map(Some)
    }

    fn subscribe_metadata_replica_progress(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Option<crate::DfsMetadataReplicaObserver>> {
        self.shard_backend(shard_id)?
            .subscribe_metadata_replica_progress(shard_id)
    }

    fn begin_replica_projection_read(
        &mut self,
        installed: &crate::DfsMetadataReplicaObservation,
    ) -> Result<Option<DfsMetadataPrefixProof>> {
        self.shard_backend_mut(&installed.shard_id)?
            .begin_replica_projection_read(installed)
    }

    fn end_replica_projection_read(&mut self, shard_id: &DfsMetadataShardId) {
        if let Ok(backend) = self.shard_backend_mut(shard_id) {
            backend.end_replica_projection_read(shard_id);
        }
    }

    fn replica_projection_namespace_jobs_page(
        &self,
        shard_id: &DfsMetadataShardId,
        after: Option<&DfsNamespaceJobPageCursor>,
        max_entries: usize,
    ) -> Result<DfsNamespaceJobPage> {
        self.shard_backend(shard_id)?
            .replica_projection_namespace_jobs_page(shard_id, after, max_entries)
    }

    fn refresh_metadata_shard_durable_view(&mut self, shard_id: &DfsMetadataShardId) -> Result<()> {
        self.refresh_metadata_shard_durable_view_and_reconcile(shard_id, true)?;
        Ok(())
    }

    fn refresh_metadata_shard_durable_view_if_changed(
        &mut self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<bool> {
        self.refresh_metadata_shard_durable_view_and_reconcile(shard_id, false)
    }

    fn refresh_metadata_change_feed_view(&mut self, shard_id: &DfsMetadataShardId) -> Result<()> {
        self.shard_backend_mut(shard_id)?
            .refresh_metadata_shard_durable_view(shard_id)
    }

    fn metadata_shard_write_freeze(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataShardWriteFreeze>> {
        self.shard_backend(shard_id)?
            .metadata_shard_write_freeze(shard_id)
    }

    fn metadata_shard_authoritative_writes_frozen(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<bool> {
        // Retired replicas deliberately retain completed handoff markers as
        // durable ingest fences. Those markers do not describe this node's
        // authoritative-write availability once another node is primary.
        // The shard-local backend still exposes and enforces the marker for
        // replica ingest; only the node-scoped authoritative projection is
        // primary-specific.
        if !self.shard_is_local_authoritative_primary(shard_id) {
            return Ok(false);
        }
        self.shard_backend(shard_id)?
            .metadata_shard_authoritative_writes_frozen(shard_id)
    }

    fn metadata_shard_instance_writes_frozen(&self, shard_id: &DfsMetadataShardId) -> Result<bool> {
        let Some(slot) = self.shards.get(shard_id) else {
            return Ok(false);
        };
        slot.backend()?
            .metadata_shard_authoritative_writes_frozen(shard_id)
    }

    fn freeze_metadata_shard_for_membership_handoff(
        &mut self,
        shard_id: &DfsMetadataShardId,
        handoff_id: &str,
        retiring_epoch: u64,
    ) -> Result<Option<DfsMetadataShardWriteFreeze>> {
        self.shard_backend_mut(shard_id)?
            .freeze_metadata_shard_for_membership_handoff(shard_id, handoff_id, retiring_epoch)
    }

    fn bind_metadata_shard_repair_handoff_cut(
        &mut self,
        shard_id: &DfsMetadataShardId,
        cut: &crate::DfsRepairHandoffCut,
    ) -> Result<()> {
        self.shard_backend_mut(shard_id)?
            .bind_metadata_shard_repair_handoff_cut(shard_id, cut)
    }

    fn mark_metadata_shard_membership_handoff_activated(
        &mut self,
        shard_id: &DfsMetadataShardId,
        handoff_id: &str,
        activated_epoch: u64,
    ) -> Result<bool> {
        self.shard_backend_mut(shard_id)?
            .mark_metadata_shard_membership_handoff_activated(shard_id, handoff_id, activated_epoch)
    }

    fn clear_metadata_shard_membership_handoff_freeze(
        &mut self,
        shard_id: &DfsMetadataShardId,
        handoff_id: &str,
        active_shard: &DfsMetadataShardConfig,
        checked_prefix: &DfsMetadataPrefixProof,
    ) -> Result<bool> {
        if active_shard.shard_id != shard_id.0 {
            return Err(MetadataBackendError::Serde(format!(
                "active metadata shard config {} does not match rebound shard {}",
                active_shard.shard_id, shard_id.0
            )));
        }
        let previous = self
            .shard_configs
            .get(&shard_id.0)
            .cloned()
            .ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "cannot rebind unconfigured metadata shard {}",
                    shard_id.0
                ))
            })?;
        if previous.file_system_id != active_shard.file_system_id
            || previous.assignment != active_shard.assignment
            || previous.default_policy != active_shard.default_policy
        {
            return Err(MetadataBackendError::Serde(format!(
                "metadata shard {} rebind changes immutable routing or policy configuration",
                shard_id.0
            )));
        }

        self.shard_configs
            .insert(shard_id.0.clone(), active_shard.clone());
        let rebound = match self.shard_backend_mut(shard_id) {
            Ok(backend) => backend.clear_metadata_shard_membership_handoff_freeze(
                shard_id,
                handoff_id,
                active_shard,
                checked_prefix,
            ),
            Err(error) => Err(error),
        };
        match rebound {
            Ok(true) => Ok(true),
            Ok(false) => {
                self.shard_configs.insert(shard_id.0.clone(), previous);
                Ok(false)
            }
            Err(error) => {
                self.shard_configs.insert(shard_id.0.clone(), previous);
                Err(error)
            }
        }
    }

    fn export_table_snapshot(&self) -> Result<DfsMetadataTableSnapshot> {
        if self.shards.len() != 1 {
            return Err(MetadataBackendError::Serde(
                "a current snapshot must name one physical shard; use export_shard_table_snapshot"
                    .into(),
            ));
        }
        let slot = self.shards.values().next().expect("one physical shard");
        Ok(LocalSobjMetadataBackend::export_table_snapshot(
            slot.backend()?,
        ))
    }

    fn observe_metadata_replication_cursors(
        &self,
        shard_id: &DfsMetadataShardId,
        cursors: &[(NodeId, u64)],
    ) {
        // The author already opened its shard to persist the prepare. Never
        // open or lock a cold backend merely to retain this optional hint.
        if let Some(backend) = self
            .shards
            .get(shard_id)
            .and_then(|slot| slot.backend.get())
        {
            backend.observe_metadata_replication_cursors(shard_id, cursors);
        }
    }

    fn metadata_replication_cursor(
        &self,
        target_node_id: &NodeId,
        shard_id: &DfsMetadataShardId,
    ) -> Result<u64> {
        self.shard_backend(shard_id)?
            .metadata_replication_cursor(target_node_id, shard_id)
    }

    fn put_metadata_replication_cursor(
        &mut self,
        target_node_id: &NodeId,
        shard_id: &DfsMetadataShardId,
        next_after_sequence: u64,
    ) -> Result<()> {
        self.shard_backend_mut(shard_id)?
            .put_metadata_replication_cursor(target_node_id, shard_id, next_after_sequence)
    }

    fn metadata_prefix_ack_floor(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataPrefixAckFloor>> {
        self.shard_backend(shard_id)?
            .metadata_prefix_ack_floor(shard_id)
    }

    fn put_metadata_prefix_ack_floor(&mut self, floor: &DfsMetadataPrefixAckFloor) -> Result<()> {
        self.shard_backend_mut(&floor.shard_id)?
            .put_metadata_prefix_ack_floor(floor)
    }

    fn metadata_authority_ack_floor(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataAuthorityAckFloor>> {
        self.shard_backend(shard_id)?
            .metadata_authority_ack_floor(shard_id)
    }

    fn put_metadata_authority_ack_floor(
        &mut self,
        floor: &DfsMetadataAuthorityAckFloor,
    ) -> Result<()> {
        self.shard_backend_mut(&floor.shard_id)?
            .put_metadata_authority_ack_floor(floor)
    }

    fn metadata_replay_cursor(&self, source_shard_id: &DfsMetadataShardId) -> Result<u64> {
        self.shard_backend(source_shard_id)?
            .metadata_replay_cursor(source_shard_id)
    }

    fn metadata_shard_statuses(&self) -> Result<Vec<DfsMetadataShardStatus>> {
        #[cfg(test)]
        SHARDED_METADATA_STATUS_QUERY_COUNT
            .set(SHARDED_METADATA_STATUS_QUERY_COUNT.get().saturating_add(1));
        let mut statuses = Vec::new();
        for shard in self.shard_configs.values() {
            let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
            let backend = self.shard_backend(&shard_id)?;
            let journal_last_sequence = backend.journal_last_sequence();
            let journal_next_sequence = journal_last_sequence.saturating_add(1);
            let mut replica_lag = Vec::new();
            for target_node_id in &shard.replica_node_ids {
                let replicated_through_sequence =
                    backend.metadata_replication_cursor(target_node_id, &shard_id)?;
                replica_lag.push(DfsMetadataReplicaLagStatus {
                    target_node_id: target_node_id.clone(),
                    replicated_through_sequence,
                    lag_entries: journal_last_sequence.saturating_sub(replicated_through_sequence),
                });
            }
            statuses.push(DfsMetadataShardStatus {
                shard_id,
                file_system_id: Some(shard.file_system_id.clone()),
                primary_node_id: Some(shard.primary_node_id.clone()),
                replica_node_ids: shard.replica_node_ids.clone(),
                epoch: Some(shard.epoch),
                journal_next_sequence,
                journal_last_sequence,
                replica_lag,
                replay_cursors: backend.replay_cursor_statuses()?,
            });
        }
        Ok(statuses)
    }

    fn persist_journal_frames_with_replica_permit_report(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        batch: &DfsMetadataJournalBatch,
        frames: &[DfsMetadataJournalFrameRecord],
    ) -> Result<DfsMetadataReplicaApplyReport> {
        self.require_replica_ingest_permit_for_shard(permit, &batch.shard_id)?;
        self.shard_backend_mut(&batch.shard_id)?
            .persist_journal_frames_with_replica_permit_report(permit, batch, frames)
    }

    fn persist_authority_journal_frames_with_replica_permit_report(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        batch: &DfsMetadataJournalBatch,
        frames: &[DfsMetadataJournalFrameRecord],
        expected_target_prefix: &DfsMetadataPrefixProof,
        expected_operation: &crate::DfsMetadataAuthorityOperation,
    ) -> Result<DfsMetadataReplicaApplyReport> {
        self.require_replica_ingest_permit_for_shard(permit, &batch.shard_id)?;
        self.shard_backend_mut(&batch.shard_id)?
            .persist_authority_journal_frames_with_replica_permit_report(
                permit,
                batch,
                frames,
                expected_target_prefix,
                expected_operation,
            )
    }

    fn replace_with_replicated_snapshot_with_replica_permit(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        source_shard_id: DfsMetadataShardId,
        replayed_through_sequence: u64,
        snapshot: crate::DfsMetadataSnapshotImage,
    ) -> Result<bool> {
        self.require_replica_ingest_permit_for_shard(permit, &source_shard_id)?;
        let mut affected_object_ids = self
            .shard_backend(&source_shard_id)?
            .export_table_snapshot()
            .tables
            .objects
            .into_iter()
            .map(|object| object.object_id)
            .collect::<BTreeSet<_>>();
        affected_object_ids.extend(
            snapshot
                .state()?
                .tables
                .objects
                .values()
                .map(|object| object.object_id.clone()),
        );
        let archived = self
            .shard_backend_mut(&source_shard_id)?
            .replace_with_replicated_snapshot_with_replica_permit(
                permit,
                source_shard_id,
                replayed_through_sequence,
                snapshot,
            )?;
        self.reconcile_object_shard_indexes(affected_object_ids.iter())?;
        Ok(archived)
    }

    fn subscribe(&self, after_sequence: u64) -> DfsMetadataSubscription {
        let shard_id = self
            .shards
            .keys()
            .next()
            .cloned()
            .unwrap_or_else(|| DfsMetadataShardId::new("default"));
        DfsMetadataSubscription::new(shard_id, after_sequence)
    }

    fn poll_subscription(
        &self,
        subscription: &mut DfsMetadataSubscription,
    ) -> Result<DfsMetadataJournalBatch> {
        self.shard_backend(&subscription.shard_id)?
            .poll_subscription(subscription)
    }
}

#[cfg(test)]
mod bounded_intent_maintenance_tests {
    use super::*;
    use crate::DfsWriteIntentMaintenanceCursor;
    use sinomo_core::DfsObjectPolicy;

    fn shard(name: &str, owner: &NodeId) -> DfsMetadataShardConfig {
        DfsMetadataShardConfig {
            file_system_id: crate::sifs_file_system_id(),
            shard_id: name.into(),
            primary_node_id: owner.clone(),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: if name == "a" {
                DfsMetadataShardAssignment::Root
            } else {
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: format!("/{name}"),
                }
            },
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        }
    }

    #[test]
    fn bounded_intent_page_limits_lazy_empty_shard_openings_and_zero_work() {
        let root = sinomo_testdir::TestDir::new("bounded-intent-empty-shards");
        let owner = NodeId::from_legacy_named("bounded-empty-primary").unwrap();
        let replica = NodeId::from_legacy_named("bounded-empty-replica").unwrap();
        let configs = (0..32)
            .map(|i| {
                let mut config = shard(&format!("s{i:03}"), &owner);
                config.replica_node_ids.push(replica.clone());
                config
            })
            .collect();
        let backend =
            LocalShardedMetadataBackend::new_with_replica_repair(root.path(), configs, &replica)
                .unwrap();
        assert!(backend.opened_shard_ids().is_empty());
        let cursor =
            DfsWriteIntentMaintenanceCursor::first_in_shard(DfsMetadataShardId::new("s000"));
        let zero = backend
            .write_intent_maintenance_page(Some(&cursor), 0)
            .unwrap();
        assert_eq!(zero.next_cursor, Some(cursor));
        assert_eq!(
            (zero.visited_record_count, zero.visited_shard_count),
            (0, 0)
        );
        assert!(backend.opened_shard_ids().is_empty());
        let mut cursor = None;
        for i in 0..32 {
            let page = backend
                .write_intent_maintenance_page(cursor.as_ref(), 1)
                .unwrap();
            assert_eq!(
                (page.visited_record_count, page.visited_shard_count),
                (0, 1)
            );
            assert_eq!(
                page.scanned_shard_id,
                Some(DfsMetadataShardId::new(format!("s{i:03}")))
            );
            assert_eq!(backend.opened_shard_ids().len(), i + 1);
            cursor = page.next_cursor;
        }
        assert!(cursor.is_none(), "last empty shard ends this sweep");
    }
}
