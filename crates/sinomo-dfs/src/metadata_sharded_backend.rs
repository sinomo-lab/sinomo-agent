//! Sharded DFS metadata backend.
//!
//! This module owns routing across shard-local metadata backends: shard
//! selection, object-shard indexes, cross-shard moves, and merged scans.
//! Durable table and journal storage stays in `metadata_backend`.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use sinomo_core::node::NodeId;

use crate::advisory_lock::{
    DfsAdvisoryLockApplyResult, DfsAdvisoryLockCommand, DfsAdvisoryLockStateRecord,
    DfsAdvisoryLockTransitionContext, prepare_advisory_lock_state_for_migration,
};
use crate::content_root_catalog::{
    DfsContentRootCatalogIncompleteReason, DfsContentRootCatalogPage,
    DfsContentRootCatalogPageRequest, DfsContentRootCatalogShardIdentity,
};
use crate::control_plane::{
    DfsMetadataNamespacePrepareCodec, DfsMetadataShardAssignment, DfsMetadataShardConfig,
    DfsMetadataShardRoutingIndex,
};
use crate::intent::write_intent_status_is_active;
use crate::metadata_api::{
    DfsMetadataBackend, DfsMetadataReplicaIngestPermit, DfsMetadataShardWriteFreeze,
};
use crate::metadata_backend::LocalSobjMetadataBackend;
use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_hardlink::sort_hardlinks_by_path;
use crate::metadata_history_gc::{
    DfsMetadataContentProtection, DfsMetadataHistoryGcOptions, DfsMetadataHistoryGcReport,
};
use crate::metadata_io::{create_dir_all, read_sobj_optional, write_sobj_atomic};
use crate::metadata_keys::{
    conflict_scan_cursor, conflict_scan_key, directory_page_cursor_with_shard, key_segment,
    local_directory_page_cursor, local_object_page_cursor, local_object_version_page_cursor,
    local_replica_receipt_page_cursor, local_version_pin_page_cursor,
    local_write_intent_page_cursor, normalize_path, normalize_posix_path,
    object_page_cursor_with_shard, object_page_key, object_version_scan_cursor,
    object_version_scan_key, replica_receipt_scan_cursor, replica_receipt_scan_key,
    write_intent_page_cursor, write_intent_page_cursor_with_shard, write_intent_scan_cursor,
    write_intent_scan_order,
};
use crate::metadata_limits::{OBJECT_LOCAL_METADATA_PAGE_SIZE, SHARD_MIGRATION_PAGE_SIZE};
use crate::metadata_lock::MetadataWriteLock;
use crate::metadata_pages::{
    collect_metadata_pages, finish_cursor_page, finish_sorted_cursor_page, walk_metadata_pages,
};
use crate::metadata_records::*;
#[cfg(test)]
use crate::metadata_shard_index::object_shard_index_path;
use crate::metadata_shard_index::{
    DfsObjectShardIndexRecord, read_object_shard_index, remove_object_shard_index,
    write_content_commit_object_shard_index, write_object_shard_index,
    write_transaction_object_shard_indexes,
};
use crate::metadata_shard_migration::{
    DfsObjectShardMigrationBundle,
    install_object_shard_migration_bundle as install_object_shard_migration_bundle_into_backend,
    put_cross_shard_tombstone as put_cross_shard_tombstone_into_backend,
};
use crate::metadata_shard_pages::{
    collect_resumed_sharded_page_entries, collect_sharded_scan_entries,
    object_version_page_with_shard_cursor, replica_receipt_page_with_shard_cursor,
    write_intent_page_with_shard_cursor,
};
use crate::metadata_summary::{
    accumulate_metadata_summary_detail_counts, metadata_summary_for_file_system_from_tables,
    metadata_summary_from_tables,
};
use crate::metadata_table_store::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION;
use crate::metadata_tombstone::{metadata_now_unix_nanos, plan_tombstone_compaction};
use crate::posix_path::posix_path_is_at_or_under_prefix;
use crate::{
    DfsFileSystem, DfsFileSystemId, DfsObjectId, DfsObjectKind, DfsObjectMetadata, DfsVersion,
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
    static SHARDED_METADATA_OBJECT_ID_LOOKUP_COUNT: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_sharded_metadata_object_id_lookup_count() {
    SHARDED_METADATA_OBJECT_ID_LOOKUP_COUNT.set(0);
}

#[cfg(test)]
pub(crate) fn sharded_metadata_object_id_lookup_count() -> usize {
    SHARDED_METADATA_OBJECT_ID_LOOKUP_COUNT.get()
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
struct DfsMetadataRoutingTopologyHistory {
    #[serde(default)]
    active: Vec<DfsMetadataShardConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    fallbacks: Vec<Vec<DfsMetadataShardConfig>>,
}

fn routing_topology_history_path(root: &std::path::Path) -> PathBuf {
    root.join("shard-index").join("routing-topologies.sobj")
}

fn canonicalize_routing_topology(shards: &mut [DfsMetadataShardConfig]) {
    shards.sort_by(|left, right| {
        left.file_system_id
            .cmp(&right.file_system_id)
            .then_with(|| left.shard_id.cmp(&right.shard_id))
    });
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
        // Upgrade compatibility: old layouts have no routing history. A root
        // assignment is the only unambiguous predecessor for a newly split
        // catalog, so retain that bounded fallback rather than scanning every
        // shard for every create miss.
        let root_fallback = active
            .iter()
            .filter(|shard| matches!(&shard.assignment, DfsMetadataShardAssignment::Root))
            .cloned()
            .collect::<Vec<_>>();
        if !root_fallback.is_empty() && root_fallback.len() < active.len() {
            history.fallbacks.push(root_fallback);
        }
    } else if history.active != active {
        let previous = std::mem::replace(&mut history.active, active.to_vec());
        history.fallbacks.insert(0, previous);
    }
    history
        .fallbacks
        .retain(|topology| !topology.is_empty() && topology != &history.active);
    history.fallbacks.dedup();
    history.fallbacks.truncate(ROUTING_TOPOLOGY_HISTORY_LIMIT);
    if persist && persisted.as_ref() != Some(&history) {
        write_sobj_atomic(&path, &history)?;
    }
    Ok(history)
}

/// Local sharded metadata router.
///
/// This backend multiplexes multiple `LocalSobjMetadataBackend` shards using
/// control-plane shard configs and a local object-to-shard index. It owns
/// cross-shard moves, shard-index repair, and merged cursor scans; shard-local
/// durable table and journal storage remain in `LocalSobjMetadataBackend`.
#[derive(Debug)]
pub struct LocalShardedMetadataBackend {
    root: PathBuf,
    local_node_id: Option<NodeId>,
    shard_configs: BTreeMap<String, DfsMetadataShardConfig>,
    routing_indexes: BTreeMap<DfsFileSystemId, DfsMetadataShardRoutingIndex>,
    fallback_routing_indexes: Vec<BTreeMap<DfsFileSystemId, DfsMetadataShardRoutingIndex>>,
    shards: BTreeMap<DfsMetadataShardId, LocalSobjMetadataBackend>,
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
        Self::open_with(root, shards, true, None)
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
        Self::open_with(root, shards, true, Some(local_node_id))
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
        Self::open_with(root, shards, false, None)
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
        Self::open_with(root, shards, false, Some(local_node_id))
    }

    fn open_with(
        root: PathBuf,
        mut shards: Vec<DfsMetadataShardConfig>,
        initialize_layout: bool,
        replica_repair_local_node_id: Option<&NodeId>,
    ) -> Result<Self> {
        if shards.is_empty() {
            return Err(MetadataBackendError::Serde(
                "local sharded metadata backend requires at least one shard".to_string(),
            ));
        }
        if initialize_layout {
            create_dir_all(root.join("shard-index").join("objects"))?;
        }
        canonicalize_routing_topology(&mut shards);
        let routing_history = load_routing_topology_history(&root, &shards, initialize_layout)?;
        let mut shard_configs = BTreeMap::new();
        let mut shard_opens = Vec::with_capacity(shards.len());
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
            shard_opens.push((shard, shard_id, shard_root));
        }
        let replica_repair_local_node_id = replica_repair_local_node_id.cloned();
        let opened = std::thread::scope(|scope| {
            let handles = shard_opens
                .into_iter()
                .map(|(shard, shard_id, shard_root)| {
                    let replica_repair_local_node_id = replica_repair_local_node_id.as_ref();
                    scope.spawn(move || {
                        let mut backend = if initialize_layout {
                            match LocalSobjMetadataBackend::new(
                                shard_id.clone(),
                                shard_root.clone(),
                            ) {
                                Ok(backend) => backend,
                                Err(open_error)
                                    if replica_repair_local_node_id.is_some_and(|local_node_id| {
                                        shard.primary_node_id != *local_node_id
                                    }) =>
                                {
                                    LocalSobjMetadataBackend::open_for_replica_repair(
                                        shard_id.clone(),
                                        shard_root,
                                    )
                                    .map_err(|repair_error| {
                                        MetadataBackendError::Serde(format!(
                                            "cannot open replica metadata shard {}: {open_error}; repair-only open failed: {repair_error}",
                                            shard.shard_id
                                        ))
                                    })?
                                }
                                Err(error) => return Err(error),
                            }
                        } else {
                            LocalSobjMetadataBackend::open_for_inspection(
                                shard_id.clone(),
                                shard_root,
                            )?
                        };
                        backend.set_namespace_prepare_codec(shard.namespace_prepare_codec);
                        Ok((shard_id, backend))
                    })
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
        #[cfg(test)]
        crate::metadata_backend::record_metadata_backend_open_calls_from_workers(opened.len());
        let backends = opened.into_iter().collect::<BTreeMap<_, _>>();
        let routing_indexes = compile_routing_indexes(shard_configs.values().cloned())?;
        let fallback_routing_indexes = routing_history
            .fallbacks
            .iter()
            .map(|topology| compile_routing_indexes(topology.iter().cloned()))
            .collect::<Result<Vec<_>>>()?;

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

    fn shard_is_local_authoritative_primary(&self, shard_id: &DfsMetadataShardId) -> bool {
        self.local_node_id.as_ref().is_none_or(|local_node_id| {
            self.shard_configs
                .get(&shard_id.0)
                .is_some_and(|config| config.primary_node_id == *local_node_id)
        })
    }

    /// Consume this router without reopening its shard-local backends.
    ///
    /// Some control-plane operations bind write authority to the exact
    /// in-memory backend instance that validated a completed handoff. Moving
    /// those instances into a bounded multi-shard operation preserves that
    /// capability; reopening the same durable paths would intentionally
    /// produce unbound, fenced instances instead.
    pub(crate) fn into_shard_backends(
        self,
    ) -> BTreeMap<DfsMetadataShardId, LocalSobjMetadataBackend> {
        self.shards
    }

    #[cfg(test)]
    pub(crate) fn object_shard_index_path(&self, object_id: &DfsObjectId) -> PathBuf {
        object_shard_index_path(&self.root, object_id)
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

    fn write_content_commit_object_shard_index(
        &self,
        object: &DfsObjectMetadata,
        shard_id: &DfsMetadataShardId,
    ) -> Result<()> {
        write_content_commit_object_shard_index(&self.root, object, shard_id)
    }

    fn write_transaction_object_shard_indexes(
        &self,
        objects: &[DfsObjectMetadata],
        shard_id: &DfsMetadataShardId,
    ) -> Result<()> {
        write_transaction_object_shard_indexes(&self.root, objects, shard_id)
    }

    fn remove_object_shard_index(&self, object_id: &DfsObjectId) -> Result<()> {
        remove_object_shard_index(&self.root, object_id)
    }

    pub(crate) fn shard_backend(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<&LocalSobjMetadataBackend> {
        self.shards.get(shard_id).ok_or_else(|| {
            MetadataBackendError::Serde(format!("unknown local DFS metadata shard: {}", shard_id.0))
        })
    }

    pub(crate) fn shard_backend_mut(
        &mut self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<&mut LocalSobjMetadataBackend> {
        self.shards.get_mut(shard_id).ok_or_else(|| {
            MetadataBackendError::Serde(format!("unknown local DFS metadata shard: {}", shard_id.0))
        })
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

    fn configured_shard_ids_for_file_system(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Vec<DfsMetadataShardId>> {
        let shard_ids = self
            .shard_configs
            .values()
            .filter(|shard| shard.file_system_id == *file_system_id)
            .map(|shard| DfsMetadataShardId::new(shard.shard_id.clone()))
            .collect::<Vec<_>>();
        if shard_ids.is_empty() {
            return Err(MetadataBackendError::UnknownFileSystem {
                file_system_id: file_system_id.clone(),
            });
        }
        Ok(shard_ids)
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

    fn clear_routing_fallbacks_for_file_system(
        &mut self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<()> {
        for indexes in &mut self.fallback_routing_indexes {
            indexes.remove(file_system_id);
        }
        self.fallback_routing_indexes
            .retain(|indexes| !indexes.is_empty());

        let path = routing_topology_history_path(&self.root);
        let Some(mut history) = read_sobj_optional::<DfsMetadataRoutingTopologyHistory>(&path)?
        else {
            return Ok(());
        };
        for topology in &mut history.fallbacks {
            topology.retain(|shard| shard.file_system_id != *file_system_id);
        }
        history.fallbacks.retain(|topology| !topology.is_empty());
        write_sobj_atomic(&path, &history)
    }

    fn shard_id_for_object(&self, object_id: &DfsObjectId) -> Result<Option<DfsMetadataShardId>> {
        if let Some(index) = self.read_object_shard_index(object_id)? {
            if self.object_shard_index_is_corroborated(object_id, &index)? {
                return Ok(Some(index.shard_id));
            }
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

    /// Resolve physical object authority for merged scans without letting a
    /// stale derived index or shard iteration order override a newer routed
    /// record. Only object-less pending creates fall back to the derived index.
    fn shard_id_for_global_scan_object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<Option<DfsMetadataShardId>> {
        if let Some((shard_id, _)) = self.authoritative_object_placement(object_id)? {
            return Ok(Some(shard_id));
        }
        let Some(index) = self.read_object_shard_index(object_id)? else {
            return Ok(None);
        };
        if !self.shards.contains_key(&index.shard_id)
            || self.select_shard_id_for_path(&index.file_system_id, &index.path)? != index.shard_id
        {
            return Ok(None);
        }
        Ok(Some(index.shard_id))
    }

    /// Resolve the one shard whose receipt records may participate in restart
    /// recovery for an object/version pair.
    ///
    /// Visible objects use routed/generation-aware physical authority. A
    /// pending create has no visible object yet, so it uses a valid derived
    /// index when present; an index-less metadata replica falls back to the
    /// unique shard that carries its active intent. Ambiguous physical copies
    /// fail closed rather than allowing an arbitrary shard's receipts to
    /// satisfy write quorum.
    fn shard_id_for_receipt_scan_record(
        &self,
        receipt: &DfsReplicaReceiptRecord,
    ) -> Result<Option<DfsMetadataShardId>> {
        let object_id = &receipt.receipt.object_id;
        let version = &receipt.receipt.committed_version;
        if let Some(shard_id) = self.shard_id_for_global_scan_object(object_id)? {
            return Ok(Some(shard_id));
        }

        let mut active_intent_shard_id = None;
        for (shard_id, backend) in &self.shards {
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
        let mut candidates = Vec::new();
        for (shard_id, backend) in &self.shards {
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
                            && index.as_ref().is_some_and(|index| {
                                index.shard_id == shard_id && index.shard_id != *selected_shard
                            }))
                });
            if replace {
                selected = Some((shard_id, object));
            }
        }
        Ok(selected)
    }

    fn reconcile_object_shard_index(&self, object_id: &DfsObjectId) -> Result<()> {
        let Some((shard_id, object)) = self.authoritative_object_placement(object_id)? else {
            return self.remove_object_shard_index(object_id);
        };
        if self
            .read_object_shard_index(object_id)?
            .is_some_and(|index| {
                index.file_system_id == object.file_system_id
                    && index.path == normalize_path(&object.path)
                    && index.shard_id == shard_id
            })
        {
            return Ok(());
        }
        self.write_object_shard_index(&object, &shard_id)
    }

    fn reconcile_object_shard_indexes<'a>(
        &self,
        object_ids: impl IntoIterator<Item = &'a DfsObjectId>,
    ) -> Result<()> {
        for object_id in object_ids {
            self.reconcile_object_shard_index(object_id)?;
        }
        Ok(())
    }

    fn refresh_metadata_shard_durable_view_and_reconcile(
        &mut self,
        shard_id: &DfsMetadataShardId,
        reconcile_unchanged: bool,
    ) -> Result<bool> {
        let generation_before = self.shard_backend(shard_id)?.cached_table_generation();
        let mut affected_object_ids = self
            .shard_backend(shard_id)?
            .export_table_snapshot()
            .tables
            .into_iter()
            .flat_map(|tables| tables.objects)
            .map(|object| object.object_id)
            .collect::<BTreeSet<_>>();
        self.shard_backend_mut(shard_id)?
            .refresh_metadata_shard_durable_view(shard_id)?;
        let changed = self.shard_backend(shard_id)?.cached_table_generation() != generation_before;
        if !changed && !reconcile_unchanged {
            return Ok(false);
        }
        affected_object_ids.extend(
            self.shard_backend(shard_id)?
                .export_table_snapshot()
                .tables
                .into_iter()
                .flat_map(|tables| tables.objects)
                .map(|object| object.object_id),
        );
        self.begin_consistent_read_snapshot()?;
        let result = self.reconcile_object_shard_indexes(affected_object_ids.iter());
        self.end_consistent_read_snapshot();
        result.map(|()| changed)
    }

    fn indexed_shard_id_for_object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<Option<DfsMetadataShardId>> {
        Ok(self.read_object_shard_index(object_id)?.and_then(|index| {
            self.shards
                .contains_key(&index.shard_id)
                .then_some(index.shard_id)
        }))
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

    fn intent_is_listable_from_shard(
        &self,
        intent: &DfsWriteIntentRecord,
        backend: &LocalSobjMetadataBackend,
        shard_id: &DfsMetadataShardId,
    ) -> Result<bool> {
        if write_intent_status_is_active(&intent.status) {
            return self.active_intent_is_listable_from_shard(intent, shard_id);
        }
        if let Some(object) = backend.get_object(&intent.object_id)? {
            return self.object_is_listable_from_shard(&object, shard_id);
        }
        if let Some(object) = intent.object_metadata.as_ref()
            && object.object_id == intent.object_id
            && object.file_system_id == intent.file_system_id
        {
            return self.object_is_listable_from_shard(object, shard_id);
        }
        if let Some(index) = self.read_object_shard_index(&intent.object_id)?
            && self.shards.contains_key(&index.shard_id)
            && index.shard_id != *shard_id
        {
            return Ok(false);
        }
        Ok(true)
    }

    fn active_intent_is_listable_from_shard(
        &self,
        intent: &DfsWriteIntentRecord,
        shard_id: &DfsMetadataShardId,
    ) -> Result<bool> {
        let Some(index) = self.read_object_shard_index(&intent.object_id)? else {
            return Ok(true);
        };
        if !self.shards.contains_key(&index.shard_id) || index.shard_id == *shard_id {
            return Ok(true);
        }
        let Some(current_object) = self
            .shard_backend(&index.shard_id)?
            .get_object(&intent.object_id)?
        else {
            return Ok(true);
        };
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

    fn shard_id_for_existing_intent(
        &self,
        object_id: &DfsObjectId,
        intent_id: &str,
    ) -> Result<Option<DfsMetadataShardId>> {
        for (shard_id, backend) in &self.shards {
            if backend.get_intent(object_id, intent_id)?.is_some() {
                return Ok(Some(shard_id.clone()));
            }
        }
        Ok(None)
    }

    fn shard_id_for_intent_write(
        &self,
        intent: &DfsWriteIntentRecord,
    ) -> Result<DfsMetadataShardId> {
        if let Some(shard_id) =
            self.shard_id_for_existing_intent(&intent.object_id, &intent.intent_id)?
        {
            return Ok(shard_id);
        }
        self.shard_id_for_object(&intent.object_id)?.ok_or_else(|| {
            MetadataBackendError::UnknownObject {
                object_id: intent.object_id.clone(),
            }
        })
    }

    fn shard_id_for_pending_object_record(
        &self,
        object_id: &DfsObjectId,
        file_system_id: &DfsFileSystemId,
        object: Option<&DfsObjectMetadata>,
    ) -> Result<DfsMetadataShardId> {
        if let Some(shard_id) = self.shard_id_for_object(object_id)? {
            return Ok(shard_id);
        }
        let Some(object) = object else {
            return Err(MetadataBackendError::UnknownObject {
                object_id: object_id.clone(),
            });
        };
        if object.object_id != *object_id || object.file_system_id != *file_system_id {
            return Err(MetadataBackendError::UnknownObject {
                object_id: object_id.clone(),
            });
        }
        self.select_shard_id_for_path(file_system_id, &object.path)
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
        for (shard_id, backend) in &self.shards {
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

    pub(crate) fn ensure_file_system_on_all_shards(
        &mut self,
        file_system: &DfsFileSystem,
    ) -> Result<usize> {
        let shard_ids = self.configured_shard_ids_for_file_system(&file_system.file_system_id)?;
        let mut written = 0usize;
        for shard_id in shard_ids {
            let backend = self.shard_backend_mut(&shard_id)?;
            if backend
                .get_file_system(&file_system.file_system_id)?
                .as_ref()
                .is_some_and(|current| {
                    crate::placement::dfs_file_system_semantically_eq(current, file_system)
                })
            {
                continue;
            }
            backend.put_file_system(file_system)?;
            written = written.saturating_add(1);
        }
        Ok(written)
    }

    pub(crate) fn migrate_objects_to_configured_shards(
        &mut self,
        file_system_id: &DfsFileSystemId,
        max_migrations: usize,
    ) -> Result<DfsMetadataShardMigrationReport> {
        let file_system = self.get_file_system(file_system_id)?.ok_or_else(|| {
            MetadataBackendError::UnknownFileSystem {
                file_system_id: file_system_id.clone(),
            }
        })?;
        let mut report = DfsMetadataShardMigrationReport {
            file_system_id: file_system_id.clone(),
            scanned_objects: 0,
            indexed_objects: 0,
            migrated_objects: 0,
            skipped_stale_sources: 0,
            reached_limit: false,
        };
        let shard_ids = self.shard_ids();
        let mut visited_object_ids = BTreeSet::<DfsObjectId>::new();
        let mut completed_migration_steps = 0usize;
        for source_shard_id in shard_ids {
            walk_metadata_pages(|cursor| {
                let page = self
                    .shard_backend(&source_shard_id)?
                    .objects_for_file_system_page(
                        file_system_id,
                        cursor,
                        SHARD_MIGRATION_PAGE_SIZE,
                    )?;
                for object in page.objects {
                    let first_visit = visited_object_ids.insert(object.object_id.clone());
                    let existing_index = self.read_object_shard_index(&object.object_id)?;
                    let needs_physical_source_cleanup = existing_index
                        .as_ref()
                        .is_some_and(|index| index.shard_id != source_shard_id);
                    if !first_visit && !needs_physical_source_cleanup {
                        // A migration can install the target into a shard that
                        // this same bounded walk has not visited yet. Do not
                        // count or re-process that physical target copy, while
                        // still allowing a later indexed source copy to pass
                        // through the fenced cleanup branch below.
                        continue;
                    }
                    if completed_migration_steps >= max_migrations {
                        report.reached_limit = true;
                        return Ok(None);
                    }
                    if first_visit {
                        report.scanned_objects = report.scanned_objects.saturating_add(1);
                    }
                    if let Some(index) = existing_index
                        && index.shard_id != source_shard_id
                    {
                        let target_object = self
                            .shard_backend(&index.shard_id)?
                            .get_object(&object.object_id)?;
                        if let Some(target_object) = target_object
                            && target_object.kind != DfsObjectKind::Tombstone
                            && self.select_shard_id_for_path(
                                &target_object.file_system_id,
                                &target_object.path,
                            )? == index.shard_id
                        {
                            // Crash recovery for the narrow index→source
                            // cleanup window. Target install and exact pin-set
                            // reconciliation happen before the index write.
                            // Once indexed, new pins are legitimate target
                            // authority state, so verify the copied source set
                            // is still present but never remove target extras.
                            let (source_lock, bundle) = self.object_shard_migration_bundle(
                                &source_shard_id,
                                &object.object_id,
                            )?;
                            let target_pins = self
                                .shard_backend(&index.shard_id)?
                                .version_pins_for_object(&object.object_id)?;
                            for source_pin in &bundle.version_pins {
                                if !target_pins
                                    .iter()
                                    .any(|target_pin| target_pin == source_pin)
                                {
                                    return Err(MetadataBackendError::Serde(format!(
                                        "indexed target shard {} is missing durable source pin {} for {}/{} during migration recovery",
                                        index.shard_id.0,
                                        source_pin.pin_id,
                                        source_pin.object_id,
                                        source_pin.version
                                    )));
                                }
                            }
                            let eviction = self
                                .shard_backend_mut(&source_shard_id)?
                                .put_cross_shard_tombstone_if_unchanged_while_locked(
                                    &source_lock,
                                    &bundle.object,
                                    &target_object,
                                    &source_shard_id,
                                    &index.shard_id,
                                    bundle.source_journal_sequence,
                                    None,
                                )?;
                            match eviction {
                                crate::metadata_backend::DfsConditionalSourceEviction::Applied {
                                    ..
                                }
                                | crate::metadata_backend::DfsConditionalSourceEviction::AlreadyApplied => {
                                    completed_migration_steps =
                                        completed_migration_steps.saturating_add(1);
                                    report.migrated_objects =
                                        report.migrated_objects.saturating_add(1);
                                    continue;
                                }
                                crate::metadata_backend::DfsConditionalSourceEviction::JournalAdvanced
                                | crate::metadata_backend::DfsConditionalSourceEviction::SourceChanged => {
                                    return Err(MetadataBackendError::Serde(format!(
                                        "source shard {} changed while recovering indexed migration of object {}",
                                        source_shard_id.0, object.object_id
                                    )));
                                }
                            }
                        }
                        report.skipped_stale_sources =
                            report.skipped_stale_sources.saturating_add(1);
                        continue;
                    }
                    let target_shard_id =
                        self.select_shard_id_for_path(&object.file_system_id, &object.path)?;
                    if source_shard_id == target_shard_id {
                        let expected_path = normalize_path(&object.path);
                        let needs_index = self
                            .read_object_shard_index(&object.object_id)?
                            .is_none_or(|index| {
                                index.shard_id != target_shard_id || index.path != expected_path
                            });
                        if needs_index {
                            self.write_object_shard_index(&object, &target_shard_id)?;
                            report.indexed_objects = report.indexed_objects.saturating_add(1);
                        }
                        continue;
                    }
                    if let Some(target_object) = self
                        .shard_backend(&target_shard_id)?
                        .get_object(&object.object_id)?
                        && target_object.kind != DfsObjectKind::Tombstone
                    {
                        let target_object_shard_id = self.select_shard_id_for_path(
                            &target_object.file_system_id,
                            &target_object.path,
                        )?;
                        if target_object_shard_id == target_shard_id {
                            let (source_lock, bundle) = self.object_shard_migration_bundle(
                                &source_shard_id,
                                &object.object_id,
                            )?;
                            let target_pins = self
                                .shard_backend(&target_shard_id)?
                                .version_pins_for_object(&object.object_id)?;
                            for source_pin in &bundle.version_pins {
                                if !target_pins
                                    .iter()
                                    .any(|target_pin| target_pin == source_pin)
                                {
                                    return Err(MetadataBackendError::Serde(format!(
                                        "existing migration target shard {} is missing durable source pin {} for {}/{}",
                                        target_shard_id.0,
                                        source_pin.pin_id,
                                        source_pin.object_id,
                                        source_pin.version
                                    )));
                                }
                            }
                            self.write_object_shard_index(&target_object, &target_shard_id)?;
                            report.indexed_objects = report.indexed_objects.saturating_add(1);
                            let eviction = self
                                .shard_backend_mut(&source_shard_id)?
                                .put_cross_shard_tombstone_if_unchanged_while_locked(
                                    &source_lock,
                                    &bundle.object,
                                    &target_object,
                                    &source_shard_id,
                                    &target_shard_id,
                                    bundle.source_journal_sequence,
                                    None,
                                )?;
                            if !matches!(
                                eviction,
                                crate::metadata_backend::DfsConditionalSourceEviction::Applied {
                                    ..
                                } | crate::metadata_backend::DfsConditionalSourceEviction::AlreadyApplied
                            ) {
                                return Err(MetadataBackendError::Serde(format!(
                                    "source shard {} changed while completing installed migration of object {}",
                                    source_shard_id.0, object.object_id
                                )));
                            }
                            completed_migration_steps = completed_migration_steps.saturating_add(1);
                            report.skipped_stale_sources =
                                report.skipped_stale_sources.saturating_add(1);
                            continue;
                        }
                    }
                    let (source_lock, bundle) =
                        self.object_shard_migration_bundle(&source_shard_id, &object.object_id)?;
                    self.install_object_shard_migration_bundle(
                        &file_system,
                        &bundle,
                        &target_shard_id,
                    )?;
                    self.write_object_shard_index(&bundle.object, &target_shard_id)?;
                    let eviction = self
                        .shard_backend_mut(&source_shard_id)?
                        .put_cross_shard_tombstone_if_unchanged_while_locked(
                            &source_lock,
                            &bundle.object,
                            &bundle.object,
                            &source_shard_id,
                            &target_shard_id,
                            bundle.source_journal_sequence,
                            None,
                        )?;
                    if !matches!(
                        eviction,
                        crate::metadata_backend::DfsConditionalSourceEviction::Applied { .. }
                            | crate::metadata_backend::DfsConditionalSourceEviction::AlreadyApplied
                    ) {
                        return Err(MetadataBackendError::Serde(format!(
                            "source shard {} changed while migrating object {} to shard {}",
                            source_shard_id.0, bundle.object.object_id, target_shard_id.0
                        )));
                    }
                    completed_migration_steps = completed_migration_steps.saturating_add(1);
                    report.migrated_objects = report.migrated_objects.saturating_add(1);
                }
                Ok(page.next_cursor)
            })?;
            if report.reached_limit {
                return Ok(report);
            }
        }
        // A complete scan proves that every live object is now at the active
        // route (or has durable source-eviction evidence). Retire historical
        // routing generations so steady-state negative lookups touch exactly
        // one shard.
        self.clear_routing_fallbacks_for_file_system(file_system_id)?;
        Ok(report)
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

    fn install_object_shard_migration_bundle(
        &mut self,
        file_system: &DfsFileSystem,
        bundle: &DfsObjectShardMigrationBundle,
        target_shard_id: &DfsMetadataShardId,
    ) -> Result<()> {
        install_object_shard_migration_bundle_into_backend(
            self.shard_backend_mut(target_shard_id)?,
            file_system,
            bundle,
            target_shard_id,
            OBJECT_LOCAL_METADATA_PAGE_SIZE,
        )
    }

    fn put_cross_shard_tombstone(
        &mut self,
        object: &DfsObjectMetadata,
        source_shard_id: &DfsMetadataShardId,
        target_shard_id: &DfsMetadataShardId,
    ) -> Result<()> {
        self.require_unpinned_source_for_unmigrated_move(
            source_shard_id,
            target_shard_id,
            &object.object_id,
            "unfenced source eviction",
        )?;
        put_cross_shard_tombstone_into_backend(
            self.shard_backend_mut(source_shard_id)?,
            object,
            source_shard_id,
            target_shard_id,
        )
        .map(|_| ())
    }

    fn require_unpinned_source_for_unmigrated_move(
        &self,
        source_shard_id: &DfsMetadataShardId,
        target_shard_id: &DfsMetadataShardId,
        object_id: &DfsObjectId,
        operation: &str,
    ) -> Result<()> {
        if source_shard_id == target_shard_id {
            return Ok(());
        }
        let pins = self
            .shard_backend(source_shard_id)?
            .version_pins_for_object(object_id)?;
        if let Some(pin) = pins.first() {
            return Err(MetadataBackendError::Serde(format!(
                "{operation} cannot move pinned object {object_id} from shard {} to shard {} without a fenced full-family transfer; pin {} protects version {}",
                source_shard_id.0, target_shard_id.0, pin.pin_id, pin.version
            )));
        }
        Ok(())
    }

    fn put_object_routed(&mut self, object: &DfsObjectMetadata) -> Result<DfsMetadataJournalEntry> {
        let object = normalized_object_metadata(object);
        let target_shard_id =
            self.select_shard_id_for_path(&object.file_system_id, &object.path)?;
        let source_shard_id = self.shard_id_for_object(&object.object_id)?;
        if let Some(source_shard_id) = source_shard_id.as_ref() {
            self.require_unpinned_source_for_unmigrated_move(
                source_shard_id,
                &target_shard_id,
                &object.object_id,
                "routed object update",
            )?;
        }
        let entry = self
            .shard_backend_mut(&target_shard_id)?
            .put_object(&object)?;
        self.write_object_shard_index(&object, &target_shard_id)?;
        if let Some(source_shard_id) = source_shard_id {
            self.put_cross_shard_tombstone(&object, &source_shard_id, &target_shard_id)?;
        }
        Ok(entry)
    }
}

impl DfsMetadataBackend for LocalShardedMetadataBackend {
    fn begin_consistent_read_snapshot(&mut self) -> Result<()> {
        let shard_ids = self.shards.keys().cloned().collect::<Vec<_>>();
        for (index, shard_id) in shard_ids.iter().enumerate() {
            let begin = self
                .shards
                .get_mut(shard_id)
                .expect("collected shard id remains installed")
                .begin_consistent_read_snapshot();
            if let Err(error) = begin {
                for pinned_shard_id in &shard_ids[..index] {
                    self.shards
                        .get_mut(pinned_shard_id)
                        .expect("pinned shard remains installed")
                        .end_consistent_read_snapshot();
                }
                return Err(error);
            }
        }
        Ok(())
    }

    fn end_consistent_read_snapshot(&mut self) {
        for backend in self.shards.values_mut() {
            backend.end_consistent_read_snapshot();
        }
    }

    fn put_file_system(&mut self, file_system: &DfsFileSystem) -> Result<DfsMetadataJournalEntry> {
        let shard_ids = self.configured_shard_ids_for_file_system(&file_system.file_system_id)?;
        let mut last_entry = None;
        for shard_id in shard_ids {
            last_entry = Some(
                self.shard_backend_mut(&shard_id)?
                    .put_file_system(file_system)?,
            );
        }
        last_entry.ok_or_else(|| MetadataBackendError::UnknownFileSystem {
            file_system_id: file_system.file_system_id.clone(),
        })
    }

    fn ensure_file_system(
        &mut self,
        file_system: &DfsFileSystem,
    ) -> Result<Option<DfsMetadataJournalEntry>> {
        let shard_ids = self.configured_shard_ids_for_file_system(&file_system.file_system_id)?;
        let mut last_entry = None;
        for shard_id in shard_ids {
            let backend = self.shard_backend_mut(&shard_id)?;
            if backend
                .get_file_system(&file_system.file_system_id)?
                .as_ref()
                .is_some_and(|current| {
                    crate::placement::dfs_file_system_semantically_eq(current, file_system)
                })
            {
                continue;
            }
            last_entry = Some(backend.put_file_system(file_system)?);
        }
        Ok(last_entry)
    }

    fn ensure_file_system_on_shard(
        &mut self,
        shard_id: &DfsMetadataShardId,
        file_system: &DfsFileSystem,
    ) -> Result<Option<DfsMetadataJournalEntry>> {
        self.shard_backend_mut(shard_id)?
            .ensure_file_system(file_system)
    }

    fn get_file_system(&self, file_system_id: &DfsFileSystemId) -> Result<Option<DfsFileSystem>> {
        for backend in self.shards.values() {
            if let Some(file_system) = backend.get_file_system(file_system_id)? {
                return Ok(Some(file_system));
            }
        }
        Ok(None)
    }

    fn list_file_systems(&self) -> Result<Vec<DfsFileSystem>> {
        let mut file_systems = BTreeMap::<DfsFileSystemId, DfsFileSystem>::new();
        for backend in self.shards.values() {
            for file_system in backend.list_file_systems()? {
                file_systems.insert(file_system.file_system_id.clone(), file_system);
            }
        }
        Ok(file_systems.into_values().collect())
    }

    fn put_object(&mut self, object: &DfsObjectMetadata) -> Result<DfsMetadataJournalEntry> {
        self.put_object_routed(object)
    }

    fn add_object_location_if_current(
        &mut self,
        object_id: &DfsObjectId,
        expected_version: &DfsVersion,
        expected_content_hash: &str,
        replica_node_id: &NodeId,
    ) -> Result<Option<DfsObjectMetadata>> {
        let Some((shard_id, authoritative_object)) =
            self.authoritative_object_placement(object_id)?
        else {
            return Ok(None);
        };
        if authoritative_object.kind == DfsObjectKind::Tombstone
            || authoritative_object.version != *expected_version
            || authoritative_object.content_hash.as_deref() != Some(expected_content_hash)
        {
            return Ok(None);
        }
        let updated = self
            .shard_backend_mut(&shard_id)?
            .add_object_location_if_current(
                object_id,
                expected_version,
                expected_content_hash,
                replica_node_id,
            )?;
        if let Some(object) = updated.as_ref() {
            self.write_object_shard_index(object, &shard_id)?;
        }
        Ok(updated)
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

    fn apply_advisory_lock_command(
        &mut self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        context: DfsAdvisoryLockTransitionContext,
        command: &DfsAdvisoryLockCommand,
    ) -> Result<DfsAdvisoryLockApplyResult> {
        let shard_id = self.require_shard_id_for_object(object_id)?;
        self.shard_backend_mut(&shard_id)?
            .apply_advisory_lock_command(file_system_id, object_id, context, command)
    }

    fn install_advisory_lock_state_for_migration(
        &mut self,
        state: &DfsAdvisoryLockStateRecord,
    ) -> Result<Option<DfsMetadataJournalEntry>> {
        let shard_id = self.require_shard_id_for_object(&state.object_id)?;
        self.shard_backend_mut(&shard_id)?
            .install_advisory_lock_state_for_migration(state)
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
        if let Some(object) = object {
            if self.object_is_visible_from_shard(&object, &shard_id)? {
                return Ok(Some(object));
            }
            // A non-authoritative physical row on the currently selected
            // shard does not suppress a live fallback. In particular, a
            // source-eviction marker may become path-selected after a later
            // topology change, but it remains storage-only evidence.
        }
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
        for (shard_id, backend) in &self.shards {
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
            |_, backend, cursor, limit| {
                let page = backend.objects_for_file_system_page(file_system_id, cursor, limit)?;
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

    fn child_tombstones_for_directory(
        &self,
        file_system_id: &DfsFileSystemId,
        directory_path: &str,
    ) -> Result<Vec<DfsObjectMetadata>> {
        let mut candidate_paths = BTreeSet::new();
        for backend in self.shards.values() {
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
        if max_entries == 0 {
            return Ok(DfsDirectoryPage {
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
            local_directory_page_cursor,
            |_, backend, cursor, limit| {
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
        let (objects, next_cursor) = finish_cursor_page(entries, max_entries);
        Ok(DfsDirectoryPage {
            objects,
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
            |_, backend, cursor, limit| {
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
        for (shard_id, backend) in &self.shards {
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

    fn put_version(&mut self, version: &DfsObjectVersionRecord) -> Result<DfsMetadataJournalEntry> {
        let shard_id = self.require_shard_id_for_object(&version.object_id)?;
        self.shard_backend_mut(&shard_id)?.put_version(version)
    }

    fn put_version_pin(
        &mut self,
        pin: &DfsMetadataVersionPinRecord,
    ) -> Result<Option<DfsMetadataJournalEntry>> {
        let shard_id = self.require_shard_id_for_object(&pin.object_id)?;
        self.shard_backend_mut(&shard_id)?.put_version_pin(pin)
    }

    fn remove_version_pin(
        &mut self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        pin_id: &str,
    ) -> Result<Option<DfsMetadataJournalEntry>> {
        let mut containing_shards = Vec::new();
        let key = (version.clone(), pin_id.to_string());
        for (shard_id, backend) in &self.shards {
            if let Some(owner) = backend
                .cross_shard_pin_transfers_for_object(object_id)?
                .into_iter()
                .find(|transfer| {
                    transfer
                        .source_pins
                        .iter()
                        .any(|pin| &pin.version == version && pin.pin_id == pin_id)
                })
            {
                return Err(MetadataBackendError::Serde(format!(
                    "metadata version pin {pin_id} for {object_id}/{version} on shard {} is fenced by active cross-shard transfer {}",
                    shard_id.0, owner.transfer_id
                )));
            }
            if let Some(pin) = backend
                .version_pins_for_object(object_id)?
                .into_iter()
                .find(|pin| (pin.version.clone(), pin.pin_id.clone()) == key)
            {
                if &pin.file_system_id != file_system_id {
                    return Err(MetadataBackendError::Serde(format!(
                        "metadata version pin {pin_id} on shard {} has filesystem {}, expected {file_system_id}",
                        shard_id.0, pin.file_system_id
                    )));
                }
                containing_shards.push(shard_id.clone());
            }
        }
        let mut first_entry = None;
        for shard_id in containing_shards {
            if let Some(entry) = self.shard_backend_mut(&shard_id)?.remove_version_pin(
                file_system_id,
                object_id,
                version,
                pin_id,
            )? {
                first_entry.get_or_insert(entry);
            }
        }
        Ok(first_entry)
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

    fn install_cross_shard_version_pin_family(
        &mut self,
        object: &DfsObjectMetadata,
        versions: &[DfsObjectVersionRecord],
        pins: &[DfsMetadataVersionPinRecord],
    ) -> Result<()> {
        let target_shard_id =
            self.select_shard_id_for_path(&object.file_system_id, &object.path)?;
        for version in versions {
            if version.object_id != object.object_id
                || version.file_system_id != object.file_system_id
            {
                return Err(MetadataBackendError::Serde(format!(
                    "cross-shard pin-family install for object {} received unrelated version {}/{}",
                    object.object_id, version.object_id, version.version
                )));
            }
            match self
                .shard_backend(&target_shard_id)?
                .get_version(&version.object_id, &version.version)?
            {
                Some(existing) if existing != *version => {
                    return Err(MetadataBackendError::Serde(format!(
                        "target shard {} has conflicting version {} for pinned object {}",
                        target_shard_id.0, version.version, object.object_id
                    )));
                }
                Some(_) => {}
                None => {
                    self.shard_backend_mut(&target_shard_id)?
                        .put_version(version)?;
                }
            }
        }
        let mut desired = BTreeMap::new();
        for pin in pins {
            if pin.object_id != object.object_id || pin.file_system_id != object.file_system_id {
                return Err(MetadataBackendError::Serde(format!(
                    "cross-shard pin-family install for object {} received unrelated pin {} for {}/{}",
                    object.object_id, pin.pin_id, pin.object_id, pin.version
                )));
            }
            desired.insert((pin.version.clone(), pin.pin_id.clone()), pin.clone());
        }
        let target_is_already_authoritative = self
            .shard_id_for_object(&object.object_id)?
            .is_some_and(|shard_id| shard_id == target_shard_id);
        let existing = self
            .shard_backend(&target_shard_id)?
            .version_pins_for_object(&object.object_id)?;
        if !target_is_already_authoritative {
            for pin in &existing {
                let key = (pin.version.clone(), pin.pin_id.clone());
                if desired.get(&key) == Some(pin) {
                    continue;
                }
                self.shard_backend_mut(&target_shard_id)?
                    .remove_version_pin(
                        &pin.file_system_id,
                        &pin.object_id,
                        &pin.version,
                        &pin.pin_id,
                    )?;
            }
        }
        for pin in desired.values() {
            if target_is_already_authoritative
                && existing.iter().any(|current| {
                    current.version == pin.version && current.pin_id == pin.pin_id && current != pin
                })
            {
                return Err(MetadataBackendError::Serde(format!(
                    "authoritative target shard {} has a conflicting new pin {} for {}/{}",
                    target_shard_id.0, pin.pin_id, pin.object_id, pin.version
                )));
            }
            self.shard_backend_mut(&target_shard_id)?
                .put_version_pin(pin)?;
        }
        Ok(())
    }

    fn reconcile_cross_shard_pin_transfer(
        &mut self,
        transfer_id: &str,
        object: &DfsObjectMetadata,
        source_path: &str,
        versions: &[DfsObjectVersionRecord],
        source_pins: &[DfsMetadataVersionPinRecord],
    ) -> Result<bool> {
        let target_shard_id =
            self.select_shard_id_for_path(&object.file_system_id, &object.path)?;
        self.shard_backend_mut(&target_shard_id)?
            .reconcile_cross_shard_pin_transfer(
                transfer_id,
                object,
                source_path,
                versions,
                source_pins,
            )
    }

    fn finalize_cross_shard_pin_transfer(
        &mut self,
        transfer_id: &str,
        object: &DfsObjectMetadata,
        source_path: &str,
    ) -> Result<()> {
        let target_shard_id =
            self.select_shard_id_for_path(&object.file_system_id, &object.path)?;
        self.shard_backend_mut(&target_shard_id)?
            .finalize_cross_shard_pin_transfer(transfer_id, object, source_path)
    }

    fn cross_shard_pin_transfers_for_object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<Vec<DfsCrossShardPinTransferRecord>> {
        let shard_id = self.require_shard_id_for_object(object_id)?;
        self.shard_backend(&shard_id)?
            .cross_shard_pin_transfers_for_object(object_id)
    }

    fn install_cross_shard_pin_transfer_for_authoritative_migration(
        &mut self,
        transfer: &DfsCrossShardPinTransferRecord,
        versions: &[DfsObjectVersionRecord],
    ) -> Result<()> {
        let target_shard_id =
            self.select_shard_id_for_path(&transfer.file_system_id, &transfer.target_path)?;
        self.shard_backend_mut(&target_shard_id)?
            .install_cross_shard_pin_transfer_for_authoritative_migration(transfer, versions)
    }

    fn remove_cross_shard_pin_transfer_for_authoritative_migration(
        &mut self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        transfer_id: &str,
    ) -> Result<()> {
        let shard_ids = self.shard_ids();
        for shard_id in shard_ids {
            self.shard_backend_mut(&shard_id)?
                .remove_cross_shard_pin_transfer_for_authoritative_migration(
                    file_system_id,
                    object_id,
                    transfer_id,
                )?;
        }
        Ok(())
    }

    fn capture_cross_shard_source_pin_fence(
        &mut self,
        object_id: &DfsObjectId,
    ) -> Result<DfsCrossShardVersionPinFence> {
        let shard_id = self.require_shard_id_for_object(object_id)?;
        self.shard_backend_mut(&shard_id)?
            .capture_cross_shard_source_pin_fence(object_id)
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

    fn put_object_and_version(
        &mut self,
        object: &DfsObjectMetadata,
        version: &DfsObjectVersionRecord,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let object = normalized_object_metadata(object);
        let target_shard_id =
            self.select_shard_id_for_path(&object.file_system_id, &object.path)?;
        let source_shard_id = self.shard_id_for_object(&object.object_id)?;
        if let Some(source_shard_id) = source_shard_id.as_ref() {
            self.require_unpinned_source_for_unmigrated_move(
                source_shard_id,
                &target_shard_id,
                &object.object_id,
                "combined object/version update",
            )?;
        }
        let entries = self
            .shard_backend_mut(&target_shard_id)?
            .put_object_and_version(&object, version)?;
        self.write_object_shard_index(&object, &target_shard_id)?;
        if let Some(source_shard_id) = source_shard_id {
            self.put_cross_shard_tombstone(&object, &source_shard_id, &target_shard_id)?;
        }
        Ok(entries)
    }

    fn put_object_version_intent_and_receipts(
        &mut self,
        object: &DfsObjectMetadata,
        version: &DfsObjectVersionRecord,
        intent: &DfsWriteIntentRecord,
        receipts: &[DfsReplicaReceiptRecord],
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let object = normalized_object_metadata(object);
        let target_shard_id =
            self.select_shard_id_for_path(&object.file_system_id, &object.path)?;
        let source_shard_id = self.shard_id_for_object(&object.object_id)?;
        if let Some(source_shard_id) = source_shard_id.as_ref() {
            self.require_unpinned_source_for_unmigrated_move(
                source_shard_id,
                &target_shard_id,
                &object.object_id,
                "combined visible-version commit",
            )?;
        }
        let entries = self
            .shard_backend_mut(&target_shard_id)?
            .put_object_version_intent_and_receipts(&object, version, intent, receipts)?;
        self.write_content_commit_object_shard_index(&object, &target_shard_id)?;
        if let Some(source_shard_id) = source_shard_id {
            self.put_cross_shard_tombstone(&object, &source_shard_id, &target_shard_id)?;
        }
        Ok(entries)
    }

    fn commit_create_intent_receipts_and_namespace(
        &mut self,
        pending_intent: &DfsWriteIntentRecord,
        receipts: &[DfsReplicaReceiptRecord],
        transaction: &DfsNamespaceTransaction,
        committed_intent: &DfsWriteIntentRecord,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let mut shard_ids = BTreeSet::new();
        for object in &transaction.objects {
            shard_ids.insert(self.select_shard_id_for_path(&object.file_system_id, &object.path)?);
        }
        if shard_ids.len() != 1 {
            return Err(MetadataBackendError::Serde(format!(
                "combined create transaction {} spans {} metadata shards",
                transaction.transaction_id,
                shard_ids.len()
            )));
        }
        let shard_id = shard_ids.into_iter().next().ok_or_else(|| {
            MetadataBackendError::Serde(format!(
                "combined create transaction {} contains no objects",
                transaction.transaction_id
            ))
        })?;
        if !transaction
            .objects
            .iter()
            .any(|object| object.object_id == pending_intent.object_id)
        {
            return Err(MetadataBackendError::Serde(format!(
                "combined create transaction {} is missing intent object {}",
                transaction.transaction_id, pending_intent.object_id
            )));
        }
        let entries = self
            .shard_backend_mut(&shard_id)?
            .commit_create_intent_receipts_and_namespace(
                pending_intent,
                receipts,
                transaction,
                committed_intent,
            )?;
        self.write_transaction_object_shard_indexes(&transaction.objects, &shard_id)?;
        Ok(entries)
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

    fn put_receipt(
        &mut self,
        receipt: &DfsReplicaReceiptRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        let shard_id = self.require_shard_id_for_object(&receipt.receipt.object_id)?;
        self.shard_backend_mut(&shard_id)?.put_receipt(receipt)
    }

    fn remove_receipt(
        &mut self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
    ) -> Result<Option<DfsMetadataJournalEntry>> {
        let Some(shard_id) = self.shard_id_for_object(object_id)? else {
            return Ok(None);
        };
        self.shard_backend_mut(&shard_id)?
            .remove_receipt(object_id, version, replica_node_id)
    }

    fn put_receipt_for_object(
        &mut self,
        object: Option<&DfsObjectMetadata>,
        receipt: &DfsReplicaReceiptRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        let shard_id = self.shard_id_for_pending_object_record(
            &receipt.receipt.object_id,
            &receipt.receipt.file_system_id,
            object,
        )?;
        let entry = self.shard_backend_mut(&shard_id)?.put_receipt(receipt)?;
        if let Some(object) = object {
            self.write_object_shard_index(&normalized_object_metadata(object), &shard_id)?;
        }
        Ok(entry)
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

    fn put_intent(&mut self, intent: &DfsWriteIntentRecord) -> Result<DfsMetadataJournalEntry> {
        let shard_id = self.shard_id_for_intent_write(intent)?;
        self.shard_backend_mut(&shard_id)?.put_intent(intent)
    }

    fn put_intent_for_object(
        &mut self,
        object: Option<&DfsObjectMetadata>,
        intent: &DfsWriteIntentRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        if let Some(shard_id) =
            self.shard_id_for_existing_intent(&intent.object_id, &intent.intent_id)?
        {
            return self.shard_backend_mut(&shard_id)?.put_intent(intent);
        }
        let shard_id = self.shard_id_for_pending_object_record(
            &intent.object_id,
            &intent.file_system_id,
            object,
        )?;
        let entry = self.shard_backend_mut(&shard_id)?.put_intent(intent)?;
        if let Some(object) = object {
            self.write_object_shard_index(&normalized_object_metadata(object), &shard_id)?;
        }
        Ok(entry)
    }

    fn put_migrated_intent_for_object(
        &mut self,
        object: Option<&DfsObjectMetadata>,
        intent: &DfsWriteIntentRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        let shard_id = if let Some(object) = object {
            if object.object_id != intent.object_id
                || object.file_system_id != intent.file_system_id
            {
                return Err(MetadataBackendError::Serde(format!(
                    "migrated intent {}/{} destination object identity mismatch",
                    intent.object_id, intent.intent_id
                )));
            }
            self.select_shard_id_for_path(&object.file_system_id, &object.path)?
        } else if let Some(shard_id) =
            self.shard_id_for_existing_intent(&intent.object_id, &intent.intent_id)?
        {
            shard_id
        } else {
            self.shard_id_for_pending_object_record(
                &intent.object_id,
                &intent.file_system_id,
                None,
            )?
        };
        let entry = self
            .shard_backend_mut(&shard_id)?
            .put_migrated_intent_for_object(object, intent)?;
        if let Some(object) = object {
            self.write_object_shard_index(&normalized_object_metadata(object), &shard_id)?;
        }
        Ok(entry)
    }

    fn put_intent_receipt_for_object(
        &mut self,
        object: Option<&DfsObjectMetadata>,
        receipt: &DfsWriteIntentReceipt,
    ) -> Result<DfsMetadataJournalEntry> {
        let shard_id = if let Some(object) = object {
            if object.object_id != receipt.object_id
                || object.file_system_id != receipt.file_system_id
            {
                return Err(MetadataBackendError::Serde(format!(
                    "migrated intent receipt {}/{} destination object identity mismatch",
                    receipt.object_id, receipt.intent_id
                )));
            }
            self.select_shard_id_for_path(&object.file_system_id, &object.path)?
        } else if let Some(shard_id) =
            self.shard_id_for_existing_intent(&receipt.object_id, &receipt.intent_id)?
        {
            shard_id
        } else {
            self.shard_id_for_pending_object_record(
                &receipt.object_id,
                &receipt.file_system_id,
                None,
            )?
        };
        let entry = self
            .shard_backend_mut(&shard_id)?
            .put_intent_receipt_for_object(object, receipt)?;
        if let Some(object) = object {
            self.write_object_shard_index(&normalized_object_metadata(object), &shard_id)?;
        }
        Ok(entry)
    }

    fn merge_intent_receipt_retention_for_object(
        &mut self,
        object: Option<&DfsObjectMetadata>,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        retention: &DfsWriteIntentReceiptRetentionState,
    ) -> Result<DfsMetadataJournalEntry> {
        let shard_id =
            self.shard_id_for_pending_object_record(object_id, file_system_id, object)?;
        let entry = self
            .shard_backend_mut(&shard_id)?
            .merge_intent_receipt_retention_for_object(
                object,
                file_system_id,
                object_id,
                retention,
            )?;
        if let Some(object) = object {
            self.write_object_shard_index(&normalized_object_metadata(object), &shard_id)?;
        }
        Ok(entry)
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
        for backend in self.shards.values() {
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
        for backend in self.shards.values() {
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
        for backend in self.shards.values() {
            retention.merge_conservative(backend.intent_receipt_retention_for_object(object_id)?);
        }
        Ok(retention)
    }

    fn rebind_prepared_write_intent(
        &mut self,
        object: Option<&DfsObjectMetadata>,
        expected_intent: &DfsWriteIntentRecord,
        rebound_intent: &DfsWriteIntentRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        let shard_id = self
            .shard_id_for_existing_intent(&expected_intent.object_id, &expected_intent.intent_id)?
            .ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "write intent {} does not exist for object {}",
                    expected_intent.intent_id, expected_intent.object_id
                ))
            })?;
        if let Some(object) = object {
            let selected = self.select_shard_id_for_path(&object.file_system_id, &object.path)?;
            if selected != shard_id {
                return Err(MetadataBackendError::Serde(format!(
                    "write intent {} is durable on shard {}, but object path {} routes to shard {}",
                    expected_intent.intent_id, shard_id.0, object.path, selected.0
                )));
            }
        }
        self.shard_backend_mut(&shard_id)?
            .rebind_prepared_write_intent(object, expected_intent, rebound_intent)
    }

    fn put_intent_and_receipts_for_object(
        &mut self,
        object: Option<&DfsObjectMetadata>,
        intent: &DfsWriteIntentRecord,
        receipts: &[DfsReplicaReceiptRecord],
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        if let Some(shard_id) =
            self.shard_id_for_existing_intent(&intent.object_id, &intent.intent_id)?
        {
            return self
                .shard_backend_mut(&shard_id)?
                .put_intent_and_receipts_for_object(object, intent, receipts);
        }
        let shard_id = self.shard_id_for_pending_object_record(
            &intent.object_id,
            &intent.file_system_id,
            object,
        )?;
        let entries = self
            .shard_backend_mut(&shard_id)?
            .put_intent_and_receipts_for_object(object, intent, receipts)?;
        if let Some(object) = object {
            self.write_object_shard_index(&normalized_object_metadata(object), &shard_id)?;
        }
        Ok(entries)
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
        for backend in self.shards.values() {
            if let Some(intent) = backend.get_intent(object_id, intent_id)? {
                return Ok(Some(intent));
            }
        }
        Ok(None)
    }

    fn write_intent_counter_floor(&self) -> Result<u64> {
        self.shards.values().try_fold(0_u64, |floor, backend| {
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
        for backend in self.shards.values() {
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
            |_, backend, cursor, limit| {
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
            |_, backend, cursor, limit| {
                let page = backend.intents_for_object_version_page(
                    object_id,
                    target_version,
                    cursor,
                    limit,
                )?;
                Ok((page.intents, page.next_cursor))
            },
            |shard_id, backend, intent| {
                if !self.intent_is_listable_from_shard(&intent, backend, shard_id)? {
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
            |_, backend, cursor, limit| {
                let page = backend.write_intents_page(cursor, limit)?;
                Ok((page.intents, page.next_cursor))
            },
            |shard_id, backend, intent| {
                if !self.intent_is_listable_from_shard(&intent, backend, shard_id)? {
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

    fn write_intents_on_shard_page(
        &self,
        shard_id: &DfsMetadataShardId,
        after: Option<&DfsWriteIntentScanCursor>,
        max_entries: usize,
    ) -> Result<Option<DfsWriteIntentScanPage>> {
        let Some(backend) = self.shards.get(shard_id) else {
            return Ok(None);
        };
        backend.write_intents_on_shard_page(shard_id, after, max_entries)
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
            |_, backend, cursor, limit| {
                let page = backend.version_records_page(cursor, limit)?;
                Ok((page.versions, page.next_cursor))
            },
            |shard_id, backend, version| {
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
            |_, backend, cursor, limit| {
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

    fn conflict_records_page(
        &self,
        after: Option<&DfsConflictScanCursor>,
        max_entries: usize,
    ) -> Result<DfsConflictScanPage> {
        if max_entries == 0 {
            return Ok(DfsConflictScanPage {
                conflicts: Vec::new(),
                next_cursor: None,
            });
        }
        let mut seen = BTreeSet::<(DfsObjectId, String)>::new();
        let mut authoritative_shards = BTreeMap::<DfsObjectId, Option<DfsMetadataShardId>>::new();
        let entries = collect_sharded_scan_entries(
            &self.shards,
            after,
            max_entries,
            |_, backend, cursor, limit| {
                let page = backend.conflict_records_page(cursor, limit)?;
                Ok((page.conflicts, page.next_cursor))
            },
            |shard_id, _, conflict| {
                let authoritative_shard_id =
                    if let Some(shard_id) = authoritative_shards.get(&conflict.object_id) {
                        shard_id.clone()
                    } else {
                        let shard_id = self.shard_id_for_global_scan_object(&conflict.object_id)?;
                        authoritative_shards.insert(conflict.object_id.clone(), shard_id.clone());
                        shard_id
                    };
                if authoritative_shard_id.as_ref() != Some(shard_id) {
                    return Ok(None);
                }
                if !seen.insert((conflict.object_id.clone(), conflict.conflict_id.clone())) {
                    return Ok(None);
                }
                let cursor = conflict_scan_cursor(&conflict);
                Ok(Some((conflict, cursor)))
            },
        )?;
        let (conflicts, next_cursor) =
            finish_sorted_cursor_page(entries, max_entries, |left, right| {
                conflict_scan_key(left).cmp(&conflict_scan_key(right))
            });
        Ok(DfsConflictScanPage {
            conflicts,
            next_cursor,
        })
    }

    fn commit_visible_version(
        &mut self,
        expected_version: &DfsVersion,
        committed_object: &DfsObjectMetadata,
        visible_version: &DfsObjectVersionRecord,
        required_replica_node_ids: &[NodeId],
    ) -> Result<DfsMetadataJournalEntry> {
        let shard_id = self.require_shard_id_for_object(&committed_object.object_id)?;
        let entry = self.shard_backend_mut(&shard_id)?.commit_visible_version(
            expected_version,
            committed_object,
            visible_version,
            required_replica_node_ids,
        )?;
        self.write_content_commit_object_shard_index(
            &normalized_object_metadata(committed_object),
            &shard_id,
        )?;
        Ok(entry)
    }

    fn apply_namespace_transaction(
        &mut self,
        transaction: &DfsNamespaceTransaction,
    ) -> Result<DfsMetadataJournalEntry> {
        let mut objects_by_shard = BTreeMap::<DfsMetadataShardId, Vec<DfsObjectMetadata>>::new();
        let mut versions_by_shard =
            BTreeMap::<DfsMetadataShardId, Vec<DfsObjectVersionRecord>>::new();
        let mut advisory_lock_states_by_shard =
            BTreeMap::<DfsMetadataShardId, Vec<DfsAdvisoryLockStateRecord>>::new();
        let mut object_shards = BTreeMap::<DfsObjectId, DfsMetadataShardId>::new();
        let mut cross_shard_tombstones =
            Vec::<(DfsObjectMetadata, DfsMetadataShardId, DfsMetadataShardId)>::new();
        let explicit_lock_state_object_ids = transaction
            .advisory_lock_states
            .iter()
            .map(|state| state.object_id.clone())
            .collect::<BTreeSet<_>>();
        for object in &transaction.objects {
            let object = normalized_object_metadata(object);
            let target_shard_id =
                self.select_shard_id_for_path(&object.file_system_id, &object.path)?;
            object_shards.insert(object.object_id.clone(), target_shard_id.clone());
            if let Some(source_shard_id) = self.indexed_shard_id_for_object(&object.object_id)?
                && source_shard_id != target_shard_id
            {
                let mut source_pins = self
                    .shard_backend(&source_shard_id)?
                    .version_pins_for_object(&object.object_id)?;
                source_pins.sort();
                if source_pins.is_empty() {
                    cross_shard_tombstones.push((
                        object.clone(),
                        source_shard_id.clone(),
                        target_shard_id.clone(),
                    ));
                } else {
                    let mut target_pins = self
                        .shard_backend(&target_shard_id)?
                        .version_pins_for_object(&object.object_id)?;
                    target_pins.sort();
                    if target_pins != source_pins {
                        return Err(MetadataBackendError::Serde(format!(
                            "namespace transaction {} cannot adopt pinned object {} until target shard {} durably contains the exact source pin set from shard {}",
                            transaction.transaction_id,
                            object.object_id,
                            target_shard_id.0,
                            source_shard_id.0
                        )));
                    }
                    // The exact target copy is durable, but source cleanup is
                    // deferred to the caller's journal-head/fingerprint
                    // receipt fence after this transaction publishes the
                    // destination object and index.
                }
                if !explicit_lock_state_object_ids.contains(&object.object_id)
                    && let Some(source_state) = self
                        .shard_backend(&source_shard_id)?
                        .get_advisory_lock_state(&object.file_system_id, &object.object_id)?
                {
                    let (prepared_state, _) = prepare_advisory_lock_state_for_migration(
                        &source_state,
                    )
                    .map_err(|source| {
                        MetadataBackendError::Serde(format!(
                            "prepare advisory-lock state for local shard move of {}: {source}",
                            object.object_id
                        ))
                    })?;
                    advisory_lock_states_by_shard
                        .entry(target_shard_id.clone())
                        .or_default()
                        .push(prepared_state);
                }
            }
            objects_by_shard
                .entry(target_shard_id)
                .or_default()
                .push(object);
        }
        for version in &transaction.versions {
            let version = normalized_version_record(version);
            let shard_id = if let Some(shard_id) = object_shards.get(&version.object_id) {
                shard_id.clone()
            } else {
                self.require_shard_id_for_object(&version.object_id)?
            };
            versions_by_shard.entry(shard_id).or_default().push(version);
        }
        for state in &transaction.advisory_lock_states {
            let mut state = state.clone();
            state.authority_rebind_required = true;
            let shard_id = if let Some(shard_id) = object_shards.get(&state.object_id) {
                shard_id.clone()
            } else {
                self.require_shard_id_for_object(&state.object_id)?
            };
            advisory_lock_states_by_shard
                .entry(shard_id)
                .or_default()
                .push(state);
        }
        let mut shard_ids = objects_by_shard.keys().cloned().collect::<BTreeSet<_>>();
        shard_ids.extend(versions_by_shard.keys().cloned());
        shard_ids.extend(advisory_lock_states_by_shard.keys().cloned());
        let mut last_entry = None;
        for shard_id in shard_ids {
            let objects = objects_by_shard.remove(&shard_id).unwrap_or_default();
            let versions = versions_by_shard.remove(&shard_id).unwrap_or_default();
            let advisory_lock_states = advisory_lock_states_by_shard
                .remove(&shard_id)
                .unwrap_or_default();
            let shard_transaction = DfsNamespaceTransaction {
                transaction_id: format!("{}:{}", transaction.transaction_id, shard_id.0),
                file_system_id: transaction.file_system_id.clone(),
                objects,
                versions,
                advisory_lock_states,
                created_at: transaction.created_at,
            };
            let entry = self
                .shard_backend_mut(&shard_id)?
                .apply_namespace_transaction(&shard_transaction)?;
            self.write_transaction_object_shard_indexes(&shard_transaction.objects, &shard_id)?;
            last_entry = Some(entry);
        }
        for (object, source_shard_id, target_shard_id) in cross_shard_tombstones {
            self.put_cross_shard_tombstone(&object, &source_shard_id, &target_shard_id)?;
        }
        last_entry.ok_or_else(|| {
            MetadataBackendError::Serde(format!(
                "namespace transaction {} did not contain any objects, versions, or advisory-lock states",
                transaction.transaction_id
            ))
        })
    }

    fn prepare_namespace_transaction(
        &mut self,
        prepare: &DfsNamespacePrepareRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        let prepare = normalized_namespace_prepare(prepare);
        if !self.shards.contains_key(&prepare.shard_id) {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {} targets unknown local shard {}",
                prepare.operation_id, prepare.shard_id.0
            )));
        }
        let (configured_epoch, configured_codec) = self
            .shard_configs
            .get(&prepare.shard_id.0)
            .map(|shard| (shard.epoch, shard.namespace_prepare_codec))
            .ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "namespace prepare {} targets unconfigured local shard {}",
                    prepare.operation_id, prepare.shard_id.0
                ))
            })?;
        if configured_codec == DfsMetadataNamespacePrepareCodec::PackedV1
            && prepare.owner_epoch != configured_epoch
        {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {} owner epoch {} does not match configured shard {} epoch {}",
                prepare.operation_id, prepare.owner_epoch, prepare.shard_id.0, configured_epoch
            )));
        }
        for path in prepare
            .transaction
            .objects
            .iter()
            .map(|object| object.path.as_str())
            .chain(
                prepare
                    .expected_path_occupants
                    .iter()
                    .map(|expected| expected.path.as_str()),
            )
        {
            let selected = self.select_shard_id_for_path(&prepare.file_system_id, path)?;
            if selected != prepare.shard_id {
                return Err(MetadataBackendError::Serde(format!(
                    "namespace prepare {} path {} routes to shard {}, not prepared shard {}",
                    prepare.operation_id, path, selected.0, prepare.shard_id.0
                )));
            }
        }
        self.shard_backend_mut(&prepare.shard_id)?
            .prepare_namespace_transaction(&prepare)
    }

    fn rebind_prepared_namespace_transaction(
        &mut self,
        operation_id: &str,
        expected_operation_digest: &str,
        expected_owner_epoch: u64,
        rebound_prepare: &DfsNamespacePrepareRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        let rebound = normalized_namespace_prepare(rebound_prepare);
        let mut found_shard_id = None;
        for (shard_id, backend) in &self.shards {
            if backend.get_namespace_prepare(operation_id)?.is_some() {
                if found_shard_id.is_some() {
                    return Err(MetadataBackendError::Serde(format!(
                        "namespace prepare {operation_id} exists on multiple local shards"
                    )));
                }
                found_shard_id = Some(shard_id.clone());
            }
        }
        let shard_id = found_shard_id.ok_or_else(|| {
            MetadataBackendError::Serde(format!("namespace prepare {operation_id} does not exist"))
        })?;
        if rebound.shard_id != shard_id {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {operation_id} is durable on shard {}, not rebound shard {}",
                shard_id.0, rebound.shard_id.0
            )));
        }
        for path in rebound
            .transaction
            .objects
            .iter()
            .map(|object| object.path.as_str())
            .chain(
                rebound
                    .expected_path_occupants
                    .iter()
                    .map(|expected| expected.path.as_str()),
            )
        {
            let selected = self.select_shard_id_for_path(&rebound.file_system_id, path)?;
            if selected != shard_id {
                return Err(MetadataBackendError::Serde(format!(
                    "namespace prepare {operation_id} path {path} routes to shard {}, not durable shard {}",
                    selected.0, shard_id.0
                )));
            }
        }
        self.shard_backend_mut(&shard_id)?
            .rebind_prepared_namespace_transaction(
                operation_id,
                expected_operation_digest,
                expected_owner_epoch,
                &rebound,
            )
    }

    fn commit_prepared_namespace_transaction(
        &mut self,
        operation_id: &str,
        expected_operation_digest: &str,
        expected_owner_epoch: u64,
    ) -> Result<DfsMetadataJournalEntry> {
        let mut found = None;
        for (shard_id, backend) in &self.shards {
            if let Some(prepare) = backend.get_namespace_prepare(operation_id)? {
                if found.is_some() {
                    return Err(MetadataBackendError::Serde(format!(
                        "namespace prepare {operation_id} exists on multiple local shards"
                    )));
                }
                found = Some((shard_id.clone(), prepare));
            }
        }
        let (shard_id, prepare) = found.ok_or_else(|| {
            MetadataBackendError::Serde(format!("namespace prepare {operation_id} does not exist"))
        })?;
        let entry = self
            .shard_backend_mut(&shard_id)?
            .commit_prepared_namespace_transaction(
                operation_id,
                expected_operation_digest,
                expected_owner_epoch,
            )?;
        self.write_transaction_object_shard_indexes(&prepare.transaction.objects, &shard_id)?;
        Ok(entry)
    }

    fn commit_prepared_namespace_transaction_on_shard(
        &mut self,
        shard_id: &DfsMetadataShardId,
        operation_id: &str,
        expected_operation_digest: &str,
        expected_owner_epoch: u64,
    ) -> Result<DfsMetadataJournalEntry> {
        let prepare = self
            .shard_backend(shard_id)?
            .get_namespace_prepare(operation_id)?
            .ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "namespace prepare {operation_id} does not exist on shard {}",
                    shard_id.0
                ))
            })?;
        if &prepare.shard_id != shard_id {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {operation_id} claims shard {}, not durable shard {}",
                prepare.shard_id.0, shard_id.0
            )));
        }
        let entry = self
            .shard_backend_mut(shard_id)?
            .commit_prepared_namespace_transaction(
                operation_id,
                expected_operation_digest,
                expected_owner_epoch,
            )?;
        self.write_transaction_object_shard_indexes(&prepare.transaction.objects, shard_id)?;
        Ok(entry)
    }

    fn abort_prepared_namespace_transaction(
        &mut self,
        operation_id: &str,
        expected_operation_digest: &str,
        expected_owner_epoch: u64,
    ) -> Result<DfsMetadataJournalEntry> {
        let mut found_shard_id = None;
        for (shard_id, backend) in &self.shards {
            if backend.get_namespace_prepare(operation_id)?.is_some() {
                if found_shard_id.is_some() {
                    return Err(MetadataBackendError::Serde(format!(
                        "namespace prepare {operation_id} exists on multiple local shards"
                    )));
                }
                found_shard_id = Some(shard_id.clone());
            }
        }
        let shard_id = found_shard_id.ok_or_else(|| {
            MetadataBackendError::Serde(format!("namespace prepare {operation_id} does not exist"))
        })?;
        self.shard_backend_mut(&shard_id)?
            .abort_prepared_namespace_transaction(
                operation_id,
                expected_operation_digest,
                expected_owner_epoch,
            )
    }

    fn evict_object_record_for_foreign_shard_move(
        &mut self,
        moved_object: &DfsObjectMetadata,
        source_path: &str,
    ) -> Result<()> {
        let target_shard_id =
            self.select_shard_id_for_path(&moved_object.file_system_id, &moved_object.path)?;
        let source_shard_id =
            self.select_shard_id_for_path(&moved_object.file_system_id, source_path)?;
        if source_shard_id != target_shard_id {
            self.put_cross_shard_tombstone(moved_object, &source_shard_id, &target_shard_id)?;
        }
        self.write_object_shard_index(moved_object, &target_shard_id)?;
        Ok(())
    }

    fn evict_object_record_for_foreign_shard_move_with_pin_fence(
        &mut self,
        moved_object: &DfsObjectMetadata,
        source_path: &str,
        expected_source_object: &DfsObjectMetadata,
        expected_source_journal_sequence: u64,
        expected_version_pins: &[DfsMetadataVersionPinRecord],
        pin_transfer_id: Option<&str>,
    ) -> Result<()> {
        let target_shard_id =
            self.select_shard_id_for_path(&moved_object.file_system_id, &moved_object.path)?;
        let source_shard_id =
            self.select_shard_id_for_path(&moved_object.file_system_id, source_path)?;
        if source_shard_id == target_shard_id {
            self.write_object_shard_index(moved_object, &target_shard_id)?;
            return Ok(());
        }
        let (source_lock, bundle) =
            self.object_shard_migration_bundle(&source_shard_id, &moved_object.object_id)?;
        if &bundle.object != expected_source_object {
            return Err(MetadataBackendError::Serde(format!(
                "source shard {} object {} generation changed before pinned eviction",
                source_shard_id.0, moved_object.object_id
            )));
        }
        if bundle.source_journal_sequence != expected_source_journal_sequence {
            return Err(MetadataBackendError::Serde(format!(
                "source shard {} advanced from acknowledged sequence {} to {} before pinned object {} eviction",
                source_shard_id.0,
                expected_source_journal_sequence,
                bundle.source_journal_sequence,
                moved_object.object_id
            )));
        }
        let mut current_pins = bundle.version_pins.clone();
        current_pins.sort();
        let mut expected_pins = expected_version_pins.to_vec();
        expected_pins.sort();
        if current_pins != expected_pins {
            return Err(MetadataBackendError::Serde(format!(
                "source shard {} durable pin set changed before object {} eviction",
                source_shard_id.0, moved_object.object_id
            )));
        }
        self.write_object_shard_index(moved_object, &target_shard_id)?;
        let eviction = self
            .shard_backend_mut(&source_shard_id)?
            .put_cross_shard_tombstone_if_unchanged_while_locked(
                &source_lock,
                &bundle.object,
                moved_object,
                &source_shard_id,
                &target_shard_id,
                bundle.source_journal_sequence,
                pin_transfer_id,
            )?;
        match eviction {
            crate::metadata_backend::DfsConditionalSourceEviction::Applied { .. }
            | crate::metadata_backend::DfsConditionalSourceEviction::AlreadyApplied => Ok(()),
            crate::metadata_backend::DfsConditionalSourceEviction::JournalAdvanced => {
                Err(MetadataBackendError::Serde(format!(
                    "source shard {} advanced while evicting pinned object {}",
                    source_shard_id.0, moved_object.object_id
                )))
            }
            crate::metadata_backend::DfsConditionalSourceEviction::SourceChanged => {
                Err(MetadataBackendError::Serde(format!(
                    "source shard {} object {} changed while evicting pinned family",
                    source_shard_id.0, moved_object.object_id
                )))
            }
        }
    }

    fn tombstone_compaction_projection(
        &self,
        options: DfsTombstoneCompactionOptions,
    ) -> Result<DfsTombstoneCompactionReport> {
        let mut report = DfsTombstoneCompactionReport::for_options(&options);
        let now = options
            .now_unix_nanos
            .unwrap_or_else(metadata_now_unix_nanos);
        for (shard_id, backend) in &self.shards {
            if !self.shard_is_local_authoritative_primary(shard_id)
                || self.metadata_shard_authoritative_writes_frozen(shard_id)?
            {
                continue;
            }
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
        for (shard_id, backend) in &self.shards {
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

    fn gc_metadata_history(
        &mut self,
        options: DfsMetadataHistoryGcOptions,
    ) -> Result<DfsMetadataHistoryGcReport> {
        let mut report = DfsMetadataHistoryGcReport::for_options(&options);
        let shard_ids = self.shard_ids();
        for shard_id in shard_ids {
            if !self.shard_is_local_authoritative_primary(&shard_id)
                || self.metadata_shard_authoritative_writes_frozen(&shard_id)?
            {
                continue;
            }
            let mut shard_options = options.clone();
            if !options.dry_run
                && let Some(max_removals) = options.max_removals
            {
                let removed = report.applied_record_count();
                let remaining = max_removals.saturating_sub(removed);
                if remaining == 0 {
                    report.max_removals_reached = true;
                    break;
                }
                shard_options.max_removals = Some(remaining);
            }
            match self
                .shard_backend_mut(&shard_id)?
                .gc_metadata_history_authoritative(shard_options)
            {
                Ok(shard_report) => report.merge(shard_report),
                Err(MetadataBackendError::MembershipHandoffFrozen { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
        Ok(report)
    }

    fn metadata_content_protection(&self) -> Result<DfsMetadataContentProtection> {
        let mut protection = DfsMetadataContentProtection::default();
        for backend in self.shards.values() {
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

    fn put_namespace_job(&mut self, job: &DfsNamespaceJobRecord) -> Result<()> {
        let shard_id = self.select_shard_id_for_path(&job.file_system_id, &job.root_path)?;
        self.shard_backend_mut(&shard_id)?.put_namespace_job(job)
    }

    fn put_namespace_job_receipt(&mut self, receipt: &DfsNamespaceJobReceipt) -> Result<()> {
        let shard_id =
            self.select_shard_id_for_path(&receipt.file_system_id, &receipt.root_path)?;
        self.shard_backend_mut(&shard_id)?
            .put_namespace_job_receipt(receipt)
    }

    fn put_namespace_request_on_shard(
        &mut self,
        shard_id: &DfsMetadataShardId,
        request: &DfsNamespaceRequestRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        let durable_shard_id = if let Some(existing) =
            self.get_namespace_request(&request.requester_node_id, &request.request_id)?
        {
            Some(existing.authority_shard_id)
        } else {
            self.get_namespace_request_receipt(&request.requester_node_id, &request.request_id)?
                .map(|receipt| receipt.authority_shard_id)
        };
        if durable_shard_id.is_none()
            && self.namespace_request_history_expired_on_shard(
                shard_id,
                &request.requester_node_id,
                &request.request_id,
            )?
        {
            return Err(MetadataBackendError::Serde(format!(
                "namespace request {}/{} idempotency history expired or is unknown",
                request.requester_node_id, request.request_id
            )));
        }
        let target_shard_id = durable_shard_id.as_ref().unwrap_or(shard_id);
        self.shard_backend_mut(target_shard_id)?
            .put_namespace_request_on_shard(target_shard_id, request)
    }

    fn get_namespace_request(
        &self,
        requester_node_id: &NodeId,
        request_id: &str,
    ) -> Result<Option<DfsNamespaceRequestRecord>> {
        let mut found = None;
        for backend in self.shards.values() {
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
        for backend in self.shards.values() {
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
        for (history_shard_id, backend) in &self.shards {
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

    fn list_namespace_requests(&self) -> Result<Vec<DfsNamespaceRequestRecord>> {
        let mut requests = BTreeMap::new();
        for backend in self.shards.values() {
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
        for backend in self.shards.values() {
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

    fn get_prepared_namespace_prepare_for_request(
        &self,
        requester_node_id: &NodeId,
        request_id: &str,
    ) -> Result<Option<DfsNamespacePrepareRecord>> {
        let mut found = None;
        for backend in self.shards.values() {
            let Some(prepare) = backend
                .get_prepared_namespace_prepare_for_request(requester_node_id, request_id)?
            else {
                continue;
            };
            if found.is_some() {
                return Err(MetadataBackendError::Serde(format!(
                    "pending namespace request {requester_node_id}/{request_id} owns prepared transactions on multiple local shards"
                )));
            }
            found = Some(prepare);
        }
        Ok(found)
    }

    fn get_namespace_transaction(
        &self,
        transaction_id: &str,
    ) -> Result<Option<DfsNamespaceTransaction>> {
        for backend in self.shards.values() {
            if let Some(transaction) = backend.get_namespace_transaction(transaction_id)? {
                return Ok(Some(transaction));
            }
        }
        Ok(None)
    }

    fn get_namespace_prepare(
        &self,
        operation_id: &str,
    ) -> Result<Option<DfsNamespacePrepareRecord>> {
        let mut found = None;
        for backend in self.shards.values() {
            if let Some(prepare) = backend.get_namespace_prepare(operation_id)? {
                if found.is_some() {
                    return Err(MetadataBackendError::Serde(format!(
                        "namespace prepare {operation_id} exists on multiple local shards"
                    )));
                }
                found = Some(prepare);
            }
        }
        Ok(found)
    }

    fn list_prepared_namespace_prepares(&self) -> Result<Vec<DfsNamespacePrepareRecord>> {
        let mut prepares = BTreeMap::new();
        for backend in self.shards.values() {
            for prepare in backend.list_prepared_namespace_prepares()? {
                if prepares
                    .insert(prepare.operation_id.clone(), prepare.clone())
                    .is_some()
                {
                    return Err(MetadataBackendError::Serde(format!(
                        "namespace prepare {} exists on multiple local shards",
                        prepare.operation_id
                    )));
                }
            }
        }
        Ok(prepares.into_values().collect())
    }

    fn get_namespace_job(&self, job_id: &str) -> Result<Option<DfsNamespaceJobRecord>> {
        let mut found = None;
        for backend in self.shards.values() {
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
        for backend in self.shards.values() {
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
        for backend in self.shards.values() {
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
        for backend in self.shards.values() {
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
        let mut summary = DfsMetadataSummary {
            file_system_count: self.list_file_systems()?.len(),
            ..DfsMetadataSummary::default()
        };
        for (shard_id, backend) in &self.shards {
            let tables = backend.current_tables_snapshot()?;
            let shard_summary = metadata_summary_from_tables(&tables, |object| {
                self.object_is_listable_from_shard(object, shard_id)
            })?;
            accumulate_metadata_summary_detail_counts(&mut summary, &shard_summary);
        }
        Ok(summary)
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
        for (shard_id, backend) in &self.shards {
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

    fn put_conflict(&mut self, conflict: &DfsConflictRecord) -> Result<DfsMetadataJournalEntry> {
        let shard_id = self.require_shard_id_for_object(&conflict.object_id)?;
        self.shard_backend_mut(&shard_id)?.put_conflict(conflict)
    }

    fn get_conflict(
        &self,
        object_id: &DfsObjectId,
        conflict_id: &str,
    ) -> Result<Option<DfsConflictRecord>> {
        let Some(shard_id) = self.shard_id_for_object(object_id)? else {
            return Ok(None);
        };
        self.shard_backend(&shard_id)?
            .get_conflict(object_id, conflict_id)
    }

    fn conflicts_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsConflictRecord>> {
        let Some(shard_id) = self.shard_id_for_object(object_id)? else {
            return Ok(Vec::new());
        };
        let backend = self.shard_backend(&shard_id)?;
        collect_metadata_pages(|cursor| {
            let page = backend.conflicts_for_object_page(
                object_id,
                cursor,
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            Ok((page.conflicts, page.next_cursor))
        })
    }

    fn conflicts_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsConflictPageCursor>,
        max_entries: usize,
    ) -> Result<DfsConflictPage> {
        let Some(shard_id) = self.shard_id_for_object(object_id)? else {
            return Ok(DfsConflictPage {
                conflicts: Vec::new(),
                next_cursor: None,
            });
        };
        self.shard_backend(&shard_id)?
            .conflicts_for_object_page(object_id, after, max_entries)
    }

    fn remove_conflict(
        &mut self,
        object_id: &DfsObjectId,
        conflict_id: &str,
    ) -> Result<Option<DfsConflictRecord>> {
        let Some(shard_id) = self.shard_id_for_object(object_id)? else {
            return Ok(None);
        };
        self.shard_backend_mut(&shard_id)?
            .remove_conflict(object_id, conflict_id)
    }

    fn update_conflict_status(
        &mut self,
        object_id: &DfsObjectId,
        conflict_id: &str,
        status: DfsConflictStatus,
    ) -> Result<Option<DfsConflictRecord>> {
        let Some(shard_id) = self.shard_id_for_object(object_id)? else {
            return Ok(None);
        };
        self.shard_backend_mut(&shard_id)?
            .update_conflict_status(object_id, conflict_id, status)
    }

    fn journal_since(&self, after_sequence: u64) -> Result<Vec<DfsMetadataJournalEntry>> {
        let mut entries = Vec::new();
        for backend in self.shards.values() {
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

    fn metadata_shard_write_freeze(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataShardWriteFreeze>> {
        self.shard_backend(shard_id)?
            .metadata_shard_write_freeze(shard_id)
    }

    fn has_any_metadata_shard_write_freeze(&self) -> Result<bool> {
        for (shard_id, backend) in &self.shards {
            if backend.metadata_shard_write_freeze(shard_id)?.is_some() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn metadata_shard_authoritative_writes_frozen(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<bool> {
        self.shard_backend(shard_id)?
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
        let mut tables = DfsMetadataTableSnapshotTables::default();
        for backend in self.shards.values() {
            if let Some(snapshot_tables) =
                LocalSobjMetadataBackend::export_table_snapshot(backend).tables
            {
                tables.file_systems.extend(snapshot_tables.file_systems);
                tables.objects.extend(snapshot_tables.objects);
                tables
                    .file_system_objects
                    .extend(snapshot_tables.file_system_objects);
                tables.paths.extend(snapshot_tables.paths);
                tables.inodes.extend(snapshot_tables.inodes);
                tables.dirfrags.extend(snapshot_tables.dirfrags);
                tables.dentries.extend(snapshot_tables.dentries);
                tables.hardlinks.extend(snapshot_tables.hardlinks);
                tables
                    .compacted_tombstones
                    .extend(snapshot_tables.compacted_tombstones);
                tables.versions.extend(snapshot_tables.versions);
                tables.version_pins.extend(snapshot_tables.version_pins);
                tables.receipts.extend(snapshot_tables.receipts);
                tables.intents.extend(snapshot_tables.intents);
                tables
                    .intent_receipts
                    .extend(snapshot_tables.intent_receipts);
                tables
                    .intent_receipt_retention
                    .merge_conservative(snapshot_tables.intent_receipt_retention);
                tables.conflicts.extend(snapshot_tables.conflicts);
                tables
                    .advisory_lock_states
                    .extend(snapshot_tables.advisory_lock_states);
                tables
                    .visible_commits
                    .extend(snapshot_tables.visible_commits);
                tables
                    .namespace_transactions
                    .extend(snapshot_tables.namespace_transactions);
                tables
                    .namespace_transaction_receipts
                    .extend(snapshot_tables.namespace_transaction_receipts);
                tables
                    .namespace_prepares
                    .extend(snapshot_tables.namespace_prepares);
                tables
                    .namespace_prepare_receipts
                    .extend(snapshot_tables.namespace_prepare_receipts);
                tables.namespace_jobs.extend(snapshot_tables.namespace_jobs);
                tables
                    .namespace_job_receipts
                    .extend(snapshot_tables.namespace_job_receipts);
                tables
                    .namespace_request_receipts
                    .extend(snapshot_tables.namespace_request_receipts);
                tables
                    .namespace_requests
                    .extend(snapshot_tables.namespace_requests);
                tables
                    .namespace_receipt_retention
                    .merge_conservative(snapshot_tables.namespace_receipt_retention);
            }
        }
        Ok(DfsMetadataTableSnapshot {
            format_version: METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
            materialized_through_sequence: None,
            tables: Some(tables),
            ops: Vec::new(),
            semantic_state_fingerprint: None,
            prefix_proof: None,
        })
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

    fn apply_journal_batch(&mut self, batch: &DfsMetadataJournalBatch) -> Result<usize> {
        let affected_object_ids = batch
            .entries
            .iter()
            .flat_map(|entry| entry.mutation.affected_object_ids())
            .collect::<BTreeSet<_>>();
        let applied = self
            .shard_backend_mut(&batch.shard_id)?
            .apply_journal_batch(batch)?;
        self.reconcile_object_shard_indexes(affected_object_ids.iter())?;
        Ok(applied)
    }

    fn apply_journal_batch_with_replica_permit(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        batch: &DfsMetadataJournalBatch,
    ) -> Result<usize> {
        self.require_replica_ingest_permit_for_shard(permit, &batch.shard_id)?;
        let affected_object_ids = batch
            .entries
            .iter()
            .flat_map(|entry| entry.mutation.affected_object_ids())
            .collect::<BTreeSet<_>>();
        let applied = self
            .shard_backend_mut(&batch.shard_id)?
            .apply_journal_batch_with_replica_permit(permit, batch)?;
        self.reconcile_object_shard_indexes(affected_object_ids.iter())?;
        Ok(applied)
    }

    fn replace_with_replicated_snapshot(
        &mut self,
        source_shard_id: DfsMetadataShardId,
        replayed_through_sequence: u64,
        snapshot: DfsMetadataTableSnapshot,
    ) -> Result<bool> {
        let mut affected_object_ids = self
            .shard_backend(&source_shard_id)?
            .export_table_snapshot()
            .tables
            .into_iter()
            .flat_map(|tables| tables.objects)
            .map(|object| object.object_id)
            .collect::<BTreeSet<_>>();
        if let Some(tables) = snapshot.tables.as_ref() {
            affected_object_ids
                .extend(tables.objects.iter().map(|object| object.object_id.clone()));
        }
        let archived = self
            .shard_backend_mut(&source_shard_id)?
            .replace_with_replicated_snapshot(
                source_shard_id,
                replayed_through_sequence,
                snapshot,
            )?;
        self.reconcile_object_shard_indexes(affected_object_ids.iter())?;
        Ok(archived)
    }

    fn replace_with_replicated_snapshot_with_replica_permit(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        source_shard_id: DfsMetadataShardId,
        replayed_through_sequence: u64,
        snapshot: DfsMetadataTableSnapshot,
    ) -> Result<bool> {
        self.require_replica_ingest_permit_for_shard(permit, &source_shard_id)?;
        let mut affected_object_ids = self
            .shard_backend(&source_shard_id)?
            .export_table_snapshot()
            .tables
            .into_iter()
            .flat_map(|tables| tables.objects)
            .map(|object| object.object_id)
            .collect::<BTreeSet<_>>();
        if let Some(tables) = snapshot.tables.as_ref() {
            affected_object_ids
                .extend(tables.objects.iter().map(|object| object.object_id.clone()));
        }
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
