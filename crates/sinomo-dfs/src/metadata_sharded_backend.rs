//! Sharded DFS metadata backend.
//!
//! This module owns routing across shard-local metadata backends: shard
//! selection, object-shard indexes, cross-shard moves, and merged scans.
//! Durable table and journal storage stays in `metadata_backend`.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use sinomo_core::node::NodeId;

use crate::control_plane::{DfsMetadataShardConfig, select_metadata_shard_config_for_path};
use crate::intent::write_intent_status_is_active;
use crate::metadata_api::DfsMetadataBackend;
use crate::metadata_backend::LocalSobjMetadataBackend;
use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_hardlink::sort_hardlinks_by_path;
use crate::metadata_io::create_dir_all;
use crate::metadata_keys::{
    conflict_scan_cursor, conflict_scan_key, directory_page_cursor_with_shard, key_segment,
    local_directory_page_cursor, local_object_page_cursor, local_object_version_page_cursor,
    local_replica_receipt_page_cursor, local_write_intent_page_cursor, normalize_path,
    normalize_posix_path, object_page_cursor_with_shard, object_page_key,
    object_version_scan_cursor, object_version_scan_key, replica_receipt_scan_cursor,
    replica_receipt_scan_key, write_intent_page_cursor, write_intent_page_cursor_with_shard,
    write_intent_scan_cursor, write_intent_scan_order,
};
use crate::metadata_limits::{OBJECT_LOCAL_METADATA_PAGE_SIZE, SHARD_MIGRATION_PAGE_SIZE};
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
    DfsObjectShardMigrationBundle, collect_object_shard_migration_bundle,
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
use crate::posix_path::posix_path_is_at_or_under_prefix;
use crate::{
    DfsFileSystem, DfsFileSystemId, DfsObjectId, DfsObjectKind, DfsObjectMetadata, DfsVersion,
};

/// Local sharded metadata router.
///
/// This backend multiplexes multiple `LocalSobjMetadataBackend` shards using
/// control-plane shard configs and a local object-to-shard index. It owns
/// cross-shard moves, shard-index repair, and merged cursor scans; shard-local
/// durable table and journal storage remain in `LocalSobjMetadataBackend`.
#[derive(Debug)]
pub struct LocalShardedMetadataBackend {
    root: PathBuf,
    shard_configs: BTreeMap<String, DfsMetadataShardConfig>,
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
        Self::open_with(root, shards, true)
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
        Self::open_with(root, shards, false)
    }

    fn open_with(
        root: PathBuf,
        shards: Vec<DfsMetadataShardConfig>,
        initialize_layout: bool,
    ) -> Result<Self> {
        if shards.is_empty() {
            return Err(MetadataBackendError::Serde(
                "local sharded metadata backend requires at least one shard".to_string(),
            ));
        }
        if initialize_layout {
            create_dir_all(root.join("shard-index").join("objects"))?;
        }
        let mut shard_configs = BTreeMap::new();
        let mut backends = BTreeMap::new();
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
            let backend = if initialize_layout {
                LocalSobjMetadataBackend::new(shard_id.clone(), shard_root)?
            } else {
                LocalSobjMetadataBackend::open_for_inspection(shard_id.clone(), shard_root)?
            };
            backends.insert(shard_id, backend);
        }
        Ok(Self {
            root,
            shard_configs,
            shards: backends,
        })
    }

    pub(crate) fn shard_ids(&self) -> Vec<DfsMetadataShardId> {
        self.shards.keys().cloned().collect()
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

    fn shard_configs_for_file_system(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Vec<DfsMetadataShardConfig> {
        self.shard_configs
            .values()
            .filter(|shard| shard.file_system_id == *file_system_id)
            .cloned()
            .collect()
    }

    fn configured_shard_ids_for_file_system(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Vec<DfsMetadataShardId>> {
        let shard_ids = self
            .shard_configs_for_file_system(file_system_id)
            .into_iter()
            .map(|shard| DfsMetadataShardId::new(shard.shard_id))
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
        let selected = select_metadata_shard_config_for_path(
            file_system_id,
            &self.shard_configs_for_file_system(file_system_id),
            path,
        )
        .map_err(|error| MetadataBackendError::Serde(error.to_string()))?;
        selected
            .map(|shard| DfsMetadataShardId::new(shard.shard_id))
            .ok_or_else(|| MetadataBackendError::UnknownFileSystem {
                file_system_id: file_system_id.clone(),
            })
    }

    fn shard_id_for_object(&self, object_id: &DfsObjectId) -> Result<Option<DfsMetadataShardId>> {
        if let Some(index) = self.read_object_shard_index(object_id)? {
            if self.shards.contains_key(&index.shard_id) {
                return Ok(Some(index.shard_id));
            }
        }
        // Without an index record (replicas never write one), the same object
        // id can appear in several shards after a cross-shard move: the live
        // record in the destination shard and the eviction tombstone left in
        // the source shard. The live record must win regardless of shard
        // iteration order; tombstone-only matches are returned as a fallback
        // so deleted objects still resolve.
        let mut tombstone_shard = None;
        for (shard_id, backend) in &self.shards {
            if let Some(object) = backend.get_object(object_id)? {
                if object.kind != DfsObjectKind::Tombstone {
                    return Ok(Some(shard_id.clone()));
                }
                if tombstone_shard.is_none() {
                    tombstone_shard = Some(shard_id.clone());
                }
            }
        }
        Ok(tombstone_shard)
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
        if object.kind == DfsObjectKind::Tombstone {
            return Ok(true);
        }
        let Some(index) = self.read_object_shard_index(&object.object_id)? else {
            return Ok(true);
        };
        if !self.shards.contains_key(&index.shard_id) {
            return Ok(true);
        }
        Ok(index.shard_id == *shard_id)
    }

    fn object_is_listable_from_shard(
        &self,
        object: &DfsObjectMetadata,
        shard_id: &DfsMetadataShardId,
    ) -> Result<bool> {
        if let Some(index) = self.read_object_shard_index(&object.object_id)?
            && self.shards.contains_key(&index.shard_id)
            && index.shard_id != *shard_id
        {
            return Ok(false);
        }
        Ok(true)
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
            if backend.get_file_system(&file_system.file_system_id)? == Some(file_system.clone()) {
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
                    if visited_object_ids.contains(&object.object_id) {
                        continue;
                    }
                    if report.migrated_objects >= max_migrations {
                        report.reached_limit = true;
                        return Ok(None);
                    }
                    report.scanned_objects = report.scanned_objects.saturating_add(1);
                    if let Some(index) = self.read_object_shard_index(&object.object_id)?
                        && index.shard_id != source_shard_id
                    {
                        report.skipped_stale_sources =
                            report.skipped_stale_sources.saturating_add(1);
                        continue;
                    }
                    visited_object_ids.insert(object.object_id.clone());
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
                            self.write_object_shard_index(&target_object, &target_shard_id)?;
                            report.indexed_objects = report.indexed_objects.saturating_add(1);
                            report.skipped_stale_sources =
                                report.skipped_stale_sources.saturating_add(1);
                            continue;
                        }
                    }
                    let bundle =
                        self.object_shard_migration_bundle(&source_shard_id, &object.object_id)?;
                    self.install_object_shard_migration_bundle(
                        &file_system,
                        &bundle,
                        &target_shard_id,
                    )?;
                    self.write_object_shard_index(&bundle.object, &target_shard_id)?;
                    self.put_cross_shard_tombstone(
                        &bundle.object,
                        &source_shard_id,
                        &target_shard_id,
                    )?;
                    report.migrated_objects = report.migrated_objects.saturating_add(1);
                }
                Ok(page.next_cursor)
            })?;
            if report.reached_limit {
                return Ok(report);
            }
        }
        Ok(report)
    }

    fn object_shard_migration_bundle(
        &self,
        source_shard_id: &DfsMetadataShardId,
        object_id: &DfsObjectId,
    ) -> Result<DfsObjectShardMigrationBundle> {
        collect_object_shard_migration_bundle(
            self.shard_backend(source_shard_id)?,
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
        put_cross_shard_tombstone_into_backend(
            self.shard_backend_mut(source_shard_id)?,
            object,
            source_shard_id,
            target_shard_id,
        )
    }

    fn put_object_routed(&mut self, object: &DfsObjectMetadata) -> Result<DfsMetadataJournalEntry> {
        let object = normalized_object_metadata(object);
        let target_shard_id =
            self.select_shard_id_for_path(&object.file_system_id, &object.path)?;
        let source_shard_id = self.shard_id_for_object(&object.object_id)?;
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
                == Some(file_system)
            {
                continue;
            }
            last_entry = Some(backend.put_file_system(file_system)?);
        }
        Ok(last_entry)
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

    fn get_object(&self, object_id: &DfsObjectId) -> Result<Option<DfsObjectMetadata>> {
        if let Some(shard_id) = self.shard_id_for_object(object_id)? {
            return self.shard_backend(&shard_id)?.get_object(object_id);
        }
        Ok(None)
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
        if let Some(object) = object.as_ref()
            && !self.object_is_visible_from_shard(object, &shard_id)?
        {
            return Ok(None);
        }
        if object.is_some() {
            return Ok(object);
        }
        for (fallback_shard_id, backend) in &self.shards {
            if fallback_shard_id == &shard_id {
                continue;
            }
            let Some(candidate) = backend.get_object_by_path(file_system_id, path)? else {
                continue;
            };
            if self.object_is_visible_from_shard(&candidate, fallback_shard_id)? {
                return Ok(Some(candidate));
            }
        }
        Ok(None)
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
                    if let Some(index) = self.read_object_shard_index(&object.object_id)?
                        && index.shard_id != *shard_id
                    {
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

    fn put_object_and_version(
        &mut self,
        object: &DfsObjectMetadata,
        version: &DfsObjectVersionRecord,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let object = normalized_object_metadata(object);
        let target_shard_id =
            self.select_shard_id_for_path(&object.file_system_id, &object.path)?;
        let source_shard_id = self.shard_id_for_object(&object.object_id)?;
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
        let entries = self
            .shard_backend_mut(&target_shard_id)?
            .put_object_version_intent_and_receipts(&object, version, intent, receipts)?;
        self.write_content_commit_object_shard_index(&object, &target_shard_id)?;
        if let Some(source_shard_id) = source_shard_id {
            self.put_cross_shard_tombstone(&object, &source_shard_id, &target_shard_id)?;
        }
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
        let entries = collect_sharded_scan_entries(
            &self.shards,
            after,
            max_entries,
            |_, backend, cursor, limit| {
                let page = backend.receipt_records_page(cursor, limit)?;
                Ok((page.receipts, page.next_cursor))
            },
            |shard_id, backend, receipt| {
                let Some(object) = backend.get_object(&receipt.receipt.object_id)? else {
                    return Ok(None);
                };
                if !self.object_is_listable_from_shard(&object, shard_id)? {
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
        let entries = collect_sharded_scan_entries(
            &self.shards,
            after,
            max_entries,
            |_, backend, cursor, limit| {
                let page = backend.conflict_records_page(cursor, limit)?;
                Ok((page.conflicts, page.next_cursor))
            },
            |shard_id, backend, conflict| {
                let Some(object) = backend.get_object(&conflict.object_id)? else {
                    return Ok(None);
                };
                if !self.object_is_listable_from_shard(&object, shard_id)? {
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
        let mut object_shards = BTreeMap::<DfsObjectId, DfsMetadataShardId>::new();
        let mut cross_shard_tombstones =
            Vec::<(DfsObjectMetadata, DfsMetadataShardId, DfsMetadataShardId)>::new();
        for object in &transaction.objects {
            let object = normalized_object_metadata(object);
            let target_shard_id =
                self.select_shard_id_for_path(&object.file_system_id, &object.path)?;
            object_shards.insert(object.object_id.clone(), target_shard_id.clone());
            if let Some(source_shard_id) = self.indexed_shard_id_for_object(&object.object_id)?
                && source_shard_id != target_shard_id
            {
                cross_shard_tombstones.push((
                    object.clone(),
                    source_shard_id,
                    target_shard_id.clone(),
                ));
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
        let mut shard_ids = objects_by_shard.keys().cloned().collect::<BTreeSet<_>>();
        shard_ids.extend(versions_by_shard.keys().cloned());
        let mut last_entry = None;
        for shard_id in shard_ids {
            let objects = objects_by_shard.remove(&shard_id).unwrap_or_default();
            let versions = versions_by_shard.remove(&shard_id).unwrap_or_default();
            let shard_transaction = DfsNamespaceTransaction {
                transaction_id: format!("{}:{}", transaction.transaction_id, shard_id.0),
                file_system_id: transaction.file_system_id.clone(),
                objects,
                versions,
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
                "namespace transaction {} did not contain any objects or versions",
                transaction.transaction_id
            ))
        })
    }

    fn evict_object_record_for_foreign_shard_move(
        &mut self,
        moved_object: &DfsObjectMetadata,
    ) -> Result<()> {
        let target_shard_id =
            self.select_shard_id_for_path(&moved_object.file_system_id, &moved_object.path)?;
        if let Some(source_shard_id) = self.shard_id_for_object(&moved_object.object_id)?
            && source_shard_id != target_shard_id
        {
            self.put_cross_shard_tombstone(moved_object, &source_shard_id, &target_shard_id)?;
        }
        self.write_object_shard_index(moved_object, &target_shard_id)?;
        Ok(())
    }

    fn compact_tombstones(
        &mut self,
        options: DfsTombstoneCompactionOptions,
    ) -> Result<DfsTombstoneCompactionReport> {
        let mut report = DfsTombstoneCompactionReport::for_options(&options);
        let shard_ids = self.shard_ids();
        let mut compacted_object_ids = Vec::new();
        for shard_id in shard_ids {
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
            let (shard_report, shard_compacted_object_ids) = self
                .shard_backend_mut(&shard_id)?
                .compact_tombstones_with_ids(shard_options)?;
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

    fn get_namespace_job(&self, job_id: &str) -> Result<Option<DfsNamespaceJobRecord>> {
        for backend in self.shards.values() {
            if let Some(job) = backend.get_namespace_job(job_id)? {
                return Ok(Some(job));
            }
        }
        Ok(None)
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
                tables.receipts.extend(snapshot_tables.receipts);
                tables.intents.extend(snapshot_tables.intents);
                tables.conflicts.extend(snapshot_tables.conflicts);
                tables
                    .visible_commits
                    .extend(snapshot_tables.visible_commits);
                tables
                    .namespace_transactions
                    .extend(snapshot_tables.namespace_transactions);
                tables.namespace_jobs.extend(snapshot_tables.namespace_jobs);
            }
        }
        Ok(DfsMetadataTableSnapshot {
            format_version: METADATA_TABLE_SNAPSHOT_FORMAT_VERSION,
            materialized_through_sequence: None,
            tables: Some(tables),
            ops: Vec::new(),
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
        self.shard_backend_mut(&batch.shard_id)?
            .apply_journal_batch(batch)
    }

    fn replace_with_replicated_snapshot(
        &mut self,
        source_shard_id: DfsMetadataShardId,
        replayed_through_sequence: u64,
        snapshot: DfsMetadataTableSnapshot,
    ) -> Result<bool> {
        self.shard_backend_mut(&source_shard_id)?
            .replace_with_replicated_snapshot(source_shard_id, replayed_through_sequence, snapshot)
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
