//! Service-local metadata mutations and tombstone index maintenance.
//!
//! Public methods here update DFS metadata state and keep the in-memory service
//! view synchronized with the durable metadata backend when one is configured.

use std::collections::BTreeMap;
use std::time::Instant;

use sinomo_core::node::NodeId;
use time::OffsetDateTime;

use crate::metadata_pages::collect_metadata_pages;
use crate::metadata_receipt::receipt_for_replica_from_backend;
use crate::{
    DFS_METADATA_OBJECT_INTENT_PAGE_SIZE, DFS_METADATA_OBJECT_RECEIPT_PAGE_SIZE,
    DfsControlPlaneStore, DfsDistributedError, DfsError, DfsFileSystem, DfsFileSystemConfig,
    DfsFileSystemId, DfsMessageTransport, DfsNamespaceTransaction, DfsObjectId, DfsObjectKind,
    DfsObjectMetadata, DfsObjectVersionRecord, DfsReplicaReceiptRecord, DfsVersion,
    DfsWriteIntentRecord, NetworkedDfsService, dfs_perf_trace, metadata_path,
    namespace_transaction_id_for_batch, normalize_current_locations, object_version_record,
    path_index_key,
};

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub fn add_file_system(&mut self, file_system: DfsFileSystem) -> Result<(), DfsError> {
        self.clear_metadata_lookup_cache();
        if let Some(kv) = self.control_plane.as_mut() {
            let file_system_config = DfsFileSystemConfig::from(&file_system);
            let mut store = DfsControlPlaneStore::new(kv.as_mut());
            if store.get_file_system_config(&file_system.file_system_id)?
                != Some(file_system_config.clone())
            {
                store.put_file_system_config(&file_system_config)?;
            }
        }
        if let Some(backend) = self.metadata_backend.as_mut() {
            backend.ensure_file_system(&file_system)?;
        }
        self.metadata.add_file_system(file_system);
        self.clear_metadata_lookup_cache();
        Ok(())
    }

    pub fn add_object_metadata(&mut self, object: DfsObjectMetadata) -> Result<(), DfsError> {
        self.clear_metadata_lookup_cache();
        self.put_visible_object_in_metadata_backend(&object)?;
        self.transient_metadata_object_ids.remove(&object.object_id);
        self.coordinator.set_committed_version(
            object.file_system_id.clone(),
            object.object_id.clone(),
            object.version.clone(),
        );
        self.track_metadata_tombstone(&object);
        let result = self.metadata.add_object(object);
        self.clear_metadata_lookup_cache();
        result
    }

    pub fn add_object_metadata_batch(
        &mut self,
        objects: impl IntoIterator<Item = DfsObjectMetadata>,
    ) -> Result<(), DfsError> {
        self.clear_metadata_lookup_cache();
        let mut grouped =
            BTreeMap::<DfsFileSystemId, Vec<(DfsObjectMetadata, DfsObjectVersionRecord)>>::new();
        for mut object in objects {
            normalize_current_locations(&mut object.current_locations);
            let size_bytes = if matches!(object.kind, DfsObjectKind::File | DfsObjectKind::Symlink)
                && object.content_size_bytes.is_some()
            {
                object.content_size_bytes
            } else {
                self.object_content_size_hint(&object)?
            };
            let version = object_version_record(&object, size_bytes);
            grouped
                .entry(object.file_system_id.clone())
                .or_default()
                .push((object, version));
        }
        if let Some(backend) = self.metadata_backend.as_mut() {
            for (file_system_id, records) in &grouped {
                let Some((object, _)) = records.first() else {
                    continue;
                };
                let file_system =
                    backend
                        .get_file_system(file_system_id)?
                        .unwrap_or_else(|| DfsFileSystem {
                            file_system_id: file_system_id.clone(),
                            display_name: file_system_id.to_string(),
                            owner_node_id: object.owner_node_id.clone(),
                            subtree_policies: Vec::new(),
                        });
                backend.ensure_file_system(&file_system)?;

                let mut transaction_objects = Vec::new();
                let mut transaction_versions = Vec::new();
                let lookup_keys = records
                    .iter()
                    .map(|(object, version)| (object.object_id.clone(), version.version.clone()))
                    .collect::<Vec<_>>();
                let existing_pairs = backend.get_object_version_pairs(&lookup_keys)?;
                for ((object, version), (existing_object, existing_version)) in
                    records.iter().zip(existing_pairs.into_iter())
                {
                    if existing_object.as_ref() != Some(object) {
                        transaction_objects.push(object.clone());
                    }
                    let mut version = version.clone();
                    if let Some(existing) = existing_version {
                        version.created_at = existing.created_at;
                        if existing == version {
                            continue;
                        }
                    }
                    transaction_versions.push(version);
                }
                if transaction_objects.is_empty() && transaction_versions.is_empty() {
                    continue;
                }
                backend.apply_namespace_transaction(&DfsNamespaceTransaction {
                    transaction_id: namespace_transaction_id_for_batch(
                        "visible-batch",
                        file_system_id,
                        &transaction_objects,
                        &transaction_versions,
                    ),
                    file_system_id: file_system_id.clone(),
                    objects: transaction_objects,
                    versions: transaction_versions,
                    advisory_lock_states: Vec::new(),
                    created_at: OffsetDateTime::now_utc(),
                })?;
            }
        }
        for (_, records) in grouped {
            for (object, _) in records {
                self.transient_metadata_object_ids.remove(&object.object_id);
                self.coordinator.set_committed_version(
                    object.file_system_id.clone(),
                    object.object_id.clone(),
                    object.version.clone(),
                );
                self.track_metadata_tombstone(&object);
                self.metadata.add_object(object)?;
            }
        }
        self.clear_metadata_lookup_cache();
        Ok(())
    }

    pub fn add_transient_object_metadata(
        &mut self,
        mut object: DfsObjectMetadata,
    ) -> Result<(), DfsError> {
        self.clear_metadata_lookup_cache();
        normalize_current_locations(&mut object.current_locations);
        let object_id = object.object_id.clone();
        let backend_object = self
            .metadata_backend
            .as_ref()
            // This lookup only classifies pagination overlays. A backend read
            // failure must not turn an already committed write into a
            // caller-visible error; treating it as absent conservatively keeps
            // the in-memory namespace object visible until a later durable
            // observation clears the transient marker.
            .and_then(|backend| backend.get_object(&object_id).ok().flatten());
        let changes_namespace_view = backend_object.as_ref().is_none_or(|backend_object| {
            backend_object.file_system_id != object.file_system_id
                || backend_object.path != object.path
                || backend_object.kind != object.kind
        });
        self.track_metadata_tombstone(&object);
        let result = self.metadata.add_object(object);
        if result.is_ok() {
            if changes_namespace_view {
                self.transient_metadata_object_ids.insert(object_id);
            } else {
                self.transient_metadata_object_ids.remove(&object_id);
            }
        }
        self.clear_metadata_lookup_cache();
        result
    }

    pub(crate) fn track_metadata_tombstone(&mut self, object: &DfsObjectMetadata) {
        let key = (object.file_system_id.clone(), metadata_path(&object.path));
        if object.kind == DfsObjectKind::Tombstone {
            let replace = self
                .tombstones_by_metadata_path
                .get(&key)
                .is_none_or(|existing| existing.version < object.version);
            if replace {
                self.tombstones_by_metadata_path.insert(key, object.clone());
            }
            return;
        }
        self.tombstones_by_metadata_path.remove(&key);
    }

    pub(crate) fn track_hydrated_metadata_tombstone(&mut self, object: &DfsObjectMetadata) {
        if object.kind != DfsObjectKind::Tombstone {
            let key = (object.file_system_id.clone(), metadata_path(&object.path));
            if self
                .tombstones_by_metadata_path
                .get(&key)
                .is_some_and(|existing| existing.version > object.version)
            {
                return;
            }
            self.track_metadata_tombstone(object);
            return;
        }
        let live_path = path_index_key(&object.file_system_id, &object.path).map(|(_, path)| path);
        let visible_object = self.metadata.object_by_normalized_posix_path(
            &object.file_system_id,
            live_path.as_deref().unwrap_or(&object.path),
        );
        if visible_object.is_none_or(|visible| visible.version < object.version) {
            self.track_metadata_tombstone(object);
        }
    }

    pub fn remove_transient_object_metadata(&mut self, object_id: &DfsObjectId) {
        self.clear_metadata_lookup_cache();
        self.transient_metadata_object_ids.remove(object_id);
        self.metadata.remove_object(object_id);
        self.clear_metadata_lookup_cache();
    }

    // Shared metadata-backend read helpers for service paths that need durable
    // receipts or intents while preserving the service's no-backend fallback.
    pub(crate) fn backend_receipts_for_version(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<Vec<DfsReplicaReceiptRecord>, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(Vec::new());
        };
        let receipts = collect_metadata_pages(|cursor| {
            let page = backend.receipts_for_version_page(
                object_id,
                version,
                cursor,
                DFS_METADATA_OBJECT_RECEIPT_PAGE_SIZE,
            )?;
            Ok((page.receipts, page.next_cursor))
        })?;
        Ok(receipts)
    }

    pub(crate) fn backend_intent_ids_for_version(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<Vec<String>, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(Vec::new());
        };
        let intents = collect_metadata_pages(|cursor| {
            let page = backend.intents_for_object_version_page(
                object_id,
                version,
                cursor,
                DFS_METADATA_OBJECT_INTENT_PAGE_SIZE,
            )?;
            Ok((page.intents, page.next_cursor))
        })?;
        Ok(intents.into_iter().map(|intent| intent.intent_id).collect())
    }

    pub(crate) fn backend_receipt_for_replica(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
    ) -> Result<Option<DfsReplicaReceiptRecord>, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(None);
        };
        Ok(receipt_for_replica_from_backend(
            backend.as_ref(),
            object_id,
            version,
            replica_node_id,
            DFS_METADATA_OBJECT_RECEIPT_PAGE_SIZE,
        )?)
    }

    pub(crate) fn restore_backend_durable_receipt(
        &mut self,
        intent_id: &str,
        replica_node_id: NodeId,
        allow_unknown_intent: bool,
    ) -> Result<(), DfsError> {
        self.restore_backend_durable_receipt_with_repair_rebuild(
            intent_id,
            replica_node_id,
            allow_unknown_intent,
            true,
        )
    }

    /// `rebuild_repair_jobs` selects between the eager per-receipt repair
    /// rebuild and the deferred one for bulk restore loops: hydration restores
    /// every receipt of an unsettled-write backlog, and rebuilding per receipt
    /// made SIFS opens burn seconds of CPU once a degraded cluster link had
    /// let that backlog accumulate.
    pub(crate) fn restore_backend_durable_receipt_with_repair_rebuild(
        &mut self,
        intent_id: &str,
        replica_node_id: NodeId,
        allow_unknown_intent: bool,
        rebuild_repair_jobs: bool,
    ) -> Result<(), DfsError> {
        let restored = if rebuild_repair_jobs {
            self.coordinator
                .restore_durable_receipt(intent_id, replica_node_id)
        } else {
            self.coordinator
                .restore_durable_receipt_deferring_repair_rebuild(intent_id, replica_node_id)
                .map(drop)
        };
        match restored {
            Ok(()) => Ok(()),
            Err(DfsDistributedError::UnknownIntent { .. }) if allow_unknown_intent => Ok(()),
            Err(
                DfsDistributedError::UnexpectedReplica { .. }
                | DfsDistributedError::IntentClosed { .. },
            ) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub(crate) fn restore_backend_receipt_pages_for_intent(
        &mut self,
        intent_record: &DfsWriteIntentRecord,
        rebuild_repair_jobs: bool,
    ) -> Result<(), DfsError> {
        for receipt in self
            .backend_receipts_for_version(&intent_record.object_id, &intent_record.target_version)?
        {
            if let Some(expected_hash) = intent_record.content_hash.as_deref()
                && receipt.content_hash.as_deref() != Some(expected_hash)
            {
                continue;
            }
            self.restore_backend_durable_receipt_with_repair_rebuild(
                &intent_record.intent_id,
                receipt.replica_node_id,
                false,
                rebuild_repair_jobs,
            )?;
        }
        Ok(())
    }

    pub fn apply_namespace_transaction(
        &mut self,
        transaction: DfsNamespaceTransaction,
    ) -> Result<(), DfsError> {
        let trace_started = Instant::now();
        let trace_object_count = transaction.objects.len();
        let trace_version_count = transaction.versions.len();
        let mut trace_ensure_fs_ms = 0;
        let mut trace_backend_apply_ms = 0;
        let mut trace_memory_coordinator_ms = 0;
        let mut trace_memory_tombstone_ms = 0;
        let mut trace_memory_add_ms = 0;
        if let Some(backend) = self.metadata_backend.as_mut() {
            let ensure_started = Instant::now();
            let file_system = backend
                .get_file_system(&transaction.file_system_id)?
                .unwrap_or_else(|| DfsFileSystem {
                    file_system_id: transaction.file_system_id.clone(),
                    display_name: transaction.file_system_id.to_string(),
                    owner_node_id: self.node_id.clone(),
                    subtree_policies: Vec::new(),
                });
            backend.ensure_file_system(&file_system)?;
            trace_ensure_fs_ms = ensure_started.elapsed().as_millis();
            let backend_apply_started = Instant::now();
            backend.apply_namespace_transaction(&transaction)?;
            trace_backend_apply_ms = backend_apply_started.elapsed().as_millis();
        }
        for object in transaction
            .objects
            .iter()
            .filter(|object| object.kind == DfsObjectKind::Tombstone)
            .chain(
                transaction
                    .objects
                    .iter()
                    .filter(|object| object.kind != DfsObjectKind::Tombstone),
            )
            .cloned()
        {
            let coordinator_started = Instant::now();
            self.coordinator.set_committed_version(
                object.file_system_id.clone(),
                object.object_id.clone(),
                object.version.clone(),
            );
            trace_memory_coordinator_ms += coordinator_started.elapsed().as_millis();
            let tombstone_started = Instant::now();
            self.track_metadata_tombstone(&object);
            trace_memory_tombstone_ms += tombstone_started.elapsed().as_millis();
            let add_started = Instant::now();
            self.metadata.add_object(object)?;
            trace_memory_add_ms += add_started.elapsed().as_millis();
        }
        dfs_perf_trace(format_args!(
            "apply-namespace-transaction node={} objects={} versions={} ensure_fs_ms={} backend_apply_ms={} memory_coordinator_ms={} memory_tombstone_ms={} memory_add_ms={} total_ms={}",
            self.node_id,
            trace_object_count,
            trace_version_count,
            trace_ensure_fs_ms,
            trace_backend_apply_ms,
            trace_memory_coordinator_ms,
            trace_memory_tombstone_ms,
            trace_memory_add_ms,
            trace_started.elapsed().as_millis()
        ));
        Ok(())
    }
}
