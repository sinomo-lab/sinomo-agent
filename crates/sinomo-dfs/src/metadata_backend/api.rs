//! Durable metadata interface implementation; writer admission stays in the parent backend.

use super::*;
use std::ops::Bound::{Included, Unbounded};

#[cfg(test)]
thread_local! {
    static INITIAL_NAMESPACE_BATCH_BEFORE_MATERIALIZE: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
    static FINAL_NAMESPACE_BATCH_BEFORE_MATERIALIZE: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}
#[cfg(test)]
pub(super) fn final_namespace_batch_before_materialize_for_test(action: impl FnOnce() + 'static) {
    FINAL_NAMESPACE_BATCH_BEFORE_MATERIALIZE
        .with(|hook| *hook.borrow_mut() = Some(Box::new(action)));
}

#[cfg(test)]
pub(super) fn initial_namespace_batch_before_materialize_for_test(action: impl FnOnce() + 'static) {
    INITIAL_NAMESPACE_BATCH_BEFORE_MATERIALIZE
        .with(|hook| *hook.borrow_mut() = Some(Box::new(action)));
}

impl DfsMetadataBackend for LocalSobjMetadataBackend {
    #[cfg(test)]
    fn test_receipt_scan_counts_by_shard(&self) -> BTreeMap<DfsMetadataShardId, (usize, usize)> {
        BTreeMap::from([(self.shard_id.clone(), self.test_receipt_scan_counts())])
    }
    #[cfg(test)]
    fn reset_test_receipt_scan_counts_by_shard(&self) {
        self.reset_test_receipt_scan_counts();
    }

    fn prepared_content_reference_protection(
        &self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        max_prepares: usize,
    ) -> Result<(bool, bool)> {
        self.current_tables_snapshot()?
            .prepared_content_reference_protection(file_system_id, object_id, max_prepares)
    }
    fn plan_cross_shard_pin_transfer(
        &self,
        transfer_id: &str,
        moved_object: &DfsObjectMetadata,
        source_path: &str,
        incoming_versions: &[DfsObjectVersionRecord],
        incoming_pins: &[DfsMetadataVersionPinRecord],
    ) -> Result<crate::metadata_api::DfsCrossShardPinTransferPlan> {
        self.plan_cross_shard_pin_transfer_current(
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
        self.plan_foreign_shard_source_eviction_current(
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
        self.current_prepared_operation_record(operation_id)
    }
    fn list_prepared_operations(&self) -> Result<Vec<DfsPreparedOperationRecord>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables
            .prepared_operations_v7
            .values()
            .filter(|record| record.status == DfsPreparedOperationStatus::Prepared)
            .cloned()
            .collect())
    }
    fn prepared_operation_reservations(
        &self,
        payload: &crate::DfsPreparedOperationPayload,
    ) -> Result<Vec<DfsPreparedOperationRecord>> {
        if payload.shard_id != self.shard_id {
            return Err(MetadataBackendError::Serde(
                "prepared reservation query names a foreign physical shard".into(),
            ));
        }
        Ok(self
            .current_tables_snapshot()?
            .prepared_operation_reservations(payload))
    }
    fn prepared_operation_for_request(
        &self,
        requester: &NodeId,
        request_id: &str,
    ) -> Result<Option<DfsPreparedOperationRecord>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables
            .prepared_operation_for_request(requester, request_id)?
            .cloned())
    }

    #[cfg(any(test, feature = "test-support"))]
    fn native_held_snapshot_for_scope(
        &self,
        fs: &DfsFileSystemId,
        shard: &DfsMetadataShardId,
    ) -> Option<crate::NativeHeldMetadataSnapshotV1> {
        if shard != &self.shard_id || self.tables.file_systems.keys().any(|actual| actual != fs) {
            return None;
        }
        Some(self.native_prepared_kernel_held_snapshot_v1(None))
    }

    fn prepare_operation(
        &mut self,
        payload: crate::metadata_records::DfsPreparedOperationPayload,
        source: Option<&crate::source_adoption::DfsSourceAdoptionWitness>,
    ) -> Result<crate::metadata_records::DfsPreparedOperationDigest> {
        self.prepare_operation_owned(payload, source)
    }

    fn prepared_operation_digest(
        &self,
        operation_id: &str,
    ) -> Result<Option<crate::metadata_records::DfsPreparedOperationDigest>> {
        self.current_prepared_operation_record(operation_id)
            .map(|record| record.map(|record| record.digest))
    }

    fn prepared_operation_payload(
        &self,
        operation_id: &str,
    ) -> Result<Option<Arc<crate::metadata_records::DfsPreparedOperationPayload>>> {
        self.current_prepared_operation_record(operation_id)
            .map(|record| record.map(|record| record.payload))
    }

    fn committed_prepared_operation(
        &self,
        digest: &crate::metadata_records::DfsPreparedOperationDigest,
    ) -> Result<Option<DfsMetadataJournalEntry>> {
        let tables = self.current_tables_snapshot()?;
        if let Some(record) = tables.prepared_operations_v7.get(&digest.operation_id) {
            record.validate()?;
            if record.digest != *digest || digest.shard_id != self.shard_id {
                return Err(MetadataBackendError::Serde(
                    "stored prepared outcome digest/scope mismatch".into(),
                ));
            }
            return Ok(record
                .committed_entry
                .as_ref()
                .map(|reference| reference.journal_entry()));
        }
        if let Some(witness) = tables
            .prepared_operation_evictions
            .get(&digest.operation_id)
        {
            witness.validate()?;
            if witness.digest != *digest || digest.shard_id != self.shard_id {
                return Err(MetadataBackendError::Serde(
                    "evicted outcome digest/scope mismatch".into(),
                ));
            }
            return Ok(witness
                .committed_entry
                .as_ref()
                .map(|reference| reference.journal_entry()));
        }
        Ok(None)
    }

    fn publication_owner_scope(
        &self,
        file_system_id: &DfsFileSystemId,
        shard_id: &DfsMetadataShardId,
    ) -> Result<crate::publication_guard::DfsPublicationOwnerScope> {
        self.publication_scope_for_backend(file_system_id, shard_id)
    }

    fn publication_writer_session(
        &mut self,
        file_system_id: &DfsFileSystemId,
        shard_id: &DfsMetadataShardId,
        digest: &crate::metadata_records::DfsPreparedOperationDigest,
        source: Option<&crate::DfsSourceAdoptionWitness>,
        provider: Option<&crate::publication_guard::DfsPublicationExecutionProvider>,
        execution: Option<&crate::publication_guard::DfsPublicationExecutionOwners>,
        clustered: Option<&crate::DfsMetadataShardConfig>,
    ) -> Result<crate::publication_guard::DfsPublicationWriterSession> {
        self.publication_writer_session_for_backend(
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
        let scope = self.publication_scope_for_backend(file_system_id, &self.shard_id)?;
        if &scope != session.owner_scope() {
            return Err(MetadataBackendError::Serde(
                "held writer filesystem lookup names a different owner".into(),
            ));
        }
        Ok(self.tables.file_systems.get(file_system_id).cloned())
    }

    fn publication_writer_object(
        &self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        session: &crate::publication_guard::DfsPublicationWriterSession,
    ) -> Result<Option<DfsObjectMetadata>> {
        let scope = self.publication_scope_for_backend(file_system_id, &self.shard_id)?;
        if &scope != session.owner_scope() {
            return Err(MetadataBackendError::Serde(
                "held writer object lookup names a different owner".into(),
            ));
        }
        Ok(self
            .tables
            .objects
            .get(object_id)
            .filter(|object| &object.file_system_id == file_system_id)
            .cloned())
    }

    fn commit_prepared_operation(
        &mut self,
        digest: &crate::metadata_records::DfsPreparedOperationDigest,
        guard: crate::publication_guard::DfsPublicationGuard,
        enrollment: Option<&mut dyn FnMut(&[crate::DfsRepairAttemptRecord]) -> Result<()>>,
    ) -> Result<crate::metadata_records::DfsPreparedOperationCommitResult> {
        self.commit_prepared_operation_owned(digest, guard, enrollment)
    }

    #[tracing::instrument(
        target = "sinomo.perf",
        level = "trace",
        name = "dfs.metadata.read_snapshot",
        skip_all,
        fields(stage = "dfs.metadata.read_snapshot")
    )]
    fn begin_consistent_read_snapshot(&mut self) -> Result<()> {
        if self.consistent_read_snapshot_depth == 0 {
            let _lock = self.metadata_write_lock()?;
            self.refresh_after_metadata_write_lock()?;
        }
        self.consistent_read_snapshot_depth = self.consistent_read_snapshot_depth.saturating_add(1);
        Ok(())
    }

    fn end_consistent_read_snapshot(&mut self) {
        self.consistent_read_snapshot_depth = self.consistent_read_snapshot_depth.saturating_sub(1);
    }

    fn get_file_system(&self, file_system_id: &DfsFileSystemId) -> Result<Option<DfsFileSystem>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables.file_systems.get(file_system_id).cloned())
    }
    fn get_file_system_on_shard(
        &self,
        shard_id: &DfsMetadataShardId,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<DfsFileSystem>> {
        self.require_replication_source_shard(shard_id)?;
        self.get_file_system(file_system_id)
    }

    fn list_file_systems(&self) -> Result<Vec<DfsFileSystem>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables.file_systems.values().cloned().collect())
    }

    fn get_object(&self, object_id: &DfsObjectId) -> Result<Option<DfsObjectMetadata>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables.objects.get(object_id).cloned())
    }

    fn get_object_on_shard(
        &self,
        shard_id: &DfsMetadataShardId,
        object_id: &DfsObjectId,
    ) -> Result<Option<DfsObjectMetadata>> {
        self.require_replication_source_shard(shard_id)?;
        self.get_object(object_id)
    }

    fn get_object_by_path_on_shard(
        &self,
        shard_id: &DfsMetadataShardId,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        self.require_replication_source_shard(shard_id)?;
        self.get_object_by_path(file_system_id, path)
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
        self.require_replication_source_shard(shard_id)?;
        let tables = self.current_tables_snapshot()?;
        namespace_prepare_expectations_from_lookups(
            transaction,
            |object_id| Ok(tables.objects.get(object_id).cloned()),
            |file_system_id, path| Ok(object_by_path_from_tables(&tables, file_system_id, path)),
        )
    }

    fn get_advisory_lock_state(
        &self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
    ) -> Result<Option<DfsAdvisoryLockStateRecord>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables
            .advisory_lock_states
            .get(object_id)
            .filter(|state| state.file_system_id == *file_system_id)
            .cloned())
    }

    fn get_object_by_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        let tables = self.current_tables_snapshot()?;
        Ok(object_by_path_from_tables(&tables, file_system_id, path))
    }

    fn objects_for_file_system(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Vec<DfsObjectMetadata>> {
        collect_metadata_pages(|cursor| {
            let page = self.objects_for_file_system_page(
                file_system_id,
                cursor,
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            Ok((page.objects, page.next_cursor))
        })
    }

    fn objects_for_file_system_page(
        &self,
        file_system_id: &DfsFileSystemId,
        after: Option<&DfsObjectPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectPage> {
        let tables = self.current_tables_snapshot()?;
        Ok(objects_for_file_system_page_from_tables(
            &tables,
            file_system_id,
            after,
            max_entries,
        ))
    }

    fn objects_for_subtree_page(
        &self,
        file_system_id: &DfsFileSystemId,
        subtree_path: &str,
        after: Option<&DfsObjectPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectPage> {
        let tables = self.current_tables_snapshot()?;
        Ok(crate::metadata_pages::objects_for_subtree_page_from_tables(
            &tables,
            file_system_id,
            subtree_path,
            after,
            max_entries,
        ))
    }

    fn count_objects_for_file_system(&self, file_system_id: &DfsFileSystemId) -> Result<usize> {
        let tables = self.current_tables_snapshot()?;
        Ok(count_objects_for_file_system_from_tables(
            &tables,
            file_system_id,
        ))
    }

    fn child_tombstones_for_directory(
        &self,
        file_system_id: &DfsFileSystemId,
        directory_path: &str,
    ) -> Result<Vec<DfsObjectMetadata>> {
        let tables = self.current_tables_snapshot()?;
        let directory_path = normalize_path(directory_path);
        let prefix = if directory_path.is_empty() {
            String::new()
        } else {
            format!("{directory_path}/")
        };
        let is_direct_child = |path: &str| {
            let Some(rest) = path.strip_prefix(&prefix) else {
                return false;
            };
            !rest.is_empty() && !rest.contains('/')
        };
        let mut tombstones = BTreeMap::<String, DfsObjectMetadata>::new();
        for ((_record_file_system_id, path), path_record) in tables
            .paths
            .range((
                Included((file_system_id.clone(), prefix.clone())),
                Unbounded,
            ))
            .take_while(|((record_file_system_id, path), _)| {
                record_file_system_id == file_system_id
                    && (prefix.is_empty() || path.starts_with(&prefix))
            })
        {
            if !is_direct_child(path) {
                continue;
            }
            let object = tables
                .objects
                .get(&path_record.object_id)
                .cloned()
                .or_else(|| tables.compacted_tombstone_by_path(file_system_id, path));
            let Some(object) = object.filter(|object| object.kind == DfsObjectKind::Tombstone)
            else {
                continue;
            };
            tombstones.insert(path.clone(), object);
        }
        for ((record_file_system_id, path), object_id) in tables
            .compacted_tombstone_paths
            .range((
                Included((file_system_id.clone(), prefix.clone())),
                Unbounded,
            ))
            .take_while(|((record_file_system_id, path), _)| {
                record_file_system_id == file_system_id
                    && (prefix.is_empty() || path.starts_with(&prefix))
            })
        {
            if !is_direct_child(path) {
                continue;
            }
            if let Some(object) = tables.compacted_tombstones.get(object_id)
                && object.file_system_id == *record_file_system_id
                && object.kind == DfsObjectKind::Tombstone
            {
                tombstones
                    .entry(path.clone())
                    .or_insert_with(|| object.clone());
            }
        }
        Ok(tombstones.into_values().collect())
    }

    fn child_objects_for_directory(
        &self,
        file_system_id: &DfsFileSystemId,
        directory_path: &str,
    ) -> Result<Vec<DfsObjectMetadata>> {
        let tables = self.current_tables_snapshot()?;
        Ok(child_objects_for_directory_from_tables(
            &tables,
            file_system_id,
            directory_path,
        ))
    }

    fn child_objects_for_directory_page(
        &self,
        file_system_id: &DfsFileSystemId,
        directory_path: &str,
        after: Option<&DfsDirectoryPageCursor>,
        max_entries: usize,
    ) -> Result<DfsDirectoryPage> {
        let page = self.child_objects_for_directory_page_entries(
            file_system_id,
            directory_path,
            after,
            max_entries,
        )?;
        Ok(DfsDirectoryPage {
            objects: page.entries.into_iter().map(|(object, _)| object).collect(),
            next_cursor: page.next_cursor,
        })
    }

    fn directory_page_cursor_for_object(
        &self,
        object: &DfsObjectMetadata,
    ) -> Result<DfsDirectoryPageCursor> {
        self.indexed_directory_page_cursor_for_object(object)
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
        let page = LocalSobjMetadataBackend::child_objects_for_directory_page_entries(
            self,
            file_system_id,
            directory_path,
            after,
            max_entries,
        )?;
        Ok((page.entries, page.next_cursor))
    }

    fn hardlinks_to_object(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
    ) -> Result<Vec<DfsObjectMetadata>> {
        collect_metadata_pages(|cursor| {
            let page = self.hardlinks_to_object_page(
                file_system_id,
                target_object_id,
                cursor,
                OBJECT_LOCAL_METADATA_PAGE_SIZE,
            )?;
            Ok((page.objects, page.next_cursor))
        })
    }

    fn hardlinks_to_object_page(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
        after: Option<&DfsObjectPageCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectPage> {
        let tables = self.current_tables_snapshot()?;
        Ok(hardlinks_to_object_page_from_tables(
            &tables,
            file_system_id,
            target_object_id,
            after,
            max_entries,
        ))
    }

    fn hardlink_count_to_object(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
    ) -> Result<usize> {
        let tables = self.current_tables_snapshot()?;
        Ok(hardlink_count_to_object_from_tables(
            &tables,
            file_system_id,
            target_object_id,
        ))
    }

    fn first_hardlink_to_object(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
    ) -> Result<Option<DfsObjectMetadata>> {
        Ok(self
            .hardlinks_to_object_page(file_system_id, target_object_id, None, 1)?
            .objects
            .into_iter()
            .next())
    }

    fn first_hardlink_to_object_outside_prefix(
        &self,
        file_system_id: &DfsFileSystemId,
        target_object_id: &DfsObjectId,
        prefix: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        let tables = self.current_tables_snapshot()?;
        Ok(first_hardlink_to_object_outside_prefix_from_tables(
            &tables,
            file_system_id,
            target_object_id,
            prefix,
        ))
    }

    fn version_pins_for_object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<Vec<DfsMetadataVersionPinRecord>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables
            .version_pins
            .values()
            .filter(|pin| &pin.object_id == object_id)
            .cloned()
            .collect())
    }

    fn version_pins_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsMetadataVersionPinPageCursor>,
        max_entries: usize,
    ) -> Result<DfsMetadataVersionPinPage> {
        let tables = self.current_tables_snapshot()?;
        Ok(version_pins_for_object_page_from_tables(
            &tables,
            object_id,
            after,
            max_entries,
        ))
    }

    fn cross_shard_pin_transfers_for_object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<Vec<DfsCrossShardPinTransferRecord>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables
            .cross_shard_pin_transfers
            .values()
            .filter(|transfer| &transfer.object_id == object_id)
            .cloned()
            .collect())
    }

    fn capture_cross_shard_source_pin_fence(
        &mut self,
        object_id: &DfsObjectId,
    ) -> Result<DfsCrossShardVersionPinFence> {
        LocalSobjMetadataBackend::capture_cross_shard_source_pin_fence(self, object_id)
    }

    fn capture_source_adoption_source(
        &mut self,
        object_id: &DfsObjectId,
        source_path: &str,
    ) -> Result<DfsSourceAdoptionSourceCapture> {
        self.capture_source_adoption_source_for_backend(object_id, source_path)
    }

    fn capture_source_adoption_job(&mut self, job_id: &str) -> Result<DfsSourceAdoptionJobCapture> {
        self.capture_source_adoption_job_for_backend(job_id)
    }

    fn capture_source_adoption(
        &mut self,
        object_id: &DfsObjectId,
        source_path: &str,
        job_id: &str,
    ) -> Result<DfsSourceAdoptionCapture> {
        self.capture_source_adoption_for_backend(object_id, source_path, job_id)
    }

    fn capture_cross_shard_source_family(
        &mut self,
        object_id: &DfsObjectId,
        source_path: &str,
    ) -> Result<DfsCrossShardSourceFamily> {
        let capture = LocalSobjMetadataBackend::capture_cross_shard_source_family(self, object_id)?;
        if normalize_path(&capture.source_object.path) != normalize_path(source_path) {
            return Err(MetadataBackendError::Serde(format!(
                "object {} is at {}, not requested source path {}",
                object_id, capture.source_object.path, source_path
            )));
        }
        Ok(capture)
    }

    fn get_version(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<Option<DfsObjectVersionRecord>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables
            .versions
            .get(&(object_id.clone(), version.clone()))
            .cloned())
    }

    fn get_object_version_pairs(
        &self,
        keys: &[(DfsObjectId, DfsVersion)],
    ) -> Result<Vec<(Option<DfsObjectMetadata>, Option<DfsObjectVersionRecord>)>> {
        let tables = self.current_tables_snapshot()?;
        Ok(keys
            .iter()
            .map(|(object_id, version)| {
                (
                    tables.objects.get(object_id).cloned(),
                    tables
                        .versions
                        .get(&(object_id.clone(), version.clone()))
                        .cloned(),
                )
            })
            .collect())
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
        let tables = self.current_tables_snapshot()?;
        Ok(versions_for_object_page_from_tables(
            &tables,
            object_id,
            after,
            max_entries,
        ))
    }

    fn query_file_history_on_shard(
        &self,
        query: &crate::DfsMetadataShardQuery,
    ) -> Result<crate::DfsMetadataQueryResult> {
        if !query.shard_id.0.is_empty() && query.shard_id != self.shard_id {
            return Err(MetadataBackendError::Serde(
                "file history query names another metadata shard".into(),
            ));
        }
        crate::file_history_query::query_backend_metadata(self, query)
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
        let tables = self.current_tables_snapshot()?;
        Ok(retained_revision_records_page_from_tables(
            &tables,
            namespace,
            file_system_id,
            object_id,
            path_prefix,
            after,
            max_records,
        ))
    }

    fn version_records_page(
        &self,
        after: Option<&DfsObjectVersionScanCursor>,
        max_entries: usize,
    ) -> Result<DfsObjectVersionScanPage> {
        let tables = self.current_tables_snapshot()?;
        Ok(version_records_page_from_tables(
            &tables,
            after,
            max_entries,
        ))
    }

    fn receipt_records_page(
        &self,
        after: Option<&DfsReplicaReceiptScanCursor>,
        max_entries: usize,
    ) -> Result<DfsReplicaReceiptScanPage> {
        #[cfg(test)]
        self.receipt_scan_counters
            .global_pages
            .fetch_add(1, Ordering::Relaxed);
        let tables = self.current_tables_snapshot()?;
        Ok(receipt_records_page_from_tables(
            &tables,
            after,
            max_entries,
        ))
    }

    fn all_intents(&self) -> Result<Vec<DfsWriteIntentRecord>> {
        collect_metadata_pages(|cursor| {
            let page = self.write_intents_page(cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            Ok((page.intents, page.next_cursor))
        })
    }

    fn metadata_summary(&self) -> Result<DfsMetadataSummary> {
        let tables = self.current_tables_snapshot()?;
        metadata_summary_from_tables(&tables, |_| Ok(true))
    }

    fn metadata_status_summaries(&self) -> Result<(DfsMetadataSummary, DfsMetadataSummary)> {
        let tables = self.current_tables_snapshot()?;
        let summary = tables.metadata_status_summary();
        Ok((summary.clone(), summary))
    }

    fn metadata_summary_for_file_system(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<DfsMetadataSummary>> {
        let tables = self.current_tables_snapshot()?;
        metadata_summary_for_file_system_from_tables(&tables, file_system_id, |_| Ok(true))
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
        #[cfg(test)]
        self.receipt_scan_counters
            .object_version_pages
            .fetch_add(1, Ordering::Relaxed);
        let tables = self.current_tables_snapshot()?;
        Ok(receipts_for_version_page_from_tables(
            &tables,
            object_id,
            version,
            after,
            max_entries,
        ))
    }

    fn has_current_placement_repair_owner(&self, object: &DfsObjectMetadata) -> Result<bool> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables
            .intent_targets
            .range((
                std::ops::Bound::Included((
                    object.object_id.clone(),
                    object.version.clone(),
                    String::new(),
                )),
                std::ops::Bound::Unbounded,
            ))
            .take_while(|((id, version, _), _)| {
                id == &object.object_id && version == &object.version
            })
            .filter_map(|(_, key)| tables.intents.get(key))
            .any(|intent| {
                intent.publication.is_current()
                    && intent.status == DfsWriteIntentStatus::Committed
                    && intent.content_hash == object.content_hash
                    && crate::placement::placement_reconciliation_job_id_from_intent_id(
                        &intent.intent_id,
                    )
                    .is_some()
            }))
    }

    fn object_write_intent_history(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<DfsObjectWriteIntentHistory> {
        let tables = self.current_tables_snapshot()?;
        Ok(DfsObjectWriteIntentHistory {
            intents: tables
                .intents
                .values()
                .filter(|intent| intent.object_id == *object_id)
                .cloned()
                .collect(),
            receipts: tables
                .intent_receipts
                .values()
                .filter(|receipt| receipt.object_id == *object_id)
                .cloned()
                .collect(),
            retention: tables.intent_receipt_retention.for_object(object_id),
        })
    }

    fn get_intent_receipt(
        &self,
        object_id: &DfsObjectId,
        intent_id: &str,
    ) -> Result<Option<DfsWriteIntentReceipt>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables
            .intent_receipts
            .get(&(object_id.clone(), intent_id.to_string()))
            .cloned())
    }

    fn intent_receipt_retention_for_object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<DfsWriteIntentReceiptRetentionState> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables.intent_receipt_retention.for_object(object_id))
    }

    fn get_intent(
        &self,
        object_id: &DfsObjectId,
        intent_id: &str,
    ) -> Result<Option<DfsWriteIntentRecord>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables.intent_record(object_id, intent_id))
    }

    fn get_intent_on_shard(
        &self,
        shard_id: &DfsMetadataShardId,
        object_id: &DfsObjectId,
        intent_id: &str,
    ) -> Result<Option<DfsWriteIntentRecord>> {
        if shard_id != &self.shard_id {
            return Ok(None);
        }
        self.get_intent(object_id, intent_id)
    }

    fn get_receipt_on_shard(
        &self,
        shard_id: &DfsMetadataShardId,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
    ) -> Result<Option<DfsReplicaReceiptRecord>> {
        if shard_id != &self.shard_id {
            return Ok(None);
        }
        LocalSobjMetadataBackend::receipt_for_replica(self, object_id, version, replica_node_id)
    }

    fn write_intent_counter_floor(&self) -> Result<u64> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables.intent_receipt_retention.max_evicted_counter())
    }

    fn intents_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsWriteIntentRecord>> {
        collect_metadata_pages(|cursor| {
            let page =
                self.intents_for_object_page(object_id, cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            Ok((page.intents, page.next_cursor))
        })
    }

    fn intents_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsWriteIntentPageCursor>,
        max_entries: usize,
    ) -> Result<DfsWriteIntentPage> {
        let tables = self.current_tables_snapshot()?;
        Ok(intents_for_object_page_from_tables(
            &tables,
            object_id,
            after,
            max_entries,
        ))
    }

    fn intents_for_object_version_page(
        &self,
        object_id: &DfsObjectId,
        target_version: &DfsVersion,
        after: Option<&DfsWriteIntentPageCursor>,
        max_entries: usize,
    ) -> Result<DfsWriteIntentPage> {
        let tables = self.current_tables_snapshot()?;
        Ok(intents_for_object_version_page_from_tables(
            &tables,
            object_id,
            target_version,
            after,
            max_entries,
        ))
    }

    fn write_intents_page(
        &self,
        after: Option<&DfsWriteIntentScanCursor>,
        max_entries: usize,
    ) -> Result<DfsWriteIntentScanPage> {
        let tables = self.current_tables_snapshot()?;
        Ok(write_intents_page_from_tables(&tables, after, max_entries))
    }

    fn write_intent_maintenance_page(
        &self,
        after: Option<&crate::DfsWriteIntentMaintenanceCursor>,
        max_records: usize,
    ) -> Result<crate::DfsWriteIntentMaintenancePage> {
        if max_records == 0 {
            return Ok(crate::DfsWriteIntentMaintenancePage {
                next_cursor: after.cloned(),
                ..Default::default()
            });
        }
        if after.is_some_and(|cursor| cursor.shard_id > self.shard_id) {
            return Ok(crate::DfsWriteIntentMaintenancePage::default());
        }
        let tables = self.current_tables_snapshot()?;
        Ok(
            crate::metadata_intent_maintenance::bounded_intents_from_tables(
                &tables,
                &self.shard_id,
                after,
                max_records,
            ),
        )
    }

    fn write_intent_maintenance_candidate_is_listable(
        &self,
        shard_id: &DfsMetadataShardId,
        intent: &DfsWriteIntentRecord,
    ) -> Result<bool> {
        Ok(shard_id == &self.shard_id
            && crate::intent::write_intent_status_is_active(&intent.status))
    }

    fn write_intents_on_shard_page(
        &self,
        shard_id: &DfsMetadataShardId,
        after: Option<&DfsWriteIntentScanCursor>,
        max_entries: usize,
    ) -> Result<Option<DfsWriteIntentScanPage>> {
        if shard_id != &self.shard_id {
            return Ok(None);
        }
        self.write_intents_page(after, max_entries).map(Some)
    }

    fn tombstone_compaction_projection(
        &self,
        options: DfsTombstoneCompactionOptions,
    ) -> Result<DfsTombstoneCompactionReport> {
        let tables = self.current_tables_snapshot()?;
        let now = options
            .now_unix_nanos
            .unwrap_or_else(metadata_now_unix_nanos);
        Ok(plan_tombstone_compaction(&tables, &options, now).report)
    }

    fn metadata_history_gc_projection(
        &self,
        options: DfsMetadataHistoryGcOptions,
    ) -> Result<DfsMetadataHistoryGcReport> {
        let tables = self.current_tables_snapshot()?;
        let snapshot_size_bytes = fs::metadata(metadata_table_snapshot_path(&self.root))
            .ok()
            .map(|metadata| metadata.len());
        let mut report = plan_metadata_history_gc(
            self.shard_id.clone(),
            &tables,
            &options,
            snapshot_size_bytes,
        )?
        .report;
        self.populate_metadata_history_storage_stats(&mut report)?;
        Ok(report)
    }

    fn plan_metadata_history_gc(
        &mut self,
        options: DfsMetadataHistoryGcOptions,
    ) -> Result<crate::metadata_api::DfsMetadataHistoryEffectsPlan> {
        self.plan_metadata_history_effects(options)
    }

    fn content_root_catalog_page(
        &mut self,
        request: &DfsContentRootCatalogPageRequest,
    ) -> Result<DfsContentRootCatalogPage> {
        request.validate()?;
        let generated_at_unix_nanos = request
            .after
            .as_ref()
            .map(|cursor| cursor.generated_at_unix_nanos)
            .unwrap_or_else(metadata_now_unix_nanos);
        if request.identity.shard_id != self.shard_id {
            return Ok(DfsContentRootCatalogPage::incomplete_for_request(
                request,
                DfsContentRootCatalogIncompleteReason::UnknownShard,
                generated_at_unix_nanos,
            ));
        }

        if request.after.is_some() {
            // Freeze installation uses this same cross-process writer fence.
            // Select and clone the immutable cut while holding it, then release
            // before slicing or encoding the page.
            let cache_lock = self.metadata_write_lock()?;
            if let Some(freeze) = self.membership_handoff_write_freeze()?
                && freeze
                    .activated_epoch
                    .is_none_or(|epoch| self.instance_authority_epoch != Some(epoch))
            {
                return Ok(DfsContentRootCatalogPage::incomplete_for_request(
                    request,
                    DfsContentRootCatalogIncompleteReason::MembershipHandoffFrozen {
                        handoff_id: Some(freeze.handoff_id),
                    },
                    generated_at_unix_nanos,
                ));
            }
            let cached_snapshot = [
                self.content_root_catalog_snapshot_cache.as_ref(),
                self.content_root_catalog_previous_snapshot_cache.as_ref(),
            ]
            .into_iter()
            .flatten()
            .find(|snapshot| snapshot.satisfies_request(request))
            .cloned();
            drop(cache_lock);
            if let Some(snapshot) = cached_snapshot {
                return content_root_catalog_page_from_snapshot(
                    request,
                    generated_at_unix_nanos,
                    &snapshot,
                );
            }
        }

        // Build one immutable exact-cut snapshot under the cross-process
        // writer fence. Page slicing and encoding happen after releasing the
        // fence, and exact continuation requests can reuse the snapshot even
        // when later metadata writes advance the live journal.
        let lock = self.metadata_write_lock()?;
        self.refresh_after_metadata_write_lock()?;
        if let Some(freeze) = self.membership_handoff_write_freeze()?
            && freeze
                .activated_epoch
                .is_none_or(|epoch| self.instance_authority_epoch != Some(epoch))
        {
            return Ok(DfsContentRootCatalogPage::incomplete_for_request(
                request,
                DfsContentRootCatalogIncompleteReason::MembershipHandoffFrozen {
                    handoff_id: Some(freeze.handoff_id),
                },
                generated_at_unix_nanos,
            ));
        }

        let journal_sequence = self.materialized_through_sequence;
        if journal_sequence < request.minimum_journal_sequence {
            return Ok(DfsContentRootCatalogPage::incomplete_for_request(
                request,
                DfsContentRootCatalogIncompleteReason::DurableCutUnavailable {
                    minimum_journal_sequence: request.minimum_journal_sequence,
                    observed_journal_sequence: journal_sequence,
                },
                generated_at_unix_nanos,
            ));
        }

        let prefix_proof = self.prefix_proof.as_ref().ok_or_else(|| {
            MetadataBackendError::Serde(format!(
                "metadata shard {} has no durable prefix proof for content-root catalog",
                self.shard_id.0
            ))
        })?;
        if !prefix_proof.is_well_formed() || prefix_proof.journal_sequence != journal_sequence {
            return Err(MetadataBackendError::Serde(format!(
                "metadata shard {} content-root catalog prefix proof does not match durable sequence {}",
                self.shard_id.0, journal_sequence
            )));
        }
        let state_fingerprint = prefix_proof.digest.clone();
        let prefix_proof_version = u32::from(prefix_proof.version);

        let cut_changed = request.after.as_ref().is_some_and(|after| {
            after.journal_sequence != journal_sequence
                || after.state_fingerprint != state_fingerprint
        }) || (request.after.is_none()
            && journal_sequence == request.minimum_journal_sequence
            && request
                .minimum_state_fingerprint
                .as_ref()
                .is_some_and(|expected| expected != &state_fingerprint));
        if cut_changed {
            return Ok(DfsContentRootCatalogPage::incomplete_for_request(
                request,
                DfsContentRootCatalogIncompleteReason::StateFingerprintChanged,
                generated_at_unix_nanos,
            ));
        }

        let cache_matches = self
            .content_root_catalog_snapshot_cache
            .as_ref()
            .is_some_and(|cache| {
                cache.journal_sequence == journal_sequence
                    && cache.prefix_proof_version == prefix_proof_version
                    && cache.state_fingerprint == state_fingerprint
            });
        if !cache_matches {
            let protection = metadata_content_protection_from_tables(&self.tables);
            let entries = dfs_content_root_catalog_entries_from_canonical_protection(protection);
            self.content_root_catalog_previous_snapshot_cache =
                self.content_root_catalog_snapshot_cache.take();
            self.content_root_catalog_snapshot_cache =
                Some(Arc::new(DfsContentRootCatalogSnapshotCache {
                    prefix_proof_version,
                    journal_sequence,
                    state_fingerprint: state_fingerprint.clone(),
                    entries: Arc::new(entries),
                }));
            #[cfg(test)]
            self.content_root_catalog_snapshot_builds
                .fetch_add(1, Ordering::Relaxed);
        }
        let snapshot = self
            .content_root_catalog_snapshot_cache
            .as_ref()
            .cloned()
            .expect("content-root catalog snapshot cache was populated");
        drop(lock);
        content_root_catalog_page_from_snapshot(request, generated_at_unix_nanos, &snapshot)
    }

    fn metadata_content_protection(&self) -> Result<DfsMetadataContentProtection> {
        let tables = self.current_tables_snapshot()?;
        Ok(metadata_content_protection_from_tables(&tables))
    }

    fn compact_tombstones(
        &mut self,
        options: DfsTombstoneCompactionOptions,
    ) -> Result<DfsTombstoneCompactionReport> {
        self.compact_tombstones_with_ids(options)
            .map(|(report, _)| report)
    }

    fn get_namespace_request(
        &self,
        requester_node_id: &NodeId,
        request_id: &str,
    ) -> Result<Option<DfsNamespaceRequestRecord>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables
            .namespace_requests
            .get(&(requester_node_id.clone(), request_id.to_string()))
            .cloned())
    }

    fn get_namespace_request_receipt(
        &self,
        requester_node_id: &NodeId,
        request_id: &str,
    ) -> Result<Option<DfsNamespaceRequestReceipt>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables
            .namespace_request_receipts
            .get(&(requester_node_id.clone(), request_id.to_string()))
            .cloned())
    }

    fn namespace_request_history_expired_on_shard(
        &self,
        shard_id: &DfsMetadataShardId,
        requester_node_id: &NodeId,
        request_id: &str,
    ) -> Result<bool> {
        if shard_id != &self.shard_id {
            return Err(MetadataBackendError::Serde(format!(
                "namespace request retention shard {} does not match backend shard {}",
                shard_id.0, self.shard_id.0
            )));
        }
        let tables = self.current_tables_snapshot()?;
        Ok(tables.namespace_request_history_expired(requester_node_id, request_id))
    }

    fn namespace_request_admission_snapshot(
        &mut self,
        shard_id: &DfsMetadataShardId,
        requester_node_id: &NodeId,
        request_id: &str,
    ) -> Result<DfsNamespaceRequestAdmissionSnapshot> {
        if shard_id != &self.shard_id {
            return Err(MetadataBackendError::Serde(format!(
                "namespace request admission shard {} does not match backend shard {}",
                shard_id.0, self.shard_id.0
            )));
        }
        let tables = self.current_tables_snapshot()?;
        let request_key = (requester_node_id.clone(), request_id.to_string());
        let request = tables.namespace_requests.get(&request_key).cloned();
        let receipt = tables.namespace_request_receipts.get(&request_key).cloned();
        let prepared = tables
            .prepared_operation_for_request(requester_node_id, request_id)?
            .cloned();
        let history_expired = request.is_none()
            && receipt.is_none()
            && prepared.is_none()
            && tables.namespace_request_history_expired(requester_node_id, request_id);
        Ok(DfsNamespaceRequestAdmissionSnapshot {
            request,
            receipt,
            prepared,
            history_expired,
        })
    }

    fn list_namespace_requests(&self) -> Result<Vec<DfsNamespaceRequestRecord>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables.namespace_requests.values().cloned().collect())
    }

    fn list_pending_namespace_requests_page(
        &self,
        after: Option<&DfsPendingNamespaceRequestPageCursor>,
        max_entries: usize,
    ) -> Result<DfsPendingNamespaceRequestPage> {
        let tables = self.current_tables_snapshot()?;
        Ok(pending_namespace_requests_page_from_tables(
            &tables,
            after,
            max_entries,
        ))
    }

    fn get_namespace_transaction_receipt(
        &self,
        transaction_id: &str,
    ) -> Result<Option<DfsNamespaceTransactionReceipt>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables
            .namespace_transaction_receipts
            .get(transaction_id)
            .cloned())
    }

    fn content_reference_outcomes_for_object_page(
        &self,
        fs: &DfsFileSystemId,
        object: &DfsObjectId,
        after: Option<&str>,
        limit: usize,
    ) -> Result<crate::DfsContentReferenceOutcomePage> {
        self.reference_outcome_page(fs, object, after, limit)
    }

    fn content_reference_outcome_retention(
        &self,
        fs: &DfsFileSystemId,
        object: &DfsObjectId,
    ) -> Result<Option<crate::DfsContentReferenceOutcomeRetention>> {
        self.reference_outcome_retention(fs, object)
    }

    fn get_namespace_job(&self, job_id: &str) -> Result<Option<DfsNamespaceJobRecord>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables.namespace_jobs.get(job_id).cloned())
    }

    fn get_namespace_job_receipt(&self, job_id: &str) -> Result<Option<DfsNamespaceJobReceipt>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables.namespace_job_receipts.get(job_id).cloned())
    }

    fn completed_namespace_job_receipt_for_paths_since(
        &self,
        file_system_id: &DfsFileSystemId,
        root_path: &str,
        target_path: &str,
        kinds: &[DfsNamespaceJobKind],
        created_at: OffsetDateTime,
        origin: &DfsNamespaceJobOrigin,
    ) -> Result<Option<DfsNamespaceJobReceipt>> {
        let tables = self.current_tables_snapshot()?;
        let root_path = normalize_path(root_path);
        let target_path = normalize_path(target_path);
        Ok(tables
            .namespace_job_receipts
            .values()
            .filter(|receipt| {
                receipt.file_system_id == *file_system_id
                    && receipt.status == DfsNamespaceJobStatus::Completed
                    && kinds.contains(&receipt.kind)
                    && receipt.created_at >= created_at
                    && normalize_path(&receipt.root_path) == root_path
                    && receipt
                        .target_path
                        .as_deref()
                        .is_some_and(|path| normalize_path(path) == target_path)
                    && receipt
                        .origin
                        .as_ref()
                        .is_some_and(|actual| actual == origin)
            })
            .max_by_key(|receipt| receipt.created_at)
            .cloned())
    }

    fn list_namespace_jobs(&self) -> Result<Vec<DfsNamespaceJobRecord>> {
        collect_metadata_pages(|cursor| {
            let page = self.list_namespace_jobs_page(cursor, OBJECT_LOCAL_METADATA_PAGE_SIZE)?;
            Ok((page.jobs, page.next_cursor))
        })
    }

    fn list_namespace_jobs_page(
        &self,
        after: Option<&DfsNamespaceJobPageCursor>,
        max_entries: usize,
    ) -> Result<DfsNamespaceJobPage> {
        let tables = self.current_tables_snapshot()?;
        Ok(namespace_jobs_page_from_tables(&tables, after, max_entries))
    }

    fn journal_since(&self, after_sequence: u64) -> Result<Vec<DfsMetadataJournalEntry>> {
        LocalSobjMetadataBackend::journal_since_limited(self, after_sequence, usize::MAX)
    }

    fn journal_since_limited(
        &self,
        after_sequence: u64,
        max_entries: usize,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        LocalSobjMetadataBackend::journal_since_limited(self, after_sequence, max_entries)
    }

    fn journal_batch_since_limited(
        &self,
        after_sequence: u64,
        max_entries: usize,
        max_encoded_bytes: usize,
    ) -> Result<Option<DfsMetadataJournalBatch>> {
        LocalSobjMetadataBackend::journal_batch_since_limited(
            self,
            after_sequence,
            max_entries,
            max_encoded_bytes,
        )
    }

    fn journal_batch_for_shard_since_limited(
        &self,
        shard_id: &DfsMetadataShardId,
        after_sequence: u64,
        max_entries: usize,
        max_encoded_bytes: usize,
    ) -> Result<Option<DfsMetadataJournalBatch>> {
        if shard_id != &self.shard_id {
            return Ok(None);
        }
        LocalSobjMetadataBackend::journal_batch_since_limited(
            self,
            after_sequence,
            max_entries,
            max_encoded_bytes,
        )
    }

    fn journal_frames_for_batch(
        &self,
        batch: &DfsMetadataJournalBatch,
    ) -> Result<Vec<DfsMetadataJournalFrameRecord>> {
        self.require_replication_source_shard(&batch.shard_id)?;
        let sequences = batch
            .entries
            .iter()
            .map(|entry| entry.sequence)
            .collect::<Vec<_>>();
        let exact_frames = read_segmented_journal_frames_at_sequences(&self.root, &sequences)?;
        batch
            .entries
            .iter()
            .zip(exact_frames)
            .map(|(entry, frame)| {
                let frame = frame.ok_or_else(|| {
                    MetadataBackendError::Serde(format!(
                        "metadata shard {} has no exact journal frame at sequence {}",
                        self.shard_id.0, entry.sequence
                    ))
                })?;
                if frame.entry() != entry {
                    return Err(MetadataBackendError::Serde(format!(
                        "metadata shard {} exact journal frame {} differs from selected batch entry",
                        self.shard_id.0, entry.sequence
                    )));
                }
                Ok(DfsMetadataJournalFrameRecord {
                    sequence: entry.sequence,
                    exact_bytes: bytes::Bytes::from_owner(frame.exact_bytes_arc()),
                })
            })
            .collect()
    }

    fn journal_last_sequence(&self) -> Result<u64> {
        Ok(LocalSobjMetadataBackend::journal_last_sequence(self))
    }

    fn export_table_snapshot(&self) -> Result<DfsMetadataTableSnapshot> {
        Ok(LocalSobjMetadataBackend::export_table_snapshot(self))
    }

    fn export_shard_snapshot_image(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Option<crate::DfsMetadataSnapshotImage>> {
        if shard_id != &self.shard_id {
            return Ok(None);
        }
        Ok(Some(self.export_snapshot_image()?))
    }

    fn export_shard_table_snapshot(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataTableSnapshot>> {
        if shard_id == &self.shard_id {
            return Ok(Some(LocalSobjMetadataBackend::export_table_snapshot(self)));
        }
        Ok(None)
    }

    fn metadata_prefix_proof(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataPrefixProof>> {
        if shard_id != &self.shard_id {
            return Ok(None);
        }
        LocalSobjMetadataBackend::metadata_prefix_proof(self).map(Some)
    }

    fn subscribe_metadata_replica_progress(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Option<crate::DfsMetadataReplicaObserver>> {
        Ok((shard_id == &self.shard_id).then(|| self.replica_progress.clone()))
    }

    #[tracing::instrument(
        target = "sinomo.perf",
        level = "trace",
        name = "dfs.replica.pin_prefix",
        skip_all,
        fields(stage = "dfs.replica.pin_prefix")
    )]
    fn begin_replica_projection_read(
        &mut self,
        observed: &crate::DfsMetadataReplicaObservation,
    ) -> Result<Option<DfsMetadataPrefixProof>> {
        if observed.shard_id != self.shard_id {
            return Ok(None);
        }
        self.begin_consistent_read_snapshot()?;
        let result = (|| {
            let current = LocalSobjMetadataBackend::metadata_prefix_proof(self)?;
            if current != observed.prefix {
                self.verify_historical_prefix_against_durable_floor(
                    &observed.prefix,
                    &DfsMetadataPrefixAckFloor {
                        shard_id: self.shard_id.clone(),
                        journal_sequence: current.journal_sequence,
                        state_fingerprint: String::new(),
                        durable_prefix_proof: current.clone(),
                    },
                    current.journal_sequence,
                    &self.shard_id,
                )?;
            }
            Ok(Some(current))
        })();
        if result.is_err() {
            self.end_consistent_read_snapshot();
        }
        result
    }

    fn end_replica_projection_read(&mut self, shard_id: &DfsMetadataShardId) {
        if shard_id == &self.shard_id {
            self.end_consistent_read_snapshot();
        }
    }

    fn replica_projection_namespace_jobs_page(
        &self,
        shard_id: &DfsMetadataShardId,
        after: Option<&DfsNamespaceJobPageCursor>,
        max_entries: usize,
    ) -> Result<DfsNamespaceJobPage> {
        self.require_replication_source_shard(shard_id)?;
        self.list_namespace_jobs_page(after, max_entries)
    }

    fn refresh_metadata_shard_durable_view(&mut self, shard_id: &DfsMetadataShardId) -> Result<()> {
        if shard_id != &self.shard_id {
            return Ok(());
        }
        let _lock = self.metadata_write_lock()?;
        self.refresh_after_metadata_write_lock()
    }

    fn metadata_shard_write_freeze(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataShardWriteFreeze>> {
        if shard_id != &self.shard_id {
            return Ok(None);
        }
        self.membership_handoff_write_freeze()
    }

    fn metadata_shard_authoritative_writes_frozen(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<bool> {
        if shard_id != &self.shard_id {
            return Ok(false);
        }
        let Some(freeze) = self.membership_handoff_write_freeze()? else {
            return Ok(false);
        };
        Ok(freeze
            .activated_epoch
            .is_none_or(|epoch| self.instance_authority_epoch != Some(epoch)))
    }

    fn freeze_metadata_shard_for_membership_handoff(
        &mut self,
        shard_id: &DfsMetadataShardId,
        handoff_id: &str,
        retiring_epoch: u64,
    ) -> Result<Option<DfsMetadataShardWriteFreeze>> {
        if shard_id != &self.shard_id {
            return Ok(None);
        }
        self.freeze_for_membership_handoff(handoff_id, retiring_epoch)
            .map(Some)
    }

    fn mark_metadata_shard_membership_handoff_activated(
        &mut self,
        shard_id: &DfsMetadataShardId,
        handoff_id: &str,
        activated_epoch: u64,
    ) -> Result<bool> {
        if shard_id != &self.shard_id {
            return Ok(false);
        }
        self.mark_membership_handoff_write_freeze_activated(handoff_id, activated_epoch)
    }

    fn clear_metadata_shard_membership_handoff_freeze(
        &mut self,
        shard_id: &DfsMetadataShardId,
        handoff_id: &str,
        active_shard: &DfsMetadataShardConfig,
        checked_prefix: &DfsMetadataPrefixProof,
    ) -> Result<bool> {
        if shard_id != &self.shard_id || active_shard.shard_id != shard_id.0 {
            return Ok(false);
        }
        self.clear_membership_handoff_write_freeze(handoff_id, active_shard, checked_prefix)
    }

    fn observe_metadata_replication_cursors(
        &self,
        shard_id: &DfsMetadataShardId,
        cursors: &[(NodeId, u64)],
    ) {
        if shard_id == &self.shard_id {
            self.replica_progress.observe_outbound(cursors);
        }
    }

    fn metadata_replication_cursor(
        &self,
        target_node_id: &NodeId,
        shard_id: &DfsMetadataShardId,
    ) -> Result<u64> {
        let sequence =
            metadata_cursor::metadata_replication_cursor(&self.root, target_node_id, shard_id)?
                .max(
                    self.replica_progress
                        .outbound_cursor(target_node_id)
                        .filter(|_| shard_id == &self.shard_id)
                        .unwrap_or(0),
                );
        self.observe_resident_metadata_replication_cursor(target_node_id, shard_id, sequence);
        Ok(sequence)
    }

    fn put_metadata_replication_cursor(
        &mut self,
        target_node_id: &NodeId,
        shard_id: &DfsMetadataShardId,
        next_after_sequence: u64,
    ) -> Result<()> {
        // Replica acknowledgement cursors are independent scheduling hints:
        // they neither observe nor mutate the materialized metadata tables.
        // Serialize their read-max-write update with other shard writers, but
        // do not refresh/replay a potentially large rotated checkpoint merely
        // to advance one monotonic cursor.
        let _lock = self.metadata_write_lock()?;
        metadata_cursor::put_metadata_replication_cursor(
            &self.root,
            target_node_id,
            shard_id,
            next_after_sequence,
        )?;
        self.observe_resident_metadata_replication_cursor(
            target_node_id,
            shard_id,
            next_after_sequence,
        );
        Ok(())
    }

    fn metadata_prefix_ack_floor(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataPrefixAckFloor>> {
        metadata_cursor::metadata_prefix_ack_floor(&self.root, shard_id)
    }

    fn put_metadata_prefix_ack_floor(&mut self, floor: &DfsMetadataPrefixAckFloor) -> Result<()> {
        // Prefix acknowledgements are durable sidecars. Ordinary replica ACKs
        // deliberately persist the exact journal proof before rebuildable
        // tables catch up, so this path must not force materialization.
        let _lock = self.metadata_write_lock()?;
        self.next_sequence =
            self.next_sequence
                .max(crate::metadata_journal::admission::certify_for_write(
                    &self.root,
                    &self.shard_id,
                )?);

        metadata_cursor::put_metadata_prefix_ack_floor(&self.root, floor)
    }

    fn metadata_authority_ack_floor(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataAuthorityAckFloor>> {
        metadata_cursor::metadata_authority_ack_floor(&self.root, shard_id)
    }

    fn put_metadata_authority_ack_floor(
        &mut self,
        floor: &DfsMetadataAuthorityAckFloor,
    ) -> Result<()> {
        // This acknowledgement floor is a monotonic durable sidecar. The
        // caller has already verified the exact prepared operation and prefix;
        // persisting that evidence must not rebuild a rotated materialized
        // table generation a second time.
        let _lock = self.metadata_write_lock()?;
        self.next_sequence =
            self.next_sequence
                .max(crate::metadata_journal::admission::certify_for_write(
                    &self.root,
                    &self.shard_id,
                )?);

        metadata_cursor::put_metadata_authority_ack_floor(&self.root, floor)
    }

    fn metadata_replay_cursor(&self, source_shard_id: &DfsMetadataShardId) -> Result<u64> {
        self.require_replication_source_shard(source_shard_id)?;
        let checkpoint =
            metadata_cursor::read_replay_cursor(&self.root, source_shard_id)?.last_sequence;
        let sequence = self
            .prefix_proof
            .as_ref()
            .filter(|_| &self.shard_id == source_shard_id)
            .map(|proof| proof.journal_sequence.max(checkpoint))
            .unwrap_or(checkpoint);
        self.observe_resident_metadata_replay_cursor(source_shard_id, sequence);
        Ok(sequence)
    }

    fn metadata_shard_statuses(&self) -> Result<Vec<DfsMetadataShardStatus>> {
        let journal_next_sequence = self.next_sequence;
        Ok(vec![DfsMetadataShardStatus {
            shard_id: self.shard_id.clone(),
            file_system_id: None,
            primary_node_id: None,
            replica_node_ids: Vec::new(),
            epoch: None,
            journal_next_sequence,
            journal_last_sequence: journal_next_sequence.saturating_sub(1),
            replica_lag: Vec::new(),
            replay_cursors: self.replay_cursor_statuses()?,
        }])
    }

    fn persist_journal_frames_with_replica_permit_report(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        batch: &DfsMetadataJournalBatch,
        frames: &[DfsMetadataJournalFrameRecord],
    ) -> Result<DfsMetadataReplicaApplyReport> {
        LocalSobjMetadataBackend::persist_journal_frames_with_replica_permit_report(
            self, permit, batch, frames,
        )
    }

    fn persist_authority_journal_frames_with_replica_permit_report(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        batch: &DfsMetadataJournalBatch,
        frames: &[DfsMetadataJournalFrameRecord],
        expected_target_prefix: &DfsMetadataPrefixProof,
        expected_operation: &crate::DfsMetadataAuthorityOperation,
    ) -> Result<DfsMetadataReplicaApplyReport> {
        LocalSobjMetadataBackend::persist_authority_journal_frames_with_replica_permit_report(
            self,
            permit,
            batch,
            frames,
            expected_target_prefix,
            expected_operation,
        )
    }

    fn materialize_pending_replica_journal(&mut self) -> Result<usize> {
        LocalSobjMetadataBackend::materialize_pending_replica_journal(self)
    }

    fn replace_with_replicated_snapshot_with_replica_permit(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        source_shard_id: DfsMetadataShardId,
        replayed_through_sequence: u64,
        snapshot: crate::DfsMetadataSnapshotImage,
    ) -> Result<bool> {
        LocalSobjMetadataBackend::replace_with_replicated_snapshot_with_replica_permit(
            self,
            permit,
            source_shard_id,
            replayed_through_sequence,
            snapshot,
        )
    }

    fn subscribe(&self, after_sequence: u64) -> DfsMetadataSubscription {
        DfsMetadataSubscription::new(self.shard_id.clone(), after_sequence)
    }

    fn poll_subscription(
        &self,
        subscription: &mut DfsMetadataSubscription,
    ) -> Result<DfsMetadataJournalBatch> {
        let entries = self.journal_since(subscription.next_after_sequence)?;
        if let Some(last) = entries.last() {
            subscription.next_after_sequence = last.sequence;
        }
        Ok(DfsMetadataJournalBatch {
            shard_id: self.shard_id.clone(),
            entries,
            next_after_sequence: subscription.next_after_sequence,
        })
    }
}
