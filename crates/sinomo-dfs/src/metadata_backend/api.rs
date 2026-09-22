//! Durable metadata interface implementation; writer admission stays in the parent backend.

use super::*;

impl DfsMetadataBackend for LocalSobjMetadataBackend {
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

    fn put_file_system(&mut self, file_system: &DfsFileSystem) -> Result<DfsMetadataJournalEntry> {
        file_system
            .file_history
            .validate()
            .map_err(MetadataBackendError::Serde)?;
        let _lock = self.begin_metadata_write()?;
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::put_file_system(file_system),
            vec![DfsMetadataTableOp::PutFileSystem {
                file_system: file_system.clone(),
            }],
        )
    }

    fn get_file_system(&self, file_system_id: &DfsFileSystemId) -> Result<Option<DfsFileSystem>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables.file_systems.get(file_system_id).cloned())
    }

    fn list_file_systems(&self) -> Result<Vec<DfsFileSystem>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables.file_systems.values().cloned().collect())
    }

    fn put_object(&mut self, object: &DfsObjectMetadata) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        crate::file_history::validate_file_history_transition(&self.tables, object)?;
        let object = normalized_object_metadata(object);
        if self.get_file_system(&object.file_system_id)?.is_none() {
            return Err(MetadataBackendError::UnknownFileSystem {
                file_system_id: object.file_system_id.clone(),
            });
        }

        let previous = self.get_object(&object.object_id)?;
        self.backfill_file_history_durability_unlocked(
            crate::file_history::legacy_capture_candidates(&self.tables, [&object]),
        )?;
        self.write_journal_entry_and_materialize_unlocked(
            DfsMetadataMutation::put_object(&object),
            |backend, entry| {
                let mut ops =
                    backend.object_index_table_ops_with_previous(&object, previous.as_ref())?;
                crate::file_history::stamp_policy_captures(
                    &backend.tables,
                    &mut ops,
                    entry.created_at,
                );
                Ok(ops)
            },
        )
    }

    fn add_object_location_if_current(
        &mut self,
        object_id: &DfsObjectId,
        expected_version: &DfsVersion,
        expected_content_hash: &str,
        replica_node_id: &NodeId,
    ) -> Result<Option<DfsObjectLocationUpdate>> {
        let _lock = self.begin_metadata_write()?;
        let Some(mut object) = self.get_object(object_id)? else {
            return Ok(None);
        };
        if object.kind == DfsObjectKind::Tombstone
            || &object.version != expected_version
            || object.content_hash.as_deref() != Some(expected_content_hash)
        {
            return Ok(None);
        }

        if !object.current_locations.contains(replica_node_id) {
            object.current_locations.push(replica_node_id.clone());
        }
        object = normalized_object_metadata(&object);

        let existing_version = self.get_version(object_id, expected_version)?;
        let mut version = existing_version
            .clone()
            .unwrap_or_else(|| object_version_record(&object, object.content_size_bytes));
        version.locations = object.current_locations.clone();
        version = normalized_version_record(&version);
        if self.get_object(object_id)?.as_ref() == Some(&object)
            && existing_version.as_ref() == Some(&version)
        {
            return Ok(Some(DfsObjectLocationUpdate {
                object,
                journal_appended: false,
            }));
        }

        let transaction = normalized_namespace_transaction(&DfsNamespaceTransaction {
            file_version_pins: None,
            transaction_id: namespace_transaction_id_for_batch(
                "location-repair",
                &object.file_system_id,
                std::slice::from_ref(&object),
                std::slice::from_ref(&version),
            ),
            file_system_id: object.file_system_id.clone(),
            objects: vec![object.clone()],
            versions: vec![version],
            advisory_lock_states: Vec::new(),
            created_at: OffsetDateTime::now_utc(),
        });
        validate_namespace_transaction_preconditions(&self.tables, &transaction)?;
        let table_ops = namespace_transaction_table_ops(&self.tables, &transaction);
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::ApplyNamespaceTransaction {
                file_system_id: transaction.file_system_id.clone(),
                transaction_id: transaction.transaction_id.clone(),
                object_count: transaction.objects.len(),
                operation_id: None,
                transaction: Some(transaction),
            },
            table_ops,
        )?;
        Ok(Some(DfsObjectLocationUpdate {
            object,
            journal_appended: true,
        }))
    }

    fn remove_object_location_if_current(
        &mut self,
        object_id: &DfsObjectId,
        expected_version: &DfsVersion,
        expected_content_hash: Option<&str>,
        replica_node_id: &NodeId,
    ) -> Result<Option<DfsObjectLocationUpdate>> {
        let _lock = self.begin_metadata_write()?;
        let Some(mut object) = self.get_object(object_id)? else {
            return Ok(None);
        };
        if object.kind == DfsObjectKind::Tombstone
            || &object.version != expected_version
            || object.content_hash.as_deref() != expected_content_hash
        {
            return Ok(None);
        }
        let mut mutations = Vec::new();
        if object.current_locations.contains(replica_node_id) {
            object
                .current_locations
                .retain(|node| node != replica_node_id);
            mutations.push(DfsMetadataMutation::put_object(&object));
        }
        match self.get_version(object_id, expected_version)? {
            Some(mut version)
                if version.content_hash.as_deref() == expected_content_hash
                    && version.locations.contains(replica_node_id) =>
            {
                version.locations.retain(|node| node != replica_node_id);
                mutations.push(DfsMetadataMutation::put_version(&version));
            }
            None => mutations.push(DfsMetadataMutation::put_version(&object_version_record(
                &object,
                object.content_size_bytes,
            ))),
            _ => {}
        }
        if let Some(receipt) = self.tables.receipts.get(&(
            object_id.clone(),
            expected_version.clone(),
            replica_node_id.clone(),
        )) && receipt.content_hash.as_deref() == expected_content_hash
        {
            mutations.push(DfsMetadataMutation::RemoveReceipt {
                file_system_id: object.file_system_id.clone(),
                object_id: object_id.clone(),
                version: expected_version.clone(),
                replica_node_id: replica_node_id.clone(),
            });
        }
        let journal_appended = !mutations.is_empty();
        if journal_appended {
            let entries = self.write_journal_entries_unlocked(mutations)?;
            let planned = self.plan_written_journal_entries_table_ops(&entries)?;
            self.append_preapplied_table_ops(
                planned.last_materialized_sequence,
                planned.table_ops,
                planned.rollback,
            )?;
            if let Some(sequence) = entries.last().map(|entry| entry.sequence) {
                self.checkpoint_local_materialization_cursor_if_due(sequence)?;
            }
        }
        Ok(Some(DfsObjectLocationUpdate {
            object,
            journal_appended,
        }))
    }

    fn put_objects(
        &mut self,
        objects: &[DfsObjectMetadata],
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        if objects.is_empty() {
            return Ok(Vec::new());
        }
        let _lock = self.begin_metadata_write()?;
        let objects = objects
            .iter()
            .map(normalized_object_metadata)
            .collect::<Vec<_>>();
        for object in &objects {
            crate::file_history::validate_file_history_transition(&self.tables, object)?;
            if self.get_file_system(&object.file_system_id)?.is_none() {
                return Err(MetadataBackendError::UnknownFileSystem {
                    file_system_id: object.file_system_id.clone(),
                });
            }
        }
        self.backfill_file_history_durability_unlocked(
            crate::file_history::legacy_capture_candidates(&self.tables, &objects),
        )?;
        let entries = self.write_journal_entries_unlocked(
            objects
                .iter()
                .map(DfsMetadataMutation::put_object)
                .collect(),
        )?;
        let mut table_ops = Vec::new();
        let mut rollback = DfsMetadataTableRollback::default();
        for (object, entry) in objects.iter().zip(&entries) {
            let previous = self.get_object(&object.object_id)?;
            if let Err(error) = self.plan_object_index_table_ops_with_previous(
                object,
                previous.as_ref(),
                entry.created_at,
                &mut table_ops,
                &mut rollback,
            ) {
                rollback.rollback(&mut self.tables);
                return Err(error);
            }
        }
        let last_sequence = entries
            .last()
            .map(|entry| entry.sequence)
            .unwrap_or_else(|| self.next_sequence.saturating_sub(1));
        self.append_preapplied_table_ops_for_written_entries(
            &entries,
            last_sequence,
            table_ops,
            rollback,
        )?;
        Ok(entries)
    }

    fn get_object(&self, object_id: &DfsObjectId) -> Result<Option<DfsObjectMetadata>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables.objects.get(object_id).cloned())
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

    fn apply_advisory_lock_command(
        &mut self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        context: DfsAdvisoryLockTransitionContext,
        command: &DfsAdvisoryLockCommand,
    ) -> Result<DfsAdvisoryLockApplyResult> {
        let _lock = self.begin_metadata_write()?;
        let object = self.tables.objects.get(object_id).ok_or_else(|| {
            MetadataBackendError::UnknownObject {
                object_id: object_id.clone(),
            }
        })?;
        if object.file_system_id != *file_system_id {
            return Err(MetadataBackendError::AdvisoryLock(format!(
                "object {object_id} belongs to filesystem {}, not {file_system_id}",
                object.file_system_id
            )));
        }
        let transition = transition_advisory_lock_state(
            self.tables.advisory_lock_states.get(object_id),
            file_system_id,
            object_id,
            context,
            command,
        )
        .map_err(|error| MetadataBackendError::AdvisoryLock(error.to_string()))?;
        let journal_entry = if transition.changed {
            Some(self.write_journal_entry_and_apply_table_ops_unlocked(
                DfsMetadataMutation::put_advisory_lock_state(&transition.state),
                vec![DfsMetadataTableOp::PutAdvisoryLockState {
                    state: transition.state,
                }],
            )?)
        } else {
            None
        };
        let durable_through_sequence = journal_entry
            .as_ref()
            .map(|entry| entry.sequence)
            .unwrap_or_else(|| self.journal_last_sequence());
        Ok(DfsAdvisoryLockApplyResult {
            outcome: transition.outcome,
            shard_id: self.shard_id.clone(),
            durable_through_sequence,
            journal_entry,
        })
    }

    fn install_advisory_lock_state_for_migration(
        &mut self,
        state: &DfsAdvisoryLockStateRecord,
    ) -> Result<Option<DfsMetadataJournalEntry>> {
        LocalSobjMetadataBackend::install_advisory_lock_state_for_migration(self, state)
    }

    fn get_object_by_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        let tables = self.current_tables_snapshot()?;
        Ok(object_by_path_from_tables(&tables, file_system_id, path))
    }

    fn namespace_prepare_expectations(
        &self,
        _shard_id: &DfsMetadataShardId,
        transaction: &DfsNamespaceTransaction,
    ) -> Result<(
        Vec<DfsNamespaceObjectVersionExpectation>,
        Vec<DfsNamespacePathOccupantExpectation>,
        BTreeSet<String>,
    )> {
        let tables = self.current_tables_snapshot()?;
        namespace_prepare_expectations_from_lookups(
            transaction,
            |object_id| Ok(tables.objects.get(object_id).cloned()),
            |file_system_id, path| Ok(object_by_path_from_tables(&tables, file_system_id, path)),
        )
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

    fn put_version(&mut self, version: &DfsObjectVersionRecord) -> Result<DfsMetadataJournalEntry> {
        crate::file_history::validate_file_version_record(version)?;
        let _lock = self.begin_metadata_write()?;
        crate::file_history::validate_retained_version_update(
            self.tables
                .versions
                .get(&(version.object_id.clone(), version.version.clone())),
            version,
        )?;
        let version = normalized_version_record(version);
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::put_version(&version),
            vec![DfsMetadataTableOp::PutVersion { version }],
        )
    }

    fn capture_file_version(
        &mut self,
        record: &DfsObjectVersionRecord,
        pin: &DfsMetadataVersionPinRecord,
        expected_current_version: Option<&DfsVersion>,
    ) -> Result<Option<DfsMetadataJournalEntry>> {
        let _lock = self.begin_metadata_write()?;
        let record = normalized_version_record(record);
        let ops =
            super::pin_validation::file_version_capture_table_ops(&self.tables, &record, pin)?;
        if ops.is_empty() {
            return Ok(None);
        }
        if let Some(expected) = expected_current_version {
            let current = self.get_object(&record.object_id)?.ok_or_else(|| {
                MetadataBackendError::UnknownObject {
                    object_id: record.object_id.clone(),
                }
            })?;
            if &current.version != expected || current.kind == DfsObjectKind::Tombstone {
                return Err(MetadataBackendError::Conflict {
                    object_id: record.object_id.clone(),
                    expected: expected.clone(),
                    current: current.version,
                });
            }
        }
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::CaptureFileVersion {
                file_system_id: record.file_system_id.clone(),
                object_id: record.object_id.clone(),
                version: record.version.clone(),
                record: Box::new(record),
                pin: pin.clone(),
            },
            ops,
        )
        .map(Some)
    }

    fn put_version_pin(
        &mut self,
        pin: &DfsMetadataVersionPinRecord,
    ) -> Result<Option<DfsMetadataJournalEntry>> {
        validate_version_pin_payload(
            &pin.file_system_id,
            &pin.object_id,
            &pin.version,
            &pin.pin_id,
            pin,
        )?;
        let _lock = self.begin_metadata_write()?;
        let key = (
            pin.object_id.clone(),
            pin.version.clone(),
            pin.pin_id.clone(),
        );
        if let Some(owner) =
            self.tables
                .cross_shard_pin_transfer_fence(&pin.object_id, &pin.version, &pin.pin_id)
        {
            return Err(MetadataBackendError::Serde(format!(
                "metadata version pin {} for {}/{} is fenced by active cross-shard transfer {}",
                pin.pin_id, pin.object_id, pin.version, owner.transfer_id
            )));
        }
        let version = self
            .tables
            .versions
            .get(&(pin.object_id.clone(), pin.version.clone()))
            .ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "cannot pin missing metadata version {}/{}",
                    pin.object_id, pin.version
                ))
            })?;
        if version.file_system_id != pin.file_system_id {
            return Err(MetadataBackendError::Serde(format!(
                "metadata version pin {} filesystem {} does not match version filesystem {}",
                pin.pin_id, pin.file_system_id, version.file_system_id
            )));
        }
        if let Some(existing) = self.tables.version_pins.get(&key) {
            if existing == pin {
                return Ok(None);
            }
            return Err(MetadataBackendError::Serde(format!(
                "metadata version pin {} for {}/{} was reused with a different payload",
                pin.pin_id, pin.object_id, pin.version
            )));
        }
        require_version_pin_capacity_for_new(&self.tables, &pin.object_id)?;
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::put_version_pin(pin),
            vec![DfsMetadataTableOp::PutVersionPin { pin: pin.clone() }],
        )
        .map(Some)
    }

    fn remove_version_pin(
        &mut self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        pin_id: &str,
    ) -> Result<Option<DfsMetadataJournalEntry>> {
        let _lock = self.begin_metadata_write()?;
        let key = (object_id.clone(), version.clone(), pin_id.to_string());
        if let Some(owner) = self
            .tables
            .cross_shard_pin_transfer_fence(object_id, version, pin_id)
        {
            return Err(MetadataBackendError::Serde(format!(
                "metadata version pin {pin_id} for {object_id}/{version} is fenced by active cross-shard transfer {}",
                owner.transfer_id
            )));
        }
        let Some(existing) = self.tables.version_pins.get(&key) else {
            return Ok(None);
        };
        if &existing.file_system_id != file_system_id {
            return Err(MetadataBackendError::Serde(format!(
                "metadata version pin {pin_id} filesystem {} does not match removal filesystem {file_system_id}",
                existing.file_system_id
            )));
        }
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::remove_version_pin(
                file_system_id.clone(),
                object_id.clone(),
                version.clone(),
                pin_id.to_string(),
            ),
            vec![DfsMetadataTableOp::RemoveVersionPin {
                object_id: object_id.clone(),
                version: version.clone(),
                pin_id: pin_id.to_string(),
            }],
        )
        .map(Some)
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

    fn install_cross_shard_version_pin_family(
        &mut self,
        object: &DfsObjectMetadata,
        versions: &[DfsObjectVersionRecord],
        pins: &[DfsMetadataVersionPinRecord],
    ) -> Result<()> {
        let target_is_already_authoritative =
            self.get_object(&object.object_id)?.is_some_and(|existing| {
                existing.kind != crate::DfsObjectKind::Tombstone
                    && existing.file_system_id == object.file_system_id
                    && normalize_path(&existing.path) == normalize_path(&object.path)
            });
        for version in versions {
            if version.object_id != object.object_id
                || version.file_system_id != object.file_system_id
            {
                return Err(MetadataBackendError::Serde(format!(
                    "cross-shard pin-family install for object {} received unrelated version {}/{}",
                    object.object_id, version.object_id, version.version
                )));
            }
            match self.get_version(&version.object_id, &version.version)? {
                Some(existing) if existing != *version => {
                    return Err(MetadataBackendError::Serde(format!(
                        "destination has conflicting version {} for pinned object {}",
                        version.version, object.object_id
                    )));
                }
                Some(_) => {}
                None => {
                    self.put_version(version)?;
                }
            }
        }
        let desired = pins
            .iter()
            .map(|pin| ((pin.version.clone(), pin.pin_id.clone()), pin.clone()))
            .collect::<BTreeMap<_, _>>();
        let existing = self.version_pins_for_object(&object.object_id)?;
        if !target_is_already_authoritative {
            for pin in &existing {
                if desired.get(&(pin.version.clone(), pin.pin_id.clone())) == Some(pin) {
                    continue;
                }
                self.remove_version_pin(
                    &pin.file_system_id,
                    &pin.object_id,
                    &pin.version,
                    &pin.pin_id,
                )?;
            }
        }
        for pin in desired.values() {
            self.put_version_pin(pin)?;
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
        let source_path = normalize_path(source_path);
        let target_path = normalize_path(&object.path);
        if transfer_id.trim().is_empty()
            || transfer_id.len() > 256
            || source_path == target_path
            || source_pins.len() > MAX_DURABLE_VERSION_PINS_PER_OBJECT
        {
            return Err(MetadataBackendError::Serde(format!(
                "invalid bounded cross-shard pin transfer {} for {} -> {}",
                transfer_id, source_path, target_path
            )));
        }
        let versions = versions
            .iter()
            .map(normalized_version_record)
            .collect::<Vec<_>>();
        for version in &versions {
            if version.object_id != object.object_id
                || version.file_system_id != object.file_system_id
            {
                return Err(MetadataBackendError::Serde(format!(
                    "cross-shard pin transfer {} received unrelated version {}/{}",
                    transfer_id, version.object_id, version.version
                )));
            }
        }

        let _lock = self.begin_metadata_write()?;
        let transfer_key = (object.object_id.clone(), transfer_id.to_string());
        let previous = self
            .tables
            .cross_shard_pin_transfers
            .get(&transfer_key)
            .cloned();
        if source_pins.is_empty() && previous.is_none() {
            return Ok(false);
        }
        if self
            .tables
            .cross_shard_pin_transfers
            .values()
            .any(|other| other.object_id == object.object_id && other.transfer_id != transfer_id)
        {
            return Err(MetadataBackendError::Serde(format!(
                "object {} already has another active cross-shard pin transfer",
                object.object_id
            )));
        }
        if let Some(previous) = previous.as_ref()
            && (previous.file_system_id != object.file_system_id
                || previous.object_id != object.object_id
                || normalize_path(&previous.source_path) != source_path
                || normalize_path(&previous.target_path) != target_path
                || previous.target_version != object.version)
        {
            return Err(MetadataBackendError::Serde(format!(
                "cross-shard pin transfer {} was reused with a different handoff identity",
                transfer_id
            )));
        }

        let previous_owned = previous
            .as_ref()
            .map(|record| record.copied_pins.iter().collect::<BTreeSet<_>>())
            .unwrap_or_default();
        let mut source_keys = BTreeSet::new();
        let mut owned_pins = Vec::new();
        for pin in source_pins {
            validate_version_pin_payload(
                &pin.file_system_id,
                &pin.object_id,
                &pin.version,
                &pin.pin_id,
                pin,
            )?;
            if pin.file_system_id != object.file_system_id || pin.object_id != object.object_id {
                return Err(MetadataBackendError::Serde(format!(
                    "cross-shard pin transfer {} received unrelated source pin {} for {}/{}",
                    transfer_id, pin.pin_id, pin.object_id, pin.version
                )));
            }
            let key = (
                pin.object_id.clone(),
                pin.version.clone(),
                pin.pin_id.clone(),
            );
            if !source_keys.insert(key.clone()) {
                return Err(MetadataBackendError::Serde(format!(
                    "cross-shard pin transfer {} repeats source pin {} for {}/{}",
                    transfer_id, pin.pin_id, pin.object_id, pin.version
                )));
            }
            let pinned_version = self
                .tables
                .versions
                .get(&(pin.object_id.clone(), pin.version.clone()))
                .or_else(|| {
                    versions
                        .iter()
                        .find(|version| version.version == pin.version)
                })
                .ok_or_else(|| {
                    MetadataBackendError::Serde(format!(
                        "cross-shard pin transfer {} is missing destination version {}/{}",
                        transfer_id, pin.object_id, pin.version
                    ))
                })?;
            if pinned_version.file_system_id != pin.file_system_id {
                return Err(MetadataBackendError::Serde(format!(
                    "cross-shard pin transfer {} source pin filesystem mismatch",
                    transfer_id
                )));
            }
            if let Some(owner) = self.tables.cross_shard_pin_transfer_owner(
                &pin.object_id,
                &pin.version,
                &pin.pin_id,
            ) && owner.transfer_id != transfer_id
            {
                return Err(MetadataBackendError::Serde(format!(
                    "cross-shard pin {} for {}/{} is already owned by transfer {}",
                    pin.pin_id, pin.object_id, pin.version, owner.transfer_id
                )));
            }
            match self.tables.version_pins.get(&key) {
                Some(existing) if existing != pin => {
                    return Err(MetadataBackendError::Serde(format!(
                        "cross-shard pin transfer {} conflicts with target pin {} for {}/{}",
                        transfer_id, pin.pin_id, pin.object_id, pin.version
                    )));
                }
                Some(_) if previous_owned.contains(pin) => owned_pins.push(pin.clone()),
                Some(_) => {
                    // An exact pre-existing target pin is independent state.
                    // It satisfies the source consumer without becoming
                    // transfer-owned or eligible for stale-copy removal.
                }
                None => owned_pins.push(pin.clone()),
            }
        }
        owned_pins.sort();
        let mut acknowledged_pins = source_pins.to_vec();
        acknowledged_pins.sort();
        let now = time::OffsetDateTime::now_utc();
        let unchanged = previous.as_ref().is_some_and(|previous| {
            previous.copied_pins == owned_pins && previous.source_pins == acknowledged_pins
        });
        let transfer = DfsCrossShardPinTransferRecord {
            transfer_id: transfer_id.to_string(),
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            source_path,
            target_path,
            target_version: object.version.clone(),
            source_pins: acknowledged_pins,
            copied_pins: owned_pins,
            created_at: previous
                .as_ref()
                .map(|previous| previous.created_at)
                .unwrap_or(now),
            updated_at: if unchanged {
                previous
                    .as_ref()
                    .map(|previous| previous.updated_at)
                    .unwrap_or(now)
            } else {
                now
            },
        };
        if validate_cross_shard_pin_transfer_table_transition(
            &self.tables,
            &object.file_system_id,
            &object.object_id,
            transfer_id,
            false,
            &versions,
            &transfer,
        )? {
            return Ok(true);
        }
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::reconcile_cross_shard_pin_transfer(
                &transfer,
                versions.clone(),
                false,
            ),
            vec![DfsMetadataTableOp::ReconcileCrossShardPinTransfer { versions, transfer }],
        )?;
        Ok(true)
    }

    fn finalize_cross_shard_pin_transfer(
        &mut self,
        transfer_id: &str,
        object: &DfsObjectMetadata,
        source_path: &str,
    ) -> Result<()> {
        let _lock = self.begin_metadata_write()?;
        let key = (object.object_id.clone(), transfer_id.to_string());
        let Some(transfer) = self.tables.cross_shard_pin_transfers.get(&key) else {
            return Ok(());
        };
        if transfer.file_system_id != object.file_system_id
            || normalize_path(&transfer.source_path) != normalize_path(source_path)
            || normalize_path(&transfer.target_path) != normalize_path(&object.path)
            || transfer.target_version != object.version
        {
            return Err(MetadataBackendError::Serde(format!(
                "cross-shard pin transfer {} finalization identity mismatch",
                transfer_id
            )));
        }
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::remove_cross_shard_pin_transfer(
                object.file_system_id.clone(),
                object.object_id.clone(),
                transfer_id.to_string(),
            ),
            vec![DfsMetadataTableOp::RemoveCrossShardPinTransfer {
                object_id: object.object_id.clone(),
                transfer_id: transfer_id.to_string(),
            }],
        )?;
        Ok(())
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

    fn install_cross_shard_pin_transfer_for_authoritative_migration(
        &mut self,
        transfer: &DfsCrossShardPinTransferRecord,
        versions: &[DfsObjectVersionRecord],
    ) -> Result<()> {
        let versions = versions
            .iter()
            .map(normalized_version_record)
            .collect::<Vec<_>>();
        let _lock = self.begin_metadata_write()?;
        if validate_cross_shard_pin_transfer_table_transition(
            &self.tables,
            &transfer.file_system_id,
            &transfer.object_id,
            &transfer.transfer_id,
            true,
            &versions,
            transfer,
        )? {
            return Ok(());
        }
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::reconcile_cross_shard_pin_transfer(
                transfer,
                versions.clone(),
                true,
            ),
            vec![DfsMetadataTableOp::ReconcileCrossShardPinTransfer {
                versions,
                transfer: transfer.clone(),
            }],
        )?;
        Ok(())
    }

    fn remove_cross_shard_pin_transfer_for_authoritative_migration(
        &mut self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        transfer_id: &str,
    ) -> Result<()> {
        let _lock = self.begin_metadata_write()?;
        let Some(existing) = self
            .tables
            .cross_shard_pin_transfers
            .get(&(object_id.clone(), transfer_id.to_string()))
        else {
            return Ok(());
        };
        if &existing.file_system_id != file_system_id {
            return Err(MetadataBackendError::Serde(format!(
                "cross-shard pin transfer {} filesystem {} does not match authoritative migration filesystem {}",
                transfer_id, existing.file_system_id, file_system_id
            )));
        }
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::remove_cross_shard_pin_transfer(
                file_system_id.clone(),
                object_id.clone(),
                transfer_id.to_string(),
            ),
            vec![DfsMetadataTableOp::RemoveCrossShardPinTransfer {
                object_id: object_id.clone(),
                transfer_id: transfer_id.to_string(),
            }],
        )?;
        Ok(())
    }

    fn capture_cross_shard_source_pin_fence(
        &mut self,
        object_id: &DfsObjectId,
    ) -> Result<DfsCrossShardVersionPinFence> {
        LocalSobjMetadataBackend::capture_cross_shard_source_pin_fence(self, object_id)
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

    fn evict_object_record_for_foreign_shard_move_with_pin_fence(
        &mut self,
        moved_object: &DfsObjectMetadata,
        source_path: &str,
        expected_source_object: &DfsObjectMetadata,
        expected_source_journal_sequence: u64,
        expected_version_pins: &[DfsMetadataVersionPinRecord],
        pin_transfer_id: Option<&str>,
    ) -> Result<()> {
        let _ = (
            source_path,
            expected_source_object,
            expected_source_journal_sequence,
            pin_transfer_id,
        );
        let current = self.get_object(&moved_object.object_id)?.ok_or_else(|| {
            MetadataBackendError::UnknownObject {
                object_id: moved_object.object_id.clone(),
            }
        })?;
        if current.file_system_id != moved_object.file_system_id
            || normalize_path(&current.path) != normalize_path(&moved_object.path)
        {
            return Err(MetadataBackendError::Serde(format!(
                "single-shard pinned adoption of object {} is not visible at {}",
                moved_object.object_id, moved_object.path
            )));
        }
        let current_pins = self.version_pins_for_object(&moved_object.object_id)?;
        for expected in expected_version_pins {
            if !current_pins.iter().any(|pin| pin == expected) {
                return Err(MetadataBackendError::Serde(format!(
                    "single-shard pinned adoption lost source pin {} for {}/{}",
                    expected.pin_id, expected.object_id, expected.version
                )));
            }
        }
        Ok(())
    }

    fn put_object_and_version(
        &mut self,
        object: &DfsObjectMetadata,
        version: &DfsObjectVersionRecord,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let _lock = self.begin_metadata_write()?;
        crate::file_history::validate_file_history_transition(&self.tables, object)?;
        let object = normalized_object_metadata(object);
        let version = normalized_version_record(version);
        crate::file_history::validate_retained_version_update(
            self.tables
                .versions
                .get(&(version.object_id.clone(), version.version.clone())),
            &version,
        )?;
        let metadata_matches = object.file_system_id == version.file_system_id
            && object.object_id == version.object_id;
        if !metadata_matches {
            return Err(MetadataBackendError::Serde(format!(
                "object {} {} does not match version {} {}",
                object.file_system_id, object.object_id, version.file_system_id, version.object_id
            )));
        }
        if self.get_file_system(&object.file_system_id)?.is_none() {
            return Err(MetadataBackendError::UnknownFileSystem {
                file_system_id: object.file_system_id.clone(),
            });
        }
        let previous = self.get_object(&object.object_id)?;
        self.backfill_file_history_durability_unlocked(
            crate::file_history::legacy_capture_candidates(&self.tables, [&object]),
        )?;
        let entries = self.write_journal_entries_unlocked(vec![
            DfsMetadataMutation::put_object(&object),
            DfsMetadataMutation::put_version(&version),
        ])?;
        let mut table_ops = Vec::new();
        let mut rollback = DfsMetadataTableRollback::default();
        if let Err(error) = self.plan_object_index_table_ops_with_previous(
            &object,
            previous.as_ref(),
            entries[0].created_at,
            &mut table_ops,
            &mut rollback,
        ) {
            rollback.rollback(&mut self.tables);
            return Err(error);
        }
        self.plan_preapplied_table_op(
            DfsMetadataTableOp::PutVersion { version },
            &mut table_ops,
            &mut rollback,
        );
        let last_sequence = entries
            .last()
            .map(|entry| entry.sequence)
            .unwrap_or_else(|| self.next_sequence.saturating_sub(1));
        self.append_preapplied_table_ops_for_written_entries(
            &entries,
            last_sequence,
            table_ops,
            rollback,
        )?;
        Ok(entries)
    }

    fn put_object_version_intent_and_receipts(
        &mut self,
        object: &DfsObjectMetadata,
        version: &DfsObjectVersionRecord,
        intent: &DfsWriteIntentRecord,
        receipts: &[DfsReplicaReceiptRecord],
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let trace_started = Instant::now();
        let trace_object_id = object.object_id.clone();
        let trace_version = version.version.clone();
        let lock_started = Instant::now();
        let _lock = self.begin_metadata_write()?;
        self.preserved_terminal_intent(intent)?;
        let lock_ms = lock_started.elapsed().as_millis();
        let validate_started = Instant::now();
        let object = normalized_object_metadata(object);
        crate::file_history::validate_file_history_transition(&self.tables, &object)?;
        let version = normalized_version_record(version);
        crate::file_history::validate_retained_version_update(
            self.tables
                .versions
                .get(&(version.object_id.clone(), version.version.clone())),
            &version,
        )?;
        let expected_version = intent.base_version.as_ref().ok_or_else(|| {
            MetadataBackendError::Serde(format!(
                "combined content commit for object {} requires an existing base version",
                object.object_id
            ))
        })?;
        if object.file_system_id != version.file_system_id
            || object.object_id != version.object_id
            || object.file_system_id != intent.file_system_id
            || object.object_id != intent.object_id
            || object.version != version.version
            || object.version != intent.target_version
            || intent.status != DfsWriteIntentStatus::Committed
            || !intent.publication.is_current()
            || intent.touch_parent_on_create
        {
            return Err(MetadataBackendError::Serde(format!(
                "combined object/version/intent metadata mismatch for object {}",
                object.object_id
            )));
        }
        let current = self.get_object(&object.object_id)?.ok_or_else(|| {
            MetadataBackendError::UnknownObject {
                object_id: object.object_id.clone(),
            }
        })?;
        if &current.version != expected_version {
            return Err(MetadataBackendError::Conflict {
                object_id: object.object_id.clone(),
                expected: expected_version.clone(),
                current: current.version,
            });
        }
        let Some(expected_content_hash) = intent.content_hash.as_deref() else {
            return Err(MetadataBackendError::Serde(format!(
                "combined content commit for object {} is missing its content hash",
                object.object_id
            )));
        };
        if object.content_hash.as_deref() != Some(expected_content_hash)
            || version.content_hash.as_deref() != Some(expected_content_hash)
            || !version.visible
        {
            return Err(MetadataBackendError::Serde(format!(
                "combined content commit hash/version mismatch for object {}",
                object.object_id
            )));
        }
        let mut required_replica_node_ids = BTreeSet::new();
        for receipt in receipts {
            if receipt.receipt.file_system_id != object.file_system_id
                || receipt.receipt.object_id != object.object_id
                || receipt.receipt.committed_version != version.version
                || receipt.content_hash.as_deref() != Some(expected_content_hash)
                || !intent
                    .desired_replica_node_ids
                    .contains(&receipt.replica_node_id)
            {
                return Err(MetadataBackendError::Serde(format!(
                    "combined receipt metadata mismatch for object {}",
                    object.object_id
                )));
            }
            let receipt_key = (
                object.object_id.clone(),
                version.version.clone(),
                receipt.replica_node_id.clone(),
            );
            if self
                .tables
                .receipts
                .get(&receipt_key)
                .is_some_and(|existing| existing.content_hash != receipt.content_hash)
            {
                return Err(MetadataBackendError::Serde(format!(
                    "combined content commit conflicts with the durable receipt for object {} replica {}",
                    object.object_id, receipt.replica_node_id
                )));
            }
            required_replica_node_ids.insert(receipt.replica_node_id.clone());
        }
        if required_replica_node_ids.len() < intent.policy.min_commit_replicas {
            return Err(MetadataBackendError::Serde(format!(
                "combined content commit for object {} has {} durable receipts; {} required",
                object.object_id,
                required_replica_node_ids.len(),
                intent.policy.min_commit_replicas
            )));
        }
        if self.get_file_system(&object.file_system_id)?.is_none() {
            return Err(MetadataBackendError::UnknownFileSystem {
                file_system_id: object.file_system_id.clone(),
            });
        }
        let validate_ms = validate_started.elapsed().as_millis();
        self.backfill_file_history_durability_unlocked(
            crate::file_history::legacy_capture_candidates(&self.tables, [&object]),
        )?;
        let build_started = Instant::now();
        let required_replica_node_ids = required_replica_node_ids.into_iter().collect::<Vec<_>>();
        let mut pending_intent = intent.clone();
        pending_intent.status = DfsWriteIntentStatus::ReceiptsPending;
        pending_intent.object_metadata = Some(current);
        pending_intent.updated_at = Some(OffsetDateTime::now_utc());
        let visible_commit = visible_commit_record_from_parts(
            expected_version,
            &object,
            &version,
            &required_replica_node_ids,
            OffsetDateTime::now_utc(),
        );
        let mut mutations = Vec::with_capacity(3 + receipts.len());
        // Replay prefixes before CommitVisibleVersion retain the old visible
        // version. A prefix ending at the commit already contains the matching
        // pending intent and W receipts; recovery can terminalize that intent.
        mutations.push(DfsMetadataMutation::put_intent(&pending_intent));
        mutations.extend(receipts.iter().map(DfsMetadataMutation::put_receipt));
        mutations.push(DfsMetadataMutation::commit_visible_version(&visible_commit));
        mutations.push(DfsMetadataMutation::put_intent(intent));
        let build_ms = build_started.elapsed().as_millis();
        let journal_started = Instant::now();
        let entries = self.write_journal_entries_unlocked(mutations)?;
        let journal_ms = journal_started.elapsed().as_millis();
        let materialize_started = Instant::now();
        let planned = self.plan_written_journal_entries_table_ops(&entries)?;
        let materialize_ms = materialize_started.elapsed().as_millis();
        let table_append_started = Instant::now();
        self.append_preapplied_table_ops(
            planned.last_materialized_sequence,
            planned.table_ops,
            planned.rollback,
        )?;
        let table_append_ms = table_append_started.elapsed().as_millis();
        let checkpoint_started = Instant::now();
        if let Some(last_sequence) = entries.last().map(|entry| entry.sequence) {
            self.checkpoint_local_materialization_cursor_if_due(last_sequence)?;
        }
        let checkpoint_ms = checkpoint_started.elapsed().as_millis();
        metadata_backend_perf_trace(format_args!(
            "metadata-commit-object-version-intent-receipts shard={} object={} version={} receipts={} entries={} lock_ms={} validate_ms={} build_ms={} journal_ms={} materialize_ms={} table_append_ms={} checkpoint_ms={} total_ms={}",
            self.shard_id.0,
            trace_object_id,
            trace_version,
            receipts.len(),
            entries.len(),
            lock_ms,
            validate_ms,
            build_ms,
            journal_ms,
            materialize_ms,
            table_append_ms,
            checkpoint_ms,
            trace_started.elapsed().as_millis()
        ));
        Ok(entries)
    }

    fn commit_create_intent_receipts_and_namespace(
        &mut self,
        pending_intent: &DfsWriteIntentRecord,
        receipts: &[DfsReplicaReceiptRecord],
        transaction: &DfsNamespaceTransaction,
        committed_intent: &DfsWriteIntentRecord,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let trace_started = Instant::now();
        let _lock = self.begin_metadata_write()?;
        self.preserved_terminal_intent(committed_intent)?;
        let transaction = normalized_namespace_transaction(transaction);
        if self.get_file_system(&transaction.file_system_id)?.is_none() {
            return Err(MetadataBackendError::UnknownFileSystem {
                file_system_id: transaction.file_system_id.clone(),
            });
        }
        if pending_intent.status != DfsWriteIntentStatus::ReceiptsPending
            || committed_intent.status != DfsWriteIntentStatus::Committed
            || !pending_intent.touch_parent_on_create
            || !committed_intent.touch_parent_on_create
            || pending_intent.intent_id != committed_intent.intent_id
            || pending_intent.file_system_id != committed_intent.file_system_id
            || pending_intent.object_id != committed_intent.object_id
            || pending_intent.base_version != committed_intent.base_version
            || pending_intent.target_version != committed_intent.target_version
            || pending_intent.owner_epoch != committed_intent.owner_epoch
            || pending_intent.desired_replica_node_ids != committed_intent.desired_replica_node_ids
            || pending_intent.policy != committed_intent.policy
            || pending_intent.content_hash != committed_intent.content_hash
            || pending_intent.create_predecessor_object_id
                != committed_intent.create_predecessor_object_id
            || pending_intent.file_system_id != transaction.file_system_id
        {
            return Err(MetadataBackendError::Serde(format!(
                "combined create intent transition mismatch for object {}",
                pending_intent.object_id
            )));
        }
        let child = transaction
            .objects
            .iter()
            .find(|object| object.object_id == pending_intent.object_id)
            .ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "combined create transaction {} is missing child object {}",
                    transaction.transaction_id, pending_intent.object_id
                ))
            })?;
        let child_version = transaction
            .versions
            .iter()
            .find(|version| {
                version.object_id == pending_intent.object_id
                    && version.version == pending_intent.target_version
            })
            .ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "combined create transaction {} is missing child version {}",
                    transaction.transaction_id, pending_intent.target_version
                ))
            })?;
        if self.get_object(&child.object_id)?.is_some() {
            return Err(MetadataBackendError::AlreadyExists {
                path: child.path.clone(),
            });
        }
        let Some(expected_content_hash) = pending_intent.content_hash.as_deref() else {
            return Err(MetadataBackendError::Serde(format!(
                "combined create for object {} is missing its content hash",
                pending_intent.object_id
            )));
        };
        if child.version != pending_intent.target_version
            || child.file_system_id != pending_intent.file_system_id
            || child.content_hash.as_deref() != Some(expected_content_hash)
            || child_version.file_system_id != child.file_system_id
            || child_version.content_hash.as_deref() != Some(expected_content_hash)
            || child_version.size_bytes != child.content_size_bytes
            || child_version.locations != child.current_locations
            || child_version.created_by_event_id != child.last_modified_by_event_id
            || !child_version.visible
        {
            return Err(MetadataBackendError::Serde(format!(
                "combined create child/version mismatch for object {}",
                pending_intent.object_id
            )));
        }
        let normalized_child_path = normalize_path(&child.path);
        let child_path_occupant = object_by_path_from_tables(
            &self.tables,
            &pending_intent.file_system_id,
            &normalized_child_path,
        );
        if child_path_occupant.as_ref().is_some_and(|occupant| {
            occupant.kind != DfsObjectKind::Tombstone
                || occupant.object_id == child.object_id
                || pending_intent.create_predecessor_object_id.as_ref() != Some(&occupant.object_id)
        }) {
            return Err(MetadataBackendError::AlreadyExists {
                path: child.path.clone(),
            });
        }
        let parent_path = normalized_child_path
            .rsplit_once('/')
            .map(|(parent, _)| parent)
            .unwrap_or("");
        let durable_parent =
            object_by_path_from_tables(&self.tables, &pending_intent.file_system_id, parent_path)
                .ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "combined create parent {} is not durable",
                    parent_path
                ))
            })?;
        let touched_parent = transaction
            .objects
            .iter()
            .find(|object| normalize_path(&object.path) == parent_path)
            .ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "combined create transaction {} is missing parent {}",
                    transaction.transaction_id, parent_path
                ))
            })?;
        let mut parent_transition_base = touched_parent.clone();
        parent_transition_base.version = durable_parent.version.clone();
        parent_transition_base.modified_at_unix_nanos = durable_parent.modified_at_unix_nanos;
        parent_transition_base.changed_at_unix_nanos = durable_parent.changed_at_unix_nanos;
        parent_transition_base.last_modified_by_event_id =
            durable_parent.last_modified_by_event_id.clone();
        let touched_parent_version = transaction.versions.iter().find(|version| {
            version.object_id == touched_parent.object_id
                && version.version == touched_parent.version
                && version.visible
        });
        if durable_parent.kind != DfsObjectKind::Directory
            || touched_parent.kind != DfsObjectKind::Directory
            || touched_parent.object_id != durable_parent.object_id
            || touched_parent.last_modified_by_event_id.is_none()
            || parent_transition_base != durable_parent
            || touched_parent_version.is_none_or(|version| {
                version.file_system_id != touched_parent.file_system_id
                    || version.content_hash != touched_parent.content_hash
                    || version.size_bytes != touched_parent.content_size_bytes
                    || version.locations != touched_parent.current_locations
                    || version.created_by_event_id != touched_parent.last_modified_by_event_id
            })
        {
            return Err(MetadataBackendError::Serde(format!(
                "combined create parent transition mismatch for {}: durable={:?} {} {}, touched={:?} {} {}, version_record={}",
                parent_path,
                durable_parent.kind,
                durable_parent.object_id,
                durable_parent.version,
                touched_parent.kind,
                touched_parent.object_id,
                touched_parent.version,
                touched_parent_version.is_some()
            )));
        }
        if touched_parent.version == durable_parent.version
            || touched_parent.modified_at_unix_nanos <= durable_parent.modified_at_unix_nanos
            || touched_parent.changed_at_unix_nanos <= durable_parent.changed_at_unix_nanos
        {
            return Err(MetadataBackendError::Conflict {
                object_id: durable_parent.object_id,
                expected: touched_parent.version.clone(),
                current: durable_parent.version,
            });
        }
        let mut durable_replica_node_ids = BTreeSet::new();
        for receipt in receipts {
            if receipt.receipt.file_system_id != pending_intent.file_system_id
                || receipt.receipt.object_id != pending_intent.object_id
                || receipt.receipt.committed_version != pending_intent.target_version
                || receipt.content_hash.as_deref() != Some(expected_content_hash)
                || !pending_intent
                    .desired_replica_node_ids
                    .contains(&receipt.replica_node_id)
            {
                return Err(MetadataBackendError::Serde(format!(
                    "combined create receipt mismatch for object {}",
                    pending_intent.object_id
                )));
            }
            let receipt_key = (
                pending_intent.object_id.clone(),
                pending_intent.target_version.clone(),
                receipt.replica_node_id.clone(),
            );
            if self
                .tables
                .receipts
                .get(&receipt_key)
                .is_some_and(|existing| existing.content_hash != receipt.content_hash)
            {
                return Err(MetadataBackendError::Serde(format!(
                    "combined create conflicts with the durable receipt for object {} replica {}",
                    pending_intent.object_id, receipt.replica_node_id
                )));
            }
            durable_replica_node_ids.insert(receipt.replica_node_id.clone());
        }
        if durable_replica_node_ids.len() < pending_intent.policy.min_commit_replicas {
            return Err(MetadataBackendError::Serde(format!(
                "combined create for object {} has {} durable receipts; {} required",
                pending_intent.object_id,
                durable_replica_node_ids.len(),
                pending_intent.policy.min_commit_replicas
            )));
        }
        validate_namespace_transaction_preconditions(&self.tables, &transaction)?;

        self.backfill_file_history_durability_unlocked(
            crate::file_history::legacy_capture_candidates(&self.tables, &transaction.objects),
        )?;
        let mut mutations = Vec::with_capacity(receipts.len() + 3);
        mutations.push(DfsMetadataMutation::put_intent(pending_intent));
        mutations.extend(receipts.iter().map(DfsMetadataMutation::put_receipt));
        mutations.push(DfsMetadataMutation::ApplyNamespaceTransaction {
            file_system_id: transaction.file_system_id.clone(),
            transaction_id: transaction.transaction_id.clone(),
            object_count: transaction.objects.len(),
            operation_id: committed_intent.operation_id.clone(),
            transaction: Some(transaction),
        });
        mutations.push(DfsMetadataMutation::put_intent(committed_intent));
        let entries = self.write_journal_entries_unlocked(mutations)?;
        let planned = self.plan_written_journal_entries_table_ops(&entries)?;
        self.append_preapplied_table_ops_for_written_entries(
            &entries,
            planned.last_materialized_sequence,
            planned.table_ops,
            planned.rollback,
        )?;
        metadata_backend_perf_trace(format_args!(
            "metadata-commit-create-intent-receipts-namespace shard={} object={} version={} receipts={} entries={} total_ms={}",
            self.shard_id.0,
            pending_intent.object_id,
            pending_intent.target_version,
            receipts.len(),
            entries.len(),
            trace_started.elapsed().as_millis()
        ));
        Ok(entries)
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
        query: &crate::DfsFileHistoryShardQuery,
    ) -> Result<crate::DfsFileHistoryShardResult> {
        if !query.shard_id.0.is_empty() && query.shard_id != self.shard_id {
            return Err(MetadataBackendError::Serde(
                "file history query names another metadata shard".into(),
            ));
        }
        crate::file_history_query::query_backend_file_history(self, query)
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

    fn conflict_records_page(
        &self,
        after: Option<&DfsConflictScanCursor>,
        max_entries: usize,
    ) -> Result<DfsConflictScanPage> {
        let tables = self.current_tables_snapshot()?;
        Ok(conflict_records_page_from_tables(
            &tables,
            after,
            max_entries,
        ))
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

    fn put_receipt(
        &mut self,
        receipt: &DfsReplicaReceiptRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::put_receipt(receipt),
            vec![DfsMetadataTableOp::PutReceipt {
                receipt: receipt.clone(),
            }],
        )
    }

    fn reconcile_write_intent_if_unchanged(
        &mut self,
        shard_id: &DfsMetadataShardId,
        expected: &DfsWriteIntentRecord,
        expected_object: Option<&DfsObjectMetadata>,
        allow_abort_missing: bool,
        reject_if_unpublished: bool,
    ) -> Result<Option<DfsWriteIntentRecord>> {
        if shard_id != &self.shard_id
            || !expected.publication.is_current()
            || !matches!(
                expected.status,
                DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
            )
        {
            return Ok(None);
        }
        let _lock = self.begin_metadata_write()?;
        if self
            .get_intent(&expected.object_id, &expected.intent_id)?
            .as_ref()
            != Some(expected)
        {
            return Ok(None);
        }
        let current = self.get_object(&expected.object_id)?;
        let unchanged = match (current.as_ref(), expected_object) {
            (None, None) => true,
            (Some(current), Some(observed)) => {
                current.file_system_id == observed.file_system_id
                    && current.object_id == observed.object_id
                    && current.version == observed.version
                    && current.path == observed.path
                    && current.kind == observed.kind
                    && current.content_hash == observed.content_hash
            }
            _ => false,
        };
        if !unchanged {
            return Ok(None);
        }
        let matching_hash = |hash: Option<&str>| {
            expected
                .content_hash
                .as_deref()
                .is_none_or(|expected| hash == Some(expected))
        };
        let published = current.as_ref().is_some_and(|object| {
            object.file_system_id == expected.file_system_id
                && object.version == expected.target_version
                && matching_hash(object.content_hash.as_deref())
        }) || self
            .get_version(&expected.object_id, &expected.target_version)?
            .is_some_and(|version| {
                version.visible
                    && version.file_system_id == expected.file_system_id
                    && matching_hash(version.content_hash.as_deref())
            });
        let status = if published {
            DfsWriteIntentStatus::Committed
        } else if current.as_ref().is_some_and(|object| {
            object.file_system_id == expected.file_system_id
                && Some(&object.version) != expected.base_version.as_ref()
                && object.version != expected.target_version
        }) || (current.is_none() && allow_abort_missing)
        {
            DfsWriteIntentStatus::Aborted
        } else if reject_if_unpublished {
            DfsWriteIntentStatus::Rejected
        } else {
            return Ok(None);
        };
        let mut updated = expected.clone();
        updated.status = status;
        updated.updated_at = Some(OffsetDateTime::now_utc());
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::put_intent(&updated),
            vec![DfsMetadataTableOp::PutIntent {
                intent: updated.clone(),
            }],
        )?;
        Ok(Some(updated))
    }

    fn remove_receipt(
        &mut self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
    ) -> Result<Option<DfsMetadataJournalEntry>> {
        let _lock = self.begin_metadata_write()?;
        let Some(existing) = self
            .tables
            .receipts
            .get(&(object_id.clone(), version.clone(), replica_node_id.clone()))
            .cloned()
        else {
            return Ok(None);
        };
        let entry = self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::RemoveReceipt {
                file_system_id: existing.receipt.file_system_id,
                object_id: object_id.clone(),
                version: version.clone(),
                replica_node_id: replica_node_id.clone(),
            },
            vec![DfsMetadataTableOp::RemoveReceipt {
                object_id: object_id.clone(),
                version: version.clone(),
                replica_node_id: replica_node_id.clone(),
            }],
        )?;
        Ok(Some(entry))
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

    fn put_intent(&mut self, intent: &DfsWriteIntentRecord) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        let preserved = self.preserved_terminal_intent(intent)?;
        let intent = preserved.as_ref().unwrap_or(intent);
        let key = (intent.object_id.clone(), intent.intent_id.clone());
        if let Some(receipt) = self.tables.intent_receipts.get(&key).cloned() {
            if !receipt.matches_intent(intent) {
                return Err(MetadataBackendError::Serde(format!(
                    "write intent {}/{} was reused after compaction with a different payload",
                    intent.object_id, intent.intent_id
                )));
            }
            return self.write_journal_entry_and_apply_table_ops_unlocked(
                DfsMetadataMutation::put_intent_receipt(&receipt),
                vec![DfsMetadataTableOp::PutIntentReceipt { receipt }],
            );
        }
        if !self.tables.intents.contains_key(&key)
            && self.tables.intent_receipt_retention.history_expired(intent)
        {
            return Err(MetadataBackendError::Serde(format!(
                "write intent {}/{} idempotency history expired or is unknown",
                intent.object_id, intent.intent_id
            )));
        }
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::put_intent(intent),
            vec![DfsMetadataTableOp::PutIntent {
                intent: intent.clone(),
            }],
        )
    }

    fn put_migrated_intent_for_object(
        &mut self,
        _object: Option<&DfsObjectMetadata>,
        intent: &DfsWriteIntentRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        let key = (intent.object_id.clone(), intent.intent_id.clone());
        if let Some(receipt) = self.tables.intent_receipts.get(&key).cloned() {
            if !receipt.matches_intent(intent) {
                return Err(MetadataBackendError::Serde(format!(
                    "migrated write intent {}/{} conflicts with a compacted target receipt",
                    intent.object_id, intent.intent_id
                )));
            }
            return self.write_journal_entry_and_apply_table_ops_unlocked(
                DfsMetadataMutation::put_intent_receipt(&receipt),
                vec![DfsMetadataTableOp::PutIntentReceipt { receipt }],
            );
        }
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::put_migrated_intent(intent),
            vec![DfsMetadataTableOp::PutIntent {
                intent: intent.clone(),
            }],
        )
    }

    fn put_intent_receipt_for_object(
        &mut self,
        _object: Option<&DfsObjectMetadata>,
        receipt: &DfsWriteIntentReceipt,
    ) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        let key = (receipt.object_id.clone(), receipt.intent_id.clone());
        if let Some(intent) = self.tables.intents.get(&key)
            && !receipt.matches_intent(intent)
        {
            return Err(MetadataBackendError::Serde(format!(
                "migrated intent receipt {}/{} conflicts with a full target intent",
                receipt.object_id, receipt.intent_id
            )));
        }
        if let Some(existing) = self.tables.intent_receipts.get(&key)
            && existing != receipt
        {
            return Err(MetadataBackendError::Serde(format!(
                "migrated intent receipt {}/{} conflicts with a compact target receipt",
                receipt.object_id, receipt.intent_id
            )));
        }
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::put_intent_receipt(receipt),
            vec![DfsMetadataTableOp::PutIntentReceipt {
                receipt: receipt.clone(),
            }],
        )
    }

    fn merge_intent_receipt_retention_for_object(
        &mut self,
        _object: Option<&DfsObjectMetadata>,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        retention: &DfsWriteIntentReceiptRetentionState,
    ) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        let retention = retention.for_object(object_id);
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::merge_intent_receipt_retention(
                file_system_id.clone(),
                object_id.clone(),
                retention.clone(),
            ),
            vec![DfsMetadataTableOp::MergeIntentReceiptRetention { retention }],
        )
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

    fn rebind_prepared_write_intent(
        &mut self,
        _object: Option<&DfsObjectMetadata>,
        expected_intent: &DfsWriteIntentRecord,
        rebound_intent: &DfsWriteIntentRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        let key = (
            expected_intent.object_id.clone(),
            expected_intent.intent_id.clone(),
        );
        let current = self.tables.intents.get(&key).cloned().ok_or_else(|| {
            MetadataBackendError::Serde(format!(
                "write intent {} does not exist for object {}",
                expected_intent.intent_id, expected_intent.object_id
            ))
        })?;
        if current == *rebound_intent {
            return self.write_journal_entry_and_apply_table_ops_unlocked(
                DfsMetadataMutation::put_intent(rebound_intent),
                vec![DfsMetadataTableOp::PutIntent {
                    intent: rebound_intent.clone(),
                }],
            );
        }
        if current != *expected_intent {
            return Err(MetadataBackendError::Serde(format!(
                "write intent {} rebind record fence mismatch",
                expected_intent.intent_id
            )));
        }
        if !matches!(
            current.status,
            DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
        ) || !prepared_write_intent_rebind_payload_matches(&current, rebound_intent)
        {
            return Err(MetadataBackendError::Serde(format!(
                "write intent {} rebind changes the prepared operation payload or status",
                expected_intent.intent_id
            )));
        }
        if rebound_intent.owner_epoch <= current.owner_epoch {
            return Err(MetadataBackendError::Serde(format!(
                "write intent {} rebind epoch {} must be newer than {}",
                expected_intent.intent_id, rebound_intent.owner_epoch, current.owner_epoch
            )));
        }
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::put_intent(rebound_intent),
            vec![DfsMetadataTableOp::PutIntent {
                intent: rebound_intent.clone(),
            }],
        )
    }

    fn put_intent_and_receipts_for_object(
        &mut self,
        object: Option<&DfsObjectMetadata>,
        intent: &DfsWriteIntentRecord,
        receipts: &[DfsReplicaReceiptRecord],
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        let _lock = self.begin_metadata_write()?;
        let preserved = self.preserved_terminal_intent(intent)?;
        let intent = preserved.as_ref().unwrap_or(intent);
        let key = (intent.object_id.clone(), intent.intent_id.clone());
        if let Some(receipt) = self.tables.intent_receipts.get(&key).cloned() {
            if !receipt.matches_intent(intent) {
                return Err(MetadataBackendError::Serde(format!(
                    "write intent {}/{} was reused after compaction with a different payload",
                    intent.object_id, intent.intent_id
                )));
            }
            let entries = self.write_journal_entries_unlocked(
                std::iter::once(DfsMetadataMutation::put_intent_receipt(&receipt))
                    .chain(receipts.iter().map(DfsMetadataMutation::put_receipt))
                    .collect(),
            )?;
            self.materialize_written_journal_entries_unlocked(&entries)?;
            return Ok(entries);
        }
        if !self.tables.intents.contains_key(&key)
            && self.tables.intent_receipt_retention.history_expired(intent)
        {
            return Err(MetadataBackendError::Serde(format!(
                "write intent {}/{} idempotency history expired or is unknown",
                intent.object_id, intent.intent_id
            )));
        }
        let mut mutations = Vec::with_capacity(1 + receipts.len());
        let mut backfill_entries = Vec::new();
        if intent.publication.is_current()
            && crate::intent::write_intent_status_is_active(&intent.status)
            && intent.content_hash.is_some()
            && self
                .tables
                .file_systems
                .get(&intent.file_system_id)
                .is_some_and(|fs| !fs.file_history.is_empty())
            && let Some(mut next) = object.or(intent.object_metadata.as_ref()).cloned()
        {
            next.version = intent.target_version.clone();
            next.content_hash = intent.content_hash.clone();
            backfill_entries = self.backfill_file_history_durability_unlocked(
                crate::file_history::legacy_capture_candidates(&self.tables, [&next]),
            )?;
        }
        mutations.push(DfsMetadataMutation::put_intent(intent));
        mutations.extend(receipts.iter().map(DfsMetadataMutation::put_receipt));
        let entries = self.write_journal_entries_unlocked(mutations)?;
        self.materialize_written_journal_entries_unlocked(&entries)?;
        backfill_entries.extend(entries);
        Ok(backfill_entries)
    }

    fn get_intent(
        &self,
        object_id: &DfsObjectId,
        intent_id: &str,
    ) -> Result<Option<DfsWriteIntentRecord>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables
            .intents
            .get(&(object_id.clone(), intent_id.to_string()))
            .cloned()
            .or_else(|| {
                tables
                    .intent_receipts
                    .get(&(object_id.clone(), intent_id.to_string()))
                    .map(DfsWriteIntentReceipt::to_intent_record)
            }))
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

    fn commit_visible_version(
        &mut self,
        expected_version: &DfsVersion,
        committed_object: &DfsObjectMetadata,
        visible_version: &DfsObjectVersionRecord,
        required_replica_node_ids: &[NodeId],
    ) -> Result<DfsMetadataJournalEntry> {
        self.commit_visible_version_with_operation_id(
            expected_version,
            committed_object,
            visible_version,
            required_replica_node_ids,
            None,
        )
    }

    fn commit_visible_version_with_operation_id(
        &mut self,
        expected_version: &DfsVersion,
        committed_object: &DfsObjectMetadata,
        visible_version: &DfsObjectVersionRecord,
        required_replica_node_ids: &[NodeId],
        operation_id: Option<&str>,
    ) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        crate::file_history::validate_file_history_transition(&self.tables, committed_object)?;
        let current = self
            .get_object(&committed_object.object_id)?
            .ok_or_else(|| MetadataBackendError::UnknownObject {
                object_id: committed_object.object_id.clone(),
            })?;
        if &current.version != expected_version {
            return Err(MetadataBackendError::Conflict {
                object_id: committed_object.object_id.clone(),
                expected: expected_version.clone(),
                current: current.version,
            });
        }

        for node_id in required_replica_node_ids {
            if !self.tables.receipts.contains_key(&(
                committed_object.object_id.clone(),
                committed_object.version.clone(),
                node_id.clone(),
            )) {
                return Err(MetadataBackendError::Conflict {
                    object_id: committed_object.object_id.clone(),
                    expected: expected_version.clone(),
                    current: current.version.clone(),
                });
            }
        }

        let mut commit_record = visible_commit_record_from_parts(
            expected_version,
            committed_object,
            visible_version,
            required_replica_node_ids,
            OffsetDateTime::now_utc(),
        );
        commit_record.operation_id = operation_id.map(str::to_owned);
        self.backfill_file_history_durability_unlocked(
            crate::file_history::legacy_capture_candidates(&self.tables, [committed_object]),
        )?;
        self.write_journal_entry_and_materialize_unlocked(
            DfsMetadataMutation::commit_visible_version(&commit_record),
            |backend, entry| {
                commit_record.created_at = entry.created_at;
                Ok(visible_commit_table_ops(&backend.tables, &commit_record))
            },
        )
    }

    fn apply_namespace_transaction(
        &mut self,
        transaction: &DfsNamespaceTransaction,
    ) -> Result<DfsMetadataJournalEntry> {
        self.apply_namespace_transaction_with_operation_id(transaction, None)
    }

    fn apply_namespace_transaction_with_operation_id(
        &mut self,
        transaction: &DfsNamespaceTransaction,
        operation_id: Option<&str>,
    ) -> Result<DfsMetadataJournalEntry> {
        self.apply_namespace_transaction_inner(transaction, operation_id, None)
    }

    fn apply_namespace_transaction_if_unchanged(
        &mut self,
        transaction: &DfsNamespaceTransaction,
        expected_objects: &[DfsNamespaceObjectVersionExpectation],
        expected_paths: &[DfsNamespacePathOccupantExpectation],
        operation_id: Option<&str>,
    ) -> Result<DfsMetadataJournalEntry> {
        self.apply_namespace_transaction_inner(
            transaction,
            operation_id,
            Some((expected_objects, expected_paths)),
        )
    }
    fn prepare_namespace_transaction(
        &mut self,
        prepare: &DfsNamespacePrepareRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        let mut prepare = prepare.clone();
        self.prepare_namespace_transaction_in_place(&mut prepare)
    }

    fn prepare_normalized_namespace_transaction_in_place(
        &mut self,
        prepare: DfsNormalizedNamespacePrepare<'_>,
    ) -> Result<DfsMetadataJournalEntry> {
        let prepare = prepare.as_record();
        let trace_started = Instant::now();
        let lock_started = Instant::now();
        let _lock = self.begin_metadata_write()?;
        let lock_ms = lock_started.elapsed().as_millis();
        if prepare.status != DfsNamespacePrepareStatus::Prepared {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {} must enter with prepared status",
                prepare.operation_id
            )));
        }
        if let Some(receipt) = self
            .tables
            .namespace_prepare_receipts
            .get(&prepare.operation_id)
            .cloned()
        {
            let canonical_digest =
                crate::service_replica::metadata_namespace_prepare_payload_digest_normalized(
                    prepare,
                )
                .map_err(|error| MetadataBackendError::Serde(error.to_string()))?;
            if prepare.operation_digest != canonical_digest
                || !receipt.matches_prepare_identity(prepare)
            {
                return Err(MetadataBackendError::Serde(format!(
                    "namespace prepare {} reuses a compacted operation id with a different payload",
                    prepare.operation_id
                )));
            }
            if receipt.status == DfsNamespacePrepareStatus::Aborted {
                return Err(MetadataBackendError::Serde(format!(
                    "namespace prepare {} is aborted",
                    prepare.operation_id
                )));
            }
            let entry = self.write_journal_entry_and_apply_table_ops_unlocked(
                DfsMetadataMutation::put_namespace_prepare_receipt(&receipt),
                vec![DfsMetadataTableOp::PutNamespacePrepareReceipt { receipt }],
            )?;
            return Ok(entry);
        }
        if self.tables.namespace_prepare_history_expired(prepare) {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {} idempotency history expired or is unknown",
                prepare.operation_id
            )));
        }
        if let Some(existing) = self
            .tables
            .namespace_prepares
            .get(&prepare.operation_id)
            .cloned()
            && existing.status == DfsNamespacePrepareStatus::Committed
            && namespace_prepare_payload_matches(&existing, prepare)
            && namespace_transaction_recovery_is_complete(&self.tables, &existing.transaction)
            && namespace_prepare_content_receipts_are_complete(&self.tables, &existing)
            && namespace_prepare_request_completion_is_materialized(&self.tables, &existing)
        {
            // A lost response may retry the exact prepared operation after its
            // visible transaction already committed. Replicate the durable
            // committed marker again instead of attempting the forbidden
            // Committed -> Prepared transition.
            let entry = self.write_namespace_prepare_and_materialize_unlocked(
                &existing,
                vec![DfsMetadataTableOp::put_namespace_prepare(existing.clone())],
            )?;
            return Ok(entry);
        }
        let plan_started = Instant::now();
        let mut table_plan = namespace_prepare_table_plan_for_payload(
            &self.tables,
            &self.shard_id,
            Cow::Borrowed(prepare),
        )?;
        if table_plan.ops.is_empty() {
            table_plan
                .ops
                .push(DfsMetadataTableOp::put_namespace_prepare(
                    (*prepare).clone(),
                ));
        }
        let prepared_footprint = table_plan.prepared_footprint.take().ok_or_else(|| {
            MetadataBackendError::Serde(format!(
                "namespace prepare {} validated without an active footprint",
                prepare.operation_id
            ))
        })?;
        let plan_ms = plan_started.elapsed().as_millis();
        let write_started = Instant::now();
        self.backfill_file_history_durability_unlocked(
            crate::file_history::legacy_capture_candidates(
                &self.tables,
                &prepare.transaction.objects,
            ),
        )?;
        let entry = self.write_namespace_prepare_and_materialize_with_footprint_unlocked(
            prepare,
            table_plan.ops,
            Some(prepared_footprint),
        )?;
        let write_ms = write_started.elapsed().as_millis();
        metadata_backend_perf_trace(format_args!(
            "metadata-prepare-namespace-transaction shard={} operation={} objects={} versions={} receipts={} lock_ms={} plan_ms={} write_ms={} total_ms={}",
            self.shard_id.0,
            prepare.operation_id,
            prepare.transaction.objects.len(),
            prepare.transaction.versions.len(),
            prepare.content_receipts.len(),
            lock_ms,
            plan_ms,
            write_ms,
            trace_started.elapsed().as_millis()
        ));
        Ok(entry)
    }

    fn rebind_prepared_namespace_transaction(
        &mut self,
        operation_id: &str,
        expected_operation_digest: &str,
        expected_owner_epoch: u64,
        rebound_prepare: &DfsNamespacePrepareRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        let current = self
            .tables
            .namespace_prepares
            .get(operation_id)
            .cloned()
            .ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "namespace prepare {operation_id} does not exist"
                ))
            })?;
        let rebound = normalized_namespace_prepare(rebound_prepare);
        if rebound.operation_id != operation_id {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare rebind id {operation_id} does not match rebound id {}",
                rebound.operation_id
            )));
        }
        let canonical_digest =
            crate::service_replica::metadata_namespace_prepare_payload_digest(&rebound)
                .map_err(|error| MetadataBackendError::Serde(error.to_string()))?;
        if rebound.operation_digest != canonical_digest {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {operation_id} rebound digest is not canonical"
            )));
        }
        // A lost response may repeat the exact rebind after the first journal
        // entry became durable. Re-journal the same prepared record without
        // weakening the old-fence compare for any different replacement.
        if current == rebound {
            return self.write_namespace_prepare_and_materialize_unlocked(
                &rebound,
                vec![DfsMetadataTableOp::put_namespace_prepare(rebound.clone())],
            );
        }
        if current.status != DfsNamespacePrepareStatus::Prepared
            || current.operation_digest != expected_operation_digest
            || current.owner_epoch != expected_owner_epoch
        {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {operation_id} rebind digest/epoch fence mismatch"
            )));
        }
        if !namespace_prepare_rebind_payload_matches(&current, &rebound) {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {operation_id} rebind changes the prepared operation payload"
            )));
        }
        if rebound.owner_epoch <= current.owner_epoch {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {operation_id} rebind epoch {} must be newer than {}",
                rebound.owner_epoch, current.owner_epoch
            )));
        }
        let validated = validate_namespace_prepare_shape(&self.tables, &self.shard_id, &rebound)?;
        validate_namespace_prepare_expectations(&self.tables, &rebound)?;
        validate_namespace_prepare_does_not_overlap(&self.tables, &rebound, &validated.footprint)?;
        validate_namespace_transaction_preconditions(&self.tables, &rebound.transaction)?;
        self.write_namespace_prepare_and_materialize_unlocked(
            &rebound,
            vec![DfsMetadataTableOp::put_namespace_prepare(rebound.clone())],
        )
    }

    fn commit_prepared_namespace_transaction(
        &mut self,
        operation_id: &str,
        expected_operation_digest: &str,
        expected_owner_epoch: u64,
    ) -> Result<DfsMetadataJournalEntry> {
        let trace_started = Instant::now();
        let lock_started = Instant::now();
        let _lock = self.begin_metadata_write()?;
        let lock_ms = lock_started.elapsed().as_millis();
        let lookup_started = Instant::now();
        if let Some(receipt) = self
            .tables
            .namespace_prepare_receipts
            .get(operation_id)
            .cloned()
        {
            if receipt.operation_digest != expected_operation_digest
                || receipt.owner_epoch != expected_owner_epoch
            {
                return Err(MetadataBackendError::Serde(format!(
                    "namespace prepare {operation_id} digest/epoch fence mismatch"
                )));
            }
            if receipt.status == DfsNamespacePrepareStatus::Aborted {
                return Err(MetadataBackendError::Serde(format!(
                    "namespace prepare {operation_id} is aborted"
                )));
            }
            return self.write_journal_entry_and_apply_table_ops_unlocked(
                DfsMetadataMutation::put_namespace_prepare_receipt(&receipt),
                vec![DfsMetadataTableOp::PutNamespacePrepareReceipt { receipt }],
            );
        }
        let prepare = self
            .tables
            .namespace_prepares
            .get(operation_id)
            .ok_or_else(|| {
                if self.tables.namespace_prepare_history_has_evictions() {
                    MetadataBackendError::Serde(format!(
                        "namespace prepare {operation_id} idempotency history expired or is unknown"
                    ))
                } else {
                    MetadataBackendError::Serde(format!(
                        "namespace prepare {operation_id} does not exist"
                    ))
                }
            })?;
        let lookup_ms = lookup_started.elapsed().as_millis();
        if prepare.operation_digest != expected_operation_digest
            || prepare.owner_epoch != expected_owner_epoch
        {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {operation_id} digest/epoch fence mismatch"
            )));
        }
        if prepare.status == DfsNamespacePrepareStatus::Aborted {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {operation_id} is aborted"
            )));
        }
        if prepare.status == DfsNamespacePrepareStatus::Prepared {
            // Validate against the still-hidden namespace before making the
            // committed marker durable. The materialized log can then refer to
            // that exact prepared body instead of encoding every derived table
            // operation again. The journal remains the source of truth across
            // a table append failure or crash.
            //
            // Payload shape and active-footprint overlap were already validated
            // under the write lock which installed this exact durable Prepared
            // record. Recheck only table state which could change while the
            // primary waited for replica acknowledgement.
            let commit_state_started = Instant::now();
            validate_prepared_namespace_commit_state(&self.tables, prepare)?;
            let commit_state_ms = commit_state_started.elapsed().as_millis();
            let expectations_started = Instant::now();
            validate_namespace_prepare_expectations(&self.tables, prepare)?;
            let expectations_ms = expectations_started.elapsed().as_millis();
            let preconditions_started = Instant::now();
            validate_namespace_transaction_preconditions(&self.tables, &prepare.transaction)?;
            let preconditions_ms = preconditions_started.elapsed().as_millis();

            let mutation = DfsMetadataMutation::commit_namespace_prepare(prepare);
            let object_count = prepare.transaction.objects.len();
            let version_count = prepare.transaction.versions.len();
            let receipt_count = prepare.content_receipts.len();
            let journal_started = Instant::now();
            let entry = self.write_journal_unlocked(mutation)?;
            let journal_ms = journal_started.elapsed().as_millis();
            let table_op = DfsMetadataTableOp::CommitNamespacePrepare {
                operation_id: operation_id.to_string(),
                operation_digest: expected_operation_digest.to_string(),
                owner_epoch: expected_owner_epoch,
                retained_at: Some(entry.created_at),
            };
            // The preceding Prepared table record already owns the complete
            // transaction, receipt set, and routed-request evidence. Persist
            // one semantic commit marker, then expand it directly into the
            // in-memory indexes. This avoids encoding the same 4k-file batch
            // as tens of thousands of derived table operations while keeping
            // the authoritative journal and crash replay exact.
            let table_started = Instant::now();
            self.append_and_apply_table_ops(entry.sequence, vec![table_op])?;
            let table_ms = table_started.elapsed().as_millis();
            let checkpoint_started = Instant::now();
            self.checkpoint_local_materialization_cursor_if_due(entry.sequence)?;
            let checkpoint_ms = checkpoint_started.elapsed().as_millis();
            metadata_backend_perf_trace(format_args!(
                "metadata-commit-namespace-prepare shard={} operation={} objects={} versions={} receipts={} lock_ms={} lookup_ms={} commit_state_ms={} expectations_ms={} preconditions_ms={} journal_ms={} table_ms={} checkpoint_ms={} total_ms={}",
                self.shard_id.0,
                operation_id,
                object_count,
                version_count,
                receipt_count,
                lock_ms,
                lookup_ms,
                commit_state_ms,
                expectations_ms,
                preconditions_ms,
                journal_ms,
                table_ms,
                checkpoint_ms,
                trace_started.elapsed().as_millis()
            ));
            return Ok(entry);
        }

        // A repeated commit can be repairing a durable Committed marker whose
        // namespace table batch was interrupted. Keep that idempotent recovery
        // path on the recovery/reference planner.
        let mut prepare = prepare.clone();
        prepare.status = DfsNamespacePrepareStatus::Committed;
        let mut table_ops =
            namespace_prepare_table_ops_for_record(&self.tables, &self.shard_id, &prepare)?;
        if table_ops.is_empty() {
            table_ops.push(DfsMetadataTableOp::put_namespace_prepare(prepare.clone()));
        }
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::commit_namespace_prepare(&prepare),
            table_ops,
        )
    }

    fn abort_prepared_namespace_transaction(
        &mut self,
        operation_id: &str,
        expected_operation_digest: &str,
        expected_owner_epoch: u64,
    ) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        if let Some(receipt) = self
            .tables
            .namespace_prepare_receipts
            .get(operation_id)
            .cloned()
        {
            if receipt.operation_digest != expected_operation_digest
                || receipt.owner_epoch != expected_owner_epoch
            {
                return Err(MetadataBackendError::Serde(format!(
                    "namespace prepare {operation_id} digest/epoch fence mismatch"
                )));
            }
            if receipt.status == DfsNamespacePrepareStatus::Committed {
                return Err(MetadataBackendError::Serde(format!(
                    "namespace prepare {operation_id} is already committed"
                )));
            }
            return self.write_journal_entry_and_apply_table_ops_unlocked(
                DfsMetadataMutation::put_namespace_prepare_receipt(&receipt),
                vec![DfsMetadataTableOp::PutNamespacePrepareReceipt { receipt }],
            );
        }
        let mut prepare = self
            .tables
            .namespace_prepares
            .get(operation_id)
            .cloned()
            .ok_or_else(|| {
                if self.tables.namespace_prepare_history_has_evictions() {
                    MetadataBackendError::Serde(format!(
                        "namespace prepare {operation_id} idempotency history expired or is unknown"
                    ))
                } else {
                    MetadataBackendError::Serde(format!(
                        "namespace prepare {operation_id} does not exist"
                    ))
                }
            })?;
        if prepare.operation_digest != expected_operation_digest
            || prepare.owner_epoch != expected_owner_epoch
        {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {operation_id} digest/epoch fence mismatch"
            )));
        }
        if prepare.status == DfsNamespacePrepareStatus::Committed {
            return Err(MetadataBackendError::Serde(format!(
                "namespace prepare {operation_id} is already committed"
            )));
        }
        prepare.status = DfsNamespacePrepareStatus::Aborted;
        let mut table_ops =
            namespace_prepare_table_ops_for_record(&self.tables, &self.shard_id, &prepare)?;
        if table_ops.is_empty() {
            table_ops.push(DfsMetadataTableOp::put_namespace_prepare(prepare.clone()));
        }
        self.write_namespace_prepare_and_materialize_unlocked(&prepare, table_ops)
    }

    fn get_namespace_prepare(
        &self,
        operation_id: &str,
    ) -> Result<Option<DfsNamespacePrepareRecord>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables.namespace_prepares.get(operation_id).cloned())
    }

    fn list_prepared_namespace_prepares(&self) -> Result<Vec<DfsNamespacePrepareRecord>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables
            .namespace_prepares
            .values()
            .filter(|prepare| prepare.status == DfsNamespacePrepareStatus::Prepared)
            .cloned()
            .collect())
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
            self.supports_retained_repair_owners(),
        )?
        .report;
        self.populate_metadata_history_storage_stats(&mut report)?;
        Ok(report)
    }

    fn gc_metadata_history(
        &mut self,
        options: DfsMetadataHistoryGcOptions,
    ) -> Result<DfsMetadataHistoryGcReport> {
        self.gc_metadata_history_authoritative(options)
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

    fn put_namespace_job(&mut self, job: &DfsNamespaceJobRecord) -> Result<()> {
        let _lock = self.begin_metadata_write()?;
        if let Some(receipt) = self.tables.namespace_job_receipts.get(&job.job_id).cloned() {
            if !receipt.matches_terminal_job(job) {
                return Err(MetadataBackendError::Serde(format!(
                    "namespace job {} was reused after compaction with a different payload",
                    job.job_id
                )));
            }
            return self
                .write_journal_entry_and_apply_table_ops_unlocked(
                    DfsMetadataMutation::put_namespace_job_receipt(&receipt),
                    vec![DfsMetadataTableOp::PutNamespaceJobReceipt { receipt }],
                )
                .map(drop);
        }
        if let Some(existing) = self.tables.namespace_jobs.get(&job.job_id) {
            if existing == job {
                return Ok(());
            }
            if existing.status != DfsNamespaceJobStatus::Running {
                let same_identity = existing.file_system_id == job.file_system_id
                    && existing.origin == job.origin
                    && existing.kind == job.kind
                    && existing.root_path == job.root_path
                    && existing.target_path == job.target_path
                    && existing.created_at == job.created_at;
                if same_identity && job.status == DfsNamespaceJobStatus::Running {
                    // Another runner already made the same durable saga
                    // terminal. A stale in-memory frame must not resurrect it;
                    // the caller will reload the canonical terminal record.
                    return Ok(());
                }
                return Err(MetadataBackendError::Serde(format!(
                    "namespace job {} cannot replace terminal state {:?} with {:?}",
                    job.job_id, existing.status, job.status
                )));
            }
        }
        if !self.tables.namespace_jobs.contains_key(&job.job_id)
            && self
                .tables
                .namespace_job_history_expired(job.created_at, &job.job_id)
        {
            return Err(MetadataBackendError::Serde(format!(
                "namespace job {} idempotency history expired or is unknown",
                job.job_id
            )));
        }
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::put_namespace_job(job),
            vec![DfsMetadataTableOp::PutNamespaceJob { job: job.clone() }],
        )
        .map(drop)
    }

    fn put_namespace_request_on_shard(
        &mut self,
        shard_id: &DfsMetadataShardId,
        request: &DfsNamespaceRequestRecord,
    ) -> Result<DfsMetadataJournalEntry> {
        if shard_id != &self.shard_id {
            return Err(MetadataBackendError::Serde(format!(
                "namespace request shard {} does not match backend shard {}",
                shard_id.0, self.shard_id.0
            )));
        }
        validate_namespace_request_shape(request)?;
        let _lock = self.begin_metadata_write()?;
        let key = (
            request.requester_node_id.clone(),
            request.request_id.clone(),
        );
        if let Some(receipt) = self.tables.namespace_request_receipts.get(&key).cloned() {
            if !receipt.matches_completed_request(request) {
                return Err(MetadataBackendError::Serde(format!(
                    "namespace request id {}/{} was reused after compaction with a different payload",
                    request.requester_node_id, request.request_id
                )));
            }
            return self.write_journal_entry_and_apply_table_ops_unlocked(
                DfsMetadataMutation::put_namespace_request_receipt(&receipt),
                vec![DfsMetadataTableOp::put_namespace_request_receipt(receipt)],
            );
        }
        if let Some(existing) = self.tables.namespace_requests.get(&key)
            && !namespace_request_transition_is_valid(existing, request)
        {
            return Err(MetadataBackendError::Serde(format!(
                "namespace request id {}/{} was reused with a different payload or invalid status transition",
                request.requester_node_id, request.request_id
            )));
        }
        if !self.tables.namespace_requests.contains_key(&key)
            && self
                .tables
                .namespace_request_history_expired(&request.requester_node_id, &request.request_id)
        {
            return Err(MetadataBackendError::Serde(format!(
                "namespace request {}/{} idempotency history expired or is unknown",
                request.requester_node_id, request.request_id
            )));
        }
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::put_namespace_request(request),
            vec![DfsMetadataTableOp::put_namespace_request(request.clone())],
        )
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
        let prepared = match tables.namespace_prepare_request_owners.get(&request_key) {
            None => None,
            Some(operation_ids) if operation_ids.len() != 1 => {
                return Err(MetadataBackendError::Serde(format!(
                    "pending namespace request {requester_node_id}/{request_id} owns {} prepared transactions",
                    operation_ids.len()
                )));
            }
            Some(operation_ids) => {
                let operation_id = operation_ids
                    .first()
                    .expect("non-empty request prepare owner index");
                Some(
                    tables
                        .namespace_prepares
                        .get(operation_id)
                        .cloned()
                        .ok_or_else(|| {
                            MetadataBackendError::Serde(format!(
                                "pending namespace request {requester_node_id}/{request_id} indexes missing prepare {operation_id}"
                            ))
                        })?,
                )
            }
        };
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

    fn get_prepared_namespace_prepare_for_request(
        &self,
        requester_node_id: &NodeId,
        request_id: &str,
    ) -> Result<Option<DfsNamespacePrepareRecord>> {
        let tables = self.current_tables_snapshot()?;
        let request_key = (requester_node_id.clone(), request_id.to_string());
        let Some(operation_ids) = tables.namespace_prepare_request_owners.get(&request_key) else {
            return Ok(None);
        };
        if operation_ids.len() != 1 {
            return Err(MetadataBackendError::Serde(format!(
                "pending namespace request {requester_node_id}/{request_id} owns {} prepared transactions",
                operation_ids.len()
            )));
        }
        let operation_id = operation_ids
            .first()
            .expect("non-empty request prepare owner index");
        let prepare = tables
            .namespace_prepares
            .get(operation_id)
            .cloned()
            .ok_or_else(|| {
                MetadataBackendError::Serde(format!(
                    "pending namespace request {requester_node_id}/{request_id} indexes missing prepare {operation_id}"
                ))
            })?;
        Ok(Some(prepare))
    }

    fn get_namespace_transaction(
        &self,
        transaction_id: &str,
    ) -> Result<Option<DfsNamespaceTransaction>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables.namespace_transactions.get(transaction_id).cloned())
    }

    fn get_namespace_job(&self, job_id: &str) -> Result<Option<DfsNamespaceJobRecord>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables.namespace_jobs.get(job_id).cloned())
    }

    fn put_namespace_job_receipt(&mut self, receipt: &DfsNamespaceJobReceipt) -> Result<()> {
        if receipt.status == DfsNamespaceJobStatus::Running {
            return Err(MetadataBackendError::Serde(format!(
                "namespace job receipt {} is not terminal",
                receipt.job_id
            )));
        }
        let _lock = self.begin_metadata_write()?;
        if let Some(job) = self.tables.namespace_jobs.get(&receipt.job_id)
            && !receipt.matches_terminal_job(job)
        {
            return Err(MetadataBackendError::Serde(format!(
                "namespace job receipt {} conflicts with its full job",
                receipt.job_id
            )));
        }
        if let Some(existing) = self.tables.namespace_job_receipts.get(&receipt.job_id)
            && existing != receipt
        {
            return Err(MetadataBackendError::Serde(format!(
                "namespace job receipt {} was reused with a different payload",
                receipt.job_id
            )));
        }
        if !self.tables.namespace_jobs.contains_key(&receipt.job_id)
            && !self
                .tables
                .namespace_job_receipts
                .contains_key(&receipt.job_id)
            && self
                .tables
                .namespace_job_history_expired(receipt.created_at, &receipt.job_id)
        {
            return Err(MetadataBackendError::Serde(format!(
                "namespace job {} idempotency history expired or is unknown",
                receipt.job_id
            )));
        }
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::put_namespace_job_receipt(receipt),
            vec![DfsMetadataTableOp::PutNamespaceJobReceipt {
                receipt: receipt.clone(),
            }],
        )
        .map(drop)
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
                        .is_none_or(|actual| actual == origin)
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

    fn put_conflict(&mut self, conflict: &DfsConflictRecord) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::PutConflict {
                file_system_id: conflict.file_system_id.clone(),
                object_id: conflict.object_id.clone(),
                conflict_id: conflict.conflict_id.clone(),
                conflict: Some(conflict.clone()),
            },
            vec![DfsMetadataTableOp::PutConflict {
                conflict: conflict.clone(),
            }],
        )
    }

    fn get_conflict(
        &self,
        object_id: &DfsObjectId,
        conflict_id: &str,
    ) -> Result<Option<DfsConflictRecord>> {
        let tables = self.current_tables_snapshot()?;
        Ok(tables
            .conflicts
            .get(&(object_id.clone(), conflict_id.to_string()))
            .cloned())
    }

    fn conflicts_for_object(&self, object_id: &DfsObjectId) -> Result<Vec<DfsConflictRecord>> {
        let tables = self.current_tables_snapshot()?;
        Ok(conflicts_for_object_from_tables(&tables, object_id))
    }

    fn conflicts_for_object_page(
        &self,
        object_id: &DfsObjectId,
        after: Option<&DfsConflictPageCursor>,
        max_entries: usize,
    ) -> Result<DfsConflictPage> {
        let tables = self.current_tables_snapshot()?;
        Ok(conflicts_for_object_page_from_tables(
            &tables,
            object_id,
            after,
            max_entries,
        ))
    }

    fn remove_conflict(
        &mut self,
        object_id: &DfsObjectId,
        conflict_id: &str,
    ) -> Result<Option<DfsConflictRecord>> {
        let Some(existing) = self.get_conflict(object_id, conflict_id)? else {
            return Ok(None);
        };
        let _lock = self.begin_metadata_write()?;
        self.write_journal_entry_and_apply_table_ops_unlocked(
            DfsMetadataMutation::PutConflict {
                file_system_id: existing.file_system_id.clone(),
                object_id: object_id.clone(),
                conflict_id: conflict_id.to_string(),
                conflict: None,
            },
            vec![DfsMetadataTableOp::RemoveConflict {
                object_id: object_id.clone(),
                conflict_id: conflict_id.to_string(),
            }],
        )?;
        Ok(Some(existing))
    }

    fn update_conflict_status(
        &mut self,
        object_id: &DfsObjectId,
        conflict_id: &str,
        status: DfsConflictStatus,
    ) -> Result<Option<DfsConflictRecord>> {
        let Some(mut conflict) = self.get_conflict(object_id, conflict_id)? else {
            return Ok(None);
        };
        conflict.status = status;
        self.put_conflict(&conflict)?;
        Ok(Some(conflict))
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
    ) -> Result<bool> {
        if shard_id != &self.shard_id || active_shard.shard_id != shard_id.0 {
            return Ok(false);
        }
        self.clear_membership_handoff_write_freeze(handoff_id, active_shard)
    }

    fn metadata_replication_cursor(
        &self,
        target_node_id: &NodeId,
        shard_id: &DfsMetadataShardId,
    ) -> Result<u64> {
        let sequence =
            metadata_cursor::metadata_replication_cursor(&self.root, target_node_id, shard_id)?;
        self.observe_resident_metadata_replication_cursor(target_node_id, shard_id, sequence);
        Ok(sequence)
    }

    fn put_metadata_replication_cursor(
        &mut self,
        target_node_id: &NodeId,
        shard_id: &DfsMetadataShardId,
        next_after_sequence: u64,
    ) -> Result<()> {
        // Replica acknowledgement cursors are independent durable sidecars:
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

    fn metadata_storage_migrations(
        &self,
    ) -> Result<Vec<sinomo_core::storage::migration::MigrationJob>> {
        Ok(self.storage_migration_status()?.into_iter().collect())
    }

    fn apply_journal_batch(&mut self, batch: &DfsMetadataJournalBatch) -> Result<usize> {
        LocalSobjMetadataBackend::apply_journal_batch(self, batch)
    }

    fn apply_journal_batch_with_replica_permit(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        batch: &DfsMetadataJournalBatch,
    ) -> Result<usize> {
        LocalSobjMetadataBackend::apply_journal_batch_with_replica_permit(self, permit, batch)
    }

    fn apply_journal_batch_with_replica_permit_report(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        batch: &DfsMetadataJournalBatch,
    ) -> Result<DfsMetadataReplicaApplyReport> {
        LocalSobjMetadataBackend::apply_journal_batch_with_replica_permit_report(
            self, permit, batch,
        )
    }

    fn persist_journal_batch_with_replica_permit_report(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        batch: &DfsMetadataJournalBatch,
    ) -> Result<DfsMetadataReplicaApplyReport> {
        LocalSobjMetadataBackend::persist_journal_batch_with_replica_permit_report(
            self, permit, batch,
        )
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
    ) -> Result<DfsMetadataReplicaApplyReport> {
        LocalSobjMetadataBackend::persist_authority_journal_frames_with_replica_permit_report(
            self,
            permit,
            batch,
            frames,
            expected_target_prefix,
        )
    }

    fn materialize_pending_replica_journal(&mut self) -> Result<usize> {
        LocalSobjMetadataBackend::materialize_pending_replica_journal(self)
    }

    fn replace_with_replicated_snapshot(
        &mut self,
        source_shard_id: DfsMetadataShardId,
        replayed_through_sequence: u64,
        snapshot: DfsMetadataTableSnapshot,
    ) -> Result<bool> {
        LocalSobjMetadataBackend::replace_with_replicated_snapshot(
            self,
            source_shard_id,
            replayed_through_sequence,
            snapshot,
        )
    }

    fn replace_with_replicated_snapshot_with_replica_permit(
        &mut self,
        permit: &DfsMetadataReplicaIngestPermit,
        source_shard_id: DfsMetadataShardId,
        replayed_through_sequence: u64,
        snapshot: DfsMetadataTableSnapshot,
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

impl LocalSobjMetadataBackend {
    /// Delayed workers cannot reopen or contradict a durable terminal outcome.
    /// In particular, republishing an exact prepare after a lost reply must
    /// retain Committed instead of downgrading the row to ReceiptsPending.
    fn preserved_terminal_intent(
        &self,
        proposed: &DfsWriteIntentRecord,
    ) -> Result<Option<DfsWriteIntentRecord>> {
        let Some(existing) = self.get_intent(&proposed.object_id, &proposed.intent_id)? else {
            return Ok(None);
        };
        if !matches!(
            existing.status,
            DfsWriteIntentStatus::Committed
                | DfsWriteIntentStatus::Aborted
                | DfsWriteIntentStatus::Rejected
                | DfsWriteIntentStatus::Conflict
        ) {
            return Ok(None);
        }
        if existing.file_system_id != proposed.file_system_id
            || existing.target_version != proposed.target_version
            || existing.base_version != proposed.base_version
            || existing.publication != proposed.publication
            || existing.operation_id != proposed.operation_id
            || (existing.content_hash.is_some()
                && proposed.content_hash.is_some()
                && existing.content_hash != proposed.content_hash)
        {
            return Err(MetadataBackendError::Serde(format!(
                "terminal write intent {} was reused with a different operation",
                proposed.intent_id
            )));
        }
        if existing.status == DfsWriteIntentStatus::Committed
            && matches!(
                proposed.status,
                DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
            )
        {
            return Ok(Some(existing));
        }
        if existing.status != proposed.status {
            return Err(MetadataBackendError::Serde(format!(
                "terminal write intent {} cannot change from {:?} to {:?}",
                proposed.intent_id, existing.status, proposed.status
            )));
        }
        Ok(None)
    }

    fn apply_namespace_transaction_inner(
        &mut self,
        transaction: &DfsNamespaceTransaction,
        operation_id: Option<&str>,
        expectations: Option<(
            &[DfsNamespaceObjectVersionExpectation],
            &[DfsNamespacePathOccupantExpectation],
        )>,
    ) -> Result<DfsMetadataJournalEntry> {
        let _lock = self.begin_metadata_write()?;
        if self.get_file_system(&transaction.file_system_id)?.is_none() {
            return Err(MetadataBackendError::UnknownFileSystem {
                file_system_id: transaction.file_system_id.clone(),
            });
        }
        if transaction
            .objects
            .iter()
            .any(|object| object.file_system_id != transaction.file_system_id)
        {
            return Err(MetadataBackendError::UnknownFileSystem {
                file_system_id: transaction.file_system_id.clone(),
            });
        }

        let transaction = normalized_namespace_transaction(transaction);
        if let Some(receipt) = self
            .tables
            .namespace_transaction_receipts
            .get(&transaction.transaction_id)
            .cloned()
        {
            if !receipt
                .matches_transaction(&transaction)
                .map_err(|source| MetadataBackendError::Serde(source.to_string()))?
            {
                return Err(MetadataBackendError::Serde(format!(
                    "namespace transaction {} reuses a compacted transaction id with a different payload",
                    transaction.transaction_id
                )));
            }
            return self.write_journal_entry_and_apply_table_ops_unlocked(
                DfsMetadataMutation::put_namespace_transaction_receipt(&receipt),
                vec![DfsMetadataTableOp::PutNamespaceTransactionReceipt { receipt }],
            );
        }
        if self
            .tables
            .namespace_transaction_history_expired(&transaction)
        {
            return Err(MetadataBackendError::Serde(format!(
                "namespace transaction {} idempotency history expired or is unknown",
                transaction.transaction_id
            )));
        }
        if let Some(existing) = self
            .tables
            .namespace_transactions
            .get(&transaction.transaction_id)
            && normalized_namespace_transaction(existing) != transaction
        {
            return Err(MetadataBackendError::Serde(format!(
                "namespace transaction {} reuses a transaction id with a different payload",
                transaction.transaction_id
            )));
        }
        if let Some((objects, paths)) = expectations {
            validate_namespace_read_set(
                &self.tables,
                &transaction.file_system_id,
                &transaction.transaction_id,
                objects,
                paths,
            )?;
        }
        validate_namespace_transaction_preconditions(&self.tables, &transaction)?;
        self.backfill_file_history_durability_unlocked(
            crate::file_history::legacy_capture_candidates(&self.tables, &transaction.objects),
        )?;
        let entry =
            self.write_journal_unlocked(DfsMetadataMutation::ApplyNamespaceTransaction {
                file_system_id: transaction.file_system_id.clone(),
                transaction_id: transaction.transaction_id.clone(),
                object_count: transaction.objects.len(),
                operation_id: operation_id.map(str::to_owned),
                transaction: Some(transaction),
            })?;
        let mut table_ops = Vec::new();
        let mut rollback = DfsMetadataTableRollback::default();
        let transaction = match &entry.mutation {
            DfsMetadataMutation::ApplyNamespaceTransaction {
                transaction: Some(transaction),
                ..
            } => transaction,
            _ => unreachable!("the namespace transaction journal entry preserves its payload"),
        };
        self.plan_namespace_transaction_table_ops(
            transaction,
            entry.created_at,
            &mut table_ops,
            &mut rollback,
        )?;
        self.append_preapplied_table_ops_for_written_entries(
            std::slice::from_ref(&entry),
            entry.sequence,
            table_ops,
            rollback,
        )?;
        Ok(entry)
    }
}
