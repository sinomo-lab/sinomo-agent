//! PreparedV7's native writer kernel. One immutable body and one digest marker
//! use the same materialization/replay projection under actual writer ownership.

use super::*;
use crate::metadata_records::{
    DfsPreparedOperationDigest, DfsPreparedOperationPayload, DfsPreparedOperationRecord,
};
use crate::publication_guard::{
    DfsPublicationGuard, DfsPublicationOwnerScope, DfsPublicationWriterSession,
};

/// Native held-view capture for the opt-in test-support exporter. This is
/// actual storage data; absent freeze/incarnation evidence is unavailable.
/// The exporter must losslessly encode u64 fields and certify the initial cut.
#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Debug, serde::Serialize)]
pub struct NativeHeldMetadataSnapshotV1 {
    pub backend_root: std::path::PathBuf,
    pub shard_id: DfsMetadataShardId,
    pub backend_instance_authority_epoch: Option<u64>,
    /// Normal getter input only. None means no epoch view was requested;
    /// neither this input nor writer_bound_epoch is publication authority.
    pub requested_writer_epoch_view: Option<u64>,
    /// Derived storage view: the requested epoch matched an actual instance.
    /// An admitted native guard separately supplies publication capability.
    pub writer_bound_epoch: Option<u64>,
    pub journal_next_sequence: u64,
    pub certified_prefix: Option<DfsMetadataPrefixProof>,
    pub materialized_through_sequence: u64,
    pub write_freeze:
        crate::native_shard_trace::ObservedNative<crate::metadata_api::DfsMetadataShardWriteFreeze>,
    pub tables: crate::metadata_records::DfsMetadataTableSnapshotTables,
}

impl LocalSobjMetadataBackend {
    pub(crate) fn held_prepared_operation_payload(
        &self,
        operation_id: &str,
    ) -> Option<Arc<DfsPreparedOperationPayload>> {
        self.tables
            .prepared_operations_v7
            .get(operation_id)
            .map(|record| record.payload.clone())
    }
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn native_prepared_kernel_held_snapshot_v1(
        &self,
        writer_bound_epoch: Option<u64>,
    ) -> NativeHeldMetadataSnapshotV1 {
        NativeHeldMetadataSnapshotV1 {
            backend_root: self.root.clone(),
            shard_id: self.shard_id.clone(),
            backend_instance_authority_epoch: self.instance_authority_epoch,
            // A caller's digest epoch does not bind a previously unbound
            // backend. Preserve only a matching actual native instance bind.
            requested_writer_epoch_view: writer_bound_epoch,
            writer_bound_epoch: writer_bound_epoch
                .filter(|epoch| self.instance_authority_epoch == Some(*epoch)),
            journal_next_sequence: self.next_sequence,
            certified_prefix: self.prefix_proof.clone(),
            materialized_through_sequence: self.materialized_through_sequence,
            // SAME operational read under the actual write gate; unknown
            // remains distinct from certified observed absence.
            write_freeze: self.native_observed_write_freeze.clone(),
            tables: crate::metadata_table_store::metadata_tables_snapshot(&self.tables),
        }
    }

    pub(super) fn prepare_operation_owned(
        &mut self,
        mut payload: DfsPreparedOperationPayload,
        source: Option<&crate::source_adoption::DfsSourceAdoptionWitness>,
    ) -> Result<DfsPreparedOperationDigest> {
        payload.normalize();
        payload.validate()?;
        if payload.shard_id != self.shard_id {
            return Err(MetadataBackendError::Serde(
                "prepared operation targets a foreign shard".into(),
            ));
        }
        #[cfg(any(test, feature = "test-support"))]
        let mut native_observation =
            crate::native_shard_trace::interested(&self.root, &self.shard_id);
        #[cfg(any(test, feature = "test-support"))]
        {
            self.native_observed_exact_frame = None;
        }
        let source_custody = if let Some(source) = source {
            let scope =
                self.publication_scope_for_backend(&payload.file_system_id, &payload.shard_id)?;
            source.target_writer_custody(&scope)?
        } else {
            None
        };
        let _lock = if let Some(custody) = &source_custody {
            let scope =
                self.publication_scope_for_backend(&payload.file_system_id, &payload.shard_id)?;
            custody.validate_scope(&scope)?;
            self.validate_retained_metadata_writer()?;
            custody.writer()
        } else {
            Arc::new(self.begin_metadata_write()?)
        };
        if self
            .instance_authority_epoch
            .is_some_and(|epoch| epoch != payload.owner_epoch)
        {
            return Err(MetadataBackendError::Serde(
                "preparation names a retired authority epoch".into(),
            ));
        }
        if self
            .tables
            .prepared_operation_evictions
            .contains_key(&payload.operation_id)
        {
            return Err(MetadataBackendError::Serde(
                "operation identity was terminally evicted; it cannot be authored again".into(),
            ));
        }
        if let Some(existing) = self
            .tables
            .prepared_operations_v7
            .get(&payload.operation_id)
        {
            existing.validate()?;
            let incoming_digest = payload.canonical_digest()?;
            if (existing.digest.payload_digest != incoming_digest
                || existing.payload.as_ref() != &payload)
                && !committed_capture_request_race_matches(existing, &payload)
            {
                let stored = existing.payload.as_ref();
                let completed_request_stable_equal = match (
                    stored.completed_request.as_ref(),
                    payload.completed_request.as_ref(),
                ) {
                    (Some(old), Some(new)) => {
                        old.file_system_id == new.file_system_id
                            && old.requester_node_id == new.requester_node_id
                            && old.request_id == new.request_id
                            && old.operation_id == new.operation_id
                            && old.authority_shard_id == new.authority_shard_id
                            && old.operation == new.operation
                            && old.command_digest == new.command_digest
                            && old.status == new.status
                            && old.result == new.result
                            && old.posix_coordination_result == new.posix_coordination_result
                            && old.recovery_attempts == new.recovery_attempts
                            && old.last_recovery_error == new.last_recovery_error
                            && old.created_at == new.created_at
                    }
                    (None, None) => true,
                    _ => false,
                };
                tracing::warn!(
                    target: "sinomo.dfs.metadata",
                    operation_id = %payload.operation_id,
                    file_system_id = %payload.file_system_id,
                    shard_id = %payload.shard_id.0,
                    stored_status = ?existing.status,
                    stored_prepare_sequence = existing.digest.prepare_sequence,
                    stored_commit_sequence = ?existing.committed_entry.as_ref().map(|entry| entry.sequence),
                    stored_digest = %existing.digest.payload_digest,
                    incoming_digest = %incoming_digest,
                    protocol_equal = stored.protocol_version == payload.protocol_version,
                    filesystem_equal = stored.file_system_id == payload.file_system_id,
                    shard_equal = stored.shard_id == payload.shard_id,
                    epoch_equal = stored.owner_epoch == payload.owner_epoch,
                    operation_equal = stored.operation_id == payload.operation_id,
                    journal_id_equal = stored.journal_operation_id == payload.journal_operation_id,
                    journal_sequence_equal = stored.expected_journal_sequence == payload.expected_journal_sequence,
                    object_expectations_equal = stored.expected_object_versions == payload.expected_object_versions,
                    path_expectations_equal = stored.expected_path_occupants == payload.expected_path_occupants,
                    transaction_equal = stored.transaction == payload.transaction,
                    transaction_versions_equal = stored.transaction.versions == payload.transaction.versions,
                    transaction_extension_expectations_equal = stored.transaction.extension_expectations == payload.transaction.extension_expectations,
                    transaction_pins_equal = stored.transaction.file_version_pins == payload.transaction.file_version_pins,
                    transaction_created_at_equal = stored.transaction.created_at == payload.transaction.created_at,
                    metadata_effects_equal = stored.metadata_effects == payload.metadata_effects,
                    content_receipts_equal = stored.content_receipts == payload.content_receipts,
                    required_replicas_equal = stored.required_replica_node_ids == payload.required_replica_node_ids,
                    prepared_intent_equal = stored.prepared_intent == payload.prepared_intent,
                    terminal_intent_equal = stored.terminal_intent == payload.terminal_intent,
                    completed_request_equal = stored.completed_request == payload.completed_request,
                    completed_request_stable_equal,
                    completed_request_updated_at_equal = stored.completed_request.as_ref().map(|request| request.updated_at)
                        == payload.completed_request.as_ref().map(|request| request.updated_at),
                    completed_request_completion_equal = stored.completed_request_completion == payload.completed_request_completion,
                    "prepared operation immutable body mismatch"
                );
                return Err(MetadataBackendError::Serde(
                    "prepared operation id reuses a different immutable body".into(),
                ));
            }
            if matches!(
                existing.status,
                DfsPreparedOperationStatus::Aborted | DfsPreparedOperationStatus::Superseded
            ) {
                return Err(MetadataBackendError::Serde(
                    "prepared operation is aborted".into(),
                ));
            }
            if let Some(source) = source {
                source.validate_for(&existing.payload, &existing.digest)?;
            }
            #[cfg(any(test, feature = "test-support"))]
            if let Some(observation) = &mut native_observation {
                observation.observe(
                    &existing.digest,
                    crate::native_shard_trace::KernelPhase::ExactPreparedReplay,
                    self.native_prepared_kernel_held_snapshot_v1(Some(existing.owner_epoch)),
                    self.native_prepared_kernel_held_snapshot_v1(Some(existing.owner_epoch)),
                    None,
                );
                observation.complete();
            }
            return Ok(existing.digest.clone());
        }
        if payload
            .expected_journal_sequence
            .is_some_and(|sequence| sequence != self.journal_last_sequence())
        {
            return Err(MetadataBackendError::Serde(
                "source family journal cut changed before preparation".into(),
            ));
        }

        let digest = DfsPreparedOperationDigest::at_sequence(&payload, self.next_sequence.max(1))?;
        let source_family = source
            .map(|source| source.validate_for(&payload, &digest))
            .transpose()?
            .flatten();
        let record = DfsPreparedOperationRecord {
            digest: digest.clone(),
            payload: Arc::new(payload),
            status: DfsPreparedOperationStatus::Prepared,
            committed_entry: None,
        };
        validate_metadata_effect_content_bindings(&self.tables, &record, source_family)?;
        self.validate_prepared_operation_body(&record)?;
        #[cfg(any(test, feature = "test-support"))]
        let native_before = native_observation
            .as_ref()
            .map(|_| self.native_prepared_kernel_held_snapshot_v1(Some(digest.owner_epoch)));
        let entry = self.write_journal_unlocked(DfsMetadataMutation::PrepareOperationV7 {
            file_system_id: digest.file_system_id.clone(),
            operation_id: digest.operation_id.clone(),
            record: Box::new(record.clone()),
        })?;
        if entry.sequence != digest.prepare_sequence {
            return Err(MetadataBackendError::Serde(
                "prepared operation journal slot changed under its writer".into(),
            ));
        }
        #[cfg(any(test, feature = "test-support"))]
        if let (Some(observation), Some(before)) = (&mut native_observation, &native_before) {
            observation.observe(
                &digest,
                crate::native_shard_trace::KernelPhase::PrepareDurable,
                before.clone(),
                self.native_prepared_kernel_held_snapshot_v1(Some(digest.owner_epoch)),
                self.native_observed_exact_frame.take(),
            );
        }
        self.append_and_apply_table_ops(
            entry.sequence,
            vec![DfsMetadataTableOp::PutPreparedOperationV7 {
                record: Box::new(record),
            }],
        )?;
        self.checkpoint_local_materialization_cursor_if_due(entry.sequence)?;
        #[cfg(any(test, feature = "test-support"))]
        if let (Some(observation), Some(before)) = (&mut native_observation, native_before) {
            observation.observe(
                &digest,
                crate::native_shard_trace::KernelPhase::PrepareMaterialized,
                before,
                self.native_prepared_kernel_held_snapshot_v1(Some(digest.owner_epoch)),
                None,
            );
            observation.complete();
        }
        Ok(digest)
    }

    /// Provisional hidden intent/receipt rows are undone before persistence.
    /// This reuses the existing exact namespace/W validators without cloning
    /// unrelated tables or making a visible effect before the prepare is durable.
    pub(super) fn validate_prepared_operation_body(
        &mut self,
        record: &DfsPreparedOperationRecord,
    ) -> Result<()> {
        validate_preparation_against_tables(&mut self.tables, &self.shard_id, record)
    }

    pub(super) fn current_prepared_operation_record(
        &self,
        operation_id: &str,
    ) -> Result<Option<DfsPreparedOperationRecord>> {
        let tables = self.current_tables_snapshot()?;
        tables
            .prepared_operations_v7
            .get(operation_id)
            .map(|record| {
                record.validate()?;
                Ok(record.clone())
            })
            .transpose()
    }

    pub(super) fn publication_scope_for_backend(
        &self,
        file_system_id: &DfsFileSystemId,
        shard_id: &DfsMetadataShardId,
    ) -> Result<DfsPublicationOwnerScope> {
        if shard_id != &self.shard_id {
            return Err(MetadataBackendError::Serde(
                "publication scope names a foreign physical shard".into(),
            ));
        }
        DfsPublicationOwnerScope::for_backend(&self.root, file_system_id.clone(), shard_id.clone())
    }

    pub(super) fn publication_writer_session_for_backend(
        &mut self,
        file_system_id: &DfsFileSystemId,
        shard_id: &DfsMetadataShardId,
        digest: &DfsPreparedOperationDigest,
        source: Option<&crate::DfsSourceAdoptionWitness>,
        provider: Option<&crate::publication_guard::DfsPublicationExecutionProvider>,
        execution: Option<&crate::publication_guard::DfsPublicationExecutionOwners>,
        clustered: Option<&crate::DfsMetadataShardConfig>,
    ) -> Result<DfsPublicationWriterSession> {
        if shard_id != &self.shard_id || digest.shard_id != self.shard_id {
            return Err(MetadataBackendError::Serde(
                "publication writer names a foreign physical backend shard".into(),
            ));
        }
        let scope = self.publication_scope_for_backend(file_system_id, shard_id)?;
        let custody = source
            .map(|source| source.target_writer_custody(&scope))
            .transpose()?
            .flatten();
        let lock = if let Some(custody) = &custody {
            custody.validate_scope(&scope)?;
            self.validate_retained_metadata_writer()?;
            custody.writer()
        } else {
            Arc::new(self.begin_metadata_write()?)
        };
        if let Some(expected) = clustered
            && self.instance_authority_epoch.is_none()
        {
            // The held writer validator already rejects every durable marker
            // for an unbound instance. No pending/activated fence is bypassed.
            let provider = provider.ok_or_else(|| {
                MetadataBackendError::Serde(
                    "unbound clustered writer requires the actual retained host provider".into(),
                )
            })?;
            let execution = execution.ok_or_else(|| {
                MetadataBackendError::Serde("initial epoch requires actual effect admission".into())
            })?;
            provider.validate_initial_epoch_binding(&scope, digest, execution, expected)?;
            self.instance_authority_epoch = Some(digest.owner_epoch);
        }
        let record = self
            .tables
            .prepared_operations_v7
            .get(&digest.operation_id)
            .ok_or_else(|| {
                MetadataBackendError::Serde("publication has no durable v7 prepare".into())
            })?;
        record.validate()?;
        if record.digest != *digest
            || &digest.file_system_id != file_system_id
            || &digest.shard_id != shard_id
            || record.status == DfsPreparedOperationStatus::Aborted
            || (clustered.is_some() && self.instance_authority_epoch != Some(digest.owner_epoch))
            || self
                .instance_authority_epoch
                .is_some_and(|epoch| epoch != digest.owner_epoch)
        {
            return Err(MetadataBackendError::Serde(
                "publication writer session digest/epoch/scope fence mismatch".into(),
            ));
        }
        Ok(DfsPublicationWriterSession::for_shared_backend(
            lock,
            scope,
            digest.clone(),
        ))
    }

    pub(super) fn commit_prepared_operation_owned(
        &mut self,
        digest: &DfsPreparedOperationDigest,
        guard: DfsPublicationGuard,
        enrollment: Option<&mut dyn FnMut(&[crate::DfsRepairAttemptRecord]) -> Result<()>>,
    ) -> Result<crate::metadata_records::DfsPreparedOperationCommitResult> {
        if digest.shard_id != self.shard_id {
            return Err(MetadataBackendError::Serde(
                "prepared commit names a foreign physical backend shard".into(),
            ));
        }
        let scope = self.publication_scope_for_backend(&digest.file_system_id, &digest.shard_id)?;
        // The consumed lease owns the ALREADY acquired actual metadata lock.
        // Do not reacquire begin_metadata_write or release it across recovery.
        #[cfg(any(test, feature = "test-support"))]
        let mut native_observation =
            crate::native_shard_trace::interested(&self.root, &self.shard_id);
        #[cfg(any(test, feature = "test-support"))]
        {
            self.native_observed_exact_frame = None;
        }
        let commit_owner = guard.consume_for(&scope, digest)?;
        let record = self
            .tables
            .prepared_operations_v7
            .get(&digest.operation_id)
            .cloned()
            .ok_or_else(|| {
                MetadataBackendError::Serde("commit has no immutable v7 preparation".into())
            })?;
        record.validate()?;
        if record.digest != *digest
            || matches!(
                record.status,
                DfsPreparedOperationStatus::Aborted | DfsPreparedOperationStatus::Superseded
            )
        {
            return Err(MetadataBackendError::Serde(
                "commit digest does not name the durable prepared body".into(),
            ));
        }
        if let Some(entry) = &record.committed_entry {
            if commit_owner
                .source_witness()
                .is_some_and(|source| source.supplies_source_family())
            {
                let prefix = self.prefix_proof.as_ref().ok_or_else(|| {
                    MetadataBackendError::Serde(
                        "native source terminal has no certified kernel prefix".into(),
                    )
                })?;
                commit_owner.record_source_terminal(
                    entry,
                    prefix,
                    metadata_table_storage_generation(&self.root)?,
                )?;
            }
            #[cfg(any(test, feature = "test-support"))]
            if let Some(observation) = &mut native_observation {
                observation.observe(
                    digest,
                    crate::native_shard_trace::KernelPhase::ExactCommittedReplay,
                    self.native_prepared_kernel_held_snapshot_v1(Some(digest.owner_epoch)),
                    self.native_prepared_kernel_held_snapshot_v1(Some(digest.owner_epoch)),
                    None,
                );
                observation.complete();
            }
            #[cfg(test)]
            if crate::native_boundary_diagnostics::selected_operation(
                &self.root,
                &digest.operation_id,
            ) {
                crate::native_boundary_diagnostics::record(
                    &self.root,
                    "kernel-committed-replay",
                    || {
                        serde_json::json!({
                            "digest": digest, "status": record.status, "committed_entry": record.committed_entry,
                            "instance_epoch": self.instance_authority_epoch, "guard_consumed": true,
                        })
                    },
                    true,
                );
            }
            return Ok(crate::metadata_records::DfsPreparedOperationCommitResult {
                entry: entry.journal_entry(),
                published: false,
            });
        }
        #[cfg(test)]
        crate::default_cluster::genesis::defer_native_genesis_commit_at_writer(
            &self.root,
            &record,
        )
        .map_err(|error| MetadataBackendError::Serde(error.to_string()))?;
        let source_family = commit_owner
            .source_witness()
            .map(|source| source.validate_for(&record.payload, digest))
            .transpose()?
            .flatten();
        validate_metadata_effect_content_bindings(&self.tables, &record, source_family)?;
        validate_prepared_operation_content(&self.tables, &record)?;
        if record.status == DfsPreparedOperationStatus::Prepared {
            let view = &record;
            namespace_prepare::validate_prepared_namespace_commit_state(&self.tables, &view)?;
            namespace_prepare::validate_namespace_prepare_expectations(&self.tables, &view)?;
            validate_namespace_transaction_preconditions(&self.tables, &view.transaction)?;
        }
        validate_metadata_effects(&self.tables, &record)?;
        if record.payload.expected_journal_sequence.is_some()
            && self.journal_last_sequence() != digest.prepare_sequence
        {
            return Err(MetadataBackendError::Serde(
                "source family journal cut changed after preparation".into(),
            ));
        }
        #[cfg(any(test, feature = "test-support"))]
        let native_before = native_observation
            .as_ref()
            .map(|_| self.native_prepared_kernel_held_snapshot_v1(Some(digest.owner_epoch)));
        let frame = crate::metadata_journal::DfsMetadataJournalFrame::local(
            &self.shard_id,
            self.next_sequence,
            DfsMetadataMutation::CommitPreparedOperationV7 {
                file_system_id: digest.file_system_id.clone(),
                operation_id: digest.operation_id.clone(),
                digest: digest.clone(),
            },
        )?;
        let commit_op = DfsMetadataTableOp::CommitPreparedOperationV7 {
            digest: digest.clone(),
            committed_entry: Box::new(
                crate::DfsPreparedOperationCommitReference::from_journal_entry(frame.entry())?,
            ),
        };
        let mut rollback = DfsMetadataTableRollback::default();
        let prepared = (|| -> Result<()> {
            self.tables
                .apply_checked_with_rollback(commit_op.clone(), &mut rollback)?;
            let owners = rollback.new_repair_owners(&self.tables);
            let records = new_repair_owner_targets(&self.tables, &owners, digest)?;
            if let Some(enroll) = enrollment {
                enroll(&records)?;
            }
            Ok(())
        })();
        if let Err(error) = prepared {
            rollback.rollback(&mut self.tables);
            return Err(error);
        }
        let mut rollback = Some(rollback);
        let (entry, _) = self.write_journal_frame_unlocked(&frame, &mut rollback)?;
        #[cfg(test)]
        if crate::native_boundary_diagnostics::selected_operation(&self.root, &digest.operation_id)
        {
            crate::native_boundary_diagnostics::record(
                &self.root,
                "kernel-commit-durable",
                || {
                    serde_json::json!({
                        "digest": digest, "entry": entry, "instance_epoch": self.instance_authority_epoch,
                    })
                },
                digest.operation_id == "v7-host-bind"
                    || digest.operation_id.starts_with("filesystem:"),
            );
        }
        #[cfg(any(test, feature = "test-support"))]
        if let (Some(observation), Some(before)) = (&mut native_observation, &native_before) {
            observation.observe(
                digest,
                crate::native_shard_trace::KernelPhase::CommitDurable,
                before.clone(),
                self.native_prepared_kernel_held_snapshot_v1(Some(digest.owner_epoch)),
                self.native_observed_exact_frame.take(),
            );
        }
        self.append_preapplied_table_ops(
            entry.sequence,
            vec![commit_op],
            rollback.expect("owned preapplied commit"),
        )?;
        self.checkpoint_local_materialization_cursor_if_due(entry.sequence)?;
        if commit_owner
            .source_witness()
            .is_some_and(|source| source.supplies_source_family())
        {
            let prefix = self.prefix_proof.as_ref().ok_or_else(|| {
                MetadataBackendError::Serde(
                    "native source terminal has no certified kernel prefix".into(),
                )
            })?;
            commit_owner.record_source_terminal(
                &crate::DfsPreparedOperationCommitReference::from_journal_entry(&entry)?,
                prefix,
                metadata_table_storage_generation(&self.root)?,
            )?;
        }
        #[cfg(any(test, feature = "test-support"))]
        if let (Some(observation), Some(before)) = (&mut native_observation, native_before) {
            observation.observe(
                digest,
                crate::native_shard_trace::KernelPhase::CommitMaterialized,
                before,
                self.native_prepared_kernel_held_snapshot_v1(Some(digest.owner_epoch)),
                None,
            );
            observation.complete();
        }
        Ok(crate::metadata_records::DfsPreparedOperationCommitResult {
            entry,
            published: true,
        })
    }
}

/// Only newly created retained or committed Current placement owners enter
/// this SAME preapplied commit/native writer boundary. Restored/existing owners
/// and ordinary prepared Current writes gain no birth observation here.
fn new_repair_owner_targets(
    tables: &DfsMetadataTables,
    owners: &[DfsWriteIntentRecord],
    digest: &DfsPreparedOperationDigest,
) -> Result<Vec<crate::DfsRepairAttemptRecord>> {
    let mut targets = Vec::new();
    for owner in owners {
        let version = tables
            .versions
            .get(&(owner.object_id.clone(), owner.target_version.clone()))
            .ok_or_else(|| {
                MetadataBackendError::Serde("new repair owner has no captured version".into())
            })?;
        if owner.publication.is_retained_repair() {
            let expected = crate::file_history::retained_repair_intent(version, None)?;
            if !crate::file_history::retained_repair_plan_matches(owner, &expected) {
                return Err(MetadataBackendError::Serde(
                    "new repair owner changed its recorded plan".into(),
                ));
            }
        } else {
            let object = tables.objects.get(&owner.object_id).ok_or_else(|| {
                MetadataBackendError::Serde(
                    "new placement owner has no actual current object".into(),
                )
            })?;
            if !crate::placement::current_placement_repair_owner_matches_identity(owner)
                || owner.file_system_id != digest.file_system_id
                || owner.owner_epoch != digest.owner_epoch
                || owner.base_version.as_ref() != Some(&owner.target_version)
                || object.file_system_id != owner.file_system_id
                || object.version != owner.target_version
                || object.content_hash != owner.content_hash
                || version.file_system_id != owner.file_system_id
                || version.object_id != owner.object_id
                || version.version != owner.target_version
                || version.content_hash != owner.content_hash
                || owner.object_metadata.as_ref().is_none_or(|anchor| {
                    anchor.file_system_id != owner.file_system_id
                        || anchor.object_id != owner.object_id
                        || anchor.version != owner.target_version
                        || anchor.content_hash != owner.content_hash
                })
            {
                return Err(MetadataBackendError::Serde(
                    "new Current placement birth changed its actual committed scope/version".into(),
                ));
            }
        }
        require_recorded_content_w(tables, version, None)?;
        // Recorded source receipts prove W above. Only receipts for this
        // owner's exact target generation settle its new repair custody.
        let holders = owner
            .desired_replica_node_ids
            .iter()
            .filter_map(|node| {
                let receipt = tables.receipts.get(&(
                    owner.object_id.clone(),
                    owner.target_version.clone(),
                    node.clone(),
                ))?;
                crate::content_durability::receipt_matches_target_version(receipt, version, node)
                    .then(|| receipt.replica_node_id.clone())
            })
            .collect::<BTreeSet<_>>();
        let now = u64::try_from(OffsetDateTime::now_utc().unix_timestamp_nanos()).unwrap_or(0);
        for target in owner
            .desired_replica_node_ids
            .iter()
            .filter(|target| !holders.contains(*target))
        {
            targets.push(crate::DfsRepairAttemptRecord {
                intent_id: owner.intent_id.clone(),
                object_id: owner.object_id.clone(),
                version: owner.target_version.clone(),
                content_hash: owner.content_hash.clone().expect("validated repair hash"),
                missing_replica_node_id: target.clone(),
                attempts: 0,
                in_flight: false,
                last_attempt_unix_nanos: 0,
                next_retry_unix_nanos: now,
                transfer_request_id: None,
                last_error: None,
                transfer_delivery_state: Some(
                    sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent,
                ),
                completed_transfer_request_id: None,
                transfer_kind: Some(sinomo_api::dfs::maintenance::DfsRepairTransferKind::Repair),
            });
        }
    }
    Ok(targets)
}

pub(crate) fn validate_preparation_against_tables(
    tables: &mut DfsMetadataTables,
    shard_id: &DfsMetadataShardId,
    record: &DfsPreparedOperationRecord,
) -> Result<()> {
    record.validate()?;
    let view = record;
    let mut rollback = DfsMetadataTableRollback::default();
    let shadowed = (|| -> Result<()> {
        for effect in &record.metadata_effects {
            if let crate::DfsPreparedMetadataEffect::ResolvePreparedOperation { digest, status } =
                effect
            {
                let old = tables
                    .prepared_operations_v7
                    .get(&digest.operation_id)
                    .cloned()
                    .ok_or_else(|| {
                        MetadataBackendError::Serde(
                            "replacement has no exact retiring preparation".into(),
                        )
                    })?;
                if old.digest != *digest
                    || old.status != DfsPreparedOperationStatus::Prepared
                    || record.operation_id == old.operation_id
                    || record.file_system_id != old.file_system_id
                    || record.shard_id != old.shard_id
                {
                    return Err(MetadataBackendError::Serde(
                        "replacement scope/identity changed".into(),
                    ));
                }
                if *status == DfsPreparedOperationStatus::Superseded {
                    let mut unchanged = record.payload.as_ref().clone();
                    unchanged.operation_id = old.operation_id.clone();
                    unchanged.owner_epoch = old.owner_epoch;
                    unchanged.metadata_effects.retain(|effect|!matches!(effect,crate::DfsPreparedMetadataEffect::ResolvePreparedOperation {digest:target,..} if target==digest));
                    if let Some(intent) = &mut unchanged.prepared_intent {
                        intent.owner_epoch = old.owner_epoch;
                    }
                    if let Some(intent) = &mut unchanged.terminal_intent {
                        intent.owner_epoch = old.owner_epoch;
                    }
                    if unchanged != *old.payload {
                        return Err(MetadataBackendError::Serde(
                            "superseding operation changed original effects/read-set/request"
                                .into(),
                        ));
                    }
                }
                tables.apply_checked_with_rollback(
                    DfsMetadataTableOp::ResolvePreparedOperation {
                        digest: digest.clone(),
                        status: *status,
                    },
                    &mut rollback,
                )?;
            }
        }

        Ok(())
    })();
    if let Err(error) = shadowed {
        rollback.rollback(tables);
        return Err(error);
    }
    for effect in &record.metadata_effects {
        if let crate::DfsPreparedMetadataEffect::PutFileSystem { file_system } = effect {
            tables.apply_with_rollback(
                DfsMetadataTableOp::PutFileSystem {
                    file_system: file_system.clone(),
                },
                &mut rollback,
            );
        }
    }
    if let Some(intent) = &record.payload.prepared_intent {
        if tables
            .intents
            .get(&(intent.object_id.clone(), intent.intent_id.clone()))
            .is_some_and(|existing| {
                let mut expected = existing.clone();
                expected.owner_epoch = intent.owner_epoch;
                expected != *intent
            })
            || tables
                .intent_receipts
                .contains_key(&(intent.object_id.clone(), intent.intent_id.clone()))
        {
            rollback.rollback(tables);
            return Err(MetadataBackendError::Serde(
                "preparation changes intent identity or reuses terminal outcome".into(),
            ));
        }
        tables.apply_with_rollback(
            DfsMetadataTableOp::PutIntent {
                intent: intent.clone(),
            },
            &mut rollback,
        );
    }
    for receipt in &record.payload.content_receipts {
        tables.apply_with_rollback(
            DfsMetadataTableOp::PutReceipt {
                receipt: receipt.clone(),
            },
            &mut rollback,
        );
    }
    let validated = (|| {
        let shape = namespace_prepare::validate_prepared_operation_shape(tables, shard_id, &view)?;
        namespace_prepare::validate_namespace_prepare_expectations(tables, &view)?;
        namespace_prepare::validate_namespace_prepare_does_not_overlap(
            tables,
            &view,
            &shape.footprint,
        )?;
        validate_namespace_transaction_preconditions(tables, &view.transaction)?;
        validate_prepared_operation_content(tables, record)?;
        validate_metadata_effects(tables, record)
    })();
    rollback.rollback(tables);
    validated
}

pub(crate) fn validate_prepared_operation_commit(
    tables: &DfsMetadataTables,
    record: &DfsPreparedOperationRecord,
) -> Result<()> {
    validate_prepared_operation_content(tables, record)?;
    let view = record;
    namespace_prepare::validate_prepared_namespace_commit_state(tables, &view)?;
    namespace_prepare::validate_namespace_prepare_expectations(tables, &view)?;
    validate_namespace_transaction_preconditions(tables, &view.transaction)?;
    validate_metadata_effects(tables, record)
}

pub(crate) fn validate_prepared_operation_content(
    tables: &DfsMetadataTables,
    record: &DfsPreparedOperationRecord,
) -> Result<()> {
    sealed_local_eventual_batch_versions(tables, record, true)?;
    let payload = &record.payload;
    if let (Some(prepared), Some(terminal)) = (&payload.prepared_intent, &payload.terminal_intent) {
        let object = terminal.object_metadata.as_ref().ok_or_else(|| {
            MetadataBackendError::Serde("sealed terminal intent has no exact object".into())
        })?;
        let mut expected = prepared.clone();
        expected.status = DfsWriteIntentStatus::Committed;
        expected.updated_at = terminal.updated_at;
        expected.object_metadata = Some(object.clone());
        if expected != *terminal
            || object.object_id != terminal.object_id
            || object.file_system_id != terminal.file_system_id
            || object.version != terminal.target_version
            || object.content_hash != terminal.content_hash
        {
            return Err(MetadataBackendError::Serde(
                "sealed terminal intent changed its immutable operation identity".into(),
            ));
        }
        let sealed_version = payload
            .transaction
            .versions
            .iter()
            .find(|version| {
                version.object_id == terminal.object_id
                    && version.version == terminal.target_version
                    && version.file_system_id == terminal.file_system_id
                    && version.content_hash == terminal.content_hash
                    && version.size_bytes == object.content_size_bytes
            })
            .ok_or_else(|| {
                MetadataBackendError::Serde(
                    "complete content publication has no exact sealed version".into(),
                )
            })?;
        match &terminal.publication {
            crate::DfsWritePublication::Current => {
                if !payload.expected_object_versions.iter().any(|expectation| {
                    expectation.object_id == prepared.object_id
                        && expectation.expected_version == prepared.base_version
                }) || !payload
                    .transaction
                    .objects
                    .iter()
                    .any(|effect| effect == object)
                    || !sealed_version.visible
                {
                    return Err(MetadataBackendError::Serde(
                        "current content publication does not seal its original head and visible effects".into(),
                    ));
                }
            }
            crate::DfsWritePublication::Retained { capture } => {
                let source_head = tables.objects.get(&prepared.object_id).ok_or_else(|| {
                    MetadataBackendError::Serde(
                        "retained content has no current object anchor".into(),
                    )
                })?;
                if !payload.transaction.objects.is_empty()
                    || sealed_version.visible
                    || !payload.expected_object_versions.iter().any(|expectation| {
                        expectation.object_id == source_head.object_id
                            && expectation.expected_version.as_ref() == Some(&source_head.version)
                    })
                    || sealed_version
                        .retention_grant(&capture.namespace)
                        .is_none_or(|grant| grant.object.as_ref() != object)
                    || sealed_version.module_metadata.get(&capture.namespace)
                        != capture.metadata.as_ref()
                    || !payload
                        .transaction
                        .file_version_pins
                        .as_ref()
                        .is_some_and(|pins| {
                            pins.remove.is_empty()
                                && pins.add.len() == 1
                                && pins.add.iter().any(|pin| {
                                    pin.file_system_id == sealed_version.file_system_id
                                        && pin.object_id == sealed_version.object_id
                                        && pin.version == sealed_version.version
                                        && crate::lifecycle::content_reference_record_id(pin)
                                            == crate::lifecycle::content_reference_storage_id(
                                                &capture.namespace,
                                                &capture.reference_id,
                                            )
                                })
                        })
                {
                    return Err(MetadataBackendError::Serde(
                        "retained content publication changed its source, grant, or reference"
                            .into(),
                    ));
                }
            }
            crate::DfsWritePublication::RetainedRepair => {
                return Err(MetadataBackendError::Serde(
                    "repair ownership cannot publish new retained bytes".into(),
                ));
            }
        }
        let mut receipt_nodes = BTreeSet::new();
        for node in &payload.required_replica_node_ids {
            let receipt = tables
                .receipts
                .get(&(
                    terminal.object_id.clone(),
                    terminal.target_version.clone(),
                    node.clone(),
                ))
                .ok_or_else(|| {
                    MetadataBackendError::Serde(
                        "prepared operation lacks exact required durable content receipt".into(),
                    )
                })?;
            if receipt.receipt.file_system_id != terminal.file_system_id
                || receipt.receipt.object_id != terminal.object_id
                || receipt.receipt.committed_version != terminal.target_version
                || receipt.content_hash != terminal.content_hash
                || !terminal.desired_replica_node_ids.contains(node)
            {
                return Err(MetadataBackendError::Serde(
                    "prepared operation content receipt tuple mismatch".into(),
                ));
            }
            receipt_nodes.insert(node);
        }
        if receipt_nodes.len() < terminal.policy.min_commit_replicas {
            return Err(MetadataBackendError::Serde(
                "prepared operation lacks its content W threshold".into(),
            ));
        }
    } else if !payload.required_replica_node_ids.is_empty() {
        return Err(MetadataBackendError::Serde(
            "required content receipt set has no sealed intent".into(),
        ));
    }
    Ok(())
}

pub(crate) fn apply_metadata_effect_with_rollback(
    tables: &mut DfsMetadataTables,
    effect: &crate::DfsPreparedMetadataEffect,
    rollback: &mut DfsMetadataTableRollback,
) -> Result<()> {
    if let crate::DfsPreparedMetadataEffect::PutObject { object } = effect {
        let previous = tables.objects.get(&object.object_id);
        let ops = crate::metadata_object_index::object_index_table_ops_with_previous(
            tables, object, previous,
        );
        tables.apply_with_rollback(
            DfsMetadataTableOp::PutObject {
                object: object.clone(),
            },
            rollback,
        );
        for op in ops {
            tables.apply_checked_with_rollback(op, rollback)?;
        }
    } else {
        tables.apply_checked_with_rollback(effect.clone().into_table_op(), rollback)?;
    }
    Ok(())
}
fn require_recorded_content_w(
    tables: &DfsMetadataTables,
    version: &DfsObjectVersionRecord,
    source: Option<&crate::DfsCrossShardSourceFamily>,
) -> Result<()> {
    let plan = version.content_durability.as_ref().ok_or_else(|| {
        MetadataBackendError::Serde(
            "carried content has no recorded data-generation binding".into(),
        )
    })?;
    crate::validate_replication_plan(&plan.desired_replica_node_ids, &plan.policy)
        .map_err(|error| MetadataBackendError::Serde(error.to_string()))?;
    if version.content_hash.is_none() || version.size_bytes.is_none() {
        return Err(MetadataBackendError::Serde(
            "carried content has no complete byte identity".into(),
        ));
    }
    let acknowledged = plan
        .desired_replica_node_ids
        .iter()
        .filter(|node| {
            let receipt = if let Some(source) = source {
                source.receipts.iter().find(|receipt| {
                    receipt.receipt.object_id == version.object_id
                        && receipt.receipt.committed_version == plan.source_version
                        && receipt.replica_node_id == **node
                })
            } else {
                tables.receipts.get(&(
                    version.object_id.clone(),
                    plan.source_version.clone(),
                    (*node).clone(),
                ))
            };
            receipt.is_some_and(|receipt| {
                receipt.replica_node_id == **node
                    && receipt.receipt.file_system_id == version.file_system_id
                    && receipt.receipt.object_id == version.object_id
                    && receipt.receipt.committed_version == plan.source_version
                    && receipt.content_hash == version.content_hash
            })
        })
        .count();
    if acknowledged < plan.policy.min_commit_replicas {
        return Err(MetadataBackendError::Serde(
            "carried content lacks its recorded same-object data-generation W receipts".into(),
        ));
    }
    Ok(())
}
// The existing local-eventual batch seals one actual caller receipt per file.
// It is a fresh data write, not a same-byte successor of an earlier head.
fn sealed_local_eventual_batch_versions(
    tables: &DfsMetadataTables,
    record: &DfsPreparedOperationRecord,
    require_staged_receipts: bool,
) -> Result<BTreeSet<(DfsObjectId, DfsVersion)>> {
    let mut sealed = BTreeSet::new();
    if record.terminal_intent.is_some()
        || record.content_receipts.is_empty()
        || record.expected_journal_sequence.is_some()
        || !record.transaction.versions.iter().any(|version| {
            version
                .content_durability
                .as_ref()
                .is_some_and(|plan| plan.source_version == version.version)
                && record.content_receipts.iter().any(|receipt| {
                    receipt.receipt.object_id == version.object_id
                        && receipt.receipt.committed_version == version.version
                })
        })
    {
        return Ok(sealed);
    }
    let reject = || {
        MetadataBackendError::Serde(
            "local-eventual batch does not seal its exact file/version/receipt/base binding".into(),
        )
    };
    if record.transaction.file_version_pins.is_some()
        || record.transaction.contribution_digest.is_some()
        || !record.transaction.extension_expectations.is_empty()
        || !record.transaction.advisory_lock_states.is_empty()
        || record
            .transaction
            .objects
            .iter()
            .any(|object| !matches!(object.kind, DfsObjectKind::File | DfsObjectKind::Directory))
    {
        return Err(reject());
    }
    let receipts = record
        .content_receipts
        .iter()
        .map(|receipt| (&receipt.receipt.object_id, receipt))
        .collect::<std::collections::BTreeMap<_, _>>();
    let versions = record
        .transaction
        .versions
        .iter()
        .map(|version| (&version.object_id, version))
        .collect::<std::collections::BTreeMap<_, _>>();
    if receipts.len() != record.content_receipts.len()
        || versions.len() != record.transaction.versions.len()
        || versions.len() != record.transaction.objects.len()
        || receipts.len()
            != record
                .transaction
                .objects
                .iter()
                .filter(|object| object.kind == DfsObjectKind::File)
                .count()
    {
        return Err(reject());
    }
    for object in record
        .transaction
        .objects
        .iter()
        .filter(|object| object.kind == DfsObjectKind::File)
    {
        let receipt = receipts.get(&object.object_id).ok_or_else(reject)?;
        let version = versions.get(&object.object_id).ok_or_else(reject)?;
        let current = tables.objects.get(&object.object_id);
        let expected = record
            .expected_object_versions
            .iter()
            .find(|expected| expected.object_id == object.object_id)
            .ok_or_else(reject)?;
        if object.file_system_id != record.file_system_id
            || object.policy != sinomo_core::DfsObjectPolicy::ReplicatedEventual
            || object.owner_node_id != receipt.replica_node_id
            || object.current_locations.as_slice() != std::slice::from_ref(&receipt.replica_node_id)
            || object.content_hash.is_none()
            || object.content_size_bytes.is_none()
            || object.last_modified_by_event_id.as_ref() != Some(&receipt.receipt.event_id)
            || receipt.receipt.file_system_id != record.file_system_id
            || receipt.receipt.committed_version != object.version
            || receipt.content_hash != object.content_hash
            || !version.visible
            || version.file_system_id != object.file_system_id
            || version.version != object.version
            || version.content_hash != object.content_hash
            || version.size_bytes != object.content_size_bytes
            || version.locations != object.current_locations
            || version.created_by_event_id != object.last_modified_by_event_id
            || !version.retention.is_empty()
            || !version.module_metadata.is_empty()
            || version.content_durability.as_ref()
                != Some(&crate::content_durability::from_local_receipt(receipt))
            || expected.expected_version.as_ref() != current.map(|current| &current.version)
            || !record
                .expected_path_occupants
                .iter()
                .any(|path| path.path == crate::metadata_keys::normalize_path(&object.path))
            || match tables.receipts.get(&(
                object.object_id.clone(),
                object.version.clone(),
                receipt.replica_node_id.clone(),
            )) {
                Some(staged) => staged != *receipt,
                None => require_staged_receipts,
            }
        {
            return Err(reject());
        }
        if let Some(current) = current.filter(|current| current.kind != DfsObjectKind::Tombstone) {
            if current.kind != DfsObjectKind::File || current.version == object.version {
                return Err(reject());
            }
            let mut allowed = current.clone();
            allowed.version = object.version.clone();
            allowed.content_hash = object.content_hash.clone();
            allowed.content_size_bytes = object.content_size_bytes;
            allowed.current_locations = object.current_locations.clone();
            allowed.modified_at_unix_nanos = object.modified_at_unix_nanos;
            allowed.changed_at_unix_nanos = object.changed_at_unix_nanos;
            allowed.last_modified_by_event_id = object.last_modified_by_event_id.clone();
            if allowed != *object {
                return Err(reject());
            }
        }
        sealed.insert((object.object_id.clone(), object.version.clone()));
    }
    Ok(sealed)
}

fn source_for_object<'a>(
    source: Option<&'a crate::DfsCrossShardSourceFamily>,
    fs: &DfsFileSystemId,
    object: &DfsObjectId,
) -> Option<&'a crate::DfsCrossShardSourceFamily> {
    source.filter(|source| {
        source.source_object.file_system_id == *fs && source.source_object.object_id == *object
    })
}
fn recorded_head<'a>(
    tables: &'a DfsMetadataTables,
    source: Option<&'a crate::DfsCrossShardSourceFamily>,
    object: &DfsObjectId,
) -> Option<(&'a DfsObjectMetadata, &'a DfsObjectVersionRecord)> {
    if let Some(source) = source {
        let head = &source.source_object;
        source
            .versions
            .iter()
            .find(|version| version.object_id == *object && version.version == head.version)
            .map(|version| (head, version))
    } else {
        let head = tables.objects.get(object)?;
        tables
            .versions
            .get(&(object.clone(), head.version.clone()))
            .map(|version| (head, version))
    }
}
fn validate_carried_visible_version(
    tables: &DfsMetadataTables,
    record: &DfsPreparedOperationRecord,
    version: &DfsObjectVersionRecord,
    source: Option<&crate::DfsCrossShardSourceFamily>,
    local_batch: &BTreeSet<(DfsObjectId, DfsVersion)>,
) -> Result<()> {
    if local_batch.contains(&(version.object_id.clone(), version.version.clone()))
        && record
            .transaction
            .versions
            .iter()
            .any(|sealed| sealed == version)
    {
        return Ok(());
    }
    if !version.visible || version.content_hash.is_none() {
        return Ok(());
    }
    if record.terminal_intent.as_ref().is_some_and(|terminal| {
        terminal.object_id == version.object_id
            && terminal.file_system_id == version.file_system_id
            && terminal.target_version == version.version
            && terminal.content_hash == version.content_hash
            && terminal
                .object_metadata
                .as_ref()
                .is_some_and(|object| object.content_size_bytes == version.size_bytes)
            && record
                .transaction
                .versions
                .iter()
                .any(|captured| captured == version)
    }) {
        return Ok(());
    }
    let source = source_for_object(source, &record.file_system_id, &version.object_id);
    // Extension and placement changes may update a recorded historical row.
    // Its immutable byte/data-generation identity is evidence for that exact
    // revision; it does not authorize a new revision from another object's hash.
    let existing = if let Some(source) = source {
        source
            .versions
            .iter()
            .find(|old| old.object_id == version.object_id && old.version == version.version)
    } else {
        tables
            .versions
            .get(&(version.object_id.clone(), version.version.clone()))
    };
    if let Some(old) = existing {
        if old.file_system_id != version.file_system_id
            || !old.visible
            || old.content_hash != version.content_hash
            || old.size_bytes != version.size_bytes
            || old.content_durability.is_none()
            || old.content_durability != version.content_durability
        {
            return Err(MetadataBackendError::Serde(
                "recorded visible revision changes its immutable byte/data-generation binding"
                    .into(),
            ));
        }
        return require_recorded_content_w(tables, old, source);
    }
    let (current, old) = recorded_head(tables, source, &version.object_id).ok_or_else(|| {
        MetadataBackendError::Serde(
            "visible carried version has no actual same-object head/version".into(),
        )
    })?;
    if current.file_system_id != record.file_system_id
        || version.file_system_id != record.file_system_id
        || !old.visible
        || old.file_system_id != record.file_system_id
        || old.object_id != version.object_id
        || old.content_hash != version.content_hash
        || old.size_bytes != version.size_bytes
        || current.content_hash != version.content_hash
        || current.content_size_bytes != version.size_bytes
        || old.content_durability.is_none()
        || old.content_durability != version.content_durability
        || (source.is_none()
            && !record.expected_object_versions.iter().any(|expected| {
                expected.object_id == current.object_id
                    && expected.expected_version.as_ref() == Some(&current.version)
            }))
    {
        return Err(MetadataBackendError::Serde(
            "visible carried version changes its original object/head/data-generation binding"
                .into(),
        ));
    }
    require_recorded_content_w(tables, old, source)
}
fn validate_committed_intent_content(
    tables: &DfsMetadataTables,
    record: &DfsPreparedOperationRecord,
    intent: &DfsWriteIntentRecord,
    source: Option<&crate::DfsCrossShardSourceFamily>,
) -> Result<()> {
    if intent.status != DfsWriteIntentStatus::Committed {
        return Ok(());
    }
    let source = source_for_object(source, &intent.file_system_id, &intent.object_id);
    let key = (intent.object_id.clone(), intent.intent_id.clone());
    let previous = if let Some(source) = source {
        source
            .intents
            .iter()
            .find(|old| old.object_id == intent.object_id && old.intent_id == intent.intent_id)
            .cloned()
            .or_else(|| {
                source
                    .intent_receipts
                    .iter()
                    .find(|old| {
                        old.object_id == intent.object_id && old.intent_id == intent.intent_id
                    })
                    .map(|old| old.to_intent_record())
            })
    } else {
        tables.intents.get(&key).cloned().or_else(|| {
            tables
                .intent_receipts
                .get(&key)
                .map(|receipt| receipt.to_intent_record())
        })
    };
    if let Some(mut previous) =
        previous.filter(|previous| previous.status == DfsWriteIntentStatus::Committed)
    {
        if intent.object_metadata.is_none() {
            previous.object_metadata = None;
        }
        if previous == *intent {
            return Ok(());
        }
        return Err(MetadataBackendError::Serde(
            "recorded committed intent changes its exact terminal outcome".into(),
        ));
    }
    // A carried terminal keeps its original shard's epoch. Only the exact
    // recorded outcome above (local or from the validated source witness) can
    // retain an epoch newer than this independent destination shard's epoch.
    // Content identity alone cannot mint such an outcome.
    if intent.owner_epoch > record.owner_epoch {
        return Err(MetadataBackendError::Serde(
            "intent effect claims a future authority epoch".into(),
        ));
    }
    if record.terminal_intent.as_ref().is_some_and(|terminal| {
        let mut captured = terminal.clone();
        if intent.object_metadata.is_none() {
            captured.object_metadata = None;
        }
        captured == *intent
    }) {
        return Ok(());
    }
    let anchored = recorded_head(tables, source, &intent.object_id)
        .filter(|(object, version)| {
            object.file_system_id == intent.file_system_id
                && version.visible
                && object.content_hash == intent.content_hash
                && version.content_hash == intent.content_hash
                && object.content_size_bytes == version.size_bytes
                && intent
                    .object_metadata
                    .as_ref()
                    .is_none_or(|captured| captured == *object)
        })
        .map(|(_, version)| version)
        .filter(|version| {
            version
                .content_durability
                .as_ref()
                .is_some_and(|plan| plan.source_version == intent.target_version)
        });
    let retained = if intent.publication.is_retained_repair() {
        let version = if let Some(source) = source {
            source.versions.iter().find(|version| {
                version.object_id == intent.object_id && version.version == intent.target_version
            })
        } else {
            tables
                .versions
                .get(&(intent.object_id.clone(), intent.target_version.clone()))
        };
        version.filter(|version| {
            // The repair owns the captured metadata revision. Its byte
            // receipts still belong to the recorded original data generation;
            // metadata-only successors deliberately have different versions.
            version.file_system_id == intent.file_system_id
                && version.object_id == intent.object_id
                && version.version == intent.target_version
                && version.content_hash == intent.content_hash
                && crate::file_history::retained_repair_intent(version, None)
                    .is_ok_and(|owner| owner == *intent)
        })
    } else {
        None
    };
    let version = anchored.or(retained).ok_or_else(|| {
        MetadataBackendError::Serde(
            "committed intent effect has neither a sealed terminal nor actual recorded content/repair evidence".into(),
        )
    })?;
    require_recorded_content_w(tables, version, source)
}
fn validate_content_object_binding(
    tables: &DfsMetadataTables,
    record: &DfsPreparedOperationRecord,
    object: &DfsObjectMetadata,
    source: Option<&crate::DfsCrossShardSourceFamily>,
    local_batch: &BTreeSet<(DfsObjectId, DfsVersion)>,
) -> Result<()> {
    use crate::DfsPreparedMetadataEffect as E;
    if local_batch.contains(&(object.object_id.clone(), object.version.clone()))
        && record
            .transaction
            .objects
            .iter()
            .any(|sealed| sealed == object)
    {
        return Ok(());
    }
    if !matches!(
        object.kind,
        DfsObjectKind::File | DfsObjectKind::Artifact | DfsObjectKind::Symlink
    ) || object.content_hash.is_none()
    {
        return Ok(());
    }
    if record
        .terminal_intent
        .as_ref()
        .is_some_and(|terminal| terminal.object_metadata.as_ref() == Some(object))
    {
        return Ok(());
    }
    let source = source_for_object(source, &object.file_system_id, &object.object_id);
    let (current, old) = recorded_head(tables, source, &object.object_id).ok_or_else(|| {
        MetadataBackendError::Serde(
            "object effect has no sealed or recorded same-object head".into(),
        )
    })?;
    if current.file_system_id != object.file_system_id
        || current.content_hash != object.content_hash
        || current.content_size_bytes != object.content_size_bytes
    {
        return Err(MetadataBackendError::Serde(
            "object effect cannot bypass sealed content durability".into(),
        ));
    }
    let version = if object.version == current.version {
        Some(old)
    } else {
        record
            .transaction
            .versions
            .iter()
            .find(|version| {
                version.file_system_id == object.file_system_id
                    && version.object_id == object.object_id
                    && version.version == object.version
            })
            .or_else(|| {
                record
                    .metadata_effects
                    .iter()
                    .find_map(|effect| match effect {
                        E::PutVersion { version }
                            if version.file_system_id == object.file_system_id
                                && version.object_id == object.object_id
                                && version.version == object.version =>
                        {
                            Some(version)
                        }
                        _ => None,
                    })
            })
    }
    .ok_or_else(|| {
        MetadataBackendError::Serde(
            "same-byte successor lacks matching complete version/data binding".into(),
        )
    })?;
    if !version.visible
        || version.content_hash != object.content_hash
        || version.size_bytes != object.content_size_bytes
    {
        return Err(MetadataBackendError::Serde(
            "same-byte successor has a mismatched visible version".into(),
        ));
    }
    validate_carried_visible_version(tables, record, version, source, local_batch)?;
    Ok(())
}
pub(super) fn validate_metadata_effect_content_bindings(
    tables: &DfsMetadataTables,
    record: &DfsPreparedOperationRecord,
    source: Option<&crate::DfsCrossShardSourceFamily>,
) -> Result<()> {
    use crate::DfsPreparedMetadataEffect as E;
    let local_batch = sealed_local_eventual_batch_versions(tables, record, false)?;
    for version in &record.transaction.versions {
        validate_carried_visible_version(tables, record, version, source, &local_batch)?;
    }
    for object in &record.transaction.objects {
        validate_content_object_binding(tables, record, object, source, &local_batch)?;
    }
    for effect in &record.metadata_effects {
        match effect {
            E::PutVersion { version } => {
                validate_carried_visible_version(tables, record, version, source, &local_batch)?
            }
            E::ReconcileCrossShardPinTransfer { versions, .. } => {
                for version in versions {
                    validate_carried_visible_version(
                        tables,
                        record,
                        version,
                        source,
                        &local_batch,
                    )?;
                }
            }
            E::PutObject { object } => {
                validate_content_object_binding(tables, record, object, source, &local_batch)?;
            }
            E::PutIntent { intent } => {
                validate_committed_intent_content(tables, record, intent, source)?
            }
            E::PutIntentReceipt { receipt } => validate_committed_intent_content(
                tables,
                record,
                &receipt.to_intent_record(),
                source,
            )?,
            _ => {}
        }
    }
    Ok(())
}

fn validate_metadata_effects(
    tables: &DfsMetadataTables,
    record: &DfsPreparedOperationRecord,
) -> Result<()> {
    use crate::DfsPreparedMetadataEffect as E;
    let mut intent_identities = std::collections::BTreeMap::new();
    for effect in &record.metadata_effects {
        let scope = match effect {
            E::PutFileSystem { file_system } => Some(&file_system.file_system_id),
            E::PutObject { object } => Some(&object.file_system_id),
            E::PutVersion { version } => Some(&version.file_system_id),
            E::PutVersionPin { pin } => Some(&pin.file_system_id),
            E::PutReceipt { receipt } => Some(&receipt.receipt.file_system_id),
            E::PutIntent { intent } => Some(&intent.file_system_id),
            E::PutIntentReceipt { receipt } => Some(&receipt.file_system_id),
            E::PutAdvisoryLockState { state } => Some(&state.file_system_id),
            E::PutNamespaceJob { job } => Some(&job.file_system_id),
            E::PutNamespaceJobReceipt { receipt } => Some(&receipt.file_system_id),
            E::PutNamespaceRequest { request } => Some(&request.file_system_id),
            E::PutNamespaceRequestReceipt { receipt } => Some(&receipt.file_system_id),
            E::PutNamespaceTransactionReceipt { receipt } => Some(&receipt.file_system_id),
            E::FinalizeCrossShardSourceEviction { tombstone, .. } => {
                Some(&tombstone.file_system_id)
            }
            E::ResolvePreparedOperation { digest, .. } => Some(&digest.file_system_id),
            _ => None,
        };
        if scope.is_some_and(|fs| fs != &record.file_system_id) {
            return Err(MetadataBackendError::Serde(
                "sealed metadata effects cross filesystem scope".into(),
            ));
        }
        for object_id in effect.affected_object_ids() {
            if tables
                .objects
                .get(&object_id)
                .is_some_and(|object| object.file_system_id != record.file_system_id)
            {
                return Err(MetadataBackendError::Serde(
                    "metadata effect targets a foreign existing object".into(),
                ));
            }
        }
        match effect {
            E::PutIntent { intent } => {
                validate_intent_effect(tables, record, intent, &mut intent_identities)?;
            }
            E::PutIntentReceipt { receipt } => {
                if receipt.publication.is_retained_repair() {
                    return Err(MetadataBackendError::Serde(
                        "repair ownership is not a user operation receipt".into(),
                    ));
                }
                validate_intent_effect(
                    tables,
                    record,
                    &receipt.to_intent_record(),
                    &mut intent_identities,
                )?;
            }
            E::PutNamespaceJob { job } => {
                crate::service_restore_jobs::validate_content_copy_job(job)?;
                if let Some(previous) = tables.namespace_jobs.get(&job.job_id) {
                    crate::service_restore_jobs::validate_content_copy_job_transition(
                        previous, job,
                    )?;
                }
                if tables
                    .namespace_job_receipts
                    .get(&job.job_id)
                    .is_some_and(|receipt| !receipt.matches_terminal_job(job))
                {
                    return Err(MetadataBackendError::Serde(
                        "namespace job changed its retained terminal outcome".into(),
                    ));
                }
            }
            E::PutNamespaceJobReceipt { receipt } => {
                crate::service_restore_jobs::validate_content_copy_job_receipt(receipt)?;
            }
            E::MergeContentReferenceOutcomeRetention { retention, .. } => {
                retention
                    .validate_file_system_scope(&record.file_system_id)
                    .map_err(MetadataBackendError::Serde)?;
            }
            E::PutVersionPin { pin } => {
                super::pin_validation::validate_version_pin_payload(
                    &record.file_system_id,
                    &pin.object_id,
                    &pin.version,
                    &pin.pin_id,
                    pin,
                )?;
                if !tables.version_pins.contains_key(&(
                    pin.object_id.clone(),
                    pin.version.clone(),
                    pin.pin_id.clone(),
                )) {
                    super::pin_validation::require_version_pin_capacity_for_new(
                        tables,
                        &pin.object_id,
                    )?;
                }
            }
            E::ReconcileCrossShardPinTransfer { versions, transfer } => {
                super::pin_validation::validate_cross_shard_pin_transfer_table_transition(
                    tables,
                    &record.file_system_id,
                    &transfer.object_id,
                    &transfer.transfer_id,
                    false,
                    versions,
                    transfer,
                )?;
            }
            E::FinalizeCrossShardSourceEviction {
                tombstone,
                removed_pins,
                removed_pin_transfers,
            } => {
                super::pin_validation::validate_cross_shard_source_eviction_table_transition(
                    tables,
                    &record.file_system_id,
                    &tombstone.object_id,
                    tombstone,
                    removed_pins,
                    removed_pin_transfers,
                )?;
            }
            E::PutObject { object } => {
                if !record
                    .expected_object_versions
                    .iter()
                    .any(|expected| expected.object_id == object.object_id)
                    || !record.expected_path_occupants.iter().any(|expected| {
                        normalize_path(&expected.path) == normalize_path(&object.path)
                    })
                {
                    return Err(MetadataBackendError::Serde(
                        "object metadata effect lacks original head/path observations".into(),
                    ));
                }
                crate::file_history::validate_file_history_transition(tables, object)?;
            }
            E::PutVersion { version } => {
                version
                    .validate_extension_metadata()
                    .map_err(MetadataBackendError::Serde)?;
            }
            E::RemoveVersion { object_id, version } => {
                if tables
                    .versions
                    .get(&(object_id.clone(), version.clone()))
                    .is_some_and(|row| row.file_system_id != record.file_system_id)
                    || crate::metadata_history_gc::version_is_protected_from_history_gc(
                        tables,
                        object_id,
                        version,
                        &record.operation_id,
                    )
                {
                    return Err(MetadataBackendError::Serde(
                        "version collection targets foreign or currently protected metadata".into(),
                    ));
                }
            }
            E::RemoveReceipt {
                object_id,
                version,
                replica_node_id,
            } => {
                let location_withdrawal = record.expected_journal_sequence.is_some()
                    && record.expected_object_versions.iter().any(|read| {
                        read.object_id == *object_id
                            && read.expected_version.as_ref() == Some(version)
                    })
                    && tables.objects.get(object_id).is_some_and(|current| {
                        current.version == *version
                            && record.metadata_effects.iter().any(|effect| {
                                if let E::PutObject { object } = effect {
                                    let mut expected = current.clone();
                                    expected
                                        .current_locations
                                        .retain(|node| node != replica_node_id);
                                    object == &expected
                                        && !object.current_locations.contains(replica_node_id)
                                } else {
                                    false
                                }
                            })
                    })
                    && tables
                        .versions
                        .get(&(object_id.clone(), version.clone()))
                        .is_some_and(|current| {
                            record.metadata_effects.iter().any(|effect| {
                                if let E::PutVersion { version: changed } = effect {
                                    let mut expected = current.clone();
                                    expected.locations.retain(|node| node != replica_node_id);
                                    changed == &expected
                                        && !changed.locations.contains(replica_node_id)
                                } else {
                                    false
                                }
                            })
                        });
                if tables
                    .receipts
                    .get(&(object_id.clone(), version.clone(), replica_node_id.clone()))
                    .is_some_and(|row| row.receipt.file_system_id != record.file_system_id)
                    || (!location_withdrawal
                        && crate::metadata_history_gc::version_is_protected_from_history_gc(
                            tables,
                            object_id,
                            version,
                            &record.operation_id,
                        ))
                {
                    return Err(MetadataBackendError::Serde(
                        "receipt collection targets foreign or currently protected metadata".into(),
                    ));
                }
            }
            E::PutReceipt { receipt } => {
                if receipt.receipt.file_system_id != record.file_system_id {
                    return Err(MetadataBackendError::Serde(
                        "receipt effect scope mismatch".into(),
                    ));
                }
            }
            E::RemovePreparedOperation { digest } => {
                let target = tables
                    .prepared_operations_v7
                    .get(&digest.operation_id)
                    .ok_or_else(|| MetadataBackendError::Serde("eviction target missing".into()))?;
                if target.digest != *digest
                    || target.status == DfsPreparedOperationStatus::Prepared
                    || digest.file_system_id != record.file_system_id
                    || digest.shard_id != record.shard_id
                {
                    return Err(MetadataBackendError::Serde(
                        "eviction target changed/scope mismatch".into(),
                    ));
                }
            }
            E::ResolvePreparedOperation { digest, status } => {
                let old = tables
                    .prepared_operations_v7
                    .get(&digest.operation_id)
                    .ok_or_else(|| {
                        MetadataBackendError::Serde(
                            "resolution names no retained current operation".into(),
                        )
                    })?;
                if old.digest != *digest
                    || !(old.status == DfsPreparedOperationStatus::Prepared
                        || old.status == *status)
                    || !matches!(
                        status,
                        DfsPreparedOperationStatus::Aborted
                            | DfsPreparedOperationStatus::Superseded
                    )
                    || digest.shard_id != record.shard_id
                {
                    return Err(MetadataBackendError::Serde(
                        "prepared resolution digest/scope/phase mismatch".into(),
                    ));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_intent_effect(
    tables: &DfsMetadataTables,
    record: &DfsPreparedOperationRecord,
    intent: &crate::DfsWriteIntentRecord,
    seen: &mut std::collections::BTreeMap<
        (crate::DfsObjectId, String),
        crate::DfsWriteIntentRecord,
    >,
) -> Result<()> {
    crate::file_history::validate_intent_publication(intent)?;
    // Committed outcomes are checked against their original recorded source
    // by validate_committed_intent_content at both author prepare and commit.
    // Their historical epoch is not destination publication authority.
    if intent.status != DfsWriteIntentStatus::Committed && intent.owner_epoch > record.owner_epoch {
        return Err(MetadataBackendError::Serde(
            "intent effect claims a future authority epoch".into(),
        ));
    }
    let key = (intent.object_id.clone(), intent.intent_id.clone());
    let compacted;
    let previous = if let Some(previous) = seen.get(&key) {
        Some(previous)
    } else if let Some(previous) = tables.intents.get(&key) {
        Some(previous)
    } else if let Some(receipt) = tables.intent_receipts.get(&key) {
        compacted = receipt.to_intent_record();
        Some(&compacted)
    } else {
        None
    };
    if let Some(previous) = previous {
        let mut identity = previous.clone();
        identity.status = intent.status.clone();
        identity.updated_at = intent.updated_at;
        identity.object_metadata = intent.object_metadata.clone();
        identity.owner_epoch = intent.owner_epoch;
        if identity != *intent
            || intent.owner_epoch < previous.owner_epoch
            || (!matches!(
                previous.status,
                DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
            ) && intent.status != previous.status)
        {
            let previous_identity = serde_json::to_value(&identity)
                .map_err(|error| MetadataBackendError::Serde(error.to_string()))?;
            let incoming_identity = serde_json::to_value(intent)
                .map_err(|error| MetadataBackendError::Serde(error.to_string()))?;
            let changed_fields = previous_identity
                .as_object()
                .expect("intent identity is an object")
                .keys()
                .chain(
                    incoming_identity
                        .as_object()
                        .expect("intent identity is an object")
                        .keys(),
                )
                .cloned()
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .filter(|field| {
                    previous_identity.get(field.as_str()) != incoming_identity.get(field.as_str())
                })
                .collect::<Vec<_>>();
            return Err(MetadataBackendError::Serde(format!(
                "intent effect changed its immutable identity or reopened a terminal outcome: id={} previous_status={:?} incoming_status={:?} changed_fields={changed_fields:?}",
                intent.intent_id, previous.status, intent.status,
            )));
        }
    }
    seen.insert(key, intent.clone());
    Ok(())
}

/// A foreground planner can reach the actual writer after ordinary Main
/// recovery completed its SAME request. Return only that stored terminal body;
/// the newly generated decision clocks never become another commit or digest.
fn committed_capture_request_race_matches(
    existing: &DfsPreparedOperationRecord,
    incoming: &DfsPreparedOperationPayload,
) -> bool {
    if existing.status != crate::DfsPreparedOperationStatus::Committed
        || existing.committed_entry.is_none()
    {
        return false;
    }
    let stored = existing.payload.as_ref();
    let (Some(original), Some(proposed)) = (&stored.completed_request, &incoming.completed_request)
    else {
        return false;
    };
    let crate::DfsNamespaceOperation::ContentReference { request } = &original.operation else {
        return false;
    };
    if !matches!(
        request.operation,
        crate::DfsContentReferenceOperation::CaptureCurrent { .. }
    ) {
        return false;
    }
    // Every actor, stable request, typed command, status/result, authority and
    // original admission time must match. Only owner decision time may differ.
    let mut decision = proposed.clone();
    decision.updated_at = original.updated_at;
    if &decision != original
        || stored.protocol_version != incoming.protocol_version
        || stored.file_system_id != incoming.file_system_id
        || stored.shard_id != incoming.shard_id
        || stored.owner_epoch != incoming.owner_epoch
        || stored.operation_id != incoming.operation_id
        || stored.journal_operation_id != incoming.journal_operation_id
        || stored.expected_journal_sequence != incoming.expected_journal_sequence
        || stored.expected_object_versions != incoming.expected_object_versions
        || stored.expected_path_occupants != incoming.expected_path_occupants
        || stored.content_receipts != incoming.content_receipts
        || stored.required_replica_node_ids != incoming.required_replica_node_ids
        || stored.metadata_effects != incoming.metadata_effects
        || stored.prepared_intent != incoming.prepared_intent
        || stored.terminal_intent != incoming.terminal_intent
        || stored.completed_request_completion != incoming.completed_request_completion
    {
        return false;
    }
    // Compare typed effects with only the two CaptureCurrent decision clocks
    // taken from the STORED body. Hashes, lengths, module payloads, grants,
    // minima, expectations and all other attributes remain exact comparisons.
    // This comparison clone is never hashed, stored, published or returned.
    let mut candidate = incoming.transaction.clone();
    if candidate.versions.len() != stored.transaction.versions.len() {
        return false;
    }
    for (new, old) in candidate
        .versions
        .iter_mut()
        .zip(&stored.transaction.versions)
    {
        if new.file_system_id == request.identity.file_system_id
            && new.object_id == request.identity.object_id
            && new.version == request.identity.version
            && old.file_system_id == new.file_system_id
            && old.object_id == new.object_id
            && old.version == new.version
            && let (Some(new), Some(old)) = (
                new.retention
                    .get_mut(&request.namespace)
                    .and_then(|state| state.grant.as_mut()),
                old.retention
                    .get(&request.namespace)
                    .and_then(|state| state.grant.as_ref()),
            )
        {
            new.granted_at_unix_nanos = old.granted_at_unix_nanos;
        }
    }
    if let (Some(new), Some(old)) = (
        &mut candidate.file_version_pins,
        &stored.transaction.file_version_pins,
    ) {
        if new.add.len() != old.add.len() {
            return false;
        }
        for (new, old) in new.add.iter_mut().zip(&old.add) {
            if new.namespace == request.namespace
                && new.file_system_id == request.identity.file_system_id
                && new.object_id == request.identity.object_id
                && new.version == request.identity.version
                && old.namespace == new.namespace
                && old.file_system_id == new.file_system_id
                && old.object_id == new.object_id
                && old.version == new.version
                && old.pin_id == new.pin_id
            {
                new.created_at = old.created_at;
            }
        }
    }
    candidate == stored.transaction
}

#[cfg(test)]
mod committed_capture_race_tests {
    use super::*;

    fn actual_committed_capture() -> DfsPreparedOperationRecord {
        // DATA copied from the real r242 Main maintenance winner: prepare 7,
        // commit 8. It is never installed as metadata or authority.
        let data: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/committed-capture-r242.json")).unwrap();
        let prepared: crate::DfsMetadataJournalEntry =
            serde_json::from_value(data["prepare"].clone()).unwrap();
        let committed: crate::DfsMetadataJournalEntry =
            serde_json::from_value(data["commit"].clone()).unwrap();
        let crate::DfsMetadataMutation::PrepareOperationV7 { mut record, .. } = prepared.mutation
        else {
            panic!("actual captured preparation");
        };
        record.status = DfsPreparedOperationStatus::Committed;
        record.committed_entry = Some(Box::new(
            crate::DfsPreparedOperationCommitReference::from_journal_entry(&committed).unwrap(),
        ));
        record.validate().unwrap();
        *record
    }

    fn regenerated_capture(record: &DfsPreparedOperationRecord) -> DfsPreparedOperationPayload {
        let mut incoming = record.payload.as_ref().clone();
        incoming.completed_request.as_mut().unwrap().updated_at +=
            time::Duration::milliseconds(115);
        let grant = incoming.transaction.versions[0]
            .retention
            .get_mut("module.dfs.history")
            .unwrap()
            .grant
            .as_mut()
            .unwrap();
        grant.granted_at_unix_nanos += 115_000_000;
        incoming.transaction.file_version_pins.as_mut().unwrap().add[0].created_at +=
            time::Duration::milliseconds(115);
        incoming
    }

    #[test]
    fn committed_capture_race_preserves_actual_stored_body_and_digest() {
        let actual = actual_committed_capture();
        let body = actual.payload.clone();
        let digest = actual.digest.clone();
        let commit = actual.committed_entry.clone();
        let incoming = regenerated_capture(&actual);
        assert_ne!(incoming.canonical_digest().unwrap(), digest.payload_digest);
        assert!(committed_capture_request_race_matches(&actual, &incoming));
        assert_eq!(actual.payload, body);
        assert_eq!(actual.digest, digest);
        assert_eq!(actual.committed_entry, commit);
        actual.validate().unwrap();
    }

    #[test]
    fn committed_capture_race_rejects_changed_semantics_actor_readset_and_pin() {
        let actual = actual_committed_capture();
        let incoming = regenerated_capture(&actual);
        let value = serde_json::to_value(&incoming).unwrap();
        let wrong_actor = serde_json::json!("00000000000000000000000001");
        let mut cases: Vec<(&str, serde_json::Value)> = Vec::new();
        for (name, pointer, replacement) in [
            ("actor", "/completed_request/requester_node_id", wrong_actor),
            (
                "request-id",
                "/completed_request/request_id",
                serde_json::json!("different-request"),
            ),
            (
                "command-digest",
                "/completed_request/command_digest",
                serde_json::json!("blake3:different"),
            ),
            (
                "namespace",
                "/completed_request/operation/content-reference/request/namespace",
                serde_json::json!("different.namespace"),
            ),
            (
                "typed-command",
                "/completed_request/operation/content-reference/request/operation/reference_id",
                serde_json::json!("different-reference"),
            ),
            (
                "module-bytes",
                "/transaction/versions/0/module_metadata/module.dfs.history/payload/0",
                serde_json::json!(0),
            ),
            (
                "content-hash",
                "/transaction/versions/0/content_hash",
                serde_json::json!(
                    "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                ),
            ),
            (
                "content-length",
                "/transaction/versions/0/size_bytes",
                serde_json::json!(9),
            ),
            (
                "expected-object",
                "/expected_object_versions/0/expected_version",
                serde_json::json!("00000000000000000000000001"),
            ),
            (
                "expected-path",
                "/expected_path_occupants/0/path",
                serde_json::json!("services/different"),
            ),
            (
                "extension-readset",
                "/transaction/extension_expectations/0/expected/size_bytes",
                serde_json::json!(9),
            ),
            (
                "contribution-digest",
                "/transaction/contribution_digest",
                serde_json::json!("different-contribution"),
            ),
            (
                "pin-id",
                "/transaction/file_version_pins/add/0/pin_id",
                serde_json::json!("different-pin"),
            ),
            (
                "grant-object",
                "/transaction/versions/0/retention/module.dfs.history/grant/object/mode",
                serde_json::json!(0),
            ),
            (
                "created-at",
                "/transaction/created_at",
                serde_json::json!("2026-10-09T03:24:48Z"),
            ),
            ("authority-epoch", "/owner_epoch", serde_json::json!(2)),
        ] {
            let mut changed = value.clone();
            *changed.pointer_mut(pointer).expect(name) = replacement.clone();
            // Keep a changed immutable hash/length internally well typed so
            // these controls reach the stored-body comparison, not the grant
            // identity decoder's independent rejection.
            if name == "content-hash" {
                changed["transaction"]["versions"][0]["retention"]["module.dfs.history"]["grant"]
                    ["object"]["content_hash"] = replacement;
            } else if name == "content-length" {
                changed["transaction"]["versions"][0]["retention"]["module.dfs.history"]["grant"]
                    ["object"]["content_size_bytes"] = replacement;
            }
            cases.push((name, changed));
        }
        let mut extra_pin = value.clone();
        let mut pin = extra_pin["transaction"]["file_version_pins"]["add"][0].clone();
        pin["pin_id"] = serde_json::json!("different-second-pin");
        extra_pin["transaction"]["file_version_pins"]["add"]
            .as_array_mut()
            .unwrap()
            .push(pin);
        cases.push(("additional-pin", extra_pin));
        for (name, changed) in cases {
            let decoded = serde_json::from_value::<DfsPreparedOperationPayload>(changed);
            if name == "module-bytes" {
                // The altered opening JSON byte is rejected by the actual
                // typed module-payload decoder before comparison.
                assert!(decoded.is_err(), "{name} must fail typed decoding");
                continue;
            }
            let proposed = decoded.unwrap_or_else(|error| panic!("{name}: {error}"));
            assert!(
                !committed_capture_request_race_matches(&actual, &proposed),
                "{name}"
            );
        }
        let mut changed_module = incoming.clone();
        changed_module.transaction.versions[0]
            .module_metadata
            .get_mut("module.dfs.history")
            .unwrap()
            .payload =
            serde_json::json!({"kind": "different", "operation_id": "different-operation"});
        assert!(!committed_capture_request_race_matches(
            &actual,
            &changed_module
        ));
        let mut changed_receipts = incoming.clone();
        changed_receipts
            .required_replica_node_ids
            .push(sinomo_core::node::NodeId::from_legacy_named("different-replica").unwrap());
        assert!(!committed_capture_request_race_matches(
            &actual,
            &changed_receipts
        ));
        actual.validate().unwrap();
    }

    #[test]
    fn capture_clock_difference_does_not_replay_prepared_or_missing_terminal_entry() {
        let actual = actual_committed_capture();
        let incoming = regenerated_capture(&actual);
        for status in [
            DfsPreparedOperationStatus::Prepared,
            DfsPreparedOperationStatus::Aborted,
            DfsPreparedOperationStatus::Superseded,
        ] {
            let mut changed = actual.clone();
            changed.status = status;
            assert!(!committed_capture_request_race_matches(&changed, &incoming));
        }
        let mut missing = actual.clone();
        missing.committed_entry = None;
        assert!(!committed_capture_request_race_matches(&missing, &incoming));
        assert_ne!(incoming, *actual.payload);
    }
}
