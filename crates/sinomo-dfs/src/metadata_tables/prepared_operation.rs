//! Sole current effect projector, shared by durable commit and replica recovery.
use super::*;
use crate::metadata_records::{
    DfsPreparedOperationCommitReference, DfsPreparedOperationDigest, DfsPreparedOperationRecord,
    DfsPreparedOperationStatus,
};
impl DfsMetadataTables {
    pub(super) fn apply_prepared_operation_v7_with_rollback(
        &mut self,
        record: DfsPreparedOperationRecord,
        rollback: &mut DfsMetadataTableRollback,
    ) -> Result<()> {
        record.validate()?;
        if self
            .prepared_operation_evictions
            .contains_key(&record.operation_id)
        {
            return Err(MetadataBackendError::Serde(
                "prepared identity has already been terminally evicted".into(),
            ));
        }
        if let Some(previous) = self.prepared_operations_v7.get(&record.operation_id) {
            if previous.digest != record.digest || previous.payload != record.payload {
                return Err(MetadataBackendError::Serde(
                    "current operation id reuses another immutable body".into(),
                ));
            }
            return Ok(());
        }
        if record.status != DfsPreparedOperationStatus::Prepared {
            return Err(MetadataBackendError::Serde(
                "journal prepare must be nonterminal".into(),
            ));
        }
        crate::metadata_backend::prepared_operation::validate_preparation_against_tables(
            self,
            &record.shard_id,
            &record,
        )?;
        rollback.record(
            self,
            &DfsMetadataTableOp::PutPreparedOperationV7 {
                record: Box::new(record.clone()),
            },
        );
        if let Some(intent) = &record.prepared_intent {
            self.apply_with_rollback(
                DfsMetadataTableOp::PutIntent {
                    intent: intent.clone(),
                },
                rollback,
            );
        }
        for receipt in &record.content_receipts {
            self.apply_with_rollback(
                DfsMetadataTableOp::PutReceipt {
                    receipt: receipt.clone(),
                },
                rollback,
            );
        }
        if let Some(completion) = &record.completed_request {
            let key = (
                completion.requester_node_id.clone(),
                completion.request_id.clone(),
            );
            if !self.namespace_requests.contains_key(&key) {
                let mut pending = completion.clone();
                pending.status = DfsNamespaceRequestStatus::Pending;
                pending.result = None;
                pending.posix_coordination_result = None;
                pending.recovery_attempts = 0;
                pending.last_recovery_error = None;
                pending.updated_at = pending.created_at;
                self.apply_with_rollback(
                    DfsMetadataTableOp::put_namespace_request(pending),
                    rollback,
                );
            }
        }
        self.put_prepared_operation_indexed(record);
        Ok(())
    }
    pub(super) fn validate_prepared_operation_v7_commit(
        &self,
        digest: &DfsPreparedOperationDigest,
        entry: &DfsPreparedOperationCommitReference,
    ) -> Result<()> {
        if let Some(witness) = self.prepared_operation_evictions.get(&digest.operation_id) {
            witness.validate()?;
            if witness.digest == *digest && witness.committed_entry.as_deref() == Some(entry) {
                return Ok(());
            }
            return Err(MetadataBackendError::Serde(
                "commit changes an evicted terminal identity".into(),
            ));
        }
        let record = self
            .prepared_operations_v7
            .get(&digest.operation_id)
            .ok_or_else(|| {
                MetadataBackendError::Serde("commit has no durable complete preparation".into())
            })?;
        record.validate()?;
        entry.validate(digest)?;
        if record.digest != *digest
            || matches!(
                record.status,
                DfsPreparedOperationStatus::Aborted | DfsPreparedOperationStatus::Superseded
            )
        {
            return Err(MetadataBackendError::Serde(
                "current commit dependency/scope/terminal state mismatch".into(),
            ));
        }
        if record
            .committed_entry
            .as_ref()
            .is_some_and(|previous| previous.as_ref() != entry)
        {
            return Err(MetadataBackendError::Serde(
                "exact current commit replay changes its retained journal slot".into(),
            ));
        }
        Ok(())
    }
    pub(super) fn commit_prepared_operation_v7_with_rollback(
        &mut self,
        digest: &DfsPreparedOperationDigest,
        entry: &DfsPreparedOperationCommitReference,
        rollback: &mut DfsMetadataTableRollback,
    ) -> Result<()> {
        self.validate_prepared_operation_v7_commit(digest, entry)?;
        if self
            .prepared_operation_evictions
            .contains_key(&digest.operation_id)
        {
            return Ok(());
        }
        let mut record = self
            .prepared_operations_v7
            .get(&digest.operation_id)
            .expect("checked current record")
            .clone();
        if record.committed_entry.is_some() {
            return Ok(());
        }
        crate::metadata_backend::prepared_operation::validate_prepared_operation_commit(
            self, &record,
        )?;
        rollback.record(
            self,
            &DfsMetadataTableOp::CommitPreparedOperationV7 {
                digest: digest.clone(),
                committed_entry: Box::new(entry.clone()),
            },
        );
        for effect in &record.metadata_effects {
            crate::metadata_backend::prepared_operation::apply_metadata_effect_with_rollback(
                self, effect, rollback,
            )?;
        }
        if !record.transaction.objects.is_empty()
            || !record.transaction.versions.is_empty()
            || !record.transaction.advisory_lock_states.is_empty()
            || record.transaction.file_version_pins.is_some()
            || record.transaction.content_reference_identity.is_some()
        {
            // The commit already owns exclusive mutable tables and a rollback
            // for the complete replay batch. Use the same ordered planner once
            // instead of copying unrelated history and applying its result again.
            crate::metadata_namespace_transaction::preapply_namespace_transaction_table_ops_at(
                self,
                &record.transaction,
                entry.created_at,
                None,
                rollback,
            );
        }
        if let Some(terminal) = &record.terminal_intent {
            // The same commit publishes the complete terminal write witness.
            // It still owns unfinished N repair and a running content-copy
            // job's result recovery. History GC alone compacts that owner once
            // those durable lifetimes are complete; a retry receipt cannot
            // execute its remaining work.
            self.apply_with_rollback(
                DfsMetadataTableOp::PutIntent {
                    intent: terminal.clone(),
                },
                rollback,
            );
        }
        let request_op = if let Some(request) = &record.completed_request {
            self.missing_namespace_request_completion_op(std::borrow::Cow::Borrowed(request))
        } else {
            record
                .completed_request_completion
                .as_ref()
                .and_then(|completion| {
                    self.missing_compact_namespace_request_completion_op(completion)
                })
        };
        if let Some(op) = request_op {
            self.apply_with_rollback(op, rollback);
        }
        record.status = DfsPreparedOperationStatus::Committed;
        record.committed_entry = Some(Box::new(entry.clone()));
        record.validate()?;
        self.put_prepared_operation_indexed(record);
        Ok(())
    }
}
