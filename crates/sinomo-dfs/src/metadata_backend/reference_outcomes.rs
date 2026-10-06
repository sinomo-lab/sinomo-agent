//! Object-indexed reference outcome transfer; no old transaction body replay.
use super::*;
use crate::{DfsContentReferenceOutcomePage, DfsContentReferenceOutcomeRetention};

impl LocalSobjMetadataBackend {
    pub(super) fn reference_outcome_page(
        &self,
        fs: &DfsFileSystemId,
        object: &DfsObjectId,
        after: Option<&str>,
        limit: usize,
    ) -> Result<DfsContentReferenceOutcomePage> {
        if limit == 0 || limit > 128 {
            return Err(MetadataBackendError::Serde(
                "reference outcome page limit must be in 1..=128".into(),
            ));
        }
        let tables = self.current_tables_snapshot()?;
        let mut outcomes = Vec::new();
        let Some(ids) = tables.content_reference_outcome_ids_for_object(object) else {
            return Ok(DfsContentReferenceOutcomePage {
                outcomes,
                next_cursor: None,
            });
        };
        let range = (
            after.map_or(std::ops::Bound::Unbounded, |id| {
                std::ops::Bound::Excluded(id.to_string())
            }),
            std::ops::Bound::Unbounded,
        );
        for id in ids.range(range).take(limit + 1) {
            let outcome = if let Some(tx) = tables.namespace_transactions.get(id) {
                DfsNamespaceTransactionReceipt::from_transaction(tx, tx.created_at)
                    .map_err(|error| MetadataBackendError::Serde(error.to_string()))?
            } else {
                tables
                    .namespace_transaction_receipts
                    .get(id)
                    .ok_or_else(|| {
                        MetadataBackendError::Serde(
                            "reference outcome index has no durable witness".into(),
                        )
                    })?
                    .clone()
            };
            outcome
                .validate_content_reference_annotation()
                .map_err(MetadataBackendError::Serde)?;
            if outcome
                .content_reference_identity
                .as_ref()
                .is_none_or(|identity| {
                    identity.file_system_id != *fs || identity.object_id != *object
                })
            {
                return Err(MetadataBackendError::Serde(
                    "reference outcome index has foreign identity".into(),
                ));
            }
            outcomes.push(outcome);
        }
        let next_cursor = if outcomes.len() > limit {
            outcomes.pop();
            outcomes
                .last()
                .map(|outcome| outcome.transaction_id.clone())
        } else {
            None
        };
        Ok(DfsContentReferenceOutcomePage {
            outcomes,
            next_cursor,
        })
    }

    pub(super) fn reference_outcome_retention(
        &self,
        fs: &DfsFileSystemId,
        object: &DfsObjectId,
    ) -> Result<Option<DfsContentReferenceOutcomeRetention>> {
        let tables = self.current_tables_snapshot()?;
        let retention = tables
            .namespace_receipt_retention
            .content_reference_outcome_retention
            .get(object)
            .cloned();
        if retention
            .as_ref()
            .is_some_and(|retention| retention.file_system_id != *fs)
        {
            return Err(MetadataBackendError::Serde(
                "foreign reference outcome expiry identity".into(),
            ));
        }
        Ok(retention)
    }

    pub(super) fn install_reference_outcomes(
        &mut self,
        object: &DfsObjectMetadata,
        outcomes: &[DfsNamespaceTransactionReceipt],
        retention: Option<&DfsContentReferenceOutcomeRetention>,
    ) -> Result<Vec<DfsMetadataJournalEntry>> {
        if outcomes.is_empty() && retention.is_none() {
            return Ok(Vec::new());
        }
        if self.namespace_prepare_codec_configured
            && !self
                .namespace_prepare_codec
                .supports_retention_metadata_mutations()
        {
            return Err(MetadataBackendError::Serde(
                "reference outcome transfer requires a packed-v6 authority".into(),
            ));
        }
        let _lock = self.begin_metadata_write()?;
        let mut proposed = BTreeMap::<String, DfsNamespaceTransactionReceipt>::new();
        for incoming in outcomes {
            incoming
                .validate_content_reference_annotation()
                .map_err(MetadataBackendError::Serde)?;
            if incoming
                .content_reference_identity
                .as_ref()
                .is_none_or(|identity| {
                    identity.file_system_id != object.file_system_id
                        || identity.object_id != object.object_id
                })
            {
                return Err(MetadataBackendError::Serde(
                    "foreign transferred reference outcome".into(),
                ));
            }
            let existing = if let Some(tx) = self
                .tables
                .namespace_transactions
                .get(&incoming.transaction_id)
            {
                Some(
                    DfsNamespaceTransactionReceipt::from_transaction(tx, tx.created_at)
                        .map_err(|error| MetadataBackendError::Serde(error.to_string()))?,
                )
            } else {
                self.tables
                    .namespace_transaction_receipts
                    .get(&incoming.transaction_id)
                    .cloned()
            };
            let mut incoming = incoming.clone();
            if let Some(existing) = existing {
                if !incoming.same_outcome_identity(&existing) {
                    return Err(MetadataBackendError::Serde(
                        "conflicting transferred reference outcome".into(),
                    ));
                }
                incoming.retained_at = incoming.retained_at.min(existing.retained_at);
                if self
                    .tables
                    .namespace_transactions
                    .contains_key(&incoming.transaction_id)
                    || self
                        .tables
                        .namespace_transaction_receipts
                        .get(&incoming.transaction_id)
                        == Some(&incoming)
                {
                    continue;
                }
            }
            if let Some(existing) = proposed.get_mut(&incoming.transaction_id) {
                if !existing.same_outcome_identity(&incoming) {
                    return Err(MetadataBackendError::Serde(
                        "duplicate conflicting transferred reference outcome".into(),
                    ));
                }
                existing.retained_at = existing.retained_at.min(incoming.retained_at);
            } else {
                proposed.insert(incoming.transaction_id.clone(), incoming);
            }
        }
        if let Some(retention) = retention
            && (retention.file_system_id != object.file_system_id
                || retention
                    .max_evicted_operation_order_token
                    .as_ref()
                    .is_some_and(|token| {
                        crate::intent::stable_operation_order_token(token).as_ref() != Some(token)
                    }))
        {
            return Err(MetadataBackendError::Serde(
                "invalid transferred reference expiry state".into(),
            ));
        }
        let mut entries = Vec::new();
        for outcome in proposed.into_values() {
            entries.push(self.write_journal_entry_and_apply_table_ops_unlocked(
                DfsMetadataMutation::put_namespace_transaction_receipt(&outcome),
                vec![DfsMetadataTableOp::PutNamespaceTransactionReceipt { receipt: outcome }],
            )?);
        }
        if let Some(retention) = retention {
            let mut merged = self
                .tables
                .namespace_receipt_retention
                .content_reference_outcome_retention
                .get(&object.object_id)
                .cloned()
                .unwrap_or_else(|| retention.clone());
            merged.merge_conservative(retention);
            if self
                .tables
                .namespace_receipt_retention
                .content_reference_outcome_retention
                .get(&object.object_id)
                != Some(&merged)
            {
                let mut delta = DfsNamespaceReceiptRetentionState::default();
                delta
                    .content_reference_outcome_retention
                    .insert(object.object_id.clone(), merged);
                let transaction_id = format!("reference-expiry:{}", object.object_id);
                entries.push(self.write_journal_entry_and_apply_table_ops_unlocked(
                    DfsMetadataMutation::RemoveNamespaceTransactionReceipt {
                        file_system_id: object.file_system_id.clone(),
                        transaction_id: transaction_id.clone(),
                        retention: delta.clone(),
                    },
                    vec![DfsMetadataTableOp::RemoveNamespaceTransactionReceipt {
                        transaction_id,
                        retention: delta,
                    }],
                )?);
            }
        }
        Ok(entries)
    }
}
