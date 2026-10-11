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
            let outcome = tables
                .namespace_transaction_receipts
                .get(id)
                .ok_or_else(|| {
                    MetadataBackendError::Serde(
                        "reference outcome index has no durable witness".into(),
                    )
                })?
                .clone();
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
}
