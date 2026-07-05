//! Conflict records and reconnect reconciliation for DFS services.
//!
//! This module keeps conflict publication, conflict lookup, and reconnect-time
//! local manifest reconciliation together. Conflict records are part of DFS
//! metadata, so the service always merges the in-memory view with backend pages
//! through one bounded collector.

use std::collections::BTreeMap;

use sinomo_core::node::NodeId;
use time::OffsetDateTime;

use crate::metadata_pages::collect_metadata_pages;
use crate::*;

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    /// Returns every known conflict for an object, merging transient in-memory
    /// records with durable backend pages.
    pub fn conflicts_for_object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<Vec<DfsConflictRecord>, DfsError> {
        self.collect_conflict_records_for_object(
            object_id,
            self.metadata.conflicts_for_object(object_id),
            |_| true,
        )
    }

    /// Returns unresolved conflicts for an object, merging transient in-memory
    /// records with durable backend pages.
    pub fn open_conflicts_for_object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<Vec<DfsConflictRecord>, DfsError> {
        self.collect_conflict_records_for_object(
            object_id,
            self.metadata.open_conflicts_for_object(object_id),
            |conflict| conflict.status == DfsConflictStatus::Open,
        )
    }

    /// Updates a conflict status in the authoritative backend when configured,
    /// and mirrors the resulting record into the service's transient metadata.
    pub fn update_conflict_status(
        &mut self,
        object_id: &DfsObjectId,
        conflict_id: &str,
        status: DfsConflictStatus,
    ) -> Result<Option<DfsConflictRecord>, DfsError> {
        let updated = if let Some(backend) = self.metadata_backend.as_mut() {
            backend.update_conflict_status(object_id, conflict_id, status.clone())?
        } else {
            self.metadata
                .conflicts_for_object(object_id)
                .into_iter()
                .find(|conflict| conflict.conflict_id == conflict_id)
                .map(|mut conflict| {
                    conflict.status = status;
                    conflict
                })
        };
        if let Some(conflict) = updated.clone() {
            self.metadata.add_conflict(conflict);
        }
        Ok(updated)
    }

    pub(crate) fn reconcile_stale_conflicts_bounded(
        &mut self,
        max_records: usize,
    ) -> Result<DfsConflictMaintenanceReport, DfsError> {
        if max_records == 0 {
            return Ok(DfsConflictMaintenanceReport::default());
        }
        let conflicts = self.open_conflict_maintenance_candidates(max_records)?;
        let mut report = DfsConflictMaintenanceReport {
            scanned_open_conflict_count: conflicts.len(),
            ..DfsConflictMaintenanceReport::default()
        };
        for conflict in conflicts {
            let Some(status) = self.reconciled_conflict_status(&conflict)? else {
                continue;
            };
            let Some(updated) =
                self.update_conflict_status(&conflict.object_id, &conflict.conflict_id, status)?
            else {
                continue;
            };
            match updated.status {
                DfsConflictStatus::Resolved => {
                    report.resolved_conflict_count =
                        report.resolved_conflict_count.saturating_add(1);
                }
                DfsConflictStatus::Superseded => {
                    report.superseded_conflict_count =
                        report.superseded_conflict_count.saturating_add(1);
                }
                DfsConflictStatus::Open => {}
            }
        }
        Ok(report)
    }

    fn open_conflict_maintenance_candidates(
        &self,
        max_records: usize,
    ) -> Result<Vec<DfsConflictRecord>, DfsError> {
        let mut candidates = Vec::new();
        if let Some(backend) = self.metadata_backend.as_ref() {
            let mut cursor = None;
            while candidates.len() < max_records {
                let remaining = max_records.saturating_sub(candidates.len());
                let page = backend.conflict_records_page(cursor.as_ref(), remaining)?;
                for conflict in page.conflicts {
                    if conflict.status == DfsConflictStatus::Open {
                        candidates.push(conflict);
                        if candidates.len() >= max_records {
                            break;
                        }
                    }
                }
                let Some(next_cursor) = page.next_cursor else {
                    break;
                };
                cursor = Some(next_cursor);
            }
            return Ok(candidates);
        }

        for conflicts in self.metadata.conflicts.values() {
            for conflict in conflicts.values() {
                if conflict.status != DfsConflictStatus::Open {
                    continue;
                }
                candidates.push(conflict.clone());
                if candidates.len() >= max_records {
                    return Ok(candidates);
                }
            }
        }
        Ok(candidates)
    }

    fn reconciled_conflict_status(
        &self,
        conflict: &DfsConflictRecord,
    ) -> Result<Option<DfsConflictStatus>, DfsError> {
        if conflict.status != DfsConflictStatus::Open {
            return Ok(None);
        }
        let object = match self.current_read_object_metadata(&conflict.object_id) {
            Ok(object) => object,
            Err(DfsError::UnknownObject { .. }) => {
                return Ok(Some(DfsConflictStatus::Superseded));
            }
            Err(error) => return Err(error),
        };
        if object.version == conflict.local_version || object.version == conflict.remote_version {
            return Ok(Some(DfsConflictStatus::Resolved));
        }
        if conflict
            .base_version
            .as_ref()
            .is_some_and(|base_version| object.version == *base_version)
        {
            return Ok(None);
        }
        Ok(Some(DfsConflictStatus::Superseded))
    }

    /// Reconciles locally stored manifests for an object after reconnect.
    ///
    /// Matching current-version content refreshes local replica receipts. Local
    /// manifests that do not match the authoritative current or historical
    /// backend version become open conflict records instead of overwriting the
    /// visible object.
    pub fn reconcile_local_manifests(
        &mut self,
        object_id: &DfsObjectId,
    ) -> Result<DfsReconnectReconciliation, DfsError> {
        let object = self.current_read_object_metadata(object_id)?;
        let manifests = self
            .content
            .manifests_for_object(object.object_id.as_str())?;
        let mut refreshed_receipts = 0;
        let mut conflicts = Vec::new();
        let mut has_current_manifest = false;

        for manifest in manifests {
            let manifest_version = DfsVersion::deserialize_migrating_legacy(
                manifest.version.clone(),
            )
            .map_err(|error| DfsError::ContentStore {
                message: error.to_string(),
            })?;
            if manifest_version == object.version {
                if object
                    .content_hash
                    .as_ref()
                    .is_some_and(|hash| hash == &manifest.content_hash)
                {
                    if self
                        .content
                        .has_blob(&manifest.content_hash)
                        .unwrap_or(false)
                    {
                        has_current_manifest = true;
                        let local_node_id = self.node_id.clone();
                        let receipt_inserted = self.put_metadata_receipt(
                            &object.file_system_id,
                            &object.object_id,
                            &object.version,
                            &local_node_id,
                            &manifest.content_hash,
                        )?;
                        self.mark_repaired_location(
                            &object.object_id,
                            &object.version,
                            &local_node_id,
                            &manifest.content_hash,
                        )?;
                        if receipt_inserted {
                            refreshed_receipts += 1;
                        }
                    }
                } else {
                    conflicts.push(self.create_write_conflict(
                        &object,
                        Some(object.version.clone()),
                        manifest_version,
                        self.node_id.clone(),
                        Some(manifest.content_hash),
                        "local current-version manifest hash diverged during reconnect".to_string(),
                        None,
                    )?);
                }
            } else if !self.local_manifest_is_known_historical_version(
                &object.object_id,
                &manifest_version,
                &manifest.content_hash,
            )? {
                conflicts.push(self.create_write_conflict(
                    &object,
                    Some(object.version.clone()),
                    manifest_version,
                    self.node_id.clone(),
                    Some(manifest.content_hash),
                    "local non-visible manifest diverged during reconnect".to_string(),
                    None,
                )?);
            }
        }

        Ok(DfsReconnectReconciliation {
            object_id: object.object_id.clone(),
            refreshed_receipts,
            conflicts,
            missing_current: object.content_hash.is_some() && !has_current_manifest,
        })
    }

    pub(crate) fn create_write_conflict(
        &mut self,
        object: &DfsObjectMetadata,
        base_version: Option<DfsVersion>,
        local_version: DfsVersion,
        detected_by_node_id: NodeId,
        local_content_hash: Option<String>,
        reason: String,
        created_by_intent_id: Option<String>,
    ) -> Result<DfsConflictRecord, DfsError> {
        let conflict_id = conflict_id_for(
            &object.object_id,
            &local_version,
            local_content_hash.as_deref(),
            &detected_by_node_id,
        );
        if let Some(existing) =
            self.get_conflict_from_memory_or_backend(&object.object_id, &conflict_id)?
        {
            return Ok(existing);
        }
        let conflict = DfsConflictRecord {
            conflict_id,
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            path: object.path.clone(),
            base_version,
            local_version,
            remote_version: object.version.clone(),
            detected_by_node_id,
            reason,
            local_content_hash,
            remote_content_hash: object.content_hash.clone(),
            created_by_intent_id,
            strategy: Some("branch".to_string()),
            status: DfsConflictStatus::Open,
            detected_at: OffsetDateTime::now_utc(),
        };
        self.put_conflict_record(&conflict)?;
        Ok(conflict)
    }

    pub(crate) fn put_conflict_record(
        &mut self,
        conflict: &DfsConflictRecord,
    ) -> Result<(), DfsError> {
        if let Some(backend) = self.metadata_backend.as_mut() {
            if backend
                .get_conflict(&conflict.object_id, &conflict.conflict_id)?
                .as_ref()
                == Some(conflict)
            {
                self.metadata.add_conflict(conflict.clone());
                return Ok(());
            }
            backend.put_conflict(conflict)?;
        }
        self.metadata.add_conflict(conflict.clone());
        Ok(())
    }

    pub(crate) fn add_backend_conflict_pages_to_memory(
        &mut self,
        object_id: &DfsObjectId,
    ) -> Result<(), DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(());
        };
        let conflicts = collect_metadata_pages(|cursor| {
            let page = backend.conflicts_for_object_page(
                object_id,
                cursor,
                DFS_CONFLICT_SCAN_PAGE_SIZE,
            )?;
            Ok((page.conflicts, page.next_cursor))
        })?;
        for conflict in conflicts {
            self.metadata.add_conflict(conflict);
        }
        Ok(())
    }

    fn collect_conflict_records_for_object<I, F>(
        &self,
        object_id: &DfsObjectId,
        memory_conflicts: I,
        mut include_backend_conflict: F,
    ) -> Result<Vec<DfsConflictRecord>, DfsError>
    where
        I: IntoIterator<Item = DfsConflictRecord>,
        F: FnMut(&DfsConflictRecord) -> bool,
    {
        let mut conflicts: BTreeMap<String, DfsConflictRecord> = memory_conflicts
            .into_iter()
            .map(|conflict| (conflict.conflict_id.clone(), conflict))
            .collect();
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(conflicts.into_values().collect());
        };
        let backend_conflicts = collect_metadata_pages(|cursor| {
            let page = backend.conflicts_for_object_page(
                object_id,
                cursor,
                DFS_CONFLICT_SCAN_PAGE_SIZE,
            )?;
            Ok((page.conflicts, page.next_cursor))
        })?;
        for conflict in backend_conflicts {
            if include_backend_conflict(&conflict) {
                conflicts.insert(conflict.conflict_id.clone(), conflict);
            }
        }
        Ok(conflicts.into_values().collect())
    }

    fn get_conflict_from_memory_or_backend(
        &self,
        object_id: &DfsObjectId,
        conflict_id: &str,
    ) -> Result<Option<DfsConflictRecord>, DfsError> {
        if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(conflict) = backend.get_conflict(object_id, conflict_id)?
        {
            return Ok(Some(conflict));
        }
        Ok(self
            .metadata
            .conflicts_for_object(object_id)
            .into_iter()
            .find(|conflict| conflict.conflict_id == conflict_id))
    }

    fn local_manifest_is_known_historical_version(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &str,
    ) -> Result<bool, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(false);
        };
        Ok(backend
            .get_version(object_id, version)?
            .is_some_and(|record| record.content_hash.as_deref() == Some(content_hash)))
    }
}
