//! Current prepared-operation recovery and verified local-content reconciliation.
use crate::*;
use std::time::Duration;

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn reconcile_stale_write_intents_bounded(
        &mut self,
        max_records: usize,
        _stale_min_age: Duration,
    ) -> Result<DfsWriteIntentMaintenanceReport, DfsError> {
        let page = self.active_write_intent_maintenance_candidates(max_records)?;
        let mut report = DfsWriteIntentMaintenanceReport {
            scanned_active_intent_count: page.intents.len(),
            visited_intent_record_count: page.visited_record_count,
            visited_intent_shard_count: page.visited_shard_count,
            scan_has_more: page.next_cursor.is_some(),
            ..Default::default()
        };
        for intent in page.intents {
            if let Some(terminal) = self.reconcile_observed_write_intent(&intent, false)? {
                match terminal.status {
                    DfsWriteIntentStatus::Committed => report.committed_visible_intent_count += 1,
                    DfsWriteIntentStatus::Aborted | DfsWriteIntentStatus::Rejected => {
                        report.aborted_superseded_intent_count += 1
                    }
                    _ => {}
                }
            }
        }
        Ok(report)
    }

    pub(crate) fn reconcile_observed_write_intent(
        &mut self,
        intent: &DfsWriteIntentRecord,
        allow_abort_missing: bool,
    ) -> Result<Option<DfsWriteIntentRecord>, DfsError> {
        self.reconcile_observed_write_intent_with_policy(intent, allow_abort_missing, false)
    }

    pub(crate) fn reconcile_observed_write_intent_with_policy(
        &mut self,
        intent: &DfsWriteIntentRecord,
        _allow_abort_missing: bool,
        reject_if_unpublished: bool,
    ) -> Result<Option<DfsWriteIntentRecord>, DfsError> {
        let record = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message: "intent reconciliation requires a current durable backend".into(),
            })?
            .prepared_operation_record(&format!("write:{}", intent.intent_id))?;
        let Some(record) = record else {
            return Ok(None);
        };
        let original =
            record
                .payload
                .prepared_intent
                .as_ref()
                .ok_or_else(|| DfsError::Metadata {
                    message: "write reconciliation has no captured intent".into(),
                })?;
        if original.intent_id != intent.intent_id
            || original.object_id != intent.object_id
            || original.file_system_id != intent.file_system_id
            || original.base_version != intent.base_version
            || original.target_version != intent.target_version
            || original.content_hash != intent.content_hash
        {
            return Err(DfsError::Metadata {
                message: "write reconciliation differs from the retained immutable operation"
                    .into(),
            });
        }
        match record.status {
            DfsPreparedOperationStatus::Prepared if !reject_if_unpublished => {
                self.recover_one_prepared_operation(record.clone())?;
                self.metadata_backend
                    .as_ref()
                    .expect("reconciliation backend")
                    .get_intent(&intent.object_id, &intent.intent_id)
                    .map_err(DfsError::from)
            }
            DfsPreparedOperationStatus::Committed => {
                self.refresh_recorded_prepared_rows(&record.payload)?;
                if self.coordinator.intent(&intent.intent_id).is_some() {
                    self.coordinator
                        .restore_recorded_committed_intent(&intent.intent_id)?;
                }
                Ok(record.payload.terminal_intent.clone())
            }
            DfsPreparedOperationStatus::Aborted => {
                let mut terminal = intent.clone();
                terminal.status = DfsWriteIntentStatus::Aborted;
                self.remove_pending_write_state(&intent.intent_id);
                Ok(Some(terminal))
            }
            DfsPreparedOperationStatus::Prepared | DfsPreparedOperationStatus::Superseded => {
                Ok(None)
            }
        }
    }

    fn active_write_intent_maintenance_candidates(
        &self,
        max_records: usize,
    ) -> Result<crate::DfsWriteIntentMaintenancePage, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(Default::default());
        };
        let trace_started = crate::dfs_perf_trace_enabled().then(std::time::Instant::now);
        let cursor = self.write_intent_maintenance_scan.cursor()?;
        let mut page = backend.write_intent_maintenance_page(cursor.as_ref(), max_records)?;
        // A cursor is observation progress, not a completion receipt. Advance
        // before fallible recovery so a bad candidate cannot pin every future
        // pass to the same prefix. Its durable row is revisited after EOF wrap.
        self.write_intent_maintenance_scan
            .advance(&cursor, page.next_cursor.clone())?;
        let mut eligible = Vec::with_capacity(page.intents.len());
        if let Some(shard_id) = page.scanned_shard_id.as_ref() {
            for intent in page.intents {
                if backend.write_intent_maintenance_candidate_is_listable(shard_id, &intent)? {
                    eligible.push(intent);
                }
            }
        } else if !page.intents.is_empty() {
            return Err(DfsError::Metadata {
                message: "bounded intent page lost physical shard identity".into(),
            });
        }
        page.intents = eligible;
        if let Some(started) = trace_started {
            tracing::trace!(target: "sinomo.dfs.perf", node = %self.node_id,
                resume_shard = ?cursor.as_ref().map(|cursor| &cursor.shard_id),
                resume_after = ?cursor.as_ref().and_then(|cursor| cursor.after.as_ref()),
                scanned_shard = ?page.scanned_shard_id,
                next_cursor = ?page.next_cursor,
                max_visited = max_records, visited_records = page.visited_record_count,
                visited_shards = page.visited_shard_count, active_candidates = page.intents.len(),
                has_more = page.next_cursor.is_some(), elapsed_us = started.elapsed().as_micros() as u64,
                "dfs-maintenance-intent-scan");
        }
        Ok(page)
    }

    pub fn reconcile_local_manifests(
        &mut self,
        object_id: &DfsObjectId,
    ) -> Result<DfsReconnectReconciliation, DfsError> {
        let object = self.current_read_object_metadata(object_id)?;
        let manifests = self
            .content
            .manifests_for_object(object.object_id.as_str())?;
        let mut refreshed_receipts = 0;
        let mut ignored_divergent_manifests = 0usize;
        let mut has_current_manifest = false;
        let may_refresh_local_replica_receipt = self.local_manifest_receipt_refresh_is_standalone();

        for manifest in manifests {
            let manifest_version = DfsVersion::new(manifest.version.clone()).map_err(|error| {
                DfsError::ContentStore {
                    message: error.to_string(),
                }
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
                        if may_refresh_local_replica_receipt {
                            let local_node_id = self.node_id.clone();
                            let receipt_inserted = self.put_metadata_receipt(
                                &object.file_system_id,
                                &object.object_id,
                                &object.version,
                                &local_node_id,
                                &manifest.content_hash,
                            )?;
                            if receipt_inserted {
                                refreshed_receipts += 1;
                            }
                        }
                    }
                } else {
                    ignored_divergent_manifests = ignored_divergent_manifests.saturating_add(1);
                }
            } else if !self.local_manifest_is_known_historical_version(
                &object.object_id,
                &manifest_version,
                &manifest.content_hash,
            )? {
                ignored_divergent_manifests = ignored_divergent_manifests.saturating_add(1);
            }
        }

        Ok(DfsReconnectReconciliation {
            object_id: object.object_id.clone(),
            refreshed_receipts,
            ignored_divergent_manifests,
            missing_current: object.content_hash.is_some() && !has_current_manifest,
        })
    }

    fn local_manifest_receipt_refresh_is_standalone(&self) -> bool {
        self.control_plane.is_none()
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
