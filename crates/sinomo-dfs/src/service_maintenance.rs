//! Networked service maintenance and metadata-journal replication helpers.
//!
//! The reusable maintenance hosts live in `maintenance`; this module contains
//! the `NetworkedDfsService` methods that inspect local content, run lifecycle
//! maintenance against the service's stores, and schedule metadata journal
//! replication to configured shard replicas.

use sinomo_core::node::NodeId;

use std::collections::BTreeSet;

use crate::*;

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub fn gc_metadata_history(
        &mut self,
        options: DfsMetadataHistoryGcOptions,
    ) -> Result<DfsMetadataHistoryGcReport, DfsError> {
        let plan = self
            .metadata_backend
            .as_mut()
            .ok_or_else(|| DfsError::Metadata {
                message: "history collection requires current durable metadata".into(),
            })?
            .plan_metadata_history_gc(options)?;
        for ((fs, shard_id), effects) in plan.effects_by_scope {
            if effects.is_empty() {
                continue;
            }
            let epoch = if let Some(control) = self.control_plane.as_mut() {
                let shard = DfsControlPlaneStore::new(control.as_mut())
                    .get_metadata_shard(&fs, &shard_id.0)?
                    .ok_or_else(|| DfsError::Metadata {
                        message: "history collection has no exact current shard".into(),
                    })?;
                if shard.primary_node_id != self.node_id {
                    return Err(DfsError::StaleOwnerEpoch {
                        file_system_id: fs,
                        local_node_id: self.node_id.clone(),
                        primary_node_id: shard.primary_node_id,
                        epoch: shard.epoch,
                    });
                }
                shard.epoch
            } else {
                0
            };
            let scope = DfsPreparedOperationScope {
                file_system_id: fs.clone(),
                shard_id: shard_id.clone(),
                owner_epoch: epoch,
                operation_id: format!("history-gc:{}", sinomo_core::Id::generate()),
            };
            let _execution = self.acquire_prepared_execution(&scope)?;
            let ids = effects
                .iter()
                .flat_map(|effect| effect.affected_object_ids())
                .collect::<std::collections::BTreeSet<_>>();
            // Capture object and namespace identity from one exact physical
            // native cut. A retired SourceEviction row can remember a path now
            // occupied by a distinct tombstone or a newly created live object.
            let (objects, paths) = if ids.is_empty() {
                (Vec::new(), Vec::new())
            } else {
                let backend = self.metadata_backend.as_mut().expect("GC backend");
                backend.begin_consistent_read_snapshot_on_shard(&shard_id)?;
                let expectations: Result<(Vec<_>, Vec<_>), DfsError> = (|| {
                    let mut objects = Vec::new();
                    let mut paths = std::collections::BTreeMap::new();
                    for id in ids {
                        let row = backend.get_object_on_shard(&shard_id, &id)?;
                        objects.push(DfsNamespaceObjectVersionExpectation {
                            object_id: id,
                            expected_version: row.as_ref().map(|row| row.version.clone()),
                        });
                        if let Some(row) = row {
                            let path = crate::namespace::metadata_path(&row.path);
                            let occupant = backend
                                .get_object_by_path_on_shard(&shard_id, &fs, &path)?
                                .map(|current| current.object_id);
                            match paths.entry(path) {
                                std::collections::btree_map::Entry::Vacant(entry) => {
                                    entry.insert(occupant);
                                }
                                std::collections::btree_map::Entry::Occupied(entry)
                                    if entry.get() != &occupant =>
                                {
                                    return Err(DfsError::Metadata {
                                        message: "history GC namespace identity changed within its exact read cut".into(),
                                    });
                                }
                                std::collections::btree_map::Entry::Occupied(_) => {}
                            }
                        }
                    }
                    Ok((
                        objects,
                        paths
                            .into_iter()
                            .map(
                                |(path, expected_object_id)| DfsNamespacePathOccupantExpectation {
                                    path,
                                    expected_object_id,
                                },
                            )
                            .collect(),
                    ))
                })();
                backend.end_consistent_read_snapshot_on_shard(&shard_id);
                expectations?
            };
            // Compacted and removed owners change the repair index, which
            // drops their needs.
            self.publish_metadata_effects_on_shard(scope, objects, paths, effects)?;
        }
        Ok(plan.report)
    }

    pub fn run_local_content_anti_entropy(
        &mut self,
        options: DfsAntiEntropyOptions,
    ) -> Result<DfsAntiEntropyReport, DfsError> {
        let mut report = DfsAntiEntropyReport {
            file_system_id: options.file_system_id.clone(),
            after: options.after.clone(),
            next_cursor: None,
            metadata_backend_configured: self.metadata_backend.is_some(),
            scanned_object_count: 0,
            checked_local_location_count: 0,
            stale_local_location_count: 0,
            repaired_location_count: 0,
            issue_count: 0,
            issues: Vec::new(),
            max_issues_reached: false,
        };
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(report);
        };
        let page = backend.objects_for_file_system_page(
            &options.file_system_id,
            options.after.as_ref(),
            options.max_objects.max(1),
        )?;
        report.next_cursor = page.next_cursor;
        let local_node_id = self.node_id.clone();

        for mut object in page.objects {
            report.scanned_object_count = report.scanned_object_count.saturating_add(1);
            if !matches!(object.kind, DfsObjectKind::File | DfsObjectKind::Symlink) {
                continue;
            }
            if !object.current_locations.contains(&local_node_id) {
                continue;
            }
            let Some(content_hash) = object.content_hash.clone() else {
                continue;
            };
            report.checked_local_location_count =
                report.checked_local_location_count.saturating_add(1);
            let local_metadata_primary =
                options.repair_metadata && self.is_local_metadata_primary_for_object(&object)?;
            let verified_local_size = if local_metadata_primary {
                match self
                    .content
                    .read_manifest(object.object_id.as_str(), object.version.as_str())
                {
                    Ok(manifest)
                        if manifest.content_hash == content_hash
                            && self
                                .content
                                .manifest_storage_candidate_available(&manifest)
                                .unwrap_or(false) =>
                    {
                        Some(manifest.size_bytes)
                    }
                    Ok(_) => None,
                    Err(_)
                        if object
                            .content_size_bytes
                            .is_none_or(|expected_size| expected_size == 0) =>
                    {
                        self.content
                            .open_blob_reader(&content_hash)
                            .ok()
                            .map(|reader| reader.size_bytes())
                    }
                    Err(_) => None,
                }
            } else {
                None
            };
            if let Some(actual_size) = verified_local_size
                && object.content_size_bytes != Some(actual_size)
            {
                let expected_size = object.content_size_bytes;
                self.content.put_blob_only_manifest(
                    object.object_id.as_str(),
                    object.version.as_str(),
                    &content_hash,
                    actual_size,
                )?;
                object.content_size_bytes = Some(actual_size);
                self.put_visible_object_and_version_in_metadata_backend(
                    &object,
                    Some(actual_size),
                )?;
                self.metadata.add_object(object.clone())?;
                report.stale_local_location_count =
                    report.stale_local_location_count.saturating_add(1);
                report.repaired_location_count = report.repaired_location_count.saturating_add(1);
                record_anti_entropy_issue(
                    &mut report,
                    options.max_issues,
                    DfsAntiEntropyIssue {
                        object_id: object.object_id,
                        version: object.version,
                        path: object.path,
                        content_hash,
                        reason: format!(
                            "local verified content size differs from metadata: expected {expected_size:?}, actual {actual_size}"
                        ),
                        repaired: true,
                    },
                );
                continue;
            }
            let Some(reason) = self.local_content_anti_entropy_issue_reason(&object, &content_hash)
            else {
                continue;
            };
            report.stale_local_location_count = report.stale_local_location_count.saturating_add(1);
            let repaired = if options.repair_metadata {
                self.mark_remote_read_location_missing(
                    &object.object_id,
                    &object.version,
                    &object.content_hash,
                    &local_node_id,
                )?;
                report.repaired_location_count = report.repaired_location_count.saturating_add(1);
                true
            } else {
                false
            };
            record_anti_entropy_issue(
                &mut report,
                options.max_issues,
                DfsAntiEntropyIssue {
                    object_id: object.object_id,
                    version: object.version,
                    path: object.path,
                    content_hash,
                    reason,
                    repaired,
                },
            );
        }

        Ok(report)
    }

    pub fn run_lifecycle_maintenance(
        &mut self,
        options: DfsLifecycleMaintenanceOptions,
    ) -> Result<DfsLifecycleMaintenanceReport, DfsError> {
        let content_scrub = if options.scrub_content {
            Some(self.content.scrub()?)
        } else {
            None
        };
        let stale_staging_gc = options
            .stale_staging_gc
            .map(|options| self.content.collect_stale_staging(options))
            .transpose()?;
        let anti_entropy = options
            .anti_entropy
            .map(|options| self.run_local_content_anti_entropy(options))
            .transpose()?;
        // Metadata is the reachability authority. Journal and materialize its
        // bounded removals before deleting local manifests or blobs, then
        // derive content roots from the post-GC table image.
        let metadata_history_gc = options
            .metadata_history_gc
            .map(|options| self.gc_metadata_history(options))
            .transpose()?;
        let content_gc_requested = options.version_gc.is_some() || options.orphan_gc.is_some();
        let content_reservation_reconciliation_requested =
            options.content_reservation_reconciliation.is_some();
        // A local metadata image is not a cluster-wide reachability proof. Only
        // a complete, topology-current, freshness-checked catalog assembled
        // from every exact shard primary can authorize destructive collection.
        let content_root_catalog_validation = options.content_root_catalog_validation;
        let content_protection = content_root_catalog_validation
            .as_ref()
            .filter(|validation| validation.is_collection_authorized())
            .and_then(|validation| validation.protection().cloned());
        let catalog_generation = content_root_catalog_validation
            .as_ref()
            .and_then(|validation| validation.catalog_generation().map(str::to_owned));
        let scan_cut = content_root_catalog_validation
            .as_ref()
            .and_then(|validation| validation.catalog_scan_cut())
            .cloned();
        let scan_cut = match scan_cut {
            Some(cut) if self.content.catalog_scan_cut_matches_store(&cut)? => Some(cut),
            _ => None,
        };
        let content_gc_metadata_protection_unavailable =
            content_gc_requested && (content_protection.is_none() || scan_cut.is_none());
        let content_reservation_reconciliation = match (
            content_protection.as_ref(),
            options.content_reservation_reconciliation,
        ) {
            (Some(content_protection), Some(mut reconciliation)) => {
                reconciliation.catalog_generation = catalog_generation
                    .clone()
                    .expect("authorized catalog generation");
                // Local start is age evidence only; the durable ordinal proves
                // ordering, including a clock rollback between cut and renewal.
                reconciliation.catalog_generated_at_unix_nanos = scan_cut
                    .as_ref()
                    .map_or(0, |cut| cut.started_at_unix_nanos());
                reconciliation = reconciliation.with_catalog_scan_cut(scan_cut.clone());
                let versions = content_protection
                    .versions
                    .iter()
                    .map(|identity| {
                        (
                            identity.object_id.as_str().to_string(),
                            identity.version.as_str().to_string(),
                        )
                    })
                    .collect();
                let content_hashes = content_protection.content_hashes.iter().cloned().collect();
                Some(self.content.reconcile_content_reservations_with_catalog(
                    &versions,
                    &content_hashes,
                    reconciliation,
                )?)
            }
            _ => None,
        };
        let version_gc = match (
            options.version_gc,
            content_protection.as_ref(),
            scan_cut.as_ref(),
        ) {
            (Some(options), Some(content_protection), Some(cut)) => {
                let options =
                    options
                        .with_catalog_observation(
                            catalog_generation
                                .clone()
                                .expect("authorized catalog generation"),
                            cut.started_at_unix_nanos(),
                        )
                        .with_protected_versions(content_protection.versions.iter().map(
                            |identity| (identity.object_id.as_str(), identity.version.as_str()),
                        ));
                Some(self.content.collect_versions(options)?)
            }
            (Some(options), _, _) if options.dry_run => {
                Some(self.content.collect_versions(options)?)
            }
            _ => None,
        };
        let orphan_gc = match (
            options.orphan_gc,
            content_protection.as_ref(),
            scan_cut.as_ref(),
        ) {
            (Some(options), Some(content_protection), Some(cut)) => {
                let options = options
                    .with_catalog_observation(
                        catalog_generation
                            .clone()
                            .expect("authorized catalog generation"),
                        cut.started_at_unix_nanos(),
                    )
                    .with_protected_content_hashes(
                        content_protection.content_hashes.iter().map(String::as_str),
                    );
                Some(self.content.collect_garbage(options)?)
            }
            (Some(options), _, _) if options.dry_run => {
                Some(self.content.collect_garbage(options)?)
            }
            _ => None,
        };
        let durable_content_reservation_count =
            if content_gc_requested || content_reservation_reconciliation_requested {
                self.content.content_reservation_roots()?.reservation_count
            } else {
                0
            };
        let tombstone_compaction_requested = options.tombstone_compaction.is_some();
        let metadata_backend_configured = self.metadata_backend.is_some();
        let tombstone_compaction =
            match (options.tombstone_compaction, self.metadata_backend.as_mut()) {
                (Some(options), Some(backend)) => Some(backend.compact_tombstones(options)?),
                (Some(_), None) | (None, _) => None,
            };

        Ok(DfsLifecycleMaintenanceReport {
            content_scrub,
            stale_staging_gc,
            anti_entropy,
            version_gc,
            orphan_gc,
            metadata_history_gc,
            content_root_catalog_validation,
            content_reservation_reconciliation,
            durable_content_reservation_count,
            content_gc_metadata_protection_unavailable,
            metadata_backend_configured,
            tombstone_compaction_requested,
            tombstone_compaction,
        })
    }

    /// Releases only reservations whose exact object-version identity was
    /// positively witnessed by a previously installed complete catalog.
    ///
    /// This path never rejects an uncovered reservation and never authorizes
    /// manifest or blob deletion. Positive historical coverage is monotonic:
    /// once authoritative metadata accepted the version, the local
    /// pre-catalog reservation no longer needs to stand in for that evidence.
    pub(crate) fn reconcile_content_reservations_from_installed_catalog(
        &mut self,
        catalog_versions: &BTreeSet<(String, String)>,
        catalog_generation: &str,
        max_reservations: u64,
    ) -> Result<DfsLifecycleMaintenanceReport, DfsError> {
        let reconciliation = self.content.reconcile_content_reservations_with_catalog(
            catalog_versions,
            &BTreeSet::new(),
            ContentStoreReservationReconciliationOptions::bounded(
                0,
                std::time::Duration::ZERO,
                max_reservations,
            )
            .with_catalog_generation(catalog_generation),
        )?;
        let durable_content_reservation_count =
            self.content.content_reservation_roots()?.reservation_count;
        Ok(DfsLifecycleMaintenanceReport {
            content_reservation_reconciliation: Some(reconciliation),
            durable_content_reservation_count,
            metadata_backend_configured: self.metadata_backend.is_some(),
            ..DfsLifecycleMaintenanceReport::default()
        })
    }

    fn local_content_anti_entropy_issue_reason(
        &self,
        object: &DfsObjectMetadata,
        expected_content_hash: &str,
    ) -> Option<String> {
        let manifest = match self
            .content
            .read_manifest(object.object_id.as_str(), object.version.as_str())
        {
            Ok(manifest) => manifest,
            Err(error) => {
                if let Ok(reader) = self.content.open_blob_stream(expected_content_hash)
                    && object
                        .content_size_bytes
                        .is_none_or(|expected_size| expected_size == reader.size_bytes())
                {
                    return None;
                }
                return Some(format!(
                    "local object-version manifest is missing or unreadable: {error}"
                ));
            }
        };
        if manifest.content_hash != expected_content_hash {
            return Some(format!(
                "local object-version manifest hash mismatch: expected {expected_content_hash}, actual {}",
                manifest.content_hash
            ));
        }
        if let Some(expected_size) = object.content_size_bytes
            && manifest.size_bytes != expected_size
        {
            return Some(format!(
                "local object-version manifest size mismatch: expected {expected_size}, actual {}",
                manifest.size_bytes
            ));
        }
        match self.content.manifest_storage_candidate_available(&manifest) {
            Ok(true) => None,
            Ok(false) => Some(
                "local object-version manifest references missing or corrupt content".to_string(),
            ),
            Err(error) => Some(format!(
                "local object-version manifest content is unreadable: {error}"
            )),
        }
    }

    pub fn send_metadata_journal_since(
        &mut self,
        target_node_id: &NodeId,
        after_sequence: u64,
    ) -> Result<Option<DfsMetadataJournalBatch>, DfsError> {
        let Some(batch) = self.metadata_journal_batch_since(after_sequence)? else {
            return Ok(None);
        };
        let authority_shard = match self.control_plane.as_mut() {
            Some(control_plane) => {
                let file_system_id = metadata_journal_batch_file_system_id(&batch).ok_or_else(|| {
                    DfsError::Metadata {
                        message: format!(
                            "clustered metadata journal batch for shard {} has no filesystem identity",
                            batch.shard_id.0
                        ),
                    }
                })?;
                Some(
                    DfsControlPlaneStore::new(control_plane.as_mut())
                        .get_metadata_shard(&file_system_id, &batch.shard_id.0)?
                        .ok_or_else(|| DfsError::Metadata {
                            message: format!(
                                "metadata journal batch has no configured shard {} for filesystem {}",
                                batch.shard_id.0, file_system_id
                            ),
                        })?,
                )
            }
            None => None,
        };
        let request_id = self.next_request_id();
        let sent = self.send_dfs_message_if_routable(
            target_node_id,
            DfsNetworkMessage::MetadataJournalBatch(DfsMetadataJournalBatchCommand {
                request_id,
                source_node_id: Some(self.node_id.clone()),
                file_system_id: authority_shard
                    .as_ref()
                    .map(|shard| shard.file_system_id.clone()),
                authority_epoch: authority_shard.as_ref().map(|shard| shard.epoch),
                verify_prefix_only: false,
                prefix_probe: None,
                authority_probe: None,
                durable_replication: None,
                batch: crate::metadata_replication::prepare_dfs_metadata_journal_frame_batch(
                    self.metadata_backend
                        .as_ref()
                        .expect("journal source backend")
                        .as_ref(),
                    &batch,
                )?,
            }),
        )?;
        Ok(sent.then_some(batch))
    }
}

#[cfg(test)]
mod compacted_transfer_retirement_data_tests {
    use super::*;
    #[test]
    fn current_queue_observations_require_explicit_unknown_fields() {
        let data: serde_json::Value = serde_json::from_str(include_str!(
            "test_support/fixtures/native-retained-repair-retirement-r276-unknown.json"
        ))
        .unwrap();
        let original = data["queue"][0].clone();
        let _: DfsRepairAttemptRecord = serde_json::from_value(original.clone()).unwrap();
        for field in [
            "transfer_delivery_state",
            "completed_transfer_request_id",
            "transfer_kind",
        ] {
            let mut missing = original.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(serde_json::from_value::<DfsRepairAttemptRecord>(missing).is_err());
        }
    }
}

#[cfg(test)]
#[path = "service_maintenance/gc_path_native_tests.rs"]
mod gc_path_native_tests;
