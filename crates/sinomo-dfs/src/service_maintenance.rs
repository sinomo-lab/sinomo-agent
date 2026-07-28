//! Networked service maintenance and metadata-journal replication helpers.
//!
//! The reusable maintenance hosts live in `maintenance`; this module contains
//! the `NetworkedDfsService` methods that inspect local content, run lifecycle
//! maintenance against the service's stores, and schedule metadata journal
//! replication to configured shard replicas.

use sinomo_core::node::NodeId;

use crate::*;

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
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
        let version_gc = options
            .version_gc
            .map(|options| self.content.collect_versions(options))
            .transpose()?;
        let orphan_gc = options
            .orphan_gc
            .map(|options| self.content.collect_garbage(options))
            .transpose()?;
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
            metadata_backend_configured,
            tombstone_compaction_requested,
            tombstone_compaction,
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
                batch: batch.clone(),
            }),
        )?;
        Ok(sent.then_some(batch))
    }

    pub fn replicate_metadata_shard_to_configured_replicas(
        &mut self,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
    ) -> Result<Vec<DfsMetadataReplicationOutcome>, DfsError> {
        self.pending_metadata_replications
            .retain(|_, pending| pending.created_at.elapsed() < std::time::Duration::from_secs(60));
        let Some(control_plane) = self.control_plane.as_mut() else {
            return Ok(Vec::new());
        };
        let Some(shard) = DfsControlPlaneStore::new(control_plane.as_mut())
            .get_metadata_shard(file_system_id, shard_id)?
        else {
            return Ok(Vec::new());
        };
        if shard.primary_node_id != self.node_id {
            return Err(DfsError::StaleOwnerEpoch {
                file_system_id: shard.file_system_id,
                local_node_id: self.node_id.clone(),
                primary_node_id: shard.primary_node_id,
                epoch: shard.epoch,
            });
        }

        let authority_file_system_id = shard.file_system_id.clone();
        let authority_epoch = shard.epoch;
        let shard_id = DfsMetadataShardId::new(shard.shard_id);
        let local_node_id = self.node_id.clone();
        let mut outcomes = Vec::new();
        for target_node_id in shard
            .replica_node_ids
            .into_iter()
            .filter(|target_node_id| target_node_id != &local_node_id)
        {
            let cursor_key = (target_node_id.clone(), shard_id.clone());
            let after_sequence =
                if let Some(after_sequence) = self.metadata_replication_cursors.get(&cursor_key) {
                    *after_sequence
                } else if let Some(backend) = self.metadata_backend.as_ref() {
                    let after_sequence =
                        backend.metadata_replication_cursor(&target_node_id, &shard_id)?;
                    self.metadata_replication_cursors
                        .insert(cursor_key.clone(), after_sequence);
                    after_sequence
                } else {
                    0
                };
            let Some(batch) =
                self.metadata_journal_batch_for_shard_since(&shard_id, after_sequence)?
            else {
                outcomes.push(DfsMetadataReplicationOutcome::NoChanges {
                    target_node_id,
                    shard_id: shard_id.clone(),
                    after_sequence,
                });
                continue;
            };
            let pending_next_after_sequence = self
                .pending_metadata_replications
                .values()
                .filter(|pending| {
                    pending.target_node_id == target_node_id && pending.shard_id == shard_id
                })
                .map(|pending| pending.next_after_sequence)
                .max();
            if let Some(pending_next_after_sequence) = pending_next_after_sequence
                && pending_next_after_sequence >= batch.next_after_sequence
            {
                outcomes.push(DfsMetadataReplicationOutcome::InFlight {
                    target_node_id,
                    shard_id: shard_id.clone(),
                    next_after_sequence: pending_next_after_sequence,
                });
                continue;
            }
            let entries = batch.entries.len();
            let next_after_sequence = batch.next_after_sequence;
            let request_id = self.next_request_id();
            self.pending_metadata_replications.insert(
                request_id.clone(),
                PendingMetadataReplication {
                    target_node_id: target_node_id.clone(),
                    shard_id: shard_id.clone(),
                    next_after_sequence,
                    verify_prefix_only: false,
                    required_authority_confirmation_id: None,
                    created_at: std::time::Instant::now(),
                },
            );
            let sent = self.send_dfs_message_if_routable(
                &target_node_id,
                DfsNetworkMessage::MetadataJournalBatch(DfsMetadataJournalBatchCommand {
                    request_id: request_id.clone(),
                    source_node_id: Some(self.node_id.clone()),
                    file_system_id: Some(authority_file_system_id.clone()),
                    authority_epoch: Some(authority_epoch),
                    verify_prefix_only: false,
                    prefix_probe: None,
                    authority_probe: None,
                    batch,
                }),
            )?;
            if sent {
                outcomes.push(DfsMetadataReplicationOutcome::Sent {
                    target_node_id,
                    shard_id: shard_id.clone(),
                    entries,
                    next_after_sequence,
                });
            } else {
                self.pending_metadata_replications.remove(&request_id);
                outcomes.push(DfsMetadataReplicationOutcome::Unroutable {
                    target_node_id,
                    shard_id: shard_id.clone(),
                    after_sequence,
                });
            }
        }
        Ok(outcomes)
    }

    pub fn replicate_configured_metadata_shards(
        &mut self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Vec<DfsMetadataReplicationOutcome>, DfsError> {
        let Some(control_plane) = self.control_plane.as_mut() else {
            return Ok(Vec::new());
        };
        let shard_ids = DfsControlPlaneStore::new(control_plane.as_mut())
            .list_metadata_shards(file_system_id)?
            .into_iter()
            .filter(|shard| shard.primary_node_id == self.node_id)
            .map(|shard| shard.shard_id)
            .collect::<Vec<_>>();

        let mut outcomes = Vec::new();
        for shard_id in shard_ids {
            outcomes.extend(
                self.replicate_metadata_shard_to_configured_replicas(file_system_id, &shard_id)?,
            );
        }
        Ok(outcomes)
    }
}
