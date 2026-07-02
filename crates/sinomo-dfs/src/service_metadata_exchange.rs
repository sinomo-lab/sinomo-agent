//! In-service metadata journal and snapshot exchange.
//!
//! This module handles synchronous DFS data-plane messages that replicate
//! metadata journal batches, advance replication cursors, and install snapshot
//! repairs after a replica or routed-response mirror falls behind. The
//! background publish planner remains in `metadata_replication`.

use sinomo_core::node::NodeId;

use crate::*;

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn handle_metadata_journal_batch_response(
        &mut self,
        source_node_id: NodeId,
        response: DfsMetadataJournalBatchResponse,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        let pending = self
            .pending_metadata_replications
            .remove(&response.request_id);
        let Some(pending) = pending else {
            if let DfsMetadataJournalBatchResult::Rejected { reason } = &response.result
                && metadata_journal_rejection_allows_snapshot_repair(reason)
            {
                self.send_metadata_snapshot_repair(
                    &source_node_id,
                    &response.shard_id,
                    reason.clone(),
                )?;
            }
            return Ok(None);
        };
        if pending.target_node_id != source_node_id || pending.shard_id != response.shard_id {
            return Ok(Some(
                NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                    request_id: response.request_id,
                    source_node_id,
                    shard_id: response.shard_id,
                    reason: "metadata journal batch response does not match pending replication"
                        .to_string(),
                },
            ));
        }

        Ok(match response.result {
            DfsMetadataJournalBatchResult::Applied => {
                let cursor = self.advance_metadata_replication_cursor(
                    &source_node_id,
                    &response.shard_id,
                    response.next_after_sequence,
                    pending.next_after_sequence,
                )?;
                Some(NetworkedDfsServiceEvent::MetadataJournalBatchApplied {
                    request_id: response.request_id,
                    source_node_id,
                    shard_id: response.shard_id,
                    applied_entries: response.applied_entries,
                    next_after_sequence: cursor,
                })
            }
            DfsMetadataJournalBatchResult::Rejected { reason } => {
                if metadata_journal_rejection_allows_snapshot_repair(&reason) {
                    self.send_metadata_snapshot_repair(
                        &source_node_id,
                        &response.shard_id,
                        reason.clone(),
                    )?;
                }
                Some(NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                    request_id: response.request_id,
                    source_node_id,
                    shard_id: response.shard_id,
                    reason,
                })
            }
        })
    }

    fn advance_metadata_replication_cursor(
        &mut self,
        target_node_id: &NodeId,
        shard_id: &DfsMetadataShardId,
        next_after_sequence: u64,
        pending_next_after_sequence: u64,
    ) -> Result<u64, DfsError> {
        let cursor_key = (target_node_id.clone(), shard_id.clone());
        let cursor = self
            .metadata_replication_cursors
            .entry(cursor_key)
            .or_insert(0);
        *cursor = (*cursor)
            .max(next_after_sequence)
            .max(pending_next_after_sequence);
        if let Some(backend) = self.metadata_backend.as_mut() {
            backend.put_metadata_replication_cursor(target_node_id, shard_id, *cursor)?;
        }
        Ok(*cursor)
    }

    /// A replica whose journal forked from this primary's stream (for
    /// example a locally bootstrapped node joining as a metadata mirror) cannot
    /// apply batches; repair it by installing a compact table snapshot, the
    /// same reconciliation the standalone publisher performs.
    fn send_metadata_snapshot_repair(
        &mut self,
        target_node_id: &NodeId,
        shard_id: &DfsMetadataShardId,
        reason: String,
    ) -> Result<(), DfsError> {
        match self.metadata_shard_config_for_shard_id(shard_id)? {
            Some(shard) if shard.primary_node_id == self.node_id => {}
            _ => return Ok(()),
        }
        let next_after_sequence = self
            .metadata_journal_last_sequence_for_shard(shard_id)?
            .unwrap_or(0);
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(());
        };
        let Some(snapshot) = backend.export_shard_table_snapshot(shard_id)? else {
            return Ok(());
        };
        let command = DfsMetadataSnapshotInstallCommand {
            request_id: format!(
                "dfs_meta_snapshot_{}_{}_{}",
                self.node_id, target_node_id, next_after_sequence
            ),
            source_node_id: self.node_id.clone(),
            shard_id: shard_id.clone(),
            next_after_sequence,
            snapshot,
            reason,
        };
        let request_id = command.request_id.clone();
        let sent = self.send_dfs_message_if_routable(
            target_node_id,
            DfsNetworkMessage::MetadataSnapshotInstall(command),
        )?;
        if sent {
            self.pending_metadata_replications.insert(
                request_id,
                PendingMetadataReplication {
                    target_node_id: target_node_id.clone(),
                    shard_id: shard_id.clone(),
                    next_after_sequence,
                },
            );
        }
        Ok(())
    }

    /// Resolves a shard configuration by shard id across the filesystems this
    /// service knows about.
    fn metadata_shard_config_for_shard_id(
        &mut self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataShardConfig>, DfsError> {
        if self.control_plane.is_none() {
            return Ok(None);
        }
        let file_system_ids: Vec<DfsFileSystemId> =
            self.metadata.filesystems.keys().cloned().collect();
        for file_system_id in file_system_ids {
            let kv = self
                .control_plane
                .as_mut()
                .expect("control plane checked above");
            if let Some(shard) = DfsControlPlaneStore::new(kv.as_mut())
                .get_metadata_shard(&file_system_id, &shard_id.0)
                .map_err(DfsError::from)?
            {
                return Ok(Some(shard));
            }
        }
        Ok(None)
    }

    /// Replica side of in-service snapshot repair: validates that the sender
    /// is the shard's configured primary and that this node is a configured
    /// replica, replaces the local shard state with the snapshot, and
    /// rehydrates in-memory service state from the replaced backend.
    pub(crate) fn handle_metadata_snapshot_install(
        &mut self,
        source_node_id: NodeId,
        command: DfsMetadataSnapshotInstallCommand,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let request_id = command.request_id.clone();
        let shard_id = command.shard_id.clone();
        let next_after_sequence = command.next_after_sequence;
        let rejection = if command.source_node_id != source_node_id {
            Some("metadata snapshot source does not match transport source".to_string())
        } else {
            match self.metadata_shard_config_for_shard_id(&shard_id)? {
                None if self.control_plane.is_some() => Some(format!(
                    "metadata snapshot install has no configured shard {}",
                    shard_id.0
                )),
                Some(shard) if shard.primary_node_id != source_node_id => Some(format!(
                    "metadata snapshot source {source_node_id} is not shard {} primary {}",
                    shard.shard_id, shard.primary_node_id
                )),
                Some(shard) if shard.primary_node_id == self.node_id => {
                    Some("metadata primary must not install a replica snapshot".to_string())
                }
                // Non-replica metadata mirrors accept primary-sourced snapshot
                // repair too; their journals are fed by routed-response and
                // SyncPath batches and can need the same divergence repair as
                // configured replicas.
                _ => None,
            }
        };
        let result = if let Some(reason) = rejection {
            DfsMetadataSnapshotInstallResult::Rejected { reason }
        } else if let Some(backend) = self.metadata_backend.as_mut() {
            match backend.replace_with_replicated_snapshot(
                shard_id.clone(),
                next_after_sequence,
                command.snapshot,
            ) {
                Ok(archived_existing) => {
                    // The backend was replaced wholesale; in-memory state
                    // must follow it.
                    self.hydrate_from_metadata_backend()?;
                    DfsMetadataSnapshotInstallResult::Installed { archived_existing }
                }
                Err(error) => DfsMetadataSnapshotInstallResult::Rejected {
                    reason: error.to_string(),
                },
            }
        } else {
            DfsMetadataSnapshotInstallResult::Rejected {
                reason: "local DFS metadata backend is not configured".to_string(),
            }
        };
        let installed = matches!(result, DfsMetadataSnapshotInstallResult::Installed { .. });
        let reason = match &result {
            DfsMetadataSnapshotInstallResult::Rejected { reason } => Some(reason.clone()),
            DfsMetadataSnapshotInstallResult::Installed { .. } => None,
        };
        let _ = self.send_dfs_message_if_routable(
            &source_node_id,
            DfsNetworkMessage::MetadataSnapshotInstallResponse(
                DfsMetadataSnapshotInstallResponse {
                    request_id: request_id.clone(),
                    shard_id: shard_id.clone(),
                    next_after_sequence,
                    result,
                },
            ),
        )?;
        Ok(if installed {
            NetworkedDfsServiceEvent::MetadataSnapshotInstalled {
                request_id,
                source_node_id,
                shard_id,
                next_after_sequence,
            }
        } else {
            NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                request_id,
                source_node_id,
                shard_id,
                reason: reason.unwrap_or_else(|| "snapshot install rejected".to_string()),
            }
        })
    }

    /// Primary side of in-service snapshot repair: a successful install
    /// advances the replica's replication cursor to the snapshot head.
    pub(crate) fn handle_metadata_snapshot_install_response(
        &mut self,
        source_node_id: NodeId,
        response: DfsMetadataSnapshotInstallResponse,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        let Some(pending) = self
            .pending_metadata_replications
            .remove(&response.request_id)
        else {
            return Ok(None);
        };
        if pending.target_node_id != source_node_id || pending.shard_id != response.shard_id {
            return Ok(Some(
                NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                    request_id: response.request_id,
                    source_node_id,
                    shard_id: response.shard_id,
                    reason: "metadata snapshot response does not match pending replication"
                        .to_string(),
                },
            ));
        }
        Ok(match response.result {
            DfsMetadataSnapshotInstallResult::Installed { .. } => {
                let cursor = self.advance_metadata_replication_cursor(
                    &source_node_id,
                    &response.shard_id,
                    response.next_after_sequence,
                    pending.next_after_sequence,
                )?;
                Some(NetworkedDfsServiceEvent::MetadataSnapshotInstalled {
                    request_id: response.request_id,
                    source_node_id,
                    shard_id: response.shard_id,
                    next_after_sequence: cursor,
                })
            }
            DfsMetadataSnapshotInstallResult::Rejected { reason } => {
                Some(NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                    request_id: response.request_id,
                    source_node_id,
                    shard_id: response.shard_id,
                    reason,
                })
            }
        })
    }

    pub(crate) fn metadata_journal_batch_since(
        &self,
        after_sequence: u64,
    ) -> Result<Option<DfsMetadataJournalBatch>, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(None);
        };
        backend
            .journal_batch_since_limited(
                after_sequence,
                DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
                DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
            )
            .map_err(Into::into)
    }

    pub(crate) fn metadata_journal_batch_for_shard_since(
        &self,
        shard_id: &DfsMetadataShardId,
        after_sequence: u64,
    ) -> Result<Option<DfsMetadataJournalBatch>, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(None);
        };
        backend
            .journal_batch_for_shard_since_limited(
                shard_id,
                after_sequence,
                DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
                DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
            )
            .map_err(Into::into)
    }

    pub(crate) fn metadata_journal_last_sequence_for_shard(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Option<u64>, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(None);
        };
        Ok(backend
            .metadata_shard_statuses()?
            .into_iter()
            .find(|status| &status.shard_id == shard_id)
            .map(|status| status.journal_last_sequence))
    }

    pub(crate) fn handle_metadata_journal_batch(
        &mut self,
        source_node_id: NodeId,
        command: DfsMetadataJournalBatchCommand,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let shard_id = command.batch.shard_id.clone();
        let next_after_sequence = command.batch.next_after_sequence;
        let source_rejection = if command
            .source_node_id
            .as_ref()
            .is_some_and(|claimed| claimed != &source_node_id)
        {
            Some("metadata journal batch source does not match transport source".to_string())
        } else {
            self.validate_metadata_journal_batch_source(&source_node_id, &command.batch)?
        };
        if let Some(reason) = source_rejection {
            self.send_metadata_journal_batch_rejection(
                &source_node_id,
                &command.request_id,
                &shard_id,
                next_after_sequence,
                &reason,
            )?;
            return Ok(NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                request_id: command.request_id,
                source_node_id,
                shard_id,
                reason,
            });
        }
        let Some(backend) = self.metadata_backend.as_mut() else {
            let reason = "local DFS metadata backend is not configured".to_string();
            self.send_metadata_journal_batch_rejection(
                &source_node_id,
                &command.request_id,
                &shard_id,
                next_after_sequence,
                &reason,
            )?;
            return Ok(NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                request_id: command.request_id,
                source_node_id,
                shard_id,
                reason,
            });
        };

        match backend.apply_journal_batch(&command.batch) {
            Ok(applied_entries) => {
                self.apply_metadata_journal_batch_to_service_state(&command.batch)?;
                let _ = self.send_dfs_message_if_routable(
                    &source_node_id,
                    DfsNetworkMessage::MetadataJournalBatchResponse(
                        DfsMetadataJournalBatchResponse {
                            request_id: command.request_id.clone(),
                            shard_id: shard_id.clone(),
                            next_after_sequence,
                            applied_entries,
                            result: DfsMetadataJournalBatchResult::Applied,
                        },
                    ),
                )?;
                Ok(NetworkedDfsServiceEvent::MetadataJournalBatchApplied {
                    request_id: command.request_id,
                    source_node_id,
                    shard_id,
                    applied_entries,
                    next_after_sequence,
                })
            }
            Err(error) => {
                let reason = error.to_string();
                self.send_metadata_journal_batch_rejection(
                    &source_node_id,
                    &command.request_id,
                    &shard_id,
                    next_after_sequence,
                    &reason,
                )?;
                Ok(NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                    request_id: command.request_id,
                    source_node_id,
                    shard_id,
                    reason,
                })
            }
        }
    }

    fn send_metadata_journal_batch_rejection(
        &mut self,
        source_node_id: &NodeId,
        request_id: &str,
        shard_id: &DfsMetadataShardId,
        next_after_sequence: u64,
        reason: &str,
    ) -> Result<(), DfsError> {
        self.send_dfs_message_if_routable(
            source_node_id,
            DfsNetworkMessage::MetadataJournalBatchResponse(DfsMetadataJournalBatchResponse {
                request_id: request_id.to_string(),
                shard_id: shard_id.clone(),
                next_after_sequence,
                applied_entries: 0,
                result: DfsMetadataJournalBatchResult::Rejected {
                    reason: reason.to_string(),
                },
            }),
        )?;
        Ok(())
    }

    fn validate_metadata_journal_batch_source(
        &mut self,
        source_node_id: &NodeId,
        batch: &DfsMetadataJournalBatch,
    ) -> Result<Option<String>, DfsError> {
        let Some(control_plane) = self.control_plane.as_mut() else {
            return Ok(None);
        };
        let Some(file_system_id) = metadata_journal_batch_file_system_id(batch) else {
            return Ok(None);
        };
        let Some(shard) = DfsControlPlaneStore::new(control_plane.as_mut())
            .get_metadata_shard(&file_system_id, &batch.shard_id.0)?
        else {
            return Ok(Some(format!(
                "metadata journal batch has no configured shard {}",
                batch.shard_id.0
            )));
        };
        if shard.primary_node_id != *source_node_id {
            return Ok(Some(format!(
                "metadata journal source {source_node_id} is not shard {} primary {}",
                shard.shard_id, shard.primary_node_id
            )));
        }
        if shard.primary_node_id == self.node_id {
            return Ok(Some(
                "metadata primary must not apply a replica journal batch".to_string(),
            ));
        }
        // Routed operations still accept primary-sourced journal catch-up
        // batches so requesters keep read-your-write even for custom shards
        // where they are not durable replicas.
        Ok(None)
    }
}
