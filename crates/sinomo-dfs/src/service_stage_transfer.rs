//! Stage-transfer request and response handling.
//!
//! Stage blob, stage patch, and commit handlers report closely related transfer
//! results. This module owns stage validation, persistence, and the shared
//! response shape; the network session owns body framing while repair/fetch/read
//! handling lives in their dedicated service modules.

use sinomo_core::node::NodeId;

use crate::*;

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn handle_stage_blob(
        &mut self,
        requester_node_id: NodeId,
        command: DfsStageBlobCommand,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let storage_result = self
            .validate_stage_blob_command(&requester_node_id, &command)
            .and_then(|()| {
                self.store_command_bytes(
                    &command.object_id,
                    &command.version,
                    &command.content_hash,
                    &command.bytes,
                )
            });
        self.finish_stage_blob(requester_node_id, command, storage_result)
    }

    pub(crate) fn handle_preingested_stage_blob(
        &mut self,
        requester_node_id: NodeId,
        command: DfsStageBlobCommand,
        blob: ContentStoreQuarantinedBlob,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let storage_result = self
            .validate_stage_blob_command(&requester_node_id, &command)
            .and_then(|()| {
                if blob.content_hash != command.content_hash {
                    return Err(DfsError::ContentHashMismatch {
                        expected: command.content_hash.clone(),
                        actual: blob.content_hash.clone(),
                    });
                }
                self.content.bind_quarantined_blob(
                    command.object_id.as_str(),
                    command.version.as_str(),
                    blob,
                )?;
                Ok(())
            });
        self.finish_stage_blob(requester_node_id, command, storage_result)
    }

    fn validate_stage_blob_command(
        &mut self,
        requester_node_id: &NodeId,
        command: &DfsStageBlobCommand,
    ) -> Result<(), DfsError> {
        self.ensure_replica_command_authorized_with_metadata(
            requester_node_id,
            &command.intent_id,
            &command.object_id,
            command.object_metadata.as_ref(),
            command.authority_shard.as_ref(),
        )
        .and_then(|()| self.ensure_replica_object_metadata(command.object_metadata.as_ref()))
        .and_then(|()| {
            self.ensure_no_conflicting_pending_replica_write(
                "stage",
                requester_node_id,
                &command.intent_id,
                &command.object_id,
                &command.version,
                &command.content_hash,
            )
        })
        .and_then(|()| {
            self.ensure_command_base_matches_current(
                "stage",
                &command.object_id,
                command.expected_base_version.as_ref(),
                &command.version,
                &command.content_hash,
            )
        })
        .and_then(|()| {
            let local_node_id = self.node_id.clone();
            self.ensure_metadata_receipt_accepts(
                &command.object_id,
                &command.version,
                &local_node_id,
                &command.content_hash,
            )
        })
    }

    fn finish_stage_blob(
        &mut self,
        requester_node_id: NodeId,
        command: DfsStageBlobCommand,
        storage_result: Result<(), DfsError>,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let result = match storage_result {
            Ok(()) => {
                let local_node_id = self.node_id.clone();
                self.put_replica_receipt_if_authoritative(
                    &command.file_system_id,
                    &command.object_id,
                    &command.version,
                    &local_node_id,
                    &command.content_hash,
                    command.object_metadata.as_ref(),
                )?;
                self.record_active_replica_stage(
                    &command.object_id,
                    &command.intent_id,
                    &command.version,
                    &command.content_hash,
                    &requester_node_id,
                );
                DfsBlobTransferResult::Durable {
                    content_hash: command.content_hash.clone(),
                }
            }
            Err(error) => DfsBlobTransferResult::Rejected {
                reason: error.to_string(),
            },
        };
        self.send_stage_blob_response(
            &requester_node_id,
            command.request_id.clone(),
            command.intent_id.clone(),
            command.file_system_id,
            command.object_id.clone(),
            command.version.clone(),
            result.clone(),
        )?;
        Ok(self.stage_blob_result_event(
            command.intent_id,
            command.object_id,
            command.version,
            result,
        ))
    }

    pub(crate) fn handle_stage_patch(
        &mut self,
        requester_node_id: NodeId,
        command: DfsStagePatchCommand,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let result = match self
            .ensure_replica_command_authorized_with_metadata(
                &requester_node_id,
                &command.intent_id,
                &command.object_id,
                command.object_metadata.as_ref(),
                command.authority_shard.as_ref(),
            )
            .and_then(|()| self.ensure_replica_object_metadata(command.object_metadata.as_ref()))
            .and_then(|()| {
                self.ensure_no_conflicting_pending_replica_write(
                    "stage-patch",
                    &requester_node_id,
                    &command.intent_id,
                    &command.object_id,
                    &command.version,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                self.ensure_command_base_matches_current(
                    "stage-patch",
                    &command.object_id,
                    Some(&command.base_version),
                    &command.version,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                let local_node_id = self.node_id.clone();
                self.ensure_metadata_receipt_accepts(
                    &command.object_id,
                    &command.version,
                    &local_node_id,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                let base_identity = self.patch_base_identity_for_current_object(
                    &command.object_id,
                    &command.base_version,
                )?;
                self.store_command_patch(
                    &command.object_id,
                    &command.base_version,
                    &command.version,
                    &command.content_hash,
                    command.offset,
                    &command.patch_hash,
                    &command.bytes,
                    base_identity.as_ref(),
                )
            }) {
            Ok(()) => {
                let local_node_id = self.node_id.clone();
                self.put_replica_receipt_if_authoritative(
                    &command.file_system_id,
                    &command.object_id,
                    &command.version,
                    &local_node_id,
                    &command.content_hash,
                    command.object_metadata.as_ref(),
                )?;
                self.record_active_replica_stage(
                    &command.object_id,
                    &command.intent_id,
                    &command.version,
                    &command.content_hash,
                    &requester_node_id,
                );
                DfsBlobTransferResult::Durable {
                    content_hash: command.content_hash.clone(),
                }
            }
            Err(error) => DfsBlobTransferResult::Rejected {
                reason: error.to_string(),
            },
        };
        self.send_stage_blob_response(
            &requester_node_id,
            command.request_id.clone(),
            command.intent_id.clone(),
            command.file_system_id,
            command.object_id.clone(),
            command.version.clone(),
            result.clone(),
        )?;
        Ok(self.stage_blob_result_event(
            command.intent_id,
            command.object_id,
            command.version,
            result,
        ))
    }

    pub(crate) fn send_stage_blob_response(
        &mut self,
        requester_node_id: &NodeId,
        request_id: String,
        intent_id: String,
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        version: DfsVersion,
        result: DfsBlobTransferResult,
    ) -> Result<(), DfsError> {
        self.send_dfs_message_if_routable(
            requester_node_id,
            DfsNetworkMessage::StageBlobResponse(DfsStageBlobResponse {
                request_id,
                intent_id,
                file_system_id,
                object_id,
                version,
                replica_node_id: self.node_id.clone(),
                result,
            }),
        )?;
        Ok(())
    }

    pub(crate) fn stage_blob_result_event(
        &self,
        intent_id: String,
        object_id: DfsObjectId,
        version: DfsVersion,
        result: DfsBlobTransferResult,
    ) -> NetworkedDfsServiceEvent {
        match result {
            DfsBlobTransferResult::Durable { content_hash }
            | DfsBlobTransferResult::Committed { content_hash } => {
                NetworkedDfsServiceEvent::BlobStaged {
                    intent_id,
                    object_id,
                    version,
                    replica_node_id: self.node_id.clone(),
                    content_hash,
                }
            }
            DfsBlobTransferResult::Rejected { reason } => {
                NetworkedDfsServiceEvent::BlobStageRejected {
                    intent_id,
                    object_id,
                    replica_node_id: self.node_id.clone(),
                    reason,
                }
            }
        }
    }

    pub(crate) fn handle_commit_version(
        &mut self,
        requester_node_id: NodeId,
        command: DfsCommitVersionCommand,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let preflight = self
            .ensure_replica_command_authorized_with_metadata(
                &requester_node_id,
                &command.intent_id,
                &command.object_id,
                command.object_metadata.as_ref(),
                command.authority_shard.as_ref(),
            )
            .and_then(|()| self.ensure_replica_object_metadata(command.object_metadata.as_ref()))
            .and_then(|()| {
                self.ensure_replica_command_hash_matches_intent(
                    &command.intent_id,
                    &command.object_id,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                self.ensure_no_conflicting_pending_replica_write(
                    "commit",
                    &requester_node_id,
                    &command.intent_id,
                    &command.object_id,
                    &command.version,
                    &command.content_hash,
                )
            });
        let commit = preflight.and_then(|()| {
            if let Err(content_error) = self.ensure_commit_version_content_available(&command) {
                return match self.transition_missing_local_commit_content_to_repair(
                    &command.object_id,
                    &command.version,
                    &command.content_hash,
                ) {
                    Ok(true) => Err(DfsError::CommitContentRepairPending {
                        object_id: command.object_id.clone(),
                        version: command.version.clone(),
                        content_hash: command.content_hash.clone(),
                    }),
                    Ok(false) => Err(content_error),
                    Err(transition_error) => Err(DfsError::Metadata {
                        message: format!(
                            "{content_error}; failed to transition stale replica receipt to repair: {transition_error}"
                        ),
                    }),
                };
            }
            self.ensure_command_base_matches_current(
                "commit",
                &command.object_id,
                command.expected_base_version.as_ref(),
                &command.version,
                &command.content_hash,
            )?;
            let local_node_id = self.node_id.clone();
            self.commit_replica_metadata_cache(
                &command.file_system_id,
                &command.object_id,
                &command.version,
                &local_node_id,
                &command.content_hash,
                command.object_metadata.as_ref(),
            )
        });
        let result = match commit {
            Ok(()) => {
                // The version is now durably committed on this replica, so the
                // in-flight stage guard for it is no longer active.
                self.clear_active_replica_stage(&command.object_id, &command.version);
                DfsBlobTransferResult::Committed {
                    content_hash: command.content_hash.clone(),
                }
            }
            Err(error) => DfsBlobTransferResult::Rejected {
                reason: error.to_string(),
            },
        };
        self.send_commit_version_response(
            &requester_node_id,
            command.request_id.clone(),
            command.intent_id.clone(),
            command.file_system_id.clone(),
            command.object_id.clone(),
            command.version.clone(),
            result.clone(),
        )?;
        Ok(self.commit_version_result_event(command, result))
    }

    fn ensure_commit_version_content_available(
        &self,
        command: &DfsCommitVersionCommand,
    ) -> Result<(), DfsError> {
        let manifest = self
            .content
            .read_manifest(command.object_id.as_str(), command.version.as_str())?;
        if manifest.content_hash != command.content_hash {
            return Err(DfsError::ContentHashMismatch {
                expected: command.content_hash.clone(),
                actual: manifest.content_hash,
            });
        }
        if !self.content.manifest_content_available(&manifest)? {
            return Err(DfsError::ContentStore {
                message: format!(
                    "committed content bytes are missing for {} {}",
                    command.object_id, command.version
                ),
            });
        }
        Ok(())
    }

    fn send_commit_version_response(
        &mut self,
        requester_node_id: &NodeId,
        request_id: String,
        intent_id: String,
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        version: DfsVersion,
        result: DfsBlobTransferResult,
    ) -> Result<(), DfsError> {
        self.send_dfs_message_if_routable(
            requester_node_id,
            DfsNetworkMessage::CommitVersionResponse(DfsCommitVersionResponse {
                request_id,
                intent_id,
                file_system_id,
                object_id,
                version,
                replica_node_id: self.node_id.clone(),
                result,
            }),
        )?;
        Ok(())
    }

    fn commit_version_result_event(
        &self,
        command: DfsCommitVersionCommand,
        result: DfsBlobTransferResult,
    ) -> NetworkedDfsServiceEvent {
        match result {
            DfsBlobTransferResult::Committed { content_hash }
            | DfsBlobTransferResult::Durable { content_hash } => {
                NetworkedDfsServiceEvent::VersionCommitted {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    version: command.version,
                    content_hash,
                }
            }
            DfsBlobTransferResult::Rejected { reason } => {
                NetworkedDfsServiceEvent::RepairRejected {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    replica_node_id: self.node_id.clone(),
                    reason,
                }
            }
        }
    }

    pub(crate) fn handle_stage_blob_response(
        &mut self,
        frame_source_node_id: NodeId,
        response: DfsStageBlobResponse,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        if frame_source_node_id != response.replica_node_id {
            let reason = format!(
                "DFS stage response source mismatch: frame source {}, payload replica {}",
                frame_source_node_id, response.replica_node_id
            );
            return Ok(Some(NetworkedDfsServiceEvent::BlobStageRejected {
                intent_id: response.intent_id,
                object_id: response.object_id,
                replica_node_id: frame_source_node_id,
                reason,
            }));
        }
        match response.result {
            DfsBlobTransferResult::Durable { ref content_hash }
            | DfsBlobTransferResult::Committed { ref content_hash } => {
                if self
                    .coordinator
                    .intent(&response.intent_id)
                    .is_some_and(|intent| {
                        matches!(
                            intent.state,
                            DfsWriteIntentState::Aborted
                                | DfsWriteIntentState::Rejected
                                | DfsWriteIntentState::Conflict
                        )
                    })
                {
                    let intent_id = response.intent_id.clone();
                    return Ok(Some(NetworkedDfsServiceEvent::BlobStageRejected {
                        intent_id: response.intent_id,
                        object_id: response.object_id,
                        replica_node_id: response.replica_node_id,
                        reason: format!("missing pending write for stage response: {intent_id}"),
                    }));
                }
                self.restore_pending_write_for_intent_if_missing(
                    &response.intent_id,
                    &response.object_id,
                )?;
                if let Err(error) =
                    self.ensure_stage_response_matches_pending(&response, content_hash)
                {
                    return Ok(Some(NetworkedDfsServiceEvent::BlobStageRejected {
                        intent_id: response.intent_id,
                        object_id: response.object_id,
                        replica_node_id: response.replica_node_id,
                        reason: error.to_string(),
                    }));
                }
                let event =
                    self.with_placement_intent_authority(&response.intent_id, |service| {
                        service.put_metadata_receipt(
                            &response.file_system_id,
                            &response.object_id,
                            &response.version,
                            &response.replica_node_id,
                            content_hash,
                        )?;
                        let intent_was_committed = service
                            .coordinator
                            .intent(&response.intent_id)
                            .is_some_and(|intent| {
                                matches!(intent.state, DfsWriteIntentState::Committed)
                            });
                        let updated_intent = service.coordinator.record_durable_receipt(
                            &response.intent_id,
                            response.replica_node_id.clone(),
                        )?;
                        let event = NetworkedDfsServiceEvent::BlobStaged {
                            intent_id: response.intent_id.clone(),
                            object_id: response.object_id.clone(),
                            version: response.version.clone(),
                            replica_node_id: response.replica_node_id.clone(),
                            content_hash: content_hash.clone(),
                        };
                        if intent_was_committed {
                            service.finalize_late_committed_stage_receipt_under_authority(
                                &updated_intent.intent_id,
                                &response.replica_node_id,
                            )?;
                        } else {
                            service
                                .try_apply_committed_intent_if_ready(&updated_intent.intent_id)?;
                        }
                        service.send_untracked_completed_routed_write_response_if_committed(
                            &updated_intent.intent_id,
                            &response.object_id,
                        )?;
                        if let Some(pending) = service.pending_writes.get_mut(&response.intent_id) {
                            pending
                                .stage_inflight_request_ids
                                .remove(&response.replica_node_id);
                            pending.stage_retries.remove(&response.replica_node_id);
                        }
                        service.cleanup_committed_pending_write_if_repaired(&response.intent_id);
                        Ok(event)
                    })?;
                Ok(Some(event.unwrap_or_else(|| {
                    NetworkedDfsServiceEvent::BlobStageRejected {
                        intent_id: response.intent_id,
                        object_id: response.object_id,
                        replica_node_id: response.replica_node_id,
                        reason: "write intent is closed or its placement policy was superseded"
                            .to_string(),
                    }
                })))
            }
            DfsBlobTransferResult::Rejected { ref reason } => {
                match self.ensure_stage_response_request_matches_pending(&response) {
                    Ok(()) => {
                        self.record_stage_patch_fallback(
                            &response.intent_id,
                            &response.replica_node_id,
                        );
                        self.record_stage_retry_scheduled(
                            &response.intent_id,
                            &response.replica_node_id,
                        );
                        let object_metadata = self.current_object_for_write(&response.object_id)?;
                        self.stage_missing_desired_replicas_for_intent(
                            &response.intent_id,
                            object_metadata,
                        )?;
                        Ok(Some(NetworkedDfsServiceEvent::BlobStageRejected {
                            intent_id: response.intent_id,
                            object_id: response.object_id,
                            replica_node_id: response.replica_node_id,
                            reason: reason.clone(),
                        }))
                    }
                    Err(error) => Ok(Some(NetworkedDfsServiceEvent::BlobStageRejected {
                        intent_id: response.intent_id,
                        object_id: response.object_id,
                        replica_node_id: response.replica_node_id,
                        reason: error.to_string(),
                    })),
                }
            }
        }
    }

    pub(crate) fn ensure_stage_response_request_matches_pending(
        &self,
        response: &DfsStageBlobResponse,
    ) -> Result<(), DfsError> {
        let Some(pending) = self.pending_writes.get(&response.intent_id) else {
            return Err(DfsError::Distributed {
                message: format!(
                    "missing pending write for stage response: {}",
                    response.intent_id
                ),
            });
        };
        let expected_request_id = pending
            .stage_inflight_request_ids
            .get(&response.replica_node_id);
        if let Some(expected_request_id) = expected_request_id {
            if expected_request_id != &response.request_id {
                return Err(DfsError::Distributed {
                    message: format!(
                        "unexpected DFS stage response request id for intent {} from {}: expected {}, actual {}",
                        response.intent_id,
                        response.replica_node_id,
                        expected_request_id,
                        response.request_id
                    ),
                });
            }
            return Ok(());
        }
        if pending
            .stage_retries
            .contains_key(&response.replica_node_id)
        {
            return Err(DfsError::Distributed {
                message: format!(
                    "expired or superseded DFS stage response for intent {} from {}: {}",
                    response.intent_id, response.replica_node_id, response.request_id
                ),
            });
        }
        if !pending.stage_inflight_request_ids.is_empty() {
            return Err(DfsError::Distributed {
                message: format!(
                    "unexpected DFS stage response source for intent {}: {}",
                    response.intent_id, response.replica_node_id
                ),
            });
        }
        Ok(())
    }

    pub(crate) fn ensure_stage_response_matches_pending(
        &self,
        response: &DfsStageBlobResponse,
        content_hash: &str,
    ) -> Result<(), DfsError> {
        self.ensure_stage_response_request_matches_pending(response)?;
        let pending = self
            .pending_writes
            .get(&response.intent_id)
            .expect("pending write validated");
        self.ensure_response_matches_pending_write(
            "stage",
            &response.intent_id,
            &response.file_system_id,
            &response.object_id,
            &response.version,
            &response.replica_node_id,
            content_hash,
            pending,
        )
    }
}
