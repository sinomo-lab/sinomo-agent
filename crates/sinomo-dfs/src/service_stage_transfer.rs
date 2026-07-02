//! Stage-transfer request and response handling.
//!
//! Stage blob, stage patch, commit, and chunked stage handlers all report
//! closely related transfer protocol results. This module owns stage request
//! paths, stage chunk assembly, and the shared response shape while
//! repair/fetch/read transfer assembly lives in their dedicated service modules.

use sinomo_core::node::NodeId;

use crate::transfer::{
    DFS_BLOB_CHUNK_BYTES, InProgressBlobTransfer, InProgressPatchTransfer, blob_transfer_key,
    validate_chunk_bytes, validate_chunk_hash,
};
use crate::*;

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn handle_stage_blob(
        &mut self,
        requester_node_id: NodeId,
        command: DfsStageBlobCommand,
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
                    "stage",
                    &requester_node_id,
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
            .and_then(|()| {
                self.store_command_bytes(
                    &command.object_id,
                    &command.version,
                    &command.content_hash,
                    &command.bytes,
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

    pub(crate) fn handle_stage_patch_chunk(
        &mut self,
        requester_node_id: NodeId,
        command: DfsStagePatchChunkCommand,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        if command.total_chunks == 0 {
            let reason = format!(
                "stage patch chunk transfer {} has no chunks",
                command.intent_id
            );
            return self.reject_stage_patch_chunk(requester_node_id, command, reason);
        }
        if let Err(error) = validate_chunk_bytes(
            "chunked DFS patch transfer",
            command.total_size_bytes,
            command.chunk_index,
            command.total_chunks,
            &command.bytes,
        ) {
            return self.reject_stage_patch_chunk(requester_node_id, command, error.to_string());
        }
        let transfer_key = blob_transfer_key(
            "stage-patch",
            &requester_node_id,
            &command.intent_id,
            &command.request_id,
        );
        if let Err(error) = validate_chunk_hash(
            "chunked DFS patch transfer",
            command.chunk_index,
            &command.bytes,
            command.chunk_hash.as_deref(),
        ) {
            self.incoming_patch_transfers.remove(&transfer_key);
            return self.reject_stage_patch_chunk(requester_node_id, command, error.to_string());
        }
        if !self.incoming_patch_transfers.contains_key(&transfer_key) {
            if let Err(error) = self.ensure_replica_command_authorized_with_metadata(
                &requester_node_id,
                &command.intent_id,
                &command.object_id,
                command.object_metadata.as_ref(),
                command.authority_shard.as_ref(),
            ) {
                return self.reject_stage_patch_chunk(
                    requester_node_id,
                    command,
                    error.to_string(),
                );
            }
            if let Err(error) =
                self.ensure_replica_object_metadata(command.object_metadata.as_ref())
            {
                return self.reject_stage_patch_chunk(
                    requester_node_id,
                    command,
                    error.to_string(),
                );
            }
            if let Err(error) = self.ensure_command_base_matches_current(
                "stage-patch",
                &command.object_id,
                Some(&command.base_version),
                &command.version,
                &command.content_hash,
            ) {
                return self.reject_stage_patch_chunk(
                    requester_node_id,
                    command,
                    error.to_string(),
                );
            }
            if let Err(error) = self.ensure_no_conflicting_pending_replica_write(
                "stage-patch",
                &requester_node_id,
                &command.intent_id,
                &command.object_id,
                &command.version,
                &command.content_hash,
            ) {
                return self.reject_stage_patch_chunk(
                    requester_node_id,
                    command,
                    error.to_string(),
                );
            }
            let local_node_id = self.node_id.clone();
            if let Err(error) = self.ensure_metadata_receipt_accepts(
                &command.object_id,
                &command.version,
                &local_node_id,
                &command.content_hash,
            ) {
                return self.reject_stage_patch_chunk(
                    requester_node_id,
                    command,
                    error.to_string(),
                );
            }
            if self
                .local_object_version_manifest_size_if_matches(
                    &command.object_id,
                    &command.version,
                    &command.content_hash,
                )
                .is_some()
            {
                let local_node_id = self.node_id.clone();
                if let Err(error) = self.put_replica_receipt_if_authoritative(
                    &command.file_system_id,
                    &command.object_id,
                    &command.version,
                    &local_node_id,
                    &command.content_hash,
                    command.object_metadata.as_ref(),
                ) {
                    return self.reject_stage_patch_chunk(
                        requester_node_id,
                        command,
                        error.to_string(),
                    );
                }
                self.record_active_replica_stage(
                    &command.object_id,
                    &command.intent_id,
                    &command.version,
                    &command.content_hash,
                    &requester_node_id,
                );
                let result = DfsBlobTransferResult::Durable {
                    content_hash: command.content_hash.clone(),
                };
                self.send_stage_blob_response(
                    &requester_node_id,
                    command.request_id,
                    command.intent_id.clone(),
                    command.file_system_id,
                    command.object_id.clone(),
                    command.version.clone(),
                    result.clone(),
                )?;
                return Ok(Some(self.stage_blob_result_event(
                    command.intent_id,
                    command.object_id,
                    command.version,
                    result,
                )));
            }
            let assembler = match self.content.begin_chunked_object_version(
                command.object_id.as_str(),
                command.version.as_str(),
                &command.patch_hash,
                command.total_size_bytes,
                command.total_chunks,
                DFS_BLOB_CHUNK_BYTES,
            ) {
                Ok(assembler) => assembler,
                Err(error) => {
                    return self.reject_stage_patch_chunk(
                        requester_node_id,
                        command,
                        error.to_string(),
                    );
                }
            };
            let transfer = InProgressPatchTransfer::new(
                command.request_id.clone(),
                command.intent_id.clone(),
                command.file_system_id.clone(),
                command.object_id.clone(),
                command.base_version.clone(),
                command.version.clone(),
                command.content_hash.clone(),
                command.offset,
                command.patch_hash.clone(),
                command.total_size_bytes,
                command.total_chunks,
                assembler,
            )?;
            self.incoming_patch_transfers
                .insert(transfer_key.clone(), transfer);
        }
        let transfer = self
            .incoming_patch_transfers
            .get_mut(&transfer_key)
            .expect("transfer inserted");
        if transfer.file_system_id != command.file_system_id
            || transfer.object_id != command.object_id
            || transfer.base_version != command.base_version
            || transfer.version != command.version
            || transfer.content_hash != command.content_hash
            || transfer.offset != command.offset
            || transfer.patch_hash != command.patch_hash
            || transfer.total_size_bytes() != command.total_size_bytes
        {
            self.incoming_patch_transfers.remove(&transfer_key);
            let reason = format!(
                "conflicting stage patch chunk metadata for {}",
                command.intent_id
            );
            return self.reject_stage_patch_chunk(requester_node_id, command, reason);
        }
        let transfer_complete = match transfer.insert_chunk(
            command.chunk_index,
            command.total_chunks,
            command.bytes.clone(),
        ) {
            Ok(true) => true,
            Ok(false) => return Ok(None),
            Err(error) => {
                self.incoming_patch_transfers.remove(&transfer_key);
                return self.reject_stage_patch_chunk(
                    requester_node_id,
                    command,
                    error.to_string(),
                );
            }
        };
        if !transfer_complete {
            return Ok(None);
        }
        let transfer = self
            .incoming_patch_transfers
            .remove(&transfer_key)
            .expect("completed transfer");
        let base_identity = self
            .patch_base_identity_for_current_object(&transfer.object_id, &transfer.base_version)?;
        let manifest = match self
            .content
            .commit_chunked_object_version_patch_with_base_identity(
                transfer.chunks.assembler,
                transfer.base_version.as_str(),
                transfer.offset,
                base_identity.as_ref(),
            ) {
            Ok(manifest) => manifest,
            Err(error) => {
                return self.reject_stage_patch_chunk(
                    requester_node_id,
                    command,
                    error.to_string(),
                );
            }
        };
        if manifest.content_hash != transfer.content_hash {
            let reason = format!(
                "stage patch manifest hash mismatch for {}: expected {}, actual {}",
                transfer.intent_id, transfer.content_hash, manifest.content_hash
            );
            return self.reject_stage_patch_chunk(requester_node_id, command, reason);
        }
        let local_node_id = self.node_id.clone();
        self.put_replica_receipt_if_authoritative(
            &transfer.file_system_id,
            &transfer.object_id,
            &transfer.version,
            &local_node_id,
            &manifest.content_hash,
            None,
        )?;
        self.record_active_replica_stage(
            &transfer.object_id,
            &transfer.intent_id,
            &transfer.version,
            &manifest.content_hash,
            &requester_node_id,
        );
        let result = DfsBlobTransferResult::Durable {
            content_hash: manifest.content_hash.clone(),
        };
        self.send_stage_blob_response(
            &requester_node_id,
            transfer.request_id,
            transfer.intent_id.clone(),
            transfer.file_system_id,
            transfer.object_id.clone(),
            transfer.version.clone(),
            result.clone(),
        )?;
        Ok(Some(self.stage_blob_result_event(
            transfer.intent_id,
            transfer.object_id,
            transfer.version,
            result,
        )))
    }

    fn reject_stage_patch_chunk(
        &mut self,
        requester_node_id: NodeId,
        command: DfsStagePatchChunkCommand,
        reason: String,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        let transfer_key = blob_transfer_key(
            "stage-patch",
            &requester_node_id,
            &command.intent_id,
            &command.request_id,
        );
        self.incoming_patch_transfers.remove(&transfer_key);
        let result = DfsBlobTransferResult::Rejected {
            reason: reason.clone(),
        };
        self.send_stage_blob_response(
            &requester_node_id,
            command.request_id,
            command.intent_id.clone(),
            command.file_system_id,
            command.object_id.clone(),
            command.version.clone(),
            result.clone(),
        )?;
        Ok(Some(self.stage_blob_result_event(
            command.intent_id,
            command.object_id,
            command.version,
            result,
        )))
    }

    pub(crate) fn handle_stage_blob_chunk(
        &mut self,
        requester_node_id: NodeId,
        command: DfsStageBlobChunkCommand,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        if command.total_chunks == 0 {
            let reason = format!(
                "stage blob chunk transfer {} has no chunks",
                command.intent_id
            );
            return self.reject_stage_blob_chunk(requester_node_id, command, reason);
        }
        if let Err(error) = validate_chunk_bytes(
            "chunked DFS blob transfer",
            command.total_size_bytes,
            command.chunk_index,
            command.total_chunks,
            &command.bytes,
        ) {
            return self.reject_stage_blob_chunk(requester_node_id, command, error.to_string());
        }
        let transfer_key = blob_transfer_key(
            "stage",
            &requester_node_id,
            &command.intent_id,
            &command.request_id,
        );
        if let Err(error) = validate_chunk_hash(
            "chunked DFS blob transfer",
            command.chunk_index,
            &command.bytes,
            command.chunk_hash.as_deref(),
        ) {
            self.incoming_blob_transfers.remove(&transfer_key);
            return self.reject_stage_blob_chunk(requester_node_id, command, error.to_string());
        }
        if !self.incoming_blob_transfers.contains_key(&transfer_key) {
            if let Err(error) = self.ensure_replica_command_authorized_with_metadata(
                &requester_node_id,
                &command.intent_id,
                &command.object_id,
                command.object_metadata.as_ref(),
                command.authority_shard.as_ref(),
            ) {
                return self.reject_stage_blob_chunk(requester_node_id, command, error.to_string());
            }
            if let Err(error) =
                self.ensure_replica_object_metadata(command.object_metadata.as_ref())
            {
                return self.reject_stage_blob_chunk(requester_node_id, command, error.to_string());
            }
            if let Err(error) = self.ensure_command_base_matches_current(
                "stage",
                &command.object_id,
                command.expected_base_version.as_ref(),
                &command.version,
                &command.content_hash,
            ) {
                return self.reject_stage_blob_chunk(requester_node_id, command, error.to_string());
            }
            if let Err(error) = self.ensure_no_conflicting_pending_replica_write(
                "stage",
                &requester_node_id,
                &command.intent_id,
                &command.object_id,
                &command.version,
                &command.content_hash,
            ) {
                return self.reject_stage_blob_chunk(requester_node_id, command, error.to_string());
            }
            let local_node_id = self.node_id.clone();
            if let Err(error) = self.ensure_metadata_receipt_accepts(
                &command.object_id,
                &command.version,
                &local_node_id,
                &command.content_hash,
            ) {
                return self.reject_stage_blob_chunk(requester_node_id, command, error.to_string());
            }
            if self.local_object_version_manifest_matches(
                &command.object_id,
                &command.version,
                &command.content_hash,
                command.total_size_bytes,
            ) {
                let local_node_id = self.node_id.clone();
                if let Err(error) = self.put_replica_receipt_if_authoritative(
                    &command.file_system_id,
                    &command.object_id,
                    &command.version,
                    &local_node_id,
                    &command.content_hash,
                    command.object_metadata.as_ref(),
                ) {
                    return self.reject_stage_blob_chunk(
                        requester_node_id,
                        command,
                        error.to_string(),
                    );
                }
                self.record_active_replica_stage(
                    &command.object_id,
                    &command.intent_id,
                    &command.version,
                    &command.content_hash,
                    &requester_node_id,
                );
                let result = DfsBlobTransferResult::Durable {
                    content_hash: command.content_hash.clone(),
                };
                self.send_stage_blob_response(
                    &requester_node_id,
                    command.request_id,
                    command.intent_id.clone(),
                    command.file_system_id,
                    command.object_id.clone(),
                    command.version.clone(),
                    result.clone(),
                )?;
                return Ok(Some(self.stage_blob_result_event(
                    command.intent_id,
                    command.object_id,
                    command.version,
                    result,
                )));
            }
            if command.chunk_index != 0 {
                let reason = format!(
                    "stage blob chunk transfer {} started at chunk {} without earlier chunks",
                    command.intent_id, command.chunk_index
                );
                return self.reject_stage_blob_chunk(requester_node_id, command, reason);
            }
            let assembler = match self.content.begin_chunked_object_version(
                command.object_id.as_str(),
                command.version.as_str(),
                &command.content_hash,
                command.total_size_bytes,
                command.total_chunks,
                DFS_BLOB_CHUNK_BYTES,
            ) {
                Ok(assembler) => assembler,
                Err(error) => {
                    return self.reject_stage_blob_chunk(
                        requester_node_id,
                        command,
                        error.to_string(),
                    );
                }
            };
            let transfer = InProgressBlobTransfer::new(
                command.request_id.clone(),
                command.intent_id.clone(),
                command.file_system_id.clone(),
                command.object_id.clone(),
                command.expected_base_version.clone(),
                command.version.clone(),
                command.content_hash.clone(),
                command.object_metadata.clone(),
                command.total_size_bytes,
                command.total_chunks,
                assembler,
            )?;
            self.incoming_blob_transfers
                .insert(transfer_key.clone(), transfer);
        }
        let transfer = self
            .incoming_blob_transfers
            .get_mut(&transfer_key)
            .expect("transfer inserted");
        if transfer.file_system_id != command.file_system_id
            || transfer.object_id != command.object_id
            || transfer.expected_base_version != command.expected_base_version
            || transfer.version != command.version
            || transfer.content_hash != command.content_hash
        {
            self.incoming_blob_transfers.remove(&transfer_key);
            let reason = format!(
                "conflicting stage blob chunk metadata for {}",
                command.intent_id
            );
            return self.reject_stage_blob_chunk(requester_node_id, command, reason);
        }
        let transfer_complete = match transfer.insert_chunk(
            command.chunk_index,
            command.total_chunks,
            command.bytes.clone(),
        ) {
            Ok(true) => true,
            Ok(false) => return Ok(None),
            Err(error) => {
                self.incoming_blob_transfers.remove(&transfer_key);
                return self.reject_stage_blob_chunk(requester_node_id, command, error.to_string());
            }
        };
        if !transfer_complete {
            return Ok(None);
        }
        let transfer = self
            .incoming_blob_transfers
            .remove(&transfer_key)
            .expect("completed transfer");
        let manifest = match self
            .content
            .commit_chunked_object_version(transfer.chunks.assembler)
        {
            Ok(manifest) => manifest,
            Err(error) => {
                return self.reject_stage_blob_chunk(requester_node_id, command, error.to_string());
            }
        };
        let local_node_id = self.node_id.clone();
        self.put_replica_receipt_if_authoritative(
            &transfer.file_system_id,
            &transfer.object_id,
            &transfer.version,
            &local_node_id,
            &manifest.content_hash,
            transfer.object_metadata.as_ref(),
        )?;
        self.record_active_replica_stage(
            &transfer.object_id,
            &transfer.intent_id,
            &transfer.version,
            &manifest.content_hash,
            &requester_node_id,
        );
        let result = DfsBlobTransferResult::Durable {
            content_hash: manifest.content_hash.clone(),
        };
        self.send_stage_blob_response(
            &requester_node_id,
            transfer.request_id,
            transfer.intent_id.clone(),
            transfer.file_system_id,
            transfer.object_id.clone(),
            transfer.version.clone(),
            result.clone(),
        )?;
        Ok(Some(self.stage_blob_result_event(
            transfer.intent_id,
            transfer.object_id,
            transfer.version,
            result,
        )))
    }

    fn reject_stage_blob_chunk(
        &mut self,
        requester_node_id: NodeId,
        command: DfsStageBlobChunkCommand,
        reason: String,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        let transfer_key = blob_transfer_key(
            "stage",
            &requester_node_id,
            &command.intent_id,
            &command.request_id,
        );
        self.incoming_blob_transfers.remove(&transfer_key);
        let result = DfsBlobTransferResult::Rejected {
            reason: reason.clone(),
        };
        self.send_stage_blob_response(
            &requester_node_id,
            command.request_id,
            command.intent_id.clone(),
            command.file_system_id,
            command.object_id.clone(),
            command.version.clone(),
            result.clone(),
        )?;
        Ok(Some(self.stage_blob_result_event(
            command.intent_id,
            command.object_id,
            command.version,
            result,
        )))
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
            })
            .and_then(|()| {
                self.content
                    .read_manifest(command.object_id.as_str(), command.version.as_str())
                    .map_err(DfsError::from)
            })
            .and_then(|manifest| {
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
            })
            .and_then(|()| {
                self.ensure_command_base_matches_current(
                    "commit",
                    &command.object_id,
                    command.expected_base_version.as_ref(),
                    &command.version,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                let local_node_id = self.node_id.clone();
                self.commit_replica_metadata_cache(
                    &command.file_system_id,
                    &command.object_id,
                    &command.version,
                    &local_node_id,
                    &command.content_hash,
                    command.object_metadata.as_ref(),
                )
            }) {
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
                self.put_metadata_receipt(
                    &response.file_system_id,
                    &response.object_id,
                    &response.version,
                    &response.replica_node_id,
                    content_hash,
                )?;
                let updated_intent = self.coordinator.record_durable_receipt(
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
                self.try_apply_committed_intent_if_ready(&updated_intent.intent_id)?;
                self.send_untracked_completed_routed_write_response_if_committed(
                    &updated_intent.intent_id,
                    &response.object_id,
                )?;
                if let Some(pending) = self.pending_writes.get_mut(&response.intent_id) {
                    pending
                        .stage_inflight_request_ids
                        .remove(&response.replica_node_id);
                    pending
                        .stage_next_chunk_indexes
                        .remove(&response.replica_node_id);
                    pending.stage_retries.remove(&response.replica_node_id);
                }
                self.cleanup_committed_pending_write_if_repaired(&response.intent_id);
                Ok(Some(event))
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
