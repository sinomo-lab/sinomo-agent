//! DFS data-plane transfer message handlers.
//!
//! This module handles inbound stage, patch, commit, repair, fetch, bulk-read,
//! and read-response messages. It intentionally keeps transfer validation and
//! response assembly close together while write intent state machines and retry
//! scheduling remain in the service modules that own those lifecycles.

use std::io::Read;

use sinomo_node::NodeId;

use crate::transfer::{
    DFS_BLOB_CHUNK_BYTES, InProgressBlobTransfer, InProgressFetchBlobTransfer,
    InProgressPatchTransfer, InProgressReadMemoryTransfer, InProgressReadTransfer,
    PendingNetworkedWrite, blob_chunk_count, blob_chunk_count_for_size, blob_chunk_len,
    blob_transfer_key, chunk_hash, chunk_prefix_bytes, dfs_byte_count_to_usize,
    fetch_response_window_end, range_transfer_size_bytes, read_response_window_range_len,
    validate_chunk_bytes, validate_chunk_hash, validate_chunked_transfer_shape,
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
        self.send_dfs_message_if_routable(
            &requester_node_id,
            DfsNetworkMessage::StageBlobResponse(DfsStageBlobResponse {
                request_id: command.request_id.clone(),
                intent_id: command.intent_id.clone(),
                file_system_id: command.file_system_id,
                object_id: command.object_id.clone(),
                version: command.version.clone(),
                replica_node_id: self.node_id.clone(),
                result: result.clone(),
            }),
        )?;
        Ok(match result {
            DfsBlobTransferResult::Durable { content_hash } => {
                NetworkedDfsServiceEvent::BlobStaged {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    version: command.version,
                    replica_node_id: self.node_id.clone(),
                    content_hash,
                }
            }
            DfsBlobTransferResult::Committed { content_hash } => {
                NetworkedDfsServiceEvent::BlobStaged {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    version: command.version,
                    replica_node_id: self.node_id.clone(),
                    content_hash,
                }
            }
            DfsBlobTransferResult::Rejected { reason } => {
                NetworkedDfsServiceEvent::BlobStageRejected {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    replica_node_id: self.node_id.clone(),
                    reason,
                }
            }
        })
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
        self.send_dfs_message_if_routable(
            &requester_node_id,
            DfsNetworkMessage::StageBlobResponse(DfsStageBlobResponse {
                request_id: command.request_id.clone(),
                intent_id: command.intent_id.clone(),
                file_system_id: command.file_system_id,
                object_id: command.object_id.clone(),
                version: command.version.clone(),
                replica_node_id: self.node_id.clone(),
                result: result.clone(),
            }),
        )?;
        Ok(match result {
            DfsBlobTransferResult::Durable { content_hash }
            | DfsBlobTransferResult::Committed { content_hash } => {
                NetworkedDfsServiceEvent::BlobStaged {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    version: command.version,
                    replica_node_id: self.node_id.clone(),
                    content_hash,
                }
            }
            DfsBlobTransferResult::Rejected { reason } => {
                NetworkedDfsServiceEvent::BlobStageRejected {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    replica_node_id: self.node_id.clone(),
                    reason,
                }
            }
        })
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
                self.send_dfs_message_if_routable(
                    &requester_node_id,
                    DfsNetworkMessage::StageBlobResponse(DfsStageBlobResponse {
                        request_id: command.request_id,
                        intent_id: command.intent_id.clone(),
                        file_system_id: command.file_system_id,
                        object_id: command.object_id.clone(),
                        version: command.version.clone(),
                        replica_node_id: self.node_id.clone(),
                        result: DfsBlobTransferResult::Durable {
                            content_hash: command.content_hash.clone(),
                        },
                    }),
                )?;
                return Ok(Some(NetworkedDfsServiceEvent::BlobStaged {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    version: command.version,
                    replica_node_id: self.node_id.clone(),
                    content_hash: command.content_hash,
                }));
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
        self.send_dfs_message_if_routable(
            &requester_node_id,
            DfsNetworkMessage::StageBlobResponse(DfsStageBlobResponse {
                request_id: transfer.request_id,
                intent_id: transfer.intent_id.clone(),
                file_system_id: transfer.file_system_id,
                object_id: transfer.object_id.clone(),
                version: transfer.version.clone(),
                replica_node_id: self.node_id.clone(),
                result: DfsBlobTransferResult::Durable {
                    content_hash: manifest.content_hash.clone(),
                },
            }),
        )?;
        Ok(Some(NetworkedDfsServiceEvent::BlobStaged {
            intent_id: transfer.intent_id,
            object_id: transfer.object_id,
            version: transfer.version,
            replica_node_id: self.node_id.clone(),
            content_hash: manifest.content_hash,
        }))
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
        self.send_dfs_message_if_routable(
            &requester_node_id,
            DfsNetworkMessage::StageBlobResponse(DfsStageBlobResponse {
                request_id: command.request_id,
                intent_id: command.intent_id.clone(),
                file_system_id: command.file_system_id,
                object_id: command.object_id.clone(),
                version: command.version,
                replica_node_id: self.node_id.clone(),
                result: DfsBlobTransferResult::Rejected {
                    reason: reason.clone(),
                },
            }),
        )?;
        Ok(Some(NetworkedDfsServiceEvent::BlobStageRejected {
            intent_id: command.intent_id,
            object_id: command.object_id,
            replica_node_id: self.node_id.clone(),
            reason,
        }))
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
                self.send_dfs_message_if_routable(
                    &requester_node_id,
                    DfsNetworkMessage::StageBlobResponse(DfsStageBlobResponse {
                        request_id: command.request_id,
                        intent_id: command.intent_id.clone(),
                        file_system_id: command.file_system_id,
                        object_id: command.object_id.clone(),
                        version: command.version.clone(),
                        replica_node_id: self.node_id.clone(),
                        result: DfsBlobTransferResult::Durable {
                            content_hash: command.content_hash.clone(),
                        },
                    }),
                )?;
                return Ok(Some(NetworkedDfsServiceEvent::BlobStaged {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    version: command.version,
                    replica_node_id: self.node_id.clone(),
                    content_hash: command.content_hash,
                }));
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
        self.send_dfs_message_if_routable(
            &requester_node_id,
            DfsNetworkMessage::StageBlobResponse(DfsStageBlobResponse {
                request_id: transfer.request_id,
                intent_id: transfer.intent_id.clone(),
                file_system_id: transfer.file_system_id,
                object_id: transfer.object_id.clone(),
                version: transfer.version.clone(),
                replica_node_id: self.node_id.clone(),
                result: DfsBlobTransferResult::Durable {
                    content_hash: manifest.content_hash.clone(),
                },
            }),
        )?;
        Ok(Some(NetworkedDfsServiceEvent::BlobStaged {
            intent_id: transfer.intent_id,
            object_id: transfer.object_id,
            version: transfer.version,
            replica_node_id: self.node_id.clone(),
            content_hash: manifest.content_hash,
        }))
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
        self.send_dfs_message_if_routable(
            &requester_node_id,
            DfsNetworkMessage::StageBlobResponse(DfsStageBlobResponse {
                request_id: command.request_id,
                intent_id: command.intent_id.clone(),
                file_system_id: command.file_system_id,
                object_id: command.object_id.clone(),
                version: command.version,
                replica_node_id: self.node_id.clone(),
                result: DfsBlobTransferResult::Rejected {
                    reason: reason.clone(),
                },
            }),
        )?;
        Ok(Some(NetworkedDfsServiceEvent::BlobStageRejected {
            intent_id: command.intent_id,
            object_id: command.object_id,
            replica_node_id: self.node_id.clone(),
            reason,
        }))
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
                // in-flight stage guard for it is no longer "active". Clearing it
                // prevents a stale entry from rejecting a later write of a new
                // version of the same object to this replica.
                self.clear_active_replica_stage(&command.object_id, &command.version);
                DfsBlobTransferResult::Committed {
                    content_hash: command.content_hash.clone(),
                }
            }
            Err(error) => DfsBlobTransferResult::Rejected {
                reason: error.to_string(),
            },
        };
        self.send_dfs_message_if_routable(
            &requester_node_id,
            DfsNetworkMessage::CommitVersionResponse(DfsCommitVersionResponse {
                request_id: command.request_id,
                intent_id: command.intent_id.clone(),
                file_system_id: command.file_system_id,
                object_id: command.object_id.clone(),
                version: command.version.clone(),
                replica_node_id: self.node_id.clone(),
                result: result.clone(),
            }),
        )?;
        Ok(match result {
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
        })
    }

    fn ensure_command_base_matches_current(
        &self,
        operation: &str,
        object_id: &DfsObjectId,
        expected_base_version: Option<&DfsVersion>,
        version: &DfsVersion,
        content_hash: &str,
    ) -> Result<(), DfsError> {
        let current = if let Some(backend) = self.metadata_backend.as_ref() {
            backend.get_object(object_id)?
        } else {
            self.metadata.object(object_id).cloned()
        };
        let Some(current) = current else {
            return Ok(());
        };
        if current.version == *version
            && let Some(current_hash) = current.content_hash.as_deref()
            && current_hash == content_hash
        {
            return Ok(());
        }
        if let Some(expected_base_version) = expected_base_version
            && &current.version != expected_base_version
        {
            // Content is addressed by hash, so a repair/stage carrying the exact
            // bytes the object *currently* references is never stale in a harmful
            // way — even when the version label advanced past `version`. This
            // happens when a metadata-only change bumps the version (chmod, chown,
            // utimes, hardlink/rename) while leaving content unchanged: the
            // content repair queued for the older version would otherwise be
            // rejected here as a stale base, dropping the only copy of the
            // content-addressed blob the replica needs to serve the current
            // version. Accept it; a genuinely stale write carries a different hash
            // and is still rejected below.
            if current.content_hash.as_deref() != Some(content_hash) {
                return Err(DfsError::Distributed {
                    message: format!(
                        "stale DFS {operation} base for {object_id}: expected {}, current {}, command {}",
                        expected_base_version, current.version, version
                    ),
                });
            }
        }
        if current.version == *version
            && let Some(current_hash) = current.content_hash.as_deref()
            && current_hash != content_hash
        {
            return Err(DfsError::ContentHashMismatch {
                expected: current_hash.to_string(),
                actual: content_hash.to_string(),
            });
        }
        Ok(())
    }

    fn patch_base_identity_for_current_object(
        &self,
        object_id: &DfsObjectId,
        base_version: &DfsVersion,
    ) -> Result<Option<ContentVersionIdentity>, DfsError> {
        let current = if let Some(backend) = self.metadata_backend.as_ref() {
            backend.get_object(object_id)?
        } else {
            self.metadata.object(object_id).cloned()
        };
        Ok(current
            .as_ref()
            .filter(|object| object.version == *base_version)
            .and_then(object_content_identity))
    }

    fn ensure_no_conflicting_pending_replica_write(
        &self,
        operation: &str,
        requester_node_id: &NodeId,
        intent_id: &str,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &str,
    ) -> Result<(), DfsError> {
        if let Some(stage) = self.active_replica_stages.get(object_id) {
            if stage.intent_id != intent_id
                && (stage.version != *version || stage.content_hash != content_hash)
            {
                return Err(DfsError::Distributed {
                    message: format!(
                        "conflicting active DFS {operation} for {object_id}: local stage {} from {} targets {} ({}) while remote intent {intent_id} from {} targets {} ({})",
                        stage.intent_id,
                        stage.requester_node_id,
                        stage.version,
                        stage.content_hash,
                        requester_node_id,
                        version,
                        content_hash
                    ),
                });
            }
        }
        for (pending_intent_id, pending) in &self.pending_writes {
            if pending.object_id != *object_id {
                continue;
            }
            let Some(intent) = self.coordinator.intent(pending_intent_id) else {
                continue;
            };
            if !matches!(intent.state, DfsWriteIntentState::AwaitingReceipts) {
                continue;
            }
            if pending_intent_id == intent_id && intent.requesting_node_id == *requester_node_id {
                continue;
            }
            if pending.version == *version && pending.content_hash == content_hash {
                continue;
            }
            return Err(DfsError::Distributed {
                message: format!(
                    "conflicting pending DFS {operation} for {object_id}: local intent {} from {} targets {} ({}) while remote intent {intent_id} from {} targets {} ({})",
                    pending_intent_id,
                    intent.requesting_node_id,
                    pending.version,
                    pending.content_hash,
                    requester_node_id,
                    version,
                    content_hash
                ),
            });
        }
        Ok(())
    }

    fn ensure_stage_response_request_matches_pending(
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

    fn ensure_stage_response_matches_pending(
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

    pub(crate) fn handle_repair_replica(
        &mut self,
        requester_node_id: NodeId,
        command: DfsRepairReplicaCommand,
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
                    "repair",
                    &requester_node_id,
                    &command.intent_id,
                    &command.object_id,
                    &command.version,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                self.ensure_command_base_matches_current(
                    "repair",
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
                match self.commit_replica_metadata_cache(
                    &command.file_system_id,
                    &command.object_id,
                    &command.version,
                    &local_node_id,
                    &command.content_hash,
                    command.object_metadata.as_ref(),
                ) {
                    Ok(()) => DfsBlobTransferResult::Committed {
                        content_hash: command.content_hash.clone(),
                    },
                    Err(error) => DfsBlobTransferResult::Rejected {
                        reason: error.to_string(),
                    },
                }
            }
            Err(error) => DfsBlobTransferResult::Rejected {
                reason: error.to_string(),
            },
        };
        self.send_dfs_message_if_routable(
            &requester_node_id,
            DfsNetworkMessage::RepairReplicaResponse(DfsRepairReplicaResponse {
                request_id: command.request_id,
                intent_id: command.intent_id.clone(),
                file_system_id: command.file_system_id,
                object_id: command.object_id.clone(),
                version: command.version.clone(),
                replica_node_id: self.node_id.clone(),
                result: result.clone(),
            }),
        )?;
        Ok(match result {
            DfsBlobTransferResult::Committed { content_hash }
            | DfsBlobTransferResult::Durable { content_hash } => {
                NetworkedDfsServiceEvent::ReplicaRepaired {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    version: command.version,
                    replica_node_id: self.node_id.clone(),
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
        })
    }

    fn ensure_repair_response_matches_pending(
        &self,
        response: &DfsRepairReplicaResponse,
        content_hash: &str,
    ) -> Result<(), DfsError> {
        self.ensure_repair_response_request_matches_pending(response)?;
        let pending = self
            .pending_writes
            .get(&response.intent_id)
            .expect("pending write validated");
        self.ensure_response_matches_pending_write(
            "repair",
            &response.intent_id,
            &response.file_system_id,
            &response.object_id,
            &response.version,
            &response.replica_node_id,
            content_hash,
            pending,
        )
    }

    fn ensure_repair_response_request_matches_pending(
        &self,
        response: &DfsRepairReplicaResponse,
    ) -> Result<(), DfsError> {
        let Some(pending) = self.pending_writes.get(&response.intent_id) else {
            return Err(DfsError::Distributed {
                message: format!(
                    "missing pending write for repair response: {}",
                    response.intent_id
                ),
            });
        };
        let expected_request_id = pending
            .repair_inflight_request_ids
            .get(&response.replica_node_id);
        if let Some(expected_request_id) = expected_request_id {
            if expected_request_id != &response.request_id {
                return Err(DfsError::Distributed {
                    message: format!(
                        "unexpected DFS repair response request id for intent {} from {}: expected {}, actual {}",
                        response.intent_id,
                        response.replica_node_id,
                        expected_request_id,
                        response.request_id
                    ),
                });
            }
            return Ok(());
        }
        if !pending.repair_inflight_request_ids.is_empty() {
            return Err(DfsError::Distributed {
                message: format!(
                    "unexpected DFS repair response source for intent {}: {}",
                    response.intent_id, response.replica_node_id
                ),
            });
        }
        if self.coordinator.repair_jobs().iter().any(|job| {
            job.intent_id == response.intent_id
                && job.missing_replica_node_id == response.replica_node_id
        }) {
            return Ok(());
        }
        Err(DfsError::Distributed {
            message: format!(
                "unexpected DFS repair response source for intent {}: {}",
                response.intent_id, response.replica_node_id
            ),
        })
    }

    fn ensure_response_matches_pending_write(
        &self,
        label: &str,
        intent_id: &str,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
        content_hash: &str,
        pending: &PendingNetworkedWrite,
    ) -> Result<(), DfsError> {
        let intent = self
            .coordinator
            .intent(intent_id)
            .ok_or_else(|| DfsError::Distributed {
                message: format!("unknown DFS {label} response intent: {intent_id}"),
            })?;
        if &intent.file_system_id != file_system_id
            || &intent.object_id != object_id
            || &intent.target_version != version
        {
            return Err(DfsError::Distributed {
                message: format!("DFS {label} response metadata does not match intent {intent_id}"),
            });
        }
        if !intent.desired_replica_nodes.contains(replica_node_id) {
            return Err(DfsError::Distributed {
                message: format!(
                    "DFS {label} response from undesired replica {replica_node_id} for intent {intent_id}"
                ),
            });
        }
        if &pending.object_id != object_id
            || &pending.version != version
            || pending.content_hash != content_hash
        {
            return Err(DfsError::ContentHashMismatch {
                expected: pending.content_hash.clone(),
                actual: content_hash.to_string(),
            });
        }
        Ok(())
    }

    pub(crate) fn handle_repair_replica_chunk(
        &mut self,
        requester_node_id: NodeId,
        command: DfsRepairReplicaChunkCommand,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        if command.total_chunks == 0 {
            let reason = format!(
                "repair replica chunk transfer {} has no chunks",
                command.intent_id
            );
            return self.reject_repair_replica_chunk(requester_node_id, command, reason);
        }
        if let Err(error) = validate_chunk_bytes(
            "chunked DFS blob transfer",
            command.total_size_bytes,
            command.chunk_index,
            command.total_chunks,
            &command.bytes,
        ) {
            return self.reject_repair_replica_chunk(requester_node_id, command, error.to_string());
        }
        let transfer_key = blob_transfer_key(
            "repair",
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
            return self.reject_repair_replica_chunk(requester_node_id, command, error.to_string());
        }
        if !self.incoming_blob_transfers.contains_key(&transfer_key) {
            if let Err(error) = self.ensure_replica_command_authorized_with_metadata(
                &requester_node_id,
                &command.intent_id,
                &command.object_id,
                command.object_metadata.as_ref(),
                command.authority_shard.as_ref(),
            ) {
                return self.reject_repair_replica_chunk(
                    requester_node_id,
                    command,
                    error.to_string(),
                );
            }
            if let Err(error) = self
                .ensure_replica_object_metadata(command.object_metadata.as_ref())
                .and_then(|()| {
                    self.ensure_replica_command_hash_matches_intent(
                        &command.intent_id,
                        &command.object_id,
                        &command.content_hash,
                    )
                })
                .and_then(|()| {
                    self.ensure_no_conflicting_pending_replica_write(
                        "repair",
                        &requester_node_id,
                        &command.intent_id,
                        &command.object_id,
                        &command.version,
                        &command.content_hash,
                    )
                })
                .and_then(|()| {
                    self.ensure_command_base_matches_current(
                        "repair",
                        &command.object_id,
                        command.expected_base_version.as_ref(),
                        &command.version,
                        &command.content_hash,
                    )
                })
            {
                return self.reject_repair_replica_chunk(
                    requester_node_id,
                    command,
                    error.to_string(),
                );
            }
            if self.local_object_version_manifest_matches(
                &command.object_id,
                &command.version,
                &command.content_hash,
                command.total_size_bytes,
            ) {
                let local_node_id = self.node_id.clone();
                self.commit_replica_metadata_cache(
                    &command.file_system_id,
                    &command.object_id,
                    &command.version,
                    &local_node_id,
                    &command.content_hash,
                    command.object_metadata.as_ref(),
                )?;
                self.send_dfs_message_if_routable(
                    &requester_node_id,
                    DfsNetworkMessage::RepairReplicaResponse(DfsRepairReplicaResponse {
                        request_id: command.request_id,
                        intent_id: command.intent_id.clone(),
                        file_system_id: command.file_system_id,
                        object_id: command.object_id.clone(),
                        version: command.version.clone(),
                        replica_node_id: self.node_id.clone(),
                        result: DfsBlobTransferResult::Committed {
                            content_hash: command.content_hash.clone(),
                        },
                    }),
                )?;
                return Ok(Some(NetworkedDfsServiceEvent::ReplicaRepaired {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    version: command.version,
                    replica_node_id: self.node_id.clone(),
                    content_hash: command.content_hash,
                }));
            }
            if command.chunk_index != 0 {
                let reason = format!(
                    "repair replica chunk transfer {} started at chunk {} without earlier chunks",
                    command.intent_id, command.chunk_index
                );
                return self.reject_repair_replica_chunk(requester_node_id, command, reason);
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
                    return self.reject_repair_replica_chunk(
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
                "conflicting repair replica chunk metadata for {}",
                command.intent_id
            );
            return self.reject_repair_replica_chunk(requester_node_id, command, reason);
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
                return self.reject_repair_replica_chunk(
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
            .incoming_blob_transfers
            .remove(&transfer_key)
            .expect("completed transfer");
        let manifest = match self
            .content
            .commit_chunked_object_version(transfer.chunks.assembler)
        {
            Ok(manifest) => manifest,
            Err(error) => {
                return self.reject_repair_replica_chunk(
                    requester_node_id,
                    command,
                    error.to_string(),
                );
            }
        };
        let local_node_id = self.node_id.clone();
        self.commit_replica_metadata_cache(
            &transfer.file_system_id,
            &transfer.object_id,
            &transfer.version,
            &local_node_id,
            &manifest.content_hash,
            transfer.object_metadata.as_ref(),
        )?;
        self.send_dfs_message_if_routable(
            &requester_node_id,
            DfsNetworkMessage::RepairReplicaResponse(DfsRepairReplicaResponse {
                request_id: transfer.request_id,
                intent_id: transfer.intent_id.clone(),
                file_system_id: transfer.file_system_id,
                object_id: transfer.object_id.clone(),
                version: transfer.version.clone(),
                replica_node_id: self.node_id.clone(),
                result: DfsBlobTransferResult::Committed {
                    content_hash: manifest.content_hash.clone(),
                },
            }),
        )?;
        Ok(Some(NetworkedDfsServiceEvent::ReplicaRepaired {
            intent_id: transfer.intent_id,
            object_id: transfer.object_id,
            version: transfer.version,
            replica_node_id: self.node_id.clone(),
            content_hash: manifest.content_hash,
        }))
    }

    fn reject_repair_replica_chunk(
        &mut self,
        requester_node_id: NodeId,
        command: DfsRepairReplicaChunkCommand,
        reason: String,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        let transfer_key = blob_transfer_key(
            "repair",
            &requester_node_id,
            &command.intent_id,
            &command.request_id,
        );
        self.incoming_blob_transfers.remove(&transfer_key);
        self.send_dfs_message_if_routable(
            &requester_node_id,
            DfsNetworkMessage::RepairReplicaResponse(DfsRepairReplicaResponse {
                request_id: command.request_id,
                intent_id: command.intent_id.clone(),
                file_system_id: command.file_system_id,
                object_id: command.object_id.clone(),
                version: command.version,
                replica_node_id: self.node_id.clone(),
                result: DfsBlobTransferResult::Rejected {
                    reason: reason.clone(),
                },
            }),
        )?;
        Ok(Some(NetworkedDfsServiceEvent::RepairRejected {
            intent_id: command.intent_id,
            object_id: command.object_id,
            replica_node_id: self.node_id.clone(),
            reason,
        }))
    }

    pub(crate) fn handle_repair_replica_response(
        &mut self,
        frame_source_node_id: NodeId,
        response: DfsRepairReplicaResponse,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        if frame_source_node_id != response.replica_node_id {
            let reason = format!(
                "DFS repair response source mismatch: frame source {}, payload replica {}",
                frame_source_node_id, response.replica_node_id
            );
            return Ok(NetworkedDfsServiceEvent::RepairRejected {
                intent_id: response.intent_id,
                object_id: response.object_id,
                replica_node_id: frame_source_node_id,
                reason,
            });
        }
        self.restore_pending_write_for_intent_if_missing(&response.intent_id, &response.object_id)?;
        match response.result {
            DfsBlobTransferResult::Committed { ref content_hash }
            | DfsBlobTransferResult::Durable { ref content_hash } => {
                if let Err(error) =
                    self.ensure_repair_response_matches_pending(&response, content_hash)
                {
                    return Ok(NetworkedDfsServiceEvent::RepairRejected {
                        intent_id: response.intent_id,
                        object_id: response.object_id,
                        replica_node_id: response.replica_node_id,
                        reason: error.to_string(),
                    });
                }
                self.put_metadata_receipt(
                    &response.file_system_id,
                    &response.object_id,
                    &response.version,
                    &response.replica_node_id,
                    content_hash,
                )?;
                self.coordinator.record_durable_receipt(
                    &response.intent_id,
                    response.replica_node_id.clone(),
                )?;
                self.mark_repaired_location(
                    &response.object_id,
                    &response.version,
                    &response.replica_node_id,
                    content_hash,
                )?;
                self.record_repair_metadata_settle(&response.intent_id, &response.replica_node_id);
                self.cleanup_committed_pending_write_if_repaired(&response.intent_id);
                Ok(NetworkedDfsServiceEvent::ReplicaRepaired {
                    intent_id: response.intent_id,
                    object_id: response.object_id,
                    version: response.version,
                    replica_node_id: response.replica_node_id,
                    content_hash: content_hash.clone(),
                })
            }
            DfsBlobTransferResult::Rejected { ref reason } => {
                match self.ensure_repair_response_request_matches_pending(&response) {
                    Ok(()) => {
                        if repair_rejection_indicates_lost_authority(reason) {
                            self.suppress_repair_retries_for_lost_authority(
                                &response.intent_id,
                                Some(&response.replica_node_id),
                                false,
                            );
                        } else if repair_rejection_indicates_stale_chunk_resume(reason) {
                            self.record_repair_resume_reset(
                                &response.intent_id,
                                &response.replica_node_id,
                            );
                        } else {
                            self.record_repair_retry_scheduled(
                                &response.intent_id,
                                &response.replica_node_id,
                            );
                        }
                        Ok(NetworkedDfsServiceEvent::RepairRejected {
                            intent_id: response.intent_id,
                            object_id: response.object_id,
                            replica_node_id: response.replica_node_id,
                            reason: reason.clone(),
                        })
                    }
                    Err(error) => Ok(NetworkedDfsServiceEvent::RepairRejected {
                        intent_id: response.intent_id,
                        object_id: response.object_id,
                        replica_node_id: response.replica_node_id,
                        reason: error.to_string(),
                    }),
                }
            }
        }
    }

    pub(crate) fn handle_fetch_blob(
        &mut self,
        requester_node_id: NodeId,
        command: DfsFetchBlobCommand,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let open_result = self
            .ensure_blob_fetch_authorized(
                &requester_node_id,
                &command.intent_id,
                &command.object_id,
                command.authority_shard.as_ref(),
            )
            .and_then(|()| {
                self.ensure_replica_command_hash_matches_intent(
                    &command.intent_id,
                    &command.object_id,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                self.open_content_hash_stream(
                    &command.object_id,
                    &command.version,
                    &command.content_hash,
                    None,
                )
            });

        let mut sent_chunked_response = false;
        let result = match open_result {
            Ok((total_size_bytes, reader)) => {
                if total_size_bytes > DFS_BLOB_INLINE_LIMIT_BYTES as u64 {
                    let reader = if command.start_chunk_index == 0 {
                        reader
                    } else {
                        self.open_fetch_response_chunk_reader(total_size_bytes, &command)?
                    };
                    self.send_fetch_blob_response_chunks_from_reader(
                        &requester_node_id,
                        &command,
                        reader,
                        total_size_bytes,
                    )?;
                    sent_chunked_response = true;
                    DfsFetchBlobResult::Found {
                        content_hash: command.content_hash.clone(),
                        bytes: Vec::new(),
                    }
                } else if command.start_chunk_index > 0 {
                    match self.open_fetch_response_chunk_reader(total_size_bytes, &command) {
                        Ok(reader) => {
                            self.send_fetch_blob_response_chunks_from_reader(
                                &requester_node_id,
                                &command,
                                reader,
                                total_size_bytes,
                            )?;
                            sent_chunked_response = true;
                            DfsFetchBlobResult::Found {
                                content_hash: command.content_hash.clone(),
                                bytes: Vec::new(),
                            }
                        }
                        Err(error) => DfsFetchBlobResult::Rejected {
                            reason: error.to_string(),
                        },
                    }
                } else {
                    let mut reader = reader;
                    match read_small_blob_from_reader(
                        &command.content_hash,
                        total_size_bytes,
                        &mut reader,
                    ) {
                        Ok(bytes) => DfsFetchBlobResult::Found {
                            content_hash: command.content_hash.clone(),
                            bytes,
                        },
                        Err(error) => DfsFetchBlobResult::Rejected {
                            reason: error.to_string(),
                        },
                    }
                }
            }
            Err(DfsError::ContentStore { .. }) => DfsFetchBlobResult::NotFound,
            Err(error) => DfsFetchBlobResult::Rejected {
                reason: error.to_string(),
            },
        };
        match &result {
            DfsFetchBlobResult::Found { .. } if sent_chunked_response => {}
            _ => {
                self.send_dfs_message_if_routable(
                    &requester_node_id,
                    DfsNetworkMessage::FetchBlobResponse(DfsFetchBlobResponse {
                        request_id: command.request_id,
                        intent_id: command.intent_id.clone(),
                        object_id: command.object_id.clone(),
                        version: command.version.clone(),
                        source_node_id: self.node_id.clone(),
                        result: result.clone(),
                    }),
                )?;
            }
        }
        Ok(match result {
            DfsFetchBlobResult::Found { content_hash, .. } => {
                NetworkedDfsServiceEvent::BlobFetched {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    version: command.version,
                    source_node_id: self.node_id.clone(),
                    content_hash,
                }
            }
            DfsFetchBlobResult::NotFound => NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id: command.intent_id,
                object_id: command.object_id,
                source_node_id: self.node_id.clone(),
                reason: "blob not found".to_string(),
            },
            DfsFetchBlobResult::Rejected { reason } => {
                NetworkedDfsServiceEvent::BlobFetchRejected {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    source_node_id: self.node_id.clone(),
                    reason,
                }
            }
        })
    }

    #[cfg(test)]
    pub(crate) fn send_fetch_blob_response_chunks(
        &mut self,
        requester_node_id: &NodeId,
        command: &DfsFetchBlobCommand,
        bytes: &[u8],
    ) -> Result<(), DfsError> {
        let total_chunks = blob_chunk_count(bytes)?;
        let end_chunk_index =
            fetch_response_window_end(command.start_chunk_index, total_chunks, command.max_chunks);
        for (chunk_index, chunk) in bytes
            .chunks(DFS_BLOB_CHUNK_BYTES)
            .enumerate()
            .skip(command.start_chunk_index as usize)
            .take(end_chunk_index.saturating_sub(command.start_chunk_index) as usize)
        {
            let sent = self.send_dfs_message_if_routable(
                requester_node_id,
                DfsNetworkMessage::FetchBlobResponseChunk(DfsFetchBlobResponseChunk {
                    request_id: command.request_id.clone(),
                    intent_id: command.intent_id.clone(),
                    object_id: command.object_id.clone(),
                    version: command.version.clone(),
                    source_node_id: self.node_id.clone(),
                    content_hash: command.content_hash.clone(),
                    total_size_bytes: bytes.len() as u64,
                    chunk_index: chunk_index as u32,
                    total_chunks,
                    chunk_hash: Some(chunk_hash(chunk)),
                    bytes: chunk.to_vec(),
                }),
            )?;
            if !sent {
                break;
            }
        }
        Ok(())
    }

    fn send_fetch_blob_response_chunks_from_reader(
        &mut self,
        requester_node_id: &NodeId,
        command: &DfsFetchBlobCommand,
        reader: ObjectVersionReader,
        total_size_bytes: u64,
    ) -> Result<(), DfsError> {
        let source_node_id = self.node_id.clone();
        self.send_object_version_response_chunks_from_reader(
            requester_node_id,
            command.start_chunk_index,
            command.max_chunks,
            total_size_bytes,
            &command.content_hash,
            reader,
            |chunk_index, total_chunks, bytes| {
                DfsNetworkMessage::FetchBlobResponseChunk(DfsFetchBlobResponseChunk {
                    request_id: command.request_id.clone(),
                    intent_id: command.intent_id.clone(),
                    object_id: command.object_id.clone(),
                    version: command.version.clone(),
                    source_node_id: source_node_id.clone(),
                    content_hash: command.content_hash.clone(),
                    total_size_bytes,
                    chunk_index,
                    total_chunks,
                    chunk_hash: Some(chunk_hash(&bytes)),
                    bytes,
                })
            },
        )
    }

    fn open_fetch_response_chunk_reader(
        &self,
        total_size_bytes: u64,
        command: &DfsFetchBlobCommand,
    ) -> Result<ObjectVersionReader, DfsError> {
        let total_chunks = blob_chunk_count_for_size(total_size_bytes)?;
        if command.start_chunk_index >= total_chunks {
            return Err(DfsError::Distributed {
                message: format!(
                    "DFS fetch resume chunk {} is outside total chunks {} for {} {}",
                    command.start_chunk_index, total_chunks, command.object_id, command.version
                ),
            });
        }
        let start_offset = u64::from(command.start_chunk_index) * DFS_BLOB_CHUNK_BYTES as u64;
        let remaining_bytes = total_size_bytes.saturating_sub(start_offset);
        let remaining_bytes = usize::try_from(remaining_bytes).map_err(|_| {
            DfsError::Distributed {
                message: format!(
                    "DFS fetch response range is too large to fit in memory window: {remaining_bytes} bytes"
                ),
            }
        })?;
        self.open_content_hash_range_stream(
            &command.object_id,
            &command.version,
            &command.content_hash,
            Some(total_size_bytes),
            start_offset,
            remaining_bytes,
        )
    }

    pub(crate) fn fetch_window_end_chunk_index(
        start_chunk_index: u32,
        max_chunks: u32,
    ) -> Option<u32> {
        (max_chunks > 0).then(|| start_chunk_index.saturating_add(max_chunks))
    }

    pub(crate) fn read_window_end_chunk_index(
        start_chunk_index: u32,
        max_chunks: u32,
    ) -> Option<u32> {
        (max_chunks > 0).then(|| start_chunk_index.saturating_add(max_chunks))
    }

    pub(crate) fn handle_fetch_blob_response(
        &mut self,
        frame_source_node_id: NodeId,
        response: DfsFetchBlobResponse,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        if frame_source_node_id != response.source_node_id {
            let reason = format!(
                "DFS fetch response source mismatch: frame source {}, payload source {}",
                frame_source_node_id, response.source_node_id
            );
            return Ok(NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id: response.intent_id,
                object_id: response.object_id,
                source_node_id: frame_source_node_id,
                reason,
            });
        }
        self.restore_pending_write_for_intent_if_missing(&response.intent_id, &response.object_id)?;
        let Some(pending) = self.pending_writes.get(&response.intent_id).cloned() else {
            let reason = format!(
                "distributed DFS error: missing pending write for fetched blob: {}",
                response.intent_id
            );
            return Ok(NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id: response.intent_id,
                object_id: response.object_id,
                source_node_id: response.source_node_id,
                reason,
            });
        };
        let restored_fetch_response =
            self.restored_fetch_response_source_is_authorized_without_inflight(
                &response.intent_id,
                &frame_source_node_id,
            ) && pending.fetch_inflight_from.is_empty()
                && pending.fetch_inflight_request_ids.is_empty();
        if !pending.fetch_inflight_from.contains(&frame_source_node_id) && !restored_fetch_response
        {
            return Ok(NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id: response.intent_id,
                object_id: response.object_id,
                source_node_id: frame_source_node_id,
                reason: "unexpected DFS fetch response source".to_string(),
            });
        }
        if let Some(expected_request_id) = pending
            .fetch_inflight_request_ids
            .get(&frame_source_node_id)
        {
            if expected_request_id != &response.request_id {
                return Ok(NetworkedDfsServiceEvent::BlobFetchRejected {
                    intent_id: response.intent_id,
                    object_id: response.object_id,
                    source_node_id: frame_source_node_id,
                    reason: "unexpected DFS fetch response request id".to_string(),
                });
            }
        } else if !restored_fetch_response {
            return Ok(NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id: response.intent_id,
                object_id: response.object_id,
                source_node_id: frame_source_node_id,
                reason: "unexpected DFS fetch response request id".to_string(),
            });
        }
        if let Some(pending) = self.pending_writes.get_mut(&response.intent_id) {
            pending.fetch_inflight_from.remove(&frame_source_node_id);
            pending
                .fetch_inflight_request_ids
                .remove(&frame_source_node_id);
            pending
                .fetch_inflight_started_ticks
                .remove(&frame_source_node_id);
        }
        match response.result {
            DfsFetchBlobResult::Found {
                content_hash,
                bytes,
            } => {
                if pending.object_id != response.object_id
                    || pending.version != response.version
                    || pending.content_hash != content_hash
                {
                    self.record_fetch_failed_source_and_wake_retries(
                        &response.intent_id,
                        &response.source_node_id,
                    );
                    let reason = DfsError::ContentHashMismatch {
                        expected: pending.content_hash,
                        actual: content_hash,
                    }
                    .to_string();
                    return Ok(NetworkedDfsServiceEvent::BlobFetchRejected {
                        intent_id: response.intent_id,
                        object_id: response.object_id,
                        source_node_id: response.source_node_id,
                        reason,
                    });
                }
                if let Some(local_size_bytes) = self.local_object_version_manifest_size_if_matches(
                    &response.object_id,
                    &response.version,
                    &content_hash,
                ) {
                    self.record_fetched_pending_write_size(
                        &response.intent_id,
                        &response.object_id,
                        &response.version,
                        &content_hash,
                        local_size_bytes,
                    )?;
                    self.record_local_pending_write_receipt_if_desired(
                        &response.intent_id,
                        &response.object_id,
                        &response.version,
                        &content_hash,
                    )?;
                    self.wake_pending_retries_after_fetch_success(&response.intent_id);
                    return Ok(NetworkedDfsServiceEvent::BlobFetched {
                        intent_id: response.intent_id,
                        object_id: response.object_id,
                        version: response.version,
                        source_node_id: frame_source_node_id,
                        content_hash,
                    });
                }
                if let Err(error) = self.store_command_bytes(
                    &response.object_id,
                    &response.version,
                    &content_hash,
                    &bytes,
                ) {
                    self.record_fetch_failed_source_and_wake_retries(
                        &response.intent_id,
                        &response.source_node_id,
                    );
                    return Ok(NetworkedDfsServiceEvent::BlobFetchRejected {
                        intent_id: response.intent_id,
                        object_id: response.object_id,
                        source_node_id: response.source_node_id,
                        reason: error.to_string(),
                    });
                }
                self.record_fetched_pending_write_size(
                    &response.intent_id,
                    &response.object_id,
                    &response.version,
                    &content_hash,
                    bytes.len() as u64,
                )?;
                self.record_local_pending_write_receipt_if_desired(
                    &response.intent_id,
                    &response.object_id,
                    &response.version,
                    &content_hash,
                )?;
                self.wake_pending_retries_after_fetch_success(&response.intent_id);
                Ok(NetworkedDfsServiceEvent::BlobFetched {
                    intent_id: response.intent_id,
                    object_id: response.object_id,
                    version: response.version,
                    source_node_id: frame_source_node_id,
                    content_hash,
                })
            }
            DfsFetchBlobResult::NotFound => {
                self.record_fetch_failed_source_and_wake_retries(
                    &response.intent_id,
                    &response.source_node_id,
                );
                Ok(NetworkedDfsServiceEvent::BlobFetchRejected {
                    intent_id: response.intent_id,
                    object_id: response.object_id,
                    source_node_id: response.source_node_id,
                    reason: "blob not found".to_string(),
                })
            }
            DfsFetchBlobResult::Rejected { reason } => {
                if repair_rejection_indicates_lost_authority(&reason) {
                    self.suppress_repair_retries_for_lost_authority(
                        &response.intent_id,
                        None,
                        false,
                    );
                } else {
                    self.record_fetch_failed_source_and_wake_retries(
                        &response.intent_id,
                        &response.source_node_id,
                    );
                }
                Ok(NetworkedDfsServiceEvent::BlobFetchRejected {
                    intent_id: response.intent_id,
                    object_id: response.object_id,
                    source_node_id: response.source_node_id,
                    reason,
                })
            }
        }
    }

    fn restored_fetch_response_source_is_authorized_without_inflight(
        &self,
        intent_id: &str,
        source_node_id: &NodeId,
    ) -> bool {
        let Some(intent) = self.coordinator.intent(intent_id) else {
            return false;
        };
        intent.desired_replica_nodes.contains(source_node_id)
            && self
                .coordinator
                .receipts_for_intent(intent_id)
                .iter()
                .any(|receipt| receipt.replica_node_id == *source_node_id)
    }

    pub(crate) fn handle_fetch_blob_response_chunk(
        &mut self,
        frame_source_node_id: NodeId,
        chunk: DfsFetchBlobResponseChunk,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        if frame_source_node_id != chunk.source_node_id {
            let reason = format!(
                "DFS fetch response source mismatch: frame source {}, payload source {}",
                frame_source_node_id, chunk.source_node_id
            );
            return Ok(Some(NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id: chunk.intent_id,
                object_id: chunk.object_id,
                source_node_id: frame_source_node_id,
                reason,
            }));
        }
        self.restore_pending_write_for_intent_if_missing(&chunk.intent_id, &chunk.object_id)?;
        let Some(pending) = self.pending_writes.get(&chunk.intent_id).cloned() else {
            let reason = format!(
                "distributed DFS error: missing pending write for fetched blob chunk: {}",
                chunk.intent_id
            );
            return Ok(Some(NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id: chunk.intent_id,
                object_id: chunk.object_id,
                source_node_id: chunk.source_node_id,
                reason,
            }));
        };
        let restored_fetch_response =
            self.restored_fetch_response_source_is_authorized_without_inflight(
                &chunk.intent_id,
                &frame_source_node_id,
            ) && pending.fetch_inflight_from.is_empty()
                && pending.fetch_inflight_request_ids.is_empty();
        if !pending.fetch_inflight_from.contains(&frame_source_node_id) && !restored_fetch_response
        {
            return Ok(Some(NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id: chunk.intent_id,
                object_id: chunk.object_id,
                source_node_id: frame_source_node_id,
                reason: "unexpected DFS fetch response source".to_string(),
            }));
        }
        if let Some(expected_request_id) = pending
            .fetch_inflight_request_ids
            .get(&frame_source_node_id)
        {
            if expected_request_id != &chunk.request_id {
                return Ok(Some(NetworkedDfsServiceEvent::BlobFetchRejected {
                    intent_id: chunk.intent_id,
                    object_id: chunk.object_id,
                    source_node_id: frame_source_node_id,
                    reason: "unexpected DFS fetch response request id".to_string(),
                }));
            }
        } else if !restored_fetch_response {
            return Ok(Some(NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id: chunk.intent_id,
                object_id: chunk.object_id,
                source_node_id: frame_source_node_id,
                reason: "unexpected DFS fetch response request id".to_string(),
            }));
        }
        if pending.object_id != chunk.object_id
            || pending.version != chunk.version
            || pending.content_hash != chunk.content_hash
        {
            let reason = DfsError::ContentHashMismatch {
                expected: pending.content_hash,
                actual: chunk.content_hash.clone(),
            }
            .to_string();
            return self.reject_fetch_response_chunk_and_fail_source(chunk, reason);
        }
        if let Some(local_size_bytes) = self.local_object_version_manifest_size_if_matches(
            &chunk.object_id,
            &chunk.version,
            &chunk.content_hash,
        ) {
            self.record_fetched_pending_write_size(
                &chunk.intent_id,
                &chunk.object_id,
                &chunk.version,
                &chunk.content_hash,
                local_size_bytes,
            )?;
            self.record_local_pending_write_receipt_if_desired(
                &chunk.intent_id,
                &chunk.object_id,
                &chunk.version,
                &chunk.content_hash,
            )?;
            self.clear_fetch_response_transfer_state(
                &chunk.intent_id,
                &chunk.source_node_id,
                &chunk.request_id,
            );
            self.wake_pending_retries_after_fetch_success(&chunk.intent_id);
            return Ok(Some(NetworkedDfsServiceEvent::BlobFetched {
                intent_id: chunk.intent_id,
                object_id: chunk.object_id,
                version: chunk.version,
                source_node_id: frame_source_node_id,
                content_hash: chunk.content_hash,
            }));
        }
        if chunk.total_chunks == 0 {
            let reason = format!(
                "chunked DFS fetch response {} has no chunks",
                chunk.request_id
            );
            return self.reject_fetch_response_chunk_and_fail_source(chunk, reason);
        }
        if pending.size_bytes != 0 && chunk.total_size_bytes != pending.size_bytes {
            let reason = format!(
                "chunked DFS fetch response size does not match pending write: expected {}, actual {}",
                pending.size_bytes, chunk.total_size_bytes
            );
            return self.reject_fetch_response_chunk_and_fail_source(chunk, reason);
        }
        if let Err(error) = validate_chunk_bytes(
            "chunked DFS fetch response",
            chunk.total_size_bytes,
            chunk.chunk_index,
            chunk.total_chunks,
            &chunk.bytes,
        )
        .and_then(|()| {
            validate_chunk_hash(
                "chunked DFS fetch response",
                chunk.chunk_index,
                &chunk.bytes,
                chunk.chunk_hash.as_deref(),
            )
        }) {
            self.incoming_fetch_transfers.remove(&chunk.request_id);
            return self.reject_fetch_response_chunk_and_fail_source(chunk, error.to_string());
        }

        if !self
            .incoming_fetch_transfers
            .contains_key(&chunk.request_id)
        {
            let assembler = match self.content.begin_chunked_object_version(
                chunk.object_id.as_str(),
                chunk.version.as_str(),
                &chunk.content_hash,
                chunk.total_size_bytes,
                chunk.total_chunks,
                DFS_BLOB_CHUNK_BYTES,
            ) {
                Ok(assembler) => assembler,
                Err(error) => {
                    return self
                        .reject_fetch_response_chunk_and_fail_source(chunk, error.to_string());
                }
            };
            let transfer = match InProgressFetchBlobTransfer::new(
                chunk.object_id.clone(),
                chunk.version.clone(),
                chunk.content_hash.clone(),
                chunk.total_size_bytes,
                chunk.total_chunks,
                assembler,
            ) {
                Ok(transfer) => transfer,
                Err(error) => {
                    return self
                        .reject_fetch_response_chunk_and_fail_source(chunk, error.to_string());
                }
            };
            self.incoming_fetch_transfers
                .insert(chunk.request_id.clone(), transfer);
        }
        let transfer = self
            .incoming_fetch_transfers
            .get_mut(&chunk.request_id)
            .expect("transfer inserted");
        if transfer.object_id != chunk.object_id
            || transfer.version != chunk.version
            || transfer.content_hash != chunk.content_hash
            || transfer.total_size_bytes() != chunk.total_size_bytes
        {
            let reason = format!(
                "conflicting DFS fetch response chunk metadata for {}",
                chunk.request_id
            );
            return self.reject_fetch_response_chunk_and_fail_source(chunk, reason);
        }
        let request_id = chunk.request_id.clone();
        let intent_id = chunk.intent_id.clone();
        let object_id = chunk.object_id.clone();
        let version = chunk.version.clone();
        let source_node_id = chunk.source_node_id.clone();
        let content_hash = chunk.content_hash.clone();
        let transfer_complete =
            match transfer.insert_chunk(chunk.chunk_index, chunk.total_chunks, chunk.bytes.clone())
            {
                Ok(true) => true,
                Ok(false) => false,
                Err(error) => {
                    return self
                        .reject_fetch_response_chunk_and_fail_source(chunk, error.to_string());
                }
            };
        let next_chunk_index = transfer.contiguous_received_chunk_count();
        if let Some(pending) = self.pending_writes.get_mut(&intent_id) {
            let entry = pending
                .fetch_next_chunk_indexes
                .entry(source_node_id.clone())
                .or_insert(0);
            *entry = (*entry).max(next_chunk_index);
        }
        if !transfer_complete {
            self.request_next_fetch_window_if_due(
                &intent_id,
                &source_node_id,
                next_chunk_index,
                chunk.total_chunks,
            )?;
            return Ok(None);
        }
        let transfer = self
            .incoming_fetch_transfers
            .remove(&request_id)
            .expect("completed fetch transfer");
        if let Some(pending) = self.pending_writes.get_mut(&intent_id) {
            pending.fetch_inflight_from.remove(&source_node_id);
            pending.fetch_inflight_request_ids.remove(&source_node_id);
            pending.fetch_inflight_started_ticks.remove(&source_node_id);
            pending.fetch_next_chunk_indexes.remove(&source_node_id);
            pending
                .fetch_window_end_chunk_indexes
                .remove(&source_node_id);
        }
        if let Some(local_size_bytes) =
            self.local_object_version_manifest_size_if_matches(&object_id, &version, &content_hash)
        {
            self.record_fetched_pending_write_size(
                &intent_id,
                &object_id,
                &version,
                &content_hash,
                local_size_bytes,
            )?;
            self.record_local_pending_write_receipt_if_desired(
                &intent_id,
                &object_id,
                &version,
                &content_hash,
            )?;
            self.wake_pending_retries_after_fetch_success(&intent_id);
            return Ok(Some(NetworkedDfsServiceEvent::BlobFetched {
                intent_id,
                object_id,
                version,
                source_node_id,
                content_hash,
            }));
        }
        let manifest = match self
            .content
            .commit_chunked_object_version(transfer.chunks.assembler)
        {
            Ok(manifest) => manifest,
            Err(error) => {
                self.record_fetch_failed_source_and_wake_retries(&intent_id, &source_node_id);
                return Ok(Some(NetworkedDfsServiceEvent::BlobFetchRejected {
                    intent_id,
                    object_id,
                    source_node_id,
                    reason: error.to_string(),
                }));
            }
        };
        if manifest.content_hash != content_hash {
            self.record_fetch_failed_source_and_wake_retries(&intent_id, &source_node_id);
            return Ok(Some(NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id,
                object_id,
                source_node_id,
                reason: DfsError::ContentHashMismatch {
                    expected: content_hash,
                    actual: manifest.content_hash,
                }
                .to_string(),
            }));
        }
        self.record_fetched_pending_write_size(
            &intent_id,
            &object_id,
            &version,
            &manifest.content_hash,
            manifest.size_bytes,
        )?;
        self.record_local_pending_write_receipt_if_desired(
            &intent_id,
            &object_id,
            &version,
            &manifest.content_hash,
        )?;
        self.wake_pending_retries_after_fetch_success(&intent_id);
        Ok(Some(NetworkedDfsServiceEvent::BlobFetched {
            intent_id,
            object_id,
            version,
            source_node_id,
            content_hash: manifest.content_hash,
        }))
    }

    fn reject_fetch_response_chunk_and_fail_source(
        &mut self,
        chunk: DfsFetchBlobResponseChunk,
        reason: String,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        self.clear_fetch_response_transfer_state(
            &chunk.intent_id,
            &chunk.source_node_id,
            &chunk.request_id,
        );
        self.record_fetch_failed_source_and_wake_retries(&chunk.intent_id, &chunk.source_node_id);
        Ok(Some(NetworkedDfsServiceEvent::BlobFetchRejected {
            intent_id: chunk.intent_id,
            object_id: chunk.object_id,
            source_node_id: chunk.source_node_id,
            reason,
        }))
    }

    fn record_fetched_pending_write_size(
        &mut self,
        intent_id: &str,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &str,
        size_bytes: u64,
    ) -> Result<(), DfsError> {
        if let Some(pending) = self.pending_writes.get_mut(intent_id)
            && pending.size_bytes == 0
        {
            pending.size_bytes = size_bytes;
        }
        if let Some(backend) = self.metadata_backend.as_mut()
            && let Some(mut version_record) = backend.get_version(object_id, version)?
            && version_record.content_hash.as_deref() == Some(content_hash)
            && version_record.size_bytes != Some(size_bytes)
        {
            version_record.size_bytes = Some(size_bytes);
            backend.put_version(&version_record)?;
        }
        Ok(())
    }

    pub(crate) fn record_local_pending_write_receipt_if_desired(
        &mut self,
        intent_id: &str,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &str,
    ) -> Result<(), DfsError> {
        let Some(intent) = self.coordinator.intent(intent_id).cloned() else {
            return Ok(());
        };
        if !intent.desired_replica_nodes.contains(&self.node_id) {
            return Ok(());
        }
        if self
            .coordinator
            .receipts_for_intent(intent_id)
            .iter()
            .any(|receipt| receipt.replica_node_id == self.node_id)
        {
            return Ok(());
        }
        if self
            .local_object_version_manifest_size_if_matches(object_id, version, content_hash)
            .is_none()
        {
            return Ok(());
        }
        let local_node_id = self.node_id.clone();
        self.put_metadata_receipt(
            &intent.file_system_id,
            object_id,
            version,
            &local_node_id,
            content_hash,
        )?;
        self.coordinator
            .record_durable_receipt(intent_id, local_node_id)?;
        self.try_apply_committed_intent_if_ready(intent_id)?;
        self.send_untracked_completed_routed_write_response_if_committed(intent_id, object_id)?;
        self.cleanup_committed_pending_write_if_repaired(intent_id);
        Ok(())
    }

    fn wake_pending_retries_after_fetch_success(&mut self, intent_id: &str) {
        let stale_request_ids = self
            .pending_writes
            .get(intent_id)
            .map(|pending| {
                pending
                    .fetch_inflight_request_ids
                    .values()
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        for request_id in stale_request_ids {
            self.incoming_fetch_transfers.remove(&request_id);
        }
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            for state in pending.repair_retries.values_mut() {
                wake_repair_retry_state(state);
                state.inflight = false;
            }
            for state in pending.stage_retries.values_mut() {
                state.next_retry_tick = 0;
                state.inflight = false;
            }
            pending.fetch_inflight_from.clear();
            pending.fetch_inflight_request_ids.clear();
            pending.fetch_inflight_started_ticks.clear();
            pending.fetch_next_chunk_indexes.clear();
            pending.fetch_window_end_chunk_indexes.clear();
            pending.fetch_failed_from.clear();
        }
    }

    fn clear_fetch_response_transfer_state(
        &mut self,
        intent_id: &str,
        source_node_id: &NodeId,
        request_id: &str,
    ) {
        self.incoming_fetch_transfers.remove(request_id);
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            pending.fetch_inflight_from.remove(source_node_id);
            pending.fetch_inflight_request_ids.remove(source_node_id);
            pending.fetch_inflight_started_ticks.remove(source_node_id);
            pending.fetch_next_chunk_indexes.remove(source_node_id);
            pending
                .fetch_window_end_chunk_indexes
                .remove(source_node_id);
        }
    }

    pub(crate) fn record_fetch_failed_source_and_wake_retries(
        &mut self,
        intent_id: &str,
        source_node_id: &NodeId,
    ) {
        let stale_request_id = self
            .pending_writes
            .get_mut(intent_id)
            .and_then(|pending| pending.fetch_inflight_request_ids.remove(source_node_id));
        if let Some(request_id) = stale_request_id {
            self.incoming_fetch_transfers.remove(&request_id);
        }
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            pending.fetch_inflight_from.remove(source_node_id);
            pending.fetch_inflight_started_ticks.remove(source_node_id);
            pending.fetch_next_chunk_indexes.remove(source_node_id);
            pending
                .fetch_window_end_chunk_indexes
                .remove(source_node_id);
            pending.fetch_failed_from.insert(source_node_id.clone());
            for state in pending.stage_retries.values_mut() {
                state.next_retry_tick = 0;
                state.inflight = false;
            }
            for state in pending.repair_retries.values_mut() {
                wake_repair_retry_state(state);
                state.inflight = false;
            }
        }
    }

    pub(crate) fn handle_networked_read_request(
        &mut self,
        requester_node_id: NodeId,
        request: DfsReadRequest,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let requested_range = match (request.range_offset, request.range_max_bytes) {
            (Some(offset), Some(max_bytes)) => Ok(Some((offset, max_bytes))),
            (None, None) => Ok(None),
            _ => Err("DFS read range request must include both offset and max bytes".to_string()),
        };
        let mut sent_chunked_response = false;
        let result = match requested_range {
            Err(reason) => DfsReadResult::Rejected { reason },
            Ok(range) => {
                if let Some((result, sent_chunked)) = self.serve_networked_read_from_request_hints(
                    &requester_node_id,
                    &request,
                    range,
                )? {
                    sent_chunked_response = sent_chunked;
                    result
                } else {
                    match self.current_read_object_metadata(&request.object_id) {
                        Ok(object) if !object.current_locations.contains(&self.node_id) => {
                            DfsReadResult::Rejected {
                                reason: format!(
                                    "node {} is not a current location for DFS object {}",
                                    self.node_id, request.object_id
                                ),
                            }
                        }
                        Ok(_) => {
                            match range {
                                Some((offset, max_bytes)) => {
                                    let chunked_range = max_bytes > DFS_BLOB_INLINE_LIMIT_BYTES
                                        || request.start_chunk_index != 0
                                        || request.max_chunks != 0;
                                    if chunked_range {
                                        match self.open_local_current_object_range_stream_with_state(
                                &request.object_id,
                                offset,
                                max_bytes,
                                request.start_chunk_index,
                            )? {
                                NetworkedDfsRangeReadStreamOutcome::Found(stream)
                                    if stream.range_size_bytes > 0 =>
                                {
                                    self.send_networked_range_read_response_chunks_from_reader(
                                        &requester_node_id,
                                        &request,
                                        &stream.data,
                                        stream.range_size_bytes,
                                        stream.reader,
                                    )?;
                                    sent_chunked_response = true;
                                    DfsReadResult::RangeFound(stream.data)
                                }
                                NetworkedDfsRangeReadStreamOutcome::Found(stream) => {
                                    DfsReadResult::RangeFound(stream.data)
                                }
                                NetworkedDfsRangeReadStreamOutcome::Conflicted { conflicts } => {
                                    DfsReadResult::Rejected {
                                        reason: read_conflicts_reason(conflicts),
                                    }
                                }
                                NetworkedDfsRangeReadStreamOutcome::PendingReplication
                                | NetworkedDfsRangeReadStreamOutcome::NotFound => {
                                    DfsReadResult::NotFound
                                }
                            }
                                    } else {
                                        match self.read_local_object_range_with_state(
                                            &request.object_id,
                                            offset,
                                            max_bytes,
                                            false,
                                        )? {
                                            NetworkedDfsRangeReadOutcome::Found {
                                                data, ..
                                            } => DfsReadResult::RangeFound(data),
                                            NetworkedDfsRangeReadOutcome::Conflicted {
                                                conflicts,
                                                ..
                                            } => DfsReadResult::Rejected {
                                                reason: read_conflicts_reason(conflicts),
                                            },
                                            NetworkedDfsRangeReadOutcome::PendingReplication {
                                                ..
                                            }
                                            | NetworkedDfsRangeReadOutcome::NotConfirmed {
                                                ..
                                            }
                                            | NetworkedDfsRangeReadOutcome::NotFound => {
                                                DfsReadResult::NotFound
                                            }
                                        }
                                    }
                                }
                                None => {
                                    let stream_outcome = self
                                        .open_local_current_object_stream_with_state(
                                            &request.object_id,
                                        )?;
                                    match stream_outcome {
                                        NetworkedDfsReadStreamOutcome::Found(stream)
                                            if stream.total_size_bytes
                                                > DFS_BLOB_INLINE_LIMIT_BYTES as u64 =>
                                        {
                                            let window_outcome = if request.start_chunk_index == 0
                                                && request.max_chunks == 0
                                            {
                                                Ok(NetworkedDfsReadStreamOutcome::Found(stream))
                                            } else {
                                                self.open_local_current_object_read_window_stream_with_state(
                                        &request.object_id,
                                        request.start_chunk_index,
                                        request.max_chunks,
                                    )
                                            };
                                            match window_outcome {
                                    Ok(NetworkedDfsReadStreamOutcome::Found(stream)) => {
                                        self.send_networked_read_response_chunks_from_reader(
                                            &requester_node_id,
                                            &request,
                                            &stream.data,
                                            stream.total_size_bytes,
                                            stream.reader,
                                        )?;
                                        sent_chunked_response = true;
                                        DfsReadResult::Found(stream.data)
                                    }
                                    Ok(NetworkedDfsReadStreamOutcome::Conflicted { conflicts }) => {
                                        DfsReadResult::Rejected {
                                            reason: read_conflicts_reason(conflicts),
                                        }
                                    }
                                    Ok(NetworkedDfsReadStreamOutcome::PendingReplication)
                                    | Ok(NetworkedDfsReadStreamOutcome::NotFound) => {
                                        DfsReadResult::NotFound
                                    }
                                    Err(error) => DfsReadResult::Rejected {
                                        reason: error.to_string(),
                                    },
                                }
                                        }
                                        NetworkedDfsReadStreamOutcome::Found(_) => {
                                            if request.start_chunk_index != 0 {
                                                DfsReadResult::Rejected {
                                                    reason: format!(
                                                        "DFS read chunk window starts outside inline object: {}",
                                                        request.start_chunk_index
                                                    ),
                                                }
                                            } else {
                                                match self
                                        .read_local_object_with_state(&request.object_id, false)?
                                    {
                                        NetworkedDfsReadOutcome::Found { data, .. } => {
                                            DfsReadResult::Found(data)
                                        }
                                        NetworkedDfsReadOutcome::Conflicted {
                                            conflicts, ..
                                        } => DfsReadResult::Rejected {
                                            reason: read_conflicts_reason(conflicts),
                                        },
                                        NetworkedDfsReadOutcome::PendingReplication { .. }
                                        | NetworkedDfsReadOutcome::NotConfirmed { .. }
                                        | NetworkedDfsReadOutcome::NotFound => {
                                            DfsReadResult::NotFound
                                        }
                                    }
                                            }
                                        }
                                        NetworkedDfsReadStreamOutcome::Conflicted { conflicts } => {
                                            DfsReadResult::Rejected {
                                                reason: read_conflicts_reason(conflicts),
                                            }
                                        }
                                        NetworkedDfsReadStreamOutcome::PendingReplication
                                        | NetworkedDfsReadStreamOutcome::NotFound => {
                                            DfsReadResult::NotFound
                                        }
                                    }
                                }
                            }
                        }
                        Err(DfsError::UnknownObject { .. }) => DfsReadResult::NotFound,
                        Err(error) => return Err(error),
                    }
                }
            }
        };
        match &result {
            DfsReadResult::Found(_) if sent_chunked_response => {}
            DfsReadResult::RangeFound(_) if sent_chunked_response => {}
            DfsReadResult::Found(data)
                if data.bytes.len() > DFS_BLOB_INLINE_LIMIT_BYTES
                    && !direct_full_read_response_allowed(&request, data.bytes.len() as u64) =>
            {
                self.send_networked_read_response_chunks(&requester_node_id, &request, data)?;
            }
            _ => {
                self.send_dfs_message_if_routable(
                    &requester_node_id,
                    DfsNetworkMessage::ReadResponse(DfsReadResponse {
                        request_id: request.request_id.clone(),
                        object_id: request.object_id.clone(),
                        result: result.clone(),
                    }),
                )?;
            }
        }
        Ok(NetworkedDfsServiceEvent::RemoteReadServed {
            request_id: request.request_id,
            object_id: request.object_id,
            requester_node_id,
            result,
        })
    }

    fn serve_networked_read_from_request_hints(
        &mut self,
        requester_node_id: &NodeId,
        request: &DfsReadRequest,
        requested_range: Option<(u64, usize)>,
    ) -> Result<Option<(DfsReadResult, bool)>, DfsError> {
        let (Some(version), Some(content_hash)) =
            (request.version.as_ref(), request.content_hash.as_ref())
        else {
            return Ok(None);
        };
        let hinted_size = request.content_size_bytes;
        let object_id = request.object_id.clone();
        let version = version.clone();
        let content_hash = content_hash.clone();
        match requested_range {
            Some((offset, max_bytes)) => {
                let Some(total_size_bytes) = hinted_size else {
                    return Ok(None);
                };
                let range_offset = offset.min(total_size_bytes);
                let range_size_bytes =
                    range_transfer_size_bytes(total_size_bytes, offset, max_bytes);
                let chunked_range = max_bytes > DFS_BLOB_INLINE_LIMIT_BYTES
                    || request.start_chunk_index != 0
                    || request.max_chunks != 0;
                if chunked_range {
                    let window_prefix_bytes =
                        chunk_prefix_bytes(range_size_bytes, request.start_chunk_index);
                    let window_offset = range_offset.saturating_add(window_prefix_bytes);
                    let range_len = read_response_window_range_len(
                        request.start_chunk_index,
                        range_size_bytes,
                        request.max_chunks,
                    )?;
                    let reader = match self.content.open_blob_range_stream(
                        &content_hash,
                        window_offset,
                        range_len,
                    ) {
                        Ok(reader) => ObjectVersionReader::from_blob_range(reader),
                        Err(_) => return Ok(Some((DfsReadResult::NotFound, false))),
                    };
                    let data = DfsReadRangeData {
                        object_id,
                        version,
                        content_hash: Some(content_hash),
                        offset: range_offset,
                        total_size_bytes,
                        bytes: Vec::new(),
                    };
                    self.send_networked_range_read_response_chunks_from_reader(
                        requester_node_id,
                        request,
                        &data,
                        range_size_bytes,
                        reader,
                    )?;
                    return Ok(Some((DfsReadResult::RangeFound(data), true)));
                }
                let bytes = match self
                    .content
                    .read_blob_range(&content_hash, offset, max_bytes)
                {
                    Ok(bytes) => bytes,
                    Err(_) => return Ok(Some((DfsReadResult::NotFound, false))),
                };
                return Ok(Some((
                    DfsReadResult::RangeFound(DfsReadRangeData {
                        object_id,
                        version,
                        content_hash: Some(content_hash),
                        offset: range_offset,
                        total_size_bytes,
                        bytes,
                    }),
                    false,
                )));
            }
            None => {
                let total_size_bytes = match hinted_size {
                    Some(size) => size,
                    None => {
                        let Some(bytes) = self.read_blob_once_for_remote_response(&content_hash)?
                        else {
                            return Ok(Some((DfsReadResult::NotFound, false)));
                        };
                        return Ok(Some((
                            DfsReadResult::Found(DfsReadData {
                                object_id,
                                version,
                                content_hash: Some(content_hash),
                                bytes,
                            }),
                            false,
                        )));
                    }
                };
                if total_size_bytes > DFS_BLOB_INLINE_LIMIT_BYTES as u64
                    || request.start_chunk_index != 0
                    || request.max_chunks != 0
                {
                    if direct_full_read_response_allowed(request, total_size_bytes) {
                        let Some(bytes) = self.read_blob_for_direct_remote_response(
                            &content_hash,
                            total_size_bytes,
                        )?
                        else {
                            return Ok(Some((DfsReadResult::NotFound, false)));
                        };
                        return Ok(Some((
                            DfsReadResult::Found(DfsReadData {
                                object_id,
                                version,
                                content_hash: Some(content_hash),
                                bytes,
                            }),
                            false,
                        )));
                    }
                    let window_offset =
                        chunk_prefix_bytes(total_size_bytes, request.start_chunk_index);
                    let range_len = read_response_window_range_len(
                        request.start_chunk_index,
                        total_size_bytes,
                        request.max_chunks,
                    )?;
                    let reader = match self.content.open_blob_range_stream(
                        &content_hash,
                        window_offset,
                        range_len,
                    ) {
                        Ok(reader) => ObjectVersionReader::from_blob_range(reader),
                        Err(_) => return Ok(Some((DfsReadResult::NotFound, false))),
                    };
                    let data = DfsReadData {
                        object_id,
                        version,
                        content_hash: Some(content_hash),
                        bytes: Vec::new(),
                    };
                    self.send_networked_read_response_chunks_from_reader(
                        requester_node_id,
                        request,
                        &data,
                        total_size_bytes,
                        reader,
                    )?;
                    return Ok(Some((DfsReadResult::Found(data), true)));
                }
                let Some(bytes) = self.read_blob_once_for_remote_response(&content_hash)? else {
                    return Ok(Some((DfsReadResult::NotFound, false)));
                };
                Ok(Some((
                    DfsReadResult::Found(DfsReadData {
                        object_id,
                        version,
                        content_hash: Some(content_hash),
                        bytes,
                    }),
                    false,
                )))
            }
        }
    }

    fn read_blob_once_for_remote_response(
        &self,
        content_hash: &str,
    ) -> Result<Option<Vec<u8>>, DfsError> {
        let mut reader = match self.content.open_blob_stream(content_hash) {
            Ok(reader) => reader,
            Err(_) => return Ok(None),
        };
        let mut bytes =
            Vec::with_capacity(usize::try_from(reader.size_bytes()).unwrap_or(usize::MAX));
        reader
            .read_to_end(&mut bytes)
            .map_err(|source| DfsError::ContentStore {
                message: format!("failed to read blob {content_hash}: {source}"),
            })?;
        Ok(Some(bytes))
    }

    fn read_blob_for_direct_remote_response(
        &self,
        content_hash: &str,
        expected_size_bytes: u64,
    ) -> Result<Option<Vec<u8>>, DfsError> {
        if expected_size_bytes > DFS_DIRECT_REMOTE_READ_MAX_BYTES {
            return Ok(None);
        }
        let mut reader = match self.content.open_blob_stream(content_hash) {
            Ok(reader) => reader,
            Err(_) => return Ok(None),
        };
        if reader.size_bytes() != expected_size_bytes {
            return Ok(None);
        }
        let capacity = dfs_byte_count_to_usize("DFS direct remote read", expected_size_bytes)?;
        let mut bytes = Vec::with_capacity(capacity);
        reader
            .read_to_end(&mut bytes)
            .map_err(|source| DfsError::ContentStore {
                message: format!("failed to read blob {content_hash}: {source}"),
            })?;
        if bytes.len() != capacity {
            return Err(DfsError::ContentStore {
                message: format!(
                    "failed to read full blob {content_hash}: expected {capacity} bytes, actual {}",
                    bytes.len()
                ),
            });
        }
        Ok(Some(bytes))
    }

    pub(crate) fn handle_networked_bulk_read_request(
        &mut self,
        requester_node_id: NodeId,
        request: DfsBulkReadRequest,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let result_count = request.reads.len();
        let mut results = Vec::with_capacity(request.reads.len());
        for item in request.reads {
            let result = self.bulk_read_item_result(&item)?;
            results.push(DfsBulkReadItemResponse {
                request_id: item.request_id,
                object_id: item.object_id,
                result,
            });
        }
        self.send_dfs_message_if_routable(
            &requester_node_id,
            DfsNetworkMessage::BulkReadResponse(DfsBulkReadResponse {
                request_id: request.request_id.clone(),
                results,
            }),
        )?;
        Ok(NetworkedDfsServiceEvent::RemoteBulkReadServed {
            request_id: request.request_id,
            requester_node_id,
            result_count,
        })
    }

    fn bulk_read_item_result(&self, item: &DfsBulkReadItem) -> Result<DfsReadResult, DfsError> {
        if item.content_size_bytes > DFS_BLOB_INLINE_LIMIT_BYTES as u64 {
            return Ok(DfsReadResult::Rejected {
                reason: format!(
                    "bulk read item {} is {} bytes, above inline limit {}",
                    item.object_id, item.content_size_bytes, DFS_BLOB_INLINE_LIMIT_BYTES
                ),
            });
        }
        let Some(bytes) = self.read_blob_once_for_remote_response(&item.content_hash)? else {
            return Ok(DfsReadResult::NotFound);
        };
        if bytes.len() as u64 != item.content_size_bytes {
            return Ok(DfsReadResult::Rejected {
                reason: format!(
                    "bulk read item {} size mismatch: expected {}, actual {}",
                    item.object_id,
                    item.content_size_bytes,
                    bytes.len()
                ),
            });
        }
        Ok(DfsReadResult::Found(DfsReadData {
            object_id: item.object_id.clone(),
            version: item.version.clone(),
            content_hash: Some(item.content_hash.clone()),
            bytes,
        }))
    }

    pub(crate) fn handle_networked_bulk_read_response(
        &mut self,
        source_node_id: NodeId,
        response: DfsBulkReadResponse,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        Ok(NetworkedDfsServiceEvent::RemoteBulkReadCompleted {
            request_id: response.request_id,
            source_node_id,
            results: response.results,
        })
    }

    pub(crate) fn send_networked_read_response_chunks(
        &mut self,
        requester_node_id: &NodeId,
        request: &DfsReadRequest,
        data: &DfsReadData,
    ) -> Result<(), DfsError> {
        let total_chunks = blob_chunk_count(&data.bytes)?;
        let end_chunk_index =
            fetch_response_window_end(request.start_chunk_index, total_chunks, request.max_chunks);
        for (chunk_index, chunk) in data
            .bytes
            .chunks(DFS_BLOB_CHUNK_BYTES)
            .enumerate()
            .skip(request.start_chunk_index as usize)
            .take(end_chunk_index.saturating_sub(request.start_chunk_index) as usize)
        {
            let sent = self.send_dfs_message_if_routable(
                requester_node_id,
                DfsNetworkMessage::ReadResponseChunk(DfsReadResponseChunk {
                    request_id: request.request_id.clone(),
                    object_id: request.object_id.clone(),
                    version: data.version.clone(),
                    content_hash: data.content_hash.clone(),
                    range_offset: None,
                    object_total_size_bytes: None,
                    total_size_bytes: data.bytes.len() as u64,
                    chunk_index: chunk_index as u32,
                    total_chunks,
                    chunk_hash: Some(chunk_hash(chunk)),
                    bytes: chunk.to_vec(),
                }),
            )?;
            if !sent {
                break;
            }
        }
        Ok(())
    }

    fn send_networked_read_response_chunks_from_reader(
        &mut self,
        requester_node_id: &NodeId,
        request: &DfsReadRequest,
        data: &DfsReadData,
        total_size_bytes: u64,
        reader: ObjectVersionReader,
    ) -> Result<(), DfsError> {
        let read_error_subject = format!("{:?}", data.content_hash);
        self.send_object_version_response_chunks_from_reader(
            requester_node_id,
            request.start_chunk_index,
            request.max_chunks,
            total_size_bytes,
            &read_error_subject,
            reader,
            |chunk_index, total_chunks, bytes| {
                DfsNetworkMessage::ReadResponseChunk(DfsReadResponseChunk {
                    request_id: request.request_id.clone(),
                    object_id: request.object_id.clone(),
                    version: data.version.clone(),
                    content_hash: data.content_hash.clone(),
                    range_offset: None,
                    object_total_size_bytes: None,
                    total_size_bytes,
                    chunk_index,
                    total_chunks,
                    chunk_hash: Some(chunk_hash(&bytes)),
                    bytes,
                })
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn send_object_version_response_chunks_from_reader(
        &mut self,
        requester_node_id: &NodeId,
        start_chunk_index: u32,
        max_chunks: u32,
        total_size_bytes: u64,
        read_error_subject: &str,
        mut reader: ObjectVersionReader,
        mut message_for_chunk: impl FnMut(u32, u32, Vec<u8>) -> DfsNetworkMessage,
    ) -> Result<(), DfsError> {
        let total_chunks = blob_chunk_count_for_size(total_size_bytes)?;
        let end_chunk_index =
            fetch_response_window_end(start_chunk_index, total_chunks, max_chunks);
        tracing::debug!(
            target: "sinomo.dfs.transfer",
            local_node_id = %self.node_id,
            requester_node_id = %requester_node_id,
            read_error_subject = %read_error_subject,
            total_size_bytes,
            start_chunk_index,
            end_chunk_index,
            total_chunks,
            "sending DFS response chunk window"
        );
        let mut buffer = vec![0_u8; DFS_BLOB_CHUNK_BYTES];
        for chunk_index in start_chunk_index..end_chunk_index {
            let chunk_len = blob_chunk_len(total_size_bytes, chunk_index)?;
            reader
                .read_exact(&mut buffer[..chunk_len])
                .map_err(|source| DfsError::ContentStore {
                    message: format!(
                        "failed to read blob {read_error_subject} chunk {chunk_index}: {source}"
                    ),
                })?;
            let sent = self.send_dfs_message_if_routable(
                requester_node_id,
                message_for_chunk(chunk_index, total_chunks, buffer[..chunk_len].to_vec()),
            )?;
            if !sent {
                tracing::warn!(
                    target: "sinomo.dfs.transfer",
                    local_node_id = %self.node_id,
                    requester_node_id = %requester_node_id,
                    read_error_subject = %read_error_subject,
                    failed_chunk_index = chunk_index,
                    total_chunks,
                    "DFS response chunk window stopped because requester was not routable"
                );
                break;
            }
        }
        tracing::debug!(
            target: "sinomo.dfs.transfer",
            local_node_id = %self.node_id,
            requester_node_id = %requester_node_id,
            read_error_subject = %read_error_subject,
            next_chunk_index = end_chunk_index,
            total_chunks,
            "sent DFS response chunk window"
        );
        Ok(())
    }

    fn send_networked_range_read_response_chunks_from_reader(
        &mut self,
        requester_node_id: &NodeId,
        request: &DfsReadRequest,
        data: &DfsReadRangeData,
        range_size_bytes: u64,
        reader: ObjectVersionReader,
    ) -> Result<(), DfsError> {
        let total_chunks = blob_chunk_count_for_size(range_size_bytes)?;
        let max_chunks = if request.max_chunks == 0 {
            self.adaptive_read_chunk_window(requester_node_id, range_size_bytes)
        } else {
            request.max_chunks
        };
        let end_chunk_index =
            fetch_response_window_end(request.start_chunk_index, total_chunks, max_chunks);
        if end_chunk_index <= request.start_chunk_index {
            return Ok(());
        }
        let read_error_subject = format!("{:?} range", data.content_hash);
        self.send_object_version_response_chunks_from_reader(
            requester_node_id,
            request.start_chunk_index,
            max_chunks,
            range_size_bytes,
            &read_error_subject,
            reader,
            |chunk_index, total_chunks, bytes| {
                DfsNetworkMessage::ReadResponseChunk(DfsReadResponseChunk {
                    request_id: request.request_id.clone(),
                    object_id: request.object_id.clone(),
                    version: data.version.clone(),
                    content_hash: data.content_hash.clone(),
                    range_offset: Some(data.offset),
                    object_total_size_bytes: Some(data.total_size_bytes),
                    total_size_bytes: range_size_bytes,
                    chunk_index,
                    total_chunks,
                    chunk_hash: Some(chunk_hash(&bytes)),
                    bytes,
                })
            },
        )
    }

    pub(crate) fn handle_networked_read_response(
        &mut self,
        source_node_id: NodeId,
        response: DfsReadResponse,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let Some(pending) = self.pending_reads.get(&response.request_id).cloned() else {
            return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: response.request_id,
                object_id: response.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: "unknown DFS read request".to_string(),
                },
            });
        };
        if source_node_id != pending.target_node_id {
            let reason = format!(
                "unexpected DFS read response source: expected {}, actual {}",
                pending.target_node_id, source_node_id
            );
            return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: response.request_id,
                object_id: response.object_id,
                source_node_id,
                result: DfsReadResult::Rejected { reason },
            });
        }
        self.pending_reads.remove(&response.request_id);
        self.incoming_read_memory_transfers
            .remove(&response.request_id);
        if pending.object_id != response.object_id {
            self.incoming_read_transfers.remove(&response.request_id);
            self.incoming_read_memory_transfers
                .remove(&response.request_id);
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: response.request_id,
                object_id: response.object_id.clone(),
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: format!(
                        "unexpected DFS read response object: expected {}, actual {}",
                        pending.object_id, response.object_id
                    ),
                },
            });
        }
        match &response.result {
            DfsReadResult::Found(data) => {
                if pending.range_offset.is_some() || pending.range_max_bytes.is_some() {
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: "unexpected full DFS read response for range request"
                                .to_string(),
                        },
                    });
                }
                if data.object_id != pending.object_id {
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: format!(
                                "unexpected DFS read response payload object: expected {}, actual {}",
                                pending.object_id, data.object_id
                            ),
                        },
                    });
                }
                if pending.cache_response && self.pending_read_metadata_advanced(&pending)? {
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: "stale DFS read response: object metadata advanced".to_string(),
                        },
                    });
                }
                if pending.cache_response
                    && !self.pending_read_source_still_current(&pending, &source_node_id)?
                {
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason:
                                "stale DFS read response: source is no longer a current location"
                                    .to_string(),
                        },
                    });
                }
                if data.version != pending.version {
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: format!(
                                "stale DFS read response: expected version {}, actual {}",
                                pending.version, data.version
                            ),
                        },
                    });
                }
                if data.content_hash != pending.content_hash {
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: format!(
                                "stale DFS read response: expected content hash {:?}, actual {:?}",
                                pending.content_hash, data.content_hash
                            ),
                        },
                    });
                }
                let validation_started = Instant::now();
                let validation = if pending.cache_response {
                    self.validate_remote_read_data(data)
                } else {
                    self.validate_remote_read_data_for_pending(&pending, data)
                };
                dfs_perf_trace(format_args!(
                    "remote-read-validate request={} object={} bytes={} cache_response={} elapsed_ms={}",
                    response.request_id,
                    pending.object_id,
                    data.bytes.len(),
                    pending.cache_response,
                    validation_started.elapsed().as_millis()
                ));
                if let Err(error) = validation {
                    if matches!(error, DfsError::ContentHashMismatch { .. }) {
                        self.mark_remote_read_location_missing(
                            &pending.object_id,
                            &pending.version,
                            &pending.content_hash,
                            &source_node_id,
                        )?;
                        return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                            request_id: response.request_id,
                            object_id: response.object_id,
                            source_node_id,
                            result: DfsReadResult::Rejected {
                                reason: error.to_string(),
                            },
                        });
                    }
                    return Err(error);
                }
                if pending.cache_response
                    && let Err(error) = self.store_remote_read_data(&source_node_id, data)
                {
                    if matches!(error, DfsError::ContentHashMismatch { .. }) {
                        self.mark_remote_read_location_missing(
                            &pending.object_id,
                            &pending.version,
                            &pending.content_hash,
                            &source_node_id,
                        )?;
                        return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                            request_id: response.request_id,
                            object_id: response.object_id,
                            source_node_id,
                            result: DfsReadResult::Rejected {
                                reason: error.to_string(),
                            },
                        });
                    }
                    return Err(error);
                }
                if !pending.cache_response {
                    self.clear_transient_remote_read_location_prunes_for_object(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                    );
                }
                self.clear_pending_remote_reads_for_object_version(
                    &pending.object_id,
                    &pending.version,
                    &pending.content_hash,
                );
            }
            DfsReadResult::RangeFound(data) => {
                let (Some(requested_offset), Some(requested_max_bytes)) =
                    (pending.range_offset, pending.range_max_bytes)
                else {
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: "unexpected range DFS read response for full-object request"
                                .to_string(),
                        },
                    });
                };
                if data.object_id != pending.object_id {
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: format!(
                                "unexpected DFS range read response payload object: expected {}, actual {}",
                                pending.object_id, data.object_id
                            ),
                        },
                    });
                }
                if pending.cache_response && self.pending_read_metadata_advanced(&pending)? {
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: "stale DFS read response: object metadata advanced".to_string(),
                        },
                    });
                }
                if pending.cache_response
                    && !self.pending_read_source_still_current(&pending, &source_node_id)?
                {
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason:
                                "stale DFS read response: source is no longer a current location"
                                    .to_string(),
                        },
                    });
                }
                if data.version != pending.version {
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: format!(
                                "stale DFS range read response: expected version {}, actual {}",
                                pending.version, data.version
                            ),
                        },
                    });
                }
                if data.content_hash != pending.content_hash {
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: format!(
                                "stale DFS range read response: expected content hash {:?}, actual {:?}",
                                pending.content_hash, data.content_hash
                            ),
                        },
                    });
                }
                if let Some(expected_size) = pending.content_size_bytes
                    && expected_size != data.total_size_bytes
                {
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: format!(
                                "DFS range read response size does not match object metadata: expected {expected_size}, actual {}",
                                data.total_size_bytes
                            ),
                        },
                    });
                }
                let expected_offset = requested_offset.min(data.total_size_bytes);
                if data.offset != expected_offset {
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: format!(
                                "DFS range read response offset mismatch: expected {expected_offset}, actual {}",
                                data.offset
                            ),
                        },
                    });
                }
                if data.bytes.len() > requested_max_bytes {
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: format!(
                                "DFS range read response exceeds requested window: expected at most {requested_max_bytes}, actual {}",
                                data.bytes.len()
                            ),
                        },
                    });
                }
                let Some(range_end) = data.offset.checked_add(data.bytes.len() as u64) else {
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: "DFS range read response offset overflow".to_string(),
                        },
                    });
                };
                if range_end > data.total_size_bytes {
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: format!(
                                "DFS range read response exceeds total size: end {range_end}, total {}",
                                data.total_size_bytes
                            ),
                        },
                    });
                }
            }
            DfsReadResult::NotFound => {
                if pending.cache_response && self.pending_read_metadata_advanced(&pending)? {
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: "stale DFS read response: object metadata advanced".to_string(),
                        },
                    });
                }
                self.mark_remote_read_location_missing(
                    &pending.object_id,
                    &pending.version,
                    &pending.content_hash,
                    &source_node_id,
                )?;
            }
            DfsReadResult::Rejected { reason } => {
                if remote_read_rejection_prunes_location(reason) {
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                }
            }
        }
        Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
            request_id: response.request_id,
            object_id: response.object_id,
            source_node_id,
            result: response.result,
        })
    }

    pub(crate) fn mark_remote_read_location_missing(
        &mut self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &Option<String>,
        source_node_id: &NodeId,
    ) -> Result<(), DfsError> {
        self.record_transient_remote_read_location_prune(
            object_id,
            version,
            content_hash,
            source_node_id,
        );
        if let Some(object) = self.metadata.objects.get_mut(object_id) {
            if object.version == *version && object.content_hash == *content_hash {
                object
                    .current_locations
                    .retain(|node_id| node_id != source_node_id);
            }
        }
        let backend_object = if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(mut object) = backend.get_object(object_id)?
            && object.version == *version
            && object.content_hash == *content_hash
        {
            if !object.current_locations.contains(source_node_id) {
                None
            } else {
                object
                    .current_locations
                    .retain(|node_id| node_id != source_node_id);
                Some(object)
            }
        } else {
            None
        };
        let backend_version = if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(mut version_record) = backend.get_version(object_id, version)?
            && version_record.content_hash == *content_hash
        {
            if !version_record.locations.contains(source_node_id) {
                None
            } else {
                version_record
                    .locations
                    .retain(|node_id| node_id != source_node_id);
                Some(version_record)
            }
        } else {
            None
        };
        let should_persist = self.should_persist_remote_read_location_prune(
            backend_object.as_ref(),
            backend_version.as_ref(),
        )?;
        if should_persist && let Some(backend) = self.metadata_backend.as_mut() {
            let _ = backend.remove_receipt(object_id, version, source_node_id)?;
        }
        if should_persist && let Some(object) = backend_object {
            self.put_visible_object_in_metadata_backend(&object)?;
        } else if should_persist
            && let Some(version_record) = backend_version
            && let Some(backend) = self.metadata_backend.as_mut()
        {
            backend.put_version(&version_record)?;
        }
        Ok(())
    }

    fn should_persist_remote_read_location_prune(
        &mut self,
        object: Option<&DfsObjectMetadata>,
        version_record: Option<&DfsObjectVersionRecord>,
    ) -> Result<bool, DfsError> {
        let Some(object) = object else {
            return Ok(version_record.is_some() && self.control_plane.is_none());
        };
        Ok(self
            .select_metadata_shard_for_object(object)?
            .is_none_or(|shard| shard.primary_node_id == self.node_id))
    }

    pub(crate) fn handle_networked_range_read_response_chunk(
        &mut self,
        source_node_id: NodeId,
        chunk: DfsReadResponseChunk,
        pending: PendingNetworkedRead,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        let request_id = chunk.request_id.clone();
        let object_id = chunk.object_id.clone();
        let version = chunk.version.clone();
        let content_hash = chunk.content_hash.clone();
        let Some(requested_offset) = pending.range_offset else {
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id,
                object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: "range read chunk is missing requested offset state".to_string(),
                },
            }));
        };
        let Some(requested_max_bytes) = pending.range_max_bytes else {
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id,
                object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: "range read chunk is missing requested max bytes state".to_string(),
                },
            }));
        };
        if pending.version != chunk.version || pending.content_hash != chunk.content_hash {
            self.pending_reads.remove(&chunk.request_id);
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id,
                object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: "stale DFS range read response chunk".to_string(),
                },
            }));
        }
        if pending.cache_response && self.pending_read_metadata_advanced(&pending)? {
            self.pending_reads.remove(&chunk.request_id);
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id,
                object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: "stale DFS range read response: object metadata advanced".to_string(),
                },
            }));
        }
        if pending.cache_response
            && !self.pending_read_source_still_current(&pending, &source_node_id)?
        {
            self.pending_reads.remove(&chunk.request_id);
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id,
                object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: "stale DFS range read response: source is no longer a current location"
                        .to_string(),
                },
            }));
        }
        let Some(object_total_size_bytes) = chunk.object_total_size_bytes else {
            self.pending_reads.remove(&chunk.request_id);
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id,
                object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: "chunked DFS range read response is missing object size".to_string(),
                },
            }));
        };
        if let Some(expected_size) = pending.content_size_bytes
            && expected_size != object_total_size_bytes
        {
            self.pending_reads.remove(&chunk.request_id);
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id,
                object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: format!(
                        "chunked DFS range read response object size does not match metadata: expected {expected_size}, actual {object_total_size_bytes}"
                    ),
                },
            }));
        }
        let expected_range_offset = requested_offset.min(object_total_size_bytes);
        if chunk.range_offset != Some(expected_range_offset) {
            self.pending_reads.remove(&chunk.request_id);
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id,
                object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: format!(
                        "chunked DFS range read response offset mismatch: expected {expected_range_offset}, actual {:?}",
                        chunk.range_offset
                    ),
                },
            }));
        }
        let expected_range_size_bytes = range_transfer_size_bytes(
            object_total_size_bytes,
            requested_offset,
            requested_max_bytes,
        );
        if chunk.total_size_bytes != expected_range_size_bytes {
            self.pending_reads.remove(&chunk.request_id);
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id,
                object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: format!(
                        "chunked DFS range read response size mismatch: expected {expected_range_size_bytes}, actual {}",
                        chunk.total_size_bytes
                    ),
                },
            }));
        }
        if chunk.chunk_index != pending.next_chunk_index {
            self.pending_reads.remove(&chunk.request_id);
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id,
                object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: format!(
                        "chunked DFS range read response is out of order: expected chunk {}, actual {}",
                        pending.next_chunk_index, chunk.chunk_index
                    ),
                },
            }));
        }
        if let Err(error) = validate_chunk_bytes(
            "chunked DFS range read response",
            chunk.total_size_bytes,
            chunk.chunk_index,
            chunk.total_chunks,
            &chunk.bytes,
        )
        .and_then(|()| {
            validate_chunk_hash(
                "chunked DFS range read response",
                chunk.chunk_index,
                &chunk.bytes,
                chunk.chunk_hash.as_deref(),
            )
        }) {
            self.pending_reads.remove(&chunk.request_id);
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id,
                object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: error.to_string(),
                },
            }));
        }
        let next_chunk_index = chunk.chunk_index.saturating_add(1);
        let complete = next_chunk_index >= chunk.total_chunks;
        if complete {
            self.pending_reads.remove(&chunk.request_id);
        } else if let Some(pending) = self.pending_reads.get_mut(&chunk.request_id) {
            pending.next_chunk_index = next_chunk_index;
        }
        let total_chunks = chunk.total_chunks;
        let chunk_index = chunk.chunk_index;
        let bytes = chunk.bytes;
        if !complete
            && let Some(event) = self.request_next_read_window_if_due(
                &request_id,
                &source_node_id,
                next_chunk_index,
                total_chunks,
            )?
        {
            return Ok(Some(event));
        }
        Ok(Some(
            NetworkedDfsServiceEvent::RemoteRangeReadChunkReceived {
                request_id,
                object_id,
                source_node_id,
                version,
                content_hash,
                range_offset: expected_range_offset,
                range_size_bytes: expected_range_size_bytes,
                object_total_size_bytes,
                chunk_index,
                total_chunks,
                bytes,
                complete,
            },
        ))
    }

    pub(crate) fn handle_networked_read_response_chunk(
        &mut self,
        source_node_id: NodeId,
        chunk: DfsReadResponseChunk,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        let Some(pending) = self.pending_reads.get(&chunk.request_id).cloned() else {
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id,
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: "unknown DFS read request".to_string(),
                },
            }));
        };
        if source_node_id != pending.target_node_id {
            let reason = format!(
                "unexpected DFS read response source: expected {}, actual {}",
                pending.target_node_id, source_node_id
            );
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id,
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected { reason },
            }));
        }
        if pending.object_id != chunk.object_id {
            let reason = format!(
                "unexpected DFS read response object: expected {}, actual {}",
                pending.object_id, chunk.object_id
            );
            self.pending_reads.remove(&chunk.request_id);
            self.incoming_read_transfers.remove(&chunk.request_id);
            self.incoming_read_memory_transfers
                .remove(&chunk.request_id);
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id,
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected { reason },
            }));
        }
        if pending.range_offset.is_some() || pending.range_max_bytes.is_some() {
            return self.handle_networked_range_read_response_chunk(source_node_id, chunk, pending);
        }
        if pending.version != chunk.version || pending.content_hash != chunk.content_hash {
            self.pending_reads.remove(&chunk.request_id);
            self.incoming_read_transfers.remove(&chunk.request_id);
            self.incoming_read_memory_transfers
                .remove(&chunk.request_id);
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id,
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: "stale DFS read response chunk".to_string(),
                },
            }));
        }
        if pending.cache_response && self.pending_read_metadata_advanced(&pending)? {
            self.pending_reads.remove(&chunk.request_id);
            self.incoming_read_transfers.remove(&chunk.request_id);
            self.incoming_read_memory_transfers
                .remove(&chunk.request_id);
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id,
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: "stale DFS read response: object metadata advanced".to_string(),
                },
            }));
        }
        if pending.cache_response
            && !self.pending_read_source_still_current(&pending, &source_node_id)?
        {
            self.pending_reads.remove(&chunk.request_id);
            self.incoming_read_transfers.remove(&chunk.request_id);
            self.incoming_read_memory_transfers
                .remove(&chunk.request_id);
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id,
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: "stale DFS read response: source is no longer a current location"
                        .to_string(),
                },
            }));
        }
        if let Some(expected_size) = pending.content_size_bytes
            && expected_size != chunk.total_size_bytes
        {
            self.pending_reads.remove(&chunk.request_id);
            self.incoming_read_transfers.remove(&chunk.request_id);
            self.incoming_read_memory_transfers
                .remove(&chunk.request_id);
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id,
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: format!(
                        "chunked DFS read response size does not match object metadata: expected {expected_size}, actual {}",
                        chunk.total_size_bytes
                    ),
                },
            }));
        }
        if chunk.total_chunks == 0 {
            self.pending_reads.remove(&chunk.request_id);
            self.incoming_read_transfers.remove(&chunk.request_id);
            self.incoming_read_memory_transfers
                .remove(&chunk.request_id);
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id.clone(),
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: format!(
                        "chunked DFS read response {} has no chunks",
                        chunk.request_id
                    ),
                },
            }));
        }

        let Some(chunk_content_hash) = chunk.content_hash.clone() else {
            self.pending_reads.remove(&chunk.request_id);
            self.incoming_read_transfers.remove(&chunk.request_id);
            self.incoming_read_memory_transfers
                .remove(&chunk.request_id);
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id,
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: "chunked DFS read response is missing content hash".to_string(),
                },
            }));
        };

        if let Err(error) = validate_chunk_bytes(
            "chunked DFS read response",
            chunk.total_size_bytes,
            chunk.chunk_index,
            chunk.total_chunks,
            &chunk.bytes,
        )
        .and_then(|()| {
            validate_chunk_hash(
                "chunked DFS read response",
                chunk.chunk_index,
                &chunk.bytes,
                chunk.chunk_hash.as_deref(),
            )
        }) {
            self.pending_reads.remove(&chunk.request_id);
            self.incoming_read_transfers.remove(&chunk.request_id);
            self.incoming_read_memory_transfers
                .remove(&chunk.request_id);
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id,
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: error.to_string(),
                },
            }));
        }

        if !pending.cache_response {
            return self.handle_networked_uncached_read_response_chunk(
                source_node_id,
                chunk,
                pending,
                chunk_content_hash,
            );
        }

        if !self.incoming_read_transfers.contains_key(&chunk.request_id) {
            if let Err(error) = validate_chunked_transfer_shape(
                "chunked DFS read response",
                chunk.total_size_bytes,
                chunk.total_chunks,
            ) {
                self.clear_read_response_transfer_state(&chunk.request_id);
                self.mark_remote_read_location_missing(
                    &pending.object_id,
                    &pending.version,
                    &pending.content_hash,
                    &source_node_id,
                )?;
                return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                    request_id: chunk.request_id,
                    object_id: chunk.object_id,
                    source_node_id,
                    result: DfsReadResult::Rejected {
                        reason: error.to_string(),
                    },
                }));
            }
            let assembler = match self.content.begin_chunked_object_version(
                chunk.object_id.as_str(),
                chunk.version.as_str(),
                &chunk_content_hash,
                chunk.total_size_bytes,
                chunk.total_chunks,
                DFS_BLOB_CHUNK_BYTES,
            ) {
                Ok(assembler) => assembler,
                Err(error) => {
                    self.clear_read_response_transfer_state(&chunk.request_id);
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                    return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: chunk.request_id,
                        object_id: chunk.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: error.to_string(),
                        },
                    }));
                }
            };
            let transfer = match InProgressReadTransfer::new(
                chunk.object_id.clone(),
                chunk.version.clone(),
                chunk_content_hash.clone(),
                chunk.total_size_bytes,
                chunk.total_chunks,
                assembler,
            ) {
                Ok(transfer) => transfer,
                Err(error) => {
                    self.clear_read_response_transfer_state(&chunk.request_id);
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                    return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: chunk.request_id,
                        object_id: chunk.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: error.to_string(),
                        },
                    }));
                }
            };
            self.incoming_read_transfers
                .insert(chunk.request_id.clone(), transfer);
        }
        let transfer = self
            .incoming_read_transfers
            .get_mut(&chunk.request_id)
            .expect("transfer inserted");
        if transfer.object_id != chunk.object_id
            || transfer.version != chunk.version
            || transfer.content_hash != chunk_content_hash
            || transfer.total_size_bytes() != chunk.total_size_bytes
        {
            self.pending_reads.remove(&chunk.request_id);
            self.incoming_read_transfers.remove(&chunk.request_id);
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id.clone(),
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: format!(
                        "conflicting DFS read response chunk metadata for {}",
                        chunk.request_id
                    ),
                },
            }));
        }
        let request_id = chunk.request_id.clone();
        let object_id = chunk.object_id.clone();
        let version = chunk.version.clone();
        let content_hash = chunk_content_hash.clone();
        let transfer_complete =
            match transfer.insert_chunk(chunk.chunk_index, chunk.total_chunks, chunk.bytes) {
                Ok(true) => true,
                Ok(false) => false,
                Err(error) => {
                    self.clear_read_response_transfer_state(&chunk.request_id);
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                    return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: chunk.request_id,
                        object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: error.to_string(),
                        },
                    }));
                }
            };
        let next_chunk_index = transfer.contiguous_received_chunk_count();
        if let Some(pending) = self.pending_reads.get_mut(&request_id) {
            pending.next_chunk_index = pending.next_chunk_index.max(next_chunk_index);
        }
        if !transfer_complete {
            return self.request_next_read_window_if_due(
                &request_id,
                &source_node_id,
                next_chunk_index,
                chunk.total_chunks,
            );
        }
        let transfer = self
            .incoming_read_transfers
            .remove(&request_id)
            .expect("completed read transfer");
        self.pending_reads.remove(&request_id);
        let manifest = match self
            .content
            .commit_chunked_object_version(transfer.chunks.assembler)
        {
            Ok(manifest) => manifest,
            Err(error) => {
                self.mark_remote_read_location_missing(
                    &pending.object_id,
                    &pending.version,
                    &pending.content_hash,
                    &source_node_id,
                )?;
                return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                    request_id,
                    object_id,
                    source_node_id,
                    result: DfsReadResult::Rejected {
                        reason: error.to_string(),
                    },
                }));
            }
        };
        if manifest.content_hash != content_hash {
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id,
                object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: DfsError::ContentHashMismatch {
                        expected: content_hash,
                        actual: manifest.content_hash,
                    }
                    .to_string(),
                },
            }));
        }
        let data = DfsReadData {
            object_id: object_id.clone(),
            version,
            content_hash: Some(manifest.content_hash),
            bytes: Vec::new(),
        };
        if let Err(error) = self.store_remote_read_manifest(&data, manifest.size_bytes) {
            if matches!(error, DfsError::ContentHashMismatch { .. }) {
                self.mark_remote_read_location_missing(
                    &pending.object_id,
                    &pending.version,
                    &pending.content_hash,
                    &source_node_id,
                )?;
                return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                    request_id,
                    object_id,
                    source_node_id,
                    result: DfsReadResult::Rejected {
                        reason: error.to_string(),
                    },
                }));
            }
            return Err(error);
        }
        self.clear_pending_remote_reads_for_object_version(
            &data.object_id,
            &data.version,
            &data.content_hash,
        );
        Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
            request_id,
            object_id,
            source_node_id,
            result: DfsReadResult::Found(data),
        }))
    }

    pub(crate) fn handle_networked_uncached_read_response_chunk(
        &mut self,
        source_node_id: NodeId,
        chunk: DfsReadResponseChunk,
        pending: PendingNetworkedRead,
        chunk_content_hash: String,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        if !self
            .incoming_read_memory_transfers
            .contains_key(&chunk.request_id)
        {
            let transfer = match InProgressReadMemoryTransfer::new(
                chunk.object_id.clone(),
                chunk.version.clone(),
                chunk_content_hash.clone(),
                chunk.total_size_bytes,
                chunk.total_chunks,
                DFS_BLOB_CHUNK_BYTES,
            ) {
                Ok(transfer) => transfer,
                Err(error) => {
                    self.clear_read_response_transfer_state(&chunk.request_id);
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                    return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: chunk.request_id,
                        object_id: chunk.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: error.to_string(),
                        },
                    }));
                }
            };
            self.incoming_read_memory_transfers
                .insert(chunk.request_id.clone(), transfer);
        }
        let transfer = self
            .incoming_read_memory_transfers
            .get_mut(&chunk.request_id)
            .expect("transfer inserted");
        if transfer.object_id != chunk.object_id
            || transfer.version != chunk.version
            || transfer.content_hash != chunk_content_hash
            || transfer.total_size_bytes() != chunk.total_size_bytes
        {
            self.clear_read_response_transfer_state(&chunk.request_id);
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id.clone(),
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: format!(
                        "conflicting DFS read response chunk metadata for {}",
                        chunk.request_id
                    ),
                },
            }));
        }
        let request_id = chunk.request_id.clone();
        let object_id = chunk.object_id.clone();
        let version = chunk.version.clone();
        let content_hash = chunk_content_hash.clone();
        let total_chunks = chunk.total_chunks;
        let transfer_complete =
            match transfer.insert_chunk(chunk.chunk_index, chunk.total_chunks, chunk.bytes) {
                Ok(complete) => complete,
                Err(error) => {
                    self.clear_read_response_transfer_state(&chunk.request_id);
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                    return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: chunk.request_id,
                        object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: error.to_string(),
                        },
                    }));
                }
            };
        let next_chunk_index = transfer.contiguous_received_chunk_count();
        if let Some(pending) = self.pending_reads.get_mut(&request_id) {
            pending.next_chunk_index = pending.next_chunk_index.max(next_chunk_index);
        }
        if !transfer_complete {
            return self.request_next_read_window_if_due(
                &request_id,
                &source_node_id,
                next_chunk_index,
                total_chunks,
            );
        }
        let transfer = self
            .incoming_read_memory_transfers
            .remove(&request_id)
            .expect("completed memory read transfer");
        self.pending_reads.remove(&request_id);
        let bytes = match transfer.into_bytes() {
            Ok(bytes) => bytes,
            Err(error) => {
                self.mark_remote_read_location_missing(
                    &pending.object_id,
                    &pending.version,
                    &pending.content_hash,
                    &source_node_id,
                )?;
                return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                    request_id,
                    object_id,
                    source_node_id,
                    result: DfsReadResult::Rejected {
                        reason: error.to_string(),
                    },
                }));
            }
        };
        let data = DfsReadData {
            object_id: object_id.clone(),
            version,
            content_hash: Some(content_hash),
            bytes,
        };
        if let Err(error) = self.validate_remote_read_data_for_pending(&pending, &data) {
            if matches!(error, DfsError::ContentHashMismatch { .. }) {
                self.mark_remote_read_location_missing(
                    &pending.object_id,
                    &pending.version,
                    &pending.content_hash,
                    &source_node_id,
                )?;
                return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                    request_id,
                    object_id,
                    source_node_id,
                    result: DfsReadResult::Rejected {
                        reason: error.to_string(),
                    },
                }));
            }
            return Err(error);
        }
        self.clear_transient_remote_read_location_prunes_for_object(
            &data.object_id,
            &data.version,
            &data.content_hash,
        );
        self.clear_pending_remote_reads_for_object_version(
            &data.object_id,
            &data.version,
            &data.content_hash,
        );
        Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
            request_id,
            object_id,
            source_node_id,
            result: DfsReadResult::Found(data),
        }))
    }

    fn request_next_read_window_if_due(
        &mut self,
        request_id: &str,
        source_node_id: &NodeId,
        next_chunk_index: u32,
        total_chunks: u32,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        if next_chunk_index >= total_chunks {
            return Ok(None);
        }
        let Some(pending) = self.pending_reads.get(request_id).cloned() else {
            return Ok(None);
        };
        let Some(window_end) = pending.window_end_chunk_index else {
            return Ok(None);
        };
        if next_chunk_index < window_end {
            return Ok(None);
        }
        let transfer_size_bytes = pending
            .range_max_bytes
            .map(|max_bytes| max_bytes as u64)
            .or(pending.content_size_bytes)
            .unwrap_or_else(|| u64::from(total_chunks) * DFS_BLOB_CHUNK_BYTES as u64);
        let max_chunks = self.adaptive_read_chunk_window(source_node_id, transfer_size_bytes);
        let sent = self.send_dfs_message_if_routable(
            source_node_id,
            DfsNetworkMessage::ReadRequest(DfsReadRequest {
                request_id: request_id.to_string(),
                object_id: pending.object_id.clone(),
                version: Some(pending.version.clone()),
                content_hash: pending.content_hash.clone(),
                content_size_bytes: pending.content_size_bytes,
                range_offset: pending.range_offset,
                range_max_bytes: pending.range_max_bytes,
                start_chunk_index: next_chunk_index,
                max_chunks,
            }),
        )?;
        if sent {
            if let Some(pending) = self.pending_reads.get_mut(request_id) {
                pending.next_chunk_index = next_chunk_index;
                pending.window_end_chunk_index =
                    Self::read_window_end_chunk_index(next_chunk_index, max_chunks);
                pending.next_retry_at = Instant::now() + DFS_REMOTE_READ_RETRY_DELAY;
            }
            return Ok(None);
        }

        self.clear_read_response_transfer_state(request_id);
        self.mark_remote_read_location_missing(
            &pending.object_id,
            &pending.version,
            &pending.content_hash,
            source_node_id,
        )?;
        Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
            request_id: request_id.to_string(),
            object_id: pending.object_id,
            source_node_id: source_node_id.clone(),
            result: DfsReadResult::Rejected {
                reason: "unable to route DFS read continuation request".to_string(),
            },
        }))
    }

    fn clear_read_response_transfer_state(&mut self, request_id: &str) {
        self.pending_reads.remove(request_id);
        self.incoming_read_transfers.remove(request_id);
        self.incoming_read_memory_transfers.remove(request_id);
    }

    fn pending_read_metadata_advanced(
        &self,
        pending: &PendingNetworkedRead,
    ) -> Result<bool, DfsError> {
        let object = self.current_read_object_metadata(&pending.object_id)?;
        Ok(object.version != pending.version
            || object.content_hash != pending.content_hash
            || object.content_size_bytes != pending.content_size_bytes)
    }

    fn pending_read_source_still_current(
        &self,
        pending: &PendingNetworkedRead,
        source_node_id: &NodeId,
    ) -> Result<bool, DfsError> {
        let object = self.current_read_object_metadata(&pending.object_id)?;
        Ok(object.version == pending.version
            && object.content_hash == pending.content_hash
            && object.content_size_bytes == pending.content_size_bytes
            && object.current_locations.contains(source_node_id))
    }

    pub(crate) fn clear_pending_remote_reads_for_object_version(
        &mut self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &Option<String>,
    ) {
        let request_ids: Vec<String> = self
            .pending_reads
            .iter()
            .filter(|(_, pending)| {
                pending.object_id == *object_id
                    && pending.version == *version
                    && pending.content_hash == *content_hash
            })
            .map(|(request_id, _)| request_id.clone())
            .collect();
        for request_id in request_ids {
            self.pending_reads.remove(&request_id);
            self.incoming_read_transfers.remove(&request_id);
            self.incoming_read_memory_transfers.remove(&request_id);
        }
    }

    pub(crate) fn store_remote_read_data(
        &mut self,
        _source_node_id: &NodeId,
        data: &DfsReadData,
    ) -> Result<(), DfsError> {
        let (expected_hash, size_bytes) = self.validate_remote_read_data(data)?;
        self.store_command_bytes(&data.object_id, &data.version, &expected_hash, &data.bytes)?;
        self.store_remote_read_manifest(data, size_bytes)
    }

    fn validate_remote_read_data(&self, data: &DfsReadData) -> Result<(String, u64), DfsError> {
        let object = self.current_read_object_metadata(&data.object_id)?;
        if object.version != data.version {
            return Err(DfsError::ContentHashMismatch {
                expected: object.version.to_string(),
                actual: data.version.to_string(),
            });
        }
        let Some(expected_hash) = object.content_hash.clone() else {
            return Err(DfsError::ContentHashMismatch {
                expected: "committed content hash".to_string(),
                actual: data
                    .content_hash
                    .clone()
                    .unwrap_or_else(|| "none".to_string()),
            });
        };
        if let Some(actual_hash) = data.content_hash.as_ref()
            && actual_hash != &expected_hash
        {
            return Err(DfsError::ContentHashMismatch {
                expected: expected_hash,
                actual: actual_hash.clone(),
            });
        }
        let actual_hash = content_hash(&data.bytes);
        if actual_hash != expected_hash {
            return Err(DfsError::ContentHashMismatch {
                expected: expected_hash,
                actual: actual_hash,
            });
        }
        let size_bytes = data.bytes.len() as u64;
        if let Some(expected_size) = object.content_size_bytes
            && expected_size != size_bytes
        {
            return Err(DfsError::Distributed {
                message: format!(
                    "remote read size does not match object metadata: expected {expected_size}, actual {size_bytes}"
                ),
            });
        }
        Ok((expected_hash, size_bytes))
    }

    fn validate_remote_read_data_for_pending(
        &self,
        pending: &PendingNetworkedRead,
        data: &DfsReadData,
    ) -> Result<(String, u64), DfsError> {
        if pending.object_id != data.object_id {
            return Err(DfsError::ContentHashMismatch {
                expected: pending.object_id.to_string(),
                actual: data.object_id.to_string(),
            });
        }
        if pending.version != data.version {
            return Err(DfsError::ContentHashMismatch {
                expected: pending.version.to_string(),
                actual: data.version.to_string(),
            });
        }
        let Some(expected_hash) = pending.content_hash.clone() else {
            return Err(DfsError::ContentHashMismatch {
                expected: "committed content hash".to_string(),
                actual: data
                    .content_hash
                    .clone()
                    .unwrap_or_else(|| "none".to_string()),
            });
        };
        if let Some(actual_hash) = data.content_hash.as_ref()
            && actual_hash != &expected_hash
        {
            return Err(DfsError::ContentHashMismatch {
                expected: expected_hash,
                actual: actual_hash.clone(),
            });
        }
        let actual_hash = content_hash(&data.bytes);
        if actual_hash != expected_hash {
            return Err(DfsError::ContentHashMismatch {
                expected: expected_hash,
                actual: actual_hash,
            });
        }
        let size_bytes = data.bytes.len() as u64;
        if let Some(expected_size) = pending.content_size_bytes
            && expected_size != size_bytes
        {
            return Err(DfsError::Distributed {
                message: format!(
                    "remote read size does not match pending metadata: expected {expected_size}, actual {size_bytes}"
                ),
            });
        }
        Ok((expected_hash, size_bytes))
    }

    pub(crate) fn store_remote_read_manifest(
        &mut self,
        data: &DfsReadData,
        size_bytes: u64,
    ) -> Result<(), DfsError> {
        let object = self.current_read_object_metadata(&data.object_id)?;
        if object.version != data.version {
            return Err(DfsError::ContentHashMismatch {
                expected: object.version.to_string(),
                actual: data.version.to_string(),
            });
        }
        let Some(expected_hash) = object.content_hash.clone() else {
            return Err(DfsError::ContentHashMismatch {
                expected: "committed content hash".to_string(),
                actual: data
                    .content_hash
                    .clone()
                    .unwrap_or_else(|| "none".to_string()),
            });
        };
        if let Some(actual_hash) = data.content_hash.as_ref()
            && actual_hash != &expected_hash
        {
            return Err(DfsError::ContentHashMismatch {
                expected: expected_hash,
                actual: actual_hash.clone(),
            });
        }
        if let Some(expected_size) = object.content_size_bytes
            && expected_size != size_bytes
        {
            return Err(DfsError::Distributed {
                message: format!(
                    "remote read size does not match object metadata: expected {expected_size}, actual {size_bytes}"
                ),
            });
        }
        let mut object = object;
        object.content_size_bytes = Some(size_bytes);
        if self.is_local_metadata_primary_for_object(&object)? {
            self.put_object_version_size_in_metadata_backend(&object, Some(size_bytes))?;
        }
        self.metadata.add_object(object)?;
        self.clear_transient_remote_read_location_prunes_for_object(
            &data.object_id,
            &data.version,
            &data.content_hash,
        );
        Ok(())
    }
}
