//! Outbound DFS data-plane message sending helpers.
//!
//! Inbound transfer validation lives in `service_transfer`; this module owns
//! the companion helpers that package local content into stage, repair, fetch,
//! and best-effort routed messages.

use std::io::Read;

use sinomo_network::NetworkError;
use sinomo_node::NodeId;

#[cfg(test)]
use crate::transfer::blob_chunk_count;
use crate::transfer::{
    DFS_BLOB_CHUNK_BYTES, blob_chunk_count_for_size, blob_chunk_len, chunk_hash,
    repair_replica_chunk_start_and_window,
};
use crate::*;

impl<T> NetworkedDfsService<T>
where
    T: AsyncDfsMessageTransport + DfsTransportIdentity + DfsMessageTransport,
{
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn send_stage_patch_window_if_routable_async(
        &mut self,
        target_node_id: &NodeId,
        request_id: String,
        intent_id: &str,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        base_version: &DfsVersion,
        version: &DfsVersion,
        content_hash: &str,
        offset: u64,
        patch_hash: &str,
        patch_size_bytes: u64,
        start_chunk_index: u32,
        max_chunks: u32,
        object_metadata: Option<DfsObjectMetadata>,
    ) -> Result<StageBlobSendOutcome, DfsError> {
        let authority_shard = self.command_authority_shard_for_object(object_metadata.as_ref())?;
        if patch_size_bytes <= DFS_BLOB_INLINE_LIMIT_BYTES as u64 {
            let bytes = self.read_patch_blob_range(patch_hash, 0, patch_size_bytes)?;
            let sent = self
                .send_best_effort_message_if_routable_async(
                    target_node_id,
                    DfsNetworkMessage::StagePatch(DfsStagePatchCommand {
                        request_id,
                        intent_id: intent_id.to_string(),
                        file_system_id: file_system_id.clone(),
                        object_id: object_id.clone(),
                        base_version: base_version.clone(),
                        version: version.clone(),
                        content_hash: content_hash.to_string(),
                        offset,
                        patch_hash: patch_hash.to_string(),
                        bytes,
                        object_metadata,
                        authority_shard,
                    }),
                )
                .await?;
            return Ok(StageBlobSendOutcome {
                sent,
                complete: sent,
                next_chunk_index: 0,
            });
        }

        let total_size_bytes = patch_size_bytes;
        let total_chunks = blob_chunk_count_for_size(total_size_bytes)?;
        if start_chunk_index >= total_chunks {
            return Ok(StageBlobSendOutcome {
                sent: true,
                complete: true,
                next_chunk_index: total_chunks,
            });
        }
        let end_chunk_index = start_chunk_index
            .saturating_add(max_chunks.max(1))
            .min(total_chunks);
        for chunk_index in start_chunk_index..end_chunk_index {
            let chunk_offset = u64::from(chunk_index).saturating_mul(DFS_BLOB_CHUNK_BYTES as u64);
            let chunk_len = blob_chunk_len(total_size_bytes, chunk_index)?;
            let chunk = self.read_patch_blob_range(patch_hash, chunk_offset, chunk_len as u64)?;
            let sent = self
                .send_best_effort_message_if_routable_async(
                    target_node_id,
                    DfsNetworkMessage::StagePatchChunk(DfsStagePatchChunkCommand {
                        request_id: request_id.clone(),
                        intent_id: intent_id.to_string(),
                        file_system_id: file_system_id.clone(),
                        object_id: object_id.clone(),
                        base_version: base_version.clone(),
                        version: version.clone(),
                        content_hash: content_hash.to_string(),
                        offset,
                        patch_hash: patch_hash.to_string(),
                        total_size_bytes,
                        chunk_index,
                        total_chunks,
                        chunk_hash: Some(chunk_hash(&chunk)),
                        bytes: chunk,
                        object_metadata: object_metadata.clone(),
                        authority_shard: authority_shard.clone(),
                    }),
                )
                .await?;
            if !sent {
                return Ok(StageBlobSendOutcome {
                    sent: false,
                    complete: false,
                    next_chunk_index: chunk_index,
                });
            }
        }
        Ok(StageBlobSendOutcome {
            sent: true,
            complete: end_chunk_index >= total_chunks,
            next_chunk_index: end_chunk_index,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn send_stage_blob_window_if_routable_async(
        &mut self,
        target_node_id: &NodeId,
        request_id: String,
        intent_id: &str,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        expected_base_version: Option<DfsVersion>,
        version: &DfsVersion,
        content_hash: &str,
        total_size_bytes: u64,
        start_chunk_index: u32,
        max_chunks: u32,
        object_metadata: Option<DfsObjectMetadata>,
    ) -> Result<StageBlobSendOutcome, DfsError> {
        let authority_shard = self.command_authority_shard_for_object(object_metadata.as_ref())?;
        if total_size_bytes <= DFS_BLOB_INLINE_LIMIT_BYTES as u64 {
            let mut reader = self.open_content_hash_range_stream(
                object_id,
                version,
                content_hash,
                Some(total_size_bytes),
                0,
                usize::try_from(total_size_bytes).unwrap_or(usize::MAX),
            )?;
            let bytes = read_small_blob_from_reader(content_hash, total_size_bytes, &mut reader)?;
            let sent = self
                .send_best_effort_message_if_routable_async(
                    target_node_id,
                    DfsNetworkMessage::StageBlob(DfsStageBlobCommand {
                        request_id,
                        intent_id: intent_id.to_string(),
                        file_system_id: file_system_id.clone(),
                        object_id: object_id.clone(),
                        expected_base_version,
                        version: version.clone(),
                        content_hash: content_hash.to_string(),
                        bytes,
                        object_metadata,
                        authority_shard,
                    }),
                )
                .await?;
            return Ok(StageBlobSendOutcome {
                sent,
                complete: sent,
                next_chunk_index: 0,
            });
        }

        self.send_object_version_chunk_window_if_routable_async(
            target_node_id,
            object_id,
            version,
            content_hash,
            total_size_bytes,
            start_chunk_index,
            max_chunks,
            "stage",
            |chunk_index, total_chunks, bytes| {
                DfsNetworkMessage::StageBlobChunk(DfsStageBlobChunkCommand {
                    request_id: request_id.clone(),
                    intent_id: intent_id.to_string(),
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    expected_base_version: expected_base_version.clone(),
                    version: version.clone(),
                    content_hash: content_hash.to_string(),
                    total_size_bytes,
                    chunk_index,
                    total_chunks,
                    chunk_hash: Some(chunk_hash(&bytes)),
                    bytes,
                    object_metadata: object_metadata.clone(),
                    authority_shard: authority_shard.clone(),
                })
            },
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn send_repair_replica_window_if_routable_async(
        &mut self,
        target_node_id: &NodeId,
        request_id: String,
        intent_id: &str,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        expected_base_version: Option<DfsVersion>,
        version: &DfsVersion,
        content_hash: &str,
        total_size_bytes: u64,
        start_chunk_index: u32,
        max_chunks: u32,
        object_metadata: Option<DfsObjectMetadata>,
    ) -> Result<StageBlobSendOutcome, DfsError> {
        let authority_shard = self.command_authority_shard_for_object(object_metadata.as_ref())?;
        if total_size_bytes <= DFS_BLOB_INLINE_LIMIT_BYTES as u64 {
            let mut reader = self.open_content_hash_range_stream(
                object_id,
                version,
                content_hash,
                Some(total_size_bytes),
                0,
                usize::try_from(total_size_bytes).unwrap_or(usize::MAX),
            )?;
            let bytes = read_small_blob_from_reader(content_hash, total_size_bytes, &mut reader)?;
            let sent = self
                .send_best_effort_message_if_routable_async(
                    target_node_id,
                    DfsNetworkMessage::RepairReplica(DfsRepairReplicaCommand {
                        request_id: request_id.clone(),
                        intent_id: intent_id.to_string(),
                        file_system_id: file_system_id.clone(),
                        object_id: object_id.clone(),
                        expected_base_version,
                        version: version.clone(),
                        content_hash: content_hash.to_string(),
                        bytes,
                        object_metadata,
                        authority_shard,
                    }),
                )
                .await?;
            return Ok(StageBlobSendOutcome {
                sent,
                complete: sent,
                next_chunk_index: 0,
            });
        }

        let total_chunks = blob_chunk_count_for_size(total_size_bytes)?;
        let (start_chunk_index, max_chunks) =
            repair_replica_chunk_start_and_window(start_chunk_index, max_chunks, total_chunks);
        self.send_object_version_chunk_window_if_routable_async(
            target_node_id,
            object_id,
            version,
            content_hash,
            total_size_bytes,
            start_chunk_index,
            max_chunks,
            "repair",
            |chunk_index, total_chunks, bytes| {
                DfsNetworkMessage::RepairReplicaChunk(DfsRepairReplicaChunkCommand {
                    request_id: request_id.clone(),
                    intent_id: intent_id.to_string(),
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    expected_base_version: expected_base_version.clone(),
                    version: version.clone(),
                    content_hash: content_hash.to_string(),
                    total_size_bytes,
                    chunk_index,
                    total_chunks,
                    chunk_hash: Some(chunk_hash(&bytes)),
                    bytes,
                    object_metadata: object_metadata.clone(),
                    authority_shard: authority_shard.clone(),
                })
            },
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn send_object_version_chunk_window_if_routable_async(
        &mut self,
        target_node_id: &NodeId,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &str,
        total_size_bytes: u64,
        start_chunk_index: u32,
        max_chunks: u32,
        operation_label: &str,
        mut message_for_chunk: impl FnMut(u32, u32, Vec<u8>) -> DfsNetworkMessage,
    ) -> Result<StageBlobSendOutcome, DfsError> {
        let total_chunks = blob_chunk_count_for_size(total_size_bytes)?;
        if start_chunk_index >= total_chunks {
            return Ok(StageBlobSendOutcome {
                sent: true,
                complete: true,
                next_chunk_index: total_chunks,
            });
        }
        let max_chunks = max_chunks.max(1);
        let end_chunk_index = start_chunk_index
            .saturating_add(max_chunks)
            .min(total_chunks);
        let start_offset = u64::from(start_chunk_index) * DFS_BLOB_CHUNK_BYTES as u64;
        let end_offset = if end_chunk_index >= total_chunks {
            total_size_bytes
        } else {
            u64::from(end_chunk_index) * DFS_BLOB_CHUNK_BYTES as u64
        };
        let range_len = usize::try_from(end_offset.saturating_sub(start_offset)).map_err(|_| {
            DfsError::Distributed {
                message: format!(
                    "DFS {operation_label} chunk window is too large: {start_offset}..{end_offset}"
                ),
            }
        })?;
        tracing::debug!(
            target: "sinomo.dfs.transfer",
            local_node_id = %self.node_id,
            target_node_id = %target_node_id,
            operation = %operation_label,
            object_id = %object_id,
            version = %version,
            content_hash = %content_hash,
            total_size_bytes,
            start_chunk_index,
            end_chunk_index,
            total_chunks,
            range_len,
            "sending DFS chunk window"
        );
        let mut reader = self.open_content_hash_range_stream(
            object_id,
            version,
            content_hash,
            Some(total_size_bytes),
            start_offset,
            range_len,
        )?;
        let mut buffer = vec![0_u8; DFS_BLOB_CHUNK_BYTES];
        for chunk_index in start_chunk_index..end_chunk_index {
            let chunk_len = blob_chunk_len(total_size_bytes, chunk_index)?;
            reader
                .read_exact(&mut buffer[..chunk_len])
                .map_err(|source| DfsError::ContentStore {
                    message: format!(
                        "failed to read blob {content_hash} {operation_label} chunk {chunk_index}: {source}"
                    ),
                })?;
            let sent = self
                .send_best_effort_message_if_routable_async(
                    target_node_id,
                    message_for_chunk(chunk_index, total_chunks, buffer[..chunk_len].to_vec()),
                )
                .await?;
            if !sent {
                tracing::warn!(
                    target: "sinomo.dfs.transfer",
                    local_node_id = %self.node_id,
                    target_node_id = %target_node_id,
                    operation = %operation_label,
                    object_id = %object_id,
                    version = %version,
                    content_hash = %content_hash,
                    failed_chunk_index = chunk_index,
                    total_chunks,
                    "DFS chunk window stopped because target was not routable"
                );
                return Ok(StageBlobSendOutcome {
                    sent: false,
                    complete: false,
                    next_chunk_index: chunk_index,
                });
            }
        }
        tracing::debug!(
            target: "sinomo.dfs.transfer",
            local_node_id = %self.node_id,
            target_node_id = %target_node_id,
            operation = %operation_label,
            object_id = %object_id,
            version = %version,
            content_hash = %content_hash,
            complete = end_chunk_index >= total_chunks,
            next_chunk_index = end_chunk_index,
            total_chunks,
            "sent DFS chunk window"
        );
        Ok(StageBlobSendOutcome {
            sent: true,
            complete: end_chunk_index >= total_chunks,
            next_chunk_index: end_chunk_index,
        })
    }

    pub(crate) async fn send_best_effort_message_if_routable_async(
        &mut self,
        target_node_id: &NodeId,
        message: DfsNetworkMessage,
    ) -> Result<bool, DfsError> {
        match self
            .send_dfs_message_if_routable_async(target_node_id, message)
            .await
        {
            Ok(sent) => Ok(sent),
            Err(DfsError::Network { .. }) => Ok(false),
            Err(error) => Err(error),
        }
    }
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn send_dfs_message_if_routable(
        &mut self,
        target_node_id: &NodeId,
        message: DfsNetworkMessage,
    ) -> Result<bool, DfsError> {
        let routed_message = self.next_dfs_routed_message(target_node_id, message);
        match self
            .transport
            .send_routed_message(target_node_id, routed_message)
        {
            Ok(()) => Ok(true),
            Err(NetworkError::UnknownNode { .. }) => Ok(false),
            Err(error) => Err(DfsError::from_network(error)),
        }
    }

    pub fn flush_detached_transport_sends(&mut self) -> Result<(), DfsError> {
        self.transport
            .flush_detached_sends()
            .map_err(DfsError::from_network)
    }

    pub fn flush_deferred_content_durability(&mut self) -> Result<(), DfsError> {
        self.content.flush_deferred_durable_dirs()?;
        Ok(())
    }

    pub(crate) fn send_dfs_message_if_routable_detached(
        &mut self,
        target_node_id: &NodeId,
        message: DfsNetworkMessage,
    ) -> Result<bool, DfsError> {
        let routed_message = self.next_dfs_routed_message(target_node_id, message);
        match self
            .transport
            .send_detached_routed_message(target_node_id, routed_message)
        {
            Ok(()) => Ok(true),
            Err(NetworkError::UnknownNode { .. }) => Ok(false),
            Err(error) => Err(DfsError::from_network(error)),
        }
    }

    #[cfg(test)]
    pub(crate) fn send_stage_blob_if_routable(
        &mut self,
        target_node_id: &NodeId,
        request_id: String,
        intent_id: &str,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        expected_base_version: Option<DfsVersion>,
        version: &DfsVersion,
        content_hash: &str,
        bytes: &[u8],
        object_metadata: Option<DfsObjectMetadata>,
    ) -> Result<bool, DfsError> {
        let authority_shard = self.command_authority_shard_for_object(object_metadata.as_ref())?;
        self.send_blob_bytes_if_routable_for_test(
            target_node_id,
            bytes,
            |bytes| {
                DfsNetworkMessage::StageBlob(DfsStageBlobCommand {
                    request_id: request_id.clone(),
                    intent_id: intent_id.to_string(),
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    expected_base_version: expected_base_version.clone(),
                    version: version.clone(),
                    content_hash: content_hash.to_string(),
                    bytes,
                    object_metadata: object_metadata.clone(),
                    authority_shard: authority_shard.clone(),
                })
            },
            |chunk_index, total_chunks, chunk| {
                DfsNetworkMessage::StageBlobChunk(DfsStageBlobChunkCommand {
                    request_id: request_id.clone(),
                    intent_id: intent_id.to_string(),
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    expected_base_version: expected_base_version.clone(),
                    version: version.clone(),
                    content_hash: content_hash.to_string(),
                    total_size_bytes: bytes.len() as u64,
                    chunk_index: chunk_index as u32,
                    total_chunks,
                    chunk_hash: Some(chunk_hash(chunk)),
                    bytes: chunk.to_vec(),
                    object_metadata: object_metadata.clone(),
                    authority_shard: authority_shard.clone(),
                })
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn send_stage_patch_window_if_routable(
        &mut self,
        target_node_id: &NodeId,
        request_id: String,
        intent_id: &str,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        base_version: &DfsVersion,
        version: &DfsVersion,
        content_hash: &str,
        offset: u64,
        patch_hash: &str,
        patch_size_bytes: u64,
        start_chunk_index: u32,
        max_chunks: u32,
        object_metadata: Option<DfsObjectMetadata>,
    ) -> Result<StageBlobSendOutcome, DfsError> {
        let authority_shard = self.command_authority_shard_for_object(object_metadata.as_ref())?;
        if patch_size_bytes <= DFS_BLOB_INLINE_LIMIT_BYTES as u64 {
            let bytes = self.read_patch_blob_range(patch_hash, 0, patch_size_bytes)?;
            let sent = self.send_best_effort_message_if_routable(
                target_node_id,
                DfsNetworkMessage::StagePatch(DfsStagePatchCommand {
                    request_id,
                    intent_id: intent_id.to_string(),
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    base_version: base_version.clone(),
                    version: version.clone(),
                    content_hash: content_hash.to_string(),
                    offset,
                    patch_hash: patch_hash.to_string(),
                    bytes,
                    object_metadata,
                    authority_shard,
                }),
            )?;
            return Ok(StageBlobSendOutcome {
                sent,
                complete: sent,
                next_chunk_index: 0,
            });
        }

        let total_size_bytes = patch_size_bytes;
        let total_chunks = blob_chunk_count_for_size(total_size_bytes)?;
        if start_chunk_index >= total_chunks {
            return Ok(StageBlobSendOutcome {
                sent: true,
                complete: true,
                next_chunk_index: total_chunks,
            });
        }
        let end_chunk_index = start_chunk_index
            .saturating_add(max_chunks.max(1))
            .min(total_chunks);
        for chunk_index in start_chunk_index..end_chunk_index {
            let chunk_offset = u64::from(chunk_index).saturating_mul(DFS_BLOB_CHUNK_BYTES as u64);
            let chunk_len = blob_chunk_len(total_size_bytes, chunk_index)?;
            let chunk = self.read_patch_blob_range(patch_hash, chunk_offset, chunk_len as u64)?;
            let sent = self.send_best_effort_message_if_routable(
                target_node_id,
                DfsNetworkMessage::StagePatchChunk(DfsStagePatchChunkCommand {
                    request_id: request_id.clone(),
                    intent_id: intent_id.to_string(),
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    base_version: base_version.clone(),
                    version: version.clone(),
                    content_hash: content_hash.to_string(),
                    offset,
                    patch_hash: patch_hash.to_string(),
                    total_size_bytes,
                    chunk_index,
                    total_chunks,
                    chunk_hash: Some(chunk_hash(&chunk)),
                    bytes: chunk,
                    object_metadata: object_metadata.clone(),
                    authority_shard: authority_shard.clone(),
                }),
            )?;
            if !sent {
                return Ok(StageBlobSendOutcome {
                    sent: false,
                    complete: false,
                    next_chunk_index: chunk_index,
                });
            }
        }
        Ok(StageBlobSendOutcome {
            sent: true,
            complete: end_chunk_index >= total_chunks,
            next_chunk_index: end_chunk_index,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn send_stage_blob_window_if_routable(
        &mut self,
        target_node_id: &NodeId,
        request_id: String,
        intent_id: &str,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        expected_base_version: Option<DfsVersion>,
        version: &DfsVersion,
        content_hash: &str,
        total_size_bytes: u64,
        start_chunk_index: u32,
        max_chunks: u32,
        object_metadata: Option<DfsObjectMetadata>,
    ) -> Result<StageBlobSendOutcome, DfsError> {
        let authority_shard = self.command_authority_shard_for_object(object_metadata.as_ref())?;
        if total_size_bytes <= DFS_BLOB_INLINE_LIMIT_BYTES as u64 {
            let mut reader = self.open_content_hash_range_stream(
                object_id,
                version,
                content_hash,
                Some(total_size_bytes),
                0,
                usize::try_from(total_size_bytes).unwrap_or(usize::MAX),
            )?;
            let bytes = read_small_blob_from_reader(content_hash, total_size_bytes, &mut reader)?;
            let sent = self.send_best_effort_message_if_routable(
                target_node_id,
                DfsNetworkMessage::StageBlob(DfsStageBlobCommand {
                    request_id,
                    intent_id: intent_id.to_string(),
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    expected_base_version,
                    version: version.clone(),
                    content_hash: content_hash.to_string(),
                    bytes,
                    object_metadata,
                    authority_shard,
                }),
            )?;
            return Ok(StageBlobSendOutcome {
                sent,
                complete: sent,
                next_chunk_index: 0,
            });
        }

        self.send_object_version_chunk_window_if_routable(
            target_node_id,
            object_id,
            version,
            content_hash,
            total_size_bytes,
            start_chunk_index,
            max_chunks,
            "stage",
            |chunk_index, total_chunks, bytes| {
                DfsNetworkMessage::StageBlobChunk(DfsStageBlobChunkCommand {
                    request_id: request_id.clone(),
                    intent_id: intent_id.to_string(),
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    expected_base_version: expected_base_version.clone(),
                    version: version.clone(),
                    content_hash: content_hash.to_string(),
                    total_size_bytes,
                    chunk_index,
                    total_chunks,
                    chunk_hash: Some(chunk_hash(&bytes)),
                    bytes,
                    object_metadata: object_metadata.clone(),
                    authority_shard: authority_shard.clone(),
                })
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn send_repair_replica_window_if_routable(
        &mut self,
        target_node_id: &NodeId,
        request_id: String,
        intent_id: &str,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        expected_base_version: Option<DfsVersion>,
        version: &DfsVersion,
        content_hash: &str,
        total_size_bytes: u64,
        start_chunk_index: u32,
        max_chunks: u32,
        object_metadata: Option<DfsObjectMetadata>,
    ) -> Result<StageBlobSendOutcome, DfsError> {
        let authority_shard = self.command_authority_shard_for_object(object_metadata.as_ref())?;
        if total_size_bytes <= DFS_BLOB_INLINE_LIMIT_BYTES as u64 {
            let mut reader = self.open_content_hash_range_stream(
                object_id,
                version,
                content_hash,
                Some(total_size_bytes),
                0,
                usize::try_from(total_size_bytes).unwrap_or(usize::MAX),
            )?;
            let bytes = read_small_blob_from_reader(content_hash, total_size_bytes, &mut reader)?;
            let sent = self.send_best_effort_message_if_routable(
                target_node_id,
                DfsNetworkMessage::RepairReplica(DfsRepairReplicaCommand {
                    request_id: request_id.clone(),
                    intent_id: intent_id.to_string(),
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    expected_base_version,
                    version: version.clone(),
                    content_hash: content_hash.to_string(),
                    bytes,
                    object_metadata,
                    authority_shard,
                }),
            )?;
            return Ok(StageBlobSendOutcome {
                sent,
                complete: sent,
                next_chunk_index: 0,
            });
        }

        let total_chunks = blob_chunk_count_for_size(total_size_bytes)?;
        let (start_chunk_index, max_chunks) =
            repair_replica_chunk_start_and_window(start_chunk_index, max_chunks, total_chunks);
        self.send_object_version_chunk_window_if_routable(
            target_node_id,
            object_id,
            version,
            content_hash,
            total_size_bytes,
            start_chunk_index,
            max_chunks,
            "repair",
            |chunk_index, total_chunks, bytes| {
                DfsNetworkMessage::RepairReplicaChunk(DfsRepairReplicaChunkCommand {
                    request_id: request_id.clone(),
                    intent_id: intent_id.to_string(),
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    expected_base_version: expected_base_version.clone(),
                    version: version.clone(),
                    content_hash: content_hash.to_string(),
                    total_size_bytes,
                    chunk_index,
                    total_chunks,
                    chunk_hash: Some(chunk_hash(&bytes)),
                    bytes,
                    object_metadata: object_metadata.clone(),
                    authority_shard: authority_shard.clone(),
                })
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn send_object_version_chunk_window_if_routable(
        &mut self,
        target_node_id: &NodeId,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &str,
        total_size_bytes: u64,
        start_chunk_index: u32,
        max_chunks: u32,
        operation_label: &str,
        mut message_for_chunk: impl FnMut(u32, u32, Vec<u8>) -> DfsNetworkMessage,
    ) -> Result<StageBlobSendOutcome, DfsError> {
        let total_chunks = blob_chunk_count_for_size(total_size_bytes)?;
        if start_chunk_index >= total_chunks {
            return Ok(StageBlobSendOutcome {
                sent: true,
                complete: true,
                next_chunk_index: total_chunks,
            });
        }
        let max_chunks = max_chunks.max(1);
        let end_chunk_index = start_chunk_index
            .saturating_add(max_chunks)
            .min(total_chunks);
        let start_offset = u64::from(start_chunk_index) * DFS_BLOB_CHUNK_BYTES as u64;
        let end_offset = if end_chunk_index >= total_chunks {
            total_size_bytes
        } else {
            u64::from(end_chunk_index) * DFS_BLOB_CHUNK_BYTES as u64
        };
        let range_len = usize::try_from(end_offset.saturating_sub(start_offset)).map_err(|_| {
            DfsError::Distributed {
                message: format!(
                    "DFS {operation_label} chunk window is too large: {start_offset}..{end_offset}"
                ),
            }
        })?;
        tracing::debug!(
            target: "sinomo.dfs.transfer",
            local_node_id = %self.node_id,
            target_node_id = %target_node_id,
            operation = %operation_label,
            object_id = %object_id,
            version = %version,
            content_hash = %content_hash,
            total_size_bytes,
            start_chunk_index,
            end_chunk_index,
            total_chunks,
            range_len,
            "sending DFS chunk window"
        );
        let mut reader = self.open_content_hash_range_stream(
            object_id,
            version,
            content_hash,
            Some(total_size_bytes),
            start_offset,
            range_len,
        )?;
        let mut buffer = vec![0_u8; DFS_BLOB_CHUNK_BYTES];
        for chunk_index in start_chunk_index..end_chunk_index {
            let chunk_len = blob_chunk_len(total_size_bytes, chunk_index)?;
            reader
                .read_exact(&mut buffer[..chunk_len])
                .map_err(|source| DfsError::ContentStore {
                    message: format!(
                        "failed to read blob {content_hash} {operation_label} chunk {chunk_index}: {source}"
                    ),
                })?;
            let sent = self.send_best_effort_message_if_routable(
                target_node_id,
                message_for_chunk(chunk_index, total_chunks, buffer[..chunk_len].to_vec()),
            )?;
            if !sent {
                tracing::warn!(
                    target: "sinomo.dfs.transfer",
                    local_node_id = %self.node_id,
                    target_node_id = %target_node_id,
                    operation = %operation_label,
                    object_id = %object_id,
                    version = %version,
                    content_hash = %content_hash,
                    failed_chunk_index = chunk_index,
                    total_chunks,
                    "DFS chunk window stopped because target was not routable"
                );
                return Ok(StageBlobSendOutcome {
                    sent: false,
                    complete: false,
                    next_chunk_index: chunk_index,
                });
            }
        }
        tracing::debug!(
            target: "sinomo.dfs.transfer",
            local_node_id = %self.node_id,
            target_node_id = %target_node_id,
            operation = %operation_label,
            object_id = %object_id,
            version = %version,
            content_hash = %content_hash,
            complete = end_chunk_index >= total_chunks,
            next_chunk_index = end_chunk_index,
            total_chunks,
            "sent DFS chunk window"
        );
        Ok(StageBlobSendOutcome {
            sent: true,
            complete: end_chunk_index >= total_chunks,
            next_chunk_index: end_chunk_index,
        })
    }

    pub(crate) fn send_best_effort_message_if_routable(
        &mut self,
        target_node_id: &NodeId,
        message: DfsNetworkMessage,
    ) -> Result<bool, DfsError> {
        match self.send_dfs_message_if_routable(target_node_id, message) {
            Ok(sent) => Ok(sent),
            Err(DfsError::Network { .. }) => Ok(false),
            Err(error) => Err(error),
        }
    }

    #[cfg(test)]
    pub(crate) fn send_repair_replica_if_routable(
        &mut self,
        target_node_id: &NodeId,
        request_id: String,
        intent_id: &str,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        expected_base_version: Option<DfsVersion>,
        version: &DfsVersion,
        content_hash: &str,
        bytes: &[u8],
        object_metadata: Option<DfsObjectMetadata>,
    ) -> Result<bool, DfsError> {
        let authority_shard = self.command_authority_shard_for_object(object_metadata.as_ref())?;
        self.send_blob_bytes_if_routable_for_test(
            target_node_id,
            bytes,
            |bytes| {
                DfsNetworkMessage::RepairReplica(DfsRepairReplicaCommand {
                    request_id: request_id.clone(),
                    intent_id: intent_id.to_string(),
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    expected_base_version: expected_base_version.clone(),
                    version: version.clone(),
                    content_hash: content_hash.to_string(),
                    bytes,
                    object_metadata: object_metadata.clone(),
                    authority_shard: authority_shard.clone(),
                })
            },
            |chunk_index, total_chunks, chunk| {
                DfsNetworkMessage::RepairReplicaChunk(DfsRepairReplicaChunkCommand {
                    request_id: request_id.clone(),
                    intent_id: intent_id.to_string(),
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    expected_base_version: expected_base_version.clone(),
                    version: version.clone(),
                    content_hash: content_hash.to_string(),
                    total_size_bytes: bytes.len() as u64,
                    chunk_index: chunk_index as u32,
                    total_chunks,
                    chunk_hash: Some(chunk_hash(chunk)),
                    bytes: chunk.to_vec(),
                    object_metadata: object_metadata.clone(),
                    authority_shard: authority_shard.clone(),
                })
            },
        )
    }

    #[cfg(test)]
    fn send_blob_bytes_if_routable_for_test(
        &mut self,
        target_node_id: &NodeId,
        bytes: &[u8],
        mut inline_message: impl FnMut(Vec<u8>) -> DfsNetworkMessage,
        mut chunk_message: impl FnMut(u32, u32, &[u8]) -> DfsNetworkMessage,
    ) -> Result<bool, DfsError> {
        if bytes.len() <= DFS_BLOB_INLINE_LIMIT_BYTES {
            return self.send_best_effort_message_if_routable(
                target_node_id,
                inline_message(bytes.to_vec()),
            );
        }

        let total_chunks = blob_chunk_count(bytes)?;
        for (chunk_index, chunk) in bytes.chunks(DFS_BLOB_CHUNK_BYTES).enumerate() {
            let sent = self.send_best_effort_message_if_routable(
                target_node_id,
                chunk_message(chunk_index as u32, total_chunks, chunk),
            )?;
            if !sent {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(crate) fn next_request_id(&mut self) -> String {
        let request_id = format!("dfs_net_req_{}", self.next_request_id);
        self.next_request_id += 1;
        request_id
    }
}

pub(crate) fn read_small_blob_from_reader(
    content_hash: &str,
    total_size_bytes: u64,
    reader: &mut ObjectVersionReader,
) -> Result<Vec<u8>, DfsError> {
    let size = usize::try_from(total_size_bytes).map_err(|_| DfsError::Distributed {
        message: format!("DFS blob is too large to inline: {total_size_bytes} bytes"),
    })?;
    let mut bytes = vec![0_u8; size];
    reader
        .read_exact(&mut bytes)
        .map_err(|source| DfsError::ContentStore {
            message: format!("failed to read inline blob {content_hash}: {source}"),
        })?;
    Ok(bytes)
}
