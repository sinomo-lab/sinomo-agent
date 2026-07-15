//! Outbound DFS data-plane message sending helpers.
//!
//! Inbound transfer validation lives in the focused transfer modules; this
//! module owns the companion helpers that package local content into stage,
//! repair, fetch, and best-effort routed messages.

use std::io::Read;

use sinomo_core::events::EventId;
use sinomo_core::network::NetworkError;
use sinomo_core::node::NodeId;

use crate::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DfsBestEffortMessageSendOutcome {
    pub(crate) sent: bool,
    pub(crate) last_error: Option<String>,
}

/// Refinement of `StageFanoutCap` in the verified repair-content model.
pub(crate) const DFS_REPLICA_BODY_FANOUT_CAP: usize = 2;

fn validate_replica_body_fanout_outcomes(
    expected_targets: &[NodeId],
    outcomes: Vec<DfsRoutedBodySendOutcome>,
) -> Result<Vec<StageBlobSendOutcome>, DfsError> {
    if outcomes.len() != expected_targets.len() {
        return Err(DfsError::Distributed {
            message: format!(
                "DFS replica body fan-out returned {} outcomes for {} targets",
                outcomes.len(),
                expected_targets.len()
            ),
        });
    }
    expected_targets
        .iter()
        .zip(outcomes)
        .map(|(expected_target, outcome)| {
            if &outcome.target_node_id != expected_target {
                return Err(DfsError::Distributed {
                    message: format!(
                        "DFS replica body fan-out expected target {expected_target}, got {}",
                        outcome.target_node_id
                    ),
                });
            }
            Ok(match outcome.result {
                Ok(()) => StageBlobSendOutcome {
                    sent: true,
                    last_error: None,
                },
                Err(NetworkError::UnknownNode { .. }) => StageBlobSendOutcome {
                    sent: false,
                    last_error: None,
                },
                Err(error) => StageBlobSendOutcome {
                    sent: false,
                    last_error: Some(error.to_string()),
                },
            })
        })
        .collect()
}

impl<T> NetworkedDfsService<T>
where
    T: AsyncDfsMessageTransport + DfsTransportIdentity + DfsMessageTransport,
{
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn send_stage_patch_body_if_routable_async(
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
                last_error: None,
            });
        }

        let reader = self.open_patch_blob_stream(patch_hash, patch_size_bytes)?;
        let send_outcome = self
            .send_best_effort_message_body_if_routable_async(
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
                    bytes: Vec::new(),
                    object_metadata,
                    authority_shard,
                }),
                DfsOutboundBody::from_reader(reader, patch_size_bytes, patch_hash),
            )
            .await?;
        Ok(StageBlobSendOutcome {
            sent: send_outcome.sent,
            last_error: send_outcome.last_error,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn send_stage_blob_body_if_routable_async(
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
                last_error: None,
            });
        }

        let (_, reader) = self.open_content_hash_stream(
            object_id,
            version,
            content_hash,
            Some(total_size_bytes),
        )?;
        let send_outcome = self
            .send_best_effort_message_body_if_routable_async(
                target_node_id,
                DfsNetworkMessage::StageBlob(DfsStageBlobCommand {
                    request_id,
                    intent_id: intent_id.to_string(),
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    expected_base_version,
                    version: version.clone(),
                    content_hash: content_hash.to_string(),
                    bytes: Vec::new(),
                    object_metadata,
                    authority_shard,
                }),
                DfsOutboundBody::from_reader(reader, total_size_bytes, content_hash),
            )
            .await?;
        Ok(StageBlobSendOutcome {
            sent: send_outcome.sent,
            last_error: send_outcome.last_error,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn send_repair_replica_body_if_routable_async(
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
            let send_outcome = self
                .send_best_effort_message_if_routable_outcome_async(
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
                sent: send_outcome.sent,
                last_error: send_outcome.last_error,
            });
        }

        let (_, reader) = self.open_content_hash_stream(
            object_id,
            version,
            content_hash,
            Some(total_size_bytes),
        )?;
        let send_outcome = self
            .send_best_effort_message_body_if_routable_async(
                target_node_id,
                DfsNetworkMessage::RepairReplica(DfsRepairReplicaCommand {
                    request_id,
                    intent_id: intent_id.to_string(),
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    expected_base_version,
                    version: version.clone(),
                    content_hash: content_hash.to_string(),
                    bytes: Vec::new(),
                    object_metadata,
                    authority_shard,
                }),
                DfsOutboundBody::from_reader(reader, total_size_bytes, content_hash),
            )
            .await?;
        Ok(StageBlobSendOutcome {
            sent: send_outcome.sent,
            last_error: send_outcome.last_error,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn send_stage_blob_bodies_if_routable_async(
        &mut self,
        targets: &[(NodeId, String)],
        intent_id: &str,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        expected_base_version: Option<DfsVersion>,
        version: &DfsVersion,
        content_hash: &str,
        total_size_bytes: u64,
        object_metadata: Option<DfsObjectMetadata>,
    ) -> Result<Vec<StageBlobSendOutcome>, DfsError> {
        if targets.is_empty() {
            return Ok(Vec::new());
        }
        if total_size_bytes <= DFS_BLOB_INLINE_LIMIT_BYTES as u64 {
            let mut outcomes = Vec::with_capacity(targets.len());
            for (target_node_id, request_id) in targets {
                outcomes.push(
                    self.send_stage_blob_body_if_routable_async(
                        target_node_id,
                        request_id.clone(),
                        intent_id,
                        file_system_id,
                        object_id,
                        expected_base_version.clone(),
                        version,
                        content_hash,
                        total_size_bytes,
                        object_metadata.clone(),
                    )
                    .await?,
                );
            }
            return Ok(outcomes);
        }

        let authority_shard = self.command_authority_shard_for_object(object_metadata.as_ref())?;
        let mut requests = Vec::with_capacity(targets.len());
        for (target_node_id, request_id) in targets {
            let (_, reader) = self.open_content_hash_stream(
                object_id,
                version,
                content_hash,
                Some(total_size_bytes),
            )?;
            let message = DfsNetworkMessage::StageBlob(DfsStageBlobCommand {
                request_id: request_id.clone(),
                intent_id: intent_id.to_string(),
                file_system_id: file_system_id.clone(),
                object_id: object_id.clone(),
                expected_base_version: expected_base_version.clone(),
                version: version.clone(),
                content_hash: content_hash.to_string(),
                bytes: Vec::new(),
                object_metadata: object_metadata.clone(),
                authority_shard: authority_shard.clone(),
            });
            let routed_message = self.next_dfs_routed_message(target_node_id, message);
            requests.push(DfsRoutedBodySend::new(
                target_node_id.clone(),
                routed_message,
                DfsOutboundBody::from_reader(reader, total_size_bytes, content_hash),
            ));
        }
        let expected_targets = targets
            .iter()
            .map(|(target, _)| target.clone())
            .collect::<Vec<_>>();
        let outcomes = self
            .transport
            .send_routed_message_bodies_bounded_async(requests, DFS_REPLICA_BODY_FANOUT_CAP)
            .await
            .map_err(DfsError::from_network)?;
        validate_replica_body_fanout_outcomes(&expected_targets, outcomes)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn send_repair_replica_bodies_if_routable_async(
        &mut self,
        targets: &[(NodeId, String)],
        intent_id: &str,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        expected_base_version: Option<DfsVersion>,
        version: &DfsVersion,
        content_hash: &str,
        total_size_bytes: u64,
        object_metadata: Option<DfsObjectMetadata>,
    ) -> Result<Vec<StageBlobSendOutcome>, DfsError> {
        if targets.is_empty() {
            return Ok(Vec::new());
        }
        if total_size_bytes <= DFS_BLOB_INLINE_LIMIT_BYTES as u64 {
            let mut outcomes = Vec::with_capacity(targets.len());
            for (target_node_id, request_id) in targets {
                outcomes.push(
                    self.send_repair_replica_body_if_routable_async(
                        target_node_id,
                        request_id.clone(),
                        intent_id,
                        file_system_id,
                        object_id,
                        expected_base_version.clone(),
                        version,
                        content_hash,
                        total_size_bytes,
                        object_metadata.clone(),
                    )
                    .await?,
                );
            }
            return Ok(outcomes);
        }

        let authority_shard = self.command_authority_shard_for_object(object_metadata.as_ref())?;
        let mut requests = Vec::with_capacity(targets.len());
        for (target_node_id, request_id) in targets {
            let (_, reader) = self.open_content_hash_stream(
                object_id,
                version,
                content_hash,
                Some(total_size_bytes),
            )?;
            let message = DfsNetworkMessage::RepairReplica(DfsRepairReplicaCommand {
                request_id: request_id.clone(),
                intent_id: intent_id.to_string(),
                file_system_id: file_system_id.clone(),
                object_id: object_id.clone(),
                expected_base_version: expected_base_version.clone(),
                version: version.clone(),
                content_hash: content_hash.to_string(),
                bytes: Vec::new(),
                object_metadata: object_metadata.clone(),
                authority_shard: authority_shard.clone(),
            });
            let routed_message = self.next_dfs_routed_message(target_node_id, message);
            requests.push(DfsRoutedBodySend::new(
                target_node_id.clone(),
                routed_message,
                DfsOutboundBody::from_reader(reader, total_size_bytes, content_hash),
            ));
        }
        let expected_targets = targets
            .iter()
            .map(|(target, _)| target.clone())
            .collect::<Vec<_>>();
        let outcomes = self
            .transport
            .send_routed_message_bodies_bounded_async(requests, DFS_REPLICA_BODY_FANOUT_CAP)
            .await
            .map_err(DfsError::from_network)?;
        validate_replica_body_fanout_outcomes(&expected_targets, outcomes)
    }

    pub(crate) async fn send_best_effort_message_if_routable_async(
        &mut self,
        target_node_id: &NodeId,
        message: DfsNetworkMessage,
    ) -> Result<bool, DfsError> {
        self.send_best_effort_message_if_routable_outcome_async(target_node_id, message)
            .await
            .map(|outcome| outcome.sent)
    }

    pub(crate) async fn send_best_effort_message_body_if_routable_async(
        &mut self,
        target_node_id: &NodeId,
        message: DfsNetworkMessage,
        body: DfsOutboundBody,
    ) -> Result<DfsBestEffortMessageSendOutcome, DfsError> {
        let routed_message = self.next_dfs_routed_message(target_node_id, message);
        match self
            .transport
            .send_routed_message_with_body_async(target_node_id, routed_message, body)
            .await
        {
            Ok(()) => Ok(DfsBestEffortMessageSendOutcome {
                sent: true,
                last_error: None,
            }),
            Err(NetworkError::UnknownNode { .. }) => Ok(DfsBestEffortMessageSendOutcome {
                sent: false,
                last_error: None,
            }),
            Err(error) => Ok(DfsBestEffortMessageSendOutcome {
                sent: false,
                last_error: Some(error.to_string()),
            }),
        }
    }

    pub(crate) async fn send_best_effort_message_if_routable_outcome_async(
        &mut self,
        target_node_id: &NodeId,
        message: DfsNetworkMessage,
    ) -> Result<DfsBestEffortMessageSendOutcome, DfsError> {
        match self
            .send_dfs_message_if_routable_async(target_node_id, message)
            .await
        {
            Ok(sent) => Ok(DfsBestEffortMessageSendOutcome {
                sent,
                last_error: None,
            }),
            Err(DfsError::Network { message }) => Ok(DfsBestEffortMessageSendOutcome {
                sent: false,
                last_error: Some(message),
            }),
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

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn send_stage_patch_body_if_routable(
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
                last_error: None,
            });
        }

        let reader = self.open_patch_blob_stream(patch_hash, patch_size_bytes)?;
        let send_outcome = self.send_best_effort_message_body_if_routable(
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
                bytes: Vec::new(),
                object_metadata,
                authority_shard,
            }),
            DfsOutboundBody::from_reader(reader, patch_size_bytes, patch_hash),
        )?;
        Ok(StageBlobSendOutcome {
            sent: send_outcome.sent,
            last_error: send_outcome.last_error,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn send_stage_blob_body_if_routable(
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
                last_error: None,
            });
        }

        let (_, reader) = self.open_content_hash_stream(
            object_id,
            version,
            content_hash,
            Some(total_size_bytes),
        )?;
        let send_outcome = self.send_best_effort_message_body_if_routable(
            target_node_id,
            DfsNetworkMessage::StageBlob(DfsStageBlobCommand {
                request_id,
                intent_id: intent_id.to_string(),
                file_system_id: file_system_id.clone(),
                object_id: object_id.clone(),
                expected_base_version,
                version: version.clone(),
                content_hash: content_hash.to_string(),
                bytes: Vec::new(),
                object_metadata,
                authority_shard,
            }),
            DfsOutboundBody::from_reader(reader, total_size_bytes, content_hash),
        )?;
        Ok(StageBlobSendOutcome {
            sent: send_outcome.sent,
            last_error: send_outcome.last_error,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn send_repair_replica_body_if_routable(
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
            let send_outcome = self.send_best_effort_message_if_routable_outcome(
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
                sent: send_outcome.sent,
                last_error: send_outcome.last_error,
            });
        }

        let (_, reader) = self.open_content_hash_stream(
            object_id,
            version,
            content_hash,
            Some(total_size_bytes),
        )?;
        let send_outcome = self.send_best_effort_message_body_if_routable(
            target_node_id,
            DfsNetworkMessage::RepairReplica(DfsRepairReplicaCommand {
                request_id,
                intent_id: intent_id.to_string(),
                file_system_id: file_system_id.clone(),
                object_id: object_id.clone(),
                expected_base_version,
                version: version.clone(),
                content_hash: content_hash.to_string(),
                bytes: Vec::new(),
                object_metadata,
                authority_shard,
            }),
            DfsOutboundBody::from_reader(reader, total_size_bytes, content_hash),
        )?;
        Ok(StageBlobSendOutcome {
            sent: send_outcome.sent,
            last_error: send_outcome.last_error,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn send_stage_blob_bodies_if_routable(
        &mut self,
        targets: &[(NodeId, String)],
        intent_id: &str,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        expected_base_version: Option<DfsVersion>,
        version: &DfsVersion,
        content_hash: &str,
        total_size_bytes: u64,
        object_metadata: Option<DfsObjectMetadata>,
    ) -> Result<Vec<StageBlobSendOutcome>, DfsError> {
        if targets.is_empty() {
            return Ok(Vec::new());
        }
        if total_size_bytes <= DFS_BLOB_INLINE_LIMIT_BYTES as u64 {
            return targets
                .iter()
                .map(|(target_node_id, request_id)| {
                    self.send_stage_blob_body_if_routable(
                        target_node_id,
                        request_id.clone(),
                        intent_id,
                        file_system_id,
                        object_id,
                        expected_base_version.clone(),
                        version,
                        content_hash,
                        total_size_bytes,
                        object_metadata.clone(),
                    )
                })
                .collect();
        }

        let authority_shard = self.command_authority_shard_for_object(object_metadata.as_ref())?;
        let mut requests = Vec::with_capacity(targets.len());
        for (target_node_id, request_id) in targets {
            let (_, reader) = self.open_content_hash_stream(
                object_id,
                version,
                content_hash,
                Some(total_size_bytes),
            )?;
            let message = DfsNetworkMessage::StageBlob(DfsStageBlobCommand {
                request_id: request_id.clone(),
                intent_id: intent_id.to_string(),
                file_system_id: file_system_id.clone(),
                object_id: object_id.clone(),
                expected_base_version: expected_base_version.clone(),
                version: version.clone(),
                content_hash: content_hash.to_string(),
                bytes: Vec::new(),
                object_metadata: object_metadata.clone(),
                authority_shard: authority_shard.clone(),
            });
            let routed_message = self.next_dfs_routed_message(target_node_id, message);
            requests.push(DfsRoutedBodySend::new(
                target_node_id.clone(),
                routed_message,
                DfsOutboundBody::from_reader(reader, total_size_bytes, content_hash),
            ));
        }
        let expected_targets = targets
            .iter()
            .map(|(target, _)| target.clone())
            .collect::<Vec<_>>();
        let outcomes = self
            .transport
            .send_routed_message_bodies_bounded(requests, DFS_REPLICA_BODY_FANOUT_CAP)
            .map_err(DfsError::from_network)?;
        validate_replica_body_fanout_outcomes(&expected_targets, outcomes)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn send_repair_replica_bodies_if_routable(
        &mut self,
        targets: &[(NodeId, String)],
        intent_id: &str,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        expected_base_version: Option<DfsVersion>,
        version: &DfsVersion,
        content_hash: &str,
        total_size_bytes: u64,
        object_metadata: Option<DfsObjectMetadata>,
    ) -> Result<Vec<StageBlobSendOutcome>, DfsError> {
        if targets.is_empty() {
            return Ok(Vec::new());
        }
        if total_size_bytes <= DFS_BLOB_INLINE_LIMIT_BYTES as u64 {
            return targets
                .iter()
                .map(|(target_node_id, request_id)| {
                    self.send_repair_replica_body_if_routable(
                        target_node_id,
                        request_id.clone(),
                        intent_id,
                        file_system_id,
                        object_id,
                        expected_base_version.clone(),
                        version,
                        content_hash,
                        total_size_bytes,
                        object_metadata.clone(),
                    )
                })
                .collect();
        }

        let authority_shard = self.command_authority_shard_for_object(object_metadata.as_ref())?;
        let mut requests = Vec::with_capacity(targets.len());
        for (target_node_id, request_id) in targets {
            let (_, reader) = self.open_content_hash_stream(
                object_id,
                version,
                content_hash,
                Some(total_size_bytes),
            )?;
            let message = DfsNetworkMessage::RepairReplica(DfsRepairReplicaCommand {
                request_id: request_id.clone(),
                intent_id: intent_id.to_string(),
                file_system_id: file_system_id.clone(),
                object_id: object_id.clone(),
                expected_base_version: expected_base_version.clone(),
                version: version.clone(),
                content_hash: content_hash.to_string(),
                bytes: Vec::new(),
                object_metadata: object_metadata.clone(),
                authority_shard: authority_shard.clone(),
            });
            let routed_message = self.next_dfs_routed_message(target_node_id, message);
            requests.push(DfsRoutedBodySend::new(
                target_node_id.clone(),
                routed_message,
                DfsOutboundBody::from_reader(reader, total_size_bytes, content_hash),
            ));
        }
        let expected_targets = targets
            .iter()
            .map(|(target, _)| target.clone())
            .collect::<Vec<_>>();
        let outcomes = self
            .transport
            .send_routed_message_bodies_bounded(requests, DFS_REPLICA_BODY_FANOUT_CAP)
            .map_err(DfsError::from_network)?;
        validate_replica_body_fanout_outcomes(&expected_targets, outcomes)
    }

    pub(crate) fn send_best_effort_message_if_routable(
        &mut self,
        target_node_id: &NodeId,
        message: DfsNetworkMessage,
    ) -> Result<bool, DfsError> {
        self.send_best_effort_message_if_routable_outcome(target_node_id, message)
            .map(|outcome| outcome.sent)
    }

    /// Queue one message without waiting for the target to process it. Replica
    /// promotion uses this after visible commit so delivery cannot extend the
    /// W-visible response path. Production served transports retain failed
    /// CommitVersion work until a matching positive acknowledgement; if a
    /// transport cannot queue at all, durable intent/receipt hydration can
    /// reconstruct the exact idempotent promotion after restart.
    pub(crate) fn send_best_effort_message_if_routable_detached(
        &mut self,
        target_node_id: &NodeId,
        message: DfsNetworkMessage,
    ) -> Result<bool, DfsError> {
        match self.send_dfs_message_if_routable_detached(target_node_id, message) {
            Ok(sent) => Ok(sent),
            Err(DfsError::Network { .. }) => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub(crate) fn send_best_effort_message_body_if_routable(
        &mut self,
        target_node_id: &NodeId,
        message: DfsNetworkMessage,
        body: DfsOutboundBody,
    ) -> Result<DfsBestEffortMessageSendOutcome, DfsError> {
        let routed_message = self.next_dfs_routed_message(target_node_id, message);
        match self
            .transport
            .send_routed_message_with_body(target_node_id, routed_message, body)
        {
            Ok(()) => Ok(DfsBestEffortMessageSendOutcome {
                sent: true,
                last_error: None,
            }),
            Err(NetworkError::UnknownNode { .. }) => Ok(DfsBestEffortMessageSendOutcome {
                sent: false,
                last_error: None,
            }),
            Err(error) => Ok(DfsBestEffortMessageSendOutcome {
                sent: false,
                last_error: Some(error.to_string()),
            }),
        }
    }

    pub(crate) fn send_best_effort_message_if_routable_outcome(
        &mut self,
        target_node_id: &NodeId,
        message: DfsNetworkMessage,
    ) -> Result<DfsBestEffortMessageSendOutcome, DfsError> {
        match self.send_dfs_message_if_routable(target_node_id, message) {
            Ok(sent) => Ok(DfsBestEffortMessageSendOutcome {
                sent,
                last_error: None,
            }),
            Err(DfsError::Network { message }) => Ok(DfsBestEffortMessageSendOutcome {
                sent: false,
                last_error: Some(message),
            }),
            Err(error) => Err(error),
        }
    }

    pub(crate) fn next_request_id(&mut self) -> String {
        // The sequence is useful in traces, while the event suffix makes the
        // operation identity unique across daemon/service restarts. Exclusive
        // create replay safety must never alias a fresh request after a local
        // counter reset.
        let request_id = format!(
            "dfs_net_req_{}_{}",
            self.next_request_id,
            EventId::generate()
        );
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
