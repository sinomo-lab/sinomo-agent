//! Outbound DFS data-plane message sending helpers.
//!
//! Inbound transfer validation lives in the focused transfer modules; this
//! module owns the companion helpers that package local content into stage,
//! repair, fetch, and best-effort routed messages.

use sinomo_core::events::EventId;
use sinomo_core::network::NetworkError;
use sinomo_core::node::NodeId;

use crate::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DfsBestEffortMessageSendOutcome {
    pub(crate) sent: bool,
    pub(crate) last_error: Option<String>,
    pub(crate) delivery_state: Option<sinomo_core::network::NetworkDeliveryState>,
}

/// Refinement of `StageFanoutCap` in the verified repair-content model.
pub(crate) const DFS_REPLICA_BODY_FANOUT_CAP: usize = 2;

#[derive(Clone, Copy)]
enum ReplicaTransferKind {
    Stage,
    Repair,
    BackgroundRepair,
}

struct ReplicaTransfer<'a> {
    intent_id: &'a str,
    file_system_id: &'a DfsFileSystemId,
    object_id: &'a DfsObjectId,
    expected_base_version: Option<DfsVersion>,
    version: &'a DfsVersion,
    content_hash: &'a str,
    total_size_bytes: u64,
    object_metadata: Option<DfsObjectMetadata>,
    kind: ReplicaTransferKind,
}

pub(crate) struct PreparedReplicaSends {
    requests: Vec<DfsRoutedSend>,
    expected_targets: Vec<NodeId>,
}

impl PreparedReplicaSends {
    pub(crate) fn into_requests(self) -> Vec<DfsRoutedSend> {
        self.requests
    }

}

fn validate_replica_body_fanout_outcomes(
    expected_targets: &[NodeId],
    outcomes: Vec<DfsRoutedSendOutcome>,
) -> Result<Vec<StageBlobSendOutcome>, DfsError> {
    if outcomes.len() != expected_targets.len() {
        return Err(DfsError::Distributed {
            message: format!(
                "DFS replica attachment fan-out returned {} outcomes for {} targets",
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
                        "DFS replica attachment fan-out expected target {expected_target}, got {}",
                        outcome.target_node_id
                    ),
                });
            }
            Ok(match outcome.result {
                Ok(()) => StageBlobSendOutcome {
                    delivery_state: Some(
                        sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
                    ),
                    sent: true,
                    last_error: None,
                },
                Err(NetworkError::UnknownNode { .. }) => StageBlobSendOutcome {
                    delivery_state: None,
                    sent: false,
                    last_error: None,
                },
                Err(error) => StageBlobSendOutcome {
                    delivery_state: error.delivery_state(),
                    sent: false,
                    last_error: Some(error.to_string()),
                },
            })
        })
        .collect()
}

fn replica_transfer_identity(message: &DfsNetworkMessage) -> Option<(String, String, bool)> {
    match message {
        DfsNetworkMessage::StageBlob(command) | DfsNetworkMessage::StageRetainedBlob(command) => {
            Some((command.intent_id.clone(), command.request_id.clone(), true))
        }
        DfsNetworkMessage::StagePatch(command) => {
            Some((command.intent_id.clone(), command.request_id.clone(), true))
        }
        DfsNetworkMessage::RepairRetainedBlob(command) => {
            Some((command.intent_id.clone(), command.request_id.clone(), false))
        }
        DfsNetworkMessage::RepairReplica(command) => {
            Some((command.intent_id.clone(), command.request_id.clone(), false))
        }
        _ => None,
    }
}

impl<T> NetworkedDfsService<T>
where
    T: AsyncDfsMessageTransport + DfsTransportIdentity + DfsMessageTransport,
{
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn send_stage_patch_attachment_if_routable_async(
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
        let receipt_generation =
            self.backend_receipt_slot_generation(object_id, version, target_node_id)?;
        let reader = self.open_patch_blob_stream(patch_hash, patch_size_bytes)?;
        let send_outcome = self
            .send_best_effort_message_attachment_if_routable_async(
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
                    receipt_generation,
                }),
                DfsOutboundAttachment::from_reader(reader, patch_size_bytes, patch_hash),
            )
            .await?;
        Ok(StageBlobSendOutcome {
            delivery_state: send_outcome.delivery_state,
            sent: send_outcome.sent,
            last_error: send_outcome.last_error,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn send_stage_blob_attachments_if_routable_async(
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
        let PreparedReplicaSends {
            requests,
            expected_targets,
        } = self.prepare_replica_attachments(
            targets,
            ReplicaTransfer {
                intent_id,
                file_system_id,
                object_id,
                expected_base_version,
                version,
                content_hash,
                total_size_bytes,
                object_metadata,
                kind: ReplicaTransferKind::Stage,
            },
        )?;
        let identities = self.begin_replica_sends(&requests)?;
        let outcomes = {
            #[cfg(any(test, feature = "test-support"))]
            let _native_context = self.native_stage_send_context(&requests);
            self.transport
                .send_routed_calls_bounded_async(requests, DFS_REPLICA_BODY_FANOUT_CAP)
        }
        .await;
        let outcomes = self.observe_replica_fanout_result(&identities, outcomes)?;
        self.finish_replica_sends(&identities, &expected_targets, outcomes)
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

    pub(crate) async fn send_best_effort_message_attachment_if_routable_async(
        &mut self,
        target_node_id: &NodeId,
        message: DfsNetworkMessage,
        attachment: DfsOutboundAttachment,
    ) -> Result<DfsBestEffortMessageSendOutcome, DfsError> {
        let transfer = replica_transfer_identity(&message);
        let background_repair = matches!(&message, DfsNetworkMessage::RepairReplica(command) if command.background_repair);
        if let Some((intent, request_id, stage)) = &transfer {
            self.begin_replica_transfer_attempt(
                intent,
                target_node_id,
                request_id,
                *stage,
                background_repair,
            )?;
        }
        let routed_message = self.next_dfs_routed_message(target_node_id, message);
        let outcome = match self
            .transport
            .send_routed_call_async(
                target_node_id,
                DfsRoutedCall::with_attachment(routed_message, attachment),
            )
            .await
        {
            Ok(()) => DfsBestEffortMessageSendOutcome {
                delivery_state: Some(sinomo_core::network::NetworkDeliveryState::PossiblyApplied),
                sent: true,
                last_error: None,
            },
            Err(NetworkError::UnknownNode { .. }) => DfsBestEffortMessageSendOutcome {
                delivery_state: None,
                sent: false,
                last_error: None,
            },
            Err(error) => DfsBestEffortMessageSendOutcome {
                delivery_state: error.delivery_state(),
                sent: false,
                last_error: Some(error.to_string()),
            },
        };
        if let Some((intent, request_id, stage)) = transfer {
            self.observe_replica_transfer_delivery(
                &intent,
                target_node_id,
                &request_id,
                stage,
                &StageBlobSendOutcome {
                    sent: outcome.sent,
                    last_error: outcome.last_error.clone(),
                    delivery_state: outcome.delivery_state,
                },
            )?;
        }
        Ok(outcome)
    }

    pub(crate) async fn send_best_effort_message_if_routable_outcome_async(
        &mut self,
        target_node_id: &NodeId,
        message: DfsNetworkMessage,
    ) -> Result<DfsBestEffortMessageSendOutcome, DfsError> {
        let transfer = replica_transfer_identity(&message);
        let background_repair = matches!(&message, DfsNetworkMessage::RepairReplica(command) if command.background_repair);
        let routed = self.next_dfs_routed_message(target_node_id, message);
        let call = crate::message_transport::routed_call_with_content(routed)?;
        if let Some((intent, request_id, stage)) = &transfer {
            self.begin_replica_transfer_attempt(
                intent,
                target_node_id,
                request_id,
                *stage,
                background_repair,
            )?;
        }
        let result = self
            .transport
            .send_routed_call_async(target_node_id, call)
            .await;
        let outcome = match result {
            Ok(()) => DfsBestEffortMessageSendOutcome {
                sent: true,
                last_error: None,
                delivery_state: Some(sinomo_core::network::NetworkDeliveryState::PossiblyApplied),
            },
            Err(error) => DfsBestEffortMessageSendOutcome {
                sent: false,
                delivery_state: error.delivery_state(),
                last_error: if matches!(&error, NetworkError::UnknownNode { .. }) {
                    None
                } else {
                    Some(error.to_string())
                },
            },
        };
        if let Some((intent, request_id, stage)) = transfer {
            self.observe_replica_transfer_delivery(
                &intent,
                target_node_id,
                &request_id,
                stage,
                &StageBlobSendOutcome {
                    sent: outcome.sent,
                    last_error: outcome.last_error.clone(),
                    delivery_state: outcome.delivery_state,
                },
            )?;
        }
        Ok(outcome)
    }
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    #[cfg(any(test, feature = "test-support"))]
    fn native_stage_send_context(
        &self,
        requests: &[DfsRoutedSend],
    ) -> Option<crate::native_shard_trace::NativeBoundaryContextLease> {
        let DfsNetworkMessage::StageBlob(command) = &requests.first()?.call.message.message else {
            return None;
        };
        let shard = command.authority_shard.as_ref()?;
        let (root, owner) = self.native_publication_owner_for_observation()?;
        Some(crate::native_shard_trace::enter_boundary_context(
            &root,
            &owner,
            &DfsMetadataShardId::new(shard.shard_id.clone()),
        ))
    }

    fn begin_replica_sends(
        &mut self,
        requests: &[DfsRoutedSend],
    ) -> Result<Vec<(NodeId, String, String, bool)>, DfsError> {
        let identities = requests
            .iter()
            .filter_map(|request| {
                replica_transfer_identity(&request.call.message.message).map(
                    |(intent, request_id, stage)| {
                        (request.target_node_id.clone(), intent, request_id, stage)
                    },
                )
            })
            .collect::<Vec<_>>();
        if identities.len() != requests.len() {
            return Err(DfsError::Metadata {
                message: "replica fanout omitted an actual transfer identity".into(),
            });
        }
        for ((target, intent, request_id, stage), request) in identities.iter().zip(requests) {
            self.begin_replica_transfer_attempt(intent, target, request_id, *stage,
                matches!(&request.call.message.message, DfsNetworkMessage::RepairReplica(command) if command.background_repair))?;
        }
        Ok(identities)
    }

    fn finish_replica_sends(
        &mut self,
        identities: &[(NodeId, String, String, bool)],
        expected_targets: &[NodeId],
        outcomes: Vec<DfsRoutedSendOutcome>,
    ) -> Result<Vec<StageBlobSendOutcome>, DfsError> {
        let outcomes = validate_replica_body_fanout_outcomes(expected_targets, outcomes)?;
        if identities.len() != outcomes.len() {
            return Err(DfsError::Metadata {
                message: "replica send results omitted actual attempt identities".into(),
            });
        }
        for ((target, intent, request_id, stage), outcome) in identities.iter().zip(&outcomes) {
            self.observe_replica_transfer_delivery(intent, target, request_id, *stage, outcome)?;
        }
        Ok(outcomes)
    }

    fn observe_replica_fanout_result(
        &mut self,
        identities: &[(NodeId, String, String, bool)],
        result: Result<Vec<DfsRoutedSendOutcome>, NetworkError>,
    ) -> Result<Vec<DfsRoutedSendOutcome>, DfsError> {
        match result {
            Ok(outcomes) => Ok(outcomes),
            Err(error) => {
                let delivery_state = error.delivery_state().filter(|state| {
                    *state == sinomo_core::network::NetworkDeliveryState::PossiblyApplied
                });
                let outcome = StageBlobSendOutcome {
                    sent: false,
                    last_error: Some(error.to_string()),
                    delivery_state,
                };
                for (target, intent, request_id, stage) in identities {
                    self.observe_replica_transfer_delivery(
                        intent, target, request_id, *stage, &outcome,
                    )?;
                }
                Err(DfsError::from_network(error))
            }
        }
    }

    /// Package one repair transfer per target, planned against each slot's
    /// current receipt generation. `background` hands it to a detached
    /// worker after maintenance releases its admission.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn prepare_repair_replica_sends(
        &mut self,
        targets: &[(NodeId, String)],
        intent_id: &str,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        expected_base_version: Option<DfsVersion>,
        version: &DfsVersion,
        content_hash: &str,
        total_size_bytes: u64,
        object: DfsObjectMetadata,
        background: bool,
    ) -> Result<PreparedReplicaSends, DfsError> {
        self.prepare_replica_attachments(
            targets,
            ReplicaTransfer {
                intent_id,
                file_system_id,
                object_id,
                expected_base_version,
                version,
                content_hash,
                total_size_bytes,
                object_metadata: Some(object),
                kind: if background {
                    ReplicaTransferKind::BackgroundRepair
                } else {
                    ReplicaTransferKind::Repair
                },
            },
        )
    }

    fn prepare_replica_attachments(
        &mut self,
        targets: &[(NodeId, String)],
        transfer: ReplicaTransfer<'_>,
    ) -> Result<PreparedReplicaSends, DfsError> {
        let ReplicaTransfer {
            intent_id,
            file_system_id,
            object_id,
            expected_base_version,
            version,
            content_hash,
            total_size_bytes,
            object_metadata,
            kind,
        } = transfer;
        if targets.is_empty() {
            return Ok(PreparedReplicaSends {
                requests: Vec::new(),
                expected_targets: Vec::new(),
            });
        }
        let authority_shard = self.command_authority_shard_for_object(object_metadata.as_ref())?;
        let retained_transfer = matches!(
            kind,
            ReplicaTransferKind::Repair | ReplicaTransferKind::BackgroundRepair
        ) && self
            .retained_content_transfer_required(intent_id, object_id, version)?;
        let mut requests = Vec::with_capacity(targets.len());
        for (target_node_id, request_id) in targets {
            // Every new transfer plans against the target slot's current
            // generation before it is sent; its reply may publish only there.
            let receipt_generation =
                self.backend_receipt_slot_generation(object_id, version, target_node_id)?;
            // Every repair attempt, detached or not, is an ordinary attempt of
            // the node's repair executor. No original-repair custody is born or
            // continued here: the slot's receipt generation alone fences a late
            // reply, and a re-send at the same generation is idempotent.
            let (_, reader) = self.open_content_hash_stream(
                object_id,
                version,
                content_hash,
                Some(total_size_bytes),
            )?;
            let message = match kind {
                ReplicaTransferKind::Stage => {
                    let command = DfsStageBlobCommand {
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
                        receipt_generation,
                    };
                    if self
                        .coordinator
                        .intent(intent_id)
                        .is_some_and(|intent| !intent.publication.is_current())
                    {
                        DfsNetworkMessage::StageRetainedBlob(command)
                    } else {
                        DfsNetworkMessage::StageBlob(command)
                    }
                }
                ReplicaTransferKind::Repair | ReplicaTransferKind::BackgroundRepair => {
                    if retained_transfer {
                        DfsNetworkMessage::RepairRetainedBlob(DfsStageBlobCommand {
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
                            receipt_generation,
                        })
                    } else {
                        DfsNetworkMessage::RepairReplica(DfsRepairReplicaCommand {
                            background_repair: matches!(
                                kind,
                                ReplicaTransferKind::BackgroundRepair
                            ),
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
                            receipt_generation,
                        })
                    }
                }
            };
            let routed_message = self.next_dfs_routed_message(target_node_id, message);
            requests.push(DfsRoutedSend::new(
                target_node_id.clone(),
                DfsRoutedCall::with_attachment(
                    routed_message,
                    DfsOutboundAttachment::from_reader(reader, total_size_bytes, content_hash),
                ),
            ));
        }
        let expected_targets = targets
            .iter()
            .map(|(target, _)| target.clone())
            .collect::<Vec<_>>();
        Ok(PreparedReplicaSends {
            requests,
            expected_targets,
        })
    }

    pub(crate) fn send_dfs_message_if_routable(
        &mut self,
        target_node_id: &NodeId,
        message: DfsNetworkMessage,
    ) -> Result<bool, DfsError> {
        let routed_message = self.next_dfs_routed_message(target_node_id, message);
        self.send_routed_dfs_message_if_routable(routed_message)
    }

    /// Send an already framed message to its target over the ordinary routed path.
    pub(crate) fn send_routed_dfs_message_if_routable(
        &mut self,
        routed_message: DfsRoutedMessage,
    ) -> Result<bool, DfsError> {
        self.check_mounted_call_running()?;
        let target_node_id = routed_message.target_node_id.clone();
        match self.transport.send_routed_call(
            &target_node_id,
            crate::message_transport::routed_call_with_content(routed_message)?,
        ) {
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

    pub(crate) fn send_dfs_message_if_routable_detached(
        &mut self,
        target_node_id: &NodeId,
        message: DfsNetworkMessage,
    ) -> Result<bool, DfsError> {
        if matches!(&message, DfsNetworkMessage::MetadataSnapshotInstall(_)) {
            return Err(DfsError::Distributed {
                message: "snapshot images require the ordinary streamed call owner".into(),
            });
        }
        self.check_mounted_call_running()?;
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
    pub(crate) fn send_stage_patch_attachment_if_routable(
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
        let receipt_generation =
            self.backend_receipt_slot_generation(object_id, version, target_node_id)?;
        let reader = self.open_patch_blob_stream(patch_hash, patch_size_bytes)?;
        let send_outcome = self.send_best_effort_message_attachment_if_routable(
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
                receipt_generation,
            }),
            DfsOutboundAttachment::from_reader(reader, patch_size_bytes, patch_hash),
        )?;
        Ok(StageBlobSendOutcome {
            delivery_state: send_outcome.delivery_state,
            sent: send_outcome.sent,
            last_error: send_outcome.last_error,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn send_stage_blob_attachments_if_routable(
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
        let PreparedReplicaSends {
            requests,
            expected_targets,
        } = self.prepare_replica_attachments(
            targets,
            ReplicaTransfer {
                intent_id,
                file_system_id,
                object_id,
                expected_base_version,
                version,
                content_hash,
                total_size_bytes,
                object_metadata,
                kind: ReplicaTransferKind::Stage,
            },
        )?;
        let identities = self.begin_replica_sends(&requests)?;
        #[cfg(any(test, feature = "test-support"))]
        let _native_context = self.native_stage_send_context(&requests);
        let outcomes = self
            .transport
            .send_routed_calls_bounded(requests, DFS_REPLICA_BODY_FANOUT_CAP);
        let outcomes = self.observe_replica_fanout_result(&identities, outcomes)?;
        self.finish_replica_sends(&identities, &expected_targets, outcomes)
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

    pub(crate) fn send_best_effort_message_attachment_if_routable(
        &mut self,
        target_node_id: &NodeId,
        message: DfsNetworkMessage,
        attachment: DfsOutboundAttachment,
    ) -> Result<DfsBestEffortMessageSendOutcome, DfsError> {
        let transfer = replica_transfer_identity(&message);
        let background_repair = matches!(&message, DfsNetworkMessage::RepairReplica(command) if command.background_repair);
        if let Some((intent, request_id, stage)) = &transfer {
            self.begin_replica_transfer_attempt(
                intent,
                target_node_id,
                request_id,
                *stage,
                background_repair,
            )?;
        }
        let routed_message = self.next_dfs_routed_message(target_node_id, message);
        let outcome = match self.transport.send_routed_call(
            target_node_id,
            DfsRoutedCall::with_attachment(routed_message, attachment),
        ) {
            Ok(()) => DfsBestEffortMessageSendOutcome {
                delivery_state: Some(sinomo_core::network::NetworkDeliveryState::PossiblyApplied),
                sent: true,
                last_error: None,
            },
            Err(NetworkError::UnknownNode { .. }) => DfsBestEffortMessageSendOutcome {
                delivery_state: None,
                sent: false,
                last_error: None,
            },
            Err(error) => DfsBestEffortMessageSendOutcome {
                delivery_state: error.delivery_state(),
                sent: false,
                last_error: Some(error.to_string()),
            },
        };
        if let Some((intent, request_id, stage)) = transfer {
            self.observe_replica_transfer_delivery(
                &intent,
                target_node_id,
                &request_id,
                stage,
                &StageBlobSendOutcome {
                    sent: outcome.sent,
                    last_error: outcome.last_error.clone(),
                    delivery_state: outcome.delivery_state,
                },
            )?;
        }
        Ok(outcome)
    }

    pub(crate) fn send_best_effort_message_if_routable_outcome(
        &mut self,
        target_node_id: &NodeId,
        message: DfsNetworkMessage,
    ) -> Result<DfsBestEffortMessageSendOutcome, DfsError> {
        let transfer = replica_transfer_identity(&message);
        let background_repair = matches!(&message, DfsNetworkMessage::RepairReplica(command) if command.background_repair);
        let routed = self.next_dfs_routed_message(target_node_id, message);
        let call = crate::message_transport::routed_call_with_content(routed)?;
        if let Some((intent, request_id, stage)) = &transfer {
            self.begin_replica_transfer_attempt(
                intent,
                target_node_id,
                request_id,
                *stage,
                background_repair,
            )?;
        }
        let result = self.transport.send_routed_call(target_node_id, call);
        let outcome = match result {
            Ok(()) => DfsBestEffortMessageSendOutcome {
                sent: true,
                last_error: None,
                delivery_state: Some(sinomo_core::network::NetworkDeliveryState::PossiblyApplied),
            },
            Err(error) => DfsBestEffortMessageSendOutcome {
                sent: false,
                delivery_state: error.delivery_state(),
                last_error: if matches!(&error, NetworkError::UnknownNode { .. }) {
                    None
                } else {
                    Some(error.to_string())
                },
            },
        };
        if let Some((intent, request_id, stage)) = transfer {
            self.observe_replica_transfer_delivery(
                &intent,
                target_node_id,
                &request_id,
                stage,
                &StageBlobSendOutcome {
                    sent: outcome.sent,
                    last_error: outcome.last_error.clone(),
                    delivery_state: outcome.delivery_state,
                },
            )?;
        }
        Ok(outcome)
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
