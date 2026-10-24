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
    pub(crate) fn is_current_background_repair(&self) -> bool {
        self.requests.iter().all(|request| {
            matches!(
                &request.call.message.message,
                DfsNetworkMessage::RepairReplica(command) if command.background_repair
            )
        })
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

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn send_repair_replica_attachments_if_routable_async(
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
        let background_dispatch = self.transport.supports_background_replica_dispatch()
            && !self
                .writes
                .pending_remote_write_responses
                .contains_key(intent_id);
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
                kind: if background_dispatch {
                    ReplicaTransferKind::BackgroundRepair
                } else {
                    ReplicaTransferKind::Repair
                },
            },
        )?;
        self.dispatch_prepared_repair_replica_sends(
            PreparedReplicaSends {
                requests,
                expected_targets,
            },
            background_dispatch,
        )
        .await
    }

    pub(crate) async fn dispatch_prepared_repair_replica_sends(
        &mut self,
        prepared: PreparedReplicaSends,
        background_dispatch: bool,
    ) -> Result<Vec<StageBlobSendOutcome>, DfsError> {
        if background_dispatch && prepared.is_current_background_repair() {
            return self.queue_current_background_repair_sends(prepared);
        }
        let PreparedReplicaSends {
            requests,
            expected_targets,
        } = prepared;
        let identities = self.begin_replica_sends(&requests)?;
        let outcomes = self
            .transport
            .send_routed_calls_bounded_async(requests, DFS_REPLICA_BODY_FANOUT_CAP)
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
        let call = crate::message_transport::native_snapshot_routed_call(routed)?;
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

    /// The grouped repair owner must reach its durable queue checkpoint without
    /// an asynchronous retained-transfer fallback after retiring other work.
    /// Validate the immutable packet kind and hand off only to the existing
    /// synchronous prepared-payload owner. This is not an authority decision.
    pub(crate) fn queue_current_background_repair_sends(
        &mut self,
        prepared: PreparedReplicaSends,
    ) -> Result<Vec<StageBlobSendOutcome>, DfsError> {
        if !prepared.is_current_background_repair() {
            return Err(DfsError::Distributed {
                message: "grouped repair payload changed to retained or non-background transfer before checkpoint".into(),
            });
        }
        let PreparedReplicaSends {
            requests,
            expected_targets,
        } = prepared;
        let identities = self.begin_replica_sends(&requests)?;
        // Bind original producer evidence while this same admitted service owns
        // the actual claims, before either queue checkpoint or payload handoff.
        // A later service generation cannot reinterpret these owned readers.
        let producer_delivery = requests
            .iter()
            .map(|request| {
                let DfsNetworkMessage::RepairReplica(command) = &request.call.message.message
                else {
                    return None;
                };
                if command.background_repair
                    && request.target_node_id == request.call.message.target_node_id
                    && request.call.message.source_node_id == self.node_id
                    && self
                        .coordinator
                        .intent(&command.intent_id)
                        .is_some_and(|intent| {
                            intent.file_system_id == command.file_system_id
                                && intent.object_id == command.object_id
                                && intent.target_version == command.version
                                && intent.base_version == command.expected_base_version
                        })
                    && let Some(pending) = self.writes.pending_writes.get(&command.intent_id)
                    && pending.object_id == command.object_id
                    && pending.version == command.version
                    && pending.content_hash == command.content_hash
                    && pending
                        .repair_inflight_request_ids
                        .get(&request.target_node_id)
                        == Some(&command.request_id)
                    && !pending
                        .stage_inflight_request_ids
                        .contains_key(&request.target_node_id)
                    && let Some(state) = pending.repair_retries.get(&request.target_node_id)
                    && !state.unresolved_custody_without_request
                    && state.completed_transfer_request_id.is_none()
                {
                    Some((
                        state.retrying_unresolved_transfer,
                        state.previous_transfer_delivery_state,
                    ))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        if let Err(error) = self
            .transport
            .prepare_background_replica_dispatch(requests, &producer_delivery)
        {
            self.observe_replica_fanout_result(&identities, Err(error))?;
            unreachable!("failed fanout result returns its error");
        }
        let outcomes = expected_targets
            .iter()
            .map(|_| StageBlobSendOutcome {
                sent: true,
                last_error: None,
                delivery_state: Some(sinomo_core::network::NetworkDeliveryState::PossiblyApplied),
            })
            .collect::<Vec<_>>();
        for ((target, intent, request_id, stage), outcome) in identities.iter().zip(&outcomes) {
            self.observe_replica_transfer_delivery(intent, target, request_id, *stage, outcome)?;
        }
        Ok(outcomes)
    }

    pub(crate) fn prepare_current_background_repair_replica_sends(
        &mut self,
        targets: &[(NodeId, String)],
        intent: &DistributedWriteIntent,
        pending: &crate::transfer::PendingNetworkedWrite,
        object: DfsObjectMetadata,
    ) -> Result<PreparedReplicaSends, DfsError> {
        self.prepare_replica_attachments(
            targets,
            ReplicaTransfer {
                intent_id: &intent.intent_id,
                file_system_id: &intent.file_system_id,
                object_id: &pending.object_id,
                expected_base_version: intent.base_version.clone(),
                version: &pending.version,
                content_hash: &pending.content_hash,
                total_size_bytes: pending.size_bytes,
                object_metadata: Some(object),
                kind: ReplicaTransferKind::BackgroundRepair,
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
            if matches!(
                kind,
                ReplicaTransferKind::Repair | ReplicaTransferKind::BackgroundRepair
            ) && (!matches!(kind, ReplicaTransferKind::BackgroundRepair) || retained_transfer)
                && let Some(store) = self.owned_repair_queue_store()?
                && store
                    .original_repair_for_slot(intent_id, target_node_id)?
                    .is_some()
            {
                return Err(DfsError::Distributed {
                    message: "alternate Repair dispatch cannot bypass an unresolved original"
                        .into(),
                });
            }
            // Only current native background Repair enters this custody slice.
            // Stage, retained repair and non-native transports keep their owners.
            if matches!(kind, ReplicaTransferKind::BackgroundRepair)
                && !retained_transfer
                && let Some(store) = self.owned_repair_queue_store()?
            {
                let pending_error = || DfsError::Distributed {
                    message: format!(
                        "original Repair custody pending for intent {intent_id}, target {target_node_id}; identity/body cannot be reconstructed or replaced"
                    ),
                };
                let incarnation = store.custody_incarnation()?;
                let existing = store.original_repair_for_slot(intent_id, target_node_id)?;
                let (routed_message, reader, descriptor) = if let Some(birth) = existing {
                    let DfsNetworkMessage::RepairReplica(original) = &birth.original.message else {
                        return Err(pending_error());
                    };
                    if original.request_id != *request_id
                        || !birth.observed.is_empty()
                        || birth.original.source_node_id != self.node_id
                        || original.file_system_id != *file_system_id
                        || original.object_id != *object_id
                        || original.version != *version
                        || original.content_hash != content_hash
                        || birth.attachment.total_size_bytes != total_size_bytes
                    {
                        return Err(pending_error());
                    }
                    // Reuse the complete immutable original command, including
                    // authority and base. Only carrier correlation is renewed.
                    let reader = self.content.open_retained_repair_payload(&birth.root)?;
                    let routed = self
                        .next_dfs_routed_message(target_node_id, birth.original.message.clone());
                    (routed, reader, birth.attachment)
                } else {
                    if store.legacy_repair_is_uncovered(intent_id, target_node_id)?
                        || self
                            .writes
                            .pending_writes
                            .get(intent_id)
                            .and_then(|pending| pending.repair_retries.get(target_node_id))
                            .is_some_and(|retry| {
                                retry.unresolved_custody_without_request
                                    || retry.retrying_unresolved_transfer
                                    || retry.inflight
                            })
                    {
                        return Err(pending_error());
                    }
                    if let Some(root) = self.content.retained_repair_for_slot(
                        &self.node_id.to_string(),
                        &target_node_id.to_string(),
                        intent_id,
                    )? {
                        let Some(closed) = store.closed_repair_birth(&root.subject)? else {
                            return Err(pending_error());
                        };
                        self.content.release_repair_payload(&closed)?;
                    }
                    let message = DfsNetworkMessage::RepairReplica(DfsRepairReplicaCommand {
                        background_repair: true,
                        request_id: request_id.clone(),
                        intent_id: intent_id.to_owned(),
                        file_system_id: file_system_id.clone(),
                        object_id: object_id.clone(),
                        expected_base_version: expected_base_version.clone(),
                        version: version.clone(),
                        content_hash: content_hash.to_owned(),
                        bytes: Vec::new(),
                        object_metadata: object_metadata.clone(),
                        authority_shard: authority_shard.clone(),
                    });
                    let routed = self.next_dfs_routed_message(target_node_id, message);
                    let descriptor = DfsRpcAttachmentDescriptor {
                        total_size_bytes,
                        content_hash: Some(content_hash.to_owned()),
                    };
                    let subject = crate::repair_queue_store::repair_subject(&incarnation, &routed)?;
                    let digest =
                        crate::repair_queue_store::original_repair_digest(&routed, &descriptor)?;
                    let retained = self.content.capture_repair_payload(
                        subject,
                        digest,
                        object_id.as_str(),
                        version.as_str(),
                        content_hash,
                        total_size_bytes,
                    )?;
                    store.enroll_original_repair(&routed, &descriptor, &retained)?;
                    let (_, reader) = retained.into_parts();
                    (routed, reader, descriptor)
                };
                // This durable unknown attempt exists before either synchronous
                // or detached transport can observe the reader/head.
                store.record_original_repair_attempt(&routed_message)?;
                requests.push(DfsRoutedSend::new(
                    target_node_id.clone(),
                    DfsRoutedCall::with_attachment(
                        routed_message,
                        DfsOutboundAttachment::from_reader(
                            reader,
                            descriptor.total_size_bytes,
                            descriptor
                                .content_hash
                                .as_deref()
                                .expect("enrolled Repair hash"),
                        ),
                    ),
                ));
                continue;
            }
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
        match self.transport.send_routed_call(
            target_node_id,
            crate::message_transport::native_snapshot_routed_call(routed_message)?,
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

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn send_repair_replica_attachments_if_routable(
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
                kind: ReplicaTransferKind::Repair,
            },
        )?;
        let identities = self.begin_replica_sends(&requests)?;
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
        let call = crate::message_transport::native_snapshot_routed_call(routed)?;
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
