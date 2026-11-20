//! Dispatch of one admitted need. The command is built from the need and the
//! shard tables alone: the owner intent's record, the version's record, the
//! current object, its authority shard and the slot's current receipt
//! generation. No lane state takes part; a reply publishes on any lane from
//! the reply and the tables through the receipt-generation compare.

use super::*;
use crate::{DfsFetchBlobCommand, DfsNetworkMessage, DfsWriteIntentRecord};

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(super) fn dispatch_repair_need(
        &mut self,
        executor: &DfsRepairExecutor,
        candidate: &DfsRepairExecutorCandidate,
    ) -> Result<DfsRepairDispatchResult, DfsError> {
        let superseded = Ok(DfsRepairDispatchResult::Outcome(
            DfsRepairAttemptOutcome::Superseded,
        ));
        let (object_id, version, target) = &candidate.key;
        let need = &candidate.need;
        let Some(intent_id) = need.owner_intent_id.as_deref() else {
            return superseded;
        };
        let backend = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message: "repair dispatch requires the shard tables".into(),
            })?;
        // The index derived this need from these rows; one missing now means
        // a newer change is on its way to the executor.
        let Some(intent) = backend.get_intent(object_id, intent_id)? else {
            return superseded;
        };
        let Some(version_record) = backend.get_version(object_id, version)? else {
            return superseded;
        };
        let object = self.current_read_object_metadata(object_id)?;
        if !self.is_local_metadata_primary_for_object(&object)? {
            // The local authority view has not caught up with the published
            // assignment: no failure, the authority edge releases it.
            tracing::debug!(target: "sinomo.dfs.repair", object_id = %object_id,
                version = %version, target = %target,
                "repair dispatch waits for the local authority view");
            return Ok(DfsRepairDispatchResult::Outcome(
                DfsRepairAttemptOutcome::AwaitingAuthority,
            ));
        }
        let generation = self.backend_receipt_slot_generation(object_id, version, target)?;
        let size_bytes = version_record.size_bytes.unwrap_or_default();
        let local_content = self
            .local_object_version_manifest_size_if_matches(object_id, version, &need.content_hash)
            .is_some();
        if target == &self.node_id {
            if local_content {
                return self.publish_local_repair_receipt(&intent, candidate, generation);
            }
            return self.dispatch_repair_fetch(executor, &intent, candidate, &object, generation);
        }
        if !local_content {
            // This node fills its own copy first; the fetch's outcome lets the
            // next pass send from it.
            return self.dispatch_repair_fetch(executor, &intent, candidate, &object, generation);
        }
        let request_id = self.next_request_id();
        lock(&executor.attempts).insert(
            candidate.key.clone(),
            DfsRepairAttempt {
                request_id: request_id.clone(),
                generation,
                sent_unix_nanos: dfs_now_unix_nanos(),
            },
        );
        let sent = self.send_repair_replica(&intent, candidate, &object, size_bytes, &request_id);
        match sent {
            Ok(Ok(())) => Ok(DfsRepairDispatchResult::Sent { request_id }),
            Ok(Err(error)) => {
                lock(&executor.attempts).remove(&candidate.key);
                Ok(DfsRepairDispatchResult::SendFailed { error })
            }
            Err(error) => {
                lock(&executor.attempts).remove(&candidate.key);
                Err(error)
            }
        }
    }

    /// Send one repair replica, current or retained. A transport that owns
    /// detached sends takes the command after maintenance releases its
    /// admission; its send failure reaches the executor as this attempt's
    /// outcome. `Ok(Err(_))` is a send that failed at once.
    fn send_repair_replica(
        &mut self,
        intent: &DfsWriteIntentRecord,
        candidate: &DfsRepairExecutorCandidate,
        object: &crate::DfsObjectMetadata,
        size_bytes: u64,
        request_id: &str,
    ) -> Result<Result<(), String>, DfsError> {
        let (object_id, version, target) = &candidate.key;
        let background = self.transport.detaches_executor_sends();
        let prepared = self.prepare_repair_replica_sends(
            &[(target.clone(), request_id.to_owned())],
            &intent.intent_id,
            &candidate.need.file_system_id,
            object_id,
            intent.base_version.clone(),
            version,
            &candidate.need.content_hash,
            size_bytes,
            object.clone(),
            background,
        )?;
        let requests = prepared.into_requests();
        if background {
            self.transport
                .prepare_background_replica_dispatch(requests)
                .map_err(DfsError::from_network)?;
            return Ok(Ok(()));
        }
        let outcomes = self
            .transport
            .send_routed_calls_bounded(
                requests,
                crate::service_outbound::DFS_REPLICA_BODY_FANOUT_CAP,
            )
            .map_err(DfsError::from_network)?;
        Ok(
            match outcomes
                .into_iter()
                .find_map(|outcome| outcome.result.err())
            {
                Some(error) => Err(error.to_string()),
                None => Ok(()),
            },
        )
    }

    /// This node is the missing replica and already holds the content: its
    /// own receipt publishes at the slot's current generation.
    fn publish_local_repair_receipt(
        &mut self,
        intent: &DfsWriteIntentRecord,
        candidate: &DfsRepairExecutorCandidate,
        generation: u64,
    ) -> Result<DfsRepairDispatchResult, DfsError> {
        let (object_id, version, target) = &candidate.key;
        let content_hash = candidate.need.content_hash.clone();
        let file_system_id = candidate.need.file_system_id.clone();
        let published =
            self.with_table_intent_placement_authority(object_id, &intent.intent_id, version, target, &candidate.need.content_hash, |service| {
                if !service.ensure_local_repair_manifest(object_id, version, &content_hash)? {
                    return Ok(false);
                }
                match service.put_metadata_receipt(
                    &file_system_id,
                    object_id,
                    version,
                    target,
                    &content_hash,
                    generation,
                ) {
                    Ok(_) => Ok(true),
                    Err(DfsError::ReceiptGenerationSuperseded { .. }) => Ok(false),
                    Err(error) => Err(error),
                }
            })?;
        Ok(match published {
            Some(true) => DfsRepairDispatchResult::Outcome(DfsRepairAttemptOutcome::Settled),
            Some(false) => DfsRepairDispatchResult::Outcome(DfsRepairAttemptOutcome::Superseded),
            None => DfsRepairDispatchResult::Failed {
                error: "the owner intent's placement authority is no longer current".into(),
            },
        })
    }

    /// Fetch the content to this node from a replica that holds it. The
    /// executor serves the reply from the tables; it re-plans the need, whose
    /// next pass publishes this node's receipt or sends from this copy.
    fn dispatch_repair_fetch(
        &mut self,
        executor: &DfsRepairExecutor,
        intent: &DfsWriteIntentRecord,
        candidate: &DfsRepairExecutorCandidate,
        object: &crate::DfsObjectMetadata,
        generation: u64,
    ) -> Result<DfsRepairDispatchResult, DfsError> {
        let (object_id, version, target) = &candidate.key;
        let backend = self.metadata_backend.as_ref().expect("checked by dispatch");
        let mut sources = backend
            .receipts_for_version(object_id, version)?
            .into_iter()
            .map(|receipt| receipt.replica_node_id)
            .collect::<BTreeSet<_>>();
        sources.extend(object.current_locations.iter().cloned());
        sources.remove(&self.node_id);
        sources.remove(target);
        let authority_shard = self.command_authority_shard_for_object(Some(object))?;
        // Under maintenance the fetch leaves through the detached worker once
        // admission is released, and its reply returns through the worker's
        // admitted handler: the source serves a fetch under its own exclusive
        // admission, so waiting for it here could close a cycle with a
        // request the source is serving against this node.
        let detached = self.transport.detaches_executor_sends();
        let mut last_error = None;
        for source in self.ranked_transfer_sources(sources) {
            let request_id = self.next_request_id();
            lock(&executor.attempts).insert(
                candidate.key.clone(),
                DfsRepairAttempt {
                    request_id: request_id.clone(),
                    generation,
                    sent_unix_nanos: dfs_now_unix_nanos(),
                },
            );
            let message = DfsNetworkMessage::FetchBlob(DfsFetchBlobCommand {
                request_id: request_id.clone(),
                intent_id: intent.intent_id.clone(),
                object_id: object_id.clone(),
                version: version.clone(),
                content_hash: candidate.need.content_hash.clone(),
                authority_shard: authority_shard.clone(),
            });
            let outcome = if detached {
                self.send_best_effort_message_if_routable_detached(&source, message)
                    .map(|queued| {
                        (!queued).then(|| format!("fetch source {source} is not routable"))
                    })
            } else {
                self.send_best_effort_message_if_routable_outcome(&source, message)
                    .map(|outcome| {
                        (!outcome.sent).then(|| {
                            outcome
                                .last_error
                                .unwrap_or_else(|| format!("fetch source {source} is not routable"))
                        })
                    })
            };
            match outcome {
                Ok(None) => {
                    return Ok(DfsRepairDispatchResult::Sent { request_id });
                }
                Ok(Some(error)) => {
                    lock(&executor.attempts).remove(&candidate.key);
                    last_error = Some(error);
                }
                Err(error) => {
                    lock(&executor.attempts).remove(&candidate.key);
                    return Err(error);
                }
            }
        }
        Ok(match last_error {
            Some(error) => DfsRepairDispatchResult::SendFailed { error },
            None => DfsRepairDispatchResult::Failed {
                error: "no replica holds the content to fetch".into(),
            },
        })
    }
}
