//! Write ownership and timeout service operations.
//!
//! This module owns user-facing control operations that adjust write
//! lifecycle state outside the normal stage/commit data path: confirmed
//! write timeout handling, aborts, and object metadata selection for writes.

use crate::*;

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub fn handle_confirmed_write_timeout(
        &mut self,
        intent_id: &str,
        action: DfsConfirmedTimeoutAction,
    ) -> Result<DfsConfirmedTimeoutOutcome, DfsError> {
        match action {
            DfsConfirmedTimeoutAction::KeepPending => match self.write_outcome(intent_id)? {
                NetworkedDfsWriteOutcome::Pending {
                    intent_id,
                    durable_receipts,
                    required_receipts,
                } => Ok(DfsConfirmedTimeoutOutcome::StillPending {
                    intent_id,
                    durable_receipts,
                    required_receipts,
                }),
                outcome @ NetworkedDfsWriteOutcome::Committed { .. } => {
                    Ok(DfsConfirmedTimeoutOutcome::Committed(outcome))
                }
            },
            DfsConfirmedTimeoutAction::Abort => {
                self.abort_pending_write(intent_id)?;
                Ok(DfsConfirmedTimeoutOutcome::Aborted {
                    intent_id: intent_id.to_string(),
                })
            }
            DfsConfirmedTimeoutAction::DowngradeToEventual {
                min_commit_replicas,
            } => {
                let intent = self.coordinator.intent(intent_id).cloned().ok_or_else(|| {
                    DfsError::Distributed {
                        message: format!("unknown intent: {intent_id}"),
                    }
                })?;
                let policy = intent
                    .policy
                    .with_counts(min_commit_replicas, intent.policy.desired_replicas)?;
                let updated_intent = self
                    .coordinator
                    .downgrade_intent_policy(intent_id, policy)?;
                let content_hash = self
                    .writes
                    .pending_writes
                    .get(intent_id)
                    .map(|pending| pending.content_hash.clone());
                self.put_metadata_intent(&updated_intent, content_hash.as_deref())?;
                self.try_apply_committed_intent_if_ready(intent_id)?;
                Ok(DfsConfirmedTimeoutOutcome::Downgraded(
                    self.write_outcome(intent_id)?,
                ))
            }
        }
    }

    pub(crate) fn current_object_for_write(
        &mut self,
        object_id: &DfsObjectId,
    ) -> Result<DfsObjectMetadata, DfsError> {
        let memory_object = self.metadata.object(object_id).cloned();
        if let Some(memory_object) = memory_object.as_ref() {
            // Writes must not race an in-flight cross-shard move of the
            // object's path; the move would fork the object between the
            // source and destination shards.
            self.ensure_object_not_in_cross_shard_flight(memory_object)?;
        }
        if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(backend_object) = backend.get_object(object_id)?
        {
            self.ensure_object_not_in_cross_shard_flight(&backend_object)?;
            if let Some(memory_object) = memory_object.as_ref()
                && self.local_write_view_overrides_backend(
                    object_id,
                    memory_object,
                    Some(&backend_object),
                )
            {
                self.coordinator.set_committed_version(
                    memory_object.file_system_id.clone(),
                    memory_object.object_id.clone(),
                    memory_object.version.clone(),
                );
                return Ok(memory_object.clone());
            }
            self.coordinator.set_committed_version(
                backend_object.file_system_id.clone(),
                backend_object.object_id.clone(),
                backend_object.version.clone(),
            );
            self.metadata.add_object(backend_object.clone())?;
            return Ok(backend_object);
        }
        if let Some(memory_object) = memory_object {
            return Ok(memory_object);
        }
        self.metadata
            .object(object_id)
            .cloned()
            .ok_or_else(|| DfsError::UnknownObject {
                object_id: object_id.clone(),
            })
    }

    pub fn abort_pending_write(
        &mut self,
        intent_id: &str,
    ) -> Result<DistributedWriteIntent, DfsError> {
        if self.transport.delegates_local_namespace_authority()
            || self.writes.pending_routed_writes.contains_key(intent_id)
        {
            return self.route_pending_write_abort(intent_id);
        }
        self.abort_pending_write_locally(intent_id)
    }

    pub(crate) fn abort_pending_write_locally(
        &mut self,
        intent_id: &str,
    ) -> Result<DistributedWriteIntent, DfsError> {
        let original =
            self.coordinator
                .intent(intent_id)
                .cloned()
                .ok_or_else(|| DfsError::Distributed {
                    message: format!("unknown pending write intent: {intent_id}"),
                })?;
        if original.state != DfsWriteIntentState::AwaitingReceipts {
            return Err(DfsError::Distributed {
                message: "closed write intent cannot be aborted".into(),
            });
        }
        let content_hash = self
            .writes
            .pending_writes
            .get(intent_id)
            .map(|pending| pending.content_hash.clone());
        let mut candidate = original.clone();
        candidate.state = DfsWriteIntentState::Aborted;
        // This is desired DATA only. The coordinator remains pending until the
        // existing kernel durably publishes the same terminal intent/resolution.
        self.put_metadata_intent(&candidate, content_hash.as_deref())?;
        let record = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message: "write abort requires an actual durable backend".into(),
            })?
            .get_intent(&original.object_id, intent_id)?
            .ok_or_else(|| DfsError::Metadata {
                message: "write abort lost its actual durable terminal intent".into(),
            })?;
        let terminal = crate::intent::distributed_intent_from_record(&record);
        if terminal != candidate || record.status != DfsWriteIntentStatus::Aborted {
            return Err(DfsError::Metadata {
                message: "write abort did not retain its exact terminal body".into(),
            });
        }
        self.retire_aborted_write_execution(&terminal)?;
        Ok(terminal)
    }

    fn retire_aborted_write_execution(
        &mut self,
        terminal: &DistributedWriteIntent,
    ) -> Result<(), DfsError> {
        let retry_records = self
            .repair_attempt_records
            .keys()
            .any(|(id, _)| id == &terminal.intent_id)
            || self
                .writes
                .pending_writes
                .get(&terminal.intent_id)
                .is_some_and(|pending| !pending.repair_retries.is_empty());
        if let Some(current) = self.coordinator.intent(&terminal.intent_id).cloned() {
            let mut expected = current.clone();
            expected.state = DfsWriteIntentState::Aborted;
            if &expected != terminal {
                return Err(DfsError::Metadata {
                    message: "aborted terminal body differs from live execution".into(),
                });
            }
            if current.state == DfsWriteIntentState::AwaitingReceipts {
                self.coordinator.abort_intent(&terminal.intent_id)?;
            } else if current.state != DfsWriteIntentState::Aborted {
                return Err(DfsError::Metadata {
                    message: "closed live execution differs from durable abort".into(),
                });
            }
        }
        self.remove_pending_write_state(&terminal.intent_id);
        self.repair_attempt_records
            .retain(|(id, _), _| id != &terminal.intent_id);
        if retry_records {
            // Actual tracked retries need the SAME normal queue reconciliation.
            self.record_message_committed_work(DfsMessageCommittedWork {
                repair_maintenance: true,
                ..Default::default()
            });
        }
        Ok(())
    }

    fn route_pending_write_abort(
        &mut self,
        intent_id: &str,
    ) -> Result<DistributedWriteIntent, DfsError> {
        // Only the original retained request supplies this locator. An absent or
        // expired request is uncertainty, never permission to guess a write owner.
        let pending = self
            .writes
            .pending_routed_writes
            .get(intent_id)
            .and_then(|tracked| {
                // A real remote intent is an alias whose body intentionally
                // lives only on the original canonical request.
                self.writes
                    .pending_routed_writes
                    .get(&tracked.canonical_request_id)
            })
            .cloned()
            .or_else(|| {
                self.writes
                    .pending_routed_writes
                    .values()
                    .find(|pending| {
                        pending.command.as_ref().is_some_and(|command| {
                            crate::intent::stable_write_intent_id(
                                &command.request.requesting_node_id,
                                &command.request.object_id,
                                &command.request_id,
                            ) == intent_id
                        })
                    })
                    .cloned()
            })
            .ok_or_else(|| DfsError::Distributed {
                message: format!("pending write {intent_id} has no retained original request"),
            })?;
        let command = pending
            .command
            .as_ref()
            .ok_or_else(|| DfsError::Distributed {
                message: "completed or expired routed writes cannot authorize an abort".into(),
            })?;
        if command.request.requesting_node_id != self.node_id {
            return Err(DfsError::Distributed {
                message: "write abort requires its original authenticated requester".into(),
            });
        }
        let object = match self.current_read_object_metadata(&command.request.object_id) {
            Ok(object) => object,
            Err(DfsError::UnknownObject { .. }) => {
                command
                    .object_metadata
                    .clone()
                    .ok_or_else(|| DfsError::Distributed {
                        message: "pending create abort has no original object locator".into(),
                    })?
            }
            Err(error) => return Err(error),
        };
        let shard = self
            .select_metadata_shard_for_object(&object)?
            .ok_or_else(|| crate::service_replica::missing_metadata_shard_assignment(&object))?;
        let actual_intent_id = crate::intent::stable_write_intent_id(
            &command.request.requesting_node_id,
            &command.request.object_id,
            &command.request_id,
        );
        if pending
            .remote_intent_id
            .as_ref()
            .is_some_and(|actual| actual != &actual_intent_id)
        {
            return Err(DfsError::Distributed {
                message: "pending write abort changed its original stable intent binding".into(),
            });
        }
        let response = self
            .route_namespace_operation_to_metadata_primary_with_request_id_and_options(
                format!(
                    "abort:{}",
                    blake3::hash(actual_intent_id.as_bytes()).to_hex()
                ),
                command.request.file_system_id.clone(),
                shard.primary_node_id,
                DfsNamespaceOperation::AbortPendingWrite {
                    path: object.path,
                    object_id: command.request.object_id.clone(),
                    target_version: command.request.target_version.clone(),
                    intent_id: actual_intent_id.clone(),
                    write_operation_id: command.request_id.clone(),
                },
                crate::DFS_ROUTED_NAMESPACE_OP_TIMEOUT,
                false,
                false,
            )?;
        let intent = match (response.result, response.posix_coordination_result) {
            (
                DfsNamespaceResult::Applied,
                Some(DfsPosixCoordinationResult::AbortedWriteIntent(intent)),
            ) => *intent,
            (DfsNamespaceResult::Rejected { reason, .. }, _) => {
                return Err(DfsError::Distributed { message: reason });
            }
            _ => {
                return Err(DfsError::Distributed {
                    message: "write abort lacks its actual typed durable intent outcome".into(),
                });
            }
        };
        if intent.state != DfsWriteIntentState::Aborted
            || intent.intent_id != actual_intent_id
            || intent.file_system_id != command.request.file_system_id
            || intent.object_id != command.request.object_id
            || intent.target_version != command.request.target_version
            || intent.requesting_node_id != self.node_id
            || intent.operation_id.as_deref() != Some(command.request_id.as_str())
        {
            return Err(DfsError::Distributed {
                message: "write abort response changed its exact original intent binding".into(),
            });
        }
        self.abandon_pending_routed_write(&pending.canonical_request_id);
        Ok(intent)
    }

    pub(crate) fn abort_routed_pending_write(
        &mut self,
        requester: &sinomo_core::node::NodeId,
        file_system_id: &DfsFileSystemId,
        operation: &DfsNamespaceOperation,
    ) -> Result<DistributedWriteIntent, DfsError> {
        let DfsNamespaceOperation::AbortPendingWrite {
            path,
            object_id,
            target_version,
            intent_id,
            write_operation_id,
        } = operation
        else {
            return Err(DfsError::Distributed {
                message: "invalid write abort operation".into(),
            });
        };
        crate::service_routed_write::validate_write_operation_id(write_operation_id)?;
        let record = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message: "write abort requires actual durable metadata".into(),
            })?
            .get_intent(object_id, intent_id)?
            .ok_or_else(|| DfsError::Distributed {
                message: "write abort has no exact durable intent".into(),
            })?;
        if record.file_system_id != *file_system_id
            || record.object_id != *object_id
            || record.target_version != *target_version
            || record.intent_id != *intent_id
            || record.coordinator_node_id != *requester
            || record.operation_id.as_deref() != Some(write_operation_id.as_str())
            || crate::intent::stable_write_intent_id(requester, object_id, write_operation_id)
                != *intent_id
        {
            return Err(DfsError::Distributed {
                message: "write abort changed its original durable actor or intent binding".into(),
            });
        }
        record
            .publication
            .validate()
            .map_err(|message| DfsError::Metadata { message })?;
        record.policy.validate()?;
        let actual = match self.current_read_object_metadata(object_id) {
            Ok(object) => object,
            Err(DfsError::UnknownObject { .. }) => {
                record
                    .object_metadata
                    .clone()
                    .ok_or_else(|| DfsError::Distributed {
                        message: "pending create abort has no durable object locator".into(),
                    })?
            }
            Err(error) => return Err(error),
        };
        if actual.object_id != *object_id
            || actual.file_system_id != *file_system_id
            || actual.path != *path
        {
            return Err(DfsError::Distributed {
                message: "write abort path does not name its actual durable object".into(),
            });
        }
        let shard = self
            .select_metadata_shard_for_object(&actual)?
            .ok_or_else(|| crate::service_replica::missing_metadata_shard_assignment(&actual))?;
        if shard.primary_node_id != self.node_id {
            return Err(DfsError::StaleOwnerEpoch {
                file_system_id: shard.file_system_id,
                local_node_id: self.node_id.clone(),
                primary_node_id: shard.primary_node_id,
                epoch: shard.epoch,
            });
        }
        self.ensure_object_not_in_cross_shard_flight(&actual)?;
        let intent = crate::intent::distributed_intent_from_record(&record);
        if record.status == DfsWriteIntentStatus::Aborted {
            // Replay the same actual durable outcome; never author another body.
            self.retire_aborted_write_execution(&intent)?;
            return Ok(intent);
        }
        if !crate::intent::write_intent_status_is_active(&record.status) {
            return Err(DfsError::Distributed {
                message: "closed write intent cannot be aborted".into(),
            });
        }
        if record.publication.is_current()
            && record
                .base_version
                .as_ref()
                .is_some_and(|base| base != &actual.version)
        {
            return Err(DfsError::Distributed {
                message: "write abort observed a changed current head".into(),
            });
        }
        // Reuse normal per-intent hydration on the actual admitted owner.
        // It restores only real durable bodies, receipts and content sources.
        self.restore_pending_write_for_intent_if_missing(intent_id, object_id)?;
        if self.coordinator.intent(intent_id) != Some(&intent) {
            return Err(DfsError::Distributed {
                message: "live write intent differs from its actual durable body".into(),
            });
        }
        self.abort_pending_write_locally(intent_id)?;
        let aborted = self
            .metadata_backend
            .as_ref()
            .expect("checked actual backend")
            .get_intent(object_id, intent_id)?
            .ok_or_else(|| DfsError::Metadata {
                message: "write abort lost its actual durable terminal intent".into(),
            })?;
        let expected = crate::intent::distributed_intent_from_record(&record);
        let terminal = crate::intent::distributed_intent_from_record(&aborted);
        let mut unchanged = terminal.clone();
        unchanged.state = expected.state.clone();
        if aborted.status != DfsWriteIntentStatus::Aborted || unchanged != expected {
            return Err(DfsError::Metadata {
                message: "write abort changed its exact durable intent body".into(),
            });
        }
        Ok(terminal)
    }
}
