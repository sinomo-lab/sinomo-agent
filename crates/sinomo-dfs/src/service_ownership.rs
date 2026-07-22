//! Write ownership, timeout, and owner-handoff service operations.
//!
//! This module owns user-facing control operations that adjust write
//! lifecycle state outside the normal stage/commit data path: confirmed
//! write timeout handling, aborts, object metadata selection for writes,
//! and metadata-shard owner handoff.

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
                NetworkedDfsWriteOutcome::Conflict { conflict } => {
                    Ok(DfsConfirmedTimeoutOutcome::Conflict { conflict })
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
        let content_hash = self
            .pending_writes
            .get(intent_id)
            .map(|pending| pending.content_hash.clone());
        let intent = self.coordinator.abort_intent(intent_id)?;
        self.put_metadata_intent(&intent, content_hash.as_deref())?;
        self.abort_create_namespace_prepare(&intent)?;
        self.remove_pending_write_state(intent_id);
        Ok(intent)
    }

    fn abort_create_namespace_prepare(
        &mut self,
        intent: &DistributedWriteIntent,
    ) -> Result<(), DfsError> {
        if !intent.touch_parent_on_create {
            return Ok(());
        }
        let operation_id = format!("namespace:create:{}", intent.intent_id);
        let Some(backend) = self.metadata_backend.as_mut() else {
            return Ok(());
        };
        let Some(prepare) = backend.get_namespace_prepare(&operation_id)? else {
            return Ok(());
        };
        match prepare.status {
            DfsNamespacePrepareStatus::Prepared => {
                backend.abort_prepared_namespace_transaction(
                    &operation_id,
                    &prepare.operation_digest,
                    prepare.owner_epoch,
                )?;
                Ok(())
            }
            DfsNamespacePrepareStatus::Aborted => Ok(()),
            DfsNamespacePrepareStatus::Committed => Err(DfsError::Metadata {
                message: format!(
                    "cannot abort write intent {} because namespace prepare {operation_id} is committed",
                    intent.intent_id
                ),
            }),
        }
    }

    pub fn handoff_object_owner(
        &mut self,
        object_id: &DfsObjectId,
        shard_id: &str,
        expected_epoch: u64,
    ) -> Result<DfsOwnerHandoffOutcome, DfsError> {
        let object = self.current_read_object_metadata(object_id)?;
        let previous_owner_node_id = object.owner_node_id.clone();
        let Some(expected_hash) = object.content_hash.as_deref() else {
            return Ok(DfsOwnerHandoffOutcome::Rejected {
                reason:
                    "candidate cannot prove latest content for an object without a visible hash"
                        .to_string(),
            });
        };
        let manifest = match self
            .content
            .read_manifest(object.object_id.as_str(), object.version.as_str())
        {
            Ok(manifest) => manifest,
            Err(error) => {
                return Ok(DfsOwnerHandoffOutcome::Rejected {
                    reason: format!("candidate is missing latest manifest: {error}"),
                });
            }
        };
        if manifest.content_hash != expected_hash {
            return Ok(DfsOwnerHandoffOutcome::Rejected {
                reason: "candidate manifest hash does not match visible object hash".to_string(),
            });
        }

        let Some(kv) = self.control_plane.as_mut() else {
            return Ok(DfsOwnerHandoffOutcome::Rejected {
                reason: "owner handoff requires control-plane shard fencing".to_string(),
            });
        };
        let transferred = DfsControlPlaneStore::new(kv.as_mut()).transfer_metadata_shard_primary(
            &object.file_system_id,
            shard_id,
            expected_epoch,
            self.node_id.clone(),
        )?;
        let Some(transferred) = transferred else {
            return Ok(DfsOwnerHandoffOutcome::Rejected {
                reason: "control-plane epoch compare failed".to_string(),
            });
        };

        let file_system_to_persist =
            if let Some(file_system) = self.metadata.filesystems.get_mut(&object.file_system_id) {
                file_system.owner_node_id = self.node_id.clone();
                Some(file_system.clone())
            } else {
                None
            };
        if let Some(file_system) = file_system_to_persist.as_ref() {
            if let Some(backend) = self.metadata_backend.as_mut() {
                backend.ensure_file_system(file_system)?;
            }
            if let Some(kv) = self.control_plane.as_mut() {
                DfsControlPlaneStore::new(kv.as_mut())
                    .put_file_system_config(&DfsFileSystemConfig::from(file_system))?;
            }
        }

        let mut updated_object = object.clone();
        updated_object.owner_node_id = self.node_id.clone();
        if !updated_object.current_locations.contains(&self.node_id) {
            updated_object.current_locations.push(self.node_id.clone());
        }
        self.metadata
            .objects
            .insert(updated_object.object_id.clone(), updated_object.clone());
        self.put_visible_object_in_metadata_backend(&updated_object)?;

        Ok(DfsOwnerHandoffOutcome::Transferred {
            file_system_id: updated_object.file_system_id,
            object_id: updated_object.object_id,
            previous_owner_node_id,
            new_owner_node_id: self.node_id.clone(),
            previous_epoch: expected_epoch,
            new_epoch: transferred.epoch,
        })
    }
}
