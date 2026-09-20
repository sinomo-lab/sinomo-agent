//! Retained content uses ordinary intents and receipts, with no head publication.

use crate::transfer::PendingNetworkedWrite;
use crate::*;
use sinomo_core::DfsObjectPolicy;
use time::OffsetDateTime;

fn history_content_error(message: impl Into<String>) -> DfsError {
    DfsError::Metadata {
        message: message.into(),
    }
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub fn preserve_local_file_version<R: std::io::Read>(
        &mut self,
        request: DfsFileHistoryPreserveRequest,
        reader: R,
        options: DfsWriteOptions,
    ) -> Result<DfsFileHistoryPreserveOutcome, DfsError> {
        crate::service_routed_write::validate_write_operation_id(&request.operation_id)?;
        let publication = DfsWritePublication::Retained {
            capture: Box::new(DfsRetainedWritePublication {
                pin_id: request.pin_id,
                source: DfsFileVersionSource::LocalOperation {
                    operation_id: request.local_operation_id,
                    base_version: request.base_version,
                },
            }),
        };
        publication.validate().map_err(history_content_error)?;
        let object = match self.current_object_for_write(&request.object_id) {
            Ok(object) => object,
            Err(DfsError::UnknownObject { .. })
                if request.file_system_id == sifs_file_system_id() =>
            {
                let object = self
                    .file_history_capture_object(&request.path)
                    .map_err(|source| history_content_error(source.to_string()))?;
                if object.object_id != request.object_id {
                    return Err(history_content_error(
                        "preserved content path now identifies a different file",
                    ));
                }
                self.apply_metadata_object_to_service_state(object.clone())?;
                object
            }
            Err(source) => return Err(source),
        };
        if object.file_system_id != request.file_system_id {
            return Err(history_content_error(
                "preserved content must name its canonical filesystem",
            ));
        }
        if object.policy == DfsObjectPolicy::LocalOwned && object.owner_node_id != self.node_id {
            return Err(history_content_error(
                "local-owned content can only be preserved by its owner",
            ));
        }
        self.require_file_history_shard_support(&object.file_system_id, &object.path)?;
        let target_version = crate::intent::stable_write_target_version(
            &self.node_id,
            &object.object_id,
            &request.operation_id,
        );
        let identity = DfsFileVersionIdentity {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            version: target_version.clone(),
        };
        let outcome = self.write_stable_distributed_reader(
            sinomo_core::DfsWriteRequest {
                file_system_id: object.file_system_id,
                object_id: object.object_id,
                target_version,
                requesting_node_id: self.node_id.clone(),
            },
            None,
            reader,
            options.replica_candidates,
            options.policy,
            request.operation_id,
            crate::service_write::DfsDistributedWriteOptions {
                publication,
                repair: if options.foreground_replica_repair {
                    crate::service_write::DfsWriteRepair::Foreground
                } else {
                    crate::service_write::DfsWriteRepair::Background
                },
                ..Default::default()
            },
        )?;
        let outcome = if options.wait_for_commit {
            self.settle_ready_write_outcome(outcome, options.foreground_replica_repair)?
        } else {
            outcome
        };
        match outcome {
            NetworkedDfsWriteOutcome::Committed { .. } => {
                Ok(DfsFileHistoryPreserveOutcome::Retained { identity })
            }
            NetworkedDfsWriteOutcome::Pending { intent_id, .. } => {
                Ok(DfsFileHistoryPreserveOutcome::Pending {
                    identity,
                    intent_id,
                })
            }
            NetworkedDfsWriteOutcome::Conflict { .. } => Err(history_content_error(
                "retained publication cannot create a filesystem conflict",
            )),
        }
    }

    pub(crate) fn ensure_retained_write_object(
        &mut self,
        command: &DfsDistributedWriteCommand,
    ) -> Result<(), DfsError> {
        command.require_publication_mode(true)?;
        let object = self.current_object_for_write(&command.request.object_id)?;
        if object.file_system_id != command.request.file_system_id
            || object.tombstone_provenance.is_some()
            || (object.policy == DfsObjectPolicy::LocalOwned
                && command.request.requesting_node_id != object.owner_node_id)
            || self
                .transient_metadata_object_ids
                .contains(&object.object_id)
        {
            return Err(history_content_error(
                "retained write requires its existing canonical identity",
            ));
        }
        self.require_file_history_shard_support(&object.file_system_id, &object.path)
    }

    pub(crate) fn replayed_retained_write_outcome(
        &mut self,
        requester: &NodeId,
        command: &DfsDistributedWriteCommand,
    ) -> Result<Option<NetworkedDfsWriteOutcome>, DfsError> {
        command.require_publication_mode(true)?;
        crate::service_routed_write::validate_write_operation_id(&command.request_id)?;
        let intent_id = crate::intent::stable_write_intent_id(
            requester,
            &command.request.object_id,
            &command.request_id,
        );
        let Some(backend) = &self.metadata_backend else {
            return Err(history_content_error(
                "retained writes require durable metadata",
            ));
        };
        let record = backend.get_intent(&command.request.object_id, &intent_id)?;
        let Some(record) = record else {
            if backend
                .intent_receipt_retention_for_object(&command.request.object_id)?
                .operation_history_expired(
                    &command.request.object_id,
                    requester,
                    &intent_id,
                    crate::intent::stable_operation_order_token(&command.request_id).as_deref(),
                )
            {
                return Err(DfsError::WriteReconciliationRequired {
                    operation_id: command.request_id.clone(),
                    object_id: command.request.object_id.clone(),
                    reason: "retained operation receipt expired or is unknown".into(),
                });
            }
            return Ok(None);
        };
        let hash = crate::intent::routed_write_command_content_hash(command);
        let size = command
            .content_size_bytes
            .unwrap_or(command.bytes.len() as u64);
        if requester != &command.request.requesting_node_id
            || !crate::intent::stable_write_intent_record_matches_routed_write_command(
                &record, requester, command, &hash,
            )
            || record
                .object_metadata
                .as_ref()
                .and_then(|object| object.content_size_bytes)
                .is_some_and(|expected| expected != size)
        {
            return Err(DfsError::WriteOperationIdConflict {
                operation_id: command.request_id.clone(),
                object_id: command.request.object_id.clone(),
            });
        }
        if record.status == DfsWriteIntentStatus::Committed {
            return Ok(Some(NetworkedDfsWriteOutcome::Committed {
                intent_id,
                version: record.target_version,
                content_hash: hash,
                repair_jobs: self
                    .coordinator
                    .repair_jobs()
                    .iter()
                    .filter(|job| job.intent_id == record.intent_id)
                    .cloned()
                    .collect(),
            }));
        }
        self.restore_retained_content_intent(&record)?;
        self.try_apply_committed_intent_if_ready(&record.intent_id)?;
        self.write_outcome(&record.intent_id).map(Some)
    }

    pub(crate) fn retained_file_content_is_live(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &str,
    ) -> Result<bool, DfsError> {
        let Some(backend) = &self.metadata_backend else {
            return Ok(false);
        };
        let Some(object) = backend.get_object(object_id)? else {
            return Ok(false);
        };
        if object.tombstone_provenance.is_some() {
            return Ok(false);
        }
        let Some(record) = backend.get_version(object_id, version)? else {
            return Ok(false);
        };
        let Some(retained) = &record.retained else {
            return Ok(false);
        };
        if record.file_system_id != object.file_system_id
            || record.content_hash.as_deref() != Some(content_hash)
        {
            return Ok(false);
        }
        if backend
            .version_pins_for_object(object_id)?
            .iter()
            .any(|pin| pin.version == *version)
        {
            return Ok(true);
        }
        let Some(capture) = &retained.policy_capture else {
            return Ok(false);
        };
        let high = backend
            .get_version(object_id, &object.version)?
            .map(|current| current.history_order)
            .unwrap_or(record.history_order)
            .max(record.history_order);
        let newer = usize::try_from(high.saturating_sub(capture.order)).unwrap_or(usize::MAX);
        Ok(capture.retention.retains(
            capture.captured_at_unix_nanos,
            crate::metadata_records::dfs_now_unix_nanos(),
            newer,
        ))
    }

    pub(crate) fn retained_content_transfer_required(
        &self,
        intent_id: &str,
        object_id: &DfsObjectId,
        version: &DfsVersion,
    ) -> Result<bool, DfsError> {
        if self
            .coordinator
            .intent(intent_id)
            .is_some_and(|intent| !intent.publication.is_current())
        {
            return Ok(true);
        }
        Ok(self
            .metadata_backend
            .as_ref()
            .map(|backend| backend.get_version(object_id, version))
            .transpose()?
            .flatten()
            .is_some_and(|record| record.retained.is_some()))
    }

    pub(crate) fn commit_retained_file_intent(
        &mut self,
        intent: &DistributedWriteIntent,
        pending: &PendingNetworkedWrite,
    ) -> Result<(), DfsError> {
        let DfsWritePublication::Retained { capture } = &intent.publication else {
            return Err(history_content_error(
                "retained publication requires an explicit capture",
            ));
        };
        intent
            .publication
            .validate()
            .map_err(history_content_error)?;
        if !self.coordinator.is_commit_ready(&intent.intent_id)? {
            return Err(history_content_error(
                "retained content has not reached its configured receipt threshold",
            ));
        }
        let backend = self
            .metadata_backend
            .as_ref()
            .ok_or_else(|| history_content_error("retained content requires durable metadata"))?;
        let mut record = backend
            .get_intent(&intent.object_id, &intent.intent_id)?
            .ok_or_else(|| history_content_error("retained publication lost its durable intent"))?;
        if record.publication != intent.publication
            || record.target_version != pending.version
            || record.content_hash.as_deref() != Some(&pending.content_hash)
        {
            return Err(history_content_error(
                "retained publication does not match its durable content intent",
            ));
        }
        let current = backend.get_object(&intent.object_id)?.ok_or_else(|| {
            history_content_error("retained publication has no canonical object anchor")
        })?;
        let existing = backend.get_version(&intent.object_id, &intent.target_version)?;
        let transaction_id = format!("file-history-content:{}", intent.intent_id);
        let prepare_id = format!("namespace:{transaction_id}");
        let prepared = backend.get_namespace_prepare(&prepare_id)?;
        let already_captured = existing.as_ref().is_some_and(|version| {
            version.file_system_id == intent.file_system_id
                && version.content_hash.as_deref() == Some(&pending.content_hash)
                && version.size_bytes == Some(pending.size_bytes)
                && version
                    .retained
                    .as_ref()
                    .is_some_and(|retained| retained.source == capture.source)
        });
        if existing.is_some() && !already_captured {
            return Err(history_content_error(
                "retained publication target already has different content or provenance",
            ));
        }
        self.require_file_history_shard_support(&intent.file_system_id, &current.path)?;
        if !already_captured {
            let mut captured_object = record
                .object_metadata
                .clone()
                .unwrap_or_else(|| current.clone());
            captured_object.file_system_id = intent.file_system_id.clone();
            captured_object.object_id = intent.object_id.clone();
            captured_object.path =
                crate::metadata_keys::normalize_posix_path(&captured_object.path);
            captured_object.version = intent.target_version.clone();
            captured_object.content_hash = Some(pending.content_hash.clone());
            captured_object.content_size_bytes = Some(pending.size_bytes);
            captured_object.tombstone_provenance = None;
            if captured_object.kind == DfsObjectKind::Tombstone {
                captured_object.kind = DfsObjectKind::File;
            }
            captured_object.last_modified_by_event_id = None;
            let mut receipts =
                self.backend_receipts_for_version(&intent.object_id, &intent.target_version)?;
            receipts.retain(|receipt| {
                intent
                    .desired_replica_nodes
                    .contains(&receipt.replica_node_id)
                    && receipt.content_hash.as_deref() == Some(&pending.content_hash)
            });
            receipts.sort_by(|left, right| left.replica_node_id.cmp(&right.replica_node_id));
            receipts.dedup_by(|left, right| left.replica_node_id == right.replica_node_id);
            if receipts.len() < intent.policy.min_commit_replicas {
                return Err(history_content_error(
                    "retained publication lacks durable metadata receipt evidence",
                ));
            }
            captured_object.current_locations = receipts
                .iter()
                .map(|receipt| receipt.replica_node_id.clone())
                .collect();
            let version = DfsObjectVersionRecord {
                file_system_id: intent.file_system_id.clone(),
                object_id: intent.object_id.clone(),
                version: intent.target_version.clone(),
                content_hash: Some(pending.content_hash.clone()),
                size_bytes: Some(pending.size_bytes),
                locations: receipts
                    .iter()
                    .map(|receipt| receipt.replica_node_id.clone())
                    .collect(),
                created_by_event_id: None,
                created_at: record.created_at,
                visible: false,
                history_order: 0,
                retained: Some(Box::new(DfsRetainedFileVersion {
                    object: Box::new(captured_object),
                    captured_at_unix_nanos: u64::try_from(record.created_at.unix_timestamp_nanos())
                        .unwrap_or(0),
                    source: capture.source.clone(),
                    policy_capture: None,
                })),
            };
            let pin = DfsMetadataVersionPinRecord {
                file_system_id: intent.file_system_id.clone(),
                object_id: intent.object_id.clone(),
                version: intent.target_version.clone(),
                pin_id: capture.pin_id.clone(),
                created_at: record.created_at,
            };
            let transaction =
                prepared
                    .map(|prepare| prepare.transaction)
                    .unwrap_or(DfsNamespaceTransaction {
                        transaction_id,
                        file_system_id: intent.file_system_id.clone(),
                        objects: Vec::new(),
                        versions: vec![version],
                        file_version_pins: Some(DfsFileVersionPinMutation {
                            add: vec![pin],
                            remove: Vec::new(),
                        }),
                        advisory_lock_states: Vec::new(),
                        created_at: record.created_at,
                    });
            self.apply_retained_content_transaction(transaction, receipts)?;
        }
        // The durable capture is the commit boundary; recovery may repeat this
        // bookkeeping without re-creating a pin explicitly released afterward.
        record.status = DfsWriteIntentStatus::Committed;
        record.updated_at = Some(OffsetDateTime::now_utc());
        self.metadata_backend
            .as_mut()
            .expect("durable capture backend")
            .put_intent(&record)?;
        self.coordinator.commit_intent(&intent.intent_id)?;
        if !self.retained_file_content_is_live(
            &intent.object_id,
            &intent.target_version,
            &pending.content_hash,
        )? {
            // A completed capture may be replayed after its pin was released.
            // Complete the original write witness without restarting its N work.
            self.retire_committed_repair(&intent.intent_id);
        }
        Ok(())
    }

    pub(crate) fn restore_retained_content_intent(
        &mut self,
        record: &DfsWriteIntentRecord,
    ) -> Result<(), DfsError> {
        let Some(anchor) = self
            .metadata_backend
            .as_ref()
            .map(|backend| backend.get_object(&record.object_id))
            .transpose()?
            .flatten()
        else {
            return Ok(());
        };
        let intent = crate::intent::distributed_intent_from_record(record);
        match self
            .coordinator
            .restore_intent_deferring_repair_rebuild(intent.clone())
        {
            Ok(()) => {}
            Err(DfsDistributedError::DuplicateIntent { .. })
                if self.coordinator.intent(&intent.intent_id) == Some(&intent) => {}
            Err(error) => return Err(error.into()),
        }
        let Some(hash) = &record.content_hash else {
            return Ok(());
        };
        for receipt in
            self.backend_receipts_for_version(&record.object_id, &record.target_version)?
        {
            if receipt.content_hash.as_ref() == Some(hash)
                && intent
                    .desired_replica_nodes
                    .contains(&receipt.replica_node_id)
            {
                self.coordinator
                    .restore_durable_receipt_deferring_repair_rebuild(
                        &intent.intent_id,
                        receipt.replica_node_id,
                    )?;
            }
        }
        let active = matches!(
            record.status,
            DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
        );
        let locally_owned = if active {
            record.coordinator_node_id == self.node_id
        } else {
            self.is_local_metadata_primary_for_object(&anchor)?
        };
        let rooted = active
            || self.retained_file_content_is_live(
                &record.object_id,
                &record.target_version,
                hash,
            )?;
        if !rooted && record.status == DfsWriteIntentStatus::Committed {
            self.retire_committed_repair(&record.intent_id);
            return Ok(());
        }
        if locally_owned
            && rooted
            && matches!(
                record.status,
                DfsWriteIntentStatus::Staging
                    | DfsWriteIntentStatus::ReceiptsPending
                    | DfsWriteIntentStatus::Committed
            )
        {
            let version = self
                .metadata_backend
                .as_ref()
                .map(|backend| backend.get_version(&record.object_id, &record.target_version))
                .transpose()?
                .flatten();
            let size = version
                .and_then(|version| version.size_bytes)
                .or_else(|| {
                    record
                        .object_metadata
                        .as_ref()
                        .and_then(|object| object.content_size_bytes)
                })
                .unwrap_or(0);
            self.writes
                .pending_writes
                .entry(record.intent_id.clone())
                .or_insert_with(|| {
                    PendingNetworkedWrite::new(
                        record.object_id.clone(),
                        record.target_version.clone(),
                        hash.clone(),
                        size,
                    )
                    .with_foreground_replica_repair(false)
                });
        }
        Ok(())
    }
}
