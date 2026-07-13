//! Routed distributed-write request and response lifecycle.
//!
//! This module owns metadata-primary routed writes: replay detection,
//! requester-side response handling, responder-side pending response tracking,
//! authority redirects, and deferred parent-directory touches for newly
//! created objects. Generic inbound message dispatch remains in
//! `service_protocol`.

use std::time::Instant;

use sinomo_core::DfsWriteRequest;
use sinomo_core::events::EventId;
use sinomo_core::node::NodeId;

use crate::metadata_pages::walk_metadata_pages;
use crate::*;

fn routed_write_command_content_size_bytes(command: &DfsDistributedWriteCommand) -> Option<u64> {
    if command.content_hash.is_some() {
        command.content_size_bytes
    } else {
        Some(command.bytes.len() as u64)
    }
}

fn routed_exclusive_create_event_id(command: &DfsDistributedWriteCommand) -> Option<EventId> {
    if !command.exclusive_create {
        return None;
    }
    let object = command.object_metadata.as_ref()?;
    let identity = format!(
        "{}\0{}\0{}\0{:?}\0{}\0{}\0{}",
        command.request_id,
        command.request.object_id,
        object.path,
        object.kind,
        object.mode,
        object.uid,
        object.gid,
    );
    Some(EventId::derived(
        "sinomo-dfs.routed-exclusive-create.v1",
        identity.as_bytes(),
    ))
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn replayed_routed_write_outcome_if_known(
        &mut self,
        requester_node_id: &NodeId,
        command: &DfsDistributedWriteCommand,
    ) -> Result<Option<NetworkedDfsWriteOutcome>, DfsError> {
        if requester_node_id != &command.request.requesting_node_id {
            return Ok(None);
        }
        if let Some(expected_event_id) = routed_exclusive_create_event_id(command) {
            let exact_create_is_still_visible =
                match self.current_read_object_metadata(&command.request.object_id) {
                    Ok(object) => {
                        object.kind != DfsObjectKind::Tombstone
                            && object.version == command.request.target_version
                            && object.last_modified_by_event_id.as_ref() == Some(&expected_event_id)
                    }
                    Err(DfsError::UnknownObject { .. }) => false,
                    Err(error) => return Err(error),
                };
            if !exact_create_is_still_visible {
                return Ok(None);
            }
        }
        let expected_content_hash = routed_write_command_content_hash(command);
        let Some(expected_content_size_bytes) = routed_write_command_content_size_bytes(command)
        else {
            return Ok(None);
        };
        if let Some(intent) = self
            .coordinator
            .intent_for_target(
                &command.request.file_system_id,
                &command.request.object_id,
                &command.request.target_version,
            )
            .cloned()
            && distributed_intent_matches_routed_write_command(&intent, requester_node_id, command)
            && self.routed_write_intent_matches_content_identity(
                &intent,
                &expected_content_hash,
                expected_content_size_bytes,
            )?
        {
            return self.replayed_routed_write_outcome_for_intent(&intent);
        }

        let matching_record = {
            let Some(backend) = self.metadata_backend.as_ref() else {
                return Ok(None);
            };
            let Some(version) =
                backend.get_version(&command.request.object_id, &command.request.target_version)?
            else {
                return Ok(None);
            };
            if version
                .size_bytes
                .is_some_and(|size| size != expected_content_size_bytes)
            {
                return Ok(None);
            }
            let mut matching_record = None;
            walk_metadata_pages(|cursor| {
                let page = backend.intents_for_object_version_page(
                    &command.request.object_id,
                    &command.request.target_version,
                    cursor,
                    DFS_METADATA_OBJECT_INTENT_PAGE_SIZE,
                )?;
                for record in page.intents {
                    if write_intent_record_matches_routed_write_command(
                        &record,
                        requester_node_id,
                        command,
                        &expected_content_hash,
                    ) {
                        matching_record = Some(record);
                        return Ok(None);
                    }
                }
                Ok(page.next_cursor)
            })?;
            matching_record
        };
        let Some(record) = matching_record else {
            return Ok(None);
        };
        self.restore_pending_write_for_intent_if_missing(&record.intent_id, &record.object_id)?;
        self.restore_backend_receipt_pages_for_intent(&record, true)?;
        if matches!(record.status, DfsWriteIntentStatus::Committed) {
            self.restore_visible_committed_object_location_receipts(&record)?;
        }
        let Some(intent) = self.coordinator.intent(&record.intent_id).cloned() else {
            return Ok(None);
        };
        self.replayed_routed_write_outcome_for_intent(&intent)
    }

    /// Proves that an in-memory intent belongs to this exact write payload
    /// before the response-safe transport path treats the command as a replay.
    /// The coordinator intentionally does not own content bytes, so consult the
    /// pending transfer first and then its durable intent/current version.
    fn routed_write_intent_matches_content_identity(
        &self,
        intent: &DistributedWriteIntent,
        expected_content_hash: &str,
        expected_size_bytes: u64,
    ) -> Result<bool, DfsError> {
        if let Some(pending) = self.pending_writes.get(&intent.intent_id) {
            return Ok(pending.content_hash == expected_content_hash
                && pending.size_bytes == expected_size_bytes);
        }

        if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(record) = backend.get_intent(&intent.object_id, &intent.intent_id)?
        {
            if record.content_hash.as_deref() != Some(expected_content_hash) {
                return Ok(false);
            }
            if let Some(version) = backend.get_version(&intent.object_id, &intent.target_version)?
                && version
                    .size_bytes
                    .is_some_and(|size| size != expected_size_bytes)
            {
                return Ok(false);
            }
            return Ok(true);
        }

        Ok(matches!(intent.state, DfsWriteIntentState::Committed)
            && self
                .current_read_object_metadata(&intent.object_id)
                .is_ok_and(|object| {
                    object.version == intent.target_version
                        && object.content_hash.as_deref() == Some(expected_content_hash)
                        && object
                            .content_size_bytes
                            .is_none_or(|size| size == expected_size_bytes)
                }))
    }

    fn replayed_routed_write_outcome_for_intent(
        &mut self,
        intent: &DistributedWriteIntent,
    ) -> Result<Option<NetworkedDfsWriteOutcome>, DfsError> {
        match self.write_outcome(&intent.intent_id) {
            Ok(outcome) => Ok(Some(outcome)),
            Err(error) if matches!(intent.state, DfsWriteIntentState::AwaitingReceipts) => {
                let _ = error;
                Ok(Some(NetworkedDfsWriteOutcome::Pending {
                    intent_id: intent.intent_id.clone(),
                    durable_receipts: self.coordinator.durable_receipt_count(&intent.intent_id),
                    required_receipts: intent.policy.min_commit_replicas,
                }))
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) fn handle_distributed_write_request(
        &mut self,
        requester_node_id: NodeId,
        mut command: DfsDistributedWriteCommand,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let request_id = command.request_id.clone();
        let object_id = command.request.object_id.clone();
        let file_system_id = command.request.file_system_id.clone();
        let target_version = command.request.target_version.clone();
        let foreground_replica_repair = command.foreground_replica_repair;
        let metadata_journal_after_sequence = command.metadata_journal_after_sequence;
        let routed_object_path = command
            .object_metadata
            .as_ref()
            .map(|object| object.path.clone());
        let touch_parent_on_create = command.touch_parent_on_create;
        let trace_started = Instant::now();
        let trace_payload_bytes = command.bytes.len();
        let trace_manifest_only = command.content_hash.is_some();
        let mut created_object_path = None;
        let mut injected_transient_metadata = false;
        let apply_started = Instant::now();
        let result = if requester_node_id != command.request.requesting_node_id {
            DfsDistributedWriteResult::Rejected {
                reason: format!(
                    "distributed write requester mismatch: frame source {}, payload requester {}",
                    requester_node_id, command.request.requesting_node_id
                ),
                current_primary_node_id: None,
            }
        } else if let Some(outcome) =
            self.replayed_routed_write_outcome_if_known(&requester_node_id, &command)?
        {
            DfsDistributedWriteResult::Outcome { outcome }
        } else {
            self.rebase_routed_path_write_if_current(&mut command)?;
            let write_foreground_replica_repair = foreground_replica_repair
                && !self
                    .transport
                    .can_return_committed_foreground_response_before_replica_repair();
            let manifest = match command.content_hash.clone() {
                Some(content_hash) => {
                    let content_size_bytes =
                        command
                            .content_size_bytes
                            .ok_or_else(|| DfsError::Distributed {
                                message: format!(
                                    "routed reader-backed write for {} missing content size",
                                    command.request.object_id
                                ),
                            });
                    if !command.bytes.is_empty() {
                        Err(DfsError::Distributed {
                            message: format!(
                                "routed reader-backed write for {} must not include inline bytes",
                                command.request.object_id
                            ),
                        })
                    } else {
                        content_size_bytes.map(|size| Some((content_hash, size)))
                    }
                }
                None => Ok(None),
            };
            match manifest.and_then(|manifest| {
                let exclusive_create_event_id = routed_exclusive_create_event_id(&command);
                self
                .ensure_routed_write_object_metadata(
                    &command.request,
                    command.object_metadata.as_ref(),
                    command.exclusive_create,
                    exclusive_create_event_id,
                )
                .and_then(|created| {
                    injected_transient_metadata = created;
                    if created && touch_parent_on_create {
                        created_object_path = routed_object_path;
                    }
                    if let Some((content_hash, content_size_bytes)) = manifest {
                        self.write_distributed_eventual_from_remote_manifest_with_base_and_parent_touch(
                            command.request,
                            command.expected_base_version,
                            content_hash,
                            content_size_bytes,
                            command.desired_replica_nodes,
                            command.policy,
                            created_object_path.is_some(),
                            command.exclusive_create,
                            write_foreground_replica_repair,
                        )
                    } else {
                        self.write_distributed_eventual_with_base_and_parent_touch_and_repair_mode(
                            command.request,
                            command.expected_base_version,
                            command.bytes,
                            command.desired_replica_nodes,
                            command.policy,
                            created_object_path.is_some(),
                            command.exclusive_create,
                            write_foreground_replica_repair,
                            false,
                            true,
                        )
                    }
                })
            }) {
                Ok(outcome) => {
                    let response_outcome = match &outcome {
                        NetworkedDfsWriteOutcome::Pending { intent_id, .. } => {
                            self.track_pending_remote_write_response(
                                intent_id,
                                &requester_node_id,
                                &request_id,
                                &object_id,
                                metadata_journal_after_sequence,
                                created_object_path.clone(),
                                foreground_replica_repair,
                            );
                            outcome.clone()
                        }
                        NetworkedDfsWriteOutcome::Committed {
                            intent_id,
                            repair_jobs,
                            ..
                        } if foreground_replica_repair && !repair_jobs.is_empty() => {
                            self.track_pending_remote_write_response(
                                intent_id,
                                &requester_node_id,
                                &request_id,
                                &object_id,
                                metadata_journal_after_sequence,
                                None,
                                foreground_replica_repair,
                            );
                            NetworkedDfsWriteOutcome::Pending {
                                intent_id: intent_id.clone(),
                                durable_receipts: self.coordinator.durable_receipt_count(intent_id),
                                required_receipts: self
                                    .coordinator
                                    .intent(intent_id)
                                    .map(|intent| intent.policy.min_commit_replicas)
                                    .unwrap_or_else(|| {
                                        self.coordinator.durable_receipt_count(intent_id)
                                    }),
                            }
                        }
                        NetworkedDfsWriteOutcome::Committed { .. } => outcome.clone(),
                        NetworkedDfsWriteOutcome::Conflict { .. } => outcome.clone(),
                    };
                    DfsDistributedWriteResult::Outcome {
                        outcome: response_outcome,
                    }
                }
                Err(error) => {
                    let failed_intent = self
                        .coordinator
                        .intent_for_target(
                            &file_system_id,
                            &object_id,
                            &target_version,
                        )
                        .cloned();
                    let committed = failed_intent.as_ref().is_some_and(|intent| {
                        matches!(intent.state, DfsWriteIntentState::Committed)
                    });
                    let mut cleanup_error = None;
                    if injected_transient_metadata && !committed {
                        let mut cleanup_completed = true;
                        if let Some(intent) = failed_intent {
                            let content_hash = self
                                .pending_writes
                                .get(&intent.intent_id)
                                .map(|pending| pending.content_hash.clone());
                            let aborted = if matches!(
                                intent.state,
                                DfsWriteIntentState::AwaitingReceipts
                            ) {
                                match self.coordinator.abort_intent(&intent.intent_id) {
                                    Ok(aborted) => Some(aborted),
                                    Err(source) => {
                                        cleanup_error = Some(source.to_string());
                                        cleanup_completed = false;
                                        None
                                    }
                                }
                            } else {
                                Some(intent.clone())
                            };
                            if let Some(aborted) = aborted {
                                if let Err(source) =
                                    self.put_metadata_intent(&aborted, content_hash.as_deref())
                                {
                                    cleanup_error = Some(source.to_string());
                                    cleanup_completed = false;
                                } else {
                                    self.pending_writes.remove(&intent.intent_id);
                                }
                            }
                        }
                        if cleanup_completed {
                            self.remove_transient_object_metadata(&object_id);
                        }
                    }
                    if let Some(cleanup_error) = cleanup_error {
                        return Err(DfsError::Distributed {
                            message: format!(
                                "{error}; failed to durably abort rejected routed create: {cleanup_error}; metadata authority is quarantining the create"
                            ),
                        });
                    }
                    let current_primary_node_id =
                        distributed_write_rejection_current_primary(&error);
                    DfsDistributedWriteResult::Rejected {
                        reason: error.to_string(),
                        current_primary_node_id,
                    }
                }
            }
        };
        let apply_elapsed = apply_started.elapsed();
        let journal_started = Instant::now();
        let metadata_journal_batches = self.routed_write_response_metadata_journal_batches(
            &object_id,
            &result,
            metadata_journal_after_sequence,
            if foreground_replica_repair {
                DFS_ROUTED_RESPONSE_JOURNAL_BATCH_LIMIT
            } else {
                DFS_ROUTED_WRITE_RESPONSE_JOURNAL_BATCH_LIMIT
            },
        )?;
        let journal_elapsed = journal_started.elapsed();
        let metadata_journal_batch_count = metadata_journal_batches.len();
        let send_started = Instant::now();
        let send_result = self.send_dfs_message_if_routable(
            &requester_node_id,
            DfsNetworkMessage::DistributedWriteResponse(DfsDistributedWriteResponse {
                request_id: request_id.clone(),
                object_id: object_id.clone(),
                result: result.clone(),
                metadata_journal_batches,
            }),
        );
        let send_elapsed = send_started.elapsed();
        dfs_perf_trace(format_args!(
            "distributed-write-serve node={} requester={} request={} object={} payload_bytes={} manifest_only={} result={} journal_batches={} apply_ms={} journal_ms={} response_send_ms={} total_ms={} response_sent={}",
            self.node_id,
            requester_node_id,
            request_id,
            object_id,
            trace_payload_bytes,
            trace_manifest_only,
            dfs_distributed_write_result_trace_label(&result),
            metadata_journal_batch_count,
            apply_elapsed.as_millis(),
            journal_elapsed.as_millis(),
            send_elapsed.as_millis(),
            trace_started.elapsed().as_millis(),
            send_result.is_ok()
        ));
        send_result?;
        self.schedule_committed_routed_write_local_fetch_if_needed(&result)?;
        Ok(NetworkedDfsServiceEvent::RemoteWriteServed {
            request_id,
            object_id,
            requester_node_id,
            result,
        })
    }

    fn schedule_committed_routed_write_local_fetch_if_needed(
        &mut self,
        result: &DfsDistributedWriteResult,
    ) -> Result<(), DfsError> {
        let DfsDistributedWriteResult::Outcome {
            outcome:
                NetworkedDfsWriteOutcome::Committed {
                    intent_id,
                    repair_jobs,
                    ..
                },
        } = result
        else {
            return Ok(());
        };
        if !repair_jobs
            .iter()
            .any(|job| job.missing_replica_node_id == self.node_id)
        {
            return Ok(());
        }
        let Some(intent) = self.coordinator.intent(intent_id).cloned() else {
            return Ok(());
        };
        let Some(pending) = self.pending_writes.get(intent_id).cloned() else {
            return Ok(());
        };
        let _ = self.request_stage_blob_from_alternate(&intent, &pending)?;
        Ok(())
    }

    fn track_pending_remote_write_response(
        &mut self,
        intent_id: &str,
        requester_node_id: &NodeId,
        request_id: &str,
        object_id: &DfsObjectId,
        metadata_journal_after_sequence: Option<u64>,
        created_object_path: Option<String>,
        foreground_replica_repair: bool,
    ) {
        if let Some(existing) = self.pending_remote_write_responses.get_mut(intent_id) {
            if existing.requester_node_id == *requester_node_id
                && existing.request_id == request_id
                && existing.object_id == *object_id
            {
                // A response-safe transport retry can redispatch the exact
                // request while its intent is still pending. Replay no longer
                // runs namespace creation, so preserve the first dispatch's
                // parent-touch obligation until the write commits.
                if existing.created_object_path.is_none() {
                    existing.created_object_path = created_object_path;
                }
            }
            return;
        }
        self.pending_remote_write_responses.insert(
            intent_id.to_string(),
            PendingRemoteWriteResponse {
                requester_node_id: requester_node_id.clone(),
                request_id: request_id.to_string(),
                object_id: object_id.clone(),
                metadata_journal_after_sequence,
                created_object_path,
                foreground_replica_repair,
                created_at: Instant::now(),
            },
        );
    }

    fn rebase_routed_path_write_if_current(
        &mut self,
        command: &mut DfsDistributedWriteCommand,
    ) -> Result<(), DfsError> {
        if command.bytes.is_empty() && command.content_hash.is_none() {
            return Ok(());
        }
        let Some(path_object) = command.object_metadata.as_ref() else {
            return Ok(());
        };
        let current = match self.current_object_for_write(&command.request.object_id) {
            Ok(current) => current,
            Err(DfsError::UnknownObject { .. }) => return Ok(()),
            Err(error) => return Err(error),
        };
        let target = if current.kind == DfsObjectKind::Hardlink {
            self.resolve_hardlink_target(&current)
                .map_err(|error| DfsError::Metadata {
                    message: error.to_string(),
                })?
        } else {
            current.clone()
        };
        if !matches!(target.kind, DfsObjectKind::File | DfsObjectKind::Symlink) {
            return Ok(());
        }
        let path_resolves_to_hardlink = current.kind == DfsObjectKind::Hardlink;
        if path_object.content_hash.is_none() && !path_resolves_to_hardlink {
            return Ok(());
        }
        if path_object.path != current.path && path_object.path != target.path {
            return Ok(());
        }
        let stale_base = command
            .expected_base_version
            .as_ref()
            .is_some_and(|expected| expected != &target.version);
        let different_object = command.request.object_id != target.object_id;
        if !stale_base && !different_object {
            return Ok(());
        }
        command.request.object_id = target.object_id.clone();
        command.request.target_version = namespace::next_version(&target.version);
        command.expected_base_version = Some(target.version.clone());
        command.object_metadata = Some(target);
        if command.content_hash.is_some() && !different_object {
            command.foreground_replica_repair = false;
        }
        Ok(())
    }

    pub(crate) fn handle_distributed_write_response(
        &mut self,
        primary_node_id: NodeId,
        response: DfsDistributedWriteResponse,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        // Late, duplicate, or mismatched responses (a request that already
        // settled, timed out, or was retried against another primary) are
        // discarded; a stray frame must never fail the whole event loop.
        {
            let Some(pending) = self.pending_routed_writes.get(&response.request_id) else {
                return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                    request_id: response.request_id,
                    source_node_id: primary_node_id,
                    reason: "distributed write response for an untracked request".to_string(),
                });
            };
            if pending.primary_node_id != primary_node_id {
                let reason = format!(
                    "distributed write response did not come from primary {}",
                    pending.primary_node_id
                );
                return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                    request_id: response.request_id,
                    source_node_id: primary_node_id,
                    reason,
                });
            }
            if pending.object_id != response.object_id {
                let reason = format!(
                    "distributed write response object mismatch: expected {}, actual {}",
                    pending.object_id, response.object_id
                );
                return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                    request_id: response.request_id,
                    source_node_id: primary_node_id,
                    reason,
                });
            }
        }
        for batch in &response.metadata_journal_batches {
            if self.routed_response_journal_batch_targets_local_primary(batch)? {
                continue;
            }
            if let Err(error) =
                self.apply_routed_write_metadata_journal_batch(&primary_node_id, batch)
            {
                // A replay failure is this requester's problem (for example a
                // forked local journal); fail the request with the cause
                // instead of poisoning the whole event loop.
                let reason = format!("routed write journal replay failed: {error}");
                if let Some(pending) = self.pending_routed_writes.get_mut(&response.request_id) {
                    pending.result = Some(Err(DfsError::Distributed {
                        message: reason.clone(),
                    }));
                }
                return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                    request_id: response.request_id,
                    source_node_id: primary_node_id,
                    reason,
                });
            }
        }
        if let DfsDistributedWriteResult::Rejected {
            reason,
            current_primary_node_id: Some(current_primary_node_id),
        } = &response.result
        {
            let redirect = self
                .pending_routed_writes
                .get(&response.request_id)
                .and_then(|pending| {
                    (current_primary_node_id != &pending.primary_node_id).then(|| {
                        (
                            current_primary_node_id.clone(),
                            pending.command.clone(),
                            pending.authority_redirects_remaining,
                        )
                    })
                });
            if let Some((current_primary_node_id, command, redirects_remaining)) = redirect {
                if redirects_remaining == 0 {
                    if let Some(pending) = self.pending_routed_writes.get_mut(&response.request_id)
                    {
                        pending.result = Some(Err(DfsError::Distributed {
                            message: format!(
                                "routed distributed write exceeded {DFS_ROUTED_WRITE_AUTHORITY_REDIRECT_LIMIT} authority redirects: {reason}"
                            ),
                        }));
                    }
                    return Ok(NetworkedDfsServiceEvent::RemoteWriteCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        primary_node_id,
                        result: response.result,
                    });
                } else {
                    let message = if command.exclusive_create {
                        DfsNetworkMessage::ExclusiveCreateRequest(command.clone())
                    } else {
                        DfsNetworkMessage::DistributedWriteRequest(command.clone())
                    };
                    if self.send_dfs_message_if_routable(&current_primary_node_id, message)? {
                        if let Some(pending) =
                            self.pending_routed_writes.get_mut(&response.request_id)
                        {
                            pending.primary_node_id = current_primary_node_id.clone();
                            pending.command = command;
                            pending.authority_redirects_remaining = redirects_remaining - 1;
                            pending.remote_intent_id = None;
                            pending.durable_receipts = 0;
                            pending.required_receipts = 1;
                            pending.result = None;
                        }
                        return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                            request_id: response.request_id,
                            source_node_id: primary_node_id,
                            reason: format!(
                                "distributed write authority moved to {current_primary_node_id}; retrying"
                            ),
                        });
                    }
                    if let Some(pending) = self.pending_routed_writes.get_mut(&response.request_id)
                    {
                        pending.result = Some(Err(DfsError::Network {
                            message: format!(
                                "redirected metadata primary {current_primary_node_id} is not routable for distributed write"
                            ),
                        }));
                        return Ok(NetworkedDfsServiceEvent::RemoteWriteCompleted {
                            request_id: response.request_id,
                            object_id: response.object_id,
                            primary_node_id,
                            result: response.result,
                        });
                    }
                }
            }
        }
        let pending = self
            .pending_routed_writes
            .get_mut(&response.request_id)
            .expect("pending routed write was validated before metadata replay");
        let mut remote_intent_alias = None;
        let mut committed_manifest_publish = None;
        match &response.result {
            DfsDistributedWriteResult::Outcome {
                outcome:
                    NetworkedDfsWriteOutcome::Pending {
                        intent_id,
                        durable_receipts,
                        required_receipts,
                        ..
                    },
            } => {
                if pending.result.is_none() {
                    pending.remote_intent_id = Some(intent_id.clone());
                    pending.durable_receipts = *durable_receipts;
                    pending.required_receipts = *required_receipts;
                }
                if intent_id != &response.request_id {
                    remote_intent_alias = Some((intent_id.clone(), pending.clone()));
                }
            }
            DfsDistributedWriteResult::Outcome {
                outcome: outcome @ NetworkedDfsWriteOutcome::Committed { intent_id, .. },
            } => {
                committed_manifest_publish = Some((pending.command.clone(), outcome.clone()));
                pending.result = Some(Ok(outcome.clone()));
                if intent_id != &response.request_id {
                    remote_intent_alias = Some((intent_id.clone(), pending.clone()));
                }
            }
            DfsDistributedWriteResult::Outcome { outcome } => {
                pending.result = Some(Ok(outcome.clone()));
            }
            DfsDistributedWriteResult::Rejected { reason, .. } => {
                pending.result = Some(Err(routed_write_rejection_error(reason)));
            }
        }
        if let Some((command, outcome)) = committed_manifest_publish {
            self.publish_committed_routed_write_manifest_if_local(&command, &outcome);
        }
        if let Some((intent_id, pending)) = remote_intent_alias {
            self.pending_routed_writes
                .entry(intent_id)
                .or_insert(pending);
        }
        Ok(NetworkedDfsServiceEvent::RemoteWriteCompleted {
            request_id: response.request_id,
            object_id: response.object_id,
            primary_node_id,
            result: response.result,
        })
    }

    fn publish_committed_routed_write_manifest_if_local(
        &mut self,
        command: &DfsDistributedWriteCommand,
        outcome: &NetworkedDfsWriteOutcome,
    ) {
        let NetworkedDfsWriteOutcome::Committed {
            version,
            content_hash,
            ..
        } = outcome
        else {
            return;
        };
        let size_bytes = command
            .content_size_bytes
            .unwrap_or_else(|| command.bytes.len() as u64);
        let object_id = self
            .current_read_object_metadata(&command.request.object_id)
            .ok()
            .and_then(|object| self.resolve_hardlink_target(&object).ok())
            .map(|object| object.object_id)
            .unwrap_or_else(|| command.request.object_id.clone());
        if let Err(error) = self.content.put_blob_only_manifest(
            object_id.as_str(),
            version.as_str(),
            content_hash,
            size_bytes,
        ) {
            dfs_perf_trace(format_args!(
                "routed-write-local-manifest-publish-failed node={} request={} object={} version={} error={}",
                self.node_id, command.request_id, object_id, version, error
            ));
        }
    }

    pub(crate) fn flush_completed_pending_remote_write_responses(
        &mut self,
        events: &mut Vec<NetworkedDfsServiceEvent>,
    ) -> Result<(), DfsError> {
        if self.pending_remote_write_responses.is_empty() {
            return Ok(());
        }
        let intent_ids = self
            .pending_remote_write_responses
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for intent_id in intent_ids {
            let result = match self.write_outcome(&intent_id) {
                Ok(NetworkedDfsWriteOutcome::Pending { .. }) => continue,
                Ok(outcome) => DfsDistributedWriteResult::Outcome { outcome },
                Err(error) => {
                    let current_primary_node_id =
                        distributed_write_rejection_current_primary(&error);
                    DfsDistributedWriteResult::Rejected {
                        reason: error.to_string(),
                        current_primary_node_id,
                    }
                }
            };
            let committed_foreground_with_repairs = self
                .pending_remote_write_responses
                .get(&intent_id)
                .is_some_and(|pending| pending.foreground_replica_repair)
                && matches!(
                    &result,
                    DfsDistributedWriteResult::Outcome {
                        outcome: NetworkedDfsWriteOutcome::Committed { repair_jobs, .. },
                    } if !repair_jobs.is_empty()
                );
            if committed_foreground_with_repairs
                && !self
                    .transport
                    .can_return_committed_foreground_response_before_replica_repair()
            {
                continue;
            }
            // Node-routed DFS RPCs can return once the metadata primary has
            // committed the intent. Remaining repair jobs stay in the normal
            // repair backlog so a slow desired replica does not consume the
            // caller's data-plane deadline after the write is durable.
            let Some(pending) = self.pending_remote_write_responses.remove(&intent_id) else {
                continue;
            };
            let metadata_journal_batches = self.routed_write_response_metadata_journal_batches(
                &pending.object_id,
                &result,
                pending.metadata_journal_after_sequence,
                if pending.foreground_replica_repair {
                    DFS_ROUTED_RESPONSE_JOURNAL_BATCH_LIMIT
                } else {
                    DFS_ROUTED_WRITE_RESPONSE_JOURNAL_BATCH_LIMIT
                },
            )?;
            self.send_dfs_message_if_routable(
                &pending.requester_node_id,
                DfsNetworkMessage::DistributedWriteResponse(DfsDistributedWriteResponse {
                    request_id: pending.request_id.clone(),
                    object_id: pending.object_id.clone(),
                    result: result.clone(),
                    metadata_journal_batches,
                }),
            )?;
            events.push(NetworkedDfsServiceEvent::RemoteWriteServed {
                request_id: pending.request_id,
                object_id: pending.object_id,
                requester_node_id: pending.requester_node_id,
                result,
            });
        }
        Ok(())
    }

    pub(crate) fn send_untracked_completed_routed_write_response_if_committed(
        &mut self,
        intent_id: &str,
        object_id: &DfsObjectId,
    ) -> Result<(), DfsError> {
        if !self.transport.can_send_synchronously() {
            self.deferred_untracked_routed_write_responses
                .insert(intent_id.to_string(), object_id.clone());
            return Ok(());
        }
        let Some(intent) = self.coordinator.intent(intent_id).cloned() else {
            return Ok(());
        };
        if !matches!(intent.state, DfsWriteIntentState::Committed)
            || intent.requesting_node_id == self.node_id
            || self.pending_remote_write_responses.contains_key(intent_id)
        {
            return Ok(());
        }
        let outcome = self.write_outcome(intent_id)?;
        if !matches!(outcome, NetworkedDfsWriteOutcome::Committed { .. }) {
            return Ok(());
        }
        let result = DfsDistributedWriteResult::Outcome { outcome };
        let metadata_journal_batches = self.routed_write_response_metadata_journal_batches(
            object_id,
            &result,
            None,
            DFS_ROUTED_WRITE_RESPONSE_JOURNAL_BATCH_LIMIT,
        )?;
        self.send_dfs_message_if_routable(
            &intent.requesting_node_id,
            DfsNetworkMessage::DistributedWriteResponse(DfsDistributedWriteResponse {
                request_id: intent.intent_id,
                object_id: object_id.clone(),
                result,
                metadata_journal_batches,
            }),
        )?;
        Ok(())
    }

    /// Ensures the routed write's object metadata is known locally. Returns
    /// `true` when the object was unknown and the requester-provided metadata
    /// was injected, i.e. the routed write creates the object at its path.
    fn ensure_routed_write_object_metadata(
        &mut self,
        request: &DfsWriteRequest,
        object_metadata: Option<&DfsObjectMetadata>,
        exclusive_create: bool,
        exclusive_create_event_id: Option<EventId>,
    ) -> Result<bool, DfsError> {
        match self.current_object_for_write(&request.object_id) {
            Ok(current) if exclusive_create && current.kind != DfsObjectKind::Tombstone => {
                return Err(DfsError::AlreadyExists { path: current.path });
            }
            Ok(_) => return Ok(false),
            Err(DfsError::UnknownObject { .. }) => {}
            Err(error) => return Err(error),
        }
        let Some(object) = object_metadata else {
            return Err(DfsError::UnknownObject {
                object_id: request.object_id.clone(),
            });
        };
        if object.object_id != request.object_id || object.file_system_id != request.file_system_id
        {
            return Err(DfsError::Distributed {
                message: format!(
                    "routed write metadata mismatch for object {}",
                    request.object_id
                ),
            });
        }
        let Some(mut shard) = self.select_metadata_shard_for_object(object)? else {
            return Err(DfsError::Distributed {
                message: format!(
                    "routed write for {} has no metadata shard assignment",
                    request.object_id
                ),
            });
        };
        if shard.primary_node_id != self.node_id {
            self.refresh_control_plane_latest()?;
            let Some(refreshed) = self.select_metadata_shard_for_object(object)? else {
                return Err(DfsError::Distributed {
                    message: format!(
                        "routed write for {} has no metadata shard assignment",
                        request.object_id
                    ),
                });
            };
            shard = refreshed;
        }
        if shard.primary_node_id != self.node_id {
            return Err(DfsError::StaleOwnerEpoch {
                file_system_id: object.file_system_id.clone(),
                local_node_id: self.node_id.clone(),
                primary_node_id: shard.primary_node_id,
                epoch: shard.epoch,
            });
        }
        if exclusive_create {
            let path_key = crate::posix_path::normalized_posix_path_key(&object.path)
                .unwrap_or_else(|| object.path.clone());
            let metadata_key = crate::namespace::metadata_path(&path_key);
            let memory_object = self
                .metadata
                .object_by_normalized_posix_path(&object.file_system_id, &path_key)
                .cloned();
            let backend_object = self
                .metadata_backend
                .as_ref()
                .map(|backend| backend.get_object_by_path(&object.file_system_id, &metadata_key))
                .transpose()?
                .flatten();
            let current = match (memory_object.as_ref(), backend_object.as_ref()) {
                (Some(memory_object), Some(backend_object)) => {
                    Some(self.resolve_authoritative_object_metadata(
                        &backend_object.object_id,
                        Some(memory_object),
                        Some(backend_object),
                    )?)
                }
                (_, Some(backend_object)) => Some(backend_object.clone()),
                (Some(memory_object), None) => Some(memory_object.clone()),
                (None, None) => None,
            };
            if let Some(current) = current
                && current.kind != DfsObjectKind::Tombstone
            {
                return Err(DfsError::AlreadyExists { path: current.path });
            }
        }
        let mut object = object.clone();
        if object.content_hash.is_none() {
            let parent = self.ensure_routed_create_parent_current(&object)?;
            let attrs = DfsPosixCreateAttrs::new(object.mode, object.uid, object.gid);
            let attrs = parent.map_or(attrs, |parent| {
                crate::posix_object::inherit_posix_create_attrs(
                    attrs,
                    object.kind == DfsObjectKind::Directory,
                    parent.mode,
                    parent.gid,
                )
            });
            crate::posix_object::apply_posix_create_attrs(&mut object, attrs);
        }
        if exclusive_create {
            object.last_modified_by_event_id = exclusive_create_event_id;
        }
        self.add_transient_object_metadata(object)?;
        Ok(true)
    }

    fn ensure_routed_create_parent_current(
        &self,
        object: &DfsObjectMetadata,
    ) -> Result<Option<DfsObjectMetadata>, DfsError> {
        let Some(parent_path) = crate::posix_path::parent_posix_path(&object.path) else {
            return Ok(None);
        };
        let parent_key = crate::posix_path::normalized_posix_path_key(&parent_path)
            .unwrap_or_else(|| parent_path.clone());
        let metadata_key = crate::namespace::metadata_path(&parent_key);
        if let Some(tombstone) = self
            .tombstones_by_metadata_path
            .get(&(object.file_system_id.clone(), metadata_key.clone()))
            && tombstone.kind == DfsObjectKind::Tombstone
        {
            return Err(DfsError::Distributed {
                message: format!("parent directory does not exist: {parent_path}"),
            });
        }
        let memory_parent = self
            .metadata
            .object_by_normalized_posix_path(&object.file_system_id, &parent_key)
            .cloned();
        let backend_parent = self
            .metadata_backend
            .as_ref()
            .map(|backend| backend.get_object_by_path(&object.file_system_id, &metadata_key))
            .transpose()?
            .flatten();
        let parent = match (memory_parent.as_ref(), backend_parent.as_ref()) {
            (Some(memory_parent), Some(backend_parent)) => {
                Some(self.resolve_authoritative_object_metadata(
                    &backend_parent.object_id,
                    Some(memory_parent),
                    Some(backend_parent),
                )?)
            }
            (_, Some(backend_parent)) => Some(backend_parent.clone()),
            (Some(memory_parent), None) => Some(memory_parent.clone()),
            (None, None) => None,
        };
        match parent {
            Some(parent) if parent.kind == DfsObjectKind::Directory => Ok(Some(parent)),
            Some(parent) if parent.kind == DfsObjectKind::Tombstone => Err(DfsError::Distributed {
                message: format!("parent directory does not exist: {parent_path}"),
            }),
            Some(parent) => Err(DfsError::Distributed {
                message: format!(
                    "parent path {parent_path} is not a directory for routed create {}: {:?}",
                    object.path, parent.kind
                ),
            }),
            None => Err(DfsError::Distributed {
                message: format!("parent directory does not exist: {parent_path}"),
            }),
        }
    }

    fn touch_routed_create_parent_directory(&mut self, path: &str) {
        let _ = self.touch_parent_directory_modified(path);
    }

    pub(crate) fn drain_deferred_routed_create_parent_touches(&mut self) {
        if self.deferred_routed_create_parent_touches.is_empty() {
            return;
        }
        let paths = self
            .deferred_routed_create_parent_touches
            .iter()
            .take(DFS_DEFERRED_PARENT_TOUCH_DRAIN_LIMIT)
            .cloned()
            .collect::<Vec<_>>();
        for path in paths {
            self.deferred_routed_create_parent_touches.remove(&path);
            self.touch_routed_create_parent_directory(&path);
        }
    }
}
