//! Incoming DFS data-plane dispatch.
//!
//! This module owns the synchronous inbound message loop and routes each
//! network message to its functional service module. Blob transfer, routed
//! writes, routed namespace operations, repair, reads, and metadata exchange
//! handlers live with their respective service concerns.

use std::time::{Duration, Instant};

use sinomo_core::node::NodeId;

use crate::*;

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub fn process_incoming(&mut self) -> Result<Vec<NetworkedDfsServiceEvent>, DfsError> {
        self.process_incoming_with_options(true)
    }

    pub fn process_incoming_without_deferred_parent_touches(
        &mut self,
    ) -> Result<Vec<NetworkedDfsServiceEvent>, DfsError> {
        self.process_incoming_with_options(false)
    }

    pub fn process_incoming_until_pending_routed_writes_ready(
        &mut self,
        timeout: Duration,
    ) -> Result<Vec<NetworkedDfsServiceEvent>, DfsError> {
        let pending_request_ids = self
            .writes
            .pending_routed_writes
            .iter()
            .filter(|(request_id, pending)| *request_id == &pending.canonical_request_id)
            .map(|(request_id, _)| request_id.clone())
            .collect::<Vec<_>>();
        if pending_request_ids.is_empty() {
            return self.process_incoming();
        }
        let started = Instant::now();
        let mut events = Vec::new();
        loop {
            match self.process_incoming() {
                Ok(incoming) => events.extend(incoming),
                Err(error) => {
                    for request_id in &pending_request_ids {
                        self.compact_pending_routed_write(request_id);
                    }
                    return Err(error);
                }
            }
            let mut all_ready = true;
            for request_id in &pending_request_ids {
                if !self.writes.pending_routed_writes.contains_key(request_id) {
                    continue;
                }
                if matches!(
                    self.take_write_outcome(request_id),
                    Ok(NetworkedDfsWriteOutcome::Pending { .. })
                ) {
                    all_ready = false;
                }
            }
            if all_ready {
                return Ok(events);
            }
            let elapsed = started.elapsed();
            if elapsed >= timeout {
                for request_id in &pending_request_ids {
                    self.compact_pending_routed_write(request_id);
                }
                return Err(DfsError::Network {
                    message: format!(
                        "DFS routed write responses did not arrive within {}ms",
                        timeout.as_millis()
                    ),
                });
            }
            let wait = Duration::from_millis(10).min(timeout.saturating_sub(elapsed));
            if let Err(error) = self.transport.wait_for_incoming(wait) {
                for request_id in &pending_request_ids {
                    self.compact_pending_routed_write(request_id);
                }
                return Err(DfsError::from_network(error));
            }
        }
    }

    pub(crate) fn process_incoming_until_quiescent(
        &mut self,
    ) -> Result<Vec<NetworkedDfsServiceEvent>, DfsError> {
        let mut events = Vec::new();
        for _ in 0..DFS_DATA_PLANE_MESSAGE_DRAIN_MAX_PASSES {
            let batch = self.process_incoming()?;
            events.extend(batch);
            if self
                .transport
                .pending_incoming_count()
                .map_err(DfsError::from_network)?
                == Some(0)
            {
                return Ok(events);
            }
        }
        Err(DfsError::Network {
            message: format!(
                "DFS data-plane maintenance did not quiesce after {DFS_DATA_PLANE_MESSAGE_DRAIN_MAX_PASSES} passes"
            ),
        })
    }

    pub(crate) fn process_incoming_with_options(
        &mut self,
        drain_deferred_parent_touches: bool,
    ) -> Result<Vec<NetworkedDfsServiceEvent>, DfsError> {
        self.prune_idle_transfer_state();
        if self
            .transport
            .pending_incoming_count()
            .map_err(DfsError::from_network)?
            == Some(0)
        {
            let mut events = Vec::new();
            self.flush_completed_pending_remote_write_responses(&mut events)?;
            if drain_deferred_parent_touches {
                self.drain_deferred_routed_create_parent_touches();
            }
            self.prune_expired_pending_remote_reads();
            self.last_events = retained_networked_dfs_service_events(&events);
            return Ok(events);
        }
        let inbound_messages = self
            .transport
            .drain_incoming_messages()
            .map_err(DfsError::from_network)?;
        let events = self.process_routed_messages_with_options(
            inbound_messages,
            drain_deferred_parent_touches,
        )?;
        Ok(events)
    }

    pub(crate) fn process_routed_messages_with_options(
        &mut self,
        routed_messages: Vec<DfsRoutedMessage>,
        drain_deferred_parent_touches: bool,
    ) -> Result<Vec<NetworkedDfsServiceEvent>, DfsError> {
        let mut events = Vec::new();

        for message in routed_messages {
            self.process_routed_message_with_options(
                message,
                drain_deferred_parent_touches,
                &mut events,
            )?;
        }

        self.prune_expired_pending_remote_reads();
        self.last_events = retained_networked_dfs_service_events(&events);
        Ok(events)
    }

    pub(crate) fn process_routed_message_with_options(
        &mut self,
        routed_message: DfsRoutedMessage,
        drain_deferred_parent_touches: bool,
        events: &mut Vec<NetworkedDfsServiceEvent>,
    ) -> Result<(), DfsError> {
        if routed_message.target_node_id != self.node_id {
            return Err(DfsError::Network {
                message: format!(
                    "dfs message target {} does not match local node {}",
                    routed_message.target_node_id, self.node_id
                ),
            });
        }
        self.process_network_message_with_options(
            routed_message.source_node_id,
            routed_message.message,
            drain_deferred_parent_touches,
            events,
        )
    }

    fn process_network_message_with_options(
        &mut self,
        source_node_id: NodeId,
        message: DfsNetworkMessage,
        drain_deferred_parent_touches: bool,
        events: &mut Vec<NetworkedDfsServiceEvent>,
    ) -> Result<(), DfsError> {
        let trace_message_label = dfs_network_message_trace_label(&message);

        match message {
            DfsNetworkMessage::StageBlob(command) => {
                events.push(self.handle_stage_blob(source_node_id, command)?);
            }
            DfsNetworkMessage::StageRetainedBlob(command) => {
                events.push(self.handle_stage_retained_blob(source_node_id, command, None)?);
            }
            DfsNetworkMessage::RepairRetainedBlob(command) => {
                events.push(self.handle_repair_retained_blob(source_node_id, command, None)?);
            }
            DfsNetworkMessage::StagePatch(command) => {
                events.push(self.handle_stage_patch(source_node_id, command)?);
            }
            DfsNetworkMessage::StageBlobResponse(response) => {
                if let Some(event) = self.handle_stage_blob_response(source_node_id, response)? {
                    events.push(event);
                }
            }
            DfsNetworkMessage::CommitVersion(command) => {
                events.push(self.handle_commit_version(source_node_id, command)?);
            }
            DfsNetworkMessage::RepairReplica(command) => {
                events.push(self.handle_repair_replica(source_node_id, command)?);
            }
            DfsNetworkMessage::RepairReplicaResponse(response) => {
                events.push(self.handle_repair_replica_response(source_node_id, response)?);
            }
            DfsNetworkMessage::FetchBlob(command) => {
                events.push(self.handle_fetch_blob(source_node_id, command)?);
            }
            DfsNetworkMessage::FetchBlobResponse(response) => {
                events.push(self.handle_fetch_blob_response(source_node_id.clone(), response)?);
            }
            DfsNetworkMessage::ReadRequest(request) => {
                events.push(self.handle_networked_read_request(source_node_id, request)?);
            }
            DfsNetworkMessage::ReadResponse(response) => {
                events.push(self.handle_networked_read_response(source_node_id, response)?);
            }
            DfsNetworkMessage::BulkReadRequest(request) => {
                events.push(self.handle_networked_bulk_read_request(source_node_id, request)?);
            }
            DfsNetworkMessage::BulkReadResponse(response) => {
                events.push(self.handle_networked_bulk_read_response(source_node_id, response)?);
            }
            DfsNetworkMessage::DistributedWriteRequest(mut command) => {
                command.require_publication_mode(false)?;
                // The additive command field is internal transfer state, not a
                // wire-level capability: peers predating it would ignore it.
                // Only the distinct message variant may request fail-closed
                // exclusive creation.
                command.exclusive_create = false;
                events.push(self.handle_distributed_write_request(source_node_id, command)?);
            }
            DfsNetworkMessage::ExclusiveCreateRequest(mut command) => {
                command.require_publication_mode(false)?;
                command.exclusive_create = true;
                events.push(self.handle_distributed_write_request(source_node_id, command)?);
            }
            DfsNetworkMessage::RetainedWriteRequest(command) => {
                command.require_publication_mode(true)?;
                events.push(self.handle_distributed_write_request(source_node_id, command)?);
            }
            DfsNetworkMessage::DistributedWriteResponse(response) => {
                events.push(self.handle_distributed_write_response(source_node_id, response)?);
            }
            DfsNetworkMessage::StableWriteReconciliationRequest(request) => {
                events.push(
                    self.handle_stable_write_reconciliation_request(source_node_id, request)?,
                );
            }
            DfsNetworkMessage::StableWriteReconciliationResponse(response) => {
                events.push(
                    self.handle_stable_write_reconciliation_response(source_node_id, response)?,
                );
            }
            DfsNetworkMessage::NamespaceRequest(command) => {
                events.push(self.handle_namespace_request(source_node_id, command)?);
            }
            DfsNetworkMessage::NamespaceResponse(response) => {
                events.push(self.handle_namespace_response(source_node_id, response)?);
            }
            DfsNetworkMessage::StableNamespaceReconciliationRequest(request) => {
                events.push(
                    self.handle_stable_namespace_reconciliation_request(source_node_id, request)?,
                );
            }
            DfsNetworkMessage::StableNamespaceReconciliationResponse(response) => {
                events.push(
                    self.handle_stable_namespace_reconciliation_response(source_node_id, response)?,
                );
            }
            DfsNetworkMessage::MetadataJournalBatch(command) => {
                events.push(self.handle_metadata_journal_batch(source_node_id, command)?);
            }
            DfsNetworkMessage::MetadataJournalBatchResponse(response) => {
                if let Some(event) =
                    self.handle_metadata_journal_batch_response(source_node_id, response)?
                {
                    events.push(event);
                }
            }
            DfsNetworkMessage::MetadataSnapshotInstall(command) => {
                events.push(self.handle_metadata_snapshot_install(source_node_id, command)?);
            }
            DfsNetworkMessage::MetadataSnapshotInstallResponse(response) => {
                if let Some(event) =
                    self.handle_metadata_snapshot_install_response(source_node_id, response)?
                {
                    events.push(event);
                }
            }
            DfsNetworkMessage::ContentRootCatalogPageRequest(request) => {
                events
                    .push(self.handle_content_root_catalog_page_request(source_node_id, request)?);
            }
            DfsNetworkMessage::ContentRootCatalogPageResponse(page) => {
                events.push(self.handle_content_root_catalog_page_response(source_node_id, page)?);
            }
            DfsNetworkMessage::CommitVersionResponse(_)
            | DfsNetworkMessage::WriteRequest(_)
            | DfsNetworkMessage::WriteResponse(_) => {}
        }
        let flush_started = Instant::now();
        self.flush_completed_pending_remote_write_responses(events)?;
        let flush_ms = flush_started.elapsed().as_millis();
        if drain_deferred_parent_touches {
            let drain_started = Instant::now();
            let deferred_parent_touches = self.writes.deferred_routed_create_parent_touches.len();
            self.drain_deferred_routed_create_parent_touches();
            let drain_ms = drain_started.elapsed().as_millis();
            if flush_ms > 0 || drain_ms > 0 || deferred_parent_touches > 0 {
                dfs_perf_trace(format_args!(
                    "inbound-post-message node={} message={} flush_ms={} deferred_parent_touches={} drain_ms={}",
                    self.node_id, trace_message_label, flush_ms, deferred_parent_touches, drain_ms
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn prune_expired_pending_remote_reads(&mut self) {
        if self.reads.pending_reads.is_empty() {
            return;
        }
        let now = Instant::now();
        let expired_request_ids = self
            .reads
            .pending_reads
            .iter()
            .filter_map(|(request_id, pending)| {
                let prune_after = pending
                    .next_retry_at
                    .checked_add(DFS_PENDING_TRANSFER_STALE_AFTER)
                    .unwrap_or(pending.next_retry_at);
                (prune_after <= now).then_some(request_id.clone())
            })
            .collect::<Vec<_>>();
        for request_id in expired_request_ids {
            self.reads.pending_reads.remove(&request_id);
        }
    }
}
