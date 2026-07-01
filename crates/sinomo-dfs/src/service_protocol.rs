//! Incoming DFS data-plane dispatch and routed coordination handlers.
//!
//! This module owns the synchronous inbound message loop, distributed-write
//! request/response routing, namespace coordination, metadata journal and
//! snapshot message handling, and small helpers required to settle routed
//! operations. Lower-level blob stage/fetch/read/repair transfer handlers live
//! with the remaining transfer protocol code.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use sinomo_node::NodeId;

use crate::*;

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub fn process_incoming(&mut self) -> Result<Vec<NetworkedDfsServiceEvent>, DfsError> {
        self.process_incoming_with_options(true)
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
        self.prune_idle_chunk_transfers();
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
            DfsNetworkMessage::StageBlobChunk(command) => {
                if let Some(event) = self.handle_stage_blob_chunk(source_node_id, command)? {
                    events.push(event);
                }
            }
            DfsNetworkMessage::StagePatch(command) => {
                events.push(self.handle_stage_patch(source_node_id, command)?);
            }
            DfsNetworkMessage::StagePatchChunk(command) => {
                if let Some(event) = self.handle_stage_patch_chunk(source_node_id, command)? {
                    events.push(event);
                }
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
            DfsNetworkMessage::RepairReplicaChunk(command) => {
                if let Some(event) = self.handle_repair_replica_chunk(source_node_id, command)? {
                    events.push(event);
                }
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
            DfsNetworkMessage::FetchBlobResponseChunk(chunk) => {
                if let Some(event) =
                    self.handle_fetch_blob_response_chunk(source_node_id.clone(), chunk)?
                {
                    events.push(event);
                }
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
            DfsNetworkMessage::ReadResponseChunk(chunk) => {
                if let Some(event) =
                    self.handle_networked_read_response_chunk(source_node_id, chunk)?
                {
                    events.push(event);
                }
            }
            DfsNetworkMessage::DistributedWriteRequest(command) => {
                events.push(self.handle_distributed_write_request(source_node_id, command)?);
            }
            DfsNetworkMessage::DistributedWriteResponse(response) => {
                events.push(self.handle_distributed_write_response(source_node_id, response)?);
            }
            DfsNetworkMessage::NamespaceRequest(command) => {
                events.push(self.handle_namespace_request(source_node_id, command)?);
            }
            DfsNetworkMessage::NamespaceResponse(response) => {
                events.push(self.handle_namespace_response(source_node_id, response)?);
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
            DfsNetworkMessage::CommitVersionResponse(_)
            | DfsNetworkMessage::WriteRequest(_)
            | DfsNetworkMessage::WriteResponse(_) => {}
        }
        let flush_started = Instant::now();
        self.flush_completed_pending_remote_write_responses(events)?;
        let flush_ms = flush_started.elapsed().as_millis();
        if drain_deferred_parent_touches {
            let drain_started = Instant::now();
            let deferred_parent_touches = self.deferred_routed_create_parent_touches.len();
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
        if self.pending_reads.is_empty() {
            return;
        }
        let now = Instant::now();
        let expired_request_ids = self
            .pending_reads
            .iter()
            .filter_map(|(request_id, pending)| {
                let prune_after = pending
                    .next_retry_at
                    .checked_add(DFS_CHUNK_TRANSFER_IDLE_TIMEOUT)
                    .unwrap_or(pending.next_retry_at);
                (prune_after <= now).then_some(request_id.clone())
            })
            .collect::<Vec<_>>();
        for request_id in expired_request_ids {
            self.pending_reads.remove(&request_id);
            self.incoming_read_transfers.remove(&request_id);
            self.incoming_read_memory_transfers.remove(&request_id);
        }
    }

    fn synced_namespace_object_for_operation(
        &self,
        file_system_id: &DfsFileSystemId,
        operation: &DfsNamespaceOperation,
    ) -> Result<Option<DfsObjectMetadata>, DfsError> {
        Ok(self
            .synced_namespace_objects_for_operation(file_system_id, operation)?
            .into_iter()
            .next())
    }

    fn synced_mkdir_all_paths(path: &str) -> Vec<String> {
        let Some(normalized) = posix::normalized_posix_path_key(path) else {
            return Vec::new();
        };
        if normalized == "/" {
            return vec![normalized];
        }
        let mut paths = Vec::new();
        let mut current = String::new();
        for part in normalized.trim_matches('/').split('/') {
            current.push('/');
            current.push_str(part);
            paths.push(current.clone());
        }
        paths
    }

    fn synced_namespace_objects_for_operation(
        &self,
        file_system_id: &DfsFileSystemId,
        operation: &DfsNamespaceOperation,
    ) -> Result<Vec<DfsObjectMetadata>, DfsError> {
        let paths: Vec<String> = match operation {
            DfsNamespaceOperation::SyncPath { path }
            | DfsNamespaceOperation::LookupPath { path }
            | DfsNamespaceOperation::Mkdir { path } => {
                posix::normalized_posix_path_key(path).into_iter().collect()
            }
            DfsNamespaceOperation::MkdirAll { path } => Self::synced_mkdir_all_paths(path),
            DfsNamespaceOperation::LookupPaths { paths } => paths
                .iter()
                .filter_map(|path| posix::normalized_posix_path_key(path))
                .collect(),
            _ => return Ok(Vec::new()),
        };
        let mut objects = Vec::new();
        for path in &paths {
            if let Some(object) = self.synced_namespace_object_for_path(file_system_id, path)? {
                objects.push(object);
            }
        }
        Ok(objects)
    }

    fn synced_namespace_object_for_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsObjectMetadata>, DfsError> {
        let normalized_path = match posix::normalized_posix_path_key(path) {
            Some(path) => path,
            None => return Ok(None),
        };
        if let Some(tombstone) = self
            .tombstones_by_metadata_path
            .get(&(file_system_id.clone(), metadata_path(&normalized_path)))
        {
            return Ok(Some(tombstone.clone()));
        }
        let memory_object = self
            .metadata
            .object_by_normalized_posix_path(file_system_id, &normalized_path)
            .cloned();
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(memory_object);
        };
        let backend_object = backend.get_object_by_path(file_system_id, &metadata_path(path))?;
        let object = match (memory_object.as_ref(), backend_object.as_ref()) {
            (Some(memory_object), Some(backend_object)) => {
                Some(self.resolve_authoritative_object_metadata(
                    &backend_object.object_id,
                    Some(memory_object),
                    Some(backend_object),
                )?)
            }
            (Some(memory_object), None) => Some(memory_object.clone()),
            (_, Some(backend_object)) => Some(backend_object.clone()),
            (None, None) => None,
        };
        object
            .map(|object| self.object_metadata_with_receipt_locations(object))
            .transpose()
    }

    /// Serves a namespace mutation routed from a replica. The local node must
    /// be the metadata-shard primary for every shard the operation touches;
    /// it executes the operation authoritatively and answers with journal
    /// catch-up batches so the requester reads its own namespace write.
    pub(crate) fn handle_namespace_request(
        &mut self,
        requester_node_id: NodeId,
        command: DfsNamespaceCommand,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let request_id = command.request_id.clone();
        let trace_started = Instant::now();
        let trace_operation = dfs_namespace_operation_trace_label(&command.operation);
        let trace_involved_paths = command.operation.involved_paths().len();
        let mut trace_affected_ms = 0;
        let mut trace_affected_shards = 0;
        let mut trace_execute_ms = 0;
        let mut affected_shard_ids: Vec<DfsMetadataShardId> = Vec::new();
        // The authority the requester should retry against when this node is
        // not the shard primary (the requester routed on a stale shard view).
        let mut authority_redirect: Option<NodeId> = None;
        let result = if requester_node_id != command.requesting_node_id {
            DfsNamespaceResult::Rejected {
                reason: format!(
                    "namespace request requester mismatch: frame source {}, payload requester {}",
                    requester_node_id, command.requesting_node_id
                ),
                posix_error: None,
                current_primary_node_id: None,
            }
        } else if let Some(reason) = self.adoption_source_authority_rejection(
            &command.file_system_id,
            &command.operation,
            &requester_node_id,
        )? {
            DfsNamespaceResult::Rejected {
                reason,
                posix_error: None,
                current_primary_node_id: None,
            }
        } else {
            let affected_started = Instant::now();
            match self.affected_namespace_shards(&command.file_system_id, &command.operation) {
                Err(error) => {
                    trace_affected_ms = affected_started.elapsed().as_millis();
                    DfsNamespaceResult::Rejected {
                        reason: error.to_string(),
                        posix_error: None,
                        current_primary_node_id: None,
                    }
                }
                Ok(shards) => {
                    trace_affected_ms = affected_started.elapsed().as_millis();
                    trace_affected_shards = shards.len();
                    // Response batches may only carry shards this node is
                    // primary for; foreign halves of coordinated operations
                    // are synced from their own primaries.
                    affected_shard_ids = shards
                        .iter()
                        .filter(|shard| shard.primary_node_id == self.node_id)
                        .map(|shard| DfsMetadataShardId::new(shard.shard_id.clone()))
                        .collect();
                    let foreign_shard = shards
                        .iter()
                        .find(|shard| shard.primary_node_id != self.node_id)
                        .cloned();
                    let execute_started = Instant::now();
                    let outcome = match (&foreign_shard, &command.operation) {
                        (None, _) => self.execute_namespace_operation_local(&command.operation),
                        // Operations spanning another primary's shard run
                        // through the cross-shard coordinator when this node
                        // owns the source side.
                        (Some(_), DfsNamespaceOperation::Rename { from, to }) => {
                            let from = from.clone();
                            let to = to.clone();
                            self.namespace_coordinator_dispatch_rename(
                                &command.file_system_id,
                                &from,
                                &to,
                            )
                        }
                        (
                            Some(_),
                            DfsNamespaceOperation::Hardlink {
                                existing_path,
                                link_path,
                            },
                        ) => {
                            let existing_path = existing_path.clone();
                            let link_path = link_path.clone();
                            self.namespace_coordinator_dispatch_hardlink(
                                &command.file_system_id,
                                &existing_path,
                                &link_path,
                            )
                        }
                        (Some(shard), _) => {
                            authority_redirect = Some(shard.primary_node_id.clone());
                            Err(crate::posix::DfsPosixError::Dfs(DfsError::Distributed {
                                message: format!(
                                    "node {} is not the metadata primary for shard {} (primary {}, epoch {})",
                                    self.node_id,
                                    shard.shard_id,
                                    shard.primary_node_id,
                                    shard.epoch
                                ),
                            }))
                        }
                    };
                    trace_execute_ms = execute_started.elapsed().as_millis();
                    match outcome {
                        Ok(()) => DfsNamespaceResult::Applied,
                        Err(error) => DfsNamespaceResult::Rejected {
                            current_primary_node_id: authority_redirect
                                .or_else(|| namespace_rejection_current_primary(&error)),
                            posix_error: crate::posix::namespace_posix_error_for(&error),
                            reason: error.to_string(),
                        },
                    }
                }
            }
        };
        let journal_started = Instant::now();
        let metadata_journal_batches = if command.include_metadata_journal_batches
            && matches!(result, DfsNamespaceResult::Applied)
            && !matches!(
                command.operation,
                DfsNamespaceOperation::LookupPath { .. }
                    | DfsNamespaceOperation::LookupPaths { .. }
            ) {
            let mut batches = Vec::new();
            for shard_id in &affected_shard_ids {
                let after_sequence = command
                    .metadata_journal_after_sequences
                    .iter()
                    .find(|cursor| &cursor.shard_id == shard_id)
                    .map(|cursor| cursor.after_sequence)
                    .unwrap_or(0);
                batches.extend(
                    self.metadata_journal_batches_for_shard_since(shard_id, after_sequence)?,
                );
            }
            batches
        } else {
            Vec::new()
        };
        let trace_journal_ms = journal_started.elapsed().as_millis();
        let trace_journal_batches = metadata_journal_batches.len();
        let sync_started = Instant::now();
        let synced_path_objects = if matches!(result, DfsNamespaceResult::Applied) {
            self.synced_namespace_objects_for_operation(
                &command.file_system_id,
                &command.operation,
            )?
        } else {
            Vec::new()
        };
        let synced_path_object = match &command.operation {
            DfsNamespaceOperation::SyncPath { .. } | DfsNamespaceOperation::LookupPath { .. } => {
                synced_path_objects.first().cloned()
            }
            _ => None,
        };
        let trace_sync_ms = sync_started.elapsed().as_millis();
        let trace_synced_path_objects = synced_path_objects.len();
        let send_started = Instant::now();
        self.send_dfs_message_if_routable(
            &requester_node_id,
            DfsNetworkMessage::NamespaceResponse(DfsNamespaceResponse {
                request_id: request_id.clone(),
                result: result.clone(),
                synced_path_object,
                synced_path_objects,
                metadata_journal_batches,
            }),
        )?;
        let trace_send_ms = send_started.elapsed().as_millis();
        dfs_perf_trace(format_args!(
            "namespace-request node={} requester={} request={} operation={} involved_paths={} affected_shards={} result={} affected_ms={} execute_ms={} journal_batches={} journal_ms={} synced_path_objects={} sync_ms={} response_send_ms={} total_ms={}",
            self.node_id,
            requester_node_id,
            request_id,
            trace_operation,
            trace_involved_paths,
            trace_affected_shards,
            match &result {
                DfsNamespaceResult::Applied => "applied",
                DfsNamespaceResult::Rejected { .. } => "rejected",
            },
            trace_affected_ms,
            trace_execute_ms,
            trace_journal_batches,
            trace_journal_ms,
            trace_synced_path_objects,
            trace_sync_ms,
            trace_send_ms,
            trace_started.elapsed().as_millis()
        ));
        let mutates_metadata = !matches!(
            command.operation,
            DfsNamespaceOperation::LookupPath { .. }
                | DfsNamespaceOperation::LookupPaths { .. }
                | DfsNamespaceOperation::SyncPath { .. }
        );
        Ok(NetworkedDfsServiceEvent::RemoteNamespaceServed {
            request_id,
            requester_node_id,
            result,
            mutates_metadata,
        })
    }

    /// Adoption operations install records minted by another shard authority;
    /// only the source path's configured primary may send them. Returns the
    /// rejection reason for an unauthorized sender, `None` when authorized or
    /// for non-adoption operations.
    fn adoption_source_authority_rejection(
        &mut self,
        file_system_id: &DfsFileSystemId,
        operation: &DfsNamespaceOperation,
        requester_node_id: &NodeId,
    ) -> Result<Option<String>, DfsError> {
        let source_path = match operation {
            DfsNamespaceOperation::AdoptObjectAtPath { source_path, .. }
            | DfsNamespaceOperation::AdoptHardlinkAlias { source_path, .. } => source_path,
            _ => return Ok(None),
        };
        if self.control_plane.is_none() {
            return Ok(None);
        }
        let Some(normalized) = crate::posix::normalized_posix_path_key(source_path) else {
            return Ok(Some(format!(
                "adoption carries an invalid source path: {source_path}"
            )));
        };
        let kv = self
            .control_plane
            .as_mut()
            .expect("control plane checked above");
        let Some(shard) = DfsControlPlaneStore::new(kv.as_mut())
            .select_metadata_shard_for_path_strict_default_sifs_split(file_system_id, &normalized)
            .map_err(DfsError::from)?
        else {
            return Ok(Some(format!(
                "adoption source path {normalized} has no metadata shard assignment"
            )));
        };
        if &shard.primary_node_id != requester_node_id {
            return Ok(Some(format!(
                "adoption sender {requester_node_id} is not the primary of source shard {} (primary {}, epoch {})",
                shard.shard_id, shard.primary_node_id, shard.epoch
            )));
        }
        Ok(None)
    }

    /// The metadata shards a namespace operation can mutate: the shards of
    /// every involved path plus their parents (parent records share the
    /// child's prefix shard except at shard-boundary anchors).
    fn affected_namespace_shards(
        &mut self,
        file_system_id: &DfsFileSystemId,
        operation: &DfsNamespaceOperation,
    ) -> Result<Vec<DfsMetadataShardConfig>, DfsError> {
        if let DfsNamespaceOperation::ApplyTransaction { transaction } = operation {
            return self.affected_namespace_transaction_shards(file_system_id, transaction);
        }
        let mut shards: Vec<DfsMetadataShardConfig> = Vec::new();
        for path in operation.involved_paths() {
            for shard in self.namespace_shards_for_path(file_system_id, path)? {
                if !shards
                    .iter()
                    .any(|existing| existing.shard_id == shard.shard_id)
                {
                    shards.push(shard);
                }
            }
        }
        Ok(shards)
    }

    fn affected_namespace_transaction_shards(
        &mut self,
        file_system_id: &DfsFileSystemId,
        transaction: &DfsNamespaceTransaction,
    ) -> Result<Vec<DfsMetadataShardConfig>, DfsError> {
        if transaction.objects.is_empty() || self.control_plane.is_none() {
            return Ok(Vec::new());
        }
        let shards = {
            let kv = self
                .control_plane
                .as_mut()
                .expect("control plane checked before namespace transaction shard selection");
            DfsControlPlaneStore::new(kv.as_mut())
                .list_metadata_shards(file_system_id)
                .map_err(DfsError::from)?
        };
        let mut candidate_paths = BTreeSet::new();
        for object in &transaction.objects {
            let normalized =
                crate::posix::normalized_posix_path_key(&object.path).ok_or_else(|| {
                    DfsError::Distributed {
                        message: format!("invalid namespace transaction path: {}", object.path),
                    }
                })?;
            if let Some(parent) = crate::posix::parent_posix_path(&normalized) {
                candidate_paths.insert(parent);
            }
            candidate_paths.insert(normalized);
        }

        let mut affected = Vec::new();
        for path in candidate_paths {
            let Some(shard) =
                crate::control_plane::select_metadata_shard_config_for_path_strict_default_sifs_split(
                    file_system_id,
                    &shards,
                    &path,
                )
                .map_err(DfsError::from)?
            else {
                continue;
            };
            if !affected
                .iter()
                .any(|existing: &DfsMetadataShardConfig| existing.shard_id == shard.shard_id)
            {
                affected.push(shard);
            }
        }
        Ok(affected)
    }

    /// The metadata shards covering a path and its parent directory.
    pub(crate) fn namespace_shards_for_path(
        &mut self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Vec<DfsMetadataShardConfig>, DfsError> {
        if self.control_plane.is_none() {
            return Ok(Vec::new());
        }
        let normalized =
            crate::posix::normalized_posix_path_key(path).ok_or_else(|| DfsError::Distributed {
                message: format!("invalid namespace operation path: {path}"),
            })?;
        let mut candidates = vec![normalized.clone()];
        if let Some(parent) = crate::posix::parent_posix_path(&normalized) {
            candidates.push(parent);
        }
        let mut shards: Vec<DfsMetadataShardConfig> = Vec::new();
        for candidate in candidates {
            let kv = self
                .control_plane
                .as_mut()
                .expect("control plane checked above");
            let Some(shard) = DfsControlPlaneStore::new(kv.as_mut())
                .select_metadata_shard_for_path_strict_default_sifs_split(
                    file_system_id,
                    &candidate,
                )
                .map_err(DfsError::from)?
            else {
                continue;
            };
            if !shards
                .iter()
                .any(|existing| existing.shard_id == shard.shard_id)
            {
                shards.push(shard);
            }
        }
        Ok(shards)
    }

    /// Validates the source side of a coordinated rename and resolves the
    /// single foreign destination primary before handing off to the
    /// cross-shard coordinator.
    pub(crate) fn namespace_coordinator_dispatch_rename(
        &mut self,
        file_system_id: &DfsFileSystemId,
        from: &str,
        to: &str,
    ) -> crate::posix::Result<()> {
        if self.routed_namespace_settle_depth >= DFS_MAX_NESTED_NAMESPACE_COORDINATION_DEPTH {
            return Err(crate::posix::DfsPosixError::Dfs(DfsError::Busy {
                path: from.to_string(),
            }));
        }
        if self.resume_running_cross_shard_namespace_job_for_paths(
            file_system_id,
            from,
            to,
            &[
                DfsNamespaceJobKind::RenameObjectAcrossShardPrimaries,
                DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries,
            ],
        )? {
            return Ok(());
        }
        let destination_primary =
            self.namespace_coordinator_foreign_primary(file_system_id, from, to)?;
        let Some(destination_primary) = destination_primary else {
            return self.posix_rename_local(from, to).map(drop);
        };
        self.coordinate_cross_shard_rename(from, to, destination_primary)
    }

    pub(crate) fn namespace_coordinator_dispatch_hardlink(
        &mut self,
        file_system_id: &DfsFileSystemId,
        existing_path: &str,
        link_path: &str,
    ) -> crate::posix::Result<()> {
        if self.routed_namespace_settle_depth >= DFS_MAX_NESTED_NAMESPACE_COORDINATION_DEPTH {
            return Err(crate::posix::DfsPosixError::Dfs(DfsError::Busy {
                path: existing_path.to_string(),
            }));
        }
        if self.resume_running_cross_shard_namespace_job_for_paths(
            file_system_id,
            existing_path,
            link_path,
            &[DfsNamespaceJobKind::HardlinkAcrossShardPrimaries],
        )? {
            return Ok(());
        }
        let destination_primary =
            self.namespace_coordinator_foreign_primary(file_system_id, existing_path, link_path)?;
        let Some(destination_primary) = destination_primary else {
            return self
                .posix_hardlink_local(existing_path, link_path)
                .map(drop);
        };
        self.coordinate_cross_shard_hardlink(existing_path, link_path, destination_primary)
    }

    /// For a two-path coordinated operation: every shard on the source side
    /// must be owned by this node, and the destination side must resolve to
    /// at most one foreign primary.
    fn namespace_coordinator_foreign_primary(
        &mut self,
        file_system_id: &DfsFileSystemId,
        source_path: &str,
        destination_path: &str,
    ) -> crate::posix::Result<Option<NodeId>> {
        let source_shards = self.namespace_shards_for_path(file_system_id, source_path)?;
        if let Some(shard) = source_shards
            .iter()
            .find(|shard| shard.primary_node_id != self.node_id)
        {
            return Err(crate::posix::DfsPosixError::Dfs(
                DfsError::StaleOwnerEpoch {
                    file_system_id: file_system_id.clone(),
                    local_node_id: self.node_id.clone(),
                    primary_node_id: shard.primary_node_id.clone(),
                    epoch: shard.epoch,
                },
            ));
        }
        let destination_shards =
            self.namespace_shards_for_path(file_system_id, destination_path)?;
        let mut foreign_primary: Option<NodeId> = None;
        for shard in destination_shards {
            if shard.primary_node_id == self.node_id {
                continue;
            }
            match &foreign_primary {
                Some(existing) if existing != &shard.primary_node_id => {
                    return Err(crate::posix::DfsPosixError::InvalidPath {
                        path: format!(
                            "{destination_path} spans metadata shards with different primaries"
                        ),
                    });
                }
                _ => foreign_primary = Some(shard.primary_node_id),
            }
        }
        Ok(foreign_primary)
    }

    /// Marks a path as participating in an in-flight cross-shard move so
    /// concurrent mutations cannot fork the object between shards. Directory
    /// moves lock their whole subtree: the check matches descendants and
    /// ancestors of the locked path, not just the path itself.
    pub(crate) fn lock_cross_shard_namespace_path(
        &mut self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) {
        let Some(normalized) = crate::posix::normalized_posix_path_key(path) else {
            return;
        };
        self.cross_shard_namespace_locks
            .insert((file_system_id.clone(), normalized));
    }

    pub(crate) fn unlock_cross_shard_namespace_path(
        &mut self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) {
        let Some(normalized) = crate::posix::normalized_posix_path_key(path) else {
            return;
        };
        self.cross_shard_namespace_locks
            .remove(&(file_system_id.clone(), normalized));
    }

    pub(crate) fn ensure_path_not_in_cross_shard_flight(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<(), DfsError> {
        if self.cross_shard_namespace_locks.is_empty() {
            return Ok(());
        }
        let Some(normalized) = crate::posix::normalized_posix_path_key(path) else {
            return Ok(());
        };
        let blocked =
            self.cross_shard_namespace_locks
                .iter()
                .any(|(locked_file_system_id, locked_path)| {
                    locked_file_system_id == file_system_id
                        && (crate::posix::posix_path_is_at_or_under_prefix(
                            &normalized,
                            locked_path,
                        ) || crate::posix::posix_path_is_at_or_under_prefix(
                            locked_path,
                            &normalized,
                        ))
                });
        if blocked {
            return Err(DfsError::Busy {
                path: path.to_string(),
            });
        }
        Ok(())
    }

    pub(crate) fn ensure_object_not_in_cross_shard_flight(
        &self,
        object: &DfsObjectMetadata,
    ) -> Result<(), DfsError> {
        self.ensure_path_not_in_cross_shard_flight(&object.file_system_id, &object.path)
    }

    pub(crate) fn handle_namespace_response(
        &mut self,
        primary_node_id: NodeId,
        response: DfsNamespaceResponse,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        // Late, duplicate, or mismatched responses are discarded; a stray
        // frame must never fail the whole event loop.
        let (replay_journal_batches, apply_synced_path_object) = {
            let Some(pending) = self.pending_routed_namespace_ops.get(&response.request_id) else {
                return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                    request_id: response.request_id,
                    source_node_id: primary_node_id,
                    reason: "namespace response for an untracked request".to_string(),
                });
            };
            if pending.primary_node_id != primary_node_id {
                let reason = format!(
                    "namespace response did not come from primary {}",
                    pending.primary_node_id
                );
                return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                    request_id: response.request_id,
                    source_node_id: primary_node_id,
                    reason,
                });
            }
            (
                pending.replay_journal_batches,
                pending.apply_synced_path_object,
            )
        };
        if replay_journal_batches {
            for batch in &response.metadata_journal_batches {
                if let Err(error) =
                    self.apply_routed_write_metadata_journal_batch(&primary_node_id, batch)
                {
                    let reason = format!("routed namespace journal replay failed: {error}");
                    if let Some(pending) = self
                        .pending_routed_namespace_ops
                        .get_mut(&response.request_id)
                    {
                        pending.result = Some(PendingRoutedNamespaceResult {
                            result: DfsNamespaceResult::Rejected {
                                reason: reason.clone(),
                                posix_error: None,
                                current_primary_node_id: None,
                            },
                            synced_path_object: None,
                            synced_path_objects: Vec::new(),
                        });
                    }
                    return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                        request_id: response.request_id,
                        source_node_id: primary_node_id,
                        reason,
                    });
                }
            }
        }
        if apply_synced_path_object
            && let Some(object) = response.synced_path_object.as_ref()
            && !self.routed_response_object_targets_local_primary(object)?
            && let Err(error) = self.apply_metadata_object_to_service_state(object.clone())
        {
            let reason = format!("routed namespace path sync failed: {error}");
            if let Some(pending) = self
                .pending_routed_namespace_ops
                .get_mut(&response.request_id)
            {
                pending.result = Some(PendingRoutedNamespaceResult {
                    result: DfsNamespaceResult::Rejected {
                        reason: reason.clone(),
                        posix_error: None,
                        current_primary_node_id: None,
                    },
                    synced_path_object: None,
                    synced_path_objects: Vec::new(),
                });
            }
            return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                request_id: response.request_id,
                source_node_id: primary_node_id,
                reason,
            });
        }
        if apply_synced_path_object {
            for object in &response.synced_path_objects {
                if self.routed_response_object_targets_local_primary(object)? {
                    continue;
                }
                if let Err(error) = self.apply_metadata_object_to_service_state(object.clone()) {
                    let reason = format!("routed namespace path sync failed: {error}");
                    if let Some(pending) = self
                        .pending_routed_namespace_ops
                        .get_mut(&response.request_id)
                    {
                        pending.result = Some(PendingRoutedNamespaceResult {
                            result: DfsNamespaceResult::Rejected {
                                reason: reason.clone(),
                                posix_error: None,
                                current_primary_node_id: None,
                            },
                            synced_path_object: None,
                            synced_path_objects: Vec::new(),
                        });
                    }
                    return Ok(NetworkedDfsServiceEvent::RemoteResponseDiscarded {
                        request_id: response.request_id,
                        source_node_id: primary_node_id,
                        reason,
                    });
                }
            }
        }
        let pending = self
            .pending_routed_namespace_ops
            .get_mut(&response.request_id)
            .expect("pending routed namespace op was validated before metadata replay");
        pending.result = Some(PendingRoutedNamespaceResult {
            result: response.result.clone(),
            synced_path_object: response.synced_path_object.clone(),
            synced_path_objects: response.synced_path_objects.clone(),
        });
        Ok(NetworkedDfsServiceEvent::RemoteNamespaceCompleted {
            request_id: response.request_id,
            primary_node_id,
            result: response.result,
        })
    }

    /// The requester's last applied journal sequence per locally known shard,
    /// reported with routed namespace operations so the primary's response
    /// batches replay exactly the missing entries.
    pub(crate) fn local_metadata_shard_cursors(
        &self,
    ) -> Result<Vec<DfsNamespaceShardCursor>, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(Vec::new());
        };
        Ok(backend
            .metadata_shard_statuses()?
            .into_iter()
            .map(|status| DfsNamespaceShardCursor {
                shard_id: status.shard_id,
                after_sequence: status.journal_last_sequence,
            })
            .collect())
    }

    /// Routes a namespace mutation to the metadata-shard primary and blocks
    /// until the primary answers (or the timeout elapses). On `Applied`, the
    /// response journal batches have already been replayed locally, so the
    /// caller immediately reads its own namespace write.
    pub(crate) fn route_namespace_operation_to_metadata_primary(
        &mut self,
        file_system_id: DfsFileSystemId,
        primary_node_id: NodeId,
        operation: DfsNamespaceOperation,
    ) -> Result<DfsNamespaceResult, DfsError> {
        self.route_namespace_operation_to_metadata_primary_with_timeout(
            file_system_id,
            primary_node_id,
            operation,
            DFS_ROUTED_NAMESPACE_OP_TIMEOUT,
        )
    }

    pub(crate) fn route_namespace_operation_to_metadata_primary_with_timeout(
        &mut self,
        file_system_id: DfsFileSystemId,
        primary_node_id: NodeId,
        operation: DfsNamespaceOperation,
        timeout: Duration,
    ) -> Result<DfsNamespaceResult, DfsError> {
        Ok(self
            .route_namespace_operation_to_metadata_primary_with_timeout_and_options(
                file_system_id,
                primary_node_id,
                operation,
                timeout,
                true,
                true,
            )?
            .result)
    }

    pub(crate) fn route_namespace_lookup_path_to_metadata_primary(
        &mut self,
        file_system_id: DfsFileSystemId,
        primary_node_id: NodeId,
        path: String,
        timeout: Duration,
    ) -> Result<Option<DfsObjectMetadata>, DfsError> {
        let mut primary = primary_node_id;
        let operation = DfsNamespaceOperation::LookupPath { path };
        for _ in 0..DFS_ROUTED_WRITE_AUTHORITY_REDIRECT_LIMIT {
            let settled = self
                .route_namespace_operation_to_metadata_primary_with_timeout_and_options(
                    file_system_id.clone(),
                    primary.clone(),
                    operation.clone(),
                    timeout,
                    false,
                    false,
                )?;
            match settled.result {
                DfsNamespaceResult::Applied => return Ok(settled.synced_path_object),
                DfsNamespaceResult::Rejected {
                    reason: _,
                    current_primary_node_id: Some(current),
                    ..
                } if current == self.node_id => {
                    return self.synced_namespace_object_for_operation(&file_system_id, &operation);
                }
                DfsNamespaceResult::Rejected {
                    current_primary_node_id: Some(current),
                    ..
                } if current != primary => {
                    self.cached_sifs_metadata_shards = None;
                    primary = current;
                }
                DfsNamespaceResult::Rejected { reason, .. } => {
                    return Err(DfsError::Distributed { message: reason });
                }
            }
        }
        Err(DfsError::Distributed {
            message: format!(
                "routed namespace lookup exceeded {DFS_ROUTED_WRITE_AUTHORITY_REDIRECT_LIMIT} authority redirects"
            ),
        })
    }

    pub(crate) fn route_namespace_lookup_paths_to_metadata_primary(
        &mut self,
        file_system_id: DfsFileSystemId,
        primary_node_id: NodeId,
        paths: Vec<String>,
        timeout: Duration,
    ) -> Result<Vec<DfsObjectMetadata>, DfsError> {
        if paths.is_empty() {
            return Ok(Vec::new());
        }
        let mut primary = primary_node_id;
        let operation = DfsNamespaceOperation::LookupPaths { paths };
        for _ in 0..DFS_ROUTED_WRITE_AUTHORITY_REDIRECT_LIMIT {
            let settled = self
                .route_namespace_operation_to_metadata_primary_with_timeout_and_options(
                    file_system_id.clone(),
                    primary.clone(),
                    operation.clone(),
                    timeout,
                    false,
                    false,
                )?;
            match settled.result {
                DfsNamespaceResult::Applied => return Ok(settled.synced_path_objects),
                DfsNamespaceResult::Rejected {
                    reason: _,
                    current_primary_node_id: Some(current),
                    ..
                } if current == self.node_id => {
                    return self
                        .synced_namespace_objects_for_operation(&file_system_id, &operation);
                }
                DfsNamespaceResult::Rejected {
                    current_primary_node_id: Some(current),
                    ..
                } if current != primary => {
                    self.cached_sifs_metadata_shards = None;
                    primary = current;
                }
                DfsNamespaceResult::Rejected { reason, .. } => {
                    return Err(DfsError::Distributed { message: reason });
                }
            }
        }
        Err(DfsError::Distributed {
            message: format!(
                "routed namespace lookup exceeded {DFS_ROUTED_WRITE_AUTHORITY_REDIRECT_LIMIT} authority redirects"
            ),
        })
    }

    pub(crate) fn route_namespace_operation_to_metadata_primary_without_local_replay(
        &mut self,
        file_system_id: DfsFileSystemId,
        primary_node_id: NodeId,
        operation: DfsNamespaceOperation,
    ) -> Result<DfsNamespaceResult, DfsError> {
        Ok(self
            .route_namespace_operation_to_metadata_primary_with_timeout_and_options(
                file_system_id,
                primary_node_id,
                operation,
                DFS_ROUTED_NAMESPACE_OP_TIMEOUT,
                false,
                false,
            )?
            .result)
    }

    fn route_namespace_operation_to_metadata_primary_with_timeout_and_options(
        &mut self,
        file_system_id: DfsFileSystemId,
        primary_node_id: NodeId,
        operation: DfsNamespaceOperation,
        timeout: Duration,
        replay_journal_batches: bool,
        apply_synced_path_object: bool,
    ) -> Result<PendingRoutedNamespaceResult, DfsError> {
        let request_id = self.next_request_id();
        let metadata_journal_after_sequences = if replay_journal_batches {
            self.local_metadata_shard_cursors()?
        } else {
            Vec::new()
        };
        let sent = match self.send_dfs_message_if_routable(
            &primary_node_id,
            DfsNetworkMessage::NamespaceRequest(DfsNamespaceCommand {
                request_id: request_id.clone(),
                file_system_id,
                requesting_node_id: self.node_id.clone(),
                include_metadata_journal_batches: replay_journal_batches,
                metadata_journal_after_sequences,
                operation,
            }),
        ) {
            Ok(sent) => sent,
            Err(error @ DfsError::Network { .. }) => {
                self.cached_sifs_metadata_shards = None;
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        if !sent {
            self.cached_sifs_metadata_shards = None;
            return Err(DfsError::Network {
                message: format!(
                    "metadata primary {primary_node_id} is not routable for namespace operation"
                ),
            });
        }
        self.pending_routed_namespace_ops.insert(
            request_id.clone(),
            PendingRoutedNamespaceOp {
                primary_node_id,
                replay_journal_batches,
                apply_synced_path_object,
                result: None,
            },
        );
        self.settle_routed_namespace_op(&request_id, timeout)
    }

    fn routed_response_object_targets_local_primary(
        &mut self,
        object: &DfsObjectMetadata,
    ) -> Result<bool, DfsError> {
        Ok(self
            .select_metadata_shard_for_object(object)?
            .is_some_and(|shard| shard.primary_node_id == self.node_id))
    }

    fn settle_routed_namespace_op(
        &mut self,
        request_id: &str,
        timeout: Duration,
    ) -> Result<PendingRoutedNamespaceResult, DfsError> {
        self.routed_namespace_settle_depth = self.routed_namespace_settle_depth.saturating_add(1);
        let result = self.settle_routed_namespace_op_inner(request_id, timeout);
        self.routed_namespace_settle_depth = self.routed_namespace_settle_depth.saturating_sub(1);
        result
    }

    fn settle_routed_namespace_op_inner(
        &mut self,
        request_id: &str,
        timeout: Duration,
    ) -> Result<PendingRoutedNamespaceResult, DfsError> {
        let started_at = Instant::now();
        loop {
            let events = match self.process_incoming() {
                Ok(events) => events,
                Err(error) => {
                    self.pending_routed_namespace_ops.remove(request_id);
                    return Err(error);
                }
            };
            if self
                .pending_routed_namespace_ops
                .get(request_id)
                .is_some_and(|pending| pending.result.is_some())
            {
                let pending = self
                    .pending_routed_namespace_ops
                    .remove(request_id)
                    .expect("pending routed namespace op checked above");
                return Ok(pending.result.expect("settled result checked above"));
            }
            let elapsed = started_at.elapsed();
            if elapsed >= timeout {
                self.pending_routed_namespace_ops.remove(request_id);
                self.cached_sifs_metadata_shards = None;
                return Err(DfsError::Network {
                    message: format!(
                        "routed namespace operation {request_id} timed out waiting for the metadata primary"
                    ),
                });
            }
            if events.is_empty() {
                let wait = DFS_ROUTED_NAMESPACE_IDLE_WAIT.min(timeout.saturating_sub(elapsed));
                self.transport
                    .wait_for_incoming(wait)
                    .map_err(DfsError::from_network)?;
            }
        }
    }

    /// Whether this node is the metadata write authority for the object's
    /// path: the configured metadata-shard primary, or the local service
    /// itself when no cluster shard configuration exists.
    pub(crate) fn is_local_metadata_primary_for_object(
        &mut self,
        object: &DfsObjectMetadata,
    ) -> Result<bool, DfsError> {
        Ok(match self.select_metadata_shard_for_object(object)? {
            Some(shard) => shard.primary_node_id == self.node_id,
            None => true,
        })
    }

    fn handle_metadata_journal_batch_response(
        &mut self,
        source_node_id: NodeId,
        response: DfsMetadataJournalBatchResponse,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        let pending = self
            .pending_metadata_replications
            .remove(&response.request_id);
        let Some(pending) = pending else {
            if let DfsMetadataJournalBatchResult::Rejected { reason } = &response.result
                && metadata_journal_rejection_allows_snapshot_repair(reason)
            {
                self.send_metadata_snapshot_repair(
                    &source_node_id,
                    &response.shard_id,
                    reason.clone(),
                )?;
            }
            return Ok(None);
        };
        if pending.target_node_id != source_node_id || pending.shard_id != response.shard_id {
            return Ok(Some(
                NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                    request_id: response.request_id,
                    source_node_id,
                    shard_id: response.shard_id,
                    reason: "metadata journal batch response does not match pending replication"
                        .to_string(),
                },
            ));
        }

        Ok(match response.result {
            DfsMetadataJournalBatchResult::Applied => {
                let cursor_key = (source_node_id.clone(), response.shard_id.clone());
                let cursor = self
                    .metadata_replication_cursors
                    .entry(cursor_key)
                    .or_insert(0);
                *cursor = (*cursor)
                    .max(response.next_after_sequence)
                    .max(pending.next_after_sequence);
                if let Some(backend) = self.metadata_backend.as_mut() {
                    backend.put_metadata_replication_cursor(
                        &source_node_id,
                        &response.shard_id,
                        *cursor,
                    )?;
                }
                Some(NetworkedDfsServiceEvent::MetadataJournalBatchApplied {
                    request_id: response.request_id,
                    source_node_id,
                    shard_id: response.shard_id,
                    applied_entries: response.applied_entries,
                    next_after_sequence: *cursor,
                })
            }
            DfsMetadataJournalBatchResult::Rejected { reason } => {
                if metadata_journal_rejection_allows_snapshot_repair(&reason) {
                    self.send_metadata_snapshot_repair(
                        &source_node_id,
                        &response.shard_id,
                        reason.clone(),
                    )?;
                }
                Some(NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                    request_id: response.request_id,
                    source_node_id,
                    shard_id: response.shard_id,
                    reason,
                })
            }
        })
    }

    /// A replica whose journal forked from this primary's stream (for
    /// example a locally bootstrapped node joining as a metadata mirror) cannot
    /// apply batches; repair it by installing a compact table snapshot, the
    /// same reconciliation the standalone publisher performs.
    fn send_metadata_snapshot_repair(
        &mut self,
        target_node_id: &NodeId,
        shard_id: &DfsMetadataShardId,
        reason: String,
    ) -> Result<(), DfsError> {
        match self.metadata_shard_config_for_shard_id(shard_id)? {
            Some(shard) if shard.primary_node_id == self.node_id => {}
            _ => return Ok(()),
        }
        let next_after_sequence = self
            .metadata_journal_last_sequence_for_shard(shard_id)?
            .unwrap_or(0);
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(());
        };
        let Some(snapshot) = backend.export_shard_table_snapshot(shard_id)? else {
            return Ok(());
        };
        let command = DfsMetadataSnapshotInstallCommand {
            request_id: format!(
                "dfs_meta_snapshot_{}_{}_{}",
                self.node_id, target_node_id, next_after_sequence
            ),
            source_node_id: self.node_id.clone(),
            shard_id: shard_id.clone(),
            next_after_sequence,
            snapshot,
            reason,
        };
        let request_id = command.request_id.clone();
        let sent = self.send_dfs_message_if_routable(
            target_node_id,
            DfsNetworkMessage::MetadataSnapshotInstall(command),
        )?;
        if sent {
            self.pending_metadata_replications.insert(
                request_id,
                PendingMetadataReplication {
                    target_node_id: target_node_id.clone(),
                    shard_id: shard_id.clone(),
                    next_after_sequence,
                },
            );
        }
        Ok(())
    }

    /// Resolves a shard configuration by shard id across the filesystems this
    /// service knows about.
    fn metadata_shard_config_for_shard_id(
        &mut self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Option<DfsMetadataShardConfig>, DfsError> {
        if self.control_plane.is_none() {
            return Ok(None);
        }
        let file_system_ids: Vec<DfsFileSystemId> =
            self.metadata.filesystems.keys().cloned().collect();
        for file_system_id in file_system_ids {
            let kv = self
                .control_plane
                .as_mut()
                .expect("control plane checked above");
            if let Some(shard) = DfsControlPlaneStore::new(kv.as_mut())
                .get_metadata_shard(&file_system_id, &shard_id.0)
                .map_err(DfsError::from)?
            {
                return Ok(Some(shard));
            }
        }
        Ok(None)
    }

    /// Replica side of in-service snapshot repair: validates that the sender
    /// is the shard's configured primary and that this node is a configured
    /// replica, replaces the local shard state with the snapshot, and
    /// rehydrates in-memory service state from the replaced backend.
    fn handle_metadata_snapshot_install(
        &mut self,
        source_node_id: NodeId,
        command: DfsMetadataSnapshotInstallCommand,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let request_id = command.request_id.clone();
        let shard_id = command.shard_id.clone();
        let next_after_sequence = command.next_after_sequence;
        let rejection = if command.source_node_id != source_node_id {
            Some("metadata snapshot source does not match transport source".to_string())
        } else {
            match self.metadata_shard_config_for_shard_id(&shard_id)? {
                None if self.control_plane.is_some() => Some(format!(
                    "metadata snapshot install has no configured shard {}",
                    shard_id.0
                )),
                Some(shard) if shard.primary_node_id != source_node_id => Some(format!(
                    "metadata snapshot source {source_node_id} is not shard {} primary {}",
                    shard.shard_id, shard.primary_node_id
                )),
                Some(shard) if shard.primary_node_id == self.node_id => {
                    Some("metadata primary must not install a replica snapshot".to_string())
                }
                // Non-replica metadata mirrors accept primary-sourced snapshot
                // repair too; their journals are fed by routed-response and
                // SyncPath batches and can need the same divergence repair as
                // configured replicas.
                _ => None,
            }
        };
        let result = if let Some(reason) = rejection {
            DfsMetadataSnapshotInstallResult::Rejected { reason }
        } else if let Some(backend) = self.metadata_backend.as_mut() {
            match backend.replace_with_replicated_snapshot(
                shard_id.clone(),
                next_after_sequence,
                command.snapshot,
            ) {
                Ok(archived_existing) => {
                    // The backend was replaced wholesale; in-memory state
                    // must follow it.
                    self.hydrate_from_metadata_backend()?;
                    DfsMetadataSnapshotInstallResult::Installed { archived_existing }
                }
                Err(error) => DfsMetadataSnapshotInstallResult::Rejected {
                    reason: error.to_string(),
                },
            }
        } else {
            DfsMetadataSnapshotInstallResult::Rejected {
                reason: "local DFS metadata backend is not configured".to_string(),
            }
        };
        let installed = matches!(result, DfsMetadataSnapshotInstallResult::Installed { .. });
        let reason = match &result {
            DfsMetadataSnapshotInstallResult::Rejected { reason } => Some(reason.clone()),
            DfsMetadataSnapshotInstallResult::Installed { .. } => None,
        };
        let _ = self.send_dfs_message_if_routable(
            &source_node_id,
            DfsNetworkMessage::MetadataSnapshotInstallResponse(
                DfsMetadataSnapshotInstallResponse {
                    request_id: request_id.clone(),
                    shard_id: shard_id.clone(),
                    next_after_sequence,
                    result,
                },
            ),
        )?;
        Ok(if installed {
            NetworkedDfsServiceEvent::MetadataSnapshotInstalled {
                request_id,
                source_node_id,
                shard_id,
                next_after_sequence,
            }
        } else {
            NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                request_id,
                source_node_id,
                shard_id,
                reason: reason.unwrap_or_else(|| "snapshot install rejected".to_string()),
            }
        })
    }

    /// Primary side of in-service snapshot repair: a successful install
    /// advances the replica's replication cursor to the snapshot head.
    fn handle_metadata_snapshot_install_response(
        &mut self,
        source_node_id: NodeId,
        response: DfsMetadataSnapshotInstallResponse,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        let Some(pending) = self
            .pending_metadata_replications
            .remove(&response.request_id)
        else {
            return Ok(None);
        };
        if pending.target_node_id != source_node_id || pending.shard_id != response.shard_id {
            return Ok(Some(
                NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                    request_id: response.request_id,
                    source_node_id,
                    shard_id: response.shard_id,
                    reason: "metadata snapshot response does not match pending replication"
                        .to_string(),
                },
            ));
        }
        Ok(match response.result {
            DfsMetadataSnapshotInstallResult::Installed { .. } => {
                let cursor_key = (source_node_id.clone(), response.shard_id.clone());
                let cursor = self
                    .metadata_replication_cursors
                    .entry(cursor_key)
                    .or_insert(0);
                *cursor = (*cursor)
                    .max(response.next_after_sequence)
                    .max(pending.next_after_sequence);
                if let Some(backend) = self.metadata_backend.as_mut() {
                    backend.put_metadata_replication_cursor(
                        &source_node_id,
                        &response.shard_id,
                        *cursor,
                    )?;
                }
                Some(NetworkedDfsServiceEvent::MetadataSnapshotInstalled {
                    request_id: response.request_id,
                    source_node_id,
                    shard_id: response.shard_id,
                    next_after_sequence: *cursor,
                })
            }
            DfsMetadataSnapshotInstallResult::Rejected { reason } => {
                Some(NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                    request_id: response.request_id,
                    source_node_id,
                    shard_id: response.shard_id,
                    reason,
                })
            }
        })
    }

    pub(crate) fn metadata_journal_batch_since(
        &self,
        after_sequence: u64,
    ) -> Result<Option<DfsMetadataJournalBatch>, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(None);
        };
        backend
            .journal_batch_since_limited(
                after_sequence,
                DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
                DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
            )
            .map_err(Into::into)
    }

    pub(crate) fn metadata_journal_batch_for_shard_since(
        &self,
        shard_id: &DfsMetadataShardId,
        after_sequence: u64,
    ) -> Result<Option<DfsMetadataJournalBatch>, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(None);
        };
        backend
            .journal_batch_for_shard_since_limited(
                shard_id,
                after_sequence,
                DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
                DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
            )
            .map_err(Into::into)
    }

    pub(crate) fn metadata_journal_last_sequence_for_shard(
        &self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Option<u64>, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(None);
        };
        Ok(backend
            .metadata_shard_statuses()?
            .into_iter()
            .find(|status| &status.shard_id == shard_id)
            .map(|status| status.journal_last_sequence))
    }

    fn handle_metadata_journal_batch(
        &mut self,
        source_node_id: NodeId,
        command: DfsMetadataJournalBatchCommand,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let shard_id = command.batch.shard_id.clone();
        let next_after_sequence = command.batch.next_after_sequence;
        let source_rejection = if command
            .source_node_id
            .as_ref()
            .is_some_and(|claimed| claimed != &source_node_id)
        {
            Some("metadata journal batch source does not match transport source".to_string())
        } else {
            self.validate_metadata_journal_batch_source(&source_node_id, &command.batch)?
        };
        if let Some(reason) = source_rejection {
            let _ = self.send_dfs_message_if_routable(
                &source_node_id,
                DfsNetworkMessage::MetadataJournalBatchResponse(DfsMetadataJournalBatchResponse {
                    request_id: command.request_id.clone(),
                    shard_id: shard_id.clone(),
                    next_after_sequence,
                    applied_entries: 0,
                    result: DfsMetadataJournalBatchResult::Rejected {
                        reason: reason.clone(),
                    },
                }),
            )?;
            return Ok(NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                request_id: command.request_id,
                source_node_id,
                shard_id,
                reason,
            });
        }
        let Some(backend) = self.metadata_backend.as_mut() else {
            let reason = "local DFS metadata backend is not configured".to_string();
            let _ = self.send_dfs_message_if_routable(
                &source_node_id,
                DfsNetworkMessage::MetadataJournalBatchResponse(DfsMetadataJournalBatchResponse {
                    request_id: command.request_id.clone(),
                    shard_id: shard_id.clone(),
                    next_after_sequence,
                    applied_entries: 0,
                    result: DfsMetadataJournalBatchResult::Rejected {
                        reason: reason.clone(),
                    },
                }),
            )?;
            return Ok(NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                request_id: command.request_id,
                source_node_id,
                shard_id,
                reason,
            });
        };

        match backend.apply_journal_batch(&command.batch) {
            Ok(applied_entries) => {
                self.apply_metadata_journal_batch_to_service_state(&command.batch)?;
                let _ = self.send_dfs_message_if_routable(
                    &source_node_id,
                    DfsNetworkMessage::MetadataJournalBatchResponse(
                        DfsMetadataJournalBatchResponse {
                            request_id: command.request_id.clone(),
                            shard_id: shard_id.clone(),
                            next_after_sequence,
                            applied_entries,
                            result: DfsMetadataJournalBatchResult::Applied,
                        },
                    ),
                )?;
                Ok(NetworkedDfsServiceEvent::MetadataJournalBatchApplied {
                    request_id: command.request_id,
                    source_node_id,
                    shard_id,
                    applied_entries,
                    next_after_sequence,
                })
            }
            Err(error) => {
                let reason = error.to_string();
                let _ = self.send_dfs_message_if_routable(
                    &source_node_id,
                    DfsNetworkMessage::MetadataJournalBatchResponse(
                        DfsMetadataJournalBatchResponse {
                            request_id: command.request_id.clone(),
                            shard_id: shard_id.clone(),
                            next_after_sequence,
                            applied_entries: 0,
                            result: DfsMetadataJournalBatchResult::Rejected {
                                reason: reason.clone(),
                            },
                        },
                    ),
                )?;
                Ok(NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                    request_id: command.request_id,
                    source_node_id,
                    shard_id,
                    reason,
                })
            }
        }
    }

    fn validate_metadata_journal_batch_source(
        &mut self,
        source_node_id: &NodeId,
        batch: &DfsMetadataJournalBatch,
    ) -> Result<Option<String>, DfsError> {
        let Some(control_plane) = self.control_plane.as_mut() else {
            return Ok(None);
        };
        let Some(file_system_id) = metadata_journal_batch_file_system_id(batch) else {
            return Ok(None);
        };
        let Some(shard) = DfsControlPlaneStore::new(control_plane.as_mut())
            .get_metadata_shard(&file_system_id, &batch.shard_id.0)?
        else {
            return Ok(Some(format!(
                "metadata journal batch has no configured shard {}",
                batch.shard_id.0
            )));
        };
        if shard.primary_node_id != *source_node_id {
            return Ok(Some(format!(
                "metadata journal source {source_node_id} is not shard {} primary {}",
                shard.shard_id, shard.primary_node_id
            )));
        }
        if shard.primary_node_id == self.node_id {
            return Ok(Some(
                "metadata primary must not apply a replica journal batch".to_string(),
            ));
        }
        // Routed operations still accept primary-sourced journal catch-up
        // batches so requesters keep read-your-write even for custom shards
        // where they are not durable replicas.
        Ok(None)
    }
}
