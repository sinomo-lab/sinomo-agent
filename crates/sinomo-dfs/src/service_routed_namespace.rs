//! Routed namespace request and response lifecycle.
//!
//! This module owns metadata-primary namespace routing: serving routed
//! namespace commands, collecting response journal batches, applying synced
//! path objects on requesters, and settling pending namespace operations. The
//! generic data-plane dispatcher remains in `service_protocol`.

use std::time::{Duration, Instant};

use sinomo_core::node::NodeId;

use crate::*;
use crate::{posix_model, posix_path};

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
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
        let Some(normalized) = posix_path::normalized_posix_path_key(path) else {
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
            | DfsNamespaceOperation::Mkdir { path } => posix_path::normalized_posix_path_key(path)
                .into_iter()
                .collect(),
            DfsNamespaceOperation::MkdirAll { path } => Self::synced_mkdir_all_paths(path),
            DfsNamespaceOperation::LookupPaths { paths } => paths
                .iter()
                .filter_map(|path| posix_path::normalized_posix_path_key(path))
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
        let normalized_path = match posix_path::normalized_posix_path_key(path) {
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
                            Err(posix_model::DfsPosixError::Dfs(DfsError::Distributed {
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
                            posix_error: posix_model::namespace_posix_error_for(&error),
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
                batches.extend(self.metadata_journal_batches_for_shard_since_limited(
                    shard_id,
                    after_sequence,
                    DFS_ROUTED_NAMESPACE_RESPONSE_JOURNAL_BATCH_LIMIT,
                )?);
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
                            journal_batch_limit_reached: false,
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
                    journal_batch_limit_reached: false,
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
                            journal_batch_limit_reached: false,
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
            journal_batch_limit_reached: response.metadata_journal_batches.len()
                >= DFS_ROUTED_NAMESPACE_RESPONSE_JOURNAL_BATCH_LIMIT,
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
                    let next_primary = self.refreshed_primary_after_authority_redirect(
                        &file_system_id,
                        &operation,
                        &primary,
                        current,
                    )?;
                    if next_primary == self.node_id {
                        return self
                            .synced_namespace_object_for_operation(&file_system_id, &operation);
                    }
                    primary = next_primary;
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
                    let next_primary = self.refreshed_primary_after_authority_redirect(
                        &file_system_id,
                        &operation,
                        &primary,
                        current,
                    )?;
                    if next_primary == self.node_id {
                        return self
                            .synced_namespace_objects_for_operation(&file_system_id, &operation);
                    }
                    primary = next_primary;
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

    pub(crate) fn refreshed_primary_after_authority_redirect(
        &mut self,
        file_system_id: &DfsFileSystemId,
        operation: &DfsNamespaceOperation,
        attempted_primary: &NodeId,
        redirected_primary: NodeId,
    ) -> Result<NodeId, DfsError> {
        self.cached_sifs_metadata_shards = None;
        Ok(
            match self
                .refreshed_single_primary_for_namespace_operation(file_system_id, operation)?
            {
                Some(refreshed_primary) if refreshed_primary != *attempted_primary => {
                    refreshed_primary
                }
                _ => redirected_primary,
            },
        )
    }

    fn refreshed_single_primary_for_namespace_operation(
        &mut self,
        file_system_id: &DfsFileSystemId,
        operation: &DfsNamespaceOperation,
    ) -> Result<Option<NodeId>, DfsError> {
        let shards = self.affected_namespace_shards(file_system_id, operation)?;
        let Some(first) = shards.first().map(|shard| shard.primary_node_id.clone()) else {
            return Ok(None);
        };
        if shards.iter().all(|shard| shard.primary_node_id == first) {
            Ok(Some(first))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn route_namespace_operation_to_metadata_primary_with_timeout_and_options(
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
            if let Err(cancelled) = self.check_cancelled("routed namespace settle") {
                self.pending_routed_namespace_ops.remove(request_id);
                return Err(cancelled);
            }
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
}
