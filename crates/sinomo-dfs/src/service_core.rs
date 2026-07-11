//! Core construction, accessors, routing helpers, and metrics for the networked DFS service.

use std::collections::{BTreeMap, BTreeSet};

use sinomo_core::network::NetworkError;
use sinomo_core::node::NodeId;

use crate::transfer::{
    DFS_FETCH_CHUNK_WINDOW_SIZE, DFS_READ_CHUNK_WINDOW_SIZE, DFS_STAGE_CHUNK_WINDOW_SIZE,
    chunk_prefix_bytes, dfs_adaptive_chunk_window_size, pending_stage_transfer_size_bytes,
    range_transfer_size_bytes,
};
use crate::{
    AsyncDfsMessageTransport, DfsCancellationFlag, DfsConflictStatus, DfsControlPlaneKvClient,
    DfsError, DfsFileSystem, DfsMessageTransport, DfsMetadataBackend, DfsMetadataStore,
    DfsNetworkMessage, DfsOperationalMetrics, DfsRoutedMessage, DfsTransportIdentity,
    DistributedEventualCoordinator, LocalContentStore, NetworkedDfsService,
    NetworkedDfsServiceEvent, build_dfs_routed_message,
};

impl<T> NetworkedDfsService<T> {
    pub fn invalidate_sifs_metadata_shard_cache(&mut self) {
        self.cached_sifs_metadata_shards = None;
    }

    pub(crate) fn next_dfs_routed_message(
        &mut self,
        target_node_id: &NodeId,
        message: DfsNetworkMessage,
    ) -> DfsRoutedMessage {
        let routed_message = build_dfs_routed_message(
            &self.node_id,
            target_node_id,
            self.next_frame_id,
            self.next_message_id,
            message,
        );
        self.next_frame_id += 1;
        self.next_message_id += 1;
        routed_message
    }
}

impl<T: DfsTransportIdentity> NetworkedDfsService<T> {
    pub fn new(transport: T, content: LocalContentStore) -> Self {
        let node_id = DfsTransportIdentity::local_node_id(&transport).clone();
        let mut metadata = DfsMetadataStore::new();
        metadata.add_file_system(DfsFileSystem::sifs(node_id.clone()));
        Self {
            node_id,
            metadata,
            tombstones_by_metadata_path: BTreeMap::new(),
            transient_metadata_object_ids: BTreeSet::new(),
            transient_metadata_predecessor_object_ids: BTreeMap::new(),
            content,
            coordinator: DistributedEventualCoordinator::new(),
            control_plane: None,
            cached_sifs_metadata_shards: None,
            metadata_backend: None,
            metadata_lookup_cache: std::sync::Mutex::new(BTreeMap::new()),
            metadata_lookup_cache_depth: std::sync::Mutex::new(0),
            active_replica_stages: BTreeMap::new(),
            pending_writes: BTreeMap::new(),
            pending_reads: BTreeMap::new(),
            repair_attempt_records: BTreeMap::new(),
            transient_remote_read_pruned_locations: BTreeMap::new(),
            incoming_read_transfers: BTreeMap::new(),
            incoming_read_memory_transfers: BTreeMap::new(),
            incoming_fetch_transfers: BTreeMap::new(),
            incoming_blob_transfers: BTreeMap::new(),
            incoming_patch_transfers: BTreeMap::new(),
            local_metadata_journal_cursors: BTreeMap::new(),
            metadata_replication_cursors: BTreeMap::new(),
            recent_namespace_freshness_syncs: BTreeMap::new(),
            pending_metadata_replications: BTreeMap::new(),
            pending_routed_writes: BTreeMap::new(),
            pending_remote_write_responses: BTreeMap::new(),
            deferred_routed_create_parent_touches: BTreeSet::new(),
            deferred_untracked_routed_write_responses: BTreeMap::new(),
            pending_routed_namespace_ops: BTreeMap::new(),
            served_namespace_replays: std::collections::VecDeque::new(),
            routed_namespace_settle_depth: 0,
            cross_shard_namespace_locks: BTreeSet::new(),
            last_events: Vec::new(),
            transport,
            repair_retry_tick: 0,
            next_frame_id: 1,
            next_message_id: 1,
            next_request_id: 1,
            cancellation: None,
        }
    }
}

impl<T> NetworkedDfsService<T>
where
    T: AsyncDfsMessageTransport + DfsTransportIdentity,
{
    pub async fn send_dfs_message_if_routable_async(
        &mut self,
        target_node_id: &NodeId,
        message: DfsNetworkMessage,
    ) -> Result<bool, DfsError> {
        let routed_message = self.next_dfs_routed_message(target_node_id, message);
        match self
            .transport
            .send_routed_message_async(target_node_id, routed_message)
            .await
        {
            Ok(()) => Ok(true),
            Err(NetworkError::UnknownNode { .. }) => Ok(false),
            Err(error) => Err(DfsError::from_network(error)),
        }
    }
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn clear_metadata_lookup_cache(&self) {
        if let Ok(mut cache) = self.metadata_lookup_cache.lock() {
            cache.clear();
        }
    }

    pub(crate) fn metadata_lookup_cache_is_active(&self) -> bool {
        self.metadata_lookup_cache_depth
            .lock()
            .map(|depth| *depth > 0)
            .unwrap_or(false)
    }

    pub(crate) fn with_metadata_lookup_cache<R>(&self, operation: impl FnOnce(&Self) -> R) -> R {
        let outer = self.enter_metadata_lookup_cache_scope();
        let result = operation(self);
        self.exit_metadata_lookup_cache_scope(outer);
        result
    }

    fn enter_metadata_lookup_cache_scope(&self) -> bool {
        let outer = if let Ok(mut depth) = self.metadata_lookup_cache_depth.lock() {
            let outer = *depth == 0;
            *depth = depth.saturating_add(1);
            outer
        } else {
            true
        };
        if outer {
            self.clear_metadata_lookup_cache();
        }
        outer
    }

    fn exit_metadata_lookup_cache_scope(&self, outer: bool) {
        let should_clear = if let Ok(mut depth) = self.metadata_lookup_cache_depth.lock() {
            *depth = depth.saturating_sub(1);
            *depth == 0
        } else {
            true
        };
        if outer || should_clear {
            self.clear_metadata_lookup_cache();
        }
    }

    pub(crate) fn adaptive_stage_chunk_window(
        &self,
        target_node_id: &NodeId,
        total_size_bytes: u64,
    ) -> u32 {
        dfs_adaptive_chunk_window_size(
            self.transport.transfer_profile(target_node_id),
            total_size_bytes,
            DFS_STAGE_CHUNK_WINDOW_SIZE,
        )
    }

    pub(crate) fn adaptive_read_chunk_window(
        &self,
        target_node_id: &NodeId,
        total_size_bytes: u64,
    ) -> u32 {
        dfs_adaptive_chunk_window_size(
            self.transport.transfer_profile(target_node_id),
            total_size_bytes,
            DFS_READ_CHUNK_WINDOW_SIZE,
        )
    }

    pub(crate) fn adaptive_fetch_chunk_window(
        &self,
        target_node_id: &NodeId,
        total_size_bytes: u64,
    ) -> u32 {
        dfs_adaptive_chunk_window_size(
            self.transport.transfer_profile(target_node_id),
            total_size_bytes,
            DFS_FETCH_CHUNK_WINDOW_SIZE,
        )
    }

    /// Install a cooperative cancellation flag. Distributed wait/retry loops
    /// (write settling, remote reads, routed namespace sync) observe the flag
    /// and stop with [`DfsError::Cancelled`] once it flips — the caller's way
    /// to abandon budgeted work promptly when the cluster link is degraded.
    pub fn with_cancellation(mut self, cancellation: DfsCancellationFlag) -> Self {
        self.cancellation = Some(cancellation);
        self
    }

    pub fn set_cancellation(&mut self, cancellation: Option<DfsCancellationFlag>) {
        self.cancellation = cancellation;
    }

    pub(crate) fn check_cancelled(&self, operation: &'static str) -> Result<(), DfsError> {
        if self
            .cancellation
            .as_ref()
            .is_some_and(DfsCancellationFlag::is_cancelled)
        {
            return Err(DfsError::Cancelled { operation });
        }
        Ok(())
    }

    pub fn with_control_plane(mut self, kv: impl DfsControlPlaneKvClient + Send + 'static) -> Self {
        self.control_plane = Some(Box::new(kv));
        self.cached_sifs_metadata_shards = None;
        self
    }

    pub fn with_boxed_control_plane(mut self, kv: Box<dyn DfsControlPlaneKvClient>) -> Self {
        self.control_plane = Some(kv);
        self.cached_sifs_metadata_shards = None;
        self
    }

    pub(crate) fn refresh_control_plane_latest(&mut self) -> Result<(), DfsError> {
        let Some(control_plane) = self.control_plane.as_mut() else {
            return Ok(());
        };
        control_plane.refresh_latest().map_err(DfsError::from)?;
        self.cached_sifs_metadata_shards = None;
        Ok(())
    }

    pub fn with_metadata_backend(
        mut self,
        backend: impl DfsMetadataBackend + Send + 'static,
    ) -> Result<Self, DfsError> {
        self.metadata_backend = Some(Box::new(backend));
        self.hydrate_from_metadata_backend()?;
        Ok(self)
    }

    pub fn with_metadata_backend_lazy(
        mut self,
        backend: impl DfsMetadataBackend + Send + 'static,
    ) -> Result<Self, DfsError> {
        self.metadata_backend = Some(Box::new(backend));
        self.hydrate_file_systems_from_metadata_backend()?;
        Ok(self)
    }

    pub fn with_boxed_metadata_backend(
        mut self,
        backend: Box<dyn DfsMetadataBackend>,
    ) -> Result<Self, DfsError> {
        self.metadata_backend = Some(backend);
        self.hydrate_from_metadata_backend()?;
        Ok(self)
    }

    pub fn with_boxed_metadata_backend_lazy(
        mut self,
        backend: Box<dyn DfsMetadataBackend>,
    ) -> Result<Self, DfsError> {
        self.metadata_backend = Some(backend);
        self.hydrate_file_systems_from_metadata_backend()?;
        Ok(self)
    }

    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    pub fn metadata(&self) -> &DfsMetadataStore {
        &self.metadata
    }

    pub fn content(&self) -> &LocalContentStore {
        &self.content
    }

    pub fn last_events(&self) -> &[NetworkedDfsServiceEvent] {
        &self.last_events
    }

    pub fn into_transport(self) -> T {
        self.transport
    }

    pub fn coordinator(&self) -> &DistributedEventualCoordinator {
        &self.coordinator
    }

    pub fn metadata_backend(&self) -> Option<&dyn DfsMetadataBackend> {
        self.metadata_backend.as_deref()
    }

    pub fn operational_metrics(&self) -> Result<DfsOperationalMetrics, DfsError> {
        let open_conflict_count = self
            .metadata
            .conflicts
            .values()
            .map(|conflicts| {
                conflicts
                    .values()
                    .filter(|conflict| conflict.status == DfsConflictStatus::Open)
                    .count()
            })
            .sum();
        let stage_retry_count = self
            .pending_writes
            .values()
            .map(|pending| pending.stage_retries.len())
            .sum();
        let repair_retry_count = self
            .pending_writes
            .values()
            .map(|pending| pending.repair_retries.len())
            .sum();
        let stage_inflight_count = self
            .pending_writes
            .values()
            .map(|pending| pending.stage_inflight_request_ids.len())
            .sum();
        let repair_inflight_count = self
            .pending_writes
            .values()
            .map(|pending| pending.repair_inflight_request_ids.len())
            .sum();
        let fetch_inflight_count = self
            .pending_writes
            .values()
            .map(|pending| pending.fetch_inflight_from.len())
            .sum();
        let fetch_failed_source_count = self
            .pending_writes
            .values()
            .map(|pending| pending.fetch_failed_from.len())
            .sum();
        let stage_window_resume_count = self
            .pending_writes
            .values()
            .map(|pending| pending.stage_next_chunk_indexes.len())
            .sum();
        let stage_window_resume_bytes = self
            .pending_writes
            .values()
            .flat_map(|pending| {
                pending
                    .stage_next_chunk_indexes
                    .iter()
                    .map(|(node_id, next_chunk_index)| {
                        let size_bytes = pending_stage_transfer_size_bytes(pending, node_id);
                        chunk_prefix_bytes(size_bytes, *next_chunk_index)
                    })
            })
            .fold(0_u64, |total, bytes| total.saturating_add(bytes));
        let repair_window_resume_count = self
            .pending_writes
            .values()
            .map(|pending| pending.repair_next_chunk_indexes.len())
            .sum();
        let repair_window_resume_bytes = self
            .pending_writes
            .values()
            .flat_map(|pending| {
                pending
                    .repair_next_chunk_indexes
                    .values()
                    .map(|next_chunk_index| {
                        chunk_prefix_bytes(pending.size_bytes, *next_chunk_index)
                    })
            })
            .fold(0_u64, |total, bytes| total.saturating_add(bytes));
        let fetch_window_resume_count = self
            .pending_writes
            .values()
            .map(|pending| {
                pending
                    .fetch_next_chunk_indexes
                    .values()
                    .filter(|next_chunk_index| **next_chunk_index > 0)
                    .count()
            })
            .sum();
        let fetch_window_resume_bytes = self
            .pending_writes
            .values()
            .flat_map(|pending| {
                pending
                    .fetch_next_chunk_indexes
                    .values()
                    .filter(|next_chunk_index| **next_chunk_index > 0)
                    .map(|next_chunk_index| {
                        chunk_prefix_bytes(pending.size_bytes, *next_chunk_index)
                    })
            })
            .fold(0_u64, |total, bytes| total.saturating_add(bytes));
        let pending_read_window_resume_count = self
            .pending_reads
            .values()
            .filter(|pending| pending.next_chunk_index > 0)
            .count();
        let pending_read_window_resume_bytes = self
            .pending_reads
            .values()
            .filter_map(|pending| {
                pending.content_size_bytes.map(|size_bytes| {
                    let transfer_size_bytes = match (pending.range_offset, pending.range_max_bytes)
                    {
                        (Some(offset), Some(max_bytes)) => {
                            range_transfer_size_bytes(size_bytes, offset, max_bytes)
                        }
                        _ => size_bytes,
                    };
                    chunk_prefix_bytes(transfer_size_bytes, pending.next_chunk_index)
                })
            })
            .fold(0_u64, |total, bytes| total.saturating_add(bytes));
        let incoming_transfer_bytes = self
            .incoming_blob_transfers
            .values()
            .map(|transfer| transfer.total_size_bytes())
            .chain(
                self.incoming_patch_transfers
                    .values()
                    .map(|transfer| transfer.total_size_bytes()),
            )
            .chain(
                self.incoming_fetch_transfers
                    .values()
                    .map(|transfer| transfer.total_size_bytes()),
            )
            .chain(
                self.incoming_read_transfers
                    .values()
                    .map(|transfer| transfer.total_size_bytes()),
            )
            .fold(0_u64, |total, size| total.saturating_add(size));
        let incoming_blob_transfer_received_bytes = self
            .incoming_blob_transfers
            .values()
            .map(|transfer| {
                chunk_prefix_bytes(
                    transfer.total_size_bytes(),
                    transfer.contiguous_received_chunk_count(),
                )
            })
            .fold(0_u64, |total, bytes| total.saturating_add(bytes));
        let incoming_patch_transfer_received_bytes = self
            .incoming_patch_transfers
            .values()
            .map(|transfer| {
                chunk_prefix_bytes(
                    transfer.total_size_bytes(),
                    transfer.contiguous_received_chunk_count(),
                )
            })
            .fold(0_u64, |total, bytes| total.saturating_add(bytes));
        let incoming_fetch_transfer_received_bytes = self
            .incoming_fetch_transfers
            .values()
            .map(|transfer| {
                chunk_prefix_bytes(
                    transfer.total_size_bytes(),
                    transfer.contiguous_received_chunk_count(),
                )
            })
            .fold(0_u64, |total, bytes| total.saturating_add(bytes));
        let incoming_read_transfer_received_bytes = self
            .incoming_read_transfers
            .values()
            .map(|transfer| {
                chunk_prefix_bytes(
                    transfer.total_size_bytes(),
                    transfer.contiguous_received_chunk_count(),
                )
            })
            .fold(0_u64, |total, bytes| total.saturating_add(bytes));

        Ok(DfsOperationalMetrics {
            node_id: self.node_id.clone(),
            file_system_count: self.metadata.filesystems.len(),
            object_count: self.metadata.objects.len(),
            open_conflict_count,
            coordinator_intent_count: self.coordinator.intent_count(),
            coordinator_receipt_count: self.coordinator.receipt_count(),
            coordinator_committed_version_count: self.coordinator.committed_version_count(),
            repair_job_count: self.coordinator.repair_jobs().len(),
            repair_attempt_count: self.active_repair_attempt_records().len(),
            pending_write_count: self.pending_writes.len(),
            pending_read_count: self.pending_reads.len(),
            stage_retry_count,
            repair_retry_count,
            stage_inflight_count,
            repair_inflight_count,
            fetch_inflight_count,
            fetch_failed_source_count,
            stage_window_resume_count,
            stage_window_resume_bytes,
            repair_window_resume_count,
            repair_window_resume_bytes,
            fetch_window_resume_count,
            fetch_window_resume_bytes,
            pending_read_window_resume_count,
            pending_read_window_resume_bytes,
            incoming_blob_transfer_count: self.incoming_blob_transfers.len(),
            incoming_patch_transfer_count: self.incoming_patch_transfers.len(),
            incoming_fetch_transfer_count: self.incoming_fetch_transfers.len(),
            incoming_read_transfer_count: self.incoming_read_transfers.len(),
            incoming_transfer_bytes,
            incoming_blob_transfer_received_bytes,
            incoming_patch_transfer_received_bytes,
            incoming_fetch_transfer_received_bytes,
            incoming_read_transfer_received_bytes,
            active_replica_stage_count: self.active_replica_stages.len(),
            metadata_replication_cursor_count: self.metadata_replication_cursors.len(),
            pending_metadata_replication_count: self.pending_metadata_replications.len(),
            pending_routed_write_count: self.pending_routed_writes.len(),
            pending_remote_write_response_count: self.pending_remote_write_responses.len(),
            pending_routed_namespace_op_count: self.pending_routed_namespace_ops.len(),
            cross_shard_namespace_lock_count: self.cross_shard_namespace_locks.len(),
            metadata_backend_configured: self.metadata_backend.is_some(),
            content: self.content.stats()?,
        })
    }
}
