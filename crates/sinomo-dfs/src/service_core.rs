//! Core construction, accessors, routing helpers, and metrics for the networked DFS service.

use std::collections::{BTreeMap, BTreeSet};

use sinomo_core::network::NetworkError;
use sinomo_core::node::NodeId;

use crate::{
    AsyncDfsMessageTransport, DfsCancellationFlag, DfsCommittedWorkTracker, DfsConflictStatus,
    DfsControlPlaneKvClient, DfsError, DfsFileSystem, DfsMessageCommittedWork, DfsMessageTransport,
    DfsMetadataBackend, DfsMetadataStore, DfsNetworkMessage, DfsOperationalMetrics, DfsRoutedCall,
    DfsRoutedMessage, DfsTransportIdentity, DistributedEventualCoordinator, LocalContentStore,
    NetworkedDfsService, NetworkedDfsServiceEvent, build_dfs_routed_message,
};

impl<T> NetworkedDfsService<T> {
    /// Retained for callers that react to control-plane updates. Shard-config
    /// snapshots are operation-scoped now, so there is no cross-operation
    /// cache to invalidate.
    pub fn invalidate_sifs_metadata_shard_cache(&mut self) {}

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
            metadata_backend: None,
            metadata_lookup_cache: std::sync::Mutex::new(BTreeMap::new()),
            metadata_lookup_cache_depth: std::sync::Mutex::new(0),
            active_replica_stages: BTreeMap::new(),
            pending_writes: BTreeMap::new(),
            pending_reads: BTreeMap::new(),
            repair_attempt_records: BTreeMap::new(),
            transient_remote_read_pruned_locations: BTreeMap::new(),
            local_metadata_journal_cursors: BTreeMap::new(),
            metadata_replication_cursors: BTreeMap::new(),
            recent_namespace_freshness_syncs: BTreeMap::new(),
            pending_metadata_replications: BTreeMap::new(),
            pending_metadata_authority_probes: BTreeMap::new(),
            metadata_authority_probe_results: BTreeMap::new(),
            pending_routed_writes: BTreeMap::new(),
            pending_remote_write_responses: BTreeMap::new(),
            deferred_routed_create_parent_touches: BTreeSet::new(),
            deferred_untracked_routed_write_responses: BTreeMap::new(),
            pending_routed_namespace_ops: BTreeMap::new(),
            active_namespace_operation_context: None,
            pending_namespace_request_recovery_cursor: None,
            active_durable_namespace_request: None,
            active_durable_namespace_prepare_recorded: false,
            routed_namespace_settle_depth: 0,
            cross_shard_namespace_locks: BTreeSet::new(),
            last_events: Vec::new(),
            message_committed_work: DfsMessageCommittedWork::default(),
            committed_work_tracker: None,
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
            .send_routed_call_async(target_node_id, DfsRoutedCall::value(routed_message))
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

    /// Add a cancellation source without replacing one already installed by
    /// the caller which opened the service.
    pub fn add_cancellation(&mut self, cancellation: DfsCancellationFlag) {
        self.cancellation = Some(match self.cancellation.take() {
            Some(existing) => DfsCancellationFlag::linked([existing, cancellation]),
            None => cancellation,
        });
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
        self
    }

    pub fn with_boxed_control_plane(mut self, kv: Box<dyn DfsControlPlaneKvClient>) -> Self {
        self.control_plane = Some(kv);
        self
    }

    pub(crate) fn refresh_control_plane_latest(&mut self) -> Result<(), DfsError> {
        let Some(control_plane) = self.control_plane.as_mut() else {
            return Ok(());
        };
        control_plane.refresh_latest().map_err(DfsError::from)?;
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

    /// Attach and hydrate a backend while explicitly reconstructing durable
    /// post-response CommitVersion work into this service's detached outbox.
    /// Ordinary/request-lane hydration deliberately does not perform this
    /// background recovery.
    pub fn with_boxed_metadata_backend_for_commit_promotion_recovery(
        mut self,
        backend: Box<dyn DfsMetadataBackend>,
    ) -> Result<Self, DfsError> {
        self.metadata_backend = Some(backend);
        self.hydrate_from_metadata_backend_for_commit_promotion_recovery()?;
        Ok(self)
    }

    pub(crate) fn with_boxed_metadata_backend_for_commit_promotion_recovery_until_cancelled(
        mut self,
        backend: Box<dyn DfsMetadataBackend>,
        should_cancel: &(dyn Fn() -> bool + Sync),
    ) -> Result<Option<Self>, DfsError> {
        self.metadata_backend = Some(backend);
        if !self.hydrate_from_metadata_backend_for_commit_promotion_recovery_until_cancelled(
            should_cancel,
        )? {
            return Ok(None);
        }
        Ok(Some(self))
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

    pub(crate) fn begin_message_committed_work(&mut self) {
        self.message_committed_work = DfsMessageCommittedWork::default();
    }

    pub(crate) fn record_message_committed_work(&mut self, work: DfsMessageCommittedWork) {
        self.message_committed_work.merge(work);
        if let Some(tracker) = &self.committed_work_tracker {
            tracker.record(work);
        }
    }

    pub(crate) fn set_committed_work_tracker(&mut self, tracker: DfsCommittedWorkTracker) {
        self.committed_work_tracker = Some(tracker);
    }

    pub(crate) fn take_message_committed_work(&mut self) -> DfsMessageCommittedWork {
        std::mem::take(&mut self.message_committed_work)
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
        let mut authoritative_write_frozen_shard_count = 0usize;
        if let Some(backend) = self.metadata_backend.as_deref() {
            for status in backend.metadata_shard_statuses()? {
                if status.primary_node_id.as_ref() != Some(&self.node_id) {
                    continue;
                }
                if backend.metadata_shard_authoritative_writes_frozen(&status.shard_id)? {
                    authoritative_write_frozen_shard_count =
                        authoritative_write_frozen_shard_count.saturating_add(1);
                }
            }
        }
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
            active_replica_stage_count: self.active_replica_stages.len(),
            metadata_replication_cursor_count: self.metadata_replication_cursors.len(),
            pending_metadata_replication_count: self.pending_metadata_replications.len(),
            pending_routed_write_count: self.pending_routed_writes.len(),
            pending_remote_write_response_count: self.pending_remote_write_responses.len(),
            pending_routed_namespace_op_count: self.pending_routed_namespace_ops.len(),
            cross_shard_namespace_lock_count: self.cross_shard_namespace_locks.len(),
            authoritative_write_frozen_shard_count,
            metadata_backend_configured: self.metadata_backend.is_some(),
            content: self.content.stats()?,
        })
    }
}
