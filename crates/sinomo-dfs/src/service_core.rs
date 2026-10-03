//! Core construction, accessors, routing helpers, and metrics for the networked DFS service.

use std::collections::BTreeMap;

use sinomo_core::network::NetworkError;
use sinomo_core::node::NodeId;

use crate::service_state::DfsMetadataBackendOwner;
use crate::{
    AsyncDfsMessageTransport, DfsCancellationFlag, DfsCommittedWorkTracker, DfsConflictStatus,
    DfsControlPlaneKvClient, DfsError, DfsFileSystem, DfsMessageCommittedWork, DfsMessageTransport,
    DfsMetadataBackend, DfsMetadataStore, DfsNetworkMessage, DfsObjectId, DfsOperationalMetrics,
    DfsRoutedCall, DfsRoutedMessage, DfsTransportIdentity, DistributedEventualCoordinator,
    LocalContentStore, NetworkedDfsService, NetworkedDfsServiceEvent,
    ResidentLocalMetadataShardLease, ResidentLocalShardedMetadataBackendLease,
    build_dfs_routed_message,
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
            writes: Default::default(),
            reads: Default::default(),
            replication: Default::default(),
            metadata,
            tombstones_by_metadata_path: BTreeMap::new(),
            transient_metadata_object_ids: Default::default(),
            transient_metadata_predecessor_object_ids: BTreeMap::new(),
            content,
            coordinator: DistributedEventualCoordinator::new(),
            control_plane: None,
            placement_snapshot_cache: Default::default(),
            content_pin_node_ids: BTreeMap::new(),
            metadata_backend: None,
            metadata_lookup_cache: std::sync::Mutex::new(BTreeMap::new()),
            metadata_lookup_cache_depth: std::sync::Mutex::new(0),
            passive_fetch_started_at: BTreeMap::new(),
            repair_attempt_records: BTreeMap::new(),
            implicit_read_replica_ingest_enabled: true,
            pending_routed_namespace_ops: BTreeMap::new(),
            stable_namespace_reconciliation_results: BTreeMap::new(),
            stable_namespace_reconciliation_targets: BTreeMap::new(),
            namespace_execution: Default::default(),
            pending_namespace_request_recovery_cursor: None,
            write_intent_maintenance_scan: std::sync::Arc::new(Default::default()),
            routed_namespace_settle_depth: 0,
            cross_shard_namespace_locks: Default::default(),
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
    /// Share the live cross-shard namespace admission projection with another
    /// request shell over the same resident metadata generation.
    pub fn with_cross_shard_namespace_lock_registry(
        mut self,
        registry: crate::DfsCrossShardNamespaceLockRegistry,
    ) -> Self {
        self.cross_shard_namespace_locks = registry;
        self
    }

    pub fn cross_shard_namespace_lock_registry(&self) -> crate::DfsCrossShardNamespaceLockRegistry {
        self.cross_shard_namespace_locks.clone()
    }

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

    pub(crate) fn with_metadata_lookup_cache_mut<R>(
        &mut self,
        operation: impl FnOnce(&mut Self) -> R,
    ) -> R {
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

    /// Keep ordinary mounted reads non-mutating. Explicit directory refreshes
    /// may still replay replica journals, but their adapters must own the
    /// crash-safe mounted-mutation handoff around that call.
    pub fn disable_implicit_read_replica_ingest(&mut self) {
        self.implicit_read_replica_ingest_enabled = false;
        self.replication.recent_namespace_freshness_syncs.clear();
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

    pub fn with_control_plane(mut self, kv: impl DfsControlPlaneKvClient + 'static) -> Self {
        self.control_plane = Some(Box::new(kv));
        self
    }

    pub fn with_boxed_control_plane(mut self, kv: Box<dyn DfsControlPlaneKvClient>) -> Self {
        self.control_plane = Some(kv);
        self
    }

    pub fn with_placement_snapshot_cache(
        mut self,
        cache: crate::DfsPlacementSnapshotCache,
    ) -> Self {
        self.placement_snapshot_cache = cache;
        self
    }

    pub fn placement_snapshot_cache(&self) -> crate::DfsPlacementSnapshotCache {
        self.placement_snapshot_cache.clone()
    }

    pub fn set_content_pin_snapshot(
        &mut self,
        pins: impl IntoIterator<Item = (DfsObjectId, NodeId)>,
    ) {
        self.content_pin_node_ids.clear();
        for (object_id, node_id) in pins {
            self.content_pin_node_ids
                .entry(object_id)
                .or_default()
                .insert(node_id);
        }
    }

    pub fn content_pin_nodes(&self, object_id: &DfsObjectId) -> Vec<NodeId> {
        self.content_pin_node_ids
            .get(object_id)
            .into_iter()
            .flat_map(|node_ids| node_ids.iter().cloned())
            .collect()
    }

    pub(crate) fn ranked_transfer_sources(
        &self,
        sources: impl IntoIterator<Item = NodeId>,
    ) -> Vec<NodeId> {
        self.placement_snapshot_cache
            .rank_sources(&self.node_id, sources)
    }

    /// sinomo:kv-configuration: Invalidate stale configuration after redirect or authority evidence; hosted clients reuse the retained projection.
    pub(crate) fn refresh_control_plane_latest(&mut self) -> Result<(), DfsError> {
        let Some(control_plane) = self.control_plane.as_mut() else {
            return Ok(());
        };
        control_plane.refresh_latest().map_err(DfsError::from)?;
        Ok(())
    }

    pub fn with_metadata_backend(
        mut self,
        backend: impl DfsMetadataBackend + 'static,
    ) -> Result<Self, DfsError> {
        self.metadata_backend = Some(DfsMetadataBackendOwner::Owned(Box::new(backend)));
        self.hydrate_from_metadata_backend()?;
        Ok(self)
    }

    pub fn with_metadata_backend_lazy(
        mut self,
        backend: impl DfsMetadataBackend + 'static,
    ) -> Result<Self, DfsError> {
        self.metadata_backend = Some(DfsMetadataBackendOwner::Owned(Box::new(backend)));
        self.hydrate_file_systems_from_metadata_backend()?;
        Ok(self)
    }

    #[cfg(test)]
    pub(crate) fn set_metadata_backend_unhydrated(
        &mut self,
        backend: impl DfsMetadataBackend + 'static,
    ) {
        self.metadata_backend = Some(DfsMetadataBackendOwner::Owned(Box::new(backend)));
    }

    pub fn with_boxed_metadata_backend(
        mut self,
        backend: Box<dyn DfsMetadataBackend>,
    ) -> Result<Self, DfsError> {
        self.metadata_backend = Some(DfsMetadataBackendOwner::Owned(backend));
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
        self.metadata_backend = Some(DfsMetadataBackendOwner::Owned(backend));
        self.hydrate_from_metadata_backend_for_commit_promotion_recovery()?;
        Ok(self)
    }

    pub(crate) fn with_boxed_metadata_backend_for_commit_promotion_recovery_until_cancelled(
        mut self,
        backend: Box<dyn DfsMetadataBackend>,
        should_cancel: &(dyn Fn() -> bool + Sync),
    ) -> Result<Option<Self>, DfsError> {
        self.metadata_backend = Some(DfsMetadataBackendOwner::Owned(backend));
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
        self.metadata_backend = Some(DfsMetadataBackendOwner::Owned(backend));
        self.hydrate_file_systems_from_metadata_backend()?;
        Ok(self)
    }

    /// Attach a metadata backend for a service lane which admits read-only
    /// requests exclusively.
    ///
    /// Unlike [`Self::with_boxed_metadata_backend_lazy`], this does not scan the
    /// filesystem catalog, running namespace jobs, or metadata-shard journal
    /// heads. Those bootstrap projections are mutation-lane state: targeted
    /// reads resolve their filesystem and shard authority from the control
    /// plane and read the durable backend directly. The caller must keep this
    /// service on a read-only admission lane for its entire lifetime.
    pub fn with_boxed_metadata_backend_for_read_only_requests(
        mut self,
        backend: Box<dyn DfsMetadataBackend>,
    ) -> Self {
        self.metadata_backend = Some(DfsMetadataBackendOwner::Owned(backend));
        self
    }

    /// Attach one checked-out resident shard and hydrate the bounded mutation
    /// lane without transferring ownership of the process-wide backend pool.
    pub fn with_resident_metadata_shard_backend_lazy(
        mut self,
        backend: ResidentLocalMetadataShardLease,
    ) -> Result<Self, DfsError> {
        self.metadata_backend = Some(DfsMetadataBackendOwner::ResidentShard(backend));
        self.hydrate_file_systems_from_metadata_backend()?;
        Ok(self)
    }

    /// Attach the checked-out resident router and hydrate the compatibility
    /// lane used for operations which cannot prove one exact shard.
    pub fn with_resident_sharded_metadata_backend_lazy(
        mut self,
        backend: ResidentLocalShardedMetadataBackendLease,
    ) -> Result<Self, DfsError> {
        self.metadata_backend = Some(DfsMetadataBackendOwner::ResidentSharded(backend));
        self.hydrate_file_systems_from_metadata_backend()?;
        Ok(self)
    }

    /// Attach an exact resident backend immediately before one served request.
    pub fn attach_resident_metadata_shard_backend(
        &mut self,
        backend: ResidentLocalMetadataShardLease,
    ) -> Result<(), DfsError> {
        self.attach_resident_metadata_backend(DfsMetadataBackendOwner::ResidentShard(backend))
    }

    /// Attach the resident all-shard router immediately before one global
    /// compatibility request or maintenance scope.
    pub fn attach_resident_sharded_metadata_backend(
        &mut self,
        backend: ResidentLocalShardedMetadataBackendLease,
    ) -> Result<(), DfsError> {
        self.attach_resident_metadata_backend(DfsMetadataBackendOwner::ResidentSharded(backend))
    }

    fn attach_resident_metadata_backend(
        &mut self,
        backend: DfsMetadataBackendOwner,
    ) -> Result<(), DfsError> {
        if self.metadata_backend.is_some() {
            return Err(DfsError::Metadata {
                message: "DFS service already has an attached metadata backend".to_string(),
            });
        }
        self.metadata_backend = Some(backend);
        Ok(())
    }

    /// Return a request-scoped resident backend to its pool. Owned backends are
    /// never detached through this API.
    pub fn release_resident_metadata_backend(&mut self) -> Result<(), DfsError> {
        if self
            .metadata_backend
            .as_ref()
            .is_some_and(|backend| matches!(backend, DfsMetadataBackendOwner::Owned(_)))
        {
            return Err(DfsError::Metadata {
                message: "cannot release an owned DFS metadata backend as a resident lease"
                    .to_string(),
            });
        }
        self.metadata_backend.take();
        Ok(())
    }

    /// Rebuild the lightweight service shell while retaining this exact
    /// backend generation, transport, content store, and control-plane client.
    ///
    /// Broad maintenance temporarily hydrates complete object, intent, and
    /// receipt state. A long-lived daemon request lane needs only the durable
    /// backend plus filesystem and namespace guards between requests; keeping
    /// the full hydrated projection would retain a second copy of accumulated
    /// metadata. Consuming the service also makes it impossible for callers to
    /// accidentally reuse pending in-memory operation state after maintenance.
    pub fn into_lazy_metadata_backend_service(mut self) -> Result<Self, DfsError> {
        let metadata_backend = self.metadata_backend.take();
        let control_plane = self.control_plane.take();
        let Self {
            transport,
            content,
            cross_shard_namespace_locks,
            replication,
            implicit_read_replica_ingest_enabled,
            pending_namespace_request_recovery_cursor,
            write_intent_maintenance_scan,
            committed_work_tracker,
            repair_retry_tick,
            next_frame_id,
            next_message_id,
            next_request_id,
            cancellation,
            ..
        } = self;
        let mut service = Self::new(transport, content);
        service.control_plane = control_plane;
        service.metadata_backend = metadata_backend;
        // Request shells over one resident generation must continue sharing
        // the same live job-owned admission projection after explicit disaster
        // recovery collapses its fully hydrated service back to lazy state.
        service.cross_shard_namespace_locks = cross_shard_namespace_locks;
        service.replication = replication.into_lazy();
        service.implicit_read_replica_ingest_enabled = implicit_read_replica_ingest_enabled;
        // This cursor is tiny but provides bounded-scan fairness across routine
        // maintenance ticks. Restarting at the first durable page after every
        // lazy rebuild could starve later locally authoritative requests behind
        // a large prefix of foreign requests.
        service.pending_namespace_request_recovery_cursor =
            pending_namespace_request_recovery_cursor;
        service.write_intent_maintenance_scan = write_intent_maintenance_scan;
        service.committed_work_tracker = committed_work_tracker;
        // Rebuilding a shell is not a process restart. Preserve monotonic
        // transport/request identities and lifecycle state so delayed replies
        // cannot collide with a newly reset counter sequence.
        service.repair_retry_tick = repair_retry_tick;
        service.next_frame_id = next_frame_id;
        service.next_message_id = next_message_id;
        service.next_request_id = next_request_id;
        service.cancellation = cancellation;
        service.hydrate_file_systems_from_metadata_backend()?;
        Ok(service)
    }

    /// Whether this service owns no request-local state that would be lost by
    /// rebuilding it as a lazy metadata shell.
    ///
    /// The hydrated metadata store, coordinator history, tombstones, lookup
    /// cache, journal cursors, freshness observations, repair-attempt cache,
    /// and retained events are all rebuildable from durable state (or are
    /// advisory caches). The small bounded-recovery cursor, shared cross-shard
    /// lock registry, replica cursors, committed-work tracker, transport
    /// counters, and cancellation flag are preserved by
    /// [`Self::into_lazy_metadata_backend_service`].
    /// Everything checked below instead owns a live request, response,
    /// replication, repair, or namespace transition and must fail closed.
    pub fn can_release_hydrated_metadata_projection(&self) -> bool {
        let metadata_lookup_scope_quiescent = self
            .metadata_lookup_cache_depth
            .lock()
            .is_ok_and(|depth| *depth == 0);
        let committed_work_has_durable_owner = self.message_committed_work
            == DfsMessageCommittedWork::default()
            || self.committed_work_tracker.is_some();
        let repair_attempts_are_not_in_flight = self
            .repair_attempt_records
            .values()
            .all(|record| !record.in_flight);

        metadata_lookup_scope_quiescent
            && self.writes.is_quiescent()
            && self.reads.is_quiescent()
            && repair_attempts_are_not_in_flight
            && self.replication.is_quiescent()
            && self.pending_routed_namespace_ops.is_empty()
            && self.namespace_execution.is_quiescent()
            && self.routed_namespace_settle_depth == 0
            && committed_work_has_durable_owner
    }

    /// Release a quiescent exact-shard lane's rebuildable metadata mirror.
    ///
    /// `false` means some live operation state still owns the projection and
    /// nothing was changed. Broad maintenance services use
    /// [`Self::can_release_hydrated_metadata_projection`] before consuming the
    /// service through [`Self::into_lazy_metadata_backend_service`]; exact
    /// request lanes can use this mutating form because their resident backend
    /// lease remains attached until the caller's ordinary release boundary.
    pub fn release_hydrated_metadata_projection_if_quiescent(&mut self) -> Result<bool, DfsError> {
        if !self.can_release_hydrated_metadata_projection() {
            return Ok(false);
        }
        self.collapse_exact_namespace_request_projection()?;
        // These fields are intentionally outside the live-operation predicate:
        // they are durable/reconstructable retry inventory or advisory caches,
        // not owners of an in-flight request. `pending_writes` above is the
        // exact live owner for an executable repair and prevents this reset.
        self.repair_attempt_records.clear();
        self.reads.transient_remote_read_pruned_locations.clear();
        self.replication.local_metadata_journal_cursors.clear();
        self.replication.recent_namespace_freshness_syncs.clear();
        self.last_events.clear();
        self.message_committed_work = DfsMessageCommittedWork::default();
        // Re-seed the exact durable cursor after clearing a possibly stale
        // hydrated high-water mark. This retains incremental replication
        // correctness without retaining the old projection.
        self.hydrate_file_systems_from_metadata_backend()?;
        Ok(true)
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
        self.coordinator.begin_message_repair_job_capture();
    }

    pub(crate) fn record_message_committed_work(&mut self, work: DfsMessageCommittedWork) {
        self.message_committed_work.merge(work);
        if let Some(tracker) = &self.committed_work_tracker {
            tracker.record(work);
        }
    }

    /// Retain durable work outside this service generation.
    ///
    /// Daemon request shells install one tracker before processing and consume
    /// it only after draining synchronous follow-up work. Maintenance uses the
    /// same contract across cancellable broad projections.
    pub fn set_committed_work_tracker(&mut self, tracker: DfsCommittedWorkTracker) {
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

    /// Borrow the request-scoped metadata capability for an operator mutation.
    /// Resident callers must still hold the daemon's ordinary metadata
    /// admission until this borrow and the surrounding lease are released.
    pub fn metadata_backend_mut(&mut self) -> Option<&mut (dyn DfsMetadataBackend + 'static)> {
        self.metadata_backend.as_deref_mut()
    }

    /// Count locally authoritative shards whose write lane is frozen without
    /// collecting unrelated content-store observability. Maintenance uses this
    /// as a safety gate and must not scan the content tree as a side effect.
    pub fn authoritative_write_frozen_shard_count(&self) -> Result<usize, DfsError> {
        let mut frozen_shard_count = 0usize;
        if let Some(backend) = self.metadata_backend.as_deref() {
            for status in backend.metadata_shard_statuses()? {
                if status.primary_node_id.as_ref() != Some(&self.node_id) {
                    continue;
                }
                if backend.metadata_shard_authoritative_writes_frozen(&status.shard_id)? {
                    frozen_shard_count = frozen_shard_count.saturating_add(1);
                }
            }
        }
        Ok(frozen_shard_count)
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
            .writes
            .pending_writes
            .values()
            .map(|pending| pending.stage_retries.len())
            .sum();
        let repair_retry_count = self
            .writes
            .pending_writes
            .values()
            .map(|pending| pending.repair_retries.len())
            .sum();
        let stage_inflight_count = self
            .writes
            .pending_writes
            .values()
            .map(|pending| pending.stage_inflight_request_ids.len())
            .sum();
        let repair_inflight_count = self
            .writes
            .pending_writes
            .values()
            .map(|pending| pending.repair_inflight_request_ids.len())
            .sum();
        let fetch_inflight_count = self
            .writes
            .pending_writes
            .values()
            .map(|pending| pending.fetch_inflight_from.len())
            .sum();
        let fetch_failed_source_count = self
            .writes
            .pending_writes
            .values()
            .map(|pending| pending.fetch_failed_from.len())
            .sum();
        let authoritative_write_frozen_shard_count =
            self.authoritative_write_frozen_shard_count()?;
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
            pending_write_count: self.writes.pending_writes.len(),
            pending_read_count: self.reads.pending_reads.len(),
            stage_retry_count,
            repair_retry_count,
            stage_inflight_count,
            repair_inflight_count,
            fetch_inflight_count,
            fetch_failed_source_count,
            active_replica_stage_count: self.writes.active_replica_stages.len(),
            metadata_replication_cursor_count: self.replication.metadata_replication_cursors.len(),
            pending_metadata_replication_count: self
                .replication
                .pending_metadata_replications
                .len(),
            pending_routed_write_count: self.writes.pending_routed_writes.len(),
            pending_remote_write_response_count: self.writes.pending_remote_write_responses.len(),
            pending_routed_namespace_op_count: self.pending_routed_namespace_ops.len(),
            cross_shard_namespace_lock_count: self.cross_shard_namespace_locks.len(),
            authoritative_write_frozen_shard_count,
            metadata_backend_configured: self.metadata_backend.is_some(),
            content: self.content.stats()?,
        })
    }
}

#[cfg(test)]
mod projection_release_tests {
    use std::collections::BTreeSet;
    use std::path::PathBuf;
    use std::time::{Instant, SystemTime, UNIX_EPOCH};

    use sinomo_core::DfsObjectPolicy;
    use sinomo_core::network::{InProcessNetwork, InProcessTransport};
    use time::OffsetDateTime;

    use super::*;
    use crate::transfer::PendingNetworkedWrite;
    use crate::{
        DfsMetadataShardAssignment, DfsMetadataShardConfig, DfsMetadataShardId, DfsObjectId,
        DfsVersion, LocalShardedMetadataBackend, directory_object, sifs_file_system_id,
    };

    fn temporary_root(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        std::env::temp_dir().join(format!("sinomo-dfs-{name}-{}-{unique}", std::process::id()))
    }

    fn service_at(root: &std::path::Path, node: NodeId) -> NetworkedDfsService<InProcessTransport> {
        NetworkedDfsService::new(
            InProcessNetwork::new().add_node(node),
            LocalContentStore::new(root.join("content")).expect("content store"),
        )
    }

    #[test]
    fn projection_release_quiescence_fails_closed_for_live_operation_state() {
        let root = temporary_root("projection-release-quiescence");
        let node = NodeId::from_legacy_named("node_projection_release_quiescence").expect("node");
        let mut service = service_at(&root, node);
        assert!(service.can_release_hydrated_metadata_projection());

        service.writes.pending_writes.insert(
            "intent-live".to_string(),
            PendingNetworkedWrite::new(
                DfsObjectId::from_legacy_named("object_projection_release").expect("object"),
                DfsVersion::from_legacy_named("version_projection_release").expect("version"),
                "content-hash".to_string(),
                0,
            ),
        );
        assert!(!service.can_release_hydrated_metadata_projection());
        assert!(
            !service
                .release_hydrated_metadata_projection_if_quiescent()
                .expect("blocked release")
        );
        service.writes.pending_writes.clear();

        service
            .writes
            .deferred_routed_create_parent_touches
            .insert("request-parent-touch".to_string());
        assert!(!service.can_release_hydrated_metadata_projection());
        service.writes.deferred_routed_create_parent_touches.clear();

        service.namespace_execution.record_prepare();
        assert!(!service.can_release_hydrated_metadata_projection());
        service.namespace_execution.finish_request();
        service.namespace_execution.record_job();
        assert!(!service.can_release_hydrated_metadata_projection());
        service.namespace_execution.finish_request();
        assert!(service.can_release_hydrated_metadata_projection());

        service.pending_namespace_request_recovery_cursor =
            Some(crate::DfsPendingNamespaceRequestPageCursor {
                updated_at: OffsetDateTime::UNIX_EPOCH,
                requester_node_id: service.node_id().clone(),
                request_id: "bounded-recovery-progress".to_string(),
            });
        let rebuilt = service
            .into_lazy_metadata_backend_service()
            .expect("lazy rebuild");
        assert_eq!(
            rebuilt
                .pending_namespace_request_recovery_cursor
                .as_ref()
                .map(|cursor| cursor.request_id.as_str()),
            Some("bounded-recovery-progress")
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn frozen_shard_gate_does_not_collect_or_persist_content_stats() {
        let root = temporary_root("frozen-shard-gate-read-only");
        let node = NodeId::from_legacy_named("node_frozen_shard_gate_read_only").expect("node");
        let service = service_at(&root, node);

        assert_eq!(
            service
                .authoritative_write_frozen_shard_count()
                .expect("read frozen shard gate"),
            0
        );
        assert!(
            !root.join("content").join("stats.sobj").exists(),
            "the maintenance safety gate must not collect content metrics"
        );
        assert!(
            !root.join("content").join("stats.lock").exists(),
            "the maintenance safety gate must not create the stats lock"
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn quiescent_exact_lane_releases_only_the_rebuildable_projection() {
        let root = temporary_root("projection-release-exact-lane");
        let node = NodeId::from_legacy_named("node_projection_release_exact").expect("node");
        let file_system_id = sifs_file_system_id();
        let shard_id = DfsMetadataShardId::new("root");
        let resident = LocalShardedMetadataBackend::new(
            root.join("metadata"),
            vec![DfsMetadataShardConfig {
                file_system_id: file_system_id.clone(),
                shard_id: shard_id.0.clone(),
                primary_node_id: node.clone(),
                replica_node_ids: Vec::new(),
                epoch: 1,
                assignment: DfsMetadataShardAssignment::Root,
                namespace_prepare_codec: Default::default(),
                default_policy: DfsObjectPolicy::ReplicatedEventual,
            }],
        )
        .expect("resident backend")
        .into_resident();
        let mut service = service_at(&root, node)
            .with_resident_metadata_shard_backend_lazy(
                resident
                    .scope_shard(&file_system_id, &shard_id)
                    .expect("exact scope")
                    .checkout()
                    .expect("exact checkout"),
            )
            .expect("exact service");
        let object = directory_object("/services/projection-release", service.node_id().clone());
        service.metadata.add_object(object.clone()).expect("object");
        let replica =
            NodeId::from_legacy_named("node_projection_release_replica").expect("replica");
        service.repair_attempt_records.insert(
            ("durable-repair".to_string(), replica.clone()),
            crate::DfsRepairAttemptRecord {
                intent_id: "durable-repair".to_string(),
                object_id: object.object_id.clone(),
                version: object.version.clone(),
                content_hash: "repair-content".to_string(),
                missing_replica_node_id: replica.clone(),
                attempts: 2,
                in_flight: false,
                last_attempt_unix_nanos: 3,
                next_retry_unix_nanos: 4,
                transfer_request_id: None,
                last_error: Some("retry later".to_string()),
            },
        );
        service.reads.transient_remote_read_pruned_locations.insert(
            (object.object_id.clone(), object.version.clone(), None),
            BTreeSet::from([replica.clone()]),
        );
        service
            .replication
            .local_metadata_journal_cursors
            .insert(shard_id.clone(), 91);
        service.replication.recent_namespace_freshness_syncs.insert(
            (file_system_id.clone(), shard_id.clone(), replica.clone()),
            Instant::now(),
        );
        service.pending_namespace_request_recovery_cursor =
            Some(crate::DfsPendingNamespaceRequestPageCursor {
                updated_at: OffsetDateTime::UNIX_EPOCH,
                requester_node_id: replica.clone(),
                request_id: "recovery-cursor".to_string(),
            });
        service
            .last_events
            .push(NetworkedDfsServiceEvent::RepairRetryFailed {
                reason: "retained observation".to_string(),
            });
        service
            .replication
            .metadata_replication_cursors
            .insert((replica.clone(), shard_id.clone()), 92);
        let committed_work_tracker = DfsCommittedWorkTracker::new();
        service.set_committed_work_tracker(committed_work_tracker.clone());
        service.record_message_committed_work(DfsMessageCommittedWork {
            publish_metadata: true,
            ..DfsMessageCommittedWork::default()
        });
        service.next_request_id = 73;

        assert!(service.can_release_hydrated_metadata_projection());
        assert!(
            service
                .release_hydrated_metadata_projection_if_quiescent()
                .expect("projection release")
        );
        assert!(service.metadata().object(&object.object_id).is_none());
        assert!(service.metadata.filesystems.contains_key(&file_system_id));
        assert!(service.repair_attempt_records.is_empty());
        assert!(
            service
                .reads
                .transient_remote_read_pruned_locations
                .is_empty()
        );
        assert_eq!(
            service
                .replication
                .local_metadata_journal_cursors
                .get(&shard_id),
            Some(&0)
        );
        assert!(
            service
                .replication
                .recent_namespace_freshness_syncs
                .is_empty()
        );
        assert_eq!(
            service
                .pending_namespace_request_recovery_cursor
                .as_ref()
                .map(|cursor| cursor.request_id.as_str()),
            Some("recovery-cursor")
        );
        assert!(service.last_events.is_empty());
        assert_eq!(
            service
                .replication
                .metadata_replication_cursors
                .get(&(replica, shard_id)),
            Some(&92)
        );
        assert_eq!(
            service.message_committed_work,
            DfsMessageCommittedWork::default()
        );
        assert!(committed_work_tracker.take().publish_metadata);
        assert_eq!(service.next_request_id, 73);
        assert!(service.metadata_backend().is_some());

        std::fs::remove_dir_all(root).ok();
    }
}
