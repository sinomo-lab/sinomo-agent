//! Core construction, accessors, routing helpers, and metrics for the networked DFS service.

use std::collections::BTreeMap;

use sinomo_core::network::NetworkError;
use sinomo_core::node::NodeId;

use crate::service_state::DfsMetadataBackendOwner;
use crate::{
    AsyncDfsMessageTransport, DfsCancellationFlag, DfsCommittedWorkTracker,
    DfsControlPlaneKvClient, DfsError, DfsFileSystem, DfsMessageCommittedWork, DfsMessageTransport,
    DfsMetadataBackend, DfsMetadataStore, DfsNetworkMessage, DfsObjectId, DfsOperationalMetrics,
    DfsRoutedMessage, DfsTransportIdentity, DistributedEventualCoordinator, LocalContentStore,
    NetworkedDfsService, NetworkedDfsServiceEvent, ResidentLocalMetadataShardLease,
    ResidentLocalShardedMetadataBackendLease, build_dfs_routed_message,
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
        self.note_mounted_call_send(&message);
        build_dfs_routed_message(
            &self.node_id,
            target_node_id,
            self.next_frame_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            self.next_message_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            message,
        )
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
            routed_lookup_cache: std::sync::Mutex::new(BTreeMap::new()),
            read_shard_snapshot: None,
            passive_fetch_started_at: BTreeMap::new(),
            pending_routed_namespace_ops: BTreeMap::new(),
            stable_namespace_reconciliation_results: BTreeMap::new(),
            stable_namespace_reconciliation_targets: BTreeMap::new(),
            namespace_execution: Default::default(),
            remote_namespace_phase: None,
            pending_namespace_request_recovery: None,
            completed_remote_namespace_phases: BTreeMap::new(),
            answered_remote_reads: BTreeMap::new(),
            read_handoff: Default::default(),
            read_yield_fresh_requests: Default::default(),
            file_sync_wait_deadline: None,
            prepared_remote_sources: BTreeMap::new(),
            publication_execution: Default::default(),
            publication_provider: Default::default(),
            source_adoption: Default::default(),
            pending_namespace_request_recovery_cursor: None,
            write_intent_maintenance_scan: std::sync::Arc::new(Default::default()),
            foreign_live_write_intents: Default::default(),
            maintenance_progress: std::sync::Arc::new(Default::default()),
            routed_namespace_settle_depth: 0,
            cross_shard_namespace_locks: Default::default(),
            last_events: Vec::new(),
            message_committed_work: DfsMessageCommittedWork::default(),
            committed_work_tracker: None,
            transport,
            repair_retry_tick: 0,
            next_frame_id: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(1)),
            next_message_id: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(1)),
            next_request_id: 1,
            cancellation: None,
            mounted_call: None,
            record_waits_parked: false,
            attribute_read_replans: false,
            attribute_read_resend: None,
            node_data_dir: None,
            held_write: None,
            unclaimed_answers: None,
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
            .send_routed_call_async(
                target_node_id,
                crate::message_transport::routed_call_with_content(routed_message)?,
            )
            .await
        {
            Ok(()) => Ok(true),
            Err(NetworkError::UnknownNode { .. }) => Ok(false),
            Err(error) => Err(DfsError::from_network(error)),
        }
    }
}

/// The cancellation and operation deadline a service had before it began
/// serving a request.
pub(crate) struct DfsServedRequestRestore {
    cancellation: Option<DfsCancellationFlag>,
    operation_deadline: Option<std::time::Instant>,
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    /// Share the live cross-shard namespace admission projection with another
    /// request shell over the same resident metadata generation.
    pub(crate) fn with_cross_shard_namespace_lock_registry(
        mut self,
        registry: crate::DfsCrossShardNamespaceLockRegistry,
    ) -> Self {
        self.cross_shard_namespace_locks = registry;
        self
    }

    #[cfg(test)]
    pub(crate) fn cross_shard_namespace_lock_registry(
        &self,
    ) -> crate::DfsCrossShardNamespaceLockRegistry {
        self.cross_shard_namespace_locks.clone()
    }

    pub(crate) fn clear_metadata_lookup_cache(&self) {
        if let Ok(mut cache) = self.metadata_lookup_cache.lock() {
            cache.clear();
        }
        if let Ok(mut cache) = self.routed_lookup_cache.lock() {
            cache.clear();
        }
    }

    /// The authority's answer for an exact path already routed in the active
    /// lookup-cache scope: `Some(None)` is an observed absence.
    pub(crate) fn cached_routed_lookup(
        &self,
        path: &str,
    ) -> Option<Option<crate::DfsObjectMetadata>> {
        if !self.metadata_lookup_cache_is_active() {
            return None;
        }
        let key = (
            crate::sifs_file_system_id(),
            crate::namespace::metadata_path(path),
        );
        self.routed_lookup_cache.lock().ok()?.get(&key).cloned()
    }

    /// The filesystem and path at which an authority showed `object_id`
    /// during the active lookup-cache scope. It is a routing hint only.
    pub(crate) fn routed_lookup_path_of_object(
        &self,
        object_id: &crate::DfsObjectId,
    ) -> Option<(crate::DfsFileSystemId, String)> {
        if !self.metadata_lookup_cache_is_active() {
            return None;
        }
        self.routed_lookup_cache
            .lock()
            .ok()?
            .values()
            .flatten()
            .find(|object| object.object_id == *object_id)
            .map(|object| (object.file_system_id.clone(), object.path.clone()))
    }

    /// Retain an authority's exact-path answer for the active scope. Outside
    /// a scope every lookup routes, so nothing is retained.
    pub(crate) fn remember_routed_lookup(
        &self,
        path: &str,
        object: &Option<crate::DfsObjectMetadata>,
    ) {
        if !self.metadata_lookup_cache_is_active() {
            return;
        }
        if let Ok(mut cache) = self.routed_lookup_cache.lock() {
            cache.insert(
                (
                    crate::sifs_file_system_id(),
                    crate::namespace::metadata_path(path),
                ),
                object.clone(),
            );
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

    /// Name the node this requester runs on, whose network verdicts its held
    /// writes watch.
    pub(crate) fn with_node_data_dir(
        mut self,
        data_dir: sinomo_core::local::SinomoDataDir,
    ) -> Self {
        self.node_data_dir = Some(data_dir);
        self
    }

    /// Install a cooperative cancellation flag. Distributed wait/retry loops
    /// (write settling, remote reads, routed namespace sync) observe the flag
    /// and stop with [`DfsError::Cancelled`] once it flips — the caller's way
    /// to abandon budgeted work promptly when the cluster link is degraded.
    pub fn with_cancellation(mut self, cancellation: DfsCancellationFlag) -> Self {
        self.set_cancellation(Some(cancellation));
        self
    }

    pub fn set_cancellation(&mut self, cancellation: Option<DfsCancellationFlag>) {
        self.cancellation = cancellation;
        // The transport's blocking sends watch the same cancellation.
        self.transport
            .set_send_cancellation(self.cancellation.clone());
    }

    /// Add a cancellation source without replacing one already installed by
    /// the caller which opened the service.
    pub fn add_cancellation(&mut self, cancellation: DfsCancellationFlag) {
        let cancellation = match self.cancellation.take() {
            Some(existing) => DfsCancellationFlag::linked([existing, cancellation]),
            None => cancellation,
        };
        self.set_cancellation(Some(cancellation));
    }

    /// Bound this service's waits by one served request: its requester's
    /// budget becomes the transport's operation deadline, which every wait and
    /// routed send already observes, and its requester's departure joins the
    /// service's cancellation. Returns what [`Self::leave_served_request`]
    /// restores.
    pub(crate) fn enter_served_request(
        &mut self,
        served: &crate::DfsServedRequestScope,
    ) -> DfsServedRequestRestore {
        let cancellation = self.cancellation.clone();
        self.add_cancellation(served.cancellation().clone());
        let observed = self.transport.operation_deadline();
        let replaced = self
            .transport
            .set_operation_deadline(served.deadline_within(observed));
        DfsServedRequestRestore {
            cancellation,
            operation_deadline: replaced.or(observed),
        }
    }

    pub(crate) fn leave_served_request(&mut self, restore: DfsServedRequestRestore) {
        self.set_cancellation(restore.cancellation);
        self.transport
            .set_operation_deadline(restore.operation_deadline);
    }

    /// Wake `waker` when this service's cancellation fires, while the
    /// returned registration lives. Register before the wait's final
    /// [`Self::check_cancelled`].
    pub(crate) fn register_cancellation_waker(
        &self,
        waker: &crate::service_state::DfsCancellationWaker,
    ) -> Option<crate::service_state::DfsCancellationWakerRegistration> {
        self.cancellation
            .as_ref()
            .map(|cancellation| cancellation.register_waker(waker))
    }

    /// Wait until a re-send is due, or until this service's cancellation
    /// fires, whichever is first.
    pub(crate) fn wait_for_resend(&self, wait: std::time::Duration) {
        let due = std::sync::Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let cancelled = std::sync::Arc::clone(&due);
        let waker: crate::service_state::DfsCancellationWaker = std::sync::Arc::new(move || {
            *cancelled
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
            cancelled.1.notify_all();
        });
        let _registration = self.register_cancellation_waker(&waker);
        if self
            .cancellation
            .as_ref()
            .is_some_and(DfsCancellationFlag::is_cancelled)
        {
            return;
        }
        let woken = due
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        drop(
            due.1
                .wait_timeout_while(woken, wait, |woken| !*woken)
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
    }

    pub(crate) fn check_cancelled(&self, operation: &'static str) -> Result<(), DfsError> {
        if self
            .cancellation
            .as_ref()
            .is_some_and(DfsCancellationFlag::is_cancelled)
        {
            return Err(DfsError::Cancelled { operation });
        }
        // A mounted call's run that handed its record wait back is over.
        self.check_mounted_call_running()
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
    pub(crate) fn with_boxed_metadata_backend_for_commit_promotion_recovery(
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

    /// The configuration this service's control plane holds for the current
    /// operation, or `None` without a control plane or publication.
    /// sinomo:kv-configuration: Borrow the control plane's published configuration snapshot without starting a KV read of its own.
    pub fn published_configuration_snapshot(
        &self,
    ) -> Result<Option<std::sync::Arc<crate::DfsConfigurationSnapshot>>, DfsError> {
        match self.control_plane.as_ref() {
            Some(control_plane) => control_plane
                .configuration_snapshot()
                .map_err(DfsError::from),
            None => Ok(None),
        }
    }

    /// Initialize a caller that delegates namespace work to its node's Main.
    /// It keeps no local metadata store: Main alone owns and recovers the
    /// node's shards, reads go to their authorities and writes to Main. This
    /// initializer seeds only SIFS configuration.
    pub fn into_main_sifs_client(mut self) -> Result<Self, DfsError> {
        if !self.transport.delegates_local_namespace_authority() || self.control_plane.is_none() {
            return Err(DfsError::Metadata {
                message: "Main SIFS client initialization requires delegated namespace routing and an existing control-plane client".into(),
            });
        }
        self.bootstrap_distributed_sifs()
            .map_err(|source| match source {
                crate::DfsPosixError::Dfs(error) => error,
                other => DfsError::Metadata {
                    message: other.to_string(),
                },
            })?;
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
    #[cfg(test)]
    pub(crate) fn with_boxed_metadata_backend_for_read_only_requests(
        mut self,
        backend: Box<dyn DfsMetadataBackend>,
    ) -> Self {
        self.metadata_backend = Some(DfsMetadataBackendOwner::Owned(backend));
        self
    }

    /// Attach one checked-out resident shard and hydrate the bounded mutation
    /// lane without transferring ownership of the process-wide backend pool.
    pub(crate) fn with_resident_metadata_shard_backend_lazy(
        mut self,
        backend: ResidentLocalMetadataShardLease,
    ) -> Result<Self, DfsError> {
        self.metadata_backend = Some(DfsMetadataBackendOwner::ResidentShard(backend));
        self.hydrate_file_systems_from_metadata_backend()?;
        Ok(self)
    }

    /// Attach the checked-out resident router and hydrate the compatibility
    /// lane used for operations which cannot prove one exact shard.
    pub(crate) fn with_resident_sharded_metadata_backend_lazy(
        mut self,
        backend: ResidentLocalShardedMetadataBackendLease,
    ) -> Result<Self, DfsError> {
        self.metadata_backend = Some(DfsMetadataBackendOwner::ResidentSharded(backend));
        self.hydrate_file_systems_from_metadata_backend()?;
        Ok(self)
    }

    /// Attach an exact resident backend immediately before one served request.
    pub(crate) fn attach_resident_metadata_shard_backend(
        &mut self,
        backend: ResidentLocalMetadataShardLease,
    ) -> Result<(), DfsError> {
        self.attach_resident_metadata_backend(DfsMetadataBackendOwner::ResidentShard(backend))
    }

    /// Attach the resident all-shard router immediately before one global
    /// compatibility request or maintenance scope.
    pub(crate) fn attach_resident_sharded_metadata_backend(
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
    pub(crate) fn release_resident_metadata_backend(&mut self) -> Result<(), DfsError> {
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
    pub(crate) fn into_lazy_metadata_backend_service(mut self) -> Result<Self, DfsError> {
        let metadata_backend = self.metadata_backend.take();
        let control_plane = self.control_plane.take();
        let Self {
            transport,
            content,
            cross_shard_namespace_locks,
            placement_snapshot_cache,
            content_pin_node_ids,
            replication,
            pending_namespace_request_recovery_cursor,
            write_intent_maintenance_scan,
            maintenance_progress,
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
        // These are the same host-owned placement inputs, not hydrated metadata.
        // Dropping the shared cache disconnects later writes from live placement
        // and falls back to standalone owner normalization, which can widen N.
        service.placement_snapshot_cache = placement_snapshot_cache;
        service.content_pin_node_ids = content_pin_node_ids;
        service.replication = replication.into_lazy();
        // This cursor is tiny but provides bounded-scan fairness across routine
        // maintenance ticks. Restarting at the first durable page after every
        // lazy rebuild could starve later locally authoritative requests behind
        // a large prefix of foreign requests.
        service.pending_namespace_request_recovery_cursor =
            pending_namespace_request_recovery_cursor;
        service.write_intent_maintenance_scan = write_intent_maintenance_scan;
        service.maintenance_progress = maintenance_progress;
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
    pub(crate) fn can_release_hydrated_metadata_projection(&self) -> bool {
        let metadata_lookup_scope_quiescent = self
            .metadata_lookup_cache_depth
            .lock()
            .is_ok_and(|depth| *depth == 0);
        let committed_work_has_durable_owner = self.message_committed_work
            == DfsMessageCommittedWork::default()
            || self.committed_work_tracker.is_some();

        metadata_lookup_scope_quiescent
            && !self.coordinator.has_open_intents()
            && self.writes.is_quiescent()
            && self.reads.is_quiescent()
            && self.replication.is_quiescent()
            && self.pending_routed_namespace_ops.is_empty()
            && self.namespace_execution.is_quiescent()
            && self.pending_namespace_request_recovery.is_none()
            && self.routed_namespace_settle_depth == 0
            && committed_work_has_durable_owner
    }

    /// Whether a routed write this service served is unfinished: it still
    /// awaits receipts or its commit, or still owes its requester a reply.
    pub(crate) fn has_unfinished_routed_writes(&self) -> bool {
        self.coordinator.has_open_intents()
            || !self.writes.pending_writes.is_empty()
            || !self.writes.pending_remote_write_responses.is_empty()
    }

    /// The write intents of [`Self::has_unfinished_routed_writes`].
    pub(crate) fn unfinished_routed_write_intent_ids(&self) -> std::collections::BTreeSet<String> {
        self.coordinator
            .open_intent_ids()
            .chain(self.writes.pending_writes.keys())
            .chain(self.writes.pending_remote_write_responses.keys())
            .cloned()
            .collect()
    }

    /// Release a quiescent exact-shard lane's rebuildable metadata mirror.
    ///
    /// `false` means some live operation state still owns the projection and
    /// nothing was changed. Broad maintenance services use
    /// [`Self::can_release_hydrated_metadata_projection`] before consuming the
    /// service through [`Self::into_lazy_metadata_backend_service`]; exact
    /// request lanes can use this mutating form because their resident backend
    /// lease remains attached until the caller's ordinary release boundary.
    pub(crate) fn release_hydrated_metadata_projection_if_quiescent(
        &mut self,
    ) -> Result<bool, DfsError> {
        if !self.can_release_hydrated_metadata_projection() {
            return Ok(false);
        }
        self.collapse_exact_namespace_request_projection()?;
        // These fields are intentionally outside the live-operation predicate:
        // they are durable/reconstructable retry inventory or advisory caches,
        // not owners of an in-flight request. `pending_writes` above is the
        // exact live owner for an executable repair and prevents this reset.
        self.reads.transient_remote_read_pruned_locations.clear();
        self.replication.local_metadata_journal_cursors.clear();
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
        let stage_retry_count = self
            .writes
            .pending_writes
            .values()
            .map(|pending| pending.stage_retries.len())
            .sum();
        let stage_inflight_count = self
            .writes
            .pending_writes
            .values()
            .map(|pending| pending.stage_inflight_request_ids.len())
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
        let repair = crate::repair_executor::repair_executor_work_for_node(&self.node_id)
            .unwrap_or_default();
        Ok(DfsOperationalMetrics {
            node_id: self.node_id.clone(),
            file_system_count: self.metadata.filesystems.len(),
            object_count: self.metadata.objects.len(),
            coordinator_intent_count: self.coordinator.intent_count(),
            coordinator_receipt_count: self.coordinator.receipt_count(),
            coordinator_committed_version_count: self.coordinator.committed_version_count(),
            // Repair attempts live in the node's repair executor, not in a
            // service lane; this lane reports its coordinator's derived view.
            repair_job_count: self.coordinator.repair_jobs().len(),
            repair_attempt_count: repair.tracked_need_count,
            pending_write_count: self.writes.pending_writes.len(),
            pending_read_count: self.reads.pending_reads.len(),
            stage_retry_count,
            repair_retry_count: repair.backoff_count,
            stage_inflight_count,
            repair_inflight_count: repair.in_flight_count,
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
    use time::OffsetDateTime;

    use crate::transfer::PendingNetworkedWrite;
    use crate::{DfsObjectId, DfsVersion};

    #[test]
    fn projection_release_quiescence_fails_closed_for_live_operation_state() {
        let (runtime, mut service, _) = crate::test_support::current_native_metadata_service(
            "native-projection-release-quiescence",
            1,
            "/services",
        )
        .expect("actual current runtime");
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

        drop(rebuilt);
        runtime.close().expect("native owner drain");
    }

    #[test]
    fn frozen_shard_gate_does_not_collect_or_persist_content_stats() {
        let (runtime, service, config) = crate::test_support::current_native_metadata_service(
            "native-frozen-shard-gate-read-only",
            1,
            "/services",
        )
        .expect("actual current runtime");
        let content_root = runtime
            .data_dir(&config.primary_node_id)
            .dfs_dir()
            .join("content");
        let before =
            ["stats.sobj", "stats.lock"].map(|name| std::fs::read(content_root.join(name)).ok());

        assert_eq!(
            service
                .authoritative_write_frozen_shard_count()
                .expect("read frozen shard gate"),
            0
        );
        assert_eq!(
            ["stats.sobj", "stats.lock"].map(|name| std::fs::read(content_root.join(name)).ok()),
            before,
            "the normal safety gate must leave current content counters unchanged",
        );
        drop(service);
        runtime.close().expect("native owner drain");
    }
}
