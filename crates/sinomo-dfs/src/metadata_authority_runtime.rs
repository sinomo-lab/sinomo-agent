use crate::write_configuration_projection::{
    DFS_WRITE_CONFIGURATION_PREFIXES, DfsWriteConfigurationState,
};
use crate::{
    DfsAuthoritativePrefixProjection, DfsError, DfsFileSystemId, DfsMetadataReplicaShardContext,
    DfsMetadataShardConfig, DfsMetadataShardId, DfsNamespaceCommand,
    dfs_metadata_authority_key_is_exact, metadata_replica_context_matches_authority_entries,
    metadata_shard_index_from_authority_entries, namespace_command_matches_authority_index,
};
use sinomo_core::kv::{
    ClusterKvCommittedDelta, KvAuthorityProof, KvEntry, KvEvent, KvLocalRead, KvRevision,
    KvSnapshot,
};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    Arc, Mutex, OnceLock, Weak,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

pub const DFS_METADATA_AUTHORITY_PREFIX: &str = "/dfs/filesystems/";

pub type DfsMetadataAuthorityReadFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<sinomo_core::kv::KvAuthoritativeRead<Vec<KvEntry>>, DfsError>>
            + Send
            + 'a,
    >,
>;

pub type DfsMetadataAuthorityTask = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Host capabilities required by the DFS metadata-authority runtime.
///
/// The owner component retains all projection and refresh state. The embedding
/// daemon supplies only a linearized prefix read, task spawning on its runtime,
/// and the wake edge emitted after an exact topology change.
pub trait DfsMetadataAuthorityHost: Send + Sync {
    fn read_authoritative_prefix(&self) -> DfsMetadataAuthorityReadFuture<'_>;
    fn supports_write_configuration_projection(&self) -> bool {
        false
    }
    fn allows_unconfigured_single_node_write_configuration(
        &self,
        _authority: &KvAuthorityProof,
        _floor: &KvLocalRead<Option<KvEntry>>,
    ) -> bool {
        false
    }
    fn read_authoritative_configuration_prefix<'a>(
        &'a self,
        prefix: &'a str,
    ) -> DfsMetadataAuthorityReadFuture<'a> {
        if prefix == DFS_METADATA_AUTHORITY_PREFIX {
            return self.read_authoritative_prefix();
        }
        Box::pin(async {
            Err(DfsError::Network {
                message: "host has no authoritative write configuration projection".into(),
            })
        })
    }
    fn write_configuration_floor(&self) -> Result<KvLocalRead<Option<KvEntry>>, DfsError> {
        Err(DfsError::Network {
            message: "host has no resident configuration authority floor".into(),
        })
    }
    fn spawn(&self, task: DfsMetadataAuthorityTask);
    fn authority_changed(&self);
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct DfsMetadataAuthorityProjection {
    revision: Option<KvRevision>,
    required_revision: KvRevision,
    topology_comparison_baseline: bool,
    entries: Vec<KvEntry>,
    metadata_shards_by_file_system: BTreeMap<String, Result<Vec<DfsMetadataShardConfig>, String>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DfsMetadataAuthorityDeltaObservation {
    pub accepted: bool,
    pub authority_changed: bool,
    pub proof_withdrawn: bool,
}

#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsMetadataAuthorityDebugSnapshot {
    pub revision: Option<KvRevision>,
    pub required_revision: KvRevision,
    pub topology_comparison_baseline: bool,
    pub entry_count: usize,
    pub metadata_file_system_count: usize,
}

impl DfsMetadataAuthorityDeltaObservation {
    const fn accepted(authority_changed: bool) -> Self {
        Self {
            accepted: true,
            authority_changed,
            proof_withdrawn: false,
        }
    }

    const fn invalidated(proof_withdrawn: bool) -> Self {
        Self {
            accepted: false,
            authority_changed: false,
            proof_withdrawn,
        }
    }
}

/// Process-resident DFS authority projection and generation state.
///
/// The embedding runtime supplies ReadIndex refresh and wake hooks. This owner
/// component decides whether a proof or committed delta is complete, applies
/// resource bounds, advances topology generations, and answers exact shard or
/// namespace authority queries.
pub struct DfsMetadataAuthorityState {
    projection: Mutex<DfsMetadataAuthorityProjection>,
    projection_generation: AtomicU64,
    generation: AtomicU64,
    storage_generation: AtomicU64,
    #[cfg(any(test, feature = "test-support"))]
    projection_rebuild_count: AtomicU64,
    #[cfg(any(test, feature = "test-support"))]
    projection_materialization_count: AtomicU64,
}

#[derive(Clone, Copy)]
struct DfsSingleNodeBootstrapAuthority {
    authority: KvAuthorityProof,
    observed_revision: KvRevision,
}

pub struct DfsMetadataAuthorityRuntime {
    state: DfsMetadataAuthorityState,
    write_configuration: Mutex<DfsWriteConfigurationState>,
    write_configuration_requested: AtomicBool,
    write_configuration_has_committed_config: AtomicBool,
    single_node_bootstrap_authority: Mutex<Option<DfsSingleNodeBootstrapAuthority>>,
    refresh_requested: AtomicBool,
    host: Mutex<Option<Arc<dyn DfsMetadataAuthorityHost>>>,
    delta_subscription: OnceLock<sinomo_core::cluster::ClusterKvCommittedDeltaSubscription>,
    snapshot_subscription: OnceLock<sinomo_core::cluster::ClusterKvCommittedSnapshotSubscription>,
    observation_subscription: OnceLock<sinomo_core::kv::KvCommittedObservationSubscription>,
    discontinuity_generation: AtomicU64,
    refresh_in_flight: AtomicBool,
    configuration_refresh_in_flight: AtomicBool,
    refresh_release_pending: AtomicBool,
    refresh_completion: tokio::sync::watch::Sender<u64>,
    #[cfg(any(test, feature = "test-support"))]
    refresh_before_claim: Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>,
    #[cfg(any(test, feature = "test-support"))]
    refresh_count: AtomicU64,
}

#[derive(Debug)]
pub struct DfsMetadataAuthorityRuntimeRegistry<K> {
    name: &'static str,
    runtimes: Mutex<BTreeMap<K, Weak<DfsMetadataAuthorityRuntime>>>,
}

impl<K> DfsMetadataAuthorityRuntimeRegistry<K>
where
    K: Clone + Ord,
{
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            runtimes: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn runtime(&self, key: K) -> Result<(Arc<DfsMetadataAuthorityRuntime>, bool), DfsError> {
        let mut runtimes = self.runtimes.lock().map_err(|_| DfsError::Network {
            message: format!("{} lock poisoned", self.name),
        })?;
        runtimes.retain(|_, runtime| runtime.strong_count() > 0);
        if let Some(runtime) = runtimes.get(&key).and_then(Weak::upgrade) {
            return Ok((runtime, false));
        }
        let runtime = Arc::new(DfsMetadataAuthorityRuntime::new());
        runtimes.insert(key, Arc::downgrade(&runtime));
        Ok((runtime, true))
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn live_keys(&self) -> Result<Vec<K>, DfsError> {
        let mut runtimes = self.runtimes.lock().map_err(|_| DfsError::Network {
            message: format!("{} lock poisoned", self.name),
        })?;
        runtimes.retain(|_, runtime| runtime.strong_count() > 0);
        Ok(runtimes.keys().cloned().collect())
    }
}

struct DfsMetadataAuthorityRefreshOwner {
    runtime: Arc<DfsMetadataAuthorityRuntime>,
}

impl Drop for DfsMetadataAuthorityRefreshOwner {
    fn drop(&mut self) {
        // Cancellation may drop this outer guard before the configuration
        // future. Retain singleflight until its token is released as well.
        self.runtime
            .refresh_release_pending
            .store(true, Ordering::Release);
        if !self
            .runtime
            .configuration_refresh_in_flight
            .load(Ordering::Acquire)
        {
            self.runtime.complete_pending_refresh_release();
        }
    }
}

struct DfsWriteConfigurationRefreshOwner {
    runtime: Arc<DfsMetadataAuthorityRuntime>,
    token: u64,
}

impl Drop for DfsWriteConfigurationRefreshOwner {
    fn drop(&mut self) {
        if let Ok(mut configuration) = self.runtime.write_configuration.lock() {
            configuration.finish_refresh(self.token);
        }
        self.runtime
            .configuration_refresh_in_flight
            .store(false, Ordering::Release);
        self.runtime.complete_pending_refresh_release();
    }
}

impl std::fmt::Debug for DfsMetadataAuthorityRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DfsMetadataAuthorityRuntime")
            .field("generation", &self.generation())
            .field("projection_generation", &self.projection_generation())
            .finish_non_exhaustive()
    }
}

impl Default for DfsMetadataAuthorityRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl DfsMetadataAuthorityRuntime {
    pub fn new() -> Self {
        Self {
            state: DfsMetadataAuthorityState::new(),
            write_configuration: Mutex::new(DfsWriteConfigurationState::new()),
            write_configuration_requested: AtomicBool::new(false),
            write_configuration_has_committed_config: AtomicBool::new(false),
            single_node_bootstrap_authority: Mutex::new(None),
            refresh_requested: AtomicBool::new(false),
            host: Mutex::new(None),
            delta_subscription: OnceLock::new(),
            snapshot_subscription: OnceLock::new(),
            observation_subscription: OnceLock::new(),
            discontinuity_generation: AtomicU64::new(0),
            refresh_in_flight: AtomicBool::new(false),
            configuration_refresh_in_flight: AtomicBool::new(false),
            refresh_release_pending: AtomicBool::new(false),
            refresh_completion: tokio::sync::watch::channel(0).0,
            #[cfg(any(test, feature = "test-support"))]
            refresh_before_claim: Mutex::new(None),
            #[cfg(any(test, feature = "test-support"))]
            refresh_count: AtomicU64::new(0),
        }
    }

    pub fn attach_host(&self, host: Arc<dyn DfsMetadataAuthorityHost>) -> bool {
        if host.supports_write_configuration_projection() {
            self.write_configuration_requested
                .store(true, Ordering::Release);
        }
        let Ok(mut attached) = self.host.lock() else {
            return false;
        };
        let newly_attached = attached.is_none();
        *attached = Some(host);
        newly_attached
    }

    pub fn attach_delta_subscription(
        &self,
        subscription: sinomo_core::cluster::ClusterKvCommittedDeltaSubscription,
    ) -> Result<(), sinomo_core::cluster::ClusterKvCommittedDeltaSubscription> {
        self.delta_subscription.set(subscription)
    }

    pub fn attach_snapshot_subscription(
        &self,
        subscription: sinomo_core::cluster::ClusterKvCommittedSnapshotSubscription,
    ) -> Result<(), sinomo_core::cluster::ClusterKvCommittedSnapshotSubscription> {
        self.snapshot_subscription.set(subscription)
    }

    pub fn attach_observation_subscription(
        &self,
        subscription: sinomo_core::kv::KvCommittedObservationSubscription,
    ) -> Result<(), sinomo_core::kv::KvCommittedObservationSubscription> {
        self.observation_subscription.set(subscription)
    }

    pub fn generation(&self) -> u64 {
        self.state.generation()
    }

    pub fn projection_generation(&self) -> u64 {
        self.state.projection_generation()
    }

    /// Physical metadata-store lifecycle. Authority proof and policy changes
    /// retain stores while continuing to advance the separate authority fence.
    pub fn storage_generation(&self) -> u64 {
        self.state.storage_generation()
    }

    pub(crate) fn lifecycle_generations(&self) -> Result<(u64, u64), DfsError> {
        self.state.lifecycle_generations()
    }

    pub fn cached_metadata_shards(
        &self,
        file_system_id: &DfsFileSystemId,
        expected_projection_generation: u64,
    ) -> Result<Option<Vec<DfsMetadataShardConfig>>, sinomo_core::kv::KvError> {
        self.state
            .cached_metadata_shards(file_system_id, expected_projection_generation)
    }

    pub fn shard_projection_state(
        &self,
        file_system_id: &DfsFileSystemId,
        shard_id: &DfsMetadataShardId,
    ) -> (u64, bool) {
        self.state.shard_projection_state(file_system_id, shard_id)
    }

    pub fn namespace_read_stamp_state(
        &self,
        command: &DfsNamespaceCommand,
    ) -> Result<(u64, bool), DfsError> {
        self.state.namespace_read_stamp_state(command)
    }

    pub fn replica_context_state(
        &self,
        context: &DfsMetadataReplicaShardContext,
    ) -> Result<(u64, bool), DfsError> {
        self.state.replica_context_state(context)
    }

    pub fn current_complete_prefix(self: &Arc<Self>) -> Option<DfsAuthoritativePrefixProjection> {
        let retained = self.state.current_complete_prefix();
        if retained.is_none() {
            self.request_authoritative_refresh();
        }
        retained
    }

    pub fn current_complete_prefix_for_generation(
        self: &Arc<Self>,
        expected_generation: u64,
    ) -> Option<DfsAuthoritativePrefixProjection> {
        let retained = self
            .state
            .retained_complete_prefix_for_generation(expected_generation);
        if retained.is_none() && self.generation() == expected_generation {
            self.request_authoritative_refresh();
        }
        retained
    }

    pub fn retained_complete_prefix_for_generation(
        &self,
        expected_generation: u64,
    ) -> Option<DfsAuthoritativePrefixProjection> {
        self.state
            .retained_complete_prefix_for_generation(expected_generation)
    }

    pub fn retained_complete_prefix_revision_for_generation(
        &self,
        expected_generation: u64,
    ) -> Option<KvRevision> {
        self.state
            .retained_complete_prefix_revision_for_generation(expected_generation)
    }

    pub fn load_complete_prefix_for_generation(
        self: &Arc<Self>,
        expected_generation: u64,
    ) -> Option<DfsAuthoritativePrefixProjection> {
        self.current_complete_prefix_for_generation(expected_generation)
    }

    pub fn current_complete_prefix_revision(self: &Arc<Self>) -> Option<KvRevision> {
        let revision = self.state.current_complete_prefix_revision();
        if revision.is_none()
            || (self.write_configuration_requested.load(Ordering::Acquire)
                && self.write_configuration_needs_refresh())
        {
            self.request_authoritative_refresh();
        }
        revision
    }

    pub fn install_authoritative_projection(
        &self,
        projection: DfsAuthoritativePrefixProjection,
    ) -> bool {
        let Some(authority_changed) = self.state.install_authoritative_projection(projection)
        else {
            return false;
        };
        if authority_changed {
            self.host()
                .into_iter()
                .for_each(|host| host.authority_changed());
        }
        true
    }

    pub fn require_authoritative_revision(self: &Arc<Self>, revision: KvRevision) {
        if self.state.require_authoritative_revision(revision) {
            self.request_authoritative_refresh();
        }
    }

    pub fn observe_delta(self: &Arc<Self>, delta: &ClusterKvCommittedDelta) -> bool {
        self.observe_delta_observation(delta).accepted
    }

    pub fn observe_delta_observation(
        self: &Arc<Self>,
        delta: &ClusterKvCommittedDelta,
    ) -> DfsMetadataAuthorityDeltaObservation {
        let config_key = sinomo_core::kv::cluster_kv_raft_config_key();
        if delta
            .final_key_states
            .iter()
            .any(|state| state.key == config_key && state.entry.is_some())
        {
            self.write_configuration_has_committed_config
                .store(true, Ordering::Release);
            if self
                .host()
                .is_some_and(|host| host.supports_write_configuration_projection())
            {
                self.write_configuration_requested
                    .store(true, Ordering::Release);
            }
        }
        if delta
            .events
            .iter()
            .any(|event| event.key == config_key || event.key.starts_with("/cluster/members/"))
        {
            if let Ok(mut authority) = self.single_node_bootstrap_authority.lock() {
                *authority = None;
            }
        }
        let mut observation = DfsMetadataAuthorityDeltaObservation::invalidated(false);
        let provider_configuration_touched = delta.events.iter().any(|event| {
            event.key.starts_with(DFS_WRITE_CONFIGURATION_PREFIXES[1])
                || event.key.starts_with(DFS_WRITE_CONFIGURATION_PREFIXES[2])
        });
        let mut configuration_change = false;
        let mut configuration_became_ready = false;
        let configuration_ready = self
            .write_configuration
            .lock()
            .is_ok_and(|mut configuration| {
                observation = self.state.observe_delta(delta);
                let was_ready = configuration.snapshot().is_some();
                let new_delta = delta.revision > configuration.observed_revision();
                let ready = configuration.observe_delta(delta);
                configuration_became_ready = !was_ready && ready;
                configuration_change = self.write_configuration_requested.load(Ordering::Acquire)
                    && ((new_delta && provider_configuration_touched) || (was_ready && !ready));
                if let Some(published) = configuration.snapshot() {
                    match self
                        .state
                        .install_write_configuration_projection(&published, configuration_change)
                    {
                        Ok(authority_changed) => configuration_change |= authority_changed,
                        Err(_) => {
                            if was_ready || configuration_change {
                                configuration_change = true;
                                let _ = self.state.advance_configuration_generation();
                            }
                            configuration.invalidate(delta.revision);
                            return false;
                        }
                    }
                } else if configuration_change {
                    // Provider and zone configuration fences authority but cannot
                    // retire or reopen a physical metadata store.
                    if self.state.advance_configuration_generation().is_err() {
                        configuration.invalidate(delta.revision);
                        return false;
                    }
                }
                ready
            });
        if let Ok(mut bootstrap) = self.single_node_bootstrap_authority.lock()
            && let Some(anchor) = bootstrap.as_mut()
        {
            if !observation.accepted
                || anchor.observed_revision < delta.compacted_revision
                || !kv_events_cover_revision_range(
                    anchor.observed_revision,
                    delta.revision,
                    &delta.events,
                )
            {
                *bootstrap = None;
            } else {
                // The original native pristine certificate never changes. Only
                // complete committed history can extend its absence evidence.
                anchor.observed_revision = anchor.observed_revision.max(delta.revision);
            }
        }
        if observation.authority_changed || configuration_change {
            self.host()
                .into_iter()
                .for_each(|host| host.authority_changed());
        }
        if configuration_became_ready && configuration_ready {
            // A coherent pending bundle can finish on a committed delta after
            // its read owner has already returned. Wake its existing waiters.
            self.refresh_completion
                .send_modify(|generation| *generation = generation.saturating_add(1));
        }
        if self.write_configuration_requested.load(Ordering::Acquire)
            && !configuration_ready
            && self.write_configuration_needs_refresh()
            && (configuration_change
                || !observation.accepted
                || self
                    .write_configuration_has_committed_config
                    .load(Ordering::Acquire))
        {
            self.request_authoritative_refresh();
        }
        observation.authority_changed |= configuration_change;
        observation
    }

    /// Snapshot imports can replace the observed view without emitting a
    /// contiguous delta. Treat them only as invalidation evidence; the normal
    /// bounded ReadIndex refresh is still required to establish authority.
    pub fn observe_snapshot(self: &Arc<Self>, snapshot: &KvSnapshot) {
        if let Ok(mut bootstrap) = self.single_node_bootstrap_authority.lock()
            && bootstrap
                .as_ref()
                .is_some_and(|anchor| snapshot.revision > anchor.observed_revision)
        {
            // A complete filesystem scope cannot prove that an unseen Raft
            // config was never committed and deleted before this replacement.
            *bootstrap = None;
        }
        if snapshot
            .get(&sinomo_core::kv::cluster_kv_raft_config_key())
            .is_some()
            && self
                .host()
                .is_some_and(|host| host.supports_write_configuration_projection())
        {
            self.write_configuration_has_committed_config
                .store(true, Ordering::Release);
            self.write_configuration_requested
                .store(true, Ordering::Release);
        }
        if self.write_configuration_requested.load(Ordering::Acquire)
            && let Ok(mut configuration) = self.write_configuration.lock()
        {
            // A replacement snapshot cannot certify scope continuity. The
            // existing shared refresh owner restores a complete configuration.
            let behind = configuration.observed_revision() < snapshot.revision;
            if behind {
                if configuration.snapshot().is_some() {
                    let _ = self.state.advance_configuration_generation();
                }
                configuration.invalidate(snapshot.revision);
            }
        }
        if self.state.observe_snapshot(snapshot) {
            self.request_authoritative_refresh();
        }
        if self.write_configuration_requested.load(Ordering::Acquire)
            && self.write_configuration_needs_refresh()
        {
            self.request_authoritative_refresh();
        }
    }

    /// Withdraw every retained admission proof when canonical publication
    /// cannot supply its complete observation. Equal revision hints cannot
    /// establish continuity. Retain the topology solely as a comparison base;
    /// only a fresh native authoritative read may restore readiness.
    /// Unknown projection-repair hints withdraw at the retained required floor;
    /// they never manufacture a canonical revision or a future native barrier.
    pub(crate) fn observe_discontinuity_hint(
        self: &Arc<Self>,
        revision: Option<KvRevision>,
    ) -> DfsMetadataAuthorityDeltaObservation {
        let revision = revision.unwrap_or_else(|| {
            self.state
                .projection
                .lock()
                .map(|projection| projection.required_revision)
                .unwrap_or(KvRevision::ZERO)
        });
        self.observe_discontinuity(revision)
    }

    pub fn observe_discontinuity(
        self: &Arc<Self>,
        revision: KvRevision,
    ) -> DfsMetadataAuthorityDeltaObservation {
        let mut observation = match self.write_configuration.lock() {
            Ok(mut configuration) => {
                DfsMetadataAuthorityState::advance(&self.discontinuity_generation);
                let mut observation = self.state.observe_discontinuity(revision);
                let configuration_was_ready = configuration.snapshot().is_some();
                configuration.invalidate(revision);
                if configuration_was_ready {
                    let _ = self.state.advance_configuration_generation();
                    observation.authority_changed = true;
                    observation.proof_withdrawn = true;
                }
                observation
            }
            Err(_) => {
                DfsMetadataAuthorityState::advance(&self.discontinuity_generation);
                self.state.observe_discontinuity(revision)
            }
        };
        let bootstrap_withdrawn = self
            .single_node_bootstrap_authority
            .lock()
            .is_ok_and(|mut bootstrap| bootstrap.take().is_some());
        observation.proof_withdrawn |= bootstrap_withdrawn;
        if bootstrap_withdrawn
            && self
                .host()
                .is_some_and(|host| host.supports_write_configuration_projection())
        {
            // Restoring a pristine bootstrap certificate also requires the
            // native floor/coherence checks, not only a filesystem prefix.
            self.write_configuration_requested
                .store(true, Ordering::Release);
        }
        if observation.authority_changed {
            if let Some(host) = self.host() {
                host.authority_changed();
            }
        }
        self.request_authoritative_refresh();
        observation
    }

    fn write_configuration_needs_refresh(&self) -> bool {
        self.write_configuration
            .lock()
            .is_ok_and(|configuration| configuration.needs_refresh())
    }

    fn handoff_pending_authoritative_refresh(self: &Arc<Self>) {
        if !self.refresh_in_flight.load(Ordering::Acquire)
            && self.refresh_requested.load(Ordering::Acquire)
            && (self.state.current_complete_prefix_revision().is_none()
                || (self.write_configuration_requested.load(Ordering::Acquire)
                    && self.write_configuration_needs_refresh()))
        {
            self.request_authoritative_refresh();
        }
    }

    fn complete_pending_refresh_release(self: &Arc<Self>) {
        if !self.refresh_release_pending.swap(false, Ordering::AcqRel) {
            return;
        }
        self.refresh_in_flight.store(false, Ordering::Release);
        // Reserve the latched successor before announcing completion. A
        // waiter must distinguish an intermediate handoff from a terminal
        // failed owner, without racing a newly queued configuration refresh.
        self.handoff_pending_authoritative_refresh();
        self.refresh_completion
            .send_modify(|generation| *generation = generation.saturating_add(1));
    }

    fn allows_verified_single_node_bootstrap(
        &self,
        host: &Arc<dyn DfsMetadataAuthorityHost>,
    ) -> bool {
        if self
            .write_configuration_has_committed_config
            .load(Ordering::Acquire)
        {
            return false;
        }
        let authority = self
            .single_node_bootstrap_authority
            .lock()
            .ok()
            .and_then(|proof| *proof);
        let Some(anchor) = authority else {
            return false;
        };
        let Ok(floor) = host.write_configuration_floor() else {
            return false;
        };
        floor.revision <= anchor.observed_revision
            && host.allows_unconfigured_single_node_write_configuration(&anchor.authority, &floor)
    }

    /// Borrow one already-published configuration without reading or hydrating
    /// KV. A detected authority transition wakes the existing refresh owner.
    /// Unknown native authority fails closed.
    /// sinomo:kv-configuration: Validate the retained configuration against the resident committed Raft configuration and higher-term fence.
    pub fn write_configuration_projection(
        self: &Arc<Self>,
    ) -> Result<Option<Arc<crate::DfsWriteConfigurationProjection>>, DfsError> {
        if !self.write_configuration_requested.load(Ordering::Acquire) {
            if self.host().is_some_and(|host| {
                host.supports_write_configuration_projection()
                    && !self.allows_verified_single_node_bootstrap(&host)
            }) {
                return Err(DfsError::Network {
                    message: "native configuration floor is unavailable".into(),
                });
            }
            return Ok(None);
        }
        let host = self.host().ok_or_else(|| DfsError::Network {
            message: "DFS write configuration host is unavailable".into(),
        })?;
        let view = self
            .write_configuration
            .lock()
            .map_err(|_| DfsError::Network {
                message: "DFS write configuration projection lock poisoned".into(),
            })?
            .snapshot();
        let Some(view) = view else {
            if !self
                .write_configuration_has_committed_config
                .load(Ordering::Acquire)
                && self.allows_verified_single_node_bootstrap(&host)
            {
                return Ok(None);
            }
            return Err(DfsError::Network {
                message: "DFS write configuration is awaiting a coherent authoritative projection"
                    .into(),
            });
        };
        let floor = host.write_configuration_floor()?;
        if !view.matches_floor(&floor) {
            let mut configuration =
                self.write_configuration
                    .lock()
                    .map_err(|_| DfsError::Network {
                        message: "DFS write configuration projection lock poisoned".into(),
                    })?;
            let invalidated = configuration
                .snapshot()
                .is_some_and(|current| !current.matches_floor(&floor));
            if invalidated {
                self.state.advance_configuration_generation()?;
                configuration.invalidate(floor.revision);
            }
            drop(configuration);
            if invalidated {
                host.authority_changed();
                self.request_authoritative_refresh();
            }
            return Err(DfsError::Network {
                message: "DFS write configuration authority floor advanced; no write was admitted"
                    .into(),
            });
        }
        Ok(Some(view))
    }

    pub fn write_configuration_readiness(
        self: &Arc<Self>,
    ) -> Result<crate::DfsWriteConfigurationReadiness, DfsError> {
        match self.write_configuration_projection() {
            Ok(view) => Ok(crate::DfsWriteConfigurationReadiness::Ready(view)),
            Err(error) => {
                let missing = self
                    .write_configuration
                    .lock()
                    .map(|configuration| configuration.snapshot().is_none())
                    .map_err(|_| DfsError::Network {
                        message: "DFS write configuration projection lock poisoned".into(),
                    })?;
                if self.write_configuration_requested.load(Ordering::Acquire) && missing {
                    Ok(crate::DfsWriteConfigurationReadiness::Pending)
                } else {
                    Err(error)
                }
            }
        }
    }

    /// Preeffect cold/gap admission joins the same reactive owner as metadata
    /// authority. Ready access performs only the existing native floor check.
    /// The caller supplies its existing projection/RPC deadline around this
    /// future; neither handoff nor a pending bundle creates another deadline.
    pub async fn ensure_write_configuration_ready_async(self: &Arc<Self>) -> Result<(), DfsError> {
        let mut completion = self.refresh_completion.subscribe();
        let mut joined_owner = false;
        loop {
            let error = match self.write_configuration_projection() {
                Ok(_) => return Ok(()),
                Err(error) => error,
            };
            let (missing, pending) = self
                .write_configuration
                .lock()
                .map(|configuration| {
                    (
                        configuration.snapshot().is_none(),
                        configuration.bootstrap_pending(),
                    )
                })
                .map_err(|_| DfsError::Network {
                    message: "DFS write configuration projection lock poisoned".into(),
                })?;
            if !missing {
                // A native floor error with a still-published view remains an
                // error. It is never converted into a speculative refresh.
                return Err(error);
            }
            if self.refresh_in_flight.load(Ordering::Acquire) {
                joined_owner = true;
            } else if !pending {
                if joined_owner {
                    return Err(error);
                }
                // Missing configuration is the explicit preeffect recovery
                // edge. Claim at most one owner, then join only its latched
                // successors; an unsuccessful terminal owner is final.
                joined_owner = true;
                self.request_authoritative_refresh();
                if !self.refresh_in_flight.load(Ordering::Acquire) {
                    // A promptly completed owner may publish before this load.
                    return self.write_configuration_projection().map(|_| ());
                }
            }
            if completion.changed().await.is_err() {
                return Err(error);
            }
        }
    }

    async fn refresh_authoritative_projection(
        self: &Arc<Self>,
        host: &Arc<dyn DfsMetadataAuthorityHost>,
        mode: &'static str,
    ) -> Option<KvRevision> {
        let discontinuity_generation = self.discontinuity_generation.load(Ordering::Acquire);
        if !self.write_configuration_requested.load(Ordering::Acquire) {
            return self.apply_read_result(
                host.read_authoritative_prefix().await,
                mode,
                discontinuity_generation,
            );
        }
        let (token, discontinuity_generation) = {
            let mut configuration = self.write_configuration.lock().ok()?;
            (
                configuration.begin_refresh(),
                self.discontinuity_generation.load(Ordering::Acquire),
            )
        };
        self.configuration_refresh_in_flight
            .store(true, Ordering::Release);
        let _configuration_owner = DfsWriteConfigurationRefreshOwner {
            runtime: Arc::clone(self),
            token,
        };
        let mut authority_changed = false;
        let result = async {
            let filesystem = host
                .read_authoritative_configuration_prefix(DFS_WRITE_CONFIGURATION_PREFIXES[0])
                .await?;
            let revision = filesystem.authority.revision;
            let metadata_projection = DfsAuthoritativePrefixProjection::complete(
                DFS_METADATA_AUTHORITY_PREFIX,
                filesystem.value.clone(),
                revision,
            )
            .map_err(DfsError::from)?;
            // Retain the valid filesystem proof immediately, but complete this
            // bounded configuration bootstrap before waking topology owners.
            // Otherwise initial discovery can cold-open a metadata generation
            // while the foreground preflight still reads its other scopes.
            {
                // A same-revision discontinuity can replace the underlying
                // canonical view. Serialize this publication with withdrawal;
                // a read started before that edge cannot restore readiness.
                let _configuration =
                    self.write_configuration
                        .lock()
                        .map_err(|_| DfsError::Network {
                            message: "DFS write configuration projection lock poisoned".into(),
                        })?;
                if self.discontinuity_generation.load(Ordering::Acquire) != discontinuity_generation
                {
                    return Err(DfsError::Network {
                        message:
                            "native filesystem read crossed a committed observation discontinuity"
                                .into(),
                    });
                }
                authority_changed |= self
                    .state
                    .install_authoritative_projection(metadata_projection)
                    .unwrap_or(false);
            }
            // The first native ReadIndex initializes the ordinary resident
            // owner. The floor access below itself never opens or replays KV.
            let before = host.write_configuration_floor()?;
            if before.value.is_some() {
                self.write_configuration_has_committed_config
                    .store(true, Ordering::Release);
            } else if !self
                .write_configuration_has_committed_config
                .load(Ordering::Acquire)
                && filesystem.authority.revision == KvRevision::ZERO
                && before.revision == KvRevision::ZERO
                && host.allows_unconfigured_single_node_write_configuration(
                    &filesystem.authority,
                    &before,
                )
            {
                let _configuration =
                    self.write_configuration
                        .lock()
                        .map_err(|_| DfsError::Network {
                            message: "DFS write configuration projection lock poisoned".into(),
                        })?;
                if self.discontinuity_generation.load(Ordering::Acquire) != discontinuity_generation
                {
                    return Err(DfsError::Network {
                        message:
                            "native bootstrap read crossed a committed observation discontinuity"
                                .into(),
                    });
                }
                *self
                    .single_node_bootstrap_authority
                    .lock()
                    .map_err(|_| DfsError::Network {
                        message: "DFS single-node bootstrap authority lock poisoned".into(),
                    })? = Some(DfsSingleNodeBootstrapAuthority {
                    authority: filesystem.authority,
                    observed_revision: KvRevision::ZERO,
                });
                // Preserve the existing native single-node bootstrap path.
                // A later committed config delta enables the shared view.
                self.write_configuration_requested
                    .store(false, Ordering::Release);
                return Ok(revision);
            }
            let content = host
                .read_authoritative_configuration_prefix(DFS_WRITE_CONFIGURATION_PREFIXES[1])
                .await?;
            let providers = host
                .read_authoritative_configuration_prefix(DFS_WRITE_CONFIGURATION_PREFIXES[2])
                .await?;
            let after = host.write_configuration_floor()?;
            if before.value.is_none()
                || before.value != after.value
                || before.observed_term != after.observed_term
            {
                return Err(DfsError::Network {
                    message: "configuration authority changed during bounded projection bootstrap"
                        .into(),
                });
            }
            let mut configuration =
                self.write_configuration
                    .lock()
                    .map_err(|_| DfsError::Network {
                        message: "DFS write configuration projection lock poisoned".into(),
                    })?;
            if self.discontinuity_generation.load(Ordering::Acquire) != discontinuity_generation {
                return Err(DfsError::Network {
                    message:
                        "native configuration read crossed a committed observation discontinuity"
                            .into(),
                });
            }
            configuration
                .install_bootstrap(token, [filesystem, content, providers], after)
                .map_err(DfsError::from)?;
            authority_changed |= if let Some(published) = configuration.snapshot() {
                match self
                    .state
                    .install_write_configuration_projection(&published, false)
                {
                    Ok(authority_changed) => authority_changed,
                    Err(error) => {
                        configuration.invalidate(revision);
                        return Err(error);
                    }
                }
            } else {
                false
            };
            drop(configuration);
            Ok(revision)
        }
        .await;
        if authority_changed {
            host.authority_changed();
        }
        match result {
            Ok(revision) => Some(revision),
            Err(error) => {
                tracing::warn!(target: "sinomo.dfs.metadata_authority", %error,
                    "DFS write configuration refresh failed; authority remains fail-closed");
                None
            }
        }
    }

    pub async fn ensure_complete_prefix_for_generation_async(
        self: &Arc<Self>,
        expected_generation: u64,
    ) -> Option<KvRevision> {
        let mut completion = self.refresh_completion.subscribe();
        let retained = self
            .state
            .retained_complete_prefix_revision_for_generation(expected_generation);
        if retained.is_some() || self.generation() != expected_generation {
            return retained;
        }
        let host = self.host()?;
        #[cfg(any(test, feature = "test-support"))]
        let before_claim = self
            .refresh_before_claim
            .lock()
            .ok()
            .and_then(|hook| hook.clone());
        #[cfg(any(test, feature = "test-support"))]
        if let Some((reached, release)) = before_claim {
            reached.notify_one();
            release.notified().await;
        }
        if let Some(refresh_owner) = self.try_claim_refresh() {
            self.refresh_requested.store(false, Ordering::Release);
            let retained = self
                .state
                .retained_complete_prefix_revision_for_generation(expected_generation);
            if retained.is_some() || self.generation() != expected_generation {
                drop(refresh_owner);
                return retained;
            }
            self.record_refresh();
            let proof_revision = self
                .refresh_authoritative_projection(&host, "async cold")
                .await;
            let refresh_required =
                proof_revision.is_some_and(|revision| self.state.refresh_required_after(revision));
            drop(refresh_owner);
            if refresh_required {
                self.request_authoritative_refresh();
            }
        }
        loop {
            let retained = self
                .state
                .retained_complete_prefix_revision_for_generation(expected_generation);
            if retained.is_some() || self.generation() != expected_generation {
                return retained;
            }
            if !self.refresh_in_flight.load(Ordering::Acquire) {
                return None;
            }
            // Join only an existing owner, including a latched successor. The
            // caller retains its original deadline; a terminal failed refresh
            // never starts another read or invents an authority retry here.
            if completion.changed().await.is_err() {
                return None;
            }
        }
    }

    pub(crate) fn subscribe_refresh_completion(&self) -> tokio::sync::watch::Receiver<u64> {
        self.refresh_completion.subscribe()
    }

    pub fn request_authoritative_refresh(self: &Arc<Self>) {
        self.refresh_requested.store(true, Ordering::Release);
        let Some(host) = self.host() else {
            return;
        };
        let Some(refresh_owner) = self.try_claim_refresh() else {
            return;
        };
        self.refresh_requested.store(false, Ordering::Release);
        if self.state.current_complete_prefix_revision().is_some()
            && (!self.write_configuration_requested.load(Ordering::Acquire)
                || !self.write_configuration_needs_refresh())
        {
            drop(refresh_owner);
            return;
        }
        self.record_refresh();
        let runtime = Arc::clone(self);
        let task_host = Arc::clone(&host);
        host.spawn(Box::pin(async move {
            let proof_revision = runtime
                .refresh_authoritative_projection(&task_host, "bounded")
                .await;
            let refresh_required = proof_revision
                .is_some_and(|revision| runtime.state.refresh_required_after(revision));
            drop(refresh_owner);
            if refresh_required {
                runtime.request_authoritative_refresh();
            }
        }));
    }

    fn apply_read_result(
        &self,
        read: Result<sinomo_core::kv::KvAuthoritativeRead<Vec<KvEntry>>, DfsError>,
        mode: &'static str,
        discontinuity_generation: u64,
    ) -> Option<KvRevision> {
        match read {
            Ok(read) => {
                let revision = read.authority.revision;
                match DfsAuthoritativePrefixProjection::complete(
                    DFS_METADATA_AUTHORITY_PREFIX,
                    read.value,
                    revision,
                ) {
                    Ok(projection) => {
                        let authority_changed = {
                            let _configuration = self.write_configuration.lock().ok()?;
                            if self.discontinuity_generation.load(Ordering::Acquire)
                                != discontinuity_generation
                            {
                                return None;
                            }
                            self.state
                                .install_authoritative_projection(projection)
                                .unwrap_or(false)
                        };
                        if authority_changed {
                            if let Some(host) = self.host() {
                                host.authority_changed();
                            }
                        }
                    }
                    Err(error) => tracing::warn!(
                        target: "sinomo.dfs.metadata_authority",
                        revision = revision.0,
                        "DFS metadata-authority {mode} prefix proof was rejected; authority remains fail-closed: {error}"
                    ),
                }
                Some(revision)
            }
            Err(error) => {
                tracing::warn!(
                    target: "sinomo.dfs.metadata_authority",
                    "DFS metadata-authority {mode} ReadIndex refresh failed; authority remains fail-closed: {error}"
                );
                None
            }
        }
    }

    fn try_claim_refresh(self: &Arc<Self>) -> Option<DfsMetadataAuthorityRefreshOwner> {
        self.refresh_in_flight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| DfsMetadataAuthorityRefreshOwner {
                runtime: Arc::clone(self),
            })
    }

    fn host(&self) -> Option<Arc<dyn DfsMetadataAuthorityHost>> {
        self.host.lock().ok().and_then(|host| host.clone())
    }

    fn record_refresh(&self) {
        #[cfg(any(test, feature = "test-support"))]
        self.refresh_count.fetch_add(1, Ordering::AcqRel);
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn projection_rebuild_count(&self) -> u64 {
        self.state.projection_rebuild_count()
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn projection_materialization_count(&self) -> u64 {
        self.state.projection_materialization_count()
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn refresh_count(&self) -> u64 {
        self.refresh_count.load(Ordering::Acquire)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn authority_refresh_count(&self) -> u64 {
        self.refresh_count()
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn install_refresh_before_claim_hook(
        &self,
        hook: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
    ) {
        if let Ok(mut current) = self.refresh_before_claim.lock() {
            *current = hook;
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn debug_snapshot(&self) -> DfsMetadataAuthorityDebugSnapshot {
        self.state.debug_snapshot()
    }
}

impl std::fmt::Debug for DfsMetadataAuthorityState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DfsMetadataAuthorityState")
            .field("generation", &self.generation())
            .field("projection_generation", &self.projection_generation())
            .finish_non_exhaustive()
    }
}

impl Default for DfsMetadataAuthorityState {
    fn default() -> Self {
        Self::new()
    }
}

impl DfsMetadataAuthorityState {
    pub fn new() -> Self {
        Self {
            projection: Mutex::new(DfsMetadataAuthorityProjection {
                revision: None,
                required_revision: KvRevision::ZERO,
                topology_comparison_baseline: false,
                entries: Vec::new(),
                metadata_shards_by_file_system: BTreeMap::new(),
            }),
            projection_generation: AtomicU64::new(1),
            generation: AtomicU64::new(1),
            storage_generation: AtomicU64::new(1),
            #[cfg(any(test, feature = "test-support"))]
            projection_rebuild_count: AtomicU64::new(0),
            #[cfg(any(test, feature = "test-support"))]
            projection_materialization_count: AtomicU64::new(0),
        }
    }

    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub fn projection_generation(&self) -> u64 {
        self.projection_generation.load(Ordering::Acquire)
    }

    pub fn storage_generation(&self) -> u64 {
        self.storage_generation.load(Ordering::Acquire)
    }

    fn lifecycle_generations(&self) -> Result<(u64, u64), DfsError> {
        // A structural commit advances two fences under this lock. Capture
        // them as one transition, never a new authority with old store owners.
        let _projection = self.projection.lock().map_err(|_| DfsError::Network {
            message: "DFS metadata authority projection lock poisoned".to_string(),
        })?;
        Ok((self.generation(), self.storage_generation()))
    }

    /// Shared write configuration changes fence retained clients and metadata
    /// admissions together, while preserving the physical store generation.
    fn advance_configuration_generation(&self) -> Result<(), DfsError> {
        let _projection = self.projection.lock().map_err(|_| DfsError::Network {
            message: "DFS metadata authority projection lock poisoned".to_string(),
        })?;
        Self::advance(&self.generation);
        Self::advance(&self.projection_generation);
        Ok(())
    }

    /// Publish the shared view's complete filesystem scope and its authority
    /// fence under the same lock. The caller still owns the configuration lock,
    /// so a write cannot observe the shared view before this local rebind.
    fn install_write_configuration_projection(
        &self,
        configuration: &crate::DfsWriteConfigurationProjection,
        configuration_changed: bool,
    ) -> Result<bool, DfsError> {
        let prefix = DfsAuthoritativePrefixProjection::complete(
            DFS_METADATA_AUTHORITY_PREFIX,
            configuration.scopes[0].clone(),
            configuration.revision,
        )
        .map_err(DfsError::from)?;
        let mut observed = self.projection.lock().map_err(|_| DfsError::Network {
            message: "DFS metadata authority projection lock poisoned".to_string(),
        })?;
        let authority_changed = self
            .install_authoritative_projection_locked(&mut observed, prefix)
            .ok_or_else(|| DfsError::Network {
                message: "DFS shared write configuration predates its filesystem authority floor"
                    .to_string(),
            })?;
        if configuration_changed {
            Self::advance(&self.generation);
            Self::advance(&self.projection_generation);
        }
        Ok(authority_changed || configuration_changed)
    }

    /// Borrow the existing decoded authority index only while its complete
    /// proof and the caller's configuration generation still match. Gaps and
    /// concurrent changes must use the normal authoritative refresh path.
    pub fn cached_metadata_shards(
        &self,
        file_system_id: &DfsFileSystemId,
        expected_projection_generation: u64,
    ) -> Result<Option<Vec<DfsMetadataShardConfig>>, sinomo_core::kv::KvError> {
        let projection =
            self.projection
                .lock()
                .map_err(|_| sinomo_core::kv::KvError::AuthorityUnavailable {
                    detail: "DFS metadata authority projection lock poisoned".to_string(),
                })?;
        if projection.revision.is_none()
            || self.projection_generation() != expected_projection_generation
        {
            return Ok(None);
        }
        match projection
            .metadata_shards_by_file_system
            .get(file_system_id.as_str())
        {
            Some(Ok(shards)) => Ok(Some(shards.clone())),
            Some(Err(detail)) => Err(sinomo_core::kv::KvError::Serialization {
                detail: detail.clone(),
            }),
            None => Ok(Some(Vec::new())),
        }
    }

    pub fn shard_projection_state(
        &self,
        file_system_id: &DfsFileSystemId,
        shard_id: &DfsMetadataShardId,
    ) -> (u64, bool) {
        let key = crate::metadata_shard_key(file_system_id, &shard_id.0);
        let Ok(projection) = self.projection.lock() else {
            return (self.generation(), false);
        };
        if projection.revision.is_none() {
            return (self.generation(), false);
        }
        let contains_shard = projection
            .entries
            .binary_search_by(|entry| entry.key.as_str().cmp(key.as_str()))
            .is_ok();
        (self.generation(), contains_shard)
    }

    pub fn namespace_read_stamp_state(
        &self,
        command: &DfsNamespaceCommand,
    ) -> Result<(u64, bool), DfsError> {
        let projection = self.projection.lock().map_err(|_| DfsError::Network {
            message: "DFS metadata authority projection lock poisoned".to_string(),
        })?;
        if projection.revision.is_none() {
            return Ok((self.generation(), false));
        }
        let stamp_matches = namespace_command_matches_authority_index(
            &projection.metadata_shards_by_file_system,
            command,
        )?;
        Ok((self.generation(), stamp_matches))
    }

    pub fn replica_context_state(
        &self,
        context: &DfsMetadataReplicaShardContext,
    ) -> Result<(u64, bool), DfsError> {
        let projection = self.projection.lock().map_err(|_| DfsError::Network {
            message: "DFS metadata authority projection lock poisoned".to_string(),
        })?;
        if projection.revision.is_none() {
            return Ok((self.generation(), false));
        }
        let matches =
            metadata_replica_context_matches_authority_entries(&projection.entries, context)?;
        Ok((self.generation(), matches))
    }

    pub fn retained_complete_prefix_for_generation(
        &self,
        expected_generation: u64,
    ) -> Option<DfsAuthoritativePrefixProjection> {
        self.projection.lock().ok().and_then(|projection| {
            if self.generation() != expected_generation {
                return None;
            }
            let revision = projection.revision?;
            #[cfg(any(test, feature = "test-support"))]
            self.projection_materialization_count
                .fetch_add(1, Ordering::AcqRel);
            DfsAuthoritativePrefixProjection::complete(
                DFS_METADATA_AUTHORITY_PREFIX,
                projection.entries.clone(),
                revision,
            )
            .ok()
        })
    }

    pub fn retained_complete_prefix_revision_for_generation(
        &self,
        expected_generation: u64,
    ) -> Option<KvRevision> {
        self.projection.lock().ok().and_then(|projection| {
            (self.generation() == expected_generation)
                .then_some(projection.revision)
                .flatten()
        })
    }

    pub fn current_complete_prefix(&self) -> Option<DfsAuthoritativePrefixProjection> {
        let generation = self.generation();
        self.retained_complete_prefix_for_generation(generation)
    }

    pub fn current_complete_prefix_revision(&self) -> Option<KvRevision> {
        self.projection
            .lock()
            .ok()
            .and_then(|projection| projection.revision)
    }

    /// Install one complete bounded authoritative proof. `None` rejects the
    /// proof; `Some` reports whether exact topology changed.
    pub fn install_authoritative_projection(
        &self,
        projection: DfsAuthoritativePrefixProjection,
    ) -> Option<bool> {
        if !projection.is_complete() {
            return None;
        }
        let Ok(mut observed) = self.projection.lock() else {
            return None;
        };
        self.install_authoritative_projection_locked(&mut observed, projection)
    }

    fn install_authoritative_projection_locked(
        &self,
        observed: &mut DfsMetadataAuthorityProjection,
        projection: DfsAuthoritativePrefixProjection,
    ) -> Option<bool> {
        let (entries, revision, _) = projection.into_parts();
        if revision < observed.required_revision {
            return None;
        }
        if observed
            .revision
            .is_some_and(|observed_revision| revision <= observed_revision)
        {
            return Some(false);
        }
        #[cfg(any(test, feature = "test-support"))]
        self.projection_rebuild_count.fetch_add(1, Ordering::AcqRel);
        let replaced_valid_projection = observed.revision.is_some();
        let projection_changed = !replaced_valid_projection || observed.entries != entries;
        let authority_changed = observed
            .entries
            .iter()
            .filter(|entry| dfs_metadata_authority_key_is_exact(&entry.key))
            .map(|entry| (&entry.key, &entry.value))
            .ne(entries
                .iter()
                .filter(|entry| dfs_metadata_authority_key_is_exact(&entry.key))
                .map(|entry| (&entry.key, &entry.value)));
        if replaced_valid_projection || observed.topology_comparison_baseline {
            if authority_changed {
                Self::advance(&self.generation);
            }
            if crate::metadata_authority_projection::metadata_storage_configuration_changed_in_projection(
                &observed.entries,
                &entries,
            ) {
                Self::advance(&self.storage_generation);
            }
        }
        observed.revision = Some(revision);
        observed.required_revision = revision;
        observed.topology_comparison_baseline = true;
        observed.entries = entries;
        Self::rebuild_metadata_shard_index_locked(observed);
        if projection_changed {
            Self::advance(&self.projection_generation);
        }
        Some(authority_changed)
    }

    pub fn refresh_required_after(&self, proof_revision: KvRevision) -> bool {
        self.projection.lock().is_ok_and(|projection| {
            projection.revision.is_none() && projection.required_revision > proof_revision
        })
    }

    /// Require at least this authoritative revision, withdrawing readiness when
    /// necessary. Returns whether the host must request a new ReadIndex proof.
    pub fn require_authoritative_revision(&self, required_revision: KvRevision) -> bool {
        let Ok(mut projection) = self.projection.lock() else {
            return false;
        };
        if projection
            .revision
            .is_some_and(|revision| revision >= required_revision)
        {
            return false;
        }
        self.invalidate_for_revision_catchup_locked(&mut projection, required_revision);
        true
    }

    pub fn observe_snapshot(&self, snapshot: &KvSnapshot) -> bool {
        let Ok(mut observed) = self.projection.lock() else {
            return false;
        };
        if observed
            .revision
            .is_some_and(|revision| snapshot.revision <= revision)
            || snapshot
                .prefix_entries(DFS_METADATA_AUTHORITY_PREFIX)
                .eq(observed.entries.iter())
            || (observed.revision.is_none() && snapshot.revision <= observed.required_revision)
        {
            return false;
        }
        self.invalidate_for_revision_catchup_locked(&mut observed, snapshot.revision);
        true
    }

    pub fn observe_discontinuity(
        &self,
        revision: KvRevision,
    ) -> DfsMetadataAuthorityDeltaObservation {
        let Ok(mut observed) = self.projection.lock() else {
            return DfsMetadataAuthorityDeltaObservation::invalidated(false);
        };
        let proof_withdrawn = observed.revision.is_some();
        self.invalidate_for_revision_catchup_locked(&mut observed, revision);
        DfsMetadataAuthorityDeltaObservation::invalidated(proof_withdrawn)
    }

    pub fn observe_delta(
        &self,
        delta: &ClusterKvCommittedDelta,
    ) -> DfsMetadataAuthorityDeltaObservation {
        let Ok(mut observed) = self.projection.lock() else {
            return DfsMetadataAuthorityDeltaObservation::invalidated(false);
        };
        let proof_withdrawn = observed.revision.is_some();
        let Some(previous_revision) = observed.revision else {
            self.invalidate_for_revision_catchup_locked(&mut observed, delta.revision);
            return DfsMetadataAuthorityDeltaObservation::invalidated(proof_withdrawn);
        };
        if delta.revision <= previous_revision {
            return DfsMetadataAuthorityDeltaObservation::accepted(false);
        }
        if previous_revision < delta.compacted_revision
            || !kv_events_cover_revision_range(previous_revision, delta.revision, &delta.events)
        {
            self.invalidate_for_revision_catchup_locked(&mut observed, delta.revision);
            return DfsMetadataAuthorityDeltaObservation::invalidated(proof_withdrawn);
        }

        let relevant_event_keys = delta
            .events
            .iter()
            .filter(|event| event.key.starts_with(DFS_METADATA_AUTHORITY_PREFIX))
            .map(|event| event.key.as_str())
            .collect::<BTreeSet<_>>();
        let mut final_keys = BTreeSet::new();
        let mut relevant_final_keys = BTreeSet::new();
        for state in delta.final_key_states.iter() {
            if !final_keys.insert(state.key.as_str())
                || state
                    .entry
                    .as_ref()
                    .is_some_and(|entry| entry.key != state.key)
                || state.entry.as_ref().is_some_and(|entry| {
                    state.key.starts_with(DFS_METADATA_AUTHORITY_PREFIX)
                        && (entry.create_revision > entry.modify_revision
                            || entry.modify_revision > delta.revision)
                })
            {
                self.invalidate_for_revision_catchup_locked(&mut observed, delta.revision);
                return DfsMetadataAuthorityDeltaObservation::invalidated(proof_withdrawn);
            }
            if state.key.starts_with(DFS_METADATA_AUTHORITY_PREFIX) {
                relevant_final_keys.insert(state.key.as_str());
            }
        }
        if relevant_event_keys != relevant_final_keys {
            self.invalidate_for_revision_catchup_locked(&mut observed, delta.revision);
            return DfsMetadataAuthorityDeltaObservation::invalidated(proof_withdrawn);
        }

        let mut authority_changed = false;
        let mut storage_changed = false;
        let mut prefix_changed = false;
        for state in delta.final_key_states.iter() {
            if !state.key.starts_with(DFS_METADATA_AUTHORITY_PREFIX) {
                continue;
            }
            let authority_key = dfs_metadata_authority_key_is_exact(&state.key);
            let position = observed
                .entries
                .binary_search_by(|entry| entry.key.as_str().cmp(state.key.as_str()));
            storage_changed |=
                crate::metadata_authority_projection::metadata_storage_configuration_changed(
                    &state.key,
                    position.ok().map(|index| &observed.entries[index]),
                    state.entry.as_ref(),
                );
            match (position, state.entry.as_ref()) {
                (Ok(index), Some(entry)) => {
                    if authority_key && observed.entries[index].value != entry.value {
                        authority_changed = true;
                    }
                    if observed.entries[index] != *entry {
                        observed.entries[index] = entry.clone();
                        prefix_changed = true;
                    }
                }
                (Err(index), Some(entry)) => {
                    observed.entries.insert(index, entry.clone());
                    authority_changed |= authority_key;
                    prefix_changed = true;
                }
                (Ok(index), None) => {
                    observed.entries.remove(index);
                    authority_changed |= authority_key;
                    prefix_changed = true;
                }
                (Err(_), None) => {}
            }
        }
        if observed.entries.len() > sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES
            || observed
                .entries
                .iter()
                .try_fold(0_usize, |bytes, entry| {
                    let bytes = bytes
                        .checked_add(entry.key.len())?
                        .checked_add(entry.value.len())?;
                    (bytes <= sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES)
                        .then_some(bytes)
                })
                .is_none()
        {
            tracing::warn!(
                target: "sinomo.dfs.metadata_authority",
                revision = delta.revision.0,
                entry_count = observed.entries.len(),
                "DFS metadata-authority delta exceeded bounded prefix limits; invalidating retained authority"
            );
            self.invalidate_projection_locked(&mut observed, delta.revision);
            return DfsMetadataAuthorityDeltaObservation::invalidated(proof_withdrawn);
        }
        if authority_changed {
            Self::advance(&self.generation);
        }
        if storage_changed {
            Self::advance(&self.storage_generation);
        }
        if prefix_changed {
            Self::advance(&self.projection_generation);
        }
        observed.revision = Some(delta.revision);
        observed.required_revision = delta.revision;
        if prefix_changed {
            Self::rebuild_metadata_shard_index_locked(&mut observed);
        }
        DfsMetadataAuthorityDeltaObservation::accepted(authority_changed)
    }

    fn invalidate_projection_locked(
        &self,
        observed: &mut DfsMetadataAuthorityProjection,
        required_revision: KvRevision,
    ) {
        let was_valid = observed.revision.take().is_some();
        observed.required_revision = observed.required_revision.max(required_revision);
        observed.topology_comparison_baseline = false;
        observed.entries.clear();
        observed.metadata_shards_by_file_system.clear();
        if was_valid {
            Self::advance(&self.projection_generation);
            Self::advance(&self.generation);
            Self::advance(&self.storage_generation);
        }
    }

    fn invalidate_for_revision_catchup_locked(
        &self,
        observed: &mut DfsMetadataAuthorityProjection,
        required_revision: KvRevision,
    ) {
        let was_valid = observed.revision.take().is_some();
        observed.required_revision = observed.required_revision.max(required_revision);
        if was_valid {
            observed.topology_comparison_baseline = true;
            Self::advance(&self.projection_generation);
        }
    }

    fn rebuild_metadata_shard_index_locked(projection: &mut DfsMetadataAuthorityProjection) {
        projection.metadata_shards_by_file_system =
            metadata_shard_index_from_authority_entries(&projection.entries);
    }

    fn advance(generation: &AtomicU64) {
        let _ = generation.fetch_update(Ordering::AcqRel, Ordering::Acquire, |generation| {
            Some(generation.saturating_add(1))
        });
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn projection_rebuild_count(&self) -> u64 {
        self.projection_rebuild_count.load(Ordering::Acquire)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn projection_materialization_count(&self) -> u64 {
        self.projection_materialization_count
            .load(Ordering::Acquire)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn debug_snapshot(&self) -> DfsMetadataAuthorityDebugSnapshot {
        let projection = self
            .projection
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        DfsMetadataAuthorityDebugSnapshot {
            revision: projection.revision,
            required_revision: projection.required_revision,
            topology_comparison_baseline: projection.topology_comparison_baseline,
            entry_count: projection.entries.len(),
            metadata_file_system_count: projection.metadata_shards_by_file_system.len(),
        }
    }
}

fn kv_events_cover_revision_range(
    previous_revision: KvRevision,
    current_revision: KvRevision,
    events: &[KvEvent],
) -> bool {
    current_revision <= previous_revision
        || (events.is_empty() && current_revision == previous_revision.next())
        || (events.first().map(|event| event.revision) == Some(previous_revision.next())
            && events.last().map(|event| event.revision) == Some(current_revision)
            && events.windows(2).all(|events| {
                let previous = events[0].revision;
                let current = events[1].revision;
                current == previous || current == previous.next()
            }))
}

#[cfg(test)]
#[path = "metadata_authority_runtime/write_configuration_tests.rs"]
mod write_configuration_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::kv::{ClusterKvCommittedKeyState, KvEventKind, KvVersion};

    fn entry(key: &str, value: &[u8], revision: u64, version: u64) -> KvEntry {
        KvEntry {
            key: key.to_string(),
            value: value.to_vec(),
            create_revision: KvRevision(1),
            modify_revision: KvRevision(revision),
            version: KvVersion(version),
            lease_id: None,
        }
    }

    fn projection(revision: u64, entries: Vec<KvEntry>) -> DfsAuthoritativePrefixProjection {
        DfsAuthoritativePrefixProjection::complete(
            DFS_METADATA_AUTHORITY_PREFIX,
            entries,
            KvRevision(revision),
        )
        .expect("complete projection")
    }

    fn config_entry(config: &crate::DfsFileSystemConfig, revision: u64) -> KvEntry {
        entry(
            &crate::file_system_config_key(&config.file_system_id),
            &sinomo_core::kv::encode_kv_value(config).unwrap(),
            revision,
            revision,
        )
    }

    #[test]
    fn policy_deltas_advance_authority_without_replacing_metadata_stores() {
        let state = DfsMetadataAuthorityState::new();
        let owner =
            sinomo_core::node::NodeId::from_legacy_named("storage_generation_owner").unwrap();
        let mut config = crate::DfsFileSystemConfig::from(&crate::distributed_sifs(owner));
        let original = config_entry(&config, 1);
        state
            .install_authoritative_projection(projection(1, vec![original.clone()]))
            .unwrap();
        let storage_generation = state.storage_generation();
        let authority_generation = state.generation();
        config.subtree_policies[2].min_commit_replicas = Some(2);
        config.subtree_policies[2].desired_replicas = Some(3);
        let changed = config_entry(&config, 2);
        let observation = state.observe_delta(&ClusterKvCommittedDelta {
            revision: KvRevision(2),
            compacted_revision: KvRevision::ZERO,
            events: vec![KvEvent {
                revision: KvRevision(2),
                key: changed.key.clone(),
                kind: KvEventKind::Put {
                    value: changed.value.clone(),
                },
            }]
            .into(),
            final_key_states: vec![ClusterKvCommittedKeyState {
                key: changed.key.clone(),
                entry: Some(changed),
            }]
            .into(),
        });
        assert!(observation.accepted && observation.authority_changed);
        assert_eq!(state.generation(), authority_generation + 1);
        assert_eq!(state.storage_generation(), storage_generation);
        let retained = state.current_complete_prefix().unwrap();
        let projected = retained.entries()[0]
            .decode::<crate::DfsFileSystemConfig>()
            .unwrap();
        assert_eq!(
            projected, config,
            "the delta must apply the new policy immediately"
        );

        // Snapshot recovery uses the same typed lifecycle classification.
        config.display_name = "new display name".into();
        state
            .install_authoritative_projection(projection(3, vec![config_entry(&config, 3)]))
            .unwrap();
        assert_eq!(state.storage_generation(), storage_generation);
        config.owner_node_id =
            sinomo_core::node::NodeId::from_legacy_named("storage_generation_new_owner").unwrap();
        state
            .install_authoritative_projection(projection(4, vec![config_entry(&config, 4)]))
            .unwrap();
        assert_eq!(state.storage_generation(), storage_generation + 1);
    }

    #[test]
    fn proof_deltas_keep_stores_while_structural_shard_changes_replace_them() {
        let state = DfsMetadataAuthorityState::new();
        let shard_key = "/dfs/filesystems/sifs/metadata-shards/root";
        let proof_key = "/dfs/filesystems/sifs/metadata-head-watermarks/root";
        state
            .install_authoritative_projection(projection(1, vec![entry(shard_key, b"v1", 1, 1)]))
            .unwrap();
        let storage_generation = state.storage_generation();
        state
            .install_authoritative_projection(projection(
                2,
                vec![
                    entry(proof_key, b"proof", 2, 1),
                    entry(shard_key, b"v1", 1, 1),
                ],
            ))
            .unwrap();
        assert_eq!(state.storage_generation(), storage_generation);
        assert_eq!(state.generation(), 2, "proof still fences exact authority");
        state
            .install_authoritative_projection(projection(
                3,
                vec![
                    entry(proof_key, b"proof", 2, 1),
                    entry(shard_key, b"v2", 3, 2),
                ],
            ))
            .unwrap();
        assert_eq!(state.storage_generation(), storage_generation + 1);
        state
            .install_authoritative_projection(projection(4, vec![entry(proof_key, b"proof", 2, 1)]))
            .unwrap();
        assert_eq!(
            state.storage_generation(),
            storage_generation + 2,
            "removed shard retires its stores"
        );
    }

    #[test]
    fn replacement_proof_advances_only_changed_topology() {
        let state = DfsMetadataAuthorityState::new();
        let key = "/dfs/filesystems/sifs/metadata-shards/root";
        assert_eq!(
            state.install_authoritative_projection(projection(1, vec![entry(key, b"v1", 1, 1)],)),
            Some(true)
        );
        assert_eq!(
            state.generation(),
            1,
            "initial proof establishes generation"
        );
        assert_eq!(
            state.install_authoritative_projection(projection(2, vec![entry(key, b"v1", 2, 2)],)),
            Some(false)
        );
        assert_eq!(state.generation(), 1, "equal topology keeps generation");
        assert_eq!(
            state.install_authoritative_projection(projection(3, vec![entry(key, b"v2", 3, 3)],)),
            Some(true)
        );
        assert_eq!(
            state.generation(),
            2,
            "changed topology advances generation"
        );
    }

    #[test]
    fn gapped_policy_and_proof_refresh_retains_stores_but_not_stale_admission() {
        let state = DfsMetadataAuthorityState::new();
        let owner = sinomo_core::node::NodeId::from_legacy_named("gap_policy_store_owner").unwrap();
        let mut config = crate::DfsFileSystemConfig::from(&crate::distributed_sifs(owner));
        let shard_key = crate::metadata_shard_key(&config.file_system_id, "root");
        state
            .install_authoritative_projection(projection(
                1,
                vec![config_entry(&config, 1), entry(&shard_key, b"v1", 1, 1)],
            ))
            .unwrap();
        let authority_generation = state.generation();
        let storage_generation = state.storage_generation();
        let file_system_id = config.file_system_id.clone();
        let shard_id = DfsMetadataShardId::new("root");
        assert!(state.shard_projection_state(&file_system_id, &shard_id).1);
        config.subtree_policies[2].min_commit_replicas = Some(2);
        config.subtree_policies[2].desired_replicas = Some(3);
        let changed = config_entry(&config, 3);
        let observation = state.observe_delta(&ClusterKvCommittedDelta {
            revision: KvRevision(3),
            compacted_revision: KvRevision::ZERO,
            events: vec![KvEvent {
                revision: KvRevision(3),
                key: changed.key.clone(),
                kind: KvEventKind::Put {
                    value: changed.value.clone(),
                },
            }]
            .into(),
            final_key_states: vec![ClusterKvCommittedKeyState {
                key: changed.key.clone(),
                entry: Some(changed.clone()),
            }]
            .into(),
        });
        assert!(!observation.accepted);
        assert_eq!(state.storage_generation(), storage_generation);
        assert_eq!(
            state.retained_complete_prefix_revision_for_generation(authority_generation),
            None
        );
        assert!(
            !state.shard_projection_state(&file_system_id, &shard_id).1,
            "retained stores cannot admit an operation without a current authority certificate"
        );
        let proof_key = format!("/dfs/filesystems/{file_system_id}/metadata-head-watermarks/root");
        let mut entries = vec![
            changed,
            entry(&shard_key, b"v1", 1, 1),
            entry(&proof_key, b"new-proof", 3, 1),
        ];
        entries.sort_by(|left, right| left.key.cmp(&right.key));
        state
            .install_authoritative_projection(projection(3, entries))
            .unwrap();
        assert!(state.shard_projection_state(&file_system_id, &shard_id).1);
        assert_eq!(state.storage_generation(), storage_generation);
        assert_eq!(state.generation(), authority_generation + 1);
    }

    #[test]
    fn gapped_structural_refresh_replaces_stores_only_after_authority_is_certified() {
        let state = DfsMetadataAuthorityState::new();
        let key = "/dfs/filesystems/sifs/metadata-shards/root";
        state
            .install_authoritative_projection(projection(1, vec![entry(key, b"v1", 1, 1)]))
            .unwrap();
        let storage_generation = state.storage_generation();
        assert!(
            !state
                .observe_delta(&ClusterKvCommittedDelta {
                    revision: KvRevision(3),
                    compacted_revision: KvRevision::ZERO,
                    events: vec![KvEvent {
                        revision: KvRevision(3),
                        key: key.to_string(),
                        kind: KvEventKind::Put {
                            value: b"v2".to_vec()
                        }
                    }]
                    .into(),
                    final_key_states: vec![ClusterKvCommittedKeyState {
                        key: key.to_string(),
                        entry: Some(entry(key, b"v2", 3, 2))
                    }]
                    .into(),
                })
                .accepted
        );
        assert_eq!(
            state.storage_generation(),
            storage_generation,
            "a gap alone owns no store transition"
        );
        assert!(state.current_complete_prefix().is_none());
        state
            .install_authoritative_projection(projection(3, vec![entry(key, b"v2", 3, 2)]))
            .unwrap();
        assert_eq!(state.storage_generation(), storage_generation + 1);
        assert!(state.current_complete_prefix().is_some());
    }

    #[test]
    fn gapped_delta_withdraws_readiness_but_retains_comparison_baseline() {
        let state = DfsMetadataAuthorityState::new();
        let key = "/dfs/filesystems/sifs/metadata-shards/root";
        state
            .install_authoritative_projection(projection(1, vec![entry(key, b"v1", 1, 1)]))
            .expect("initial projection");
        let generation = state.generation();
        let observation = state.observe_delta(&ClusterKvCommittedDelta {
            revision: KvRevision(3),
            compacted_revision: KvRevision::ZERO,
            events: vec![KvEvent {
                revision: KvRevision(3),
                key: key.to_string(),
                kind: KvEventKind::Put {
                    value: b"v2".to_vec(),
                },
            }]
            .into(),
            final_key_states: vec![ClusterKvCommittedKeyState {
                key: key.to_string(),
                entry: Some(entry(key, b"v2", 3, 2)),
            }]
            .into(),
        });
        assert!(!observation.accepted);
        assert_eq!(state.generation(), generation);
        assert_eq!(
            state.retained_complete_prefix_revision_for_generation(generation),
            None
        );
        let snapshot = state.debug_snapshot();
        assert_eq!(snapshot.required_revision, KvRevision(3));
        assert!(snapshot.topology_comparison_baseline);
        assert_eq!(snapshot.entry_count, 1);
    }

    #[test]
    fn contiguous_noop_delta_advances_without_authoritative_refresh() {
        let state = DfsMetadataAuthorityState::new();
        let key = "/dfs/filesystems/sifs/metadata-shards/root";
        state
            .install_authoritative_projection(projection(7, vec![entry(key, b"v1", 7, 1)]))
            .expect("initial projection");
        let generation = state.generation();
        let projection_generation = state.projection_generation();

        let observation = state.observe_delta(&ClusterKvCommittedDelta {
            revision: KvRevision(8),
            compacted_revision: KvRevision::ZERO,
            events: Vec::new().into(),
            final_key_states: Vec::new().into(),
        });

        assert_eq!(
            observation,
            DfsMetadataAuthorityDeltaObservation::accepted(false)
        );
        assert_eq!(state.generation(), generation);
        assert_eq!(state.projection_generation(), projection_generation);
        assert_eq!(
            state.retained_complete_prefix_revision_for_generation(generation),
            Some(KvRevision(8)),
            "a contiguous no-op must retain readiness at the newer revision"
        );
    }

    #[test]
    fn refresh_owner_drop_wakes_waiters_and_releases_singleflight() {
        let runtime = Arc::new(DfsMetadataAuthorityRuntime::new());
        let mut completion = runtime.refresh_completion.subscribe();
        let owner = runtime.try_claim_refresh().expect("refresh owner");
        drop(owner);

        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime")
            .block_on(completion.changed())
            .expect("owner drop notification");
        assert!(!runtime.refresh_in_flight.load(Ordering::Acquire));
        assert!(
            runtime.try_claim_refresh().is_some(),
            "cancelled refresh ownership must not strand later consumers",
        );
    }
}
