use crate::configuration_snapshot::{
    DfsConfigurationSnapshot, DfsFileSystemConfigsByKey, DfsMetadataAuthorityScope,
    file_system_configs_from_authority_entries,
};
use crate::metadata_authority_projection::DfsMetadataShardsByFileSystem;
use crate::write_configuration_projection::{
    DFS_WRITE_CONFIGURATION_PREFIXES, DfsWriteConfigurationState,
};
use crate::{
    DfsAuthoritativePrefixProjection, DfsError, DfsFileSystemId, DfsMetadataReplicaShardContext,
    DfsMetadataShardConfig, DfsMetadataShardId, DfsNamespaceCommand,
    dfs_metadata_authority_key_is_exact, metadata_shard_index_from_authority_entries,
};
use sinomo_core::kv::{
    ClusterKvCommittedDelta, KvAuthorityProof, KvEntry, KvEvent, KvLocalRead, KvRevision,
    KvSnapshot,
};
use sinomo_core::node::NodeId;
use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    Arc, Mutex, MutexGuard, OnceLock, Weak,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

pub const DFS_METADATA_AUTHORITY_PREFIX: &str = "/dfs/filesystems/";

/// A failed bounded refresh is re-sent at most this many consecutive times.
const DFS_CONFIGURATION_REFRESH_RETRY_LIMIT: u32 = 5;
/// First re-send delay; each later attempt doubles it.
const DFS_CONFIGURATION_REFRESH_RETRY_DELAY: std::time::Duration =
    std::time::Duration::from_millis(100);

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

/// Writer-side filesystem-scope projection and generation state.
///
/// This owner decides whether a proof or committed delta is complete, applies
/// resource bounds and advances topology generations. It holds no lock: the
/// runtime mutates it under its single configuration writer lock and
/// publishes the result as one [`DfsConfigurationSnapshot`]. The entries and
/// decoded indexes are shared with that publication, never copied beside it.
#[derive(Debug)]
pub struct DfsMetadataAuthorityState {
    revision: Option<KvRevision>,
    required_revision: KvRevision,
    /// A withdrawn scope retains its last entries only to compare topology
    /// when the next complete proof arrives. They are never published.
    topology_comparison_baseline: bool,
    entries: Arc<Vec<KvEntry>>,
    shards: Arc<DfsMetadataShardsByFileSystem>,
    file_systems: Arc<DfsFileSystemConfigsByKey>,
    generation: u64,
    projection_generation: u64,
    storage_generation: u64,
    /// The node whose physical stores the storage generation tracks. With it,
    /// a change to a write-set shard that leaves this node's placement
    /// unchanged rebinds stores in place instead of advancing the storage
    /// generation.
    local_node_id: Option<NodeId>,
    #[cfg(any(test, feature = "test-support"))]
    projection_rebuild_count: u64,
}

#[derive(Clone, Copy)]
struct DfsSingleNodeBootstrapAuthority {
    authority: KvAuthorityProof,
    observed_revision: KvRevision,
}

/// Every configuration writer path holds this one lock while it mutates the
/// write-configuration bootstrap/journal and the filesystem scope, then
/// publishes their combined snapshot before releasing it.
#[derive(Debug)]
struct DfsConfigurationWriter {
    write: DfsWriteConfigurationState,
    metadata: DfsMetadataAuthorityState,
}

impl DfsConfigurationWriter {
    fn snapshot(&self) -> DfsConfigurationSnapshot {
        self.metadata.snapshot(self.write.snapshot())
    }
}

pub struct DfsMetadataAuthorityRuntime {
    writer: Mutex<DfsConfigurationWriter>,
    published: tokio::sync::watch::Sender<Arc<DfsConfigurationSnapshot>>,
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
    refresh_failures: std::sync::atomic::AtomicU32,
    /// Unit fixtures that drive each refresh task by hand opt out of re-sends.
    #[cfg(test)]
    refresh_resend_disabled: AtomicBool,
    #[cfg(any(test, feature = "test-support"))]
    refresh_count: AtomicU64,
    #[cfg(any(test, feature = "test-support"))]
    projection_materialization_count: AtomicU64,
}

#[derive(Debug)]
pub struct DfsMetadataAuthorityRuntimeRegistry<K> {
    name: &'static str,
    runtimes: Mutex<std::collections::BTreeMap<K, Weak<DfsMetadataAuthorityRuntime>>>,
}

impl<K> DfsMetadataAuthorityRuntimeRegistry<K>
where
    K: Clone + Ord,
{
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            runtimes: Mutex::new(std::collections::BTreeMap::new()),
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

// Keep a missing publication distinct from a failed native floor read.
enum DfsWriteConfigurationReadError {
    Pending(DfsError),
    Unavailable(DfsError),
}

impl From<DfsError> for DfsWriteConfigurationReadError {
    fn from(error: DfsError) -> Self {
        Self::Unavailable(error)
    }
}

fn writer_poisoned() -> DfsError {
    DfsError::Network {
        message: "DFS configuration writer lock poisoned".into(),
    }
}

fn configuration_pending() -> DfsError {
    DfsError::Network {
        message: "DFS write configuration is awaiting a coherent authoritative projection".into(),
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
        // A cancelled or failed owner leaves no publication: the published
        // write configuration was already absent when this refresh began.
        if let Ok(mut writer) = self.runtime.writer.lock() {
            writer.write.finish_refresh(self.token);
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
            writer: Mutex::new(DfsConfigurationWriter {
                write: DfsWriteConfigurationState::new(),
                metadata: DfsMetadataAuthorityState::new(),
            }),
            published: tokio::sync::watch::channel(Arc::new(DfsConfigurationSnapshot::initial())).0,
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
            refresh_failures: std::sync::atomic::AtomicU32::new(0),
            #[cfg(test)]
            refresh_resend_disabled: AtomicBool::new(false),
            #[cfg(any(test, feature = "test-support"))]
            refresh_count: AtomicU64::new(0),
            #[cfg(any(test, feature = "test-support"))]
            projection_materialization_count: AtomicU64::new(0),
        }
    }

    /// Attach the refresh host. Once committed observations are also attached,
    /// a newly attached host owns the initial bootstrap edge.
    pub fn attach_host(self: &Arc<Self>, host: Arc<dyn DfsMetadataAuthorityHost>) -> bool {
        if host.supports_write_configuration_projection() {
            self.write_configuration_requested
                .store(true, Ordering::Release);
        }
        let newly_attached = {
            let Ok(mut attached) = self.host.lock() else {
                return false;
            };
            let newly_attached = attached.is_none();
            *attached = Some(host);
            newly_attached
        };
        if newly_attached && self.observation_subscription.get().is_some() {
            self.request_authoritative_refresh();
        }
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

    /// Committed observations make this runtime reactive. Attaching them is
    /// the cold-start edge: the first bounded bootstrap begins only after
    /// every later committed delta can reach its journal.
    pub fn attach_observation_subscription(
        self: &Arc<Self>,
        subscription: sinomo_core::kv::KvCommittedObservationSubscription,
    ) -> Result<(), sinomo_core::kv::KvCommittedObservationSubscription> {
        self.observation_subscription.set(subscription)?;
        if self.host().is_some() {
            self.request_authoritative_refresh();
        }
        Ok(())
    }

    /// Cheap load of the current publication. It performs no floor check;
    /// write admission uses [`Self::checked_configuration_snapshot`].
    pub fn configuration_snapshot(&self) -> Arc<DfsConfigurationSnapshot> {
        Arc::clone(&self.published.borrow())
    }

    /// Every replacement of the published snapshot is one `changed` event.
    pub fn subscribe_configuration_snapshot(
        &self,
    ) -> tokio::sync::watch::Receiver<Arc<DfsConfigurationSnapshot>> {
        self.published.subscribe()
    }

    pub fn generation(&self) -> u64 {
        self.published.borrow().generation
    }

    pub fn projection_generation(&self) -> u64 {
        self.published.borrow().projection_generation
    }

    /// Physical metadata-store lifecycle. Authority proof and policy changes
    /// retain stores while continuing to advance the separate authority fence.
    pub fn storage_generation(&self) -> u64 {
        self.published.borrow().storage_generation
    }

    /// Track physical stores for `local_node_id`
    /// ([`DfsMetadataAuthorityState::bind_local_node`]).
    pub fn bind_local_node(&self, local_node_id: NodeId) {
        if let Ok(mut writer) = self.writer.lock() {
            writer.metadata.bind_local_node(local_node_id);
        }
    }

    pub(crate) fn lifecycle_generations(&self) -> Result<(u64, u64), DfsError> {
        // One publication carries both fences of a structural transition.
        let snapshot = self.published.borrow();
        Ok((snapshot.generation, snapshot.storage_generation))
    }

    /// Shards and filesystem configuration from one current publication with
    /// its projection generation. Callers retain their semantic and storage
    /// revalidation.
    pub(crate) fn cached_current_publication_configuration(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<
        Option<(
            u64,
            Vec<DfsMetadataShardConfig>,
            Option<crate::DfsFileSystemConfig>,
        )>,
        sinomo_core::kv::KvError,
    > {
        let snapshot = self.configuration_snapshot();
        Ok(snapshot
            .publication_configuration(file_system_id)?
            .map(|(shards, config)| (snapshot.projection_generation, shards, config)))
    }

    /// The floor-checked publication when it carries a write configuration.
    /// Pending readiness is an error and `None` is a ready host without one,
    /// exactly as for [`Self::write_configuration_projection`].
    pub(crate) fn write_configuration_snapshot(
        self: &Arc<Self>,
    ) -> Result<Option<Arc<DfsConfigurationSnapshot>>, DfsError> {
        let snapshot = self
            .read_configuration_snapshot()
            .map_err(|error| match error {
                DfsWriteConfigurationReadError::Pending(error)
                | DfsWriteConfigurationReadError::Unavailable(error) => error,
            })?;
        Ok(snapshot.write.is_some().then_some(snapshot))
    }

    #[cfg(test)]
    /// The floor-checked publication with its complete write configuration.
    /// Owned read input for one operation, not an execution capability.
    pub(crate) fn current_write_configuration_snapshot(
        self: &Arc<Self>,
    ) -> Result<Arc<DfsConfigurationSnapshot>, DfsError> {
        self.write_configuration_snapshot()?
            .filter(|snapshot| snapshot.metadata.is_some())
            .ok_or_else(|| DfsError::Metadata {
                message: "original Repair configuration publication was withdrawn or replaced during capture".into(),
            })
    }

    pub fn shard_projection_state(
        &self,
        file_system_id: &DfsFileSystemId,
        shard_id: &DfsMetadataShardId,
    ) -> (u64, bool) {
        let snapshot = self.configuration_snapshot();
        (
            snapshot.generation,
            snapshot.contains_shard_key(file_system_id, shard_id),
        )
    }

    pub fn namespace_read_stamp_state(
        &self,
        command: &DfsNamespaceCommand,
    ) -> Result<(u64, bool), DfsError> {
        let snapshot = self.configuration_snapshot();
        Ok((
            snapshot.generation,
            snapshot.namespace_read_stamp_matches(command)?,
        ))
    }

    pub fn replica_context_state(
        &self,
        context: &DfsMetadataReplicaShardContext,
    ) -> Result<(u64, bool), DfsError> {
        self.replica_context_lifecycle_state(context)
            .map(|((generation, _), matches)| (generation, matches))
    }

    pub(crate) fn replica_context_lifecycle_state(
        &self,
        context: &DfsMetadataReplicaShardContext,
    ) -> Result<((u64, u64), bool), DfsError> {
        // The exact context and physical-store generation belong to the same
        // publication. A provider/proof-only change need not replace a store.
        let snapshot = self.configuration_snapshot();
        Ok((
            (snapshot.generation, snapshot.storage_generation),
            snapshot.replica_context_matches(context)?,
        ))
    }

    fn materialize_prefix(
        &self,
        snapshot: &DfsConfigurationSnapshot,
    ) -> Option<DfsAuthoritativePrefixProjection> {
        let prefix = snapshot.complete_prefix()?;
        #[cfg(any(test, feature = "test-support"))]
        self.projection_materialization_count
            .fetch_add(1, Ordering::AcqRel);
        Some(prefix)
    }

    pub fn current_complete_prefix(self: &Arc<Self>) -> Option<DfsAuthoritativePrefixProjection> {
        let retained = self.materialize_prefix(&self.configuration_snapshot());
        if retained.is_none() {
            self.request_authoritative_refresh();
        }
        retained
    }

    pub fn current_complete_prefix_for_generation(
        self: &Arc<Self>,
        expected_generation: u64,
    ) -> Option<DfsAuthoritativePrefixProjection> {
        let snapshot = self.configuration_snapshot();
        if snapshot.generation != expected_generation {
            return None;
        }
        let retained = self.materialize_prefix(&snapshot);
        if retained.is_none() {
            self.request_authoritative_refresh();
        }
        retained
    }

    pub fn retained_complete_prefix_for_generation(
        &self,
        expected_generation: u64,
    ) -> Option<DfsAuthoritativePrefixProjection> {
        let snapshot = self.configuration_snapshot();
        (snapshot.generation == expected_generation)
            .then(|| self.materialize_prefix(&snapshot))
            .flatten()
    }

    pub fn retained_complete_prefix_revision_for_generation(
        &self,
        expected_generation: u64,
    ) -> Option<KvRevision> {
        let snapshot = self.published.borrow();
        (snapshot.generation == expected_generation)
            .then(|| snapshot.revision())
            .flatten()
    }

    pub fn load_complete_prefix_for_generation(
        self: &Arc<Self>,
        expected_generation: u64,
    ) -> Option<DfsAuthoritativePrefixProjection> {
        self.current_complete_prefix_for_generation(expected_generation)
    }

    pub fn current_complete_prefix_revision(self: &Arc<Self>) -> Option<KvRevision> {
        let revision = self.published.borrow().revision();
        if revision.is_none()
            || (self.write_configuration_requested.load(Ordering::Acquire)
                && self.write_configuration_needs_refresh())
        {
            self.request_authoritative_refresh();
        }
        revision
    }

    fn writer(&self) -> Option<MutexGuard<'_, DfsConfigurationWriter>> {
        self.writer.lock().ok()
    }

    /// Replace the publication with the writer's current state. Called by
    /// every writer path before it releases the writer lock. An unchanged
    /// state is not an event.
    fn publish(&self, writer: &DfsConfigurationWriter) {
        let candidate = writer.snapshot();
        self.published.send_if_modified(|current| {
            if current.same_publication(&candidate) {
                return false;
            }
            *current = Arc::new(candidate);
            true
        });
    }

    pub fn install_authoritative_projection(
        &self,
        projection: DfsAuthoritativePrefixProjection,
    ) -> bool {
        let authority_changed = {
            let Some(mut writer) = self.writer() else {
                return false;
            };
            let Some(authority_changed) =
                writer.metadata.install_authoritative_projection(projection)
            else {
                return false;
            };
            self.publish(&writer);
            authority_changed
        };
        if authority_changed {
            self.host()
                .into_iter()
                .for_each(|host| host.authority_changed());
        }
        true
    }

    pub fn require_authoritative_revision(self: &Arc<Self>, revision: KvRevision) {
        let refresh = {
            let Some(mut guard) = self.writer() else {
                return;
            };
            let writer = &mut *guard;
            let refresh = writer.metadata.require_authoritative_revision(revision);
            // Withdraw both scopes at the same boundary. Otherwise a service
            // opener can accept the old configuration while the required
            // refresh is queued. Preserve a bootstrap already buffering it.
            // Catch-up alone keeps the generation: only changed exact
            // authority advances it when the refresh publishes.
            if refresh && writer.write.observed_revision() < revision {
                writer.write.invalidate(revision);
            }
            self.publish(writer);
            refresh
        };
        if refresh {
            self.request_authoritative_refresh();
        }
    }

    /// A client observed `revision` through its own ReadIndex read or commit
    /// while the publication is older. The committed observer delivers every
    /// applied delta, so this is a catch-up edge only once the resident state
    /// has applied past the publication: then a delta was missed. Otherwise
    /// its delta is still arriving and no refresh is needed. A host without a
    /// resident floor keeps the plain revision requirement.
    pub fn observe_client_revision(self: &Arc<Self>, revision: KvRevision) {
        let behind = || {
            self.published
                .borrow()
                .revision()
                .is_none_or(|published| published < revision)
        };
        if !behind() {
            return;
        }
        let Some(host) = self.host() else {
            return;
        };
        let applied = match host.write_configuration_floor() {
            Ok(floor) => floor.revision >= revision,
            Err(_) => true,
        };
        if applied && behind() {
            self.require_authoritative_revision(revision);
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
        let configuration_ready = match self.writer() {
            Some(mut guard) => {
                let writer = &mut *guard;
                observation = writer.metadata.observe_delta(delta);
                let was_ready = writer.write.snapshot().is_some();
                let new_delta = delta.revision > writer.write.observed_revision();
                let ready = writer.write.observe_delta(delta);
                configuration_became_ready = !was_ready && ready;
                configuration_change = self.write_configuration_requested.load(Ordering::Acquire)
                    && ((new_delta && provider_configuration_touched) || (was_ready && !ready));
                let ready = if let Some(published) = writer.write.snapshot() {
                    match writer
                        .metadata
                        .install_write_configuration_projection(&published, configuration_change)
                    {
                        Ok(authority_changed) => {
                            configuration_change |= authority_changed;
                            true
                        }
                        Err(_) => {
                            if was_ready || configuration_change {
                                configuration_change = true;
                                writer.metadata.advance_configuration_generation();
                            }
                            writer.write.invalidate(delta.revision);
                            false
                        }
                    }
                } else {
                    if configuration_change {
                        // Provider and zone configuration fences authority but
                        // cannot retire or reopen a physical metadata store.
                        writer.metadata.advance_configuration_generation();
                    }
                    ready
                };
                self.publish(writer);
                ready
            }
            None => false,
        };
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
            // its read owner has already returned.
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
        let refresh = match self.writer() {
            Some(mut guard) => {
                let writer = &mut *guard;
                if self.write_configuration_requested.load(Ordering::Acquire)
                    && writer.write.observed_revision() < snapshot.revision
                {
                    // A replacement snapshot cannot certify scope continuity.
                    // The shared refresh owner restores a complete configuration.
                    if writer.write.snapshot().is_some() {
                        writer.metadata.advance_configuration_generation();
                    }
                    writer.write.invalidate(snapshot.revision);
                }
                let refresh = writer.metadata.observe_snapshot(snapshot);
                self.publish(writer);
                refresh
            }
            None => false,
        };
        if refresh {
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
            self.writer()
                .map(|writer| writer.metadata.required_revision)
                .unwrap_or(KvRevision::ZERO)
        });
        self.observe_discontinuity(revision)
    }

    pub fn observe_discontinuity(
        self: &Arc<Self>,
        revision: KvRevision,
    ) -> DfsMetadataAuthorityDeltaObservation {
        let mut observation = match self.writer() {
            Some(mut guard) => {
                let writer = &mut *guard;
                DfsMetadataAuthorityState::advance(&self.discontinuity_generation);
                let mut observation = writer.metadata.observe_discontinuity(revision);
                let configuration_was_ready = writer.write.snapshot().is_some();
                writer.write.invalidate(revision);
                if configuration_was_ready {
                    writer.metadata.advance_configuration_generation();
                    observation.authority_changed = true;
                    observation.proof_withdrawn = true;
                }
                self.publish(writer);
                observation
            }
            None => {
                DfsMetadataAuthorityState::advance(&self.discontinuity_generation);
                DfsMetadataAuthorityDeltaObservation::invalidated(false)
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
        self.writer()
            .is_some_and(|writer| writer.write.needs_refresh())
    }

    fn handoff_pending_authoritative_refresh(self: &Arc<Self>) {
        if !self.refresh_in_flight.load(Ordering::Acquire)
            && self.refresh_requested.load(Ordering::Acquire)
            && (self.published.borrow().revision().is_none()
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
        // Reserve the latched successor before announcing completion.
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

    /// Borrow the already-published write configuration without reading or
    /// hydrating KV. A detected authority transition wakes the existing
    /// refresh owner. Unknown native authority fails closed.
    pub fn write_configuration_projection(
        self: &Arc<Self>,
    ) -> Result<Option<Arc<crate::DfsWriteConfigurationProjection>>, DfsError> {
        self.read_configuration_snapshot()
            .map(|snapshot| snapshot.write.clone())
            .map_err(|error| match error {
                DfsWriteConfigurationReadError::Pending(error)
                | DfsWriteConfigurationReadError::Unavailable(error) => error,
            })
    }

    /// The current publication after its native floor check. `None` means a
    /// coherent configuration is still pending; waiters subscribe instead.
    pub fn checked_configuration_snapshot(
        self: &Arc<Self>,
    ) -> Result<Option<Arc<DfsConfigurationSnapshot>>, DfsError> {
        match self.read_configuration_snapshot() {
            Ok(snapshot) => Ok(Some(snapshot)),
            Err(DfsWriteConfigurationReadError::Pending(_)) => Ok(None),
            Err(DfsWriteConfigurationReadError::Unavailable(error)) => Err(error),
        }
    }

    /// sinomo:kv-configuration: Check the existing resident native floor before lending one published configuration snapshot.
    fn read_configuration_snapshot(
        self: &Arc<Self>,
    ) -> Result<Arc<DfsConfigurationSnapshot>, DfsWriteConfigurationReadError> {
        let snapshot = self.configuration_snapshot();
        if !self.write_configuration_requested.load(Ordering::Acquire) {
            if self.host().is_some_and(|host| {
                host.supports_write_configuration_projection()
                    && !self.allows_verified_single_node_bootstrap(&host)
            }) {
                return Err(DfsError::Network {
                    message: "native configuration floor is unavailable".into(),
                }
                .into());
            }
            return Ok(snapshot);
        }
        let host = self.host().ok_or_else(|| DfsError::Network {
            message: "DFS write configuration host is unavailable".into(),
        })?;
        let Some(view) = snapshot.write.as_ref() else {
            if !self
                .write_configuration_has_committed_config
                .load(Ordering::Acquire)
                && self.allows_verified_single_node_bootstrap(&host)
            {
                return Ok(snapshot);
            }
            return Err(DfsWriteConfigurationReadError::Pending(
                configuration_pending(),
            ));
        };
        #[cfg(any(test, feature = "test-support"))]
        if crate::test_runtime::native_configuration_floor_is_denied(self.as_ref()) {
            return Err(DfsError::Network {
                message: "native configuration floor is unavailable at the injected read boundary"
                    .into(),
            }
            .into());
        }
        let floor = host.write_configuration_floor()?;
        if !view.matches_floor(&floor) {
            let invalidated = {
                let mut writer = self.writer().ok_or_else(writer_poisoned)?;
                let invalidated = writer
                    .write
                    .snapshot()
                    .is_some_and(|current| !current.matches_floor(&floor));
                if invalidated {
                    writer.metadata.advance_configuration_generation();
                    writer.write.invalidate(floor.revision);
                    self.publish(&writer);
                }
                invalidated
            };
            if invalidated {
                host.authority_changed();
                self.request_authoritative_refresh();
            }
            return Err(DfsWriteConfigurationReadError::Pending(DfsError::Network {
                message: "DFS write configuration authority floor advanced; no write was admitted"
                    .into(),
            }));
        }
        Ok(snapshot)
    }

    pub fn write_configuration_readiness(
        self: &Arc<Self>,
    ) -> Result<crate::DfsWriteConfigurationReadiness, DfsError> {
        match self.read_configuration_snapshot() {
            Ok(snapshot) => Ok(crate::DfsWriteConfigurationReadiness::Ready(
                snapshot.write.clone(),
            )),
            Err(DfsWriteConfigurationReadError::Pending(_)) => {
                Ok(crate::DfsWriteConfigurationReadiness::Pending)
            }
            Err(DfsWriteConfigurationReadError::Unavailable(error)) => Err(error),
        }
    }

    /// Wait for a floor-checked publication. The caller supplies its existing
    /// operation deadline around this future. Waiters never start a refresh:
    /// every edge that withdraws or has not yet produced a publication has
    /// already requested the shared bounded refresh.
    pub async fn ready_configuration_snapshot_async(
        self: &Arc<Self>,
    ) -> Result<Arc<DfsConfigurationSnapshot>, DfsError> {
        let mut published = self.published.subscribe();
        loop {
            match self.read_configuration_snapshot() {
                Ok(snapshot) => return Ok(snapshot),
                Err(DfsWriteConfigurationReadError::Unavailable(error)) => return Err(error),
                Err(DfsWriteConfigurationReadError::Pending(_)) => {}
            }
            if published.changed().await.is_err() {
                return Err(configuration_pending());
            }
        }
    }

    /// Preeffect cold/gap admission joins the same reactive publication as
    /// metadata authority. Ready access performs only the native floor check.
    pub async fn ensure_write_configuration_ready_async(self: &Arc<Self>) -> Result<(), DfsError> {
        self.ready_configuration_snapshot_async().await.map(|_| ())
    }

    /// Return the very cut whose native floor passed readiness. This
    /// immutable configuration is read input, not publication authority.
    pub(crate) async fn ready_write_configuration_projection_async(
        self: &Arc<Self>,
    ) -> Result<Option<Arc<crate::DfsWriteConfigurationProjection>>, DfsError> {
        self.ready_configuration_snapshot_async()
            .await
            .map(|snapshot| snapshot.write.clone())
    }

    /// Wait until the filesystem scope is published for `expected_generation`
    /// or that generation is superseded. The caller owns the deadline.
    pub async fn ensure_complete_prefix_for_generation_async(
        self: &Arc<Self>,
        expected_generation: u64,
    ) -> Option<KvRevision> {
        let mut published = self.published.subscribe();
        loop {
            {
                let snapshot = published.borrow_and_update();
                if snapshot.generation != expected_generation {
                    return None;
                }
                if let Some(revision) = snapshot.revision() {
                    return Some(revision);
                }
            }
            if published.changed().await.is_err() {
                return None;
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
            let mut writer = self.writer()?;
            // Committed deltas can complete a pending bundle after this owner
            // was queued. An obsolete owner must not withdraw that publication.
            if writer.write.snapshot().is_some()
                && let Some(revision) = writer.metadata.revision
            {
                return Some(revision);
            }
            (
                writer.write.begin_refresh(),
                self.discontinuity_generation.load(Ordering::Acquire),
            )
        };
        self.configuration_refresh_in_flight
            .store(true, Ordering::Release);
        let _configuration_owner = DfsWriteConfigurationRefreshOwner {
            runtime: Arc::clone(self),
            token,
        };
        #[cfg(any(test, feature = "test-support"))]
        crate::test_runtime::pause_native_configuration_rebuild(self.as_ref()).await;
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
            // Retain the valid filesystem proof immediately. The write
            // configuration stays unpublished until the bounded bootstrap
            // completes, so service openers still wait for every scope.
            {
                // A same-revision discontinuity can replace the underlying
                // canonical view. A read started before that edge cannot
                // restore readiness.
                let mut writer = self.writer().ok_or_else(writer_poisoned)?;
                if self.discontinuity_generation.load(Ordering::Acquire) != discontinuity_generation
                {
                    return Err(DfsError::Network {
                        message:
                            "native filesystem read crossed a committed observation discontinuity"
                                .into(),
                    });
                }
                authority_changed |= writer
                    .metadata
                    .install_authoritative_projection(metadata_projection)
                    .unwrap_or(false);
                self.publish(&writer);
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
                let _writer = self.writer().ok_or_else(writer_poisoned)?;
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
            let members = host
                .read_authoritative_configuration_prefix(DFS_WRITE_CONFIGURATION_PREFIXES[3])
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
            let mut guard = self.writer().ok_or_else(writer_poisoned)?;
            let writer = &mut *guard;
            if self.discontinuity_generation.load(Ordering::Acquire) != discontinuity_generation {
                return Err(DfsError::Network {
                    message:
                        "native configuration read crossed a committed observation discontinuity"
                            .into(),
                });
            }
            writer
                .write
                .install_bootstrap(token, [filesystem, content, providers, members], after)
                .map_err(DfsError::from)?;
            if let Some(published) = writer.write.snapshot() {
                match writer
                    .metadata
                    .install_write_configuration_projection(&published, false)
                {
                    Ok(changed) => authority_changed |= changed,
                    Err(error) => {
                        writer.write.invalidate(revision);
                        self.publish(writer);
                        return Err(error);
                    }
                }
            }
            self.publish(writer);
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
        if self.published.borrow().revision().is_some()
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
            let refresh_required = proof_revision.is_some_and(|revision| {
                runtime
                    .writer()
                    .is_some_and(|writer| writer.metadata.refresh_required_after(revision))
            });
            drop(refresh_owner);
            if refresh_required {
                runtime.request_authoritative_refresh();
            } else if proof_revision.is_some() {
                runtime.refresh_failures.store(0, Ordering::Release);
            } else {
                runtime.resend_failed_refresh(&task_host);
            }
        }));
    }

    /// A failed bounded refresh leaves the publication unavailable, and no
    /// later committed edge is guaranteed to request another one. Re-send it
    /// on a bounded doubling delay; a success resets the sequence and an
    /// exhausted sequence waits for the next edge. Waiters keep their own
    /// operation deadlines throughout.
    fn resend_failed_refresh(self: &Arc<Self>, host: &Arc<dyn DfsMetadataAuthorityHost>) {
        #[cfg(test)]
        if self.refresh_resend_disabled.load(Ordering::Acquire) {
            return;
        }
        let attempt = self.refresh_failures.fetch_add(1, Ordering::AcqRel);
        if attempt >= DFS_CONFIGURATION_REFRESH_RETRY_LIMIT {
            return;
        }
        let delay = DFS_CONFIGURATION_REFRESH_RETRY_DELAY.saturating_mul(1 << attempt);
        let runtime = Arc::downgrade(self);
        host.spawn(Box::pin(async move {
            tokio::time::sleep(delay).await;
            if let Some(runtime) = runtime.upgrade() {
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
                            let mut writer = self.writer()?;
                            if self.discontinuity_generation.load(Ordering::Acquire)
                                != discontinuity_generation
                            {
                                return None;
                            }
                            let changed = writer
                                .metadata
                                .install_authoritative_projection(projection)
                                .unwrap_or(false);
                            self.publish(&writer);
                            changed
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
        self.writer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .metadata
            .projection_rebuild_count()
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn projection_materialization_count(&self) -> u64 {
        self.projection_materialization_count
            .load(Ordering::Acquire)
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
    pub fn debug_snapshot(&self) -> DfsMetadataAuthorityDebugSnapshot {
        self.writer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .metadata
            .debug_snapshot()
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
            revision: None,
            required_revision: KvRevision::ZERO,
            topology_comparison_baseline: false,
            entries: Arc::new(Vec::new()),
            shards: Arc::new(DfsMetadataShardsByFileSystem::new()),
            file_systems: Arc::new(DfsFileSystemConfigsByKey::new()),
            generation: 1,
            projection_generation: 1,
            storage_generation: 1,
            local_node_id: None,
            #[cfg(any(test, feature = "test-support"))]
            projection_rebuild_count: 0,
        }
    }

    /// Track physical stores for `local_node_id`. Set once, before the first
    /// projection; a runtime without it classifies every shard change by
    /// its bytes.
    pub fn bind_local_node(&mut self, local_node_id: NodeId) {
        if self.local_node_id.is_none() {
            self.local_node_id = Some(local_node_id);
        }
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn projection_generation(&self) -> u64 {
        self.projection_generation
    }

    pub fn storage_generation(&self) -> u64 {
        self.storage_generation
    }

    /// The publication of this scope beside the current write configuration.
    pub(crate) fn snapshot(
        &self,
        write: Option<Arc<crate::DfsWriteConfigurationProjection>>,
    ) -> DfsConfigurationSnapshot {
        DfsConfigurationSnapshot {
            generation: self.generation,
            projection_generation: self.projection_generation,
            storage_generation: self.storage_generation,
            metadata: self.revision.map(|revision| DfsMetadataAuthorityScope {
                revision,
                entries: Arc::clone(&self.entries),
                shards: Arc::clone(&self.shards),
                file_systems: Arc::clone(&self.file_systems),
            }),
            write,
        }
    }

    /// Shared write configuration changes fence retained clients and metadata
    /// admissions together, while preserving the physical store generation.
    fn advance_configuration_generation(&mut self) {
        self.generation = self.generation.saturating_add(1);
        self.projection_generation = self.projection_generation.saturating_add(1);
    }

    /// Publish the write configuration's complete filesystem scope and its
    /// authority fence at the same writer step.
    fn install_write_configuration_projection(
        &mut self,
        configuration: &crate::DfsWriteConfigurationProjection,
        configuration_changed: bool,
    ) -> Result<bool, DfsError> {
        let predates = || DfsError::Network {
            message: "DFS shared write configuration predates its filesystem authority floor"
                .to_string(),
        };
        if configuration.revision < self.required_revision {
            return Err(predates());
        }
        let authority_changed = if self
            .revision
            .is_some_and(|revision| configuration.revision <= revision)
        {
            // Committed replay already advanced this same scope.
            false
        } else {
            let prefix = DfsAuthoritativePrefixProjection::complete(
                DFS_METADATA_AUTHORITY_PREFIX,
                configuration.scopes[0].clone(),
                configuration.revision,
            )
            .map_err(DfsError::from)?;
            self.install_authoritative_projection(prefix)
                .ok_or_else(predates)?
        };
        if configuration_changed {
            self.advance_configuration_generation();
        }
        Ok(authority_changed || configuration_changed)
    }

    /// Install one complete bounded authoritative proof. `None` rejects the
    /// proof; `Some` reports whether exact topology changed.
    pub fn install_authoritative_projection(
        &mut self,
        projection: DfsAuthoritativePrefixProjection,
    ) -> Option<bool> {
        if !projection.is_complete() {
            return None;
        }
        let (entries, revision, _) = projection.into_parts();
        if revision < self.required_revision {
            return None;
        }
        if self
            .revision
            .is_some_and(|observed_revision| revision <= observed_revision)
        {
            return Some(false);
        }
        #[cfg(any(test, feature = "test-support"))]
        {
            self.projection_rebuild_count = self.projection_rebuild_count.saturating_add(1);
        }
        let replaced_valid_projection = self.revision.is_some();
        let projection_changed = !replaced_valid_projection || *self.entries != entries;
        let authority_changed = self
            .entries
            .iter()
            .filter(|entry| dfs_metadata_authority_key_is_exact(&entry.key))
            .map(|entry| (&entry.key, &entry.value))
            .ne(entries
                .iter()
                .filter(|entry| dfs_metadata_authority_key_is_exact(&entry.key))
                .map(|entry| (&entry.key, &entry.value)));
        if replaced_valid_projection || self.topology_comparison_baseline {
            if authority_changed {
                self.generation = self.generation.saturating_add(1);
            }
            if crate::metadata_authority_projection::metadata_storage_configuration_changed_in_projection_for_node(
                self.local_node_id.as_ref(),
                &self.entries,
                &entries,
            ) {
                self.storage_generation = self.storage_generation.saturating_add(1);
            }
        }
        self.revision = Some(revision);
        self.required_revision = revision;
        self.topology_comparison_baseline = true;
        if projection_changed {
            self.replace_entries(entries);
            self.projection_generation = self.projection_generation.saturating_add(1);
        }
        Some(authority_changed)
    }

    fn replace_entries(&mut self, entries: Vec<KvEntry>) {
        self.shards = Arc::new(metadata_shard_index_from_authority_entries(&entries));
        self.file_systems = Arc::new(file_system_configs_from_authority_entries(&entries));
        self.entries = Arc::new(entries);
    }

    pub fn refresh_required_after(&self, proof_revision: KvRevision) -> bool {
        self.revision.is_none() && self.required_revision > proof_revision
    }

    /// Require at least this authoritative revision, withdrawing readiness when
    /// necessary. Returns whether the host must request a new ReadIndex proof.
    pub fn require_authoritative_revision(&mut self, required_revision: KvRevision) -> bool {
        if self
            .revision
            .is_some_and(|revision| revision >= required_revision)
        {
            return false;
        }
        self.invalidate_for_revision_catchup(required_revision);
        true
    }

    pub fn observe_snapshot(&mut self, snapshot: &KvSnapshot) -> bool {
        if self
            .revision
            .is_some_and(|revision| snapshot.revision <= revision)
            || snapshot
                .prefix_entries(DFS_METADATA_AUTHORITY_PREFIX)
                .eq(self.entries.iter())
            || (self.revision.is_none() && snapshot.revision <= self.required_revision)
        {
            return false;
        }
        self.invalidate_for_revision_catchup(snapshot.revision);
        true
    }

    pub fn observe_discontinuity(
        &mut self,
        revision: KvRevision,
    ) -> DfsMetadataAuthorityDeltaObservation {
        let proof_withdrawn = self.revision.is_some();
        self.invalidate_for_revision_catchup(revision);
        DfsMetadataAuthorityDeltaObservation::invalidated(proof_withdrawn)
    }

    pub fn observe_delta(
        &mut self,
        delta: &ClusterKvCommittedDelta,
    ) -> DfsMetadataAuthorityDeltaObservation {
        let proof_withdrawn = self.revision.is_some();
        let Some(previous_revision) = self.revision else {
            self.invalidate_for_revision_catchup(delta.revision);
            return DfsMetadataAuthorityDeltaObservation::invalidated(proof_withdrawn);
        };
        if delta.revision <= previous_revision {
            return DfsMetadataAuthorityDeltaObservation::accepted(false);
        }
        if previous_revision < delta.compacted_revision
            || !kv_events_cover_revision_range(previous_revision, delta.revision, &delta.events)
        {
            self.invalidate_for_revision_catchup(delta.revision);
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
                self.invalidate_for_revision_catchup(delta.revision);
                return DfsMetadataAuthorityDeltaObservation::invalidated(proof_withdrawn);
            }
            if state.key.starts_with(DFS_METADATA_AUTHORITY_PREFIX) {
                relevant_final_keys.insert(state.key.as_str());
            }
        }
        if relevant_event_keys != relevant_final_keys {
            self.invalidate_for_revision_catchup(delta.revision);
            return DfsMetadataAuthorityDeltaObservation::invalidated(proof_withdrawn);
        }

        // Copy-on-write: the published snapshot keeps the previous entries.
        let mut next: Option<Vec<KvEntry>> = None;
        let mut authority_changed = false;
        let mut storage_changed = false;
        // A shard change is classified against the whole delta, because a
        // write-set shard's configuration and write-set key change together.
        let mut changed_shard_keys = Vec::new();
        for state in delta.final_key_states.iter() {
            if !state.key.starts_with(DFS_METADATA_AUTHORITY_PREFIX) {
                continue;
            }
            let authority_key = dfs_metadata_authority_key_is_exact(&state.key);
            let current = next.as_ref().unwrap_or(self.entries.as_ref());
            let position =
                current.binary_search_by(|entry| entry.key.as_str().cmp(state.key.as_str()));
            if crate::metadata_authority_projection::dfs_metadata_shard_config_key_is_exact(
                &state.key,
            ) {
                changed_shard_keys.push(state.key.as_str());
            } else {
                storage_changed |=
                    crate::metadata_authority_projection::metadata_storage_configuration_changed(
                        &state.key,
                        position.ok().map(|index| &current[index]),
                        state.entry.as_ref(),
                    );
            }
            match (position, state.entry.as_ref()) {
                (Ok(index), Some(entry)) => {
                    if authority_key && current[index].value != entry.value {
                        authority_changed = true;
                    }
                    if current[index] != *entry {
                        let entries = next.get_or_insert_with(|| self.entries.as_ref().clone());
                        entries[index] = entry.clone();
                    }
                }
                (Err(index), Some(entry)) => {
                    let entries = next.get_or_insert_with(|| self.entries.as_ref().clone());
                    entries.insert(index, entry.clone());
                    authority_changed |= authority_key;
                }
                (Ok(index), None) => {
                    let entries = next.get_or_insert_with(|| self.entries.as_ref().clone());
                    entries.remove(index);
                    authority_changed |= authority_key;
                }
                (Err(_), None) => {}
            }
        }
        if let Some(entries) = next.as_ref()
            && (entries.len() > sinomo_core::kv::CLUSTER_KV_COMPLETE_RANGE_PREFIX_MAX_ENTRIES
                || entries
                    .iter()
                    .try_fold(0_usize, |bytes, entry| {
                        let bytes = bytes
                            .checked_add(entry.key.len())?
                            .checked_add(entry.value.len())?;
                        (bytes
                            <= sinomo_core::kv::CLUSTER_KV_COMPLETE_RANGE_PREFIX_MAX_PAYLOAD_BYTES)
                            .then_some(bytes)
                    })
                    .is_none())
        {
            tracing::warn!(
                target: "sinomo.dfs.metadata_authority",
                revision = delta.revision.0,
                entry_count = entries.len(),
                "DFS metadata-authority delta exceeded bounded prefix limits; invalidating retained authority"
            );
            self.invalidate_projection(delta.revision);
            return DfsMetadataAuthorityDeltaObservation::invalidated(proof_withdrawn);
        }
        if !changed_shard_keys.is_empty() {
            let current = next.as_ref().unwrap_or(self.entries.as_ref());
            storage_changed |= changed_shard_keys.iter().any(|key| {
                crate::metadata_authority_projection::metadata_storage_configuration_changed_for_node(
                    self.local_node_id.as_ref(),
                    key,
                    &self.entries,
                    current,
                )
            });
        }
        if authority_changed {
            self.generation = self.generation.saturating_add(1);
        }
        if storage_changed {
            self.storage_generation = self.storage_generation.saturating_add(1);
        }
        self.revision = Some(delta.revision);
        self.required_revision = delta.revision;
        if let Some(entries) = next {
            self.replace_entries(entries);
            self.projection_generation = self.projection_generation.saturating_add(1);
        }
        DfsMetadataAuthorityDeltaObservation::accepted(authority_changed)
    }

    fn invalidate_projection(&mut self, required_revision: KvRevision) {
        let was_valid = self.revision.take().is_some();
        self.required_revision = self.required_revision.max(required_revision);
        self.topology_comparison_baseline = false;
        self.replace_entries(Vec::new());
        if was_valid {
            self.projection_generation = self.projection_generation.saturating_add(1);
            self.generation = self.generation.saturating_add(1);
            self.storage_generation = self.storage_generation.saturating_add(1);
        }
    }

    fn invalidate_for_revision_catchup(&mut self, required_revision: KvRevision) {
        let was_valid = self.revision.take().is_some();
        self.required_revision = self.required_revision.max(required_revision);
        if was_valid {
            self.topology_comparison_baseline = true;
            self.projection_generation = self.projection_generation.saturating_add(1);
        }
    }

    fn advance(generation: &AtomicU64) {
        let _ = generation.fetch_update(Ordering::AcqRel, Ordering::Acquire, |generation| {
            Some(generation.saturating_add(1))
        });
    }

    /// Materialize the retained scope for the current generation.
    pub fn current_complete_prefix(&self) -> Option<DfsAuthoritativePrefixProjection> {
        self.snapshot(None).complete_prefix()
    }

    pub fn retained_complete_prefix_revision_for_generation(
        &self,
        expected_generation: u64,
    ) -> Option<KvRevision> {
        (self.generation == expected_generation)
            .then_some(self.revision)
            .flatten()
    }

    pub fn shard_projection_state(
        &self,
        file_system_id: &DfsFileSystemId,
        shard_id: &DfsMetadataShardId,
    ) -> (u64, bool) {
        (
            self.generation,
            self.snapshot(None)
                .contains_shard_key(file_system_id, shard_id),
        )
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn projection_rebuild_count(&self) -> u64 {
        self.projection_rebuild_count
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn debug_snapshot(&self) -> DfsMetadataAuthorityDebugSnapshot {
        DfsMetadataAuthorityDebugSnapshot {
            revision: self.revision,
            required_revision: self.required_revision,
            topology_comparison_baseline: self.topology_comparison_baseline,
            entry_count: self.entries.len(),
            metadata_file_system_count: self.shards.len(),
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
        let mut state = DfsMetadataAuthorityState::new();
        let owner =
            sinomo_core::node::NodeId::from_legacy_named("storage_generation_owner").unwrap();
        let mut config = crate::DfsFileSystemConfig::from(&crate::distributed_sifs(owner));
        config.subtree_policies.push(crate::DfsSubtreePolicy::new(
            crate::SIFS_USERS_PATH,
            sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            1,
            1,
        ));
        let original = config_entry(&config, 1);
        state
            .install_authoritative_projection(projection(1, vec![original.clone()]))
            .unwrap();
        let storage_generation = state.storage_generation();
        let authority_generation = state.generation();
        let policy = config
            .subtree_policies
            .iter_mut()
            .find(|policy| {
                policy.path_prefix == crate::SIFS_USERS_PATH
                    && policy.scope == crate::DfsPlacementScope::Subtree
            })
            .expect("explicit users subtree policy");
        policy.min_commit_replicas = 2;
        policy.desired_replicas = 3;
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
        let mut state = DfsMetadataAuthorityState::new();
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
        let mut state = DfsMetadataAuthorityState::new();
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
        let mut state = DfsMetadataAuthorityState::new();
        let owner = sinomo_core::node::NodeId::from_legacy_named("gap_policy_store_owner").unwrap();
        let mut config = crate::DfsFileSystemConfig::from(&crate::distributed_sifs(owner));
        config.subtree_policies.push(crate::DfsSubtreePolicy::new(
            crate::SIFS_USERS_PATH,
            sinomo_core::DfsObjectPolicy::ReplicatedEventual,
            1,
            1,
        ));
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
        let policy = config
            .subtree_policies
            .iter_mut()
            .find(|policy| {
                policy.path_prefix == crate::SIFS_USERS_PATH
                    && policy.scope == crate::DfsPlacementScope::Subtree
            })
            .expect("explicit users subtree policy");
        policy.min_commit_replicas = 2;
        policy.desired_replicas = 3;
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
        let mut state = DfsMetadataAuthorityState::new();
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
        let mut state = DfsMetadataAuthorityState::new();
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
        let mut state = DfsMetadataAuthorityState::new();
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
