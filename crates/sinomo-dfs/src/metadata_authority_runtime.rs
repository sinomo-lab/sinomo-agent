use crate::{
    DfsAuthoritativePrefixProjection, DfsError, DfsFileSystemId, DfsMetadataReplicaShardContext,
    DfsMetadataShardConfig, DfsMetadataShardId, DfsNamespaceCommand,
    dfs_metadata_authority_key_is_exact, metadata_replica_context_matches_authority_entries,
    metadata_shard_index_from_authority_entries, namespace_command_matches_authority_index,
};
use sinomo_core::kv::{ClusterKvCommittedDelta, KvEntry, KvEvent, KvRevision};
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
        }
    }

    const fn invalidated() -> Self {
        Self {
            accepted: false,
            authority_changed: false,
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
    #[cfg(any(test, feature = "test-support"))]
    projection_rebuild_count: AtomicU64,
    #[cfg(any(test, feature = "test-support"))]
    projection_materialization_count: AtomicU64,
}

pub struct DfsMetadataAuthorityRuntime {
    state: DfsMetadataAuthorityState,
    host: Mutex<Option<Arc<dyn DfsMetadataAuthorityHost>>>,
    delta_subscription: OnceLock<sinomo_core::cluster::ClusterKvCommittedDeltaSubscription>,
    refresh_in_flight: AtomicBool,
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
        self.runtime
            .refresh_in_flight
            .store(false, Ordering::Release);
        self.runtime
            .refresh_completion
            .send_modify(|generation| *generation = generation.saturating_add(1));
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
            host: Mutex::new(None),
            delta_subscription: OnceLock::new(),
            refresh_in_flight: AtomicBool::new(false),
            refresh_completion: tokio::sync::watch::channel(0).0,
            #[cfg(any(test, feature = "test-support"))]
            refresh_before_claim: Mutex::new(None),
            #[cfg(any(test, feature = "test-support"))]
            refresh_count: AtomicU64::new(0),
        }
    }

    pub fn attach_host(&self, host: Arc<dyn DfsMetadataAuthorityHost>) -> bool {
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

    pub fn generation(&self) -> u64 {
        self.state.generation()
    }

    pub fn projection_generation(&self) -> u64 {
        self.state.projection_generation()
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
        if revision.is_none() {
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
        let observation = self.state.observe_delta(delta);
        if observation.authority_changed {
            self.host()
                .into_iter()
                .for_each(|host| host.authority_changed());
        }
        observation.accepted
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
        let Some(refresh_owner) = self.try_claim_refresh() else {
            let _ = completion.changed().await;
            return self
                .state
                .retained_complete_prefix_revision_for_generation(expected_generation);
        };
        let retained = self
            .state
            .retained_complete_prefix_revision_for_generation(expected_generation);
        if retained.is_some() || self.generation() != expected_generation {
            drop(refresh_owner);
            return retained;
        }
        self.record_refresh();
        let read = host.read_authoritative_prefix().await;
        let proof_revision = self.apply_read_result(read, "async cold");
        let refresh_required =
            proof_revision.is_some_and(|revision| self.state.refresh_required_after(revision));
        drop(refresh_owner);
        if refresh_required {
            self.request_authoritative_refresh();
        }
        self.state
            .retained_complete_prefix_revision_for_generation(expected_generation)
    }

    pub fn request_authoritative_refresh(self: &Arc<Self>) {
        let Some(host) = self.host() else {
            return;
        };
        let Some(refresh_owner) = self.try_claim_refresh() else {
            return;
        };
        if self.state.current_complete_prefix_revision().is_some() {
            drop(refresh_owner);
            return;
        }
        self.record_refresh();
        let runtime = Arc::clone(self);
        let task_host = Arc::clone(&host);
        host.spawn(Box::pin(async move {
            let read = task_host.read_authoritative_prefix().await;
            let proof_revision = runtime.apply_read_result(read, "bounded");
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
                        let _ = self.install_authoritative_projection(projection);
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
        let (entries, revision, _) = projection.into_parts();
        let Ok(mut observed) = self.projection.lock() else {
            return None;
        };
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
        if authority_changed && (replaced_valid_projection || observed.topology_comparison_baseline)
        {
            Self::advance(&self.generation);
        }
        observed.revision = Some(revision);
        observed.required_revision = revision;
        observed.topology_comparison_baseline = true;
        observed.entries = entries;
        Self::rebuild_metadata_shard_index_locked(&mut observed);
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

    pub fn observe_delta(
        &self,
        delta: &ClusterKvCommittedDelta,
    ) -> DfsMetadataAuthorityDeltaObservation {
        let Ok(mut observed) = self.projection.lock() else {
            return DfsMetadataAuthorityDeltaObservation::invalidated();
        };
        let Some(previous_revision) = observed.revision else {
            self.invalidate_for_revision_catchup_locked(&mut observed, delta.revision);
            return DfsMetadataAuthorityDeltaObservation::invalidated();
        };
        if delta.revision <= previous_revision {
            return DfsMetadataAuthorityDeltaObservation::accepted(false);
        }
        if previous_revision < delta.compacted_revision
            || delta.events.is_empty()
            || !kv_events_cover_revision_range(previous_revision, delta.revision, &delta.events)
        {
            self.invalidate_for_revision_catchup_locked(&mut observed, delta.revision);
            return DfsMetadataAuthorityDeltaObservation::invalidated();
        }

        let relevant_event_keys = delta
            .events
            .iter()
            .filter(|event| event.key.starts_with(DFS_METADATA_AUTHORITY_PREFIX))
            .map(|event| event.key.as_str())
            .collect::<BTreeSet<_>>();
        let mut final_keys = BTreeSet::new();
        let mut relevant_final_keys = BTreeSet::new();
        for state in &delta.final_key_states {
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
                return DfsMetadataAuthorityDeltaObservation::invalidated();
            }
            if state.key.starts_with(DFS_METADATA_AUTHORITY_PREFIX) {
                relevant_final_keys.insert(state.key.as_str());
            }
        }
        if relevant_event_keys != relevant_final_keys {
            self.invalidate_for_revision_catchup_locked(&mut observed, delta.revision);
            return DfsMetadataAuthorityDeltaObservation::invalidated();
        }

        let mut authority_changed = false;
        let mut prefix_changed = false;
        for state in &delta.final_key_states {
            if !state.key.starts_with(DFS_METADATA_AUTHORITY_PREFIX) {
                continue;
            }
            let authority_key = dfs_metadata_authority_key_is_exact(&state.key);
            let position = observed
                .entries
                .binary_search_by(|entry| entry.key.as_str().cmp(state.key.as_str()));
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
            return DfsMetadataAuthorityDeltaObservation::invalidated();
        }
        if authority_changed {
            Self::advance(&self.generation);
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
        || (events.first().map(|event| event.revision) == Some(previous_revision.next())
            && events.last().map(|event| event.revision) == Some(current_revision)
            && events.windows(2).all(|events| {
                let previous = events[0].revision;
                let current = events[1].revision;
                current == previous || current == previous.next()
            }))
}

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
            }],
            final_key_states: vec![ClusterKvCommittedKeyState {
                key: key.to_string(),
                entry: Some(entry(key, b"v2", 3, 2)),
            }],
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
