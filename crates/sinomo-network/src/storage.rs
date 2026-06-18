use crate::ClusterPaths;
use crate::connectivity::{
    ClusterConnectivityGraph, ClusterConnectivityProbeBatch, ClusterConnectivityProbeBatchEntry,
    ClusterConnectivityProbeState, ClusterConnectivityRecord, ClusterConnectivityState,
    cluster_connectivity_probe_due_with_graph, cluster_connectivity_probe_key,
};
pub(crate) use crate::connectivity_publish::{
    ClusterConnectivityPublishHost, ClusterNetworkRecordExpiryHost,
};
#[cfg(test)]
pub(crate) use crate::connectivity_publish::{
    MAX_CLUSTER_CONNECTIVITY_SOURCE_RECORDS, cluster_connectivity_full_sync_batch_ops,
    cluster_connectivity_stable_eq, cluster_connectivity_sync_is_noop,
    cluster_network_expiry_delete_ops, expire_cluster_network_record_ops,
    expire_cluster_network_records,
};
#[cfg(test)]
pub(crate) use crate::connectivity_store::{
    local_cluster_connectivity_prune_passes_for_test,
    local_cluster_connectivity_record_cache_capacity_for_test,
    local_cluster_connectivity_record_cache_contains_for_test,
    remove_local_cluster_connectivity_record_cache_entries_under_root_for_test,
    reset_local_cluster_connectivity_prune_passes_for_test,
    write_local_cluster_connectivity_record_set,
};
use crate::endpoint_records::{
    cleanup_cluster_iroh_endpoints, cleanup_member_iroh_endpoints, cluster_endpoint_list_stable_eq,
    cluster_endpoint_stable_eq, upsert_member_endpoint,
};
use crate::endpoint_selector::{
    ClusterEndpointSelectionPurpose, ClusterEndpointSelector, cluster_raft_live_voter_targets,
};
use crate::error::NetworkError;
use crate::invite_publish::{
    ClusterInvitePublishHost, handle_cluster_invite_publish_request_with_host,
    read_cluster_invitation_from_kv,
};
use crate::io_observer;
#[cfg(test)]
pub(crate) use crate::keyspace::cluster_certificate_key;
pub(crate) use crate::keyspace::{
    cluster_capability_policy_key, cluster_node_status_key, cluster_node_status_key_for_process,
};
use crate::keyspace::{
    cluster_connectivity_source_prefix, cluster_endpoint_key, cluster_endpoint_prefix,
    cluster_identity_key, cluster_member_key,
};
use crate::kv_forward::{
    ClusterKvTxnForwardAsyncHost, ClusterKvTxnForwardHost, ClusterKvTxnForwardOptions,
    ClusterKvTxnForwardResult, forward_cluster_kv_txn_to_voter_async_with_host,
    forward_cluster_kv_txn_to_voter_with_host,
};
pub(crate) use crate::kv_read_model::{
    cluster_node_status_map_from_kv, read_cluster_capability_policies_from_snapshot,
    read_cluster_members_from_kv, read_cluster_node_statuses_from_kv,
};
#[cfg(test)]
pub(crate) use crate::kv_read_model::{
    read_cluster_endpoints_from_kv, read_cluster_network_info_from_kv,
};
use crate::kv_semantics::cluster_semantic_txn_no_effect_response;
use crate::kv_snapshot::{
    ClusterKvSnapshotRefreshHost, ClusterKvSnapshotRefreshOptions,
    refresh_current_cluster_kv_store_with_host,
};
use crate::local_membership::{
    ClusterLocalDaemonMembershipObservation, apply_cluster_node_role,
    locally_learned_remote_route_endpoints, merge_cluster_join_registration,
    preserve_locally_learned_remote_route_endpoints, refresh_local_daemon_membership_from_disk,
};
pub(crate) use crate::local_membership::{
    apply_local_daemon_membership_observation, cluster_state_needs_join_registration,
};
#[cfg(test)]
pub(crate) use crate::local_membership::{
    ensure_main_daemon_cluster_member,
    prepare_cluster_join_registration_state_with_local_daemon_members,
};
use crate::maintenance::{
    CLUSTER_CONNECTIVITY_REFRESH_MARGIN_SECONDS, CLUSTER_NETWORK_PUBLISH_INTERVAL,
    cluster_connectivity_probe_max_endpoints_per_tick_for_role,
    cluster_network_publish_interval_for_role,
};
use crate::member_publish::{
    ClusterLocalMemberPublishHost, ClusterMemberPublishHost,
    handle_cluster_member_publish_request_with_host,
};
use crate::membership::{
    DAEMON_SHADOW_CLUSTER_ROLE, cluster_capabilities_are_legacy_shadow_only,
    cluster_member_is_legacy_shadow_daemon, cluster_member_is_local_process,
    cluster_node_is_local_process,
};
use crate::membership_forward::{
    ClusterMembershipRpcForwardHost, forward_cluster_membership_rpc_to_voter_async_with_host,
    forward_cluster_membership_rpc_to_voter_with_host,
};
use crate::membership_rpc::{
    ClusterAuthenticatedPeer, ClusterInvitePublishRequest, ClusterJoinRegistrationRequest,
    ClusterJoinRegistrationResponse, ClusterMemberPublishRequest, ClusterMemberPublishResponse,
};
use crate::network_info::ClusterNetworkInfoRecord;
use crate::object::SinomoObjectCodec;
use crate::prelude::*;
pub(crate) use crate::records::ClusterCapabilityPolicyRecord;
use crate::records::{
    ClusterIdentityRecord, cluster_capability_policy_stable_eq, cluster_identity_record_stable_eq,
};
use crate::route::ClusterSignedEndpointRpcAsyncHost;
pub(crate) use crate::route_graph_read_model::read_cluster_connectivity_graph;
#[cfg(test)]
pub(crate) use crate::route_graph_read_model::read_cluster_connectivity_graph_with_query;
use crate::rpc::{
    CLUSTER_INVITE_PUBLISH_ROUTE, CLUSTER_JOIN_CHALLENGE_ROUTE, CLUSTER_JOIN_REGISTER_ROUTE,
    CLUSTER_MEMBER_PUBLISH_ROUTE, CLUSTER_MEMBERSHIP_RPC_CATEGORY, ObjectRpcRequest,
    ObjectRpcResponse,
};
use crate::status_publish::{ClusterNetworkInfoPublishHost, ClusterNodeStatusPublishHost};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime};

use sinomo_kv::{
    CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES, ClusterKvRaftCommitResult, ClusterKvRaftConfigRecord,
    ClusterKvRaftLeaderElectionOptions, ClusterKvRaftLocalVoteMetadata,
    ClusterKvRaftUncommittedAppend, ClusterRaftAppendRequest, ClusterRaftAppendResponse,
    ClusterRaftVoteRequest, ClusterRaftVoteResponse, KvRevision,
};
#[cfg(test)]
use sinomo_kv::{
    ClusterKvWatchCancelRequest, ClusterKvWatchCancelResponse, ClusterKvWatchDrainRequest,
    ClusterKvWatchDrainResponse, ClusterKvWatchPrefixRequest, ClusterKvWatchRegistration,
};
use sinomo_storage::durable_file::file_ino;

mod durable_object_file;

#[cfg(any(test, feature = "iroh-transport"))]
pub(crate) use durable_object_file::write_object_file_create_new_serialized;
#[cfg(test)]
pub(crate) use durable_object_file::{
    MAX_OBJECT_FILE_READ_BYTES, clear_write_object_file_create_new_unsupported_link_for_test,
    force_write_object_file_create_new_unsupported_link_for_test,
    object_file_write_mark_cache_capacity_for_test, object_file_write_mark_cache_contains_for_test,
    record_object_file_write_mark_for_test, refresh_object_file_write_mark_for_test,
    remove_object_file_write_mark_entries_under_root_for_test,
};
pub(crate) use durable_object_file::{
    read_object_file, read_optional_object_file, write_object_file,
};
#[cfg(all(test, feature = "iroh-transport"))]
pub(crate) use durable_object_file::{
    reset_write_object_file_create_new_attempts_for_test,
    write_object_file_create_new_attempts_for_test,
};

pub(crate) const MAX_CLUSTER_MEMBER_ENDPOINT_RECORDS: usize = CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES;
#[cfg(test)]
const MAX_CLUSTER_KV_WATCH_EVENT_CACHE_ENTRIES: usize = 128;
#[cfg(test)]
const MAX_CLUSTER_KV_WATCH_REGISTRATION_CACHE_ENTRIES: usize =
    sinomo_kv::MAX_CLUSTER_KV_ACTIVE_WATCHES;
pub use sinomo_node::DAEMON_MAIN_RUN_CAPABILITY;
const FILE_CACHE_CAPACITY: usize = 4096;
const MAX_CLUSTER_STATE_CACHE_ENTRIES: usize = 128;
const CLUSTER_STATE_CACHE_MAX_AGE: Duration = Duration::from_secs(1);
#[cfg(test)]
static CLUSTER_KV_WATCH_CACHE_CLOCK: AtomicU64 = AtomicU64::new(1);
static CLUSTER_KV_METADATA_CACHE_CLOCK: AtomicU64 = AtomicU64::new(1);
static CLUSTER_STATE_CACHE_CLOCK: AtomicU64 = AtomicU64::new(1);
static CLUSTER_RAFT_FANOUT_ACTIVE: AtomicBool = AtomicBool::new(false);
static CLUSTER_RAFT_FANOUT_PENDING: AtomicBool = AtomicBool::new(false);
static CLUSTER_STATES: LazyLock<Mutex<HashMap<PathBuf, ClusterStateCacheEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(test)]
static CLUSTER_KV_WATCH_EVENT_CACHE: LazyLock<
    Mutex<HashMap<PathBuf, ClusterKvWatchEventCacheEntry>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(test)]
static CLUSTER_KV_WATCH_EVENT_APPEND_LOCKS: LazyLock<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(test)]
static CLUSTER_KV_WATCH_REGISTRATION_CACHE: LazyLock<
    Mutex<HashMap<PathBuf, ClusterKvWatchRegistrationCacheEntry>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(test)]
static CLUSTER_KV_WATCH_ALLOCATOR_TRUST: LazyLock<
    Mutex<HashMap<PathBuf, ClusterKvWatchAllocatorTrustEntry>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
static CLUSTER_KV_READER_NODE_ID_CACHE: LazyLock<
    Mutex<HashMap<PathBuf, ClusterKvReaderNodeIdCacheEntry>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(test)]
static CLUSTER_KV_WATCH_DRAIN_ONCE_CALLS_FOR_TEST: LazyLock<Mutex<HashMap<PathBuf, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(test)]
static CLUSTER_KV_WATCH_EVENT_LOG_READ_BYTES_FOR_TEST: LazyLock<Mutex<HashMap<PathBuf, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(test)]
static CLUSTER_KV_WATCH_EVENT_LOG_WRITE_BYTES_FOR_TEST: LazyLock<Mutex<HashMap<PathBuf, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(test)]
static CLUSTER_KV_WATCH_DIR_SCANS_FOR_TEST: LazyLock<Mutex<HashMap<PathBuf, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(test)]
static CLUSTER_KV_WATCH_ALLOCATOR_PERSIST_FAILURES_FOR_TEST: LazyLock<
    Mutex<HashMap<PathBuf, u64>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(test)]
static CLUSTER_KV_CACHED_SNAPSHOT_READS_FOR_TEST: LazyLock<Mutex<HashMap<PathBuf, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(test)]
static CLUSTER_KV_CURRENT_SNAPSHOT_READS_FOR_TEST: LazyLock<Mutex<HashMap<PathBuf, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ClusterFileMark {
    exists: bool,
    len: u64,
    modified: Option<SystemTime>,
    ino: u64,
}

#[cfg(test)]
#[derive(Clone)]
struct ClusterKvWatchEventCacheEntry {
    checkpoint: ClusterFileMark,
    log: ClusterFileMark,
    events: Arc<Vec<KvEvent>>,
    log_records: usize,
    append_safe: bool,
    last_used: u64,
}

#[cfg(test)]
#[derive(Clone)]
struct ClusterKvWatchRegistrationCacheEntry {
    mark: ClusterFileMark,
    registration: ClusterKvWatchRegistration,
    durable_next_revision: KvRevision,
    empty_drain_mark: Option<ClusterKvWatchEmptyDrainMark>,
    last_used: u64,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ClusterKvWatchEmptyDrainMark {
    generation: sinomo_kv::ClusterKvWatchObservedGeneration,
    state: ClusterFileMark,
    materialized_snapshot: ClusterFileMark,
    raft_hard_state: ClusterFileMark,
    raft_wal: ClusterFileMark,
    raft_snapshot: ClusterFileMark,
}

#[derive(Clone)]
struct ClusterStateCacheEntry {
    mark: ClusterStateCacheMark,
    state: ClusterBootstrapState,
    cached_at: Instant,
    last_used: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ClusterStateCacheMark {
    state: ClusterFileMark,
    kv_snapshot: ClusterFileMark,
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
struct ClusterKvWatchAllocatorTrustEntry {
    allocator_mark: ClusterFileMark,
    watch_dir_mark: ClusterFileMark,
    active_watch_count: Option<usize>,
    active_registrations: Option<Vec<ClusterKvWatchRegistration>>,
    last_used: u64,
}

#[derive(Clone)]
struct ClusterKvReaderNodeIdCacheEntry {
    mark: ClusterFileMark,
    node_id: NodeId,
    last_used: u64,
}

pub(crate) fn cluster_file_mark(path: &PathBuf) -> Option<ClusterFileMark> {
    match fs::metadata(path) {
        Ok(metadata) => Some(ClusterFileMark {
            exists: true,
            len: metadata.len(),
            modified: metadata.modified().ok(),
            ino: file_ino(&metadata),
        }),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Some(ClusterFileMark {
            exists: false,
            len: 0,
            modified: None,
            ino: 0,
        }),
        Err(_) => None,
    }
}

fn cluster_state_cache_tick() -> u64 {
    CLUSTER_STATE_CACHE_CLOCK.fetch_add(1, Ordering::Relaxed)
}

fn cluster_state_cache_mark(paths: &ClusterPaths) -> Option<ClusterStateCacheMark> {
    Some(ClusterStateCacheMark {
        state: cluster_file_mark(&paths.state)?,
        kv_snapshot: cluster_file_mark(&paths.kv_snapshot)?,
    })
}

fn read_cluster_state_from_cache(
    paths: &ClusterPaths,
    mark: Option<ClusterStateCacheMark>,
    checked_at: Instant,
) -> Option<ClusterBootstrapState> {
    let mark = mark?;
    let mut cache = CLUSTER_STATES.lock().ok()?;
    let stale = {
        let entry = cache.get(&paths.root)?;
        entry.mark != mark
            || checked_at.duration_since(entry.cached_at) >= CLUSTER_STATE_CACHE_MAX_AGE
    };
    if stale {
        cache.remove(&paths.root);
        return None;
    }
    let entry = cache.get_mut(&paths.root)?;
    entry.last_used = cluster_state_cache_tick();
    Some(entry.state.clone())
}

fn record_cluster_state_cache(
    paths: &ClusterPaths,
    mark: ClusterStateCacheMark,
    state: ClusterBootstrapState,
    checked_at: Instant,
) {
    let Ok(mut cache) = CLUSTER_STATES.lock() else {
        return;
    };
    evict_cluster_state_cache_entry_if_needed(&mut cache, &paths.root);
    cache.insert(
        paths.root.clone(),
        ClusterStateCacheEntry {
            mark,
            state,
            cached_at: checked_at,
            last_used: cluster_state_cache_tick(),
        },
    );
}

fn evict_cluster_state_cache_entry_if_needed(
    cache: &mut HashMap<PathBuf, ClusterStateCacheEntry>,
    protected: &PathBuf,
) {
    if cache.contains_key(protected) || cache.len() < MAX_CLUSTER_STATE_CACHE_ENTRIES {
        return;
    }
    let eviction_candidate = cache
        .iter()
        .min_by_key(|(_, entry)| entry.last_used)
        .map(|(path, _)| path.clone());
    if let Some(path) = eviction_candidate {
        cache.remove(&path);
    }
}

#[cfg(test)]
fn cluster_kv_watch_event_cache_marks(
    paths: &ClusterPaths,
) -> Option<(ClusterFileMark, ClusterFileMark)> {
    Some((
        cluster_file_mark(&paths.kv_watch_events)?,
        cluster_file_mark(&paths.kv_watch_events_log)?,
    ))
}

#[cfg(test)]
fn cluster_kv_watch_empty_drain_mark(
    paths: &ClusterPaths,
    generation: sinomo_kv::ClusterKvWatchObservedGeneration,
) -> Option<ClusterKvWatchEmptyDrainMark> {
    let node_id = read_cluster_kv_reader_node_id(paths).ok()?;
    let raft_paths = cluster_raft_wal_paths(paths, &node_id);
    Some(ClusterKvWatchEmptyDrainMark {
        generation,
        state: cluster_file_mark(&paths.state)?,
        materialized_snapshot: cluster_file_mark(&paths.kv_snapshot)?,
        raft_hard_state: cluster_file_mark(&raft_paths.hard_state)?,
        raft_wal: cluster_file_mark(&raft_paths.wal)?,
        raft_snapshot: cluster_file_mark(&raft_paths.snapshot)?,
    })
}

#[cfg(test)]
fn cluster_kv_watch_cache_tick() -> u64 {
    CLUSTER_KV_WATCH_CACHE_CLOCK.fetch_add(1, Ordering::Relaxed)
}

#[cfg(test)]
fn evict_cluster_kv_watch_event_cache_entry_if_needed(
    cache: &mut HashMap<PathBuf, ClusterKvWatchEventCacheEntry>,
    protected: &PathBuf,
) {
    if cache.contains_key(protected) || cache.len() < MAX_CLUSTER_KV_WATCH_EVENT_CACHE_ENTRIES {
        return;
    }
    let eviction_candidate = cache
        .iter()
        .filter(|(path, _)| *path != protected)
        .min_by_key(|(_, entry)| entry.last_used)
        .map(|(path, _)| path.clone());
    if let Some(path) = eviction_candidate {
        cache.remove(&path);
    }
}

#[cfg(test)]
fn evict_cluster_kv_watch_registration_cache_entry_if_needed(
    cache: &mut HashMap<PathBuf, ClusterKvWatchRegistrationCacheEntry>,
    protected: &PathBuf,
) {
    if cache.contains_key(protected)
        || cache.len() < MAX_CLUSTER_KV_WATCH_REGISTRATION_CACHE_ENTRIES
    {
        return;
    }
    let eviction_candidate = cache
        .iter()
        .filter(|(path, _)| *path != protected)
        .min_by_key(|(_, entry)| entry.last_used)
        .map(|(path, _)| path.clone());
    if let Some(path) = eviction_candidate {
        cache.remove(&path);
    }
}

#[cfg(test)]
fn evict_cluster_kv_watch_allocator_trust_entry_if_needed(
    cache: &mut HashMap<PathBuf, ClusterKvWatchAllocatorTrustEntry>,
    protected: &PathBuf,
) {
    if cache.contains_key(protected) || cache.len() < FILE_CACHE_CAPACITY {
        return;
    }
    let eviction_candidate = cache
        .iter()
        .filter(|(path, _)| *path != protected)
        .min_by_key(|(_, entry)| entry.last_used)
        .map(|(path, _)| path.clone());
    if let Some(path) = eviction_candidate {
        cache.remove(&path);
    }
}

#[cfg(test)]
fn current_cluster_kv_watch_allocator_trust_marks(
    paths: &ClusterPaths,
) -> Option<(ClusterFileMark, ClusterFileMark)> {
    let allocator_mark = cluster_file_mark(&paths.kv_watches_next_id)?;
    if !allocator_mark.exists {
        return None;
    }
    let watch_dir_mark = cluster_file_mark(&paths.kv_watches)?;
    if !watch_dir_mark.exists {
        return None;
    }
    Some((allocator_mark, watch_dir_mark))
}

#[cfg(test)]
fn cluster_kv_watch_allocator_trusted(paths: &ClusterPaths) -> bool {
    let Some((allocator_mark, watch_dir_mark)) =
        current_cluster_kv_watch_allocator_trust_marks(paths)
    else {
        return false;
    };
    let Ok(mut cache) = CLUSTER_KV_WATCH_ALLOCATOR_TRUST.lock() else {
        return false;
    };
    let Some(entry) = cache.get_mut(&paths.kv_watches_next_id) else {
        return false;
    };
    if entry.allocator_mark != allocator_mark || entry.watch_dir_mark != watch_dir_mark {
        cache.remove(&paths.kv_watches_next_id);
        return false;
    }
    entry.last_used = cluster_kv_watch_cache_tick();
    true
}

#[cfg(test)]
fn cached_cluster_kv_active_watch_registrations(
    paths: &ClusterPaths,
) -> Option<Vec<ClusterKvWatchRegistration>> {
    let (allocator_mark, watch_dir_mark) = current_cluster_kv_watch_allocator_trust_marks(paths)?;
    let mut cache = CLUSTER_KV_WATCH_ALLOCATOR_TRUST.lock().ok()?;
    let entry = cache.get_mut(&paths.kv_watches_next_id)?;
    if entry.allocator_mark != allocator_mark || entry.watch_dir_mark != watch_dir_mark {
        cache.remove(&paths.kv_watches_next_id);
        return None;
    }
    let registrations = entry.active_registrations.clone()?;
    if entry.active_watch_count != Some(registrations.len()) {
        cache.remove(&paths.kv_watches_next_id);
        return None;
    }
    entry.last_used = cluster_kv_watch_cache_tick();
    Some(registrations)
}

#[cfg(test)]
fn refresh_cached_cluster_kv_active_watch_registration(
    paths: &ClusterPaths,
    registration: &ClusterKvWatchRegistration,
) {
    let Some((allocator_mark, watch_dir_mark)) =
        current_cluster_kv_watch_allocator_trust_marks(paths)
    else {
        return;
    };
    let Ok(mut cache) = CLUSTER_KV_WATCH_ALLOCATOR_TRUST.lock() else {
        return;
    };
    let Some(entry) = cache.get_mut(&paths.kv_watches_next_id) else {
        return;
    };
    if entry.allocator_mark != allocator_mark || entry.watch_dir_mark != watch_dir_mark {
        cache.remove(&paths.kv_watches_next_id);
        return;
    }
    let Some(registrations) = entry.active_registrations.as_mut() else {
        return;
    };
    if let Some(cached) = registrations
        .iter_mut()
        .find(|cached| cached.watch_id == registration.watch_id)
    {
        *cached = registration.clone();
        entry.last_used = cluster_kv_watch_cache_tick();
    }
}

#[cfg(test)]
fn cluster_kv_watch_event_append_lock(paths: &ClusterPaths) -> Arc<Mutex<()>> {
    let key = cluster_kv_watch_notify_key(paths);
    let mut locks = CLUSTER_KV_WATCH_EVENT_APPEND_LOCKS
        .lock()
        .expect("cluster kv watch event append locks");
    locks
        .entry(key)
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

#[cfg(test)]
fn cached_cluster_kv_watch_events_arc(
    paths: &ClusterPaths,
    checkpoint: ClusterFileMark,
    log: ClusterFileMark,
) -> Option<Arc<Vec<KvEvent>>> {
    cached_cluster_kv_watch_event_cache_entry(paths, checkpoint, log).map(|entry| entry.events)
}

#[cfg(test)]
fn cached_cluster_kv_watch_event_cache_entry(
    paths: &ClusterPaths,
    checkpoint: ClusterFileMark,
    log: ClusterFileMark,
) -> Option<ClusterKvWatchEventCacheEntry> {
    let key = cluster_kv_watch_notify_key(paths);
    let mut cache = CLUSTER_KV_WATCH_EVENT_CACHE.lock().ok()?;
    let entry = cache.get_mut(&key)?;
    if entry.checkpoint == checkpoint && entry.log == log {
        entry.last_used = cluster_kv_watch_cache_tick();
        Some(entry.clone())
    } else {
        None
    }
}

#[cfg(test)]
fn cluster_kv_watch_event_append_cache_view(
    paths: &ClusterPaths,
) -> Option<sinomo_kv::ClusterKvWatchEventAppendCacheView> {
    let (checkpoint, log) = cluster_kv_watch_event_cache_marks(paths)?;
    let entry = cached_cluster_kv_watch_event_cache_entry(paths, checkpoint, log)?;
    Some(sinomo_kv::ClusterKvWatchEventAppendCacheView {
        events: entry.events.as_ref().clone(),
        log_records: entry.log_records,
        log_bytes: log.len,
        append_safe: entry.append_safe,
    })
}

#[cfg(test)]
fn record_cluster_kv_watch_event_cache(
    paths: &ClusterPaths,
    checkpoint: ClusterFileMark,
    log: ClusterFileMark,
    events: Vec<KvEvent>,
    log_records: usize,
    append_safe: bool,
) {
    record_cluster_kv_watch_event_cache_arc(
        paths,
        checkpoint,
        log,
        Arc::new(events),
        log_records,
        append_safe,
    );
}

#[cfg(test)]
fn record_cluster_kv_watch_event_cache_arc(
    paths: &ClusterPaths,
    checkpoint: ClusterFileMark,
    log: ClusterFileMark,
    events: Arc<Vec<KvEvent>>,
    log_records: usize,
    append_safe: bool,
) {
    let key = cluster_kv_watch_notify_key(paths);
    if let Ok(mut cache) = CLUSTER_KV_WATCH_EVENT_CACHE.lock() {
        evict_cluster_kv_watch_event_cache_entry_if_needed(&mut cache, &key);
        cache.insert(
            key,
            ClusterKvWatchEventCacheEntry {
                checkpoint,
                log,
                events,
                log_records,
                append_safe,
                last_used: cluster_kv_watch_cache_tick(),
            },
        );
    }
}

#[cfg(test)]
fn record_current_cluster_kv_watch_event_cache(
    paths: &ClusterPaths,
    events: Vec<KvEvent>,
    log_records: usize,
    append_safe: bool,
) {
    if let Some((checkpoint, log)) = cluster_kv_watch_event_cache_marks(paths) {
        record_cluster_kv_watch_event_cache(
            paths,
            checkpoint,
            log,
            events,
            log_records,
            append_safe,
        );
    }
}

#[cfg(test)]
fn cached_cluster_kv_watch_registration_entry(
    path: &PathBuf,
    mark: ClusterFileMark,
) -> Option<ClusterKvWatchRegistrationCacheEntry> {
    let mut cache = CLUSTER_KV_WATCH_REGISTRATION_CACHE.lock().ok()?;
    let entry = cache.get_mut(path)?;
    if entry.mark == mark {
        entry.last_used = cluster_kv_watch_cache_tick();
        Some(entry.clone())
    } else {
        None
    }
}

#[cfg(test)]
fn record_cluster_kv_watch_registration_cache(
    path: &PathBuf,
    mark: ClusterFileMark,
    registration: ClusterKvWatchRegistration,
) {
    record_cluster_kv_watch_registration_cache_with_durable(
        path,
        mark,
        registration.clone(),
        registration.next_revision,
    );
}

#[cfg(test)]
fn record_cluster_kv_watch_registration_cache_with_durable(
    path: &PathBuf,
    mark: ClusterFileMark,
    registration: ClusterKvWatchRegistration,
    durable_next_revision: KvRevision,
) {
    record_cluster_kv_watch_registration_cache_with_durable_and_empty_mark(
        path,
        mark,
        registration,
        durable_next_revision,
        None,
    );
}

#[cfg(test)]
fn record_cluster_kv_watch_registration_cache_with_durable_and_empty_mark(
    path: &PathBuf,
    mark: ClusterFileMark,
    registration: ClusterKvWatchRegistration,
    durable_next_revision: KvRevision,
    empty_drain_mark: Option<ClusterKvWatchEmptyDrainMark>,
) {
    if let Ok(mut cache) = CLUSTER_KV_WATCH_REGISTRATION_CACHE.lock() {
        evict_cluster_kv_watch_registration_cache_entry_if_needed(&mut cache, path);
        cache.insert(
            path.clone(),
            ClusterKvWatchRegistrationCacheEntry {
                mark,
                registration,
                durable_next_revision,
                empty_drain_mark,
                last_used: cluster_kv_watch_cache_tick(),
            },
        );
    }
}

#[cfg(test)]
fn record_cluster_kv_watch_registration_empty_drain_cache(
    path: &PathBuf,
    registration_read: &ClusterKvWatchRegistrationRead,
    registration: ClusterKvWatchRegistration,
    empty_drain_mark: ClusterKvWatchEmptyDrainMark,
) {
    record_cluster_kv_watch_registration_cache_with_durable_and_empty_mark(
        path,
        registration_read.mark,
        registration,
        registration_read.durable_next_revision,
        Some(empty_drain_mark),
    );
}

#[cfg(test)]
fn record_current_cluster_kv_watch_registration_cache(
    path: &PathBuf,
    registration: ClusterKvWatchRegistration,
) {
    if let Some(mark) = cluster_file_mark(path)
        && mark.exists
    {
        record_cluster_kv_watch_registration_cache(path, mark, registration);
    }
}

#[cfg(test)]
fn invalidate_cluster_kv_watch_registration_cache(path: &PathBuf) {
    if let Ok(mut cache) = CLUSTER_KV_WATCH_REGISTRATION_CACHE.lock() {
        cache.remove(path);
    }
}

#[cfg(test)]
fn record_current_cluster_kv_watch_allocator_trust(
    paths: &ClusterPaths,
    active_watch_count: Option<usize>,
) {
    record_current_cluster_kv_watch_allocator_trust_with_active_registrations(
        paths,
        active_watch_count,
        None,
    );
}

#[cfg(test)]
fn record_current_cluster_kv_watch_allocator_trust_with_active_registrations(
    paths: &ClusterPaths,
    active_watch_count: Option<usize>,
    active_registrations: Option<Vec<ClusterKvWatchRegistration>>,
) {
    let Some(allocator_mark) = cluster_file_mark(&paths.kv_watches_next_id) else {
        return;
    };
    if !allocator_mark.exists {
        return;
    }
    let Some(watch_dir_mark) = cluster_file_mark(&paths.kv_watches) else {
        return;
    };
    if !watch_dir_mark.exists {
        return;
    }
    if let Ok(mut cache) = CLUSTER_KV_WATCH_ALLOCATOR_TRUST.lock() {
        evict_cluster_kv_watch_allocator_trust_entry_if_needed(
            &mut cache,
            &paths.kv_watches_next_id,
        );
        cache.insert(
            paths.kv_watches_next_id.clone(),
            ClusterKvWatchAllocatorTrustEntry {
                allocator_mark,
                watch_dir_mark,
                active_watch_count,
                active_registrations,
                last_used: cluster_kv_watch_cache_tick(),
            },
        );
    }
}

fn cluster_kv_metadata_cache_tick() -> u64 {
    CLUSTER_KV_METADATA_CACHE_CLOCK.fetch_add(1, Ordering::Relaxed)
}

fn evict_cluster_kv_reader_node_id_cache_entry_if_needed(
    cache: &mut HashMap<PathBuf, ClusterKvReaderNodeIdCacheEntry>,
    protected: &PathBuf,
) {
    if cache.contains_key(protected) || cache.len() < FILE_CACHE_CAPACITY {
        return;
    }
    let eviction_candidate = cache
        .iter()
        .filter(|(path, _)| *path != protected)
        .min_by_key(|(_, entry)| entry.last_used)
        .map(|(path, _)| path.clone());
    if let Some(path) = eviction_candidate {
        cache.remove(&path);
    }
}

fn cached_cluster_kv_reader_node_id(path: &PathBuf, mark: ClusterFileMark) -> Option<NodeId> {
    let mut cache = CLUSTER_KV_READER_NODE_ID_CACHE.lock().ok()?;
    let entry = cache.get_mut(path)?;
    if entry.mark == mark {
        entry.last_used = cluster_kv_metadata_cache_tick();
        Some(entry.node_id.clone())
    } else {
        None
    }
}

fn record_cluster_kv_reader_node_id_cache(path: &PathBuf, mark: ClusterFileMark, node_id: NodeId) {
    if let Ok(mut cache) = CLUSTER_KV_READER_NODE_ID_CACHE.lock() {
        evict_cluster_kv_reader_node_id_cache_entry_if_needed(&mut cache, path);
        cache.insert(
            path.clone(),
            ClusterKvReaderNodeIdCacheEntry {
                mark,
                node_id,
                last_used: cluster_kv_metadata_cache_tick(),
            },
        );
    }
}

fn record_current_cluster_kv_reader_node_id_cache(paths: &ClusterPaths, node_id: NodeId) {
    if let Some(mark) = cluster_file_mark(&paths.state)
        && mark.exists
    {
        record_cluster_kv_reader_node_id_cache(&paths.state, mark, node_id);
    }
}

#[cfg(test)]
fn invalidate_cluster_kv_reader_node_id_cache(path: &PathBuf) {
    if let Ok(mut cache) = CLUSTER_KV_READER_NODE_ID_CACHE.lock() {
        cache.remove(path);
    }
}

#[cfg(test)]
pub(crate) fn cached_cluster_kv_watch_events_for_test(
    paths: &ClusterPaths,
) -> Option<Vec<KvEvent>> {
    let (checkpoint, log) = cluster_kv_watch_event_cache_marks(paths)?;
    cached_cluster_kv_watch_events_arc(paths, checkpoint, log).map(|events| events.as_ref().clone())
}

#[cfg(test)]
pub(crate) fn cached_cluster_kv_watch_registration_for_test(
    paths: &ClusterPaths,
    watch_id: WatchId,
) -> Option<ClusterKvWatchRegistration> {
    let path = cluster_kv_watch_path(paths, watch_id);
    let mark = cluster_file_mark(&path)?;
    cached_cluster_kv_watch_registration_entry(&path, mark).map(|entry| entry.registration)
}

#[cfg(test)]
fn record_cluster_kv_watch_drain_once_for_test(paths: &ClusterPaths) {
    if let Ok(mut calls) = CLUSTER_KV_WATCH_DRAIN_ONCE_CALLS_FOR_TEST.lock() {
        *calls.entry(cluster_kv_watch_notify_key(paths)).or_default() += 1;
    }
}

#[cfg(test)]
pub(crate) fn reset_cluster_kv_watch_drain_once_calls_for_test(paths: &ClusterPaths) {
    if let Ok(mut calls) = CLUSTER_KV_WATCH_DRAIN_ONCE_CALLS_FOR_TEST.lock() {
        calls.remove(&cluster_kv_watch_notify_key(paths));
    }
}

#[cfg(test)]
pub(crate) fn cluster_kv_watch_drain_once_calls_for_test(paths: &ClusterPaths) -> u64 {
    CLUSTER_KV_WATCH_DRAIN_ONCE_CALLS_FOR_TEST
        .lock()
        .ok()
        .and_then(|calls| calls.get(&cluster_kv_watch_notify_key(paths)).copied())
        .unwrap_or(0)
}

#[cfg(test)]
fn record_cluster_kv_watch_event_log_read_for_test(paths: &ClusterPaths, bytes: u64) {
    if let Ok(mut reads) = CLUSTER_KV_WATCH_EVENT_LOG_READ_BYTES_FOR_TEST.lock() {
        *reads.entry(cluster_kv_watch_notify_key(paths)).or_default() += bytes;
    }
}

#[cfg(test)]
fn record_cluster_kv_watch_event_log_write_for_test(paths: &ClusterPaths, bytes: u64) {
    if let Ok(mut writes) = CLUSTER_KV_WATCH_EVENT_LOG_WRITE_BYTES_FOR_TEST.lock() {
        *writes
            .entry(cluster_kv_watch_notify_key(paths))
            .or_default() += bytes;
    }
}

#[cfg(test)]
pub(crate) fn reset_cluster_kv_watch_event_log_read_bytes_for_test(paths: &ClusterPaths) {
    if let Ok(mut reads) = CLUSTER_KV_WATCH_EVENT_LOG_READ_BYTES_FOR_TEST.lock() {
        reads.remove(&cluster_kv_watch_notify_key(paths));
    }
}

#[cfg(test)]
pub(crate) fn cluster_kv_watch_event_log_read_bytes_for_test(paths: &ClusterPaths) -> u64 {
    CLUSTER_KV_WATCH_EVENT_LOG_READ_BYTES_FOR_TEST
        .lock()
        .ok()
        .and_then(|reads| reads.get(&cluster_kv_watch_notify_key(paths)).copied())
        .unwrap_or(0)
}

#[cfg(test)]
pub(crate) fn reset_cluster_kv_watch_event_log_write_bytes_for_test(paths: &ClusterPaths) {
    if let Ok(mut writes) = CLUSTER_KV_WATCH_EVENT_LOG_WRITE_BYTES_FOR_TEST.lock() {
        writes.remove(&cluster_kv_watch_notify_key(paths));
    }
}

#[cfg(test)]
pub(crate) fn cluster_kv_watch_event_log_write_bytes_for_test(paths: &ClusterPaths) -> u64 {
    CLUSTER_KV_WATCH_EVENT_LOG_WRITE_BYTES_FOR_TEST
        .lock()
        .ok()
        .and_then(|writes| writes.get(&cluster_kv_watch_notify_key(paths)).copied())
        .unwrap_or(0)
}

#[cfg(test)]
fn record_cluster_kv_current_snapshot_read_for_test(paths: &ClusterPaths) {
    if let Ok(mut reads) = CLUSTER_KV_CURRENT_SNAPSHOT_READS_FOR_TEST.lock() {
        *reads.entry(cluster_kv_watch_notify_key(paths)).or_default() += 1;
    }
}

#[cfg(test)]
pub(crate) fn reset_cluster_kv_current_snapshot_reads_for_test(paths: &ClusterPaths) {
    if let Ok(mut reads) = CLUSTER_KV_CURRENT_SNAPSHOT_READS_FOR_TEST.lock() {
        reads.remove(&cluster_kv_watch_notify_key(paths));
    }
}

#[cfg(test)]
pub(crate) fn cluster_kv_current_snapshot_reads_for_test(paths: &ClusterPaths) -> u64 {
    CLUSTER_KV_CURRENT_SNAPSHOT_READS_FOR_TEST
        .lock()
        .ok()
        .and_then(|reads| reads.get(&cluster_kv_watch_notify_key(paths)).copied())
        .unwrap_or(0)
}

#[cfg(test)]
pub(crate) fn reset_cluster_kv_watch_dir_scans_for_test(paths: &ClusterPaths) {
    if let Ok(mut scans) = CLUSTER_KV_WATCH_DIR_SCANS_FOR_TEST.lock() {
        scans.remove(&cluster_kv_watch_notify_key(paths));
    }
}

#[cfg(test)]
pub(crate) fn cluster_kv_watch_dir_scans_for_test(paths: &ClusterPaths) -> u64 {
    CLUSTER_KV_WATCH_DIR_SCANS_FOR_TEST
        .lock()
        .ok()
        .and_then(|scans| scans.get(&cluster_kv_watch_notify_key(paths)).copied())
        .unwrap_or(0)
}

#[cfg(test)]
fn record_cluster_kv_watch_dir_scan_for_test(paths: &ClusterPaths) {
    if let Ok(mut scans) = CLUSTER_KV_WATCH_DIR_SCANS_FOR_TEST.lock() {
        *scans.entry(cluster_kv_watch_notify_key(paths)).or_default() += 1;
    }
}

#[cfg(test)]
pub(crate) fn fail_next_cluster_kv_watch_allocator_persist_for_test(paths: &ClusterPaths) {
    if let Ok(mut failures) = CLUSTER_KV_WATCH_ALLOCATOR_PERSIST_FAILURES_FOR_TEST.lock() {
        *failures
            .entry(paths.kv_watches_next_id.clone())
            .or_default() += 1;
    }
}

#[cfg(test)]
fn consume_cluster_kv_watch_allocator_persist_failure_for_test(paths: &ClusterPaths) -> bool {
    let Ok(mut failures) = CLUSTER_KV_WATCH_ALLOCATOR_PERSIST_FAILURES_FOR_TEST.lock() else {
        return false;
    };
    let Some(remaining) = failures.get_mut(&paths.kv_watches_next_id) else {
        return false;
    };
    *remaining = remaining.saturating_sub(1);
    if *remaining == 0 {
        failures.remove(&paths.kv_watches_next_id);
    }
    true
}

#[cfg(test)]
fn record_cluster_kv_cached_snapshot_read_for_test(paths: &ClusterPaths) {
    if let Ok(mut reads) = CLUSTER_KV_CACHED_SNAPSHOT_READS_FOR_TEST.lock() {
        *reads.entry(paths.kv_snapshot.clone()).or_default() += 1;
    }
}

#[cfg(test)]
pub(crate) fn reset_cluster_kv_cached_snapshot_reads_for_test(paths: &ClusterPaths) {
    if let Ok(mut reads) = CLUSTER_KV_CACHED_SNAPSHOT_READS_FOR_TEST.lock() {
        reads.remove(&paths.kv_snapshot);
    }
}

#[cfg(test)]
pub(crate) fn cluster_kv_cached_snapshot_reads_for_test(paths: &ClusterPaths) -> u64 {
    CLUSTER_KV_CACHED_SNAPSHOT_READS_FOR_TEST
        .lock()
        .ok()
        .and_then(|reads| reads.get(&paths.kv_snapshot).copied())
        .unwrap_or(0)
}

#[cfg(test)]
pub(crate) fn cached_cluster_kv_reader_node_id_for_test(paths: &ClusterPaths) -> Option<NodeId> {
    let mark = cluster_file_mark(&paths.state)?;
    cached_cluster_kv_reader_node_id(&paths.state, mark)
}

#[cfg(test)]
pub(crate) fn cluster_kv_watch_event_cache_capacity_for_test() -> usize {
    MAX_CLUSTER_KV_WATCH_EVENT_CACHE_ENTRIES
}

#[cfg(test)]
pub(crate) fn cluster_kv_watch_registration_cache_capacity_for_test() -> usize {
    MAX_CLUSTER_KV_WATCH_REGISTRATION_CACHE_ENTRIES
}

#[cfg(test)]
pub(crate) fn record_cluster_kv_watch_event_cache_for_test(paths: &ClusterPaths) {
    record_cluster_kv_watch_event_cache(
        paths,
        ClusterFileMark {
            exists: false,
            len: 0,
            modified: None,
            ino: 0,
        },
        ClusterFileMark {
            exists: false,
            len: 0,
            modified: None,
            ino: 0,
        },
        Vec::new(),
        0,
        true,
    );
}

#[cfg(test)]
pub(crate) fn refresh_cluster_kv_watch_event_cache_for_test(paths: &ClusterPaths) -> bool {
    cached_cluster_kv_watch_event_cache_entry(
        paths,
        ClusterFileMark {
            exists: false,
            len: 0,
            modified: None,
            ino: 0,
        },
        ClusterFileMark {
            exists: false,
            len: 0,
            modified: None,
            ino: 0,
        },
    )
    .is_some()
}

#[cfg(test)]
pub(crate) fn cluster_kv_watch_event_cache_contains_for_test(paths: &ClusterPaths) -> bool {
    let key = cluster_kv_watch_notify_key(paths);
    CLUSTER_KV_WATCH_EVENT_CACHE
        .lock()
        .map(|cache| cache.contains_key(&key))
        .unwrap_or(false)
}

#[cfg(test)]
pub(crate) fn remove_cluster_kv_watch_event_cache_entries_under_root_for_test(
    root: &std::path::Path,
) {
    if let Ok(mut cache) = CLUSTER_KV_WATCH_EVENT_CACHE.lock() {
        cache.retain(|path, _| !path.starts_with(root));
    }
}

#[cfg(test)]
pub(crate) fn record_cluster_kv_watch_registration_cache_for_test(path: &PathBuf) {
    record_cluster_kv_watch_registration_cache(
        path,
        ClusterFileMark {
            exists: true,
            len: 1,
            modified: Some(SystemTime::UNIX_EPOCH),
            ino: 1,
        },
        ClusterKvWatchRegistration {
            watch_id: WatchId(1),
            prefix: "/cache/test/".to_string(),
            next_revision: KvRevision(1),
        },
    );
}

#[cfg(test)]
pub(crate) fn refresh_cluster_kv_watch_registration_cache_for_test(path: &PathBuf) -> bool {
    cached_cluster_kv_watch_registration_entry(
        path,
        ClusterFileMark {
            exists: true,
            len: 1,
            modified: Some(SystemTime::UNIX_EPOCH),
            ino: 1,
        },
    )
    .is_some()
}

#[cfg(test)]
pub(crate) fn cluster_kv_watch_registration_cache_contains_for_test(path: &PathBuf) -> bool {
    CLUSTER_KV_WATCH_REGISTRATION_CACHE
        .lock()
        .map(|cache| cache.contains_key(path))
        .unwrap_or(false)
}

#[cfg(test)]
pub(crate) fn remove_cluster_kv_watch_registration_cache_entries_under_root_for_test(
    root: &std::path::Path,
) {
    if let Ok(mut cache) = CLUSTER_KV_WATCH_REGISTRATION_CACHE.lock() {
        cache.retain(|path, _| !path.starts_with(root));
    }
}

#[cfg(test)]
pub(crate) fn cluster_kv_metadata_cache_capacity_for_test() -> usize {
    FILE_CACHE_CAPACITY
}

#[cfg(test)]
pub(crate) fn record_cluster_kv_watch_allocator_trust_for_test(paths: &ClusterPaths) {
    if let Ok(mut cache) = CLUSTER_KV_WATCH_ALLOCATOR_TRUST.lock() {
        evict_cluster_kv_watch_allocator_trust_entry_if_needed(
            &mut cache,
            &paths.kv_watches_next_id,
        );
        cache.insert(
            paths.kv_watches_next_id.clone(),
            ClusterKvWatchAllocatorTrustEntry {
                allocator_mark: ClusterFileMark {
                    exists: true,
                    len: 1,
                    modified: Some(SystemTime::UNIX_EPOCH),
                    ino: 1,
                },
                watch_dir_mark: ClusterFileMark {
                    exists: true,
                    len: 1,
                    modified: Some(SystemTime::UNIX_EPOCH),
                    ino: 1,
                },
                active_watch_count: Some(0),
                active_registrations: Some(Vec::new()),
                last_used: cluster_kv_watch_cache_tick(),
            },
        );
    }
}

#[cfg(test)]
pub(crate) fn refresh_cluster_kv_watch_allocator_trust_for_test(paths: &ClusterPaths) -> bool {
    CLUSTER_KV_WATCH_ALLOCATOR_TRUST
        .lock()
        .ok()
        .and_then(|mut cache| {
            cache.get_mut(&paths.kv_watches_next_id).map(|entry| {
                entry.last_used = cluster_kv_watch_cache_tick();
            })
        })
        .is_some()
}

#[cfg(test)]
pub(crate) fn cluster_kv_watch_allocator_trust_contains_for_test(paths: &ClusterPaths) -> bool {
    CLUSTER_KV_WATCH_ALLOCATOR_TRUST
        .lock()
        .map(|cache| cache.contains_key(&paths.kv_watches_next_id))
        .unwrap_or(false)
}

#[cfg(test)]
pub(crate) fn remove_cluster_kv_watch_allocator_trust_entries_under_root_for_test(
    root: &std::path::Path,
) {
    if let Ok(mut cache) = CLUSTER_KV_WATCH_ALLOCATOR_TRUST.lock() {
        cache.retain(|path, _| !path.starts_with(root));
    }
}

#[cfg(test)]
pub(crate) fn record_cluster_kv_reader_node_id_cache_for_test(path: &PathBuf, node_id: &NodeId) {
    record_cluster_kv_reader_node_id_cache(
        path,
        ClusterFileMark {
            exists: true,
            len: 1,
            modified: Some(SystemTime::UNIX_EPOCH),
            ino: 1,
        },
        node_id.clone(),
    );
}

#[cfg(test)]
pub(crate) fn refresh_cluster_kv_reader_node_id_cache_for_test(path: &PathBuf) -> bool {
    cached_cluster_kv_reader_node_id(
        path,
        ClusterFileMark {
            exists: true,
            len: 1,
            modified: Some(SystemTime::UNIX_EPOCH),
            ino: 1,
        },
    )
    .is_some()
}

#[cfg(test)]
pub(crate) fn cluster_kv_reader_node_id_cache_contains_for_test(path: &PathBuf) -> bool {
    CLUSTER_KV_READER_NODE_ID_CACHE
        .lock()
        .map(|cache| cache.contains_key(path))
        .unwrap_or(false)
}

#[cfg(test)]
pub(crate) fn remove_cluster_kv_reader_node_id_cache_entries_under_root_for_test(
    root: &std::path::Path,
) {
    if let Ok(mut cache) = CLUSTER_KV_READER_NODE_ID_CACHE.lock() {
        cache.retain(|path, _| !path.starts_with(root));
    }
}

#[cfg(test)]
fn cluster_kv_watch_notify_key(paths: &ClusterPaths) -> PathBuf {
    paths.root.clone()
}

#[cfg(test)]
fn cluster_kv_watch_generation(paths: &ClusterPaths) -> u64 {
    sinomo_kv::cluster_kv_watch_generation(&paths.kv_paths())
}

#[cfg(test)]
fn cluster_kv_watch_observed_generation(
    paths: &ClusterPaths,
    watch_id: WatchId,
) -> sinomo_kv::ClusterKvWatchObservedGeneration {
    sinomo_kv::cluster_kv_watch_observed_generation(&paths.kv_paths(), watch_id)
}

#[cfg(test)]
fn notify_cluster_kv_watchers_for_events(paths: &ClusterPaths, events: &[KvEvent]) {
    sinomo_kv::notify_cluster_kv_watchers_for_events(&paths.kv_paths(), events);
}

#[cfg(test)]
fn wait_for_cluster_kv_watch_change(
    paths: &ClusterPaths,
    observed_generation: u64,
    timeout: Duration,
) -> bool {
    sinomo_kv::wait_for_cluster_kv_watch_change(&paths.kv_paths(), observed_generation, timeout)
}

#[cfg(test)]
pub(crate) fn cluster_kv_watch_notifier_capacity_for_test() -> usize {
    sinomo_kv::cluster_kv_watch_notifier_capacity_for_test()
}

#[cfg(test)]
pub(crate) fn cluster_kv_watch_notifier_generation_for_test(paths: &ClusterPaths) -> u64 {
    cluster_kv_watch_generation(paths)
}

#[cfg(test)]
pub(crate) fn cluster_kv_watch_notifier_has_key_for_test(paths: &ClusterPaths) -> bool {
    sinomo_kv::cluster_kv_watch_notifier_has_key_for_test(&paths.kv_paths())
}

#[cfg(test)]
pub(crate) fn cluster_kv_watch_notifier_has_raw_key_for_test(path: &std::path::Path) -> bool {
    sinomo_kv::cluster_kv_watch_notifier_has_raw_key_for_test(path)
}

#[cfg(test)]
pub(crate) fn cluster_kv_watch_notifier_waiter_count_for_test(paths: &ClusterPaths) -> usize {
    sinomo_kv::cluster_kv_watch_notifier_waiter_count_for_test(&paths.kv_paths())
}

#[cfg(test)]
pub(crate) fn remove_cluster_kv_watch_notifier_entries_under_root_for_test(root: &std::path::Path) {
    sinomo_kv::remove_cluster_kv_watch_notifier_entries_under_root_for_test(root);
}

#[cfg(test)]
pub(crate) fn touch_cluster_kv_watch_notifier_for_test(paths: &ClusterPaths) {
    sinomo_kv::touch_cluster_kv_watch_notifier_for_test(&paths.kv_paths());
}

#[cfg(test)]
pub(crate) fn record_cluster_kv_watch_notifier_waiter_key_for_test(path: PathBuf) {
    sinomo_kv::record_cluster_kv_watch_notifier_waiter_key_for_test(path);
}

#[cfg(test)]
pub(crate) fn reset_cluster_kv_watch_unchanged_wakeups_for_test(
    paths: &ClusterPaths,
    watch_id: WatchId,
) {
    sinomo_kv::reset_cluster_kv_watch_unchanged_wakeups_for_test(&paths.kv_paths(), watch_id);
}

#[cfg(test)]
pub(crate) fn cluster_kv_watch_unchanged_wakeups_for_test(
    paths: &ClusterPaths,
    watch_id: WatchId,
) -> u64 {
    sinomo_kv::cluster_kv_watch_unchanged_wakeups_for_test(&paths.kv_paths(), watch_id)
}

#[cfg(test)]
pub(crate) fn wait_for_cluster_kv_watch_change_for_test(
    paths: &ClusterPaths,
    observed_generation: u64,
    timeout: Duration,
) -> bool {
    wait_for_cluster_kv_watch_change(paths, observed_generation, timeout)
}

#[cfg(test)]
pub fn remove_local_cluster_state(paths: &ClusterPaths) -> Result<(), NetworkError> {
    if !remove_optional_file(&paths.state)? {
        return Err(NetworkError::ClusterOrphan);
    }
    invalidate_cluster_kv_reader_node_id_cache(&paths.state);
    remove_optional_file(&paths.kv_snapshot)?;
    Ok(())
}

pub(crate) fn remove_optional_file(path: &PathBuf) -> Result<bool, NetworkError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(NetworkError::StorageIo {
            path: path.display().to_string(),
            detail: source.to_string(),
        }),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterJoinReconciliation {
    pub accepted_endpoint: Option<ClusterEndpointRecord>,
    pub kv_snapshot: Option<KvSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterNodeRoleUpdate {
    pub node_id: NodeId,
    pub previous_role: ClusterNodeRole,
    pub role: ClusterNodeRole,
    pub changed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterRaftRoleUpdate {
    pub node_id: NodeId,
    pub previous_role: ClusterMemberRole,
    pub previous_state: ClusterMemberState,
    pub role: ClusterMemberRole,
    pub state: ClusterMemberState,
    pub changed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterMemberRemoval {
    pub removed: Vec<ClusterMemberRecord>,
}

pub trait ClusterMemberMutationHost {
    fn prepare_cluster_state_for_kv_sync(
        &mut self,
        _paths: &ClusterPaths,
        _state: &mut ClusterBootstrapState,
    ) -> Result<(), NetworkError> {
        Ok(())
    }

    fn current_cluster_kv_store(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<InProcessKvStore, NetworkError>;

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, NetworkError>;
}

pub trait ClusterMemberMutationAsyncHost {
    fn prepare_cluster_state_for_kv_sync_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _state: &'a mut ClusterBootstrapState,
    ) -> ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn commit_cluster_kv_txn_with_quorum_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> ClusterMembershipRpcFuture<'a, KvSnapshot>;
}

pub fn prepare_cluster_join_registration_response(
    paths: &ClusterPaths,
    state: ClusterBootstrapState,
    request: ClusterJoinRegistrationRequest,
    now: OffsetDateTime,
) -> Result<ClusterJoinRegistrationResponse, NetworkError> {
    let persisted_invite = read_cluster_invitation_from_kv(paths, &request.invite.invite_id)?;
    if request.invite.cluster_public_key != persisted_invite.cluster_public_key
        || request.invitation_secret.trim().is_empty()
        || request.invitation_secret != persisted_invite.invitation_secret
    {
        return Ok(ClusterJoinRegistrationResponse {
            accepted: false,
            detail: "invitation rejected".to_string(),
            state,
            kv_snapshot: None,
        });
    }
    if let Err(error) = crate::verify_cluster_join_invite_signature(&persisted_invite) {
        return Ok(ClusterJoinRegistrationResponse {
            accepted: false,
            detail: format!("invite signature rejected: {error}"),
            state,
            kv_snapshot: None,
        });
    }
    if !state.members.iter().any(|member| {
        member.node_id == persisted_invite.issuer_node_id
            && member.node_public_key == persisted_invite.issuer_public_key
            && member.state.is_authorized()
    }) {
        return Ok(ClusterJoinRegistrationResponse {
            accepted: false,
            detail: format!(
                "invite issuer {} is not an authorized cluster member",
                persisted_invite.issuer_node_id
            ),
            state,
            kv_snapshot: None,
        });
    }
    if persisted_invite.cluster_id != state.cluster_id {
        return Ok(ClusterJoinRegistrationResponse {
            accepted: false,
            detail: format!(
                "invite cluster `{}` does not match introducer cluster `{}`",
                persisted_invite.cluster_id, state.cluster_id
            ),
            state,
            kv_snapshot: None,
        });
    }
    if persisted_invite.expires_at <= now {
        return Ok(ClusterJoinRegistrationResponse {
            accepted: false,
            detail: format!(
                "invite expired at {}",
                persisted_invite
                    .expires_at
                    .format(&time::format_description::well_known::Rfc3339)
                    .unwrap_or_else(|_| "unknown".to_string())
            ),
            state,
            kv_snapshot: None,
        });
    }
    if request
        .members
        .iter()
        .any(|member| !persisted_invite.allowed_member_roles.contains(&member.role))
    {
        return Ok(ClusterJoinRegistrationResponse {
            accepted: false,
            detail: "join request includes a member role not allowed by the invite".to_string(),
            state,
            kv_snapshot: None,
        });
    }

    let mut accepted_state = state.clone();
    for mut member in request.members {
        member.cluster_id = accepted_state.cluster_id.clone();
        if matches!(member.state, ClusterMemberState::Pending) {
            member.state = match member.role {
                ClusterMemberRole::Learner => ClusterMemberState::Learner,
                ClusterMemberRole::NonVoting => ClusterMemberState::NonVoting,
                ClusterMemberRole::Voter => ClusterMemberState::Voter,
            };
        }
        member.updated_at = now;
        upsert_member_record(&mut accepted_state, member);
    }
    accepted_state.kv_revision = accepted_state.kv_revision.saturating_add(1);
    accepted_state.updated_at = now;
    Ok(ClusterJoinRegistrationResponse {
        accepted: true,
        detail: "join registered".to_string(),
        state: accepted_state,
        kv_snapshot: None,
    })
}

pub fn handle_cluster_join_registration_request_with_host(
    paths: &ClusterPaths,
    request: ClusterJoinRegistrationRequest,
    now: OffsetDateTime,
    host: &mut impl ClusterJoinRegistrationHost,
) -> Result<ClusterJoinRegistrationResponse, NetworkError> {
    let mut state = read_cluster_state(paths)?;
    if let Some(observation) = host.local_daemon_membership_observation(paths, &state)? {
        apply_local_daemon_membership_observation(&mut state, observation);
    }
    let mut prepared =
        prepare_cluster_join_registration_response(paths, state.clone(), request.clone(), now)?;
    if !prepared.accepted {
        return Ok(prepared);
    }

    if !sinomo_kv::cluster_kv_raft_local_is_voter(&state) {
        let rpc_request = ObjectRpcRequest::encode_body(
            CLUSTER_JOIN_REGISTER_ROUTE,
            CLUSTER_MEMBERSHIP_RPC_CATEGORY,
            &request,
        )?;
        let response = forward_cluster_membership_rpc_to_voter_with_host(
            paths,
            &state,
            &rpc_request,
            "join introducer is not a voter and could not forward the join to any voter",
            host,
        )?;
        let response: ClusterJoinRegistrationResponse = response.decode_body()?;
        if response.accepted {
            record_forwarded_join_registration_acceptance_with_host(
                paths,
                &state,
                &request.members,
                &response,
                host,
            )?;
        }
        return Ok(response);
    }

    let snapshot = host.commit_join_registration_state(paths, &prepared.state)?;
    write_cluster_state(paths, &prepared.state)?;
    prepared.kv_snapshot = Some(snapshot);
    Ok(prepared)
}

pub async fn handle_cluster_join_registration_request_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    request: ClusterJoinRegistrationRequest,
    now: OffsetDateTime,
    host: &mut (impl ClusterJoinRegistrationAsyncHost + Send),
) -> Result<ClusterJoinRegistrationResponse, NetworkError> {
    let state_paths = paths.clone();
    let mut state = runtime
        .spawn_blocking(move || read_cluster_state(&state_paths))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "read cluster state".to_string(),
            detail: format!("cluster state read task failed: {source}"),
        })??;
    if let Some(observation) = host
        .local_daemon_membership_observation_async(paths, &state)
        .await?
    {
        apply_local_daemon_membership_observation(&mut state, observation);
    }
    let prepare_paths = paths.clone();
    let prepare_state = state.clone();
    let prepare_request = request.clone();
    let mut prepared = runtime
        .spawn_blocking(move || {
            prepare_cluster_join_registration_response(
                &prepare_paths,
                prepare_state,
                prepare_request,
                now,
            )
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster join registration prepare".to_string(),
            detail: format!("join registration prepare task failed: {source}"),
        })??;
    if !prepared.accepted {
        return Ok(prepared);
    }

    if !sinomo_kv::cluster_kv_raft_local_is_voter(&state) {
        let rpc_request = ObjectRpcRequest::encode_body(
            CLUSTER_JOIN_REGISTER_ROUTE,
            CLUSTER_MEMBERSHIP_RPC_CATEGORY,
            &request,
        )?;
        let response = forward_cluster_membership_rpc_to_voter_async_with_host(
            runtime,
            paths,
            &state,
            &rpc_request,
            "join introducer is not a voter and could not forward the join to any voter",
            host,
        )
        .await?;
        let response: ClusterJoinRegistrationResponse = response.decode_body()?;
        if response.accepted {
            record_forwarded_join_registration_acceptance_async_with_host(
                runtime,
                paths,
                &state,
                &request.members,
                &response,
                host,
            )
            .await?;
        }
        return Ok(response);
    }

    let snapshot = host
        .commit_join_registration_state_async(paths, &prepared.state)
        .await?;
    let write_paths = paths.clone();
    let write_state = prepared.state.clone();
    runtime
        .spawn_blocking(move || write_cluster_state(&write_paths, &write_state))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "write cluster state".to_string(),
            detail: format!("cluster state write task failed: {source}"),
        })??;
    prepared.kv_snapshot = Some(snapshot);
    Ok(prepared)
}

pub fn send_cluster_join_registration_over_membership_rpc<S>(
    endpoint: &ClusterEndpointRecord,
    request: &ClusterJoinRegistrationRequest,
    timeout: Duration,
    mut send_rpc: S,
) -> Result<ClusterJoinRegistrationResponse, NetworkError>
where
    S: FnMut(
        &ClusterEndpointRecord,
        &ObjectRpcRequest,
        Duration,
    ) -> Result<ObjectRpcResponse, NetworkError>,
{
    let challenge = crate::invite::random_url_secret(32)?;
    let challenge_request = crate::invite::ClusterInviteChallengeRequest {
        protocol: crate::invite::CLUSTER_INVITE_CHALLENGE_PROTOCOL.to_string(),
        invite_id: request.invite.invite_id.clone(),
        challenge: challenge.clone(),
    };
    let challenge_rpc = ObjectRpcRequest::encode_body(
        CLUSTER_JOIN_CHALLENGE_ROUTE,
        CLUSTER_MEMBERSHIP_RPC_CATEGORY,
        &challenge_request,
    )?;
    let proof_response = send_rpc(endpoint, &challenge_rpc, timeout)?;
    let proof: crate::invite::ClusterInviteChallengeProof = proof_response.decode_body()?;
    crate::invite::verify_cluster_invite_challenge_proof(&request.invite, &challenge, &proof)?;

    let register_rpc = ObjectRpcRequest::encode_body(
        CLUSTER_JOIN_REGISTER_ROUTE,
        CLUSTER_MEMBERSHIP_RPC_CATEGORY,
        request,
    )?;
    let response = send_rpc(endpoint, &register_rpc, timeout)?;
    response.decode_body()
}

pub fn record_forwarded_join_registration_acceptance_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    request_members: &[ClusterMemberRecord],
    response: &ClusterJoinRegistrationResponse,
    host: &mut impl ClusterJoinRegistrationHost,
) -> Result<(), NetworkError> {
    if !response.accepted {
        return Ok(());
    }
    if let Some(snapshot) = response.kv_snapshot.as_ref() {
        write_imported_cluster_kv_snapshot_if_changed(paths, snapshot)?;
    }
    let mut local_state = read_cluster_state_without_kv(paths)?;
    merge_cluster_join_registration(&mut local_state, response.state.clone());
    write_cluster_state(paths, &local_state)?;
    host.remember_successful_join_targets(paths, state, request_members)
}

pub async fn record_forwarded_join_registration_acceptance_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    request_members: &[ClusterMemberRecord],
    response: &ClusterJoinRegistrationResponse,
    host: &mut (impl ClusterJoinRegistrationAsyncHost + Send),
) -> Result<(), NetworkError> {
    if !response.accepted {
        return Ok(());
    }
    let paths_for_state = paths.clone();
    let response_state = response.state.clone();
    let snapshot = response.kv_snapshot.clone();
    runtime
        .spawn_blocking(move || {
            if let Some(snapshot) = snapshot.as_ref() {
                write_imported_cluster_kv_snapshot_if_changed(&paths_for_state, snapshot)?;
            }
            let mut local_state = read_cluster_state_without_kv(&paths_for_state)?;
            merge_cluster_join_registration(&mut local_state, response_state);
            write_cluster_state(&paths_for_state, &local_state)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster forwarded join acceptance".to_string(),
            detail: format!("forwarded join acceptance task failed: {source}"),
        })??;
    host.remember_successful_join_targets_async(paths, state, request_members)
        .await
}

pub fn reconcile_cluster_join_with_introducers_with_host(
    state: &mut ClusterBootstrapState,
    invite: &ClusterJoinInvite,
    tcp_timeout: Duration,
    iroh_timeout: Duration,
    connectivity_paths: Option<&ClusterPaths>,
    host: &mut impl ClusterJoinReconciliationHost,
) -> Result<ClusterJoinReconciliation, NetworkError> {
    if invite.communication_channels.is_empty() && invite.introducers.is_empty() {
        return Ok(ClusterJoinReconciliation {
            accepted_endpoint: None,
            kv_snapshot: None,
        });
    }
    let request = ClusterJoinRegistrationRequest {
        invitation_secret: invite.invitation_secret.clone(),
        invite: invite.clone(),
        members: state.members.clone(),
    };
    let deadline = Instant::now() + tcp_timeout.max(iroh_timeout);
    let remaining_for = |channel_timeout: Duration| {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_else(|| Duration::from_millis(1));
        remaining.min(channel_timeout)
    };
    let mut failures = Vec::new();
    for channel in crate::invite_channels_for_join_registration(&request.invite) {
        let tcp_remaining = remaining_for(tcp_timeout);
        let iroh_remaining = remaining_for(iroh_timeout);
        match host.send_join_registration(
            &channel.endpoint,
            &request,
            tcp_remaining,
            iroh_remaining,
        ) {
            Ok(response) => {
                if response.accepted {
                    if let Some(reconciliation) = reconcile_accepted_join_response_with_host(
                        state,
                        &request.invite,
                        &channel.endpoint,
                        response,
                        connectivity_paths,
                        host,
                    )? {
                        return Ok(reconciliation);
                    }
                    failures.push(format!(
                        "{} ({:?}) returned unverified join acceptance",
                        channel.endpoint.address, channel.endpoint.transport
                    ));
                    continue;
                }
                if let Some(reconciliation) =
                    reconcile_already_accepted_join_from_snapshot_with_host(
                        state,
                        &request.invite,
                        &channel.endpoint,
                        match channel.endpoint.transport {
                            ClusterTransportKind::Iroh => iroh_remaining,
                            _ => tcp_remaining,
                        },
                        connectivity_paths,
                        host,
                    )?
                {
                    return Ok(reconciliation);
                }
                failures.push(format!(
                    "{} ({:?}) rejected join: {}",
                    channel.endpoint.address, channel.endpoint.transport, response.detail
                ));
            }
            Err(error) => {
                if let Some(reconciliation) =
                    reconcile_already_accepted_join_from_snapshot_with_host(
                        state,
                        &request.invite,
                        &channel.endpoint,
                        match channel.endpoint.transport {
                            ClusterTransportKind::Iroh => iroh_remaining,
                            _ => tcp_remaining,
                        },
                        connectivity_paths,
                        host,
                    )?
                {
                    return Ok(reconciliation);
                }
                failures.push(format!(
                    "{} ({:?}) failed: {}",
                    channel.endpoint.address, channel.endpoint.transport, error
                ));
            }
        }
    }
    if !failures.is_empty() {
        return Err(NetworkError::Io {
            operation: "cluster introducers".to_string(),
            detail: format!(
                "join registration failed for all introducers: {}. If the invite only contains 127.0.0.1 or localhost, create a new invite with `sinomo cluster invite --introducer <host-lan-ip>:21353`.",
                failures.join("; ")
            ),
        });
    }
    Ok(ClusterJoinReconciliation {
        accepted_endpoint: None,
        kv_snapshot: None,
    })
}

fn reconcile_accepted_join_response_with_host(
    state: &mut ClusterBootstrapState,
    invite: &ClusterJoinInvite,
    endpoint: &ClusterEndpointRecord,
    response: ClusterJoinRegistrationResponse,
    connectivity_paths: Option<&ClusterPaths>,
    host: &mut impl ClusterJoinReconciliationHost,
) -> Result<Option<ClusterJoinReconciliation>, NetworkError> {
    let reconciliation =
        reconcile_accepted_cluster_join_response(state, invite, endpoint, response)?;
    observe_join_reconciliation_connectivity(
        connectivity_paths,
        state,
        reconciliation.as_ref(),
        host,
    )?;
    Ok(reconciliation)
}

fn reconcile_already_accepted_join_from_snapshot_with_host(
    state: &mut ClusterBootstrapState,
    invite: &ClusterJoinInvite,
    endpoint: &ClusterEndpointRecord,
    timeout: Duration,
    connectivity_paths: Option<&ClusterPaths>,
    host: &mut impl ClusterJoinReconciliationHost,
) -> Result<Option<ClusterJoinReconciliation>, NetworkError> {
    let snapshot = match host.fetch_cluster_kv_snapshot(endpoint, timeout) {
        Ok(snapshot) => snapshot,
        Err(_) => return Ok(None),
    };
    let reconciliation =
        reconcile_accepted_cluster_join_snapshot(state, invite, endpoint, snapshot)?;
    observe_join_reconciliation_connectivity(
        connectivity_paths,
        state,
        reconciliation.as_ref(),
        host,
    )?;
    Ok(reconciliation)
}

fn observe_join_reconciliation_connectivity(
    connectivity_paths: Option<&ClusterPaths>,
    state: &ClusterBootstrapState,
    reconciliation: Option<&ClusterJoinReconciliation>,
    host: &mut impl ClusterJoinReconciliationHost,
) -> Result<(), NetworkError> {
    let (Some(paths), Some(reconciliation)) = (connectivity_paths, reconciliation) else {
        return Ok(());
    };
    host.observe_join_reconciliation_connectivity(paths, state, reconciliation)
}

pub fn accepted_cluster_join_state_from_kv_snapshot(
    pending: &ClusterBootstrapState,
    invite: &ClusterJoinInvite,
    snapshot: &KvSnapshot,
) -> Result<Option<ClusterBootstrapState>, NetworkError> {
    let Some(identity_entry) = snapshot.get(&cluster_identity_key()) else {
        return Ok(None);
    };
    let identity: ClusterIdentityRecord = identity_entry.decode()?;
    if crate::invite_has_embedded_cluster_identity(invite)
        && identity.cluster_id != invite.cluster_id
    {
        return Ok(None);
    }
    let mut members = Vec::new();
    for entry in
        snapshot.range_prefix_limited("/cluster/members/", CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES)?
    {
        let mut member: ClusterMemberRecord = entry.decode()?;
        if member.cluster_id != identity.cluster_id {
            return Ok(None);
        }
        if let Some(policy_entry) = snapshot.get(&cluster_capability_policy_key(&member.node_id)) {
            let policy: ClusterCapabilityPolicyRecord = policy_entry.decode()?;
            member.capabilities = policy.capabilities;
            member.allowed_capabilities = policy.allowed_capabilities;
        }
        for endpoint_entry in snapshot.range_prefix_limited(
            &cluster_endpoint_prefix(&member.node_id),
            CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
        )? {
            let endpoint: ClusterEndpointRecord = endpoint_entry.decode()?;
            if endpoint.node_id == member.node_id {
                upsert_member_endpoint(&mut member, endpoint);
            }
        }
        cleanup_member_iroh_endpoints(&mut member);
        members.push(member);
    }
    let authority_public_key = if invite.issuer_public_key.trim().is_empty() {
        invite.cluster_public_key.as_str()
    } else {
        invite.issuer_public_key.as_str()
    };
    let authority_member_is_authorized = members.iter().any(|member| {
        member.node_public_key == authority_public_key
            && member.state.is_authorized()
            && (invite.issuer_public_key.trim().is_empty()
                || member.node_id == invite.issuer_node_id)
    });
    if members.is_empty()
        || authority_public_key.trim().is_empty()
        || !authority_member_is_authorized
    {
        return Ok(None);
    }
    let local_processes = pending
        .members
        .iter()
        .filter(|member| cluster_member_is_local_process(pending, member))
        .filter(|member| !member.node_public_key.trim().is_empty())
        .collect::<Vec<_>>();
    if local_processes.is_empty()
        || local_processes.iter().any(|local| {
            !members.iter().any(|member| {
                member.node_id == local.node_id
                    && member.node_public_key == local.node_public_key
                    && member.state.is_authorized()
            })
        })
    {
        return Ok(None);
    }
    members.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    Ok(Some(ClusterBootstrapState {
        cluster_id: identity.cluster_id,
        cluster_name: identity.cluster_name,
        local_node_id: identity.local_node_id,
        local_node_display_name: identity.local_node_display_name,
        term: identity.term,
        kv_revision: identity.kv_revision,
        members,
        initialized_at: identity.initialized_at,
        updated_at: identity.updated_at,
    }))
}

pub fn accepted_cluster_join_response_state_authorizes_local_processes(
    pending: &ClusterBootstrapState,
    accepted: &ClusterBootstrapState,
) -> bool {
    if accepted.cluster_id != pending.cluster_id {
        return false;
    }
    let local_processes = pending
        .members
        .iter()
        .filter(|member| cluster_member_is_local_process(pending, member))
        .filter(|member| !member.node_public_key.trim().is_empty())
        .collect::<Vec<_>>();
    !local_processes.is_empty()
        && local_processes.iter().all(|local| {
            accepted.members.iter().any(|member| {
                member.node_id == local.node_id
                    && member.node_public_key == local.node_public_key
                    && member.state.is_authorized()
            })
        })
}

pub fn reconcile_accepted_cluster_join_response(
    state: &mut ClusterBootstrapState,
    invite: &ClusterJoinInvite,
    endpoint: &ClusterEndpointRecord,
    response: ClusterJoinRegistrationResponse,
) -> Result<Option<ClusterJoinReconciliation>, NetworkError> {
    if let Some(snapshot) = response.kv_snapshot {
        let snapshot = snapshot.canonicalized();
        let Some(accepted_state) =
            accepted_cluster_join_state_from_kv_snapshot(state, invite, &snapshot)?
        else {
            return Ok(None);
        };
        return Ok(Some(apply_accepted_cluster_join_state(
            state,
            invite,
            endpoint,
            accepted_state,
            Some(snapshot),
        )));
    }

    if !crate::invite_has_embedded_cluster_identity(invite)
        || !accepted_cluster_join_response_state_authorizes_local_processes(state, &response.state)
    {
        return Ok(None);
    }

    Ok(Some(apply_accepted_cluster_join_state(
        state,
        invite,
        endpoint,
        response.state,
        None,
    )))
}

pub fn reconcile_accepted_cluster_join_snapshot(
    state: &mut ClusterBootstrapState,
    invite: &ClusterJoinInvite,
    endpoint: &ClusterEndpointRecord,
    snapshot: KvSnapshot,
) -> Result<Option<ClusterJoinReconciliation>, NetworkError> {
    let snapshot = snapshot.canonicalized();
    let Some(accepted_state) =
        accepted_cluster_join_state_from_kv_snapshot(state, invite, &snapshot)?
    else {
        return Ok(None);
    };
    Ok(Some(apply_accepted_cluster_join_state(
        state,
        invite,
        endpoint,
        accepted_state,
        Some(snapshot),
    )))
}

pub fn apply_accepted_cluster_join_state(
    state: &mut ClusterBootstrapState,
    invite: &ClusterJoinInvite,
    endpoint: &ClusterEndpointRecord,
    accepted_state: ClusterBootstrapState,
    kv_snapshot: Option<KvSnapshot>,
) -> ClusterJoinReconciliation {
    merge_cluster_join_registration(state, accepted_state);
    let accepted_endpoint = join_channel_endpoint_for_accepted_state(state, invite, endpoint);
    remember_join_channel_endpoint_in_local_state(state, &accepted_endpoint);
    ClusterJoinReconciliation {
        accepted_endpoint: Some(accepted_endpoint),
        kv_snapshot,
    }
}

pub fn join_channel_endpoint_for_accepted_state(
    state: &ClusterBootstrapState,
    invite: &ClusterJoinInvite,
    endpoint: &ClusterEndpointRecord,
) -> ClusterEndpointRecord {
    let Some(host) = cluster_endpoint_address_host(&endpoint.address) else {
        return endpoint.clone();
    };
    let member = state.members.iter().find(|member| {
        member.state.is_authorized()
            && (member.display_name == host
                || member.endpoints.iter().any(|candidate| {
                    cluster_endpoint_address_host(&candidate.address).as_deref() == Some(host)
                }))
    });
    let node_id = member
        .map(|member| &member.node_id)
        .or_else(|| {
            state
                .members
                .iter()
                .any(|member| {
                    member.node_id == invite.issuer_node_id && member.state.is_authorized()
                })
                .then_some(&invite.issuer_node_id)
        })
        .unwrap_or(&endpoint.node_id);
    retarget_cluster_endpoint_to_node(endpoint, node_id)
}

fn retarget_cluster_endpoint_to_node(
    endpoint: &ClusterEndpointRecord,
    node_id: &NodeId,
) -> ClusterEndpointRecord {
    if *node_id == endpoint.node_id {
        return endpoint.clone();
    }
    match endpoint.transport {
        ClusterTransportKind::LanTcp | ClusterTransportKind::Loopback => {
            crate::daemon_lan_endpoint(node_id, endpoint.address.clone(), endpoint.priority)
        }
        _ => {
            let mut retargeted = endpoint.clone();
            retargeted.node_id = node_id.clone();
            retargeted
        }
    }
}

fn remember_join_channel_endpoint_in_local_state(
    state: &mut ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
) {
    if endpoint.node_id == state.local_node_id
        || endpoint.address.parse::<SocketAddr>().is_ok()
        || !matches!(
            endpoint.transport,
            ClusterTransportKind::LanTcp | ClusterTransportKind::Loopback
        )
    {
        return;
    }
    let Some(member) = state
        .members
        .iter_mut()
        .find(|member| member.node_id == endpoint.node_id && member.state.is_authorized())
    else {
        return;
    };
    upsert_member_endpoint(member, endpoint.clone());
}

fn cluster_endpoint_address_host(address: &str) -> Option<&str> {
    let host = address
        .rsplit_once(':')
        .map(|(host, _)| host)
        .unwrap_or(address)
        .trim()
        .trim_start_matches('[')
        .trim_end_matches(']');
    (!host.is_empty()).then_some(host)
}

pub fn cluster_join_challenge_rpc_request<T: Serialize + ?Sized>(
    challenge: &T,
) -> Result<ObjectRpcRequest, NetworkError> {
    ObjectRpcRequest::encode_body(
        CLUSTER_JOIN_CHALLENGE_ROUTE,
        CLUSTER_MEMBERSHIP_RPC_CATEGORY,
        challenge,
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterRaftAppendReplicationOptions {
    pub max_attempts: usize,
    pub timeout: Duration,
    pub tcp_command_timeout: Duration,
    pub iroh_command_timeout: Duration,
    pub tcp_foreground_attempt_timeout: Duration,
    pub public_foreground_attempt_timeout: Duration,
    pub iroh_foreground_attempt_timeout: Duration,
}

impl ClusterRaftAppendReplicationOptions {
    pub fn kv_options(&self) -> sinomo_kv::ClusterKvRaftAppendReplicationOptions {
        sinomo_kv::ClusterKvRaftAppendReplicationOptions {
            max_attempts: self.max_attempts,
            timeout: self.timeout,
        }
    }

    pub fn command_timeout(
        &self,
        endpoint: &ClusterEndpointRecord,
        remaining: Duration,
    ) -> Duration {
        match endpoint.transport {
            ClusterTransportKind::Iroh => remaining.min(self.iroh_command_timeout),
            _ => remaining.min(self.tcp_command_timeout),
        }
    }

    pub fn fair_command_timeout(
        &self,
        endpoint: &ClusterEndpointRecord,
        remaining: Duration,
    ) -> Duration {
        let attempt_cap = match endpoint.transport {
            ClusterTransportKind::Iroh => self.iroh_foreground_attempt_timeout,
            _ if matches!(endpoint.privacy, ClusterEndpointPrivacy::InternetDirect) => {
                self.public_foreground_attempt_timeout
            }
            _ => self.tcp_foreground_attempt_timeout,
        };
        self.command_timeout(endpoint, remaining.min(attempt_cap))
    }

    pub fn append_attempt_timeout(
        &self,
        endpoint: &ClusterEndpointRecord,
        remaining: Duration,
        request: &ClusterRaftAppendRequest,
    ) -> Duration {
        if request.snapshot.is_some() || request.append.entries.len() > 64 {
            return self.command_timeout(endpoint, remaining);
        }
        self.fair_command_timeout(endpoint, remaining)
    }
}

pub type ClusterRaftAppendReplicationOutcome = sinomo_kv::ClusterKvRaftAppendReplicationOutcome;

pub trait ClusterRaftAppendReplicationHost {
    fn last_local_raft_log_index(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<u64, NetworkError>;

    fn build_raft_append_request(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        next_index: u64,
    ) -> Result<ClusterRaftAppendRequest, NetworkError>;

    fn send_raft_append(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError>;

    fn on_raft_append_sent(&mut self) {}
}

pub trait ClusterRaftAppendReplicationAsyncHost {
    fn last_local_raft_log_index_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
    ) -> ClusterMembershipRpcFuture<'a, u64>;

    fn build_raft_append_request_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        next_index: u64,
    ) -> ClusterMembershipRpcFuture<'a, ClusterRaftAppendRequest>;

    fn send_raft_append_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterRaftAppendResponse>;

    fn on_raft_append_sent_async<'a>(&'a mut self) -> ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

pub trait ClusterRaftQuorumAsyncHost: ClusterRaftAppendReplicationAsyncHost {
    fn record_endpoint_command_connectivity(
        &mut self,
        _paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
        _endpoint: &ClusterEndpointRecord,
        _connectivity_state: ClusterConnectivityState,
        _latency_ms: Option<u64>,
    ) {
    }

    fn fan_out_committed_cluster_raft_command_async<'a>(
        &'a mut self,
        _paths: ClusterPaths,
        _state: ClusterBootstrapState,
        _events: Vec<KvEvent>,
        _targets: Vec<ClusterEndpointRecord>,
    ) -> ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

#[cfg(test)]
struct LocalClusterRaftAppendReplicationHost<S, O> {
    send_raft_append: S,
    on_raft_append_sent: O,
}

#[cfg(test)]
impl<S, O> ClusterRaftAppendReplicationHost for LocalClusterRaftAppendReplicationHost<S, O>
where
    S: FnMut(
        &ClusterEndpointRecord,
        &ClusterRaftAppendRequest,
        Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError>,
    O: FnMut(),
{
    fn last_local_raft_log_index(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<u64, NetworkError> {
        cluster_kv_raft_store(paths)
            .locked_last_log_index(&state.local_node_id)
            .map_err(Into::into)
    }

    fn build_raft_append_request(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        next_index: u64,
    ) -> Result<ClusterRaftAppendRequest, NetworkError> {
        cluster_kv_raft_store(paths)
            .locked_build_cluster_raft_append_request_from_next_index(state, next_index)
            .map_err(Into::into)
    }

    fn send_raft_append(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError> {
        (self.send_raft_append)(endpoint, request, timeout)
    }

    fn on_raft_append_sent(&mut self) {
        (self.on_raft_append_sent)();
    }
}

#[cfg(test)]
pub fn replicate_cluster_raft_to_endpoint_with_local_host<S, O>(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    watch_events: &[KvEvent],
    options: ClusterRaftAppendReplicationOptions,
    send_raft_append: S,
    on_raft_append_sent: O,
) -> Result<ClusterRaftAppendReplicationOutcome, NetworkError>
where
    S: FnMut(
        &ClusterEndpointRecord,
        &ClusterRaftAppendRequest,
        Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError>,
    O: FnMut(),
{
    let mut host = LocalClusterRaftAppendReplicationHost {
        send_raft_append,
        on_raft_append_sent,
    };
    replicate_cluster_raft_to_endpoint_with_host(
        paths,
        state,
        endpoint,
        watch_events,
        options,
        &mut host,
    )
}

pub type ClusterRaftQuorumCommitOptions = sinomo_kv::ClusterKvRaftQuorumCommitOptions;

pub type ClusterRaftQuorumCommitResult = sinomo_kv::ClusterKvRaftQuorumCommitResult;

pub trait ClusterRaftQuorumTransportHost {
    fn replicate_cluster_raft_to_endpoint(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError>;

    fn record_endpoint_command_connectivity(
        &mut self,
        _paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
        _endpoint: &ClusterEndpointRecord,
        _connectivity_state: ClusterConnectivityState,
        _latency_ms: Option<u64>,
    ) {
    }

    fn fan_out_committed_cluster_raft_command(
        &mut self,
        _paths: ClusterPaths,
        _state: ClusterBootstrapState,
        _events: Vec<KvEvent>,
        _targets: Vec<ClusterEndpointRecord>,
    ) {
    }
}

pub trait ClusterRaftQuorumCommitHost:
    sinomo_kv::ClusterKvRaftCommandStorage<Error = NetworkError> + ClusterRaftQuorumTransportHost
{
}

impl<T> ClusterRaftQuorumCommitHost for T where
    T: sinomo_kv::ClusterKvRaftCommandStorage<Error = NetworkError>
        + ClusterRaftQuorumTransportHost
{
}

pub(crate) struct LocalClusterRaftCommandStorage<'a> {
    paths: &'a ClusterPaths,
    inner: sinomo_kv::ClusterKvRaftLocalCommandStorage<fn(&str) -> bool>,
}

impl<'a> LocalClusterRaftCommandStorage<'a> {
    pub(crate) fn new(paths: &'a ClusterPaths, local_node_id: NodeId) -> Self {
        Self {
            paths,
            inner: sinomo_kv::ClusterKvRaftLocalCommandStorage::new(
                cluster_kv_raft_store(paths),
                local_node_id,
                cluster_kv_key_can_defer_materialized_snapshot,
            ),
        }
    }

    fn observe_last_snapshot_publish(&mut self) {
        if let Some(wrote) = self.inner.take_last_snapshot_publish_wrote() {
            observe_cluster_kv_snapshot_write(self.paths, wrote);
        }
    }

    pub(crate) fn append_uncommitted_or_no_effect(
        &mut self,
        command: KvCommand,
    ) -> Result<sinomo_kv::ClusterKvRaftUncommittedAppend, NetworkError> {
        self.inner
            .append_uncommitted_or_no_effect(command)
            .map_err(Into::into)
    }

    pub(crate) fn discard_uncommitted_tail_and_clear_leader(
        &mut self,
    ) -> Result<KvSnapshot, NetworkError> {
        let snapshot = self.inner.discard_uncommitted_tail_and_clear_leader()?;
        self.observe_last_snapshot_publish();
        Ok(snapshot)
    }

    pub(crate) fn commit_index_with_events(
        &mut self,
        index: u64,
    ) -> Result<sinomo_kv::ClusterKvRaftCommitResult, NetworkError> {
        let result = self.inner.commit_index_with_events(index)?;
        self.observe_last_snapshot_publish();
        Ok(result)
    }
}

impl sinomo_kv::ClusterKvRaftCommandStorage for LocalClusterRaftCommandStorage<'_> {
    type Error = NetworkError;

    fn append_cluster_raft_command_uncommitted_or_no_effect(
        &mut self,
        command: KvCommand,
    ) -> Result<sinomo_kv::ClusterKvRaftUncommittedAppend, NetworkError> {
        self.append_uncommitted_or_no_effect(command)
    }

    fn discard_cluster_raft_uncommitted_tail_and_clear_leader(
        &mut self,
    ) -> Result<(), NetworkError> {
        self.discard_uncommitted_tail_and_clear_leader().map(|_| ())
    }

    fn commit_cluster_raft_index_with_events(
        &mut self,
        index: u64,
    ) -> Result<sinomo_kv::ClusterKvRaftCommitResult, NetworkError> {
        self.commit_index_with_events(index)
    }
}

#[cfg(test)]
struct LocalClusterRaftQuorumCommitHost<'a, S, R, F, O> {
    storage: LocalClusterRaftCommandStorage<'a>,
    control_plane_timeouts: ClusterControlPlaneTimeouts,
    send_raft_append: S,
    record_endpoint_command_connectivity: R,
    fan_out_committed_cluster_raft_command: F,
    on_raft_append_sent: O,
}

#[cfg(test)]
impl<S, R, F, O> sinomo_kv::ClusterKvRaftCommandStorage
    for LocalClusterRaftQuorumCommitHost<'_, S, R, F, O>
{
    type Error = NetworkError;

    fn append_cluster_raft_command_uncommitted_or_no_effect(
        &mut self,
        command: KvCommand,
    ) -> Result<sinomo_kv::ClusterKvRaftUncommittedAppend, NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::append_cluster_raft_command_uncommitted_or_no_effect(
            &mut self.storage,
            command,
        )
    }

    fn discard_cluster_raft_uncommitted_tail_and_clear_leader(
        &mut self,
    ) -> Result<(), NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::discard_cluster_raft_uncommitted_tail_and_clear_leader(
            &mut self.storage,
        )
    }

    fn commit_cluster_raft_index_with_events(
        &mut self,
        index: u64,
    ) -> Result<sinomo_kv::ClusterKvRaftCommitResult, NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::commit_cluster_raft_index_with_events(
            &mut self.storage,
            index,
        )
    }
}

#[cfg(test)]
impl<S, R, F, O> ClusterRaftQuorumTransportHost for LocalClusterRaftQuorumCommitHost<'_, S, R, F, O>
where
    S: FnMut(
        &ClusterEndpointRecord,
        &ClusterRaftAppendRequest,
        Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError>,
    R: FnMut(
        &ClusterPaths,
        &ClusterBootstrapState,
        &ClusterEndpointRecord,
        ClusterConnectivityState,
        Option<u64>,
    ),
    F: FnMut(ClusterPaths, ClusterBootstrapState, Vec<KvEvent>, Vec<ClusterEndpointRecord>),
    O: FnMut(),
{
    fn replicate_cluster_raft_to_endpoint(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError> {
        let send_raft_append = &mut self.send_raft_append;
        let on_raft_append_sent = &mut self.on_raft_append_sent;
        replicate_cluster_raft_to_endpoint_with_local_host(
            paths,
            state,
            endpoint,
            &[],
            self.control_plane_timeouts
                .raft_append_replication_options(timeout),
            |endpoint, request, timeout| send_raft_append(endpoint, request, timeout),
            || on_raft_append_sent(),
        )
        .map(|outcome| outcome.response)
    }

    fn record_endpoint_command_connectivity(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
        connectivity_state: ClusterConnectivityState,
        latency_ms: Option<u64>,
    ) {
        (self.record_endpoint_command_connectivity)(
            paths,
            state,
            endpoint,
            connectivity_state,
            latency_ms,
        );
    }

    fn fan_out_committed_cluster_raft_command(
        &mut self,
        paths: ClusterPaths,
        state: ClusterBootstrapState,
        events: Vec<KvEvent>,
        targets: Vec<ClusterEndpointRecord>,
    ) {
        (self.fan_out_committed_cluster_raft_command)(paths, state, events, targets);
    }
}

#[cfg(test)]
pub fn commit_cluster_raft_command_with_local_storage_and_transport<S, R, F, O>(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    command: KvCommand,
    options: ClusterRaftQuorumCommitOptions,
    control_plane_timeouts: ClusterControlPlaneTimeouts,
    send_raft_append: S,
    record_endpoint_command_connectivity: R,
    fan_out_committed_cluster_raft_command: F,
    on_raft_append_sent: O,
) -> Result<ClusterRaftQuorumCommitResult, NetworkError>
where
    S: FnMut(
        &ClusterEndpointRecord,
        &ClusterRaftAppendRequest,
        Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError>,
    R: FnMut(
        &ClusterPaths,
        &ClusterBootstrapState,
        &ClusterEndpointRecord,
        ClusterConnectivityState,
        Option<u64>,
    ),
    F: FnMut(ClusterPaths, ClusterBootstrapState, Vec<KvEvent>, Vec<ClusterEndpointRecord>),
    O: FnMut(),
{
    let mut host = LocalClusterRaftQuorumCommitHost {
        storage: LocalClusterRaftCommandStorage::new(paths, state.local_node_id.clone()),
        control_plane_timeouts,
        send_raft_append,
        record_endpoint_command_connectivity,
        fan_out_committed_cluster_raft_command,
        on_raft_append_sent,
    };
    commit_cluster_raft_command_with_quorum_driver(paths, state, command, options, &mut host)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterRaftCommittedFanoutOptions {
    pub raft_append_timeout: Duration,
    pub watch_event_timeout: Duration,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClusterRaftCommittedFanoutReport {
    pub attempted_endpoints: usize,
    pub replicated_nodes: Vec<NodeId>,
    pub watch_event_batches_sent: usize,
}

pub trait ClusterRaftCommittedFanoutAsyncHost {
    fn replicate_cluster_raft_to_endpoint_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        endpoint: &'a ClusterEndpointRecord,
        watch_events: &'a [KvEvent],
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterRaftAppendReplicationOutcome>;

    fn send_cluster_kv_watch_events_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a ClusterKvWatchEventsRequest,
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterKvWatchEventsResponse>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterVoterReachabilityOptions {
    pub timeout: Duration,
    pub tcp_command_timeout: Duration,
    pub iroh_command_timeout: Duration,
    pub tcp_foreground_attempt_timeout: Duration,
    pub public_foreground_attempt_timeout: Duration,
    pub iroh_foreground_attempt_timeout: Duration,
}

impl ClusterVoterReachabilityOptions {
    pub fn command_timeout(
        &self,
        endpoint: &ClusterEndpointRecord,
        remaining: Duration,
    ) -> Duration {
        match endpoint.transport {
            ClusterTransportKind::Iroh => remaining.min(self.iroh_command_timeout),
            _ => remaining.min(self.tcp_command_timeout),
        }
    }

    pub fn fair_command_timeout(
        &self,
        endpoint: &ClusterEndpointRecord,
        remaining: Duration,
    ) -> Duration {
        let attempt_cap = match endpoint.transport {
            ClusterTransportKind::Iroh => self.iroh_foreground_attempt_timeout,
            _ if matches!(endpoint.privacy, ClusterEndpointPrivacy::InternetDirect) => {
                self.public_foreground_attempt_timeout
            }
            _ => self.tcp_foreground_attempt_timeout,
        };
        self.command_timeout(endpoint, remaining.min(attempt_cap))
    }
}

pub trait ClusterVoterReachabilityHost {
    fn probe_voter(&mut self, endpoint: &ClusterEndpointRecord, timeout: Duration) -> bool;
}

pub trait ClusterVoterReachabilityAsyncHost {
    fn probe_voter_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, bool>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterRaftLeaderElectionOptions {
    pub kv: ClusterKvRaftLeaderElectionOptions,
    pub timeout: Duration,
    pub tcp_command_timeout: Duration,
    pub iroh_command_timeout: Duration,
    pub tcp_foreground_attempt_timeout: Duration,
    pub public_foreground_attempt_timeout: Duration,
    pub iroh_foreground_attempt_timeout: Duration,
}

impl ClusterRaftLeaderElectionOptions {
    pub fn kv_options(&self) -> sinomo_kv::ClusterKvRaftLeaderElectionOptions {
        self.kv
    }

    pub fn command_timeout(
        &self,
        endpoint: &ClusterEndpointRecord,
        remaining: Duration,
    ) -> Duration {
        match endpoint.transport {
            ClusterTransportKind::Iroh => remaining.min(self.iroh_command_timeout),
            _ => remaining.min(self.tcp_command_timeout),
        }
    }

    pub fn fair_command_timeout(
        &self,
        endpoint: &ClusterEndpointRecord,
        remaining: Duration,
    ) -> Duration {
        let attempt_cap = match endpoint.transport {
            ClusterTransportKind::Iroh => self.iroh_foreground_attempt_timeout,
            _ if matches!(endpoint.privacy, ClusterEndpointPrivacy::InternetDirect) => {
                self.public_foreground_attempt_timeout
            }
            _ => self.tcp_foreground_attempt_timeout,
        };
        self.command_timeout(endpoint, remaining.min(attempt_cap))
    }
}

pub trait ClusterRaftVoteTransportHost {
    fn send_raft_vote(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterRaftVoteRequest,
        timeout: Duration,
    ) -> Result<ClusterRaftVoteResponse, NetworkError>;
}

pub trait ClusterRaftVoteTransportAsyncHost {
    fn send_raft_vote_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a ClusterRaftVoteRequest,
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterRaftVoteResponse>;
}

struct NetworkClusterKvRaftLeaderElectionHost<'a, H> {
    paths: &'a ClusterPaths,
    state: &'a ClusterBootstrapState,
    options: ClusterRaftLeaderElectionOptions,
    remote_vote_targets: Vec<ClusterEndpointRecord>,
    attempt_deadline: Instant,
    transport: &'a mut H,
}

impl<'a, H> NetworkClusterKvRaftLeaderElectionHost<'a, H> {
    fn new(
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        options: ClusterRaftLeaderElectionOptions,
        remote_vote_targets: Vec<ClusterEndpointRecord>,
        transport: &'a mut H,
    ) -> Self {
        Self {
            paths,
            state,
            options,
            remote_vote_targets,
            attempt_deadline: Instant::now(),
            transport,
        }
    }
}

impl<H> sinomo_kv::ClusterKvRaftLeaderElectionHost for NetworkClusterKvRaftLeaderElectionHost<'_, H>
where
    H: ClusterRaftVoteTransportHost,
{
    type Error = NetworkError;

    fn begin_election_attempt(&mut self) -> Result<(), Self::Error> {
        self.attempt_deadline = Instant::now() + self.options.timeout;
        Ok(())
    }

    fn local_vote_metadata(&mut self) -> Result<ClusterKvRaftLocalVoteMetadata, Self::Error> {
        locked_cluster_raft_local_vote_metadata(self.paths, &self.state.local_node_id)
    }

    fn apply_local_vote(
        &mut self,
        vote: RaftRequestVoteRequest,
    ) -> Result<RaftRequestVoteResponse, Self::Error> {
        locked_apply_cluster_raft_vote(self.paths, &self.state.local_node_id, vote)
    }

    fn should_request_more_remote_votes(&mut self) -> bool {
        self.attempt_deadline
            .checked_duration_since(Instant::now())
            .is_some_and(|remaining| !remaining.is_zero())
    }

    fn request_remote_vote(
        &mut self,
        target_index: usize,
        voter_id: &NodeId,
        vote: &RaftRequestVoteRequest,
    ) -> Result<RaftRequestVoteResponse, Self::Error> {
        let endpoint =
            self.remote_vote_targets
                .get(target_index)
                .ok_or_else(|| NetworkError::Io {
                    operation: "cluster raft vote".to_string(),
                    detail: format!("missing vote endpoint for target index {target_index}"),
                })?;
        if &endpoint.node_id != voter_id {
            return Err(NetworkError::Io {
                operation: "cluster raft vote".to_string(),
                detail: format!(
                    "vote endpoint target mismatch: endpoint is {}, election target is {}",
                    endpoint.node_id, voter_id
                ),
            });
        }
        let remaining = self
            .attempt_deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            return Err(NetworkError::Io {
                operation: "cluster raft vote".to_string(),
                detail: "election attempt timed out".to_string(),
            });
        }
        let request = ClusterRaftVoteRequest {
            state: self.state.clone(),
            vote: vote.clone(),
        };
        self.transport
            .send_raft_vote(
                endpoint,
                &request,
                self.options.fair_command_timeout(endpoint, remaining),
            )
            .map(|response| response.vote)
    }

    fn observe_raft_term(&mut self, term: u64) -> Result<(), Self::Error> {
        locked_observe_cluster_raft_term(self.paths, &self.state.local_node_id, term).map(|_| ())
    }

    fn set_raft_leader(&mut self, term: u64) -> Result<(), Self::Error> {
        locked_set_cluster_raft_leader(self.paths, &self.state.local_node_id, term).map(|_| ())
    }
}

struct NetworkClusterKvRaftLeaderElectionAsyncHost<'a, H> {
    runtime: &'a tokio::runtime::Handle,
    paths: &'a ClusterPaths,
    state: &'a ClusterBootstrapState,
    options: ClusterRaftLeaderElectionOptions,
    remote_vote_targets: Vec<ClusterEndpointRecord>,
    attempt_deadline: Instant,
    transport: &'a mut H,
}

impl<'a, H> NetworkClusterKvRaftLeaderElectionAsyncHost<'a, H> {
    fn new(
        runtime: &'a tokio::runtime::Handle,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        options: ClusterRaftLeaderElectionOptions,
        remote_vote_targets: Vec<ClusterEndpointRecord>,
        transport: &'a mut H,
    ) -> Self {
        Self {
            runtime,
            paths,
            state,
            options,
            remote_vote_targets,
            attempt_deadline: Instant::now(),
            transport,
        }
    }
}

impl<H> sinomo_kv::ClusterKvRaftLeaderElectionAsyncHost
    for NetworkClusterKvRaftLeaderElectionAsyncHost<'_, H>
where
    H: ClusterRaftVoteTransportAsyncHost + Send,
{
    type Error = NetworkError;

    fn begin_election_attempt_async<'a>(
        &'a mut self,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        Box::pin(async move {
            self.attempt_deadline = Instant::now() + self.options.timeout;
            Ok(())
        })
    }

    fn local_vote_metadata_async<'a>(
        &'a mut self,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, ClusterKvRaftLocalVoteMetadata, Self::Error> {
        let paths = self.paths.clone();
        let node_id = self.state.local_node_id.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            runtime
                .spawn_blocking(move || locked_cluster_raft_local_vote_metadata(&paths, &node_id))
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "cluster raft leader election".to_string(),
                    detail: format!("local vote metadata task failed: {source}"),
                })?
        })
    }

    fn apply_local_vote_async<'a>(
        &'a mut self,
        vote: RaftRequestVoteRequest,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, RaftRequestVoteResponse, Self::Error> {
        let paths = self.paths.clone();
        let node_id = self.state.local_node_id.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            runtime
                .spawn_blocking(move || locked_apply_cluster_raft_vote(&paths, &node_id, vote))
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "cluster raft leader election".to_string(),
                    detail: format!("local vote apply task failed: {source}"),
                })?
        })
    }

    fn should_request_more_remote_votes(&mut self) -> bool {
        self.attempt_deadline
            .checked_duration_since(Instant::now())
            .is_some_and(|remaining| !remaining.is_zero())
    }

    fn request_remote_vote_async<'a>(
        &'a mut self,
        target_index: usize,
        voter_id: &'a NodeId,
        vote: &'a RaftRequestVoteRequest,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, RaftRequestVoteResponse, Self::Error> {
        let endpoint = match self.remote_vote_targets.get(target_index).cloned() {
            Some(endpoint) => endpoint,
            None => {
                return Box::pin(async move {
                    Err(NetworkError::Io {
                        operation: "cluster raft vote".to_string(),
                        detail: format!("missing vote endpoint for target index {target_index}"),
                    })
                });
            }
        };
        if &endpoint.node_id != voter_id {
            let endpoint_node_id = endpoint.node_id.clone();
            let voter_id = voter_id.clone();
            return Box::pin(async move {
                Err(NetworkError::Io {
                    operation: "cluster raft vote".to_string(),
                    detail: format!(
                        "vote endpoint target mismatch: endpoint is {endpoint_node_id}, election target is {voter_id}"
                    ),
                })
            });
        }
        let remaining = self
            .attempt_deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            return Box::pin(async {
                Err(NetworkError::Io {
                    operation: "cluster raft vote".to_string(),
                    detail: "election attempt timed out".to_string(),
                })
            });
        }
        let request = ClusterRaftVoteRequest {
            state: self.state.clone(),
            vote: vote.clone(),
        };
        let timeout = self.options.fair_command_timeout(&endpoint, remaining);
        let transport = &mut *self.transport;
        Box::pin(async move {
            transport
                .send_raft_vote_async(&endpoint, &request, timeout)
                .await
                .map(|response| response.vote)
        })
    }

    fn observe_raft_term_async<'a>(
        &'a mut self,
        term: u64,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        let paths = self.paths.clone();
        let node_id = self.state.local_node_id.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            runtime
                .spawn_blocking(move || locked_observe_cluster_raft_term(&paths, &node_id, term))
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "cluster raft leader election".to_string(),
                    detail: format!("term observation task failed: {source}"),
                })?
                .map(|_| ())
        })
    }

    fn set_raft_leader_async<'a>(
        &'a mut self,
        term: u64,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        let paths = self.paths.clone();
        let node_id = self.state.local_node_id.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            runtime
                .spawn_blocking(move || locked_set_cluster_raft_leader(&paths, &node_id, term))
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "cluster raft leader election".to_string(),
                    detail: format!("leader write task failed: {source}"),
                })?
                .map(|_| ())
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterKvTxnQuorumOptions {
    pub allow_forwarding: bool,
    pub raft_append_timeout: Duration,
    pub raft_append_replication_options: ClusterRaftAppendReplicationOptions,
    pub forward_options: ClusterKvTxnForwardOptions,
    pub leader_election_options: ClusterRaftLeaderElectionOptions,
    pub voter_reachability_options: ClusterVoterReachabilityOptions,
}

pub const DEFAULT_CLUSTER_CONTROL_PLANE_RAFT_APPEND_MAX_ATTEMPTS: usize = 32;
pub const DEFAULT_CLUSTER_CONTROL_PLANE_RPC_TIMEOUT: Duration = Duration::from_millis(750);
pub const DEFAULT_CLUSTER_CONTROL_PLANE_MEMBERSHIP_RPC_TIMEOUT: Duration = Duration::from_secs(15);
pub const DEFAULT_CLUSTER_CONTROL_PLANE_TCP_COMMAND_TIMEOUT: Duration = Duration::from_secs(5);
pub const DEFAULT_CLUSTER_CONTROL_PLANE_IROH_COMMAND_TIMEOUT: Duration = Duration::from_secs(3);
pub const DEFAULT_CLUSTER_CONTROL_PLANE_TCP_FOREGROUND_ATTEMPT_TIMEOUT: Duration =
    Duration::from_secs(1);
pub const DEFAULT_CLUSTER_CONTROL_PLANE_PUBLIC_FOREGROUND_ATTEMPT_TIMEOUT: Duration =
    Duration::from_secs(4);
pub const DEFAULT_CLUSTER_CONTROL_PLANE_IROH_FOREGROUND_ATTEMPT_TIMEOUT: Duration =
    Duration::from_millis(1500);
pub const DEFAULT_CLUSTER_CONTROL_PLANE_RAFT_APPEND_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterControlPlaneTimeouts {
    pub max_raft_append_attempts: usize,
    pub raft_append_timeout: Duration,
    pub rpc_timeout: Duration,
    pub membership_rpc_timeout: Duration,
    pub tcp_command_timeout: Duration,
    pub iroh_command_timeout: Duration,
    pub tcp_foreground_attempt_timeout: Duration,
    pub public_foreground_attempt_timeout: Duration,
    pub iroh_foreground_attempt_timeout: Duration,
}

impl ClusterControlPlaneTimeouts {
    pub fn default_daemon_transport() -> Self {
        Self {
            max_raft_append_attempts: DEFAULT_CLUSTER_CONTROL_PLANE_RAFT_APPEND_MAX_ATTEMPTS,
            raft_append_timeout: DEFAULT_CLUSTER_CONTROL_PLANE_RAFT_APPEND_TIMEOUT,
            rpc_timeout: DEFAULT_CLUSTER_CONTROL_PLANE_RPC_TIMEOUT,
            membership_rpc_timeout: DEFAULT_CLUSTER_CONTROL_PLANE_MEMBERSHIP_RPC_TIMEOUT,
            tcp_command_timeout: DEFAULT_CLUSTER_CONTROL_PLANE_TCP_COMMAND_TIMEOUT,
            iroh_command_timeout: DEFAULT_CLUSTER_CONTROL_PLANE_IROH_COMMAND_TIMEOUT,
            tcp_foreground_attempt_timeout:
                DEFAULT_CLUSTER_CONTROL_PLANE_TCP_FOREGROUND_ATTEMPT_TIMEOUT,
            public_foreground_attempt_timeout:
                DEFAULT_CLUSTER_CONTROL_PLANE_PUBLIC_FOREGROUND_ATTEMPT_TIMEOUT,
            iroh_foreground_attempt_timeout:
                DEFAULT_CLUSTER_CONTROL_PLANE_IROH_FOREGROUND_ATTEMPT_TIMEOUT,
        }
    }

    pub fn raft_append_replication_options(
        &self,
        timeout: Duration,
    ) -> ClusterRaftAppendReplicationOptions {
        ClusterRaftAppendReplicationOptions {
            max_attempts: self.max_raft_append_attempts,
            timeout,
            tcp_command_timeout: self.tcp_command_timeout,
            iroh_command_timeout: self.iroh_command_timeout,
            tcp_foreground_attempt_timeout: self.tcp_foreground_attempt_timeout,
            public_foreground_attempt_timeout: self.public_foreground_attempt_timeout,
            iroh_foreground_attempt_timeout: self.iroh_foreground_attempt_timeout,
        }
    }

    pub fn committed_fanout_options(&self) -> ClusterRaftCommittedFanoutOptions {
        ClusterRaftCommittedFanoutOptions {
            raft_append_timeout: self.raft_append_timeout,
            watch_event_timeout: self.rpc_timeout,
        }
    }

    pub fn leader_election_options(&self) -> ClusterRaftLeaderElectionOptions {
        ClusterRaftLeaderElectionOptions {
            kv: ClusterKvRaftLeaderElectionOptions {
                max_attempts: sinomo_kv::CLUSTER_KV_RAFT_LEADER_ELECTION_ATTEMPTS,
            },
            timeout: self.raft_append_timeout,
            tcp_command_timeout: self.tcp_command_timeout,
            iroh_command_timeout: self.iroh_command_timeout,
            tcp_foreground_attempt_timeout: self.tcp_foreground_attempt_timeout,
            public_foreground_attempt_timeout: self.public_foreground_attempt_timeout,
            iroh_foreground_attempt_timeout: self.iroh_foreground_attempt_timeout,
        }
    }

    pub fn voter_reachability_options(&self) -> ClusterVoterReachabilityOptions {
        ClusterVoterReachabilityOptions {
            timeout: self.raft_append_timeout,
            tcp_command_timeout: self.tcp_command_timeout,
            iroh_command_timeout: self.iroh_command_timeout,
            tcp_foreground_attempt_timeout: self.tcp_foreground_attempt_timeout,
            public_foreground_attempt_timeout: self.public_foreground_attempt_timeout,
            iroh_foreground_attempt_timeout: self.iroh_foreground_attempt_timeout,
        }
    }

    pub fn kv_txn_forward_options(&self) -> ClusterKvTxnForwardOptions {
        ClusterKvTxnForwardOptions {
            timeout: self.raft_append_timeout,
            tcp_command_timeout: self.tcp_command_timeout,
            iroh_command_timeout: self.iroh_command_timeout,
        }
    }

    pub fn kv_txn_quorum_options(&self, allow_forwarding: bool) -> ClusterKvTxnQuorumOptions {
        ClusterKvTxnQuorumOptions {
            allow_forwarding,
            raft_append_timeout: self.raft_append_timeout,
            raft_append_replication_options: self
                .raft_append_replication_options(self.raft_append_timeout),
            forward_options: self.kv_txn_forward_options(),
            leader_election_options: self.leader_election_options(),
            voter_reachability_options: self.voter_reachability_options(),
        }
    }

    pub fn kv_snapshot_refresh_options(&self) -> ClusterKvSnapshotRefreshOptions {
        ClusterKvSnapshotRefreshOptions {
            without_local_snapshot_timeout: self.raft_append_timeout,
            with_local_snapshot_timeout: self.rpc_timeout,
            tcp_command_timeout: self.rpc_timeout,
            iroh_command_timeout: self.rpc_timeout,
            tcp_foreground_attempt_timeout: self.tcp_foreground_attempt_timeout,
            public_foreground_attempt_timeout: self.public_foreground_attempt_timeout,
            iroh_foreground_attempt_timeout: self.iroh_foreground_attempt_timeout,
        }
    }
}

pub trait ClusterKvTxnQuorumHost {
    fn refresh_current_cluster_kv_store(
        &mut self,
        _paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
    ) -> Result<(), NetworkError> {
        Ok(())
    }

    fn local_is_raft_leader(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<bool, NetworkError>;

    fn forward_cluster_kv_txn_to_voter(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        txn: &KvTxn,
        options: ClusterKvTxnForwardOptions,
    ) -> Result<Option<ClusterKvTxnForwardResult>, NetworkError>;

    fn elect_cluster_raft_leader(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        options: ClusterRaftLeaderElectionOptions,
    ) -> Result<bool, NetworkError>;

    fn reachable_voter_count(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        options: ClusterVoterReachabilityOptions,
    ) -> usize;

    fn commit_cluster_raft_command_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        command: KvCommand,
        options: ClusterRaftQuorumCommitOptions,
    ) -> Result<ClusterRaftQuorumCommitResult, NetworkError>;
}

pub trait ClusterKvTxnQuorumAsyncHost:
    ClusterKvTxnForwardAsyncHost
    + ClusterSignedEndpointRpcAsyncHost
    + ClusterRaftQuorumAsyncHost
    + ClusterRaftVoteTransportAsyncHost
    + ClusterVoterReachabilityAsyncHost
    + Send
{
}

impl<T> ClusterKvTxnQuorumAsyncHost for T where
    T: ClusterKvTxnForwardAsyncHost
        + ClusterSignedEndpointRpcAsyncHost
        + ClusterRaftQuorumAsyncHost
        + ClusterRaftVoteTransportAsyncHost
        + ClusterVoterReachabilityAsyncHost
        + Send
{
}

pub trait ClusterControlPlaneTransport {
    fn send_raft_append(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError>;

    fn send_raft_vote(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterRaftVoteRequest,
        timeout: Duration,
    ) -> Result<ClusterRaftVoteResponse, NetworkError>;

    fn probe_voter(&mut self, endpoint: &ClusterEndpointRecord, timeout: Duration) -> bool;

    fn send_cluster_kv_txn(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        txn: &KvTxn,
        timeout: Duration,
    ) -> Result<KvTxnResponse, NetworkError>;

    fn fetch_cluster_kv_snapshot(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> Result<KvSnapshot, NetworkError>;

    fn send_cluster_member_publish(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterMemberPublishRequest,
        timeout: Duration,
    ) -> Result<ClusterMemberPublishResponse, NetworkError>;

    fn send_cluster_membership_rpc(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, NetworkError>;

    fn send_object_rpc_to_node(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, NetworkError>;

    fn record_endpoint_command_connectivity(
        &mut self,
        _paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
        _endpoint: &ClusterEndpointRecord,
        _connectivity_state: ClusterConnectivityState,
        _latency_ms: Option<u64>,
    ) {
    }

    fn published_network_info_matches(
        &mut self,
        _paths: &ClusterPaths,
        _info: &ClusterNetworkInfoRecord,
    ) -> bool {
        false
    }

    fn remember_published_network_info(
        &mut self,
        _paths: &ClusterPaths,
        _info: &ClusterNetworkInfoRecord,
    ) {
    }

    fn published_node_status_matches(
        &mut self,
        _paths: &ClusterPaths,
        _status: &ClusterNodeStatusRecord,
    ) -> bool {
        false
    }

    fn remember_published_node_status(
        &mut self,
        _paths: &ClusterPaths,
        _status: &ClusterNodeStatusRecord,
    ) {
    }

    fn published_connectivity_record_set_matches(
        &mut self,
        _paths: &ClusterPaths,
        _source_node_id: &NodeId,
        _records: &[ClusterConnectivityRecord],
    ) -> bool {
        false
    }

    fn remember_published_connectivity_record_set(
        &mut self,
        _paths: &ClusterPaths,
        _source_node_id: &NodeId,
        _records: &[ClusterConnectivityRecord],
    ) {
    }

    fn fan_out_committed_cluster_raft_command(
        &mut self,
        _paths: ClusterPaths,
        _state: ClusterBootstrapState,
        _events: Vec<KvEvent>,
        _targets: Vec<ClusterEndpointRecord>,
    ) {
    }

    fn on_raft_append_sent(&mut self) {}
}

pub struct ClusterControlPlaneDriver<T> {
    transport: T,
    timeouts: ClusterControlPlaneTimeouts,
}

impl<T> ClusterControlPlaneDriver<T> {
    pub fn new(transport: T) -> Self {
        Self::with_timeouts(
            transport,
            ClusterControlPlaneTimeouts::default_daemon_transport(),
        )
    }

    pub fn with_timeouts(transport: T, timeouts: ClusterControlPlaneTimeouts) -> Self {
        Self {
            transport,
            timeouts,
        }
    }

    pub fn transport_mut(&mut self) -> &mut T {
        &mut self.transport
    }

    pub fn into_transport(self) -> T {
        self.transport
    }
}

struct DriverRaftAppendReplicationHost<'a, T> {
    transport: &'a mut T,
}

impl<T> ClusterRaftAppendReplicationHost for DriverRaftAppendReplicationHost<'_, T>
where
    T: ClusterControlPlaneTransport,
{
    fn last_local_raft_log_index(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<u64, NetworkError> {
        cluster_kv_raft_store(paths)
            .locked_last_log_index(&state.local_node_id)
            .map_err(Into::into)
    }

    fn build_raft_append_request(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        next_index: u64,
    ) -> Result<ClusterRaftAppendRequest, NetworkError> {
        cluster_kv_raft_store(paths)
            .locked_build_cluster_raft_append_request_from_next_index(state, next_index)
            .map_err(Into::into)
    }

    fn send_raft_append(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError> {
        self.transport.send_raft_append(endpoint, request, timeout)
    }

    fn on_raft_append_sent(&mut self) {
        self.transport.on_raft_append_sent();
    }
}

struct DriverRaftQuorumCommitHost<'a, T> {
    storage: LocalClusterRaftCommandStorage<'a>,
    driver: &'a mut ClusterControlPlaneDriver<T>,
}

impl<T> sinomo_kv::ClusterKvRaftCommandStorage for DriverRaftQuorumCommitHost<'_, T> {
    type Error = NetworkError;

    fn append_cluster_raft_command_uncommitted_or_no_effect(
        &mut self,
        command: KvCommand,
    ) -> Result<sinomo_kv::ClusterKvRaftUncommittedAppend, NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::append_cluster_raft_command_uncommitted_or_no_effect(
            &mut self.storage,
            command,
        )
    }

    fn discard_cluster_raft_uncommitted_tail_and_clear_leader(
        &mut self,
    ) -> Result<(), NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::discard_cluster_raft_uncommitted_tail_and_clear_leader(
            &mut self.storage,
        )
    }

    fn commit_cluster_raft_index_with_events(
        &mut self,
        index: u64,
    ) -> Result<sinomo_kv::ClusterKvRaftCommitResult, NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::commit_cluster_raft_index_with_events(
            &mut self.storage,
            index,
        )
    }
}

impl<T> ClusterRaftQuorumTransportHost for DriverRaftQuorumCommitHost<'_, T>
where
    T: ClusterControlPlaneTransport,
{
    fn replicate_cluster_raft_to_endpoint(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError> {
        let mut host = DriverRaftAppendReplicationHost {
            transport: &mut self.driver.transport,
        };
        replicate_cluster_raft_to_endpoint_with_host(
            paths,
            state,
            endpoint,
            &[],
            self.driver
                .timeouts
                .raft_append_replication_options(timeout),
            &mut host,
        )
        .map(|outcome| outcome.response)
    }

    fn record_endpoint_command_connectivity(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
        connectivity_state: ClusterConnectivityState,
        latency_ms: Option<u64>,
    ) {
        self.driver.transport.record_endpoint_command_connectivity(
            paths,
            state,
            endpoint,
            connectivity_state,
            latency_ms,
        );
    }

    fn fan_out_committed_cluster_raft_command(
        &mut self,
        paths: ClusterPaths,
        state: ClusterBootstrapState,
        events: Vec<KvEvent>,
        targets: Vec<ClusterEndpointRecord>,
    ) {
        self.driver
            .transport
            .fan_out_committed_cluster_raft_command(paths, state, events, targets);
    }
}

impl<T> ClusterRaftCommittedFanoutAsyncHost for ClusterControlPlaneDriver<T>
where
    T: ClusterRaftCommittedFanoutAsyncHost,
{
    fn replicate_cluster_raft_to_endpoint_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        endpoint: &'a ClusterEndpointRecord,
        watch_events: &'a [KvEvent],
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterRaftAppendReplicationOutcome> {
        self.transport.replicate_cluster_raft_to_endpoint_async(
            paths,
            state,
            endpoint,
            watch_events,
            timeout,
        )
    }

    fn send_cluster_kv_watch_events_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a ClusterKvWatchEventsRequest,
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterKvWatchEventsResponse> {
        self.transport
            .send_cluster_kv_watch_events_async(endpoint, request, timeout)
    }
}

impl<T> ClusterVoterReachabilityHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    fn probe_voter(&mut self, endpoint: &ClusterEndpointRecord, timeout: Duration) -> bool {
        self.transport.probe_voter(endpoint, timeout)
    }
}

impl<T> ClusterRaftVoteTransportHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    fn send_raft_vote(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterRaftVoteRequest,
        timeout: Duration,
    ) -> Result<ClusterRaftVoteResponse, NetworkError> {
        self.transport.send_raft_vote(endpoint, request, timeout)
    }
}

impl<T> ClusterKvSnapshotRefreshHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    fn known_raft_leader_id(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<Option<NodeId>, NetworkError> {
        locked_cluster_raft_known_leader_id(paths, state)
    }

    fn fetch_cluster_kv_snapshot(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> Result<KvSnapshot, NetworkError> {
        self.transport.fetch_cluster_kv_snapshot(endpoint, timeout)
    }

    fn restore_cluster_raft_from_snapshot_if_newer(
        &mut self,
        paths: &ClusterPaths,
        local_node_id: &NodeId,
        snapshot: KvSnapshot,
    ) -> Result<bool, NetworkError> {
        locked_restore_cluster_raft_from_snapshot_if_newer(paths, local_node_id, snapshot)
    }

    fn send_object_rpc_to_node(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, NetworkError> {
        self.transport
            .send_object_rpc_to_node(state, target_node_id, request, timeout)
    }
}

impl<T> ClusterKvTxnForwardHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    fn known_raft_leader_id(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<Option<NodeId>, NetworkError> {
        locked_cluster_raft_known_leader_id(paths, state)
    }

    fn send_cluster_kv_txn(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        txn: &KvTxn,
        timeout: Duration,
    ) -> Result<KvTxnResponse, NetworkError> {
        self.transport.send_cluster_kv_txn(endpoint, txn, timeout)
    }

    fn fetch_cluster_kv_snapshot(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> Result<KvSnapshot, NetworkError> {
        self.transport.fetch_cluster_kv_snapshot(endpoint, timeout)
    }

    fn send_object_rpc_to_node(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, NetworkError> {
        self.transport
            .send_object_rpc_to_node(state, target_node_id, request, timeout)
    }
}

impl<T> ClusterKvTxnQuorumHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    fn refresh_current_cluster_kv_store(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<(), NetworkError> {
        refresh_current_cluster_kv_store_with_host(
            paths,
            state,
            self.timeouts.kv_snapshot_refresh_options(),
            self,
        )
        .map(|_| ())
    }

    fn local_is_raft_leader(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<bool, NetworkError> {
        locked_cluster_raft_local_is_leader(paths, state)
    }

    fn forward_cluster_kv_txn_to_voter(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        txn: &KvTxn,
        options: ClusterKvTxnForwardOptions,
    ) -> Result<Option<ClusterKvTxnForwardResult>, NetworkError> {
        forward_cluster_kv_txn_to_voter_with_host(paths, state, txn, options, self)
    }

    fn elect_cluster_raft_leader(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        options: ClusterRaftLeaderElectionOptions,
    ) -> Result<bool, NetworkError> {
        elect_cluster_raft_leader_with_host(paths, state, options, self)
    }

    fn reachable_voter_count(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        options: ClusterVoterReachabilityOptions,
    ) -> usize {
        cluster_reachable_voter_count_with_host(paths, state, options, self)
    }

    fn commit_cluster_raft_command_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        command: KvCommand,
        options: ClusterRaftQuorumCommitOptions,
    ) -> Result<ClusterRaftQuorumCommitResult, NetworkError> {
        let mut host = DriverRaftQuorumCommitHost {
            storage: LocalClusterRaftCommandStorage::new(paths, state.local_node_id.clone()),
            driver: self,
        };
        commit_cluster_raft_command_with_quorum_driver(paths, state, command, options, &mut host)
    }
}

impl<T> ClusterMemberPublishHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    type Error = NetworkError;

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, Self::Error> {
        let options = self.timeouts.kv_txn_quorum_options(true);
        commit_cluster_kv_txn_with_quorum_response_with_host(paths, state, txn, options, self)
            .map(|result| result.snapshot)
    }
}

impl<T> ClusterLocalMemberPublishHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    fn cluster_has_local_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> bool {
        let options = self.timeouts.voter_reachability_options();
        cluster_has_local_quorum_with_host(paths, state, options, self)
    }

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, NetworkError> {
        let options = self.timeouts.kv_txn_quorum_options(true);
        commit_cluster_kv_txn_with_quorum_response_with_host(paths, state, txn, options, self)
            .map(|result| result.snapshot)
    }

    fn send_cluster_member_publish(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterMemberPublishRequest,
        timeout: Duration,
    ) -> Result<ClusterMemberPublishResponse, NetworkError> {
        self.transport
            .send_cluster_member_publish(endpoint, request, timeout)
    }
}

impl<T> ClusterInvitePublishHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    type Error = NetworkError;

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, Self::Error> {
        let options = self.timeouts.kv_txn_quorum_options(true);
        commit_cluster_kv_txn_with_quorum_response_with_host(paths, state, txn, options, self)
            .map(|result| result.snapshot)
    }
}

impl<T> ClusterNetworkRecordExpiryHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    type Error = NetworkError;

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, Self::Error> {
        let options = self.timeouts.kv_txn_quorum_options(true);
        commit_cluster_kv_txn_with_quorum_response_with_host(paths, state, txn, options, self)
            .map(|result| result.snapshot)
    }
}

impl<T> ClusterNetworkInfoPublishHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    type Error = NetworkError;

    fn published_network_info_matches(
        &mut self,
        paths: &ClusterPaths,
        info: &ClusterNetworkInfoRecord,
    ) -> bool {
        self.transport.published_network_info_matches(paths, info)
    }

    fn remember_published_network_info(
        &mut self,
        paths: &ClusterPaths,
        info: &ClusterNetworkInfoRecord,
    ) {
        self.transport.remember_published_network_info(paths, info);
    }

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, Self::Error> {
        let options = self.timeouts.kv_txn_quorum_options(true);
        commit_cluster_kv_txn_with_quorum_response_with_host(paths, state, txn, options, self)
            .map(|result| result.snapshot)
    }
}

impl<T> ClusterNodeStatusPublishHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    type Error = NetworkError;

    fn published_node_status_matches(
        &mut self,
        paths: &ClusterPaths,
        status: &ClusterNodeStatusRecord,
    ) -> bool {
        self.transport.published_node_status_matches(paths, status)
    }

    fn remember_published_node_status(
        &mut self,
        paths: &ClusterPaths,
        status: &ClusterNodeStatusRecord,
    ) {
        self.transport.remember_published_node_status(paths, status);
    }

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, Self::Error> {
        let options = self.timeouts.kv_txn_quorum_options(true);
        commit_cluster_kv_txn_with_quorum_response_with_host(paths, state, txn, options, self)
            .map(|result| result.snapshot)
    }
}

impl<T> ClusterConnectivityPublishHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    type Error = NetworkError;

    fn published_connectivity_record_set_matches(
        &mut self,
        paths: &ClusterPaths,
        source_node_id: &NodeId,
        records: &[ClusterConnectivityRecord],
    ) -> bool {
        self.transport
            .published_connectivity_record_set_matches(paths, source_node_id, records)
    }

    fn remember_published_connectivity_record_set(
        &mut self,
        paths: &ClusterPaths,
        source_node_id: &NodeId,
        records: &[ClusterConnectivityRecord],
    ) {
        self.transport
            .remember_published_connectivity_record_set(paths, source_node_id, records);
    }

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, Self::Error> {
        let options = self.timeouts.kv_txn_quorum_options(true);
        commit_cluster_kv_txn_with_quorum_response_with_host(paths, state, txn, options, self)
            .map(|result| result.snapshot)
    }
}

impl<T> ClusterMemberMutationHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    fn current_cluster_kv_store(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<InProcessKvStore, NetworkError> {
        refresh_current_cluster_kv_store_with_host(
            paths,
            state,
            self.timeouts.kv_snapshot_refresh_options(),
            self,
        )
    }

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, NetworkError> {
        let options = self.timeouts.kv_txn_quorum_options(true);
        commit_cluster_kv_txn_with_quorum_response_with_host(paths, state, txn, options, self)
            .map(|result| result.snapshot)
    }
}

impl<T> ClusterMembershipRpcForwardHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    fn send_membership_rpc_to_node(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
    ) -> Result<ObjectRpcResponse, NetworkError> {
        self.transport.send_object_rpc_to_node(
            state,
            target_node_id,
            request,
            self.timeouts.membership_rpc_timeout,
        )
    }
}

impl<T> ClusterJoinRegistrationHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    fn commit_join_registration_state(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<KvSnapshot, NetworkError> {
        sync_cluster_state_to_kv_with_host(paths, state, self)
    }

    fn remember_successful_join_endpoint(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
    ) -> Result<(), NetworkError> {
        self.transport.record_endpoint_command_connectivity(
            paths,
            state,
            endpoint,
            ClusterConnectivityState::Healthy,
            None,
        );
        Ok(())
    }
}

impl<T> ClusterJoinReconciliationHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    fn send_join_registration(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterJoinRegistrationRequest,
        tcp_timeout: Duration,
        iroh_timeout: Duration,
    ) -> Result<ClusterJoinRegistrationResponse, NetworkError> {
        let timeout = match endpoint.transport {
            ClusterTransportKind::Iroh => iroh_timeout,
            _ => tcp_timeout,
        };
        send_cluster_join_registration_over_membership_rpc(
            endpoint,
            request,
            timeout,
            |endpoint, request, timeout| {
                self.transport
                    .send_cluster_membership_rpc(endpoint, request, timeout)
            },
        )
    }

    fn fetch_cluster_kv_snapshot(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> Result<KvSnapshot, NetworkError> {
        self.transport.fetch_cluster_kv_snapshot(endpoint, timeout)
    }

    fn observe_join_reconciliation_connectivity(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        reconciliation: &ClusterJoinReconciliation,
    ) -> Result<(), NetworkError> {
        if let Some(endpoint) = reconciliation.accepted_endpoint.as_ref() {
            self.transport.record_endpoint_command_connectivity(
                paths,
                state,
                endpoint,
                ClusterConnectivityState::Healthy,
                None,
            );
        }
        Ok(())
    }
}

pub type ClusterMembershipRpcFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, NetworkError>> + Send + 'a>>;

pub trait ClusterJoinRegistrationHost: ClusterMembershipRpcForwardHost {
    fn local_daemon_membership_observation(
        &mut self,
        _paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
    ) -> Result<Option<ClusterLocalDaemonMembershipObservation>, NetworkError> {
        Ok(None)
    }

    fn commit_join_registration_state(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<KvSnapshot, NetworkError>;

    fn remember_successful_join_targets(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        members: &[ClusterMemberRecord],
    ) -> Result<(), NetworkError> {
        for member in members {
            if member.node_id == state.local_node_id {
                continue;
            }
            for endpoint in &member.endpoints {
                if endpoint.privacy == ClusterEndpointPrivacy::LocalOnly {
                    continue;
                }
                self.remember_successful_join_endpoint(paths, state, endpoint)?;
            }
        }
        Ok(())
    }

    fn remember_successful_join_endpoint(
        &mut self,
        _paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
        _endpoint: &ClusterEndpointRecord,
    ) -> Result<(), NetworkError> {
        Ok(())
    }
}

pub trait ClusterJoinRegistrationAsyncHost: ClusterSignedEndpointRpcAsyncHost {
    fn local_daemon_membership_observation_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _state: &'a ClusterBootstrapState,
    ) -> ClusterMembershipRpcFuture<'a, Option<ClusterLocalDaemonMembershipObservation>> {
        Box::pin(async { Ok(None) })
    }

    fn commit_join_registration_state_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
    ) -> ClusterMembershipRpcFuture<'a, KvSnapshot>;

    fn remember_successful_join_targets_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        members: &'a [ClusterMemberRecord],
    ) -> ClusterMembershipRpcFuture<'a, ()>
    where
        Self: Send,
    {
        Box::pin(async move {
            for member in members {
                if member.node_id == state.local_node_id {
                    continue;
                }
                for endpoint in &member.endpoints {
                    if endpoint.privacy == ClusterEndpointPrivacy::LocalOnly {
                        continue;
                    }
                    self.remember_successful_join_endpoint_async(paths, state, endpoint)
                        .await?;
                }
            }
            Ok(())
        })
    }

    fn remember_successful_join_endpoint_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _state: &'a ClusterBootstrapState,
        _endpoint: &'a ClusterEndpointRecord,
    ) -> ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

pub trait ClusterLocalDaemonMembershipObserver {
    fn local_daemon_membership_observation(
        &mut self,
        _paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
    ) -> Result<Option<ClusterLocalDaemonMembershipObservation>, NetworkError> {
        Ok(None)
    }
}

pub trait ClusterLocalDaemonMembershipAsyncObserver {
    fn local_daemon_membership_observation_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _state: &'a ClusterBootstrapState,
    ) -> ClusterMembershipRpcFuture<'a, Option<ClusterLocalDaemonMembershipObservation>> {
        Box::pin(async { Ok(None) })
    }
}

pub struct ClusterJoinRegistrationDriver<T, O> {
    driver: ClusterControlPlaneDriver<T>,
    observer: O,
}

impl<T, O> ClusterJoinRegistrationDriver<T, O> {
    pub fn new(driver: ClusterControlPlaneDriver<T>, observer: O) -> Self {
        Self { driver, observer }
    }

    pub fn with_transport(transport: T, observer: O) -> Self {
        Self::new(ClusterControlPlaneDriver::new(transport), observer)
    }

    pub fn driver_mut(&mut self) -> &mut ClusterControlPlaneDriver<T> {
        &mut self.driver
    }

    pub fn observer_mut(&mut self) -> &mut O {
        &mut self.observer
    }
}

impl<T, O> ClusterMembershipRpcForwardHost for ClusterJoinRegistrationDriver<T, O>
where
    T: ClusterControlPlaneTransport,
{
    fn send_membership_rpc_to_node(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
    ) -> Result<ObjectRpcResponse, NetworkError> {
        ClusterMembershipRpcForwardHost::send_membership_rpc_to_node(
            &mut self.driver,
            state,
            target_node_id,
            request,
        )
    }
}

impl<T, O> ClusterJoinRegistrationHost for ClusterJoinRegistrationDriver<T, O>
where
    T: ClusterControlPlaneTransport,
    O: ClusterLocalDaemonMembershipObserver,
{
    fn local_daemon_membership_observation(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<Option<ClusterLocalDaemonMembershipObservation>, NetworkError> {
        self.observer
            .local_daemon_membership_observation(paths, state)
    }

    fn commit_join_registration_state(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<KvSnapshot, NetworkError> {
        ClusterJoinRegistrationHost::commit_join_registration_state(&mut self.driver, paths, state)
    }

    fn remember_successful_join_endpoint(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
    ) -> Result<(), NetworkError> {
        ClusterJoinRegistrationHost::remember_successful_join_endpoint(
            &mut self.driver,
            paths,
            state,
            endpoint,
        )
    }
}

pub struct ClusterMemberMutationDriver<T, O> {
    driver: ClusterControlPlaneDriver<T>,
    observer: O,
}

impl<T, O> ClusterMemberMutationDriver<T, O> {
    pub fn new(driver: ClusterControlPlaneDriver<T>, observer: O) -> Self {
        Self { driver, observer }
    }

    pub fn with_transport(transport: T, observer: O) -> Self {
        Self::new(ClusterControlPlaneDriver::new(transport), observer)
    }

    pub fn driver_mut(&mut self) -> &mut ClusterControlPlaneDriver<T> {
        &mut self.driver
    }

    pub fn observer_mut(&mut self) -> &mut O {
        &mut self.observer
    }
}

impl<T, O> ClusterMemberMutationHost for ClusterMemberMutationDriver<T, O>
where
    T: ClusterControlPlaneTransport,
    O: ClusterLocalDaemonMembershipObserver,
{
    fn prepare_cluster_state_for_kv_sync(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
    ) -> Result<(), NetworkError> {
        if let Some(observation) = self
            .observer
            .local_daemon_membership_observation(paths, state)?
        {
            refresh_local_daemon_membership_from_disk(paths, state, observation)?;
        }
        Ok(())
    }

    fn current_cluster_kv_store(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<InProcessKvStore, NetworkError> {
        ClusterMemberMutationHost::current_cluster_kv_store(&mut self.driver, paths, state)
    }

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, NetworkError> {
        ClusterMemberMutationHost::commit_cluster_kv_txn_with_quorum(
            &mut self.driver,
            paths,
            state,
            txn,
        )
    }
}

pub trait ClusterJoinReconciliationHost {
    fn send_join_registration(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterJoinRegistrationRequest,
        tcp_timeout: Duration,
        iroh_timeout: Duration,
    ) -> Result<ClusterJoinRegistrationResponse, NetworkError>;

    fn fetch_cluster_kv_snapshot(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> Result<KvSnapshot, NetworkError>;

    fn observe_join_reconciliation_connectivity(
        &mut self,
        _paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
        _reconciliation: &ClusterJoinReconciliation,
    ) -> Result<(), NetworkError> {
        Ok(())
    }
}

pub fn handle_cluster_raft_rpc_request(
    paths: &ClusterPaths,
    request: ObjectRpcRequest,
    authenticated_peer: &ClusterAuthenticatedPeer,
) -> Result<ObjectRpcResponse, NetworkError> {
    let route = request.route.clone();
    let Some(payload) = sinomo_kv::handle_cluster_raft_rpc_payload(
        request.route.as_str(),
        &request.body,
        |append| handle_locked_cluster_raft_append_request(paths, append, authenticated_peer),
        |vote| handle_locked_cluster_raft_vote_request(paths, vote, authenticated_peer),
    )?
    else {
        return Err(NetworkError::UnknownRoute { route });
    };
    Ok(ObjectRpcResponse::from_encoded_body(
        payload.route,
        payload.body,
    ))
}

pub async fn handle_cluster_raft_rpc_request_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    request: ObjectRpcRequest,
    authenticated_peer: &ClusterAuthenticatedPeer,
) -> Result<ObjectRpcResponse, NetworkError> {
    let paths = paths.clone();
    let authenticated_peer = authenticated_peer.clone();
    runtime
        .spawn_blocking(move || {
            handle_cluster_raft_rpc_request(&paths, request, &authenticated_peer)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster raft rpc".to_string(),
            detail: format!("cluster raft rpc task failed: {source}"),
        })?
}

pub fn handle_cluster_membership_rpc_request_with_hosts<E, J, M, I>(
    paths: &ClusterPaths,
    request: ObjectRpcRequest,
    signer_node_id: &str,
    signer_public_key: &str,
    now: OffsetDateTime,
    join_host: &mut J,
    member_host: &mut M,
    invite_host: &mut I,
) -> Result<ObjectRpcResponse, E>
where
    E: From<NetworkError> + ToString,
    J: ClusterJoinRegistrationHost,
    M: ClusterMemberPublishHost<Error = E>,
    I: ClusterInvitePublishHost<Error = E>,
{
    match request.route.as_str() {
        CLUSTER_JOIN_REGISTER_ROUTE => {
            let payload: ClusterJoinRegistrationRequest = request.decode_body().map_err(E::from)?;
            let response =
                handle_cluster_join_registration_request_with_host(paths, payload, now, join_host)
                    .map_err(E::from)?;
            ObjectRpcResponse::encode_body(CLUSTER_JOIN_REGISTER_ROUTE, &response).map_err(E::from)
        }
        CLUSTER_INVITE_PUBLISH_ROUTE => {
            let payload: ClusterInvitePublishRequest = request.decode_body().map_err(E::from)?;
            let response = handle_cluster_invite_publish_request_with_host(
                paths,
                payload,
                signer_node_id,
                signer_public_key,
                now,
                invite_host,
            )?;
            ObjectRpcResponse::encode_body(CLUSTER_INVITE_PUBLISH_ROUTE, &response).map_err(E::from)
        }
        CLUSTER_MEMBER_PUBLISH_ROUTE => {
            let payload: ClusterMemberPublishRequest = request.decode_body().map_err(E::from)?;
            let response = handle_cluster_member_publish_request_with_host(
                paths,
                payload,
                signer_node_id,
                member_host,
            )?;
            ObjectRpcResponse::encode_body(CLUSTER_MEMBER_PUBLISH_ROUTE, &response).map_err(E::from)
        }
        other => Err(E::from(NetworkError::UnknownRoute {
            route: other.to_string(),
        })),
    }
}

pub fn cluster_network_publish_interval_for_node(
    paths: &ClusterPaths,
    node_id: &NodeId,
) -> Duration {
    read_cluster_state_without_kv(paths)
        .ok()
        .and_then(|state| {
            state
                .members
                .iter()
                .find(|member| member.node_id == *node_id)
                .map(|member| cluster_network_publish_interval_for_role(&member.node_role))
        })
        .unwrap_or(CLUSTER_NETWORK_PUBLISH_INTERVAL)
}

pub fn select_cluster_connectivity_probe_batch(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    connectivity: &mut BTreeMap<String, ClusterConnectivityProbeState>,
    connectivity_cursor: usize,
    local_node_role: &ClusterNodeRole,
    startup_settle_active: bool,
    now: OffsetDateTime,
) -> ClusterConnectivityProbeBatch {
    let refresh_deadline =
        now + time::Duration::seconds(CLUSTER_CONNECTIVITY_REFRESH_MARGIN_SECONDS);
    let targets = ClusterEndpointSelector::new(paths, state)
        .select(ClusterEndpointSelectionPurpose::NetworkProbe);
    let active_keys = targets
        .iter()
        .map(|endpoint| cluster_connectivity_probe_key(&state.local_node_id, endpoint))
        .collect::<BTreeSet<_>>();

    // Seed the in-memory probe cache from the merged graph (KV + the local probe store,
    // local-authoritative for our own edges). A learner cannot commit its connectivity
    // edges to KV, so seeding from KV alone would discard its last-known reachability.
    if connectivity.is_empty() {
        for record in read_cluster_connectivity_graph(paths)
            .unwrap_or_default()
            .into_iter()
            .filter(|record| record.source_node_id == state.local_node_id)
        {
            // Must match `cluster_connectivity_probe_key` ("src:tgt:endpoint"),
            // not the KV key, or the seed never lands in the probe cache.
            let key = format!(
                "{}:{}:{}",
                record.source_node_id, record.target_node_id, record.endpoint_id
            );
            if active_keys.contains(&key) {
                connectivity.insert(key, ClusterConnectivityProbeState { record });
            }
        }
    }

    let graph = ClusterConnectivityGraph::read_for_local(paths, state);
    let mut probe_entries = targets
        .iter()
        .filter_map(|endpoint| {
            let key = cluster_connectivity_probe_key(&state.local_node_id, endpoint);
            let previous = connectivity.get(&key).map(|probe| probe.record.clone());
            let due = previous.as_ref().is_none_or(|record| {
                cluster_connectivity_probe_due_with_graph(
                    record,
                    endpoint,
                    now,
                    refresh_deadline,
                    local_node_role,
                    &graph,
                    &state.local_node_id,
                    startup_settle_active,
                )
            });
            due.then(|| ClusterConnectivityProbeBatchEntry {
                key,
                endpoint: endpoint.clone(),
                previous,
            })
        })
        .collect::<Vec<_>>();
    probe_entries.sort_by_key(|entry| {
        (
            cluster_connectivity_probe_expiry_rank(entry.previous.as_ref()),
            cluster_connectivity_probe_state_rank(entry.previous.as_ref()),
            entry.endpoint.node_id.clone(),
            entry.endpoint.priority,
            entry.endpoint.address.clone(),
        )
    });
    let probe_count =
        probe_entries
            .len()
            .min(cluster_connectivity_probe_max_endpoints_per_tick_for_role(
                local_node_role,
            ));
    let _ = connectivity_cursor;
    let next_cursor = 0;
    let entries = probe_entries.into_iter().take(probe_count).collect();

    ClusterConnectivityProbeBatch {
        active_keys,
        entries,
        next_cursor,
    }
}

fn cluster_connectivity_probe_expiry_rank(
    record: Option<&ClusterConnectivityRecord>,
) -> OffsetDateTime {
    record
        .map(|record| record.expires_at)
        .unwrap_or(OffsetDateTime::UNIX_EPOCH)
}

fn cluster_connectivity_probe_state_rank(record: Option<&ClusterConnectivityRecord>) -> u8 {
    match record.map(|record| &record.state) {
        None => 0,
        Some(ClusterConnectivityState::Healthy) => 1,
        Some(ClusterConnectivityState::Degraded) => 2,
        Some(ClusterConnectivityState::Unreachable) => 3,
    }
}

pub(crate) fn apply_cluster_raft_role(
    member: &mut ClusterMemberRecord,
    role: ClusterMemberRole,
    timestamp: OffsetDateTime,
) {
    sinomo_node::apply_cluster_raft_role(member, role, timestamp);
}

pub(crate) fn cluster_raft_state_for_role(role: &ClusterMemberRole) -> ClusterMemberState {
    match role {
        ClusterMemberRole::Voter => ClusterMemberState::Voter,
        ClusterMemberRole::Learner => ClusterMemberState::Learner,
        ClusterMemberRole::NonVoting => ClusterMemberState::NonVoting,
    }
}

pub fn read_cluster_state(paths: &ClusterPaths) -> Result<ClusterBootstrapState, NetworkError> {
    let mut state = read_cluster_state_without_kv(paths)?;
    let local_member = state
        .members
        .iter()
        .find(|member| member.node_id == state.local_node_id && member.state.is_authorized())
        .cloned();
    let local_remote_route_endpoints = locally_learned_remote_route_endpoints(&state);
    let kv_members = match read_cluster_members_from_kv(paths) {
        Ok(members) => members,
        Err(error) => {
            eprintln!(
                "warning: ignoring unreadable cluster kv snapshot while loading members: {error}"
            );
            Vec::new()
        }
    };
    if !kv_members.is_empty() {
        let local_process_members = state
            .members
            .iter()
            .filter(|member| {
                cluster_node_is_local_process(&state, &member.node_id)
                    && member.state.is_authorized()
            })
            .cloned()
            .collect::<Vec<_>>();
        state.members = kv_members;
        preserve_locally_learned_remote_route_endpoints(&mut state, local_remote_route_endpoints);
        for local_member in local_process_members {
            if let Some(mut kv_member) = state
                .members
                .iter()
                .find(|member| member.node_id == local_member.node_id)
                .cloned()
            {
                let mut local_member = local_member;
                for endpoint in kv_member.endpoints.drain(..) {
                    upsert_member_endpoint(&mut local_member, endpoint);
                }
                cleanup_member_iroh_endpoints(&mut local_member);
                upsert_member_record(&mut state, local_member);
            } else {
                upsert_member_record(&mut state, local_member);
            }
        }
        state
            .members
            .sort_by(|left, right| left.node_id.cmp(&right.node_id));
        cleanup_cluster_iroh_endpoints(&mut state);
        return Ok(state);
    }
    for member in kv_members {
        if member.node_id == state.local_node_id
            && let Some(mut local_member) = state
                .members
                .iter()
                .find(|candidate| candidate.node_id == member.node_id)
                .cloned()
            && local_member.state.is_authorized()
        {
            for endpoint in member.endpoints {
                upsert_member_endpoint(&mut local_member, endpoint);
            }
            cleanup_member_iroh_endpoints(&mut local_member);
            upsert_member_record(&mut state, local_member);
            continue;
        }
        merge_member_record(&mut state, member);
    }
    state
        .members
        .sort_by(|left, right| left.node_id.cmp(&right.node_id));
    if let Some(local_member) = local_member {
        upsert_member_record(&mut state, local_member);
    }
    cleanup_cluster_iroh_endpoints(&mut state);
    Ok(state)
}

pub fn read_cluster_state_cached(
    paths: &ClusterPaths,
) -> Result<ClusterBootstrapState, NetworkError> {
    let checked_at = Instant::now();
    let before_mark = cluster_state_cache_mark(paths);
    if let Some(state) = read_cluster_state_from_cache(paths, before_mark, checked_at) {
        return Ok(state);
    }
    let state = read_cluster_state(paths)?;
    if let Some(mark) = before_mark
        && cluster_state_cache_mark(paths) == Some(mark)
    {
        record_cluster_state_cache(paths, mark, state.clone(), checked_at);
    }
    Ok(state)
}

pub async fn read_cluster_state_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
) -> Result<ClusterBootstrapState, NetworkError> {
    let paths = paths.clone();
    runtime
        .spawn_blocking(move || read_cluster_state(&paths))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "read cluster state".to_string(),
            detail: format!("cluster state read task failed: {source}"),
        })?
}

pub fn write_cluster_state(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<(), NetworkError> {
    write_object_file(&paths.state, state)?;
    record_current_cluster_kv_reader_node_id_cache(paths, state.local_node_id.clone());
    Ok(())
}

pub async fn write_cluster_state_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<(), NetworkError> {
    let paths = paths.clone();
    let state = state.clone();
    runtime
        .spawn_blocking(move || write_cluster_state(&paths, &state))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "write cluster state".to_string(),
            detail: format!("cluster state write task failed: {source}"),
        })?
}

pub(crate) fn read_cluster_kv_snapshot(paths: &ClusterPaths) -> Result<KvSnapshot, NetworkError> {
    cluster_kv_local_cache(paths)
        .cached_snapshot()
        .map_err(Into::into)
}

pub(crate) fn write_cluster_kv_snapshot(
    paths: &ClusterPaths,
    snapshot: &KvSnapshot,
) -> Result<(), NetworkError> {
    let wrote =
        sinomo_kv::write_cluster_kv_materialized_snapshot_if_changed(&paths.kv_paths(), snapshot)?;
    observe_cluster_kv_snapshot_write(paths, wrote);
    Ok(())
}

pub(crate) fn write_imported_cluster_kv_snapshot_if_changed(
    paths: &ClusterPaths,
    snapshot: &KvSnapshot,
) -> Result<bool, NetworkError> {
    let wrote = sinomo_kv::write_imported_cluster_kv_snapshot_if_changed(
        &paths.kv_paths(),
        snapshot,
        cluster_kv_key_can_defer_materialized_snapshot,
    )?;
    observe_cluster_kv_snapshot_write(paths, wrote);
    Ok(wrote)
}

pub fn cluster_kv_key_can_defer_materialized_snapshot(key: &str) -> bool {
    key.starts_with("/cluster/network/connectivity/")
        || key.starts_with("/cluster/network/info/")
        || key.starts_with("/cluster/node-status/")
}

fn observe_cluster_kv_snapshot_write(paths: &ClusterPaths, wrote: bool) {
    if let Some(observer) = io_observer() {
        if wrote {
            observer.on_kv_snapshot_actual_write();
            // The snapshot is written inside sinomo-kv, so account its on-disk
            // byte size with a single stat after a confirmed write.
            if let Ok(metadata) = fs::metadata(&paths.kv_snapshot) {
                observer.on_disk_write(metadata.len());
            }
        } else {
            observer.on_kv_snapshot_skipped_write();
        }
    }
}

#[cfg(test)]
pub(crate) fn read_cluster_kv_store(
    paths: &ClusterPaths,
) -> Result<InProcessKvStore, NetworkError> {
    let node_id = read_cluster_kv_reader_node_id(paths)?;
    sinomo_kv::read_cluster_kv_store_for_node(&paths.kv_paths(), node_id).map_err(Into::into)
}

pub(crate) fn read_cluster_kv_current_snapshot(
    paths: &ClusterPaths,
) -> Result<KvSnapshot, NetworkError> {
    read_cluster_kv_current_snapshot_arc(paths).map(|snapshot| (*snapshot).clone())
}

pub(crate) fn read_cluster_kv_current_snapshot_arc(
    paths: &ClusterPaths,
) -> Result<Arc<KvSnapshot>, NetworkError> {
    #[cfg(test)]
    record_cluster_kv_current_snapshot_read_for_test(paths);

    let node_id = read_cluster_kv_reader_node_id(paths)?;
    sinomo_kv::read_cluster_kv_current_snapshot_arc_for_node(&paths.kv_paths(), node_id)
        .map_err(Into::into)
}

pub(crate) fn read_cluster_kv_current_or_cached_snapshot_arc_if_present(
    paths: &ClusterPaths,
) -> Result<Option<Arc<KvSnapshot>>, NetworkError> {
    match read_cluster_kv_current_snapshot_arc(paths) {
        Ok(snapshot) => Ok(Some(snapshot)),
        Err(NetworkError::ClusterOrphan) => read_cluster_kv_cached_snapshot_if_present(paths),
        Err(error) => Err(error),
    }
}

pub(crate) fn read_cluster_kv_latest_observed_snapshot_arc_if_present(
    paths: &ClusterPaths,
) -> Result<Option<Arc<KvSnapshot>>, NetworkError> {
    let current = match read_cluster_kv_current_snapshot_arc(paths) {
        Ok(snapshot) => Some(snapshot),
        Err(NetworkError::ClusterOrphan) => None,
        Err(error) => return Err(error),
    };
    let cached = read_cluster_kv_cached_snapshot_if_present(paths)?;
    Ok(match (current, cached) {
        (Some(current), Some(cached)) if cached.revision > current.revision => Some(cached),
        (Some(current), _) => Some(current),
        (None, cached) => cached,
    })
}

pub(crate) fn read_cluster_kv_current_or_empty_snapshot_arc(
    paths: &ClusterPaths,
) -> Result<Arc<KvSnapshot>, NetworkError> {
    read_cluster_kv_current_or_cached_snapshot_arc_if_present(paths)
        .map(|snapshot| snapshot.unwrap_or_else(|| Arc::new(InProcessKvStore::new().snapshot())))
}

pub(crate) fn read_cluster_kv_entry(
    paths: &ClusterPaths,
    key: &str,
) -> Result<Option<KvEntry>, NetworkError> {
    let snapshot = read_cluster_kv_current_snapshot_arc(paths)?;
    Ok(snapshot.get(key).cloned())
}

#[cfg(test)]
pub(crate) fn read_cluster_kv_prefix_entries(
    paths: &ClusterPaths,
    prefix: &str,
) -> Result<Vec<KvEntry>, NetworkError> {
    read_cluster_kv_prefix_entries_limited(paths, prefix, CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES)
}

#[cfg(test)]
pub(crate) fn read_cluster_kv_prefix_entries_limited(
    paths: &ClusterPaths,
    prefix: &str,
    max_entries: usize,
) -> Result<Vec<KvEntry>, NetworkError> {
    read_cluster_kv_prefix_entries_limited_by_payload_bytes(
        paths,
        prefix,
        max_entries,
        sinomo_kv::CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES,
    )
}

#[cfg(test)]
pub(crate) fn read_cluster_kv_prefix_entries_limited_by_payload_bytes(
    paths: &ClusterPaths,
    prefix: &str,
    max_entries: usize,
    max_payload_bytes: usize,
) -> Result<Vec<KvEntry>, NetworkError> {
    sinomo_kv::read_cluster_kv_prefix_entries_limited_by_payload_bytes(
        &paths.kv_paths(),
        prefix,
        max_entries,
        max_payload_bytes,
    )
    .map_err(Into::into)
}

pub(crate) fn read_cluster_kv_cached_snapshot(
    paths: &ClusterPaths,
) -> Result<Arc<KvSnapshot>, NetworkError> {
    #[cfg(test)]
    record_cluster_kv_cached_snapshot_read_for_test(paths);
    cluster_kv_local_cache(paths)
        .cached_snapshot_arc()
        .map_err(NetworkError::from)
}

pub(crate) fn read_cluster_kv_cached_snapshot_if_present(
    paths: &ClusterPaths,
) -> Result<Option<Arc<KvSnapshot>>, NetworkError> {
    let snapshot = cluster_kv_local_cache(paths)
        .cached_snapshot_arc_if_present()
        .map_err(NetworkError::from)?;
    #[cfg(test)]
    if snapshot.is_some() {
        record_cluster_kv_cached_snapshot_read_for_test(paths);
    }
    Ok(snapshot)
}

fn cluster_kv_local_cache(paths: &ClusterPaths) -> KvLocalCache {
    paths.kv_paths().local_cache()
}

fn cluster_kv_raft_store(paths: &ClusterPaths) -> sinomo_kv::ClusterKvRaftStore {
    paths.kv_paths().raft_store()
}

fn read_cluster_kv_reader_node_id(paths: &ClusterPaths) -> Result<NodeId, NetworkError> {
    if let Some(mark) = cluster_file_mark(&paths.state)
        && mark.exists
        && let Some(node_id) = cached_cluster_kv_reader_node_id(&paths.state, mark)
    {
        return Ok(node_id);
    }
    match read_cluster_state_without_kv(paths) {
        Ok(state) => {
            let node_id = state.local_node_id;
            record_current_cluster_kv_reader_node_id_cache(paths, node_id.clone());
            Ok(node_id)
        }
        Err(_) if !paths.kv_raft.exists() => {
            Ok(NodeId::from_legacy_named("cluster_kv_cache_reader")
                .expect("static cache reader node id"))
        }
        Err(error) => Err(error),
    }
}

#[cfg(test)]
pub(crate) fn cluster_raft_wal_paths(paths: &ClusterPaths, node_id: &NodeId) -> KvWalPaths {
    cluster_kv_raft_store(paths).wal_paths(node_id)
}

pub(crate) fn open_cluster_raft_kv_node(
    paths: &ClusterPaths,
    node_id: &NodeId,
) -> Result<DurableRaftKvNode, NetworkError> {
    cluster_kv_raft_store(paths)
        .open_node(node_id)
        .map_err(Into::into)
}

#[cfg(test)]
pub(crate) fn restore_cluster_raft_from_snapshot_if_newer(
    paths: &ClusterPaths,
    node_id: &NodeId,
    snapshot: KvSnapshot,
) -> Result<bool, NetworkError> {
    cluster_kv_raft_store(paths)
        .restore_from_snapshot_if_newer_and_notify(node_id, snapshot)
        .map_err(NetworkError::from)
}

pub(crate) fn locked_restore_cluster_raft_from_snapshot_if_newer(
    paths: &ClusterPaths,
    node_id: &NodeId,
    snapshot: KvSnapshot,
) -> Result<bool, NetworkError> {
    cluster_kv_raft_store(paths)
        .locked_restore_from_snapshot_if_newer_and_notify(node_id, snapshot)
        .map_err(NetworkError::from)
}

#[cfg(test)]
pub(crate) async fn open_cluster_raft_kv_node_async(
    paths: &ClusterPaths,
    node_id: &NodeId,
) -> Result<DurableRaftKvNode, NetworkError> {
    cluster_kv_raft_store(paths)
        .open_node_async(node_id)
        .await
        .map_err(Into::into)
}

pub(crate) fn append_cluster_raft_command(
    paths: &ClusterPaths,
    node_id: &NodeId,
    command: KvCommand,
) -> Result<KvSnapshot, NetworkError> {
    append_cluster_raft_command_with_response(paths, node_id, command).map(|(snapshot, _)| snapshot)
}

pub(crate) fn append_cluster_raft_command_with_response(
    paths: &ClusterPaths,
    node_id: &NodeId,
    command: KvCommand,
) -> Result<(KvSnapshot, Option<KvTxnResponse>), NetworkError> {
    let store = cluster_kv_raft_store(paths);
    let (result, wrote) = store
        .append_command_with_response_and_publish_report(
            node_id,
            command,
            cluster_kv_key_can_defer_materialized_snapshot,
        )
        .map_err(NetworkError::from)?;
    observe_cluster_kv_snapshot_write(paths, wrote);
    Ok((result.snapshot, result.txn_response))
}

#[cfg(test)]
pub(crate) fn append_cluster_raft_command_uncommitted(
    paths: &ClusterPaths,
    node_id: &NodeId,
    command: KvCommand,
) -> Result<KvLogEntry, NetworkError> {
    append_cluster_raft_command_uncommitted_entry(paths, node_id, command)
}

pub(crate) async fn append_cluster_raft_command_uncommitted_or_no_effect_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    node_id: &NodeId,
    command: KvCommand,
) -> Result<ClusterKvRaftUncommittedAppend, NetworkError> {
    let paths = paths.clone();
    let node_id = node_id.clone();
    runtime
        .spawn_blocking(move || {
            LocalClusterRaftCommandStorage::new(&paths, node_id)
                .append_uncommitted_or_no_effect(command)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster raft append uncommitted command".to_string(),
            detail: format!("cluster raft append uncommitted command task failed: {source}"),
        })?
}

#[cfg(test)]
fn append_cluster_raft_command_uncommitted_entry(
    paths: &ClusterPaths,
    node_id: &NodeId,
    command: KvCommand,
) -> Result<KvLogEntry, NetworkError> {
    cluster_kv_raft_store(paths)
        .append_command_uncommitted(node_id, command)
        .map_err(Into::into)
}

#[cfg(test)]
fn cluster_member_put_op(member: &ClusterMemberRecord) -> Result<KvOp, NetworkError> {
    KvOp::put_serde(cluster_member_key(&member.node_id), member, None).map_err(NetworkError::from)
}

#[cfg(test)]
pub fn cluster_member_put_command(member: &ClusterMemberRecord) -> Result<KvCommand, NetworkError> {
    cluster_member_put_op(member).map(KvCommand::Op)
}

pub async fn discard_cluster_raft_uncommitted_tail_and_clear_leader_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    node_id: &NodeId,
) -> Result<KvSnapshot, NetworkError> {
    let paths = paths.clone();
    let node_id = node_id.clone();
    runtime
        .spawn_blocking(move || {
            LocalClusterRaftCommandStorage::new(&paths, node_id)
                .discard_uncommitted_tail_and_clear_leader()
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster raft discard uncommitted tail".to_string(),
            detail: format!("cluster raft discard uncommitted tail task failed: {source}"),
        })?
}

pub async fn commit_cluster_raft_index_with_events_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    node_id: &NodeId,
    commit_index: u64,
) -> Result<ClusterKvRaftCommitResult, NetworkError> {
    let paths = paths.clone();
    let node_id = node_id.clone();
    runtime
        .spawn_blocking(move || {
            LocalClusterRaftCommandStorage::new(&paths, node_id)
                .commit_index_with_events(commit_index)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster raft commit index".to_string(),
            detail: format!("cluster raft commit index task failed: {source}"),
        })?
}

pub(crate) fn apply_cluster_raft_append(
    paths: &ClusterPaths,
    node_id: &NodeId,
    snapshot: Option<RaftSnapshot>,
    append: RaftAppendEntriesRequest,
) -> Result<RaftAppendEntriesResponse, NetworkError> {
    let (outcome, wrote) = cluster_kv_raft_store(paths)
        .apply_append_entries_with_optional_snapshot_and_publish(
            node_id,
            snapshot,
            append,
            cluster_kv_key_can_defer_materialized_snapshot,
        )?;
    observe_cluster_kv_snapshot_write(paths, wrote);
    Ok(outcome.response)
}

pub(crate) fn locked_apply_cluster_raft_vote(
    paths: &ClusterPaths,
    node_id: &NodeId,
    vote: RaftRequestVoteRequest,
) -> Result<RaftRequestVoteResponse, NetworkError> {
    cluster_kv_raft_store(paths)
        .locked_apply_vote(node_id, vote)
        .map_err(Into::into)
}

pub(crate) fn handle_cluster_raft_append_request(
    paths: &ClusterPaths,
    request: ClusterRaftAppendRequest,
    authenticated_peer: &ClusterAuthenticatedPeer,
) -> Result<ClusterRaftAppendResponse, NetworkError> {
    let mut state = read_cluster_state(paths)?;
    if request.state.cluster_id != state.cluster_id {
        return cluster_raft_append_rejection(
            paths,
            &state,
            format!(
                "peer cluster `{}` does not match local cluster `{}`",
                request.state.cluster_id, state.cluster_id
            ),
        );
    }
    let leader_authorized_by_local_state =
        sinomo_kv::cluster_kv_raft_node_is_voter(&state, &request.append.leader_id)
            && cluster_authenticated_peer_matches_node(
                &state,
                authenticated_peer,
                &request.append.leader_id,
            );
    let leader_authorized_for_pending_join = cluster_state_needs_join_registration(&state)
        && request.append.leader_id == request.state.local_node_id
        && sinomo_kv::cluster_kv_raft_node_is_voter(&request.state, &request.append.leader_id)
        && cluster_authenticated_peer_matches_node(
            &request.state,
            authenticated_peer,
            &request.append.leader_id,
        );
    if !leader_authorized_by_local_state && !leader_authorized_for_pending_join {
        return cluster_raft_append_rejection(
            paths,
            &state,
            format!(
                "raft append rejected from unauthorized or mismatched leader {}",
                request.append.leader_id
            ),
        );
    }
    merge_cluster_state_from_peer(&mut state, request.state);
    write_cluster_state(paths, &state)?;
    let append = apply_cluster_raft_append(
        paths,
        &state.local_node_id,
        request.snapshot,
        request.append,
    )?;
    Ok(ClusterRaftAppendResponse {
        accepted: append.success,
        detail: if append.success {
            "raft append applied".to_string()
        } else {
            "raft append rejected by local log".to_string()
        },
        state,
        append,
    })
}

pub(crate) fn handle_locked_cluster_raft_append_request(
    paths: &ClusterPaths,
    request: ClusterRaftAppendRequest,
    authenticated_peer: &ClusterAuthenticatedPeer,
) -> Result<ClusterRaftAppendResponse, NetworkError> {
    let _raft_guard = sinomo_kv::lock_cluster_kv_raft_operation_for_paths(&paths.kv_paths())?;
    handle_cluster_raft_append_request(paths, request, authenticated_peer)
}

fn cluster_raft_append_rejection(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    detail: String,
) -> Result<ClusterRaftAppendResponse, NetworkError> {
    Ok(ClusterRaftAppendResponse {
        accepted: false,
        detail,
        state: state.clone(),
        append: cluster_kv_raft_store(paths)
            .append_entries_rejection_response(&state.local_node_id)?,
    })
}

pub(crate) fn handle_cluster_raft_vote_request(
    paths: &ClusterPaths,
    request: ClusterRaftVoteRequest,
    authenticated_peer: &ClusterAuthenticatedPeer,
) -> Result<ClusterRaftVoteResponse, NetworkError> {
    let mut state = read_cluster_state(paths)?;
    if request.state.cluster_id != state.cluster_id {
        return cluster_kv_raft_store(paths)
            .cluster_raft_vote_rejection_response(
                &state,
                format!(
                    "peer cluster `{}` does not match local cluster `{}`",
                    request.state.cluster_id, state.cluster_id
                ),
            )
            .map_err(Into::into);
    }
    let candidate_is_authorized_voter = sinomo_kv::cluster_kv_raft_candidate_is_authorized_voter(
        &state,
        &request.vote.candidate_id,
    );
    if !candidate_is_authorized_voter
        || !cluster_authenticated_peer_matches_node(
            &state,
            authenticated_peer,
            &request.vote.candidate_id,
        )
    {
        return cluster_kv_raft_store(paths)
            .cluster_raft_vote_rejection_response(
                &state,
                format!(
                    "raft vote rejected for unauthorized or mismatched candidate {}",
                    request.vote.candidate_id
                ),
            )
            .map_err(Into::into);
    }
    merge_cluster_state_from_peer(&mut state, request.state);
    write_cluster_state(paths, &state)?;
    cluster_kv_raft_store(paths)
        .apply_vote_response(&state, request.vote)
        .map_err(Into::into)
}

pub(crate) fn handle_locked_cluster_raft_vote_request(
    paths: &ClusterPaths,
    request: ClusterRaftVoteRequest,
    authenticated_peer: &ClusterAuthenticatedPeer,
) -> Result<ClusterRaftVoteResponse, NetworkError> {
    let _raft_guard = sinomo_kv::lock_cluster_kv_raft_operation_for_paths(&paths.kv_paths())?;
    handle_cluster_raft_vote_request(paths, request, authenticated_peer)
}

pub fn cluster_authenticated_peer_matches_node(
    state: &ClusterBootstrapState,
    authenticated_peer: &ClusterAuthenticatedPeer,
    node_id: &NodeId,
) -> bool {
    state.members.iter().any(|member| {
        member.node_id == *node_id
            && member.cluster_id == state.cluster_id
            && member.state.is_authorized()
            && member.node_id.as_str() == authenticated_peer.node_id
            && member.node_public_key == authenticated_peer.public_key
    })
}

pub(crate) fn locked_observe_cluster_raft_term(
    paths: &ClusterPaths,
    node_id: &NodeId,
    term: u64,
) -> Result<bool, NetworkError> {
    cluster_kv_raft_store(paths)
        .locked_observe_term_and_write_snapshot_if_changed(node_id, term)
        .map_err(Into::into)
}

pub(crate) fn locked_set_cluster_raft_leader(
    paths: &ClusterPaths,
    node_id: &NodeId,
    term: u64,
) -> Result<KvSnapshot, NetworkError> {
    cluster_kv_raft_store(paths)
        .locked_set_leader_and_write_snapshot_if_changed(node_id, term)
        .map_err(Into::into)
}

pub(crate) fn locked_cluster_raft_local_vote_metadata(
    paths: &ClusterPaths,
    node_id: &NodeId,
) -> Result<ClusterKvRaftLocalVoteMetadata, NetworkError> {
    cluster_kv_raft_store(paths)
        .locked_local_vote_metadata(node_id)
        .map_err(Into::into)
}

pub(crate) fn locked_cluster_raft_known_leader_id(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Option<NodeId>, NetworkError> {
    cluster_kv_raft_store(paths)
        .locked_known_voter_leader_id(state)
        .map_err(Into::into)
}

pub(crate) fn locked_cluster_raft_local_is_leader(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<bool, NetworkError> {
    cluster_kv_raft_store(paths)
        .locked_local_is_leader(state)
        .map_err(Into::into)
}

#[cfg(test)]
pub fn create_cluster_kv_watch(
    paths: &ClusterPaths,
    request: ClusterKvWatchPrefixRequest,
) -> Result<ClusterKvWatchRegistration, NetworkError> {
    let snapshot = read_cluster_kv_current_snapshot_arc(paths)?;
    let allocator_trusted = cluster_kv_watch_allocator_trusted(paths);
    let report = sinomo_kv::create_cluster_kv_watch_registration(
        &paths.kv_paths(),
        request,
        snapshot.compacted_revision,
        allocator_trusted,
        sinomo_kv::MAX_CLUSTER_KV_ACTIVE_WATCHES,
    )?;
    if let Some(scan) = report.scan.as_ref() {
        record_cluster_kv_watch_scan_observation(paths, scan);
    }
    if let Err(error) = persist_cluster_kv_watch_allocator_state(paths, &report.allocator) {
        let _ = sinomo_kv::remove_cluster_kv_watch_registration_file_if_exists(&report.watch_path);
        invalidate_cluster_kv_watch_registration_cache(&report.watch_path);
        return Err(error);
    }
    record_current_cluster_kv_watch_registration_cache(
        &report.watch_path,
        report.registration.clone(),
    );
    Ok(report.registration)
}

#[cfg(test)]
pub fn drain_cluster_kv_watch(
    paths: &ClusterPaths,
    request: ClusterKvWatchDrainRequest,
) -> Result<ClusterKvWatchDrainResponse, NetworkError> {
    let wait_timeout = request.wait_timeout();
    if wait_timeout.is_zero() {
        return drain_cluster_kv_watch_once_limited_by_payload_bytes(
            paths,
            request.watch_id,
            sinomo_kv::MAX_CLUSTER_KV_WATCH_DRAIN_PAYLOAD_BYTES,
        );
    }
    let Some(deadline) = Instant::now().checked_add(wait_timeout) else {
        return drain_cluster_kv_watch_once_limited_by_payload_bytes(
            paths,
            request.watch_id,
            sinomo_kv::MAX_CLUSTER_KV_WATCH_DRAIN_PAYLOAD_BYTES,
        );
    };
    loop {
        let observed_generation = cluster_kv_watch_observed_generation(paths, request.watch_id);
        let response = drain_cluster_kv_watch_once_limited_by_payload_bytes(
            paths,
            request.watch_id,
            sinomo_kv::MAX_CLUSTER_KV_WATCH_DRAIN_PAYLOAD_BYTES,
        )?;
        if !response.events.is_empty() {
            return Ok(response);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(response);
        }
        if !sinomo_kv::wait_for_cluster_kv_watch_change_for_watch(
            &paths.kv_paths(),
            request.watch_id,
            observed_generation,
            remaining,
        ) {
            return Ok(response);
        }
    }
}

#[cfg(test)]
fn cached_cluster_kv_watch_empty_drain_response(
    paths: &ClusterPaths,
    watch_path: &PathBuf,
    watch_id: WatchId,
    observed_generation: sinomo_kv::ClusterKvWatchObservedGeneration,
) -> Option<ClusterKvWatchDrainResponse> {
    let mark = cluster_file_mark(watch_path)?;
    if !mark.exists {
        return None;
    }
    let entry = cached_cluster_kv_watch_registration_entry(watch_path, mark)?;
    if entry.registration.watch_id != watch_id {
        return None;
    }
    let current_mark = cluster_kv_watch_empty_drain_mark(paths, observed_generation)?;
    if entry.empty_drain_mark != Some(current_mark) {
        return None;
    }
    Some(ClusterKvWatchDrainResponse {
        watch_id: entry.registration.watch_id,
        events: Vec::new(),
        next_revision: entry.registration.next_revision,
    })
}

#[cfg(test)]
pub(crate) fn drain_cluster_kv_watch_once_limited_by_payload_bytes(
    paths: &ClusterPaths,
    watch_id: WatchId,
    max_payload_bytes: usize,
) -> Result<ClusterKvWatchDrainResponse, NetworkError> {
    #[cfg(test)]
    record_cluster_kv_watch_drain_once_for_test(paths);

    let watch_path = cluster_kv_watch_path(paths, watch_id);
    let observed_generation = cluster_kv_watch_observed_generation(paths, watch_id);
    if let Some(response) = cached_cluster_kv_watch_empty_drain_response(
        paths,
        &watch_path,
        watch_id,
        observed_generation,
    ) {
        return Ok(response);
    }
    let empty_drain_mark_before_read =
        cluster_kv_watch_empty_drain_mark(paths, observed_generation);
    let registration_read = read_cluster_kv_watch_registration_with_cursor(&watch_path, watch_id)?;
    let snapshot = read_cluster_kv_current_snapshot_arc(paths)?;
    let persisted_events = cluster_kv_watch_events_arc(paths)?;
    let (registration_file_unchanged, registration_file_age) =
        cluster_kv_watch_registration_cursor_observation(&watch_path, &registration_read);
    let report = match sinomo_kv::drain_cluster_kv_watch_registration(
        registration_read.registration.clone(),
        registration_read.durable_next_revision,
        persisted_events.as_ref(),
        &snapshot,
        max_payload_bytes,
        registration_file_unchanged,
        registration_file_age,
    ) {
        Ok(report) => report,
        Err(error @ KvError::Compacted { .. }) => {
            remove_cluster_kv_watch_registration_with_allocator(paths, watch_id)?;
            return Err(NetworkError::Kv(error));
        }
        Err(error) => return Err(error.into()),
    };
    if !report.response.events.is_empty() {
        update_cluster_kv_watch_registration_after_drain(
            paths,
            &watch_path,
            &registration_read,
            &report,
        )?;
    } else if let Some(empty_drain_mark) = empty_drain_mark_before_read
        && cluster_kv_watch_empty_drain_mark(paths, observed_generation) == Some(empty_drain_mark)
    {
        record_cluster_kv_watch_registration_empty_drain_cache(
            &watch_path,
            &registration_read,
            report.registration.clone(),
            empty_drain_mark,
        );
    }
    Ok(report.response)
}

#[cfg(test)]
fn update_cluster_kv_watch_registration_after_drain(
    paths: &ClusterPaths,
    watch_path: &PathBuf,
    registration_read: &ClusterKvWatchRegistrationRead,
    report: &sinomo_kv::ClusterKvWatchDrainRegistrationReport,
) -> Result<(), NetworkError> {
    let persistence = sinomo_kv::persist_cluster_kv_watch_drain_cursor(watch_path, report)?;
    match persistence {
        sinomo_kv::ClusterKvWatchDrainCursorPersistence::Persisted { registration } => {
            record_current_cluster_kv_watch_registration_cache(watch_path, registration.clone());
        }
        sinomo_kv::ClusterKvWatchDrainCursorPersistence::CacheOnly {
            registration,
            durable_next_revision,
        } => {
            record_cluster_kv_watch_registration_cache_with_durable(
                watch_path,
                registration_read.mark,
                registration.clone(),
                durable_next_revision,
            );
        }
        sinomo_kv::ClusterKvWatchDrainCursorPersistence::Unchanged { .. } => {}
    };
    refresh_cached_cluster_kv_active_watch_registration(paths, &report.registration);
    Ok(())
}

#[cfg(test)]
fn cluster_kv_watch_registration_cursor_observation(
    watch_path: &PathBuf,
    registration_read: &ClusterKvWatchRegistrationRead,
) -> (bool, Option<Duration>) {
    let current_mark = cluster_file_mark(watch_path);
    let registration_file_unchanged = current_mark
        .as_ref()
        .is_some_and(|mark| *mark == registration_read.mark);
    let registration_file_age = current_mark
        .and_then(|mark| mark.modified)
        .and_then(|modified| SystemTime::now().duration_since(modified).ok());
    (registration_file_unchanged, registration_file_age)
}

pub fn handle_cluster_kv_watch_events(
    paths: &ClusterPaths,
    request: ClusterKvWatchEventsRequest,
) -> Result<ClusterKvWatchEventsResponse, NetworkError> {
    let state = read_cluster_state(paths)?;
    sinomo_kv::handle_cluster_kv_watch_events(&paths.kv_paths(), &state, request)
        .map_err(Into::into)
}

#[cfg(test)]
pub fn cancel_cluster_kv_watch(
    paths: &ClusterPaths,
    request: ClusterKvWatchCancelRequest,
) -> Result<ClusterKvWatchCancelResponse, NetworkError> {
    let removed = remove_cluster_kv_watch_registration_with_allocator(paths, request.watch_id)?;
    if removed {
        sinomo_kv::notify_cluster_kv_watchers(&paths.kv_paths());
    }
    Ok(ClusterKvWatchCancelResponse {
        watch_id: request.watch_id,
        cancelled: removed,
    })
}

#[cfg(test)]
pub fn append_cluster_kv_watch_events(
    paths: &ClusterPaths,
    incoming: &[KvEvent],
) -> Result<usize, NetworkError> {
    if incoming.is_empty() {
        return Ok(0);
    }
    let append_lock = cluster_kv_watch_event_append_lock(paths);
    let _append_guard = append_lock.lock().map_err(|source| NetworkError::Io {
        operation: "cluster kv watch event append".to_string(),
        detail: format!("append mutex poisoned: {source}"),
    })?;
    if cached_cluster_kv_watch_events_contain_all(paths, incoming) {
        return Ok(0);
    }
    if cached_cluster_kv_snapshot_events_contain_all(paths, incoming) {
        return Ok(0);
    }
    if persisted_cluster_kv_watch_events_contain_all(paths, incoming)? {
        return Ok(0);
    }
    let cache = cluster_kv_watch_event_append_cache_view(paths);
    let report = sinomo_kv::append_cluster_kv_watch_events_to_storage_with_observers(
        &paths.kv_paths(),
        incoming,
        cache.as_ref(),
        |read| observe_cluster_kv_watch_event_log_read(paths, read),
        |written| observe_cluster_kv_watch_event_log_write(paths, written),
    )?;
    record_current_cluster_kv_watch_event_cache(
        paths,
        report.retained,
        report.log_records,
        report.append_safe,
    );
    if report.appended.is_empty() {
        return Ok(0);
    }
    notify_cluster_kv_watchers_for_events(paths, &report.appended);
    Ok(report.appended.len())
}

#[cfg(test)]
pub fn append_cluster_kv_watch_events_for_active_watches(
    paths: &ClusterPaths,
    incoming: &[KvEvent],
) -> Result<usize, NetworkError> {
    if incoming.is_empty() || (!paths.kv_watches.exists() && !paths.kv_watches_next_id.exists()) {
        return Ok(0);
    }
    let snapshot = read_cluster_kv_current_snapshot_arc(paths)?;
    let registrations =
        if let Some(registrations) = cached_cluster_kv_active_watch_registrations(paths) {
            registrations
        } else {
            let active = sinomo_kv::cluster_kv_active_watch_registration_report(
                &paths.kv_paths(),
                snapshot.compacted_revision,
            )?;
            if let Some(scan) = active.scan.as_ref() {
                record_cluster_kv_watch_scan_observation(paths, scan);
            }
            if let Some(allocator) = active.allocator.as_ref() {
                let registrations = active
                    .scan
                    .as_ref()
                    .map(|scan| {
                        scan.retained_registrations
                            .iter()
                            .map(|retained| retained.registration.clone())
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                persist_cluster_kv_watch_allocator_state(paths, allocator)?;
                record_current_cluster_kv_watch_allocator_trust_with_active_registrations(
                    paths,
                    allocator.active_watch_count,
                    Some(registrations.clone()),
                );
                registrations
            } else {
                Vec::new()
            }
        };
    if registrations.is_empty() {
        return Ok(0);
    }
    let incoming =
        sinomo_kv::cluster_kv_watch_events_matching_registrations(&registrations, incoming);
    if incoming.is_empty()
        || incoming
            .iter()
            .all(|event| sinomo_kv::cluster_kv_events_contain(&snapshot.events, event))
    {
        return Ok(0);
    }
    append_cluster_kv_watch_events(paths, &incoming)
}

#[cfg(test)]
fn persisted_cluster_kv_watch_events_contain_all(
    paths: &ClusterPaths,
    incoming: &[KvEvent],
) -> Result<bool, NetworkError> {
    let Some(log_mark) = cluster_file_mark(&paths.kv_watch_events_log) else {
        return Ok(false);
    };
    if log_mark.exists {
        return Ok(false);
    }
    let events = cluster_kv_watch_events_arc(paths)?;
    let still_no_log =
        cluster_file_mark(&paths.kv_watch_events_log).is_some_and(|mark| !mark.exists);
    Ok(still_no_log
        && incoming
            .iter()
            .all(|event| sinomo_kv::cluster_kv_events_contain(events.as_ref(), event)))
}

#[cfg(test)]
pub(crate) fn cached_cluster_kv_watch_events_contain_all(
    paths: &ClusterPaths,
    incoming: &[KvEvent],
) -> bool {
    let Some((checkpoint, log)) = cluster_kv_watch_event_cache_marks(paths) else {
        return false;
    };
    let Some(entry) = cached_cluster_kv_watch_event_cache_entry(paths, checkpoint, log) else {
        return false;
    };
    if !entry.append_safe {
        return false;
    }
    incoming
        .iter()
        .all(|event| sinomo_kv::cluster_kv_events_contain(entry.events.as_ref(), event))
}

#[cfg(test)]
fn cached_cluster_kv_snapshot_events_contain_all(
    paths: &ClusterPaths,
    incoming: &[KvEvent],
) -> bool {
    read_cluster_kv_cached_snapshot_if_present(paths).is_ok_and(|snapshot| {
        snapshot.is_some_and(|snapshot| {
            incoming
                .iter()
                .all(|event| sinomo_kv::cluster_kv_events_contain(&snapshot.events, event))
        })
    })
}

#[cfg(test)]
pub fn cluster_kv_watch_events(paths: &ClusterPaths) -> Result<Vec<KvEvent>, NetworkError> {
    cluster_kv_watch_events_arc(paths).map(|events| events.as_ref().clone())
}

#[cfg(test)]
fn cluster_kv_watch_events_arc(paths: &ClusterPaths) -> Result<Arc<Vec<KvEvent>>, NetworkError> {
    let cache_marks = cluster_kv_watch_event_cache_marks(paths);
    if let Some((checkpoint_mark, log_mark)) = cache_marks {
        if let Some(events) = cached_cluster_kv_watch_events_arc(paths, checkpoint_mark, log_mark) {
            return Ok(events);
        }
    }
    let read = sinomo_kv::read_cluster_kv_watch_events_with_observer(&paths.kv_paths(), |read| {
        observe_cluster_kv_watch_event_log_read(paths, read)
    });
    let read = read?;
    let log_records = read.log_records;
    let append_safe = read.append_safe;
    let events = Arc::new(read.events);
    if let Some((checkpoint_mark, log_mark)) = cache_marks {
        record_cluster_kv_watch_event_cache_arc(
            paths,
            checkpoint_mark,
            log_mark,
            events.clone(),
            log_records,
            append_safe,
        );
    }
    Ok(events)
}

#[cfg(test)]
fn observe_cluster_kv_watch_event_log_read(_paths: &ClusterPaths, read: u64) {
    #[cfg(test)]
    record_cluster_kv_watch_event_log_read_for_test(_paths, read);
    if let Some(observer) = io_observer() {
        observer.on_disk_read(read);
    }
}

#[cfg(test)]
fn observe_cluster_kv_watch_event_log_write(_paths: &ClusterPaths, written: u64) {
    #[cfg(test)]
    record_cluster_kv_watch_event_log_write_for_test(_paths, written);
    if let Some(observer) = io_observer() {
        observer.on_disk_write(written);
    }
}

#[cfg(test)]
fn cluster_kv_watch_path(paths: &ClusterPaths, watch_id: WatchId) -> PathBuf {
    paths.kv_paths().watch_path(watch_id)
}

#[cfg(test)]
struct ClusterKvWatchRegistrationRead {
    mark: ClusterFileMark,
    registration: ClusterKvWatchRegistration,
    durable_next_revision: KvRevision,
}

#[cfg(test)]
fn read_cluster_kv_watch_registration_with_cursor(
    path: &PathBuf,
    watch_id: WatchId,
) -> Result<ClusterKvWatchRegistrationRead, NetworkError> {
    let Some(mark) = cluster_file_mark(path) else {
        return read_cluster_kv_watch_registration_from_file(path, watch_id).map(|registration| {
            ClusterKvWatchRegistrationRead {
                mark: ClusterFileMark {
                    exists: true,
                    len: 0,
                    modified: None,
                    ino: 0,
                },
                durable_next_revision: registration.next_revision,
                registration,
            }
        });
    };
    if !mark.exists {
        return Err(NetworkError::Kv(KvError::UnknownWatch { watch_id }));
    }
    if let Some(entry) = cached_cluster_kv_watch_registration_entry(path, mark) {
        if entry.registration.watch_id == watch_id {
            return Ok(ClusterKvWatchRegistrationRead {
                mark,
                registration: entry.registration,
                durable_next_revision: entry.durable_next_revision,
            });
        }
        invalidate_cluster_kv_watch_registration_cache(path);
        remove_cluster_kv_watch_file(path);
        return Err(NetworkError::Kv(KvError::UnknownWatch { watch_id }));
    }
    read_cluster_kv_watch_registration_from_file(path, watch_id).map(|registration| {
        ClusterKvWatchRegistrationRead {
            mark,
            durable_next_revision: registration.next_revision,
            registration,
        }
    })
}

#[cfg(test)]
fn read_cluster_kv_watch_registration_from_file(
    path: &PathBuf,
    watch_id: WatchId,
) -> Result<ClusterKvWatchRegistration, NetworkError> {
    let registration = match sinomo_kv::read_cluster_kv_watch_registration_file(path, watch_id) {
        Ok(registration) => registration,
        Err(KvError::UnknownWatch { watch_id }) => {
            remove_cluster_kv_watch_file(path);
            return Err(NetworkError::Kv(KvError::UnknownWatch { watch_id }));
        }
        Err(error) => return Err(error.into()),
    };
    record_current_cluster_kv_watch_registration_cache(path, registration.clone());
    Ok(registration)
}

#[cfg(test)]
fn remove_cluster_kv_watch_file(path: &PathBuf) {
    if let Err(error) = remove_cluster_kv_watch_file_if_exists(path) {
        eprintln!(
            "warning: could not remove stale cluster kv watch registration {}: {}",
            path.display(),
            error
        );
    }
}

#[cfg(test)]
fn remove_cluster_kv_watch_file_if_exists(path: &PathBuf) -> Result<bool, NetworkError> {
    let removed = sinomo_kv::remove_cluster_kv_watch_registration_file_if_exists(path)?;
    invalidate_cluster_kv_watch_registration_cache(path);
    Ok(removed)
}

#[cfg(test)]
fn remove_cluster_kv_watch_registration_with_allocator(
    paths: &ClusterPaths,
    watch_id: WatchId,
) -> Result<bool, NetworkError> {
    let report = sinomo_kv::remove_cluster_kv_watch_registration_with_allocator(
        &paths.kv_paths(),
        watch_id,
    )?;
    invalidate_cluster_kv_watch_registration_cache(&report.watch_path);
    if let Some(allocator) = report.allocator {
        persist_cluster_kv_watch_allocator_state(paths, &allocator)?;
    }
    Ok(report.removed)
}

#[cfg(test)]
fn persist_cluster_kv_watch_allocator_state(
    paths: &ClusterPaths,
    state: &sinomo_kv::ClusterKvWatchAllocatorState,
) -> Result<(), NetworkError> {
    #[cfg(test)]
    if consume_cluster_kv_watch_allocator_persist_failure_for_test(paths) {
        return Err(NetworkError::StorageIo {
            path: paths.kv_watches_next_id.display().to_string(),
            detail: "injected cluster kv watch allocator persist failure".to_string(),
        });
    }
    sinomo_kv::write_cluster_kv_watch_allocator_state(&paths.kv_paths(), state)?;
    record_current_cluster_kv_watch_allocator_trust(paths, state.active_watch_count);
    Ok(())
}

#[cfg(test)]
fn record_cluster_kv_watch_scan_observation(
    paths: &ClusterPaths,
    scan: &sinomo_kv::ClusterKvWatchScanResult,
) {
    if scan.scanned {
        record_cluster_kv_watch_dir_scan_for_test(paths);
    }
    for removed_path in &scan.removed_paths {
        invalidate_cluster_kv_watch_registration_cache(removed_path);
    }
    for retained in &scan.retained_registrations {
        record_current_cluster_kv_watch_registration_cache(
            &retained.path,
            retained.registration.clone(),
        );
    }
}

pub fn read_cluster_state_without_kv(
    paths: &ClusterPaths,
) -> Result<ClusterBootstrapState, NetworkError> {
    read_optional_object_file(&paths.state)?.ok_or(NetworkError::ClusterOrphan)
}

pub async fn read_cluster_state_without_kv_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
) -> Result<ClusterBootstrapState, NetworkError> {
    let paths = paths.clone();
    runtime
        .spawn_blocking(move || read_cluster_state_without_kv(&paths))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "read local cluster state".to_string(),
            detail: format!("local cluster state read task failed: {source}"),
        })?
}

/// Mirrors a cluster state into the local durable Raft store without proving
/// replication to a quorum.
///
/// Runtime cluster-control writes should go through the CLI quorum wrapper.
/// This helper remains public for bootstrapping, recovery tooling, and tests
/// that need to seed a local store deliberately.
pub fn sync_cluster_state_to_local_raft_unchecked(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<KvSnapshot, NetworkError> {
    let initial_snapshot = open_cluster_raft_kv_node(paths, &state.local_node_id)
        .map(|node| node.store().snapshot())
        .or_else(|error| {
            eprintln!("warning: replacing unreadable cluster kv raft state: {error}");
            write_cluster_kv_snapshot(paths, &InProcessKvStore::new().snapshot())?;
            Ok::<KvSnapshot, NetworkError>(InProcessKvStore::new().snapshot())
        })?;
    let store = InProcessKvStore::restore(initial_snapshot.clone());
    let txn = cluster_state_sync_txn_from_store(&store, state, false)?;
    if txn.success.is_empty() {
        return Ok(initial_snapshot);
    }
    append_cluster_raft_command(paths, &state.local_node_id, KvCommand::Txn(txn))
}

pub fn cluster_state_sync_txn(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<KvTxn, NetworkError> {
    cluster_state_sync_txn_with_current_snapshot(paths, state).map(|(_, txn)| txn)
}

pub fn sync_cluster_state_to_kv_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    host: &mut impl ClusterMemberMutationHost,
) -> Result<KvSnapshot, NetworkError> {
    let mut keyed_state = state.clone();
    host.prepare_cluster_state_for_kv_sync(paths, &mut keyed_state)?;
    cleanup_cluster_iroh_endpoints(&mut keyed_state);
    let (current_snapshot, txn) =
        cluster_state_sync_txn_with_current_snapshot(paths, &keyed_state)?;
    if txn.success.is_empty() {
        return Ok(current_snapshot);
    }
    let mut commit_state = keyed_state;
    host.commit_cluster_kv_txn_with_quorum(paths, &mut commit_state, txn)
}

pub async fn sync_cluster_state_to_kv_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    host: &mut impl ClusterMemberMutationAsyncHost,
) -> Result<KvSnapshot, NetworkError> {
    let mut keyed_state = state.clone();
    host.prepare_cluster_state_for_kv_sync_async(paths, &mut keyed_state)
        .await?;
    cleanup_cluster_iroh_endpoints(&mut keyed_state);
    let paths_for_txn = paths.clone();
    let keyed_state_for_txn = keyed_state.clone();
    let (current_snapshot, txn) = runtime
        .spawn_blocking(move || {
            cluster_state_sync_txn_with_current_snapshot(&paths_for_txn, &keyed_state_for_txn)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster state sync txn".to_string(),
            detail: format!("cluster state sync txn task failed: {source}"),
        })??;
    if txn.success.is_empty() {
        return Ok(current_snapshot);
    }
    let mut commit_state = keyed_state;
    host.commit_cluster_kv_txn_with_quorum_async(paths, &mut commit_state, txn)
        .await
}

pub fn cluster_state_sync_txn_with_current_snapshot(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<(KvSnapshot, KvTxn), NetworkError> {
    let snapshot = read_cluster_kv_current_snapshot(paths)?;
    let store = InProcessKvStore::restore(snapshot.clone());
    cluster_state_sync_txn_from_store(&store, state, true).map(|txn| (snapshot, txn))
}

fn cluster_state_sync_txn_from_store(
    store: &InProcessKvStore,
    state: &ClusterBootstrapState,
    validate_config_transition: bool,
) -> Result<KvTxn, NetworkError> {
    let mut state = state.clone();
    cleanup_cluster_iroh_endpoints(&mut state);
    let new_config = ClusterKvRaftConfigRecord::from(&state);
    let current_config_entry = store.get(&sinomo_kv::cluster_kv_raft_config_key()).cloned();
    let mut compares = Vec::new();
    if let Some(current_config_entry) = &current_config_entry {
        let current_config: ClusterKvRaftConfigRecord = current_config_entry.decode()?;
        if validate_config_transition {
            sinomo_kv::validate_cluster_kv_raft_config_transition(&current_config, &new_config)?;
        }
        compares.push(KvCompare::ValueEquals {
            key: sinomo_kv::cluster_kv_raft_config_key(),
            value: current_config_entry.value.clone(),
        });
    } else {
        compares.push(KvCompare::Missing {
            key: sinomo_kv::cluster_kv_raft_config_key(),
        });
    }

    // Only emit a put when it would actually change stored KV. Re-publishing
    // semantically-identical state otherwise rewrites identity/raft-config every
    // cycle (their `updated_at`/`kv_revision` fields differ) and re-puts unchanged
    // member/endpoint records, each bumping the KV revision and rewriting the whole
    // materialized snapshot. Dedup so an unchanged state produces an empty txn.
    let mut success = Vec::new();

    let new_identity = ClusterIdentityRecord::from(&state);
    let identity_unchanged = store
        .get(&cluster_identity_key())
        .and_then(|entry| entry.decode::<ClusterIdentityRecord>().ok())
        .is_some_and(|stored| cluster_identity_record_stable_eq(&stored, &new_identity));
    if !identity_unchanged {
        success.push(KvOp::put_serde(
            cluster_identity_key(),
            &new_identity,
            None,
        )?);
    }

    let config_unchanged = current_config_entry
        .as_ref()
        .and_then(|entry| entry.decode::<ClusterKvRaftConfigRecord>().ok())
        .is_some_and(|stored| {
            sinomo_kv::cluster_kv_raft_config_record_stable_eq(&stored, &new_config)
        });
    if !config_unchanged {
        success.push(KvOp::put_serde(
            sinomo_kv::cluster_kv_raft_config_key(),
            &new_config,
            None,
        )?);
    }

    push_stale_member_identity_replacement_deletes(&store, &state, &mut success)?;

    for member in &state.members {
        let member_op = KvOp::put_serde(cluster_member_key(&member.node_id), member, None)?;
        if cluster_member_put_changes_store(&store, &member_op, member) {
            success.push(member_op);
        }
        let policy = ClusterCapabilityPolicyRecord::from(member);
        let policy_op = KvOp::put_serde(
            cluster_capability_policy_key(&member.node_id),
            &policy,
            None,
        )?;
        if cluster_capability_policy_put_changes_store(&store, &policy_op, &policy) {
            success.push(policy_op);
        }

        // Endpoint records are keyed individually. Diff them so a single endpoint
        // change does not delete and re-put the whole member endpoint set.
        let endpoint_prefix = cluster_endpoint_prefix(&member.node_id);
        let stored_endpoints: BTreeMap<String, (Vec<u8>, Option<ClusterEndpointRecord>)> = store
            .range_prefix_limited(&endpoint_prefix, MAX_CLUSTER_MEMBER_ENDPOINT_RECORDS)?
            .into_iter()
            .map(|entry| {
                let endpoint =
                    sinomo_kv::decode_kv_value::<ClusterEndpointRecord>(&entry.value).ok();
                (entry.key, (entry.value, endpoint))
            })
            .collect();
        let mut desired_endpoints: BTreeMap<String, (Vec<u8>, ClusterEndpointRecord)> =
            BTreeMap::new();
        for endpoint in &member.endpoints {
            let key = cluster_endpoint_key(&member.node_id, endpoint);
            desired_endpoints.insert(
                key,
                (
                    SinomoObjectCodec::default().encode(endpoint)?,
                    endpoint.clone(),
                ),
            );
        }
        if !cluster_endpoint_maps_stable_eq(&stored_endpoints, &desired_endpoints) {
            for key in stored_endpoints
                .keys()
                .filter(|key| !desired_endpoints.contains_key(*key))
            {
                success.push(KvOp::Delete { key: key.clone() });
            }
            for (key, (value, endpoint)) in desired_endpoints {
                if !stored_endpoints
                    .get(&key)
                    .is_some_and(|(stored_value, stored)| {
                        stored_value == &value
                            || stored
                                .as_ref()
                                .is_some_and(|stored| cluster_endpoint_stable_eq(stored, &endpoint))
                    })
                {
                    success.push(KvOp::Put {
                        key,
                        value,
                        lease_id: None,
                    });
                }
            }
        }
    }

    // Nothing changed: return an empty txn so callers can skip committing.
    if success.is_empty() {
        compares.clear();
    }
    Ok(KvTxn {
        compares,
        success,
        failure: Vec::new(),
    })
}

pub fn update_cluster_node_role_with_host(
    paths: &ClusterPaths,
    mut state: ClusterBootstrapState,
    node_id: NodeId,
    role: ClusterNodeRole,
    now: OffsetDateTime,
    host: &mut impl ClusterMemberMutationHost,
) -> Result<ClusterNodeRoleUpdate, NetworkError> {
    let store = host.current_cluster_kv_store(paths, &state)?;
    let member_key = cluster_member_key(&node_id);
    let Some(member_entry) = store.get(&member_key).cloned() else {
        return Err(NetworkError::InvalidClusterCommand {
            detail: format!("cluster member `{node_id}` was not found"),
        });
    };
    let mut member: ClusterMemberRecord = member_entry.decode()?;
    let previous_role = member.node_role.clone();
    let changed = previous_role != role;
    if changed {
        apply_cluster_node_role(&mut member, role.clone(), now);
        host.commit_cluster_kv_txn_with_quorum(
            paths,
            &mut state,
            cluster_member_update_txn_from_store(&store, member_entry, &member)?,
        )?;
    }
    update_local_cluster_node_role_if_present(paths, &node_id, role.clone(), now)?;
    Ok(ClusterNodeRoleUpdate {
        node_id,
        previous_role,
        role,
        changed,
    })
}

fn update_local_cluster_node_role_if_present(
    paths: &ClusterPaths,
    node_id: &NodeId,
    role: ClusterNodeRole,
    now: OffsetDateTime,
) -> Result<(), NetworkError> {
    let mut state = read_cluster_state_without_kv(paths)?;
    let Some(member) = state
        .members
        .iter_mut()
        .find(|member| member.node_id == *node_id)
    else {
        return Ok(());
    };
    if member.node_role == role {
        return Ok(());
    }
    apply_cluster_node_role(member, role, now);
    state.updated_at = now;
    write_cluster_state(paths, &state)?;
    Ok(())
}

pub fn update_cluster_raft_role_with_host(
    paths: &ClusterPaths,
    mut state: ClusterBootstrapState,
    node_id: NodeId,
    role: ClusterMemberRole,
    now: OffsetDateTime,
    host: &mut impl ClusterMemberMutationHost,
) -> Result<ClusterRaftRoleUpdate, NetworkError> {
    let store = host.current_cluster_kv_store(paths, &state)?;
    let member_key = cluster_member_key(&node_id);
    let Some(member_entry) = store.get(&member_key).cloned() else {
        return Err(NetworkError::InvalidClusterCommand {
            detail: format!("cluster member `{node_id}` was not found"),
        });
    };
    let new_state = cluster_raft_state_for_role(&role);
    let mut member: ClusterMemberRecord = member_entry.decode()?;
    let previous_role = member.role.clone();
    let previous_state = member.state.clone();
    let config_role = store
        .get(&sinomo_kv::cluster_kv_raft_config_key())
        .and_then(|entry| entry.decode::<ClusterKvRaftConfigRecord>().ok())
        .and_then(|config| sinomo_kv::cluster_kv_raft_config_member_role(&config, &node_id));
    let changed =
        previous_role != role || previous_state != new_state || config_role.as_ref() != Some(&role);
    if changed {
        apply_cluster_raft_role(&mut member, role.clone(), now);
        host.commit_cluster_kv_txn_with_quorum(
            paths,
            &mut state,
            cluster_member_raft_update_txn_from_store(&store, member_entry, &member, now)?,
        )?;
    }
    Ok(ClusterRaftRoleUpdate {
        node_id,
        previous_role,
        previous_state,
        role,
        state: new_state,
        changed,
    })
}

fn cluster_member_update_txn_from_store(
    store: &InProcessKvStore,
    current_entry: KvEntry,
    member: &ClusterMemberRecord,
) -> Result<KvTxn, NetworkError> {
    Ok(KvTxn {
        compares: vec![KvCompare::ValueEquals {
            key: current_entry.key,
            value: current_entry.value,
        }],
        success: cluster_member_update_ops_from_store(store, member)?,
        failure: Vec::new(),
    })
}

fn cluster_member_raft_update_txn_from_store(
    store: &InProcessKvStore,
    current_entry: KvEntry,
    member: &ClusterMemberRecord,
    now: OffsetDateTime,
) -> Result<KvTxn, NetworkError> {
    let raft_config_key = sinomo_kv::cluster_kv_raft_config_key();
    let Some(current_config_entry) = store.get(&raft_config_key).cloned() else {
        return Err(NetworkError::ClusterReadOnly {
            detail: "cluster raft config is missing".to_string(),
        });
    };
    let current_config: ClusterKvRaftConfigRecord = current_config_entry.decode()?;
    let next_config = cluster_raft_config_from_store(store, member, &current_config, now)?;
    sinomo_kv::validate_cluster_kv_raft_config_transition(&current_config, &next_config)?;
    let mut success = cluster_member_update_ops_from_store(store, member)?;
    push_bounded_txn_success_op(
        &mut success,
        KvOp::put_serde(raft_config_key.clone(), &next_config, None)?,
    )?;
    Ok(KvTxn {
        compares: vec![
            KvCompare::ValueEquals {
                key: current_entry.key,
                value: current_entry.value,
            },
            KvCompare::ValueEquals {
                key: raft_config_key,
                value: current_config_entry.value,
            },
        ],
        success,
        failure: Vec::new(),
    })
}

pub(crate) fn cluster_member_update_ops_from_store(
    store: &InProcessKvStore,
    member: &ClusterMemberRecord,
) -> Result<Vec<KvOp>, NetworkError> {
    let mut member = member.clone();
    cleanup_member_iroh_endpoints(&mut member);
    let mut ops = Vec::new();
    let member_key = cluster_member_key(&member.node_id);
    if store.get(&member_key).is_none_or(|entry| {
        entry.lease_id.is_some()
            || entry
                .decode::<ClusterMemberRecord>()
                .map(|stored| !cluster_member_stable_eq(&stored, &member))
                .unwrap_or(true)
    }) {
        push_bounded_txn_success_op(&mut ops, KvOp::put_serde(member_key, &member, None)?)?;
    }

    let policy = ClusterCapabilityPolicyRecord::from(&member);
    let policy_key = cluster_capability_policy_key(&member.node_id);
    if store.get(&policy_key).is_none_or(|entry| {
        entry.lease_id.is_some()
            || entry
                .decode::<ClusterCapabilityPolicyRecord>()
                .map(|stored| !cluster_capability_policy_stable_eq(&stored, &policy))
                .unwrap_or(true)
    }) {
        push_bounded_txn_success_op(&mut ops, KvOp::put_serde(policy_key, &policy, None)?)?;
    }
    push_store_cluster_member_endpoint_diff_ops(store, &member, &mut ops)?;
    Ok(ops)
}

fn push_store_cluster_member_endpoint_diff_ops(
    store: &InProcessKvStore,
    member: &ClusterMemberRecord,
    ops: &mut Vec<KvOp>,
) -> Result<(), NetworkError> {
    let endpoint_prefix = cluster_endpoint_prefix(&member.node_id);
    let mut desired_endpoints = BTreeMap::new();
    for endpoint in &member.endpoints {
        let key = cluster_endpoint_key(&member.node_id, endpoint);
        let op = KvOp::put_serde(key, endpoint, None)?;
        let KvOp::Put { key, value, .. } = op else {
            unreachable!("put_serde must produce a put operation");
        };
        desired_endpoints.insert(key, (value, endpoint.clone()));
    }

    for entry in
        store.range_prefix_limited(&endpoint_prefix, MAX_CLUSTER_MEMBER_ENDPOINT_RECORDS)?
    {
        if !desired_endpoints.contains_key(&entry.key) {
            push_bounded_txn_success_op(ops, KvOp::Delete { key: entry.key })?;
        }
    }

    for (key, (value, endpoint)) in desired_endpoints {
        if store.get(&key).is_none_or(|entry| {
            entry.lease_id.is_some()
                || (entry.value != value
                    && entry
                        .decode::<ClusterEndpointRecord>()
                        .map(|stored| !cluster_endpoint_stable_eq(&stored, &endpoint))
                        .unwrap_or(true))
        }) {
            push_bounded_txn_success_op(
                ops,
                KvOp::Put {
                    key,
                    value,
                    lease_id: None,
                },
            )?;
        }
    }

    Ok(())
}

pub(crate) fn cluster_raft_config_from_store(
    store: &InProcessKvStore,
    updated_member: &ClusterMemberRecord,
    current_config: &ClusterKvRaftConfigRecord,
    now: OffsetDateTime,
) -> Result<ClusterKvRaftConfigRecord, NetworkError> {
    let mut voters = Vec::new();
    let mut learners = Vec::new();
    let mut non_voting = Vec::new();
    for entry in
        store.range_prefix_limited("/cluster/members/", CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES)?
    {
        let mut member: ClusterMemberRecord = entry.decode()?;
        if member.node_id == updated_member.node_id {
            member = updated_member.clone();
        }
        if !member.state.is_authorized() {
            continue;
        }
        match member.state {
            ClusterMemberState::Voter => voters.push(member.node_id),
            ClusterMemberState::Learner => learners.push(member.node_id),
            ClusterMemberState::NonVoting => non_voting.push(member.node_id),
            ClusterMemberState::Active => match member.role {
                ClusterMemberRole::Voter => voters.push(member.node_id),
                ClusterMemberRole::Learner => learners.push(member.node_id),
                ClusterMemberRole::NonVoting => non_voting.push(member.node_id),
            },
            ClusterMemberState::Pending
            | ClusterMemberState::Suspect
            | ClusterMemberState::Offline
            | ClusterMemberState::Removed => {}
        }
    }
    voters.sort();
    learners.sort();
    non_voting.sort();
    Ok(ClusterKvRaftConfigRecord {
        cluster_id: current_config.cluster_id.clone(),
        term: current_config.term,
        kv_revision: current_config.kv_revision,
        voters,
        learners,
        non_voting,
        updated_at: now,
    })
}

pub fn remove_cluster_members_with_host(
    paths: &ClusterPaths,
    mut state: ClusterBootstrapState,
    node_ids: Vec<NodeId>,
    host: &mut impl ClusterMemberMutationHost,
) -> Result<ClusterMemberRemoval, NetworkError> {
    let store = host.current_cluster_kv_store(paths, &state)?;
    let requested = node_ids.into_iter().collect::<BTreeSet<_>>();
    if requested.is_empty() {
        return Err(NetworkError::InvalidClusterCommand {
            detail: "no cluster members were specified".to_string(),
        });
    }
    let local_shadow_node_id = shadow_daemon_node_id(&state.local_node_id);
    if requested.contains(&state.local_node_id) || requested.contains(&local_shadow_node_id) {
        return Err(NetworkError::InvalidClusterCommand {
            detail: "use `sinomo cluster leave` to remove the local daemon members".to_string(),
        });
    }

    let mut removed = Vec::new();
    for node_id in &requested {
        let member_key = cluster_member_key(node_id);
        let Some(member_entry) = store.get(&member_key).cloned() else {
            return Err(NetworkError::InvalidClusterCommand {
                detail: format!("cluster member `{node_id}` was not found"),
            });
        };
        let member: ClusterMemberRecord = member_entry.decode()?;
        if sinomo_kv::cluster_kv_store_member_is_raft_voter(&store, &member.node_id)? {
            return Err(NetworkError::ClusterReadOnly {
                detail: format!(
                    "cluster member `{}` is a voter; change its raft role before removal",
                    member.node_id
                ),
            });
        }
        removed.push(member);
    }

    for txn in cluster_member_removal_txns_from_store(&store, &removed)? {
        host.commit_cluster_kv_txn_with_quorum(paths, &mut state, txn)?;
    }
    Ok(ClusterMemberRemoval { removed })
}

pub fn cleanup_legacy_shadow_daemon_members_with_host(
    paths: &ClusterPaths,
    mut state: ClusterBootstrapState,
    host: &mut impl ClusterMemberMutationHost,
) -> Result<ClusterMemberRemoval, NetworkError> {
    let store = host.current_cluster_kv_store(paths, &state)?;
    let snapshot = store.snapshot();
    let policies = read_cluster_capability_policies_from_snapshot(&snapshot)?;
    let mut current_member_ids = BTreeSet::new();
    let mut removed = Vec::new();
    for entry in store.prefix_entries("/cluster/members/") {
        let mut member: ClusterMemberRecord = entry.decode()?;
        current_member_ids.insert(member.node_id.clone());
        if let Some(policy) = policies.get(&member.node_id) {
            member.capabilities = policy.capabilities.clone();
            member.allowed_capabilities = policy.allowed_capabilities.clone();
        }
        if !cluster_member_is_legacy_shadow_daemon(&member) {
            continue;
        }
        if sinomo_kv::cluster_kv_store_member_is_raft_voter(&store, &member.node_id)? {
            return Err(NetworkError::ClusterReadOnly {
                detail: format!(
                    "legacy shadow member `{}` is a voter; change its raft role before cleanup",
                    member.node_id
                ),
            });
        }
        removed.push(member);
    }
    removed.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    let orphan_artifact_node_ids =
        cluster_legacy_shadow_orphan_artifact_node_ids_from_store(&store, &current_member_ids)?;

    for txn in cluster_member_removal_txns_from_store(&store, &removed)? {
        host.commit_cluster_kv_txn_with_quorum(paths, &mut state, txn)?;
    }
    for txn in cluster_legacy_shadow_orphan_artifact_cleanup_txns_from_store(
        &store,
        &orphan_artifact_node_ids,
    )? {
        host.commit_cluster_kv_txn_with_quorum(paths, &mut state, txn)?;
    }
    Ok(ClusterMemberRemoval { removed })
}

pub(crate) fn cluster_member_removal_txns_from_store(
    store: &InProcessKvStore,
    members: &[ClusterMemberRecord],
) -> Result<Vec<KvTxn>, NetworkError> {
    let mut txns = Vec::new();
    let final_endpoint_prefix_delete_limit =
        sinomo_kv::MAX_KV_MUTATION_EVENTS_PER_COMMAND.saturating_sub(2);
    let explicit_delete_batch_limit =
        sinomo_kv::MAX_KV_TXN_OPS_PER_BRANCH.min(sinomo_kv::MAX_KV_MUTATION_EVENTS_PER_COMMAND);
    for member in members {
        let mut connectivity_delete_ops = Vec::new();
        for key in cluster_member_connectivity_delete_keys_from_store(store, &member.node_id)? {
            connectivity_delete_ops.push(KvOp::Delete { key });
            if connectivity_delete_ops.len() >= explicit_delete_batch_limit {
                txns.push(KvTxn {
                    compares: Vec::new(),
                    success: std::mem::take(&mut connectivity_delete_ops),
                    failure: Vec::new(),
                });
            }
        }
        if !connectivity_delete_ops.is_empty() {
            txns.push(KvTxn {
                compares: Vec::new(),
                success: connectivity_delete_ops,
                failure: Vec::new(),
            });
        }

        let endpoint_prefix = cluster_endpoint_prefix(&member.node_id);
        let mut residual_endpoint_keys = VecDeque::new();
        let mut explicit_delete_ops = Vec::new();
        for entry in store.prefix_entries(&endpoint_prefix) {
            residual_endpoint_keys.push_back(entry.key.clone());
            if residual_endpoint_keys.len() <= final_endpoint_prefix_delete_limit {
                continue;
            }
            if let Some(key) = residual_endpoint_keys.pop_front() {
                explicit_delete_ops.push(KvOp::Delete { key });
            }
            if explicit_delete_ops.len() >= explicit_delete_batch_limit {
                txns.push(KvTxn {
                    compares: Vec::new(),
                    success: std::mem::take(&mut explicit_delete_ops),
                    failure: Vec::new(),
                });
            }
        }
        if !explicit_delete_ops.is_empty() {
            txns.push(KvTxn {
                compares: Vec::new(),
                success: explicit_delete_ops,
                failure: Vec::new(),
            });
        }

        txns.push(KvTxn {
            compares: Vec::new(),
            success: vec![
                KvOp::Delete {
                    key: cluster_member_key(&member.node_id),
                },
                KvOp::Delete {
                    key: cluster_capability_policy_key(&member.node_id),
                },
                KvOp::DeletePrefix {
                    prefix: endpoint_prefix,
                },
            ],
            failure: Vec::new(),
        });
    }
    Ok(txns)
}

fn cluster_member_connectivity_delete_keys_from_store(
    store: &InProcessKvStore,
    node_id: &NodeId,
) -> Result<Vec<String>, NetworkError> {
    let mut keys = BTreeSet::new();
    for entry in store.prefix_entries(&cluster_connectivity_source_prefix(node_id)) {
        keys.insert(entry.key.clone());
    }
    for entry in store.prefix_entries("/cluster/network/connectivity/") {
        if keys.contains(&entry.key) {
            continue;
        }
        let record: ClusterConnectivityRecord = entry.decode()?;
        if record.source_node_id == *node_id || record.target_node_id == *node_id {
            keys.insert(entry.key.clone());
        }
    }
    Ok(keys.into_iter().collect())
}

fn cluster_legacy_shadow_orphan_artifact_node_ids_from_store(
    store: &InProcessKvStore,
    current_member_ids: &BTreeSet<NodeId>,
) -> Result<BTreeSet<NodeId>, NetworkError> {
    let mut node_ids = BTreeSet::new();
    for entry in store.prefix_entries("/cluster/policy/capabilities/") {
        let policy: ClusterCapabilityPolicyRecord = entry.decode()?;
        if !current_member_ids.contains(&policy.node_id)
            && cluster_capabilities_are_legacy_shadow_only(&policy.capabilities)
        {
            node_ids.insert(policy.node_id);
        }
    }
    for entry in store.prefix_entries("/cluster/node-status/") {
        let status: ClusterNodeStatusRecord = entry.decode()?;
        if !current_member_ids.contains(&status.node_id)
            && status.process == DAEMON_SHADOW_CLUSTER_ROLE
        {
            node_ids.insert(status.node_id);
        }
    }
    for entry in store.prefix_entries("/cluster/network/connectivity/") {
        let record: ClusterConnectivityRecord = entry.decode()?;
        if !current_member_ids.contains(&record.source_node_id) {
            node_ids.insert(record.source_node_id);
        }
        if !current_member_ids.contains(&record.target_node_id) {
            node_ids.insert(record.target_node_id);
        }
    }
    Ok(node_ids)
}

fn cluster_legacy_shadow_orphan_artifact_cleanup_txns_from_store(
    store: &InProcessKvStore,
    node_ids: &BTreeSet<NodeId>,
) -> Result<Vec<KvTxn>, NetworkError> {
    let explicit_delete_batch_limit =
        sinomo_kv::MAX_KV_TXN_OPS_PER_BRANCH.min(sinomo_kv::MAX_KV_MUTATION_EVENTS_PER_COMMAND);
    let mut txns = Vec::new();
    let mut ops = Vec::new();
    for node_id in node_ids {
        for key in cluster_member_connectivity_delete_keys_from_store(store, node_id)? {
            ops.push(KvOp::Delete { key });
            if ops.len() >= explicit_delete_batch_limit {
                txns.push(KvTxn {
                    compares: Vec::new(),
                    success: std::mem::take(&mut ops),
                    failure: Vec::new(),
                });
            }
        }
        ops.push(KvOp::Delete {
            key: cluster_capability_policy_key(node_id),
        });
        ops.push(KvOp::Delete {
            key: cluster_node_status_key(node_id),
        });
        ops.push(KvOp::Delete {
            key: cluster_node_status_key_for_process(node_id, DAEMON_SHADOW_CLUSTER_ROLE),
        });
        ops.push(KvOp::DeletePrefix {
            prefix: cluster_endpoint_prefix(node_id),
        });
        if ops.len() >= explicit_delete_batch_limit {
            txns.push(KvTxn {
                compares: Vec::new(),
                success: std::mem::take(&mut ops),
                failure: Vec::new(),
            });
        }
    }
    if !ops.is_empty() {
        txns.push(KvTxn {
            compares: Vec::new(),
            success: ops,
            failure: Vec::new(),
        });
    }
    Ok(txns)
}

fn cluster_member_put_changes_store(
    store: &InProcessKvStore,
    op: &KvOp,
    member: &ClusterMemberRecord,
) -> bool {
    match op {
        KvOp::Put { key, value, .. } => store
            .get(key)
            .map(|entry| {
                entry.value.as_slice() != value.as_slice()
                    && entry
                        .decode::<ClusterMemberRecord>()
                        .map(|stored| !cluster_member_stable_eq(&stored, member))
                        .unwrap_or(true)
            })
            .unwrap_or(true),
        _ => true,
    }
}

fn push_stale_member_identity_replacement_deletes(
    store: &InProcessKvStore,
    state: &ClusterBootstrapState,
    ops: &mut Vec<KvOp>,
) -> Result<(), NetworkError> {
    let mut desired_nodes_by_public_key: BTreeMap<String, BTreeSet<NodeId>> = BTreeMap::new();
    for member in &state.members {
        if member.node_public_key.trim().is_empty() {
            continue;
        }
        desired_nodes_by_public_key
            .entry(member.node_public_key.clone())
            .or_default()
            .insert(member.node_id.clone());
    }
    if desired_nodes_by_public_key.is_empty() {
        return Ok(());
    }

    for entry in
        store.range_prefix_limited("/cluster/members/", CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES)?
    {
        let stored: ClusterMemberRecord = entry.decode()?;
        let Some(desired_nodes) = desired_nodes_by_public_key.get(&stored.node_public_key) else {
            continue;
        };
        if desired_nodes.contains(&stored.node_id) {
            continue;
        }

        push_bounded_txn_success_op(ops, KvOp::Delete { key: entry.key })?;
        push_bounded_txn_success_op(
            ops,
            KvOp::Delete {
                key: cluster_capability_policy_key(&stored.node_id),
            },
        )?;
        for endpoint in store.range_prefix_limited(
            &cluster_endpoint_prefix(&stored.node_id),
            MAX_CLUSTER_MEMBER_ENDPOINT_RECORDS,
        )? {
            push_bounded_txn_success_op(ops, KvOp::Delete { key: endpoint.key })?;
        }
    }
    Ok(())
}

fn cluster_capability_policy_put_changes_store(
    store: &InProcessKvStore,
    op: &KvOp,
    policy: &ClusterCapabilityPolicyRecord,
) -> bool {
    match op {
        KvOp::Put { key, value, .. } => store
            .get(key)
            .map(|entry| {
                entry.value.as_slice() != value.as_slice()
                    && entry
                        .decode::<ClusterCapabilityPolicyRecord>()
                        .map(|stored| !cluster_capability_policy_stable_eq(&stored, policy))
                        .unwrap_or(true)
            })
            .unwrap_or(true),
        _ => true,
    }
}

fn cluster_endpoint_maps_stable_eq(
    stored: &BTreeMap<String, (Vec<u8>, Option<ClusterEndpointRecord>)>,
    desired: &BTreeMap<String, (Vec<u8>, ClusterEndpointRecord)>,
) -> bool {
    stored.len() == desired.len()
        && desired.iter().all(|(key, (desired_value, desired))| {
            stored.get(key).is_some_and(|(stored_value, stored)| {
                stored_value == desired_value
                    || stored
                        .as_ref()
                        .is_some_and(|stored| cluster_endpoint_stable_eq(stored, desired))
            })
        })
}

struct NetworkClusterKvRaftAppendReplicationHost<'a, H> {
    paths: &'a ClusterPaths,
    options: ClusterRaftAppendReplicationOptions,
    host: &'a mut H,
}

impl<H> sinomo_kv::ClusterKvRaftAppendReplicationHost
    for NetworkClusterKvRaftAppendReplicationHost<'_, H>
where
    H: ClusterRaftAppendReplicationHost,
{
    type Error = NetworkError;
    type Target = ClusterEndpointRecord;

    fn last_local_raft_log_index(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<u64, Self::Error> {
        self.host.last_local_raft_log_index(self.paths, state)
    }

    fn build_raft_append_request(
        &mut self,
        state: &ClusterBootstrapState,
        next_index: u64,
    ) -> Result<ClusterRaftAppendRequest, Self::Error> {
        self.host
            .build_raft_append_request(self.paths, state, next_index)
    }

    fn append_attempt_timeout(
        &mut self,
        target: &Self::Target,
        remaining: Duration,
        request: &ClusterRaftAppendRequest,
    ) -> Duration {
        self.options
            .append_attempt_timeout(target, remaining, request)
    }

    fn on_raft_append_sent(&mut self) -> Result<(), Self::Error> {
        self.host.on_raft_append_sent();
        Ok(())
    }

    fn send_raft_append(
        &mut self,
        target: &Self::Target,
        request: &ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, Self::Error> {
        self.host.send_raft_append(target, request, timeout)
    }
}

struct NetworkClusterKvRaftAppendReplicationAsyncHost<'a, H> {
    paths: &'a ClusterPaths,
    options: ClusterRaftAppendReplicationOptions,
    host: &'a mut H,
}

impl<H> sinomo_kv::ClusterKvRaftAppendReplicationAsyncHost
    for NetworkClusterKvRaftAppendReplicationAsyncHost<'_, H>
where
    H: ClusterRaftAppendReplicationAsyncHost,
{
    type Error = NetworkError;
    type Target = ClusterEndpointRecord;

    fn last_local_raft_log_index_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, u64, Self::Error> {
        self.host.last_local_raft_log_index_async(self.paths, state)
    }

    fn build_raft_append_request_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        next_index: u64,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, ClusterRaftAppendRequest, Self::Error> {
        self.host
            .build_raft_append_request_async(self.paths, state, next_index)
    }

    fn append_attempt_timeout(
        &mut self,
        target: &Self::Target,
        remaining: Duration,
        request: &ClusterRaftAppendRequest,
    ) -> Duration {
        self.options
            .append_attempt_timeout(target, remaining, request)
    }

    fn on_raft_append_sent_async<'a>(
        &'a mut self,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        self.host.on_raft_append_sent_async()
    }

    fn send_raft_append_async<'a>(
        &'a mut self,
        target: &'a Self::Target,
        request: &'a ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, ClusterRaftAppendResponse, Self::Error> {
        self.host.send_raft_append_async(target, request, timeout)
    }
}

pub fn replicate_cluster_raft_to_endpoint_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    watch_events: &[KvEvent],
    options: ClusterRaftAppendReplicationOptions,
    host: &mut impl ClusterRaftAppendReplicationHost,
) -> Result<ClusterRaftAppendReplicationOutcome, NetworkError> {
    let mut kv_host = NetworkClusterKvRaftAppendReplicationHost {
        paths,
        options,
        host,
    };
    sinomo_kv::replicate_cluster_kv_raft_to_target_with_host(
        state,
        endpoint,
        watch_events,
        options.kv_options(),
        &mut kv_host,
    )
}

pub async fn replicate_cluster_raft_to_endpoint_async_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    watch_events: &[KvEvent],
    options: ClusterRaftAppendReplicationOptions,
    host: &mut impl ClusterRaftAppendReplicationAsyncHost,
) -> Result<ClusterRaftAppendReplicationOutcome, NetworkError> {
    let mut kv_host = NetworkClusterKvRaftAppendReplicationAsyncHost {
        paths,
        options,
        host,
    };
    sinomo_kv::replicate_cluster_kv_raft_to_target_async_with_host(
        state,
        endpoint,
        watch_events,
        options.kv_options(),
        &mut kv_host,
    )
    .await
}

struct NetworkClusterKvRaftQuorumCommitHost<'a, H> {
    paths: &'a ClusterPaths,
    host: &'a mut H,
}

impl<H> sinomo_kv::ClusterKvRaftCommandStorage for NetworkClusterKvRaftQuorumCommitHost<'_, H>
where
    H: ClusterRaftQuorumCommitHost,
{
    type Error = NetworkError;

    fn lock_cluster_raft_commit_sequence(&mut self) -> Result<Option<std::fs::File>, NetworkError> {
        sinomo_kv::lock_cluster_kv_commit_sequence(
            &self.paths.kv_paths(),
            sinomo_kv::CLUSTER_KV_COMMIT_SEQUENCE_LOCK_WAIT,
        )
        .map(Some)
        .map_err(Into::into)
    }

    fn append_cluster_raft_command_uncommitted_or_no_effect(
        &mut self,
        command: KvCommand,
    ) -> Result<sinomo_kv::ClusterKvRaftUncommittedAppend, NetworkError> {
        self.host
            .append_cluster_raft_command_uncommitted_or_no_effect(command)
    }

    fn discard_cluster_raft_uncommitted_tail_and_clear_leader(
        &mut self,
    ) -> Result<(), NetworkError> {
        self.host
            .discard_cluster_raft_uncommitted_tail_and_clear_leader()
    }

    fn commit_cluster_raft_index_with_events(
        &mut self,
        index: u64,
    ) -> Result<sinomo_kv::ClusterKvRaftCommitResult, NetworkError> {
        self.host.commit_cluster_raft_index_with_events(index)
    }
}

impl<H> sinomo_kv::ClusterKvRaftQuorumCommitHost for NetworkClusterKvRaftQuorumCommitHost<'_, H>
where
    H: ClusterRaftQuorumCommitHost,
{
    type Target = ClusterEndpointRecord;

    fn remote_voter_targets(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<Vec<Self::Target>, NetworkError> {
        Ok(cluster_raft_live_voter_targets(self.paths, state))
    }

    fn target_node_id<'a>(&self, target: &'a Self::Target) -> &'a NodeId {
        &target.node_id
    }

    fn target_description(&self, target: &Self::Target) -> String {
        format!("{} ({:?})", target.address, target.transport)
    }

    fn replicate_cluster_raft_to_target(
        &mut self,
        state: &ClusterBootstrapState,
        target: &Self::Target,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError> {
        self.host
            .replicate_cluster_raft_to_endpoint(self.paths, state, target, timeout)
    }

    fn record_target_replication_status(
        &mut self,
        state: &ClusterBootstrapState,
        target: &Self::Target,
        status: sinomo_kv::ClusterKvRaftTargetReplicationStatus,
        latency_ms: Option<u64>,
    ) -> Result<(), NetworkError> {
        let connectivity_state = match status {
            sinomo_kv::ClusterKvRaftTargetReplicationStatus::Accepted
            | sinomo_kv::ClusterKvRaftTargetReplicationStatus::Rejected => {
                ClusterConnectivityState::Healthy
            }
            sinomo_kv::ClusterKvRaftTargetReplicationStatus::Failed => {
                ClusterConnectivityState::Degraded
            }
        };
        self.host.record_endpoint_command_connectivity(
            self.paths,
            state,
            target,
            connectivity_state,
            latency_ms,
        );
        Ok(())
    }

    fn observe_accepted_append_state(
        &mut self,
        state: &mut ClusterBootstrapState,
        _target: &Self::Target,
        peer_state: ClusterBootstrapState,
    ) -> Result<(), NetworkError> {
        merge_cluster_state_from_peer(state, peer_state);
        Ok(())
    }

    fn fan_out_committed_cluster_raft_command(
        &mut self,
        state: ClusterBootstrapState,
        events: Vec<KvEvent>,
        targets: Vec<Self::Target>,
    ) -> Result<(), NetworkError> {
        self.host.fan_out_committed_cluster_raft_command(
            self.paths.clone(),
            state,
            events,
            targets,
        );
        Ok(())
    }

    fn cluster_read_only_error(&mut self, detail: String) -> NetworkError {
        NetworkError::ClusterReadOnly { detail }
    }
}

pub fn commit_cluster_raft_command_with_quorum_driver(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    command: KvCommand,
    options: ClusterRaftQuorumCommitOptions,
    host: &mut impl ClusterRaftQuorumCommitHost,
) -> Result<ClusterRaftQuorumCommitResult, NetworkError> {
    let mut kv_host = NetworkClusterKvRaftQuorumCommitHost { paths, host };
    sinomo_kv::commit_cluster_kv_raft_command_with_quorum(state, command, options, &mut kv_host)
}

struct NetworkClusterKvRaftQuorumCommitAsyncHost<'a, H> {
    runtime: &'a tokio::runtime::Handle,
    paths: &'a ClusterPaths,
    local_node_id: NodeId,
    append_options: ClusterRaftAppendReplicationOptions,
    host: &'a mut H,
}

impl<H> sinomo_kv::ClusterKvRaftQuorumCommitAsyncHost
    for NetworkClusterKvRaftQuorumCommitAsyncHost<'_, H>
where
    H: ClusterRaftQuorumAsyncHost + Send,
{
    type Error = NetworkError;
    type Target = ClusterEndpointRecord;

    fn lock_cluster_raft_commit_sequence_async<'a>(
        &'a mut self,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, Option<std::fs::File>, NetworkError> {
        let runtime = self.runtime;
        let kv_paths = self.paths.kv_paths();
        Box::pin(async move {
            runtime
                .spawn_blocking(move || {
                    sinomo_kv::lock_cluster_kv_commit_sequence(
                        &kv_paths,
                        sinomo_kv::CLUSTER_KV_COMMIT_SEQUENCE_LOCK_WAIT,
                    )
                })
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "cluster raft commit sequence lock".to_string(),
                    detail: format!("cluster raft commit sequence lock task failed: {source}"),
                })?
                .map(Some)
                .map_err(Into::into)
        })
    }

    fn append_cluster_raft_command_uncommitted_or_no_effect_async<'a>(
        &'a mut self,
        command: KvCommand,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<
        'a,
        sinomo_kv::ClusterKvRaftUncommittedAppend,
        NetworkError,
    > {
        let runtime = self.runtime;
        let paths = self.paths;
        let local_node_id = self.local_node_id.clone();
        Box::pin(async move {
            append_cluster_raft_command_uncommitted_or_no_effect_async(
                runtime,
                paths,
                &local_node_id,
                command,
            )
            .await
        })
    }

    fn discard_cluster_raft_uncommitted_tail_and_clear_leader_async<'a>(
        &'a mut self,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), NetworkError> {
        let runtime = self.runtime;
        let paths = self.paths;
        let local_node_id = self.local_node_id.clone();
        Box::pin(async move {
            discard_cluster_raft_uncommitted_tail_and_clear_leader_async(
                runtime,
                paths,
                &local_node_id,
            )
            .await
            .map(|_| ())
        })
    }

    fn commit_cluster_raft_index_with_events_async<'a>(
        &'a mut self,
        index: u64,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, sinomo_kv::ClusterKvRaftCommitResult, NetworkError>
    {
        let runtime = self.runtime;
        let paths = self.paths;
        let local_node_id = self.local_node_id.clone();
        Box::pin(async move {
            commit_cluster_raft_index_with_events_async(runtime, paths, &local_node_id, index).await
        })
    }

    fn remote_voter_targets_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, Vec<Self::Target>, NetworkError> {
        let runtime = self.runtime;
        let paths = self.paths;
        Box::pin(async move { cluster_raft_live_voter_targets_async(runtime, paths, state).await })
    }

    fn target_node_id<'a>(&self, target: &'a Self::Target) -> &'a NodeId {
        &target.node_id
    }

    fn target_description(&self, target: &Self::Target) -> String {
        format!("{} ({:?})", target.address, target.transport)
    }

    fn replicate_cluster_raft_to_target_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        target: &'a Self::Target,
        timeout: Duration,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, ClusterRaftAppendResponse, NetworkError> {
        let paths = self.paths;
        let mut endpoint_options = self.append_options;
        let host = &mut *self.host;
        Box::pin(async move {
            endpoint_options.timeout = timeout;
            replicate_cluster_raft_to_endpoint_async_with_host(
                paths,
                state,
                target,
                &[],
                endpoint_options,
                host,
            )
            .await
            .map(|outcome| outcome.response)
        })
    }

    fn record_target_replication_status_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        target: &'a Self::Target,
        status: sinomo_kv::ClusterKvRaftTargetReplicationStatus,
        latency_ms: Option<u64>,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), NetworkError> {
        let paths = self.paths;
        let host = &mut *self.host;
        Box::pin(async move {
            let connectivity_state = match status {
                sinomo_kv::ClusterKvRaftTargetReplicationStatus::Accepted
                | sinomo_kv::ClusterKvRaftTargetReplicationStatus::Rejected => {
                    ClusterConnectivityState::Healthy
                }
                sinomo_kv::ClusterKvRaftTargetReplicationStatus::Failed => {
                    ClusterConnectivityState::Degraded
                }
            };
            host.record_endpoint_command_connectivity(
                paths,
                state,
                target,
                connectivity_state,
                latency_ms,
            );
            Ok(())
        })
    }

    fn observe_accepted_append_state_async<'a>(
        &'a mut self,
        state: &'a mut ClusterBootstrapState,
        _target: &'a Self::Target,
        peer_state: ClusterBootstrapState,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), NetworkError> {
        Box::pin(async move {
            merge_cluster_state_from_peer(state, peer_state);
            Ok(())
        })
    }

    fn fan_out_committed_cluster_raft_command_async<'a>(
        &'a mut self,
        state: ClusterBootstrapState,
        events: Vec<KvEvent>,
        targets: Vec<Self::Target>,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), NetworkError> {
        let paths = self.paths.clone();
        let host = &mut *self.host;
        Box::pin(async move {
            host.fan_out_committed_cluster_raft_command_async(paths, state, events, targets)
                .await
        })
    }

    fn cluster_read_only_error(&mut self, detail: String) -> NetworkError {
        NetworkError::ClusterReadOnly { detail }
    }
}

pub async fn commit_cluster_raft_command_with_quorum_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    command: KvCommand,
    options: ClusterRaftQuorumCommitOptions,
    append_options: ClusterRaftAppendReplicationOptions,
    host: &mut H,
) -> Result<ClusterRaftQuorumCommitResult, NetworkError>
where
    H: ClusterRaftQuorumAsyncHost + Send,
{
    let mut kv_host = NetworkClusterKvRaftQuorumCommitAsyncHost {
        runtime,
        paths,
        local_node_id: state.local_node_id.clone(),
        append_options,
        host,
    };
    sinomo_kv::commit_cluster_kv_raft_command_with_quorum_async(
        state,
        command,
        options,
        &mut kv_host,
    )
    .await
}

pub async fn fan_out_committed_cluster_raft_command_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    events: &[KvEvent],
    options: ClusterRaftCommittedFanoutOptions,
    host: &mut impl ClusterRaftCommittedFanoutAsyncHost,
) -> Result<ClusterRaftCommittedFanoutReport, NetworkError> {
    let mut report = ClusterRaftCommittedFanoutReport::default();
    let mut replicated_nodes = BTreeSet::new();
    for endpoint in cluster_raft_member_replication_targets_async(runtime, paths, state).await? {
        if replicated_nodes.contains(&endpoint.node_id) {
            continue;
        }
        report.attempted_endpoints += 1;
        let Ok(outcome) = host
            .replicate_cluster_raft_to_endpoint_async(
                paths,
                state,
                &endpoint,
                events,
                options.raft_append_timeout,
            )
            .await
        else {
            continue;
        };
        if !outcome.response.accepted {
            continue;
        }
        replicated_nodes.insert(endpoint.node_id.clone());
        report.replicated_nodes.push(endpoint.node_id.clone());
        if outcome.watch_event_fanout_needed {
            report.watch_event_batches_sent +=
                send_cluster_kv_watch_event_fanout_batches_async_with_host(
                    state,
                    &endpoint,
                    events,
                    options.watch_event_timeout,
                    host,
                )
                .await;
        }
    }
    Ok(report)
}

pub fn spawn_cluster_raft_committed_fanout_worker_on_runtime<H, F>(
    runtime: tokio::runtime::Handle,
    paths: ClusterPaths,
    state: ClusterBootstrapState,
    events: Vec<KvEvent>,
    options: ClusterRaftCommittedFanoutOptions,
    host_factory: F,
) where
    H: ClusterRaftCommittedFanoutAsyncHost + Send + 'static,
    F: FnMut() -> H + Send + 'static,
{
    CLUSTER_RAFT_FANOUT_PENDING.store(true, Ordering::Release);
    if CLUSTER_RAFT_FANOUT_ACTIVE.swap(true, Ordering::AcqRel) {
        return;
    }
    let worker_runtime = runtime.clone();
    runtime.spawn(async move {
        run_cluster_raft_committed_fanout_worker_async_with_host(
            worker_runtime,
            paths,
            state,
            events,
            options,
            host_factory,
        )
        .await;
    });
}

async fn run_cluster_raft_committed_fanout_worker_async_with_host<H, F>(
    runtime: tokio::runtime::Handle,
    paths: ClusterPaths,
    mut state: ClusterBootstrapState,
    mut events: Vec<KvEvent>,
    options: ClusterRaftCommittedFanoutOptions,
    mut host_factory: F,
) where
    H: ClusterRaftCommittedFanoutAsyncHost,
    F: FnMut() -> H,
{
    loop {
        while CLUSTER_RAFT_FANOUT_PENDING.swap(false, Ordering::AcqRel) {
            let mut host = host_factory();
            let _ = fan_out_committed_cluster_raft_command_async_with_host(
                &runtime, &paths, &state, &events, options, &mut host,
            )
            .await;
            if let Some((latest_state, latest_events)) =
                read_latest_cluster_raft_fanout_inputs_async(&runtime, &paths).await
            {
                state = latest_state;
                events = latest_events;
            } else {
                events = Vec::new();
            }
        }

        if !claim_next_cluster_raft_fanout_pass(
            &CLUSTER_RAFT_FANOUT_ACTIVE,
            &CLUSTER_RAFT_FANOUT_PENDING,
        ) {
            break;
        }
    }
}

async fn read_latest_cluster_raft_fanout_inputs_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
) -> Option<(ClusterBootstrapState, Vec<KvEvent>)> {
    let paths = paths.clone();
    runtime
        .spawn_blocking(move || {
            let latest_state = read_cluster_state(&paths)?;
            let latest_events = read_cluster_kv_snapshot(&paths)
                .map(|snapshot| snapshot.events)
                .unwrap_or_default();
            Ok::<_, NetworkError>((latest_state, latest_events))
        })
        .await
        .ok()
        .and_then(Result::ok)
}

#[cfg(test)]
pub(crate) fn claim_next_cluster_raft_fanout_pass_for_test(
    active: &AtomicBool,
    pending: &AtomicBool,
) -> bool {
    claim_next_cluster_raft_fanout_pass(active, pending)
}

fn claim_next_cluster_raft_fanout_pass(active: &AtomicBool, pending: &AtomicBool) -> bool {
    active.store(false, Ordering::Release);
    if !pending.load(Ordering::Acquire) {
        return false;
    }
    !active.swap(true, Ordering::AcqRel)
}

async fn send_cluster_kv_watch_event_fanout_batches_async_with_host(
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    events: &[KvEvent],
    timeout: Duration,
    host: &mut impl ClusterRaftCommittedFanoutAsyncHost,
) -> usize {
    let mut sent = 0;
    for event_batch in sinomo_kv::cluster_kv_watch_event_fanout_batches(events) {
        let response = host
            .send_cluster_kv_watch_events_async(
                endpoint,
                &ClusterKvWatchEventsRequest {
                    state: state.clone(),
                    leader_id: state.local_node_id.clone(),
                    events: event_batch.to_vec(),
                },
                timeout,
            )
            .await;
        if !response.is_ok_and(|response| response.accepted) {
            break;
        }
        sent += 1;
    }
    sent
}

pub fn cluster_reachable_voter_count_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterVoterReachabilityOptions,
    host: &mut impl ClusterVoterReachabilityHost,
) -> usize {
    let quorum_size = sinomo_kv::cluster_kv_raft_quorum_size_for_state(state);
    let mut voters = BTreeSet::new();
    if sinomo_kv::cluster_kv_raft_local_is_voter(state) {
        voters.insert(state.local_node_id.clone());
        if voters.len() >= quorum_size {
            return voters.len();
        }
    }
    let deadline = Instant::now() + options.timeout;
    let targets = cluster_raft_live_voter_targets(paths, state);
    let graph = ClusterConnectivityGraph::read_for_local(paths, state);
    for (index, endpoint) in targets.iter().enumerate() {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            break;
        }
        if host.probe_voter(
            endpoint,
            cluster_voter_probe_timeout(&graph, state, endpoint, options, remaining),
        ) {
            voters.insert(endpoint.node_id.clone());
            if voters.len() >= quorum_size {
                break;
            }
        }
        if index + 1 >= targets.len() {
            break;
        }
    }
    voters.len()
}

pub async fn cluster_reachable_voter_count_async_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterVoterReachabilityOptions,
    host: &mut impl ClusterVoterReachabilityAsyncHost,
) -> Result<usize, NetworkError> {
    let quorum_size = sinomo_kv::cluster_kv_raft_quorum_size_for_state(state);
    let mut voters = BTreeSet::new();
    if sinomo_kv::cluster_kv_raft_local_is_voter(state) {
        voters.insert(state.local_node_id.clone());
        if voters.len() >= quorum_size {
            return Ok(voters.len());
        }
    }
    let deadline = Instant::now() + options.timeout;
    let targets = cluster_raft_live_voter_targets(paths, state);
    let graph = ClusterConnectivityGraph::read_for_local(paths, state);
    for (index, endpoint) in targets.iter().enumerate() {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            break;
        }
        if host
            .probe_voter_async(
                endpoint,
                cluster_voter_probe_timeout(&graph, state, endpoint, options, remaining),
            )
            .await?
        {
            voters.insert(endpoint.node_id.clone());
            if voters.len() >= quorum_size {
                break;
            }
        }
        if index + 1 >= targets.len() {
            break;
        }
    }
    Ok(voters.len())
}

fn cluster_voter_probe_timeout(
    graph: &ClusterConnectivityGraph,
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    options: ClusterVoterReachabilityOptions,
    remaining: Duration,
) -> Duration {
    if crate::cluster_route_next_hop(graph, &state.local_node_id, &endpoint.node_id).is_some()
        && crate::cluster_endpoint_prefers_relay_with_graph(graph, state, endpoint)
    {
        options.command_timeout(endpoint, remaining)
    } else {
        options.fair_command_timeout(endpoint, remaining)
    }
}

pub fn cluster_has_local_quorum_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterVoterReachabilityOptions,
    host: &mut impl ClusterVoterReachabilityHost,
) -> bool {
    cluster_reachable_voter_count_with_host(paths, state, options, host)
        >= sinomo_kv::cluster_kv_raft_quorum_size_for_state(state)
}

pub async fn cluster_has_local_quorum_async_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterVoterReachabilityOptions,
    host: &mut impl ClusterVoterReachabilityAsyncHost,
) -> Result<bool, NetworkError> {
    Ok(
        cluster_reachable_voter_count_async_with_host(paths, state, options, host).await?
            >= sinomo_kv::cluster_kv_raft_quorum_size_for_state(state),
    )
}

pub fn elect_cluster_raft_leader_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterRaftLeaderElectionOptions,
    host: &mut impl ClusterRaftVoteTransportHost,
) -> Result<bool, NetworkError> {
    let voter_ids = sinomo_kv::cluster_kv_raft_voter_ids(state);
    let remote_vote_targets = cluster_raft_live_voter_targets(paths, state)
        .into_iter()
        .filter(|endpoint| {
            endpoint.node_id != state.local_node_id && voter_ids.contains(&endpoint.node_id)
        })
        .collect::<Vec<_>>();
    let remote_vote_target_ids = remote_vote_targets
        .iter()
        .map(|endpoint| endpoint.node_id.clone())
        .collect::<Vec<_>>();
    let mut election_host = NetworkClusterKvRaftLeaderElectionHost::new(
        paths,
        state,
        options,
        remote_vote_targets,
        host,
    );
    sinomo_kv::elect_cluster_kv_raft_leader_with_host(
        &state.local_node_id,
        voter_ids,
        remote_vote_target_ids,
        options.kv_options(),
        &mut election_host,
    )
}

pub async fn elect_cluster_raft_leader_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterRaftLeaderElectionOptions,
    host: &mut (impl ClusterRaftVoteTransportAsyncHost + Send),
) -> Result<bool, NetworkError> {
    let voter_ids = sinomo_kv::cluster_kv_raft_voter_ids(state);
    let remote_vote_targets = cluster_raft_live_voter_targets_async(runtime, paths, state)
        .await?
        .into_iter()
        .filter(|endpoint| {
            endpoint.node_id != state.local_node_id && voter_ids.contains(&endpoint.node_id)
        })
        .collect::<Vec<_>>();
    let remote_vote_target_ids = remote_vote_targets
        .iter()
        .map(|endpoint| endpoint.node_id.clone())
        .collect::<Vec<_>>();
    let mut election_host = NetworkClusterKvRaftLeaderElectionAsyncHost::new(
        runtime,
        paths,
        state,
        options,
        remote_vote_targets,
        host,
    );
    sinomo_kv::elect_cluster_kv_raft_leader_async_with_host(
        &state.local_node_id,
        voter_ids,
        remote_vote_target_ids,
        options.kv_options(),
        &mut election_host,
    )
    .await
}

struct NetworkClusterKvTxnQuorumHost<'a, H> {
    paths: &'a ClusterPaths,
    options: ClusterKvTxnQuorumOptions,
    host: &'a mut H,
}

impl<H> sinomo_kv::ClusterKvTxnQuorumHost for NetworkClusterKvTxnQuorumHost<'_, H>
where
    H: ClusterKvTxnQuorumHost,
{
    type Error = NetworkError;

    fn refresh_current_cluster_kv_store(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<(), Self::Error> {
        self.host
            .refresh_current_cluster_kv_store(self.paths, state)
    }

    fn local_is_raft_leader(&mut self, state: &ClusterBootstrapState) -> Result<bool, Self::Error> {
        self.host.local_is_raft_leader(self.paths, state)
    }

    fn forward_cluster_kv_txn_to_voter(
        &mut self,
        state: &ClusterBootstrapState,
        txn: &KvTxn,
    ) -> Result<Option<sinomo_kv::ClusterKvTxnCommitResult>, Self::Error> {
        self.host.forward_cluster_kv_txn_to_voter(
            self.paths,
            state,
            txn,
            self.options.forward_options,
        )
    }

    fn elect_cluster_raft_leader(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<bool, Self::Error> {
        self.host
            .elect_cluster_raft_leader(self.paths, state, self.options.leader_election_options)
    }

    fn reachable_voter_count(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<usize, Self::Error> {
        Ok(self.host.reachable_voter_count(
            self.paths,
            state,
            self.options.voter_reachability_options,
        ))
    }

    fn semantic_txn_no_effect_response(
        &mut self,
        txn: &KvTxn,
    ) -> Result<Option<sinomo_kv::ClusterKvTxnCommitResult>, Self::Error> {
        Ok(
            cluster_semantic_txn_no_effect_response(self.paths, txn, OffsetDateTime::now_utc())?
                .map(
                    |(snapshot, txn_response)| sinomo_kv::ClusterKvTxnCommitResult {
                        snapshot,
                        txn_response: Some(txn_response),
                    },
                ),
        )
    }

    fn commit_cluster_raft_txn_with_quorum(
        &mut self,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<sinomo_kv::ClusterKvRaftQuorumCommitResult, Self::Error> {
        self.host.commit_cluster_raft_command_with_quorum(
            self.paths,
            state,
            KvCommand::Txn(txn),
            ClusterRaftQuorumCommitOptions {
                timeout: self.options.raft_append_timeout,
            },
        )
    }

    fn cluster_read_only_error(&mut self, detail: String) -> Self::Error {
        NetworkError::ClusterReadOnly { detail }
    }
}

pub fn commit_cluster_kv_txn_with_quorum_response_with_host(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    txn: KvTxn,
    options: ClusterKvTxnQuorumOptions,
    host: &mut impl ClusterKvTxnQuorumHost,
) -> Result<ClusterKvTxnForwardResult, NetworkError> {
    let allow_forwarding = options.allow_forwarding;
    let mut kv_host = NetworkClusterKvTxnQuorumHost {
        paths,
        options,
        host,
    };
    sinomo_kv::commit_cluster_kv_txn_with_quorum_with_host(
        state,
        txn,
        sinomo_kv::ClusterKvTxnQuorumDriverOptions { allow_forwarding },
        &mut kv_host,
    )
}

#[cfg(test)]
pub fn handle_cluster_kv_txn_request_with_host(
    paths: &ClusterPaths,
    txn: KvTxn,
    options: ClusterKvTxnQuorumOptions,
    host: &mut impl ClusterKvTxnQuorumHost,
) -> Result<KvTxnResponse, NetworkError> {
    let mut state = read_cluster_state_without_kv(paths).map_err(|error| match error {
        NetworkError::ClusterOrphan => NetworkError::ClusterReadOnly {
            detail: "local node has no cluster state".to_string(),
        },
        other => other,
    })?;
    let result = commit_cluster_kv_txn_with_quorum_response_with_host(
        paths, &mut state, txn, options, host,
    )?;
    write_imported_cluster_kv_snapshot_if_changed(paths, &result.snapshot)?;
    Ok(result.txn_response.unwrap_or(KvTxnResponse {
        succeeded: true,
        revision: result.snapshot.revision,
        responses: Vec::new(),
    }))
}

pub(crate) async fn locked_cluster_raft_known_leader_id_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Option<NodeId>, NetworkError> {
    let paths = paths.clone();
    let state = state.clone();
    runtime
        .spawn_blocking(move || locked_cluster_raft_known_leader_id(&paths, &state))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster raft known leader".to_string(),
            detail: format!("known leader task failed: {source}"),
        })?
}

async fn locked_cluster_raft_local_is_leader_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<bool, NetworkError> {
    let paths = paths.clone();
    let state = state.clone();
    runtime
        .spawn_blocking(move || locked_cluster_raft_local_is_leader(&paths, &state))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster raft local leader".to_string(),
            detail: format!("local leader task failed: {source}"),
        })?
}

pub(crate) async fn cluster_node_blocks_leader_preference_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    node_id: &NodeId,
) -> Result<bool, NetworkError> {
    let paths = paths.clone();
    let state = state.clone();
    let node_id = node_id.clone();
    runtime
        .spawn_blocking(move || {
            Ok(cluster_node_blocks_leader_preference(
                &paths, &state, &node_id,
            ))
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster raft leader preference".to_string(),
            detail: format!("leader preference task failed: {source}"),
        })?
}

async fn cluster_semantic_txn_no_effect_response_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    txn: &KvTxn,
    now: OffsetDateTime,
) -> Result<Option<(KvSnapshot, KvTxnResponse)>, NetworkError> {
    let paths = paths.clone();
    let txn = txn.clone();
    runtime
        .spawn_blocking(move || cluster_semantic_txn_no_effect_response(&paths, &txn, now))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster semantic txn no-effect check".to_string(),
            detail: format!("semantic txn no-effect task failed: {source}"),
        })?
}

async fn write_imported_cluster_kv_snapshot_if_changed_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    snapshot: &KvSnapshot,
) -> Result<(), NetworkError> {
    let paths = paths.clone();
    let snapshot = snapshot.clone();
    runtime
        .spawn_blocking(move || write_imported_cluster_kv_snapshot_if_changed(&paths, &snapshot))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster kv snapshot import".to_string(),
            detail: format!("cluster kv snapshot import task failed: {source}"),
        })?
        .map(|_| ())
}

struct NetworkClusterKvTxnQuorumAsyncHost<'a, H> {
    runtime: &'a tokio::runtime::Handle,
    paths: &'a ClusterPaths,
    options: ClusterKvTxnQuorumOptions,
    host: &'a mut H,
}

impl<H> sinomo_kv::ClusterKvTxnQuorumAsyncHost for NetworkClusterKvTxnQuorumAsyncHost<'_, H>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    type Error = NetworkError;

    fn local_is_raft_leader_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, bool, Self::Error> {
        let runtime = self.runtime;
        let paths = self.paths;
        Box::pin(
            async move { locked_cluster_raft_local_is_leader_async(runtime, paths, state).await },
        )
    }

    fn forward_cluster_kv_txn_to_voter_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        txn: &'a KvTxn,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<
        'a,
        Option<sinomo_kv::ClusterKvTxnCommitResult>,
        Self::Error,
    > {
        let runtime = self.runtime;
        let paths = self.paths;
        let options = self.options.forward_options;
        let host = &mut *self.host;
        Box::pin(async move {
            forward_cluster_kv_txn_to_voter_async_with_host(
                runtime, paths, state, txn, options, host,
            )
            .await
        })
    }

    fn elect_cluster_raft_leader_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, bool, Self::Error> {
        let runtime = self.runtime;
        let paths = self.paths;
        let options = self.options.leader_election_options;
        let host = &mut *self.host;
        Box::pin(async move {
            elect_cluster_raft_leader_async_with_host(runtime, paths, state, options, host).await
        })
    }

    fn reachable_voter_count_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, usize, Self::Error> {
        let paths = self.paths;
        let options = self.options.voter_reachability_options;
        let host = &mut *self.host;
        Box::pin(async move {
            cluster_reachable_voter_count_async_with_host(paths, state, options, host).await
        })
    }

    fn semantic_txn_no_effect_response_async<'a>(
        &'a mut self,
        txn: &'a KvTxn,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<
        'a,
        Option<sinomo_kv::ClusterKvTxnCommitResult>,
        Self::Error,
    > {
        let runtime = self.runtime;
        let paths = self.paths;
        Box::pin(async move {
            Ok(cluster_semantic_txn_no_effect_response_async(
                runtime,
                paths,
                txn,
                OffsetDateTime::now_utc(),
            )
            .await?
            .map(
                |(snapshot, txn_response)| sinomo_kv::ClusterKvTxnCommitResult {
                    snapshot,
                    txn_response: Some(txn_response),
                },
            ))
        })
    }

    fn commit_cluster_raft_txn_with_quorum_async<'a>(
        &'a mut self,
        state: &'a mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<
        'a,
        sinomo_kv::ClusterKvRaftQuorumCommitResult,
        Self::Error,
    > {
        let runtime = self.runtime;
        let paths = self.paths;
        let options = self.options;
        let host = &mut *self.host;
        Box::pin(async move {
            commit_cluster_raft_command_with_quorum_async_with_host(
                runtime,
                paths,
                state,
                KvCommand::Txn(txn),
                ClusterRaftQuorumCommitOptions {
                    timeout: options.raft_append_timeout,
                },
                options.raft_append_replication_options,
                host,
            )
            .await
        })
    }

    fn cluster_read_only_error(&mut self, detail: String) -> Self::Error {
        NetworkError::ClusterReadOnly { detail }
    }
}

pub async fn commit_cluster_kv_txn_with_quorum_response_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    txn: KvTxn,
    options: ClusterKvTxnQuorumOptions,
    host: &mut H,
) -> Result<ClusterKvTxnForwardResult, NetworkError>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    let allow_forwarding = options.allow_forwarding;
    let mut kv_host = NetworkClusterKvTxnQuorumAsyncHost {
        runtime,
        paths,
        options,
        host,
    };
    let result = sinomo_kv::commit_cluster_kv_txn_with_quorum_async_with_host(
        state,
        txn,
        sinomo_kv::ClusterKvTxnQuorumDriverOptions { allow_forwarding },
        &mut kv_host,
    )
    .await?;
    // A forwarded commit observes the voter's post-commit snapshot; import it
    // when it advances the local view so the forwarding node reads its own
    // write immediately. The committed-append fan-out runs on the voter and
    // can miss this node (cold formation: no relay route back yet), and
    // re-committing the same payload is a semantic no-effect that triggers no
    // new fan-out — without this import a forwarded write can stay invisible
    // locally until the periodic snapshot pull.
    let import_paths = paths.clone();
    let import_snapshot = result.snapshot.clone();
    runtime
        .spawn_blocking(move || {
            if imported_snapshot_advances_local_kv(&import_paths, &import_snapshot) {
                write_imported_cluster_kv_snapshot_if_changed(&import_paths, &import_snapshot)?;
            }
            Ok::<_, NetworkError>(())
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster kv snapshot import".to_string(),
            detail: format!("cluster kv snapshot import task failed: {source}"),
        })??;
    Ok(result)
}

pub async fn handle_cluster_kv_txn_request_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    txn: KvTxn,
    options: ClusterKvTxnQuorumOptions,
    host: &mut H,
) -> Result<KvTxnResponse, NetworkError>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    let mut state = read_cluster_state_without_kv_async(runtime, paths)
        .await
        .map_err(|error| match error {
            NetworkError::ClusterOrphan => NetworkError::ClusterReadOnly {
                detail: "local node has no cluster state".to_string(),
            },
            other => other,
        })?;
    let result = commit_cluster_kv_txn_with_quorum_response_async_with_host(
        runtime, paths, &mut state, txn, options, host,
    )
    .await?;
    write_imported_cluster_kv_snapshot_if_changed_async(runtime, paths, &result.snapshot).await?;
    Ok(result.txn_response.unwrap_or(KvTxnResponse {
        succeeded: true,
        revision: result.snapshot.revision,
        responses: Vec::new(),
    }))
}

/// Replication targets for the committed-command fan-out: every authorized
/// main-daemon member (learners included), connectivity-aware. Learners are
/// pure raft followers — pushing committed appends to them on commit is the
/// event-driven convergence path; nothing is sent when nothing commits.
pub fn cluster_raft_member_replication_targets(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    ClusterEndpointSelector::new(paths, state)
        .select(ClusterEndpointSelectionPurpose::RaftMemberReplication)
}

pub async fn cluster_raft_member_replication_targets_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    let paths = paths.clone();
    let state = state.clone();
    runtime
        .spawn_blocking(move || cluster_raft_member_replication_targets(&paths, &state))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster raft member replication target selection".to_string(),
            detail: format!("member replication target selection task failed: {source}"),
        })
}

pub(crate) async fn cluster_raft_live_voter_targets_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    let paths = paths.clone();
    let state = state.clone();
    runtime
        .spawn_blocking(move || {
            Ok(ClusterEndpointSelector::new(&paths, &state)
                .select(ClusterEndpointSelectionPurpose::LiveVoterCommand))
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster raft voter target selection".to_string(),
            detail: format!("cluster raft voter target selection task failed: {source}"),
        })?
}

pub fn cluster_node_blocks_leader_preference(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    node_id: &NodeId,
) -> bool {
    // A node blocks leader preference if it has gracefully reported itself
    // offline, or if active connectivity probes can no longer reach it. The
    // self-reported `observed_at` is not consulted: it is no longer refreshed on
    // a timer, so an idle-but-healthy node must not be deprioritized.
    let node_statuses = cluster_node_status_map_from_kv(paths);
    if node_statuses
        .get(node_id)
        .is_some_and(|status| status.availability == ClusterNodeAvailability::Offline)
    {
        return true;
    }
    state
        .members
        .iter()
        .find(|member| member.node_id == *node_id)
        .is_some_and(|member| cluster_member_unreachable_via_connectivity(paths, state, member))
}

/// True when `member` has probeable endpoints but the local node's active
/// connectivity probes currently mark every one of them unreachable.
///
/// This is the probe-based liveness signal: unlike the self-reported node-status
/// `observed_at` (which is no longer periodically refreshed), it reflects whether
/// we can actually reach the peer right now. A member we have never probed (no
/// connectivity record yet) is not reported unreachable.
pub fn cluster_member_unreachable_via_connectivity(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
) -> bool {
    if cluster_member_is_local_process(state, member) {
        return false;
    }
    let graph = ClusterConnectivityGraph::read_for_local(paths, state);
    let mut has_probeable_endpoint = false;
    for endpoint in &member.endpoints {
        let probeable = endpoint.privacy != ClusterEndpointPrivacy::LocalOnly
            && matches!(
                endpoint.transport,
                ClusterTransportKind::LanTcp
                    | ClusterTransportKind::Loopback
                    | ClusterTransportKind::Iroh
            );
        if !probeable {
            continue;
        }
        has_probeable_endpoint = true;
        if graph.unavailability_rank(endpoint) == 0 {
            // At least one probeable endpoint is not marked unreachable.
            return false;
        }
    }
    has_probeable_endpoint
}

pub fn cluster_bootstrap_state_stable_eq(
    left: &ClusterBootstrapState,
    right: &ClusterBootstrapState,
) -> bool {
    left.cluster_id == right.cluster_id
        && left.cluster_name == right.cluster_name
        && left.local_node_id == right.local_node_id
        && left.local_node_display_name == right.local_node_display_name
        && left.term == right.term
        && left.initialized_at == right.initialized_at
        && left.members.len() == right.members.len()
        && left
            .members
            .iter()
            .zip(right.members.iter())
            .all(|(left, right)| cluster_member_stable_eq(left, right))
}

pub fn cluster_member_stable_eq(left: &ClusterMemberRecord, right: &ClusterMemberRecord) -> bool {
    left.cluster_id == right.cluster_id
        && left.node_id == right.node_id
        && left.display_name == right.display_name
        && left.node_role == right.node_role
        && left.role == right.role
        && left.state == right.state
        && left.trust_domain == right.trust_domain
        && left.node_public_key == right.node_public_key
        && left.certificate_id == right.certificate_id
        && left.capabilities == right.capabilities
        && left.allowed_capabilities == right.allowed_capabilities
        && cluster_endpoint_list_stable_eq(&left.endpoints, &right.endpoints)
        && left.joined_at == right.joined_at
}

/// The local node's current cluster KV snapshot revision, if one is readable;
/// `None` asks the voter to ship its full snapshot back.
pub(crate) fn local_cluster_kv_snapshot_revision(paths: &ClusterPaths) -> Option<KvRevision> {
    read_cluster_kv_current_snapshot(paths)
        .ok()
        .map(|snapshot| snapshot.revision)
}

/// Direction guard for snapshot imports: under raft's lag-only invariant an
/// imported snapshot must move local history FORWARD. A responder that lags
/// the local node (a stale bridge answering a fresher publisher) must never
/// roll the local KV back.
pub(crate) fn imported_snapshot_advances_local_kv(
    paths: &ClusterPaths,
    snapshot: &KvSnapshot,
) -> bool {
    match local_cluster_kv_snapshot_revision(paths) {
        Some(local) => snapshot.revision > local,
        None => true,
    }
}

pub(crate) fn push_bounded_txn_success_op(
    ops: &mut Vec<KvOp>,
    op: KvOp,
) -> Result<(), NetworkError> {
    if ops.len() >= sinomo_kv::MAX_KV_TXN_OPS_PER_BRANCH {
        return Err(NetworkError::Kv(KvError::TxnLimit {
            compares: 0,
            success_ops: ops.len().saturating_add(1),
            failure_ops: 0,
            max_compares: sinomo_kv::MAX_KV_TXN_COMPARES,
            max_ops_per_branch: sinomo_kv::MAX_KV_TXN_OPS_PER_BRANCH,
        }));
    }
    ops.push(op);
    Ok(())
}

pub(crate) fn append_cluster_raft_ops_in_batches(
    paths: &ClusterPaths,
    node_id: &NodeId,
    ops: Vec<KvOp>,
) -> Result<KvSnapshot, NetworkError> {
    let mut ops = ops.into_iter();
    let mut latest = None;
    loop {
        let chunk = ops
            .by_ref()
            .take(sinomo_kv::MAX_KV_MUTATION_EVENTS_PER_COMMAND)
            .collect::<Vec<_>>();
        if chunk.is_empty() {
            break;
        }
        latest = Some(append_cluster_raft_command(
            paths,
            node_id,
            KvCommand::Txn(KvTxn {
                compares: Vec::new(),
                success: chunk,
                failure: Vec::new(),
            }),
        )?);
    }
    latest.ok_or_else(|| NetworkError::StorageIo {
        path: paths.kv_snapshot.display().to_string(),
        detail: "cannot append an empty batch of cluster kv operations".to_string(),
    })
}

#[cfg(test)]
pub fn sync_cluster_certificate_to_kv(
    paths: &ClusterPaths,
    certificate: &ClusterCertificate,
) -> Result<KvSnapshot, NetworkError> {
    let state = read_cluster_state_without_kv(paths)?;
    put_cluster_kv_value(
        paths,
        &state.local_node_id,
        cluster_certificate_key(certificate),
        certificate,
    )
}

#[cfg(test)]
fn put_cluster_kv_value<T: Serialize + ?Sized>(
    paths: &ClusterPaths,
    node_id: &NodeId,
    key: String,
    value: &T,
) -> Result<KvSnapshot, NetworkError> {
    let op = KvOp::put_serde(key, value, None)?;
    if cluster_kv_put_value_is_noop(paths, &op)? {
        return read_cluster_kv_snapshot(paths);
    }
    append_cluster_raft_command(paths, node_id, KvCommand::Op(op))
}

#[cfg(test)]
fn cluster_kv_put_value_is_noop(paths: &ClusterPaths, op: &KvOp) -> Result<bool, NetworkError> {
    let KvOp::Put {
        key,
        value,
        lease_id,
    } = op
    else {
        return Ok(false);
    };
    let Some(snapshot) = read_cluster_kv_cached_snapshot_if_present(paths)? else {
        return Ok(false);
    };
    let Some(entry) = snapshot.get(key) else {
        return Ok(false);
    };
    Ok(entry.value.as_slice() == value.as_slice() && entry.lease_id == *lease_id)
}

pub fn merge_cluster_state_from_peer(
    local: &mut ClusterBootstrapState,
    peer: ClusterBootstrapState,
) -> bool {
    if peer.cluster_id != local.cluster_id {
        return false;
    }
    let mut changed = false;
    for member in peer.members {
        changed |= merge_member_record(local, member);
    }
    if peer.kv_revision > local.kv_revision {
        local.kv_revision = peer.kv_revision;
    }
    if peer.updated_at > local.updated_at {
        local.updated_at = peer.updated_at;
    }
    changed
}

pub fn merge_member_record(
    local: &mut ClusterBootstrapState,
    incoming: ClusterMemberRecord,
) -> bool {
    if incoming.cluster_id != local.cluster_id {
        return false;
    }
    if let Some(existing) = local
        .members
        .iter_mut()
        .find(|member| member.node_id == incoming.node_id)
    {
        let mut changed = false;
        if incoming.updated_at > existing.updated_at {
            *existing = incoming;
            cleanup_member_iroh_endpoints(existing);
            return true;
        }
        for endpoint in incoming.endpoints {
            changed |= upsert_member_endpoint(existing, endpoint);
        }
        changed |= cleanup_member_iroh_endpoints(existing);
        changed
    } else {
        let mut incoming = incoming;
        cleanup_member_iroh_endpoints(&mut incoming);
        local.members.push(incoming);
        true
    }
}

pub fn upsert_member_record(state: &mut ClusterBootstrapState, member: ClusterMemberRecord) {
    if let Some(existing) = state
        .members
        .iter_mut()
        .find(|existing| existing.node_id == member.node_id)
    {
        *existing = member;
    } else {
        state.members.push(member);
    }
}
