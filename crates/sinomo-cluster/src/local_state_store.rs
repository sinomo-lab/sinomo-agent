use crate::kv_local_store::ClusterKvPathsExt;
#[cfg(any(test, feature = "test-support"))]
use crate::kv_local_store::invalidate_cluster_kv_reader_node_id_cache;
#[allow(unused_imports)]
use crate::kv_local_store::kv_err;
use crate::kv_local_store::read_cluster_kv_current_snapshot_arc;
use crate::kv_local_store::record_current_cluster_kv_reader_node_id_cache;
use crate::kv_read_model::read_cluster_members_from_kv;
use crate::local_membership::{
    locally_learned_remote_route_endpoints, merge_cluster_join_registration,
    preserve_locally_learned_remote_route_endpoints,
};
use crate::member_store::merge_member_record;
use fs2::FileExt;
use sinomo_base::{
    ClusterBootstrapState, ClusterMemberRole, ClusterMemberState, DAEMON_MAIN_RUN_CAPABILITY,
};
use sinomo_kv::ClusterKvRaftConfigRecord;
use sinomo_network::ClusterPaths;
use sinomo_network::NetworkError;
#[cfg(any(test, feature = "test-support"))]
use sinomo_network::remove_optional_file;
use sinomo_network::{ClusterFileMark, cluster_file_mark};
use sinomo_network::{
    cleanup_cluster_endpoint_duplicates, cleanup_member_endpoint_duplicates, upsert_member_endpoint,
};
use sinomo_network::{read_optional_object_file, write_object_file_with_semantic_skip};
use sinomo_node::{
    cluster_bootstrap_state_stable_eq, cluster_node_is_local_process, upsert_member_record,
};
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

const MAX_CLUSTER_STATE_CACHE_ENTRIES: usize = 128;
const CLUSTER_STATE_CACHE_VALIDATION_LEASE: Duration = Duration::from_millis(250);

static CLUSTER_STATE_CACHE_CLOCK: AtomicU64 = AtomicU64::new(1);
static CLUSTER_STATES: LazyLock<Mutex<HashMap<PathBuf, ClusterStateCacheEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static LOCAL_CLUSTER_STATES: LazyLock<Mutex<HashMap<PathBuf, LocalClusterStateCacheEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Clone)]
struct ClusterStateCacheEntry {
    mark: ClusterStateCacheMark,
    state: Arc<ClusterBootstrapState>,
    validated_at: Instant,
    last_used: u64,
}

#[derive(Clone)]
struct LocalClusterStateCacheEntry {
    mark: ClusterFileMark,
    state: Arc<ClusterBootstrapState>,
    validated_at: Instant,
    last_used: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ClusterStateCacheMark {
    state: ClusterFileMark,
    kv_snapshot: ClusterFileMark,
}

fn cluster_state_cache_tick() -> u64 {
    CLUSTER_STATE_CACHE_CLOCK.fetch_add(1, Ordering::Relaxed)
}

fn cluster_state_cache_mark(paths: &ClusterPaths) -> Option<ClusterStateCacheMark> {
    Some(ClusterStateCacheMark {
        state: cluster_file_mark(&paths.state)?,
        kv_snapshot: cluster_file_mark(&paths.kv_paths().snapshot)?,
    })
}

fn read_cluster_state_from_fresh_cache(
    paths: &ClusterPaths,
    checked_at: Instant,
) -> Option<Arc<ClusterBootstrapState>> {
    let mut cache = CLUSTER_STATES.lock().ok()?;
    let entry = cache.get_mut(&paths.root)?;
    if checked_at.saturating_duration_since(entry.validated_at)
        >= CLUSTER_STATE_CACHE_VALIDATION_LEASE
    {
        return None;
    }
    entry.last_used = cluster_state_cache_tick();
    Some(Arc::clone(&entry.state))
}

fn read_cluster_state_from_validated_cache(
    paths: &ClusterPaths,
    mark: Option<ClusterStateCacheMark>,
) -> Option<Arc<ClusterBootstrapState>> {
    let mark = mark?;
    let mut cache = CLUSTER_STATES.lock().ok()?;
    if cache.get(&paths.root)?.mark != mark {
        cache.remove(&paths.root);
        return None;
    }
    let entry = cache.get_mut(&paths.root)?;
    // Validation may have raced another reader that installed a newer entry
    // after `checked_at` was captured. Never move the lease timestamp
    // backwards in that case.
    entry.validated_at = Instant::now();
    entry.last_used = cluster_state_cache_tick();
    Some(Arc::clone(&entry.state))
}

fn record_cluster_state_cache(
    paths: &ClusterPaths,
    mark: ClusterStateCacheMark,
    state: Arc<ClusterBootstrapState>,
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
            validated_at: checked_at,
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

fn read_local_cluster_state_from_fresh_cache(
    paths: &ClusterPaths,
    checked_at: Instant,
) -> Option<Arc<ClusterBootstrapState>> {
    let mut cache = LOCAL_CLUSTER_STATES.lock().ok()?;
    let entry = cache.get_mut(&paths.root)?;
    if checked_at.saturating_duration_since(entry.validated_at)
        >= CLUSTER_STATE_CACHE_VALIDATION_LEASE
    {
        return None;
    }
    entry.last_used = cluster_state_cache_tick();
    Some(Arc::clone(&entry.state))
}

fn read_local_cluster_state_from_validated_cache(
    paths: &ClusterPaths,
    mark: Option<ClusterFileMark>,
) -> Option<Arc<ClusterBootstrapState>> {
    let mark = mark?;
    let mut cache = LOCAL_CLUSTER_STATES.lock().ok()?;
    if cache.get(&paths.root)?.mark != mark {
        cache.remove(&paths.root);
        return None;
    }
    let entry = cache.get_mut(&paths.root)?;
    entry.validated_at = Instant::now();
    entry.last_used = cluster_state_cache_tick();
    Some(Arc::clone(&entry.state))
}

fn record_local_cluster_state_cache(
    paths: &ClusterPaths,
    mark: ClusterFileMark,
    state: Arc<ClusterBootstrapState>,
    checked_at: Instant,
) {
    let Ok(mut cache) = LOCAL_CLUSTER_STATES.lock() else {
        return;
    };
    if !cache.contains_key(&paths.root) && cache.len() >= MAX_CLUSTER_STATE_CACHE_ENTRIES {
        let eviction_candidate = cache
            .iter()
            .min_by_key(|(_, entry)| entry.last_used)
            .map(|(path, _)| path.clone());
        if let Some(path) = eviction_candidate {
            cache.remove(&path);
        }
    }
    cache.insert(
        paths.root.clone(),
        LocalClusterStateCacheEntry {
            mark,
            state,
            validated_at: checked_at,
            last_used: cluster_state_cache_tick(),
        },
    );
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
        let mut local_process_members = Vec::new();
        for member in state.members.iter().filter(|member| {
            cluster_node_is_local_process(&state, &member.node_id) && member.state.is_authorized()
        }) {
            sinomo_network::try_reserve_vec(
                &mut local_process_members,
                1,
                "collect local process members",
            )?;
            local_process_members.push(member.clone());
        }
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
                cleanup_member_endpoint_duplicates(&mut local_member);
                upsert_member_record(&mut state, local_member);
            } else {
                upsert_member_record(&mut state, local_member);
            }
        }
        state
            .members
            .sort_by(|left, right| left.node_id.cmp(&right.node_id));
        cleanup_cluster_endpoint_duplicates(&mut state);
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
            cleanup_member_endpoint_duplicates(&mut local_member);
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
    cleanup_cluster_endpoint_duplicates(&mut state);
    Ok(state)
}

/// Reads the locally applied committed Raft configuration and projects it onto
/// the routing/member view used by consensus operations.
///
/// `ClusterBootstrapState` remains the topology and identity carrier, but its
/// member role fields can lag a committed `/cluster/raft/config` transition.
/// Consensus must never silently fall back to those stale roles: once a config
/// record exists, it is the effective voter/learner set until another config
/// entry commits.
pub fn read_cluster_consensus_state(
    paths: &ClusterPaths,
) -> Result<ClusterBootstrapState, NetworkError> {
    let mut state = read_cluster_state(paths)?;
    project_committed_cluster_raft_config(paths, &mut state)?;
    Ok(state)
}

/// Overlays the locally applied committed config on an existing topology
/// snapshot. This is used at consensus boundaries so a caller cannot smuggle a
/// proposed member-role mutation into the quorum calculation.
pub fn project_committed_cluster_raft_config(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
) -> Result<bool, NetworkError> {
    let snapshot = match read_cluster_kv_current_snapshot_arc(paths) {
        Ok(snapshot) => snapshot,
        Err(NetworkError::ClusterOrphan) => return Ok(false),
        Err(error) => return Err(error),
    };
    let Some(entry) = snapshot.get(&sinomo_kv::cluster_kv_raft_config_key()) else {
        return Ok(false);
    };
    let config: ClusterKvRaftConfigRecord = entry.decode().map_err(kv_err)?;
    if config.cluster_id != state.cluster_id {
        return Err(NetworkError::InvalidClusterCommand {
            detail: format!(
                "committed Raft config cluster `{}` does not match local cluster `{}`",
                config.cluster_id, state.cluster_id
            ),
        });
    }

    for voter in &config.voters {
        if config.learners.contains(voter) || config.non_voting.contains(voter) {
            return Err(NetworkError::InvalidClusterCommand {
                detail: format!(
                    "committed Raft config assigns node `{voter}` to overlapping roles"
                ),
            });
        }
    }
    for learner in &config.learners {
        if config.non_voting.contains(learner) {
            return Err(NetworkError::InvalidClusterCommand {
                detail: format!(
                    "committed Raft config assigns node `{learner}` to overlapping roles"
                ),
            });
        }
    }

    for member in &mut state.members {
        if config.voters.contains(&member.node_id) {
            member.role = ClusterMemberRole::Voter;
            member.state = ClusterMemberState::Voter;
        } else if config.learners.contains(&member.node_id) {
            member.role = ClusterMemberRole::Learner;
            member.state = ClusterMemberState::Learner;
        } else if config.non_voting.contains(&member.node_id) {
            member.role = ClusterMemberRole::NonVoting;
            member.state = ClusterMemberState::NonVoting;
        } else if member.has_raft_vote() {
            // A legacy/local member record must not add a voter that is absent
            // from the committed configuration.
            member.role = ClusterMemberRole::NonVoting;
            member.state = ClusterMemberState::NonVoting;
        }
    }

    for voter in &config.voters {
        let Some(member) = state.members.iter().find(|member| member.node_id == *voter) else {
            return Err(NetworkError::InvalidClusterCommand {
                detail: format!("committed Raft voter `{voter}` has no member identity record"),
            });
        };
        if !member
            .capabilities
            .iter()
            .any(|capability| capability.as_str() == DAEMON_MAIN_RUN_CAPABILITY)
        {
            return Err(NetworkError::InvalidClusterCommand {
                detail: format!(
                    "committed Raft voter `{voter}` lacks capability `{DAEMON_MAIN_RUN_CAPABILITY}`"
                ),
            });
        }
    }

    Ok(true)
}

/// Reads the merged cluster state through a short, syscall-free validation
/// lease and shares the resident snapshot with hot-path callers.
///
/// In-process writers invalidate the entry immediately. Out-of-process file
/// changes become visible no later than the validation lease, when the durable
/// state and KV marks are sampled before reusing the resident snapshot.
pub fn read_cluster_state_cached_shared(
    paths: &ClusterPaths,
) -> Result<Arc<ClusterBootstrapState>, NetworkError> {
    let checked_at = Instant::now();
    if let Some(state) = read_cluster_state_from_fresh_cache(paths, checked_at) {
        return Ok(state);
    }
    let before_mark = cluster_state_cache_mark(paths);
    if let Some(state) = read_cluster_state_from_validated_cache(paths, before_mark) {
        return Ok(state);
    }
    let state = Arc::new(read_cluster_state(paths)?);
    if let Some(mark) = before_mark
        && cluster_state_cache_mark(paths) == Some(mark)
    {
        record_cluster_state_cache(paths, mark, Arc::clone(&state), checked_at);
    }
    Ok(state)
}

pub fn read_cluster_state_cached(
    paths: &ClusterPaths,
) -> Result<ClusterBootstrapState, NetworkError> {
    read_cluster_state_cached_shared(paths).map(|state| state.as_ref().clone())
}

pub(crate) fn invalidate_cluster_state_cache(paths: &ClusterPaths) {
    if let Ok(mut cache) = CLUSTER_STATES.lock() {
        cache.remove(&paths.root);
    }
    if let Ok(mut cache) = LOCAL_CLUSTER_STATES.lock() {
        cache.remove(&paths.root);
    }
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

fn finish_cluster_state_write(paths: &ClusterPaths, state: &ClusterBootstrapState) {
    invalidate_cluster_state_cache(paths);
    sinomo_network::invalidate_cluster_delivery_routing_table_cache(paths);
    record_current_cluster_kv_reader_node_id_cache(paths, state.local_node_id.clone());
}

/// Writes a state while its caller holds the local-state mutation lock.
///
/// Keep this separate from [`write_cluster_state`] so read/modify/write paths
/// can cover their read and their atomic rename with one lock acquisition.
/// Calling the public writer from such a path would recursively acquire the
/// advisory lock and can deadlock on platforms where independent file handles
/// conflict even inside one process.
pub(crate) fn write_cluster_state_unlocked(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<(), NetworkError> {
    write_object_file_with_semantic_skip(&paths.state, state, |existing| {
        cluster_bootstrap_state_stable_eq(existing, state)
    })?;
    finish_cluster_state_write(paths, state);
    Ok(())
}

/// Persists an authoritative projection even when only its KV revision moved.
///
/// The ordinary local-state comparator deliberately ignores revision churn.
/// Accepted joins instead need the revision on disk as a generation fence for
/// delayed local read/modify/write work, so only an exact revision match may
/// take the semantic-skip path here.
fn write_accepted_cluster_state_unlocked(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<(), NetworkError> {
    write_object_file_with_semantic_skip(&paths.state, state, |existing| {
        existing.kv_revision == state.kv_revision
            && cluster_bootstrap_state_stable_eq(existing, state)
    })?;
    finish_cluster_state_write(paths, state);
    Ok(())
}

pub fn write_cluster_state(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<(), NetworkError> {
    let _mutation_lock = acquire_local_cluster_state_mutation_lock(paths)?;
    // A caller may have read this projection before awaiting endpoint or
    // daemon discovery. Once an accepted join has durably advanced the local
    // generation, that delayed writer must not last-rename the old membership
    // over it. Different cluster identities are intentional bootstrap/join
    // transitions and therefore are not compared on this fence.
    match read_optional_object_file::<ClusterBootstrapState>(&paths.state) {
        Ok(Some(existing))
            if existing.cluster_id == state.cluster_id
                && (state.kv_revision, state.term) < (existing.kv_revision, existing.term) =>
        {
            tracing::debug!(
                target: "sinomo.cluster.formation",
                candidate_revision = state.kv_revision,
                candidate_term = state.term,
                persisted_revision = existing.kv_revision,
                persisted_term = existing.term,
                "skipping superseded local cluster state write"
            );
            return Ok(());
        }
        Ok(_) => {}
        Err(error) => {
            // The caller is supplying a complete replacement object. A torn
            // or trailing-byte predecessor cannot provide a trustworthy
            // generation fence, but it also must not make the durable writer
            // incapable of repairing that object through atomic replacement.
            tracing::warn!(
                target: "sinomo.cluster.formation",
                path = %paths.state.display(),
                %error,
                "replacing unreadable local cluster state"
            );
        }
    }
    write_cluster_state_unlocked(paths, state)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AcceptedClusterJoinStatePersistence {
    Applied {
        previous_revision: u64,
        persisted_revision: u64,
    },
    Superseded {
        accepted_revision: u64,
        accepted_term: u64,
        persisted_revision: u64,
        persisted_term: u64,
    },
}

fn local_cluster_state_mutation_lock_path(paths: &ClusterPaths) -> PathBuf {
    let parent = paths.state.parent().unwrap_or_else(|| Path::new("."));
    let file_name = paths
        .state
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("state.sobj");
    parent.join(format!("{file_name}.lock"))
}

/// Serializes local-state read/merge/write transactions.
///
/// Atomic replacement protects readers from torn object files, but it cannot
/// prevent two accepted joins from reading the same state and letting the last
/// rename discard the other join. The main daemon, shadow daemon, and command
/// clients may share this directory, so this must be a cross-process advisory
/// lock rather than only an in-process mutex.
pub(crate) fn acquire_local_cluster_state_mutation_lock(
    paths: &ClusterPaths,
) -> Result<std::fs::File, NetworkError> {
    let lock_path = local_cluster_state_mutation_lock_path(paths);
    if let Some(parent) = lock_path.parent() {
        fs::create_dir_all(parent).map_err(|source| NetworkError::StorageIo {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true);
    #[cfg(unix)]
    options.mode(0o600);
    let lock = options
        .open(&lock_path)
        .map_err(|source| NetworkError::StorageIo {
            path: lock_path.display().to_string(),
            detail: source.to_string(),
        })?;
    let wait_started = Instant::now();
    lock.lock_exclusive()
        .map_err(|source| NetworkError::StorageIo {
            path: lock_path.display().to_string(),
            detail: source.to_string(),
        })?;
    tracing::trace!(
        target: "sinomo.cluster.formation",
        path = %lock_path.display(),
        wait_ms = wait_started.elapsed().as_secs_f64() * 1_000.0,
        "acquired local cluster state mutation lock"
    );
    Ok(lock)
}

/// Merge an accepted join projection without allowing an older response to
/// replace a newer durable membership view.
///
/// The authoritative KV transaction is already fenced separately. This helper
/// protects only the local bootstrap projection, preserving its local identity
/// and locally learned route endpoints through `merge_cluster_join_registration`.
pub(crate) fn persist_accepted_cluster_join_state(
    paths: &ClusterPaths,
    accepted_state: ClusterBootstrapState,
) -> Result<AcceptedClusterJoinStatePersistence, NetworkError> {
    let _mutation_lock = acquire_local_cluster_state_mutation_lock(paths)?;
    let mut local_state = read_cluster_state_without_kv(paths)?;
    if local_state.cluster_id != accepted_state.cluster_id {
        return Err(NetworkError::InvalidClusterCommand {
            detail: format!(
                "accepted cluster {} does not match local cluster {}",
                accepted_state.cluster_id, local_state.cluster_id
            ),
        });
    }
    // Revision is the primary projection generation. For equal revisions, a
    // higher term is the newer authority observation; accepting a lower-term
    // response at that revision could roll membership back after an election.
    if (accepted_state.kv_revision, accepted_state.term)
        < (local_state.kv_revision, local_state.term)
    {
        tracing::trace!(
            target: "sinomo.cluster.formation",
            accepted_revision = accepted_state.kv_revision,
            accepted_term = accepted_state.term,
            persisted_revision = local_state.kv_revision,
            persisted_term = local_state.term,
            "accepted join projection was superseded before local persistence"
        );
        return Ok(AcceptedClusterJoinStatePersistence::Superseded {
            accepted_revision: accepted_state.kv_revision,
            accepted_term: accepted_state.term,
            persisted_revision: local_state.kv_revision,
            persisted_term: local_state.term,
        });
    }

    let previous_revision = local_state.kv_revision;
    let previous_term = local_state.term;
    merge_cluster_join_registration(&mut local_state, accepted_state);
    local_state.term = local_state.term.max(previous_term);
    local_state.kv_revision = local_state.kv_revision.max(previous_revision);
    let persisted_revision = local_state.kv_revision;
    write_accepted_cluster_state_unlocked(paths, &local_state)?;
    tracing::trace!(
        target: "sinomo.cluster.formation",
        previous_revision,
        persisted_revision,
        persisted_term = local_state.term,
        "persisted accepted join projection"
    );
    Ok(AcceptedClusterJoinStatePersistence::Applied {
        previous_revision,
        persisted_revision,
    })
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

pub fn read_cluster_state_without_kv(
    paths: &ClusterPaths,
) -> Result<ClusterBootstrapState, NetworkError> {
    read_optional_object_file(&paths.state)?.ok_or(NetworkError::ClusterOrphan)
}

/// Reads only the durable local bootstrap state through a short syscall-free
/// validation lease and shares the resident snapshot with request hot paths.
///
/// This deliberately does not merge KV membership. In-process state writers
/// invalidate both state caches immediately; external replacements are
/// observed after at most the shared 250 ms validation lease.
pub fn read_cluster_state_without_kv_cached_shared(
    paths: &ClusterPaths,
) -> Result<Arc<ClusterBootstrapState>, NetworkError> {
    let checked_at = Instant::now();
    if let Some(state) = read_local_cluster_state_from_fresh_cache(paths, checked_at) {
        return Ok(state);
    }
    let before_mark = cluster_file_mark(&paths.state);
    if let Some(state) = read_local_cluster_state_from_validated_cache(paths, before_mark) {
        return Ok(state);
    }
    let state = Arc::new(read_cluster_state_without_kv(paths)?);
    if let Some(mark) = before_mark
        && cluster_file_mark(&paths.state) == Some(mark)
    {
        record_local_cluster_state_cache(paths, mark, Arc::clone(&state), checked_at);
    }
    Ok(state)
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

#[cfg(any(test, feature = "test-support"))]
pub fn remove_local_cluster_state(paths: &ClusterPaths) -> Result<(), NetworkError> {
    if !remove_optional_file(&paths.state)? {
        return Err(NetworkError::ClusterOrphan);
    }
    invalidate_cluster_kv_reader_node_id_cache(&paths.state);
    remove_optional_file(&paths.kv_paths().snapshot)?;
    invalidate_cluster_state_cache(paths);
    sinomo_network::invalidate_cluster_delivery_routing_table_cache(paths);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_base::ClusterMemberRecord;
    use sinomo_local::SinomoDataDir;
    use sinomo_node::{ClusterId, NodeId};
    use std::sync::Barrier;
    use time::OffsetDateTime;

    fn test_state(name: &str) -> ClusterBootstrapState {
        ClusterBootstrapState::initialized_locally(
            ClusterId::from_legacy_named(format!("cluster_{name}")).expect("test cluster id"),
            name,
            NodeId::from_legacy_named(format!("node_{name}")).expect("test node id"),
            "local",
            "pk-local",
            OffsetDateTime::now_utc(),
        )
    }

    fn add_learner(state: &mut ClusterBootstrapState, name: &str) -> NodeId {
        let node_id = NodeId::from_legacy_named(format!("node_{name}")).expect("learner node id");
        let mut member = ClusterMemberRecord::pending_learner(
            state.cluster_id.clone(),
            node_id.clone(),
            name,
            format!("pk-{name}"),
            OffsetDateTime::now_utc(),
        );
        member.state = ClusterMemberState::Learner;
        state.members.push(member);
        state
            .members
            .sort_by(|left, right| left.node_id.cmp(&right.node_id));
        node_id
    }

    #[test]
    fn accepted_join_persistence_rejects_cluster_mismatch() {
        let root = sinomo_testdir::TestDir::new("accepted-join-state-cluster-mismatch");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let local = test_state("accepted_join_local_cluster");
        write_cluster_state(&paths, &local).expect("write local state");

        let error = persist_accepted_cluster_join_state(
            &paths,
            test_state("accepted_join_foreign_cluster"),
        )
        .expect_err("foreign accepted state must be rejected");
        assert!(matches!(
            error,
            NetworkError::InvalidClusterCommand { detail }
                if detail.contains("does not match local cluster")
        ));
        assert_eq!(
            read_cluster_state_without_kv(&paths).expect("unchanged local state"),
            local
        );
    }

    #[test]
    fn accepted_join_persistence_durably_advances_revision_only_projection() {
        let root = sinomo_testdir::TestDir::new("accepted-join-state-revision-only");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let mut initial = test_state("accepted_join_revision_only");
        initial.kv_revision = 10;
        write_cluster_state(&paths, &initial).expect("write initial state");

        let mut accepted = initial.clone();
        accepted.kv_revision = 11;
        assert_eq!(
            persist_accepted_cluster_join_state(&paths, accepted)
                .expect("persist revision-only accepted projection"),
            AcceptedClusterJoinStatePersistence::Applied {
                previous_revision: 10,
                persisted_revision: 11,
            }
        );
        assert_eq!(
            read_cluster_state_without_kv(&paths)
                .expect("read exact accepted projection")
                .kv_revision,
            11,
            "accepted revision is a durable fence, not volatile bookkeeping"
        );
    }

    #[test]
    fn delayed_local_state_write_cannot_overwrite_accepted_join_generation() {
        let root = sinomo_testdir::TestDir::new("accepted-join-state-delayed-writer");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let mut initial = test_state("accepted_join_delayed_writer");
        initial.kv_revision = 10;
        write_cluster_state(&paths, &initial).expect("write initial state");

        // Model a daemon membership refresh that read before endpoint discovery
        // awaited. Its final write occurs only after the join has committed and
        // persisted a larger membership projection.
        let mut delayed = read_cluster_state_without_kv(&paths).expect("read delayed candidate");
        delayed.local_node_display_name = "delayed local refresh".to_string();
        let mut accepted = initial.clone();
        let joined = add_learner(&mut accepted, "accepted_join_during_refresh");
        accepted.kv_revision = 11;
        persist_accepted_cluster_join_state(&paths, accepted).expect("persist accepted join");
        write_cluster_state(&paths, &delayed).expect("skip superseded delayed write");

        let stored = read_cluster_state_without_kv(&paths).expect("read fenced local state");
        assert_eq!(stored.kv_revision, 11);
        assert!(stored.members.iter().any(|member| member.node_id == joined));
        assert_ne!(
            stored.local_node_display_name,
            delayed.local_node_display_name
        );
    }

    #[test]
    fn accepted_join_persistence_does_not_regress_newer_state() {
        let root = sinomo_testdir::TestDir::new("accepted-join-state-no-regression");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let mut newest = test_state("accepted_join_no_regression");
        let first = add_learner(&mut newest, "accepted_join_first");
        let second = add_learner(&mut newest, "accepted_join_second");
        newest.kv_revision = 30;
        newest.term = 7;
        write_cluster_state(&paths, &newest).expect("write newest state");

        let mut older = newest.clone();
        older.local_node_id =
            NodeId::from_legacy_named("node_remote_authority").expect("remote authority id");
        older.local_node_display_name = "remote authority".to_string();
        older.members.retain(|member| member.node_id != second);
        older.term = 6;
        let outcome = persist_accepted_cluster_join_state(&paths, older)
            .expect("ignore superseded accepted state");

        assert_eq!(
            outcome,
            AcceptedClusterJoinStatePersistence::Superseded {
                accepted_revision: 30,
                accepted_term: 6,
                persisted_revision: 30,
                persisted_term: 7,
            }
        );
        let stored = read_cluster_state_without_kv(&paths).expect("stored newest state");
        assert_eq!(stored.local_node_id, newest.local_node_id);
        assert_eq!(
            stored.local_node_display_name,
            newest.local_node_display_name
        );
        assert!(stored.members.iter().any(|member| member.node_id == first));
        assert!(stored.members.iter().any(|member| member.node_id == second));
        assert_eq!(stored.kv_revision, 30);
        assert_eq!(stored.term, 7);

        let mut lower_revision = newest.clone();
        lower_revision
            .members
            .retain(|member| member.node_id != second);
        lower_revision.kv_revision = 29;
        lower_revision.term = 8;
        assert_eq!(
            persist_accepted_cluster_join_state(&paths, lower_revision)
                .expect("ignore lower-revision accepted state"),
            AcceptedClusterJoinStatePersistence::Superseded {
                accepted_revision: 29,
                accepted_term: 8,
                persisted_revision: 30,
                persisted_term: 7,
            },
            "a newer term cannot make an older projection revision current"
        );
    }

    #[test]
    fn concurrent_accepted_join_persistence_keeps_newest_projection() {
        let root = sinomo_testdir::TestDir::new("accepted-join-state-concurrent");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let mut initial = test_state("accepted_join_concurrent");
        initial.kv_revision = 10;
        write_cluster_state(&paths, &initial).expect("write initial state");

        let mut first = initial.clone();
        let first_node = add_learner(&mut first, "accepted_join_concurrent_first");
        first.kv_revision = 11;
        let mut second = first.clone();
        let second_node = add_learner(&mut second, "accepted_join_concurrent_second");
        second.kv_revision = 12;

        let start = Arc::new(Barrier::new(3));
        let first_start = Arc::clone(&start);
        let first_paths = paths.clone();
        let first_thread = std::thread::spawn(move || {
            first_start.wait();
            persist_accepted_cluster_join_state(&first_paths, first)
        });
        let second_start = Arc::clone(&start);
        let second_paths = paths.clone();
        let second_thread = std::thread::spawn(move || {
            second_start.wait();
            persist_accepted_cluster_join_state(&second_paths, second)
        });
        start.wait();
        first_thread
            .join()
            .expect("first persistence thread")
            .expect("persist first accepted state");
        second_thread
            .join()
            .expect("second persistence thread")
            .expect("persist second accepted state");

        let stored = read_cluster_state_without_kv(&paths).expect("stored concurrent state");
        assert_eq!(stored.kv_revision, 12);
        assert!(
            stored
                .members
                .iter()
                .any(|member| member.node_id == first_node)
        );
        assert!(
            stored
                .members
                .iter()
                .any(|member| member.node_id == second_node)
        );
    }

    #[test]
    fn shared_cluster_state_hot_lease_skips_mark_and_decode_until_expiry() {
        let root = sinomo_testdir::TestDir::new("cluster-state-shared-cache-lease");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let initial = test_state("shared_cache_initial");
        sinomo_network::write_object_file(&paths.state, &initial).expect("initial state");

        let first = read_cluster_state_cached_shared(&paths).expect("first cached state");
        let first_mark = CLUSTER_STATES
            .lock()
            .expect("cluster state cache")
            .get(&paths.root)
            .expect("cached state")
            .mark;

        let mut replacement = initial.clone();
        replacement.cluster_name = "shared cache replacement".to_string();
        replacement.updated_at += time::Duration::seconds(1);
        sinomo_network::write_object_file(&paths.state, &replacement)
            .expect("externally replace state");
        assert_ne!(cluster_state_cache_mark(&paths), Some(first_mark));
        CLUSTER_STATES
            .lock()
            .expect("cluster state cache")
            .get_mut(&paths.root)
            .expect("cached state")
            .validated_at = Instant::now();

        let leased = read_cluster_state_cached_shared(&paths).expect("leased cached state");
        assert!(
            Arc::ptr_eq(&first, &leased),
            "the hot lease must not decode a replacement snapshot"
        );
        assert_eq!(
            CLUSTER_STATES
                .lock()
                .expect("cluster state cache")
                .get(&paths.root)
                .expect("cached state")
                .mark,
            first_mark,
            "the hot lease must not sample durable file marks"
        );

        CLUSTER_STATES
            .lock()
            .expect("cluster state cache")
            .get_mut(&paths.root)
            .expect("cached state")
            .validated_at = Instant::now()
            .checked_sub(CLUSTER_STATE_CACHE_VALIDATION_LEASE)
            .expect("backdated validation lease");
        let refreshed = read_cluster_state_cached_shared(&paths).expect("refreshed state");
        assert!(!Arc::ptr_eq(&first, &refreshed));
        assert_eq!(refreshed.cluster_name, replacement.cluster_name);
    }

    #[test]
    fn shared_local_cluster_state_hot_lease_skips_decode_until_expiry() {
        let root = sinomo_testdir::TestDir::new("local-cluster-state-shared-cache-lease");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let initial = test_state("local_shared_cache_initial");
        sinomo_network::write_object_file(&paths.state, &initial).expect("initial state");

        let first =
            read_cluster_state_without_kv_cached_shared(&paths).expect("first cached local state");
        let mut replacement = initial.clone();
        replacement.cluster_name = "local shared cache replacement".to_string();
        replacement.updated_at += time::Duration::seconds(1);
        sinomo_network::write_object_file(&paths.state, &replacement)
            .expect("externally replace local state");

        let leased =
            read_cluster_state_without_kv_cached_shared(&paths).expect("leased local state");
        assert!(
            Arc::ptr_eq(&first, &leased),
            "the hot local-state lease must not decode a replacement snapshot"
        );

        LOCAL_CLUSTER_STATES
            .lock()
            .expect("local cluster state cache")
            .get_mut(&paths.root)
            .expect("cached local state")
            .validated_at = Instant::now()
            .checked_sub(CLUSTER_STATE_CACHE_VALIDATION_LEASE)
            .expect("backdated local-state validation lease");
        let refreshed =
            read_cluster_state_without_kv_cached_shared(&paths).expect("refreshed local state");
        assert!(!Arc::ptr_eq(&first, &refreshed));
        assert_eq!(refreshed.cluster_name, replacement.cluster_name);
    }

    #[test]
    fn cluster_state_write_invalidates_shared_cache_immediately() {
        let root = sinomo_testdir::TestDir::new("cluster-state-shared-cache-write");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let initial = test_state("shared_cache_write_initial");
        write_cluster_state(&paths, &initial).expect("initial state");
        let first = read_cluster_state_cached_shared(&paths).expect("first cached state");
        let first_local =
            read_cluster_state_without_kv_cached_shared(&paths).expect("first cached local state");

        let mut replacement = initial;
        replacement.cluster_name = "shared cache write replacement".to_string();
        replacement.updated_at += time::Duration::seconds(1);
        write_cluster_state(&paths, &replacement).expect("replacement state");
        assert!(
            !CLUSTER_STATES
                .lock()
                .expect("cluster state cache")
                .contains_key(&paths.root),
            "in-process state writes must invalidate the resident snapshot"
        );
        assert!(
            !LOCAL_CLUSTER_STATES
                .lock()
                .expect("local cluster state cache")
                .contains_key(&paths.root),
            "in-process state writes must invalidate the local-only resident snapshot"
        );

        let refreshed = read_cluster_state_cached_shared(&paths).expect("refreshed state");
        let refreshed_local =
            read_cluster_state_without_kv_cached_shared(&paths).expect("refreshed local state");
        assert!(!Arc::ptr_eq(&first, &refreshed));
        assert!(!Arc::ptr_eq(&first_local, &refreshed_local));
        assert_eq!(refreshed.cluster_name, replacement.cluster_name);
        assert_eq!(refreshed_local.cluster_name, replacement.cluster_name);
    }
}
