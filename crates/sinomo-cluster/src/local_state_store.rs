use crate::kv_local_store::ClusterKvPathsExt;
#[cfg(any(test, feature = "test-support"))]
use crate::kv_local_store::invalidate_cluster_kv_reader_node_id_cache;
#[allow(unused_imports)]
use crate::kv_local_store::kv_err;
use crate::kv_local_store::record_current_cluster_kv_reader_node_id_cache;
use crate::kv_read_model::read_cluster_members_from_kv;
use crate::local_membership::{
    locally_learned_remote_route_endpoints, preserve_locally_learned_remote_route_endpoints,
};
use crate::member_store::merge_member_record;
use sinomo_base::ClusterBootstrapState;
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
use std::path::PathBuf;
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

pub fn write_cluster_state(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<(), NetworkError> {
    write_object_file_with_semantic_skip(&paths.state, state, |existing| {
        cluster_bootstrap_state_stable_eq(existing, state)
    })?;
    invalidate_cluster_state_cache(paths);
    sinomo_network::invalidate_cluster_delivery_routing_table_cache(paths);
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
    use sinomo_local::SinomoDataDir;
    use sinomo_node::{ClusterId, NodeId};
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
