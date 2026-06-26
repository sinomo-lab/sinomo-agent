use crate::ClusterPaths;
use crate::endpoint_records::{
    cleanup_cluster_iroh_endpoints, cleanup_member_iroh_endpoints, upsert_member_endpoint,
};
use crate::error::NetworkError;
use crate::file_mark::{ClusterFileMark, cluster_file_mark};
#[cfg(test)]
use crate::kv_local_store::invalidate_cluster_kv_reader_node_id_cache;
use crate::kv_local_store::record_current_cluster_kv_reader_node_id_cache;
use crate::kv_read_model::read_cluster_members_from_kv;
use crate::local_membership::{
    locally_learned_remote_route_endpoints, preserve_locally_learned_remote_route_endpoints,
};
use crate::member_store::merge_member_record;
#[cfg(test)]
use crate::object_file_store::remove_optional_file;
use crate::object_file_store::{read_optional_object_file, write_object_file_with_semantic_skip};
use sinomo_core::ClusterBootstrapState;
use sinomo_node::{
    cluster_bootstrap_state_stable_eq, cluster_node_is_local_process, upsert_member_record,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

const MAX_CLUSTER_STATE_CACHE_ENTRIES: usize = 128;
const CLUSTER_STATE_CACHE_MAX_AGE: Duration = Duration::from_secs(1);

static CLUSTER_STATE_CACHE_CLOCK: AtomicU64 = AtomicU64::new(1);
static CLUSTER_STATES: LazyLock<Mutex<HashMap<PathBuf, ClusterStateCacheEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

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
            crate::object::try_reserve_vec(
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
    write_object_file_with_semantic_skip(&paths.state, state, |existing| {
        cluster_bootstrap_state_stable_eq(existing, state)
    })?;
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

#[cfg(test)]
pub fn remove_local_cluster_state(paths: &ClusterPaths) -> Result<(), NetworkError> {
    if !remove_optional_file(&paths.state)? {
        return Err(NetworkError::ClusterOrphan);
    }
    invalidate_cluster_kv_reader_node_id_cache(&paths.state);
    remove_optional_file(&paths.kv_snapshot)?;
    Ok(())
}
