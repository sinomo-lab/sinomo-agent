use crate::connectivity::{
    ClusterConnectivityRecord, ClusterConnectivityState, MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS,
    MAX_CLUSTER_CONNECTIVITY_SOURCE_RECORDS, cluster_connectivity_record_for_command_auth_denied,
    cluster_connectivity_record_for_hard_failure,
    cluster_connectivity_record_for_identity_mismatch,
    cluster_connectivity_record_for_probe_outcome, cluster_connectivity_record_should_refresh,
    cluster_connectivity_stable_eq, cluster_network_record_live,
    validate_cluster_connectivity_records_for_source,
};
use crate::error::NetworkError;
use crate::object_file_store::{
    read_optional_object_file, remove_optional_file, write_object_file,
};
use crate::{ClusterEndpointId, ClusterPaths};
use fs2::FileExt;
use sinomo_base::{ClusterBootstrapState, ClusterEndpointRecord, ClusterTransportKind};
use sinomo_node::{ClusterId, NodeId};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

const MAX_LOCAL_CLUSTER_CONNECTIVITY_CACHE_ENTRIES: usize = 128;
type LocalClusterConnectivityRecordKey = (NodeId, NodeId, ClusterEndpointId);

static LOCAL_CLUSTER_CONNECTIVITY_CACHE_CLOCK: AtomicU64 = AtomicU64::new(1);
static LOCAL_CLUSTER_CONNECTIVITY_CONTENT_GENERATION_CLOCK: AtomicU64 = AtomicU64::new(1);
static LOCAL_CLUSTER_CONNECTIVITY_CONTENT_GENERATIONS: LazyLock<Mutex<HashMap<PathBuf, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static LOCAL_CLUSTER_CONNECTIVITY_ROUTING_GENERATION_CLOCK: AtomicU64 = AtomicU64::new(1);
static LOCAL_CLUSTER_CONNECTIVITY_ROUTING_GENERATIONS: LazyLock<Mutex<HashMap<PathBuf, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
const LOCAL_CLUSTER_CONNECTIVITY_ROUTING_GENERATION_CHANNEL_CAPACITY: usize = 128;
static LOCAL_CLUSTER_CONNECTIVITY_ROUTING_GENERATION_CHANGES: LazyLock<
    tokio::sync::broadcast::Sender<LocalClusterConnectivityRoutingGenerationChange>,
> = LazyLock::new(|| {
    let (sender, _receiver) = tokio::sync::broadcast::channel(
        LOCAL_CLUSTER_CONNECTIVITY_ROUTING_GENERATION_CHANNEL_CAPACITY,
    );
    sender
});
static LOCAL_CLUSTER_CONNECTIVITY_RECORDS: LazyLock<
    Mutex<HashMap<PathBuf, LocalClusterConnectivityRecordCacheEntry>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
static LOCAL_CLUSTER_CONNECTIVITY_OBSERVATION_WATERMARKS: LazyLock<
    Mutex<HashMap<PathBuf, LocalClusterConnectivityObservationWatermarkEntry>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(any(test, feature = "test-support"))]
static LOCAL_CLUSTER_CONNECTIVITY_PRUNE_PASSES_FOR_TEST: LazyLock<Mutex<HashMap<PathBuf, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Clone)]
struct LocalClusterConnectivityRecordCacheEntry {
    records: Vec<ClusterConnectivityRecord>,
    earliest_expires_at: Option<OffsetDateTime>,
    last_used: u64,
}

#[derive(Clone, Default)]
struct LocalClusterConnectivityObservationWatermarkEntry {
    observed_at: BTreeMap<LocalClusterConnectivityRecordKey, OffsetDateTime>,
    generations: BTreeMap<LocalClusterConnectivityRecordKey, u64>,
    last_used: u64,
}

pub fn local_cluster_connectivity_content_generation(paths: &ClusterPaths) -> u64 {
    LOCAL_CLUSTER_CONNECTIVITY_CONTENT_GENERATIONS
        .lock()
        .ok()
        .and_then(|generations| generations.get(&paths.root).copied())
        .unwrap_or(0)
}

fn bump_local_cluster_connectivity_content_generation(paths: &ClusterPaths) {
    let next = LOCAL_CLUSTER_CONNECTIVITY_CONTENT_GENERATION_CLOCK.fetch_add(1, Ordering::AcqRel);
    if let Ok(mut generations) = LOCAL_CLUSTER_CONNECTIVITY_CONTENT_GENERATIONS.lock() {
        generations.insert(paths.root.clone(), next);
    }
}

/// Generation of changes that can alter route reachability or topology.
///
/// Observation timestamps and latency samples are not store generations at
/// all: the store retains its current evidence until expiry renewal is due or
/// a material reachability/topology field changes. Delivery routing tables
/// therefore remain stable across ordinary probe and command-RPC success.
#[cfg(test)]
fn local_cluster_connectivity_routing_generation(paths: &ClusterPaths) -> u64 {
    LOCAL_CLUSTER_CONNECTIVITY_ROUTING_GENERATIONS
        .lock()
        .ok()
        .and_then(|generations| generations.get(&paths.root).copied())
        .unwrap_or(0)
}

fn bump_local_cluster_connectivity_routing_generation(paths: &ClusterPaths) {
    let next = LOCAL_CLUSTER_CONNECTIVITY_ROUTING_GENERATION_CLOCK.fetch_add(1, Ordering::AcqRel);
    if let Ok(mut generations) = LOCAL_CLUSTER_CONNECTIVITY_ROUTING_GENERATIONS.lock() {
        generations.insert(paths.root.clone(), next);
    }
    // The store owns this exact semantic edge. Consumers subscribe to the
    // bounded generation stream instead of polling the connectivity file or
    // inferring route changes from unrelated maintenance traffic.
    let _ = LOCAL_CLUSTER_CONNECTIVITY_ROUTING_GENERATION_CHANGES.send(
        LocalClusterConnectivityRoutingGenerationChange {
            root: paths.root.clone(),
            generation: next,
        },
    );
}

#[derive(Clone, Debug)]
struct LocalClusterConnectivityRoutingGenerationChange {
    root: PathBuf,
    generation: u64,
}

/// Reactive subscription to route-relevant local connectivity-store changes.
///
/// The stream is path-scoped and generation-valued. A lagged receiver compares
/// the current generation for its exact root, so coalescing never loses the
/// only wake after a route becomes usable. Observation-only timestamp refreshes
/// do not advance this stream.
#[doc(hidden)]
pub struct LocalClusterConnectivityRoutingGenerationSubscription {
    root: PathBuf,
    observed_generation: u64,
    receiver: tokio::sync::broadcast::Receiver<LocalClusterConnectivityRoutingGenerationChange>,
}

impl LocalClusterConnectivityRoutingGenerationSubscription {
    /// Waits until this root's route-relevant generation differs from the last
    /// generation returned by the subscription.
    pub async fn changed(&mut self) -> u64 {
        loop {
            let current = LOCAL_CLUSTER_CONNECTIVITY_ROUTING_GENERATIONS
                .lock()
                .ok()
                .and_then(|generations| generations.get(&self.root).copied())
                .unwrap_or(0);
            if current != self.observed_generation {
                self.observed_generation = current;
                return current;
            }
            match self.receiver.recv().await {
                Ok(change)
                    if change.root == self.root
                        && change.generation != self.observed_generation =>
                {
                    self.observed_generation = change.generation;
                    return change.generation;
                }
                Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                // The sender is process-static, but keep the terminal behavior
                // explicit if that ownership ever changes.
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    std::future::pending::<()>().await;
                }
            }
        }
    }
}

/// Subscribes to route-relevant changes for one local connectivity root.
///
/// Registration precedes the initial generation read in `changed`, closing the
/// usual register/read race without a filesystem watcher or timer.
#[doc(hidden)]
pub fn subscribe_local_cluster_connectivity_routing_generation(
    paths: &ClusterPaths,
) -> LocalClusterConnectivityRoutingGenerationSubscription {
    LocalClusterConnectivityRoutingGenerationSubscription {
        root: paths.root.clone(),
        observed_generation: 0,
        receiver: LOCAL_CLUSTER_CONNECTIVITY_ROUTING_GENERATION_CHANGES.subscribe(),
    }
}

pub fn read_local_cluster_connectivity_records(
    paths: &ClusterPaths,
) -> Result<Vec<ClusterConnectivityRecord>, NetworkError> {
    let now = OffsetDateTime::now_utc();
    if let Some(records) = read_local_cluster_connectivity_records_from_cache(paths, now)? {
        return Ok(records);
    }

    let Some(mut records) =
        read_optional_object_file::<Vec<ClusterConnectivityRecord>>(&paths.connectivity_local)?
    else {
        return Ok(Vec::new());
    };
    records.retain(|record| cluster_network_record_live(record.expires_at, now));
    sort_cluster_connectivity_records(&mut records);
    if records.is_empty() {
        return Ok(Vec::new());
    }

    let mut cache =
        LOCAL_CLUSTER_CONNECTIVITY_RECORDS
            .lock()
            .map_err(|error| NetworkError::Io {
                operation: "lock local cluster connectivity cache".to_string(),
                detail: error.to_string(),
            })?;
    if let Some(entry) = cache.get_mut(&paths.root) {
        prune_expired_local_cluster_connectivity_cache_entry(&paths.root, entry, now);
        if entry.records.is_empty() {
            cache.remove(&paths.root);
        } else {
            entry.last_used = local_cluster_connectivity_cache_tick();
            let mut cached_records = entry.records.clone();
            sort_cluster_connectivity_records(&mut cached_records);
            return Ok(cached_records);
        }
    }
    store_local_cluster_connectivity_cache_entry(
        &mut cache,
        paths.root.clone(),
        records.clone(),
        now,
    );
    Ok(records)
}

/// Discards this process's local connectivity snapshot so the next read
/// reloads the shared on-disk state.
///
/// Daemon-control clients can update the local connectivity file from another
/// process. Advancing both generations ensures cached route graphs are rebuilt
/// from the reloaded records as well.
pub fn invalidate_local_cluster_connectivity_cache(
    paths: &ClusterPaths,
) -> Result<(), NetworkError> {
    LOCAL_CLUSTER_CONNECTIVITY_RECORDS
        .lock()
        .map_err(|error| NetworkError::Io {
            operation: "lock local cluster connectivity cache".to_string(),
            detail: error.to_string(),
        })?
        .remove(&paths.root);
    LOCAL_CLUSTER_CONNECTIVITY_OBSERVATION_WATERMARKS
        .lock()
        .map_err(|error| NetworkError::Io {
            operation: "lock local cluster connectivity observation watermarks".to_string(),
            detail: error.to_string(),
        })?
        .remove(&paths.root);
    bump_local_cluster_connectivity_content_generation(paths);
    bump_local_cluster_connectivity_routing_generation(paths);
    Ok(())
}

pub fn read_local_cluster_connectivity_records_for_source(
    paths: &ClusterPaths,
    source_node_id: &NodeId,
) -> Result<Vec<ClusterConnectivityRecord>, NetworkError> {
    let records = read_local_cluster_connectivity_records(paths)?;
    let mut filtered = Vec::new();
    for record in records
        .into_iter()
        .filter(|record| record.source_node_id == *source_node_id)
    {
        crate::object::try_reserve_vec(
            &mut filtered,
            1,
            "collect local connectivity records by source",
        )?;
        filtered.push(record);
    }
    Ok(filtered)
}

/// Records a command-path observation from outside the daemon's reactive
/// connectivity owner.
///
/// Exact observation detail is persisted even when reachability is unchanged.
/// That one local write is the cross-process fence by which a concurrently
/// running daemon detects that its earlier source-set snapshot was superseded.
/// Steady-state daemon traffic uses the hidden owner entry point below and
/// keeps timestamp/latency-only observations entirely in memory.
pub fn record_local_cluster_endpoint_command_connectivity(
    paths: &ClusterPaths,
    state: &sinomo_base::ClusterBootstrapState,
    endpoint: &sinomo_base::ClusterEndpointRecord,
    connectivity_state: ClusterConnectivityState,
    latency_ms: Option<u64>,
    allow_failure_grace: bool,
) -> Result<ClusterConnectivityRecord, NetworkError> {
    record_local_cluster_endpoint_command_connectivity_inner(
        paths,
        state,
        endpoint,
        connectivity_state,
        latency_ms,
        allow_failure_grace,
        true,
    )
}

/// Records command connectivity on the daemon-owned reactive observation
/// stream without persisting timestamp/latency-only refreshes.
///
/// External processes must use [`record_local_cluster_endpoint_command_connectivity`]
/// so their exact write remains visible to the daemon's cross-process CAS.
#[doc(hidden)]
pub fn record_daemon_owned_local_cluster_endpoint_command_connectivity(
    paths: &ClusterPaths,
    state: &sinomo_base::ClusterBootstrapState,
    endpoint: &sinomo_base::ClusterEndpointRecord,
    connectivity_state: ClusterConnectivityState,
    latency_ms: Option<u64>,
    allow_failure_grace: bool,
) -> Result<ClusterConnectivityRecord, NetworkError> {
    record_local_cluster_endpoint_command_connectivity_inner(
        paths,
        state,
        endpoint,
        connectivity_state,
        latency_ms,
        allow_failure_grace,
        false,
    )
}

fn record_local_cluster_endpoint_command_connectivity_inner(
    paths: &ClusterPaths,
    state: &sinomo_base::ClusterBootstrapState,
    endpoint: &sinomo_base::ClusterEndpointRecord,
    connectivity_state: ClusterConnectivityState,
    latency_ms: Option<u64>,
    allow_failure_grace: bool,
    persist_observation_details: bool,
) -> Result<ClusterConnectivityRecord, NetworkError> {
    let previous = read_local_cluster_connectivity_records_for_source(paths, &state.local_node_id)?
        .into_iter()
        .find(|record| {
            record.target_node_id == endpoint.node_id && record.endpoint_id == endpoint.endpoint_id
        });
    let now = OffsetDateTime::now_utc();
    let timestamp = now
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string());
    let is_healthy = matches!(&connectivity_state, ClusterConnectivityState::Healthy);
    let observed_latency_ms = latency_ms.unwrap_or_else(|| {
        previous
            .as_ref()
            .and_then(|record| record.latency_ms)
            .unwrap_or(0)
    });
    let record = if matches!(&connectivity_state, ClusterConnectivityState::Unreachable) {
        cluster_connectivity_record_for_identity_mismatch(state, endpoint, &timestamp, now)
    } else if !is_healthy && !allow_failure_grace {
        cluster_connectivity_record_for_hard_failure(
            state,
            endpoint,
            previous.as_ref(),
            &timestamp,
            now,
        )
    } else {
        cluster_connectivity_record_for_probe_outcome(
            state,
            endpoint,
            previous.as_ref(),
            is_healthy,
            observed_latency_ms,
            &timestamp,
            now,
        )
    };
    write_local_cluster_connectivity_records_inner(
        paths,
        &state.local_node_id,
        std::slice::from_ref(&record),
        false,
        persist_observation_details,
    )?;
    Ok(record)
}

pub fn record_local_cluster_endpoint_command_auth_denied(
    paths: &ClusterPaths,
    state: &sinomo_base::ClusterBootstrapState,
    endpoint: &sinomo_base::ClusterEndpointRecord,
) -> Result<ClusterConnectivityRecord, NetworkError> {
    let now = OffsetDateTime::now_utc();
    let timestamp = now
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string());
    let record =
        cluster_connectivity_record_for_command_auth_denied(state, endpoint, &timestamp, now);
    write_local_cluster_connectivity_records(
        paths,
        &state.local_node_id,
        std::slice::from_ref(&record),
    )?;
    Ok(record)
}

pub fn write_local_cluster_connectivity_records(
    paths: &ClusterPaths,
    source_node_id: &NodeId,
    records: &[ClusterConnectivityRecord],
) -> Result<(), NetworkError> {
    write_local_cluster_connectivity_records_inner(paths, source_node_id, records, false, false)
}

pub async fn write_local_cluster_connectivity_records_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    source_node_id: &NodeId,
    records: &[ClusterConnectivityRecord],
) -> Result<(), NetworkError> {
    let paths = paths.clone();
    let source_node_id = source_node_id.clone();
    let records = records.to_vec();
    runtime
        .spawn_blocking(move || {
            write_local_cluster_connectivity_records(&paths, &source_node_id, &records)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "write local cluster connectivity records".to_string(),
            detail: format!("local cluster connectivity write task failed: {source}"),
        })?
}

/// Replaces the complete set owned by `source_node_id`.
///
/// This is an authoritative-current operation for synchronous source owners
/// such as environment changes and explicit tunnel pruning. It is not an
/// optimistic-concurrency API: an omitted key carries no timestamp and cannot
/// prove when the producer captured its set. Any producer that performs async
/// work before replacement must capture a
/// [`LocalClusterConnectivityStoreSnapshot`] first and use
/// [`commit_local_cluster_connectivity_source_set_if_snapshot`] so omissions
/// and empty sets are fenced by per-key observation generations.
pub fn write_local_cluster_connectivity_record_set(
    paths: &ClusterPaths,
    source_node_id: &NodeId,
    records: &[ClusterConnectivityRecord],
) -> Result<(), NetworkError> {
    write_local_cluster_connectivity_records_inner(paths, source_node_id, records, true, false)
}

/// Blocking-pool adapter for an already authoritative current source set.
///
/// This does not turn a set assembled asynchronously into a generation-fenced
/// set; see [`write_local_cluster_connectivity_record_set`].
pub async fn write_local_cluster_connectivity_record_set_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    source_node_id: &NodeId,
    records: &[ClusterConnectivityRecord],
) -> Result<(), NetworkError> {
    let paths = paths.clone();
    let source_node_id = source_node_id.clone();
    let records = records.to_vec();
    runtime
        .spawn_blocking(move || {
            write_local_cluster_connectivity_record_set(&paths, &source_node_id, &records)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "write local cluster connectivity records".to_string(),
            detail: format!("local cluster connectivity write task failed: {source}"),
        })?
}

/// Merge record subsets owned by several source nodes in one process-safe
/// local-store transaction.
///
/// This is intentionally merge-only: imported peer snapshots do not carry a
/// source-set generation watermark, so an omitted key cannot authorize a
/// deletion. Expiry and topology pruning remove stale imported observations.
pub fn write_local_cluster_connectivity_record_groups(
    paths: &ClusterPaths,
    record_groups: &BTreeMap<NodeId, Vec<ClusterConnectivityRecord>>,
) -> Result<(), NetworkError> {
    if record_groups.is_empty() {
        return Ok(());
    }
    let mut observed_records = 0_usize;
    for (source_node_id, records) in record_groups {
        observed_records = observed_records.checked_add(records.len()).ok_or(
            NetworkError::ClusterConnectivitySnapshotRecordLimit {
                observed_records: usize::MAX,
                max_records: MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS,
            },
        )?;
        if observed_records > MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS {
            return Err(NetworkError::ClusterConnectivitySnapshotRecordLimit {
                observed_records,
                max_records: MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS,
            });
        }
        validate_cluster_connectivity_records_for_source(source_node_id, records)?;
    }

    let _mutation_lock = acquire_local_cluster_connectivity_mutation_lock(paths)?;
    let now = OffsetDateTime::now_utc();
    let existing_records = read_persisted_live_local_cluster_connectivity_records(paths, now)?;
    let observation_watermarks = local_cluster_connectivity_observation_watermarks(paths)?;
    let mut merged = existing_records
        .iter()
        .map(|record| (cluster_connectivity_record_key(record), record.clone()))
        .collect::<BTreeMap<_, _>>();
    for records in record_groups.values() {
        for record in records {
            if !cluster_network_record_live(record.expires_at, now) {
                continue;
            }
            let key = cluster_connectivity_record_key(record);
            if merged.get(&key).is_some_and(|existing| {
                local_cluster_connectivity_latest_observed_at(
                    existing,
                    &observation_watermarks.observed_at,
                ) > record.observed_at
            }) {
                continue;
            }
            if merged.get(&key).is_some_and(|existing| {
                local_cluster_connectivity_observation_is_stable(existing, record, now)
            }) {
                continue;
            }
            merged.insert(key, record.clone());
        }
    }
    if merged.len() > MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS {
        return Err(NetworkError::ClusterConnectivitySnapshotRecordLimit {
            observed_records: merged.len(),
            max_records: MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS,
        });
    }
    for source_node_id in record_groups.keys() {
        let retained_source_records = merged
            .values()
            .filter(|record| record.source_node_id == *source_node_id)
            .count();
        if retained_source_records > MAX_CLUSTER_CONNECTIVITY_SOURCE_RECORDS {
            return Err(NetworkError::ClusterConnectivityRecordLimit {
                source_node_id: source_node_id.clone(),
                observed_records: retained_source_records,
                max_records: MAX_CLUSTER_CONNECTIVITY_SOURCE_RECORDS,
            });
        }
    }

    let mut merged_records = Vec::new();
    crate::object::try_reserve_vec(
        &mut merged_records,
        merged.len(),
        "collect aggregate merged connectivity records",
    )?;
    merged_records.extend(merged.into_values());
    sort_cluster_connectivity_records(&mut merged_records);

    let records_changed = existing_records != merged_records;
    let route_records_stable =
        local_cluster_connectivity_route_records_stable(&existing_records, &merged_records);
    let persisted_records_stable = route_records_stable
        && local_cluster_connectivity_persisted_records_stable(&existing_records, &merged_records);
    if !persisted_records_stable {
        if merged_records.is_empty() {
            remove_optional_file(&paths.connectivity_local)?;
        } else {
            write_object_file(&paths.connectivity_local, &merged_records)?;
        }
    }
    reconcile_local_cluster_connectivity_observation_watermarks(
        paths,
        &merged_records,
        record_groups.values().flatten(),
    )?;
    let mut cache =
        LOCAL_CLUSTER_CONNECTIVITY_RECORDS
            .lock()
            .map_err(|error| NetworkError::Io {
                operation: "lock local cluster connectivity cache".to_string(),
                detail: error.to_string(),
            })?;
    if merged_records.is_empty() {
        cache.remove(&paths.root);
    } else {
        store_local_cluster_connectivity_cache_entry(
            &mut cache,
            paths.root.clone(),
            merged_records,
            now,
        );
    }
    if records_changed {
        bump_local_cluster_connectivity_content_generation(paths);
    }
    if !route_records_stable {
        bump_local_cluster_connectivity_routing_generation(paths);
    }
    Ok(())
}

pub async fn write_local_cluster_connectivity_record_groups_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    record_groups: &BTreeMap<NodeId, Vec<ClusterConnectivityRecord>>,
) -> Result<(), NetworkError> {
    let paths = paths.clone();
    let record_groups = record_groups.clone();
    runtime
        .spawn_blocking(move || {
            write_local_cluster_connectivity_record_groups(&paths, &record_groups)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "write aggregate local cluster connectivity records".to_string(),
            detail: format!("local cluster connectivity write task failed: {source}"),
        })?
}

fn local_cluster_connectivity_mutation_lock_path(paths: &ClusterPaths) -> PathBuf {
    let parent = paths
        .connectivity_local
        .parent()
        .unwrap_or_else(|| Path::new("."));
    let file_name = paths
        .connectivity_local
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("connectivity-local.sobj");
    parent.join(format!("{file_name}.lock"))
}

/// Serializes the complete on-disk connectivity read/modify/write transaction.
///
/// The main daemon, shadow daemon, and command clients can share the cluster
/// directory. Atomic file replacement prevents torn reads, but without this
/// advisory lock two writers updating different source-node sets can both read
/// the same snapshot and let the last rename silently discard the other source.
fn acquire_local_cluster_connectivity_mutation_lock(
    paths: &ClusterPaths,
) -> Result<std::fs::File, NetworkError> {
    let lock_path = local_cluster_connectivity_mutation_lock_path(paths);
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
    lock.lock_exclusive()
        .map_err(|source| NetworkError::StorageIo {
            path: lock_path.display().to_string(),
            detail: source.to_string(),
        })?;
    Ok(lock)
}

fn read_persisted_live_local_cluster_connectivity_records(
    paths: &ClusterPaths,
    now: OffsetDateTime,
) -> Result<Vec<ClusterConnectivityRecord>, NetworkError> {
    let mut records =
        read_optional_object_file::<Vec<ClusterConnectivityRecord>>(&paths.connectivity_local)?
            .unwrap_or_default();
    records.retain(|record| cluster_network_record_live(record.expires_at, now));
    sort_cluster_connectivity_records(&mut records);
    Ok(records)
}

pub fn prune_local_cluster_connectivity_records_for_state(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<usize, NetworkError> {
    let _mutation_lock = acquire_local_cluster_connectivity_mutation_lock(paths)?;
    let now = OffsetDateTime::now_utc();
    let Some(mut existing_records) =
        read_optional_object_file::<Vec<ClusterConnectivityRecord>>(&paths.connectivity_local)?
    else {
        if let Ok(mut cache) = LOCAL_CLUSTER_CONNECTIVITY_RECORDS.lock() {
            cache.remove(&paths.root);
        }
        reset_local_cluster_connectivity_observation_watermarks(paths, &[])?;
        return Ok(0);
    };
    sort_cluster_connectivity_records(&mut existing_records);

    let topology = LocalConnectivityTopology::from_state(state);
    let mut retained = existing_records
        .iter()
        .filter(|record| topology.record_is_usable(record, now))
        .cloned()
        .collect::<Vec<_>>();
    sort_cluster_connectivity_records(&mut retained);

    let removed = existing_records.len().saturating_sub(retained.len());
    if existing_records == retained {
        reconcile_local_cluster_connectivity_observation_watermarks(
            paths,
            &retained,
            std::iter::empty(),
        )?;
        let cache_changed = replace_local_cluster_connectivity_cache_records(paths, retained, now)?;
        if cache_changed {
            bump_local_cluster_connectivity_content_generation(paths);
            // A prune pass repairs an in-process cache that disagrees with the
            // authoritative on-disk record set. Conservatively invalidate the
            // routing table too; this path is maintenance, not the per-RPC
            // observation hot path.
            bump_local_cluster_connectivity_routing_generation(paths);
        }
        return Ok(0);
    }

    if retained.is_empty() {
        remove_optional_file(&paths.connectivity_local)?;
    } else {
        write_object_file(&paths.connectivity_local, &retained)?;
    }

    reconcile_local_cluster_connectivity_observation_watermarks(
        paths,
        &retained,
        std::iter::empty(),
    )?;
    let _cache_changed = replace_local_cluster_connectivity_cache_records(paths, retained, now)?;
    bump_local_cluster_connectivity_content_generation(paths);
    bump_local_cluster_connectivity_routing_generation(paths);
    Ok(removed)
}

pub async fn prune_local_cluster_connectivity_records_for_state_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<usize, NetworkError> {
    let paths = paths.clone();
    let state = state.clone();
    runtime
        .spawn_blocking(move || prune_local_cluster_connectivity_records_for_state(&paths, &state))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "prune local cluster connectivity records".to_string(),
            detail: format!("local cluster connectivity prune task failed: {source}"),
        })?
}

/// Result of an atomic topology-prune plus source-set replacement.
///
/// This is the internal contract between the daemon's reactive projection
/// owner and the local connectivity store. A superseded result is read-only and
/// returns the newer source snapshot that prevented the stale mutation.
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LocalClusterConnectivitySourceSetCommit {
    Committed {
        records: Vec<ClusterConnectivityRecord>,
        snapshot: LocalClusterConnectivityStoreSnapshot,
        /// Whether this commit changed the material local source projection.
        /// Observation timestamps, latency samples, and diagnostic timestamps
        /// alone deliberately leave this false.
        content_changed: bool,
    },
    Superseded {
        records: Vec<ClusterConnectivityRecord>,
        snapshot: LocalClusterConnectivityStoreSnapshot,
    },
}

/// Exact cross-process fence for one local connectivity-store generation.
///
/// The process-local generation catches supported in-process writes and
/// explicit invalidations. The canonical persisted snapshot catches writers
/// in other processes, whose in-memory generation is necessarily independent.
/// Both are compared while holding the mutation lock; neither is used as a
/// transport-integrity mechanism.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct LocalClusterConnectivityStoreSnapshot {
    content_generation: u64,
    persisted_records: Arc<Vec<ClusterConnectivityRecord>>,
    /// Per-key observation ordering captured with pending source-set work.
    ///
    /// This is intentionally excluded from material snapshot equality: a
    /// stable command observation must fence an older omission without
    /// invalidating an in-flight publication of identical durable evidence.
    observation_generations: Arc<BTreeMap<LocalClusterConnectivityRecordKey, u64>>,
}

impl PartialEq for LocalClusterConnectivityStoreSnapshot {
    fn eq(&self, other: &Self) -> bool {
        self.content_generation == other.content_generation
            && self.persisted_records == other.persisted_records
    }
}

impl Eq for LocalClusterConnectivityStoreSnapshot {}

/// Captures the exact persisted base that a later source-set commit may
/// replace. The read and cache reconciliation share the cross-process
/// mutation lock with all supported writers.
#[doc(hidden)]
pub fn capture_local_cluster_connectivity_store_snapshot(
    paths: &ClusterPaths,
) -> Result<LocalClusterConnectivityStoreSnapshot, NetworkError> {
    let _mutation_lock = acquire_local_cluster_connectivity_mutation_lock(paths)?;
    let now = OffsetDateTime::now_utc();
    let mut persisted_records =
        read_optional_object_file::<Vec<ClusterConnectivityRecord>>(&paths.connectivity_local)?
            .unwrap_or_default();
    sort_cluster_connectivity_records(&mut persisted_records);
    reconcile_local_cluster_connectivity_cache_with_persisted_records(
        paths,
        &persisted_records,
        now,
    )?;
    let observation_watermarks = local_cluster_connectivity_observation_watermarks(paths)?;
    Ok(LocalClusterConnectivityStoreSnapshot {
        content_generation: local_cluster_connectivity_content_generation(paths),
        persisted_records: Arc::new(persisted_records),
        observation_generations: Arc::new(observation_watermarks.generations),
    })
}

/// Atomically verifies `expected_snapshot`, prunes records against `state`,
/// and replaces the complete source-owned set.
///
/// Both snapshot comparisons occur while holding the same process-safe file
/// lock as every connectivity writer. Consequently a concurrent command/join
/// observation either wins first and makes this operation read-only, or lands
/// after this commit; even an empty stale set can never clear that observation.
#[doc(hidden)]
pub fn commit_local_cluster_connectivity_source_set_if_snapshot(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    source_node_id: &NodeId,
    records: &[ClusterConnectivityRecord],
    expected_snapshot: &LocalClusterConnectivityStoreSnapshot,
) -> Result<LocalClusterConnectivitySourceSetCommit, NetworkError> {
    validate_cluster_connectivity_records_for_source(source_node_id, records)?;
    let _mutation_lock = acquire_local_cluster_connectivity_mutation_lock(paths)?;
    let actual_generation = local_cluster_connectivity_content_generation(paths);
    let now = OffsetDateTime::now_utc();
    let mut existing_records =
        read_optional_object_file::<Vec<ClusterConnectivityRecord>>(&paths.connectivity_local)?
            .unwrap_or_default();
    sort_cluster_connectivity_records(&mut existing_records);
    let persisted_changed =
        existing_records.as_slice() != expected_snapshot.persisted_records.as_slice();
    if actual_generation != expected_snapshot.content_generation || persisted_changed {
        if persisted_changed {
            // The file changed after the expected snapshot was captured. It
            // therefore wins over this process's possibly stale cache even
            // when the route-stable fields happen to compare equal.
            let live_records = existing_records
                .iter()
                .filter(|record| cluster_network_record_live(record.expires_at, now))
                .cloned()
                .collect::<Vec<_>>();
            let route_records_stable = local_cluster_connectivity_route_records_stable(
                expected_snapshot.persisted_records.as_slice(),
                &live_records,
            );
            reset_local_cluster_connectivity_observation_watermarks(paths, &live_records)?;
            if replace_local_cluster_connectivity_cache_records(paths, live_records, now)? {
                bump_local_cluster_connectivity_content_generation(paths);
                if !route_records_stable {
                    bump_local_cluster_connectivity_routing_generation(paths);
                }
            }
        }
        let current_records = read_local_cluster_connectivity_records(paths)?;
        let records = connectivity_source_records_from_slice(
            &current_records,
            source_node_id,
            now,
            "collect superseding connectivity source generation",
        )?;
        return Ok(LocalClusterConnectivitySourceSetCommit::Superseded {
            records,
            snapshot: LocalClusterConnectivityStoreSnapshot {
                content_generation: local_cluster_connectivity_content_generation(paths),
                persisted_records: Arc::new(existing_records),
                observation_generations: Arc::new(
                    local_cluster_connectivity_observation_watermarks(paths)?.generations,
                ),
            },
        });
    }

    let topology = LocalConnectivityTopology::from_state(state);
    let observation_watermarks = local_cluster_connectivity_observation_watermarks(paths)?;
    let existing_by_key = existing_records
        .iter()
        .map(|record| (cluster_connectivity_record_key(record), record))
        .collect::<BTreeMap<_, _>>();
    let incoming_source_keys = records
        .iter()
        .filter(|record| {
            record.source_node_id == *source_node_id && topology.record_is_usable(record, now)
        })
        .map(cluster_connectivity_record_key)
        .collect::<BTreeSet<_>>();
    let mut merged = existing_records
        .iter()
        .filter(|record| {
            topology.record_is_usable(record, now)
                && (record.source_node_id != *source_node_id
                    || (!incoming_source_keys.contains(&cluster_connectivity_record_key(record))
                        && local_cluster_connectivity_observation_advanced_since_snapshot(
                            &cluster_connectivity_record_key(record),
                            &observation_watermarks.generations,
                            expected_snapshot,
                        )))
        })
        .map(|record| (cluster_connectivity_record_key(record), record.clone()))
        .collect::<BTreeMap<_, _>>();
    for record in records {
        if record.source_node_id == *source_node_id && topology.record_is_usable(record, now) {
            let key = cluster_connectivity_record_key(record);
            let retained = existing_by_key
                .get(&key)
                .copied()
                .filter(|existing| topology.record_is_usable(existing, now))
                .filter(|existing| {
                    local_cluster_connectivity_latest_observed_at(
                        existing,
                        &observation_watermarks.observed_at,
                    ) > record.observed_at
                        || local_cluster_connectivity_observation_is_stable(existing, record, now)
                })
                .unwrap_or(record);
            merged.insert(key, retained.clone());
        }
    }
    let retained_source_records = merged
        .values()
        .filter(|record| record.source_node_id == *source_node_id)
        .count();
    if retained_source_records > MAX_CLUSTER_CONNECTIVITY_SOURCE_RECORDS {
        return Err(NetworkError::ClusterConnectivityRecordLimit {
            source_node_id: source_node_id.clone(),
            observed_records: retained_source_records,
            max_records: MAX_CLUSTER_CONNECTIVITY_SOURCE_RECORDS,
        });
    }

    let mut committed_records = Vec::new();
    crate::object::try_reserve_vec(
        &mut committed_records,
        merged.len(),
        "collect generation-fenced connectivity records",
    )?;
    committed_records.extend(merged.into_values());
    sort_cluster_connectivity_records(&mut committed_records);
    let source_records = connectivity_source_records_from_slice(
        &committed_records,
        source_node_id,
        now,
        "collect committed connectivity source generation",
    )?;
    let records_changed = existing_records != committed_records;
    let route_records_stable =
        local_cluster_connectivity_route_records_stable(&existing_records, &committed_records);
    let persisted_records_stable = route_records_stable
        && local_cluster_connectivity_persisted_records_stable(
            &existing_records,
            &committed_records,
        );
    let persisted_records = if !persisted_records_stable {
        if committed_records.is_empty() {
            remove_optional_file(&paths.connectivity_local)?;
        } else {
            write_object_file(&paths.connectivity_local, &committed_records)?;
        }
        committed_records.clone()
    } else {
        existing_records
    };
    reconcile_local_cluster_connectivity_observation_watermarks(
        paths,
        &committed_records,
        records.iter(),
    )?;
    let mut cache =
        LOCAL_CLUSTER_CONNECTIVITY_RECORDS
            .lock()
            .map_err(|error| NetworkError::Io {
                operation: "lock local cluster connectivity cache".to_string(),
                detail: error.to_string(),
            })?;
    if committed_records.is_empty() {
        cache.remove(&paths.root);
    } else {
        store_local_cluster_connectivity_cache_entry(
            &mut cache,
            paths.root.clone(),
            committed_records,
            now,
        );
    }
    drop(cache);
    if records_changed {
        bump_local_cluster_connectivity_content_generation(paths);
    }
    if !route_records_stable {
        bump_local_cluster_connectivity_routing_generation(paths);
    }
    Ok(LocalClusterConnectivitySourceSetCommit::Committed {
        records: source_records,
        snapshot: LocalClusterConnectivityStoreSnapshot {
            content_generation: local_cluster_connectivity_content_generation(paths),
            persisted_records: Arc::new(persisted_records),
            observation_generations: Arc::new(
                local_cluster_connectivity_observation_watermarks(paths)?.generations,
            ),
        },
        content_changed: records_changed,
    })
}

#[doc(hidden)]
pub async fn commit_local_cluster_connectivity_source_set_if_snapshot_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    source_node_id: &NodeId,
    records: &[ClusterConnectivityRecord],
    expected_snapshot: &LocalClusterConnectivityStoreSnapshot,
) -> Result<LocalClusterConnectivitySourceSetCommit, NetworkError> {
    let paths = paths.clone();
    let state = state.clone();
    let source_node_id = source_node_id.clone();
    let records = records.to_vec();
    let expected_snapshot = expected_snapshot.clone();
    runtime
        .spawn_blocking(move || {
            commit_local_cluster_connectivity_source_set_if_snapshot(
                &paths,
                &state,
                &source_node_id,
                &records,
                &expected_snapshot,
            )
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "commit generation-fenced local connectivity source set".to_string(),
            detail: format!("local connectivity commit task failed: {source}"),
        })?
}

fn reconcile_local_cluster_connectivity_cache_with_persisted_records(
    paths: &ClusterPaths,
    persisted_records: &[ClusterConnectivityRecord],
    now: OffsetDateTime,
) -> Result<(), NetworkError> {
    let mut live_persisted_records = persisted_records
        .iter()
        .filter(|record| cluster_network_record_live(record.expires_at, now))
        .cloned()
        .collect::<Vec<_>>();
    sort_cluster_connectivity_records(&mut live_persisted_records);
    let mut cache =
        LOCAL_CLUSTER_CONNECTIVITY_RECORDS
            .lock()
            .map_err(|error| NetworkError::Io {
                operation: "lock local cluster connectivity cache".to_string(),
                detail: error.to_string(),
            })?;
    let cached_records = cache.get_mut(&paths.root).map(|entry| {
        prune_expired_local_cluster_connectivity_cache_entry(&paths.root, entry, now);
        let mut records = entry.records.clone();
        sort_cluster_connectivity_records(&mut records);
        records
    });
    let persisted_is_current = cached_records.as_ref().is_none_or(|cached_records| {
        cached_records == &live_persisted_records
            || (local_cluster_connectivity_persisted_records_stable(
                &live_persisted_records,
                cached_records,
            ) && local_cluster_connectivity_route_records_stable(
                &live_persisted_records,
                cached_records,
            ))
    });
    if persisted_is_current {
        if cached_records.is_none() && !live_persisted_records.is_empty() {
            store_local_cluster_connectivity_cache_entry(
                &mut cache,
                paths.root.clone(),
                live_persisted_records.clone(),
                now,
            );
        }
        drop(cache);
        reconcile_local_cluster_connectivity_observation_watermarks(
            paths,
            &live_persisted_records,
            live_persisted_records.iter(),
        )?;
        return Ok(());
    }
    if live_persisted_records.is_empty() {
        cache.remove(&paths.root);
    } else {
        store_local_cluster_connectivity_cache_entry(
            &mut cache,
            paths.root.clone(),
            live_persisted_records.clone(),
            now,
        );
    }
    drop(cache);
    // A materially different external snapshot is authoritative. Resetting
    // here prevents an observation order fence from a stale in-process cache
    // from masking the cross-process winner.
    reset_local_cluster_connectivity_observation_watermarks(paths, &live_persisted_records)?;
    bump_local_cluster_connectivity_content_generation(paths);
    bump_local_cluster_connectivity_routing_generation(paths);
    Ok(())
}

fn connectivity_source_records_from_slice(
    records: &[ClusterConnectivityRecord],
    source_node_id: &NodeId,
    now: OffsetDateTime,
    operation: &str,
) -> Result<Vec<ClusterConnectivityRecord>, NetworkError> {
    let mut source_records = Vec::new();
    for record in records.iter().filter(|record| {
        record.source_node_id == *source_node_id
            && cluster_network_record_live(record.expires_at, now)
    }) {
        crate::object::try_reserve_vec(&mut source_records, 1, operation)?;
        source_records.push(record.clone());
    }
    sort_cluster_connectivity_records(&mut source_records);
    Ok(source_records)
}

#[derive(Default)]
struct LocalConnectivityTopology {
    cluster_id: Option<sinomo_node::ClusterId>,
    authorized_nodes: BTreeSet<NodeId>,
    endpoints: BTreeMap<ClusterEndpointId, ClusterEndpointRecord>,
}

impl LocalConnectivityTopology {
    fn from_state(state: &ClusterBootstrapState) -> Self {
        let mut topology = Self {
            cluster_id: Some(state.cluster_id.clone()),
            authorized_nodes: BTreeSet::new(),
            endpoints: BTreeMap::new(),
        };
        for member in &state.members {
            if !member.state.is_authorized() {
                continue;
            }
            topology.authorized_nodes.insert(member.node_id.clone());
            for endpoint in &member.endpoints {
                topology
                    .endpoints
                    .entry(endpoint.endpoint_id.clone())
                    .or_insert_with(|| endpoint.clone());
            }
        }
        topology
    }

    fn record_is_usable(&self, record: &ClusterConnectivityRecord, now: OffsetDateTime) -> bool {
        if self
            .cluster_id
            .as_ref()
            .is_some_and(|cluster_id| record.cluster_id != *cluster_id)
            || !cluster_network_record_live(record.expires_at, now)
            || !self.authorized_nodes.contains(&record.source_node_id)
            || !self.authorized_nodes.contains(&record.target_node_id)
        {
            return false;
        }
        self.endpoints
            .get(&record.endpoint_id)
            .is_some_and(|endpoint| {
                endpoint.node_id == record.target_node_id
                    && endpoint.transport == record.transport
                    && endpoint.address == record.address
            })
    }
}

fn local_cluster_connectivity_cache_tick() -> u64 {
    LOCAL_CLUSTER_CONNECTIVITY_CACHE_CLOCK.fetch_add(1, Ordering::Relaxed)
}

fn read_local_cluster_connectivity_records_from_cache(
    paths: &ClusterPaths,
    now: OffsetDateTime,
) -> Result<Option<Vec<ClusterConnectivityRecord>>, NetworkError> {
    let mut cache =
        LOCAL_CLUSTER_CONNECTIVITY_RECORDS
            .lock()
            .map_err(|error| NetworkError::Io {
                operation: "lock local cluster connectivity cache".to_string(),
                detail: error.to_string(),
            })?;
    let Some(entry) = cache.get_mut(&paths.root) else {
        return Ok(None);
    };
    prune_expired_local_cluster_connectivity_cache_entry(&paths.root, entry, now);
    if entry.records.is_empty() {
        cache.remove(&paths.root);
        return Ok(Some(Vec::new()));
    }
    entry.last_used = local_cluster_connectivity_cache_tick();
    let mut records = entry.records.clone();
    sort_cluster_connectivity_records(&mut records);
    Ok(Some(records))
}

fn earliest_cluster_connectivity_expiry(
    records: &[ClusterConnectivityRecord],
) -> Option<OffsetDateTime> {
    records.iter().map(|record| record.expires_at).min()
}

#[cfg(any(test, feature = "test-support"))]
fn record_local_cluster_connectivity_prune_pass_for_test(path: &PathBuf) {
    if let Ok(mut passes) = LOCAL_CLUSTER_CONNECTIVITY_PRUNE_PASSES_FOR_TEST.lock() {
        *passes.entry(path.clone()).or_default() += 1;
    }
}

fn prune_expired_local_cluster_connectivity_cache_entry(
    _path: &PathBuf,
    entry: &mut LocalClusterConnectivityRecordCacheEntry,
    now: OffsetDateTime,
) {
    if !entry
        .earliest_expires_at
        .is_some_and(|expires_at| expires_at <= now)
    {
        return;
    }
    #[cfg(any(test, feature = "test-support"))]
    record_local_cluster_connectivity_prune_pass_for_test(_path);
    entry
        .records
        .retain(|record| cluster_network_record_live(record.expires_at, now));
    entry.earliest_expires_at = earliest_cluster_connectivity_expiry(&entry.records);
}

fn prune_expired_local_cluster_connectivity_cache_entries(
    cache: &mut HashMap<PathBuf, LocalClusterConnectivityRecordCacheEntry>,
    now: OffsetDateTime,
) {
    cache.retain(|path, entry| {
        prune_expired_local_cluster_connectivity_cache_entry(path, entry, now);
        !entry.records.is_empty()
    });
}

fn evict_local_cluster_connectivity_cache_entry_if_needed(
    cache: &mut HashMap<PathBuf, LocalClusterConnectivityRecordCacheEntry>,
    protected: &PathBuf,
) {
    if cache.contains_key(protected) || cache.len() < MAX_LOCAL_CLUSTER_CONNECTIVITY_CACHE_ENTRIES {
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

fn store_local_cluster_connectivity_cache_entry(
    cache: &mut HashMap<PathBuf, LocalClusterConnectivityRecordCacheEntry>,
    key: PathBuf,
    records: Vec<ClusterConnectivityRecord>,
    now: OffsetDateTime,
) {
    prune_expired_local_cluster_connectivity_cache_entries(cache, now);
    evict_local_cluster_connectivity_cache_entry_if_needed(cache, &key);
    cache.insert(
        key,
        LocalClusterConnectivityRecordCacheEntry {
            earliest_expires_at: earliest_cluster_connectivity_expiry(&records),
            records,
            last_used: local_cluster_connectivity_cache_tick(),
        },
    );
}

fn replace_local_cluster_connectivity_cache_records(
    paths: &ClusterPaths,
    mut records: Vec<ClusterConnectivityRecord>,
    now: OffsetDateTime,
) -> Result<bool, NetworkError> {
    sort_cluster_connectivity_records(&mut records);
    let mut cache =
        LOCAL_CLUSTER_CONNECTIVITY_RECORDS
            .lock()
            .map_err(|error| NetworkError::Io {
                operation: "lock local cluster connectivity cache".to_string(),
                detail: error.to_string(),
            })?;
    let changed = match cache.get(&paths.root) {
        Some(entry) => {
            let mut cached_records = entry.records.clone();
            sort_cluster_connectivity_records(&mut cached_records);
            cached_records != records
        }
        None => !records.is_empty(),
    };
    if records.is_empty() {
        cache.remove(&paths.root);
    } else {
        store_local_cluster_connectivity_cache_entry(&mut cache, paths.root.clone(), records, now);
    }
    Ok(changed)
}

/// Snapshots the process-local observation order fence for one store root.
///
/// Stable timestamp/latency observations deliberately do not mutate the
/// material record set, but their timestamp still prevents an older failure or
/// full-set task from landing afterward. Keeping this bounded watermark beside
/// (rather than inside) the record cache preserves ordering without turning
/// observation traffic into content/routing generations.
fn local_cluster_connectivity_observation_watermarks(
    paths: &ClusterPaths,
) -> Result<LocalClusterConnectivityObservationWatermarkEntry, NetworkError> {
    let mut watermarks = LOCAL_CLUSTER_CONNECTIVITY_OBSERVATION_WATERMARKS
        .lock()
        .map_err(|error| NetworkError::Io {
            operation: "lock local cluster connectivity observation watermarks".to_string(),
            detail: error.to_string(),
        })?;
    let Some(entry) = watermarks.get_mut(&paths.root) else {
        return Ok(LocalClusterConnectivityObservationWatermarkEntry::default());
    };
    entry.last_used = local_cluster_connectivity_cache_tick();
    Ok(entry.clone())
}

fn advance_local_cluster_connectivity_observation_watermark(
    entry: &mut LocalClusterConnectivityObservationWatermarkEntry,
    key: LocalClusterConnectivityRecordKey,
    observed_at: OffsetDateTime,
) {
    let latest_observed_at = entry
        .observed_at
        .get(&key)
        .copied()
        .map(|existing| existing.max(observed_at))
        .unwrap_or(observed_at);
    let advanced = entry.observed_at.get(&key).copied() != Some(latest_observed_at)
        || !entry.generations.contains_key(&key);
    if !advanced {
        return;
    }
    entry.observed_at.insert(key.clone(), latest_observed_at);
    entry
        .generations
        .insert(key, local_cluster_connectivity_cache_tick());
}

fn reconcile_local_cluster_connectivity_observation_watermarks<'a>(
    paths: &ClusterPaths,
    retained_records: &[ClusterConnectivityRecord],
    observations: impl IntoIterator<Item = &'a ClusterConnectivityRecord>,
) -> Result<(), NetworkError> {
    let retained_keys = retained_records
        .iter()
        .map(cluster_connectivity_record_key)
        .collect::<BTreeSet<_>>();
    let mut watermarks = LOCAL_CLUSTER_CONNECTIVITY_OBSERVATION_WATERMARKS
        .lock()
        .map_err(|error| NetworkError::Io {
            operation: "lock local cluster connectivity observation watermarks".to_string(),
            detail: error.to_string(),
        })?;
    if !watermarks.contains_key(&paths.root) {
        while watermarks.len() >= MAX_LOCAL_CLUSTER_CONNECTIVITY_CACHE_ENTRIES {
            let Some(evicted) = watermarks
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(path, _)| path.clone())
            else {
                break;
            };
            watermarks.remove(&evicted);
        }
    }
    let entry = watermarks.entry(paths.root.clone()).or_default();
    entry
        .observed_at
        .retain(|key, _| retained_keys.contains(key));
    entry
        .generations
        .retain(|key, _| retained_keys.contains(key));
    for record in retained_records {
        advance_local_cluster_connectivity_observation_watermark(
            entry,
            cluster_connectivity_record_key(record),
            record.observed_at,
        );
    }
    for record in observations {
        let key = cluster_connectivity_record_key(record);
        if !retained_keys.contains(&key) {
            continue;
        }
        advance_local_cluster_connectivity_observation_watermark(entry, key, record.observed_at);
    }
    entry.last_used = local_cluster_connectivity_cache_tick();
    if entry.observed_at.is_empty() {
        watermarks.remove(&paths.root);
    }
    Ok(())
}

fn reset_local_cluster_connectivity_observation_watermarks(
    paths: &ClusterPaths,
    retained_records: &[ClusterConnectivityRecord],
) -> Result<(), NetworkError> {
    LOCAL_CLUSTER_CONNECTIVITY_OBSERVATION_WATERMARKS
        .lock()
        .map_err(|error| NetworkError::Io {
            operation: "lock local cluster connectivity observation watermarks".to_string(),
            detail: error.to_string(),
        })?
        .remove(&paths.root);
    reconcile_local_cluster_connectivity_observation_watermarks(
        paths,
        retained_records,
        std::iter::empty::<&ClusterConnectivityRecord>(),
    )
}

fn local_cluster_connectivity_latest_observed_at(
    record: &ClusterConnectivityRecord,
    watermarks: &BTreeMap<LocalClusterConnectivityRecordKey, OffsetDateTime>,
) -> OffsetDateTime {
    watermarks
        .get(&cluster_connectivity_record_key(record))
        .copied()
        .unwrap_or(record.observed_at)
        .max(record.observed_at)
}

fn local_cluster_connectivity_observation_advanced_since_snapshot(
    key: &LocalClusterConnectivityRecordKey,
    current_generations: &BTreeMap<LocalClusterConnectivityRecordKey, u64>,
    expected_snapshot: &LocalClusterConnectivityStoreSnapshot,
) -> bool {
    current_generations.get(key) != expected_snapshot.observation_generations.get(key)
}

/// Whether a new observation carries no material connectivity information.
///
/// Probe and command RPC success timestamps are diagnostic evidence, not a
/// control-plane generation. Exact latency samples are likewise kept on the
/// in-memory probe/BDP paths rather than turning normal jitter into local file,
/// route-cache, and cluster-KV churn. Failure-count progress is material even
/// before it crosses a reachability-state boundary, so it remains part of this
/// projection.
///
/// A stable record is retained only while it has more than the probe refresh
/// margin left. Once expiry enters that margin, accepting the new observation
/// renews the durable evidence and gives the ordinary publisher enough time to
/// refresh cluster KV before the old record expires.
fn local_cluster_connectivity_observation_is_stable(
    existing: &ClusterConnectivityRecord,
    observed: &ClusterConnectivityRecord,
    now: OffsetDateTime,
) -> bool {
    cluster_connectivity_stable_eq(existing, observed)
        && existing.consecutive_failures == observed.consecutive_failures
        && !cluster_connectivity_record_should_refresh(existing.expires_at, now)
}

fn local_cluster_connectivity_persisted_records_stable(
    existing_records: &[ClusterConnectivityRecord],
    merged_records: &[ClusterConnectivityRecord],
) -> bool {
    existing_records.len() == merged_records.len()
        && existing_records
            .iter()
            .zip(merged_records)
            .all(|(existing, merged)| cluster_connectivity_stable_eq(existing, merged))
}

fn local_cluster_connectivity_raw_persisted_records_stable(
    existing_records: &[ClusterConnectivityRecord],
    source_node_id: &NodeId,
    records: &[ClusterConnectivityRecord],
    replace_source_records: bool,
) -> bool {
    let mut candidate = existing_records
        .iter()
        .map(|record| (cluster_connectivity_record_key(record), record.clone()))
        .collect::<BTreeMap<_, _>>();
    if replace_source_records {
        candidate.retain(|(_, candidate_source_node_id, _), _| {
            candidate_source_node_id != source_node_id
        });
    }
    for record in records {
        candidate.insert(cluster_connectivity_record_key(record), record.clone());
    }
    let mut candidate = candidate.into_values().collect::<Vec<_>>();
    sort_cluster_connectivity_records(&mut candidate);
    local_cluster_connectivity_persisted_records_stable(existing_records, &candidate)
}

fn local_cluster_connectivity_route_records_stable(
    existing_records: &[ClusterConnectivityRecord],
    merged_records: &[ClusterConnectivityRecord],
) -> bool {
    existing_records.len() == merged_records.len()
        && existing_records
            .iter()
            .zip(merged_records)
            .all(local_cluster_connectivity_routing_record_stable_eq)
}

/// Route-relevant projection shared by the local connectivity store and
/// higher-level reactive consumers of the committed connectivity directory.
///
/// Observation timestamps, diagnostic strings, expiry extensions, and latency
/// samples are intentionally excluded. Live path metrics own measurement;
/// connectivity generations own reachability and topology.
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterConnectivityRoutingProjection {
    cluster_id: ClusterId,
    source_node_id: NodeId,
    target_node_id: NodeId,
    endpoint_id: ClusterEndpointId,
    transport: ClusterTransportKind,
    address: String,
    state: ClusterConnectivityState,
    failure_reason: Option<crate::connectivity::ClusterConnectivityFailureReason>,
    consecutive_failures: u16,
}

/// Returns the exact semantic input used to invalidate cached route ordering.
#[doc(hidden)]
pub fn cluster_connectivity_routing_projection(
    record: &ClusterConnectivityRecord,
) -> ClusterConnectivityRoutingProjection {
    ClusterConnectivityRoutingProjection {
        cluster_id: record.cluster_id.clone(),
        source_node_id: record.source_node_id.clone(),
        target_node_id: record.target_node_id.clone(),
        endpoint_id: record.endpoint_id.clone(),
        transport: record.transport.clone(),
        address: record.address.clone(),
        state: record.state.clone(),
        failure_reason: record.failure_reason.clone(),
        consecutive_failures: record.consecutive_failures,
    }
}

fn local_cluster_connectivity_routing_record_stable_eq(
    existing: (&ClusterConnectivityRecord, &ClusterConnectivityRecord),
) -> bool {
    let (existing, merged) = existing;
    cluster_connectivity_routing_projection(existing)
        == cluster_connectivity_routing_projection(merged)
        && merged.expires_at >= existing.expires_at
}

fn cluster_connectivity_record_key(
    record: &ClusterConnectivityRecord,
) -> LocalClusterConnectivityRecordKey {
    (
        record.source_node_id.clone(),
        record.target_node_id.clone(),
        record.endpoint_id.clone(),
    )
}

fn write_local_cluster_connectivity_records_inner(
    paths: &ClusterPaths,
    source_node_id: &NodeId,
    records: &[ClusterConnectivityRecord],
    replace_source_records: bool,
    persist_observation_details: bool,
) -> Result<(), NetworkError> {
    validate_cluster_connectivity_records_for_source(source_node_id, records)?;
    let _mutation_lock = acquire_local_cluster_connectivity_mutation_lock(paths)?;
    let now = OffsetDateTime::now_utc();
    // Always reload the shared file after acquiring the process-safe lock. The
    // in-process route cache is intentionally allowed to lag an external
    // daemon-control writer and therefore cannot be the base of a mutation.
    let existing_records = read_persisted_live_local_cluster_connectivity_records(paths, now)?;
    let observation_watermarks = local_cluster_connectivity_observation_watermarks(paths)?;
    let replacement_keys = if replace_source_records {
        records
            .iter()
            .filter(|record| {
                record.source_node_id == *source_node_id
                    && cluster_network_record_live(record.expires_at, now)
            })
            .map(cluster_connectivity_record_key)
            .collect::<BTreeSet<_>>()
    } else {
        BTreeSet::new()
    };
    let replacement_observed_at = replace_source_records
        .then(|| {
            records
                .iter()
                .filter(|record| {
                    record.source_node_id == *source_node_id
                        && cluster_network_record_live(record.expires_at, now)
                })
                .map(|record| record.observed_at)
                .max()
        })
        .flatten();
    let mut merged = existing_records
        .iter()
        .filter(|record| {
            cluster_network_record_live(record.expires_at, now)
                && (!replace_source_records
                    || record.source_node_id != *source_node_id
                    || replacement_keys.contains(&cluster_connectivity_record_key(record))
                    // This timestamp check is a conservative same-process
                    // defense for non-empty authoritative sets, not a source-set
                    // concurrency token: omitted keys have no observation of
                    // their own in the incoming value. Async producers use the
                    // generation-fenced commit API above. An explicit empty set
                    // remains an explicit authority-owned clear.
                    || replacement_observed_at
                        .is_some_and(|observed_at| {
                            local_cluster_connectivity_latest_observed_at(
                                record,
                                &observation_watermarks.observed_at,
                            ) > observed_at
                        }))
        })
        .map(|record| (cluster_connectivity_record_key(record), record.clone()))
        .collect::<BTreeMap<_, _>>();
    for record in records {
        if record.source_node_id == *source_node_id
            && cluster_network_record_live(record.expires_at, now)
        {
            let key = cluster_connectivity_record_key(record);
            if merged.get(&key).is_some_and(|existing| {
                local_cluster_connectivity_latest_observed_at(
                    existing,
                    &observation_watermarks.observed_at,
                ) > record.observed_at
            }) {
                // Observation tasks and full-set maintenance writes can finish
                // out of order. Never let a late older result regress a newer
                // reachability decision and make cluster KV oscillate.
                continue;
            }
            if !persist_observation_details
                && merged.get(&key).is_some_and(|existing| {
                    local_cluster_connectivity_observation_is_stable(existing, record, now)
                })
            {
                continue;
            }
            merged.insert(key, record.clone());
        }
    }
    let retained_source_records = merged
        .values()
        .filter(|record| record.source_node_id == *source_node_id)
        .count();
    if retained_source_records > MAX_CLUSTER_CONNECTIVITY_SOURCE_RECORDS {
        return Err(NetworkError::ClusterConnectivityRecordLimit {
            source_node_id: source_node_id.clone(),
            observed_records: retained_source_records,
            max_records: MAX_CLUSTER_CONNECTIVITY_SOURCE_RECORDS,
        });
    }
    let mut merged_records = Vec::new();
    crate::object::try_reserve_vec(
        &mut merged_records,
        merged.len(),
        "collect merged connectivity records",
    )?;
    merged_records.extend(merged.into_values());
    let mut merged = merged_records;
    sort_cluster_connectivity_records(&mut merged);
    let records_changed = existing_records != merged;
    let route_records_stable =
        local_cluster_connectivity_route_records_stable(&existing_records, &merged);
    let mut persisted_records_stable = existing_records == merged
        || (!persist_observation_details
            && route_records_stable
            && local_cluster_connectivity_persisted_records_stable(&existing_records, &merged));
    let explicit_source_clear = replace_source_records && records.is_empty();
    if !persisted_records_stable
        && !explicit_source_clear
        && !persist_observation_details
        && let Some(mut raw_existing_records) =
            read_optional_object_file::<Vec<ClusterConnectivityRecord>>(&paths.connectivity_local)?
    {
        sort_cluster_connectivity_records(&mut raw_existing_records);
        persisted_records_stable = local_cluster_connectivity_raw_persisted_records_stable(
            &raw_existing_records,
            source_node_id,
            records,
            replace_source_records,
        );
    }
    if !persisted_records_stable {
        if merged.is_empty() {
            remove_optional_file(&paths.connectivity_local)?;
        } else {
            write_object_file(&paths.connectivity_local, &merged)?;
        }
    }
    reconcile_local_cluster_connectivity_observation_watermarks(paths, &merged, records.iter())?;
    let mut cache =
        LOCAL_CLUSTER_CONNECTIVITY_RECORDS
            .lock()
            .map_err(|error| NetworkError::Io {
                operation: "lock local cluster connectivity cache".to_string(),
                detail: error.to_string(),
            })?;
    if merged.is_empty() {
        cache.remove(&paths.root);
    } else {
        store_local_cluster_connectivity_cache_entry(&mut cache, paths.root.clone(), merged, now);
    }
    if records_changed {
        bump_local_cluster_connectivity_content_generation(paths);
    }
    if !route_records_stable {
        bump_local_cluster_connectivity_routing_generation(paths);
    }
    Ok(())
}

pub(crate) fn sort_cluster_connectivity_records(records: &mut [ClusterConnectivityRecord]) {
    records.sort_by(|left, right| {
        (
            left.source_node_id.as_str(),
            left.target_node_id.as_str(),
            left.endpoint_id.as_str(),
        )
            .cmp(&(
                right.source_node_id.as_str(),
                right.target_node_id.as_str(),
                right.endpoint_id.as_str(),
            ))
    });
}

#[cfg(any(test, feature = "test-support"))]
pub fn local_cluster_connectivity_record_cache_contains_for_test(paths: &ClusterPaths) -> bool {
    LOCAL_CLUSTER_CONNECTIVITY_RECORDS
        .lock()
        .map(|cache| cache.contains_key(&paths.root))
        .unwrap_or(false)
}

#[cfg(any(test, feature = "test-support"))]
pub fn reset_local_cluster_connectivity_prune_passes_for_test(paths: &ClusterPaths) {
    if let Ok(mut passes) = LOCAL_CLUSTER_CONNECTIVITY_PRUNE_PASSES_FOR_TEST.lock() {
        passes.remove(&paths.root);
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn local_cluster_connectivity_prune_passes_for_test(paths: &ClusterPaths) -> u64 {
    LOCAL_CLUSTER_CONNECTIVITY_PRUNE_PASSES_FOR_TEST
        .lock()
        .ok()
        .and_then(|passes| passes.get(&paths.root).copied())
        .unwrap_or(0)
}

#[cfg(any(test, feature = "test-support"))]
pub fn local_cluster_connectivity_record_cache_capacity_for_test() -> usize {
    MAX_LOCAL_CLUSTER_CONNECTIVITY_CACHE_ENTRIES
}

#[cfg(any(test, feature = "test-support"))]
pub fn remove_local_cluster_connectivity_record_cache_entries_under_root_for_test(
    root: &std::path::Path,
) {
    if let Ok(mut cache) = LOCAL_CLUSTER_CONNECTIVITY_RECORDS.lock() {
        cache.retain(|path, _| !path.starts_with(root));
    }
    if let Ok(mut watermarks) = LOCAL_CLUSTER_CONNECTIVITY_OBSERVATION_WATERMARKS.lock() {
        watermarks.retain(|path, _| !path.starts_with(root));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectivity::{CLUSTER_CONNECTIVITY_TTL_SECONDS, cluster_connectivity_record};
    use sinomo_base::{
        ClusterEndpointPrivacy, ClusterEndpointSource, ClusterMemberRecord, ClusterTransportKind,
    };
    use sinomo_local::SinomoDataDir;
    use sinomo_node::ClusterId;

    fn connectivity_generation_fixture(
        name: &str,
    ) -> (
        sinomo_testdir::TestDir,
        ClusterPaths,
        ClusterBootstrapState,
        NodeId,
        ClusterEndpointRecord,
        OffsetDateTime,
    ) {
        let root = sinomo_testdir::TestDir::new(name);
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("layout");
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named(format!("cluster_{name}"))
            .expect("generation fixture cluster");
        let local = NodeId::from_legacy_named(format!("node_{name}_local"))
            .expect("generation fixture local node");
        let remote = NodeId::from_legacy_named(format!("node_{name}_remote"))
            .expect("generation fixture remote node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "connectivity generation fixture",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &remote,
            "target:21353".to_string(),
            10,
        );
        let mut remote_member =
            ClusterMemberRecord::local_non_voting(cluster_id, remote, "remote", "pk", now);
        remote_member.endpoints = vec![endpoint.clone()];
        state.members.push(remote_member);
        (root, paths, state, local, endpoint, now)
    }

    #[test]
    fn identical_healthy_source_generation_preserves_store_and_generations() {
        let (_root, paths, state, local, endpoint, now) =
            connectivity_generation_fixture("connectivity_observation_generation");
        let initial = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(3),
            0,
            Some("initial success".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_records(&paths, &local, &[initial])
            .expect("write initial connectivity observation");
        let initial = read_local_cluster_connectivity_records_for_source(&paths, &local)
            .expect("read initial source generation")[0]
            .clone();
        let initial_bytes = std::fs::read(&paths.connectivity_local)
            .expect("read initial persisted connectivity bytes");
        let expected_snapshot = capture_local_cluster_connectivity_store_snapshot(&paths)
            .expect("capture initial source generation");
        let content_before = local_cluster_connectivity_content_generation(&paths);
        let routing_before = local_cluster_connectivity_routing_generation(&paths);

        let refreshed_at = now + time::Duration::seconds(1);
        let refreshed = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(777),
            0,
            Some("refreshed success".to_string()),
            None,
            refreshed_at,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let outcome = commit_local_cluster_connectivity_source_set_if_snapshot(
            &paths,
            &state,
            &local,
            &[refreshed],
            &expected_snapshot,
        )
        .expect("commit repeated healthy source generation");

        assert!(
            matches!(
                &outcome,
                LocalClusterConnectivitySourceSetCommit::Committed {
                    records,
                    content_changed: false,
                    ..
                } if records == std::slice::from_ref(&initial)
            ),
            "a latency/timestamp-only generation must retain the prior durable evidence: {outcome:?}"
        );
        assert_eq!(
            local_cluster_connectivity_content_generation(&paths),
            content_before,
            "a repeated healthy observation is not a content generation"
        );
        assert_eq!(
            local_cluster_connectivity_routing_generation(&paths),
            routing_before,
            "latency and timestamp refreshes must not invalidate the delivery routing table"
        );
        assert_eq!(
            std::fs::read(&paths.connectivity_local)
                .expect("read connectivity bytes after stable generation"),
            initial_bytes,
            "stable observations must not rewrite the local object file"
        );
        assert_eq!(
            read_local_cluster_connectivity_records_for_source(&paths, &local)
                .expect("read stable source generation"),
            vec![initial],
            "the in-process cache must retain the same source generation as disk"
        );
    }

    #[test]
    fn stable_noop_observation_fences_late_material_source_generation() {
        let (_root, paths, mut state, local, endpoint_a, now) =
            connectivity_generation_fixture("connectivity_stable_observation_order_fence");
        let mut endpoint_b = endpoint_a.clone();
        endpoint_b.endpoint_id = ClusterEndpointId::derived(
            "sinomo.test.connectivity.stable-order-fence.endpoint-b",
            b"endpoint-b",
        );
        endpoint_b.address = "target:21354".to_string();
        state
            .members
            .iter_mut()
            .find(|member| member.node_id == endpoint_b.node_id)
            .expect("remote member for endpoint b")
            .endpoints
            .push(endpoint_b.clone());

        let initial_a = cluster_connectivity_record(
            &state,
            &endpoint_a,
            ClusterConnectivityState::Healthy,
            Some(3),
            0,
            Some("initial a".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let initial_b = cluster_connectivity_record(
            &state,
            &endpoint_b,
            ClusterConnectivityState::Healthy,
            Some(4),
            0,
            Some("initial b".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_record_set(
            &paths,
            &local,
            &[initial_a.clone(), initial_b.clone()],
        )
        .expect("seed initial source generation");
        let expected_snapshot = capture_local_cluster_connectivity_store_snapshot(&paths)
            .expect("capture pending probe base");
        let content_before = local_cluster_connectivity_content_generation(&paths);
        let routing_before = local_cluster_connectivity_routing_generation(&paths);

        let stable_at = now + time::Duration::seconds(2);
        let stable_a = cluster_connectivity_record(
            &state,
            &endpoint_a,
            ClusterConnectivityState::Healthy,
            Some(777),
            0,
            Some("newer command success a".to_string()),
            None,
            stable_at,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let stable_b = cluster_connectivity_record(
            &state,
            &endpoint_b,
            ClusterConnectivityState::Healthy,
            Some(888),
            0,
            Some("newer command success b".to_string()),
            None,
            stable_at,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_records(&paths, &local, &[stable_a, stable_b])
            .expect("record stable command observations");
        assert_eq!(
            local_cluster_connectivity_content_generation(&paths),
            content_before,
            "the ordering fence must not become a publish generation"
        );

        let stale_at = now + time::Duration::seconds(1);
        let stale_a = cluster_connectivity_record(
            &state,
            &endpoint_a,
            ClusterConnectivityState::Degraded,
            None,
            1,
            None,
            Some("late probe failure".to_string()),
            stale_at,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let outcome = commit_local_cluster_connectivity_source_set_if_snapshot(
            &paths,
            &state,
            &local,
            &[stale_a],
            &expected_snapshot,
        )
        .expect("commit late source generation");

        let mut expected = vec![initial_a, initial_b];
        sort_cluster_connectivity_records(&mut expected);
        assert!(
            matches!(
                &outcome,
                LocalClusterConnectivitySourceSetCommit::Committed {
                    records,
                    content_changed: false,
                    ..
                } if records == &expected
            ),
            "the quiet observation fence must retain both the newer same-key state and newer omitted endpoint: {outcome:?}"
        );
        assert_eq!(
            local_cluster_connectivity_content_generation(&paths),
            content_before
        );
        assert_eq!(
            local_cluster_connectivity_routing_generation(&paths),
            routing_before
        );

        let empty_outcome = commit_local_cluster_connectivity_source_set_if_snapshot(
            &paths,
            &state,
            &local,
            &[],
            &expected_snapshot,
        )
        .expect("commit stale empty source generation");
        assert!(
            matches!(
                empty_outcome,
                LocalClusterConnectivitySourceSetCommit::Committed {
                    records,
                    content_changed: false,
                    ..
                } if records == expected
            ),
            "an empty task captured before a stable observation must remain read-only"
        );

        let fresh_snapshot = capture_local_cluster_connectivity_store_snapshot(&paths)
            .expect("capture post-observation source generation");
        assert_eq!(
            fresh_snapshot, expected_snapshot,
            "observation ordering must not alter material snapshot equality"
        );
        let fresh_empty_outcome = commit_local_cluster_connectivity_source_set_if_snapshot(
            &paths,
            &state,
            &local,
            &[],
            &fresh_snapshot,
        )
        .expect("commit fresh empty source generation");
        assert!(
            matches!(
                fresh_empty_outcome,
                LocalClusterConnectivitySourceSetCommit::Committed {
                    records,
                    content_changed: true,
                    ..
                } if records.is_empty()
            ),
            "an empty task captured after the observation remains an explicit clear"
        );
    }

    #[test]
    fn source_set_omission_uses_captured_key_generation_not_latest_result_time() {
        let (_root, paths, mut state, local, endpoint_a, now) =
            connectivity_generation_fixture("connectivity_per_key_omission_fence");
        let mut endpoint_b = endpoint_a.clone();
        endpoint_b.endpoint_id = ClusterEndpointId::derived(
            "sinomo.test.connectivity.per-key-omission.endpoint-b",
            b"endpoint-b",
        );
        endpoint_b.address = "target:21354".to_string();
        state
            .members
            .iter_mut()
            .find(|member| member.node_id == endpoint_b.node_id)
            .expect("remote member for endpoint b")
            .endpoints
            .push(endpoint_b.clone());

        let initial_a = cluster_connectivity_record(
            &state,
            &endpoint_a,
            ClusterConnectivityState::Healthy,
            Some(3),
            0,
            Some("initial a".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let initial_b = cluster_connectivity_record(
            &state,
            &endpoint_b,
            ClusterConnectivityState::Healthy,
            Some(4),
            0,
            Some("initial b".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_record_set(
            &paths,
            &local,
            &[initial_a, initial_b.clone()],
        )
        .expect("seed initial source generation");
        let expected_snapshot = capture_local_cluster_connectivity_store_snapshot(&paths)
            .expect("capture pending mixed probe base");

        let stable_b = cluster_connectivity_record(
            &state,
            &endpoint_b,
            ClusterConnectivityState::Healthy,
            Some(888),
            0,
            Some("newer command success b".to_string()),
            None,
            now + time::Duration::seconds(2),
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_records(&paths, &local, &[stable_b])
            .expect("record stable endpoint b observation");
        let later_a = cluster_connectivity_record(
            &state,
            &endpoint_a,
            ClusterConnectivityState::Degraded,
            None,
            1,
            None,
            Some("later probe failure a".to_string()),
            now + time::Duration::seconds(3),
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let outcome = commit_local_cluster_connectivity_source_set_if_snapshot(
            &paths,
            &state,
            &local,
            std::slice::from_ref(&later_a),
            &expected_snapshot,
        )
        .expect("commit mixed-time source generation");

        let mut expected = vec![later_a, initial_b];
        sort_cluster_connectivity_records(&mut expected);
        assert!(
            matches!(
                outcome,
                LocalClusterConnectivitySourceSetCommit::Committed {
                    records,
                    content_changed: true,
                    ..
                } if records == expected
            ),
            "endpoint b changed after capture and must survive even though endpoint a completed later"
        );
    }

    #[test]
    fn identical_healthy_source_generation_renews_inside_expiry_margin() {
        let (_root, paths, state, local, endpoint, now) =
            connectivity_generation_fixture("connectivity_observation_expiry_refresh");
        let mut expiring = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(3),
            0,
            Some("initial success".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        expiring.expires_at = OffsetDateTime::now_utc() + time::Duration::seconds(30);
        write_local_cluster_connectivity_records(&paths, &local, &[expiring])
            .expect("write expiring connectivity observation");
        let expected_snapshot = capture_local_cluster_connectivity_store_snapshot(&paths)
            .expect("capture expiring source generation");
        let content_before = local_cluster_connectivity_content_generation(&paths);

        let refreshed_at = OffsetDateTime::now_utc();
        let refreshed = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(9),
            0,
            Some("renewed success".to_string()),
            None,
            refreshed_at,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let outcome = commit_local_cluster_connectivity_source_set_if_snapshot(
            &paths,
            &state,
            &local,
            std::slice::from_ref(&refreshed),
            &expected_snapshot,
        )
        .expect("renew expiring healthy source generation");

        assert!(
            matches!(
                &outcome,
                LocalClusterConnectivitySourceSetCommit::Committed {
                    records,
                    content_changed: true,
                    ..
                } if records == std::slice::from_ref(&refreshed)
            ),
            "expiry-driven renewal must remain a real durable generation: {outcome:?}"
        );
        assert_ne!(
            local_cluster_connectivity_content_generation(&paths),
            content_before
        );
    }

    #[tokio::test]
    async fn routing_generation_subscription_emits_only_semantic_route_changes() {
        let (_root, paths, state, local, endpoint, now) =
            connectivity_generation_fixture("connectivity_routing_generation_subscription");
        let mut changes = subscribe_local_cluster_connectivity_routing_generation(&paths);
        let initial = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(3),
            0,
            Some("initial success".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_records(&paths, &local, &[initial])
            .expect("write initial connectivity observation");
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(1), changes.changed())
                .await
                .expect("initial route generation wake"),
            local_cluster_connectivity_routing_generation(&paths)
        );

        let refreshed_at = now + time::Duration::seconds(1);
        let refreshed = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(7),
            0,
            Some("refreshed success".to_string()),
            None,
            refreshed_at,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_records(&paths, &local, &[refreshed])
            .expect("refresh connectivity observation");
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(25), changes.changed())
                .await
                .is_err(),
            "timestamp and same-class latency refreshes must stay quiet"
        );

        let degraded_at = now + time::Duration::seconds(2);
        let degraded = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Degraded,
            None,
            1,
            Some("refreshed success".to_string()),
            Some("route failed".to_string()),
            degraded_at,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_records(&paths, &local, &[degraded])
            .expect("write degraded connectivity observation");
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(1), changes.changed())
                .await
                .expect("degraded route generation wake"),
            local_cluster_connectivity_routing_generation(&paths)
        );
    }

    #[test]
    fn explicit_invalidation_reloads_cross_process_connectivity_write() {
        let (_root, paths, state, local, endpoint, now) =
            connectivity_generation_fixture("connectivity_cross_process_reload");
        let initial = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(3),
            0,
            Some("initial success".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_records(&paths, &local, &[initial])
            .expect("write and cache initial connectivity");

        let changed_at = now + time::Duration::seconds(1);
        let externally_written = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Degraded,
            None,
            2,
            None,
            Some("external failure".to_string()),
            changed_at,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_object_file(&paths.connectivity_local, &vec![externally_written])
            .expect("simulate another process replacing connectivity file");
        assert_eq!(
            read_local_cluster_connectivity_records(&paths).expect("read cached connectivity")[0]
                .state,
            ClusterConnectivityState::Healthy,
            "the test must prewarm the stale in-process cache"
        );

        invalidate_local_cluster_connectivity_cache(&paths).expect("invalidate connectivity");

        assert_eq!(
            read_local_cluster_connectivity_records(&paths).expect("reload connectivity from disk")
                [0]
            .state,
            ClusterConnectivityState::Degraded
        );
    }

    #[test]
    fn persisted_snapshot_fence_preserves_child_process_source_write() {
        let (root, paths, state, local, endpoint, now) =
            connectivity_generation_fixture("connectivity_child_process_snapshot_fence");
        let expected_snapshot = capture_local_cluster_connectivity_store_snapshot(&paths)
            .expect("capture empty persisted connectivity base");
        let parent_generation = local_cluster_connectivity_content_generation(&paths);
        let externally_written = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(2),
            0,
            Some("child process success".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let data_dir = SinomoDataDir::new(&root);
        let child_records_path = data_dir.root().join("child-connectivity-records.sobj");
        write_object_file(&child_records_path, &vec![externally_written.clone()])
            .expect("write child process input");
        let child = std::process::Command::new(std::env::current_exe().expect("current test exe"))
            .arg("--exact")
            .arg("connectivity_store::tests::external_connectivity_writer_child")
            .arg("--nocapture")
            .env("SINOMO_CONNECTIVITY_WRITER_CHILD_DATA_DIR", data_dir.root())
            .env("SINOMO_CONNECTIVITY_WRITER_CHILD_SOURCE", local.to_string())
            .env(
                "SINOMO_CONNECTIVITY_WRITER_CHILD_RECORDS",
                &child_records_path,
            )
            .output()
            .expect("run external connectivity writer child");
        assert!(
            child.status.success(),
            "external connectivity writer child failed: stdout={} stderr={}",
            String::from_utf8_lossy(&child.stdout),
            String::from_utf8_lossy(&child.stderr),
        );
        assert_eq!(
            local_cluster_connectivity_content_generation(&paths),
            parent_generation,
            "a child process cannot advance this process's in-memory generation"
        );

        let outcome = commit_local_cluster_connectivity_source_set_if_snapshot(
            &paths,
            &state,
            &local,
            &[],
            &expected_snapshot,
        )
        .expect("generation-fenced empty commit");
        assert!(
            matches!(
                &outcome,
                LocalClusterConnectivitySourceSetCommit::Superseded { records, .. }
                    if records == std::slice::from_ref(&externally_written)
            ),
            "the exact persisted snapshot, not the process-local generation, must fence the clear: {outcome:?}"
        );
        assert_eq!(
            read_local_cluster_connectivity_records_for_source(&paths, &local)
                .expect("read child process source write"),
            vec![externally_written]
        );
    }

    #[test]
    fn persisted_snapshot_fence_observes_external_stable_command() {
        let (root, paths, state, local, endpoint, now) =
            connectivity_generation_fixture("connectivity_external_stable_command_fence");
        let initial = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(3),
            0,
            Some("initial success".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_records(&paths, &local, &[initial])
            .expect("seed initial connectivity");
        let expected_snapshot = capture_local_cluster_connectivity_store_snapshot(&paths)
            .expect("capture pending probe base");
        let routing_before = local_cluster_connectivity_routing_generation(&paths);

        let data_dir = SinomoDataDir::new(&root);
        let state_path = data_dir.root().join("external-command-state.sobj");
        let endpoint_path = data_dir.root().join("external-command-endpoint.sobj");
        write_object_file(&state_path, &state).expect("write child state");
        write_object_file(&endpoint_path, &endpoint).expect("write child endpoint");
        let child = std::process::Command::new(std::env::current_exe().expect("current test exe"))
            .arg("--exact")
            .arg("connectivity_store::tests::external_stable_command_writer_child")
            .arg("--nocapture")
            .env(
                "SINOMO_CONNECTIVITY_COMMAND_CHILD_DATA_DIR",
                data_dir.root(),
            )
            .env("SINOMO_CONNECTIVITY_COMMAND_CHILD_STATE", &state_path)
            .env("SINOMO_CONNECTIVITY_COMMAND_CHILD_ENDPOINT", &endpoint_path)
            .output()
            .expect("run external stable command child");
        assert!(
            child.status.success(),
            "external stable command child failed: stdout={} stderr={}",
            String::from_utf8_lossy(&child.stdout),
            String::from_utf8_lossy(&child.stderr),
        );

        let stale_failure = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Degraded,
            None,
            1,
            None,
            Some("pending older probe failure".to_string()),
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let outcome = commit_local_cluster_connectivity_source_set_if_snapshot(
            &paths,
            &state,
            &local,
            &[stale_failure],
            &expected_snapshot,
        )
        .expect("fence pending probe against external command");
        assert!(
            matches!(
                &outcome,
                LocalClusterConnectivitySourceSetCommit::Superseded { records, .. }
                    if records.len() == 1
                        && records[0].state == ClusterConnectivityState::Healthy
                        && records[0].latency_ms == Some(777)
            ),
            "the external exact observation must supersede the pending probe: {outcome:?}"
        );
        assert_eq!(
            local_cluster_connectivity_routing_generation(&paths),
            routing_before,
            "an external timestamp/latency refresh must not invalidate delivery routes"
        );
    }

    #[test]
    fn external_connectivity_writer_child() {
        let Ok(data_dir) = std::env::var("SINOMO_CONNECTIVITY_WRITER_CHILD_DATA_DIR") else {
            return;
        };
        let source_node_id = std::env::var("SINOMO_CONNECTIVITY_WRITER_CHILD_SOURCE")
            .expect("child source node")
            .parse::<NodeId>()
            .expect("parse child source node");
        let records_path = PathBuf::from(
            std::env::var("SINOMO_CONNECTIVITY_WRITER_CHILD_RECORDS").expect("child records path"),
        );
        let records = read_optional_object_file::<Vec<ClusterConnectivityRecord>>(&records_path)
            .expect("read child records")
            .expect("child records exist");
        let paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
        write_local_cluster_connectivity_records(&paths, &source_node_id, &records)
            .expect("write connectivity from child process");
    }

    #[test]
    fn external_stable_command_writer_child() {
        let Ok(data_dir) = std::env::var("SINOMO_CONNECTIVITY_COMMAND_CHILD_DATA_DIR") else {
            return;
        };
        let state_path = PathBuf::from(
            std::env::var("SINOMO_CONNECTIVITY_COMMAND_CHILD_STATE").expect("child state path"),
        );
        let endpoint_path = PathBuf::from(
            std::env::var("SINOMO_CONNECTIVITY_COMMAND_CHILD_ENDPOINT")
                .expect("child endpoint path"),
        );
        let state = read_optional_object_file::<ClusterBootstrapState>(&state_path)
            .expect("read child state")
            .expect("child state exists");
        let endpoint = read_optional_object_file::<ClusterEndpointRecord>(&endpoint_path)
            .expect("read child endpoint")
            .expect("child endpoint exists");
        let paths = ClusterPaths::new(&SinomoDataDir::new(data_dir));
        record_local_cluster_endpoint_command_connectivity(
            &paths,
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(777),
            true,
        )
        .expect("record external stable command connectivity");
    }

    #[test]
    fn serialized_cross_source_mutation_preserves_external_source_set() {
        let (_root, paths, state, source_a, endpoint, now) =
            connectivity_generation_fixture("connectivity_cross_source_serialization");
        let record_a = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(3),
            0,
            Some("source a".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_records(
            &paths,
            &source_a,
            std::slice::from_ref(&record_a),
        )
        .expect("seed and cache source a");

        let source_b = NodeId::from_legacy_named("node_connectivity_cross_source_serialization_b")
            .expect("source b");
        let mut record_b = record_a.clone();
        record_b.source_node_id = source_b.clone();
        record_b.last_success_at = Some("source b".to_string());

        let source_c = NodeId::from_legacy_named("node_connectivity_cross_source_serialization_c")
            .expect("source c");
        let mut record_c = record_a.clone();
        record_c.source_node_id = source_c;
        record_c.last_success_at = Some("external source c".to_string());
        let mut expected = vec![record_a.clone(), record_b.clone(), record_c.clone()];
        sort_cluster_connectivity_records(&mut expected);

        // Model another process updating a different source while this
        // process still has only source A cached. A concurrent source-B
        // writer must wait for the transaction lock, then reload A+C from
        // disk instead of building its replacement from the stale cache.
        let mutation_lock =
            acquire_local_cluster_connectivity_mutation_lock(&paths).expect("mutation lock");
        write_object_file(
            &paths.connectivity_local,
            &vec![record_a.clone(), record_c.clone()],
        )
        .expect("external source c write");

        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let writer_paths = paths.clone();
        let writer = std::thread::spawn(move || {
            started_tx.send(()).expect("announce writer");
            let result = write_local_cluster_connectivity_records(
                &writer_paths,
                &source_b,
                std::slice::from_ref(&record_b),
            );
            done_tx.send(result).expect("report writer");
        });
        started_rx.recv().expect("writer started");
        assert!(
            matches!(
                done_rx.recv_timeout(std::time::Duration::from_millis(50)),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout)
            ),
            "a second connectivity mutation must wait for the process-safe transaction lock"
        );
        drop(mutation_lock);
        done_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("writer should finish after lock release")
            .expect("merge source b");
        writer.join().expect("writer thread");

        invalidate_local_cluster_connectivity_cache(&paths).expect("reload persisted records");
        let stored =
            read_local_cluster_connectivity_records(&paths).expect("read serialized source sets");
        assert_eq!(stored, expected);
    }

    #[test]
    fn aggregate_group_merge_rejects_merged_total_without_partial_persistence() {
        let (_root, paths, state, source_a, endpoint, now) =
            connectivity_generation_fixture("connectivity_aggregate_merged_limit");
        let template = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(3),
            0,
            Some("source a".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let mut existing = Vec::with_capacity(MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS - 1);
        for index in 0..MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS - 1 {
            let mut record = template.clone();
            record.endpoint_id = ClusterEndpointId::derived(
                "sinomo.test.connectivity.aggregate-limit.existing",
                &index.to_le_bytes(),
            );
            record.address = format!("target:{index}");
            existing.push(record);
        }
        write_local_cluster_connectivity_record_set(&paths, &source_a, &existing)
            .expect("seed aggregate limit minus one");

        let source_b = NodeId::from_legacy_named("node_connectivity_aggregate_merged_limit_b")
            .expect("source b");
        let mut incoming = Vec::new();
        for index in 0_u64..2 {
            let mut record = template.clone();
            record.source_node_id = source_b.clone();
            record.endpoint_id = ClusterEndpointId::derived(
                "sinomo.test.connectivity.aggregate-limit.incoming",
                &index.to_le_bytes(),
            );
            record.address = format!("incoming:{index}");
            incoming.push(record);
        }
        let groups = BTreeMap::from([(source_b.clone(), incoming)]);

        let error = write_local_cluster_connectivity_record_groups(&paths, &groups)
            .expect_err("merged aggregate must remain bounded");
        assert!(matches!(
            error,
            NetworkError::ClusterConnectivitySnapshotRecordLimit {
                observed_records,
                max_records: MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS,
            } if observed_records == MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS + 1
        ));
        let stored = read_local_cluster_connectivity_records(&paths)
            .expect("read records after rejected aggregate merge");
        assert_eq!(stored.len(), MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS - 1);
        assert!(
            stored
                .iter()
                .all(|record| record.source_node_id == source_a),
            "aggregate rejection must happen before persisting any incoming source"
        );
    }

    #[test]
    fn route_semantic_change_advances_connectivity_routing_generation() {
        let (_root, paths, state, local, endpoint, now) =
            connectivity_generation_fixture("connectivity_routing_generation");
        let initial = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(3),
            0,
            Some("initial success".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_records(&paths, &local, &[initial])
            .expect("write initial healthy connectivity");
        let content_before = local_cluster_connectivity_content_generation(&paths);
        let routing_before = local_cluster_connectivity_routing_generation(&paths);

        let failed_at = now + time::Duration::seconds(1);
        let degraded = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Degraded,
            None,
            1,
            None,
            Some("connection failed".to_string()),
            failed_at,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_records(&paths, &local, &[degraded])
            .expect("write degraded connectivity");

        assert_ne!(
            local_cluster_connectivity_content_generation(&paths),
            content_before
        );
        assert_ne!(
            local_cluster_connectivity_routing_generation(&paths),
            routing_before,
            "reachability-state changes must invalidate the delivery routing table"
        );
    }

    #[test]
    fn latency_only_change_preserves_connectivity_routing_generation() {
        let (_root, paths, state, local, endpoint, now) =
            connectivity_generation_fixture("connectivity_latency_generation");
        let initial = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(3),
            0,
            Some("initial success".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_records(&paths, &local, &[initial])
            .expect("write initial connectivity observation");
        let routing_before = local_cluster_connectivity_routing_generation(&paths);

        let refreshed_at = now + time::Duration::seconds(1);
        let materially_slower = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(750),
            0,
            Some("materially slower success".to_string()),
            None,
            refreshed_at,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_records(&paths, &local, &[materially_slower])
            .expect("write materially slower connectivity observation");

        assert_eq!(
            local_cluster_connectivity_routing_generation(&paths),
            routing_before,
            "latency samples are path measurements, not connectivity topology generations"
        );
    }

    #[test]
    fn routing_stability_detects_failure_count_and_shorter_expiry() {
        let (_root, _paths, state, _local, endpoint, now) =
            connectivity_generation_fixture("connectivity_routing_stability");
        let degraded = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Degraded,
            Some(20),
            1,
            Some("earlier success".to_string()),
            Some("first failure".to_string()),
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );

        let mut failed_again = degraded.clone();
        failed_again.consecutive_failures = 2;
        assert!(
            !local_cluster_connectivity_routing_record_stable_eq((&degraded, &failed_again)),
            "failure-cost changes must invalidate cached route ordering"
        );

        let mut shorter_lived = degraded.clone();
        shorter_lived.expires_at = degraded.expires_at - time::Duration::seconds(1);
        assert!(
            !local_cluster_connectivity_routing_record_stable_eq((&degraded, &shorter_lived)),
            "an earlier expiry must not leave a route cached past its lifetime"
        );
    }

    #[test]
    fn older_connectivity_observation_cannot_regress_newer_state() {
        let (_root, paths, state, local, endpoint, now) =
            connectivity_generation_fixture("connectivity_observation_order");
        let newer = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(4),
            0,
            Some("newer success".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_records(&paths, &local, std::slice::from_ref(&newer))
            .expect("write newer connectivity observation");
        let content_before = local_cluster_connectivity_content_generation(&paths);
        let routing_before = local_cluster_connectivity_routing_generation(&paths);

        let older_at = now - time::Duration::seconds(1);
        let older = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Unreachable,
            None,
            3,
            None,
            Some("older failure".to_string()),
            older_at,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_record_set(&paths, &local, &[older])
            .expect("late full-set write must preserve newer observation");

        let stored = read_local_cluster_connectivity_records(&paths)
            .expect("read connectivity after stale write");
        assert_eq!(stored, vec![newer]);
        assert_eq!(
            local_cluster_connectivity_content_generation(&paths),
            content_before,
            "discarding a stale observation must not churn route-graph content"
        );
        assert_eq!(
            local_cluster_connectivity_routing_generation(&paths),
            routing_before,
            "discarding a stale observation must not invalidate delivery routes"
        );
    }

    #[test]
    fn stale_full_set_omission_cannot_delete_newer_endpoint() {
        let (_root, paths, state, local, endpoint_a, now) =
            connectivity_generation_fixture("connectivity_full_set_omission_order");
        let mut endpoint_b = endpoint_a.clone();
        endpoint_b.endpoint_id = ClusterEndpointId::derived(
            "sinomo.test.connectivity.full-set.endpoint-b",
            b"endpoint-b",
        );
        endpoint_b.address = "target:21354".to_string();
        let newer_at = now + time::Duration::seconds(2);
        let newer_a = cluster_connectivity_record(
            &state,
            &endpoint_a,
            ClusterConnectivityState::Healthy,
            Some(4),
            0,
            Some("newer a".to_string()),
            None,
            newer_at,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let newer_b = cluster_connectivity_record(
            &state,
            &endpoint_b,
            ClusterConnectivityState::Healthy,
            Some(5),
            0,
            Some("newer b".to_string()),
            None,
            newer_at,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_record_set(
            &paths,
            &local,
            &[newer_a.clone(), newer_b.clone()],
        )
        .expect("seed newer complete source set");

        let stale_at = now + time::Duration::seconds(1);
        let stale_a = cluster_connectivity_record(
            &state,
            &endpoint_a,
            ClusterConnectivityState::Degraded,
            None,
            2,
            None,
            Some("stale a".to_string()),
            stale_at,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_record_set(&paths, &local, &[stale_a])
            .expect("merge stale complete source set");

        let mut expected = vec![newer_a, newer_b];
        sort_cluster_connectivity_records(&mut expected);
        assert_eq!(
            read_local_cluster_connectivity_records_for_source(&paths, &local)
                .expect("read source after stale full-set write"),
            expected
        );
    }

    #[test]
    fn newer_full_set_still_removes_older_omitted_endpoint() {
        let (_root, paths, state, local, endpoint_a, now) =
            connectivity_generation_fixture("connectivity_full_set_removal");
        let mut endpoint_b = endpoint_a.clone();
        endpoint_b.endpoint_id = ClusterEndpointId::derived(
            "sinomo.test.connectivity.full-set-removal.endpoint-b",
            b"endpoint-b",
        );
        endpoint_b.address = "target:21354".to_string();
        let initial_a = cluster_connectivity_record(
            &state,
            &endpoint_a,
            ClusterConnectivityState::Healthy,
            Some(4),
            0,
            Some("initial a".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let initial_b = cluster_connectivity_record(
            &state,
            &endpoint_b,
            ClusterConnectivityState::Healthy,
            Some(5),
            0,
            Some("initial b".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_record_set(
            &paths,
            &local,
            &[initial_a.clone(), initial_b],
        )
        .expect("seed initial complete source set");

        let replacement_at = now + time::Duration::seconds(1);
        let replacement_a = cluster_connectivity_record(
            &state,
            &endpoint_a,
            ClusterConnectivityState::Healthy,
            Some(4),
            0,
            Some("replacement a".to_string()),
            None,
            replacement_at,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_record_set(
            &paths,
            &local,
            std::slice::from_ref(&replacement_a),
        )
        .expect("write newer complete source set");

        assert_eq!(
            read_local_cluster_connectivity_records_for_source(&paths, &local)
                .expect("read source after newer removal"),
            vec![initial_a],
            "the newer complete set removes omitted keys without rewriting stable retained evidence"
        );
    }

    #[test]
    fn merge_only_late_peer_subset_preserves_newer_source_set_without_generation_churn() {
        let (_root, paths, state, source, endpoint_a, now) =
            connectivity_generation_fixture("connectivity_peer_subset_order");
        let mut endpoint_b = endpoint_a.clone();
        endpoint_b.endpoint_id = ClusterEndpointId::derived(
            "sinomo.test.connectivity.peer-subset.endpoint-b",
            b"endpoint-b",
        );
        endpoint_b.address = "target:21354".to_string();

        let newer_at = now + time::Duration::seconds(1);
        let newer_a = cluster_connectivity_record(
            &state,
            &endpoint_a,
            ClusterConnectivityState::Healthy,
            Some(4),
            0,
            Some("newer a".to_string()),
            None,
            newer_at,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let newer_b = cluster_connectivity_record(
            &state,
            &endpoint_b,
            ClusterConnectivityState::Healthy,
            Some(5),
            0,
            Some("newer b".to_string()),
            None,
            newer_at,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_record_set(
            &paths,
            &source,
            &[newer_a.clone(), newer_b.clone()],
        )
        .expect("seed newer peer source set");
        let content_before = local_cluster_connectivity_content_generation(&paths);
        let routing_before = local_cluster_connectivity_routing_generation(&paths);

        let stale_a = cluster_connectivity_record(
            &state,
            &endpoint_a,
            ClusterConnectivityState::Unreachable,
            None,
            2,
            None,
            Some("stale a".to_string()),
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_records(&paths, &source, &[stale_a])
            .expect("merge late peer subset");

        let stored = read_local_cluster_connectivity_records_for_source(&paths, &source)
            .expect("read merged peer source");
        let mut expected = vec![newer_a, newer_b];
        sort_cluster_connectivity_records(&mut expected);
        assert_eq!(stored, expected);
        assert_eq!(
            local_cluster_connectivity_content_generation(&paths),
            content_before,
            "discarding a stale subset must not churn route-graph content"
        );
        assert_eq!(
            local_cluster_connectivity_routing_generation(&paths),
            routing_before,
            "discarding a stale subset must not invalidate delivery routes"
        );
    }

    #[test]
    fn topology_prune_removes_removed_members_and_unadvertised_endpoints() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-connectivity-topology-prune");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("layout");
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_connectivity_topology_prune").expect("cluster");
        let local =
            NodeId::from_legacy_named("node_connectivity_topology_prune_local").expect("local");
        let remote =
            NodeId::from_legacy_named("node_connectivity_topology_prune_remote").expect("remote");
        let removed =
            NodeId::from_legacy_named("node_connectivity_topology_prune_removed").expect("removed");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "connectivity topology prune",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &remote,
            "target:21353".to_string(),
            10,
        );
        let mut remote_member =
            ClusterMemberRecord::local_non_voting(cluster_id, remote.clone(), "remote", "pk", now);
        remote_member.endpoints = vec![endpoint.clone()];
        state.members.push(remote_member);

        let current = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(3),
            0,
            Some("ok".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let dirty_endpoint = ClusterEndpointRecord {
            endpoint_id: ClusterEndpointId::derived(
                "sinomo.test.connectivity.prune.dirty",
                b"dirty",
            ),
            node_id: remote.clone(),
            transport: ClusterTransportKind::LanTcp,
            address: "stale.example.test:21353".to_string(),
            source: ClusterEndpointSource::Static,
            privacy: ClusterEndpointPrivacy::Lan,
            priority: 20,
            last_observed_latency_ms: None,
        };
        let dirty = cluster_connectivity_record(
            &state,
            &dirty_endpoint,
            ClusterConnectivityState::Healthy,
            Some(1),
            0,
            Some("dirty".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let removed_endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &removed,
            "removed:21353".to_string(),
            10,
        );
        let removed_record = cluster_connectivity_record(
            &state,
            &removed_endpoint,
            ClusterConnectivityState::Healthy,
            Some(2),
            0,
            Some("removed".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_record_set(
            &paths,
            &local,
            &[current.clone(), dirty, removed_record],
        )
        .expect("seed local connectivity");

        let removed_count =
            prune_local_cluster_connectivity_records_for_state(&paths, &state).expect("prune");

        assert_eq!(removed_count, 2);
        let records =
            read_local_cluster_connectivity_records(&paths).expect("read pruned connectivity");
        assert_eq!(records.len(), 1);
        assert!(records.contains(&current));
    }

    #[test]
    fn topology_prune_refreshes_stale_in_process_cache_when_disk_is_clean() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-connectivity-prune-cache");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("layout");
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");

        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_connectivity_prune_cache").expect("cluster");
        let local =
            NodeId::from_legacy_named("node_connectivity_prune_cache_local").expect("local");
        let remote =
            NodeId::from_legacy_named("node_connectivity_prune_cache_remote").expect("remote");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "connectivity prune cache",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let endpoint = crate::endpoint_advertisement::daemon_lan_endpoint(
            &remote,
            "target:21353".to_string(),
            10,
        );
        let mut remote_member =
            ClusterMemberRecord::local_non_voting(cluster_id, remote.clone(), "remote", "pk", now);
        remote_member.endpoints = vec![endpoint.clone()];
        state.members.push(remote_member);

        let current = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(3),
            0,
            Some("ok".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let dirty_endpoint = ClusterEndpointRecord {
            endpoint_id: ClusterEndpointId::derived(
                "sinomo.test.connectivity.prune.cache-dirty",
                b"dirty",
            ),
            node_id: remote,
            transport: ClusterTransportKind::LanTcp,
            address: "stale.example.test:21353".to_string(),
            source: ClusterEndpointSource::Static,
            privacy: ClusterEndpointPrivacy::Lan,
            priority: 20,
            last_observed_latency_ms: None,
        };
        let dirty = cluster_connectivity_record(
            &state,
            &dirty_endpoint,
            ClusterConnectivityState::Healthy,
            Some(1),
            0,
            Some("dirty".to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );

        write_local_cluster_connectivity_record_set(
            &paths,
            &local,
            &[current.clone(), dirty.clone()],
        )
        .expect("seed dirty cache");
        assert!(
            read_local_cluster_connectivity_records(&paths)
                .expect("read dirty cache")
                .contains(&dirty)
        );
        write_object_file(&paths.connectivity_local, &vec![current.clone()])
            .expect("make disk clean without touching cache");

        let removed_count =
            prune_local_cluster_connectivity_records_for_state(&paths, &state).expect("prune");

        assert_eq!(removed_count, 0);
        let records =
            read_local_cluster_connectivity_records(&paths).expect("read pruned connectivity");
        assert_eq!(records, vec![current]);
    }
}
