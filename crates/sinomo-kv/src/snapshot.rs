use crate::internal::*;

#[cfg(test)]
#[derive(Clone)]
struct ClusterKvColdReadPause {
    after_open: bool,
    before_handoff: bool,
    claimed: Arc<std::sync::Barrier>,
    resume: Arc<std::sync::Barrier>,
}

#[cfg(test)]
static CLUSTER_KV_COLD_READ_PAUSES: LazyLock<Mutex<HashMap<PathBuf, ClusterKvColdReadPause>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[cfg(test)]
pub(crate) fn install_cluster_kv_cold_read_pause_for_test(
    root: PathBuf,
    claimed: Arc<std::sync::Barrier>,
    resume: Arc<std::sync::Barrier>,
) {
    CLUSTER_KV_COLD_READ_PAUSES
        .lock()
        .expect("cluster KV cold-read pause")
        .insert(
            root.clone(),
            ClusterKvColdReadPause {
                after_open: false,
                before_handoff: false,
                claimed,
                resume,
            },
        );
}

#[cfg(test)]
pub(crate) fn install_cluster_kv_cold_read_after_open_pause_for_test(
    root: PathBuf,
    opened: Arc<std::sync::Barrier>,
    resume: Arc<std::sync::Barrier>,
) {
    CLUSTER_KV_COLD_READ_PAUSES
        .lock()
        .expect("cluster KV cold-read pause")
        .insert(
            root.clone(),
            ClusterKvColdReadPause {
                after_open: true,
                before_handoff: false,
                claimed: opened,
                resume,
            },
        );
}

#[cfg(test)]
pub(crate) fn install_cluster_kv_cold_read_before_handoff_pause_for_test(
    root: PathBuf,
    observed: Arc<std::sync::Barrier>,
    resume: Arc<std::sync::Barrier>,
) {
    CLUSTER_KV_COLD_READ_PAUSES
        .lock()
        .expect("cluster KV cold-read pause")
        .insert(
            root,
            ClusterKvColdReadPause {
                after_open: false,
                before_handoff: true,
                claimed: observed,
                resume,
            },
        );
}

#[cfg(test)]
pub(crate) fn clear_cluster_kv_cold_read_pause_for_test(root: &Path) {
    CLUSTER_KV_COLD_READ_PAUSES
        .lock()
        .expect("cluster KV cold-read pause")
        .remove(root);
}

#[cfg(test)]
fn pause_cluster_kv_cold_read_for_test(root: &Path, after_open: bool) {
    let pause = CLUSTER_KV_COLD_READ_PAUSES.lock().ok().and_then(|pauses| {
        pauses
            .get(root)
            .filter(|pause| !pause.before_handoff && pause.after_open == after_open)
            .cloned()
    });
    if let Some(pause) = pause {
        pause.claimed.wait();
        pause.resume.wait();
    }
}

#[cfg(not(test))]
fn pause_cluster_kv_cold_read_for_test(_root: &Path, _after_open: bool) {}

#[cfg(test)]
fn pause_cluster_kv_cold_read_before_handoff_for_test(root: &Path) {
    let pause = CLUSTER_KV_COLD_READ_PAUSES.lock().ok().and_then(|pauses| {
        pauses
            .get(root)
            .filter(|pause| pause.before_handoff)
            .cloned()
    });
    if let Some(pause) = pause {
        pause.claimed.wait();
        pause.resume.wait();
    }
}

#[cfg(not(test))]
fn pause_cluster_kv_cold_read_before_handoff_for_test(_root: &Path) {}

pub fn read_cluster_kv_cached_snapshot(paths: &ClusterKvPaths) -> Result<KvSnapshot, KvError> {
    paths.local_cache().cached_snapshot()
}

/// Returns the already-decoded materialized snapshot only when it is resident
/// in this process and still matches the durable file identity.
///
/// This is an admission probe for latency-bounded callers. It performs only the
/// identity stat and process-cache lookup: a miss never reads or decodes the
/// materialized file, opens the resident Raft node, or restores snapshot/WAL
/// state.
pub fn read_cluster_kv_cached_snapshot_arc_if_hot(
    paths: &ClusterKvPaths,
) -> Result<Option<Arc<KvSnapshot>>, KvError> {
    paths.local_cache().cached_snapshot_arc_if_hot()
}

#[cfg(any(test, feature = "test-support"))]
pub fn evict_cluster_kv_cached_snapshot_arc_for_test(paths: &ClusterKvPaths) {
    if let Ok(mut cache) = materialized_snapshot_cache().lock() {
        cache.remove(&paths.snapshot);
    }
}

/// Reads a small set of exact keys from one immutable materialized snapshot.
///
/// Opening the materialized snapshot independently for each key can straddle
/// an atomic snapshot replacement and return values from different revisions.
/// Keep the shared snapshot alive until every requested entry has been
/// projected so callers receive one coherent local view without cloning the
/// complete keyspace.
pub fn read_cluster_kv_cached_entries_consistently(
    paths: &ClusterKvPaths,
    keys: &[String],
) -> Result<Vec<Option<KvEntry>>, KvError> {
    let Some(snapshot) = paths.local_cache().cached_snapshot_arc_if_present()? else {
        let mut missing = Vec::new();
        missing
            .try_reserve(keys.len())
            .map_err(|_| KvError::ResourceExhausted {
                operation: "read coherent materialized cluster KV entries".to_string(),
                requested_bytes: keys
                    .len()
                    .saturating_mul(std::mem::size_of::<Option<KvEntry>>()),
            })?;
        missing.resize_with(keys.len(), || None);
        return Ok(missing);
    };

    let mut entries = Vec::new();
    entries
        .try_reserve(keys.len())
        .map_err(|_| KvError::ResourceExhausted {
            operation: "read coherent materialized cluster KV entries".to_string(),
            requested_bytes: keys
                .len()
                .saturating_mul(std::mem::size_of::<Option<KvEntry>>()),
        })?;
    for key in keys {
        entries.push(
            snapshot
                .get(key)
                .map(|entry| {
                    try_clone_kv_entry(entry, "read coherent materialized cluster KV entry")
                })
                .transpose()?,
        );
    }
    Ok(entries)
}

pub fn read_cluster_kv_current_snapshot_for_node(
    paths: &ClusterKvPaths,
    node_id: NodeId,
) -> Result<KvSnapshot, KvError> {
    paths
        .local_cache()
        .current_snapshot_without_materialized_refresh(node_id)
}

pub fn read_cluster_kv_current_snapshot_arc_for_node(
    paths: &ClusterKvPaths,
    node_id: NodeId,
) -> Result<Arc<KvSnapshot>, KvError> {
    paths
        .local_cache()
        .current_snapshot_arc_without_materialized_refresh(node_id)
}

pub fn read_cluster_kv_store_for_node(
    paths: &ClusterKvPaths,
    node_id: NodeId,
) -> Result<InProcessKvStore, KvError> {
    read_cluster_kv_current_snapshot_for_node(paths, node_id).and_then(InProcessKvStore::restore)
}

enum ClusterKvLocalImmutableSnapshot {
    View(KvLocalRead<Arc<RaftLocalReadView>>),
    RetryResident,
}

/// Reads or seeds the immutable local Raft-view cache without ever checking a
/// node into the resident writer slot. The first unbounded read after process
/// start still performs recovery work proportional to the durable snapshot and
/// retained WAL; moving that restored indexed store into this bounded cache
/// amortizes the cost across subsequent point and prefix reads and avoids a
/// second O(total keyspace) snapshot conversion. Deadline-aware authority reads
/// may join an existing recovery but never become its uninterruptible owner.
fn read_cluster_kv_local_immutable_view_from_cache(
    cache: &KvMaterializedCache,
    node_id: &NodeId,
    wal_paths: &KvWalPaths,
    deadline: Option<Instant>,
) -> Result<ClusterKvLocalImmutableSnapshot, KvError> {
    // A shadow-role denial applies to voter-derived hot projections as well as
    // file opens. Never serve a view seeded before the one-way process role
    // boundary was installed.
    crate::raft::require_process_local_raft_access(&wal_paths.root)?;
    for _ in 0..3 {
        let (_, identity) = raft_view_identity_for_read(wal_paths)?;
        if let Some(identity) = identity
            && let Some(view) = cache_lookup_raft_view(wal_paths, node_id, identity)
        {
            return Ok(ClusterKvLocalImmutableSnapshot::View(KvLocalRead {
                revision: view.revision(),
                observed_term: view.observed_term,
                applied_index: view.applied_index,
                known_leader: view.known_leader.clone(),
                value: view,
            }));
        }

        let loader = match begin_resident_raft_cold_read(wal_paths, deadline)? {
            ResidentRaftColdReadStart::Load(loader) => loader,
            ResidentRaftColdReadStart::RetryImmutableCache => continue,
            ResidentRaftColdReadStart::RetryResident => {
                return Ok(ClusterKvLocalImmutableSnapshot::RetryResident);
            }
        };
        pause_cluster_kv_cold_read_for_test(&wal_paths.root, false);
        if loader.preempt_requested() {
            loader.yield_before_open();
            return Ok(ClusterKvLocalImmutableSnapshot::RetryResident);
        }
        let node = DurableRaftKvNode::try_open_with_materialized_cache(
            node_id.clone(),
            wal_paths.clone(),
            cache,
        )?;
        pause_cluster_kv_cold_read_for_test(&wal_paths.root, true);
        let (loader, node) = if loader.preempt_requested() {
            pause_cluster_kv_cold_read_before_handoff_for_test(&wal_paths.root);
            match loader.try_yield_restored_node(node) {
                Ok(()) => return Ok(ClusterKvLocalImmutableSnapshot::RetryResident),
                Err(loader_and_node) => loader_and_node,
            }
        } else {
            (loader, node)
        };
        let identity = raft_view_identity(wal_paths).ok_or_else(|| KvError::Io {
            path: wal_paths.root.display().to_string(),
            detail: "durable Raft identity unavailable after local read replay".to_string(),
        })?;
        let view = node.try_into_local_read_view()?;
        let revision = view.revision();
        let observed_term = view.observed_term;
        let applied_index = view.applied_index;
        let known_leader = view.known_leader.clone();
        let raft_config_entry = view.store.get(&cluster_kv_raft_config_key()).cloned();
        cache_store_raft_view(wal_paths, node_id.clone(), identity, view.clone());
        let view = match cache_lookup_raft_view(wal_paths, node_id, identity) {
            Some(cached) => cached,
            // An oversized indexed store is deliberately not retained in the
            // process-wide cache. It may still serve this one caller after the
            // same post-flock identity fence used by a cached view.
            None if raft_view_identity(wal_paths) == Some(identity) => view,
            None => {
                // Another process changed the durable view after the operation-
                // scoped reader released its flock. Never return the unfenced
                // parse; retry against the new identity while this load owns the
                // in-process singleflight.
                return Err(KvError::Busy {
                    path: wal_paths.root.display().to_string(),
                });
            }
        };
        loader.complete(
            node_id.clone(),
            identity,
            revision,
            observed_term,
            applied_index,
            known_leader.clone(),
            raft_config_entry,
        );
        return Ok(ClusterKvLocalImmutableSnapshot::View(KvLocalRead {
            value: view,
            revision,
            observed_term,
            applied_index,
            known_leader,
        }));
    }
    Err(KvError::Busy {
        path: wal_paths.root.display().to_string(),
    })
}

/// Retain the actual normal cold reader's DATA view across cache eviction.
#[cfg(any(test, feature = "test-support"))]
pub(crate) fn read_cluster_kv_local_indexed_view_for_test(
    paths: &ClusterKvPaths,
    node_id: &NodeId,
) -> Result<KvLocalRead<Arc<RaftLocalReadView>>, KvError> {
    let wal_paths = paths.raft_wal_paths(node_id);
    let cache = paths.local_cache();
    match read_cluster_kv_local_immutable_view_from_cache(
        &cache.materialized,
        node_id,
        &wal_paths,
        None,
    )? {
        ClusterKvLocalImmutableSnapshot::View(read) => Ok(read),
        ClusterKvLocalImmutableSnapshot::RetryResident => Err(KvError::Busy {
            path: wal_paths.root.display().to_string(),
        }),
    }
}

fn read_cluster_kv_local_immutable_snapshot(
    paths: &ClusterKvPaths,
    node_id: &NodeId,
    wal_paths: &KvWalPaths,
    deadline: Option<Instant>,
) -> Result<ClusterKvLocalImmutableSnapshot, KvError> {
    let cache = paths.local_cache();
    read_cluster_kv_local_immutable_view_from_cache(
        &cache.materialized,
        node_id,
        wal_paths,
        deadline,
    )
}

fn cluster_kv_local_materialized_fallback(
    paths: &ClusterKvPaths,
    wal_paths: &KvWalPaths,
    node_id: &NodeId,
    busy_error: KvError,
) -> Result<(Arc<KvSnapshot>, u64, u64, Option<NodeId>), KvError> {
    let Some(snapshot) = paths.local_cache().cached_snapshot_arc_if_present()? else {
        return Err(busy_error);
    };
    // A materialized snapshot is an observational read model, not an anchored
    // Raft snapshot. Only attach resident consensus metadata when it describes
    // this exact revision; otherwise report the conservative unknown values.
    let (observed_term, applied_index, known_leader) =
        match resident_raft_local_read_metadata(wal_paths, node_id) {
            Some((revision, observed_term, applied_index, known_leader))
                if revision == snapshot.revision =>
            {
                (observed_term, applied_index, known_leader)
            }
            _ => (0, 0, None),
        };
    Ok((snapshot, observed_term, applied_index, known_leader))
}

/// Reads the best locally available KV view without contacting a quorum.
///
/// The returned metadata is observational only: neither the known leader nor
/// the observed term establishes freshness.
fn read_cluster_kv_local_snapshot_inner(
    paths: &ClusterKvPaths,
    node_id: &NodeId,
    deadline: Option<Instant>,
) -> Result<KvLocalRead<KvSnapshot>, KvError> {
    let started = Instant::now();
    let result = (|| {
        let wal_paths = paths.raft_wal_paths(node_id);
        if let Some(read) = read_resident_raft_local_snapshot(&wal_paths, node_id) {
            return read;
        }
        match read_cluster_kv_local_immutable_snapshot(paths, node_id, &wal_paths, deadline) {
            Ok(ClusterKvLocalImmutableSnapshot::View(read)) => Ok(KvLocalRead {
                revision: read.revision,
                observed_term: read.observed_term,
                applied_index: read.applied_index,
                known_leader: read.known_leader,
                value: read.value.try_snapshot()?,
            }),
            Ok(ClusterKvLocalImmutableSnapshot::RetryResident) => {
                if let Some(read) = read_resident_raft_local_snapshot(&wal_paths, node_id) {
                    return read;
                }
                let error = KvError::Busy {
                    path: wal_paths.root.display().to_string(),
                };
                let (snapshot, observed_term, applied_index, known_leader) =
                    cluster_kv_local_materialized_fallback(paths, &wal_paths, node_id, error)?;
                Ok(KvLocalRead {
                    revision: snapshot.revision,
                    observed_term,
                    applied_index,
                    known_leader,
                    value: (*snapshot).clone(),
                })
            }
            Err(error @ KvError::Busy { .. }) => {
                let (snapshot, observed_term, applied_index, known_leader) =
                    cluster_kv_local_materialized_fallback(paths, &wal_paths, node_id, error)?;
                Ok(KvLocalRead {
                    revision: snapshot.revision,
                    observed_term,
                    applied_index,
                    known_leader,
                    value: (*snapshot).clone(),
                })
            }
            Err(error) => Err(error),
        }
    })();
    record_kv_read(
        "local",
        "durable_raft_snapshot",
        if result.is_ok() { "ok" } else { "error" },
        started,
    );
    result
}

pub fn read_cluster_kv_local_snapshot(
    paths: &ClusterKvPaths,
    node_id: &NodeId,
) -> Result<KvLocalRead<KvSnapshot>, KvError> {
    read_cluster_kv_local_snapshot_inner(paths, node_id, None)
}

/// Deadline-aware form used when the caller already owns an end-to-end
/// authority budget. Only joining an in-process cold restore observes this
/// deadline; the restore owner performs the actual local I/O synchronously.
pub fn read_cluster_kv_local_snapshot_with_deadline(
    paths: &ClusterKvPaths,
    node_id: &NodeId,
    deadline: Instant,
) -> Result<KvLocalRead<KvSnapshot>, KvError> {
    read_cluster_kv_local_snapshot_inner(paths, node_id, Some(deadline))
}

fn read_cluster_kv_local_entry_inner(
    paths: &ClusterKvPaths,
    node_id: &NodeId,
    key: &str,
    deadline: Option<Instant>,
) -> Result<KvLocalRead<Option<KvEntry>>, KvError> {
    let started = Instant::now();
    let wal_paths = paths.raft_wal_paths(node_id);
    let (source, result) = match read_resident_raft_local_entry(&wal_paths, node_id, key) {
        Some(result) => ("resident_raft_entry", result),
        None => (
            "immutable_or_materialized_entry",
            (|| match read_cluster_kv_local_immutable_snapshot(paths, node_id, &wal_paths, deadline)
            {
                Ok(ClusterKvLocalImmutableSnapshot::View(read)) => Ok(KvLocalRead {
                    value: read.value.store.get(key).cloned(),
                    revision: read.revision,
                    observed_term: read.observed_term,
                    applied_index: read.applied_index,
                    known_leader: read.known_leader,
                }),
                Ok(ClusterKvLocalImmutableSnapshot::RetryResident) => {
                    if let Some(read) = read_resident_raft_local_entry(&wal_paths, node_id, key) {
                        return read;
                    }
                    let error = KvError::Busy {
                        path: wal_paths.root.display().to_string(),
                    };
                    let (snapshot, observed_term, applied_index, known_leader) =
                        cluster_kv_local_materialized_fallback(paths, &wal_paths, node_id, error)?;
                    Ok(KvLocalRead {
                        value: snapshot.get(key).cloned(),
                        revision: snapshot.revision,
                        observed_term,
                        applied_index,
                        known_leader,
                    })
                }
                Err(error @ KvError::Busy { .. }) => {
                    let (snapshot, observed_term, applied_index, known_leader) =
                        cluster_kv_local_materialized_fallback(paths, &wal_paths, node_id, error)?;
                    Ok(KvLocalRead {
                        value: snapshot.get(key).cloned(),
                        revision: snapshot.revision,
                        observed_term,
                        applied_index,
                        known_leader,
                    })
                }
                Err(error) => Err(error),
            })(),
        ),
    };
    record_kv_read(
        "local",
        source,
        if result.is_ok() { "ok" } else { "error" },
        started,
    );
    result
}

pub fn read_cluster_kv_local_entry(
    paths: &ClusterKvPaths,
    node_id: &NodeId,
    key: &str,
) -> Result<KvLocalRead<Option<KvEntry>>, KvError> {
    read_cluster_kv_local_entry_inner(paths, node_id, key, None)
}

pub fn read_cluster_kv_local_entry_with_deadline(
    paths: &ClusterKvPaths,
    node_id: &NodeId,
    key: &str,
    deadline: Instant,
) -> Result<KvLocalRead<Option<KvEntry>>, KvError> {
    read_cluster_kv_local_entry_inner(paths, node_id, key, Some(deadline))
}

/// Reads one exact key only when the canonical resident Raft node is idle.
///
/// Unlike [`read_cluster_kv_local_entry`], this readiness-oriented path never
/// opens or replays durable state and never materializes a full snapshot. A
/// cold or currently checked-out resident is reported as `Busy` so callers can
/// retry after the operation that owns the resident node makes it available.
pub fn read_cluster_kv_resident_local_entry(
    paths: &ClusterKvPaths,
    node_id: &NodeId,
    key: &str,
) -> Result<KvLocalRead<Option<KvEntry>>, KvError> {
    let started = Instant::now();
    let wal_paths = paths.raft_wal_paths(node_id);
    let result = read_resident_raft_local_entry(&wal_paths, node_id, key).unwrap_or_else(|| {
        Err(KvError::Busy {
            path: wal_paths.root.display().to_string(),
        })
    });
    record_kv_read(
        "local",
        "resident_raft_entry_strict",
        if result.is_ok() { "ok" } else { "busy" },
        started,
    );
    result
}

/// Read the fixed, completed local Raft configuration floor without opening
/// storage, obtaining mutable admission, or reading the materialized cache.
///
/// The small retained projection remains readable while a mutable owner has
/// checked out the node. It is a local floor, not a quorum authority proof or
/// a claim about that owner's unfinished changes. Exact storage ownership and
/// the pending higher-term fence are validated before returning it. A cold,
/// invalid, or temporarily unavailable owner fails closed.
pub fn read_cluster_kv_resident_configuration_floor(
    paths: &ClusterKvPaths,
    node_id: &NodeId,
) -> Result<KvLocalRead<Option<KvEntry>>, KvError> {
    let started = Instant::now();
    let result = read_resident_raft_configuration_floor(&paths.raft_wal_paths(node_id), node_id);
    record_kv_read(
        "local",
        "resident_raft_configuration_floor",
        if result.is_ok() { "ok" } else { "unavailable" },
        started,
    );
    result
}

/// Reads an ordered, bounded exact-key set from one canonical resident Raft
/// view. The result cannot straddle applied revisions and never opens/replays
/// durable state or falls back to a materialized snapshot.
pub fn read_cluster_kv_resident_local_entries(
    paths: &ClusterKvPaths,
    node_id: &NodeId,
    keys: &[String],
) -> Result<KvLocalRead<Vec<Option<KvEntry>>>, KvError> {
    let started = Instant::now();
    let wal_paths = paths.raft_wal_paths(node_id);
    let result = read_resident_raft_local_entries(&wal_paths, node_id, keys).unwrap_or_else(|| {
        Err(KvError::Busy {
            path: wal_paths.root.display().to_string(),
        })
    });
    record_kv_read(
        "local",
        "resident_raft_exact_set_strict",
        if result.is_ok() { "ok" } else { "busy" },
        started,
    );
    result
}

fn read_cluster_kv_local_prefix_inner(
    paths: &ClusterKvPaths,
    node_id: &NodeId,
    prefix: &str,
    deadline: Option<Instant>,
) -> Result<KvLocalRead<Vec<KvEntry>>, KvError> {
    let started = Instant::now();
    let wal_paths = paths.raft_wal_paths(node_id);
    let (source, result) = match read_resident_raft_local_prefix(&wal_paths, node_id, prefix) {
        Some(result) => ("resident_raft_prefix", result),
        None => (
            "immutable_or_materialized_prefix",
            (|| match read_cluster_kv_local_immutable_snapshot(paths, node_id, &wal_paths, deadline)
            {
                Ok(ClusterKvLocalImmutableSnapshot::View(read)) => Ok(KvLocalRead {
                    value: read.value.store.range_prefix(prefix)?,
                    revision: read.revision,
                    observed_term: read.observed_term,
                    applied_index: read.applied_index,
                    known_leader: read.known_leader,
                }),
                Ok(ClusterKvLocalImmutableSnapshot::RetryResident) => {
                    if let Some(read) = read_resident_raft_local_prefix(&wal_paths, node_id, prefix)
                    {
                        return read;
                    }
                    let error = KvError::Busy {
                        path: wal_paths.root.display().to_string(),
                    };
                    let (snapshot, observed_term, applied_index, known_leader) =
                        cluster_kv_local_materialized_fallback(paths, &wal_paths, node_id, error)?;
                    Ok(KvLocalRead {
                        value: snapshot.try_range_prefix(prefix)?,
                        revision: snapshot.revision,
                        observed_term,
                        applied_index,
                        known_leader,
                    })
                }
                Err(error @ KvError::Busy { .. }) => {
                    let (snapshot, observed_term, applied_index, known_leader) =
                        cluster_kv_local_materialized_fallback(paths, &wal_paths, node_id, error)?;
                    Ok(KvLocalRead {
                        value: snapshot.try_range_prefix(prefix)?,
                        revision: snapshot.revision,
                        observed_term,
                        applied_index,
                        known_leader,
                    })
                }
                Err(error) => Err(error),
            })(),
        ),
    };
    record_kv_read(
        "local",
        source,
        if result.is_ok() { "ok" } else { "error" },
        started,
    );
    result
}

pub fn read_cluster_kv_local_prefix(
    paths: &ClusterKvPaths,
    node_id: &NodeId,
    prefix: &str,
) -> Result<KvLocalRead<Vec<KvEntry>>, KvError> {
    read_cluster_kv_local_prefix_inner(paths, node_id, prefix, None)
}

/// Reads one prefix only from the canonical resident Raft node.
///
/// This is the prefix counterpart to [`read_cluster_kv_resident_local_entry`].
/// It never opens durable state and, critically, never falls back to the
/// observational materialized snapshot. The result is only the canonical
/// locally applied view; it is not current-term quorum authority. Observers
/// should fail fast on `Busy`. Destructive work and routing decisions must use
/// a ReadIndex-backed authority proof or fail closed. A successful result is
/// complete within the standard prefix cardinality and payload bounds;
/// oversized prefixes fail with `RangeLimit`/`RangePayloadLimit`.
pub fn read_cluster_kv_resident_local_prefix(
    paths: &ClusterKvPaths,
    node_id: &NodeId,
    prefix: &str,
) -> Result<KvLocalRead<Vec<KvEntry>>, KvError> {
    let started = Instant::now();
    let wal_paths = paths.raft_wal_paths(node_id);
    let result = read_resident_raft_local_prefix_limited_by_payload_bytes(
        &wal_paths,
        node_id,
        prefix,
        CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
        CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES,
    )
    .unwrap_or_else(|| {
        Err(KvError::Busy {
            path: wal_paths.root.display().to_string(),
        })
    });
    record_kv_read(
        "local",
        "resident_raft_prefix_strict",
        if result.is_ok() { "ok" } else { "busy" },
        started,
    );
    result
}

/// Observe a complete key-selected subset of one canonical resident prefix.
///
/// Every examined key counts against the ordinary prefix entry bound. Only
/// matching entries are copied and charged to the unchanged response payload
/// bound. The key predicate must be a pure local classification. Missing or
/// checked-out resident state fails closed; this neither opens durable state
/// nor supplies quorum authority. No partial selection is returned on error.
pub fn read_cluster_kv_resident_local_prefix_matching(
    paths: &ClusterKvPaths,
    node_id: &NodeId,
    prefix: &str,
    matches_key: fn(&str) -> bool,
) -> Result<KvLocalRead<Vec<KvEntry>>, KvError> {
    let started = Instant::now();
    let wal_paths = paths.raft_wal_paths(node_id);
    let result = read_resident_raft_local_prefix_matching(&wal_paths, node_id, prefix, matches_key)
        .unwrap_or_else(|| {
            Err(KvError::Busy {
                path: wal_paths.root.display().to_string(),
            })
        });
    record_kv_read(
        "local",
        "resident_raft_matching_prefix_strict",
        if result.is_ok() { "ok" } else { "busy" },
        started,
    );
    result
}

pub fn read_cluster_kv_local_prefix_with_deadline(
    paths: &ClusterKvPaths,
    node_id: &NodeId,
    prefix: &str,
    deadline: Instant,
) -> Result<KvLocalRead<Vec<KvEntry>>, KvError> {
    read_cluster_kv_local_prefix_inner(paths, node_id, prefix, Some(deadline))
}

pub fn read_cluster_kv_local_event_history(
    paths: &ClusterKvPaths,
    node_id: &NodeId,
) -> Result<KvLocalRead<ClusterKvEventReadView>, KvError> {
    let started = Instant::now();
    let wal_paths = paths.raft_wal_paths(node_id);
    let result = match read_resident_raft_local_event_history(&wal_paths, node_id) {
        Some(result) => result,
        None => match read_cluster_kv_local_immutable_snapshot(paths, node_id, &wal_paths, None) {
            Ok(ClusterKvLocalImmutableSnapshot::View(read)) => Ok(KvLocalRead {
                value: ClusterKvEventReadView {
                    compacted_revision: read.value.store.compacted_revision(),
                    events: read
                        .value
                        .store
                        .retained_events()
                        .map(|event| {
                            try_clone_kv_event(event, "clone immutable cluster KV event history")
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                },
                revision: read.revision,
                observed_term: read.observed_term,
                applied_index: read.applied_index,
                known_leader: read.known_leader,
            }),
            Ok(ClusterKvLocalImmutableSnapshot::RetryResident) => {
                if let Some(read) = read_resident_raft_local_event_history(&wal_paths, node_id) {
                    return read;
                }
                let error = KvError::Busy {
                    path: wal_paths.root.display().to_string(),
                };
                let (snapshot, observed_term, applied_index, known_leader) =
                    cluster_kv_local_materialized_fallback(paths, &wal_paths, node_id, error)?;
                Ok(KvLocalRead {
                    value: ClusterKvEventReadView {
                        compacted_revision: snapshot.compacted_revision,
                        events: snapshot.events.clone(),
                    },
                    revision: snapshot.revision,
                    observed_term,
                    applied_index,
                    known_leader,
                })
            }
            Err(error @ KvError::Busy { .. }) => {
                let (snapshot, observed_term, applied_index, known_leader) =
                    cluster_kv_local_materialized_fallback(paths, &wal_paths, node_id, error)?;
                Ok(KvLocalRead {
                    value: ClusterKvEventReadView {
                        compacted_revision: snapshot.compacted_revision,
                        events: snapshot.events.clone(),
                    },
                    revision: snapshot.revision,
                    observed_term,
                    applied_index,
                    known_leader,
                })
            }
            Err(error) => Err(error),
        },
    };
    record_kv_read(
        "local",
        "raft_event_history",
        if result.is_ok() { "ok" } else { "error" },
        started,
    );
    result
}

pub fn write_cluster_kv_snapshot(
    paths: &ClusterKvPaths,
    snapshot: &KvSnapshot,
) -> Result<(), KvError> {
    write_cluster_kv_materialized_snapshot_if_changed(paths, snapshot).map(|_| ())
}

pub fn write_cluster_kv_materialized_snapshot_if_changed(
    paths: &ClusterKvPaths,
    snapshot: &KvSnapshot,
) -> Result<bool, KvError> {
    write_cluster_kv_materialized_snapshot_if_changed_with_observation_report(paths, snapshot)
        .map(|(wrote, _)| wrote)
}

/// Return preparation evidence from this writer's own completed receipt.
pub fn write_cluster_kv_materialized_snapshot_if_changed_with_observation_report(
    paths: &ClusterKvPaths,
    snapshot: &KvSnapshot,
) -> Result<(bool, bool), KvError> {
    let (wrote, delivery_prepared) = paths
        .local_cache()
        .materialized
        .write_materialized_snapshot_with_observation_report(snapshot)?;
    if wrote {
        notify_cluster_kv_watchers(paths);
    }
    Ok((wrote, delivery_prepared))
}

/// Materializes an imported read model without replacing a locally cached view
/// at a higher revision. Anchored Raft snapshot restoration uses the explicit
/// local-cache writer because consensus metadata, not revision alone, makes that
/// replacement authoritative.
pub fn write_cluster_kv_snapshot_if_changed(
    paths: &ClusterKvPaths,
    snapshot: &KvSnapshot,
) -> Result<bool, KvError> {
    write_cluster_kv_snapshot_if_changed_with_observation_report(paths, snapshot)
        .map(|(wrote, _)| wrote)
}

/// Preserve the imported writer's own delivery preparation evidence.
pub fn write_cluster_kv_snapshot_if_changed_with_observation_report(
    paths: &ClusterKvPaths,
    snapshot: &KvSnapshot,
) -> Result<(bool, bool), KvError> {
    let (wrote, delivery_prepared) = paths
        .local_cache()
        .materialized
        .write_imported_snapshot_with_observation_report(snapshot)?;
    if wrote {
        notify_cluster_kv_watchers(paths);
    }
    Ok((wrote, delivery_prepared))
}

pub(crate) fn write_cluster_kv_authoritative_snapshot_if_changed(
    paths: &ClusterKvPaths,
    snapshot: &KvSnapshot,
) -> Result<bool, KvError> {
    let wrote = paths
        .local_cache()
        .write_authoritative_snapshot_if_changed(snapshot)?;
    if wrote {
        notify_cluster_kv_watchers(paths);
    }
    Ok(wrote)
}

pub fn write_imported_cluster_kv_snapshot_if_changed<F>(
    paths: &ClusterKvPaths,
    snapshot: &KvSnapshot,
    key_can_defer_materialized_snapshot: F,
) -> Result<bool, KvError>
where
    F: FnMut(&str) -> bool,
{
    write_imported_cluster_kv_snapshot_if_changed_with_observation_report(
        paths,
        snapshot,
        key_can_defer_materialized_snapshot,
    )
    .map(|(wrote, _)| wrote)
}

/// Report only this imported write's completed preparation, never subscriber
/// presence or another producer's invalidation.
pub fn write_imported_cluster_kv_snapshot_if_changed_with_observation_report<F>(
    paths: &ClusterKvPaths,
    snapshot: &KvSnapshot,
    key_can_defer_materialized_snapshot: F,
) -> Result<(bool, bool), KvError>
where
    F: FnMut(&str) -> bool,
{
    let (wrote, delivery_prepared) = paths
        .local_cache()
        .materialized
        .write_imported_snapshot_with_defer_and_observation_report(
            snapshot,
            key_can_defer_materialized_snapshot,
        )?;
    if wrote {
        notify_cluster_kv_watchers(paths);
    }
    Ok((wrote, delivery_prepared))
}

pub(crate) fn cluster_kv_materialized_snapshot_age(paths: &ClusterKvPaths) -> Option<Duration> {
    let Ok(metadata) = fs::metadata(&paths.snapshot) else {
        return None;
    };
    metadata
        .modified()
        .ok()
        .and_then(|modified| SystemTime::now().duration_since(modified).ok())
}

pub fn read_cluster_kv_entry(
    paths: &ClusterKvPaths,
    key: &str,
) -> Result<Option<KvEntry>, KvError> {
    let Some(snapshot) = paths.local_cache().cached_snapshot_arc_if_present()? else {
        return Ok(None);
    };
    Ok(snapshot.get(key).cloned())
}

pub fn read_cluster_kv_prefix_entries(
    paths: &ClusterKvPaths,
    prefix: &str,
) -> Result<Vec<KvEntry>, KvError> {
    read_cluster_kv_prefix_entries_limited(paths, prefix, CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES)
}

pub fn read_cluster_kv_prefix_entries_limited(
    paths: &ClusterKvPaths,
    prefix: &str,
    max_entries: usize,
) -> Result<Vec<KvEntry>, KvError> {
    read_cluster_kv_prefix_entries_limited_by_payload_bytes(
        paths,
        prefix,
        max_entries,
        CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES,
    )
}

pub fn read_cluster_kv_prefix_entries_limited_by_payload_bytes(
    paths: &ClusterKvPaths,
    prefix: &str,
    max_entries: usize,
    max_payload_bytes: usize,
) -> Result<Vec<KvEntry>, KvError> {
    let Some(snapshot) = paths.local_cache().cached_snapshot_arc_if_present()? else {
        return Ok(Vec::new());
    };
    snapshot.range_prefix_limited_by_payload_bytes(prefix, max_entries, max_payload_bytes)
}

impl KvSnapshot {
    /// Validates externally decoded snapshot resource semantics without imposing
    /// a fixed total snapshot size. Individual values, lease cardinality, and
    /// retained event history already have KV-domain limits; total payload bytes
    /// are only checked for arithmetic overflow.
    pub fn validate_resource_bounds(&self) -> Result<(), KvError> {
        self.validate_resource_bounds_with_committed_request_size_sink(|_| {})
    }

    pub(crate) fn validate_resource_bounds_with_committed_request_sizes(
        &self,
    ) -> Result<Vec<usize>, KvError> {
        // Restore needs both the normal snapshot validation and the immutable
        // per-record measurements. Collect them during the validation pass so a
        // restored store never re-encodes the retained ledger to initialize its
        // cached accounting.
        self.validate_resource_cardinality_bounds()?;
        let mut encoded_sizes = Vec::new();
        try_reserve_kv_items(
            &mut encoded_sizes,
            self.committed_requests.len(),
            "restore committed KV request byte accounting",
        )?;
        self.validate_resource_bounds_after_cardinality(|encoded_bytes| {
            encoded_sizes.push(encoded_bytes);
        })?;
        Ok(encoded_sizes)
    }

    fn validate_resource_bounds_with_committed_request_size_sink<F>(
        &self,
        committed_request_size_sink: F,
    ) -> Result<(), KvError>
    where
        F: FnMut(usize),
    {
        self.validate_resource_cardinality_bounds()?;
        self.validate_resource_bounds_after_cardinality(committed_request_size_sink)
    }

    fn validate_resource_cardinality_bounds(&self) -> Result<(), KvError> {
        if self.leases.len() > MAX_ACTIVE_LEASES {
            return Err(KvError::LeaseLimit {
                max: MAX_ACTIVE_LEASES,
            });
        }
        if self.events.len() > MAX_RETAINED_EVENTS {
            return Err(KvError::SnapshotEventLimit {
                observed_events: self.events.len(),
                max_events: MAX_RETAINED_EVENTS,
            });
        }
        if self.committed_requests.len() > MAX_COMMITTED_KV_REQUESTS {
            return Err(KvError::RequestRecordLimit {
                observed_records: self.committed_requests.len(),
                max_records: MAX_COMMITTED_KV_REQUESTS,
            });
        }
        if self.expired_requests.len() > MAX_EXPIRED_KV_REQUEST_TOMBSTONES {
            return Err(KvError::RequestRecordLimit {
                observed_records: self.expired_requests.len(),
                max_records: MAX_EXPIRED_KV_REQUEST_TOMBSTONES,
            });
        }
        Ok(())
    }

    fn validate_resource_bounds_after_cardinality<F>(
        &self,
        mut committed_request_size_sink: F,
    ) -> Result<(), KvError>
    where
        F: FnMut(usize),
    {
        let mut committed_request_bytes = 0_usize;
        for record in &self.committed_requests {
            let encoded_bytes = encode_kv_value(record)?.len();
            committed_request_size_sink(encoded_bytes);
            committed_request_bytes = committed_request_bytes
                .checked_add(encoded_bytes)
                .ok_or_else(|| KvError::ResourceExhausted {
                    operation: "validate committed KV request record bytes".to_string(),
                    requested_bytes: usize::MAX,
                })?;
        }
        if committed_request_bytes > MAX_COMMITTED_KV_REQUEST_BYTES {
            return Err(KvError::RequestRecordBytesLimit {
                observed_bytes: committed_request_bytes,
                max_bytes: MAX_COMMITTED_KV_REQUEST_BYTES,
            });
        }

        let mut payload_bytes = 0_usize;
        for entry in &self.entries {
            // Snapshots written by an older binary are an independent ingress
            // path. Refuse a leased membership key here so lease expiry can
            // never silently erase the authoritative voter configuration.
            validate_cluster_kv_raft_config_is_unleased(&entry.key, entry.lease_id)?;
            validate_kv_value_len(entry.value.len())?;
            payload_bytes = checked_snapshot_payload_bytes(
                payload_bytes,
                entry.key.len().saturating_add(entry.value.len()),
            )?;
        }
        for event in &self.events {
            payload_bytes = checked_snapshot_payload_bytes(payload_bytes, event.key.len())?;
            if let KvEventKind::Put { value } = &event.kind {
                validate_kv_value_len(value.len())?;
                payload_bytes = checked_snapshot_payload_bytes(payload_bytes, value.len())?;
            }
        }
        Ok(())
    }

    /// Returns a snapshot with stable key/lease ordering and duplicate records
    /// collapsed to the same last-record-wins state as `InProcessKvStore::restore`.
    ///
    /// Call this before using the snapshot point/range helpers on externally
    /// supplied snapshots. Snapshots produced by `InProcessKvStore::snapshot`,
    /// `KvMaterializedCache`, and `KvLocalCache` are already canonical.
    pub fn canonicalized(mut self) -> Self {
        self.canonicalize();
        self
    }

    /// Canonicalizes this snapshot in place.
    pub fn canonicalize(&mut self) {
        self.entries.sort_by(|left, right| left.key.cmp(&right.key));
        self.entries.reverse();
        self.entries.dedup_by(|left, right| left.key == right.key);
        self.entries.reverse();
        self.leases.sort_by_key(|left| left.lease_id);
        self.leases.reverse();
        self.committed_requests
            .sort_by(|left, right| left.request_id.cmp(&right.request_id));
        self.committed_requests.reverse();
        self.committed_requests
            .dedup_by(|left, right| left.request_id == right.request_id);
        self.committed_requests.reverse();
        self.leases
            .dedup_by(|left, right| left.lease_id == right.lease_id);
        self.leases.reverse();
    }

    /// Looks up a key in a canonical snapshot.
    ///
    /// This is intentionally `O(log n)` and does not validate ordering in release
    /// builds; canonicalize externally supplied snapshots first.
    pub fn get(&self, key: &str) -> Option<&KvEntry> {
        debug_assert!(self.is_canonical());
        self.entries
            .binary_search_by(|entry| entry.key.as_str().cmp(key))
            .ok()
            .and_then(|index| self.entries.get(index))
    }

    /// Returns all entries under `prefix` from a canonical snapshot.
    pub fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
        self.try_range_prefix(prefix)
    }

    /// Fallibly returns all entries under `prefix` from a canonical snapshot.
    pub fn try_range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
        let mut entries = Vec::new();
        for entry in self.prefix_entries(prefix) {
            try_reserve_kv_items(&mut entries, 1, "collect snapshot prefix range")?;
            entries.push(try_clone_kv_entry(entry, "collect snapshot prefix range")?);
        }
        Ok(entries)
    }

    /// Iterates entries under `prefix` from a canonical snapshot without cloning.
    pub fn prefix_entries<'a>(&'a self, prefix: &'a str) -> impl Iterator<Item = &'a KvEntry> + 'a {
        debug_assert!(self.is_canonical());
        let start = self
            .entries
            .partition_point(|entry| entry.key.as_str() < prefix);
        self.entries[start..]
            .iter()
            .take_while(move |entry| entry.key.starts_with(prefix))
    }

    /// Returns at most `max_entries` entries under `prefix` from a canonical snapshot.
    pub fn range_prefix_limited(
        &self,
        prefix: &str,
        max_entries: usize,
    ) -> Result<Vec<KvEntry>, KvError> {
        self.range_prefix_limited_by_payload_bytes(prefix, max_entries, usize::MAX)
    }

    pub fn range_prefix_limited_by_payload_bytes(
        &self,
        prefix: &str,
        max_entries: usize,
        max_payload_bytes: usize,
    ) -> Result<Vec<KvEntry>, KvError> {
        let mut entries = Vec::new();
        let mut payload_bytes = 0_usize;
        for entry in self.prefix_entries(prefix) {
            if entries.len() >= max_entries {
                return Err(KvError::RangeLimit {
                    prefix: prefix.to_string(),
                    max_entries,
                });
            }
            payload_bytes = payload_bytes.saturating_add(kv_entry_payload_bytes(entry));
            if payload_bytes > max_payload_bytes {
                return Err(KvError::RangePayloadLimit {
                    prefix: prefix.to_string(),
                    observed_bytes: payload_bytes,
                    max_bytes: max_payload_bytes,
                });
            }
            try_reserve_kv_items(&mut entries, 1, "collect limited snapshot prefix range")?;
            entries.push(try_clone_kv_entry(
                entry,
                "collect limited snapshot prefix range",
            )?);
        }
        Ok(entries)
    }

    pub fn range_prefix_serde_limited_by_payload_bytes<T: DeserializeOwned>(
        &self,
        prefix: &str,
        max_entries: usize,
        max_payload_bytes: usize,
    ) -> Result<Vec<T>, KvError> {
        let entries =
            self.range_prefix_limited_by_payload_bytes(prefix, max_entries, max_payload_bytes)?;
        let mut values = Vec::new();
        try_reserve_kv_items(
            &mut values,
            entries.len(),
            "decode limited snapshot prefix range",
        )?;
        for entry in entries {
            values.push(entry.decode()?);
        }
        Ok(values)
    }

    fn is_canonical(&self) -> bool {
        self.entries
            .windows(2)
            .all(|window| window[0].key < window[1].key)
            && self
                .leases
                .windows(2)
                .all(|window| window[0].lease_id < window[1].lease_id)
            && self
                .committed_requests
                .windows(2)
                .all(|window| window[0].request_id < window[1].request_id)
    }
}

fn checked_snapshot_payload_bytes(current: usize, additional: usize) -> Result<usize, KvError> {
    current
        .checked_add(additional)
        .ok_or_else(|| KvError::ResourceExhausted {
            operation: "validate kv snapshot payload".to_string(),
            requested_bytes: usize::MAX,
        })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvMaterializedCache {
    path: PathBuf,
    /// When set, the persisted snapshot's watch-event history is compacted to at
    /// most this many events before writing. Keeps the materialized file small and
    /// — crucially — ensures every writer (the explicit publish path and the
    /// read-triggered refresh in `read_store_with_raft_view`) persists the same
    /// event tail, so they don't perpetually rewrite each other's snapshots.
    max_materialized_events: Option<usize>,
}

#[cfg(test)]
#[derive(Clone)]
struct MaterializedCacheRaftWritePause {
    path: PathBuf,
    compared: Arc<std::sync::Barrier>,
    resume: Arc<std::sync::Barrier>,
}

#[cfg(test)]
static MATERIALIZED_CACHE_RAFT_WRITE_PAUSE: LazyLock<
    Mutex<Option<MaterializedCacheRaftWritePause>>,
> = LazyLock::new(|| Mutex::new(None));

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn install_materialized_cache_raft_write_pause_for_test(
    path: PathBuf,
    compared: Arc<std::sync::Barrier>,
    resume: Arc<std::sync::Barrier>,
) {
    *MATERIALIZED_CACHE_RAFT_WRITE_PAUSE
        .lock()
        .expect("materialized cache write pause") = Some(MaterializedCacheRaftWritePause {
        path,
        compared,
        resume,
    });
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn clear_materialized_cache_raft_write_pause_for_test(path: &Path) {
    let mut pause = MATERIALIZED_CACHE_RAFT_WRITE_PAUSE
        .lock()
        .expect("materialized cache write pause");
    if pause.as_ref().is_some_and(|pause| pause.path == path) {
        *pause = None;
    }
}

#[cfg(test)]
fn pause_materialized_cache_raft_write_after_compare_for_test(path: &Path) {
    let pause = MATERIALIZED_CACHE_RAFT_WRITE_PAUSE
        .lock()
        .ok()
        .and_then(|pause| pause.as_ref().filter(|pause| pause.path == path).cloned());
    if let Some(pause) = pause {
        pause.compared.wait();
        pause.resume.wait();
    }
}

/// Stages one derived-view publication while its real writer owner is held.
/// The immutable Arc is the same allocation installed in the materialized cache.
struct KvSnapshotObservationContext {
    source: crate::KvObservationCapacityPermit,
    owns_writer_fence: bool,
    staged: Mutex<KvSnapshotObservationStaged>,
}

#[derive(Default)]
struct KvSnapshotObservationStaged {
    receipt: Option<crate::KvCommittedObservationReceipt>,
    snapshot: Option<Arc<KvSnapshot>>,
    encoded_bytes: Option<usize>,
    potential_write: bool,
    no_effect_resolved: bool,
}

impl KvSnapshotObservationContext {
    fn admit(cache: &KvMaterializedCache) -> Result<Self, KvError> {
        let paths = ClusterKvPaths::new(cache.path.parent().unwrap_or_else(|| Path::new("")));
        Ok(Self::new(
            crate::try_admit_committed_observation(
                &paths,
                crate::KvObservationProposalOwnership::Absent,
            )?,
            true,
        ))
    }

    fn new(source: crate::KvObservationCapacityPermit, owns_writer_fence: bool) -> Self {
        Self {
            source,
            owns_writer_fence,
            staged: Mutex::new(Default::default()),
        }
    }

    fn begin(&self, previous: KvRevision) -> Result<(), KvError> {
        let receipt = self.source.commit(
            previous,
            previous,
            None,
            crate::KvCommittedObservationKind::Replacement,
        )?;
        self.staged
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .receipt = Some(receipt);
        Ok(())
    }

    fn begin_unknown(&self) -> Result<(), KvError> {
        let receipt = self.source.commit_unknown_projection_prestate()?;
        self.staged
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .receipt = Some(receipt);
        Ok(())
    }

    fn before_write(&self) {
        self.staged
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .potential_write = true;
    }

    fn captured(
        &self,
        snapshot: Arc<KvSnapshot>,
        wrote: bool,
        encoded_bytes: usize,
    ) -> Result<(), KvError> {
        let mut staged = self
            .staged
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let receipt = staged
            .receipt
            .as_ref()
            .expect("writer reserved its provisional receipt");
        if wrote {
            receipt.advance_projection(snapshot.revision)?;
            staged.snapshot = Some(snapshot);
            staged.encoded_bytes = Some(encoded_bytes);
        } else {
            receipt.no_effect()?;
            staged.potential_write = false;
            staged.no_effect_resolved = true;
        }
        Ok(())
    }

    fn finish(self) -> Result<bool, KvError> {
        let staged = self
            .staged
            .into_inner()
            .unwrap_or_else(|error| error.into_inner());
        let Some(receipt) = staged.receipt else {
            return Ok(false);
        };
        if let Some(snapshot) = staged.snapshot {
            receipt.ready(Arc::new(crate::KvCommittedObservationPayload {
                delta: None,
                snapshot: Some(snapshot),
                snapshot_applied_index: None,
                encoded_bytes: staged.encoded_bytes,
            }))?;
        } else if staged.potential_write {
            receipt.abandon_publication()?;
        } else if !staged.no_effect_resolved {
            // A comparison, validation or serialization rejected the attempt
            // before any potentially published write. It owns no state change.
            receipt.no_effect()?;
        }
        receipt.wait_delivered()?;
        Ok(receipt.was_delivery_prepared())
    }
}

struct KvSnapshotWriteOwner {
    file: Option<File>,
    fence: Option<crate::KvObservationOwnerFence>,
}

impl Drop for KvSnapshotWriteOwner {
    fn drop(&mut self) {
        // File releases the actual cross-process writer lock before the last
        // source fence can dispatch callbacks or wake a delivery waiter.
        drop(self.file.take());
        drop(self.fence.take());
    }
}

/// A caller-owned publication stage for reconciliation whose outer Raft guard
/// already holds the source's storage fence. Finish only after that guard and
/// fence have both released; the inner File is released before methods return.
pub(crate) struct KvObservedLocalCache {
    cache: KvLocalCache,
    observation: KvSnapshotObservationContext,
}

impl KvObservedLocalCache {
    pub(crate) fn cache_snapshot_if_newer_than_raft(
        &self,
        node_id: NodeId,
        snapshot: KvSnapshot,
    ) -> Result<bool, KvError> {
        self.cache.cache_snapshot_if_newer_than_raft_inner(
            node_id,
            snapshot,
            Some(&self.observation),
        )
    }

    pub(crate) fn finish_observation(self) -> Result<(), KvError> {
        self.observation.finish().map(|_| ())
    }
}

fn finish_snapshot_observation<T>(
    context: KvSnapshotObservationContext,
    result: Result<T, KvError>,
) -> Result<T, KvError> {
    finish_snapshot_observation_report(context, result).map(|(value, _)| value)
}

fn finish_snapshot_observation_report<T>(
    context: KvSnapshotObservationContext,
    result: Result<T, KvError>,
) -> Result<(T, bool), KvError> {
    let delivery = context.finish();
    match (result, delivery) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Ok(value), Ok(delivery_prepared)) => Ok((value, delivery_prepared)),
    }
}

impl KvMaterializedCache {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            max_materialized_events: None,
        }
    }

    pub fn with_max_materialized_events(mut self, max_events: usize) -> Self {
        self.max_materialized_events = Some(max_events);
        self
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn exists(&self) -> bool {
        self.path.exists()
    }
    pub fn read_snapshot_arc(&self) -> Result<Arc<KvSnapshot>, KvError> {
        let identity = file_identity(&self.path);
        if let Some(identity) = identity
            && let Some(cached) = cache_lookup_snapshot(&self.path, identity)
        {
            return Ok(cached);
        }
        let snapshot = read_object_file::<KvSnapshot>(&self.path)?;
        snapshot.validate_resource_bounds()?;
        let snapshot = Arc::new(snapshot.canonicalized());
        if let Some(identity) = identity {
            cache_store_snapshot(&self.path, identity, snapshot.clone());
        }
        Ok(snapshot)
    }

    pub fn read_snapshot_arc_if_present(&self) -> Result<Option<Arc<KvSnapshot>>, KvError> {
        let identity = optional_file_identity_for_read(&self.path)?;
        let cache_identity = match identity {
            Some(Some(identity)) => Some(identity),
            Some(None) => return Ok(None),
            None => None,
        };
        if let Some(identity) = cache_identity
            && let Some(cached) = cache_lookup_snapshot(&self.path, identity)
        {
            return Ok(Some(cached));
        }
        let Some(bytes) = read_optional_object_file_bytes(&self.path)? else {
            return Ok(None);
        };
        let snapshot = decode_object_file::<KvSnapshot>(&self.path, &bytes)?;
        snapshot.validate_resource_bounds()?;
        let snapshot = Arc::new(snapshot.canonicalized());
        if let Some(identity) = cache_identity {
            cache_store_snapshot(&self.path, identity, snapshot.clone());
        }
        Ok(Some(snapshot))
    }

    pub fn read_snapshot_arc_if_hot(&self) -> Result<Option<Arc<KvSnapshot>>, KvError> {
        let Some(Some(identity)) = optional_file_identity_for_read(&self.path)? else {
            return Ok(None);
        };
        Ok(cache_lookup_snapshot(&self.path, identity))
    }

    /// Reads the file while the materialized-cache write lock is held. The full
    /// file identity makes a process-cache hit safe here: a completed write from
    /// another process changes the atomic replacement's inode (as well as its
    /// usual mtime/length identity), while the lock excludes a concurrent writer.
    /// Avoiding an unconditional SOBJ decode is important because every routine
    /// materialized publication passes through this comparison.
    fn read_snapshot_arc_if_present_locked(&self) -> Result<Option<Arc<KvSnapshot>>, KvError> {
        let identity = optional_file_identity_for_read(&self.path)?;
        let cache_identity = match identity {
            Some(Some(identity)) => Some(identity),
            Some(None) => return Ok(None),
            None => None,
        };
        if let Some(identity) = cache_identity
            && let Some(cached) = cache_lookup_snapshot(&self.path, identity)
        {
            return Ok(Some(cached));
        }
        let Some(bytes) = read_optional_object_file_bytes(&self.path)? else {
            return Ok(None);
        };
        let snapshot = decode_object_file::<KvSnapshot>(&self.path, &bytes)?;
        snapshot.validate_resource_bounds()?;
        let snapshot = Arc::new(snapshot.canonicalized());
        if let Some(identity) = cache_identity {
            cache_store_snapshot(&self.path, identity, snapshot.clone());
        }
        Ok(Some(snapshot))
    }

    pub fn read_snapshot(&self) -> Result<KvSnapshot, KvError> {
        let snapshot = self.read_snapshot_arc()?;
        Ok((*snapshot).clone())
    }

    pub fn read_snapshot_with_raft_view(
        &self,
        node_id: NodeId,
        paths: KvWalPaths,
    ) -> Result<Arc<KvSnapshot>, KvError> {
        self.read_snapshot_with_raft_view_inner(node_id, paths, true)
    }

    pub fn read_snapshot_with_raft_view_without_materialized_refresh(
        &self,
        node_id: NodeId,
        paths: KvWalPaths,
    ) -> Result<Arc<KvSnapshot>, KvError> {
        self.read_snapshot_with_raft_view_inner(node_id, paths, false)
    }

    fn read_snapshot_with_raft_view_inner(
        &self,
        node_id: NodeId,
        paths: KvWalPaths,
        refresh_materialized: bool,
    ) -> Result<Arc<KvSnapshot>, KvError> {
        let started = Instant::now();
        let (has_durable_state, identity) = match raft_view_identity_for_read(&paths) {
            Ok(view) => view,
            Err(error) => {
                record_kv_read("local", "raft_metadata", "error", started);
                return Err(error);
            }
        };
        if has_durable_state {
            if let Some(read) = read_resident_raft_local_snapshot(&paths, &node_id) {
                let snapshot = read?.value.canonicalized();
                if refresh_materialized {
                    self.write_raft_snapshot_if_changed(&snapshot)?;
                }
                record_kv_read("local", "resident_raft", "ok", started);
                return Ok(Arc::new(snapshot));
            }
            if let Some(identity) = identity
                && let Some(view) = cache_lookup_raft_view(&paths, &node_id, identity)
            {
                let snapshot = Arc::new(view.try_snapshot()?);
                if refresh_materialized {
                    self.write_raft_snapshot_if_changed(&snapshot)?;
                }
                record_kv_read("local", "raft_view_cache", "ok", started);
                return Ok(snapshot);
            }
            // The non-refresh contract is explicitly observational and already
            // falls back to the materialized view when a resident writer owns
            // the flock. If the identity-fenced resident slot proves that exact
            // condition, avoid spending the full local-read lock window only to
            // reach the same fallback. Refresh callers keep the existing wait
            // and durable-open attempt because they may need the current view.
            if !refresh_materialized
                && resident_raft_writer_is_checked_out(&paths, &node_id)
                && let Ok(Some(snapshot)) = self.read_snapshot_arc_if_present()
            {
                record_kv_read(
                    "local",
                    "materialized_resident_busy_fallback",
                    "ok",
                    started,
                );
                return Ok(snapshot);
            }
            let view =
                match read_cluster_kv_local_immutable_view_from_cache(self, &node_id, &paths, None)
                {
                    Ok(ClusterKvLocalImmutableSnapshot::View(read)) => read.value,
                    Ok(ClusterKvLocalImmutableSnapshot::RetryResident) => {
                        if let Some(read) = read_resident_raft_local_snapshot(&paths, &node_id) {
                            let snapshot = Arc::new(read?.value.canonicalized());
                            if refresh_materialized {
                                self.write_raft_snapshot_if_changed(&snapshot)?;
                            }
                            return Ok(snapshot);
                        }
                        let error = KvError::Busy {
                            path: paths.root.display().to_string(),
                        };
                        let fallback = self.read_snapshot_arc_if_present()?.ok_or(error);
                        record_kv_read(
                            "local",
                            "materialized_busy_fallback",
                            if fallback.is_ok() { "ok" } else { "error" },
                            started,
                        );
                        return fallback;
                    }
                    Err(error @ KvError::Busy { .. }) => {
                        let fallback = self.read_snapshot_arc_if_present()?.ok_or(error);
                        record_kv_read(
                            "local",
                            "materialized_busy_fallback",
                            if fallback.is_ok() { "ok" } else { "error" },
                            started,
                        );
                        return fallback;
                    }
                    Err(error) => return Err(error),
                };
            let snapshot = Arc::new(view.try_snapshot()?);
            if refresh_materialized {
                self.write_raft_snapshot_if_changed(&snapshot)?;
            }
            record_kv_read("local", "durable_raft_open", "ok", started);
            return Ok(snapshot);
        }
        match self.read_snapshot_arc_if_present() {
            Ok(Some(snapshot)) => {
                record_kv_read("local", "materialized_only", "ok", started);
                return Ok(snapshot);
            }
            Ok(None) => {}
            Err(error) => {
                record_kv_read("local", "materialized_only", "error", started);
                return Err(error);
            }
        }
        let snapshot = Arc::new(InProcessKvStore::new().try_snapshot()?.canonicalized());
        record_kv_read("local", "empty", "ok", started);
        Ok(snapshot)
    }

    pub fn read_store(&self) -> Result<InProcessKvStore, KvError> {
        self.read_snapshot().and_then(InProcessKvStore::restore)
    }

    fn write_lock_path(&self) -> PathBuf {
        let mut path = self.path.as_os_str().to_os_string();
        path.push(".lock");
        PathBuf::from(path)
    }

    fn lock_write(&self) -> Result<File, KvError> {
        let lock_path = self.write_lock_path();
        if let Some(parent) = lock_path.parent() {
            fs::create_dir_all(parent).map_err(|source| KvError::Io {
                path: parent.display().to_string(),
                detail: source.to_string(),
            })?;
        }
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)
            .map_err(|source| KvError::Io {
                path: lock_path.display().to_string(),
                detail: source.to_string(),
            })?;
        lock_node_exclusive(&lock, &lock_path, RAFT_NODE_LOCK_WAIT)?;
        Ok(lock)
    }

    /// Returns the snapshot that should actually be persisted: the input with its
    /// event history compacted to the configured materialized-event limit.
    fn materialized(&self, snapshot: &KvSnapshot) -> KvSnapshot {
        let mut snapshot = match self.max_materialized_events {
            Some(max_events) if snapshot.events.len() > max_events => {
                let mut snapshot = snapshot.clone();
                compact_kv_snapshot_event_history(&mut snapshot, max_events);
                snapshot
            }
            _ => snapshot.clone(),
        };
        snapshot.canonicalize();
        snapshot
    }

    /// Persists `snapshot` exactly as materialized and reports whether the object
    /// file changed. Unlike `write_snapshot_if_changed`, this treats revision and
    /// watch-event history as meaningful so callers that publish snapshot-backed
    /// events can suppress only truly redundant notifications.
    fn write_and_cache_if_materialized_changed_locked(
        &self,
        snapshot: KvSnapshot,
        observation: Option<&KvSnapshotObservationContext>,
    ) -> Result<bool, KvError> {
        snapshot.validate_resource_bounds()?;
        // Serialize once, as the existing writer does. The optional captured
        // byte count comes from this builder, never from a second sizing scan.
        let content =
            sinomo_object_serde::to_bytes(&snapshot, KV_WAL_OBJECT_VERSION_ID, LengthMode::U32)
                .map_err(kv_object_error)?;
        if let Some(observation) = observation {
            observation.before_write();
        }
        let wrote = write_object_file_bytes_if_changed(&self.path, &content)?;
        let snapshot = Arc::new(snapshot);
        if let Some(identity) = file_identity(&self.path) {
            cache_store_snapshot(&self.path, identity, Arc::clone(&snapshot));
        }
        if let Some(observation) = observation {
            observation.captured(snapshot, wrote, content.len())?;
        }
        Ok(wrote)
    }

    fn lock_write_observed(
        &self,
        observation: Option<&KvSnapshotObservationContext>,
    ) -> Result<KvSnapshotWriteOwner, KvError> {
        let fence = match observation {
            Some(observation) if observation.owns_writer_fence => {
                Some(observation.source.storage_owner()?)
            }
            _ => None,
        };
        let file = self.lock_write()?;
        Ok(KvSnapshotWriteOwner {
            file: Some(file),
            fence,
        })
    }

    fn begin_observation(
        &self,
        observation: Option<&KvSnapshotObservationContext>,
        existing: Option<&KvSnapshot>,
    ) -> Result<(), KvError> {
        if let Some(observation) = observation {
            observation.begin(existing.map_or(KvRevision::ZERO, |snapshot| snapshot.revision))?;
        }
        Ok(())
    }

    pub fn write_snapshot(&self, snapshot: &KvSnapshot) -> Result<(), KvError> {
        self.write_snapshot_if_materialized_changed(snapshot)
            .map(|_| ())
    }

    pub fn write_snapshot_if_materialized_changed(
        &self,
        snapshot: &KvSnapshot,
    ) -> Result<bool, KvError> {
        self.write_materialized_snapshot_with_observation_report(snapshot)
            .map(|(wrote, _)| wrote)
    }

    fn write_materialized_snapshot_with_observation_report(
        &self,
        snapshot: &KvSnapshot,
    ) -> Result<(bool, bool), KvError> {
        let observation = KvSnapshotObservationContext::admit(self)?;
        let result =
            self.write_materialized_snapshot_with_observation(snapshot, Some(&observation));
        finish_snapshot_observation_report(observation, result)
    }

    fn write_materialized_snapshot_with_observation(
        &self,
        snapshot: &KvSnapshot,
        observation: Option<&KvSnapshotObservationContext>,
    ) -> Result<bool, KvError> {
        let desired = self.materialized(snapshot);
        let _write_owner = self.lock_write_observed(observation)?;
        if let Some(observation) = observation {
            // The raw writer never decoded the previous file. Preserve that
            // cost and its ability to repair corruption: use only a hot Arc
            // whose complete file identity still matches under this lock.
            match optional_file_identity_for_read(&self.path) {
                Ok(Some(None)) => observation.begin(KvRevision::ZERO)?,
                Ok(Some(Some(identity))) => {
                    if let Some(existing) = cache_lookup_snapshot(&self.path, identity) {
                        observation.begin(existing.revision)?;
                    } else {
                        observation.begin_unknown()?;
                    }
                }
                Ok(None) | Err(_) => observation.begin_unknown()?,
            }
        }
        self.write_and_cache_if_materialized_changed_locked(desired, observation)
    }

    pub fn write_snapshot_if_changed(&self, snapshot: &KvSnapshot) -> Result<bool, KvError> {
        let observation = KvSnapshotObservationContext::admit(self)?;
        let result = self.write_snapshot_if_changed_with_observation(snapshot, Some(&observation));
        finish_snapshot_observation(observation, result)
    }

    fn write_snapshot_if_changed_with_observation(
        &self,
        snapshot: &KvSnapshot,
        observation: Option<&KvSnapshotObservationContext>,
    ) -> Result<bool, KvError> {
        let _write_owner = self.lock_write_observed(observation)?;
        let existing = self.read_snapshot_arc_if_present_locked()?;
        self.begin_observation(observation, existing.as_deref())?;
        self.write_snapshot_if_changed_against_locked(snapshot, existing.as_deref(), observation)
    }

    fn write_authoritative_snapshot_if_changed(
        &self,
        snapshot: &KvSnapshot,
    ) -> Result<bool, KvError> {
        // Native Raft restoration already owns its canonical observation.
        self.write_materialized_snapshot_with_observation(snapshot, None)
    }

    fn write_snapshot_if_changed_against_locked(
        &self,
        snapshot: &KvSnapshot,
        existing: Option<&KvSnapshot>,
        observation: Option<&KvSnapshotObservationContext>,
    ) -> Result<bool, KvError> {
        if existing.is_some_and(|existing| kv_snapshot_durable_eq(existing, snapshot)) {
            return Ok(false);
        }
        let desired = self.materialized(snapshot);
        self.write_and_cache_if_materialized_changed_locked(desired, observation)?;
        Ok(true)
    }

    fn write_raft_read_state_if_changed_against_locked(
        &self,
        snapshot: &KvSnapshot,
        existing: Option<&KvSnapshot>,
        observation: Option<&KvSnapshotObservationContext>,
    ) -> Result<bool, KvError> {
        // Routine native Raft publication retains its existing read-state
        // comparator. Canonical native receipts carry revision/index advances.
        if existing
            .is_some_and(|existing| kv_snapshot_materialized_read_state_eq(existing, snapshot))
        {
            return Ok(false);
        }
        let desired = self.materialized(snapshot);
        self.write_and_cache_if_materialized_changed_locked(desired, observation)?;
        Ok(true)
    }

    fn write_raft_snapshot_if_changed(&self, snapshot: &KvSnapshot) -> Result<bool, KvError> {
        self.write_raft_snapshot_with_observation(snapshot, None)
    }

    fn write_raft_snapshot_with_observation(
        &self,
        snapshot: &KvSnapshot,
        observation: Option<&KvSnapshotObservationContext>,
    ) -> Result<bool, KvError> {
        let _write_owner = self.lock_write_observed(observation)?;
        let existing = self.read_snapshot_arc_if_present_locked()?;
        self.begin_observation(observation, existing.as_deref())?;
        if existing.as_deref().is_some_and(|existing| {
            kv_snapshot_materialized_generation(existing)
                > kv_snapshot_materialized_generation(snapshot)
        }) {
            return Ok(false);
        }
        #[cfg(test)]
        pause_materialized_cache_raft_write_after_compare_for_test(&self.path);
        self.write_raft_read_state_if_changed_against_locked(
            snapshot,
            existing.as_deref(),
            observation,
        )
    }

    #[cfg(test)]
    fn write_imported_snapshot_if_changed(&self, snapshot: &KvSnapshot) -> Result<bool, KvError> {
        self.write_imported_snapshot_with_observation_report(snapshot)
            .map(|(wrote, _)| wrote)
    }

    fn write_imported_snapshot_with_observation_report(
        &self,
        snapshot: &KvSnapshot,
    ) -> Result<(bool, bool), KvError> {
        let observation = KvSnapshotObservationContext::admit(self)?;
        let result = self.write_imported_snapshot_with_observation(snapshot, Some(&observation));
        finish_snapshot_observation_report(observation, result)
    }

    fn write_imported_snapshot_with_observation(
        &self,
        snapshot: &KvSnapshot,
        observation: Option<&KvSnapshotObservationContext>,
    ) -> Result<bool, KvError> {
        let _write_owner = self.lock_write_observed(observation)?;
        let existing = self.read_snapshot_arc_if_present_locked()?;
        self.begin_observation(observation, existing.as_deref())?;
        if let Some(existing) = existing.as_deref() {
            match kv_snapshot_materialized_generation(existing)
                .cmp(&kv_snapshot_materialized_generation(snapshot))
            {
                std::cmp::Ordering::Greater => return Ok(false),
                std::cmp::Ordering::Equal => {
                    if kv_snapshot_durable_eq(existing, snapshot) {
                        return Ok(false);
                    }
                    return Err(KvError::ImportedSnapshotRevisionConflict {
                        revision: snapshot.revision,
                    });
                }
                std::cmp::Ordering::Less => {}
            }
        }
        self.write_snapshot_if_changed_against_locked(snapshot, existing.as_deref(), observation)
    }

    fn write_imported_snapshot_with_defer_and_observation_report<F>(
        &self,
        snapshot: &KvSnapshot,
        key_can_defer_materialized_snapshot: F,
    ) -> Result<(bool, bool), KvError>
    where
        F: FnMut(&str) -> bool,
    {
        let observation = KvSnapshotObservationContext::admit(self)?;
        let result = self.write_imported_snapshot_with_defer_and_observation(
            snapshot,
            key_can_defer_materialized_snapshot,
            Some(&observation),
        );
        finish_snapshot_observation_report(observation, result)
    }

    fn write_imported_snapshot_with_defer_and_observation<F>(
        &self,
        snapshot: &KvSnapshot,
        key_can_defer_materialized_snapshot: F,
        observation: Option<&KvSnapshotObservationContext>,
    ) -> Result<bool, KvError>
    where
        F: FnMut(&str) -> bool,
    {
        let _write_owner = self.lock_write_observed(observation)?;
        let existing = self.read_snapshot_arc_if_present_locked()?;
        self.begin_observation(observation, existing.as_deref())?;
        if let Some(existing) = existing.as_deref() {
            match kv_snapshot_materialized_generation(existing)
                .cmp(&kv_snapshot_materialized_generation(snapshot))
            {
                std::cmp::Ordering::Greater => return Ok(false),
                std::cmp::Ordering::Equal => {
                    if kv_snapshot_durable_eq(existing, snapshot) {
                        return Ok(false);
                    }
                    return Err(KvError::ImportedSnapshotRevisionConflict {
                        revision: snapshot.revision,
                    });
                }
                std::cmp::Ordering::Less => {}
            }
            // Retain the existing pure classifier under the actual File lock.
            if cluster_kv_snapshot_delta_only_deferable(
                existing,
                snapshot,
                key_can_defer_materialized_snapshot,
            ) && cluster_kv_materialized_snapshot_is_fresh_enough_against(
                existing.revision,
                snapshot.revision,
                self.materialized_snapshot_age(),
            ) {
                return Ok(false);
            }
        }
        self.write_snapshot_if_changed_against_locked(snapshot, existing.as_deref(), observation)
    }

    fn materialized_snapshot_age(&self) -> Option<Duration> {
        let Ok(metadata) = fs::metadata(&self.path) else {
            return None;
        };
        metadata
            .modified()
            .ok()
            .and_then(|modified| SystemTime::now().duration_since(modified).ok())
    }

    pub fn read_store_with_raft_view(
        &self,
        node_id: NodeId,
        paths: KvWalPaths,
    ) -> Result<InProcessKvStore, KvError> {
        self.read_snapshot_with_raft_view(node_id, paths)
            .and_then(|snapshot| InProcessKvStore::restore((*snapshot).clone()))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvLocalCache {
    materialized: KvMaterializedCache,
    raft_root: PathBuf,
}

impl KvLocalCache {
    pub(crate) fn with_observation_source(
        &self,
        source: crate::KvObservationCapacityPermit,
        owns_writer_fence: bool,
    ) -> KvObservedLocalCache {
        KvObservedLocalCache {
            cache: self.clone(),
            observation: KvSnapshotObservationContext::new(source, owns_writer_fence),
        }
    }

    pub fn new(materialized_snapshot: impl Into<PathBuf>, raft_root: impl Into<PathBuf>) -> Self {
        Self {
            materialized: KvMaterializedCache::new(materialized_snapshot),
            raft_root: raft_root.into(),
        }
    }

    /// Bound the persisted snapshot's event history to `max_events`, applied
    /// consistently across the explicit publish path and the read-triggered
    /// materialized-cache refresh.
    pub fn with_max_materialized_events(mut self, max_events: usize) -> Self {
        self.materialized = self.materialized.with_max_materialized_events(max_events);
        self
    }

    pub fn current_store(&self, node_id: NodeId) -> Result<InProcessKvStore, KvError> {
        self.materialized
            .read_store_with_raft_view(node_id.clone(), self.raft_wal_paths(&node_id))
    }

    pub fn cached_store(&self) -> Result<InProcessKvStore, KvError> {
        self.materialized.read_store()
    }

    pub fn current_get(&self, node_id: NodeId, key: &str) -> Result<Option<KvEntry>, KvError> {
        let wal_paths = self.raft_wal_paths(&node_id);
        if let Some(read) = read_resident_raft_local_entry(&wal_paths, &node_id, key) {
            return read.map(|read| read.value);
        }
        if !raft_view_identity_for_read(&wal_paths)?.0 {
            return Ok(self.materialized.read_snapshot_arc()?.get(key).cloned());
        }
        match read_cluster_kv_local_immutable_view_from_cache(
            &self.materialized,
            &node_id,
            &wal_paths,
            None,
        ) {
            Ok(ClusterKvLocalImmutableSnapshot::View(read)) => {
                Ok(read.value.store.get(key).cloned())
            }
            Ok(ClusterKvLocalImmutableSnapshot::RetryResident) => {
                if let Some(read) = read_resident_raft_local_entry(&wal_paths, &node_id, key) {
                    return read.map(|read| read.value);
                }
                Ok(self.materialized.read_snapshot_arc()?.get(key).cloned())
            }
            Err(KvError::Busy { .. }) => {
                Ok(self.materialized.read_snapshot_arc()?.get(key).cloned())
            }
            Err(error) => Err(error),
        }
    }

    pub fn current_get_serde<T: DeserializeOwned>(
        &self,
        node_id: NodeId,
        key: &str,
    ) -> Result<Option<T>, KvError> {
        self.current_get(node_id, key)?
            .map(|entry| entry.decode())
            .transpose()
    }

    pub fn current_range_prefix(
        &self,
        node_id: NodeId,
        prefix: &str,
    ) -> Result<Vec<KvEntry>, KvError> {
        let wal_paths = self.raft_wal_paths(&node_id);
        if let Some(read) = read_resident_raft_local_prefix(&wal_paths, &node_id, prefix) {
            return read.map(|read| read.value);
        }
        if !raft_view_identity_for_read(&wal_paths)?.0 {
            return self
                .materialized
                .read_snapshot_arc()?
                .try_range_prefix(prefix);
        }
        match read_cluster_kv_local_immutable_view_from_cache(
            &self.materialized,
            &node_id,
            &wal_paths,
            None,
        ) {
            Ok(ClusterKvLocalImmutableSnapshot::View(read)) => {
                read.value.store.range_prefix(prefix)
            }
            Ok(ClusterKvLocalImmutableSnapshot::RetryResident) => {
                if let Some(read) = read_resident_raft_local_prefix(&wal_paths, &node_id, prefix) {
                    return read.map(|read| read.value);
                }
                self.materialized
                    .read_snapshot_arc()?
                    .try_range_prefix(prefix)
            }
            Err(KvError::Busy { .. }) => self
                .materialized
                .read_snapshot_arc()?
                .try_range_prefix(prefix),
            Err(error) => Err(error),
        }
    }

    pub fn current_range_prefix_limited(
        &self,
        node_id: NodeId,
        prefix: &str,
        max_entries: usize,
    ) -> Result<Vec<KvEntry>, KvError> {
        let wal_paths = self.raft_wal_paths(&node_id);
        if let Some(read) = read_resident_raft_local_prefix_limited_by_payload_bytes(
            &wal_paths,
            &node_id,
            prefix,
            max_entries,
            usize::MAX,
        ) {
            return read.map(|read| read.value);
        }
        if !raft_view_identity_for_read(&wal_paths)?.0 {
            return self
                .materialized
                .read_snapshot_arc()?
                .range_prefix_limited(prefix, max_entries);
        }
        match read_cluster_kv_local_immutable_view_from_cache(
            &self.materialized,
            &node_id,
            &wal_paths,
            None,
        ) {
            Ok(ClusterKvLocalImmutableSnapshot::View(read)) => {
                read.value.store.range_prefix_limited(prefix, max_entries)
            }
            Ok(ClusterKvLocalImmutableSnapshot::RetryResident) => {
                if let Some(read) = read_resident_raft_local_prefix_limited_by_payload_bytes(
                    &wal_paths,
                    &node_id,
                    prefix,
                    max_entries,
                    usize::MAX,
                ) {
                    return read.map(|read| read.value);
                }
                self.materialized
                    .read_snapshot_arc()?
                    .range_prefix_limited(prefix, max_entries)
            }
            Err(KvError::Busy { .. }) => self
                .materialized
                .read_snapshot_arc()?
                .range_prefix_limited(prefix, max_entries),
            Err(error) => Err(error),
        }
    }

    pub fn current_range_prefix_limited_by_payload_bytes(
        &self,
        node_id: NodeId,
        prefix: &str,
        max_entries: usize,
        max_payload_bytes: usize,
    ) -> Result<Vec<KvEntry>, KvError> {
        let wal_paths = self.raft_wal_paths(&node_id);
        if let Some(read) = read_resident_raft_local_prefix_limited_by_payload_bytes(
            &wal_paths,
            &node_id,
            prefix,
            max_entries,
            max_payload_bytes,
        ) {
            return read.map(|read| read.value);
        }
        if !raft_view_identity_for_read(&wal_paths)?.0 {
            return self
                .materialized
                .read_snapshot_arc()?
                .range_prefix_limited_by_payload_bytes(prefix, max_entries, max_payload_bytes);
        }
        match read_cluster_kv_local_immutable_view_from_cache(
            &self.materialized,
            &node_id,
            &wal_paths,
            None,
        ) {
            Ok(ClusterKvLocalImmutableSnapshot::View(read)) => read
                .value
                .store
                .range_prefix_limited_by_payload_bytes(prefix, max_entries, max_payload_bytes),
            Ok(ClusterKvLocalImmutableSnapshot::RetryResident) => {
                if let Some(read) = read_resident_raft_local_prefix_limited_by_payload_bytes(
                    &wal_paths,
                    &node_id,
                    prefix,
                    max_entries,
                    max_payload_bytes,
                ) {
                    return read.map(|read| read.value);
                }
                self.materialized
                    .read_snapshot_arc()?
                    .range_prefix_limited_by_payload_bytes(prefix, max_entries, max_payload_bytes)
            }
            Err(KvError::Busy { .. }) => self
                .materialized
                .read_snapshot_arc()?
                .range_prefix_limited_by_payload_bytes(prefix, max_entries, max_payload_bytes),
            Err(error) => Err(error),
        }
    }

    pub fn current_range_prefix_serde<T: DeserializeOwned>(
        &self,
        node_id: NodeId,
        prefix: &str,
    ) -> Result<Vec<T>, KvError> {
        let entries = self.current_range_prefix(node_id, prefix)?;
        let mut values = Vec::new();
        for entry in entries {
            try_reserve_kv_items(&mut values, 1, "decode snapshot prefix range")?;
            values.push(entry.decode()?);
        }
        Ok(values)
    }

    pub fn current_range_prefix_serde_limited_by_payload_bytes<T: DeserializeOwned>(
        &self,
        node_id: NodeId,
        prefix: &str,
        max_entries: usize,
        max_payload_bytes: usize,
    ) -> Result<Vec<T>, KvError> {
        let entries = self.current_range_prefix_limited_by_payload_bytes(
            node_id,
            prefix,
            max_entries,
            max_payload_bytes,
        )?;
        let mut values = Vec::new();
        for entry in entries {
            try_reserve_kv_items(&mut values, 1, "decode snapshot prefix range")?;
            values.push(entry.decode()?);
        }
        Ok(values)
    }

    pub fn current_snapshot(&self, node_id: NodeId) -> Result<KvSnapshot, KvError> {
        self.current_snapshot_arc(node_id)
            .map(|snapshot| (*snapshot).clone())
    }

    pub fn current_snapshot_arc(&self, node_id: NodeId) -> Result<Arc<KvSnapshot>, KvError> {
        self.materialized
            .read_snapshot_with_raft_view(node_id.clone(), self.raft_wal_paths(&node_id))
    }

    pub fn current_snapshot_without_materialized_refresh(
        &self,
        node_id: NodeId,
    ) -> Result<KvSnapshot, KvError> {
        self.current_snapshot_arc_without_materialized_refresh(node_id)
            .map(|snapshot| (*snapshot).clone())
    }

    pub fn current_snapshot_arc_without_materialized_refresh(
        &self,
        node_id: NodeId,
    ) -> Result<Arc<KvSnapshot>, KvError> {
        self.materialized
            .read_snapshot_with_raft_view_without_materialized_refresh(
                node_id.clone(),
                self.raft_wal_paths(&node_id),
            )
    }

    pub fn cached_snapshot(&self) -> Result<KvSnapshot, KvError> {
        self.materialized.read_snapshot()
    }

    pub fn cached_snapshot_arc(&self) -> Result<Arc<KvSnapshot>, KvError> {
        self.materialized.read_snapshot_arc()
    }

    pub fn cached_snapshot_arc_if_present(&self) -> Result<Option<Arc<KvSnapshot>>, KvError> {
        self.materialized.read_snapshot_arc_if_present()
    }

    pub fn cached_snapshot_arc_if_hot(&self) -> Result<Option<Arc<KvSnapshot>>, KvError> {
        self.materialized.read_snapshot_arc_if_hot()
    }

    pub fn write_snapshot(&self, snapshot: &KvSnapshot) -> Result<(), KvError> {
        self.materialized.write_snapshot(snapshot)
    }

    pub fn write_snapshot_if_materialized_changed(
        &self,
        snapshot: &KvSnapshot,
    ) -> Result<bool, KvError> {
        self.materialized
            .write_snapshot_if_materialized_changed(snapshot)
    }

    pub fn write_snapshot_if_changed(&self, snapshot: &KvSnapshot) -> Result<bool, KvError> {
        self.materialized.write_snapshot_if_changed(snapshot)
    }

    pub(crate) fn write_raft_snapshot_if_changed(
        &self,
        snapshot: &KvSnapshot,
    ) -> Result<bool, KvError> {
        self.materialized.write_raft_snapshot_if_changed(snapshot)
    }

    #[cfg(test)]
    pub(crate) fn write_imported_snapshot_if_changed(
        &self,
        snapshot: &KvSnapshot,
    ) -> Result<bool, KvError> {
        self.materialized
            .write_imported_snapshot_if_changed(snapshot)
    }

    pub(crate) fn write_authoritative_snapshot_if_changed(
        &self,
        snapshot: &KvSnapshot,
    ) -> Result<bool, KvError> {
        self.materialized
            .write_authoritative_snapshot_if_changed(snapshot)
    }

    /// Cache a remote read model when it is newer than the local applied Raft
    /// state. This deliberately does not restore Raft: `KvSnapshot` has no
    /// log index or term and therefore cannot be a consensus baseline.
    ///
    /// Returns `true` when reconciling the materialized cache changed it.
    pub fn cache_snapshot_if_newer_than_raft(
        &self,
        node_id: NodeId,
        snapshot: KvSnapshot,
    ) -> Result<bool, KvError> {
        let observation = KvSnapshotObservationContext::admit(&self.materialized)?;
        let result =
            self.cache_snapshot_if_newer_than_raft_inner(node_id, snapshot, Some(&observation));
        finish_snapshot_observation(observation, result)
    }

    fn cache_snapshot_if_newer_than_raft_inner(
        &self,
        node_id: NodeId,
        snapshot: KvSnapshot,
        observation: Option<&KvSnapshotObservationContext>,
    ) -> Result<bool, KvError> {
        let wal_paths = self.raft_wal_paths(&node_id);
        let local_snapshot = match read_resident_raft_local_snapshot(&wal_paths, &node_id) {
            Some(read) => read?.value,
            None => {
                let node = DurableRaftKvNode::open_with_materialized_cache(
                    node_id.clone(),
                    wal_paths,
                    &self.materialized,
                )?;
                node.store().try_snapshot()?
            }
        };
        if kv_snapshot_materialized_generation(&snapshot)
            <= kv_snapshot_materialized_generation(&local_snapshot)
        {
            return self
                .materialized
                .write_raft_snapshot_with_observation(&local_snapshot, observation);
        }
        // Cache the newer read model, but do not install it into Raft. A plain
        // `KvSnapshot` carries no last-included index or term, so it cannot
        // establish a safe consensus baseline. The learner must replay from its
        // actual log position or receive an anchored `RaftSnapshot` from the
        // leader.
        self.materialized
            .write_imported_snapshot_with_observation(&snapshot, observation)
    }

    pub fn open_raft_node(&self, node_id: NodeId) -> Result<DurableRaftKvNode, KvError> {
        DurableRaftKvNode::open_with_materialized_cache(
            node_id.clone(),
            self.raft_wal_paths(&node_id),
            &self.materialized,
        )
    }

    pub fn open_raft_node_discarding_uncommitted_tail(
        &self,
        node_id: NodeId,
    ) -> Result<DurableRaftKvNode, KvError> {
        DurableRaftKvNode::open_discarding_uncommitted_tail_with_materialized_cache(
            node_id.clone(),
            self.raft_wal_paths(&node_id),
            &self.materialized,
        )
    }

    pub async fn open_raft_node_async(
        &self,
        node_id: NodeId,
    ) -> Result<DurableRaftKvNode, KvError> {
        DurableRaftKvNode::open_async_with_materialized_cache(
            node_id.clone(),
            self.raft_wal_paths(&node_id),
            self.materialized.clone(),
        )
        .await
    }

    pub fn raft_wal_paths(&self, node_id: &NodeId) -> KvWalPaths {
        KvWalPaths::new(self.raft_root.join(node_id.as_str()))
    }
}

pub fn kv_snapshot_durable_eq(left: &KvSnapshot, right: &KvSnapshot) -> bool {
    kv_snapshot_materialized_read_state_eq(left, right)
        && left.committed_requests == right.committed_requests
        && left.expired_requests == right.expired_requests
}

fn kv_snapshot_materialized_read_state_eq(left: &KvSnapshot, right: &KvSnapshot) -> bool {
    if left.entries.len() != right.entries.len() || left.leases.len() != right.leases.len() {
        return false;
    }
    let left_entries = left
        .entries
        .iter()
        .map(|entry| (&entry.key, &entry.value, &entry.lease_id));
    let right_entries = right
        .entries
        .iter()
        .map(|entry| (&entry.key, &entry.value, &entry.lease_id));
    if !left_entries.eq(right_entries) {
        return false;
    }
    let left_leases = left
        .leases
        .iter()
        .map(|lease| (&lease.lease_id, lease.ttl_ticks, lease.remaining_ticks));
    let right_leases = right
        .leases
        .iter()
        .map(|lease| (&lease.lease_id, lease.ttl_ticks, lease.remaining_ticks));
    left_leases.eq(right_leases)
}

/// Returns the monotonic generation available in the current `KvSnapshot`
/// format for materialized read-model arbitration.
///
/// KV revision counts key mutation events, so a committed barrier, semantic
/// no-op, or lease-only command may advance the applied Raft log while leaving
/// that revision unchanged. Every normally applied command records its log
/// index in `committed_requests`, and bounded request retention removes the
/// oldest records first, so the maximum retained index disambiguates that legal
/// same-revision progress. Legacy snapshots without request records retain the
/// former revision-only ordering through the zero fallback.
///
/// This is a materialized-cache compatibility generation, not a consensus
/// position. Replication must continue to use an anchored `RaftSnapshot` or the
/// explicit applied index carried by `KvLocalRead`.
pub(crate) fn kv_snapshot_materialized_generation(snapshot: &KvSnapshot) -> (KvRevision, u64) {
    let max_committed_log_index = snapshot
        .committed_requests
        .iter()
        .map(|record| record.log_index)
        .max()
        .unwrap_or(0);
    (snapshot.revision, max_committed_log_index)
}

pub fn compact_kv_snapshot_event_history(snapshot: &mut KvSnapshot, max_events: usize) {
    compact_snapshot_event_history(
        &mut snapshot.events,
        &mut snapshot.compacted_revision,
        max_events,
    );
}

pub(crate) fn compact_snapshot_event_history(
    events: &mut Vec<KvEvent>,
    compacted_revision: &mut KvRevision,
    max_events: usize,
) {
    let excess = events.len().saturating_sub(max_events);
    if excess == 0 {
        return;
    }
    let compacted = events[excess - 1].revision;
    events.drain(..excess);
    *compacted_revision = (*compacted_revision).max(compacted);
}

pub(crate) fn compact_shared_event_history(
    events: &mut Vec<Arc<KvEvent>>,
    compacted_revision: &mut KvRevision,
    max_events: usize,
) {
    let excess = events.len().saturating_sub(max_events);
    if excess == 0 {
        return;
    }
    let compacted = events[excess - 1].revision;
    events.drain(..excess);
    *compacted_revision = (*compacted_revision).max(compacted);
}

pub(crate) fn build_lease_key_index<'a>(
    entries: impl IntoIterator<Item = &'a KvEntry>,
) -> Result<KvLeaseKeyIndex, KvError> {
    let mut lease_keys = KvLeaseKeyIndex::default();
    for entry in entries {
        if let Some(lease_id) = entry.lease_id {
            lease_keys.try_add_key(
                lease_id,
                try_clone_kv_string(&entry.key, "build lease key index")?,
                "build lease key index",
            )?;
        }
    }
    Ok(lease_keys)
}

pub(crate) fn build_lease_deadline_index(
    leases: &KvLeaseIndex,
    lease_clock: u64,
) -> Result<(KvLeaseDeadlineByIdIndex, KvLeaseDeadlineIndex), KvError> {
    let mut deadline_by_id = KvLeaseDeadlineByIdIndex::default();
    let mut deadlines = KvLeaseDeadlineIndex::default();
    for lease in leases.values() {
        let deadline = lease_clock.saturating_add(lease.remaining_ticks);
        deadline_by_id.try_insert(lease.lease_id, deadline, "build lease deadline index")?;
        deadlines.try_insert(deadline, lease.lease_id, "build lease deadline index")?;
    }
    Ok((deadline_by_id, deadlines))
}

#[cfg(test)]
mod observed_snapshot_writer_tests {
    use super::*;
    use crate::{KvCommittedObservationValue, KvObservationProposalOwnership};

    fn snapshot(value: &[u8], puts: usize) -> KvSnapshot {
        let mut store = InProcessKvStore::new();
        for _ in 0..puts {
            store.put("observed/key", value.to_vec(), None).unwrap();
        }
        store.try_snapshot().unwrap()
    }

    fn connectivity_projection_visibility_control(imported: bool) {
        let root = sinomo_testdir::TestDir::new("connectivity-projection-visibility");
        let writer = ClusterKvPaths::new(root.join("writer"));
        let reader = if imported {
            ClusterKvPaths::new(root.join("receiver"))
        } else {
            writer.clone()
        };
        let node = NodeId::from_legacy_named("connectivity-projection-writer").unwrap();
        let store = writer.raft_store();
        store.set_leader_compact(&node, 1).unwrap();
        let connectivity = "/cluster/network/connectivity/source/target/endpoint";
        let telemetry = "/cluster/node-status/source";
        let put = |key: &str, value: &[u8]| KvOp::Put {
            key: key.into(),
            value: value.to_vec(),
            lease_id: None,
        };
        let commit = |command| {
            let result = store
                .append_command_with_response_and_publish(
                    &node,
                    command,
                    cluster_kv_key_can_defer_materialized_snapshot,
                )
                .unwrap();
            assert!(result.committed_request.log_index > 0);
            assert_eq!(result.committed_request.term, 1);
            assert_eq!(result.committed_request.revision, result.snapshot.revision);
            if imported {
                write_imported_cluster_kv_snapshot_if_changed_with_observation_report(
                    &reader,
                    &result.snapshot,
                    cluster_kv_key_can_defer_materialized_snapshot,
                )
                .unwrap();
            }
            result
        };
        // Opaque domain values keep KV independent of networking. The actual
        // publication paths, durable log, and physical projection writer run.
        commit(KvCommand::Txn(KvTxn {
            compares: Vec::new(),
            success: vec![
                put("projection/bootstrap", b"ready"),
                put(connectivity, b"old"),
            ],
            failure: Vec::new(),
        }));
        let read_file = || decode_kv_value::<KvSnapshot>(&fs::read(&reader.snapshot).unwrap()).unwrap();
        assert_eq!(read_file().get(connectivity).unwrap().value, b"old");
        let initial = fs::read(&reader.snapshot).unwrap();
        commit(KvCommand::Op(put(telemetry, b"first telemetry")));
        assert_eq!(
            fs::read(&reader.snapshot).unwrap(),
            initial,
            "ordinary telemetry must retain its existing deferral"
        );

        let renewal = commit(KvCommand::Op(put(connectivity, b"renewed")));
        let projected = read_file();
        assert_eq!(
            projected.get(connectivity).unwrap().value,
            b"renewed",
            "a cold reader must see the committed renewal without a resident Main or another write"
        );
        assert_eq!(projected.revision, renewal.snapshot.revision);
        assert_eq!(
            cluster_kv_default_read_authority_for_key(connectivity),
            KvReadAuthority::Local
        );
        assert_eq!(
            cluster_kv_keyspace_class(connectivity),
            ClusterKvKeyspaceClass::Telemetry
        );

        let renewed = fs::read(&reader.snapshot).unwrap();
        commit(KvCommand::Op(put(telemetry, b"second telemetry")));
        assert_eq!(
            fs::read(&reader.snapshot).unwrap(),
            renewed,
            "the routing exception must not disable subsequent telemetry deferral"
        );
        commit(KvCommand::Op(KvOp::Delete {
            key: connectivity.into(),
        }));
        assert!(
            read_file().get(connectivity).is_none(),
            "the same writer boundary must expose a committed connectivity withdrawal"
        );
    }

    #[test]
    fn connectivity_renewal_is_visible_in_the_native_writer_projection() {
        connectivity_projection_visibility_control(false);
    }

    #[test]
    fn connectivity_renewal_is_visible_in_the_imported_projection() {
        connectivity_projection_visibility_control(true);
    }

    fn compact_connectivity_projection_control(follower: bool) {
        let root = sinomo_testdir::TestDir::new("compact-connectivity-projection");
        let writer = ClusterKvPaths::new(root.join("writer"));
        let reader = if follower {
            ClusterKvPaths::new(root.join("follower"))
        } else {
            writer.clone()
        };
        let leader = NodeId::from_legacy_named("compact-connectivity-leader").unwrap();
        let peer = NodeId::from_legacy_named("compact-connectivity-follower").unwrap();
        let store = writer.raft_store();
        store.set_leader_compact(&leader, 1).unwrap();
        let connectivity = "/cluster/network/connectivity/source/target/endpoint";
        let telemetry = "/cluster/node-status/source";
        let put = |key: &str, value: &[u8]| KvCommand::Op(KvOp::Put {
            key: key.into(),
            value: value.to_vec(),
            lease_id: None,
        });
        let seed = store
            .append_command_with_response_and_publish(
                &leader,
                put(connectivity, b"old"),
                cluster_kv_key_can_defer_materialized_snapshot,
            )
            .unwrap();
        let append = |entry: KvLogEntry| RaftAppendEntriesRequest {
            term: entry.term,
            leader_id: leader.clone(),
            prev_log_index: entry.index - 1,
            prev_log_term: if entry.index == 1 { 0 } else { 1 },
            leader_commit: entry.index,
            entries: vec![entry],
        };
        if follower {
            let entry = store
                .open_node(&leader)
                .unwrap()
                .log_entry_at(seed.committed_request.log_index)
                .unwrap()
                .clone();
            reader
                .raft_store()
                .apply_append_entries_with_optional_snapshot_and_publish(
                    &peer,
                    None,
                    append(entry),
                    cluster_kv_key_can_defer_materialized_snapshot,
                )
                .unwrap();
        }
        let read_file = || decode_kv_value::<KvSnapshot>(&fs::read(&reader.snapshot).unwrap()).unwrap();
        assert_eq!(read_file().get(connectivity).unwrap().value, b"old");
        let mut storage = ClusterKvRaftLocalCommandStorage::new(
            store,
            leader.clone(),
            cluster_kv_key_can_defer_materialized_snapshot as fn(&str) -> bool,
        );
        let mut commit = |command| {
            let request = KvCommandRequest::generated(command);
            let entry = storage.append_uncommitted(request.clone()).unwrap();
            storage.release_session_for_quorum();
            let committed = storage.commit_index_compact(entry.index).unwrap();
            assert_eq!(committed.committed_request.request_id, request.request_id);
            assert_eq!(committed.committed_request.log_index, entry.index);
            assert_eq!(committed.read_state.applied_index, entry.index);
            storage.release_session_after_commit();
            ClusterKvRaftCommandStorage::publish_cluster_raft_post_commit_observations(
                &mut storage,
                Some(&committed),
            )
            .unwrap();
            if follower {
                let (applied, _) = reader
                    .raft_store()
                    .apply_append_entries_with_optional_snapshot_compact_and_publish(
                        &peer,
                        None,
                        append(entry),
                    )
                    .unwrap();
                assert!(applied.response.success);
                assert_eq!(applied.read_state, committed.read_state);
            }
            committed.read_state.revision
        };
        let initial = fs::read(&reader.snapshot).unwrap();
        commit(put(telemetry, b"ordinary telemetry"));
        assert_eq!(fs::read(&reader.snapshot).unwrap(), initial);
        let revision = commit(put(connectivity, b"renewed"));
        let renewed = read_file();
        assert_eq!(
            renewed.get(connectivity).unwrap().value,
            b"renewed",
            "the production compact publication path must expose the committed renewal"
        );
        assert_eq!(renewed.revision, revision);
        let bytes = fs::read(&reader.snapshot).unwrap();
        commit(put(telemetry, b"later telemetry"));
        assert_eq!(fs::read(&reader.snapshot).unwrap(), bytes);
        commit(KvCommand::Op(KvOp::Delete {
            key: connectivity.into(),
        }));
        assert!(read_file().get(connectivity).is_none());
    }

    #[test]
    fn compact_connectivity_renewal_is_visible_after_leader_commit() {
        compact_connectivity_projection_control(false);
    }

    #[test]
    fn compact_connectivity_renewal_is_visible_after_follower_apply() {
        compact_connectivity_projection_control(true);
    }

    #[test]
    fn compact_connectivity_renewal_after_restore_preserves_installed_snapshot_identity() {
        let root = sinomo_testdir::TestDir::new("compact-connectivity-restore");
        let paths = ClusterKvPaths::new(root.to_path_buf());
        let leader = NodeId::from_legacy_named("compact-connectivity-restore-leader").unwrap();
        let follower = NodeId::from_legacy_named("compact-connectivity-restore-follower").unwrap();
        let key = "/cluster/network/connectivity/source/target/endpoint";
        let mut seed = InProcessKvStore::new();
        seed.put(key, b"old".to_vec(), None).unwrap();
        let installed = Arc::new(seed.try_snapshot().unwrap());
        let request = KvCommandRequest::generated(KvCommand::Op(KvOp::Put {
            key: key.into(),
            value: b"renewed".to_vec(),
            lease_id: None,
        }));
        let (applied, wrote) = paths
            .raft_store()
            .apply_append_entries_with_optional_snapshot_compact_and_publish(
                &follower,
                Some(RaftSnapshot {
                    last_included_index: 7,
                    last_included_term: 1,
                    kv: Arc::clone(&installed),
                }),
                RaftAppendEntriesRequest {
                    term: 1,
                    leader_id: leader,
                    prev_log_index: 7,
                    prev_log_term: 1,
                    entries: vec![KvLogEntry {
                        index: 8,
                        term: 1,
                        request_id: request.request_id,
                        command: request.command,
                        revision: installed.revision,
                    }],
                    leader_commit: 8,
                },
            )
            .unwrap();
        assert!(applied.response.success && wrote);
        assert_eq!(applied.restored_snapshot.as_ref(), Some(&installed));
        assert_eq!(applied.restored_snapshot_applied_index, Some(7));
        assert_eq!(applied.read_state.applied_index, 8);
        let cold = decode_kv_value::<KvSnapshot>(&fs::read(&paths.snapshot).unwrap()).unwrap();
        assert_eq!(cold.get(key).unwrap().value, b"renewed");
        assert_eq!(cold.revision, applied.read_state.revision);
    }

    fn require_writer_released(path: &Path) {
        let lock_path = KvMaterializedCache::new(path).write_lock_path();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&lock_path)
            .unwrap();
        lock_node_exclusive(&file, &lock_path, Duration::ZERO)
            .expect("snapshot callback must execute after the real writer File released");
    }

    #[test]
    fn writer_reports_only_its_own_completed_delivery_preparation() {
        let root = sinomo_testdir::TestDir::new("observed-writer-preparation-report");
        let paths = ClusterKvPaths::new(root.join("cluster"));
        let mut store = InProcessKvStore::new();
        store.put("report/one", b"one".to_vec(), None).unwrap();
        assert_eq!(
            write_cluster_kv_materialized_snapshot_if_changed_with_observation_report(
                &paths,
                &store.try_snapshot().unwrap(),
            )
            .unwrap(),
            (true, false)
        );
        let preparations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = Arc::clone(&preparations);
        let subscription = crate::subscribe_committed_observations(&paths, move |event| {
            event.prepare_once(|| {
                count.fetch_add(1, Ordering::SeqCst);
            });
        });
        store.put("report/two", b"two".to_vec(), None).unwrap();
        assert_eq!(
            write_cluster_kv_snapshot_if_changed_with_observation_report(
                &paths,
                &store.try_snapshot().unwrap(),
            )
            .unwrap(),
            (true, true)
        );
        store.put("report/three", b"three".to_vec(), None).unwrap();
        assert_eq!(
            write_imported_cluster_kv_snapshot_if_changed_with_observation_report(
                &paths,
                &store.try_snapshot().unwrap(),
                |_| false,
            )
            .unwrap(),
            (true, true)
        );
        assert_eq!(preparations.load(Ordering::SeqCst), 2);
        drop(subscription);
        store.put("report/four", b"four".to_vec(), None).unwrap();
        assert_eq!(
            write_cluster_kv_materialized_snapshot_if_changed_with_observation_report(
                &paths,
                &store.try_snapshot().unwrap(),
            )
            .unwrap(),
            (true, false),
            "a prior prepared writer does not suppress this writer's fallback"
        );
    }

    #[test]
    fn observed_materialized_writer_shares_cached_arc_and_skips_unchanged_io() {
        let root = sinomo_testdir::TestDir::new("observed-materialized-writer");
        let paths = ClusterKvPaths::new(root.join("cluster"));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&seen);
        let callback_paths = paths.clone();
        let _subscription = crate::subscribe_committed_observations(&paths, move |event| {
            require_writer_released(&callback_paths.snapshot);
            if let KvCommittedObservationValue::Snapshot(snapshot) = &event.value {
                let cache = callback_paths
                    .local_cache()
                    .cached_snapshot_arc_if_hot()
                    .unwrap()
                    .unwrap();
                assert!(
                    Arc::ptr_eq(snapshot, &cache),
                    "queue and cache share the writer's one immutable snapshot"
                );
            }
            captured.lock().unwrap().push(event.clone());
        });
        let first = snapshot(b"first", 1);
        assert!(write_cluster_kv_materialized_snapshot_if_changed(&paths, &first).unwrap());
        assert!(!write_cluster_kv_materialized_snapshot_if_changed(&paths, &first).unwrap());
        let events = seen.lock().unwrap();
        assert_eq!(
            events.len(),
            1,
            "unchanged bytes publish neither replacement nor gap"
        );
        assert_eq!(events[0].previous_revision, Some(KvRevision::ZERO));
        assert_eq!(events[0].revision, Some(first.revision));
        assert_eq!(
            events[0].applied_index, None,
            "derived cache publication invents no Raft index"
        );
        assert!(matches!(
            events[0].value,
            KvCommittedObservationValue::Snapshot(_)
        ));
    }

    #[test]
    fn observed_imported_writer_conflict_and_unchanged_view_emit_no_replacement() {
        let root = sinomo_testdir::TestDir::new("observed-imported-writer");
        let paths = ClusterKvPaths::new(root.join("cluster"));
        let first = snapshot(b"first", 1);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&seen);
        let _subscription = crate::subscribe_committed_observations(&paths, move |event| {
            captured.lock().unwrap().push(event.clone());
        });
        assert!(write_cluster_kv_snapshot_if_changed(&paths, &first).unwrap());
        assert!(!write_cluster_kv_snapshot_if_changed(&paths, &first).unwrap());
        let mut conflict = first.clone();
        conflict.entries[0].value = b"conflict".to_vec();
        assert!(matches!(
            write_cluster_kv_snapshot_if_changed(&paths, &conflict),
            Err(KvError::ImportedSnapshotRevisionConflict { .. })
        ));
        assert_eq!(
            seen.lock().unwrap().len(),
            1,
            "a definitely unapplied comparison failure is an ordered skip"
        );
        assert_eq!(paths.local_cache().cached_snapshot().unwrap(), first);
    }

    #[test]
    fn observed_writer_ordinals_follow_actual_writer_lock_order() {
        let root = sinomo_testdir::TestDir::new("observed-snapshot-writer-order");
        let paths = ClusterKvPaths::new(root.join("cluster"));
        let cache = paths.local_cache().materialized;
        let seen = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&seen);
        let callback_path = paths.snapshot.clone();
        let _subscription = crate::subscribe_committed_observations(&paths, move |event| {
            require_writer_released(&callback_path);
            captured.lock().unwrap().push(event.revision);
        });
        // Admission order is deliberately opposite to actual File ownership.
        let admitted_first = KvSnapshotObservationContext::admit(&cache).unwrap();
        let admitted_second = KvSnapshotObservationContext::admit(&cache).unwrap();
        cache
            .write_materialized_snapshot_with_observation(
                &snapshot(b"first", 1),
                Some(&admitted_second),
            )
            .unwrap();
        cache
            .write_materialized_snapshot_with_observation(
                &snapshot(b"second", 2),
                Some(&admitted_first),
            )
            .unwrap();
        assert!(seen.lock().unwrap().is_empty());
        admitted_second.finish().unwrap();
        admitted_first.finish().unwrap();
        assert_eq!(
            *seen.lock().unwrap(),
            vec![Some(KvRevision(1)), Some(KvRevision(2))]
        );
    }

    #[test]
    fn observed_writer_uncertain_io_failure_preserves_known_prestate_gap() {
        let root = sinomo_testdir::TestDir::new("observed-snapshot-writer-error");
        let paths = ClusterKvPaths::new(root.join("cluster"));
        let cache = paths.local_cache().materialized;
        let first = snapshot(b"first", 1);
        cache
            .write_authoritative_snapshot_if_changed(&first)
            .unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&seen);
        let callback_path = paths.snapshot.clone();
        let _subscription = crate::subscribe_committed_observations(&paths, move |event| {
            require_writer_released(&callback_path);
            captured.lock().unwrap().push(event.clone());
        });
        let context = KvSnapshotObservationContext::admit(&cache).unwrap();
        let owner = cache.lock_write_observed(Some(&context)).unwrap();
        context.begin(first.revision).unwrap();
        // Make the actual replacement fail after its known prestate was
        // captured under File ownership, without manufacturing future metadata.
        fs::remove_file(&paths.snapshot).unwrap();
        fs::create_dir(&paths.snapshot).unwrap();
        let result = cache
            .write_and_cache_if_materialized_changed_locked(snapshot(b"future", 2), Some(&context));
        assert!(result.is_err());
        assert!(seen.lock().unwrap().is_empty());
        drop(owner);
        assert!(finish_snapshot_observation(context, result).is_err());
        let events = seen.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].previous_revision, Some(first.revision));
        assert_eq!(events[0].revision, Some(first.revision));
        assert_eq!(events[0].applied_index, None);
        assert!(matches!(
            events[0].value,
            KvCommittedObservationValue::Discontinuity(
                crate::KvCommittedObservationDiscontinuity::PublicationAbandoned
            )
        ));
    }

    #[test]
    fn observed_raw_replacement_repairs_corrupt_old_view_with_unknown_prestate() {
        let root = sinomo_testdir::TestDir::new("observed-corrupt-snapshot-repair");
        let paths = ClusterKvPaths::new(root.join("cluster"));
        fs::create_dir_all(&paths.root).unwrap();
        fs::write(&paths.snapshot, b"corrupt prior materialization").unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&seen);
        let _subscription = crate::subscribe_committed_observations(&paths, move |event| {
            captured.lock().unwrap().push(event.clone());
        });
        let desired = snapshot(b"restored", 2);
        assert!(write_cluster_kv_materialized_snapshot_if_changed(&paths, &desired).unwrap());
        let events = seen.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].previous_revision, None,
            "the corrupt old file supplied no revision evidence"
        );
        assert_eq!(events[0].revision, Some(desired.revision));
        assert_eq!(events[0].applied_index, None);
        assert!(matches!(
            events[0].value,
            KvCommittedObservationValue::Snapshot(_)
        ));
    }

    #[test]
    fn observed_cache_reconciliation_finishes_after_outer_raft_owner_releases() {
        let root = sinomo_testdir::TestDir::new("observed-cache-outer-raft-owner");
        let paths = ClusterKvPaths::new(root.join("cluster"));
        let node = NodeId::from_legacy_named("observed-cache-local").unwrap();
        let store = paths.raft_store();
        let source =
            crate::try_admit_committed_observation(&paths, KvObservationProposalOwnership::Absent)
                .unwrap();
        let fence = source.storage_owner().unwrap();
        let actual_owner = store.lock_operation().unwrap();
        let outer_released = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let released = Arc::clone(&outer_released);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&seen);
        let callback_store = store.clone();
        let callback_path = paths.snapshot.clone();
        let _subscription = crate::subscribe_committed_observations(&paths, move |event| {
            assert!(
                released.load(Ordering::Acquire),
                "combined outer owner must drop before callback entry"
            );
            let _operation = callback_store.lock_operation().unwrap();
            require_writer_released(&callback_path);
            captured.lock().unwrap().push(event.clone());
        });
        let observed = paths.local_cache().with_observation_source(source, false);
        assert!(
            observed
                .cache_snapshot_if_newer_than_raft(node, snapshot(b"remote", 1))
                .unwrap()
        );
        assert!(seen.lock().unwrap().is_empty());
        drop(actual_owner);
        outer_released.store(true, Ordering::Release);
        drop(fence);
        assert!(
            seen.lock().unwrap().is_empty(),
            "captured payload remains staged until explicit finish"
        );
        observed.finish_observation().unwrap();
        assert_eq!(seen.lock().unwrap().len(), 1);
    }
}
