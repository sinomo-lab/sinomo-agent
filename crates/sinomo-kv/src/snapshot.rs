use crate::internal::*;

pub fn read_cluster_kv_cached_snapshot(paths: &ClusterKvPaths) -> Result<KvSnapshot, KvError> {
    paths.local_cache().cached_snapshot()
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

/// Reads the best locally available KV view without contacting a quorum.
///
/// The returned metadata is observational only: neither the known leader nor
/// the observed term establishes freshness.
pub fn read_cluster_kv_local_snapshot(
    paths: &ClusterKvPaths,
    node_id: &NodeId,
) -> Result<KvLocalRead<KvSnapshot>, KvError> {
    let node = paths.raft_store().open_node(node_id)?;
    // Read value and metadata from the same opened Raft view. Reopening via
    // the materialized-cache helper here can observe a different applied
    // index (or fall back while this node lock is held).
    let snapshot = node.store().try_snapshot()?;
    Ok(KvLocalRead {
        revision: snapshot.revision,
        observed_term: node.hard_state().current_term,
        applied_index: node.applied_index(),
        known_leader: node.hard_state().leader_id.clone(),
        value: snapshot,
    })
}

pub fn read_cluster_kv_local_entry(
    paths: &ClusterKvPaths,
    node_id: &NodeId,
    key: &str,
) -> Result<KvLocalRead<Option<KvEntry>>, KvError> {
    let local = read_cluster_kv_local_snapshot(paths, node_id)?;
    let value = local.value.get(key).cloned();
    Ok(KvLocalRead {
        value,
        revision: local.revision,
        observed_term: local.observed_term,
        applied_index: local.applied_index,
        known_leader: local.known_leader,
    })
}

pub fn read_cluster_kv_local_prefix(
    paths: &ClusterKvPaths,
    node_id: &NodeId,
    prefix: &str,
) -> Result<KvLocalRead<Vec<KvEntry>>, KvError> {
    let local = read_cluster_kv_local_snapshot(paths, node_id)?;
    let value = local.value.try_range_prefix(prefix)?;
    Ok(KvLocalRead {
        value,
        revision: local.revision,
        observed_term: local.observed_term,
        applied_index: local.applied_index,
        known_leader: local.known_leader,
    })
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
    let wrote = paths
        .local_cache()
        .write_snapshot_if_materialized_changed(snapshot)?;
    if wrote {
        notify_cluster_kv_watchers(paths);
    }
    Ok(wrote)
}

pub fn write_cluster_kv_snapshot_if_changed(
    paths: &ClusterKvPaths,
    snapshot: &KvSnapshot,
) -> Result<bool, KvError> {
    let wrote = paths.local_cache().write_snapshot_if_changed(snapshot)?;
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
    if cluster_kv_imported_snapshot_can_defer_materialized_delta(
        paths,
        snapshot,
        key_can_defer_materialized_snapshot,
    )? {
        return Ok(false);
    }
    write_cluster_kv_snapshot_if_changed(paths, snapshot)
}

pub(crate) fn cluster_kv_imported_snapshot_can_defer_materialized_delta<F>(
    paths: &ClusterKvPaths,
    snapshot: &KvSnapshot,
    key_can_defer_materialized_snapshot: F,
) -> Result<bool, KvError>
where
    F: FnMut(&str) -> bool,
{
    let Some(existing) = paths.local_cache().cached_snapshot_arc_if_present()? else {
        return Ok(false);
    };
    if !cluster_kv_snapshot_delta_only_deferable(
        existing.as_ref(),
        snapshot,
        key_can_defer_materialized_snapshot,
    ) {
        return Ok(false);
    }
    Ok(cluster_kv_materialized_snapshot_is_fresh_enough_against(
        existing.revision,
        snapshot.revision,
        cluster_kv_materialized_snapshot_age(paths),
    ))
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
        let mut committed_request_bytes = 0_usize;
        for record in &self.committed_requests {
            committed_request_bytes = committed_request_bytes
                .checked_add(encode_kv_value(record)?.len())
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
        self.leases
            .sort_by(|left, right| left.lease_id.cmp(&right.lease_id));
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
        let (has_durable_state, identity) = raft_view_identity_for_read(&paths)?;
        if has_durable_state {
            if let Some(identity) = identity
                && let Some(snapshot) = cache_lookup_raft_view(&paths, identity)
            {
                return Ok(snapshot);
            }
            let node =
                match DurableRaftKvNode::try_open_with_materialized_cache(node_id, paths, self) {
                    Ok(node) => node,
                    Err(error @ KvError::Busy { .. }) => {
                        return match self.read_snapshot_arc_if_present()? {
                            Some(snapshot) => Ok(snapshot),
                            None => Err(error),
                        };
                    }
                    Err(error) => return Err(error),
                };
            let snapshot = node.store().try_snapshot()?.canonicalized();
            if refresh_materialized {
                self.write_snapshot_if_changed(&snapshot)?;
            }
            let snapshot = Arc::new(snapshot);
            if let Some(identity) = raft_view_identity(&node.paths) {
                cache_store_raft_view(&node.paths, identity, snapshot.clone());
            }
            return Ok(snapshot);
        }
        if let Some(snapshot) = self.read_snapshot_arc_if_present()? {
            return Ok(snapshot);
        }
        let snapshot = Arc::new(InProcessKvStore::new().try_snapshot()?.canonicalized());
        Ok(snapshot)
    }

    pub fn read_store(&self) -> Result<InProcessKvStore, KvError> {
        self.read_snapshot().and_then(InProcessKvStore::restore)
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

    /// Persists `snapshot` and refreshes the process-wide read cache so subsequent
    /// reads hit memory rather than re-reading the file.
    fn write_and_cache(&self, snapshot: KvSnapshot) -> Result<(), KvError> {
        self.write_and_cache_if_materialized_changed(snapshot)
            .map(|_| ())
    }

    /// Persists `snapshot` exactly as materialized and reports whether the object
    /// file changed. Unlike `write_snapshot_if_changed`, this treats revision and
    /// watch-event history as meaningful so callers that publish snapshot-backed
    /// events can suppress only truly redundant notifications.
    fn write_and_cache_if_materialized_changed(
        &self,
        snapshot: KvSnapshot,
    ) -> Result<bool, KvError> {
        snapshot.validate_resource_bounds()?;
        let wrote = write_object_file_if_changed(&self.path, &snapshot)?;
        if let Some(identity) = file_identity(&self.path) {
            cache_store_snapshot(&self.path, identity, Arc::new(snapshot));
        }
        Ok(wrote)
    }

    pub fn write_snapshot(&self, snapshot: &KvSnapshot) -> Result<(), KvError> {
        self.write_and_cache(self.materialized(snapshot))
    }

    pub fn write_snapshot_if_materialized_changed(
        &self,
        snapshot: &KvSnapshot,
    ) -> Result<bool, KvError> {
        self.write_and_cache_if_materialized_changed(self.materialized(snapshot))
    }

    pub fn write_snapshot_if_changed(&self, snapshot: &KvSnapshot) -> Result<bool, KvError> {
        // Skip the rewrite when the durable key/value/lease state is unchanged, even
        // if the snapshot revision, per-entry revision metadata, or watch-event tail
        // advanced. Re-putting an identical value bumps modify_revision/version and
        // appends events, which would otherwise rewrite the whole (~25 KiB+)
        // materialized file for no durable change. The materialized snapshot is only
        // a read cache rather than writable Raft state (the WAL and anchored Raft
        // snapshots are the source of truth for revision/version/events), so
        // persisting stale bookkeeping there is safe. Check before materializing
        // `snapshot` so the common no-op path avoids cloning the full entry set.
        if self.read_snapshot_arc_if_present().is_ok_and(|existing| {
            existing.is_some_and(|existing| kv_snapshot_durable_eq(existing.as_ref(), snapshot))
        }) {
            return Ok(false);
        }
        let desired = self.materialized(snapshot);
        self.write_and_cache(desired)?;
        Ok(true)
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
        let snapshot = self
            .materialized
            .read_snapshot_with_raft_view(node_id.clone(), self.raft_wal_paths(&node_id))?;
        Ok(snapshot.get(key).cloned())
    }

    pub fn current_get_serde<T: DeserializeOwned>(
        &self,
        node_id: NodeId,
        key: &str,
    ) -> Result<Option<T>, KvError> {
        let snapshot = self
            .materialized
            .read_snapshot_with_raft_view(node_id.clone(), self.raft_wal_paths(&node_id))?;
        snapshot.get(key).map(|entry| entry.decode()).transpose()
    }

    pub fn current_range_prefix(
        &self,
        node_id: NodeId,
        prefix: &str,
    ) -> Result<Vec<KvEntry>, KvError> {
        let snapshot = self
            .materialized
            .read_snapshot_with_raft_view(node_id.clone(), self.raft_wal_paths(&node_id))?;
        snapshot.try_range_prefix(prefix)
    }

    pub fn current_range_prefix_limited(
        &self,
        node_id: NodeId,
        prefix: &str,
        max_entries: usize,
    ) -> Result<Vec<KvEntry>, KvError> {
        let snapshot = self
            .materialized
            .read_snapshot_with_raft_view(node_id.clone(), self.raft_wal_paths(&node_id))?;
        snapshot.range_prefix_limited(prefix, max_entries)
    }

    pub fn current_range_prefix_limited_by_payload_bytes(
        &self,
        node_id: NodeId,
        prefix: &str,
        max_entries: usize,
        max_payload_bytes: usize,
    ) -> Result<Vec<KvEntry>, KvError> {
        let snapshot = self
            .materialized
            .read_snapshot_with_raft_view(node_id.clone(), self.raft_wal_paths(&node_id))?;
        snapshot.range_prefix_limited_by_payload_bytes(prefix, max_entries, max_payload_bytes)
    }

    pub fn current_range_prefix_serde<T: DeserializeOwned>(
        &self,
        node_id: NodeId,
        prefix: &str,
    ) -> Result<Vec<T>, KvError> {
        let snapshot = self
            .materialized
            .read_snapshot_with_raft_view(node_id.clone(), self.raft_wal_paths(&node_id))?;
        let mut values = Vec::new();
        for entry in snapshot.prefix_entries(prefix) {
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
        let snapshot = self
            .materialized
            .read_snapshot_with_raft_view(node_id.clone(), self.raft_wal_paths(&node_id))?;
        snapshot.range_prefix_serde_limited_by_payload_bytes(prefix, max_entries, max_payload_bytes)
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
        let wal_paths = self.raft_wal_paths(&node_id);
        let node = DurableRaftKvNode::open_with_materialized_cache(
            node_id.clone(),
            wal_paths,
            &self.materialized,
        )?;
        if snapshot.revision <= node.store().revision() {
            return self
                .materialized
                .write_snapshot_if_changed(&node.store().try_snapshot()?);
        }
        // Cache the newer read model, but do not install it into Raft. A plain
        // `KvSnapshot` carries no last-included index or term, so it cannot
        // establish a safe consensus baseline. The learner must replay from its
        // actual log position or receive an anchored `RaftSnapshot` from the
        // leader.
        self.materialized.write_snapshot_if_changed(&snapshot)
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
        && left.committed_requests == right.committed_requests
        && left.expired_requests == right.expired_requests
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
