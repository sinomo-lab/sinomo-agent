use crate::internal::*;

mod event_log;
mod notifier;
pub use event_log::{
    ClusterKvWatchEventAppendCacheView, ClusterKvWatchEventAppendReport,
    ClusterKvWatchEventLogRead, ClusterKvWatchEventsRead, EncodedClusterKvWatchEventLogBatch,
    append_cluster_kv_watch_events_to_storage,
    append_cluster_kv_watch_events_to_storage_with_observers, cluster_kv_watch_event_payload_bytes,
    read_cluster_kv_watch_event_checkpoint, read_cluster_kv_watch_event_log,
    read_cluster_kv_watch_event_log_from_file_with_observer,
    read_cluster_kv_watch_event_log_with_observer, read_cluster_kv_watch_events,
    read_cluster_kv_watch_events_with_observer, write_cluster_kv_watch_event_checkpoint,
};
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use event_log::{
    append_cluster_kv_watch_event_log_batch_to_file, decode_cluster_kv_watch_event_log_record,
    encode_cluster_kv_watch_event_log_batch, lock_cluster_kv_watch_event_log_for_append,
    open_cluster_kv_watch_event_log_for_append, truncate_cluster_kv_watch_event_log_file,
};
pub use notifier::{
    ClusterKvWatchObservedGeneration, cluster_kv_watch_generation,
    cluster_kv_watch_observed_generation, notify_cluster_kv_watchers,
    notify_cluster_kv_watchers_for_events, notify_cluster_kv_watchers_for_watch_ids,
    wait_for_cluster_kv_watch_change, wait_for_cluster_kv_watch_change_for_watch,
};
#[cfg(any(test, feature = "test-support"))]
pub use notifier::{
    cluster_kv_watch_notifier_capacity_for_test, cluster_kv_watch_notifier_has_key_for_test,
    cluster_kv_watch_notifier_has_raw_key_for_test,
    cluster_kv_watch_notifier_waiter_count_for_test, cluster_kv_watch_unchanged_wakeups_for_test,
    record_cluster_kv_watch_notifier_waiter_key_for_test,
    remove_cluster_kv_watch_notifier_entries_under_root_for_test,
    reset_cluster_kv_watch_unchanged_wakeups_for_test, touch_cluster_kv_watch_notifier_for_test,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterKvWatchPrefixRequest {
    pub prefix: String,
    pub start_revision: KvRevision,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterKvWatchRegistration {
    pub watch_id: WatchId,
    pub prefix: String,
    pub next_revision: KvRevision,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterKvWatchAllocatorState {
    pub next_watch_id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_watch_count: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pruned_compacted_revision: Option<KvRevision>,
}

impl ClusterKvWatchAllocatorState {
    pub fn initial_for_missing_state(next_watch_id: u64) -> Self {
        Self {
            next_watch_id: next_watch_id.max(1),
            active_watch_count: None,
            pruned_compacted_revision: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterKvWatchDrainRequest {
    pub watch_id: WatchId,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub wait_timeout_millis: u64,
}

impl ClusterKvWatchDrainRequest {
    pub fn wait_timeout(&self) -> Duration {
        let max_millis = MAX_CLUSTER_KV_WATCH_DRAIN_WAIT.as_millis() as u64;
        Duration::from_millis(self.wait_timeout_millis.min(max_millis))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterKvWatchDrainResponse {
    pub watch_id: WatchId,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_fallible_vec"
    )]
    pub events: Vec<KvEvent>,
    pub next_revision: KvRevision,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterKvWatchCancelRequest {
    pub watch_id: WatchId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterKvWatchCancelResponse {
    pub watch_id: WatchId,
    pub cancelled: bool,
}

pub fn collect_cluster_kv_watch_drain_events(
    persisted_events: &[KvEvent],
    snapshot: &KvSnapshot,
    registration: &ClusterKvWatchRegistration,
    max_payload_bytes: usize,
) -> Result<Vec<KvEvent>, KvError> {
    let mut events = Vec::new();
    for event in persisted_events
        .iter()
        .chain(snapshot.events.iter())
        .filter(|event| {
            event.revision >= registration.next_revision
                && event.key.starts_with(&registration.prefix)
        })
    {
        try_reserve_kv_items(&mut events, 1, "collect watch drain events")?;
        events.push(event);
    }
    events.sort_by(|left, right| {
        (left.revision, left.key.as_str()).cmp(&(right.revision, right.key.as_str()))
    });
    events.dedup_by(|left, right| {
        left.revision == right.revision && left.key == right.key && left.kind == right.kind
    });
    limit_cluster_kv_watch_drain_events(events, max_payload_bytes)
}

pub(crate) fn limit_cluster_kv_watch_drain_events(
    events: Vec<&KvEvent>,
    max_payload_bytes: usize,
) -> Result<Vec<KvEvent>, KvError> {
    let mut limited = Vec::new();
    let mut payload_bytes = 0_usize;
    let mut events = events.into_iter().peekable();
    while let Some(event) = events.next() {
        let event_bytes = cluster_kv_watch_event_payload_bytes(event);
        let next_payload_bytes = payload_bytes.saturating_add(event_bytes);
        if !limited.is_empty() && next_payload_bytes > max_payload_bytes {
            let current_revision = limited
                .last()
                .map(|last: &KvEvent| last.revision)
                .unwrap_or(KvRevision::ZERO);
            if event.revision != current_revision {
                break;
            }
        }
        payload_bytes = next_payload_bytes;
        try_reserve_kv_items(&mut limited, 1, "limit watch drain events")?;
        limited.push(event.clone());
        if payload_bytes >= max_payload_bytes {
            let current_revision = event.revision;
            if !events
                .peek()
                .is_some_and(|candidate| candidate.revision == current_revision)
            {
                break;
            }
        }
    }
    Ok(limited)
}

pub fn cluster_kv_watch_registration_cursor_should_persist(
    drained_events: usize,
    next_revision: KvRevision,
    durable_next_revision: KvRevision,
    registration_file_unchanged: bool,
    registration_file_age: Option<Duration>,
) -> bool {
    if drained_events >= CLUSTER_KV_WATCH_DEFERRED_CURSOR_MAX_EVENTS {
        return true;
    }
    let revision_lag = next_revision.0.saturating_sub(durable_next_revision.0);
    if revision_lag >= CLUSTER_KV_WATCH_DEFERRED_CURSOR_MAX_REVISION_LAG {
        return true;
    }
    if !registration_file_unchanged {
        return true;
    }
    match registration_file_age {
        Some(age) => age >= CLUSTER_KV_WATCH_DEFERRED_CURSOR_MAX_AGE,
        None => true,
    }
}

pub fn cluster_kv_watch_events_payload_bytes(events: &[KvEvent]) -> usize {
    events.iter().fold(0_usize, |total, event| {
        total.saturating_add(cluster_kv_watch_event_payload_bytes(event))
    })
}

pub fn cluster_kv_events_can_defer_materialized_snapshot(
    events: &[KvEvent],
    mut key_can_defer: impl FnMut(&str) -> bool,
) -> bool {
    !events.is_empty() && events.iter().all(|event| key_can_defer(&event.key))
}

pub fn cluster_kv_key_can_defer_materialized_snapshot(key: &str) -> bool {
    key.starts_with("/cluster/network/connectivity/")
        || key.starts_with("/cluster/network/info/")
        || key.starts_with("/cluster/node-status/")
}

pub fn cluster_kv_snapshot_delta_only_deferable(
    existing: &KvSnapshot,
    snapshot: &KvSnapshot,
    mut key_can_defer: impl FnMut(&str) -> bool,
) -> bool {
    if existing.leases.len() != snapshot.leases.len() {
        return false;
    }
    let leases_match = existing
        .leases
        .iter()
        .map(|lease| (&lease.lease_id, lease.ttl_ticks, lease.remaining_ticks))
        .eq(snapshot
            .leases
            .iter()
            .map(|lease| (&lease.lease_id, lease.ttl_ticks, lease.remaining_ticks)));
    if !leases_match {
        return false;
    }

    let mut changed = false;
    let mut existing_entries = existing.entries.iter().peekable();
    let mut incoming_entries = snapshot.entries.iter().peekable();
    loop {
        match (existing_entries.peek(), incoming_entries.peek()) {
            (Some(existing_entry), Some(incoming_entry)) => {
                match existing_entry.key.cmp(&incoming_entry.key) {
                    std::cmp::Ordering::Less => {
                        if !key_can_defer(&existing_entry.key) {
                            return false;
                        }
                        changed = true;
                        existing_entries.next();
                    }
                    std::cmp::Ordering::Greater => {
                        if !key_can_defer(&incoming_entry.key) {
                            return false;
                        }
                        changed = true;
                        incoming_entries.next();
                    }
                    std::cmp::Ordering::Equal => {
                        if existing_entry.value != incoming_entry.value
                            || existing_entry.lease_id != incoming_entry.lease_id
                        {
                            if !key_can_defer(&incoming_entry.key) {
                                return false;
                            }
                            changed = true;
                        }
                        existing_entries.next();
                        incoming_entries.next();
                    }
                }
            }
            (Some(existing_entry), None) => {
                if !key_can_defer(&existing_entry.key) {
                    return false;
                }
                changed = true;
                existing_entries.next();
            }
            (None, Some(incoming_entry)) => {
                if !key_can_defer(&incoming_entry.key) {
                    return false;
                }
                changed = true;
                incoming_entries.next();
            }
            (None, None) => return changed,
        }
    }
}

pub fn cluster_kv_materialized_snapshot_is_fresh_enough_against(
    existing_revision: KvRevision,
    snapshot_revision: KvRevision,
    materialized_snapshot_age: Option<Duration>,
) -> bool {
    if existing_revision > snapshot_revision {
        return false;
    }
    let revision_lag = snapshot_revision.0.saturating_sub(existing_revision.0);
    if revision_lag >= CLUSTER_KV_DEFERRED_MATERIALIZED_MAX_REVISION_LAG {
        return false;
    }
    matches!(
        materialized_snapshot_age,
        Some(age) if age < CLUSTER_KV_DEFERRED_MATERIALIZED_MIN_INTERVAL
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClusterKvWatchEventFanoutBatchLimit {
    EventCount {
        observed_events: usize,
        max_events: usize,
    },
    PayloadBytes {
        observed_bytes: usize,
        max_bytes: usize,
    },
}

impl ClusterKvWatchEventFanoutBatchLimit {
    pub fn detail(&self) -> String {
        match self {
            Self::EventCount {
                observed_events,
                max_events,
            } => {
                format!(
                    "watch event fan-out batch has {observed_events} events; max is {max_events}"
                )
            }
            Self::PayloadBytes {
                observed_bytes,
                max_bytes,
            } => {
                format!(
                    "watch event fan-out batch is {observed_bytes} payload bytes; max is {max_bytes}"
                )
            }
        }
    }
}

pub fn validate_cluster_kv_watch_event_fanout_batch(
    events: &[KvEvent],
) -> Result<(), ClusterKvWatchEventFanoutBatchLimit> {
    validate_cluster_kv_watch_event_fanout_batch_with_limits(
        events,
        CLUSTER_KV_WATCH_EVENT_FANOUT_BATCH_EVENTS,
        CLUSTER_KV_WATCH_EVENT_FANOUT_BATCH_PAYLOAD_BYTES,
    )
}

pub fn validate_cluster_kv_watch_event_fanout_batch_with_limits(
    events: &[KvEvent],
    max_events: usize,
    max_payload_bytes: usize,
) -> Result<(), ClusterKvWatchEventFanoutBatchLimit> {
    if events.len() > max_events {
        return Err(ClusterKvWatchEventFanoutBatchLimit::EventCount {
            observed_events: events.len(),
            max_events,
        });
    }
    let payload_bytes = cluster_kv_watch_events_payload_bytes(events);
    if payload_bytes > max_payload_bytes {
        return Err(ClusterKvWatchEventFanoutBatchLimit::PayloadBytes {
            observed_bytes: payload_bytes,
            max_bytes: max_payload_bytes,
        });
    }
    Ok(())
}

pub fn cluster_kv_watch_event_fanout_batches(events: &[KvEvent]) -> Vec<&[KvEvent]> {
    cluster_kv_watch_event_fanout_batches_with_limits(
        events,
        CLUSTER_KV_WATCH_EVENT_FANOUT_BATCH_EVENTS,
        CLUSTER_KV_WATCH_EVENT_FANOUT_BATCH_PAYLOAD_BYTES,
    )
}

pub fn cluster_kv_watch_event_fanout_batches_with_limits(
    events: &[KvEvent],
    max_events: usize,
    max_payload_bytes: usize,
) -> Vec<&[KvEvent]> {
    let mut batches = Vec::new();
    let mut start = 0_usize;
    while start < events.len() {
        let mut end = start;
        let mut payload_bytes = 0_usize;
        while end < events.len() {
            if end - start >= max_events {
                break;
            }
            let event_bytes = cluster_kv_watch_event_payload_bytes(&events[end]);
            let next_payload_bytes = payload_bytes.saturating_add(event_bytes);
            if end > start && next_payload_bytes > max_payload_bytes {
                break;
            }
            payload_bytes = next_payload_bytes;
            end += 1;
        }
        if end == start {
            end += 1;
        }
        batches.push(&events[start..end]);
        start = end;
    }
    batches
}

pub fn compact_cluster_kv_watch_events(mut events: Vec<KvEvent>) -> Vec<KvEvent> {
    events.sort_by(|left, right| {
        (left.revision, left.key.as_str()).cmp(&(right.revision, right.key.as_str()))
    });
    events.dedup_by(|left, right| {
        left.revision == right.revision && left.key == right.key && left.kind == right.kind
    });
    if events.len() > MAX_CLUSTER_KV_WATCH_EVENTS {
        let mut split_at = events.len() - MAX_CLUSTER_KV_WATCH_EVENTS;
        let retained_revision = events[split_at].revision;
        while split_at > 0 && events[split_at - 1].revision == retained_revision {
            split_at -= 1;
        }
        events.split_off(split_at)
    } else {
        events
    }
}

pub fn compact_cluster_kv_watch_events_incremental(
    mut retained: Vec<KvEvent>,
    incoming: &[KvEvent],
) -> Vec<KvEvent> {
    for event in incoming {
        if cluster_kv_events_contain(&retained, event) {
            continue;
        }
        retained.push(event.clone());
        if retained.len() > MAX_CLUSTER_KV_WATCH_EVENT_LOG_RECORDS {
            retained = compact_cluster_kv_watch_events(retained);
        }
    }
    compact_cluster_kv_watch_events(retained)
}

pub fn cluster_kv_watch_event_append_should_checkpoint(
    incoming_events: usize,
    appended_events: usize,
    current_log_records: usize,
    current_log_bytes: u64,
    appended_log_bytes: u64,
) -> bool {
    incoming_events > MAX_CLUSTER_KV_WATCH_EVENTS
        || current_log_records.saturating_add(appended_events)
            >= MAX_CLUSTER_KV_WATCH_EVENT_LOG_RECORDS
        || current_log_bytes.saturating_add(appended_log_bytes)
            >= MAX_CLUSTER_KV_WATCH_EVENT_LOG_BYTES
}

pub fn cluster_kv_events_contain(events: &[KvEvent], event: &KvEvent) -> bool {
    events.iter().any(|candidate| {
        candidate.revision == event.revision
            && candidate.key == event.key
            && candidate.kind == event.kind
    })
}

pub fn cluster_kv_events_contain_all(existing: &[KvEvent], incoming: &[KvEvent]) -> bool {
    incoming
        .iter()
        .all(|event| cluster_kv_events_contain(existing, event))
}

pub fn cluster_raft_append_request_needs_watch_event_fanout(
    request: &ClusterRaftAppendRequest,
    events: &[KvEvent],
) -> bool {
    if events.is_empty() {
        return false;
    }
    let Some(snapshot) = request.snapshot.as_ref() else {
        return false;
    };
    !cluster_kv_events_contain_all(&snapshot.kv.events, events)
}

pub fn cluster_kv_snapshot_compare(snapshot: &KvSnapshot, compare: &KvCompare) -> bool {
    match compare {
        KvCompare::Exists { key } => snapshot.get(key).is_some(),
        KvCompare::Missing { key } => snapshot.get(key).is_none(),
        KvCompare::ValueEquals { key, value } => snapshot
            .get(key)
            .is_some_and(|entry| entry.value.as_slice() == value.as_slice()),
        KvCompare::VersionEquals { key, version } => snapshot
            .get(key)
            .is_some_and(|entry| entry.version == *version),
        KvCompare::CreateRevisionEquals { key, revision } => snapshot
            .get(key)
            .is_some_and(|entry| entry.create_revision == *revision),
        KvCompare::ModifyRevisionEquals { key, revision } => snapshot
            .get(key)
            .is_some_and(|entry| entry.modify_revision == *revision),
        KvCompare::LeaseEquals { key, lease_id } => snapshot
            .get(key)
            .is_some_and(|entry| entry.lease_id == *lease_id),
    }
}

pub fn read_cluster_kv_watch_registration_file(
    path: &Path,
    watch_id: WatchId,
) -> Result<ClusterKvWatchRegistration, KvError> {
    let Some(registration) = read_optional_object::<ClusterKvWatchRegistration>(path)? else {
        return Err(KvError::UnknownWatch { watch_id });
    };
    if registration.watch_id != watch_id {
        return Err(KvError::UnknownWatch { watch_id });
    }
    Ok(registration)
}

pub fn write_cluster_kv_watch_registration_file(
    path: &Path,
    registration: &ClusterKvWatchRegistration,
) -> Result<(), KvError> {
    write_object_file(path, registration)
}

pub fn write_cluster_kv_watch_registration_file_create_new(
    path: &Path,
    registration: &ClusterKvWatchRegistration,
) -> Result<bool, KvError> {
    let content =
        sinomo_object_serde::to_bytes(registration, KV_WAL_OBJECT_VERSION_ID, LengthMode::U32)
            .map_err(|source| KvError::Serialization {
                detail: source.to_string(),
            })?;
    write_object_file_bytes_create_new_serialized(path, &content)
}

pub fn remove_cluster_kv_watch_registration_file_if_exists(path: &Path) -> Result<bool, KvError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(KvError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        }),
    }
}

pub fn read_cluster_kv_watch_allocator_state(
    paths: &ClusterKvPaths,
) -> Result<ClusterKvWatchAllocatorState, KvError> {
    let state = read_optional_object::<ClusterKvWatchAllocatorState>(&paths.watches_next_id)?
        .unwrap_or_else(|| ClusterKvWatchAllocatorState::initial_for_missing_state(1));
    Ok(ClusterKvWatchAllocatorState {
        next_watch_id: state.next_watch_id.max(1),
        active_watch_count: state.active_watch_count,
        pruned_compacted_revision: state.pruned_compacted_revision,
    })
}

pub fn write_cluster_kv_watch_allocator_state(
    paths: &ClusterKvPaths,
    state: &ClusterKvWatchAllocatorState,
) -> Result<(), KvError> {
    write_object_file(&paths.watches_next_id, state)
}

pub fn lock_cluster_kv_watch_allocator(paths: &ClusterKvPaths) -> Result<File, KvError> {
    fs::create_dir_all(&paths.root).map_err(|source| KvError::Io {
        path: paths.root.display().to_string(),
        detail: source.to_string(),
    })?;
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&paths.watches_lock)
        .map_err(|source| KvError::Io {
            path: paths.watches_lock.display().to_string(),
            detail: source.to_string(),
        })?;
    fs2::FileExt::lock_exclusive(&file).map_err(|source| KvError::Io {
        path: paths.watches_lock.display().to_string(),
        detail: source.to_string(),
    })?;
    Ok(file)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvWatchScanRegistration {
    pub path: PathBuf,
    pub registration: ClusterKvWatchRegistration,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClusterKvWatchScanResult {
    pub scanned: bool,
    pub removed: usize,
    pub retained: usize,
    pub max_seen_id: u64,
    pub removed_paths: Vec<PathBuf>,
    pub retained_registrations: Vec<ClusterKvWatchScanRegistration>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvWatchRegistrationCreateReport {
    pub watch_path: PathBuf,
    pub registration: ClusterKvWatchRegistration,
    pub allocator: ClusterKvWatchAllocatorState,
    pub scan: Option<ClusterKvWatchScanResult>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClusterKvWatchDrainCursorAction {
    Unchanged,
    CacheOnly { durable_next_revision: KvRevision },
    Persist,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvWatchDrainRegistrationReport {
    pub registration: ClusterKvWatchRegistration,
    pub response: ClusterKvWatchDrainResponse,
    pub cursor_action: ClusterKvWatchDrainCursorAction,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClusterKvWatchDrainCursorPersistence {
    Persisted {
        registration: ClusterKvWatchRegistration,
    },
    CacheOnly {
        registration: ClusterKvWatchRegistration,
        durable_next_revision: KvRevision,
    },
    Unchanged {
        registration: ClusterKvWatchRegistration,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvWatchRegistrationRemovalReport {
    pub watch_path: PathBuf,
    pub removed: bool,
    pub allocator: Option<ClusterKvWatchAllocatorState>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClusterKvActiveWatchRegistrationReport {
    pub has_active: bool,
    pub allocator: Option<ClusterKvWatchAllocatorState>,
    pub scan: Option<ClusterKvWatchScanResult>,
}

pub fn create_cluster_kv_watch_registration(
    paths: &ClusterKvPaths,
    request: ClusterKvWatchPrefixRequest,
    compacted_revision: KvRevision,
    allocator_trusted: bool,
    max_active_watches: usize,
) -> Result<ClusterKvWatchRegistrationCreateReport, KvError> {
    if request.start_revision <= compacted_revision && compacted_revision != KvRevision::ZERO {
        return Err(KvError::Compacted {
            requested: request.start_revision,
            compacted: compacted_revision,
        });
    }
    fs::create_dir_all(&paths.watches).map_err(|source| KvError::Io {
        path: paths.watches.display().to_string(),
        detail: source.to_string(),
    })?;

    let _allocator_lock = lock_cluster_kv_watch_allocator(paths)?;
    let mut allocator = read_cluster_kv_watch_allocator_state(paths)?;
    let scan =
        if cluster_kv_watch_allocator_needs_scan(allocator_trusted, &allocator, compacted_revision)
        {
            let scan = scan_cluster_kv_watch_registrations(paths, compacted_revision)?;
            allocator = cluster_kv_watch_allocator_after_scan(allocator, &scan, compacted_revision);
            Some(scan)
        } else {
            None
        };

    let active_watch_count = allocator.active_watch_count.unwrap_or_default();
    if active_watch_count >= max_active_watches {
        return Err(KvError::WatchLimit {
            max: max_active_watches,
        });
    }

    let mut watch_id = WatchId(allocator.next_watch_id.max(1));
    loop {
        let registration = ClusterKvWatchRegistration {
            watch_id,
            prefix: request.prefix.clone(),
            next_revision: request.start_revision,
        };
        let watch_path = paths.watch_path(watch_id);
        if write_cluster_kv_watch_registration_file_create_new(&watch_path, &registration)? {
            allocator.next_watch_id = watch_id.0.saturating_add(1).max(1);
            allocator.active_watch_count = Some(active_watch_count.saturating_add(1));
            allocator.pruned_compacted_revision = Some(compacted_revision);
            return Ok(ClusterKvWatchRegistrationCreateReport {
                watch_path,
                registration,
                allocator,
                scan,
            });
        }
        if watch_id.0 == u64::MAX {
            return Err(KvError::WatchIdExhausted);
        }
        watch_id = WatchId(watch_id.0.saturating_add(1).max(1));
    }
}

pub fn cluster_kv_active_watch_registration_report(
    paths: &ClusterKvPaths,
    compacted_revision: KvRevision,
) -> Result<ClusterKvActiveWatchRegistrationReport, KvError> {
    if !paths.watches.exists() && !paths.watches_next_id.exists() {
        return Ok(ClusterKvActiveWatchRegistrationReport::default());
    }
    let allocator = read_cluster_kv_watch_allocator_state(paths)?;
    let scan = scan_cluster_kv_watch_registrations(paths, compacted_revision)?;
    let allocator = cluster_kv_watch_allocator_after_scan(allocator, &scan, compacted_revision);
    Ok(ClusterKvActiveWatchRegistrationReport {
        has_active: scan.retained > 0,
        allocator: (paths.watches.exists() || paths.watches_next_id.exists()).then_some(allocator),
        scan: Some(scan),
    })
}

pub fn drain_cluster_kv_watch_registration(
    mut registration: ClusterKvWatchRegistration,
    durable_next_revision: KvRevision,
    persisted_events: &[KvEvent],
    snapshot: &KvSnapshot,
    max_payload_bytes: usize,
    registration_file_unchanged: bool,
    registration_file_age: Option<Duration>,
) -> Result<ClusterKvWatchDrainRegistrationReport, KvError> {
    if registration.next_revision <= snapshot.compacted_revision
        && snapshot.compacted_revision != KvRevision::ZERO
    {
        return Err(KvError::Compacted {
            requested: registration.next_revision,
            compacted: snapshot.compacted_revision,
        });
    }
    let events = collect_cluster_kv_watch_drain_events(
        persisted_events,
        snapshot,
        &registration,
        max_payload_bytes,
    )?;
    let cursor_action = if let Some(last) = events.last() {
        registration.next_revision = KvRevision(last.revision.0.saturating_add(1));
        if cluster_kv_watch_registration_cursor_should_persist(
            events.len(),
            registration.next_revision,
            durable_next_revision,
            registration_file_unchanged,
            registration_file_age,
        ) {
            ClusterKvWatchDrainCursorAction::Persist
        } else {
            ClusterKvWatchDrainCursorAction::CacheOnly {
                durable_next_revision,
            }
        }
    } else {
        ClusterKvWatchDrainCursorAction::Unchanged
    };
    Ok(ClusterKvWatchDrainRegistrationReport {
        response: ClusterKvWatchDrainResponse {
            watch_id: registration.watch_id,
            events,
            next_revision: registration.next_revision,
        },
        registration,
        cursor_action,
    })
}

pub fn persist_cluster_kv_watch_drain_cursor(
    watch_path: &Path,
    report: &ClusterKvWatchDrainRegistrationReport,
) -> Result<ClusterKvWatchDrainCursorPersistence, KvError> {
    match report.cursor_action {
        ClusterKvWatchDrainCursorAction::Persist => {
            write_cluster_kv_watch_registration_file(watch_path, &report.registration)?;
            Ok(ClusterKvWatchDrainCursorPersistence::Persisted {
                registration: report.registration.clone(),
            })
        }
        ClusterKvWatchDrainCursorAction::CacheOnly {
            durable_next_revision,
        } => Ok(ClusterKvWatchDrainCursorPersistence::CacheOnly {
            registration: report.registration.clone(),
            durable_next_revision,
        }),
        ClusterKvWatchDrainCursorAction::Unchanged => {
            Ok(ClusterKvWatchDrainCursorPersistence::Unchanged {
                registration: report.registration.clone(),
            })
        }
    }
}

pub fn create_cluster_kv_watch_for_node(
    paths: &ClusterKvPaths,
    node_id: NodeId,
    request: ClusterKvWatchPrefixRequest,
) -> Result<ClusterKvWatchRegistration, KvError> {
    let snapshot = read_cluster_kv_current_snapshot_arc_for_node(paths, node_id)?;
    let report = create_cluster_kv_watch_registration(
        paths,
        request,
        snapshot.compacted_revision,
        false,
        MAX_CLUSTER_KV_ACTIVE_WATCHES,
    )?;
    if let Err(error) = write_cluster_kv_watch_allocator_state(paths, &report.allocator) {
        let _ = remove_cluster_kv_watch_registration_file_if_exists(&report.watch_path);
        return Err(error);
    }
    Ok(report.registration)
}

pub fn drain_cluster_kv_watch_for_node(
    paths: &ClusterKvPaths,
    node_id: NodeId,
    request: ClusterKvWatchDrainRequest,
) -> Result<ClusterKvWatchDrainResponse, KvError> {
    let wait_timeout = request.wait_timeout();
    if wait_timeout.is_zero() {
        return drain_cluster_kv_watch_once_for_node(paths, node_id, request.watch_id);
    }

    let Some(deadline) = Instant::now().checked_add(wait_timeout) else {
        return drain_cluster_kv_watch_once_for_node(paths, node_id, request.watch_id);
    };
    loop {
        let observed_generation = cluster_kv_watch_observed_generation(paths, request.watch_id);
        let response =
            drain_cluster_kv_watch_once_for_node(paths, node_id.clone(), request.watch_id)?;
        if !response.events.is_empty() {
            return Ok(response);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(response);
        }
        if !wait_for_cluster_kv_watch_change_for_watch(
            paths,
            request.watch_id,
            observed_generation,
            remaining,
        ) {
            return Ok(response);
        }
    }
}

pub(crate) fn drain_cluster_kv_watch_once_for_node(
    paths: &ClusterKvPaths,
    node_id: NodeId,
    watch_id: WatchId,
) -> Result<ClusterKvWatchDrainResponse, KvError> {
    let watch_path = paths.watch_path(watch_id);
    let registration = match read_cluster_kv_watch_registration_file(&watch_path, watch_id) {
        Ok(registration) => registration,
        Err(error @ KvError::UnknownWatch { .. }) => {
            let _ = remove_cluster_kv_watch_registration_file_if_exists(&watch_path);
            return Err(error);
        }
        Err(error) => return Err(error),
    };
    let durable_next_revision = registration.next_revision;
    let snapshot = read_cluster_kv_current_snapshot_arc_for_node(paths, node_id)?;
    let persisted_events = read_cluster_kv_watch_events(paths)?.events;
    let report = match drain_cluster_kv_watch_registration(
        registration,
        durable_next_revision,
        &persisted_events,
        snapshot.as_ref(),
        MAX_CLUSTER_KV_WATCH_DRAIN_PAYLOAD_BYTES,
        false,
        None,
    ) {
        Ok(report) => report,
        Err(error @ KvError::Compacted { .. }) => {
            let _ = cancel_cluster_kv_watch(paths, ClusterKvWatchCancelRequest { watch_id });
            return Err(error);
        }
        Err(error) => return Err(error),
    };
    let _ = persist_cluster_kv_watch_drain_cursor(&watch_path, &report)?;
    Ok(report.response)
}

pub fn cancel_cluster_kv_watch(
    paths: &ClusterKvPaths,
    request: ClusterKvWatchCancelRequest,
) -> Result<ClusterKvWatchCancelResponse, KvError> {
    let report = remove_cluster_kv_watch_registration_with_allocator(paths, request.watch_id)?;
    #[cfg(any(test, feature = "test-support"))]
    crate::watch_store::invalidate_cluster_kv_watch_registration_cache(&report.watch_path);
    if let Some(allocator) = report.allocator {
        write_cluster_kv_watch_allocator_state(paths, &allocator)?;
        #[cfg(any(test, feature = "test-support"))]
        crate::watch_store::record_current_cluster_kv_watch_allocator_trust(
            paths,
            allocator.active_watch_count,
        );
    }
    if report.removed {
        notify_cluster_kv_watchers(paths);
    }
    Ok(ClusterKvWatchCancelResponse {
        watch_id: request.watch_id,
        cancelled: report.removed,
    })
}

pub fn append_cluster_kv_watch_events_for_active_watches_for_node(
    paths: &ClusterKvPaths,
    node_id: NodeId,
    incoming: &[KvEvent],
) -> Result<usize, KvError> {
    if incoming.is_empty() || (!paths.watches.exists() && !paths.watches_next_id.exists()) {
        return Ok(0);
    }
    let snapshot = read_cluster_kv_current_snapshot_arc_for_node(paths, node_id)?;
    let active = cluster_kv_active_watch_registration_report(paths, snapshot.compacted_revision)?;
    if let Some(allocator) = active.allocator {
        write_cluster_kv_watch_allocator_state(paths, &allocator)?;
    }
    if !active.has_active {
        return Ok(0);
    }
    let registrations = if let Some(scan) = active.scan {
        let mut registrations = Vec::new();
        try_reserve_kv_items(
            &mut registrations,
            scan.retained_registrations.len(),
            "collect active watch registrations",
        )?;
        for retained in scan.retained_registrations {
            registrations.push(retained.registration);
        }
        registrations
    } else {
        list_cluster_kv_watch_registrations(paths)?
    };
    let incoming = cluster_kv_watch_events_matching_registrations(&registrations, incoming);
    if incoming.is_empty()
        || incoming
            .iter()
            .all(|event| cluster_kv_events_contain(&snapshot.events, event))
    {
        return Ok(0);
    }
    let report = append_cluster_kv_watch_events_to_storage(paths, &incoming, None)?;
    if !report.appended.is_empty() {
        notify_cluster_kv_watchers_for_events(paths, &report.appended);
    }
    Ok(report.appended.len())
}

pub fn remove_cluster_kv_watch_registration_with_allocator(
    paths: &ClusterKvPaths,
    watch_id: WatchId,
) -> Result<ClusterKvWatchRegistrationRemovalReport, KvError> {
    let watch_path = paths.watch_path(watch_id);
    if !watch_path.exists() {
        return Ok(ClusterKvWatchRegistrationRemovalReport {
            watch_path,
            removed: false,
            allocator: None,
        });
    }
    let _allocator_lock = lock_cluster_kv_watch_allocator(paths)?;
    let removed = remove_cluster_kv_watch_registration_file_if_exists(&watch_path)?;
    let allocator = if removed {
        let allocator = read_cluster_kv_watch_allocator_state(paths)?;
        allocator
            .active_watch_count
            .is_some()
            .then(|| cluster_kv_watch_allocator_after_registration_removed(allocator))
    } else {
        None
    };
    Ok(ClusterKvWatchRegistrationRemovalReport {
        watch_path,
        removed,
        allocator,
    })
}

pub fn scan_cluster_kv_watch_registrations(
    paths: &ClusterKvPaths,
    compacted_revision: KvRevision,
) -> Result<ClusterKvWatchScanResult, KvError> {
    let mut result = ClusterKvWatchScanResult::default();
    let entries = match fs::read_dir(&paths.watches) {
        Ok(entries) => entries,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(result),
        Err(source) => {
            return Err(KvError::Io {
                path: paths.watches.display().to_string(),
                detail: source.to_string(),
            });
        }
    };
    result.scanned = true;
    for entry in entries {
        let entry = entry.map_err(|source| KvError::Io {
            path: paths.watches.display().to_string(),
            detail: source.to_string(),
        })?;
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        let Some(file_watch_id) = cluster_kv_watch_id_from_file_name(&name) else {
            continue;
        };
        result.max_seen_id = result.max_seen_id.max(file_watch_id);
        let path = entry.path();
        let watch_id = WatchId(file_watch_id);
        match read_cluster_kv_watch_registration_file(&path, watch_id) {
            Ok(registration)
                if compacted_revision != KvRevision::ZERO
                    && registration.next_revision <= compacted_revision =>
            {
                let _ = remove_cluster_kv_watch_registration_file_if_exists(&path)?;
                result.removed = result.removed.saturating_add(1);
                result.removed_paths.push(path);
            }
            Ok(registration) => {
                result.retained = result.retained.saturating_add(1);
                result
                    .retained_registrations
                    .push(ClusterKvWatchScanRegistration { path, registration });
            }
            Err(KvError::UnknownWatch { watch_id: returned }) if returned == watch_id => {
                let _ = remove_cluster_kv_watch_registration_file_if_exists(&path)?;
                result.removed = result.removed.saturating_add(1);
                result.removed_paths.push(path);
            }
            Err(error) => {
                eprintln!(
                    "warning: removing unreadable cluster kv watch registration {}: {}",
                    path.display(),
                    error
                );
                let _ = remove_cluster_kv_watch_registration_file_if_exists(&path)?;
                result.removed = result.removed.saturating_add(1);
                result.removed_paths.push(path);
            }
        }
    }
    Ok(result)
}

pub fn list_cluster_kv_watch_registrations(
    paths: &ClusterKvPaths,
) -> Result<Vec<ClusterKvWatchRegistration>, KvError> {
    let entries = match fs::read_dir(&paths.watches) {
        Ok(entries) => entries,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => {
            return Err(KvError::Io {
                path: paths.watches.display().to_string(),
                detail: source.to_string(),
            });
        }
    };
    let mut registrations = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|source| KvError::Io {
            path: paths.watches.display().to_string(),
            detail: source.to_string(),
        })?;
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        let Some(file_watch_id) = cluster_kv_watch_id_from_file_name(&name) else {
            continue;
        };
        let watch_id = WatchId(file_watch_id);
        match read_cluster_kv_watch_registration_file(&entry.path(), watch_id) {
            Ok(registration) => registrations.push(registration),
            Err(KvError::UnknownWatch { watch_id: returned }) if returned == watch_id => {}
            Err(error) => {
                eprintln!(
                    "warning: ignoring unreadable cluster kv watch registration {} during fan-out filtering: {}",
                    entry.path().display(),
                    error
                );
            }
        }
    }
    Ok(registrations)
}

pub fn cluster_kv_watch_events_matching_registrations(
    registrations: &[ClusterKvWatchRegistration],
    incoming: &[KvEvent],
) -> Vec<KvEvent> {
    if registrations.is_empty() {
        return Vec::new();
    }
    incoming
        .iter()
        .filter(|event| {
            registrations.iter().any(|registration| {
                event.revision >= registration.next_revision
                    && event.key.starts_with(&registration.prefix)
            })
        })
        .cloned()
        .collect()
}

pub fn cluster_kv_watch_ids_matching_events(
    registrations: &[ClusterKvWatchRegistration],
    events: &[KvEvent],
) -> Vec<WatchId> {
    if events.is_empty() {
        return Vec::new();
    }
    let mut watch_ids = BTreeSet::new();
    for registration in registrations {
        if events.iter().any(|event| {
            event.revision >= registration.next_revision
                && event.key.starts_with(&registration.prefix)
        }) {
            watch_ids.insert(registration.watch_id);
        }
    }
    watch_ids.into_iter().collect()
}

pub fn cluster_kv_watch_id_from_file_name(name: &str) -> Option<u64> {
    name.strip_prefix("watch-")
        .and_then(|name| name.strip_suffix(".sobj"))
        .and_then(|id| id.parse::<u64>().ok())
}

pub fn cluster_kv_watch_allocator_needs_scan(
    allocator_trusted: bool,
    allocator: &ClusterKvWatchAllocatorState,
    compacted_revision: KvRevision,
) -> bool {
    if !allocator_trusted {
        return true;
    }
    if allocator.active_watch_count.is_none() {
        return true;
    }
    match allocator.pruned_compacted_revision {
        Some(pruned) => pruned < compacted_revision,
        None => true,
    }
}

pub fn cluster_kv_watch_allocator_after_scan(
    mut allocator: ClusterKvWatchAllocatorState,
    scan: &ClusterKvWatchScanResult,
    compacted_revision: KvRevision,
) -> ClusterKvWatchAllocatorState {
    allocator.next_watch_id = allocator
        .next_watch_id
        .max(scan.max_seen_id.saturating_add(1))
        .max(1);
    allocator.active_watch_count = Some(scan.retained);
    allocator.pruned_compacted_revision = Some(compacted_revision);
    allocator
}

pub fn cluster_kv_watch_allocator_after_registration_removed(
    mut allocator: ClusterKvWatchAllocatorState,
) -> ClusterKvWatchAllocatorState {
    if let Some(active_watch_count) = allocator.active_watch_count {
        allocator.active_watch_count = Some(active_watch_count.saturating_sub(1));
    }
    allocator
}

pub(crate) fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}
