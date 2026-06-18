use super::*;

pub(crate) const MAX_CLUSTER_KV_WATCH_NOTIFIER_ENTRIES: usize = MAX_ACTIVE_WATCHES * 2 + 128;
pub(crate) static CLUSTER_KV_WATCH_NOTIFIER_CLOCK: AtomicU64 = AtomicU64::new(1);
pub(crate) static CLUSTER_KV_WATCH_NOTIFIERS: LazyLock<
    Mutex<HashMap<PathBuf, ClusterKvWatchNotifierEntry>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
pub(crate) static CLUSTER_KV_WATCH_UNCHANGED_WAKEUPS_FOR_TEST: LazyLock<
    Mutex<HashMap<PathBuf, u64>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));

pub(crate) struct ClusterKvWatchNotifierEntry {
    pub(crate) generation: u64,
    pub(crate) last_used: u64,
    pub(crate) waiters: usize,
    pub(crate) changed: Arc<Condvar>,
}

impl Default for ClusterKvWatchNotifierEntry {
    fn default() -> Self {
        Self {
            generation: 0,
            last_used: 0,
            waiters: 0,
            changed: Arc::new(Condvar::new()),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterKvWatchObservedGeneration {
    pub root: u64,
    pub watch: u64,
}

pub(crate) fn cluster_kv_watch_notify_key(paths: &ClusterKvPaths) -> PathBuf {
    paths.root.clone()
}

pub(crate) fn cluster_kv_watch_notify_key_for_watch(
    paths: &ClusterKvPaths,
    watch_id: WatchId,
) -> PathBuf {
    paths.watch_path(watch_id)
}

pub(crate) fn cluster_kv_watch_notifier_tick() -> u64 {
    CLUSTER_KV_WATCH_NOTIFIER_CLOCK.fetch_add(1, Ordering::Relaxed)
}

pub(crate) fn evict_cluster_kv_watch_notifier_entry_if_needed(
    guard: &mut HashMap<PathBuf, ClusterKvWatchNotifierEntry>,
    protected: &PathBuf,
) {
    if guard.contains_key(protected) || guard.len() < MAX_CLUSTER_KV_WATCH_NOTIFIER_ENTRIES {
        return;
    }
    let eviction_candidate = guard
        .iter()
        .filter(|(path, entry)| *path != protected && entry.waiters == 0)
        .min_by_key(|(_, entry)| entry.last_used)
        .map(|(path, _)| path.clone());
    if let Some(path) = eviction_candidate {
        guard.remove(&path);
    }
}

pub(crate) fn ensure_cluster_kv_watch_notifier_entry<'a>(
    guard: &'a mut HashMap<PathBuf, ClusterKvWatchNotifierEntry>,
    key: PathBuf,
) -> &'a mut ClusterKvWatchNotifierEntry {
    evict_cluster_kv_watch_notifier_entry_if_needed(guard, &key);
    guard.entry(key).or_default()
}

pub(crate) fn finish_cluster_kv_watch_wait(
    guard: &mut HashMap<PathBuf, ClusterKvWatchNotifierEntry>,
    key: &PathBuf,
) {
    if let Some(entry) = guard.get_mut(key) {
        entry.waiters = entry.waiters.saturating_sub(1);
        entry.last_used = cluster_kv_watch_notifier_tick();
    }
}

pub fn cluster_kv_watch_generation(paths: &ClusterKvPaths) -> u64 {
    CLUSTER_KV_WATCH_NOTIFIERS
        .lock()
        .ok()
        .and_then(|guard| {
            guard
                .get(&cluster_kv_watch_notify_key(paths))
                .map(|entry| entry.generation)
        })
        .unwrap_or(0)
}

pub fn cluster_kv_watch_observed_generation(
    paths: &ClusterKvPaths,
    watch_id: WatchId,
) -> ClusterKvWatchObservedGeneration {
    let root_key = cluster_kv_watch_notify_key(paths);
    let watch_key = cluster_kv_watch_notify_key_for_watch(paths, watch_id);
    CLUSTER_KV_WATCH_NOTIFIERS
        .lock()
        .map(|guard| ClusterKvWatchObservedGeneration {
            root: guard
                .get(&root_key)
                .map(|entry| entry.generation)
                .unwrap_or(0),
            watch: guard
                .get(&watch_key)
                .map(|entry| entry.generation)
                .unwrap_or(0),
        })
        .unwrap_or(ClusterKvWatchObservedGeneration { root: 0, watch: 0 })
}

pub fn notify_cluster_kv_watchers(paths: &ClusterKvPaths) {
    let Ok(mut guard) = CLUSTER_KV_WATCH_NOTIFIERS.lock() else {
        return;
    };
    let key = cluster_kv_watch_notify_key(paths);
    let tick = cluster_kv_watch_notifier_tick();
    let mut changed = Vec::new();
    {
        let entry = ensure_cluster_kv_watch_notifier_entry(&mut guard, key.clone());
        entry.generation = entry.generation.saturating_add(1);
        entry.last_used = tick;
        changed.push(entry.changed.clone());
    }
    for (path, entry) in guard.iter_mut() {
        if path != &key && path.starts_with(&paths.root) && entry.waiters > 0 {
            entry.generation = entry.generation.saturating_add(1);
            entry.last_used = tick;
            changed.push(entry.changed.clone());
        }
    }
    drop(guard);
    for changed in changed {
        changed.notify_all();
    }
}

pub fn notify_cluster_kv_watchers_for_watch_ids(paths: &ClusterKvPaths, watch_ids: &[WatchId]) {
    if watch_ids.is_empty() {
        return;
    }
    let Ok(mut guard) = CLUSTER_KV_WATCH_NOTIFIERS.lock() else {
        return;
    };
    let tick = cluster_kv_watch_notifier_tick();
    let mut changed = Vec::new();
    for watch_id in watch_ids {
        let key = cluster_kv_watch_notify_key_for_watch(paths, *watch_id);
        let entry = ensure_cluster_kv_watch_notifier_entry(&mut guard, key);
        entry.generation = entry.generation.saturating_add(1);
        entry.last_used = tick;
        if entry.waiters > 0 {
            changed.push(entry.changed.clone());
        }
    }
    drop(guard);
    for changed in changed {
        changed.notify_all();
    }
}

pub fn notify_cluster_kv_watchers_for_events(paths: &ClusterKvPaths, events: &[KvEvent]) {
    if events.is_empty() {
        return;
    }
    match list_cluster_kv_watch_registrations(paths)
        .map(|registrations| cluster_kv_watch_ids_matching_events(&registrations, events))
    {
        Ok(watch_ids) => notify_cluster_kv_watchers_for_watch_ids(paths, &watch_ids),
        Err(error) => {
            eprintln!(
                "warning: waking all cluster kv watch waiters after active watch lookup failed for {}: {}",
                paths.watches.display(),
                error
            );
            notify_cluster_kv_watchers(paths);
        }
    }
}

pub(crate) fn cluster_kv_watch_observed_generation_changed(
    guard: &HashMap<PathBuf, ClusterKvWatchNotifierEntry>,
    root_key: &PathBuf,
    watch_key: &PathBuf,
    observed_generation: ClusterKvWatchObservedGeneration,
) -> bool {
    guard
        .get(root_key)
        .map(|entry| entry.generation)
        .unwrap_or(0)
        != observed_generation.root
        || guard
            .get(watch_key)
            .map(|entry| entry.generation)
            .unwrap_or(0)
            != observed_generation.watch
}

pub fn wait_for_cluster_kv_watch_change(
    paths: &ClusterKvPaths,
    observed_generation: u64,
    timeout: Duration,
) -> bool {
    if timeout.is_zero() {
        return false;
    }
    let Some(deadline) = Instant::now().checked_add(timeout) else {
        return false;
    };
    let key = cluster_kv_watch_notify_key(paths);
    let Ok(mut guard) = CLUSTER_KV_WATCH_NOTIFIERS.lock() else {
        return false;
    };
    let changed = {
        let tick = cluster_kv_watch_notifier_tick();
        let entry = ensure_cluster_kv_watch_notifier_entry(&mut guard, key.clone());
        entry.waiters = entry.waiters.saturating_add(1);
        entry.last_used = tick;
        entry.changed.clone()
    };
    loop {
        if guard.get(&key).map(|entry| entry.generation).unwrap_or(0) != observed_generation {
            finish_cluster_kv_watch_wait(&mut guard, &key);
            return true;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            finish_cluster_kv_watch_wait(&mut guard, &key);
            return false;
        }
        let (next_guard, wait) = match changed.wait_timeout(guard, remaining) {
            Ok(result) => result,
            Err(poisoned) => {
                let (mut poisoned_guard, _) = poisoned.into_inner();
                finish_cluster_kv_watch_wait(&mut poisoned_guard, &key);
                return false;
            }
        };
        guard = next_guard;
        if let Some(entry) = guard.get_mut(&key) {
            entry.last_used = cluster_kv_watch_notifier_tick();
        }
        if wait.timed_out() {
            let changed =
                guard.get(&key).map(|entry| entry.generation).unwrap_or(0) != observed_generation;
            finish_cluster_kv_watch_wait(&mut guard, &key);
            return changed;
        }
    }
}

pub fn wait_for_cluster_kv_watch_change_for_watch(
    paths: &ClusterKvPaths,
    watch_id: WatchId,
    observed_generation: ClusterKvWatchObservedGeneration,
    timeout: Duration,
) -> bool {
    if timeout.is_zero() {
        return false;
    }
    let Some(deadline) = Instant::now().checked_add(timeout) else {
        return false;
    };
    let root_key = cluster_kv_watch_notify_key(paths);
    let watch_key = cluster_kv_watch_notify_key_for_watch(paths, watch_id);
    let Ok(mut guard) = CLUSTER_KV_WATCH_NOTIFIERS.lock() else {
        return false;
    };
    let changed = {
        let tick = cluster_kv_watch_notifier_tick();
        let entry = ensure_cluster_kv_watch_notifier_entry(&mut guard, watch_key.clone());
        entry.waiters = entry.waiters.saturating_add(1);
        entry.last_used = tick;
        entry.changed.clone()
    };
    loop {
        if cluster_kv_watch_observed_generation_changed(
            &guard,
            &root_key,
            &watch_key,
            observed_generation,
        ) {
            finish_cluster_kv_watch_wait(&mut guard, &watch_key);
            return true;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            finish_cluster_kv_watch_wait(&mut guard, &watch_key);
            return false;
        }
        let (next_guard, wait) = match changed.wait_timeout(guard, remaining) {
            Ok(result) => result,
            Err(poisoned) => {
                let (mut poisoned_guard, _) = poisoned.into_inner();
                finish_cluster_kv_watch_wait(&mut poisoned_guard, &watch_key);
                return false;
            }
        };
        guard = next_guard;
        let tick = cluster_kv_watch_notifier_tick();
        if let Some(entry) = guard.get_mut(&watch_key) {
            entry.last_used = tick;
        }
        if !wait.timed_out()
            && !cluster_kv_watch_observed_generation_changed(
                &guard,
                &root_key,
                &watch_key,
                observed_generation,
            )
        {
            record_cluster_kv_watch_unchanged_wakeup_for_test(&watch_key);
        }
        if wait.timed_out() {
            let changed = cluster_kv_watch_observed_generation_changed(
                &guard,
                &root_key,
                &watch_key,
                observed_generation,
            );
            finish_cluster_kv_watch_wait(&mut guard, &watch_key);
            return changed;
        }
    }
}

pub fn cluster_kv_watch_notifier_capacity_for_test() -> usize {
    MAX_CLUSTER_KV_WATCH_NOTIFIER_ENTRIES
}

pub fn cluster_kv_watch_notifier_has_key_for_test(paths: &ClusterKvPaths) -> bool {
    let key = cluster_kv_watch_notify_key(paths);
    cluster_kv_watch_notifier_has_raw_key_for_test(&key)
}

pub fn cluster_kv_watch_notifier_has_raw_key_for_test(path: &Path) -> bool {
    CLUSTER_KV_WATCH_NOTIFIERS
        .lock()
        .map(|guard| guard.contains_key(path))
        .unwrap_or(false)
}

pub fn cluster_kv_watch_notifier_waiter_count_for_test(paths: &ClusterKvPaths) -> usize {
    let key = cluster_kv_watch_notify_key(paths);
    CLUSTER_KV_WATCH_NOTIFIERS
        .lock()
        .ok()
        .and_then(|guard| guard.get(&key).map(|entry| entry.waiters))
        .unwrap_or(0)
}

pub fn remove_cluster_kv_watch_notifier_entries_under_root_for_test(root: &Path) {
    if let Ok(mut guard) = CLUSTER_KV_WATCH_NOTIFIERS.lock() {
        guard.retain(|path, _| !path.starts_with(root));
    }
}

pub fn touch_cluster_kv_watch_notifier_for_test(paths: &ClusterKvPaths) {
    notify_cluster_kv_watchers(paths);
}

pub fn record_cluster_kv_watch_notifier_waiter_key_for_test(path: PathBuf) {
    if let Ok(mut guard) = CLUSTER_KV_WATCH_NOTIFIERS.lock() {
        evict_cluster_kv_watch_notifier_entry_if_needed(&mut guard, &path);
        guard.insert(
            path,
            ClusterKvWatchNotifierEntry {
                generation: 0,
                last_used: cluster_kv_watch_notifier_tick(),
                waiters: 1,
                changed: Arc::new(Condvar::new()),
            },
        );
    }
}

pub(crate) fn record_cluster_kv_watch_unchanged_wakeup_for_test(path: &PathBuf) {
    if let Ok(mut wakeups) = CLUSTER_KV_WATCH_UNCHANGED_WAKEUPS_FOR_TEST.lock() {
        *wakeups.entry(path.clone()).or_default() += 1;
    }
}

pub fn reset_cluster_kv_watch_unchanged_wakeups_for_test(
    paths: &ClusterKvPaths,
    watch_id: WatchId,
) {
    let key = cluster_kv_watch_notify_key_for_watch(paths, watch_id);
    if let Ok(mut wakeups) = CLUSTER_KV_WATCH_UNCHANGED_WAKEUPS_FOR_TEST.lock() {
        wakeups.remove(&key);
    }
}

pub fn cluster_kv_watch_unchanged_wakeups_for_test(
    paths: &ClusterKvPaths,
    watch_id: WatchId,
) -> u64 {
    let key = cluster_kv_watch_notify_key_for_watch(paths, watch_id);
    CLUSTER_KV_WATCH_UNCHANGED_WAKEUPS_FOR_TEST
        .lock()
        .ok()
        .and_then(|wakeups| wakeups.get(&key).copied())
        .unwrap_or(0)
}
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
    pub fn legacy(next_watch_id: u64) -> Self {
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
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
) -> Vec<KvEvent> {
    let mut events = persisted_events
        .iter()
        .chain(snapshot.events.iter())
        .filter(|event| {
            event.revision >= registration.next_revision
                && event.key.starts_with(&registration.prefix)
        })
        .collect::<Vec<_>>();
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
) -> Vec<KvEvent> {
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
    limited
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedClusterKvWatchEventLogBatch {
    pub records: Vec<Vec<u8>>,
    pub bytes: u64,
}

pub fn open_cluster_kv_watch_event_log_for_append(paths: &ClusterKvPaths) -> Result<File, KvError> {
    durable_file::open_file_for_append(&paths.watch_events_log)
        .map(|append| append.file)
        .map_err(kv_durable_file_error)
}

pub fn lock_cluster_kv_watch_event_log_for_append(
    paths: &ClusterKvPaths,
    file: &File,
) -> Result<(), KvError> {
    file.lock_exclusive().map_err(|source| KvError::Io {
        path: paths.watch_events_log.display().to_string(),
        detail: source.to_string(),
    })
}

pub fn encode_cluster_kv_watch_event_log_batch(
    events: &[KvEvent],
) -> Result<EncodedClusterKvWatchEventLogBatch, KvError> {
    let mut records = Vec::with_capacity(events.len());
    let mut bytes = 0_u64;
    for event in events {
        let record = sinomo_object_serde::to_bytes(event, KV_OBJECT_VERSION_ID, LengthMode::U32)
            .map_err(|source| KvError::Serialization {
                detail: source.to_string(),
            })?;
        let len = u32::try_from(record.len()).map_err(|_| KvError::Serialization {
            detail: "cluster kv watch-event log record exceeds u32 length".to_string(),
        })?;
        bytes = bytes.saturating_add(4).saturating_add(u64::from(len));
        records.push(record);
    }
    Ok(EncodedClusterKvWatchEventLogBatch { records, bytes })
}

pub fn append_cluster_kv_watch_event_log_batch_to_file(
    paths: &ClusterKvPaths,
    file: &mut File,
    batch: &EncodedClusterKvWatchEventLogBatch,
) -> Result<u64, KvError> {
    if batch.records.is_empty() {
        return Ok(0);
    }
    durable_file::append_length_prefixed_records(
        file,
        &paths.watch_events_log,
        batch.records.iter().map(Vec::as_slice),
        durable_file::DurableRecordAppendOptions::new(durable_file::DurableLengthEndian::Big)
            .sync(durable_file::DurableFileSync::Data),
    )
    .map_err(kv_durable_file_error)
}

pub fn truncate_cluster_kv_watch_event_log_file(
    paths: &ClusterKvPaths,
    file: &mut File,
) -> Result<(), KvError> {
    durable_file::truncate_open_file_and_sync(
        file,
        &paths.watch_events_log,
        0,
        durable_file::DurableFileSync::Data,
    )
    .map_err(kv_durable_file_error)
}

pub fn decode_cluster_kv_watch_event_log_record(record: &[u8]) -> Result<KvEvent, KvError> {
    sinomo_object_serde::from_bytes(record).map_err(|source| KvError::Serialization {
        detail: source.to_string(),
    })
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClusterKvWatchEventsRead {
    pub events: Vec<KvEvent>,
    pub log_records: usize,
    pub append_safe: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClusterKvWatchEventLogRead {
    pub events: Vec<KvEvent>,
    pub records: usize,
    pub truncated: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClusterKvWatchEventAppendCacheView {
    pub events: Vec<KvEvent>,
    pub log_records: usize,
    pub log_bytes: u64,
    pub append_safe: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClusterKvWatchEventAppendReport {
    pub appended: Vec<KvEvent>,
    pub retained: Vec<KvEvent>,
    pub log_records: usize,
    pub append_safe: bool,
}

pub fn read_cluster_kv_watch_events(
    paths: &ClusterKvPaths,
) -> Result<ClusterKvWatchEventsRead, KvError> {
    read_cluster_kv_watch_events_with_observer(paths, |_| {})
}

pub fn read_cluster_kv_watch_events_with_observer<F>(
    paths: &ClusterKvPaths,
    on_log_read: F,
) -> Result<ClusterKvWatchEventsRead, KvError>
where
    F: FnMut(u64),
{
    let checkpoint = read_cluster_kv_watch_event_checkpoint(paths)?;
    let log = read_cluster_kv_watch_event_log_with_observer(paths, on_log_read)?;
    let events =
        compact_cluster_kv_watch_events(checkpoint.into_iter().chain(log.events).collect());
    Ok(ClusterKvWatchEventsRead {
        events,
        log_records: log.records,
        append_safe: !log.truncated,
    })
}

pub fn read_cluster_kv_watch_event_checkpoint(
    paths: &ClusterKvPaths,
) -> Result<Vec<KvEvent>, KvError> {
    read_optional_object(&paths.watch_events).map(|events| events.unwrap_or_default())
}

pub fn write_cluster_kv_watch_event_checkpoint(
    paths: &ClusterKvPaths,
    events: &[KvEvent],
) -> Result<(), KvError> {
    let parent = paths
        .watch_events
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));
    fs::create_dir_all(parent).map_err(|source| KvError::Io {
        path: parent.display().to_string(),
        detail: source.to_string(),
    })?;
    write_object_file(&paths.watch_events, &events)
}

pub fn append_cluster_kv_watch_events_to_storage(
    paths: &ClusterKvPaths,
    incoming: &[KvEvent],
    cache: Option<&ClusterKvWatchEventAppendCacheView>,
) -> Result<ClusterKvWatchEventAppendReport, KvError> {
    append_cluster_kv_watch_events_to_storage_with_observers(paths, incoming, cache, |_| {}, |_| {})
}

pub fn append_cluster_kv_watch_events_to_storage_with_observers<R, W>(
    paths: &ClusterKvPaths,
    incoming: &[KvEvent],
    cache: Option<&ClusterKvWatchEventAppendCacheView>,
    mut on_log_read: R,
    mut on_log_write: W,
) -> Result<ClusterKvWatchEventAppendReport, KvError>
where
    R: FnMut(u64),
    W: FnMut(u64),
{
    if incoming.is_empty() {
        return Ok(ClusterKvWatchEventAppendReport::default());
    }

    let mut log_file = open_cluster_kv_watch_event_log_for_append(paths)?;
    lock_cluster_kv_watch_event_log_for_append(paths, &log_file)?;

    let (existing, log_records, log_bytes) =
        if let Some(cache) = cache.filter(|cache| cache.append_safe) {
            (cache.events.clone(), cache.log_records, cache.log_bytes)
        } else {
            let checkpoint = read_cluster_kv_watch_event_checkpoint(paths)?;
            let log = read_cluster_kv_watch_event_log_from_file_with_observer(
                paths,
                &mut log_file,
                &mut on_log_read,
            )?;
            let existing =
                compact_cluster_kv_watch_events(checkpoint.into_iter().chain(log.events).collect());
            if log.truncated {
                write_cluster_kv_watch_event_checkpoint(paths, &existing)?;
                truncate_cluster_kv_watch_event_log_file(paths, &mut log_file)?;
                (existing, 0, 0)
            } else {
                let log_bytes = log_file
                    .metadata()
                    .map(|metadata| metadata.len())
                    .unwrap_or(0);
                (existing, log.records, log_bytes)
            }
        };

    let retained = compact_cluster_kv_watch_events_incremental(existing.clone(), incoming);
    let appended = retained
        .iter()
        .filter(|event| !cluster_kv_events_contain(&existing, event))
        .cloned()
        .collect::<Vec<_>>();
    if appended.is_empty() {
        return Ok(ClusterKvWatchEventAppendReport {
            appended,
            retained,
            log_records,
            append_safe: true,
        });
    }

    let encoded_appended = encode_cluster_kv_watch_event_log_batch(&appended)?;
    let mut current_log_records = log_records;
    let should_checkpoint_without_append = cluster_kv_watch_event_append_should_checkpoint(
        incoming.len(),
        appended.len(),
        current_log_records,
        log_bytes,
        encoded_appended.bytes,
    );
    if should_checkpoint_without_append {
        write_cluster_kv_watch_event_checkpoint(paths, &retained)?;
        truncate_cluster_kv_watch_event_log_file(paths, &mut log_file)?;
        current_log_records = 0;
    } else {
        let written = append_cluster_kv_watch_event_log_batch_to_file(
            paths,
            &mut log_file,
            &encoded_appended,
        )?;
        if written > 0 {
            on_log_write(written);
        }
        current_log_records = current_log_records.saturating_add(appended.len());
        if log_file
            .metadata()
            .map(|metadata| metadata.len() >= MAX_CLUSTER_KV_WATCH_EVENT_LOG_BYTES)
            .unwrap_or(false)
        {
            write_cluster_kv_watch_event_checkpoint(paths, &retained)?;
            truncate_cluster_kv_watch_event_log_file(paths, &mut log_file)?;
            current_log_records = 0;
        }
    }

    Ok(ClusterKvWatchEventAppendReport {
        appended,
        retained,
        log_records: current_log_records,
        append_safe: true,
    })
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
        .unwrap_or_else(|| ClusterKvWatchAllocatorState::legacy(1));
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
    file.lock_exclusive().map_err(|source| KvError::Io {
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
    );
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
    if let Some(allocator) = report.allocator {
        write_cluster_kv_watch_allocator_state(paths, &allocator)?;
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
        scan.retained_registrations
            .into_iter()
            .map(|retained| retained.registration)
            .collect::<Vec<_>>()
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

pub fn read_cluster_kv_watch_event_log(
    paths: &ClusterKvPaths,
) -> Result<ClusterKvWatchEventLogRead, KvError> {
    read_cluster_kv_watch_event_log_with_observer(paths, |_| {})
}

pub fn read_cluster_kv_watch_event_log_with_observer<F>(
    paths: &ClusterKvPaths,
    on_log_read: F,
) -> Result<ClusterKvWatchEventLogRead, KvError>
where
    F: FnMut(u64),
{
    let mut file = match File::open(&paths.watch_events_log) {
        Ok(file) => file,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ClusterKvWatchEventLogRead::default());
        }
        Err(source) => {
            return Err(KvError::Io {
                path: paths.watch_events_log.display().to_string(),
                detail: source.to_string(),
            });
        }
    };
    file.lock_shared().map_err(|source| KvError::Io {
        path: paths.watch_events_log.display().to_string(),
        detail: source.to_string(),
    })?;
    read_cluster_kv_watch_event_log_from_file_with_observer(paths, &mut file, on_log_read)
}

pub fn read_cluster_kv_watch_event_log_from_file_with_observer<F>(
    paths: &ClusterKvPaths,
    file: &mut File,
    mut on_log_read: F,
) -> Result<ClusterKvWatchEventLogRead, KvError>
where
    F: FnMut(u64),
{
    file.seek(SeekFrom::Start(0))
        .map_err(|source| KvError::Io {
            path: paths.watch_events_log.display().to_string(),
            detail: source.to_string(),
        })?;
    let mut events = Vec::new();
    let mut record_number = 0_usize;
    let mut read_bytes = 0_u64;
    let mut needs_repair = false;
    loop {
        let next_record_number = record_number.saturating_add(1);
        let record_start = file.stream_position().map_err(|source| KvError::Io {
            path: paths.watch_events_log.display().to_string(),
            detail: source.to_string(),
        })?;
        let file_len = file
            .metadata()
            .map_err(|source| KvError::Io {
                path: paths.watch_events_log.display().to_string(),
                detail: source.to_string(),
            })?
            .len();
        let record = match durable_file::read_length_prefixed_record_with_observer(
            file,
            &paths.watch_events_log,
            durable_file::DurableRecordReadOptions::new(durable_file::DurableLengthEndian::Big)
                .max_record_bytes(MAX_CLUSTER_KV_WATCH_EVENT_RECORD_BYTES),
            |bytes| {
                read_bytes = read_bytes.saturating_add(bytes);
                on_log_read(bytes);
            },
        ) {
            Ok(Some(record)) => record,
            Ok(None) => {
                if record_start < file_len {
                    eprintln!(
                        "warning: ignoring partial cluster kv watch-event log record {} in {}",
                        next_record_number,
                        paths.watch_events_log.display()
                    );
                    return Ok(ClusterKvWatchEventLogRead {
                        events,
                        records: record_number,
                        truncated: true,
                    });
                }
                break;
            }
            Err(error) if error.detail().contains("length-prefixed record is") => {
                eprintln!(
                    "warning: ignoring corrupt cluster kv watch-event log record {} in {}",
                    next_record_number,
                    paths.watch_events_log.display()
                );
                return Ok(ClusterKvWatchEventLogRead {
                    events,
                    records: record_number,
                    truncated: true,
                });
            }
            Err(error) => return Err(kv_durable_file_error(error)),
        };
        record_number = next_record_number;
        if record.bytes.is_empty() {
            eprintln!(
                "warning: ignoring corrupt cluster kv watch-event log record {} in {}",
                record_number,
                paths.watch_events_log.display()
            );
            return Ok(ClusterKvWatchEventLogRead {
                events,
                records: record_number.saturating_sub(1),
                truncated: true,
            });
        }
        match decode_cluster_kv_watch_event_log_record(&record.bytes) {
            Ok(event) => events.push(event),
            Err(source) => {
                eprintln!(
                    "warning: ignoring unreadable cluster kv watch-event log record {} in {}: {}",
                    record_number,
                    paths.watch_events_log.display(),
                    source
                );
                return Ok(ClusterKvWatchEventLogRead {
                    events,
                    records: record_number.saturating_sub(1),
                    truncated: true,
                });
            }
        }
        if record_number >= MAX_CLUSTER_KV_WATCH_EVENT_LOG_RECORDS
            || read_bytes >= MAX_CLUSTER_KV_WATCH_EVENT_LOG_BYTES
        {
            needs_repair = true;
        }
        if events.len() > MAX_CLUSTER_KV_WATCH_EVENT_LOG_RECORDS {
            events = compact_cluster_kv_watch_events(events);
        }
    }
    if needs_repair {
        eprintln!(
            "warning: repairing overgrown cluster kv watch-event log {} after reading {} records / {} bytes",
            paths.watch_events_log.display(),
            record_number,
            read_bytes
        );
    }
    Ok(ClusterKvWatchEventLogRead {
        events,
        records: record_number,
        truncated: needs_repair,
    })
}

pub fn cluster_kv_watch_event_payload_bytes(event: &KvEvent) -> usize {
    let value_bytes = match &event.kind {
        KvEventKind::Put { value } => value.len(),
        KvEventKind::Delete => 0,
    };
    event.key.len().saturating_add(value_bytes)
}

pub(crate) fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}
