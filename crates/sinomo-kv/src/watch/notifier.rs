use super::*;

pub(crate) const MAX_CLUSTER_KV_WATCH_NOTIFIER_ENTRIES: usize = MAX_ACTIVE_WATCHES * 2 + 128;
pub(crate) static CLUSTER_KV_WATCH_NOTIFIER_CLOCK: AtomicU64 = AtomicU64::new(1);
pub(crate) static CLUSTER_KV_WATCH_NOTIFIERS: LazyLock<
    Mutex<HashMap<PathBuf, ClusterKvWatchNotifierEntry>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(any(test, feature = "test-support"))]
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

pub(crate) fn ensure_cluster_kv_watch_notifier_entry(
    guard: &mut HashMap<PathBuf, ClusterKvWatchNotifierEntry>,
    key: PathBuf,
) -> &mut ClusterKvWatchNotifierEntry {
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
        if entry.waiters > 0 && path != &key && path.starts_with(&paths.root) {
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
            #[cfg(any(test, feature = "test-support"))]
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

#[cfg(any(test, feature = "test-support"))]
pub fn cluster_kv_watch_notifier_capacity_for_test() -> usize {
    MAX_CLUSTER_KV_WATCH_NOTIFIER_ENTRIES
}

#[cfg(any(test, feature = "test-support"))]
pub fn cluster_kv_watch_notifier_has_key_for_test(paths: &ClusterKvPaths) -> bool {
    let key = cluster_kv_watch_notify_key(paths);
    cluster_kv_watch_notifier_has_raw_key_for_test(&key)
}

#[cfg(any(test, feature = "test-support"))]
pub fn cluster_kv_watch_notifier_has_raw_key_for_test(path: &Path) -> bool {
    CLUSTER_KV_WATCH_NOTIFIERS
        .lock()
        .map(|guard| guard.contains_key(path))
        .unwrap_or(false)
}

#[cfg(any(test, feature = "test-support"))]
pub fn cluster_kv_watch_notifier_waiter_count_for_test(paths: &ClusterKvPaths) -> usize {
    let key = cluster_kv_watch_notify_key(paths);
    CLUSTER_KV_WATCH_NOTIFIERS
        .lock()
        .ok()
        .and_then(|guard| guard.get(&key).map(|entry| entry.waiters))
        .unwrap_or(0)
}

#[cfg(any(test, feature = "test-support"))]
pub fn remove_cluster_kv_watch_notifier_entries_under_root_for_test(root: &Path) {
    if let Ok(mut guard) = CLUSTER_KV_WATCH_NOTIFIERS.lock() {
        guard.retain(|path, _| !path.starts_with(root));
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn touch_cluster_kv_watch_notifier_for_test(paths: &ClusterKvPaths) {
    notify_cluster_kv_watchers(paths);
}

#[cfg(any(test, feature = "test-support"))]
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

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn record_cluster_kv_watch_unchanged_wakeup_for_test(path: &Path) {
    if let Ok(mut wakeups) = CLUSTER_KV_WATCH_UNCHANGED_WAKEUPS_FOR_TEST.lock() {
        *wakeups.entry(path.to_path_buf()).or_default() += 1;
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn reset_cluster_kv_watch_unchanged_wakeups_for_test(
    paths: &ClusterKvPaths,
    watch_id: WatchId,
) {
    let key = cluster_kv_watch_notify_key_for_watch(paths, watch_id);
    if let Ok(mut wakeups) = CLUSTER_KV_WATCH_UNCHANGED_WAKEUPS_FOR_TEST.lock() {
        wakeups.remove(&key);
    }
}

#[cfg(any(test, feature = "test-support"))]
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
