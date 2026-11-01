use crate::internal::*;

#[cfg(test)]
static EVENTS_AFTER_REVISION_PARTITION_PROBES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

#[cfg(any(test, feature = "test-support"))]
thread_local! {
    static COMPLETE_SNAPSHOT_MATERIALIZATIONS: std::cell::Cell<u64> = const {
        std::cell::Cell::new(0)
    };
}
#[cfg(test)]
thread_local! {
    static IN_PROCESS_STORE_CLONES: std::cell::Cell<u64> = const {
        std::cell::Cell::new(0)
    };
}

#[cfg(test)]
pub(crate) fn reset_events_after_revision_partition_probes_for_test() {
    EVENTS_AFTER_REVISION_PARTITION_PROBES.store(0, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(test)]
pub(crate) fn events_after_revision_partition_probes_for_test() -> u64 {
    EVENTS_AFTER_REVISION_PARTITION_PROBES.load(std::sync::atomic::Ordering::Relaxed)
}

#[cfg(test)]
pub(crate) fn reset_complete_snapshot_materializations_for_test() {
    COMPLETE_SNAPSHOT_MATERIALIZATIONS.with(|count| count.set(0));
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn complete_snapshot_materializations_for_test() -> u64 {
    COMPLETE_SNAPSHOT_MATERIALIZATIONS.with(std::cell::Cell::get)
}

#[cfg(test)]
pub(crate) fn reset_in_process_store_clones_for_test() {
    IN_PROCESS_STORE_CLONES.with(|count| count.set(0));
}

#[cfg(test)]
pub(crate) fn in_process_store_clones_for_test() -> u64 {
    IN_PROCESS_STORE_CLONES.with(std::cell::Cell::get)
}

#[cfg(any(test, feature = "test-support"))]
fn record_complete_snapshot_materialization_for_test() {
    COMPLETE_SNAPSHOT_MATERIALIZATIONS.with(|count| count.set(count.get().saturating_add(1)));
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct SimulatedKeyState {
    pub(crate) present: bool,
    pub(crate) lease_id: Option<LeaseId>,
    pub(crate) create_revision: KvRevision,
    pub(crate) modify_revision: KvRevision,
    pub(crate) version: KvVersion,
}

impl SimulatedKeyState {
    const ABSENT: Self = Self {
        present: false,
        lease_id: None,
        create_revision: KvRevision::ZERO,
        modify_revision: KvRevision::ZERO,
        version: KvVersion(0),
    };

    fn from_entry(entry: &KvEntry) -> Self {
        Self {
            present: true,
            lease_id: entry.lease_id,
            create_revision: entry.create_revision,
            modify_revision: entry.modify_revision,
            version: entry.version,
        }
    }

    fn after_put(previous: Self, lease_id: Option<LeaseId>, revision: KvRevision) -> Self {
        Self {
            present: true,
            lease_id,
            create_revision: if previous.present {
                previous.create_revision
            } else {
                revision
            },
            modify_revision: revision,
            version: if previous.present {
                KvVersion(previous.version.0.saturating_add(1))
            } else {
                KvVersion(1)
            },
        }
    }
}

#[derive(Debug, Default)]
struct SimulatedLeaseSet {
    leases: BTreeSet<LeaseId>,
}

impl SimulatedLeaseSet {
    fn from_leases(leases: &KvLeaseIndex) -> Result<Self, KvError> {
        let active = leases.keys().copied().collect();
        Ok(Self { leases: active })
    }

    fn len(&self) -> usize {
        self.leases.len()
    }

    fn contains(&self, lease_id: &LeaseId) -> bool {
        self.leases.contains(lease_id)
    }

    fn try_insert(&mut self, lease_id: LeaseId) -> Result<(), KvError> {
        self.leases.insert(lease_id);
        Ok(())
    }

    fn remove(&mut self, lease_id: &LeaseId) {
        self.leases.remove(lease_id);
    }
}

#[derive(Debug, Default)]
struct SimulatedKeyStateIndex {
    entries: BTreeMap<String, SimulatedKeyState>,
    exact_values: BTreeMap<String, Vec<u8>>,
    keys_by_lease: BTreeMap<LeaseId, BTreeSet<String>>,
}

impl SimulatedKeyStateIndex {
    fn get(&self, key: &str) -> Option<SimulatedKeyState> {
        self.entries.get(key).copied()
    }

    fn exact_value(&self, key: &str) -> Option<&[u8]> {
        self.exact_values.get(key).map(Vec::as_slice)
    }

    fn try_insert(
        &mut self,
        key: &str,
        state: SimulatedKeyState,
        operation: &str,
    ) -> Result<(), KvError> {
        let key = try_clone_kv_string(key, operation)?;
        self.exact_values.remove(&key);
        if let Some(previous) = self.entries.insert(key.clone(), state)
            && let Some(lease_id) = previous.lease_id
            && let Some(keys) = self.keys_by_lease.get_mut(&lease_id)
        {
            keys.remove(&key);
            if keys.is_empty() {
                self.keys_by_lease.remove(&lease_id);
            }
        }
        if state.present
            && let Some(lease_id) = state.lease_id
        {
            self.keys_by_lease.entry(lease_id).or_default().insert(key);
        }
        Ok(())
    }

    fn try_insert_with_value(
        &mut self,
        key: &str,
        state: SimulatedKeyState,
        value: &[u8],
        operation: &str,
    ) -> Result<(), KvError> {
        let exact_key = try_clone_kv_string(key, operation)?;
        let exact_value = try_clone_kv_bytes(value, operation)?;
        self.try_insert(key, state, operation)?;
        self.exact_values.insert(exact_key, exact_value);
        Ok(())
    }

    fn iter(&self) -> impl Iterator<Item = (&str, &SimulatedKeyState)> {
        self.entries
            .iter()
            .map(|(key, state)| (key.as_str(), state))
    }

    fn remove_prefix(&mut self, prefix: &str) -> Result<(), KvError> {
        let start = try_clone_kv_string(prefix, "scan simulated touched-key prefix")?;
        let mut keys = Vec::new();
        for (key, _) in self
            .entries
            .range(start..)
            .take_while(|(key, _)| key.starts_with(prefix))
        {
            try_reserve_kv_items(&mut keys, 1, "collect simulated touched-key prefix")?;
            keys.push(try_clone_kv_string(
                key,
                "collect simulated touched-key prefix",
            )?);
        }
        for key in keys {
            self.remove(&key);
        }
        Ok(())
    }

    fn expire_lease(&mut self, lease_id: LeaseId) {
        let Some(keys) = self.keys_by_lease.remove(&lease_id) else {
            return;
        };
        for key in keys {
            if let Some(state) = self.entries.get_mut(&key) {
                *state = SimulatedKeyState::ABSENT;
            }
            self.exact_values.remove(&key);
        }
    }

    fn remove(&mut self, key: &str) {
        self.exact_values.remove(key);
        let Some(previous) = self.entries.remove(key) else {
            return;
        };
        let Some(lease_id) = previous.lease_id else {
            return;
        };
        let Some(keys) = self.keys_by_lease.get_mut(&lease_id) else {
            return;
        };
        keys.remove(key);
        if keys.is_empty() {
            self.keys_by_lease.remove(&lease_id);
        }
    }
}

#[derive(Debug, Default)]
struct SimulatedDeletedPrefixIndex {
    prefixes: BTreeSet<String>,
}

impl SimulatedDeletedPrefixIndex {
    fn contains_prefix_of(&self, key: &str) -> bool {
        self.prefixes
            .range::<str, _>((std::ops::Bound::Unbounded, std::ops::Bound::Included(key)))
            .next_back()
            .is_some_and(|prefix| key.starts_with(prefix))
    }

    fn try_insert(&mut self, prefix: &str) -> Result<(), KvError> {
        if self.contains_prefix_of(prefix) {
            return Ok(());
        }
        let start = try_clone_kv_string(prefix, "scan simulated deleted prefixes")?;
        let mut descendants = Vec::new();
        for descendant in self
            .prefixes
            .range(start..)
            .take_while(|candidate| candidate.starts_with(prefix))
        {
            try_reserve_kv_items(
                &mut descendants,
                1,
                "collect redundant simulated deleted prefixes",
            )?;
            descendants.push(try_clone_kv_string(
                descendant,
                "collect redundant simulated deleted prefixes",
            )?);
        }
        for descendant in descendants {
            self.prefixes.remove(&descendant);
        }
        self.prefixes.insert(try_clone_kv_string(
            prefix,
            "track simulated deleted prefix",
        )?);
        Ok(())
    }
}

#[derive(Debug, Default)]
struct SimulatedDeleteSummary {
    total: usize,
    keys_by_lease: BTreeMap<LeaseId, usize>,
}

impl SimulatedDeleteSummary {
    fn record(&mut self, state: SimulatedKeyState) {
        self.total = self.total.saturating_add(1);
        if let Some(lease_id) = state.lease_id {
            *self.keys_by_lease.entry(lease_id).or_default() += 1;
        }
    }
}

fn update_simulated_lease_key_counts(
    counts: &mut BTreeMap<LeaseId, usize>,
    old_lease_id: Option<LeaseId>,
    new_lease_id: Option<LeaseId>,
) {
    if old_lease_id == new_lease_id {
        return;
    }
    if let Some(lease_id) = old_lease_id {
        let count = counts.entry(lease_id).or_default();
        *count = count.saturating_sub(1);
    }
    if let Some(lease_id) = new_lease_id {
        let count = counts.entry(lease_id).or_default();
        *count = count.saturating_add(1);
    }
}

#[derive(Debug)]
struct KvPreflightOverlay {
    active_leases: SimulatedLeaseSet,
    touched_keys: SimulatedKeyStateIndex,
    deleted_prefixes: SimulatedDeletedPrefixIndex,
    revoked_leases: BTreeSet<LeaseId>,
    lease_key_counts: BTreeMap<LeaseId, usize>,
    revision: KvRevision,
}

impl KvPreflightOverlay {
    fn from_store(store: &InProcessKvStore) -> Result<Self, KvError> {
        Ok(Self {
            active_leases: SimulatedLeaseSet::from_leases(&store.leases)?,
            touched_keys: SimulatedKeyStateIndex::default(),
            deleted_prefixes: SimulatedDeletedPrefixIndex::default(),
            revoked_leases: BTreeSet::new(),
            lease_key_counts: store
                .lease_keys
                .keys_by_lease
                .iter()
                .map(|(lease_id, keys)| (*lease_id, keys.len()))
                .collect(),
            revision: store.revision,
        })
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct KvEntryIndex {
    entries: BTreeMap<String, KvEntry>,
}

impl KvEntryIndex {
    fn len(&self) -> usize {
        self.entries.len()
    }

    fn values(&self) -> impl Iterator<Item = &KvEntry> {
        self.entries.values()
    }

    fn get(&self, key: &str) -> Option<&KvEntry> {
        self.entries.get(key)
    }

    fn contains_key(&self, key: &str) -> bool {
        self.entries.contains_key(key)
    }

    fn remove(&mut self, key: &str) -> Option<KvEntry> {
        self.entries.remove(key)
    }

    fn try_insert(&mut self, entry: KvEntry, operation: &str) -> Result<Option<KvEntry>, KvError> {
        let key = try_clone_kv_string(&entry.key, operation)?;
        Ok(self.entries.insert(key, entry))
    }

    fn prefix_entries<'a>(&'a self, prefix: &'a str) -> impl Iterator<Item = &'a KvEntry> + 'a {
        self.entries
            .range(prefix.to_owned()..)
            .map_while(move |(_, entry)| entry.key.starts_with(prefix).then_some(entry))
    }

    fn first_prefix_entry(&self, prefix: &str) -> Option<&KvEntry> {
        self.entries
            .range(prefix.to_owned()..)
            .next()
            .and_then(|(_, entry)| entry.key.starts_with(prefix).then_some(entry))
    }

    fn into_values(self) -> impl Iterator<Item = KvEntry> {
        self.entries.into_values()
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct KvLeaseIndex {
    leases: BTreeMap<LeaseId, KvLease>,
}

impl KvLeaseIndex {
    fn len(&self) -> usize {
        self.leases.len()
    }

    pub(crate) fn contains_key(&self, lease_id: &LeaseId) -> bool {
        self.leases.contains_key(lease_id)
    }

    fn get(&self, lease_id: &LeaseId) -> Option<&KvLease> {
        self.leases.get(lease_id)
    }

    fn get_mut(&mut self, lease_id: &LeaseId) -> Option<&mut KvLease> {
        self.leases.get_mut(lease_id)
    }

    fn keys(&self) -> impl Iterator<Item = &LeaseId> {
        self.leases.keys()
    }

    pub(crate) fn values(&self) -> impl Iterator<Item = &KvLease> {
        self.leases.values()
    }

    fn try_reserve_insert(&mut self, lease_id: LeaseId, operation: &str) -> Result<(), KvError> {
        let _ = (lease_id, operation);
        Ok(())
    }

    fn insert_reserved(&mut self, lease: KvLease) -> Option<KvLease> {
        self.leases.insert(lease.lease_id, lease)
    }

    fn try_insert(&mut self, lease: KvLease, operation: &str) -> Result<Option<KvLease>, KvError> {
        self.try_reserve_insert(lease.lease_id, operation)?;
        Ok(self.insert_reserved(lease))
    }

    fn remove(&mut self, lease_id: &LeaseId) -> Option<KvLease> {
        self.leases.remove(lease_id)
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct KvLeaseDeadlineByIdIndex {
    deadlines: BTreeMap<LeaseId, u64>,
}

impl KvLeaseDeadlineByIdIndex {
    fn get(&self, lease_id: &LeaseId) -> Option<&u64> {
        self.deadlines.get(lease_id)
    }

    fn try_reserve_insert(&mut self, lease_id: LeaseId, operation: &str) -> Result<(), KvError> {
        let _ = (lease_id, operation);
        Ok(())
    }

    fn insert_reserved(&mut self, lease_id: LeaseId, deadline: u64) -> Option<u64> {
        self.deadlines.insert(lease_id, deadline)
    }

    pub(crate) fn try_insert(
        &mut self,
        lease_id: LeaseId,
        deadline: u64,
        operation: &str,
    ) -> Result<Option<u64>, KvError> {
        self.try_reserve_insert(lease_id, operation)?;
        Ok(self.insert_reserved(lease_id, deadline))
    }

    fn remove(&mut self, lease_id: &LeaseId) -> Option<u64> {
        self.deadlines.remove(lease_id)
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct KvLeaseDeadlineIndex {
    deadlines: BTreeMap<u64, BTreeSet<LeaseId>>,
}

impl KvLeaseDeadlineIndex {
    fn try_reserve_insert(
        &mut self,
        deadline: u64,
        lease_id: LeaseId,
        operation: &str,
    ) -> Result<(), KvError> {
        let _ = (deadline, lease_id, operation);
        Ok(())
    }

    fn insert_reserved(&mut self, deadline: u64, lease_id: LeaseId) {
        self.deadlines.entry(deadline).or_default().insert(lease_id);
    }

    pub(crate) fn try_insert(
        &mut self,
        deadline: u64,
        lease_id: LeaseId,
        operation: &str,
    ) -> Result<(), KvError> {
        self.try_reserve_insert(deadline, lease_id, operation)?;
        self.insert_reserved(deadline, lease_id);
        Ok(())
    }

    fn remove(&mut self, deadline: u64, lease_id: LeaseId) -> bool {
        let Some(lease_ids) = self.deadlines.get_mut(&deadline) else {
            return false;
        };
        let removed = lease_ids.remove(&lease_id);
        if lease_ids.is_empty() {
            self.deadlines.remove(&deadline);
        }
        removed
    }

    fn push_expired_candidates(
        &self,
        lease_clock: u64,
        candidates: &mut Vec<LeaseId>,
    ) -> Result<(), KvError> {
        for (_, lease_ids) in self.deadlines.range(..=lease_clock) {
            for lease_id in lease_ids {
                try_reserve_kv_items(candidates, 1, "collect expired lease candidates")?;
                candidates.push(*lease_id);
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct KvLeaseKeyIndex {
    keys_by_lease: BTreeMap<LeaseId, BTreeSet<String>>,
}

impl KvLeaseKeyIndex {
    #[allow(dead_code)]
    pub(crate) fn contains_key(&self, lease_id: &LeaseId) -> bool {
        self.keys_by_lease.contains_key(lease_id)
    }

    pub(crate) fn get(&self, lease_id: &LeaseId) -> Option<&BTreeSet<String>> {
        self.keys_by_lease.get(lease_id)
    }

    fn remove(&mut self, lease_id: &LeaseId) -> Option<BTreeSet<String>> {
        self.keys_by_lease.remove(lease_id)
    }

    fn try_reserve_add_key(
        &mut self,
        lease_id: LeaseId,
        key: &str,
        operation: &str,
    ) -> Result<(), KvError> {
        let _ = (lease_id, key, operation);
        Ok(())
    }

    fn add_key_reserved(&mut self, lease_id: LeaseId, key: String) -> Result<(), KvError> {
        self.keys_by_lease.entry(lease_id).or_default().insert(key);
        Ok(())
    }

    pub(crate) fn try_add_key(
        &mut self,
        lease_id: LeaseId,
        key: String,
        operation: &str,
    ) -> Result<(), KvError> {
        self.try_reserve_add_key(lease_id, &key, operation)?;
        self.add_key_reserved(lease_id, key)
    }

    fn remove_key(&mut self, lease_id: LeaseId, key: &str) {
        let Some(keys) = self.keys_by_lease.get_mut(&lease_id) else {
            return;
        };
        keys.remove(key);
        if keys.is_empty() {
            self.keys_by_lease.remove(&lease_id);
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct KvWatchIndex {
    watches: BTreeMap<WatchId, KvWatchState>,
}

impl KvWatchIndex {
    fn len(&self) -> usize {
        self.watches.len()
    }

    pub(crate) fn get(&self, watch_id: &WatchId) -> Option<&KvWatchState> {
        self.watches.get(watch_id)
    }

    fn get_mut(&mut self, watch_id: &WatchId) -> Option<&mut KvWatchState> {
        self.watches.get_mut(watch_id)
    }

    fn try_insert(
        &mut self,
        watch_id: WatchId,
        watch: KvWatchState,
        operation: &str,
    ) -> Result<Option<KvWatchState>, KvError> {
        let _ = operation;
        Ok(self.watches.insert(watch_id, watch))
    }

    fn remove(&mut self, watch_id: &WatchId) -> Option<KvWatchState> {
        self.watches.remove(watch_id)
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct KvWatchPrefixIndex {
    watch_ids_by_prefix: BTreeMap<String, BTreeSet<WatchId>>,
}

impl KvWatchPrefixIndex {
    fn get(&self, prefix: &str) -> Option<&BTreeSet<WatchId>> {
        self.watch_ids_by_prefix.get(prefix)
    }

    fn try_add(&mut self, watch_id: WatchId, prefix: &str, operation: &str) -> Result<(), KvError> {
        self.watch_ids_by_prefix
            .entry(try_clone_kv_string(prefix, operation)?)
            .or_default()
            .insert(watch_id);
        Ok(())
    }

    fn remove(&mut self, watch_id: WatchId, prefix: &str) {
        let Some(watch_ids) = self.watch_ids_by_prefix.get_mut(prefix) else {
            return;
        };
        watch_ids.remove(&watch_id);
        if watch_ids.is_empty() {
            self.watch_ids_by_prefix.remove(prefix);
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct KvCommittedRequestEntry {
    // Retained records are immutable, so this one-time measurement remains exact
    // until the entry is evicted.
    record: KvCommittedRequestRecord,
    encoded_bytes: usize,
}

#[derive(Debug, Default)]
pub struct InProcessKvStore {
    pub(crate) revision: KvRevision,
    pub(crate) compacted_revision: KvRevision,
    pub(crate) entries: KvEntryIndex,
    pub(crate) events: Vec<Arc<KvEvent>>,
    pub(crate) leases: KvLeaseIndex,
    pub(crate) lease_clock: u64,
    pub(crate) lease_deadline_by_id: KvLeaseDeadlineByIdIndex,
    pub(crate) lease_deadlines: KvLeaseDeadlineIndex,
    pub(crate) lease_keys: KvLeaseKeyIndex,
    pub(crate) watches: KvWatchIndex,
    pub(crate) watch_prefixes: KvWatchPrefixIndex,
    pub(crate) next_watch_id: u64,
    pub(crate) committed_requests: BTreeMap<KvRequestId, KvCommittedRequestEntry>,
    // Ordered separately from the request-id lookup so steady-state eviction
    // removes the oldest committed record in O(log n), not by scanning the
    // entire retained ledger on every insert.
    pub(crate) committed_request_order: BTreeSet<(u64, KvRequestId)>,
    // Exact sum of `KvCommittedRequestEntry::encoded_bytes` for the map above.
    pub(crate) committed_request_encoded_bytes: usize,
    pub(crate) expired_request_ids: BTreeSet<KvRequestId>,
    pub(crate) expired_request_order: VecDeque<KvRequestId>,
}

impl Clone for InProcessKvStore {
    fn clone(&self) -> Self {
        #[cfg(test)]
        IN_PROCESS_STORE_CLONES.with(|count| count.set(count.get().saturating_add(1)));
        Self {
            revision: self.revision,
            compacted_revision: self.compacted_revision,
            entries: self.entries.clone(),
            events: self.events.clone(),
            leases: self.leases.clone(),
            lease_clock: self.lease_clock,
            lease_deadline_by_id: self.lease_deadline_by_id.clone(),
            lease_deadlines: self.lease_deadlines.clone(),
            lease_keys: self.lease_keys.clone(),
            watches: self.watches.clone(),
            watch_prefixes: self.watch_prefixes.clone(),
            next_watch_id: self.next_watch_id,
            committed_requests: self.committed_requests.clone(),
            committed_request_order: self.committed_request_order.clone(),
            committed_request_encoded_bytes: self.committed_request_encoded_bytes,
            expired_request_ids: self.expired_request_ids.clone(),
            expired_request_order: self.expired_request_order.clone(),
        }
    }
}

impl InProcessKvStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn revision(&self) -> KvRevision {
        self.revision
    }

    /// The existing read-model generation, not a consensus or authority index.
    /// The maintained request-order index contains exactly the retained memo.
    pub(crate) fn materialized_generation(&self) -> (KvRevision, u64) {
        let retained_request_index = self
            .committed_request_order
            .iter()
            .next_back()
            .map_or(0, |(log_index, _)| *log_index);
        (self.revision, retained_request_index)
    }

    pub fn compacted_revision(&self) -> KvRevision {
        self.compacted_revision
    }

    pub fn committed_request(&self, request_id: &KvRequestId) -> Option<&KvCommittedRequestRecord> {
        self.committed_requests
            .get(request_id)
            .map(|entry| &entry.record)
    }

    pub fn request_id_expired(&self, request_id: &KvRequestId) -> bool {
        self.expired_request_ids.contains(request_id)
    }

    pub(crate) fn record_committed_request(
        &mut self,
        record: KvCommittedRequestRecord,
    ) -> Result<(), KvError> {
        if self.request_id_expired(&record.request_id) {
            return Err(KvError::RequestExpired {
                request_id: record.request_id,
            });
        }
        if let Some(existing) = self.committed_requests.get(&record.request_id) {
            if existing.record.command_digest != record.command_digest {
                return Err(KvError::RequestIdConflict {
                    request_id: record.request_id,
                });
            }
            return Ok(());
        }
        let encoded_bytes = encode_kv_value(&record)?.len();
        let committed_request_encoded_bytes = self
            .committed_request_encoded_bytes
            .checked_add(encoded_bytes)
            .ok_or_else(|| KvError::ResourceExhausted {
                operation: "measure committed KV request records".to_string(),
                requested_bytes: usize::MAX,
            })?;
        let request_id = record.request_id.clone();
        let log_index = record.log_index;
        self.committed_requests.insert(
            request_id.clone(),
            KvCommittedRequestEntry {
                record,
                encoded_bytes,
            },
        );
        self.committed_request_order.insert((log_index, request_id));
        self.committed_request_encoded_bytes = committed_request_encoded_bytes;
        while self.committed_requests.len() > MAX_COMMITTED_KV_REQUESTS
            || self.committed_request_encoded_bytes > MAX_COMMITTED_KV_REQUEST_BYTES
        {
            let Some((_, expired_id)) = self.committed_request_order.pop_first() else {
                break;
            };
            let expired = self
                .committed_requests
                .remove(&expired_id)
                .expect("selected committed KV request must remain present");
            self.committed_request_encoded_bytes = self
                .committed_request_encoded_bytes
                .checked_sub(expired.encoded_bytes)
                .expect("committed KV request byte accounting must not underflow");
            if self.expired_request_ids.insert(expired_id.clone()) {
                self.expired_request_order.push_back(expired_id);
            }
            while self.expired_request_order.len() > MAX_EXPIRED_KV_REQUEST_TOMBSTONES {
                if let Some(expired_id) = self.expired_request_order.pop_front() {
                    self.expired_request_ids.remove(&expired_id);
                }
            }
        }
        Ok(())
    }

    pub fn event_history_len(&self) -> usize {
        self.events.len()
    }

    pub fn get(&self, key: &str) -> Option<&KvEntry> {
        self.entries.get(key)
    }

    pub fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
        let mut entries = Vec::new();
        for entry in self.prefix_entries(prefix) {
            try_reserve_kv_items(&mut entries, 1, "collect kv prefix range")?;
            entries.push(try_clone_kv_entry(entry, "collect kv prefix range")?);
        }
        Ok(entries)
    }

    pub fn prefix_entries<'a>(&'a self, prefix: &'a str) -> impl Iterator<Item = &'a KvEntry> + 'a {
        self.entries.prefix_entries(prefix)
    }

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
            try_reserve_kv_items(&mut entries, 1, "collect kv prefix range")?;
            entries.push(try_clone_kv_entry(entry, "collect kv prefix range")?);
        }
        Ok(entries)
    }

    pub fn put(
        &mut self,
        key: impl Into<String>,
        value: Vec<u8>,
        lease_id: Option<LeaseId>,
    ) -> Result<KvRevision, KvError> {
        self.apply_op(KvOp::Put {
            key: key.into(),
            value,
            lease_id,
        })
        .map(|response| response.revision())
    }

    pub fn put_serde<T: Serialize + ?Sized>(
        &mut self,
        key: impl Into<String>,
        value: &T,
        lease_id: Option<LeaseId>,
    ) -> Result<KvRevision, KvError> {
        self.apply_op(KvOp::put_serde(key, value, lease_id)?)
            .map(|response| response.revision())
    }

    pub fn get_serde<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, KvError> {
        self.get(key).map(KvEntry::decode).transpose()
    }

    pub fn range_prefix_serde<T: DeserializeOwned>(&self, prefix: &str) -> Result<Vec<T>, KvError> {
        let mut values = Vec::new();
        for entry in self.prefix_entries(prefix) {
            try_reserve_kv_items(&mut values, 1, "decode kv prefix range")?;
            values.push(entry.decode()?);
        }
        Ok(values)
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
        try_reserve_kv_items(&mut values, entries.len(), "decode limited kv prefix range")?;
        for entry in entries {
            values.push(entry.decode()?);
        }
        Ok(values)
    }

    pub fn delete(&mut self, key: impl Into<String>) -> Result<bool, KvError> {
        match self.apply_op(KvOp::Delete { key: key.into() })? {
            KvOpResponse::Delete { deleted, .. } => Ok(deleted),
            _ => unreachable!("delete response"),
        }
    }

    pub fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
        self.validate_txn_shape(&txn)?;
        validate_kv_txn_payload(&txn)?;
        let succeeded = txn.compares.iter().all(|compare| self.compare(compare));
        let ops = if succeeded { txn.success } else { txn.failure };
        self.validate_ops(&ops)?;
        let mut responses = Vec::new();
        let requested_bytes = ops
            .len()
            .saturating_mul(std::mem::size_of::<KvOpResponse>().max(1));
        responses
            .try_reserve(ops.len())
            .map_err(|_| KvError::ResourceExhausted {
                operation: "kv transaction responses".to_string(),
                requested_bytes,
            })?;
        let mut revision = self.revision;

        for op in ops {
            if let Some(response) = self.no_effect_response_for_op(&op) {
                responses.push(response);
                continue;
            }
            let response = self.apply_op(op)?;
            revision = self.revision;
            responses.push(response);
        }
        Ok(KvTxnResponse {
            succeeded,
            revision,
            responses,
        })
    }

    pub fn txn_response_if_selected_branch_empty(
        &self,
        txn: &KvTxn,
    ) -> Result<Option<KvTxnResponse>, KvError> {
        self.validate_txn_shape(txn)?;
        validate_kv_txn_payload(txn)?;
        let succeeded = txn.compares.iter().all(|compare| self.compare(compare));
        let selected_branch_is_empty = if succeeded {
            txn.success.is_empty()
        } else {
            txn.failure.is_empty()
        };
        if !selected_branch_is_empty {
            return Ok(None);
        }
        Ok(Some(KvTxnResponse {
            succeeded,
            revision: self.revision,
            responses: Vec::new(),
        }))
    }

    pub fn txn_response_if_selected_branch_no_effect(
        &self,
        txn: &KvTxn,
    ) -> Result<Option<KvTxnResponse>, KvError> {
        self.validate_txn_shape(txn)?;
        validate_kv_txn_payload(txn)?;
        let succeeded = txn.compares.iter().all(|compare| self.compare(compare));
        let ops = if succeeded {
            txn.success.as_slice()
        } else {
            txn.failure.as_slice()
        };
        self.validate_ops(ops)?;

        let mut responses = Vec::new();
        let requested_bytes = ops
            .len()
            .saturating_mul(std::mem::size_of::<KvOpResponse>().max(1));
        responses
            .try_reserve(ops.len())
            .map_err(|_| KvError::ResourceExhausted {
                operation: "kv no-effect transaction responses".to_string(),
                requested_bytes,
            })?;
        for op in ops {
            let Some(response) = self.no_effect_response_for_op(op) else {
                return Ok(None);
            };
            responses.push(response);
        }
        Ok(Some(KvTxnResponse {
            succeeded,
            revision: self.revision,
            responses,
        }))
    }

    pub fn op_response_if_no_effect(&self, op: &KvOp) -> Result<Option<KvOpResponse>, KvError> {
        validate_kv_op_payload(op)?;
        validate_kv_payload_total(kv_op_payload_bytes(op))?;
        self.validate_ops(std::slice::from_ref(op))?;
        Ok(self.no_effect_response_for_op(op))
    }

    pub fn apply_command(&mut self, command: KvCommand) -> Result<KvRevision, KvError> {
        self.apply_command_with_response(command)
            .map(|(revision, _)| revision)
    }

    pub fn apply_command_with_response(
        &mut self,
        command: KvCommand,
    ) -> Result<(KvRevision, Option<KvTxnResponse>), KvError> {
        validate_kv_command_payload(&command)?;
        match command {
            KvCommand::Op(op) => {
                // A fresh logical request still receives its own committed
                // Raft record, but replaying the state-machine operation must
                // preserve no-effect semantics: no revision bump or event.
                if self.no_effect_response_for_op(&op).is_none() {
                    self.apply_op(op)?;
                }
                Ok((self.revision, None))
            }
            KvCommand::Txn(txn) => {
                let response = self.txn(txn)?;
                Ok((response.revision, Some(response)))
            }
            KvCommand::Barrier => Ok((self.revision, None)),
        }
    }

    pub(crate) fn preflight_apply_command(&self, command: &KvCommand) -> Result<(), KvError> {
        validate_kv_command_payload(command)?;
        match command {
            KvCommand::Op(op) => self.validate_ops(std::slice::from_ref(op)),
            KvCommand::Txn(txn) => {
                self.validate_txn_shape(txn)?;
                validate_kv_txn_payload(txn)?;
                let succeeded = txn.compares.iter().all(|compare| self.compare(compare));
                let ops = if succeeded {
                    txn.success.as_slice()
                } else {
                    txn.failure.as_slice()
                };
                self.validate_ops(ops)
            }
            KvCommand::Barrier => Ok(()),
        }
    }

    pub fn watch_prefix(
        &mut self,
        prefix: impl Into<String>,
        start_revision: KvRevision,
    ) -> Result<WatchId, KvError> {
        if start_revision <= self.compacted_revision && self.compacted_revision != KvRevision::ZERO
        {
            return Err(KvError::Compacted {
                requested: start_revision,
                compacted: self.compacted_revision,
            });
        }
        if self.watches.len() >= MAX_ACTIVE_WATCHES {
            return Err(KvError::WatchLimit {
                max: MAX_ACTIVE_WATCHES,
            });
        }
        if self.next_watch_id == u64::MAX {
            return Err(KvError::WatchIdExhausted);
        }

        let watch_id = WatchId(self.next_watch_id.max(1));
        self.next_watch_id = watch_id.0.saturating_add(1);
        let prefix = prefix.into();
        let prefix_for_index = try_clone_kv_string(&prefix, "register kv watch prefix")?;
        let mut backlog = Vec::new();
        for event in self
            .events
            .iter()
            .filter(|event| event.revision >= start_revision && event.key.starts_with(&prefix))
        {
            try_reserve_kv_items(&mut backlog, 1, "collect kv watch backlog")?;
            backlog.push(Arc::clone(event));
        }
        let mut watch = KvWatchState {
            watch: KvWatch {
                watch_id,
                prefix,
                start_revision,
            },
            pending: backlog.into(),
            compacted_revision: None,
        };
        compact_watch_pending(&mut watch);
        self.index_watch_prefix(watch_id, &prefix_for_index)?;
        if let Err(error) = self
            .watches
            .try_insert(watch_id, watch, "register kv watch")
        {
            self.remove_watch_prefix_index(watch_id, &prefix_for_index);
            return Err(error);
        }
        Ok(watch_id)
    }

    pub fn drain_watch(&mut self, watch_id: WatchId) -> Result<Vec<KvEvent>, KvError> {
        if let Some(compacted) = self
            .watches
            .get(&watch_id)
            .and_then(|watch| watch.compacted_revision)
        {
            let requested = self
                .remove_watch(watch_id)
                .map(|watch| watch.watch.start_revision)
                .unwrap_or(KvRevision::ZERO);
            return Err(KvError::Compacted {
                requested,
                compacted,
            });
        }
        let watch = self
            .watches
            .get_mut(&watch_id)
            .ok_or(KvError::UnknownWatch { watch_id })?;
        let mut drained = Vec::new();
        try_reserve_kv_items(&mut drained, watch.pending.len(), "drain kv watch events")?;
        for event in watch.pending.drain(..) {
            drained.push(try_clone_kv_event(&event, "drain kv watch events")?);
        }
        Ok(drained)
    }

    pub fn cancel_watch(&mut self, watch_id: WatchId) -> Result<bool, KvError> {
        Ok(self.remove_watch(watch_id).is_some())
    }

    pub fn snapshot(&self) -> Result<KvSnapshot, KvError> {
        self.try_snapshot()
    }

    pub(crate) fn events_after_revision(
        &self,
        revision: KvRevision,
    ) -> Result<Vec<KvEvent>, KvError> {
        // Retained events are revision ordered. Locate the first new event in
        // logarithmic time instead of filtering the full retained history on
        // every resident-node checkin.
        let first_new = self.events.partition_point(|event| {
            #[cfg(test)]
            EVENTS_AFTER_REVISION_PARTITION_PROBES
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            event.revision <= revision
        });
        let new_events = &self.events[first_new..];
        metrics::histogram!("sinomo.kv.commit.incremental_events", "kind" => "retained")
            .record(self.events.len() as f64);
        metrics::histogram!("sinomo.kv.commit.incremental_events", "kind" => "selected")
            .record(new_events.len() as f64);
        let mut events = Vec::new();
        try_reserve_kv_items(
            &mut events,
            new_events.len(),
            "collect incremental committed KV events",
        )?;
        for event in new_events {
            events.push(try_clone_kv_event(
                event,
                "collect incremental committed KV events",
            )?);
        }
        Ok(events)
    }

    pub(crate) fn retained_events(&self) -> impl Iterator<Item = &KvEvent> {
        self.events.iter().map(AsRef::as_ref)
    }

    /// Existing thread-local observation for sibling native integration tests.
    /// This counts full materializations on this thread and grants no KV access.
    #[cfg(feature = "test-support")]
    pub fn complete_snapshot_materializations_for_test() -> u64 {
        complete_snapshot_materializations_for_test()
    }

    /// Materializes the complete wire/persistence snapshot.
    ///
    /// This is deliberately an explicit `O(total retained state)` boundary:
    /// every entry, event, lease, and committed-request record becomes owned by
    /// the returned snapshot. Point and prefix reads must use the indexed store
    /// APIs instead. If cluster KV grows beyond its control-plane role, replace
    /// this owned representation with a persistent/COW snapshot view before
    /// putting this method on a per-operation path.
    pub fn try_snapshot(&self) -> Result<KvSnapshot, KvError> {
        #[cfg(any(test, feature = "test-support"))]
        record_complete_snapshot_materialization_for_test();
        metrics::histogram!("sinomo.kv.snapshot.materialize.entries")
            .record(self.entries.len() as f64);
        metrics::histogram!("sinomo.kv.snapshot.materialize.events")
            .record(self.events.len() as f64);
        metrics::histogram!("sinomo.kv.snapshot.materialize.committed_requests")
            .record(self.committed_requests.len() as f64);
        let mut entries = Vec::new();
        try_reserve_kv_items(
            &mut entries,
            self.entries.len(),
            "build kv snapshot entries",
        )?;
        for entry in self.entries.values() {
            entries.push(try_clone_kv_entry(entry, "build kv snapshot entries")?);
        }

        let mut events = Vec::new();
        try_reserve_kv_items(&mut events, self.events.len(), "build kv snapshot events")?;
        for event in &self.events {
            events.push(try_clone_kv_event(event, "build kv snapshot events")?);
        }

        Ok(KvSnapshot {
            revision: self.revision,
            compacted_revision: self.compacted_revision,
            entries,
            leases: self.try_current_leases_for_snapshot()?,
            events,
            committed_requests: self
                .committed_requests
                .values()
                .map(|entry| entry.record.clone())
                .collect(),
            expired_requests: self.expired_request_order.iter().cloned().collect(),
        })
    }

    /// Consumes a store and transfers its owned state into a snapshot.
    #[allow(dead_code)]
    pub(crate) fn try_into_snapshot(self) -> Result<KvSnapshot, KvError> {
        #[cfg(any(test, feature = "test-support"))]
        record_complete_snapshot_materialization_for_test();
        let leases = self.try_current_leases_for_snapshot()?;
        let Self {
            revision,
            compacted_revision,
            entries,
            events: shared_events,
            committed_requests,
            expired_request_order,
            ..
        } = self;

        let mut snapshot_entries = Vec::new();
        try_reserve_kv_items(
            &mut snapshot_entries,
            entries.len(),
            "move kv snapshot entries",
        )?;
        snapshot_entries.extend(entries.into_values());
        let mut events = Vec::new();
        try_reserve_kv_items(&mut events, shared_events.len(), "move kv snapshot events")?;
        for event in shared_events {
            events.push(match Arc::try_unwrap(event) {
                Ok(event) => event,
                Err(event) => try_clone_kv_event(&event, "move shared kv snapshot event")?,
            });
        }
        let mut snapshot_committed_requests = Vec::new();
        try_reserve_kv_items(
            &mut snapshot_committed_requests,
            committed_requests.len(),
            "move kv snapshot committed requests",
        )?;
        snapshot_committed_requests
            .extend(committed_requests.into_values().map(|entry| entry.record));
        let mut expired_requests = Vec::new();
        try_reserve_kv_items(
            &mut expired_requests,
            expired_request_order.len(),
            "move kv snapshot expired requests",
        )?;
        expired_requests.extend(expired_request_order);
        Ok(KvSnapshot {
            revision,
            compacted_revision,
            entries: snapshot_entries,
            leases,
            events,
            committed_requests: snapshot_committed_requests,
            expired_requests,
        })
    }

    pub fn restore(snapshot: KvSnapshot) -> Result<Self, KvError> {
        let committed_request_encoded_sizes =
            snapshot.validate_resource_bounds_with_committed_request_sizes()?;
        let KvSnapshot {
            revision,
            compacted_revision,
            entries: snapshot_entries,
            leases: snapshot_leases,
            events: snapshot_events,
            committed_requests: snapshot_committed_requests,
            expired_requests: snapshot_expired_requests,
        } = snapshot;
        let mut entries = KvEntryIndex::default();
        for entry in snapshot_entries {
            entries.try_insert(entry, "restore kv snapshot entries")?;
        }
        let lease_keys = build_lease_key_index(entries.values())?;
        let mut leases = KvLeaseIndex::default();
        for lease in snapshot_leases {
            leases.try_insert(lease, "restore kv snapshot leases")?;
        }
        let (lease_deadline_by_id, lease_deadlines) = build_lease_deadline_index(&leases, 0)?;
        let mut events = Vec::new();
        try_reserve_kv_items(
            &mut events,
            snapshot_events.len(),
            "restore kv snapshot events",
        )?;
        for event in snapshot_events {
            events.push(Arc::new(event));
        }
        let mut committed_requests: BTreeMap<KvRequestId, KvCommittedRequestEntry> =
            BTreeMap::new();
        let mut committed_request_order = BTreeSet::new();
        let mut committed_request_encoded_bytes = 0_usize;
        for (record, encoded_bytes) in snapshot_committed_requests
            .into_iter()
            .zip(committed_request_encoded_sizes)
        {
            if let Some(existing) = committed_requests.get(&record.request_id)
                && existing.record != record
            {
                return Err(KvError::RequestIdConflict {
                    request_id: record.request_id,
                });
            }
            if committed_requests.contains_key(&record.request_id) {
                continue;
            }
            committed_request_encoded_bytes = committed_request_encoded_bytes
                .checked_add(encoded_bytes)
                .ok_or_else(|| KvError::ResourceExhausted {
                    operation: "restore committed KV request byte accounting".to_string(),
                    requested_bytes: usize::MAX,
                })?;
            committed_request_order.insert((record.log_index, record.request_id.clone()));
            committed_requests.insert(
                record.request_id.clone(),
                KvCommittedRequestEntry {
                    record,
                    encoded_bytes,
                },
            );
        }
        let mut expired_request_order = VecDeque::new();
        let mut expired_request_ids = BTreeSet::new();
        for request_id in snapshot_expired_requests {
            if expired_request_ids.insert(request_id.clone()) {
                expired_request_order.push_back(request_id);
            }
        }
        Ok(Self {
            revision,
            compacted_revision,
            entries,
            events,
            leases,
            lease_clock: 0,
            lease_deadline_by_id,
            lease_deadlines,
            lease_keys,
            watches: KvWatchIndex::default(),
            watch_prefixes: KvWatchPrefixIndex::default(),
            next_watch_id: 1,
            committed_requests,
            committed_request_order,
            committed_request_encoded_bytes,
            expired_request_ids,
            expired_request_order,
        })
    }

    pub fn compact(&mut self, revision: KvRevision) -> Result<(), KvError> {
        if revision > self.revision {
            return Err(KvError::RevisionAhead {
                requested: revision,
                current: self.revision,
            });
        }
        if revision > self.compacted_revision {
            self.compacted_revision = revision;
            self.events.retain(|event| event.revision > revision);
        }
        Ok(())
    }

    pub fn compact_event_history_to(&mut self, max_events: usize) {
        compact_shared_event_history(&mut self.events, &mut self.compacted_revision, max_events);
    }

    pub fn tick_leases(&mut self, ticks: u64) -> Result<Vec<LeaseId>, KvError> {
        self.lease_clock = self.lease_clock.saturating_add(ticks);
        let candidates = self.expired_lease_candidates()?;

        let mut expired = Vec::new();
        let mut mutation_events = 0_usize;
        for lease_id in candidates {
            if let Some(lease) = self.leases.get_mut(&lease_id) {
                lease.remaining_ticks = 0;
            } else {
                self.remove_lease_deadline(lease_id);
                continue;
            }
            let remaining_budget =
                MAX_KV_MUTATION_EVENTS_PER_COMMAND.saturating_sub(mutation_events);
            let (keys, complete) = self.lease_revoke_batch(lease_id, remaining_budget)?;
            if keys.is_empty() && !complete {
                break;
            }
            mutation_events = mutation_events.saturating_add(keys.len());
            if complete {
                self.remove_lease_and_delete_keys(lease_id, keys)?;
                expired.push(lease_id);
            } else {
                self.delete_lease_keys(keys)?;
                break;
            }
        }

        Ok(expired)
    }

    pub(crate) fn apply_op(&mut self, op: KvOp) -> Result<KvOpResponse, KvError> {
        match op {
            KvOp::Put {
                key,
                value,
                lease_id,
            } => {
                // This is also an apply-time invariant: old WAL entries can
                // reach the state machine without passing today's proposal
                // preflight.
                validate_cluster_kv_raft_config_is_unleased(&key, lease_id)?;
                self.apply_put(key, value, lease_id)
            }
            KvOp::Delete { key } => self.apply_delete(key),
            KvOp::DeletePrefix { prefix } => self.apply_delete_prefix(prefix),
            KvOp::LeaseGrant {
                lease_id,
                ttl_ticks,
            } => {
                if ttl_ticks == 0 {
                    return Err(KvError::InvalidLeaseTtl { ttl_ticks });
                }
                if !self.leases.contains_key(&lease_id) && self.leases.len() >= MAX_ACTIVE_LEASES {
                    return Err(KvError::LeaseLimit {
                        max: MAX_ACTIVE_LEASES,
                    });
                }
                self.upsert_lease(KvLease::new(lease_id, ttl_ticks))?;
                Ok(KvOpResponse::LeaseGrant { lease_id })
            }
            KvOp::LeaseKeepAlive { lease_id } => {
                let ttl_ticks = self
                    .leases
                    .get(&lease_id)
                    .ok_or(KvError::UnknownLease { lease_id })?
                    .ttl_ticks;
                self.upsert_lease(KvLease::new(lease_id, ttl_ticks))?;
                Ok(KvOpResponse::LeaseKeepAlive { lease_id })
            }
            KvOp::LeaseRevoke { lease_id } => {
                let keys =
                    self.keys_for_lease_limited(lease_id, MAX_KV_MUTATION_EVENTS_PER_COMMAND)?;
                self.remove_lease_and_delete_keys(lease_id, keys)
            }
        }
    }

    fn validate_txn_shape(&self, txn: &KvTxn) -> Result<(), KvError> {
        if txn.compares.len() > MAX_KV_TXN_COMPARES
            || txn.success.len() > MAX_KV_TXN_OPS_PER_BRANCH
            || txn.failure.len() > MAX_KV_TXN_OPS_PER_BRANCH
        {
            return Err(KvError::TxnLimit {
                compares: txn.compares.len(),
                success_ops: txn.success.len(),
                failure_ops: txn.failure.len(),
                max_compares: MAX_KV_TXN_COMPARES,
                max_ops_per_branch: MAX_KV_TXN_OPS_PER_BRANCH,
            });
        }
        Ok(())
    }

    fn validate_ops(&self, ops: &[KvOp]) -> Result<(), KvError> {
        let mut overlay = KvPreflightOverlay::from_store(self)?;
        self.validate_ops_with_overlay(ops, &mut overlay)
    }

    fn validate_ops_with_overlay(
        &self,
        ops: &[KvOp],
        overlay: &mut KvPreflightOverlay,
    ) -> Result<(), KvError> {
        let KvPreflightOverlay {
            active_leases,
            touched_keys,
            deleted_prefixes,
            revoked_leases,
            lease_key_counts,
            revision,
        } = overlay;
        let mut mutation_events = 0_usize;
        for op in ops {
            validate_kv_op_payload(op)?;
            if let KvOp::Put { key, lease_id, .. } = op {
                validate_cluster_kv_raft_config_is_unleased(key, *lease_id)?;
            }
            match op {
                KvOp::Put {
                    key,
                    value,
                    lease_id: Some(lease_id),
                } => {
                    if !active_leases.contains(lease_id) {
                        return Err(KvError::UnknownLease {
                            lease_id: *lease_id,
                        });
                    }
                    let old_state = self.simulated_key_state(
                        key,
                        touched_keys,
                        deleted_prefixes,
                        revoked_leases,
                    );
                    if old_state.lease_id != Some(*lease_id) {
                        let lease_keys =
                            lease_key_counts.get(lease_id).copied().unwrap_or_default();
                        if lease_keys >= MAX_KEYS_PER_LEASE {
                            return Err(KvError::LeaseKeyLimit {
                                lease_id: *lease_id,
                                observed_keys: lease_keys.saturating_add(1),
                                max_keys: MAX_KEYS_PER_LEASE,
                            });
                        }
                    }
                    let changed = !(old_state.present
                        && old_state.lease_id == Some(*lease_id)
                        && self.simulated_value_equals(
                            key,
                            value,
                            touched_keys,
                            deleted_prefixes,
                            revoked_leases,
                        ));
                    if changed {
                        record_kv_mutation_events(&mut mutation_events, 1)?;
                    }
                    update_simulated_lease_key_counts(
                        lease_key_counts,
                        old_state.lease_id,
                        Some(*lease_id),
                    );
                    if changed {
                        *revision = revision.next();
                        touched_keys.try_insert_with_value(
                            key,
                            SimulatedKeyState::after_put(old_state, Some(*lease_id), *revision),
                            value,
                            "track simulated touched key",
                        )?;
                    }
                }
                KvOp::Put {
                    key,
                    value,
                    lease_id: None,
                } => {
                    let old_state = self.simulated_key_state(
                        key,
                        touched_keys,
                        deleted_prefixes,
                        revoked_leases,
                    );
                    let changed = !(old_state.present
                        && old_state.lease_id.is_none()
                        && self.simulated_value_equals(
                            key,
                            value,
                            touched_keys,
                            deleted_prefixes,
                            revoked_leases,
                        ));
                    if changed {
                        record_kv_mutation_events(&mut mutation_events, 1)?;
                    }
                    update_simulated_lease_key_counts(lease_key_counts, old_state.lease_id, None);
                    if changed {
                        *revision = revision.next();
                        touched_keys.try_insert_with_value(
                            key,
                            SimulatedKeyState::after_put(old_state, None, *revision),
                            value,
                            "track simulated touched key",
                        )?;
                    }
                }
                KvOp::Delete { key } => {
                    let old_state = self.simulated_key_state(
                        key,
                        touched_keys,
                        deleted_prefixes,
                        revoked_leases,
                    );
                    if old_state.present {
                        record_kv_mutation_events(&mut mutation_events, 1)?;
                        *revision = revision.next();
                    }
                    update_simulated_lease_key_counts(lease_key_counts, old_state.lease_id, None);
                    touched_keys.try_insert(
                        key,
                        SimulatedKeyState::ABSENT,
                        "track simulated touched key",
                    )?;
                }
                KvOp::DeletePrefix { prefix } => {
                    let deleted = self.simulated_delete_prefix(
                        prefix,
                        touched_keys,
                        deleted_prefixes,
                        revoked_leases,
                        MAX_DELETE_PREFIX_KEYS,
                    )?;
                    record_kv_mutation_events(&mut mutation_events, deleted.total)?;
                    revision.0 = revision.0.saturating_add(deleted.total as u64);
                    for (lease_id, deleted_keys) in deleted.keys_by_lease {
                        let count = lease_key_counts.entry(lease_id).or_default();
                        *count = count.saturating_sub(deleted_keys);
                    }
                    touched_keys.remove_prefix(prefix)?;
                    deleted_prefixes.try_insert(prefix)?;
                }
                KvOp::LeaseGrant {
                    lease_id,
                    ttl_ticks,
                } => {
                    if *ttl_ticks == 0 {
                        return Err(KvError::InvalidLeaseTtl {
                            ttl_ticks: *ttl_ticks,
                        });
                    }
                    if !active_leases.contains(lease_id) && active_leases.len() >= MAX_ACTIVE_LEASES
                    {
                        return Err(KvError::LeaseLimit {
                            max: MAX_ACTIVE_LEASES,
                        });
                    }
                    active_leases.try_insert(*lease_id)?;
                }
                KvOp::LeaseKeepAlive { lease_id } => {
                    if !active_leases.contains(lease_id) {
                        return Err(KvError::UnknownLease {
                            lease_id: *lease_id,
                        });
                    }
                }
                KvOp::LeaseRevoke { lease_id } => {
                    let deleted_count = lease_key_counts.get(lease_id).copied().unwrap_or_default();
                    record_kv_mutation_events(&mut mutation_events, deleted_count)?;
                    revision.0 = revision.0.saturating_add(deleted_count as u64);
                    active_leases.remove(lease_id);
                    lease_key_counts.insert(*lease_id, 0);
                    touched_keys.expire_lease(*lease_id);
                    revoked_leases.insert(*lease_id);
                }
            }
        }
        Ok(())
    }

    fn apply_put(
        &mut self,
        key: String,
        value: Vec<u8>,
        lease_id: Option<LeaseId>,
    ) -> Result<KvOpResponse, KvError> {
        validate_kv_value_len(value.len())?;
        if let Some(lease_id) = lease_id {
            if !self.leases.contains_key(&lease_id) {
                return Err(KvError::UnknownLease { lease_id });
            }
            let old_lease_id = self.entries.get(&key).and_then(|entry| entry.lease_id);
            if old_lease_id != Some(lease_id) {
                self.validate_lease_key_capacity(lease_id)?;
            }
            self.lease_keys
                .try_reserve_add_key(lease_id, &key, "index kv lease key")?;
        }

        let revision = self.bump_revision();
        let old_lease_id = self.entries.get(&key).and_then(|entry| entry.lease_id);
        let existing_meta = self
            .entries
            .get(&key)
            .map(|entry| (entry.create_revision, entry.version));
        let entry = if let Some((create_revision, version)) = existing_meta {
            KvEntry {
                key: try_clone_kv_string(&key, "store kv put key")?,
                value: try_clone_kv_bytes(&value, "store kv put value")?,
                create_revision,
                modify_revision: revision,
                version: KvVersion(version.0.saturating_add(1)),
                lease_id,
            }
        } else {
            KvEntry {
                key: try_clone_kv_string(&key, "store kv put key")?,
                value: try_clone_kv_bytes(&value, "store kv put value")?,
                create_revision: revision,
                modify_revision: revision,
                version: KvVersion(1),
                lease_id,
            }
        };

        let added_lease_key = if let Some(lease_id) = lease_id
            && old_lease_id != Some(lease_id)
        {
            self.add_lease_key(lease_id, key.clone())?;
            true
        } else {
            false
        };
        if let Err(error) = self.entries.try_insert(entry, "insert kv entry") {
            if added_lease_key && let Some(lease_id) = lease_id {
                self.remove_lease_key(lease_id, &key);
            }
            return Err(error);
        }
        if let Some(old_lease_id) = old_lease_id
            && Some(old_lease_id) != lease_id
        {
            self.remove_lease_key(old_lease_id, &key);
        }
        self.record_event(KvEvent {
            revision,
            key: try_clone_kv_string(&key, "record kv put key")?,
            kind: KvEventKind::Put { value },
        })?;
        Ok(KvOpResponse::Put { key, revision })
    }

    fn apply_delete(&mut self, key: String) -> Result<KvOpResponse, KvError> {
        let Some(entry) = self.entries.remove(&key) else {
            return Ok(KvOpResponse::Delete {
                key,
                deleted: false,
            });
        };
        if let Some(lease_id) = entry.lease_id {
            self.remove_lease_key(lease_id, &key);
        }
        let revision = self.bump_revision();
        self.record_event(KvEvent {
            revision,
            key: try_clone_kv_string(&key, "record kv delete key")?,
            kind: KvEventKind::Delete,
        })?;
        Ok(KvOpResponse::Delete { key, deleted: true })
    }

    fn apply_delete_prefix(&mut self, prefix: String) -> Result<KvOpResponse, KvError> {
        let keys = self.keys_with_prefix_limited(&prefix, MAX_DELETE_PREFIX_KEYS)?;
        let deleted_count = keys.len();
        for key in keys {
            self.apply_delete(key)?;
        }
        Ok(KvOpResponse::DeletePrefix {
            prefix,
            deleted_count,
        })
    }

    fn keys_with_prefix_limited(
        &self,
        prefix: &str,
        max_keys: usize,
    ) -> Result<Vec<String>, KvError> {
        let mut keys = Vec::new();
        for entry in self.entries.prefix_entries(prefix) {
            if keys.len() >= max_keys {
                return Err(KvError::DeletePrefixLimit {
                    prefix: prefix.to_string(),
                    observed_keys: max_keys.saturating_add(1),
                    max_keys,
                });
            }
            try_reserve_kv_items(&mut keys, 1, "collect delete-prefix keys")?;
            keys.push(try_clone_kv_string(
                &entry.key,
                "collect delete-prefix keys",
            )?);
        }
        Ok(keys)
    }

    fn simulated_delete_prefix(
        &self,
        prefix: &str,
        touched_keys: &SimulatedKeyStateIndex,
        deleted_prefixes: &SimulatedDeletedPrefixIndex,
        revoked_leases: &BTreeSet<LeaseId>,
        max_keys: usize,
    ) -> Result<SimulatedDeleteSummary, KvError> {
        let mut deleted = SimulatedDeleteSummary::default();
        let base_prefix_already_deleted = deleted_prefixes.contains_prefix_of(prefix);
        if !base_prefix_already_deleted {
            for entry in self.entries.prefix_entries(prefix) {
                let state = self.simulated_key_state(
                    &entry.key,
                    touched_keys,
                    deleted_prefixes,
                    revoked_leases,
                );
                if state.present {
                    deleted.record(state);
                }
                if deleted.total > max_keys {
                    return Err(KvError::DeletePrefixLimit {
                        prefix: prefix.to_string(),
                        observed_keys: deleted.total,
                        max_keys,
                    });
                }
            }
        }
        for (key, state) in touched_keys.iter() {
            if state.present
                && key.starts_with(prefix)
                && (base_prefix_already_deleted || !self.entries.contains_key(key))
            {
                deleted.record(*state);
                if deleted.total > max_keys {
                    return Err(KvError::DeletePrefixLimit {
                        prefix: prefix.to_string(),
                        observed_keys: deleted.total,
                        max_keys,
                    });
                }
            }
        }
        Ok(deleted)
    }

    fn validate_lease_key_capacity(&self, lease_id: LeaseId) -> Result<(), KvError> {
        let lease_keys = self
            .lease_keys
            .get(&lease_id)
            .map(BTreeSet::len)
            .unwrap_or_default();
        if lease_keys >= MAX_KEYS_PER_LEASE {
            return Err(KvError::LeaseKeyLimit {
                lease_id,
                observed_keys: lease_keys.saturating_add(1),
                max_keys: MAX_KEYS_PER_LEASE,
            });
        }
        Ok(())
    }

    fn no_effect_response_for_op(&self, op: &KvOp) -> Option<KvOpResponse> {
        match op {
            KvOp::Put {
                key,
                value,
                lease_id,
            } => self.entries.get(key).and_then(|entry| {
                (entry.value.as_slice() == value.as_slice() && entry.lease_id == *lease_id)
                    .then_some(KvOpResponse::Put {
                        key: key.clone(),
                        revision: entry.modify_revision,
                    })
            }),
            KvOp::Delete { key } => {
                (!self.entries.contains_key(key)).then(|| KvOpResponse::Delete {
                    key: key.clone(),
                    deleted: false,
                })
            }
            KvOp::DeletePrefix { prefix } => {
                let has_matching_key = self.entries.first_prefix_entry(prefix).is_some();
                (!has_matching_key).then(|| KvOpResponse::DeletePrefix {
                    prefix: prefix.clone(),
                    deleted_count: 0,
                })
            }
            KvOp::LeaseGrant {
                lease_id,
                ttl_ticks,
            } => self.leases.get(lease_id).and_then(|lease| {
                (lease.ttl_ticks == *ttl_ticks
                    && self.lease_remaining_ticks(*lease_id) == Some(lease.ttl_ticks))
                .then_some(KvOpResponse::LeaseGrant {
                    lease_id: *lease_id,
                })
            }),
            KvOp::LeaseKeepAlive { lease_id } => {
                let lease = self.leases.get(lease_id)?;
                (self.lease_remaining_ticks(*lease_id) == Some(lease.ttl_ticks)).then_some(
                    KvOpResponse::LeaseKeepAlive {
                        lease_id: *lease_id,
                    },
                )
            }
            KvOp::LeaseRevoke { lease_id } => {
                (!self.leases.contains_key(lease_id)).then_some(KvOpResponse::LeaseRevoke {
                    lease_id: *lease_id,
                    deleted_count: 0,
                })
            }
        }
    }

    fn keys_for_lease_limited(
        &self,
        lease_id: LeaseId,
        max_keys: usize,
    ) -> Result<Vec<String>, KvError> {
        let Some(keys) = self.lease_keys.get(&lease_id) else {
            return Ok(Vec::new());
        };
        if keys.len() > max_keys {
            return Err(KvError::MutationEventLimit {
                observed_events: keys.len(),
                max_events: max_keys,
            });
        }
        let mut retained = Vec::new();
        try_reserve_kv_items(&mut retained, keys.len(), "collect lease keys")?;
        for key in keys {
            retained.push(try_clone_kv_string(key, "collect lease keys")?);
        }
        Ok(retained)
    }

    fn lease_revoke_batch(
        &self,
        lease_id: LeaseId,
        max_keys: usize,
    ) -> Result<(Vec<String>, bool), KvError> {
        let Some(keys) = self.lease_keys.get(&lease_id) else {
            return Ok((Vec::new(), true));
        };
        let complete = keys.len() <= max_keys;
        let mut retained = Vec::new();
        try_reserve_kv_items(
            &mut retained,
            keys.len().min(max_keys),
            "collect lease revoke batch",
        )?;
        for key in keys.iter().take(max_keys) {
            retained.push(try_clone_kv_string(key, "collect lease revoke batch")?);
        }
        Ok((retained, complete))
    }

    fn try_current_leases_for_snapshot(&self) -> Result<Vec<KvLease>, KvError> {
        let mut leases = Vec::new();
        try_reserve_kv_items(&mut leases, self.leases.len(), "build kv snapshot leases")?;
        for lease in self.leases.values() {
            let mut lease = lease.clone();
            lease.remaining_ticks = self
                .lease_remaining_ticks(lease.lease_id)
                .unwrap_or(lease.remaining_ticks);
            leases.push(lease);
        }
        Ok(leases)
    }

    fn lease_remaining_ticks(&self, lease_id: LeaseId) -> Option<u64> {
        let lease = self.leases.get(&lease_id)?;
        let deadline = self.lease_deadline_by_id.get(&lease_id).copied()?;
        Some(
            deadline
                .saturating_sub(self.lease_clock)
                .min(lease.ttl_ticks),
        )
    }

    fn expired_lease_candidates(&self) -> Result<Vec<LeaseId>, KvError> {
        let mut candidates = Vec::new();
        self.lease_deadlines
            .push_expired_candidates(self.lease_clock, &mut candidates)?;
        Ok(candidates)
    }

    fn upsert_lease(&mut self, lease: KvLease) -> Result<(), KvError> {
        let lease_id = lease.lease_id;
        let deadline = self.lease_clock.saturating_add(lease.remaining_ticks);
        self.leases
            .try_reserve_insert(lease_id, "upsert kv lease")?;
        self.lease_deadline_by_id
            .try_reserve_insert(lease_id, "index kv lease deadline")?;
        self.lease_deadlines
            .try_reserve_insert(deadline, lease_id, "index kv lease deadline")?;
        self.remove_lease_deadline(lease_id);
        self.lease_deadline_by_id
            .insert_reserved(lease_id, deadline);
        self.lease_deadlines.insert_reserved(deadline, lease_id);
        self.leases.insert_reserved(lease);
        Ok(())
    }

    fn remove_lease_deadline(&mut self, lease_id: LeaseId) -> Option<u64> {
        let deadline = self.lease_deadline_by_id.remove(&lease_id)?;
        self.lease_deadlines.remove(deadline, lease_id);
        Some(deadline)
    }

    fn remove_lease_and_delete_keys(
        &mut self,
        lease_id: LeaseId,
        keys: Vec<String>,
    ) -> Result<KvOpResponse, KvError> {
        self.leases.remove(&lease_id);
        self.remove_lease_deadline(lease_id);
        self.lease_keys.remove(&lease_id);
        let deleted_count = keys.len();
        self.delete_lease_keys(keys)?;
        Ok(KvOpResponse::LeaseRevoke {
            lease_id,
            deleted_count,
        })
    }

    fn delete_lease_keys(&mut self, keys: Vec<String>) -> Result<(), KvError> {
        for key in keys {
            self.apply_delete(key)?;
        }
        Ok(())
    }

    fn simulated_key_state(
        &self,
        key: &str,
        touched_keys: &SimulatedKeyStateIndex,
        deleted_prefixes: &SimulatedDeletedPrefixIndex,
        revoked_leases: &BTreeSet<LeaseId>,
    ) -> SimulatedKeyState {
        if let Some(state) = touched_keys.get(key) {
            return state;
        }
        if deleted_prefixes.contains_prefix_of(key) {
            return SimulatedKeyState::ABSENT;
        }
        let Some(entry) = self.entries.get(key) else {
            return SimulatedKeyState::ABSENT;
        };
        if entry
            .lease_id
            .is_some_and(|lease_id| revoked_leases.contains(&lease_id))
        {
            return SimulatedKeyState::ABSENT;
        }
        SimulatedKeyState::from_entry(entry)
    }

    fn simulated_value_equals(
        &self,
        key: &str,
        value: &[u8],
        touched_keys: &SimulatedKeyStateIndex,
        deleted_prefixes: &SimulatedDeletedPrefixIndex,
        revoked_leases: &BTreeSet<LeaseId>,
    ) -> bool {
        if let Some(state) = touched_keys.get(key) {
            return state.present && touched_keys.exact_value(key) == Some(value);
        }
        if deleted_prefixes.contains_prefix_of(key) {
            return false;
        }
        self.entries.get(key).is_some_and(|entry| {
            !entry
                .lease_id
                .is_some_and(|lease_id| revoked_leases.contains(&lease_id))
                && entry.value == value
        })
    }

    fn compare_with_overlay(&self, compare: &KvCompare, overlay: &KvPreflightOverlay) -> bool {
        let key = match compare {
            KvCompare::Exists { key }
            | KvCompare::Missing { key }
            | KvCompare::ValueEquals { key, .. }
            | KvCompare::VersionEquals { key, .. }
            | KvCompare::CreateRevisionEquals { key, .. }
            | KvCompare::ModifyRevisionEquals { key, .. }
            | KvCompare::LeaseEquals { key, .. } => key,
        };
        let state = self.simulated_key_state(
            key,
            &overlay.touched_keys,
            &overlay.deleted_prefixes,
            &overlay.revoked_leases,
        );
        match compare {
            KvCompare::Exists { .. } => state.present,
            KvCompare::Missing { .. } => !state.present,
            KvCompare::ValueEquals { value, .. } => self.simulated_value_equals(
                key,
                value,
                &overlay.touched_keys,
                &overlay.deleted_prefixes,
                &overlay.revoked_leases,
            ),
            KvCompare::VersionEquals { version, .. } => state.present && state.version == *version,
            KvCompare::CreateRevisionEquals { revision, .. } => {
                state.present && state.create_revision == *revision
            }
            KvCompare::ModifyRevisionEquals { revision, .. } => {
                state.present && state.modify_revision == *revision
            }
            KvCompare::LeaseEquals { lease_id, .. } => state.present && state.lease_id == *lease_id,
        }
    }

    fn preflight_apply_command_with_overlay(
        &self,
        command: &KvCommand,
        overlay: &mut KvPreflightOverlay,
    ) -> Result<(), KvError> {
        validate_kv_command_payload(command)?;
        match command {
            KvCommand::Op(op) => self.validate_ops_with_overlay(std::slice::from_ref(op), overlay),
            KvCommand::Txn(txn) => {
                self.validate_txn_shape(txn)?;
                validate_kv_txn_payload(txn)?;
                let succeeded = txn
                    .compares
                    .iter()
                    .all(|compare| self.compare_with_overlay(compare, overlay));
                let ops = if succeeded {
                    txn.success.as_slice()
                } else {
                    txn.failure.as_slice()
                };
                self.validate_ops_with_overlay(ops, overlay)
            }
            KvCommand::Barrier => Ok(()),
        }
    }

    fn compare(&self, compare: &KvCompare) -> bool {
        match compare {
            KvCompare::Exists { key } => self.entries.contains_key(key),
            KvCompare::Missing { key } => !self.entries.contains_key(key),
            KvCompare::ValueEquals { key, value } => self
                .entries
                .get(key)
                .is_some_and(|entry| &entry.value == value),
            KvCompare::VersionEquals { key, version } => self
                .entries
                .get(key)
                .is_some_and(|entry| &entry.version == version),
            KvCompare::CreateRevisionEquals { key, revision } => self
                .entries
                .get(key)
                .is_some_and(|entry| &entry.create_revision == revision),
            KvCompare::ModifyRevisionEquals { key, revision } => self
                .entries
                .get(key)
                .is_some_and(|entry| &entry.modify_revision == revision),
            KvCompare::LeaseEquals { key, lease_id } => self
                .entries
                .get(key)
                .is_some_and(|entry| &entry.lease_id == lease_id),
        }
    }

    fn bump_revision(&mut self) -> KvRevision {
        self.revision = self.revision.next();
        self.revision
    }

    fn record_event(&mut self, event: KvEvent) -> Result<(), KvError> {
        let watch_ids = self.matching_watch_ids_for_event_key(&event.key)?;
        let event = Arc::new(event);
        try_reserve_kv_items(&mut self.events, 1, "record kv event")?;
        for watch_id in watch_ids {
            if let Some(watch) = self.watches.get_mut(&watch_id)
                && event.revision >= watch.watch.start_revision
            {
                let requested_bytes = watch
                    .pending
                    .len()
                    .saturating_add(1)
                    .saturating_mul(std::mem::size_of::<KvEvent>().max(1));
                watch
                    .pending
                    .try_reserve(1)
                    .map_err(|_| KvError::ResourceExhausted {
                        operation: "queue kv watch event".to_string(),
                        requested_bytes,
                    })?;
                watch.pending.push_back(Arc::clone(&event));
                compact_watch_pending(watch);
            }
        }
        self.events.push(event);
        self.trim_event_history();
        Ok(())
    }

    fn index_watch_prefix(&mut self, watch_id: WatchId, prefix: &str) -> Result<(), KvError> {
        self.watch_prefixes
            .try_add(watch_id, prefix, "index kv watch prefix")
    }

    fn remove_watch(&mut self, watch_id: WatchId) -> Option<KvWatchState> {
        let watch = self.watches.remove(&watch_id)?;
        self.remove_watch_prefix_index(watch_id, &watch.watch.prefix);
        Some(watch)
    }

    fn remove_watch_prefix_index(&mut self, watch_id: WatchId, prefix: &str) {
        self.watch_prefixes.remove(watch_id, prefix);
    }

    fn matching_watch_ids_for_event_key(&self, key: &str) -> Result<Vec<WatchId>, KvError> {
        let mut watch_ids = Vec::new();
        if let Some(prefix_watch_ids) = self.watch_prefixes.get("") {
            try_reserve_kv_items(
                &mut watch_ids,
                prefix_watch_ids.len(),
                "collect matching watch ids",
            )?;
            watch_ids.extend(prefix_watch_ids.iter().copied());
        }
        for (start, ch) in key.char_indices() {
            let end = start + ch.len_utf8();
            if let Some(prefix_watch_ids) = self.watch_prefixes.get(&key[..end]) {
                try_reserve_kv_items(
                    &mut watch_ids,
                    prefix_watch_ids.len(),
                    "collect matching watch ids",
                )?;
                watch_ids.extend(prefix_watch_ids.iter().copied());
            }
        }
        Ok(watch_ids)
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn matching_watch_ids_for_event_key_for_test(&self, key: &str) -> Vec<WatchId> {
        let mut watch_ids = self
            .matching_watch_ids_for_event_key(key)
            .expect("matching watch ids");
        watch_ids.sort();
        watch_ids
    }

    fn trim_event_history(&mut self) {
        self.compact_event_history_to(MAX_RETAINED_EVENTS);
    }

    fn add_lease_key(&mut self, lease_id: LeaseId, key: String) -> Result<(), KvError> {
        self.lease_keys
            .try_add_key(lease_id, key, "index kv lease key")
    }

    fn remove_lease_key(&mut self, lease_id: LeaseId, key: &str) {
        self.lease_keys.remove_key(lease_id, key);
    }
}

/// Compares two snapshots by their durable key/value/lease state, ignoring the
/// top-level revision, per-entry revision metadata (`create_revision`,
/// `modify_revision`, `version`), lease grant wall-clock timestamps, and the
/// watch-event tail. Lease `remaining_ticks` is intentionally durable: once lease
/// ticking starts, skipping that field would let a restart resurrect expired time.
/// Two snapshots that are durable-equal materialize to the same observable KV
/// state, so there is no need to rewrite the persisted file.
#[derive(Clone, Debug)]
pub(crate) struct KvWatchState {
    pub(crate) watch: KvWatch,
    pub(crate) pending: VecDeque<Arc<KvEvent>>,
    pub(crate) compacted_revision: Option<KvRevision>,
}

pub(crate) fn compact_watch_pending(watch: &mut KvWatchState) {
    let excess = watch
        .pending
        .len()
        .saturating_sub(MAX_PENDING_EVENTS_PER_WATCH);
    if excess == 0 {
        return;
    }
    let compacted = watch
        .pending
        .get(excess - 1)
        .map(|event| event.revision)
        .unwrap_or(watch.watch.start_revision);
    watch.pending.drain(..excess);
    watch.compacted_revision = Some(
        watch
            .compacted_revision
            .unwrap_or(KvRevision::ZERO)
            .max(compacted),
    );
}

fn single_log_entry_vec(entry: KvLogEntry, operation: &str) -> Result<Vec<KvLogEntry>, KvError> {
    let mut entries = Vec::new();
    try_reserve_kv_items(&mut entries, 1, operation)?;
    entries.push(entry);
    Ok(entries)
}

fn try_clone_kv_log_entry(entry: &KvLogEntry, operation: &str) -> Result<KvLogEntry, KvError> {
    Ok(KvLogEntry {
        index: entry.index,
        term: entry.term,
        request_id: entry.request_id.clone(),
        command: try_clone_kv_command(&entry.command, operation)?,
        revision: entry.revision,
    })
}

fn try_clone_raft_vote_request(
    request: &RaftRequestVoteRequest,
    operation: &str,
) -> Result<RaftRequestVoteRequest, KvError> {
    Ok(RaftRequestVoteRequest {
        term: request.term,
        candidate_id: try_clone_kv_node_id(&request.candidate_id, operation)?,
        last_log_index: request.last_log_index,
        last_log_term: request.last_log_term,
    })
}

#[derive(Clone, Debug, Default)]
struct KvNodeSet {
    nodes: BTreeSet<NodeId>,
}

impl KvNodeSet {
    fn from_ids(node_ids: Vec<NodeId>, operation: &str) -> Result<Self, KvError> {
        let _ = operation;
        let nodes = node_ids.into_iter().collect();
        Ok(Self { nodes })
    }

    fn len(&self) -> usize {
        self.nodes.len()
    }

    fn contains(&self, node_id: &NodeId) -> bool {
        self.nodes.contains(node_id)
    }

    fn iter(&self) -> impl Iterator<Item = &NodeId> {
        self.nodes.iter()
    }
}

#[derive(Clone, Debug, Default)]
struct KvRaftNodeIndex {
    nodes: BTreeMap<NodeId, ReplicatedKvNode>,
    online: BTreeSet<NodeId>,
}

impl KvRaftNodeIndex {
    fn from_voters(voters: &KvNodeSet) -> Result<Self, KvError> {
        let mut nodes = BTreeMap::new();
        let mut online = BTreeSet::new();
        for node_id in voters.iter() {
            let node_id = try_clone_kv_node_id(node_id, "initialize raft node id")?;
            online.insert(try_clone_kv_node_id(
                &node_id,
                "initialize raft online node id",
            )?);
            nodes.insert(node_id, ReplicatedKvNode::empty());
        }
        Ok(Self { nodes, online })
    }

    fn contains_key(&self, node_id: &NodeId) -> bool {
        self.nodes.contains_key(node_id)
    }

    fn get(&self, node_id: &NodeId) -> Option<&ReplicatedKvNode> {
        self.nodes.get(node_id)
    }

    fn get_mut(&mut self, node_id: &NodeId) -> Option<&mut ReplicatedKvNode> {
        self.nodes.get_mut(node_id)
    }

    fn is_online(&self, node_id: &NodeId) -> bool {
        self.online.contains(node_id)
    }

    fn set_online(&mut self, node_id: &NodeId, online: bool) -> Result<(), KvError> {
        if !self.nodes.contains_key(node_id) {
            return Err(KvError::UnknownNode {
                node_id: try_clone_kv_node_id(node_id, "report unknown raft node")?,
            });
        }
        if online {
            self.online
                .insert(try_clone_kv_node_id(node_id, "mark raft node online")?);
        } else {
            self.online.remove(node_id);
        }
        Ok(())
    }

    fn online_node_ids(&self, operation: &str) -> Result<Vec<NodeId>, KvError> {
        let mut node_ids = Vec::new();
        try_reserve_kv_items(&mut node_ids, self.online.len(), operation)?;
        for node_id in &self.online {
            node_ids.push(try_clone_kv_node_id(node_id, operation)?);
        }
        Ok(node_ids)
    }

    fn online_voter_count(&self, voters: &KvNodeSet) -> usize {
        voters
            .iter()
            .filter(|node_id| self.is_online(node_id))
            .count()
    }

    fn online_voter_ids(
        &self,
        voters: &KvNodeSet,
        operation: &str,
    ) -> Result<Vec<NodeId>, KvError> {
        let online_count = self.online_voter_count(voters);
        let mut node_ids = Vec::new();
        try_reserve_kv_items(&mut node_ids, online_count, operation)?;
        for node_id in voters.iter().filter(|node_id| self.is_online(node_id)) {
            node_ids.push(try_clone_kv_node_id(node_id, operation)?);
        }
        Ok(node_ids)
    }

    fn values_mut(&mut self) -> impl Iterator<Item = &mut ReplicatedKvNode> {
        self.nodes.values_mut()
    }

    fn try_insert_online(
        &mut self,
        node_id: NodeId,
        node: ReplicatedKvNode,
        operation: &str,
    ) -> Result<Option<ReplicatedKvNode>, KvError> {
        self.online
            .insert(try_clone_kv_node_id(&node_id, operation)?);
        Ok(self.nodes.insert(node_id, node))
    }
}

#[derive(Clone, Debug)]
pub struct InProcessRaftKvCluster {
    term: u64,
    leader_id: NodeId,
    voters: KvNodeSet,
    nodes: KvRaftNodeIndex,
    next_log_index: u64,
    commit_index: u64,
}

impl InProcessRaftKvCluster {
    pub fn new(voter_ids: Vec<NodeId>) -> Result<Self, KvError> {
        let Some(leader_id) = voter_ids.first() else {
            return Err(KvError::NoVoters);
        };
        let leader_id = try_clone_kv_node_id(leader_id, "select initial raft leader")?;
        let voters = KvNodeSet::from_ids(voter_ids, "initialize raft voters")?;
        let nodes = KvRaftNodeIndex::from_voters(&voters)?;
        Ok(Self {
            term: 1,
            leader_id,
            voters,
            nodes,
            next_log_index: 1,
            commit_index: 0,
        })
    }

    pub fn leader_id(&self) -> &NodeId {
        &self.leader_id
    }

    pub fn store(&self, node_id: &NodeId) -> Option<&InProcessKvStore> {
        self.nodes.get(node_id).map(|node| &node.store)
    }

    pub fn set_node_online(&mut self, node_id: &NodeId, online: bool) -> Result<(), KvError> {
        self.nodes.set_online(node_id, online)?;
        if online {
            self.catch_up_node(node_id)?;
        }
        Ok(())
    }

    pub fn apply_as_leader(&mut self, command: KvCommand) -> Result<KvRevision, KvError> {
        self.propose_command(command)
            .map(|outcome| outcome.revision)
    }

    pub fn txn_response(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
        let outcome = self.propose_command(KvCommand::Txn(txn))?;
        outcome.txn_response.ok_or_else(|| KvError::Serialization {
            detail: "committed KV transaction omitted its transaction response".to_string(),
        })
    }

    fn propose_command(&mut self, command: KvCommand) -> Result<RaftCommitOutcome, KvError> {
        if !self.nodes.is_online(&self.leader_id) {
            return Err(KvError::LeaderOffline {
                leader_id: try_clone_kv_node_id(&self.leader_id, "report offline raft leader")?,
            });
        }
        let online_voters = self.nodes.online_voter_count(&self.voters);
        if online_voters < self.quorum_size() {
            return Err(KvError::QuorumUnavailable {
                online_voters,
                quorum: self.quorum_size(),
            });
        }

        let index = self.next_log_index;
        self.next_log_index += 1;
        let entry = KvLogEntry {
            index,
            term: self.term,
            request_id: KvRequestId::generate(),
            command,
            revision: KvRevision::ZERO,
        };
        let leader_id = try_clone_kv_node_id(&self.leader_id, "propose raft leader entry")?;
        self.node_mut(&leader_id)?
            .append_entries(single_log_entry_vec(
                try_clone_kv_log_entry(&entry, "append leader raft entry")?,
                "append leader raft entry",
            )?)?;
        let mut voter_acks = usize::from(self.voters.contains(&leader_id));

        let online_nodes = self.nodes.online_node_ids("collect online raft nodes")?;
        for node_id in online_nodes.iter() {
            if *node_id == leader_id {
                continue;
            }
            let ack = self.append_entries_to_follower(
                node_id,
                single_log_entry_vec(
                    try_clone_kv_log_entry(&entry, "append follower raft entry")?,
                    "append follower raft entry",
                )?,
            )?;
            if ack.success && self.voters.contains(node_id) {
                voter_acks += 1;
            }
        }

        if voter_acks < self.quorum_size() {
            return Err(KvError::QuorumUnavailable {
                online_voters: voter_acks,
                quorum: self.quorum_size(),
            });
        }

        self.commit_index = index;
        for node_id in online_nodes {
            self.node_mut(&node_id)?.commit_to(index)?;
        }
        let leader = self.node(&leader_id)?;
        let committed = leader
            .log
            .iter()
            .find(|entry| entry.index == index)
            .ok_or(KvError::MissingCommittedEntry { index })?;
        let txn_response = leader.txn_responses.get_cloned(index)?;
        Ok(RaftCommitOutcome {
            revision: committed.revision,
            txn_response,
        })
    }

    fn append_entries_to_follower(
        &mut self,
        node_id: &NodeId,
        entries: Vec<KvLogEntry>,
    ) -> Result<RaftAppendEntriesResponse, KvError> {
        let prev_log_index = entries
            .first()
            .map(|entry| entry.index.saturating_sub(1))
            .unwrap_or(self.commit_index);
        let prev_log_term = self
            .node(&self.leader_id)
            .ok()
            .and_then(|node| node.term_at(prev_log_index))
            .unwrap_or(0);
        let request = RaftAppendEntriesRequest {
            term: self.term,
            leader_id: try_clone_kv_node_id(&self.leader_id, "build raft append request")?,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: self.commit_index,
        };
        self.node_mut(node_id)?
            .handle_append_entries(request)
            .map(|outcome| outcome.response)
    }

    pub fn catch_up_node(&mut self, node_id: &NodeId) -> Result<(), KvError> {
        if !self.nodes.contains_key(node_id) {
            return Err(KvError::UnknownNode {
                node_id: try_clone_kv_node_id(node_id, "report unknown raft node")?,
            });
        }
        if node_id == &self.leader_id {
            return Ok(());
        }
        let follower_last_log_index = self.node(node_id)?.last_log_index();
        let (snapshot, missing, leader_commit_index) = {
            let leader = self.node(&self.leader_id)?;
            let (snapshot, missing_after_index) = if follower_last_log_index < leader.snapshot_index
            {
                (
                    Some(RaftSnapshot {
                        last_included_index: leader.applied_index,
                        last_included_term: leader
                            .term_at(leader.applied_index)
                            .unwrap_or(leader.term()),
                        kv: (leader.store.try_snapshot()?).into(),
                    }),
                    leader.applied_index,
                )
            } else {
                (None, follower_last_log_index)
            };
            let mut missing = Vec::new();
            for entry in leader
                .log
                .iter()
                .filter(|entry| entry.index > missing_after_index)
            {
                try_reserve_kv_items(&mut missing, 1, "collect missing raft entries")?;
                missing.push(try_clone_kv_log_entry(
                    entry,
                    "collect missing raft entries",
                )?);
            }
            (snapshot, missing, leader.hard_state.commit_index)
        };
        let follower = self.node_mut(node_id)?;
        if let Some(snapshot) = snapshot {
            follower.install_snapshot(snapshot)?;
        }
        follower.append_entries(missing)?;
        follower.commit_to(leader_commit_index)
    }

    pub fn request_vote(
        &mut self,
        voter_id: &NodeId,
        request: RaftRequestVoteRequest,
    ) -> Result<RaftRequestVoteResponse, KvError> {
        self.node_mut(voter_id)?.handle_request_vote(request)
    }

    pub fn log(&self, node_id: &NodeId) -> Option<&[KvLogEntry]> {
        self.nodes.get(node_id).map(|node| node.log.as_slice())
    }

    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }

    pub fn hard_state(&self, node_id: &NodeId) -> Option<&RaftHardState> {
        self.nodes.get(node_id).map(|node| &node.hard_state)
    }

    pub fn watch_prefix(
        &mut self,
        node_id: &NodeId,
        prefix: impl Into<String>,
        start_revision: KvRevision,
    ) -> Result<WatchId, KvError> {
        self.node_mut(node_id)?
            .store
            .watch_prefix(prefix, start_revision)
    }

    pub fn drain_watch(
        &mut self,
        node_id: &NodeId,
        watch_id: WatchId,
    ) -> Result<Vec<KvEvent>, KvError> {
        self.node_mut(node_id)?.store.drain_watch(watch_id)
    }

    pub fn cancel_watch(&mut self, node_id: &NodeId, watch_id: WatchId) -> Result<bool, KvError> {
        self.node_mut(node_id)?.store.cancel_watch(watch_id)
    }

    pub fn transfer_leader(&mut self, new_leader_id: NodeId) -> Result<(), KvError> {
        if !self.voters.contains(&new_leader_id) {
            return Err(KvError::UnknownNode {
                node_id: new_leader_id,
            });
        }
        if !self.nodes.is_online(&new_leader_id) {
            return Err(KvError::LeaderOffline {
                leader_id: new_leader_id,
            });
        }
        self.catch_up_node(&new_leader_id)?;
        self.term = self.term.saturating_add(1);
        self.leader_id = try_clone_kv_node_id(&new_leader_id, "transfer raft leader")?;
        let term = self.term;
        for node in self.nodes.values_mut() {
            node.hard_state.current_term = term;
            node.hard_state.voted_for = Some(try_clone_kv_node_id(
                &new_leader_id,
                "record transferred raft vote",
            )?);
            node.hard_state.leader_id = Some(try_clone_kv_node_id(
                &new_leader_id,
                "record transferred raft leader",
            )?);
        }
        Ok(())
    }

    pub fn elect_leader(&mut self, candidate_id: NodeId) -> Result<bool, KvError> {
        if !self.voters.contains(&candidate_id) {
            return Err(KvError::UnknownNode {
                node_id: candidate_id,
            });
        }
        if !self.nodes.is_online(&candidate_id) {
            return Err(KvError::LeaderOffline {
                leader_id: candidate_id,
            });
        }
        self.catch_up_node(&candidate_id)?;
        let (last_log_index, last_log_term) = {
            let candidate = self.node(&candidate_id)?;
            (candidate.last_log_index(), candidate.last_log_term())
        };
        let election_term = self.term.saturating_add(1);
        let request = RaftRequestVoteRequest {
            term: election_term,
            candidate_id: try_clone_kv_node_id(&candidate_id, "build raft vote request")?,
            last_log_index,
            last_log_term,
        };
        let mut granted = 0;
        let online_voters = self
            .nodes
            .online_voter_ids(&self.voters, "collect online raft voters")?;
        for voter_id in online_voters {
            let response = self.request_vote(
                &voter_id,
                try_clone_raft_vote_request(&request, "send raft vote request")?,
            )?;
            if response.term > self.term {
                self.term = response.term;
            }
            if response.vote_granted {
                granted += 1;
            }
        }
        if granted < self.quorum_size() {
            return Ok(false);
        }
        self.term = election_term.max(self.term);
        self.leader_id = try_clone_kv_node_id(&candidate_id, "elect raft leader")?;
        for node in self.nodes.values_mut() {
            if node.hard_state.current_term <= self.term {
                node.hard_state.current_term = self.term;
                node.hard_state.leader_id = Some(try_clone_kv_node_id(
                    &candidate_id,
                    "record elected raft leader",
                )?);
            }
        }
        Ok(true)
    }

    fn node(&self, node_id: &NodeId) -> Result<&ReplicatedKvNode, KvError> {
        match self.nodes.get(node_id) {
            Some(node) => Ok(node),
            None => Err(KvError::UnknownNode {
                node_id: try_clone_kv_node_id(node_id, "report unknown raft node")?,
            }),
        }
    }

    fn node_mut(&mut self, node_id: &NodeId) -> Result<&mut ReplicatedKvNode, KvError> {
        match self.nodes.get_mut(node_id) {
            Some(node) => Ok(node),
            None => Err(KvError::UnknownNode {
                node_id: try_clone_kv_node_id(node_id, "report unknown raft node")?,
            }),
        }
    }

    pub fn put(&mut self, key: impl Into<String>, value: Vec<u8>) -> Result<KvRevision, KvError> {
        self.apply_as_leader(KvCommand::Op(KvOp::Put {
            key: key.into(),
            value,
            lease_id: None,
        }))
    }

    pub fn txn(&mut self, txn: KvTxn) -> Result<KvRevision, KvError> {
        self.txn_response(txn).map(|response| response.revision)
    }

    pub fn install_snapshot(
        &mut self,
        target_node_id: NodeId,
        snapshot: KvSnapshot,
    ) -> Result<(), KvError> {
        let node = self
            .nodes
            .get_mut(&target_node_id)
            .ok_or(KvError::UnknownNode {
                node_id: target_node_id,
            })?;
        node.install_snapshot(RaftSnapshot {
            last_included_index: self.commit_index,
            last_included_term: self.term,
            kv: (snapshot).into(),
        })?;
        Ok(())
    }

    pub fn add_learner_from_snapshot(
        &mut self,
        node_id: NodeId,
        snapshot: KvSnapshot,
    ) -> Result<(), KvError> {
        if self.nodes.contains_key(&node_id) {
            return Err(KvError::DuplicateNode { node_id });
        }
        let node = ReplicatedKvNode::from_snapshot(
            snapshot,
            self.term,
            &self.leader_id,
            self.commit_index,
        )?;
        self.nodes
            .try_insert_online(node_id, node, "add raft learner node")?;
        Ok(())
    }

    fn quorum_size(&self) -> usize {
        self.voters.len() / 2 + 1
    }
}

pub(crate) enum CommitEntries {
    Single(KvLogEntry),
    Batch(Vec<KvLogEntry>),
}

impl CommitEntries {
    pub(crate) fn as_slice(&self) -> &[KvLogEntry] {
        match self {
            CommitEntries::Single(entry) => std::slice::from_ref(entry),
            CommitEntries::Batch(entries) => entries,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct KvTxnResponseIndex {
    responses: BTreeMap<u64, KvTxnResponse>,
}

impl KvTxnResponseIndex {
    pub(crate) fn get(&self, index: u64) -> Option<&KvTxnResponse> {
        self.responses.get(&index)
    }

    fn get_cloned(&self, index: u64) -> Result<Option<KvTxnResponse>, KvError> {
        let Some(response) = self.get(index) else {
            return Ok(None);
        };
        Ok(Some(try_clone_kv_txn_response(
            response,
            "clone raft txn response",
        )?))
    }

    fn try_insert(
        &mut self,
        index: u64,
        response: KvTxnResponse,
    ) -> Result<Option<KvTxnResponse>, KvError> {
        Ok(self.responses.insert(index, response))
    }

    fn retain_after(&mut self, cutoff: u64) {
        self.responses.retain(|index, _| *index > cutoff);
    }

    fn clear(&mut self) {
        self.responses.clear();
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ReplicatedKvNode {
    pub(crate) store: InProcessKvStore,
    pub(crate) log: Vec<KvLogEntry>,
    pub(crate) hard_state: RaftHardState,
    pub(crate) applied_index: u64,
    pub(crate) snapshot_index: u64,
    pub(crate) snapshot_term: u64,
    pub(crate) txn_responses: KvTxnResponseIndex,
}

impl ReplicatedKvNode {
    pub(crate) fn empty() -> Self {
        Self {
            store: InProcessKvStore::new(),
            log: Vec::new(),
            hard_state: RaftHardState::default(),
            applied_index: 0,
            snapshot_index: 0,
            snapshot_term: 0,
            txn_responses: KvTxnResponseIndex::default(),
        }
    }

    fn from_snapshot(
        snapshot: KvSnapshot,
        term: u64,
        leader_id: &NodeId,
        commit_index: u64,
    ) -> Result<Self, KvError> {
        Ok(Self {
            store: InProcessKvStore::restore(snapshot)?,
            log: Vec::new(),
            hard_state: RaftHardState {
                current_term: term,
                voted_for: None,
                leader_id: Some(try_clone_kv_node_id(
                    leader_id,
                    "record raft learner leader",
                )?),
                commit_index,
            },
            applied_index: commit_index,
            snapshot_index: commit_index,
            snapshot_term: term,
            txn_responses: KvTxnResponseIndex::default(),
        })
    }

    pub(crate) fn term(&self) -> u64 {
        self.hard_state.current_term
    }

    pub(crate) fn last_log_index(&self) -> u64 {
        self.log
            .last()
            .map(|entry| entry.index)
            .unwrap_or(self.snapshot_index)
            .max(self.snapshot_index)
    }

    pub(crate) fn last_log_term(&self) -> u64 {
        self.log
            .last()
            .filter(|entry| entry.index > self.snapshot_index)
            .map(|entry| entry.term)
            .unwrap_or(self.snapshot_term)
    }

    pub(crate) fn log_position(&self, index: u64) -> Option<usize> {
        self.log
            .binary_search_by_key(&index, |entry| entry.index)
            .ok()
    }

    pub(crate) fn log_entry_at(&self, index: u64) -> Option<&KvLogEntry> {
        self.log_position(index)
            .and_then(|position| self.log.get(position))
    }

    pub(crate) fn log_entry_at_mut(&mut self, index: u64) -> Option<&mut KvLogEntry> {
        let position = self.log_position(index)?;
        self.log.get_mut(position)
    }

    pub(crate) fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        if index == self.snapshot_index {
            return Some(self.snapshot_term);
        }
        self.log_entry_at(index).map(|entry| entry.term)
    }

    pub(crate) fn read_only_append_entries_response(
        &self,
        request: &RaftAppendEntriesRequest,
    ) -> Option<RaftAppendEntriesResponse> {
        if request.term < self.hard_state.current_term {
            return Some(RaftAppendEntriesResponse {
                term: self.hard_state.current_term,
                success: false,
                match_index: self.last_log_index(),
            });
        }

        if request.term != self.hard_state.current_term
            || self.hard_state.leader_id.as_ref() != Some(&request.leader_id)
            || !request.entries.is_empty()
        {
            return None;
        }

        if self.term_at(request.prev_log_index) != Some(request.prev_log_term) {
            return Some(RaftAppendEntriesResponse {
                term: self.hard_state.current_term,
                success: false,
                match_index: self.last_log_index(),
            });
        }

        if request.leader_commit > self.hard_state.commit_index {
            return None;
        }

        Some(RaftAppendEntriesResponse {
            term: self.hard_state.current_term,
            success: true,
            match_index: request.prev_log_index,
        })
    }

    /// Loads persisted WAL entries on open. Entries beyond the installed snapshot
    /// are appended (and later applied) as normal; already-applied entries are kept
    /// in the log as a contiguous replication tail (not re-applied — they are
    /// reflected in the snapshot/store). Any valid-but-non-contiguous record marks
    /// the remaining WAL tail corrupt so open can rewrite the safe prefix instead
    /// of bricking recovery on a `LogGap`.
    pub(crate) fn load_persisted_entries(&mut self, entries: Vec<KvLogEntry>) -> bool {
        let mut truncated = false;
        for entry in entries {
            if entry.index == 0 {
                truncated = true;
                break;
            }
            if entry.index > self.applied_index {
                if let Err(error) = self.append_entries(vec![entry]) {
                    eprintln!(
                        "warning: truncating cluster kv WAL tail at invalid persisted record: {error}"
                    );
                    truncated = true;
                    break;
                }
                continue;
            }
            if let Ok(existing_position) = self
                .log
                .binary_search_by_key(&entry.index, |existing| existing.index)
            {
                let existing = &self.log[existing_position];
                if existing.term != entry.term
                    || existing.request_id != entry.request_id
                    || existing.command != entry.command
                {
                    // Conflict replacement is append-only in the live WAL. If a
                    // snapshot becomes durable before compaction rewrites that WAL,
                    // recovery can see both the superseded entry and its replacement
                    // below applied_index. Replay must preserve the later record just
                    // as AppendEntries did, then rewrite the canonical tail.
                    self.log.truncate(existing_position);
                    self.log.push(entry);
                    truncated = true;
                }
                continue;
            }
            let contiguous = match self.log.last() {
                Some(last) => entry.index == last.index.saturating_add(1),
                None => true,
            };
            if contiguous {
                self.log.push(entry);
            } else {
                truncated = true;
                break;
            }
        }
        self.log.sort_by_key(|entry| entry.index);
        // Snapshot publication and WAL replacement are separate durability
        // steps. After a crash the WAL can still describe the superseded log.
        // Reconcile only after processing all append-only conflict replacements:
        // a later WAL record may supply the matching snapshot boundary.
        if self.snapshot_index > 0 {
            match self.log_entry_at(self.snapshot_index) {
                Some(entry) if entry.term != self.snapshot_term => {
                    self.log.clear();
                    truncated = true;
                }
                None => {
                    // A WAL ending before the installed boundary cannot prove
                    // a replication prefix. A rewritten WAL may instead start
                    // with the valid suffix immediately after that boundary.
                    let before = self.log.len();
                    self.log.retain(|entry| entry.index > self.snapshot_index);
                    truncated |= self.log.len() != before;
                }
                Some(_) => {}
            }
        }
        let second_pending_config_index = self
            .log
            .iter()
            .filter(|entry| {
                entry.index > self.hard_state.commit_index
                    && command_may_mutate_cluster_raft_config(&entry.command)
            })
            .nth(1)
            .map(|entry| entry.index);
        if let Some(second_pending_config_index) = second_pending_config_index {
            eprintln!(
                "error: preserving inconsistent cluster KV WAL suffix with multiple uncommitted membership changes at index {second_pending_config_index}; local leadership is disabled until Raft reconciliation"
            );
            // Either durable entry may already exist on voters. Unilateral
            // restart truncation would lose log freshness and request identity;
            // retain both for AppendEntries conflict reconciliation, but never
            // continue claiming local authority over the inconsistent suffix.
            self.hard_state.leader_id = None;
        }
        truncated
    }

    pub(crate) fn append_entries(
        &mut self,
        entries: Vec<KvLogEntry>,
    ) -> Result<Vec<KvLogEntry>, KvError> {
        let mut appended = Vec::new();
        for entry in entries {
            validate_kv_command_payload(&entry.command)?;
            if entry.index <= self.applied_index {
                if let Some(existing) = self.log_entry_at(entry.index)
                    && (existing.term != entry.term
                        || existing.request_id != entry.request_id
                        || existing.command != entry.command)
                {
                    return Err(KvError::ConflictingLogEntry { index: entry.index });
                }
                continue;
            }
            match self
                .log
                .binary_search_by_key(&entry.index, |candidate| candidate.index)
            {
                Ok(existing_position) if self.log[existing_position].term != entry.term => {
                    self.log.truncate(existing_position);
                    try_reserve_kv_items(&mut self.log, 1, "append raft log entry")?;
                    self.log
                        .push(try_clone_kv_log_entry(&entry, "append raft log entry")?);
                    try_reserve_kv_items(&mut appended, 1, "record appended raft entry")?;
                    appended.push(entry);
                }
                Ok(existing_position)
                    if self.log[existing_position].request_id != entry.request_id
                        || self.log[existing_position].command != entry.command =>
                {
                    return Err(KvError::ConflictingLogEntry { index: entry.index });
                }
                Ok(_) => {}
                Err(_) => {
                    let expected = self.last_log_index().saturating_add(1);
                    if entry.index != expected {
                        return Err(KvError::LogGap {
                            expected,
                            actual: entry.index,
                        });
                    }
                    try_reserve_kv_items(&mut self.log, 1, "append raft log entry")?;
                    self.log
                        .push(try_clone_kv_log_entry(&entry, "append raft log entry")?);
                    try_reserve_kv_items(&mut appended, 1, "record appended raft entry")?;
                    appended.push(entry);
                }
            }
        }
        Ok(appended)
    }

    fn validate_single_pending_config_after_append(
        &self,
        incoming: &[KvLogEntry],
        resulting_commit_index: u64,
    ) -> Result<(), KvError> {
        let truncate_from = incoming.iter().find_map(|entry| {
            self.log_entry_at(entry.index)
                .filter(|existing| existing.term != entry.term)
                .map(|_| entry.index)
        });
        let mut config_indices = BTreeSet::new();
        for entry in self.log.iter().filter(|entry| {
            entry.index > resulting_commit_index
                && truncate_from.is_none_or(|truncate_from| entry.index < truncate_from)
                && !incoming
                    .iter()
                    .any(|candidate| candidate.index == entry.index)
        }) {
            if command_may_mutate_cluster_raft_config(&entry.command) {
                config_indices.insert(entry.index);
            }
        }
        for entry in incoming
            .iter()
            .filter(|entry| entry.index > resulting_commit_index)
        {
            if command_may_mutate_cluster_raft_config(&entry.command) {
                config_indices.insert(entry.index);
            }
        }
        if config_indices.len() > 1 {
            return Err(KvError::InvalidRaftConfigTransition {
                detail: format!(
                    "Raft append would retain multiple uncommitted cluster membership changes at indexes {}",
                    config_indices
                        .iter()
                        .map(u64::to_string)
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            });
        }
        Ok(())
    }

    pub(crate) fn ensure_no_uncommitted_tail(&self) -> Result<(), KvError> {
        let commit_index = self.hard_state.commit_index.max(self.applied_index);
        let last_log_index = self.last_log_index();
        if last_log_index > commit_index {
            return Err(KvError::RaftLogTruncationDenied {
                commit_index,
                last_log_index,
            });
        }
        Ok(())
    }

    pub(crate) fn prepare_append_entries(
        &mut self,
        request: RaftAppendEntriesRequest,
    ) -> Result<RaftAppendEntriesPreparation, KvError> {
        let RaftAppendEntriesRequest {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        } = request;
        if term < self.hard_state.current_term {
            return Ok(RaftAppendEntriesPreparation {
                response: RaftAppendEntriesResponse {
                    term: self.hard_state.current_term,
                    success: false,
                    match_index: self.last_log_index(),
                },
                appended_entries: Vec::new(),
                commit_index: None,
            });
        }
        if term > self.hard_state.current_term {
            self.hard_state.voted_for = None;
        }
        if term == self.hard_state.current_term
            && self
                .hard_state
                .leader_id
                .as_ref()
                .is_some_and(|known_leader| known_leader != &leader_id)
        {
            return Ok(RaftAppendEntriesPreparation {
                response: RaftAppendEntriesResponse {
                    term: self.hard_state.current_term,
                    success: false,
                    match_index: self.last_log_index(),
                },
                appended_entries: Vec::new(),
                commit_index: None,
            });
        }
        self.hard_state.current_term = term;
        self.hard_state.leader_id = Some(leader_id);
        if self.term_at(prev_log_index) != Some(prev_log_term) {
            return Ok(RaftAppendEntriesPreparation {
                response: RaftAppendEntriesResponse {
                    term: self.hard_state.current_term,
                    success: false,
                    match_index: self.last_log_index(),
                },
                appended_entries: Vec::new(),
                commit_index: None,
            });
        }
        // Only the prefix matched by this request is safe to acknowledge or
        // commit. An empty heartbeat proves prev_log_index, not any extra
        // uncommitted entries left over from an earlier leader.
        let matched_log_index = entries
            .last()
            .map(|entry| entry.index)
            .unwrap_or(prev_log_index);
        let commit_index = leader_commit.min(matched_log_index);
        self.validate_single_pending_config_after_append(
            &entries,
            self.hard_state.commit_index.max(commit_index),
        )?;
        let appended_entries = self.append_entries(entries)?;
        Ok(RaftAppendEntriesPreparation {
            response: RaftAppendEntriesResponse {
                term: self.hard_state.current_term,
                success: true,
                match_index: matched_log_index,
            },
            appended_entries,
            commit_index: Some(commit_index),
        })
    }

    pub(crate) fn handle_append_entries(
        &mut self,
        request: RaftAppendEntriesRequest,
    ) -> Result<RaftAppendEntriesOutcome, KvError> {
        let preparation = self.prepare_append_entries(request)?;
        if let Some(commit_index) = preparation.commit_index {
            self.commit_to(commit_index)?;
        }
        Ok(RaftAppendEntriesOutcome {
            response: preparation.response,
        })
    }

    pub(crate) fn handle_request_vote(
        &mut self,
        request: RaftRequestVoteRequest,
    ) -> Result<RaftRequestVoteResponse, KvError> {
        if request.term < self.hard_state.current_term {
            return Ok(RaftRequestVoteResponse {
                term: self.hard_state.current_term,
                vote_granted: false,
            });
        }
        if request.term > self.hard_state.current_term {
            self.hard_state.current_term = request.term;
            self.hard_state.voted_for = None;
            self.hard_state.leader_id = None;
        }
        let candidate_log_is_current = (request.last_log_term, request.last_log_index)
            >= (self.last_log_term(), self.last_log_index());
        let can_vote = self
            .hard_state
            .voted_for
            .as_ref()
            .is_none_or(|voted_for| voted_for == &request.candidate_id);
        let vote_granted = can_vote && candidate_log_is_current;
        if vote_granted {
            self.hard_state.voted_for = Some(request.candidate_id);
            self.hard_state.leader_id = None;
        }
        Ok(RaftRequestVoteResponse {
            term: self.hard_state.current_term,
            vote_granted,
        })
    }

    pub(crate) fn observe_term(&mut self, term: u64) -> bool {
        if term <= self.hard_state.current_term {
            return false;
        }
        self.hard_state.current_term = term;
        self.hard_state.voted_for = None;
        self.hard_state.leader_id = None;
        true
    }

    pub(crate) fn commit_to(&mut self, commit_index: u64) -> Result<(), KvError> {
        if commit_index <= self.applied_index {
            self.hard_state.commit_index = self.hard_state.commit_index.max(commit_index);
            return Ok(());
        }
        let entries = self.entries_to_commit(commit_index)?;
        self.preflight_commit_entries(entries.as_slice())?;
        self.apply_preflighted_commit(entries, commit_index)
    }

    pub(crate) fn entries_to_commit(&self, commit_index: u64) -> Result<CommitEntries, KvError> {
        let last_log_index = self.last_log_index();
        if commit_index > last_log_index {
            return Err(KvError::MissingCommittedEntry {
                index: last_log_index.saturating_add(1),
            });
        }
        let first_unapplied_index = self.applied_index.saturating_add(1);
        if commit_index == first_unapplied_index {
            let entry =
                self.log_entry_at(commit_index)
                    .cloned()
                    .ok_or(KvError::MissingCommittedEntry {
                        index: commit_index,
                    })?;
            return Ok(CommitEntries::Single(entry));
        }
        let start = self
            .log
            .partition_point(|entry| entry.index <= self.applied_index);
        let end = self
            .log
            .partition_point(|entry| entry.index <= commit_index);
        let mut entries = Vec::new();
        try_reserve_kv_items(
            &mut entries,
            end.saturating_sub(start),
            "collect commit entries",
        )?;
        for entry in &self.log[start..end] {
            entries.push(try_clone_kv_log_entry(entry, "collect commit entries")?);
        }
        let mut expected_index = self.applied_index.saturating_add(1);
        for entry in &entries {
            if entry.index != expected_index {
                return Err(KvError::MissingCommittedEntry {
                    index: expected_index,
                });
            }
            expected_index = expected_index.saturating_add(1);
        }
        if expected_index != commit_index.saturating_add(1) {
            return Err(KvError::MissingCommittedEntry {
                index: expected_index,
            });
        }
        Ok(CommitEntries::Batch(entries))
    }

    pub(crate) fn preflight_commit_entries(&self, entries: &[KvLogEntry]) -> Result<(), KvError> {
        match entries {
            [] => Ok(()),
            [entry] => self.preflight_commit_entry(entry),
            _ => {
                let mut pending_requests = BTreeMap::<KvRequestId, KvCommandDigest>::new();
                let mut overlay = KvPreflightOverlay::from_store(&self.store)?;
                for entry in entries {
                    if self.store.request_id_expired(&entry.request_id) {
                        return Err(KvError::RequestExpired {
                            request_id: entry.request_id.clone(),
                        });
                    }
                    let digest = kv_command_digest(&entry.command)?;
                    if let Some(existing) = self.store.committed_request(&entry.request_id) {
                        if existing.command_digest != digest {
                            return Err(KvError::RequestIdConflict {
                                request_id: entry.request_id.clone(),
                            });
                        }
                        continue;
                    }
                    if let Some(existing) = pending_requests.get(&entry.request_id) {
                        if *existing != digest {
                            return Err(KvError::RequestIdConflict {
                                request_id: entry.request_id.clone(),
                            });
                        }
                        continue;
                    }
                    pending_requests.insert(entry.request_id.clone(), digest);
                    self.store
                        .preflight_apply_command_with_overlay(&entry.command, &mut overlay)?;
                }
                Ok(())
            }
        }
    }

    /// Checks the request identity for a current-term no-op without replaying
    /// the inherited state-machine suffix. Barriers carry no KV mutation, but
    /// still participate in the durable idempotency contract.
    pub(crate) fn preflight_local_barrier_request(
        &self,
        request: &KvCommandRequest,
    ) -> Result<(), KvError> {
        debug_assert!(matches!(&request.command, KvCommand::Barrier));
        if self.store.request_id_expired(&request.request_id) {
            return Err(KvError::RequestExpired {
                request_id: request.request_id.clone(),
            });
        }
        let digest = request.digest()?;
        if self
            .store
            .committed_request(&request.request_id)
            .is_some_and(|existing| existing.command_digest != digest)
        {
            return Err(KvError::RequestIdConflict {
                request_id: request.request_id.clone(),
            });
        }
        if let Some(existing) = self
            .log
            .iter()
            .find(|entry| entry.request_id == request.request_id)
        {
            if existing.command != request.command {
                return Err(KvError::RequestIdConflict {
                    request_id: request.request_id.clone(),
                });
            }
            return Err(KvError::AuthorityUnavailable {
                detail: format!(
                    "Raft request {} already has an uncommitted entry at index {}",
                    request.request_id, existing.index
                ),
            });
        }
        Ok(())
    }

    pub(crate) fn pending_current_term_barrier(&self) -> Result<Option<KvLogEntry>, KvError> {
        let mut barriers = self.log.iter().filter(|entry| {
            entry.index > self.applied_index
                && entry.term == self.hard_state.current_term
                && matches!(&entry.command, KvCommand::Barrier)
        });
        let Some(barrier) = barriers.next() else {
            return Ok(None);
        };
        if let Some(duplicate) = barriers.next() {
            return Err(KvError::AuthorityUnavailable {
                detail: format!(
                    "multiple uncommitted Raft recovery barriers exist in term {} at indexes {} and {}",
                    self.hard_state.current_term, barrier.index, duplicate.index
                ),
            });
        }
        Ok(Some(try_clone_kv_log_entry(
            barrier,
            "discover pending current-term Raft barrier",
        )?))
    }

    /// Validates a fresh local proposal against the state that will exist when
    /// its complete inherited Raft prefix is committed.
    ///
    /// The common case has no unapplied suffix and retains the direct,
    /// command-sized preflight. After an ambiguous proposal survives a leader
    /// change, however, the next proposal can only be checked correctly by
    /// previewing the retained suffix and the candidate in log order. The
    /// preview is exceptional and bounded by the durable WAL tail already
    /// retained by this node.
    pub(crate) fn preflight_local_append_request(
        &self,
        _term: u64,
        request: &KvCommandRequest,
    ) -> Result<(), KvError> {
        let last_log_index = self.last_log_index();
        if last_log_index <= self.applied_index {
            if self.store.request_id_expired(&request.request_id) {
                return Err(KvError::RequestExpired {
                    request_id: request.request_id.clone(),
                });
            }
            let digest = request.digest()?;
            if self
                .store
                .committed_request(&request.request_id)
                .is_some_and(|existing| existing.command_digest != digest)
            {
                return Err(KvError::RequestIdConflict {
                    request_id: request.request_id.clone(),
                });
            }
            return self.store.preflight_apply_command(&request.command);
        }

        let digest = request.digest()?;
        if self.store.request_id_expired(&request.request_id) {
            return Err(KvError::RequestExpired {
                request_id: request.request_id.clone(),
            });
        }
        if let Some(existing) = self.store.committed_request(&request.request_id) {
            if existing.command_digest != digest {
                return Err(KvError::RequestIdConflict {
                    request_id: request.request_id.clone(),
                });
            }
            return Err(KvError::AuthorityUnavailable {
                detail: format!("KV request {} is already committed", request.request_id),
            });
        }

        let mut expected_index = self.applied_index.saturating_add(1);
        let mut pending_requests = BTreeMap::<KvRequestId, KvCommandDigest>::new();
        let mut overlay = KvPreflightOverlay::from_store(&self.store)?;
        for entry in self
            .log
            .iter()
            .filter(|entry| entry.index > self.applied_index)
        {
            if entry.index != expected_index {
                return Err(KvError::MissingCommittedEntry {
                    index: expected_index,
                });
            }
            expected_index = expected_index.saturating_add(1);
            let existing_digest = kv_command_digest(&entry.command)?;
            if let Some(committed) = self.store.committed_request(&entry.request_id) {
                if committed.command_digest != existing_digest {
                    return Err(KvError::RequestIdConflict {
                        request_id: entry.request_id.clone(),
                    });
                }
                continue;
            }
            if let Some(previous) = pending_requests.get(&entry.request_id) {
                if *previous != existing_digest {
                    return Err(KvError::RequestIdConflict {
                        request_id: entry.request_id.clone(),
                    });
                }
                continue;
            }
            pending_requests.insert(entry.request_id.clone(), existing_digest);
            self.store
                .preflight_apply_command_with_overlay(&entry.command, &mut overlay)?;

            if entry.request_id == request.request_id {
                if existing_digest != digest {
                    return Err(KvError::RequestIdConflict {
                        request_id: request.request_id.clone(),
                    });
                }
                return Err(KvError::AuthorityUnavailable {
                    detail: format!(
                        "Raft request {} already has an uncommitted entry at index {}",
                        request.request_id, entry.index
                    ),
                });
            }
        }
        if expected_index != last_log_index.saturating_add(1) {
            return Err(KvError::MissingCommittedEntry {
                index: expected_index,
            });
        }

        self.store
            .preflight_apply_command_with_overlay(&request.command, &mut overlay)
    }

    fn preflight_commit_entry(&self, entry: &KvLogEntry) -> Result<(), KvError> {
        if self.store.request_id_expired(&entry.request_id) {
            return Err(KvError::RequestExpired {
                request_id: entry.request_id.clone(),
            });
        }
        let digest = kv_command_digest(&entry.command)?;
        if let Some(existing) = self.store.committed_request(&entry.request_id) {
            if existing.command_digest != digest {
                return Err(KvError::RequestIdConflict {
                    request_id: entry.request_id.clone(),
                });
            }
            return Ok(());
        }
        self.store.preflight_apply_command(&entry.command)
    }

    pub(crate) fn apply_preflighted_commit(
        &mut self,
        entries: CommitEntries,
        commit_index: u64,
    ) -> Result<(), KvError> {
        match entries {
            CommitEntries::Single(entry) => self.apply_preflighted_commit_entry(entry)?,
            CommitEntries::Batch(entries) => {
                for entry in entries {
                    self.apply_preflighted_commit_entry(entry)?;
                }
            }
        }
        self.hard_state.commit_index = commit_index;
        Ok(())
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn apply_preflighted_commit_failing_after_for_test(
        &mut self,
        entries: CommitEntries,
        commit_index: u64,
        fail_after: usize,
    ) -> Result<(), KvError> {
        let mut applied_entries = 0_usize;
        let fail_if_requested = |applied_entries| {
            if applied_entries == fail_after {
                return Err(KvError::ResourceExhausted {
                    operation: "injected durable raft apply failure".to_string(),
                    requested_bytes: usize::MAX,
                });
            }
            Ok(())
        };
        match entries {
            CommitEntries::Single(entry) => {
                self.apply_preflighted_commit_entry(entry)?;
                applied_entries = 1;
                fail_if_requested(applied_entries)?;
            }
            CommitEntries::Batch(entries) => {
                for entry in entries {
                    self.apply_preflighted_commit_entry(entry)?;
                    applied_entries = applied_entries.saturating_add(1);
                    fail_if_requested(applied_entries)?;
                }
            }
        }
        self.hard_state.commit_index = commit_index;
        Ok(())
    }

    pub(crate) fn apply_preflighted_commit_entry(
        &mut self,
        entry: KvLogEntry,
    ) -> Result<(), KvError> {
        let digest = kv_command_digest(&entry.command)?;
        let (revision, txn_response) = if let Some(existing) =
            self.store.committed_request(&entry.request_id)
        {
            if existing.command_digest != digest {
                return Err(KvError::RequestIdConflict {
                    request_id: entry.request_id,
                });
            }
            (existing.revision, existing.txn_response.clone())
        } else {
            let (revision, txn_response) = self.store.apply_command_with_response(entry.command)?;
            self.store
                .record_committed_request(KvCommittedRequestRecord {
                    request_id: entry.request_id.clone(),
                    command_digest: digest,
                    term: entry.term,
                    log_index: entry.index,
                    revision,
                    txn_response: txn_response.clone(),
                })?;
            (revision, txn_response)
        };
        if let Some(log_entry) = self.log_entry_at_mut(entry.index) {
            log_entry.revision = revision;
        }
        if let Some(response) = txn_response {
            self.txn_responses.try_insert(entry.index, response)?;
        }
        self.applied_index = entry.index;
        Ok(())
    }

    /// Compacts the in-memory log around the already-applied state, keeping the
    /// most recent `retain` committed entries so slightly-behind followers can be
    /// replicated incrementally rather than via a full snapshot install. The store,
    /// applied_index and commit_index are unchanged (this is local compaction, not
    /// installing a received snapshot).
    pub(crate) fn local_compact(&mut self, retain: u64) {
        let snapshot_index = self.applied_index;
        self.snapshot_term = self.term_at(snapshot_index).unwrap_or(self.snapshot_term);
        self.snapshot_index = snapshot_index;
        let cutoff = snapshot_index.saturating_sub(retain);
        self.log.retain(|entry| entry.index > cutoff);
        self.txn_responses.retain_after(cutoff);
    }

    pub(crate) fn install_snapshot(&mut self, snapshot: RaftSnapshot) -> Result<(), KvError> {
        let retain_suffix =
            self.term_at(snapshot.last_included_index) == Some(snapshot.last_included_term);
        self.store = InProcessKvStore::restore(Arc::unwrap_or_clone(snapshot.kv))?;
        self.snapshot_index = snapshot.last_included_index;
        self.snapshot_term = snapshot.last_included_term;
        self.applied_index = snapshot.last_included_index;
        self.hard_state.commit_index = snapshot.last_included_index;
        if retain_suffix {
            self.log
                .retain(|entry| entry.index > snapshot.last_included_index);
        } else {
            self.log.clear();
        }
        self.txn_responses.clear();
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct RaftCommitOutcome {
    pub(crate) revision: KvRevision,
    pub(crate) txn_response: Option<KvTxnResponse>,
}

#[derive(Clone, Debug)]
pub(crate) struct RaftAppendEntriesPreparation {
    pub(crate) response: RaftAppendEntriesResponse,
    pub(crate) appended_entries: Vec<KvLogEntry>,
    pub(crate) commit_index: Option<u64>,
}

#[derive(Clone, Debug)]
pub(crate) struct RaftAppendEntriesOutcome {
    pub(crate) response: RaftAppendEntriesResponse,
}
