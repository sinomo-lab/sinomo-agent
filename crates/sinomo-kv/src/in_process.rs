use crate::internal::*;

#[derive(Clone, Copy, Debug)]
pub(crate) struct SimulatedKeyState {
    pub(crate) present: bool,
    pub(crate) lease_id: Option<LeaseId>,
    pub(crate) value_hash: Option<blake3::Hash>,
}

impl SimulatedKeyState {
    const ABSENT: Self = Self {
        present: false,
        lease_id: None,
        value_hash: None,
    };

    fn present_with_value(lease_id: Option<LeaseId>, value: &[u8]) -> Self {
        Self {
            present: true,
            lease_id,
            value_hash: Some(blake3::hash(value)),
        }
    }
}

pub(crate) fn simulated_put_is_no_effect(
    old_state: &SimulatedKeyState,
    value: &[u8],
    lease_id: Option<LeaseId>,
) -> bool {
    old_state.present
        && old_state.lease_id == lease_id
        && old_state.value_hash == Some(blake3::hash(value))
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
}

impl SimulatedKeyStateIndex {
    fn get(&self, key: &str) -> Option<SimulatedKeyState> {
        self.entries.get(key).copied()
    }

    fn try_insert(
        &mut self,
        key: &str,
        state: SimulatedKeyState,
        operation: &str,
    ) -> Result<(), KvError> {
        self.entries
            .insert(try_clone_kv_string(key, operation)?, state);
        Ok(())
    }

    fn iter(&self) -> impl Iterator<Item = (&str, &SimulatedKeyState)> {
        self.entries
            .iter()
            .map(|(key, state)| (key.as_str(), state))
    }

    fn iter_mut(&mut self) -> impl Iterator<Item = (&str, &mut SimulatedKeyState)> {
        self.entries
            .iter_mut()
            .map(|(key, state)| (key.as_str(), state))
    }

    fn values_mut(&mut self) -> impl Iterator<Item = &mut SimulatedKeyState> {
        self.entries.values_mut()
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

    fn try_insert(
        &mut self,
        entry: KvEntry,
        operation: &str,
    ) -> Result<Option<KvEntry>, KvError> {
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

    fn try_insert(
        &mut self,
        lease: KvLease,
        operation: &str,
    ) -> Result<Option<KvLease>, KvError> {
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
        for (_, lease_ids) in self
            .deadlines
            .range(..=lease_clock)
        {
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

    fn try_add(
        &mut self,
        watch_id: WatchId,
        prefix: &str,
        operation: &str,
    ) -> Result<(), KvError> {
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

#[derive(Clone, Debug, Default)]
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
}

impl InProcessKvStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn revision(&self) -> KvRevision {
        self.revision
    }

    pub fn compacted_revision(&self) -> KvRevision {
        self.compacted_revision
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
        let entries = self.range_prefix_limited_by_payload_bytes(prefix, max_entries, max_payload_bytes)?;
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
            .checked_mul(std::mem::size_of::<KvOpResponse>().max(1))
            .unwrap_or(usize::MAX);
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
            .checked_mul(std::mem::size_of::<KvOpResponse>().max(1))
            .unwrap_or(usize::MAX);
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
                self.apply_op(op)?;
                Ok((self.revision, None))
            }
            KvCommand::Txn(txn) => {
                let response = self.txn(txn)?;
                Ok((response.revision, Some(response)))
            }
        }
    }

    fn preflight_apply_command(&self, command: &KvCommand) -> Result<(), KvError> {
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

    pub fn try_snapshot(&self) -> Result<KvSnapshot, KvError> {
        let mut entries = Vec::new();
        try_reserve_kv_items(&mut entries, self.entries.len(), "build kv snapshot entries")?;
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
        })
    }

    pub fn restore(snapshot: KvSnapshot) -> Result<Self, KvError> {
        let mut entries = KvEntryIndex::default();
        for entry in snapshot.entries {
            entries.try_insert(entry, "restore kv snapshot entries")?;
        }
        let lease_keys = build_lease_key_index(entries.values())?;
        let mut leases = KvLeaseIndex::default();
        for lease in snapshot.leases {
            leases.try_insert(lease, "restore kv snapshot leases")?;
        }
        let (lease_deadline_by_id, lease_deadlines) = build_lease_deadline_index(&leases, 0)?;
        let mut events = Vec::new();
        try_reserve_kv_items(&mut events, snapshot.events.len(), "restore kv snapshot events")?;
        for event in snapshot.events {
            events.push(Arc::new(event));
        }
        Ok(Self {
            revision: snapshot.revision,
            compacted_revision: snapshot.compacted_revision,
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
            } => self.apply_put(key, value, lease_id),
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
        let mut active_leases = SimulatedLeaseSet::from_leases(&self.leases)?;
        let mut touched_keys = SimulatedKeyStateIndex::default();
        let mut deleted_prefixes = Vec::<String>::new();
        let mut revoked_leases = Vec::<LeaseId>::new();
        let mut mutation_events = 0_usize;
        for op in ops {
            validate_kv_op_payload(op)?;
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
                        &touched_keys,
                        &deleted_prefixes,
                        &revoked_leases,
                    );
                    if old_state.lease_id != Some(*lease_id) {
                        let lease_keys = self.simulated_lease_key_count(
                            *lease_id,
                            &touched_keys,
                            &deleted_prefixes,
                            &revoked_leases,
                        );
                        if lease_keys >= MAX_KEYS_PER_LEASE {
                            return Err(KvError::LeaseKeyLimit {
                                lease_id: *lease_id,
                                observed_keys: lease_keys.saturating_add(1),
                                max_keys: MAX_KEYS_PER_LEASE,
                            });
                        }
                    }
                    if !simulated_put_is_no_effect(&old_state, value, Some(*lease_id)) {
                        record_kv_mutation_events(&mut mutation_events, 1)?;
                    }
                    touched_keys.try_insert(
                        key,
                        SimulatedKeyState::present_with_value(Some(*lease_id), value),
                        "track simulated touched key",
                    )?;
                }
                KvOp::Put {
                    key,
                    value,
                    lease_id: None,
                } => {
                    let old_state = self.simulated_key_state(
                        key,
                        &touched_keys,
                        &deleted_prefixes,
                        &revoked_leases,
                    );
                    if !simulated_put_is_no_effect(&old_state, value, None) {
                        record_kv_mutation_events(&mut mutation_events, 1)?;
                    }
                    touched_keys.try_insert(
                        key,
                        SimulatedKeyState::present_with_value(None, value),
                        "track simulated touched key",
                    )?;
                }
                KvOp::Delete { key } => {
                    if self
                        .simulated_key_state(key, &touched_keys, &deleted_prefixes, &revoked_leases)
                        .present
                    {
                        record_kv_mutation_events(&mut mutation_events, 1)?;
                    }
                    touched_keys.try_insert(
                        key,
                        SimulatedKeyState::ABSENT,
                        "track simulated touched key",
                    )?;
                }
                KvOp::DeletePrefix { prefix } => {
                    let deleted_count = self.simulated_delete_prefix_count(
                        prefix,
                        &touched_keys,
                        &deleted_prefixes,
                        &revoked_leases,
                        MAX_DELETE_PREFIX_KEYS,
                    )?;
                    record_kv_mutation_events(&mut mutation_events, deleted_count)?;
                    for (key, state) in touched_keys.iter_mut() {
                        if key.starts_with(prefix) {
                            *state = SimulatedKeyState::ABSENT;
                        }
                    }
                    try_reserve_kv_items(
                        &mut deleted_prefixes,
                        1,
                        "track simulated deleted prefix",
                    )?;
                    deleted_prefixes.push(try_clone_kv_string(
                        prefix,
                        "track simulated deleted prefix",
                    )?);
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
                    let deleted_count = self.simulated_lease_revoke_count(
                        *lease_id,
                        &touched_keys,
                        &deleted_prefixes,
                        &revoked_leases,
                    );
                    record_kv_mutation_events(&mut mutation_events, deleted_count)?;
                    active_leases.remove(lease_id);
                    for state in touched_keys.values_mut() {
                        if state.present && state.lease_id == Some(*lease_id) {
                            *state = SimulatedKeyState::ABSENT;
                        }
                    }
                    try_reserve_kv_items(
                        &mut revoked_leases,
                        1,
                        "track simulated revoked lease",
                    )?;
                    revoked_leases.push(*lease_id);
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
            if added_lease_key
                && let Some(lease_id) = lease_id
            {
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

    fn simulated_delete_prefix_count(
        &self,
        prefix: &str,
        touched_keys: &SimulatedKeyStateIndex,
        deleted_prefixes: &[String],
        revoked_leases: &[LeaseId],
        max_keys: usize,
    ) -> Result<usize, KvError> {
        let mut count = 0_usize;
        for entry in self.entries.prefix_entries(prefix) {
            if self
                .simulated_key_state(
                    &entry.key,
                    touched_keys,
                    deleted_prefixes,
                    revoked_leases,
                )
                .present
            {
                count = count.saturating_add(1);
                if count > max_keys {
                    return Err(KvError::DeletePrefixLimit {
                        prefix: prefix.to_string(),
                        observed_keys: count,
                        max_keys,
                    });
                }
            }
        }
        for (key, state) in touched_keys.iter() {
            if state.present && key.starts_with(prefix) && !self.entries.contains_key(key) {
                count = count.saturating_add(1);
                if count > max_keys {
                    return Err(KvError::DeletePrefixLimit {
                        prefix: prefix.to_string(),
                        observed_keys: count,
                        max_keys,
                    });
                }
            }
        }
        Ok(count)
    }

    fn simulated_lease_revoke_count(
        &self,
        lease_id: LeaseId,
        touched_keys: &SimulatedKeyStateIndex,
        deleted_prefixes: &[String],
        revoked_leases: &[LeaseId],
    ) -> usize {
        self.simulated_lease_key_count(lease_id, touched_keys, deleted_prefixes, revoked_leases)
    }

    fn simulated_lease_key_count(
        &self,
        lease_id: LeaseId,
        touched_keys: &SimulatedKeyStateIndex,
        deleted_prefixes: &[String],
        revoked_leases: &[LeaseId],
    ) -> usize {
        let mut count = 0_usize;
        if let Some(keys) = self.lease_keys.get(&lease_id) {
            for key in keys {
                let state =
                    self.simulated_key_state(key, touched_keys, deleted_prefixes, revoked_leases);
                if state.present && state.lease_id == Some(lease_id) {
                    count = count.saturating_add(1);
                }
            }
        }
        for (key, state) in touched_keys.iter() {
            if state.present
                && state.lease_id == Some(lease_id)
                && !self
                    .lease_keys
                    .get(&lease_id)
                    .is_some_and(|keys| keys.iter().any(|existing| existing == key))
            {
                count = count.saturating_add(1);
            }
        }
        count
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
        deleted_prefixes: &[String],
        revoked_leases: &[LeaseId],
    ) -> SimulatedKeyState {
        if let Some(state) = touched_keys.get(key) {
            return state;
        }
        if deleted_prefixes
            .iter()
            .any(|prefix| key.starts_with(prefix))
        {
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
        SimulatedKeyState::present_with_value(entry.lease_id, &entry.value)
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
            online.insert(try_clone_kv_node_id(&node_id, "initialize raft online node id")?);
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
        Ok(outcome.txn_response.unwrap_or_else(|| KvTxnResponse {
            succeeded: true,
            revision: outcome.revision,
            responses: Vec::new(),
        }))
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
                        kv: leader.store.try_snapshot()?,
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
            node.hard_state.voted_for =
                Some(try_clone_kv_node_id(&new_leader_id, "record transferred raft vote")?);
            node.hard_state.leader_id =
                Some(try_clone_kv_node_id(&new_leader_id, "record transferred raft leader")?);
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
                node.hard_state.leader_id =
                    Some(try_clone_kv_node_id(&candidate_id, "record elected raft leader")?);
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
            kv: snapshot,
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
    fn len(&self) -> usize {
        self.responses.len()
    }

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

    fn retain_applied_through(&mut self, committed: u64) {
        self.responses.retain(|index, _| *index <= committed);
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
    fn empty() -> Self {
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
    }

    pub(crate) fn last_log_term(&self) -> u64 {
        self.log
            .last()
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
            match_index: self.last_log_index(),
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
            if self
                .log
                .iter()
                .any(|existing| existing.index == entry.index)
            {
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
                    && (existing.term != entry.term || existing.command != entry.command)
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
                Ok(existing_position) if self.log[existing_position].command != entry.command => {
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

    pub(crate) fn discard_uncommitted_tail(&mut self) -> bool {
        let committed = self.hard_state.commit_index.max(self.applied_index);
        let old_log_len = self.log.len();
        let old_response_len = self.txn_responses.len();
        self.log.retain(|entry| entry.index <= committed);
        self.txn_responses.retain_applied_through(committed);
        old_log_len != self.log.len() || old_response_len != self.txn_responses.len()
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
        let appended_entries = self.append_entries(entries)?;
        let commit_index = leader_commit.min(self.last_log_index());
        Ok(RaftAppendEntriesPreparation {
            response: RaftAppendEntriesResponse {
                term: self.hard_state.current_term,
                success: true,
                match_index: self.last_log_index(),
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
        try_reserve_kv_items(&mut entries, end.saturating_sub(start), "collect commit entries")?;
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
            [entry] => self.store.preflight_apply_command(&entry.command),
            _ => {
                let mut store = self.store.clone();
                for entry in entries {
                    store.apply_command_with_response(entry.command.clone())?;
                }
                Ok(())
            }
        }
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

    pub(crate) fn apply_preflighted_commit_entry(
        &mut self,
        entry: KvLogEntry,
    ) -> Result<(), KvError> {
        let (revision, txn_response) = self.store.apply_command_with_response(entry.command)?;
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
        self.store = InProcessKvStore::restore(snapshot.kv)?;
        self.snapshot_index = snapshot.last_included_index;
        self.snapshot_term = snapshot.last_included_term;
        self.applied_index = snapshot.last_included_index;
        self.hard_state.commit_index = snapshot.last_included_index;
        self.log
            .retain(|entry| entry.index > snapshot.last_included_index);
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
