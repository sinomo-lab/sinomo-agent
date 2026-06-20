use crate::internal::*;

#[derive(Clone, Copy)]
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
#[derive(Clone, Debug, Default)]
pub struct InProcessKvStore {
    pub(crate) revision: KvRevision,
    pub(crate) compacted_revision: KvRevision,
    pub(crate) entries: BTreeMap<String, KvEntry>,
    pub(crate) events: Vec<Arc<KvEvent>>,
    pub(crate) leases: BTreeMap<LeaseId, KvLease>,
    pub(crate) lease_clock: u64,
    pub(crate) lease_deadline_by_id: BTreeMap<LeaseId, u64>,
    pub(crate) lease_deadlines: BTreeMap<u64, BTreeSet<LeaseId>>,
    pub(crate) lease_keys: BTreeMap<LeaseId, BTreeSet<String>>,
    pub(crate) watches: BTreeMap<WatchId, KvWatchState>,
    pub(crate) watch_prefixes: BTreeMap<String, BTreeSet<WatchId>>,
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

    pub fn range_prefix(&self, prefix: &str) -> Vec<KvEntry> {
        self.prefix_entries(prefix).cloned().collect()
    }

    pub fn prefix_entries<'a>(&'a self, prefix: &'a str) -> impl Iterator<Item = &'a KvEntry> + 'a {
        self.entries
            .range(prefix.to_string()..)
            .map_while(move |(key, entry)| key.starts_with(prefix).then_some(entry))
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
            entries.push(entry.clone());
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
        self.range_prefix(prefix)
            .iter()
            .map(KvEntry::decode)
            .collect()
    }

    pub fn range_prefix_serde_limited_by_payload_bytes<T: DeserializeOwned>(
        &self,
        prefix: &str,
        max_entries: usize,
        max_payload_bytes: usize,
    ) -> Result<Vec<T>, KvError> {
        self.range_prefix_limited_by_payload_bytes(prefix, max_entries, max_payload_bytes)?
            .iter()
            .map(KvEntry::decode)
            .collect()
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
        let mut responses = Vec::with_capacity(ops.len());
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

        let mut responses = Vec::with_capacity(ops.len());
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
        let backlog = self
            .events
            .iter()
            .filter(|event| event.revision >= start_revision && event.key.starts_with(&prefix))
            .cloned()
            .collect();
        let mut watch = KvWatchState {
            watch: KvWatch {
                watch_id,
                prefix,
                start_revision,
            },
            pending: backlog,
            compacted_revision: None,
        };
        compact_watch_pending(&mut watch);
        self.index_watch_prefix(watch_id, &watch.watch.prefix);
        self.watches.insert(watch_id, watch);
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
        Ok(watch
            .pending
            .drain(..)
            .map(|event| event.as_ref().clone())
            .collect())
    }

    pub fn cancel_watch(&mut self, watch_id: WatchId) -> Result<bool, KvError> {
        Ok(self.remove_watch(watch_id).is_some())
    }

    pub fn snapshot(&self) -> KvSnapshot {
        KvSnapshot {
            revision: self.revision,
            compacted_revision: self.compacted_revision,
            entries: self.entries.values().cloned().collect(),
            leases: self.current_leases_for_snapshot(),
            events: self
                .events
                .iter()
                .map(|event| event.as_ref().clone())
                .collect(),
        }
    }

    pub fn restore(snapshot: KvSnapshot) -> Self {
        let entries: BTreeMap<_, _> = snapshot
            .entries
            .into_iter()
            .map(|entry| (entry.key.clone(), entry))
            .collect();
        let lease_keys = build_lease_key_index(&entries);
        let leases: BTreeMap<_, _> = snapshot
            .leases
            .into_iter()
            .map(|lease| (lease.lease_id, lease))
            .collect();
        let (lease_deadline_by_id, lease_deadlines) = build_lease_deadline_index(&leases, 0);
        Self {
            revision: snapshot.revision,
            compacted_revision: snapshot.compacted_revision,
            entries,
            events: snapshot.events.into_iter().map(Arc::new).collect(),
            leases,
            lease_clock: 0,
            lease_deadline_by_id,
            lease_deadlines,
            lease_keys,
            watches: BTreeMap::new(),
            watch_prefixes: BTreeMap::new(),
            next_watch_id: 1,
        }
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
        let candidates = self.expired_lease_candidates();

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
            let (keys, complete) = self.lease_revoke_batch(lease_id, remaining_budget);
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
                self.upsert_lease(KvLease::new(lease_id, ttl_ticks));
                Ok(KvOpResponse::LeaseGrant { lease_id })
            }
            KvOp::LeaseKeepAlive { lease_id } => {
                let ttl_ticks = self
                    .leases
                    .get(&lease_id)
                    .ok_or(KvError::UnknownLease { lease_id })?
                    .ttl_ticks;
                self.upsert_lease(KvLease::new(lease_id, ttl_ticks));
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
        let mut active_leases = self.leases.keys().copied().collect::<BTreeSet<_>>();
        let mut touched_keys = BTreeMap::<String, SimulatedKeyState>::new();
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
                    touched_keys.insert(
                        key.clone(),
                        SimulatedKeyState::present_with_value(Some(*lease_id), value),
                    );
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
                    touched_keys.insert(
                        key.clone(),
                        SimulatedKeyState::present_with_value(None, value),
                    );
                }
                KvOp::Delete { key } => {
                    if self
                        .simulated_key_state(key, &touched_keys, &deleted_prefixes, &revoked_leases)
                        .present
                    {
                        record_kv_mutation_events(&mut mutation_events, 1)?;
                    }
                    touched_keys.insert(key.clone(), SimulatedKeyState::ABSENT);
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
                    for (key, state) in &mut touched_keys {
                        if key.starts_with(prefix) {
                            *state = SimulatedKeyState::ABSENT;
                        }
                    }
                    deleted_prefixes.push(prefix.clone());
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
                    active_leases.insert(*lease_id);
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
        }

        let revision = self.bump_revision();
        let old_lease_id = self.entries.get(&key).and_then(|entry| entry.lease_id);
        let entry = match self.entries.remove(&key) {
            Some(mut existing) => {
                existing.value = value.clone();
                existing.modify_revision = revision;
                existing.version = KvVersion(existing.version.0.saturating_add(1));
                existing.lease_id = lease_id;
                existing
            }
            None => KvEntry {
                key: key.clone(),
                value: value.clone(),
                create_revision: revision,
                modify_revision: revision,
                version: KvVersion(1),
                lease_id,
            },
        };
        if let Some(old_lease_id) = old_lease_id {
            self.remove_lease_key(old_lease_id, &key);
        }
        if let Some(lease_id) = lease_id {
            self.add_lease_key(lease_id, key.clone());
        }
        self.entries.insert(key.clone(), entry);
        self.record_event(KvEvent {
            revision,
            key: key.clone(),
            kind: KvEventKind::Put { value },
        });
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
            key: key.clone(),
            kind: KvEventKind::Delete,
        });
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
        for key in self.entries.range(prefix.to_string()..).map(|(key, _)| key) {
            if !key.starts_with(prefix) {
                break;
            }
            if keys.len() >= max_keys {
                return Err(KvError::DeletePrefixLimit {
                    prefix: prefix.to_string(),
                    observed_keys: max_keys.saturating_add(1),
                    max_keys,
                });
            }
            keys.push(key.clone());
        }
        Ok(keys)
    }

    fn simulated_delete_prefix_count(
        &self,
        prefix: &str,
        touched_keys: &BTreeMap<String, SimulatedKeyState>,
        deleted_prefixes: &[String],
        revoked_leases: &[LeaseId],
        max_keys: usize,
    ) -> Result<usize, KvError> {
        let mut count = 0_usize;
        for key in self.entries.range(prefix.to_string()..).map(|(key, _)| key) {
            if !key.starts_with(prefix) {
                break;
            }
            if self
                .simulated_key_state(key, touched_keys, deleted_prefixes, revoked_leases)
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
        for (key, state) in touched_keys {
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
        touched_keys: &BTreeMap<String, SimulatedKeyState>,
        deleted_prefixes: &[String],
        revoked_leases: &[LeaseId],
    ) -> usize {
        self.simulated_lease_key_count(lease_id, touched_keys, deleted_prefixes, revoked_leases)
    }

    fn simulated_lease_key_count(
        &self,
        lease_id: LeaseId,
        touched_keys: &BTreeMap<String, SimulatedKeyState>,
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
        for (key, state) in touched_keys {
            if state.present
                && state.lease_id == Some(lease_id)
                && !self
                    .lease_keys
                    .get(&lease_id)
                    .is_some_and(|keys| keys.contains(key))
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
                let mut entries = self.entries.range(prefix.clone()..);
                let has_matching_key = entries
                    .next()
                    .is_some_and(|(key, _)| key.starts_with(prefix));
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
        Ok(keys.iter().cloned().collect())
    }

    fn lease_revoke_batch(&self, lease_id: LeaseId, max_keys: usize) -> (Vec<String>, bool) {
        let Some(keys) = self.lease_keys.get(&lease_id) else {
            return (Vec::new(), true);
        };
        let complete = keys.len() <= max_keys;
        (keys.iter().take(max_keys).cloned().collect(), complete)
    }

    fn current_leases_for_snapshot(&self) -> Vec<KvLease> {
        self.leases
            .values()
            .map(|lease| {
                let mut lease = lease.clone();
                lease.remaining_ticks = self
                    .lease_remaining_ticks(lease.lease_id)
                    .unwrap_or(lease.remaining_ticks);
                lease
            })
            .collect()
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

    fn expired_lease_candidates(&self) -> Vec<LeaseId> {
        self.lease_deadlines
            .range(..=self.lease_clock)
            .flat_map(|(_, lease_ids)| lease_ids.iter().copied())
            .collect()
    }

    fn upsert_lease(&mut self, lease: KvLease) {
        let lease_id = lease.lease_id;
        self.remove_lease_deadline(lease_id);
        let deadline = self.lease_clock.saturating_add(lease.remaining_ticks);
        self.lease_deadline_by_id.insert(lease_id, deadline);
        self.lease_deadlines
            .entry(deadline)
            .or_default()
            .insert(lease_id);
        self.leases.insert(lease_id, lease);
    }

    fn remove_lease_deadline(&mut self, lease_id: LeaseId) -> Option<u64> {
        let deadline = self.lease_deadline_by_id.remove(&lease_id)?;
        let should_remove_deadline =
            if let Some(lease_ids) = self.lease_deadlines.get_mut(&deadline) {
                lease_ids.remove(&lease_id);
                lease_ids.is_empty()
            } else {
                false
            };
        if should_remove_deadline {
            self.lease_deadlines.remove(&deadline);
        }
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
        touched_keys: &BTreeMap<String, SimulatedKeyState>,
        deleted_prefixes: &[String],
        revoked_leases: &[LeaseId],
    ) -> SimulatedKeyState {
        if let Some(state) = touched_keys.get(key) {
            return *state;
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

    fn record_event(&mut self, event: KvEvent) {
        let event = Arc::new(event);
        for watch_id in self.matching_watch_ids_for_event_key(&event.key) {
            if let Some(watch) = self.watches.get_mut(&watch_id)
                && event.revision >= watch.watch.start_revision
            {
                watch.pending.push_back(Arc::clone(&event));
                compact_watch_pending(watch);
            }
        }
        self.events.push(event);
        self.trim_event_history();
    }

    fn index_watch_prefix(&mut self, watch_id: WatchId, prefix: &str) {
        self.watch_prefixes
            .entry(prefix.to_string())
            .or_default()
            .insert(watch_id);
    }

    fn remove_watch(&mut self, watch_id: WatchId) -> Option<KvWatchState> {
        let watch = self.watches.remove(&watch_id)?;
        self.remove_watch_prefix_index(watch_id, &watch.watch.prefix);
        Some(watch)
    }

    fn remove_watch_prefix_index(&mut self, watch_id: WatchId, prefix: &str) {
        let should_remove = if let Some(watch_ids) = self.watch_prefixes.get_mut(prefix) {
            watch_ids.remove(&watch_id);
            watch_ids.is_empty()
        } else {
            false
        };
        if should_remove {
            self.watch_prefixes.remove(prefix);
        }
    }

    fn matching_watch_ids_for_event_key(&self, key: &str) -> Vec<WatchId> {
        let mut watch_ids = Vec::new();
        if let Some(prefix_watch_ids) = self.watch_prefixes.get("") {
            watch_ids.extend(prefix_watch_ids.iter().copied());
        }
        for (start, ch) in key.char_indices() {
            let end = start + ch.len_utf8();
            if let Some(prefix_watch_ids) = self.watch_prefixes.get(&key[..end]) {
                watch_ids.extend(prefix_watch_ids.iter().copied());
            }
        }
        watch_ids
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn matching_watch_ids_for_event_key_for_test(&self, key: &str) -> Vec<WatchId> {
        let mut watch_ids = self.matching_watch_ids_for_event_key(key);
        watch_ids.sort();
        watch_ids
    }

    fn trim_event_history(&mut self) {
        self.compact_event_history_to(MAX_RETAINED_EVENTS);
    }

    fn add_lease_key(&mut self, lease_id: LeaseId, key: String) {
        self.lease_keys.entry(lease_id).or_default().insert(key);
    }

    fn remove_lease_key(&mut self, lease_id: LeaseId, key: &str) {
        let Some(keys) = self.lease_keys.get_mut(&lease_id) else {
            return;
        };
        keys.remove(key);
        let remove_empty = keys.is_empty();
        if remove_empty {
            self.lease_keys.remove(&lease_id);
        }
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

#[derive(Clone, Debug)]
pub struct InProcessRaftKvCluster {
    term: u64,
    leader_id: NodeId,
    voters: BTreeSet<NodeId>,
    online: BTreeSet<NodeId>,
    nodes: BTreeMap<NodeId, ReplicatedKvNode>,
    next_log_index: u64,
    commit_index: u64,
}

impl InProcessRaftKvCluster {
    pub fn new(voter_ids: Vec<NodeId>) -> Result<Self, KvError> {
        let Some(leader_id) = voter_ids.first().cloned() else {
            return Err(KvError::NoVoters);
        };
        let voters: BTreeSet<NodeId> = voter_ids.into_iter().collect();
        let online = voters.clone();
        let nodes = voters
            .iter()
            .cloned()
            .map(|node_id| {
                (
                    node_id.clone(),
                    ReplicatedKvNode {
                        store: InProcessKvStore::new(),
                        log: Vec::new(),
                        hard_state: RaftHardState::default(),
                        applied_index: 0,
                        snapshot_index: 0,
                        snapshot_term: 0,
                        txn_responses: BTreeMap::new(),
                    },
                )
            })
            .collect();
        Ok(Self {
            term: 1,
            leader_id,
            voters,
            online,
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

    pub fn set_node_online(&mut self, node_id: &NodeId, online: bool) {
        if online {
            self.online.insert(node_id.clone());
            let _ = self.catch_up_node(node_id);
        } else {
            self.online.remove(node_id);
        }
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
        if !self.online.contains(&self.leader_id) {
            return Err(KvError::LeaderOffline {
                leader_id: self.leader_id.clone(),
            });
        }
        let online_voters = self.voters.intersection(&self.online).count();
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
        let leader_id = self.leader_id.clone();
        self.node_mut(&leader_id)?
            .append_entries(vec![entry.clone()])?;
        let mut voter_acks = usize::from(self.voters.contains(&leader_id));

        for node_id in self.online.iter().cloned().collect::<Vec<_>>() {
            if node_id == leader_id {
                continue;
            }
            let ack = self.append_entries_to_follower(&node_id, vec![entry.clone()])?;
            if ack.success && self.voters.contains(&node_id) {
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
        let online_nodes = self.online.iter().cloned().collect::<Vec<_>>();
        for node_id in online_nodes {
            self.node_mut(&node_id)?.commit_to(index)?;
        }
        let leader = self.node(&leader_id)?;
        let committed = leader
            .log
            .iter()
            .find(|entry| entry.index == index)
            .ok_or(KvError::MissingCommittedEntry { index })?;
        let txn_response = leader.txn_responses.get(&index).cloned();
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
            leader_id: self.leader_id.clone(),
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
                node_id: node_id.clone(),
            });
        }
        if node_id == &self.leader_id {
            return Ok(());
        }
        let leader = self.node(&self.leader_id)?.clone();
        let follower = self.node_mut(node_id)?;
        if follower.last_log_index() < leader.snapshot_index {
            follower.install_snapshot(RaftSnapshot {
                last_included_index: leader.applied_index,
                last_included_term: leader
                    .term_at(leader.applied_index)
                    .unwrap_or(leader.term()),
                kv: leader.store.snapshot(),
            });
        }
        let missing = leader
            .log
            .iter()
            .filter(|entry| entry.index > follower.last_log_index())
            .cloned()
            .collect::<Vec<_>>();
        follower.append_entries(missing)?;
        follower.commit_to(leader.hard_state.commit_index)
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
        if !self.online.contains(&new_leader_id) {
            return Err(KvError::LeaderOffline {
                leader_id: new_leader_id,
            });
        }
        self.catch_up_node(&new_leader_id)?;
        self.term = self.term.saturating_add(1);
        self.leader_id = new_leader_id.clone();
        let term = self.term;
        for node in self.nodes.values_mut() {
            node.hard_state.current_term = term;
            node.hard_state.voted_for = Some(new_leader_id.clone());
            node.hard_state.leader_id = Some(new_leader_id.clone());
        }
        Ok(())
    }

    pub fn elect_leader(&mut self, candidate_id: NodeId) -> Result<bool, KvError> {
        if !self.voters.contains(&candidate_id) {
            return Err(KvError::UnknownNode {
                node_id: candidate_id,
            });
        }
        if !self.online.contains(&candidate_id) {
            return Err(KvError::LeaderOffline {
                leader_id: candidate_id,
            });
        }
        self.catch_up_node(&candidate_id)?;
        let candidate = self.node(&candidate_id)?.clone();
        let election_term = self.term.saturating_add(1);
        let request = RaftRequestVoteRequest {
            term: election_term,
            candidate_id: candidate_id.clone(),
            last_log_index: candidate.last_log_index(),
            last_log_term: candidate.last_log_term(),
        };
        let mut granted = 0;
        for voter_id in self
            .voters
            .intersection(&self.online)
            .cloned()
            .collect::<Vec<_>>()
        {
            let response = self.request_vote(&voter_id, request.clone())?;
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
        self.leader_id = candidate_id.clone();
        for node in self.nodes.values_mut() {
            if node.hard_state.current_term <= self.term {
                node.hard_state.current_term = self.term;
                node.hard_state.leader_id = Some(candidate_id.clone());
            }
        }
        Ok(true)
    }

    fn node(&self, node_id: &NodeId) -> Result<&ReplicatedKvNode, KvError> {
        self.nodes.get(node_id).ok_or_else(|| KvError::UnknownNode {
            node_id: node_id.clone(),
        })
    }

    fn node_mut(&mut self, node_id: &NodeId) -> Result<&mut ReplicatedKvNode, KvError> {
        self.nodes
            .get_mut(node_id)
            .ok_or_else(|| KvError::UnknownNode {
                node_id: node_id.clone(),
            })
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
        });
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
        self.nodes.insert(
            node_id.clone(),
            ReplicatedKvNode {
                store: InProcessKvStore::restore(snapshot),
                log: Vec::new(),
                hard_state: RaftHardState {
                    current_term: self.term,
                    voted_for: None,
                    leader_id: Some(self.leader_id.clone()),
                    commit_index: self.commit_index,
                },
                applied_index: self.commit_index,
                snapshot_index: self.commit_index,
                snapshot_term: self.term,
                txn_responses: BTreeMap::new(),
            },
        );
        self.online.insert(node_id);
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

#[derive(Clone, Debug)]
pub(crate) struct ReplicatedKvNode {
    pub(crate) store: InProcessKvStore,
    pub(crate) log: Vec<KvLogEntry>,
    pub(crate) hard_state: RaftHardState,
    pub(crate) applied_index: u64,
    pub(crate) snapshot_index: u64,
    pub(crate) snapshot_term: u64,
    pub(crate) txn_responses: BTreeMap<u64, KvTxnResponse>,
}

impl ReplicatedKvNode {
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
                    self.log.push(entry.clone());
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
                    self.log.push(entry.clone());
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
        self.txn_responses.retain(|index, _| *index <= committed);
        old_log_len != self.log.len() || old_response_len != self.txn_responses.len()
    }

    pub(crate) fn prepare_append_entries(
        &mut self,
        request: RaftAppendEntriesRequest,
    ) -> Result<RaftAppendEntriesPreparation, KvError> {
        if request.term < self.hard_state.current_term {
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
        if request.term > self.hard_state.current_term {
            self.hard_state.voted_for = None;
        }
        self.hard_state.current_term = request.term;
        self.hard_state.leader_id = Some(request.leader_id.clone());
        if self.term_at(request.prev_log_index) != Some(request.prev_log_term) {
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
        let appended_entries = self.append_entries(request.entries)?;
        let commit_index = request.leader_commit.min(self.last_log_index());
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
        let entries = self.log[start..end].to_vec();
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
            self.txn_responses.insert(entry.index, response);
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
        self.txn_responses.retain(|index, _| *index > cutoff);
    }

    pub(crate) fn install_snapshot(&mut self, snapshot: RaftSnapshot) {
        self.store = InProcessKvStore::restore(snapshot.kv);
        self.snapshot_index = snapshot.last_included_index;
        self.snapshot_term = snapshot.last_included_term;
        self.applied_index = snapshot.last_included_index;
        self.hard_state.commit_index = snapshot.last_included_index;
        self.log
            .retain(|entry| entry.index > snapshot.last_included_index);
        self.txn_responses.clear();
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
