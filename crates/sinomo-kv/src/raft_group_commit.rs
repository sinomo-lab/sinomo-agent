//! Bounded proposal preparation. Entries retain their original wire format and
//! request identities; only the physical persistence boundaries are shared.
use super::*;

pub const CLUSTER_KV_GROUP_COMMIT_MAX_ENTRIES: usize = 64;
pub const CLUSTER_KV_GROUP_COMMIT_MAX_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug)]
pub struct ClusterKvRaftProposal {
    pub state: ClusterBootstrapState,
    pub request: KvCommandRequest,
    pub deadline: Instant,
    cancelled: Option<Arc<std::sync::atomic::AtomicBool>>,
}

impl ClusterKvRaftProposal {
    pub fn new(state: ClusterBootstrapState, request: KvCommandRequest, deadline: Instant) -> Self {
        Self {
            state,
            request,
            deadline,
            cancelled: None,
        }
    }

    pub fn with_cancellation(mut self, cancelled: Arc<std::sync::atomic::AtomicBool>) -> Self {
        self.cancelled = Some(cancelled);
        self
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled
            .as_ref()
            .is_some_and(|flag| flag.load(Ordering::Acquire))
    }
}

#[derive(Clone, Debug)]
pub enum ClusterKvRaftPreparedProposal {
    Replayed(KvCommittedRequestRecord),
    Appended(KvLogEntry),
}

pub fn cluster_kv_raft_request_can_group(request: &KvCommandRequest) -> bool {
    !matches!(request.command, KvCommand::Barrier)
        && !command_may_mutate_cluster_raft_config(&request.command)
}

pub fn cluster_kv_raft_entry_group_bytes(entry: &KvLogEntry) -> usize {
    command_payload_bytes(&entry.command).saturating_add(256)
}

pub fn cluster_kv_raft_request_group_bytes(request: &KvCommandRequest) -> usize {
    command_payload_bytes(&request.command).saturating_add(256)
}

/// Prepares independent requests under one proposal turn. No staged entry is
/// visible outside resident ownership until the entire WAL batch is synced.
/// Membership changes and election barriers retain their individual path.
pub fn prepare_cluster_kv_raft_proposals(
    paths: &ClusterKvPaths,
    node_id: &NodeId,
    proposals: &[ClusterKvRaftProposal],
) -> Vec<Result<ClusterKvRaftPreparedProposal, KvError>> {
    if proposals.is_empty() {
        return Vec::new();
    }
    let bytes = proposals
        .iter()
        .map(|p| cluster_kv_raft_request_group_bytes(&p.request))
        .fold(0usize, usize::saturating_add);
    if proposals.len() > CLUSTER_KV_GROUP_COMMIT_MAX_ENTRIES
        || (proposals.len() > 1 && bytes > CLUSTER_KV_GROUP_COMMIT_MAX_BYTES)
        || proposals.iter().any(|p| {
            !cluster_kv_raft_request_can_group(&p.request) || &p.state.local_node_id != node_id
        })
    {
        return vec![
            Err(KvError::ResourceExhausted {
                operation: "prepare bounded Raft proposal group".into(),
                requested_bytes: bytes,
            });
            proposals.len()
        ];
    }
    let deadline = proposals.iter().map(|p| p.deadline).max().unwrap();
    let sequence = match lock_cluster_kv_commit_sequence(
        paths,
        deadline.saturating_duration_since(Instant::now()),
    ) {
        Ok(lock) => lock,
        Err(error) => return vec![Err(error); proposals.len()],
    };
    let mut storage = ClusterKvRaftLocalCommandStorage::new_with_operation_deadline(
        paths.raft_store(),
        node_id.clone(),
        cluster_kv_key_can_defer_materialized_snapshot as fn(&str) -> bool,
        deadline,
    );
    let results = storage.prepare_proposal_group(proposals, append_wal_entries);
    drop(sequence);
    results
}

impl<F: FnMut(&str) -> bool> ClusterKvRaftLocalCommandStorage<F> {
    /// The group owner has already made this exact entry durable. Subsequent
    /// replication/apply still revalidate its term, leader, and full identity.
    pub fn adopt_group_prepared_entry(&mut self, entry: KvLogEntry) {
        self.release_session_for_quorum();
        self.operation_deadline = None;
        self.pending_entry = Some(entry);
    }

    fn group_preflight(&mut self, proposal: &ClusterKvRaftProposal) -> Result<(), KvError> {
        self.operation_deadline = Some(proposal.deadline);
        self.remaining_operation_wait()?;
        if proposal.is_cancelled() {
            return Err(KvError::Busy {
                path: "cancelled Raft group admission".into(),
            });
        }
        if !self.local_is_leader(&proposal.state)? {
            return Err(KvError::AuthorityUnavailable {
                detail: "Raft group lost its expected leadership".into(),
            });
        }
        // Projection precedes queue admission. Revalidate its voter set against
        // resident committed authority after any intervening membership commit.
        if let Some(entry) = self.node_mut()?.store().get(&cluster_kv_raft_config_key()) {
            let config: ClusterKvRaftConfigRecord = entry.decode()?;
            let mut actual = config.voters;
            let mut expected = cluster_kv_raft_voter_ids(&proposal.state)?;
            actual.sort();
            expected.sort();
            if config.cluster_id != proposal.state.cluster_id || actual != expected {
                return Err(KvError::AuthorityUnavailable {
                    detail: "Raft group membership fence changed before append".into(),
                });
            }
        }
        self.preflight_uncommitted_request(&proposal.request)
    }

    fn prepare_proposal_group<W>(
        &mut self,
        proposals: &[ClusterKvRaftProposal],
        write_wal: W,
    ) -> Vec<Result<ClusterKvRaftPreparedProposal, KvError>>
    where
        W: FnOnce(&Path, &[KvLogEntry]) -> Result<(), KvError>,
    {
        let mut results = vec![None; proposals.len()];
        let mut eligible = Vec::new();
        for (i, p) in proposals.iter().enumerate() {
            self.operation_deadline = Some(p.deadline);
            let checked = self
                .replay_record_if_committed(&p.request)
                .and_then(|replay| {
                    if let Some(record) = replay {
                        return Ok(Some(record));
                    }
                    self.group_preflight(p)?;
                    Ok(None)
                });
            match checked {
                Ok(Some(record)) => {
                    results[i] = Some(Ok(ClusterKvRaftPreparedProposal::Replayed(record)))
                }
                Ok(None) => eligible.push(i),
                Err(error) => results[i] = Some(Err(error)),
            }
        }
        self.release_session_for_quorum();
        for (i, result) in results.iter().enumerate() {
            if let Some(Err(error)) = result {
                let proposal = &proposals[i];
                if !proposal.is_cancelled() && Instant::now() < proposal.deadline {
                    // The ledger preserves an older Submitted/Unknown attempt;
                    // a rejected retry never disproves its earlier delivery.
                    let _ = mark_cluster_kv_request_rejected_before_commit_before(
                        self.store.paths(),
                        &proposal.request,
                        error.to_string(),
                        proposal.deadline,
                    );
                }
            }
        }
        if eligible.is_empty() {
            return results.into_iter().map(Option::unwrap).collect();
        }
        let requests = eligible
            .iter()
            .map(|i| &proposals[*i].request)
            .collect::<Vec<_>>();
        let deadline = eligible
            .iter()
            .map(|i| proposals[*i].deadline)
            .min()
            .unwrap();
        let ledger_started = Instant::now();
        let receipts = crate::request::mark_cluster_kv_requests_submitted_together(
            self.store.paths(),
            &requests,
            deadline,
        );
        metrics::histogram!("sinomo.kv.group_commit.ledger.us").record(duration_us(ledger_started));
        let receipts = match receipts {
            Ok(receipts) => receipts,
            Err(error) => {
                for i in eligible {
                    results[i] = Some(Err(error.clone()));
                }
                return results.into_iter().map(Option::unwrap).collect();
            }
        };
        let mut entries = Vec::new();
        let mut staged = Vec::new();
        let mut rejected = Vec::new();
        let mut retain_resident = None;
        let mut original_log_len = None;
        for (i, receipt) in eligible.into_iter().zip(receipts) {
            let receipt = match receipt {
                Ok(receipt) => receipt,
                Err(error) => {
                    results[i] = Some(Err(error));
                    continue;
                }
            };
            let p = &proposals[i];
            let checked = self
                .group_preflight(p)
                .and_then(|()| self.remaining_operation_wait().map(|_| ()));
            if let Err(error) = checked {
                rejected.push((i, receipt, error));
                continue;
            }
            let node = match self.node_mut() {
                Ok(node) => node,
                Err(error) => {
                    rejected.push((i, receipt, error));
                    continue;
                }
            };
            retain_resident.get_or_insert_with(|| node.begin_resident_mutation());
            original_log_len.get_or_insert(node.node.log.len());
            let before_len = node.node.log.len();
            let entry = KvLogEntry {
                index: node.last_log_index().saturating_add(1),
                term: p.state.term,
                request_id: p.request.request_id.clone(),
                command: p.request.command.clone(),
                revision: KvRevision::ZERO,
            };
            match node.node.append_entries(vec![entry.clone()]) {
                Ok(_) => {
                    entries.push(entry);
                    staged.push((i, receipt));
                }
                Err(error) => {
                    node.node.log.truncate(before_len);
                    rejected.push((i, receipt, error));
                }
            }
        }
        if staged
            .iter()
            .any(|(i, _)| Instant::now() >= proposals[*i].deadline)
        {
            let node = self.node.as_mut().expect("staged batch owns resident node");
            node.node.log.truncate(original_log_len.unwrap());
            node.finish_resident_mutation(retain_resident.unwrap_or(false));
            for (i, receipt) in staged.drain(..) {
                rejected.push((
                    i,
                    receipt,
                    KvError::Busy {
                        path: node.paths.wal.display().to_string(),
                    },
                ));
            }
            entries.clear();
        }
        let mut ambiguous = Vec::new();
        if !entries.is_empty() {
            // Admission has happened under the exclusive resident lease. Finish
            // persistence even if a deadline/caller expires during disk I/O.
            self.operation_deadline = None;
            let node = self
                .node
                .as_mut()
                .expect("staged entries retain resident ownership");
            let started = Instant::now();
            let persisted = write_wal(&node.paths.wal, &entries);
            metrics::histogram!("sinomo.kv.group_commit.wal.us").record(duration_us(started));
            metrics::histogram!("sinomo.kv.group_commit.entries").record(entries.len() as f64);
            match persisted {
                Ok(()) => {
                    node.wal_synced_in_process = true;
                    node.record_live_metrics();
                    node.finish_resident_mutation(retain_resident.unwrap_or(false));
                    for ((i, _), entry) in staged.into_iter().zip(entries) {
                        results[i] = Some(Ok(ClusterKvRaftPreparedProposal::Appended(entry)));
                    }
                }
                Err(error) => {
                    // A partial write/sync failure may have persisted a prefix.
                    // Never reuse the speculative log; cold replay owns recovery.
                    node.poisoned = true;
                    for (i, _) in staged {
                        results[i] = Some(Err(error.clone()));
                        ambiguous.push((i, error.to_string()));
                    }
                }
            }
        } else if let Some(retain) = retain_resident
            && let Some(node) = self.node.as_mut()
        {
            node.finish_resident_mutation(retain);
        }
        self.release_session_for_quorum();
        for (i, detail) in ambiguous {
            let _ =
                mark_cluster_kv_request_unknown(self.store.paths(), &proposals[i].request, detail);
        }
        for (i, receipt, error) in rejected {
            let closed = self.close_exact_submission_rejected_before_wal(
                &proposals[i].request,
                receipt.as_ref(),
                &error.to_string(),
            );
            results[i] = Some(Err(closed.err().unwrap_or(error)));
        }
        results.into_iter().map(Option::unwrap).collect()
    }
}

/// Extends a proven request only through bounded ordinary entries in its term.
/// Control entries remain owned by their configuration/barrier operation. The
/// event bound keeps the complete applied delta inside retained watch history.
pub(super) fn bounded_group_commit_index(
    node: &DurableRaftKvNode,
    index: u64,
    acknowledged: u64,
) -> u64 {
    let limit = acknowledged
        .min(node.last_log_index())
        .min(index.saturating_add(CLUSTER_KV_GROUP_COMMIT_MAX_ENTRIES as u64 - 1));
    if limit <= index || !node.wal_synced_in_process {
        return index;
    }
    let Some(requested) = node.log_entry_at(index) else {
        return index;
    };
    let mut events = 0usize;
    let mut bytes = 0usize;
    let mut target = index;
    for entry in node
        .log()
        .iter()
        .filter(|e| e.index > node.applied_index() && e.index <= limit)
    {
        let Some(bound) = simple_event_bound(&entry.command) else {
            break;
        };
        if command_may_mutate_cluster_raft_config(&entry.command)
            || (entry.index >= index && entry.term != requested.term)
        {
            break;
        }
        events = events.saturating_add(bound);
        bytes = bytes.saturating_add(cluster_kv_raft_entry_group_bytes(entry));
        if events > MAX_RETAINED_EVENTS || bytes > CLUSTER_KV_GROUP_COMMIT_MAX_BYTES {
            break;
        }
        target = target.max(entry.index);
    }
    target
}
fn simple_event_bound(command: &KvCommand) -> Option<usize> {
    fn simple(op: &KvOp) -> bool {
        matches!(op, KvOp::Put { .. } | KvOp::Delete { .. })
    }
    match command {
        KvCommand::Op(op) if simple(op) => Some(1),
        KvCommand::Txn(txn) if txn.success.iter().chain(&txn.failure).all(simple) => {
            Some(txn.success.len().max(txn.failure.len()))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn partial_group_wal_failure_preserves_durable_prefix_and_unknown_outcomes() {
        let root = sinomo_testdir::TestDir::new("partial-group-wal");
        let paths = ClusterKvPaths::new(root.path());
        let local = NodeId::from_legacy_named("partial_group_local").unwrap();
        let mut state = ClusterBootstrapState::initialized_locally(
            Id::from_legacy_named("partial_group_cluster").unwrap(),
            "group",
            local.clone(),
            "local",
            "pk",
            OffsetDateTime::now_utc(),
        );
        state.term = 7;
        state.members[0].capabilities = vec![
            sinomo_base::CapabilityName::new(sinomo_node::DAEMON_MAIN_RUN_CAPABILITY).unwrap(),
        ];
        state.members[0].allowed_capabilities = state.members[0].capabilities.clone();
        paths
            .raft_store()
            .open_node(&local)
            .unwrap()
            .become_leader(7)
            .unwrap();
        let proposals: Vec<_> = (0..2)
            .map(|i| {
                ClusterKvRaftProposal::new(
                    state.clone(),
                    KvCommandRequest::generated(KvCommand::Op(KvOp::Put {
                        key: format!("/partial/{i}"),
                        value: vec![1],
                        lease_id: None,
                    })),
                    Instant::now() + Duration::from_secs(5),
                )
            })
            .collect();
        let mut storage = paths.local_raft_command_storage(
            local.clone(),
            cluster_kv_key_can_defer_materialized_snapshot as fn(&str) -> bool,
        );
        let sequence = lock_cluster_kv_commit_sequence(&paths, Duration::from_secs(5)).unwrap();
        let results = storage.prepare_proposal_group(&proposals, |path, entries| {
            append_wal_entries(path, &entries[..1])?;
            Err(KvError::Io {
                path: path.display().to_string(),
                detail: "injected failure after durable partial batch".into(),
            })
        });
        drop(sequence);
        drop(storage);
        assert!(results.iter().all(Result::is_err));
        for p in &proposals {
            assert!(matches!(
                crate::request::read_cluster_kv_request_attempt(&paths, &p.request.request_id)
                    .unwrap(),
                Some(KvRequestOutcome::Unknown { .. })
            ));
        }
        let mut node = paths.raft_store().open_node(&local).unwrap();
        assert_eq!(
            node.last_log_index(),
            1,
            "poisoned resident must reload the actual WAL prefix"
        );
        assert_eq!(
            node.log_entry_at(1).unwrap().request_id,
            proposals[0].request.request_id
        );
        let next = node
            .append_local_command(
                7,
                KvCommand::Op(KvOp::Put {
                    key: "/later".into(),
                    value: vec![2],
                    lease_id: None,
                }),
            )
            .unwrap();
        assert_eq!(next.index, 2);
        node.commit_to(2).unwrap();
        assert!(
            node.store()
                .committed_request(&proposals[0].request.request_id)
                .is_some()
        );
        assert!(
            node.store()
                .committed_request(&proposals[1].request.request_id)
                .is_none()
        );
    }
}
