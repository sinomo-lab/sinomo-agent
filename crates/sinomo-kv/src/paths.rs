use crate::internal::*;

pub const CLUSTER_KV_COMMIT_SEQUENCE_LOCK_WAIT: Duration = Duration::from_secs(30);

#[derive(Default)]
struct CommitSequenceAdmissionState {
    next_ticket: u64,
    serving_ticket: u64,
    cancelled_tickets: BTreeSet<u64>,
}

#[derive(Default)]
struct CommitSequenceAdmissionGate {
    state: Mutex<CommitSequenceAdmissionState>,
    changed: Condvar,
}

struct CommitSequenceAdmissionTurn {
    gate: Arc<CommitSequenceAdmissionGate>,
    ticket: u64,
}

static COMMIT_SEQUENCE_ADMISSION_GATES: LazyLock<
    Mutex<HashMap<PathBuf, std::sync::Weak<CommitSequenceAdmissionGate>>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));

fn commit_sequence_admission_gate(lock_path: &Path) -> Arc<CommitSequenceAdmissionGate> {
    let mut gates = COMMIT_SEQUENCE_ADMISSION_GATES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(gate) = gates.get(lock_path).and_then(std::sync::Weak::upgrade) {
        return gate;
    }
    gates.retain(|_, gate| gate.strong_count() > 0);
    let gate = Arc::new(CommitSequenceAdmissionGate::default());
    gates.insert(lock_path.to_path_buf(), Arc::downgrade(&gate));
    gate
}

fn advance_commit_sequence_admission(state: &mut CommitSequenceAdmissionState) {
    state.serving_ticket = state.serving_ticket.saturating_add(1);
    while state.cancelled_tickets.remove(&state.serving_ticket) {
        state.serving_ticket = state.serving_ticket.saturating_add(1);
    }
}

impl CommitSequenceAdmissionGate {
    fn acquire(
        self: &Arc<Self>,
        lock_path: &Path,
        deadline: Instant,
    ) -> Result<CommitSequenceAdmissionTurn, KvError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let ticket = state.next_ticket;
        state.next_ticket = state.next_ticket.saturating_add(1);
        loop {
            if state.serving_ticket == ticket {
                return Ok(CommitSequenceAdmissionTurn {
                    gate: Arc::clone(self),
                    ticket,
                });
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                state.cancelled_tickets.insert(ticket);
                return Err(KvError::Busy {
                    path: lock_path.display().to_string(),
                });
            }
            let (next_state, wait) = self
                .changed
                .wait_timeout(state, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state = next_state;
            if wait.timed_out() && state.serving_ticket != ticket {
                state.cancelled_tickets.insert(ticket);
                return Err(KvError::Busy {
                    path: lock_path.display().to_string(),
                });
            }
        }
    }
}

impl Drop for CommitSequenceAdmissionTurn {
    fn drop(&mut self) {
        let mut state = self
            .gate
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        debug_assert_eq!(state.serving_ticket, self.ticket);
        advance_commit_sequence_admission(&mut state);
        drop(state);
        self.gate.changed.notify_all();
    }
}

/// Locks one cluster-Raft proposal or finalization phase against concurrent
/// committers from other tasks and other processes sharing this data dir. The
/// quorum driver holds it for replay/leader-check/append, releases it across
/// transport, and reacquires it for exact-entry revalidation/apply. This keeps
/// local log order deterministic without turning a slow voter into a WAN-sized
/// critical section.
///
/// The returned file holds an exclusive advisory lock released on drop.
pub fn lock_cluster_kv_commit_sequence(
    paths: &ClusterKvPaths,
    wait: Duration,
) -> Result<File, KvError> {
    fs::create_dir_all(&paths.raft).map_err(|source| KvError::Io {
        path: paths.raft.display().to_string(),
        detail: source.to_string(),
    })?;
    let lock_path = paths.raft.join("commit-sequence.lock");
    let deadline = Instant::now() + wait;
    // `try_lock_exclusive` is deliberately bounded, but polling it directly is
    // not fair: a newly-arrived task can acquire between the older waiter's
    // exponentially-backed-off probes. Admit one waiter per process in ticket
    // order, then retain the file lock as the cross-process authority. This
    // keeps timeout and process-death recovery semantics unchanged while
    // preventing a fresh local mutation from starving an older commit.
    let admission = commit_sequence_admission_gate(&lock_path);
    let _turn = admission.acquire(&lock_path, deadline)?;
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
    lock_node_exclusive(
        &lock,
        &lock_path,
        deadline.saturating_duration_since(Instant::now()),
    )?;
    Ok(lock)
}

#[doc(hidden)]
#[cfg(any(test, feature = "test-support"))]
pub fn cluster_kv_commit_sequence_admission_depth_for_test(paths: &ClusterKvPaths) -> usize {
    let lock_path = paths.raft.join("commit-sequence.lock");
    let gates = COMMIT_SEQUENCE_ADMISSION_GATES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let Some(gate) = gates.get(&lock_path).and_then(std::sync::Weak::upgrade) else {
        return 0;
    };
    let state = gate
        .state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    usize::try_from(state.next_ticket.saturating_sub(state.serving_ticket)).unwrap_or(usize::MAX)
}

pub fn register_storage_paths(
    layout: &mut sinomo_storage::StorageLayout,
) -> Result<(), sinomo_storage::StorageContractError> {
    use sinomo_storage::{ObjectSchema, STORAGE_CONTRACT_OBJECT_VERSION, StorageRing};

    layout.register_sinomo_object(
        "sinomo-kv",
        StorageRing::R1,
        "cluster/kv-snapshot.sobj",
        ObjectSchema::new("KvSnapshot", STORAGE_CONTRACT_OBJECT_VERSION)
            .field("revision", "KvRevision")
            .field("entries", "Vec<KvEntry>")
            .field("events", "Vec<KvEvent>"),
    )?;
    layout.register_file(sinomo_storage::StorageFileContract::binary(
        "sinomo-kv",
        StorageRing::R1,
        "cluster/kv-snapshot.sobj.lock",
    )?)?;
    layout.register_sinomo_object(
        "sinomo-kv",
        StorageRing::R1,
        "cluster/watch-events.sobj",
        ObjectSchema::new("Vec<KvEvent>", STORAGE_CONTRACT_OBJECT_VERSION),
    )?;
    layout.register_object_log("sinomo-kv", StorageRing::R1, "cluster/watch-events.sobjlog")?;
    layout.register_file(sinomo_storage::StorageFileContract::binary(
        "sinomo-kv",
        StorageRing::R1,
        "cluster/watches.lock",
    )?)?;
    layout.register_sinomo_object(
        "sinomo-kv",
        StorageRing::R1,
        "cluster/watches-next.sobj",
        ObjectSchema::new(
            "ClusterKvWatchAllocatorState",
            STORAGE_CONTRACT_OBJECT_VERSION,
        )
        .field("next_watch_id", "u64"),
    )?;
    layout.register_sinomo_object(
        "sinomo-kv",
        StorageRing::R1,
        "cluster/kv-raft/hard-state.sobj",
        ObjectSchema::new("RaftHardState", STORAGE_CONTRACT_OBJECT_VERSION),
    )?;
    layout.register_sinomo_object(
        "sinomo-kv",
        StorageRing::R1,
        "cluster/kv-raft/snapshot.sobj",
        ObjectSchema::new("RaftSnapshot", STORAGE_CONTRACT_OBJECT_VERSION),
    )?;
    layout.register_object_log("sinomo-kv", StorageRing::R1, "cluster/kv-raft/wal.sobjlog")?;
    layout.register_sinomo_object(
        "sinomo-kv",
        StorageRing::R1,
        "cluster/kv-request-attempts.sobj",
        ObjectSchema::new("KvRequestAttemptLedger", STORAGE_CONTRACT_OBJECT_VERSION),
    )?;
    layout.register_object_log(
        "sinomo-kv",
        StorageRing::R1,
        "cluster/kv-request-attempts.sobjlog",
    )?;
    layout.register_file(sinomo_storage::StorageFileContract::binary(
        "sinomo-kv",
        StorageRing::R1,
        "cluster/kv-request-attempts.lock",
    )?)?;
    layout.register_sinomo_object(
        "sinomo-kv",
        StorageRing::R1,
        "cluster/kv-raft-peer-progress.sobj",
        ObjectSchema::new(
            "ClusterKvRaftPeerProgressLedger",
            STORAGE_CONTRACT_OBJECT_VERSION,
        ),
    )?;
    layout.register_escape_path(
        "sinomo-kv",
        StorageRing::R1,
        "cluster/kv-raft",
        "durable raft nodes use dynamic node-id subdirectories for locks, WALs, hard state, and snapshots",
    )?;
    layout.register_escape_path(
        "sinomo-kv",
        StorageRing::R1,
        "cluster/watches",
        "dynamic cluster KV watch registration object files keyed by watch id",
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvPaths {
    pub root: PathBuf,
    pub snapshot: PathBuf,
    pub raft: PathBuf,
    pub watches: PathBuf,
    pub watches_lock: PathBuf,
    pub watches_next_id: PathBuf,
    pub watch_events: PathBuf,
    pub watch_events_log: PathBuf,
    pub request_attempts: PathBuf,
    pub request_attempts_log: PathBuf,
    pub request_attempts_lock: PathBuf,
    pub raft_peer_progress: PathBuf,
}

impl ClusterKvPaths {
    pub fn new(cluster_root: impl Into<PathBuf>) -> Self {
        let root = cluster_root.into();
        Self {
            snapshot: root.join("kv-snapshot.sobj"),
            raft: root.join("kv-raft"),
            watches: root.join("watches"),
            watches_lock: root.join("watches.lock"),
            watches_next_id: root.join("watches-next.sobj"),
            watch_events: root.join("watch-events.sobj"),
            watch_events_log: root.join("watch-events.sobjlog"),
            request_attempts: root.join("kv-request-attempts.sobj"),
            request_attempts_log: root.join("kv-request-attempts.sobjlog"),
            request_attempts_lock: root.join("kv-request-attempts.lock"),
            raft_peer_progress: root.join("kv-raft-peer-progress.sobj"),
            root,
        }
    }

    pub fn local_cache(&self) -> KvLocalCache {
        KvLocalCache::new(self.snapshot.clone(), self.raft.clone())
            .with_max_materialized_events(MAX_CLUSTER_KV_MATERIALIZED_EVENTS)
    }

    pub fn raft_wal_paths(&self, node_id: &NodeId) -> KvWalPaths {
        KvWalPaths::new(self.raft.join(node_id.as_str()))
    }

    pub fn watch_path(&self, watch_id: WatchId) -> PathBuf {
        self.watches.join(format!("watch-{}.sobj", watch_id.0))
    }

    pub fn raft_store(&self) -> ClusterKvRaftStore {
        ClusterKvRaftStore::new(self.clone(), default_cluster_kv_raft_compaction_policy())
    }

    pub fn append_kv_command_and_publish<F>(
        &self,
        node_id: &NodeId,
        command: KvCommand,
        key_can_defer_materialized_snapshot: F,
    ) -> Result<KvSnapshot, KvError>
    where
        F: FnMut(&str) -> bool,
    {
        self.raft_store().append_command_and_publish(
            node_id,
            command,
            key_can_defer_materialized_snapshot,
        )
    }

    pub fn append_kv_command_with_response_and_publish<F>(
        &self,
        node_id: &NodeId,
        command: KvCommand,
        key_can_defer_materialized_snapshot: F,
    ) -> Result<(KvSnapshot, Option<KvTxnResponse>), KvError>
    where
        F: FnMut(&str) -> bool,
    {
        self.raft_store()
            .append_command_with_response_and_publish(
                node_id,
                command,
                key_can_defer_materialized_snapshot,
            )
            .map(|result| (Arc::unwrap_or_clone(result.snapshot), result.txn_response))
    }

    pub fn append_kv_command_with_response_and_publish_report<F>(
        &self,
        node_id: &NodeId,
        command: KvCommand,
        key_can_defer_materialized_snapshot: F,
    ) -> Result<(KvSnapshot, Option<KvTxnResponse>, bool), KvError>
    where
        F: FnMut(&str) -> bool,
    {
        self.raft_store()
            .append_command_with_response_and_publish_report(
                node_id,
                command,
                key_can_defer_materialized_snapshot,
            )
            .map(|(result, wrote)| {
                (
                    Arc::unwrap_or_clone(result.snapshot),
                    result.txn_response,
                    wrote,
                )
            })
    }

    pub fn current_snapshot_revision_for_node(
        &self,
        node_id: &NodeId,
    ) -> Result<KvRevision, KvError> {
        read_cluster_kv_current_snapshot_for_node(self, node_id.clone())
            .map(|snapshot| snapshot.revision)
    }

    /// Publishes a local applied-Raft projection with monotonic generation
    /// comparison. Quorum hosts call this only after releasing Raft storage so
    /// materialized-file serialization and fsync cannot block resident reads.
    pub fn write_raft_snapshot_if_changed(&self, snapshot: &KvSnapshot) -> Result<bool, KvError> {
        self.local_cache().write_raft_snapshot_if_changed(snapshot)
    }

    pub fn apply_raft_append_with_response_and_publish_report<F>(
        &self,
        node_id: &NodeId,
        snapshot: Option<RaftSnapshot>,
        append: RaftAppendEntriesRequest,
        key_can_defer_materialized_snapshot: F,
    ) -> Result<(RaftAppendEntriesResponse, bool), KvError>
    where
        F: FnMut(&str) -> bool,
    {
        self.raft_store()
            .apply_append_entries_with_optional_snapshot_and_publish(
                node_id,
                snapshot,
                append,
                key_can_defer_materialized_snapshot,
            )
            .map(|(outcome, wrote)| (outcome.response, wrote))
    }

    pub fn apply_raft_append_compact_with_publish_report(
        &self,
        node_id: &NodeId,
        snapshot: Option<RaftSnapshot>,
        append: RaftAppendEntriesRequest,
    ) -> Result<(RaftAppendEntriesCompactApplyOutcome, bool), KvError> {
        self.raft_store()
            .apply_append_entries_with_optional_snapshot_compact_and_publish(
                node_id, snapshot, append,
            )
    }

    pub fn apply_raft_vote_response(
        &self,
        state: &ClusterBootstrapState,
        vote: RaftRequestVoteRequest,
    ) -> Result<ClusterRaftVoteResponse, KvError> {
        self.raft_store().apply_vote_response(state, vote)
    }

    pub fn locked_apply_raft_vote(
        &self,
        node_id: &NodeId,
        vote: RaftRequestVoteRequest,
    ) -> Result<RaftRequestVoteResponse, KvError> {
        self.raft_store().locked_apply_vote(node_id, vote)
    }

    pub fn raft_append_rejection_response(
        &self,
        node_id: &NodeId,
    ) -> Result<RaftAppendEntriesResponse, KvError> {
        self.raft_store().append_entries_rejection_response(node_id)
    }

    pub fn raft_vote_rejection_response(
        &self,
        state: &ClusterBootstrapState,
        detail: String,
    ) -> Result<ClusterRaftVoteResponse, KvError> {
        self.raft_store()
            .cluster_raft_vote_rejection_response(state, detail)
    }

    pub fn known_raft_voter_leader_id(
        &self,
        state: &ClusterBootstrapState,
    ) -> Result<Option<NodeId>, KvError> {
        self.raft_store().known_voter_leader_id(state)
    }

    pub fn locked_known_raft_voter_leader_id(
        &self,
        state: &ClusterBootstrapState,
    ) -> Result<Option<NodeId>, KvError> {
        self.raft_store().locked_known_voter_leader_id(state)
    }

    pub fn locked_known_raft_voter_leader_id_before(
        &self,
        state: &ClusterBootstrapState,
        deadline: Instant,
    ) -> Result<Option<NodeId>, KvError> {
        self.raft_store()
            .locked_known_voter_leader_id_before(state, deadline)
    }

    pub fn locked_raft_local_is_leader(
        &self,
        state: &ClusterBootstrapState,
    ) -> Result<bool, KvError> {
        self.raft_store().locked_local_is_leader(state)
    }

    pub fn set_raft_leader_and_write_snapshot_if_changed(
        &self,
        node_id: &NodeId,
        term: u64,
    ) -> Result<KvSnapshot, KvError> {
        self.raft_store()
            .set_leader_and_write_snapshot_if_changed(node_id, term)
    }

    pub fn locked_set_raft_leader_and_write_snapshot_if_changed(
        &self,
        node_id: &NodeId,
        term: u64,
    ) -> Result<KvSnapshot, KvError> {
        self.raft_store()
            .locked_set_leader_and_write_snapshot_if_changed(node_id, term)
    }

    pub fn locked_set_raft_leader_compact(
        &self,
        node_id: &NodeId,
        term: u64,
    ) -> Result<(), KvError> {
        self.raft_store().locked_set_leader_compact(node_id, term)
    }

    pub fn clear_raft_leader_and_write_snapshot_if_changed(
        &self,
        node_id: &NodeId,
    ) -> Result<KvSnapshot, KvError> {
        self.raft_store()
            .clear_leader_and_write_snapshot_if_changed(node_id)
    }

    pub fn locked_clear_raft_leader_compact(&self, node_id: &NodeId) -> Result<(), KvError> {
        self.raft_store().locked_clear_leader_compact(node_id)
    }

    /// Initialize the process-resident owner without altering pending history.
    /// Another voter may have accepted or committed entries beyond our local
    /// commit position, including when a predecessor dies during quorum I/O.
    pub fn locked_initialize_raft_resident(&self, node_id: &NodeId) -> Result<(), KvError> {
        let _raft_guard = lock_cluster_kv_raft_operation_for_paths(self)?;
        let _node = self.raft_store().open_node(node_id)?;
        Ok(())
    }

    pub fn locked_discard_raft_uncommitted_tail(&self, node_id: &NodeId) -> Result<bool, KvError> {
        let _raft_guard = lock_cluster_kv_raft_operation_for_paths(self)?;
        self.raft_store().discard_uncommitted_tail(node_id)
    }

    pub fn locked_observe_raft_term_and_write_snapshot_if_changed(
        &self,
        node_id: &NodeId,
        term: u64,
    ) -> Result<bool, KvError> {
        self.raft_store()
            .locked_observe_term_and_write_snapshot_if_changed(node_id, term)
    }

    pub fn locked_observe_raft_term_compact(
        &self,
        node_id: &NodeId,
        term: u64,
    ) -> Result<bool, KvError> {
        self.raft_store().locked_observe_term_compact(node_id, term)
    }

    pub fn locked_local_raft_vote_metadata(
        &self,
        node_id: &NodeId,
    ) -> Result<ClusterKvRaftLocalVoteMetadata, KvError> {
        self.raft_store().locked_local_vote_metadata(node_id)
    }

    pub fn locked_last_raft_log_index(&self, node_id: &NodeId) -> Result<u64, KvError> {
        self.raft_store().locked_last_log_index(node_id)
    }

    pub fn open_raft_node(&self, node_id: &NodeId) -> Result<DurableRaftKvNode, KvError> {
        self.raft_store().open_node(node_id)
    }

    pub fn append_raft_command_uncommitted(
        &self,
        node_id: &NodeId,
        command: KvCommand,
    ) -> Result<KvLogEntry, KvError> {
        self.raft_store()
            .append_command_uncommitted(node_id, command)
    }

    pub fn local_raft_command_storage<F>(
        &self,
        node_id: NodeId,
        key_can_defer_materialized_snapshot: F,
    ) -> ClusterKvRaftLocalCommandStorage<F>
    where
        F: FnMut(&str) -> bool,
    {
        ClusterKvRaftLocalCommandStorage::new(
            self.raft_store(),
            node_id,
            key_can_defer_materialized_snapshot,
        )
    }

    pub async fn open_raft_node_async(
        &self,
        node_id: &NodeId,
    ) -> Result<DurableRaftKvNode, KvError> {
        self.raft_store().open_node_async(node_id).await
    }

    pub async fn locked_last_raft_log_index_async(&self, node_id: &NodeId) -> Result<u64, KvError> {
        self.raft_store().locked_last_log_index_async(node_id).await
    }

    pub fn build_raft_append_request_from_next_index(
        &self,
        state: &ClusterBootstrapState,
        next_index: u64,
    ) -> Result<ClusterRaftAppendRequest, KvError> {
        self.raft_store()
            .build_cluster_raft_append_request_from_next_index(state, next_index)
    }

    pub fn locked_build_raft_append_request_from_next_index(
        &self,
        state: &ClusterBootstrapState,
        next_index: u64,
    ) -> Result<ClusterRaftAppendRequest, KvError> {
        self.raft_store()
            .locked_build_cluster_raft_append_request_from_next_index(state, next_index)
    }

    pub async fn locked_build_raft_append_request_from_next_index_async(
        &self,
        state: &ClusterBootstrapState,
        next_index: u64,
    ) -> Result<ClusterRaftAppendRequest, KvError> {
        self.raft_store()
            .locked_build_cluster_raft_append_request_from_next_index_async(state, next_index)
            .await
    }

    pub fn append_kv_ops_in_batches_and_publish<F>(
        &self,
        node_id: &NodeId,
        ops: Vec<KvOp>,
        key_can_defer_materialized_snapshot: F,
    ) -> Result<(KvSnapshot, bool), KvError>
    where
        F: FnMut(&str) -> bool,
    {
        self.append_kv_ops_in_batches_and_publish_with_observation_report(
            node_id,
            ops,
            key_can_defer_materialized_snapshot,
        )
        .map(|(snapshot, wrote, _)| (snapshot, wrote))
    }

    /// The final preparation flag covers every batch's own completed receipt.
    /// One prepared batch cannot conceal an unprepared earlier/later effect.
    pub fn append_kv_ops_in_batches_and_publish_with_observation_report<F>(
        &self,
        node_id: &NodeId,
        ops: Vec<KvOp>,
        mut key_can_defer_materialized_snapshot: F,
    ) -> Result<(KvSnapshot, bool, bool), KvError>
    where
        F: FnMut(&str) -> bool,
    {
        let mut ops = ops.into_iter();
        let mut latest = None;
        let mut wrote_any = false;
        let mut all_delivery_prepared = true;
        let store = self.raft_store();
        loop {
            let mut chunk = Vec::new();
            for op in ops.by_ref().take(MAX_KV_MUTATION_EVENTS_PER_COMMAND) {
                try_reserve_kv_items(&mut chunk, 1, "batch kv ops for publish")?;
                chunk.push(op);
            }
            if chunk.is_empty() {
                break;
            }
            let (result, wrote) = store.append_command_with_response_and_publish_report(
                node_id,
                KvCommand::Txn(KvTxn {
                    compares: Vec::new(),
                    success: chunk,
                    failure: Vec::new(),
                }),
                &mut key_can_defer_materialized_snapshot,
            )?;
            wrote_any |= wrote;
            all_delivery_prepared &= result
                .observation_receipt
                .as_ref()
                .is_some_and(|receipt| receipt.was_delivery_prepared());
            latest = Some(Arc::unwrap_or_clone(result.snapshot));
        }
        latest
            .map(|snapshot| (snapshot, wrote_any, all_delivery_prepared))
            .ok_or_else(|| KvError::Io {
                path: self.snapshot.display().to_string(),
                detail: "cannot append an empty batch of cluster kv operations".to_string(),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batched_publication_report_requires_every_own_effect_prepared() {
        let root = sinomo_testdir::TestDir::new("batched-publication-preparation-report");
        let paths = ClusterKvPaths::new(root.path());
        let node = NodeId::from_legacy_named("node_batched_preparation_report").unwrap();
        let observed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = Arc::clone(&observed);
        let _subscription = crate::subscribe_committed_observations(&paths, move |event| {
            if count.fetch_add(1, Ordering::SeqCst) == 0 {
                event.prepare_once(|| {});
            }
        });
        let ops = (0..=MAX_KV_MUTATION_EVENTS_PER_COMMAND)
            .map(|index| KvOp::Put {
                key: format!("/batch/report/{index}"),
                value: b"value".to_vec(),
                lease_id: None,
            })
            .collect();
        let (snapshot, wrote, all_prepared) = paths
            .append_kv_ops_in_batches_and_publish_with_observation_report(&node, ops, |_| false)
            .unwrap();
        assert!(wrote);
        assert_eq!(
            snapshot.entries.len(),
            MAX_KV_MUTATION_EVENTS_PER_COMMAND + 1
        );
        assert_eq!(observed.load(Ordering::SeqCst), 2);
        assert!(
            !all_prepared,
            "the first prepared receipt cannot conceal the second unprepared receipt"
        );
    }

    fn wait_for_issued_ticket(gate: &CommitSequenceAdmissionGate, issued: u64) {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let next_ticket = gate
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .next_ticket;
            if next_ticket >= issued {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for admission ticket {issued}"
            );
            thread::yield_now();
        }
    }

    #[test]
    fn commit_sequence_admission_is_fifo() {
        let gate = Arc::new(CommitSequenceAdmissionGate::default());
        let path = PathBuf::from("commit-sequence.lock");
        let first = gate
            .acquire(&path, Instant::now() + Duration::from_secs(1))
            .expect("first turn");
        let order = Arc::new(Mutex::new(Vec::new()));

        let oldest = {
            let gate = Arc::clone(&gate);
            let path = path.clone();
            let order = Arc::clone(&order);
            thread::spawn(move || {
                let _turn = gate
                    .acquire(&path, Instant::now() + Duration::from_secs(1))
                    .expect("oldest waiting turn");
                order
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push("oldest");
            })
        };
        wait_for_issued_ticket(&gate, 2);
        let newest = {
            let gate = Arc::clone(&gate);
            let order = Arc::clone(&order);
            thread::spawn(move || {
                let _turn = gate
                    .acquire(&path, Instant::now() + Duration::from_secs(1))
                    .expect("newest waiting turn");
                order
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push("newest");
            })
        };
        wait_for_issued_ticket(&gate, 3);

        drop(first);
        oldest.join().expect("oldest waiter");
        newest.join().expect("newest waiter");
        assert_eq!(
            *order
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            ["oldest", "newest"]
        );
    }

    #[test]
    fn commit_sequence_admission_skips_timed_out_ticket() {
        let gate = Arc::new(CommitSequenceAdmissionGate::default());
        let path = PathBuf::from("commit-sequence.lock");
        let first = gate
            .acquire(&path, Instant::now() + Duration::from_secs(1))
            .expect("first turn");
        let timed_out = {
            let gate = Arc::clone(&gate);
            let path = path.clone();
            thread::spawn(move || gate.acquire(&path, Instant::now() + Duration::from_millis(20)))
        };
        wait_for_issued_ticket(&gate, 2);
        assert!(matches!(
            timed_out.join().expect("timed-out waiter"),
            Err(KvError::Busy { .. })
        ));

        let next = {
            let gate = Arc::clone(&gate);
            thread::spawn(move || gate.acquire(&path, Instant::now() + Duration::from_secs(1)))
        };
        wait_for_issued_ticket(&gate, 3);
        drop(first);
        let turn = next
            .join()
            .expect("next waiter")
            .expect("next turn after cancellation");
        assert_eq!(turn.ticket, 2);
    }
}

#[cfg(test)]
#[path = "paths/imported_generation_tests.rs"]
mod imported_generation_tests;
