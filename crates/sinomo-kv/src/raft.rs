use crate::internal::*;

#[path = "raft_group_commit.rs"]
mod group_commit;
pub use group_commit::*;

static CLUSTER_KV_RAFT_OPERATION_LOCKS: LazyLock<
    Mutex<HashMap<PathBuf, std::sync::Weak<ClusterKvRaftOperationLock>>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
/// Per-WAL-root authority gates for Raft terms observed from the network but
/// not yet known to be durable in this process's node-local hard state.
///
/// A higher-term response invalidates local authority immediately. Persisting
/// that observation requires the ordinary operation/resident-node admission
/// path and can therefore be delayed briefly. Each ordinary authority action
/// owns a shared lease across its Raft linearization point. Publication raises
/// the monotonic floor under the small gate mutex without waiting; the durable
/// closer later takes the exclusive side and drains already-admitted leases,
/// so checking the floor and then ACKing or mutating cannot race. The
/// process-global registry is held only long enough to clone one root's gate;
/// unrelated clusters and nodes never serialize their I/O through it.
static PENDING_HIGHER_RAFT_TERM_GATES: LazyLock<
    Mutex<HashMap<PathBuf, Arc<PendingHigherRaftTermGate>>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Debug, Default)]
struct PendingHigherRaftTermGateState {
    floor: Option<u64>,
    known_durable: Option<PendingHigherRaftTermDurableState>,
    active_authority_leases: usize,
    exclusive: bool,
    #[cfg(test)]
    settled_read_waiters: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PendingHigherRaftTermGeneration {
    lock_identity: FileIdentity,
    root_ino: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PendingHigherRaftTermDurableState {
    generation: PendingHigherRaftTermGeneration,
    term: u64,
}

impl PendingHigherRaftTermGateState {
    fn acknowledge_durable_term(&mut self, generation: PendingHigherRaftTermGeneration, term: u64) {
        if let Some(previous) = self.known_durable {
            if previous.generation == generation && term >= previous.term {
                self.known_durable = Some(PendingHigherRaftTermDurableState { generation, term });
                return;
            }
            // Publication may have suppressed `previous.term` immediately
            // before this acknowledgement proved that durable storage changed
            // generations or regressed. Resurrect that observation as a floor
            // before replacing the watermark. Both updates share the gate
            // mutex, so publication cannot slip between the carry and reset;
            // a stale lower acknowledgement retains at least the prior floor.
            self.floor = Some(self.floor.unwrap_or_default().max(previous.term));
        }
        // A valid durable Raft hard-state term never decreases. Observing a
        // lower term is therefore proof that storage was replaced/recovered,
        // even when the supported same-path handoff preserves its root and
        // lock identities. Resetting downward after restoring the prior floor
        // is deliberately fail-closed.
        self.known_durable = Some(PendingHigherRaftTermDurableState { generation, term });
    }
}

#[derive(Debug, Default)]
struct PendingHigherRaftTermGate {
    state: Mutex<PendingHigherRaftTermGateState>,
    changed: Condvar,
}

fn pending_higher_term_gate_for_root(
    root: &Path,
) -> Result<Arc<PendingHigherRaftTermGate>, KvError> {
    let mut gates = PENDING_HIGHER_RAFT_TERM_GATES
        .lock()
        .map_err(|error| KvError::Io {
            path: root.display().to_string(),
            detail: format!("pending higher Raft term gate registry poisoned: {error}"),
        })?;
    if let Some(gate) = gates.get(root) {
        return Ok(Arc::clone(gate));
    }
    // A strong registry entry is required while a floor is pending: a
    // Weak-only table could forget the fence after publication returns but
    // before its persistence worker starts. Retain the process-known durable
    // term too, so replayed same/older observations cannot re-fence authority.
    // Prune only gates that have never learned durable state and are quiescent.
    gates.retain(|_, gate| {
        if Arc::strong_count(gate) > 1 {
            return true;
        }
        let state = gate
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.floor.is_some()
            || state.known_durable.is_some()
            || state.active_authority_leases > 0
            || state.exclusive
    });
    Ok(Arc::clone(gates.entry(root.to_path_buf()).or_default()))
}

fn acquire_pending_higher_term_authority_lease_for_root(
    root: &Path,
    durable_term: Option<u64>,
    operation: &'static str,
) -> Result<PendingHigherRaftTermAuthorityLease, KvError> {
    let gate = pending_higher_term_gate_for_root(root)?;
    let mut state = gate
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(durable_term) = durable_term
        && let Some(known) = state.known_durable
        && known.term > durable_term
    {
        // A direct hard-state authority read may be the first operation to see
        // a supported same-path replacement. Publication could already have
        // suppressed `known.term`, so restore it as a floor before normal
        // admission. This shares the gate mutex with publication and durable
        // acknowledgement; no authority lease can pass between the carry and
        // the comparison below.
        state.floor = Some(state.floor.unwrap_or_default().max(known.term));
    }
    if state.exclusive {
        return Err(KvError::AuthorityUnavailable {
            detail: format!("cannot {operation} while a higher Raft term is being made durable"),
        });
    }
    if let Some(pending_term) = state.floor {
        if durable_term.is_some_and(|term| term >= pending_term) {
            state.floor = None;
        } else {
            return Err(KvError::AuthorityUnavailable {
                detail: match durable_term {
                    Some(durable_term) => format!(
                        "cannot {operation} while observed Raft term {pending_term} is pending durable hard state at term {durable_term}"
                    ),
                    None => format!(
                        "cannot {operation} while observed Raft term {pending_term} is pending durable hard state"
                    ),
                },
            });
        }
    }
    state.active_authority_leases =
        state
            .active_authority_leases
            .checked_add(1)
            .ok_or_else(|| KvError::Io {
                path: root.display().to_string(),
                detail: "pending higher Raft term authority lease count overflow".to_string(),
            })?;
    drop(state);
    Ok(PendingHigherRaftTermAuthorityLease { gate })
}

#[derive(Debug)]
struct PendingHigherRaftTermAuthorityLease {
    gate: Arc<PendingHigherRaftTermGate>,
}

impl Drop for PendingHigherRaftTermAuthorityLease {
    fn drop(&mut self) {
        let mut state = self
            .gate
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active_authority_leases = state.active_authority_leases.saturating_sub(1);
        self.gate.changed.notify_all();
    }
}

#[derive(Debug)]
struct PendingHigherRaftTermExclusiveLease {
    gate: Arc<PendingHigherRaftTermGate>,
}

impl PendingHigherRaftTermExclusiveLease {
    fn pending_floor(&self) -> Option<u64> {
        self.gate
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .floor
    }

    fn clear_if_durable(
        &self,
        generation: PendingHigherRaftTermGeneration,
        durable_term: u64,
    ) -> bool {
        let mut state = self
            .gate
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.acknowledge_durable_term(generation, durable_term);
        if state
            .floor
            .is_some_and(|pending_term| durable_term >= pending_term)
        {
            state.floor = None;
            return true;
        }
        false
    }
}

impl Drop for PendingHigherRaftTermExclusiveLease {
    fn drop(&mut self) {
        let mut state = self
            .gate
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.exclusive = false;
        self.gate.changed.notify_all();
    }
}

/// Admission retained while one AppendEntries request applies. A request at
/// or below already-durable hard state shares the ordinary authority side of
/// the gate: a later higher-term publication linearizes after it and waits for
/// this lease to drain. Only a request that can advance/close durable term
/// owns the exclusive side and briefly fences local authority.
#[derive(Debug)]
#[allow(dead_code)] // The settled variant owns its lease solely for Drop.
enum PendingHigherRaftTermAppendLease {
    Settled(PendingHigherRaftTermAuthorityLease),
    Closing(PendingHigherRaftTermExclusiveLease),
}

impl PendingHigherRaftTermAppendLease {
    fn ensure_observation_is_current(&self, observed_term: u64) -> Result<(), KvError> {
        let Self::Closing(lease) = self else {
            // A higher-term publication after shared admission is ordered
            // after this request and waits for the retained authority lease.
            return Ok(());
        };
        ClusterKvRaftStore::ensure_pending_higher_term_observation_is_current(lease, observed_term)
    }

    fn clear_if_durable(&self, generation: PendingHigherRaftTermGeneration, durable_term: u64) {
        if let Self::Closing(lease) = self {
            lease.clear_if_durable(generation, durable_term);
        }
    }
}
/// Process-resident durable Raft nodes, keyed by their node-local WAL root.
///
/// etcd keeps its MVCC store, Raft storage, and backend open for the lifetime of
/// the server and restores them only during startup/recovery. Sinomo has the
/// same single-main-daemon ownership invariant, so reopening and replaying the
/// complete snapshot/WAL for every command is unnecessary. The cached file
/// handle deliberately retains the existing exclusive node lock: a shadow or
/// recovery process cannot become a second writer while the main daemon owns
/// the resident state. A process that only needs a read can still use the
/// materialized snapshot fallback when that lock is busy.
static RESIDENT_RAFT_NODES: LazyLock<Mutex<HashMap<PathBuf, Arc<ResidentRaftNodeSlot>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(test)]
#[derive(Clone)]
struct ResidentRaftAfterHandoffPause {
    observed: Arc<std::sync::Barrier>,
    resume: Arc<std::sync::Barrier>,
}
#[cfg(test)]
static RESIDENT_RAFT_AFTER_HANDOFF_PAUSES: LazyLock<
    Mutex<HashMap<PathBuf, ResidentRaftAfterHandoffPause>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
/// Raft roots that this process is permanently forbidden to own.
///
/// A shadow daemon shares the main daemon's data directory and node identity,
/// but it is not a second Raft voter.  Keeping this one-way process-local gate
/// below every resident/cold node open makes that ownership boundary survive
/// new callers that accidentally reach the local KV read model.
static PROCESS_LOCAL_RAFT_DENIED_ROOTS: LazyLock<Mutex<BTreeSet<PathBuf>>> =
    LazyLock::new(|| Mutex::new(BTreeSet::new()));
const RESIDENT_RAFT_NODE_CAPACITY: usize = 64;
static RESIDENT_RAFT_NODE_ACCESS_CLOCK: AtomicU64 = AtomicU64::new(1);

#[cfg(test)]
pub(crate) fn install_resident_raft_after_handoff_pause_for_test(
    root: PathBuf,
    observed: Arc<std::sync::Barrier>,
    resume: Arc<std::sync::Barrier>,
) {
    RESIDENT_RAFT_AFTER_HANDOFF_PAUSES
        .lock()
        .expect("resident Raft after-handoff pause")
        .insert(root, ResidentRaftAfterHandoffPause { observed, resume });
}

#[cfg(test)]
pub(crate) fn clear_resident_raft_after_handoff_pause_for_test(root: &Path) {
    RESIDENT_RAFT_AFTER_HANDOFF_PAUSES
        .lock()
        .expect("resident Raft after-handoff pause")
        .remove(root);
}

#[cfg(test)]
fn pause_resident_raft_after_handoff_for_test(root: &Path) {
    let pause = RESIDENT_RAFT_AFTER_HANDOFF_PAUSES
        .lock()
        .ok()
        .and_then(|pauses| pauses.get(root).cloned());
    if let Some(pause) = pause {
        pause.observed.wait();
        pause.resume.wait();
    }
}

#[cfg(not(test))]
fn pause_resident_raft_after_handoff_for_test(_root: &Path) {}

/// Permanently denies local Raft ownership below `raft_root` for this process.
///
/// The gate is intentionally one-way in production. Main and shadow daemons
/// are distinct processes; once a process identifies itself as the shadow it
/// must never become a local Raft owner later in its lifetime. Daemon startup
/// installs the denial before spawning worker threads. The resident registry
/// check below additionally rejects an already-active owner; callers must not
/// use this API as a live role transition while arbitrary nonresident recovery
/// tooling is concurrently opening the same root.
pub fn deny_process_local_raft_access_for_root(
    raft_root: impl Into<PathBuf>,
) -> Result<(), KvError> {
    let raft_root = raft_root.into();
    let mut denied = PROCESS_LOCAL_RAFT_DENIED_ROOTS
        .lock()
        .map_err(|error| KvError::Io {
            path: raft_root.display().to_string(),
            detail: format!("process-local Raft access gate poisoned: {error}"),
        })?;
    denied.insert(raft_root.clone());
    drop(denied);
    evict_cached_raft_views_under_root(&raft_root)?;

    // Drop an idle cached owner immediately. An actively checked-out owner is
    // a startup invariant violation: leave the denial installed (so no new
    // checkout can pass) and fail instead of pretending the shadow is safe.
    let mut resident = RESIDENT_RAFT_NODES.lock().map_err(|error| KvError::Io {
        path: raft_root.display().to_string(),
        detail: format!("resident cluster KV Raft node registry poisoned: {error}"),
    })?;
    let matching: Vec<PathBuf> = resident
        .keys()
        .filter(|path| path.starts_with(&raft_root))
        .cloned()
        .collect();
    let mut active = Vec::new();
    for path in matching {
        if resident
            .get(&path)
            .is_some_and(|slot| Arc::strong_count(slot) == 1)
        {
            resident.remove(&path);
        } else {
            active.push(path);
        }
    }
    drop(resident);
    if !active.is_empty() {
        return Err(KvError::AuthorityUnavailable {
            detail: format!(
                "cannot deny local Raft access below {} while resident ownership is active at {}",
                raft_root.display(),
                active
                    .iter()
                    .map(|path| path.display().to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        });
    }
    Ok(())
}

/// Returns whether this process is forbidden to open a Raft node at `path`.
pub fn process_local_raft_access_is_denied(path: &Path) -> bool {
    match PROCESS_LOCAL_RAFT_DENIED_ROOTS.lock() {
        Ok(roots) => roots.iter().any(|root| path.starts_with(root)),
        // Policy corruption must never upgrade a passive process into a Raft
        // owner. `require_process_local_raft_access` will turn this into the
        // same fail-closed authority error.
        Err(_) => true,
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn clear_process_local_raft_access_denial_for_test(raft_root: &Path) -> bool {
    PROCESS_LOCAL_RAFT_DENIED_ROOTS
        .lock()
        .map(|mut roots| roots.remove(raft_root))
        .unwrap_or(false)
}

pub(crate) fn require_process_local_raft_access(path: &Path) -> Result<(), KvError> {
    if process_local_raft_access_is_denied(path) {
        return Err(KvError::AuthorityUnavailable {
            detail: format!(
                "this process role is not permitted to own local Raft state at {}",
                path.display()
            ),
        });
    }
    Ok(())
}

#[derive(Debug)]
struct ResidentRaftNode {
    node_id: NodeId,
    identity: RaftViewIdentity,
    lock_identity: FileIdentity,
    root_ino: u64,
    lock: File,
    node: ReplicatedKvNode,
    wal_synced_in_process: bool,
}

fn file_handle_identity(file: &File, path: &Path) -> Result<FileIdentity, KvError> {
    let metadata = file.metadata().map_err(|source| KvError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    let mtime = metadata.modified().map_err(|source| KvError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    Ok(FileIdentity {
        mtime: Some(mtime),
        len: metadata.len(),
        ino: file_ino(&metadata),
    })
}

fn directory_ino(path: &Path) -> Result<u64, KvError> {
    fs::metadata(path)
        .map(|metadata| file_ino(&metadata))
        .map_err(|source| KvError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })
}

#[derive(Clone, Debug)]
struct ResidentRaftReadMetadata {
    node_id: NodeId,
    identity: RaftViewIdentity,
    /// Stable ownership markers are separate from `identity`: normal WAL and
    /// hard-state writes change the full Raft view while the same resident
    /// owner remains checked out.
    lock_identity: Option<FileIdentity>,
    root_ino: Option<u64>,
    revision: KvRevision,
    observed_term: u64,
    applied_index: u64,
    known_leader: Option<NodeId>,
    /// Small consensus-critical projection retained while the mutable node is
    /// checked out. This keeps voter configuration reads authoritative without
    /// cloning the complete KV store or falling back to a stale materialized
    /// snapshot during an unrelated Raft operation.
    raft_config_entry: Option<Arc<KvEntry>>,
}

impl ResidentRaftReadMetadata {
    fn retained_raft_config_entry(
        node: &ReplicatedKvNode,
        previous: Option<&Self>,
    ) -> Option<Arc<KvEntry>> {
        let current = node.store.get(&cluster_kv_raft_config_key())?;
        if let Some(previous) = previous.and_then(|metadata| metadata.raft_config_entry.as_ref())
            && previous.modify_revision == current.modify_revision
            && previous.version == current.version
        {
            return Some(Arc::clone(previous));
        }
        Some(Arc::new(current.clone()))
    }

    fn from_resident(resident: &ResidentRaftNode, previous: Option<&Self>) -> Self {
        Self {
            node_id: resident.node_id.clone(),
            identity: resident.identity,
            lock_identity: Some(resident.lock_identity),
            root_ino: Some(resident.root_ino),
            revision: resident.node.store.revision(),
            observed_term: resident.node.hard_state.current_term,
            applied_index: resident.node.applied_index,
            known_leader: resident.node.hard_state.leader_id.clone(),
            raft_config_entry: Self::retained_raft_config_entry(&resident.node, previous),
        }
    }

    fn from_open_node(
        node: &DurableRaftKvNode,
        identity: RaftViewIdentity,
        previous: Option<&Self>,
    ) -> Self {
        Self {
            node_id: node.node_id.clone(),
            identity,
            lock_identity: Some(node.lock_identity),
            root_ino: Some(node.root_ino),
            revision: node.node.store.revision(),
            observed_term: node.node.hard_state.current_term,
            applied_index: node.node.applied_index,
            known_leader: node.node.hard_state.leader_id.clone(),
            raft_config_entry: Self::retained_raft_config_entry(&node.node, previous),
        }
    }
}

#[derive(Debug, Default)]
struct ResidentRaftNodeSlotState {
    checked_out: bool,
    read_loading: bool,
    read_load_waiters: usize,
    /// A mutable/consensus checkout must never sit behind optional local-read
    /// hydration for the generic five-second node-lock window. The checkout
    /// raises this flag so the cold reader can yield before opening durable
    /// state or hand the already-restored node to the resident owner.
    read_load_preempt_requested: bool,
    /// FIFO admission for mutable resident-node ownership. A plain condition
    /// variable lets newly arriving callers barge ahead of an already-woken
    /// waiter; sustained ReadIndex traffic can therefore starve one request all
    /// the way to `RAFT_NODE_LOCK_WAIT` even though every individual holder is
    /// short-lived. Tickets remain only while their caller is actively waiting.
    checkout_waiters: VecDeque<ResidentRaftCheckoutWaiter>,
    next_checkout_ticket: u64,
    cached: Option<ResidentRaftNode>,
    /// Small authority metadata for the last immutable committed view. The
    /// values are observational while a writer is checked out; key/value data
    /// lives either in the actual idle resident node or the bounded immutable
    /// Raft-view snapshot cache.
    read_metadata: Option<ResidentRaftReadMetadata>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ResidentRaftCheckoutWaiter {
    ticket: u64,
    deadline: Instant,
}

#[derive(Debug, Default)]
struct ResidentRaftNodeSlot {
    state: Mutex<ResidentRaftNodeSlotState>,
    ready: Condvar,
    /// Approximate process-local recency used only to choose among inactive
    /// cache entries. Checked-out/load-in-flight slots remain protected by
    /// their operation-owned `Arc`; this clock prevents a newly seeded idle
    /// daemon root from being discarded ahead of unrelated stale roots.
    last_access: AtomicU64,
}

impl ResidentRaftNodeSlot {
    fn touch(&self) {
        self.last_access.store(
            RESIDENT_RAFT_NODE_ACCESS_CLOCK.fetch_add(1, Ordering::Relaxed),
            Ordering::Relaxed,
        );
    }

    fn access_generation(&self) -> u64 {
        self.last_access.load(Ordering::Relaxed)
    }

    fn remove_checkout_waiter(&self, state: &mut ResidentRaftNodeSlotState, ticket: u64) {
        let was_front = state
            .checkout_waiters
            .front()
            .is_some_and(|waiter| waiter.ticket == ticket);
        state
            .checkout_waiters
            .retain(|waiter| waiter.ticket != ticket);
        let cleared_stale_preempt = state.checkout_waiters.is_empty()
            && std::mem::take(&mut state.read_load_preempt_requested);
        if cleared_stale_preempt {
            metrics::counter!(
                "sinomo.kv.raft.local_read_load_preemptions",
                "outcome" => "requester_departed"
            )
            .increment(1);
        }
        if was_front || cleared_stale_preempt {
            self.ready.notify_all();
        }
    }

    /// Remove callers whose admission budget has already expired while the
    /// slot mutex is held. This is also used by cold-read handoff, so a loader
    /// cannot publish a restored node specifically for a caller that no longer
    /// has authority to acquire it.
    fn remove_expired_checkout_waiters(&self, state: &mut ResidentRaftNodeSlotState, now: Instant) {
        let had_front = state.checkout_waiters.front().map(|waiter| waiter.ticket);
        state
            .checkout_waiters
            .retain(|waiter| now < waiter.deadline);
        let has_front = state.checkout_waiters.front().map(|waiter| waiter.ticket);
        if had_front != has_front {
            self.ready.notify_all();
        }
        if state.checkout_waiters.is_empty() {
            state.read_load_preempt_requested = false;
        }
    }

    fn checkout(&self, root: &Path, wait: Duration) -> Result<Option<ResidentRaftNode>, KvError> {
        self.touch();
        let mut state = self.state.lock().map_err(|error| KvError::Io {
            path: root.display().to_string(),
            detail: format!("resident cluster KV Raft node cache poisoned: {error}"),
        })?;
        state
            .checkout_waiters
            .try_reserve(1)
            .map_err(|_| KvError::ResourceExhausted {
                operation: "queue resident cluster KV Raft node checkout".to_string(),
                requested_bytes: std::mem::size_of::<u64>(),
            })?;
        let ticket = state.next_checkout_ticket;
        state.next_checkout_ticket = state.next_checkout_ticket.wrapping_add(1);
        let deadline = Instant::now() + wait;
        state
            .checkout_waiters
            .push_back(ResidentRaftCheckoutWaiter { ticket, deadline });
        let queue_depth = state.checkout_waiters.len();
        metrics::histogram!("sinomo.kv.raft.resident.checkout_queue_depth")
            .record(queue_depth as f64);
        let started = Instant::now();
        while state.checked_out
            || state.read_loading
            || state
                .checkout_waiters
                .front()
                .is_none_or(|waiter| waiter.ticket != ticket)
        {
            if state.read_loading && !state.read_load_preempt_requested {
                state.read_load_preempt_requested = true;
                metrics::counter!(
                    "sinomo.kv.raft.local_read_load_preemptions",
                    "outcome" => "requested"
                )
                .increment(1);
                self.ready.notify_all();
            }
            let now = Instant::now();
            if wait.is_zero() || now >= deadline {
                self.remove_checkout_waiter(&mut state, ticket);
                metrics::histogram!(
                    "sinomo.kv.raft.resident.checkout_wait.us",
                    "outcome" => "busy"
                )
                .record(started.elapsed().as_micros() as f64);
                return Err(KvError::Busy {
                    path: root.display().to_string(),
                });
            }
            let remaining = deadline.saturating_duration_since(now);
            let (next_state, timeout) = match self.ready.wait_timeout(state, remaining) {
                Ok(waited) => waited,
                Err(error) => {
                    let detail = format!(
                        "resident cluster KV Raft node cache poisoned while waiting: {error}"
                    );
                    let (mut poisoned_state, _) = error.into_inner();
                    self.remove_checkout_waiter(&mut poisoned_state, ticket);
                    return Err(KvError::Io {
                        path: root.display().to_string(),
                        detail,
                    });
                }
            };
            state = next_state;
            // A simultaneous release/handoff does not revive an expired
            // ticket. Condvar timeout status is advisory at the boundary, so
            // revalidate the absolute deadline before inspecting availability.
            if timeout.timed_out() || Instant::now() >= deadline {
                self.remove_checkout_waiter(&mut state, ticket);
                metrics::histogram!(
                    "sinomo.kv.raft.resident.checkout_wait.us",
                    "outcome" => "busy"
                )
                .record(started.elapsed().as_micros() as f64);
                return Err(KvError::Busy {
                    path: root.display().to_string(),
                });
            }
        }
        // The slot may have become available at exactly the timeout boundary.
        // Admission is authorized by the deadline, not by queue position.
        if !wait.is_zero() && Instant::now() >= deadline {
            self.remove_checkout_waiter(&mut state, ticket);
            metrics::histogram!(
                "sinomo.kv.raft.resident.checkout_wait.us",
                "outcome" => "busy"
            )
            .record(started.elapsed().as_micros() as f64);
            return Err(KvError::Busy {
                path: root.display().to_string(),
            });
        }
        let dequeued_ticket = state
            .checkout_waiters
            .pop_front()
            .map(|waiter| waiter.ticket);
        debug_assert_eq!(dequeued_ticket, Some(ticket));
        state.checked_out = true;
        metrics::histogram!(
            "sinomo.kv.raft.resident.checkout_wait.us",
            "outcome" => "acquired"
        )
        .record(started.elapsed().as_micros() as f64);
        Ok(state.cached.take())
    }

    fn checkin(&self, resident: ResidentRaftNode) {
        // Completion, rather than admission, is the meaningful recency point
        // for a long durable operation. Once its operation-owned `Arc` drops,
        // the just-published resident view must not look older than unrelated
        // roots touched while the operation was running.
        self.touch();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.read_metadata = Some(ResidentRaftReadMetadata::from_resident(
            &resident,
            state.read_metadata.as_ref(),
        ));
        state.cached = Some(resident);
        state.checked_out = false;
        state.read_load_preempt_requested = false;
        // Wake every waiter so the FIFO front is guaranteed to run. `notify_one`
        // can wake a later ticket, which must go back to sleep and can leave the
        // eligible front waiter asleep indefinitely.
        self.ready.notify_all();
    }

    fn release_without_cache(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.cached = None;
        state.read_metadata = None;
        state.checked_out = false;
        state.read_loading = false;
        state.read_load_preempt_requested = false;
        self.ready.notify_all();
    }

    fn clear_read_metadata(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.read_metadata = None;
    }
}

/// Serializes the one cold snapshot/WAL replay needed to seed an immutable
/// local read view. This is deliberately separate from resident writer
/// ownership: completing or unwinding the load releases the flock and wakes
/// every reader waiting to recheck the snapshot cache.
pub(crate) struct ResidentRaftColdReadGuard {
    slot: Arc<ResidentRaftNodeSlot>,
    complete: bool,
    started: Instant,
}

pub(crate) enum ResidentRaftColdReadStart {
    Load(ResidentRaftColdReadGuard),
    /// Another read just populated the immutable view cache. The caller must
    /// recheck that cache rather than mistaking the registry-only slot for a
    /// resident writer.
    RetryImmutableCache,
    /// An actual idle resident writer appeared after the caller's first
    /// resident read probe.
    RetryResident,
}

impl ResidentRaftColdReadGuard {
    pub(crate) fn preempt_requested(&self) -> bool {
        self.slot
            .state
            .lock()
            .map(|state| state.read_load_preempt_requested)
            .unwrap_or(true)
    }

    pub(crate) fn yield_before_open(mut self) {
        let mut state = self
            .slot
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.read_loading = false;
        state.read_load_preempt_requested = false;
        self.complete = true;
        drop(state);
        metrics::counter!(
            "sinomo.kv.raft.local_read_load_preemptions",
            "outcome" => "yielded_before_open"
        )
        .increment(1);
        metrics::histogram!(
            "sinomo.kv.raft.local_read_load.us",
            "outcome" => "yielded_before_open"
        )
        .record(self.started.elapsed().as_micros() as f64);
        self.slot.ready.notify_all();
    }

    /// Atomically hands an operation-scoped restored node to the exact FIFO
    /// writer that is still waiting for it. A preemption observation alone is
    /// insufficient: the writer may exhaust its admission budget between that
    /// observation and this handoff. In that case the caller keeps both the
    /// loader and node and finishes constructing the immutable read view.
    pub(crate) fn try_yield_restored_node(
        mut self,
        mut node: DurableRaftKvNode,
    ) -> Result<(), (Self, DurableRaftKvNode)> {
        let mut state = self
            .slot
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.slot
            .remove_expired_checkout_waiters(&mut state, Instant::now());
        if !state.read_loading
            || !state.read_load_preempt_requested
            || state.checkout_waiters.front().is_none()
        {
            state.read_load_preempt_requested = false;
            drop(state);
            return Err((self, node));
        }

        let Some(identity) = raft_view_identity(&node.paths) else {
            state.read_load_preempt_requested = false;
            drop(state);
            return Err((self, node));
        };
        let ownership_matches = node.lock.as_ref().is_some_and(|lock| {
            file_identity(&node.paths.lock) == Some(node.lock_identity)
                && file_handle_identity(lock, &node.paths.lock).ok() == Some(node.lock_identity)
                && directory_ino(&node.paths.root).ok() == Some(node.root_ino)
        });
        if !ownership_matches {
            state.read_load_preempt_requested = false;
            drop(state);
            return Err((self, node));
        }

        let lock = node.lock.take().expect("validated cold-read handoff lock");
        let resident = ResidentRaftNode {
            node_id: node.node_id.clone(),
            identity,
            lock_identity: node.lock_identity,
            root_ino: node.root_ino,
            lock,
            node: std::mem::replace(&mut node.node, ReplicatedKvNode::empty()),
            wal_synced_in_process: node.wal_synced_in_process,
        };
        node.cache_on_drop = false;
        node.resident_slot = None;
        state.read_metadata = Some(ResidentRaftReadMetadata::from_resident(
            &resident,
            state.read_metadata.as_ref(),
        ));
        state.cached = Some(resident);
        state.read_loading = false;
        state.read_load_preempt_requested = false;
        self.complete = true;
        drop(state);
        self.slot.touch();
        // Test-only synchronization for the exact cold-read handoff window:
        // the replacement node is now idle and visible to a ReadIndex probe,
        // while the waiting writer has not yet been notified to open/ACK it.
        pause_resident_raft_after_handoff_for_test(&node.paths.root);
        drop(node);
        metrics::counter!(
            "sinomo.kv.raft.local_read_load_preemptions",
            "outcome" => "resident_adopted"
        )
        .increment(1);
        metrics::histogram!(
            "sinomo.kv.raft.local_read_load.us",
            "outcome" => "resident_adopted"
        )
        .record(self.started.elapsed().as_micros() as f64);
        self.slot.ready.notify_all();
        Ok(())
    }

    pub(crate) fn complete(
        mut self,
        node_id: NodeId,
        identity: RaftViewIdentity,
        revision: KvRevision,
        observed_term: u64,
        applied_index: u64,
        known_leader: Option<NodeId>,
        raft_config_entry: Option<KvEntry>,
    ) {
        self.slot.touch();
        let mut state = self
            .slot
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.read_metadata = Some(ResidentRaftReadMetadata {
            node_id,
            identity,
            lock_identity: identity.lock,
            root_ino: identity.root.map(|root| root.ino),
            revision,
            observed_term,
            applied_index,
            known_leader,
            raft_config_entry: raft_config_entry.map(Arc::new),
        });
        state.read_loading = false;
        state.read_load_preempt_requested = false;
        self.complete = true;
        metrics::histogram!(
            "sinomo.kv.raft.local_read_load.us",
            "outcome" => "completed"
        )
        .record(self.started.elapsed().as_micros() as f64);
        self.slot.ready.notify_all();
    }
}

impl Drop for ResidentRaftColdReadGuard {
    fn drop(&mut self) {
        if self.complete {
            return;
        }
        let mut state = self
            .slot
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.read_loading = false;
        state.read_load_preempt_requested = false;
        self.slot.ready.notify_all();
        metrics::counter!("sinomo.kv.raft.local_read_loads", "outcome" => "abandoned").increment(1);
        metrics::histogram!(
            "sinomo.kv.raft.local_read_load.us",
            "outcome" => "abandoned"
        )
        .record(self.started.elapsed().as_micros() as f64);
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ResidentRaftRegistryAdmission {
    evictions: u64,
    active_overflow: bool,
}

fn resident_raft_node_slot_in_registry(
    slots: &mut HashMap<PathBuf, Arc<ResidentRaftNodeSlot>>,
    root: &Path,
) -> (Arc<ResidentRaftNodeSlot>, ResidentRaftRegistryAdmission) {
    if let Some(slot) = slots.get(root) {
        slot.touch();
        return (slot.clone(), ResidentRaftRegistryAdmission::default());
    }
    let mut admission = ResidentRaftRegistryAdmission::default();
    while slots.len() >= RESIDENT_RAFT_NODE_CAPACITY {
        // A registry-only reference means no operation has the slot checked out.
        // Evicting it drops the retained file lock and bounded resident state.
        let eviction = slots
            .iter()
            .filter(|(_, slot)| Arc::strong_count(slot) == 1)
            .min_by_key(|(_, slot)| slot.access_generation())
            .map(|(path, _)| path.clone());
        let Some(path) = eviction else {
            // Same-root operations must always share one coordination slot.
            // Returning an unregistered operation-scoped slot here lets a
            // concurrent caller create a second slot and collide on the durable
            // node lock. Permit only a transient overflow for roots that are all
            // genuinely active; the loop prunes back to the hard retained-cache
            // capacity as soon as an inactive slot exists.
            admission.active_overflow = true;
            break;
        };
        slots.remove(&path);
        admission.evictions = admission.evictions.saturating_add(1);
    }
    let slot = Arc::new(ResidentRaftNodeSlot::default());
    slot.touch();
    slots.insert(root.to_path_buf(), slot.clone());
    (slot, admission)
}

fn resident_raft_node_slot(root: &Path) -> Result<Arc<ResidentRaftNodeSlot>, KvError> {
    let mut slots = RESIDENT_RAFT_NODES.lock().map_err(|error| KvError::Io {
        path: root.display().to_string(),
        detail: format!("resident cluster KV Raft node registry poisoned: {error}"),
    })?;
    // Recheck while holding the registry lock. Either an opener observes a
    // concurrent denial, or denial observes the opener's active slot.
    require_process_local_raft_access(root)?;
    let (slot, admission) = resident_raft_node_slot_in_registry(&mut slots, root);
    if admission.evictions > 0 {
        metrics::counter!("sinomo.kv.raft.resident.evictions").increment(admission.evictions);
    }
    if admission.active_overflow {
        metrics::counter!("sinomo.kv.raft.resident.opens", "outcome" => "active_overflow")
            .increment(1);
    }
    Ok(slot)
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn registered_resident_raft_node_exists(paths: &KvWalPaths) -> bool {
    RESIDENT_RAFT_NODES
        .lock()
        .ok()
        .is_some_and(|slots| slots.contains_key(&paths.root))
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn cached_resident_raft_node_exists_for_test(paths: &KvWalPaths) -> bool {
    RESIDENT_RAFT_NODES
        .lock()
        .ok()
        .and_then(|slots| slots.get(&paths.root).cloned())
        .and_then(|slot| slot.state.lock().ok().map(|state| state.cached.is_some()))
        .unwrap_or(false)
}

#[cfg(test)]
pub(crate) fn resident_raft_cold_read_preempt_requested_for_test(paths: &KvWalPaths) -> bool {
    RESIDENT_RAFT_NODES
        .lock()
        .ok()
        .and_then(|slots| slots.get(&paths.root).cloned())
        .and_then(|slot| {
            slot.state
                .lock()
                .ok()
                .map(|state| state.read_load_preempt_requested)
        })
        .unwrap_or(false)
}

#[cfg(test)]
pub(crate) fn resident_raft_cold_read_waiter_count_for_test(paths: &KvWalPaths) -> usize {
    RESIDENT_RAFT_NODES
        .lock()
        .ok()
        .and_then(|slots| slots.get(&paths.root).cloned())
        .and_then(|slot| slot.state.lock().ok().map(|state| state.read_load_waiters))
        .unwrap_or(0)
}

#[cfg(any(test, feature = "test-support"))]
pub fn evict_resident_raft_node_for_test(paths: &KvWalPaths) -> bool {
    let slot = RESIDENT_RAFT_NODES
        .lock()
        .ok()
        .and_then(|mut slots| slots.remove(&paths.root));
    slot.is_some()
}

/// Keeps one test fixture's resident coordination slot registered while other
/// parallel fixtures churn the bounded process-wide cache.
///
/// This pins only the lightweight slot, not mutable node checkout. Production
/// code never needs it because one daemon owns one cluster root; integration
/// tests intentionally exercise many independent roots in one process.
#[cfg(any(test, feature = "test-support"))]
pub struct ResidentRaftNodeRegistryPin {
    _slot: Arc<ResidentRaftNodeSlot>,
}

#[cfg(any(test, feature = "test-support"))]
pub fn pin_resident_raft_node_registry_for_test(
    paths: &KvWalPaths,
) -> Result<ResidentRaftNodeRegistryPin, KvError> {
    resident_raft_node_slot(&paths.root).map(|slot| ResidentRaftNodeRegistryPin { _slot: slot })
}

/// Returns true only when the canonical resident writer for this exact durable
/// root is currently checked out. This is an observational scheduling probe,
/// not a freshness claim: callers may use it to avoid a doomed nonresident
/// flock attempt only when their consistency contract already permits the
/// materialized fallback.
pub(crate) fn resident_raft_writer_is_checked_out(paths: &KvWalPaths, node_id: &NodeId) -> bool {
    let Some(slot) = RESIDENT_RAFT_NODES
        .lock()
        .ok()
        .and_then(|slots| slots.get(&paths.root).cloned())
    else {
        return false;
    };
    slot.touch();
    let ownership = {
        let Ok(state) = slot.state.lock() else {
            return false;
        };
        if !state.checked_out || state.read_loading {
            return false;
        }
        let Some(metadata) = state.read_metadata.as_ref() else {
            return false;
        };
        if metadata.node_id != *node_id {
            return false;
        }
        (metadata.lock_identity, metadata.root_ino)
    };
    let (Some(lock_identity), Some(root_ino)) = ownership else {
        return false;
    };
    file_identity(&paths.lock) == Some(lock_identity)
        && directory_ino(&paths.root).ok() == Some(root_ino)
}

pub(crate) fn begin_resident_raft_cold_read(
    paths: &KvWalPaths,
    deadline: Option<Instant>,
) -> Result<ResidentRaftColdReadStart, KvError> {
    let slot = resident_raft_node_slot(&paths.root)?;
    let mut state = slot.state.lock().map_err(|error| KvError::Io {
        path: paths.root.display().to_string(),
        detail: format!("resident cluster KV Raft read loader poisoned: {error}"),
    })?;
    let wait_started = Instant::now();
    if deadline.is_some_and(|deadline| wait_started >= deadline) {
        metrics::counter!(
            "sinomo.kv.raft.local_read_load_admission",
            "outcome" => "deadline"
        )
        .increment(1);
        return Err(KvError::Busy {
            path: paths.root.display().to_string(),
        });
    }
    let mut waited_for_loader = false;
    if state.read_loading {
        state.read_load_waiters = state.read_load_waiters.saturating_add(1);
        metrics::gauge!("sinomo.kv.raft.local_read_load_waiters").increment(1.0);
    }
    while state.read_loading {
        waited_for_loader = true;
        let Some(deadline) = deadline else {
            metrics::counter!(
                "sinomo.kv.raft.local_read_load_admission",
                "outcome" => "busy_no_wait"
            )
            .increment(1);
            metrics::histogram!(
                "sinomo.kv.raft.local_read_load_wait.us",
                "outcome" => "busy_no_wait"
            )
            .record(wait_started.elapsed().as_micros() as f64);
            state.read_load_waiters = state.read_load_waiters.saturating_sub(1);
            metrics::gauge!("sinomo.kv.raft.local_read_load_waiters").decrement(1.0);
            return Err(KvError::Busy {
                path: paths.root.display().to_string(),
            });
        };
        let now = Instant::now();
        if now >= deadline {
            metrics::counter!(
                "sinomo.kv.raft.local_read_load_admission",
                "outcome" => "deadline"
            )
            .increment(1);
            metrics::histogram!(
                "sinomo.kv.raft.local_read_load_wait.us",
                "outcome" => "deadline"
            )
            .record(wait_started.elapsed().as_micros() as f64);
            state.read_load_waiters = state.read_load_waiters.saturating_sub(1);
            metrics::gauge!("sinomo.kv.raft.local_read_load_waiters").decrement(1.0);
            return Err(KvError::Busy {
                path: paths.root.display().to_string(),
            });
        }
        let (next, timeout) = slot
            .ready
            .wait_timeout(state, deadline.saturating_duration_since(now))
            .map_err(|error| KvError::Io {
                path: paths.root.display().to_string(),
                detail: format!(
                    "resident cluster KV Raft read loader poisoned while waiting: {error}"
                ),
            })?;
        state = next;
        if timeout.timed_out() && state.read_loading {
            metrics::counter!(
                "sinomo.kv.raft.local_read_load_admission",
                "outcome" => "deadline"
            )
            .increment(1);
            metrics::histogram!(
                "sinomo.kv.raft.local_read_load_wait.us",
                "outcome" => "deadline"
            )
            .record(wait_started.elapsed().as_micros() as f64);
            state.read_load_waiters = state.read_load_waiters.saturating_sub(1);
            metrics::gauge!("sinomo.kv.raft.local_read_load_waiters").decrement(1.0);
            return Err(KvError::Busy {
                path: paths.root.display().to_string(),
            });
        }
    }
    if waited_for_loader {
        state.read_load_waiters = state.read_load_waiters.saturating_sub(1);
        metrics::gauge!("sinomo.kv.raft.local_read_load_waiters").decrement(1.0);
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            metrics::counter!(
                "sinomo.kv.raft.local_read_load_admission",
                "outcome" => "deadline"
            )
            .increment(1);
            metrics::histogram!(
                "sinomo.kv.raft.local_read_load_wait.us",
                "outcome" => "deadline"
            )
            .record(wait_started.elapsed().as_micros() as f64);
            return Err(KvError::Busy {
                path: paths.root.display().to_string(),
            });
        }
        metrics::counter!(
            "sinomo.kv.raft.local_read_load_admission",
            "outcome" => "joined"
        )
        .increment(1);
        metrics::histogram!(
            "sinomo.kv.raft.local_read_load_wait.us",
            "outcome" => "joined"
        )
        .record(wait_started.elapsed().as_micros() as f64);
        return Ok(ResidentRaftColdReadStart::RetryImmutableCache);
    }
    if state.cached.is_some() {
        return Ok(ResidentRaftColdReadStart::RetryResident);
    }
    if state.checked_out || !state.checkout_waiters.is_empty() {
        return Err(KvError::Busy {
            path: paths.root.display().to_string(),
        });
    }
    // Authority-budgeted reads may join a cold restore already doing useful
    // work, but they must never start snapshot/WAL replay that cannot be
    // interrupted at their deadline. Unbounded local reads remain responsible
    // for the once-per-process recovery load.
    if deadline.is_some() {
        metrics::counter!(
            "sinomo.kv.raft.local_read_load_admission",
            "outcome" => "deadline_owner_refused"
        )
        .increment(1);
        return Err(KvError::Busy {
            path: paths.root.display().to_string(),
        });
    }
    state.read_load_preempt_requested = false;
    state.read_loading = true;
    drop(state);
    metrics::counter!(
        "sinomo.kv.raft.local_read_load_admission",
        "outcome" => "owner"
    )
    .increment(1);
    Ok(ResidentRaftColdReadStart::Load(ResidentRaftColdReadGuard {
        slot,
        complete: false,
        started: Instant::now(),
    }))
}

fn resident_raft_ownership_matches(
    resident: &ResidentRaftNode,
    paths: &KvWalPaths,
    node_id: &NodeId,
) -> bool {
    resident.node_id == *node_id
        && file_identity(&paths.lock) == Some(resident.lock_identity)
        && file_handle_identity(&resident.lock, &paths.lock).ok() == Some(resident.lock_identity)
        && directory_ino(&paths.root).ok() == Some(resident.root_ino)
        && raft_view_identity(paths) == Some(resident.identity)
}

fn with_idle_resident_raft_node<T, F>(
    paths: &KvWalPaths,
    node_id: &NodeId,
    read: F,
) -> Option<Result<KvLocalRead<T>, KvError>>
where
    F: FnOnce(&ReplicatedKvNode) -> Result<T, KvError>,
{
    if process_local_raft_access_is_denied(&paths.root) {
        return None;
    }
    let slot = RESIDENT_RAFT_NODES.lock().ok()?.get(&paths.root)?.clone();
    slot.touch();
    let mut state = slot.state.lock().ok()?;
    if state.checked_out || state.read_loading {
        return None;
    }
    let valid = state
        .cached
        .as_ref()
        .is_some_and(|resident| resident_raft_ownership_matches(resident, paths, node_id));
    if !valid {
        if state.cached.take().is_some() {
            state.read_metadata = None;
            metrics::counter!("sinomo.kv.raft.resident.opens", "outcome" => "read_invalidated")
                .increment(1);
        }
        return None;
    }
    let resident = state.cached.as_ref()?;
    Some(read(&resident.node).map(|value| KvLocalRead {
        value,
        revision: resident.node.store.revision(),
        observed_term: resident.node.hard_state.current_term,
        applied_index: resident.node.applied_index,
        known_leader: resident.node.hard_state.leader_id.clone(),
    }))
}

fn with_idle_resident_raft_node_before<T, F>(
    paths: &KvWalPaths,
    node_id: &NodeId,
    deadline: Instant,
    read: F,
) -> Option<Result<KvLocalRead<T>, KvError>>
where
    F: FnOnce(&ReplicatedKvNode) -> Result<T, KvError>,
{
    if process_local_raft_access_is_denied(&paths.root) {
        return None;
    }
    let slot = RESIDENT_RAFT_NODES.lock().ok()?.get(&paths.root)?.clone();
    slot.touch();
    let started = Instant::now();
    let mut state = slot.state.lock().ok()?;
    loop {
        slot.remove_expired_checkout_waiters(&mut state, Instant::now());
        if !state.checked_out && !state.read_loading && state.checkout_waiters.is_empty() {
            break;
        }
        let now = Instant::now();
        if now >= deadline {
            metrics::histogram!(
                "sinomo.kv.raft.resident.read_projection_wait.us",
                "outcome" => "deadline"
            )
            .record(started.elapsed().as_micros() as f64);
            return None;
        }
        let remaining = deadline.saturating_duration_since(now);
        let (next_state, timeout) = slot.ready.wait_timeout(state, remaining).ok()?;
        state = next_state;
        if timeout.timed_out() && Instant::now() >= deadline {
            metrics::histogram!(
                "sinomo.kv.raft.resident.read_projection_wait.us",
                "outcome" => "deadline"
            )
            .record(started.elapsed().as_micros() as f64);
            return None;
        }
    }
    let valid = state
        .cached
        .as_ref()
        .is_some_and(|resident| resident_raft_ownership_matches(resident, paths, node_id));
    if !valid {
        if state.cached.take().is_some() {
            state.read_metadata = None;
            metrics::counter!("sinomo.kv.raft.resident.opens", "outcome" => "read_invalidated")
                .increment(1);
        }
        metrics::histogram!(
            "sinomo.kv.raft.resident.read_projection_wait.us",
            "outcome" => "invalidated"
        )
        .record(started.elapsed().as_micros() as f64);
        return None;
    }
    let resident = state.cached.as_ref()?;
    let result = read(&resident.node).map(|value| KvLocalRead {
        value,
        revision: resident.node.store.revision(),
        observed_term: resident.node.hard_state.current_term,
        applied_index: resident.node.applied_index,
        known_leader: resident.node.hard_state.leader_id.clone(),
    });
    metrics::histogram!(
        "sinomo.kv.raft.resident.read_projection_wait.us",
        "outcome" => "acquired"
    )
    .record(started.elapsed().as_micros() as f64);
    Some(result)
}

/// A bounded, immutable projection borrowed from the canonical resident Raft
/// node while its coordination slot is read-locked.
///
/// This is the post-ReadIndex equivalent of etcd's MVCC read transaction: many
/// readers can project point/range values without checking the mutable Raft node
/// out through the operation queue. The enclosing callback is intentionally
/// synchronous and bounded; callers must not retain this borrow or perform
/// network/durable work from it.
pub struct ClusterKvRaftResidentReadProjection<'a> {
    node: &'a ReplicatedKvNode,
}

impl ClusterKvRaftResidentReadProjection<'_> {
    pub fn read_index_state(&self) -> ClusterKvRaftReadIndexState {
        let current_term = self.node.hard_state.current_term;
        let commit_index = self.node.hard_state.commit_index;
        let commit_term = self.node.term_at(commit_index);
        ClusterKvRaftReadIndexState {
            current_term,
            leader_id: self.node.hard_state.leader_id.clone(),
            commit_index,
            commit_term,
            applied_index: self.node.applied_index,
            revision: self.node.store.revision(),
            has_current_term_commit: commit_index > 0 && commit_term == Some(current_term),
        }
    }

    pub fn read_index_snapshot(&self) -> Result<KvSnapshot, KvError> {
        self.node.store.try_snapshot()
    }

    pub fn read_index_entry(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
        self.node
            .store
            .get(key)
            .map(|entry| try_clone_kv_entry(entry, "read shared resident Raft KV entry"))
            .transpose()
    }

    pub fn pending_current_term_barrier(&self) -> Result<Option<KvLogEntry>, KvError> {
        self.node.pending_current_term_barrier()
    }

    pub fn build_read_index_heartbeat(
        &self,
        node_id: &NodeId,
        state: &ClusterBootstrapState,
        commit_index: u64,
    ) -> Result<ClusterRaftAppendRequest, KvError> {
        if node_id != &state.local_node_id
            || self.node.hard_state.current_term != state.term
            || self.node.hard_state.leader_id.as_ref() != Some(node_id)
        {
            return Err(KvError::AuthorityUnavailable {
                detail: format!(
                    "cannot build shared ReadIndex heartbeat for {} at state term {} from resident term {} with leader {}",
                    state.local_node_id,
                    state.term,
                    self.node.hard_state.current_term,
                    self.node
                        .hard_state
                        .leader_id
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_else(|| "none".to_string())
                ),
            });
        }
        if commit_index > self.node.hard_state.commit_index {
            return Err(KvError::AuthorityUnavailable {
                detail: format!(
                    "shared ReadIndex heartbeat anchor {commit_index} exceeds committed Raft index {}",
                    self.node.hard_state.commit_index
                ),
            });
        }
        let prev_log_term = self.node.term_at(commit_index).ok_or_else(|| {
            KvError::AuthorityUnavailable {
                detail: format!(
                    "shared ReadIndex heartbeat anchor {commit_index} is not retained by the resident Raft node"
                ),
            }
        })?;
        Ok(ClusterRaftAppendRequest {
            state: state.clone(),
            snapshot: None,
            append: RaftAppendEntriesRequest {
                term: self.node.hard_state.current_term,
                leader_id: node_id.clone(),
                prev_log_index: commit_index,
                prev_log_term,
                entries: Vec::new(),
                leader_commit: commit_index,
            },
            read_index_probe: true,
        })
    }

    pub fn read_index_entries(&self, keys: &[String]) -> Result<Vec<Option<KvEntry>>, KvError> {
        if keys.len() > CLUSTER_KV_AUTHORITATIVE_ENTRIES_MAX_KEYS {
            return Err(KvError::ResourceExhausted {
                operation: "read shared resident Raft exact-key set cardinality".to_string(),
                requested_bytes: keys.len(),
            });
        }
        let mut entries = Vec::new();
        entries
            .try_reserve(keys.len())
            .map_err(|_| KvError::ResourceExhausted {
                operation: "read shared resident Raft exact-key set".to_string(),
                requested_bytes: keys
                    .len()
                    .saturating_mul(std::mem::size_of::<Option<KvEntry>>()),
            })?;
        let mut payload_bytes = 0_usize;
        for key in keys {
            let entry = self
                .node
                .store
                .get(key)
                .map(|entry| {
                    payload_bytes = payload_bytes.saturating_add(kv_entry_payload_bytes(entry));
                    try_clone_kv_entry(entry, "read shared resident Raft exact-key set")
                })
                .transpose()?;
            if payload_bytes > CLUSTER_KV_AUTHORITATIVE_ENTRIES_MAX_PAYLOAD_BYTES {
                return Err(KvError::ResourceExhausted {
                    operation: "read shared resident Raft exact-key set payload".to_string(),
                    requested_bytes: payload_bytes,
                });
            }
            entries.push(entry);
        }
        Ok(entries)
    }

    pub fn read_index_request_outcome(&self, request_id: &KvRequestId) -> Option<KvRequestOutcome> {
        if let Some(record) = self.node.store.committed_request(request_id) {
            return Some(KvRequestOutcome::Committed(record.clone()));
        }
        self.node
            .store
            .request_id_expired(request_id)
            .then_some(KvRequestOutcome::Expired)
    }

    pub fn read_index_prefix_limited_by_payload_bytes(
        &self,
        prefix: &str,
        max_entries: usize,
        max_payload_bytes: usize,
    ) -> Result<Vec<KvEntry>, KvError> {
        self.node.store.range_prefix_limited_by_payload_bytes(
            prefix,
            max_entries,
            max_payload_bytes,
        )
    }
}

/// Runs one bounded projection against the idle canonical resident node without
/// taking the exclusive Raft operation or checkout gates.
///
/// `None` means the resident is cold, invalid, or owned by a mutation. The
/// caller must retry or fail closed; this function never replays durable state
/// and never falls back to the materialized snapshot.
pub fn with_cluster_kv_resident_read_projection<T, F>(
    paths: &KvWalPaths,
    node_id: &NodeId,
    read: F,
) -> Option<Result<KvLocalRead<T>, KvError>>
where
    F: FnOnce(&mut ClusterKvRaftResidentReadProjection<'_>) -> T,
{
    with_idle_resident_raft_node(paths, node_id, |node| {
        let _authority_lease = acquire_pending_higher_term_authority_lease_for_root(
            &paths.root,
            Some(node.hard_state.current_term),
            "project resident Raft ReadIndex state",
        )?;
        let mut projection = ClusterKvRaftResidentReadProjection { node };
        Ok(read(&mut projection))
    })
}

/// Wait for a transient resident mutation to finish, then borrow the exact
/// canonical Raft view before the caller's existing absolute deadline.
///
/// The resident slot remains the authority fence. Queued mutable operations
/// retain FIFO priority, and the projection still fails closed if ownership or
/// the durable view changes before admission.
pub fn with_cluster_kv_resident_read_projection_before<T, F>(
    paths: &KvWalPaths,
    node_id: &NodeId,
    deadline: Instant,
    read: F,
) -> Option<Result<KvLocalRead<T>, KvError>>
where
    F: FnOnce(&mut ClusterKvRaftResidentReadProjection<'_>) -> T,
{
    with_idle_resident_raft_node_before(paths, node_id, deadline, |node| {
        let _authority_lease = acquire_pending_higher_term_authority_lease_for_root(
            &paths.root,
            Some(node.hard_state.current_term),
            "project resident Raft ReadIndex state",
        )?;
        let mut projection = ClusterKvRaftResidentReadProjection { node };
        Ok(read(&mut projection))
    })
}

fn read_index_probe_metadata_from_node(
    node: &ReplicatedKvNode,
) -> Result<ClusterKvRaftReadIndexProbeMetadata, KvError> {
    Ok(ClusterKvRaftReadIndexProbeMetadata {
        revision: node.store.revision(),
        observed_term: node.hard_state.current_term,
        applied_index: node.applied_index,
        known_leader: node.hard_state.leader_id.clone(),
        raft_config_entry: node
            .store
            .get(&cluster_kv_raft_config_key())
            .map(|entry| try_clone_kv_entry(entry, "capture resident ReadIndex probe config"))
            .transpose()?,
    })
}

fn read_index_probe_metadata_from_retained(
    metadata: &ResidentRaftReadMetadata,
) -> Result<ClusterKvRaftReadIndexProbeMetadata, KvError> {
    Ok(ClusterKvRaftReadIndexProbeMetadata {
        revision: metadata.revision,
        observed_term: metadata.observed_term,
        applied_index: metadata.applied_index,
        known_leader: metadata.known_leader.clone(),
        raft_config_entry: metadata
            .raft_config_entry
            .as_deref()
            .map(|entry| try_clone_kv_entry(entry, "capture retained ReadIndex probe config"))
            .transpose()?,
    })
}

fn read_resident_raft_read_index_probe(
    paths: &KvWalPaths,
    node_id: &NodeId,
    request: &RaftAppendEntriesRequest,
) -> Result<ClusterKvRaftReadIndexProbeOutcome, KvError> {
    if !request.entries.is_empty() {
        return Ok(ClusterKvRaftReadIndexProbeOutcome::NeedsDurable);
    }
    let Some(slot) = RESIDENT_RAFT_NODES
        .lock()
        .map_err(|error| KvError::Io {
            path: paths.root.display().to_string(),
            detail: format!("resident cluster KV Raft node registry poisoned: {error}"),
        })?
        .get(&paths.root)
        .cloned()
    else {
        return Ok(ClusterKvRaftReadIndexProbeOutcome::NeedsDurable);
    };
    slot.touch();
    let mut state = slot.state.lock().map_err(|error| KvError::Io {
        path: paths.root.display().to_string(),
        detail: format!("resident cluster KV Raft node cache poisoned: {error}"),
    })?;

    if state.checked_out || state.read_loading {
        let Some(metadata) = state
            .read_metadata
            .as_ref()
            .filter(|metadata| metadata.node_id == *node_id)
        else {
            return Ok(ClusterKvRaftReadIndexProbeOutcome::NeedsDurable);
        };
        if request.term > metadata.observed_term {
            // A higher term is durable safety state. It must be observed by the
            // ordinary AppendEntries path once the current owner checks in.
            return Ok(ClusterKvRaftReadIndexProbeOutcome::NeedsDurable);
        }
        let metadata = read_index_probe_metadata_from_retained(metadata)?;
        metrics::counter!(
            "sinomo.kv.raft.read_index_probe",
            "outcome" => "resident_busy"
        )
        .increment(1);
        return Ok(ClusterKvRaftReadIndexProbeOutcome::Busy { metadata });
    }

    let valid = state
        .cached
        .as_ref()
        .is_some_and(|resident| resident_raft_ownership_matches(resident, paths, node_id));
    if !valid {
        if state.cached.take().is_some() {
            state.read_metadata = None;
            metrics::counter!("sinomo.kv.raft.resident.opens", "outcome" => "probe_invalidated")
                .increment(1);
        }
        return Ok(ClusterKvRaftReadIndexProbeOutcome::NeedsDurable);
    }
    let resident = state
        .cached
        .as_ref()
        .expect("validated resident ReadIndex probe node");
    // Preserve the resident-slot -> higher-term-gate lock order used by every
    // resident projection. This is the linearizing authority check: a cold
    // reader may have handed off a replacement node before its waiting writer
    // opens and acknowledges that lower durable term.
    let _authority_lease = match acquire_pending_higher_term_authority_lease_for_root(
        &paths.root,
        Some(resident.node.hard_state.current_term),
        "acknowledge a Raft ReadIndex probe",
    ) {
        Ok(lease) => lease,
        Err(KvError::AuthorityUnavailable { .. }) => {
            return Ok(ClusterKvRaftReadIndexProbeOutcome::NeedsDurable);
        }
        Err(error) => return Err(error),
    };
    let Some(response) = resident.node.read_only_append_entries_response(request) else {
        return Ok(ClusterKvRaftReadIndexProbeOutcome::NeedsDurable);
    };
    let metadata = read_index_probe_metadata_from_node(&resident.node)?;
    metrics::counter!(
        "sinomo.kv.raft.read_index_probe",
        "outcome" => if response.success {
            "resident_accepted"
        } else {
            "resident_rejected"
        }
    )
    .increment(1);
    Ok(ClusterKvRaftReadIndexProbeOutcome::Responded { response, metadata })
}

pub(crate) fn resident_raft_local_read_metadata(
    paths: &KvWalPaths,
    node_id: &NodeId,
) -> Option<(KvRevision, u64, u64, Option<NodeId>)> {
    if process_local_raft_access_is_denied(&paths.root) {
        return None;
    }
    let slot = RESIDENT_RAFT_NODES.lock().ok()?.get(&paths.root)?.clone();
    slot.touch();
    let mut state = slot.state.lock().ok()?;
    if !state.checked_out && !state.read_loading {
        if let Some(resident) = state.cached.as_ref() {
            if !resident_raft_ownership_matches(resident, paths, node_id) {
                state.cached = None;
                state.read_metadata = None;
                return None;
            }
        } else if state.read_metadata.as_ref().is_some_and(|metadata| {
            metadata.node_id != *node_id || raft_view_identity(paths) != Some(metadata.identity)
        }) {
            state.read_metadata = None;
            return None;
        }
    }
    let metadata = state.read_metadata.as_ref()?;
    if metadata.node_id != *node_id {
        return None;
    }
    Some((
        metadata.revision,
        metadata.observed_term,
        metadata.applied_index,
        metadata.known_leader.clone(),
    ))
}

pub(crate) fn read_resident_raft_local_entry(
    paths: &KvWalPaths,
    node_id: &NodeId,
    key: &str,
) -> Option<Result<KvLocalRead<Option<KvEntry>>, KvError>> {
    if process_local_raft_access_is_denied(&paths.root) {
        return None;
    }
    // Consensus configuration is a fixed, bounded projection retained beside
    // the resident metadata. Unlike arbitrary keys it remains readable while
    // the mutable node is checked out, so consensus never resurrects a stale
    // voter set from the materialized fallback.
    if key == cluster_kv_raft_config_key()
        && let Some(slot) = RESIDENT_RAFT_NODES.lock().ok()?.get(&paths.root).cloned()
    {
        slot.touch();
        let state = slot.state.lock().ok()?;
        if state.checked_out && !state.read_loading {
            let metadata = state.read_metadata.as_ref()?;
            if metadata.node_id != *node_id {
                return None;
            }
            let value = metadata
                .raft_config_entry
                .as_deref()
                .map(|entry| try_clone_kv_entry(entry, "read resident Raft config projection"))
                .transpose();
            return Some(value.map(|value| KvLocalRead {
                value,
                revision: metadata.revision,
                observed_term: metadata.observed_term,
                applied_index: metadata.applied_index,
                known_leader: metadata.known_leader.clone(),
            }));
        }
    }
    with_idle_resident_raft_node(paths, node_id, |node| {
        node.store
            .get(key)
            .map(|entry| try_clone_kv_entry(entry, "read resident local KV entry"))
            .transpose()
    })
}

pub(crate) fn read_resident_raft_configuration_floor(
    paths: &KvWalPaths,
    node_id: &NodeId,
) -> Result<KvLocalRead<Option<KvEntry>>, KvError> {
    require_process_local_raft_access(&paths.root)?;
    let busy = || KvError::Busy {
        path: paths.root.display().to_string(),
    };
    // These are the existing metadata critical sections, not operation or
    // checkout admission. The mutable owner releases the slot mutex before
    // doing storage work. Joining its short metadata handoff avoids turning
    // an otherwise known completed floor into a transient foreground failure.
    let slot = RESIDENT_RAFT_NODES
        .lock()
        .map_err(|_| busy())?
        .get(&paths.root)
        .cloned()
        .ok_or_else(busy)?;
    slot.touch();
    let state = slot.state.lock().map_err(|_| busy())?;
    if state.read_loading {
        return Err(busy());
    }
    let metadata = state.read_metadata.as_ref().ok_or_else(busy)?;
    if metadata.node_id != *node_id {
        return Err(busy());
    }
    let valid_owner = if state.checked_out {
        // Normal WAL/hard-state writes change the full view identity while the
        // same owner is checked out. The lock and root identities must remain
        // exact; an unknown marker or a same-path replacement cannot borrow the
        // previous owner's retained configuration.
        metadata.lock_identity.is_some()
            && metadata.root_ino.is_some()
            && file_identity(&paths.lock) == metadata.lock_identity
            && directory_ino(&paths.root).ok() == metadata.root_ino
    } else {
        state
            .cached
            .as_ref()
            .is_some_and(|resident| resident_raft_ownership_matches(resident, paths, node_id))
    };
    if !valid_owner {
        return Err(busy());
    }
    // Retain the established slot -> higher-term-gate lock order. The lease
    // rejects any observed higher term before the fixed projection is copied;
    // publication racing after admission is ordered after this bounded read.
    let _authority_lease = acquire_pending_higher_term_authority_lease_for_root(
        &paths.root,
        Some(metadata.observed_term),
        "read resident Raft configuration floor",
    )?;
    Ok(KvLocalRead {
        value: metadata
            .raft_config_entry
            .as_deref()
            .map(|entry| try_clone_kv_entry(entry, "read resident Raft configuration floor"))
            .transpose()?,
        revision: metadata.revision,
        observed_term: metadata.observed_term,
        applied_index: metadata.applied_index,
        known_leader: metadata.known_leader.clone(),
    })
}

pub(crate) fn read_resident_raft_local_entries(
    paths: &KvWalPaths,
    node_id: &NodeId,
    keys: &[String],
) -> Option<Result<KvLocalRead<Vec<Option<KvEntry>>>, KvError>> {
    with_idle_resident_raft_node(paths, node_id, |node| {
        if keys.len() > CLUSTER_KV_AUTHORITATIVE_ENTRIES_MAX_KEYS {
            return Err(KvError::ResourceExhausted {
                operation: "read resident local Raft exact-key set cardinality".to_string(),
                requested_bytes: keys.len(),
            });
        }
        let mut entries = Vec::new();
        entries
            .try_reserve(keys.len())
            .map_err(|_| KvError::ResourceExhausted {
                operation: "read resident local Raft exact-key set".to_string(),
                requested_bytes: keys
                    .len()
                    .saturating_mul(std::mem::size_of::<Option<KvEntry>>()),
            })?;
        let mut payload_bytes = 0_usize;
        for key in keys {
            let entry = node
                .store
                .get(key)
                .map(|entry| {
                    payload_bytes = payload_bytes.saturating_add(kv_entry_payload_bytes(entry));
                    try_clone_kv_entry(entry, "read resident local Raft exact-key set")
                })
                .transpose()?;
            if payload_bytes > CLUSTER_KV_AUTHORITATIVE_ENTRIES_MAX_PAYLOAD_BYTES {
                return Err(KvError::ResourceExhausted {
                    operation: "read resident local Raft exact-key set payload".to_string(),
                    requested_bytes: payload_bytes,
                });
            }
            entries.push(entry);
        }
        Ok(entries)
    })
}

pub(crate) fn read_resident_raft_local_prefix(
    paths: &KvWalPaths,
    node_id: &NodeId,
    prefix: &str,
) -> Option<Result<KvLocalRead<Vec<KvEntry>>, KvError>> {
    with_idle_resident_raft_node(paths, node_id, |node| node.store.range_prefix(prefix))
}

pub(crate) fn read_resident_raft_local_prefix_limited_by_payload_bytes(
    paths: &KvWalPaths,
    node_id: &NodeId,
    prefix: &str,
    max_entries: usize,
    max_payload_bytes: usize,
) -> Option<Result<KvLocalRead<Vec<KvEntry>>, KvError>> {
    with_idle_resident_raft_node(paths, node_id, |node| {
        node.store
            .range_prefix_limited_by_payload_bytes(prefix, max_entries, max_payload_bytes)
    })
}

pub(crate) fn read_resident_raft_local_prefix_matching(
    paths: &KvWalPaths,
    node_id: &NodeId,
    prefix: &str,
    matches_key: fn(&str) -> bool,
) -> Option<Result<KvLocalRead<Vec<KvEntry>>, KvError>> {
    with_idle_resident_raft_node(paths, node_id, |node| {
        let mut selected = Vec::new();
        let mut payload_bytes = 0_usize;
        for (scanned, entry) in node.store.prefix_entries(prefix).enumerate() {
            // Bound every examined key, including records outside the query.
            // Filtering never turns a truncated scan into a complete result.
            if scanned >= CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES {
                return Err(KvError::RangeLimit {
                    prefix: prefix.to_owned(),
                    max_entries: CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
                });
            }
            if !matches_key(&entry.key) {
                continue;
            }
            payload_bytes = payload_bytes.saturating_add(kv_entry_payload_bytes(entry));
            if payload_bytes > CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES {
                return Err(KvError::RangePayloadLimit {
                    prefix: prefix.to_owned(),
                    observed_bytes: payload_bytes,
                    max_bytes: CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES,
                });
            }
            try_reserve_kv_items(&mut selected, 1, "collect matching resident prefix")?;
            selected.push(try_clone_kv_entry(
                entry,
                "collect matching resident prefix",
            )?);
        }
        Ok(selected)
    })
}

pub(crate) fn read_resident_raft_local_snapshot(
    paths: &KvWalPaths,
    node_id: &NodeId,
) -> Option<Result<KvLocalRead<KvSnapshot>, KvError>> {
    with_idle_resident_raft_node(paths, node_id, |node| node.store.try_snapshot())
}

pub(crate) fn read_resident_raft_local_event_history(
    paths: &KvWalPaths,
    node_id: &NodeId,
) -> Option<Result<KvLocalRead<ClusterKvEventReadView>, KvError>> {
    with_idle_resident_raft_node(paths, node_id, |node| {
        let mut events = Vec::new();
        try_reserve_kv_items(
            &mut events,
            node.store.event_history_len(),
            "clone resident cluster KV event history",
        )?;
        for event in node.store.retained_events() {
            events.push(try_clone_kv_event(
                event,
                "clone resident cluster KV event history",
            )?);
        }
        Ok(ClusterKvEventReadView {
            compacted_revision: node.store.compacted_revision(),
            events,
        })
    })
}

#[cfg(test)]
static RESIDENT_RAFT_OPEN_COUNTS_FOR_TEST: LazyLock<Mutex<HashMap<PathBuf, (u64, u64)>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(test)]
static NONRESIDENT_RAFT_OPEN_COUNTS_FOR_TEST: LazyLock<Mutex<HashMap<PathBuf, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
#[cfg(test)]
static NONRESIDENT_RAFT_OPEN_ATTEMPTS_FOR_TEST: LazyLock<Mutex<HashMap<PathBuf, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[cfg(test)]
fn record_resident_raft_open_for_test(root: &Path, warm: bool) {
    let mut counts = RESIDENT_RAFT_OPEN_COUNTS_FOR_TEST
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let count = counts.entry(root.to_path_buf()).or_default();
    if warm {
        count.1 = count.1.saturating_add(1);
    } else {
        count.0 = count.0.saturating_add(1);
    }
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn reset_resident_raft_open_counts_for_test(root: &Path) {
    RESIDENT_RAFT_OPEN_COUNTS_FOR_TEST
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(root);
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn resident_raft_open_counts_for_test(root: &Path) -> (u64, u64) {
    RESIDENT_RAFT_OPEN_COUNTS_FOR_TEST
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(root)
        .copied()
        .unwrap_or_default()
}

#[cfg(test)]
pub(crate) fn reset_nonresident_raft_open_count_for_test(root: &Path) {
    NONRESIDENT_RAFT_OPEN_COUNTS_FOR_TEST
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(root);
}

#[cfg(test)]
pub(crate) fn nonresident_raft_open_count_for_test(root: &Path) -> u64 {
    NONRESIDENT_RAFT_OPEN_COUNTS_FOR_TEST
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(root)
        .copied()
        .unwrap_or_default()
}

#[cfg(test)]
pub(crate) fn reset_nonresident_raft_open_attempts_for_test(root: &Path) {
    NONRESIDENT_RAFT_OPEN_ATTEMPTS_FOR_TEST
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(root);
}

#[cfg(test)]
pub(crate) fn nonresident_raft_open_attempts_for_test(root: &Path) -> u64 {
    NONRESIDENT_RAFT_OPEN_ATTEMPTS_FOR_TEST
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(root)
        .copied()
        .unwrap_or_default()
}
#[cfg(test)]
pub(crate) static CLUSTER_KV_RAFT_SNAPSHOT_BYTES_LIMIT_FOR_TEST: AtomicU64 =
    AtomicU64::new(MAX_CLUSTER_KV_RAFT_SNAPSHOT_BYTES);
#[cfg(test)]
static CLUSTER_KV_RAFT_APPLY_FAILURES_FOR_TEST: LazyLock<Mutex<HashMap<PathBuf, usize>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn fail_cluster_kv_raft_apply_after_entries_for_test(
    paths: &KvWalPaths,
    applied_entries: usize,
) {
    assert!(applied_entries > 0, "failure must follow a partial apply");
    CLUSTER_KV_RAFT_APPLY_FAILURES_FOR_TEST
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(paths.root.clone(), applied_entries);
}

#[cfg(test)]
fn take_cluster_kv_raft_apply_failure_for_test(paths: &KvWalPaths) -> Option<usize> {
    CLUSTER_KV_RAFT_APPLY_FAILURES_FOR_TEST
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(&paths.root)
}

#[derive(Default)]
struct ClusterKvRaftOperationLockState {
    held: bool,
    waiters: VecDeque<u64>,
    next_ticket: u64,
}

struct ClusterKvRaftOperationLock {
    state: Mutex<ClusterKvRaftOperationLockState>,
    ready: Condvar,
}

struct ClusterKvRaftOperationWaiter {
    lock: Arc<ClusterKvRaftOperationLock>,
    ticket: u64,
    queued: bool,
    metric_active: bool,
    started: Instant,
}

impl ClusterKvRaftOperationWaiter {
    fn enqueue(lock: Arc<ClusterKvRaftOperationLock>, root: &Path) -> Result<Self, KvError> {
        let mut state = lock.state.lock().map_err(|error| KvError::Io {
            path: root.display().to_string(),
            detail: format!("cluster kv raft operation lock poisoned: {error}"),
        })?;
        state
            .waiters
            .try_reserve(1)
            .map_err(|_| KvError::ResourceExhausted {
                operation: "queue cluster kv raft operation".to_string(),
                requested_bytes: std::mem::size_of::<u64>(),
            })?;
        let ticket = state.next_ticket;
        state.next_ticket = state.next_ticket.wrapping_add(1);
        state.waiters.push_back(ticket);
        let queue_depth = state.waiters.len();
        drop(state);

        metrics::gauge!("sinomo.kv.raft.operation.queue_depth").increment(1.0);
        metrics::histogram!("sinomo.kv.raft.operation.queue_depth.observed")
            .record(queue_depth as f64);
        Ok(Self {
            lock,
            ticket,
            queued: true,
            metric_active: true,
            started: Instant::now(),
        })
    }

    fn finish_after_dequeue(mut self, outcome: &'static str) {
        self.queued = false;
        self.finish_metric(outcome);
    }

    fn cancel(mut self, outcome: &'static str) {
        self.remove_from_queue();
        self.finish_metric(outcome);
    }

    fn remove_from_queue(&mut self) {
        if !self.queued {
            return;
        }
        let mut state = self
            .lock
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let was_front = state.waiters.front().copied() == Some(self.ticket);
        state.waiters.retain(|ticket| *ticket != self.ticket);
        self.queued = false;
        drop(state);
        if was_front {
            self.lock.ready.notify_all();
        }
    }

    fn finish_metric(&mut self, outcome: &'static str) {
        if !self.metric_active {
            return;
        }
        metrics::gauge!("sinomo.kv.raft.operation.queue_depth").decrement(1.0);
        metrics::histogram!(
            "sinomo.kv.raft.operation.wait.us",
            "outcome" => outcome
        )
        .record(self.started.elapsed().as_micros() as f64);
        self.metric_active = false;
    }
}

impl Drop for ClusterKvRaftOperationWaiter {
    fn drop(&mut self) {
        // Acquisition is synchronous today, but retaining cleanup in the
        // registration itself keeps the FIFO queue correct if a caller times
        // out, an error unwinds the wait, or a future cancellable wrapper drops
        // the pending registration.
        self.remove_from_queue();
        self.finish_metric("cancelled");
    }
}

/// Owned permit for one local cluster-KV Raft operation.
///
/// Unlike `std::sync::MutexGuard`, this permit is `Send`, so an async quorum
/// command can retain it while its durable node remains open across blocking
/// storage phases and network awaits.
pub struct ClusterKvRaftOperationGuard {
    root: PathBuf,
    lock: Arc<ClusterKvRaftOperationLock>,
    acquired_at: Instant,
}

fn remove_cluster_kv_raft_operation_lock_if_unused(
    root: &Path,
    lock: &Arc<ClusterKvRaftOperationLock>,
) {
    let mut locks = CLUSTER_KV_RAFT_OPERATION_LOCKS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let is_registered_lock = locks
        .get(root)
        .is_some_and(|registered| registered.as_ptr() == Arc::as_ptr(lock));
    if is_registered_lock && Arc::strong_count(lock) == 1 {
        locks.remove(root);
    }
}

impl Drop for ClusterKvRaftOperationGuard {
    fn drop(&mut self) {
        metrics::gauge!("sinomo.kv.raft.operation.inflight").decrement(1.0);
        let held_us = {
            let mut state = self
                .lock
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.held = false;
            // Every waiter re-checks its FIFO ticket. `notify_one` can wake a
            // later ticket and leave the eligible front asleep indefinitely.
            self.lock.ready.notify_all();
            self.acquired_at.elapsed().as_micros() as f64
        };
        metrics::histogram!("sinomo.kv.raft.operation.hold.us").record(held_us);
        remove_cluster_kv_raft_operation_lock_if_unused(&self.root, &self.lock);
    }
}

pub fn lock_cluster_kv_raft_operation_for_root(
    root: impl AsRef<Path>,
) -> Result<ClusterKvRaftOperationGuard, KvError> {
    lock_cluster_kv_raft_operation_for_root_with_wait(root, None)
}

fn lock_cluster_kv_raft_operation_for_root_with_wait(
    root: impl AsRef<Path>,
    wait: Option<Duration>,
) -> Result<ClusterKvRaftOperationGuard, KvError> {
    let root = root.as_ref().to_path_buf();
    let lock = {
        let mut guard = CLUSTER_KV_RAFT_OPERATION_LOCKS
            .lock()
            .map_err(|error| KvError::Io {
                path: "cluster kv raft operation locks".to_string(),
                detail: format!("cluster kv raft operation lock table poisoned: {error}"),
            })?;
        match guard.get(&root).and_then(std::sync::Weak::upgrade) {
            Some(lock) => lock,
            None => {
                let lock = Arc::new(ClusterKvRaftOperationLock {
                    state: Mutex::new(ClusterKvRaftOperationLockState::default()),
                    ready: Condvar::new(),
                });
                guard.insert(root.clone(), Arc::downgrade(&lock));
                lock
            }
        }
    };
    // Production operation admission remains unbounded, matching the existing
    // contract. The optional deadline exists for timeout cleanup tests and for
    // callers that already own an external deadline; it does not change any
    // deployed timeout.
    let deadline = wait.and_then(|wait| Instant::now().checked_add(wait));
    let waiter = ClusterKvRaftOperationWaiter::enqueue(Arc::clone(&lock), &root)?;
    // Declare the state guard after the waiter registration. During unwinding
    // Rust drops it first, so the waiter's cancellation cleanup can safely
    // reacquire the mutex and remove its ticket.
    let mut state = match lock.state.lock() {
        Ok(state) => state,
        Err(error) => {
            let detail = format!("cluster kv raft operation lock poisoned: {error}");
            drop(error);
            waiter.cancel("error");
            remove_cluster_kv_raft_operation_lock_if_unused(&root, &lock);
            return Err(KvError::Io {
                path: root.display().to_string(),
                detail,
            });
        }
    };
    while state.held || state.waiters.front().copied() != Some(waiter.ticket) {
        if let Some(deadline) = deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                drop(state);
                waiter.cancel("timeout");
                remove_cluster_kv_raft_operation_lock_if_unused(&root, &lock);
                return Err(KvError::Busy {
                    path: root.display().to_string(),
                });
            }
            let (next_state, timeout) = match lock.ready.wait_timeout(state, remaining) {
                Ok(waited) => waited,
                Err(error) => {
                    let detail =
                        format!("cluster kv raft operation lock poisoned while waiting: {error}");
                    drop(error);
                    waiter.cancel("error");
                    remove_cluster_kv_raft_operation_lock_if_unused(&root, &lock);
                    return Err(KvError::Io {
                        path: root.display().to_string(),
                        detail,
                    });
                }
            };
            state = next_state;
            if timeout.timed_out()
                && (state.held || state.waiters.front().copied() != Some(waiter.ticket))
            {
                drop(state);
                waiter.cancel("timeout");
                remove_cluster_kv_raft_operation_lock_if_unused(&root, &lock);
                return Err(KvError::Busy {
                    path: root.display().to_string(),
                });
            }
        } else {
            state = match lock.ready.wait(state) {
                Ok(state) => state,
                Err(error) => {
                    let detail =
                        format!("cluster kv raft operation lock poisoned while waiting: {error}");
                    drop(error);
                    waiter.cancel("error");
                    remove_cluster_kv_raft_operation_lock_if_unused(&root, &lock);
                    return Err(KvError::Io {
                        path: root.display().to_string(),
                        detail,
                    });
                }
            };
        }
    }
    let dequeued_ticket = state.waiters.pop_front();
    debug_assert_eq!(dequeued_ticket, Some(waiter.ticket));
    state.held = true;
    let acquired_at = Instant::now();
    drop(state);
    waiter.finish_after_dequeue("acquired");
    metrics::gauge!("sinomo.kv.raft.operation.inflight").increment(1.0);
    Ok(ClusterKvRaftOperationGuard {
        root,
        lock,
        acquired_at,
    })
}

#[cfg(test)]
fn cluster_kv_raft_operation_lock_is_registered_for_test(root: &Path) -> bool {
    CLUSTER_KV_RAFT_OPERATION_LOCKS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .contains_key(root)
}

#[cfg(test)]
mod operation_lock_tests {
    use super::*;
    use std::sync::mpsc;

    fn operation_lock_for_test(root: &Path) -> Arc<ClusterKvRaftOperationLock> {
        CLUSTER_KV_RAFT_OPERATION_LOCKS
            .lock()
            .expect("operation lock registry")
            .get(root)
            .and_then(std::sync::Weak::upgrade)
            .expect("registered operation lock")
    }

    fn wait_for_operation_queue_depth(lock: &ClusterKvRaftOperationLock, expected: usize) {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let depth = lock
                .state
                .lock()
                .expect("operation lock state")
                .waiters
                .len();
            if depth == expected {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "operation queue did not reach depth {expected}; observed {depth}"
            );
            thread::yield_now();
        }
    }

    fn wait_for_operation_ticket_count(lock: &ClusterKvRaftOperationLock, expected: u64) {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let ticket_count = lock.state.lock().expect("operation lock state").next_ticket;
            if ticket_count == expected {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "operation ticket count did not reach {expected}; observed {ticket_count}"
            );
            thread::yield_now();
        }
    }

    #[test]
    fn operation_lock_registry_releases_churned_roots() {
        fn assert_send<T: Send>() {}
        assert_send::<ClusterKvRaftOperationGuard>();

        let base = std::env::temp_dir().join(format!(
            "sinomo-kv-operation-lock-churn-{}",
            std::process::id()
        ));
        for index in 0..512 {
            let root = base.join(index.to_string());
            let guard =
                lock_cluster_kv_raft_operation_for_root(&root).expect("operation lock acquired");
            assert!(cluster_kv_raft_operation_lock_is_registered_for_test(&root));
            drop(guard);
            assert!(
                !cluster_kv_raft_operation_lock_is_registered_for_test(&root),
                "dropped operation lock root {index} remained registered"
            );
        }
    }

    #[test]
    fn operation_lock_registry_preserves_waiter_handoff() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-kv-operation-lock-waiter-{}",
            std::process::id()
        ));
        let first =
            lock_cluster_kv_raft_operation_for_root(&root).expect("first operation lock acquired");
        let lock = operation_lock_for_test(&root);
        let waiter_root = root.clone();
        let (acquired_tx, acquired_rx) = std::sync::mpsc::sync_channel(0);
        let waiter = std::thread::spawn(move || {
            let guard = lock_cluster_kv_raft_operation_for_root(&waiter_root)
                .expect("waiting operation lock acquired");
            acquired_tx.send(()).expect("waiter acquisition reported");
            drop(guard);
        });

        // Observe the second ticket's registration before releasing the first
        // guard so the test deterministically exercises an overlapping handoff.
        wait_for_operation_ticket_count(&lock, 2);
        drop(lock);
        assert!(
            matches!(
                acquired_rx.try_recv(),
                Err(std::sync::mpsc::TryRecvError::Empty)
            ),
            "waiter acquired while the first operation still held the lock"
        );
        drop(first);
        acquired_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("waiter acquired after handoff");
        waiter.join().expect("waiter completed");
        assert!(!cluster_kv_raft_operation_lock_is_registered_for_test(
            &root
        ));
    }

    #[test]
    fn operation_lock_fifo_prevents_late_bargers_from_starving_front_waiter() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-kv-operation-lock-fifo-{}",
            std::process::id()
        ));
        let first =
            lock_cluster_kv_raft_operation_for_root(&root).expect("first operation lock acquired");
        let lock = operation_lock_for_test(&root);
        let (acquired_tx, acquired_rx) = mpsc::channel();
        let mut waiters = Vec::new();

        // Queue the sentinel first, then stage every fresh arrival behind it
        // before releasing the current holder. Channel order is acquisition
        // order because each sender still owns the exclusive guard.
        for waiter_id in 0..9_u64 {
            let waiter_root = root.clone();
            let acquired_tx = acquired_tx.clone();
            waiters.push(thread::spawn(move || {
                let guard = lock_cluster_kv_raft_operation_for_root(&waiter_root)
                    .expect("queued operation lock acquired");
                acquired_tx
                    .send(waiter_id)
                    .expect("operation acquisition reported");
                drop(guard);
            }));
            wait_for_operation_queue_depth(&lock, waiter_id as usize + 1);
        }
        drop(acquired_tx);
        drop(lock);
        drop(first);

        for expected in 0..9_u64 {
            assert_eq!(
                acquired_rx
                    .recv_timeout(Duration::from_secs(1))
                    .expect("next FIFO operation acquisition"),
                expected
            );
        }
        for waiter in waiters {
            waiter.join().expect("join operation waiter");
        }
        assert!(!cluster_kv_raft_operation_lock_is_registered_for_test(
            &root
        ));
    }

    #[test]
    fn timed_out_operation_waiter_removes_front_ticket_and_unblocks_follower() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-kv-operation-lock-timeout-{}",
            std::process::id()
        ));
        let first =
            lock_cluster_kv_raft_operation_for_root(&root).expect("first operation lock acquired");
        let lock = operation_lock_for_test(&root);
        let timeout_root = root.clone();
        let (timeout_tx, timeout_rx) = mpsc::sync_channel(0);
        let timeout_waiter = thread::spawn(move || {
            let result = lock_cluster_kv_raft_operation_for_root_with_wait(
                &timeout_root,
                Some(Duration::from_millis(100)),
            );
            timeout_tx
                .send(matches!(result, Err(KvError::Busy { .. })))
                .expect("timeout reported");
        });
        wait_for_operation_queue_depth(&lock, 1);

        let follower_root = root.clone();
        let (follower_tx, follower_rx) = mpsc::sync_channel(0);
        let follower = thread::spawn(move || {
            let guard = lock_cluster_kv_raft_operation_for_root(&follower_root)
                .expect("follower operation lock acquired");
            follower_tx.send(()).expect("follower acquisition reported");
            drop(guard);
        });
        wait_for_operation_queue_depth(&lock, 2);

        assert!(
            timeout_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("front waiter timed out")
        );
        timeout_waiter.join().expect("join timed waiter");
        wait_for_operation_queue_depth(&lock, 1);
        drop(lock);
        drop(first);
        follower_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("follower acquired after timed ticket removal");
        follower.join().expect("join follower waiter");
        assert!(!cluster_kv_raft_operation_lock_is_registered_for_test(
            &root
        ));
    }

    #[test]
    fn deadline_bound_command_storage_does_not_execute_after_waiter_timeout() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-kv-operation-storage-deadline-{}",
            std::process::id()
        ));
        let paths = ClusterKvPaths::new(root.clone());
        let first =
            lock_cluster_kv_raft_operation_for_root(&root).expect("first operation lock acquired");
        let lock = operation_lock_for_test(&root);
        let node_id = NodeId::from_legacy_named("node_operation_storage_deadline").expect("node");
        let waiter = thread::spawn(move || {
            let mut storage = ClusterKvRaftLocalCommandStorage::new_with_operation_deadline(
                paths.raft_store(),
                node_id,
                |_| false,
                Instant::now() + Duration::from_millis(100),
            );
            storage.read_index_state()
        });
        wait_for_operation_queue_depth(&lock, 1);

        assert!(matches!(
            waiter.join().expect("join deadline-bound storage"),
            Err(KvError::Busy { .. })
        ));
        wait_for_operation_queue_depth(&lock, 0);
        drop(lock);
        drop(first);

        let replacement = lock_cluster_kv_raft_operation_for_root(&root)
            .expect("replacement operation acquires after timed-out storage");
        drop(replacement);
        assert!(!cluster_kv_raft_operation_lock_is_registered_for_test(
            &root
        ));
    }

    #[test]
    fn expired_locked_leader_lookup_does_not_read_hard_state() {
        let root = sinomo_testdir::TestDir::new("sinomo-kv-expired-locked-leader-read");
        let paths = ClusterKvPaths::new(&root);
        let node_id = NodeId::from_legacy_named("node_expired_locked_leader_read").expect("node");
        let state = ClusterBootstrapState::initialized_locally(
            Id::from_legacy_named("cluster_expired_locked_leader_read").expect("cluster"),
            "expired leader read",
            node_id.clone(),
            "node",
            "public-key",
            OffsetDateTime::now_utc(),
        );
        let wal_paths = paths.raft_wal_paths(&node_id);
        reset_persisted_hard_state_actual_reads_for_test(&wal_paths.hard_state);

        assert!(matches!(
            paths
                .raft_store()
                .locked_known_voter_leader_id_before(&state, Instant::now()),
            Err(KvError::Busy { .. })
        ));
        assert_eq!(
            persisted_hard_state_actual_reads_for_test(&wal_paths.hard_state),
            0,
            "expired admission must be checked before the tiny leader lookup"
        );
    }

    #[test]
    fn try_locked_leader_lookup_does_not_queue_or_read_while_operation_is_busy() {
        let root = sinomo_testdir::TestDir::new("sinomo-kv-try-locked-leader-read");
        let paths = ClusterKvPaths::new(&root);
        let store = paths.raft_store();
        let node_id = NodeId::from_legacy_named("node_try_locked_leader_read").expect("node");
        let state = ClusterBootstrapState::initialized_locally(
            Id::from_legacy_named("cluster_try_locked_leader_read").expect("cluster"),
            "try leader read",
            node_id.clone(),
            "node",
            "public-key",
            OffsetDateTime::now_utc(),
        );
        let wal_paths = paths.raft_wal_paths(&node_id);
        reset_persisted_hard_state_actual_reads_for_test(&wal_paths.hard_state);
        let held = store.lock_operation().expect("hold operation lock");
        let lock = operation_lock_for_test(&paths.root);

        let started = Instant::now();
        assert!(matches!(
            store.try_locked_known_voter_leader_id(&state),
            Err(KvError::Busy { .. })
        ));
        assert!(
            started.elapsed() < Duration::from_millis(25),
            "an advisory leader hint must not wait behind the operation owner"
        );
        assert!(
            lock.state
                .lock()
                .expect("operation lock state")
                .waiters
                .is_empty(),
            "a try-only hint must not leave an operation waiter"
        );
        assert_eq!(
            persisted_hard_state_actual_reads_for_test(&wal_paths.hard_state),
            0,
            "a busy try-only lookup must not read hard state"
        );

        drop(lock);
        drop(held);
    }

    #[test]
    fn expired_election_self_vote_cannot_mutate_after_operation_lock_release() {
        let root = sinomo_testdir::TestDir::new("sinomo-kv-expired-election-self-vote");
        let paths = ClusterKvPaths::new(&root);
        let store = paths.raft_store();
        let node_id = NodeId::from_legacy_named("node_expired_election_self_vote").expect("node");
        let before = store
            .open_node(&node_id)
            .expect("seed raft node")
            .hard_state()
            .clone();
        let held = store.lock_operation().expect("hold operation lock");
        let lock = operation_lock_for_test(&paths.root);
        let queued_store = store.clone();
        let queued_node_id = node_id.clone();
        let vote_term = before.current_term.saturating_add(2);
        let waiter = thread::spawn(move || {
            queued_store.locked_apply_vote_before(
                &queued_node_id,
                RaftRequestVoteRequest {
                    term: vote_term,
                    candidate_id: queued_node_id.clone(),
                    last_log_index: 0,
                    last_log_term: 0,
                },
                Instant::now() + Duration::from_millis(50),
            )
        });
        wait_for_operation_queue_depth(&lock, 1);
        thread::sleep(Duration::from_millis(75));
        drop(held);
        assert!(matches!(
            waiter.join().expect("join expired self-vote"),
            Err(KvError::Busy { .. })
        ));
        let after = store
            .open_node(&node_id)
            .expect("inspect raft node")
            .hard_state()
            .clone();
        assert_eq!(
            after, before,
            "expired self-vote mutated durable hard state"
        );
    }

    #[test]
    fn blocking_pool_saturation_cannot_run_an_expired_election_self_vote_late() {
        let root = sinomo_testdir::TestDir::new("sinomo-kv-expired-election-blocking-pool");
        let paths = ClusterKvPaths::new(&root);
        let store = paths.raft_store();
        let node_id =
            NodeId::from_legacy_named("node_expired_election_blocking_pool").expect("node");
        let before = store
            .open_node(&node_id)
            .expect("seed raft node")
            .hard_state()
            .clone();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .expect("runtime");
        let (started_tx, started_rx) = mpsc::sync_channel(0);
        let (release_tx, release_rx) = mpsc::sync_channel(0);
        let blocker = runtime.spawn_blocking(move || {
            started_tx.send(()).expect("report saturated pool");
            release_rx.recv().expect("release blocking pool");
        });
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("blocking worker saturated");
        let queued_store = store.clone();
        let queued_node_id = node_id.clone();
        let deadline = Instant::now() + Duration::from_millis(50);
        let vote_term = before.current_term.saturating_add(2);
        let late_vote = runtime.spawn_blocking(move || {
            queued_store.locked_apply_vote_before(
                &queued_node_id,
                RaftRequestVoteRequest {
                    term: vote_term,
                    candidate_id: queued_node_id.clone(),
                    last_log_index: 0,
                    last_log_term: 0,
                },
                deadline,
            )
        });
        thread::sleep(Duration::from_millis(75));
        release_tx.send(()).expect("release blocking pool");
        runtime.block_on(blocker).expect("join blocker");
        assert!(matches!(
            runtime.block_on(late_vote).expect("join late vote"),
            Err(KvError::Busy { .. })
        ));
        let after = store
            .open_node(&node_id)
            .expect("inspect raft node")
            .hard_state()
            .clone();
        assert_eq!(after, before, "queued expired self-vote mutated hard state");
    }

    #[test]
    fn resident_preflight_rechecks_deadline_before_durable_append() {
        let root = sinomo_testdir::TestDir::new("sinomo-kv-preappend-deadline");
        let paths = ClusterKvPaths::new(&root);
        let node_id = NodeId::from_legacy_named("node_preappend_deadline").expect("node");
        let request = KvCommandRequest::generated(KvCommand::Op(KvOp::Put {
            key: "/deadline/must-not-append".to_string(),
            value: b"late".to_vec(),
            lease_id: None,
        }));
        let mut storage = ClusterKvRaftLocalCommandStorage::new_with_operation_deadline(
            paths.raft_store(),
            node_id.clone(),
            |_| false,
            Instant::now() + Duration::from_secs(60),
        );
        storage
            .read_index_state()
            .expect("open resident node before deadline");
        storage.expire_operation_deadline_before_append_for_test();

        assert!(matches!(
            storage.append_uncommitted(request.clone()),
            Err(KvError::Busy { .. })
        ));
        assert_eq!(
            paths
                .raft_store()
                .open_node(&node_id)
                .expect("reopen durable node after expired session release")
                .last_log_index(),
            0,
            "expired preflight must not append a Raft entry"
        );
        assert!(
            read_cluster_kv_request_attempt(&paths, &request.request_id)
                .expect("read request-attempt ledger")
                .is_none(),
            "expired preflight must not touch the durable request-attempt ledger"
        );

        let rejected = KvCommandRequest::generated(KvCommand::Op(KvOp::Put {
            key: "/deadline/invalid-lease".to_string(),
            value: b"late".to_vec(),
            lease_id: Some(LeaseId(404)),
        }));
        assert!(matches!(
            storage.append_uncommitted(rejected.clone()),
            Err(KvError::Busy { .. })
        ));
        assert!(
            read_cluster_kv_request_attempt(&paths, &rejected.request_id)
                .expect("read rejected request-attempt ledger")
                .is_none(),
            "late preflight rejection must not mutate the durable ledger"
        );
    }

    #[test]
    fn submitted_request_rejected_before_wal_becomes_terminal_and_releases_raft() {
        let root = sinomo_testdir::TestDir::new("sinomo-kv-submitted-pre-wal-rejection");
        let paths = ClusterKvPaths::new(&root);
        let store = paths.raft_store();
        let node_id = NodeId::from_legacy_named("node_submitted_pre_wal_rejection").expect("node");
        store
            .set_leader_compact(&node_id, 7)
            .expect("establish local leader");
        let baseline_log_index = store
            .open_node(&node_id)
            .expect("inspect baseline node")
            .last_log_index();
        let request = KvCommandRequest::generated(KvCommand::Op(KvOp::Put {
            key: "/deadline/submitted-but-not-appended".to_string(),
            value: b"never-appended".to_vec(),
            lease_id: None,
        }));
        let mut storage = ClusterKvRaftLocalCommandStorage::new_with_operation_deadline(
            store.clone(),
            node_id.clone(),
            |_| false,
            Instant::now() + Duration::from_secs(1),
        );
        storage.expire_operation_deadline_after_submission_for_test();
        assert!(matches!(
            storage.append_uncommitted(request.clone()),
            Err(KvError::Busy { .. })
        ));
        assert_eq!(
            store
                .open_node(&node_id)
                .expect("inspect rejected node")
                .last_log_index(),
            baseline_log_index,
            "post-ledger admission failure must not append the Raft WAL"
        );
        assert!(matches!(
            read_cluster_kv_request_attempt(&paths, &request.request_id)
                .expect("read terminal request attempt"),
            Some(KvRequestOutcome::RejectedBeforeCommit { .. })
        ));

        let started = Instant::now();
        let mut read = ClusterKvRaftLocalCommandStorage::new_read_index_with_operation_deadline(
            store,
            node_id,
            |_| false,
            Instant::now() + Duration::from_millis(50),
        );
        assert!(
            read.read_index_entry("/deadline/submitted-but-not-appended")
                .expect("ReadIndex admission after terminal rejection")
                .is_none()
        );
        read.release_session_for_quorum();
        assert!(
            started.elapsed() < Duration::from_millis(50),
            "terminal attempt durability retained Raft ownership"
        );
    }

    #[test]
    fn held_attempt_ledger_does_not_own_raft_or_append_after_mutation_deadline() {
        let root = sinomo_testdir::TestDir::new("sinomo-kv-held-attempt-ledger-deadline");
        let paths = ClusterKvPaths::new(&root);
        let store = paths.raft_store();
        let node_id = NodeId::from_legacy_named("node_held_attempt_ledger_deadline").expect("node");
        store
            .set_leader_compact(&node_id, 5)
            .expect("establish local leader");
        let baseline_log_index = store
            .open_node(&node_id)
            .expect("inspect baseline node")
            .last_log_index();
        let held_ledger = lock_cluster_kv_request_attempt_ledger_for_test(&paths)
            .expect("hold request-attempt ledger");
        let request = KvCommandRequest::generated(KvCommand::Op(KvOp::Put {
            key: "/deadline/held-ledger-must-not-append".to_string(),
            value: b"late".to_vec(),
            lease_id: None,
        }));
        let request_id = request.request_id.clone();
        let writer_store = store.clone();
        let writer_node_id = node_id.clone();
        let (started_tx, started_rx) = mpsc::sync_channel(0);
        let writer = thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_millis(50);
            let mut storage = ClusterKvRaftLocalCommandStorage::new_with_operation_deadline(
                writer_store,
                writer_node_id,
                |_| false,
                deadline,
            );
            started_tx.send(()).expect("report writer start");
            let started = Instant::now();
            (storage.append_uncommitted(request), started.elapsed())
        });
        started_rx.recv().expect("writer started");

        // Give the writer enough time to complete its resident preflight and
        // queue on the independent ledger flock. It must have checked the node
        // back in before that wait, so a bounded point/exact projection can
        // acquire the same operation/resident path immediately.
        thread::sleep(Duration::from_millis(10));
        let read_started = Instant::now();
        let mut read = ClusterKvRaftLocalCommandStorage::new_read_index_with_operation_deadline(
            store.clone(),
            node_id.clone(),
            |_| false,
            Instant::now() + Duration::from_millis(100),
        );
        assert_eq!(
            read.read_index_entry("/deadline/held-ledger-must-not-append")
                .expect("point ReadIndex remains admissible"),
            None
        );
        assert_eq!(
            read.read_index_entries(&[
                "/deadline/held-ledger-must-not-append".to_string(),
                cluster_kv_raft_config_key(),
            ])
            .expect("exact-set ReadIndex remains admissible"),
            vec![None, None]
        );
        read.release_session_for_quorum();
        assert!(
            read_started.elapsed() < Duration::from_millis(100),
            "ledger contention leaked into resident ReadIndex admission"
        );

        let (result, elapsed) = writer.join().expect("join deadline-bound writer");
        assert!(matches!(result, Err(KvError::Busy { .. })));
        assert!(
            elapsed < Duration::from_millis(250),
            "attempt-ledger wait escaped the original 50ms mutation deadline: {elapsed:?}"
        );
        assert_eq!(
            store
                .open_node(&node_id)
                .expect("inspect node before ledger release")
                .last_log_index(),
            baseline_log_index,
            "a timed-out Submitted record must not append the Raft WAL"
        );

        // A deadline-expired flock waiter must be gone, not a detached late
        // mutation that runs when the independent ledger becomes available.
        drop(held_ledger);
        thread::sleep(Duration::from_millis(100));
        assert_eq!(
            store
                .open_node(&node_id)
                .expect("inspect node after ledger release")
                .last_log_index(),
            baseline_log_index,
            "expired attempt-ledger work appended late after lock release"
        );
        assert_eq!(
            read_cluster_kv_request_attempt(&paths, &request_id)
                .expect("inspect request-attempt ledger"),
            None,
            "expired attempt-ledger work mutated the ledger after return"
        );
    }

    #[test]
    fn durable_append_disarms_admission_deadline_for_reconciliation() {
        let root = sinomo_testdir::TestDir::new("sinomo-kv-postappend-deadline");
        let paths = ClusterKvPaths::new(&root);
        let node_id = NodeId::from_legacy_named("node_postappend_deadline").expect("node");
        let store = paths.raft_store();
        store
            .set_leader_compact(&node_id, 7)
            .expect("establish local leader");
        let mut storage = ClusterKvRaftLocalCommandStorage::new_with_operation_deadline(
            store,
            node_id,
            |_| false,
            Instant::now() + Duration::from_secs(60),
        );

        storage
            .append_uncommitted(KvCommandRequest::generated(KvCommand::Op(KvOp::Put {
                key: "/deadline/must-reconcile".to_string(),
                value: b"durable".to_vec(),
                lease_id: None,
            })))
            .expect("append before admission deadline");

        assert!(
            !storage.operation_deadline_is_armed_for_test(),
            "a possibly applied proposal must not retain its admission deadline"
        );
    }

    #[test]
    fn read_index_heartbeat_does_not_clone_large_uncommitted_tail_or_snapshot() {
        const UNCOMMITTED_TAIL_ENTRIES: u64 = 4_096;

        let root = sinomo_testdir::TestDir::new("sinomo-kv-read-index-empty-heartbeat");
        let paths = ClusterKvPaths::new(&root);
        let node_id =
            NodeId::from_legacy_named("node_read_index_empty_heartbeat").expect("node id");
        let cluster_id =
            Id::from_legacy_named("cluster_read_index_empty_heartbeat").expect("cluster id");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "read-index-empty-heartbeat",
            node_id.clone(),
            "node",
            "public-key",
            OffsetDateTime::UNIX_EPOCH,
        );
        state.term = 7;
        let store = paths.raft_store();
        let mut node = store.open_node(&node_id).expect("open Raft node");
        node.become_leader(state.term).expect("establish leader");
        let entries = (1..=UNCOMMITTED_TAIL_ENTRIES.saturating_add(1))
            .map(|index| KvLogEntry {
                index,
                term: state.term,
                request_id: KvRequestId::generate(),
                command: KvCommand::Barrier,
                revision: KvRevision::ZERO,
            })
            .collect::<Vec<_>>();
        let response = node
            .append_entries(RaftAppendEntriesRequest {
                term: state.term,
                leader_id: node_id.clone(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries,
                leader_commit: 1,
            })
            .expect("seed committed anchor and uncommitted tail");
        assert!(response.success);
        assert_eq!(node.hard_state().commit_index, 1);
        assert_eq!(
            node.last_log_index()
                .saturating_sub(node.hard_state().commit_index),
            UNCOMMITTED_TAIL_ENTRIES
        );
        drop(node);

        let mut storage = ClusterKvRaftLocalCommandStorage::new_read_index_with_operation_deadline(
            store,
            node_id.clone(),
            |_| false,
            Instant::now() + Duration::from_secs(30),
        );
        let read_state = storage
            .read_index_state()
            .expect("resident ReadIndex state");
        assert_eq!(read_state.commit_index, 1);
        reset_raft_append_build_work_for_test();

        let heartbeat = storage
            .build_cluster_raft_read_index_heartbeat(&state, read_state.commit_index)
            .expect("build direct empty heartbeat");

        assert!(heartbeat.read_index_probe);
        assert!(heartbeat.snapshot.is_none());
        assert!(heartbeat.append.entries.is_empty());
        assert_eq!(heartbeat.append.prev_log_index, read_state.commit_index);
        assert_eq!(
            heartbeat.append.prev_log_term,
            read_state.commit_term.unwrap()
        );
        assert_eq!(heartbeat.append.leader_commit, read_state.commit_index);
        assert_eq!(
            raft_append_build_work_snapshot_for_test(),
            RaftAppendBuildWorkForTest::default(),
            "the direct heartbeat must not clone any tail entry or materialize a snapshot"
        );
    }

    #[test]
    fn dropped_operation_waiter_registration_removes_its_ticket() {
        let root = std::env::temp_dir().join(format!(
            "sinomo-kv-operation-lock-cancel-{}",
            std::process::id()
        ));
        let first =
            lock_cluster_kv_raft_operation_for_root(&root).expect("first operation lock acquired");
        let lock = operation_lock_for_test(&root);
        let waiter = ClusterKvRaftOperationWaiter::enqueue(Arc::clone(&lock), &root)
            .expect("waiter registered");
        wait_for_operation_queue_depth(&lock, 1);
        drop(waiter);
        wait_for_operation_queue_depth(&lock, 0);
        drop(lock);
        drop(first);
        assert!(!cluster_kv_raft_operation_lock_is_registered_for_test(
            &root
        ));
    }
}

pub fn lock_cluster_kv_raft_operation_for_paths(
    paths: &ClusterKvPaths,
) -> Result<ClusterKvRaftOperationGuard, KvError> {
    lock_cluster_kv_raft_operation_for_root(&paths.root)
}

pub fn lock_cluster_kv_raft_operation_for_wal_paths(
    paths: &KvWalPaths,
) -> Result<ClusterKvRaftOperationGuard, KvError> {
    lock_cluster_kv_raft_operation_for_root(&paths.root)
}

#[cfg(any(test, feature = "test-support"))]
pub fn cluster_kv_raft_operation_queue_depth_for_test(paths: &ClusterKvPaths) -> usize {
    CLUSTER_KV_RAFT_OPERATION_LOCKS
        .lock()
        .ok()
        .and_then(|locks| locks.get(&paths.root).and_then(std::sync::Weak::upgrade))
        .and_then(|lock| lock.state.lock().ok().map(|state| state.waiters.len()))
        .unwrap_or_default()
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KvLogEntry {
    pub index: u64,
    pub term: u64,
    pub request_id: KvRequestId,
    pub command: KvCommand,
    pub revision: KvRevision,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RaftHardState {
    pub current_term: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub voted_for: Option<NodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leader_id: Option<NodeId>,
    pub commit_index: u64,
}

impl Default for RaftHardState {
    fn default() -> Self {
        Self {
            current_term: 1,
            voted_for: None,
            leader_id: None,
            commit_index: 0,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RaftSnapshot {
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub kv: Arc<KvSnapshot>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RaftAppendEntriesRequest {
    pub term: u64,
    pub leader_id: NodeId,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_fallible_vec"
    )]
    pub entries: Vec<KvLogEntry>,
    pub leader_commit: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RaftAppendEntriesResponse {
    pub term: u64,
    pub success: bool,
    pub match_index: u64,
}

/// Exact bounded resident metadata captured with a read-only ReadIndex probe.
///
/// The committed configuration is returned from the same resident-node view
/// which evaluated the log anchor. Cluster code must use this entry, rather
/// than a separately cached topology projection, when authorizing the leader
/// whose heartbeat it is about to acknowledge.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftReadIndexProbeMetadata {
    pub revision: KvRevision,
    pub observed_term: u64,
    pub applied_index: u64,
    pub known_leader: Option<NodeId>,
    pub raft_config_entry: Option<KvEntry>,
}

/// Non-blocking result of evaluating a marked ReadIndex heartbeat against the
/// process-resident Raft owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClusterKvRaftReadIndexProbeOutcome {
    /// The idle resident node evaluated the exact term/log/commit anchor.
    Responded {
        response: RaftAppendEntriesResponse,
        metadata: ClusterKvRaftReadIndexProbeMetadata,
    },
    /// A mutable owner (or cold-loader handoff) currently owns the resident
    /// slot. The caller must return a non-progress response instead of joining
    /// the durable operation queue when the retained term covers the request.
    Busy {
        metadata: ClusterKvRaftReadIndexProbeMetadata,
    },
    /// No exact resident classification is available, or the request carries a
    /// genuinely newer term/commit which must pass through normal durable Raft.
    NeedsDurable,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RaftRequestVoteRequest {
    pub term: u64,
    pub candidate_id: NodeId,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RaftRequestVoteResponse {
    pub term: u64,
    pub vote_granted: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClusterKvRaftUncommittedAppend {
    Appended(KvLogEntry),
    CommittedReplay {
        snapshot: KvSnapshot,
        record: KvCommittedRequestRecord,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftCommitResult {
    /// Explicit owned materialization for compatibility callers that need a
    /// complete post-commit view. This is `O(total retained state)`; daemon hot
    /// paths should use [`ClusterKvRaftCompactCommitResult`] and resident
    /// point/prefix reads, reserving this representation for checkpoints and
    /// exports.
    pub snapshot: Arc<KvSnapshot>,
    pub txn_response: Option<KvTxnResponse>,
    pub events: Arc<Vec<KvEvent>>,
    pub committed_request: KvCommittedRequestRecord,
    pub observation_receipt: Option<KvCommittedObservationReceipt>,
}

/// Incremental result of applying one committed Raft entry to the resident KV
/// state. Its cost is proportional to the command's emitted event set, not the
/// complete retained keyspace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftCompactCommitResult {
    pub txn_response: Option<KvTxnResponse>,
    pub events: Arc<Vec<KvEvent>>,
    pub committed_request: KvCommittedRequestRecord,
    pub read_state: ClusterKvRaftReadIndexState,
    pub compacted_revision: KvRevision,
    /// Present when this operation built a periodic Raft checkpoint or changed
    /// a critical/unknown keyspace whose shared main/shadow projection cannot
    /// safely lag. Deferrable operational and telemetry commits keep this
    /// `None` and remain proportional to their command delta.
    pub checkpoint_snapshot: Option<Arc<KvSnapshot>>,
    pub final_key_states: Arc<Vec<ClusterKvCommittedKeyState>>,
    /// Process-local delivery ownership; never serialized or used as authority.
    pub observation_receipt: Option<KvCommittedObservationReceipt>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftAppendRequest {
    pub snapshot: Option<RaftSnapshot>,
    pub append: RaftAppendEntriesRequest,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftLocalVoteMetadata {
    pub current_term: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

/// Exact local durable generation of an advisory voter-leader hint.
///
/// Callers that invalidate a rejected hint must compare both fields under the
/// Raft operation lock. The node id alone is not an adequate fence because the
/// same voter can become leader again in a later term while an older RPC
/// rejection is still in flight.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftLeaderFence {
    pub leader_id: NodeId,
    pub current_term: u64,
}

/// Result of validating a RequestVote candidate against the committed Raft
/// configuration captured from the same resident node that will apply it.
///
/// The cluster bridge owns identity and membership policy, while KV owns the
/// operation/resident and pending-higher-term fences. Keeping the disposition
/// generic preserves that boundary without splitting validation and durable
/// vote application into independently admitted storage operations.
pub enum ClusterKvRaftVoteDisposition<T> {
    Apply(T),
    Reject(T),
}

/// Coherent metadata for an etcd-style Raft ReadIndex operation.
///
/// This view is captured from the same resident node that serves the eventual
/// point, prefix, or snapshot read. Holding one local command-storage session
/// keeps the node and operation permit stable while the caller confirms the
/// leader's authority with a quorum.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftReadIndexState {
    pub current_term: u64,
    pub leader_id: Option<NodeId>,
    pub commit_index: u64,
    pub commit_term: Option<u64>,
    pub applied_index: u64,
    pub revision: KvRevision,
    pub has_current_term_commit: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterKvRaftConfigRecord {
    pub cluster_id: Id,
    pub term: u64,
    pub kv_revision: u64,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_fallible_vec"
    )]
    pub voters: Vec<NodeId>,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_fallible_vec"
    )]
    pub learners: Vec<NodeId>,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_fallible_vec"
    )]
    pub non_voting: Vec<NodeId>,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Default)]
struct RaftNodeIdSet {
    node_ids: BTreeSet<NodeId>,
}

impl RaftNodeIdSet {
    fn from_iter(
        node_ids: impl IntoIterator<Item = NodeId>,
        operation: &str,
    ) -> Result<Self, KvError> {
        let mut set = Self::default();
        for node_id in node_ids {
            set.try_insert_owned(node_id, operation)?;
        }
        Ok(set)
    }

    fn len(&self) -> usize {
        self.node_ids.len()
    }

    fn contains(&self, node_id: &NodeId) -> bool {
        self.node_ids.contains(node_id)
    }

    fn try_insert_owned(&mut self, node_id: NodeId, operation: &str) -> Result<bool, KvError> {
        let _ = operation;
        Ok(self.node_ids.insert(node_id))
    }

    fn try_insert_clone(&mut self, node_id: &NodeId, operation: &str) -> Result<bool, KvError> {
        Ok(self
            .node_ids
            .insert(try_clone_kv_node_id(node_id, operation)?))
    }
}

fn cluster_kv_authority_failure(
    state: &ClusterBootstrapState,
    reason: KvAuthorityFailureReason,
    detail: String,
    observed_term: u64,
    revision: KvRevision,
    reachable_voters: usize,
    known_leader: Option<NodeId>,
) -> KvAuthorityFailure {
    // The caller's projected bootstrap state carries the durable Raft/read
    // view floor. Some generic failure branches have no more-specific
    // observation, but must not erase that measured floor with placeholder
    // zeroes in operator diagnostics.
    let observed_term = observed_term.max(state.term);
    let revision = KvRevision(revision.0.max(state.kv_revision));
    let local_role = state
        .members
        .iter()
        .find(|member| member.node_id == state.local_node_id)
        .map(|member| member.role.clone())
        .unwrap_or(ClusterMemberRole::NonVoting);
    let suggested_recovery = match reason {
        KvAuthorityFailureReason::HigherTermObserved | KvAuthorityFailureReason::VoterRejected => {
            KvRecoveryAction::RefreshClusterState
        }
        KvAuthorityFailureReason::TransportFailure => KvRecoveryAction::ContactKnownLeader,
        KvAuthorityFailureReason::LocalNodeNotVoter
        | KvAuthorityFailureReason::NoQuorum
        | KvAuthorityFailureReason::Timeout => KvRecoveryAction::RetryWhenQuorumIsReachable,
    };
    KvAuthorityFailure {
        reason,
        detail,
        diagnostics: KvAuthorityDiagnostics {
            local_node_id: state.local_node_id.clone(),
            local_role,
            known_leader,
            observed_term,
            reachable_voters,
            required_quorum: cluster_kv_raft_quorum_size_for_state(state),
            last_successful_append_at: None,
            last_snapshot_refresh_at: None,
            authority: KvReadAuthority::Local,
            revision,
            freshness: KvFreshness::Unknown,
            suggested_recovery,
        },
    }
}

fn node_id_slice_contains(node_ids: &[NodeId], node_id: &NodeId) -> bool {
    node_ids.iter().any(|candidate| candidate == node_id)
}

fn unique_node_id_count(node_ids: &[NodeId]) -> usize {
    let mut count = 0;
    for (index, node_id) in node_ids.iter().enumerate() {
        if node_id_slice_contains(&node_ids[..index], node_id) {
            continue;
        }
        count += 1;
    }
    count
}

fn unique_node_id_overlap_count(left: &[NodeId], right: &[NodeId]) -> usize {
    let mut count = 0;
    for (index, node_id) in left.iter().enumerate() {
        if node_id_slice_contains(&left[..index], node_id) {
            continue;
        }
        if node_id_slice_contains(right, node_id) {
            count += 1;
        }
    }
    count
}

fn try_push_unique_node_id(
    node_ids: &mut Vec<NodeId>,
    node_id: &NodeId,
    operation: &str,
) -> Result<(), KvError> {
    if node_id_slice_contains(node_ids, node_id) {
        return Ok(());
    }
    try_reserve_kv_items(node_ids, 1, operation)?;
    node_ids.push(try_clone_kv_node_id(node_id, operation)?);
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftLeaderElectionOptions {
    pub max_attempts: usize,
}

pub trait ClusterKvRaftLeaderElectionHost {
    type Error;

    /// Wraps a KV store error in the host's error type. Replaces the old
    /// `From<KvError>` bound so host error types in other crates do not need
    /// orphan-rule-breaking `From` impls.
    fn kv_error(error: KvError) -> Self::Error;

    /// Begins an attempt, or returns an election outcome already made durable
    /// by another campaign while this caller was waiting for admission.
    fn begin_election_attempt(&mut self) -> Result<Option<bool>, Self::Error> {
        Ok(None)
    }

    fn local_vote_metadata(&mut self) -> Result<ClusterKvRaftLocalVoteMetadata, Self::Error>;

    fn apply_local_vote(
        &mut self,
        vote: RaftRequestVoteRequest,
    ) -> Result<RaftRequestVoteResponse, Self::Error>;

    fn request_remote_votes(
        &mut self,
        voter_ids: Vec<NodeId>,
        vote: &RaftRequestVoteRequest,
        required_grants: usize,
    ) -> Result<Vec<(NodeId, Result<RaftRequestVoteResponse, Self::Error>)>, Self::Error>;

    fn observe_raft_term(&mut self, term: u64) -> Result<(), Self::Error>;

    fn set_raft_leader(&mut self, term: u64) -> Result<(), Self::Error>;
}

pub fn cluster_kv_raft_quorum_size(voter_count: usize) -> usize {
    voter_count / 2 + 1
}

pub fn cluster_kv_member_has_raft_vote(member: &ClusterMemberRecord) -> bool {
    member.has_raft_vote()
}

pub fn cluster_kv_raft_node_is_voter(state: &ClusterBootstrapState, node_id: &NodeId) -> bool {
    state.node_is_raft_voter(node_id)
}

pub fn cluster_kv_raft_candidate_is_authorized_voter(
    state: &ClusterBootstrapState,
    candidate_id: &NodeId,
) -> bool {
    cluster_kv_raft_node_is_voter(state, candidate_id)
}

pub fn cluster_kv_raft_local_is_voter(state: &ClusterBootstrapState) -> bool {
    cluster_kv_raft_node_is_voter(state, &state.local_node_id)
}

pub fn cluster_kv_raft_voter_ids(state: &ClusterBootstrapState) -> Result<Vec<NodeId>, KvError> {
    let voter_count = state
        .members
        .iter()
        .filter(|member| cluster_kv_raft_node_is_voter(state, &member.node_id))
        .count();
    let mut voters = Vec::new();
    try_reserve_kv_items(&mut voters, voter_count, "collect cluster raft voters")?;
    for member in state
        .members
        .iter()
        .filter(|member| cluster_kv_raft_node_is_voter(state, &member.node_id))
    {
        voters.push(try_clone_kv_node_id(
            &member.node_id,
            "collect cluster raft voters",
        )?);
    }
    voters.sort_unstable();
    voters.dedup();
    Ok(voters)
}

pub fn cluster_kv_raft_quorum_size_for_state(state: &ClusterBootstrapState) -> usize {
    let voter_count = state
        .members
        .iter()
        .filter(|member| cluster_kv_raft_node_is_voter(state, &member.node_id))
        .count();
    cluster_kv_raft_quorum_size(voter_count)
}

pub fn cluster_kv_raft_config_key() -> String {
    "/cluster/raft/config".to_string()
}

pub(crate) fn validate_cluster_kv_raft_config_is_unleased(
    key: &str,
    lease_id: Option<LeaseId>,
) -> Result<(), KvError> {
    if key == cluster_kv_raft_config_key()
        && let Some(lease_id) = lease_id
    {
        return Err(KvError::InvalidRaftConfigTransition {
            detail: format!(
                "reserved cluster membership key cannot be attached to lease {}",
                lease_id.0
            ),
        });
    }
    Ok(())
}

fn kv_op_may_mutate_exact_key(op: &KvOp, key: &str) -> bool {
    match op {
        KvOp::Put { key: op_key, .. } | KvOp::Delete { key: op_key } => op_key == key,
        KvOp::DeletePrefix { prefix } => key.starts_with(prefix),
        KvOp::LeaseGrant { .. } | KvOp::LeaseKeepAlive { .. } | KvOp::LeaseRevoke { .. } => false,
    }
}

/// Conservative equivalent of etcd's `pendingConfIndex` classification. Both
/// transaction branches count because the selected branch is decided only when
/// the inherited prefix is applied.
pub(crate) fn command_may_mutate_cluster_raft_config(command: &KvCommand) -> bool {
    let key = cluster_kv_raft_config_key();
    match command {
        KvCommand::Barrier => false,
        KvCommand::Op(op) => kv_op_may_mutate_exact_key(op, &key),
        KvCommand::Txn(txn) => txn
            .success
            .iter()
            .chain(&txn.failure)
            .any(|op| kv_op_may_mutate_exact_key(op, &key)),
    }
}

pub(crate) const MAX_CLUSTER_KV_UNCOMMITTED_ENTRIES: usize =
    RAFT_COMPACTION_RETAINED_ENTRIES as usize;
const MAX_CLUSTER_KV_UNCOMMITTED_PAYLOAD_BYTES: usize = MAX_COMMITTED_KV_REQUEST_BYTES;

fn command_payload_bytes(command: &KvCommand) -> usize {
    match command {
        KvCommand::Barrier => 0,
        KvCommand::Op(op) => kv_op_payload_bytes(op),
        KvCommand::Txn(txn) => txn
            .compares
            .iter()
            .map(kv_compare_payload_bytes)
            .chain(
                txn.success
                    .iter()
                    .chain(&txn.failure)
                    .map(kv_op_payload_bytes),
            )
            .fold(0_usize, usize::saturating_add),
    }
}

pub fn cluster_kv_raft_config_record_stable_eq(
    left: &ClusterKvRaftConfigRecord,
    right: &ClusterKvRaftConfigRecord,
) -> bool {
    left.cluster_id == right.cluster_id
        && left.term == right.term
        && left.voters == right.voters
        && left.learners == right.learners
        && left.non_voting == right.non_voting
}

pub fn cluster_kv_raft_config_member_role(
    config: &ClusterKvRaftConfigRecord,
    node_id: &NodeId,
) -> Option<ClusterMemberRole> {
    if config.voters.iter().any(|candidate| candidate == node_id) {
        Some(ClusterMemberRole::Voter)
    } else if config.learners.iter().any(|candidate| candidate == node_id) {
        Some(ClusterMemberRole::Learner)
    } else if config
        .non_voting
        .iter()
        .any(|candidate| candidate == node_id)
    {
        Some(ClusterMemberRole::NonVoting)
    } else {
        None
    }
}

pub fn cluster_kv_store_member_is_raft_voter(
    store: &InProcessKvStore,
    node_id: &NodeId,
) -> Result<bool, KvError> {
    let Some(config_entry) = store.get(&cluster_kv_raft_config_key()) else {
        return Ok(false);
    };
    let config: ClusterKvRaftConfigRecord = config_entry.decode()?;
    Ok(config.voters.iter().any(|voter| voter == node_id))
}

pub fn validate_cluster_kv_raft_config_transition(
    current: &ClusterKvRaftConfigRecord,
    next: &ClusterKvRaftConfigRecord,
) -> Result<(), KvError> {
    if current.cluster_id != next.cluster_id {
        return Err(KvError::InvalidRaftConfigTransition {
            detail: format!(
                "cluster raft config cluster id changed from `{}` to `{}`",
                current.cluster_id, next.cluster_id
            ),
        });
    }
    let mut removed_voters = Vec::new();
    for voter in current
        .voters
        .iter()
        .filter(|voter| !node_id_slice_contains(&next.voters, voter))
    {
        try_push_unique_node_id(&mut removed_voters, voter, "validate raft removed voters")?;
    }
    if !removed_voters.is_empty() {
        let retained_voters = unique_node_id_overlap_count(&current.voters, &next.voters);
        let current_voter_count = unique_node_id_count(&current.voters);
        let next_voter_count = unique_node_id_count(&next.voters);
        let current_quorum = cluster_kv_raft_quorum_size(current_voter_count);
        let next_quorum = cluster_kv_raft_quorum_size(next_voter_count);
        let voter_union = current_voter_count
            .saturating_add(next_voter_count)
            .saturating_sub(retained_voters);
        // Every old quorum must intersect every new quorum. Retaining enough
        // individual voters to form both quorums is not sufficient: for example,
        // shrinking five voters to three retains three voters but still permits
        // disjoint old/new quorums of sizes three and two.
        let every_quorum_overlaps = current_quorum.saturating_add(next_quorum) > voter_union;
        if retained_voters < current_quorum
            || retained_voters < next_quorum
            || !every_quorum_overlaps
        {
            return Err(KvError::InvalidRaftConfigTransition {
                detail: format!(
                    "raft voter removal must retain both quorum thresholds and guarantee every old/new quorum overlaps; refusing to remove {} (retained {retained_voters}, old quorum {current_quorum}, new quorum {next_quorum}, voter union {voter_union})",
                    removed_voters
                        .iter()
                        .map(NodeId::as_str)
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            });
        }
    }
    let mut added_voters = Vec::new();
    for voter in next
        .voters
        .iter()
        .filter(|voter| !node_id_slice_contains(&current.voters, voter))
    {
        try_push_unique_node_id(&mut added_voters, voter, "validate raft added voters")?;
    }
    if added_voters.len() > 1 {
        return Err(KvError::InvalidRaftConfigTransition {
            detail: format!(
                "raft voter expansion is limited to one voter per committed config change; requested {} additions",
                added_voters.len()
            ),
        });
    }
    if let Some(added_voter) = added_voters.first()
        && !current
            .learners
            .iter()
            .chain(current.non_voting.iter())
            .any(|node_id| node_id == added_voter)
    {
        return Err(KvError::InvalidRaftConfigTransition {
            detail: format!(
                "raft voter `{added_voter}` must already be a committed learner or non-voting member before promotion"
            ),
        });
    }
    Ok(())
}

pub fn elect_cluster_kv_raft_leader_with_host<H>(
    local_node_id: &NodeId,
    voter_ids: impl IntoIterator<Item = NodeId>,
    remote_vote_targets: impl IntoIterator<Item = NodeId>,
    options: ClusterKvRaftLeaderElectionOptions,
    host: &mut H,
) -> Result<bool, H::Error>
where
    H: ClusterKvRaftLeaderElectionHost,
{
    let voter_ids = RaftNodeIdSet::from_iter(voter_ids, "collect raft election voters")
        .map_err(<H as ClusterKvRaftLeaderElectionHost>::kv_error)?;
    if !voter_ids.contains(local_node_id) {
        return Ok(false);
    }
    let quorum_size = cluster_kv_raft_quorum_size(voter_ids.len());
    let mut remote_vote_targets_vec = Vec::new();
    for voter_id in remote_vote_targets {
        if voter_id != *local_node_id && voter_ids.contains(&voter_id) {
            try_reserve_kv_items(&mut remote_vote_targets_vec, 1, "collect raft vote targets")
                .map_err(<H as ClusterKvRaftLeaderElectionHost>::kv_error)?;
            remote_vote_targets_vec.push(voter_id);
        }
    }
    let remote_vote_targets = remote_vote_targets_vec;

    for _ in 0..options.max_attempts {
        if let Some(outcome) = host.begin_election_attempt()? {
            return Ok(outcome);
        }
        let metadata = host.local_vote_metadata()?;
        let election_term = metadata
            .current_term
            .saturating_add(CLUSTER_KV_RAFT_LEADER_ELECTION_TERM_BUMP);
        let vote_request = RaftRequestVoteRequest {
            term: election_term,
            candidate_id: try_clone_kv_node_id(local_node_id, "build raft election vote request")
                .map_err(<H as ClusterKvRaftLeaderElectionHost>::kv_error)?,
            last_log_index: metadata.last_log_index,
            last_log_term: metadata.last_log_term,
        };
        let local_vote = host.apply_local_vote(RaftRequestVoteRequest {
            term: vote_request.term,
            candidate_id: try_clone_kv_node_id(&vote_request.candidate_id, "apply local raft vote")
                .map_err(<H as ClusterKvRaftLeaderElectionHost>::kv_error)?,
            last_log_index: vote_request.last_log_index,
            last_log_term: vote_request.last_log_term,
        })?;
        let mut highest_observed_term = local_vote.term.max(election_term);
        let mut same_term_rejection = local_vote.term == election_term && !local_vote.vote_granted;
        let mut granted = RaftNodeIdSet::default();
        if local_vote.vote_granted {
            granted
                .try_insert_clone(local_node_id, "record local raft vote")
                .map_err(<H as ClusterKvRaftLeaderElectionHost>::kv_error)?;
        }

        let required_remote_grants = quorum_size.saturating_sub(granted.len());
        let remote_votes = if required_remote_grants == 0 {
            Vec::new()
        } else {
            host.request_remote_votes(
                remote_vote_targets.clone(),
                &vote_request,
                required_remote_grants,
            )?
        };
        for (voter_id, response) in remote_votes {
            let Ok(response) = response else {
                continue;
            };
            highest_observed_term = highest_observed_term.max(response.term);
            same_term_rejection |= response.term == election_term
                && !response.vote_granted
                && voter_ids.contains(&voter_id);
            if response.vote_granted && voter_ids.contains(&voter_id) {
                granted
                    .try_insert_clone(&voter_id, "record remote raft vote")
                    .map_err(<H as ClusterKvRaftLeaderElectionHost>::kv_error)?;
            }
        }

        if highest_observed_term > election_term {
            host.observe_raft_term(highest_observed_term)?;
            continue;
        }
        if granted.len() < quorum_size {
            // A same-term rejection is evidence of a contested election (most
            // commonly two candidates splitting the vote), so advance to the
            // next bounded attempt. Transport failures and partial grants
            // without a rejection are only evidence that quorum is currently
            // unavailable; retrying those immediately would add durable term
            // bumps without any new convergence signal.
            if same_term_rejection {
                continue;
            }
            return Ok(false);
        }
        host.set_raft_leader(election_term)?;
        return Ok(true);
    }
    Ok(false)
}

pub trait ClusterKvRaftLeaderElectionAsyncHost {
    type Error;

    /// Wraps a KV store error in the host's error type. Replaces the old
    /// `From<KvError>` bound so host error types in other crates do not need
    /// orphan-rule-breaking `From` impls.
    fn kv_error(error: KvError) -> Self::Error;

    fn begin_election_attempt_async<'a>(
        &'a mut self,
    ) -> ClusterKvRaftQuorumFuture<'a, Option<bool>, Self::Error>;

    fn local_vote_metadata_async<'a>(
        &'a mut self,
    ) -> ClusterKvRaftQuorumFuture<'a, ClusterKvRaftLocalVoteMetadata, Self::Error>;

    fn apply_local_vote_async<'a>(
        &'a mut self,
        vote: RaftRequestVoteRequest,
    ) -> ClusterKvRaftQuorumFuture<'a, RaftRequestVoteResponse, Self::Error>;

    fn request_remote_votes_async<'a>(
        &'a mut self,
        voter_ids: Vec<NodeId>,
        vote: &'a RaftRequestVoteRequest,
        required_grants: usize,
    ) -> ClusterKvRaftQuorumFuture<
        'a,
        Vec<(NodeId, Result<RaftRequestVoteResponse, Self::Error>)>,
        Self::Error,
    >;

    fn observe_raft_term_async<'a>(
        &'a mut self,
        term: u64,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error>;

    fn set_raft_leader_async<'a>(
        &'a mut self,
        term: u64,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error>;
}

pub async fn elect_cluster_kv_raft_leader_async_with_host<H>(
    local_node_id: &NodeId,
    voter_ids: impl IntoIterator<Item = NodeId>,
    remote_vote_targets: impl IntoIterator<Item = NodeId>,
    options: ClusterKvRaftLeaderElectionOptions,
    host: &mut H,
) -> Result<bool, H::Error>
where
    H: ClusterKvRaftLeaderElectionAsyncHost,
{
    let voter_ids = RaftNodeIdSet::from_iter(voter_ids, "collect async raft election voters")
        .map_err(<H as ClusterKvRaftLeaderElectionAsyncHost>::kv_error)?;
    if !voter_ids.contains(local_node_id) {
        return Ok(false);
    }
    let quorum_size = cluster_kv_raft_quorum_size(voter_ids.len());
    let mut remote_vote_targets_vec = Vec::new();
    for voter_id in remote_vote_targets {
        if voter_id != *local_node_id && voter_ids.contains(&voter_id) {
            try_reserve_kv_items(
                &mut remote_vote_targets_vec,
                1,
                "collect async raft vote targets",
            )
            .map_err(<H as ClusterKvRaftLeaderElectionAsyncHost>::kv_error)?;
            remote_vote_targets_vec.push(voter_id);
        }
    }
    let remote_vote_targets = remote_vote_targets_vec;

    for _ in 0..options.max_attempts {
        if let Some(outcome) = host.begin_election_attempt_async().await? {
            return Ok(outcome);
        }
        let metadata = host.local_vote_metadata_async().await?;
        let election_term = metadata
            .current_term
            .saturating_add(CLUSTER_KV_RAFT_LEADER_ELECTION_TERM_BUMP);
        let vote_request = RaftRequestVoteRequest {
            term: election_term,
            candidate_id: try_clone_kv_node_id(
                local_node_id,
                "build async raft election vote request",
            )
            .map_err(<H as ClusterKvRaftLeaderElectionAsyncHost>::kv_error)?,
            last_log_index: metadata.last_log_index,
            last_log_term: metadata.last_log_term,
        };
        let local_vote = host
            .apply_local_vote_async(RaftRequestVoteRequest {
                term: vote_request.term,
                candidate_id: try_clone_kv_node_id(
                    &vote_request.candidate_id,
                    "apply local async raft vote",
                )
                .map_err(<H as ClusterKvRaftLeaderElectionAsyncHost>::kv_error)?,
                last_log_index: vote_request.last_log_index,
                last_log_term: vote_request.last_log_term,
            })
            .await?;
        let mut highest_observed_term = local_vote.term.max(election_term);
        let mut same_term_rejection = local_vote.term == election_term && !local_vote.vote_granted;
        let mut granted = RaftNodeIdSet::default();
        if local_vote.vote_granted {
            granted
                .try_insert_clone(local_node_id, "record local async raft vote")
                .map_err(<H as ClusterKvRaftLeaderElectionAsyncHost>::kv_error)?;
        }

        let required_remote_grants = quorum_size.saturating_sub(granted.len());
        let remote_votes = if required_remote_grants == 0 {
            Vec::new()
        } else {
            host.request_remote_votes_async(
                remote_vote_targets.clone(),
                &vote_request,
                required_remote_grants,
            )
            .await?
        };
        for (voter_id, response) in remote_votes {
            let Ok(response) = response else {
                continue;
            };
            highest_observed_term = highest_observed_term.max(response.term);
            same_term_rejection |= response.term == election_term
                && !response.vote_granted
                && voter_ids.contains(&voter_id);
            if response.vote_granted && voter_ids.contains(&voter_id) {
                granted
                    .try_insert_clone(&voter_id, "record remote async raft vote")
                    .map_err(<H as ClusterKvRaftLeaderElectionAsyncHost>::kv_error)?;
            }
        }

        if highest_observed_term > election_term {
            host.observe_raft_term_async(highest_observed_term).await?;
            continue;
        }
        if granted.len() < quorum_size {
            // Match the synchronous driver: only a contested same-term vote
            // justifies another immediate durable election generation.
            if same_term_rejection {
                continue;
            }
            return Ok(false);
        }
        host.set_raft_leader_async(election_term).await?;
        return Ok(true);
    }
    Ok(false)
}

/// Storage-side contract for applying a cluster KV command to the local Raft log.
///
/// Implementors own KV/Raft durability only. They must not choose peers, route
/// messages, score routes, or interpret the cluster connectivity graph.
/// Proposal ownership and its process-local observation release fence.
///
/// The file is always dropped before the fence, including cancellation and
/// unwinding. Observer callbacks may start only after storage ownership also
/// releases its separately captured fence.
#[derive(Debug)]
pub struct ClusterKvRaftCommitSequenceGuard {
    ownership: Option<Arc<ClusterKvRaftCommitSequenceOwnership>>,
}

/// One actual proposal file and release fence. An already-admitted canonical
/// storage phase retains this owner when its cancellable caller disappears.
#[derive(Debug)]
pub(crate) struct ClusterKvRaftCommitSequenceOwnership {
    file: Option<File>,
    observation_fence: Option<KvObservationOwnerFence>,
}

impl ClusterKvRaftCommitSequenceGuard {
    pub(crate) fn from_retained_ownership(
        ownership: Arc<ClusterKvRaftCommitSequenceOwnership>,
    ) -> Self {
        Self {
            ownership: Some(ownership),
        }
    }

    pub fn new(file: File, observation_fence: Option<KvObservationOwnerFence>) -> Self {
        let ownership = Arc::new(ClusterKvRaftCommitSequenceOwnership {
            file: Some(file),
            observation_fence,
        });
        if let Some(fence) = ownership.observation_fence.as_ref() {
            fence.bind_proposal_ownership(&ownership);
        }
        Self {
            ownership: Some(ownership),
        }
    }
}

impl Drop for ClusterKvRaftCommitSequenceGuard {
    fn drop(&mut self) {
        drop(self.ownership.take());
    }
}

impl From<File> for ClusterKvRaftCommitSequenceGuard {
    fn from(file: File) -> Self {
        Self::new(file, None)
    }
}

impl Drop for ClusterKvRaftCommitSequenceOwnership {
    fn drop(&mut self) {
        drop(self.file.take());
        drop(self.observation_fence.take());
    }
}

pub trait ClusterKvRaftCommandStorage {
    type Error;

    /// Wraps a KV store error in the host's error type. Replaces the old
    /// `From<KvError>` bound so host error types in other crates do not need
    /// orphan-rule-breaking `From` impls.
    fn kv_error(error: KvError) -> Self::Error;

    /// Only committed delivery failure changes the caller's completion result.
    /// Other derived read-model/convergence faults retain their existing policy.
    fn is_committed_observation_delivery_failure(_error: &Self::Error) -> bool {
        false
    }

    /// Locks one local proposal/finalization phase against concurrent
    /// committers (see [`lock_cluster_kv_commit_sequence`]). The quorum driver
    /// releases this permit after durable append and reacquires it before
    /// revalidation/apply, so transport latency never owns the proposal queue.
    /// Hosts backed by a shared on-disk store must implement this; the default
    /// (no lock) is only safe for single-committer hosts such as tests.
    fn observation_proposal_fence(
        &mut self,
    ) -> Result<Option<KvObservationOwnerFence>, Self::Error> {
        Ok(None)
    }

    fn lock_cluster_raft_commit_sequence(
        &mut self,
    ) -> Result<Option<ClusterKvRaftCommitSequenceGuard>, Self::Error> {
        Ok(None)
    }

    fn replay_cluster_raft_request_if_committed(
        &mut self,
        request: &KvCommandRequest,
    ) -> Result<Option<(KvSnapshot, KvCommittedRequestRecord)>, Self::Error>;

    /// Replays only the durable request record needed to answer an idempotent
    /// command. The default preserves compatibility for snapshot-oriented
    /// hosts; resident production storage overrides it to avoid materializing
    /// the complete keyspace on a replay hit.
    fn replay_cluster_raft_request_record_if_committed(
        &mut self,
        request: &KvCommandRequest,
    ) -> Result<Option<KvCommittedRequestRecord>, Self::Error> {
        self.replay_cluster_raft_request_if_committed(request)
            .map(|replay| replay.map(|(_, record)| record))
    }

    fn append_cluster_raft_request_uncommitted(
        &mut self,
        request: KvCommandRequest,
    ) -> Result<KvLogEntry, Self::Error>;

    /// Releases broad local Raft storage ownership after the proposal is
    /// durably appended. The quorum driver also releases its proposal-sequence
    /// lock before transport. Production resident storage retains only the
    /// exact entry identity needed to revalidate before request construction
    /// and apply.
    fn release_cluster_raft_command_session_for_quorum(&mut self) {}

    fn record_cluster_raft_request_rejected_before_commit(
        &mut self,
        request: &KvCommandRequest,
        failure: &KvAuthorityFailure,
    ) -> Result<(), Self::Error>;

    fn record_cluster_raft_request_unknown(
        &mut self,
        request: &KvCommandRequest,
        detail: &str,
    ) -> Result<(), Self::Error>;

    /// Legacy cleanup hook. Implementations must preserve valid pending log
    /// entries: they may already have been replicated by another proposal or
    /// by background maintenance. Only relinquish local leadership.
    fn discard_cluster_raft_uncommitted_tail_and_clear_leader(&mut self)
    -> Result<(), Self::Error>;

    /// Clears only this node's self-leadership while preserving the exact
    /// pending log entry. This is the terminal path once append transport may
    /// have reached another voter: truncating the entry could erase a proposal
    /// that another node can still commit.
    fn step_down_preserving_cluster_raft_pending_entry(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Raise the process-local higher-term safety floor synchronously at the
    /// network-response linearization point. This must be O(1) and must not
    /// wait for Raft operation or resident admission; durable observation is
    /// the separate mandatory phase below.
    fn publish_cluster_raft_higher_term(
        &mut self,
        _state: &ClusterBootstrapState,
        _term: u64,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn observe_cluster_raft_term(
        &mut self,
        _state: &ClusterBootstrapState,
        _term: u64,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn commit_cluster_raft_index_with_events(
        &mut self,
        index: u64,
    ) -> Result<ClusterKvRaftCommitResult, Self::Error>;

    /// Applies a committed entry and returns only its delta and authority
    /// metadata. Snapshot-oriented hosts may use the compatibility default;
    /// resident production storage overrides it with an `O(command delta)`
    /// implementation.
    fn commit_cluster_raft_index_compact(
        &mut self,
        index: u64,
    ) -> Result<ClusterKvRaftCompactCommitResult, Self::Error> {
        self.commit_cluster_raft_index_with_events(index)
            .map(|result| {
                let revision = result.committed_request.revision;
                let compacted_revision = result.snapshot.compacted_revision;
                ClusterKvRaftCompactCommitResult {
                    txn_response: result.txn_response,
                    events: (result.events).into(),
                    committed_request: result.committed_request,
                    read_state: ClusterKvRaftReadIndexState {
                        current_term: 0,
                        leader_id: None,
                        commit_index: index,
                        commit_term: None,
                        applied_index: index,
                        revision,
                        has_current_term_commit: false,
                    },
                    compacted_revision,
                    checkpoint_snapshot: (Some(result.snapshot)).map(Into::into),
                    final_key_states: (Vec::new()).into(),
                    observation_receipt: None,
                }
            })
    }

    /// Explicitly materializes the complete post-commit KV view for legacy
    /// callers. The compact quorum driver never invokes this method; it exists
    /// only behind [`commit_cluster_kv_raft_command_with_quorum`].
    ///
    /// Snapshot-oriented hosts can keep the compatibility default. Resident
    /// production hosts should call the compact quorum API and serve point or
    /// prefix reads from their resident indexes instead.
    fn materialize_cluster_raft_snapshot_after_commit(
        &mut self,
        request: &KvCommandRequest,
        index: u64,
        replayed_committed_request: bool,
    ) -> Result<KvSnapshot, Self::Error> {
        if replayed_committed_request {
            return self
                .replay_cluster_raft_request_if_committed(request)?
                .map(|(snapshot, _)| snapshot)
                .ok_or_else(|| Self::kv_error(KvError::MissingCommittedEntry { index }));
        }
        self.commit_cluster_raft_index_with_events(index)
            .map(|result| Arc::unwrap_or_clone(result.snapshot))
    }

    /// Releases any operation-scoped resident node after the durable local
    /// commit has completed. The global proposal lock is intentionally held
    /// until this returns so the next proposal cannot race the final local
    /// state publication, but post-commit fanout must not retain either lock.
    fn release_cluster_raft_command_session_after_commit(&mut self) {}

    /// Publishes process-local observations of an already durable commit.
    ///
    /// The quorum driver invokes this only after releasing both the resident
    /// command session and the global commit-sequence lock. Implementations may
    /// synchronously wake local watchers or invoke observers here without
    /// extending the serialized proposal critical section. `None` denotes an
    /// exact committed-request replay, which can still carry repair work queued
    /// while validating the durable request record.
    fn publish_cluster_raft_post_commit_observations(
        &mut self,
        _commit: Option<&ClusterKvRaftCompactCommitResult>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftQuorumCommitOptions {
    pub timeout: Duration,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterKvRaftQuorumCommitResult {
    pub snapshot: KvSnapshot,
    pub txn_response: Option<KvTxnResponse>,
    pub authority: KvAuthorityProof,
    pub replayed_committed_request: bool,
}

/// Quorum-commit result containing a bounded applied delta rather than
/// the complete retained keyspace. This is the canonical result for daemon
/// mutation paths; [`ClusterKvRaftQuorumCommitResult`] is a compatibility
/// materialization for exports and callers that truly require a full view.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterKvRaftQuorumCompactCommitResult {
    pub txn_response: Option<KvTxnResponse>,
    pub events: Arc<Vec<KvEvent>>,
    pub compacted_revision: KvRevision,
    pub checkpoint_snapshot: Option<Arc<KvSnapshot>>,
    pub final_key_states: Arc<Vec<ClusterKvCommittedKeyState>>,
    /// Covers the complete delta when group finalization applied entries after
    /// this request. `authority` and `txn_response` still identify this request.
    /// Transaction RPC responses do not serialize this internal group result.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub applied_authority: Option<KvAuthorityProof>,
    pub authority: KvAuthorityProof,
    pub replayed_committed_request: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterKvTxnCommitResult {
    pub request_id: KvRequestId,
    pub command_digest: KvCommandDigest,
    pub snapshot: KvSnapshot,
    pub txn_response: Option<KvTxnResponse>,
    pub authority: KvAuthorityProof,
    pub replayed_committed_request: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterKvTxnCompactCommitResult {
    pub request_id: KvRequestId,
    /// Digest of the transaction exactly as submitted to the forwarding
    /// voter, before that voter appends authenticated authorization fences.
    pub submitted_command_digest: Option<KvCommandDigest>,
    /// Digest of the command that actually entered the durable Raft log.
    pub command_digest: KvCommandDigest,
    pub txn_response: Option<KvTxnResponse>,
    pub authority: KvAuthorityProof,
    pub replayed_committed_request: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClusterKvRaftTargetReplicationStatus {
    Accepted,
    Rejected,
    Failed,
}

pub struct ClusterKvRaftTargetReplicationAttempt<T, E> {
    pub target: T,
    pub latency_ms: Option<u64>,
    pub result: Result<ClusterRaftAppendResponse, E>,
}

pub type ClusterKvRaftTargetReplicationAttempts<T, E> =
    Vec<ClusterKvRaftTargetReplicationAttempt<T, E>>;

pub type ClusterKvRaftQuorumFuture<'a, T, E> =
    Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftAppendReplicationOptions {
    pub max_attempts: usize,
    pub timeout: Duration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftAppendReplicationOutcome {
    pub response: ClusterRaftAppendResponse,
    pub watch_event_fanout_needed: bool,
}

/// KV-owned append replication retry driver.
///
/// KV owns Raft log catch-up semantics: initial next-index selection, rejected
/// append backoff, acceptance detection, and watch-event fanout eligibility.
/// Hosts own durability access and transport-specific timeout/sending policy.
pub trait ClusterKvRaftAppendReplicationHost {
    type Error;

    /// Wraps a KV store error in the host's error type. Replaces the old
    /// `From<KvError>` bound so host error types in other crates do not need
    /// orphan-rule-breaking `From` impls.
    fn kv_error(error: KvError) -> Self::Error;
    type Target: Clone;

    fn last_local_raft_log_index(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<u64, Self::Error>;

    fn raft_peer_progress_next_index_hint(
        &mut self,
        _state: &ClusterBootstrapState,
        _target: &Self::Target,
        _last_local_raft_log_index: u64,
    ) -> Option<u64> {
        None
    }

    fn build_raft_append_request(
        &mut self,
        state: &ClusterBootstrapState,
        next_index: u64,
    ) -> Result<ClusterRaftAppendRequest, Self::Error>;

    fn append_attempt_timeout(
        &mut self,
        target: &Self::Target,
        remaining: Duration,
        request: &ClusterRaftAppendRequest,
    ) -> Duration;

    fn on_raft_append_sent(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn send_raft_append(
        &mut self,
        target: &Self::Target,
        request: &ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, Self::Error>;
}

pub trait ClusterKvRaftAppendReplicationAsyncHost {
    type Error;

    /// Wraps a KV store error in the host's error type. Replaces the old
    /// `From<KvError>` bound so host error types in other crates do not need
    /// orphan-rule-breaking `From` impls.
    fn kv_error(error: KvError) -> Self::Error;
    type Target: Clone + Send + Sync;

    fn last_local_raft_log_index_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> ClusterKvRaftQuorumFuture<'a, u64, Self::Error>;

    fn raft_peer_progress_next_index_hint_async<'a>(
        &'a mut self,
        _state: &'a ClusterBootstrapState,
        _target: &'a Self::Target,
        _last_local_raft_log_index: u64,
    ) -> ClusterKvRaftQuorumFuture<'a, Option<u64>, Self::Error> {
        Box::pin(async { Ok(None) })
    }

    fn build_raft_append_request_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        next_index: u64,
    ) -> ClusterKvRaftQuorumFuture<'a, ClusterRaftAppendRequest, Self::Error>;

    fn append_attempt_timeout(
        &mut self,
        target: &Self::Target,
        remaining: Duration,
        request: &ClusterRaftAppendRequest,
    ) -> Duration;

    fn on_raft_append_sent_async<'a>(
        &'a mut self,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        Box::pin(async { Ok(()) })
    }

    fn send_raft_append_async<'a>(
        &'a mut self,
        target: &'a Self::Target,
        request: &'a ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> ClusterKvRaftQuorumFuture<'a, ClusterRaftAppendResponse, Self::Error>;
}

pub fn cluster_kv_raft_next_index_after_rejection(
    next_index: u64,
    request: &ClusterRaftAppendRequest,
    match_index: u64,
) -> u64 {
    let rejected_next_index = match_index.saturating_add(1);
    if rejected_next_index < next_index {
        rejected_next_index
    } else if rejected_next_index == next_index
        && request.snapshot.is_none()
        && match_index == request.append.prev_log_index
        && request.append.prev_log_index == request.append.leader_commit
    {
        // The follower reports that its log ends at our prev_log_index but
        // rejected the committed anchor's term. Backtracking by one would resend
        // that conflicting committed entry. Restart from the catch-up anchor so
        // a compacted leader builds an authoritative snapshot instead.
        1
    } else if match_index >= next_index {
        next_index.saturating_div(2).max(1)
    } else {
        next_index.saturating_sub(1).max(1)
    }
}

pub fn replicate_cluster_kv_raft_to_target_with_host<H>(
    state: &ClusterBootstrapState,
    target: &H::Target,
    watch_events: &[KvEvent],
    options: ClusterKvRaftAppendReplicationOptions,
    host: &mut H,
) -> Result<ClusterKvRaftAppendReplicationOutcome, H::Error>
where
    H: ClusterKvRaftAppendReplicationHost,
{
    let deadline = Instant::now() + options.timeout;
    let last_local_raft_log_index = host.last_local_raft_log_index(state)?;
    let default_next_index = last_local_raft_log_index.saturating_add(1);
    let mut next_index = host
        .raft_peer_progress_next_index_hint(state, target, last_local_raft_log_index)
        .filter(|next_index| (1..=default_next_index).contains(next_index))
        .unwrap_or(default_next_index);
    let mut last_response = None;
    for _ in 0..options.max_attempts {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            break;
        }
        let request = host.build_raft_append_request(state, next_index)?;
        let attempt_timeout = host.append_attempt_timeout(target, remaining, &request);
        host.on_raft_append_sent()?;
        let watch_event_fanout_needed =
            cluster_raft_append_request_needs_watch_event_fanout(&request, watch_events);
        let response = host.send_raft_append(target, &request, attempt_timeout)?;
        if response.accepted {
            return Ok(ClusterKvRaftAppendReplicationOutcome {
                response,
                watch_event_fanout_needed,
            });
        }
        next_index = cluster_kv_raft_next_index_after_rejection(
            next_index,
            &request,
            response.append.match_index,
        );
        last_response = Some(response);
    }
    Ok(ClusterKvRaftAppendReplicationOutcome {
        response: last_response.unwrap_or_else(|| ClusterRaftAppendResponse {
            accepted: false,
            detail: format!(
                "raft append did not complete before {}ms catch-up budget",
                options.timeout.as_millis()
            ),
            state: state.clone(),
            append: RaftAppendEntriesResponse {
                term: 0,
                success: false,
                match_index: 0,
            },
        }),
        watch_event_fanout_needed: false,
    })
}

pub async fn replicate_cluster_kv_raft_to_target_async_with_host<H>(
    state: &ClusterBootstrapState,
    target: &H::Target,
    watch_events: &[KvEvent],
    options: ClusterKvRaftAppendReplicationOptions,
    host: &mut H,
) -> Result<ClusterKvRaftAppendReplicationOutcome, H::Error>
where
    H: ClusterKvRaftAppendReplicationAsyncHost,
{
    let deadline = Instant::now() + options.timeout;
    let last_local_raft_log_index = host.last_local_raft_log_index_async(state).await?;
    let default_next_index = last_local_raft_log_index.saturating_add(1);
    let mut next_index = host
        .raft_peer_progress_next_index_hint_async(state, target, last_local_raft_log_index)
        .await?
        .filter(|next_index| (1..=default_next_index).contains(next_index))
        .unwrap_or(default_next_index);
    let mut last_response = None;
    for _ in 0..options.max_attempts {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            break;
        }
        let request = host
            .build_raft_append_request_async(state, next_index)
            .await?;
        let attempt_timeout = host.append_attempt_timeout(target, remaining, &request);
        host.on_raft_append_sent_async().await?;
        let watch_event_fanout_needed =
            cluster_raft_append_request_needs_watch_event_fanout(&request, watch_events);
        let response = host
            .send_raft_append_async(target, &request, attempt_timeout)
            .await?;
        if response.accepted {
            return Ok(ClusterKvRaftAppendReplicationOutcome {
                response,
                watch_event_fanout_needed,
            });
        }
        next_index = cluster_kv_raft_next_index_after_rejection(
            next_index,
            &request,
            response.append.match_index,
        );
        last_response = Some(response);
    }
    Ok(ClusterKvRaftAppendReplicationOutcome {
        response: last_response.unwrap_or_else(|| ClusterRaftAppendResponse {
            accepted: false,
            detail: format!(
                "raft append did not complete before {}ms catch-up budget",
                options.timeout.as_millis()
            ),
            state: state.clone(),
            append: RaftAppendEntriesResponse {
                term: 0,
                success: false,
                match_index: 0,
            },
        }),
        watch_event_fanout_needed: false,
    })
}

/// Storage-side Raft quorum commit driver.
///
/// KV owns the Raft append/commit/rollback decision flow and quorum accounting.
/// The host owns all topology-specific work: choosing remote voter targets,
/// transmitting append RPCs, merging any peer cluster-state observations, and
/// reporting route/connectivity observations.
pub trait ClusterKvRaftQuorumCommitHost: ClusterKvRaftCommandStorage {
    type Target: Clone;

    /// Returns true only when durable local Raft state records this node as the
    /// elected leader for the current term. Reachability or routing preference
    /// alone is not sufficient.
    fn local_has_verified_raft_leadership(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<bool, Self::Error>;

    fn remote_voter_targets(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<Vec<Self::Target>, Self::Error>;

    fn target_node_id<'a>(&self, target: &'a Self::Target) -> &'a NodeId;

    fn target_description(&self, target: &Self::Target) -> String {
        self.target_node_id(target).to_string()
    }

    fn replicate_cluster_raft_to_target(
        &mut self,
        state: &ClusterBootstrapState,
        target: &Self::Target,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, Self::Error>;

    fn record_target_replication_status(
        &mut self,
        _state: &ClusterBootstrapState,
        _target: &Self::Target,
        _status: ClusterKvRaftTargetReplicationStatus,
        _latency_ms: Option<u64>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn observe_accepted_append_state(
        &mut self,
        _state: &mut ClusterBootstrapState,
        _target: &Self::Target,
        _peer_state: ClusterBootstrapState,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn fan_out_committed_cluster_raft_command(
        &mut self,
        _state: ClusterBootstrapState,
        _events: std::sync::Arc<Vec<KvEvent>>,
        _targets: Vec<Self::Target>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn cluster_authority_error(&mut self, failure: KvAuthorityFailure) -> Self::Error;
}

/// Async storage-side Raft quorum commit host.
///
/// This mirrors `ClusterKvRaftQuorumCommitHost`, but every operation that may
/// cross an async storage or transport boundary is supplied by the caller. KV
/// still owns the quorum decision flow; the host owns transport and topology.
pub trait ClusterKvRaftQuorumCommitAsyncHost: Send {
    type Error: ToString + Send;

    /// Wraps a KV store error in the host's error type. Replaces the old
    /// `From<KvError>` bound so host error types in other crates do not need
    /// orphan-rule-breaking `From` impls.
    fn kv_error(error: KvError) -> Self::Error;

    /// Async counterpart of the committed observation failure classifier.
    fn is_committed_observation_delivery_failure(_error: &Self::Error) -> bool {
        false
    }
    type Target: Clone + Send + Sync;

    /// Optional host-owned group admission. A returned entry is already durable
    /// and exposed to concurrent proposals; no caller may discard its suffix.
    /// The host must validate leadership/membership and retain the exact entry
    /// in its command session before returning it.
    fn prepare_cluster_raft_request_async<'a>(
        &'a mut self,
        _state: &'a ClusterBootstrapState,
        _request: &'a KvCommandRequest,
    ) -> ClusterKvRaftQuorumFuture<'a, Option<ClusterKvRaftPreparedProposal>, Self::Error> {
        Box::pin(async { Ok(None) })
    }

    fn local_has_verified_raft_leadership_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> ClusterKvRaftQuorumFuture<'a, bool, Self::Error>;

    /// Async counterpart of
    /// [`ClusterKvRaftCommandStorage::lock_cluster_raft_commit_sequence`].
    fn lock_cluster_raft_commit_sequence_async<'a>(
        &'a mut self,
    ) -> ClusterKvRaftQuorumFuture<'a, Option<ClusterKvRaftCommitSequenceGuard>, Self::Error> {
        Box::pin(async { Ok(None) })
    }

    fn replay_cluster_raft_request_if_committed_async<'a>(
        &'a mut self,
        request: &'a KvCommandRequest,
    ) -> ClusterKvRaftQuorumFuture<'a, Option<(KvSnapshot, KvCommittedRequestRecord)>, Self::Error>;

    fn replay_cluster_raft_request_record_if_committed_async<'a>(
        &'a mut self,
        request: &'a KvCommandRequest,
    ) -> ClusterKvRaftQuorumFuture<'a, Option<KvCommittedRequestRecord>, Self::Error> {
        Box::pin(async move {
            self.replay_cluster_raft_request_if_committed_async(request)
                .await
                .map(|replay| replay.map(|(_, record)| record))
        })
    }

    fn append_cluster_raft_request_uncommitted_async<'a>(
        &'a mut self,
        request: KvCommandRequest,
    ) -> ClusterKvRaftQuorumFuture<'a, KvLogEntry, Self::Error>;

    /// Async counterpart of
    /// [`ClusterKvRaftCommandStorage::release_cluster_raft_command_session_for_quorum`].
    fn release_cluster_raft_command_session_for_quorum_async<'a>(
        &'a mut self,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        Box::pin(async { Ok(()) })
    }

    fn record_cluster_raft_request_rejected_before_commit_async<'a>(
        &'a mut self,
        request: &'a KvCommandRequest,
        failure: &'a KvAuthorityFailure,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error>;

    fn record_cluster_raft_request_unknown_async<'a>(
        &'a mut self,
        request: &'a KvCommandRequest,
        detail: &'a str,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error>;

    fn discard_cluster_raft_uncommitted_tail_and_clear_leader_async<'a>(
        &'a mut self,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error>;

    /// Async counterpart of
    /// [`ClusterKvRaftCommandStorage::step_down_preserving_cluster_raft_pending_entry`].
    fn step_down_preserving_cluster_raft_pending_entry_async<'a>(
        &'a mut self,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        Box::pin(async { Ok(()) })
    }

    /// Synchronous O(1) publication counterpart to the mandatory async
    /// durability phase. It is invoked immediately when a response is decoded,
    /// before allocation, bookkeeping, lock reacquisition, or another await.
    fn publish_cluster_raft_higher_term(
        &mut self,
        _state: &ClusterBootstrapState,
        _term: u64,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn observe_cluster_raft_term_async<'a>(
        &'a mut self,
        _state: &'a ClusterBootstrapState,
        _term: u64,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        Box::pin(async { Ok(()) })
    }

    /// Applies only a prefix certified by distinct current-term voter ACKs.
    /// Hosts without grouped finalization retain the requested-index behavior.
    fn commit_cluster_raft_acknowledged_prefix_compact_async<'a>(
        &'a mut self,
        index: u64,
        _acknowledged_index: u64,
    ) -> ClusterKvRaftQuorumFuture<'a, ClusterKvRaftCompactCommitResult, Self::Error> {
        self.commit_cluster_raft_index_compact_async(index)
    }

    fn commit_cluster_raft_index_with_events_async<'a>(
        &'a mut self,
        index: u64,
    ) -> ClusterKvRaftQuorumFuture<'a, ClusterKvRaftCommitResult, Self::Error>;

    fn commit_cluster_raft_index_compact_async<'a>(
        &'a mut self,
        index: u64,
    ) -> ClusterKvRaftQuorumFuture<'a, ClusterKvRaftCompactCommitResult, Self::Error> {
        Box::pin(async move {
            self.commit_cluster_raft_index_with_events_async(index)
                .await
                .map(|result| {
                    let revision = result.committed_request.revision;
                    let compacted_revision = result.snapshot.compacted_revision;
                    ClusterKvRaftCompactCommitResult {
                        txn_response: result.txn_response,
                        events: (result.events).into(),
                        committed_request: result.committed_request,
                        read_state: ClusterKvRaftReadIndexState {
                            current_term: 0,
                            leader_id: None,
                            commit_index: index,
                            commit_term: None,
                            applied_index: index,
                            revision,
                            has_current_term_commit: false,
                        },
                        compacted_revision,
                        checkpoint_snapshot: (Some(result.snapshot)).map(Into::into),
                        final_key_states: (Vec::new()).into(),
                        observation_receipt: None,
                    }
                })
        })
    }

    /// Async compatibility materialization used only by the legacy full
    /// quorum-commit API. The canonical compact driver does not call it.
    fn materialize_cluster_raft_snapshot_after_commit_async<'a>(
        &'a mut self,
        request: &'a KvCommandRequest,
        index: u64,
        replayed_committed_request: bool,
    ) -> ClusterKvRaftQuorumFuture<'a, KvSnapshot, Self::Error> {
        Box::pin(async move {
            if replayed_committed_request {
                return self
                    .replay_cluster_raft_request_if_committed_async(request)
                    .await?
                    .map(|(snapshot, _)| snapshot)
                    .ok_or_else(|| Self::kv_error(KvError::MissingCommittedEntry { index }));
            }
            self.commit_cluster_raft_index_with_events_async(index)
                .await
                .map(|result| Arc::unwrap_or_clone(result.snapshot))
        })
    }

    /// Async counterpart of
    /// [`ClusterKvRaftCommandStorage::release_cluster_raft_command_session_after_commit`].
    fn release_cluster_raft_command_session_after_commit_async<'a>(
        &'a mut self,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        Box::pin(async { Ok(()) })
    }

    /// Async counterpart of
    /// [`ClusterKvRaftCommandStorage::publish_cluster_raft_post_commit_observations`].
    fn publish_cluster_raft_post_commit_observations_async<'a>(
        &'a mut self,
        _commit: Option<&'a ClusterKvRaftCompactCommitResult>,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        Box::pin(async { Ok(()) })
    }

    fn remote_voter_targets_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> ClusterKvRaftQuorumFuture<'a, Vec<Self::Target>, Self::Error>;

    fn target_node_id<'a>(&self, target: &'a Self::Target) -> &'a NodeId;

    fn target_description(&self, target: &Self::Target) -> String {
        self.target_node_id(target).to_string()
    }

    fn replicate_cluster_raft_to_target_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        target: &'a Self::Target,
        timeout: Duration,
    ) -> ClusterKvRaftQuorumFuture<'a, ClusterRaftAppendResponse, Self::Error>;

    /// Replicates to enough remote voters to satisfy the requested remote
    /// acknowledgement count. The default preserves compatibility for hosts
    /// that can only issue one transport call at a time; network hosts should
    /// override this to race independent voter RPCs and return as soon as the
    /// required acknowledgements arrive.
    fn replicate_cluster_raft_to_quorum_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        targets: Vec<Self::Target>,
        required_remote_acks: usize,
        timeout: Duration,
    ) -> ClusterKvRaftQuorumFuture<
        'a,
        ClusterKvRaftTargetReplicationAttempts<Self::Target, Self::Error>,
        Self::Error,
    > {
        Box::pin(async move {
            let deadline = Instant::now() + timeout;
            let mut accepted = 0_usize;
            let mut attempted_node_ids = RaftNodeIdSet::default();
            let mut attempts = Vec::new();
            for target in targets {
                if accepted >= required_remote_acks {
                    break;
                }
                if !attempted_node_ids
                    .try_insert_clone(
                        self.target_node_id(&target),
                        "dedupe default async raft replication target",
                    )
                    .map_err(Self::kv_error)?
                {
                    continue;
                }
                let remaining = deadline
                    .checked_duration_since(Instant::now())
                    .unwrap_or_default();
                if remaining.is_zero() {
                    break;
                }
                let started = Instant::now();
                let result = self
                    .replicate_cluster_raft_to_target_async(state, &target, remaining)
                    .await;
                let latency_ms =
                    Some(started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64);
                if result.as_ref().is_ok_and(|response| response.accepted) {
                    accepted = accepted.saturating_add(1);
                }
                try_reserve_kv_items(&mut attempts, 1, "record async raft replication attempt")
                    .map_err(Self::kv_error)?;
                attempts.push(ClusterKvRaftTargetReplicationAttempt {
                    target,
                    latency_ms,
                    result,
                });
            }
            Ok(attempts)
        })
    }

    fn record_target_replication_status_async<'a>(
        &'a mut self,
        _state: &'a ClusterBootstrapState,
        _target: &'a Self::Target,
        _status: ClusterKvRaftTargetReplicationStatus,
        _latency_ms: Option<u64>,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        Box::pin(async { Ok(()) })
    }

    fn observe_accepted_append_state_async<'a>(
        &'a mut self,
        _state: &'a mut ClusterBootstrapState,
        _target: &'a Self::Target,
        _peer_state: ClusterBootstrapState,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        Box::pin(async { Ok(()) })
    }

    fn fan_out_committed_cluster_raft_command_async<'a>(
        &'a mut self,
        _state: ClusterBootstrapState,
        _events: std::sync::Arc<Vec<KvEvent>>,
        _targets: Vec<Self::Target>,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        Box::pin(async { Ok(()) })
    }

    fn cluster_authority_error(&mut self, failure: KvAuthorityFailure) -> Self::Error;
}

fn reconcile_cluster_raft_post_append_failure<H>(
    host: &mut H,
    request: &KvCommandRequest,
    detail: &str,
    transport_may_have_applied: bool,
) -> Result<(), H::Error>
where
    H: ClusterKvRaftQuorumCommitHost,
{
    if !transport_may_have_applied {
        return host.discard_cluster_raft_uncommitted_tail_and_clear_leader();
    }

    // The attempt ledger is an independent durability domain. Ambiguous
    // outcome recording may legitimately wait for its own cross-process lock,
    // so never retain resident Raft or operation ownership across that wait.
    host.release_cluster_raft_command_session_for_quorum();
    // Both actions are mandatory and independent. Even if the attempt ledger
    // write fails, still step down; even if stepping down fails, retain the
    // ledger error only after attempting the safety transition.
    let record_error = host
        .record_cluster_raft_request_unknown(request, detail)
        .err();
    let step_down_error = host.step_down_preserving_cluster_raft_pending_entry().err();
    match step_down_error.or(record_error) {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

async fn reconcile_cluster_raft_post_append_failure_async<H>(
    host: &mut H,
    request: &KvCommandRequest,
    detail: &str,
    transport_may_have_applied: bool,
) -> Result<(), H::Error>
where
    H: ClusterKvRaftQuorumCommitAsyncHost,
{
    if !transport_may_have_applied {
        return host
            .discard_cluster_raft_uncommitted_tail_and_clear_leader_async()
            .await;
    }

    let record_error = host
        .record_cluster_raft_request_unknown_async(request, detail)
        .await
        .err();
    let step_down_error = host
        .step_down_preserving_cluster_raft_pending_entry_async()
        .await
        .err();
    match step_down_error.or(record_error) {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// Compatibility quorum commit that explicitly materializes the complete KV
/// snapshot after the compact commit has become durable. New daemon mutation
/// paths should call [`commit_cluster_kv_raft_command_compact_with_quorum`].
///
/// A materialization error is a read-view failure after the returned authority
/// index is already durably committed; it does not roll back the request. It is
/// surfaced as [`KvError::CommittedViewUnavailable`] through the host's error
/// adapter, carrying the exact request id and authority coordinates. A caller
/// must preserve that identity for reconciliation rather than resubmit under a
/// new id. Latency-sensitive mutation paths should use the compact API, whose
/// success is not coupled to full-store materialization.
pub fn commit_cluster_kv_raft_command_with_quorum<H>(
    state: &mut ClusterBootstrapState,
    request: KvCommandRequest,
    options: ClusterKvRaftQuorumCommitOptions,
    host: &mut H,
) -> Result<ClusterKvRaftQuorumCommitResult, H::Error>
where
    H: ClusterKvRaftQuorumCommitHost,
    H::Error: ToString,
{
    let materialization_request = request.clone();
    let compact =
        commit_cluster_kv_raft_command_compact_with_quorum(state, request, options, host)?;
    let snapshot = match compact.checkpoint_snapshot.clone() {
        // Snapshot-oriented compatibility hosts already paid for a complete
        // projection while applying the entry. Reuse that exact view instead
        // of invoking their commit callback a second time.
        Some(snapshot) => Arc::unwrap_or_clone(snapshot),
        None => {
            let materialized = host.materialize_cluster_raft_snapshot_after_commit(
                &materialization_request,
                compact.authority.commit_index,
                compact.replayed_committed_request,
            );
            // A compatibility materializer may reopen resident Raft state
            // after the compact commit released it. Return that checkout
            // before this wrapper completes; otherwise a reused host can hold
            // the resident lock while its next proposal waits for the commit
            // sequence, deadlocking against a finalizer with the inverse lock
            // order. The release contract is intentionally idempotent.
            host.release_cluster_raft_command_session_after_commit();
            match materialized {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    let detail = error.to_string();
                    report_cluster_kv_post_commit_fault("full-view materialization", &detail);
                    return Err(H::kv_error(KvError::CommittedViewUnavailable {
                        request_id: materialization_request.request_id.clone(),
                        term: compact.authority.term,
                        log_index: compact.authority.commit_index,
                        revision: compact.authority.revision,
                        detail,
                    }));
                }
            }
        }
    };
    Ok(ClusterKvRaftQuorumCommitResult {
        snapshot,
        txn_response: compact.txn_response,
        authority: compact.authority,
        replayed_committed_request: compact.replayed_committed_request,
    })
}

fn compact_quorum_result_for_exact_replay(
    record: KvCommittedRequestRecord,
) -> ClusterKvRaftQuorumCompactCommitResult {
    ClusterKvRaftQuorumCompactCommitResult {
        applied_authority: None,
        txn_response: record.txn_response.clone(),
        events: (Vec::new()).into(),
        compacted_revision: KvRevision::ZERO,
        checkpoint_snapshot: None,
        final_key_states: (Vec::new()).into(),
        authority: KvAuthorityProof {
            term: record.term,
            commit_index: record.log_index,
            revision: record.revision,
            basis: KvAuthorityBasis::ExactCommittedRequest,
        },
        replayed_committed_request: true,
    }
}

fn report_cluster_kv_post_commit_fault(stage: &'static str, detail: impl std::fmt::Display) {
    metrics::counter!(
        "sinomo.kv.raft.post_commit_faults_total",
        "stage" => stage
    )
    .increment(1);
    eprintln!(
        "error: cluster KV request is durably committed but post-commit {stage} failed; convergence remains pending: {detail}"
    );
}

/// Canonical quorum mutation path. It keeps the resident Raft state alive,
/// applies one command, and returns only that command's delta and authority
/// proof. No full-store snapshot is built unless the periodic checkpoint
/// threshold is reached naturally during the commit.
pub fn commit_cluster_kv_raft_command_compact_with_quorum<H>(
    state: &mut ClusterBootstrapState,
    request: KvCommandRequest,
    options: ClusterKvRaftQuorumCommitOptions,
    host: &mut H,
) -> Result<ClusterKvRaftQuorumCompactCommitResult, H::Error>
where
    H: ClusterKvRaftQuorumCommitHost,
    H::Error: ToString,
{
    let mut command_metrics = KvCommandMetricGuard::start(&request.command);
    let mut concurrency_metrics = KvCommitConcurrencyMetricGuard::queued();
    let mut commit_sequence_lock = Some({
        let _phase = KvCommandPhaseMetricGuard::start("commit_sequence_wait");
        host.lock_cluster_raft_commit_sequence()?
    });
    concurrency_metrics.acquired();
    let replay_result = {
        let _phase = KvCommandPhaseMetricGuard::start("request_replay");
        host.replay_cluster_raft_request_record_if_committed(&request)
    };
    let replay = match replay_result {
        Ok(replay) => replay,
        Err(error) => {
            host.release_cluster_raft_command_session_for_quorum();
            drop(commit_sequence_lock);
            return Err(error);
        }
    };
    if let Some(record) = replay {
        command_metrics.finish("replayed");
        host.release_cluster_raft_command_session_after_commit();
        drop(commit_sequence_lock);
        if let Err(error) = host.publish_cluster_raft_post_commit_observations(None) {
            report_cluster_kv_post_commit_fault("exact replay publication", error.to_string());
        }
        return Ok(compact_quorum_result_for_exact_replay(record));
    }
    let local_has_leadership_result = {
        let _phase = KvCommandPhaseMetricGuard::start("leadership_check");
        host.local_has_verified_raft_leadership(state)
    };
    let local_has_leadership = match local_has_leadership_result {
        Ok(local_has_leadership) => local_has_leadership,
        Err(error) => {
            host.release_cluster_raft_command_session_for_quorum();
            drop(commit_sequence_lock);
            return Err(error);
        }
    };
    if !local_has_leadership {
        let failure = cluster_kv_authority_failure(
            state,
            if cluster_kv_raft_local_is_voter(state) {
                KvAuthorityFailureReason::VoterRejected
            } else {
                KvAuthorityFailureReason::LocalNodeNotVoter
            },
            "local node lacks verified current-term Raft leadership".to_string(),
            0,
            KvRevision::ZERO,
            usize::from(cluster_kv_raft_local_is_voter(state)),
            None,
        );
        command_metrics.finish("not_leader");
        host.release_cluster_raft_command_session_for_quorum();
        let record_result =
            host.record_cluster_raft_request_rejected_before_commit(&request, &failure);
        drop(commit_sequence_lock);
        record_result?;
        return Err(host.cluster_authority_error(failure));
    }
    let request_for_outcome = request.clone();
    let append_result = {
        let _phase = KvCommandPhaseMetricGuard::start("local_append");
        host.append_cluster_raft_request_uncommitted(request)
    };
    let entry = match append_result {
        Ok(entry) => entry,
        Err(error) => {
            host.release_cluster_raft_command_session_for_quorum();
            drop(commit_sequence_lock);
            return Err(error);
        }
    };
    host.release_cluster_raft_command_session_for_quorum();
    // The durable entry and its exact identity are now sufficient to resume a
    // normal proposal. Do not retain the cross-process proposal queue while
    // WAN quorum I/O is in flight. Membership safety is enforced by the
    // durable pending-config fence during every later append, matching etcd's
    // `pendingConfIndex` without a WAN-sized mutex critical section.
    let proposal_exposed_to_pipeline = true;
    drop(commit_sequence_lock.take());

    macro_rules! reacquire_commit_sequence {
        () => {{
            if commit_sequence_lock.is_none() {
                let _phase = KvCommandPhaseMetricGuard::start("commit_sequence_finalize_wait");
                commit_sequence_lock = Some(match host.lock_cluster_raft_commit_sequence() {
                    Ok(lock) => lock,
                    Err(error) => {
                        command_metrics.finish("finalize_lock_error");
                        let detail = format!(
                            "Raft proposal at term {} index {} could not reacquire ordered finalization after pipeline exposure: {}",
                            entry.term,
                            entry.index,
                            error.to_string()
                        );
                        let reconciliation = reconcile_cluster_raft_post_append_failure(
                            host,
                            &request_for_outcome,
                            &detail,
                            true,
                        );
                        reconciliation?;
                        let failure = cluster_kv_authority_failure(
                            state,
                            KvAuthorityFailureReason::Timeout,
                            detail,
                            entry.term,
                            entry.revision,
                            usize::from(cluster_kv_raft_local_is_voter(state)),
                            Some(state.local_node_id.clone()),
                        );
                        return Err(host.cluster_authority_error(failure));
                    }
                });
            }
        }};
    }

    macro_rules! return_pipeline_replay_if_committed {
        () => {{
            let replay = host.replay_cluster_raft_request_record_if_committed(&request_for_outcome);
            match replay {
                Ok(Some(record)) => {
                    command_metrics.finish("replayed_after_pipeline");
                    host.release_cluster_raft_command_session_after_commit();
                    drop(commit_sequence_lock.take());
                    if let Err(error) = host.publish_cluster_raft_post_commit_observations(None) {
                        report_cluster_kv_post_commit_fault(
                            "pipelined exact replay publication",
                            error.to_string(),
                        );
                    }
                    return Ok(compact_quorum_result_for_exact_replay(record));
                }
                Ok(None) => {}
                Err(error) => {
                    let detail = format!(
                        "failed to reconcile exact Raft proposal at term {} index {} after pipeline release: {}",
                        entry.term,
                        entry.index,
                        error.to_string()
                    );
                    command_metrics.finish("pipeline_replay_error");
                    let reconciliation = reconcile_cluster_raft_post_append_failure(
                        host,
                        &request_for_outcome,
                        &detail,
                        true,
                    );
                    drop(commit_sequence_lock.take());
                    reconciliation?;
                    return Err(error);
                }
            }
        }};
    }

    let mut transport_may_have_applied = false;
    macro_rules! post_append_try {
        ($result:expr, $stage:literal, $force_ambiguous:expr) => {{
            match $result {
                Ok(value) => value,
                Err(error) => {
                    let detail = format!(
                        "Raft proposal failed during {} after durable append at term {} index {}",
                        $stage, entry.term, entry.index
                    );
                    command_metrics.finish("post_append_error");
                    reacquire_commit_sequence!();
                    if proposal_exposed_to_pipeline {
                        return_pipeline_replay_if_committed!();
                    }
                    let reconciliation = reconcile_cluster_raft_post_append_failure(
                        host,
                        &request_for_outcome,
                        &detail,
                        proposal_exposed_to_pipeline
                            || $force_ambiguous
                            || transport_may_have_applied,
                    );
                    drop(commit_sequence_lock.take());
                    reconciliation?;
                    return Err(error);
                }
            }
        }};
    }

    let mut voter_acks = RaftNodeIdSet::default();
    let mut fanout_node_ids = RaftNodeIdSet::default();
    let mut fanout_targets = Vec::new();
    let mut deferred_replication_statuses = Vec::new();
    if cluster_kv_raft_local_is_voter(state) {
        let local_ack = voter_acks
            .try_insert_clone(&state.local_node_id, "record local raft commit ack")
            .map_err(<H as ClusterKvRaftCommandStorage>::kv_error);
        post_append_try!(local_ack, "local quorum accounting", false);
    }
    let quorum_size = cluster_kv_raft_quorum_size_for_state(state);
    let mut failures = Vec::new();
    let mut higher_term_observed: Option<u64> = None;
    let deadline = Instant::now() + options.timeout;
    let quorum_metrics = KvCommandPhaseMetricGuard::start("quorum");
    let targets = post_append_try!(
        host.remote_voter_targets(state),
        "remote voter discovery",
        false
    );
    for target in &targets {
        if voter_acks.len() >= quorum_size {
            break;
        }
        if voter_acks.contains(host.target_node_id(target)) {
            continue;
        }
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            failures.push(format!(
                "raft command did not reach quorum before {}ms replication budget",
                options.timeout.as_millis()
            ));
            break;
        }
        let started = Instant::now();
        match host.replicate_cluster_raft_to_target(state, target, remaining) {
            Ok(response) if response.accepted => {
                transport_may_have_applied = true;
                let reserve_status = try_reserve_kv_items(
                    &mut deferred_replication_statuses,
                    1,
                    "defer accepted Raft replication status",
                )
                .map_err(<H as ClusterKvRaftCommandStorage>::kv_error);
                post_append_try!(reserve_status, "accepted replication accounting", true);
                deferred_replication_statuses.push((
                    target.clone(),
                    ClusterKvRaftTargetReplicationStatus::Accepted,
                    Some(started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64),
                ));
                let remote_ack = voter_acks
                    .try_insert_clone(host.target_node_id(target), "record remote raft commit ack")
                    .map_err(<H as ClusterKvRaftCommandStorage>::kv_error);
                post_append_try!(remote_ack, "accepted voter accounting", true);
                let observe = host.observe_accepted_append_state(state, target, response.state);
                post_append_try!(observe, "accepted peer-state observation", true);
                let insert_fanout = fanout_node_ids
                    .try_insert_clone(
                        host.target_node_id(target),
                        "record accepted raft fanout target",
                    )
                    .map_err(<H as ClusterKvRaftCommandStorage>::kv_error);
                if post_append_try!(insert_fanout, "accepted fanout accounting", true) {
                    let reserve_fanout = try_reserve_kv_items(
                        &mut fanout_targets,
                        1,
                        "record accepted raft fanout target",
                    )
                    .map_err(<H as ClusterKvRaftCommandStorage>::kv_error);
                    post_append_try!(reserve_fanout, "accepted fanout reservation", true);
                    fanout_targets.push(target.clone());
                }
            }
            Ok(response) => {
                if response.append.term > entry.term {
                    let publish =
                        host.publish_cluster_raft_higher_term(state, response.append.term);
                    post_append_try!(publish, "publish rejected peer term floor", true);
                    higher_term_observed = Some(
                        higher_term_observed
                            .unwrap_or_default()
                            .max(response.append.term),
                    );
                }
                let reserve_status = try_reserve_kv_items(
                    &mut deferred_replication_statuses,
                    1,
                    "defer rejected Raft replication status",
                )
                .map_err(<H as ClusterKvRaftCommandStorage>::kv_error);
                post_append_try!(reserve_status, "rejected replication accounting", false);
                deferred_replication_statuses.push((
                    target.clone(),
                    ClusterKvRaftTargetReplicationStatus::Rejected,
                    Some(started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64),
                ));
                failures.push(format!(
                    "{} rejected append: {} (term {}, match_index {})",
                    host.target_description(target),
                    response.detail,
                    response.append.term,
                    response.append.match_index
                ));
            }
            Err(error) => {
                transport_may_have_applied = true;
                let reserve_status = try_reserve_kv_items(
                    &mut deferred_replication_statuses,
                    1,
                    "defer failed Raft replication status",
                )
                .map_err(<H as ClusterKvRaftCommandStorage>::kv_error);
                post_append_try!(reserve_status, "failed replication accounting", true);
                deferred_replication_statuses.push((
                    target.clone(),
                    ClusterKvRaftTargetReplicationStatus::Failed,
                    None,
                ));
                failures.push(format!(
                    "{} failed: {}",
                    host.target_description(target),
                    error.to_string()
                ));
            }
        }
    }
    if let Some(term) = higher_term_observed {
        // Persist before commit-sequence reacquisition. Publication above is
        // already fail-closed; this mandatory phase must not wait behind a
        // proposal holding the sequence lock while it needs resident storage.
        let observe_term = host.observe_cluster_raft_term(state, term);
        post_append_try!(observe_term, "rejected peer term observation", true);
    }
    drop(quorum_metrics);
    metrics::histogram!("sinomo.kv.quorum.acks", "kind" => "received")
        .record(voter_acks.len() as f64);
    metrics::histogram!("sinomo.kv.quorum.acks", "kind" => "required").record(quorum_size as f64);
    reacquire_commit_sequence!();

    if voter_acks.len() < quorum_size || higher_term_observed.is_some() {
        // A higher-term response is durable Raft input even if another
        // pipelined finalizer already committed this exact request as part of
        // its prefix. It was observed above; now an exact replay can complete
        // the caller without an unnecessary step-down.
        if proposal_exposed_to_pipeline {
            return_pipeline_replay_if_committed!();
        }
        let detail = format!(
            "raft command at index {} replicated to {} voters, {} required{}{}",
            entry.index,
            voter_acks.len(),
            quorum_size,
            higher_term_observed
                .map(|term| format!("; observed higher term {term}"))
                .unwrap_or_default(),
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        );
        let reason = if higher_term_observed.is_some() {
            command_metrics.finish("higher_term");
            KvAuthorityFailureReason::HigherTermObserved
        } else if Instant::now() >= deadline {
            command_metrics.finish("timeout");
            KvAuthorityFailureReason::Timeout
        } else {
            command_metrics.finish("no_quorum");
            KvAuthorityFailureReason::NoQuorum
        };
        let reconciliation = reconcile_cluster_raft_post_append_failure(
            host,
            &request_for_outcome,
            &detail,
            proposal_exposed_to_pipeline || transport_may_have_applied,
        );
        let failure = cluster_kv_authority_failure(
            state,
            reason,
            detail,
            entry.term,
            KvRevision::ZERO,
            voter_acks.len(),
            Some(state.local_node_id.clone()),
        );
        drop(commit_sequence_lock.take());
        reconciliation?;
        for (target, status, latency_ms) in deferred_replication_statuses {
            let _ = host.record_target_replication_status(state, &target, status, latency_ms);
        }
        return Err(host.cluster_authority_error(failure));
    }

    // A higher-index pipelined finalizer may already have committed this exact
    // prefix while our quorum wave was in flight. Reconcile before applying so
    // the earlier caller receives ExactCommittedRequest rather than a fresh
    // proof paired with the later entry's materialized snapshot.
    if proposal_exposed_to_pipeline {
        return_pipeline_replay_if_committed!();
    }

    let commit_result = {
        let _phase = KvCommandPhaseMetricGuard::start("local_apply");
        host.commit_cluster_raft_index_compact(entry.index)
    };
    let commit = post_append_try!(commit_result, "local apply after quorum", true);
    let authority = KvAuthorityProof {
        term: entry.term,
        commit_index: entry.index,
        revision: commit.committed_request.revision,
        basis: KvAuthorityBasis::CurrentTermQuorum {
            voter_acks: voter_acks.len(),
            required_quorum: quorum_size,
        },
    };
    if let Some(receipt) = commit.observation_receipt.as_ref() {
        receipt.observe_original_current_term_commits(
            &state.local_node_id,
            &request_for_outcome,
            &commit.committed_request,
            &entry,
            authority,
        );
    }
    command_metrics.finish("committed");
    host.release_cluster_raft_command_session_after_commit();
    // Append, quorum confirmation, and durable local apply are the serialized
    // commit. Post-response convergence is operation-owned but must not keep
    // the resident node, operation guard, or global proposal queue locked
    // across fanout transport work.
    drop(commit_sequence_lock.take());
    let publication_failure =
        match host.publish_cluster_raft_post_commit_observations(Some(&commit)) {
            Ok(()) => None,
            Err(error) => {
                report_cluster_kv_post_commit_fault("commit publication", error.to_string());
                H::is_committed_observation_delivery_failure(&error).then_some(error)
            }
        };
    for (target, status, latency_ms) in deferred_replication_statuses {
        let _ = host.record_target_replication_status(state, &target, status, latency_ms);
    }
    for target in targets {
        let inserted = match fanout_node_ids.try_insert_clone(
            host.target_node_id(&target),
            "dedupe committed raft fanout target",
        ) {
            Ok(inserted) => inserted,
            Err(error) => {
                report_cluster_kv_post_commit_fault("fanout target deduplication", error);
                continue;
            }
        };
        if inserted {
            if let Err(error) = try_reserve_kv_items(
                &mut fanout_targets,
                1,
                "dedupe committed raft fanout target",
            ) {
                report_cluster_kv_post_commit_fault("fanout target allocation", error);
                continue;
            }
            fanout_targets.push(target);
        }
    }
    // The fan-out replicates the committed history to every member (learners
    // included — they converge event-driven on commit), so it runs even when
    // there are no REMOTE VOTER targets (single-voter clusters).
    {
        let _phase = KvCommandPhaseMetricGuard::start("fanout_dispatch");
        if let Err(error) = host.fan_out_committed_cluster_raft_command(
            state.clone(),
            commit.events.clone(),
            fanout_targets,
        ) {
            report_cluster_kv_post_commit_fault("fanout dispatch", error.to_string());
        }
    }
    // Keep required post-commit convergence above, then preserve the delivery
    // failure's committed/unknown classification for the public caller.
    if let Some(error) = publication_failure {
        return Err(error);
    }
    Ok(ClusterKvRaftQuorumCompactCommitResult {
        applied_authority: None,
        txn_response: commit.txn_response,
        events: (commit.events).into(),
        compacted_revision: commit.compacted_revision,
        checkpoint_snapshot: (commit.checkpoint_snapshot).map(Into::into),
        final_key_states: (commit.final_key_states).into(),
        authority,
        replayed_committed_request: false,
    })
}

/// Async full-view compatibility wrapper.
///
/// As in the synchronous wrapper, failure to materialize the complete snapshot
/// occurs after the compact result is durably committed and is returned through
/// [`KvError::CommittedViewUnavailable`] with its exact authority coordinates.
/// Latency-sensitive mutations should use
/// [`commit_cluster_kv_raft_command_compact_with_quorum_async`]; consumers of
/// this API preserve the request identity for any reconciliation retry.
pub async fn commit_cluster_kv_raft_command_with_quorum_async<H>(
    state: &mut ClusterBootstrapState,
    request: KvCommandRequest,
    options: ClusterKvRaftQuorumCommitOptions,
    host: &mut H,
) -> Result<ClusterKvRaftQuorumCommitResult, H::Error>
where
    H: ClusterKvRaftQuorumCommitAsyncHost,
{
    let materialization_request = request.clone();
    let compact =
        commit_cluster_kv_raft_command_compact_with_quorum_async(state, request, options, host)
            .await?;
    let snapshot = match compact.checkpoint_snapshot.clone() {
        // Match the synchronous compatibility path: the default compact
        // adapter already returned the complete post-commit view.
        Some(snapshot) => Arc::unwrap_or_clone(snapshot),
        None => {
            let materialized = host
                .materialize_cluster_raft_snapshot_after_commit_async(
                    &materialization_request,
                    compact.authority.commit_index,
                    compact.replayed_committed_request,
                )
                .await;
            if let Err(release_error) = host
                .release_cluster_raft_command_session_after_commit_async()
                .await
            {
                let detail = match &materialized {
                    Ok(_) => format!(
                        "full-view materialization completed, but releasing its Raft command session failed: {}",
                        release_error.to_string()
                    ),
                    Err(materialization_error) => format!(
                        "full-view materialization failed: {}; releasing its Raft command session also failed: {}",
                        materialization_error.to_string(),
                        release_error.to_string()
                    ),
                };
                report_cluster_kv_post_commit_fault(
                    "full-view materialization session release",
                    &detail,
                );
                return Err(H::kv_error(KvError::CommittedViewUnavailable {
                    request_id: materialization_request.request_id.clone(),
                    term: compact.authority.term,
                    log_index: compact.authority.commit_index,
                    revision: compact.authority.revision,
                    detail,
                }));
            }
            match materialized {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    let detail = error.to_string();
                    report_cluster_kv_post_commit_fault("full-view materialization", &detail);
                    return Err(H::kv_error(KvError::CommittedViewUnavailable {
                        request_id: materialization_request.request_id.clone(),
                        term: compact.authority.term,
                        log_index: compact.authority.commit_index,
                        revision: compact.authority.revision,
                        detail,
                    }));
                }
            }
        }
    };
    Ok(ClusterKvRaftQuorumCommitResult {
        snapshot,
        txn_response: compact.txn_response,
        authority: compact.authority,
        replayed_committed_request: compact.replayed_committed_request,
    })
}

/// Async counterpart of
/// [`commit_cluster_kv_raft_command_compact_with_quorum`].
pub async fn commit_cluster_kv_raft_command_compact_with_quorum_async<H>(
    state: &mut ClusterBootstrapState,
    request: KvCommandRequest,
    options: ClusterKvRaftQuorumCommitOptions,
    host: &mut H,
) -> Result<ClusterKvRaftQuorumCompactCommitResult, H::Error>
where
    H: ClusterKvRaftQuorumCommitAsyncHost,
{
    let mut command_metrics = KvCommandMetricGuard::start(&request.command);
    let mut concurrency_metrics = KvCommitConcurrencyMetricGuard::queued();
    let mut commit_sequence_lock = None;
    let mut proposal_exposed_to_pipeline = false;
    let request_for_outcome = request.clone();
    let prepared = host
        .prepare_cluster_raft_request_async(state, &request)
        .await?;
    let entry = if let Some(prepared) = prepared {
        concurrency_metrics.acquired();
        match prepared {
            ClusterKvRaftPreparedProposal::Replayed(record) => {
                command_metrics.finish("replayed");
                if let Err(error) = host
                    .publish_cluster_raft_post_commit_observations_async(None)
                    .await
                {
                    report_cluster_kv_post_commit_fault(
                        "group replay publication",
                        error.to_string(),
                    );
                }
                return Ok(compact_quorum_result_for_exact_replay(record));
            }
            ClusterKvRaftPreparedProposal::Appended(entry) => {
                proposal_exposed_to_pipeline = true;
                entry
            }
        }
    } else {
        commit_sequence_lock = Some({
            let _phase = KvCommandPhaseMetricGuard::start("commit_sequence_wait");
            host.lock_cluster_raft_commit_sequence_async().await?
        });
        concurrency_metrics.acquired();
        let replay_result = {
            let _phase = KvCommandPhaseMetricGuard::start("request_replay");
            host.replay_cluster_raft_request_record_if_committed_async(&request)
                .await
        };
        let replay = match replay_result {
            Ok(replay) => replay,
            Err(error) => {
                let release = host
                    .release_cluster_raft_command_session_for_quorum_async()
                    .await;
                drop(commit_sequence_lock);
                return match release {
                    Ok(()) => Err(error),
                    Err(release_error) => Err(release_error),
                };
            }
        };
        if let Some(record) = replay {
            command_metrics.finish("replayed");
            if let Err(error) = host
                .release_cluster_raft_command_session_after_commit_async()
                .await
            {
                report_cluster_kv_post_commit_fault(
                    "exact replay session release",
                    error.to_string(),
                );
            }
            drop(commit_sequence_lock);
            if let Err(error) = host
                .publish_cluster_raft_post_commit_observations_async(None)
                .await
            {
                report_cluster_kv_post_commit_fault("exact replay publication", error.to_string());
            }
            return Ok(compact_quorum_result_for_exact_replay(record));
        }
        let local_has_leadership_result = {
            let _phase = KvCommandPhaseMetricGuard::start("leadership_check");
            host.local_has_verified_raft_leadership_async(state).await
        };
        let local_has_leadership = match local_has_leadership_result {
            Ok(local_has_leadership) => local_has_leadership,
            Err(error) => {
                let release = host
                    .release_cluster_raft_command_session_for_quorum_async()
                    .await;
                drop(commit_sequence_lock);
                return match release {
                    Ok(()) => Err(error),
                    Err(release_error) => Err(release_error),
                };
            }
        };
        if !local_has_leadership {
            let failure = cluster_kv_authority_failure(
                state,
                if cluster_kv_raft_local_is_voter(state) {
                    KvAuthorityFailureReason::VoterRejected
                } else {
                    KvAuthorityFailureReason::LocalNodeNotVoter
                },
                "local node lacks verified current-term Raft leadership".to_string(),
                0,
                KvRevision::ZERO,
                usize::from(cluster_kv_raft_local_is_voter(state)),
                None,
            );
            command_metrics.finish("not_leader");
            let release = host
                .release_cluster_raft_command_session_for_quorum_async()
                .await;
            if let Err(error) = release {
                drop(commit_sequence_lock);
                return Err(error);
            }
            let record_result = host
                .record_cluster_raft_request_rejected_before_commit_async(&request, &failure)
                .await;
            drop(commit_sequence_lock);
            record_result?;
            return Err(host.cluster_authority_error(failure));
        }
        let append_result = {
            let _phase = KvCommandPhaseMetricGuard::start("local_append");
            host.append_cluster_raft_request_uncommitted_async(request)
                .await
        };
        match append_result {
            Ok(entry) => entry,
            Err(error) => {
                let release = host
                    .release_cluster_raft_command_session_for_quorum_async()
                    .await;
                drop(commit_sequence_lock);
                return match release {
                    Ok(()) => Err(error),
                    Err(release_error) => Err(release_error),
                };
            }
        }
    };

    let mut transport_may_have_applied = false;
    macro_rules! reacquire_commit_sequence_async {
        () => {{
            if commit_sequence_lock.is_none() {
                let _phase = KvCommandPhaseMetricGuard::start("commit_sequence_finalize_wait");
                commit_sequence_lock = Some(match host.lock_cluster_raft_commit_sequence_async().await {
                    Ok(lock) => lock,
                    Err(error) => {
                        command_metrics.finish("finalize_lock_error");
                        let detail = format!(
                            "Raft proposal at term {} index {} could not reacquire ordered finalization after pipeline exposure: {}",
                            entry.term,
                            entry.index,
                            error.to_string()
                        );
                        let reconciliation = reconcile_cluster_raft_post_append_failure_async(
                            host,
                            &request_for_outcome,
                            &detail,
                            true,
                        )
                        .await;
                        reconciliation?;
                        let failure = cluster_kv_authority_failure(
                            state,
                            KvAuthorityFailureReason::Timeout,
                            detail,
                            entry.term,
                            entry.revision,
                            usize::from(cluster_kv_raft_local_is_voter(state)),
                            Some(state.local_node_id.clone()),
                        );
                        return Err(host.cluster_authority_error(failure));
                    }
                });
            }
        }};
    }

    macro_rules! return_pipeline_replay_if_committed_async {
        () => {{
            let replay = host
                .replay_cluster_raft_request_record_if_committed_async(&request_for_outcome)
                .await;
            match replay {
                Ok(Some(record)) => {
                    command_metrics.finish("replayed_after_pipeline");
                    if let Err(error) = host
                        .release_cluster_raft_command_session_after_commit_async()
                        .await
                    {
                        report_cluster_kv_post_commit_fault(
                            "pipelined exact replay session release",
                            error.to_string(),
                        );
                    }
                    drop(commit_sequence_lock.take());
                    if let Err(error) = host
                        .publish_cluster_raft_post_commit_observations_async(None)
                        .await
                    {
                        report_cluster_kv_post_commit_fault(
                            "pipelined exact replay publication",
                            error.to_string(),
                        );
                    }
                    return Ok(compact_quorum_result_for_exact_replay(record));
                }
                Ok(None) => {}
                Err(error) => {
                    let detail = format!(
                        "failed to reconcile exact Raft proposal at term {} index {} after pipeline release: {}",
                        entry.term,
                        entry.index,
                        error.to_string()
                    );
                    command_metrics.finish("pipeline_replay_error");
                    let reconciliation = reconcile_cluster_raft_post_append_failure_async(
                        host,
                        &request_for_outcome,
                        &detail,
                        true,
                    )
                    .await;
                    drop(commit_sequence_lock.take());
                    reconciliation?;
                    return Err(error);
                }
            }
        }};
    }

    macro_rules! post_append_try_async {
        ($result:expr, $stage:literal, $force_ambiguous:expr) => {{
            match $result {
                Ok(value) => value,
                Err(error) => {
                    let detail = format!(
                        "Raft proposal failed during {} after durable append at term {} index {}",
                        $stage, entry.term, entry.index
                    );
                    command_metrics.finish("post_append_error");
                    reacquire_commit_sequence_async!();
                    if proposal_exposed_to_pipeline {
                        return_pipeline_replay_if_committed_async!();
                    }
                    let reconciliation = reconcile_cluster_raft_post_append_failure_async(
                        host,
                        &request_for_outcome,
                        &detail,
                        proposal_exposed_to_pipeline
                            || $force_ambiguous
                            || transport_may_have_applied,
                    )
                    .await;
                    drop(commit_sequence_lock.take());
                    reconciliation?;
                    return Err(error);
                }
            }
        }};
    }
    let release_for_quorum = host
        .release_cluster_raft_command_session_for_quorum_async()
        .await;
    post_append_try_async!(release_for_quorum, "pre-quorum storage release", false);
    // The durable pending-config and same-term barrier fences protect Raft
    // semantics after release; no command class needs to retain the global
    // proposal queue across quorum transport.
    drop(commit_sequence_lock.take());
    proposal_exposed_to_pipeline = true;

    let mut voter_acks = RaftNodeIdSet::default();
    let mut fanout_node_ids = RaftNodeIdSet::default();
    let mut fanout_targets = Vec::new();
    let mut deferred_replication_statuses = Vec::new();
    if cluster_kv_raft_local_is_voter(state) {
        let local_ack = voter_acks
            .try_insert_clone(&state.local_node_id, "record local async raft commit ack")
            .map_err(<H as ClusterKvRaftQuorumCommitAsyncHost>::kv_error);
        post_append_try_async!(local_ack, "local quorum accounting", false);
    }
    let quorum_size = cluster_kv_raft_quorum_size_for_state(state);
    let quorum_voters = post_append_try_async!(
        cluster_kv_raft_voter_ids(state).map_err(H::kv_error),
        "capture grouped commit voter fence",
        true
    );
    let mut matched_prefixes = Vec::new();
    post_append_try_async!(
        try_reserve_kv_items(
            &mut matched_prefixes,
            quorum_voters.len(),
            "capture quorum matched prefixes"
        )
        .map_err(H::kv_error),
        "grouped commit acknowledgement reservation",
        true
    );
    if cluster_kv_raft_local_is_voter(state) {
        // The storage owner caps this watermark at its durable local log.
        matched_prefixes.push(u64::MAX);
    }

    let mut failures = Vec::new();
    let mut higher_term_observed: Option<u64> = None;
    // One absolute quorum deadline covers target selection and transport.
    // Dynamic target providers can touch storage or routing state; giving the
    // replication batch a fresh timeout after that await silently lets one
    // proposal consume two complete budgets.
    let deadline = Instant::now() + options.timeout;
    let quorum_metrics = KvCommandPhaseMetricGuard::start("quorum");
    let discovery_budget = deadline
        .checked_duration_since(Instant::now())
        .unwrap_or_default();
    let targets_result = if discovery_budget.is_zero() {
        Err(H::kv_error(KvError::AuthorityUnavailable {
            detail: format!(
                "remote voter discovery exhausted the {}ms Raft quorum budget",
                options.timeout.as_millis()
            ),
        }))
    } else {
        match tokio::time::timeout(discovery_budget, host.remote_voter_targets_async(state)).await {
            Ok(result) => result,
            Err(_) => Err(H::kv_error(KvError::AuthorityUnavailable {
                detail: format!(
                    "remote voter discovery exhausted the {}ms Raft quorum budget",
                    options.timeout.as_millis()
                ),
            })),
        }
    };
    let targets = post_append_try_async!(targets_result, "remote voter discovery", false);
    let mut replication_node_ids = RaftNodeIdSet::default();
    let mut replication_targets = Vec::new();
    for target in &targets {
        let target_node_id = host.target_node_id(target);
        if voter_acks.contains(target_node_id) {
            continue;
        }
        let insert_target = replication_node_ids
            .try_insert_clone(
                target_node_id,
                "dedupe async raft quorum replication target",
            )
            .map_err(<H as ClusterKvRaftQuorumCommitAsyncHost>::kv_error);
        if !post_append_try_async!(insert_target, "replication target accounting", false) {
            continue;
        }
        let reserve_target = try_reserve_kv_items(
            &mut replication_targets,
            1,
            "collect unique async raft quorum replication target",
        )
        .map_err(<H as ClusterKvRaftQuorumCommitAsyncHost>::kv_error);
        post_append_try_async!(reserve_target, "replication target reservation", false);
        replication_targets.push(target.clone());
    }
    let required_remote_acks = quorum_size.saturating_sub(voter_acks.len());
    let replication_budget = deadline
        .checked_duration_since(Instant::now())
        .unwrap_or_default();
    let quorum_transport_may_have_started =
        !replication_targets.is_empty() && !replication_budget.is_zero();
    let attempts = if replication_budget.is_zero() {
        Vec::new()
    } else {
        let attempts_result = host
            .replicate_cluster_raft_to_quorum_async(
                state,
                replication_targets,
                required_remote_acks,
                replication_budget,
            )
            .await;
        post_append_try_async!(
            attempts_result,
            "quorum append transport",
            quorum_transport_may_have_started
        )
    };
    for attempt in attempts {
        let target = attempt.target;
        match attempt.result {
            Ok(response) if response.accepted => {
                transport_may_have_applied = true;
                let reserve_status = try_reserve_kv_items(
                    &mut deferred_replication_statuses,
                    1,
                    "defer accepted async Raft replication status",
                )
                .map_err(<H as ClusterKvRaftQuorumCommitAsyncHost>::kv_error);
                post_append_try_async!(reserve_status, "accepted replication accounting", true);
                deferred_replication_statuses.push((
                    target.clone(),
                    ClusterKvRaftTargetReplicationStatus::Accepted,
                    attempt.latency_ms,
                ));
                let remote_ack = voter_acks
                    .try_insert_clone(
                        host.target_node_id(&target),
                        "record remote async raft commit ack",
                    )
                    .map_err(<H as ClusterKvRaftQuorumCommitAsyncHost>::kv_error);
                let new_ack = post_append_try_async!(remote_ack, "accepted voter accounting", true);
                if new_ack
                    && response.append.success
                    && response.append.term == entry.term
                    && response.append.match_index >= entry.index
                    && quorum_voters.contains(host.target_node_id(&target))
                {
                    matched_prefixes.push(response.append.match_index);
                }
                let observe = host
                    .observe_accepted_append_state_async(state, &target, response.state)
                    .await;
                post_append_try_async!(observe, "accepted peer-state observation", true);
                let insert_fanout = fanout_node_ids
                    .try_insert_clone(
                        host.target_node_id(&target),
                        "record accepted async raft fanout target",
                    )
                    .map_err(<H as ClusterKvRaftQuorumCommitAsyncHost>::kv_error);
                if post_append_try_async!(insert_fanout, "accepted fanout accounting", true) {
                    let reserve_fanout = try_reserve_kv_items(
                        &mut fanout_targets,
                        1,
                        "record accepted async raft fanout target",
                    )
                    .map_err(<H as ClusterKvRaftQuorumCommitAsyncHost>::kv_error);
                    post_append_try_async!(reserve_fanout, "accepted fanout reservation", true);
                    fanout_targets.push(target);
                }
            }
            Ok(response) => {
                if response.append.term > entry.term {
                    let publish =
                        host.publish_cluster_raft_higher_term(state, response.append.term);
                    post_append_try_async!(publish, "publish rejected async peer term floor", true);
                    higher_term_observed = Some(
                        higher_term_observed
                            .unwrap_or_default()
                            .max(response.append.term),
                    );
                }
                let reserve_status = try_reserve_kv_items(
                    &mut deferred_replication_statuses,
                    1,
                    "defer rejected async Raft replication status",
                )
                .map_err(<H as ClusterKvRaftQuorumCommitAsyncHost>::kv_error);
                post_append_try_async!(reserve_status, "rejected replication accounting", false);
                deferred_replication_statuses.push((
                    target.clone(),
                    ClusterKvRaftTargetReplicationStatus::Rejected,
                    attempt.latency_ms,
                ));
                failures.push(format!(
                    "{} rejected append: {} (term {}, match_index {})",
                    host.target_description(&target),
                    response.detail,
                    response.append.term,
                    response.append.match_index
                ));
            }
            Err(error) => {
                // The batch host may report a per-target error after the
                // remote side has accepted the append.
                transport_may_have_applied = true;
                let reserve_status = try_reserve_kv_items(
                    &mut deferred_replication_statuses,
                    1,
                    "defer failed async Raft replication status",
                )
                .map_err(<H as ClusterKvRaftQuorumCommitAsyncHost>::kv_error);
                post_append_try_async!(reserve_status, "failed replication accounting", true);
                deferred_replication_statuses.push((
                    target.clone(),
                    ClusterKvRaftTargetReplicationStatus::Failed,
                    attempt.latency_ms,
                ));
                failures.push(format!(
                    "{} failed: {}",
                    host.target_description(&target),
                    error.to_string()
                ));
            }
        }
    }
    if let Some(term) = higher_term_observed {
        // This is the first await after the synchronous floor publication.
        // Production hosts supervise the durable closer independently of this
        // caller, so cancellation cannot discard the observed term.
        let observe_term = host.observe_cluster_raft_term_async(state, term).await;
        post_append_try_async!(observe_term, "rejected peer term observation", true);
    }
    // Peer-state observation can touch durable per-target progress and may
    // await storage.  Keep it outside the global proposal sequence just like
    // the quorum transport itself; error paths above reacquire before
    // reconciliation, while the successful path reacquires once all remote
    // observations have completed.
    reacquire_commit_sequence_async!();
    drop(quorum_metrics);
    metrics::histogram!("sinomo.kv.quorum.acks", "kind" => "received")
        .record(voter_acks.len() as f64);
    metrics::histogram!("sinomo.kv.quorum.acks", "kind" => "required").record(quorum_size as f64);

    if voter_acks.len() < quorum_size && Instant::now() >= deadline {
        failures.push(format!(
            "raft command did not reach quorum before {}ms replication budget",
            options.timeout.as_millis()
        ));
    }

    if voter_acks.len() < quorum_size || higher_term_observed.is_some() {
        // Never let an exact committed-prefix replay suppress a higher-term
        // Raft response learned by this quorum wave. The term was observed
        // above; reconcile the request before failure handling now.
        return_pipeline_replay_if_committed_async!();
        let detail = format!(
            "raft command at index {} replicated to {} voters, {} required{}{}",
            entry.index,
            voter_acks.len(),
            quorum_size,
            higher_term_observed
                .map(|term| format!("; observed higher term {term}"))
                .unwrap_or_default(),
            if failures.is_empty() {
                String::new()
            } else {
                format!(": {}", failures.join("; "))
            }
        );
        let reason = if higher_term_observed.is_some() {
            command_metrics.finish("higher_term");
            KvAuthorityFailureReason::HigherTermObserved
        } else if Instant::now() >= deadline {
            command_metrics.finish("timeout");
            KvAuthorityFailureReason::Timeout
        } else {
            command_metrics.finish("no_quorum");
            KvAuthorityFailureReason::NoQuorum
        };
        let reconciliation = reconcile_cluster_raft_post_append_failure_async(
            host,
            &request_for_outcome,
            &detail,
            true,
        )
        .await;
        let failure = cluster_kv_authority_failure(
            state,
            reason,
            detail,
            entry.term,
            KvRevision::ZERO,
            voter_acks.len(),
            Some(state.local_node_id.clone()),
        );
        drop(commit_sequence_lock.take());
        reconciliation?;
        for (target, status, latency_ms) in deferred_replication_statuses {
            let _ = host
                .record_target_replication_status_async(state, &target, status, latency_ms)
                .await;
        }
        return Err(host.cluster_authority_error(failure));
    }

    if proposal_exposed_to_pipeline {
        return_pipeline_replay_if_committed_async!();
    }

    matched_prefixes.sort_unstable_by(|left, right| right.cmp(left));
    let acknowledged_index = matched_prefixes
        .get(quorum_size.saturating_sub(1))
        .copied()
        .unwrap_or(entry.index)
        .max(entry.index);
    let commit_result = {
        let _phase = KvCommandPhaseMetricGuard::start("local_apply");
        host.commit_cluster_raft_acknowledged_prefix_compact_async(entry.index, acknowledged_index)
            .await
    };
    let commit = post_append_try_async!(commit_result, "local apply after quorum", true);
    let authority = KvAuthorityProof {
        term: entry.term,
        commit_index: entry.index,
        revision: commit.committed_request.revision,
        basis: KvAuthorityBasis::CurrentTermQuorum {
            voter_acks: voter_acks.len(),
            required_quorum: quorum_size,
        },
    };
    let applied_authority =
        (commit.read_state.commit_index > entry.index).then(|| KvAuthorityProof {
            term: entry.term,
            commit_index: commit.read_state.commit_index,
            revision: commit.read_state.revision,
            basis: KvAuthorityBasis::CurrentTermQuorum {
                voter_acks: matched_prefixes
                    .iter()
                    .filter(|index| **index >= commit.read_state.commit_index)
                    .count(),
                required_quorum: quorum_size,
            },
        });
    if let Some(receipt) = commit.observation_receipt.as_ref() {
        receipt.observe_original_current_term_commits(
            &state.local_node_id,
            &request_for_outcome,
            &commit.committed_request,
            &entry,
            applied_authority.unwrap_or(authority),
        );
    }
    command_metrics.finish("committed");
    if let Err(error) = host
        .release_cluster_raft_command_session_after_commit_async()
        .await
    {
        report_cluster_kv_post_commit_fault("commit session release", error.to_string());
    }
    // Match the synchronous driver: only the exact durable commit stays in the
    // proposal critical section. Fanout is safe after local apply and release
    // of the resident operation guard, and must not cause head-of-line
    // blocking for the next proposal.
    drop(commit_sequence_lock.take());
    let publication_failure = match host
        .publish_cluster_raft_post_commit_observations_async(Some(&commit))
        .await
    {
        Ok(()) => None,
        Err(error) => {
            report_cluster_kv_post_commit_fault("commit publication", error.to_string());
            H::is_committed_observation_delivery_failure(&error).then_some(error)
        }
    };
    for (target, status, latency_ms) in deferred_replication_statuses {
        let _ = host
            .record_target_replication_status_async(state, &target, status, latency_ms)
            .await;
    }
    for target in targets {
        let inserted = match fanout_node_ids.try_insert_clone(
            host.target_node_id(&target),
            "dedupe committed async raft fanout target",
        ) {
            Ok(inserted) => inserted,
            Err(error) => {
                report_cluster_kv_post_commit_fault("fanout target deduplication", error);
                continue;
            }
        };
        if inserted {
            if let Err(error) = try_reserve_kv_items(
                &mut fanout_targets,
                1,
                "dedupe committed async raft fanout target",
            ) {
                report_cluster_kv_post_commit_fault("fanout target allocation", error);
                continue;
            }
            fanout_targets.push(target);
        }
    }
    {
        let _phase = KvCommandPhaseMetricGuard::start("fanout_dispatch");
        if let Err(error) = host
            .fan_out_committed_cluster_raft_command_async(
                state.clone(),
                commit.events.clone(),
                fanout_targets,
            )
            .await
        {
            report_cluster_kv_post_commit_fault("fanout dispatch", error.to_string());
        }
    }
    if let Some(error) = publication_failure {
        return Err(error);
    }
    Ok(ClusterKvRaftQuorumCompactCommitResult {
        applied_authority,
        txn_response: commit.txn_response,
        events: (commit.events).into(),
        compacted_revision: commit.compacted_revision,
        checkpoint_snapshot: (commit.checkpoint_snapshot).map(Into::into),
        final_key_states: (commit.final_key_states).into(),
        authority,
        replayed_committed_request: false,
    })
}

/// Establishes a fresh current-term quorum barrier and returns the snapshot at
/// that barrier. A new request id is intentionally generated for every call so
/// an old committed barrier can never be mistaken for current authority.
pub fn read_cluster_kv_authoritative_snapshot_with_host<H>(
    state: &mut ClusterBootstrapState,
    options: ClusterKvRaftQuorumCommitOptions,
    host: &mut H,
) -> Result<KvAuthoritativeRead<KvSnapshot>, H::Error>
where
    H: ClusterKvRaftQuorumCommitHost,
    H::Error: ToString,
{
    let result = commit_cluster_kv_raft_command_with_quorum(
        state,
        KvCommandRequest::generated(KvCommand::Barrier),
        options,
        host,
    )?;
    Ok(KvAuthoritativeRead {
        value: result.snapshot,
        authority: result.authority,
    })
}

pub async fn read_cluster_kv_authoritative_snapshot_async_with_host<H>(
    state: &mut ClusterBootstrapState,
    options: ClusterKvRaftQuorumCommitOptions,
    host: &mut H,
) -> Result<KvAuthoritativeRead<KvSnapshot>, H::Error>
where
    H: ClusterKvRaftQuorumCommitAsyncHost,
{
    let result = commit_cluster_kv_raft_command_with_quorum_async(
        state,
        KvCommandRequest::generated(KvCommand::Barrier),
        options,
        host,
    )
    .await?;
    Ok(KvAuthoritativeRead {
        value: result.snapshot,
        authority: result.authority,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterKvTxnQuorumDriverOptions {
    pub allow_forwarding: bool,
}

const CLUSTER_KV_TXN_QUORUM_COMMIT_MAX_ATTEMPTS: usize = 2;

fn invalid_forwarded_cluster_kv_commit(detail: impl Into<String>) -> KvError {
    KvError::AuthorityUnavailable {
        detail: detail.into(),
    }
}

pub(crate) fn validate_cluster_kv_txn_commit_response(
    state: &ClusterBootstrapState,
    request: &KvTxnRequest,
    response: &ClusterKvTxnCommitResult,
) -> Result<(), KvError> {
    if response.request_id != request.request_id {
        return Err(invalid_forwarded_cluster_kv_commit(format!(
            "cluster KV commit response request ID {} does not match submitted request ID {}",
            response.request_id, request.request_id
        )));
    }
    response.snapshot.validate_resource_bounds()?;
    let txn_response = response.txn_response.as_ref().ok_or_else(|| {
        invalid_forwarded_cluster_kv_commit(
            "cluster KV transaction commit response omitted its transaction result",
        )
    })?;
    if response.authority.term == 0 || response.authority.commit_index == 0 {
        return Err(invalid_forwarded_cluster_kv_commit(
            "cluster KV commit response has an empty authority term or commit index",
        ));
    }
    match response.authority.basis {
        KvAuthorityBasis::CurrentTermQuorum {
            voter_acks,
            required_quorum,
        } => {
            let voter_count = cluster_kv_raft_voter_ids(state)?.len();
            let expected_quorum = cluster_kv_raft_quorum_size(voter_count);
            if response.replayed_committed_request {
                return Err(invalid_forwarded_cluster_kv_commit(
                    "fresh-quorum commit response was marked as an exact committed replay",
                ));
            }
            if response.authority.term < state.term {
                return Err(invalid_forwarded_cluster_kv_commit(format!(
                    "cluster KV commit response term {} is older than observed term {}",
                    response.authority.term, state.term
                )));
            }
            if required_quorum != expected_quorum
                || voter_acks < expected_quorum
                || voter_acks > voter_count
            {
                return Err(invalid_forwarded_cluster_kv_commit(format!(
                    "cluster KV commit response reported {voter_acks} voter acknowledgements and quorum {required_quorum}, but membership has {voter_count} voters and requires {expected_quorum}"
                )));
            }
            if response.authority.revision != response.snapshot.revision {
                return Err(invalid_forwarded_cluster_kv_commit(format!(
                    "cluster KV commit proof revision {} does not match returned snapshot revision {}",
                    response.authority.revision.0, response.snapshot.revision.0
                )));
            }
            if txn_response.revision != response.authority.revision {
                return Err(invalid_forwarded_cluster_kv_commit(format!(
                    "cluster KV transaction result revision {} does not match authority revision {}",
                    txn_response.revision.0, response.authority.revision.0
                )));
            }
        }
        KvAuthorityBasis::ExactCommittedRequest => {
            if !response.replayed_committed_request {
                return Err(invalid_forwarded_cluster_kv_commit(
                    "exact committed-request authority was not marked as a replay",
                ));
            }
            let record = response
                .snapshot
                .committed_requests
                .iter()
                .find(|record| record.request_id == request.request_id)
                .ok_or_else(|| {
                    invalid_forwarded_cluster_kv_commit(format!(
                        "exact committed-request response omitted durable record {}",
                        request.request_id
                    ))
                })?;
            if record.command_digest != response.command_digest
                || record.term != response.authority.term
                || record.log_index != response.authority.commit_index
                || record.revision != response.authority.revision
                || record.txn_response != response.txn_response
                || response.snapshot.revision < record.revision
            {
                return Err(invalid_forwarded_cluster_kv_commit(format!(
                    "exact committed-request proof for {} does not match its durable snapshot record",
                    request.request_id
                )));
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_cluster_kv_txn_compact_commit_response(
    state: &ClusterBootstrapState,
    request: &KvTxnRequest,
    response: &ClusterKvTxnCompactCommitResult,
) -> Result<(), KvError> {
    if response.request_id != request.request_id {
        return Err(invalid_forwarded_cluster_kv_commit(
            "compact cluster KV response request identity does not match the request",
        ));
    }
    let submitted_command_digest = request.clone().into_command_request().digest()?;
    if response
        .submitted_command_digest
        .is_some_and(|digest| digest != submitted_command_digest)
    {
        return Err(invalid_forwarded_cluster_kv_commit(
            "compact cluster KV response digest does not match the submitted transaction",
        ));
    }
    let txn_response = response.txn_response.as_ref().ok_or_else(|| {
        invalid_forwarded_cluster_kv_commit(
            "compact cluster KV transaction response omitted its transaction result",
        )
    })?;
    if response.authority.term == 0
        || response.authority.commit_index == 0
        || txn_response.revision != response.authority.revision
    {
        return Err(invalid_forwarded_cluster_kv_commit(
            "compact cluster KV transaction response carries an invalid authority proof",
        ));
    }
    match response.authority.basis {
        KvAuthorityBasis::CurrentTermQuorum {
            voter_acks,
            required_quorum,
        } => {
            let voter_count = cluster_kv_raft_voter_ids(state)?.len();
            let expected_quorum = cluster_kv_raft_quorum_size(voter_count);
            if response.replayed_committed_request
                || response.authority.term < state.term
                || required_quorum != expected_quorum
                || voter_acks < expected_quorum
                || voter_acks > voter_count
            {
                return Err(invalid_forwarded_cluster_kv_commit(
                    "compact cluster KV quorum proof does not match current voter membership",
                ));
            }
        }
        KvAuthorityBasis::ExactCommittedRequest => {
            if !response.replayed_committed_request {
                return Err(invalid_forwarded_cluster_kv_commit(
                    "compact exact-request proof was not marked as a replay",
                ));
            }
        }
    }
    Ok(())
}

pub trait ClusterKvTxnQuorumHost {
    type Error: ToString;

    /// Wraps a KV store error in the host's error type. Replaces the old
    /// `From<KvError>` bound so host error types in other crates do not need
    /// orphan-rule-breaking `From` impls.
    fn kv_error(error: KvError) -> Self::Error;

    fn refresh_current_cluster_kv_store(
        &mut self,
        _state: &ClusterBootstrapState,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn local_is_raft_leader(&mut self, state: &ClusterBootstrapState) -> Result<bool, Self::Error>;

    fn forward_cluster_kv_txn_to_voter(
        &mut self,
        state: &ClusterBootstrapState,
        request: &KvTxnRequest,
    ) -> Result<Option<ClusterKvTxnCommitResult>, Self::Error>;

    fn elect_cluster_raft_leader(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<bool, Self::Error>;

    fn reachable_voter_count(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<usize, Self::Error>;

    fn commit_cluster_raft_txn_with_quorum(
        &mut self,
        state: &mut ClusterBootstrapState,
        request: KvTxnRequest,
    ) -> Result<ClusterKvRaftQuorumCommitResult, Self::Error>;

    fn cluster_quorum_commit_error_allows_retry(&mut self, _error: &Self::Error) -> bool {
        false
    }

    fn cluster_authority_error(&mut self, failure: KvAuthorityFailure) -> Self::Error;
}

pub trait ClusterKvTxnQuorumAsyncHost: Send {
    type Error: ToString;

    /// Wraps a KV store error in the host's error type. Replaces the old
    /// `From<KvError>` bound so host error types in other crates do not need
    /// orphan-rule-breaking `From` impls.
    fn kv_error(error: KvError) -> Self::Error;

    fn refresh_current_cluster_kv_store_async<'a>(
        &'a mut self,
        _state: &'a ClusterBootstrapState,
    ) -> ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        Box::pin(async { Ok(()) })
    }

    fn local_is_raft_leader_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> ClusterKvRaftQuorumFuture<'a, bool, Self::Error>;

    fn forward_cluster_kv_txn_to_voter_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        request: &'a KvTxnRequest,
    ) -> ClusterKvRaftQuorumFuture<'a, Option<ClusterKvTxnCommitResult>, Self::Error>;

    fn forward_cluster_kv_txn_compact_to_voter_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        request: &'a KvTxnRequest,
    ) -> ClusterKvRaftQuorumFuture<'a, Option<ClusterKvTxnCompactCommitResult>, Self::Error> {
        Box::pin(async move {
            let submitted_command_digest = request
                .clone()
                .into_command_request()
                .digest()
                .map_err(Self::kv_error)?;
            self.forward_cluster_kv_txn_to_voter_async(state, request)
                .await
                .map(|response| {
                    response.map(|response| ClusterKvTxnCompactCommitResult {
                        request_id: response.request_id,
                        submitted_command_digest: Some(submitted_command_digest),
                        command_digest: response.command_digest,
                        txn_response: response.txn_response,
                        authority: response.authority,
                        replayed_committed_request: response.replayed_committed_request,
                    })
                })
        })
    }

    fn elect_cluster_raft_leader_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> ClusterKvRaftQuorumFuture<'a, bool, Self::Error>;

    fn reachable_voter_count_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> ClusterKvRaftQuorumFuture<'a, usize, Self::Error>;

    fn commit_cluster_raft_txn_with_quorum_async<'a>(
        &'a mut self,
        state: &'a mut ClusterBootstrapState,
        request: KvTxnRequest,
    ) -> ClusterKvRaftQuorumFuture<'a, ClusterKvRaftQuorumCommitResult, Self::Error>;

    fn commit_cluster_raft_txn_compact_with_quorum_async<'a>(
        &'a mut self,
        state: &'a mut ClusterBootstrapState,
        request: KvTxnRequest,
    ) -> ClusterKvRaftQuorumFuture<'a, ClusterKvRaftQuorumCompactCommitResult, Self::Error> {
        Box::pin(async move {
            self.commit_cluster_raft_txn_with_quorum_async(state, request)
                .await
                .map(|commit| ClusterKvRaftQuorumCompactCommitResult {
                    applied_authority: None,
                    txn_response: commit.txn_response,
                    events: (Vec::new()).into(),
                    compacted_revision: commit.snapshot.compacted_revision,
                    checkpoint_snapshot: (Some(commit.snapshot)).map(Into::into),
                    final_key_states: (Vec::new()).into(),
                    authority: commit.authority,
                    replayed_committed_request: commit.replayed_committed_request,
                })
        })
    }

    fn cluster_quorum_commit_error_allows_retry(&mut self, _error: &Self::Error) -> bool {
        false
    }

    fn cluster_authority_error(&mut self, failure: KvAuthorityFailure) -> Self::Error;
}

pub fn commit_cluster_kv_txn_with_quorum_with_host<H>(
    state: &mut ClusterBootstrapState,
    request: KvTxnRequest,
    options: ClusterKvTxnQuorumDriverOptions,
    host: &mut H,
) -> Result<ClusterKvTxnCommitResult, H::Error>
where
    H: ClusterKvTxnQuorumHost,
{
    let _ = host.refresh_current_cluster_kv_store(state);
    if !cluster_kv_raft_local_is_voter(state) {
        if options.allow_forwarding
            && let Some(response) = host.forward_cluster_kv_txn_to_voter(state, &request)?
        {
            validate_cluster_kv_txn_commit_response(state, &request, &response)
                .map_err(H::kv_error)?;
            return Ok(response);
        }
        let reachable_voters = host.reachable_voter_count(state)?;
        let detail = format!(
            "local node is not a Raft voter and could not forward to a voter ({} reachable voters, {} required)",
            reachable_voters,
            cluster_kv_raft_quorum_size_for_state(state)
        );
        return Err(host.cluster_authority_error(cluster_kv_authority_failure(
            state,
            KvAuthorityFailureReason::LocalNodeNotVoter,
            detail,
            0,
            KvRevision::ZERO,
            reachable_voters,
            None,
        )));
    }

    let mut previous_commit_failure: Option<H::Error> = None;
    for attempt in 0..CLUSTER_KV_TXN_QUORUM_COMMIT_MAX_ATTEMPTS {
        if !host.local_is_raft_leader(state)? {
            if !options.allow_forwarding {
                let reachable_voters = host.reachable_voter_count(state)?;
                return Err(host.cluster_authority_error(cluster_kv_authority_failure(
                    state,
                    KvAuthorityFailureReason::NoQuorum,
                    "forwarded cluster KV transaction reached a non-leader voter; forwarded requests do not start elections".to_string(),
                    0,
                    KvRevision::ZERO,
                    reachable_voters,
                    None,
                )));
            }
            let forwarding_failure = match host.forward_cluster_kv_txn_to_voter(state, &request) {
                Ok(Some(response)) => {
                    validate_cluster_kv_txn_commit_response(state, &request, &response)
                        .map_err(H::kv_error)?;
                    return Ok(response);
                }
                Ok(None) => None,
                Err(error) => Some(error),
            };
            if !host.elect_cluster_raft_leader(state)? {
                let mut detail = if let Some(forwarding_failure) = forwarding_failure {
                    format!(
                        "local node is not the Raft leader and could not win quorum; forwarding also failed: {}",
                        forwarding_failure.to_string()
                    )
                } else {
                    "local node is not the Raft leader and could not win quorum".to_string()
                };
                if let Some(error) = previous_commit_failure.as_ref() {
                    detail.push_str("; previous local commit failed: ");
                    detail.push_str(&error.to_string());
                }
                let reachable_voters = host.reachable_voter_count(state)?;
                return Err(host.cluster_authority_error(cluster_kv_authority_failure(
                    state,
                    KvAuthorityFailureReason::NoQuorum,
                    detail,
                    0,
                    KvRevision::ZERO,
                    reachable_voters,
                    None,
                )));
            }
        }
        if !host.local_is_raft_leader(state)? {
            let mut detail =
                "local node is not the Raft leader and could not win quorum".to_string();
            if let Some(error) = previous_commit_failure.as_ref() {
                detail.push_str("; previous local commit failed: ");
                detail.push_str(&error.to_string());
            }
            let reachable_voters = host.reachable_voter_count(state)?;
            return Err(host.cluster_authority_error(cluster_kv_authority_failure(
                state,
                KvAuthorityFailureReason::NoQuorum,
                detail,
                0,
                KvRevision::ZERO,
                reachable_voters,
                None,
            )));
        }
        match host.commit_cluster_raft_txn_with_quorum(state, request.clone()) {
            Ok(commit) => {
                return Ok(ClusterKvTxnCommitResult {
                    request_id: request.request_id.clone(),
                    command_digest: request
                        .clone()
                        .into_command_request()
                        .digest()
                        .map_err(H::kv_error)?,
                    snapshot: commit.snapshot,
                    txn_response: commit.txn_response,
                    authority: commit.authority,
                    replayed_committed_request: commit.replayed_committed_request,
                });
            }
            Err(error)
                if attempt + 1 < CLUSTER_KV_TXN_QUORUM_COMMIT_MAX_ATTEMPTS
                    && options.allow_forwarding
                    && host.cluster_quorum_commit_error_allows_retry(&error) =>
            {
                previous_commit_failure = Some(error);
            }
            Err(error) => return Err(error),
        }
    }
    let detail = previous_commit_failure
        .as_ref()
        .map(|error| {
            format!(
                "local node could not commit after retrying Raft leadership: {}",
                error.to_string()
            )
        })
        .unwrap_or_else(|| {
            "local node could not commit after retrying Raft leadership".to_string()
        });
    let reachable_voters = host.reachable_voter_count(state)?;
    Err(host.cluster_authority_error(cluster_kv_authority_failure(
        state,
        KvAuthorityFailureReason::NoQuorum,
        detail,
        0,
        KvRevision::ZERO,
        reachable_voters,
        None,
    )))
}

pub async fn commit_cluster_kv_txn_with_quorum_async_with_host<H>(
    state: &mut ClusterBootstrapState,
    request: KvTxnRequest,
    options: ClusterKvTxnQuorumDriverOptions,
    host: &mut H,
) -> Result<ClusterKvTxnCommitResult, H::Error>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    let _ = host.refresh_current_cluster_kv_store_async(state).await;
    if !cluster_kv_raft_local_is_voter(state) {
        if options.allow_forwarding
            && let Some(response) = host
                .forward_cluster_kv_txn_to_voter_async(state, &request)
                .await?
        {
            // A forwarded response carries the full bounded snapshot. Its
            // synchronous structural validation is intentionally kept off the
            // async worker's deeply nested stream/RPC/quorum poll stack. This
            // is a scheduling boundary, not a larger-stack workaround: the
            // validator starts with an ordinary clean blocking-task stack and
            // returns the same owned response only after validation succeeds.
            let validation_state = state.clone();
            let validation_request = request.clone();
            let response = tokio::task::spawn_blocking(move || {
                validate_cluster_kv_txn_commit_response(
                    &validation_state,
                    &validation_request,
                    &response,
                )?;
                Ok::<_, KvError>(response)
            })
            .await
            .map_err(|source| {
                H::kv_error(KvError::Serialization {
                    detail: format!("cluster KV commit response validation task failed: {source}"),
                })
            })?
            .map_err(H::kv_error)?;
            return Ok(response);
        }
        let reachable_voters = host.reachable_voter_count_async(state).await?;
        let detail = format!(
            "local node is not a Raft voter and could not forward to a voter ({} reachable voters, {} required)",
            reachable_voters,
            cluster_kv_raft_quorum_size_for_state(state)
        );
        return Err(host.cluster_authority_error(cluster_kv_authority_failure(
            state,
            KvAuthorityFailureReason::LocalNodeNotVoter,
            detail,
            0,
            KvRevision::ZERO,
            reachable_voters,
            None,
        )));
    }

    let mut previous_commit_failure: Option<H::Error> = None;
    for attempt in 0..CLUSTER_KV_TXN_QUORUM_COMMIT_MAX_ATTEMPTS {
        if !host.local_is_raft_leader_async(state).await? {
            if !options.allow_forwarding {
                let reachable_voters = host.reachable_voter_count_async(state).await?;
                return Err(host.cluster_authority_error(cluster_kv_authority_failure(
                    state,
                    KvAuthorityFailureReason::NoQuorum,
                    "forwarded cluster KV transaction reached a non-leader voter; forwarded requests do not start elections".to_string(),
                    0,
                    KvRevision::ZERO,
                    reachable_voters,
                    None,
                )));
            }
            let forwarding_failure = match host
                .forward_cluster_kv_txn_to_voter_async(state, &request)
                .await
            {
                Ok(Some(response)) => {
                    validate_cluster_kv_txn_commit_response(state, &request, &response)
                        .map_err(H::kv_error)?;
                    return Ok(response);
                }
                Ok(None) => None,
                Err(error) => Some(error),
            };
            if !host.elect_cluster_raft_leader_async(state).await? {
                let mut detail = if let Some(forwarding_failure) = forwarding_failure {
                    format!(
                        "local node is not the Raft leader and could not win quorum; forwarding also failed: {}",
                        forwarding_failure.to_string()
                    )
                } else {
                    "local node is not the Raft leader and could not win quorum".to_string()
                };
                if let Some(error) = previous_commit_failure.as_ref() {
                    detail.push_str("; previous local commit failed: ");
                    detail.push_str(&error.to_string());
                }
                let reachable_voters = host.reachable_voter_count_async(state).await?;
                return Err(host.cluster_authority_error(cluster_kv_authority_failure(
                    state,
                    KvAuthorityFailureReason::NoQuorum,
                    detail,
                    0,
                    KvRevision::ZERO,
                    reachable_voters,
                    None,
                )));
            }
        }
        if !host.local_is_raft_leader_async(state).await? {
            let mut detail =
                "local node is not the Raft leader and could not win quorum".to_string();
            if let Some(error) = previous_commit_failure.as_ref() {
                detail.push_str("; previous local commit failed: ");
                detail.push_str(&error.to_string());
            }
            let reachable_voters = host.reachable_voter_count_async(state).await?;
            return Err(host.cluster_authority_error(cluster_kv_authority_failure(
                state,
                KvAuthorityFailureReason::NoQuorum,
                detail,
                0,
                KvRevision::ZERO,
                reachable_voters,
                None,
            )));
        }
        match host
            .commit_cluster_raft_txn_with_quorum_async(state, request.clone())
            .await
        {
            Ok(commit) => {
                return Ok(ClusterKvTxnCommitResult {
                    request_id: request.request_id.clone(),
                    command_digest: request
                        .clone()
                        .into_command_request()
                        .digest()
                        .map_err(H::kv_error)?,
                    snapshot: commit.snapshot,
                    txn_response: commit.txn_response,
                    authority: commit.authority,
                    replayed_committed_request: commit.replayed_committed_request,
                });
            }
            Err(error)
                if attempt + 1 < CLUSTER_KV_TXN_QUORUM_COMMIT_MAX_ATTEMPTS
                    && options.allow_forwarding
                    && host.cluster_quorum_commit_error_allows_retry(&error) =>
            {
                previous_commit_failure = Some(error);
            }
            Err(error) => return Err(error),
        }
    }
    let detail = previous_commit_failure
        .as_ref()
        .map(|error| {
            format!(
                "local node could not commit after retrying Raft leadership: {}",
                error.to_string()
            )
        })
        .unwrap_or_else(|| {
            "local node could not commit after retrying Raft leadership".to_string()
        });
    let reachable_voters = host.reachable_voter_count_async(state).await?;
    Err(host.cluster_authority_error(cluster_kv_authority_failure(
        state,
        KvAuthorityFailureReason::NoQuorum,
        detail,
        0,
        KvRevision::ZERO,
        reachable_voters,
        None,
    )))
}

/// Compact transaction driver used by daemon mutation/RPC paths. It preserves
/// the existing forwarding, election, retry, and authority checks while
/// keeping both local and forwarded success results independent of keyspace
/// size.
pub async fn commit_cluster_kv_txn_compact_with_quorum_async_with_host<H>(
    state: &mut ClusterBootstrapState,
    request: KvTxnRequest,
    options: ClusterKvTxnQuorumDriverOptions,
    host: &mut H,
) -> Result<ClusterKvTxnCompactCommitResult, H::Error>
where
    H: ClusterKvTxnQuorumAsyncHost,
{
    let _ = host.refresh_current_cluster_kv_store_async(state).await;
    if !cluster_kv_raft_local_is_voter(state) {
        if options.allow_forwarding
            && let Some(response) = host
                .forward_cluster_kv_txn_compact_to_voter_async(state, &request)
                .await?
        {
            validate_cluster_kv_txn_compact_commit_response(state, &request, &response)
                .map_err(H::kv_error)?;
            return Ok(response);
        }
        let reachable_voters = host.reachable_voter_count_async(state).await?;
        return Err(host.cluster_authority_error(cluster_kv_authority_failure(
            state,
            KvAuthorityFailureReason::LocalNodeNotVoter,
            format!(
                "local node is not a Raft voter and could not forward to a voter ({} reachable voters, {} required)",
                reachable_voters,
                cluster_kv_raft_quorum_size_for_state(state)
            ),
            0,
            KvRevision::ZERO,
            reachable_voters,
            None,
        )));
    }

    let mut previous_commit_failure: Option<H::Error> = None;
    for attempt in 0..CLUSTER_KV_TXN_QUORUM_COMMIT_MAX_ATTEMPTS {
        if !host.local_is_raft_leader_async(state).await? {
            if !options.allow_forwarding {
                let reachable_voters = host.reachable_voter_count_async(state).await?;
                return Err(host.cluster_authority_error(cluster_kv_authority_failure(
                    state,
                    KvAuthorityFailureReason::NoQuorum,
                    "forwarded cluster KV transaction reached a non-leader voter; forwarded requests do not start elections".to_string(),
                    0,
                    KvRevision::ZERO,
                    reachable_voters,
                    None,
                )));
            }
            let forwarding_failure = match host
                .forward_cluster_kv_txn_compact_to_voter_async(state, &request)
                .await
            {
                Ok(Some(response)) => {
                    validate_cluster_kv_txn_compact_commit_response(state, &request, &response)
                        .map_err(H::kv_error)?;
                    return Ok(response);
                }
                Ok(None) => None,
                Err(error) => Some(error),
            };
            if !host.elect_cluster_raft_leader_async(state).await? {
                let mut detail = forwarding_failure.map_or_else(
                    || "local node is not the Raft leader and could not win quorum".to_string(),
                    |error| {
                        format!(
                            "local node is not the Raft leader and could not win quorum; forwarding also failed: {}",
                            error.to_string()
                        )
                    },
                );
                if let Some(error) = previous_commit_failure.as_ref() {
                    detail.push_str("; previous local commit failed: ");
                    detail.push_str(&error.to_string());
                }
                let reachable_voters = host.reachable_voter_count_async(state).await?;
                return Err(host.cluster_authority_error(cluster_kv_authority_failure(
                    state,
                    KvAuthorityFailureReason::NoQuorum,
                    detail,
                    0,
                    KvRevision::ZERO,
                    reachable_voters,
                    None,
                )));
            }
        }
        if !host.local_is_raft_leader_async(state).await? {
            let reachable_voters = host.reachable_voter_count_async(state).await?;
            return Err(host.cluster_authority_error(cluster_kv_authority_failure(
                state,
                KvAuthorityFailureReason::NoQuorum,
                "local node is not the Raft leader after election".to_string(),
                0,
                KvRevision::ZERO,
                reachable_voters,
                None,
            )));
        }
        match host
            .commit_cluster_raft_txn_compact_with_quorum_async(state, request.clone())
            .await
        {
            Ok(commit) => {
                return Ok(ClusterKvTxnCompactCommitResult {
                    request_id: request.request_id.clone(),
                    submitted_command_digest: Some(
                        request
                            .clone()
                            .into_command_request()
                            .digest()
                            .map_err(H::kv_error)?,
                    ),
                    command_digest: request
                        .clone()
                        .into_command_request()
                        .digest()
                        .map_err(H::kv_error)?,
                    txn_response: commit.txn_response,
                    authority: commit.authority,
                    replayed_committed_request: commit.replayed_committed_request,
                });
            }
            Err(error)
                if attempt + 1 < CLUSTER_KV_TXN_QUORUM_COMMIT_MAX_ATTEMPTS
                    && options.allow_forwarding
                    && host.cluster_quorum_commit_error_allows_retry(&error) =>
            {
                previous_commit_failure = Some(error);
            }
            Err(error) => return Err(error),
        }
    }
    let reachable_voters = host.reachable_voter_count_async(state).await?;
    Err(host.cluster_authority_error(cluster_kv_authority_failure(
        state,
        KvAuthorityFailureReason::NoQuorum,
        previous_commit_failure.map_or_else(
            || "local node could not commit after retrying Raft leadership".to_string(),
            |error| {
                format!(
                    "local node could not commit after retrying Raft leadership: {}",
                    error.to_string()
                )
            },
        ),
        0,
        KvRevision::ZERO,
        reachable_voters,
        None,
    )))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterKvRaftCompactionPolicy {
    pub max_snapshot_bytes: u64,
    pub max_snapshot_events: usize,
}

pub fn default_cluster_kv_raft_compaction_policy() -> ClusterKvRaftCompactionPolicy {
    ClusterKvRaftCompactionPolicy {
        max_snapshot_bytes: cluster_kv_raft_snapshot_byte_limit(),
        max_snapshot_events: MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS,
    }
}

pub fn cluster_kv_raft_snapshot_byte_limit() -> u64 {
    #[cfg(test)]
    {
        CLUSTER_KV_RAFT_SNAPSHOT_BYTES_LIMIT_FOR_TEST.load(Ordering::Relaxed)
    }
    #[cfg(not(test))]
    {
        MAX_CLUSTER_KV_RAFT_SNAPSHOT_BYTES
    }
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn set_cluster_kv_raft_snapshot_byte_limit_for_test(limit: u64) -> u64 {
    CLUSTER_KV_RAFT_SNAPSHOT_BYTES_LIMIT_FOR_TEST.swap(limit, Ordering::Relaxed)
}

pub fn cluster_kv_raft_node_should_compact(
    node: &DurableRaftKvNode,
    snapshot_bytes: u64,
    policy: ClusterKvRaftCompactionPolicy,
) -> bool {
    // Snapshotting rewrites the entire WAL down to the retained tail plus the
    // snapshot file, so it must only run when it makes durable progress.
    //
    // Compaction advances the on-disk snapshot to `applied_index` and drops log
    // entries older than the retained tail. The durable progress is
    // `applied_index - snapshot_index`; the in-memory log length and event
    // history are not valid progress triggers because they include the retained
    // replication tail.
    let compaction_high_watermark = RAFT_COMPACTION_RETAINED_ENTRIES.saturating_mul(2);
    let unsnapshotted_applied = node.applied_index().saturating_sub(node.snapshot_index());
    let has_compactable_progress = unsnapshotted_applied > compaction_high_watermark;
    let oversized_snapshot_can_shrink = snapshot_bytes > policy.max_snapshot_bytes
        && node.store().event_history_len() > policy.max_snapshot_events;
    has_compactable_progress || oversized_snapshot_can_shrink
}

pub fn compact_cluster_kv_raft_node_if_needed(
    node: &mut DurableRaftKvNode,
    snapshot_bytes: u64,
    policy: ClusterKvRaftCompactionPolicy,
) -> Result<bool, KvError> {
    compact_cluster_kv_raft_node_with_checkpoint_if_needed(node, snapshot_bytes, policy)
        .map(|snapshot| snapshot.is_some())
}

fn compact_cluster_kv_raft_node_with_checkpoint_if_needed(
    node: &mut DurableRaftKvNode,
    snapshot_bytes: u64,
    policy: ClusterKvRaftCompactionPolicy,
) -> Result<Option<RaftSnapshot>, KvError> {
    if !cluster_kv_raft_node_should_compact(node, snapshot_bytes, policy) {
        return Ok(None);
    }
    let mut metric = KvStorageMetricGuard::start("compaction", "sinomo.kv.compaction.us");
    let log_entries_before = node.log().len();
    node.compact_event_history_to(policy.max_snapshot_events);
    let snapshot = node.save_snapshot()?;
    metrics::counter!("sinomo.kv.compaction.runs", "outcome" => "ok").increment(1);
    metrics::histogram!("sinomo.kv.compaction.removed_log_entries")
        .record(log_entries_before.saturating_sub(node.log().len()) as f64);
    metric.succeed();
    Ok(Some(snapshot))
}

#[derive(Clone, Debug)]
pub struct ClusterKvRaftStore {
    paths: ClusterKvPaths,
    compaction_policy: ClusterKvRaftCompactionPolicy,
}

fn append_entries_request_uses_snapshot(node: &DurableRaftKvNode, next_index: u64) -> bool {
    let prev_log_index = next_index.saturating_sub(1).min(node.last_log_index());
    let earliest_log_index = node.log().first().map(|entry| entry.index);
    let prev_log_resolvable = prev_log_index == node.snapshot_index()
        || earliest_log_index.is_some_and(|first| prev_log_index >= first);
    node.snapshot_index() > 0 && !prev_log_resolvable
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct RaftAppendBuildWorkForTest {
    cloned_log_entries: usize,
    materialized_snapshots: usize,
}

#[cfg(test)]
thread_local! {
    static RAFT_APPEND_BUILD_WORK_FOR_TEST: std::cell::Cell<RaftAppendBuildWorkForTest> =
        std::cell::Cell::new(RaftAppendBuildWorkForTest::default());
}

#[cfg(test)]
fn reset_raft_append_build_work_for_test() {
    RAFT_APPEND_BUILD_WORK_FOR_TEST.set(RaftAppendBuildWorkForTest::default());
}

#[cfg(test)]
fn raft_append_build_work_snapshot_for_test() -> RaftAppendBuildWorkForTest {
    RAFT_APPEND_BUILD_WORK_FOR_TEST.get()
}

#[cfg(test)]
fn record_raft_append_entry_clone_for_test() {
    let mut work = RAFT_APPEND_BUILD_WORK_FOR_TEST.get();
    work.cloned_log_entries = work.cloned_log_entries.saturating_add(1);
    RAFT_APPEND_BUILD_WORK_FOR_TEST.set(work);
}

#[cfg(not(test))]
fn record_raft_append_entry_clone_for_test() {}

#[cfg(test)]
fn record_raft_append_snapshot_materialization_for_test() {
    let mut work = RAFT_APPEND_BUILD_WORK_FOR_TEST.get();
    work.materialized_snapshots = work.materialized_snapshots.saturating_add(1);
    RAFT_APPEND_BUILD_WORK_FOR_TEST.set(work);
}

#[cfg(not(test))]
fn record_raft_append_snapshot_materialization_for_test() {}

fn build_append_entries_request_from_open_node(
    node: &DurableRaftKvNode,
    leader_id: &NodeId,
    next_index: u64,
) -> Result<ClusterKvRaftAppendRequest, KvError> {
    let prev_log_index = next_index.saturating_sub(1).min(node.last_log_index());
    let include_snapshot = append_entries_request_uses_snapshot(node, next_index);
    let (snapshot, prev_log_index, entries) = if include_snapshot {
        record_raft_append_snapshot_materialization_for_test();
        let mut kv = node.store().try_snapshot()?;
        compact_kv_snapshot_event_history(&mut kv, MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS);
        let snapshot = RaftSnapshot {
            last_included_index: node.applied_index(),
            last_included_term: node.term_at(node.applied_index()).unwrap_or(1),
            kv: kv.into(),
        };
        let mut entries = Vec::new();
        for entry in node
            .log()
            .iter()
            .filter(|entry| entry.index > snapshot.last_included_index)
        {
            try_reserve_kv_items(&mut entries, 1, "build raft append snapshot tail")?;
            record_raft_append_entry_clone_for_test();
            entries.push(entry.clone());
        }
        (Some(snapshot), node.applied_index(), entries)
    } else {
        let mut entries = Vec::new();
        for entry in node.log().iter().filter(|entry| entry.index >= next_index) {
            try_reserve_kv_items(&mut entries, 1, "build raft append tail")?;
            record_raft_append_entry_clone_for_test();
            entries.push(entry.clone());
        }
        (None, prev_log_index, entries)
    };
    let prev_log_term = node.term_at(prev_log_index).unwrap_or(0);
    Ok(ClusterKvRaftAppendRequest {
        snapshot,
        append: RaftAppendEntriesRequest {
            term: node.hard_state().current_term,
            leader_id: leader_id.clone(),
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: node.hard_state().commit_index,
        },
    })
}

fn build_cluster_raft_read_index_heartbeat_from_open_node(
    node: &DurableRaftKvNode,
    state: &ClusterBootstrapState,
    commit_index: u64,
) -> Result<ClusterRaftAppendRequest, KvError> {
    if node.node_id() != &state.local_node_id
        || node.hard_state().current_term != state.term
        || node.hard_state().leader_id.as_ref() != Some(&state.local_node_id)
    {
        return Err(KvError::AuthorityUnavailable {
            detail: format!(
                "cannot build ReadIndex heartbeat for {} at state term {} from resident node {} at term {} with leader {}",
                state.local_node_id,
                state.term,
                node.node_id(),
                node.hard_state().current_term,
                node.hard_state()
                    .leader_id
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "none".to_string())
            ),
        });
    }
    if commit_index > node.hard_state().commit_index {
        return Err(KvError::AuthorityUnavailable {
            detail: format!(
                "ReadIndex heartbeat anchor {commit_index} exceeds committed Raft index {}",
                node.hard_state().commit_index
            ),
        });
    }
    let prev_log_term = node.term_at(commit_index).ok_or_else(|| {
        KvError::AuthorityUnavailable {
            detail: format!(
                "ReadIndex heartbeat anchor {commit_index} is not retained by the resident Raft node"
            ),
        }
    })?;
    Ok(ClusterRaftAppendRequest {
        state: state.clone(),
        snapshot: None,
        append: RaftAppendEntriesRequest {
            term: node.hard_state().current_term,
            leader_id: state.local_node_id.clone(),
            prev_log_index: commit_index,
            prev_log_term,
            entries: Vec::new(),
            leader_commit: commit_index,
        },
        read_index_probe: true,
    })
}

fn read_index_state_from_open_node(node: &DurableRaftKvNode) -> ClusterKvRaftReadIndexState {
    let current_term = node.hard_state().current_term;
    let commit_index = node.hard_state().commit_index;
    let commit_term = node.term_at(commit_index);
    ClusterKvRaftReadIndexState {
        current_term,
        leader_id: node.hard_state().leader_id.clone(),
        commit_index,
        commit_term,
        applied_index: node.applied_index(),
        revision: node.store().revision(),
        has_current_term_commit: commit_index > 0 && commit_term == Some(current_term),
    }
}

fn committed_final_key_states(
    store: &InProcessKvStore,
    events: &[KvEvent],
) -> Result<Vec<ClusterKvCommittedKeyState>, KvError> {
    let mut keys = Vec::new();
    keys.try_reserve(events.len())
        .map_err(|_| KvError::ResourceExhausted {
            operation: "collect committed KV delta keys".to_string(),
            requested_bytes: events.len().saturating_mul(std::mem::size_of::<String>()),
        })?;
    for event in events {
        keys.push(try_clone_kv_string(
            &event.key,
            "collect committed KV delta key",
        )?);
    }
    keys.sort_unstable();
    keys.dedup();
    let mut states = Vec::new();
    try_reserve_kv_items(
        &mut states,
        keys.len(),
        "collect committed KV final key states",
    )?;
    for key in keys {
        let entry = store
            .get(&key)
            .map(|entry| try_clone_kv_entry(entry, "collect committed KV final key state"))
            .transpose()?;
        states.push(ClusterKvCommittedKeyState { key, entry });
    }
    Ok(states)
}

fn committed_command_has_selected_lease_mutation(
    command: &KvCommand,
    txn_response: Option<&KvTxnResponse>,
) -> bool {
    match command {
        KvCommand::Barrier => false,
        KvCommand::Op(
            KvOp::LeaseGrant { .. } | KvOp::LeaseKeepAlive { .. } | KvOp::LeaseRevoke { .. },
        ) => true,
        KvCommand::Op(_) => false,
        KvCommand::Txn(_) => txn_response.is_some_and(|response| {
            response.responses.iter().any(|response| {
                matches!(
                    response,
                    KvOpResponse::LeaseGrant { .. }
                        | KvOpResponse::LeaseKeepAlive { .. }
                        | KvOpResponse::LeaseRevoke { .. }
                )
            })
        }),
    }
}

impl ClusterKvRaftStore {
    pub fn new(paths: ClusterKvPaths, compaction_policy: ClusterKvRaftCompactionPolicy) -> Self {
        Self {
            paths,
            compaction_policy,
        }
    }

    pub fn paths(&self) -> &ClusterKvPaths {
        &self.paths
    }

    pub fn lock_operation(&self) -> Result<ClusterKvRaftOperationGuard, KvError> {
        lock_cluster_kv_raft_operation_for_paths(&self.paths)
    }

    fn lock_operation_with_wait(
        &self,
        wait: Duration,
    ) -> Result<ClusterKvRaftOperationGuard, KvError> {
        lock_cluster_kv_raft_operation_for_root_with_wait(&self.paths.root, Some(wait))
    }

    pub fn wal_paths(&self, node_id: &NodeId) -> KvWalPaths {
        self.paths.raft_wal_paths(node_id)
    }

    fn pending_higher_term_gate(
        &self,
        node_id: &NodeId,
    ) -> Result<Arc<PendingHigherRaftTermGate>, KvError> {
        let root = self.wal_paths(node_id).root;
        pending_higher_term_gate_for_root(&root)
    }

    fn acquire_pending_higher_term_authority_lease(
        &self,
        node_id: &NodeId,
        durable_term: Option<u64>,
        operation: &'static str,
    ) -> Result<PendingHigherRaftTermAuthorityLease, KvError> {
        let root = self.wal_paths(node_id).root;
        acquire_pending_higher_term_authority_lease_for_root(&root, durable_term, operation)
    }

    /// Reject work that has not yet entered the Raft operation/resident
    /// critical section when a higher observed term already fences local
    /// authority.
    ///
    /// This is intentionally only a preflight, not the linearizing lease: a
    /// publication may race immediately after it returns. The caller must
    /// still acquire and retain the ordinary authority lease after local
    /// admission. Performing this check first prevents newly arriving reads
    /// from queueing on the operation lock ahead of the mandatory term closer,
    /// without reversing the operation/resident -> retained-lease lock order.
    fn ensure_pending_higher_term_authority_available(
        &self,
        node_id: &NodeId,
        operation: &'static str,
    ) -> Result<(), KvError> {
        let gate = self.pending_higher_term_gate(node_id)?;
        let state = gate
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.exclusive {
            return Err(KvError::AuthorityUnavailable {
                detail: format!(
                    "cannot {operation} while a higher Raft term is being made durable"
                ),
            });
        }
        if let Some(pending_term) = state.floor {
            return Err(KvError::AuthorityUnavailable {
                detail: format!(
                    "cannot {operation} while observed Raft term {pending_term} is pending durable hard state"
                ),
            });
        }
        Ok(())
    }

    /// Preflight a term-bearing request before it waits for resident-node
    /// admission. A request at or above the current floor is allowed to enter
    /// because its durable application can close the fence; an older request
    /// can never do so and must fail without joining the local lock convoy.
    fn ensure_pending_higher_term_request_can_close(
        &self,
        node_id: &NodeId,
        observed_term: u64,
        operation: &'static str,
    ) -> Result<(), KvError> {
        let gate = self.pending_higher_term_gate(node_id)?;
        let state = gate
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // A term closer may already own the exclusive lease for the same
        // observation. Term-bearing requests must be allowed to wait behind
        // that short durability phase: once it releases, this request either
        // closes a still-newer floor itself or lets ordinary Raft term checks
        // reject it. Returning Busy here makes valid higher-term AppendEntries
        // fail nondeterministically depending on which closer wins the race.
        if let Some(pending_term) = state.floor
            && observed_term < pending_term
        {
            return Err(KvError::AuthorityUnavailable {
                detail: format!(
                    "cannot {operation} at Raft term {observed_term} while observed Raft term {pending_term} is pending durable hard state"
                ),
            });
        }
        Ok(())
    }

    fn acquire_pending_higher_term_exclusive_lease(
        &self,
        node_id: &NodeId,
    ) -> Result<PendingHigherRaftTermExclusiveLease, KvError> {
        let gate = self.pending_higher_term_gate(node_id)?;
        let mut state = gate
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while state.exclusive {
            state = gate
                .changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        // Claim writer intent before draining existing actions. Publication is
        // still allowed to raise the floor under the small state mutex, but no
        // new ordinary authority lease can pass this exclusive intent.
        state.exclusive = true;
        while state.active_authority_leases > 0 {
            state = gate
                .changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        drop(state);
        Ok(PendingHigherRaftTermExclusiveLease { gate })
    }

    fn acquire_pending_higher_term_append_lease(
        &self,
        node_id: &NodeId,
        append_term: u64,
        durable_term: u64,
    ) -> Result<PendingHigherRaftTermAppendLease, KvError> {
        let root = self.wal_paths(node_id).root;
        let gate = self.pending_higher_term_gate(node_id)?;
        let mut state = gate
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while state.exclusive {
            state = gate
                .changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        if state.floor.is_none() && append_term <= durable_term {
            state.active_authority_leases = state
                .active_authority_leases
                .checked_add(1)
                .ok_or_else(|| KvError::Io {
                    path: root.display().to_string(),
                    detail: "pending higher Raft term authority lease count overflow".to_string(),
                })?;
            drop(state);
            return Ok(PendingHigherRaftTermAppendLease::Settled(
                PendingHigherRaftTermAuthorityLease { gate },
            ));
        }
        if let Some(pending_term) = state.floor
            && append_term < pending_term
        {
            return Err(KvError::AuthorityUnavailable {
                detail: format!(
                    "cannot apply a Raft append at term {append_term} while observed Raft term {pending_term} is pending durable hard state"
                ),
            });
        }
        state.exclusive = true;
        while state.active_authority_leases > 0 {
            state = gate
                .changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        drop(state);
        Ok(PendingHigherRaftTermAppendLease::Closing(
            PendingHigherRaftTermExclusiveLease { gate },
        ))
    }

    /// Publish a higher term before queuing the durable hard-state update.
    /// Floors are monotonic so concurrent observations cannot make an older
    /// persistence completion reopen authority prematurely.
    pub fn publish_pending_higher_term(&self, node_id: &NodeId, term: u64) -> Result<(), KvError> {
        let gate = self.pending_higher_term_gate(node_id)?;
        let mut state = gate
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Publication is deliberately O(1), even on a Tokio worker. Existing
        // authority leases linearized before this observation and may drain;
        // the raised floor immediately rejects every later authority action.
        // The supervised durable closer owns any blocking wait.
        if state
            .known_durable
            .is_none_or(|durable| durable.term < term)
        {
            state.floor = Some(state.floor.unwrap_or_default().max(term));
        }
        gate.changed.notify_all();
        Ok(())
    }

    pub fn pending_higher_term(&self, node_id: &NodeId) -> Result<Option<u64>, KvError> {
        let root = self.wal_paths(node_id).root;
        let gate = self.pending_higher_term_gate(node_id)?;
        let mut state = gate.state.lock().map_err(|error| KvError::Io {
            path: root.display().to_string(),
            detail: format!("pending higher Raft term gate poisoned: {error}"),
        })?;
        // An exclusive closer may already have made the hard-state write
        // externally visible while it is still refreshing the resident read
        // view and clearing the matching floor. Do not expose that internal
        // transition as a contradictory settled state. Publication never uses
        // this accessor and remains O(1); observers join only the already
        // admitted node-local durability phase.
        #[cfg(test)]
        let mut registered_waiter = false;
        while state.exclusive {
            #[cfg(test)]
            if !registered_waiter {
                state.settled_read_waiters = state.settled_read_waiters.saturating_add(1);
                registered_waiter = true;
                gate.changed.notify_all();
            }
            state = gate.changed.wait(state).map_err(|error| KvError::Io {
                path: root.display().to_string(),
                detail: format!(
                    "pending higher Raft term gate poisoned while joining its durable close: {error}"
                ),
            })?;
        }
        #[cfg(test)]
        if registered_waiter {
            state.settled_read_waiters = state.settled_read_waiters.saturating_sub(1);
        }
        Ok(state.floor)
    }

    /// Record the acknowledged durable term and clear the fence only when that
    /// hard state reaches the newest floor. A different ownership generation,
    /// or a lower term proving same-path durable-state replacement, resets the
    /// cached watermark. Returns whether a floor was removed. A concurrent
    /// higher observation is retained because the comparison is made while
    /// holding the gate lock.
    fn clear_pending_higher_term_if_durable(
        &self,
        node_id: &NodeId,
        generation: PendingHigherRaftTermGeneration,
        durable_term: u64,
    ) -> Result<bool, KvError> {
        let root = self.wal_paths(node_id).root;
        let gate = self.pending_higher_term_gate(node_id)?;
        let mut state = gate.state.lock().map_err(|error| KvError::Io {
            path: root.display().to_string(),
            detail: format!("pending higher Raft term gate poisoned: {error}"),
        })?;
        while state.exclusive {
            state = gate.changed.wait(state).map_err(|error| KvError::Io {
                path: root.display().to_string(),
                detail: format!("pending higher Raft term gate poisoned while clearing: {error}"),
            })?;
        }
        state.acknowledge_durable_term(generation, durable_term);
        if state
            .floor
            .is_some_and(|pending_term| durable_term >= pending_term)
        {
            state.floor = None;
            return Ok(true);
        }
        Ok(false)
    }

    fn ensure_pending_higher_term_observation_is_current(
        lease: &PendingHigherRaftTermExclusiveLease,
        observed_term: u64,
    ) -> Result<(), KvError> {
        let Some(pending_term) = lease.pending_floor() else {
            return Ok(());
        };
        if observed_term >= pending_term {
            return Ok(());
        }
        Err(KvError::AuthorityUnavailable {
            detail: format!(
                "cannot close pending Raft term fence at term {observed_term}; current observed floor is {pending_term}"
            ),
        })
    }

    fn acknowledge_open_node_term(
        &self,
        node_id: &NodeId,
        node: DurableRaftKvNode,
    ) -> Result<DurableRaftKvNode, KvError> {
        self.clear_pending_higher_term_if_durable(
            node_id,
            node.pending_higher_term_generation(),
            node.hard_state().current_term,
        )?;
        Ok(node)
    }

    fn snapshot_bytes(&self, node_id: &NodeId) -> u64 {
        fs::metadata(&self.wal_paths(node_id).snapshot)
            .map(|metadata| metadata.len())
            .unwrap_or(0)
    }

    fn compact_node_if_needed(&self, node: &mut DurableRaftKvNode) -> Result<bool, KvError> {
        let Some(snapshot) = self.compact_node_if_needed_with_checkpoint(node)? else {
            return Ok(false);
        };
        self.paths
            .local_cache()
            .write_raft_snapshot_if_changed(&snapshot)?;
        Ok(true)
    }

    fn compact_node_if_needed_with_checkpoint(
        &self,
        node: &mut DurableRaftKvNode,
    ) -> Result<Option<Arc<KvSnapshot>>, KvError> {
        compact_cluster_kv_raft_node_with_checkpoint_if_needed(
            node,
            self.snapshot_bytes(node.node_id()),
            self.compaction_policy,
        )
        .map(|snapshot| snapshot.map(|snapshot| snapshot.kv))
    }

    fn open_observed_node(&self, node_id: &NodeId) -> Result<KvObservedDurableNode, KvError> {
        // Admission is immediate and precedes every native owner acquisition.
        let capacity =
            try_admit_committed_observation(&self.paths, KvObservationProposalOwnership::Absent)?;
        let fence = capacity.storage_owner()?;
        let mut owner = KvObservedDurableNode {
            node: None,
            fence: Some(fence),
        };
        let mut node = self.open_node(node_id)?;
        node.observation = Some(KvDurableObservationContext::new(capacity));
        owner.node = Some(node);
        Ok(owner)
    }

    pub fn open_node(&self, node_id: &NodeId) -> Result<DurableRaftKvNode, KvError> {
        let node = DurableRaftKvNode::open_resident(node_id.clone(), self.wal_paths(node_id))?;
        self.acknowledge_open_node_term(node_id, node)
    }

    /// Evaluate a marked ReadIndex heartbeat without waiting for either the
    /// Raft operation lock or mutable resident checkout.
    pub fn read_index_probe(
        &self,
        node_id: &NodeId,
        request: &RaftAppendEntriesRequest,
    ) -> Result<ClusterKvRaftReadIndexProbeOutcome, KvError> {
        read_resident_raft_read_index_probe(&self.wal_paths(node_id), node_id, request)
    }

    /// Compatibility guard: opening succeeds only without a pending suffix.
    /// Normal initialization and reads must use `open_node` and preserve it.
    pub fn open_node_discarding_uncommitted_tail(
        &self,
        node_id: &NodeId,
    ) -> Result<DurableRaftKvNode, KvError> {
        let node = DurableRaftKvNode::open_resident_discarding_uncommitted_tail(
            node_id.clone(),
            self.wal_paths(node_id),
        )?;
        self.acknowledge_open_node_term(node_id, node)
    }

    fn open_resident_node(&self, node_id: &NodeId) -> Result<DurableRaftKvNode, KvError> {
        self.open_node(node_id)
    }

    fn open_resident_node_with_wait(
        &self,
        node_id: &NodeId,
        wait: Duration,
    ) -> Result<DurableRaftKvNode, KvError> {
        let node = DurableRaftKvNode::open_resident_inner(
            node_id.clone(),
            self.wal_paths(node_id),
            false,
            wait,
        )?;
        self.acknowledge_open_node_term(node_id, node)
    }

    fn open_existing_resident_node_with_wait(
        &self,
        node_id: &NodeId,
        wait: Duration,
    ) -> Result<DurableRaftKvNode, KvError> {
        let node = DurableRaftKvNode::open_existing_resident_inner(
            node_id.clone(),
            self.wal_paths(node_id),
            wait,
        )?;
        self.acknowledge_open_node_term(node_id, node)
    }

    fn open_resident_node_discarding_uncommitted_tail(
        &self,
        node_id: &NodeId,
    ) -> Result<DurableRaftKvNode, KvError> {
        self.open_node_discarding_uncommitted_tail(node_id)
    }

    fn open_resident_node_discarding_uncommitted_tail_with_wait(
        &self,
        node_id: &NodeId,
        wait: Duration,
    ) -> Result<DurableRaftKvNode, KvError> {
        let node = DurableRaftKvNode::open_resident_inner(
            node_id.clone(),
            self.wal_paths(node_id),
            true,
            wait,
        )?;
        self.acknowledge_open_node_term(node_id, node)
    }

    pub async fn open_node_async(&self, node_id: &NodeId) -> Result<DurableRaftKvNode, KvError> {
        let store = self.clone();
        let node_id = node_id.clone();
        let error_path = store.wal_paths(&node_id).root.display().to_string();
        tokio::task::spawn_blocking(move || store.open_node(&node_id))
            .await
            .map_err(|source| KvError::Io {
                path: error_path,
                detail: source.to_string(),
            })?
    }

    pub fn build_append_entries_request_from_next_index(
        &self,
        node_id: &NodeId,
        leader_id: &NodeId,
        next_index: u64,
    ) -> Result<ClusterKvRaftAppendRequest, KvError> {
        let node = self.open_node(node_id)?;
        let _authority_lease = self.acquire_pending_higher_term_authority_lease(
            node_id,
            Some(node.hard_state().current_term),
            "build a leader replication request",
        )?;
        build_append_entries_request_from_open_node(&node, leader_id, next_index)
    }

    pub fn build_cluster_raft_append_request_from_next_index(
        &self,
        state: &ClusterBootstrapState,
        next_index: u64,
    ) -> Result<ClusterRaftAppendRequest, KvError> {
        let request = self.build_append_entries_request_from_next_index(
            &state.local_node_id,
            &state.local_node_id,
            next_index,
        )?;
        Ok(ClusterRaftAppendRequest {
            state: state.clone(),
            snapshot: request.snapshot,
            append: request.append,
            read_index_probe: false,
        })
    }

    pub fn locked_build_cluster_raft_append_request_from_next_index(
        &self,
        state: &ClusterBootstrapState,
        next_index: u64,
    ) -> Result<ClusterRaftAppendRequest, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.build_cluster_raft_append_request_from_next_index(state, next_index)
    }

    pub async fn locked_build_cluster_raft_append_request_from_next_index_async(
        &self,
        state: &ClusterBootstrapState,
        next_index: u64,
    ) -> Result<ClusterRaftAppendRequest, KvError> {
        let store = self.clone();
        let state = state.clone();
        let error_path = store
            .wal_paths(&state.local_node_id)
            .root
            .display()
            .to_string();
        tokio::task::spawn_blocking(move || {
            let _raft_guard = store.lock_operation()?;
            store.build_cluster_raft_append_request_from_next_index(&state, next_index)
        })
        .await
        .map_err(|source| KvError::Io {
            path: error_path,
            detail: source.to_string(),
        })?
    }

    pub fn cache_snapshot_if_newer_than_raft(
        &self,
        node_id: &NodeId,
        snapshot: KvSnapshot,
    ) -> Result<bool, KvError> {
        let source =
            try_admit_committed_observation(&self.paths, KvObservationProposalOwnership::Absent)?;
        let cache = self
            .paths
            .local_cache()
            .with_observation_source(source, true);
        let result = cache.cache_snapshot_if_newer_than_raft(node_id.clone(), snapshot);
        let delivery = cache.finish_observation();
        result.and_then(|changed| delivery.map(|()| changed))
    }

    pub fn cache_snapshot_if_newer_than_raft_and_notify(
        &self,
        node_id: &NodeId,
        snapshot: KvSnapshot,
    ) -> Result<bool, KvError> {
        let cache_changed = self.cache_snapshot_if_newer_than_raft(node_id, snapshot)?;
        if cache_changed {
            notify_cluster_kv_watchers(&self.paths);
        }
        Ok(cache_changed)
    }

    pub fn locked_cache_snapshot_if_newer_than_raft_and_notify(
        &self,
        node_id: &NodeId,
        snapshot: KvSnapshot,
    ) -> Result<bool, KvError> {
        let source =
            try_admit_committed_observation(&self.paths, KvObservationProposalOwnership::Absent)?;
        let fence = source.storage_owner()?;
        let mut owner = KvObservedOperationGuard {
            guard: None,
            fence: Some(fence),
        };
        owner.guard = Some(self.lock_operation()?);
        // The inner file owner releases before this call returns. One outer
        // fence covers that nested owner and the native operation permit.
        let cache = self
            .paths
            .local_cache()
            .with_observation_source(source, false);
        let result = cache.cache_snapshot_if_newer_than_raft(node_id.clone(), snapshot);
        drop(owner);
        let delivery = cache.finish_observation();
        let changed = result.and_then(|changed| delivery.map(|()| changed))?;
        if changed {
            notify_cluster_kv_watchers(&self.paths);
        }
        Ok(changed)
    }

    pub fn local_vote_metadata(
        &self,
        node_id: &NodeId,
    ) -> Result<ClusterKvRaftLocalVoteMetadata, KvError> {
        let node = self.open_node(node_id)?;
        let _authority_lease = self.acquire_pending_higher_term_authority_lease(
            node_id,
            Some(node.hard_state().current_term),
            "inspect local election state",
        )?;
        Ok(ClusterKvRaftLocalVoteMetadata {
            current_term: node.hard_state().current_term,
            last_log_index: node.last_log_index(),
            last_log_term: node.last_log_term(),
        })
    }

    pub fn locked_local_vote_metadata(
        &self,
        node_id: &NodeId,
    ) -> Result<ClusterKvRaftLocalVoteMetadata, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.local_vote_metadata(node_id)
    }

    pub fn locked_local_vote_metadata_before(
        &self,
        node_id: &NodeId,
        deadline: Instant,
    ) -> Result<ClusterKvRaftLocalVoteMetadata, KvError> {
        let wait = deadline.saturating_duration_since(Instant::now());
        if wait.is_zero() {
            return Err(KvError::Busy {
                path: self.paths.root.display().to_string(),
            });
        }
        let _raft_guard = self.lock_operation_with_wait(wait)?;
        if Instant::now() >= deadline {
            return Err(KvError::Busy {
                path: self.paths.root.display().to_string(),
            });
        }
        let node = self.open_resident_node_with_wait(
            node_id,
            deadline.saturating_duration_since(Instant::now()),
        )?;
        if Instant::now() >= deadline {
            return Err(KvError::Busy {
                path: self.paths.root.display().to_string(),
            });
        }
        let _authority_lease = self.acquire_pending_higher_term_authority_lease(
            node_id,
            Some(node.hard_state().current_term),
            "inspect local election state",
        )?;
        Ok(ClusterKvRaftLocalVoteMetadata {
            current_term: node.hard_state().current_term,
            last_log_index: node.last_log_index(),
            last_log_term: node.last_log_term(),
        })
    }

    pub fn last_log_index(&self, node_id: &NodeId) -> Result<u64, KvError> {
        self.open_node(node_id).map(|node| node.last_log_index())
    }

    pub fn locked_last_log_index(&self, node_id: &NodeId) -> Result<u64, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.last_log_index(node_id)
    }

    pub async fn locked_last_log_index_async(&self, node_id: &NodeId) -> Result<u64, KvError> {
        let store = self.clone();
        let node_id = node_id.clone();
        let error_path = store.wal_paths(&node_id).root.display().to_string();
        tokio::task::spawn_blocking(move || {
            let _raft_guard = store.lock_operation()?;
            store.last_log_index(&node_id)
        })
        .await
        .map_err(|source| KvError::Io {
            path: error_path,
            detail: format!("cluster kv raft log index task failed: {source}"),
        })?
    }

    pub fn apply_vote(
        &self,
        node_id: &NodeId,
        vote: RaftRequestVoteRequest,
    ) -> Result<RaftRequestVoteResponse, KvError> {
        // A vote carries the same durable term evidence as AppendEntries. If
        // its term reaches the published floor, let it serialize with the
        // current closer and durably close the fence itself; an older vote
        // cannot restore authority and must not join the resident wait queue.
        self.ensure_pending_higher_term_request_can_close(node_id, vote.term, "apply a Raft vote")?;
        let mut node = self.open_node(node_id)?;
        let term_lease = self.acquire_pending_higher_term_exclusive_lease(node_id)?;
        Self::ensure_pending_higher_term_observation_is_current(&term_lease, vote.term)?;
        let response = node.request_vote(vote)?;
        term_lease.clear_if_durable(
            node.pending_higher_term_generation(),
            node.hard_state().current_term,
        );
        Ok(response)
    }

    /// Validate and apply one term-bearing vote while the caller retains the
    /// matching Raft operation permit.
    ///
    /// The validation callback receives the committed config entry borrowed
    /// from the exact resident node that will process `vote`. It must remain
    /// bounded and must not enter another KV operation. A validation error is
    /// returned separately from storage errors so `sinomo-cluster` can retain
    /// ownership of authentication and membership policy without introducing
    /// a dependency from KV back to the cluster bridge.
    pub fn apply_vote_after_config_validation<T, E, F>(
        &self,
        operation_guard: &ClusterKvRaftOperationGuard,
        node_id: &NodeId,
        vote: RaftRequestVoteRequest,
        validate: F,
    ) -> Result<Result<(RaftRequestVoteResponse, T), E>, KvError>
    where
        F: FnOnce(Option<&KvEntry>) -> Result<ClusterKvRaftVoteDisposition<T>, E>,
    {
        if operation_guard.root != self.paths.root {
            return Err(KvError::Io {
                path: self.paths.root.display().to_string(),
                detail: "Raft vote validation received an operation permit for another store"
                    .to_string(),
            });
        }
        self.ensure_pending_higher_term_request_can_close(
            node_id,
            vote.term,
            "validate and apply a Raft vote",
        )?;
        let mut node = self.open_node(node_id)?;
        let term_lease = self.acquire_pending_higher_term_exclusive_lease(node_id)?;
        Self::ensure_pending_higher_term_observation_is_current(&term_lease, vote.term)?;
        let config_key = cluster_kv_raft_config_key();
        let disposition = match validate(node.store().get(&config_key)) {
            Ok(disposition) => disposition,
            Err(error) => return Ok(Err(error)),
        };
        let (response, value) = match disposition {
            ClusterKvRaftVoteDisposition::Apply(value) => {
                // Validation may include a bounded local-state write. A newer
                // authenticated observation can raise the floor concurrently,
                // so close the check/apply window again before durable voting.
                Self::ensure_pending_higher_term_observation_is_current(&term_lease, vote.term)?;
                let response = node.request_vote(vote)?;
                term_lease.clear_if_durable(
                    node.pending_higher_term_generation(),
                    node.hard_state().current_term,
                );
                (response, value)
            }
            ClusterKvRaftVoteDisposition::Reject(value) => (
                RaftRequestVoteResponse {
                    term: node.hard_state().current_term,
                    vote_granted: false,
                },
                value,
            ),
        };
        Ok(Ok((response, value)))
    }

    pub fn locked_apply_vote(
        &self,
        node_id: &NodeId,
        vote: RaftRequestVoteRequest,
    ) -> Result<RaftRequestVoteResponse, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.apply_vote(node_id, vote)
    }

    /// Apply a vote only if every admission phase completes before `deadline`.
    /// In particular, a queued blocking task that starts after its caller has
    /// timed out cannot mutate durable hard state later.
    pub fn locked_apply_vote_before(
        &self,
        node_id: &NodeId,
        vote: RaftRequestVoteRequest,
        deadline: Instant,
    ) -> Result<RaftRequestVoteResponse, KvError> {
        self.ensure_pending_higher_term_request_can_close(node_id, vote.term, "apply a Raft vote")?;
        let wait = deadline.saturating_duration_since(Instant::now());
        if wait.is_zero() {
            return Err(KvError::Busy {
                path: self.paths.root.display().to_string(),
            });
        }
        let _raft_guard = self.lock_operation_with_wait(wait)?;
        if Instant::now() >= deadline {
            return Err(KvError::Busy {
                path: self.paths.root.display().to_string(),
            });
        }
        let mut node = self.open_resident_node_with_wait(
            node_id,
            deadline.saturating_duration_since(Instant::now()),
        )?;
        if Instant::now() >= deadline {
            return Err(KvError::Busy {
                path: self.paths.root.display().to_string(),
            });
        }
        let term_lease = self.acquire_pending_higher_term_exclusive_lease(node_id)?;
        Self::ensure_pending_higher_term_observation_is_current(&term_lease, vote.term)?;
        let response = node.request_vote(vote)?;
        term_lease.clear_if_durable(
            node.pending_higher_term_generation(),
            node.hard_state().current_term,
        );
        Ok(response)
    }

    pub fn apply_vote_response(
        &self,
        state: &ClusterBootstrapState,
        vote: RaftRequestVoteRequest,
    ) -> Result<ClusterRaftVoteResponse, KvError> {
        let vote = self.apply_vote(&state.local_node_id, vote)?;
        Ok(ClusterRaftVoteResponse {
            accepted: vote.vote_granted,
            detail: if vote.vote_granted {
                "raft vote granted".to_string()
            } else {
                "raft vote rejected by local hard state".to_string()
            },
            state: state.clone(),
            vote,
        })
    }

    pub fn apply_append_entries_with_optional_snapshot(
        &self,
        node_id: &NodeId,
        snapshot: Option<RaftSnapshot>,
        append: RaftAppendEntriesRequest,
    ) -> Result<RaftAppendEntriesApplyOutcome, KvError> {
        let (compact, snapshot) = self
            .apply_append_entries_with_optional_snapshot_capture(node_id, snapshot, append, true)?;
        Ok(RaftAppendEntriesApplyOutcome {
            response: compact.response,
            snapshot: snapshot.expect("full compatibility apply captured its terminal view"),
            snapshot_restored: compact.snapshot_restored,
            restored_snapshot: compact.restored_snapshot,
            committed_events: compact.committed_events,
            observation_receipt: compact.observation_receipt,
            restored_snapshot_applied_index: compact.restored_snapshot_applied_index,
        })
    }

    /// Applies follower AppendEntries without converting the resident KV store
    /// into a full snapshot. A full value is carried only when the leader
    /// supplied one for restore or this append naturally crossed the periodic
    /// checkpoint threshold.
    pub fn apply_append_entries_with_optional_snapshot_compact(
        &self,
        node_id: &NodeId,
        snapshot: Option<RaftSnapshot>,
        append: RaftAppendEntriesRequest,
    ) -> Result<RaftAppendEntriesCompactApplyOutcome, KvError> {
        self.apply_append_entries_with_optional_snapshot_capture(node_id, snapshot, append, false)
            .map(|(outcome, _)| outcome)
    }

    // Only the legacy full-view API materializes the terminal snapshot. Capture
    // it under the same native owner as apply, rather than reopening a later
    // producer's view after that owner releases.
    fn apply_append_entries_with_optional_snapshot_capture(
        &self,
        node_id: &NodeId,
        snapshot: Option<RaftSnapshot>,
        append: RaftAppendEntriesRequest,
        capture_full_snapshot: bool,
    ) -> Result<
        (
            RaftAppendEntriesCompactApplyOutcome,
            Option<Arc<KvSnapshot>>,
        ),
        KvError,
    > {
        // `read_index_probe` routes a fenced request to this durable path.
        // Do not let an old-term heartbeat become successful merely because
        // the pending higher-term fsync is still queued. An append at or above
        // the floor may itself durably close the fence.
        self.ensure_pending_higher_term_request_can_close(
            node_id,
            append.term,
            "apply a Raft append",
        )?;
        let incoming_snapshot = snapshot.as_ref().map(|snapshot| Arc::clone(&snapshot.kv));
        let incoming_snapshot_index = snapshot
            .as_ref()
            .map(|snapshot| snapshot.last_included_index);
        let mut node = self.open_observed_node(node_id)?;
        let term_lease = self.acquire_pending_higher_term_append_lease(
            node_id,
            append.term,
            node.hard_state().current_term,
        )?;
        term_lease.ensure_observation_is_current(append.term)?;
        let before_applied_index = node.applied_index();
        let (response, snapshot_restored, before_revision) =
            node.apply_append_entries_with_optional_snapshot_state(snapshot, append)?;
        term_lease.clear_if_durable(
            node.pending_higher_term_generation(),
            node.hard_state().current_term,
        );
        let commit_advanced = node.applied_index() > before_applied_index;
        let selected_lease_mutation = commit_advanced
            && (before_applied_index.saturating_add(1)..=node.applied_index()).any(|index| {
                node.log_entry_at(index).is_some_and(|entry| {
                    committed_command_has_selected_lease_mutation(
                        &entry.command,
                        node.txn_response_at(index),
                    )
                })
            });
        // Capture the complete newly-applied batch before checkpoint compaction
        // trims retained event history at a revision boundary.
        let delta_base = if snapshot_restored {
            incoming_snapshot
                .as_ref()
                .map_or(before_revision, |snapshot| snapshot.revision)
        } else {
            before_revision
        };
        let committed_events = if node.store().revision() == delta_base {
            Vec::new()
        } else {
            node.store().events_after_revision(delta_base)?
        };
        let final_key_states = committed_final_key_states(node.store(), &committed_events)?;
        let checkpoint_snapshot = self.compact_node_if_needed_with_checkpoint(&mut node)?;
        let read_state = read_index_state_from_open_node(&node);
        let compacted_revision = node.store().compacted_revision();
        let full_snapshot = if capture_full_snapshot {
            Some(match checkpoint_snapshot.as_ref() {
                Some(snapshot) => Arc::clone(snapshot),
                None => Arc::new(node.store().try_snapshot()?),
            })
        } else {
            None
        };
        node.refresh_resident_read_view();
        Ok((
            RaftAppendEntriesCompactApplyOutcome {
                response,
                snapshot_restored,
                restored_snapshot: (snapshot_restored.then_some(incoming_snapshot).flatten())
                    .map(Into::into),
                committed_events: committed_events.into(),
                read_state,
                compacted_revision,
                checkpoint_snapshot: checkpoint_snapshot.map(Into::into),
                final_key_states: final_key_states.into(),
                commit_advanced,
                selected_lease_mutation,
                observation_receipt: node.observation_receipt(),
                restored_snapshot_applied_index: snapshot_restored
                    .then_some(incoming_snapshot_index)
                    .flatten(),
            },
            full_snapshot,
        ))
    }

    pub fn apply_append_entries_with_optional_snapshot_and_publish<F>(
        &self,
        node_id: &NodeId,
        snapshot: Option<RaftSnapshot>,
        append: RaftAppendEntriesRequest,
        key_can_defer_materialized_snapshot: F,
    ) -> Result<(RaftAppendEntriesApplyOutcome, bool), KvError>
    where
        F: FnMut(&str) -> bool,
    {
        let outcome =
            self.apply_append_entries_with_optional_snapshot(node_id, snapshot, append)?;
        let wrote = self
            .publish_append_entries_apply_outcome(&outcome, key_can_defer_materialized_snapshot)?;
        Ok((outcome, wrote))
    }

    pub fn apply_append_entries_with_optional_snapshot_compact_and_publish(
        &self,
        node_id: &NodeId,
        snapshot: Option<RaftSnapshot>,
        append: RaftAppendEntriesRequest,
    ) -> Result<(RaftAppendEntriesCompactApplyOutcome, bool), KvError> {
        let outcome =
            self.apply_append_entries_with_optional_snapshot_compact(node_id, snapshot, append)?;
        // The materialized file is an observational checkpoint. A follower
        // publishes only an installed snapshot or a periodic Raft checkpoint;
        // ordinary committed deltas stay in the resident indexed state.
        let wrote = self.publish_compact_append_entries_apply_outcome(&outcome)?;
        Ok((outcome, wrote))
    }

    pub fn append_entries_rejection_response(
        &self,
        node_id: &NodeId,
    ) -> Result<RaftAppendEntriesResponse, KvError> {
        let node = self.open_node(node_id)?;
        Ok(RaftAppendEntriesResponse {
            term: node.hard_state().current_term,
            success: false,
            match_index: node.last_log_index(),
        })
    }

    pub fn request_vote_rejection_response(
        &self,
        node_id: &NodeId,
    ) -> Result<RaftRequestVoteResponse, KvError> {
        let hard_state = self.wal_paths(node_id).read_persisted_hard_state()?;
        Ok(RaftRequestVoteResponse {
            term: hard_state.current_term,
            vote_granted: false,
        })
    }

    pub fn cluster_raft_vote_rejection_response(
        &self,
        state: &ClusterBootstrapState,
        detail: String,
    ) -> Result<ClusterRaftVoteResponse, KvError> {
        Ok(ClusterRaftVoteResponse {
            accepted: false,
            detail,
            state: state.clone(),
            vote: self.request_vote_rejection_response(&state.local_node_id)?,
        })
    }

    pub fn known_leader_id(&self, node_id: &NodeId) -> Result<Option<NodeId>, KvError> {
        // Read only the tiny hard-state file rather than opening the full Raft
        // node, which may replay a large WAL. Topology validity remains the
        // caller's job because KV does not interpret cluster membership.
        let hard_state = self.wal_paths(node_id).read_persisted_hard_state()?;
        let _authority_lease = self.acquire_pending_higher_term_authority_lease(
            node_id,
            Some(hard_state.current_term),
            "trust local Raft leadership",
        )?;
        Ok(hard_state.leader_id)
    }

    pub fn locked_known_leader_id(&self, node_id: &NodeId) -> Result<Option<NodeId>, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.known_leader_id(node_id)
    }

    pub fn known_voter_leader_id(
        &self,
        state: &ClusterBootstrapState,
    ) -> Result<Option<NodeId>, KvError> {
        Ok(self
            .known_voter_leader_fence(state)?
            .map(|fence| fence.leader_id))
    }

    pub fn known_voter_leader_fence(
        &self,
        state: &ClusterBootstrapState,
    ) -> Result<Option<ClusterKvRaftLeaderFence>, KvError> {
        // Read the id and term from one tiny durable hard-state image. Reading
        // them through separate helpers would admit an ABA leadership change
        // between the two observations.
        let hard_state = self
            .wal_paths(&state.local_node_id)
            .read_persisted_hard_state()?;
        let _authority_lease = self.acquire_pending_higher_term_authority_lease(
            &state.local_node_id,
            Some(hard_state.current_term),
            "capture local Raft voter-leader fence",
        )?;
        Ok(hard_state
            .leader_id
            .filter(|leader_id| cluster_kv_raft_node_is_voter(state, leader_id))
            .map(|leader_id| ClusterKvRaftLeaderFence {
                leader_id,
                current_term: hard_state.current_term,
            }))
    }

    pub fn locked_known_voter_leader_id(
        &self,
        state: &ClusterBootstrapState,
    ) -> Result<Option<NodeId>, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.known_voter_leader_id(state)
    }

    /// Returns the durable voter-leader hint only when the process-local Raft
    /// operation gate is immediately available.
    ///
    /// The hint is advisory: follower forwarding must never queue behind a
    /// mutation or authoritative read merely to choose which exact voter to
    /// try first. A busy gate therefore returns [`KvError::Busy`] without
    /// registering a durable waiter or reading hard state.
    pub fn try_locked_known_voter_leader_id(
        &self,
        state: &ClusterBootstrapState,
    ) -> Result<Option<NodeId>, KvError> {
        let _raft_guard = self.lock_operation_with_wait(Duration::ZERO)?;
        self.known_voter_leader_id(state)
    }

    /// Returns one coherent durable voter-leader id and term only when the
    /// process-local Raft operation gate is immediately available.
    pub fn try_locked_known_voter_leader_fence(
        &self,
        state: &ClusterBootstrapState,
    ) -> Result<Option<ClusterKvRaftLeaderFence>, KvError> {
        let _raft_guard = self.lock_operation_with_wait(Duration::ZERO)?;
        self.known_voter_leader_fence(state)
    }

    pub fn locked_known_voter_leader_id_before(
        &self,
        state: &ClusterBootstrapState,
        deadline: Instant,
    ) -> Result<Option<NodeId>, KvError> {
        let wait = deadline.saturating_duration_since(Instant::now());
        if wait.is_zero() {
            return Err(KvError::Busy {
                path: self.paths.root.display().to_string(),
            });
        }
        let _raft_guard = self.lock_operation_with_wait(wait)?;
        // Winning admission at the deadline does not authorize work after it.
        // Recheck before even the tiny hard-state lookup.
        if Instant::now() >= deadline {
            return Err(KvError::Busy {
                path: self.paths.root.display().to_string(),
            });
        }
        self.known_voter_leader_id(state)
    }

    pub fn locked_leader_vote_metadata_and_config_before(
        &self,
        local_node_id: &NodeId,
        deadline: Instant,
    ) -> Result<
        (
            Option<NodeId>,
            ClusterKvRaftLocalVoteMetadata,
            Option<KvEntry>,
        ),
        KvError,
    > {
        let wait = deadline.saturating_duration_since(Instant::now());
        if wait.is_zero() {
            return Err(KvError::Busy {
                path: self.paths.root.display().to_string(),
            });
        }
        let _raft_guard = self.lock_operation_with_wait(wait)?;
        if Instant::now() >= deadline {
            return Err(KvError::Busy {
                path: self.paths.root.display().to_string(),
            });
        }
        let node = self.open_resident_node_with_wait(
            local_node_id,
            deadline.saturating_duration_since(Instant::now()),
        )?;
        if Instant::now() >= deadline {
            return Err(KvError::Busy {
                path: self.paths.root.display().to_string(),
            });
        }
        let _authority_lease = self.acquire_pending_higher_term_authority_lease(
            local_node_id,
            Some(node.hard_state().current_term),
            "inspect local election state",
        )?;
        let leader_id = node.hard_state().leader_id.clone();
        let metadata = ClusterKvRaftLocalVoteMetadata {
            current_term: node.hard_state().current_term,
            last_log_index: node.last_log_index(),
            last_log_term: node.last_log_term(),
        };
        let config_entry = node
            .store()
            .get(&cluster_kv_raft_config_key())
            .map(|entry| try_clone_kv_entry(entry, "capture election Raft config fence"))
            .transpose()?;
        Ok((leader_id, metadata, config_entry))
    }

    pub fn local_is_leader(&self, state: &ClusterBootstrapState) -> Result<bool, KvError> {
        Ok(
            self.known_voter_leader_id(state)?.as_ref() == Some(&state.local_node_id)
                && cluster_kv_raft_local_is_voter(state),
        )
    }

    pub fn locked_local_is_leader(&self, state: &ClusterBootstrapState) -> Result<bool, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.local_is_leader(state)
    }

    pub fn observe_term(&self, node_id: &NodeId, term: u64) -> Result<Option<KvSnapshot>, KvError> {
        let changed = self.observe_term_compact(node_id, term)?;
        if changed {
            let node = self.open_node(node_id)?;
            Ok(Some(node.store().try_snapshot()?))
        } else {
            Ok(None)
        }
    }

    pub fn observe_term_compact(&self, node_id: &NodeId, term: u64) -> Result<bool, KvError> {
        let mut node = self.open_node(node_id)?;
        let term_lease = self.acquire_pending_higher_term_exclusive_lease(node_id)?;
        Self::ensure_pending_higher_term_observation_is_current(&term_lease, term)?;
        let changed = node.observe_term(term)?;
        if changed {
            node.refresh_resident_read_view();
        }
        term_lease.clear_if_durable(
            node.pending_higher_term_generation(),
            node.hard_state().current_term,
        );
        Ok(changed)
    }

    pub fn locked_observe_term_compact(
        &self,
        node_id: &NodeId,
        term: u64,
    ) -> Result<bool, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.observe_term_compact(node_id, term)
    }

    pub fn observe_term_and_write_snapshot_if_changed(
        &self,
        node_id: &NodeId,
        term: u64,
    ) -> Result<bool, KvError> {
        let snapshot = self.observe_term(node_id, term)?;
        if let Some(snapshot) = snapshot {
            self.paths
                .local_cache()
                .write_raft_snapshot_if_changed(&snapshot)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn locked_observe_term_and_write_snapshot_if_changed(
        &self,
        node_id: &NodeId,
        term: u64,
    ) -> Result<bool, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.observe_term_and_write_snapshot_if_changed(node_id, term)
    }

    pub fn set_leader(&self, node_id: &NodeId, term: u64) -> Result<KvSnapshot, KvError> {
        self.set_leader_compact(node_id, term)?;
        let node = self.open_node(node_id)?;
        node.store().try_snapshot()
    }

    pub fn set_leader_compact(&self, node_id: &NodeId, term: u64) -> Result<(), KvError> {
        let mut node = self.open_node(node_id)?;
        let _authority_lease = self.acquire_pending_higher_term_authority_lease(
            node_id,
            Some(node.hard_state().current_term),
            "publish local Raft leadership",
        )?;
        node.become_leader(term)?;
        node.refresh_resident_read_view();
        Ok(())
    }

    pub fn locked_set_leader_compact(&self, node_id: &NodeId, term: u64) -> Result<(), KvError> {
        let _raft_guard = self.lock_operation()?;
        self.set_leader_compact(node_id, term)
    }

    pub fn locked_set_leader_compact_before(
        &self,
        node_id: &NodeId,
        term: u64,
        deadline: Instant,
    ) -> Result<(), KvError> {
        let wait = deadline.saturating_duration_since(Instant::now());
        if wait.is_zero() {
            return Err(KvError::Busy {
                path: self.paths.root.display().to_string(),
            });
        }
        let _raft_guard = self.lock_operation_with_wait(wait)?;
        if Instant::now() >= deadline {
            return Err(KvError::Busy {
                path: self.paths.root.display().to_string(),
            });
        }
        let mut node = self.open_resident_node_with_wait(
            node_id,
            deadline.saturating_duration_since(Instant::now()),
        )?;
        if Instant::now() >= deadline {
            return Err(KvError::Busy {
                path: self.paths.root.display().to_string(),
            });
        }
        let _authority_lease = self.acquire_pending_higher_term_authority_lease(
            node_id,
            Some(node.hard_state().current_term),
            "publish local Raft leadership",
        )?;
        node.become_leader(term)?;
        node.refresh_resident_read_view();
        Ok(())
    }

    /// Publishes leadership only while the exact committed configuration used
    /// by the election is still resident. The comparison and hard-state
    /// mutation share one operation lock so a concurrent follower append
    /// cannot advance configuration in between them.
    pub fn locked_set_leader_compact_if_config_before(
        &self,
        node_id: &NodeId,
        term: u64,
        expected_config: Option<&KvEntry>,
        deadline: Instant,
    ) -> Result<bool, KvError> {
        let wait = deadline.saturating_duration_since(Instant::now());
        if wait.is_zero() {
            return Err(KvError::Busy {
                path: self.paths.root.display().to_string(),
            });
        }
        let _raft_guard = self.lock_operation_with_wait(wait)?;
        if Instant::now() >= deadline {
            return Err(KvError::Busy {
                path: self.paths.root.display().to_string(),
            });
        }
        let mut node = self.open_resident_node_with_wait(
            node_id,
            deadline.saturating_duration_since(Instant::now()),
        )?;
        if Instant::now() >= deadline {
            return Err(KvError::Busy {
                path: self.paths.root.display().to_string(),
            });
        }
        if node.store().get(&cluster_kv_raft_config_key()) != expected_config {
            return Ok(false);
        }
        // Configuration equality was established under this same operation
        // guard. Recheck the absolute deadline immediately before mutation.
        if Instant::now() >= deadline {
            return Err(KvError::Busy {
                path: self.paths.root.display().to_string(),
            });
        }
        let _authority_lease = self.acquire_pending_higher_term_authority_lease(
            node_id,
            Some(node.hard_state().current_term),
            "publish local Raft leadership",
        )?;
        node.become_leader(term)?;
        node.refresh_resident_read_view();
        Ok(true)
    }

    pub fn set_leader_and_write_snapshot_if_changed(
        &self,
        node_id: &NodeId,
        term: u64,
    ) -> Result<KvSnapshot, KvError> {
        let snapshot = self.set_leader(node_id, term)?;
        self.paths
            .local_cache()
            .write_raft_snapshot_if_changed(&snapshot)?;
        Ok(snapshot)
    }

    pub fn locked_set_leader_and_write_snapshot_if_changed(
        &self,
        node_id: &NodeId,
        term: u64,
    ) -> Result<KvSnapshot, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.set_leader_and_write_snapshot_if_changed(node_id, term)
    }

    pub fn clear_leader(&self, node_id: &NodeId) -> Result<KvSnapshot, KvError> {
        self.clear_leader_compact(node_id)?;
        let node = self.open_node(node_id)?;
        node.store().try_snapshot()
    }

    pub fn clear_leader_compact(&self, node_id: &NodeId) -> Result<(), KvError> {
        let mut node = self.open_node(node_id)?;
        node.clear_known_leader()?;
        node.refresh_resident_read_view();
        Ok(())
    }

    pub fn locked_clear_leader_compact(&self, node_id: &NodeId) -> Result<(), KvError> {
        let _raft_guard = self.lock_operation()?;
        self.clear_leader_compact(node_id)
    }

    /// Clear exactly the stale voter-leader generation the caller fenced.
    /// This does not discard log entries and cannot clear a newer election
    /// outcome, including one that elects the same node in a later term.
    pub fn clear_verified_voter_leader_compact(
        &self,
        state: &ClusterBootstrapState,
        expected: &ClusterKvRaftLeaderFence,
    ) -> Result<bool, KvError> {
        let _raft_guard = self.lock_operation()?;
        let mut node = self.open_node(&state.local_node_id)?;
        let _authority_lease = self.acquire_pending_higher_term_authority_lease(
            &state.local_node_id,
            Some(node.hard_state().current_term),
            "clear verified stale Raft voter leader",
        )?;
        let verified = node.hard_state().current_term == expected.current_term
            && node
                .hard_state()
                .leader_id
                .as_ref()
                .filter(|leader_id| cluster_kv_raft_node_is_voter(state, leader_id))
                == Some(&expected.leader_id);
        if !verified {
            return Ok(false);
        }
        node.clear_known_leader()?;
        node.refresh_resident_read_view();
        Ok(true)
    }

    pub fn clear_verified_voter_leader_compact_before(
        &self,
        state: &ClusterBootstrapState,
        expected: &ClusterKvRaftLeaderFence,
        deadline: Instant,
    ) -> Result<bool, KvError> {
        let wait = deadline.saturating_duration_since(Instant::now());
        if wait.is_zero() {
            return Err(KvError::Busy {
                path: self.paths.root.display().to_string(),
            });
        }
        let _raft_guard = self.lock_operation_with_wait(wait)?;
        if Instant::now() >= deadline {
            return Err(KvError::Busy {
                path: self.paths.root.display().to_string(),
            });
        }
        let mut node = self.open_resident_node_with_wait(
            &state.local_node_id,
            deadline.saturating_duration_since(Instant::now()),
        )?;
        if Instant::now() >= deadline {
            return Err(KvError::Busy {
                path: self.paths.root.display().to_string(),
            });
        }
        let _authority_lease = self.acquire_pending_higher_term_authority_lease(
            &state.local_node_id,
            Some(node.hard_state().current_term),
            "clear verified stale Raft voter leader",
        )?;
        if Instant::now() >= deadline {
            return Err(KvError::Busy {
                path: self.paths.root.display().to_string(),
            });
        }
        let verified = node.hard_state().current_term == expected.current_term
            && node
                .hard_state()
                .leader_id
                .as_ref()
                .filter(|leader_id| cluster_kv_raft_node_is_voter(state, leader_id))
                == Some(&expected.leader_id);
        if !verified {
            return Ok(false);
        }
        node.clear_known_leader()?;
        node.refresh_resident_read_view();
        Ok(true)
    }

    pub fn clear_leader_and_write_snapshot_if_changed(
        &self,
        node_id: &NodeId,
    ) -> Result<KvSnapshot, KvError> {
        let snapshot = self.clear_leader(node_id)?;
        self.paths
            .local_cache()
            .write_raft_snapshot_if_changed(&snapshot)?;
        Ok(snapshot)
    }

    pub fn locked_clear_leader_and_write_snapshot_if_changed(
        &self,
        node_id: &NodeId,
    ) -> Result<KvSnapshot, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.clear_leader_and_write_snapshot_if_changed(node_id)
    }

    pub fn append_command_with_response(
        &self,
        node_id: &NodeId,
        command: KvCommand,
    ) -> Result<ClusterKvRaftCommitResult, KvError> {
        self.append_request_with_response(node_id, KvCommandRequest::generated(command))
    }

    pub fn append_request_with_response(
        &self,
        node_id: &NodeId,
        request: KvCommandRequest,
    ) -> Result<ClusterKvRaftCommitResult, KvError> {
        let mut node = self.open_observed_node(node_id)?;
        let _authority_lease = self.acquire_pending_higher_term_authority_lease(
            node_id,
            Some(node.hard_state().current_term),
            "append a local Raft proposal",
        )?;
        let digest = request.digest()?;
        if node.store().request_id_expired(&request.request_id) {
            return Err(KvError::RequestExpired {
                request_id: request.request_id,
            });
        }
        if let Some(record) = node.store().committed_request(&request.request_id) {
            if record.command_digest != digest {
                return Err(KvError::RequestIdConflict {
                    request_id: request.request_id,
                });
            }
            let txn_response = record.txn_response.clone();
            let committed_request = record.clone();
            return Ok(ClusterKvRaftCommitResult {
                snapshot: (node.store().try_snapshot()?).into(),
                txn_response,
                events: (Vec::new()).into(),
                committed_request,
                observation_receipt: None,
            });
        }
        if let Err(error) = node
            .node
            .as_ref()
            .expect("active observed Raft owner")
            .node
            .preflight_local_append_request(node.hard_state().current_term, &request)
        {
            let _ = mark_cluster_kv_request_rejected_before_commit(
                &self.paths,
                &request,
                error.to_string(),
            );
            return Err(error);
        }
        mark_cluster_kv_request_submitted(&self.paths, &request)?;
        let before_revision = node.store().revision();
        let term = node.hard_state().current_term.max(1);
        let request_for_outcome = request.clone();
        let entry = match node.append_local_request(term, request) {
            Ok(entry) => entry,
            Err(error) => {
                let _ = mark_cluster_kv_request_unknown(
                    &self.paths,
                    &request_for_outcome,
                    error.to_string(),
                );
                return Err(error);
            }
        };
        if let Err(error) = node.commit_to(entry.index) {
            let _ = mark_cluster_kv_request_unknown(
                &self.paths,
                &request_for_outcome,
                error.to_string(),
            );
            return Err(error);
        }
        let txn_response = node.txn_response_at(entry.index).cloned();
        let committed_request = node
            .store()
            .committed_request(&entry.request_id)
            .cloned()
            .ok_or(KvError::MissingCommittedEntry { index: entry.index })?;
        let events = node.store().events_after_revision(before_revision)?;
        self.compact_node_if_needed(&mut node)?;
        let snapshot = node.store().try_snapshot()?;
        Ok(ClusterKvRaftCommitResult {
            snapshot: snapshot.into(),
            txn_response,
            events: events.into(),
            committed_request,
            observation_receipt: node.observation_receipt(),
        })
    }

    pub fn append_command(
        &self,
        node_id: &NodeId,
        command: KvCommand,
    ) -> Result<KvSnapshot, KvError> {
        self.append_command_with_response(node_id, command)
            .map(|result| Arc::unwrap_or_clone(result.snapshot))
    }

    pub fn append_command_with_response_and_publish<F>(
        &self,
        node_id: &NodeId,
        command: KvCommand,
        key_can_defer_materialized_snapshot: F,
    ) -> Result<ClusterKvRaftCommitResult, KvError>
    where
        F: FnMut(&str) -> bool,
    {
        let (result, _) = self.append_command_with_response_and_publish_report(
            node_id,
            command,
            key_can_defer_materialized_snapshot,
        )?;
        Ok(result)
    }

    pub fn append_command_with_response_and_publish_report<F>(
        &self,
        node_id: &NodeId,
        command: KvCommand,
        key_can_defer_materialized_snapshot: F,
    ) -> Result<(ClusterKvRaftCommitResult, bool), KvError>
    where
        F: FnMut(&str) -> bool,
    {
        let result = self.append_command_with_response(node_id, command)?;
        let wrote = self.publish_commit_result(&result, key_can_defer_materialized_snapshot)?;
        Ok((result, wrote))
    }

    pub fn append_command_and_publish<F>(
        &self,
        node_id: &NodeId,
        command: KvCommand,
        key_can_defer_materialized_snapshot: F,
    ) -> Result<KvSnapshot, KvError>
    where
        F: FnMut(&str) -> bool,
    {
        self.append_command_with_response_and_publish(
            node_id,
            command,
            key_can_defer_materialized_snapshot,
        )
        .map(|result| Arc::unwrap_or_clone(result.snapshot))
    }

    pub fn publish_commit_result<F>(
        &self,
        result: &ClusterKvRaftCommitResult,
        key_can_defer_materialized_snapshot: F,
    ) -> Result<bool, KvError>
    where
        F: FnMut(&str) -> bool,
    {
        let completion =
            KvObservationPublicationCompletion::new(result.observation_receipt.as_ref());
        let publication = (|| {
            let wrote = self.write_snapshot_after_commit(
                &result.snapshot,
                &result.events,
                key_can_defer_materialized_snapshot,
            )?;
            notify_cluster_kv_watchers_for_events(&self.paths, &result.events);
            let final_key_states =
                committed_final_key_states_from_snapshot(&result.snapshot, &result.events)?;
            publish_committed_observation(
                result.observation_receipt.as_ref(),
                result.snapshot.revision,
                result.snapshot.compacted_revision,
                Arc::clone(&result.events),
                Arc::new(final_key_states),
                None,
                None,
            )?;
            Ok(wrote)
        })();
        completion.finish(publication)
    }

    pub fn publish_append_entries_apply_outcome<F>(
        &self,
        outcome: &RaftAppendEntriesApplyOutcome,
        key_can_defer_materialized_snapshot: F,
    ) -> Result<bool, KvError>
    where
        F: FnMut(&str) -> bool,
    {
        let completion =
            KvObservationPublicationCompletion::new(outcome.observation_receipt.as_ref());
        let publication = (|| {
            if outcome.snapshot_restored {
                let wrote = write_cluster_kv_authoritative_snapshot_if_changed(
                    &self.paths,
                    &outcome.snapshot,
                )?;
                let states = committed_final_key_states_from_snapshot(
                    &outcome.snapshot,
                    &outcome.committed_events,
                )?;
                publish_committed_observation(
                    outcome.observation_receipt.as_ref(),
                    outcome.snapshot.revision,
                    outcome.snapshot.compacted_revision,
                    Arc::clone(&outcome.committed_events),
                    Arc::new(states),
                    Some(Arc::clone(
                        outcome
                            .restored_snapshot
                            .as_ref()
                            .unwrap_or(&outcome.snapshot),
                    )),
                    outcome.restored_snapshot_applied_index,
                )?;
                return Ok(wrote);
            }
            if outcome.committed_events.is_empty() {
                publish_committed_observation(
                    outcome.observation_receipt.as_ref(),
                    outcome.snapshot.revision,
                    outcome.snapshot.compacted_revision,
                    Arc::clone(&outcome.committed_events),
                    Arc::new(Vec::new()),
                    None,
                    None,
                )?;
                return Ok(false);
            }
            let wrote = self.write_snapshot_after_commit(
                &outcome.snapshot,
                &outcome.committed_events,
                key_can_defer_materialized_snapshot,
            )?;
            notify_cluster_kv_watchers_for_events(&self.paths, &outcome.committed_events);
            let states = committed_final_key_states_from_snapshot(
                &outcome.snapshot,
                &outcome.committed_events,
            )?;
            publish_committed_observation(
                outcome.observation_receipt.as_ref(),
                outcome.snapshot.revision,
                outcome.snapshot.compacted_revision,
                Arc::clone(&outcome.committed_events),
                Arc::new(states),
                None,
                None,
            )?;
            Ok(wrote)
        })();
        completion.finish(publication)
    }

    /// Publishes only state that was already materialized by follower apply.
    /// Heartbeats and ordinary appends therefore perform no complete-keyspace
    /// conversion or materialized snapshot rewrite.
    pub fn publish_compact_append_entries_apply_outcome(
        &self,
        outcome: &RaftAppendEntriesCompactApplyOutcome,
    ) -> Result<bool, KvError> {
        let completion =
            KvObservationPublicationCompletion::new(outcome.observation_receipt.as_ref());
        let publication = (|| {
            let wrote = if let Some(snapshot) = outcome.restored_snapshot.as_ref() {
                write_cluster_kv_authoritative_snapshot_if_changed(&self.paths, snapshot)?
            } else if let Some(snapshot) = outcome.checkpoint_snapshot.as_ref() {
                self.paths
                    .local_cache()
                    .write_raft_snapshot_if_changed(snapshot)?
            } else {
                false
            };
            if outcome.committed_events.is_empty() && outcome.commit_advanced {
                notify_cluster_kv_watchers(&self.paths);
            } else {
                notify_cluster_kv_watchers_for_events(&self.paths, &outcome.committed_events);
            }
            // An index describes a captured snapshot phase, never an absent
            // snapshot. Ordinary compact appends carry their native progress
            // through the receipt; restored snapshots retain their own index.
            let (snapshot, snapshot_applied_index) =
                if let Some(snapshot) = outcome.restored_snapshot.as_ref() {
                    (
                        Some(Arc::clone(snapshot)),
                        outcome.restored_snapshot_applied_index,
                    )
                } else if let Some(snapshot) = outcome.checkpoint_snapshot.as_ref() {
                    (
                        Some(Arc::clone(snapshot)),
                        Some(outcome.read_state.applied_index),
                    )
                } else {
                    (None, None)
                };
            publish_committed_observation(
                outcome.observation_receipt.as_ref(),
                outcome.read_state.revision,
                outcome.compacted_revision,
                Arc::clone(&outcome.committed_events),
                Arc::clone(&outcome.final_key_states),
                snapshot,
                snapshot_applied_index,
            )?;
            Ok(wrote)
        })();
        completion.finish(publication)
    }

    pub fn write_snapshot_after_commit<F>(
        &self,
        snapshot: &KvSnapshot,
        events: &[KvEvent],
        key_can_defer_materialized_snapshot: F,
    ) -> Result<bool, KvError>
    where
        F: FnMut(&str) -> bool,
    {
        if cluster_kv_events_can_defer_materialized_snapshot(
            events,
            key_can_defer_materialized_snapshot,
        ) && self.materialized_snapshot_is_fresh_enough(snapshot)
        {
            return Ok(false);
        }
        self.paths
            .local_cache()
            .write_raft_snapshot_if_changed(snapshot)
    }

    fn materialized_snapshot_is_fresh_enough(&self, snapshot: &KvSnapshot) -> bool {
        let Some(existing) = self
            .paths
            .local_cache()
            .cached_snapshot_arc_if_present()
            .ok()
            .flatten()
        else {
            return false;
        };
        cluster_kv_materialized_snapshot_is_fresh_enough_against(
            existing.revision,
            snapshot.revision,
            self.materialized_snapshot_age(),
        )
    }

    fn materialized_snapshot_age(&self) -> Option<Duration> {
        cluster_kv_materialized_snapshot_age(&self.paths)
    }

    pub fn append_command_uncommitted(
        &self,
        node_id: &NodeId,
        command: KvCommand,
    ) -> Result<KvLogEntry, KvError> {
        let mut node = self.open_node(node_id)?;
        self.append_command_to_open_node(&mut node, command)
    }

    pub fn replay_request_if_committed(
        &self,
        node_id: &NodeId,
        request: &KvCommandRequest,
    ) -> Result<Option<(KvSnapshot, KvCommittedRequestRecord)>, KvError> {
        let node = self.open_node(node_id)?;
        let _authority_lease = self.acquire_pending_higher_term_authority_lease(
            node_id,
            Some(node.hard_state().current_term),
            "read a committed Raft request",
        )?;
        let digest = request.digest()?;
        if node.store().request_id_expired(&request.request_id) {
            return Err(KvError::RequestExpired {
                request_id: request.request_id.clone(),
            });
        }
        let Some(record) = node.store().committed_request(&request.request_id) else {
            return Ok(None);
        };
        if record.command_digest != digest {
            return Err(KvError::RequestIdConflict {
                request_id: request.request_id.clone(),
            });
        }
        let record = record.clone();
        Ok(Some((node.store().try_snapshot()?, record)))
    }

    pub fn locked_replay_request_if_committed(
        &self,
        node_id: &NodeId,
        request: &KvCommandRequest,
    ) -> Result<Option<(KvSnapshot, KvCommittedRequestRecord)>, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.replay_request_if_committed(node_id, request)
    }

    pub fn append_request_uncommitted(
        &self,
        node_id: &NodeId,
        request: KvCommandRequest,
    ) -> Result<KvLogEntry, KvError> {
        // A published term fence is a definite pre-proposal rejection. Check
        // it before resident admission and, critically, before creating a
        // durable request-attempt record that would falsely make the outcome
        // look ambiguous.
        self.ensure_pending_higher_term_authority_available(
            node_id,
            "append a local Raft proposal",
        )?;
        let mut node = self.open_node(node_id)?;
        let preflight = (|| {
            let digest = request.digest()?;
            if node.store().request_id_expired(&request.request_id) {
                return Err(KvError::RequestExpired {
                    request_id: request.request_id.clone(),
                });
            }
            if let Some(existing) = node.store().committed_request(&request.request_id)
                && existing.command_digest != digest
            {
                return Err(KvError::RequestIdConflict {
                    request_id: request.request_id.clone(),
                });
            }
            node.node
                .preflight_local_append_request(node.hard_state().current_term, &request)
        })();
        if let Err(error) = preflight {
            let _ = mark_cluster_kv_request_rejected_before_commit(
                &self.paths,
                &request,
                error.to_string(),
            );
            return Err(error);
        }
        mark_cluster_kv_request_submitted(&self.paths, &request)?;
        let request_for_outcome = request.clone();
        match self.append_request_to_open_node(&mut node, request) {
            Ok(entry) => Ok(entry),
            Err(error) => {
                let _ = mark_cluster_kv_request_unknown(
                    &self.paths,
                    &request_for_outcome,
                    error.to_string(),
                );
                Err(error)
            }
        }
    }

    pub fn locked_append_request_uncommitted(
        &self,
        node_id: &NodeId,
        request: KvCommandRequest,
    ) -> Result<KvLogEntry, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.append_request_uncommitted(node_id, request)
    }

    pub fn append_request_uncommitted_or_replay(
        &self,
        node_id: &NodeId,
        request: KvCommandRequest,
    ) -> Result<ClusterKvRaftUncommittedAppend, KvError> {
        if let Some((snapshot, record)) = self.replay_request_if_committed(node_id, &request)? {
            return Ok(ClusterKvRaftUncommittedAppend::CommittedReplay { snapshot, record });
        }
        self.append_request_uncommitted(node_id, request)
            .map(ClusterKvRaftUncommittedAppend::Appended)
    }

    pub fn locked_append_request_uncommitted_or_replay(
        &self,
        node_id: &NodeId,
        request: KvCommandRequest,
    ) -> Result<ClusterKvRaftUncommittedAppend, KvError> {
        let _raft_guard = self.lock_operation()?;
        self.append_request_uncommitted_or_replay(node_id, request)
    }

    pub fn discard_uncommitted_tail(&self, node_id: &NodeId) -> Result<bool, KvError> {
        let mut node = self.open_node_discarding_uncommitted_tail(node_id)?;
        let discarded = node.discard_uncommitted_tail()?;
        self.compact_node_if_needed(&mut node)?;
        Ok(discarded)
    }

    /// Legacy step-down API; pending consensus history is always retained.
    pub fn discard_uncommitted_tail_and_clear_leader(
        &self,
        node_id: &NodeId,
    ) -> Result<KvSnapshot, KvError> {
        let mut node = self.open_node(node_id)?;
        node.clear_known_leader()?;
        self.compact_node_if_needed(&mut node)?;
        node.store().try_snapshot()
    }

    pub fn discard_uncommitted_tail_and_clear_leader_and_publish_report(
        &self,
        node_id: &NodeId,
    ) -> Result<(KvSnapshot, bool), KvError> {
        let snapshot = self.discard_uncommitted_tail_and_clear_leader(node_id)?;
        let wrote = self
            .paths
            .local_cache()
            .write_raft_snapshot_if_changed(&snapshot)?;
        Ok((snapshot, wrote))
    }

    pub fn locked_discard_uncommitted_tail_and_clear_leader_and_publish_report(
        &self,
        node_id: &NodeId,
    ) -> Result<(KvSnapshot, bool), KvError> {
        let _raft_guard = self.lock_operation()?;
        self.discard_uncommitted_tail_and_clear_leader_and_publish_report(node_id)
    }

    pub fn commit_index_with_events(
        &self,
        node_id: &NodeId,
        commit_index: u64,
    ) -> Result<ClusterKvRaftCommitResult, KvError> {
        let mut node = self.open_observed_node(node_id)?;
        self.commit_index_with_events_on_open_node(node_id, commit_index, &mut node)
    }

    fn commit_index_with_events_on_open_node(
        &self,
        node_id: &NodeId,
        commit_index: u64,
        node: &mut DurableRaftKvNode,
    ) -> Result<ClusterKvRaftCommitResult, KvError> {
        let _authority_lease = self.acquire_pending_higher_term_authority_lease(
            node_id,
            Some(node.hard_state().current_term),
            "commit a local Raft proposal",
        )?;
        let before_revision = node.store().revision();
        node.commit_to(commit_index)?;
        let txn_response = node.txn_response_at(commit_index).cloned();
        let events = node.store().events_after_revision(before_revision)?;
        self.compact_node_if_needed(node)?;
        let committed_request = node
            .log_entry_at(commit_index)
            .and_then(|entry| node.store().committed_request(&entry.request_id))
            .cloned()
            .ok_or(KvError::MissingCommittedEntry {
                index: commit_index,
            })?;
        let snapshot = node.store().try_snapshot()?;
        Ok(ClusterKvRaftCommitResult {
            snapshot: snapshot.into(),
            txn_response,
            events: events.into(),
            committed_request,
            observation_receipt: node.observation_receipt(),
        })
    }

    pub fn commit_index_with_events_and_publish_report<F>(
        &self,
        node_id: &NodeId,
        commit_index: u64,
        key_can_defer_materialized_snapshot: F,
    ) -> Result<(ClusterKvRaftCommitResult, bool), KvError>
    where
        F: FnMut(&str) -> bool,
    {
        let result = self.commit_index_with_events(node_id, commit_index)?;
        let wrote = self.publish_commit_result(&result, key_can_defer_materialized_snapshot)?;
        Ok((result, wrote))
    }

    pub fn locked_commit_index_with_events_and_publish_report<F>(
        &self,
        node_id: &NodeId,
        commit_index: u64,
        key_can_defer_materialized_snapshot: F,
    ) -> Result<(ClusterKvRaftCommitResult, bool), KvError>
    where
        F: FnMut(&str) -> bool,
    {
        let source =
            try_admit_committed_observation(&self.paths, KvObservationProposalOwnership::Absent)?;
        let mut owner = KvObservedOperationGuard {
            guard: None,
            fence: Some(source.storage_owner()?),
        };
        owner.guard = Some(self.lock_operation()?);
        let mut node = self.open_node(node_id)?;
        node.observation = Some(KvDurableObservationContext::new(source));
        let result = self.commit_index_with_events_on_open_node(node_id, commit_index, &mut node);
        drop(node);
        drop(owner);
        let result = result?;
        let wrote = self.publish_commit_result(&result, key_can_defer_materialized_snapshot)?;
        Ok((result, wrote))
    }

    fn append_command_to_open_node(
        &self,
        node: &mut DurableRaftKvNode,
        command: KvCommand,
    ) -> Result<KvLogEntry, KvError> {
        let _authority_lease = self.acquire_pending_higher_term_authority_lease(
            node.node_id(),
            Some(node.hard_state().current_term),
            "append a local Raft proposal",
        )?;
        let term = node.hard_state().current_term.max(1);
        node.append_local_command(term, command)
    }

    fn append_request_to_open_node(
        &self,
        node: &mut DurableRaftKvNode,
        request: KvCommandRequest,
    ) -> Result<KvLogEntry, KvError> {
        let _authority_lease = self.acquire_pending_higher_term_authority_lease(
            node.node_id(),
            Some(node.hard_state().current_term),
            "append a local Raft proposal",
        )?;
        let term = node.hard_state().current_term.max(1);
        node.append_local_request(term, request)
    }

    fn events_after_revision(
        snapshot: &KvSnapshot,
        before_revision: KvRevision,
    ) -> Result<Vec<KvEvent>, KvError> {
        let mut events = Vec::new();
        for event in snapshot
            .events
            .iter()
            .filter(|event| event.revision > before_revision)
        {
            try_reserve_kv_items(&mut events, 1, "collect committed raft events")?;
            events.push(event.clone());
        }
        Ok(events)
    }
}

fn committed_final_key_states_from_snapshot(
    snapshot: &KvSnapshot,
    events: &[KvEvent],
) -> Result<Vec<ClusterKvCommittedKeyState>, KvError> {
    let mut keys = events
        .iter()
        .map(|event| event.key.as_str())
        .collect::<Vec<_>>();
    keys.sort_unstable();
    keys.dedup();
    let mut states = Vec::new();
    try_reserve_kv_items(
        &mut states,
        keys.len(),
        "capture legacy committed final key states",
    )?;
    for key in keys {
        states.push(ClusterKvCommittedKeyState {
            key: key.to_string(),
            entry: snapshot.get(key).cloned(),
        });
    }
    Ok(states)
}

/// Borrowed results may outlive a failed publication attempt. Resolve their
/// own ordinal now so retaining that result cannot strand the next producer.
struct KvObservationPublicationCompletion<'a> {
    receipt: Option<&'a KvCommittedObservationReceipt>,
    finished: bool,
}

impl<'a> KvObservationPublicationCompletion<'a> {
    fn new(receipt: Option<&'a KvCommittedObservationReceipt>) -> Self {
        Self {
            receipt,
            finished: false,
        }
    }

    fn abandon_and_complete(&self) {
        if let Some(receipt) = self.receipt {
            let _ = receipt.abandon_publication();
            let _ = receipt.wait_delivered();
        }
    }

    fn finish<T>(mut self, result: Result<T, KvError>) -> Result<T, KvError> {
        if result.is_err() {
            // Preserve the original error. This delivery attempt is safety
            // completion after an effect, never a pre-effect retry signal.
            self.abandon_and_complete();
        }
        self.finished = true;
        result
    }
}

impl Drop for KvObservationPublicationCompletion<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.abandon_and_complete();
        }
    }
}

fn publish_committed_observation(
    receipt: Option<&KvCommittedObservationReceipt>,
    revision: KvRevision,
    compacted_revision: KvRevision,
    events: Arc<Vec<KvEvent>>,
    final_key_states: Arc<Vec<ClusterKvCommittedKeyState>>,
    snapshot: Option<Arc<KvSnapshot>>,
    snapshot_applied_index: Option<u64>,
) -> Result<(), KvError> {
    let Some(receipt) = receipt else {
        return Ok(());
    };
    ready_committed_observation(
        receipt,
        revision,
        compacted_revision,
        events,
        final_key_states,
        snapshot,
        snapshot_applied_index,
    )?;
    receipt.wait_delivered()
}

/// Retain the complete captured effect under its existing exact release
/// latches. This step never waits for delivery while source owners are active.
fn ready_committed_observation(
    receipt: &KvCommittedObservationReceipt,
    revision: KvRevision,
    compacted_revision: KvRevision,
    events: Arc<Vec<KvEvent>>,
    final_key_states: Arc<Vec<ClusterKvCommittedKeyState>>,
    snapshot: Option<Arc<KvSnapshot>>,
    snapshot_applied_index: Option<u64>,
) -> Result<(), KvError> {
    // A pure native restore already publishes its complete installed prefix.
    // Retain an empty Delta only when a real post-install applied-index step
    // advances that prefix; ordinary/checkpoint progress remains unchanged.
    let snapshot_covers_terminal = events.is_empty()
        && final_key_states.is_empty()
        && snapshot.as_ref().is_some_and(|snapshot| {
            snapshot.revision == revision
                && snapshot_applied_index.is_some_and(|index| {
                    receipt.replacement_covers_native_snapshot(snapshot.revision, index)
                })
        });
    let delta = (!snapshot_covers_terminal).then(|| {
        Arc::new(ClusterKvCommittedDelta {
            revision,
            compacted_revision,
            events,
            final_key_states,
        })
    });
    receipt.ready(Arc::new(KvCommittedObservationPayload {
        delta,
        snapshot,
        snapshot_applied_index,
        encoded_bytes: None,
    }))
}

pub struct ClusterKvRaftLocalCommandStorage<F>
where
    F: FnMut(&str) -> bool,
{
    store: ClusterKvRaftStore,
    node_id: NodeId,
    /// Optional end-to-end admission deadline owned by the async caller.
    /// Blocking-pool work must not remain queued behind the operation or
    /// resident guards after the future that requested it has timed out.
    operation_deadline: Option<Instant>,
    /// Callers with an independent recovery/forward path must never enqueue
    /// behind a local mutation merely to discover that the Raft node is busy.
    /// In this mode both the Raft operation gate and resident checkout are one
    /// atomic try-acquire.
    immediate_operation_admission: bool,
    /// ReadIndex preparation/final projection is observational and already
    /// caller-deadline-bound. It may wait for or adopt an existing resident
    /// recovery, but must not start an uninterruptible snapshot/WAL replay.
    require_existing_resident: bool,
    node: Option<DurableRaftKvNode>,
    operation_guard: Option<ClusterKvRaftOperationGuard>,
    /// Shared per-root authority admission retained for exactly the resident
    /// storage phase. Higher-term publication raises its floor immediately;
    /// its durable closer then drains this lease before mutating hard state.
    authority_lease: Option<PendingHigherRaftTermAuthorityLease>,
    /// The exact proposal durably appended by this command session.
    ///
    /// The resident node and its broad operation guard are deliberately
    /// released while quorum transport is in flight. Retaining this small
    /// identity lets every later storage phase prove that it reopened the same
    /// proposal under the same leadership before it builds another append or
    /// applies the quorum result.
    pending_entry: Option<KvLogEntry>,
    key_can_defer_materialized_snapshot: F,
    last_snapshot_publish_wrote: Option<bool>,
    observation_capacity: Option<KvObservationCapacityPermit>,
    observation_storage_fence: Option<KvObservationOwnerFence>,
    #[cfg(test)]
    expire_deadline_after_submission_for_test: bool,
}

impl<F> Drop for ClusterKvRaftLocalCommandStorage<F>
where
    F: FnMut(&str) -> bool,
{
    fn drop(&mut self) {
        self.release_resident_node_and_operation_guard();
    }
}

/// Timing captured while appending one local Raft request before quorum
/// replication begins. The observation is returned to the caller so emitting
/// diagnostics remains outside the KV correctness path.
#[derive(Debug)]
pub struct ClusterKvRaftObservedAppend {
    pub result: Result<KvLogEntry, KvError>,
    pub compare_duration: Duration,
    pub log_append_wal_duration: Option<Duration>,
}

impl<F> ClusterKvRaftLocalCommandStorage<F>
where
    F: FnMut(&str) -> bool,
{
    pub fn new(
        store: ClusterKvRaftStore,
        node_id: NodeId,
        key_can_defer_materialized_snapshot: F,
    ) -> Self {
        Self::new_with_optional_operation_deadline(
            store,
            node_id,
            key_can_defer_materialized_snapshot,
            None,
            false,
            false,
        )
    }

    pub fn new_with_operation_deadline(
        store: ClusterKvRaftStore,
        node_id: NodeId,
        key_can_defer_materialized_snapshot: F,
        operation_deadline: Instant,
    ) -> Self {
        Self::new_with_optional_operation_deadline(
            store,
            node_id,
            key_can_defer_materialized_snapshot,
            Some(operation_deadline),
            false,
            false,
        )
    }

    /// Creates a general command session that may hydrate a cold node after
    /// immediate operation admission, but never queues behind an existing
    /// operation owner. This is used by the reactive higher-term closer: a
    /// term-bearing AppendEntries already in the critical section can close the
    /// same floor, so the background worker must yield instead of queueing and
    /// reopening the resident node after that request returns.
    pub fn new_with_immediate_operation_admission(
        store: ClusterKvRaftStore,
        node_id: NodeId,
        key_can_defer_materialized_snapshot: F,
    ) -> Self {
        Self::new_with_optional_operation_deadline(
            store,
            node_id,
            key_can_defer_materialized_snapshot,
            None,
            false,
            true,
        )
    }

    pub fn new_read_index_with_operation_deadline(
        store: ClusterKvRaftStore,
        node_id: NodeId,
        key_can_defer_materialized_snapshot: F,
        operation_deadline: Instant,
    ) -> Self {
        Self::new_with_optional_operation_deadline(
            store,
            node_id,
            key_can_defer_materialized_snapshot,
            Some(operation_deadline),
            true,
            false,
        )
    }

    pub fn new_read_index_with_immediate_operation_admission(
        store: ClusterKvRaftStore,
        node_id: NodeId,
        key_can_defer_materialized_snapshot: F,
    ) -> Self {
        Self::new_with_optional_operation_deadline(
            store,
            node_id,
            key_can_defer_materialized_snapshot,
            None,
            true,
            true,
        )
    }

    fn new_with_optional_operation_deadline(
        store: ClusterKvRaftStore,
        node_id: NodeId,
        key_can_defer_materialized_snapshot: F,
        operation_deadline: Option<Instant>,
        require_existing_resident: bool,
        immediate_operation_admission: bool,
    ) -> Self {
        Self {
            store,
            node_id,
            operation_deadline,
            immediate_operation_admission,
            require_existing_resident,
            node: None,
            operation_guard: None,
            authority_lease: None,
            pending_entry: None,
            key_can_defer_materialized_snapshot,
            last_snapshot_publish_wrote: None,
            observation_capacity: None,
            observation_storage_fence: None,
            #[cfg(test)]
            expire_deadline_after_submission_for_test: false,
        }
    }

    pub fn with_observation_capacity(mut self, capacity: KvObservationCapacityPermit) -> Self {
        self.observation_capacity = Some(capacity);
        self
    }

    pub fn proposal_custody_handle(&self) -> Option<crate::KvObservationProposalCustodyHandle> {
        self.observation_capacity
            .as_ref()
            .map(KvObservationCapacityPermit::proposal_custody_handle)
    }

    /// Ends the admitted terminal apply's physical/session/source ownership.
    /// Retain exact pending identity for existing post-apply protocol steps;
    /// their normal `release_session_after_commit` remains idempotent.
    pub fn release_session_after_admitted_apply(&mut self) {
        self.release_resident_node_and_operation_guard();
        self.observation_capacity = None;
    }

    pub fn observation_proposal_fence(&self) -> Result<Option<KvObservationOwnerFence>, KvError> {
        self.observation_capacity
            .as_ref()
            .map(|source| source.proposal_owner())
            .transpose()
    }

    fn remaining_operation_wait(&self) -> Result<Option<Duration>, KvError> {
        if self.immediate_operation_admission {
            return Ok(Some(Duration::ZERO));
        }
        let Some(deadline) = self.operation_deadline else {
            return Ok(None);
        };
        let wait = deadline.saturating_duration_since(Instant::now());
        if wait.is_zero() {
            return Err(KvError::Busy {
                path: self
                    .store
                    .wal_paths(&self.node_id)
                    .root
                    .display()
                    .to_string(),
            });
        }
        Ok(Some(wait))
    }

    fn open_node_if_missing(&mut self, discard_uncommitted_tail: bool) -> Result<(), KvError> {
        if self.node.is_none() {
            let operation_guard = if self.operation_guard.is_none() {
                Some(match self.remaining_operation_wait()? {
                    Some(wait) => self.store.lock_operation_with_wait(wait)?,
                    None => self.store.lock_operation()?,
                })
            } else {
                None
            };
            let node = match self.remaining_operation_wait()? {
                Some(wait) if self.require_existing_resident => self
                    .store
                    .open_existing_resident_node_with_wait(&self.node_id, wait)?,
                Some(wait) if discard_uncommitted_tail => self
                    .store
                    .open_resident_node_discarding_uncommitted_tail_with_wait(
                        &self.node_id,
                        wait,
                    )?,
                Some(wait) => self
                    .store
                    .open_resident_node_with_wait(&self.node_id, wait)?,
                None if discard_uncommitted_tail => self
                    .store
                    .open_resident_node_discarding_uncommitted_tail(&self.node_id)?,
                None => self.store.open_resident_node(&self.node_id)?,
            };
            let mut node = node;
            if let Some(capacity) = &self.observation_capacity {
                let fence = capacity.storage_owner()?;
                node.observation = Some(KvDurableObservationContext::new(capacity.clone()));
                self.observation_storage_fence = Some(fence);
            }
            if let Some(operation_guard) = operation_guard {
                self.operation_guard = Some(operation_guard);
            }
            self.node = Some(node);
        }
        Ok(())
    }

    /// Opens the durable node once for one local storage phase.
    ///
    /// A quorum proposal retains the much narrower commit-sequence lock across
    /// transport, but checks this resident node back in after durable append
    /// and after each immutable append-request build. This mirrors etcd's
    /// proposal/Ready/apply split: network latency must not monopolize the
    /// local Raft storage guard needed by reads and diagnostics.
    fn node_mut_with_open_mode(
        &mut self,
        discard_uncommitted_tail: bool,
    ) -> Result<&mut DurableRaftKvNode, KvError> {
        // Recheck even when this command session already retains a resident
        // node and lease. The lease protects one in-flight local action from a
        // publication race; it must not let a later method call on the same
        // hydrated session continue using authority after that publication.
        // The retained lease acquired below remains the actual linearization
        // point when this preflight races with a new observation.
        self.store.ensure_pending_higher_term_authority_available(
            &self.node_id,
            "use local Raft authority",
        )?;
        self.open_node_if_missing(discard_uncommitted_tail)?;
        let durable_term = self
            .node
            .as_ref()
            .map(|node| node.hard_state().current_term)
            .ok_or_else(|| KvError::Io {
                path: self
                    .store
                    .wal_paths(&self.node_id)
                    .root
                    .display()
                    .to_string(),
                detail: "cluster KV Raft command session did not retain its node".to_string(),
            })?;
        // Acquire after operation/resident admission and retain through the
        // complete local phase. Publication never waits here: it raises the
        // floor immediately, while the mandatory closer drains this already
        // admitted phase before persisting the new term.
        if self.authority_lease.is_none() {
            self.authority_lease = Some(self.store.acquire_pending_higher_term_authority_lease(
                &self.node_id,
                Some(durable_term),
                "use local Raft authority",
            )?);
        }
        self.node.as_mut().ok_or_else(|| KvError::Io {
            path: self
                .store
                .wal_paths(&self.node_id)
                .root
                .display()
                .to_string(),
            detail: "cluster KV Raft command session did not retain its node".to_string(),
        })
    }

    fn node_mut(&mut self) -> Result<&mut DurableRaftKvNode, KvError> {
        let _checkout = tracing::trace_span!(
            target: "sinomo.perf", "kv.raft.resident_checkout",
            stage = "kv.raft.resident_checkout", root = %self.store.paths().root.display(),
            node = %self.node_id,
        )
        .entered();
        self.node_mut_with_open_mode(false)
    }

    /// Checks the resident node back in while retaining the exact pending
    /// proposal identity for the post-quorum apply phase.
    pub fn release_session_for_quorum(&mut self) {
        self.release_resident_node_and_operation_guard();
    }

    /// Ends the operation-scoped resident session after a durable commit.
    /// Drop the node first so its final resident-state handoff is complete
    /// before releasing the broader operation lock, then forget the proposal
    /// identity only after the terminal local phase has completed.
    pub fn release_session_after_commit(&mut self) {
        self.release_resident_node_and_operation_guard();
        self.pending_entry = None;
        // Later observational or independently-mutating phases must not reuse
        // the sealed source of this completed effect.
        self.observation_capacity = None;
    }

    pub fn local_is_leader(&mut self, state: &ClusterBootstrapState) -> Result<bool, KvError> {
        let local_node_id = state.local_node_id.clone();
        let node = self.node_mut()?;
        Ok(cluster_kv_raft_local_is_voter(state)
            && node.hard_state().current_term == state.term
            && node.hard_state().leader_id.as_ref() == Some(&local_node_id))
    }

    pub fn last_log_index(&mut self) -> Result<u64, KvError> {
        self.node_mut().map(|node| node.last_log_index())
    }

    pub fn read_index_state(&mut self) -> Result<ClusterKvRaftReadIndexState, KvError> {
        self.node_mut()
            .map(|node| read_index_state_from_open_node(node))
    }

    /// Materializes a complete wire snapshot only when the ReadIndex caller
    /// actually needs one. Point and prefix callers should use the indexed
    /// helpers below and avoid this `O(total retained state)` boundary.
    pub fn read_index_snapshot(&mut self) -> Result<KvSnapshot, KvError> {
        self.node_mut()?.store().try_snapshot()
    }

    pub fn read_index_entry(&mut self, key: &str) -> Result<Option<KvEntry>, KvError> {
        self.node_mut()?
            .store()
            .get(key)
            .map(|entry| try_clone_kv_entry(entry, "read resident Raft KV entry"))
            .transpose()
    }

    /// Looks up an ordered, bounded exact-key set from one resident Raft view.
    /// The caller establishes ReadIndex once around this projection, so values
    /// cannot straddle revisions while unrelated keys are changing.
    pub fn read_index_entries(&mut self, keys: &[String]) -> Result<Vec<Option<KvEntry>>, KvError> {
        if keys.len() > CLUSTER_KV_AUTHORITATIVE_ENTRIES_MAX_KEYS {
            return Err(KvError::ResourceExhausted {
                operation: "read resident Raft exact-key set cardinality".to_string(),
                requested_bytes: keys.len(),
            });
        }
        let store = self.node_mut()?.store();
        let mut entries = Vec::new();
        entries
            .try_reserve(keys.len())
            .map_err(|_| KvError::ResourceExhausted {
                operation: "read resident Raft exact-key set".to_string(),
                requested_bytes: keys
                    .len()
                    .saturating_mul(std::mem::size_of::<Option<KvEntry>>()),
            })?;
        let mut payload_bytes = 0_usize;
        for key in keys {
            let entry = store
                .get(key)
                .map(|entry| {
                    payload_bytes = payload_bytes.saturating_add(kv_entry_payload_bytes(entry));
                    try_clone_kv_entry(entry, "read resident Raft exact-key set")
                })
                .transpose()?;
            if payload_bytes > CLUSTER_KV_AUTHORITATIVE_ENTRIES_MAX_PAYLOAD_BYTES {
                return Err(KvError::ResourceExhausted {
                    operation: "read resident Raft exact-key set payload".to_string(),
                    requested_bytes: payload_bytes,
                });
            }
            entries.push(entry);
        }
        Ok(entries)
    }

    /// Looks up only the bounded durable request-retention indexes needed by a
    /// request-outcome ReadIndex. A missing result deliberately says nothing
    /// about the node-local attempt ledger; callers must establish authority
    /// first and consult that ledger only after committed history is ruled out.
    pub fn read_index_request_outcome(
        &mut self,
        request_id: &KvRequestId,
    ) -> Result<Option<KvRequestOutcome>, KvError> {
        let store = self.node_mut()?.store();
        if let Some(record) = store.committed_request(request_id) {
            return Ok(Some(KvRequestOutcome::Committed(record.clone())));
        }
        if store.request_id_expired(request_id) {
            return Ok(Some(KvRequestOutcome::Expired));
        }
        Ok(None)
    }

    pub fn read_index_prefix(&mut self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
        self.node_mut()?.store().range_prefix(prefix)
    }

    pub fn read_index_prefix_limited_by_payload_bytes(
        &mut self,
        prefix: &str,
        max_entries: usize,
        max_payload_bytes: usize,
    ) -> Result<Vec<KvEntry>, KvError> {
        self.node_mut()?
            .store()
            .range_prefix_limited_by_payload_bytes(prefix, max_entries, max_payload_bytes)
    }

    /// Revalidates the exact durable proposal after a quorum-network gap.
    ///
    /// Incoming vote/append handling is allowed to use the local Raft storage
    /// while this proposal waits on the network. Consequently, reacquiring the
    /// operation guard is not enough: a newer term or leader may have replaced
    /// the log slot. Only the same entry under the same local leadership may be
    /// replicated further or applied from this proposal's quorum result.
    fn ensure_pending_entry_is_still_authoritative(
        &mut self,
        operation: &'static str,
    ) -> Result<(), KvError> {
        let Some(pending) = self.pending_entry.clone() else {
            return Ok(());
        };
        let local_node_id = self.node_id.clone();
        let (current_term, leader_id, current_entry) = {
            let node = self.node_mut()?;
            (
                node.hard_state().current_term,
                node.hard_state().leader_id.clone(),
                node.log_entry_at(pending.index).cloned(),
            )
        };
        if current_term == pending.term
            && leader_id.as_ref() == Some(&local_node_id)
            && current_entry.as_ref() == Some(&pending)
        {
            return Ok(());
        }

        let detail = format!(
            "{operation} refused stale Raft proposal at term {} index {}: current term {}, leader {}, log entry {}",
            pending.term,
            pending.index,
            current_term,
            leader_id
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "none".to_string()),
            if current_entry.is_some() {
                "differs"
            } else {
                "missing"
            }
        );
        self.release_session_for_quorum();
        let _ = mark_cluster_kv_request_unknown(
            self.store.paths(),
            &KvCommandRequest {
                request_id: pending.request_id.clone(),
                command: pending.command.clone(),
            },
            detail.clone(),
        );
        Err(KvError::AuthorityUnavailable { detail })
    }

    pub fn build_cluster_raft_append_request_from_next_index(
        &mut self,
        state: &ClusterBootstrapState,
        next_index: u64,
    ) -> Result<ClusterRaftAppendRequest, KvError> {
        let result = (|| {
            self.ensure_pending_entry_is_still_authoritative("build Raft append request")?;
            let node = self.node_mut()?;
            let request = build_append_entries_request_from_open_node(
                node,
                &state.local_node_id,
                next_index,
            )?;
            Ok(ClusterRaftAppendRequest {
                state: state.clone(),
                snapshot: request.snapshot,
                append: request.append,
                read_index_probe: false,
            })
        })();
        // The request owns its snapshot/tail bytes. No storage or resident-node
        // borrow is needed while the transport waits for the remote voter.
        self.release_session_for_quorum();
        result
    }

    /// Builds the mutation-free heartbeat used to confirm a linearizable
    /// ReadIndex directly from the resident committed anchor.
    ///
    /// Unlike the ordinary replication builder, this never walks or clones the
    /// uncommitted log tail and can never materialize a snapshot. The caller
    /// must already have captured `commit_index` from this operation-fenced
    /// resident session.
    pub fn build_cluster_raft_read_index_heartbeat(
        &mut self,
        state: &ClusterBootstrapState,
        commit_index: u64,
    ) -> Result<ClusterRaftAppendRequest, KvError> {
        let result = (|| {
            let node = self.node_mut()?;
            build_cluster_raft_read_index_heartbeat_from_open_node(node, state, commit_index)
        })();
        // The returned heartbeat owns its bounded state projection. Network
        // confirmation must not retain resident Raft or operation ownership.
        self.release_session_for_quorum();
        result
    }

    /// Returns whether `next_index` normalizes to the resident node's current
    /// snapshot-plus-tail append payload. Callers may use this only while they
    /// retain the same operation-fenced command-storage session that builds
    /// the corresponding request.
    pub fn cluster_raft_append_request_uses_snapshot(
        &mut self,
        next_index: u64,
    ) -> Result<bool, KvError> {
        let node = self.node_mut()?;
        Ok(append_entries_request_uses_snapshot(node, next_index))
    }

    pub fn take_last_snapshot_publish_wrote(&mut self) -> Option<bool> {
        self.last_snapshot_publish_wrote.take()
    }

    pub fn append_uncommitted_or_replay(
        &mut self,
        request: KvCommandRequest,
    ) -> Result<ClusterKvRaftUncommittedAppend, KvError> {
        self.last_snapshot_publish_wrote = None;
        if let Some((snapshot, record)) = self.replay_if_committed(&request)? {
            return Ok(ClusterKvRaftUncommittedAppend::CommittedReplay { snapshot, record });
        }
        self.append_uncommitted(request)
            .map(ClusterKvRaftUncommittedAppend::Appended)
    }

    pub fn replay_if_committed(
        &mut self,
        request: &KvCommandRequest,
    ) -> Result<Option<(KvSnapshot, KvCommittedRequestRecord)>, KvError> {
        let Some(record) = self.replay_record_if_committed(request)? else {
            return Ok(None);
        };
        let snapshot = self.read_index_snapshot()?;
        self.release_resident_node_and_operation_guard();
        Ok(Some((snapshot, record)))
    }

    pub fn replay_record_if_committed(
        &mut self,
        request: &KvCommandRequest,
    ) -> Result<Option<KvCommittedRequestRecord>, KvError> {
        self.last_snapshot_publish_wrote = None;
        let digest = request.digest()?;
        let record = {
            // Exact replay is observational. In particular, an ambiguous
            // quorum result may have left an entry that another voter can
            // still commit; looking up the request must never truncate it.
            let node = self.node_mut()?;
            if node.store().request_id_expired(&request.request_id) {
                return Err(KvError::RequestExpired {
                    request_id: request.request_id.clone(),
                });
            }
            let Some(record) = node.store().committed_request(&request.request_id) else {
                return Ok(None);
            };
            if record.command_digest != digest {
                return Err(KvError::RequestIdConflict {
                    request_id: request.request_id.clone(),
                });
            }
            record.clone()
        };

        // Exact replay is a resident Raft lookup. The shared materialized file
        // is a derived observational checkpoint, so neither a missing nor a
        // stale copy can turn replay into an O(total-state) serialization/fsync
        // on the correctness path.
        self.release_resident_node_and_operation_guard();
        if self
            .pending_entry
            .as_ref()
            .is_some_and(|pending| pending.request_id == record.request_id)
        {
            self.pending_entry = None;
        }
        Ok(Some(record))
    }

    pub(crate) fn release_resident_node_and_operation_guard(&mut self) {
        // Check the decoded node back into the resident slot before releasing
        // the wider process operation permit. A following command can then
        // acquire both without replaying durable state.
        drop(self.node.take());
        drop(self.authority_lease.take());
        drop(self.operation_guard.take());
        // The exact owner fence is released only after the actual node,
        // higher-term lease and operation guard have all been dropped.
        drop(self.observation_storage_fence.take());
    }

    /// Validates one proposal against the exact resident term, log suffix,
    /// committed state-machine view, and pending-config fence that would own
    /// its WAL slot. The async mutation path intentionally invokes this both
    /// before and after request-attempt durability: the ledger fsync runs with
    /// resident/operation ownership released, so incoming Raft traffic may
    /// legitimately change every one of these inputs in between.
    fn preflight_uncommitted_request(&mut self, request: &KvCommandRequest) -> Result<(), KvError> {
        let local_node_id = self.node_id.clone();
        let pending = self.pending_entry.clone();
        let node = self.node_mut()?;
        let unapplied = node
            .node
            .log
            .iter()
            .filter(|entry| entry.index > node.node.applied_index);
        let current_term = node.hard_state().current_term.max(1);
        let proposal_is_barrier = matches!(&request.command, KvCommand::Barrier);
        let mut pending_configs = unapplied
            .clone()
            .filter(|entry| command_may_mutate_cluster_raft_config(&entry.command));
        let pending_config = pending_configs.next();
        if let Some(second) = pending_configs.next() {
            return Err(KvError::InvalidRaftConfigTransition {
                detail: format!(
                    "durable Raft suffix contains multiple unapplied cluster membership changes; second change is at term {} index {}",
                    second.term, second.index
                ),
            });
        }
        if let Some(pending_config) = pending_config {
            // A new leader must still be able to append exactly one
            // current-term no-op so the inherited old-term config can become
            // committed. No data/config proposal may be evaluated using the
            // stale pre-change voter view.
            if !proposal_is_barrier || pending_config.term >= current_term {
                return Err(KvError::AuthorityUnavailable {
                    detail: "refused a new Raft proposal while an unapplied cluster membership change is pending"
                        .to_string(),
                });
            }
        }
        if proposal_is_barrier
            && unapplied.clone().any(|entry| {
                entry.term == current_term && matches!(&entry.command, KvCommand::Barrier)
            })
        {
            return Err(KvError::AuthorityUnavailable {
                detail: format!(
                    "refused duplicate uncommitted Raft recovery barrier in term {current_term}"
                ),
            });
        }
        // Bound the pipelined suffix just as etcd bounds `uncommittedSize`.
        // Empty current-term barriers do not consume the mutation quota;
        // exactly one per term remains available to commit a full old-term
        // suffix.
        let unapplied_count = unapplied
            .clone()
            .filter(|entry| !matches!(&entry.command, KvCommand::Barrier))
            .count();
        if !proposal_is_barrier && unapplied_count >= MAX_CLUSTER_KV_UNCOMMITTED_ENTRIES {
            return Err(KvError::ResourceExhausted {
                operation: "admit cluster KV uncommitted Raft entry count".to_string(),
                requested_bytes: unapplied_count.saturating_add(1),
            });
        }
        let unapplied_payload_bytes = unapplied
            .map(|entry| command_payload_bytes(&entry.command))
            .fold(0_usize, usize::saturating_add);
        let proposed_payload_bytes =
            unapplied_payload_bytes.saturating_add(command_payload_bytes(&request.command));
        if !proposal_is_barrier
            && unapplied_count > 0
            && proposed_payload_bytes > MAX_CLUSTER_KV_UNCOMMITTED_PAYLOAD_BYTES
        {
            return Err(KvError::ResourceExhausted {
                operation: "admit cluster KV uncommitted Raft payload".to_string(),
                requested_bytes: proposed_payload_bytes,
            });
        }
        if let Some(pending) = pending {
            let pending_is_exact_and_uncommitted = node.hard_state().commit_index < pending.index
                && node.log_entry_at(pending.index) == Some(&pending);
            if pending_is_exact_and_uncommitted {
                let current_term = node.hard_state().current_term;
                let local_is_leader = node.hard_state().leader_id.as_ref() == Some(&local_node_id);
                if current_term <= pending.term || !local_is_leader {
                    return Err(KvError::AuthorityUnavailable {
                        detail: format!(
                            "refused to supersede unresolved Raft proposal at term {} index {} while current term is {} and leader is {}",
                            pending.term,
                            pending.index,
                            current_term,
                            node.hard_state()
                                .leader_id
                                .as_ref()
                                .map(ToString::to_string)
                                .unwrap_or_else(|| "none".to_string())
                        ),
                    });
                }
                // A verified newer-term local leader appends after the
                // preserved entry. Committing the new tail can also commit the
                // earlier, possibly replicated request.
            }
        }
        // A barrier has no state-machine semantics of its own. Every inherited
        // entry was preflighted when proposed, so replaying the whole suffix
        // again here would recreate quadratic recovery work.
        if proposal_is_barrier {
            node.node.preflight_local_barrier_request(request)
        } else {
            node.node
                .preflight_local_append_request(current_term, request)
        }
    }

    fn close_exact_submission_rejected_before_wal(
        &mut self,
        request: &KvCommandRequest,
        submission: Option<&crate::request::ClusterKvRequestAttemptSubmissionReceipt>,
        reason: &str,
    ) -> Result<(), KvError> {
        self.release_session_for_quorum();
        let Some(submission) = submission else {
            // The Submitted state predated this call, so another node or an
            // earlier invocation may already have observed the request. Its
            // ambiguity cannot be collapsed by this local pre-WAL rejection.
            return Ok(());
        };
        if crate::request::mark_cluster_kv_request_rejected_after_exact_submission(
            self.store.paths(),
            request,
            submission,
            reason,
        )? {
            return Ok(());
        }
        Err(KvError::AuthorityUnavailable {
            detail: format!(
                "could not close exact Submitted request {} as rejected before WAL append because its attempt record changed concurrently",
                request.request_id
            ),
        })
    }

    pub fn append_uncommitted(&mut self, request: KvCommandRequest) -> Result<KvLogEntry, KvError> {
        self.append_uncommitted_with_timing(request, false).result
    }

    pub fn append_uncommitted_observed(
        &mut self,
        request: KvCommandRequest,
    ) -> ClusterKvRaftObservedAppend {
        self.append_uncommitted_with_timing(request, true)
    }

    fn append_uncommitted_with_timing(
        &mut self,
        request: KvCommandRequest,
        capture_timing: bool,
    ) -> ClusterKvRaftObservedAppend {
        self.last_snapshot_publish_wrote = None;
        let compare_started = capture_timing.then(Instant::now);
        if matches!(&request.command, KvCommand::Barrier) {
            let adoption = self
                .node_mut()
                .and_then(|node| node.node.pending_current_term_barrier());
            match adoption {
                Ok(Some(existing)) if existing.request_id == request.request_id => {
                    // This proposal is already durably present. The caller's
                    // admission deadline must not cancel its subsequent
                    // quorum reconciliation or local apply.
                    self.operation_deadline = None;
                    self.pending_entry = Some(existing.clone());
                    return ClusterKvRaftObservedAppend {
                        result: Ok(existing),
                        compare_duration: compare_started
                            .map(|started| started.elapsed())
                            .unwrap_or_default(),
                        log_append_wal_duration: Some(Duration::ZERO),
                    };
                }
                Ok(_) => {}
                Err(error) => {
                    return ClusterKvRaftObservedAppend {
                        result: Err(error),
                        compare_duration: compare_started
                            .map(|started| started.elapsed())
                            .unwrap_or_default(),
                        log_append_wal_duration: None,
                    };
                }
            }
        }
        let preflight = self.preflight_uncommitted_request(&request);
        let compare_duration = compare_started
            .map(|started| started.elapsed())
            .unwrap_or_default();
        if let Err(error) = preflight {
            // Recording a definite rejection is itself a durable ledger
            // mutation. If preflight consumed the caller's remaining budget,
            // return the admission timeout without starting even that write.
            if let Err(deadline_error) = self.remaining_operation_wait() {
                self.release_session_for_quorum();
                return ClusterKvRaftObservedAppend {
                    result: Err(deadline_error),
                    compare_duration,
                    log_append_wal_duration: None,
                };
            }
            let operation_deadline = self.operation_deadline;
            self.release_session_for_quorum();
            let _ = match operation_deadline {
                Some(deadline) => mark_cluster_kv_request_rejected_before_commit_before(
                    self.store.paths(),
                    &request,
                    error.to_string(),
                    deadline,
                ),
                None => mark_cluster_kv_request_rejected_before_commit(
                    self.store.paths(),
                    &request,
                    error.to_string(),
                ),
            };
            return ClusterKvRaftObservedAppend {
                result: Err(error),
                compare_duration,
                log_append_wal_duration: None,
            };
        }
        // Acquiring the operation/resident guards before the caller deadline
        // does not grant an unbounded right to mutate later. Bounded preflight
        // can consume the remaining budget, so recheck immediately before the
        // first durable request-attempt or Raft-WAL write. This timeout is a
        // definite pre-append rejection and therefore intentionally does not
        // touch the durable attempt ledger.
        if let Err(error) = self.remaining_operation_wait() {
            self.release_session_for_quorum();
            return ClusterKvRaftObservedAppend {
                result: Err(error),
                compare_duration,
                log_append_wal_duration: None,
            };
        }
        // The request-attempt fsync has its own lock and can legitimately wait
        // behind another process. It must never monopolize the resident node
        // or operation admission needed by ReadIndex and inbound Raft RPCs.
        let operation_deadline = self.operation_deadline;
        self.release_session_for_quorum();
        let submission = match crate::request::mark_cluster_kv_request_submitted_with_receipt(
            self.store.paths(),
            &request,
            operation_deadline,
        ) {
            Ok(submission) => submission,
            Err(error) => {
                return ClusterKvRaftObservedAppend {
                    result: Err(error),
                    compare_duration,
                    log_append_wal_duration: None,
                };
            }
        };
        #[cfg(test)]
        if std::mem::take(&mut self.expire_deadline_after_submission_for_test) {
            self.operation_deadline = Some(Instant::now());
        }

        // Reopen and repeat the complete preflight before assigning the WAL
        // slot. The released interval above permits a follower append, vote,
        // apply, or higher-term fence to change leadership, the committed KV
        // view, or the pending configuration.
        if let Err(error) = self.preflight_uncommitted_request(&request) {
            let terminal = self.close_exact_submission_rejected_before_wal(
                &request,
                submission.as_ref(),
                &error.to_string(),
            );
            return ClusterKvRaftObservedAppend {
                result: terminal.map_or_else(Err, |_| Err(error)),
                compare_duration,
                log_append_wal_duration: None,
            };
        }
        if let Err(error) = self.remaining_operation_wait() {
            let terminal = self.close_exact_submission_rejected_before_wal(
                &request,
                submission.as_ref(),
                &error.to_string(),
            );
            return ClusterKvRaftObservedAppend {
                result: terminal.map_or_else(Err, |_| Err(error)),
                compare_duration,
                log_append_wal_duration: None,
            };
        }

        let log_append_wal_started = capture_timing.then(Instant::now);
        let request_for_outcome = request.clone();
        let result: Result<KvLogEntry, KvError> = (|| {
            let term = self.node_mut()?.hard_state().current_term.max(1);
            // An uncommitted append advances none of the compaction progress
            // inputs. Keep the post-WAL path O(delta) and infallible so a
            // durable entry can never be returned as a pre-append error before
            // `pending_entry` records its exact identity. The commit/apply
            // phase owns the explicit periodic checkpoint check.
            self.node_mut()?.append_local_request(term, request)
        })();
        // Once append has been attempted, the outcome can be ambiguous and may
        // already be replicated or committed elsewhere. Never let the original
        // admission deadline abort the mandatory reconciliation/apply phase.
        self.operation_deadline = None;
        let log_append_wal_duration = log_append_wal_started
            .map(|started| started.elapsed())
            .unwrap_or_default();
        if let Err(error) = &result {
            // A WAL error is ambiguous durability and therefore mandatory
            // reconciliation, but the ledger's independent 30-second safety
            // budget must run only after Raft resident/operation ownership is
            // returned. The async caller supervises this admitted blocking
            // phase to completion even if its response future is cancelled.
            self.release_session_for_quorum();
            let _ = mark_cluster_kv_request_unknown(
                self.store.paths(),
                &request_for_outcome,
                error.to_string(),
            );
        } else if let Ok(entry) = &result {
            self.pending_entry = Some(entry.clone());
        }
        ClusterKvRaftObservedAppend {
            result,
            compare_duration,
            log_append_wal_duration: Some(log_append_wal_duration),
        }
    }

    #[cfg(test)]
    pub(crate) fn expire_operation_deadline_before_append_for_test(&mut self) {
        self.operation_deadline = Some(Instant::now());
    }

    #[cfg(test)]
    pub(crate) fn expire_operation_deadline_after_submission_for_test(&mut self) {
        self.expire_deadline_after_submission_for_test = true;
    }

    #[cfg(test)]
    pub(crate) fn operation_deadline_is_armed_for_test(&self) -> bool {
        self.operation_deadline.is_some()
    }

    /// Finds the unique durable current-term no-op so a restarted cold-read
    /// singleflight can adopt its stable request identity instead of proposing
    /// a duplicate barrier.
    pub fn pending_current_term_barrier(&mut self) -> Result<Option<KvLogEntry>, KvError> {
        self.node_mut()?.node.pending_current_term_barrier()
    }

    pub fn record_rejected_before_commit(
        &mut self,
        request: &KvCommandRequest,
        failure: &KvAuthorityFailure,
    ) -> Result<(), KvError> {
        mark_cluster_kv_request_rejected_before_commit(
            self.store.paths(),
            request,
            failure.detail.clone(),
        )
    }

    pub fn record_unknown(
        &mut self,
        request: &KvCommandRequest,
        detail: &str,
    ) -> Result<(), KvError> {
        mark_cluster_kv_request_unknown(self.store.paths(), request, detail.to_string())
    }

    pub fn discard_uncommitted_tail_and_clear_leader(&mut self) -> Result<KvSnapshot, KvError> {
        self.last_snapshot_publish_wrote = None;
        self.discard_uncommitted_tail_and_clear_leader_compact()?;
        // Compact cleanup may already have checked the node in so network
        // failure handling cannot retain the broad guard. Reopen only for the
        // legacy full-snapshot return value, then release again below.
        let snapshot = self.node_mut()?.store().try_snapshot()?;
        let store = self.store.clone();
        let wrote = store
            .paths()
            .local_cache()
            .write_raft_snapshot_if_changed(&snapshot)?;
        self.last_snapshot_publish_wrote = Some(wrote);
        self.release_resident_node_and_operation_guard();
        Ok(snapshot)
    }

    /// Legacy failure-cleanup entrypoint. Preserve pending history even when
    /// this caller did not observe transport: another request or maintenance
    /// wave may have replicated the same prefix, or this is an adopted barrier.
    pub fn discard_uncommitted_tail_and_clear_leader_compact(&mut self) -> Result<(), KvError> {
        self.step_down_preserving_pending_entry()
    }

    /// Ends an ambiguous proposal without truncating its durable log entry.
    ///
    /// Another voter may already have accepted the entry, so the only safe
    /// local action is to stop acting as its leader and let normal Raft
    /// reconciliation either commit or replace the preserved suffix.
    pub fn step_down_preserving_pending_entry(&mut self) -> Result<(), KvError> {
        self.last_snapshot_publish_wrote = None;
        let local_node_id = self.node_id.clone();
        let pending_term = self.pending_entry.as_ref().map(|entry| entry.term);
        let result = (|| {
            let node = self.node_mut()?;
            if node.hard_state().leader_id.as_ref() == Some(&local_node_id)
                && pending_term.is_none_or(|term| term == node.hard_state().current_term)
            {
                node.clear_known_leader()?;
                node.refresh_resident_read_view();
            }
            Ok(())
        })();
        self.release_resident_node_and_operation_guard();
        result
    }

    pub fn observe_term(&mut self, term: u64) -> Result<(), KvError> {
        self.last_snapshot_publish_wrote = None;
        let node = self.node_mut()?;
        if node.observe_term(term)? {
            // Term/leader changes live in durable hard state. They do not
            // mutate the KV state machine and therefore need no O(N)
            // materialized snapshot rewrite.
            node.refresh_resident_read_view();
        }
        Ok(())
    }

    /// Persist exactly the higher-term observation that closes the
    /// process-resident safety fence. This is the sole command-session path
    /// allowed to open/use a node while the ordinary authority gate is closed;
    /// it performs no reads, proposals, or leadership mutations, rejects a
    /// term below the newest pending floor, and clears only after hard state is
    /// durably at or above that floor.
    pub fn observe_pending_higher_term(&mut self, term: u64) -> Result<(), KvError> {
        self.last_snapshot_publish_wrote = None;
        let store = self.store.clone();
        let node_id = self.node_id.clone();
        // Preserve the global lock order: operation/resident admission first,
        // then the per-root exclusive term closer. Publication itself takes
        // neither operation nor resident ownership and remains O(1).
        self.open_node_if_missing(false)?;
        let term_lease = store.acquire_pending_higher_term_exclusive_lease(&node_id)?;
        // Do not silently rebase an explicitly stale persistence attempt. The
        // reactive worker will retry with the newest published floor; direct
        // callers must be told that their observation no longer closes it.
        ClusterKvRaftStore::ensure_pending_higher_term_observation_is_current(&term_lease, term)?;
        loop {
            let target_term = term_lease.pending_floor().unwrap_or(term).max(term);
            let node = self.node.as_mut().ok_or_else(|| KvError::Io {
                path: store.wal_paths(&node_id).root.display().to_string(),
                detail: "cluster KV Raft term observer did not retain its node".to_string(),
            })?;
            if node.observe_term(target_term)? {
                node.refresh_resident_read_view();
            }
            let generation = node.pending_higher_term_generation();
            let durable_term = node.hard_state().current_term;
            term_lease.clear_if_durable(generation, durable_term);
            let Some(pending_term) = term_lease.pending_floor() else {
                break;
            };
            if durable_term >= pending_term {
                term_lease.clear_if_durable(generation, durable_term);
                if term_lease.pending_floor().is_none() {
                    break;
                }
            }
        }
        Ok(())
    }

    pub fn commit_index_compact(
        &mut self,
        index: u64,
    ) -> Result<ClusterKvRaftCompactCommitResult, KvError> {
        self.commit_acknowledged_prefix_compact(index, index)
    }

    pub fn commit_acknowledged_prefix_compact(
        &mut self,
        index: u64,
        acknowledged_index: u64,
    ) -> Result<ClusterKvRaftCompactCommitResult, KvError> {
        if acknowledged_index < index {
            return Err(KvError::AuthorityUnavailable {
                detail: "quorum acknowledged prefix does not cover the requested entry".into(),
            });
        }
        self.last_snapshot_publish_wrote = None;
        if let Err(error) =
            self.ensure_pending_entry_is_still_authoritative("apply quorum-committed Raft proposal")
        {
            self.release_resident_node_and_operation_guard();
            return Err(error);
        }
        let store = self.store.clone();
        let result = {
            let node = self.node_mut()?;
            let before_revision = node.store().revision();
            let before_index = node.applied_index();
            let commit_index =
                group_commit::bounded_group_commit_index(node, index, acknowledged_index);
            node.commit_to(commit_index)?;
            metrics::histogram!("sinomo.kv.group_commit.finalized_entries")
                .record(commit_index.saturating_sub(before_index) as f64);
            let txn_response = node.txn_response_at(index).cloned();
            // Capture the complete command delta before periodic checkpoint
            // compaction trims retained watch history. One transaction may
            // emit more events than a checkpoint retains, but fanout and
            // committed observers must still receive every event from this
            // commit exactly once.
            let events = node.store().events_after_revision(before_revision)?;
            let final_key_states = committed_final_key_states(node.store(), &events)?;
            if let Some(receipt) = node.observation_receipt() {
                receipt.capture_original_current_term_requests(node, before_index, commit_index);
            }
            let checkpoint_snapshot = store.compact_node_if_needed_with_checkpoint(node)?;
            let committed_request = node
                .log_entry_at(index)
                .and_then(|entry| node.store().committed_request(&entry.request_id))
                .cloned()
                .ok_or(KvError::MissingCommittedEntry { index })?;
            ClusterKvRaftCompactCommitResult {
                txn_response,
                events: events.into(),
                committed_request,
                read_state: read_index_state_from_open_node(node),
                compacted_revision: node.store().compacted_revision(),
                checkpoint_snapshot: checkpoint_snapshot.map(Into::into),
                final_key_states: final_key_states.into(),
                observation_receipt: node.observation_receipt(),
            }
        };
        // Preserve the captured native effect before this storage phase can
        // return to a cancellable caller. Its exact node/proposal release
        // latches still defer every callback until physical ownership drops.
        // Later projection/fanout publishers join this same receipt, so an
        // available payload cannot become a gap when their caller disappears.
        if let Some(receipt) = result.observation_receipt.as_ref() {
            ready_committed_observation(
                receipt,
                result.read_state.revision,
                result.compacted_revision,
                Arc::clone(&result.events),
                Arc::clone(&result.final_key_states),
                result.checkpoint_snapshot.clone(),
                result
                    .checkpoint_snapshot
                    .as_ref()
                    .map(|_| result.read_state.applied_index),
            )?;
        }
        // `checkpoint_snapshot` is populated only by the Raft compaction
        // policy. Ordinary commits, including critical-key and lease changes,
        // remain bounded deltas and never serialize the complete state machine.
        if let Some(node) = self.node.as_ref() {
            node.refresh_resident_read_view();
        }
        Ok(result)
    }

    pub fn commit_index_with_events(
        &mut self,
        index: u64,
    ) -> Result<ClusterKvRaftCommitResult, KvError> {
        let compact = self.commit_index_compact(index)?;
        let store = self.store.clone();
        let checkpoint_snapshot = compact.checkpoint_snapshot.clone();
        let snapshot = match compact.checkpoint_snapshot.clone() {
            Some(snapshot) => snapshot,
            None => self
                .node
                .as_ref()
                .ok_or_else(|| KvError::Io {
                    path: self
                        .store
                        .wal_paths(&self.node_id)
                        .root
                        .display()
                        .to_string(),
                    detail: "cluster KV Raft command session lost its committed node".to_string(),
                })?
                .store()
                .try_snapshot()?
                .into(),
        };
        let result = ClusterKvRaftCommitResult {
            snapshot: snapshot.into(),
            txn_response: compact.txn_response,
            events: (compact.events).into(),
            committed_request: compact.committed_request,
            observation_receipt: compact.observation_receipt,
        };
        // The resident node and broad operation guard protect durable apply,
        // not the derived materialized read model. Publish only after checking
        // the node back in so an O(total-state) serialization/fsync cannot
        // block ReadIndex admission. The materialized cache's generation
        // comparator prevents an older post-commit writer from overwriting a
        // newer projection if publications reorder after this release.
        self.release_session_after_commit();
        let wrote = match checkpoint_snapshot.as_ref() {
            Some(snapshot) => store
                .paths()
                .local_cache()
                .write_raft_snapshot_if_changed(snapshot)?,
            None => store
                .publish_commit_result(&result, &mut self.key_can_defer_materialized_snapshot)?,
        };
        self.last_snapshot_publish_wrote = Some(wrote);
        Ok(result)
    }
}

impl<F> ClusterKvRaftCommandStorage for ClusterKvRaftLocalCommandStorage<F>
where
    F: FnMut(&str) -> bool,
{
    type Error = KvError;

    fn kv_error(error: KvError) -> Self::Error {
        error
    }

    fn is_committed_observation_delivery_failure(error: &Self::Error) -> bool {
        matches!(error, KvError::CommittedObservationDeliveryFailed { .. })
    }

    fn replay_cluster_raft_request_if_committed(
        &mut self,
        request: &KvCommandRequest,
    ) -> Result<Option<(KvSnapshot, KvCommittedRequestRecord)>, Self::Error> {
        self.replay_if_committed(request)
    }

    fn replay_cluster_raft_request_record_if_committed(
        &mut self,
        request: &KvCommandRequest,
    ) -> Result<Option<KvCommittedRequestRecord>, Self::Error> {
        self.replay_record_if_committed(request)
    }

    fn append_cluster_raft_request_uncommitted(
        &mut self,
        request: KvCommandRequest,
    ) -> Result<KvLogEntry, Self::Error> {
        self.append_uncommitted(request)
    }

    fn release_cluster_raft_command_session_for_quorum(&mut self) {
        self.release_session_for_quorum();
    }

    fn record_cluster_raft_request_rejected_before_commit(
        &mut self,
        request: &KvCommandRequest,
        failure: &KvAuthorityFailure,
    ) -> Result<(), Self::Error> {
        self.record_rejected_before_commit(request, failure)
    }

    fn record_cluster_raft_request_unknown(
        &mut self,
        request: &KvCommandRequest,
        detail: &str,
    ) -> Result<(), Self::Error> {
        self.record_unknown(request, detail)
    }

    fn discard_cluster_raft_uncommitted_tail_and_clear_leader(
        &mut self,
    ) -> Result<(), Self::Error> {
        self.discard_uncommitted_tail_and_clear_leader_compact()
    }

    fn step_down_preserving_cluster_raft_pending_entry(&mut self) -> Result<(), Self::Error> {
        self.step_down_preserving_pending_entry()
    }

    fn publish_cluster_raft_higher_term(
        &mut self,
        _state: &ClusterBootstrapState,
        term: u64,
    ) -> Result<(), Self::Error> {
        self.store.publish_pending_higher_term(&self.node_id, term)
    }

    fn observe_cluster_raft_term(
        &mut self,
        _state: &ClusterBootstrapState,
        term: u64,
    ) -> Result<(), Self::Error> {
        self.observe_pending_higher_term(term)
    }

    fn commit_cluster_raft_index_with_events(
        &mut self,
        index: u64,
    ) -> Result<ClusterKvRaftCommitResult, Self::Error> {
        self.commit_index_with_events(index)
    }

    fn commit_cluster_raft_index_compact(
        &mut self,
        index: u64,
    ) -> Result<ClusterKvRaftCompactCommitResult, Self::Error> {
        self.commit_index_compact(index)
    }

    fn observation_proposal_fence(
        &mut self,
    ) -> Result<Option<KvObservationOwnerFence>, Self::Error> {
        ClusterKvRaftLocalCommandStorage::observation_proposal_fence(self)
    }

    fn release_cluster_raft_command_session_after_commit(&mut self) {
        self.release_session_after_commit();
    }

    fn publish_cluster_raft_post_commit_observations(
        &mut self,
        commit: Option<&ClusterKvRaftCompactCommitResult>,
    ) -> Result<(), Self::Error> {
        let mut snapshot_repaired = self.last_snapshot_publish_wrote == Some(true);
        if let Some(commit) = commit {
            if let Some(snapshot) = commit.checkpoint_snapshot.as_ref() {
                snapshot_repaired = self
                    .store
                    .paths()
                    .local_cache()
                    .write_raft_snapshot_if_changed(snapshot)?;
                self.last_snapshot_publish_wrote = Some(snapshot_repaired);
            }
            if commit.events.is_empty() {
                notify_cluster_kv_watchers(self.store.paths());
            } else {
                notify_cluster_kv_watchers_for_events(self.store.paths(), &commit.events);
            }
            publish_committed_observation(
                commit.observation_receipt.as_ref(),
                commit.read_state.revision,
                commit.compacted_revision,
                Arc::clone(&commit.events),
                Arc::clone(&commit.final_key_states),
                commit.checkpoint_snapshot.clone(),
                commit
                    .checkpoint_snapshot
                    .as_ref()
                    .map(|_| commit.read_state.applied_index),
            )?;
        } else if snapshot_repaired {
            notify_cluster_kv_watchers(self.store.paths());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RaftAppendEntriesApplyOutcome {
    pub response: RaftAppendEntriesResponse,
    pub snapshot: Arc<KvSnapshot>,
    pub snapshot_restored: bool,
    /// Exact installed view; `snapshot` is the possibly later terminal view.
    pub restored_snapshot: Option<Arc<KvSnapshot>>,
    pub committed_events: Arc<Vec<KvEvent>>,
    pub observation_receipt: Option<KvCommittedObservationReceipt>,
    pub restored_snapshot_applied_index: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RaftAppendEntriesCompactApplyOutcome {
    pub response: RaftAppendEntriesResponse,
    pub snapshot_restored: bool,
    /// The leader-supplied snapshot, retained only when it was actually
    /// installed. This is not rebuilt from the resident KV state.
    pub restored_snapshot: Option<Arc<KvSnapshot>>,
    pub restored_snapshot_applied_index: Option<u64>,
    pub committed_events: Arc<Vec<KvEvent>>,
    pub read_state: ClusterKvRaftReadIndexState,
    pub compacted_revision: KvRevision,
    /// Present when follower apply emitted a periodic checkpoint or when a
    /// critical/unknown keyspace requires an immediate shared projection.
    pub checkpoint_snapshot: Option<Arc<KvSnapshot>>,
    pub final_key_states: Arc<Vec<ClusterKvCommittedKeyState>>,
    pub commit_advanced: bool,
    pub selected_lease_mutation: bool,
    /// Process-local delivery ownership; never serialized or used as authority.
    pub observation_receipt: Option<KvCommittedObservationReceipt>,
}

struct KvObservedOperationGuard {
    guard: Option<ClusterKvRaftOperationGuard>,
    fence: Option<KvObservationOwnerFence>,
}

impl Drop for KvObservedOperationGuard {
    fn drop(&mut self) {
        drop(self.guard.take());
        drop(self.fence.take());
    }
}

struct KvObservedDurableNode {
    node: Option<DurableRaftKvNode>,
    fence: Option<KvObservationOwnerFence>,
}

impl std::ops::Deref for KvObservedDurableNode {
    type Target = DurableRaftKvNode;
    fn deref(&self) -> &Self::Target {
        self.node.as_ref().expect("owned observed node")
    }
}

impl std::ops::DerefMut for KvObservedDurableNode {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.node.as_mut().expect("owned observed node")
    }
}

impl Drop for KvObservedDurableNode {
    fn drop(&mut self) {
        drop(self.node.take());
        drop(self.fence.take());
    }
}

#[derive(Debug)]
struct KvDurableObservationContext {
    capacity: KvObservationCapacityPermit,
    receipt: Option<KvCommittedObservationReceipt>,
}

impl KvDurableObservationContext {
    fn new(capacity: KvObservationCapacityPermit) -> Self {
        Self {
            capacity,
            receipt: None,
        }
    }

    fn begin(
        &mut self,
        revision: KvRevision,
        applied_index: u64,
        kind: KvCommittedObservationKind,
    ) -> Result<(), KvError> {
        if self.receipt.is_none() {
            self.receipt =
                Some(
                    self.capacity
                        .commit(revision, revision, Some(applied_index), kind)?,
                );
        }
        Ok(())
    }

    fn advance(&self, revision: KvRevision, applied_index: u64) -> Result<(), KvError> {
        if let Some(receipt) = &self.receipt {
            receipt.advance_committed(revision, applied_index)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct DurableRaftKvNode {
    node_id: NodeId,
    pub(crate) paths: KvWalPaths,
    lock: Option<File>,
    lock_identity: FileIdentity,
    root_ino: u64,
    node: ReplicatedKvNode,
    wal_synced_in_process: bool,
    poisoned: bool,
    cache_on_drop: bool,
    resident_slot: Option<Arc<ResidentRaftNodeSlot>>,
    observation: Option<KvDurableObservationContext>,
}

struct RaftOpenModeMetricGuard {
    mode: &'static str,
    outcome: &'static str,
    started: Instant,
}

impl RaftOpenModeMetricGuard {
    fn start(mode: &'static str) -> Self {
        Self {
            mode,
            outcome: "error",
            started: Instant::now(),
        }
    }

    fn succeed(&mut self) {
        self.outcome = "ok";
    }
}

impl Drop for RaftOpenModeMetricGuard {
    fn drop(&mut self) {
        metrics::histogram!(
            "sinomo.kv.raft.open.mode.us",
            "mode" => self.mode,
            "outcome" => self.outcome
        )
        .record(self.started.elapsed().as_micros() as f64);
    }
}

impl DurableRaftKvNode {
    fn pending_higher_term_generation(&self) -> PendingHigherRaftTermGeneration {
        PendingHigherRaftTermGeneration {
            lock_identity: self.lock_identity,
            root_ino: self.root_ino,
        }
    }

    pub fn open(node_id: NodeId, paths: KvWalPaths) -> Result<Self, KvError> {
        Self::open_nonresident_inner(node_id, paths, false, RAFT_NODE_LOCK_WAIT)
    }

    /// Compatibility guard that refuses, rather than erases, pending entries.
    /// Use `open` for normal restart and Raft reconciliation.
    pub fn open_discarding_uncommitted_tail(
        node_id: NodeId,
        paths: KvWalPaths,
    ) -> Result<Self, KvError> {
        Self::open_nonresident_inner(node_id, paths, true, RAFT_NODE_LOCK_WAIT)
    }

    pub(crate) fn open_resident(node_id: NodeId, paths: KvWalPaths) -> Result<Self, KvError> {
        Self::open_resident_inner(node_id, paths, false, RAFT_NODE_LOCK_WAIT)
    }

    pub(crate) fn open_resident_discarding_uncommitted_tail(
        node_id: NodeId,
        paths: KvWalPaths,
    ) -> Result<Self, KvError> {
        Self::open_resident_inner(node_id, paths, true, RAFT_NODE_LOCK_WAIT)
    }

    fn open_existing_resident_inner(
        node_id: NodeId,
        paths: KvWalPaths,
        lock_wait: Duration,
    ) -> Result<Self, KvError> {
        require_process_local_raft_access(&paths.root)?;
        let resident_slot = RESIDENT_RAFT_NODES
            .lock()
            .map_err(|error| KvError::Io {
                path: paths.root.display().to_string(),
                detail: format!("resident cluster KV Raft node registry poisoned: {error}"),
            })?
            .get(&paths.root)
            .cloned()
            .ok_or_else(|| {
                metrics::counter!("sinomo.kv.raft.resident.read_admission", "outcome" => "not_resident").increment(1);
                KvError::Busy { path: paths.root.display().to_string() }
            })?;
        resident_slot.touch();
        let cached = resident_slot.checkout(&paths.root, lock_wait)?;
        let Some(cached) = cached else {
            resident_slot.release_without_cache();
            metrics::counter!("sinomo.kv.raft.resident.read_admission", "outcome" => "empty_cache")
                .increment(1);
            return Err(KvError::Busy {
                path: paths.root.display().to_string(),
            });
        };
        if !resident_raft_ownership_matches(&cached, &paths, &node_id) {
            drop(cached);
            resident_slot.release_without_cache();
            metrics::counter!("sinomo.kv.raft.resident.read_admission", "outcome" => "identity_changed").increment(1);
            return Err(KvError::Busy {
                path: paths.root.display().to_string(),
            });
        }
        evict_cached_raft_view(&paths);
        metrics::counter!(
            "sinomo.kv.raft.resident.opens",
            "outcome" => "existing_read_index"
        )
        .increment(1);
        Ok(Self {
            node_id,
            paths,
            lock: Some(cached.lock),
            lock_identity: cached.lock_identity,
            root_ino: cached.root_ino,
            node: cached.node,
            wal_synced_in_process: cached.wal_synced_in_process,
            poisoned: false,
            cache_on_drop: true,
            resident_slot: Some(resident_slot),
            observation: None,
        })
    }

    fn open_nonresident_inner(
        node_id: NodeId,
        paths: KvWalPaths,
        discard_uncommitted_tail: bool,
        lock_wait: Duration,
    ) -> Result<Self, KvError> {
        require_process_local_raft_access(&paths.root)?;
        #[cfg(test)]
        {
            let mut attempts = NONRESIDENT_RAFT_OPEN_ATTEMPTS_FOR_TEST
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *attempts.entry(paths.root.clone()).or_default() += 1;
        }
        let mut metric = KvStorageMetricGuard::start("raft_open", "sinomo.kv.raft.open.us");
        let mut mode_metric = RaftOpenModeMetricGuard::start("nonresident");
        let opened = Self::open_cold(node_id, paths, discard_uncommitted_tail, lock_wait, None)?;
        #[cfg(test)]
        {
            let mut counts = NONRESIDENT_RAFT_OPEN_COUNTS_FOR_TEST
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *counts.entry(opened.paths.root.clone()).or_default() += 1;
        }
        metric.succeed();
        mode_metric.succeed();
        Ok(opened)
    }

    pub(crate) fn open_resident_inner(
        node_id: NodeId,
        paths: KvWalPaths,
        discard_uncommitted_tail: bool,
        lock_wait: Duration,
    ) -> Result<Self, KvError> {
        let mut metric = KvStorageMetricGuard::start("raft_open", "sinomo.kv.raft.open.us");
        let mut mode_metric = RaftOpenModeMetricGuard::start("resident");
        let resident_slot = resident_raft_node_slot(&paths.root)?;
        let cached = resident_slot.checkout(&paths.root, lock_wait)?;
        // From this point the process owns the single resident writer slot.
        // An immutable cold-read image for the same root would be a redundant
        // full keyspace copy and cannot serve fresher data than this node.
        evict_cached_raft_view(&paths);
        if let Some(cached) = cached {
            if resident_raft_ownership_matches(&cached, &paths, &node_id) {
                let mut opened = Self {
                    node_id,
                    paths,
                    lock: Some(cached.lock),
                    lock_identity: cached.lock_identity,
                    root_ino: cached.root_ino,
                    node: cached.node,
                    wal_synced_in_process: cached.wal_synced_in_process,
                    poisoned: false,
                    cache_on_drop: true,
                    resident_slot: Some(resident_slot),
                    observation: None,
                };
                if discard_uncommitted_tail {
                    opened.discard_uncommitted_tail()?;
                }
                metrics::counter!("sinomo.kv.raft.resident.opens", "outcome" => "hit").increment(1);
                #[cfg(test)]
                record_resident_raft_open_for_test(&opened.paths.root, true);
                metrics::histogram!("sinomo.kv.raft.open.log_entries")
                    .record(opened.log().len() as f64);
                opened.record_live_metrics();
                metric.succeed();
                mode_metric.succeed();
                return Ok(opened);
            }
            // Direct recovery tooling and power-loss tests may replace durable
            // files without participating in the node lock. Never serve an old
            // resident view after such a replacement.
            metrics::counter!("sinomo.kv.raft.resident.opens", "outcome" => "invalidated")
                .increment(1);
            resident_slot.clear_read_metadata();
            drop(cached);
        }

        let opened = Self::open_cold(
            node_id,
            paths,
            discard_uncommitted_tail,
            lock_wait,
            Some(resident_slot.clone()),
        );
        if opened.is_err() {
            resident_slot.release_without_cache();
        }
        let opened = opened?;
        metrics::counter!("sinomo.kv.raft.resident.opens", "outcome" => "miss").increment(1);
        #[cfg(test)]
        record_resident_raft_open_for_test(&opened.paths.root, false);
        metric.succeed();
        mode_metric.succeed();
        Ok(opened)
    }

    fn open_cold(
        node_id: NodeId,
        paths: KvWalPaths,
        discard_uncommitted_tail: bool,
        lock_wait: Duration,
        resident_slot: Option<Arc<ResidentRaftNodeSlot>>,
    ) -> Result<Self, KvError> {
        paths.ensure()?;
        let lock = open_node_lock(&paths)?;
        {
            let mut metric = KvStorageMetricGuard::start(
                "raft_open_file_lock",
                "sinomo.kv.raft.open.file_lock.us",
            );
            if let Err(error) = lock_node_exclusive(&lock, &paths.lock, lock_wait) {
                if matches!(&error, KvError::Busy { .. }) {
                    metric.finish("busy");
                }
                return Err(error);
            }
            metric.succeed();
        }
        // Startup installs the role denial before concurrency. Rechecking
        // after flock acquisition also makes an accidental concurrent denial
        // drop this handle before any Raft state is restored or returned.
        require_process_local_raft_access(&paths.root)?;
        let lock_identity = file_handle_identity(&lock, &paths.lock)?;
        let root_ino = directory_ino(&paths.root)?;
        let mut node = {
            let mut metric = KvStorageMetricGuard::start(
                "raft_open_snapshot_restore",
                "sinomo.kv.raft.open.snapshot_restore.us",
            );
            let snapshot = read_optional_object::<RaftSnapshot>(&paths.snapshot)?;
            let snapshot_present = snapshot.is_some();
            let hard_state = paths.read_persisted_hard_state()?;
            let node = if let Some(snapshot) = snapshot {
                let mut node = ReplicatedKvNode {
                    store: InProcessKvStore::new(),
                    log: Vec::new(),
                    hard_state: hard_state.clone(),
                    applied_index: 0,
                    snapshot_index: 0,
                    snapshot_term: 0,
                    txn_responses: KvTxnResponseIndex::default(),
                };
                node.install_snapshot(snapshot)?;
                node.hard_state.current_term = hard_state.current_term;
                node.hard_state.voted_for = hard_state.voted_for;
                node.hard_state.leader_id = hard_state.leader_id;
                node.hard_state.commit_index =
                    hard_state.commit_index.max(node.hard_state.commit_index);
                node
            } else {
                ReplicatedKvNode {
                    store: InProcessKvStore::new(),
                    log: Vec::new(),
                    hard_state,
                    applied_index: 0,
                    snapshot_index: 0,
                    snapshot_term: 0,
                    txn_responses: KvTxnResponseIndex::default(),
                }
            };
            metric.finish(if snapshot_present {
                "restored"
            } else {
                "empty"
            });
            node
        };
        {
            let mut metric = KvStorageMetricGuard::start(
                "raft_open_wal_restore",
                "sinomo.kv.raft.open.wal_restore.us",
            );
            let retain_after_index = node
                .snapshot_index
                .saturating_sub(RAFT_COMPACTION_RETAINED_ENTRIES);
            let WalRead { entries, truncated } =
                read_wal_entries(&paths.wal, None, retain_after_index)?;
            // Preserve the already-applied tail (kept by local_compact for incremental
            // replication) instead of discarding it as append_entries would.
            let load_truncated = node.load_persisted_entries(entries);
            let max_committable_index = node.last_log_index();
            if node.hard_state.commit_index > max_committable_index {
                return Err(KvError::MissingCommittedEntry {
                    index: max_committable_index.saturating_add(1),
                });
            }
            if discard_uncommitted_tail {
                node.ensure_no_uncommitted_tail()?;
            }
            let commit_index = node.hard_state.commit_index;
            node.commit_to(commit_index)?;
            if truncated || load_truncated {
                rewrite_wal_entries(&paths.wal, &node.log)?;
            }
            metric.succeed();
        }
        let cache_on_drop = resident_slot.is_some();
        let opened = Self {
            node_id,
            paths,
            lock: Some(lock),
            lock_identity,
            root_ino,
            node,
            // A cold read is not evidence that an earlier failed fsync was
            // repaired. A successful local WAL append enables prefix lookahead.
            wal_synced_in_process: false,
            poisoned: false,
            cache_on_drop,
            resident_slot,
            observation: None,
        };
        // Publish the bounded consensus projection immediately. A concurrent
        // consensus-state read must not fall back to a stale materialized
        // voter configuration merely because this first opener still owns the
        // mutable resident node.
        opened.refresh_resident_read_view();
        metrics::histogram!("sinomo.kv.raft.open.log_entries").record(opened.log().len() as f64);
        opened.record_live_metrics();
        Ok(opened)
    }

    pub async fn open_async(node_id: NodeId, paths: KvWalPaths) -> Result<Self, KvError> {
        let error_path = paths.root.display().to_string();
        tokio::task::spawn_blocking(move || Self::open(node_id, paths))
            .await
            .map_err(|source| KvError::Io {
                path: error_path,
                detail: source.to_string(),
            })?
    }

    pub fn open_with_materialized_cache(
        node_id: NodeId,
        paths: KvWalPaths,
        cache: &KvMaterializedCache,
    ) -> Result<Self, KvError> {
        Self::open_with_materialized_cache_inner(node_id, paths, cache, false, RAFT_NODE_LOCK_WAIT)
    }

    pub fn open_discarding_uncommitted_tail_with_materialized_cache(
        node_id: NodeId,
        paths: KvWalPaths,
        cache: &KvMaterializedCache,
    ) -> Result<Self, KvError> {
        Self::open_with_materialized_cache_inner(node_id, paths, cache, true, RAFT_NODE_LOCK_WAIT)
    }

    pub(crate) fn try_open_with_materialized_cache(
        node_id: NodeId,
        paths: KvWalPaths,
        cache: &KvMaterializedCache,
    ) -> Result<Self, KvError> {
        Self::open_with_materialized_cache_inner(
            node_id,
            paths,
            cache,
            false,
            RAFT_LOCAL_READ_LOCK_WAIT,
        )
    }

    fn open_with_materialized_cache_inner(
        node_id: NodeId,
        paths: KvWalPaths,
        _cache: &KvMaterializedCache,
        discard_uncommitted_tail: bool,
        lock_wait: Duration,
    ) -> Result<Self, KvError> {
        // A materialized `KvSnapshot` has no Raft log position or term. Treating
        // it as an index-zero Raft snapshot makes a fresh learner apply entries
        // 1..N on top of state that already includes those entries, inflating KV
        // revisions and potentially changing compare/transaction results.
        //
        // The materialized snapshot remains a read cache. Writable Raft state
        // may only come from the durable Raft files or an explicitly anchored
        // `RaftSnapshot { last_included_index, last_included_term, .. }` received
        // through replication. Corrupt durable state must likewise fail closed;
        // silently quarantining it and restarting at index zero loses the
        // consensus position needed for a safe recovery.
        Self::open_nonresident_inner(node_id, paths, discard_uncommitted_tail, lock_wait)
    }

    pub async fn open_async_with_materialized_cache(
        node_id: NodeId,
        paths: KvWalPaths,
        cache: KvMaterializedCache,
    ) -> Result<Self, KvError> {
        let error_path = paths.root.display().to_string();
        tokio::task::spawn_blocking(move || {
            Self::open_with_materialized_cache(node_id, paths, &cache)
        })
        .await
        .map_err(|source| KvError::Io {
            path: error_path,
            detail: source.to_string(),
        })?
    }

    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Reports whether a post-persistence apply failure made this in-memory
    /// view unsafe to reuse.
    ///
    /// The WAL and hard state remain authoritative. Drop and reopen a poisoned
    /// node to replay the committed prefix into a fresh store.
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    pub fn hard_state(&self) -> &RaftHardState {
        self.assert_healthy();
        &self.node.hard_state
    }

    pub fn store(&self) -> &InProcessKvStore {
        self.assert_healthy();
        &self.node.store
    }

    fn refresh_resident_read_view(&self) {
        let Some(slot) = self.resident_slot.as_ref() else {
            return;
        };
        let Some(identity) = raft_view_identity(&self.paths) else {
            return;
        };
        let Ok(mut state) = slot.state.lock() else {
            return;
        };
        state.read_metadata = Some(ResidentRaftReadMetadata::from_open_node(
            self,
            identity,
            state.read_metadata.as_ref(),
        ));
    }

    /// Transfers the restored, indexed KV store into an immutable local read
    /// view and releases this operation-scoped durable owner.
    ///
    /// A point/prefix cold read still has to restore the durable Raft snapshot
    /// and committed WAL once, but it must not immediately walk that complete
    /// store again merely to manufacture a vector snapshot.  This mirrors
    /// etcd's long-lived MVCC read view: the indexed store is the read model;
    /// an owned snapshot is produced only for callers that actually request
    /// the whole keyspace.
    pub(crate) fn try_into_local_read_view(mut self) -> Result<Arc<RaftLocalReadView>, KvError> {
        self.ensure_healthy()?;
        let observed_term = self.node.hard_state.current_term;
        let applied_index = self.node.applied_index;
        let known_leader = self.node.hard_state.leader_id.clone();
        // This path is intentionally operation scoped.  If a future caller
        // passes a resident handle, fail closed by evicting it on drop rather
        // than caching a node whose store has been moved away.
        self.cache_on_drop = false;
        let node = std::mem::replace(&mut self.node, ReplicatedKvNode::empty());
        Ok(Arc::new(RaftLocalReadView::new(
            node.store,
            observed_term,
            applied_index,
            known_leader,
        )))
    }

    pub fn log(&self) -> &[KvLogEntry] {
        self.assert_healthy();
        &self.node.log
    }

    pub fn last_log_index(&self) -> u64 {
        self.assert_healthy();
        self.node.last_log_index()
    }

    pub fn last_log_term(&self) -> u64 {
        self.assert_healthy();
        self.node.last_log_term()
    }

    pub fn term_at(&self, index: u64) -> Option<u64> {
        self.assert_healthy();
        self.node.term_at(index)
    }

    pub fn log_entry_at(&self, index: u64) -> Option<&KvLogEntry> {
        self.assert_healthy();
        self.node.log_entry_at(index)
    }

    pub fn applied_index(&self) -> u64 {
        self.assert_healthy();
        self.node.applied_index
    }

    pub fn snapshot_index(&self) -> u64 {
        self.assert_healthy();
        self.node.snapshot_index
    }

    pub fn txn_response_at(&self, index: u64) -> Option<&KvTxnResponse> {
        self.assert_healthy();
        self.node.txn_responses.get(index)
    }

    pub fn append_entries(
        &mut self,
        request: RaftAppendEntriesRequest,
    ) -> Result<RaftAppendEntriesResponse, KvError> {
        self.ensure_healthy()?;
        let retain_resident = self.begin_resident_mutation();
        if let Some(response) = self.node.read_only_append_entries_response(&request) {
            self.finish_resident_mutation(retain_resident);
            return Ok(response);
        }

        let hard_state_before = self.node.hard_state.clone();
        let log_before = self.node.log.clone();
        let txn_responses_before = self.node.txn_responses.clone();
        let applied_index_before = self.node.applied_index;
        let preparation = match self.node.prepare_append_entries(request) {
            Ok(preparation) => preparation,
            Err(error) => {
                self.restore_after_append_entries_persist_failure(
                    None,
                    hard_state_before,
                    log_before,
                    txn_responses_before,
                    applied_index_before,
                );
                return Err(error);
            }
        };

        let entries_to_apply = match preparation.commit_index {
            Some(commit_index) if commit_index > self.node.applied_index => {
                match self.node.entries_to_commit(commit_index) {
                    Ok(entries) => {
                        if let Err(error) = self.node.preflight_commit_entries(entries.as_slice()) {
                            self.restore_after_append_entries_persist_failure(
                                None,
                                hard_state_before,
                                log_before,
                                txn_responses_before,
                                applied_index_before,
                            );
                            return Err(error);
                        }
                        Some((commit_index, entries))
                    }
                    Err(error) => {
                        self.restore_after_append_entries_persist_failure(
                            None,
                            hard_state_before,
                            log_before,
                            txn_responses_before,
                            applied_index_before,
                        );
                        return Err(error);
                    }
                }
            }
            Some(commit_index) => {
                self.node.hard_state.commit_index =
                    self.node.hard_state.commit_index.max(commit_index);
                None
            }
            None => None,
        };

        if preparation.response.success
            && let Err(error) = append_wal_entries(&self.paths.wal, &preparation.appended_entries)
        {
            self.restore_after_append_entries_persist_failure(
                None,
                hard_state_before,
                log_before,
                txn_responses_before,
                applied_index_before,
            );
            return Err(error);
        }
        if let Some((commit_index, _)) = &entries_to_apply {
            self.begin_observation_effect(KvCommittedObservationKind::Contiguous)?;
            self.node.hard_state.commit_index = *commit_index;
        }
        if let Err(error) = self.persist_hard_state_if_changed(&hard_state_before) {
            self.restore_after_append_entries_persist_failure(
                None,
                hard_state_before,
                log_before,
                txn_responses_before,
                applied_index_before,
            );
            return Err(error);
        }
        if let Some((commit_index, entries)) = entries_to_apply {
            let mut metric = KvStorageMetricGuard::start("apply", "sinomo.kv.apply.us");
            self.apply_persisted_commit(entries, commit_index, "append_entries")?;
            metric.succeed();
        }
        self.record_live_metrics();
        self.finish_resident_mutation(retain_resident);
        Ok(preparation.response)
    }

    pub fn apply_append_entries_with_optional_snapshot(
        &mut self,
        snapshot: Option<RaftSnapshot>,
        append: RaftAppendEntriesRequest,
    ) -> Result<RaftAppendEntriesApplyOutcome, KvError> {
        let restored_snapshot = snapshot.as_ref().map(|snapshot| Arc::clone(&snapshot.kv));
        let (response, snapshot_restored, before_revision) =
            self.apply_append_entries_with_optional_snapshot_state(snapshot, append)?;
        let snapshot = self.store().try_snapshot()?;
        let committed_events = if snapshot_restored || snapshot.revision == before_revision {
            Vec::new()
        } else {
            ClusterKvRaftStore::events_after_revision(&snapshot, before_revision)?
        };
        Ok(RaftAppendEntriesApplyOutcome {
            response,

            snapshot: snapshot.into(),

            snapshot_restored,
            restored_snapshot: snapshot_restored.then_some(restored_snapshot).flatten(),

            committed_events: committed_events.into(),

            observation_receipt: None,
            restored_snapshot_applied_index: None,
        })
    }

    fn apply_append_entries_with_optional_snapshot_state(
        &mut self,
        snapshot: Option<RaftSnapshot>,
        append: RaftAppendEntriesRequest,
    ) -> Result<(RaftAppendEntriesResponse, bool, KvRevision), KvError> {
        self.ensure_healthy()?;
        let before_revision = self.store().revision();
        let mut snapshot_restored = false;
        if let Some(snapshot) = snapshot {
            let applied_index = self.applied_index();
            let snapshot_matches_append = append.prev_log_index >= snapshot.last_included_index
                && (append.prev_log_index != snapshot.last_included_index
                    || append.prev_log_term == snapshot.last_included_term);
            let snapshot_is_newer = snapshot.last_included_index > applied_index;
            let append_term_is_current = append.term >= self.hard_state().current_term;
            // Raft snapshots advance the committed prefix; they never arbitrate
            // between two states at the same committed index. A same-index
            // mismatch is a safety violation or durable corruption and must be
            // rejected by AppendEntries without overwriting local committed
            // state.
            if append_term_is_current && snapshot_matches_append && snapshot_is_newer {
                self.restore_snapshot(snapshot)?;
                snapshot_restored = true;
            }
        }
        let response = self.append_entries(append)?;
        Ok((response, snapshot_restored, before_revision))
    }

    pub fn request_vote(
        &mut self,
        request: RaftRequestVoteRequest,
    ) -> Result<RaftRequestVoteResponse, KvError> {
        self.ensure_healthy()?;
        let retain_resident = self.begin_resident_mutation();
        let hard_state_before = self.node.hard_state.clone();
        let response = match self.node.handle_request_vote(request) {
            Ok(response) => response,
            Err(error) => {
                self.node.hard_state = hard_state_before;
                return Err(error);
            }
        };
        if let Err(error) = self.persist_hard_state_if_changed(&hard_state_before) {
            self.node.hard_state = hard_state_before;
            return Err(error);
        }
        self.record_live_metrics();
        self.finish_resident_mutation(retain_resident);
        Ok(response)
    }

    pub fn observe_term(&mut self, term: u64) -> Result<bool, KvError> {
        self.ensure_healthy()?;
        let retain_resident = self.begin_resident_mutation();
        let hard_state_before = self.node.hard_state.clone();
        if !self.node.observe_term(term) {
            self.finish_resident_mutation(retain_resident);
            return Ok(false);
        }
        if let Err(error) = self.persist_hard_state() {
            self.node.hard_state = hard_state_before;
            return Err(error);
        }
        self.record_live_metrics();
        self.finish_resident_mutation(retain_resident);
        Ok(true)
    }

    pub fn become_leader(&mut self, term: u64) -> Result<(), KvError> {
        self.ensure_healthy()?;
        let retain_resident = self.begin_resident_mutation();
        // Leadership was won for exactly `term`; a higher local term means a
        // concurrent vote or append advanced the node past this election and
        // claiming leadership for the newer term would not be backed by a
        // quorum of votes.
        if self.node.hard_state.current_term > term {
            return Err(KvError::StaleLeadershipTerm {
                term,
                current_term: self.node.hard_state.current_term,
            });
        }
        let hard_state_before = self.node.hard_state.clone();
        self.node.hard_state.current_term = term;
        self.node.hard_state.voted_for = Some(self.node_id.clone());
        self.node.hard_state.leader_id = Some(self.node_id.clone());
        if let Err(error) = self.persist_hard_state_if_changed(&hard_state_before) {
            self.node.hard_state = hard_state_before;
            return Err(error);
        }
        self.record_live_metrics();
        self.finish_resident_mutation(retain_resident);
        Ok(())
    }

    pub fn clear_known_leader(&mut self) -> Result<(), KvError> {
        self.ensure_healthy()?;
        let retain_resident = self.begin_resident_mutation();
        let hard_state_before = self.node.hard_state.clone();
        self.node.hard_state.leader_id = None;
        if let Err(error) = self.persist_hard_state_if_changed(&hard_state_before) {
            self.node.hard_state = hard_state_before;
            return Err(error);
        }
        self.record_live_metrics();
        self.finish_resident_mutation(retain_resident);
        Ok(())
    }

    pub fn append_local_command(
        &mut self,
        term: u64,
        command: KvCommand,
    ) -> Result<KvLogEntry, KvError> {
        self.append_local_request(term, KvCommandRequest::generated(command))
    }

    pub fn append_local_request(
        &mut self,
        term: u64,
        request: KvCommandRequest,
    ) -> Result<KvLogEntry, KvError> {
        self.ensure_healthy()?;
        let retain_resident = self.begin_resident_mutation();
        let hard_state_before = self.node.hard_state.clone();
        let log_before = self.node.log.clone();
        let entry = KvLogEntry {
            index: self.node.last_log_index().saturating_add(1),
            term,
            request_id: request.request_id,
            command: request.command,
            revision: KvRevision::ZERO,
        };
        self.node.hard_state.current_term = term;
        self.node.hard_state.leader_id = Some(self.node_id.clone());
        if let Err(error) = self.node.append_entries(vec![entry.clone()]) {
            self.node.hard_state = hard_state_before;
            self.node.log = log_before;
            return Err(error);
        }
        if let Err(error) = append_wal_entries(&self.paths.wal, std::slice::from_ref(&entry)) {
            self.node.hard_state = hard_state_before;
            self.node.log = log_before;
            return Err(error);
        }
        self.wal_synced_in_process = true;
        if let Err(error) = self.persist_hard_state_if_changed(&hard_state_before) {
            self.node.hard_state = hard_state_before;
            self.node.log = log_before;
            return Err(error);
        }
        self.record_live_metrics();
        self.finish_resident_mutation(retain_resident);
        Ok(entry)
    }

    /// Refuse unilateral deletion of valid pending history. Local commit
    /// knowledge cannot prove that an entry was never replicated or committed
    /// elsewhere. Only a matching leader's AppendEntries may replace a suffix.
    pub fn discard_uncommitted_tail(&mut self) -> Result<bool, KvError> {
        self.ensure_healthy()?;
        self.node.ensure_no_uncommitted_tail()?;
        Ok(false)
    }

    pub fn compact_event_history_to(&mut self, max_events: usize) {
        self.assert_healthy();
        self.node.store.compact_event_history_to(max_events);
    }

    pub fn commit_to(&mut self, commit_index: u64) -> Result<(), KvError> {
        self.ensure_healthy()?;
        let mut metric = KvStorageMetricGuard::start("apply", "sinomo.kv.apply.us");
        let hard_state_before = self.node.hard_state.clone();
        if commit_index <= self.node.applied_index {
            let retain_resident = self.begin_resident_mutation();
            if let Err(error) = self.node.commit_to(commit_index) {
                self.node.hard_state = hard_state_before;
                return Err(error);
            }
            if let Err(error) = self.persist_hard_state_if_changed(&hard_state_before) {
                self.node.hard_state = hard_state_before;
                return Err(error);
            }
            self.record_live_metrics();
            metric.succeed();
            self.finish_resident_mutation(retain_resident);
            return Ok(());
        }

        let entries = self.node.entries_to_commit(commit_index)?;
        self.node.preflight_commit_entries(entries.as_slice())?;
        self.begin_observation_effect(KvCommittedObservationKind::Contiguous)?;
        // Preflight and source admission have no native effect. In particular,
        // cancellation before proposal custody cannot discard a healthy
        // resident merely because its observation source rejects this apply.
        let retain_resident = self.begin_resident_mutation();
        self.node.hard_state.commit_index = commit_index;
        if let Err(error) = self.persist_hard_state_if_changed(&hard_state_before) {
            self.node.hard_state = hard_state_before;
            return Err(error);
        }
        let result = self.apply_persisted_commit(entries, commit_index, "commit_to");
        if result.is_ok() {
            self.record_live_metrics();
            metric.succeed();
            self.finish_resident_mutation(retain_resident);
        }
        result
    }

    pub fn save_snapshot(&mut self) -> Result<RaftSnapshot, KvError> {
        self.ensure_healthy()?;
        let retain_resident = self.begin_resident_mutation();
        let mut metric =
            KvStorageMetricGuard::start("snapshot_write", "sinomo.kv.snapshot.write.us");
        let node_before = self.node.clone();
        let hard_state_before = node_before.hard_state.clone();
        let snapshot = RaftSnapshot {
            last_included_index: self.node.applied_index,
            last_included_term: self
                .node
                .term_at(self.node.applied_index)
                .unwrap_or(self.node.snapshot_term),
            kv: (self.node.store.try_snapshot()?).into(),
        };
        write_object_file(&self.paths.snapshot, &snapshot)?;
        // Local compaction keeps a tail of recent committed entries in memory for
        // incremental replication; the snapshot file still records the full state at
        // applied_index for cold recovery.
        self.node.local_compact(RAFT_COMPACTION_RETAINED_ENTRIES);
        if let Err(error) = rewrite_wal_entries(&self.paths.wal, &self.node.log) {
            self.node = node_before;
            return Err(error);
        }
        if let Err(error) = self.persist_hard_state_if_changed(&hard_state_before) {
            self.node = node_before;
            return Err(error);
        }
        let written_bytes = fs::metadata(&self.paths.snapshot)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        metrics::counter!("sinomo.kv.snapshot.write.bytes").increment(written_bytes);
        self.record_live_metrics();
        metric.succeed();
        self.finish_resident_mutation(retain_resident);
        Ok(snapshot)
    }

    pub fn restore_snapshot(&mut self, snapshot: RaftSnapshot) -> Result<(), KvError> {
        self.ensure_healthy()?;
        let mut metric =
            KvStorageMetricGuard::start("snapshot_restore", "sinomo.kv.snapshot.restore.us");
        // Prepare the complete replacement before publishing any durable file.
        // Resource bounds alone are insufficient: restoring also checks request
        // identities and builds the state-machine indexes. Serialize once before
        // consuming the snapshot so validation needs no extra snapshot clone.
        let content =
            sinomo_object_serde::to_bytes(&snapshot, KV_WAL_OBJECT_VERSION_ID, LengthMode::U32)
                .map_err(kv_object_error)?;
        let mut restored = self.node.clone();
        restored.install_snapshot(snapshot)?;
        let retain_resident = self.begin_resident_mutation();
        self.begin_observation_effect(KvCommittedObservationKind::Replacement)?;
        let persisted = (|| {
            write_object_file_bytes_if_changed(&self.paths.snapshot, &content)?;
            rewrite_wal_entries(&self.paths.wal, &restored.log)?;
            if restored.hard_state != self.node.hard_state {
                self.paths
                    .write_persisted_hard_state(&restored.hard_state)?;
            }
            Ok::<(), KvError>(())
        })();
        if let Err(error) = persisted {
            // A replacement or its directory sync may already have succeeded.
            // The old in-memory state cannot safely resume writes against that
            // generation; cold replay reconciles the snapshot and remaining WAL.
            self.poisoned = true;
            return Err(error);
        }
        self.node = restored;
        self.advance_observation_effect()?;
        self.record_live_metrics();
        metric.succeed();
        self.finish_resident_mutation(retain_resident);
        Ok(())
    }

    fn begin_observation_effect(
        &mut self,
        kind: KvCommittedObservationKind,
    ) -> Result<(), KvError> {
        let revision = self.node.store.revision();
        let applied_index = self.node.applied_index;
        if let Some(context) = &mut self.observation {
            context.begin(revision, applied_index, kind)?;
        }
        Ok(())
    }

    fn advance_observation_effect(&self) -> Result<(), KvError> {
        if let Some(context) = &self.observation {
            context.advance(self.node.store.revision(), self.node.applied_index)?;
        }
        Ok(())
    }

    fn observation_receipt(&self) -> Option<KvCommittedObservationReceipt> {
        self.observation
            .as_ref()
            .and_then(|context| context.receipt.clone())
    }

    fn persist_hard_state(&self) -> Result<(), KvError> {
        self.paths.write_persisted_hard_state(&self.node.hard_state)
    }

    fn apply_persisted_commit(
        &mut self,
        entries: CommitEntries,
        commit_index: u64,
        phase: &'static str,
    ) -> Result<(), KvError> {
        // The commit index is durable before this method runs. If applying the
        // committed entries fails, rolling only part of the in-memory store
        // backward would be unsafe and cloning the whole keyspace before every
        // commit would be prohibitively expensive. Poison this handle instead;
        // reopening deterministically rebuilds it from the durable prefix.
        #[cfg(test)]
        let result =
            if let Some(fail_after) = take_cluster_kv_raft_apply_failure_for_test(&self.paths) {
                self.node.apply_preflighted_commit_failing_after_for_test(
                    entries,
                    commit_index,
                    fail_after,
                )
            } else {
                self.node.apply_preflighted_commit(entries, commit_index)
            };
        #[cfg(not(test))]
        let result = self.node.apply_preflighted_commit(entries, commit_index);
        let observation_result = self.advance_observation_effect();
        if result.is_err() {
            self.poisoned = true;
            metrics::counter!("sinomo.kv.raft.poisoned_sessions", "phase" => phase).increment(1);
        }
        result.and(observation_result)
    }

    fn ensure_healthy(&self) -> Result<(), KvError> {
        if self.poisoned {
            return Err(KvError::RaftSessionPoisoned);
        }
        Ok(())
    }

    /// Makes resident reuse fail closed across a mutation. The caller restores
    /// reuse only after every durability step and in-memory apply has succeeded;
    /// any early error therefore drops the cached node and forces a cold replay.
    fn begin_resident_mutation(&mut self) -> bool {
        let retain_on_success = self.resident_slot.is_some() && self.cache_on_drop;
        self.cache_on_drop = false;
        retain_on_success
    }

    fn finish_resident_mutation(&mut self, retain_on_success: bool) {
        if retain_on_success && !self.poisoned {
            self.cache_on_drop = true;
        }
    }

    fn assert_healthy(&self) {
        assert!(
            !self.poisoned,
            "durable raft node session is poisoned; drop and reopen the node"
        );
    }

    fn persist_hard_state_if_changed(&self, before: &RaftHardState) -> Result<(), KvError> {
        if self.node.hard_state == *before {
            return Ok(());
        }
        self.persist_hard_state()
    }

    fn record_live_metrics(&self) {
        record_cluster_kv_raft_live_metrics(
            &self.paths.root,
            self.node.hard_state.current_term,
            self.node.hard_state.leader_id.clone(),
            self.node.hard_state.commit_index,
            self.node.applied_index,
            self.node.last_log_index(),
            self.node.snapshot_index,
            self.node.log.len(),
        );
    }

    fn restore_after_append_entries_persist_failure(
        &mut self,
        node_before: Option<ReplicatedKvNode>,
        hard_state_before: RaftHardState,
        log_before: Vec<KvLogEntry>,
        txn_responses_before: KvTxnResponseIndex,
        applied_index_before: u64,
    ) {
        if let Some(node_before) = node_before {
            self.node = node_before;
            return;
        }
        self.node.hard_state = hard_state_before;
        self.node.log = log_before;
        self.node.txn_responses = txn_responses_before;
        self.node.applied_index = applied_index_before;
    }
}

impl Drop for DurableRaftKvNode {
    fn drop(&mut self) {
        let Some(resident_slot) = self.resident_slot.clone() else {
            return;
        };
        let Some(lock) = self.lock.take() else {
            resident_slot.release_without_cache();
            return;
        };
        if process_local_raft_access_is_denied(&self.paths.root) {
            drop(lock);
            resident_slot.release_without_cache();
            return;
        }
        if self.poisoned || !self.cache_on_drop || std::thread::panicking() {
            drop(lock);
            resident_slot.release_without_cache();
            return;
        }
        let Some(identity) = raft_view_identity(&self.paths) else {
            drop(lock);
            resident_slot.release_without_cache();
            return;
        };
        if file_identity(&self.paths.lock) != Some(self.lock_identity)
            || file_handle_identity(&lock, &self.paths.lock).ok() != Some(self.lock_identity)
            || directory_ino(&self.paths.root).ok() != Some(self.root_ino)
        {
            drop(lock);
            resident_slot.release_without_cache();
            metrics::counter!("sinomo.kv.raft.resident.opens", "outcome" => "ownership_replaced")
                .increment(1);
            return;
        }
        let node = std::mem::replace(&mut self.node, ReplicatedKvNode::empty());
        resident_slot.checkin(ResidentRaftNode {
            node_id: self.node_id.clone(),
            identity,
            lock_identity: self.lock_identity,
            root_ino: self.root_ino,
            lock,
            node,
            wal_synced_in_process: self.wal_synced_in_process,
        });
    }
}

impl From<&ClusterBootstrapState> for ClusterKvRaftConfigRecord {
    fn from(state: &ClusterBootstrapState) -> Self {
        let mut voters = Vec::new();
        let mut learners = Vec::new();
        let mut non_voting = Vec::new();
        for member in &state.members {
            if !member.state.is_authorized() {
                continue;
            }
            match member.state {
                ClusterMemberState::Voter => voters.push(member.node_id.clone()),
                ClusterMemberState::Learner => learners.push(member.node_id.clone()),
                ClusterMemberState::NonVoting => non_voting.push(member.node_id.clone()),
                ClusterMemberState::Active => match member.role {
                    ClusterMemberRole::Voter => voters.push(member.node_id.clone()),
                    ClusterMemberRole::Learner => learners.push(member.node_id.clone()),
                    ClusterMemberRole::NonVoting => non_voting.push(member.node_id.clone()),
                },
                ClusterMemberState::Pending
                | ClusterMemberState::Suspect
                | ClusterMemberState::Offline
                | ClusterMemberState::Removed => {}
            }
        }
        voters.sort();
        learners.sort();
        non_voting.sort();
        Self {
            cluster_id: state.cluster_id.clone(),
            term: state.term,
            kv_revision: state.kv_revision,
            voters,
            learners,
            non_voting,
            updated_at: state.updated_at,
        }
    }
}

#[cfg(test)]
mod pending_higher_term_gate_tests {
    use super::*;
    use std::sync::mpsc;

    fn test_generation(ino: u64) -> PendingHigherRaftTermGeneration {
        PendingHigherRaftTermGeneration {
            lock_identity: FileIdentity {
                mtime: None,
                len: 0,
                ino,
            },
            root_ino: ino,
        }
    }

    #[test]
    fn settled_append_uses_shared_term_admission_until_later_publication() {
        let paths = ClusterKvPaths::new("/pending-higher-term-settled-append");
        let node_id =
            NodeId::from_legacy_named("node_pending_higher_term_settled_append").expect("node id");
        let store = ClusterKvRaftStore::new(paths, default_cluster_kv_raft_compaction_policy());
        let lease = store
            .acquire_pending_higher_term_append_lease(&node_id, 3, 3)
            .expect("admit settled append");
        assert!(matches!(
            lease,
            PendingHigherRaftTermAppendLease::Settled(_)
        ));
        let gate = store
            .pending_higher_term_gate(&node_id)
            .expect("pending term gate");
        {
            let state = gate.state.lock().expect("pending term state");
            assert!(!state.exclusive);
            assert_eq!(state.active_authority_leases, 1);
        }

        store
            .publish_pending_higher_term(&node_id, 4)
            .expect("publish after append admission");
        {
            let state = gate.state.lock().expect("published term state");
            assert_eq!(state.floor, Some(4));
            assert!(!state.exclusive);
            assert_eq!(state.active_authority_leases, 1);
        }
        drop(lease);
        let state = gate.state.lock().expect("released term state");
        assert_eq!(state.active_authority_leases, 0);
        assert_eq!(state.floor, Some(4));
    }

    #[test]
    fn durable_watermark_reset_carries_the_suppressed_floor_atomically() {
        let first_generation = test_generation(1);
        let replacement_generation = test_generation(2);
        let mut state = PendingHigherRaftTermGateState::default();
        state.acknowledge_durable_term(first_generation, 7);

        // This models a term-7 publication suppressed by the cached watermark,
        // followed by acknowledgement of a lower replacement generation.
        state.acknowledge_durable_term(replacement_generation, 3);
        assert_eq!(state.floor, Some(7));
        assert_eq!(
            state.known_durable,
            Some(PendingHigherRaftTermDurableState {
                generation: replacement_generation,
                term: 3,
            })
        );

        // A stale lower acknowledgement cannot lower a concurrently newer
        // floor because both the carry and watermark reset hold one mutex.
        state.floor = Some(9);
        state.acknowledge_durable_term(replacement_generation, 2);
        assert_eq!(state.floor, Some(9));
    }

    #[test]
    fn settled_floor_read_joins_an_exclusive_durable_close() {
        let paths = ClusterKvPaths::new("/pending-higher-term-settled-read");
        let node_id =
            NodeId::from_legacy_named("node_pending_higher_term_settled_read").expect("node id");
        let store = ClusterKvRaftStore::new(paths, default_cluster_kv_raft_compaction_policy());
        store
            .publish_pending_higher_term(&node_id, 2)
            .expect("publish pending term");
        let exclusive = store
            .acquire_pending_higher_term_exclusive_lease(&node_id)
            .expect("claim durable close");
        let gate = store
            .pending_higher_term_gate(&node_id)
            .expect("pending term gate");

        let reader_store = store.clone();
        let reader_node_id = node_id.clone();
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let reader = thread::spawn(move || {
            result_tx
                .send(reader_store.pending_higher_term(&reader_node_id))
                .expect("report settled floor");
        });

        let mut state = gate.state.lock().expect("pending term state");
        while state.settled_read_waiters == 0 {
            state = gate.changed.wait(state).expect("pending term waiter");
        }
        drop(state);
        assert_eq!(
            result_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty),
            "a settled observer must not expose the floor while its exclusive close is in flight"
        );

        assert!(
            exclusive.clear_if_durable(test_generation(1), 2),
            "the durable close must clear its matching floor"
        );
        drop(exclusive);
        assert_eq!(
            result_rx
                .recv()
                .expect("settled floor result")
                .expect("floor read"),
            None
        );
        reader.join().expect("join settled floor reader");
    }
}

#[cfg(test)]
mod resident_configuration_floor_tests {
    use super::*;

    fn fixture(
        name: &str,
    ) -> (
        sinomo_testdir::TestDir,
        ClusterKvPaths,
        NodeId,
        ResidentRaftNodeRegistryPin,
        ClusterKvRaftConfigRecord,
    ) {
        let root = sinomo_testdir::TestDir::new(name);
        let paths = ClusterKvPaths::new(&root);
        let node_id = NodeId::from_legacy_named(format!("{name}-local")).unwrap();
        let pin =
            pin_resident_raft_node_registry_for_test(&paths.raft_wal_paths(&node_id)).unwrap();
        let config = ClusterKvRaftConfigRecord {
            cluster_id: Id::from_legacy_named(format!("{name}-cluster")).unwrap(),
            term: 7,
            kv_revision: 0,
            voters: vec![
                node_id.clone(),
                NodeId::from_legacy_named(format!("{name}-peer-a")).unwrap(),
            ],
            learners: Vec::new(),
            non_voting: Vec::new(),
            updated_at: OffsetDateTime::now_utc(),
        };
        paths.raft_store().set_leader_compact(&node_id, 7).unwrap();
        paths
            .raft_store()
            .append_command(
                &node_id,
                KvCommand::Op(
                    KvOp::put_serde(cluster_kv_raft_config_key(), &config, None).unwrap(),
                ),
            )
            .unwrap();
        (root, paths, node_id, pin, config)
    }

    #[test]
    fn configuration_floor_reads_warm_and_checked_out_completed_metadata() {
        let (_root, paths, node_id, _pin, config) = fixture("kv-configuration-floor-owned");
        let before = read_cluster_kv_resident_configuration_floor(&paths, &node_id).unwrap();
        assert_eq!(before.observed_term, 7);
        assert_eq!(
            before
                .value
                .as_ref()
                .unwrap()
                .decode::<ClusterKvRaftConfigRecord>()
                .unwrap(),
            config
        );
        let mut owner = ClusterKvRaftLocalCommandStorage::new_read_index_with_operation_deadline(
            paths.raft_store(),
            node_id.clone(),
            |_| false,
            Instant::now() + Duration::from_secs(1),
        );
        owner.read_index_state().unwrap();
        let floor = read_cluster_kv_resident_configuration_floor(&paths, &node_id).unwrap();
        assert_eq!(floor.value, before.value);
        assert_eq!(floor.revision, before.revision);
        assert_eq!(floor.observed_term, before.observed_term);
        assert_eq!(floor.applied_index, before.applied_index);
        assert_eq!(floor.known_leader, before.known_leader);
        let slot = RESIDENT_RAFT_NODES
            .lock()
            .unwrap()
            .get(&paths.raft_wal_paths(&node_id).root)
            .cloned()
            .unwrap();
        assert!(
            slot.state.lock().unwrap().checkout_waiters.is_empty(),
            "floor reads must not enqueue mutable checkout"
        );
        drop(owner);
    }

    #[test]
    fn configuration_floor_joins_metadata_handoff_without_mutable_admission() {
        let (_root, paths, node_id, _pin, _) = fixture("kv-configuration-floor-handoff");
        let wal_paths = paths.raft_wal_paths(&node_id);
        let slot = RESIDENT_RAFT_NODES
            .lock()
            .unwrap()
            .get(&wal_paths.root)
            .cloned()
            .unwrap();
        let metadata_handoff = slot.state.lock().unwrap();
        let reader_paths = paths.clone();
        let reader_node_id = node_id.clone();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let reader = thread::spawn(move || {
            started_tx.send(()).unwrap();
            result_tx
                .send(read_cluster_kv_resident_configuration_floor(
                    &reader_paths,
                    &reader_node_id,
                ))
                .unwrap();
        });
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(matches!(
            result_rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        drop(metadata_handoff);
        assert!(
            result_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .is_ok(),
            "short metadata contention must not become Busy"
        );
        reader.join().unwrap();
        assert!(slot.state.lock().unwrap().checkout_waiters.is_empty());
    }

    #[test]
    fn configuration_floor_cold_read_never_hydrates_or_uses_materialized_checkpoint() {
        let root = sinomo_testdir::TestDir::new("kv-configuration-floor-cold");
        let paths = ClusterKvPaths::new(&root);
        let node_id = NodeId::from_legacy_named("configuration-floor-cold-node").unwrap();
        fs::write(&paths.snapshot, b"invalid materialized checkpoint").unwrap();
        let wal_paths = paths.raft_wal_paths(&node_id);
        assert!(!registered_resident_raft_node_exists(&wal_paths));
        assert!(matches!(
            read_cluster_kv_resident_configuration_floor(&paths, &node_id),
            Err(KvError::Busy { .. })
        ));
        assert!(
            !registered_resident_raft_node_exists(&wal_paths),
            "read must not create a resident slot"
        );
        assert!(
            !wal_paths.root.exists(),
            "read must not create or open durable Raft state"
        );
    }

    #[test]
    fn configuration_floor_rejects_pending_higher_term_during_mutable_ownership() {
        let (_root, paths, node_id, _pin, _) = fixture("kv-configuration-floor-term");
        let store = paths.raft_store();
        let mut owner = ClusterKvRaftLocalCommandStorage::new_read_index_with_operation_deadline(
            store.clone(),
            node_id.clone(),
            |_| false,
            Instant::now() + Duration::from_secs(1),
        );
        owner.read_index_state().unwrap();
        store.publish_pending_higher_term(&node_id, 8).unwrap();
        assert!(matches!(
            read_cluster_kv_resident_configuration_floor(&paths, &node_id),
            Err(KvError::AuthorityUnavailable { .. })
        ));
        drop(owner);
        assert!(matches!(
            read_cluster_kv_resident_configuration_floor(&paths, &node_id),
            Err(KvError::AuthorityUnavailable { .. })
        ));
        store.observe_term_compact(&node_id, 8).unwrap();
        assert_eq!(
            read_cluster_kv_resident_configuration_floor(&paths, &node_id)
                .unwrap()
                .observed_term,
            8
        );
    }

    #[test]
    fn configuration_floor_preserves_exact_same_count_membership_changes() {
        let (_root, paths, node_id, _pin, mut config) =
            fixture("kv-configuration-floor-membership");
        let first = read_cluster_kv_resident_configuration_floor(&paths, &node_id).unwrap();
        config.voters[1] =
            NodeId::from_legacy_named("configuration-floor-replacement-peer").unwrap();
        paths
            .raft_store()
            .append_command(
                &node_id,
                KvCommand::Op(
                    KvOp::put_serde(cluster_kv_raft_config_key(), &config, None).unwrap(),
                ),
            )
            .unwrap();
        let second = read_cluster_kv_resident_configuration_floor(&paths, &node_id).unwrap();
        assert!(second.revision > first.revision);
        assert!(second.applied_index > first.applied_index);
        assert_ne!(second.value, first.value);
        let observed = second
            .value
            .unwrap()
            .decode::<ClusterKvRaftConfigRecord>()
            .unwrap();
        assert_eq!(
            observed.voters.len(),
            first
                .value
                .unwrap()
                .decode::<ClusterKvRaftConfigRecord>()
                .unwrap()
                .voters
                .len()
        );
        assert_eq!(
            observed, config,
            "membership cardinality cannot stand in for exact committed configuration"
        );
    }

    #[test]
    fn configuration_floor_rejects_wrong_owner_and_checked_out_storage_replacement() {
        let (_root, paths, node_id, _pin, _) = fixture("kv-configuration-floor-storage");
        let wal_paths = paths.raft_wal_paths(&node_id);
        let wrong_node = NodeId::from_legacy_named("configuration-floor-wrong-owner").unwrap();
        assert!(matches!(
            read_resident_raft_configuration_floor(&wal_paths, &wrong_node),
            Err(KvError::Busy { .. })
        ));
        assert!(read_cluster_kv_resident_configuration_floor(&paths, &node_id).is_ok());
        let mut owner = ClusterKvRaftLocalCommandStorage::new_read_index_with_operation_deadline(
            paths.raft_store(),
            node_id.clone(),
            |_| false,
            Instant::now() + Duration::from_secs(1),
        );
        owner.read_index_state().unwrap();
        fs::rename(&wal_paths.lock, wal_paths.root.join("previous-owner.lock")).unwrap();
        fs::write(&wal_paths.lock, []).unwrap();
        assert!(
            matches!(
                read_cluster_kv_resident_configuration_floor(&paths, &node_id),
                Err(KvError::Busy { .. })
            ),
            "retained metadata cannot outlive exact storage ownership"
        );
        drop(owner);
        assert!(matches!(
            read_cluster_kv_resident_configuration_floor(&paths, &node_id),
            Err(KvError::Busy { .. })
        ));
    }
}

#[cfg(test)]
mod resident_checkout_tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Barrier, mpsc};

    fn wait_for_checkout_queue_depth(slot: &ResidentRaftNodeSlot, expected: usize) {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let depth = slot
                .state
                .lock()
                .expect("resident slot state")
                .checkout_waiters
                .len();
            if depth == expected {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "resident checkout queue did not reach depth {expected}; observed {depth}"
            );
            thread::yield_now();
        }
    }

    #[test]
    fn post_quorum_projection_waits_for_transient_resident_owner() {
        let raft_root = std::env::temp_dir().join(format!(
            "sinomo-kv-resident-projection-wait-{}-{}",
            std::process::id(),
            RESIDENT_RAFT_NODE_ACCESS_CLOCK.fetch_add(1, Ordering::Relaxed)
        ));
        let node_id = NodeId::from_legacy_named("node_resident_projection_wait").expect("node id");
        let wal_paths = KvWalPaths::new(raft_root.join(node_id.as_str()));
        drop(
            DurableRaftKvNode::open_resident(node_id.clone(), wal_paths.clone())
                .expect("seed resident owner"),
        );

        let held = DurableRaftKvNode::open_resident(node_id.clone(), wal_paths.clone())
            .expect("hold resident mutation owner");
        let expected_term = held.hard_state().current_term;
        assert!(
            with_cluster_kv_resident_read_projection(&wal_paths, &node_id, |_| ()).is_none(),
            "the immediate projection must remain nonblocking"
        );
        let release = Arc::new(Barrier::new(2));
        let release_worker = Arc::clone(&release);
        let owner = thread::spawn(move || {
            release_worker.wait();
            thread::sleep(Duration::from_millis(20));
            drop(held);
        });
        release.wait();
        let started = Instant::now();
        let projected = with_cluster_kv_resident_read_projection_before(
            &wal_paths,
            &node_id,
            Instant::now() + Duration::from_secs(1),
            |projection| projection.read_index_state(),
        )
        .expect("resident projection becomes available")
        .expect("project resident state");
        assert!(started.elapsed() >= Duration::from_millis(10));
        assert_eq!(projected.value.current_term, expected_term);
        owner.join().expect("release resident owner");

        let held = DurableRaftKvNode::open_resident(node_id.clone(), wal_paths.clone())
            .expect("hold resident owner past deadline");
        let callback_ran = AtomicBool::new(false);
        assert!(
            with_cluster_kv_resident_read_projection_before(
                &wal_paths,
                &node_id,
                Instant::now() + Duration::from_millis(20),
                |_| callback_ran.store(true, Ordering::SeqCst),
            )
            .is_none(),
            "the projection must preserve the caller's absolute deadline"
        );
        assert!(!callback_ran.load(Ordering::SeqCst));
        drop(held);
        let _ = fs::remove_dir_all(raft_root);
    }

    #[test]
    fn shadow_role_denial_drops_cached_owner_before_a_fresh_main_open() {
        let fixture = sinomo_testdir::TestDir::new("sinomo-kv-shadow-denial");
        let raft_root = fixture.to_path_buf();
        let node_id = NodeId::from_legacy_named("node_shadow_denial_fresh_main").expect("node id");
        let wal_paths = KvWalPaths::new(raft_root.join(node_id.as_str()));

        drop(
            DurableRaftKvNode::open_resident(node_id.clone(), wal_paths.clone())
                .expect("seed resident owner"),
        );
        assert!(cached_resident_raft_node_exists_for_test(&wal_paths));

        deny_process_local_raft_access_for_root(raft_root.clone())
            .expect("install one-way shadow denial");
        assert!(process_local_raft_access_is_denied(&wal_paths.root));
        assert!(
            !registered_resident_raft_node_exists(&wal_paths),
            "installing the shadow boundary must drop an idle cached node.lock owner"
        );
        assert!(matches!(
            DurableRaftKvNode::open_resident(node_id.clone(), wal_paths.clone()),
            Err(KvError::AuthorityUnavailable { .. })
        ));
        assert!(matches!(
            DurableRaftKvNode::open(node_id.clone(), wal_paths.clone()),
            Err(KvError::AuthorityUnavailable { .. })
        ));

        assert!(clear_process_local_raft_access_denial_for_test(&raft_root));
        drop(
            DurableRaftKvNode::open(node_id, wal_paths)
                .expect("a fresh main process can open after the shadow exits"),
        );
    }

    #[test]
    fn shadow_role_denial_evicts_and_rejects_preseeded_immutable_raft_view() {
        let fixture = sinomo_testdir::TestDir::new("sinomo-kv-shadow-view-denial");
        let raft_root = fixture.to_path_buf();
        let node_id =
            NodeId::from_legacy_named("node_shadow_denial_immutable_view").expect("node id");
        let wal_paths = KvWalPaths::new(raft_root.join(node_id.as_str()));
        drop(
            DurableRaftKvNode::open(node_id.clone(), wal_paths.clone()).expect("seed durable node"),
        );
        wal_paths
            .write_persisted_hard_state(&RaftHardState::default())
            .expect("seed durable Raft identity");
        let cache = KvLocalCache::new(raft_root.join("materialized.sobj"), raft_root.clone());
        let identity = raft_view_identity(&wal_paths).expect("durable view identity");
        let snapshot = InProcessKvStore::new()
            .try_snapshot()
            .expect("empty snapshot");
        cache_store_raft_view(
            &wal_paths,
            node_id.clone(),
            identity,
            Arc::new(RaftLocalReadView::from_snapshot_for_test(&snapshot).expect("read view")),
        );
        assert!(cache_lookup_raft_view(&wal_paths, &node_id, identity).is_some());

        deny_process_local_raft_access_for_root(raft_root.clone()).expect("install shadow denial");
        assert!(
            cache_lookup_raft_view(&wal_paths, &node_id, identity).is_none(),
            "denial must evict and reject a voter-derived immutable hot view"
        );
        assert!(matches!(
            cache.current_snapshot_arc_without_materialized_refresh(node_id),
            Err(KvError::AuthorityUnavailable { .. })
        ));
        assert!(clear_process_local_raft_access_denial_for_test(&raft_root));
    }

    #[test]
    fn active_registry_overflow_preserves_same_root_coordination() {
        let prefix = "/resident-registry-active-overflow";
        let roots = (0..RESIDENT_RAFT_NODE_CAPACITY)
            .map(|index| PathBuf::from(format!("{prefix}-{index}")))
            .collect::<Vec<_>>();
        let mut slots = HashMap::new();
        let retained = roots
            .iter()
            .map(|root| resident_raft_node_slot_in_registry(&mut slots, root).0)
            .collect::<Vec<_>>();
        let overflow_root = PathBuf::from(format!("{prefix}-overflow"));

        let (first, admission) = resident_raft_node_slot_in_registry(&mut slots, &overflow_root);
        let second = resident_raft_node_slot_in_registry(&mut slots, &overflow_root).0;
        assert_eq!(admission.evictions, 0);
        assert!(admission.active_overflow);
        assert!(
            Arc::ptr_eq(&first, &second),
            "capacity pressure must not split one durable root across independent coordination slots"
        );
        assert_eq!(slots.len(), RESIDENT_RAFT_NODE_CAPACITY + 1);
        drop((second, first, retained));

        let next_root = PathBuf::from(format!("{prefix}-after-release"));
        let (_, pruned) = resident_raft_node_slot_in_registry(&mut slots, &next_root);
        assert_eq!(slots.len(), RESIDENT_RAFT_NODE_CAPACITY);
        assert_eq!(pruned.evictions, 2);
        assert!(!pruned.active_overflow);
    }

    #[test]
    fn registry_capacity_evicts_least_recently_used_inactive_root() {
        let prefix = "/resident-registry-recency";
        let roots = (0..RESIDENT_RAFT_NODE_CAPACITY)
            .map(|index| PathBuf::from(format!("{prefix}-{index}")))
            .collect::<Vec<_>>();
        let mut slots = HashMap::new();
        for root in &roots {
            drop(resident_raft_node_slot_in_registry(&mut slots, root).0);
        }

        let recently_used = roots[0].clone();
        drop(resident_raft_node_slot_in_registry(
            &mut slots,
            &recently_used,
        ));
        let oldest = roots[1].clone();
        let replacement = PathBuf::from(format!("{prefix}-replacement"));
        let (_, admission) = resident_raft_node_slot_in_registry(&mut slots, &replacement);

        assert_eq!(admission.evictions, 1);
        assert!(slots.contains_key(&recently_used));
        assert!(slots.contains_key(&replacement));
        assert!(
            !slots.contains_key(&oldest),
            "bounded admission must discard the least recently used inactive root"
        );
    }

    #[test]
    fn resident_checkout_admission_is_fifo_under_contention() {
        let slot = Arc::new(ResidentRaftNodeSlot::default());
        let root = PathBuf::from("/resident-checkout-fifo-test");
        slot.state.lock().expect("resident slot state").checked_out = true;
        let (acquired_tx, acquired_rx) = mpsc::channel();
        let mut waiters = Vec::new();

        for waiter_id in 0..4_u64 {
            let waiter_slot = Arc::clone(&slot);
            let root = root.clone();
            let acquired_tx = acquired_tx.clone();
            waiters.push(thread::spawn(move || {
                let cached = waiter_slot
                    .checkout(&root, Duration::from_secs(2))
                    .expect("queued resident checkout");
                assert!(cached.is_none());
                acquired_tx.send(waiter_id).expect("report checkout");
            }));
            wait_for_checkout_queue_depth(&slot, waiter_id as usize + 1);
        }
        drop(acquired_tx);

        {
            let mut state = slot.state.lock().expect("resident slot state");
            state.checked_out = false;
            slot.ready.notify_all();
        }
        for expected in 0..4_u64 {
            assert_eq!(
                acquired_rx
                    .recv_timeout(Duration::from_secs(1))
                    .expect("next FIFO checkout"),
                expected
            );
            let mut state = slot.state.lock().expect("resident slot state");
            assert!(state.checked_out);
            state.checked_out = false;
            slot.ready.notify_all();
        }
        for waiter in waiters {
            waiter.join().expect("join resident checkout waiter");
        }
        assert!(
            slot.state
                .lock()
                .expect("resident slot state")
                .checkout_waiters
                .is_empty()
        );
    }

    #[test]
    fn timed_out_resident_checkout_removes_its_fifo_ticket() {
        let slot = ResidentRaftNodeSlot::default();
        let root = Path::new("/resident-checkout-timeout-test");
        slot.state.lock().expect("resident slot state").checked_out = true;

        assert!(matches!(
            slot.checkout(root, Duration::ZERO),
            Err(KvError::Busy { .. })
        ));
        assert!(
            slot.state
                .lock()
                .expect("resident slot state")
                .checkout_waiters
                .is_empty()
        );
    }

    #[test]
    fn zero_budget_resident_checkout_is_a_nonblocking_try_acquire() {
        let slot = ResidentRaftNodeSlot::default();
        let root = Path::new("/resident-checkout-expired-available-test");

        assert!(
            slot.checkout(root, Duration::ZERO)
                .expect("try acquire")
                .is_none()
        );
        let state = slot.state.lock().expect("resident slot state");
        assert!(
            state.checked_out,
            "an immediately free slot must be acquired"
        );
        assert!(
            state.checkout_waiters.is_empty(),
            "a successful checkout must dequeue its FIFO ticket"
        );
    }

    #[test]
    fn expired_nonzero_resident_checkout_cannot_acquire_an_available_slot() {
        let slot = ResidentRaftNodeSlot::default();
        let root = Path::new("/resident-checkout-expired-nonzero-available-test");

        assert!(matches!(
            slot.checkout(root, Duration::from_nanos(1)),
            Err(KvError::Busy { .. })
        ));
        let state = slot.state.lock().expect("resident slot state");
        assert!(!state.checked_out);
        assert!(state.checkout_waiters.is_empty());
    }

    #[test]
    fn cold_read_handoff_purges_an_expired_front_ticket() {
        let slot = ResidentRaftNodeSlot::default();
        let mut state = slot.state.lock().expect("resident slot state");
        state.read_loading = true;
        state.read_load_preempt_requested = true;
        state
            .checkout_waiters
            .push_back(ResidentRaftCheckoutWaiter {
                ticket: 7,
                deadline: Instant::now(),
            });

        slot.remove_expired_checkout_waiters(&mut state, Instant::now());

        assert!(state.checkout_waiters.is_empty());
        assert!(
            !state.read_load_preempt_requested,
            "an expired writer cannot remain a restored-node handoff target"
        );
    }
}

#[cfg(test)]
mod cancelled_admitted_native_apply_tests {
    use super::*;
    use std::sync::mpsc;

    #[test]
    fn cancelled_caller_keeps_actual_proposal_owner_through_admitted_apply() {
        for (checkpoint, subscribed) in [(false, true), (true, true), (false, false), (true, false)]
        {
            let root = sinomo_testdir::TestDir::new(if checkpoint {
                "sinomo-kv-cancelled-admitted-checkpoint"
            } else {
                "sinomo-kv-cancelled-admitted-delta"
            });
            let paths = ClusterKvPaths::new(root.to_path_buf());
            let node_id = NodeId::from_legacy_named("node_cancelled_admitted_apply").unwrap();
            let store = ClusterKvRaftStore::new(
                paths.clone(),
                ClusterKvRaftCompactionPolicy {
                    max_snapshot_bytes: if checkpoint { 0 } else { u64::MAX },
                    max_snapshot_events: if checkpoint {
                        0
                    } else {
                        crate::MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS
                    },
                },
            );
            store.set_leader_compact(&node_id, 1).unwrap();
            if checkpoint {
                // The real compaction policy requires a shrinkable existing
                // snapshot and retained events above its configured limit.
                store.open_node(&node_id).unwrap().save_snapshot().unwrap();
            }
            let observed = Arc::new(Mutex::new(Vec::new()));
            let seen = Arc::clone(&observed);
            let _subscription = subscribed.then(|| {
                crate::subscribe_committed_observations(&paths, move |event| {
                    seen.lock().unwrap().push(event.clone());
                })
            });
            let capacity = crate::try_admit_committed_observation(
                &paths,
                crate::KvObservationProposalOwnership::Required,
            )
            .unwrap();
            let mut storage = ClusterKvRaftLocalCommandStorage::new(
                store.clone(),
                node_id.clone(),
                crate::cluster_kv_key_can_defer_materialized_snapshot as fn(&str) -> bool,
            )
            .with_observation_capacity(capacity);
            let proposal_fence = storage.observation_proposal_fence().unwrap();
            assert!(
                proposal_fence.is_some(),
                "canonical native proposal ownership exists even without subscribers"
            );
            let proposal = ClusterKvRaftCommitSequenceGuard::new(
                crate::lock_cluster_kv_commit_sequence(&paths, Duration::from_secs(1)).unwrap(),
                proposal_fence,
            );
            let key = "/dfs/filesystems/cancelled/metadata-shards/root";
            let entry = storage
                .append_uncommitted(KvCommandRequest::generated(KvCommand::Op(
                    crate::KvOp::Put {
                        key: key.into(),
                        value: b"native-after-caller-cancellation".to_vec(),
                        lease_id: None,
                    },
                )))
                .unwrap();
            storage.release_session_for_quorum();
            let index = entry.index;
            let (admitted_tx, admitted_rx) = mpsc::sync_channel(1);
            let (release_tx, release_rx) = mpsc::sync_channel(1);
            let worker = thread::spawn(move || {
                // This is the real canonical storage/node scope and provisional
                // receipt immediately before persistence/apply. Hold that cut
                // deterministically while the async caller's owner disappears.
                storage
                    .node_mut()
                    .unwrap()
                    .begin_observation_effect(KvCommittedObservationKind::Contiguous)
                    .unwrap();
                admitted_tx.send(()).unwrap();
                release_rx.recv().expect("release admitted native work");
                let result = storage.commit_index_compact(index);
                let result = result.map(|commit| {
                    let revision = commit.read_state.revision;
                    assert_eq!(commit.checkpoint_snapshot.is_some(), checkpoint);
                    drop(commit);
                    revision
                });
                storage.release_session_after_commit();
                result
            });
            admitted_rx.recv_timeout(Duration::from_secs(2)).unwrap();
            drop(proposal);

            let contending =
                crate::lock_cluster_kv_commit_sequence(&paths, Duration::from_millis(25));
            let held = matches!(contending, Err(KvError::Busy { .. }));
            drop(contending);
            release_tx.send(()).unwrap();
            let revision = worker.join().unwrap().expect(
                "admitted native apply must advance its original receipt after caller cancellation",
            );
            assert!(
                held,
                "actual proposal flock remains owned until admitted native work finishes"
            );

            let events = observed.lock().unwrap();
            if subscribed {
                assert_eq!(events.len(), if checkpoint { 2 } else { 1 });
                assert!(
                    matches!(
                        events[0].value,
                        crate::KvCommittedObservationValue::Delta(_)
                    ),
                    "completed captured work must publish its delta rather than an artificial gap"
                );
                assert_eq!(events[0].revision, Some(revision));
                assert_eq!(events[0].applied_index, Some(index));
                if checkpoint {
                    assert_eq!(events[1].kind, KvCommittedObservationKind::Checkpoint);
                    assert!(matches!(
                        events[1].value,
                        crate::KvCommittedObservationValue::Snapshot(_)
                    ));
                }
            } else {
                assert!(events.is_empty());
            }
            drop(events);

            // No later mutation, explicit cold load, or cached-proof shortcut.
            let mut read = ClusterKvRaftLocalCommandStorage::new_read_index_with_operation_deadline(
                store,
                node_id,
                crate::cluster_kv_key_can_defer_materialized_snapshot as fn(&str) -> bool,
                Instant::now() + Duration::from_secs(1),
            );
            assert_eq!(read.read_index_state().unwrap().applied_index, index);
            assert_eq!(
                read.read_index_entry(key).unwrap().unwrap().value,
                b"native-after-caller-cancellation"
            );
        }
    }

    #[test]
    fn cancelled_caller_before_storage_custody_rejects_apply_without_losing_resident() {
        let root = sinomo_testdir::TestDir::new("sinomo-kv-cancelled-before-storage-custody");
        let paths = ClusterKvPaths::new(root.to_path_buf());
        let node_id = NodeId::from_legacy_named("node_cancelled_before_storage_custody").unwrap();
        let store = ClusterKvRaftStore::new(
            paths.clone(),
            ClusterKvRaftCompactionPolicy {
                max_snapshot_bytes: u64::MAX,
                max_snapshot_events: crate::MAX_CLUSTER_KV_RAFT_SNAPSHOT_EVENTS,
            },
        );
        store.set_leader_compact(&node_id, 1).unwrap();
        let committed = store
            .append_command_with_response_and_publish_report(
                &node_id,
                KvCommand::Op(crate::KvOp::Put {
                    key: "/dfs/filesystems/custody/seed".into(),
                    value: b"current-term-native-prefix".to_vec(),
                    lease_id: None,
                }),
                |_| false,
            )
            .unwrap()
            .0;
        let applied_before = committed.committed_request.log_index;
        let revision_before = committed.snapshot.revision;
        drop(committed);
        let observed = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&observed);
        let _subscription = crate::subscribe_committed_observations(&paths, move |event| {
            seen.lock().unwrap().push(event.clone());
        });
        let capacity = crate::try_admit_committed_observation(
            &paths,
            crate::KvObservationProposalOwnership::Required,
        )
        .unwrap();
        let mut storage = ClusterKvRaftLocalCommandStorage::new(
            store.clone(),
            node_id.clone(),
            crate::cluster_kv_key_can_defer_materialized_snapshot as fn(&str) -> bool,
        )
        .with_observation_capacity(capacity);
        let proposal = ClusterKvRaftCommitSequenceGuard::new(
            crate::lock_cluster_kv_commit_sequence(&paths, Duration::from_secs(1)).unwrap(),
            storage.observation_proposal_fence().unwrap(),
        );
        let key = "/dfs/filesystems/custody/not-applied";
        let pending = storage
            .append_uncommitted(KvCommandRequest::generated(KvCommand::Op(
                crate::KvOp::Put {
                    key: key.into(),
                    value: b"must-remain-pending".to_vec(),
                    lease_id: None,
                },
            )))
            .unwrap();
        storage.release_session_for_quorum();

        // Cancellation wins before the new canonical storage phase can retain
        // actual proposal custody. Reopening for preflight/read-only work is
        // still legitimate, but this source cannot begin another apply.
        drop(proposal);
        let error = storage.commit_index_compact(pending.index).unwrap_err();
        assert!(matches!(error, KvError::Serialization { detail }
            if detail.contains("requires an active proposal owner")));
        storage.release_session_after_commit();
        drop(storage);
        assert!(
            observed.lock().unwrap().is_empty(),
            "definite no-apply rejection publishes neither a delta nor a gap"
        );

        // Rejection before the durability boundary preserves native ownership.
        // Do not seed another mutation or open an unbounded recovery session.
        let mut read = ClusterKvRaftLocalCommandStorage::new_read_index_with_operation_deadline(
            store,
            node_id,
            crate::cluster_kv_key_can_defer_materialized_snapshot as fn(&str) -> bool,
            Instant::now() + Duration::from_secs(1),
        );
        let state = read
            .read_index_state()
            .expect("unchanged native resident remains readable");
        assert_eq!(state.applied_index, applied_before);
        assert_eq!(state.revision, revision_before);
        assert!(read.read_index_entry(key).unwrap().is_none());
        assert_eq!(
            read.read_index_entry("/dfs/filesystems/custody/seed")
                .unwrap()
                .unwrap()
                .value,
            b"current-term-native-prefix"
        );
    }
}
