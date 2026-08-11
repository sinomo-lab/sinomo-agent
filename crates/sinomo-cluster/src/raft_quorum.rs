use crate::control_plane_driver::{ClusterControlPlaneDriver, ClusterControlPlaneTransport};
#[cfg(any(test, feature = "test-support"))]
use crate::control_plane_options::ClusterControlPlaneTimeouts;
use crate::control_plane_options::ClusterRaftAppendReplicationOptions;
use crate::kv_local_store::ClusterKvPathsExt;
use crate::kv_local_store::LocalClusterRaftCommandStorage;
use crate::kv_local_store::LocalClusterRaftReadIndexAdmission;
use crate::kv_local_store::LocalClusterRaftReadProjection;
use crate::kv_local_store::kv_err;
use crate::local_state_store::project_cluster_raft_config_record;
use crate::member_store::merge_cluster_state_from_peer;
use crate::prelude::*;
#[allow(unused_imports)]
use sinomo_kv::KvError;
use sinomo_kv::{
    ClusterKvRaftQuorumCommitOptions, ClusterKvRaftQuorumCommitResult, ClusterRaftAppendRequest,
    ClusterRaftAppendResponse,
};
use sinomo_kv::{KvCommandRequest, KvEvent, KvRequestId, KvSnapshot};
use sinomo_network::ClusterConnectivityState;
use sinomo_network::ClusterMembershipRpcFuture;
use sinomo_network::ClusterPaths;
use sinomo_network::NetworkError;
use sinomo_network::{cleanup_member_endpoint_duplicates, upsert_member_endpoint};
use sinomo_node::ClusterCapabilityPolicyRecord;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, Weak};
use std::time::{Duration, Instant};

type SharedLocalClusterRaftCommandStorage = Arc<Mutex<LocalClusterRaftCommandStorage>>;

#[derive(Debug)]
struct ClusterRaftHigherTermPersistenceWorker {
    wake_state: Mutex<ClusterRaftHigherTermPersistenceWakeState>,
    wake: Condvar,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ClusterRaftHigherTermPersistenceWakeState {
    revision: u64,
    target_term: Option<u64>,
    starting: bool,
    running: bool,
    #[cfg(test)]
    start_waiters: usize,
}

impl Default for ClusterRaftHigherTermPersistenceWorker {
    fn default() -> Self {
        Self {
            wake_state: Mutex::new(ClusterRaftHigherTermPersistenceWakeState::default()),
            wake: Condvar::new(),
        }
    }
}

static CLUSTER_RAFT_HIGHER_TERM_PERSISTENCE_WORKERS: OnceLock<
    Mutex<BTreeMap<PathBuf, Weak<ClusterRaftHigherTermPersistenceWorker>>>,
> = OnceLock::new();
#[cfg(test)]
static CLUSTER_RAFT_HIGHER_TERM_INJECTED_FAILURES: OnceLock<Mutex<BTreeMap<PathBuf, usize>>> =
    OnceLock::new();
#[cfg(test)]
#[derive(Clone)]
struct ClusterRaftHigherTermInjectedStartFailure {
    entered: Arc<std::sync::Barrier>,
    release: Arc<std::sync::Barrier>,
}
#[cfg(test)]
static CLUSTER_RAFT_HIGHER_TERM_INJECTED_START_FAILURES: OnceLock<
    Mutex<BTreeMap<PathBuf, ClusterRaftHigherTermInjectedStartFailure>>,
> = OnceLock::new();

#[cfg(test)]
fn take_cluster_raft_higher_term_injected_failure(root: &PathBuf) -> bool {
    let failures =
        CLUSTER_RAFT_HIGHER_TERM_INJECTED_FAILURES.get_or_init(|| Mutex::new(BTreeMap::new()));
    let mut failures = failures
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(remaining) = failures.get_mut(root) else {
        return false;
    };
    if *remaining == 0 {
        failures.remove(root);
        return false;
    }
    *remaining -= 1;
    if *remaining == 0 {
        failures.remove(root);
    }
    true
}

#[cfg(test)]
fn take_cluster_raft_higher_term_injected_start_failure(root: &PathBuf) -> Option<std::io::Error> {
    let failure = CLUSTER_RAFT_HIGHER_TERM_INJECTED_START_FAILURES
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(root)?;
    failure.entered.wait();
    failure.release.wait();
    Some(std::io::Error::other(
        "injected higher-term worker start failure",
    ))
}

#[cfg(not(test))]
fn take_cluster_raft_higher_term_injected_start_failure(_root: &PathBuf) -> Option<std::io::Error> {
    None
}

struct ClusterRaftHigherTermStartReservation {
    worker: Arc<ClusterRaftHigherTermPersistenceWorker>,
    resolved: bool,
}

impl ClusterRaftHigherTermStartReservation {
    fn publish_running(mut self) {
        let mut wake_state = self
            .worker
            .wake_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        debug_assert!(wake_state.starting);
        debug_assert!(!wake_state.running);
        wake_state.starting = false;
        wake_state.running = true;
        self.resolved = true;
        self.worker.wake.notify_all();
    }
}

impl Drop for ClusterRaftHigherTermStartReservation {
    fn drop(&mut self) {
        if self.resolved {
            return;
        }
        let mut wake_state = self
            .worker
            .wake_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        debug_assert!(wake_state.starting);
        debug_assert!(!wake_state.running);
        wake_state.starting = false;
        wake_state.running = false;
        // Keep revision/target intact. A publisher already waiting with this
        // worker Arc can now claim the idle reservation and start a successor.
        self.worker.wake.notify_all();
    }
}

fn cluster_raft_higher_term_persistence_worker(
    root: PathBuf,
) -> Arc<ClusterRaftHigherTermPersistenceWorker> {
    let registry =
        CLUSTER_RAFT_HIGHER_TERM_PERSISTENCE_WORKERS.get_or_init(|| Mutex::new(BTreeMap::new()));
    let mut workers = registry
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    workers.retain(|_, worker| worker.strong_count() > 0);
    if let Some(worker) = workers.get(&root).and_then(Weak::upgrade) {
        return worker;
    }
    let worker = Arc::new(ClusterRaftHigherTermPersistenceWorker::default());
    workers.insert(root, Arc::downgrade(&worker));
    worker
}

fn wait_for_cluster_raft_higher_term_retry(
    worker: &ClusterRaftHigherTermPersistenceWorker,
    observed_revision: u64,
    backoff: Duration,
) {
    let wake_state = worker
        .wake_state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let _ = worker
        .wake
        .wait_timeout_while(wake_state, backoff, |wake_state| {
            wake_state.revision == observed_revision
        })
        .unwrap_or_else(std::sync::PoisonError::into_inner);
}

fn cluster_raft_higher_term_worker_start_was_published(
    worker: &ClusterRaftHigherTermPersistenceWorker,
) -> bool {
    let mut wake_state = worker
        .wake_state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    while wake_state.starting {
        wake_state = worker
            .wake
            .wait(wake_state)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
    }
    wake_state.running
}

fn start_cluster_raft_higher_term_persistence_worker(
    paths: ClusterPaths,
    node_id: NodeId,
    root: PathBuf,
    reservation: ClusterRaftHigherTermStartReservation,
) -> Result<(), NetworkError> {
    let worker_state = Arc::clone(&reservation.worker);
    let spawn_result = match take_cluster_raft_higher_term_injected_start_failure(&root) {
        Some(error) => Err(error),
        None => std::thread::Builder::new()
            .name("sinomo-kv-higher-term".to_string())
            .spawn(move || {
                // The OS thread can run before `Builder::spawn` returns. Keep
                // it behind the start latch until the publisher has made the
                // successful spawn outcome visible to joined publishers.
                if cluster_raft_higher_term_worker_start_was_published(&worker_state) {
                    run_cluster_raft_higher_term_persistence_worker(paths, node_id, worker_state);
                }
            }),
    };
    match spawn_result {
        Ok(_worker_thread) => {
            reservation.publish_running();
            Ok(())
        }
        Err(source) => {
            let error = NetworkError::Io {
                operation: "start mandatory higher Raft term persistence".to_string(),
                detail: source.to_string(),
            };
            // Drop publishes Starting -> Idle and wakes any joined publisher
            // before this failed starter returns its error.
            drop(reservation);
            Err(error)
        }
    }
}

fn run_cluster_raft_higher_term_persistence_worker(
    paths: ClusterPaths,
    node_id: NodeId,
    worker: Arc<ClusterRaftHigherTermPersistenceWorker>,
) {
    let store = paths.kv_paths().raft_store();
    #[cfg(test)]
    let root = store.wal_paths(&node_id).root;
    let mut backoff = Duration::from_millis(10);
    loop {
        let (target_revision, target_term) = {
            let wake_state = worker
                .wake_state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (wake_state.revision, wake_state.target_term)
        };
        let pending_term = match store.pending_higher_term(&node_id).map_err(kv_err) {
            Ok(pending_term) => pending_term,
            Err(error) => {
                metrics::counter!("sinomo.kv.higher_term.persistence_failures").increment(1);
                tracing::warn!(
                    target: "sinomo.cluster.raft_term",
                    %node_id,
                    retry_ms = backoff.as_millis().min(u128::from(u64::MAX)) as u64,
                    %error,
                    "mandatory higher Raft term floor lookup failed; retrying reactively"
                );
                wait_for_cluster_raft_higher_term_retry(&worker, target_revision, backoff);
                backoff = backoff.saturating_mul(2).min(Duration::from_secs(1));
                continue;
            }
        };
        let term = match (target_term, pending_term) {
            (Some(target), Some(pending)) => Some(target.max(pending)),
            (Some(target), None) => Some(target),
            (None, Some(pending)) => Some(pending),
            (None, None) => None,
        };
        let Some(term) = term else {
            // Retire under the same small mutex publication uses to install a
            // verification target and reserve startup. A publisher either
            // claims Starting after this worker publishes Idle, or this worker
            // sees its target and keeps ownership. Never hold this mutex across
            // the gate lookup.
            let should_continue = {
                let mut wake_state = worker
                    .wake_state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if wake_state.target_term.is_some() {
                    true
                } else {
                    debug_assert!(!wake_state.starting);
                    debug_assert!(wake_state.running);
                    wake_state.running = false;
                    worker.wake.notify_all();
                    false
                }
            };
            if should_continue {
                continue;
            }

            // A non-reactive floor publication can race retirement too. Once
            // running is false, recheck both sources; either this worker
            // reclaims ownership or a reactive publisher starts its successor.
            let floor_outstanding = match store.pending_higher_term(&node_id) {
                Ok(pending) => pending.is_some(),
                Err(_) => true,
            };
            let mut wake_state = worker
                .wake_state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let work_outstanding = floor_outstanding || wake_state.target_term.is_some();
            if work_outstanding {
                if wake_state.starting || wake_state.running {
                    // A publisher claimed the idle lifecycle and either owns
                    // startup or already published its live successor.
                    return;
                }
                // This thread already exists, so it can reclaim Idle directly
                // as Running without a Starting reservation.
                wake_state.running = true;
                drop(wake_state);
                continue;
            }
            return;
        };

        metrics::counter!("sinomo.kv.higher_term.persistence_attempts").increment(1);
        #[cfg(test)]
        let injected_failure = take_cluster_raft_higher_term_injected_failure(&root);
        #[cfg(not(test))]
        let injected_failure = false;
        let result = if injected_failure {
            Err(NetworkError::Io {
                operation: "persist higher Raft term".to_string(),
                detail: "injected transient persistence failure".to_string(),
            })
        } else {
            let mut storage =
                sinomo_kv::ClusterKvRaftLocalCommandStorage::new_with_immediate_operation_admission(
                    store.clone(),
                    node_id.clone(),
                    sinomo_kv::cluster_kv_key_can_defer_materialized_snapshot as fn(&str) -> bool,
                );
            match storage.observe_pending_higher_term(term) {
                Ok(()) => Ok(()),
                Err(sinomo_kv::KvError::Busy { .. }) => {
                    metrics::counter!(
                        "sinomo.kv.higher_term.persistence_attempts",
                        "outcome" => "deferred_busy"
                    )
                    .increment(1);
                    wait_for_cluster_raft_higher_term_retry(&worker, target_revision, backoff);
                    backoff = backoff.saturating_mul(2).min(Duration::from_secs(1));
                    continue;
                }
                Err(error) => Err(kv_err(error)),
            }
        };
        match result {
            Ok(()) => {
                // The storage open/ACK is the durable-generation verification
                // even when publication was suppressed and the gate floor was
                // absent. Clear only the exact publication revision we acted
                // on; an equal/newer concurrent publication must be retried.
                let mut wake_state = worker
                    .wake_state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if wake_state.revision == target_revision
                    && wake_state.target_term.is_some_and(|target| target <= term)
                {
                    wake_state.target_term = None;
                }
                backoff = Duration::from_millis(10);
                continue;
            }
            Err(error) => {
                metrics::counter!("sinomo.kv.higher_term.persistence_failures").increment(1);
                tracing::warn!(
                    target: "sinomo.cluster.raft_term",
                    %node_id,
                    term,
                    retry_ms = backoff.as_millis().min(u128::from(u64::MAX)) as u64,
                    %error,
                    "mandatory higher Raft term persistence failed; retrying reactively"
                );
            }
        }
        wait_for_cluster_raft_higher_term_retry(&worker, target_revision, backoff);
        backoff = backoff.saturating_mul(2).min(Duration::from_secs(1));
    }
}

/// Raise the higher-term floor synchronously, then ensure exactly one
/// caller-independent worker per WAL root drives the latest floor to durable
/// hard state. Every publication wakes retries immediately and records a
/// monotonic verification target, including terms a process-local durable
/// watermark suppresses from the gate floor.
pub(crate) fn publish_cluster_raft_higher_term_reactively(
    paths: &ClusterPaths,
    node_id: &NodeId,
    term: u64,
) -> Result<(), NetworkError> {
    let store = paths.kv_paths().raft_store();
    store
        .publish_pending_higher_term(node_id, term)
        .map_err(kv_err)?;
    let root = store.wal_paths(node_id).root;
    let worker = cluster_raft_higher_term_persistence_worker(root.clone());
    let reservation = {
        let mut wake_state = worker
            .wake_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        wake_state.revision = wake_state.revision.wrapping_add(1);
        wake_state.target_term = Some(wake_state.target_term.unwrap_or_default().max(term));
        worker.wake.notify_all();
        loop {
            // A cleared target can only come from a successful worker attempt
            // whose snapshotted revision still matched the current revision.
            // It therefore proves this publication was durably consumed even
            // if the worker completed and retired before we reacquired here.
            if wake_state.target_term.is_none() {
                return Ok(());
            }
            if wake_state.running {
                return Ok(());
            }
            if !wake_state.starting {
                wake_state.starting = true;
                break ClusterRaftHigherTermStartReservation {
                    worker: Arc::clone(&worker),
                    resolved: false,
                };
            }
            // A reserved start is not a live worker. Join its outcome rather
            // than returning Ok; on failure this publisher can atomically
            // claim the idle lifecycle and start the retained target itself.
            #[cfg(test)]
            {
                wake_state.start_waiters = wake_state.start_waiters.saturating_add(1);
            }
            wake_state = worker
                .wake
                .wait(wake_state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            #[cfg(test)]
            {
                wake_state.start_waiters = wake_state.start_waiters.saturating_sub(1);
            }
        }
    };
    start_cluster_raft_higher_term_persistence_worker(
        paths.clone(),
        node_id.clone(),
        root,
        reservation,
    )
}

/// Per-target delivery metadata around an immutable append payload. Voters at
/// the same progress index share the payload allocation while transports keep
/// independent targets, deadlines, and outcomes.
pub type ClusterRaftQuorumBatch = Vec<(NodeId, Arc<ClusterRaftAppendRequest>, Duration)>;
pub type ClusterRaftQuorumBatchOutcomes = Vec<(
    NodeId,
    Arc<ClusterRaftAppendRequest>,
    Result<ClusterRaftAppendResponse, NetworkError>,
)>;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum ClusterRaftAppendRequestBuildKey {
    Snapshot,
    Tail(u64),
}

#[cfg(test)]
mod post_commit_observation_tests {
    use super::*;
    use sinomo_kv::{ClusterKvRaftConfigRecord, KvCommand, KvOp, encode_kv_value};
    use std::sync::{Condvar, mpsc};
    use time::OffsetDateTime;

    struct NoopRaftTransport;

    impl ClusterRaftQuorumAsyncHost for NoopRaftTransport {
        fn send_raft_append_to_node_async<'a>(
            &'a mut self,
            _paths: &'a ClusterPaths,
            _state: &'a ClusterBootstrapState,
            _target_node_id: &'a NodeId,
            _request: &'a ClusterRaftAppendRequest,
            _timeout: Duration,
        ) -> ClusterMembershipRpcFuture<'a, ClusterRaftAppendResponse> {
            Box::pin(async {
                Err(NetworkError::Io {
                    operation: "unexpected post-commit test Raft send".to_string(),
                    detail: "single-voter test must not use network transport".to_string(),
                })
            })
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blocking_delta_observer_runs_after_sequence_and_storage_locks_release() {
        let root = sinomo_testdir::TestDir::new("sinomo-cluster-post-commit-observer-locks");
        let data_dir = sinomo_local::SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let local = NodeId::from_legacy_named("node_post_commit_observer").expect("local node");
        let cluster_id =
            ClusterId::from_legacy_named("cluster_post_commit_observer").expect("cluster id");
        let now = OffsetDateTime::now_utc();
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "post commit observer",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        state.term = 7;
        let daemon_capability =
            sinomo_base::CapabilityName::new(sinomo_node::DAEMON_MAIN_RUN_CAPABILITY)
                .expect("daemon capability");
        state.members[0].capabilities = vec![daemon_capability.clone()];
        state.members[0].allowed_capabilities = vec![daemon_capability];
        let config = ClusterKvRaftConfigRecord {
            cluster_id,
            term: 7,
            kv_revision: 1,
            voters: vec![local.clone()],
            learners: Vec::new(),
            non_voting: Vec::new(),
            updated_at: now,
        };
        let kv_paths = paths.kv_paths();
        let mut node = kv_paths.raft_store().open_node(&local).expect("open node");
        node.become_leader(7).expect("become initial leader");
        node.append_local_command(
            7,
            KvCommand::Op(KvOp::Put {
                key: sinomo_kv::cluster_kv_raft_config_key(),
                value: encode_kv_value(&config).expect("encode config"),
                lease_id: None,
            }),
        )
        .expect("append config");
        let value_entry = node
            .append_local_command(
                7,
                KvCommand::Op(KvOp::Put {
                    key: "/post-commit/fixture".to_string(),
                    value: b"ready".to_vec(),
                    lease_id: None,
                }),
            )
            .expect("append fixture value");
        node.commit_to(value_entry.index).expect("commit fixture");
        state.kv_revision = node.store().revision().0;
        drop(node);
        crate::raft_election::locked_set_cluster_raft_leader(&paths, &local, 7)
            .expect("publish resident local leadership");

        let storage = Arc::new(Mutex::new(LocalClusterRaftCommandStorage::new(
            &paths,
            local.clone(),
        )));
        let observer_storage = Arc::clone(&storage);
        let observer_kv_paths = paths.kv_paths();
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let observer_release = Arc::clone(&release);
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let _subscription =
            crate::kv_snapshot_observer::subscribe_cluster_kv_committed_deltas(&paths, move |_| {
                let storage_guard = observer_storage
                    .try_lock()
                    .expect("post-commit observer must not run under the storage mutex");
                drop(storage_guard);
                let sequence_guard = sinomo_kv::lock_cluster_kv_commit_sequence(
                    &observer_kv_paths,
                    Duration::from_millis(100),
                )
                .expect("post-commit observer must not run under the proposal sequence lock");
                drop(sequence_guard);
                entered_tx.send(()).expect("report observer entry");
                let (released, wake) = &*observer_release;
                let mut released = released.lock().expect("observer release lock");
                while !*released {
                    released = wake.wait(released).expect("observer release wait");
                }
            });

        let request = KvCommandRequest::generated(KvCommand::Op(KvOp::Put {
            key: "/cluster/members/post-commit-observer".to_string(),
            value: b"member".to_vec(),
            lease_id: None,
        }));
        let trace = ClusterKvQuorumCommitTrace::new(
            request.request_id.clone(),
            local,
            state.term,
            state.kv_revision,
        );
        let runtime = tokio::runtime::Handle::current();
        let mut transport = NoopRaftTransport;
        let operation_deadline = Instant::now() + Duration::from_secs(1);
        let mut host = NetworkClusterKvRaftQuorumCommitAsyncHost {
            runtime: &runtime,
            paths: &paths,
            storage,
            operation_deadline,
            proposal_may_be_durable: false,
            append_options: ClusterControlPlaneTimeouts::default_daemon_transport()
                .raft_append_replication_options(Duration::from_secs(1)),
            trace,
            initial_term: state.term,
            initial_revision: state.kv_revision,
            host: &mut transport,
        };
        let commit = sinomo_kv::commit_cluster_kv_raft_command_compact_with_quorum_async(
            &mut state,
            request,
            ClusterKvRaftQuorumCommitOptions {
                timeout: Duration::from_secs(1),
            },
            &mut host,
        );
        let release_observer = async move {
            tokio::task::spawn_blocking(move || {
                let entered = entered_rx.recv_timeout(Duration::from_secs(2)).is_ok();
                let (released, wake) = &*release;
                *released.lock().expect("observer release lock") = true;
                wake.notify_all();
                entered
            })
            .await
            .expect("release observer task")
        };
        let (result, observer_entered) = tokio::join!(commit, release_observer);
        result.expect("commit completes after observer release");
        assert!(observer_entered, "blocking observer must be invoked");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn preappend_sequence_wait_honors_original_deadline_without_late_mutation() {
        let root = sinomo_testdir::TestDir::new("sinomo-cluster-preappend-sequence-deadline");
        let data_dir = sinomo_local::SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let local =
            NodeId::from_legacy_named("node_preappend_sequence_deadline").expect("local node");
        let cluster_id = ClusterId::from_legacy_named("cluster_preappend_sequence_deadline")
            .expect("cluster id");
        let now = OffsetDateTime::now_utc();
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "preappend sequence deadline",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        state.term = 11;
        let daemon_capability =
            sinomo_base::CapabilityName::new(sinomo_node::DAEMON_MAIN_RUN_CAPABILITY)
                .expect("daemon capability");
        state.members[0].capabilities = vec![daemon_capability.clone()];
        state.members[0].allowed_capabilities = vec![daemon_capability];
        let config = ClusterKvRaftConfigRecord {
            cluster_id,
            term: state.term,
            kv_revision: 1,
            voters: vec![local.clone()],
            learners: Vec::new(),
            non_voting: Vec::new(),
            updated_at: now,
        };
        let kv_paths = paths.kv_paths();
        let mut node = kv_paths.raft_store().open_node(&local).expect("open node");
        node.become_leader(state.term).expect("become leader");
        let config_entry = node
            .append_local_command(
                state.term,
                KvCommand::Op(KvOp::Put {
                    key: sinomo_kv::cluster_kv_raft_config_key(),
                    value: encode_kv_value(&config).expect("encode config"),
                    lease_id: None,
                }),
            )
            .expect("append config");
        node.commit_to(config_entry.index).expect("commit config");
        state.kv_revision = node.store().revision().0;
        let baseline_log_index = node.last_log_index();
        drop(node);
        crate::raft_election::locked_set_cluster_raft_leader(&paths, &local, state.term)
            .expect("publish resident local leadership");
        // Model a topology snapshot captured just before the durable election
        // and config apply. Authority admission must project the coherent
        // resident term/revision instead of rejecting the healthy leader and
        // retrying until the outer request expires.
        state.term = state.term.saturating_sub(1);
        state.kv_revision = 0;

        let sequence_guard =
            sinomo_kv::lock_cluster_kv_commit_sequence(&kv_paths, Duration::from_secs(1))
                .expect("hold proposal sequence lock");
        let key = "/deadline/preappend-must-not-appear";
        let request = KvCommandRequest::generated(KvCommand::Op(KvOp::Put {
            key: key.to_string(),
            value: b"late".to_vec(),
            lease_id: None,
        }));
        let timeout = Duration::from_millis(50);
        let started = Instant::now();
        let mut transport = NoopRaftTransport;
        let error = commit_cluster_raft_command_compact_with_quorum_async_with_host(
            &tokio::runtime::Handle::current(),
            &paths,
            &mut state,
            request,
            ClusterKvRaftQuorumCommitOptions { timeout },
            ClusterControlPlaneTimeouts::default_daemon_transport()
                .raft_append_replication_options(timeout),
            &mut transport,
        )
        .await
        .expect_err("blocked pre-append sequence admission must time out");
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_millis(500),
            "pre-append sequence admission returned after {elapsed:?}, not the caller budget"
        );
        assert!(
            error
                .to_string()
                .contains("pre-append commit-sequence admission"),
            "unexpected timeout error: {error}"
        );

        // Releasing the blocker after the caller has already received its
        // timeout must not let the detached blocking task append or commit.
        drop(sequence_guard);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let node = kv_paths
            .raft_store()
            .open_node(&local)
            .expect("inspect durable node after timeout");
        assert_eq!(node.last_log_index(), baseline_log_index);
        assert!(node.store().get(key).is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn queued_proposal_releases_config_projection_before_sequence_admission() {
        let root = sinomo_testdir::TestDir::new("sinomo-cluster-preprojection-sequence-convoy");
        let data_dir = sinomo_local::SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let local =
            NodeId::from_legacy_named("node_preprojection_sequence_convoy").expect("local node");
        let cluster_id = ClusterId::from_legacy_named("cluster_preprojection_sequence_convoy")
            .expect("cluster id");
        let now = OffsetDateTime::now_utc();
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "preprojection sequence convoy",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        state.term = 13;
        let daemon_capability =
            sinomo_base::CapabilityName::new(sinomo_node::DAEMON_MAIN_RUN_CAPABILITY)
                .expect("daemon capability");
        state.members[0].capabilities = vec![daemon_capability.clone()];
        state.members[0].allowed_capabilities = vec![daemon_capability];
        let config = ClusterKvRaftConfigRecord {
            cluster_id,
            term: state.term,
            kv_revision: 1,
            voters: vec![local.clone()],
            learners: Vec::new(),
            non_voting: Vec::new(),
            updated_at: now,
        };
        let kv_paths = paths.kv_paths();
        let mut node = kv_paths.raft_store().open_node(&local).expect("open node");
        node.become_leader(state.term).expect("become leader");
        let config_entry = node
            .append_local_command(
                state.term,
                KvCommand::Op(KvOp::Put {
                    key: sinomo_kv::cluster_kv_raft_config_key(),
                    value: encode_kv_value(&config).expect("encode config"),
                    lease_id: None,
                }),
            )
            .expect("append config");
        node.commit_to(config_entry.index).expect("commit config");
        state.kv_revision = node.store().revision().0;
        drop(node);
        crate::raft_election::locked_set_cluster_raft_leader(&paths, &local, state.term)
            .expect("publish resident local leadership");

        let sequence_guard =
            sinomo_kv::lock_cluster_kv_commit_sequence(&kv_paths, Duration::from_secs(1))
                .expect("hold proposal sequence lock");
        let key = "/convoy/queued-proposal";
        let queued_paths = paths.clone();
        let mut queued_state = state.clone();
        let queued = tokio::spawn(async move {
            let timeout = Duration::from_secs(2);
            let mut transport = NoopRaftTransport;
            commit_cluster_raft_command_compact_with_quorum_async_with_host(
                &tokio::runtime::Handle::current(),
                &queued_paths,
                &mut queued_state,
                KvCommandRequest::generated(KvCommand::Op(KvOp::Put {
                    key: key.to_string(),
                    value: b"committed".to_vec(),
                    lease_id: None,
                })),
                ClusterKvRaftQuorumCommitOptions { timeout },
                ClusterControlPlaneTimeouts::default_daemon_transport()
                    .raft_append_replication_options(timeout),
                &mut transport,
            )
            .await
        });

        let queue_deadline = Instant::now() + Duration::from_secs(1);
        while sinomo_kv::cluster_kv_commit_sequence_admission_depth_for_test(&kv_paths) == 0 {
            assert!(
                Instant::now() < queue_deadline,
                "proposal did not reach commit-sequence admission"
            );
            tokio::task::yield_now().await;
        }

        let read_paths = paths.clone();
        let read_local = local.clone();
        let reads = tokio::task::spawn_blocking(move || {
            let deadline = Instant::now() + Duration::from_millis(100);
            let mut point = LocalClusterRaftCommandStorage::new_read_index_with_operation_deadline(
                &read_paths,
                read_local.clone(),
                deadline,
            );
            let config_key = sinomo_kv::cluster_kv_raft_config_key();
            assert!(
                point.read_index_entry(&config_key)?.is_some(),
                "point ReadIndex lost committed config"
            );
            point.release_session_for_quorum();

            let mut exact = LocalClusterRaftCommandStorage::new_read_index_with_operation_deadline(
                &read_paths,
                read_local.clone(),
                deadline,
            );
            let entries =
                exact.read_index_entries(&[config_key, "/convoy/not-present".to_string()])?;
            assert!(entries[0].is_some());
            assert!(entries[1].is_none());
            exact.release_session_for_quorum();

            let mut admission =
                LocalClusterRaftCommandStorage::new_read_index_with_operation_deadline(
                    &read_paths,
                    read_local,
                    deadline,
                );
            admission.read_index_state()?;
            admission.release_session_for_quorum();
            Ok::<_, NetworkError>(())
        });
        tokio::time::timeout(Duration::from_millis(500), reads)
            .await
            .expect("queued proposal must not block ReadIndex admission")
            .expect("join ReadIndex probes")
            .expect("point/exact ReadIndex probes");

        drop(sequence_guard);
        queued
            .await
            .expect("join queued proposal")
            .expect("queued proposal commits after sequence release");
        let node = kv_paths
            .raft_store()
            .open_node(&local)
            .expect("inspect committed proposal");
        assert_eq!(
            node.store().get(key).map(|entry| entry.value.as_slice()),
            Some(b"committed".as_slice())
        );
    }
}

fn build_deduplicated_append_request_wave<T, E>(
    requests: &[(ClusterRaftAppendRequestBuildKey, u64)],
    mut build: impl FnMut(u64) -> Result<T, E>,
) -> Result<Vec<Arc<T>>, E> {
    let mut requests_by_key: BTreeMap<ClusterRaftAppendRequestBuildKey, Arc<T>> = BTreeMap::new();
    let mut built_requests = Vec::new();
    for (key, next_index) in requests.iter().copied() {
        match requests_by_key.entry(key) {
            std::collections::btree_map::Entry::Occupied(entry) => {
                built_requests.push(Arc::clone(entry.get()));
            }
            std::collections::btree_map::Entry::Vacant(entry) => {
                let request = Arc::new(build(next_index)?);
                entry.insert(Arc::clone(&request));
                built_requests.push(request);
            }
        }
    }
    Ok(built_requests)
}

fn remaining_raft_quorum_budget(deadline: Instant, phase: &str) -> Result<Duration, NetworkError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(NetworkError::ClusterReadOnly {
            detail: format!("cluster Raft quorum replication timed out during {phase}"),
        });
    }
    Ok(remaining)
}

fn raft_quorum_phase_timeout(phase: &str) -> NetworkError {
    NetworkError::ClusterReadOnly {
        detail: format!("cluster Raft quorum replication timed out during {phase}"),
    }
}

fn remaining_raft_commit_budget(deadline: Instant, phase: &str) -> Result<Duration, NetworkError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(raft_commit_phase_timeout(phase));
    }
    Ok(remaining)
}

fn raft_commit_phase_timeout(phase: &str) -> NetworkError {
    NetworkError::ClusterReadOnly {
        detail: format!("Raft quorum commit timed out during {phase}"),
    }
}

/// Runs one storage phase, optionally under the caller's original pre-append
/// authority deadline.
///
/// `spawn_blocking` work is not canceled when its async join handle is dropped.
/// Rechecking after the shared-session mutex is acquired is therefore
/// load-bearing: a task that sat in the blocking queue, or behind an earlier
/// storage phase, must not wake after the caller timed out and then append a
/// proposal. The KV command storage independently rechecks the same deadline
/// immediately before its first durable request-ledger/WAL mutation. Passing
/// `None` is reserved for exact post-append reconciliation that must finish
/// even after the client response budget expires.
async fn run_cluster_raft_storage_phase<T, F>(
    runtime: &tokio::runtime::Handle,
    storage: SharedLocalClusterRaftCommandStorage,
    preappend_deadline: Option<Instant>,
    operation: &'static str,
    run: F,
) -> Result<T, NetworkError>
where
    T: Send + 'static,
    F: FnOnce(&mut LocalClusterRaftCommandStorage) -> Result<T, NetworkError> + Send + 'static,
{
    if let Some(deadline) = preappend_deadline {
        remaining_raft_commit_budget(deadline, operation)?;
    }
    let deadline = preappend_deadline.map(|deadline| {
        (
            deadline,
            raft_commit_phase_timeout(operation),
            ClusterRaftBlockingDeadlinePolicy::DeadlineBounded,
        )
    });
    let cancellation_policy = if preappend_deadline.is_some() {
        ClusterRaftBlockingCancellationPolicy::CancelQueued
    } else {
        ClusterRaftBlockingCancellationPolicy::CompleteAlways
    };
    run_cluster_raft_supervised_blocking(
        runtime,
        deadline,
        cancellation_policy,
        "quorum_commit",
        operation,
        operation,
        move || {
            let mut storage = lock_local_cluster_raft_command_storage(&storage, operation)?;
            if let Some(deadline) = preappend_deadline {
                remaining_raft_commit_budget(deadline, operation)?;
            }
            run(&mut storage)
        },
    )
    .await
}

fn cluster_kv_replication_status_label(
    status: sinomo_kv::ClusterKvRaftTargetReplicationStatus,
) -> &'static str {
    match status {
        sinomo_kv::ClusterKvRaftTargetReplicationStatus::Accepted => "accepted",
        sinomo_kv::ClusterKvRaftTargetReplicationStatus::Rejected => "rejected",
        sinomo_kv::ClusterKvRaftTargetReplicationStatus::Failed => "failed",
    }
}

fn record_cluster_kv_replication_target_result(
    status: sinomo_kv::ClusterKvRaftTargetReplicationStatus,
    latency_ms: Option<u64>,
) {
    let outcome = cluster_kv_replication_status_label(status);
    metrics::counter!("sinomo.kv.replication.target_results", "outcome" => outcome).increment(1);
    if let Some(latency_ms) = latency_ms {
        metrics::histogram!("sinomo.kv.replication.target.us", "outcome" => outcome)
            .record(latency_ms.saturating_mul(1_000) as f64);
    }
}

fn record_cluster_kv_replication_send(request: &ClusterRaftAppendRequest) {
    let payload = if request.snapshot.is_some() {
        "snapshot"
    } else {
        "append"
    };
    metrics::counter!("sinomo.kv.replication.attempts", "payload" => payload).increment(1);
    metrics::histogram!("sinomo.kv.replication.entries", "payload" => payload)
        .record(request.append.entries.len() as f64);
}

fn record_cluster_kv_replication_catch_up(outcome: &'static str, attempts: usize) {
    metrics::histogram!("sinomo.kv.replication.catchup_attempts", "outcome" => outcome)
        .record(attempts as f64);
}

fn lock_local_cluster_raft_command_storage<'a>(
    storage: &'a Mutex<LocalClusterRaftCommandStorage>,
    operation: &str,
) -> Result<MutexGuard<'a, LocalClusterRaftCommandStorage>, NetworkError> {
    storage.lock().map_err(|error| NetworkError::Io {
        operation: operation.to_string(),
        detail: format!("local cluster Raft command session lock poisoned: {error}"),
    })
}

#[allow(clippy::too_many_arguments)]
fn cluster_kv_quorum_commit_phase_trace(
    phase: &'static str,
    request_id: &KvRequestId,
    local_node_id: &NodeId,
    duration: Duration,
    commit_elapsed: Duration,
    emission_delay: Duration,
    term: u64,
    log_index: u64,
    revision: u64,
    outcome: &'static str,
    detail: std::fmt::Arguments<'_>,
) {
    if !cluster_kv_quorum_commit_trace_enabled() {
        return;
    }
    let duration_us = u64::try_from(duration.as_micros()).unwrap_or(u64::MAX);
    let commit_elapsed_us = u64::try_from(commit_elapsed.as_micros()).unwrap_or(u64::MAX);
    let emission_delay_us = u64::try_from(emission_delay.as_micros()).unwrap_or(u64::MAX);
    tracing::trace!(
        target: "sinomo.cluster.formation",
        phase,
        request_id = %request_id,
        local_node_id = %local_node_id,
        duration_us,
        commit_elapsed_us,
        emission_delay_us,
        term,
        log_index,
        revision,
        outcome,
        detail = %detail,
        "cluster KV quorum commit phase"
    );
    if cluster_kv_quorum_commit_stderr_trace_enabled() {
        eprintln!(
            "[sinomo cluster join] KV quorum phase={phase} request_id={request_id} local_node={local_node_id} duration_us={duration_us} commit_elapsed_us={commit_elapsed_us} emission_delay_us={emission_delay_us} term={term} log_index={log_index} revision={revision} outcome={outcome}: {detail}"
        );
    }
}

static CLUSTER_JOIN_STDERR_TRACE_ENABLED: OnceLock<bool> = OnceLock::new();

fn cluster_kv_quorum_commit_stderr_trace_enabled() -> bool {
    *CLUSTER_JOIN_STDERR_TRACE_ENABLED.get_or_init(|| {
        std::env::var("SINOMO_CLUSTER_JOIN_TRACE")
            .ok()
            .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "on"))
    })
}

fn cluster_kv_quorum_commit_trace_enabled() -> bool {
    tracing::enabled!(target: "sinomo.cluster.formation", tracing::Level::TRACE)
        || cluster_kv_quorum_commit_stderr_trace_enabled()
}

const CLUSTER_KV_QUORUM_COMMIT_TRACE_RECORD_LIMIT: usize = 32;
const CLUSTER_KV_QUORUM_COMMIT_TRACE_DETAIL_LIMIT: usize = 512;

struct ClusterKvQuorumCommitTraceRecord {
    phase: &'static str,
    duration: Duration,
    observed_at: Instant,
    commit_elapsed: Duration,
    term: u64,
    log_index: u64,
    revision: u64,
    outcome: &'static str,
    detail: String,
}

struct ClusterKvQuorumCommitTraceBuffer {
    request_id: KvRequestId,
    local_node_id: NodeId,
    started: Instant,
    initial_term: u64,
    initial_revision: u64,
    records: Vec<ClusterKvQuorumCommitTraceRecord>,
    dropped_records: u64,
}

#[derive(Clone, Default)]
struct ClusterKvQuorumCommitTrace {
    inner: Option<Arc<Mutex<ClusterKvQuorumCommitTraceBuffer>>>,
}

impl ClusterKvQuorumCommitTrace {
    fn new(request_id: KvRequestId, local_node_id: NodeId, term: u64, revision: u64) -> Self {
        if !cluster_kv_quorum_commit_trace_enabled() {
            return Self::default();
        }
        Self {
            inner: Some(Arc::new(Mutex::new(ClusterKvQuorumCommitTraceBuffer {
                request_id,
                local_node_id,
                started: Instant::now(),
                initial_term: term,
                initial_revision: revision,
                records: Vec::with_capacity(CLUSTER_KV_QUORUM_COMMIT_TRACE_RECORD_LIMIT),
                dropped_records: 0,
            }))),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn record(
        &self,
        phase: &'static str,
        duration: Duration,
        term: u64,
        log_index: u64,
        revision: u64,
        outcome: &'static str,
        detail: std::fmt::Arguments<'_>,
    ) {
        let Some(inner) = &self.inner else {
            return;
        };
        let Ok(mut buffer) = inner.lock() else {
            return;
        };
        if buffer.records.len() >= CLUSTER_KV_QUORUM_COMMIT_TRACE_RECORD_LIMIT {
            buffer.dropped_records = buffer.dropped_records.saturating_add(1);
            return;
        }
        let observed_at = Instant::now();
        let commit_elapsed = observed_at.saturating_duration_since(buffer.started);
        let mut detail = detail.to_string();
        if detail.len() > CLUSTER_KV_QUORUM_COMMIT_TRACE_DETAIL_LIMIT {
            let mut truncate_at = CLUSTER_KV_QUORUM_COMMIT_TRACE_DETAIL_LIMIT.saturating_sub(3);
            while !detail.is_char_boundary(truncate_at) {
                truncate_at = truncate_at.saturating_sub(1);
            }
            detail.truncate(truncate_at);
            detail.push_str("...");
        }
        buffer.records.push(ClusterKvQuorumCommitTraceRecord {
            phase,
            duration,
            observed_at,
            commit_elapsed,
            term,
            log_index,
            revision,
            outcome,
            detail,
        });
    }

    fn flush(&self) {
        let Some(inner) = &self.inner else {
            return;
        };
        let Ok(mut buffer) = inner.lock() else {
            return;
        };
        let request_id = buffer.request_id.clone();
        let local_node_id = buffer.local_node_id.clone();
        let initial_term = buffer.initial_term;
        let initial_revision = buffer.initial_revision;
        let started = buffer.started;
        let records = std::mem::take(&mut buffer.records);
        let dropped_records = std::mem::take(&mut buffer.dropped_records);
        drop(buffer);
        for record in records {
            cluster_kv_quorum_commit_phase_trace(
                record.phase,
                &request_id,
                &local_node_id,
                record.duration,
                record.commit_elapsed,
                record.observed_at.elapsed(),
                record.term,
                record.log_index,
                record.revision,
                record.outcome,
                format_args!("{}", record.detail),
            );
        }
        if dropped_records > 0 {
            cluster_kv_quorum_commit_phase_trace(
                "trace_buffer",
                &request_id,
                &local_node_id,
                Duration::default(),
                started.elapsed(),
                Duration::default(),
                initial_term,
                0,
                initial_revision,
                "truncated",
                format_args!(
                    "dropped {dropped_records} quorum phase records after reaching the bounded buffer limit"
                ),
            );
        }
    }
}

struct ClusterKvQuorumCommitSequenceTrace {
    trace: ClusterKvQuorumCommitTrace,
    acquired_at: Option<Instant>,
    term: u64,
    log_index: u64,
    revision: u64,
    completion_outcome: &'static str,
}

impl ClusterKvQuorumCommitSequenceTrace {
    fn new(trace: ClusterKvQuorumCommitTrace, term: u64, revision: u64) -> Self {
        Self {
            trace,
            acquired_at: None,
            term,
            log_index: 0,
            revision,
            completion_outcome: "released",
        }
    }

    fn record_acquired(&mut self) {
        self.acquired_at = Some(Instant::now());
    }

    fn record_result(&mut self, result: &Result<ClusterKvRaftQuorumCommitResult, NetworkError>) {
        match result {
            Ok(commit) => {
                self.term = commit.authority.term;
                self.log_index = commit.authority.commit_index;
                self.revision = commit.authority.revision.0;
                self.completion_outcome = if commit.replayed_committed_request {
                    "replayed"
                } else {
                    "completed"
                };
            }
            Err(_) => self.completion_outcome = "error",
        }
    }
}

impl Drop for ClusterKvQuorumCommitSequenceTrace {
    fn drop(&mut self) {
        let Some(acquired_at) = self.acquired_at else {
            return;
        };
        let outcome = if std::thread::panicking() {
            "unwound"
        } else {
            self.completion_outcome
        };
        self.trace.record(
            "commit_sequence_lock",
            acquired_at.elapsed(),
            self.term,
            self.log_index,
            self.revision,
            outcome,
            format_args!(
                "released the synchronous cluster KV commit sequence lock after quorum commit outcome {outcome}"
            ),
        );
    }
}

pub trait ClusterKvQuorumAsyncHost: Send {
    fn last_local_kv_replication_index_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
    ) -> ClusterMembershipRpcFuture<'a, u64>;

    fn build_kv_replication_request_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        next_index: u64,
    ) -> ClusterMembershipRpcFuture<'a, ClusterRaftAppendRequest>;

    fn send_cluster_kv_replication_to_node_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        target_node_id: &'a NodeId,
        request: &'a ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterRaftAppendResponse>;

    fn send_cluster_kv_replications_to_quorum_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        attempts: ClusterRaftQuorumBatch,
        required_acks: usize,
    ) -> ClusterMembershipRpcFuture<'a, ClusterRaftQuorumBatchOutcomes> {
        Box::pin(async move {
            let deadline = std::time::Instant::now()
                + attempts
                    .iter()
                    .map(|(_, _, timeout)| *timeout)
                    .max()
                    .unwrap_or_default();
            let mut accepted = 0_usize;
            let mut outcomes = Vec::new();
            for (target, request, timeout) in attempts {
                if accepted >= required_acks {
                    break;
                }
                let remaining = deadline
                    .checked_duration_since(std::time::Instant::now())
                    .unwrap_or_default();
                if remaining.is_zero() {
                    break;
                }
                let result = self
                    .send_cluster_kv_replication_to_node_async(
                        paths,
                        state,
                        &target,
                        &request,
                        timeout.min(remaining),
                    )
                    .await;
                if result.as_ref().is_ok_and(|response| response.accepted) {
                    accepted = accepted.saturating_add(1);
                }
                outcomes.push((target, request, result));
            }
            Ok(outcomes)
        })
    }

    fn on_kv_replication_sent_async<'a>(&'a mut self) -> ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn record_node_command_connectivity(
        &mut self,
        _paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
        _target_node_id: &NodeId,
        _connectivity_state: ClusterConnectivityState,
        _latency_ms: Option<u64>,
    ) {
    }

    fn fan_out_committed_cluster_kv_command_async<'a>(
        &'a mut self,
        _paths: ClusterPaths,
        _state: ClusterBootstrapState,
        _events: Vec<KvEvent>,
        _targets: Vec<NodeId>,
    ) -> ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

pub trait ClusterRaftQuorumAsyncHost: Send {
    fn send_raft_append_to_node_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        target_node_id: &'a NodeId,
        request: &'a ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterRaftAppendResponse>;

    fn send_raft_appends_to_quorum_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        attempts: ClusterRaftQuorumBatch,
        required_acks: usize,
    ) -> ClusterMembershipRpcFuture<'a, ClusterRaftQuorumBatchOutcomes> {
        Box::pin(async move {
            let deadline = std::time::Instant::now()
                + attempts
                    .iter()
                    .map(|(_, _, timeout)| *timeout)
                    .max()
                    .unwrap_or_default();
            let mut accepted = 0_usize;
            let mut outcomes = Vec::new();
            for (target, request, timeout) in attempts {
                if accepted >= required_acks {
                    break;
                }
                let remaining = deadline
                    .checked_duration_since(std::time::Instant::now())
                    .unwrap_or_default();
                if remaining.is_zero() {
                    break;
                }
                let result = self
                    .send_raft_append_to_node_async(
                        paths,
                        state,
                        &target,
                        &request,
                        timeout.min(remaining),
                    )
                    .await;
                if result.as_ref().is_ok_and(|response| response.accepted) {
                    accepted = accepted.saturating_add(1);
                }
                outcomes.push((target, request, result));
            }
            Ok(outcomes)
        })
    }

    fn on_raft_append_sent_async<'a>(&'a mut self) -> ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn record_node_command_connectivity(
        &mut self,
        _paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
        _target_node_id: &NodeId,
        _connectivity_state: ClusterConnectivityState,
        _latency_ms: Option<u64>,
    ) {
    }

    fn fan_out_committed_cluster_raft_command_async<'a>(
        &'a mut self,
        _paths: ClusterPaths,
        _state: ClusterBootstrapState,
        _events: Vec<KvEvent>,
        _targets: Vec<NodeId>,
    ) -> ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

impl<T> ClusterRaftQuorumAsyncHost for T
where
    T: ClusterKvQuorumAsyncHost,
{
    fn send_raft_append_to_node_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        target_node_id: &'a NodeId,
        request: &'a ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterRaftAppendResponse> {
        self.send_cluster_kv_replication_to_node_async(
            paths,
            state,
            target_node_id,
            request,
            timeout,
        )
    }

    fn send_raft_appends_to_quorum_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        attempts: ClusterRaftQuorumBatch,
        required_acks: usize,
    ) -> ClusterMembershipRpcFuture<'a, ClusterRaftQuorumBatchOutcomes> {
        self.send_cluster_kv_replications_to_quorum_async(paths, state, attempts, required_acks)
    }

    fn on_raft_append_sent_async<'a>(&'a mut self) -> ClusterMembershipRpcFuture<'a, ()> {
        self.on_kv_replication_sent_async()
    }

    fn record_node_command_connectivity(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        connectivity_state: ClusterConnectivityState,
        latency_ms: Option<u64>,
    ) {
        ClusterKvQuorumAsyncHost::record_node_command_connectivity(
            self,
            paths,
            state,
            target_node_id,
            connectivity_state,
            latency_ms,
        );
    }

    fn fan_out_committed_cluster_raft_command_async<'a>(
        &'a mut self,
        paths: ClusterPaths,
        state: ClusterBootstrapState,
        events: Vec<KvEvent>,
        targets: Vec<NodeId>,
    ) -> ClusterMembershipRpcFuture<'a, ()> {
        self.fan_out_committed_cluster_kv_command_async(paths, state, events, targets)
    }
}

pub trait ClusterRaftQuorumTransportHost {
    fn local_has_verified_raft_leadership(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<bool, NetworkError> {
        paths
            .kv_paths()
            .locked_raft_local_is_leader(state)
            .map_err(kv_err)
    }

    fn replicate_cluster_raft_to_node(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError>;

    fn record_node_command_connectivity(
        &mut self,
        _paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
        _target_node_id: &NodeId,
        _connectivity_state: ClusterConnectivityState,
        _latency_ms: Option<u64>,
    ) {
    }

    fn fan_out_committed_cluster_raft_command(
        &mut self,
        _paths: ClusterPaths,
        _state: ClusterBootstrapState,
        _events: Vec<KvEvent>,
        _targets: Vec<NodeId>,
    ) {
    }
}

pub trait ClusterRaftQuorumCommitHost:
    sinomo_kv::ClusterKvRaftCommandStorage<Error = NetworkError> + ClusterRaftQuorumTransportHost
{
}

impl<T> ClusterRaftQuorumCommitHost for T where
    T: sinomo_kv::ClusterKvRaftCommandStorage<Error = NetworkError>
        + ClusterRaftQuorumTransportHost
{
}

#[cfg(any(test, feature = "test-support"))]
pub struct LocalClusterRaftQuorumCommitHost<'a, S, R, F, O> {
    storage: LocalClusterRaftCommandStorage,
    _paths: std::marker::PhantomData<&'a ClusterPaths>,
    control_plane_timeouts: ClusterControlPlaneTimeouts,
    send_raft_append: S,
    record_endpoint_command_connectivity: R,
    fan_out_committed_cluster_raft_command: F,
    on_raft_append_sent: O,
}

#[cfg(any(test, feature = "test-support"))]
struct LocalResidentRaftAppendReplicationHost<'a, S, O> {
    paths: &'a ClusterPaths,
    storage: &'a mut LocalClusterRaftCommandStorage,
    options: ClusterRaftAppendReplicationOptions,
    send_raft_append: &'a mut S,
    on_raft_append_sent: &'a mut O,
}

#[cfg(any(test, feature = "test-support"))]
impl<S, O> sinomo_kv::ClusterKvRaftAppendReplicationHost
    for LocalResidentRaftAppendReplicationHost<'_, S, O>
where
    S: FnMut(
        &ClusterEndpointRecord,
        &ClusterRaftAppendRequest,
        Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError>,
    O: FnMut(),
{
    type Error = NetworkError;
    type Target = ClusterEndpointRecord;

    fn kv_error(error: KvError) -> Self::Error {
        kv_err(error)
    }

    fn last_local_raft_log_index(
        &mut self,
        _state: &ClusterBootstrapState,
    ) -> Result<u64, Self::Error> {
        self.storage.last_log_index()
    }

    fn raft_peer_progress_next_index_hint(
        &mut self,
        state: &ClusterBootstrapState,
        target: &Self::Target,
        last_local_raft_log_index: u64,
    ) -> Option<u64> {
        sinomo_kv::cluster_kv_raft_peer_progress_next_index_hint(
            &self.paths.kv_paths(),
            &target.node_id,
            state.term,
            last_local_raft_log_index,
        )
    }

    fn build_raft_append_request(
        &mut self,
        state: &ClusterBootstrapState,
        next_index: u64,
    ) -> Result<ClusterRaftAppendRequest, Self::Error> {
        self.storage.build_raft_append_request(state, next_index)
    }

    fn append_attempt_timeout(
        &mut self,
        target: &Self::Target,
        remaining: Duration,
        request: &ClusterRaftAppendRequest,
    ) -> Duration {
        self.options
            .append_attempt_timeout(target, remaining, request)
    }

    fn on_raft_append_sent(&mut self) -> Result<(), Self::Error> {
        (self.on_raft_append_sent)();
        Ok(())
    }

    fn send_raft_append(
        &mut self,
        target: &Self::Target,
        request: &ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, Self::Error> {
        (self.send_raft_append)(target, request, timeout)
    }
}

#[cfg(any(test, feature = "test-support"))]
impl<S, R, F, O> sinomo_kv::ClusterKvRaftCommandStorage
    for LocalClusterRaftQuorumCommitHost<'_, S, R, F, O>
{
    type Error = NetworkError;

    fn kv_error(error: KvError) -> Self::Error {
        crate::kv_local_store::kv_err(error)
    }

    fn replay_cluster_raft_request_if_committed(
        &mut self,
        request: &KvCommandRequest,
    ) -> Result<Option<(KvSnapshot, sinomo_kv::KvCommittedRequestRecord)>, NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::replay_cluster_raft_request_if_committed(
            &mut self.storage,
            request,
        )
    }

    fn replay_cluster_raft_request_record_if_committed(
        &mut self,
        request: &KvCommandRequest,
    ) -> Result<Option<sinomo_kv::KvCommittedRequestRecord>, NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::replay_cluster_raft_request_record_if_committed(
            &mut self.storage,
            request,
        )
    }

    fn append_cluster_raft_request_uncommitted(
        &mut self,
        request: KvCommandRequest,
    ) -> Result<sinomo_kv::KvLogEntry, NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::append_cluster_raft_request_uncommitted(
            &mut self.storage,
            request,
        )
    }

    fn release_cluster_raft_command_session_for_quorum(&mut self) {
        self.storage.release_session_for_quorum();
    }

    fn record_cluster_raft_request_rejected_before_commit(
        &mut self,
        request: &KvCommandRequest,
        failure: &sinomo_kv::KvAuthorityFailure,
    ) -> Result<(), NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::record_cluster_raft_request_rejected_before_commit(
            &mut self.storage,
            request,
            failure,
        )
    }

    fn record_cluster_raft_request_unknown(
        &mut self,
        request: &KvCommandRequest,
        detail: &str,
    ) -> Result<(), NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::record_cluster_raft_request_unknown(
            &mut self.storage,
            request,
            detail,
        )
    }

    fn discard_cluster_raft_uncommitted_tail_and_clear_leader(
        &mut self,
    ) -> Result<(), NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::discard_cluster_raft_uncommitted_tail_and_clear_leader(
            &mut self.storage,
        )
    }

    fn step_down_preserving_cluster_raft_pending_entry(&mut self) -> Result<(), NetworkError> {
        self.storage.step_down_preserving_pending_entry()
    }

    fn publish_cluster_raft_higher_term(
        &mut self,
        state: &ClusterBootstrapState,
        term: u64,
    ) -> Result<(), NetworkError> {
        self.storage.publish_pending_higher_term(state, term)
    }

    fn observe_cluster_raft_term(
        &mut self,
        state: &ClusterBootstrapState,
        term: u64,
    ) -> Result<(), NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::observe_cluster_raft_term(
            &mut self.storage,
            state,
            term,
        )
    }

    fn commit_cluster_raft_index_with_events(
        &mut self,
        index: u64,
    ) -> Result<sinomo_kv::ClusterKvRaftCommitResult, NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::commit_cluster_raft_index_with_events(
            &mut self.storage,
            index,
        )
    }

    fn commit_cluster_raft_index_compact(
        &mut self,
        index: u64,
    ) -> Result<sinomo_kv::ClusterKvRaftCompactCommitResult, NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::commit_cluster_raft_index_compact(
            &mut self.storage,
            index,
        )
    }

    fn release_cluster_raft_command_session_after_commit(&mut self) {
        self.storage.release_session_after_commit();
    }

    fn publish_cluster_raft_post_commit_observations(
        &mut self,
        commit: Option<&sinomo_kv::ClusterKvRaftCompactCommitResult>,
    ) -> Result<(), NetworkError> {
        self.storage
            .take_post_commit_observations(commit)?
            .publish();
        Ok(())
    }
}

#[cfg(any(test, feature = "test-support"))]
impl<S, R, F, O> ClusterRaftQuorumTransportHost for LocalClusterRaftQuorumCommitHost<'_, S, R, F, O>
where
    S: FnMut(
        &ClusterEndpointRecord,
        &ClusterRaftAppendRequest,
        Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError>,
    R: FnMut(&ClusterPaths, &ClusterBootstrapState, &NodeId, ClusterConnectivityState, Option<u64>),
    F: FnMut(ClusterPaths, ClusterBootstrapState, Vec<KvEvent>, Vec<NodeId>),
    O: FnMut(),
{
    fn local_has_verified_raft_leadership(
        &mut self,
        _paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<bool, NetworkError> {
        self.storage.local_is_leader(state)
    }

    fn replicate_cluster_raft_to_node(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError> {
        let endpoint = representative_cluster_endpoint_for_node(state, target_node_id)?;
        let options = self
            .control_plane_timeouts
            .raft_append_replication_options(timeout);
        let mut host = LocalResidentRaftAppendReplicationHost {
            paths,
            storage: &mut self.storage,
            options,
            send_raft_append: &mut self.send_raft_append,
            on_raft_append_sent: &mut self.on_raft_append_sent,
        };
        sinomo_kv::replicate_cluster_kv_raft_to_target_with_host(
            state,
            &endpoint,
            &[],
            options.kv_options(),
            &mut host,
        )
        .map(|outcome| outcome.response)
    }

    fn record_node_command_connectivity(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        connectivity_state: ClusterConnectivityState,
        latency_ms: Option<u64>,
    ) {
        (self.record_endpoint_command_connectivity)(
            paths,
            state,
            target_node_id,
            connectivity_state,
            latency_ms,
        );
    }

    fn fan_out_committed_cluster_raft_command(
        &mut self,
        paths: ClusterPaths,
        state: ClusterBootstrapState,
        events: Vec<KvEvent>,
        targets: Vec<NodeId>,
    ) {
        (self.fan_out_committed_cluster_raft_command)(paths, state, events, targets);
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn commit_cluster_raft_command_with_local_storage_and_transport<S, R, F, O>(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    request: KvCommandRequest,
    options: ClusterKvRaftQuorumCommitOptions,
    control_plane_timeouts: ClusterControlPlaneTimeouts,
    send_raft_append: S,
    record_endpoint_command_connectivity: R,
    fan_out_committed_cluster_raft_command: F,
    on_raft_append_sent: O,
) -> Result<ClusterKvRaftQuorumCommitResult, NetworkError>
where
    S: FnMut(
        &ClusterEndpointRecord,
        &ClusterRaftAppendRequest,
        Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError>,
    R: FnMut(&ClusterPaths, &ClusterBootstrapState, &NodeId, ClusterConnectivityState, Option<u64>),
    F: FnMut(ClusterPaths, ClusterBootstrapState, Vec<KvEvent>, Vec<NodeId>),
    O: FnMut(),
{
    let mut host = LocalClusterRaftQuorumCommitHost {
        storage: LocalClusterRaftCommandStorage::new(paths, state.local_node_id.clone()),
        _paths: std::marker::PhantomData,
        control_plane_timeouts,
        send_raft_append,
        record_endpoint_command_connectivity,
        fan_out_committed_cluster_raft_command,
        on_raft_append_sent,
    };
    commit_cluster_raft_command_with_quorum_driver(paths, state, request, options, &mut host)
}

struct DriverNodeRaftAppendReplicationHost<'a, T> {
    paths: &'a ClusterPaths,
    storage: Option<&'a mut LocalClusterRaftCommandStorage>,
    options: ClusterRaftAppendReplicationOptions,
    transport: &'a mut T,
}

impl<T> sinomo_kv::ClusterKvRaftAppendReplicationHost for DriverNodeRaftAppendReplicationHost<'_, T>
where
    T: ClusterControlPlaneTransport,
{
    type Error = NetworkError;
    type Target = NodeId;

    fn kv_error(error: KvError) -> Self::Error {
        crate::kv_local_store::kv_err(error)
    }

    fn last_local_raft_log_index(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<u64, Self::Error> {
        if let Some(storage) = self.storage.as_deref_mut() {
            return storage.last_log_index();
        }
        self.paths
            .kv_paths()
            .locked_last_raft_log_index(&state.local_node_id)
            .map_err(kv_err)
    }

    fn raft_peer_progress_next_index_hint(
        &mut self,
        state: &ClusterBootstrapState,
        target: &Self::Target,
        last_local_raft_log_index: u64,
    ) -> Option<u64> {
        sinomo_kv::cluster_kv_raft_peer_progress_next_index_hint(
            &self.paths.kv_paths(),
            target,
            state.term,
            last_local_raft_log_index,
        )
    }

    fn build_raft_append_request(
        &mut self,
        state: &ClusterBootstrapState,
        next_index: u64,
    ) -> Result<ClusterRaftAppendRequest, Self::Error> {
        if let Some(storage) = self.storage.as_deref_mut() {
            return storage.build_raft_append_request(state, next_index);
        }
        self.paths
            .kv_paths()
            .locked_build_raft_append_request_from_next_index(state, next_index)
            .map_err(kv_err)
    }

    fn append_attempt_timeout(
        &mut self,
        _target: &Self::Target,
        remaining: Duration,
        _request: &ClusterRaftAppendRequest,
    ) -> Duration {
        self.options.routed_node_append_attempt_timeout(remaining)
    }

    fn on_raft_append_sent(&mut self) -> Result<(), Self::Error> {
        self.transport.on_kv_replication_sent();
        Ok(())
    }

    fn send_raft_append(
        &mut self,
        target: &Self::Target,
        request: &ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, Self::Error> {
        self.transport
            .send_kv_replication_to_node(&request.state, target, request, timeout)
    }
}

struct DriverRaftQuorumCommitHost<'a, T> {
    storage: LocalClusterRaftCommandStorage,
    trace: ClusterKvQuorumCommitTrace,
    initial_term: u64,
    initial_revision: u64,
    driver: &'a mut ClusterControlPlaneDriver<T>,
}

impl<T> sinomo_kv::ClusterKvRaftCommandStorage for DriverRaftQuorumCommitHost<'_, T> {
    type Error = NetworkError;

    fn kv_error(error: KvError) -> Self::Error {
        crate::kv_local_store::kv_err(error)
    }

    fn replay_cluster_raft_request_if_committed(
        &mut self,
        request: &KvCommandRequest,
    ) -> Result<Option<(KvSnapshot, sinomo_kv::KvCommittedRequestRecord)>, NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::replay_cluster_raft_request_if_committed(
            &mut self.storage,
            request,
        )
    }

    fn replay_cluster_raft_request_record_if_committed(
        &mut self,
        request: &KvCommandRequest,
    ) -> Result<Option<sinomo_kv::KvCommittedRequestRecord>, NetworkError> {
        self.storage.replay_record_if_committed(request)
    }

    fn append_cluster_raft_request_uncommitted(
        &mut self,
        request: KvCommandRequest,
    ) -> Result<sinomo_kv::KvLogEntry, NetworkError> {
        let observed = self.storage.append_uncommitted_observed(request);
        if observed.log_append_wal_duration.is_some() {
            self.trace.record(
                "compare",
                observed.compare_duration,
                self.initial_term,
                0,
                self.initial_revision,
                "passed",
                format_args!("request identity and transaction branch preflight passed"),
            );
        } else if let Err(error) = &observed.result {
            self.trace.record(
                "compare",
                observed.compare_duration,
                self.initial_term,
                0,
                self.initial_revision,
                "error",
                format_args!("{error}"),
            );
        }
        if let Some(duration) = observed.log_append_wal_duration {
            match &observed.result {
                Ok(entry) => self.trace.record(
                    "log_append_wal",
                    duration,
                    entry.term,
                    entry.index,
                    entry.revision.0,
                    "appended",
                    format_args!("request appended to the local durable Raft log"),
                ),
                Err(error) => self.trace.record(
                    "log_append_wal",
                    duration,
                    self.initial_term,
                    0,
                    self.initial_revision,
                    "error",
                    format_args!("{error}"),
                ),
            }
        }
        observed.result.map_err(kv_err)
    }

    fn release_cluster_raft_command_session_for_quorum(&mut self) {
        self.storage.release_session_for_quorum();
    }

    fn record_cluster_raft_request_rejected_before_commit(
        &mut self,
        request: &KvCommandRequest,
        failure: &sinomo_kv::KvAuthorityFailure,
    ) -> Result<(), NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::record_cluster_raft_request_rejected_before_commit(
            &mut self.storage,
            request,
            failure,
        )
    }

    fn record_cluster_raft_request_unknown(
        &mut self,
        request: &KvCommandRequest,
        detail: &str,
    ) -> Result<(), NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::record_cluster_raft_request_unknown(
            &mut self.storage,
            request,
            detail,
        )
    }

    fn discard_cluster_raft_uncommitted_tail_and_clear_leader(
        &mut self,
    ) -> Result<(), NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::discard_cluster_raft_uncommitted_tail_and_clear_leader(
            &mut self.storage,
        )
    }

    fn step_down_preserving_cluster_raft_pending_entry(&mut self) -> Result<(), NetworkError> {
        self.storage.step_down_preserving_pending_entry()
    }

    fn publish_cluster_raft_higher_term(
        &mut self,
        state: &ClusterBootstrapState,
        term: u64,
    ) -> Result<(), NetworkError> {
        self.storage.publish_pending_higher_term(state, term)
    }

    fn observe_cluster_raft_term(
        &mut self,
        state: &ClusterBootstrapState,
        term: u64,
    ) -> Result<(), NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::observe_cluster_raft_term(
            &mut self.storage,
            state,
            term,
        )
    }

    fn commit_cluster_raft_index_with_events(
        &mut self,
        index: u64,
    ) -> Result<sinomo_kv::ClusterKvRaftCommitResult, NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::commit_cluster_raft_index_with_events(
            &mut self.storage,
            index,
        )
    }

    fn commit_cluster_raft_index_compact(
        &mut self,
        index: u64,
    ) -> Result<sinomo_kv::ClusterKvRaftCompactCommitResult, NetworkError> {
        self.storage.commit_index_compact(index)
    }

    fn release_cluster_raft_command_session_after_commit(&mut self) {
        self.storage.release_session_after_commit();
    }

    fn publish_cluster_raft_post_commit_observations(
        &mut self,
        commit: Option<&sinomo_kv::ClusterKvRaftCompactCommitResult>,
    ) -> Result<(), NetworkError> {
        self.storage
            .take_post_commit_observations(commit)?
            .publish();
        Ok(())
    }
}

impl<T> ClusterRaftQuorumTransportHost for DriverRaftQuorumCommitHost<'_, T>
where
    T: ClusterControlPlaneTransport,
{
    fn local_has_verified_raft_leadership(
        &mut self,
        _paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<bool, NetworkError> {
        self.storage.local_is_leader(state)
    }

    fn replicate_cluster_raft_to_node(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError> {
        let options = self
            .driver
            .timeouts
            .raft_append_replication_options(timeout);
        let mut host = DriverNodeRaftAppendReplicationHost {
            paths,
            storage: Some(&mut self.storage),
            options,
            transport: &mut self.driver.transport,
        };
        sinomo_kv::replicate_cluster_kv_raft_to_target_with_host(
            state,
            target_node_id,
            &[],
            options.kv_options(),
            &mut host,
        )
        .map(|outcome| outcome.response)
    }

    fn record_node_command_connectivity(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        connectivity_state: ClusterConnectivityState,
        latency_ms: Option<u64>,
    ) {
        let Ok(endpoint) = representative_cluster_endpoint_for_node(state, target_node_id) else {
            return;
        };
        self.driver.transport.record_endpoint_command_connectivity(
            paths,
            state,
            &endpoint,
            connectivity_state,
            latency_ms,
        );
    }

    fn fan_out_committed_cluster_raft_command(
        &mut self,
        paths: ClusterPaths,
        state: ClusterBootstrapState,
        events: Vec<KvEvent>,
        targets: Vec<NodeId>,
    ) {
        let endpoints = targets
            .iter()
            .filter_map(|node_id| representative_cluster_endpoint_for_node(&state, node_id).ok())
            .collect::<Vec<_>>();
        self.driver
            .transport
            .fan_out_committed_cluster_kv_command(paths, state, events, endpoints);
    }
}

pub(crate) fn commit_cluster_raft_command_with_control_plane_driver<T>(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    request: KvCommandRequest,
    options: ClusterKvRaftQuorumCommitOptions,
    driver: &mut ClusterControlPlaneDriver<T>,
) -> Result<ClusterKvRaftQuorumCommitResult, NetworkError>
where
    T: ClusterControlPlaneTransport,
{
    let request_id = request.request_id.clone();
    let local_node_id = state.local_node_id.clone();
    let initial_term = state.term;
    let initial_revision = state.kv_revision;
    let trace = ClusterKvQuorumCommitTrace::new(
        request_id,
        local_node_id.clone(),
        initial_term,
        initial_revision,
    );
    let mut host = DriverRaftQuorumCommitHost {
        storage: LocalClusterRaftCommandStorage::new(paths, local_node_id.clone()),
        trace: trace.clone(),
        initial_term,
        initial_revision,
        driver,
    };
    commit_cluster_raft_command_with_quorum_driver_and_trace(
        paths, state, request, options, trace, &mut host,
    )
}

pub(crate) fn catch_up_cluster_raft_member_with_control_plane_driver<T>(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    target_node_id: &NodeId,
    driver: &mut ClusterControlPlaneDriver<T>,
) -> Result<ClusterRaftAppendResponse, NetworkError>
where
    T: ClusterControlPlaneTransport,
{
    let options = driver
        .timeouts
        .raft_append_replication_options(driver.timeouts.raft_append_timeout);
    let mut host = DriverNodeRaftAppendReplicationHost {
        paths,
        storage: None,
        options,
        transport: &mut driver.transport,
    };
    sinomo_kv::replicate_cluster_kv_raft_to_target_with_host(
        state,
        target_node_id,
        &[],
        options.kv_options(),
        &mut host,
    )
    .map(|outcome| outcome.response)
}

struct NetworkClusterKvRaftQuorumCommitHost<'a, H> {
    paths: &'a ClusterPaths,
    commit_sequence_trace: ClusterKvQuorumCommitSequenceTrace,
    host: &'a mut H,
}

fn remote_voter_node_targets(
    _paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Vec<NodeId>, NetworkError> {
    if sinomo_kv::cluster_kv_raft_local_is_voter(state)
        && sinomo_kv::cluster_kv_raft_quorum_size_for_state(state) <= 1
    {
        return Ok(Vec::new());
    }
    Ok(sinomo_kv::cluster_kv_raft_voter_ids(state)
        .map_err(kv_err)?
        .into_iter()
        .filter(|node_id| node_id != &state.local_node_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect())
}

fn representative_cluster_endpoint_for_node(
    state: &ClusterBootstrapState,
    target_node_id: &NodeId,
) -> Result<ClusterEndpointRecord, NetworkError> {
    state
        .members
        .iter()
        .find(|member| member.node_id == *target_node_id)
        .and_then(|member| member.endpoints.first())
        .cloned()
        .ok_or_else(|| NetworkError::Io {
            operation: "cluster raft node endpoint representative".to_string(),
            detail: format!("node {target_node_id} has no known endpoint representative"),
        })
}

impl<H> sinomo_kv::ClusterKvRaftCommandStorage for NetworkClusterKvRaftQuorumCommitHost<'_, H>
where
    H: ClusterRaftQuorumCommitHost,
{
    type Error = NetworkError;

    fn kv_error(error: KvError) -> Self::Error {
        crate::kv_local_store::kv_err(error)
    }

    fn lock_cluster_raft_commit_sequence(&mut self) -> Result<Option<std::fs::File>, NetworkError> {
        let started = Instant::now();
        let result = sinomo_kv::lock_cluster_kv_commit_sequence(
            &self.paths.kv_paths(),
            sinomo_kv::CLUSTER_KV_COMMIT_SEQUENCE_LOCK_WAIT,
        );
        match &result {
            Ok(_) => {
                self.commit_sequence_trace.record_acquired();
                self.commit_sequence_trace.trace.record(
                    "leader_queue",
                    started.elapsed(),
                    self.commit_sequence_trace.term,
                    0,
                    self.commit_sequence_trace.revision,
                    "acquired",
                    format_args!("acquired the synchronous cluster KV commit sequence lock"),
                );
            }
            Err(error) => self.commit_sequence_trace.trace.record(
                "leader_queue",
                started.elapsed(),
                self.commit_sequence_trace.term,
                0,
                self.commit_sequence_trace.revision,
                "error",
                format_args!("{error}"),
            ),
        }
        result.map(Some).map_err(kv_err)
    }

    fn replay_cluster_raft_request_if_committed(
        &mut self,
        request: &KvCommandRequest,
    ) -> Result<Option<(KvSnapshot, sinomo_kv::KvCommittedRequestRecord)>, NetworkError> {
        self.host.replay_cluster_raft_request_if_committed(request)
    }

    fn replay_cluster_raft_request_record_if_committed(
        &mut self,
        request: &KvCommandRequest,
    ) -> Result<Option<sinomo_kv::KvCommittedRequestRecord>, NetworkError> {
        self.host
            .replay_cluster_raft_request_record_if_committed(request)
    }

    fn append_cluster_raft_request_uncommitted(
        &mut self,
        request: KvCommandRequest,
    ) -> Result<sinomo_kv::KvLogEntry, NetworkError> {
        self.host.append_cluster_raft_request_uncommitted(request)
    }

    fn release_cluster_raft_command_session_for_quorum(&mut self) {
        self.host.release_cluster_raft_command_session_for_quorum();
    }

    fn record_cluster_raft_request_rejected_before_commit(
        &mut self,
        request: &KvCommandRequest,
        failure: &sinomo_kv::KvAuthorityFailure,
    ) -> Result<(), NetworkError> {
        self.host
            .record_cluster_raft_request_rejected_before_commit(request, failure)
    }

    fn record_cluster_raft_request_unknown(
        &mut self,
        request: &KvCommandRequest,
        detail: &str,
    ) -> Result<(), NetworkError> {
        self.host
            .record_cluster_raft_request_unknown(request, detail)
    }

    fn discard_cluster_raft_uncommitted_tail_and_clear_leader(
        &mut self,
    ) -> Result<(), NetworkError> {
        self.host
            .discard_cluster_raft_uncommitted_tail_and_clear_leader()
    }

    fn step_down_preserving_cluster_raft_pending_entry(&mut self) -> Result<(), NetworkError> {
        self.host.step_down_preserving_cluster_raft_pending_entry()
    }

    fn publish_cluster_raft_higher_term(
        &mut self,
        state: &ClusterBootstrapState,
        term: u64,
    ) -> Result<(), NetworkError> {
        self.host.publish_cluster_raft_higher_term(state, term)
    }

    fn observe_cluster_raft_term(
        &mut self,
        state: &ClusterBootstrapState,
        term: u64,
    ) -> Result<(), NetworkError> {
        self.host.observe_cluster_raft_term(state, term)
    }

    fn commit_cluster_raft_index_with_events(
        &mut self,
        index: u64,
    ) -> Result<sinomo_kv::ClusterKvRaftCommitResult, NetworkError> {
        let started = Instant::now();
        let result = self.host.commit_cluster_raft_index_with_events(index);
        match &result {
            Ok(commit) => self.commit_sequence_trace.trace.record(
                "apply",
                started.elapsed(),
                commit.committed_request.term,
                index,
                commit.committed_request.revision.0,
                "applied",
                format_args!(
                    "committed the local Raft entry and published its materialized result"
                ),
            ),
            Err(error) => self.commit_sequence_trace.trace.record(
                "apply",
                started.elapsed(),
                self.commit_sequence_trace.term,
                index,
                self.commit_sequence_trace.revision,
                "error",
                format_args!("{error}"),
            ),
        }
        result
    }

    fn commit_cluster_raft_index_compact(
        &mut self,
        index: u64,
    ) -> Result<sinomo_kv::ClusterKvRaftCompactCommitResult, NetworkError> {
        let started = Instant::now();
        let result = self.host.commit_cluster_raft_index_compact(index);
        match &result {
            Ok(commit) => self.commit_sequence_trace.trace.record(
                "apply",
                started.elapsed(),
                commit.committed_request.term,
                index,
                commit.committed_request.revision.0,
                "applied",
                format_args!("committed the local Raft entry and published its compact delta"),
            ),
            Err(error) => self.commit_sequence_trace.trace.record(
                "apply",
                started.elapsed(),
                self.commit_sequence_trace.term,
                index,
                self.commit_sequence_trace.revision,
                "error",
                format_args!("{error}"),
            ),
        }
        result
    }

    fn release_cluster_raft_command_session_after_commit(&mut self) {
        self.host
            .release_cluster_raft_command_session_after_commit();
    }

    fn publish_cluster_raft_post_commit_observations(
        &mut self,
        commit: Option<&sinomo_kv::ClusterKvRaftCompactCommitResult>,
    ) -> Result<(), NetworkError> {
        self.host
            .publish_cluster_raft_post_commit_observations(commit)
    }
}

impl<H> sinomo_kv::ClusterKvRaftQuorumCommitHost for NetworkClusterKvRaftQuorumCommitHost<'_, H>
where
    H: ClusterRaftQuorumCommitHost,
{
    type Target = NodeId;

    fn local_has_verified_raft_leadership(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<bool, NetworkError> {
        self.host
            .local_has_verified_raft_leadership(self.paths, state)
    }

    fn remote_voter_targets(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<Vec<Self::Target>, NetworkError> {
        remote_voter_node_targets(self.paths, state)
    }

    fn target_node_id<'a>(&self, target: &'a Self::Target) -> &'a NodeId {
        target
    }

    fn target_description(&self, target: &Self::Target) -> String {
        target.to_string()
    }

    fn replicate_cluster_raft_to_target(
        &mut self,
        state: &ClusterBootstrapState,
        target: &Self::Target,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError> {
        let started = Instant::now();
        let result = self
            .host
            .replicate_cluster_raft_to_node(self.paths, state, target, timeout);
        match &result {
            Ok(response) => self.commit_sequence_trace.trace.record(
                "replication",
                started.elapsed(),
                response.append.term,
                response.append.match_index,
                response.state.kv_revision,
                if response.accepted {
                    "accepted"
                } else {
                    "rejected"
                },
                format_args!("synchronous Raft append to voter {target}"),
            ),
            Err(error) => self.commit_sequence_trace.trace.record(
                "replication",
                started.elapsed(),
                state.term,
                0,
                state.kv_revision,
                "error",
                format_args!("synchronous Raft append to voter {target} failed: {error}"),
            ),
        }
        result
    }

    fn record_target_replication_status(
        &mut self,
        state: &ClusterBootstrapState,
        target: &Self::Target,
        status: sinomo_kv::ClusterKvRaftTargetReplicationStatus,
        latency_ms: Option<u64>,
    ) -> Result<(), NetworkError> {
        record_cluster_kv_replication_target_result(status, latency_ms);
        let connectivity_state = match status {
            sinomo_kv::ClusterKvRaftTargetReplicationStatus::Accepted
            | sinomo_kv::ClusterKvRaftTargetReplicationStatus::Rejected => {
                ClusterConnectivityState::Healthy
            }
            sinomo_kv::ClusterKvRaftTargetReplicationStatus::Failed => {
                ClusterConnectivityState::Degraded
            }
        };
        self.host.record_node_command_connectivity(
            self.paths,
            state,
            target,
            connectivity_state,
            latency_ms,
        );
        Ok(())
    }

    fn observe_accepted_append_state(
        &mut self,
        state: &mut ClusterBootstrapState,
        _target: &Self::Target,
        peer_state: ClusterBootstrapState,
    ) -> Result<(), NetworkError> {
        merge_cluster_state_from_peer(state, peer_state);
        Ok(())
    }

    fn fan_out_committed_cluster_raft_command(
        &mut self,
        state: ClusterBootstrapState,
        events: Vec<KvEvent>,
        targets: Vec<Self::Target>,
    ) -> Result<(), NetworkError> {
        let started = Instant::now();
        let term = state.term;
        let revision = state.kv_revision;
        let event_count = events.len();
        let target_count = targets.len();
        self.host.fan_out_committed_cluster_raft_command(
            self.paths.clone(),
            state,
            events,
            targets,
        );
        self.commit_sequence_trace.trace.record(
            "response",
            started.elapsed(),
            term,
            0,
            revision,
            "completed",
            format_args!("fanned out {event_count} committed events to {target_count} targets"),
        );
        Ok(())
    }

    fn cluster_authority_error(&mut self, failure: sinomo_kv::KvAuthorityFailure) -> NetworkError {
        NetworkError::ClusterReadOnly {
            detail: serde_json::to_string(&failure).unwrap_or_else(|_| format!("{failure:?}")),
        }
    }
}

#[cfg_attr(not(any(test, feature = "test-support")), allow(dead_code))]
pub fn commit_cluster_raft_command_with_quorum_driver(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    request: KvCommandRequest,
    options: ClusterKvRaftQuorumCommitOptions,
    host: &mut impl ClusterRaftQuorumCommitHost,
) -> Result<ClusterKvRaftQuorumCommitResult, NetworkError> {
    let trace = ClusterKvQuorumCommitTrace::new(
        request.request_id.clone(),
        state.local_node_id.clone(),
        state.term,
        state.kv_revision,
    );
    commit_cluster_raft_command_with_quorum_driver_and_trace(
        paths, state, request, options, trace, host,
    )
}

fn commit_cluster_raft_command_with_quorum_driver_and_trace(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    request: KvCommandRequest,
    options: ClusterKvRaftQuorumCommitOptions,
    trace: ClusterKvQuorumCommitTrace,
    host: &mut impl ClusterRaftQuorumCommitHost,
) -> Result<ClusterKvRaftQuorumCommitResult, NetworkError> {
    let started = Instant::now();
    let deadline = started.checked_add(options.timeout).unwrap_or(started);
    let mut consensus_state = state.clone();
    project_committed_cluster_raft_authority_before_sync_commit(
        paths,
        &mut consensus_state,
        deadline,
    )?;
    // This is a durable local observation, not a tentative command result.
    // Retain it even if the following quorum attempt fails so a retry cannot
    // re-enter leadership validation with the same stale bootstrap term.
    *state = consensus_state.clone();
    let initial_term = consensus_state.term;
    let initial_revision = consensus_state.kv_revision;
    let mut kv_host = NetworkClusterKvRaftQuorumCommitHost {
        paths,
        commit_sequence_trace: ClusterKvQuorumCommitSequenceTrace::new(
            trace.clone(),
            initial_term,
            initial_revision,
        ),
        host,
    };
    let mut bounded_options = options;
    bounded_options.timeout =
        remaining_raft_commit_budget(deadline, "synchronous quorum driver admission")?;
    let result = sinomo_kv::commit_cluster_kv_raft_command_with_quorum(
        &mut consensus_state,
        request,
        bounded_options,
        &mut kv_host,
    );
    kv_host.commit_sequence_trace.record_result(&result);
    drop(kv_host);
    trace.flush();
    let result = result?;
    state.term = state.term.max(consensus_state.term);
    state.kv_revision = state.kv_revision.max(consensus_state.kv_revision);
    Ok(result)
}

/// Projects the durable hard-state/config floor before a synchronous commit.
///
/// The bootstrap topology file does not advance atomically with Raft hard
/// state. Reading only the committed config can therefore leave `state.term`
/// at zero while the resident node is a verified term-N leader. Capture term,
/// applied revision, and config from the same resident view, matching the
/// asynchronous commit path and preventing a healthy leader from rejecting
/// its own proposal before any network work starts.
fn project_committed_cluster_raft_authority_before_sync_commit(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    deadline: Instant,
) -> Result<(), NetworkError> {
    remaining_raft_commit_budget(deadline, "synchronous authority projection")?;
    let mut projected = state.clone();
    let mut storage = LocalClusterRaftCommandStorage::new_with_operation_deadline(
        paths,
        projected.local_node_id.clone(),
        deadline,
    );
    let read_state = storage.read_index_state()?;
    projected.term = projected.term.max(read_state.current_term);
    projected.kv_revision = projected.kv_revision.max(read_state.revision.0);
    if let Some(entry) = storage.read_index_entry(&sinomo_kv::cluster_kv_raft_config_key())? {
        let config: sinomo_kv::ClusterKvRaftConfigRecord = entry.decode().map_err(kv_err)?;
        project_cluster_raft_config_record(&mut projected, &config)?;
    }
    remaining_raft_commit_budget(deadline, "synchronous authority projection")?;
    *state = projected;
    Ok(())
}

struct AsyncNodeRaftAppendReplicationHost<'a, H> {
    runtime: &'a tokio::runtime::Handle,
    paths: &'a ClusterPaths,
    storage: SharedLocalClusterRaftCommandStorage,
    options: ClusterRaftAppendReplicationOptions,
    host: &'a mut H,
}

impl<H> sinomo_kv::ClusterKvRaftAppendReplicationAsyncHost
    for AsyncNodeRaftAppendReplicationHost<'_, H>
where
    H: ClusterRaftQuorumAsyncHost,
{
    type Error = NetworkError;
    type Target = NodeId;

    fn kv_error(error: KvError) -> Self::Error {
        crate::kv_local_store::kv_err(error)
    }

    fn last_local_raft_log_index_async<'a>(
        &'a mut self,
        _state: &'a ClusterBootstrapState,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, u64, Self::Error> {
        let runtime = self.runtime;
        let storage = Arc::clone(&self.storage);
        Box::pin(async move {
            run_cluster_raft_storage_phase(
                runtime,
                storage,
                None,
                "read resident local Raft log index",
                |storage| storage.last_log_index(),
            )
            .await
        })
    }

    fn raft_peer_progress_next_index_hint_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        target: &'a Self::Target,
        last_local_raft_log_index: u64,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, Option<u64>, Self::Error> {
        let paths = self.paths.kv_paths();
        let peer_node_id = target.clone();
        let current_term = state.term;
        Box::pin(async move {
            Ok(
                sinomo_kv::cluster_kv_raft_peer_progress_next_index_hint_async(
                    paths,
                    peer_node_id,
                    current_term,
                    last_local_raft_log_index,
                )
                .await,
            )
        })
    }

    fn build_raft_append_request_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        next_index: u64,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, ClusterRaftAppendRequest, Self::Error> {
        let runtime = self.runtime;
        let storage = Arc::clone(&self.storage);
        let state = state.clone();
        Box::pin(async move {
            run_cluster_raft_storage_phase(
                runtime,
                storage,
                None,
                "build resident local Raft append request",
                move |storage| storage.build_raft_append_request(&state, next_index),
            )
            .await
        })
    }

    fn append_attempt_timeout(
        &mut self,
        _target: &Self::Target,
        remaining: Duration,
        _request: &ClusterRaftAppendRequest,
    ) -> Duration {
        self.options.routed_node_append_attempt_timeout(remaining)
    }

    fn on_raft_append_sent_async<'a>(
        &'a mut self,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), Self::Error> {
        self.host.on_raft_append_sent_async()
    }

    fn send_raft_append_async<'a>(
        &'a mut self,
        target: &'a Self::Target,
        request: &'a ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, ClusterRaftAppendResponse, Self::Error> {
        self.host.send_raft_append_to_node_async(
            self.paths,
            &request.state,
            target,
            request,
            timeout,
        )
    }
}

struct NetworkClusterKvRaftQuorumCommitAsyncHost<'a, H> {
    runtime: &'a tokio::runtime::Handle,
    paths: &'a ClusterPaths,
    storage: SharedLocalClusterRaftCommandStorage,
    operation_deadline: Instant,
    proposal_may_be_durable: bool,
    append_options: ClusterRaftAppendReplicationOptions,
    trace: ClusterKvQuorumCommitTrace,
    initial_term: u64,
    initial_revision: u64,
    host: &'a mut H,
}

impl<H> sinomo_kv::ClusterKvRaftQuorumCommitAsyncHost
    for NetworkClusterKvRaftQuorumCommitAsyncHost<'_, H>
where
    H: ClusterRaftQuorumAsyncHost,
{
    type Error = NetworkError;

    fn kv_error(error: KvError) -> Self::Error {
        crate::kv_local_store::kv_err(error)
    }
    type Target = NodeId;

    fn local_has_verified_raft_leadership_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, bool, NetworkError> {
        let runtime = self.runtime;
        let storage = Arc::clone(&self.storage);
        let deadline = self.operation_deadline;
        let state = state.clone();
        Box::pin(async move {
            run_cluster_raft_storage_phase(
                runtime,
                storage,
                Some(deadline),
                "verify local Raft leadership",
                move |storage| storage.local_is_leader(&state),
            )
            .await
        })
    }

    fn lock_cluster_raft_commit_sequence_async<'a>(
        &'a mut self,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, Option<std::fs::File>, NetworkError> {
        let runtime = self.runtime;
        let kv_paths = self.paths.kv_paths();
        let trace = self.trace.clone();
        let term = self.initial_term;
        let revision = self.initial_revision;
        let operation_deadline = self.operation_deadline;
        let proposal_may_be_durable = self.proposal_may_be_durable;
        Box::pin(async move {
            let started = Instant::now();
            let wait = if proposal_may_be_durable {
                // Once an append may be durable, ordered reconciliation/apply
                // is safety work. Do not cancel it merely because the client
                // response budget expired while quorum transport was active.
                sinomo_kv::CLUSTER_KV_COMMIT_SEQUENCE_LOCK_WAIT
            } else {
                remaining_raft_commit_budget(
                    operation_deadline,
                    "pre-append commit-sequence admission",
                )?
            };
            let deadline = (!proposal_may_be_durable).then(|| {
                (
                    operation_deadline,
                    raft_commit_phase_timeout("pre-append commit-sequence admission"),
                    ClusterRaftBlockingDeadlinePolicy::DeadlineBounded,
                )
            });
            let cancellation_policy = if proposal_may_be_durable {
                ClusterRaftBlockingCancellationPolicy::CompleteAlways
            } else {
                ClusterRaftBlockingCancellationPolicy::CancelQueued
            };
            let result = run_cluster_raft_supervised_blocking(
                runtime,
                deadline,
                cancellation_policy,
                "quorum_commit",
                "commit sequence lock",
                "cluster Raft commit sequence lock",
                move || {
                    let wait = if proposal_may_be_durable {
                        wait
                    } else {
                        // Count blocking-pool queue delay. A canceled join handle
                        // can leave this closure runnable, so it must refuse to
                        // acquire the sequence lock after the original deadline.
                        remaining_raft_commit_budget(
                            operation_deadline,
                            "pre-append commit-sequence admission",
                        )?
                    };
                    sinomo_kv::lock_cluster_kv_commit_sequence(&kv_paths, wait).map_err(|error| {
                        if !proposal_may_be_durable && Instant::now() >= operation_deadline {
                            raft_commit_phase_timeout("pre-append commit-sequence admission")
                        } else {
                            kv_err(error)
                        }
                    })
                },
            )
            .await;
            match &result {
                Ok(_) => trace.record(
                    "leader_queue",
                    started.elapsed(),
                    term,
                    0,
                    revision,
                    "acquired",
                    format_args!("acquired the cluster KV commit sequence lock"),
                ),
                Err(error) => trace.record(
                    "leader_queue",
                    started.elapsed(),
                    term,
                    0,
                    revision,
                    "error",
                    format_args!("{error}"),
                ),
            }
            result.map(Some)
        })
    }

    fn replay_cluster_raft_request_if_committed_async<'a>(
        &'a mut self,
        request: &'a KvCommandRequest,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<
        'a,
        Option<(KvSnapshot, sinomo_kv::KvCommittedRequestRecord)>,
        NetworkError,
    > {
        let runtime = self.runtime;
        let storage = Arc::clone(&self.storage);
        let preappend_deadline = (!self.proposal_may_be_durable).then_some(self.operation_deadline);
        let request = request.clone();
        Box::pin(async move {
            run_cluster_raft_storage_phase(
                runtime,
                storage,
                preappend_deadline,
                "replay committed cluster Raft request",
                move |storage| storage.replay_if_committed(&request),
            )
            .await
        })
    }

    fn replay_cluster_raft_request_record_if_committed_async<'a>(
        &'a mut self,
        request: &'a KvCommandRequest,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<
        'a,
        Option<sinomo_kv::KvCommittedRequestRecord>,
        NetworkError,
    > {
        let runtime = self.runtime;
        let storage = Arc::clone(&self.storage);
        let preappend_deadline = (!self.proposal_may_be_durable).then_some(self.operation_deadline);
        let request = request.clone();
        Box::pin(async move {
            run_cluster_raft_storage_phase(
                runtime,
                storage,
                preappend_deadline,
                "replay committed cluster Raft request record",
                move |storage| storage.replay_record_if_committed(&request),
            )
            .await
        })
    }

    fn append_cluster_raft_request_uncommitted_async<'a>(
        &'a mut self,
        request: KvCommandRequest,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, sinomo_kv::KvLogEntry, NetworkError> {
        let runtime = self.runtime;
        let storage = Arc::clone(&self.storage);
        let deadline = self.operation_deadline;
        let proposal_may_be_durable = &mut self.proposal_may_be_durable;
        let trace = self.trace.clone();
        let initial_term = self.initial_term;
        let initial_revision = self.initial_revision;
        Box::pin(async move {
            let started = Instant::now();
            let observed = run_cluster_raft_storage_phase(
                runtime,
                storage,
                Some(deadline),
                "append uncommitted cluster Raft request",
                move |storage| Ok(storage.append_uncommitted_observed(request)),
            )
            .await;
            let observed = match observed {
                Ok(observed) => observed,
                Err(error) => {
                    trace.record(
                        "compare",
                        started.elapsed(),
                        initial_term,
                        0,
                        initial_revision,
                        "error",
                        format_args!("storage session failed before request comparison: {error}"),
                    );
                    return Err(error);
                }
            };
            // A successful append, an adopted durable Barrier, or an
            // ambiguous WAL append error all cross the point where the
            // proposal can exist durably. Subsequent sequence admission is
            // reconciliation/apply work and must not be canceled by the
            // original pre-append response deadline.
            *proposal_may_be_durable = observed.log_append_wal_duration.is_some();

            if observed.log_append_wal_duration.is_some() {
                trace.record(
                    "compare",
                    observed.compare_duration,
                    initial_term,
                    0,
                    initial_revision,
                    "passed",
                    format_args!("request identity and transaction branch preflight passed"),
                );
            } else if let Err(error) = &observed.result {
                trace.record(
                    "compare",
                    observed.compare_duration,
                    initial_term,
                    0,
                    initial_revision,
                    "error",
                    format_args!("{error}"),
                );
            }

            if let Some(duration) = observed.log_append_wal_duration {
                match &observed.result {
                    Ok(entry) => trace.record(
                        "log_append_wal",
                        duration,
                        entry.term,
                        entry.index,
                        entry.revision.0,
                        "appended",
                        format_args!("request appended to the local durable Raft log"),
                    ),
                    Err(error) => trace.record(
                        "log_append_wal",
                        duration,
                        initial_term,
                        0,
                        initial_revision,
                        "error",
                        format_args!("{error}"),
                    ),
                }
            }
            observed.result.map_err(kv_err)
        })
    }

    fn release_cluster_raft_command_session_for_quorum_async<'a>(
        &'a mut self,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), NetworkError> {
        let runtime = self.runtime;
        let storage = Arc::clone(&self.storage);
        Box::pin(async move {
            run_cluster_raft_storage_phase(
                runtime,
                storage,
                None,
                "release cluster Raft storage before quorum transport",
                |storage| {
                    storage.release_session_for_quorum();
                    Ok::<_, NetworkError>(())
                },
            )
            .await
        })
    }

    fn record_cluster_raft_request_rejected_before_commit_async<'a>(
        &'a mut self,
        request: &'a KvCommandRequest,
        failure: &'a sinomo_kv::KvAuthorityFailure,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), NetworkError> {
        let runtime = self.runtime;
        let kv_paths = self.paths.kv_paths();
        let deadline = self.operation_deadline;
        let request = request.clone();
        let detail = failure.detail.clone();
        Box::pin(async move {
            run_cluster_raft_supervised_blocking(
                runtime,
                Some((
                    deadline,
                    raft_commit_phase_timeout("record rejected cluster Raft request"),
                    ClusterRaftBlockingDeadlinePolicy::DeadlineBounded,
                )),
                ClusterRaftBlockingCancellationPolicy::CancelQueued,
                "quorum_commit",
                "record rejected cluster Raft request",
                "request-attempt ledger rejection",
                move || {
                    sinomo_kv::mark_cluster_kv_request_rejected_before_commit_before(
                        &kv_paths, &request, detail, deadline,
                    )
                    .map_err(kv_err)
                },
            )
            .await
        })
    }

    fn record_cluster_raft_request_unknown_async<'a>(
        &'a mut self,
        request: &'a KvCommandRequest,
        detail: &'a str,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), NetworkError> {
        let runtime = self.runtime;
        let storage = Arc::clone(&self.storage);
        let kv_paths = self.paths.kv_paths();
        let request = request.clone();
        let detail = detail.to_string();
        Box::pin(async move {
            // Ambiguous post-append outcomes must be retained durably, but a
            // contended request ledger is independent of Raft resident state.
            // One caller-independent blocking supervisor returns that state,
            // drops the storage mutex, and then completes the ledger write;
            // cancellation cannot fall into a gap between those two actions.
            run_cluster_raft_supervised_blocking(
                runtime,
                None,
                ClusterRaftBlockingCancellationPolicy::CompleteAlways,
                "quorum_commit",
                "record unknown cluster Raft request",
                "request-attempt ledger unknown reconciliation",
                move || {
                    {
                        let mut storage = lock_local_cluster_raft_command_storage(
                            &storage,
                            "release cluster Raft storage before unknown reconciliation",
                        )?;
                        storage.release_session_for_quorum();
                    }
                    sinomo_kv::mark_cluster_kv_request_unknown(&kv_paths, &request, detail)
                        .map_err(kv_err)
                },
            )
            .await
        })
    }

    fn discard_cluster_raft_uncommitted_tail_and_clear_leader_async<'a>(
        &'a mut self,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), NetworkError> {
        let runtime = self.runtime;
        let storage = Arc::clone(&self.storage);
        Box::pin(async move {
            run_cluster_raft_storage_phase(
                runtime,
                storage,
                None,
                "discard uncommitted cluster Raft tail",
                |storage| storage.discard_uncommitted_tail_and_clear_leader_compact(),
            )
            .await
        })
    }

    fn step_down_preserving_cluster_raft_pending_entry_async<'a>(
        &'a mut self,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), NetworkError> {
        let runtime = self.runtime;
        let storage = Arc::clone(&self.storage);
        Box::pin(async move {
            run_cluster_raft_storage_phase(
                runtime,
                storage,
                None,
                "step down while preserving pending cluster Raft entry",
                |storage| storage.step_down_preserving_pending_entry(),
            )
            .await
        })
    }

    fn commit_cluster_raft_index_with_events_async<'a>(
        &'a mut self,
        index: u64,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, sinomo_kv::ClusterKvRaftCommitResult, NetworkError>
    {
        let runtime = self.runtime;
        let storage = Arc::clone(&self.storage);
        let trace = self.trace.clone();
        let initial_term = self.initial_term;
        let initial_revision = self.initial_revision;
        Box::pin(async move {
            let started = Instant::now();
            let result = run_cluster_raft_storage_phase(
                runtime,
                storage,
                None,
                "commit cluster Raft index",
                move |storage| storage.commit_index_with_events(index),
            )
            .await;
            match &result {
                Ok(commit) => trace.record(
                    "apply",
                    started.elapsed(),
                    commit.committed_request.term,
                    index,
                    commit.committed_request.revision.0,
                    "applied",
                    format_args!(
                        "committed the local Raft entry and published its materialized result"
                    ),
                ),
                Err(error) => trace.record(
                    "apply",
                    started.elapsed(),
                    initial_term,
                    index,
                    initial_revision,
                    "error",
                    format_args!("{error}"),
                ),
            }
            result
        })
    }

    fn commit_cluster_raft_index_compact_async<'a>(
        &'a mut self,
        index: u64,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<
        'a,
        sinomo_kv::ClusterKvRaftCompactCommitResult,
        NetworkError,
    > {
        let runtime = self.runtime;
        let storage = Arc::clone(&self.storage);
        let trace = self.trace.clone();
        let initial_term = self.initial_term;
        let initial_revision = self.initial_revision;
        Box::pin(async move {
            let started = Instant::now();
            let result = run_cluster_raft_storage_phase(
                runtime,
                storage,
                None,
                "compact-commit cluster Raft index",
                move |storage| storage.commit_index_compact(index),
            )
            .await;
            match &result {
                Ok(commit) => trace.record(
                    "apply",
                    started.elapsed(),
                    commit.committed_request.term,
                    index,
                    commit.committed_request.revision.0,
                    "applied",
                    format_args!("committed the local Raft entry and published its compact delta"),
                ),
                Err(error) => trace.record(
                    "apply",
                    started.elapsed(),
                    initial_term,
                    index,
                    initial_revision,
                    "error",
                    format_args!("{error}"),
                ),
            }
            result
        })
    }

    fn release_cluster_raft_command_session_after_commit_async<'a>(
        &'a mut self,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), NetworkError> {
        let runtime = self.runtime;
        let storage = Arc::clone(&self.storage);
        Box::pin(async move {
            run_cluster_raft_storage_phase(
                runtime,
                storage,
                None,
                "release committed cluster Raft command session",
                |storage| {
                    storage.release_session_after_commit();
                    Ok::<_, NetworkError>(())
                },
            )
            .await
        })
    }

    fn publish_cluster_raft_post_commit_observations_async<'a>(
        &'a mut self,
        commit: Option<&'a sinomo_kv::ClusterKvRaftCompactCommitResult>,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), NetworkError> {
        let runtime = self.runtime;
        let storage = Arc::clone(&self.storage);
        let commit = commit.cloned();
        Box::pin(async move {
            run_cluster_raft_supervised_blocking(
                runtime,
                None,
                ClusterRaftBlockingCancellationPolicy::CompleteAlways,
                "quorum_commit",
                "publish committed cluster Raft observations",
                "publish committed cluster Raft observations",
                move || {
                    let observations = {
                        let mut storage = lock_local_cluster_raft_command_storage(
                            &storage,
                            "take committed cluster Raft observations",
                        )?;
                        storage.take_post_commit_observations(commit.as_ref())?
                    };
                    // Invoke arbitrary synchronous observer callbacks only after
                    // the storage guard above has been dropped. The generic
                    // quorum driver has already dropped the proposal lock too.
                    observations.publish();
                    Ok::<_, NetworkError>(())
                },
            )
            .await
        })
    }

    fn remote_voter_targets_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, Vec<Self::Target>, NetworkError> {
        Box::pin(async move {
            if sinomo_kv::cluster_kv_raft_local_is_voter(state)
                && sinomo_kv::cluster_kv_raft_quorum_size_for_state(state) <= 1
            {
                return Ok(Vec::new());
            }
            Ok(sinomo_kv::cluster_kv_raft_voter_ids(state)
                .map_err(kv_err)?
                .into_iter()
                .filter(|node_id| node_id != &state.local_node_id)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect())
        })
    }

    fn target_node_id<'a>(&self, target: &'a Self::Target) -> &'a NodeId {
        target
    }

    fn target_description(&self, target: &Self::Target) -> String {
        target.to_string()
    }

    fn replicate_cluster_raft_to_target_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        target: &'a Self::Target,
        timeout: Duration,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, ClusterRaftAppendResponse, NetworkError> {
        let paths = self.paths;
        let runtime = self.runtime;
        let storage = Arc::clone(&self.storage);
        let operation_deadline = self.operation_deadline;
        let append_options = self.append_options;
        let host = &mut *self.host;
        Box::pin(async move {
            let timeout = timeout.min(remaining_raft_commit_budget(
                operation_deadline,
                "single-target quorum replication",
            )?);
            let mut node_host = AsyncNodeRaftAppendReplicationHost {
                runtime,
                paths,
                storage,
                options: ClusterRaftAppendReplicationOptions {
                    timeout,
                    ..append_options
                },
                host,
            };
            sinomo_kv::replicate_cluster_kv_raft_to_target_async_with_host(
                state,
                target,
                &[],
                node_host.options.kv_options(),
                &mut node_host,
            )
            .await
            .map(|outcome| outcome.response)
        })
    }

    fn replicate_cluster_raft_to_quorum_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        targets: Vec<Self::Target>,
        required_remote_acks: usize,
        timeout: Duration,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<
        'a,
        Vec<sinomo_kv::ClusterKvRaftTargetReplicationAttempt<Self::Target, Self::Error>>,
        Self::Error,
    > {
        let runtime = self.runtime;
        let paths = self.paths;
        let storage = Arc::clone(&self.storage);
        let operation_deadline = self.operation_deadline;
        let append_options = self.append_options;
        let trace = self.trace.clone();
        let host = &mut *self.host;
        Box::pin(async move {
            let started = Instant::now();
            let timeout_deadline = started.checked_add(timeout).unwrap_or(operation_deadline);
            let deadline = timeout_deadline.min(operation_deadline);
            let target_count = targets.len();
            if required_remote_acks == 0 || targets.is_empty() {
                trace.record(
                    "replication",
                    started.elapsed(),
                    state.term,
                    0,
                    state.kv_revision,
                    "quorum",
                    format_args!(
                        "replicated to 0 of {required_remote_acks} required remote voters across {target_count} targets"
                    ),
                );
                return Ok(Vec::new());
            }

            struct PendingTarget {
                target: NodeId,
                next_index: u64,
                attempts: usize,
                started: std::time::Instant,
                last_response: Option<ClusterRaftAppendResponse>,
            }

            let last_log_index = {
                let storage = Arc::clone(&storage);
                remaining_raft_quorum_budget(deadline, "resident log-index preparation")?;
                run_cluster_raft_supervised_blocking(
                    runtime,
                    Some((
                        deadline,
                        raft_quorum_phase_timeout("resident log-index preparation"),
                        ClusterRaftBlockingDeadlinePolicy::DeadlineBounded,
                    )),
                    ClusterRaftBlockingCancellationPolicy::CancelQueued,
                    "raft_replication",
                    "resident log-index preparation",
                    "read resident cluster Raft log index",
                    move || {
                        let mut storage = lock_local_cluster_raft_command_storage(
                            &storage,
                            "read resident cluster Raft log index",
                        )?;
                        remaining_raft_quorum_budget(deadline, "resident log-index preparation")?;
                        storage.last_log_index()
                    },
                )
                .await?
            };
            let default_next_index = last_log_index.saturating_add(1);
            // Peer progress is one bounded soft-state ledger. Decode it once
            // for the whole replication wave instead of issuing one blocking
            // file read per voter in series.
            let peer_progress = {
                let kv_paths = paths.kv_paths();
                remaining_raft_quorum_budget(deadline, "peer-progress preparation")?;
                run_cluster_raft_supervised_blocking(
                    runtime,
                    Some((
                        deadline,
                        raft_quorum_phase_timeout("peer-progress preparation"),
                        ClusterRaftBlockingDeadlinePolicy::DeadlineBounded,
                    )),
                    ClusterRaftBlockingCancellationPolicy::CancelQueued,
                    "raft_replication",
                    "peer-progress preparation",
                    "read cluster Raft peer progress",
                    move || {
                        remaining_raft_quorum_budget(deadline, "peer-progress preparation")?;
                        sinomo_kv::read_cluster_kv_raft_peer_progress(&kv_paths).map_err(kv_err)
                    },
                )
                .await
                .ok()
                .unwrap_or_default()
            };
            let mut pending = Vec::new();
            for target in targets {
                let next_index =
                    sinomo_kv::cluster_kv_raft_peer_progress_next_index_hint_from_records(
                        &peer_progress,
                        &target,
                        state.term,
                        last_log_index,
                    )
                    .filter(|next_index| (1..=default_next_index).contains(next_index))
                    .unwrap_or(default_next_index);
                pending.push(PendingTarget {
                    target,
                    next_index,
                    attempts: 0,
                    started: std::time::Instant::now(),
                    last_response: None,
                });
            }
            let mut accepted = 0_usize;
            let mut completed = Vec::new();
            while accepted < required_remote_acks && !pending.is_empty() {
                let remaining = deadline
                    .checked_duration_since(std::time::Instant::now())
                    .unwrap_or_default();
                if remaining.is_zero() {
                    break;
                }

                // Build the complete send wave under one resident-storage
                // acquisition. The requests may have different next indexes,
                // but they do not need one spawn_blocking round trip and mutex
                // acquisition per voter.
                let next_indexes = pending
                    .iter()
                    .filter(|target| target.attempts < append_options.max_attempts)
                    .map(|target| target.next_index)
                    .collect::<Vec<_>>();
                let requests = {
                    let storage = Arc::clone(&storage);
                    let state = state.clone();
                    remaining_raft_quorum_budget(deadline, "append request-wave build")?;
                    run_cluster_raft_supervised_blocking(
                        runtime,
                        Some((
                            deadline,
                            raft_quorum_phase_timeout("append request-wave build"),
                            ClusterRaftBlockingDeadlinePolicy::DeadlineBounded,
                        )),
                        ClusterRaftBlockingCancellationPolicy::CancelQueued,
                        "raft_replication",
                        "append request-wave build",
                        "build resident cluster Raft append request wave",
                        move || {
                            let mut storage = lock_local_cluster_raft_command_storage(
                                &storage,
                                "build resident cluster Raft append request wave",
                            )?;
                            remaining_raft_quorum_budget(deadline, "append request-wave build")?;
                            // Voters commonly share the same progress index.
                            // Build each distinct tail/snapshot once per wave
                            // and Arc-share it across the matching transports;
                            // repeated durable tail construction and deep
                            // cloning are no longer proportional to voter
                            // count. Keep distinct indexes serial under this
                            // one resident operation fence: independently
                            // opening/building them would contend the same
                            // exclusive store lock and could lose the coherent
                            // log view this commit session guarantees.
                            let mut request_builds = Vec::with_capacity(next_indexes.len());
                            for next_index in next_indexes {
                                let key =
                                    if storage.raft_append_request_uses_snapshot(next_index)? {
                                        ClusterRaftAppendRequestBuildKey::Snapshot
                                    } else {
                                        ClusterRaftAppendRequestBuildKey::Tail(next_index)
                                    };
                                request_builds.push((key, next_index));
                            }
                            build_deduplicated_append_request_wave(&request_builds, |next_index| {
                                storage.build_raft_append_request(&state, next_index)
                            })
                        },
                    )
                    .await?
                };
                let mut sends = Vec::new();
                for (target, request) in pending
                    .iter_mut()
                    .filter(|target| target.attempts < append_options.max_attempts)
                    .zip(requests)
                {
                    target.attempts = target.attempts.saturating_add(1);
                    record_cluster_kv_replication_send(&request);
                    let callback_budget =
                        remaining_raft_quorum_budget(deadline, "append send callback")?;
                    tokio::time::timeout(callback_budget, host.on_raft_append_sent_async())
                        .await
                        .map_err(|_| raft_quorum_phase_timeout("append send callback"))??;
                    let attempt_budget =
                        remaining_raft_quorum_budget(deadline, "append quorum send")?;
                    sends.push((
                        target.target.clone(),
                        request,
                        append_options.routed_node_append_attempt_timeout(attempt_budget),
                    ));
                }
                if sends.is_empty() {
                    break;
                }

                let send_budget = remaining_raft_quorum_budget(deadline, "append quorum send")?;
                let outcomes = tokio::time::timeout(
                    send_budget,
                    host.send_raft_appends_to_quorum_async(
                        paths,
                        state,
                        sends,
                        required_remote_acks.saturating_sub(accepted),
                    ),
                )
                .await
                .map_err(|_| raft_quorum_phase_timeout("append quorum send"))??;
                if outcomes.is_empty() {
                    break;
                }
                for (target_node_id, request, result) in outcomes {
                    let Some(position) = pending
                        .iter()
                        .position(|pending| pending.target == target_node_id)
                    else {
                        continue;
                    };
                    let mut target = pending.swap_remove(position);
                    match result {
                        Ok(response) if response.accepted => {
                            record_cluster_kv_replication_catch_up("accepted", target.attempts);
                            accepted = accepted.saturating_add(1);
                            completed.push(sinomo_kv::ClusterKvRaftTargetReplicationAttempt {
                                target: target.target,
                                latency_ms: Some(
                                    target
                                        .started
                                        .elapsed()
                                        .as_millis()
                                        .min(u128::from(u64::MAX))
                                        as u64,
                                ),
                                result: Ok(response),
                            });
                        }
                        Ok(response) if response.append.term > request.append.term => {
                            record_cluster_kv_replication_catch_up("higher_term", target.attempts);
                            completed.push(sinomo_kv::ClusterKvRaftTargetReplicationAttempt {
                                target: target.target,
                                latency_ms: Some(
                                    target
                                        .started
                                        .elapsed()
                                        .as_millis()
                                        .min(u128::from(u64::MAX))
                                        as u64,
                                ),
                                result: Ok(response),
                            });
                        }
                        Ok(response) if target.attempts < append_options.max_attempts => {
                            metrics::counter!("sinomo.kv.replication.backtracks").increment(1);
                            target.next_index =
                                sinomo_kv::cluster_kv_raft_next_index_after_rejection(
                                    target.next_index,
                                    &request,
                                    response.append.match_index,
                                );
                            target.last_response = Some(response);
                            pending.push(target);
                        }
                        Ok(response) => {
                            record_cluster_kv_replication_catch_up("rejected", target.attempts);
                            completed.push(sinomo_kv::ClusterKvRaftTargetReplicationAttempt {
                                target: target.target,
                                latency_ms: Some(
                                    target
                                        .started
                                        .elapsed()
                                        .as_millis()
                                        .min(u128::from(u64::MAX))
                                        as u64,
                                ),
                                result: Ok(response),
                            });
                        }
                        Err(error) => {
                            record_cluster_kv_replication_catch_up("failed", target.attempts);
                            completed.push(sinomo_kv::ClusterKvRaftTargetReplicationAttempt {
                                target: target.target,
                                latency_ms: Some(
                                    target
                                        .started
                                        .elapsed()
                                        .as_millis()
                                        .min(u128::from(u64::MAX))
                                        as u64,
                                ),
                                result: Err(error),
                            });
                        }
                    }
                }
            }

            if accepted < required_remote_acks {
                for target in pending {
                    record_cluster_kv_replication_catch_up("timeout", target.attempts);
                    let response =
                        target
                            .last_response
                            .unwrap_or_else(|| ClusterRaftAppendResponse {
                                accepted: false,
                                detail: format!(
                                    "raft append did not complete before {}ms catch-up budget",
                                    timeout.as_millis()
                                ),
                                state: state.clone(),
                                append: sinomo_kv::RaftAppendEntriesResponse {
                                    term: 0,
                                    success: false,
                                    match_index: 0,
                                },
                            });
                    completed.push(sinomo_kv::ClusterKvRaftTargetReplicationAttempt {
                        target: target.target,
                        latency_ms: Some(
                            target
                                .started
                                .elapsed()
                                .as_millis()
                                .min(u128::from(u64::MAX)) as u64,
                        ),
                        result: Ok(response),
                    });
                }
            }
            trace.record(
                "replication",
                started.elapsed(),
                state.term,
                0,
                state.kv_revision,
                if accepted >= required_remote_acks {
                    "quorum"
                } else {
                    "incomplete"
                },
                format_args!(
                    "replicated to {accepted} of {required_remote_acks} required remote voters across {target_count} targets"
                ),
            );
            Ok(completed)
        })
    }

    fn record_target_replication_status_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        target: &'a Self::Target,
        status: sinomo_kv::ClusterKvRaftTargetReplicationStatus,
        latency_ms: Option<u64>,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), NetworkError> {
        let paths = self.paths;
        let host = &mut *self.host;
        Box::pin(async move {
            record_cluster_kv_replication_target_result(status, latency_ms);
            let connectivity_state = match status {
                sinomo_kv::ClusterKvRaftTargetReplicationStatus::Accepted
                | sinomo_kv::ClusterKvRaftTargetReplicationStatus::Rejected => {
                    ClusterConnectivityState::Healthy
                }
                sinomo_kv::ClusterKvRaftTargetReplicationStatus::Failed => {
                    ClusterConnectivityState::Degraded
                }
            };
            host.record_node_command_connectivity(
                paths,
                state,
                target,
                connectivity_state,
                latency_ms,
            );
            Ok(())
        })
    }

    fn observe_accepted_append_state_async<'a>(
        &'a mut self,
        state: &'a mut ClusterBootstrapState,
        _target: &'a Self::Target,
        peer_state: ClusterBootstrapState,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), NetworkError> {
        Box::pin(async move {
            merge_cluster_state_from_peer(state, peer_state);
            Ok(())
        })
    }

    fn publish_cluster_raft_higher_term(
        &mut self,
        state: &ClusterBootstrapState,
        term: u64,
    ) -> Result<(), NetworkError> {
        publish_cluster_raft_higher_term_reactively(self.paths, &state.local_node_id, term)
    }

    fn observe_cluster_raft_term_async<'a>(
        &'a mut self,
        _state: &'a ClusterBootstrapState,
        term: u64,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), NetworkError> {
        let storage = Arc::clone(&self.storage);
        let runtime = self.runtime.clone();
        Box::pin(async move {
            run_cluster_raft_storage_phase(
                &runtime,
                storage,
                None,
                "observe cluster Raft term",
                move |storage| storage.observe_pending_higher_term(term),
            )
            .await
        })
    }

    fn fan_out_committed_cluster_raft_command_async<'a>(
        &'a mut self,
        state: ClusterBootstrapState,
        events: Vec<KvEvent>,
        targets: Vec<Self::Target>,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), NetworkError> {
        let paths = self.paths.clone();
        let trace = self.trace.clone();
        let term = state.term;
        let revision = state.kv_revision;
        let event_count = events.len();
        let target_count = targets.len();
        let host = &mut *self.host;
        Box::pin(async move {
            let started = Instant::now();
            let result = host
                .fan_out_committed_cluster_raft_command_async(paths, state, events, targets)
                .await;
            match &result {
                Ok(()) => trace.record(
                    "response",
                    started.elapsed(),
                    term,
                    0,
                    revision,
                    "completed",
                    format_args!(
                        "fanned out {event_count} committed events to {target_count} targets"
                    ),
                ),
                Err(error) => trace.record(
                    "response",
                    started.elapsed(),
                    term,
                    0,
                    revision,
                    "error",
                    format_args!("{error}"),
                ),
            }
            result
        })
    }

    fn cluster_authority_error(&mut self, failure: sinomo_kv::KvAuthorityFailure) -> NetworkError {
        NetworkError::ClusterReadOnly {
            detail: serde_json::to_string(&failure).unwrap_or_else(|_| format!("{failure:?}")),
        }
    }
}

/// Legacy full-view wrapper around the compact daemon commit path.
pub async fn commit_cluster_raft_command_with_quorum_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    request: KvCommandRequest,
    options: ClusterKvRaftQuorumCommitOptions,
    append_options: ClusterRaftAppendReplicationOptions,
    host: &mut H,
) -> Result<ClusterKvRaftQuorumCommitResult, NetworkError>
where
    H: ClusterRaftQuorumAsyncHost,
{
    let started = Instant::now();
    let deadline = started.checked_add(options.timeout).unwrap_or(started);
    let compact = commit_cluster_raft_command_compact_with_quorum_async_before_deadline(
        runtime,
        paths,
        state,
        request,
        options,
        append_options,
        deadline,
        host,
    )
    .await?;
    // The authority outcome is already durably committed. Materializing the
    // legacy full view is response shaping, not another mutation phase, so do
    // not reuse the now-expired proposal deadline and turn an exact commit into
    // a retryable timeout/unknown result.
    let kv_paths = paths.kv_paths();
    let local_node_id = state.local_node_id.clone();
    let snapshot = run_cluster_raft_supervised_blocking(
        runtime,
        None,
        ClusterRaftBlockingCancellationPolicy::CompleteAlways,
        "quorum_commit",
        "materialize committed snapshot",
        "materialize cluster KV quorum result",
        move || {
            sinomo_kv::read_cluster_kv_current_snapshot_for_node(&kv_paths, local_node_id)
                .map_err(kv_err)
        },
    )
    .await?;
    Ok(ClusterKvRaftQuorumCommitResult {
        snapshot,
        txn_response: compact.txn_response,
        authority: compact.authority,
        replayed_committed_request: compact.replayed_committed_request,
    })
}

/// Canonical daemon mutation path. The result is bounded by the command delta;
/// periodic checkpoint materialization remains local to the committing node.
pub async fn commit_cluster_raft_command_compact_with_quorum_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    request: KvCommandRequest,
    options: ClusterKvRaftQuorumCommitOptions,
    append_options: ClusterRaftAppendReplicationOptions,
    host: &mut H,
) -> Result<sinomo_kv::ClusterKvRaftQuorumCompactCommitResult, NetworkError>
where
    H: ClusterRaftQuorumAsyncHost,
{
    let started = Instant::now();
    let deadline = started.checked_add(options.timeout).unwrap_or(started);
    commit_cluster_raft_command_compact_with_quorum_async_before_deadline(
        runtime,
        paths,
        state,
        request,
        options,
        append_options,
        deadline,
        host,
    )
    .await
}

async fn commit_cluster_raft_command_compact_with_quorum_async_before_deadline<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    request: KvCommandRequest,
    options: ClusterKvRaftQuorumCommitOptions,
    append_options: ClusterRaftAppendReplicationOptions,
    deadline: Instant,
    host: &mut H,
) -> Result<sinomo_kv::ClusterKvRaftQuorumCompactCommitResult, NetworkError>
where
    H: ClusterRaftQuorumAsyncHost,
{
    let local_node_id = state.local_node_id.clone();
    let storage = Arc::new(Mutex::new(
        LocalClusterRaftCommandStorage::new_with_operation_deadline(
            paths,
            local_node_id.clone(),
            deadline,
        ),
    ));
    let mut consensus_state = state.clone();
    consensus_state = run_cluster_raft_storage_phase(
        runtime,
        Arc::clone(&storage),
        Some(deadline),
        "project committed cluster Raft config",
        move |storage| {
            let projection = (|| {
                // Capture hard state, the applied revision, and the committed
                // config from one resident view. The topology snapshot can lag
                // a locally advanced term; handing that stale term to
                // `local_is_leader` would reject a healthy leader and start a
                // needless authority retry loop.
                let read_state = storage.read_index_state()?;
                consensus_state.term = consensus_state.term.max(read_state.current_term);
                consensus_state.kv_revision =
                    consensus_state.kv_revision.max(read_state.revision.0);
                if let Some(entry) =
                    storage.read_index_entry(&sinomo_kv::cluster_kv_raft_config_key())?
                {
                    let config: sinomo_kv::ClusterKvRaftConfigRecord =
                        entry.decode().map_err(kv_err)?;
                    project_cluster_raft_config_record(&mut consensus_state, &config)?;
                }
                Ok::<_, NetworkError>(())
            })();
            // Configuration projection precedes generic proposal admission.
            // Never carry its resident/operation ownership into the await for
            // the commit-sequence lock: a queued writer must not block point
            // or exact ReadIndex work before it owns the proposal turn.
            storage.release_session_for_quorum();
            projection?;
            Ok(consensus_state)
        },
    )
    .await?;
    let request_id = request.request_id.clone();
    let initial_term = consensus_state.term;
    let initial_revision = consensus_state.kv_revision;
    let trace = ClusterKvQuorumCommitTrace::new(
        request_id,
        local_node_id.clone(),
        initial_term,
        initial_revision,
    );
    let mut kv_host = NetworkClusterKvRaftQuorumCommitAsyncHost {
        runtime,
        paths,
        storage,
        operation_deadline: deadline,
        proposal_may_be_durable: false,
        append_options,
        trace: trace.clone(),
        initial_term,
        initial_revision,
        host,
    };
    let commit_started = Instant::now();
    let mut bounded_options = options;
    bounded_options.timeout =
        remaining_raft_commit_budget(deadline, "pre-append quorum driver admission")?;
    let result = sinomo_kv::commit_cluster_kv_raft_command_compact_with_quorum_async(
        &mut consensus_state,
        request,
        bounded_options,
        &mut kv_host,
    )
    .await;
    match &result {
        Ok(commit) => trace.record(
            "commit_complete",
            commit_started.elapsed(),
            commit.authority.term,
            commit.authority.commit_index,
            commit.authority.revision.0,
            if commit.replayed_committed_request {
                "replayed"
            } else {
                "completed"
            },
            format_args!(
                "asynchronous quorum commit future returned after releasing its commit sequence lock"
            ),
        ),
        Err(error) => trace.record(
            "commit_complete",
            commit_started.elapsed(),
            initial_term,
            0,
            initial_revision,
            "error",
            format_args!(
                "asynchronous quorum commit future returned after releasing its commit sequence lock: {error}"
            ),
        ),
    }
    drop(kv_host);
    trace.flush();
    let result = result?;
    state.term = state.term.max(consensus_state.term);
    state.kv_revision = state.kv_revision.max(consensus_state.kv_revision);
    Ok(result)
}

/// Confirms a linearizable read against a current-term voter quorum without
/// appending a no-op entry to the Raft log.
///
/// This follows etcd's ReadIndex safety shape: the leader must already have a
/// committed entry in its current term, the exact applied read view and
/// heartbeat anchor are captured coherently, and a majority must acknowledge
/// that anchor in the same term. `None` asks the caller to use the existing
/// barrier path, which is required for the first read after an election and is
/// also a safe catch-up fallback when a follower rejects the compact probe.
fn remaining_read_index_budget(deadline: Instant, phase: &str) -> Result<Duration, NetworkError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(NetworkError::ClusterReadOnly {
            detail: format!("Raft ReadIndex timed out during {phase}"),
        });
    }
    Ok(remaining)
}

fn read_index_phase_timeout(phase: &str) -> NetworkError {
    NetworkError::ClusterReadOnly {
        detail: format!("Raft ReadIndex timed out during {phase}"),
    }
}

const CLUSTER_RAFT_BLOCKING_QUEUED: u8 = 0;
const CLUSTER_RAFT_BLOCKING_ADMITTED: u8 = 1;
const CLUSTER_RAFT_BLOCKING_CANCELLED: u8 = 2;
const CLUSTER_RAFT_BLOCKING_COMPLETE: u8 = 3;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClusterRaftBlockingCancellationPolicy {
    /// A dropped caller or elapsed deadline prevents work that has not started.
    CancelQueued,
    /// Safety work such as higher-term persistence survives caller cancellation.
    CompleteAlways,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClusterRaftBlockingDeadlinePolicy {
    /// Complete admitted work, then report the original deadline failure.
    DeadlineBounded,
    /// Complete admitted work and return its coherent result after the deadline.
    CompleteAdmitted,
}

struct ClusterRaftSupervisedBlockingState<T> {
    admission: AtomicU8,
    result: Mutex<Option<Result<T, NetworkError>>>,
    completion: tokio::sync::Notify,
}

/// Cancels only blocking work that has not won admission yet.
///
/// Tokio cannot interrupt an admitted `spawn_blocking` closure. Aborting that
/// join handle on timeout would therefore only detach resident Raft ownership
/// from the authority read that created it. The worker-side CAS gives timeout
/// and external cancellation one exact point at which queued work can still be
/// prevented from opening storage.
struct ClusterRaftQueuedBlockingCancellation<T> {
    state: Arc<ClusterRaftSupervisedBlockingState<T>>,
    abort: tokio::task::AbortHandle,
    armed: bool,
}

impl<T> ClusterRaftQueuedBlockingCancellation<T> {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl<T> Drop for ClusterRaftQueuedBlockingCancellation<T> {
    fn drop(&mut self) {
        if self.armed
            && self
                .state
                .admission
                .compare_exchange(
                    CLUSTER_RAFT_BLOCKING_QUEUED,
                    CLUSTER_RAFT_BLOCKING_CANCELLED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
        {
            self.abort.abort();
        }
    }
}

fn record_cluster_raft_blocking_deadline_overrun(
    deadline: Instant,
    path: &'static str,
    phase: &'static str,
) {
    let overrun = Instant::now().saturating_duration_since(deadline);
    if overrun.is_zero() {
        return;
    }
    metrics::counter!(
        "sinomo.kv.authoritative_read.admitted_deadline_overrun",
        "path" => path,
        "phase" => phase,
    )
    .increment(1);
    metrics::histogram!(
        "sinomo.kv.authoritative_read.admitted_deadline_overrun.us",
        "path" => path,
        "phase" => phase,
    )
    .record(overrun.as_micros().min(u128::from(u64::MAX)) as f64);
}

async fn take_cluster_raft_supervised_blocking_result<T>(
    state: &ClusterRaftSupervisedBlockingState<T>,
) -> Result<T, NetworkError> {
    loop {
        // Register before checking the slot so completion between the check and
        // `.await` leaves a retained Notify permit instead of losing a wakeup.
        let completed = state.completion.notified();
        if let Some(result) = state
            .result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            return result;
        }
        completed.await;
    }
}

/// Supervise one blocking Raft phase independently of its async caller.
///
/// The blocking worker publishes its terminal result into shared state rather
/// than relying on a `JoinHandle` owned by the caller. Consequently, dropping
/// the async future can atomically cancel work that is still queued, but cannot
/// detach admitted resident ownership or mandatory durability. The admitted
/// worker always reaches the completion registry and releases its leases even
/// when the original future no longer exists.
pub(crate) async fn run_cluster_raft_supervised_blocking<T, F>(
    runtime: &tokio::runtime::Handle,
    deadline: Option<(Instant, NetworkError, ClusterRaftBlockingDeadlinePolicy)>,
    cancellation_policy: ClusterRaftBlockingCancellationPolicy,
    metric_path: &'static str,
    phase: &'static str,
    operation: &'static str,
    work: F,
) -> Result<T, NetworkError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, NetworkError> + Send + 'static,
{
    if cancellation_policy == ClusterRaftBlockingCancellationPolicy::CancelQueued
        && let Some((deadline, timeout_error, _)) = deadline.as_ref()
        && Instant::now() >= *deadline
    {
        return Err(timeout_error.clone());
    }
    let state = Arc::new(ClusterRaftSupervisedBlockingState {
        admission: AtomicU8::new(CLUSTER_RAFT_BLOCKING_QUEUED),
        result: Mutex::new(None),
        completion: tokio::sync::Notify::new(),
    });
    let worker_state = Arc::clone(&state);
    let task = runtime.spawn_blocking(move || {
        if worker_state
            .admission
            .compare_exchange(
                CLUSTER_RAFT_BLOCKING_QUEUED,
                CLUSTER_RAFT_BLOCKING_ADMITTED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return;
        }
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(work)).unwrap_or_else(|_| {
                Err(NetworkError::Io {
                    operation: operation.to_string(),
                    detail: "supervised blocking task panicked".to_string(),
                })
            });
        *worker_state
            .result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(result);
        worker_state
            .admission
            .store(CLUSTER_RAFT_BLOCKING_COMPLETE, Ordering::Release);
        // There is exactly one consumer of this terminal result. `notify_one`
        // retains a permit when completion wins the race before that consumer
        // first polls `notified()`; `notify_waiters` would lose that wakeup.
        worker_state.completion.notify_one();
    });
    let mut cancellation = ClusterRaftQueuedBlockingCancellation {
        state: Arc::clone(&state),
        abort: task.abort_handle(),
        armed: cancellation_policy == ClusterRaftBlockingCancellationPolicy::CancelQueued,
    };
    // The shared completion registry, not this handle, owns observation of an
    // admitted worker. Dropping the handle is intentional and safe.
    drop(task);

    let result = if let Some((deadline, timeout_error, deadline_policy)) = deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let completed = if remaining.is_zero() {
            None
        } else {
            tokio::select! {
                biased;
                result = take_cluster_raft_supervised_blocking_result(&state) => Some(result),
                _ = tokio::time::sleep(remaining) => None,
            }
        };
        let result = match completed {
            Some(result) => result,
            None => {
                if cancellation_policy == ClusterRaftBlockingCancellationPolicy::CancelQueued
                    && state
                        .admission
                        .compare_exchange(
                            CLUSTER_RAFT_BLOCKING_QUEUED,
                            CLUSTER_RAFT_BLOCKING_CANCELLED,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                {
                    cancellation.abort.abort();
                    return Err(timeout_error.clone());
                }
                // Admission (or mandatory completion) won the deadline edge.
                // Wait for the worker's terminal publication before deciding
                // whether its result is admissible to the original caller.
                take_cluster_raft_supervised_blocking_result(&state).await
            }
        };
        let overrun = Instant::now() >= deadline;
        if overrun {
            record_cluster_raft_blocking_deadline_overrun(deadline, metric_path, phase);
        }
        if overrun && deadline_policy == ClusterRaftBlockingDeadlinePolicy::DeadlineBounded {
            Err(timeout_error)
        } else {
            result
        }
    } else {
        take_cluster_raft_supervised_blocking_result(&state).await
    };
    cancellation.disarm();
    result
}

/// Run a bounded resident-Raft phase without detaching admitted blocking work.
///
/// A deadline or dropped caller atomically cancels a closure that is still in
/// Tokio's blocking queue. Once admitted, the closure is awaited to terminal
/// completion so its resident-node lease is released before this live caller
/// observes the timeout. A late successful result is still reported as a
/// deadline failure; completion is about ownership safety, not extending the
/// authority budget.
async fn run_read_index_blocking_before_deadline<T, F>(
    runtime: &tokio::runtime::Handle,
    deadline: Instant,
    phase: &'static str,
    operation: &'static str,
    work: F,
) -> Result<T, NetworkError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, NetworkError> + Send + 'static,
{
    remaining_read_index_budget(deadline, phase)?;
    run_cluster_raft_supervised_blocking(
        runtime,
        Some((
            deadline,
            read_index_phase_timeout(phase),
            ClusterRaftBlockingDeadlinePolicy::DeadlineBounded,
        )),
        ClusterRaftBlockingCancellationPolicy::CancelQueued,
        "read_index",
        phase,
        operation,
        work,
    )
    .await
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum ClusterRaftReadIndexAdmissionMode {
    WaitForLocal,
    ForwardOnBusy,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ReadIndexSingleflightKey {
    cluster_root: PathBuf,
    local_node_id: NodeId,
    admission_mode: ClusterRaftReadIndexAdmissionMode,
}

impl ReadIndexSingleflightKey {
    fn new(
        paths: &ClusterPaths,
        local_node_id: NodeId,
        admission_mode: ClusterRaftReadIndexAdmissionMode,
    ) -> Self {
        Self {
            cluster_root: paths.root.clone(),
            local_node_id,
            admission_mode,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReadIndexConfigurationFloor {
    voters: BTreeSet<NodeId>,
    learners: BTreeSet<NodeId>,
    non_voting: BTreeSet<NodeId>,
}

impl ReadIndexConfigurationFloor {
    fn from_config(config: &sinomo_kv::ClusterKvRaftConfigRecord) -> Self {
        Self {
            voters: config.voters.iter().cloned().collect(),
            learners: config.learners.iter().cloned().collect(),
            non_voting: config.non_voting.iter().cloned().collect(),
        }
    }

    fn from_state(state: &ClusterBootstrapState) -> Self {
        let config = sinomo_kv::ClusterKvRaftConfigRecord::from(state);
        Self::from_config(&config)
    }

    fn from_fence(fence: &ClusterRaftReadIndexAuthorityFence) -> Result<Self, NetworkError> {
        if let Some(entry) = fence.config_fence.as_ref() {
            let config: sinomo_kv::ClusterKvRaftConfigRecord = entry.decode().map_err(kv_err)?;
            return Ok(Self::from_config(&config));
        }
        Ok(Self::from_state(&fence.confirmed_state))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReadIndexMemberFloor {
    cluster_id: ClusterId,
    node_id: NodeId,
    display_name: String,
    node_role: ClusterNodeRole,
    role: ClusterMemberRole,
    state: ClusterMemberState,
    trust_domain: TrustDomain,
    node_public_key: String,
    certificate_id: Option<sinomo_base::Id>,
    capabilities: BTreeSet<String>,
    allowed_capabilities: BTreeSet<String>,
}

impl ReadIndexMemberFloor {
    fn from_member(member: &ClusterMemberRecord) -> Self {
        Self {
            cluster_id: member.cluster_id.clone(),
            node_id: member.node_id.clone(),
            display_name: member.display_name.clone(),
            node_role: member.node_role.clone(),
            role: member.role.clone(),
            state: member.state.clone(),
            trust_domain: member.trust_domain.clone(),
            node_public_key: member.node_public_key.clone(),
            certificate_id: member.certificate_id.clone(),
            capabilities: member
                .capabilities
                .iter()
                .map(|capability| capability.as_str().to_string())
                .collect(),
            allowed_capabilities: member
                .allowed_capabilities
                .iter()
                .map(|capability| capability.as_str().to_string())
                .collect(),
        }
    }
}

fn read_index_member_floors(
    state: &ClusterBootstrapState,
    configuration: &ReadIndexConfigurationFloor,
) -> BTreeMap<NodeId, Vec<ReadIndexMemberFloor>> {
    let allowed = configuration
        .voters
        .iter()
        .chain(&configuration.learners)
        .chain(&configuration.non_voting)
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut members = BTreeMap::<NodeId, Vec<ReadIndexMemberFloor>>::new();
    for member in &state.members {
        if allowed.contains(&member.node_id) {
            members
                .entry(member.node_id.clone())
                .or_default()
                .push(ReadIndexMemberFloor::from_member(member));
        }
    }
    members
}

/// Authority floors observed by a reader before it joins a coarse proof wave.
///
/// The owner may have entered with an older topology snapshot. A waiter can
/// share its network proof only when the durable fence is from the same
/// cluster/node, is at least as new in term and committed KV generation, and
/// names the exact same membership partition. This also rejects config ABA:
/// returning to the same member sets does not make an older KV generation
/// acceptable.
#[derive(Clone, Debug, Eq, PartialEq)]
struct ReadIndexClaimantFloor {
    cluster_id: ClusterId,
    local_node_id: NodeId,
    minimum_term: u64,
    minimum_kv_revision: u64,
    configuration: ReadIndexConfigurationFloor,
    members: BTreeMap<NodeId, Vec<ReadIndexMemberFloor>>,
}

impl ReadIndexClaimantFloor {
    fn from_state(state: &ClusterBootstrapState) -> Self {
        let configuration = ReadIndexConfigurationFloor::from_state(state);
        Self {
            cluster_id: state.cluster_id.clone(),
            local_node_id: state.local_node_id.clone(),
            minimum_term: state.term,
            minimum_kv_revision: state.kv_revision,
            members: read_index_member_floors(state, &configuration),
            configuration,
        }
    }

    fn shared_fence_mismatch(
        &self,
        fence: &ClusterRaftReadIndexAuthorityFence,
    ) -> Result<Option<&'static str>, NetworkError> {
        if fence.confirmed_state.cluster_id != self.cluster_id {
            return Ok(Some("cluster"));
        }
        if fence.confirmed_state.local_node_id != self.local_node_id {
            return Ok(Some("local_node"));
        }
        if fence.read_state.current_term < self.minimum_term {
            return Ok(Some("term"));
        }
        if fence.read_state.revision.0 < self.minimum_kv_revision {
            return Ok(Some("kv_generation"));
        }
        if ReadIndexConfigurationFloor::from_fence(fence)? != self.configuration {
            return Ok(Some("configuration"));
        }
        let mut confirmed_members =
            read_index_member_floors(&fence.confirmed_state, &self.configuration);
        let mut claimant_members = self.members.clone();
        if let Some(unresolved_local_node_id) = fence.unresolved_local_node_id.as_ref() {
            if unresolved_local_node_id != &self.local_node_id
                || claimant_members
                    .remove(unresolved_local_node_id)
                    .is_none_or(|members| members.len() != 1)
                || confirmed_members
                    .remove(unresolved_local_node_id)
                    .is_none_or(|members| members.len() != 1)
            {
                return Ok(Some("member_identity"));
            }
        }
        if confirmed_members != claimant_members {
            return Ok(Some("member_identity"));
        }
        Ok(None)
    }
}

#[derive(Clone, Debug)]
struct ReadIndexQuorumConfirmation {
    voter_acks: usize,
    quorum_size: usize,
    state_after_probe: ClusterBootstrapState,
}

#[derive(Clone, Debug)]
enum ReadIndexQuorumOutcome {
    Confirmed {
        fence: ClusterRaftReadIndexAuthorityFence,
        confirmation: ReadIndexQuorumConfirmation,
    },
    NeedsBarrier(ClusterRaftReadIndexAuthorityFence),
    NotLeader(ClusterRaftReadIndexAuthorityFence),
    ForwardOnly(ClusterRaftReadIndexForwardOnly),
    LocalAdmissionBusy,
    RetryFlight,
    HigherTerm {
        observed_term: u64,
        confirmed_term: u64,
    },
}

impl ReadIndexQuorumOutcome {
    fn authority_fence(&self) -> Option<&ClusterRaftReadIndexAuthorityFence> {
        match self {
            Self::Confirmed { fence, .. } | Self::NeedsBarrier(fence) | Self::NotLeader(fence) => {
                Some(fence)
            }
            Self::ForwardOnly(_)
            | Self::LocalAdmissionBusy
            | Self::RetryFlight
            | Self::HigherTerm { .. } => None,
        }
    }
}

type ReadIndexSingleflightResult = Result<ReadIndexQuorumOutcome, NetworkError>;

struct ReadIndexSingleflight {
    // This is the same notifier shape used by etcd's linearizable-read loop:
    // one owner performs the ReadIndex round and publishes one retained result
    // that releases every reader already waiting on the matching authority
    // fence. `watch` keeps publication race-free for late-polled waiters.
    result: tokio::sync::watch::Sender<Option<ReadIndexSingleflightResult>>,
    #[cfg(test)]
    waiters: AtomicUsize,
}

type ReadIndexSingleflightRegistry = BTreeMap<ReadIndexSingleflightKey, Arc<ReadIndexSingleflight>>;

fn read_index_singleflight_registry() -> &'static Mutex<ReadIndexSingleflightRegistry> {
    static REGISTRY: OnceLock<Mutex<ReadIndexSingleflightRegistry>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(BTreeMap::new()))
}

struct ReadIndexSingleflightOwner {
    key: ReadIndexSingleflightKey,
    flight: Arc<ReadIndexSingleflight>,
    completed: bool,
}

impl ReadIndexSingleflightOwner {
    /// Closes this proof wave to new readers immediately before the heartbeat
    /// dispatch that establishes its linearization point. Readers that begin
    /// after this removal must claim a new wave; already-subscribed readers
    /// retain the exact `Arc` and may consume the published proof.
    fn seal(&mut self) {
        self.remove_from_registry();
    }

    fn finish(mut self, result: ReadIndexSingleflightResult) -> ReadIndexSingleflightResult {
        self.completed = true;
        // A terminal flight must be unreachable to new claimants before its
        // result is published. Otherwise a caller whose invocation follows the
        // proof's linearization point could subscribe during publication and
        // incorrectly reuse the older proof.
        self.remove_from_registry();
        self.flight.result.send_replace(Some(result.clone()));
        result
    }

    fn publish_retry(mut self) {
        self.completed = true;
        // Remove the completed flight before waking waiters so the first one
        // to reclaim cannot subscribe to the stale retry result again.
        self.remove_from_registry();
        self.flight
            .result
            .send_replace(Some(Ok(ReadIndexQuorumOutcome::RetryFlight)));
    }

    fn remove_from_registry(&self) {
        let Ok(mut registry) = read_index_singleflight_registry().lock() else {
            return;
        };
        if registry
            .get(&self.key)
            .is_some_and(|flight| Arc::ptr_eq(flight, &self.flight))
        {
            registry.remove(&self.key);
        }
    }
}

impl Drop for ReadIndexSingleflightOwner {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        self.remove_from_registry();
        self.flight
            .result
            .send_replace(Some(Ok(ReadIndexQuorumOutcome::RetryFlight)));
    }
}

struct ReadIndexSingleflightWaiter {
    result: tokio::sync::watch::Receiver<Option<ReadIndexSingleflightResult>>,
    entry_floor: ReadIndexClaimantFloor,
    #[cfg(test)]
    flight: Arc<ReadIndexSingleflight>,
}

#[cfg(test)]
impl Drop for ReadIndexSingleflightWaiter {
    fn drop(&mut self) {
        self.flight.waiters.fetch_sub(1, Ordering::Relaxed);
    }
}

enum ReadIndexSingleflightClaim {
    Owner(ReadIndexSingleflightOwner),
    Waiter(ReadIndexSingleflightWaiter),
}

fn claim_read_index_singleflight(
    key: ReadIndexSingleflightKey,
    entry_floor: ReadIndexClaimantFloor,
) -> Result<ReadIndexSingleflightClaim, NetworkError> {
    let mut registry =
        read_index_singleflight_registry()
            .lock()
            .map_err(|error| NetworkError::Io {
                operation: "claim cluster Raft ReadIndex singleflight".to_string(),
                detail: format!("ReadIndex singleflight registry lock poisoned: {error}"),
            })?;
    if let Some(flight) = registry.get(&key).cloned() {
        #[cfg(test)]
        flight.waiters.fetch_add(1, Ordering::Relaxed);
        let result = flight.result.subscribe();
        return Ok(ReadIndexSingleflightClaim::Waiter(
            ReadIndexSingleflightWaiter {
                result,
                entry_floor,
                #[cfg(test)]
                flight,
            },
        ));
    }

    let (result, _initial_receiver) = tokio::sync::watch::channel(None);
    let flight = Arc::new(ReadIndexSingleflight {
        result,
        #[cfg(test)]
        waiters: AtomicUsize::new(0),
    });
    registry.insert(key.clone(), Arc::clone(&flight));
    Ok(ReadIndexSingleflightClaim::Owner(
        ReadIndexSingleflightOwner {
            key,
            flight,
            completed: false,
        },
    ))
}

async fn wait_for_read_index_singleflight(
    mut waiter: ReadIndexSingleflightWaiter,
    deadline: Instant,
) -> ReadIndexSingleflightResult {
    loop {
        if let Some(result) = waiter.result.borrow().clone() {
            return result;
        }
        let budget = remaining_read_index_budget(deadline, "shared quorum confirmation")?;
        match tokio::time::timeout(budget, waiter.result.changed()).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                return Err(NetworkError::ClusterReadOnly {
                    detail: "Raft ReadIndex quorum confirmation owner ended without a result"
                        .to_string(),
                });
            }
            Err(_) => return Err(read_index_phase_timeout("shared quorum confirmation")),
        }
    }
}

#[cfg(test)]
pub(crate) fn read_index_singleflight_waiters_for_root(root: &std::path::Path) -> usize {
    read_index_singleflight_registry()
        .lock()
        .ok()
        .map(|registry| {
            registry
                .iter()
                .filter(|(key, _)| key.cluster_root == root)
                .map(|(_, flight)| flight.waiters.load(Ordering::Relaxed))
                .sum()
        })
        .unwrap_or_default()
}

#[cfg(test)]
fn read_index_singleflight_flights_for_root(root: &std::path::Path) -> usize {
    read_index_singleflight_registry()
        .lock()
        .ok()
        .map(|registry| {
            registry
                .keys()
                .filter(|key| key.cluster_root == root)
                .count()
        })
        .unwrap_or_default()
}

#[cfg(test)]
fn read_index_resident_preparation_counts() -> &'static Mutex<BTreeMap<PathBuf, usize>> {
    static COUNTS: OnceLock<Mutex<BTreeMap<PathBuf, usize>>> = OnceLock::new();
    COUNTS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

#[cfg(test)]
fn record_read_index_resident_preparation(root: &std::path::Path) {
    if let Ok(mut counts) = read_index_resident_preparation_counts().lock() {
        let count = counts.entry(root.to_path_buf()).or_default();
        *count = count.saturating_add(1);
    }
}

#[cfg(test)]
pub(crate) fn read_index_resident_preparations_for_root(root: &std::path::Path) -> usize {
    read_index_resident_preparation_counts()
        .lock()
        .ok()
        .and_then(|counts| counts.get(root).copied())
        .unwrap_or_default()
}

#[allow(clippy::too_many_arguments)]
async fn confirm_cluster_raft_read_index_quorum<H, S>(
    _runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    deadline: Instant,
    append_options: ClusterRaftAppendReplicationOptions,
    fence: &ClusterRaftReadIndexAuthorityFence,
    read_state: &sinomo_kv::ClusterKvRaftReadIndexState,
    request: Arc<ClusterRaftAppendRequest>,
    seal_before_dispatch: S,
    host: &mut H,
) -> ReadIndexSingleflightResult
where
    H: ClusterRaftQuorumAsyncHost,
    S: FnOnce(),
{
    let mut seal_before_dispatch = Some(seal_before_dispatch);
    let quorum_size = sinomo_kv::cluster_kv_raft_quorum_size(fence.confirmed_voters.len());
    let required_remote_acks = quorum_size.saturating_sub(1);
    let mut acknowledged_voters = BTreeSet::from([state.local_node_id.clone()]);
    let mut voter_acks = 1_usize;
    if required_remote_acks > 0 {
        // The resident ReadIndex fence is already the authoritative committed
        // voter set. Do not route this proof through the endpoint selector:
        // its directory projection is unrelated to quorum membership and can
        // synchronously enter the full KV snapshot/cold-hydration path before
        // the first network request is dispatched. The batch transport owns
        // endpoint/route resolution for these exact node identities.
        let targets = fence
            .confirmed_voters
            .iter()
            .filter(|node_id| *node_id != &state.local_node_id)
            .cloned()
            .collect::<Vec<_>>();
        let mut attempts = Vec::new();
        for target in targets {
            let callback_budget = remaining_read_index_budget(deadline, "probe dispatch")?;
            tokio::time::timeout(callback_budget, host.on_raft_append_sent_async())
                .await
                .map_err(|_| read_index_phase_timeout("probe dispatch"))??;
            let remaining = remaining_read_index_budget(deadline, "quorum probe")?;
            attempts.push((
                target,
                Arc::clone(&request),
                append_options.routed_node_append_attempt_timeout(remaining),
            ));
        }
        // Membership in a ReadIndex proof wave closes at the dispatch edge.
        // A reader invoked after this point must perform a later quorum wave;
        // sharing this in-flight proof would permit a linearization point that
        // predates that reader's invocation.
        if let Some(seal) = seal_before_dispatch.take() {
            seal();
        }
        let network_budget = remaining_read_index_budget(deadline, "quorum probe")?;
        let outcomes = tokio::time::timeout(
            network_budget,
            host.send_raft_appends_to_quorum_async(paths, state, attempts, required_remote_acks),
        )
        .await
        .map_err(|_| read_index_phase_timeout("quorum probe"))??;
        let mut higher_term = None;
        for (target, _request, result) in outcomes {
            match result {
                Ok(response)
                    if response.accepted
                        && response.append.term == read_state.current_term
                        && fence.confirmed_voters.contains(&target)
                        && target != state.local_node_id
                        && acknowledged_voters.insert(target.clone()) =>
                {
                    voter_acks = voter_acks.saturating_add(1);
                    merge_cluster_state_from_peer(state, response.state);
                    host.record_node_command_connectivity(
                        paths,
                        state,
                        &target,
                        ClusterConnectivityState::Healthy,
                        None,
                    );
                }
                Ok(response) => {
                    if response.append.term > read_state.current_term {
                        higher_term = Some(
                            higher_term
                                .unwrap_or(read_state.current_term)
                                .max(response.append.term),
                        );
                    }
                    host.record_node_command_connectivity(
                        paths,
                        state,
                        &target,
                        ClusterConnectivityState::Healthy,
                        None,
                    );
                }
                Err(_) => host.record_node_command_connectivity(
                    paths,
                    state,
                    &target,
                    ClusterConnectivityState::Degraded,
                    None,
                ),
            }
        }
        if let Some(higher_term) = higher_term {
            publish_cluster_raft_higher_term_reactively(paths, &state.local_node_id, higher_term)?;
            // Publication synchronously closes the per-root authority gate;
            // the caller-independent worker owns the durable hard-state
            // update. A failed ReadIndex must not wait behind local admission
            // merely to duplicate that worker's mandatory persistence.
            state.term = state.term.max(higher_term);
            return Ok(ReadIndexQuorumOutcome::HigherTerm {
                observed_term: higher_term,
                confirmed_term: read_state.current_term,
            });
        }
    }

    if voter_acks < quorum_size {
        return Ok(ReadIndexQuorumOutcome::NeedsBarrier(fence.clone()));
    }

    // Single-voter reads have no remote dispatch edge. Close the wave before
    // publishing the local current-term proof for the same invocation-order
    // guarantee as the multi-voter path.
    if let Some(seal) = seal_before_dispatch.take() {
        seal();
    }

    Ok(ReadIndexQuorumOutcome::Confirmed {
        fence: fence.clone(),
        confirmation: ReadIndexQuorumConfirmation {
            voter_acks,
            quorum_size,
            state_after_probe: state.clone(),
        },
    })
}

#[derive(Clone, Debug)]
pub(crate) struct ClusterRaftReadIndexAuthorityFence {
    pub(crate) read_state: sinomo_kv::ClusterKvRaftReadIndexState,
    pub(crate) config_fence: Option<sinomo_kv::KvEntry>,
    pub(crate) pending_current_term_barrier: Option<sinomo_kv::KvLogEntry>,
    pub(crate) confirmed_voters: BTreeSet<NodeId>,
    pub(crate) confirmed_state: ClusterBootstrapState,
    /// Present only for the documented legacy one-node repair case. This node
    /// is never eligible for forwarding and contributes only the local
    /// hard-state-confirmed self-ack.
    unresolved_local_node_id: Option<NodeId>,
}

fn adopt_cluster_raft_read_index_state(
    state: &mut ClusterBootstrapState,
    candidate: &ClusterBootstrapState,
    fence: &ClusterRaftReadIndexAuthorityFence,
) -> Result<(), NetworkError> {
    if candidate.cluster_id != state.cluster_id
        || fence.confirmed_state.cluster_id != state.cluster_id
        || candidate.local_node_id != state.local_node_id
        || fence.confirmed_state.local_node_id != state.local_node_id
    {
        return Err(NetworkError::ClusterReadOnly {
            detail: "shared Raft ReadIndex state belongs to a different cluster or local node"
                .to_string(),
        });
    }

    let minimum_term = state
        .term
        .max(candidate.term)
        .max(fence.read_state.current_term);
    let minimum_revision = state
        .kv_revision
        .max(candidate.kv_revision)
        .max(fence.read_state.revision.0);
    let newest_updated_at = state.updated_at.max(candidate.updated_at);
    let mut adopted = candidate.clone();
    // Preserve fresher endpoint/member observations from the caller, then
    // reapply the exact committed configuration fence so topology merging can
    // never smuggle a proposed/stale role assignment into quorum state.
    merge_cluster_state_from_peer(&mut adopted, state.clone());
    let committed_config = if let Some(entry) = fence.config_fence.as_ref() {
        entry.decode().map_err(kv_err)?
    } else {
        sinomo_kv::ClusterKvRaftConfigRecord::from(&fence.confirmed_state)
    };
    project_read_index_forwarding_config(
        &mut adopted,
        &committed_config,
        fence.unresolved_local_node_id.as_ref(),
    )?;
    if ReadIndexConfigurationFloor::from_state(&adopted)
        != ReadIndexConfigurationFloor::from_fence(fence)?
    {
        return Err(NetworkError::ClusterReadOnly {
            detail:
                "shared Raft ReadIndex membership partition changed during monotonic state adoption"
                    .to_string(),
        });
    }
    let adopted_voters = read_index_voter_ids_from_config_or_state(
        &adopted,
        Some(&committed_config),
        fence.unresolved_local_node_id.as_ref(),
    )?;
    if adopted_voters != fence.confirmed_voters {
        return Err(NetworkError::ClusterReadOnly {
            detail: "shared Raft ReadIndex configuration changed during monotonic state adoption"
                .to_string(),
        });
    }
    adopted.term = minimum_term;
    adopted.kv_revision = minimum_revision;
    adopted.updated_at = newest_updated_at;
    *state = adopted;
    Ok(())
}

#[derive(Clone, Debug)]
enum PreparedClusterRaftReadIndex {
    Confirmable {
        fence: ClusterRaftReadIndexAuthorityFence,
        request: ClusterRaftAppendRequest,
    },
    NeedsBarrier(ClusterRaftReadIndexAuthorityFence),
    NotLeader(ClusterRaftReadIndexAuthorityFence),
    ForwardOnly(ClusterRaftReadIndexForwardOnly),
    LocalAdmissionBusy,
}

/// A committed resident membership view that is newer than the caller's
/// topology generation, but which is not itself a local ReadIndex authority
/// fence because another scalar floor (normally the caller's term) still
/// requires the read to be forwarded.
///
/// Member identity, trust, capability, and role fields come only from the
/// resident committed member/config records. Applying this fence may merge
/// endpoint observations from the individual forwarding caller, but only for
/// node identities admitted by the captured configuration. Keeping endpoint
/// merging at application time also prevents a singleflight owner from
/// replacing a waiter's distinct route observations.
#[derive(Clone, Debug)]
struct ClusterRaftReadIndexForwardingTopology {
    config: sinomo_kv::ClusterKvRaftConfigRecord,
    members: Vec<ClusterMemberRecord>,
    /// The sole local voter can be absent from legacy single-node member KV.
    /// This marker keeps that caller-derived structural placeholder out of
    /// every forwarding/authentication path and reapplies its fail-closed
    /// capability/endpoint stripping after topology merges.
    unresolved_local_node_id: Option<NodeId>,
}

impl ClusterRaftReadIndexForwardingTopology {
    fn apply_to(&self, state: &mut ClusterBootstrapState) -> Result<(), NetworkError> {
        if self.config.cluster_id != state.cluster_id {
            return Err(NetworkError::ClusterReadOnly {
                detail: "forward-only Raft topology belongs to a different cluster".to_string(),
            });
        }

        let mut projected = state.clone();
        projected.members = self.members.clone();
        for member in &mut projected.members {
            let member_node_id = member.node_id.clone();
            member
                .endpoints
                .retain(|endpoint| endpoint.node_id == member_node_id);
            if self.unresolved_local_node_id.as_ref() == Some(&member_node_id) {
                continue;
            }
            if let Some(caller_member) = state
                .members
                .iter()
                .find(|caller_member| caller_member.node_id == member_node_id)
            {
                for endpoint in caller_member
                    .endpoints
                    .iter()
                    .filter(|endpoint| endpoint.node_id == member_node_id)
                    .cloned()
                {
                    upsert_member_endpoint(member, endpoint);
                }
            }
            cleanup_member_endpoint_duplicates(member);
        }
        project_read_index_forwarding_config(
            &mut projected,
            &self.config,
            self.unresolved_local_node_id.as_ref(),
        )?;
        if ReadIndexConfigurationFloor::from_state(&projected)
            != ReadIndexConfigurationFloor::from_config(&self.config)
        {
            return Err(NetworkError::ClusterReadOnly {
                detail: "forward-only Raft topology does not match its committed configuration"
                    .to_string(),
            });
        }
        projected
            .members
            .sort_by(|left, right| left.node_id.cmp(&right.node_id));
        *state = projected;
        Ok(())
    }
}

fn read_cluster_raft_forwarding_topology(
    storage: &mut dyn LocalClusterRaftReadProjection,
    caller_state: &ClusterBootstrapState,
    config: sinomo_kv::ClusterKvRaftConfigRecord,
) -> Result<ClusterRaftReadIndexForwardingTopology, NetworkError> {
    if config.cluster_id != caller_state.cluster_id {
        return Err(NetworkError::ClusterReadOnly {
            detail: "newer resident Raft configuration belongs to a different cluster".to_string(),
        });
    }

    let member_ids = config
        .voters
        .iter()
        .chain(&config.learners)
        .chain(&config.non_voting)
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut projection_keys = Vec::new();
    sinomo_network::try_reserve_vec(
        &mut projection_keys,
        member_ids.len().saturating_mul(2),
        "capture newer resident Raft member and capability-policy keys",
    )?;
    projection_keys.extend(member_ids.iter().map(crate::keyspace::cluster_member_key));
    projection_keys.extend(
        member_ids
            .iter()
            .map(crate::keyspace::cluster_capability_policy_key),
    );
    let entries = storage.read_index_entries(&projection_keys)?;
    if entries.len() != projection_keys.len() {
        return Err(NetworkError::ClusterReadOnly {
            detail: "newer resident Raft member projection returned an incomplete exact-key set"
                .to_string(),
        });
    }
    let (member_entries, policy_entries) = entries.split_at(member_ids.len());

    let mut members = Vec::new();
    let mut unresolved_local_node_id = None;
    sinomo_network::try_reserve_vec(
        &mut members,
        member_ids.len(),
        "capture newer resident Raft members",
    )?;
    for (((node_id, key), entry), policy_entry) in member_ids
        .iter()
        .zip(projection_keys.iter())
        .zip(member_entries)
        .zip(policy_entries)
    {
        let Some(entry) = entry else {
            if !read_index_config_allows_unresolved_local_single_voter(&config, caller_state)
                || node_id != &caller_state.local_node_id
            {
                return Err(NetworkError::ClusterReadOnly {
                    detail: format!(
                        "newer resident Raft configuration names member `{node_id}` without committed identity record `{key}`"
                    ),
                });
            }
            let mut matching = caller_state
                .members
                .iter()
                .filter(|member| member.node_id == *node_id);
            let Some(mut placeholder) = matching.next().cloned() else {
                return Err(NetworkError::ClusterReadOnly {
                    detail: format!(
                        "unresolved local Raft voter `{node_id}` has no caller-local placeholder"
                    ),
                });
            };
            if matching.next().is_some()
                || placeholder.cluster_id != caller_state.cluster_id
                || placeholder.role != ClusterMemberRole::Voter
                || placeholder.state != ClusterMemberState::Voter
            {
                return Err(NetworkError::ClusterReadOnly {
                    detail: format!(
                        "unresolved local Raft voter `{node_id}` lacks one exact authorized caller-local voter row"
                    ),
                });
            }
            sanitize_unresolved_local_read_index_member(&mut placeholder);
            unresolved_local_node_id = Some(node_id.clone());
            members.push(placeholder);
            continue;
        };
        let mut member: ClusterMemberRecord = entry.decode().map_err(kv_err)?;
        if member.cluster_id != caller_state.cluster_id || member.node_id != *node_id {
            return Err(NetworkError::ClusterReadOnly {
                detail: format!(
                    "newer resident Raft member record `{key}` does not match cluster `{}` and node `{node_id}`",
                    caller_state.cluster_id
                ),
            });
        }
        if let Some(policy_entry) = policy_entry {
            let policy: ClusterCapabilityPolicyRecord = policy_entry.decode().map_err(kv_err)?;
            if policy.cluster_id != caller_state.cluster_id || policy.node_id != *node_id {
                return Err(NetworkError::ClusterReadOnly {
                    detail: format!(
                        "newer resident Raft capability policy for `{node_id}` does not match its member identity"
                    ),
                });
            }
            member.capabilities = policy.capabilities;
            member.allowed_capabilities = policy.allowed_capabilities;
        }
        members.push(member);
    }

    let mut resident_state = caller_state.clone();
    resident_state.members = members;
    for member in &mut resident_state.members {
        let member_node_id = member.node_id.clone();
        member
            .endpoints
            .retain(|endpoint| endpoint.node_id == member_node_id);
        cleanup_member_endpoint_duplicates(member);
    }
    project_read_index_forwarding_config(
        &mut resident_state,
        &config,
        unresolved_local_node_id.as_ref(),
    )?;
    if ReadIndexConfigurationFloor::from_state(&resident_state)
        != ReadIndexConfigurationFloor::from_config(&config)
    {
        return Err(NetworkError::ClusterReadOnly {
            detail: "newer resident Raft members do not match their committed configuration"
                .to_string(),
        });
    }
    resident_state
        .members
        .sort_by(|left, right| left.node_id.cmp(&right.node_id));
    Ok(ClusterRaftReadIndexForwardingTopology {
        config,
        members: resident_state.members,
        unresolved_local_node_id,
    })
}

fn read_index_config_allows_unresolved_local_single_voter(
    config: &sinomo_kv::ClusterKvRaftConfigRecord,
    state: &ClusterBootstrapState,
) -> bool {
    config.cluster_id == state.cluster_id
        && config.voters.as_slice() == std::slice::from_ref(&state.local_node_id)
        && config.learners.is_empty()
        && config.non_voting.is_empty()
}

fn sanitize_unresolved_local_read_index_member(member: &mut ClusterMemberRecord) {
    // None of these caller observations is committed identity authority. The
    // placeholder exists only so the exact one-voter config can represent the
    // already-running local process while it repairs its missing KV row.
    member.capabilities.clear();
    member.allowed_capabilities.clear();
    member.endpoints.clear();
}

fn project_read_index_forwarding_config(
    state: &mut ClusterBootstrapState,
    config: &sinomo_kv::ClusterKvRaftConfigRecord,
    unresolved_local_node_id: Option<&NodeId>,
) -> Result<(), NetworkError> {
    let Some(unresolved_local_node_id) = unresolved_local_node_id else {
        return project_cluster_raft_config_record(state, config);
    };
    if unresolved_local_node_id != &state.local_node_id
        || !read_index_config_allows_unresolved_local_single_voter(config, state)
    {
        return Err(NetworkError::ClusterReadOnly {
            detail: "unresolved Raft member is not the exact local single voter".to_string(),
        });
    }
    let mut matching = state
        .members
        .iter_mut()
        .filter(|member| member.node_id == *unresolved_local_node_id);
    let Some(member) = matching.next() else {
        return Err(NetworkError::ClusterReadOnly {
            detail: "unresolved local Raft voter placeholder disappeared".to_string(),
        });
    };
    if matching.next().is_some() || member.cluster_id != state.cluster_id {
        return Err(NetworkError::ClusterReadOnly {
            detail: "unresolved local Raft voter placeholder is ambiguous".to_string(),
        });
    }
    member.role = ClusterMemberRole::Voter;
    member.state = ClusterMemberState::Voter;
    sanitize_unresolved_local_read_index_member(member);
    Ok(())
}

fn read_index_voter_ids_from_config_or_state(
    state: &ClusterBootstrapState,
    config: Option<&sinomo_kv::ClusterKvRaftConfigRecord>,
    unresolved_local_node_id: Option<&NodeId>,
) -> Result<BTreeSet<NodeId>, NetworkError> {
    if let Some(unresolved_local_node_id) = unresolved_local_node_id {
        let Some(config) = config else {
            return Err(NetworkError::ClusterReadOnly {
                detail: "unresolved local Raft voter lacks its committed configuration fence"
                    .to_string(),
            });
        };
        if unresolved_local_node_id != &state.local_node_id
            || !read_index_config_allows_unresolved_local_single_voter(config, state)
        {
            return Err(NetworkError::ClusterReadOnly {
                detail: "unresolved Raft voter set is not the exact local self voter".to_string(),
            });
        }
        return Ok(BTreeSet::from([state.local_node_id.clone()]));
    }
    Ok(sinomo_kv::cluster_kv_raft_voter_ids(state)
        .map_err(kv_err)?
        .into_iter()
        .collect())
}

fn cluster_raft_forwarding_member_semantics_eq(
    left: &ClusterMemberRecord,
    right: &ClusterMemberRecord,
) -> bool {
    ReadIndexMemberFloor::from_member(left) == ReadIndexMemberFloor::from_member(right)
}

fn cluster_raft_caller_matches_forwarding_topology(
    caller_state: &ClusterBootstrapState,
    topology: &ClusterRaftReadIndexForwardingTopology,
) -> bool {
    topology.members.iter().all(|resident_member| {
        let mut matching = caller_state
            .members
            .iter()
            .filter(|caller_member| caller_member.node_id == resident_member.node_id);
        let first_matches = matching.next().is_some_and(|caller_member| {
            topology.unresolved_local_node_id.as_ref() == Some(&resident_member.node_id)
                || cluster_raft_forwarding_member_semantics_eq(caller_member, resident_member)
        });
        first_matches && matching.next().is_none()
    })
}

/// A caller-observed cluster projection that the resident Raft node cannot
/// yet satisfy locally.
///
/// This deliberately carries no local authority fence. A strictly newer
/// resident KV generation may contribute a forwarding-only topology fence,
/// but that topology can select/validate only a remote authoritative read; it
/// can never authorize a local ReadIndex, barrier, or election.
#[derive(Clone, Debug)]
pub(crate) struct ClusterRaftReadIndexLocalStateBehind {
    observed_term: u64,
    observed_revision: u64,
    required_term: u64,
    required_revision: u64,
    claimant_floor: ReadIndexClaimantFloor,
    forwarding_topology: Option<ClusterRaftReadIndexForwardingTopology>,
}

impl ClusterRaftReadIndexLocalStateBehind {
    fn from_projection(
        read_state: &sinomo_kv::ClusterKvRaftReadIndexState,
        caller_state: &ClusterBootstrapState,
        forwarding_topology: Option<ClusterRaftReadIndexForwardingTopology>,
    ) -> Option<Self> {
        (read_state.current_term < caller_state.term
            || read_state.revision.0 < caller_state.kv_revision)
            .then(|| Self {
                observed_term: read_state.current_term,
                observed_revision: read_state.revision.0,
                required_term: caller_state.term,
                required_revision: caller_state.kv_revision,
                claimant_floor: ReadIndexClaimantFloor::from_state(caller_state),
                forwarding_topology,
            })
    }

    fn belongs_to_claimant(&self, claimant: &ReadIndexClaimantFloor) -> bool {
        &self.claimant_floor == claimant
    }

    pub(crate) fn into_error(self) -> NetworkError {
        let reason = match (
            self.observed_term < self.required_term,
            self.observed_revision < self.required_revision,
        ) {
            (true, true) => format!(
                "local Raft term {} trails projected cluster term {} and local Raft revision {} trails caller-observed cluster revision {} during ReadIndex",
                self.observed_term,
                self.required_term,
                self.observed_revision,
                self.required_revision
            ),
            (true, false) => format!(
                "local Raft term {} trails projected cluster term {} during ReadIndex",
                self.observed_term, self.required_term
            ),
            (false, true) => format!(
                "local Raft revision {} trails caller-observed cluster revision {} during ReadIndex",
                self.observed_revision, self.required_revision
            ),
            (false, false) => {
                "local Raft state no longer trails the caller projection during ReadIndex"
                    .to_string()
            }
        };
        let detail = format!(
            "{reason} (observed term {}, revision {}; required term {}, revision {})",
            self.observed_term, self.observed_revision, self.required_term, self.required_revision
        );
        NetworkError::ClusterReadOnly { detail }
    }
}

/// ReadIndex preparation outcomes that may only be satisfied by forwarding.
/// No variant in this enum authorizes a local barrier or election.
#[derive(Clone, Debug)]
pub(crate) enum ClusterRaftReadIndexForwardOnly {
    LocalStateBehind(ClusterRaftReadIndexLocalStateBehind),
}

impl ClusterRaftReadIndexForwardOnly {
    fn belongs_to_claimant(&self, claimant: &ReadIndexClaimantFloor) -> bool {
        match self {
            Self::LocalStateBehind(behind) => behind.belongs_to_claimant(claimant),
        }
    }

    pub(crate) fn into_error(self) -> NetworkError {
        match self {
            Self::LocalStateBehind(behind) => behind.into_error(),
        }
    }

    /// Applies the composed floors used only to select and validate a forwarded
    /// response. A newer resident KV generation contributes its committed
    /// topology; otherwise the caller's topology remains intact. This method is
    /// never used by the local ReadIndex/barrier authority path.
    pub(crate) fn apply_forwarding_floor(
        &self,
        state: &mut ClusterBootstrapState,
    ) -> Result<(), NetworkError> {
        match self {
            Self::LocalStateBehind(behind) => {
                if behind.claimant_floor.cluster_id != state.cluster_id
                    || behind.claimant_floor.local_node_id != state.local_node_id
                {
                    return Err(NetworkError::ClusterReadOnly {
                        detail:
                            "forward-only Raft floor belongs to a different cluster or local node"
                                .to_string(),
                    });
                }
                if let Some(topology) = behind.forwarding_topology.as_ref() {
                    topology.apply_to(state)?;
                }
                state.term = state
                    .term
                    .max(behind.required_term)
                    .max(behind.observed_term);
                state.kv_revision = state
                    .kv_revision
                    .max(behind.required_revision)
                    .max(behind.observed_revision);
            }
        }
        Ok(())
    }
}

/// Capture the exact local term, committed index, and voter configuration used
/// by both synchronous and asynchronous ReadIndex transports. The storage
/// session is deadline-admitted, so a caller that times out cannot leave a
/// queued lock acquisition behind to execute later.
fn prepare_cluster_raft_read_index_from_projection(
    storage: &mut dyn LocalClusterRaftReadProjection,
    mut probe_state: ClusterBootstrapState,
    local_node_id: &NodeId,
    read_state: sinomo_kv::ClusterKvRaftReadIndexState,
) -> Result<PreparedClusterRaftReadIndex, NetworkError> {
    let caller_revision = probe_state.kv_revision;
    // A lower resident KV generation cannot say anything authoritative about
    // the caller's newer configuration. Preserve the caller topology and use
    // only the composed scalar floors when forwarding.
    if read_state.revision.0 < caller_revision {
        let behind =
            ClusterRaftReadIndexLocalStateBehind::from_projection(&read_state, &probe_state, None)
                .expect("lower resident revision must require forwarding");
        return Ok(PreparedClusterRaftReadIndex::ForwardOnly(
            ClusterRaftReadIndexForwardOnly::LocalStateBehind(behind),
        ));
    }

    let config_fence = storage.read_index_entry(&sinomo_kv::cluster_kv_raft_config_key())?;
    let committed_config = config_fence
        .as_ref()
        .map(|entry| entry.decode().map_err(kv_err))
        .transpose()?;
    let forwarding_topology = if read_state.revision.0 > caller_revision {
        let Some(config) = committed_config.clone() else {
            return Err(NetworkError::ClusterReadOnly {
                detail: format!(
                    "newer resident Raft revision {} has no committed membership configuration",
                    read_state.revision.0
                ),
            });
        };
        Some(read_cluster_raft_forwarding_topology(
            storage,
            &probe_state,
            config,
        )?)
    } else {
        if let Some(config) = committed_config.as_ref() {
            if ReadIndexConfigurationFloor::from_config(config)
                != ReadIndexConfigurationFloor::from_state(&probe_state)
            {
                return Err(NetworkError::ClusterReadOnly {
                    detail: format!(
                        "caller and resident Raft configurations disagree at equal KV revision {}",
                        caller_revision
                    ),
                });
            }
            let resident_topology =
                read_cluster_raft_forwarding_topology(storage, &probe_state, config.clone())?;
            if !cluster_raft_caller_matches_forwarding_topology(&probe_state, &resident_topology) {
                return Err(NetworkError::ClusterReadOnly {
                    detail: format!(
                        "caller and resident Raft member identities disagree at equal KV revision {}",
                        caller_revision
                    ),
                });
            }
            if resident_topology.unresolved_local_node_id.is_some() {
                Some(resident_topology)
            } else {
                None
            }
        } else {
            None
        }
    };

    if let Some(behind) = ClusterRaftReadIndexLocalStateBehind::from_projection(
        &read_state,
        &probe_state,
        forwarding_topology.clone(),
    ) {
        if forwarding_topology
            .as_ref()
            .is_some_and(|topology| topology.unresolved_local_node_id.is_some())
        {
            return Err(NetworkError::ClusterReadOnly {
                detail:
                    "unresolved local single-voter identity cannot authorize a forwarded Raft read"
                        .to_string(),
            });
        }
        return Ok(PreparedClusterRaftReadIndex::ForwardOnly(
            ClusterRaftReadIndexForwardOnly::LocalStateBehind(behind),
        ));
    }

    if let Some(topology) = forwarding_topology.as_ref() {
        topology.apply_to(&mut probe_state)?;
    } else if let Some(config) = committed_config.as_ref() {
        project_cluster_raft_config_record(&mut probe_state, config)?;
    }
    let unresolved_local_node_id = forwarding_topology
        .as_ref()
        .and_then(|topology| topology.unresolved_local_node_id.clone());
    let confirmed_voters = read_index_voter_ids_from_config_or_state(
        &probe_state,
        committed_config.as_ref(),
        unresolved_local_node_id.as_ref(),
    )?;
    if let Some(config) = committed_config.as_ref() {
        if ReadIndexConfigurationFloor::from_config(config)
            != ReadIndexConfigurationFloor::from_state(&probe_state)
        {
            return Err(NetworkError::ClusterReadOnly {
                detail: "committed Raft membership partition changed during ReadIndex preparation"
                    .to_string(),
            });
        }
    }
    probe_state.term = probe_state.term.max(read_state.current_term);
    probe_state.kv_revision = probe_state.kv_revision.max(read_state.revision.0);
    let local_is_leader = read_state.leader_id.as_ref() == Some(local_node_id)
        && if unresolved_local_node_id.is_some() {
            confirmed_voters == BTreeSet::from([local_node_id.clone()])
        } else {
            sinomo_kv::cluster_kv_raft_local_is_voter(&probe_state)
        };
    let mut fence = ClusterRaftReadIndexAuthorityFence {
        read_state,
        config_fence,
        pending_current_term_barrier: None,
        confirmed_voters,
        confirmed_state: probe_state,
        unresolved_local_node_id,
    };
    if !local_is_leader {
        if fence.unresolved_local_node_id.is_some() {
            return Err(NetworkError::ClusterReadOnly {
                detail: "unresolved local single-voter identity requires confirmed local Raft leadership"
                    .to_string(),
            });
        }
        return Ok(PreparedClusterRaftReadIndex::NotLeader(fence));
    }
    if !fence.read_state.has_current_term_commit {
        if fence.unresolved_local_node_id.is_some() {
            return Err(NetworkError::ClusterReadOnly {
                detail: "unresolved local single-voter identity requires an existing current-term self-ack"
                    .to_string(),
            });
        }
        fence.pending_current_term_barrier = storage.pending_current_term_barrier()?;
        return Ok(PreparedClusterRaftReadIndex::NeedsBarrier(fence));
    }
    if fence.read_state.applied_index < fence.read_state.commit_index {
        return Err(NetworkError::ClusterReadOnly {
            detail: format!(
                "local Raft apply index {} trails ReadIndex commit {}",
                fence.read_state.applied_index, fence.read_state.commit_index
            ),
        });
    }

    let request = storage.build_raft_read_index_heartbeat(
        local_node_id,
        &fence.confirmed_state,
        fence.read_state.commit_index,
    )?;
    if request.append.prev_log_index != fence.read_state.commit_index
        || Some(request.append.prev_log_term) != fence.read_state.commit_term
    {
        return Err(NetworkError::ClusterReadOnly {
            detail: format!(
                "ReadIndex anchor at term {} index {} is not retained by the local Raft log",
                fence.read_state.current_term, fence.read_state.commit_index
            ),
        });
    }
    Ok(PreparedClusterRaftReadIndex::Confirmable { fence, request })
}

fn prepare_cluster_raft_read_index(
    paths: &ClusterPaths,
    probe_state: ClusterBootstrapState,
    deadline: Instant,
    admission_mode: ClusterRaftReadIndexAdmissionMode,
) -> Result<PreparedClusterRaftReadIndex, NetworkError> {
    remaining_read_index_budget(deadline, "resident preparation")?;
    #[cfg(test)]
    record_read_index_resident_preparation(&paths.root);
    let local_node_id = probe_state.local_node_id.clone();
    let wal_paths = paths.kv_paths().raft_wal_paths(&local_node_id);
    if admission_mode == ClusterRaftReadIndexAdmissionMode::WaitForLocal {
        if let Some(projected) = sinomo_kv::with_cluster_kv_resident_read_projection(
            &wal_paths,
            &local_node_id,
            |storage| {
                let read_state =
                    sinomo_kv::ClusterKvRaftResidentReadProjection::read_index_state(storage);
                prepare_cluster_raft_read_index_from_projection(
                    storage,
                    probe_state.clone(),
                    &local_node_id,
                    read_state,
                )
            },
        ) {
            return projected.map_err(kv_err)?.value;
        }
    }
    let mut storage = match admission_mode {
        ClusterRaftReadIndexAdmissionMode::WaitForLocal => {
            LocalClusterRaftCommandStorage::new_read_index_with_operation_deadline(
                paths,
                local_node_id.clone(),
                deadline,
            )
        }
        ClusterRaftReadIndexAdmissionMode::ForwardOnBusy => {
            LocalClusterRaftCommandStorage::new_read_index_with_immediate_operation_admission(
                paths,
                local_node_id.clone(),
            )
        }
    };
    let read_state = match admission_mode {
        ClusterRaftReadIndexAdmissionMode::WaitForLocal => storage.read_index_state()?,
        ClusterRaftReadIndexAdmissionMode::ForwardOnBusy => {
            match storage.try_admit_read_index_state()? {
                LocalClusterRaftReadIndexAdmission::Admitted(state) => state,
                LocalClusterRaftReadIndexAdmission::Busy => {
                    metrics::counter!(
                        "sinomo.kv.authoritative_read.local_admission",
                        "outcome" => "busy_forward"
                    )
                    .increment(1);
                    return Ok(PreparedClusterRaftReadIndex::LocalAdmissionBusy);
                }
            }
        }
    };
    prepare_cluster_raft_read_index_from_projection(
        &mut storage,
        probe_state,
        &local_node_id,
        read_state,
    )
}

/// Deadline behavior for the projection admitted after a quorum-confirmed
/// ReadIndex.
///
/// Indexed point/range projections are bounded work: they must still be
/// inside the caller's deadline after the closure completes. A full snapshot
/// is proportional to the complete keyspace and cannot be interrupted while
/// it owns the resident node. Once that work is admitted, await it and return
/// the coherent result instead of timing out the caller while a detached
/// blocking task continues to hold resident ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClusterRaftReadProjectionPolicy {
    DeadlineBounded,
    CompleteAdmittedSnapshot,
}

fn finish_admitted_read_projection_deadline(
    deadline: Instant,
    policy: ClusterRaftReadProjectionPolicy,
    path: &'static str,
) -> Result<(), NetworkError> {
    match policy {
        ClusterRaftReadProjectionPolicy::DeadlineBounded => {
            remaining_read_index_budget(deadline, "final applied read projection")?;
        }
        ClusterRaftReadProjectionPolicy::CompleteAdmittedSnapshot => {
            let overrun = Instant::now().saturating_duration_since(deadline);
            if !overrun.is_zero() {
                metrics::counter!(
                    "sinomo.kv.authoritative_read.projection_deadline_overrun",
                    "path" => path,
                    "projection" => "full_snapshot"
                )
                .increment(1);
                metrics::histogram!(
                    "sinomo.kv.authoritative_read.projection_deadline_overrun.us",
                    "path" => path,
                    "projection" => "full_snapshot"
                )
                .record(overrun.as_micros().min(u128::from(u64::MAX)) as f64);
            }
        }
    }
    Ok(())
}

/// Project the value only after the quorum round trip, from a resident view
/// that still has the exact leader term and committed voter configuration
/// captured during preparation.
fn finish_cluster_raft_read_index<T, F>(
    paths: &ClusterPaths,
    mut state_for_config: ClusterBootstrapState,
    read_state: &sinomo_kv::ClusterKvRaftReadIndexState,
    config_fence: Option<sinomo_kv::KvEntry>,
    confirmed_voters: BTreeSet<NodeId>,
    unresolved_local_node_id: Option<NodeId>,
    deadline: Instant,
    projection_policy: ClusterRaftReadProjectionPolicy,
    read_value: F,
) -> Result<(sinomo_kv::ClusterKvRaftReadIndexState, T), NetworkError>
where
    F: FnOnce(&mut dyn LocalClusterRaftReadProjection) -> Result<T, NetworkError>,
{
    remaining_read_index_budget(deadline, "final applied read")?;
    let local_node_id = state_for_config.local_node_id.clone();
    let projected_local_node_id = local_node_id.clone();
    let project = move |storage: &mut dyn LocalClusterRaftReadProjection| {
        let applied_state = storage.read_index_state()?;
        if applied_state.current_term != read_state.current_term
            || applied_state.leader_id.as_ref() != Some(&projected_local_node_id)
        {
            return Err(NetworkError::ClusterReadOnly {
                detail: format!(
                    "Raft leadership changed while confirming ReadIndex term {}",
                    read_state.current_term
                ),
            });
        }
        if applied_state.applied_index > applied_state.commit_index {
            return Err(NetworkError::ClusterReadOnly {
                detail: format!(
                    "local Raft apply index {} exceeds committed index {} during ReadIndex",
                    applied_state.applied_index, applied_state.commit_index
                ),
            });
        }
        if applied_state.applied_index < read_state.commit_index {
            return Err(NetworkError::ClusterReadOnly {
                detail: format!(
                    "local Raft apply index {} trails confirmed ReadIndex {}",
                    applied_state.applied_index, read_state.commit_index
                ),
            });
        }
        let applied_config = storage.read_index_entry(&sinomo_kv::cluster_kv_raft_config_key())?;
        if applied_config != config_fence {
            return Err(NetworkError::ClusterReadOnly {
                detail:
                    "committed Raft configuration changed while confirming ReadIndex; retry the read"
                        .to_string(),
            });
        }
        let applied_config_record = applied_config
            .as_ref()
            .map(|entry| entry.decode().map_err(kv_err))
            .transpose()?;
        if let Some(config) = applied_config_record.as_ref() {
            project_read_index_forwarding_config(
                &mut state_for_config,
                config,
                unresolved_local_node_id.as_ref(),
            )?;
        }
        let applied_voters = read_index_voter_ids_from_config_or_state(
            &state_for_config,
            applied_config_record.as_ref(),
            unresolved_local_node_id.as_ref(),
        )?;
        let local_is_voter = if unresolved_local_node_id.is_some() {
            applied_voters == BTreeSet::from([projected_local_node_id.clone()])
        } else {
            sinomo_kv::cluster_kv_raft_local_is_voter(&state_for_config)
        };
        if applied_voters != confirmed_voters || !local_is_voter {
            return Err(NetworkError::ClusterReadOnly {
                detail: "local node or voter configuration changed while confirming ReadIndex; retry the read"
                    .to_string(),
            });
        }
        remaining_read_index_budget(deadline, "final applied read projection admission")?;
        let value = read_value(storage);
        finish_admitted_read_projection_deadline(deadline, projection_policy, "read_index")?;
        Ok((applied_state, value?))
    };

    // The quorum-confirmed value comes from the canonical resident node while
    // its slot is read-borrowed. Bounded readers therefore share one immutable
    // view instead of racing for exclusive command-storage checkout. A full
    // compatibility snapshot uses the same coherent borrow, but its caller runs
    // this function in the supervised blocking lane because materialization is
    // proportional to the complete keyspace.
    let wal_paths = paths.kv_paths().raft_wal_paths(&local_node_id);
    let projected = sinomo_kv::with_cluster_kv_resident_read_projection(
        &wal_paths,
        &local_node_id,
        |storage| project(storage),
    )
    .ok_or_else(|| NetworkError::ClusterReadOnly {
        detail: "resident Raft view changed after ReadIndex quorum; retry the read".to_string(),
    })?
    .map_err(kv_err)?;
    projected.value
}

/// Synchronous transport counterpart of the async ReadIndex path. Only the
/// heartbeat delivery is supplied by the caller; all resident-state fencing,
/// higher-term persistence, and post-quorum projection stay shared here.
pub(crate) fn read_cluster_raft_with_read_index_attempt_with_transport<T, F, S>(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    deadline: Instant,
    append_options: ClusterRaftAppendReplicationOptions,
    projection_policy: ClusterRaftReadProjectionPolicy,
    read_value: F,
    mut send_probe: S,
) -> Result<ClusterRaftReadIndexAttempt<T>, NetworkError>
where
    F: FnOnce(&mut dyn LocalClusterRaftReadProjection) -> Result<T, NetworkError>,
    S: FnMut(
        &ClusterBootstrapState,
        Vec<NodeId>,
        &ClusterRaftAppendRequest,
        Duration,
        usize,
    ) -> Result<
        Vec<(NodeId, Result<ClusterRaftAppendResponse, NetworkError>)>,
        NetworkError,
    >,
{
    let prepared = prepare_cluster_raft_read_index(
        paths,
        state.clone(),
        deadline,
        ClusterRaftReadIndexAdmissionMode::WaitForLocal,
    )?;
    let (fence, request) = match prepared {
        PreparedClusterRaftReadIndex::Confirmable { fence, request } => (fence, request),
        PreparedClusterRaftReadIndex::NeedsBarrier(fence) => {
            adopt_cluster_raft_read_index_state(state, &fence.confirmed_state, &fence)?;
            return Ok(ClusterRaftReadIndexAttempt::NeedsBarrier(fence));
        }
        PreparedClusterRaftReadIndex::NotLeader(fence) => {
            adopt_cluster_raft_read_index_state(state, &fence.confirmed_state, &fence)?;
            return Ok(ClusterRaftReadIndexAttempt::NotLeader);
        }
        PreparedClusterRaftReadIndex::ForwardOnly(reason) => {
            return Ok(ClusterRaftReadIndexAttempt::ForwardOnly(reason));
        }
        PreparedClusterRaftReadIndex::LocalAdmissionBusy => {
            return Ok(ClusterRaftReadIndexAttempt::LocalAdmissionBusy);
        }
    };
    adopt_cluster_raft_read_index_state(state, &fence.confirmed_state, &fence)?;
    let retry_fence = fence.clone();
    let ClusterRaftReadIndexAuthorityFence {
        read_state,
        config_fence,
        pending_current_term_barrier: _,
        confirmed_voters,
        confirmed_state,
        unresolved_local_node_id,
    } = fence;
    let quorum_size = sinomo_kv::cluster_kv_raft_quorum_size(confirmed_voters.len());
    let required_remote_acks = quorum_size.saturating_sub(1);
    let mut acknowledged_voters = BTreeSet::from([state.local_node_id.clone()]);
    let mut voter_acks = 1_usize;
    let mut higher_term = None;
    let targets = confirmed_voters
        .iter()
        .filter(|node_id| *node_id != &state.local_node_id)
        .cloned()
        .collect::<Vec<_>>();
    let remaining = remaining_read_index_budget(deadline, "synchronous quorum probe")?;
    let timeout = append_options.routed_node_append_attempt_timeout(remaining);
    let outcomes = send_probe(state, targets, &request, timeout, required_remote_acks)?;
    for (target, outcome) in outcomes {
        match outcome {
            Ok(response)
                if response.accepted
                    && response.append.term == read_state.current_term
                    && confirmed_voters.contains(&target)
                    && target != state.local_node_id
                    && acknowledged_voters.insert(target) =>
            {
                voter_acks = voter_acks.saturating_add(1);
                merge_cluster_state_from_peer(state, response.state);
            }
            Ok(response) => {
                if response.append.term > read_state.current_term {
                    higher_term = Some(
                        higher_term
                            .unwrap_or(read_state.current_term)
                            .max(response.append.term),
                    );
                }
            }
            Err(_) => {}
        }
    }
    if let Some(higher_term) = higher_term {
        // Raise the authority fence synchronously and let the caller-independent
        // worker persist it. Waiting inline here can consume the complete Raft
        // node-lock interval even though this read is already being rejected.
        publish_cluster_raft_higher_term_reactively(paths, &state.local_node_id, higher_term)?;
        state.term = state.term.max(higher_term);
        return Err(NetworkError::ClusterReadOnly {
            detail: format!(
                "Raft ReadIndex observed higher term {higher_term} while confirming term {}",
                read_state.current_term
            ),
        });
    }
    if voter_acks < quorum_size {
        return Ok(ClusterRaftReadIndexAttempt::NeedsBarrier(retry_fence));
    }

    let (applied_state, value) = finish_cluster_raft_read_index(
        paths,
        confirmed_state,
        &read_state,
        config_fence,
        confirmed_voters,
        unresolved_local_node_id,
        deadline,
        projection_policy,
        read_value,
    )?;
    state.term = state.term.max(applied_state.current_term);
    state.kv_revision = state.kv_revision.max(applied_state.revision.0);
    metrics::counter!("sinomo.kv.authoritative_read", "path" => "read_index").increment(1);
    Ok(ClusterRaftReadIndexAttempt::Confirmed(
        sinomo_kv::KvAuthoritativeRead {
            value,
            authority: sinomo_kv::KvAuthorityProof {
                term: read_state.current_term,
                commit_index: applied_state.applied_index,
                revision: applied_state.revision,
                basis: sinomo_kv::KvAuthorityBasis::CurrentTermQuorum {
                    voter_acks,
                    required_quorum: quorum_size,
                },
            },
        },
    ))
}

pub(crate) enum ClusterRaftReadIndexAttempt<T> {
    Confirmed(sinomo_kv::KvAuthoritativeRead<T>),
    NeedsBarrier(ClusterRaftReadIndexAuthorityFence),
    NotLeader,
    ForwardOnly(ClusterRaftReadIndexForwardOnly),
    LocalAdmissionBusy,
}

/// Runs one coalesced authority proof before opening resident Raft storage.
///
/// The flight is keyed by cluster root, local node, and admission mode so a
/// concurrent DFS validation herd performs one coherent
/// config/leader/ReadIndex preparation without making forwarding-enabled reads
/// inherit a wait-mode caller's local queue admission.
/// Membership is sealed at the heartbeat dispatch edge; callers invoked after
/// that edge establish a later proof wave. Every confirmed reader still opens
/// its own final resident view and revalidates the exact term, config entry,
/// voter set, and applied index before projecting its value.
pub(crate) async fn read_cluster_raft_with_read_index_attempt_async_with_host_and_admission<
    H,
    T,
    F,
>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    timeout: Duration,
    append_options: ClusterRaftAppendReplicationOptions,
    admission_mode: ClusterRaftReadIndexAdmissionMode,
    read_value: F,
    host: &mut H,
) -> Result<ClusterRaftReadIndexAttempt<T>, NetworkError>
where
    H: ClusterRaftQuorumAsyncHost,
    T: Send + 'static,
    F: FnOnce(&mut dyn LocalClusterRaftReadProjection) -> Result<T, NetworkError> + Send + 'static,
{
    read_cluster_raft_with_read_index_attempt_async_with_projection_policy_and_admission(
        runtime,
        paths,
        state,
        timeout,
        append_options,
        ClusterRaftReadProjectionPolicy::DeadlineBounded,
        admission_mode,
        read_value,
        host,
    )
    .await
}

pub(crate) async fn read_cluster_raft_with_read_index_attempt_async_with_projection_policy<
    H,
    T,
    F,
>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    timeout: Duration,
    append_options: ClusterRaftAppendReplicationOptions,
    projection_policy: ClusterRaftReadProjectionPolicy,
    read_value: F,
    host: &mut H,
) -> Result<ClusterRaftReadIndexAttempt<T>, NetworkError>
where
    H: ClusterRaftQuorumAsyncHost,
    T: Send + 'static,
    F: FnOnce(&mut dyn LocalClusterRaftReadProjection) -> Result<T, NetworkError> + Send + 'static,
{
    read_cluster_raft_with_read_index_attempt_async_with_projection_policy_and_admission(
        runtime,
        paths,
        state,
        timeout,
        append_options,
        projection_policy,
        ClusterRaftReadIndexAdmissionMode::WaitForLocal,
        read_value,
        host,
    )
    .await
}

pub(crate) async fn read_cluster_raft_with_read_index_attempt_async_with_projection_policy_and_admission<
    H,
    T,
    F,
>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    timeout: Duration,
    append_options: ClusterRaftAppendReplicationOptions,
    projection_policy: ClusterRaftReadProjectionPolicy,
    admission_mode: ClusterRaftReadIndexAdmissionMode,
    read_value: F,
    host: &mut H,
) -> Result<ClusterRaftReadIndexAttempt<T>, NetworkError>
where
    H: ClusterRaftQuorumAsyncHost,
    T: Send + 'static,
    F: FnOnce(&mut dyn LocalClusterRaftReadProjection) -> Result<T, NetworkError> + Send + 'static,
{
    let deadline = Instant::now() + timeout;
    let singleflight_key =
        ReadIndexSingleflightKey::new(paths, state.local_node_id.clone(), admission_mode);
    let entry_floor = ReadIndexClaimantFloor::from_state(state);
    let quorum_outcome = loop {
        // A cancelled or shorter-budget owner publishes a retry notification
        // after removing its flight. The longest-lived pre-dispatch waiter then
        // wins the replacement proof wave with its own remaining deadline,
        // matching etcd's notifier-loop ownership.
        remaining_read_index_budget(deadline, "singleflight claim")?;
        match claim_read_index_singleflight(singleflight_key.clone(), entry_floor.clone())? {
            ReadIndexSingleflightClaim::Owner(mut owner) => {
                metrics::counter!(
                    "sinomo.kv.authoritative_read.read_index_singleflight",
                    "role" => "owner"
                )
                .increment(1);
                let prepared = match admission_mode {
                    ClusterRaftReadIndexAdmissionMode::ForwardOnBusy => {
                        // This mode performs only try-acquires against an
                        // already-resident node and then reads bounded in-memory
                        // Raft metadata. Run it on the async worker so a saturated
                        // blocking pool cannot consume the authority budget before
                        // we discover local contention and forward.
                        prepare_cluster_raft_read_index(
                            paths,
                            state.clone(),
                            deadline,
                            admission_mode,
                        )
                    }
                    ClusterRaftReadIndexAdmissionMode::WaitForLocal => {
                        let paths_for_probe = paths.clone();
                        let state_for_probe = state.clone();
                        run_read_index_blocking_before_deadline(
                            runtime,
                            deadline,
                            "resident preparation",
                            "prepare cluster Raft ReadIndex",
                            move || {
                                Ok(prepare_cluster_raft_read_index(
                                    &paths_for_probe,
                                    state_for_probe,
                                    deadline,
                                    admission_mode,
                                ))
                            },
                        )
                        .await?
                    }
                };
                let result = match prepared {
                    Ok(PreparedClusterRaftReadIndex::Confirmable { fence, request }) => {
                        // Use the configuration captured under the resident
                        // operation fence for quorum calculation and target
                        // selection. The caller's earlier state can race a
                        // committed membership transition.
                        adopt_cluster_raft_read_index_state(state, &fence.confirmed_state, &fence)?;
                        confirm_cluster_raft_read_index_quorum(
                            runtime,
                            paths,
                            state,
                            deadline,
                            append_options,
                            &fence,
                            &fence.read_state,
                            Arc::new(request),
                            || owner.seal(),
                            host,
                        )
                        .await
                    }
                    Ok(PreparedClusterRaftReadIndex::NeedsBarrier(fence)) => {
                        adopt_cluster_raft_read_index_state(state, &fence.confirmed_state, &fence)?;
                        Ok(ReadIndexQuorumOutcome::NeedsBarrier(fence))
                    }
                    Ok(PreparedClusterRaftReadIndex::NotLeader(fence)) => {
                        adopt_cluster_raft_read_index_state(state, &fence.confirmed_state, &fence)?;
                        Ok(ReadIndexQuorumOutcome::NotLeader(fence))
                    }
                    Ok(PreparedClusterRaftReadIndex::ForwardOnly(reason)) => {
                        Ok(ReadIndexQuorumOutcome::ForwardOnly(reason))
                    }
                    Ok(PreparedClusterRaftReadIndex::LocalAdmissionBusy) => {
                        Ok(ReadIndexQuorumOutcome::LocalAdmissionBusy)
                    }
                    Err(error) => Err(error),
                };
                match result {
                    Err(error) => {
                        // A preparation error has no claimant-bound authority
                        // fence. In particular, equal-generation topology and
                        // identity validation can reject only the owner's
                        // caller projection. Publishing that raw error would
                        // let one invalid caller poison otherwise valid
                        // waiters sharing this coarse root/node flight.
                        owner.publish_retry();
                        return Err(error);
                    }
                    result => break owner.finish(result)?,
                }
            }
            ReadIndexSingleflightClaim::Waiter(waiter) => {
                metrics::counter!(
                    "sinomo.kv.authoritative_read.read_index_singleflight",
                    "role" => "waiter"
                )
                .increment(1);
                let entry_floor = waiter.entry_floor.clone();
                match wait_for_read_index_singleflight(waiter, deadline).await? {
                    ReadIndexQuorumOutcome::RetryFlight => continue,
                    outcome => {
                        if let ReadIndexQuorumOutcome::ForwardOnly(reason) = &outcome
                            && !reason.belongs_to_claimant(&entry_floor)
                        {
                            metrics::counter!(
                                "sinomo.kv.authoritative_read.read_index_singleflight",
                                "role" => "waiter_rejected",
                                "reason" => "caller_projection"
                            )
                            .increment(1);
                            continue;
                        }
                        if let Some(fence) = outcome.authority_fence()
                            && let Some(reason) = entry_floor.shared_fence_mismatch(fence)?
                        {
                            metrics::counter!(
                                "sinomo.kv.authoritative_read.read_index_singleflight",
                                "role" => "waiter_rejected",
                                "reason" => reason
                            )
                            .increment(1);
                            continue;
                        }
                        break outcome;
                    }
                }
            }
        }
    };
    let (fence, voter_acks, quorum_size) = match quorum_outcome {
        ReadIndexQuorumOutcome::Confirmed {
            fence,
            confirmation,
        } => {
            adopt_cluster_raft_read_index_state(state, &confirmation.state_after_probe, &fence)?;
            (fence, confirmation.voter_acks, confirmation.quorum_size)
        }
        ReadIndexQuorumOutcome::NeedsBarrier(fence) => {
            adopt_cluster_raft_read_index_state(state, &fence.confirmed_state, &fence)?;
            return Ok(ClusterRaftReadIndexAttempt::NeedsBarrier(fence));
        }
        ReadIndexQuorumOutcome::NotLeader(fence) => {
            adopt_cluster_raft_read_index_state(state, &fence.confirmed_state, &fence)?;
            return Ok(ClusterRaftReadIndexAttempt::NotLeader);
        }
        ReadIndexQuorumOutcome::ForwardOnly(reason) => {
            return Ok(ClusterRaftReadIndexAttempt::ForwardOnly(reason));
        }
        ReadIndexQuorumOutcome::LocalAdmissionBusy => {
            return Ok(ClusterRaftReadIndexAttempt::LocalAdmissionBusy);
        }
        ReadIndexQuorumOutcome::RetryFlight => unreachable!("retry outcomes are reclaimed above"),
        ReadIndexQuorumOutcome::HigherTerm {
            observed_term,
            confirmed_term,
        } => {
            state.term = state.term.max(observed_term);
            return Err(NetworkError::ClusterReadOnly {
                detail: format!(
                    "Raft ReadIndex observed higher term {observed_term} while confirming term {confirmed_term}"
                ),
            });
        }
    };
    let ClusterRaftReadIndexAuthorityFence {
        read_state,
        config_fence,
        pending_current_term_barrier: _,
        confirmed_voters,
        confirmed_state,
        unresolved_local_node_id,
    } = fence;

    // As in etcd, confirmation establishes a read index; it does not bless a
    // value captured before the quorum round trip. Reopen the warm resident
    // node, reject a leader/term change, wait for the confirmed index to be
    // locally applied, and only then project the requested view.
    let paths_for_read = paths.clone();
    let state_for_read = confirmed_state;
    let read_state_for_read = read_state.clone();
    let config_fence_for_read = config_fence;
    let confirmed_voters_for_read = confirmed_voters;
    let unresolved_local_node_id_for_read = unresolved_local_node_id;
    remaining_read_index_budget(deadline, "final applied read")?;
    let finish_read = move || {
        finish_cluster_raft_read_index(
            &paths_for_read,
            state_for_read,
            &read_state_for_read,
            config_fence_for_read,
            confirmed_voters_for_read,
            unresolved_local_node_id_for_read,
            deadline,
            projection_policy,
            read_value,
        )
    };
    let (applied_state, value) = match projection_policy {
        // Exact and bounded-prefix projections execute directly against the hot
        // resident view. They neither need Tokio's blocking pool nor wait for a
        // competing Raft operation after quorum has already answered.
        ClusterRaftReadProjectionPolicy::DeadlineBounded => finish_read()?,
        // A full snapshot can be proportional to the complete keyspace. Keep
        // its admitted ownership supervised and return one coherent result.
        ClusterRaftReadProjectionPolicy::CompleteAdmittedSnapshot => {
            run_cluster_raft_supervised_blocking(
                runtime,
                Some((
                    deadline,
                    read_index_phase_timeout("final applied read projection"),
                    ClusterRaftBlockingDeadlinePolicy::CompleteAdmitted,
                )),
                ClusterRaftBlockingCancellationPolicy::CancelQueued,
                "read_index",
                "final applied read projection",
                "apply cluster Raft ReadIndex",
                finish_read,
            )
            .await?
        }
    };
    state.term = state.term.max(applied_state.current_term);
    state.kv_revision = state.kv_revision.max(applied_state.revision.0);
    metrics::counter!("sinomo.kv.authoritative_read", "path" => "read_index").increment(1);
    Ok(ClusterRaftReadIndexAttempt::Confirmed(
        sinomo_kv::KvAuthoritativeRead {
            value,
            authority: sinomo_kv::KvAuthorityProof {
                term: read_state.current_term,
                // The quorum probe establishes `confirmed_commit_index` as the
                // linearization lower bound. The final coherent resident view may
                // include later entries committed by the unchanged leader. Its
                // applied index is the matching upper bound for the returned
                // revision/value and for request-outcome validation.
                commit_index: applied_state.applied_index,
                revision: applied_state.revision,
                basis: sinomo_kv::KvAuthorityBasis::CurrentTermQuorum {
                    voter_acks,
                    required_quorum: quorum_size,
                },
            },
        },
    ))
}

#[cfg(test)]
pub(crate) async fn read_cluster_raft_with_read_index_async_with_host<H, T, F>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    timeout: Duration,
    append_options: ClusterRaftAppendReplicationOptions,
    read_value: F,
    host: &mut H,
) -> Result<Option<sinomo_kv::KvAuthoritativeRead<T>>, NetworkError>
where
    H: ClusterRaftQuorumAsyncHost,
    T: Send + 'static,
    F: FnOnce(&mut dyn LocalClusterRaftReadProjection) -> Result<T, NetworkError> + Send + 'static,
{
    read_cluster_raft_with_read_index_async_with_projection_policy(
        runtime,
        paths,
        state,
        timeout,
        append_options,
        ClusterRaftReadProjectionPolicy::DeadlineBounded,
        read_value,
        host,
    )
    .await
}

pub(crate) async fn read_cluster_raft_with_read_index_async_with_projection_policy<H, T, F>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    timeout: Duration,
    append_options: ClusterRaftAppendReplicationOptions,
    projection_policy: ClusterRaftReadProjectionPolicy,
    read_value: F,
    host: &mut H,
) -> Result<Option<sinomo_kv::KvAuthoritativeRead<T>>, NetworkError>
where
    H: ClusterRaftQuorumAsyncHost,
    T: Send + 'static,
    F: FnOnce(&mut dyn LocalClusterRaftReadProjection) -> Result<T, NetworkError> + Send + 'static,
{
    match read_cluster_raft_with_read_index_attempt_async_with_projection_policy(
        runtime,
        paths,
        state,
        timeout,
        append_options,
        projection_policy,
        read_value,
        host,
    )
    .await?
    {
        ClusterRaftReadIndexAttempt::Confirmed(read) => Ok(Some(read)),
        ClusterRaftReadIndexAttempt::NeedsBarrier(_) => Ok(None),
        ClusterRaftReadIndexAttempt::NotLeader => Err(NetworkError::ClusterReadOnly {
            detail: "local node lost verified Raft leadership before ReadIndex preparation"
                .to_string(),
        }),
        ClusterRaftReadIndexAttempt::ForwardOnly(reason) => Err(reason.into_error()),
        ClusterRaftReadIndexAttempt::LocalAdmissionBusy => Err(NetworkError::ClusterReadOnly {
            detail: "local Raft admission was busy before ReadIndex preparation".to_string(),
        }),
    }
}

#[cfg(test)]
mod read_index_tests {
    use super::*;
    use sinomo_base::{CapabilityName, ClusterMemberRecord};
    use sinomo_kv::{
        ClusterKvRaftConfigRecord, ClusterKvRequestOutcomeRead, KvCommand, KvOp, KvRequestOutcome,
        encode_kv_value, validate_cluster_kv_request_outcome_read,
    };
    use sinomo_node::DAEMON_MAIN_RUN_CAPABILITY;
    use time::OffsetDateTime;

    const TEST_KEY: &str = "/read-index/value";

    fn expired_supervised_test_timeout() -> NetworkError {
        NetworkError::timeout(
            "test supervised blocking deadline",
            sinomo_network::NetworkTimeoutPhase::Overall,
            Duration::from_millis(1),
            sinomo_network::NetworkDeliveryState::DefinitelyNotSent,
        )
    }

    fn initialize_higher_term_worker_fixture(
        name: &str,
    ) -> (sinomo_testdir::TestDir, ClusterPaths, NodeId) {
        let root = sinomo_testdir::TestDir::new(name);
        let data_dir = sinomo_local::SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let node_id = NodeId::from_legacy_named(name).expect("node id");
        LocalClusterRaftCommandStorage::new(&paths, node_id.clone())
            .observe_term(1)
            .expect("initialize durable Raft term");
        (root, paths, node_id)
    }

    fn wait_for_higher_term_persistence(
        paths: &ClusterPaths,
        node_id: &NodeId,
        expected_term: u64,
    ) {
        let store = paths.kv_paths().raft_store();
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let pending_term = store
                .pending_higher_term(node_id)
                .expect("pending higher term");
            let durable_term = store
                .wal_paths(node_id)
                .read_persisted_hard_state()
                .expect("durable hard state")
                .current_term;
            if pending_term.is_none() && durable_term >= expected_term {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "higher term {expected_term} did not become durable: pending={pending_term:?}, durable={durable_term}"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn wait_for_higher_term_worker_idle(worker: &ClusterRaftHigherTermPersistenceWorker) {
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let wake_state = worker
                .wake_state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !wake_state.starting && !wake_state.running && wake_state.target_term.is_none() {
                return;
            }
            drop(wake_state);
            assert!(
                Instant::now() < deadline,
                "higher-term persistence worker did not become idle"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn reactive_higher_term_publisher_joins_failed_start_and_starts_successor() {
        let (_root, paths, node_id) =
            initialize_higher_term_worker_fixture("higher_term_join_failed_start");
        let store = paths.kv_paths().raft_store();
        let wal_root = store.wal_paths(&node_id).root;
        let worker = cluster_raft_higher_term_persistence_worker(wal_root.clone());
        let start_entered = Arc::new(std::sync::Barrier::new(2));
        let release_start = Arc::new(std::sync::Barrier::new(2));
        CLUSTER_RAFT_HIGHER_TERM_INJECTED_START_FAILURES
            .get_or_init(|| Mutex::new(BTreeMap::new()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                wal_root,
                ClusterRaftHigherTermInjectedStartFailure {
                    entered: Arc::clone(&start_entered),
                    release: Arc::clone(&release_start),
                },
            );

        let first_paths = paths.clone();
        let first_node = node_id.clone();
        let (first_tx, first_rx) = std::sync::mpsc::sync_channel(1);
        let first = std::thread::spawn(move || {
            first_tx
                .send(publish_cluster_raft_higher_term_reactively(
                    &first_paths,
                    &first_node,
                    7,
                ))
                .expect("report first publisher result");
        });
        start_entered.wait();

        let second_paths = paths.clone();
        let second_node = node_id.clone();
        let (second_tx, second_rx) = std::sync::mpsc::sync_channel(1);
        let second = std::thread::spawn(move || {
            second_tx
                .send(publish_cluster_raft_higher_term_reactively(
                    &second_paths,
                    &second_node,
                    9,
                ))
                .expect("report second publisher result");
        });

        let wait_deadline = Instant::now() + Duration::from_secs(1);
        let second_joined_start = loop {
            let wake_state = worker
                .wake_state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let joined = wake_state.revision == 2
                && wake_state.target_term == Some(9)
                && wake_state.starting
                && !wake_state.running
                && wake_state.start_waiters == 1;
            drop(wake_state);
            if joined || Instant::now() >= wait_deadline {
                break joined;
            }
            std::thread::yield_now();
        };
        let premature_second = second_rx.try_recv();
        let second_waited_for_start =
            matches!(&premature_second, Err(std::sync::mpsc::TryRecvError::Empty));

        // Always release the injected starter before asserting, so a broken
        // implementation cannot strand either publisher in this regression.
        release_start.wait();
        let first_result = first_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("first publisher reports injected start failure");
        let second_result = match premature_second {
            Ok(result) => result,
            Err(std::sync::mpsc::TryRecvError::Empty) => second_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("joined publisher starts successor"),
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                panic!("second publisher disconnected before reporting")
            }
        };
        first.join().expect("join first publisher");
        second.join().expect("join second publisher");

        assert!(
            second_joined_start,
            "second publisher did not join the reserved start with revision 2 and target 9"
        );
        assert!(
            second_waited_for_start,
            "second publisher returned before the first start outcome was published"
        );
        assert!(matches!(
            first_result,
            Err(NetworkError::Io {
                ref operation,
                ref detail,
            }) if operation == "start mandatory higher Raft term persistence"
                && detail.contains("injected higher-term worker start failure")
        ));
        second_result.expect("joined publisher must start the successor worker");

        wait_for_higher_term_persistence(&paths, &node_id, 9);
        wait_for_higher_term_worker_idle(&worker);
        let wake_state = worker
            .wake_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(wake_state.revision, 2);
        assert_eq!(wake_state.target_term, None);
        assert!(!wake_state.starting);
        assert!(!wake_state.running);
        assert_eq!(wake_state.start_waiters, 0);
    }

    #[test]
    fn reactive_higher_term_worker_retries_a_transient_failure_until_clear() {
        let (_root, paths, node_id) =
            initialize_higher_term_worker_fixture("higher_term_retry_once");
        let wal_root = paths.kv_paths().raft_store().wal_paths(&node_id).root;
        CLUSTER_RAFT_HIGHER_TERM_INJECTED_FAILURES
            .get_or_init(|| Mutex::new(BTreeMap::new()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(wal_root, 1);

        publish_cluster_raft_higher_term_reactively(&paths, &node_id, 7)
            .expect("publish higher term");
        wait_for_higher_term_persistence(&paths, &node_id, 7);
    }

    #[test]
    fn reactive_higher_term_publication_is_prompt_and_coalesces_to_max() {
        let (_root, paths, node_id) =
            initialize_higher_term_worker_fixture("higher_term_coalesce_max");
        let mut admitted = LocalClusterRaftCommandStorage::new(&paths, node_id.clone());
        admitted
            .last_log_index()
            .expect("hold an admitted authority lease");

        let publish_paths = paths.clone();
        let publish_node_id = node_id.clone();
        let (published_tx, published_rx) = std::sync::mpsc::sync_channel(1);
        let publisher = std::thread::spawn(move || {
            let result =
                publish_cluster_raft_higher_term_reactively(&publish_paths, &publish_node_id, 5);
            published_tx.send(result).expect("report publication");
        });
        let publication = match published_rx.recv_timeout(Duration::from_millis(500)) {
            Ok(result) => result,
            Err(error) => {
                drop(admitted);
                publisher.join().expect("join delayed publisher");
                panic!("higher-term floor publication blocked behind an admitted reader: {error}");
            }
        };
        publication.expect("publish while earlier authority action is admitted");
        publisher.join().expect("join publisher");

        publish_cluster_raft_higher_term_reactively(&paths, &node_id, 9)
            .expect("raise floor to max");
        publish_cluster_raft_higher_term_reactively(&paths, &node_id, 7)
            .expect("older publication is monotonic");
        assert_eq!(
            paths
                .kv_paths()
                .raft_store()
                .pending_higher_term(&node_id)
                .expect("pending floor"),
            Some(9)
        );
        assert!(
            LocalClusterRaftCommandStorage::new(&paths, node_id.clone())
                .last_log_index()
                .is_err(),
            "a reader arriving after publication must fail closed without waiting"
        );

        drop(admitted);
        wait_for_higher_term_persistence(&paths, &node_id, 9);
    }

    #[test]
    fn reactive_higher_term_worker_verifies_suppressed_term_after_lower_replacement() {
        let (_root, paths, node_id) =
            initialize_higher_term_worker_fixture("higher_term_suppressed_replacement");
        let store = paths.kv_paths().raft_store();
        let wal_paths = store.wal_paths(&node_id);

        publish_cluster_raft_higher_term_reactively(&paths, &node_id, 7)
            .expect("publish initial higher term");
        wait_for_higher_term_persistence(&paths, &node_id, 7);
        let worker = cluster_raft_higher_term_persistence_worker(wal_paths.root.clone());
        wait_for_higher_term_worker_idle(&worker);

        // Retain only operation admission so the second worker cannot open and
        // acknowledge B3 until the suppressed publication and its verification
        // target have both been observed. The A7 resident stays idle: replacing
        // hard state therefore invalidates its recorded durable-view identity,
        // exactly like the supported same-path replacement path.
        let admitted = store
            .lock_operation()
            .expect("hold Raft operation admission");
        let mut replacement = wal_paths
            .read_persisted_hard_state()
            .expect("read durable A7 hard state");
        replacement.current_term = 3;
        replacement.voted_for = None;
        replacement.leader_id = None;
        wal_paths
            .write_persisted_hard_state(&replacement)
            .expect("replace durable hard state with B3");

        publish_cluster_raft_higher_term_reactively(&paths, &node_id, 7)
            .expect("reactively publish cached term against B3");
        assert_eq!(
            store
                .pending_higher_term(&node_id)
                .expect("suppressed higher-term floor"),
            None,
            "the process-known A7 watermark must suppress the second term-7 gate floor"
        );
        assert_eq!(
            wal_paths
                .read_persisted_hard_state()
                .expect("worker remains blocked behind admitted storage")
                .current_term,
            3
        );
        assert_eq!(
            worker
                .wake_state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .target_term,
            Some(7),
            "suppressed publication must remain queued for durable-generation verification"
        );

        drop(admitted);
        wait_for_higher_term_persistence(&paths, &node_id, 7);
        wait_for_higher_term_worker_idle(&worker);
        assert_eq!(
            wal_paths
                .read_persisted_hard_state()
                .expect("worker-persisted B7 hard state")
                .current_term,
            7
        );
        assert_eq!(
            store
                .pending_higher_term(&node_id)
                .expect("worker-settled higher-term floor"),
            None
        );
        assert_eq!(
            worker
                .wake_state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .target_term,
            None,
            "the worker alone must settle its generation-verification target"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn complete_always_executes_even_when_deadline_is_expired_at_entry() {
        let ran = Arc::new(AtomicUsize::new(0));
        let work_ran = Arc::clone(&ran);
        let result = run_cluster_raft_supervised_blocking(
            &tokio::runtime::Handle::current(),
            Some((
                Instant::now()
                    .checked_sub(Duration::from_millis(1))
                    .expect("expired deadline"),
                expired_supervised_test_timeout(),
                ClusterRaftBlockingDeadlinePolicy::DeadlineBounded,
            )),
            ClusterRaftBlockingCancellationPolicy::CompleteAlways,
            "test",
            "expired-complete-always",
            "test expired mandatory blocking work",
            move || {
                work_ran.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )
        .await;

        assert!(
            result.is_err(),
            "the original deadline remains authoritative"
        );
        assert_eq!(
            ran.load(Ordering::SeqCst),
            1,
            "mandatory safety work must still reach durable completion"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancel_queued_does_not_execute_when_deadline_is_expired_at_entry() {
        let ran = Arc::new(AtomicUsize::new(0));
        let work_ran = Arc::clone(&ran);
        let result = run_cluster_raft_supervised_blocking(
            &tokio::runtime::Handle::current(),
            Some((
                Instant::now()
                    .checked_sub(Duration::from_millis(1))
                    .expect("expired deadline"),
                expired_supervised_test_timeout(),
                ClusterRaftBlockingDeadlinePolicy::DeadlineBounded,
            )),
            ClusterRaftBlockingCancellationPolicy::CancelQueued,
            "test",
            "expired-cancel-queued",
            "test expired optional blocking work",
            move || {
                work_ran.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )
        .await;

        assert!(
            result.is_err(),
            "expired optional work must fail immediately"
        );
        assert_eq!(
            ran.load(Ordering::SeqCst),
            0,
            "expired queued work must never enter the blocking pool"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn immediate_supervised_completions_cannot_lose_the_terminal_wakeup() {
        tokio::time::timeout(Duration::from_secs(2), async {
            for expected in 0_u64..2_000 {
                let value = run_cluster_raft_supervised_blocking(
                    &tokio::runtime::Handle::current(),
                    None,
                    ClusterRaftBlockingCancellationPolicy::CancelQueued,
                    "test",
                    "immediate-completion",
                    "test immediate supervised blocking completion",
                    move || Ok(expected),
                )
                .await
                .expect("immediate completion");
                assert_eq!(value, expected);
            }
        })
        .await
        .expect("terminal wakeup must never be lost");
    }

    #[test]
    fn queued_read_index_preparation_timeout_prevents_late_blocking_work() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .expect("runtime");
        let (blocker_started_tx, blocker_started_rx) = std::sync::mpsc::sync_channel(0);
        let (release_blocker_tx, release_blocker_rx) = std::sync::mpsc::sync_channel(0);
        let blocker = runtime.spawn_blocking(move || {
            blocker_started_tx.send(()).expect("blocker started");
            release_blocker_rx.recv().expect("release blocker");
        });
        blocker_started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("blocking pool saturated");

        let ran = Arc::new(AtomicUsize::new(0));
        let ran_in_work = Arc::clone(&ran);
        let result = runtime.block_on(run_read_index_blocking_before_deadline(
            runtime.handle(),
            Instant::now() + Duration::from_millis(40),
            "test queued resident preparation",
            "test queued ReadIndex preparation",
            move || {
                ran_in_work.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        ));
        assert!(result.is_err(), "queued preparation must hit its deadline");
        release_blocker_tx.send(()).expect("release blocker");
        runtime.block_on(blocker).expect("join blocker");
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(
            ran.load(Ordering::SeqCst),
            0,
            "deadline-cancelled queued work must never run later"
        );
    }

    struct ReadIndexFixture {
        _root: sinomo_testdir::TestDir,
        paths: ClusterPaths,
        state: ClusterBootstrapState,
        local: NodeId,
        initial_config: ClusterKvRaftConfigRecord,
        initial_last_index: u64,
    }

    fn read_index_fixture(leader_term: u64, committed_term: u64) -> ReadIndexFixture {
        let root = sinomo_testdir::TestDir::new("sinomo-cluster-read-index");
        let data_dir = sinomo_local::SinomoDataDir::new(root.path());
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_read_index").expect("cluster id");
        let local = NodeId::from_legacy_named("node_read_index_a").expect("local node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "read index",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        state.term = leader_term;

        let node_ids = [
            local.clone(),
            NodeId::from_legacy_named("node_read_index_b").expect("node b"),
            NodeId::from_legacy_named("node_read_index_c").expect("node c"),
            NodeId::from_legacy_named("node_read_index_d").expect("node d"),
            NodeId::from_legacy_named("node_read_index_e").expect("node e"),
        ];
        let capability =
            CapabilityName::new(DAEMON_MAIN_RUN_CAPABILITY).expect("daemon capability");
        state.members[0].capabilities = vec![capability.clone()];
        state.members[0].allowed_capabilities = vec![capability.clone()];
        for (position, node_id) in node_ids.iter().enumerate().skip(1) {
            let mut member = if position <= 2 {
                ClusterMemberRecord::local_voter(
                    cluster_id.clone(),
                    node_id.clone(),
                    format!("node-{position}"),
                    format!("pk-{position}"),
                    now,
                )
            } else {
                ClusterMemberRecord::local_non_voting(
                    cluster_id.clone(),
                    node_id.clone(),
                    format!("node-{position}"),
                    format!("pk-{position}"),
                    now,
                )
            };
            member.capabilities = vec![capability.clone()];
            member.allowed_capabilities = vec![capability.clone()];
            member.endpoints = vec![sinomo_network::daemon_lan_endpoint(
                node_id,
                format!("10.99.0.{}:21353", position + 1),
                0,
            )];
            state.members.push(member);
        }

        let initial_config = ClusterKvRaftConfigRecord {
            cluster_id,
            term: committed_term,
            kv_revision: 1,
            voters: node_ids[..3].to_vec(),
            learners: Vec::new(),
            non_voting: node_ids[3..].to_vec(),
            updated_at: now,
        };
        let kv_paths = paths.kv_paths();
        let mut node = kv_paths.raft_store().open_node(&local).expect("open node");
        node.become_leader(committed_term)
            .expect("become initial leader");
        for member in &state.members {
            node.append_local_command(
                committed_term,
                KvCommand::Op(KvOp::Put {
                    key: crate::keyspace::cluster_member_key(&member.node_id),
                    value: encode_kv_value(member).expect("encode member"),
                    lease_id: None,
                }),
            )
            .expect("append member");
        }
        let config_entry = node
            .append_local_command(
                committed_term,
                KvCommand::Op(KvOp::Put {
                    key: sinomo_kv::cluster_kv_raft_config_key(),
                    value: encode_kv_value(&initial_config).expect("encode config"),
                    lease_id: None,
                }),
            )
            .expect("append config");
        let value_entry = node
            .append_local_command(
                committed_term,
                KvCommand::Op(KvOp::Put {
                    key: TEST_KEY.to_string(),
                    value: b"confirmed".to_vec(),
                    lease_id: None,
                }),
            )
            .expect("append value");
        node.commit_to(value_entry.index).expect("commit fixture");
        assert!(config_entry.index < value_entry.index);
        if leader_term != committed_term {
            node.become_leader(leader_term)
                .expect("advance leader term");
        }
        let initial_last_index = node.last_log_index();
        drop(node);
        ReadIndexFixture {
            _root: root,
            paths,
            state,
            local,
            initial_config,
            initial_last_index,
        }
    }

    enum ReadIndexAction {
        None,
        HigherTerm(u64),
        ClearLeader,
        CommitRequest(KvCommandRequest),
        ChangeConfigTwice {
            intermediate: ClusterKvRaftConfigRecord,
            final_config: ClusterKvRaftConfigRecord,
        },
    }

    struct ReadIndexHost {
        paths: ClusterPaths,
        local: NodeId,
        action: Option<ReadIndexAction>,
        sends: usize,
        targets: Vec<NodeId>,
    }

    impl ReadIndexHost {
        fn commit_request(&self, request: KvCommandRequest) -> Result<(), NetworkError> {
            let store = self.paths.kv_paths().raft_store();
            let entry = store
                .locked_append_request_uncommitted(&self.local, request)
                .map_err(kv_err)?;
            store
                .locked_commit_index_with_events_and_publish_report(
                    &self.local,
                    entry.index,
                    |_| false,
                )
                .map_err(kv_err)?;
            Ok(())
        }

        fn apply_action(&mut self) -> Result<Option<u64>, NetworkError> {
            match self.action.take().unwrap_or(ReadIndexAction::None) {
                ReadIndexAction::None => Ok(None),
                ReadIndexAction::HigherTerm(term) => Ok(Some(term)),
                ReadIndexAction::ClearLeader => {
                    self.paths
                        .kv_paths()
                        .raft_store()
                        .discard_uncommitted_tail_and_clear_leader(&self.local)
                        .map_err(kv_err)?;
                    Ok(None)
                }
                ReadIndexAction::CommitRequest(request) => {
                    self.commit_request(request)?;
                    Ok(None)
                }
                ReadIndexAction::ChangeConfigTwice {
                    intermediate,
                    final_config,
                } => {
                    for config in [intermediate, final_config] {
                        self.commit_request(KvCommandRequest::generated(KvCommand::Op(
                            KvOp::Put {
                                key: sinomo_kv::cluster_kv_raft_config_key(),
                                value: encode_kv_value(&config).map_err(kv_err)?,
                                lease_id: None,
                            },
                        )))?;
                    }
                    Ok(None)
                }
            }
        }
    }

    impl ClusterRaftQuorumAsyncHost for ReadIndexHost {
        fn send_raft_append_to_node_async<'a>(
            &'a mut self,
            _paths: &'a ClusterPaths,
            state: &'a ClusterBootstrapState,
            _target_node_id: &'a NodeId,
            request: &'a ClusterRaftAppendRequest,
            _timeout: Duration,
        ) -> ClusterMembershipRpcFuture<'a, ClusterRaftAppendResponse> {
            Box::pin(async move {
                assert!(
                    request.read_index_probe,
                    "ReadIndex RPC must carry its marker"
                );
                self.sends = self.sends.saturating_add(1);
                let response_term = self.apply_action()?.unwrap_or(request.append.term);
                let accepted = response_term == request.append.term;
                Ok(ClusterRaftAppendResponse {
                    accepted,
                    detail: if accepted {
                        "accepted ReadIndex heartbeat".to_string()
                    } else {
                        "observed higher term".to_string()
                    },
                    state: state.clone(),
                    append: sinomo_kv::RaftAppendEntriesResponse {
                        term: response_term,
                        success: accepted,
                        match_index: request.append.prev_log_index,
                    },
                })
            })
        }

        fn send_raft_appends_to_quorum_async<'a>(
            &'a mut self,
            _paths: &'a ClusterPaths,
            state: &'a ClusterBootstrapState,
            attempts: ClusterRaftQuorumBatch,
            required_acks: usize,
        ) -> ClusterMembershipRpcFuture<'a, ClusterRaftQuorumBatchOutcomes> {
            Box::pin(async move {
                self.targets
                    .extend(attempts.iter().map(|(target, _, _)| target.clone()));
                let mut outcomes = Vec::new();
                for (target, request, _timeout) in attempts.into_iter().take(required_acks) {
                    assert!(
                        request.read_index_probe,
                        "every quorum-batch ReadIndex RPC must carry its marker"
                    );
                    self.sends = self.sends.saturating_add(1);
                    let response_term = self.apply_action()?.unwrap_or(request.append.term);
                    let accepted = response_term == request.append.term;
                    outcomes.push((
                        target,
                        Arc::clone(&request),
                        Ok(ClusterRaftAppendResponse {
                            accepted,
                            detail: if accepted {
                                "accepted ReadIndex heartbeat".to_string()
                            } else {
                                "observed higher term".to_string()
                            },
                            state: state.clone(),
                            append: sinomo_kv::RaftAppendEntriesResponse {
                                term: response_term,
                                success: accepted,
                                match_index: request.append.prev_log_index,
                            },
                        }),
                    ));
                }
                Ok(outcomes)
            })
        }
    }

    struct CoalescedReadIndexHostState {
        quorum_waves: AtomicUsize,
        wave_entered: tokio::sync::Semaphore,
        wave_release: tokio::sync::Semaphore,
    }

    struct CoalescedReadIndexHost {
        shared: Arc<CoalescedReadIndexHostState>,
    }

    impl ClusterRaftQuorumAsyncHost for CoalescedReadIndexHost {
        fn send_raft_append_to_node_async<'a>(
            &'a mut self,
            _paths: &'a ClusterPaths,
            _state: &'a ClusterBootstrapState,
            _target_node_id: &'a NodeId,
            _request: &'a ClusterRaftAppendRequest,
            _timeout: Duration,
        ) -> ClusterMembershipRpcFuture<'a, ClusterRaftAppendResponse> {
            Box::pin(async {
                Err(NetworkError::Io {
                    operation: "send coalesced ReadIndex probe".to_string(),
                    detail: "single-target path should not be used by the coalescing test"
                        .to_string(),
                })
            })
        }

        fn send_raft_appends_to_quorum_async<'a>(
            &'a mut self,
            _paths: &'a ClusterPaths,
            state: &'a ClusterBootstrapState,
            attempts: ClusterRaftQuorumBatch,
            required_acks: usize,
        ) -> ClusterMembershipRpcFuture<'a, ClusterRaftQuorumBatchOutcomes> {
            Box::pin(async move {
                self.shared.quorum_waves.fetch_add(1, Ordering::SeqCst);
                self.shared.wave_entered.add_permits(1);
                self.shared
                    .wave_release
                    .acquire()
                    .await
                    .expect("ReadIndex wave release semaphore")
                    .forget();

                Ok(attempts
                    .into_iter()
                    .take(required_acks)
                    .map(|(target, request, _timeout)| {
                        let response = ClusterRaftAppendResponse {
                            accepted: true,
                            detail: "accepted coalesced ReadIndex heartbeat".to_string(),
                            state: state.clone(),
                            append: sinomo_kv::RaftAppendEntriesResponse {
                                term: request.append.term,
                                success: true,
                                match_index: request.append.prev_log_index,
                            },
                        };
                        (target, request, Ok(response))
                    })
                    .collect())
            })
        }
    }

    struct FlooredReadIndexHostState {
        pre_dispatch_callbacks: AtomicUsize,
        pre_dispatch_entered: tokio::sync::Semaphore,
        pre_dispatch_release: tokio::sync::Semaphore,
        quorum_waves: AtomicUsize,
        wave_entered: tokio::sync::Semaphore,
        wave_release: tokio::sync::Semaphore,
    }

    impl FlooredReadIndexHostState {
        fn new() -> Self {
            Self {
                pre_dispatch_callbacks: AtomicUsize::new(0),
                pre_dispatch_entered: tokio::sync::Semaphore::new(0),
                pre_dispatch_release: tokio::sync::Semaphore::new(0),
                quorum_waves: AtomicUsize::new(0),
                wave_entered: tokio::sync::Semaphore::new(0),
                wave_release: tokio::sync::Semaphore::new(0),
            }
        }
    }

    struct FlooredReadIndexHost {
        shared: Arc<FlooredReadIndexHostState>,
    }

    impl ClusterRaftQuorumAsyncHost for FlooredReadIndexHost {
        fn on_raft_append_sent_async<'a>(&'a mut self) -> ClusterMembershipRpcFuture<'a, ()> {
            Box::pin(async move {
                if self
                    .shared
                    .pre_dispatch_callbacks
                    .fetch_add(1, Ordering::SeqCst)
                    == 0
                {
                    self.shared.pre_dispatch_entered.add_permits(1);
                    self.shared
                        .pre_dispatch_release
                        .acquire()
                        .await
                        .expect("release floored ReadIndex dispatch")
                        .forget();
                }
                Ok(())
            })
        }

        fn send_raft_append_to_node_async<'a>(
            &'a mut self,
            _paths: &'a ClusterPaths,
            _state: &'a ClusterBootstrapState,
            _target_node_id: &'a NodeId,
            _request: &'a ClusterRaftAppendRequest,
            _timeout: Duration,
        ) -> ClusterMembershipRpcFuture<'a, ClusterRaftAppendResponse> {
            Box::pin(async {
                Err(NetworkError::Io {
                    operation: "send floored ReadIndex probe".to_string(),
                    detail: "floored ReadIndex test requires the batched transport".to_string(),
                })
            })
        }

        fn send_raft_appends_to_quorum_async<'a>(
            &'a mut self,
            _paths: &'a ClusterPaths,
            state: &'a ClusterBootstrapState,
            attempts: ClusterRaftQuorumBatch,
            required_acks: usize,
        ) -> ClusterMembershipRpcFuture<'a, ClusterRaftQuorumBatchOutcomes> {
            Box::pin(async move {
                self.shared.quorum_waves.fetch_add(1, Ordering::SeqCst);
                self.shared.wave_entered.add_permits(1);
                self.shared
                    .wave_release
                    .acquire()
                    .await
                    .expect("release floored ReadIndex wave")
                    .forget();
                Ok(attempts
                    .into_iter()
                    .take(required_acks)
                    .map(|(target, request, _timeout)| {
                        let response = ClusterRaftAppendResponse {
                            accepted: true,
                            detail: "accepted floored ReadIndex heartbeat".to_string(),
                            state: state.clone(),
                            append: sinomo_kv::RaftAppendEntriesResponse {
                                term: request.append.term,
                                success: true,
                                match_index: request.append.prev_log_index,
                            },
                        };
                        (target, request, Ok(response))
                    })
                    .collect())
            })
        }
    }

    #[derive(Clone, Copy)]
    enum ReclaimFirstWave {
        Cancelled,
        TimedOut,
    }

    struct ReclaimReadIndexHostState {
        quorum_waves: AtomicUsize,
        pre_dispatch_callbacks: AtomicUsize,
        pre_dispatch_entered: tokio::sync::Semaphore,
        pre_dispatch_release: tokio::sync::Semaphore,
        first_wave_entered: tokio::sync::Semaphore,
        fail_first_wave: tokio::sync::Semaphore,
        first_wave: ReclaimFirstWave,
    }

    struct ReclaimReadIndexHost {
        shared: Arc<ReclaimReadIndexHostState>,
    }

    impl ClusterRaftQuorumAsyncHost for ReclaimReadIndexHost {
        fn on_raft_append_sent_async<'a>(&'a mut self) -> ClusterMembershipRpcFuture<'a, ()> {
            Box::pin(async move {
                if self
                    .shared
                    .pre_dispatch_callbacks
                    .fetch_add(1, Ordering::SeqCst)
                    == 0
                {
                    self.shared.pre_dispatch_entered.add_permits(1);
                    self.shared
                        .pre_dispatch_release
                        .acquire()
                        .await
                        .expect("release pre-dispatch ReadIndex owner")
                        .forget();
                }
                Ok(())
            })
        }

        fn send_raft_append_to_node_async<'a>(
            &'a mut self,
            _paths: &'a ClusterPaths,
            _state: &'a ClusterBootstrapState,
            _target_node_id: &'a NodeId,
            _request: &'a ClusterRaftAppendRequest,
            _timeout: Duration,
        ) -> ClusterMembershipRpcFuture<'a, ClusterRaftAppendResponse> {
            Box::pin(async {
                Err(NetworkError::Io {
                    operation: "send reclaimed ReadIndex probe".to_string(),
                    detail: "single-target path should not be used by the reclaim test".to_string(),
                })
            })
        }

        fn send_raft_appends_to_quorum_async<'a>(
            &'a mut self,
            _paths: &'a ClusterPaths,
            state: &'a ClusterBootstrapState,
            attempts: ClusterRaftQuorumBatch,
            required_acks: usize,
        ) -> ClusterMembershipRpcFuture<'a, ClusterRaftQuorumBatchOutcomes> {
            Box::pin(async move {
                let wave = self.shared.quorum_waves.fetch_add(1, Ordering::SeqCst);
                if wave == 0 {
                    self.shared.first_wave_entered.add_permits(1);
                    match self.shared.first_wave {
                        ReclaimFirstWave::Cancelled => std::future::pending::<()>().await,
                        ReclaimFirstWave::TimedOut => {
                            self.shared
                                .fail_first_wave
                                .acquire()
                                .await
                                .expect("release first ReadIndex timeout wave")
                                .forget();
                            return Err(NetworkError::Timeout {
                                operation: "test ReadIndex quorum wave".to_string(),
                                phase: sinomo_network::NetworkTimeoutPhase::Overall,
                                timeout: Duration::from_millis(1),
                                delivery_state:
                                    sinomo_network::NetworkDeliveryState::DefinitelyNotSent,
                            });
                        }
                    }
                }

                Ok(attempts
                    .into_iter()
                    .take(required_acks)
                    .map(|(target, request, _timeout)| {
                        let response = ClusterRaftAppendResponse {
                            accepted: true,
                            detail: "accepted reclaimed ReadIndex heartbeat".to_string(),
                            state: state.clone(),
                            append: sinomo_kv::RaftAppendEntriesResponse {
                                term: request.append.term,
                                success: true,
                                match_index: request.append.prev_log_index,
                            },
                        };
                        (target, request, Ok(response))
                    })
                    .collect())
            })
        }
    }

    fn run_read_index<T: Send + 'static>(
        fixture: &mut ReadIndexFixture,
        host: &mut ReadIndexHost,
        read_value: impl FnOnce(&mut dyn LocalClusterRaftReadProjection) -> Result<T, NetworkError>
        + Send
        + 'static,
    ) -> Result<Option<sinomo_kv::KvAuthoritativeRead<T>>, NetworkError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(read_cluster_raft_with_read_index_async_with_host(
            runtime.handle(),
            &fixture.paths,
            &mut fixture.state,
            Duration::from_secs(2),
            ClusterControlPlaneTimeouts::default_daemon_transport()
                .raft_append_replication_options(Duration::from_secs(2)),
            read_value,
            host,
        ))
    }

    fn no_action_host(fixture: &ReadIndexFixture) -> ReadIndexHost {
        ReadIndexHost {
            paths: fixture.paths.clone(),
            local: fixture.local.clone(),
            action: Some(ReadIndexAction::None),
            sends: 0,
            targets: Vec::new(),
        }
    }

    fn commit_unresolved_local_single_voter_fixture(
        fixture: &mut ReadIndexFixture,
        clear_leader: bool,
    ) {
        let mut config = fixture.initial_config.clone();
        config.term = fixture.state.term;
        config.voters = vec![fixture.local.clone()];
        config.learners.clear();
        config.non_voting.clear();
        config.updated_at = OffsetDateTime::now_utc();

        let kv_paths = fixture.paths.kv_paths();
        let mut node = kv_paths
            .raft_store()
            .open_node(&fixture.local)
            .expect("open local single-voter node");
        config.kv_revision = node.store().revision().0.saturating_add(1);
        node.append_local_command(
            fixture.state.term,
            KvCommand::Op(KvOp::Put {
                key: sinomo_kv::cluster_kv_raft_config_key(),
                value: encode_kv_value(&config).expect("encode single-voter config"),
                lease_id: None,
            }),
        )
        .expect("append single-voter config");
        let missing_member = node
            .append_local_command(
                fixture.state.term,
                KvCommand::Op(KvOp::Delete {
                    key: crate::keyspace::cluster_member_key(&fixture.local),
                }),
            )
            .expect("append missing local member row");
        node.commit_to(missing_member.index)
            .expect("commit unresolved local fixture");
        fixture.state.kv_revision = node.store().revision().0;
        fixture.initial_last_index = node.last_log_index();
        drop(node);

        fixture
            .state
            .members
            .retain(|member| member.node_id == fixture.local);
        fixture.state.members[0].role = ClusterMemberRole::Voter;
        fixture.state.members[0].state = ClusterMemberState::Voter;
        fixture.state.members[0].endpoints = vec![sinomo_network::daemon_lan_endpoint(
            &fixture.local,
            "127.0.0.1:21353".to_string(),
            99,
        )];
        fixture.initial_config = config;

        if clear_leader {
            kv_paths
                .raft_store()
                .discard_uncommitted_tail_and_clear_leader(&fixture.local)
                .expect("clear local leader");
        }
    }

    #[test]
    fn unresolved_local_member_allows_only_confirmed_single_voter_self_read() {
        let mut fixture = read_index_fixture(7, 7);
        commit_unresolved_local_single_voter_fixture(&mut fixture, false);
        let mut host = no_action_host(&fixture);

        let read = run_read_index(&mut fixture, &mut host, |storage| {
            storage.read_index_entry(TEST_KEY)
        })
        .expect("local unresolved-member ReadIndex")
        .expect("confirmed local self read");

        assert_eq!(read.value.expect("entry").value, b"confirmed".to_vec());
        assert!(matches!(
            read.authority.basis,
            sinomo_kv::KvAuthorityBasis::CurrentTermQuorum {
                voter_acks: 1,
                required_quorum: 1
            }
        ));
        assert_eq!(host.sends, 0, "the unresolved local node must never route");
        let local = fixture
            .state
            .members
            .iter()
            .find(|member| member.node_id == fixture.local)
            .expect("retained local placeholder");
        assert!(local.capabilities.is_empty());
        assert!(local.allowed_capabilities.is_empty());
        assert!(local.endpoints.is_empty());
    }

    #[test]
    fn unresolved_remote_member_in_multivoter_config_fails_closed() {
        let mut fixture = read_index_fixture(7, 7);
        let missing_remote = fixture.initial_config.voters[1].clone();
        let mut node = fixture
            .paths
            .kv_paths()
            .raft_store()
            .open_node(&fixture.local)
            .expect("open node");
        let missing_member = node
            .append_local_command(
                fixture.state.term,
                KvCommand::Op(KvOp::Delete {
                    key: crate::keyspace::cluster_member_key(&missing_remote),
                }),
            )
            .expect("append missing remote member row");
        node.commit_to(missing_member.index)
            .expect("commit missing remote member row");
        fixture.state.kv_revision = node.store().revision().0;
        drop(node);

        let error = prepare_cluster_raft_read_index(
            &fixture.paths,
            fixture.state,
            Instant::now() + Duration::from_secs(1),
            ClusterRaftReadIndexAdmissionMode::WaitForLocal,
        )
        .expect_err("a remote voter without committed identity must fail closed");
        assert!(
            error.to_string().contains(&format!(
                "configuration names member `{missing_remote}` without committed identity record"
            )),
            "unexpected remote-missing rejection: {error}"
        );
    }

    #[test]
    fn unresolved_local_member_cannot_use_nonleader_or_forwarding_path() {
        let mut fixture = read_index_fixture(7, 7);
        commit_unresolved_local_single_voter_fixture(&mut fixture, true);
        let error = prepare_cluster_raft_read_index(
            &fixture.paths,
            fixture.state,
            Instant::now() + Duration::from_secs(1),
            ClusterRaftReadIndexAdmissionMode::WaitForLocal,
        )
        .expect_err("an unresolved local identity must not forward from a nonleader");
        assert!(
            error
                .to_string()
                .contains("requires confirmed local Raft leadership"),
            "unexpected nonleader rejection: {error}"
        );
    }

    #[test]
    fn equal_revision_read_index_rejects_divergent_caller_member_identity() {
        let fixture = read_index_fixture(7, 7);
        let mut storage =
            LocalClusterRaftCommandStorage::new(&fixture.paths, fixture.local.clone());
        let resident = storage
            .read_index_state()
            .expect("resident ReadIndex state");
        drop(storage);

        let mut state = fixture.state.clone();
        state.kv_revision = resident.revision.0;
        state.members[1].node_public_key = "pk-divergent-at-same-revision".to_string();
        let error = prepare_cluster_raft_read_index(
            &fixture.paths,
            state,
            Instant::now() + Duration::from_secs(1),
            ClusterRaftReadIndexAdmissionMode::WaitForLocal,
        )
        .expect_err("equal-revision identity disagreement must fail closed");

        assert!(
            error
                .to_string()
                .contains("member identities disagree at equal KV revision"),
            "unexpected equal-revision rejection: {error}"
        );
    }

    #[test]
    fn equal_revision_read_index_allows_endpoint_and_timestamp_observations() {
        let fixture = read_index_fixture(7, 7);
        let mut storage =
            LocalClusterRaftCommandStorage::new(&fixture.paths, fixture.local.clone());
        let resident = storage
            .read_index_state()
            .expect("resident ReadIndex state");
        drop(storage);

        let mut state = fixture.state.clone();
        state.kv_revision = resident.revision.0;
        let observed_node_id = state.members[1].node_id.clone();
        state.members[1]
            .endpoints
            .push(sinomo_network::daemon_lan_endpoint(
                &observed_node_id,
                "10.199.44.9:21353".to_string(),
                77,
            ));
        state.members[1].joined_at += time::Duration::seconds(1);
        state.members[1].updated_at += time::Duration::seconds(2);

        let prepared = prepare_cluster_raft_read_index(
            &fixture.paths,
            state,
            Instant::now() + Duration::from_secs(1),
            ClusterRaftReadIndexAdmissionMode::WaitForLocal,
        )
        .expect("endpoint/timestamp observations do not change authority semantics");
        assert!(matches!(
            prepared,
            PreparedClusterRaftReadIndex::Confirmable { .. }
        ));
    }

    #[test]
    fn append_request_wave_builds_once_per_distinct_progress_index() {
        let mut shared_builds = 0_usize;
        let shared = build_deduplicated_append_request_wave(
            &[
                (ClusterRaftAppendRequestBuildKey::Tail(9), 9),
                (ClusterRaftAppendRequestBuildKey::Tail(9), 9),
                (ClusterRaftAppendRequestBuildKey::Tail(9), 9),
                (ClusterRaftAppendRequestBuildKey::Tail(9), 9),
            ],
            |next_index| -> Result<_, ()> {
                shared_builds = shared_builds.saturating_add(1);
                Ok(format!("request-{next_index}"))
            },
        )
        .expect("shared progress wave");
        assert_eq!(shared_builds, 1);
        assert_eq!(shared.len(), 4);
        assert!(Arc::ptr_eq(&shared[0], &shared[1]));
        assert!(Arc::ptr_eq(&shared[0], &shared[3]));

        let mut mixed_builds = 0_usize;
        let mixed = build_deduplicated_append_request_wave(
            &[
                (ClusterRaftAppendRequestBuildKey::Tail(9), 9),
                (ClusterRaftAppendRequestBuildKey::Tail(4), 4),
                (ClusterRaftAppendRequestBuildKey::Tail(9), 9),
                (ClusterRaftAppendRequestBuildKey::Tail(2), 2),
                (ClusterRaftAppendRequestBuildKey::Tail(4), 4),
            ],
            |next_index| -> Result<_, ()> {
                mixed_builds = mixed_builds.saturating_add(1);
                Ok(format!("request-{next_index}"))
            },
        )
        .expect("mixed progress wave");
        assert_eq!(mixed_builds, 3);
        assert_eq!(mixed[0], mixed[2]);
        assert_eq!(mixed[1], mixed[4]);
        assert!(Arc::ptr_eq(&mixed[0], &mixed[2]));
        assert!(Arc::ptr_eq(&mixed[1], &mixed[4]));
        assert!(!Arc::ptr_eq(&mixed[0], &mixed[1]));

        let mut snapshot_builds = 0_usize;
        let snapshots = build_deduplicated_append_request_wave(
            &[
                (ClusterRaftAppendRequestBuildKey::Snapshot, 1),
                (ClusterRaftAppendRequestBuildKey::Snapshot, 4),
            ],
            |next_index| -> Result<_, ()> {
                snapshot_builds = snapshot_builds.saturating_add(1);
                Ok(format!("snapshot-request-from-{next_index}"))
            },
        )
        .expect("normalized snapshot wave");
        assert_eq!(snapshot_builds, 1);
        assert!(Arc::ptr_eq(&snapshots[0], &snapshots[1]));
    }

    #[test]
    fn read_index_confirms_three_voter_read_without_appending_barrier() {
        let mut fixture = read_index_fixture(7, 7);
        let mut host = no_action_host(&fixture);
        let read = run_read_index(&mut fixture, &mut host, |storage| {
            storage.read_index_entry(TEST_KEY)
        })
        .expect("ReadIndex")
        .expect("steady-state read");

        assert_eq!(read.value.expect("entry").value, b"confirmed".to_vec());
        assert_eq!(host.sends, 1);
        assert!(matches!(
            read.authority.basis,
            sinomo_kv::KvAuthorityBasis::CurrentTermQuorum {
                voter_acks: 2,
                required_quorum: 2
            }
        ));
        let last_index = fixture
            .paths
            .kv_paths()
            .raft_store()
            .locked_last_log_index(&fixture.local)
            .expect("last index");
        assert_eq!(last_index, fixture.initial_last_index);
    }

    #[test]
    fn forwarding_read_index_bypasses_saturated_blocking_pool_through_final_projection() {
        let fixture = read_index_fixture(7, 7);
        let mut state = fixture.state.clone();
        let mut host = no_action_host(&fixture);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .expect("runtime");
        let (blocker_started_tx, blocker_started_rx) = std::sync::mpsc::sync_channel(0);
        let (release_blocker_tx, release_blocker_rx) = std::sync::mpsc::sync_channel(0);
        let blocker = runtime.spawn_blocking(move || {
            blocker_started_tx.send(()).expect("blocker started");
            release_blocker_rx.recv().expect("release blocker");
        });
        blocker_started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("blocking pool saturated");

        let authority_budget = Duration::from_secs(5);
        let started = Instant::now();
        let attempt = runtime
            .block_on(async {
                tokio::time::timeout(
                    Duration::from_millis(150),
                    read_cluster_raft_with_read_index_attempt_async_with_host_and_admission(
                        runtime.handle(),
                        &fixture.paths,
                        &mut state,
                        authority_budget,
                        ClusterControlPlaneTimeouts::default_daemon_transport()
                            .raft_append_replication_options(authority_budget),
                        ClusterRaftReadIndexAdmissionMode::ForwardOnBusy,
                        |storage| storage.read_index_entry(TEST_KEY),
                        &mut host,
                    ),
                )
                .await
            })
            .expect("bounded ReadIndex must not wait behind the blocking pool")
            .expect("bounded ReadIndex");
        let ClusterRaftReadIndexAttempt::Confirmed(read) = attempt else {
            panic!("hot resident ReadIndex should remain authoritative")
        };
        assert_eq!(read.value.expect("entry").value, b"confirmed".to_vec());
        assert!(
            started.elapsed() < Duration::from_millis(150),
            "bounded ReadIndex expanded into {:?} despite an immediate quorum",
            started.elapsed()
        );

        release_blocker_tx.send(()).expect("release blocker");
        runtime.block_on(blocker).expect("join blocker");
    }

    #[test]
    fn post_quorum_projection_bypasses_the_raft_operation_gate() {
        let fixture = read_index_fixture(7, 7);
        let shared = Arc::new(CoalescedReadIndexHostState {
            quorum_waves: AtomicUsize::new(0),
            wave_entered: tokio::sync::Semaphore::new(0),
            wave_release: tokio::sync::Semaphore::new(0),
        });
        let mut state = fixture.state.clone();
        let mut host = CoalescedReadIndexHost {
            shared: Arc::clone(&shared),
        };
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let authority_budget = Duration::from_secs(5);
        let append_options = ClusterControlPlaneTimeouts::default_daemon_transport()
            .raft_append_replication_options(authority_budget);

        let started = Instant::now();
        let (attempt, operation_guard) = runtime
            .block_on(async {
                tokio::time::timeout(Duration::from_millis(150), async {
                    let read =
                        read_cluster_raft_with_read_index_attempt_async_with_host_and_admission(
                            runtime.handle(),
                            &fixture.paths,
                            &mut state,
                            authority_budget,
                            append_options,
                            ClusterRaftReadIndexAdmissionMode::ForwardOnBusy,
                            |storage| storage.read_index_entry(TEST_KEY),
                            &mut host,
                        );
                    let hold_after_quorum = async {
                        shared
                            .wave_entered
                            .acquire()
                            .await
                            .expect("ReadIndex quorum wave entered")
                            .forget();
                        let guard = sinomo_kv::lock_cluster_kv_raft_operation_for_paths(
                            &fixture.paths.kv_paths(),
                        )
                        .expect("hold Raft operation gate after quorum preparation");
                        shared.wave_release.add_permits(1);
                        guard
                    };
                    tokio::join!(read, hold_after_quorum)
                })
                .await
            })
            .expect("post-quorum projection must not wait on the Raft operation gate");
        let ClusterRaftReadIndexAttempt::Confirmed(read) = attempt.expect("ReadIndex") else {
            panic!("post-quorum resident projection should remain authoritative")
        };
        assert_eq!(read.value.expect("entry").value, b"confirmed".to_vec());
        assert!(
            started.elapsed() < Duration::from_millis(150),
            "post-quorum projection expanded into {:?}",
            started.elapsed()
        );
        drop(operation_guard);
    }

    #[test]
    fn read_index_targets_committed_voters_without_directory_projection() {
        let mut fixture = read_index_fixture(7, 7);
        let local = fixture.local.clone();
        for member in fixture
            .state
            .members
            .iter_mut()
            .filter(|member| member.node_id != local)
        {
            member.endpoints.clear();
        }
        let mut host = no_action_host(&fixture);

        let read = run_read_index(&mut fixture, &mut host, |storage| {
            storage.read_index_entry(TEST_KEY)
        })
        .expect("ReadIndex without a directory projection")
        .expect("committed-voter ReadIndex");

        assert_eq!(read.value.expect("entry").value, b"confirmed".to_vec());
        assert_eq!(
            host.sends, 1,
            "the exact resident-fence voter ids must reach the transport even when no endpoint directory is available"
        );
        let expected_targets = fixture
            .initial_config
            .voters
            .iter()
            .filter(|node_id| *node_id != &fixture.local)
            .cloned()
            .collect::<BTreeSet<_>>();
        assert_eq!(
            host.targets.into_iter().collect::<BTreeSet<_>>(),
            expected_targets
        );
    }

    #[test]
    fn blocked_read_index_quorum_transport_does_not_monopolize_local_raft_storage() {
        let fixture = read_index_fixture(7, 7);
        let shared = Arc::new(CoalescedReadIndexHostState {
            quorum_waves: AtomicUsize::new(0),
            wave_entered: tokio::sync::Semaphore::new(0),
            wave_release: tokio::sync::Semaphore::new(0),
        });
        let mut state = fixture.state.clone();
        let mut host = CoalescedReadIndexHost {
            shared: Arc::clone(&shared),
        };
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let timeout = Duration::from_secs(2);
        let append_options = ClusterControlPlaneTimeouts::default_daemon_transport()
            .raft_append_replication_options(timeout);

        let (read, local_read) = runtime.block_on(async {
            let read = read_cluster_raft_with_read_index_async_with_host(
                runtime.handle(),
                &fixture.paths,
                &mut state,
                timeout,
                append_options,
                |storage| storage.read_index_entry(TEST_KEY),
                &mut host,
            );
            let local_read = async {
                shared
                    .wave_entered
                    .acquire()
                    .await
                    .expect("ReadIndex quorum wave entered")
                    .forget();
                let paths = fixture.paths.clone();
                let local = fixture.local.clone();
                let diagnostic = tokio::time::timeout(
                    Duration::from_millis(500),
                    tokio::task::spawn_blocking(move || {
                        paths.kv_paths().raft_store().locked_last_log_index(&local)
                    }),
                )
                .await;
                shared.wave_release.add_permits(1);
                diagnostic
                    .expect("local Raft storage must not wait for ReadIndex transport")
                    .expect("local Raft diagnostic task")
            };
            tokio::join!(read, local_read)
        });

        let read = read.expect("ReadIndex").expect("authoritative read");
        assert_eq!(read.value.expect("entry").value, b"confirmed".to_vec());
        assert_eq!(
            local_read.expect("local Raft diagnostic"),
            fixture.initial_last_index
        );
        assert_eq!(shared.quorum_waves.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn admitted_snapshot_projection_finishes_before_caller_returns_and_releases_resident_node() {
        let fixture = read_index_fixture(7, 7);
        let paths = fixture.paths.clone();
        let local = fixture.local.clone();
        let mut state = fixture.state.clone();
        let mut host = no_action_host(&fixture);
        let (projection_entered_tx, projection_entered_rx) = std::sync::mpsc::sync_channel(1);
        let (projection_release_tx, projection_release_rx) = std::sync::mpsc::sync_channel(0);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let timeout = Duration::from_millis(100);
        let append_options = ClusterControlPlaneTimeouts::default_daemon_transport()
            .raft_append_replication_options(timeout);

        runtime.block_on(async {
            let read_paths = paths.clone();
            let read = tokio::spawn(async move {
                read_cluster_raft_with_read_index_attempt_async_with_projection_policy(
                    &tokio::runtime::Handle::current(),
                    &read_paths,
                    &mut state,
                    timeout,
                    append_options,
                    ClusterRaftReadProjectionPolicy::CompleteAdmittedSnapshot,
                    move |storage| {
                        projection_entered_tx
                            .send(())
                            .expect("signal admitted snapshot projection");
                        projection_release_rx
                            .recv()
                            .expect("release admitted snapshot projection");
                        storage.read_index_snapshot()
                    },
                    &mut host,
                )
                .await
            });

            tokio::task::spawn_blocking(move || projection_entered_rx.recv())
                .await
                .expect("join projection-entry waiter")
                .expect("snapshot projection acquired resident ownership");
            tokio::time::sleep(Duration::from_millis(150)).await;
            assert!(
                !read.is_finished(),
                "caller must not return while admitted snapshot work still owns the resident node"
            );
            projection_release_tx
                .send(())
                .expect("release snapshot projection");
            let attempt = read
                .await
                .expect("join admitted snapshot caller")
                .expect("over-budget admitted snapshot result");
            let ClusterRaftReadIndexAttempt::Confirmed(read) = attempt else {
                panic!("admitted full snapshot must return its coherent result")
            };
            assert_eq!(read.value.revision, read.authority.revision);

            let diagnostic_paths = paths.clone();
            let diagnostic_local = local.clone();
            tokio::time::timeout(
                Duration::from_millis(250),
                tokio::task::spawn_blocking(move || {
                    LocalClusterRaftCommandStorage::new_read_index_with_operation_deadline(
                        &diagnostic_paths,
                        diagnostic_local,
                        Instant::now() + Duration::from_millis(200),
                    )
                    .read_index_state()
                }),
            )
            .await
            .expect("resident ownership must be released when the caller returns")
            .expect("join post-projection resident probe")
            .expect("post-projection resident probe");
        });
    }

    #[test]
    fn admitted_indexed_projection_reports_deadline_only_after_releasing_resident_node() {
        let fixture = read_index_fixture(7, 7);
        let paths = fixture.paths.clone();
        let local = fixture.local.clone();
        let mut state = fixture.state.clone();
        let mut host = no_action_host(&fixture);
        let (projection_entered_tx, projection_entered_rx) = std::sync::mpsc::sync_channel(1);
        let (projection_release_tx, projection_release_rx) = std::sync::mpsc::sync_channel(0);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let timeout = Duration::from_millis(100);
        let append_options = ClusterControlPlaneTimeouts::default_daemon_transport()
            .raft_append_replication_options(timeout);

        runtime.block_on(async {
            let read_paths = paths.clone();
            let read = tokio::spawn(async move {
                read_cluster_raft_with_read_index_attempt_async_with_projection_policy(
                    &tokio::runtime::Handle::current(),
                    &read_paths,
                    &mut state,
                    timeout,
                    append_options,
                    ClusterRaftReadProjectionPolicy::DeadlineBounded,
                    move |storage| {
                        projection_entered_tx
                            .send(())
                            .expect("signal admitted indexed projection");
                        projection_release_rx
                            .recv()
                            .expect("release admitted indexed projection");
                        storage.read_index_entry(TEST_KEY)
                    },
                    &mut host,
                )
                .await
            });

            tokio::task::spawn_blocking(move || projection_entered_rx.recv())
                .await
                .expect("join projection-entry waiter")
                .expect("indexed projection acquired resident ownership");
            tokio::time::sleep(Duration::from_millis(150)).await;
            assert!(
                !read.is_finished(),
                "caller must not detach indexed work that owns the resident node"
            );
            projection_release_tx
                .send(())
                .expect("release indexed projection");
            let error = match read.await.expect("join indexed projection caller") {
                Ok(_) => panic!("over-budget indexed projection must report its deadline"),
                Err(error) => error,
            };
            assert!(
                error.to_string().contains("final applied read projection"),
                "unexpected indexed projection error: {error}"
            );

            let diagnostic_paths = paths.clone();
            tokio::time::timeout(
                Duration::from_millis(250),
                tokio::task::spawn_blocking(move || {
                    LocalClusterRaftCommandStorage::new_read_index_with_operation_deadline(
                        &diagnostic_paths,
                        local,
                        Instant::now() + Duration::from_millis(200),
                    )
                    .read_index_state()
                }),
            )
            .await
            .expect("resident ownership must be released before the timeout is returned")
            .expect("join post-projection resident probe")
            .expect("post-projection resident probe");
        });
    }

    #[test]
    fn cancelled_admitted_snapshot_projection_is_supervised_to_resident_release() {
        let fixture = read_index_fixture(7, 7);
        let paths = fixture.paths.clone();
        let local = fixture.local.clone();
        let mut state = fixture.state.clone();
        let mut host = no_action_host(&fixture);
        let (projection_entered_tx, projection_entered_rx) = std::sync::mpsc::sync_channel(1);
        let (projection_release_tx, projection_release_rx) = std::sync::mpsc::sync_channel(0);
        let (projection_completed_tx, projection_completed_rx) = std::sync::mpsc::sync_channel(1);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let timeout = Duration::from_secs(2);
        let append_options = ClusterControlPlaneTimeouts::default_daemon_transport()
            .raft_append_replication_options(timeout);

        runtime.block_on(async {
            let read_paths = paths.clone();
            let read = tokio::spawn(async move {
                read_cluster_raft_with_read_index_attempt_async_with_projection_policy(
                    &tokio::runtime::Handle::current(),
                    &read_paths,
                    &mut state,
                    timeout,
                    append_options,
                    ClusterRaftReadProjectionPolicy::CompleteAdmittedSnapshot,
                    move |storage| {
                        projection_entered_tx
                            .send(())
                            .expect("signal admitted projection");
                        projection_release_rx
                            .recv()
                            .expect("release admitted projection");
                        let result = storage.read_index_snapshot();
                        projection_completed_tx
                            .send(())
                            .expect("signal projection completion");
                        result
                    },
                    &mut host,
                )
                .await
            });

            tokio::task::spawn_blocking(move || projection_entered_rx.recv())
                .await
                .expect("join projection-entry waiter")
                .expect("projection acquired resident ownership");
            read.abort();
            let _ = read.await;
            projection_release_tx
                .send(())
                .expect("release cancelled projection");
            tokio::task::spawn_blocking(move || projection_completed_rx.recv())
                .await
                .expect("join projection completion waiter")
                .expect("supervisor completed admitted projection");

            let diagnostic_paths = paths.clone();
            tokio::time::timeout(
                Duration::from_millis(250),
                tokio::task::spawn_blocking(move || {
                    LocalClusterRaftCommandStorage::new_read_index_with_operation_deadline(
                        &diagnostic_paths,
                        local,
                        Instant::now() + Duration::from_millis(200),
                    )
                    .read_index_state()
                }),
            )
            .await
            .expect("cancelled projection supervisor must release resident ownership")
            .expect("join post-cancellation resident probe")
            .expect("post-cancellation resident probe");
        });
    }

    #[test]
    fn concurrent_read_indexes_share_one_quorum_confirmation_wave() {
        let fixture = read_index_fixture(7, 7);
        let shared = Arc::new(CoalescedReadIndexHostState {
            quorum_waves: AtomicUsize::new(0),
            wave_entered: tokio::sync::Semaphore::new(0),
            wave_release: tokio::sync::Semaphore::new(0),
        });
        let mut first_state = fixture.state.clone();
        let mut second_state = fixture.state.clone();
        let mut first_host = CoalescedReadIndexHost {
            shared: Arc::clone(&shared),
        };
        let mut second_host = CoalescedReadIndexHost {
            shared: Arc::clone(&shared),
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let timeout = Duration::from_secs(2);
        let append_options = ClusterControlPlaneTimeouts::default_daemon_transport()
            .raft_append_replication_options(timeout);
        let root = fixture.paths.root.clone();

        let (first, second, ()) = runtime.block_on(async {
            let first = read_cluster_raft_with_read_index_async_with_host(
                runtime.handle(),
                &fixture.paths,
                &mut first_state,
                timeout,
                append_options,
                |storage| storage.read_index_entry(TEST_KEY),
                &mut first_host,
            );
            let second = read_cluster_raft_with_read_index_async_with_host(
                runtime.handle(),
                &fixture.paths,
                &mut second_state,
                timeout,
                append_options,
                |storage| storage.read_index_entry(TEST_KEY),
                &mut second_host,
            );
            let release = async {
                tokio::time::timeout(Duration::from_secs(1), async {
                    loop {
                        if read_index_singleflight_waiters_for_root(&root) == 1 {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("second read joined before the ReadIndex flight sealed");
                shared
                    .wave_entered
                    .acquire()
                    .await
                    .expect("ReadIndex quorum wave entered")
                    .forget();
                shared.wave_release.add_permits(1);
            };
            tokio::join!(first, second, release)
        });

        let first = first.expect("first ReadIndex").expect("first read");
        let second = second.expect("second ReadIndex").expect("second read");
        assert_eq!(
            first.value.expect("first entry").value,
            b"confirmed".to_vec()
        );
        assert_eq!(
            second.value.expect("second entry").value,
            b"confirmed".to_vec()
        );
        assert_eq!(shared.quorum_waves.load(Ordering::SeqCst), 1);
        assert_eq!(
            read_index_resident_preparations_for_root(&root),
            1,
            "the proof flight must be claimed before resident preparation"
        );
        assert_eq!(read_index_singleflight_waiters_for_root(&root), 0);
    }

    type TestEntryRead =
        Result<Option<sinomo_kv::KvAuthoritativeRead<Option<sinomo_kv::KvEntry>>>, NetworkError>;

    fn run_floored_read_index_pair(
        fixture: &ReadIndexFixture,
        mut waiter_state: ClusterBootstrapState,
        expected_quorum_waves: usize,
    ) -> (
        TestEntryRead,
        TestEntryRead,
        ClusterBootstrapState,
        ClusterBootstrapState,
        Arc<FlooredReadIndexHostState>,
    ) {
        let shared = Arc::new(FlooredReadIndexHostState::new());
        let mut owner_state = fixture.state.clone();
        let mut owner_host = FlooredReadIndexHost {
            shared: Arc::clone(&shared),
        };
        let mut waiter_host = FlooredReadIndexHost {
            shared: Arc::clone(&shared),
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let timeout = Duration::from_secs(2);
        let append_options = ClusterControlPlaneTimeouts::default_daemon_transport()
            .raft_append_replication_options(timeout);
        let root = fixture.paths.root.clone();

        let (owner, waiter, ()) = runtime.block_on(async {
            let owner = read_cluster_raft_with_read_index_async_with_host(
                runtime.handle(),
                &fixture.paths,
                &mut owner_state,
                timeout,
                append_options,
                |storage| storage.read_index_entry(TEST_KEY),
                &mut owner_host,
            );
            let waiter = async {
                shared
                    .pre_dispatch_entered
                    .acquire()
                    .await
                    .expect("owner reached its pre-dispatch fence")
                    .forget();
                read_cluster_raft_with_read_index_async_with_host(
                    runtime.handle(),
                    &fixture.paths,
                    &mut waiter_state,
                    timeout,
                    append_options,
                    |storage| storage.read_index_entry(TEST_KEY),
                    &mut waiter_host,
                )
                .await
            };
            let release = async {
                tokio::time::timeout(Duration::from_secs(1), async {
                    while read_index_singleflight_waiters_for_root(&root) != 1 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("floored waiter joined the owner's unsealed flight");
                shared.pre_dispatch_release.add_permits(1);
                for _ in 0..expected_quorum_waves {
                    tokio::time::timeout(Duration::from_secs(1), shared.wave_entered.acquire())
                        .await
                        .expect("expected floored ReadIndex wave did not dispatch")
                        .expect("floored ReadIndex wave entered")
                        .forget();
                    shared.wave_release.add_permits(1);
                }
            };
            tokio::join!(owner, waiter, release)
        });

        (owner, waiter, owner_state, waiter_state, shared)
    }

    #[test]
    fn term_nine_waiter_rejects_term_seven_shared_read_index_without_state_regression() {
        let fixture = read_index_fixture(7, 7);
        let mut waiter_state = fixture.state.clone();
        waiter_state.term = 9;
        let root = fixture.paths.root.clone();
        let (owner, waiter, owner_state, waiter_state, shared) =
            run_floored_read_index_pair(&fixture, waiter_state, 1);

        assert!(owner.expect("term-seven owner ReadIndex").is_some());
        let error = waiter.expect_err("term-nine waiter must not consume a term-seven proof");
        assert!(
            error
                .to_string()
                .contains("local Raft term 7 trails projected cluster term 9"),
            "unexpected waiter error: {error}"
        );
        assert_eq!(owner_state.term, 7);
        assert_eq!(waiter_state.term, 9, "shared state must never lower term");
        assert_eq!(shared.quorum_waves.load(Ordering::SeqCst), 1);
        assert_eq!(read_index_resident_preparations_for_root(&root), 2);
        assert_eq!(read_index_singleflight_waiters_for_root(&root), 0);
    }

    #[test]
    fn newer_same_membership_generation_rejects_older_shared_read_index_aba() {
        let fixture = read_index_fixture(7, 7);
        let durable_revision =
            LocalClusterRaftCommandStorage::new(&fixture.paths, fixture.local.clone())
                .read_index_state()
                .expect("durable ReadIndex state")
                .revision
                .0;
        let mut waiter_state = fixture.state.clone();
        waiter_state.kv_revision = durable_revision.saturating_add(1);
        let root = fixture.paths.root.clone();
        let (owner, waiter, _owner_state, waiter_state, shared) =
            run_floored_read_index_pair(&fixture, waiter_state, 1);

        assert!(owner.expect("older-generation owner ReadIndex").is_some());
        let error = waiter.expect_err(
            "same-membership waiter must reject a proof from an older configuration generation",
        );
        assert!(
            error.to_string().contains(&format!(
                "local Raft revision {durable_revision} trails caller-observed cluster revision {}",
                durable_revision.saturating_add(1)
            )),
            "unexpected ABA waiter error: {error}"
        );
        assert_eq!(
            waiter_state.kv_revision,
            durable_revision.saturating_add(1),
            "shared state must never lower the caller's configuration generation"
        );
        assert_eq!(shared.quorum_waves.load(Ordering::SeqCst), 1);
        assert_eq!(read_index_resident_preparations_for_root(&root), 2);
        assert_eq!(read_index_singleflight_waiters_for_root(&root), 0);
    }

    #[test]
    fn different_membership_waiter_establishes_its_own_read_index_wave() {
        let fixture = read_index_fixture(7, 7);
        let mut waiter_state = fixture.state.clone();
        waiter_state.members[2].state = sinomo_base::ClusterMemberState::NonVoting;
        let root = fixture.paths.root.clone();
        let (owner, waiter, _owner_state, waiter_state, shared) =
            run_floored_read_index_pair(&fixture, waiter_state, 2);

        assert!(owner.expect("authoritative owner ReadIndex").is_some());
        assert!(
            waiter.expect("different-config waiter ReadIndex").is_some(),
            "the waiter should rebase from durable configuration in its own wave"
        );
        assert_eq!(
            sinomo_kv::cluster_kv_raft_voter_ids(&waiter_state)
                .expect("waiter voters")
                .into_iter()
                .collect::<BTreeSet<_>>(),
            fixture
                .initial_config
                .voters
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>(),
            "the waiter's own proof may adopt only the durable exact configuration"
        );
        assert_eq!(shared.quorum_waves.load(Ordering::SeqCst), 2);
        assert_eq!(read_index_resident_preparations_for_root(&root), 2);
        assert_eq!(read_index_singleflight_waiters_for_root(&root), 0);
    }

    #[test]
    fn divergent_member_identity_waiter_cannot_reuse_shared_read_index_proof() {
        let fixture = read_index_fixture(7, 7);
        let divergent_node_id = fixture.state.members[1].node_id.clone();
        let durable_public_key = fixture.state.members[1].node_public_key.clone();
        let mut waiter_state = fixture.state.clone();
        waiter_state.members[1].node_public_key = "pk-divergent-waiter".to_string();
        let root = fixture.paths.root.clone();
        let (owner, waiter, _owner_state, waiter_state, shared) =
            run_floored_read_index_pair(&fixture, waiter_state, 2);

        assert!(owner.expect("authoritative owner ReadIndex").is_some());
        assert!(
            waiter
                .expect("identity-divergent waiter ReadIndex")
                .is_some(),
            "the waiter must establish its own resident-validated proof"
        );
        assert_eq!(
            waiter_state
                .members
                .iter()
                .find(|member| member.node_id == divergent_node_id)
                .expect("independently adopted member")
                .node_public_key,
            durable_public_key,
            "the independent proof must adopt the committed member identity"
        );
        assert_eq!(shared.quorum_waves.load(Ordering::SeqCst), 2);
        assert_eq!(read_index_resident_preparations_for_root(&root), 2);
        assert_eq!(read_index_singleflight_waiters_for_root(&root), 0);
    }

    #[test]
    fn invalid_cluster_owner_cannot_poison_valid_singleflight_waiter() {
        let fixture = read_index_fixture(7, 7);
        let mut invalid_owner_state = fixture.state.clone();
        invalid_owner_state.cluster_id =
            ClusterId::from_legacy_named("cluster_read_index_invalid_owner")
                .expect("invalid owner cluster id");
        let valid_waiter_state = fixture.state.clone();
        let shared = Arc::new(CoalescedReadIndexHostState {
            quorum_waves: AtomicUsize::new(0),
            wave_entered: tokio::sync::Semaphore::new(0),
            wave_release: tokio::sync::Semaphore::new(0),
        });
        let root = fixture.paths.root.clone();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .expect("runtime");
        let (blocker_started_tx, blocker_started_rx) = std::sync::mpsc::sync_channel(0);
        let (release_blocker_tx, release_blocker_rx) = std::sync::mpsc::sync_channel(0);
        let blocker = runtime.spawn_blocking(move || {
            blocker_started_tx.send(()).expect("blocker started");
            release_blocker_rx.recv().expect("release blocker");
        });
        blocker_started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("blocking pool saturated");

        let (owner, waiter) = runtime.block_on(async {
            let owner_paths = fixture.paths.clone();
            let owner_shared = Arc::clone(&shared);
            let owner = tokio::spawn(async move {
                let mut state = invalid_owner_state;
                let timeout = Duration::from_secs(5);
                let append_options = ClusterControlPlaneTimeouts::default_daemon_transport()
                    .raft_append_replication_options(timeout);
                let mut host = CoalescedReadIndexHost {
                    shared: owner_shared,
                };
                read_cluster_raft_with_read_index_async_with_host(
                    &tokio::runtime::Handle::current(),
                    &owner_paths,
                    &mut state,
                    timeout,
                    append_options,
                    |storage| storage.read_index_entry(TEST_KEY),
                    &mut host,
                )
                .await
            });

            tokio::time::timeout(Duration::from_secs(1), async {
                while read_index_singleflight_flights_for_root(&root) != 1 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("invalid caller claimed the shared flight");

            let waiter_paths = fixture.paths.clone();
            let waiter_shared = Arc::clone(&shared);
            let waiter = tokio::spawn(async move {
                let mut state = valid_waiter_state;
                let timeout = Duration::from_secs(5);
                let append_options = ClusterControlPlaneTimeouts::default_daemon_transport()
                    .raft_append_replication_options(timeout);
                let mut host = CoalescedReadIndexHost {
                    shared: waiter_shared,
                };
                read_cluster_raft_with_read_index_async_with_host(
                    &tokio::runtime::Handle::current(),
                    &waiter_paths,
                    &mut state,
                    timeout,
                    append_options,
                    |storage| storage.read_index_entry(TEST_KEY),
                    &mut host,
                )
                .await
            });

            tokio::time::timeout(Duration::from_secs(1), async {
                while read_index_singleflight_waiters_for_root(&root) != 1 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("valid caller joined the invalid owner's flight");
            release_blocker_tx.send(()).expect("release blocking pool");
            shared
                .wave_entered
                .acquire()
                .await
                .expect("valid waiter dispatched an independent ReadIndex wave")
                .forget();
            shared.wave_release.add_permits(1);

            (
                owner.await.expect("join invalid owner"),
                waiter.await.expect("join valid waiter"),
            )
        });
        runtime
            .block_on(blocker)
            .expect("join blocking-pool blocker");

        let owner_error = owner.expect_err("invalid owner projection must fail closed");
        assert!(
            owner_error.to_string().contains("different cluster"),
            "unexpected invalid-owner error: {owner_error}"
        );
        assert!(
            waiter.expect("valid waiter ReadIndex").is_some(),
            "the valid waiter must reclaim and establish its own proof"
        );
        assert_eq!(shared.quorum_waves.load(Ordering::SeqCst), 1);
        assert_eq!(read_index_resident_preparations_for_root(&root), 2);
        assert_eq!(read_index_singleflight_waiters_for_root(&root), 0);
        assert_eq!(read_index_singleflight_flights_for_root(&root), 0);
    }

    #[test]
    fn reader_started_after_read_index_dispatch_uses_a_later_proof_wave() {
        let fixture = read_index_fixture(7, 7);
        let shared = Arc::new(CoalescedReadIndexHostState {
            quorum_waves: AtomicUsize::new(0),
            wave_entered: tokio::sync::Semaphore::new(0),
            wave_release: tokio::sync::Semaphore::new(0),
        });
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let root = fixture.paths.root.clone();

        let (first, late) = runtime.block_on(async {
            let first_paths = fixture.paths.clone();
            let mut first_state = fixture.state.clone();
            let first_shared = Arc::clone(&shared);
            let first = tokio::spawn(async move {
                let timeout = Duration::from_secs(2);
                let append_options = ClusterControlPlaneTimeouts::default_daemon_transport()
                    .raft_append_replication_options(timeout);
                let mut host = CoalescedReadIndexHost {
                    shared: first_shared,
                };
                read_cluster_raft_with_read_index_async_with_host(
                    &tokio::runtime::Handle::current(),
                    &first_paths,
                    &mut first_state,
                    timeout,
                    append_options,
                    |storage| storage.read_index_entry(TEST_KEY),
                    &mut host,
                )
                .await
            });

            shared
                .wave_entered
                .acquire()
                .await
                .expect("first ReadIndex heartbeat dispatched")
                .forget();

            // This invocation begins after the first wave has sealed at its
            // dispatch edge, so consuming the first proof would give it a
            // linearization point before invocation.
            let late_paths = fixture.paths.clone();
            let mut late_state = fixture.state.clone();
            let late_shared = Arc::clone(&shared);
            let late = tokio::spawn(async move {
                let timeout = Duration::from_secs(2);
                let append_options = ClusterControlPlaneTimeouts::default_daemon_transport()
                    .raft_append_replication_options(timeout);
                let mut host = CoalescedReadIndexHost {
                    shared: late_shared,
                };
                read_cluster_raft_with_read_index_async_with_host(
                    &tokio::runtime::Handle::current(),
                    &late_paths,
                    &mut late_state,
                    timeout,
                    append_options,
                    |storage| storage.read_index_entry(TEST_KEY),
                    &mut host,
                )
                .await
            });

            tokio::time::timeout(Duration::from_secs(1), async {
                while shared.quorum_waves.load(Ordering::SeqCst) != 2 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("late reader dispatched a distinct proof wave");
            shared.wave_release.add_permits(2);

            (
                first.await.expect("first reader task"),
                late.await.expect("late reader task"),
            )
        });

        assert!(first.expect("first ReadIndex").is_some());
        assert!(late.expect("late ReadIndex").is_some());
        assert_eq!(shared.quorum_waves.load(Ordering::SeqCst), 2);
        assert_eq!(read_index_resident_preparations_for_root(&root), 2);
    }

    fn assert_read_index_waiter_reclaims_failed_owner(first_wave: ReclaimFirstWave) {
        let fixture = read_index_fixture(7, 7);
        let shared = Arc::new(ReclaimReadIndexHostState {
            quorum_waves: AtomicUsize::new(0),
            pre_dispatch_callbacks: AtomicUsize::new(0),
            pre_dispatch_entered: tokio::sync::Semaphore::new(0),
            pre_dispatch_release: tokio::sync::Semaphore::new(0),
            first_wave_entered: tokio::sync::Semaphore::new(0),
            fail_first_wave: tokio::sync::Semaphore::new(0),
            first_wave,
        });
        let root = fixture.paths.root.clone();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        let read = runtime.block_on(async {
            let owner_paths = fixture.paths.clone();
            let mut owner_state = fixture.state.clone();
            let owner_shared = Arc::clone(&shared);
            let owner = tokio::spawn(async move {
                let timeout = Duration::from_secs(1);
                let append_options = ClusterControlPlaneTimeouts::default_daemon_transport()
                    .raft_append_replication_options(timeout);
                let mut host = ReclaimReadIndexHost {
                    shared: owner_shared,
                };
                read_cluster_raft_with_read_index_async_with_host(
                    &tokio::runtime::Handle::current(),
                    &owner_paths,
                    &mut owner_state,
                    timeout,
                    append_options,
                    |storage| storage.read_index_entry(TEST_KEY),
                    &mut host,
                )
                .await
            });

            shared
                .pre_dispatch_entered
                .acquire()
                .await
                .expect("first ReadIndex owner reached its dispatch edge")
                .forget();

            let waiter_paths = fixture.paths.clone();
            let mut waiter_state = fixture.state.clone();
            let waiter_shared = Arc::clone(&shared);
            let waiter = tokio::spawn(async move {
                let timeout = Duration::from_secs(5);
                let append_options = ClusterControlPlaneTimeouts::default_daemon_transport()
                    .raft_append_replication_options(timeout);
                let mut host = ReclaimReadIndexHost {
                    shared: waiter_shared,
                };
                read_cluster_raft_with_read_index_async_with_host(
                    &tokio::runtime::Handle::current(),
                    &waiter_paths,
                    &mut waiter_state,
                    timeout,
                    append_options,
                    |storage| storage.read_index_entry(TEST_KEY),
                    &mut host,
                )
                .await
            });

            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    if read_index_singleflight_waiters_for_root(&root) == 1 {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("long-budget reader joined the first owner's flight");

            // Seal the now-populated cohort, then wait until the first network
            // wave has actually entered before cancelling/failing its owner.
            shared.pre_dispatch_release.add_permits(1);
            shared
                .first_wave_entered
                .acquire()
                .await
                .expect("first ReadIndex wave entered")
                .forget();

            match first_wave {
                ReclaimFirstWave::Cancelled => {
                    owner.abort();
                    let error = owner.await.expect_err("short owner was aborted");
                    assert!(error.is_cancelled());
                }
                ReclaimFirstWave::TimedOut => {
                    shared.fail_first_wave.add_permits(1);
                    let error = owner
                        .await
                        .expect("short owner task")
                        .expect_err("first ReadIndex wave timed out");
                    assert!(matches!(error, NetworkError::Timeout { .. }));
                }
            }

            tokio::time::timeout(Duration::from_secs(2), waiter)
                .await
                .expect("long-budget reader reclaimed the flight")
                .expect("long-budget reader task")
                .expect("reclaimed ReadIndex")
                .expect("authoritative read")
        });

        assert_eq!(read.value.expect("entry").value, b"confirmed".to_vec());
        assert_eq!(shared.quorum_waves.load(Ordering::SeqCst), 2);
        assert_eq!(read_index_singleflight_waiters_for_root(&root), 0);
    }

    #[test]
    fn long_budget_read_index_waiter_reclaims_cancelled_owner() {
        assert_read_index_waiter_reclaims_failed_owner(ReclaimFirstWave::Cancelled);
    }

    #[test]
    fn long_budget_read_index_waiter_reclaims_timed_out_owner() {
        assert_read_index_waiter_reclaims_failed_owner(ReclaimFirstWave::TimedOut);
    }

    #[test]
    fn read_index_falls_back_before_current_term_has_a_commit() {
        let mut fixture = read_index_fixture(8, 7);
        let mut host = no_action_host(&fixture);
        let read = run_read_index(&mut fixture, &mut host, |storage| {
            storage.read_index_entry(TEST_KEY)
        })
        .expect("fallback decision");

        assert!(read.is_none());
        assert_eq!(host.sends, 0);
    }

    #[test]
    fn read_index_observes_higher_term_and_rejects_read() {
        let mut fixture = read_index_fixture(7, 7);
        let mut host = no_action_host(&fixture);
        host.action = Some(ReadIndexAction::HigherTerm(9));
        let error = run_read_index(&mut fixture, &mut host, |storage| {
            storage.read_index_entry(TEST_KEY)
        })
        .expect_err("higher term must reject ReadIndex");

        assert!(error.to_string().contains("higher term 9"));
        wait_for_higher_term_persistence(&fixture.paths, &fixture.local, 9);
        let node = fixture
            .paths
            .kv_paths()
            .raft_store()
            .open_node(&fixture.local)
            .expect("reopen node");
        assert_eq!(node.hard_state().current_term, 9);
        assert_eq!(node.hard_state().leader_id, None);
    }

    #[test]
    fn synchronous_read_index_rejects_higher_term_before_contended_persistence() {
        let mut fixture = read_index_fixture(7, 7);
        let mut operation_guard = None;
        let started = Instant::now();
        let result = read_cluster_raft_with_read_index_attempt_with_transport(
            &fixture.paths,
            &mut fixture.state,
            Instant::now() + Duration::from_secs(2),
            ClusterControlPlaneTimeouts::default_daemon_transport()
                .raft_append_replication_options(Duration::from_secs(2)),
            ClusterRaftReadProjectionPolicy::DeadlineBounded,
            |_storage| -> Result<Option<sinomo_kv::KvEntry>, NetworkError> {
                panic!("a higher-term rejection must not project a value")
            },
            |state, targets, request, _timeout, required_acks| {
                assert_eq!(required_acks, 1);
                let target = targets.into_iter().next().expect("remote voter");
                operation_guard = Some(
                    fixture
                        .paths
                        .kv_paths()
                        .raft_store()
                        .lock_operation()
                        .expect("hold term persistence admission"),
                );
                Ok(vec![(
                    target,
                    Ok(ClusterRaftAppendResponse {
                        accepted: false,
                        detail: "test higher term".to_string(),
                        state: state.clone(),
                        append: sinomo_kv::RaftAppendEntriesResponse {
                            term: request.append.term.saturating_add(2),
                            success: false,
                            match_index: request.append.prev_log_index,
                        },
                    }),
                )])
            },
        );
        let elapsed = started.elapsed();

        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("higher term must reject synchronous ReadIndex"),
        };
        assert!(error.to_string().contains("higher term 9"));
        assert!(
            elapsed < Duration::from_millis(500),
            "ReadIndex rejection waited behind term persistence for {elapsed:?}"
        );
        assert_eq!(fixture.state.term, 9);
        assert_eq!(
            fixture
                .paths
                .kv_paths()
                .raft_store()
                .pending_higher_term(&fixture.local)
                .expect("pending higher term"),
            Some(9),
            "prompt rejection must retain the fail-closed authority floor"
        );

        drop(operation_guard.take());
        wait_for_higher_term_persistence(&fixture.paths, &fixture.local, 9);
    }

    #[test]
    fn read_index_rejects_final_leadership_change() {
        let mut fixture = read_index_fixture(7, 7);
        let mut host = no_action_host(&fixture);
        host.action = Some(ReadIndexAction::ClearLeader);
        let error = run_read_index(&mut fixture, &mut host, |storage| {
            storage.read_index_entry(TEST_KEY)
        })
        .expect_err("leadership change must reject ReadIndex");

        assert!(error.to_string().contains("leadership changed"));
    }

    #[test]
    fn read_index_rejects_two_config_transitions_even_if_voter_set_returns() {
        let mut fixture = read_index_fixture(7, 7);
        let mut intermediate = fixture.initial_config.clone();
        intermediate.kv_revision = 3;
        intermediate.voters = vec![
            fixture.state.members[0].node_id.clone(),
            fixture.state.members[3].node_id.clone(),
            fixture.state.members[4].node_id.clone(),
        ];
        intermediate.non_voting = vec![
            fixture.state.members[1].node_id.clone(),
            fixture.state.members[2].node_id.clone(),
        ];
        let mut final_config = fixture.initial_config.clone();
        final_config.kv_revision = 4;
        let mut host = no_action_host(&fixture);
        host.action = Some(ReadIndexAction::ChangeConfigTwice {
            intermediate,
            final_config,
        });

        let error = run_read_index(&mut fixture, &mut host, |storage| {
            storage.read_index_entry(TEST_KEY)
        })
        .expect_err("config generation change must reject ReadIndex");
        assert!(error.to_string().contains("configuration changed"));
    }

    #[test]
    fn read_index_proof_covers_concurrent_committed_request_in_final_view() {
        let mut fixture = read_index_fixture(7, 7);
        let concurrent_request = KvCommandRequest::generated(KvCommand::Op(KvOp::Put {
            key: "/read-index/concurrent".to_string(),
            value: b"later".to_vec(),
            lease_id: None,
        }));
        let concurrent_request_id = concurrent_request.request_id.clone();
        let mut host = no_action_host(&fixture);
        host.action = Some(ReadIndexAction::CommitRequest(concurrent_request));
        let read = run_read_index(&mut fixture, &mut host, |storage| {
            storage.read_index_snapshot()
        })
        .expect("ReadIndex")
        .expect("authoritative snapshot");
        let record = read
            .value
            .committed_requests
            .iter()
            .find(|record| record.request_id == concurrent_request_id)
            .cloned()
            .expect("concurrent committed request");

        assert!(read.authority.commit_index >= record.log_index);
        assert!(read.authority.revision >= record.revision);
        validate_cluster_kv_request_outcome_read(&ClusterKvRequestOutcomeRead {
            authority: read.authority,
            outcome: KvRequestOutcome::Committed(record),
        })
        .expect("proof covers final coherent request outcome");
    }
}
