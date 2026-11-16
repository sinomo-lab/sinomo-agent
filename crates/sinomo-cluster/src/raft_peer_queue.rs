//! Shared follower sends. Compatible overlapping tails share one durable ACK;
//! callers retain independent deadlines and no completed ACK is cached.
use sinomo_kv::{ClusterRaftAppendRequest, ClusterRaftAppendResponse};
use sinomo_network::{ClusterPaths, NetworkDeliveryState, NetworkError, NetworkTimeoutPhase};
use sinomo_node::NodeId;
use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, LazyLock, Mutex, Weak},
    time::{Duration, Instant},
};
use tokio::sync::oneshot;

type Reply = Result<ClusterRaftAppendResponse, NetworkError>;
type SendAppend = Box<dyn FnOnce(Duration) -> Pin<Box<dyn Future<Output = Reply> + Send>> + Send>;
const MAX_PENDING: usize = 256;
const MAX_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;
struct Waiter {
    reply: oneshot::Sender<Reply>,
}
struct Batch {
    request: Arc<ClusterRaftAppendRequest>,
    send: Option<SendAppend>,
    deadline: Instant,
    bytes: usize,
    waiters: Vec<Waiter>,
}
#[derive(Default)]
struct PeerQueue {
    pending: VecDeque<Batch>,
    active: Option<Batch>,
    waiters: usize,
    bytes: usize,
    running: bool,
}
type Queue = Arc<Mutex<PeerQueue>>;
type PeerKey = (PathBuf, NodeId, NodeId);
static PEERS: LazyLock<Mutex<HashMap<PeerKey, Weak<Mutex<PeerQueue>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

struct WorkerLease {
    queue: Queue,
    armed: bool,
}
impl WorkerLease {
    fn disarm(&mut self) {
        self.armed = false;
    }
}
impl Drop for WorkerLease {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut q = self.queue.lock().unwrap_or_else(|e| e.into_inner());
        q.active.take();
        q.pending.clear();
        q.bytes = 0;
        q.waiters = 0;
        q.running = false;
    }
}

fn queue_for(paths: &ClusterPaths, local: &NodeId, target: &NodeId) -> Queue {
    let key = (paths.root.clone(), local.clone(), target.clone());
    let mut peers = PEERS.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(q) = peers.get(&key).and_then(Weak::upgrade) {
        return q;
    }
    peers.retain(|_, q| q.strong_count() > 0);
    let q = Arc::new(Mutex::new(PeerQueue::default()));
    peers.insert(key, Arc::downgrade(&q));
    q
}

fn payload_bytes(request: &ClusterRaftAppendRequest) -> usize {
    request
        .append
        .entries
        .iter()
        .map(sinomo_kv::cluster_kv_raft_entry_group_bytes)
        .fold(0usize, usize::saturating_add)
        .saturating_add(256)
}

/// Only an actual extension of the same fenced history can cover a waiter.
/// ReadIndex probes require a fresh confirmation of their own read fence.
fn covers(sent: &ClusterRaftAppendRequest, wanted: &ClusterRaftAppendRequest) -> bool {
    if sent.read_index_probe
        || wanted.read_index_probe
        || sent.snapshot.is_some()
        || wanted.snapshot.is_some()
        // KV cursors and publication timestamps are observations, not changes
        // to the membership/identity fence. Comparing the complete state made
        // ordinary committed progress split one covering tail into serial RPCs.
        || !sinomo_node::cluster_bootstrap_state_stable_eq(&sent.state, &wanted.state)
        || sent.append.term != wanted.append.term
        || sent.append.leader_id != wanted.append.leader_id
        || sent.append.leader_commit < wanted.append.leader_commit
        || sent.append.prev_log_index > wanted.append.prev_log_index
    {
        return false;
    }
    let offset = wanted.append.prev_log_index - sent.append.prev_log_index;
    let Ok(offset) = usize::try_from(offset) else {
        return false;
    };
    let previous_term = if offset == 0 {
        Some(sent.append.prev_log_term)
    } else {
        sent.append.entries.get(offset - 1).map(|e| e.term)
    };
    previous_term == Some(wanted.append.prev_log_term)
        && sent
            .append
            .entries
            .get(offset..offset.saturating_add(wanted.append.entries.len()))
            == Some(wanted.append.entries.as_slice())
}

/// The owned send survives the cancellation of a foreground quorum waiter.
/// `send` must route through the normal authenticated, membership-fenced RPC.
pub async fn send_shared_cluster_raft_append<F, Fut>(
    paths: &ClusterPaths,
    target: &NodeId,
    request: Arc<ClusterRaftAppendRequest>,
    timeout: Duration,
    send: F,
) -> Reply
where
    F: FnOnce(Duration) -> Fut + Send + 'static,
    Fut: Future<Output = Reply> + Send + 'static,
{
    // Large payloads and read barriers retain their dedicated path. The shared
    // queue's byte bound never weakens the existing RPC payload contract.
    let bytes = payload_bytes(&request);
    if request.read_index_probe || request.snapshot.is_some() || bytes > MAX_PAYLOAD_BYTES {
        return send(timeout).await;
    }
    let started = Instant::now();
    let deadline = started.checked_add(timeout).unwrap_or(started);
    let queue = queue_for(paths, &request.state.local_node_id, target);
    let (reply, receive) = oneshot::channel();
    let mut incoming = Batch {
        request,
        send: Some(Box::new(move |timeout| Box::pin(send(timeout)))),
        deadline,
        bytes,
        waiters: vec![Waiter { reply }],
    };
    let start = {
        let mut q = queue.lock().unwrap_or_else(|e| e.into_inner());
        if q.waiters >= MAX_PENDING {
            return Err(NetworkError::ObjectRpcRouteSaturated {
                route: sinomo_kv::CLUSTER_RAFT_APPEND_ROUTE.into(),
                concurrency_limit: MAX_PENDING,
            });
        }
        let mut merged = false;
        if let Some(active) = q.active.as_mut()
            && covers(&active.request, &incoming.request)
        {
            active.waiters.append(&mut incoming.waiters);
            merged = true;
        }
        if !merged {
            for pending in &mut q.pending {
                if covers(&pending.request, &incoming.request) {
                    pending.deadline = pending.deadline.max(incoming.deadline);
                    pending.waiters.append(&mut incoming.waiters);
                    merged = true;
                    break;
                }
            }
        }
        if !merged {
            let covered_bytes = q
                .pending
                .iter()
                .filter(|p| covers(&incoming.request, &p.request))
                .map(|p| p.bytes)
                .fold(0usize, usize::saturating_add);
            let retained_bytes = q.bytes.saturating_sub(covered_bytes);
            if retained_bytes.saturating_add(bytes) > MAX_PAYLOAD_BYTES {
                return Err(NetworkError::ObjectRpcRouteSaturated {
                    route: sinomo_kv::CLUSTER_RAFT_APPEND_ROUTE.into(),
                    concurrency_limit: MAX_PENDING,
                });
            }
            // Prefer the newest covering tail while retaining all independent
            // waiters. Incompatible histories stay separate and cannot share ACKs.
            let mut remaining = VecDeque::new();
            while let Some(mut pending) = q.pending.pop_front() {
                if covers(&incoming.request, &pending.request) {
                    q.bytes = q.bytes.saturating_sub(pending.bytes);
                    metrics::counter!("sinomo.kv.replication.shared.coalesced")
                        .increment(pending.waiters.len() as u64);
                    incoming.deadline = incoming.deadline.max(pending.deadline);
                    incoming.waiters.append(&mut pending.waiters);
                } else {
                    remaining.push_back(pending);
                }
            }
            q.pending = remaining;
            q.bytes = q.bytes.saturating_add(bytes);
            q.pending.push_back(incoming);
        } else {
            metrics::counter!("sinomo.kv.replication.shared.coalesced").increment(1);
        }
        q.waiters += 1;
        let start = !q.running;
        q.running = true;
        start
    };
    metrics::counter!("sinomo.kv.replication.shared.requests").increment(1);
    if start {
        let worker = Arc::clone(&queue);
        let paths = paths.clone();
        let mut lease = WorkerLease {
            queue: Arc::clone(&worker),
            armed: true,
        };
        tokio::spawn(async move {
            loop {
                let (request, send, deadline) = {
                    let mut q = worker.lock().unwrap_or_else(|e| e.into_inner());
                    let Some(mut batch) = q.pending.pop_front() else {
                        q.running = false;
                        lease.disarm();
                        return;
                    };
                    let send = batch
                        .send
                        .take()
                        .expect("queued send is owned exactly once");
                    let request = Arc::clone(&batch.request);
                    let deadline = batch.deadline;
                    q.active = Some(batch);
                    (request, send, deadline)
                };
                let remaining = deadline.saturating_duration_since(Instant::now());
                let started = Instant::now();
                let mut result = if remaining.is_zero() {
                    Err(timeout_error(Duration::ZERO))
                } else {
                    metrics::counter!("sinomo.kv.replication.shared.sends").increment(1);
                    metrics::histogram!("sinomo.kv.replication.shared.entries")
                        .record(request.append.entries.len() as f64);
                    tokio::time::timeout(remaining, send(remaining))
                        .await
                        .unwrap_or_else(|_| Err(timeout_error(remaining)))
                };
                // Term observation belongs to the send owner even when every
                // original caller has stopped waiting after another peer ACKed.
                if let Ok(response) = &result
                    && response.append.term > request.append.term
                    && let Err(error) =
                        crate::raft_quorum::publish_cluster_raft_higher_term_reactively(
                            &paths,
                            &request.state.local_node_id,
                            response.append.term,
                        )
                {
                    result = Err(error);
                }
                metrics::histogram!("sinomo.kv.replication.shared.send.us")
                    .record(started.elapsed().as_micros() as f64);
                let batch = {
                    let mut q = worker.lock().unwrap_or_else(|e| e.into_inner());
                    let batch = q.active.take().expect("active send retains its waiters");
                    q.bytes = q.bytes.saturating_sub(batch.bytes);
                    q.waiters = q.waiters.saturating_sub(batch.waiters.len());
                    batch
                };
                for waiter in batch.waiters {
                    let _ = waiter.reply.send(result.clone());
                }
            }
        });
    }
    let result = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), receive).await;
    metrics::histogram!("sinomo.kv.replication.shared.wait.us")
        .record(started.elapsed().as_micros() as f64);
    match result {
        Ok(Ok(result)) => result,
        Ok(Err(_)) => Err(NetworkError::Io {
            operation: "shared Raft append".into(),
            detail: "follower send owner stopped".into(),
        }),
        Err(_) => Err(timeout_error(timeout)),
    }
}
fn timeout_error(timeout: Duration) -> NetworkError {
    NetworkError::timeout(
        "shared Raft append",
        NetworkTimeoutPhase::Overall,
        timeout,
        NetworkDeliveryState::PossiblyApplied,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_base::{ClusterBootstrapState, Id};
    use sinomo_kv::{
        KvCommand, KvCommandRequest, KvLogEntry, KvOp, KvRevision, RaftAppendEntriesRequest,
        RaftAppendEntriesResponse,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    fn fixture(
        name: &str,
    ) -> (
        sinomo_testdir::TestDir,
        ClusterPaths,
        NodeId,
        ClusterRaftAppendRequest,
    ) {
        let root = sinomo_testdir::TestDir::new(name);
        let paths = ClusterPaths::new(&sinomo_local::SinomoDataDir::new(&root));
        let local = NodeId::from_legacy_named("shared_leader").unwrap();
        let target = NodeId::from_legacy_named("shared_follower").unwrap();
        let mut state = ClusterBootstrapState::initialized_locally(
            Id::from_legacy_named("shared_cluster").unwrap(),
            "shared",
            local.clone(),
            "local",
            "pk",
            time::OffsetDateTime::UNIX_EPOCH,
        );
        state.term = 7;
        let entries = (1..=64)
            .map(|index| {
                let request = KvCommandRequest::generated(KvCommand::Op(KvOp::Put {
                    key: format!("/shared/{index}"),
                    value: vec![1],
                    lease_id: None,
                }));
                KvLogEntry {
                    index,
                    term: 7,
                    request_id: request.request_id,
                    command: request.command,
                    revision: KvRevision::ZERO,
                }
            })
            .collect();
        let request = ClusterRaftAppendRequest {
            state,
            append: RaftAppendEntriesRequest {
                term: 7,
                leader_id: local,
                prev_log_index: 0,
                prev_log_term: 0,
                entries,
                leader_commit: 0,
            },
            snapshot: None,
            read_index_probe: false,
        };
        (root, paths, target, request)
    }
    fn accepted(request: &ClusterRaftAppendRequest) -> Reply {
        Ok(ClusterRaftAppendResponse {
            accepted: true,
            detail: "durable".into(),
            state: request.state.clone(),
            append: RaftAppendEntriesResponse {
                term: request.append.term,
                success: true,
                match_index: request
                    .append
                    .entries
                    .last()
                    .map_or(request.append.prev_log_index, |e| e.index),
            },
            read_index_gap: None,
        })
    }
    async fn wait_for_waiters(queue: &Queue, count: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if queue.lock().unwrap().waiters == count {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    #[test]
    fn coverage_requires_exact_history_membership_and_commit_fences() {
        let (_root, _paths, _target, full) = fixture("shared_coverage");
        let mut small = full.clone();
        small.append.entries.truncate(4);
        assert!(covers(&full, &small));
        assert!(!covers(&small, &full));
        let mut suffix = full.clone();
        suffix.append.prev_log_index = 3;
        suffix.append.prev_log_term = 7;
        suffix.append.entries.drain(..3);
        assert!(covers(&full, &suffix));
        for mutation in 0..8 {
            let mut changed = small.clone();
            match mutation {
                0 => changed.append.term += 1,
                1 => changed.append.entries[0].request_id = sinomo_kv::KvRequestId::generate(),
                2 => changed.state.members.clear(),
                3 => changed.append.leader_commit = 1,
                4 => changed.read_index_probe = true,
                5 => changed.append.prev_log_term = 9,
                6 => changed.state.term += 1,
                _ => changed.state.members[0].node_public_key.push('x'),
            }
            assert!(!covers(&full, &changed), "mutation {mutation}");
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shared_active_send_survives_first_caller_cancellation() {
        let (_root, paths, target, request) = fixture("shared_cancel");
        let request = Arc::new(request);
        let calls = Arc::new(AtomicUsize::new(0));
        let (entered, entry) = oneshot::channel();
        let (release, gate) = oneshot::channel();
        let p = paths.clone();
        let t = target.clone();
        let r = request.clone();
        let n = calls.clone();
        let response = accepted(&r);
        let first = tokio::spawn(async move {
            send_shared_cluster_raft_append(
                &p,
                &t,
                r,
                Duration::from_secs(5),
                move |_| async move {
                    n.fetch_add(1, Ordering::SeqCst);
                    let _ = entered.send(());
                    let _ = gate.await;
                    response
                },
            )
            .await
        });
        entry.await.unwrap();
        let mut waiters = Vec::new();
        for _ in 0..31 {
            let p = paths.clone();
            let t = target.clone();
            let r = request.clone();
            let n = calls.clone();
            let response = accepted(&r);
            waiters.push(tokio::spawn(async move {
                send_shared_cluster_raft_append(
                    &p,
                    &t,
                    r,
                    Duration::from_secs(5),
                    move |_| async move {
                        n.fetch_add(1, Ordering::SeqCst);
                        response
                    },
                )
                .await
            }));
        }
        let queue = queue_for(&paths, &request.state.local_node_id, &target);
        wait_for_waiters(&queue, 32).await;
        first.abort();
        let _ = first.await;
        release.send(()).unwrap();
        for waiter in waiters {
            assert_eq!(waiter.await.unwrap().unwrap().append.match_index, 64);
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn queued_extensions_with_new_observations_share_one_followup_send_and_all_acks() {
        let (_root, paths, target, full) = fixture("shared_extensions");
        let mut first_request = full.clone();
        first_request.append.entries.truncate(1);
        let calls = Arc::new(AtomicUsize::new(0));
        let (entered, entry) = oneshot::channel();
        let (release, gate) = oneshot::channel();
        let p = paths.clone();
        let t = target.clone();
        let n = calls.clone();
        let response = accepted(&first_request);
        let first = tokio::spawn(async move {
            send_shared_cluster_raft_append(
                &p,
                &t,
                Arc::new(first_request),
                Duration::from_secs(5),
                move |_| async move {
                    n.fetch_add(1, Ordering::SeqCst);
                    let _ = entered.send(());
                    let _ = gate.await;
                    response
                },
            )
            .await
        });
        entry.await.unwrap();
        let mut waiters = Vec::new();
        for len in 2..=64 {
            let p = paths.clone();
            let t = target.clone();
            let mut r = full.clone();
            r.append.entries.truncate(len);
            // Concurrent proposals observe newer committed cursors and
            // publication timestamps without changing their membership fence.
            r.state.kv_revision += len as u64;
            r.state.updated_at += time::Duration::microseconds(len as i64);
            let response = accepted(&r);
            let n = calls.clone();
            waiters.push(tokio::spawn(async move {
                send_shared_cluster_raft_append(
                    &p,
                    &t,
                    Arc::new(r),
                    Duration::from_secs(5),
                    move |_| async move {
                        n.fetch_add(1, Ordering::SeqCst);
                        response
                    },
                )
                .await
            }));
        }
        let queue = queue_for(&paths, &full.state.local_node_id, &target);
        wait_for_waiters(&queue, 64).await;
        assert_eq!(queue.lock().unwrap().pending.len(), 1);
        release.send(()).unwrap();
        first.await.unwrap().unwrap();
        for waiter in waiters {
            assert_eq!(waiter.await.unwrap().unwrap().append.match_index, 64);
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn timed_out_waiter_does_not_cancel_other_waiters_or_owner() {
        let (_root, paths, target, request) = fixture("shared_deadlines");
        let request = Arc::new(request);
        let (entered, entry) = oneshot::channel();
        let (release, gate) = oneshot::channel();
        let p = paths.clone();
        let t = target.clone();
        let r = request.clone();
        let response = accepted(&r);
        let first = tokio::spawn(async move {
            send_shared_cluster_raft_append(
                &p,
                &t,
                r,
                Duration::from_secs(5),
                move |_| async move {
                    let _ = entered.send(());
                    let _ = gate.await;
                    response
                },
            )
            .await
        });
        entry.await.unwrap();
        let result = send_shared_cluster_raft_append(
            &paths,
            &target,
            request,
            Duration::from_millis(20),
            move |_| async { panic!("must share active send") },
        )
        .await;
        assert!(result.is_err());
        release.send(()).unwrap();
        assert!(first.await.unwrap().unwrap().accepted);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_owner_persists_higher_term_after_every_waiter_cancels() {
        use crate::kv_local_store::ClusterKvPathsExt;
        let (_root, paths, target, request) = fixture("shared_cancelled_higher_term");
        paths
            .kv_paths()
            .raft_store()
            .open_node(&request.state.local_node_id)
            .unwrap()
            .become_leader(7)
            .unwrap();
        let local = request.state.local_node_id.clone();
        let (entered, entry) = oneshot::channel();
        let (release, gate) = oneshot::channel();
        let mut response = accepted(&request).unwrap();
        response.accepted = false;
        response.append.success = false;
        response.append.term = 8;
        let p = paths.clone();
        let caller = tokio::spawn(async move {
            send_shared_cluster_raft_append(
                &p,
                &target,
                Arc::new(request),
                Duration::from_secs(5),
                move |_| async move {
                    let _ = entered.send(());
                    let _ = gate.await;
                    Ok(response)
                },
            )
            .await
        });
        entry.await.unwrap();
        caller.abort();
        let _ = caller.await;
        release.send(()).unwrap();
        let store = paths.kv_paths().raft_store();
        let wal_paths = store.wal_paths(&local);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                // Observe durable state without repeatedly checking out the
                // resident node needed by the send owner's try-admission worker.
                let hard_state = wal_paths.read_persisted_hard_state().unwrap();
                if hard_state.current_term == 8
                    && store.pending_higher_term(&local).unwrap().is_none()
                {
                    assert!(hard_state.leader_id.is_none());
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}
