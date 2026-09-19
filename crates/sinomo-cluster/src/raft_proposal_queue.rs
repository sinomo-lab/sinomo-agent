//! Operation-owned batching: one bounded queue per local Raft node, no idle loop.
use crate::kv_local_store::ClusterKvPathsExt;
use sinomo_base::ClusterBootstrapState;
use sinomo_kv::{ClusterKvRaftPreparedProposal, ClusterKvRaftProposal, KvCommandRequest, KvError};
use sinomo_network::ClusterPaths;
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex, Weak};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

const MAX_QUEUED_REQUESTS: usize = 256;
const MAX_QUEUED_BYTES: usize = sinomo_kv::MAX_KV_COMMAND_PAYLOAD_BYTES + 256;
const MAX_COALESCE_DELAY: Duration = Duration::from_millis(1);
type Reply = Result<ClusterKvRaftPreparedProposal, KvError>;
struct Pending {
    proposal: ClusterKvRaftProposal,
    bytes: usize,
    queued_at: Instant,
    reply: oneshot::Sender<Reply>,
}
#[derive(Default)]
struct Queue {
    pending: VecDeque<Pending>,
    bytes: usize,
    running: bool,
}
static QUEUES: LazyLock<Mutex<HashMap<PathBuf, Weak<Mutex<Queue>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn queue_for(key: PathBuf) -> Arc<Mutex<Queue>> {
    let mut queues = QUEUES.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(queue) = queues.get(&key).and_then(Weak::upgrade) {
        return queue;
    }
    queues.retain(|_, q| q.strong_count() > 0);
    let queue = Arc::new(Mutex::new(Queue::default()));
    queues.insert(key, Arc::downgrade(&queue));
    queue
}

struct CancelAdmission(Arc<AtomicBool>);
impl Drop for CancelAdmission {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

pub(crate) async fn prepare(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    request: KvCommandRequest,
    deadline: Instant,
) -> Reply {
    let kv_paths = paths.kv_paths();
    let queue = queue_for(kv_paths.raft_wal_paths(&state.local_node_id).root);
    let bytes = sinomo_kv::cluster_kv_raft_request_group_bytes(&request);
    let (reply, result) = oneshot::channel();
    let cancelled = Arc::new(AtomicBool::new(false));
    let _admission = CancelAdmission(Arc::clone(&cancelled));
    let start = {
        let mut q = queue.lock().unwrap_or_else(|e| e.into_inner());
        if q.pending.len() >= MAX_QUEUED_REQUESTS
            || q.bytes.saturating_add(bytes) > MAX_QUEUED_BYTES
        {
            return Err(KvError::ResourceExhausted {
                operation: "queue Raft proposal group".into(),
                requested_bytes: q.bytes.saturating_add(bytes),
            });
        }
        q.bytes += bytes;
        q.pending.push_back(Pending {
            proposal: ClusterKvRaftProposal::new(state.clone(), request, deadline)
                .with_cancellation(cancelled),
            bytes,
            queued_at: Instant::now(),
            reply,
        });
        metrics::gauge!("sinomo.kv.group_commit.queue_depth").increment(1.0);
        let start = !q.running;
        q.running = true;
        start
    };
    if start {
        let worker = Arc::clone(&queue);
        let node_id = state.local_node_id.clone();
        if let Err(error) = std::thread::Builder::new()
            .name("kv-group-commit".into())
            .spawn(move || {
                // A single short collection window on idle-to-active transition.
                // Backlogged batches are drained immediately; an empty queue exits.
                std::thread::sleep(MAX_COALESCE_DELAY);
                loop {
                    let batch = {
                        let mut q = worker.lock().unwrap_or_else(|e| e.into_inner());
                        let mut batch = Vec::new();
                        let mut bytes = 0usize;
                        while let Some(front) = q.pending.front() {
                            if batch.len() >= sinomo_kv::CLUSTER_KV_GROUP_COMMIT_MAX_ENTRIES
                                || (!batch.is_empty()
                                    && bytes.saturating_add(front.bytes)
                                        > sinomo_kv::CLUSTER_KV_GROUP_COMMIT_MAX_BYTES)
                            {
                                break;
                            }
                            let item = q.pending.pop_front().unwrap();
                            q.bytes -= item.bytes;
                            metrics::gauge!("sinomo.kv.group_commit.queue_depth").decrement(1.0);
                            if item.reply.is_closed() {
                                continue;
                            }
                            bytes += item.bytes;
                            batch.push(item);
                        }
                        if batch.is_empty() {
                            q.running = false;
                            return;
                        }
                        batch
                    };
                    for item in &batch {
                        metrics::histogram!("sinomo.kv.group_commit.queue.us")
                            .record(item.queued_at.elapsed().as_micros() as f64);
                    }
                    let proposals = batch.iter().map(|p| p.proposal.clone()).collect::<Vec<_>>();
                    let prepared = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        sinomo_kv::prepare_cluster_kv_raft_proposals(
                            &kv_paths, &node_id, &proposals,
                        )
                    }))
                    .unwrap_or_else(|_| vec![Err(KvError::RaftSessionPoisoned); proposals.len()]);
                    for (item, result) in batch.into_iter().zip(prepared) {
                        let _ = item.reply.send(result);
                    }
                }
            })
        {
            let mut q = queue.lock().unwrap_or_else(|e| e.into_inner());
            q.running = false;
            q.bytes = 0;
            for item in q.pending.drain(..) {
                metrics::gauge!("sinomo.kv.group_commit.queue_depth").decrement(1.0);
                let _ = item.reply.send(Err(KvError::Io {
                    path: paths.root.display().to_string(),
                    detail: format!("start Raft group owner: {error}"),
                }));
            }
        }
    }
    result.await.unwrap_or_else(|_| {
        Err(KvError::Io {
            path: paths.root.display().to_string(),
            detail: "Raft group owner stopped before returning its durable outcome".into(),
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_base::{CapabilityName, Id};
    use sinomo_kv::{KvCommand, KvOp};
    use sinomo_node::NodeId;
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancellation_while_group_waits_for_sequence_prevents_late_append() {
        let root = sinomo_testdir::TestDir::new("group_queue_cancel");
        let paths = ClusterPaths::new(&sinomo_local::SinomoDataDir::new(&root));
        let local = NodeId::from_legacy_named("group_queue_local").unwrap();
        let mut state = ClusterBootstrapState::initialized_locally(
            Id::from_legacy_named("group_queue_cluster").unwrap(),
            "group",
            local.clone(),
            "local",
            "pk",
            time::OffsetDateTime::now_utc(),
        );
        state.term = 7;
        state.members[0].capabilities =
            vec![CapabilityName::new(sinomo_node::DAEMON_MAIN_RUN_CAPABILITY).unwrap()];
        state.members[0].allowed_capabilities = state.members[0].capabilities.clone();
        paths
            .kv_paths()
            .raft_store()
            .open_node(&local)
            .unwrap()
            .become_leader(7)
            .unwrap();
        let sequence =
            sinomo_kv::lock_cluster_kv_commit_sequence(&paths.kv_paths(), Duration::from_secs(5))
                .unwrap();
        let queue = queue_for(paths.kv_paths().raft_wal_paths(&local).root);
        let p = paths.clone();
        let pending = tokio::spawn(async move {
            prepare(
                &p,
                &state,
                KvCommandRequest::generated(KvCommand::Op(KvOp::Put {
                    key: "/cancelled".into(),
                    value: vec![1],
                    lease_id: None,
                })),
                Instant::now() + Duration::from_secs(5),
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                {
                    let q = queue.lock().unwrap();
                    if q.running && q.pending.is_empty() {
                        break;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        pending.abort();
        let _ = pending.await;
        drop(sequence);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if !queue.lock().unwrap().running {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            paths
                .kv_paths()
                .raft_store()
                .open_node(&local)
                .unwrap()
                .last_log_index(),
            0
        );
        assert!(!paths.kv_paths().request_attempts_log.exists());
    }
}
