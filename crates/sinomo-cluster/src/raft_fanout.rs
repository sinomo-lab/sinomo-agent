use crate::control_plane_driver::ClusterControlPlaneDriver;
use crate::control_plane_options::{
    ClusterControlPlaneTimeouts, ClusterRaftAppendReplicationOptions,
    ClusterRaftCommittedFanoutOptions,
};
use crate::kv_local_store::ClusterKvPathsExt;
use crate::kv_local_store::kv_err;
use crate::local_state_store::read_cluster_state;
use crate::prelude::*;
use crate::raft_quorum::{
    ClusterKvQuorumAsyncHost, ClusterRaftBlockingCancellationPolicy,
    run_cluster_raft_supervised_blocking,
};
use sinomo_kv::ClusterKvRaftAppendReplicationOutcome;
#[allow(unused_imports)]
use sinomo_kv::{
    ClusterKvWatchEventsRequest, ClusterKvWatchEventsResponse, ClusterRaftAppendRequest,
    ClusterRaftAppendResponse, KvError, KvEvent,
};
use sinomo_network::ClusterMembershipRpcFuture;
use sinomo_network::ClusterPaths;
use sinomo_network::NetworkError;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

static CLUSTER_RAFT_FANOUT_ACTIVE: AtomicBool = AtomicBool::new(false);
static CLUSTER_RAFT_FANOUT_PENDING: AtomicBool = AtomicBool::new(false);
static CLUSTER_RAFT_PEERS_BEHIND: LazyLock<Mutex<BTreeMap<PathBuf, ClusterRaftPeersBehind>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClusterRaftCommittedFanoutReport {
    pub attempted_nodes: usize,
    pub replicated_nodes: Vec<NodeId>,
    /// Targets whose append failed or was rejected; they are known-behind.
    pub lagging_nodes: Vec<NodeId>,
    pub peer_progress_hints: Vec<sinomo_kv::ClusterKvRaftPeerProgress>,
    pub watch_event_batches_sent: usize,
}

/// Peers this process's leader knows to be behind because their committed
/// append failed or was rejected. A restored route or the peer's own contact
/// starts one targeted catch-up; an accepted append clears the peer.
#[derive(Default)]
struct ClusterRaftPeersBehind {
    behind: BTreeSet<NodeId>,
    catching_up: BTreeSet<NodeId>,
}

/// Record committed-append outcomes for known-behind tracking.
pub fn record_cluster_raft_peer_append_results(
    paths: &ClusterPaths,
    accepted: &[NodeId],
    lagging: &[NodeId],
) {
    if accepted.is_empty() && lagging.is_empty() {
        return;
    }
    let Ok(mut peers) = CLUSTER_RAFT_PEERS_BEHIND.lock() else {
        return;
    };
    let peers = peers.entry(paths.root.clone()).or_default();
    for node_id in accepted {
        peers.behind.remove(node_id);
    }
    peers.behind.extend(lagging.iter().cloned());
}

pub fn cluster_raft_peer_is_behind(paths: &ClusterPaths, node_id: &NodeId) -> bool {
    CLUSTER_RAFT_PEERS_BEHIND.lock().is_ok_and(|peers| {
        peers
            .get(&paths.root)
            .is_some_and(|peers| peers.behind.contains(node_id))
    })
}

/// Claim the single catch-up attempt for `node_id`. False while another
/// attempt for the same peer is running.
pub fn claim_cluster_raft_peer_catch_up(paths: &ClusterPaths, node_id: &NodeId) -> bool {
    CLUSTER_RAFT_PEERS_BEHIND.lock().is_ok_and(|mut peers| {
        peers
            .entry(paths.root.clone())
            .or_default()
            .catching_up
            .insert(node_id.clone())
    })
}

/// Release a claimed catch-up. An accepted append clears the peer; any other
/// outcome leaves it known-behind for the next trigger.
pub fn finish_cluster_raft_peer_catch_up(paths: &ClusterPaths, node_id: &NodeId, accepted: bool) {
    let Ok(mut peers) = CLUSTER_RAFT_PEERS_BEHIND.lock() else {
        return;
    };
    let peers = peers.entry(paths.root.clone()).or_default();
    peers.catching_up.remove(node_id);
    if accepted {
        peers.behind.remove(node_id);
    } else {
        peers.behind.insert(node_id.clone());
    }
}

fn committed_raft_fanout_target_node_ids(state: &ClusterBootstrapState) -> Vec<NodeId> {
    state
        .members
        .iter()
        .filter(|member| {
            member.node_id != state.local_node_id
                && member.state.is_authorized()
                && member.capabilities.iter().any(|capability| {
                    capability.as_str() == sinomo_node::DAEMON_MAIN_RUN_CAPABILITY
                })
        })
        .map(|member| member.node_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

pub trait ClusterKvCommittedFanoutAsyncHost {
    fn replicate_cluster_kv_to_node_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        target_node_id: &'a NodeId,
        watch_events: &'a [KvEvent],
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterKvRaftAppendReplicationOutcome>;

    fn send_cluster_kv_watch_events_to_node_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        target_node_id: &'a NodeId,
        request: &'a ClusterKvWatchEventsRequest,
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterKvWatchEventsResponse>;
}

pub trait ClusterRaftCommittedFanoutAsyncHost {
    fn replicate_cluster_raft_to_node_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        target_node_id: &'a NodeId,
        watch_events: &'a [KvEvent],
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterKvRaftAppendReplicationOutcome>;

    fn send_cluster_kv_watch_events_to_node_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        target_node_id: &'a NodeId,
        request: &'a ClusterKvWatchEventsRequest,
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterKvWatchEventsResponse>;
}

impl<T> ClusterRaftCommittedFanoutAsyncHost for T
where
    T: ClusterKvCommittedFanoutAsyncHost,
{
    fn replicate_cluster_raft_to_node_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        target_node_id: &'a NodeId,
        watch_events: &'a [KvEvent],
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterKvRaftAppendReplicationOutcome> {
        self.replicate_cluster_kv_to_node_async(paths, state, target_node_id, watch_events, timeout)
    }

    fn send_cluster_kv_watch_events_to_node_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        target_node_id: &'a NodeId,
        request: &'a ClusterKvWatchEventsRequest,
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterKvWatchEventsResponse> {
        ClusterKvCommittedFanoutAsyncHost::send_cluster_kv_watch_events_to_node_async(
            self,
            state,
            target_node_id,
            request,
            timeout,
        )
    }
}

impl<T> ClusterKvCommittedFanoutAsyncHost for ClusterControlPlaneDriver<T>
where
    T: ClusterKvCommittedFanoutAsyncHost,
{
    fn replicate_cluster_kv_to_node_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        target_node_id: &'a NodeId,
        watch_events: &'a [KvEvent],
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterKvRaftAppendReplicationOutcome> {
        self.transport.replicate_cluster_kv_to_node_async(
            paths,
            state,
            target_node_id,
            watch_events,
            timeout,
        )
    }

    fn send_cluster_kv_watch_events_to_node_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        target_node_id: &'a NodeId,
        request: &'a ClusterKvWatchEventsRequest,
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterKvWatchEventsResponse> {
        self.transport.send_cluster_kv_watch_events_to_node_async(
            state,
            target_node_id,
            request,
            timeout,
        )
    }
}

#[cfg(any(test, feature = "test-support"))]
pub async fn fan_out_committed_cluster_raft_command_async_with_host(
    _runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    events: &[KvEvent],
    options: ClusterRaftCommittedFanoutOptions,
    host: &mut impl ClusterRaftCommittedFanoutAsyncHost,
) -> Result<ClusterRaftCommittedFanoutReport, NetworkError> {
    let mut report = ClusterRaftCommittedFanoutReport::default();
    let mut replicated_nodes = BTreeSet::new();
    for target_node_id in committed_raft_fanout_target_node_ids(state) {
        if replicated_nodes.contains(&target_node_id) {
            continue;
        }
        report.attempted_nodes += 1;
        let Ok(outcome) = host
            .replicate_cluster_raft_to_node_async(
                paths,
                state,
                &target_node_id,
                events,
                options.raft_append_timeout,
            )
            .await
        else {
            report.lagging_nodes.push(target_node_id);
            continue;
        };
        if !outcome.response.accepted {
            report.lagging_nodes.push(target_node_id);
            continue;
        }
        let match_index = outcome.response.append.match_index;
        report
            .peer_progress_hints
            .push(sinomo_kv::ClusterKvRaftPeerProgress {
                peer_node_id: target_node_id.clone(),
                term: outcome.response.append.term,
                next_index: match_index.saturating_add(1),
                match_index,
                last_accepted_index: match_index,
                observed_at: time::OffsetDateTime::now_utc(),
            });
        replicated_nodes.insert(target_node_id.clone());
        report.replicated_nodes.push(target_node_id.clone());
        if outcome.watch_event_fanout_needed {
            report.watch_event_batches_sent +=
                send_cluster_kv_watch_event_fanout_batches_async_with_host(
                    state,
                    &target_node_id,
                    events,
                    options.watch_event_timeout,
                    host,
                )
                .await;
        }
    }
    Ok(report)
}

pub(crate) fn spawn_cluster_raft_committed_fanout_worker_on_runtime<H, F>(
    runtime: tokio::runtime::Handle,
    paths: ClusterPaths,
    state: ClusterBootstrapState,
    events: Vec<KvEvent>,
    timeouts: ClusterControlPlaneTimeouts,
    host_factory: F,
) where
    H: ClusterRaftCommittedFanoutAsyncHost + Send + 'static,
    F: FnMut() -> H + Send + 'static,
{
    let options = timeouts.committed_fanout_options();
    CLUSTER_RAFT_FANOUT_PENDING.store(true, Ordering::Release);
    metrics::gauge!("sinomo.kv.fanout.pending").set(1.0);
    if CLUSTER_RAFT_FANOUT_ACTIVE.swap(true, Ordering::AcqRel) {
        metrics::counter!("sinomo.kv.fanout.coalesced").increment(1);
        return;
    }
    metrics::gauge!("sinomo.kv.fanout.active").set(1.0);
    let worker_runtime = runtime.clone();
    runtime.spawn(async move {
        run_cluster_raft_committed_fanout_worker_async_with_host(
            worker_runtime,
            paths,
            state,
            events,
            options,
            host_factory,
        )
        .await;
    });
}

pub fn spawn_cluster_kv_committed_fanout_worker_on_runtime<H, F>(
    runtime: tokio::runtime::Handle,
    paths: ClusterPaths,
    state: ClusterBootstrapState,
    events: Vec<KvEvent>,
    timeouts: ClusterControlPlaneTimeouts,
    host_factory: F,
) where
    H: ClusterKvCommittedFanoutAsyncHost + Send + 'static,
    F: FnMut() -> H + Send + 'static,
{
    spawn_cluster_raft_committed_fanout_worker_on_runtime(
        runtime,
        paths,
        state,
        events,
        timeouts,
        host_factory,
    );
}

async fn run_cluster_raft_committed_fanout_worker_async_with_host<H, F>(
    runtime: tokio::runtime::Handle,
    paths: ClusterPaths,
    mut state: ClusterBootstrapState,
    mut events: Vec<KvEvent>,
    options: ClusterRaftCommittedFanoutOptions,
    mut host_factory: F,
) where
    H: ClusterRaftCommittedFanoutAsyncHost + Send + 'static,
    F: FnMut() -> H,
{
    let mut refresh_inputs_before_pass = false;
    loop {
        while CLUSTER_RAFT_FANOUT_PENDING.swap(false, Ordering::AcqRel) {
            metrics::gauge!("sinomo.kv.fanout.pending").set(0.0);
            // The first pass already owns the exact commit state/events passed
            // by its caller. Only a coalesced follow-up needs to refresh, and
            // it reads the resident event index rather than cloning the full
            // materialized keyspace after every ordinary fanout.
            if refresh_inputs_before_pass {
                if let Some((latest_state, latest_events)) =
                    read_latest_cluster_raft_fanout_inputs_async(&runtime, &paths).await
                {
                    state = latest_state;
                    events = latest_events;
                } else {
                    events = Vec::new();
                }
            }
            let started = Instant::now();
            let result = fan_out_committed_cluster_raft_command_async_with_factory(
                &runtime,
                &paths,
                &state,
                &events,
                options,
                &mut host_factory,
            )
            .await;
            let outcome = if result.is_ok() { "ok" } else { "error" };
            metrics::counter!("sinomo.kv.fanout.passes", "outcome" => outcome).increment(1);
            metrics::histogram!("sinomo.kv.fanout.pass.us", "outcome" => outcome)
                .record(started.elapsed().as_micros().min(u128::from(u64::MAX)) as f64);
            match result {
                Ok(report) => {
                    metrics::histogram!("sinomo.kv.fanout.attempted_nodes")
                        .record(report.attempted_nodes as f64);
                    metrics::histogram!("sinomo.kv.fanout.replicated_nodes")
                        .record(report.replicated_nodes.len() as f64);
                    metrics::counter!("sinomo.kv.fanout.watch_event_batches")
                        .increment(report.watch_event_batches_sent as u64);
                    record_cluster_raft_peer_append_results(
                        &paths,
                        &report.replicated_nodes,
                        &report.lagging_nodes,
                    );
                    persist_fanout_peer_progress_hints(
                        &runtime,
                        &paths,
                        report.peer_progress_hints,
                    )
                    .await;
                }
                Err(error) => {
                    tracing::debug!(
                        target: "sinomo.cluster.raft_fanout",
                        %error,
                        "committed Raft fanout wave did not complete"
                    );
                }
            }
            refresh_inputs_before_pass = true;
        }

        let claimed = claim_next_cluster_raft_fanout_pass(
            &CLUSTER_RAFT_FANOUT_ACTIVE,
            &CLUSTER_RAFT_FANOUT_PENDING,
        );
        metrics::gauge!("sinomo.kv.fanout.active").set(if claimed { 1.0 } else { 0.0 });
        metrics::gauge!("sinomo.kv.fanout.pending").set(
            if CLUSTER_RAFT_FANOUT_PENDING.load(Ordering::Acquire) {
                1.0
            } else {
                0.0
            },
        );
        if !claimed {
            break;
        }
    }
}

async fn persist_fanout_peer_progress_hints(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    hints: Vec<sinomo_kv::ClusterKvRaftPeerProgress>,
) {
    if hints.is_empty() {
        return;
    }
    let kv_paths = paths.kv_paths();
    match run_cluster_raft_supervised_blocking(
        runtime,
        None,
        ClusterRaftBlockingCancellationPolicy::CompleteAlways,
        "committed-fanout",
        "peer-progress-persistence",
        "persist committed Raft fanout peer progress",
        move || {
            sinomo_kv::record_cluster_kv_raft_peer_progress_batch(&kv_paths, hints).map_err(kv_err)
        },
    )
    .await
    {
        Ok(()) => {}
        Err(error) => tracing::warn!(
            target: "sinomo.cluster.raft_fanout",
            %error,
            "committed Raft fanout could not persist peer progress hints"
        ),
    }
}

async fn fan_out_committed_cluster_raft_command_async_with_factory<H, F>(
    _runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    events: &[KvEvent],
    options: ClusterRaftCommittedFanoutOptions,
    host_factory: &mut F,
) -> Result<ClusterRaftCommittedFanoutReport, NetworkError>
where
    H: ClusterRaftCommittedFanoutAsyncHost + Send + 'static,
    F: FnMut() -> H,
{
    // The committed member snapshot is already the authority for this wave.
    // Do not re-enter KV/status endpoint selection after every commit: an
    // offline hint is exactly when reactive catch-up must still be attempted.
    let targets = committed_raft_fanout_target_node_ids(state);
    let mut tasks = tokio::task::JoinSet::new();
    let mut scheduled = BTreeSet::new();
    for target_node_id in targets {
        if !scheduled.insert(target_node_id.clone()) {
            continue;
        }
        let mut host = host_factory();
        let paths = paths.clone();
        let state = state.clone();
        let events = events.to_vec();
        tasks.spawn(async move {
            let started = Instant::now();
            let outcome = host
                .replicate_cluster_raft_to_node_async(
                    &paths,
                    &state,
                    &target_node_id,
                    &events,
                    options.raft_append_timeout,
                )
                .await;
            let mut watch_event_batches_sent = 0_usize;
            if outcome
                .as_ref()
                .is_ok_and(|outcome| outcome.response.accepted && outcome.watch_event_fanout_needed)
            {
                watch_event_batches_sent =
                    send_cluster_kv_watch_event_fanout_batches_async_with_host(
                        &state,
                        &target_node_id,
                        &events,
                        options.watch_event_timeout,
                        &mut host,
                    )
                    .await;
            }
            let outcome_label = match &outcome {
                Ok(outcome) if outcome.response.accepted => "accepted",
                Ok(_) => "rejected",
                Err(_) => "failed",
            };
            metrics::counter!(
                "sinomo.kv.fanout.target_results",
                "outcome" => outcome_label
            )
            .increment(1);
            metrics::histogram!(
                "sinomo.kv.fanout.target.us",
                "outcome" => outcome_label
            )
            .record(started.elapsed().as_micros().min(u128::from(u64::MAX)) as f64);
            (target_node_id, outcome, watch_event_batches_sent)
        });
    }

    let mut report = ClusterRaftCommittedFanoutReport {
        attempted_nodes: scheduled.len(),
        ..ClusterRaftCommittedFanoutReport::default()
    };
    while let Some(joined) = tasks.join_next().await {
        let Ok((target_node_id, Ok(outcome), watch_event_batches_sent)) = joined else {
            continue;
        };
        if !outcome.response.accepted {
            continue;
        }
        let match_index = outcome.response.append.match_index;
        report
            .peer_progress_hints
            .push(sinomo_kv::ClusterKvRaftPeerProgress {
                peer_node_id: target_node_id.clone(),
                term: outcome.response.append.term,
                next_index: match_index.saturating_add(1),
                match_index,
                last_accepted_index: match_index,
                observed_at: time::OffsetDateTime::now_utc(),
            });
        report.replicated_nodes.push(target_node_id);
        report.watch_event_batches_sent = report
            .watch_event_batches_sent
            .saturating_add(watch_event_batches_sent);
    }
    report.replicated_nodes.sort();
    report.lagging_nodes = scheduled
        .into_iter()
        .filter(|target| report.replicated_nodes.binary_search(target).is_err())
        .collect();
    report
        .peer_progress_hints
        .sort_by(|left, right| left.peer_node_id.cmp(&right.peer_node_id));
    Ok(report)
}

async fn read_latest_cluster_raft_fanout_inputs_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
) -> Option<(ClusterBootstrapState, Vec<KvEvent>)> {
    let paths = paths.clone();
    run_cluster_raft_supervised_blocking(
        runtime,
        None,
        ClusterRaftBlockingCancellationPolicy::CancelQueued,
        "committed-fanout",
        "refresh-inputs",
        "read committed Raft fanout inputs",
        move || {
            let latest_state = read_cluster_state(&paths)?;
            let latest_events = sinomo_kv::read_cluster_kv_local_event_history(
                &paths.kv_paths(),
                &latest_state.local_node_id,
            )
            .map(|read| read.value.events)
            .unwrap_or_default();
            Ok::<_, NetworkError>((latest_state, latest_events))
        },
    )
    .await
    .ok()
}

#[cfg(any(test, feature = "test-support"))]
pub fn claim_next_cluster_raft_fanout_pass_for_test(
    active: &AtomicBool,
    pending: &AtomicBool,
) -> bool {
    claim_next_cluster_raft_fanout_pass(active, pending)
}

fn claim_next_cluster_raft_fanout_pass(active: &AtomicBool, pending: &AtomicBool) -> bool {
    active.store(false, Ordering::Release);
    if !pending.load(Ordering::Acquire) {
        return false;
    }
    !active.swap(true, Ordering::AcqRel)
}

async fn send_cluster_kv_watch_event_fanout_batches_async_with_host(
    state: &ClusterBootstrapState,
    target_node_id: &NodeId,
    events: &[KvEvent],
    timeout: Duration,
    host: &mut impl ClusterRaftCommittedFanoutAsyncHost,
) -> usize {
    let mut sent = 0;
    for event_batch in sinomo_kv::cluster_kv_watch_event_fanout_batches(events) {
        let response = host
            .send_cluster_kv_watch_events_to_node_async(
                state,
                target_node_id,
                &ClusterKvWatchEventsRequest {
                    state: state.clone(),
                    leader_id: state.local_node_id.clone(),
                    events: event_batch.to_vec(),
                },
                timeout,
            )
            .await;
        if !response.is_ok_and(|response| response.accepted) {
            break;
        }
        sent += 1;
    }
    sent
}

struct CommittedFanoutNodeAppendHost<'a, H> {
    paths: &'a ClusterPaths,
    options: ClusterRaftAppendReplicationOptions,
    host: &'a mut H,
}

impl<H> sinomo_kv::ClusterKvRaftAppendReplicationAsyncHost for CommittedFanoutNodeAppendHost<'_, H>
where
    H: ClusterKvQuorumAsyncHost,
{
    type Error = NetworkError;
    type Target = NodeId;

    fn kv_error(error: KvError) -> Self::Error {
        kv_err(error)
    }

    fn last_local_raft_log_index_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, u64, Self::Error> {
        self.host
            .last_local_kv_replication_index_async(self.paths, state)
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
        self.host
            .build_kv_replication_request_async(self.paths, state, next_index)
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
        self.host.on_kv_replication_sent_async()
    }

    fn send_raft_append_async<'a>(
        &'a mut self,
        target: &'a Self::Target,
        request: &'a ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, ClusterRaftAppendResponse, Self::Error> {
        self.host.send_cluster_kv_replication_to_node_async(
            self.paths,
            &request.state,
            target,
            request,
            timeout,
        )
    }
}

pub async fn replicate_cluster_kv_to_node_async_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    target_node_id: &NodeId,
    watch_events: &[KvEvent],
    timeouts: ClusterControlPlaneTimeouts,
    timeout: Duration,
    host: &mut impl ClusterKvQuorumAsyncHost,
) -> Result<ClusterKvRaftAppendReplicationOutcome, NetworkError> {
    let options = timeouts.raft_append_replication_options(timeout);
    let mut append_host = CommittedFanoutNodeAppendHost {
        paths,
        options,
        host,
    };
    sinomo_kv::replicate_cluster_kv_raft_to_target_async_with_host(
        state,
        target_node_id,
        watch_events,
        options.kv_options(),
        &mut append_host,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer_registry_paths(name: &str) -> ClusterPaths {
        ClusterPaths::new(&sinomo_local::SinomoDataDir::new(format!(
            "/fanout-peers-behind/{name}"
        )))
    }

    #[test]
    fn known_behind_peers_follow_append_outcomes_per_kv_root() {
        let paths = peer_registry_paths("outcomes");
        let other = peer_registry_paths("other-root");
        let lagging = NodeId::from_legacy_named("fanout_behind_lagging").expect("node");
        let current = NodeId::from_legacy_named("fanout_behind_current").expect("node");

        record_cluster_raft_peer_append_results(
            &paths,
            std::slice::from_ref(&current),
            std::slice::from_ref(&lagging),
        );
        assert!(cluster_raft_peer_is_behind(&paths, &lagging));
        assert!(!cluster_raft_peer_is_behind(&paths, &current));
        assert!(!cluster_raft_peer_is_behind(&other, &lagging));

        record_cluster_raft_peer_append_results(&paths, std::slice::from_ref(&lagging), &[]);
        assert!(!cluster_raft_peer_is_behind(&paths, &lagging));
    }

    #[test]
    fn peer_catch_up_is_single_flight_and_clears_only_on_acceptance() {
        let paths = peer_registry_paths("single-flight");
        let peer = NodeId::from_legacy_named("fanout_behind_single_flight").expect("node");
        record_cluster_raft_peer_append_results(&paths, &[], std::slice::from_ref(&peer));

        assert!(claim_cluster_raft_peer_catch_up(&paths, &peer));
        assert!(
            !claim_cluster_raft_peer_catch_up(&paths, &peer),
            "a second trigger joins the running attempt"
        );
        finish_cluster_raft_peer_catch_up(&paths, &peer, false);
        assert!(cluster_raft_peer_is_behind(&paths, &peer));

        assert!(claim_cluster_raft_peer_catch_up(&paths, &peer));
        finish_cluster_raft_peer_catch_up(&paths, &peer, true);
        assert!(!cluster_raft_peer_is_behind(&paths, &peer));
        assert!(claim_cluster_raft_peer_catch_up(&paths, &peer));
        finish_cluster_raft_peer_catch_up(&paths, &peer, true);
    }

    #[test]
    fn committed_fanout_targets_every_authorized_main_daemon_member() {
        let now = time::OffsetDateTime::now_utc();
        let local = NodeId::from_legacy_named("fanout_exact_local").expect("local");
        let mut state = ClusterBootstrapState::initialized_locally(
            sinomo_base::Id::from_legacy_named("fanout_exact_cluster").expect("cluster"),
            "fanout exact targets",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let main_capability =
            sinomo_base::CapabilityName::new(sinomo_node::DAEMON_MAIN_RUN_CAPABILITY.to_string())
                .expect("main daemon capability");
        state.members[0].capabilities = vec![main_capability.clone()];

        let mut add_member = |name: &str,
                              role: ClusterMemberRole,
                              member_state: ClusterMemberState,
                              main_daemon: bool|
         -> NodeId {
            let node_id = NodeId::from_legacy_named(name).expect("member node");
            let mut member = state.members[0].clone();
            member.node_id = node_id.clone();
            member.display_name = name.to_string();
            member.node_public_key = format!("pk-{name}");
            member.role = role;
            member.state = member_state;
            member.capabilities = if main_daemon {
                vec![main_capability.clone()]
            } else {
                Default::default()
            };
            state.members.push(member);
            node_id
        };

        let voter = add_member(
            "fanout_exact_voter",
            ClusterMemberRole::Voter,
            ClusterMemberState::Voter,
            true,
        );
        let learner = add_member(
            "fanout_exact_learner",
            ClusterMemberRole::Learner,
            ClusterMemberState::Learner,
            true,
        );
        let non_voting = add_member(
            "fanout_exact_nonvoting",
            ClusterMemberRole::NonVoting,
            ClusterMemberState::NonVoting,
            true,
        );
        let offline = add_member(
            "fanout_exact_offline",
            ClusterMemberRole::Voter,
            ClusterMemberState::Offline,
            true,
        );
        let pending = add_member(
            "fanout_exact_pending",
            ClusterMemberRole::Learner,
            ClusterMemberState::Pending,
            true,
        );
        let removed = add_member(
            "fanout_exact_removed",
            ClusterMemberRole::NonVoting,
            ClusterMemberState::Removed,
            true,
        );
        let no_main = add_member(
            "fanout_exact_no_main",
            ClusterMemberRole::Voter,
            ClusterMemberState::Voter,
            false,
        );
        let duplicate = state
            .members
            .iter()
            .find(|member| member.node_id == voter)
            .expect("voter member")
            .clone();
        state.members.push(duplicate);

        let mut expected = vec![voter, learner, non_voting, offline];
        expected.sort();
        assert_eq!(committed_raft_fanout_target_node_ids(&state), expected);
        assert!(!expected.contains(&local));
        assert!(!expected.contains(&pending));
        assert!(!expected.contains(&removed));
        assert!(!expected.contains(&no_main));
    }
}
