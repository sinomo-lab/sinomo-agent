use crate::control_plane_driver::ClusterControlPlaneDriver;
use crate::control_plane_options::{
    ClusterControlPlaneTimeouts, ClusterRaftAppendReplicationOptions,
    ClusterRaftCommittedFanoutOptions,
};
use crate::kv_local_store::kv_err;
use crate::kv_local_store::read_cluster_kv_snapshot;
use crate::local_state_store::read_cluster_state;
use crate::prelude::*;
use crate::raft_quorum::ClusterKvQuorumAsyncHost;
use sinomo_kv::ClusterKvRaftAppendReplicationOutcome;
#[allow(unused_imports)]
use sinomo_kv::{
    ClusterKvWatchEventsRequest, ClusterKvWatchEventsResponse, ClusterRaftAppendRequest,
    ClusterRaftAppendResponse, KvError, KvEvent,
};
use sinomo_network::ClusterMembershipRpcFuture;
use sinomo_network::ClusterPaths;
use sinomo_network::NetworkError;
use sinomo_network::{ClusterEndpointInternalSelectionPurpose, ClusterEndpointSelector};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

static CLUSTER_RAFT_FANOUT_ACTIVE: AtomicBool = AtomicBool::new(false);
static CLUSTER_RAFT_FANOUT_PENDING: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClusterRaftCommittedFanoutReport {
    pub attempted_nodes: usize,
    pub replicated_nodes: Vec<NodeId>,
    pub watch_event_batches_sent: usize,
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

pub async fn fan_out_committed_cluster_raft_command_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    events: &[KvEvent],
    options: ClusterRaftCommittedFanoutOptions,
    host: &mut impl ClusterRaftCommittedFanoutAsyncHost,
) -> Result<ClusterRaftCommittedFanoutReport, NetworkError> {
    let mut report = ClusterRaftCommittedFanoutReport::default();
    let mut replicated_nodes = BTreeSet::new();
    for target_node_id in ClusterEndpointSelector::select_internal_unique_node_ids_async(
        runtime,
        paths,
        state,
        ClusterEndpointInternalSelectionPurpose::RaftMemberReplication,
    )
    .await?
    {
        if target_node_id == state.local_node_id || replicated_nodes.contains(&target_node_id) {
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
            continue;
        };
        if !outcome.response.accepted {
            continue;
        }
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
    if CLUSTER_RAFT_FANOUT_ACTIVE.swap(true, Ordering::AcqRel) {
        return;
    }
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
    H: ClusterRaftCommittedFanoutAsyncHost,
    F: FnMut() -> H,
{
    loop {
        while CLUSTER_RAFT_FANOUT_PENDING.swap(false, Ordering::AcqRel) {
            let mut host = host_factory();
            let _ = fan_out_committed_cluster_raft_command_async_with_host(
                &runtime, &paths, &state, &events, options, &mut host,
            )
            .await;
            if let Some((latest_state, latest_events)) =
                read_latest_cluster_raft_fanout_inputs_async(&runtime, &paths).await
            {
                state = latest_state;
                events = latest_events;
            } else {
                events = Vec::new();
            }
        }

        if !claim_next_cluster_raft_fanout_pass(
            &CLUSTER_RAFT_FANOUT_ACTIVE,
            &CLUSTER_RAFT_FANOUT_PENDING,
        ) {
            break;
        }
    }
}

async fn read_latest_cluster_raft_fanout_inputs_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
) -> Option<(ClusterBootstrapState, Vec<KvEvent>)> {
    let paths = paths.clone();
    runtime
        .spawn_blocking(move || {
            let latest_state = read_cluster_state(&paths)?;
            let latest_events = read_cluster_kv_snapshot(&paths)
                .map(|snapshot| snapshot.events)
                .unwrap_or_default();
            Ok::<_, NetworkError>((latest_state, latest_events))
        })
        .await
        .ok()
        .and_then(Result::ok)
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
    H: ClusterKvQuorumAsyncHost + Send,
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
    host: &mut (impl ClusterKvQuorumAsyncHost + Send),
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
