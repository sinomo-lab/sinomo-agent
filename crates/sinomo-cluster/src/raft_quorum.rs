use crate::control_plane_driver::{ClusterControlPlaneDriver, ClusterControlPlaneTransport};
#[cfg(any(test, feature = "test-support"))]
use crate::control_plane_options::ClusterControlPlaneTimeouts;
use crate::control_plane_options::ClusterRaftAppendReplicationOptions;
use crate::kv_local_store::ClusterKvPathsExt;
use crate::kv_local_store::kv_err;
use crate::kv_local_store::{
    LocalClusterRaftCommandStorage, append_cluster_raft_request_uncommitted_async,
    commit_cluster_raft_index_with_events_async,
    discard_cluster_raft_uncommitted_tail_and_clear_leader_async,
    replay_cluster_raft_request_if_committed_async,
};
use crate::local_state_store::project_committed_cluster_raft_config;
use crate::member_store::merge_cluster_state_from_peer;
use crate::prelude::*;
#[cfg(any(test, feature = "test-support"))]
use crate::raft_replication::replicate_cluster_raft_to_endpoint_with_local_host;
#[allow(unused_imports)]
use sinomo_kv::KvError;
use sinomo_kv::{
    ClusterKvRaftQuorumCommitOptions, ClusterKvRaftQuorumCommitResult, ClusterRaftAppendRequest,
    ClusterRaftAppendResponse,
};
use sinomo_kv::{KvCommandRequest, KvEvent, KvSnapshot};
use sinomo_network::ClusterConnectivityState;
use sinomo_network::ClusterMembershipRpcFuture;
use sinomo_network::ClusterPaths;
use sinomo_network::NetworkError;
use sinomo_network::{ClusterEndpointInternalSelectionPurpose, ClusterEndpointSelector};
use std::time::Duration;

pub trait ClusterKvQuorumAsyncHost {
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

pub trait ClusterRaftQuorumAsyncHost {
    fn last_local_raft_log_index_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
    ) -> ClusterMembershipRpcFuture<'a, u64>;

    fn build_raft_append_request_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        next_index: u64,
    ) -> ClusterMembershipRpcFuture<'a, ClusterRaftAppendRequest>;

    fn send_raft_append_to_node_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        target_node_id: &'a NodeId,
        request: &'a ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterRaftAppendResponse>;

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
    fn last_local_raft_log_index_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
    ) -> ClusterMembershipRpcFuture<'a, u64> {
        self.last_local_kv_replication_index_async(paths, state)
    }

    fn build_raft_append_request_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a ClusterBootstrapState,
        next_index: u64,
    ) -> ClusterMembershipRpcFuture<'a, ClusterRaftAppendRequest> {
        self.build_kv_replication_request_async(paths, state, next_index)
    }

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
    storage: LocalClusterRaftCommandStorage<'a>,
    control_plane_timeouts: ClusterControlPlaneTimeouts,
    send_raft_append: S,
    record_endpoint_command_connectivity: R,
    fan_out_committed_cluster_raft_command: F,
    on_raft_append_sent: O,
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

    fn append_cluster_raft_request_uncommitted(
        &mut self,
        request: KvCommandRequest,
    ) -> Result<sinomo_kv::KvLogEntry, NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::append_cluster_raft_request_uncommitted(
            &mut self.storage,
            request,
        )
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
    fn replicate_cluster_raft_to_node(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError> {
        let endpoint = representative_cluster_endpoint_for_node(state, target_node_id)?;
        let send_raft_append = &mut self.send_raft_append;
        let on_raft_append_sent = &mut self.on_raft_append_sent;
        replicate_cluster_raft_to_endpoint_with_local_host(
            paths,
            state,
            &endpoint,
            &[],
            self.control_plane_timeouts
                .raft_append_replication_options(timeout),
            |endpoint, request, timeout| send_raft_append(endpoint, request, timeout),
            || on_raft_append_sent(),
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
    storage: LocalClusterRaftCommandStorage<'a>,
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

    fn append_cluster_raft_request_uncommitted(
        &mut self,
        request: KvCommandRequest,
    ) -> Result<sinomo_kv::KvLogEntry, NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::append_cluster_raft_request_uncommitted(
            &mut self.storage,
            request,
        )
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
}

impl<T> ClusterRaftQuorumTransportHost for DriverRaftQuorumCommitHost<'_, T>
where
    T: ClusterControlPlaneTransport,
{
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
    let mut host = DriverRaftQuorumCommitHost {
        storage: LocalClusterRaftCommandStorage::new(paths, state.local_node_id.clone()),
        driver,
    };
    commit_cluster_raft_command_with_quorum_driver(paths, state, request, options, &mut host)
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
    host: &'a mut H,
}

fn remote_voter_node_targets(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Vec<NodeId>, NetworkError> {
    if sinomo_kv::cluster_kv_raft_local_is_voter(state)
        && sinomo_kv::cluster_kv_raft_quorum_size_for_state(state) <= 1
    {
        return Ok(Vec::new());
    }
    let mut targets = ClusterEndpointSelector::new(paths, state)
        .try_select_internal_unique_node_ids(
            ClusterEndpointInternalSelectionPurpose::LiveClusterStateAuthorityCommand,
        )?;
    targets.retain(|node_id| node_id != &state.local_node_id && state.node_is_raft_voter(node_id));
    Ok(targets)
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
        sinomo_kv::lock_cluster_kv_commit_sequence(
            &self.paths.kv_paths(),
            sinomo_kv::CLUSTER_KV_COMMIT_SEQUENCE_LOCK_WAIT,
        )
        .map(Some)
        .map_err(kv_err)
    }

    fn replay_cluster_raft_request_if_committed(
        &mut self,
        request: &KvCommandRequest,
    ) -> Result<Option<(KvSnapshot, sinomo_kv::KvCommittedRequestRecord)>, NetworkError> {
        self.host.replay_cluster_raft_request_if_committed(request)
    }

    fn append_cluster_raft_request_uncommitted(
        &mut self,
        request: KvCommandRequest,
    ) -> Result<sinomo_kv::KvLogEntry, NetworkError> {
        self.host.append_cluster_raft_request_uncommitted(request)
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
        self.host.commit_cluster_raft_index_with_events(index)
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
        self.paths
            .kv_paths()
            .locked_raft_local_is_leader(state)
            .map_err(kv_err)
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
        self.host
            .replicate_cluster_raft_to_node(self.paths, state, target, timeout)
    }

    fn record_target_replication_status(
        &mut self,
        state: &ClusterBootstrapState,
        target: &Self::Target,
        status: sinomo_kv::ClusterKvRaftTargetReplicationStatus,
        latency_ms: Option<u64>,
    ) -> Result<(), NetworkError> {
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
        self.host.fan_out_committed_cluster_raft_command(
            self.paths.clone(),
            state,
            events,
            targets,
        );
        Ok(())
    }

    fn cluster_authority_error(&mut self, failure: sinomo_kv::KvAuthorityFailure) -> NetworkError {
        NetworkError::ClusterReadOnly {
            detail: serde_json::to_string(&failure).unwrap_or_else(|_| format!("{failure:?}")),
        }
    }
}

pub fn commit_cluster_raft_command_with_quorum_driver(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    request: KvCommandRequest,
    options: ClusterKvRaftQuorumCommitOptions,
    host: &mut impl ClusterRaftQuorumCommitHost,
) -> Result<ClusterKvRaftQuorumCommitResult, NetworkError> {
    let mut consensus_state = state.clone();
    project_committed_cluster_raft_config(paths, &mut consensus_state)?;
    let mut kv_host = NetworkClusterKvRaftQuorumCommitHost { paths, host };
    let result = sinomo_kv::commit_cluster_kv_raft_command_with_quorum(
        &mut consensus_state,
        request,
        options,
        &mut kv_host,
    )?;
    state.term = state.term.max(consensus_state.term);
    state.kv_revision = state.kv_revision.max(consensus_state.kv_revision);
    Ok(result)
}

struct AsyncNodeRaftAppendReplicationHost<'a, H> {
    paths: &'a ClusterPaths,
    options: ClusterRaftAppendReplicationOptions,
    host: &'a mut H,
}

impl<H> sinomo_kv::ClusterKvRaftAppendReplicationAsyncHost
    for AsyncNodeRaftAppendReplicationHost<'_, H>
where
    H: ClusterRaftQuorumAsyncHost + Send,
{
    type Error = NetworkError;
    type Target = NodeId;

    fn kv_error(error: KvError) -> Self::Error {
        crate::kv_local_store::kv_err(error)
    }

    fn last_local_raft_log_index_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, u64, Self::Error> {
        self.host.last_local_raft_log_index_async(self.paths, state)
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
            .build_raft_append_request_async(self.paths, state, next_index)
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
    local_node_id: NodeId,
    append_options: ClusterRaftAppendReplicationOptions,
    host: &'a mut H,
}

impl<H> sinomo_kv::ClusterKvRaftQuorumCommitAsyncHost
    for NetworkClusterKvRaftQuorumCommitAsyncHost<'_, H>
where
    H: ClusterRaftQuorumAsyncHost + Send,
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
        let kv_paths = self.paths.kv_paths();
        let state = state.clone();
        Box::pin(async move {
            runtime
                .spawn_blocking(move || kv_paths.locked_raft_local_is_leader(&state))
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "verify local raft leadership".to_string(),
                    detail: format!("local raft leadership task failed: {source}"),
                })?
                .map_err(kv_err)
        })
    }

    fn lock_cluster_raft_commit_sequence_async<'a>(
        &'a mut self,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, Option<std::fs::File>, NetworkError> {
        let runtime = self.runtime;
        let kv_paths = self.paths.kv_paths();
        Box::pin(async move {
            runtime
                .spawn_blocking(move || {
                    sinomo_kv::lock_cluster_kv_commit_sequence(
                        &kv_paths,
                        sinomo_kv::CLUSTER_KV_COMMIT_SEQUENCE_LOCK_WAIT,
                    )
                })
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "cluster raft commit sequence lock".to_string(),
                    detail: format!("cluster raft commit sequence lock task failed: {source}"),
                })?
                .map(Some)
                .map_err(kv_err)
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
        let paths = self.paths;
        let local_node_id = self.local_node_id.clone();
        Box::pin(async move {
            replay_cluster_raft_request_if_committed_async(runtime, paths, &local_node_id, request)
                .await
        })
    }

    fn append_cluster_raft_request_uncommitted_async<'a>(
        &'a mut self,
        request: KvCommandRequest,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, sinomo_kv::KvLogEntry, NetworkError> {
        let runtime = self.runtime;
        let paths = self.paths;
        let local_node_id = self.local_node_id.clone();
        Box::pin(async move {
            append_cluster_raft_request_uncommitted_async(runtime, paths, &local_node_id, request)
                .await
        })
    }

    fn record_cluster_raft_request_rejected_before_commit_async<'a>(
        &'a mut self,
        request: &'a KvCommandRequest,
        failure: &'a sinomo_kv::KvAuthorityFailure,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), NetworkError> {
        let runtime = self.runtime;
        let paths = self.paths.clone();
        let local_node_id = self.local_node_id.clone();
        let request = request.clone();
        let failure = failure.clone();
        Box::pin(async move {
            runtime
                .spawn_blocking(move || {
                    LocalClusterRaftCommandStorage::new(&paths, local_node_id)
                        .record_rejected_before_commit(&request, &failure)
                })
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "record rejected cluster raft request".to_string(),
                    detail: format!("record rejected cluster raft request task failed: {source}"),
                })?
        })
    }

    fn record_cluster_raft_request_unknown_async<'a>(
        &'a mut self,
        request: &'a KvCommandRequest,
        detail: &'a str,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), NetworkError> {
        let runtime = self.runtime;
        let paths = self.paths.clone();
        let local_node_id = self.local_node_id.clone();
        let request = request.clone();
        let detail = detail.to_string();
        Box::pin(async move {
            runtime
                .spawn_blocking(move || {
                    LocalClusterRaftCommandStorage::new(&paths, local_node_id)
                        .record_unknown(&request, &detail)
                })
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "record unknown cluster raft request".to_string(),
                    detail: format!("record unknown cluster raft request task failed: {source}"),
                })?
        })
    }

    fn discard_cluster_raft_uncommitted_tail_and_clear_leader_async<'a>(
        &'a mut self,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), NetworkError> {
        let runtime = self.runtime;
        let paths = self.paths;
        let local_node_id = self.local_node_id.clone();
        Box::pin(async move {
            discard_cluster_raft_uncommitted_tail_and_clear_leader_async(
                runtime,
                paths,
                &local_node_id,
            )
            .await
            .map(|_| ())
        })
    }

    fn commit_cluster_raft_index_with_events_async<'a>(
        &'a mut self,
        index: u64,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, sinomo_kv::ClusterKvRaftCommitResult, NetworkError>
    {
        let runtime = self.runtime;
        let paths = self.paths;
        let local_node_id = self.local_node_id.clone();
        Box::pin(async move {
            commit_cluster_raft_index_with_events_async(runtime, paths, &local_node_id, index).await
        })
    }

    fn remote_voter_targets_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, Vec<Self::Target>, NetworkError> {
        let runtime = self.runtime;
        let paths = self.paths;
        Box::pin(async move {
            if sinomo_kv::cluster_kv_raft_local_is_voter(state)
                && sinomo_kv::cluster_kv_raft_quorum_size_for_state(state) <= 1
            {
                return Ok(Vec::new());
            }
            let mut targets = ClusterEndpointSelector::select_internal_unique_node_ids_async(
                runtime,
                paths,
                state,
                ClusterEndpointInternalSelectionPurpose::LiveClusterStateAuthorityCommand,
            )
            .await?;
            targets.retain(|node_id| {
                node_id != &state.local_node_id && state.node_is_raft_voter(node_id)
            });
            Ok(targets)
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
        let append_options = self.append_options;
        let host = &mut *self.host;
        Box::pin(async move {
            let mut node_host = AsyncNodeRaftAppendReplicationHost {
                paths,
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

    fn observe_cluster_raft_term_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        term: u64,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), NetworkError> {
        let paths = self.paths.clone();
        let node_id = state.local_node_id.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            runtime
                .spawn_blocking(move || {
                    paths
                        .kv_paths()
                        .locked_observe_raft_term_and_write_snapshot_if_changed(&node_id, term)
                })
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "cluster raft term observation".to_string(),
                    detail: format!("term observation task failed: {source}"),
                })?
                .map(|_| ())
                .map_err(kv_err)
        })
    }

    fn fan_out_committed_cluster_raft_command_async<'a>(
        &'a mut self,
        state: ClusterBootstrapState,
        events: Vec<KvEvent>,
        targets: Vec<Self::Target>,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, (), NetworkError> {
        let paths = self.paths.clone();
        let host = &mut *self.host;
        Box::pin(async move {
            host.fan_out_committed_cluster_raft_command_async(paths, state, events, targets)
                .await
        })
    }

    fn cluster_authority_error(&mut self, failure: sinomo_kv::KvAuthorityFailure) -> NetworkError {
        NetworkError::ClusterReadOnly {
            detail: serde_json::to_string(&failure).unwrap_or_else(|_| format!("{failure:?}")),
        }
    }
}

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
    H: ClusterRaftQuorumAsyncHost + Send,
{
    let project_paths = paths.clone();
    let mut consensus_state = state.clone();
    consensus_state = runtime
        .spawn_blocking(move || {
            project_committed_cluster_raft_config(&project_paths, &mut consensus_state)?;
            Ok::<_, NetworkError>(consensus_state)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "project committed cluster Raft config".to_string(),
            detail: format!("Raft config projection task failed: {source}"),
        })??;
    let mut kv_host = NetworkClusterKvRaftQuorumCommitAsyncHost {
        runtime,
        paths,
        local_node_id: consensus_state.local_node_id.clone(),
        append_options,
        host,
    };
    let result = sinomo_kv::commit_cluster_kv_raft_command_with_quorum_async(
        &mut consensus_state,
        request,
        options,
        &mut kv_host,
    )
    .await?;
    state.term = state.term.max(consensus_state.term);
    state.kv_revision = state.kv_revision.max(consensus_state.kv_revision);
    Ok(result)
}
