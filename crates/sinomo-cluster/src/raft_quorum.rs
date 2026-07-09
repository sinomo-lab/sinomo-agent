use crate::control_plane_driver::{ClusterControlPlaneDriver, ClusterControlPlaneTransport};
#[cfg(any(test, feature = "test-support"))]
use crate::control_plane_options::ClusterControlPlaneTimeouts;
use crate::control_plane_options::ClusterRaftAppendReplicationOptions;
use crate::kv_local_store::ClusterKvPathsExt;
use crate::kv_local_store::kv_err;
use crate::kv_local_store::{
    LocalClusterRaftCommandStorage, append_cluster_raft_command_uncommitted_or_no_effect_async,
    commit_cluster_raft_index_with_events_async,
    discard_cluster_raft_uncommitted_tail_and_clear_leader_async,
};
use crate::member_store::merge_cluster_state_from_peer;
use crate::prelude::*;
#[cfg(any(test, feature = "test-support"))]
use crate::raft_replication::replicate_cluster_raft_to_endpoint_with_local_host;
#[allow(unused_imports)]
#[allow(unused_imports)]
use sinomo_kv::KvError;
use sinomo_kv::{
    ClusterKvRaftQuorumCommitOptions, ClusterKvRaftQuorumCommitResult, ClusterRaftAppendRequest,
    ClusterRaftAppendResponse,
};
use sinomo_kv::{KvCommand, KvEvent};
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

    fn append_cluster_raft_command_uncommitted_or_no_effect(
        &mut self,
        command: KvCommand,
    ) -> Result<sinomo_kv::ClusterKvRaftUncommittedAppend, NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::append_cluster_raft_command_uncommitted_or_no_effect(
            &mut self.storage,
            command,
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
    command: KvCommand,
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
    commit_cluster_raft_command_with_quorum_driver(paths, state, command, options, &mut host)
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
        request: &ClusterRaftAppendRequest,
    ) -> Duration {
        self.options
            .routed_node_append_attempt_timeout(remaining, request)
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

    fn append_cluster_raft_command_uncommitted_or_no_effect(
        &mut self,
        command: KvCommand,
    ) -> Result<sinomo_kv::ClusterKvRaftUncommittedAppend, NetworkError> {
        sinomo_kv::ClusterKvRaftCommandStorage::append_cluster_raft_command_uncommitted_or_no_effect(
            &mut self.storage,
            command,
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
    command: KvCommand,
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
    commit_cluster_raft_command_with_quorum_driver(paths, state, command, options, &mut host)
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

    fn append_cluster_raft_command_uncommitted_or_no_effect(
        &mut self,
        command: KvCommand,
    ) -> Result<sinomo_kv::ClusterKvRaftUncommittedAppend, NetworkError> {
        self.host
            .append_cluster_raft_command_uncommitted_or_no_effect(command)
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

    fn cluster_read_only_error(&mut self, detail: String) -> NetworkError {
        NetworkError::ClusterReadOnly { detail }
    }
}

pub fn commit_cluster_raft_command_with_quorum_driver(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    command: KvCommand,
    options: ClusterKvRaftQuorumCommitOptions,
    host: &mut impl ClusterRaftQuorumCommitHost,
) -> Result<ClusterKvRaftQuorumCommitResult, NetworkError> {
    let mut kv_host = NetworkClusterKvRaftQuorumCommitHost { paths, host };
    sinomo_kv::commit_cluster_kv_raft_command_with_quorum(state, command, options, &mut kv_host)
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
        request: &ClusterRaftAppendRequest,
    ) -> Duration {
        self.options
            .routed_node_append_attempt_timeout(remaining, request)
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

    fn append_cluster_raft_command_uncommitted_or_no_effect_async<'a>(
        &'a mut self,
        command: KvCommand,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<
        'a,
        sinomo_kv::ClusterKvRaftUncommittedAppend,
        NetworkError,
    > {
        let runtime = self.runtime;
        let paths = self.paths;
        let local_node_id = self.local_node_id.clone();
        Box::pin(async move {
            append_cluster_raft_command_uncommitted_or_no_effect_async(
                runtime,
                paths,
                &local_node_id,
                command,
            )
            .await
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

    fn cluster_read_only_error(&mut self, detail: String) -> NetworkError {
        NetworkError::ClusterReadOnly { detail }
    }
}

pub async fn commit_cluster_raft_command_with_quorum_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    command: KvCommand,
    options: ClusterKvRaftQuorumCommitOptions,
    append_options: ClusterRaftAppendReplicationOptions,
    host: &mut H,
) -> Result<ClusterKvRaftQuorumCommitResult, NetworkError>
where
    H: ClusterRaftQuorumAsyncHost + Send,
{
    let mut kv_host = NetworkClusterKvRaftQuorumCommitAsyncHost {
        runtime,
        paths,
        local_node_id: state.local_node_id.clone(),
        append_options,
        host,
    };
    sinomo_kv::commit_cluster_kv_raft_command_with_quorum_async(
        state,
        command,
        options,
        &mut kv_host,
    )
    .await
}
