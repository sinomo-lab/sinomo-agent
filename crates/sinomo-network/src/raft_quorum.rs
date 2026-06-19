use crate::ClusterPaths;
use crate::connectivity::ClusterConnectivityState;
use crate::control_plane_driver::{ClusterControlPlaneDriver, ClusterControlPlaneTransport};
#[cfg(test)]
use crate::control_plane_options::ClusterControlPlaneTimeouts;
use crate::control_plane_options::ClusterRaftAppendReplicationOptions;
use crate::endpoint_selector::{ClusterEndpointInternalSelectionPurpose, ClusterEndpointSelector};
use crate::error::NetworkError;
use crate::kv_local_store::{
    LocalClusterRaftCommandStorage, append_cluster_raft_command_uncommitted_or_no_effect_async,
    cluster_kv_raft_store, commit_cluster_raft_index_with_events_async,
    discard_cluster_raft_uncommitted_tail_and_clear_leader_async,
};
use crate::member_store::merge_cluster_state_from_peer;
use crate::prelude::*;
#[cfg(test)]
use crate::raft_replication::replicate_cluster_raft_to_endpoint_with_local_host;
use crate::raft_replication::{
    ClusterRaftAppendReplicationAsyncHost, ClusterRaftAppendReplicationHost,
    replicate_cluster_raft_to_endpoint_async_with_host,
    replicate_cluster_raft_to_endpoint_with_host,
};
use crate::rpc_future::ClusterMembershipRpcFuture;
use sinomo_kv::{
    ClusterKvRaftQuorumCommitOptions, ClusterKvRaftQuorumCommitResult, ClusterRaftAppendRequest,
    ClusterRaftAppendResponse,
};
use std::time::Duration;

pub trait ClusterRaftQuorumAsyncHost: ClusterRaftAppendReplicationAsyncHost {
    fn record_endpoint_command_connectivity(
        &mut self,
        _paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
        _endpoint: &ClusterEndpointRecord,
        _connectivity_state: ClusterConnectivityState,
        _latency_ms: Option<u64>,
    ) {
    }

    fn fan_out_committed_cluster_raft_command_async<'a>(
        &'a mut self,
        _paths: ClusterPaths,
        _state: ClusterBootstrapState,
        _events: Vec<KvEvent>,
        _targets: Vec<ClusterEndpointRecord>,
    ) -> ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

pub trait ClusterRaftQuorumTransportHost {
    fn replicate_cluster_raft_to_endpoint(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError>;

    fn record_endpoint_command_connectivity(
        &mut self,
        _paths: &ClusterPaths,
        _state: &ClusterBootstrapState,
        _endpoint: &ClusterEndpointRecord,
        _connectivity_state: ClusterConnectivityState,
        _latency_ms: Option<u64>,
    ) {
    }

    fn fan_out_committed_cluster_raft_command(
        &mut self,
        _paths: ClusterPaths,
        _state: ClusterBootstrapState,
        _events: Vec<KvEvent>,
        _targets: Vec<ClusterEndpointRecord>,
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

#[cfg(test)]
struct LocalClusterRaftQuorumCommitHost<'a, S, R, F, O> {
    storage: LocalClusterRaftCommandStorage<'a>,
    control_plane_timeouts: ClusterControlPlaneTimeouts,
    send_raft_append: S,
    record_endpoint_command_connectivity: R,
    fan_out_committed_cluster_raft_command: F,
    on_raft_append_sent: O,
}

#[cfg(test)]
impl<S, R, F, O> sinomo_kv::ClusterKvRaftCommandStorage
    for LocalClusterRaftQuorumCommitHost<'_, S, R, F, O>
{
    type Error = NetworkError;

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

#[cfg(test)]
impl<S, R, F, O> ClusterRaftQuorumTransportHost for LocalClusterRaftQuorumCommitHost<'_, S, R, F, O>
where
    S: FnMut(
        &ClusterEndpointRecord,
        &ClusterRaftAppendRequest,
        Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError>,
    R: FnMut(
        &ClusterPaths,
        &ClusterBootstrapState,
        &ClusterEndpointRecord,
        ClusterConnectivityState,
        Option<u64>,
    ),
    F: FnMut(ClusterPaths, ClusterBootstrapState, Vec<KvEvent>, Vec<ClusterEndpointRecord>),
    O: FnMut(),
{
    fn replicate_cluster_raft_to_endpoint(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError> {
        let send_raft_append = &mut self.send_raft_append;
        let on_raft_append_sent = &mut self.on_raft_append_sent;
        replicate_cluster_raft_to_endpoint_with_local_host(
            paths,
            state,
            endpoint,
            &[],
            self.control_plane_timeouts
                .raft_append_replication_options(timeout),
            |endpoint, request, timeout| send_raft_append(endpoint, request, timeout),
            || on_raft_append_sent(),
        )
        .map(|outcome| outcome.response)
    }

    fn record_endpoint_command_connectivity(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
        connectivity_state: ClusterConnectivityState,
        latency_ms: Option<u64>,
    ) {
        (self.record_endpoint_command_connectivity)(
            paths,
            state,
            endpoint,
            connectivity_state,
            latency_ms,
        );
    }

    fn fan_out_committed_cluster_raft_command(
        &mut self,
        paths: ClusterPaths,
        state: ClusterBootstrapState,
        events: Vec<KvEvent>,
        targets: Vec<ClusterEndpointRecord>,
    ) {
        (self.fan_out_committed_cluster_raft_command)(paths, state, events, targets);
    }
}

#[cfg(test)]
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
    R: FnMut(
        &ClusterPaths,
        &ClusterBootstrapState,
        &ClusterEndpointRecord,
        ClusterConnectivityState,
        Option<u64>,
    ),
    F: FnMut(ClusterPaths, ClusterBootstrapState, Vec<KvEvent>, Vec<ClusterEndpointRecord>),
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

struct DriverRaftAppendReplicationHost<'a, T> {
    transport: &'a mut T,
}

impl<T> ClusterRaftAppendReplicationHost for DriverRaftAppendReplicationHost<'_, T>
where
    T: ClusterControlPlaneTransport,
{
    fn last_local_raft_log_index(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<u64, NetworkError> {
        cluster_kv_raft_store(paths)
            .locked_last_log_index(&state.local_node_id)
            .map_err(Into::into)
    }

    fn build_raft_append_request(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        next_index: u64,
    ) -> Result<ClusterRaftAppendRequest, NetworkError> {
        cluster_kv_raft_store(paths)
            .locked_build_cluster_raft_append_request_from_next_index(state, next_index)
            .map_err(Into::into)
    }

    fn send_raft_append(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError> {
        self.transport.send_raft_append(endpoint, request, timeout)
    }

    fn on_raft_append_sent(&mut self) {
        self.transport.on_raft_append_sent();
    }
}

struct DriverRaftQuorumCommitHost<'a, T> {
    storage: LocalClusterRaftCommandStorage<'a>,
    driver: &'a mut ClusterControlPlaneDriver<T>,
}

impl<T> sinomo_kv::ClusterKvRaftCommandStorage for DriverRaftQuorumCommitHost<'_, T> {
    type Error = NetworkError;

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
    fn replicate_cluster_raft_to_endpoint(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError> {
        let mut host = DriverRaftAppendReplicationHost {
            transport: &mut self.driver.transport,
        };
        replicate_cluster_raft_to_endpoint_with_host(
            paths,
            state,
            endpoint,
            &[],
            self.driver
                .timeouts
                .raft_append_replication_options(timeout),
            &mut host,
        )
        .map(|outcome| outcome.response)
    }

    fn record_endpoint_command_connectivity(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
        connectivity_state: ClusterConnectivityState,
        latency_ms: Option<u64>,
    ) {
        self.driver.transport.record_endpoint_command_connectivity(
            paths,
            state,
            endpoint,
            connectivity_state,
            latency_ms,
        );
    }

    fn fan_out_committed_cluster_raft_command(
        &mut self,
        paths: ClusterPaths,
        state: ClusterBootstrapState,
        events: Vec<KvEvent>,
        targets: Vec<ClusterEndpointRecord>,
    ) {
        self.driver
            .transport
            .fan_out_committed_cluster_raft_command(paths, state, events, targets);
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

impl<H> sinomo_kv::ClusterKvRaftCommandStorage for NetworkClusterKvRaftQuorumCommitHost<'_, H>
where
    H: ClusterRaftQuorumCommitHost,
{
    type Error = NetworkError;

    fn lock_cluster_raft_commit_sequence(&mut self) -> Result<Option<std::fs::File>, NetworkError> {
        sinomo_kv::lock_cluster_kv_commit_sequence(
            &self.paths.kv_paths(),
            sinomo_kv::CLUSTER_KV_COMMIT_SEQUENCE_LOCK_WAIT,
        )
        .map(Some)
        .map_err(Into::into)
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
    type Target = ClusterEndpointRecord;

    fn remote_voter_targets(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<Vec<Self::Target>, NetworkError> {
        Ok(
            ClusterEndpointSelector::new(self.paths, state).select_internal(
                ClusterEndpointInternalSelectionPurpose::LiveClusterStateAuthorityCommand,
            ),
        )
    }

    fn target_node_id<'a>(&self, target: &'a Self::Target) -> &'a NodeId {
        &target.node_id
    }

    fn target_description(&self, target: &Self::Target) -> String {
        format!("{} ({:?})", target.address, target.transport)
    }

    fn replicate_cluster_raft_to_target(
        &mut self,
        state: &ClusterBootstrapState,
        target: &Self::Target,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError> {
        self.host
            .replicate_cluster_raft_to_endpoint(self.paths, state, target, timeout)
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
        self.host.record_endpoint_command_connectivity(
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
    type Target = ClusterEndpointRecord;

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
                .map_err(Into::into)
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
            ClusterEndpointSelector::select_internal_async(
                runtime,
                paths,
                state,
                ClusterEndpointInternalSelectionPurpose::LiveClusterStateAuthorityCommand,
            )
            .await
        })
    }

    fn target_node_id<'a>(&self, target: &'a Self::Target) -> &'a NodeId {
        &target.node_id
    }

    fn target_description(&self, target: &Self::Target) -> String {
        format!("{} ({:?})", target.address, target.transport)
    }

    fn replicate_cluster_raft_to_target_async<'a>(
        &'a mut self,
        state: &'a ClusterBootstrapState,
        target: &'a Self::Target,
        timeout: Duration,
    ) -> sinomo_kv::ClusterKvRaftQuorumFuture<'a, ClusterRaftAppendResponse, NetworkError> {
        let paths = self.paths;
        let mut endpoint_options = self.append_options;
        let host = &mut *self.host;
        Box::pin(async move {
            endpoint_options.timeout = timeout;
            replicate_cluster_raft_to_endpoint_async_with_host(
                paths,
                state,
                target,
                &[],
                endpoint_options,
                host,
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
            host.record_endpoint_command_connectivity(
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
