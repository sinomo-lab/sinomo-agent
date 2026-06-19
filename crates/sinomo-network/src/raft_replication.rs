use crate::ClusterPaths;
use crate::control_plane_options::{
    ClusterRaftAppendReplicationOptions, ClusterRaftAppendReplicationOutcome,
};
use crate::error::NetworkError;
#[cfg(test)]
use crate::kv_local_store::cluster_kv_raft_store;
use crate::prelude::*;
use crate::rpc_future::ClusterMembershipRpcFuture;
use sinomo_kv::{ClusterRaftAppendRequest, ClusterRaftAppendResponse};
use std::time::Duration;

pub trait ClusterRaftAppendReplicationHost {
    fn last_local_raft_log_index(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<u64, NetworkError>;

    fn build_raft_append_request(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        next_index: u64,
    ) -> Result<ClusterRaftAppendRequest, NetworkError>;

    fn send_raft_append(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError>;

    fn on_raft_append_sent(&mut self) {}
}

pub trait ClusterRaftAppendReplicationAsyncHost {
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

    fn send_raft_append_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterRaftAppendResponse>;

    fn on_raft_append_sent_async<'a>(&'a mut self) -> ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

struct NetworkClusterKvRaftAppendReplicationHost<'a, H> {
    paths: &'a ClusterPaths,
    options: ClusterRaftAppendReplicationOptions,
    host: &'a mut H,
}

impl<H> sinomo_kv::ClusterKvRaftAppendReplicationHost
    for NetworkClusterKvRaftAppendReplicationHost<'_, H>
where
    H: ClusterRaftAppendReplicationHost,
{
    type Error = NetworkError;
    type Target = ClusterEndpointRecord;

    fn last_local_raft_log_index(
        &mut self,
        state: &ClusterBootstrapState,
    ) -> Result<u64, Self::Error> {
        self.host.last_local_raft_log_index(self.paths, state)
    }

    fn build_raft_append_request(
        &mut self,
        state: &ClusterBootstrapState,
        next_index: u64,
    ) -> Result<ClusterRaftAppendRequest, Self::Error> {
        self.host
            .build_raft_append_request(self.paths, state, next_index)
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
        self.host.on_raft_append_sent();
        Ok(())
    }

    fn send_raft_append(
        &mut self,
        target: &Self::Target,
        request: &ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> Result<ClusterRaftAppendResponse, Self::Error> {
        self.host.send_raft_append(target, request, timeout)
    }
}

struct NetworkClusterKvRaftAppendReplicationAsyncHost<'a, H> {
    paths: &'a ClusterPaths,
    options: ClusterRaftAppendReplicationOptions,
    host: &'a mut H,
}

impl<H> sinomo_kv::ClusterKvRaftAppendReplicationAsyncHost
    for NetworkClusterKvRaftAppendReplicationAsyncHost<'_, H>
where
    H: ClusterRaftAppendReplicationAsyncHost,
{
    type Error = NetworkError;
    type Target = ClusterEndpointRecord;

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
        target: &Self::Target,
        remaining: Duration,
        request: &ClusterRaftAppendRequest,
    ) -> Duration {
        self.options
            .append_attempt_timeout(target, remaining, request)
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
        self.host.send_raft_append_async(target, request, timeout)
    }
}

pub fn replicate_cluster_raft_to_endpoint_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    watch_events: &[KvEvent],
    options: ClusterRaftAppendReplicationOptions,
    host: &mut impl ClusterRaftAppendReplicationHost,
) -> Result<ClusterRaftAppendReplicationOutcome, NetworkError> {
    let mut kv_host = NetworkClusterKvRaftAppendReplicationHost {
        paths,
        options,
        host,
    };
    sinomo_kv::replicate_cluster_kv_raft_to_target_with_host(
        state,
        endpoint,
        watch_events,
        options.kv_options(),
        &mut kv_host,
    )
}

pub async fn replicate_cluster_raft_to_endpoint_async_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    watch_events: &[KvEvent],
    options: ClusterRaftAppendReplicationOptions,
    host: &mut impl ClusterRaftAppendReplicationAsyncHost,
) -> Result<ClusterRaftAppendReplicationOutcome, NetworkError> {
    let mut kv_host = NetworkClusterKvRaftAppendReplicationAsyncHost {
        paths,
        options,
        host,
    };
    sinomo_kv::replicate_cluster_kv_raft_to_target_async_with_host(
        state,
        endpoint,
        watch_events,
        options.kv_options(),
        &mut kv_host,
    )
    .await
}

#[cfg(test)]
struct LocalClusterRaftAppendReplicationHost<S, O> {
    send_raft_append: S,
    on_raft_append_sent: O,
}

#[cfg(test)]
impl<S, O> ClusterRaftAppendReplicationHost for LocalClusterRaftAppendReplicationHost<S, O>
where
    S: FnMut(
        &ClusterEndpointRecord,
        &ClusterRaftAppendRequest,
        Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError>,
    O: FnMut(),
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
        (self.send_raft_append)(endpoint, request, timeout)
    }

    fn on_raft_append_sent(&mut self) {
        (self.on_raft_append_sent)();
    }
}

#[cfg(test)]
pub fn replicate_cluster_raft_to_endpoint_with_local_host<S, O>(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    watch_events: &[KvEvent],
    options: ClusterRaftAppendReplicationOptions,
    send_raft_append: S,
    on_raft_append_sent: O,
) -> Result<ClusterRaftAppendReplicationOutcome, NetworkError>
where
    S: FnMut(
        &ClusterEndpointRecord,
        &ClusterRaftAppendRequest,
        Duration,
    ) -> Result<ClusterRaftAppendResponse, NetworkError>,
    O: FnMut(),
{
    let mut host = LocalClusterRaftAppendReplicationHost {
        send_raft_append,
        on_raft_append_sent,
    };
    replicate_cluster_raft_to_endpoint_with_host(
        paths,
        state,
        endpoint,
        watch_events,
        options,
        &mut host,
    )
}
