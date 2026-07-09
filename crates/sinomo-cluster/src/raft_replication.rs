use crate::control_plane_options::{
    ClusterControlPlaneTimeouts, ClusterRaftAppendReplicationOptions,
};
use crate::kv_local_store::ClusterKvPathsExt;
use crate::kv_local_store::kv_err;
use crate::prelude::*;
#[allow(unused_imports)]
#[allow(unused_imports)]
use sinomo_kv::KvError;
use sinomo_kv::KvEvent;
use sinomo_kv::{
    ClusterKvRaftAppendReplicationOutcome, ClusterRaftAppendRequest, ClusterRaftAppendResponse,
};
use sinomo_network::ClusterMembershipRpcFuture;
use sinomo_network::ClusterPaths;
use sinomo_network::NetworkError;
use std::time::Duration;

#[cfg(any(test, feature = "test-support"))]
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

#[cfg(any(test, feature = "test-support"))]
pub trait ClusterKvAppendReplicationAsyncHost {
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

    fn send_kv_replication_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterRaftAppendResponse>;

    fn on_kv_replication_sent_async<'a>(&'a mut self) -> ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

#[cfg(any(test, feature = "test-support"))]
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

#[cfg(any(test, feature = "test-support"))]
impl<T> ClusterRaftAppendReplicationAsyncHost for T
where
    T: ClusterKvAppendReplicationAsyncHost,
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

    fn send_raft_append_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> ClusterMembershipRpcFuture<'a, ClusterRaftAppendResponse> {
        self.send_kv_replication_async(endpoint, request, timeout)
    }

    fn on_raft_append_sent_async<'a>(&'a mut self) -> ClusterMembershipRpcFuture<'a, ()> {
        self.on_kv_replication_sent_async()
    }
}

#[cfg(any(test, feature = "test-support"))]
struct NetworkClusterKvRaftAppendReplicationHost<'a, H> {
    paths: &'a ClusterPaths,
    options: ClusterRaftAppendReplicationOptions,
    host: &'a mut H,
}

#[cfg(any(test, feature = "test-support"))]
impl<H> sinomo_kv::ClusterKvRaftAppendReplicationHost
    for NetworkClusterKvRaftAppendReplicationHost<'_, H>
where
    H: ClusterRaftAppendReplicationHost,
{
    type Error = NetworkError;

    fn kv_error(error: KvError) -> Self::Error {
        crate::kv_local_store::kv_err(error)
    }
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

#[cfg(any(test, feature = "test-support"))]
struct NetworkClusterKvRaftAppendReplicationAsyncHost<'a, H> {
    paths: &'a ClusterPaths,
    options: ClusterRaftAppendReplicationOptions,
    host: &'a mut H,
}

#[cfg(any(test, feature = "test-support"))]
impl<H> sinomo_kv::ClusterKvRaftAppendReplicationAsyncHost
    for NetworkClusterKvRaftAppendReplicationAsyncHost<'_, H>
where
    H: ClusterRaftAppendReplicationAsyncHost,
{
    type Error = NetworkError;

    fn kv_error(error: KvError) -> Self::Error {
        crate::kv_local_store::kv_err(error)
    }
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

#[cfg(any(test, feature = "test-support"))]
pub fn replicate_cluster_raft_to_endpoint_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    watch_events: &[KvEvent],
    options: ClusterRaftAppendReplicationOptions,
    host: &mut impl ClusterRaftAppendReplicationHost,
) -> Result<ClusterKvRaftAppendReplicationOutcome, NetworkError> {
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

#[cfg(any(test, feature = "test-support"))]
pub async fn replicate_cluster_raft_to_endpoint_async_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    watch_events: &[KvEvent],
    timeouts: ClusterControlPlaneTimeouts,
    timeout: Duration,
    host: &mut impl ClusterRaftAppendReplicationAsyncHost,
) -> Result<ClusterKvRaftAppendReplicationOutcome, NetworkError> {
    replicate_cluster_raft_to_endpoint_async_with_options(
        paths,
        state,
        endpoint,
        watch_events,
        timeouts.raft_append_replication_options(timeout),
        host,
    )
    .await
}

#[cfg(any(test, feature = "test-support"))]
pub async fn replicate_cluster_kv_to_endpoint_async_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    watch_events: &[KvEvent],
    timeouts: ClusterControlPlaneTimeouts,
    timeout: Duration,
    host: &mut impl ClusterKvAppendReplicationAsyncHost,
) -> Result<ClusterKvRaftAppendReplicationOutcome, NetworkError> {
    replicate_cluster_raft_to_endpoint_async_with_options(
        paths,
        state,
        endpoint,
        watch_events,
        timeouts.raft_append_replication_options(timeout),
        host,
    )
    .await
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) async fn replicate_cluster_raft_to_endpoint_async_with_options(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    watch_events: &[KvEvent],
    options: ClusterRaftAppendReplicationOptions,
    host: &mut impl ClusterRaftAppendReplicationAsyncHost,
) -> Result<ClusterKvRaftAppendReplicationOutcome, NetworkError> {
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

#[cfg(any(test, feature = "test-support"))]
pub struct LocalClusterRaftAppendReplicationHost<S, O> {
    send_raft_append: S,
    on_raft_append_sent: O,
}

#[cfg(any(test, feature = "test-support"))]
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
        paths
            .kv_paths()
            .locked_last_raft_log_index(&state.local_node_id)
            .map_err(kv_err)
    }

    fn build_raft_append_request(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        next_index: u64,
    ) -> Result<ClusterRaftAppendRequest, NetworkError> {
        paths
            .kv_paths()
            .locked_build_raft_append_request_from_next_index(state, next_index)
            .map_err(kv_err)
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

#[cfg(any(test, feature = "test-support"))]
pub fn replicate_cluster_raft_to_endpoint_with_local_host<S, O>(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    watch_events: &[KvEvent],
    options: ClusterRaftAppendReplicationOptions,
    send_raft_append: S,
    on_raft_append_sent: O,
) -> Result<ClusterKvRaftAppendReplicationOutcome, NetworkError>
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
