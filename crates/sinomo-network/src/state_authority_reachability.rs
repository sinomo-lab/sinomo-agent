use crate::endpoint_selector::{ClusterEndpointInternalSelectionPurpose, ClusterEndpointSelector};
use crate::route::{cluster_endpoint_prefers_relay_with_graph, cluster_route_next_hop};
use crate::{ClusterConnectivityGraph, ClusterPaths, NetworkError};
use sinomo_core::{
    ClusterBootstrapState, ClusterEndpointPrivacy, ClusterEndpointRecord, ClusterTransportKind,
};
use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterStateAuthorityReachabilityOptions {
    pub timeout: Duration,
    pub tcp_command_timeout: Duration,
    pub iroh_command_timeout: Duration,
    pub tcp_foreground_attempt_timeout: Duration,
    pub public_foreground_attempt_timeout: Duration,
    pub iroh_foreground_attempt_timeout: Duration,
}

impl ClusterStateAuthorityReachabilityOptions {
    pub fn command_timeout(
        &self,
        endpoint: &ClusterEndpointRecord,
        remaining: Duration,
    ) -> Duration {
        match endpoint.transport {
            ClusterTransportKind::Iroh => remaining.min(self.iroh_command_timeout),
            _ => remaining.min(self.tcp_command_timeout),
        }
    }

    pub fn fair_command_timeout(
        &self,
        endpoint: &ClusterEndpointRecord,
        remaining: Duration,
    ) -> Duration {
        let attempt_cap = match endpoint.transport {
            ClusterTransportKind::Iroh => self.iroh_foreground_attempt_timeout,
            _ if matches!(endpoint.privacy, ClusterEndpointPrivacy::InternetDirect) => {
                self.public_foreground_attempt_timeout
            }
            _ => self.tcp_foreground_attempt_timeout,
        };
        self.command_timeout(endpoint, remaining.min(attempt_cap))
    }
}

pub trait ClusterStateAuthorityReachabilityHost {
    fn probe_cluster_state_authority(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> bool;
}

pub trait ClusterStateAuthorityReachabilityAsyncHost {
    fn probe_cluster_state_authority_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<bool, NetworkError>> + Send + 'a>>;
}

pub fn cluster_reachable_state_authority_count_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterStateAuthorityReachabilityOptions,
    host: &mut impl ClusterStateAuthorityReachabilityHost,
) -> usize {
    let quorum_size = sinomo_kv::cluster_kv_raft_quorum_size_for_state(state);
    let mut voters = BTreeSet::new();
    if sinomo_kv::cluster_kv_raft_local_is_voter(state) {
        voters.insert(state.local_node_id.clone());
        if voters.len() >= quorum_size {
            return voters.len();
        }
    }
    let deadline = Instant::now() + options.timeout;
    let targets = ClusterEndpointSelector::new(paths, state)
        .select_internal(ClusterEndpointInternalSelectionPurpose::LiveClusterStateAuthorityCommand);
    let graph = ClusterConnectivityGraph::read_for_local(paths, state);
    for (index, endpoint) in targets.iter().enumerate() {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            break;
        }
        if host.probe_cluster_state_authority(
            endpoint,
            cluster_state_authority_probe_timeout(&graph, state, endpoint, options, remaining),
        ) {
            voters.insert(endpoint.node_id.clone());
            if voters.len() >= quorum_size {
                break;
            }
        }
        if index + 1 >= targets.len() {
            break;
        }
    }
    voters.len()
}

pub async fn cluster_reachable_state_authority_count_async_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterStateAuthorityReachabilityOptions,
    host: &mut impl ClusterStateAuthorityReachabilityAsyncHost,
) -> Result<usize, NetworkError> {
    let quorum_size = sinomo_kv::cluster_kv_raft_quorum_size_for_state(state);
    let mut voters = BTreeSet::new();
    if sinomo_kv::cluster_kv_raft_local_is_voter(state) {
        voters.insert(state.local_node_id.clone());
        if voters.len() >= quorum_size {
            return Ok(voters.len());
        }
    }
    let deadline = Instant::now() + options.timeout;
    let targets = ClusterEndpointSelector::new(paths, state)
        .select_internal(ClusterEndpointInternalSelectionPurpose::LiveClusterStateAuthorityCommand);
    let graph = ClusterConnectivityGraph::read_for_local(paths, state);
    for (index, endpoint) in targets.iter().enumerate() {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            break;
        }
        if host
            .probe_cluster_state_authority_async(
                endpoint,
                cluster_state_authority_probe_timeout(&graph, state, endpoint, options, remaining),
            )
            .await?
        {
            voters.insert(endpoint.node_id.clone());
            if voters.len() >= quorum_size {
                break;
            }
        }
        if index + 1 >= targets.len() {
            break;
        }
    }
    Ok(voters.len())
}

fn cluster_state_authority_probe_timeout(
    graph: &ClusterConnectivityGraph,
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    options: ClusterStateAuthorityReachabilityOptions,
    remaining: Duration,
) -> Duration {
    if cluster_route_next_hop(graph, &state.local_node_id, &endpoint.node_id).is_some()
        && cluster_endpoint_prefers_relay_with_graph(graph, state, endpoint)
    {
        options.command_timeout(endpoint, remaining)
    } else {
        options.fair_command_timeout(endpoint, remaining)
    }
}

pub fn cluster_has_local_quorum_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterStateAuthorityReachabilityOptions,
    host: &mut impl ClusterStateAuthorityReachabilityHost,
) -> bool {
    cluster_reachable_state_authority_count_with_host(paths, state, options, host)
        >= sinomo_kv::cluster_kv_raft_quorum_size_for_state(state)
}

pub async fn cluster_has_local_quorum_async_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterStateAuthorityReachabilityOptions,
    host: &mut impl ClusterStateAuthorityReachabilityAsyncHost,
) -> Result<bool, NetworkError> {
    Ok(
        cluster_reachable_state_authority_count_async_with_host(paths, state, options, host)
            .await?
            >= sinomo_kv::cluster_kv_raft_quorum_size_for_state(state),
    )
}
