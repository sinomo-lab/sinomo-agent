pub(crate) use crate::connectivity_publish::{
    ClusterConnectivityPublishHost, ClusterNetworkRecordExpiryHost,
};
use crate::control_plane_driver::{ClusterControlPlaneDriver, ClusterControlPlaneTransport};
use crate::invite_publish::ClusterInvitePublishHost;
use crate::kv_local_store::locked_cache_cluster_kv_snapshot_if_newer_than_raft;
use crate::kv_local_store::locked_kv_state_authority_id;
use crate::kv_snapshot::ClusterKvSnapshotRefreshHost;
use crate::kv_txn_quorum::{
    commit_cluster_kv_txn_with_quorum_response_with_host,
    read_cluster_kv_authoritative_snapshot_with_host,
};
use crate::member_mutation::ClusterMemberMutationHost;
use crate::member_publish::{ClusterLocalMemberPublishHost, ClusterMemberPublishHost};
use crate::membership_rpc::{ClusterMemberPublishRequest, ClusterMemberPublishResponse};
use crate::prelude::*;
use crate::raft_quorum::catch_up_cluster_raft_member_with_control_plane_driver;
use crate::state_authority_reachability::{
    ClusterStateAuthorityReachabilityHost, cluster_has_local_quorum_with_host,
};
use crate::status_publish::{ClusterNetworkInfoPublishHost, ClusterNodeStatusPublishHost};
#[allow(unused_imports)]
use sinomo_kv::{InProcessKvStore, KvAuthoritativeRead, KvSnapshot, KvTxnRequest};
use sinomo_network::ClusterConnectivityRecord;
use sinomo_network::ClusterNetworkInfoRecord;
use sinomo_network::ClusterPaths;
use sinomo_network::NetworkError;
use sinomo_network::{
    CLUSTER_MEMBER_PUBLISH_ROUTE, CLUSTER_MEMBERSHIP_RPC_CATEGORY, ObjectRpcRequest,
    ObjectRpcResponse,
};
use std::time::Duration;

impl<T> ClusterStateAuthorityReachabilityHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    fn probe_cluster_state_authority_node(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        timeout: Duration,
    ) -> bool {
        self.transport
            .probe_cluster_state_authority_node(state, target_node_id, timeout)
    }
}

impl<T> ClusterKvSnapshotRefreshHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    fn known_kv_state_authority_id(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<Option<NodeId>, NetworkError> {
        locked_kv_state_authority_id(paths, state)
    }

    fn cache_cluster_kv_snapshot_if_newer_than_raft(
        &mut self,
        paths: &ClusterPaths,
        local_node_id: &NodeId,
        snapshot: KvSnapshot,
    ) -> Result<bool, NetworkError> {
        locked_cache_cluster_kv_snapshot_if_newer_than_raft(paths, local_node_id, snapshot)
    }

    fn call_node_object_rpc(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, NetworkError> {
        self.transport
            .call_node_object_rpc(state, target_node_id, request, timeout)
    }
}

impl<T> ClusterMemberPublishHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    type Error = NetworkError;

    fn authoritative_cluster_kv_snapshot(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
    ) -> Result<KvAuthoritativeRead<KvSnapshot>, Self::Error> {
        ClusterMemberMutationHost::authoritative_cluster_kv_snapshot(self, paths, state)
    }

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        request: KvTxnRequest,
    ) -> Result<sinomo_kv::ClusterKvTxnCommitResult, Self::Error> {
        ClusterMemberMutationHost::commit_cluster_kv_txn_with_quorum(self, paths, state, request)
    }
}

impl<T> ClusterLocalMemberPublishHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    fn cluster_has_local_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> bool {
        let options = self.timeouts.state_authority_reachability_options();
        cluster_has_local_quorum_with_host(paths, state, options, self)
    }

    fn authoritative_cluster_kv_snapshot(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
    ) -> Result<KvAuthoritativeRead<KvSnapshot>, NetworkError> {
        ClusterMemberMutationHost::authoritative_cluster_kv_snapshot(self, paths, state)
    }

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        request: KvTxnRequest,
    ) -> Result<sinomo_kv::ClusterKvTxnCommitResult, NetworkError> {
        ClusterMemberMutationHost::commit_cluster_kv_txn_with_quorum(self, paths, state, request)
    }

    fn send_cluster_member_publish(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterMemberPublishRequest,
        timeout: Duration,
    ) -> Result<ClusterMemberPublishResponse, NetworkError> {
        let request = ObjectRpcRequest::encode_body(
            CLUSTER_MEMBER_PUBLISH_ROUTE,
            CLUSTER_MEMBERSHIP_RPC_CATEGORY,
            request,
        )?;
        self.transport
            .call_endpoint_object_rpc(endpoint, &request, timeout)?
            .decode_body()
    }
}

impl<T> ClusterInvitePublishHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    type Error = NetworkError;

    fn authoritative_cluster_kv_snapshot(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
    ) -> Result<KvAuthoritativeRead<KvSnapshot>, Self::Error> {
        ClusterMemberMutationHost::authoritative_cluster_kv_snapshot(self, paths, state)
    }

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        request: KvTxnRequest,
    ) -> Result<sinomo_kv::ClusterKvTxnCommitResult, Self::Error> {
        ClusterMemberMutationHost::commit_cluster_kv_txn_with_quorum(self, paths, state, request)
    }
}

impl<T> ClusterNetworkRecordExpiryHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    type Error = NetworkError;

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        request: KvTxnRequest,
    ) -> Result<sinomo_kv::ClusterKvTxnCommitResult, Self::Error> {
        let options = self.timeouts.kv_txn_quorum_options(true);
        commit_cluster_kv_txn_with_quorum_response_with_host(paths, state, request, options, self)
    }
}

impl<T> ClusterNetworkInfoPublishHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    type Error = NetworkError;

    fn published_network_info_matches(
        &mut self,
        paths: &ClusterPaths,
        info: &ClusterNetworkInfoRecord,
    ) -> bool {
        self.transport.published_network_info_matches(paths, info)
    }

    fn remember_published_network_info(
        &mut self,
        paths: &ClusterPaths,
        info: &ClusterNetworkInfoRecord,
    ) {
        self.transport.remember_published_network_info(paths, info);
    }

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        request: KvTxnRequest,
    ) -> Result<KvSnapshot, Self::Error> {
        let options = self.timeouts.kv_txn_quorum_options(true);
        commit_cluster_kv_txn_with_quorum_response_with_host(paths, state, request, options, self)
            .map(|result| result.snapshot)
    }
}

impl<T> ClusterNodeStatusPublishHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    type Error = NetworkError;

    fn published_node_status_matches(
        &mut self,
        paths: &ClusterPaths,
        status: &ClusterNodeStatusRecord,
    ) -> bool {
        self.transport.published_node_status_matches(paths, status)
    }

    fn remember_published_node_status(
        &mut self,
        paths: &ClusterPaths,
        status: &ClusterNodeStatusRecord,
    ) {
        self.transport.remember_published_node_status(paths, status);
    }

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        request: KvTxnRequest,
    ) -> Result<KvSnapshot, Self::Error> {
        let options = self.timeouts.kv_txn_quorum_options(true);
        commit_cluster_kv_txn_with_quorum_response_with_host(paths, state, request, options, self)
            .map(|result| result.snapshot)
    }
}

impl<T> ClusterConnectivityPublishHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    type Error = NetworkError;

    fn published_connectivity_record_set_matches(
        &mut self,
        paths: &ClusterPaths,
        source_node_id: &NodeId,
        records: &[ClusterConnectivityRecord],
    ) -> bool {
        self.transport
            .published_connectivity_record_set_matches(paths, source_node_id, records)
    }

    fn remember_published_connectivity_record_set(
        &mut self,
        paths: &ClusterPaths,
        source_node_id: &NodeId,
        records: &[ClusterConnectivityRecord],
    ) {
        self.transport
            .remember_published_connectivity_record_set(paths, source_node_id, records);
    }

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        request: KvTxnRequest,
    ) -> Result<KvSnapshot, Self::Error> {
        let options = self.timeouts.kv_txn_quorum_options(true);
        commit_cluster_kv_txn_with_quorum_response_with_host(paths, state, request, options, self)
            .map(|result| result.snapshot)
    }
}

impl<T> ClusterMemberMutationHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    fn authoritative_cluster_kv_snapshot(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
    ) -> Result<KvAuthoritativeRead<KvSnapshot>, NetworkError> {
        let options = self.timeouts.kv_txn_quorum_options(true);
        self.with_active_cluster_paths(paths, |driver| {
            read_cluster_kv_authoritative_snapshot_with_host(paths, state, options, driver)
        })
    }

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        request: KvTxnRequest,
    ) -> Result<sinomo_kv::ClusterKvTxnCommitResult, NetworkError> {
        let options = self.timeouts.kv_txn_quorum_options(true);
        self.with_active_cluster_paths(paths, |driver| {
            commit_cluster_kv_txn_with_quorum_response_with_host(
                paths, state, request, options, driver,
            )
        })
    }

    fn prove_cluster_raft_member_caught_up(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        node_id: &NodeId,
        required_log_index: u64,
        now: time::OffsetDateTime,
    ) -> Result<sinomo_kv::ClusterKvRaftCatchUpProof, NetworkError> {
        let response = self.with_active_cluster_paths(paths, |driver| {
            catch_up_cluster_raft_member_with_control_plane_driver(paths, state, node_id, driver)
        })?;
        if !response.accepted || response.append.match_index < required_log_index {
            return Err(NetworkError::ClusterReadOnly {
                detail: format!(
                    "learner `{node_id}` caught up only to Raft index {} ({} required): {}",
                    response.append.match_index, required_log_index, response.detail
                ),
            });
        }
        Ok(sinomo_kv::ClusterKvRaftCatchUpProof {
            node_id: node_id.clone(),
            term: response.append.term,
            required_log_index,
            observed_match_index: response.append.match_index,
            observed_at: now,
        })
    }
}
