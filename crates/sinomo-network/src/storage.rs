use crate::ClusterPaths;
use crate::connectivity::ClusterConnectivityRecord;
pub(crate) use crate::connectivity_publish::{
    ClusterConnectivityPublishHost, ClusterNetworkRecordExpiryHost,
};
#[cfg(test)]
pub(crate) use crate::connectivity_publish::{
    MAX_CLUSTER_CONNECTIVITY_SOURCE_RECORDS, cluster_connectivity_full_sync_batch_ops,
    cluster_connectivity_stable_eq, cluster_connectivity_sync_is_noop,
    cluster_network_expiry_delete_ops, expire_cluster_network_record_ops,
    expire_cluster_network_records,
};
#[cfg(test)]
pub(crate) use crate::connectivity_store::{
    local_cluster_connectivity_prune_passes_for_test,
    local_cluster_connectivity_record_cache_capacity_for_test,
    local_cluster_connectivity_record_cache_contains_for_test,
    remove_local_cluster_connectivity_record_cache_entries_under_root_for_test,
    reset_local_cluster_connectivity_prune_passes_for_test,
    write_local_cluster_connectivity_record_set,
};
use crate::control_plane_driver::{ClusterControlPlaneDriver, ClusterControlPlaneTransport};
use crate::endpoint_records::cleanup_cluster_iroh_endpoints;
use crate::error::NetworkError;
use crate::invite_publish::{
    ClusterInvitePublishHost, handle_cluster_invite_publish_request_with_host,
};
use crate::join_registration::{
    ClusterJoinRegistrationHost, handle_cluster_join_registration_request_with_host,
};
#[cfg(test)]
pub(crate) use crate::keyspace::cluster_certificate_key;
use crate::keyspace::cluster_member_key;
use crate::kv_forward::ClusterKvTxnForwardHost;
use crate::kv_local_store::locked_cluster_raft_known_leader_id;
#[cfg(test)]
pub(crate) use crate::kv_local_store::write_imported_cluster_kv_snapshot_if_changed;
#[cfg(test)]
pub(crate) use crate::kv_local_store::{
    LocalClusterRaftCommandStorage, append_cluster_raft_command_uncommitted,
    append_cluster_raft_command_with_response, cached_cluster_kv_reader_node_id_for_test,
    cluster_kv_cached_snapshot_reads_for_test, cluster_kv_current_snapshot_reads_for_test,
    cluster_kv_metadata_cache_capacity_for_test, cluster_kv_reader_node_id_cache_contains_for_test,
    cluster_raft_wal_paths, open_cluster_raft_kv_node_async,
    read_cluster_kv_cached_snapshot_if_present, read_cluster_kv_prefix_entries,
    read_cluster_kv_prefix_entries_limited,
    read_cluster_kv_prefix_entries_limited_by_payload_bytes, read_cluster_kv_snapshot,
    read_cluster_kv_store, record_cluster_kv_reader_node_id_cache_for_test,
    refresh_cluster_kv_reader_node_id_cache_for_test,
    remove_cluster_kv_reader_node_id_cache_entries_under_root_for_test,
    reset_cluster_kv_cached_snapshot_reads_for_test,
    reset_cluster_kv_current_snapshot_reads_for_test, restore_cluster_raft_from_snapshot_if_newer,
};
pub(crate) use crate::kv_local_store::{
    append_cluster_raft_command, apply_cluster_raft_append, cluster_kv_raft_store,
    locked_restore_cluster_raft_from_snapshot_if_newer, open_cluster_raft_kv_node,
    read_cluster_kv_current_snapshot, write_cluster_kv_snapshot,
};
pub(crate) use crate::kv_read_model::read_cluster_capability_policies_from_snapshot;
#[cfg(test)]
pub(crate) use crate::kv_read_model::{
    read_cluster_endpoints_from_kv, read_cluster_network_info_from_kv,
};
use crate::kv_snapshot::{
    ClusterKvSnapshotRefreshHost, refresh_current_cluster_kv_store_with_host,
};
use crate::kv_txn_quorum::commit_cluster_kv_txn_with_quorum_response_with_host;
#[cfg(test)]
pub(crate) use crate::kv_txn_quorum::handle_cluster_kv_txn_request_with_host;
#[cfg(test)]
pub(crate) use crate::kv_watch_store::handle_cluster_kv_watch_events;
#[cfg(test)]
pub(crate) use crate::kv_watch_store::{
    append_cluster_kv_watch_events, append_cluster_kv_watch_events_for_active_watches,
    cached_cluster_kv_watch_events_contain_all, cached_cluster_kv_watch_events_for_test,
    cached_cluster_kv_watch_registration_for_test, cancel_cluster_kv_watch,
    cluster_kv_watch_allocator_trust_contains_for_test, cluster_kv_watch_dir_scans_for_test,
    cluster_kv_watch_drain_once_calls_for_test, cluster_kv_watch_event_cache_capacity_for_test,
    cluster_kv_watch_event_cache_contains_for_test, cluster_kv_watch_event_log_read_bytes_for_test,
    cluster_kv_watch_event_log_write_bytes_for_test, cluster_kv_watch_events,
    cluster_kv_watch_notifier_capacity_for_test, cluster_kv_watch_notifier_generation_for_test,
    cluster_kv_watch_notifier_has_key_for_test, cluster_kv_watch_notifier_has_raw_key_for_test,
    cluster_kv_watch_notifier_waiter_count_for_test,
    cluster_kv_watch_registration_cache_capacity_for_test,
    cluster_kv_watch_registration_cache_contains_for_test,
    cluster_kv_watch_unchanged_wakeups_for_test, create_cluster_kv_watch, drain_cluster_kv_watch,
    drain_cluster_kv_watch_once_limited_by_payload_bytes,
    fail_next_cluster_kv_watch_allocator_persist_for_test,
    record_cluster_kv_watch_allocator_trust_for_test, record_cluster_kv_watch_event_cache_for_test,
    record_cluster_kv_watch_notifier_waiter_key_for_test,
    record_cluster_kv_watch_registration_cache_for_test,
    refresh_cluster_kv_watch_allocator_trust_for_test,
    refresh_cluster_kv_watch_event_cache_for_test,
    refresh_cluster_kv_watch_registration_cache_for_test,
    remove_cluster_kv_watch_allocator_trust_entries_under_root_for_test,
    remove_cluster_kv_watch_event_cache_entries_under_root_for_test,
    remove_cluster_kv_watch_notifier_entries_under_root_for_test,
    remove_cluster_kv_watch_registration_cache_entries_under_root_for_test,
    reset_cluster_kv_watch_dir_scans_for_test, reset_cluster_kv_watch_drain_once_calls_for_test,
    reset_cluster_kv_watch_event_log_read_bytes_for_test,
    reset_cluster_kv_watch_event_log_write_bytes_for_test,
    reset_cluster_kv_watch_unchanged_wakeups_for_test, touch_cluster_kv_watch_notifier_for_test,
    wait_for_cluster_kv_watch_change_for_test,
};
use crate::local_membership::apply_cluster_node_role;
#[cfg(test)]
pub(crate) use crate::local_membership::apply_local_daemon_membership_observation;
pub(crate) use crate::local_membership::cluster_state_needs_join_registration;
#[cfg(test)]
pub(crate) use crate::local_membership::{
    ensure_main_daemon_cluster_member,
    prepare_cluster_join_registration_state_with_local_daemon_members,
};
#[cfg(test)]
pub use crate::local_state_store::remove_local_cluster_state;
pub use crate::local_state_store::{
    read_cluster_state, read_cluster_state_without_kv, write_cluster_state,
};
use crate::member_mutation::{
    ClusterMemberMutationAsyncHost, ClusterMemberMutationHost, ClusterMemberRemoval,
    ClusterNodeRoleUpdate, ClusterRaftRoleUpdate,
};
use crate::member_publish::{
    ClusterLocalMemberPublishHost, ClusterMemberPublishHost,
    handle_cluster_member_publish_request_with_host,
};
pub(crate) use crate::member_store::{
    apply_cluster_raft_role, cluster_legacy_shadow_orphan_artifact_cleanup_txns_from_store,
    cluster_legacy_shadow_orphan_artifact_node_ids_from_store,
    cluster_member_raft_update_txn_from_store, cluster_member_removal_txns_from_store,
    cluster_member_update_txn_from_store, cluster_raft_state_for_role,
    cluster_state_sync_txn_from_store, merge_cluster_state_from_peer,
};
#[cfg(test)]
pub(crate) use crate::member_store::{
    cluster_member_put_command, cluster_member_update_ops_from_store,
    cluster_raft_config_from_store,
};
use crate::membership::cluster_member_is_legacy_shadow_daemon;
use crate::membership_forward::ClusterMembershipRpcForwardHost;
use crate::membership_rpc::{
    ClusterAuthenticatedPeer, ClusterInvitePublishRequest, ClusterJoinRegistrationRequest,
    ClusterMemberPublishRequest, ClusterMemberPublishResponse,
};
use crate::network_info::ClusterNetworkInfoRecord;
use crate::prelude::*;
use crate::raft_election::ClusterRaftVoteTransportHost;
#[cfg(test)]
pub(crate) use crate::raft_election::elect_cluster_raft_leader_with_host;
#[cfg(test)]
pub(crate) use crate::raft_election::locked_set_cluster_raft_leader;
#[cfg(test)]
pub(crate) use crate::raft_fanout::claim_next_cluster_raft_fanout_pass_for_test;
#[cfg(test)]
pub(crate) use crate::raft_quorum::commit_cluster_raft_command_with_local_storage_and_transport;
#[cfg(test)]
pub(crate) use crate::raft_quorum::{
    ClusterRaftQuorumTransportHost, commit_cluster_raft_command_with_quorum_driver,
};
#[cfg(test)]
pub(crate) use crate::raft_replication::replicate_cluster_raft_to_endpoint_with_local_host;
#[cfg(test)]
pub(crate) use crate::records::ClusterCapabilityPolicyRecord;
#[cfg(test)]
pub(crate) use crate::route_graph_read_model::read_cluster_connectivity_graph;
#[cfg(test)]
pub(crate) use crate::route_graph_read_model::read_cluster_connectivity_graph_with_query;
use crate::rpc::{
    CLUSTER_INVITE_PUBLISH_ROUTE, CLUSTER_JOIN_REGISTER_ROUTE, CLUSTER_MEMBER_PUBLISH_ROUTE,
    CLUSTER_MEMBERSHIP_RPC_CATEGORY, ObjectRpcRequest, ObjectRpcResponse,
};
use crate::status_publish::{ClusterNetworkInfoPublishHost, ClusterNodeStatusPublishHost};
use crate::voter_reachability::{ClusterVoterReachabilityHost, cluster_has_local_quorum_with_host};
use std::collections::BTreeSet;
use std::time::Duration;

use sinomo_kv::{
    ClusterKvRaftConfigRecord, ClusterRaftAppendRequest, ClusterRaftAppendResponse,
    ClusterRaftVoteRequest, ClusterRaftVoteResponse,
};

impl<T> ClusterVoterReachabilityHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    fn probe_voter(&mut self, endpoint: &ClusterEndpointRecord, timeout: Duration) -> bool {
        self.transport.probe_voter(endpoint, timeout)
    }
}

impl<T> ClusterRaftVoteTransportHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    fn send_raft_vote(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterRaftVoteRequest,
        timeout: Duration,
    ) -> Result<ClusterRaftVoteResponse, NetworkError> {
        self.transport.send_raft_vote(endpoint, request, timeout)
    }
}

impl<T> ClusterKvSnapshotRefreshHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    fn known_raft_leader_id(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<Option<NodeId>, NetworkError> {
        locked_cluster_raft_known_leader_id(paths, state)
    }

    fn fetch_cluster_kv_snapshot(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> Result<KvSnapshot, NetworkError> {
        self.transport.fetch_cluster_kv_snapshot(endpoint, timeout)
    }

    fn restore_cluster_raft_from_snapshot_if_newer(
        &mut self,
        paths: &ClusterPaths,
        local_node_id: &NodeId,
        snapshot: KvSnapshot,
    ) -> Result<bool, NetworkError> {
        locked_restore_cluster_raft_from_snapshot_if_newer(paths, local_node_id, snapshot)
    }

    fn send_object_rpc_to_node(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, NetworkError> {
        self.transport
            .send_object_rpc_to_node(state, target_node_id, request, timeout)
    }
}

impl<T> ClusterKvTxnForwardHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    fn known_raft_leader_id(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<Option<NodeId>, NetworkError> {
        locked_cluster_raft_known_leader_id(paths, state)
    }

    fn send_object_rpc_to_node(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, NetworkError> {
        self.transport
            .send_object_rpc_to_node(state, target_node_id, request, timeout)
    }
}

impl<T> ClusterMemberPublishHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    type Error = NetworkError;

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, Self::Error> {
        let options = self.timeouts.kv_txn_quorum_options(true);
        commit_cluster_kv_txn_with_quorum_response_with_host(paths, state, txn, options, self)
            .map(|result| result.snapshot)
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
        let options = self.timeouts.voter_reachability_options();
        cluster_has_local_quorum_with_host(paths, state, options, self)
    }

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, NetworkError> {
        let options = self.timeouts.kv_txn_quorum_options(true);
        commit_cluster_kv_txn_with_quorum_response_with_host(paths, state, txn, options, self)
            .map(|result| result.snapshot)
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
            .send_cluster_membership_rpc(endpoint, &request, timeout)?
            .decode_body()
    }
}

impl<T> ClusterInvitePublishHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    type Error = NetworkError;

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, Self::Error> {
        let options = self.timeouts.kv_txn_quorum_options(true);
        commit_cluster_kv_txn_with_quorum_response_with_host(paths, state, txn, options, self)
            .map(|result| result.snapshot)
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
        txn: KvTxn,
    ) -> Result<KvSnapshot, Self::Error> {
        let options = self.timeouts.kv_txn_quorum_options(true);
        commit_cluster_kv_txn_with_quorum_response_with_host(paths, state, txn, options, self)
            .map(|result| result.snapshot)
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
        txn: KvTxn,
    ) -> Result<KvSnapshot, Self::Error> {
        let options = self.timeouts.kv_txn_quorum_options(true);
        commit_cluster_kv_txn_with_quorum_response_with_host(paths, state, txn, options, self)
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
        txn: KvTxn,
    ) -> Result<KvSnapshot, Self::Error> {
        let options = self.timeouts.kv_txn_quorum_options(true);
        commit_cluster_kv_txn_with_quorum_response_with_host(paths, state, txn, options, self)
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
        txn: KvTxn,
    ) -> Result<KvSnapshot, Self::Error> {
        let options = self.timeouts.kv_txn_quorum_options(true);
        commit_cluster_kv_txn_with_quorum_response_with_host(paths, state, txn, options, self)
            .map(|result| result.snapshot)
    }
}

impl<T> ClusterMemberMutationHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    fn current_cluster_kv_store(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<InProcessKvStore, NetworkError> {
        refresh_current_cluster_kv_store_with_host(
            paths,
            state,
            self.timeouts.kv_snapshot_refresh_options(),
            self,
        )
    }

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, NetworkError> {
        let options = self.timeouts.kv_txn_quorum_options(true);
        commit_cluster_kv_txn_with_quorum_response_with_host(paths, state, txn, options, self)
            .map(|result| result.snapshot)
    }
}

impl<T> ClusterMembershipRpcForwardHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    fn send_membership_rpc_to_node(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
    ) -> Result<ObjectRpcResponse, NetworkError> {
        self.transport.send_object_rpc_to_node(
            state,
            target_node_id,
            request,
            self.timeouts.membership_rpc_timeout,
        )
    }
}

pub fn handle_cluster_raft_rpc_request(
    paths: &ClusterPaths,
    request: ObjectRpcRequest,
    authenticated_peer: &ClusterAuthenticatedPeer,
) -> Result<ObjectRpcResponse, NetworkError> {
    let route = request.route.clone();
    let Some(payload) = sinomo_kv::handle_cluster_raft_rpc_payload(
        request.route.as_str(),
        &request.body,
        |append| handle_locked_cluster_raft_append_request(paths, append, authenticated_peer),
        |vote| handle_locked_cluster_raft_vote_request(paths, vote, authenticated_peer),
    )?
    else {
        return Err(NetworkError::UnknownRoute { route });
    };
    Ok(ObjectRpcResponse::from_encoded_body(
        payload.route,
        payload.body,
    ))
}

pub async fn handle_cluster_raft_rpc_request_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    request: ObjectRpcRequest,
    authenticated_peer: &ClusterAuthenticatedPeer,
) -> Result<ObjectRpcResponse, NetworkError> {
    let paths = paths.clone();
    let authenticated_peer = authenticated_peer.clone();
    runtime
        .spawn_blocking(move || {
            handle_cluster_raft_rpc_request(&paths, request, &authenticated_peer)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster raft rpc".to_string(),
            detail: format!("cluster raft rpc task failed: {source}"),
        })?
}

pub fn handle_cluster_membership_rpc_request_with_hosts<E, J, M, I>(
    paths: &ClusterPaths,
    request: ObjectRpcRequest,
    signer_node_id: &str,
    signer_public_key: &str,
    now: OffsetDateTime,
    join_host: &mut J,
    member_host: &mut M,
    invite_host: &mut I,
) -> Result<ObjectRpcResponse, E>
where
    E: From<NetworkError> + ToString,
    J: ClusterJoinRegistrationHost,
    M: ClusterMemberPublishHost<Error = E>,
    I: ClusterInvitePublishHost<Error = E>,
{
    match request.route.as_str() {
        CLUSTER_JOIN_REGISTER_ROUTE => {
            let payload: ClusterJoinRegistrationRequest = request.decode_body().map_err(E::from)?;
            let response =
                handle_cluster_join_registration_request_with_host(paths, payload, now, join_host)
                    .map_err(E::from)?;
            ObjectRpcResponse::encode_body(CLUSTER_JOIN_REGISTER_ROUTE, &response).map_err(E::from)
        }
        CLUSTER_INVITE_PUBLISH_ROUTE => {
            let payload: ClusterInvitePublishRequest = request.decode_body().map_err(E::from)?;
            let response = handle_cluster_invite_publish_request_with_host(
                paths,
                payload,
                signer_node_id,
                signer_public_key,
                now,
                invite_host,
            )?;
            ObjectRpcResponse::encode_body(CLUSTER_INVITE_PUBLISH_ROUTE, &response).map_err(E::from)
        }
        CLUSTER_MEMBER_PUBLISH_ROUTE => {
            let payload: ClusterMemberPublishRequest = request.decode_body().map_err(E::from)?;
            let response = handle_cluster_member_publish_request_with_host(
                paths,
                payload,
                signer_node_id,
                member_host,
            )?;
            ObjectRpcResponse::encode_body(CLUSTER_MEMBER_PUBLISH_ROUTE, &response).map_err(E::from)
        }
        other => Err(E::from(NetworkError::UnknownRoute {
            route: other.to_string(),
        })),
    }
}

pub(crate) fn handle_cluster_raft_append_request(
    paths: &ClusterPaths,
    request: ClusterRaftAppendRequest,
    authenticated_peer: &ClusterAuthenticatedPeer,
) -> Result<ClusterRaftAppendResponse, NetworkError> {
    let mut state = read_cluster_state(paths)?;
    if request.state.cluster_id != state.cluster_id {
        return cluster_raft_append_rejection(
            paths,
            &state,
            format!(
                "peer cluster `{}` does not match local cluster `{}`",
                request.state.cluster_id, state.cluster_id
            ),
        );
    }
    let leader_authorized_by_local_state =
        sinomo_kv::cluster_kv_raft_node_is_voter(&state, &request.append.leader_id)
            && cluster_authenticated_peer_matches_node(
                &state,
                authenticated_peer,
                &request.append.leader_id,
            );
    let leader_authorized_for_pending_join = cluster_state_needs_join_registration(&state)
        && request.append.leader_id == request.state.local_node_id
        && sinomo_kv::cluster_kv_raft_node_is_voter(&request.state, &request.append.leader_id)
        && cluster_authenticated_peer_matches_node(
            &request.state,
            authenticated_peer,
            &request.append.leader_id,
        );
    if !leader_authorized_by_local_state && !leader_authorized_for_pending_join {
        return cluster_raft_append_rejection(
            paths,
            &state,
            format!(
                "raft append rejected from unauthorized or mismatched leader {}",
                request.append.leader_id
            ),
        );
    }
    merge_cluster_state_from_peer(&mut state, request.state);
    write_cluster_state(paths, &state)?;
    let append = apply_cluster_raft_append(
        paths,
        &state.local_node_id,
        request.snapshot,
        request.append,
    )?;
    Ok(ClusterRaftAppendResponse {
        accepted: append.success,
        detail: if append.success {
            "raft append applied".to_string()
        } else {
            "raft append rejected by local log".to_string()
        },
        state,
        append,
    })
}

pub(crate) fn handle_locked_cluster_raft_append_request(
    paths: &ClusterPaths,
    request: ClusterRaftAppendRequest,
    authenticated_peer: &ClusterAuthenticatedPeer,
) -> Result<ClusterRaftAppendResponse, NetworkError> {
    let _raft_guard = sinomo_kv::lock_cluster_kv_raft_operation_for_paths(&paths.kv_paths())?;
    handle_cluster_raft_append_request(paths, request, authenticated_peer)
}

fn cluster_raft_append_rejection(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    detail: String,
) -> Result<ClusterRaftAppendResponse, NetworkError> {
    Ok(ClusterRaftAppendResponse {
        accepted: false,
        detail,
        state: state.clone(),
        append: cluster_kv_raft_store(paths)
            .append_entries_rejection_response(&state.local_node_id)?,
    })
}

pub(crate) fn handle_cluster_raft_vote_request(
    paths: &ClusterPaths,
    request: ClusterRaftVoteRequest,
    authenticated_peer: &ClusterAuthenticatedPeer,
) -> Result<ClusterRaftVoteResponse, NetworkError> {
    let mut state = read_cluster_state(paths)?;
    if request.state.cluster_id != state.cluster_id {
        return cluster_kv_raft_store(paths)
            .cluster_raft_vote_rejection_response(
                &state,
                format!(
                    "peer cluster `{}` does not match local cluster `{}`",
                    request.state.cluster_id, state.cluster_id
                ),
            )
            .map_err(Into::into);
    }
    let candidate_is_authorized_voter = sinomo_kv::cluster_kv_raft_candidate_is_authorized_voter(
        &state,
        &request.vote.candidate_id,
    );
    if !candidate_is_authorized_voter
        || !cluster_authenticated_peer_matches_node(
            &state,
            authenticated_peer,
            &request.vote.candidate_id,
        )
    {
        return cluster_kv_raft_store(paths)
            .cluster_raft_vote_rejection_response(
                &state,
                format!(
                    "raft vote rejected for unauthorized or mismatched candidate {}",
                    request.vote.candidate_id
                ),
            )
            .map_err(Into::into);
    }
    merge_cluster_state_from_peer(&mut state, request.state);
    write_cluster_state(paths, &state)?;
    cluster_kv_raft_store(paths)
        .apply_vote_response(&state, request.vote)
        .map_err(Into::into)
}

pub(crate) fn handle_locked_cluster_raft_vote_request(
    paths: &ClusterPaths,
    request: ClusterRaftVoteRequest,
    authenticated_peer: &ClusterAuthenticatedPeer,
) -> Result<ClusterRaftVoteResponse, NetworkError> {
    let _raft_guard = sinomo_kv::lock_cluster_kv_raft_operation_for_paths(&paths.kv_paths())?;
    handle_cluster_raft_vote_request(paths, request, authenticated_peer)
}

pub fn cluster_authenticated_peer_matches_node(
    state: &ClusterBootstrapState,
    authenticated_peer: &ClusterAuthenticatedPeer,
    node_id: &NodeId,
) -> bool {
    state.members.iter().any(|member| {
        member.node_id == *node_id
            && member.cluster_id == state.cluster_id
            && member.state.is_authorized()
            && member.node_id.as_str() == authenticated_peer.node_id
            && member.node_public_key == authenticated_peer.public_key
    })
}

/// Mirrors a cluster state into the local durable Raft store without proving
/// replication to a quorum.
///
/// Runtime cluster-control writes should go through the CLI quorum wrapper.
/// This helper remains public for bootstrapping, recovery tooling, and tests
/// that need to seed a local store deliberately.
pub fn sync_cluster_state_to_local_raft_unchecked(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<KvSnapshot, NetworkError> {
    let initial_snapshot = open_cluster_raft_kv_node(paths, &state.local_node_id)
        .map(|node| node.store().snapshot())
        .or_else(|error| {
            eprintln!("warning: replacing unreadable cluster kv raft state: {error}");
            write_cluster_kv_snapshot(paths, &InProcessKvStore::new().snapshot())?;
            Ok::<KvSnapshot, NetworkError>(InProcessKvStore::new().snapshot())
        })?;
    let store = InProcessKvStore::restore(initial_snapshot.clone());
    let txn = cluster_state_sync_txn_from_store(&store, state, false)?;
    if txn.success.is_empty() {
        return Ok(initial_snapshot);
    }
    append_cluster_raft_command(paths, &state.local_node_id, KvCommand::Txn(txn))
}

pub fn cluster_state_sync_txn(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<KvTxn, NetworkError> {
    cluster_state_sync_txn_with_current_snapshot(paths, state).map(|(_, txn)| txn)
}

pub fn sync_cluster_state_to_kv_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    host: &mut impl ClusterMemberMutationHost,
) -> Result<KvSnapshot, NetworkError> {
    let mut keyed_state = state.clone();
    host.prepare_cluster_state_for_kv_sync(paths, &mut keyed_state)?;
    cleanup_cluster_iroh_endpoints(&mut keyed_state);
    let (current_snapshot, txn) =
        cluster_state_sync_txn_with_current_snapshot(paths, &keyed_state)?;
    if txn.success.is_empty() {
        return Ok(current_snapshot);
    }
    let mut commit_state = keyed_state;
    host.commit_cluster_kv_txn_with_quorum(paths, &mut commit_state, txn)
}

pub async fn sync_cluster_state_to_kv_async_with_host(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    host: &mut impl ClusterMemberMutationAsyncHost,
) -> Result<KvSnapshot, NetworkError> {
    let mut keyed_state = state.clone();
    host.prepare_cluster_state_for_kv_sync_async(paths, &mut keyed_state)
        .await?;
    cleanup_cluster_iroh_endpoints(&mut keyed_state);
    let paths_for_txn = paths.clone();
    let keyed_state_for_txn = keyed_state.clone();
    let (current_snapshot, txn) = runtime
        .spawn_blocking(move || {
            cluster_state_sync_txn_with_current_snapshot(&paths_for_txn, &keyed_state_for_txn)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster state sync txn".to_string(),
            detail: format!("cluster state sync txn task failed: {source}"),
        })??;
    if txn.success.is_empty() {
        return Ok(current_snapshot);
    }
    let mut commit_state = keyed_state;
    host.commit_cluster_kv_txn_with_quorum_async(paths, &mut commit_state, txn)
        .await
}

pub fn cluster_state_sync_txn_with_current_snapshot(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<(KvSnapshot, KvTxn), NetworkError> {
    let snapshot = read_cluster_kv_current_snapshot(paths)?;
    let store = InProcessKvStore::restore(snapshot.clone());
    cluster_state_sync_txn_from_store(&store, state, true).map(|txn| (snapshot, txn))
}

pub fn update_cluster_node_role_with_host(
    paths: &ClusterPaths,
    mut state: ClusterBootstrapState,
    node_id: NodeId,
    role: ClusterNodeRole,
    now: OffsetDateTime,
    host: &mut impl ClusterMemberMutationHost,
) -> Result<ClusterNodeRoleUpdate, NetworkError> {
    let store = host.current_cluster_kv_store(paths, &state)?;
    let member_key = cluster_member_key(&node_id);
    let Some(member_entry) = store.get(&member_key).cloned() else {
        return Err(NetworkError::InvalidClusterCommand {
            detail: format!("cluster member `{node_id}` was not found"),
        });
    };
    let mut member: ClusterMemberRecord = member_entry.decode()?;
    let previous_role = member.node_role.clone();
    let changed = previous_role != role;
    if changed {
        apply_cluster_node_role(&mut member, role.clone(), now);
        host.commit_cluster_kv_txn_with_quorum(
            paths,
            &mut state,
            cluster_member_update_txn_from_store(&store, member_entry, &member)?,
        )?;
    }
    update_local_cluster_node_role_if_present(paths, &node_id, role.clone(), now)?;
    Ok(ClusterNodeRoleUpdate {
        node_id,
        previous_role,
        role,
        changed,
    })
}

fn update_local_cluster_node_role_if_present(
    paths: &ClusterPaths,
    node_id: &NodeId,
    role: ClusterNodeRole,
    now: OffsetDateTime,
) -> Result<(), NetworkError> {
    let mut state = read_cluster_state_without_kv(paths)?;
    let Some(member) = state
        .members
        .iter_mut()
        .find(|member| member.node_id == *node_id)
    else {
        return Ok(());
    };
    if member.node_role == role {
        return Ok(());
    }
    apply_cluster_node_role(member, role, now);
    state.updated_at = now;
    write_cluster_state(paths, &state)?;
    Ok(())
}

pub fn update_cluster_raft_role_with_host(
    paths: &ClusterPaths,
    mut state: ClusterBootstrapState,
    node_id: NodeId,
    role: ClusterMemberRole,
    now: OffsetDateTime,
    host: &mut impl ClusterMemberMutationHost,
) -> Result<ClusterRaftRoleUpdate, NetworkError> {
    let store = host.current_cluster_kv_store(paths, &state)?;
    let member_key = cluster_member_key(&node_id);
    let Some(member_entry) = store.get(&member_key).cloned() else {
        return Err(NetworkError::InvalidClusterCommand {
            detail: format!("cluster member `{node_id}` was not found"),
        });
    };
    let new_state = cluster_raft_state_for_role(&role);
    let mut member: ClusterMemberRecord = member_entry.decode()?;
    let previous_role = member.role.clone();
    let previous_state = member.state.clone();
    let config_role = store
        .get(&sinomo_kv::cluster_kv_raft_config_key())
        .and_then(|entry| entry.decode::<ClusterKvRaftConfigRecord>().ok())
        .and_then(|config| sinomo_kv::cluster_kv_raft_config_member_role(&config, &node_id));
    let changed =
        previous_role != role || previous_state != new_state || config_role.as_ref() != Some(&role);
    if changed {
        apply_cluster_raft_role(&mut member, role.clone(), now);
        host.commit_cluster_kv_txn_with_quorum(
            paths,
            &mut state,
            cluster_member_raft_update_txn_from_store(&store, member_entry, &member, now)?,
        )?;
    }
    Ok(ClusterRaftRoleUpdate {
        node_id,
        previous_role,
        previous_state,
        role,
        state: new_state,
        changed,
    })
}

pub fn remove_cluster_members_with_host(
    paths: &ClusterPaths,
    mut state: ClusterBootstrapState,
    node_ids: Vec<NodeId>,
    host: &mut impl ClusterMemberMutationHost,
) -> Result<ClusterMemberRemoval, NetworkError> {
    let store = host.current_cluster_kv_store(paths, &state)?;
    let requested = node_ids.into_iter().collect::<BTreeSet<_>>();
    if requested.is_empty() {
        return Err(NetworkError::InvalidClusterCommand {
            detail: "no cluster members were specified".to_string(),
        });
    }
    let local_shadow_node_id = shadow_daemon_node_id(&state.local_node_id);
    if requested.contains(&state.local_node_id) || requested.contains(&local_shadow_node_id) {
        return Err(NetworkError::InvalidClusterCommand {
            detail: "use `sinomo cluster leave` to remove the local daemon members".to_string(),
        });
    }

    let mut removed = Vec::new();
    for node_id in &requested {
        let member_key = cluster_member_key(node_id);
        let Some(member_entry) = store.get(&member_key).cloned() else {
            return Err(NetworkError::InvalidClusterCommand {
                detail: format!("cluster member `{node_id}` was not found"),
            });
        };
        let member: ClusterMemberRecord = member_entry.decode()?;
        if sinomo_kv::cluster_kv_store_member_is_raft_voter(&store, &member.node_id)? {
            return Err(NetworkError::ClusterReadOnly {
                detail: format!(
                    "cluster member `{}` is a voter; change its raft role before removal",
                    member.node_id
                ),
            });
        }
        removed.push(member);
    }

    for txn in cluster_member_removal_txns_from_store(&store, &removed)? {
        host.commit_cluster_kv_txn_with_quorum(paths, &mut state, txn)?;
    }
    Ok(ClusterMemberRemoval { removed })
}

pub fn cleanup_legacy_shadow_daemon_members_with_host(
    paths: &ClusterPaths,
    mut state: ClusterBootstrapState,
    host: &mut impl ClusterMemberMutationHost,
) -> Result<ClusterMemberRemoval, NetworkError> {
    let store = host.current_cluster_kv_store(paths, &state)?;
    let snapshot = store.snapshot();
    let policies = read_cluster_capability_policies_from_snapshot(&snapshot)?;
    let mut current_member_ids = BTreeSet::new();
    let mut removed = Vec::new();
    for entry in store.prefix_entries("/cluster/members/") {
        let mut member: ClusterMemberRecord = entry.decode()?;
        current_member_ids.insert(member.node_id.clone());
        if let Some(policy) = policies.get(&member.node_id) {
            member.capabilities = policy.capabilities.clone();
            member.allowed_capabilities = policy.allowed_capabilities.clone();
        }
        if !cluster_member_is_legacy_shadow_daemon(&member) {
            continue;
        }
        if sinomo_kv::cluster_kv_store_member_is_raft_voter(&store, &member.node_id)? {
            return Err(NetworkError::ClusterReadOnly {
                detail: format!(
                    "legacy shadow member `{}` is a voter; change its raft role before cleanup",
                    member.node_id
                ),
            });
        }
        removed.push(member);
    }
    removed.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    let orphan_artifact_node_ids =
        cluster_legacy_shadow_orphan_artifact_node_ids_from_store(&store, &current_member_ids)?;

    for txn in cluster_member_removal_txns_from_store(&store, &removed)? {
        host.commit_cluster_kv_txn_with_quorum(paths, &mut state, txn)?;
    }
    for txn in cluster_legacy_shadow_orphan_artifact_cleanup_txns_from_store(
        &store,
        &orphan_artifact_node_ids,
    )? {
        host.commit_cluster_kv_txn_with_quorum(paths, &mut state, txn)?;
    }
    Ok(ClusterMemberRemoval { removed })
}

#[cfg(test)]
pub fn sync_cluster_certificate_to_kv(
    paths: &ClusterPaths,
    certificate: &ClusterCertificate,
) -> Result<KvSnapshot, NetworkError> {
    let state = read_cluster_state_without_kv(paths)?;
    put_cluster_kv_value(
        paths,
        &state.local_node_id,
        cluster_certificate_key(certificate),
        certificate,
    )
}

#[cfg(test)]
fn put_cluster_kv_value<T: Serialize + ?Sized>(
    paths: &ClusterPaths,
    node_id: &NodeId,
    key: String,
    value: &T,
) -> Result<KvSnapshot, NetworkError> {
    let op = KvOp::put_serde(key, value, None)?;
    if cluster_kv_put_value_is_noop(paths, &op)? {
        return read_cluster_kv_snapshot(paths);
    }
    append_cluster_raft_command(paths, node_id, KvCommand::Op(op))
}

#[cfg(test)]
fn cluster_kv_put_value_is_noop(paths: &ClusterPaths, op: &KvOp) -> Result<bool, NetworkError> {
    let KvOp::Put {
        key,
        value,
        lease_id,
    } = op
    else {
        return Ok(false);
    };
    let Some(snapshot) = read_cluster_kv_cached_snapshot_if_present(paths)? else {
        return Ok(false);
    };
    let Some(entry) = snapshot.get(key) else {
        return Ok(false);
    };
    Ok(entry.value.as_slice() == value.as_slice() && entry.lease_id == *lease_id)
}
