use crate::ClusterPaths;
use crate::connectivity::ClusterConnectivityRecord;
pub(crate) use crate::connectivity_publish::{
    ClusterConnectivityPublishHost, ClusterNetworkRecordExpiryHost,
};
#[cfg(test)]
pub(crate) use crate::connectivity_publish::{
    MAX_CLUSTER_CONNECTIVITY_SOURCE_RECORDS, cluster_connectivity_full_sync_batch_ops,
    cluster_connectivity_sync_is_noop, cluster_network_expiry_delete_ops,
    expire_cluster_network_record_ops, expire_cluster_network_records,
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
use crate::error::NetworkError;
use crate::invite_publish::ClusterInvitePublishHost;
#[cfg(test)]
pub(crate) use crate::kv_local_store::apply_cluster_raft_append;
use crate::kv_local_store::locked_cluster_raft_known_leader_id;
use crate::kv_local_store::locked_restore_cluster_raft_from_snapshot_if_newer;
#[cfg(test)]
pub(crate) use crate::kv_local_store::write_imported_cluster_kv_snapshot_if_changed;
#[cfg(test)]
pub(crate) use crate::kv_local_store::{
    LocalClusterRaftCommandStorage, append_cluster_raft_command_uncommitted,
    append_cluster_raft_command_with_response, cached_cluster_kv_reader_node_id_for_test,
    cluster_kv_cached_snapshot_reads_for_test, cluster_kv_current_snapshot_reads_for_test,
    cluster_kv_metadata_cache_capacity_for_test, cluster_kv_reader_node_id_cache_contains_for_test,
    cluster_raft_wal_paths, open_cluster_raft_kv_node_async, read_cluster_kv_prefix_entries,
    read_cluster_kv_prefix_entries_limited,
    read_cluster_kv_prefix_entries_limited_by_payload_bytes, read_cluster_kv_snapshot,
    read_cluster_kv_store, record_cluster_kv_reader_node_id_cache_for_test,
    refresh_cluster_kv_reader_node_id_cache_for_test,
    remove_cluster_kv_reader_node_id_cache_entries_under_root_for_test,
    reset_cluster_kv_cached_snapshot_reads_for_test,
    reset_cluster_kv_current_snapshot_reads_for_test, restore_cluster_raft_from_snapshot_if_newer,
};
#[cfg(test)]
pub(crate) use crate::kv_local_store::{
    append_cluster_raft_command, open_cluster_raft_kv_node, read_cluster_kv_current_snapshot,
    write_cluster_kv_snapshot,
};
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
#[cfg(test)]
pub(crate) use crate::local_membership::apply_local_daemon_membership_observation;
#[cfg(test)]
pub(crate) use crate::local_membership::{
    ensure_main_daemon_cluster_member,
    prepare_cluster_join_registration_state_with_local_daemon_members,
};
#[cfg(test)]
pub(crate) use crate::local_state_store::read_cluster_state;
#[cfg(test)]
pub use crate::local_state_store::remove_local_cluster_state;
#[cfg(test)]
pub(crate) use crate::local_state_store::{read_cluster_state_without_kv, write_cluster_state};
use crate::member_mutation::ClusterMemberMutationHost;
#[cfg(test)]
pub(crate) use crate::member_mutation::sync_cluster_certificate_to_kv;
use crate::member_publish::{ClusterLocalMemberPublishHost, ClusterMemberPublishHost};
#[cfg(test)]
pub(crate) use crate::member_store::{
    apply_cluster_raft_role, cluster_member_removal_txns_from_store,
};
#[cfg(test)]
pub(crate) use crate::member_store::{
    cluster_member_put_command, cluster_member_update_ops_from_store,
    cluster_raft_config_from_store,
};
use crate::membership_rpc::{ClusterMemberPublishRequest, ClusterMemberPublishResponse};
use crate::network_info::ClusterNetworkInfoRecord;
use crate::prelude::*;
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
pub(crate) use crate::raft_rpc::{
    handle_cluster_raft_append_request, handle_cluster_raft_vote_request,
};
#[cfg(test)]
pub(crate) use crate::route_graph_read_model::read_cluster_connectivity_graph;
#[cfg(test)]
pub(crate) use crate::route_graph_read_model::read_cluster_connectivity_graph_with_query;
use crate::rpc::{
    CLUSTER_MEMBER_PUBLISH_ROUTE, CLUSTER_MEMBERSHIP_RPC_CATEGORY, ObjectRpcRequest,
    ObjectRpcResponse,
};
use crate::state_authority_reachability::{
    ClusterStateAuthorityReachabilityHost, cluster_has_local_quorum_with_host,
};
use crate::status_publish::{ClusterNetworkInfoPublishHost, ClusterNodeStatusPublishHost};
use std::time::Duration;

impl<T> ClusterStateAuthorityReachabilityHost for ClusterControlPlaneDriver<T>
where
    T: ClusterControlPlaneTransport,
{
    fn probe_cluster_state_authority(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> bool {
        self.transport
            .probe_cluster_state_authority(endpoint, timeout)
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
        locked_cluster_raft_known_leader_id(paths, state)
    }

    fn fetch_cluster_kv_snapshot(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> Result<KvSnapshot, NetworkError> {
        self.transport.fetch_cluster_kv_snapshot(endpoint, timeout)
    }

    fn restore_cluster_kv_from_snapshot_if_newer(
        &mut self,
        paths: &ClusterPaths,
        local_node_id: &NodeId,
        snapshot: KvSnapshot,
    ) -> Result<bool, NetworkError> {
        locked_restore_cluster_raft_from_snapshot_if_newer(paths, local_node_id, snapshot)
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
        let options = self.timeouts.state_authority_reachability_options();
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
