use crate::ClusterPaths;
use crate::endpoint_selector::{
    ClusterEndpointInternalSelectionPurpose, ClusterEndpointSelector,
    cluster_node_blocks_leader_preference,
};
use crate::error::NetworkError;
use crate::kv_local_store::{
    locked_kv_state_authority_id, locked_restore_cluster_raft_from_snapshot_if_newer,
    read_cluster_kv_snapshot, write_imported_cluster_kv_snapshot_if_changed,
};
use crate::rpc::{ObjectRpcRequest, ObjectRpcResponse};
use sinomo_core::{
    ClusterBootstrapState, ClusterEndpointPrivacy, ClusterEndpointRecord, ClusterTransportKind,
};
use sinomo_kv::{InProcessKvStore, KvSnapshot};
use sinomo_node::NodeId;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterKvSnapshotRefreshOptions {
    pub(crate) without_local_snapshot_timeout: Duration,
    pub(crate) with_local_snapshot_timeout: Duration,
    pub(crate) tcp_command_timeout: Duration,
    pub(crate) iroh_command_timeout: Duration,
    pub(crate) tcp_foreground_attempt_timeout: Duration,
    pub(crate) public_foreground_attempt_timeout: Duration,
    pub(crate) iroh_foreground_attempt_timeout: Duration,
}

impl ClusterKvSnapshotRefreshOptions {
    fn refresh_budget(&self, has_local_snapshot: bool) -> Duration {
        if has_local_snapshot {
            self.with_local_snapshot_timeout
        } else {
            self.without_local_snapshot_timeout
        }
    }

    pub(crate) fn command_timeout(
        &self,
        endpoint: &ClusterEndpointRecord,
        remaining: Duration,
    ) -> Duration {
        match endpoint.transport {
            ClusterTransportKind::Iroh => remaining.min(self.iroh_command_timeout),
            _ => remaining.min(self.tcp_command_timeout),
        }
    }

    pub(crate) fn fair_command_timeout(
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

pub trait ClusterKvSnapshotRefreshHost {
    fn known_kv_state_authority_id(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<Option<NodeId>, NetworkError>;

    fn fetch_cluster_kv_snapshot(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> Result<KvSnapshot, NetworkError>;

    fn restore_cluster_kv_from_snapshot_if_newer(
        &mut self,
        paths: &ClusterPaths,
        local_node_id: &NodeId,
        snapshot: KvSnapshot,
    ) -> Result<bool, NetworkError>;

    fn call_node_object_rpc(
        &mut self,
        _state: &ClusterBootstrapState,
        _target_node_id: &NodeId,
        _request: &ObjectRpcRequest,
        _timeout: Duration,
    ) -> Result<ObjectRpcResponse, NetworkError> {
        Err(NetworkError::ClusterReadOnly {
            detail: "cluster KV snapshot node object RPC is not supported by this host".to_string(),
        })
    }
}

struct LocalClusterKvSnapshotRefreshHost<F> {
    fetch_cluster_kv_snapshot: F,
}

impl<F> ClusterKvSnapshotRefreshHost for LocalClusterKvSnapshotRefreshHost<F>
where
    F: FnMut(&ClusterEndpointRecord, Duration) -> Result<KvSnapshot, NetworkError>,
{
    fn known_kv_state_authority_id(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<Option<NodeId>, NetworkError> {
        locked_kv_state_authority_id(paths, state)
    }

    fn fetch_cluster_kv_snapshot(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> Result<KvSnapshot, NetworkError> {
        (self.fetch_cluster_kv_snapshot)(endpoint, timeout)
    }

    fn restore_cluster_kv_from_snapshot_if_newer(
        &mut self,
        paths: &ClusterPaths,
        local_node_id: &NodeId,
        snapshot: KvSnapshot,
    ) -> Result<bool, NetworkError> {
        locked_restore_cluster_raft_from_snapshot_if_newer(paths, local_node_id, snapshot)
    }
}

pub fn refresh_current_cluster_kv_store_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterKvSnapshotRefreshOptions,
    host: &mut impl ClusterKvSnapshotRefreshHost,
) -> Result<InProcessKvStore, NetworkError> {
    let mut snapshots = Vec::new();
    if let Ok(snapshot) = read_cluster_kv_snapshot(paths) {
        snapshots.push(snapshot);
    }
    let mut targets = ClusterEndpointSelector::new(paths, state)
        .select_internal(ClusterEndpointInternalSelectionPurpose::LiveClusterStateAuthorityCommand);
    if let Ok(Some(leader_id)) = host.known_kv_state_authority_id(paths, state)
        && !cluster_node_blocks_leader_preference(paths, state, &leader_id)
        && let Some(position) = targets
            .iter()
            .position(|endpoint| endpoint.node_id == leader_id)
    {
        let leader = targets.remove(position);
        targets.insert(0, leader);
    }

    let deadline = Instant::now() + options.refresh_budget(!snapshots.is_empty());
    let mut fetched_remote = false;
    for endpoint in &targets {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            break;
        }
        if let Ok(snapshot) = host
            .fetch_cluster_kv_snapshot(endpoint, options.fair_command_timeout(endpoint, remaining))
        {
            snapshots.push(snapshot);
            fetched_remote = true;
        }
    }
    if !fetched_remote {
        let unique_node_targets = ClusterEndpointSelector::unique_node_targets(targets.clone());
        'node_targets: for target in &unique_node_targets {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .unwrap_or_default();
            if remaining.is_zero() {
                break 'node_targets;
            }
            if let Ok(snapshot) = fetch_cluster_kv_snapshot_from_state_authority_node(
                state,
                &target.node_id,
                remaining,
                host,
            ) {
                snapshots.push(snapshot);
                break 'node_targets;
            }
        }
    }
    let snapshot = snapshots
        .into_iter()
        .max_by_key(|snapshot| snapshot.revision)
        .ok_or_else(|| NetworkError::ClusterReadOnly {
            detail: "no local or reachable cluster KV snapshot is available".to_string(),
        })?;
    write_imported_cluster_kv_snapshot_if_changed(paths, &snapshot)?;
    host.restore_cluster_kv_from_snapshot_if_newer(paths, &state.local_node_id, snapshot.clone())?;
    Ok(InProcessKvStore::restore(snapshot))
}

fn fetch_cluster_kv_snapshot_from_state_authority_node(
    state: &ClusterBootstrapState,
    authority_node_id: &NodeId,
    timeout: Duration,
    host: &mut impl ClusterKvSnapshotRefreshHost,
) -> Result<KvSnapshot, NetworkError> {
    let payload = sinomo_kv::cluster_kv_snapshot_rpc_payload()?;
    let request =
        ObjectRpcRequest::from_encoded_body(payload.route, payload.category, payload.body);
    let response = host.call_node_object_rpc(state, authority_node_id, &request, timeout)?;
    response.error_detail()?;
    sinomo_kv::decode_cluster_kv_snapshot_rpc_body(&response.body).map_err(Into::into)
}

pub fn refresh_current_cluster_kv_store_with_local_storage_and_transport<F>(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterKvSnapshotRefreshOptions,
    fetch_cluster_kv_snapshot: F,
) -> Result<InProcessKvStore, NetworkError>
where
    F: FnMut(&ClusterEndpointRecord, Duration) -> Result<KvSnapshot, NetworkError>,
{
    let mut host = LocalClusterKvSnapshotRefreshHost {
        fetch_cluster_kv_snapshot,
    };
    refresh_current_cluster_kv_store_with_host(paths, state, options, &mut host)
}
