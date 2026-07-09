#[allow(unused_imports)]
use crate::kv_local_store::kv_err;
use crate::kv_local_store::{
    locked_kv_state_authority_id, locked_restore_cluster_raft_from_snapshot_if_newer,
    read_cluster_kv_snapshot, write_imported_cluster_kv_snapshot_if_changed,
};
use sinomo_base::ClusterBootstrapState;
use sinomo_kv::{InProcessKvStore, KvSnapshot};
use sinomo_network::ClusterPaths;
use sinomo_network::NetworkError;
use sinomo_network::{
    ClusterEndpointInternalSelectionPurpose, ClusterEndpointSelector,
    cluster_node_blocks_leader_preference,
};
use sinomo_network::{ObjectRpcRequest, ObjectRpcResponse};
use sinomo_node::NodeId;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterKvSnapshotRefreshOptions {
    pub without_local_snapshot_timeout: Duration,
    pub with_local_snapshot_timeout: Duration,
    pub tcp_command_timeout: Duration,
    pub iroh_command_timeout: Duration,
    pub tcp_foreground_attempt_timeout: Duration,
    pub public_foreground_attempt_timeout: Duration,
    pub iroh_foreground_attempt_timeout: Duration,
}

impl ClusterKvSnapshotRefreshOptions {
    fn refresh_budget(&self, has_local_snapshot: bool) -> Duration {
        if has_local_snapshot {
            self.with_local_snapshot_timeout
        } else {
            self.without_local_snapshot_timeout
        }
    }

    pub(crate) fn routed_node_command_timeout(&self, remaining: Duration) -> Duration {
        remaining.min(self.tcp_command_timeout.max(self.iroh_command_timeout))
    }

    pub(crate) fn routed_node_attempt_timeout(&self, remaining: Duration) -> Duration {
        let attempt_cap = self
            .tcp_foreground_attempt_timeout
            .max(self.public_foreground_attempt_timeout)
            .max(self.iroh_foreground_attempt_timeout);
        self.routed_node_command_timeout(remaining.min(attempt_cap))
    }
}

pub trait ClusterKvSnapshotRefreshHost {
    fn known_kv_state_authority_id(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<Option<NodeId>, NetworkError>;

    fn restore_cluster_kv_from_snapshot_if_newer(
        &mut self,
        paths: &ClusterPaths,
        local_node_id: &NodeId,
        snapshot: KvSnapshot,
    ) -> Result<bool, NetworkError>;

    fn call_node_object_rpc(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, NetworkError>;
}

struct LocalClusterKvSnapshotRefreshHost<F> {
    fetch_cluster_kv_snapshot: F,
}

impl<F> ClusterKvSnapshotRefreshHost for LocalClusterKvSnapshotRefreshHost<F>
where
    F: FnMut(&NodeId, Duration) -> Result<KvSnapshot, NetworkError>,
{
    fn known_kv_state_authority_id(
        &mut self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<Option<NodeId>, NetworkError> {
        locked_kv_state_authority_id(paths, state)
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
        _state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, NetworkError> {
        if request.route != sinomo_kv::CLUSTER_KV_SNAPSHOT_ROUTE {
            return Err(NetworkError::Io {
                operation: "cluster KV snapshot refresh".to_string(),
                detail: format!("unsupported local snapshot RPC route {}", request.route),
            });
        }
        let snapshot = (self.fetch_cluster_kv_snapshot)(target_node_id, timeout)?;
        let payload =
            sinomo_kv::cluster_kv_snapshot_response_rpc_payload(&snapshot).map_err(kv_err)?;
        Ok(ObjectRpcResponse::from_encoded_body(
            payload.route,
            payload.body,
        ))
    }
}

pub struct ClusterKvSnapshotRefreshOutcome {
    pub store: InProcessKvStore,
    pub fetched_remote: bool,
    pub restored_newer_snapshot: bool,
}

pub fn refresh_current_cluster_kv_store_with_host(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterKvSnapshotRefreshOptions,
    host: &mut impl ClusterKvSnapshotRefreshHost,
) -> Result<InProcessKvStore, NetworkError> {
    refresh_current_cluster_kv_store_with_outcome(paths, state, options, host)
        .map(|outcome| outcome.store)
}

pub fn refresh_current_cluster_kv_store_with_outcome(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterKvSnapshotRefreshOptions,
    host: &mut impl ClusterKvSnapshotRefreshHost,
) -> Result<ClusterKvSnapshotRefreshOutcome, NetworkError> {
    let mut snapshots = Vec::new();
    if let Ok(snapshot) = read_cluster_kv_snapshot(paths) {
        snapshots.push(snapshot);
    }
    let mut targets = ClusterEndpointSelector::new(paths, state).select_internal_unique_node_ids(
        ClusterEndpointInternalSelectionPurpose::LiveClusterStateAuthorityCommand,
    );
    targets.retain(|node_id| node_id != &state.local_node_id);
    if let Ok(Some(leader_id)) = host.known_kv_state_authority_id(paths, state)
        && !cluster_node_blocks_leader_preference(paths, state, &leader_id)
        && let Some(position) = targets.iter().position(|node_id| node_id == &leader_id)
    {
        let leader = targets.remove(position);
        targets.insert(0, leader);
    }

    let deadline = Instant::now() + options.refresh_budget(!snapshots.is_empty());
    let mut fetched_remote = false;
    for target_node_id in &targets {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            break;
        }
        if let Ok(snapshot) = fetch_cluster_kv_snapshot_from_state_authority_node(
            state,
            target_node_id,
            options.routed_node_attempt_timeout(remaining),
            host,
        ) {
            snapshots.push(snapshot);
            fetched_remote = true;
        }
    }
    let snapshot = snapshots
        .into_iter()
        .max_by_key(|snapshot| snapshot.revision)
        .ok_or_else(|| NetworkError::ClusterReadOnly {
            detail: "no local or reachable cluster KV snapshot is available".to_string(),
        })?;
    write_imported_cluster_kv_snapshot_if_changed(paths, &snapshot)?;
    let restored_newer_snapshot = host.restore_cluster_kv_from_snapshot_if_newer(
        paths,
        &state.local_node_id,
        snapshot.clone(),
    )?;
    let store = InProcessKvStore::restore(snapshot).map_err(kv_err)?;
    Ok(ClusterKvSnapshotRefreshOutcome {
        store,
        fetched_remote,
        restored_newer_snapshot,
    })
}

fn fetch_cluster_kv_snapshot_from_state_authority_node(
    state: &ClusterBootstrapState,
    authority_node_id: &NodeId,
    timeout: Duration,
    host: &mut impl ClusterKvSnapshotRefreshHost,
) -> Result<KvSnapshot, NetworkError> {
    let payload = sinomo_kv::cluster_kv_snapshot_rpc_payload().map_err(kv_err)?;
    let request =
        ObjectRpcRequest::from_encoded_body(payload.route, payload.category, payload.body);
    let response = host.call_node_object_rpc(state, authority_node_id, &request, timeout)?;
    response.error_detail()?;
    sinomo_kv::decode_cluster_kv_snapshot_rpc_body(&response.body).map_err(kv_err)
}

pub fn refresh_current_cluster_kv_store_with_local_storage_and_transport<F>(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    options: ClusterKvSnapshotRefreshOptions,
    fetch_cluster_kv_snapshot: F,
) -> Result<InProcessKvStore, NetworkError>
where
    F: FnMut(&NodeId, Duration) -> Result<KvSnapshot, NetworkError>,
{
    let mut host = LocalClusterKvSnapshotRefreshHost {
        fetch_cluster_kv_snapshot,
    };
    refresh_current_cluster_kv_store_with_host(paths, state, options, &mut host)
}
