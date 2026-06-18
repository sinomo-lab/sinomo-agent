use crate::ClusterPaths;
use crate::connectivity_publish::cluster_network_record_should_refresh;
use crate::error::NetworkError;
use crate::keyspace::{
    cluster_network_info_key, cluster_node_status_key_for_record,
    cluster_node_status_snapshot_record,
};
use crate::network_info::{ClusterNetworkInfoRecord, cluster_network_info_stable_eq};
use crate::prelude::*;
use crate::storage::{
    ClusterMembershipRpcFuture, append_cluster_raft_command,
    read_cluster_kv_current_or_cached_snapshot_arc_if_present, read_cluster_kv_snapshot,
    read_cluster_state, read_cluster_state_without_kv,
};

pub trait ClusterNetworkInfoPublishHost {
    type Error: From<NetworkError>;

    fn published_network_info_matches(
        &mut self,
        _paths: &ClusterPaths,
        _info: &ClusterNetworkInfoRecord,
    ) -> bool {
        false
    }

    fn remember_published_network_info(
        &mut self,
        _paths: &ClusterPaths,
        _info: &ClusterNetworkInfoRecord,
    ) {
    }

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, Self::Error>;
}

pub trait ClusterNetworkInfoPublishAsyncHost {
    type Error: From<NetworkError>;

    fn published_network_info_matches_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _info: &'a ClusterNetworkInfoRecord,
    ) -> ClusterMembershipRpcFuture<'a, bool> {
        Box::pin(async { Ok(false) })
    }

    fn remember_published_network_info_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _info: &'a ClusterNetworkInfoRecord,
    ) -> ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn commit_cluster_kv_txn_with_quorum_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> ClusterMembershipRpcFuture<'a, KvSnapshot>;
}

pub trait ClusterNodeStatusPublishHost {
    type Error: From<NetworkError>;

    fn published_node_status_matches(
        &mut self,
        _paths: &ClusterPaths,
        _status: &ClusterNodeStatusRecord,
    ) -> bool {
        false
    }

    fn remember_published_node_status(
        &mut self,
        _paths: &ClusterPaths,
        _status: &ClusterNodeStatusRecord,
    ) {
    }

    fn commit_cluster_kv_txn_with_quorum(
        &mut self,
        paths: &ClusterPaths,
        state: &mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> Result<KvSnapshot, Self::Error>;
}

pub trait ClusterNodeStatusPublishAsyncHost {
    type Error: From<NetworkError>;

    fn published_node_status_matches_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _status: &'a ClusterNodeStatusRecord,
    ) -> ClusterMembershipRpcFuture<'a, bool> {
        Box::pin(async { Ok(false) })
    }

    fn remember_published_node_status_async<'a>(
        &'a mut self,
        _paths: &'a ClusterPaths,
        _status: &'a ClusterNodeStatusRecord,
    ) -> ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn commit_cluster_kv_txn_with_quorum_async<'a>(
        &'a mut self,
        paths: &'a ClusterPaths,
        state: &'a mut ClusterBootstrapState,
        txn: KvTxn,
    ) -> ClusterMembershipRpcFuture<'a, KvSnapshot>;
}

pub fn sync_cluster_node_status_to_kv(
    paths: &ClusterPaths,
    status: ClusterNodeStatusRecord,
) -> Result<KvSnapshot, NetworkError> {
    if stored_cluster_node_status_matches(paths, &status)? {
        return read_cluster_kv_snapshot(paths);
    }
    let state = read_cluster_state_without_kv(paths)?;
    let key = cluster_node_status_key_for_record(&status);
    append_cluster_raft_command(
        paths,
        &state.local_node_id,
        KvCommand::Txn(KvTxn {
            compares: Vec::new(),
            success: vec![KvOp::put_serde(key, &status, None)?],
            failure: Vec::new(),
        }),
    )
}

pub fn publish_cluster_node_status_to_quorum_with_host<H>(
    paths: &ClusterPaths,
    status: ClusterNodeStatusRecord,
    host: &mut H,
) -> Result<Option<KvSnapshot>, H::Error>
where
    H: ClusterNodeStatusPublishHost,
{
    if host.published_node_status_matches(paths, &status) {
        return Ok(None);
    }
    if stored_cluster_node_status_matches(paths, &status)? {
        host.remember_published_node_status(paths, &status);
        return Ok(None);
    }
    let mut state = read_cluster_state(paths)?;
    let key = cluster_node_status_key_for_record(&status);
    let snapshot = host.commit_cluster_kv_txn_with_quorum(
        paths,
        &mut state,
        KvTxn {
            compares: Vec::new(),
            success: vec![KvOp::put_serde(key, &status, None).map_err(NetworkError::from)?],
            failure: Vec::new(),
        },
    )?;
    host.remember_published_node_status(paths, &status);
    Ok(Some(snapshot))
}

pub async fn publish_cluster_node_status_to_quorum_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    status: ClusterNodeStatusRecord,
    host: &mut H,
) -> Result<Option<KvSnapshot>, H::Error>
where
    H: ClusterNodeStatusPublishAsyncHost,
{
    if host
        .published_node_status_matches_async(paths, &status)
        .await?
    {
        return Ok(None);
    }
    let match_paths = paths.clone();
    let match_status = status.clone();
    let stored_matches = runtime
        .spawn_blocking(move || stored_cluster_node_status_matches(&match_paths, &match_status))
        .await
        .map_err(|source| {
            H::Error::from(NetworkError::Io {
                operation: "read stored cluster node status".to_string(),
                detail: source.to_string(),
            })
        })?
        .map_err(H::Error::from)?;
    if stored_matches {
        host.remember_published_node_status_async(paths, &status)
            .await?;
        return Ok(None);
    }

    let state_paths = paths.clone();
    let mut state = runtime
        .spawn_blocking(move || read_cluster_state(&state_paths))
        .await
        .map_err(|source| {
            H::Error::from(NetworkError::Io {
                operation: "read cluster state".to_string(),
                detail: format!("cluster state read task failed: {source}"),
            })
        })?
        .map_err(H::Error::from)?;
    let key = cluster_node_status_key_for_record(&status);
    let snapshot = host
        .commit_cluster_kv_txn_with_quorum_async(
            paths,
            &mut state,
            KvTxn {
                compares: Vec::new(),
                success: vec![
                    KvOp::put_serde(key, &status, None)
                        .map_err(NetworkError::from)
                        .map_err(H::Error::from)?,
                ],
                failure: Vec::new(),
            },
        )
        .await?;
    host.remember_published_node_status_async(paths, &status)
        .await?;
    Ok(Some(snapshot))
}

pub fn sync_cluster_network_info_to_kv(
    paths: &ClusterPaths,
    info: ClusterNetworkInfoRecord,
) -> Result<KvSnapshot, NetworkError> {
    if stored_cluster_network_info_matches(paths, &info)? {
        return read_cluster_kv_snapshot(paths);
    }
    let state = read_cluster_state_without_kv(paths)?;
    let key = cluster_network_info_key(&info.node_id);
    append_cluster_raft_command(
        paths,
        &state.local_node_id,
        KvCommand::Op(KvOp::put_serde(key, &info, None)?),
    )
}

pub fn publish_cluster_network_info_to_quorum_with_host<H>(
    paths: &ClusterPaths,
    info: ClusterNetworkInfoRecord,
    host: &mut H,
) -> Result<Option<KvSnapshot>, H::Error>
where
    H: ClusterNetworkInfoPublishHost,
{
    if host.published_network_info_matches(paths, &info) {
        return Ok(None);
    }
    if stored_cluster_network_info_matches(paths, &info)? {
        host.remember_published_network_info(paths, &info);
        return Ok(None);
    }
    let mut state = read_cluster_state(paths)?;
    let node_id = info.node_id.clone();
    let snapshot = host.commit_cluster_kv_txn_with_quorum(
        paths,
        &mut state,
        KvTxn {
            compares: Vec::new(),
            success: vec![
                KvOp::put_serde(cluster_network_info_key(&node_id), &info, None)
                    .map_err(NetworkError::from)?,
            ],
            failure: Vec::new(),
        },
    )?;
    host.remember_published_network_info(paths, &info);
    Ok(Some(snapshot))
}

pub async fn publish_cluster_network_info_to_quorum_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    info: ClusterNetworkInfoRecord,
    host: &mut H,
) -> Result<Option<KvSnapshot>, H::Error>
where
    H: ClusterNetworkInfoPublishAsyncHost,
{
    if host
        .published_network_info_matches_async(paths, &info)
        .await?
    {
        return Ok(None);
    }
    let match_paths = paths.clone();
    let match_info = info.clone();
    let stored_matches = runtime
        .spawn_blocking(move || stored_cluster_network_info_matches(&match_paths, &match_info))
        .await
        .map_err(|source| {
            H::Error::from(NetworkError::Io {
                operation: "read stored cluster network info".to_string(),
                detail: source.to_string(),
            })
        })?
        .map_err(H::Error::from)?;
    if stored_matches {
        host.remember_published_network_info_async(paths, &info)
            .await?;
        return Ok(None);
    }

    let state_paths = paths.clone();
    let mut state = runtime
        .spawn_blocking(move || read_cluster_state(&state_paths))
        .await
        .map_err(|source| {
            H::Error::from(NetworkError::Io {
                operation: "read cluster state".to_string(),
                detail: format!("cluster state read task failed: {source}"),
            })
        })?
        .map_err(H::Error::from)?;
    let node_id = info.node_id.clone();
    let snapshot = host
        .commit_cluster_kv_txn_with_quorum_async(
            paths,
            &mut state,
            KvTxn {
                compares: Vec::new(),
                success: vec![
                    KvOp::put_serde(cluster_network_info_key(&node_id), &info, None)
                        .map_err(NetworkError::from)
                        .map_err(H::Error::from)?,
                ],
                failure: Vec::new(),
            },
        )
        .await?;
    host.remember_published_network_info_async(paths, &info)
        .await?;
    Ok(Some(snapshot))
}

pub fn stored_cluster_node_status_matches(
    paths: &ClusterPaths,
    status: &ClusterNodeStatusRecord,
) -> Result<bool, NetworkError> {
    let Some(snapshot) = read_cluster_kv_current_or_cached_snapshot_arc_if_present(paths)? else {
        return Ok(false);
    };
    let Some(existing) = cluster_node_status_snapshot_record(&snapshot, status) else {
        return Ok(false);
    };
    // Match on stable content only: node status is not periodically refreshed
    // (no observed_at heartbeat). A live node's status is republished when its
    // content changes; crash detection is handled by active connectivity probes.
    Ok(cluster_node_status_stable_eq(&existing, status))
}

pub fn cluster_node_status_stable_eq(
    left: &ClusterNodeStatusRecord,
    right: &ClusterNodeStatusRecord,
) -> bool {
    left.cluster_id == right.cluster_id
        && left.node_id == right.node_id
        && left.process == right.process
        && left.availability == right.availability
        && left.daemon_version == right.daemon_version
        && left.commit == right.commit
        && left.target_os == right.target_os
        && left.target_arch == right.target_arch
        && left.address == right.address
        && left.bind_address == right.bind_address
        && left.executable == right.executable
        && left.rollout == right.rollout
        && left.daemon_started_at == right.daemon_started_at
}

pub fn stored_cluster_network_info_matches(
    paths: &ClusterPaths,
    info: &ClusterNetworkInfoRecord,
) -> Result<bool, NetworkError> {
    let Some(snapshot) = read_cluster_kv_current_or_cached_snapshot_arc_if_present(paths)? else {
        return Ok(false);
    };
    let Some(entry) = snapshot.get(&cluster_network_info_key(&info.node_id)) else {
        return Ok(false);
    };
    let Ok(existing) = entry.decode::<ClusterNetworkInfoRecord>() else {
        return Ok(false);
    };
    let now = OffsetDateTime::now_utc();
    Ok(cluster_network_info_stable_eq(&existing, info)
        && !cluster_network_record_should_refresh(existing.expires_at, now))
}
