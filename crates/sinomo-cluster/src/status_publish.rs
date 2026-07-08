use crate::keyspace::{
    cluster_network_info_key, cluster_node_status_key_for_record,
    cluster_node_status_snapshot_record,
};
use crate::kv_local_store::kv_err;
use crate::kv_local_store::read_cluster_kv_current_or_cached_snapshot_arc_if_present;
#[cfg(any(test, feature = "test-support"))]
use crate::kv_local_store::{append_cluster_raft_command, read_cluster_kv_snapshot};
use crate::local_state_store::read_cluster_state;
#[cfg(any(test, feature = "test-support"))]
use crate::local_state_store::read_cluster_state_without_kv;
use crate::prelude::*;
#[allow(unused_imports)]
use sinomo_kv::{KvCommand, KvOp, KvSnapshot, KvTxn};
use sinomo_network::ClusterMembershipRpcFuture;
use sinomo_network::ClusterPaths;
use sinomo_network::NetworkError;
use sinomo_network::{ClusterNetworkInfoRecord, cluster_network_info_publish_record_matches};
use sinomo_node::cluster_node_status_stable_eq;

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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ClusterNodePresencePublishMatches {
    pub network_info_matches: bool,
    pub node_status_matches: bool,
}

impl ClusterNodePresencePublishMatches {
    pub fn all_match(self) -> bool {
        self.network_info_matches && self.node_status_matches
    }
}

#[cfg(any(test, feature = "test-support"))]
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
            success: vec![KvOp::put_serde(key, &status, None).map_err(kv_err)?],
            failure: Vec::new(),
        }),
    )
}

pub fn cluster_node_status_publish_matches_with_host<H>(
    paths: &ClusterPaths,
    status: &ClusterNodeStatusRecord,
    host: &mut H,
) -> Result<bool, H::Error>
where
    H: ClusterNodeStatusPublishHost,
{
    if stored_cluster_node_status_matches(paths, status)? {
        host.remember_published_node_status(paths, status);
        return Ok(true);
    }
    Ok(false)
}

pub fn publish_cluster_node_status_to_quorum_with_host<H>(
    paths: &ClusterPaths,
    status: ClusterNodeStatusRecord,
    host: &mut H,
) -> Result<Option<KvSnapshot>, H::Error>
where
    H: ClusterNodeStatusPublishHost,
{
    if cluster_node_status_publish_matches_with_host(paths, &status, host)? {
        return Ok(None);
    }
    let mut state = read_cluster_state(paths)?;
    let key = cluster_node_status_key_for_record(&status);
    let snapshot = host.commit_cluster_kv_txn_with_quorum(
        paths,
        &mut state,
        KvTxn {
            compares: Vec::new(),
            success: vec![KvOp::put_serde(key, &status, None).map_err(kv_err)?],
            failure: Vec::new(),
        },
    )?;
    host.remember_published_node_status(paths, &status);
    Ok(Some(snapshot))
}

pub(crate) async fn cluster_node_status_publish_matches_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    status: &ClusterNodeStatusRecord,
    host: &mut H,
) -> Result<bool, H::Error>
where
    H: ClusterNodeStatusPublishAsyncHost,
{
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
        host.remember_published_node_status_async(paths, status)
            .await?;
        return Ok(true);
    }
    Ok(false)
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
    if cluster_node_status_publish_matches_async_with_host(runtime, paths, &status, host).await? {
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
                        .map_err(kv_err)
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

#[cfg(any(test, feature = "test-support"))]
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
        KvCommand::Op(KvOp::put_serde(key, &info, None).map_err(kv_err)?),
    )
}

pub fn cluster_network_info_publish_matches_with_host<H>(
    paths: &ClusterPaths,
    info: &ClusterNetworkInfoRecord,
    host: &mut H,
) -> Result<bool, H::Error>
where
    H: ClusterNetworkInfoPublishHost,
{
    if stored_cluster_network_info_matches(paths, info)? {
        host.remember_published_network_info(paths, info);
        return Ok(true);
    }
    Ok(false)
}

pub fn publish_cluster_network_info_to_quorum_with_host<H>(
    paths: &ClusterPaths,
    info: ClusterNetworkInfoRecord,
    host: &mut H,
) -> Result<Option<KvSnapshot>, H::Error>
where
    H: ClusterNetworkInfoPublishHost,
{
    if cluster_network_info_publish_matches_with_host(paths, &info, host)? {
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
                KvOp::put_serde(cluster_network_info_key(&node_id), &info, None).map_err(kv_err)?,
            ],
            failure: Vec::new(),
        },
    )?;
    host.remember_published_network_info(paths, &info);
    Ok(Some(snapshot))
}

pub(crate) async fn cluster_network_info_publish_matches_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    info: &ClusterNetworkInfoRecord,
    host: &mut H,
) -> Result<bool, H::Error>
where
    H: ClusterNetworkInfoPublishAsyncHost,
{
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
        host.remember_published_network_info_async(paths, info)
            .await?;
        return Ok(true);
    }
    Ok(false)
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
    if cluster_network_info_publish_matches_async_with_host(runtime, paths, &info, host).await? {
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
                        .map_err(kv_err)
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

pub fn cluster_node_presence_publish_matches_with_host<H>(
    paths: &ClusterPaths,
    network_info: &ClusterNetworkInfoRecord,
    node_status: &ClusterNodeStatusRecord,
    host: &mut H,
) -> ClusterNodePresencePublishMatches
where
    H: ClusterNetworkInfoPublishHost + ClusterNodeStatusPublishHost,
{
    ClusterNodePresencePublishMatches {
        network_info_matches: cluster_network_info_publish_matches_with_host(
            paths,
            network_info,
            host,
        )
        .unwrap_or(false),
        node_status_matches: cluster_node_status_publish_matches_with_host(
            paths,
            node_status,
            host,
        )
        .unwrap_or(false),
    }
}

pub async fn cluster_node_presence_publish_matches_async_with_host<H>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    network_info: &ClusterNetworkInfoRecord,
    node_status: &ClusterNodeStatusRecord,
    host: &mut H,
) -> ClusterNodePresencePublishMatches
where
    H: ClusterNetworkInfoPublishAsyncHost + ClusterNodeStatusPublishAsyncHost,
{
    ClusterNodePresencePublishMatches {
        network_info_matches: cluster_network_info_publish_matches_async_with_host(
            runtime,
            paths,
            network_info,
            host,
        )
        .await
        .unwrap_or(false),
        node_status_matches: cluster_node_status_publish_matches_async_with_host(
            runtime,
            paths,
            node_status,
            host,
        )
        .await
        .unwrap_or(false),
    }
}

pub(crate) fn stored_cluster_node_status_matches(
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
    Ok(cluster_node_status_publish_record_matches(
        &existing, status,
    ))
}

pub(crate) fn stored_cluster_network_info_matches(
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
    Ok(cluster_network_info_publish_record_matches(
        &existing, info, now,
    ))
}

pub fn cluster_node_status_publish_record_matches(
    known: &ClusterNodeStatusRecord,
    status: &ClusterNodeStatusRecord,
) -> bool {
    cluster_node_status_stable_eq(known, status)
}
