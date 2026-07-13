//! Mounted SIFS adapter over local cluster state and DFS peer transports.
//!
//! This module owns the low-level glue required to open SIFS as a mounted
//! networked DFS service from a local data directory.

use sinomo_core::ClusterBootstrapState;
#[allow(unused_imports)]
use sinomo_core::cluster::ClusterKvPathsExt;
use sinomo_core::kv::{KvCompare, KvEntry, KvError, KvOp, KvRevision, KvTxn, KvTxnResponse};
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::{
    ClusterPaths, NetworkError, ObjectRpcRequest, ObjectRpcResponse, ObjectRpcSessionCall,
    ObjectRpcSessionResult,
};
use sinomo_core::node::NodeId;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::{
    AsyncDfsPeerTransport, DfsControlPlaneKvClient, DfsError, DfsNodeAdvertisement, DfsNodeId,
    DfsNodeTransport, DfsObjectRpcSendOptions, DfsPeerTransport, DfsPeerTransportError,
    DfsPeerTransportFuture, DfsPosixError, NetworkedDfsService, cluster_dfs_peer_node_ids,
    content_services_prefix, fallback_dfs_node_id, object_rpc_request_from_kv_payload,
    open_cluster_dfs_service_with_peer_transports_for_nodes,
};

pub type MountedSifsService = NetworkedDfsService<DfsNodeTransport>;

#[derive(Debug)]
pub struct OpenedMountedSifsService {
    pub service: MountedSifsService,
    pub replica_candidates: Vec<DfsNodeId>,
}

pub fn open_mounted_sifs_service(
    data_dir: Option<PathBuf>,
) -> Result<OpenedMountedSifsService, DfsError> {
    open_mounted_sifs_service_with_cancellation(data_dir, None)
}

/// Like [`open_mounted_sifs_service`], with a cooperative cancellation flag
/// installed on the service before any distributed work runs. Budgeted callers
/// (app catalog loads) flip the flag to abandon the open and every subsequent
/// distributed operation promptly when the cluster link is degraded.
pub fn open_mounted_sifs_service_with_cancellation(
    data_dir: Option<PathBuf>,
    cancellation: Option<crate::DfsCancellationFlag>,
) -> Result<OpenedMountedSifsService, DfsError> {
    let data_dir = match data_dir {
        Some(path) => SinomoDataDir::new(path),
        None => SinomoDataDir::from_env_or_home().map_err(|source| DfsError::Metadata {
            message: source.to_string(),
        })?,
    };
    let (cluster_paths, cluster_state, local_node_id, peer_node_ids) =
        mounted_sifs_cluster_context(&data_dir)?;
    let peer_transport = Arc::new(MountedSifsPeerTransport {
        data_dir: data_dir.clone(),
        cluster_paths: cluster_paths.clone(),
    });
    let sync_peer_transport: Arc<dyn DfsPeerTransport> = peer_transport.clone();
    let async_peer_transport: Arc<dyn AsyncDfsPeerTransport> = peer_transport;
    let control_plane = if cluster_paths.kv_paths().snapshot.exists() {
        Some(Box::new(MountedSifsControlPlaneKvClient {
            cluster_paths: cluster_paths.clone(),
            peer_transport: Arc::clone(&sync_peer_transport),
        }) as Box<dyn DfsControlPlaneKvClient>)
    } else {
        None
    };
    open_mounted_sifs_service_from_context(
        &data_dir,
        &cluster_paths,
        cluster_state.as_ref(),
        local_node_id,
        peer_node_ids,
        sync_peer_transport,
        async_peer_transport,
        control_plane,
        cancellation,
    )
}

/// Opens mounted SIFS with caller-owned peer transports and routes clustered
/// control-plane commits through the same transport.
///
/// Application mounts use this entry point with a local-daemon-backed peer so
/// both content/metadata traffic and cluster-KV transactions share the
/// daemon's canonical routes and tunnels. Reads continue to use the local KV
/// snapshot; no clustered write falls back to a standalone router.
pub fn open_mounted_sifs_service_with_peer_transports_and_default_control_plane(
    data_dir: SinomoDataDir,
    sync_peer_transport: Arc<dyn DfsPeerTransport>,
    async_peer_transport: Arc<dyn AsyncDfsPeerTransport>,
) -> Result<OpenedMountedSifsService, DfsError> {
    let (cluster_paths, cluster_state, local_node_id, peer_node_ids) =
        mounted_sifs_cluster_context(&data_dir)?;
    let control_plane = cluster_paths.kv_paths().snapshot.exists().then(|| {
        Box::new(MountedSifsControlPlaneKvClient {
            cluster_paths: cluster_paths.clone(),
            peer_transport: Arc::clone(&sync_peer_transport),
        }) as Box<dyn DfsControlPlaneKvClient>
    });
    open_mounted_sifs_service_from_context(
        &data_dir,
        &cluster_paths,
        cluster_state.as_ref(),
        local_node_id,
        peer_node_ids,
        sync_peer_transport,
        async_peer_transport,
        control_plane,
        None,
    )
}

/// Opens mounted SIFS with caller-owned DFS peer transports.
///
/// Daemon hosts use this entry point so browser operations share the daemon's
/// established cluster router instead of constructing a second router inside
/// the daemon process. Any authorized cluster node remains a DFS client; shard
/// ownership only selects the target node for the request.
pub fn open_mounted_sifs_service_with_peer_transports(
    data_dir: SinomoDataDir,
    sync_peer_transport: Arc<dyn DfsPeerTransport>,
    async_peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
) -> Result<OpenedMountedSifsService, DfsError> {
    let (cluster_paths, cluster_state, local_node_id, peer_node_ids) =
        mounted_sifs_cluster_context(&data_dir)?;
    open_mounted_sifs_service_from_context(
        &data_dir,
        &cluster_paths,
        cluster_state.as_ref(),
        local_node_id,
        peer_node_ids,
        sync_peer_transport,
        async_peer_transport,
        control_plane,
        None,
    )
}

fn mounted_sifs_cluster_context(
    data_dir: &SinomoDataDir,
) -> Result<
    (
        ClusterPaths,
        Option<ClusterBootstrapState>,
        NodeId,
        Vec<NodeId>,
    ),
    DfsError,
> {
    data_dir
        .ensure_root_layout()
        .map_err(|source| DfsError::Metadata {
            message: source.to_string(),
        })?;
    let cluster_paths = ClusterPaths::new(data_dir);
    let cluster_state = if cluster_paths.state.exists() {
        sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths).ok()
    } else {
        None
    };
    let local_node_id = cluster_state
        .as_ref()
        .map(|state| state.local_node_id.clone())
        .unwrap_or_else(fallback_dfs_node_id);
    let peer_node_ids = cluster_state
        .as_ref()
        .map(cluster_dfs_peer_node_ids)
        .unwrap_or_default();
    Ok((cluster_paths, cluster_state, local_node_id, peer_node_ids))
}

#[allow(clippy::too_many_arguments)]
fn open_mounted_sifs_service_from_context(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    cluster_state: Option<&ClusterBootstrapState>,
    local_node_id: NodeId,
    peer_node_ids: Vec<NodeId>,
    sync_peer_transport: Arc<dyn DfsPeerTransport>,
    async_peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
    cancellation: Option<crate::DfsCancellationFlag>,
) -> Result<OpenedMountedSifsService, DfsError> {
    let mut service = open_cluster_dfs_service_with_peer_transports_for_nodes(
        data_dir,
        cluster_paths,
        local_node_id.clone(),
        peer_node_ids,
        sync_peer_transport,
        async_peer_transport,
        control_plane,
    )?;
    service.set_cancellation(cancellation);
    service
        .bootstrap_distributed_sifs()
        .map_err(|source| match source {
            DfsPosixError::Dfs(error) => error,
            other => DfsError::Metadata {
                message: other.to_string(),
            },
        })?;
    Ok(OpenedMountedSifsService {
        service,
        replica_candidates: mounted_sifs_replica_candidates_from_context(
            cluster_paths,
            cluster_state,
            local_node_id,
        ),
    })
}

fn mounted_sifs_replica_candidates_from_context(
    cluster_paths: &ClusterPaths,
    cluster_state: Option<&ClusterBootstrapState>,
    local_node_id: NodeId,
) -> Vec<DfsNodeId> {
    if cluster_state.is_none() {
        return vec![local_node_id];
    }
    let advertisements = sinomo_core::kv::read_cluster_kv_prefix_entries(
        &cluster_paths.kv_paths(),
        content_services_prefix(),
    )
    .unwrap_or_default()
    .into_iter()
    .filter_map(|entry| entry.decode::<DfsNodeAdvertisement>().ok());
    mounted_sifs_content_replica_candidates(advertisements)
}

/// Returns the current content-service nodes eligible to store SIFS replicas.
///
/// Cluster membership grants client access but does not make a node a storage
/// target. In an initialized cluster this therefore returns only nodes with a
/// current `accepts_content` service advertisement. An unclustered standalone
/// mount keeps its local-only behavior.
pub fn discover_mounted_sifs_content_replica_candidates(
    data_dir: &SinomoDataDir,
) -> Result<Vec<DfsNodeId>, DfsError> {
    let cluster_paths = ClusterPaths::new(data_dir);
    if !cluster_paths.state.exists() {
        return Ok(vec![fallback_dfs_node_id()]);
    }
    let _state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
        .map_err(DfsError::from_network)?;
    let entries = sinomo_core::kv::read_cluster_kv_prefix_entries(
        &cluster_paths.kv_paths(),
        content_services_prefix(),
    )
    .map_err(DfsError::from)?;
    Ok(mounted_sifs_content_replica_candidates(
        entries
            .into_iter()
            .filter_map(|entry| entry.decode::<DfsNodeAdvertisement>().ok()),
    ))
}

fn mounted_sifs_content_replica_candidates(
    advertisements: impl IntoIterator<Item = DfsNodeAdvertisement>,
) -> Vec<DfsNodeId> {
    let mut candidates = Vec::new();
    for advertisement in advertisements {
        if advertisement.accepts_content && !candidates.contains(&advertisement.node_id) {
            candidates.push(advertisement.node_id);
        }
    }
    candidates
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct RecordingMountedPeerTransport {
        targets: std::sync::Mutex<Vec<NodeId>>,
    }

    impl DfsPeerTransport for RecordingMountedPeerTransport {
        fn call_object_rpc(
            &self,
            target_node_id: &NodeId,
            request: &ObjectRpcRequest,
            _options: DfsObjectRpcSendOptions,
        ) -> Result<ObjectRpcResponse, DfsPeerTransportError> {
            self.targets
                .lock()
                .expect("targets")
                .push(target_node_id.clone());
            assert_eq!(request.route, sinomo_core::kv::CLUSTER_KV_TXN_ROUTE);
            let payload = sinomo_core::kv::cluster_kv_txn_response_rpc_payload(&KvTxnResponse {
                succeeded: true,
                revision: KvRevision(7),
                responses: Vec::new(),
            })
            .expect("txn response payload");
            Ok(ObjectRpcResponse::from_encoded_body(
                payload.route,
                payload.body,
            ))
        }
    }

    #[test]
    fn content_replica_candidates_exclude_client_only_nodes() {
        let client = NodeId::from_legacy_named("node_mounted_sifs_client").expect("client");
        let first_server =
            NodeId::from_legacy_named("node_mounted_sifs_content_a").expect("first server");
        let second_server =
            NodeId::from_legacy_named("node_mounted_sifs_content_b").expect("second server");
        let mut client_advertisement = DfsNodeAdvertisement::metadata(client);
        client_advertisement.accepts_content = false;

        assert_eq!(
            mounted_sifs_content_replica_candidates([
                client_advertisement,
                DfsNodeAdvertisement::content(first_server.clone()),
                DfsNodeAdvertisement::content(second_server.clone()),
            ]),
            vec![first_server, second_server]
        );
    }

    #[test]
    fn mounted_control_plane_commits_through_the_supplied_peer_transport() {
        let root = sinomo_testdir::TestDir::new("sinomo-mounted-sifs-control-plane-peer");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("data directory");
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local_node =
            NodeId::from_legacy_named("node_mounted_sifs_control_plane").expect("local node");
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named("cluster_mounted_sifs_control_plane")
                .expect("cluster id"),
            "mounted SIFS control plane",
            local_node.clone(),
            "local",
            "public-key",
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("cluster KV state");
        let peer = Arc::new(RecordingMountedPeerTransport {
            targets: std::sync::Mutex::new(Vec::new()),
        });
        let peer_transport: Arc<dyn DfsPeerTransport> = peer.clone();
        let mut client = MountedSifsControlPlaneKvClient {
            cluster_paths,
            peer_transport,
        };

        let response = client
            .txn(KvTxn {
                compares: Vec::new(),
                success: Vec::new(),
                failure: Vec::new(),
            })
            .expect("txn");

        assert_eq!(response.revision, KvRevision(7));
        assert_eq!(
            peer.targets.lock().expect("targets").as_slice(),
            &[local_node]
        );
    }
}

#[derive(Clone)]
struct MountedSifsControlPlaneKvClient {
    cluster_paths: ClusterPaths,
    peer_transport: Arc<dyn DfsPeerTransport>,
}

impl std::fmt::Debug for MountedSifsControlPlaneKvClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MountedSifsControlPlaneKvClient")
            .field("cluster_paths", &self.cluster_paths)
            .field("peer_transport", &"dyn DfsPeerTransport")
            .finish()
    }
}

impl DfsControlPlaneKvClient for MountedSifsControlPlaneKvClient {
    fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
        sinomo_core::kv::read_cluster_kv_entry(&self.cluster_paths.kv_paths(), key)
    }

    fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
        sinomo_core::kv::read_cluster_kv_prefix_entries(&self.cluster_paths.kv_paths(), prefix)
    }

    fn put(&mut self, key: String, value: Vec<u8>) -> Result<KvRevision, KvError> {
        let response = self.txn(KvTxn {
            compares: Vec::new(),
            success: vec![KvOp::Put {
                key,
                value,
                lease_id: None,
            }],
            failure: Vec::new(),
        })?;
        Ok(response.revision)
    }

    fn txn(&mut self, txn: KvTxn) -> Result<KvTxnResponse, KvError> {
        let state = sinomo_core::cluster::read_cluster_state(&self.cluster_paths)
            .map_err(|source| mounted_sifs_kv_io_error(&self.cluster_paths, source))?;
        let rpc_payload =
            sinomo_core::kv::cluster_kv_txn_rpc_payload(&txn).map_err(|source| KvError::Io {
                path: self.cluster_paths.kv_paths().snapshot.display().to_string(),
                detail: source.to_string(),
            })?;
        let request = object_rpc_request_from_kv_payload(rpc_payload);
        let response = self
            .peer_transport
            .call_object_rpc(
                &state.local_node_id,
                &request,
                DfsObjectRpcSendOptions::default(),
            )
            .map_err(|source| mounted_sifs_kv_io_error(&self.cluster_paths, source))?;
        response
            .error_detail()
            .map_err(|source| mounted_sifs_kv_io_error(&self.cluster_paths, source))?;
        sinomo_core::kv::decode_cluster_kv_txn_rpc_body(&response.body).map_err(|source| {
            KvError::Io {
                path: self.cluster_paths.kv_paths().snapshot.display().to_string(),
                detail: source.to_string(),
            }
        })
    }

    fn linearizable_compare_value(
        &mut self,
        key: String,
        expected_value: Vec<u8>,
        barrier_key: String,
    ) -> Result<bool, KvError> {
        // A fresh write keeps this on the quorum-backed transaction path. A
        // compare-only transaction could otherwise be satisfied by the
        // cluster KV semantic no-effect shortcut.
        let barrier_value =
            sinomo_core::kv::encode_kv_value(&sinomo_core::Id::generate().to_string())?;
        Ok(self
            .txn(KvTxn {
                compares: vec![KvCompare::ValueEquals {
                    key,
                    value: expected_value,
                }],
                success: vec![KvOp::Put {
                    key: barrier_key,
                    value: barrier_value,
                    lease_id: None,
                }],
                failure: Vec::new(),
            })?
            .succeeded)
    }
}

fn mounted_sifs_kv_io_error(
    cluster_paths: &ClusterPaths,
    source: impl std::fmt::Display,
) -> KvError {
    KvError::Io {
        path: cluster_paths.kv_paths().snapshot.display().to_string(),
        detail: source.to_string(),
    }
}

#[derive(Clone, Debug)]
struct MountedSifsPeerTransport {
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
}

impl MountedSifsPeerTransport {
    async fn call_object_rpc_async_inner(
        &self,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        options: DfsObjectRpcSendOptions,
        runtime: &tokio::runtime::Handle,
    ) -> Result<ObjectRpcResponse, NetworkError> {
        let state = sinomo_core::cluster::read_cluster_state(&self.cluster_paths)?;
        let mut host = MountedSifsObjectRpcHost {
            data_dir: self.data_dir.clone(),
            runtime: runtime.clone(),
        };
        let mut client = sinomo_core::network::ClusterObjectRpcClient::routed(
            &mut host,
            runtime,
            &self.cluster_paths,
        );
        let response = client
            .call_target(
                &state,
                target_node_id,
                request,
                options.cluster_rpc_options(),
            )
            .await?;
        response.error_detail()?;
        Ok(response)
    }

    async fn call_object_rpc_session_async_inner(
        &self,
        target_node_id: &NodeId,
        call: ObjectRpcSessionCall,
        options: DfsObjectRpcSendOptions,
        runtime: &tokio::runtime::Handle,
    ) -> Result<ObjectRpcSessionResult, NetworkError> {
        let state = sinomo_core::cluster::read_cluster_state(&self.cluster_paths)?;
        let mut host = MountedSifsObjectRpcHost {
            data_dir: self.data_dir.clone(),
            runtime: runtime.clone(),
        };
        let mut client = sinomo_core::network::ClusterObjectRpcClient::routed(
            &mut host,
            runtime,
            &self.cluster_paths,
        );
        let result = client
            .call_target_session(&state, target_node_id, call, options.cluster_rpc_options())
            .await?;
        result.response.error_detail()?;
        Ok(result)
    }
}

impl DfsPeerTransport for MountedSifsPeerTransport {
    fn call_object_rpc(
        &self,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        options: DfsObjectRpcSendOptions,
    ) -> Result<ObjectRpcResponse, DfsPeerTransportError> {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            return if matches!(
                handle.runtime_flavor(),
                tokio::runtime::RuntimeFlavor::MultiThread
            ) {
                let result = tokio::task::block_in_place(|| {
                    handle.block_on(self.call_object_rpc_async_inner(
                        target_node_id,
                        request,
                        options,
                        &handle,
                    ))
                });
                result.map_err(|source| DfsPeerTransportError::new(source.to_string()))
            } else {
                Err(DfsPeerTransportError::new(
                    "mounted SIFS sync peer transport cannot run inside a current-thread Tokio runtime; use the async transport",
                ))
            };
        }

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
        let result = runtime.block_on(self.call_object_rpc_async_inner(
            target_node_id,
            request,
            options,
            runtime.handle(),
        ));
        runtime.shutdown_timeout(Duration::from_millis(250));
        result.map_err(|source| DfsPeerTransportError::new(source.to_string()))
    }

    fn call_object_rpc_session(
        &self,
        target_node_id: &NodeId,
        call: ObjectRpcSessionCall,
        options: DfsObjectRpcSendOptions,
    ) -> Result<ObjectRpcSessionResult, DfsPeerTransportError> {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            return if matches!(
                handle.runtime_flavor(),
                tokio::runtime::RuntimeFlavor::MultiThread
            ) {
                let result = tokio::task::block_in_place(|| {
                    handle.block_on(self.call_object_rpc_session_async_inner(
                        target_node_id,
                        call,
                        options,
                        &handle,
                    ))
                });
                result.map_err(|source| DfsPeerTransportError::new(source.to_string()))
            } else {
                Err(DfsPeerTransportError::new(
                    "mounted SIFS sync peer transport cannot run inside a current-thread Tokio runtime; use the async transport",
                ))
            };
        }

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
        let result = runtime.block_on(self.call_object_rpc_session_async_inner(
            target_node_id,
            call,
            options,
            runtime.handle(),
        ));
        runtime.shutdown_timeout(Duration::from_millis(250));
        result.map_err(|source| DfsPeerTransportError::new(source.to_string()))
    }
}

impl AsyncDfsPeerTransport for MountedSifsPeerTransport {
    fn call_object_rpc_async<'a>(
        &'a self,
        target_node_id: &'a NodeId,
        request: &'a ObjectRpcRequest,
        options: DfsObjectRpcSendOptions,
    ) -> DfsPeerTransportFuture<'a, Result<ObjectRpcResponse, DfsPeerTransportError>> {
        Box::pin(async move {
            let runtime = tokio::runtime::Handle::try_current()
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
            self.call_object_rpc_async_inner(target_node_id, request, options, &runtime)
                .await
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))
        })
    }

    fn call_object_rpc_session_async<'a>(
        &'a self,
        target_node_id: &'a NodeId,
        call: ObjectRpcSessionCall,
        options: DfsObjectRpcSendOptions,
    ) -> DfsPeerTransportFuture<'a, Result<ObjectRpcSessionResult, DfsPeerTransportError>> {
        Box::pin(async move {
            let runtime = tokio::runtime::Handle::try_current()
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
            self.call_object_rpc_session_async_inner(target_node_id, call, options, &runtime)
                .await
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))
        })
    }
}

struct MountedSifsObjectRpcHost {
    data_dir: SinomoDataDir,
    runtime: tokio::runtime::Handle,
}

impl sinomo_core::network::ClusterSignedObjectRpcAsyncHost for MountedSifsObjectRpcHost {
    fn sign_object_rpc_request_async<'a>(
        &'a mut self,
        request: &'a ObjectRpcRequest,
    ) -> sinomo_core::network::ClusterSignedObjectRpcFuture<'a, Vec<u8>> {
        let data_dir = self.data_dir.clone();
        let request = request.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            runtime
                .spawn_blocking(move || {
                    sinomo_core::network::sign_daemon_rpc_request_from_data_dir(&data_dir, &request)
                })
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "mounted SIFS object RPC signing".to_string(),
                    detail: format!("object RPC signing task failed: {source}"),
                })?
        })
    }
}
