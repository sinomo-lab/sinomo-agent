//! Mounted SIFS adapter over local cluster state and DFS peer transports.
//!
//! This module owns the low-level glue required to open SIFS as a mounted
//! networked DFS service from a local data directory.

use sinomo_core::ClusterBootstrapState;
use sinomo_kv::{KvEntry, KvError, KvOp, KvRevision, KvTxn, KvTxnResponse};
use sinomo_local::SinomoDataDir;
use sinomo_network::{ClusterPaths, NetworkError, ObjectRpcRequest, ObjectRpcResponse};
use sinomo_node::NodeId;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::{
    AsyncDfsPeerTransport, DfsControlPlaneKvClient, DfsEndpointTransport, DfsError,
    DfsNodeAdvertisement, DfsNodeId, DfsObjectRpcSendOptions, DfsPeerTransport,
    DfsPeerTransportError, DfsPeerTransportFuture, DfsPosixError, NetworkedDfsService,
    cluster_dfs_data_endpoints, content_services_prefix, fallback_dfs_node_id,
    object_rpc_request_from_kv_payload, open_cluster_dfs_service_with_peer_transports,
};

pub type MountedSifsService = NetworkedDfsService<DfsEndpointTransport>;

#[derive(Debug)]
pub struct OpenedMountedSifsService {
    pub service: MountedSifsService,
    pub replica_candidates: Vec<DfsNodeId>,
}

const MOUNTED_SIFS_CLUSTER_KV_TXN_TIMEOUT: Duration = Duration::from_secs(5);

pub fn open_mounted_sifs_service(
    data_dir: Option<PathBuf>,
) -> Result<OpenedMountedSifsService, DfsError> {
    let data_dir = match data_dir {
        Some(path) => SinomoDataDir::new(path),
        None => SinomoDataDir::from_env_or_home().map_err(|source| DfsError::Metadata {
            message: source.to_string(),
        })?,
    };
    data_dir
        .ensure_root_layout()
        .map_err(|source| DfsError::Metadata {
            message: source.to_string(),
        })?;
    let cluster_paths = ClusterPaths::new(&data_dir);
    let cluster_state = if cluster_paths.state.exists() {
        sinomo_network::read_cluster_state_without_kv(&cluster_paths).ok()
    } else {
        None
    };
    let local_node_id = cluster_state
        .as_ref()
        .map(|state| state.local_node_id.clone())
        .unwrap_or_else(fallback_dfs_node_id);
    let endpoints = cluster_state
        .as_ref()
        .map(|state| cluster_dfs_data_endpoints(&cluster_paths, state))
        .unwrap_or_default();
    let peer_transport = Arc::new(MountedSifsPeerTransport {
        data_dir: data_dir.clone(),
        cluster_paths: cluster_paths.clone(),
    });
    let sync_peer_transport: Arc<dyn DfsPeerTransport> = peer_transport.clone();
    let async_peer_transport: Arc<dyn AsyncDfsPeerTransport> = peer_transport;
    let control_plane = if cluster_paths.kv_snapshot.exists() {
        Some(Box::new(MountedSifsControlPlaneKvClient {
            data_dir: data_dir.clone(),
            cluster_paths: cluster_paths.clone(),
        }) as Box<dyn DfsControlPlaneKvClient>)
    } else {
        None
    };
    let mut service = open_cluster_dfs_service_with_peer_transports(
        &data_dir,
        &cluster_paths,
        local_node_id.clone(),
        endpoints,
        sync_peer_transport,
        async_peer_transport,
        control_plane,
    )?;
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
        replica_candidates: mounted_sifs_replica_candidates(
            &cluster_paths,
            cluster_state.as_ref(),
            local_node_id,
        ),
    })
}

fn mounted_sifs_replica_candidates(
    cluster_paths: &ClusterPaths,
    cluster_state: Option<&ClusterBootstrapState>,
    local_node_id: NodeId,
) -> Vec<DfsNodeId> {
    let mut candidates = vec![local_node_id];
    if cluster_state.is_some()
        && let Ok(entries) = sinomo_kv::read_cluster_kv_prefix_entries(
            &cluster_paths.kv_paths(),
            content_services_prefix(),
        )
    {
        for advertisement in entries
            .into_iter()
            .filter_map(|entry| entry.decode::<DfsNodeAdvertisement>().ok())
        {
            if advertisement.accepts_content && !candidates.contains(&advertisement.node_id) {
                candidates.push(advertisement.node_id);
            }
        }
    }
    candidates
}

#[derive(Clone, Debug)]
struct MountedSifsControlPlaneKvClient {
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
}

impl DfsControlPlaneKvClient for MountedSifsControlPlaneKvClient {
    fn get(&self, key: &str) -> Result<Option<KvEntry>, KvError> {
        sinomo_kv::read_cluster_kv_entry(&self.cluster_paths.kv_paths(), key)
    }

    fn range_prefix(&self, prefix: &str) -> Result<Vec<KvEntry>, KvError> {
        sinomo_kv::read_cluster_kv_prefix_entries(&self.cluster_paths.kv_paths(), prefix)
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
        let state = sinomo_network::read_cluster_state(&self.cluster_paths)
            .map_err(|source| mounted_sifs_kv_io_error(&self.cluster_paths, source))?;
        let rpc_payload =
            sinomo_kv::cluster_kv_txn_rpc_payload(&txn).map_err(|source| KvError::Io {
                path: self.cluster_paths.kv_snapshot.display().to_string(),
                detail: source.to_string(),
            })?;
        let request = object_rpc_request_from_kv_payload(rpc_payload);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|source| KvError::Io {
                path: self.cluster_paths.kv_snapshot.display().to_string(),
                detail: format!("mounted SIFS cluster KV txn runtime: {source}"),
            })?;
        let mut host = MountedSifsObjectRpcHost {
            data_dir: self.data_dir.clone(),
            runtime: runtime.handle().clone(),
        };
        let options = sinomo_network::ClusterObjectRpcOptions::new(
            sinomo_network::DaemonRpcTimeouts::connect_and_idle(
                MOUNTED_SIFS_CLUSTER_KV_TXN_TIMEOUT,
            ),
        );
        let mut client = sinomo_network::ClusterObjectRpcClient::routed(
            &mut host,
            runtime.handle(),
            &self.cluster_paths,
        );
        let response_result = runtime
            .block_on(client.call_target(&state, &state.local_node_id, &request, options))
            .and_then(|response| {
                response.error_detail()?;
                Ok(response)
            });
        runtime.shutdown_timeout(Duration::from_millis(250));
        let response = response_result
            .map_err(|source| mounted_sifs_kv_io_error(&self.cluster_paths, source))?;
        sinomo_kv::decode_cluster_kv_txn_rpc_body(&response.body).map_err(|source| KvError::Io {
            path: self.cluster_paths.kv_snapshot.display().to_string(),
            detail: source.to_string(),
        })
    }
}

fn mounted_sifs_kv_io_error(cluster_paths: &ClusterPaths, source: NetworkError) -> KvError {
    KvError::Io {
        path: cluster_paths.kv_snapshot.display().to_string(),
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
        let state = sinomo_network::read_cluster_state(&self.cluster_paths)?;
        let mut host = MountedSifsObjectRpcHost {
            data_dir: self.data_dir.clone(),
            runtime: runtime.clone(),
        };
        let mut client =
            sinomo_network::ClusterObjectRpcClient::routed(&mut host, runtime, &self.cluster_paths);
        let response = client
            .call_target(
                &state,
                target_node_id,
                request,
                sinomo_network::ClusterObjectRpcOptions::new(
                    sinomo_network::DaemonRpcTimeouts::connect_and_idle(options.connect_timeout),
                ),
            )
            .await?;
        response.error_detail()?;
        Ok(response)
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
}

struct MountedSifsObjectRpcHost {
    data_dir: SinomoDataDir,
    runtime: tokio::runtime::Handle,
}

impl sinomo_network::ClusterSignedEndpointRpcAsyncHost for MountedSifsObjectRpcHost {
    fn sign_endpoint_rpc_request_async<'a>(
        &'a mut self,
        request: &'a ObjectRpcRequest,
    ) -> sinomo_network::ClusterSignedEndpointRpcFuture<'a, Vec<u8>> {
        let data_dir = self.data_dir.clone();
        let request = request.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            runtime
                .spawn_blocking(move || {
                    sinomo_network::sign_daemon_rpc_request_from_data_dir(&data_dir, &request)
                })
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "mounted SIFS endpoint signing".to_string(),
                    detail: format!("endpoint signing task failed: {source}"),
                })?
        })
    }
}
