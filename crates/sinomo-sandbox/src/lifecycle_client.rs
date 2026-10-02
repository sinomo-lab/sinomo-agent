//! One lifecycle interface for sandboxes on any node.
//!
//! Callers create, inspect, update, and destroy a sandbox the same way
//! whether it is hosted on this node or another cluster member. Requests go
//! through the local daemon's sandbox service; a request for another node is
//! forwarded by the daemon to that node's sandbox service. Only hosts without
//! a daemon (a one-node process such as readiness gates and tests) serve the
//! request in process, with the same local backend the service would use.

use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::ObjectRpcRequest;
use sinomo_core::node::NodeId;

use crate::{
    LocalSandboxBackend, SANDBOX_CREATE_ROUTE, SANDBOX_DESTROY_ROUTE, SANDBOX_RPC_CATEGORY,
    SANDBOX_STATUS_ROUTE, SANDBOX_UPDATE_ROUTE, SandboxBackend, SandboxCreateInstanceRequest,
    SandboxCreateInstanceResponse, SandboxDestroyRequest, SandboxDestroyResponse,
    SandboxStatusRequest, SandboxStatusResponse, SandboxUpdateInstanceRequest,
    SandboxUpdateInstanceResponse,
};

const SANDBOX_LIFECYCLE_RPC_TIMEOUT: Duration = Duration::from_secs(60);
/// Prefix of the store's not-found error, which crosses the service boundary
/// as text. Pinned by a test against `SandboxStoreError::NotFound`.
const SANDBOX_NOT_FOUND_PREFIX: &str = "sandbox instance not found";

#[derive(Debug, thiserror::Error, Clone, Eq, PartialEq)]
pub enum SandboxLifecycleError {
    #[error("sandbox `{sandbox_id}` was not found")]
    NotFound { sandbox_id: String },
    #[error("sandbox `{sandbox_id}` on node `{node_id}` requires a running daemon")]
    RemoteRequiresDaemon { sandbox_id: String, node_id: String },
    #[error("{0}")]
    Failed(String),
}

#[derive(Clone, Debug)]
enum Transport {
    Daemon { address: String },
    InProcess { runtime_root: std::path::PathBuf },
}

/// Lifecycle client for sandboxes on this node or any cluster member.
#[derive(Clone, Debug)]
pub struct SandboxLifecycleClient {
    data_dir: SinomoDataDir,
    local_node_id: Option<NodeId>,
    transport: Transport,
}

impl SandboxLifecycleClient {
    /// Route through the daemon at `address`, which serves local requests and
    /// forwards requests for other nodes. `local_node_id` identifies requests
    /// that need no forwarding.
    pub fn daemon(
        data_dir: SinomoDataDir,
        address: impl Into<String>,
        local_node_id: Option<NodeId>,
    ) -> Self {
        Self {
            data_dir,
            local_node_id,
            transport: Transport::Daemon {
                address: address.into(),
            },
        }
    }

    /// Serve requests in process for a host that runs without a daemon. Only
    /// this node's sandboxes are reachable.
    pub fn in_process(data_dir: SinomoDataDir) -> Self {
        let runtime_root = data_dir.root().join("sandbox").join("runtime");
        Self {
            data_dir,
            local_node_id: None,
            transport: Transport::InProcess { runtime_root },
        }
    }

    /// In-process client over an explicit sandbox runtime root, for hosts and
    /// tests that keep instances outside the data directory layout.
    pub fn in_process_at_runtime_root(runtime_root: impl Into<std::path::PathBuf>) -> Self {
        let runtime_root = runtime_root.into();
        Self {
            data_dir: SinomoDataDir::new(&runtime_root),
            local_node_id: None,
            transport: Transport::InProcess { runtime_root },
        }
    }

    pub fn create(
        &self,
        node: Option<&NodeId>,
        request: SandboxCreateInstanceRequest,
    ) -> Result<SandboxCreateInstanceResponse, SandboxLifecycleError> {
        let sandbox_id = request
            .sandbox_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        self.call(
            node,
            SANDBOX_CREATE_ROUTE,
            &sandbox_id,
            request,
            |backend, request| backend.create(request),
        )
    }

    pub fn status(
        &self,
        node: Option<&NodeId>,
        request: SandboxStatusRequest,
    ) -> Result<SandboxStatusResponse, SandboxLifecycleError> {
        let sandbox_id = request.sandbox_id.to_string();
        self.call(
            node,
            SANDBOX_STATUS_ROUTE,
            &sandbox_id,
            request,
            |backend, request| backend.status(request),
        )
    }

    pub fn update(
        &self,
        node: Option<&NodeId>,
        request: SandboxUpdateInstanceRequest,
    ) -> Result<SandboxUpdateInstanceResponse, SandboxLifecycleError> {
        let sandbox_id = request.sandbox_id.to_string();
        self.call(
            node,
            SANDBOX_UPDATE_ROUTE,
            &sandbox_id,
            request,
            |backend, request| backend.update(request),
        )
    }

    pub fn destroy(
        &self,
        node: Option<&NodeId>,
        request: SandboxDestroyRequest,
    ) -> Result<SandboxDestroyResponse, SandboxLifecycleError> {
        let sandbox_id = request.sandbox_id.to_string();
        self.call(
            node,
            SANDBOX_DESTROY_ROUTE,
            &sandbox_id,
            request,
            |backend, request| backend.destroy(request),
        )
    }

    fn is_local(&self, node: Option<&NodeId>) -> bool {
        match node {
            None => true,
            Some(node) => self.local_node_id.as_ref() == Some(node),
        }
    }

    fn call<Req, Resp>(
        &self,
        node: Option<&NodeId>,
        route: &str,
        sandbox_id: &str,
        request: Req,
        in_process: impl FnOnce(
            &mut dyn SandboxBackend,
            Req,
        ) -> Result<Resp, crate::SandboxBackendError>,
    ) -> Result<Resp, SandboxLifecycleError>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
    {
        let classify = |detail: String| {
            if detail.contains(SANDBOX_NOT_FOUND_PREFIX) {
                SandboxLifecycleError::NotFound {
                    sandbox_id: sandbox_id.to_string(),
                }
            } else {
                SandboxLifecycleError::Failed(detail)
            }
        };
        match &self.transport {
            Transport::InProcess { runtime_root } => {
                if let Some(node) = node.filter(|_| !self.is_local(node)) {
                    return Err(SandboxLifecycleError::RemoteRequiresDaemon {
                        sandbox_id: sandbox_id.to_string(),
                        node_id: node.to_string(),
                    });
                }
                let mut backend = LocalSandboxBackend::new(runtime_root.clone());
                in_process(&mut backend, request).map_err(|error| classify(error.to_string()))
            }
            Transport::Daemon { address } => {
                let rpc = ObjectRpcRequest::encode_body(route, SANDBOX_RPC_CATEGORY, &request)
                    .map_err(|error| SandboxLifecycleError::Failed(error.to_string()))?;
                let response = match node.filter(|_| !self.is_local(node)) {
                    None => {
                        sinomo_api::send_daemon_rpc(address, &rpc, SANDBOX_LIFECYCLE_RPC_TIMEOUT)
                    }
                    Some(node) => sinomo_api::DaemonApiClient::at_address(address.clone())
                        .with_timeout(SANDBOX_LIFECYCLE_RPC_TIMEOUT)
                        .object_rpc_to_target_from_data_dir_with_affinity(
                            &self.data_dir,
                            node,
                            rpc,
                            SANDBOX_LIFECYCLE_RPC_TIMEOUT,
                            sinomo_api::DaemonObjectRpcDestinationProcessAffinity::MainDaemon,
                        ),
                }
                .map_err(|error| classify(error.to_string()))?;
                response
                    .decode_body()
                    .map_err(|error| classify(error.to_string()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_found_text_matches_the_store_error() {
        let error = crate::SandboxStoreError::NotFound {
            sandbox_id: crate::SandboxId::derived("test", b"missing"),
        };
        assert!(error.to_string().starts_with(SANDBOX_NOT_FOUND_PREFIX));
    }
}
