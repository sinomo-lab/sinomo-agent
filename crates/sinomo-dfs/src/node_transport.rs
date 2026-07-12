//! Node-routed DFS data-plane transport and served-message processing.

use std::collections::{BTreeSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Serialize;
#[cfg(test)]
use sinomo_core::local::SinomoDataDir;
#[cfg(test)]
use sinomo_core::network::ClusterPaths;
use sinomo_core::network::{
    NetworkError, ObjectRpcBodySource, ObjectRpcIncomingBody, ObjectRpcRequest, ObjectRpcResponse,
    ObjectRpcSessionCall, ObjectRpcSessionResult, SinomoObjectCodec,
};
use sinomo_core::node::NodeId;

#[cfg(test)]
use crate::dfs_direct_read_response_from_rpc_response;
use crate::{
    AsyncDfsMessageTransport, DFS_DATA_PLANE_MESSAGE_DRAIN_MAX_PASSES,
    DfsDirectReadClientBodyResponse, DfsDirectReadClientBodyResult, DfsDirectReadRequest, DfsError,
    DfsMessageCommand, DfsMessageProcessResult, DfsMessageResponse, DfsMessageTransport,
    DfsMessageTransportFuture, DfsNetworkMessage, DfsOutboundBody, DfsReadData, DfsReadResponse,
    DfsReadResult, DfsRoutedMessage, DfsTransportIdentity, DistributedRepairJob,
    NetworkedDfsService, NetworkedDfsServiceEvent, decode_dfs_message_rpc_response,
    dfs_direct_read_request_from_read_request, dfs_direct_read_response_from_rpc_session_result,
    dfs_direct_read_rpc_request, dfs_message_is_read_only_request, dfs_message_rpc_request,
    dfs_network_message_allows_write_repair_progress, dfs_perf_trace, dfs_perf_trace_enabled,
    retained_networked_dfs_service_events,
};
#[cfg(test)]
use crate::{
    DfsControlPlaneKvClient, DfsMetadataJournalReplicationOptions,
    DfsMetadataJournalReplicationReport, LocalContentStore,
    open_cluster_aware_dfs_metadata_backend,
    run_cluster_dfs_metadata_publish_after_events_with_peer_transport,
};

const DFS_SERVED_MESSAGE_REPAIR_MAX_JOBS: usize = 2;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsPeerTransportError {
    detail: String,
}

impl DfsPeerTransportError {
    pub fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }
}

impl std::fmt::Display for DfsPeerTransportError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl std::error::Error for DfsPeerTransportError {}

pub(crate) type DfsMessageSender = Arc<
    dyn Fn(
            &NodeId,
            &DfsMessageCommand,
            DfsObjectRpcSendOptions,
        ) -> Result<DfsMessageResponse, DfsPeerTransportError>
        + Send
        + Sync,
>;

pub(crate) type DfsDirectReadSender = Arc<
    dyn Fn(
            &NodeId,
            &DfsDirectReadRequest,
            DfsObjectRpcSendOptions,
        ) -> Result<DfsReadResponse, DfsPeerTransportError>
        + Send
        + Sync,
>;

pub(crate) type AsyncDfsDirectReadSender = Arc<
    dyn for<'a> Fn(
            &'a NodeId,
            &'a DfsDirectReadRequest,
            DfsObjectRpcSendOptions,
        )
            -> DfsPeerTransportFuture<'a, Result<DfsReadResponse, DfsPeerTransportError>>
        + Send
        + Sync,
>;

pub(crate) type AsyncDfsDirectReadBodySender = Arc<
    dyn for<'a> Fn(
            &'a NodeId,
            &'a DfsDirectReadRequest,
            DfsObjectRpcSendOptions,
        ) -> DfsPeerTransportFuture<
            'a,
            Result<DfsDirectReadClientBodyResponse, DfsPeerTransportError>,
        > + Send
        + Sync,
>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DfsObjectRpcSendOptions {
    /// Connection-phase override. Defaults to the shared network policy.
    pub connect_timeout: Duration,
    /// Optional DFS semantic deadline covering routing and the full response.
    pub overall_timeout: Option<Duration>,
    pub observe_connectivity: bool,
}

impl DfsObjectRpcSendOptions {
    /// Overrides only the connection phase. Stream progress continues to use
    /// the network-owned idle timeout, and no DFS whole-call deadline is added.
    pub fn new(connect_timeout: Duration) -> Self {
        Self {
            connect_timeout,
            overall_timeout: None,
            observe_connectivity: true,
        }
    }

    pub fn with_overall_timeout(mut self, overall_timeout: Duration) -> Self {
        self.overall_timeout = Some(overall_timeout);
        self
    }

    /// Converts DFS's phase override plus optional semantic deadline into the
    /// network-owned routed-RPC policy without conflating the two.
    pub fn cluster_rpc_options(self) -> sinomo_core::network::ClusterObjectRpcOptions {
        let mut route_options =
            if self.connect_timeout == sinomo_core::network::DEFAULT_NETWORK_TIMEOUT {
                sinomo_core::network::ClusterObjectRpcOptions::default()
            } else {
                sinomo_core::network::ClusterObjectRpcOptions::new(
                    sinomo_core::network::DaemonRpcTimeouts::default()
                        .with_connect_timeout(Some(self.connect_timeout)),
                )
            };
        if let Some(overall_timeout) = self.overall_timeout {
            route_options = route_options.with_overall_timeout(overall_timeout);
        }
        route_options
    }

    pub fn with_observe_connectivity(mut self, observe_connectivity: bool) -> Self {
        self.observe_connectivity = observe_connectivity;
        self
    }
}

impl Default for DfsObjectRpcSendOptions {
    fn default() -> Self {
        Self::new(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
    }
}

pub trait DfsPeerTransport: Send + Sync {
    fn call_object_rpc(
        &self,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        options: DfsObjectRpcSendOptions,
    ) -> Result<ObjectRpcResponse, DfsPeerTransportError>;

    fn call_object_rpc_session(
        &self,
        target_node_id: &NodeId,
        call: ObjectRpcSessionCall,
        options: DfsObjectRpcSendOptions,
    ) -> Result<ObjectRpcSessionResult, DfsPeerTransportError> {
        let ObjectRpcSessionCall { request, body } = call;
        let request = if body.is_empty() {
            request
        } else {
            let body_head = crate::body_rpc::decode_dfs_body_message_rpc_request(&request)
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
            let max_bytes = usize::try_from(body_head.body.total_size_bytes)
                .map_err(|_| DfsPeerTransportError::new("DFS request body is too large"))?;
            let bytes = collect_dfs_body_source_sync(body, max_bytes)?;
            let command = crate::body_rpc::attach_dfs_message_request_body(body_head, bytes)
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
            crate::dfs_message_rpc_request(&command)
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))?
        };
        self.call_object_rpc(target_node_id, &request, options)
            .map(ObjectRpcSessionResult::buffered)
    }
}

fn collect_dfs_body_source_sync(
    body: ObjectRpcBodySource,
    max_bytes: usize,
) -> Result<Vec<u8>, DfsPeerTransportError> {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        if matches!(
            handle.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            return tokio::task::block_in_place(|| handle.block_on(body.collect_to_vec(max_bytes)))
                .map_err(|source| DfsPeerTransportError::new(source.to_string()));
        }
        return Err(DfsPeerTransportError::new(
            "sync DFS body adapter cannot run inside a current-thread Tokio runtime",
        ));
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
    let result = runtime
        .block_on(body.collect_to_vec(max_bytes))
        .map_err(|source| DfsPeerTransportError::new(source.to_string()));
    runtime.shutdown_timeout(Duration::from_millis(250));
    result
}

pub type DfsPeerTransportFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait AsyncDfsPeerTransport: Send + Sync {
    fn call_object_rpc_async<'a>(
        &'a self,
        target_node_id: &'a NodeId,
        request: &'a ObjectRpcRequest,
        options: DfsObjectRpcSendOptions,
    ) -> DfsPeerTransportFuture<'a, Result<ObjectRpcResponse, DfsPeerTransportError>>;

    fn call_object_rpc_session_async<'a>(
        &'a self,
        target_node_id: &'a NodeId,
        call: ObjectRpcSessionCall,
        options: DfsObjectRpcSendOptions,
    ) -> DfsPeerTransportFuture<'a, Result<ObjectRpcSessionResult, DfsPeerTransportError>> {
        Box::pin(async move {
            let ObjectRpcSessionCall { request, body } = call;
            let request = if body.is_empty() {
                request
            } else {
                let body_head = crate::body_rpc::decode_dfs_body_message_rpc_request(&request)
                    .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
                let max_bytes = usize::try_from(body_head.body.total_size_bytes)
                    .map_err(|_| DfsPeerTransportError::new("DFS request body is too large"))?;
                let bytes = body
                    .collect_to_vec(max_bytes)
                    .await
                    .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
                let command = crate::body_rpc::attach_dfs_message_request_body(body_head, bytes)
                    .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
                crate::dfs_message_rpc_request(&command)
                    .map_err(|source| DfsPeerTransportError::new(source.to_string()))?
            };
            let response = self
                .call_object_rpc_async(target_node_id, &request, options)
                .await?;
            Ok(ObjectRpcSessionResult::buffered(response))
        })
    }
}

pub(crate) type AsyncDfsMessageSender = Arc<
    dyn for<'a> Fn(
            &'a NodeId,
            &'a DfsMessageCommand,
            DfsObjectRpcSendOptions,
        )
            -> DfsPeerTransportFuture<'a, Result<DfsMessageResponse, DfsPeerTransportError>>
        + Send
        + Sync,
>;

pub(crate) fn async_dfs_message_sender_from_peer_transport(
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
) -> AsyncDfsMessageSender {
    Arc::new(move |target_node_id, command, options| {
        let peer_transport = Arc::clone(&peer_transport);
        Box::pin(async move {
            let request = dfs_message_rpc_request(command)
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
            let call = if let Some((head, descriptor, bytes)) =
                crate::body_rpc::split_dfs_message_request_body(command)
            {
                let request = crate::body_rpc::encode_dfs_body_message_rpc_request(
                    &request, head, descriptor,
                )
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
                ObjectRpcSessionCall::new(request, ObjectRpcBodySource::bytes(bytes))
            } else {
                ObjectRpcSessionCall::buffered(request)
            };
            let response = peer_transport
                .call_object_rpc_session_async(
                    target_node_id,
                    call,
                    options.with_observe_connectivity(!dfs_message_is_read_only_request(command)),
                )
                .await?;
            decode_dfs_message_session_result(response).await
        })
    })
}

async fn decode_dfs_message_session_result(
    result: ObjectRpcSessionResult,
) -> Result<DfsMessageResponse, DfsPeerTransportError> {
    let (response, body) = result.into_parts();
    if body.is_empty() {
        return decode_dfs_message_rpc_response(&response)
            .map_err(|source| DfsPeerTransportError::new(source.to_string()));
    }
    let head = crate::body_rpc::decode_dfs_body_message_rpc_response(&response)
        .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
    let max_bytes = usize::try_from(head.body.total_size_bytes)
        .map_err(|_| DfsPeerTransportError::new("DFS response body is too large"))?;
    let bytes = body
        .collect_to_vec(max_bytes)
        .await
        .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
    crate::body_rpc::attach_dfs_message_response_body(head, bytes)
        .map_err(|source| DfsPeerTransportError::new(source.to_string()))
}

fn decode_dfs_message_session_result_sync(
    result: ObjectRpcSessionResult,
) -> Result<DfsMessageResponse, DfsPeerTransportError> {
    let (response, body) = result.into_parts();
    if body.is_empty() {
        return decode_dfs_message_rpc_response(&response)
            .map_err(|source| DfsPeerTransportError::new(source.to_string()));
    }
    let head = crate::body_rpc::decode_dfs_body_message_rpc_response(&response)
        .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
    let max_bytes = usize::try_from(head.body.total_size_bytes)
        .map_err(|_| DfsPeerTransportError::new("DFS response body is too large"))?;
    let bytes = collect_dfs_incoming_body_sync(body, max_bytes)?;
    crate::body_rpc::attach_dfs_message_response_body(head, bytes)
        .map_err(|source| DfsPeerTransportError::new(source.to_string()))
}

fn collect_dfs_incoming_body_sync(
    body: ObjectRpcIncomingBody,
    max_bytes: usize,
) -> Result<Vec<u8>, DfsPeerTransportError> {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        if matches!(
            handle.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            return tokio::task::block_in_place(|| handle.block_on(body.collect_to_vec(max_bytes)))
                .map_err(|source| DfsPeerTransportError::new(source.to_string()));
        }
        return Err(DfsPeerTransportError::new(
            "sync DFS response body adapter cannot run inside a current-thread Tokio runtime",
        ));
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
    let result = runtime
        .block_on(body.collect_to_vec(max_bytes))
        .map_err(|source| DfsPeerTransportError::new(source.to_string()));
    runtime.shutdown_timeout(Duration::from_millis(250));
    result
}

#[cfg(test)]
pub(crate) fn async_dfs_direct_read_sender_from_peer_transport(
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
) -> AsyncDfsDirectReadSender {
    Arc::new(move |target_node_id, request, options| {
        let peer_transport = Arc::clone(&peer_transport);
        Box::pin(async move {
            let rpc_request = dfs_direct_read_rpc_request(request)
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
            let response = peer_transport
                .call_object_rpc_async(target_node_id, &rpc_request, options)
                .await?;
            dfs_direct_read_response_from_rpc_response(response)
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))
        })
    })
}

pub(crate) fn async_dfs_direct_read_body_sender_from_peer_transport(
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
) -> AsyncDfsDirectReadBodySender {
    Arc::new(move |target_node_id, request, options| {
        let peer_transport = Arc::clone(&peer_transport);
        Box::pin(async move {
            let rpc_request = dfs_direct_read_rpc_request(request)
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
            let response = peer_transport
                .call_object_rpc_session_async(
                    target_node_id,
                    ObjectRpcSessionCall::buffered(rpc_request),
                    options,
                )
                .await?;
            dfs_direct_read_response_from_rpc_session_result(response)
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))
        })
    })
}

pub(crate) fn dfs_message_sender_from_peer_transport(
    peer_transport: Arc<dyn DfsPeerTransport>,
) -> DfsMessageSender {
    Arc::new(move |target_node_id, command, options| {
        let request = dfs_message_rpc_request(command)
            .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
        let call = if let Some((head, descriptor, bytes)) =
            crate::body_rpc::split_dfs_message_request_body(command)
        {
            let request =
                crate::body_rpc::encode_dfs_body_message_rpc_request(&request, head, descriptor)
                    .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
            ObjectRpcSessionCall::new(request, ObjectRpcBodySource::bytes(bytes))
        } else {
            ObjectRpcSessionCall::buffered(request)
        };
        let response = peer_transport.call_object_rpc_session(
            target_node_id,
            call,
            options.with_observe_connectivity(!dfs_message_is_read_only_request(command)),
        )?;
        decode_dfs_message_session_result_sync(response)
    })
}

pub(crate) fn dfs_direct_read_sender_from_peer_transport(
    peer_transport: Arc<dyn DfsPeerTransport>,
) -> DfsDirectReadSender {
    Arc::new(move |target_node_id, request, options| {
        let rpc_request = dfs_direct_read_rpc_request(request)
            .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
        let result = peer_transport.call_object_rpc_session(
            target_node_id,
            ObjectRpcSessionCall::buffered(rpc_request),
            options,
        )?;
        let response = dfs_direct_read_response_from_rpc_session_result(result)
            .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
        collect_dfs_direct_read_body_response_sync(response)
    })
}

fn collect_dfs_direct_read_body_response_sync(
    response: DfsDirectReadClientBodyResponse,
) -> Result<DfsReadResponse, DfsPeerTransportError> {
    let result = match response.result {
        DfsDirectReadClientBodyResult::Found(data) => {
            let bytes = if !data.buffered_bytes.is_empty()
                || (data.content_size_bytes == 0 && data.body.is_empty())
            {
                data.buffered_bytes
            } else {
                collect_dfs_incoming_body_sync(data.body, data.content_size_bytes)?
            };
            if bytes.len() != data.content_size_bytes {
                return Err(DfsPeerTransportError::new(format!(
                    "DFS direct read body has {} bytes, metadata expected {}",
                    bytes.len(),
                    data.content_size_bytes
                )));
            }
            let actual_hash = crate::content_hash(&bytes);
            if actual_hash != data.content_hash {
                return Err(DfsPeerTransportError::new(format!(
                    "DFS direct read body hash mismatch: expected {}, actual {actual_hash}",
                    data.content_hash
                )));
            }
            DfsReadResult::Found(DfsReadData {
                object_id: data.object_id,
                version: data.version,
                content_hash: Some(data.content_hash),
                bytes,
            })
        }
        DfsDirectReadClientBodyResult::NotFound => DfsReadResult::NotFound,
        DfsDirectReadClientBodyResult::Rejected { reason } => DfsReadResult::Rejected { reason },
    };
    Ok(DfsReadResponse {
        request_id: response.request_id,
        object_id: response.object_id,
        result,
    })
}
#[derive(Clone)]
struct DeferredDfsPeerSend {
    sender: DfsMessageSender,
    options: DfsObjectRpcSendOptions,
    operation_deadline: Option<Instant>,
    target_node_id: NodeId,
    routed_message: DfsRoutedMessage,
}

#[derive(Clone)]
pub struct DfsNodeTransport {
    local_node_id: NodeId,
    known_remote_nodes: BTreeSet<NodeId>,
    incoming_messages: VecDeque<DfsRoutedMessage>,
    detached_sends: Arc<std::sync::Mutex<Vec<DeferredDfsPeerSend>>>,
    connect_timeout: Duration,
    sender: Option<DfsMessageSender>,
    async_sender: Option<AsyncDfsMessageSender>,
    direct_read_sender: Option<DfsDirectReadSender>,
    async_direct_read_sender: Option<AsyncDfsDirectReadSender>,
    async_direct_read_body_sender: Option<AsyncDfsDirectReadBodySender>,
    peer_transport: Option<Arc<dyn DfsPeerTransport>>,
    async_peer_transport: Option<Arc<dyn AsyncDfsPeerTransport>>,
    operation_deadline: Option<Instant>,
}

impl std::fmt::Debug for DfsNodeTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DfsNodeTransport")
            .field("local_node_id", &self.local_node_id)
            .field("known_remote_nodes", &self.known_remote_nodes)
            .field("incoming_messages", &self.incoming_messages)
            .field("connect_timeout", &self.connect_timeout)
            .finish_non_exhaustive()
    }
}

impl DfsNodeTransport {
    #[cfg(test)]
    pub(crate) fn new(
        local_node_id: NodeId,
        peer_node_ids: impl IntoIterator<Item = NodeId>,
        sender: DfsMessageSender,
    ) -> Self {
        Self::with_connect_timeout(
            local_node_id,
            peer_node_ids,
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            sender,
        )
    }

    #[cfg(test)]
    pub(crate) fn with_connect_timeout(
        local_node_id: NodeId,
        peer_node_ids: impl IntoIterator<Item = NodeId>,
        connect_timeout: Duration,
        sender: DfsMessageSender,
    ) -> Self {
        Self::with_senders_for_nodes(
            local_node_id,
            peer_node_ids,
            connect_timeout,
            Some(sender),
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    pub(crate) fn with_message_sender_for_nodes(
        local_node_id: NodeId,
        peer_node_ids: impl IntoIterator<Item = NodeId>,
        connect_timeout: Duration,
        sender: DfsMessageSender,
    ) -> Self {
        Self::with_senders_for_nodes(
            local_node_id,
            peer_node_ids,
            connect_timeout,
            Some(sender),
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    #[cfg(test)]
    pub(crate) fn with_connect_timeout_async(
        local_node_id: NodeId,
        peer_node_ids: impl IntoIterator<Item = NodeId>,
        connect_timeout: Duration,
        sender: AsyncDfsMessageSender,
    ) -> Self {
        Self::with_senders_for_nodes(
            local_node_id,
            peer_node_ids,
            connect_timeout,
            None,
            Some(sender),
            None,
            None,
            None,
            None,
            None,
        )
    }

    #[cfg(test)]
    pub(crate) fn with_connect_timeout_and_senders(
        local_node_id: NodeId,
        peer_node_ids: impl IntoIterator<Item = NodeId>,
        connect_timeout: Duration,
        sender: DfsMessageSender,
        async_sender: AsyncDfsMessageSender,
    ) -> Self {
        Self::with_senders_for_nodes(
            local_node_id,
            peer_node_ids,
            connect_timeout,
            Some(sender),
            Some(async_sender),
            None,
            None,
            None,
            None,
            None,
        )
    }

    #[cfg(test)]
    pub(crate) fn new_with_peer_senders(
        local_node_id: NodeId,
        peer_node_ids: impl IntoIterator<Item = NodeId>,
        sender: DfsMessageSender,
        async_sender: AsyncDfsMessageSender,
        direct_read_sender: DfsDirectReadSender,
    ) -> Self {
        Self::with_senders_for_nodes(
            local_node_id,
            peer_node_ids,
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            Some(sender),
            Some(async_sender),
            Some(direct_read_sender),
            None,
            None,
            None,
            None,
        )
    }

    pub fn with_peer_transports_for_nodes(
        local_node_id: NodeId,
        peer_node_ids: impl IntoIterator<Item = NodeId>,
        connect_timeout: Duration,
        peer_transport: Arc<dyn DfsPeerTransport>,
        async_peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    ) -> Self {
        let message_peer_transport = Arc::clone(&peer_transport);
        let direct_read_peer_transport = Arc::clone(&peer_transport);
        let message_async_peer_transport = Arc::clone(&async_peer_transport);
        let async_direct_read_peer_transport = Arc::clone(&async_peer_transport);
        Self::with_senders_for_nodes(
            local_node_id,
            peer_node_ids,
            connect_timeout,
            Some(dfs_message_sender_from_peer_transport(
                message_peer_transport,
            )),
            Some(async_dfs_message_sender_from_peer_transport(
                message_async_peer_transport,
            )),
            Some(dfs_direct_read_sender_from_peer_transport(
                direct_read_peer_transport,
            )),
            None,
            Some(async_dfs_direct_read_body_sender_from_peer_transport(
                Arc::clone(&async_direct_read_peer_transport),
            )),
            Some(peer_transport),
            Some(async_peer_transport),
        )
    }

    #[cfg(test)]
    pub(crate) fn new_with_message_peer_senders(
        local_node_id: NodeId,
        peer_node_ids: impl IntoIterator<Item = NodeId>,
        sender: DfsMessageSender,
        async_sender: AsyncDfsMessageSender,
        direct_read_sender: DfsDirectReadSender,
        async_direct_read_sender: AsyncDfsDirectReadSender,
        async_direct_read_body_sender: AsyncDfsDirectReadBodySender,
    ) -> Self {
        Self::with_senders_for_nodes(
            local_node_id,
            peer_node_ids,
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            Some(sender),
            Some(async_sender),
            Some(direct_read_sender),
            Some(async_direct_read_sender),
            Some(async_direct_read_body_sender),
            None,
            None,
        )
    }

    fn with_senders_for_nodes(
        local_node_id: NodeId,
        peer_node_ids: impl IntoIterator<Item = NodeId>,
        connect_timeout: Duration,
        sender: Option<DfsMessageSender>,
        async_sender: Option<AsyncDfsMessageSender>,
        direct_read_sender: Option<DfsDirectReadSender>,
        async_direct_read_sender: Option<AsyncDfsDirectReadSender>,
        async_direct_read_body_sender: Option<AsyncDfsDirectReadBodySender>,
        peer_transport: Option<Arc<dyn DfsPeerTransport>>,
        async_peer_transport: Option<Arc<dyn AsyncDfsPeerTransport>>,
    ) -> Self {
        let known_remote_nodes = peer_node_ids
            .into_iter()
            .filter(|node_id| node_id != &local_node_id)
            .collect();
        Self {
            local_node_id,
            known_remote_nodes,
            incoming_messages: VecDeque::new(),
            detached_sends: Arc::new(std::sync::Mutex::new(Vec::new())),
            connect_timeout,
            sender,
            async_sender,
            direct_read_sender,
            async_direct_read_sender,
            async_direct_read_body_sender,
            peer_transport,
            async_peer_transport,
            operation_deadline: None,
        }
    }

    pub fn set_operation_deadline(&mut self, deadline: Option<Instant>) -> Option<Instant> {
        std::mem::replace(&mut self.operation_deadline, deadline)
    }

    pub fn operation_deadline(&self) -> Option<Instant> {
        self.operation_deadline
    }

    fn current_send_options(
        &self,
        operation: impl Into<String>,
    ) -> Result<DfsObjectRpcSendOptions, NetworkError> {
        Self::clamp_send_options_to_operation_deadline(
            DfsObjectRpcSendOptions::new(self.connect_timeout),
            self.operation_deadline,
            operation,
        )
    }

    fn clamp_send_options_to_operation_deadline(
        mut options: DfsObjectRpcSendOptions,
        operation_deadline: Option<Instant>,
        operation: impl Into<String>,
    ) -> Result<DfsObjectRpcSendOptions, NetworkError> {
        let Some(deadline) = operation_deadline else {
            return Ok(options);
        };
        let Some(remaining) = deadline
            .checked_duration_since(Instant::now())
            .filter(|duration| !duration.is_zero())
        else {
            return Err(NetworkError::Io {
                operation: operation.into(),
                detail: "DFS operation deadline expired before routed send".to_string(),
            });
        };
        options.overall_timeout = Some(
            options
                .overall_timeout
                .map_or(remaining, |timeout| timeout.min(remaining)),
        );
        Ok(options)
    }

    fn earlier_operation_deadline(
        queued_deadline: Option<Instant>,
        current_deadline: Option<Instant>,
    ) -> Option<Instant> {
        match (queued_deadline, current_deadline) {
            (Some(queued), Some(current)) => Some(queued.min(current)),
            (Some(deadline), None) | (None, Some(deadline)) => Some(deadline),
            (None, None) => None,
        }
    }

    fn replace_peer_nodes(&mut self, peer_node_ids: impl IntoIterator<Item = NodeId>) {
        self.known_remote_nodes = peer_node_ids
            .into_iter()
            .filter(|node_id| node_id != &self.local_node_id)
            .collect();
    }

    fn send_routed_message_to_peer(
        &mut self,
        target_node_id: &NodeId,
        routed_message: DfsRoutedMessage,
        require_local_source: bool,
    ) -> Result<(), NetworkError> {
        let mut pending =
            VecDeque::from([(target_node_id.clone(), routed_message, require_local_source)]);
        while let Some((target_node_id, routed_message, require_local_source)) = pending.pop_front()
        {
            if target_node_id == self.local_node_id {
                self.incoming_messages.push_back(routed_message);
                continue;
            }
            self.validate_routed_message_target(
                &target_node_id,
                &routed_message,
                require_local_source,
            )?;
            if let Some(returned_messages) =
                self.try_send_direct_read_message_to_peer(&target_node_id, &routed_message)?
            {
                for returned_message in returned_messages {
                    pending.push_back((
                        returned_message.target_node_id.clone(),
                        returned_message,
                        false,
                    ));
                }
                continue;
            }
            let Some(sender) = self.sender.as_ref().cloned() else {
                return Err(NetworkError::Io {
                    operation: format!("send DFS message to {target_node_id}"),
                    detail: "DFS peer transport was constructed without a sync sender".to_string(),
                });
            };
            let options =
                self.current_send_options(format!("send DFS message to {target_node_id}"))?;
            let returned_messages =
                send_dfs_routed_message_to_peer(sender, options, target_node_id, routed_message)?;
            for returned_message in returned_messages {
                pending.push_back((
                    returned_message.target_node_id.clone(),
                    returned_message,
                    false,
                ));
            }
        }
        Ok(())
    }

    fn send_routed_message_body_to_peer(
        &mut self,
        target_node_id: &NodeId,
        routed_message: DfsRoutedMessage,
        body: DfsOutboundBody,
        require_local_source: bool,
    ) -> Result<(), NetworkError> {
        if target_node_id == &self.local_node_id {
            let max_bytes = usize::try_from(body.descriptor.total_size_bytes).map_err(|_| {
                NetworkError::FrameTooLarge {
                    actual_bytes: usize::MAX,
                    max_bytes: usize::MAX,
                }
            })?;
            let bytes = collect_dfs_body_source_sync(body.source, max_bytes).map_err(|source| {
                NetworkError::Io {
                    operation: "collect loopback DFS request body".to_string(),
                    detail: source.to_string(),
                }
            })?;
            let message = crate::body_rpc::attach_dfs_routed_message_body(
                routed_message,
                body.descriptor,
                bytes,
            )?;
            self.incoming_messages.push_back(message);
            return Ok(());
        }
        self.validate_routed_message_target(target_node_id, &routed_message, require_local_source)?;
        let Some(peer_transport) = self.peer_transport.as_ref().cloned() else {
            return Err(NetworkError::Io {
                operation: format!("send DFS body to {target_node_id}"),
                detail: "DFS peer transport does not support canonical request bodies".to_string(),
            });
        };
        let command = DfsMessageCommand {
            request_id: format!("dfs_message_{}", routed_message.frame_id),
            routed_message,
        };
        let request = dfs_message_rpc_request(&command)?;
        let request = crate::body_rpc::encode_dfs_body_message_rpc_request(
            &request,
            command.clone(),
            body.descriptor,
        )?;
        let options = self.current_send_options(format!("send DFS body to {target_node_id}"))?;
        let result = peer_transport
            .call_object_rpc_session(
                target_node_id,
                ObjectRpcSessionCall::new(request, body.source),
                options.with_observe_connectivity(!dfs_message_is_read_only_request(&command)),
            )
            .map_err(|source| NetworkError::Io {
                operation: format!("send DFS body to {target_node_id}"),
                detail: source.to_string(),
            })?;
        let returned =
            decode_dfs_message_session_result_sync(result).map_err(|source| NetworkError::Io {
                operation: format!("decode DFS body response from {target_node_id}"),
                detail: source.to_string(),
            })?;
        for message in returned.outbound_messages {
            let target = message.target_node_id.clone();
            self.send_routed_message_to_peer(&target, message, false)?;
        }
        Ok(())
    }

    fn try_send_direct_read_message_to_peer(
        &self,
        target_node_id: &NodeId,
        routed_message: &DfsRoutedMessage,
    ) -> Result<Option<Vec<DfsRoutedMessage>>, NetworkError> {
        let Some(sender) = self.direct_read_sender.as_ref().cloned() else {
            return Ok(None);
        };
        let DfsNetworkMessage::ReadRequest(read_request) = &routed_message.message else {
            return Ok(None);
        };
        let Some(request) = dfs_direct_read_request_from_read_request(read_request) else {
            return Ok(None);
        };
        let options =
            self.current_send_options(format!("direct DFS read from {target_node_id}"))?;
        let response =
            send_dfs_direct_read_to_peer(sender, options, target_node_id.clone(), &request)?;
        Ok(Some(vec![dfs_direct_read_response_routed_message(
            target_node_id,
            routed_message,
            response,
        )]))
    }

    async fn try_send_direct_read_message_to_peer_async(
        &self,
        target_node_id: &NodeId,
        routed_message: &DfsRoutedMessage,
    ) -> Result<Option<Vec<DfsRoutedMessage>>, NetworkError> {
        let DfsNetworkMessage::ReadRequest(read_request) = &routed_message.message else {
            return Ok(None);
        };
        let Some(request) = dfs_direct_read_request_from_read_request(read_request) else {
            return Ok(None);
        };
        let options =
            self.current_send_options(format!("direct DFS read from {target_node_id}"))?;
        let response = if let Some(sender) = self.async_direct_read_body_sender.as_ref().cloned() {
            send_dfs_direct_read_body_to_peer_async(
                sender,
                options,
                target_node_id.clone(),
                &request,
            )
            .await?
        } else if let Some(sender) = self.async_direct_read_sender.as_ref().cloned() {
            send_dfs_direct_read_to_peer_async(sender, options, target_node_id.clone(), &request)
                .await?
        } else {
            return Ok(None);
        };
        Ok(Some(vec![dfs_direct_read_response_routed_message(
            target_node_id,
            routed_message,
            response,
        )]))
    }

    fn send_routed_message_to_peer_detached(
        &mut self,
        target_node_id: &NodeId,
        routed_message: DfsRoutedMessage,
        require_local_source: bool,
    ) -> Result<(), NetworkError> {
        if target_node_id == &self.local_node_id {
            self.incoming_messages.push_back(routed_message);
            return Ok(());
        }
        self.validate_routed_message_target(target_node_id, &routed_message, require_local_source)?;
        let Some(sender) = self.sender.as_ref().cloned() else {
            return Err(NetworkError::Io {
                operation: format!("send DFS message to {target_node_id}"),
                detail: "DFS peer transport was constructed without a sync sender".to_string(),
            });
        };
        let operation_deadline = self.operation_deadline;
        let options = Self::clamp_send_options_to_operation_deadline(
            DfsObjectRpcSendOptions::new(self.connect_timeout),
            operation_deadline,
            format!("send detached DFS message to {target_node_id}"),
        )?;
        self.detached_sends
            .lock()
            .map_err(|_| NetworkError::Io {
                operation: "track detached DFS message send".to_string(),
                detail: "detached send lock is poisoned".to_string(),
            })?
            .push(DeferredDfsPeerSend {
                sender,
                options,
                operation_deadline,
                target_node_id: target_node_id.clone(),
                routed_message,
            });
        Ok(())
    }

    fn flush_detached_peer_sends(&mut self) -> Result<(), NetworkError> {
        let sends = {
            let mut sends = self.detached_sends.lock().map_err(|_| NetworkError::Io {
                operation: "flush detached DFS message sends".to_string(),
                detail: "detached send lock is poisoned".to_string(),
            })?;
            std::mem::take(&mut *sends)
        };
        let mut sends = VecDeque::from(sends);
        while let Some(send) = sends.pop_front() {
            let result = (|| {
                let operation_deadline = Self::earlier_operation_deadline(
                    send.operation_deadline,
                    self.operation_deadline,
                );
                let options = Self::clamp_send_options_to_operation_deadline(
                    send.options,
                    operation_deadline,
                    format!("flush detached DFS message send to {}", send.target_node_id),
                )?;
                let returned_messages = send_dfs_routed_message_to_peer(
                    send.sender,
                    options,
                    send.target_node_id,
                    send.routed_message,
                )?;
                for returned_message in returned_messages {
                    let target_node_id = returned_message.target_node_id.clone();
                    self.send_routed_message_to_peer(&target_node_id, returned_message, false)?;
                }
                Ok(())
            })();
            if let Err(error) = result {
                // The current send may already have reached its target, so it
                // cannot be replayed generically. The remaining suffix has not
                // been attempted, however, and must stay queued so one failed
                // peer cannot discard unrelated durable work. Put that older
                // suffix ahead of sends concurrently enqueued through a clone.
                if !sends.is_empty() {
                    let mut queued = self.detached_sends.lock().map_err(|_| NetworkError::Io {
                        operation: "restore detached DFS message sends".to_string(),
                        detail: "detached send lock is poisoned".to_string(),
                    })?;
                    let mut preserved = sends.into_iter().collect::<Vec<_>>();
                    preserved.append(&mut queued);
                    *queued = preserved;
                }
                return Err(error);
            }
        }
        Ok(())
    }

    fn validate_routed_message_target(
        &self,
        target_node_id: &NodeId,
        routed_message: &DfsRoutedMessage,
        require_local_source: bool,
    ) -> Result<(), NetworkError> {
        if require_local_source && routed_message.source_node_id != self.local_node_id {
            return Err(NetworkError::FrameSourceMismatch {
                local_node_id: self.local_node_id.clone(),
                source_node_id: routed_message.source_node_id.clone(),
            });
        }
        if !require_local_source
            && routed_message.source_node_id != self.local_node_id
            && !self
                .known_remote_nodes
                .contains(&routed_message.source_node_id)
        {
            return Err(NetworkError::UnknownNode {
                node_id: routed_message.source_node_id.clone(),
            });
        }
        if &routed_message.target_node_id != target_node_id {
            return Err(NetworkError::FrameTargetMismatch {
                requested_target_node_id: target_node_id.clone(),
                frame_target_node_id: routed_message.target_node_id.clone(),
            });
        }
        if !self.known_remote_nodes.contains(target_node_id) {
            return Err(NetworkError::UnknownNode {
                node_id: target_node_id.clone(),
            });
        }
        Ok(())
    }

    async fn send_routed_message_to_peer_async(
        &mut self,
        target_node_id: &NodeId,
        routed_message: DfsRoutedMessage,
        require_local_source: bool,
    ) -> Result<(), NetworkError> {
        let mut pending =
            VecDeque::from([(target_node_id.clone(), routed_message, require_local_source)]);
        while let Some((target_node_id, routed_message, require_local_source)) = pending.pop_front()
        {
            if target_node_id == self.local_node_id {
                self.incoming_messages.push_back(routed_message);
                continue;
            }
            self.validate_routed_message_target(
                &target_node_id,
                &routed_message,
                require_local_source,
            )?;
            if let Some(returned_messages) = self
                .try_send_direct_read_message_to_peer_async(&target_node_id, &routed_message)
                .await?
            {
                for returned_message in returned_messages {
                    pending.push_back((
                        returned_message.target_node_id.clone(),
                        returned_message,
                        false,
                    ));
                }
                continue;
            }
            let options =
                self.current_send_options(format!("send DFS message to {target_node_id}"))?;
            let Some(sender) = self.async_sender.as_ref().cloned() else {
                return Err(NetworkError::Io {
                    operation: format!("send DFS message to {target_node_id}"),
                    detail: "DFS peer transport was constructed without an async sender"
                        .to_string(),
                });
            };
            let returned_messages = send_dfs_routed_message_to_peer_async(
                sender,
                options,
                target_node_id,
                routed_message,
            )
            .await?;
            for returned_message in returned_messages {
                pending.push_back((
                    returned_message.target_node_id.clone(),
                    returned_message,
                    false,
                ));
            }
        }
        Ok(())
    }

    async fn send_routed_message_body_to_peer_async(
        &mut self,
        target_node_id: &NodeId,
        routed_message: DfsRoutedMessage,
        body: DfsOutboundBody,
        require_local_source: bool,
    ) -> Result<(), NetworkError> {
        if target_node_id == &self.local_node_id {
            let max_bytes = usize::try_from(body.descriptor.total_size_bytes).map_err(|_| {
                NetworkError::FrameTooLarge {
                    actual_bytes: usize::MAX,
                    max_bytes: usize::MAX,
                }
            })?;
            let bytes = body.source.collect_to_vec(max_bytes).await?;
            let message = crate::body_rpc::attach_dfs_routed_message_body(
                routed_message,
                body.descriptor,
                bytes,
            )?;
            self.incoming_messages.push_back(message);
            return Ok(());
        }
        self.validate_routed_message_target(target_node_id, &routed_message, require_local_source)?;
        let Some(peer_transport) = self.async_peer_transport.as_ref().cloned() else {
            return Err(NetworkError::Io {
                operation: format!("send DFS body to {target_node_id}"),
                detail: "async DFS peer transport does not support canonical request bodies"
                    .to_string(),
            });
        };
        let command = DfsMessageCommand {
            request_id: format!("dfs_message_{}", routed_message.frame_id),
            routed_message,
        };
        let request = dfs_message_rpc_request(&command)?;
        let request = crate::body_rpc::encode_dfs_body_message_rpc_request(
            &request,
            command.clone(),
            body.descriptor,
        )?;
        let options = self.current_send_options(format!("send DFS body to {target_node_id}"))?;
        let result = peer_transport
            .call_object_rpc_session_async(
                target_node_id,
                ObjectRpcSessionCall::new(request, body.source),
                options.with_observe_connectivity(!dfs_message_is_read_only_request(&command)),
            )
            .await
            .map_err(|source| NetworkError::Io {
                operation: format!("send DFS body to {target_node_id}"),
                detail: source.to_string(),
            })?;
        let returned = decode_dfs_message_session_result(result)
            .await
            .map_err(|source| NetworkError::Io {
                operation: format!("decode DFS body response from {target_node_id}"),
                detail: source.to_string(),
            })?;
        for message in returned.outbound_messages {
            let target = message.target_node_id.clone();
            self.send_routed_message_to_peer_async(&target, message, false)
                .await?;
        }
        Ok(())
    }
}

fn dfs_direct_read_response_routed_message(
    target_node_id: &NodeId,
    routed_message: &DfsRoutedMessage,
    response: DfsReadResponse,
) -> DfsRoutedMessage {
    DfsRoutedMessage {
        source_node_id: target_node_id.clone(),
        target_node_id: routed_message.source_node_id.clone(),
        frame_id: routed_message.frame_id,
        message_id: routed_message.message_id,
        message: DfsNetworkMessage::ReadResponse(response),
    }
}

fn send_dfs_routed_message_to_peer(
    sender: DfsMessageSender,
    options: DfsObjectRpcSendOptions,
    target_node_id: NodeId,
    routed_message: DfsRoutedMessage,
) -> Result<Vec<DfsRoutedMessage>, NetworkError> {
    let request_id = format!("dfs_message_{}", routed_message.frame_id);
    let source_node_id = routed_message.source_node_id.clone();
    let command = DfsMessageCommand {
        request_id,
        routed_message,
    };
    let trace_enabled = dfs_perf_trace_enabled();
    let request_body_bytes = if trace_enabled {
        Some(dfs_message_request_body_bytes(&command)?)
    } else {
        None
    };
    let send_started = Instant::now();
    match sender(&target_node_id, &command, options) {
        Ok(response) => {
            let outbound_messages = response.outbound_messages;
            if trace_enabled {
                let outbound_body_bytes =
                    dfs_message_response_body_bytes(&command.request_id, &outbound_messages)?;
                dfs_perf_trace(format_args!(
                    "dfs-message-send local={} target={} request={} request_body_bytes={} outbound_messages={} outbound_body_bytes={} elapsed_ms={}",
                    source_node_id,
                    target_node_id,
                    command.request_id,
                    request_body_bytes.unwrap_or_default(),
                    outbound_messages.len(),
                    outbound_body_bytes,
                    send_started.elapsed().as_millis()
                ));
            }
            Ok(outbound_messages)
        }
        Err(error) => {
            if trace_enabled {
                dfs_perf_trace(format_args!(
                    "dfs-message-send-error local={} target={} request={} request_body_bytes={} elapsed_ms={} error={}",
                    source_node_id,
                    target_node_id,
                    command.request_id,
                    request_body_bytes.unwrap_or_default(),
                    send_started.elapsed().as_millis(),
                    error
                ));
            }
            Err(NetworkError::Io {
                operation: format!("send DFS message to {target_node_id}"),
                detail: error.to_string(),
            })
        }
    }
}

fn send_dfs_direct_read_to_peer(
    sender: DfsDirectReadSender,
    options: DfsObjectRpcSendOptions,
    target_node_id: NodeId,
    request: &DfsDirectReadRequest,
) -> Result<DfsReadResponse, NetworkError> {
    let send_started = Instant::now();
    match sender(&target_node_id, request, options) {
        Ok(response) => {
            dfs_perf_trace(format_args!(
                "direct-read-send local_request={} target={} request={} bytes={} elapsed_ms={}",
                request.object_id,
                target_node_id,
                request.request_id,
                request.content_size_bytes,
                send_started.elapsed().as_millis()
            ));
            Ok(response)
        }
        Err(error) => {
            dfs_perf_trace(format_args!(
                "direct-read-send-error target={} request={} bytes={} elapsed_ms={} error={}",
                target_node_id,
                request.request_id,
                request.content_size_bytes,
                send_started.elapsed().as_millis(),
                error
            ));
            Err(NetworkError::Io {
                operation: format!("direct DFS read from {target_node_id}"),
                detail: error.to_string(),
            })
        }
    }
}

async fn send_dfs_direct_read_to_peer_async(
    sender: AsyncDfsDirectReadSender,
    options: DfsObjectRpcSendOptions,
    target_node_id: NodeId,
    request: &DfsDirectReadRequest,
) -> Result<DfsReadResponse, NetworkError> {
    let send_started = Instant::now();
    match sender(&target_node_id, request, options).await {
        Ok(response) => {
            dfs_perf_trace(format_args!(
                "direct-read-send-async local_request={} target={} request={} bytes={} elapsed_ms={}",
                request.object_id,
                target_node_id,
                request.request_id,
                request.content_size_bytes,
                send_started.elapsed().as_millis()
            ));
            Ok(response)
        }
        Err(error) => {
            dfs_perf_trace(format_args!(
                "direct-read-send-async-error target={} request={} bytes={} elapsed_ms={} error={}",
                target_node_id,
                request.request_id,
                request.content_size_bytes,
                send_started.elapsed().as_millis(),
                error
            ));
            Err(NetworkError::Io {
                operation: format!("direct DFS read from {target_node_id}"),
                detail: error.to_string(),
            })
        }
    }
}

async fn send_dfs_direct_read_body_to_peer_async(
    sender: AsyncDfsDirectReadBodySender,
    options: DfsObjectRpcSendOptions,
    target_node_id: NodeId,
    request: &DfsDirectReadRequest,
) -> Result<DfsReadResponse, NetworkError> {
    let send_started = Instant::now();
    match sender(&target_node_id, request, options).await {
        Ok(response) => {
            let response = response.collect_to_read_response().await?;
            dfs_perf_trace(format_args!(
                "direct-read-body-send-async local_request={} target={} request={} bytes={} elapsed_ms={}",
                request.object_id,
                target_node_id,
                request.request_id,
                request.content_size_bytes,
                send_started.elapsed().as_millis()
            ));
            Ok(response)
        }
        Err(error) => {
            dfs_perf_trace(format_args!(
                "direct-read-body-send-async-error target={} request={} bytes={} elapsed_ms={} error={}",
                target_node_id,
                request.request_id,
                request.content_size_bytes,
                send_started.elapsed().as_millis(),
                error
            ));
            Err(NetworkError::Io {
                operation: format!("direct DFS read body from {target_node_id}"),
                detail: error.to_string(),
            })
        }
    }
}

async fn send_dfs_routed_message_to_peer_async(
    sender: AsyncDfsMessageSender,
    options: DfsObjectRpcSendOptions,
    target_node_id: NodeId,
    routed_message: DfsRoutedMessage,
) -> Result<Vec<DfsRoutedMessage>, NetworkError> {
    let request_id = format!("dfs_message_{}", routed_message.frame_id);
    let source_node_id = routed_message.source_node_id.clone();
    let command = DfsMessageCommand {
        request_id,
        routed_message,
    };
    let trace_enabled = dfs_perf_trace_enabled();
    let request_body_bytes = if trace_enabled {
        Some(dfs_message_request_body_bytes(&command)?)
    } else {
        None
    };
    let send_started = Instant::now();
    match sender(&target_node_id, &command, options).await {
        Ok(response) => {
            let outbound_messages = response.outbound_messages;
            if trace_enabled {
                let outbound_body_bytes =
                    dfs_message_response_body_bytes(&command.request_id, &outbound_messages)?;
                dfs_perf_trace(format_args!(
                    "dfs-message-send-async local={} target={} request={} request_body_bytes={} outbound_messages={} outbound_body_bytes={} elapsed_ms={}",
                    source_node_id,
                    target_node_id,
                    command.request_id,
                    request_body_bytes.unwrap_or_default(),
                    outbound_messages.len(),
                    outbound_body_bytes,
                    send_started.elapsed().as_millis()
                ));
            }
            Ok(outbound_messages)
        }
        Err(error) => {
            if trace_enabled {
                dfs_perf_trace(format_args!(
                    "dfs-message-send-async-error local={} target={} request={} request_body_bytes={} elapsed_ms={} error={}",
                    source_node_id,
                    target_node_id,
                    command.request_id,
                    request_body_bytes.unwrap_or_default(),
                    send_started.elapsed().as_millis(),
                    error
                ));
            }
            Err(NetworkError::Io {
                operation: format!("send DFS message to {target_node_id}"),
                detail: error.to_string(),
            })
        }
    }
}

fn dfs_message_request_body_bytes(command: &DfsMessageCommand) -> Result<usize, NetworkError> {
    SinomoObjectCodec::default()
        .encode(command)
        .map(|body| body.len())
}

#[derive(Serialize)]
struct DfsMessageResponseTraceBody<'a> {
    request_id: &'a str,
    outbound_messages: &'a [DfsRoutedMessage],
}

fn dfs_message_response_body_bytes(
    request_id: &str,
    outbound_messages: &[DfsRoutedMessage],
) -> Result<usize, NetworkError> {
    let body = DfsMessageResponseTraceBody {
        request_id,
        outbound_messages,
    };
    SinomoObjectCodec::default()
        .encode(&body)
        .map(|body| body.len())
}

impl DfsTransportIdentity for DfsNodeTransport {
    fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }
}

impl AsyncDfsMessageTransport for DfsNodeTransport {
    fn local_node_id(&self) -> &NodeId {
        DfsTransportIdentity::local_node_id(self)
    }

    fn send_routed_message_async<'a>(
        &'a mut self,
        target_node_id: &'a NodeId,
        message: DfsRoutedMessage,
    ) -> DfsMessageTransportFuture<'a, Result<(), NetworkError>> {
        Box::pin(async move {
            self.send_routed_message_to_peer_async(target_node_id, message, true)
                .await
        })
    }

    fn send_routed_message_with_body_async<'a>(
        &'a mut self,
        target_node_id: &'a NodeId,
        message: DfsRoutedMessage,
        body: DfsOutboundBody,
    ) -> DfsMessageTransportFuture<'a, Result<(), NetworkError>> {
        Box::pin(async move {
            self.send_routed_message_body_to_peer_async(target_node_id, message, body, true)
                .await
        })
    }

    fn drain_incoming_messages_async<'a>(
        &'a mut self,
    ) -> DfsMessageTransportFuture<'a, Result<Vec<DfsRoutedMessage>, NetworkError>> {
        Box::pin(async move { Ok(self.incoming_messages.drain(..).collect()) })
    }

    fn pending_incoming_count_async<'a>(
        &'a mut self,
    ) -> DfsMessageTransportFuture<'a, Result<Option<usize>, NetworkError>> {
        Box::pin(async move { Ok(Some(self.incoming_messages.len())) })
    }
}

impl DfsMessageTransport for DfsNodeTransport {
    fn send_routed_message(
        &mut self,
        target_node_id: &NodeId,
        message: DfsRoutedMessage,
    ) -> Result<(), NetworkError> {
        self.send_routed_message_to_peer(target_node_id, message, true)
    }

    fn send_routed_message_with_body(
        &mut self,
        target_node_id: &NodeId,
        message: DfsRoutedMessage,
        body: DfsOutboundBody,
    ) -> Result<(), NetworkError> {
        self.send_routed_message_body_to_peer(target_node_id, message, body, true)
    }

    fn send_detached_routed_message(
        &mut self,
        target_node_id: &NodeId,
        message: DfsRoutedMessage,
    ) -> Result<(), NetworkError> {
        self.send_routed_message_to_peer_detached(target_node_id, message, true)
    }

    fn drain_incoming_messages(&mut self) -> Result<Vec<DfsRoutedMessage>, NetworkError> {
        Ok(self.incoming_messages.drain(..).collect())
    }

    fn flush_detached_sends(&mut self) -> Result<(), NetworkError> {
        self.flush_detached_peer_sends()
    }

    fn can_send_synchronously(&self) -> bool {
        self.sender.is_some()
    }

    fn pending_incoming_count(&self) -> Result<Option<usize>, NetworkError> {
        Ok(Some(self.incoming_messages.len()))
    }

    fn set_operation_deadline(&mut self, deadline: Option<Instant>) -> Option<Instant> {
        DfsNodeTransport::set_operation_deadline(self, deadline)
    }

    fn operation_deadline(&self) -> Option<Instant> {
        DfsNodeTransport::operation_deadline(self)
    }
}

impl NetworkedDfsService<DfsNodeTransport> {
    pub fn refresh_peer_targets(&mut self, peer_node_ids: Vec<NodeId>) {
        self.transport.replace_peer_nodes(peer_node_ids);
    }

    pub fn set_transport_operation_deadline(
        &mut self,
        deadline: Option<Instant>,
    ) -> Option<Instant> {
        self.transport.set_operation_deadline(deadline)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DfsDistributedWriteCommand, DfsNamespaceCommand, DfsNamespaceOperation, DfsObjectId,
        DfsReadResult, DfsReplicationPolicy, DfsVersion, sifs_file_system_id,
    };
    use sinomo_core::DfsWriteRequest;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    fn node_id(name: &str) -> NodeId {
        NodeId::from_legacy_named(name).expect("node id")
    }

    fn test_message(source: &NodeId, target: &NodeId, frame_id: u64) -> DfsRoutedMessage {
        DfsRoutedMessage {
            source_node_id: source.clone(),
            target_node_id: target.clone(),
            frame_id,
            message_id: frame_id,
            message: DfsNetworkMessage::ReadResponse(DfsReadResponse {
                request_id: format!("read_response_{frame_id}"),
                object_id: DfsObjectId::from_legacy_named(format!("object_{frame_id}"))
                    .expect("object id"),
                result: DfsReadResult::NotFound,
            }),
        }
    }

    fn test_namespace_message(source: &NodeId, target: &NodeId, frame_id: u64) -> DfsRoutedMessage {
        DfsRoutedMessage {
            source_node_id: source.clone(),
            target_node_id: target.clone(),
            frame_id,
            message_id: frame_id,
            message: DfsNetworkMessage::NamespaceRequest(DfsNamespaceCommand {
                request_id: format!("namespace_request_{frame_id}"),
                file_system_id: sifs_file_system_id(),
                requesting_node_id: source.clone(),
                include_metadata_journal_batches: true,
                metadata_journal_after_sequences: Vec::new(),
                operation: DfsNamespaceOperation::Mkdir {
                    path: format!("/namespace-{frame_id}"),
                },
            }),
        }
    }

    fn test_distributed_write_message(
        source: &NodeId,
        target: &NodeId,
        frame_id: u64,
    ) -> DfsRoutedMessage {
        let object_id =
            DfsObjectId::from_legacy_named(format!("write_object_{frame_id}")).expect("object id");
        DfsRoutedMessage {
            source_node_id: source.clone(),
            target_node_id: target.clone(),
            frame_id,
            message_id: frame_id,
            message: DfsNetworkMessage::DistributedWriteRequest(DfsDistributedWriteCommand {
                request_id: format!("write_request_{frame_id}"),
                request: DfsWriteRequest {
                    file_system_id: sifs_file_system_id(),
                    object_id,
                    target_version: DfsVersion::from_legacy_named(format!("version_{frame_id}"))
                        .expect("version"),
                    requesting_node_id: source.clone(),
                },
                expected_base_version: None,
                metadata_journal_after_sequence: None,
                bytes: Vec::new(),
                content_hash: Some(format!("blake3:write-{frame_id}")),
                content_size_bytes: Some(0),
                desired_replica_nodes: vec![source.clone()],
                policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
                object_metadata: None,
                touch_parent_on_create: true,
                foreground_replica_repair: false,
            }),
        }
    }

    #[test]
    fn default_send_uses_network_timeout_without_dfs_overall_deadline() {
        let local = node_id("node_default_timeout_local");
        let remote = node_id("node_default_timeout_remote");
        let observed_options = Arc::new(Mutex::new(Vec::new()));
        let sender_options = Arc::clone(&observed_options);
        let sender: DfsMessageSender = Arc::new(move |_target, command, options| {
            sender_options.lock().expect("options").push(options);
            Ok(DfsMessageResponse {
                request_id: command.request_id.clone(),
                outbound_messages: Vec::new(),
            })
        });
        let mut transport = DfsNodeTransport::new(local.clone(), vec![remote.clone()], sender);

        transport
            .send_routed_message(&remote, test_message(&local, &remote, 1))
            .expect("send");

        assert_eq!(
            observed_options.lock().expect("options").as_slice(),
            &[DfsObjectRpcSendOptions::default()]
        );
        assert_eq!(
            DfsObjectRpcSendOptions::default().connect_timeout,
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT
        );
        assert_eq!(DfsObjectRpcSendOptions::default().overall_timeout, None);
    }

    #[test]
    fn durable_mutation_sends_keep_default_network_health_policy() {
        let local = node_id("node_durable_timeout_local");
        let remote = node_id("node_durable_timeout_remote");
        let observed_options = Arc::new(Mutex::new(Vec::new()));
        let sender_options = Arc::clone(&observed_options);
        let sender: DfsMessageSender = Arc::new(move |_target, command, options| {
            sender_options.lock().expect("options").push(options);
            Ok(DfsMessageResponse {
                request_id: command.request_id.clone(),
                outbound_messages: Vec::new(),
            })
        });
        let mut transport = DfsNodeTransport::new(local.clone(), vec![remote.clone()], sender);

        transport
            .send_routed_message(&remote, test_namespace_message(&local, &remote, 1))
            .expect("namespace send");
        transport
            .send_routed_message(&remote, test_distributed_write_message(&local, &remote, 2))
            .expect("distributed write send");
        transport
            .send_routed_message(&remote, test_message(&local, &remote, 3))
            .expect("ordinary send");

        let observed = observed_options.lock().expect("options");
        assert_eq!(
            observed.as_slice(),
            &[DfsObjectRpcSendOptions::default(); 3],
            "canonical sessions keep transport health independently of application silence"
        );
    }

    #[test]
    fn operation_deadline_bounds_consecutive_sync_sends() {
        let local = node_id("node_deadline_local");
        let remote = node_id("node_deadline_remote");
        let observed_options = Arc::new(Mutex::new(Vec::new()));
        let sender_options = Arc::clone(&observed_options);
        let sender: DfsMessageSender = Arc::new(move |_target, command, options| {
            sender_options.lock().expect("options").push(options);
            std::thread::sleep(Duration::from_millis(20));
            Ok(DfsMessageResponse {
                request_id: command.request_id.clone(),
                outbound_messages: Vec::new(),
            })
        });
        let mut transport = DfsNodeTransport::with_connect_timeout(
            local.clone(),
            vec![remote.clone()],
            Duration::from_secs(60),
            sender,
        );
        transport.set_operation_deadline(Some(Instant::now() + Duration::from_millis(100)));

        transport
            .send_routed_message(&remote, test_message(&local, &remote, 1))
            .expect("first send");
        transport
            .send_routed_message(&remote, test_message(&local, &remote, 2))
            .expect("second send");

        let observed = observed_options.lock().expect("options").clone();
        assert_eq!(observed.len(), 2);
        assert_eq!(observed[0].connect_timeout, Duration::from_secs(60));
        assert_eq!(observed[1].connect_timeout, Duration::from_secs(60));
        let first_overall = observed[0].overall_timeout.expect("first operation budget");
        let second_overall = observed[1]
            .overall_timeout
            .expect("second operation budget");
        assert!(
            first_overall <= Duration::from_millis(100),
            "first send should be capped by operation deadline, got {:?}",
            observed[0]
        );
        assert!(
            second_overall < first_overall,
            "second send should receive only remaining operation budget: {observed:?}"
        );
    }

    #[test]
    fn detached_send_recomputes_remaining_operation_budget_when_flushed() {
        let local = node_id("node_detached_deadline_local");
        let remote = node_id("node_detached_deadline_remote");
        let observed_options = Arc::new(Mutex::new(Vec::new()));
        let sender_options = Arc::clone(&observed_options);
        let sender: DfsMessageSender = Arc::new(move |_target, command, options| {
            sender_options
                .lock()
                .expect("options")
                .push((options, Instant::now()));
            Ok(DfsMessageResponse {
                request_id: command.request_id.clone(),
                outbound_messages: Vec::new(),
            })
        });
        let mut transport = DfsNodeTransport::with_connect_timeout(
            local.clone(),
            vec![remote.clone()],
            Duration::from_secs(60),
            sender,
        );
        let deadline = Instant::now() + Duration::from_secs(1);
        transport.set_operation_deadline(Some(deadline));

        transport
            .send_detached_routed_message(&remote, test_namespace_message(&local, &remote, 1))
            .expect("queue detached send");
        std::thread::sleep(Duration::from_millis(100));
        transport
            .flush_detached_sends()
            .expect("flush detached send");

        let observed = observed_options.lock().expect("options");
        let (options, dispatched_at) = observed.first().expect("dispatched send");
        let overall_timeout = options.overall_timeout.expect("operation budget");
        let remaining_at_dispatch = deadline.saturating_duration_since(*dispatched_at);
        assert!(
            overall_timeout <= remaining_at_dispatch.saturating_add(Duration::from_millis(25)),
            "detached send restarted its queued timeout budget: {options:?}"
        );
        assert!(
            overall_timeout < Duration::from_millis(950),
            "detached send did not account for time spent queued: {options:?}"
        );
        assert_eq!(
            options.connect_timeout,
            Duration::from_secs(60),
            "detached dispatch must preserve the transport's explicit connect timeout"
        );
    }

    #[test]
    fn detached_send_keeps_enqueued_absolute_deadline_when_transport_deadline_changes() {
        let local = node_id("node_detached_expired_deadline_local");
        let remote = node_id("node_detached_expired_deadline_remote");
        let calls = Arc::new(AtomicUsize::new(0));
        let sender_calls = Arc::clone(&calls);
        let sender: DfsMessageSender = Arc::new(move |_target, command, _options| {
            sender_calls.fetch_add(1, Ordering::SeqCst);
            Ok(DfsMessageResponse {
                request_id: command.request_id.clone(),
                outbound_messages: Vec::new(),
            })
        });
        let mut transport = DfsNodeTransport::with_connect_timeout(
            local.clone(),
            vec![remote.clone()],
            Duration::from_secs(60),
            sender,
        );
        transport.set_operation_deadline(Some(Instant::now() + Duration::from_millis(50)));
        transport
            .send_detached_routed_message(&remote, test_message(&local, &remote, 1))
            .expect("queue detached send");

        transport.set_operation_deadline(Some(Instant::now() + Duration::from_secs(5)));
        std::thread::sleep(Duration::from_millis(80));
        let error = transport
            .flush_detached_sends()
            .expect_err("queued operation deadline should expire before dispatch");

        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(
            error
                .to_string()
                .contains("DFS operation deadline expired before routed send"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn detached_send_clamps_unbounded_queue_to_current_flush_deadline() {
        let local = node_id("node_detached_flush_deadline_local");
        let remote = node_id("node_detached_flush_deadline_remote");
        let calls = Arc::new(AtomicUsize::new(0));
        let sender_calls = Arc::clone(&calls);
        let sender: DfsMessageSender = Arc::new(move |_target, command, _options| {
            sender_calls.fetch_add(1, Ordering::SeqCst);
            Ok(DfsMessageResponse {
                request_id: command.request_id.clone(),
                outbound_messages: Vec::new(),
            })
        });
        let mut transport = DfsNodeTransport::with_connect_timeout(
            local.clone(),
            vec![remote.clone()],
            Duration::from_secs(60),
            sender,
        );
        transport
            .send_detached_routed_message(&remote, test_message(&local, &remote, 1))
            .expect("queue unbounded detached send");

        transport.set_operation_deadline(Some(Instant::now() + Duration::from_millis(50)));
        std::thread::sleep(Duration::from_millis(80));
        let error = transport
            .flush_detached_sends()
            .expect_err("current flush deadline should bound an older unbounded send");

        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(
            error
                .to_string()
                .contains("DFS operation deadline expired before routed send"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn detached_send_failure_preserves_unattempted_queue_suffix() {
        let local = node_id("node_detached_suffix_local");
        let first_remote = node_id("node_detached_suffix_first");
        let second_remote = node_id("node_detached_suffix_second");
        let calls = Arc::new(Mutex::new(Vec::new()));
        let sender_calls = Arc::clone(&calls);
        let sender: DfsMessageSender = Arc::new(move |target, command, _options| {
            let mut calls = sender_calls.lock().expect("calls");
            calls.push(target.clone());
            if calls.len() == 1 {
                return Err(DfsPeerTransportError::new("first detached send failed"));
            }
            Ok(DfsMessageResponse {
                request_id: command.request_id.clone(),
                outbound_messages: Vec::new(),
            })
        });
        let mut transport = DfsNodeTransport::with_connect_timeout(
            local.clone(),
            vec![first_remote.clone(), second_remote.clone()],
            Duration::from_secs(1),
            sender,
        );
        transport
            .send_detached_routed_message(&first_remote, test_message(&local, &first_remote, 1))
            .expect("queue first detached send");
        transport
            .send_detached_routed_message(&second_remote, test_message(&local, &second_remote, 2))
            .expect("queue second detached send");

        transport
            .flush_detached_sends()
            .expect_err("first detached send should fail");
        assert_eq!(
            transport
                .detached_sends
                .lock()
                .expect("queued suffix")
                .len(),
            1,
            "the unattempted send must remain queued"
        );

        transport
            .flush_detached_sends()
            .expect("unattempted suffix should flush on retry");
        assert_eq!(
            calls.lock().expect("calls").as_slice(),
            &[first_remote, second_remote]
        );
        assert!(
            transport
                .detached_sends
                .lock()
                .expect("empty detached queue")
                .is_empty()
        );
    }

    #[test]
    fn operation_deadline_expiry_rejects_before_sender_runs() {
        let local = node_id("node_expired_deadline_local");
        let remote = node_id("node_expired_deadline_remote");
        let calls = Arc::new(AtomicUsize::new(0));
        let sender_calls = Arc::clone(&calls);
        let sender: DfsMessageSender = Arc::new(move |_target, command, _connect_timeout| {
            sender_calls.fetch_add(1, Ordering::SeqCst);
            Ok(DfsMessageResponse {
                request_id: command.request_id.clone(),
                outbound_messages: Vec::new(),
            })
        });
        let mut transport = DfsNodeTransport::with_connect_timeout(
            local.clone(),
            vec![remote.clone()],
            Duration::from_secs(60),
            sender,
        );
        transport.set_operation_deadline(Some(Instant::now() + Duration::from_millis(1)));
        std::thread::sleep(Duration::from_millis(5));

        let error = transport
            .send_routed_message(&remote, test_message(&local, &remote, 1))
            .expect_err("expired deadline should fail before send");

        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(
            error
                .to_string()
                .contains("DFS operation deadline expired before routed send"),
            "unexpected error: {error}"
        );
    }
}

#[derive(Debug)]
pub struct DfsNodeServedMessageTransport {
    local_node_id: NodeId,
    return_node_id: Option<NodeId>,
    inbound_messages: VecDeque<DfsRoutedMessage>,
    outbound_messages: Vec<DfsRoutedMessage>,
    outbound_body: Option<DfsOutboundBody>,
    node_transport: DfsNodeTransport,
}

impl DfsNodeServedMessageTransport {
    pub fn new_with_peer_transport_for_nodes(
        local_node_id: NodeId,
        peer_node_ids: impl IntoIterator<Item = NodeId>,
        peer_transport: Arc<dyn DfsPeerTransport>,
        connect_timeout: Duration,
    ) -> Self {
        Self::new_with_message_sender_for_nodes(
            local_node_id,
            peer_node_ids,
            dfs_message_sender_from_peer_transport(peer_transport),
            connect_timeout,
        )
    }

    #[cfg(test)]
    pub(crate) fn new_with_message_sender(
        local_node_id: NodeId,
        peer_node_ids: impl IntoIterator<Item = NodeId>,
        sender: DfsMessageSender,
        connect_timeout: Duration,
    ) -> Self {
        Self::new_with_message_sender_for_nodes(
            local_node_id,
            peer_node_ids,
            sender,
            connect_timeout,
        )
    }

    fn new_with_message_sender_for_nodes(
        local_node_id: NodeId,
        peer_node_ids: impl IntoIterator<Item = NodeId>,
        sender: DfsMessageSender,
        connect_timeout: Duration,
    ) -> Self {
        let node_transport = DfsNodeTransport::with_senders_for_nodes(
            local_node_id.clone(),
            peer_node_ids,
            connect_timeout,
            Some(sender),
            None,
            None,
            None,
            None,
            None,
            None,
        );
        Self {
            local_node_id,
            return_node_id: None,
            inbound_messages: VecDeque::new(),
            outbound_messages: Vec::new(),
            outbound_body: None,
            node_transport,
        }
    }

    pub fn begin_message(&mut self, message: &DfsRoutedMessage) -> Result<(), NetworkError> {
        if message.target_node_id != self.local_node_id {
            return Err(NetworkError::FrameTargetMismatch {
                requested_target_node_id: self.local_node_id.clone(),
                frame_target_node_id: message.target_node_id.clone(),
            });
        }
        self.return_node_id = Some(message.source_node_id.clone());
        Ok(())
    }

    pub fn finish_message(&mut self) -> Vec<DfsRoutedMessage> {
        self.finish_message_with_body().0
    }

    pub fn finish_message_with_body(&mut self) -> (Vec<DfsRoutedMessage>, Option<DfsOutboundBody>) {
        self.return_node_id = None;
        self.inbound_messages.clear();
        self.node_transport.incoming_messages.clear();
        (
            std::mem::take(&mut self.outbound_messages),
            self.outbound_body.take(),
        )
    }

    pub fn replace_peer_nodes(&mut self, peer_node_ids: Vec<NodeId>) {
        self.node_transport.replace_peer_nodes(peer_node_ids);
    }

    fn drain_queued_messages(&mut self) -> Vec<DfsRoutedMessage> {
        self.inbound_messages.drain(..).collect()
    }
}

impl DfsTransportIdentity for DfsNodeServedMessageTransport {
    fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }
}

impl DfsMessageTransport for DfsNodeServedMessageTransport {
    fn send_routed_message(
        &mut self,
        target_node_id: &NodeId,
        message: DfsRoutedMessage,
    ) -> Result<(), NetworkError> {
        if &message.target_node_id != target_node_id {
            return Err(NetworkError::FrameTargetMismatch {
                requested_target_node_id: target_node_id.clone(),
                frame_target_node_id: message.target_node_id.clone(),
            });
        }
        if target_node_id == &self.local_node_id {
            self.inbound_messages.push_back(message);
            return Ok(());
        }
        if self
            .return_node_id
            .as_ref()
            .is_some_and(|return_node_id| target_node_id == return_node_id)
            && dfs_network_message_can_return_to_served_caller(&message.message)
        {
            self.outbound_messages.push(message);
            return Ok(());
        }
        self.node_transport
            .send_routed_message_to_peer(target_node_id, message, true)
    }

    fn send_routed_message_with_body(
        &mut self,
        target_node_id: &NodeId,
        message: DfsRoutedMessage,
        body: DfsOutboundBody,
    ) -> Result<(), NetworkError> {
        if &message.target_node_id != target_node_id {
            return Err(NetworkError::FrameTargetMismatch {
                requested_target_node_id: target_node_id.clone(),
                frame_target_node_id: message.target_node_id.clone(),
            });
        }
        if self
            .return_node_id
            .as_ref()
            .is_some_and(|return_node_id| target_node_id == return_node_id)
            && dfs_network_message_can_return_to_served_caller(&message.message)
        {
            if self.outbound_body.is_some() {
                return Err(NetworkError::Serialization {
                    detail: "DFS served response attempted to return more than one body"
                        .to_string(),
                });
            }
            self.outbound_messages.push(message);
            self.outbound_body = Some(body);
            return Ok(());
        }
        self.node_transport
            .send_routed_message_body_to_peer(target_node_id, message, body, true)
    }

    fn drain_incoming_messages(&mut self) -> Result<Vec<DfsRoutedMessage>, NetworkError> {
        let mut messages = self.inbound_messages.drain(..).collect::<Vec<_>>();
        messages.extend(self.node_transport.drain_incoming_messages()?);
        Ok(messages)
    }

    fn pending_incoming_count(&self) -> Result<Option<usize>, NetworkError> {
        let nested = self
            .node_transport
            .pending_incoming_count()?
            .unwrap_or_default();
        Ok(Some(self.inbound_messages.len().saturating_add(nested)))
    }

    fn can_return_committed_foreground_response_before_replica_repair(&self) -> bool {
        true
    }
}

fn dfs_network_message_is_inline_served_response(message: &DfsNetworkMessage) -> bool {
    matches!(
        message,
        DfsNetworkMessage::ReadResponse(_)
            | DfsNetworkMessage::BulkReadResponse(_)
            | DfsNetworkMessage::WriteResponse(_)
            | DfsNetworkMessage::DistributedWriteResponse(_)
            | DfsNetworkMessage::NamespaceResponse(_)
            | DfsNetworkMessage::StageBlobResponse(_)
            | DfsNetworkMessage::CommitVersionResponse(_)
            | DfsNetworkMessage::RepairReplicaResponse(_)
            | DfsNetworkMessage::FetchBlobResponse(_)
            | DfsNetworkMessage::MetadataJournalBatchResponse(_)
            | DfsNetworkMessage::MetadataSnapshotInstallResponse(_)
    )
}

fn dfs_network_message_can_return_to_served_caller(message: &DfsNetworkMessage) -> bool {
    dfs_network_message_is_inline_served_response(message)
        || matches!(message, DfsNetworkMessage::FetchBlob(_))
}

impl NetworkedDfsService<DfsNodeServedMessageTransport> {
    pub fn refresh_message_peer_targets(&mut self, peer_node_ids: Vec<NodeId>) {
        self.transport.replace_peer_nodes(peer_node_ids);
    }

    pub fn drain_message_deferred_work(&mut self) {
        self.drain_deferred_routed_create_parent_touches();
    }

    pub fn process_message_command(
        &mut self,
        command: DfsMessageCommand,
    ) -> Result<DfsMessageProcessResult, DfsError> {
        let request_id = command.request_id;
        let allow_write_repair_progress =
            dfs_network_message_allows_write_repair_progress(&command.routed_message.message);
        let foreground_write_request_id =
            served_message_foreground_write_request_id(&command.routed_message.message);
        self.transport
            .begin_message(&command.routed_message)
            .map_err(DfsError::from_network)?;
        let mut events = match self.process_served_routed_messages(vec![command.routed_message]) {
            Ok(events) => events,
            Err(error) => {
                self.transport.finish_message();
                return Err(error);
            }
        };
        for _ in 0..DFS_DATA_PLANE_MESSAGE_DRAIN_MAX_PASSES {
            let pending = match self
                .transport
                .pending_incoming_count()
                .map_err(DfsError::from_network)
            {
                Ok(pending) => pending,
                Err(error) => {
                    self.transport.finish_message();
                    return Err(error);
                }
            };
            if pending != Some(0) {
                let queued_messages = self.transport.drain_queued_messages();
                let batch = if queued_messages.is_empty() {
                    match self.process_incoming_with_options(false) {
                        Ok(batch) => batch,
                        Err(error) => {
                            self.transport.finish_message();
                            return Err(error);
                        }
                    }
                } else {
                    match self.process_served_routed_messages(queued_messages) {
                        Ok(batch) => batch,
                        Err(error) => {
                            self.transport.finish_message();
                            return Err(error);
                        }
                    }
                };
                events.extend(batch);
            }
            let defer_foreground_replica_repair = foreground_write_request_id.is_some()
                && self
                    .transport
                    .can_return_committed_foreground_response_before_replica_repair();
            let scheduled = if allow_write_repair_progress && !defer_foreground_replica_repair {
                let (scheduled, repair_events) =
                    if let Some(request_id) = foreground_write_request_id.as_deref() {
                        let intent_ids =
                            self.foreground_remote_write_intent_ids_for_request(request_id);
                        self.retry_pending_repairs_for_intents_best_effort(
                            &intent_ids,
                            DFS_SERVED_MESSAGE_REPAIR_MAX_JOBS,
                        )
                    } else {
                        self.retry_pending_repairs_best_effort_bounded(
                            DFS_SERVED_MESSAGE_REPAIR_MAX_JOBS,
                        )
                    };
                events.extend(repair_events);
                scheduled
            } else {
                Vec::new()
            };
            let pending = match self
                .transport
                .pending_incoming_count()
                .map_err(DfsError::from_network)
            {
                Ok(pending) => pending,
                Err(error) => {
                    self.transport.finish_message();
                    return Err(error);
                }
            };
            if pending == Some(0) && scheduled.is_empty() {
                let (outbound_messages, body) = self.transport.finish_message_with_body();
                return Ok(DfsMessageProcessResult {
                    response: DfsMessageResponse {
                        request_id,
                        outbound_messages,
                    },
                    body,
                    events,
                });
            }
        }
        let outbound_messages = self.transport.finish_message();
        Err(DfsError::Network {
            message: format!(
                "DFS data-plane message did not quiesce after {DFS_DATA_PLANE_MESSAGE_DRAIN_MAX_PASSES} passes; returned {} message(s) before aborting",
                outbound_messages.len()
            ),
        })
    }

    fn process_served_routed_messages(
        &mut self,
        messages: Vec<DfsRoutedMessage>,
    ) -> Result<Vec<NetworkedDfsServiceEvent>, DfsError> {
        self.prune_idle_transfer_state();
        let mut events = Vec::new();
        for message in messages {
            self.process_routed_message_with_options(message, false, &mut events)?;
        }
        self.prune_expired_pending_remote_reads();
        self.last_events = retained_networked_dfs_service_events(&events);
        Ok(events)
    }

    fn foreground_remote_write_intent_ids_for_request(&self, request_id: &str) -> BTreeSet<String> {
        self.pending_remote_write_responses
            .iter()
            .filter(|(_, pending)| {
                pending.foreground_replica_repair && pending.request_id == request_id
            })
            .map(|(intent_id, _)| intent_id.clone())
            .collect()
    }
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn retry_pending_repairs_best_effort(
        &mut self,
    ) -> (Vec<DistributedRepairJob>, Vec<NetworkedDfsServiceEvent>) {
        self.retry_pending_repairs_best_effort_bounded(usize::MAX)
    }

    pub(crate) fn retry_pending_repairs_best_effort_bounded(
        &mut self,
        max_scheduled: usize,
    ) -> (Vec<DistributedRepairJob>, Vec<NetworkedDfsServiceEvent>) {
        match self.retry_pending_repairs_bounded(max_scheduled) {
            Ok(scheduled) => (scheduled, Vec::new()),
            Err(error) => (
                Vec::new(),
                vec![NetworkedDfsServiceEvent::RepairRetryFailed {
                    reason: error.to_string(),
                }],
            ),
        }
    }

    pub(crate) fn retry_pending_repairs_for_intents_best_effort(
        &mut self,
        intent_ids: &BTreeSet<String>,
        max_scheduled: usize,
    ) -> (Vec<DistributedRepairJob>, Vec<NetworkedDfsServiceEvent>) {
        match self.retry_pending_repairs_for_intents_bounded(intent_ids, max_scheduled) {
            Ok(scheduled) => (scheduled, Vec::new()),
            Err(error) => (
                Vec::new(),
                vec![NetworkedDfsServiceEvent::RepairRetryFailed {
                    reason: error.to_string(),
                }],
            ),
        }
    }
}

fn served_message_foreground_write_request_id(message: &DfsNetworkMessage) -> Option<String> {
    match message {
        DfsNetworkMessage::DistributedWriteRequest(command)
            if command.foreground_replica_repair =>
        {
            Some(command.request_id.clone())
        }
        _ => None,
    }
}

#[cfg(test)]
#[derive(Clone, Debug)]
pub struct DfsMessageCommandProcessResult {
    pub response: DfsMessageResponse,
    pub events: Vec<NetworkedDfsServiceEvent>,
}

#[cfg(test)]
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct DfsMessageCommandHandleResult {
    pub response: DfsMessageResponse,
    pub metadata_publish_requested: bool,
    pub metadata_publish_schedule_error: Option<String>,
}

#[cfg(test)]
pub(crate) fn process_cluster_dfs_message_command(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    command: DfsMessageCommand,
    control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
) -> Result<Option<DfsMessageCommandProcessResult>, DfsError> {
    data_dir
        .ensure_root_layout()
        .map_err(|source| DfsError::Metadata {
            message: source.to_string(),
        })?;
    if !cluster_paths.state.exists() {
        return Ok(None);
    };
    let local_node_id = (sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths)
        .map_err(DfsError::from_network)?)
    .local_node_id;
    let content = LocalContentStore::new(data_dir.dfs_dir().join("content"))?;
    let metadata_backend = open_cluster_aware_dfs_metadata_backend(data_dir, cluster_paths)?;
    let message_sender: DfsMessageSender = Arc::new(|target_node_id, command, _timeout| {
        Err(DfsPeerTransportError::new(format!(
            "unexpected DFS message hop to {} while processing test command {}",
            target_node_id, command.request_id
        )))
    });
    let transport = DfsNodeServedMessageTransport::new_with_message_sender(
        local_node_id,
        Vec::new(),
        message_sender,
        sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
    );
    let mut service = NetworkedDfsService::new(transport, content);
    if let Some(control_plane) = control_plane {
        service = service.with_boxed_control_plane(control_plane);
    }
    let mut service = service.with_boxed_metadata_backend(metadata_backend)?;
    let DfsMessageProcessResult {
        mut response,
        body,
        events,
    } = service.process_message_command(command)?;
    if let Some(body) = body {
        let max_bytes = usize::try_from(body.descriptor.total_size_bytes).map_err(|_| {
            DfsError::Distributed {
                message: "DFS test response body is too large".to_string(),
            }
        })?;
        let bytes = collect_dfs_body_source_sync(body.source, max_bytes).map_err(|error| {
            DfsError::Network {
                message: error.to_string(),
            }
        })?;
        response = crate::body_rpc::attach_dfs_message_response_body(
            crate::DfsBodyMessageResponse {
                response,
                body: body.descriptor,
            },
            bytes,
        )
        .map_err(DfsError::from_network)?;
    }
    Ok(Some(DfsMessageCommandProcessResult { response, events }))
}

#[cfg(test)]
pub(crate) fn run_cluster_dfs_message_command_with_metadata_publish<R, E>(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    command: DfsMessageCommand,
    peer_transport: Arc<dyn DfsPeerTransport>,
    control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
    replication_options: DfsMetadataJournalReplicationOptions,
    on_report: R,
    on_error: E,
) -> Result<Option<DfsMessageCommandHandleResult>, DfsError>
where
    R: FnMut(DfsMetadataJournalReplicationReport),
    E: FnMut(String),
{
    let Some(result) =
        process_cluster_dfs_message_command(data_dir, cluster_paths, command, control_plane)?
    else {
        return Ok(None);
    };
    let DfsMessageCommandProcessResult { response, events } = result;
    let metadata_publish_requested =
        run_cluster_dfs_metadata_publish_after_events_with_peer_transport(
            data_dir,
            cluster_paths,
            peer_transport,
            &events,
            replication_options,
            on_report,
            on_error,
        );
    Ok(Some(DfsMessageCommandHandleResult {
        response,
        metadata_publish_requested,
        metadata_publish_schedule_error: None,
    }))
}
