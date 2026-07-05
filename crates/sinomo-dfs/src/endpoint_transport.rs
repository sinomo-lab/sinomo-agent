//! Endpoint-backed DFS data-plane transport and served-message processing.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Serialize;
use sinomo_core::ClusterEndpointRecord;
#[cfg(test)]
use sinomo_core::local::SinomoDataDir;
#[cfg(test)]
use sinomo_core::network::ClusterPaths;
use sinomo_core::network::{NetworkError, ObjectRpcRequest, ObjectRpcResponse, SinomoObjectCodec};
use sinomo_core::node::NodeId;

use crate::{
    AsyncDfsMessageTransport, DFS_DATA_PLANE_MESSAGE_DRAIN_MAX_PASSES, DfsDirectReadRequest,
    DfsError, DfsMessageCommand, DfsMessageProcessResult, DfsMessageResponse, DfsMessageTransport,
    DfsMessageTransportFuture, DfsNetworkMessage, DfsReadResponse, DfsRoutedMessage,
    DfsTransferProfile, DfsTransportIdentity, DistributedRepairJob, NetworkedDfsService,
    NetworkedDfsServiceEvent, decode_dfs_message_rpc_response,
    dfs_direct_read_request_from_read_request, dfs_direct_read_response_from_rpc_response,
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

pub const DEFAULT_DFS_MESSAGE_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
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
            Duration,
        ) -> Result<DfsMessageResponse, DfsPeerTransportError>
        + Send
        + Sync,
>;

pub(crate) type DfsDirectReadSender = Arc<
    dyn Fn(
            &NodeId,
            &DfsDirectReadRequest,
            Duration,
        ) -> Result<DfsReadResponse, DfsPeerTransportError>
        + Send
        + Sync,
>;

pub(crate) type AsyncDfsDirectReadSender = Arc<
    dyn for<'a> Fn(
            &'a NodeId,
            &'a DfsDirectReadRequest,
            Duration,
        )
            -> DfsPeerTransportFuture<'a, Result<DfsReadResponse, DfsPeerTransportError>>
        + Send
        + Sync,
>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DfsObjectRpcSendOptions {
    pub connect_timeout: Duration,
    pub observe_connectivity: bool,
}

impl DfsObjectRpcSendOptions {
    pub fn new(connect_timeout: Duration) -> Self {
        Self {
            connect_timeout,
            observe_connectivity: true,
        }
    }

    pub fn with_observe_connectivity(mut self, observe_connectivity: bool) -> Self {
        self.observe_connectivity = observe_connectivity;
        self
    }
}

pub trait DfsPeerTransport: Send + Sync {
    fn call_object_rpc(
        &self,
        target_node_id: &NodeId,
        request: &ObjectRpcRequest,
        options: DfsObjectRpcSendOptions,
    ) -> Result<ObjectRpcResponse, DfsPeerTransportError>;
}

pub type DfsPeerTransportFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait AsyncDfsPeerTransport: Send + Sync {
    fn call_object_rpc_async<'a>(
        &'a self,
        target_node_id: &'a NodeId,
        request: &'a ObjectRpcRequest,
        options: DfsObjectRpcSendOptions,
    ) -> DfsPeerTransportFuture<'a, Result<ObjectRpcResponse, DfsPeerTransportError>>;
}

pub(crate) type AsyncDfsMessageSender = Arc<
    dyn for<'a> Fn(
            &'a NodeId,
            &'a DfsMessageCommand,
            Duration,
        )
            -> DfsPeerTransportFuture<'a, Result<DfsMessageResponse, DfsPeerTransportError>>
        + Send
        + Sync,
>;

pub(crate) fn async_dfs_message_sender_from_peer_transport(
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
) -> AsyncDfsMessageSender {
    Arc::new(move |target_node_id, command, connect_timeout| {
        let peer_transport = Arc::clone(&peer_transport);
        Box::pin(async move {
            let request = dfs_message_rpc_request(command)
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
            let response = peer_transport
                .call_object_rpc_async(
                    target_node_id,
                    &request,
                    DfsObjectRpcSendOptions::new(connect_timeout)
                        .with_observe_connectivity(!dfs_message_is_read_only_request(command)),
                )
                .await?;
            decode_dfs_message_rpc_response(&response)
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))
        })
    })
}

pub(crate) fn async_dfs_direct_read_sender_from_peer_transport(
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
) -> AsyncDfsDirectReadSender {
    Arc::new(move |target_node_id, request, connect_timeout| {
        let peer_transport = Arc::clone(&peer_transport);
        Box::pin(async move {
            let rpc_request = dfs_direct_read_rpc_request(request)
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
            let response = peer_transport
                .call_object_rpc_async(
                    target_node_id,
                    &rpc_request,
                    DfsObjectRpcSendOptions::new(connect_timeout),
                )
                .await?;
            dfs_direct_read_response_from_rpc_response(response)
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))
        })
    })
}

pub(crate) fn dfs_message_sender_from_peer_transport(
    peer_transport: Arc<dyn DfsPeerTransport>,
) -> DfsMessageSender {
    Arc::new(move |target_node_id, command, connect_timeout| {
        let request = dfs_message_rpc_request(command)
            .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
        let response = peer_transport.call_object_rpc(
            target_node_id,
            &request,
            DfsObjectRpcSendOptions::new(connect_timeout)
                .with_observe_connectivity(!dfs_message_is_read_only_request(command)),
        )?;
        decode_dfs_message_rpc_response(&response)
            .map_err(|source| DfsPeerTransportError::new(source.to_string()))
    })
}

pub(crate) fn dfs_direct_read_sender_from_peer_transport(
    peer_transport: Arc<dyn DfsPeerTransport>,
) -> DfsDirectReadSender {
    Arc::new(move |target_node_id, request, connect_timeout| {
        let rpc_request = dfs_direct_read_rpc_request(request)
            .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
        let response = peer_transport.call_object_rpc(
            target_node_id,
            &rpc_request,
            DfsObjectRpcSendOptions::new(connect_timeout),
        )?;
        dfs_direct_read_response_from_rpc_response(response)
            .map_err(|source| DfsPeerTransportError::new(source.to_string()))
    })
}
#[derive(Clone)]
struct DeferredDfsEndpointSend {
    sender: DfsMessageSender,
    connect_timeout: Duration,
    target_node_id: NodeId,
    routed_message: DfsRoutedMessage,
}

#[derive(Clone)]
pub struct DfsEndpointTransport {
    local_node_id: NodeId,
    endpoints_by_node_id: BTreeMap<NodeId, Vec<ClusterEndpointRecord>>,
    incoming_messages: VecDeque<DfsRoutedMessage>,
    detached_sends: Arc<std::sync::Mutex<Vec<DeferredDfsEndpointSend>>>,
    connect_timeout: Duration,
    sender: Option<DfsMessageSender>,
    async_sender: Option<AsyncDfsMessageSender>,
    direct_read_sender: Option<DfsDirectReadSender>,
    async_direct_read_sender: Option<AsyncDfsDirectReadSender>,
}

impl std::fmt::Debug for DfsEndpointTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DfsEndpointTransport")
            .field("local_node_id", &self.local_node_id)
            .field("endpoints_by_node_id", &self.endpoints_by_node_id)
            .field("incoming_messages", &self.incoming_messages)
            .field("connect_timeout", &self.connect_timeout)
            .finish_non_exhaustive()
    }
}

impl DfsEndpointTransport {
    #[cfg(test)]
    pub(crate) fn new(
        local_node_id: NodeId,
        endpoints: impl IntoIterator<Item = ClusterEndpointRecord>,
        sender: DfsMessageSender,
    ) -> Self {
        Self::with_connect_timeout(
            local_node_id,
            endpoints,
            DEFAULT_DFS_MESSAGE_CONNECT_TIMEOUT,
            sender,
        )
    }

    #[cfg(test)]
    pub(crate) fn with_connect_timeout(
        local_node_id: NodeId,
        endpoints: impl IntoIterator<Item = ClusterEndpointRecord>,
        connect_timeout: Duration,
        sender: DfsMessageSender,
    ) -> Self {
        Self::with_senders(
            local_node_id,
            endpoints,
            connect_timeout,
            Some(sender),
            None,
            None,
            None,
        )
    }

    pub(crate) fn with_message_sender(
        local_node_id: NodeId,
        endpoints: impl IntoIterator<Item = ClusterEndpointRecord>,
        connect_timeout: Duration,
        sender: DfsMessageSender,
    ) -> Self {
        Self::with_senders(
            local_node_id,
            endpoints,
            connect_timeout,
            Some(sender),
            None,
            None,
            None,
        )
    }

    #[cfg(test)]
    pub(crate) fn with_connect_timeout_async(
        local_node_id: NodeId,
        endpoints: impl IntoIterator<Item = ClusterEndpointRecord>,
        connect_timeout: Duration,
        sender: AsyncDfsMessageSender,
    ) -> Self {
        Self::with_senders(
            local_node_id,
            endpoints,
            connect_timeout,
            None,
            Some(sender),
            None,
            None,
        )
    }

    #[cfg(test)]
    pub(crate) fn with_connect_timeout_and_senders(
        local_node_id: NodeId,
        endpoints: impl IntoIterator<Item = ClusterEndpointRecord>,
        connect_timeout: Duration,
        sender: DfsMessageSender,
        async_sender: AsyncDfsMessageSender,
    ) -> Self {
        Self::with_senders(
            local_node_id,
            endpoints,
            connect_timeout,
            Some(sender),
            Some(async_sender),
            None,
            None,
        )
    }

    #[cfg(test)]
    pub(crate) fn new_with_peer_senders(
        local_node_id: NodeId,
        endpoints: impl IntoIterator<Item = ClusterEndpointRecord>,
        sender: DfsMessageSender,
        async_sender: AsyncDfsMessageSender,
        direct_read_sender: DfsDirectReadSender,
    ) -> Self {
        Self::with_senders(
            local_node_id,
            endpoints,
            DEFAULT_DFS_MESSAGE_CONNECT_TIMEOUT,
            Some(sender),
            Some(async_sender),
            Some(direct_read_sender),
            None,
        )
    }

    pub fn with_peer_transports(
        local_node_id: NodeId,
        endpoints: impl IntoIterator<Item = ClusterEndpointRecord>,
        connect_timeout: Duration,
        peer_transport: Arc<dyn DfsPeerTransport>,
        async_peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    ) -> Self {
        let direct_read_peer_transport = Arc::clone(&peer_transport);
        let async_direct_read_peer_transport = Arc::clone(&async_peer_transport);
        Self::with_senders(
            local_node_id,
            endpoints,
            connect_timeout,
            Some(dfs_message_sender_from_peer_transport(peer_transport)),
            Some(async_dfs_message_sender_from_peer_transport(
                async_peer_transport,
            )),
            Some(dfs_direct_read_sender_from_peer_transport(
                direct_read_peer_transport,
            )),
            Some(async_dfs_direct_read_sender_from_peer_transport(
                async_direct_read_peer_transport,
            )),
        )
    }

    #[cfg(test)]
    pub(crate) fn new_with_message_peer_senders(
        local_node_id: NodeId,
        endpoints: impl IntoIterator<Item = ClusterEndpointRecord>,
        sender: DfsMessageSender,
        async_sender: AsyncDfsMessageSender,
        direct_read_sender: DfsDirectReadSender,
        async_direct_read_sender: AsyncDfsDirectReadSender,
    ) -> Self {
        Self::with_senders(
            local_node_id,
            endpoints,
            DEFAULT_DFS_MESSAGE_CONNECT_TIMEOUT,
            Some(sender),
            Some(async_sender),
            Some(direct_read_sender),
            Some(async_direct_read_sender),
        )
    }

    fn with_senders(
        local_node_id: NodeId,
        endpoints: impl IntoIterator<Item = ClusterEndpointRecord>,
        connect_timeout: Duration,
        sender: Option<DfsMessageSender>,
        async_sender: Option<AsyncDfsMessageSender>,
        direct_read_sender: Option<DfsDirectReadSender>,
        async_direct_read_sender: Option<AsyncDfsDirectReadSender>,
    ) -> Self {
        let mut endpoints_by_node_id: BTreeMap<NodeId, Vec<ClusterEndpointRecord>> =
            BTreeMap::new();
        for endpoint in endpoints {
            if endpoint.node_id != local_node_id {
                endpoints_by_node_id
                    .entry(endpoint.node_id.clone())
                    .or_default()
                    .push(endpoint);
            }
        }
        Self {
            local_node_id,
            endpoints_by_node_id,
            incoming_messages: VecDeque::new(),
            detached_sends: Arc::new(std::sync::Mutex::new(Vec::new())),
            connect_timeout,
            sender,
            async_sender,
            direct_read_sender,
            async_direct_read_sender,
        }
    }

    fn replace_endpoints(&mut self, endpoints: impl IntoIterator<Item = ClusterEndpointRecord>) {
        let mut endpoints_by_node_id: BTreeMap<NodeId, Vec<ClusterEndpointRecord>> =
            BTreeMap::new();
        for endpoint in endpoints {
            if endpoint.node_id != self.local_node_id {
                endpoints_by_node_id
                    .entry(endpoint.node_id.clone())
                    .or_default()
                    .push(endpoint);
            }
        }
        self.endpoints_by_node_id = endpoints_by_node_id;
    }

    fn transfer_profile_for_target(&self, target_node_id: &NodeId) -> DfsTransferProfile {
        if target_node_id == &self.local_node_id {
            return DfsTransferProfile::Local;
        }
        self.endpoints_by_node_id
            .get(target_node_id)
            .map(|endpoints| {
                sinomo_core::network::ClusterEndpointSelector::transfer_profile_for_endpoints(
                    endpoints.iter(),
                )
                .into()
            })
            .unwrap_or(DfsTransferProfile::Conservative)
    }

    fn send_routed_message_to_endpoint(
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
                self.try_send_direct_read_message_to_endpoint(&target_node_id, &routed_message)?
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
                    detail: "DFS endpoint transport was constructed without a sync sender"
                        .to_string(),
                });
            };
            let returned_messages = send_dfs_routed_message_to_endpoint(
                sender,
                self.connect_timeout,
                target_node_id,
                routed_message,
            )?;
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

    fn try_send_direct_read_message_to_endpoint(
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
        let response = send_dfs_direct_read_to_endpoint(
            sender,
            self.connect_timeout,
            target_node_id.clone(),
            &request,
        )?;
        Ok(Some(vec![dfs_direct_read_response_routed_message(
            target_node_id,
            routed_message,
            response,
        )]))
    }

    async fn try_send_direct_read_message_to_endpoint_async(
        &self,
        target_node_id: &NodeId,
        routed_message: &DfsRoutedMessage,
    ) -> Result<Option<Vec<DfsRoutedMessage>>, NetworkError> {
        let Some(sender) = self.async_direct_read_sender.as_ref().cloned() else {
            return Ok(None);
        };
        let DfsNetworkMessage::ReadRequest(read_request) = &routed_message.message else {
            return Ok(None);
        };
        let Some(request) = dfs_direct_read_request_from_read_request(read_request) else {
            return Ok(None);
        };
        let response = send_dfs_direct_read_to_endpoint_async(
            sender,
            self.connect_timeout,
            target_node_id.clone(),
            &request,
        )
        .await?;
        Ok(Some(vec![dfs_direct_read_response_routed_message(
            target_node_id,
            routed_message,
            response,
        )]))
    }

    fn send_routed_message_to_endpoint_detached(
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
                detail: "DFS endpoint transport was constructed without a sync sender".to_string(),
            });
        };
        self.detached_sends
            .lock()
            .map_err(|_| NetworkError::Io {
                operation: "track detached DFS message send".to_string(),
                detail: "detached send lock is poisoned".to_string(),
            })?
            .push(DeferredDfsEndpointSend {
                sender,
                connect_timeout: self.connect_timeout,
                target_node_id: target_node_id.clone(),
                routed_message,
            });
        Ok(())
    }

    fn flush_detached_endpoint_sends(&mut self) -> Result<(), NetworkError> {
        let sends = {
            let mut sends = self.detached_sends.lock().map_err(|_| NetworkError::Io {
                operation: "flush detached DFS message sends".to_string(),
                detail: "detached send lock is poisoned".to_string(),
            })?;
            std::mem::take(&mut *sends)
        };
        for send in sends {
            let returned_messages = send_dfs_routed_message_to_endpoint(
                send.sender,
                send.connect_timeout,
                send.target_node_id,
                send.routed_message,
            )?;
            for returned_message in returned_messages {
                let target_node_id = returned_message.target_node_id.clone();
                self.send_routed_message_to_endpoint(&target_node_id, returned_message, false)?;
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
                .endpoints_by_node_id
                .contains_key(&routed_message.source_node_id)
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
        if !self.endpoints_by_node_id.contains_key(target_node_id) {
            return Err(NetworkError::UnknownNode {
                node_id: target_node_id.clone(),
            });
        }
        Ok(())
    }

    async fn send_routed_message_to_endpoint_async(
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
                .try_send_direct_read_message_to_endpoint_async(&target_node_id, &routed_message)
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
            let connect_timeout = self.connect_timeout;
            let Some(sender) = self.async_sender.as_ref().cloned() else {
                return Err(NetworkError::Io {
                    operation: format!("send DFS message to {target_node_id}"),
                    detail: "DFS endpoint transport was constructed without an async sender"
                        .to_string(),
                });
            };
            let returned_messages = send_dfs_routed_message_to_endpoint_async(
                sender,
                connect_timeout,
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

fn send_dfs_routed_message_to_endpoint(
    sender: DfsMessageSender,
    connect_timeout: Duration,
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
    match sender(&target_node_id, &command, connect_timeout) {
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

fn send_dfs_direct_read_to_endpoint(
    sender: DfsDirectReadSender,
    connect_timeout: Duration,
    target_node_id: NodeId,
    request: &DfsDirectReadRequest,
) -> Result<DfsReadResponse, NetworkError> {
    let send_started = Instant::now();
    match sender(&target_node_id, request, connect_timeout) {
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

async fn send_dfs_direct_read_to_endpoint_async(
    sender: AsyncDfsDirectReadSender,
    connect_timeout: Duration,
    target_node_id: NodeId,
    request: &DfsDirectReadRequest,
) -> Result<DfsReadResponse, NetworkError> {
    let send_started = Instant::now();
    match sender(&target_node_id, request, connect_timeout).await {
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

async fn send_dfs_routed_message_to_endpoint_async(
    sender: AsyncDfsMessageSender,
    connect_timeout: Duration,
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
    match sender(&target_node_id, &command, connect_timeout).await {
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

impl DfsTransportIdentity for DfsEndpointTransport {
    fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }
}

impl AsyncDfsMessageTransport for DfsEndpointTransport {
    fn local_node_id(&self) -> &NodeId {
        DfsTransportIdentity::local_node_id(self)
    }

    fn send_routed_message_async<'a>(
        &'a mut self,
        target_node_id: &'a NodeId,
        message: DfsRoutedMessage,
    ) -> DfsMessageTransportFuture<'a, Result<(), NetworkError>> {
        Box::pin(async move {
            self.send_routed_message_to_endpoint_async(target_node_id, message, true)
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

impl DfsMessageTransport for DfsEndpointTransport {
    fn send_routed_message(
        &mut self,
        target_node_id: &NodeId,
        message: DfsRoutedMessage,
    ) -> Result<(), NetworkError> {
        self.send_routed_message_to_endpoint(target_node_id, message, true)
    }

    fn send_detached_routed_message(
        &mut self,
        target_node_id: &NodeId,
        message: DfsRoutedMessage,
    ) -> Result<(), NetworkError> {
        self.send_routed_message_to_endpoint_detached(target_node_id, message, true)
    }

    fn drain_incoming_messages(&mut self) -> Result<Vec<DfsRoutedMessage>, NetworkError> {
        Ok(self.incoming_messages.drain(..).collect())
    }

    fn flush_detached_sends(&mut self) -> Result<(), NetworkError> {
        self.flush_detached_endpoint_sends()
    }

    fn can_send_synchronously(&self) -> bool {
        self.sender.is_some()
    }

    fn pending_incoming_count(&self) -> Result<Option<usize>, NetworkError> {
        Ok(Some(self.incoming_messages.len()))
    }

    fn transfer_profile(&self, target_node_id: &NodeId) -> DfsTransferProfile {
        self.transfer_profile_for_target(target_node_id)
    }
}

impl NetworkedDfsService<DfsEndpointTransport> {
    pub fn refresh_endpoint_targets(&mut self, endpoints: Vec<ClusterEndpointRecord>) {
        self.transport.replace_endpoints(endpoints);
    }
}

#[derive(Debug)]
pub struct DfsEndpointServedMessageTransport {
    local_node_id: NodeId,
    return_node_id: Option<NodeId>,
    inbound_messages: VecDeque<DfsRoutedMessage>,
    outbound_messages: Vec<DfsRoutedMessage>,
    endpoint_transport: DfsEndpointTransport,
}

impl DfsEndpointServedMessageTransport {
    pub fn new_with_peer_transport(
        local_node_id: NodeId,
        endpoints: Vec<ClusterEndpointRecord>,
        peer_transport: Arc<dyn DfsPeerTransport>,
        connect_timeout: Duration,
    ) -> Self {
        Self::new_with_message_sender(
            local_node_id,
            endpoints,
            dfs_message_sender_from_peer_transport(peer_transport),
            connect_timeout,
        )
    }

    pub(crate) fn new_with_message_sender(
        local_node_id: NodeId,
        endpoints: Vec<ClusterEndpointRecord>,
        sender: DfsMessageSender,
        connect_timeout: Duration,
    ) -> Self {
        let endpoint_transport = DfsEndpointTransport::with_senders(
            local_node_id.clone(),
            endpoints,
            connect_timeout,
            Some(sender),
            None,
            None,
            None,
        );
        Self {
            local_node_id,
            return_node_id: None,
            inbound_messages: VecDeque::new(),
            outbound_messages: Vec::new(),
            endpoint_transport,
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
        self.return_node_id = None;
        self.inbound_messages.clear();
        self.endpoint_transport.incoming_messages.clear();
        std::mem::take(&mut self.outbound_messages)
    }

    pub fn replace_endpoints(&mut self, endpoints: Vec<ClusterEndpointRecord>) {
        self.endpoint_transport.replace_endpoints(endpoints);
    }

    fn drain_queued_messages(&mut self) -> Vec<DfsRoutedMessage> {
        self.inbound_messages.drain(..).collect()
    }
}

impl DfsTransportIdentity for DfsEndpointServedMessageTransport {
    fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }
}

impl DfsMessageTransport for DfsEndpointServedMessageTransport {
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
        {
            self.outbound_messages.push(message);
            return Ok(());
        }
        self.endpoint_transport
            .send_routed_message_to_endpoint(target_node_id, message, true)
    }

    fn drain_incoming_messages(&mut self) -> Result<Vec<DfsRoutedMessage>, NetworkError> {
        let mut messages = self.inbound_messages.drain(..).collect::<Vec<_>>();
        messages.extend(self.endpoint_transport.drain_incoming_messages()?);
        Ok(messages)
    }

    fn pending_incoming_count(&self) -> Result<Option<usize>, NetworkError> {
        let nested = self
            .endpoint_transport
            .pending_incoming_count()?
            .unwrap_or_default();
        Ok(Some(self.inbound_messages.len().saturating_add(nested)))
    }

    fn can_return_committed_foreground_response_before_replica_repair(&self) -> bool {
        true
    }

    fn transfer_profile(&self, target_node_id: &NodeId) -> DfsTransferProfile {
        if target_node_id == &self.local_node_id {
            return DfsTransferProfile::Local;
        }
        self.endpoint_transport
            .transfer_profile_for_target(target_node_id)
    }
}

impl NetworkedDfsService<DfsEndpointServedMessageTransport> {
    pub fn refresh_message_endpoints(&mut self, endpoints: Vec<ClusterEndpointRecord>) {
        self.transport.replace_endpoints(endpoints);
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
                let outbound_messages = self.transport.finish_message();
                return Ok(DfsMessageProcessResult {
                    response: DfsMessageResponse {
                        request_id,
                        outbound_messages,
                    },
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
        self.prune_idle_chunk_transfers();
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
    let message_sender: DfsMessageSender = Arc::new(|endpoint, command, _timeout| {
        Err(DfsPeerTransportError::new(format!(
            "unexpected DFS message hop to {} while processing test command {}",
            endpoint, command.request_id
        )))
    });
    let transport = DfsEndpointServedMessageTransport::new_with_message_sender(
        local_node_id,
        Vec::new(),
        message_sender,
        DEFAULT_DFS_MESSAGE_CONNECT_TIMEOUT,
    );
    let mut service = NetworkedDfsService::new(transport, content);
    if let Some(control_plane) = control_plane {
        service = service.with_boxed_control_plane(control_plane);
    }
    let mut service = service.with_boxed_metadata_backend(metadata_backend)?;
    let DfsMessageProcessResult { response, events } = service.process_message_command(command)?;
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
