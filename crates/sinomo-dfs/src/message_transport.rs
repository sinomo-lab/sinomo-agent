//! DFS message transport contracts.
//!
//! These traits adapt generic `sinomo-network` object-RPC transports to the
//! typed DFS message plane. Higher-level DFS services depend on these traits,
//! not on a concrete transport implementation.

use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};

use sinomo_core::network::{NetworkError, ObjectRpcBodySource};
use sinomo_core::node::NodeId;

use crate::protocol::DfsRoutedMessage;
use crate::routes::{dfs_routed_message_from_object_rpc_request, dfs_routed_message_rpc_request};
use crate::{
    DfsMetadataJournalBatchCommand, DfsMetadataJournalBatchResponse, DfsRpcBodyDescriptor,
    ObjectVersionReader,
};

#[derive(Debug)]
pub struct DfsOutboundBody {
    pub descriptor: DfsRpcBodyDescriptor,
    pub source: ObjectRpcBodySource,
}

impl DfsOutboundBody {
    pub fn from_reader(
        reader: ObjectVersionReader,
        total_size_bytes: u64,
        content_hash: impl Into<String>,
    ) -> Self {
        Self {
            descriptor: DfsRpcBodyDescriptor {
                total_size_bytes,
                content_hash: Some(content_hash.into()),
            },
            source: ObjectRpcBodySource::from_reader(reader, Some(total_size_bytes)),
        }
    }

    pub fn from_bytes(bytes: Vec<u8>, content_hash: impl Into<String>) -> Self {
        Self {
            descriptor: DfsRpcBodyDescriptor {
                total_size_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                content_hash: Some(content_hash.into()),
            },
            source: ObjectRpcBodySource::bytes(bytes),
        }
    }

    pub fn from_reader_with_transport_integrity(
        reader: ObjectVersionReader,
        total_size_bytes: u64,
    ) -> Self {
        Self {
            descriptor: DfsRpcBodyDescriptor {
                total_size_bytes,
                content_hash: None,
            },
            source: ObjectRpcBodySource::from_reader(reader, Some(total_size_bytes)),
        }
    }
}

pub trait DfsTransportIdentity {
    fn local_node_id(&self) -> &NodeId;
}

impl DfsTransportIdentity for sinomo_core::network::InProcessTransport {
    fn local_node_id(&self) -> &NodeId {
        self.local_node_id()
    }
}

impl DfsTransportIdentity for sinomo_core::network::AsyncInProcessTransport {
    fn local_node_id(&self) -> &NodeId {
        self.local_node_id()
    }
}

pub trait DfsMessageTransport: DfsTransportIdentity {
    fn send_routed_message(
        &mut self,
        target_node_id: &NodeId,
        message: DfsRoutedMessage,
    ) -> Result<(), NetworkError>;

    fn send_routed_message_with_body(
        &mut self,
        target_node_id: &NodeId,
        message: DfsRoutedMessage,
        body: DfsOutboundBody,
    ) -> Result<(), NetworkError> {
        let max_bytes = usize::try_from(body.descriptor.total_size_bytes).map_err(|_| {
            NetworkError::FrameTooLarge {
                actual_bytes: usize::MAX,
                max_bytes: usize::MAX,
            }
        })?;
        let bytes = collect_dfs_outbound_body_sync(body.source, max_bytes)?;
        let message =
            crate::body_rpc::attach_dfs_routed_message_body(message, body.descriptor, bytes)?;
        self.send_routed_message(target_node_id, message)
    }

    fn send_detached_routed_message(
        &mut self,
        target_node_id: &NodeId,
        message: DfsRoutedMessage,
    ) -> Result<(), NetworkError> {
        self.send_routed_message(target_node_id, message)
    }

    /// Send a metadata journal batch over the canonical metadata object-RPC
    /// route when this transport retains a direct peer transport.
    ///
    /// In-process and message-only test transports deliberately return
    /// `None`. Callers must treat that as absence of the stronger direct
    /// authority acknowledgement and use their fail-closed fallback.
    fn send_metadata_journal_batch_direct(
        &mut self,
        _target_node_id: &NodeId,
        _command: &DfsMetadataJournalBatchCommand,
    ) -> Result<Option<DfsMetadataJournalBatchResponse>, NetworkError> {
        Ok(None)
    }

    /// Send one prepared metadata batch to several replicas. Production node
    /// transports override this with concurrent fan-out so healthy authority
    /// latency is bounded by the slowest shard replica rather than the sum of
    /// replica RTTs. Message-only transports retain the compatible sequential
    /// fallback.
    fn send_metadata_journal_batches_direct(
        &mut self,
        requests: &[(NodeId, DfsMetadataJournalBatchCommand)],
    ) -> Result<Option<Vec<(NodeId, DfsMetadataJournalBatchResponse)>>, NetworkError> {
        let mut responses = Vec::with_capacity(requests.len());
        for (target_node_id, command) in requests {
            let Some(response) =
                self.send_metadata_journal_batch_direct(target_node_id, command)?
            else {
                return Ok(None);
            };
            responses.push((target_node_id.clone(), response));
        }
        Ok(Some(responses))
    }

    fn drain_incoming_messages(&mut self) -> Result<Vec<DfsRoutedMessage>, NetworkError>;

    fn flush_detached_sends(&mut self) -> Result<(), NetworkError> {
        Ok(())
    }

    fn can_send_synchronously(&self) -> bool {
        true
    }

    fn can_return_committed_foreground_response_before_replica_repair(&self) -> bool {
        false
    }

    fn pending_incoming_count(&self) -> Result<Option<usize>, NetworkError> {
        Ok(None)
    }

    fn set_operation_deadline(&mut self, _deadline: Option<Instant>) -> Option<Instant> {
        None
    }

    /// Returns the absolute deadline currently bounding the caller's DFS
    /// operation, when one was installed through [`Self::set_operation_deadline`].
    ///
    /// Service-level settle loops use the same deadline as routed sends so a
    /// caller budget cannot be extended by waiting for an already-dispatched
    /// write after the transport phase completes.
    fn operation_deadline(&self) -> Option<Instant> {
        None
    }

    fn wait_for_incoming(&self, timeout: Duration) -> Result<bool, NetworkError> {
        if self
            .pending_incoming_count()?
            .is_some_and(|count| count > 0)
        {
            return Ok(true);
        }
        if !timeout.is_zero() {
            std::thread::sleep(timeout);
        }
        Ok(self
            .pending_incoming_count()?
            .is_some_and(|count| count > 0))
    }
}

pub type DfsMessageTransportFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait AsyncDfsMessageTransport: Send {
    fn local_node_id(&self) -> &NodeId;

    fn send_routed_message_async<'a>(
        &'a mut self,
        target_node_id: &'a NodeId,
        message: DfsRoutedMessage,
    ) -> DfsMessageTransportFuture<'a, Result<(), NetworkError>>;

    fn send_routed_message_with_body_async<'a>(
        &'a mut self,
        target_node_id: &'a NodeId,
        message: DfsRoutedMessage,
        body: DfsOutboundBody,
    ) -> DfsMessageTransportFuture<'a, Result<(), NetworkError>> {
        Box::pin(async move {
            let max_bytes = usize::try_from(body.descriptor.total_size_bytes).map_err(|_| {
                NetworkError::FrameTooLarge {
                    actual_bytes: usize::MAX,
                    max_bytes: usize::MAX,
                }
            })?;
            let bytes = body.source.collect_to_vec(max_bytes).await?;
            let message =
                crate::body_rpc::attach_dfs_routed_message_body(message, body.descriptor, bytes)?;
            self.send_routed_message_async(target_node_id, message)
                .await
        })
    }

    fn drain_incoming_messages_async<'a>(
        &'a mut self,
    ) -> DfsMessageTransportFuture<'a, Result<Vec<DfsRoutedMessage>, NetworkError>>;

    fn pending_incoming_count_async<'a>(
        &'a mut self,
    ) -> DfsMessageTransportFuture<'a, Result<Option<usize>, NetworkError>> {
        Box::pin(async { Ok(None) })
    }

    fn wait_for_incoming_async<'a>(
        &'a mut self,
        timeout: Duration,
    ) -> DfsMessageTransportFuture<'a, Result<bool, NetworkError>> {
        Box::pin(async move {
            if self
                .pending_incoming_count_async()
                .await?
                .is_some_and(|count| count > 0)
            {
                return Ok(true);
            }
            if !timeout.is_zero() {
                tokio::time::sleep(timeout).await;
            }
            Ok(self
                .pending_incoming_count_async()
                .await?
                .is_some_and(|count| count > 0))
        })
    }
}

fn collect_dfs_outbound_body_sync(
    source: ObjectRpcBodySource,
    max_bytes: usize,
) -> Result<Vec<u8>, NetworkError> {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        if matches!(
            handle.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            return tokio::task::block_in_place(|| {
                handle.block_on(source.collect_to_vec(max_bytes))
            });
        }
        return Err(NetworkError::Io {
            operation: "collect DFS outbound body".to_string(),
            detail: "sync DFS body fallback cannot run inside a current-thread Tokio runtime"
                .to_string(),
        });
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| NetworkError::Io {
            operation: "create DFS body collection runtime".to_string(),
            detail: source.to_string(),
        })?;
    let result = runtime.block_on(source.collect_to_vec(max_bytes));
    runtime.shutdown_timeout(Duration::from_millis(250));
    result
}

impl DfsMessageTransport for sinomo_core::network::InProcessTransport {
    fn send_routed_message(
        &mut self,
        target_node_id: &NodeId,
        message: DfsRoutedMessage,
    ) -> Result<(), NetworkError> {
        let request = dfs_routed_message_rpc_request(&message)?;
        self.send_object_rpc_request(target_node_id, request)
    }

    fn drain_incoming_messages(&mut self) -> Result<Vec<DfsRoutedMessage>, NetworkError> {
        Ok(self
            .drain_incoming_object_rpc_requests()?
            .into_iter()
            .filter_map(|request| dfs_routed_message_from_object_rpc_request(&request).ok())
            .collect())
    }

    fn pending_incoming_count(&self) -> Result<Option<usize>, NetworkError> {
        self.pending_incoming_object_rpc_request_count()
    }

    fn wait_for_incoming(&self, timeout: Duration) -> Result<bool, NetworkError> {
        self.wait_for_incoming_object_rpc_request(timeout)
    }
}

impl AsyncDfsMessageTransport for sinomo_core::network::AsyncInProcessTransport {
    fn local_node_id(&self) -> &NodeId {
        self.local_node_id()
    }

    fn send_routed_message_async<'a>(
        &'a mut self,
        target_node_id: &'a NodeId,
        message: DfsRoutedMessage,
    ) -> DfsMessageTransportFuture<'a, Result<(), NetworkError>> {
        Box::pin(async move {
            let request = dfs_routed_message_rpc_request(&message)?;
            self.send_object_rpc_request_async(
                target_node_id,
                request,
                sinomo_core::network::AsyncNetworkSendOptions::default(),
            )
            .await
        })
    }

    fn drain_incoming_messages_async<'a>(
        &'a mut self,
    ) -> DfsMessageTransportFuture<'a, Result<Vec<DfsRoutedMessage>, NetworkError>> {
        Box::pin(async move {
            Ok(self
                .drain_incoming_object_rpc_requests(usize::MAX)
                .await?
                .into_iter()
                .filter_map(|request| dfs_routed_message_from_object_rpc_request(&request).ok())
                .collect())
        })
    }

    fn pending_incoming_count_async<'a>(
        &'a mut self,
    ) -> DfsMessageTransportFuture<'a, Result<Option<usize>, NetworkError>> {
        Box::pin(async move { self.pending_incoming_object_rpc_request_count().await })
    }

    fn wait_for_incoming_async<'a>(
        &'a mut self,
        timeout: Duration,
    ) -> DfsMessageTransportFuture<'a, Result<bool, NetworkError>> {
        Box::pin(async move { self.wait_for_incoming_object_rpc_request(timeout).await })
    }
}
