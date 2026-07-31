//! DFS message transport contracts.
//!
//! These traits adapt generic `sinomo-network` object-RPC transports to the
//! typed DFS message plane. Higher-level DFS services depend on these traits,
//! not on a concrete transport implementation.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use sinomo_core::network::{NetworkError, ObjectRpcBodySource};
use sinomo_core::node::NodeId;

use crate::protocol::DfsRoutedMessage;
use crate::routes::{dfs_routed_message_from_object_rpc_request, dfs_routed_message_rpc_request};
use crate::{
    DfsContentRootCatalogPage, DfsContentRootCatalogPageRequest, DfsMetadataJournalBatchCommand,
    DfsMetadataJournalBatchResponse, DfsRpcAttachmentDescriptor, ObjectVersionReader,
};

#[derive(Debug)]
pub struct DfsOutboundAttachment {
    pub descriptor: DfsRpcAttachmentDescriptor,
    pub source: ObjectRpcBodySource,
}

/// One DFS routed message plus its optional semantic attachment.
#[derive(Debug)]
pub struct DfsRoutedCall {
    pub message: DfsRoutedMessage,
    pub attachment: Option<DfsOutboundAttachment>,
}

impl DfsRoutedCall {
    pub fn value(message: DfsRoutedMessage) -> Self {
        Self {
            message,
            attachment: None,
        }
    }

    pub fn with_attachment(message: DfsRoutedMessage, attachment: DfsOutboundAttachment) -> Self {
        Self {
            message,
            attachment: Some(attachment),
        }
    }
}

/// One independently routable DFS call in a bounded replica fan-out.
///
/// The target is retained alongside the routed frame so transports can return
/// one ordered outcome per replica without conflating a failed peer with the
/// rest of the fan-out.
#[derive(Debug)]
pub struct DfsRoutedSend {
    pub target_node_id: NodeId,
    pub call: DfsRoutedCall,
}

impl DfsRoutedSend {
    pub fn new(target_node_id: NodeId, call: DfsRoutedCall) -> Self {
        Self {
            target_node_id,
            call,
        }
    }
}

#[derive(Debug)]
pub struct DfsRoutedSendOutcome {
    pub target_node_id: NodeId,
    pub result: Result<(), NetworkError>,
}

impl DfsOutboundAttachment {
    pub fn from_reader(
        reader: ObjectVersionReader,
        total_size_bytes: u64,
        content_hash: impl Into<String>,
    ) -> Self {
        Self {
            descriptor: DfsRpcAttachmentDescriptor {
                total_size_bytes,
                content_hash: Some(content_hash.into()),
            },
            source: ObjectRpcBodySource::from_reader(reader, Some(total_size_bytes)),
        }
    }

    pub fn from_bytes(bytes: Vec<u8>, content_hash: impl Into<String>) -> Self {
        Self {
            descriptor: DfsRpcAttachmentDescriptor {
                total_size_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                content_hash: Some(content_hash.into()),
            },
            source: ObjectRpcBodySource::bytes(bytes),
        }
    }

    pub fn from_bytes_with_transport_integrity(bytes: Vec<u8>) -> Self {
        Self {
            descriptor: DfsRpcAttachmentDescriptor {
                total_size_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                content_hash: None,
            },
            source: ObjectRpcBodySource::bytes(bytes),
        }
    }

    pub fn from_reader_with_transport_integrity(
        reader: ObjectVersionReader,
        total_size_bytes: u64,
    ) -> Self {
        Self {
            descriptor: DfsRpcAttachmentDescriptor {
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
    fn send_routed_call(
        &mut self,
        target_node_id: &NodeId,
        call: DfsRoutedCall,
    ) -> Result<(), NetworkError>;

    /// Sends independent replica calls with at most `max_inflight` sends
    /// active at once. A zero limit is normalized to one. The default
    /// implementation is sequential; node transports override it when their
    /// underlying routing handles can be forked without sharing mutable
    /// response queues.
    ///
    /// Outcomes remain in request order and a peer-level failure is isolated
    /// to that request. The outer error is reserved for fan-out orchestration
    /// failures where ordered per-peer outcomes cannot be produced.
    fn send_routed_calls_bounded(
        &mut self,
        requests: Vec<DfsRoutedSend>,
        _max_inflight: usize,
    ) -> Result<Vec<DfsRoutedSendOutcome>, NetworkError> {
        Ok(requests
            .into_iter()
            .map(|request| {
                let result = self.send_routed_call(&request.target_node_id, request.call);
                DfsRoutedSendOutcome {
                    target_node_id: request.target_node_id,
                    result,
                }
            })
            .collect())
    }

    fn send_detached_routed_message(
        &mut self,
        target_node_id: &NodeId,
        message: DfsRoutedMessage,
    ) -> Result<(), NetworkError> {
        self.send_routed_call(target_node_id, DfsRoutedCall::value(message))
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

    /// Fetch one bounded, authority-stamped content-root catalog page.
    ///
    /// Production node transports override this with an authenticated DFS
    /// object-RPC call. Message-only and rolling-upgrade transports return
    /// `None`; callers must treat that as incomplete catalog coverage and must
    /// not run destructive content collection.
    fn request_content_root_catalog_page_direct(
        &mut self,
        _target_node_id: &NodeId,
        _request: &DfsContentRootCatalogPageRequest,
    ) -> Result<Option<DfsContentRootCatalogPage>, NetworkError> {
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

    /// Returns `true` when a completed routed-write response belongs to a
    /// different caller than the request currently attached to this
    /// transport. Served transports use this to move unrelated response RPCs
    /// into their detached outbox instead of holding the service lane.
    fn should_detach_completed_response(&self, _target_node_id: &NodeId) -> bool {
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

    fn send_routed_call_async<'a>(
        &'a mut self,
        target_node_id: &'a NodeId,
        call: DfsRoutedCall,
    ) -> DfsMessageTransportFuture<'a, Result<(), NetworkError>>;

    /// Async counterpart of
    /// [`DfsMessageTransport::send_routed_calls_bounded`].
    fn send_routed_calls_bounded_async<'a>(
        &'a mut self,
        requests: Vec<DfsRoutedSend>,
        _max_inflight: usize,
    ) -> DfsMessageTransportFuture<'a, Result<Vec<DfsRoutedSendOutcome>, NetworkError>> {
        Box::pin(async move {
            let mut outcomes = Vec::with_capacity(requests.len());
            for request in requests {
                let result = self
                    .send_routed_call_async(&request.target_node_id, request.call)
                    .await;
                outcomes.push(DfsRoutedSendOutcome {
                    target_node_id: request.target_node_id,
                    result,
                });
            }
            Ok(outcomes)
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

fn collect_dfs_outbound_attachment_sync(
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
            operation: "collect DFS outbound attachment".to_string(),
            detail: "sync DFS attachment fallback cannot run inside a current-thread Tokio runtime"
                .to_string(),
        });
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| NetworkError::Io {
            operation: "create DFS attachment collection runtime".to_string(),
            detail: source.to_string(),
        })?;
    let result = runtime.block_on(source.collect_to_vec(max_bytes));
    runtime.shutdown_timeout(Duration::from_millis(250));
    result
}

fn dfs_attachment_size_for_platform(
    attachment: &DfsOutboundAttachment,
) -> Result<usize, NetworkError> {
    usize::try_from(attachment.descriptor.total_size_bytes).map_err(|_| {
        NetworkError::ResourceExhausted {
            operation: "materialize DFS attachment".to_string(),
            requested_bytes: usize::MAX,
        }
    })
}

pub(crate) fn materialize_dfs_routed_call_sync(
    call: DfsRoutedCall,
) -> Result<DfsRoutedMessage, NetworkError> {
    let DfsRoutedCall {
        message,
        attachment,
    } = call;
    let Some(attachment) = attachment else {
        return Ok(message);
    };
    let max_bytes = dfs_attachment_size_for_platform(&attachment)?;
    let bytes = collect_dfs_outbound_attachment_sync(attachment.source, max_bytes)?;
    crate::body_rpc::attach_dfs_routed_message_attachment(message, attachment.descriptor, bytes)
}

pub(crate) async fn materialize_dfs_routed_call(
    call: DfsRoutedCall,
) -> Result<DfsRoutedMessage, NetworkError> {
    let DfsRoutedCall {
        message,
        attachment,
    } = call;
    let Some(attachment) = attachment else {
        return Ok(message);
    };
    let max_bytes = dfs_attachment_size_for_platform(&attachment)?;
    let bytes = attachment.source.collect_to_vec(max_bytes).await?;
    crate::body_rpc::attach_dfs_routed_message_attachment(message, attachment.descriptor, bytes)
}

impl DfsMessageTransport for sinomo_core::network::InProcessTransport {
    fn send_routed_call(
        &mut self,
        target_node_id: &NodeId,
        call: DfsRoutedCall,
    ) -> Result<(), NetworkError> {
        let message = materialize_dfs_routed_call_sync(call)?;
        let request = dfs_routed_message_rpc_request(&message)?;
        self.send_object_rpc_request(target_node_id, request)
    }

    fn send_routed_calls_bounded(
        &mut self,
        requests: Vec<DfsRoutedSend>,
        max_inflight: usize,
    ) -> Result<Vec<DfsRoutedSendOutcome>, NetworkError> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        if requests.len() == 1 {
            let request = requests.into_iter().next().expect("one routed call");
            let result = self.send_routed_call(&request.target_node_id, request.call);
            return Ok(vec![DfsRoutedSendOutcome {
                target_node_id: request.target_node_id,
                result,
            }]);
        }
        let worker_count = max_inflight.max(1).min(requests.len());
        let pending = Mutex::new(VecDeque::from_iter(requests.into_iter().enumerate()));
        let completed = Mutex::new(Vec::new());

        std::thread::scope(|scope| -> Result<(), NetworkError> {
            let mut workers = Vec::with_capacity(worker_count);
            for _ in 0..worker_count {
                let pending = &pending;
                let completed = &completed;
                let mut transport = self.clone();
                workers.push(scope.spawn(move || {
                    loop {
                        let Some((index, request)) = pending
                            .lock()
                            .expect("in-process DFS call fan-out queue")
                            .pop_front()
                        else {
                            break;
                        };
                        let result =
                            transport.send_routed_call(&request.target_node_id, request.call);
                        completed
                            .lock()
                            .expect("in-process DFS call fan-out results")
                            .push((
                                index,
                                DfsRoutedSendOutcome {
                                    target_node_id: request.target_node_id,
                                    result,
                                },
                            ));
                    }
                }));
            }
            for worker in workers {
                worker.join().map_err(|_| NetworkError::Io {
                    operation: "join in-process DFS call fan-out".to_string(),
                    detail: "in-process DFS call fan-out worker panicked".to_string(),
                })?;
            }
            Ok(())
        })?;

        let mut completed = completed.into_inner().map_err(|_| NetworkError::Io {
            operation: "collect in-process DFS call fan-out".to_string(),
            detail: "in-process DFS call fan-out result lock is poisoned".to_string(),
        })?;
        completed.sort_by_key(|(index, _)| *index);
        Ok(completed.into_iter().map(|(_, outcome)| outcome).collect())
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

    fn send_routed_call_async<'a>(
        &'a mut self,
        target_node_id: &'a NodeId,
        call: DfsRoutedCall,
    ) -> DfsMessageTransportFuture<'a, Result<(), NetworkError>> {
        Box::pin(async move {
            let message = materialize_dfs_routed_call(call).await?;
            let request = dfs_routed_message_rpc_request(&message)?;
            self.send_object_rpc_request_async(
                target_node_id,
                request,
                sinomo_core::network::AsyncNetworkSendOptions::default(),
            )
            .await
        })
    }

    fn send_routed_calls_bounded_async<'a>(
        &'a mut self,
        requests: Vec<DfsRoutedSend>,
        max_inflight: usize,
    ) -> DfsMessageTransportFuture<'a, Result<Vec<DfsRoutedSendOutcome>, NetworkError>> {
        Box::pin(async move {
            if requests.is_empty() {
                return Ok(Vec::new());
            }
            if requests.len() == 1 {
                let request = requests.into_iter().next().expect("one routed call");
                let result = self
                    .send_routed_call_async(&request.target_node_id, request.call)
                    .await;
                return Ok(vec![DfsRoutedSendOutcome {
                    target_node_id: request.target_node_id,
                    result,
                }]);
            }

            let max_inflight = max_inflight.max(1).min(requests.len());
            // The transport owns its receive queue and is therefore not cloned. Its outbound
            // object-RPC path only borrows shared channel handles, so locally polling borrowed
            // send futures gives the same bounded overlap without splitting receive ownership.
            let transport = &*self;
            let mut pending = VecDeque::from_iter(requests.into_iter().enumerate());
            let mut active: Vec<DfsMessageTransportFuture<'_, (usize, DfsRoutedSendOutcome)>> =
                Vec::with_capacity(max_inflight);
            let mut completed = Vec::with_capacity(pending.len());

            while !pending.is_empty() || !active.is_empty() {
                while active.len() < max_inflight {
                    let Some((index, request)) = pending.pop_front() else {
                        break;
                    };
                    active.push(Box::pin(async move {
                        let result = send_async_in_process_routed_call(
                            transport,
                            &request.target_node_id,
                            request.call,
                        )
                        .await;
                        (
                            index,
                            DfsRoutedSendOutcome {
                                target_node_id: request.target_node_id,
                                result,
                            },
                        )
                    }));
                }

                let (active_index, completion) = std::future::poll_fn(|context| {
                    for (active_index, future) in active.iter_mut().enumerate() {
                        if let std::task::Poll::Ready(completion) = future.as_mut().poll(context) {
                            return std::task::Poll::Ready((active_index, completion));
                        }
                    }
                    std::task::Poll::Pending
                })
                .await;
                drop(active.swap_remove(active_index));
                completed.push(completion);
            }

            completed.sort_by_key(|(index, _)| *index);
            Ok(completed.into_iter().map(|(_, outcome)| outcome).collect())
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

async fn send_async_in_process_routed_call(
    transport: &sinomo_core::network::AsyncInProcessTransport,
    target_node_id: &NodeId,
    call: DfsRoutedCall,
) -> Result<(), NetworkError> {
    let message = materialize_dfs_routed_call(call).await?;
    let request = dfs_routed_message_rpc_request(&message)?;
    transport
        .send_object_rpc_request_async(
            target_node_id,
            request,
            sinomo_core::network::AsyncNetworkSendOptions::default(),
        )
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DfsFileSystemId, DfsNetworkMessage, DfsObjectId, DfsReadRequest, DfsStageBlobCommand,
        DfsVersion, content_hash,
    };
    use sinomo_core::network::AsyncInProcessNetwork;

    fn node_id(value: &str) -> NodeId {
        NodeId::from_legacy_named(value).expect("node id")
    }

    fn read_message(
        source_node_id: &NodeId,
        target_node_id: &NodeId,
        frame_id: u64,
    ) -> DfsRoutedMessage {
        DfsRoutedMessage {
            source_node_id: source_node_id.clone(),
            target_node_id: target_node_id.clone(),
            frame_id,
            message_id: frame_id,
            message: DfsNetworkMessage::ReadRequest(DfsReadRequest {
                request_id: format!("prefill-{frame_id}"),
                object_id: DfsObjectId::from_legacy_named(format!("prefill-object-{frame_id}"))
                    .expect("object id"),
                version: None,
                content_hash: None,
                content_size_bytes: None,
                range_offset: None,
                range_max_bytes: None,
            }),
        }
    }

    fn attachment_send(
        source_node_id: &NodeId,
        target_node_id: &NodeId,
        frame_id: u64,
    ) -> DfsRoutedSend {
        let bytes = vec![u8::try_from(frame_id).unwrap_or_default(); 16];
        let hash = content_hash(&bytes);
        DfsRoutedSend::new(
            target_node_id.clone(),
            DfsRoutedCall::with_attachment(
                DfsRoutedMessage {
                    source_node_id: source_node_id.clone(),
                    target_node_id: target_node_id.clone(),
                    frame_id,
                    message_id: frame_id,
                    message: DfsNetworkMessage::StageBlob(DfsStageBlobCommand {
                        request_id: format!("stage-{frame_id}"),
                        intent_id: format!("intent-{frame_id}"),
                        file_system_id: DfsFileSystemId::from_legacy_named("fanout-test-sifs")
                            .expect("file system id"),
                        object_id: DfsObjectId::from_legacy_named(format!(
                            "fanout-object-{frame_id}"
                        ))
                        .expect("object id"),
                        expected_base_version: None,
                        version: DfsVersion::from_legacy_named(format!(
                            "fanout-version-{frame_id}"
                        ))
                        .expect("version"),
                        content_hash: hash.clone(),
                        bytes: Vec::new(),
                        object_metadata: None,
                        authority_shard: None,
                    }),
                },
                DfsOutboundAttachment::from_bytes(bytes, hash),
            ),
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_in_process_call_fanout_honors_cap_and_preserves_ordered_failures() {
        let network = AsyncInProcessNetwork::with_mailbox_capacity_and_per_target_send_limit(1, 1);
        let local = node_id("async-attachment-fanout-local");
        let blocked = node_id("async-attachment-fanout-blocked");
        let releasable = node_id("async-attachment-fanout-releasable");
        let next = node_id("async-attachment-fanout-next");
        let missing = node_id("async-attachment-fanout-missing");
        let mut sender = network.add_node(local.clone());
        let mut blocked_receiver = network.add_node(blocked.clone());
        let mut releasable_receiver = network.add_node(releasable.clone());
        let mut next_receiver = network.add_node(next.clone());

        AsyncDfsMessageTransport::send_routed_call_async(
            &mut sender,
            &blocked,
            DfsRoutedCall::value(read_message(&local, &blocked, 1)),
        )
        .await
        .expect("prefill blocked mailbox");
        AsyncDfsMessageTransport::send_routed_call_async(
            &mut sender,
            &releasable,
            DfsRoutedCall::value(read_message(&local, &releasable, 2)),
        )
        .await
        .expect("prefill releasable mailbox");

        let requests = vec![
            attachment_send(&local, &blocked, 10),
            attachment_send(&local, &releasable, 11),
            attachment_send(&local, &next, 12),
            attachment_send(&local, &missing, 13),
        ];
        let fanout = tokio::spawn(async move {
            AsyncDfsMessageTransport::send_routed_calls_bounded_async(&mut sender, requests, 2)
                .await
        });

        assert!(
            !AsyncDfsMessageTransport::wait_for_incoming_async(
                &mut next_receiver,
                Duration::from_millis(50),
            )
            .await
            .expect("check capped third send"),
            "the third send must not start while both permits are occupied"
        );

        let prefill = releasable_receiver
            .drain_incoming_object_rpc_requests(1)
            .await
            .expect("release second mailbox");
        assert_eq!(prefill.len(), 1);
        assert!(
            AsyncDfsMessageTransport::wait_for_incoming_async(
                &mut next_receiver,
                Duration::from_secs(1),
            )
            .await
            .expect("wait for third send"),
            "releasing the second send must let the third overlap the blocked first send"
        );

        let prefill = blocked_receiver
            .drain_incoming_object_rpc_requests(1)
            .await
            .expect("release first mailbox");
        assert_eq!(prefill.len(), 1);

        let outcomes = tokio::time::timeout(Duration::from_secs(1), fanout)
            .await
            .expect("fan-out completion")
            .expect("fan-out task")
            .expect("fan-out orchestration");
        assert_eq!(
            outcomes
                .iter()
                .map(|outcome| outcome.target_node_id.clone())
                .collect::<Vec<_>>(),
            vec![
                blocked.clone(),
                releasable.clone(),
                next.clone(),
                missing.clone()
            ]
        );
        assert!(outcomes[0].result.is_ok());
        assert!(outcomes[1].result.is_ok());
        assert!(outcomes[2].result.is_ok());
        assert!(matches!(
            &outcomes[3].result,
            Err(NetworkError::UnknownNode { node_id }) if node_id == &missing
        ));

        for receiver in [
            &mut blocked_receiver,
            &mut releasable_receiver,
            &mut next_receiver,
        ] {
            let messages = AsyncDfsMessageTransport::drain_incoming_messages_async(receiver)
                .await
                .expect("drain fan-out attachment message");
            assert_eq!(messages.len(), 1);
        }
    }
}
