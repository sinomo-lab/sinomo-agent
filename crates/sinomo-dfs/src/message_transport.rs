//! DFS message transport contracts.
//!
//! These traits adapt generic `sinomo-network` object-RPC transports to the
//! typed DFS message plane. Higher-level DFS services depend on these traits,
//! not on a concrete transport implementation.

use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::mpsc::{Receiver, Sender};
use std::time::{Duration, Instant};

use sinomo_core::network::{NetworkError, ObjectRpcBodyProgress, ObjectRpcBodySource};
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

/// Preserve the same normal message route while moving a native snapshot's
/// captured file resource into its streaming body. Data heads never restore a
/// body by guessing a path or deserializing a full-table image.
pub(crate) fn native_snapshot_routed_call(
    message: DfsRoutedMessage,
) -> Result<DfsRoutedCall, crate::DfsError> {
    if let crate::DfsNetworkMessage::MetadataSnapshotInstall(command) = &message.message {
        let image = command.image()?;
        let attachment = DfsOutboundAttachment {
            descriptor: DfsRpcAttachmentDescriptor {
                total_size_bytes: image.total_size_bytes,
                content_hash: None,
            },
            source: image
                .attachment_source()
                .map_err(crate::DfsError::from_network)?,
        };
        Ok(DfsRoutedCall::with_attachment(message, attachment))
    } else {
        Ok(DfsRoutedCall::value(message))
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

/// One immutable metadata journal command shared by every target at the same
/// replica cursor.
///
/// Authority prepares can be several MiB. Keeping the target set beside one
/// owned command lets production transports encode and replay that value once
/// without first deep-cloning it into a command per replica. A transport that
/// cannot retain shared replayable values returns `None`; the service then
/// sends each target its prepare over the ordinary routed path.
#[derive(Debug)]
pub struct DfsMetadataJournalBatchDirectGroup {
    pub target_node_ids: Vec<NodeId>,
    pub command: DfsMetadataJournalBatchCommand,
}

impl DfsMetadataJournalBatchDirectGroup {
    pub fn new(target_node_ids: Vec<NodeId>, command: DfsMetadataJournalBatchCommand) -> Self {
        Self {
            target_node_ids,
            command,
        }
    }
}

/// Completion of one call started on a [`DfsMetadataAuthorityFanout`].
#[derive(Debug)]
pub enum DfsMetadataAuthorityFanoutEvent {
    /// The canonical metadata route returned, or failed, for one target.
    Direct {
        target_node_id: NodeId,
        result: Result<DfsMetadataJournalBatchResponse, NetworkError>,
    },
    /// One routed DFS message call returned. Its reply messages are not yet
    /// processed; the owner hands them to
    /// [`DfsMessageTransport::deliver_metadata_authority_fanout_replies`].
    Routed {
        target_node_id: NodeId,
        result: Result<Vec<DfsRoutedMessage>, NetworkError>,
    },
}

/// One fan-out channel item: a call completion, or `None` to wake the owner.
pub(crate) type DfsMetadataAuthorityFanoutSignal = Option<DfsMetadataAuthorityFanoutEvent>;

/// Transport-owned workers behind one [`DfsMetadataAuthorityFanout`].
pub(crate) trait DfsMetadataAuthorityFanoutWorkers: Send {
    /// Start one routed call bounded by `deadline` that reports exactly one
    /// event. Returns the message when no worker can run it.
    fn start_routed(
        &mut self,
        message: DfsRoutedMessage,
        deadline: Instant,
        events: &Sender<DfsMetadataAuthorityFanoutSignal>,
    ) -> Result<(), DfsRoutedMessage>;

    /// Stop every call still in flight.
    fn abort(&mut self);
}

/// The metadata authority calls in flight for one prepare round.
///
/// Every call carries the same network deadline and reports exactly one
/// event. Calls run until they finish or that deadline expires; the owner's
/// decision to hedge never cancels them. Dropping the fan-out abandons every
/// call still in flight.
pub struct DfsMetadataAuthorityFanout {
    deadline: Instant,
    direct_calls: BTreeMap<NodeId, ObjectRpcBodyProgress>,
    in_flight: usize,
    events_tx: Sender<DfsMetadataAuthorityFanoutSignal>,
    events: Receiver<DfsMetadataAuthorityFanoutSignal>,
    workers: Box<dyn DfsMetadataAuthorityFanoutWorkers>,
}

impl DfsMetadataAuthorityFanout {
    /// `direct_calls` names the direct calls already started with senders
    /// cloned from `events_tx`, each with the progress of its request upload.
    pub(crate) fn new(
        deadline: Instant,
        direct_calls: BTreeMap<NodeId, ObjectRpcBodyProgress>,
        events_tx: Sender<DfsMetadataAuthorityFanoutSignal>,
        events: Receiver<DfsMetadataAuthorityFanoutSignal>,
        workers: Box<dyn DfsMetadataAuthorityFanoutWorkers>,
    ) -> Self {
        Self {
            deadline,
            in_flight: direct_calls.len(),
            direct_calls,
            events_tx,
            events,
            workers,
        }
    }

    /// The network deadline shared by every call on this fan-out.
    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    /// Whether a direct call to `target_node_id` was started.
    pub fn has_direct_call(&self, target_node_id: &NodeId) -> bool {
        self.direct_calls.contains_key(target_node_id)
    }

    /// Request-body bytes of the direct call to `target_node_id` handed to the
    /// network so far.
    pub fn direct_upload_bytes(&self, target_node_id: &NodeId) -> Option<u64> {
        self.direct_calls
            .get(target_node_id)
            .map(ObjectRpcBodyProgress::bytes)
    }

    /// Ends the current or next [`Self::next_event`] wait early.
    pub(crate) fn waker(&self) -> crate::service_state::DfsCancellationWaker {
        let events = self.events_tx.clone();
        std::sync::Arc::new(move || {
            let _ = events.send(None);
        })
    }

    /// Calls whose completion event has not been returned yet.
    pub fn in_flight(&self) -> usize {
        self.in_flight
    }

    pub(crate) fn start_routed(
        &mut self,
        message: DfsRoutedMessage,
    ) -> Result<(), DfsRoutedMessage> {
        self.workers
            .start_routed(message, self.deadline, &self.events_tx)?;
        self.in_flight += 1;
        Ok(())
    }

    /// Wait for the next completion. Returns `None` at `until`, when no call
    /// remains in flight, or when a [`Self::waker`] fires.
    pub fn next_event(&mut self, until: Instant) -> Option<DfsMetadataAuthorityFanoutEvent> {
        if self.in_flight == 0 {
            return None;
        }
        let timeout = until.saturating_duration_since(Instant::now());
        let received = match tokio::runtime::Handle::try_current() {
            Ok(runtime)
                if matches!(
                    runtime.runtime_flavor(),
                    tokio::runtime::RuntimeFlavor::MultiThread
                ) =>
            {
                tokio::task::block_in_place(|| self.events.recv_timeout(timeout))
            }
            _ => self.events.recv_timeout(timeout),
        };
        let event = received.ok()??;
        self.in_flight -= 1;
        Some(event)
    }
}

impl Drop for DfsMetadataAuthorityFanout {
    fn drop(&mut self) {
        self.workers.abort();
    }
}

impl std::fmt::Debug for DfsMetadataAuthorityFanout {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DfsMetadataAuthorityFanout")
            .field("deadline", &self.deadline)
            .field("direct_target_node_ids", &self.direct_calls.keys())
            .field("in_flight", &self.in_flight)
            .finish_non_exhaustive()
    }
}

impl DfsOutboundAttachment {
    pub(crate) fn from_quarantined_patch(
        blob: std::sync::Arc<crate::content_store::ContentStoreQuarantinedBlob>,
    ) -> Result<Self, crate::DfsError> {
        let reader = blob.open_patch_reader()?;
        Ok(Self {
            descriptor: DfsRpcAttachmentDescriptor {
                total_size_bytes: blob.size_bytes,
                content_hash: Some(blob.content_hash.clone()),
            },
            source: ObjectRpcBodySource::from_reader(reader, Some(blob.size_bytes))
                .with_lifetime_guard(blob)
                .map_err(crate::DfsError::from_network)?,
        })
    }

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

    pub fn from_shared_bytes_with_transport_integrity(bytes: bytes::Bytes) -> Self {
        Self {
            descriptor: DfsRpcAttachmentDescriptor {
                total_size_bytes: bytes.len() as u64,
                content_hash: None,
            },
            source: ObjectRpcBodySource::shared_bytes(bytes),
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

pub trait DfsMessageTransport: DfsTransportIdentity {
    /// A mounted client shares its node identity with the resident authority,
    /// but must query that authority rather than treating its private metadata
    /// image as the local primary.
    fn delegates_local_namespace_authority(&self) -> bool {
        false
    }

    /// A shard-bound Core may read another physical scope on the same node
    /// only through its authenticated registered Main. This is one read-only
    /// command, not a role change or permission to drain incoming mutations.
    fn send_local_metadata_query_to_main(
        &mut self,
        _message: crate::DfsRoutedMessage,
    ) -> Result<(), sinomo_core::network::NetworkError> {
        Err(sinomo_core::network::NetworkError::Io {
            operation: "local metadata query to Main".into(),
            detail: "transport has no authenticated Main query sender".into(),
        })
    }

    /// Runtime-local response custody only. The service retains authority checks.
    fn register_routed_write_response_owner(
        &mut self,
        _canonical: &str,
        _object_id: &crate::DfsObjectId,
        _aliases: &[String],
    ) -> Result<(), sinomo_core::network::NetworkError> {
        Ok(())
    }
    fn release_routed_write_response_owner(&mut self, _canonical: &str) {}
    fn routed_write_response_registration_checkpoint(&self, _canonical: &str) -> usize {
        0
    }
    fn rollback_routed_write_response_registration(
        &mut self,
        _canonical: &str,
        _checkpoint: usize,
    ) {
    }

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

    /// Served background repair owners can hand owned attachment calls to the
    /// existing detached worker after their mutation admission is released.
    fn supports_background_replica_dispatch(&self) -> bool {
        false
    }

    fn prepare_background_replica_dispatch(
        &mut self,
        _requests: Vec<DfsRoutedSend>,
    ) -> Result<(), NetworkError> {
        Err(NetworkError::Io {
            operation: "prepare background DFS replica dispatch".to_string(),
            detail: "transport has no detached replica dispatch owner".to_string(),
        })
    }

    /// Transfer prepared payloads to their retained worker only after the
    /// maintenance owner has durably stored the matching repair attempts.
    fn checkpoint_background_replica_dispatch(&mut self) -> Result<(), NetworkError> {
        Ok(())
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

    /// Start one metadata authority prepare round on the canonical metadata
    /// route and return without waiting for replies.
    ///
    /// Each group is encoded once and replayed to its targets. `deadline` is
    /// every call's whole network deadline. The default reports the direct
    /// route as unavailable, so message-only and rolling-upgrade transports
    /// keep the caller's per-target routed prepare and its target-unique
    /// request identities.
    fn start_metadata_authority_fanout(
        &mut self,
        _groups: &[DfsMetadataJournalBatchDirectGroup],
        _deadline: Instant,
    ) -> Result<Option<DfsMetadataAuthorityFanout>, NetworkError> {
        Ok(None)
    }

    /// Start one routed DFS message call on `fanout`, so its reply arrives as a
    /// fan-out event bounded by the fan-out's deadline. Returns the message
    /// when this transport cannot run it there; the caller then uses its
    /// ordinary routed send.
    fn send_routed_message_on_metadata_authority_fanout(
        &mut self,
        _fanout: &mut DfsMetadataAuthorityFanout,
        message: DfsRoutedMessage,
    ) -> Result<Option<DfsRoutedMessage>, NetworkError> {
        Ok(Some(message))
    }

    /// Deliver the messages returned by one routed fan-out call through the
    /// same inbound custody as the replies of an ordinary routed send.
    fn deliver_metadata_authority_fanout_replies(
        &mut self,
        _messages: Vec<DfsRoutedMessage>,
    ) -> Result<(), NetworkError> {
        Err(NetworkError::Io {
            operation: "deliver DFS metadata authority fan-out replies".to_string(),
            detail: "transport starts no metadata authority fan-out".to_string(),
        })
    }

    /// Take only the requested namespace response from this transport's real
    /// inbox. Unrelated requests and replies retain their existing custody.
    /// This grants no authority; the caller revalidates the dispatch assignment.
    fn take_namespace_response(
        &mut self,
        _request_id: &str,
    ) -> Result<Option<DfsRoutedMessage>, NetworkError> {
        Err(NetworkError::Io {
            operation: "receive scoped DFS metadata query".into(),
            detail: "transport has no selective namespace response custody".into(),
        })
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

    /// True while this transport serves a routed request under the host's
    /// admission: a wait here holds that admission.
    fn serves_routed_request(&self) -> bool {
        false
    }

    /// True when the repair executor's sends leave through the detached
    /// worker the host schedules after it releases admission. A transport
    /// with no admitting host sends them directly.
    fn detaches_executor_sends(&self) -> bool {
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

    /// A waker that ends a current or next [`Self::wait_for_incoming`] early.
    /// Transports whose wait cannot be interrupted return `None`; their waits
    /// end only on an arrival or at their timeout.
    fn incoming_wait_waker(&self) -> Option<crate::service_state::DfsCancellationWaker> {
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

/// Materializes one routed call for a synchronous in-process or loopback
/// transport boundary.
///
/// Ordinary value calls return their routed message unchanged. Semantic
/// attachments are collected with the descriptor's exact platform-bounded
/// size, validated, and attached to the typed DFS message. Network transports
/// should keep using the owned body source directly instead of materializing
/// it through this compatibility helper.
pub fn materialize_dfs_routed_call_sync(
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
