//! Node-routed DFS data-plane transport and served-message processing.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{
    AtomicBool, AtomicU64, AtomicUsize as StdAtomicUsize, Ordering as AtomicOrdering,
};
use std::time::{Duration, Instant};

use serde::Serialize;
#[cfg(test)]
use sinomo_core::local::SinomoDataDir;
#[cfg(test)]
use sinomo_core::network::ClusterPaths;
#[cfg(test)]
use sinomo_core::network::ObjectRpcRequest;
use sinomo_core::network::{
    NetworkError, ObjectRpcBodySource, ObjectRpcCall, ObjectRpcIncomingBody,
    ObjectRpcReplayableCall, ObjectRpcResponse, ObjectRpcResult, SinomoObjectCodec,
};
use sinomo_core::node::NodeId;

use crate::{
    AsyncDfsMessageTransport, ContentStoreQuarantinedBlob, DFS_DATA_PLANE_MESSAGE_DRAIN_MAX_PASSES,
    DfsBlobTransferResult, DfsContentRootCatalogPage, DfsContentRootCatalogPageRequest,
    DfsDirectReadClientResponse, DfsDirectReadRequest, DfsError, DfsMessageCommand,
    DfsMessageProcessReport, DfsMessageProcessResult, DfsMessageResponse, DfsMessageTransport,
    DfsMessageTransportFuture, DfsMetadataJournalBatchCommand, DfsMetadataJournalBatchDirectGroup,
    DfsMetadataJournalBatchResponse, DfsNetworkMessage, DfsOutboundAttachment, DfsReadResponse,
    DfsRoutedCall, DfsRoutedMessage, DfsRoutedSend, DfsRoutedSendOutcome, DfsTransportIdentity,
    DistributedRepairJob, NetworkedDfsService, NetworkedDfsServiceEvent,
    decode_dfs_message_rpc_response, decode_dfs_metadata_journal_batch_rpc_response,
    dfs_direct_read_request_from_read_request, dfs_direct_read_response_from_rpc_result,
    dfs_direct_read_rpc_call, dfs_message_is_read_only_request, dfs_message_rpc_call,
    dfs_message_rpc_request, dfs_metadata_journal_batch_replayable_rpc_call,
    dfs_metadata_journal_batch_rpc_call, dfs_network_message_allows_write_repair_progress,
    dfs_perf_trace, dfs_perf_trace_enabled, dfs_rejection_is_commit_content_repair_pending,
    dfs_rejection_is_replica_not_desired, retained_networked_dfs_service_events,
};
#[cfg(test)]
use crate::{
    DfsControlPlaneKvClient, DfsMetadataJournalReplicationOptions,
    DfsMetadataJournalReplicationReport, LocalContentStore,
    open_cluster_aware_dfs_metadata_backend,
    run_cluster_dfs_metadata_publish_after_events_with_peer_transport,
};

const DFS_SERVED_MESSAGE_REPAIR_MAX_JOBS: usize = 2;
/// Refinement of `PromotionFanoutCap` in `replica_quorum_fanout.qnt`.
const DFS_COMMIT_PROMOTION_MAX_INFLIGHT: usize = 2;
/// Process-local terminal promotion identities suppress redundant hydration
/// replay, but durable committed intents and replica receipts own recovery.
/// Keep a recent window rather than retaining one entry for every object ever
/// promoted by this daemon.
const DFS_TERMINAL_COMMIT_PROMOTION_MAX_ENTRIES: usize = 4_096;
const DFS_TERMINAL_COMMIT_PROMOTION_RETAIN_ENTRIES: usize =
    DFS_TERMINAL_COMMIT_PROMOTION_MAX_ENTRIES / 2;
/// Refinement of the bounded metadata-checkpoint fan-out model. Direct
/// checkpoint RPCs are best-effort accelerators: later catch-up retains
/// correctness when a sibling is slow or unavailable.
const DFS_METADATA_CHECKPOINT_DIRECT_MAX_INFLIGHT: usize = 2;
/// A healthy metadata-authority proof is deliberately all-member rather than
/// quorum based. Start one bounded cluster-sized wave so a six-node shard does
/// not serialize required acknowledgements behind the best-effort checkpoint
/// cap and then needlessly fall back to cluster KV.
const DFS_METADATA_AUTHORITY_DIRECT_MAX_INFLIGHT: usize = 8;
/// Keep one slow checkpoint replica from holding a served mutation lane.
const DFS_METADATA_CHECKPOINT_DIRECT_FANOUT_BUDGET: Duration = Duration::from_millis(250);
/// Large namespace prepares use segmented value bodies, but direct fan-out is
/// only an optimization before the exact operation-bound cluster-quorum fence.
/// Bound a wedged sibling independently so it cannot add five seconds before
/// the safe fallback even starts.
const DFS_METADATA_AUTHORITY_DIRECT_FANOUT_BUDGET: Duration = Duration::from_secs(1);

#[cfg(test)]
std::thread_local! {
    /// One-shot delay injected immediately before encoding a distinct direct
    /// metadata value. Tests use it to prove the peer-response budget starts
    /// after local replayable-call construction.
    static DFS_METADATA_DIRECT_ENCODING_TEST_DELAY: std::cell::Cell<Duration> =
        const { std::cell::Cell::new(Duration::ZERO) };
}

#[cfg(test)]
fn take_metadata_direct_encoding_test_delay() -> Duration {
    DFS_METADATA_DIRECT_ENCODING_TEST_DELAY.with(|delay| delay.replace(Duration::ZERO))
}
/// Refinement of retry cooling in `replica_promotion_outbox.qnt`.
///
/// Failed replay-safe post-response work is intentionally ineligible for a
/// bounded exponential interval. New healthy work remains ready and bypasses
/// this cooling queue, so a poison target cannot turn retry pacing into global
/// idleness.
const DFS_DETACHED_SEND_RETRY_DELAY: Duration = Duration::from_secs(5);
const DFS_DETACHED_SEND_RETRY_MAX_EXPONENT: u32 = 6;
/// A deadline-bounded, idempotent DFS mutation must retain enough of its
/// caller budget to ask the authority for the durable result when the first
/// response is lost. Five seconds matches one normal network health phase;
/// short operations reserve one quarter instead of starving the first try.
const DFS_EXACT_RESPONSE_REPLAY_MAX_RESERVE: Duration = Duration::from_secs(5);

fn dfs_detached_send_retry_delay(retry_attempts: u32) -> Duration {
    let exponent = retry_attempts
        .saturating_sub(1)
        .min(DFS_DETACHED_SEND_RETRY_MAX_EXPONENT);
    DFS_DETACHED_SEND_RETRY_DELAY.saturating_mul(1_u32 << exponent)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsPeerTransportError {
    detail: String,
    delivery_state: Option<sinomo_core::network::NetworkDeliveryState>,
    unknown_route: Option<String>,
}

impl DfsPeerTransportError {
    pub fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
            delivery_state: None,
            unknown_route: None,
        }
    }

    pub fn with_delivery_state(
        detail: impl Into<String>,
        delivery_state: Option<sinomo_core::network::NetworkDeliveryState>,
    ) -> Self {
        Self {
            detail: detail.into(),
            delivery_state,
            unknown_route: None,
        }
    }

    pub fn from_network(error: sinomo_core::network::NetworkError) -> Self {
        let delivery_state = error.delivery_state();
        let unknown_route = match &error {
            sinomo_core::network::NetworkError::UnknownRoute { route } => Some(route.clone()),
            _ => None,
        };
        Self {
            detail: error.to_string(),
            delivery_state,
            unknown_route,
        }
    }

    pub fn delivery_state(&self) -> Option<sinomo_core::network::NetworkDeliveryState> {
        self.delivery_state
    }

    pub fn is_unknown_route(&self, expected: &str) -> bool {
        self.unknown_route.as_deref() == Some(expected)
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
        ) -> DfsPeerTransportFuture<
            'a,
            Result<DfsDirectReadClientResponse, DfsPeerTransportError>,
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
        route_options.with_destination_process_affinity(
            sinomo_core::network::DestinationProcessAffinity::MainDaemon,
        )
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

/// Size-independent DFS peer RPC. Ordinary encoded values and explicit
/// attachments share this one transport contract.
pub trait DfsPeerTransport: Send + Sync {
    fn call_object_rpc(
        &self,
        target_node_id: &NodeId,
        call: ObjectRpcCall,
        options: DfsObjectRpcSendOptions,
    ) -> Result<ObjectRpcResult, DfsPeerTransportError>;
}

fn collect_dfs_attachment_source_sync(
    attachment: ObjectRpcBodySource,
    max_bytes: usize,
) -> Result<Vec<u8>, DfsPeerTransportError> {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        if matches!(
            handle.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            return tokio::task::block_in_place(|| {
                handle.block_on(attachment.collect_to_vec(max_bytes))
            })
            .map_err(|source| DfsPeerTransportError::new(source.to_string()));
        }
        return Err(DfsPeerTransportError::new(
            "sync DFS attachment collector cannot run inside a current-thread Tokio runtime",
        ));
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
    let result = runtime
        .block_on(attachment.collect_to_vec(max_bytes))
        .map_err(|source| DfsPeerTransportError::new(source.to_string()));
    runtime.shutdown_timeout(Duration::from_millis(250));
    result
}

/// Materializes an ordinary outgoing value call for synchronous compatibility
/// adapters and in-process transport doubles.
///
/// Production network transports consume the owned body source directly. This
/// helper deliberately rejects semantic attachments so a non-empty owned value
/// body cannot be mistaken for one.
#[cfg(test)]
pub(crate) async fn materialize_dfs_value_call(
    mut call: ObjectRpcCall,
    max_bytes: usize,
) -> Result<ObjectRpcRequest, DfsPeerTransportError> {
    if call.has_streamed_value_body() {
        call.request.body = call
            .body
            .collect_to_vec(max_bytes)
            .await
            .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
        return Ok(call.request);
    }
    if !call.body.is_empty() {
        return Err(DfsPeerTransportError::new(format!(
            "DFS route {} carries a semantic attachment, not an ordinary value",
            call.request.route
        )));
    }
    Ok(call.request)
}

/// Synchronous form of [`materialize_dfs_value_call`].
#[cfg(test)]
pub(crate) fn materialize_dfs_value_call_sync(
    mut call: ObjectRpcCall,
    max_bytes: usize,
) -> Result<ObjectRpcRequest, DfsPeerTransportError> {
    if call.has_streamed_value_body() {
        call.request.body = collect_dfs_attachment_source_sync(call.body, max_bytes)?;
        return Ok(call.request);
    }
    if !call.body.is_empty() {
        return Err(DfsPeerTransportError::new(format!(
            "DFS route {} carries a semantic attachment, not an ordinary value",
            call.request.route
        )));
    }
    Ok(call.request)
}

pub type DfsPeerTransportFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Asynchronous form of the same size-independent DFS peer RPC contract.
pub trait AsyncDfsPeerTransport: Send + Sync {
    fn call_object_rpc_async<'a>(
        &'a self,
        target_node_id: &'a NodeId,
        call: ObjectRpcCall,
        options: DfsObjectRpcSendOptions,
    ) -> DfsPeerTransportFuture<'a, Result<ObjectRpcResult, DfsPeerTransportError>>;
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
            let call = dfs_message_rpc_call(command)
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
            let response = peer_transport
                .call_object_rpc_async(
                    target_node_id,
                    call,
                    options.with_observe_connectivity(!dfs_message_is_read_only_request(command)),
                )
                .await?;
            dfs_message_response_from_rpc_result(response).await
        })
    })
}

/// Asynchronously decodes a canonical DFS session response while its network
/// runtime remains active, including a streamed response attachment.
pub async fn dfs_message_response_from_rpc_result(
    result: ObjectRpcResult,
) -> Result<DfsMessageResponse, DfsPeerTransportError> {
    if result.has_streamed_value() {
        return result
            .decode_body::<DfsMessageResponse>()
            .await
            .map_err(|source| DfsPeerTransportError::new(source.to_string()));
    }
    let (response, attachment) = result.into_parts();
    if attachment.is_empty() {
        return decode_dfs_message_rpc_response(&response)
            .map_err(|source| DfsPeerTransportError::new(source.to_string()));
    }
    let head = crate::body_rpc::decode_dfs_attachment_message_rpc_response(&response)
        .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
    let max_bytes = usize::try_from(head.attachment.total_size_bytes).map_err(|_| {
        DfsPeerTransportError::new(
            "DFS response attachment does not fit this platform address space",
        )
    })?;
    let bytes = attachment
        .collect_to_vec(max_bytes)
        .await
        .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
    crate::body_rpc::attach_dfs_message_response_attachment(head, bytes)
        .map_err(|source| DfsPeerTransportError::new(source.to_string()))
}

/// Encodes one routed DFS call as a stream-native object-RPC call.
///
/// Attachment sources remain streaming bodies owned by the network layer; this
/// helper only encodes the request head and never materializes their bytes.
pub fn dfs_routed_call_rpc_call(
    call: DfsRoutedCall,
) -> Result<(DfsMessageCommand, ObjectRpcCall), NetworkError> {
    let DfsRoutedCall {
        message,
        attachment,
    } = call;
    let command = DfsMessageCommand {
        request_id: format!("dfs_message_{}", message.frame_id),
        routed_message: message,
    };
    let rpc_call = match attachment {
        Some(attachment) if attachment.descriptor.total_size_bytes == 0 => {
            // The object-RPC transport deliberately represents a zero-length
            // body as `None`, not as a semantic attachment. Bind the empty
            // payload into the typed DFS message before encoding so receivers
            // take the ordinary value path rather than trying to decode an
            // attachment wrapper whose body cannot exist on the wire.
            let command = crate::body_rpc::attach_dfs_message_request_attachment(
                crate::DfsAttachmentMessageCommand {
                    command,
                    attachment: attachment.descriptor,
                },
                Vec::new(),
            )?;
            let rpc_call = dfs_message_rpc_call(&command)?;
            return Ok((command, rpc_call));
        }
        Some(attachment) => {
            let request = dfs_message_rpc_request(&command)?;
            let request = crate::body_rpc::encode_dfs_attachment_message_rpc_request(
                &request,
                command.clone(),
                attachment.descriptor,
            )?;
            ObjectRpcCall::new(request, attachment.source)
        }
        None => dfs_message_rpc_call(&command)?,
    };
    Ok((command, rpc_call))
}

pub(crate) fn value_response_from_rpc_result(
    result: ObjectRpcResult,
) -> Result<ObjectRpcResponse, DfsPeerTransportError> {
    let streamed_value = result.has_streamed_value();
    let (mut response, body) = result.into_parts();
    if streamed_value {
        response
            .error_detail()
            .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
        response.body = collect_dfs_incoming_body_sync(body, usize::MAX)?;
        return Ok(response);
    }
    if !body.is_empty() {
        return Err(DfsPeerTransportError::new(format!(
            "DFS route {} returned an unexpected attachment",
            response.route
        )));
    }
    Ok(response)
}

/// Decodes a canonical DFS session response, including a streamed response
/// attachment when the response head declares one.
pub fn dfs_message_response_from_rpc_result_sync(
    result: ObjectRpcResult,
) -> Result<DfsMessageResponse, DfsPeerTransportError> {
    if result.has_streamed_value() {
        let response = value_response_from_rpc_result(result)?;
        return decode_dfs_message_rpc_response(&response)
            .map_err(|source| DfsPeerTransportError::new(source.to_string()));
    }
    let (response, attachment) = result.into_parts();
    if attachment.is_empty() {
        return decode_dfs_message_rpc_response(&response)
            .map_err(|source| DfsPeerTransportError::new(source.to_string()));
    }
    let head = crate::body_rpc::decode_dfs_attachment_message_rpc_response(&response)
        .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
    let max_bytes = usize::try_from(head.attachment.total_size_bytes).map_err(|_| {
        DfsPeerTransportError::new(
            "DFS response attachment does not fit this platform address space",
        )
    })?;
    let bytes = collect_dfs_incoming_body_sync(attachment, max_bytes)?;
    crate::body_rpc::attach_dfs_message_response_attachment(head, bytes)
        .map_err(|source| DfsPeerTransportError::new(source.to_string()))
}

pub(crate) fn collect_dfs_incoming_body_sync(
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
            "sync DFS response body collector cannot run inside a current-thread Tokio runtime",
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

pub(crate) fn async_dfs_direct_read_sender_from_peer_transport(
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
) -> AsyncDfsDirectReadSender {
    Arc::new(move |target_node_id, request, options| {
        let peer_transport = Arc::clone(&peer_transport);
        Box::pin(async move {
            let rpc_call = dfs_direct_read_rpc_call(request)
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
            let response = peer_transport
                .call_object_rpc_async(target_node_id, rpc_call, options)
                .await?;
            dfs_direct_read_response_from_rpc_result(response)
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))
        })
    })
}

pub(crate) fn dfs_message_sender_from_peer_transport(
    peer_transport: Arc<dyn DfsPeerTransport>,
) -> DfsMessageSender {
    Arc::new(move |target_node_id, command, options| {
        let call = dfs_message_rpc_call(command)
            .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
        let response = peer_transport.call_object_rpc(
            target_node_id,
            call,
            options.with_observe_connectivity(!dfs_message_is_read_only_request(command)),
        )?;
        dfs_message_response_from_rpc_result_sync(response)
    })
}

pub(crate) fn dfs_direct_read_sender_from_peer_transport(
    peer_transport: Arc<dyn DfsPeerTransport>,
) -> DfsDirectReadSender {
    Arc::new(move |target_node_id, request, options| {
        let rpc_call = dfs_direct_read_rpc_call(request)
            .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
        let result = peer_transport.call_object_rpc(target_node_id, rpc_call, options)?;
        let response = dfs_direct_read_response_from_rpc_result(result)
            .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
        collect_dfs_direct_read_response_sync(response)
    })
}

fn collect_dfs_direct_read_response_sync(
    response: DfsDirectReadClientResponse,
) -> Result<DfsReadResponse, DfsPeerTransportError> {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        if matches!(
            handle.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            return tokio::task::block_in_place(|| {
                handle.block_on(response.collect_to_read_response())
            })
            .map_err(|source| DfsPeerTransportError::new(source.to_string()));
        }
        return Err(DfsPeerTransportError::new(
            "sync DFS direct-read collector cannot run inside a current-thread Tokio runtime",
        ));
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
    let result = runtime
        .block_on(response.collect_to_read_response())
        .map_err(|source| DfsPeerTransportError::new(source.to_string()));
    runtime.shutdown_timeout(Duration::from_millis(250));
    result
}
#[derive(Clone)]
struct DeferredDfsPeerSend {
    sender: DfsMessageSender,
    options: DfsObjectRpcSendOptions,
    operation_deadline: Option<Instant>,
    target_node_id: NodeId,
    routed_message: DfsRoutedMessage,
    promotion_sequence: Option<u64>,
    // Process-local pacing only. Durable intent/receipt hydration reconstructs
    // outstanding promotions at tier zero after restart.
    retry_attempts: u32,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DfsCommitPromotionIdentity {
    target_node_id: NodeId,
    intent_id: String,
    file_system_id: crate::DfsFileSystemId,
    object_id: crate::DfsObjectId,
    version: crate::DfsVersion,
    content_hash: String,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DfsCommitPromotionSettlementKey {
    target_node_id: NodeId,
    file_system_id: crate::DfsFileSystemId,
    object_id: crate::DfsObjectId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DfsCommitPromotionTerminalOutcome {
    Committed,
    RetiredNotDesired,
    RepairPending,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DfsTerminalCommitPromotion {
    identity: DfsCommitPromotionIdentity,
    enqueue_sequence: u64,
    outcome: DfsCommitPromotionTerminalOutcome,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DfsCommitPromotionResponseDisposition {
    Committed,
    RetireNotDesired,
    RepairPending,
}

#[derive(Default)]
struct DfsNodeDetachedSendOutbox {
    ready_sends: VecDeque<DeferredDfsPeerSend>,
    cooling_retries: BTreeMap<Instant, VecDeque<DeferredDfsPeerSend>>,
    pending_commit_promotions: BTreeSet<DfsCommitPromotionIdentity>,
    terminal_commit_promotions:
        BTreeMap<DfsCommitPromotionSettlementKey, DfsTerminalCommitPromotion>,
    next_promotion_sequence: u64,
}

impl DfsNodeDetachedSendOutbox {
    fn compact_terminal_commit_promotions_if_needed(&mut self) {
        if self.terminal_commit_promotions.len() <= DFS_TERMINAL_COMMIT_PROMOTION_MAX_ENTRIES {
            return;
        }

        // Terminal outcomes are process-local replay suppression, not recovery
        // evidence. A committed target is idempotent, not-desired retirement
        // is authority-derived, and repair-pending has already durably removed
        // the stale receipt. Forgetting an old entry can therefore only permit
        // one safe replay reconstructed from the durable intent/receipt state.
        // Compact with hysteresis so high-churn workloads do not sort the
        // bounded window after every newly settled object.
        let mut retained = std::mem::take(&mut self.terminal_commit_promotions)
            .into_iter()
            .collect::<Vec<_>>();
        retained.sort_unstable_by_key(|entry| std::cmp::Reverse(entry.1.enqueue_sequence));
        retained.truncate(DFS_TERMINAL_COMMIT_PROMOTION_RETAIN_ENTRIES);
        self.terminal_commit_promotions.extend(retained);
    }
}

/// Queue and replay indexes shared by DFS served-message transports.
///
/// Routing capabilities, operation deadlines, and returned-message queues stay
/// on each transport. Sharing this state only makes detached, replay-safe work
/// visible to a common recovery worker and keeps its deduplication atomic.
#[derive(Clone, Default)]
pub struct DfsNodeDetachedSendState {
    inner: Arc<std::sync::Mutex<DfsNodeDetachedSendOutbox>>,
    drain_gate: Arc<std::sync::Mutex<()>>,
    change_generation: Arc<AtomicU64>,
    ready_notify: Arc<tokio::sync::Notify>,
}

impl DfsNodeDetachedSendState {
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(test)]
    pub(crate) fn queued_send_count(&self) -> usize {
        let outbox = self.inner.lock().expect("detached send outbox");
        outbox.ready_sends.len()
            + outbox
                .cooling_retries
                .values()
                .map(VecDeque::len)
                .sum::<usize>()
    }

    fn record_change(&self) {
        self.change_generation.fetch_add(1, AtomicOrdering::AcqRel);
        // `notify_waiters` deliberately does not retain a stale permit. The
        // generation recheck in `wait_for_ready_or_retry` closes the race
        // between observing the queue and registering this notification.
        self.ready_notify.notify_waiters();
    }
}

impl std::fmt::Debug for DfsNodeDetachedSendState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.inner.lock() {
            Ok(outbox) => formatter
                .debug_struct("DfsNodeDetachedSendState")
                .field("ready_sends", &outbox.ready_sends.len())
                .field("cooling_retry_deadlines", &outbox.cooling_retries.len())
                .field(
                    "pending_commit_promotions",
                    &outbox.pending_commit_promotions.len(),
                )
                .field(
                    "settled_commit_promotions",
                    &outbox
                        .terminal_commit_promotions
                        .values()
                        .filter(|promotion| {
                            promotion.outcome == DfsCommitPromotionTerminalOutcome::Committed
                        })
                        .count(),
                )
                .field(
                    "retired_not_desired_commit_promotions",
                    &outbox
                        .terminal_commit_promotions
                        .values()
                        .filter(|promotion| {
                            promotion.outcome
                                == DfsCommitPromotionTerminalOutcome::RetiredNotDesired
                        })
                        .count(),
                )
                .field(
                    "repair_pending_commit_promotions",
                    &outbox
                        .terminal_commit_promotions
                        .values()
                        .filter(|promotion| {
                            promotion.outcome == DfsCommitPromotionTerminalOutcome::RepairPending
                        })
                        .count(),
                )
                .finish(),
            Err(_) => formatter
                .debug_struct("DfsNodeDetachedSendState")
                .field("poisoned", &true)
                .finish(),
        }
    }
}

impl DfsCommitPromotionIdentity {
    fn settlement_key(&self) -> DfsCommitPromotionSettlementKey {
        DfsCommitPromotionSettlementKey {
            target_node_id: self.target_node_id.clone(),
            file_system_id: self.file_system_id.clone(),
            object_id: self.object_id.clone(),
        }
    }
}

impl DeferredDfsPeerSend {
    fn record_retry_failure(&mut self) -> Duration {
        self.retry_attempts = self.retry_attempts.saturating_add(1);
        dfs_detached_send_retry_delay(self.retry_attempts)
    }

    fn is_commit_version_promotion(&self) -> bool {
        matches!(
            &self.routed_message.message,
            DfsNetworkMessage::CommitVersion(_)
        )
    }

    fn commit_version_promotion_identity(&self) -> Option<DfsCommitPromotionIdentity> {
        let DfsNetworkMessage::CommitVersion(command) = &self.routed_message.message else {
            return None;
        };
        Some(DfsCommitPromotionIdentity {
            target_node_id: self.target_node_id.clone(),
            intent_id: command.intent_id.clone(),
            file_system_id: command.file_system_id.clone(),
            object_id: command.object_id.clone(),
            version: command.version.clone(),
            content_hash: command.content_hash.clone(),
        })
    }

    fn is_replay_safe_completed_write_response(&self) -> bool {
        matches!(
            &self.routed_message.message,
            DfsNetworkMessage::DistributedWriteResponse(_)
        )
    }

    fn uses_fresh_detached_budget(&self) -> bool {
        self.is_commit_version_promotion() || self.is_replay_safe_completed_write_response()
    }
}

fn all_commit_version_response_identity_matches(
    send: &DeferredDfsPeerSend,
    command: &crate::DfsCommitVersionCommand,
    message: &DfsRoutedMessage,
    response: &crate::DfsCommitVersionResponse,
) -> bool {
    message.source_node_id == send.target_node_id
        && message.target_node_id == send.routed_message.source_node_id
        && response.request_id == command.request_id
        && response.intent_id == command.intent_id
        && response.file_system_id == command.file_system_id
        && response.object_id == command.object_id
        && response.version == command.version
        && response.replica_node_id == send.target_node_id
}

struct DfsPreparedMetadataJournalBatchRpc {
    target_node_id: NodeId,
    request_id: String,
    shard_id: crate::DfsMetadataShardId,
    next_after_sequence: u64,
    authority_fanout: bool,
    replayable_call: Arc<ObjectRpcReplayableCall>,
    options: DfsObjectRpcSendOptions,
}

struct DfsBorrowedMetadataJournalBatchDirectGroup<'a> {
    target_node_ids: Vec<&'a NodeId>,
    command: &'a DfsMetadataJournalBatchCommand,
}

impl DfsPreparedMetadataJournalBatchRpc {
    fn call(&self) -> Result<ObjectRpcCall, NetworkError> {
        Ok(self.replayable_call.call())
    }

    fn finish(
        self,
        rpc_response: Result<ObjectRpcResult, DfsPeerTransportError>,
    ) -> Result<(NodeId, DfsMetadataJournalBatchResponse), NetworkError> {
        let rpc_response = rpc_response.map_err(|source| NetworkError::Io {
            operation: format!(
                "send direct DFS metadata journal batch to {}",
                self.target_node_id
            ),
            detail: source.to_string(),
        })?;
        let rpc_response =
            value_response_from_rpc_result(rpc_response).map_err(|source| NetworkError::Io {
                operation: format!(
                    "decode direct DFS metadata journal response from {}",
                    self.target_node_id
                ),
                detail: source.to_string(),
            })?;
        let response = decode_dfs_metadata_journal_batch_rpc_response(&rpc_response)?;
        if response.request_id != self.request_id
            || response.shard_id != self.shard_id
            || response.next_after_sequence != self.next_after_sequence
        {
            return Err(NetworkError::Serialization {
                detail: format!(
                    "{} returned mismatched direct metadata journal response",
                    self.target_node_id
                ),
            });
        }
        Ok((self.target_node_id, response))
    }
}

#[derive(Clone)]
pub struct DfsNodeTransport {
    local_node_id: NodeId,
    known_remote_nodes: BTreeSet<NodeId>,
    incoming_messages: VecDeque<DfsRoutedMessage>,
    detached_send_state: DfsNodeDetachedSendState,
    connect_timeout: Duration,
    sender: Option<DfsMessageSender>,
    async_sender: Option<AsyncDfsMessageSender>,
    direct_read_sender: Option<DfsDirectReadSender>,
    async_direct_read_sender: Option<AsyncDfsDirectReadSender>,
    peer_transport: Option<Arc<dyn DfsPeerTransport>>,
    async_peer_transport: Option<Arc<dyn AsyncDfsPeerTransport>>,
    blocking_metadata_fanout_workers: Arc<StdAtomicUsize>,
    next_content_root_catalog_frame_id: Arc<AtomicU64>,
    operation_deadline: Option<Instant>,
}

/// A clone of the served transport's peer-sending capability that shares only
/// its detached outbox. Daemon workers use this handle after releasing the DFS
/// service-lane lock, so replica promotion RPC latency cannot serialize later
/// primary mutations.
#[derive(Clone, Debug)]
pub struct DfsNodeDetachedSendWorker {
    transport: DfsNodeTransport,
    deferred_target_node_ids: BTreeSet<NodeId>,
}

impl DfsNodeDetachedSendWorker {
    pub fn drain(&mut self) -> Result<(), DfsError> {
        self.transport
            .flush_detached_peer_sends_excluding_targets(&self.deferred_target_node_ids)
            .map_err(DfsError::from_network)
    }

    /// Drain after obtaining exclusive ownership of the shared outbox worker.
    ///
    /// Long-lived daemon schedulers use this from `spawn_blocking`: returning
    /// early when another drain owns the gate can otherwise make the scheduler
    /// observe a temporarily empty outbox and exit just before failed work is
    /// restored for retry.
    pub fn drain_exclusive(&mut self) -> Result<(), DfsError> {
        self.transport
            .flush_detached_peer_sends_exclusive_excluding_targets(&self.deferred_target_node_ids)
            .map_err(DfsError::from_network)
    }

    #[cfg(test)]
    fn drain_at(&mut self, now: Instant) -> Result<(), DfsError> {
        self.transport
            .flush_detached_peer_sends_with_now_mode(&|| now, false, &self.deferred_target_node_ids)
            .map_err(DfsError::from_network)
    }

    /// Replace the targets that this worker must leave parked in the shared
    /// outbox. The sends and their promotion identities remain pending so a
    /// later worker can resume them after connectivity recovers.
    pub fn set_deferred_target_node_ids(&mut self, target_node_ids: BTreeSet<NodeId>) {
        self.deferred_target_node_ids = target_node_ids;
    }

    /// Wake a scheduler after publishing a worker with a refreshed target
    /// policy. The shared generation closes the race with an already-parked
    /// worker registering its notification.
    pub fn notify_scheduler_policy_change(&self) {
        self.transport.detached_send_state.record_change();
    }

    pub fn has_pending(&self) -> Result<bool, DfsError> {
        self.transport
            .detached_send_state
            .inner
            .lock()
            .map(|outbox| !outbox.ready_sends.is_empty() || !outbox.cooling_retries.is_empty())
            .map_err(|_| {
                DfsError::from_network(NetworkError::Io {
                    operation: "inspect detached DFS message sends".to_string(),
                    detail: "detached send lock is poisoned".to_string(),
                })
            })
    }

    /// Return whether work is immediately eligible, the earliest cooling
    /// deadline, and the shared change generation observed with that state.
    pub fn scheduling_snapshot(&self) -> Result<(bool, Option<Instant>, u64), DfsError> {
        self.transport
            .detached_send_scheduling_snapshot_at_excluding_targets(
                Instant::now(),
                &self.deferred_target_node_ids,
            )
            .map_err(DfsError::from_network)
    }

    #[cfg(test)]
    fn scheduling_snapshot_at(
        &self,
        now: Instant,
    ) -> Result<(bool, Option<Instant>, u64), DfsError> {
        self.transport
            .detached_send_scheduling_snapshot_at_excluding_targets(
                now,
                &self.deferred_target_node_ids,
            )
            .map_err(DfsError::from_network)
    }

    /// Wait without spinning until cooling work becomes eligible or a new
    /// ready send is accepted by any lane sharing this outbox.
    pub async fn wait_for_ready_or_retry(
        &self,
        observed_generation: u64,
        retry_not_before: Instant,
    ) -> Result<(), DfsError> {
        let notify = Arc::clone(&self.transport.detached_send_state.ready_notify);
        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        let (ready, current_retry_not_before, current_generation) = self
            .transport
            .detached_send_scheduling_snapshot_at_excluding_targets(
                Instant::now(),
                &self.deferred_target_node_ids,
            )
            .map_err(DfsError::from_network)?;
        if ready || current_retry_not_before.is_none() || current_generation != observed_generation
        {
            return Ok(());
        }
        let wake_at = current_retry_not_before
            .expect("cooling work has a deadline")
            .min(retry_not_before);
        tokio::select! {
            _ = &mut notified => Ok(()),
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(wake_at)) => Ok(()),
        }
    }

    /// Wait until any lane sharing this detached outbox changes its work set.
    ///
    /// Daemon schedulers stay parked here even when the outbox is empty. This
    /// is intentional: a concurrent maintenance drainer can temporarily move
    /// all work into an in-flight batch and later restore a failed send. The
    /// shared generation plus the pre-registered notification closes that
    /// restore-after-empty race without polling.
    pub async fn wait_for_change(&self, observed_generation: u64) -> Result<(), DfsError> {
        let notify = Arc::clone(&self.transport.detached_send_state.ready_notify);
        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        let (_, _, current_generation) = self
            .transport
            .detached_send_scheduling_snapshot_at_excluding_targets(
                Instant::now(),
                &self.deferred_target_node_ids,
            )
            .map_err(DfsError::from_network)?;
        if current_generation != observed_generation {
            return Ok(());
        }
        notified.await;
        Ok(())
    }
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
    /// Fork the transport state for one independent read-only request.
    ///
    /// Sender and peer-transport handles are intentionally shared: they are
    /// thread-safe connection/routing capabilities. Per-service response
    /// queues, detached sends, and operation deadlines must not be shared or
    /// copied into the fork, otherwise an opened-file read could consume a
    /// response owned by the mutable mounted service (or vice versa).
    pub(crate) fn fork_read_only_request(&self) -> Self {
        let mut fork = self.clone();
        fork.incoming_messages.clear();
        fork.detached_send_state = DfsNodeDetachedSendState::default();
        fork.operation_deadline = None;
        fork
    }

    fn fork_detached_send_worker(&self) -> DfsNodeDetachedSendWorker {
        let mut transport = self.clone();
        transport.incoming_messages.clear();
        // Background promotion is independent of the completed caller's
        // deadline. Each queued send still retains its own explicit policy.
        transport.operation_deadline = None;
        DfsNodeDetachedSendWorker {
            transport,
            deferred_target_node_ids: BTreeSet::new(),
        }
    }

    /// Fork one independent routed call while preserving the operation's
    /// absolute deadline. Returned DFS messages are accumulated in the fork
    /// and merged into the owning transport only after the bounded fan-out
    /// joins, so workers never share a mutable response queue. This slice
    /// reduces full-transfer time-to-all from a sum to a bounded maximum; it
    /// deliberately does not claim that the owner processes an early W receipt
    /// while a sibling attachment send is still active.
    fn fork_replica_call_send(&self) -> Self {
        let mut fork = self.clone();
        fork.incoming_messages.clear();
        fork.detached_send_state = DfsNodeDetachedSendState::default();
        fork
    }

    fn finish_replica_call_fanout(
        &mut self,
        mut completed: Vec<(usize, DfsRoutedSendOutcome, Vec<DfsRoutedMessage>)>,
    ) -> Vec<DfsRoutedSendOutcome> {
        completed.sort_by_key(|(index, _, _)| *index);
        let mut outcomes = Vec::with_capacity(completed.len());
        for (_, outcome, returned_messages) in completed {
            self.incoming_messages.extend(returned_messages);
            outcomes.push(outcome);
        }
        outcomes
    }

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
        )
    }

    pub fn with_peer_transports_for_nodes(
        local_node_id: NodeId,
        peer_node_ids: impl IntoIterator<Item = NodeId>,
        connect_timeout: Duration,
        peer_transport: Arc<dyn DfsPeerTransport>,
        async_peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    ) -> Self {
        Self::with_peer_transports_for_nodes_and_detached_state(
            local_node_id,
            peer_node_ids,
            connect_timeout,
            peer_transport,
            async_peer_transport,
            DfsNodeDetachedSendState::default(),
        )
    }

    /// Construct a node transport whose detached, replay-safe sends are owned
    /// by `detached_send_state`.
    ///
    /// Foreground routing queues and operation deadlines remain local to this
    /// transport. Supplying shared state does not opt ordinary node transports
    /// into the served-message early-response contract; it only lets a
    /// dedicated maintenance service reconstruct and drain durable work into
    /// the same outbox as the hot message lanes.
    pub fn with_peer_transports_for_nodes_and_detached_state(
        local_node_id: NodeId,
        peer_node_ids: impl IntoIterator<Item = NodeId>,
        connect_timeout: Duration,
        peer_transport: Arc<dyn DfsPeerTransport>,
        async_peer_transport: Arc<dyn AsyncDfsPeerTransport>,
        detached_send_state: DfsNodeDetachedSendState,
    ) -> Self {
        let message_peer_transport = Arc::clone(&peer_transport);
        let direct_read_peer_transport = Arc::clone(&peer_transport);
        let message_async_peer_transport = Arc::clone(&async_peer_transport);
        let async_direct_read_peer_transport = Arc::clone(&async_peer_transport);
        Self::with_senders_for_nodes_and_detached_state(
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
            Some(async_dfs_direct_read_sender_from_peer_transport(
                Arc::clone(&async_direct_read_peer_transport),
            )),
            Some(peer_transport),
            Some(async_peer_transport),
            detached_send_state,
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
    ) -> Self {
        Self::with_senders_for_nodes(
            local_node_id,
            peer_node_ids,
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            Some(sender),
            Some(async_sender),
            Some(direct_read_sender),
            Some(async_direct_read_sender),
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
        peer_transport: Option<Arc<dyn DfsPeerTransport>>,
        async_peer_transport: Option<Arc<dyn AsyncDfsPeerTransport>>,
    ) -> Self {
        Self::with_senders_for_nodes_and_detached_state(
            local_node_id,
            peer_node_ids,
            connect_timeout,
            sender,
            async_sender,
            direct_read_sender,
            async_direct_read_sender,
            peer_transport,
            async_peer_transport,
            DfsNodeDetachedSendState::default(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn with_senders_for_nodes_and_detached_state(
        local_node_id: NodeId,
        peer_node_ids: impl IntoIterator<Item = NodeId>,
        connect_timeout: Duration,
        sender: Option<DfsMessageSender>,
        async_sender: Option<AsyncDfsMessageSender>,
        direct_read_sender: Option<DfsDirectReadSender>,
        async_direct_read_sender: Option<AsyncDfsDirectReadSender>,
        peer_transport: Option<Arc<dyn DfsPeerTransport>>,
        async_peer_transport: Option<Arc<dyn AsyncDfsPeerTransport>>,
        detached_send_state: DfsNodeDetachedSendState,
    ) -> Self {
        let known_remote_nodes = peer_node_ids
            .into_iter()
            .filter(|node_id| node_id != &local_node_id)
            .collect();
        Self {
            local_node_id,
            known_remote_nodes,
            incoming_messages: VecDeque::new(),
            detached_send_state,
            connect_timeout,
            sender,
            async_sender,
            direct_read_sender,
            async_direct_read_sender,
            peer_transport,
            async_peer_transport,
            blocking_metadata_fanout_workers: Arc::new(StdAtomicUsize::new(0)),
            // Keep transport-owned catalog requests disjoint from the
            // service-owned low frame-id sequence while sharing one monotonic
            // source across read-only transport forks.
            next_content_root_catalog_frame_id: Arc::new(AtomicU64::new(1_u64 << 63)),
            operation_deadline: None,
        }
    }

    pub fn set_operation_deadline(&mut self, deadline: Option<Instant>) -> Option<Instant> {
        std::mem::replace(&mut self.operation_deadline, deadline)
    }

    pub fn operation_deadline(&self) -> Option<Instant> {
        self.operation_deadline
    }

    /// Fetch one bounded content-root page from the exact metadata primary
    /// named by `request`.
    ///
    /// The RPC is an ordinary authenticated DFS message call. The returned
    /// routed response is accepted only from `target_node_id`, and the page
    /// must validate against the request's filesystem, shard, primary, epoch,
    /// topology digest, cursor, and minimum durable-cut proof.
    pub fn request_content_root_catalog_page(
        &self,
        target_node_id: &NodeId,
        request: &DfsContentRootCatalogPageRequest,
    ) -> Result<DfsContentRootCatalogPage, NetworkError> {
        request
            .validate()
            .map_err(|source| NetworkError::Serialization {
                detail: format!("invalid DFS content-root catalog request: {source}"),
            })?;
        if target_node_id == &self.local_node_id
            || !self.known_remote_nodes.contains(target_node_id)
        {
            return Err(NetworkError::UnknownNode {
                node_id: target_node_id.clone(),
            });
        }
        if &request.identity.primary_node_id != target_node_id {
            return Err(NetworkError::Serialization {
                detail: format!(
                    "DFS content-root catalog target {target_node_id} does not match requested primary {}",
                    request.identity.primary_node_id
                ),
            });
        }
        let Some(sender) = self.sender.as_ref().cloned() else {
            return Err(NetworkError::Io {
                operation: format!("fetch DFS content-root catalog page from {target_node_id}"),
                detail: "DFS peer transport was constructed without a sync sender".to_string(),
            });
        };
        let frame_id = self
            .next_content_root_catalog_frame_id
            .fetch_update(AtomicOrdering::AcqRel, AtomicOrdering::Acquire, |current| {
                current.checked_add(1)
            })
            .map_err(|_| NetworkError::ResourceExhausted {
                operation: "allocate DFS content-root catalog frame".to_string(),
                requested_bytes: std::mem::size_of::<u64>(),
            })?;
        let routed_message = DfsRoutedMessage {
            source_node_id: self.local_node_id.clone(),
            target_node_id: target_node_id.clone(),
            frame_id,
            message_id: frame_id,
            message: DfsNetworkMessage::ContentRootCatalogPageRequest(request.clone()),
        };
        let options = self.current_send_options(format!(
            "fetch DFS content-root catalog page from {target_node_id}"
        ))?;
        let returned_messages = send_dfs_routed_message_to_peer(
            sender,
            options,
            target_node_id.clone(),
            routed_message,
        )?;
        let mut page = None;
        for returned in returned_messages {
            if returned.source_node_id != *target_node_id
                || returned.target_node_id != self.local_node_id
            {
                return Err(NetworkError::Serialization {
                    detail: format!(
                        "DFS content-root catalog response route mismatch: expected {target_node_id} -> {}, got {} -> {}",
                        self.local_node_id, returned.source_node_id, returned.target_node_id
                    ),
                });
            }
            let DfsNetworkMessage::ContentRootCatalogPageResponse(candidate) = returned.message
            else {
                return Err(NetworkError::Serialization {
                    detail: format!(
                        "DFS content-root catalog request to {target_node_id} returned an unexpected message"
                    ),
                });
            };
            if page.replace(candidate).is_some() {
                return Err(NetworkError::Serialization {
                    detail: format!(
                        "DFS content-root catalog request to {target_node_id} returned multiple pages"
                    ),
                });
            }
        }
        let page = page.ok_or_else(|| NetworkError::Serialization {
            detail: format!(
                "DFS content-root catalog request to {target_node_id} returned no page"
            ),
        })?;
        page.validate_for_request(request)
            .map_err(|source| NetworkError::Serialization {
                detail: format!(
                    "invalid DFS content-root catalog response from {target_node_id}: {source}"
                ),
            })?;
        Ok(page)
    }

    #[cfg(test)]
    fn detached_send_count(&self) -> usize {
        let outbox = self
            .detached_send_state
            .inner
            .lock()
            .expect("detached send outbox");
        outbox.ready_sends.len()
            + outbox
                .cooling_retries
                .values()
                .map(VecDeque::len)
                .sum::<usize>()
    }

    #[cfg(test)]
    fn detached_send_targets(&self) -> Vec<NodeId> {
        let outbox = self
            .detached_send_state
            .inner
            .lock()
            .expect("detached send outbox");
        outbox
            .ready_sends
            .iter()
            .chain(outbox.cooling_retries.values().flat_map(VecDeque::iter))
            .map(|send| send.target_node_id.clone())
            .collect()
    }

    #[cfg(test)]
    fn detached_send_frame_ids(&self) -> Vec<u64> {
        let outbox = self
            .detached_send_state
            .inner
            .lock()
            .expect("detached send outbox");
        outbox
            .ready_sends
            .iter()
            .chain(outbox.cooling_retries.values().flat_map(VecDeque::iter))
            .map(|send| send.routed_message.frame_id)
            .collect()
    }

    #[cfg(test)]
    fn detached_send_scheduling_snapshot_at_for_test(
        &self,
        now: Instant,
    ) -> (bool, Option<Instant>, u64) {
        self.detached_send_scheduling_snapshot_at(now)
            .expect("detached send scheduling snapshot")
    }

    #[cfg(test)]
    fn pending_commit_promotion_count(&self) -> usize {
        self.detached_send_state
            .inner
            .lock()
            .expect("detached send outbox")
            .pending_commit_promotions
            .len()
    }

    #[cfg(test)]
    fn settled_commit_promotion_count(&self) -> usize {
        self.detached_send_state
            .inner
            .lock()
            .expect("detached send outbox")
            .terminal_commit_promotions
            .values()
            .filter(|promotion| promotion.outcome == DfsCommitPromotionTerminalOutcome::Committed)
            .count()
    }

    #[cfg(test)]
    fn retired_not_desired_commit_promotion_count(&self) -> usize {
        self.detached_send_state
            .inner
            .lock()
            .expect("detached send outbox")
            .terminal_commit_promotions
            .values()
            .filter(|promotion| {
                promotion.outcome == DfsCommitPromotionTerminalOutcome::RetiredNotDesired
            })
            .count()
    }

    #[cfg(test)]
    fn repair_pending_commit_promotion_count(&self) -> usize {
        self.detached_send_state
            .inner
            .lock()
            .expect("detached send outbox")
            .terminal_commit_promotions
            .values()
            .filter(|promotion| {
                promotion.outcome == DfsCommitPromotionTerminalOutcome::RepairPending
            })
            .count()
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

    /// Opens the canonical stream-native DFS read response from one selected
    /// content location.
    ///
    /// The returned response retains the transport-owned body chunks. Callers
    /// that still require a contiguous [`DfsReadResponse`] must explicitly use
    /// [`DfsDirectReadClientResponse::collect_to_read_response`].
    pub async fn read_object_stream(
        &self,
        target_node_id: &NodeId,
        request: &DfsDirectReadRequest,
    ) -> Result<DfsDirectReadClientResponse, NetworkError> {
        let options =
            self.current_send_options(format!("stream DFS read from {target_node_id}"))?;
        let sender = self
            .async_direct_read_sender
            .as_ref()
            .cloned()
            .ok_or_else(|| NetworkError::Io {
                operation: format!("stream DFS read from {target_node_id}"),
                detail: "transport has no asynchronous direct-read sender".to_string(),
            })?;
        send_dfs_direct_read_to_peer_async(sender, options, target_node_id.clone(), request).await
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

    fn send_routed_message_attachment_to_peer(
        &mut self,
        target_node_id: &NodeId,
        routed_message: DfsRoutedMessage,
        attachment: DfsOutboundAttachment,
        require_local_source: bool,
    ) -> Result<(), NetworkError> {
        if target_node_id == &self.local_node_id {
            let max_bytes =
                usize::try_from(attachment.descriptor.total_size_bytes).map_err(|_| {
                    NetworkError::ResourceExhausted {
                        operation: "materialize loopback DFS attachment".to_string(),
                        requested_bytes: usize::MAX,
                    }
                })?;
            let bytes = collect_dfs_attachment_source_sync(attachment.source, max_bytes).map_err(
                |source| NetworkError::Io {
                    operation: "collect loopback DFS request attachment".to_string(),
                    detail: source.to_string(),
                },
            )?;
            let message = crate::body_rpc::attach_dfs_routed_message_attachment(
                routed_message,
                attachment.descriptor,
                bytes,
            )?;
            self.incoming_messages.push_back(message);
            return Ok(());
        }
        self.validate_routed_message_target(target_node_id, &routed_message, require_local_source)?;
        let Some(peer_transport) = self.peer_transport.as_ref().cloned() else {
            return Err(NetworkError::Io {
                operation: format!("send DFS attachment to {target_node_id}"),
                detail: "DFS peer transport does not support canonical request attachments"
                    .to_string(),
            });
        };
        let (command, call) =
            dfs_routed_call_rpc_call(DfsRoutedCall::with_attachment(routed_message, attachment))?;
        let options =
            self.current_send_options(format!("send DFS attachment to {target_node_id}"))?;
        let result = peer_transport
            .call_object_rpc(
                target_node_id,
                call,
                options.with_observe_connectivity(!dfs_message_is_read_only_request(&command)),
            )
            .map_err(|source| NetworkError::Io {
                operation: format!("send DFS attachment to {target_node_id}"),
                detail: source.to_string(),
            })?;
        let returned = dfs_message_response_from_rpc_result_sync(result).map_err(|source| {
            NetworkError::Io {
                operation: format!("decode DFS attachment response from {target_node_id}"),
                detail: source.to_string(),
            }
        })?;
        for message in returned.outbound_messages {
            let target = message.target_node_id.clone();
            self.send_routed_message_to_peer(&target, message, false)?;
        }
        Ok(())
    }

    fn send_routed_call_to_peer(
        &mut self,
        target_node_id: &NodeId,
        call: DfsRoutedCall,
        require_local_source: bool,
    ) -> Result<(), NetworkError> {
        match call.attachment {
            Some(attachment) => self.send_routed_message_attachment_to_peer(
                target_node_id,
                call.message,
                attachment,
                require_local_source,
            ),
            None => {
                self.send_routed_message_to_peer(target_node_id, call.message, require_local_source)
            }
        }
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
        if self.async_direct_read_sender.is_none() {
            return Ok(None);
        }
        // Routed-message consumers retain their historical contiguous response
        // contract. Keep that copy at this explicit compatibility boundary;
        // direct stream callers return immediately after the response head.
        let response = self
            .read_object_stream(target_node_id, &request)
            .await?
            .collect_to_read_response()
            .await?;
        Ok(Some(vec![dfs_direct_read_response_routed_message(
            target_node_id,
            routed_message,
            response,
        )]))
    }

    #[cfg(test)]
    fn detached_send_scheduling_snapshot_at(
        &self,
        now: Instant,
    ) -> Result<(bool, Option<Instant>, u64), NetworkError> {
        self.detached_send_scheduling_snapshot_at_excluding_targets(now, &BTreeSet::new())
    }

    fn detached_send_scheduling_snapshot_at_excluding_targets(
        &self,
        now: Instant,
        deferred_target_node_ids: &BTreeSet<NodeId>,
    ) -> Result<(bool, Option<Instant>, u64), NetworkError> {
        let outbox = self
            .detached_send_state
            .inner
            .lock()
            .map_err(|_| NetworkError::Io {
                operation: "inspect detached DFS message schedule".to_string(),
                detail: "detached send lock is poisoned".to_string(),
            })?;
        let next_retry_not_before = outbox.cooling_retries.iter().find_map(|(at, sends)| {
            sends
                .iter()
                .any(|send| !deferred_target_node_ids.contains(&send.target_node_id))
                .then_some(*at)
        });
        let ready = outbox
            .ready_sends
            .iter()
            .any(|send| !deferred_target_node_ids.contains(&send.target_node_id))
            || next_retry_not_before.is_some_and(|retry_not_before| retry_not_before <= now);
        let generation = self
            .detached_send_state
            .change_generation
            .load(AtomicOrdering::Acquire);
        Ok((ready, next_retry_not_before, generation))
    }

    fn take_detached_peer_sends_ready_at(
        &self,
        now: Instant,
        deferred_target_node_ids: &BTreeSet<NodeId>,
    ) -> Result<VecDeque<DeferredDfsPeerSend>, NetworkError> {
        let mut outbox = self
            .detached_send_state
            .inner
            .lock()
            .map_err(|_| NetworkError::Io {
                operation: "take ready detached DFS message sends".to_string(),
                detail: "detached send lock is poisoned".to_string(),
            })?;
        while outbox
            .cooling_retries
            .first_key_value()
            .is_some_and(|(retry_not_before, _)| *retry_not_before <= now)
        {
            let (_, mut due) = outbox
                .cooling_retries
                .pop_first()
                .expect("eligible detached retry deadline exists");
            // Healthy work accepted while this retry cooled stays ahead of it.
            outbox.ready_sends.append(&mut due);
        }
        let ready_sends = std::mem::take(&mut outbox.ready_sends);
        let (eligible, deferred): (VecDeque<_>, VecDeque<_>) = ready_sends
            .into_iter()
            .partition(|send| !deferred_target_node_ids.contains(&send.target_node_id));
        outbox.ready_sends = deferred;
        Ok(eligible)
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
        let is_commit_version_promotion =
            matches!(&routed_message.message, DfsNetworkMessage::CommitVersion(_));
        let is_replay_safe_completed_write_response = matches!(
            &routed_message.message,
            DfsNetworkMessage::DistributedWriteResponse(_)
        );
        let allows_dynamic_route_recovery =
            is_commit_version_promotion || is_replay_safe_completed_write_response;
        if let Err(error) = self.validate_routed_message_target(
            target_node_id,
            &routed_message,
            require_local_source,
        ) && !(allows_dynamic_route_recovery
            && matches!(&error, NetworkError::UnknownNode { .. }))
        {
            return Err(error);
        }
        let Some(sender) = self.sender.as_ref().cloned() else {
            return Err(NetworkError::Io {
                operation: format!("send DFS message to {target_node_id}"),
                detail: "DFS peer transport was constructed without a sync sender".to_string(),
            });
        };
        // CommitVersion and completed DistributedWriteResponse messages are
        // replay-safe post-response work. Carrying the completed caller's
        // absolute deadline into the outbox can expire them before the worker
        // is scheduled, so each attempt starts a fresh background RPC budget.
        let operation_deadline = if allows_dynamic_route_recovery {
            None
        } else {
            self.operation_deadline
        };
        let options = Self::clamp_send_options_to_operation_deadline(
            DfsObjectRpcSendOptions::new(self.connect_timeout),
            operation_deadline,
            format!("send detached DFS message to {target_node_id}"),
        )?;
        let mut send = DeferredDfsPeerSend {
            sender,
            options,
            operation_deadline,
            target_node_id: target_node_id.clone(),
            routed_message,
            promotion_sequence: None,
            retry_attempts: 0,
        };
        let promotion_identity = send.commit_version_promotion_identity();
        let mut outbox = self
            .detached_send_state
            .inner
            .lock()
            .map_err(|_| NetworkError::Io {
                operation: "track detached DFS message send".to_string(),
                detail: "detached send lock is poisoned".to_string(),
            })?;
        if let Some(identity) = promotion_identity {
            let settlement_key = identity.settlement_key();
            if outbox
                .terminal_commit_promotions
                .get(&settlement_key)
                .is_some_and(|terminal| terminal.identity == identity)
                || outbox.pending_commit_promotions.contains(&identity)
            {
                return Ok(());
            }
            outbox.next_promotion_sequence = outbox.next_promotion_sequence.saturating_add(1);
            send.promotion_sequence = Some(outbox.next_promotion_sequence);
            outbox.pending_commit_promotions.insert(identity);
        }
        outbox.ready_sends.push_back(send);
        drop(outbox);
        self.detached_send_state.record_change();
        Ok(())
    }

    fn flush_detached_peer_sends(&mut self) -> Result<(), NetworkError> {
        self.flush_detached_peer_sends_excluding_targets(&BTreeSet::new())
    }

    fn flush_detached_peer_sends_excluding_targets(
        &mut self,
        deferred_target_node_ids: &BTreeSet<NodeId>,
    ) -> Result<(), NetworkError> {
        self.flush_detached_peer_sends_with_now_mode(&Instant::now, false, deferred_target_node_ids)
    }

    fn flush_detached_peer_sends_exclusive_excluding_targets(
        &mut self,
        deferred_target_node_ids: &BTreeSet<NodeId>,
    ) -> Result<(), NetworkError> {
        self.flush_detached_peer_sends_with_now_mode(&Instant::now, true, deferred_target_node_ids)
    }

    #[cfg(test)]
    fn flush_detached_peer_sends_with_now<F>(&mut self, now: &F) -> Result<(), NetworkError>
    where
        F: Fn() -> Instant + ?Sized,
    {
        self.flush_detached_peer_sends_with_now_mode(now, false, &BTreeSet::new())
    }

    fn flush_detached_peer_sends_with_now_mode<F>(
        &mut self,
        now: &F,
        wait_for_drain_gate: bool,
        deferred_target_node_ids: &BTreeSet<NodeId>,
    ) -> Result<(), NetworkError>
    where
        F: Fn() -> Instant + ?Sized,
    {
        // The Quint fan-out cap applies to the shared outbox, not to each
        // transport clone independently. One worker owns ready work while
        // retaining cap-two concurrency inside its drain. A competing worker
        // never blocks a Tokio blocking-pool thread behind the global gate.
        let drain_gate = Arc::clone(&self.detached_send_state.drain_gate);
        let drain_lock_error = || NetworkError::Io {
            operation: "serialize detached DFS message drains".to_string(),
            detail: "detached send drain lock is poisoned".to_string(),
        };
        let _drain_guard = if wait_for_drain_gate {
            drain_gate.lock().map_err(|_| drain_lock_error())?
        } else {
            match drain_gate.try_lock() {
                Ok(guard) => guard,
                Err(std::sync::TryLockError::WouldBlock) => return Ok(()),
                Err(std::sync::TryLockError::Poisoned(_)) => return Err(drain_lock_error()),
            }
        };
        let mut first_replay_error = None;
        loop {
            let mut sends =
                self.take_detached_peer_sends_ready_at(now(), deferred_target_node_ids)?;
            if sends.is_empty() {
                break;
            }
            let mut replay_retries = VecDeque::new();
            while let Some(front) = sends.front() {
                if front.is_commit_version_promotion() {
                    let mut promotion_wave = Vec::with_capacity(DFS_COMMIT_PROMOTION_MAX_INFLIGHT);
                    while promotion_wave.len() < DFS_COMMIT_PROMOTION_MAX_INFLIGHT
                        && sends
                            .front()
                            .is_some_and(DeferredDfsPeerSend::is_commit_version_promotion)
                    {
                        promotion_wave.push(sends.pop_front().expect("promotion send front"));
                    }
                    // Promotion is independent post-response convergence work;
                    // neither its queued deadline nor a later caller deadline
                    // may retroactively cancel it.
                    let current_operation_deadline = None;
                    let results = std::thread::scope(|scope| {
                        let handles = promotion_wave
                            .iter()
                            .cloned()
                            .map(|send| {
                                scope.spawn(move || {
                                    Self::send_detached_peer_message(
                                        send,
                                        current_operation_deadline,
                                    )
                                })
                            })
                            .collect::<Vec<_>>();
                        promotion_wave
                            .into_iter()
                            .zip(handles)
                            .map(|(send, handle)| {
                                let result = handle.join().unwrap_or_else(|_| {
                                    Err(NetworkError::Io {
                                        operation: "join detached DFS commit promotion fan-out"
                                            .to_string(),
                                        detail: "commit promotion worker panicked".to_string(),
                                    })
                                });
                                (send, result)
                            })
                            .collect::<Vec<_>>()
                    });
                    for (send, result) in results {
                        let result = result.and_then(|returned_messages| {
                            Self::validate_detached_commit_promotion_response(
                                &send,
                                &returned_messages,
                            )
                        });
                        match result {
                            Ok(disposition) => {
                                let outcome = match disposition {
                                    DfsCommitPromotionResponseDisposition::Committed => {
                                        DfsCommitPromotionTerminalOutcome::Committed
                                    }
                                    DfsCommitPromotionResponseDisposition::RetireNotDesired => {
                                        DfsCommitPromotionTerminalOutcome::RetiredNotDesired
                                    }
                                    DfsCommitPromotionResponseDisposition::RepairPending => {
                                        DfsCommitPromotionTerminalOutcome::RepairPending
                                    }
                                };
                                if let Err(error) =
                                    self.mark_commit_promotion_terminal(&send, outcome)
                                {
                                    replay_retries.push_back(send);
                                    if first_replay_error.is_none() {
                                        first_replay_error = Some(error);
                                    }
                                } else if disposition
                                    == DfsCommitPromotionResponseDisposition::RetireNotDesired
                                    && let DfsNetworkMessage::CommitVersion(command) =
                                        &send.routed_message.message
                                {
                                    tracing::warn!(
                                        target_node_id = %send.target_node_id,
                                        intent_id = %command.intent_id,
                                        file_system_id = %command.file_system_id,
                                        object_id = %command.object_id,
                                        version = %command.version,
                                        "retired detached DFS CommitVersion promotion because the target is not a desired replica"
                                    );
                                } else if disposition
                                    == DfsCommitPromotionResponseDisposition::RepairPending
                                    && let DfsNetworkMessage::CommitVersion(command) =
                                        &send.routed_message.message
                                {
                                    tracing::warn!(
                                        target_node_id = %send.target_node_id,
                                        intent_id = %command.intent_id,
                                        file_system_id = %command.file_system_id,
                                        object_id = %command.object_id,
                                        version = %command.version,
                                        "retired detached DFS CommitVersion promotion after the target durably transitioned missing content to repair"
                                    );
                                }
                            }
                            Err(error) => {
                                // CommitVersion is idempotent for the same
                                // object/version/hash. Transport failure and a
                                // retryable negative/missing ACK stay in cooling.
                                replay_retries.push_back(send);
                                if first_replay_error.is_none() {
                                    first_replay_error = Some(error);
                                }
                            }
                        }
                    }
                    continue;
                }

                let send = sends.pop_front().expect("detached send front");
                let attempted_replay = send
                    .is_replay_safe_completed_write_response()
                    .then(|| send.clone());
                let current_operation_deadline = if send.uses_fresh_detached_budget() {
                    None
                } else {
                    self.operation_deadline
                };
                let result = Self::send_detached_peer_message(send, current_operation_deadline)
                    .and_then(|returned_messages| {
                        self.route_detached_peer_responses(returned_messages)
                    });
                if let Err(error) = result {
                    if let Some(attempted_replay) = attempted_replay {
                        replay_retries.push_back(attempted_replay);
                        if first_replay_error.is_none() {
                            first_replay_error = Some(error);
                        }
                        continue;
                    }
                    self.restore_detached_peer_send_work(sends, replay_retries, now())?;
                    return Err(first_replay_error.take().unwrap_or(error));
                }
            }

            self.restore_detached_peer_send_work(VecDeque::new(), replay_retries, now())?;
            // Recheck the shared ready queue before returning. Work accepted
            // concurrently through another lane is never hidden behind the
            // cooling failures from this round.
        }
        first_replay_error.map_or(Ok(()), Err)
    }

    fn send_detached_peer_message(
        send: DeferredDfsPeerSend,
        current_operation_deadline: Option<Instant>,
    ) -> Result<Vec<DfsRoutedMessage>, NetworkError> {
        let operation_deadline =
            Self::earlier_operation_deadline(send.operation_deadline, current_operation_deadline);
        let options = Self::clamp_send_options_to_operation_deadline(
            send.options,
            operation_deadline,
            format!("flush detached DFS message send to {}", send.target_node_id),
        )?;
        send_dfs_routed_message_to_peer(
            send.sender,
            options,
            send.target_node_id,
            send.routed_message,
        )
    }

    fn validate_detached_commit_promotion_response(
        send: &DeferredDfsPeerSend,
        returned_messages: &[DfsRoutedMessage],
    ) -> Result<DfsCommitPromotionResponseDisposition, NetworkError> {
        let DfsNetworkMessage::CommitVersion(command) = &send.routed_message.message else {
            return Err(NetworkError::Serialization {
                detail: "detached promotion validation received a non-CommitVersion message"
                    .to_string(),
            });
        };
        let response = returned_messages.iter().find_map(|message| {
            let DfsNetworkMessage::CommitVersionResponse(response) = &message.message else {
                return None;
            };
            all_commit_version_response_identity_matches(send, command, message, response)
                .then_some(response)
        });
        let Some(response) = response else {
            return Err(NetworkError::Serialization {
                detail: format!(
                    "detached CommitVersion promotion to {} returned no matching acknowledgement",
                    send.target_node_id
                ),
            });
        };
        match &response.result {
            DfsBlobTransferResult::Committed { content_hash }
                if content_hash == &command.content_hash =>
            {
                Ok(DfsCommitPromotionResponseDisposition::Committed)
            }
            DfsBlobTransferResult::Committed { content_hash } => Err(NetworkError::Serialization {
                detail: format!(
                    "detached CommitVersion promotion to {} acknowledged hash {}, expected {}",
                    send.target_node_id, content_hash, command.content_hash
                ),
            }),
            DfsBlobTransferResult::Durable { content_hash } => Err(NetworkError::Io {
                operation: format!(
                    "promote DFS version {} on {}",
                    command.version, send.target_node_id
                ),
                detail: format!(
                    "replica acknowledged durable content hash {content_hash} but has not committed the version"
                ),
            }),
            DfsBlobTransferResult::Rejected { reason }
                if dfs_rejection_is_replica_not_desired(
                    reason,
                    &send.routed_message.source_node_id,
                    &command.intent_id,
                ) =>
            {
                Ok(DfsCommitPromotionResponseDisposition::RetireNotDesired)
            }
            DfsBlobTransferResult::Rejected { reason }
                if dfs_rejection_is_commit_content_repair_pending(
                    reason,
                    &command.object_id,
                    &command.version,
                    &command.content_hash,
                ) =>
            {
                Ok(DfsCommitPromotionResponseDisposition::RepairPending)
            }
            DfsBlobTransferResult::Rejected { reason } => Err(NetworkError::Io {
                operation: format!(
                    "promote DFS version {} on {}",
                    command.version, send.target_node_id
                ),
                detail: reason.clone(),
            }),
        }
    }

    fn mark_commit_promotion_terminal(
        &self,
        send: &DeferredDfsPeerSend,
        outcome: DfsCommitPromotionTerminalOutcome,
    ) -> Result<(), NetworkError> {
        let Some(identity) = send.commit_version_promotion_identity() else {
            return Ok(());
        };
        let Some(enqueue_sequence) = send.promotion_sequence else {
            return Err(NetworkError::Serialization {
                detail: "detached CommitVersion promotion has no outbox sequence".to_string(),
            });
        };
        let settlement_key = identity.settlement_key();
        let mut outbox = self
            .detached_send_state
            .inner
            .lock()
            .map_err(|_| NetworkError::Io {
                operation: "record terminal DFS commit promotion".to_string(),
                detail: "terminal promotion lock is poisoned".to_string(),
            })?;
        outbox.pending_commit_promotions.remove(&identity);
        let should_advance_terminal = outbox
            .terminal_commit_promotions
            .get(&settlement_key)
            .is_none_or(|terminal| terminal.enqueue_sequence <= enqueue_sequence);
        if should_advance_terminal {
            outbox.terminal_commit_promotions.insert(
                settlement_key,
                DfsTerminalCommitPromotion {
                    identity,
                    enqueue_sequence,
                    outcome,
                },
            );
            outbox.compact_terminal_commit_promotions_if_needed();
        }
        Ok(())
    }

    fn route_detached_peer_responses(
        &mut self,
        returned_messages: Vec<DfsRoutedMessage>,
    ) -> Result<(), NetworkError> {
        for returned_message in returned_messages {
            let target_node_id = returned_message.target_node_id.clone();
            self.send_routed_message_to_peer(&target_node_id, returned_message, false)?;
        }
        Ok(())
    }

    fn restore_detached_peer_send_work(
        &mut self,
        mut ready_sends: VecDeque<DeferredDfsPeerSend>,
        mut replay_retries: VecDeque<DeferredDfsPeerSend>,
        retry_started_at: Instant,
    ) -> Result<(), NetworkError> {
        if ready_sends.is_empty() && replay_retries.is_empty() {
            return Ok(());
        }
        let schedule_changed = !ready_sends.is_empty() || !replay_retries.is_empty();
        let mut outbox = self
            .detached_send_state
            .inner
            .lock()
            .map_err(|_| NetworkError::Io {
                operation: "restore detached DFS message sends".to_string(),
                detail: "detached send lock is poisoned".to_string(),
            })?;
        if !ready_sends.is_empty() {
            // Preserve the unattempted suffix ahead of work queued while its
            // failed non-replay-safe predecessor was in flight.
            ready_sends.append(&mut outbox.ready_sends);
            outbox.ready_sends = ready_sends;
        }
        while let Some(mut replay_retry) = replay_retries.pop_front() {
            let retry_not_before = retry_started_at + replay_retry.record_retry_failure();
            outbox
                .cooling_retries
                .entry(retry_not_before)
                .or_default()
                .push_back(replay_retry);
        }
        drop(outbox);
        if schedule_changed {
            self.detached_send_state.record_change();
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

    async fn send_routed_message_attachment_to_peer_async(
        &mut self,
        target_node_id: &NodeId,
        routed_message: DfsRoutedMessage,
        attachment: DfsOutboundAttachment,
        require_local_source: bool,
    ) -> Result<(), NetworkError> {
        if target_node_id == &self.local_node_id {
            let max_bytes =
                usize::try_from(attachment.descriptor.total_size_bytes).map_err(|_| {
                    NetworkError::ResourceExhausted {
                        operation: "materialize async loopback DFS attachment".to_string(),
                        requested_bytes: usize::MAX,
                    }
                })?;
            let bytes = attachment.source.collect_to_vec(max_bytes).await?;
            let message = crate::body_rpc::attach_dfs_routed_message_attachment(
                routed_message,
                attachment.descriptor,
                bytes,
            )?;
            self.incoming_messages.push_back(message);
            return Ok(());
        }
        self.validate_routed_message_target(target_node_id, &routed_message, require_local_source)?;
        let Some(peer_transport) = self.async_peer_transport.as_ref().cloned() else {
            return Err(NetworkError::Io {
                operation: format!("send DFS attachment to {target_node_id}"),
                detail: "async DFS peer transport does not support canonical request attachments"
                    .to_string(),
            });
        };
        let (command, call) =
            dfs_routed_call_rpc_call(DfsRoutedCall::with_attachment(routed_message, attachment))?;
        let options =
            self.current_send_options(format!("send DFS attachment to {target_node_id}"))?;
        let result = peer_transport
            .call_object_rpc_async(
                target_node_id,
                call,
                options.with_observe_connectivity(!dfs_message_is_read_only_request(&command)),
            )
            .await
            .map_err(|source| NetworkError::Io {
                operation: format!("send DFS attachment to {target_node_id}"),
                detail: source.to_string(),
            })?;
        let returned = dfs_message_response_from_rpc_result(result)
            .await
            .map_err(|source| NetworkError::Io {
                operation: format!("decode DFS attachment response from {target_node_id}"),
                detail: source.to_string(),
            })?;
        for message in returned.outbound_messages {
            let target = message.target_node_id.clone();
            self.send_routed_message_to_peer_async(&target, message, false)
                .await?;
        }
        Ok(())
    }

    async fn send_routed_call_to_peer_async(
        &mut self,
        target_node_id: &NodeId,
        call: DfsRoutedCall,
        require_local_source: bool,
    ) -> Result<(), NetworkError> {
        match call.attachment {
            Some(attachment) => {
                self.send_routed_message_attachment_to_peer_async(
                    target_node_id,
                    call.message,
                    attachment,
                    require_local_source,
                )
                .await
            }
            None => {
                self.send_routed_message_to_peer_async(
                    target_node_id,
                    call.message,
                    require_local_source,
                )
                .await
            }
        }
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

fn dfs_exact_response_replay_attempt_options(
    message: &DfsNetworkMessage,
    options: DfsObjectRpcSendOptions,
) -> (DfsObjectRpcSendOptions, Option<Duration>) {
    if !crate::routes::dfs_network_message_response_retry_safe(message) {
        return (options, None);
    }
    let Some(total_timeout) = options.overall_timeout else {
        return (options, None);
    };
    let reserve = (total_timeout / 4).min(DFS_EXACT_RESPONSE_REPLAY_MAX_RESERVE);
    let first_timeout = total_timeout.saturating_sub(reserve);
    if reserve.is_zero() || first_timeout.is_zero() {
        return (options, None);
    }
    (
        DfsObjectRpcSendOptions {
            overall_timeout: Some(first_timeout),
            ..options
        },
        Some(total_timeout),
    )
}

fn dfs_exact_response_replay_options(
    options: DfsObjectRpcSendOptions,
    total_timeout: Duration,
    started: Instant,
) -> Option<DfsObjectRpcSendOptions> {
    let remaining = total_timeout
        .checked_sub(started.elapsed())
        .filter(|remaining| !remaining.is_zero())?;
    Some(DfsObjectRpcSendOptions {
        overall_timeout: Some(remaining),
        ..options
    })
}

fn dfs_peer_transport_network_error(
    operation: String,
    error: DfsPeerTransportError,
) -> NetworkError {
    match error.delivery_state() {
        Some(delivery_state) => {
            NetworkError::delivery_failed(operation, delivery_state, error.to_string())
        }
        None => NetworkError::Io {
            operation,
            detail: error.to_string(),
        },
    }
}

fn dfs_exact_response_replay_exhausted(
    operation: String,
    first_error: DfsPeerTransportError,
    retry_error: DfsPeerTransportError,
) -> NetworkError {
    NetworkError::RetryExhausted {
        operation,
        attempts: 2,
        delivery_state: retry_error
            .delivery_state()
            .or(first_error.delivery_state())
            .unwrap_or(sinomo_core::network::NetworkDeliveryState::PossiblyApplied),
        detail: format!(
            "initial response was ambiguous: {first_error}; exact request replay failed: {retry_error}"
        ),
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
    let (first_options, replay_total_timeout) =
        dfs_exact_response_replay_attempt_options(&command.routed_message.message, options);
    let response = match sender(&target_node_id, &command, first_options) {
        Ok(response) => response,
        Err(first_error)
            if first_error.delivery_state()
                == Some(sinomo_core::network::NetworkDeliveryState::PossiblyApplied) =>
        {
            let Some(replay_options) = replay_total_timeout.and_then(|total_timeout| {
                dfs_exact_response_replay_options(options, total_timeout, send_started)
            }) else {
                return Err(dfs_peer_transport_network_error(
                    format!("send DFS message to {target_node_id}"),
                    first_error,
                ));
            };
            dfs_perf_trace(format_args!(
                "dfs-message-exact-response-replay local={} target={} request={} remaining_ms={} first_error={}",
                source_node_id,
                target_node_id,
                command.request_id,
                replay_options
                    .overall_timeout
                    .unwrap_or_default()
                    .as_millis(),
                first_error
            ));
            sender(&target_node_id, &command, replay_options).map_err(|retry_error| {
                dfs_exact_response_replay_exhausted(
                    format!("reconcile DFS message response from {target_node_id}"),
                    first_error,
                    retry_error,
                )
            })?
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
            return Err(dfs_peer_transport_network_error(
                format!("send DFS message to {target_node_id}"),
                error,
            ));
        }
    };
    let outbound_messages = response.outbound_messages;
    if trace_enabled {
        let outbound_attachment_bytes =
            dfs_message_response_body_bytes(&command.request_id, &outbound_messages)?;
        dfs_perf_trace(format_args!(
            "dfs-message-send local={} target={} request={} request_body_bytes={} outbound_messages={} outbound_attachment_bytes={} elapsed_ms={}",
            source_node_id,
            target_node_id,
            command.request_id,
            request_body_bytes.unwrap_or_default(),
            outbound_messages.len(),
            outbound_attachment_bytes,
            send_started.elapsed().as_millis()
        ));
    }
    Ok(outbound_messages)
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
) -> Result<DfsDirectReadClientResponse, NetworkError> {
    let send_started = Instant::now();
    match sender(&target_node_id, request, options).await {
        Ok(response) => {
            dfs_perf_trace(format_args!(
                "direct-read-head-async local_request={} target={} request={} bytes={} elapsed_ms={}",
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
    let (first_options, replay_total_timeout) =
        dfs_exact_response_replay_attempt_options(&command.routed_message.message, options);
    let response = match sender(&target_node_id, &command, first_options).await {
        Ok(response) => response,
        Err(first_error)
            if first_error.delivery_state()
                == Some(sinomo_core::network::NetworkDeliveryState::PossiblyApplied) =>
        {
            let Some(replay_options) = replay_total_timeout.and_then(|total_timeout| {
                dfs_exact_response_replay_options(options, total_timeout, send_started)
            }) else {
                return Err(dfs_peer_transport_network_error(
                    format!("send DFS message to {target_node_id}"),
                    first_error,
                ));
            };
            dfs_perf_trace(format_args!(
                "dfs-message-exact-response-replay-async local={} target={} request={} remaining_ms={} first_error={}",
                source_node_id,
                target_node_id,
                command.request_id,
                replay_options
                    .overall_timeout
                    .unwrap_or_default()
                    .as_millis(),
                first_error
            ));
            sender(&target_node_id, &command, replay_options)
                .await
                .map_err(|retry_error| {
                    dfs_exact_response_replay_exhausted(
                        format!("reconcile DFS message response from {target_node_id}"),
                        first_error,
                        retry_error,
                    )
                })?
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
            return Err(dfs_peer_transport_network_error(
                format!("send DFS message to {target_node_id}"),
                error,
            ));
        }
    };
    let outbound_messages = response.outbound_messages;
    if trace_enabled {
        let outbound_attachment_bytes =
            dfs_message_response_body_bytes(&command.request_id, &outbound_messages)?;
        dfs_perf_trace(format_args!(
            "dfs-message-send-async local={} target={} request={} request_body_bytes={} outbound_messages={} outbound_attachment_bytes={} elapsed_ms={}",
            source_node_id,
            target_node_id,
            command.request_id,
            request_body_bytes.unwrap_or_default(),
            outbound_messages.len(),
            outbound_attachment_bytes,
            send_started.elapsed().as_millis()
        ));
    }
    Ok(outbound_messages)
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

    fn send_routed_call_async<'a>(
        &'a mut self,
        target_node_id: &'a NodeId,
        call: DfsRoutedCall,
    ) -> DfsMessageTransportFuture<'a, Result<(), NetworkError>> {
        Box::pin(async move {
            self.send_routed_call_to_peer_async(target_node_id, call, true)
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
                    .send_routed_call_to_peer_async(&request.target_node_id, request.call, true)
                    .await;
                return Ok(vec![DfsRoutedSendOutcome {
                    target_node_id: request.target_node_id,
                    result,
                }]);
            }
            let limiter = Arc::new(tokio::sync::Semaphore::new(max_inflight.max(1)));
            let mut workers = tokio::task::JoinSet::new();
            for (index, request) in requests.into_iter().enumerate() {
                let limiter = Arc::clone(&limiter);
                let mut transport = self.fork_replica_call_send();
                workers.spawn(async move {
                    let _permit = limiter
                        .acquire_owned()
                        .await
                        .map_err(|_| NetworkError::Io {
                            operation: "acquire DFS call fan-out permit".to_string(),
                            detail: "DFS call fan-out limiter closed unexpectedly".to_string(),
                        })?;
                    let result = transport
                        .send_routed_call_to_peer_async(&request.target_node_id, request.call, true)
                        .await;
                    Ok::<_, NetworkError>((
                        index,
                        DfsRoutedSendOutcome {
                            target_node_id: request.target_node_id,
                            result,
                        },
                        transport.incoming_messages.drain(..).collect::<Vec<_>>(),
                    ))
                });
            }

            let mut completed = Vec::new();
            while let Some(joined) = workers.join_next().await {
                let completion = joined.map_err(|source| NetworkError::Io {
                    operation: "join async DFS call fan-out".to_string(),
                    detail: source.to_string(),
                })??;
                completed.push(completion);
            }
            Ok(self.finish_replica_call_fanout(completed))
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

fn record_direct_metadata_journal_fanout_outcome(
    outcome: Result<(NodeId, DfsMetadataJournalBatchResponse), NetworkError>,
    responses: &mut Vec<(NodeId, DfsMetadataJournalBatchResponse)>,
) {
    match outcome {
        Ok(response) => responses.push(response),
        Err(error) => dfs_perf_trace(format_args!(
            "direct-metadata-journal-fanout-peer-error error={error}"
        )),
    }
}

fn trace_direct_metadata_journal_fanout_peer_outcome(
    target_node_id: &NodeId,
    shard_id: &crate::DfsMetadataShardId,
    next_after_sequence: u64,
    authority_fanout: bool,
    started_at: Instant,
    outcome: &Result<(NodeId, DfsMetadataJournalBatchResponse), NetworkError>,
) {
    if !dfs_perf_trace_enabled() {
        return;
    }
    let elapsed_us = started_at.elapsed().as_micros();
    match outcome {
        Ok((_, response)) => dfs_perf_trace(format_args!(
            "direct-metadata-journal-fanout-peer target={} shard={} next_after_sequence={} authority={} elapsed_us={} outcome=response result={:?} verified_authority={}",
            target_node_id,
            shard_id.0,
            next_after_sequence,
            authority_fanout,
            elapsed_us,
            response.result,
            response.verified_authority_probe_id.is_some(),
        )),
        Err(error) => dfs_perf_trace(format_args!(
            "direct-metadata-journal-fanout-peer target={} shard={} next_after_sequence={} authority={} elapsed_us={} outcome=error error={error}",
            target_node_id, shard_id.0, next_after_sequence, authority_fanout, elapsed_us,
        )),
    }
}

async fn send_direct_metadata_journal_fanout_async(
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    prepared: Vec<DfsPreparedMetadataJournalBatchRpc>,
    deadline: Instant,
    max_inflight: usize,
) -> Vec<(NodeId, DfsMetadataJournalBatchResponse)> {
    let mut pending = VecDeque::from(prepared);
    let mut workers = tokio::task::JoinSet::new();
    let mut responses = Vec::new();
    let deadline_sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
    tokio::pin!(deadline_sleep);

    loop {
        while workers.len() < max_inflight {
            let Some(prepared) = pending.pop_front() else {
                break;
            };
            let peer_transport = Arc::clone(&peer_transport);
            workers.spawn(async move {
                let trace_context = dfs_perf_trace_enabled().then(|| {
                    (
                        Instant::now(),
                        prepared.target_node_id.clone(),
                        prepared.shard_id.clone(),
                        prepared.next_after_sequence,
                        prepared.authority_fanout,
                    )
                });
                let outcome = match prepared.call() {
                    Ok(call) => {
                        let rpc_response = peer_transport
                            .call_object_rpc_async(&prepared.target_node_id, call, prepared.options)
                            .await;
                        prepared.finish(rpc_response)
                    }
                    Err(error) => Err(error),
                };
                if let Some((started_at, target_node_id, shard_id, sequence, authority)) =
                    trace_context
                {
                    trace_direct_metadata_journal_fanout_peer_outcome(
                        &target_node_id,
                        &shard_id,
                        sequence,
                        authority,
                        started_at,
                        &outcome,
                    );
                }
                outcome
            });
        }
        if workers.is_empty() {
            break;
        }

        tokio::select! {
            biased;
            joined = workers.join_next() => {
                match joined {
                    Some(Ok(outcome)) => {
                        record_direct_metadata_journal_fanout_outcome(outcome, &mut responses);
                    }
                    Some(Err(error)) => dfs_perf_trace(format_args!(
                        "direct-metadata-journal-fanout-worker-error error={error}"
                    )),
                    None => break,
                }
            }
            _ = &mut deadline_sleep => {
                dfs_perf_trace(format_args!(
                    "direct-metadata-journal-fanout-budget-expired completed={} pending={} active={}",
                    responses.len(),
                    pending.len(),
                    workers.len()
                ));
                workers.abort_all();
                break;
            }
        }
    }

    // Dropping the set aborts and drops any late sibling RPC futures rather
    // than waiting for the slowest replica after the best-effort budget.
    drop(workers);
    responses
}

fn send_direct_metadata_journal_fanout_blocking(
    peer_transport: Arc<dyn DfsPeerTransport>,
    prepared: Vec<DfsPreparedMetadataJournalBatchRpc>,
    deadline: Instant,
    worker_slots: Arc<StdAtomicUsize>,
    max_inflight: usize,
) -> Vec<(NodeId, DfsMetadataJournalBatchResponse)> {
    let expected_results = prepared.len();
    if expected_results == 0 {
        return Vec::new();
    }
    let worker_count = expected_results.min(max_inflight);
    let pending = Arc::new(std::sync::Mutex::new(VecDeque::from(prepared)));
    let canceled = Arc::new(AtomicBool::new(false));
    let (completed_tx, completed_rx) = std::sync::mpsc::channel();

    struct BlockingFanoutWorkerSlot(Arc<StdAtomicUsize>);

    impl BlockingFanoutWorkerSlot {
        fn try_acquire(in_flight: Arc<StdAtomicUsize>, max_inflight: usize) -> Option<Self> {
            in_flight
                .fetch_update(AtomicOrdering::AcqRel, AtomicOrdering::Acquire, |active| {
                    (active < max_inflight).then_some(active + 1)
                })
                .ok()
                .map(|_| Self(in_flight))
        }
    }

    impl Drop for BlockingFanoutWorkerSlot {
        fn drop(&mut self) {
            self.0.fetch_sub(1, AtomicOrdering::AcqRel);
        }
    }

    let mut spawned_workers = 0usize;
    for worker_index in 0..worker_count {
        let Some(worker_slot) =
            BlockingFanoutWorkerSlot::try_acquire(Arc::clone(&worker_slots), max_inflight)
        else {
            dfs_perf_trace(format_args!(
                "direct-metadata-journal-fanout-blocking-worker-cap active={}",
                worker_slots.load(AtomicOrdering::Acquire)
            ));
            break;
        };
        let pending = Arc::clone(&pending);
        let canceled = Arc::clone(&canceled);
        let peer_transport = Arc::clone(&peer_transport);
        let completed_tx = completed_tx.clone();
        if std::thread::Builder::new()
            .name(format!("dfs-metadata-fanout-{worker_index}"))
            .spawn(move || {
                let _worker_slot = worker_slot;
                loop {
                    if canceled.load(AtomicOrdering::Acquire) || Instant::now() >= deadline {
                        break;
                    }
                    let prepared = pending
                        .lock()
                        .ok()
                        .and_then(|mut pending| pending.pop_front());
                    let Some(prepared) = prepared else {
                        break;
                    };
                    let trace_context = dfs_perf_trace_enabled().then(|| {
                        (
                            Instant::now(),
                            prepared.target_node_id.clone(),
                            prepared.shard_id.clone(),
                            prepared.next_after_sequence,
                            prepared.authority_fanout,
                        )
                    });
                    let outcome = match prepared.call() {
                        Ok(call) => {
                            let rpc_response = peer_transport.call_object_rpc(
                                &prepared.target_node_id,
                                call,
                                prepared.options,
                            );
                            prepared.finish(rpc_response)
                        }
                        Err(error) => Err(error),
                    };
                    if let Some((started_at, target_node_id, shard_id, sequence, authority)) =
                        trace_context
                    {
                        trace_direct_metadata_journal_fanout_peer_outcome(
                            &target_node_id,
                            &shard_id,
                            sequence,
                            authority,
                            started_at,
                            &outcome,
                        );
                    }
                    if canceled.load(AtomicOrdering::Acquire) || Instant::now() >= deadline {
                        break;
                    }
                    if completed_tx.send(outcome).is_err() {
                        break;
                    }
                }
            })
            .is_ok()
        {
            spawned_workers = spawned_workers.saturating_add(1);
        }
    }
    drop(completed_tx);

    let mut received_results = 0usize;
    let mut responses = Vec::new();
    while received_results < expected_results {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            break;
        };
        match completed_rx.recv_timeout(remaining) {
            Ok(outcome) => {
                received_results = received_results.saturating_add(1);
                record_direct_metadata_journal_fanout_outcome(outcome, &mut responses);
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => break,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    canceled.store(true, AtomicOrdering::Release);
    if received_results < expected_results {
        dfs_perf_trace(format_args!(
            "direct-metadata-journal-fanout-blocking-budget-expired completed={} observed={} expected={} workers={}",
            responses.len(),
            received_results,
            expected_results,
            spawned_workers
        ));
    }
    responses
}

impl DfsNodeTransport {
    fn send_metadata_journal_batches_direct_inner(
        &mut self,
        requests: &[(NodeId, DfsMetadataJournalBatchCommand)],
        response_timeout_after_encoding: Option<Duration>,
    ) -> Result<Option<Vec<(NodeId, DfsMetadataJournalBatchResponse)>>, NetworkError> {
        let mut groups = Vec::<DfsBorrowedMetadataJournalBatchDirectGroup<'_>>::new();
        for (target_node_id, command) in requests {
            if let Some(group) = groups.iter_mut().find(|group| group.command == command) {
                group.target_node_ids.push(target_node_id);
            } else {
                groups.push(DfsBorrowedMetadataJournalBatchDirectGroup {
                    target_node_ids: vec![target_node_id],
                    command,
                });
            }
        }
        self.send_borrowed_metadata_journal_batch_groups_direct_inner(
            &groups,
            response_timeout_after_encoding,
        )
    }

    fn send_grouped_metadata_journal_batches_direct_inner(
        &mut self,
        groups: &[DfsMetadataJournalBatchDirectGroup],
        response_timeout_after_encoding: Option<Duration>,
    ) -> Result<Option<Vec<(NodeId, DfsMetadataJournalBatchResponse)>>, NetworkError> {
        let groups = groups
            .iter()
            .map(|group| DfsBorrowedMetadataJournalBatchDirectGroup {
                target_node_ids: group.target_node_ids.iter().collect(),
                command: &group.command,
            })
            .collect::<Vec<_>>();
        self.send_borrowed_metadata_journal_batch_groups_direct_inner(
            &groups,
            response_timeout_after_encoding,
        )
    }

    fn send_borrowed_metadata_journal_batch_groups_direct_inner(
        &mut self,
        groups: &[DfsBorrowedMetadataJournalBatchDirectGroup<'_>],
        response_timeout_after_encoding: Option<Duration>,
    ) -> Result<Option<Vec<(NodeId, DfsMetadataJournalBatchResponse)>>, NetworkError> {
        let peer_transport = self.peer_transport.as_ref().cloned();
        let async_peer_transport = self.async_peer_transport.as_ref().cloned();
        if peer_transport.is_none() && async_peer_transport.is_none() {
            return Ok(None);
        }
        let request_count = groups
            .iter()
            .map(|group| group.target_node_ids.len())
            .sum::<usize>();
        let authority_fanout = request_count > 0
            && groups
                .iter()
                .filter(|group| !group.target_node_ids.is_empty())
                .all(|group| group.command.authority_probe.is_some());
        let max_inflight = if authority_fanout {
            DFS_METADATA_AUTHORITY_DIRECT_MAX_INFLIGHT
        } else {
            DFS_METADATA_CHECKPOINT_DIRECT_MAX_INFLIGHT
        };
        let fanout_budget = if authority_fanout {
            DFS_METADATA_AUTHORITY_DIRECT_FANOUT_BUDGET
        } else {
            DFS_METADATA_CHECKPOINT_DIRECT_FANOUT_BUDGET
        };
        let legacy_fanout_deadline = Instant::now()
            .checked_add(fanout_budget)
            .unwrap_or_else(Instant::now);
        // Existing checkpoint callers retain their historical whole-method
        // budget. Authority callers use the duration-aware entry point: their
        // already-installed operation deadline bounds local preparation, while
        // the shorter peer-response budget begins only after encoding below.
        let preparation_deadline = match response_timeout_after_encoding {
            Some(_) => self.operation_deadline,
            None => Some(
                self.operation_deadline
                    .map_or(legacy_fanout_deadline, |deadline| {
                        deadline.min(legacy_fanout_deadline)
                    }),
            ),
        };
        let mut prepared = Vec::with_capacity(request_count);
        let mut distinct_value_count = 0usize;
        let mut encoded_value_bytes = 0usize;
        for group in groups {
            if group.target_node_ids.is_empty() {
                continue;
            }
            for target_node_id in &group.target_node_ids {
                if *target_node_id == &self.local_node_id
                    || !self.known_remote_nodes.contains(*target_node_id)
                {
                    return Err(NetworkError::UnknownNode {
                        node_id: (*target_node_id).clone(),
                    });
                }
                if preparation_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                    return Ok(Some(Vec::new()));
                }
            }
            #[cfg(test)]
            {
                let delay = take_metadata_direct_encoding_test_delay();
                if !delay.is_zero() {
                    std::thread::sleep(delay);
                }
            }
            if preparation_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return Ok(Some(Vec::new()));
            }
            let replayable_call = Arc::new(dfs_metadata_journal_batch_replayable_rpc_call(
                group.command,
            )?);
            distinct_value_count = distinct_value_count.saturating_add(1);
            encoded_value_bytes =
                encoded_value_bytes.saturating_add(replayable_call.encoded_value_bytes());
            for target_node_id in &group.target_node_ids {
                if preparation_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                    return Ok(Some(Vec::new()));
                }
                let options = self.current_send_options(format!(
                    "send direct DFS metadata journal batch to {target_node_id}"
                ))?;
                prepared.push(DfsPreparedMetadataJournalBatchRpc {
                    target_node_id: (*target_node_id).clone(),
                    request_id: group.command.request_id.clone(),
                    shard_id: group.command.batch.shard_id.clone(),
                    next_after_sequence: group.command.batch.next_after_sequence,
                    authority_fanout: group.command.authority_probe.is_some(),
                    replayable_call: Arc::clone(&replayable_call),
                    options,
                });
            }
        }
        dfs_perf_trace(format_args!(
            "direct-metadata-journal-fanout-encoding authority={} requests={} distinct_values={} encoded_bytes={}",
            authority_fanout,
            prepared.len(),
            distinct_value_count,
            encoded_value_bytes,
        ));

        let response_deadline = match response_timeout_after_encoding {
            Some(response_timeout) => Instant::now()
                .checked_add(response_timeout)
                .unwrap_or_else(Instant::now),
            None => legacy_fanout_deadline,
        };
        let deadline = self
            .operation_deadline
            .map_or(response_deadline, |deadline| {
                deadline.min(response_deadline)
            });
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return Ok(Some(Vec::new()));
        };
        for request in &mut prepared {
            request.options.overall_timeout = Some(
                request
                    .options
                    .overall_timeout
                    .map_or(remaining, |timeout| timeout.min(remaining)),
            );
        }

        if let (Some(async_peer_transport), Ok(runtime)) =
            (async_peer_transport, tokio::runtime::Handle::try_current())
            && matches!(
                runtime.runtime_flavor(),
                tokio::runtime::RuntimeFlavor::MultiThread
            )
        {
            let responses = tokio::task::block_in_place(|| {
                runtime.block_on(send_direct_metadata_journal_fanout_async(
                    async_peer_transport,
                    prepared,
                    deadline,
                    max_inflight,
                ))
            });
            return Ok(Some(responses));
        }

        let Some(peer_transport) = peer_transport else {
            return Ok(None);
        };
        let responses = send_direct_metadata_journal_fanout_blocking(
            peer_transport,
            prepared,
            deadline,
            Arc::clone(&self.blocking_metadata_fanout_workers),
            max_inflight,
        );
        Ok(Some(responses))
    }
}

impl DfsMessageTransport for DfsNodeTransport {
    fn send_routed_call(
        &mut self,
        target_node_id: &NodeId,
        call: DfsRoutedCall,
    ) -> Result<(), NetworkError> {
        self.send_routed_call_to_peer(target_node_id, call, true)
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
            let result = self.send_routed_call_to_peer(&request.target_node_id, request.call, true);
            return Ok(vec![DfsRoutedSendOutcome {
                target_node_id: request.target_node_id,
                result,
            }]);
        }
        let worker_count = max_inflight.max(1).min(requests.len());
        let pending = std::sync::Mutex::new(VecDeque::from_iter(requests.into_iter().enumerate()));
        let completed = std::sync::Mutex::new(Vec::new());

        std::thread::scope(|scope| -> Result<(), NetworkError> {
            let mut workers = Vec::with_capacity(worker_count);
            for _ in 0..worker_count {
                let pending = &pending;
                let completed = &completed;
                let mut transport = self.fork_replica_call_send();
                workers.push(scope.spawn(move || {
                    loop {
                        let Some((index, request)) =
                            pending.lock().expect("DFS call fan-out queue").pop_front()
                        else {
                            break;
                        };
                        let result = transport.send_routed_call_to_peer(
                            &request.target_node_id,
                            request.call,
                            true,
                        );
                        let returned_messages =
                            transport.incoming_messages.drain(..).collect::<Vec<_>>();
                        completed.lock().expect("DFS call fan-out results").push((
                            index,
                            DfsRoutedSendOutcome {
                                target_node_id: request.target_node_id,
                                result,
                            },
                            returned_messages,
                        ));
                    }
                }));
            }
            for worker in workers {
                worker.join().map_err(|_| NetworkError::Io {
                    operation: "join DFS call fan-out".to_string(),
                    detail: "DFS call fan-out worker panicked".to_string(),
                })?;
            }
            Ok(())
        })?;

        let completed = completed.into_inner().map_err(|_| NetworkError::Io {
            operation: "collect DFS call fan-out".to_string(),
            detail: "DFS call fan-out result lock is poisoned".to_string(),
        })?;
        Ok(self.finish_replica_call_fanout(completed))
    }

    fn send_detached_routed_message(
        &mut self,
        target_node_id: &NodeId,
        message: DfsRoutedMessage,
    ) -> Result<(), NetworkError> {
        self.send_routed_message_to_peer_detached(target_node_id, message, true)
    }

    fn send_metadata_journal_batch_direct(
        &mut self,
        target_node_id: &NodeId,
        command: &DfsMetadataJournalBatchCommand,
    ) -> Result<Option<DfsMetadataJournalBatchResponse>, NetworkError> {
        if target_node_id == &self.local_node_id
            || !self.known_remote_nodes.contains(target_node_id)
        {
            return Err(NetworkError::UnknownNode {
                node_id: target_node_id.clone(),
            });
        }
        let Some(peer_transport) = self.peer_transport.as_ref().cloned() else {
            return Ok(None);
        };
        let call = dfs_metadata_journal_batch_rpc_call(command)?;
        let options = self.current_send_options(format!(
            "send direct DFS metadata journal batch to {target_node_id}"
        ))?;
        let response = peer_transport
            .call_object_rpc(target_node_id, call, options)
            .map_err(|source| NetworkError::Io {
                operation: format!("send direct DFS metadata journal batch to {target_node_id}"),
                detail: source.to_string(),
            })?;
        let response =
            value_response_from_rpc_result(response).map_err(|source| NetworkError::Io {
                operation: format!(
                    "decode direct DFS metadata journal response from {target_node_id}"
                ),
                detail: source.to_string(),
            })?;
        let response = decode_dfs_metadata_journal_batch_rpc_response(&response)?;
        if response.request_id != command.request_id
            || response.shard_id != command.batch.shard_id
            || response.next_after_sequence != command.batch.next_after_sequence
        {
            return Err(NetworkError::Serialization {
                detail: format!(
                    "{target_node_id} returned mismatched direct metadata journal response"
                ),
            });
        }
        Ok(Some(response))
    }

    fn request_content_root_catalog_page_direct(
        &mut self,
        target_node_id: &NodeId,
        request: &DfsContentRootCatalogPageRequest,
    ) -> Result<Option<DfsContentRootCatalogPage>, NetworkError> {
        self.request_content_root_catalog_page(target_node_id, request)
            .map(Some)
    }

    fn send_metadata_journal_batches_direct(
        &mut self,
        requests: &[(NodeId, DfsMetadataJournalBatchCommand)],
    ) -> Result<Option<Vec<(NodeId, DfsMetadataJournalBatchResponse)>>, NetworkError> {
        self.send_metadata_journal_batches_direct_inner(requests, None)
    }

    fn send_metadata_journal_batches_direct_with_response_timeout(
        &mut self,
        requests: &[(NodeId, DfsMetadataJournalBatchCommand)],
        response_timeout: Duration,
    ) -> Result<Option<Vec<(NodeId, DfsMetadataJournalBatchResponse)>>, NetworkError> {
        self.send_metadata_journal_batches_direct_inner(requests, Some(response_timeout))
    }

    fn send_grouped_metadata_journal_batches_direct_with_response_timeout(
        &mut self,
        groups: &[DfsMetadataJournalBatchDirectGroup],
        response_timeout: Duration,
    ) -> Result<Option<Vec<(NodeId, DfsMetadataJournalBatchResponse)>>, NetworkError> {
        self.send_grouped_metadata_journal_batches_direct_inner(groups, Some(response_timeout))
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
        DFS_CONTENT_ROOT_CATALOG_PREFIX_PROOF_VERSION, DfsContentRootCatalogCoverage,
        DfsContentRootCatalogPage, DfsContentRootCatalogPageRequest,
        DfsContentRootCatalogShardIdentity, DfsDistributedWriteCommand,
        DfsMetadataAuthorityOperation, DfsMetadataAuthorityProbe, DfsMetadataShardAssignment,
        DfsMetadataShardConfig, DfsNamespaceCommand, DfsNamespaceOperation, DfsObjectId,
        DfsReadResult, DfsReplicationPolicy, DfsStageBlobCommand, DfsVersion,
        dfs_content_root_catalog_topology_digest, sifs_file_system_id,
    };
    use sinomo_core::{DfsObjectPolicy, DfsWriteRequest};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex, mpsc};

    #[test]
    fn dfs_peer_rpc_options_require_a_main_daemon_terminal() {
        let overall_timeout = Duration::from_secs(7);
        let actual = DfsObjectRpcSendOptions::default()
            .with_overall_timeout(overall_timeout)
            .cluster_rpc_options();
        let expected = sinomo_core::network::ClusterObjectRpcOptions::default()
            .with_overall_timeout(overall_timeout)
            .with_destination_process_affinity(
                sinomo_core::network::DestinationProcessAffinity::MainDaemon,
            );

        assert_eq!(actual, expected);
    }

    #[test]
    fn stable_reconciliation_responses_return_on_served_call_without_peer_redial() {
        let authority = node_id("node_stable_reconciliation_authority");
        let requester = node_id("node_stable_reconciliation_requester");
        let peer_sends = Arc::new(AtomicUsize::new(0));
        let observed_peer_sends = Arc::clone(&peer_sends);
        let sender: DfsMessageSender = Arc::new(move |target, command, _options| {
            observed_peer_sends.fetch_add(1, Ordering::SeqCst);
            Err(DfsPeerTransportError::new(format!(
                "stable reconciliation response unexpectedly redialed {target} for {}",
                command.request_id
            )))
        });
        let mut transport = DfsNodeServedMessageTransport::new_with_message_sender_for_nodes(
            authority.clone(),
            vec![requester.clone()],
            sender,
            None,
            Duration::from_secs(1),
        );
        let served_request = test_namespace_message(&requester, &authority, 1);
        transport
            .begin_message(&served_request)
            .expect("begin served reconciliation request");

        let expected = stable_reconciliation_test_responses()
            .into_iter()
            .enumerate()
            .map(|(index, message)| DfsRoutedMessage {
                source_node_id: authority.clone(),
                target_node_id: requester.clone(),
                frame_id: index as u64 + 2,
                message_id: index as u64 + 2,
                message,
            })
            .collect::<Vec<_>>();
        for response in expected.iter().cloned() {
            transport
                .send_routed_call(&requester, DfsRoutedCall::value(response))
                .expect("return stable reconciliation response on served call");
        }

        assert_eq!(peer_sends.load(Ordering::SeqCst), 0);
        assert_eq!(transport.finish_message(), expected);
    }

    #[derive(Default)]
    struct RecordingAsyncMessagePeerTransport {
        calls: AtomicUsize,
    }

    impl RecordingAsyncMessagePeerTransport {
        fn respond(&self, call: ObjectRpcCall) -> Result<ObjectRpcResult, DfsPeerTransportError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let request = materialize_dfs_value_call_sync(call, usize::MAX)?;
            let command = request
                .decode_body::<DfsMessageCommand>()
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
            ObjectRpcResponse::encode_body(
                &request.route,
                &DfsMessageResponse {
                    request_id: command.request_id,
                    outbound_messages: Vec::new(),
                },
            )
            .map(ObjectRpcResult::value)
            .map_err(|source| DfsPeerTransportError::new(source.to_string()))
        }
    }

    impl DfsPeerTransport for RecordingAsyncMessagePeerTransport {
        fn call_object_rpc(
            &self,
            _target_node_id: &NodeId,
            call: ObjectRpcCall,
            _options: DfsObjectRpcSendOptions,
        ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
            self.respond(call)
        }
    }

    impl AsyncDfsPeerTransport for RecordingAsyncMessagePeerTransport {
        fn call_object_rpc_async<'a>(
            &'a self,
            _target_node_id: &'a NodeId,
            call: ObjectRpcCall,
            _options: DfsObjectRpcSendOptions,
        ) -> DfsPeerTransportFuture<'a, Result<ObjectRpcResult, DfsPeerTransportError>> {
            Box::pin(async move { self.respond(call) })
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn served_transport_uses_async_peer_for_non_return_traffic() {
        let local = node_id("node_served_async_local");
        let caller = node_id("node_served_async_caller");
        let replica = node_id("node_served_async_replica");
        let peer = Arc::new(RecordingAsyncMessagePeerTransport::default());
        let sync_peer: Arc<dyn DfsPeerTransport> = peer.clone();
        let async_peer: Arc<dyn AsyncDfsPeerTransport> = peer.clone();
        let mut transport = DfsNodeServedMessageTransport::new_with_peer_transports_for_nodes(
            local.clone(),
            [caller.clone(), replica.clone()],
            sync_peer,
            async_peer,
            Duration::from_secs(1),
        );
        transport
            .begin_message(&test_namespace_message(&caller, &local, 1))
            .expect("begin served caller request");

        transport
            .send_routed_call_async(
                &replica,
                DfsRoutedCall::value(test_namespace_message(&local, &replica, 2)),
            )
            .await
            .expect("served request must retain its async peer sender");

        assert_eq!(peer.calls.load(Ordering::SeqCst), 1);
        assert!(transport.finish_message().is_empty());
    }

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
                authority_shards: Vec::new(),
                operation: DfsNamespaceOperation::Mkdir {
                    path: format!("/namespace-{frame_id}"),
                },
            }),
        }
    }

    fn test_commit_version_message(
        source: &NodeId,
        target: &NodeId,
        frame_id: u64,
    ) -> DfsRoutedMessage {
        DfsRoutedMessage {
            source_node_id: source.clone(),
            target_node_id: target.clone(),
            frame_id,
            message_id: frame_id,
            message: DfsNetworkMessage::CommitVersion(crate::DfsCommitVersionCommand {
                request_id: format!("commit_request_{frame_id}"),
                intent_id: format!("intent_{frame_id}"),
                file_system_id: sifs_file_system_id(),
                object_id: DfsObjectId::from_legacy_named(format!("commit_object_{frame_id}"))
                    .expect("object id"),
                expected_base_version: None,
                version: DfsVersion::from_legacy_named(format!("commit_version_{frame_id}"))
                    .expect("version"),
                content_hash: format!("blake3:commit-{frame_id}"),
                object_metadata: None,
                authority_shard: None,
            }),
        }
    }

    fn test_commit_version_rpc_response(
        command: &DfsMessageCommand,
        result: DfsBlobTransferResult,
    ) -> DfsMessageResponse {
        let DfsNetworkMessage::CommitVersion(commit) = &command.routed_message.message else {
            panic!("test response requires CommitVersion")
        };
        DfsMessageResponse {
            request_id: command.request_id.clone(),
            outbound_messages: vec![DfsRoutedMessage {
                source_node_id: command.routed_message.target_node_id.clone(),
                target_node_id: command.routed_message.source_node_id.clone(),
                frame_id: command.routed_message.frame_id.saturating_add(100),
                message_id: command.routed_message.message_id.saturating_add(100),
                message: DfsNetworkMessage::CommitVersionResponse(
                    crate::DfsCommitVersionResponse {
                        request_id: commit.request_id.clone(),
                        intent_id: commit.intent_id.clone(),
                        file_system_id: commit.file_system_id.clone(),
                        object_id: commit.object_id.clone(),
                        version: commit.version.clone(),
                        replica_node_id: command.routed_message.target_node_id.clone(),
                        result,
                    },
                ),
            }],
        }
    }

    fn test_committed_promotion_rpc_response(command: &DfsMessageCommand) -> DfsMessageResponse {
        let DfsNetworkMessage::CommitVersion(commit) = &command.routed_message.message else {
            panic!("test response requires CommitVersion")
        };
        test_commit_version_rpc_response(
            command,
            DfsBlobTransferResult::Committed {
                content_hash: commit.content_hash.clone(),
            },
        )
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
                publication: Default::default(),
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
                requester_blob_pre_staged: true,
                desired_replica_nodes: vec![source.clone()],
                policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
                object_metadata: None,
                authority_shard: None,
                touch_parent_on_create: true,
                foreground_replica_repair: false,
                exclusive_create: false,
            }),
        }
    }

    fn test_distributed_write_response_message(
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
            message: DfsNetworkMessage::DistributedWriteResponse(
                crate::DfsDistributedWriteResponse {
                    request_id: format!("write_request_{frame_id}"),
                    object_id,
                    result: crate::DfsDistributedWriteResult::Rejected {
                        reason: "test completed response".to_string(),
                        current_primary_node_id: None,
                    },
                    metadata_journal_batches: Vec::new(),
                },
            ),
        }
    }

    fn test_stage_attachment_send(
        source: &NodeId,
        target: &NodeId,
        frame_id: u64,
        request_id: &str,
    ) -> DfsRoutedSend {
        let bytes = vec![u8::try_from(frame_id).unwrap_or_default(); 64];
        let content_hash = crate::content_hash(&bytes);
        DfsRoutedSend::new(
            target.clone(),
            DfsRoutedCall::with_attachment(
                DfsRoutedMessage {
                    source_node_id: source.clone(),
                    target_node_id: target.clone(),
                    frame_id,
                    message_id: frame_id,
                    message: DfsNetworkMessage::StageBlob(DfsStageBlobCommand {
                        request_id: request_id.to_string(),
                        intent_id: format!("intent-{frame_id}"),
                        file_system_id: sifs_file_system_id(),
                        object_id: DfsObjectId::from_legacy_named(format!("object-{frame_id}"))
                            .expect("object id"),
                        expected_base_version: None,
                        version: DfsVersion::from_legacy_named(format!("version-{frame_id}"))
                            .expect("version"),
                        content_hash: content_hash.clone(),
                        bytes: Vec::new(),
                        object_metadata: None,
                        authority_shard: None,
                    }),
                },
                DfsOutboundAttachment::from_bytes(bytes, content_hash),
            ),
        )
    }

    #[test]
    fn routed_call_builder_preserves_value_and_attachment_body_intent() {
        let source = node_id("session_builder_source");
        let target = node_id("session_builder_target");

        let value_message = test_message(&source, &target, 6);
        let (value_command, value_call) =
            dfs_routed_call_rpc_call(DfsRoutedCall::value(value_message))
                .expect("value session call");
        assert!(value_call.has_streamed_value_body());
        let value_request = materialize_dfs_value_call_sync(value_call, usize::MAX)
            .expect("materialize value call at compatibility test boundary");
        assert_eq!(
            value_request
                .decode_body::<DfsMessageCommand>()
                .expect("value command"),
            value_command
        );

        let attachment_bytes = vec![7_u8; 64];
        let attachment_send = test_stage_attachment_send(&source, &target, 7, "stage-request");
        let (attachment_command, attachment_call) =
            dfs_routed_call_rpc_call(attachment_send.call).expect("attachment RPC call");
        assert!(!attachment_call.body.is_empty());
        let head =
            crate::body_rpc::decode_dfs_attachment_message_rpc_request(&attachment_call.request)
                .expect("attachment head");
        assert_eq!(head.command, attachment_command);
        assert_eq!(head.attachment.total_size_bytes, 64);
        assert_eq!(
            collect_dfs_attachment_source_sync(attachment_call.body, attachment_bytes.len())
                .expect("attachment body"),
            attachment_bytes
        );

        let empty_hash = crate::content_hash(&[]);
        let empty_repair_request_id = "empty-repair-request";
        let empty_repair_message = DfsRoutedMessage {
            source_node_id: source,
            target_node_id: target,
            frame_id: 8,
            message_id: 8,
            message: DfsNetworkMessage::RepairReplica(crate::DfsRepairReplicaCommand {
                request_id: empty_repair_request_id.to_string(),
                intent_id: "empty-repair-intent".to_string(),
                file_system_id: sifs_file_system_id(),
                object_id: DfsObjectId::from_legacy_named("empty-repair-object")
                    .expect("object id"),
                expected_base_version: None,
                version: DfsVersion::from_legacy_named("empty-repair-version").expect("version"),
                content_hash: empty_hash.clone(),
                bytes: Vec::new(),
                object_metadata: None,
                authority_shard: None,
            }),
        };
        let (empty_repair_command, empty_repair_call) =
            dfs_routed_call_rpc_call(DfsRoutedCall::with_attachment(
                empty_repair_message,
                DfsOutboundAttachment::from_bytes(Vec::new(), empty_hash.clone()),
            ))
            .expect("zero-byte repair RPC call");
        assert!(
            empty_repair_call.has_streamed_value_body(),
            "a zero-byte semantic payload must use the ordinary typed value path"
        );
        let empty_repair_request = materialize_dfs_value_call_sync(empty_repair_call, usize::MAX)
            .expect("materialize zero-byte repair value call");
        let decoded = empty_repair_request
            .decode_body::<DfsMessageCommand>()
            .expect("zero-byte repair must decode as an ordinary DFS command");
        assert_eq!(decoded, empty_repair_command);
        let DfsNetworkMessage::RepairReplica(repair) = decoded.routed_message.message else {
            panic!("expected zero-byte repair message");
        };
        assert_eq!(repair.request_id, empty_repair_request_id);
        assert_eq!(repair.content_hash, empty_hash);
        assert!(repair.bytes.is_empty());
    }

    fn stage_attachment_rpc_response(
        call: &ObjectRpcCall,
        calls: &Mutex<Vec<(NodeId, String)>>,
        target_node_id: &NodeId,
    ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
        let request = &call.request;
        let command = if call.body.is_empty() {
            request
                .decode_body()
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))?
        } else {
            crate::body_rpc::decode_dfs_attachment_message_rpc_request(request)
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))?
                .command
        };
        let request_id = match &command.routed_message.message {
            DfsNetworkMessage::StageBlob(command) => command.request_id.clone(),
            other => {
                return Err(DfsPeerTransportError::new(format!(
                    "expected stage attachment request, got {other:?}"
                )));
            }
        };
        calls
            .lock()
            .expect("body calls")
            .push((target_node_id.clone(), request_id));
        ObjectRpcResponse::encode_body(
            &request.route,
            &DfsMessageResponse {
                request_id: command.request_id,
                outbound_messages: Vec::new(),
            },
        )
        .map(ObjectRpcResult::value)
        .map_err(|source| DfsPeerTransportError::new(source.to_string()))
    }

    struct ConcurrentAsyncBodyPeerTransport {
        active: AtomicUsize,
        max_active: AtomicUsize,
        first_pair: tokio::sync::Barrier,
        failed_target: NodeId,
        paired_target: NodeId,
        calls: Mutex<Vec<(NodeId, String)>>,
    }

    impl DfsPeerTransport for ConcurrentAsyncBodyPeerTransport {
        fn call_object_rpc(
            &self,
            target_node_id: &NodeId,
            call: ObjectRpcCall,
            _options: DfsObjectRpcSendOptions,
        ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
            stage_attachment_rpc_response(&call, &self.calls, target_node_id)
        }
    }

    impl AsyncDfsPeerTransport for ConcurrentAsyncBodyPeerTransport {
        fn call_object_rpc_async<'a>(
            &'a self,
            target_node_id: &'a NodeId,
            call: ObjectRpcCall,
            _options: DfsObjectRpcSendOptions,
        ) -> DfsPeerTransportFuture<'a, Result<ObjectRpcResult, DfsPeerTransportError>> {
            Box::pin(async move {
                let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
                self.max_active.fetch_max(active, Ordering::SeqCst);
                if target_node_id == &self.failed_target || target_node_id == &self.paired_target {
                    self.first_pair.wait().await;
                }
                self.active.fetch_sub(1, Ordering::SeqCst);
                if target_node_id == &self.failed_target {
                    // Record the exact request before returning its isolated
                    // transport failure.
                    let _ = stage_attachment_rpc_response(&call, &self.calls, target_node_id)?;
                    return Err(DfsPeerTransportError::new("injected N2 transfer failure"));
                }
                stage_attachment_rpc_response(&call, &self.calls, target_node_id)
            })
        }
    }

    struct ConcurrentSyncBodyPeerTransport {
        active: AtomicUsize,
        max_active: AtomicUsize,
        entered: Mutex<usize>,
        both_entered: Condvar,
        failed_target: NodeId,
        paired_target: NodeId,
        calls: Mutex<Vec<(NodeId, String)>>,
    }

    impl DfsPeerTransport for ConcurrentSyncBodyPeerTransport {
        fn call_object_rpc(
            &self,
            target_node_id: &NodeId,
            call: ObjectRpcCall,
            _options: DfsObjectRpcSendOptions,
        ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(active, Ordering::SeqCst);
            if target_node_id == &self.failed_target || target_node_id == &self.paired_target {
                let mut entered = self.entered.lock().expect("sync body entries");
                *entered += 1;
                self.both_entered.notify_all();
                if *entered < 2 {
                    let (guard, _) = self
                        .both_entered
                        .wait_timeout_while(entered, Duration::from_secs(1), |entered| *entered < 2)
                        .expect("sync body overlap wait");
                    entered = guard;
                }
                drop(entered);
            }
            self.active.fetch_sub(1, Ordering::SeqCst);
            if target_node_id == &self.failed_target {
                let _ = stage_attachment_rpc_response(&call, &self.calls, target_node_id)?;
                return Err(DfsPeerTransportError::new("injected N2 transfer failure"));
            }
            stage_attachment_rpc_response(&call, &self.calls, target_node_id)
        }
    }

    impl AsyncDfsPeerTransport for ConcurrentSyncBodyPeerTransport {
        fn call_object_rpc_async<'a>(
            &'a self,
            target_node_id: &'a NodeId,
            call: ObjectRpcCall,
            options: DfsObjectRpcSendOptions,
        ) -> DfsPeerTransportFuture<'a, Result<ObjectRpcResult, DfsPeerTransportError>> {
            Box::pin(async move { self.call_object_rpc(target_node_id, call, options) })
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_replica_body_fanout_overlaps_n2_n3_and_isolates_one_failure() {
        let local = node_id("node_body_fanout_async_n1");
        let n2 = node_id("node_body_fanout_async_n2");
        let n3 = node_id("node_body_fanout_async_n3");
        let n4 = node_id("node_body_fanout_async_n4");
        let peer = Arc::new(ConcurrentAsyncBodyPeerTransport {
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            first_pair: tokio::sync::Barrier::new(2),
            failed_target: n2.clone(),
            paired_target: n3.clone(),
            calls: Mutex::new(Vec::new()),
        });
        let sync_peer: Arc<dyn DfsPeerTransport> = peer.clone();
        let async_peer: Arc<dyn AsyncDfsPeerTransport> = peer.clone();
        let mut transport = DfsNodeTransport::with_peer_transports_for_nodes(
            local.clone(),
            vec![n2.clone(), n3.clone(), n4.clone()],
            Duration::from_secs(2),
            sync_peer,
            async_peer,
        );

        let outcomes = tokio::time::timeout(
            Duration::from_secs(2),
            transport.send_routed_calls_bounded_async(
                vec![
                    test_stage_attachment_send(&local, &n2, 1, "stage-n2"),
                    test_stage_attachment_send(&local, &n3, 2, "stage-n3"),
                    test_stage_attachment_send(&local, &n4, 3, "stage-n4"),
                ],
                2,
            ),
        )
        .await
        .expect("N2 and N3 should enter concurrently")
        .expect("fan-out orchestration");

        assert_eq!(peer.max_active.load(Ordering::SeqCst), 2);
        assert_eq!(outcomes.len(), 3);
        assert_eq!(outcomes[0].target_node_id, n2);
        assert!(outcomes[0].result.is_err());
        assert_eq!(outcomes[1].target_node_id, n3);
        assert!(outcomes[1].result.is_ok());
        assert_eq!(outcomes[2].target_node_id, n4);
        assert!(outcomes[2].result.is_ok());
        let mut calls = peer.calls.lock().expect("body calls").clone();
        calls.sort();
        let mut expected_calls = vec![
            (n2, "stage-n2".to_string()),
            (n3, "stage-n3".to_string()),
            (n4, "stage-n4".to_string()),
        ];
        expected_calls.sort();
        assert_eq!(calls, expected_calls);
    }

    #[test]
    fn sync_replica_body_fanout_overlaps_n2_n3_and_isolates_one_failure() {
        let local = node_id("node_body_fanout_sync_n1");
        let n2 = node_id("node_body_fanout_sync_n2");
        let n3 = node_id("node_body_fanout_sync_n3");
        let n4 = node_id("node_body_fanout_sync_n4");
        let peer = Arc::new(ConcurrentSyncBodyPeerTransport {
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            entered: Mutex::new(0),
            both_entered: Condvar::new(),
            failed_target: n2.clone(),
            paired_target: n3.clone(),
            calls: Mutex::new(Vec::new()),
        });
        let sync_peer: Arc<dyn DfsPeerTransport> = peer.clone();
        let async_peer: Arc<dyn AsyncDfsPeerTransport> = peer.clone();
        let mut transport = DfsNodeTransport::with_peer_transports_for_nodes(
            local.clone(),
            vec![n2.clone(), n3.clone(), n4.clone()],
            Duration::from_secs(2),
            sync_peer,
            async_peer,
        );

        let outcomes = transport
            .send_routed_calls_bounded(
                vec![
                    test_stage_attachment_send(&local, &n2, 1, "stage-n2"),
                    test_stage_attachment_send(&local, &n3, 2, "stage-n3"),
                    test_stage_attachment_send(&local, &n4, 3, "stage-n4"),
                ],
                2,
            )
            .expect("fan-out orchestration");

        assert_eq!(peer.max_active.load(Ordering::SeqCst), 2);
        assert_eq!(outcomes.len(), 3);
        assert_eq!(outcomes[0].target_node_id, n2);
        assert!(outcomes[0].result.is_err());
        assert_eq!(outcomes[1].target_node_id, n3);
        assert!(outcomes[1].result.is_ok());
        assert_eq!(outcomes[2].target_node_id, n4);
        assert!(outcomes[2].result.is_ok());
        let mut calls = peer.calls.lock().expect("body calls").clone();
        calls.sort();
        let mut expected_calls = vec![
            (n2, "stage-n2".to_string()),
            (n3, "stage-n3".to_string()),
            (n4, "stage-n4".to_string()),
        ];
        expected_calls.sort();
        assert_eq!(calls, expected_calls);
    }

    struct MetadataFanoutActiveGuard<'a>(&'a AtomicUsize);

    impl Drop for MetadataFanoutActiveGuard<'_> {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    struct MetadataFanoutPeerTransport {
        active: AtomicUsize,
        max_active: AtomicUsize,
        failed_target: Option<NodeId>,
        slow_target: Option<NodeId>,
        fast_delay: Duration,
        slow_delay: Duration,
    }

    impl MetadataFanoutPeerTransport {
        fn enter(&self) -> MetadataFanoutActiveGuard<'_> {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(active, Ordering::SeqCst);
            MetadataFanoutActiveGuard(&self.active)
        }

        fn delay_for(&self, target_node_id: &NodeId) -> Duration {
            if self.slow_target.as_ref() == Some(target_node_id) {
                self.slow_delay
            } else {
                self.fast_delay
            }
        }

        fn finish_call(
            &self,
            target_node_id: &NodeId,
            call: ObjectRpcCall,
        ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
            if self.failed_target.as_ref() == Some(target_node_id) {
                return Err(DfsPeerTransportError::new(format!(
                    "injected metadata fan-out failure for {target_node_id}"
                )));
            }
            let request = materialize_dfs_value_call_sync(call, usize::MAX)?;
            let command: DfsMetadataJournalBatchCommand = request
                .decode_body()
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
            ObjectRpcResponse::encode_body(
                &request.route,
                &DfsMetadataJournalBatchResponse {
                    request_id: command.request_id,
                    shard_id: command.batch.shard_id,
                    next_after_sequence: command.batch.next_after_sequence,
                    applied_entries: command.batch.entries.len(),
                    verified_prefix_only: command.verify_prefix_only,
                    verified_authority_probe_id: None,
                    result: crate::DfsMetadataJournalBatchResult::Applied,
                },
            )
            .map(ObjectRpcResult::value)
            .map_err(|source| DfsPeerTransportError::new(source.to_string()))
        }
    }

    impl DfsPeerTransport for MetadataFanoutPeerTransport {
        fn call_object_rpc(
            &self,
            target_node_id: &NodeId,
            call: ObjectRpcCall,
            _options: DfsObjectRpcSendOptions,
        ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
            let _active = self.enter();
            std::thread::sleep(self.delay_for(target_node_id));
            self.finish_call(target_node_id, call)
        }
    }

    impl AsyncDfsPeerTransport for MetadataFanoutPeerTransport {
        fn call_object_rpc_async<'a>(
            &'a self,
            target_node_id: &'a NodeId,
            call: ObjectRpcCall,
            _options: DfsObjectRpcSendOptions,
        ) -> DfsPeerTransportFuture<'a, Result<ObjectRpcResult, DfsPeerTransportError>> {
            Box::pin(async move {
                let _active = self.enter();
                tokio::time::sleep(self.delay_for(target_node_id)).await;
                self.finish_call(target_node_id, call)
            })
        }
    }

    fn test_metadata_fanout_requests(
        local_node_id: &NodeId,
        targets: &[NodeId],
    ) -> Vec<(NodeId, DfsMetadataJournalBatchCommand)> {
        let shard_id = crate::DfsMetadataShardId::new("root");
        targets
            .iter()
            .enumerate()
            .map(|(index, target_node_id)| {
                (
                    target_node_id.clone(),
                    DfsMetadataJournalBatchCommand {
                        request_id: format!("metadata-fanout-{index}"),
                        source_node_id: Some(local_node_id.clone()),
                        file_system_id: None,
                        authority_epoch: None,
                        verify_prefix_only: false,
                        prefix_probe: None,
                        authority_probe: None,
                        durable_replication: None,
                        batch: crate::DfsMetadataJournalBatch {
                            shard_id: shard_id.clone(),
                            entries: Vec::new(),
                            next_after_sequence: 7,
                        },
                    },
                )
            })
            .collect()
    }

    fn test_metadata_authority_fanout_requests(
        local_node_id: &NodeId,
        targets: &[NodeId],
    ) -> Vec<(NodeId, DfsMetadataJournalBatchCommand)> {
        let file_system_id = sifs_file_system_id();
        let expected_shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: local_node_id.clone(),
            replica_node_ids: targets.to_vec(),
            epoch: 7,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        test_metadata_fanout_requests(local_node_id, targets)
            .into_iter()
            .enumerate()
            .map(|(index, (target_node_id, mut command))| {
                command.file_system_id = Some(file_system_id.clone());
                command.authority_epoch = Some(expected_shard.epoch);
                command.authority_probe = Some(DfsMetadataAuthorityProbe {
                    probe_id: format!("metadata-authority-fanout-{index}"),
                    expected_shard: expected_shard.clone(),
                    operation: DfsMetadataAuthorityOperation::NamespaceMutation {
                        operation_id: "metadata-authority-fanout-operation".to_string(),
                        file_system_id: file_system_id.clone(),
                        prepare_sequence: command.batch.next_after_sequence,
                        payload_digest: "blake3:metadata-authority-fanout".to_string(),
                    },
                    durable_journal_ack: false,
                    journal_frames: Vec::new(),
                    prepared_prefix: None,
                });
                (target_node_id, command)
            })
            .collect()
    }

    #[test]
    fn blocking_metadata_journal_fanout_caps_overlap_and_preserves_partial_success() {
        let local = node_id("node_metadata_fanout_blocking_n1");
        let targets = (2..=5)
            .map(|index| node_id(&format!("node_metadata_fanout_blocking_n{index}")))
            .collect::<Vec<_>>();
        let peer = Arc::new(MetadataFanoutPeerTransport {
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            failed_target: Some(targets[0].clone()),
            slow_target: None,
            fast_delay: Duration::from_millis(25),
            slow_delay: Duration::from_secs(2),
        });
        let sync_peer: Arc<dyn DfsPeerTransport> = peer.clone();
        let mut transport = DfsNodeServedMessageTransport::new_with_peer_transport_for_nodes(
            local.clone(),
            targets.clone(),
            sync_peer,
            Duration::from_secs(2),
        );

        let responses = transport
            .send_metadata_journal_batches_direct(&test_metadata_fanout_requests(&local, &targets))
            .expect("blocking metadata fan-out")
            .expect("blocking peer retained");

        assert_eq!(peer.max_active.load(Ordering::SeqCst), 2);
        assert_eq!(responses.len(), targets.len() - 1);
        assert!(
            responses
                .iter()
                .all(|(target_node_id, _)| target_node_id != &targets[0]),
            "one failed peer must not discard successful sibling responses"
        );
    }

    #[test]
    fn blocking_metadata_authority_fanout_starts_the_full_shard_together() {
        let local = node_id("node_metadata_authority_fanout_blocking_n1");
        let targets = (2..=6)
            .map(|index| node_id(&format!("node_metadata_authority_fanout_blocking_n{index}")))
            .collect::<Vec<_>>();
        let peer = Arc::new(MetadataFanoutPeerTransport {
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            failed_target: None,
            slow_target: None,
            fast_delay: Duration::from_millis(25),
            slow_delay: Duration::from_secs(2),
        });
        let sync_peer: Arc<dyn DfsPeerTransport> = peer.clone();
        let mut transport = DfsNodeServedMessageTransport::new_with_peer_transport_for_nodes(
            local.clone(),
            targets.clone(),
            sync_peer,
            Duration::from_secs(2),
        );

        let responses = transport
            .send_metadata_journal_batches_direct(&test_metadata_authority_fanout_requests(
                &local, &targets,
            ))
            .expect("blocking metadata authority fan-out")
            .expect("blocking peer retained");

        assert_eq!(peer.max_active.load(Ordering::SeqCst), targets.len());
        assert_eq!(responses.len(), targets.len());
    }

    #[test]
    fn blocking_metadata_authority_fanout_honors_operation_deadline() {
        let local = node_id("node_metadata_authority_deadline_blocking_n1");
        let targets = vec![node_id("node_metadata_authority_deadline_blocking_n2")];
        let peer = Arc::new(MetadataFanoutPeerTransport {
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            failed_target: None,
            slow_target: None,
            fast_delay: Duration::from_millis(400),
            slow_delay: Duration::from_millis(400),
        });
        let sync_peer: Arc<dyn DfsPeerTransport> = peer;
        let mut transport = DfsNodeServedMessageTransport::new_with_peer_transport_for_nodes(
            local.clone(),
            targets.clone(),
            sync_peer,
            Duration::from_secs(2),
        );
        transport.set_operation_deadline(Some(Instant::now() + Duration::from_millis(50)));

        let started = Instant::now();
        let responses = transport
            .send_metadata_journal_batches_direct(&test_metadata_authority_fanout_requests(
                &local, &targets,
            ))
            .expect("deadline-bounded metadata authority fan-out")
            .expect("blocking peer retained");

        assert!(responses.is_empty());
        assert!(
            started.elapsed() < Duration::from_millis(250),
            "ordinary authority fan-out must retain the caller's shorter deadline"
        );
    }

    #[test]
    fn metadata_authority_peer_budget_starts_after_replayable_encoding() {
        let local = node_id("node_metadata_authority_encode_budget_n1");
        let targets = vec![node_id("node_metadata_authority_encode_budget_n2")];
        let peer = Arc::new(MetadataFanoutPeerTransport {
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            failed_target: None,
            slow_target: None,
            fast_delay: Duration::from_millis(5),
            slow_delay: Duration::from_millis(5),
        });
        let sync_peer: Arc<dyn DfsPeerTransport> = peer;
        let mut transport = DfsNodeServedMessageTransport::new_with_peer_transport_for_nodes(
            local.clone(),
            targets.clone(),
            sync_peer,
            Duration::from_secs(2),
        );
        let operation_deadline = Instant::now() + Duration::from_millis(500);
        transport.set_operation_deadline(Some(operation_deadline));
        DFS_METADATA_DIRECT_ENCODING_TEST_DELAY.with(|delay| delay.set(Duration::from_millis(75)));

        let started = Instant::now();
        let responses = transport
            .send_metadata_journal_batches_direct_with_response_timeout(
                &test_metadata_authority_fanout_requests(&local, &targets),
                Duration::from_millis(40),
            )
            .expect("deadline-bounded metadata authority fan-out")
            .expect("blocking peer retained");

        assert_eq!(responses.len(), 1);
        assert!(
            started.elapsed() >= Duration::from_millis(75),
            "the test must exercise local work longer than the peer budget"
        );
        assert_eq!(transport.operation_deadline(), Some(operation_deadline));
    }

    #[test]
    fn blocking_metadata_journal_fanout_does_not_accumulate_stalled_threads() {
        let local = node_id("node_metadata_fanout_stalled_n1");
        let targets = (2..=3)
            .map(|index| node_id(&format!("node_metadata_fanout_stalled_n{index}")))
            .collect::<Vec<_>>();
        let peer = Arc::new(MetadataFanoutPeerTransport {
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            failed_target: None,
            slow_target: None,
            fast_delay: Duration::from_secs(2),
            slow_delay: Duration::from_secs(2),
        });
        let sync_peer: Arc<dyn DfsPeerTransport> = peer.clone();
        let mut transport = DfsNodeServedMessageTransport::new_with_peer_transport_for_nodes(
            local.clone(),
            targets.clone(),
            sync_peer,
            Duration::from_secs(2),
        );
        let requests = test_metadata_fanout_requests(&local, &targets);

        let first_started = Instant::now();
        let first = transport
            .send_metadata_journal_batches_direct(&requests)
            .expect("first stalled blocking metadata fan-out")
            .expect("blocking peer retained");
        assert!(first.is_empty());
        assert!(first_started.elapsed() < Duration::from_millis(600));
        assert_eq!(
            transport
                .node_transport
                .blocking_metadata_fanout_workers
                .load(AtomicOrdering::Acquire),
            DFS_METADATA_CHECKPOINT_DIRECT_MAX_INFLIGHT
        );

        let second_started = Instant::now();
        let second = transport
            .send_metadata_journal_batches_direct(&requests)
            .expect("second stalled blocking metadata fan-out")
            .expect("blocking peer retained");
        assert!(second.is_empty());
        assert!(
            second_started.elapsed() < Duration::from_millis(200),
            "a saturated blocking fan-out must not spawn more detached threads"
        );
        assert_eq!(peer.max_active.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn async_metadata_journal_fanout_caps_overlap_and_drops_slow_sibling() {
        let local = node_id("node_metadata_fanout_async_n1");
        let targets = (2..=5)
            .map(|index| node_id(&format!("node_metadata_fanout_async_n{index}")))
            .collect::<Vec<_>>();
        let peer = Arc::new(MetadataFanoutPeerTransport {
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            failed_target: Some(targets[1].clone()),
            slow_target: Some(targets[2].clone()),
            fast_delay: Duration::from_millis(20),
            slow_delay: Duration::from_secs(2),
        });
        let sync_peer: Arc<dyn DfsPeerTransport> = peer.clone();
        let async_peer: Arc<dyn AsyncDfsPeerTransport> = peer.clone();
        let mut transport = DfsNodeServedMessageTransport::new_with_peer_transports_for_nodes(
            local.clone(),
            targets.clone(),
            sync_peer,
            async_peer,
            Duration::from_secs(2),
        );
        let started = Instant::now();

        let mut response_targets = transport
            .send_metadata_journal_batches_direct(&test_metadata_fanout_requests(&local, &targets))
            .expect("async metadata fan-out")
            .expect("async peer retained")
            .into_iter()
            .map(|(target_node_id, _)| target_node_id)
            .collect::<Vec<_>>();
        response_targets.sort();
        let mut expected_targets = vec![targets[0].clone(), targets[3].clone()];
        expected_targets.sort();

        assert_eq!(peer.max_active.load(Ordering::SeqCst), 2);
        assert_eq!(response_targets, expected_targets);
        assert!(
            started.elapsed() < Duration::from_millis(600),
            "slow sibling must be canceled at the total fan-out budget"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn async_metadata_authority_fanout_starts_the_full_shard_together() {
        let local = node_id("node_metadata_authority_fanout_async_n1");
        let targets = (2..=6)
            .map(|index| node_id(&format!("node_metadata_authority_fanout_async_n{index}")))
            .collect::<Vec<_>>();
        let peer = Arc::new(MetadataFanoutPeerTransport {
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            failed_target: None,
            slow_target: None,
            fast_delay: Duration::from_millis(25),
            slow_delay: Duration::from_secs(2),
        });
        let sync_peer: Arc<dyn DfsPeerTransport> = peer.clone();
        let async_peer: Arc<dyn AsyncDfsPeerTransport> = peer.clone();
        let mut transport = DfsNodeServedMessageTransport::new_with_peer_transports_for_nodes(
            local.clone(),
            targets.clone(),
            sync_peer,
            async_peer,
            Duration::from_secs(2),
        );

        let responses = transport
            .send_metadata_journal_batches_direct(&test_metadata_authority_fanout_requests(
                &local, &targets,
            ))
            .expect("async metadata authority fan-out")
            .expect("async peer retained");

        assert_eq!(peer.max_active.load(Ordering::SeqCst), targets.len());
        assert_eq!(responses.len(), targets.len());
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
            .send_routed_call(
                &remote,
                DfsRoutedCall::value(test_message(&local, &remote, 1)),
            )
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
    fn direct_content_root_catalog_request_validates_authenticated_response_route() {
        let local = node_id("node_content_root_local");
        let primary = node_id("node_content_root_primary");
        let rogue = node_id("node_content_root_rogue");
        let shard = DfsMetadataShardConfig {
            file_system_id: sifs_file_system_id(),
            shard_id: "root".to_string(),
            primary_node_id: primary.clone(),
            replica_node_ids: Vec::new(),
            epoch: 7,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        };
        let request = DfsContentRootCatalogPageRequest {
            identity: DfsContentRootCatalogShardIdentity::from_config(&shard),
            expected_topology_digest: dfs_content_root_catalog_topology_digest(&[shard])
                .expect("topology digest"),
            minimum_journal_sequence: 3,
            minimum_state_fingerprint: None,
            after: None,
            max_entries: 16,
        };

        let response_source = Arc::new(Mutex::new(primary.clone()));
        let sender_response_source = Arc::clone(&response_source);
        let sender: DfsMessageSender = Arc::new(move |_target, command, _options| {
            let DfsNetworkMessage::ContentRootCatalogPageRequest(request) =
                &command.routed_message.message
            else {
                panic!("expected content-root catalog request")
            };
            let page = DfsContentRootCatalogPage {
                identity: request.identity.clone(),
                topology_digest: request.expected_topology_digest.clone(),
                generated_at_unix_nanos: 10,
                coverage: DfsContentRootCatalogCoverage::Complete {
                    prefix_proof_version: DFS_CONTENT_ROOT_CATALOG_PREFIX_PROOF_VERSION,
                    journal_sequence: 3,
                    state_fingerprint: format!("dfs-metadata-prefix-v2:blake3:{}", "a".repeat(64)),
                },
                entries: Vec::new(),
                next_cursor: None,
            };
            Ok(DfsMessageResponse {
                request_id: command.request_id.clone(),
                outbound_messages: vec![DfsRoutedMessage {
                    source_node_id: sender_response_source
                        .lock()
                        .expect("response source")
                        .clone(),
                    target_node_id: command.routed_message.source_node_id.clone(),
                    frame_id: command.routed_message.frame_id,
                    message_id: command.routed_message.message_id,
                    message: DfsNetworkMessage::ContentRootCatalogPageResponse(page),
                }],
            })
        });
        let transport = DfsNodeTransport::new(local, vec![primary.clone(), rogue.clone()], sender);

        let page = transport
            .request_content_root_catalog_page(&primary, &request)
            .expect("valid primary page");
        assert_eq!(page.identity.primary_node_id, primary);

        *response_source.lock().expect("response source") = rogue;
        let error = transport
            .request_content_root_catalog_page(&request.identity.primary_node_id, &request)
            .expect_err("rogue response source must be rejected");
        assert!(
            error.to_string().contains("response route mismatch"),
            "unexpected rejection: {error}"
        );
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
            .send_routed_call(
                &remote,
                DfsRoutedCall::value(test_namespace_message(&local, &remote, 1)),
            )
            .expect("namespace send");
        transport
            .send_routed_call(
                &remote,
                DfsRoutedCall::value(test_distributed_write_message(&local, &remote, 2)),
            )
            .expect("distributed write send");
        transport
            .send_routed_call(
                &remote,
                DfsRoutedCall::value(test_message(&local, &remote, 3)),
            )
            .expect("ordinary send");

        let observed = observed_options.lock().expect("options");
        assert_eq!(
            observed.as_slice(),
            &[DfsObjectRpcSendOptions::default(); 3],
            "stream-native calls keep transport health independently of application silence"
        );
    }

    #[test]
    fn deadline_bounded_namespace_send_replays_exact_request_after_ambiguous_response() {
        let local = node_id("node_namespace_reconcile_local");
        let remote = node_id("node_namespace_reconcile_remote");
        let observed = Arc::new(Mutex::new(Vec::new()));
        let sender_observed = Arc::clone(&observed);
        let calls = Arc::new(AtomicUsize::new(0));
        let sender_calls = Arc::clone(&calls);
        let sender: DfsMessageSender = Arc::new(move |_target, command, options| {
            sender_observed
                .lock()
                .expect("observed sends")
                .push((command.clone(), options));
            if sender_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(DfsPeerTransportError::with_delivery_state(
                    "response was lost after the authority may have committed",
                    Some(sinomo_core::network::NetworkDeliveryState::PossiblyApplied),
                ));
            }
            Ok(DfsMessageResponse {
                request_id: command.request_id.clone(),
                outbound_messages: Vec::new(),
            })
        });
        let mut transport = DfsNodeTransport::with_connect_timeout(
            local.clone(),
            vec![remote.clone()],
            Duration::from_secs(5),
            sender,
        );
        transport.set_operation_deadline(Some(Instant::now() + Duration::from_secs(2)));

        transport
            .send_routed_call(
                &remote,
                DfsRoutedCall::value(test_namespace_message(&local, &remote, 41)),
            )
            .expect("exact namespace replay should reconcile the response");

        let observed = observed.lock().expect("observed sends");
        assert_eq!(observed.len(), 2);
        assert_eq!(
            observed[0].0, observed[1].0,
            "reconciliation must reuse the exact DFS and namespace request identities"
        );
        let first_budget = observed[0].1.overall_timeout.expect("first budget");
        let replay_budget = observed[1].1.overall_timeout.expect("replay budget");
        assert!(
            first_budget < replay_budget && replay_budget <= Duration::from_secs(2),
            "the first attempt must leave a bounded tail for exact replay: first={first_budget:?}, replay={replay_budget:?}"
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
            .send_routed_call(
                &remote,
                DfsRoutedCall::value(test_message(&local, &remote, 1)),
            )
            .expect("first send");
        transport
            .send_routed_call(
                &remote,
                DfsRoutedCall::value(test_message(&local, &remote, 2)),
            )
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
    fn served_message_transport_propagates_operation_deadline_to_peer_sends() {
        let local = node_id("node_served_deadline_local");
        let remote = node_id("node_served_deadline_remote");
        let observed_options = Arc::new(Mutex::new(Vec::new()));
        let sender_options = Arc::clone(&observed_options);
        let sender: DfsMessageSender = Arc::new(move |_target, command, options| {
            sender_options.lock().expect("options").push(options);
            Ok(DfsMessageResponse {
                request_id: command.request_id.clone(),
                outbound_messages: Vec::new(),
            })
        });
        let mut transport = DfsNodeServedMessageTransport::new_with_message_sender_for_nodes(
            local.clone(),
            vec![remote.clone()],
            sender,
            None,
            Duration::from_secs(60),
        );
        let deadline = Instant::now() + Duration::from_millis(100);

        assert_eq!(transport.set_operation_deadline(Some(deadline)), None);
        assert_eq!(transport.operation_deadline(), Some(deadline));
        transport
            .send_routed_call(
                &remote,
                DfsRoutedCall::value(test_message(&local, &remote, 1)),
            )
            .expect("served peer send");

        let observed = observed_options.lock().expect("options");
        let options = observed.first().expect("served peer send options");
        assert_eq!(options.connect_timeout, Duration::from_secs(60));
        assert!(
            options.overall_timeout.is_some_and(|timeout| {
                !timeout.is_zero() && timeout <= Duration::from_millis(100)
            }),
            "served peer send must inherit the DFS operation deadline: {options:?}"
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
            transport.detached_send_count(),
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
        assert_eq!(transport.detached_send_count(), 0);
    }

    #[test]
    fn detached_completed_write_response_retries_ambiguous_failure_with_fresh_budget() {
        let local = node_id("node_detached_write_response_local");
        let first_requester = node_id("node_detached_write_response_first_requester");
        let second_requester = node_id("node_detached_write_response_second_requester");
        let calls = Arc::new(AtomicUsize::new(0));
        let sender_calls = Arc::clone(&calls);
        let sender: DfsMessageSender = Arc::new(move |_target, command, _options| {
            let call = sender_calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                return Err(DfsPeerTransportError::new(
                    "ambiguous completed write response failure",
                ));
            }
            Ok(DfsMessageResponse {
                request_id: command.request_id.clone(),
                outbound_messages: Vec::new(),
            })
        });
        let mut transport = DfsNodeTransport::with_connect_timeout(
            local.clone(),
            Vec::<NodeId>::new(),
            Duration::from_secs(1),
            sender,
        );
        transport.set_operation_deadline(Some(Instant::now() + Duration::from_millis(20)));
        transport
            .send_detached_routed_message(
                &first_requester,
                test_distributed_write_response_message(&local, &first_requester, 1),
            )
            .expect("queue completed write response");
        transport
            .send_detached_routed_message(
                &second_requester,
                test_distributed_write_response_message(&local, &second_requester, 2),
            )
            .expect("queue second completed write response");
        std::thread::sleep(Duration::from_millis(30));

        let retry_started = Instant::now();
        transport
            .flush_detached_peer_sends_with_now(&|| retry_started)
            .expect_err("first completed response send should fail ambiguously");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            transport.detached_send_frame_ids(),
            vec![1],
            "healthy suffix work must run before the attempted response is retried"
        );

        transport
            .flush_detached_peer_sends_with_now(&|| retry_started)
            .expect("cooling response must not be retried immediately");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            transport
                .detached_send_scheduling_snapshot_at_for_test(retry_started)
                .1,
            Some(retry_started + DFS_DETACHED_SEND_RETRY_DELAY)
        );

        transport
            .flush_detached_peer_sends_with_now(&|| retry_started + DFS_DETACHED_SEND_RETRY_DELAY)
            .expect("completed response replay should succeed");
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(transport.detached_send_count(), 0);
    }

    #[test]
    fn detached_poison_completed_response_does_not_starve_healthy_response_or_promotion() {
        let local = node_id("node_detached_poison_response_local");
        let poison_requester = node_id("node_detached_poison_response_requester");
        let healthy_requester = node_id("node_detached_healthy_response_requester");
        let replica = node_id("node_detached_healthy_promotion_replica");
        let calls = Arc::new(Mutex::new(Vec::new()));
        let sender_calls = Arc::clone(&calls);
        let sender_poison_requester = poison_requester.clone();
        let sender: DfsMessageSender = Arc::new(move |target, command, _options| {
            sender_calls
                .lock()
                .expect("detached send calls")
                .push(target.clone());
            if target == &sender_poison_requester {
                return Err(DfsPeerTransportError::new(
                    "permanently offline response requester",
                ));
            }
            if matches!(
                &command.routed_message.message,
                DfsNetworkMessage::CommitVersion(_)
            ) {
                Ok(test_committed_promotion_rpc_response(command))
            } else {
                Ok(DfsMessageResponse {
                    request_id: command.request_id.clone(),
                    outbound_messages: Vec::new(),
                })
            }
        });
        let mut transport = DfsNodeTransport::with_connect_timeout(
            local.clone(),
            Vec::<NodeId>::new(),
            Duration::from_secs(1),
            sender,
        );
        transport
            .send_detached_routed_message(
                &poison_requester,
                test_distributed_write_response_message(&local, &poison_requester, 1),
            )
            .expect("queue poison response");
        transport
            .send_detached_routed_message(
                &healthy_requester,
                test_distributed_write_response_message(&local, &healthy_requester, 2),
            )
            .expect("queue healthy response");
        transport
            .send_detached_routed_message(
                &replica,
                test_commit_version_message(&local, &replica, 3),
            )
            .expect("queue healthy promotion");

        transport
            .flush_detached_sends()
            .expect_err("poison response must remain retryable");

        assert_eq!(
            calls.lock().expect("detached send calls").as_slice(),
            &[poison_requester.clone(), healthy_requester, replica,],
            "poison response stopped ready detached work"
        );
        assert_eq!(transport.detached_send_targets(), vec![poison_requester]);
        assert_eq!(transport.pending_commit_promotion_count(), 0);
        assert_eq!(transport.settled_commit_promotion_count(), 1);
    }

    #[test]
    fn detached_cooling_retries_report_earliest_deadline_without_spinning() {
        let local = node_id("node_detached_retry_schedule_local");
        let first = node_id("node_detached_retry_schedule_first");
        let second = node_id("node_detached_retry_schedule_second");
        let calls = Arc::new(AtomicUsize::new(0));
        let sender_calls = Arc::clone(&calls);
        let sender: DfsMessageSender = Arc::new(move |_target, _command, _options| {
            sender_calls.fetch_add(1, Ordering::SeqCst);
            Err(DfsPeerTransportError::new(
                "cooling response target offline",
            ))
        });
        let mut transport = DfsNodeTransport::with_connect_timeout(
            local.clone(),
            Vec::<NodeId>::new(),
            Duration::from_secs(1),
            sender,
        );
        let started = Instant::now();
        transport
            .send_detached_routed_message(
                &first,
                test_distributed_write_response_message(&local, &first, 1),
            )
            .expect("queue first response");
        transport
            .flush_detached_peer_sends_with_now(&|| started)
            .expect_err("first response should enter cooling");

        let second_started = started + Duration::from_secs(2);
        transport
            .send_detached_routed_message(
                &second,
                test_distributed_write_response_message(&local, &second, 2),
            )
            .expect("queue second response");
        transport
            .flush_detached_peer_sends_with_now(&|| second_started)
            .expect_err("second response should enter cooling");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            transport
                .detached_send_scheduling_snapshot_at_for_test(second_started)
                .1,
            Some(started + DFS_DETACHED_SEND_RETRY_DELAY),
            "the earliest per-send eligibility must drive the wakeup"
        );

        for _ in 0..4 {
            transport
                .flush_detached_peer_sends_with_now(&|| second_started)
                .expect("cooling-only drain is idle, not an error");
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "cooling-only drain attempts must not spin on the sender"
        );

        let first_due = started + DFS_DETACHED_SEND_RETRY_DELAY;
        transport
            .flush_detached_peer_sends_with_now(&|| first_due)
            .expect_err("first retry remains offline");
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(
            transport
                .detached_send_scheduling_snapshot_at_for_test(first_due)
                .1,
            Some(second_started + DFS_DETACHED_SEND_RETRY_DELAY),
            "the next independent deadline must become the earliest wakeup"
        );
    }

    #[test]
    fn detached_replay_safe_retry_delay_grows_and_caps_without_settling() {
        let local = node_id("node_detached_retry_backoff_local");
        let remote = node_id("node_detached_retry_backoff_remote");
        let calls = Arc::new(AtomicUsize::new(0));
        let sender_calls = Arc::clone(&calls);
        let sender: DfsMessageSender = Arc::new(move |_target, _command, _options| {
            sender_calls.fetch_add(1, Ordering::SeqCst);
            Err(DfsPeerTransportError::new(
                "detached retry backoff target offline",
            ))
        });
        let mut transport = DfsNodeTransport::with_connect_timeout(
            local.clone(),
            [remote.clone()],
            Duration::from_secs(1),
            sender,
        );
        transport
            .send_detached_routed_message(&remote, test_commit_version_message(&local, &remote, 1))
            .expect("queue promotion");

        let mut attempted_at = Instant::now();
        for expected_delay_seconds in [5, 10, 20, 40, 80, 160, 320, 320] {
            transport
                .flush_detached_peer_sends_with_now(&|| attempted_at)
                .expect_err("offline promotion must remain retryable");
            let retry_not_before = attempted_at + Duration::from_secs(expected_delay_seconds);
            assert_eq!(
                transport
                    .detached_send_scheduling_snapshot_at_for_test(attempted_at)
                    .1,
                Some(retry_not_before),
                "retry delay should grow exponentially and cap at 320 seconds"
            );
            assert_eq!(transport.detached_send_count(), 1);
            assert_eq!(transport.pending_commit_promotion_count(), 1);
            assert_eq!(transport.settled_commit_promotion_count(), 0);
            attempted_at = retry_not_before;
        }
        assert_eq!(calls.load(Ordering::SeqCst), 8);
    }

    #[test]
    fn detached_worker_defers_target_without_blocking_healthy_work_or_spinning() {
        let local = node_id("node_detached_deferred_local");
        let deferred = node_id("node_detached_deferred_offline");
        let healthy = node_id("node_detached_deferred_healthy");
        let calls = Arc::new(Mutex::new(Vec::new()));
        let sender_calls = Arc::clone(&calls);
        let sender: DfsMessageSender = Arc::new(move |target, command, _options| {
            sender_calls
                .lock()
                .expect("detached deferred calls")
                .push(target.clone());
            Ok(test_committed_promotion_rpc_response(command))
        });
        let mut transport = DfsNodeTransport::with_connect_timeout(
            local.clone(),
            [deferred.clone(), healthy.clone()],
            Duration::from_secs(1),
            sender,
        );
        transport
            .send_detached_routed_message(
                &deferred,
                test_commit_version_message(&local, &deferred, 1),
            )
            .expect("queue deferred promotion");
        transport
            .send_detached_routed_message(
                &healthy,
                test_commit_version_message(&local, &healthy, 2),
            )
            .expect("queue healthy promotion");

        let started = Instant::now();
        let mut worker = transport.fork_detached_send_worker();
        worker.set_deferred_target_node_ids(BTreeSet::from([deferred.clone()]));
        worker
            .drain_at(started)
            .expect("healthy promotion bypasses deferred target");

        assert_eq!(
            calls.lock().expect("detached deferred calls").as_slice(),
            std::slice::from_ref(&healthy)
        );
        assert_eq!(transport.detached_send_count(), 1);
        assert_eq!(transport.pending_commit_promotion_count(), 1);
        let (ready, retry_not_before, _) = worker
            .scheduling_snapshot_at(started)
            .expect("deferred-only schedule");
        assert!(!ready);
        assert_eq!(retry_not_before, None);

        for _ in 0..4 {
            worker
                .drain_at(started)
                .expect("deferred-only drain stays idle");
        }
        assert_eq!(
            calls.lock().expect("detached deferred calls").as_slice(),
            std::slice::from_ref(&healthy),
            "a deferred ready send must not make the worker spin"
        );

        worker.set_deferred_target_node_ids(BTreeSet::new());
        assert!(
            worker
                .scheduling_snapshot_at(started)
                .expect("resumed schedule")
                .0
        );
        worker
            .drain_at(started)
            .expect("recovered target resumes exact promotion");
        assert_eq!(
            calls.lock().expect("detached deferred calls").as_slice(),
            &[healthy, deferred]
        );
        assert_eq!(transport.detached_send_count(), 0);
        assert_eq!(transport.pending_commit_promotion_count(), 0);
    }

    #[test]
    fn detached_worker_ignores_deferred_cooling_deadline_until_target_recovers() {
        let local = node_id("node_detached_deferred_cooling_local");
        let deferred = node_id("node_detached_deferred_cooling_remote");
        let calls = Arc::new(AtomicUsize::new(0));
        let sender_calls = Arc::clone(&calls);
        let sender: DfsMessageSender = Arc::new(move |_target, command, _options| {
            if sender_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(DfsPeerTransportError::new(
                    "deferred cooling target offline",
                ))
            } else {
                Ok(test_committed_promotion_rpc_response(command))
            }
        });
        let mut transport = DfsNodeTransport::with_connect_timeout(
            local.clone(),
            [deferred.clone()],
            Duration::from_secs(1),
            sender,
        );
        transport
            .send_detached_routed_message(
                &deferred,
                test_commit_version_message(&local, &deferred, 1),
            )
            .expect("queue cooling promotion");
        let started = Instant::now();
        transport
            .flush_detached_peer_sends_with_now(&|| started)
            .expect_err("first attempt enters cooling");
        let retry_at = started + DFS_DETACHED_SEND_RETRY_DELAY;

        let mut worker = transport.fork_detached_send_worker();
        worker.set_deferred_target_node_ids(BTreeSet::from([deferred]));
        let (ready, retry_not_before, _) = worker
            .scheduling_snapshot_at(retry_at)
            .expect("deferred cooling schedule");
        assert!(!ready);
        assert_eq!(retry_not_before, None);
        worker
            .drain_at(retry_at)
            .expect("due deferred retry remains parked");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(transport.detached_send_count(), 1);

        worker.set_deferred_target_node_ids(BTreeSet::new());
        worker
            .drain_at(retry_at)
            .expect("recovered cooling target resumes");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(transport.detached_send_count(), 0);
        assert_eq!(transport.pending_commit_promotion_count(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn detached_worker_policy_change_wakes_parked_scheduler() {
        let local = node_id("node_detached_policy_wake_local");
        let deferred = node_id("node_detached_policy_wake_remote");
        let calls = Arc::new(AtomicUsize::new(0));
        let sender_calls = Arc::clone(&calls);
        let sender: DfsMessageSender = Arc::new(move |_target, command, _options| {
            sender_calls.fetch_add(1, Ordering::SeqCst);
            Ok(test_committed_promotion_rpc_response(command))
        });
        let mut transport = DfsNodeTransport::with_connect_timeout(
            local.clone(),
            [deferred.clone()],
            Duration::from_secs(1),
            sender,
        );
        transport
            .send_detached_routed_message(
                &deferred,
                test_commit_version_message(&local, &deferred, 1),
            )
            .expect("queue policy-wake promotion");

        let mut parked_worker = transport.fork_detached_send_worker();
        parked_worker.set_deferred_target_node_ids(BTreeSet::from([deferred]));
        let (ready, retry_not_before, generation) = parked_worker
            .scheduling_snapshot()
            .expect("parked worker schedule");
        assert!(!ready);
        assert_eq!(retry_not_before, None);
        let waiter = tokio::spawn(async move { parked_worker.wait_for_change(generation).await });
        tokio::task::yield_now().await;

        let mut recovered_worker = transport.fork_detached_send_worker();
        recovered_worker.notify_scheduler_policy_change();
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("policy change must wake parked scheduler")
            .expect("parked scheduler task")
            .expect("parked scheduler wait");
        recovered_worker
            .drain_at(Instant::now())
            .expect("refreshed worker resumes retained promotion");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(transport.detached_send_count(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn detached_worker_new_send_wakes_without_policy_refresh() {
        let local = node_id("node_detached_enqueue_wake_local");
        let remote = node_id("node_detached_enqueue_wake_remote");
        let calls = Arc::new(AtomicUsize::new(0));
        let sender_calls = Arc::clone(&calls);
        let sender: DfsMessageSender = Arc::new(move |_target, command, _options| {
            sender_calls.fetch_add(1, Ordering::SeqCst);
            Ok(test_committed_promotion_rpc_response(command))
        });
        let mut transport = DfsNodeTransport::with_connect_timeout(
            local.clone(),
            [remote.clone()],
            Duration::from_secs(1),
            sender,
        );
        let waiting_worker = transport.fork_detached_send_worker();
        let mut drain_worker = waiting_worker.clone();
        let (_, _, generation) = waiting_worker
            .scheduling_snapshot()
            .expect("empty detached schedule");
        let waiter = tokio::spawn(async move { waiting_worker.wait_for_change(generation).await });
        tokio::task::yield_now().await;

        transport
            .send_detached_routed_message(&remote, test_commit_version_message(&local, &remote, 1))
            .expect("queue promotion without policy refresh");
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("new outbox work must wake parked scheduler")
            .expect("parked scheduler task")
            .expect("parked scheduler wait");
        drain_worker
            .drain_at(Instant::now())
            .expect("newly queued promotion drains");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(transport.detached_send_count(), 0);
    }

    #[test]
    fn detached_exponential_cooling_preserves_healthy_target_bypass() {
        let local = node_id("node_detached_retry_bypass_local");
        let poison = node_id("node_detached_retry_bypass_poison");
        let healthy = node_id("node_detached_retry_bypass_healthy");
        let calls = Arc::new(Mutex::new(Vec::new()));
        let sender_calls = Arc::clone(&calls);
        let sender_poison = poison.clone();
        let sender: DfsMessageSender = Arc::new(move |target, command, _options| {
            sender_calls
                .lock()
                .expect("detached retry calls")
                .push(target.clone());
            if target == &sender_poison {
                return Err(DfsPeerTransportError::new(
                    "detached retry poison target offline",
                ));
            }
            Ok(test_committed_promotion_rpc_response(command))
        });
        let mut transport = DfsNodeTransport::with_connect_timeout(
            local.clone(),
            [poison.clone(), healthy.clone()],
            Duration::from_secs(1),
            sender,
        );
        transport
            .send_detached_routed_message(&poison, test_commit_version_message(&local, &poison, 1))
            .expect("queue poison promotion");

        let started = Instant::now();
        transport
            .flush_detached_peer_sends_with_now(&|| started)
            .expect_err("poison promotion enters first cooling tier");
        let first_retry_at = started + Duration::from_secs(5);
        transport
            .flush_detached_peer_sends_with_now(&|| first_retry_at)
            .expect_err("poison promotion enters second cooling tier");
        assert_eq!(
            transport
                .detached_send_scheduling_snapshot_at_for_test(first_retry_at)
                .1,
            Some(first_retry_at + Duration::from_secs(10))
        );

        transport
            .send_detached_routed_message(
                &healthy,
                test_commit_version_message(&local, &healthy, 2),
            )
            .expect("queue healthy promotion");
        transport
            .flush_detached_peer_sends_with_now(&|| first_retry_at)
            .expect("healthy target should bypass poison cooling");

        assert_eq!(
            calls.lock().expect("detached retry calls").as_slice(),
            &[poison.clone(), poison.clone(), healthy]
        );
        assert_eq!(transport.detached_send_targets(), vec![poison]);
        assert_eq!(transport.pending_commit_promotion_count(), 1);
        assert_eq!(transport.settled_commit_promotion_count(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn detached_ready_enqueue_wakes_cooling_worker_without_retrying_poison() {
        let local = node_id("node_detached_retry_wake_local");
        let poison = node_id("node_detached_retry_wake_poison");
        let healthy = node_id("node_detached_retry_wake_healthy");
        let calls = Arc::new(Mutex::new(Vec::new()));
        let sender_calls = Arc::clone(&calls);
        let sender_poison = poison.clone();
        let sender: DfsMessageSender = Arc::new(move |target, command, _options| {
            sender_calls
                .lock()
                .expect("send calls")
                .push(target.clone());
            if target == &sender_poison {
                return Err(DfsPeerTransportError::new("poison promotion target"));
            }
            Ok(DfsMessageResponse {
                request_id: command.request_id.clone(),
                outbound_messages: Vec::new(),
            })
        });
        let mut transport = DfsNodeTransport::with_connect_timeout(
            local.clone(),
            Vec::<NodeId>::new(),
            Duration::from_secs(1),
            sender,
        );
        let started = Instant::now() + Duration::from_secs(60);
        transport
            .send_detached_routed_message(&poison, test_commit_version_message(&local, &poison, 1))
            .expect("queue poison promotion");
        transport
            .flush_detached_peer_sends_with_now(&|| started)
            .expect_err("poison promotion enters cooling");

        let worker = transport.fork_detached_send_worker();
        let (ready, retry_not_before, generation) =
            worker.scheduling_snapshot().expect("cooling schedule");
        assert!(!ready);
        let retry_not_before = retry_not_before.expect("cooling deadline");
        let waiter = tokio::spawn(async move {
            worker
                .wait_for_ready_or_retry(generation, retry_not_before)
                .await
        });
        tokio::task::yield_now().await;

        transport
            .send_detached_routed_message(
                &healthy,
                test_distributed_write_response_message(&local, &healthy, 2),
            )
            .expect("queue healthy response");
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("ready enqueue must wake waiter")
            .expect("waiter task")
            .expect("wait schedule");
        transport
            .flush_detached_peer_sends_with_now(&|| started)
            .expect("healthy response should bypass cooling promotion");
        assert_eq!(
            calls.lock().expect("send calls").as_slice(),
            &[poison.clone(), healthy],
            "wakeup must not make the poison retry immediately eligible"
        );
        assert_eq!(transport.detached_send_targets(), vec![poison]);
    }

    #[test]
    fn detached_commit_promotions_overlap_with_cap_two() {
        let local = node_id("node_commit_promotion_cap_local");
        let n2 = node_id("node_commit_promotion_cap_n2");
        let n3 = node_id("node_commit_promotion_cap_n3");
        let n4 = node_id("node_commit_promotion_cap_n4");
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new((Mutex::new(0usize), Condvar::new()));
        let calls = Arc::new(Mutex::new(Vec::new()));
        let sender_active = Arc::clone(&active);
        let sender_max_active = Arc::clone(&max_active);
        let sender_entered = Arc::clone(&entered);
        let sender_calls = Arc::clone(&calls);
        let paired_n2 = n2.clone();
        let paired_n3 = n3.clone();
        let sender: DfsMessageSender = Arc::new(move |target, command, _options| {
            let active = sender_active.fetch_add(1, Ordering::SeqCst) + 1;
            sender_max_active.fetch_max(active, Ordering::SeqCst);
            sender_calls
                .lock()
                .expect("promotion calls")
                .push(target.clone());
            if target == &paired_n2 || target == &paired_n3 {
                let (entered, both_entered) = sender_entered.as_ref();
                let mut entered = entered.lock().expect("promotion entries");
                *entered += 1;
                both_entered.notify_all();
                if *entered < 2 {
                    let (guard, _) = both_entered
                        .wait_timeout_while(entered, Duration::from_secs(1), |entered| *entered < 2)
                        .expect("promotion overlap wait");
                    entered = guard;
                }
                drop(entered);
            }
            sender_active.fetch_sub(1, Ordering::SeqCst);
            Ok(test_committed_promotion_rpc_response(command))
        });
        let mut transport = DfsNodeTransport::with_connect_timeout(
            local.clone(),
            vec![n2.clone(), n3.clone(), n4.clone()],
            Duration::from_secs(2),
            sender,
        );
        for (frame_id, target) in [(1, &n2), (2, &n3), (3, &n4)] {
            transport
                .send_detached_routed_message(
                    target,
                    test_commit_version_message(&local, target, frame_id),
                )
                .expect("queue detached commit promotion");
        }

        transport
            .flush_detached_sends()
            .expect("flush detached commit promotions");

        assert_eq!(max_active.load(Ordering::SeqCst), 2);
        let mut calls = calls.lock().expect("promotion calls").clone();
        calls.sort();
        let mut expected = vec![n2, n3, n4];
        expected.sort();
        assert_eq!(calls, expected);
    }

    #[test]
    fn detached_due_commit_promotion_retries_preserve_cap_two() {
        let local = node_id("node_due_commit_promotion_cap_local");
        let remotes = [
            node_id("node_due_commit_promotion_cap_one"),
            node_id("node_due_commit_promotion_cap_two"),
            node_id("node_due_commit_promotion_cap_three"),
        ];
        let retry_phase = Arc::new(AtomicBool::new(false));
        let calls = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new((Mutex::new(0usize), Condvar::new()));
        let sender_retry_phase = Arc::clone(&retry_phase);
        let sender_calls = Arc::clone(&calls);
        let sender_active = Arc::clone(&active);
        let sender_max_active = Arc::clone(&max_active);
        let sender_entered = Arc::clone(&entered);
        let sender: DfsMessageSender = Arc::new(move |_target, command, _options| {
            sender_calls.fetch_add(1, Ordering::SeqCst);
            if !sender_retry_phase.load(Ordering::SeqCst) {
                return Err(DfsPeerTransportError::new("initial promotion failure"));
            }
            let active = sender_active.fetch_add(1, Ordering::SeqCst) + 1;
            sender_max_active.fetch_max(active, Ordering::SeqCst);
            let (entered, changed) = sender_entered.as_ref();
            let mut entered = entered.lock().expect("due retry entries");
            *entered += 1;
            changed.notify_all();
            if *entered < DFS_COMMIT_PROMOTION_MAX_INFLIGHT {
                let (guard, timeout) = changed
                    .wait_timeout_while(entered, Duration::from_secs(1), |entered| {
                        *entered < DFS_COMMIT_PROMOTION_MAX_INFLIGHT
                    })
                    .expect("due retry overlap wait");
                assert!(!timeout.timed_out(), "due retries did not fill cap two");
                entered = guard;
            }
            drop(entered);
            sender_active.fetch_sub(1, Ordering::SeqCst);
            Ok(test_committed_promotion_rpc_response(command))
        });
        let mut transport = DfsNodeTransport::with_connect_timeout(
            local.clone(),
            remotes.iter().cloned(),
            Duration::from_secs(1),
            sender,
        );
        for (index, remote) in remotes.iter().enumerate() {
            transport
                .send_detached_routed_message(
                    remote,
                    test_commit_version_message(&local, remote, index as u64 + 1),
                )
                .expect("queue promotion");
        }
        let started = Instant::now();
        transport
            .flush_detached_peer_sends_with_now(&|| started)
            .expect_err("initial promotion attempts enter cooling");
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(transport.detached_send_count(), 3);

        retry_phase.store(true, Ordering::SeqCst);
        transport
            .flush_detached_peer_sends_with_now(&|| started + DFS_DETACHED_SEND_RETRY_DELAY)
            .expect("due retries should settle");
        assert_eq!(calls.load(Ordering::SeqCst), 6);
        assert_eq!(max_active.load(Ordering::SeqCst), 2);
        assert_eq!(transport.detached_send_count(), 0);
    }

    #[test]
    fn detached_commit_promotion_failure_retries_failed_target_without_replaying_success() {
        let local = node_id("node_commit_promotion_suffix_local");
        let first_remote = node_id("node_commit_promotion_suffix_first");
        let second_remote = node_id("node_commit_promotion_suffix_second");
        let third_remote = node_id("node_commit_promotion_suffix_third");
        let calls = Arc::new(Mutex::new(Vec::new()));
        let sender_calls = Arc::clone(&calls);
        let failed_target = first_remote.clone();
        let failed_once = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let sender_failed_once = Arc::clone(&failed_once);
        let sender: DfsMessageSender = Arc::new(move |target, command, _options| {
            sender_calls
                .lock()
                .expect("promotion calls")
                .push(target.clone());
            if target == &failed_target && !sender_failed_once.swap(true, Ordering::SeqCst) {
                return Err(DfsPeerTransportError::new(
                    "first detached commit promotion failed",
                ));
            }
            Ok(test_committed_promotion_rpc_response(command))
        });
        let mut transport = DfsNodeTransport::with_connect_timeout(
            local.clone(),
            vec![
                first_remote.clone(),
                second_remote.clone(),
                third_remote.clone(),
            ],
            Duration::from_secs(1),
            sender,
        );
        for (frame_id, target) in [(1, &first_remote), (2, &second_remote), (3, &third_remote)] {
            transport
                .send_detached_routed_message(
                    target,
                    test_commit_version_message(&local, target, frame_id),
                )
                .expect("queue detached commit promotion");
        }

        let retry_started = Instant::now();
        transport
            .flush_detached_peer_sends_with_now(&|| retry_started)
            .expect_err("first commit-promotion wave should report failure");
        assert_eq!(
            transport.detached_send_count(),
            1,
            "only the failed promotion must remain queued"
        );
        let mut first_wave_calls = calls.lock().expect("promotion calls").clone();
        first_wave_calls.sort();
        let mut expected_first_wave = vec![
            first_remote.clone(),
            second_remote.clone(),
            third_remote.clone(),
        ];
        expected_first_wave.sort();
        assert_eq!(first_wave_calls, expected_first_wave);

        transport
            .flush_detached_peer_sends_with_now(&|| retry_started)
            .expect("cooling promotion must not be retried immediately");
        assert_eq!(
            calls
                .lock()
                .expect("promotion calls")
                .iter()
                .filter(|target| *target == &first_remote)
                .count(),
            1
        );

        transport
            .flush_detached_peer_sends_with_now(&|| retry_started + DFS_DETACHED_SEND_RETRY_DELAY)
            .expect("failed promotion should flush on retry");
        let all_calls = calls.lock().expect("promotion calls").clone();
        assert_eq!(
            all_calls
                .iter()
                .filter(|target| *target == &first_remote)
                .count(),
            2,
            "the failed promotion must be replayed exactly once"
        );
        assert_eq!(
            all_calls
                .iter()
                .filter(|target| *target == &second_remote)
                .count(),
            1,
            "the successful sibling must not be replayed"
        );
        assert_eq!(all_calls.len(), 4);
        assert!(all_calls.contains(&third_remote));
        assert_eq!(transport.detached_send_count(), 0);
    }

    #[test]
    fn detached_commit_promotion_poison_targets_do_not_starve_later_waves() {
        let local = node_id("node_commit_promotion_poison_local");
        let poison_one = node_id("node_commit_promotion_poison_one");
        let poison_two = node_id("node_commit_promotion_poison_two");
        let healthy_one = node_id("node_commit_promotion_healthy_one");
        let healthy_two = node_id("node_commit_promotion_healthy_two");
        let calls = Arc::new(Mutex::new(Vec::new()));
        let sender_calls = Arc::clone(&calls);
        let sender_poison_one = poison_one.clone();
        let sender_poison_two = poison_two.clone();
        let sender: DfsMessageSender = Arc::new(move |target, command, _options| {
            sender_calls
                .lock()
                .expect("promotion calls")
                .push(target.clone());
            if target == &sender_poison_one || target == &sender_poison_two {
                return Err(DfsPeerTransportError::new("poison promotion target"));
            }
            Ok(test_committed_promotion_rpc_response(command))
        });
        let targets = vec![
            poison_one.clone(),
            poison_two.clone(),
            healthy_one.clone(),
            healthy_two.clone(),
        ];
        let mut transport = DfsNodeTransport::with_connect_timeout(
            local.clone(),
            targets.clone(),
            Duration::from_secs(1),
            sender,
        );
        for (index, target) in targets.iter().enumerate() {
            transport
                .send_detached_routed_message(
                    target,
                    test_commit_version_message(&local, target, index as u64 + 1),
                )
                .expect("queue commit promotion");
        }

        transport
            .flush_detached_sends()
            .expect_err("poison targets should remain retryable");

        let mut observed = calls.lock().expect("promotion calls").clone();
        observed.sort();
        let mut expected = targets;
        expected.sort();
        assert_eq!(observed, expected, "later promotion wave was starved");
        assert_eq!(
            transport.detached_send_targets(),
            vec![poison_one, poison_two],
            "only failed promotions should remain, at the retry tail"
        );
        assert_eq!(transport.pending_commit_promotion_count(), 2);
        assert_eq!(transport.settled_commit_promotion_count(), 2);
    }

    #[test]
    fn detached_commit_promotion_retry_stays_behind_concurrently_queued_work() {
        let local = node_id("node_commit_promotion_concurrent_local");
        let failing = node_id("node_commit_promotion_concurrent_failing");
        let healthy = node_id("node_commit_promotion_concurrent_healthy");
        let gate = Arc::new((Mutex::new((false, false)), Condvar::new()));
        let sender_gate = Arc::clone(&gate);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let sender_calls = Arc::clone(&calls);
        let failed_once = Arc::new(AtomicBool::new(false));
        let sender_failed_once = Arc::clone(&failed_once);
        let sender_failing = failing.clone();
        let sender: DfsMessageSender = Arc::new(move |target, command, _options| {
            sender_calls
                .lock()
                .expect("promotion calls")
                .push(target.clone());
            if target == &sender_failing && !sender_failed_once.swap(true, Ordering::SeqCst) {
                let (state, ready) = sender_gate.as_ref();
                let mut state = state.lock().expect("promotion gate");
                state.0 = true;
                ready.notify_all();
                let (state, timeout) = ready
                    .wait_timeout_while(state, Duration::from_secs(2), |state| !state.1)
                    .expect("promotion gate wait");
                assert!(!timeout.timed_out(), "promotion release timed out");
                drop(state);
                return Err(DfsPeerTransportError::new("first promotion failed"));
            }
            Ok(test_committed_promotion_rpc_response(command))
        });
        let mut transport = DfsNodeTransport::with_connect_timeout(
            local.clone(),
            vec![failing.clone(), healthy.clone()],
            Duration::from_secs(1),
            sender,
        );
        transport
            .send_detached_routed_message(
                &failing,
                test_commit_version_message(&local, &failing, 1),
            )
            .expect("queue failing promotion");
        let retry_started = Instant::now();
        let mut worker = transport.fork_detached_send_worker();
        let worker_handle = std::thread::spawn(move || worker.drain_at(retry_started));

        let (state, ready) = gate.as_ref();
        let state = state.lock().expect("promotion gate");
        let (mut state, timeout) = ready
            .wait_timeout_while(state, Duration::from_secs(2), |state| !state.0)
            .expect("promotion entry wait");
        assert!(!timeout.timed_out(), "promotion did not enter sender");
        transport
            .send_detached_routed_message(
                &healthy,
                test_commit_version_message(&local, &healthy, 2),
            )
            .expect("queue concurrent healthy promotion");
        state.1 = true;
        ready.notify_all();
        drop(state);

        worker_handle
            .join()
            .expect("detached worker join")
            .expect_err("first promotion attempt should fail");
        assert_eq!(
            transport.detached_send_targets(),
            vec![failing.clone()],
            "only the cooling retry should remain after ready work drains"
        );
        assert_eq!(
            calls.lock().expect("promotion calls").as_slice(),
            &[failing.clone(), healthy.clone()],
            "concurrent ready work must drain before the worker returns"
        );

        transport
            .flush_detached_peer_sends_with_now(&|| retry_started)
            .expect("cooling retry must not run immediately");
        transport
            .flush_detached_peer_sends_with_now(&|| retry_started + DFS_DETACHED_SEND_RETRY_DELAY)
            .expect("failed retry should settle once eligible");
        assert_eq!(
            calls.lock().expect("promotion calls").as_slice(),
            &[failing.clone(), healthy, failing]
        );
    }

    #[test]
    fn detached_commit_promotion_inflight_identity_suppresses_duplicate_enqueue() {
        let local = node_id("node_commit_promotion_inflight_local");
        let remote = node_id("node_commit_promotion_inflight_remote");
        let gate = Arc::new((Mutex::new((false, false)), Condvar::new()));
        let sender_gate = Arc::clone(&gate);
        let calls = Arc::new(AtomicUsize::new(0));
        let sender_calls = Arc::clone(&calls);
        let sender: DfsMessageSender = Arc::new(move |_target, command, _options| {
            sender_calls.fetch_add(1, Ordering::SeqCst);
            let (state, ready) = sender_gate.as_ref();
            let mut state = state.lock().expect("promotion gate");
            state.0 = true;
            ready.notify_all();
            let (state, timeout) = ready
                .wait_timeout_while(state, Duration::from_secs(2), |state| !state.1)
                .expect("promotion gate wait");
            assert!(!timeout.timed_out(), "promotion release timed out");
            drop(state);
            Ok(test_committed_promotion_rpc_response(command))
        });
        let mut transport = DfsNodeTransport::with_connect_timeout(
            local.clone(),
            vec![remote.clone()],
            Duration::from_secs(1),
            sender,
        );
        let promotion = test_commit_version_message(&local, &remote, 1);
        transport
            .send_detached_routed_message(&remote, promotion.clone())
            .expect("queue promotion");
        let mut worker = transport.fork_detached_send_worker();
        let worker_handle = std::thread::spawn(move || worker.drain());

        let (state, ready) = gate.as_ref();
        let state = state.lock().expect("promotion gate");
        let (mut state, timeout) = ready
            .wait_timeout_while(state, Duration::from_secs(2), |state| !state.0)
            .expect("promotion entry wait");
        assert!(!timeout.timed_out(), "promotion did not enter sender");
        transport
            .send_detached_routed_message(&remote, promotion)
            .expect("duplicate enqueue should be suppressed");
        assert_eq!(transport.detached_send_count(), 0);
        assert_eq!(transport.pending_commit_promotion_count(), 1);
        state.1 = true;
        ready.notify_all();
        drop(state);

        worker_handle
            .join()
            .expect("detached worker join")
            .expect("promotion should settle");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(transport.detached_send_count(), 0);
        assert_eq!(transport.pending_commit_promotion_count(), 0);
        assert_eq!(transport.settled_commit_promotion_count(), 1);
    }

    #[test]
    fn detached_commit_promotion_cooling_identity_suppresses_duplicate_enqueue() {
        let local = node_id("node_commit_promotion_cooling_dedupe_local");
        let remote = node_id("node_commit_promotion_cooling_dedupe_remote");
        let calls = Arc::new(AtomicUsize::new(0));
        let sender_calls = Arc::clone(&calls);
        let sender: DfsMessageSender = Arc::new(move |_target, command, _options| {
            if sender_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(DfsPeerTransportError::new("first promotion attempt failed"))
            } else {
                Ok(test_committed_promotion_rpc_response(command))
            }
        });
        let mut transport = DfsNodeTransport::with_connect_timeout(
            local.clone(),
            vec![remote.clone()],
            Duration::from_secs(1),
            sender,
        );
        let promotion = test_commit_version_message(&local, &remote, 1);
        transport
            .send_detached_routed_message(&remote, promotion.clone())
            .expect("queue promotion");
        let started = Instant::now();
        transport
            .flush_detached_peer_sends_with_now(&|| started)
            .expect_err("promotion enters cooling");

        transport
            .send_detached_routed_message(&remote, promotion)
            .expect("cooling duplicate should be suppressed");
        transport
            .flush_detached_peer_sends_with_now(&|| started)
            .expect("cooling promotion is not immediately eligible");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(transport.detached_send_count(), 1);
        assert_eq!(transport.pending_commit_promotion_count(), 1);

        transport
            .flush_detached_peer_sends_with_now(&|| started + DFS_DETACHED_SEND_RETRY_DELAY)
            .expect("eligible promotion settles");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(transport.detached_send_count(), 0);
        assert_eq!(transport.pending_commit_promotion_count(), 0);
        assert_eq!(transport.settled_commit_promotion_count(), 1);
    }

    #[test]
    fn detached_commit_promotion_durable_ack_retries_until_committed() {
        let local = node_id("node_commit_promotion_durable_local");
        let remote = node_id("node_commit_promotion_durable_remote");
        let attempts = Arc::new(AtomicUsize::new(0));
        let sender_attempts = Arc::clone(&attempts);
        let sender: DfsMessageSender = Arc::new(move |_target, command, _options| {
            if sender_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                let DfsNetworkMessage::CommitVersion(commit) = &command.routed_message.message
                else {
                    panic!("test response requires CommitVersion")
                };
                Ok(test_commit_version_rpc_response(
                    command,
                    DfsBlobTransferResult::Durable {
                        content_hash: commit.content_hash.clone(),
                    },
                ))
            } else {
                Ok(test_committed_promotion_rpc_response(command))
            }
        });
        let mut transport = DfsNodeTransport::with_connect_timeout(
            local.clone(),
            vec![remote.clone()],
            Duration::from_secs(1),
            sender,
        );
        transport
            .send_detached_routed_message(&remote, test_commit_version_message(&local, &remote, 1))
            .expect("queue promotion");

        let retry_started = Instant::now();
        transport
            .flush_detached_peer_sends_with_now(&|| retry_started)
            .expect_err("Durable does not prove visible version promotion");
        assert_eq!(transport.detached_send_count(), 1);
        assert_eq!(transport.pending_commit_promotion_count(), 1);
        assert_eq!(transport.settled_commit_promotion_count(), 0);
        transport
            .flush_detached_peer_sends_with_now(&|| retry_started + DFS_DETACHED_SEND_RETRY_DELAY)
            .expect("Committed acknowledgement should settle promotion");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(transport.pending_commit_promotion_count(), 0);
        assert_eq!(transport.settled_commit_promotion_count(), 1);
    }

    #[test]
    fn detached_commit_promotion_settlement_is_bounded_per_replica_object() {
        let local = node_id("node_commit_promotion_bounded_local");
        let remote = node_id("node_commit_promotion_bounded_remote");
        let shared_object_id =
            DfsObjectId::from_legacy_named("bounded_promotion_object").expect("object id");
        let calls = Arc::new(AtomicUsize::new(0));
        let sender_calls = Arc::clone(&calls);
        let sender: DfsMessageSender = Arc::new(move |_target, command, _options| {
            sender_calls.fetch_add(1, Ordering::SeqCst);
            Ok(test_committed_promotion_rpc_response(command))
        });
        let mut transport = DfsNodeTransport::with_connect_timeout(
            local.clone(),
            vec![remote.clone()],
            Duration::from_secs(1),
            sender,
        );
        let mut last_promotion = None;
        for frame_id in 1..=32 {
            let mut promotion = test_commit_version_message(&local, &remote, frame_id);
            let DfsNetworkMessage::CommitVersion(command) = &mut promotion.message else {
                panic!("test promotion")
            };
            command.object_id = shared_object_id.clone();
            last_promotion = Some(promotion.clone());
            transport
                .send_detached_routed_message(&remote, promotion)
                .expect("queue promotion version");
            transport
                .flush_detached_sends()
                .expect("settle promotion version");
            assert_eq!(transport.settled_commit_promotion_count(), 1);
        }
        assert_eq!(transport.pending_commit_promotion_count(), 0);
        assert_eq!(calls.load(Ordering::SeqCst), 32);

        transport
            .send_detached_routed_message(&remote, last_promotion.expect("last promotion identity"))
            .expect("settled duplicate should be suppressed");
        assert_eq!(transport.detached_send_count(), 0);
        assert_eq!(calls.load(Ordering::SeqCst), 32);
    }

    #[test]
    fn detached_commit_promotion_terminal_history_evicts_oldest_safe_replays() {
        let local = node_id("node_commit_promotion_eviction_local");
        let remote = node_id("node_commit_promotion_eviction_remote");
        let calls = Arc::new(AtomicUsize::new(0));
        let sender_calls = Arc::clone(&calls);
        let sender: DfsMessageSender = Arc::new(move |_target, command, _options| {
            sender_calls.fetch_add(1, Ordering::SeqCst);
            Ok(test_committed_promotion_rpc_response(command))
        });
        let mut transport = DfsNodeTransport::with_connect_timeout(
            local.clone(),
            vec![remote.clone()],
            Duration::from_secs(1),
            Arc::clone(&sender),
        );
        let oldest = test_commit_version_message(&local, &remote, 1);
        let newest_frame = DFS_TERMINAL_COMMIT_PROMOTION_MAX_ENTRIES as u64 + 1;
        let newest = test_commit_version_message(&local, &remote, newest_frame);

        // Populate terminal outcomes directly: the bounded-history behavior is
        // independent of RPC fan-out, and avoiding thousands of test threads
        // keeps this regression focused on the retained outbox state.
        for frame_id in 1..=newest_frame {
            let send = DeferredDfsPeerSend {
                sender: Arc::clone(&sender),
                options: DfsObjectRpcSendOptions::new(Duration::from_secs(1)),
                operation_deadline: None,
                target_node_id: remote.clone(),
                routed_message: test_commit_version_message(&local, &remote, frame_id),
                promotion_sequence: Some(frame_id),
                retry_attempts: 0,
            };
            transport
                .mark_commit_promotion_terminal(&send, DfsCommitPromotionTerminalOutcome::Committed)
                .expect("record terminal promotion");
        }

        assert_eq!(
            transport.settled_commit_promotion_count(),
            DFS_TERMINAL_COMMIT_PROMOTION_RETAIN_ENTRIES,
            "terminal promotion history should compact to its recent window"
        );
        transport
            .send_detached_routed_message(&remote, newest)
            .expect("recent terminal duplicate");
        assert_eq!(
            transport.detached_send_count(),
            0,
            "the retained recent window should still suppress exact replay"
        );

        transport
            .send_detached_routed_message(&remote, oldest)
            .expect("evicted terminal replay");
        assert_eq!(transport.detached_send_count(), 1);
        transport
            .flush_detached_sends()
            .expect("evicted committed promotion replays idempotently");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(transport.pending_commit_promotion_count(), 0);
    }

    #[test]
    fn shared_detached_send_state_enforces_global_promotion_cap_two() {
        let local = node_id("node_shared_promotion_cap_local");
        let a_one = node_id("node_shared_promotion_cap_a_one");
        let a_two = node_id("node_shared_promotion_cap_a_two");
        let b_one = node_id("node_shared_promotion_cap_b_one");
        let b_two = node_id("node_shared_promotion_cap_b_two");
        let detached_send_state = DfsNodeDetachedSendState::new();
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let a_gate = Arc::new((Mutex::new((0usize, false)), Condvar::new()));
        let a_sender_active = Arc::clone(&active);
        let a_sender_max_active = Arc::clone(&max_active);
        let a_sender_gate = Arc::clone(&a_gate);
        let a_sender: DfsMessageSender = Arc::new(move |_target, command, _options| {
            let active = a_sender_active.fetch_add(1, Ordering::SeqCst) + 1;
            a_sender_max_active.fetch_max(active, Ordering::SeqCst);
            let (state, ready) = a_sender_gate.as_ref();
            let mut state = state.lock().expect("lane A promotion gate");
            state.0 += 1;
            ready.notify_all();
            let (state, timeout) = ready
                .wait_timeout_while(state, Duration::from_secs(2), |state| !state.1)
                .expect("lane A promotion gate wait");
            assert!(!timeout.timed_out(), "lane A promotion release timed out");
            drop(state);
            a_sender_active.fetch_sub(1, Ordering::SeqCst);
            Ok(test_committed_promotion_rpc_response(command))
        });
        let (b_sender_entered, b_sender_entries) = mpsc::sync_channel(2);
        let b_sender_active = Arc::clone(&active);
        let b_sender_max_active = Arc::clone(&max_active);
        let b_sender: DfsMessageSender = Arc::new(move |_target, command, _options| {
            let active = b_sender_active.fetch_add(1, Ordering::SeqCst) + 1;
            b_sender_max_active.fetch_max(active, Ordering::SeqCst);
            b_sender_entered
                .send(())
                .expect("record lane B promotion entry");
            b_sender_active.fetch_sub(1, Ordering::SeqCst);
            Ok(test_committed_promotion_rpc_response(command))
        });
        let mut lane_a = DfsNodeServedMessageTransport::new_with_message_sender_and_detached_state(
            local.clone(),
            vec![a_one.clone(), a_two.clone()],
            a_sender,
            Duration::from_secs(1),
            detached_send_state.clone(),
        );
        let mut lane_b = DfsNodeServedMessageTransport::new_with_message_sender_and_detached_state(
            local.clone(),
            vec![b_one.clone(), b_two.clone()],
            b_sender,
            Duration::from_secs(1),
            detached_send_state,
        );
        for (frame_id, target) in [(1, &a_one), (2, &a_two)] {
            lane_a
                .node_transport
                .send_detached_routed_message(
                    target,
                    test_commit_version_message(&local, target, frame_id),
                )
                .expect("queue lane A promotion");
        }
        let mut worker_a = lane_a.node_transport.fork_detached_send_worker();
        let worker_a_handle = std::thread::spawn(move || worker_a.drain());

        let (state, ready) = a_gate.as_ref();
        let state = state.lock().expect("lane A promotion gate");
        let (mut state, timeout) = ready
            .wait_timeout_while(state, Duration::from_secs(2), |state| state.0 < 2)
            .expect("lane A promotion entry wait");
        assert!(!timeout.timed_out(), "lane A did not fill the cap-two wave");
        for (frame_id, target) in [(3, &b_one), (4, &b_two)] {
            lane_b
                .node_transport
                .send_detached_routed_message(
                    target,
                    test_commit_version_message(&local, target, frame_id),
                )
                .expect("queue lane B promotion");
        }
        let mut worker_b = lane_b.node_transport.fork_detached_send_worker();
        let (worker_b_started, worker_b_start) = mpsc::sync_channel(0);
        let worker_b_handle = std::thread::spawn(move || {
            worker_b_started.send(()).expect("start lane B worker");
            worker_b.drain()
        });
        worker_b_start
            .recv_timeout(Duration::from_secs(2))
            .expect("lane B worker did not start");
        assert_eq!(
            b_sender_entries.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout),
            "a second shared-state worker exceeded the aggregate cap"
        );

        state.1 = true;
        ready.notify_all();
        drop(state);
        worker_a_handle
            .join()
            .expect("lane A worker join")
            .expect("lane A drain");
        b_sender_entries
            .recv_timeout(Duration::from_secs(2))
            .expect("first lane B promotion did not run after release");
        b_sender_entries
            .recv_timeout(Duration::from_secs(2))
            .expect("second lane B promotion did not run after release");
        worker_b_handle
            .join()
            .expect("lane B worker join")
            .expect("lane B drain");
        assert_eq!(max_active.load(Ordering::SeqCst), 2);
        assert_eq!(lane_a.node_transport.detached_send_count(), 0);
    }

    #[test]
    fn exclusive_detached_worker_waits_for_the_shared_drain_gate() {
        let local = node_id("node_exclusive_detached_worker_local");
        let detached_send_state = DfsNodeDetachedSendState::new();
        let sender: DfsMessageSender = Arc::new(|_target, command, _options| {
            Ok(DfsMessageResponse {
                request_id: command.request_id.clone(),
                outbound_messages: Vec::new(),
            })
        });
        let transport = DfsNodeServedMessageTransport::new_with_message_sender_and_detached_state(
            local,
            Vec::new(),
            sender,
            Duration::from_secs(1),
            detached_send_state.clone(),
        );
        let mut worker = transport.node_transport.fork_detached_send_worker();
        let drain_gate = Arc::clone(&detached_send_state.drain_gate);
        let drain_guard = drain_gate.lock().expect("hold shared drain gate");
        let (started_tx, started_rx) = mpsc::sync_channel(0);
        let (done_tx, done_rx) = mpsc::sync_channel(0);
        let worker_thread = std::thread::spawn(move || {
            started_tx.send(()).expect("report worker start");
            let result = worker.drain_exclusive();
            done_tx.send(result).expect("report worker result");
        });
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("exclusive worker started");
        assert_eq!(
            done_rx.recv_timeout(Duration::from_millis(50)),
            Err(mpsc::RecvTimeoutError::Timeout),
            "exclusive worker must not mistake an in-progress drain for an empty outbox"
        );
        drop(drain_guard);
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("exclusive worker completed after gate release")
            .expect("exclusive drain");
        worker_thread.join().expect("exclusive worker join");
    }

    #[test]
    fn detached_worker_wait_observes_work_restored_by_competing_drain() {
        let local = node_id("node_detached_restore_wait_local");
        let remote = node_id("node_detached_restore_wait_remote");
        let (send_started_tx, send_started_rx) = mpsc::sync_channel(0);
        let (release_send_tx, release_send_rx) = mpsc::sync_channel(0);
        let release_send_rx = Arc::new(Mutex::new(release_send_rx));
        let sender_release = Arc::clone(&release_send_rx);
        let sender: DfsMessageSender = Arc::new(move |_target, _command, _options| {
            send_started_tx.send(()).expect("report send start");
            sender_release
                .lock()
                .expect("send release lock")
                .recv_timeout(Duration::from_secs(2))
                .expect("release failed send");
            Err(DfsPeerTransportError::new(
                "injected competing promotion failure",
            ))
        });
        let mut transport = DfsNodeTransport::with_connect_timeout(
            local.clone(),
            [remote.clone()],
            Duration::from_secs(1),
            sender,
        );
        transport
            .send_detached_routed_message(&remote, test_commit_version_message(&local, &remote, 1))
            .expect("queue promotion");
        let mut competing_worker = transport.fork_detached_send_worker();
        let waiting_worker = transport.fork_detached_send_worker();
        let inspection_worker = waiting_worker.clone();
        let drain_thread = std::thread::spawn(move || competing_worker.drain());
        send_started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("competing drain took the promotion");

        let (ready, retry_not_before, observed_generation) = waiting_worker
            .scheduling_snapshot()
            .expect("snapshot in-flight outbox");
        assert!(!ready);
        assert!(retry_not_before.is_none());

        let (wait_started_tx, wait_started_rx) = mpsc::sync_channel(0);
        let (wait_done_tx, wait_done_rx) = mpsc::sync_channel(0);
        let wait_thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("wait runtime");
            wait_started_tx.send(()).expect("report wait start");
            let result = runtime.block_on(waiting_worker.wait_for_change(observed_generation));
            wait_done_tx.send(result).expect("report wait result");
        });
        wait_started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("outbox waiter started");
        assert_eq!(
            wait_done_rx.recv_timeout(Duration::from_millis(50)),
            Err(mpsc::RecvTimeoutError::Timeout),
            "an empty in-flight outbox must keep the watcher parked"
        );

        release_send_tx.send(()).expect("release failed send");
        wait_done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("restoration woke the outbox watcher")
            .expect("wait for restored work");
        drain_thread
            .join()
            .expect("competing drain join")
            .expect_err("competing drain must restore the failed promotion");
        wait_thread.join().expect("outbox wait join");
        let (ready, retry_not_before, restored_generation) = inspection_worker
            .scheduling_snapshot()
            .expect("snapshot restored outbox");
        assert!(!ready, "restored promotion should still be cooling");
        assert!(retry_not_before.is_some());
        assert!(restored_generation > observed_generation);
    }

    #[test]
    fn detached_send_state_is_shared_without_replacing_origin_routing_capability() {
        let local = node_id("node_detached_shared_state_local");
        let remote = node_id("node_detached_shared_state_remote");
        let state = DfsNodeDetachedSendState::new();
        let primary_calls = Arc::new(AtomicUsize::new(0));
        let sender_primary_calls = Arc::clone(&primary_calls);
        let primary_sender: DfsMessageSender = Arc::new(move |_target, command, _options| {
            sender_primary_calls.fetch_add(1, Ordering::SeqCst);
            Ok(test_committed_promotion_rpc_response(command))
        });
        let recovery_calls = Arc::new(AtomicUsize::new(0));
        let sender_recovery_calls = Arc::clone(&recovery_calls);
        let recovery_sender: DfsMessageSender = Arc::new(move |_target, command, _options| {
            sender_recovery_calls.fetch_add(1, Ordering::SeqCst);
            Ok(test_committed_promotion_rpc_response(command))
        });
        let mut primary = DfsNodeServedMessageTransport::new_with_message_sender_and_detached_state(
            local.clone(),
            vec![remote.clone()],
            primary_sender,
            Duration::from_secs(1),
            state.clone(),
        );
        let recovery = DfsNodeServedMessageTransport::new_with_message_sender_and_detached_state(
            local.clone(),
            vec![remote.clone()],
            recovery_sender,
            Duration::from_secs(1),
            state,
        );
        assert_eq!(
            primary
                .detached_send_state()
                .inner
                .lock()
                .expect("state")
                .ready_sends
                .len(),
            0
        );
        primary
            .node_transport
            .send_detached_routed_message(&remote, test_commit_version_message(&local, &remote, 1))
            .expect("queue shared promotion");
        assert_eq!(recovery.node_transport.detached_send_count(), 1);

        let mut worker = recovery.node_transport.fork_detached_send_worker();
        worker.drain().expect("shared worker should drain outbox");
        assert_eq!(primary.node_transport.detached_send_count(), 0);
        assert_eq!(primary_calls.load(Ordering::SeqCst), 1);
        assert_eq!(recovery_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn detached_commit_promotion_rejection_is_requeued_until_positive_ack() {
        let local = node_id("node_commit_promotion_reject_local");
        let remote = node_id("node_commit_promotion_reject_remote");
        let attempts = Arc::new(AtomicUsize::new(0));
        let sender_attempts = Arc::clone(&attempts);
        let sender: DfsMessageSender = Arc::new(move |_target, command, _options| {
            if sender_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(test_commit_version_rpc_response(
                    command,
                    DfsBlobTransferResult::Rejected {
                        reason: "replica not ready".to_string(),
                    },
                ))
            } else {
                Ok(test_committed_promotion_rpc_response(command))
            }
        });
        let mut transport = DfsNodeTransport::with_connect_timeout(
            local.clone(),
            vec![remote.clone()],
            Duration::from_secs(1),
            sender,
        );
        transport
            .send_detached_routed_message(&remote, test_commit_version_message(&local, &remote, 1))
            .expect("queue commit promotion");

        let retry_started = Instant::now();
        transport
            .flush_detached_peer_sends_with_now(&|| retry_started)
            .expect_err("negative acknowledgement must retain promotion");
        assert_eq!(transport.detached_send_count(), 1);
        transport
            .flush_detached_peer_sends_with_now(&|| retry_started + DFS_DETACHED_SEND_RETRY_DELAY)
            .expect("positive acknowledgement should settle promotion");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(transport.detached_send_count(), 0);
    }

    #[test]
    fn detached_commit_promotion_not_desired_rejection_is_retired_boundedly() {
        let local = node_id("node_commit_promotion_retire_local");
        let remote = node_id("node_commit_promotion_retire_remote");
        let attempts = Arc::new(AtomicUsize::new(0));
        let sender_attempts = Arc::clone(&attempts);
        let sender: DfsMessageSender = Arc::new(move |_target, command, _options| {
            sender_attempts.fetch_add(1, Ordering::SeqCst);
            let DfsNetworkMessage::CommitVersion(commit) = &command.routed_message.message else {
                panic!("test response requires CommitVersion")
            };
            let reason = DfsError::UnauthorizedReplicaCommand {
                source_node_id: command.routed_message.source_node_id.clone(),
                reason: crate::dfs_replica_not_desired_reason(&commit.intent_id),
            }
            .to_string();
            Ok(test_commit_version_rpc_response(
                command,
                DfsBlobTransferResult::Rejected { reason },
            ))
        });
        let mut transport = DfsNodeTransport::with_connect_timeout(
            local.clone(),
            vec![remote.clone()],
            Duration::from_secs(1),
            sender,
        );
        let promotion = test_commit_version_message(&local, &remote, 1);
        transport
            .send_detached_routed_message(&remote, promotion.clone())
            .expect("queue commit promotion");

        let started = Instant::now();
        transport
            .flush_detached_peer_sends_with_now(&|| started)
            .expect("not-desired rejection should retire the exact promotion");
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert_eq!(transport.detached_send_count(), 0);
        assert_eq!(transport.pending_commit_promotion_count(), 0);
        assert_eq!(transport.settled_commit_promotion_count(), 0);
        assert_eq!(transport.retired_not_desired_commit_promotion_count(), 1);

        transport
            .flush_detached_peer_sends_with_now(&|| started + DFS_DETACHED_SEND_RETRY_DELAY)
            .expect("retired promotion must not enter retry cooling");
        transport
            .send_detached_routed_message(&remote, promotion.clone())
            .expect("exact retired duplicate should be suppressed");
        transport
            .flush_detached_peer_sends_with_now(&|| started + DFS_DETACHED_SEND_RETRY_DELAY)
            .expect("exact retired duplicate must remain terminal");
        assert_eq!(attempts.load(Ordering::SeqCst), 1);

        let mut superseding = promotion;
        superseding.frame_id = 2;
        superseding.message_id = 2;
        let DfsNetworkMessage::CommitVersion(command) = &mut superseding.message else {
            panic!("test promotion requires CommitVersion")
        };
        command.request_id = "commit_request_retire_superseding".to_string();
        command.intent_id = "intent_retire_superseding".to_string();
        transport
            .send_detached_routed_message(&remote, superseding)
            .expect("a new intent for the same version/hash must remain queueable");
        assert_eq!(transport.detached_send_count(), 1);
        transport
            .flush_detached_peer_sends_with_now(&|| started + DFS_DETACHED_SEND_RETRY_DELAY)
            .expect("superseding not-desired promotion should replace the retired frontier");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(transport.detached_send_count(), 0);
        assert_eq!(
            transport.retired_not_desired_commit_promotion_count(),
            1,
            "retired bookkeeping must remain bounded per target/filesystem/object key"
        );
    }

    #[test]
    fn detached_commit_promotion_not_desired_rejection_must_match_exact_command() {
        let local = node_id("node_commit_promotion_retire_exact_local");
        let remote = node_id("node_commit_promotion_retire_exact_remote");
        let wrong_requester = node_id("node_commit_promotion_retire_wrong_requester");
        let attempts = Arc::new(AtomicUsize::new(0));
        let sender_attempts = Arc::clone(&attempts);
        let sender: DfsMessageSender = Arc::new(move |_target, command, _options| {
            let attempt = sender_attempts.fetch_add(1, Ordering::SeqCst);
            let DfsNetworkMessage::CommitVersion(commit) = &command.routed_message.message else {
                panic!("test response requires CommitVersion")
            };
            let exact_reason = DfsError::UnauthorizedReplicaCommand {
                source_node_id: command.routed_message.source_node_id.clone(),
                reason: crate::dfs_replica_not_desired_reason(&commit.intent_id),
            }
            .to_string();
            let reason = match attempt {
                0 => DfsError::UnauthorizedReplicaCommand {
                    source_node_id: wrong_requester.clone(),
                    reason: crate::dfs_replica_not_desired_reason(&commit.intent_id),
                }
                .to_string(),
                1 => DfsError::UnauthorizedReplicaCommand {
                    source_node_id: command.routed_message.source_node_id.clone(),
                    reason: crate::dfs_replica_not_desired_reason("wrong_intent"),
                }
                .to_string(),
                2 => format!("wrapped: {exact_reason}"),
                _ => exact_reason,
            };
            Ok(test_commit_version_rpc_response(
                command,
                DfsBlobTransferResult::Rejected { reason },
            ))
        });
        let mut transport = DfsNodeTransport::with_connect_timeout(
            local.clone(),
            vec![remote.clone()],
            Duration::from_secs(1),
            sender,
        );
        transport
            .send_detached_routed_message(&remote, test_commit_version_message(&local, &remote, 1))
            .expect("queue commit promotion");

        let started = Instant::now();
        let mut now = started;
        for retry_attempt in 1..=3 {
            transport
                .flush_detached_peer_sends_with_now(&|| now)
                .expect_err("non-exact not-desired reason must remain retryable");
            assert_eq!(transport.detached_send_count(), 1);
            assert_eq!(transport.retired_not_desired_commit_promotion_count(), 0);
            now += dfs_detached_send_retry_delay(retry_attempt);
        }
        transport
            .flush_detached_peer_sends_with_now(&|| now)
            .expect("exact requester and intent should retire the promotion");
        assert_eq!(attempts.load(Ordering::SeqCst), 4);
        assert_eq!(transport.detached_send_count(), 0);
        assert_eq!(transport.retired_not_desired_commit_promotion_count(), 1);
    }

    #[test]
    fn detached_commit_promotion_repair_pending_rejection_is_terminal() {
        let local = node_id("node_commit_promotion_repair_pending_local");
        let remote = node_id("node_commit_promotion_repair_pending_remote");
        let attempts = Arc::new(AtomicUsize::new(0));
        let sender_attempts = Arc::clone(&attempts);
        let sender: DfsMessageSender = Arc::new(move |_target, command, _options| {
            sender_attempts.fetch_add(1, Ordering::SeqCst);
            let DfsNetworkMessage::CommitVersion(commit) = &command.routed_message.message else {
                panic!("test response requires CommitVersion")
            };
            let reason = DfsError::CommitContentRepairPending {
                object_id: commit.object_id.clone(),
                version: commit.version.clone(),
                content_hash: commit.content_hash.clone(),
            }
            .to_string();
            Ok(test_commit_version_rpc_response(
                command,
                DfsBlobTransferResult::Rejected { reason },
            ))
        });
        let mut transport = DfsNodeTransport::with_connect_timeout(
            local.clone(),
            vec![remote.clone()],
            Duration::from_secs(1),
            sender,
        );
        let promotion = test_commit_version_message(&local, &remote, 1);
        transport
            .send_detached_routed_message(&remote, promotion.clone())
            .expect("queue commit promotion");

        let started = Instant::now();
        transport
            .flush_detached_peer_sends_with_now(&|| started)
            .expect("durable repair transition should retire the exact promotion");
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert_eq!(transport.detached_send_count(), 0);
        assert_eq!(transport.pending_commit_promotion_count(), 0);
        assert_eq!(transport.settled_commit_promotion_count(), 0);
        assert_eq!(transport.repair_pending_commit_promotion_count(), 1);

        transport
            .send_detached_routed_message(&remote, promotion)
            .expect("exact repair-pending duplicate should be suppressed");
        transport
            .flush_detached_peer_sends_with_now(&|| started + DFS_DETACHED_SEND_RETRY_DELAY)
            .expect("repair-pending promotion must not enter retry cooling");
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn detached_commit_promotion_repair_pending_must_match_exact_content() {
        let local = node_id("node_commit_promotion_repair_exact_local");
        let remote = node_id("node_commit_promotion_repair_exact_remote");
        let attempts = Arc::new(AtomicUsize::new(0));
        let sender_attempts = Arc::clone(&attempts);
        let sender: DfsMessageSender = Arc::new(move |_target, command, _options| {
            let attempt = sender_attempts.fetch_add(1, Ordering::SeqCst);
            let DfsNetworkMessage::CommitVersion(commit) = &command.routed_message.message else {
                panic!("test response requires CommitVersion")
            };
            let exact_reason = DfsError::CommitContentRepairPending {
                object_id: commit.object_id.clone(),
                version: commit.version.clone(),
                content_hash: commit.content_hash.clone(),
            }
            .to_string();
            let reason = match attempt {
                0 => DfsError::CommitContentRepairPending {
                    object_id: commit.object_id.clone(),
                    version: commit.version.clone(),
                    content_hash: "blake3:wrong-repair-content".to_string(),
                }
                .to_string(),
                1 => format!("wrapped: {exact_reason}"),
                _ => exact_reason,
            };
            Ok(test_commit_version_rpc_response(
                command,
                DfsBlobTransferResult::Rejected { reason },
            ))
        });
        let mut transport = DfsNodeTransport::with_connect_timeout(
            local.clone(),
            vec![remote.clone()],
            Duration::from_secs(1),
            sender,
        );
        transport
            .send_detached_routed_message(&remote, test_commit_version_message(&local, &remote, 1))
            .expect("queue commit promotion");

        let started = Instant::now();
        let mut now = started;
        for retry_attempt in 1..=2 {
            transport
                .flush_detached_peer_sends_with_now(&|| now)
                .expect_err("non-exact repair-pending reason must remain retryable");
            assert_eq!(transport.detached_send_count(), 1);
            assert_eq!(transport.repair_pending_commit_promotion_count(), 0);
            now += dfs_detached_send_retry_delay(retry_attempt);
        }
        transport
            .flush_detached_peer_sends_with_now(&|| now)
            .expect("exact object/version/hash should terminalize for repair");
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        assert_eq!(transport.detached_send_count(), 0);
        assert_eq!(transport.repair_pending_commit_promotion_count(), 1);
    }

    #[test]
    fn detached_commit_promotion_missing_ack_is_requeued() {
        let local = node_id("node_commit_promotion_missing_ack_local");
        let remote = node_id("node_commit_promotion_missing_ack_remote");
        let attempts = Arc::new(AtomicUsize::new(0));
        let sender_attempts = Arc::clone(&attempts);
        let sender: DfsMessageSender = Arc::new(move |_target, command, _options| {
            if sender_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(DfsMessageResponse {
                    request_id: command.request_id.clone(),
                    outbound_messages: Vec::new(),
                })
            } else {
                Ok(test_committed_promotion_rpc_response(command))
            }
        });
        let mut transport = DfsNodeTransport::with_connect_timeout(
            local.clone(),
            vec![remote.clone()],
            Duration::from_secs(1),
            sender,
        );
        transport
            .send_detached_routed_message(&remote, test_commit_version_message(&local, &remote, 1))
            .expect("queue commit promotion");

        let retry_started = Instant::now();
        transport
            .flush_detached_peer_sends_with_now(&|| retry_started)
            .expect_err("missing acknowledgement must retain promotion");
        assert_eq!(transport.detached_send_count(), 1);
        transport
            .flush_detached_peer_sends_with_now(&|| retry_started + DFS_DETACHED_SEND_RETRY_DELAY)
            .expect("matching acknowledgement should settle promotion");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn detached_commit_promotion_mismatched_ack_is_requeued() {
        let local = node_id("node_commit_promotion_mismatch_local");
        let remote = node_id("node_commit_promotion_mismatch_remote");
        let wrong_replica = node_id("node_commit_promotion_mismatch_wrong");
        let attempts = Arc::new(AtomicUsize::new(0));
        let sender_attempts = Arc::clone(&attempts);
        let sender: DfsMessageSender = Arc::new(move |_target, command, _options| {
            let mut response = test_committed_promotion_rpc_response(command);
            if sender_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                let DfsNetworkMessage::CommitVersionResponse(ack) =
                    &mut response.outbound_messages[0].message
                else {
                    panic!("commit acknowledgement")
                };
                ack.replica_node_id = wrong_replica.clone();
            }
            Ok(response)
        });
        let mut transport = DfsNodeTransport::with_connect_timeout(
            local.clone(),
            vec![remote.clone()],
            Duration::from_secs(1),
            sender,
        );
        transport
            .send_detached_routed_message(&remote, test_commit_version_message(&local, &remote, 1))
            .expect("queue commit promotion");

        let retry_started = Instant::now();
        transport
            .flush_detached_peer_sends_with_now(&|| retry_started)
            .expect_err("mismatched acknowledgement must retain promotion");
        assert_eq!(transport.detached_send_count(), 1);
        transport
            .flush_detached_peer_sends_with_now(&|| retry_started + DFS_DETACHED_SEND_RETRY_DELAY)
            .expect("matching acknowledgement should settle promotion");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn detached_commit_promotion_does_not_inherit_completed_request_deadline() {
        let local = node_id("node_commit_promotion_deadline_local");
        let remote = node_id("node_commit_promotion_deadline_remote");
        let calls = Arc::new(AtomicUsize::new(0));
        let sender_calls = Arc::clone(&calls);
        let sender: DfsMessageSender = Arc::new(move |_target, command, _options| {
            sender_calls.fetch_add(1, Ordering::SeqCst);
            Ok(test_committed_promotion_rpc_response(command))
        });
        let mut transport = DfsNodeTransport::with_connect_timeout(
            local.clone(),
            vec![remote.clone()],
            Duration::from_secs(1),
            sender,
        );
        transport.set_operation_deadline(Some(Instant::now() + Duration::from_millis(10)));
        transport
            .send_detached_routed_message(&remote, test_commit_version_message(&local, &remote, 1))
            .expect("queue commit promotion");
        std::thread::sleep(Duration::from_millis(20));

        transport
            .flush_detached_sends()
            .expect("background promotion should use a fresh RPC budget");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn detached_commit_promotion_retains_temporarily_unknown_target() {
        let local = node_id("node_commit_promotion_unknown_local");
        let remote = node_id("node_commit_promotion_unknown_remote");
        let calls = Arc::new(AtomicUsize::new(0));
        let sender_calls = Arc::clone(&calls);
        let sender: DfsMessageSender = Arc::new(move |_target, command, _options| {
            sender_calls.fetch_add(1, Ordering::SeqCst);
            Ok(test_committed_promotion_rpc_response(command))
        });
        let mut transport = DfsNodeTransport::with_connect_timeout(
            local.clone(),
            Vec::<NodeId>::new(),
            Duration::from_secs(1),
            sender,
        );

        transport
            .send_detached_routed_message(&remote, test_commit_version_message(&local, &remote, 1))
            .expect("unknown promotion target must remain queueable");
        assert_eq!(transport.detached_send_count(), 1);
        transport
            .flush_detached_sends()
            .expect("dynamically routable promotion should settle");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
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
            .send_routed_call(
                &remote,
                DfsRoutedCall::value(test_message(&local, &remote, 1)),
            )
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
    outbound_attachment: Option<DfsOutboundAttachment>,
    node_transport: DfsNodeTransport,
}

impl DfsNodeServedMessageTransport {
    pub fn new_with_peer_transport_for_nodes(
        local_node_id: NodeId,
        peer_node_ids: impl IntoIterator<Item = NodeId>,
        peer_transport: Arc<dyn DfsPeerTransport>,
        connect_timeout: Duration,
    ) -> Self {
        Self::new_with_peer_transport_for_nodes_and_detached_state(
            local_node_id,
            peer_node_ids,
            peer_transport,
            connect_timeout,
            DfsNodeDetachedSendState::default(),
        )
    }

    pub fn new_with_peer_transport_for_nodes_and_detached_state(
        local_node_id: NodeId,
        peer_node_ids: impl IntoIterator<Item = NodeId>,
        peer_transport: Arc<dyn DfsPeerTransport>,
        connect_timeout: Duration,
        detached_send_state: DfsNodeDetachedSendState,
    ) -> Self {
        let message_peer_transport = Arc::clone(&peer_transport);
        Self::new_with_message_sender_for_nodes_and_detached_state(
            local_node_id,
            peer_node_ids,
            dfs_message_sender_from_peer_transport(message_peer_transport),
            Some(peer_transport),
            None,
            connect_timeout,
            detached_send_state,
        )
    }

    pub fn new_with_peer_transports_for_nodes(
        local_node_id: NodeId,
        peer_node_ids: impl IntoIterator<Item = NodeId>,
        peer_transport: Arc<dyn DfsPeerTransport>,
        async_peer_transport: Arc<dyn AsyncDfsPeerTransport>,
        connect_timeout: Duration,
    ) -> Self {
        Self::new_with_peer_transports_for_nodes_and_detached_state(
            local_node_id,
            peer_node_ids,
            peer_transport,
            async_peer_transport,
            connect_timeout,
            DfsNodeDetachedSendState::default(),
        )
    }

    pub fn new_with_peer_transports_for_nodes_and_detached_state(
        local_node_id: NodeId,
        peer_node_ids: impl IntoIterator<Item = NodeId>,
        peer_transport: Arc<dyn DfsPeerTransport>,
        async_peer_transport: Arc<dyn AsyncDfsPeerTransport>,
        connect_timeout: Duration,
        detached_send_state: DfsNodeDetachedSendState,
    ) -> Self {
        let message_peer_transport = Arc::clone(&peer_transport);
        Self::new_with_message_sender_for_nodes_and_detached_state(
            local_node_id,
            peer_node_ids,
            dfs_message_sender_from_peer_transport(message_peer_transport),
            Some(peer_transport),
            Some(async_peer_transport),
            connect_timeout,
            detached_send_state,
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
            None,
            connect_timeout,
        )
    }

    #[cfg(test)]
    pub(crate) fn new_with_message_sender_and_detached_state(
        local_node_id: NodeId,
        peer_node_ids: impl IntoIterator<Item = NodeId>,
        sender: DfsMessageSender,
        connect_timeout: Duration,
        detached_send_state: DfsNodeDetachedSendState,
    ) -> Self {
        Self::new_with_message_sender_for_nodes_and_detached_state(
            local_node_id,
            peer_node_ids,
            sender,
            None,
            None,
            connect_timeout,
            detached_send_state,
        )
    }

    #[cfg(test)]
    fn new_with_message_sender_for_nodes(
        local_node_id: NodeId,
        peer_node_ids: impl IntoIterator<Item = NodeId>,
        sender: DfsMessageSender,
        peer_transport: Option<Arc<dyn DfsPeerTransport>>,
        connect_timeout: Duration,
    ) -> Self {
        Self::new_with_message_sender_for_nodes_and_detached_state(
            local_node_id,
            peer_node_ids,
            sender,
            peer_transport,
            None,
            connect_timeout,
            DfsNodeDetachedSendState::default(),
        )
    }

    fn new_with_message_sender_for_nodes_and_detached_state(
        local_node_id: NodeId,
        peer_node_ids: impl IntoIterator<Item = NodeId>,
        sender: DfsMessageSender,
        peer_transport: Option<Arc<dyn DfsPeerTransport>>,
        async_peer_transport: Option<Arc<dyn AsyncDfsPeerTransport>>,
        connect_timeout: Duration,
        detached_send_state: DfsNodeDetachedSendState,
    ) -> Self {
        let async_sender = async_peer_transport
            .as_ref()
            .map(|transport| async_dfs_message_sender_from_peer_transport(Arc::clone(transport)));
        let node_transport = DfsNodeTransport::with_senders_for_nodes_and_detached_state(
            local_node_id.clone(),
            peer_node_ids,
            connect_timeout,
            Some(sender),
            async_sender,
            None,
            None,
            peer_transport,
            async_peer_transport,
            detached_send_state,
        );
        Self {
            local_node_id,
            return_node_id: None,
            inbound_messages: VecDeque::new(),
            outbound_messages: Vec::new(),
            outbound_attachment: None,
            node_transport,
        }
    }

    pub fn detached_send_state(&self) -> DfsNodeDetachedSendState {
        self.node_transport.detached_send_state.clone()
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
        self.finish_message_with_attachment().0
    }

    pub fn finish_message_with_attachment(
        &mut self,
    ) -> (Vec<DfsRoutedMessage>, Option<DfsOutboundAttachment>) {
        self.return_node_id = None;
        self.inbound_messages.clear();
        self.node_transport.incoming_messages.clear();
        (
            std::mem::take(&mut self.outbound_messages),
            self.outbound_attachment.take(),
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

impl AsyncDfsMessageTransport for DfsNodeServedMessageTransport {
    fn local_node_id(&self) -> &NodeId {
        DfsTransportIdentity::local_node_id(self)
    }

    fn send_routed_call_async<'a>(
        &'a mut self,
        target_node_id: &'a NodeId,
        call: DfsRoutedCall,
    ) -> DfsMessageTransportFuture<'a, Result<(), NetworkError>> {
        Box::pin(async move {
            let DfsRoutedCall {
                message,
                attachment,
            } = call;
            if &message.target_node_id != target_node_id {
                return Err(NetworkError::FrameTargetMismatch {
                    requested_target_node_id: target_node_id.clone(),
                    frame_target_node_id: message.target_node_id.clone(),
                });
            }
            if target_node_id == &self.local_node_id {
                let message =
                    crate::message_transport::materialize_dfs_routed_call(DfsRoutedCall {
                        message,
                        attachment,
                    })
                    .await?;
                self.inbound_messages.push_back(message);
                return Ok(());
            }
            if self
                .return_node_id
                .as_ref()
                .is_some_and(|return_node_id| target_node_id == return_node_id)
                && dfs_network_message_can_return_to_served_caller(&message.message)
            {
                if attachment.is_some() && self.outbound_attachment.is_some() {
                    return Err(NetworkError::Serialization {
                        detail: "DFS served response attempted to return more than one attachment"
                            .to_string(),
                    });
                }
                self.outbound_messages.push(message);
                if let Some(attachment) = attachment {
                    self.outbound_attachment = Some(attachment);
                }
                return Ok(());
            }
            self.node_transport
                .send_routed_call_to_peer_async(
                    target_node_id,
                    DfsRoutedCall {
                        message,
                        attachment,
                    },
                    true,
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
            let request_count = requests.len();
            let mut outcomes = std::iter::repeat_with(|| None)
                .take(request_count)
                .collect::<Vec<Option<DfsRoutedSendOutcome>>>();
            let mut peer_requests = Vec::new();

            for (index, request) in requests.into_iter().enumerate() {
                let handled_by_served_lane =
                    request.target_node_id == self.local_node_id
                        || (self.return_node_id.as_ref().is_some_and(|return_node_id| {
                            &request.target_node_id == return_node_id
                        }) && dfs_network_message_can_return_to_served_caller(
                            &request.call.message.message,
                        ));
                if handled_by_served_lane {
                    let target_node_id = request.target_node_id;
                    let result = self
                        .send_routed_call_async(&target_node_id, request.call)
                        .await;
                    outcomes[index] = Some(DfsRoutedSendOutcome {
                        target_node_id,
                        result,
                    });
                } else {
                    peer_requests.push((index, request));
                }
            }

            if !peer_requests.is_empty() {
                let peer_indexes = peer_requests
                    .iter()
                    .map(|(index, request)| (*index, request.target_node_id.clone()))
                    .collect::<Vec<_>>();
                let peer_outcomes = self
                    .node_transport
                    .send_routed_calls_bounded_async(
                        peer_requests
                            .into_iter()
                            .map(|(_, request)| request)
                            .collect(),
                        max_inflight,
                    )
                    .await?;
                if peer_outcomes.len() != peer_indexes.len() {
                    return Err(NetworkError::Serialization {
                        detail: "DFS served async call fan-out returned the wrong outcome count"
                            .to_string(),
                    });
                }
                for ((index, expected_target), outcome) in
                    peer_indexes.into_iter().zip(peer_outcomes)
                {
                    if outcome.target_node_id != expected_target {
                        return Err(NetworkError::Serialization {
                            detail: format!(
                                "DFS served async call fan-out expected target {expected_target}, got {}",
                                outcome.target_node_id
                            ),
                        });
                    }
                    outcomes[index] = Some(outcome);
                }
            }

            outcomes
                .into_iter()
                .map(|outcome| {
                    outcome.ok_or_else(|| NetworkError::Serialization {
                        detail: "DFS served async call fan-out omitted an outcome".to_string(),
                    })
                })
                .collect()
        })
    }

    fn drain_incoming_messages_async<'a>(
        &'a mut self,
    ) -> DfsMessageTransportFuture<'a, Result<Vec<DfsRoutedMessage>, NetworkError>> {
        Box::pin(async move {
            let mut messages = self.inbound_messages.drain(..).collect::<Vec<_>>();
            messages.extend(self.node_transport.drain_incoming_messages_async().await?);
            Ok(messages)
        })
    }

    fn pending_incoming_count_async<'a>(
        &'a mut self,
    ) -> DfsMessageTransportFuture<'a, Result<Option<usize>, NetworkError>> {
        Box::pin(async move {
            let nested = self
                .node_transport
                .pending_incoming_count_async()
                .await?
                .unwrap_or_default();
            Ok(Some(self.inbound_messages.len().saturating_add(nested)))
        })
    }
}

impl DfsMessageTransport for DfsNodeServedMessageTransport {
    fn send_routed_call(
        &mut self,
        target_node_id: &NodeId,
        call: DfsRoutedCall,
    ) -> Result<(), NetworkError> {
        let DfsRoutedCall {
            message,
            attachment,
        } = call;
        if &message.target_node_id != target_node_id {
            return Err(NetworkError::FrameTargetMismatch {
                requested_target_node_id: target_node_id.clone(),
                frame_target_node_id: message.target_node_id.clone(),
            });
        }
        if target_node_id == &self.local_node_id {
            let message = crate::materialize_dfs_routed_call_sync(DfsRoutedCall {
                message,
                attachment,
            })?;
            self.inbound_messages.push_back(message);
            return Ok(());
        }
        if self
            .return_node_id
            .as_ref()
            .is_some_and(|return_node_id| target_node_id == return_node_id)
            && dfs_network_message_can_return_to_served_caller(&message.message)
        {
            if attachment.is_some() && self.outbound_attachment.is_some() {
                return Err(NetworkError::Serialization {
                    detail: "DFS served response attempted to return more than one attachment"
                        .to_string(),
                });
            }
            self.outbound_messages.push(message);
            if let Some(attachment) = attachment {
                self.outbound_attachment = Some(attachment);
            }
            return Ok(());
        }
        self.node_transport.send_routed_call_to_peer(
            target_node_id,
            DfsRoutedCall {
                message,
                attachment,
            },
            true,
        )
    }

    fn send_detached_routed_message(
        &mut self,
        target_node_id: &NodeId,
        message: DfsRoutedMessage,
    ) -> Result<(), NetworkError> {
        self.node_transport
            .send_routed_message_to_peer_detached(target_node_id, message, true)
    }

    fn send_routed_calls_bounded(
        &mut self,
        requests: Vec<DfsRoutedSend>,
        max_inflight: usize,
    ) -> Result<Vec<DfsRoutedSendOutcome>, NetworkError> {
        let request_count = requests.len();
        let mut outcomes = std::iter::repeat_with(|| None)
            .take(request_count)
            .collect::<Vec<Option<DfsRoutedSendOutcome>>>();
        let mut peer_requests = Vec::new();

        for (index, request) in requests.into_iter().enumerate() {
            let returns_to_caller = self
                .return_node_id
                .as_ref()
                .is_some_and(|return_node_id| &request.target_node_id == return_node_id)
                && dfs_network_message_can_return_to_served_caller(&request.call.message.message);
            if returns_to_caller {
                let target_node_id = request.target_node_id;
                let result = self.send_routed_call(&target_node_id, request.call);
                outcomes[index] = Some(DfsRoutedSendOutcome {
                    target_node_id,
                    result,
                });
            } else {
                peer_requests.push((index, request));
            }
        }

        if !peer_requests.is_empty() {
            let peer_indexes = peer_requests
                .iter()
                .map(|(index, request)| (*index, request.target_node_id.clone()))
                .collect::<Vec<_>>();
            let peer_outcomes = self.node_transport.send_routed_calls_bounded(
                peer_requests
                    .into_iter()
                    .map(|(_, request)| request)
                    .collect(),
                max_inflight,
            )?;
            if peer_outcomes.len() != peer_indexes.len() {
                return Err(NetworkError::Serialization {
                    detail: "DFS served call fan-out returned the wrong outcome count".to_string(),
                });
            }
            for ((index, expected_target), outcome) in peer_indexes.into_iter().zip(peer_outcomes) {
                if outcome.target_node_id != expected_target {
                    return Err(NetworkError::Serialization {
                        detail: format!(
                            "DFS served call fan-out expected target {expected_target}, got {}",
                            outcome.target_node_id
                        ),
                    });
                }
                outcomes[index] = Some(outcome);
            }
        }

        outcomes
            .into_iter()
            .map(|outcome| {
                outcome.ok_or_else(|| NetworkError::Serialization {
                    detail: "DFS served call fan-out omitted an outcome".to_string(),
                })
            })
            .collect()
    }

    fn send_metadata_journal_batch_direct(
        &mut self,
        target_node_id: &NodeId,
        command: &DfsMetadataJournalBatchCommand,
    ) -> Result<Option<DfsMetadataJournalBatchResponse>, NetworkError> {
        self.node_transport
            .send_metadata_journal_batch_direct(target_node_id, command)
    }

    fn send_metadata_journal_batches_direct(
        &mut self,
        requests: &[(NodeId, DfsMetadataJournalBatchCommand)],
    ) -> Result<Option<Vec<(NodeId, DfsMetadataJournalBatchResponse)>>, NetworkError> {
        self.node_transport
            .send_metadata_journal_batches_direct(requests)
    }

    fn send_metadata_journal_batches_direct_with_response_timeout(
        &mut self,
        requests: &[(NodeId, DfsMetadataJournalBatchCommand)],
        response_timeout: Duration,
    ) -> Result<Option<Vec<(NodeId, DfsMetadataJournalBatchResponse)>>, NetworkError> {
        self.node_transport
            .send_metadata_journal_batches_direct_with_response_timeout(requests, response_timeout)
    }

    fn send_grouped_metadata_journal_batches_direct_with_response_timeout(
        &mut self,
        groups: &[DfsMetadataJournalBatchDirectGroup],
        response_timeout: Duration,
    ) -> Result<Option<Vec<(NodeId, DfsMetadataJournalBatchResponse)>>, NetworkError> {
        self.node_transport
            .send_grouped_metadata_journal_batches_direct_with_response_timeout(
                groups,
                response_timeout,
            )
    }

    fn request_content_root_catalog_page_direct(
        &mut self,
        target_node_id: &NodeId,
        request: &DfsContentRootCatalogPageRequest,
    ) -> Result<Option<DfsContentRootCatalogPage>, NetworkError> {
        self.node_transport
            .request_content_root_catalog_page_direct(target_node_id, request)
    }

    fn drain_incoming_messages(&mut self) -> Result<Vec<DfsRoutedMessage>, NetworkError> {
        let mut messages = self.inbound_messages.drain(..).collect::<Vec<_>>();
        messages.extend(self.node_transport.drain_incoming_messages()?);
        Ok(messages)
    }

    fn flush_detached_sends(&mut self) -> Result<(), NetworkError> {
        self.node_transport.flush_detached_peer_sends()
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

    fn should_detach_completed_response(&self, target_node_id: &NodeId) -> bool {
        self.return_node_id
            .as_ref()
            .is_some_and(|return_node_id| return_node_id != target_node_id)
    }

    fn set_operation_deadline(&mut self, deadline: Option<Instant>) -> Option<Instant> {
        self.node_transport.set_operation_deadline(deadline)
    }

    fn operation_deadline(&self) -> Option<Instant> {
        self.node_transport.operation_deadline()
    }
}

fn dfs_network_message_is_served_call_response(message: &DfsNetworkMessage) -> bool {
    matches!(
        message,
        DfsNetworkMessage::ReadResponse(_)
            | DfsNetworkMessage::BulkReadResponse(_)
            | DfsNetworkMessage::WriteResponse(_)
            | DfsNetworkMessage::DistributedWriteResponse(_)
            | DfsNetworkMessage::StableWriteReconciliationResponse(_)
            | DfsNetworkMessage::NamespaceResponse(_)
            | DfsNetworkMessage::StableNamespaceReconciliationResponse(_)
            | DfsNetworkMessage::StageBlobResponse(_)
            | DfsNetworkMessage::CommitVersionResponse(_)
            | DfsNetworkMessage::RepairReplicaResponse(_)
            | DfsNetworkMessage::FetchBlobResponse(_)
            | DfsNetworkMessage::MetadataJournalBatchResponse(_)
            | DfsNetworkMessage::MetadataSnapshotInstallResponse(_)
            | DfsNetworkMessage::ContentRootCatalogPageResponse(_)
    )
}

#[cfg(test)]
fn stable_reconciliation_test_responses() -> [DfsNetworkMessage; 2] {
    [
        DfsNetworkMessage::StableWriteReconciliationResponse(
            crate::DfsStableWriteReconciliationResponse {
                request_id: "stable-write-reconciliation".to_string(),
                reconciliation: crate::DfsStableWriteOperationReconciliation::NotApplied,
            },
        ),
        DfsNetworkMessage::StableNamespaceReconciliationResponse(
            crate::DfsStableNamespaceReconciliationResponse {
                request_id: "stable-namespace-reconciliation".to_string(),
                reconciliation: crate::DfsStableNamespaceOperationReconciliation::NotApplied,
            },
        ),
    ]
}

fn dfs_network_message_can_return_to_served_caller(message: &DfsNetworkMessage) -> bool {
    dfs_network_message_is_served_call_response(message)
        || matches!(message, DfsNetworkMessage::FetchBlob(_))
}

impl NetworkedDfsService<DfsNodeServedMessageTransport> {
    pub fn refresh_message_peer_targets(&mut self, peer_node_ids: Vec<NodeId>) {
        self.transport.replace_peer_nodes(peer_node_ids);
    }

    pub fn prepare_message_deferred_work(&mut self) -> DfsNodeDetachedSendWorker {
        self.drain_deferred_routed_create_parent_touches();
        self.transport.node_transport.fork_detached_send_worker()
    }

    /// Move served-message follow-up work off this service instance before a
    /// daemon releases its metadata-heavy lane.
    ///
    /// Parent-directory touches are lane-local and the regular preparation
    /// path intentionally drains only a bounded batch. A detached daemon
    /// worker cannot recover those touches after dropping the service, so
    /// finish that finite local queue here before returning the lightweight
    /// worker whose retry outbox is shared across service generations.
    pub fn detach_message_deferred_work(&mut self) -> DfsNodeDetachedSendWorker {
        while !self.writes.deferred_routed_create_parent_touches.is_empty() {
            self.drain_deferred_routed_create_parent_touches();
        }
        self.transport.node_transport.fork_detached_send_worker()
    }

    pub fn message_deferred_work_pending(&self) -> Result<bool, DfsError> {
        if !self.writes.deferred_routed_create_parent_touches.is_empty() {
            return Ok(true);
        }
        self.transport
            .node_transport
            .fork_detached_send_worker()
            .has_pending()
    }

    pub fn drain_message_deferred_work(&mut self) -> Result<(), DfsError> {
        let mut worker = self.prepare_message_deferred_work();
        worker.drain()
    }

    pub fn process_message_command(
        &mut self,
        command: DfsMessageCommand,
    ) -> Result<DfsMessageProcessResult, DfsError> {
        self.process_message_command_report(command).outcome
    }

    /// Process one served message while retaining durable work independently
    /// from response construction and delivery.
    pub fn process_message_command_report(
        &mut self,
        command: DfsMessageCommand,
    ) -> DfsMessageProcessReport {
        self.process_message_command_report_with_preingested_blob(command, None)
    }

    pub fn process_message_command_report_with_preingested_blob(
        &mut self,
        command: DfsMessageCommand,
        preingested_blob: Option<ContentStoreQuarantinedBlob>,
    ) -> DfsMessageProcessReport {
        self.begin_message_committed_work();
        let outcome = self.process_message_command_inner(command, preingested_blob);
        let committed_work = self.take_message_committed_work();
        let committed_repair_records = self.take_message_committed_repair_queue_records();
        DfsMessageProcessReport {
            outcome,
            committed_work,
            committed_repair_records,
        }
    }

    fn process_message_command_inner(
        &mut self,
        command: DfsMessageCommand,
        preingested_blob: Option<ContentStoreQuarantinedBlob>,
    ) -> Result<DfsMessageProcessResult, DfsError> {
        let request_id = command.request_id;
        let allow_write_repair_progress =
            dfs_network_message_allows_write_repair_progress(&command.routed_message.message);
        let foreground_write_request_id =
            served_message_foreground_write_request_id(&command.routed_message.message);
        self.transport
            .begin_message(&command.routed_message)
            .map_err(DfsError::from_network)?;
        let initial = if let Some(blob) = preingested_blob {
            let routed = command.routed_message;
            if routed.target_node_id != self.node_id {
                Err(DfsError::Network {
                    message: format!(
                        "dfs message target {} does not match local node {}",
                        routed.target_node_id, self.node_id
                    ),
                })
            } else {
                match routed.message {
                    DfsNetworkMessage::StageBlob(command) => self
                        .handle_preingested_stage_blob(routed.source_node_id, command, blob)
                        .map(|event| vec![event]),
                    DfsNetworkMessage::StageRetainedBlob(command) => self
                        .handle_stage_retained_blob(routed.source_node_id, command, Some(blob))
                        .map(|event| vec![event]),
                    DfsNetworkMessage::RepairRetainedBlob(command) => self
                        .handle_repair_retained_blob(routed.source_node_id, command, Some(blob))
                        .map(|event| vec![event]),
                    DfsNetworkMessage::DistributedWriteRequest(mut command) => {
                        let admission = command.require_publication_mode(false);
                        command.exclusive_create = false;
                        admission
                            .and_then(|()| {
                                self.handle_preingested_distributed_write_request(
                                    routed.source_node_id,
                                    command,
                                    blob,
                                )
                            })
                            .map(|event| vec![event])
                    }
                    DfsNetworkMessage::ExclusiveCreateRequest(mut command) => {
                        let admission = command.require_publication_mode(false);
                        command.exclusive_create = true;
                        admission
                            .and_then(|()| {
                                self.handle_preingested_distributed_write_request(
                                    routed.source_node_id,
                                    command,
                                    blob,
                                )
                            })
                            .map(|event| vec![event])
                    }
                    DfsNetworkMessage::RetainedWriteRequest(command) => command
                        .require_publication_mode(true)
                        .and_then(|()| {
                            self.handle_preingested_distributed_write_request(
                                routed.source_node_id,
                                command,
                                blob,
                            )
                        })
                        .map(|event| vec![event]),
                    other => Err(DfsError::Distributed {
                        message: format!(
                            "preingested DFS attachment is not supported for {other:?}"
                        ),
                    }),
                }
            }
        } else {
            self.process_served_routed_messages(vec![command.routed_message])
        };
        let mut events = match initial {
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
                let (outbound_messages, attachment) =
                    self.transport.finish_message_with_attachment();
                return Ok(DfsMessageProcessResult {
                    response: DfsMessageResponse {
                        request_id,
                        outbound_messages,
                    },
                    attachment,
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
        self.writes
            .pending_remote_write_responses
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
        | DfsNetworkMessage::ExclusiveCreateRequest(command)
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
        attachment,
        events,
    } = service.process_message_command(command)?;
    if let Some(attachment) = attachment {
        let max_bytes = usize::try_from(attachment.descriptor.total_size_bytes).map_err(|_| {
            DfsError::Distributed {
                message: "DFS test response attachment does not fit this platform address space"
                    .to_string(),
            }
        })?;
        let bytes =
            collect_dfs_attachment_source_sync(attachment.source, max_bytes).map_err(|error| {
                DfsError::Network {
                    message: error.to_string(),
                }
            })?;
        response = crate::body_rpc::attach_dfs_message_response_attachment(
            crate::DfsAttachmentMessageResponse {
                response,
                attachment: attachment.descriptor,
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
