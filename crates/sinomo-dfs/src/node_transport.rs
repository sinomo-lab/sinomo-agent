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
use sinomo_core::network::ObjectRpcBodySource;
#[cfg(test)]
use sinomo_core::network::ObjectRpcRequest;
use sinomo_core::network::{
    NetworkError, ObjectRpcCall, ObjectRpcIncomingBody, ObjectRpcReplayableCall,
    ObjectRpcResponse, ObjectRpcResult, SinomoObjectCodec,
};
use sinomo_core::node::NodeId;
use tracing::Instrument;

use crate::{
    AsyncDfsMessageTransport, ContentStoreQuarantinedBlob, DFS_DATA_PLANE_MESSAGE_DRAIN_MAX_PASSES,
    DfsBlobTransferResult, DfsContentRootCatalogPage, DfsContentRootCatalogPageRequest,
    DfsDirectReadClientResponse, DfsDirectReadRequest, DfsError, DfsMessageCommand,
    DfsMessageProcessReport, DfsMessageProcessResult, DfsMessageResponse, DfsMessageTransport,
    DfsMessageTransportFuture, DfsMetadataAuthorityFanout, DfsMetadataAuthorityFanoutEvent,
    DfsMetadataAuthorityFanoutSignal, DfsMetadataAuthorityFanoutWorkers,
    DfsMetadataJournalBatchCommand, DfsMetadataJournalBatchDirectGroup,
    DfsMetadataJournalBatchResponse, DfsNetworkMessage, DfsOutboundAttachment, DfsReadResponse,
    DfsRoutedCall, DfsRoutedMessage, DfsRoutedSend, DfsRoutedSendOutcome, DfsTransportIdentity,
    NetworkedDfsService, NetworkedDfsServiceEvent,
    decode_dfs_message_rpc_response, decode_dfs_metadata_journal_batch_rpc_response,
    dfs_direct_read_request_from_read_request, dfs_direct_read_response_from_rpc_result,
    dfs_direct_read_rpc_call, dfs_message_is_read_only_request, dfs_message_rpc_call,
    dfs_message_rpc_request, dfs_metadata_journal_batch_replayable_rpc_call,
    dfs_metadata_journal_batch_rpc_call,
    dfs_perf_trace, dfs_perf_trace_enabled, dfs_rejection_is_commit_content_repair_pending,
    dfs_rejection_is_replica_not_desired, retained_networked_dfs_service_events,
};
#[cfg(test)]
use crate::{DfsControlPlaneKvClient, LocalContentStore, open_cluster_aware_dfs_metadata_backend};

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
/// cap and then needlessly fall back to cluster KV. Blocking authority calls,
/// direct and routed, also share this many detached threads per transport; a
/// call that finds no free thread uses its ordinary routed send.
const DFS_METADATA_AUTHORITY_DIRECT_MAX_INFLIGHT: usize = 8;
/// Keep one slow checkpoint replica from holding a served mutation lane.
const DFS_METADATA_CHECKPOINT_DIRECT_FANOUT_BUDGET: Duration = Duration::from_millis(250);
/// Whole-call budget when authority-probe commands use the best-effort batch
/// API. Authority rounds start their calls with
/// `start_metadata_authority_fanout` instead, where every call carries the
/// operation deadline as its network deadline.
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

/// Per-call producer correlation DATA. Private fields prevent an injectable
/// peer from manufacturing the producer's next attempt; this is NOT proof.
#[derive(Debug)]
pub struct DfsMetadataPrefixProbeAttempt {
    id: String,
    peer: NodeId,
    command: DfsMetadataJournalBatchCommand,
    deadline: Instant,
}

impl DfsMetadataPrefixProbeAttempt {
    pub fn attempt_id(&self) -> &str {
        &self.id
    }
    pub fn peer(&self) -> &NodeId {
        &self.peer
    }
    pub fn command(&self) -> &DfsMetadataJournalBatchCommand {
        &self.command
    }
    pub fn deadline(&self) -> Instant {
        self.deadline
    }
}

#[derive(Debug)]
pub enum DfsMetadataPrefixEvidence {
    NoNativeProof,
    Native(crate::runtime_host::OwnedNativeMetadataPrefixReturn),
}

#[derive(Debug)]
pub struct DfsMetadataPrefixProbeReturn {
    pub response: DfsMetadataJournalBatchResponse,
    pub evidence: DfsMetadataPrefixEvidence,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DfsNativePrefixRoundPeerStatus {
    Unattempted,
    NativeCaptured,
    NoNativeProof,
    Failed,
    NativeBindingRejected,
}

/// One fixed slot in one round. Attempted means the R63 producer was invoked,
/// not that a wire effect is known to have happened. Unknown stays unknown.
#[cfg(test)]
#[derive(Debug)]
pub(crate) struct DfsNativePrefixRoundPeer {
    peer: NodeId,
    command: DfsMetadataJournalBatchCommand,
    status: DfsNativePrefixRoundPeerStatus,
    attempted: bool,
    response: Option<DfsMetadataJournalBatchResponse>,
    native_return: Option<crate::runtime_host::OwnedNativeMetadataPrefixReturn>,
    error: Option<NetworkError>,
    unattempted_reason: Option<String>,
}

#[cfg(test)]
impl DfsNativePrefixRoundPeer {
    pub(crate) fn peer(&self) -> &NodeId {
        &self.peer
    }
    pub(crate) fn command(&self) -> &DfsMetadataJournalBatchCommand {
        &self.command
    }
    pub(crate) fn status(&self) -> DfsNativePrefixRoundPeerStatus {
        self.status
    }
    pub(crate) fn attempted(&self) -> bool {
        self.attempted
    }
    pub(crate) fn response(&self) -> Option<&DfsMetadataJournalBatchResponse> {
        self.response.as_ref()
    }
    pub(crate) fn native_return(
        &self,
    ) -> Option<&crate::runtime_host::OwnedNativeMetadataPrefixReturn> {
        self.native_return.as_ref()
    }
    pub(crate) fn error(&self) -> Option<&NetworkError> {
        self.error.as_ref()
    }
    pub(crate) fn delivery_state(&self) -> Option<sinomo_core::network::NetworkDeliveryState> {
        self.error.as_ref().and_then(NetworkError::delivery_state)
    }
    pub(crate) fn unattempted_reason(&self) -> Option<&str> {
        self.unattempted_reason.as_deref()
    }
}

/// The common LOCAL source DATA observed by both genuine native returns. This
/// is neither an invented self acknowledgement nor current serving authority.
#[cfg(test)]
#[derive(Debug)]
pub(crate) struct DfsNativePrefixRoundSourceObservation {
    assignment: crate::DfsMetadataShardConfig,
    prefix: crate::DfsMetadataPrefixProof,
    physical: crate::runtime_host::NativeMetadataPrefixPhysicalOwner,
}

#[cfg(test)]
impl DfsNativePrefixRoundSourceObservation {
    pub(crate) fn assignment(&self) -> &crate::DfsMetadataShardConfig {
        &self.assignment
    }
    pub(crate) fn prefix(&self) -> &crate::DfsMetadataPrefixProof {
        &self.prefix
    }
    pub(crate) fn physical_owner(&self) -> &crate::runtime_host::NativeMetadataPrefixPhysicalOwner {
        &self.physical
    }
    pub(crate) fn source_node_id(&self) -> &NodeId {
        &self.assignment.primary_node_id
    }
}

/// Private, call-local complete/incomplete observation. No Clone, serde, raw
/// constructor or merge/adopt operation; retained values own no native lease.
#[cfg(test)]
#[derive(Debug)]
pub(crate) struct OwnedNativeMetadataPrefixRound {
    round_id: String,
    target: crate::DfsMetadataQuorumSyncTarget,
    deadline: Instant,
    peers: Vec<DfsNativePrefixRoundPeer>,
    common_source: Option<DfsNativePrefixRoundSourceObservation>,
    finalization_error: Option<NetworkError>,
}

#[cfg(test)]
impl OwnedNativeMetadataPrefixRound {
    pub(crate) fn round_id(&self) -> &str {
        &self.round_id
    }
    pub(crate) fn target(&self) -> &crate::DfsMetadataQuorumSyncTarget {
        &self.target
    }
    pub(crate) fn deadline(&self) -> Instant {
        self.deadline
    }
    pub(crate) fn is_complete(&self) -> bool {
        self.common_source.is_some() && self.finalization_error.is_none()
    }
    pub(crate) fn peer_outcomes(&self) -> &[DfsNativePrefixRoundPeer] {
        &self.peers
    }
    pub(crate) fn common_source_observation(
        &self,
    ) -> Option<&DfsNativePrefixRoundSourceObservation> {
        self.common_source.as_ref()
    }
    /// Local round eligibility error, separate from preserved peer errors.
    pub(crate) fn finalization_error(&self) -> Option<&NetworkError> {
        self.finalization_error.as_ref()
    }
}

/// Size-independent DFS peer RPC. Ordinary encoded values and explicit
/// attachments share this one transport contract.
pub trait DfsPeerTransport: Send + Sync {
    /// Hosted adapters may connect this transport to their existing runtime's
    /// local response-delivery owner. Standalone adapters remain independent.
    fn response_delivery_owner(&self) -> Option<Arc<crate::DfsRoutedResponseDelivery>> {
        None
    }

    /// A colocated registered Main may execute its own authenticated path
    /// write through its resident data-plane handler. Other clients use RPC.
    fn call_local_owner_write(
        &self,
        _command: &DfsMessageCommand,
        _options: DfsObjectRpcSendOptions,
    ) -> Option<Result<DfsMessageResponse, DfsPeerTransportError>> {
        None
    }

    fn call_object_rpc(
        &self,
        target_node_id: &NodeId,
        call: ObjectRpcCall,
        options: DfsObjectRpcSendOptions,
    ) -> Result<ObjectRpcResult, DfsPeerTransportError>;

    /// Generic/injectable peers retain response DATA only. The native runtime
    /// override owns its ONE actual call; no matcher/trace may construct proof.
    fn call_metadata_prefix_probe(
        &self,
        attempt: &DfsMetadataPrefixProbeAttempt,
        mut options: DfsObjectRpcSendOptions,
    ) -> Result<DfsMetadataPrefixProbeReturn, DfsPeerTransportError> {
        validate_ordinary_metadata_prefix_command(attempt.command())?;
        let call = crate::dfs_metadata_journal_batch_rpc_call(attempt.command())
            .map_err(DfsPeerTransportError::from_network)?;
        let remaining = attempt
            .deadline()
            .checked_duration_since(Instant::now())
            .filter(|left| !left.is_zero())
            .ok_or_else(|| {
                DfsPeerTransportError::new("metadata prefix attempt expired before dispatch")
            })?;
        options.overall_timeout = Some(
            options
                .overall_timeout
                .map_or(remaining, |old| old.min(remaining)),
        );
        let result = self.call_object_rpc(attempt.peer(), call, options)?;
        let response = collect_metadata_prefix_probe_return(result, attempt)?;
        Ok(DfsMetadataPrefixProbeReturn {
            response,
            evidence: DfsMetadataPrefixEvidence::NoNativeProof,
        })
    }
}

pub(crate) fn validate_ordinary_metadata_prefix_command(
    command: &DfsMetadataJournalBatchCommand,
) -> Result<(), DfsPeerTransportError> {
    let probe = command.prefix_probe.as_ref().ok_or_else(|| {
        DfsPeerTransportError::new("native prefix capture requires an ordinary PREFIX probe")
    })?;
    if !command.verify_prefix_only
        || !command.batch.journal_frames.is_empty()
        || command.authority_probe.is_some()
        || command.durable_replication.is_some()
        || probe.primary_handoff_id.is_some()
        || command.request_id.is_empty()
        || command.source_node_id.is_none()
        || command.file_system_id.is_none()
        || command.authority_epoch.is_none()
        || probe.file_system_id != command.file_system_id
        || probe.authority_epoch != command.authority_epoch
        || probe.shard_id != command.batch.shard_id
        || probe.journal_sequence != command.batch.next_after_sequence
        || probe.barrier_id.is_empty()
        || probe.state_fingerprint.is_empty()
    {
        return Err(DfsPeerTransportError::new(
            "native prefix capture refuses nonordinary, incomplete, handoff, custody, authority or backfill input",
        ));
    }
    Ok(())
}

/// Pure response decoding/correlation returns DATA only, never provenance.
pub(crate) fn collect_metadata_prefix_probe_return(
    result: ObjectRpcResult,
    attempt: &DfsMetadataPrefixProbeAttempt,
) -> Result<DfsMetadataJournalBatchResponse, DfsPeerTransportError> {
    let streamed = result.has_streamed_value();
    let (mut response, body) = result.into_parts();
    if response.route != crate::DFS_METADATA_JOURNAL_BATCH_ROUTE || (!streamed && !body.is_empty())
    {
        return Err(DfsPeerTransportError::new(
            "metadata prefix return has a wrong route or semantic attachment",
        ));
    }
    response
        .error_detail()
        .map_err(DfsPeerTransportError::from_network)?;
    if streamed {
        let deadline = attempt.deadline();
        let collect = async move {
            tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline),
                body.collect_to_vec(64 * 1024),
            )
            .await
            .map_err(|_| {
                DfsPeerTransportError::new("metadata prefix return exceeded its original deadline")
            })?
            .map_err(DfsPeerTransportError::from_network)
        };
        response.body = if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            if !matches!(
                runtime.runtime_flavor(),
                tokio::runtime::RuntimeFlavor::MultiThread
            ) {
                return Err(DfsPeerTransportError::new(
                    "metadata prefix sync return requires a blocking-capable runtime",
                ));
            }
            tokio::task::block_in_place(|| runtime.block_on(collect))?
        } else {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| DfsPeerTransportError::new(error.to_string()))?;
            let value = runtime.block_on(collect);
            runtime.shutdown_timeout(Duration::from_millis(250));
            value?
        };
    }
    if response.body.len() > 64 * 1024 || Instant::now() >= attempt.deadline() {
        return Err(DfsPeerTransportError::new(
            "metadata prefix return exceeds its value/deadline bound",
        ));
    }
    let actual = decode_dfs_metadata_journal_batch_rpc_response(&response)
        .map_err(DfsPeerTransportError::from_network)?;
    if actual.request_id != attempt.command.request_id
        || actual.shard_id != attempt.command.batch.shard_id
        || actual.next_after_sequence != attempt.command.batch.next_after_sequence
    {
        return Err(DfsPeerTransportError::new(
            "metadata prefix return differs from its actual request/shard/sequence",
        ));
    }
    Ok(actual)
}

#[cfg(test)]
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
        #[cfg(any(test, feature = "test-support"))]
        let native_context = crate::native_shard_trace::rpc_attempt_context();
        Box::pin(async move {
            let call = dfs_message_rpc_call(command)
                .map(|call| dfs_rpc_call_with_request_budget(call, options))
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
            #[cfg(any(test, feature = "test-support"))]
            let mut native_attempt =
                native_context.and_then(|context| context.begin(target_node_id, command));
            let response = peer_transport
                .call_object_rpc_async(
                    target_node_id,
                    call,
                    options.with_observe_connectivity(!dfs_message_is_read_only_request(command)),
                )
                .await;
            let result = match response {
                Ok(response) => dfs_message_response_from_rpc_result(response).await,
                Err(error) => Err(error),
            };
            #[cfg(any(test, feature = "test-support"))]
            if let Some(attempt) = native_attempt.as_mut() {
                attempt.returned(&result);
            }
            result
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

#[cfg(test)]
std::thread_local! {
    /// A budget this thread's sends stamp while their own calls stay
    /// unbounded: the serving node then reaches the budget first, so a test
    /// reads the answer it gives at the budget.
    static DFS_TEST_STAMPED_REQUEST_BUDGET: std::cell::Cell<Option<Duration>> =
        const { std::cell::Cell::new(None) };
}

#[cfg(test)]
pub(crate) fn stamp_request_budget_for_test(budget: Option<Duration>) {
    DFS_TEST_STAMPED_REQUEST_BUDGET.with(|stamped| stamped.set(budget));
}

/// Stamps the send's whole-call deadline, which is the requester's remaining
/// budget, on the request so the serving node ends its wait with the requester.
fn dfs_rpc_call_with_request_budget(
    mut call: ObjectRpcCall,
    options: DfsObjectRpcSendOptions,
) -> ObjectRpcCall {
    #[cfg(test)]
    if let Some(remaining) = DFS_TEST_STAMPED_REQUEST_BUDGET.with(std::cell::Cell::get) {
        call.request = crate::routes::with_dfs_request_budget(call.request, remaining);
        return call;
    }
    if let Some(remaining) = options.overall_timeout {
        call.request = crate::routes::with_dfs_request_budget(call.request, remaining);
    }
    call
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
    let expected = DfsMessageCommand {
        request_id: format!("dfs_message_{}", message.frame_id),
        routed_message: message,
    };
    let command = expected.clone();
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
            return Ok((expected, rpc_call));
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
    Ok((expected, rpc_call))
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
    collect_dfs_response_sync(body.collect_to_vec(max_bytes))
}

// Receiving the head does not finish a streamed call. Keep the same mount
// deadline until the bytes of a value or direct read have arrived.
fn collect_dfs_response_sync<T>(
    receive: impl Future<Output = Result<T, NetworkError>>,
) -> Result<T, DfsPeerTransportError> {
    let deadline = crate::MountedSifsCall::deadline();
    let collect = async move {
        match deadline {
            Some(deadline) => tokio::time::timeout_at(deadline.into(), receive)
                .await
                .map_err(|_| {
                    DfsPeerTransportError::with_delivery_state(
                        "mounted filesystem response body exceeded its deadline",
                        Some(sinomo_core::network::NetworkDeliveryState::PossiblyApplied),
                    )
                })?,
            None => receive.await,
        }
        .map_err(|source| DfsPeerTransportError::new(source.to_string()))
    };
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        if matches!(
            handle.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            return tokio::task::block_in_place(|| handle.block_on(collect));
        }
        return Err(DfsPeerTransportError::new(
            "sync DFS response body collector cannot run inside a current-thread Tokio runtime",
        ));
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
    let result = runtime.block_on(collect);
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
        if &command.routed_message.source_node_id == target_node_id
            && &command.routed_message.target_node_id == target_node_id
            && matches!(&command.routed_message.message, DfsNetworkMessage::NamespaceRequest(ns)
                if &ns.requesting_node_id == target_node_id && matches!(ns.operation, crate::DfsNamespaceOperation::WriteFile {..}))
            && let Some(result) = peer_transport.call_local_owner_write(command, options)
        {
            return result;
        }
        let call = dfs_message_rpc_call(command)
            .map(|call| dfs_rpc_call_with_request_budget(call, options))
            .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
        #[cfg(any(test, feature = "test-support"))]
        let mut native_attempt =
            crate::native_shard_trace::begin_rpc_attempt(target_node_id, command);
        let result = peer_transport
            .call_object_rpc(
                target_node_id,
                call,
                options.with_observe_connectivity(!dfs_message_is_read_only_request(command)),
            )
            .and_then(dfs_message_response_from_rpc_result_sync);
        #[cfg(any(test, feature = "test-support"))]
        if let Some(attempt) = native_attempt.as_mut() {
            attempt.returned(&result);
        }
        result
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
    collect_dfs_response_sync(response.collect_to_read_response())
}

#[derive(Clone)]
struct DeferredDfsPeerSend {
    // A returned repair completion has consumed its attachment and deliberately
    // carries no outbound sender. Only its admitted local handler may resume it.
    sender: Option<DfsMessageSender>,
    options: DfsObjectRpcSendOptions,
    operation_deadline: Option<Instant>,
    target_node_id: NodeId,
    routed_message: DfsRoutedMessage,
    promotion_sequence: Option<u64>,
    // Exact authenticated returns whose local completion failed. This is DATA,
    // not a receipt or authority capability: the admitted handler revalidates
    // content custody and current authority on every attempt. The shared outbox
    // retains it across worker replacement; durable intent recovery owns restart.
    pending_local_responses: VecDeque<DfsRoutedMessage>,
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
    fn eligible_for_target_policy(&self, deferred_target_node_ids: &BTreeSet<NodeId>) -> bool {
        // Required content fetches and completed write replies are protocol
        // progress for an existing operation, even in Main's detached owner.
        // Advisory maintenance exclusion must not park them. The ordinary RPC
        // still enforces its deadline, peer identity and payload checks.
        matches!(
            &self.routed_message.message,
            DfsNetworkMessage::FetchBlob(_)
        ) || self.is_replay_safe_completed_write_response()
            || !deferred_target_node_ids.contains(&self.target_node_id)
    }

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

pub(crate) fn all_background_replica_response_identity_matches(
    local_node_id: &NodeId,
    original: &DfsRoutedMessage,
    message: &DfsRoutedMessage,
) -> bool {
    let DfsNetworkMessage::RepairReplicaResponse(response) = &message.message else {
        return false;
    };
    // A background repair and a retained version's repair both return a
    // repair reply for the exact command identity.
    let (request_id, intent_id, file_system_id, object_id, version, content_hash) =
        match &original.message {
            DfsNetworkMessage::RepairReplica(command) if command.background_repair => (
                &command.request_id,
                &command.intent_id,
                &command.file_system_id,
                &command.object_id,
                &command.version,
                &command.content_hash,
            ),
            DfsNetworkMessage::RepairRetainedBlob(command) => (
                &command.request_id,
                &command.intent_id,
                &command.file_system_id,
                &command.object_id,
                &command.version,
                &command.content_hash,
            ),
            _ => return false,
        };
    original.source_node_id == *local_node_id
        && message.source_node_id == original.target_node_id
        && message.target_node_id == *local_node_id
        && response.replica_node_id == original.target_node_id
        && response.request_id == *request_id
        && response.intent_id == *intent_id
        && response.file_system_id == *file_system_id
        && response.object_id == *object_id
        && response.version == *version
        && match &response.result {
            DfsBlobTransferResult::Durable { content_hash: returned }
            | DfsBlobTransferResult::Committed { content_hash: returned } => {
                returned == content_hash
            }
            DfsBlobTransferResult::Rejected { .. } => true,
        }
}

#[cfg(test)]
#[derive(Clone, Debug)]
pub(crate) struct DfsTestMetadataDirectRpcObservation {
    pub target_node_id: NodeId,
    pub request_id: String,
    pub response: DfsMetadataJournalBatchResponse,
}

#[cfg(test)]
#[derive(Clone, Debug, Default)]
pub(crate) struct DfsTestMetadataDirectRpcProbe {
    records: Arc<std::sync::Mutex<Vec<DfsTestMetadataDirectRpcObservation>>>,
    dropped: Arc<std::sync::atomic::AtomicUsize>,
}

#[cfg(test)]
impl DfsTestMetadataDirectRpcProbe {
    fn record(&self, record: DfsTestMetadataDirectRpcObservation) {
        if let Ok(mut records) = self.records.lock() {
            if records.len() < 4096 {
                records.push(record);
                return;
            }
        }
        self.dropped
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

struct DfsPreparedMetadataJournalBatchRpc {
    #[cfg(any(test, feature = "test-support"))]
    native_attempt_context: Option<crate::native_shard_trace::NativeRpcAttemptContext>,
    #[cfg(any(test, feature = "test-support"))]
    native_command: Option<Arc<DfsMetadataJournalBatchCommand>>,
    #[cfg(test)]
    test_probe: Option<DfsTestMetadataDirectRpcProbe>,
    target_node_id: NodeId,
    request_id: String,
    shard_id: crate::DfsMetadataShardId,
    next_after_sequence: u64,
    authority_fanout: bool,
    replayable_call: Arc<ObjectRpcReplayableCall>,
    upload_progress: Option<sinomo_core::network::ObjectRpcBodyProgress>,
    options: DfsObjectRpcSendOptions,
}

struct DfsBorrowedMetadataJournalBatchDirectGroup<'a> {
    target_node_ids: Vec<&'a NodeId>,
    command: &'a DfsMetadataJournalBatchCommand,
}

impl DfsPreparedMetadataJournalBatchRpc {
    fn trace_context(&self) -> DfsDirectMetadataJournalTraceContext {
        dfs_perf_trace_enabled().then(|| {
            (
                Instant::now(),
                self.target_node_id.clone(),
                self.shard_id.clone(),
                self.next_after_sequence,
                self.authority_fanout,
            )
        })
    }

    #[cfg(any(test, feature = "test-support"))]
    fn begin_native_attempt(
        &self,
        route: &str,
    ) -> Option<crate::native_shard_trace::NativeRpcAttemptObservation> {
        self.native_attempt_context.clone()?.begin_direct_journal(
            &self.target_node_id,
            route,
            Arc::clone(self.native_command.as_ref()?),
        )
    }
    fn call(&self) -> Result<ObjectRpcCall, NetworkError> {
        Ok(match &self.upload_progress {
            Some(progress) => self.replayable_call.call_with_progress(progress),
            None => self.replayable_call.call(),
        })
    }

    fn finish(
        self,
        rpc_response: Result<ObjectRpcResult, DfsPeerTransportError>,
        #[cfg(any(test, feature = "test-support"))] mut native_attempt: Option<
            crate::native_shard_trace::NativeRpcAttemptObservation,
        >,
    ) -> Result<(NodeId, DfsMetadataJournalBatchResponse), NetworkError> {
        let rpc_response = rpc_response.map_err(|source| {
            #[cfg(any(test, feature = "test-support"))]
            if let Some(attempt) = native_attempt.as_mut() {
                attempt.returned_direct_journal(
                    None,
                    Some(source.to_string()),
                    source.delivery_state(),
                );
            }
            NetworkError::Io {
                operation: format!(
                    "send direct DFS metadata journal batch to {}",
                    self.target_node_id
                ),
                detail: source.to_string(),
            }
        })?;
        let rpc_response = value_response_from_rpc_result(rpc_response).map_err(|source| {
            #[cfg(any(test, feature = "test-support"))]
            if let Some(attempt) = native_attempt.as_mut() {
                attempt.returned_direct_journal(
                    None,
                    Some(source.to_string()),
                    source.delivery_state(),
                );
            }
            NetworkError::Io {
                operation: format!(
                    "decode direct DFS metadata journal response from {}",
                    self.target_node_id
                ),
                detail: source.to_string(),
            }
        })?;
        let response = decode_dfs_metadata_journal_batch_rpc_response(&rpc_response);
        #[cfg(any(test, feature = "test-support"))]
        if let Err(source) = &response
            && let Some(attempt) = native_attempt.as_mut()
        {
            attempt.returned_direct_journal(
                None,
                Some(source.to_string()),
                source.delivery_state(),
            );
        }
        let response = response?;
        #[cfg(any(test, feature = "test-support"))]
        if let Some(attempt) = native_attempt.as_mut() {
            attempt.returned_direct_journal(Some(&response), None, None);
        }
        #[cfg(test)]
        if let Some(probe) = &self.test_probe {
            // Observe the actual decoded native response and the actual command
            // identity. No request header, stream or protocol result is changed.
            probe.record(DfsTestMetadataDirectRpcObservation {
                target_node_id: self.target_node_id.clone(),
                request_id: self.request_id.clone(),
                response: response.clone(),
            });
        }
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

#[cfg(test)]
impl DfsNodeTransport {
    /// Let go of this transport's response inbox, as an owner that is no
    /// longer attached: its registrations stay, and Main keeps the answers
    /// for them until this transport registers them again on its new inbox.
    pub(crate) fn detach_response_owner_for_test(&mut self) {
        if self.response_inbox.is_some() {
            self.response_inbox =
                Some(crate::response_delivery::DfsRoutedResponseInboxOwner::new());
        }
    }
}

pub struct DfsNodeTransport {
    #[cfg(test)]
    test_metadata_direct_probe: Option<DfsTestMetadataDirectRpcProbe>,
    #[cfg(test)]
    test_routed_call_attempts: Option<Arc<StdAtomicUsize>>,
    #[cfg(test)]
    test_metadata_authority_routed_sends: Option<Arc<std::sync::Mutex<Vec<(NodeId, usize)>>>>,
    local_node_id: NodeId,
    known_remote_nodes: BTreeSet<NodeId>,
    incoming_messages: VecDeque<DfsRoutedMessage>,
    response_delivery: Option<Arc<crate::DfsRoutedResponseDelivery>>,
    response_inbox: Option<crate::response_delivery::DfsRoutedResponseInboxOwner>,
    response_registrations:
        BTreeMap<String, Vec<crate::response_delivery::DfsRoutedResponseRegistration>>,
    detached_send_state: DfsNodeDetachedSendState,
    connect_timeout: Duration,
    sender: Option<DfsMessageSender>,
    async_sender: Option<AsyncDfsMessageSender>,
    local_namespace_dispatch: LocalNamespaceDispatch,
    served_message: Option<Box<DfsServedMessageState>>,
    direct_read_sender: Option<DfsDirectReadSender>,
    async_direct_read_sender: Option<AsyncDfsDirectReadSender>,
    peer_transport: Option<Arc<dyn DfsPeerTransport>>,
    async_peer_transport: Option<Arc<dyn AsyncDfsPeerTransport>>,
    blocking_metadata_fanout_workers: Arc<StdAtomicUsize>,
    next_content_root_catalog_frame_id: Arc<AtomicU64>,
    operation_deadline: Option<Instant>,
    incoming_wait_interrupt: Arc<DfsIncomingWaitInterrupt>,
    /// A hint to this node's Main is on its way. One at a time: the next
    /// attempt of its sender posts the next one.
    hint_to_main_in_flight: Arc<AtomicBool>,
    /// The cancellation of the caller this transport sends for.
    send_cancellation: Option<crate::DfsCancellationFlag>,
    #[cfg(any(test, feature = "test-support"))]
    test_drain_counter: Option<Arc<std::sync::Mutex<usize>>>,
}

std::thread_local! {
    /// The cancellation of the caller whose blocking send runs on this thread.
    static DFS_SEND_CANCELLATION: std::cell::RefCell<Option<crate::DfsCancellationFlag>> =
        const { std::cell::RefCell::new(None) };
}

/// Names, for the blocking sends this thread makes while the scope lives, the
/// caller whose departure must end them.
pub(crate) struct DfsSendCancellationScope(Option<crate::DfsCancellationFlag>);

impl DfsSendCancellationScope {
    pub(crate) fn enter(cancellation: Option<crate::DfsCancellationFlag>) -> Self {
        Self(DFS_SEND_CANCELLATION.with(|current| current.replace(cancellation)))
    }
}

impl Drop for DfsSendCancellationScope {
    fn drop(&mut self) {
        let previous = self.0.take();
        DFS_SEND_CANCELLATION.with(|current| *current.borrow_mut() = previous);
    }
}

/// The cancellation a blocking send on this thread watches, if its caller
/// installed one.
pub(crate) fn current_dfs_send_cancellation() -> Option<crate::DfsCancellationFlag> {
    DFS_SEND_CANCELLATION.with(|current| current.borrow().clone())
}

/// Lets a waker end a [`DfsMessageTransport::wait_for_incoming`] that has no
/// arrival signal of its own. One interruption is retained until a wait
/// consumes it, so a wake raised just before the wait starts is not lost.
#[derive(Debug, Default)]
struct DfsIncomingWaitInterrupt {
    interrupted: std::sync::Mutex<bool>,
    ready: std::sync::Condvar,
    /// Wakes an asynchronous waiter on the same interrupt.
    ready_async: tokio::sync::Notify,
}

impl DfsIncomingWaitInterrupt {
    fn wait(&self, timeout: Duration) {
        let interrupted = self
            .interrupted
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (mut interrupted, _) = self
            .ready
            .wait_timeout_while(interrupted, timeout, |interrupted| !*interrupted)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *interrupted = false;
    }

    /// The asynchronous form of [`Self::wait`]: an interrupt ends it at once.
    async fn wait_async(&self, timeout: Duration) {
        let interrupted = self.ready_async.notified();
        tokio::pin!(interrupted);
        // Register before the check, so an interrupt between the two is seen.
        interrupted.as_mut().enable();
        let take = || {
            std::mem::take(
                &mut *self
                    .interrupted
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            )
        };
        if take() {
            return;
        }
        tokio::select! {
            () = interrupted => {}
            () = tokio::time::sleep(timeout) => {}
        }
        take();
    }

    fn interrupt(&self) {
        *self
            .interrupted
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        self.ready.notify_all();
        self.ready_async.notify_waiters();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LocalNamespaceDispatch {
    InlineCore,
    RegisteredMain,
}

/// Original producer facts for this SAME owned call, captured before admission
/// is released. They are independent of the new claim's UNKNOWN queue outcome.
#[derive(Debug)]
struct DfsCheckpointedReplicaCall {
    request: DfsRoutedSend,
}

/// The need and request id of an executor send the detached worker carries:
/// a background repair, a retained version's repair or a repair fetch. A
/// fetch names its source, so its need is found by the attempt's request id.
fn background_repair_attempt(
    local_node_id: &NodeId,
    message: &DfsRoutedMessage,
) -> Option<(crate::metadata_tables::DfsRepairNeedKey, String)> {
    match &message.message {
        DfsNetworkMessage::RepairReplica(command) => Some((
            (
                command.object_id.clone(),
                command.version.clone(),
                message.target_node_id.clone(),
            ),
            command.request_id.clone(),
        )),
        DfsNetworkMessage::RepairRetainedBlob(command) => Some((
            (
                command.object_id.clone(),
                command.version.clone(),
                message.target_node_id.clone(),
            ),
            command.request_id.clone(),
        )),
        DfsNetworkMessage::FetchBlob(command) => {
            crate::repair_executor::repair_executor_request_need(
                local_node_id,
                &command.object_id,
                &command.version,
                &command.request_id,
            )
            .map(|key| (key, command.request_id.clone()))
        }
        _ => None,
    }
}

/// A detached executor send that did not complete ends its attempt as a
/// failure carrying `error`.
fn report_background_repair_send_failure(
    local_node_id: &NodeId,
    message: &DfsRoutedMessage,
    error: &str,
) {
    if let Some((key, request_id)) = background_repair_attempt(local_node_id, message) {
        crate::repair_executor::repair_executor_attempt_ended_with_error(
            local_node_id,
            &key,
            &request_id,
            crate::repair_executor::DfsRepairAttemptOutcome::Failed,
            Some(error.to_owned()),
        );
    }
}

/// Backend-free peer-sending capability with the shared detached outbox and
/// checkpointed, owned repair attachments. Hosts publish the worker only after
/// releasing admission; replies return through an admitted validation handler.
#[derive(Clone)]
pub struct DfsNodeDetachedSendWorker {
    transport: DfsNodeTransport,
    deferred_target_node_ids: BTreeSet<NodeId>,
    prepared_replica_calls: Arc<std::sync::Mutex<Vec<DfsCheckpointedReplicaCall>>>,
    background_response_handler: Option<
        Arc<
            dyn Fn(DfsRoutedMessage, Option<&DfsRoutedMessage>) -> Result<(), DfsError>
                + Send
                + Sync,
        >,
    >,
}

impl std::fmt::Debug for DfsNodeDetachedSendWorker {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DfsNodeDetachedSendWorker")
            .field("transport", &self.transport)
            .field("deferred_target_node_ids", &self.deferred_target_node_ids)
            .field(
                "background_response_handler",
                &self.background_response_handler.is_some(),
            )
            .finish_non_exhaustive()
    }
}

impl DfsNodeDetachedSendWorker {
    /// Read-only test observation of the SAME checkpointed payload owner.
    /// This clones actual headers/descriptors and leaves every reader parked.
    #[cfg(test)]
    pub(crate) fn checkpointed_replica_messages(
        &self,
        max_records: usize,
    ) -> (
        Vec<(DfsRoutedMessage, crate::DfsRpcAttachmentDescriptor)>,
        usize,
    ) {
        let prepared = self
            .prepared_replica_calls
            .lock()
            .expect("actual checkpointed replica calls");
        let omitted = prepared.len().saturating_sub(max_records);
        let records = prepared
            .iter()
            .take(max_records)
            .map(|request| {
                (
                    request.request.call.message.clone(),
                    request
                        .request
                        .call
                        .attachment
                        .as_ref()
                        .expect("native background repair retains its actual body descriptor")
                        .descriptor
                        .clone(),
                )
            })
            .collect();
        (records, omitted)
    }

    pub fn drain(&mut self) -> Result<(), DfsError> {
        let background = self.drain_background_replica_calls();
        let ordinary = self
            .transport
            .flush_detached_peer_sends_excluding_targets(
                &self.deferred_target_node_ids,
                self.background_response_handler.as_deref(),
            )
            .map_err(DfsError::from_network);
        background.and(ordinary)
    }

    /// Drain after obtaining exclusive ownership of the shared outbox worker.
    ///
    /// Long-lived daemon schedulers use this from `spawn_blocking`: returning
    /// early when another drain owns the gate can otherwise make the scheduler
    /// observe a temporarily empty outbox and exit just before failed work is
    /// restored for retry.
    pub fn drain_exclusive(&mut self) -> Result<(), DfsError> {
        self.drain_exclusive_with_retry_observation().0
    }

    /// Ephemeral DATA from the SAME drain, before ordinary outbox work runs.
    /// A consumed failed send is not a retained pre-poll observation retry.
    pub(crate) fn drain_exclusive_with_retry_observation(
        &mut self,
    ) -> (Result<(), DfsError>, bool) {
        let background = self.drain_background_replica_calls();
        let background_retry = false;
        let ordinary = self
            .transport
            .flush_detached_peer_sends_exclusive_excluding_targets(
                &self.deferred_target_node_ids,
                self.background_response_handler.as_deref(),
            )
            .map_err(DfsError::from_network);
        (background.and(ordinary), background_retry)
    }

    pub(crate) fn set_background_response_handler<F>(&mut self, handler: F)
    where
        F: Fn(DfsRoutedMessage, Option<&DfsRoutedMessage>) -> Result<(), DfsError>
            + Send
            + Sync
            + 'static,
    {
        self.background_response_handler = Some(Arc::new(handler));
    }

    pub(crate) fn retain_prepared_calls_from(&mut self, previous: &Self) {
        if Arc::ptr_eq(
            &self.prepared_replica_calls,
            &previous.prepared_replica_calls,
        ) {
            return;
        }
        let mut previous = previous
            .prepared_replica_calls
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut current = self
            .prepared_replica_calls
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        current.extend(std::mem::take(&mut *previous));
    }

    fn drain_background_replica_calls(&mut self) -> Result<(), DfsError> {
        // Share the existing outbox drain exclusion. Its owner uses try_lock
        // and never waits with reply admission held.
        let drain_gate = Arc::clone(&self.transport.detached_send_state.drain_gate);
        let _drain = drain_gate.lock().map_err(|_| DfsError::Network {
            message: "background repair drain exclusion is poisoned".into(),
        })?;
        let calls = {
            let mut prepared = self
                .prepared_replica_calls
                .lock()
                .map_err(|_| DfsError::Network {
                    message: "background DFS replica dispatch owner is poisoned".to_string(),
                })?;
            std::mem::take(&mut *prepared)
        };
        if calls.is_empty() {
            return Ok(());
        }
        let handler =
            self.background_response_handler
                .clone()
                .ok_or_else(|| DfsError::Network {
                    message: "background DFS repair has no admitted response owner".to_string(),
                })?;
        // An executor attempt is an ordinary send. One to a target the host
        // defers as unreachable does not leave: its attempt ends as a send
        // failure, and the executor parks the need until reachability changes.
        let (requests, deferred): (Vec<_>, Vec<_>) = calls.into_iter().partition(|call| {
            !self
                .deferred_target_node_ids
                .contains(&call.request.target_node_id)
        });
        for call in &deferred {
            report_background_repair_send_failure(
                &self.transport.local_node_id,
                &call.request.call.message,
                "the target is unreachable",
            );
        }
        if requests.is_empty() {
            return Ok(());
        }
        let expected = requests
            .iter()
            .map(|call| {
                (
                    call.request.target_node_id.clone(),
                    call.request.call.message.clone(),
                )
            })
            .collect::<Vec<_>>();
        let send_result = self.transport.send_routed_calls_bounded(
            requests.into_iter().map(|call| call.request).collect(),
            crate::service_outbound::DFS_REPLICA_BODY_FANOUT_CAP,
        );
        let mut first_error = None;
        let returned = match self.transport.drain_incoming_messages() {
            Ok(returned) => returned,
            Err(error) => {
                first_error = Some(DfsError::from_network(error));
                Vec::new()
            }
        };
        for message in returned {
            let original = expected
                .iter()
                .find(|(target, original)| {
                    message.source_node_id == *target
                        && all_background_replica_response_identity_matches(
                            &self.transport.local_node_id,
                            original,
                            &message,
                        )
                })
                .map(|(_, original)| original);
            let Some(original) = original else {
                first_error.get_or_insert_with(|| DfsError::Network {
                    message: "background DFS repair returned an unmatched response identity".into(),
                });
                continue;
            };
            if let Err(error) = handler(message.clone(), Some(original)) {
                first_error.get_or_insert(error);
                if let Err(error) = self
                    .transport
                    .retain_background_replica_completion(original.clone(), message)
                {
                    first_error.get_or_insert_with(|| DfsError::from_network(error));
                }
            }
        }
        match send_result {
            Ok(outcomes) => {
                if outcomes.len() != expected.len()
                    || outcomes
                        .iter()
                        .zip(&expected)
                        .any(|(outcome, (target, _))| outcome.target_node_id != *target)
                {
                    first_error.get_or_insert_with(|| DfsError::Network {
                        message: "background DFS repair outcomes differ from their owned calls"
                            .into(),
                    });
                }
                for (outcome, (_, message)) in outcomes.into_iter().zip(&expected) {
                    if let Err(error) = outcome.result {
                        report_background_repair_send_failure(
                            &self.transport.local_node_id,
                            message,
                            &error.to_string(),
                        );
                        first_error.get_or_insert_with(|| DfsError::from_network(error));
                    }
                }
            }
            Err(error) => {
                // A fan-out-level failure sent none of them.
                for (_, message) in &expected {
                    report_background_repair_send_failure(
                        &self.transport.local_node_id,
                        message,
                        &error.to_string(),
                    );
                }
                first_error.get_or_insert_with(|| DfsError::from_network(error));
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    #[cfg(test)]
    fn drain_at(&mut self, now: Instant) -> Result<(), DfsError> {
        self.transport
            .flush_detached_peer_sends_with_now_mode(
                &|| now,
                false,
                &self.deferred_target_node_ids,
                self.background_response_handler.as_deref(),
            )
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
        if !self
            .prepared_replica_calls
            .lock()
            .map_err(|_| DfsError::Network {
                message: "background DFS replica dispatch owner is poisoned".to_string(),
            })?
            .is_empty()
        {
            return Ok(true);
        }
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
        let mut snapshot = self
            .transport
            .detached_send_scheduling_snapshot_at_excluding_targets(
                Instant::now(),
                &self.deferred_target_node_ids,
            )
            .map_err(DfsError::from_network)?;
        snapshot.0 |= self.prepared_replica_calls_ready()?;
        Ok(snapshot)
    }

    fn prepared_replica_calls_ready(&self) -> Result<bool, DfsError> {
        Ok(self
            .prepared_replica_calls
            .lock()
            .map_err(|_| DfsError::Network {
                message: "background DFS replica dispatch owner is poisoned".into(),
            })?
            .iter()
            .any(|request| {
                !self
                    .deferred_target_node_ids
                    .contains(&request.request.target_node_id)
            }))
    }

    /// Wait only for an actual retained background observation failure.
    /// Ordinary ready work and its earlier cooling deadline keep priority.
    pub(crate) async fn wait_for_background_delivery_retry(
        &self,
        observed_generation: u64,
        retry_delay: Duration,
    ) -> Result<(), DfsError> {
        let retry_at = Instant::now() + retry_delay;
        let notify = Arc::clone(&self.transport.detached_send_state.ready_notify);
        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let (ready, ordinary_retry_at, current_generation) = self
            .transport
            .detached_send_scheduling_snapshot_at_excluding_targets(
                Instant::now(),
                &self.deferred_target_node_ids,
            )
            .map_err(DfsError::from_network)?;
        if ready || current_generation != observed_generation {
            return Ok(());
        }
        let wake_at = ordinary_retry_at.map_or(retry_at, |at| at.min(retry_at));
        tokio::select! {
            _ = &mut notified => Ok(()),
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(wake_at)) => Ok(()),
        }
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

impl Clone for DfsNodeTransport {
    fn clone(&self) -> Self {
        Self {
            #[cfg(test)]
            test_metadata_direct_probe: self.test_metadata_direct_probe.clone(),
            #[cfg(test)]
            test_routed_call_attempts: self.test_routed_call_attempts.clone(),
            #[cfg(test)]
            test_metadata_authority_routed_sends: self.test_metadata_authority_routed_sends.clone(),
            local_node_id: self.local_node_id.clone(),
            known_remote_nodes: self.known_remote_nodes.clone(),
            incoming_messages: if self.served_message.is_some() {
                VecDeque::new()
            } else {
                self.incoming_messages.clone()
            },
            response_delivery: self.response_delivery.clone(),
            response_inbox: self
                .response_delivery
                .as_ref()
                .map(|_| crate::response_delivery::DfsRoutedResponseInboxOwner::new()),
            response_registrations: BTreeMap::new(),
            detached_send_state: self.detached_send_state.clone(),
            connect_timeout: self.connect_timeout,
            sender: self.sender.clone(),
            async_sender: self.async_sender.clone(),
            local_namespace_dispatch: self.local_namespace_dispatch,
            // No request attachment or checkpoint is copied by a transport clone.
            served_message: self.served_message.as_ref().map(|_| Box::default()),
            direct_read_sender: self.direct_read_sender.clone(),
            async_direct_read_sender: self.async_direct_read_sender.clone(),
            peer_transport: self.peer_transport.clone(),
            async_peer_transport: self.async_peer_transport.clone(),
            blocking_metadata_fanout_workers: self.blocking_metadata_fanout_workers.clone(),
            next_content_root_catalog_frame_id: self.next_content_root_catalog_frame_id.clone(),
            operation_deadline: self.operation_deadline,
            // Waits belong to one transport; a clone has its own.
            incoming_wait_interrupt: Arc::default(),
            hint_to_main_in_flight: Arc::default(),
            send_cancellation: None,
            #[cfg(any(test, feature = "test-support"))]
            test_drain_counter: self.test_drain_counter.clone(),
        }
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
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn test_queued_read_reply(&self, request: &str) -> Option<DfsRoutedMessage> {
        self.incoming_messages
            .iter()
            .find(|routed| {
                matches!(&routed.message,
            DfsNetworkMessage::ReadResponse(response) if response.request_id == request)
            })
            .cloned()
    }

    /// Alter only actual replies already owned by this transport's decoded
    /// inbox. No message, request, pending state or healthy response is created.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn test_alter_queued_read_replies(
        &mut self,
        change: crate::test_runtime::ReadReplyFault,
        max_replies: usize,
    ) -> Vec<DfsRoutedMessage> {
        assert!(
            (1..=32).contains(&max_replies),
            "bounded negative read control required"
        );
        let mut captured = Vec::new();
        for routed in &mut self.incoming_messages {
            if captured.len() == max_replies {
                break;
            }
            if !matches!(
                &routed.message,
                DfsNetworkMessage::ReadResponse(crate::DfsReadResponse {
                    result: crate::DfsReadResult::Found(_),
                    ..
                })
            ) {
                continue;
            }
            captured.push(routed.clone());
            let source = routed.source_node_id.clone();
            let DfsNetworkMessage::ReadResponse(response) = &mut routed.message else {
                unreachable!()
            };
            match change {
                crate::test_runtime::ReadReplyFault::NotFound => {
                    response.result = crate::DfsReadResult::NotFound
                }
                crate::test_runtime::ReadReplyFault::StaleLocation => {
                    response.result = crate::DfsReadResult::Rejected {
                        reason: format!(
                            "node {source} is not a current location for DFS object {}",
                            response.object_id
                        ),
                    };
                }
                crate::test_runtime::ReadReplyFault::WrongEnvelopeObject => {
                    response.object_id = crate::DfsObjectId::generate()
                }
                crate::test_runtime::ReadReplyFault::WrongContentHash => {
                    let crate::DfsReadResult::Found(data) = &mut response.result else {
                        unreachable!()
                    };
                    let hash = data.content_hash.as_mut().expect("captured native hash");
                    let last = hash.pop().expect("captured native nonempty hash");
                    hash.push(if last == '0' { '1' } else { '0' });
                }
                crate::test_runtime::ReadReplyFault::WrongPayloadObject => {
                    let crate::DfsReadResult::Found(data) = &mut response.result else {
                        unreachable!()
                    };
                    data.object_id = crate::DfsObjectId::generate();
                }
            }
        }
        assert!(
            !captured.is_empty(),
            "negative read control requires an actual queued Found reply"
        );
        captured
    }

    /// Unit protocol input only; healthy replication uses authenticated hosted RPC.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn test_enqueue_protocol_message(&mut self, message: DfsRoutedMessage) {
        self.incoming_messages.push_back(message);
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn test_with_drain_counter(mut self, counter: Arc<std::sync::Mutex<usize>>) -> Self {
        self.test_drain_counter = Some(counter);
        self
    }

    fn drain_owned_incoming(&mut self) -> Result<Vec<DfsRoutedMessage>, NetworkError> {
        #[cfg(any(test, feature = "test-support"))]
        if let Some(counter) = &self.test_drain_counter {
            *counter.lock().expect("normal DFS mailbox drain counter") += 1;
        }
        let mut messages: Vec<_> = self.incoming_messages.drain(..).collect();
        if let Some(inbox) = &self.response_inbox {
            messages.extend(inbox.drain()?);
        }
        Ok(messages)
    }

    fn owned_incoming_count(&self) -> Result<usize, NetworkError> {
        Ok(self.incoming_messages.len()
            + self
                .response_inbox
                .as_ref()
                .map(|inbox| inbox.len())
                .transpose()?
                .unwrap_or(0))
    }

    /// Fork the transport state for one independent read-only request.
    ///
    /// Sender and peer-transport handles are intentionally shared: they are
    /// thread-safe connection/routing capabilities. Per-service response
    /// queues, detached sends, and operation deadlines must not be shared or
    /// copied into the fork, otherwise an opened-file read could consume a
    /// response owned by the mutable mounted service (or vice versa).
    pub(crate) fn fork_read_only_request(&self) -> Self {
        let mut fork = self.fork_browser_request();
        fork.operation_deadline = None;
        fork
    }

    /// Independent caller inbox with the same Main route and original budget.
    pub(crate) fn fork_browser_request(&self) -> Self {
        let mut fork = self.clone();
        fork.served_message = None;
        fork.incoming_messages.clear();
        fork.detached_send_state = DfsNodeDetachedSendState::default();
        fork.send_cancellation = self.send_cancellation.clone();
        fork
    }

    fn fork_detached_send_worker(&self) -> DfsNodeDetachedSendWorker {
        let mut transport = self.clone();
        transport.served_message = None;
        transport.incoming_messages.clear();
        // Background promotion is independent of the completed caller's
        // deadline. Each queued send still retains its own explicit policy.
        transport.operation_deadline = None;
        DfsNodeDetachedSendWorker {
            transport,
            deferred_target_node_ids: BTreeSet::new(),
            prepared_replica_calls: Arc::new(std::sync::Mutex::new(Vec::new())),
            background_response_handler: None,
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
        fork.served_message = None;
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

    /// Route caller effects through its authenticated resident Main senders.
    /// This chooses delivery only; it grants no author or publication custody.
    pub fn with_registered_main_namespace_dispatch(mut self) -> Self {
        // Existing served custody never flips into an ordinary caller role.
        if self.served_message.is_some() {
            return self;
        }
        assert!(
            self.sender.is_some() && self.async_sender.is_some(),
            "registered Main dispatch requires the normal authenticated senders"
        );
        self.local_namespace_dispatch = LocalNamespaceDispatch::RegisteredMain;
        self
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
        let response_delivery = peer_transport
            .as_ref()
            .and_then(|peer| peer.response_delivery_owner());
        let response_inbox = response_delivery
            .as_ref()
            .map(|_| crate::response_delivery::DfsRoutedResponseInboxOwner::new());
        Self {
            #[cfg(test)]
            test_metadata_direct_probe: None,
            #[cfg(test)]
            test_routed_call_attempts: None,
            #[cfg(test)]
            test_metadata_authority_routed_sends: None,
            local_node_id,
            known_remote_nodes,
            incoming_messages: VecDeque::new(),
            response_delivery,
            response_inbox,
            response_registrations: BTreeMap::new(),
            detached_send_state,
            connect_timeout,
            sender,
            async_sender,
            local_namespace_dispatch: LocalNamespaceDispatch::InlineCore,
            served_message: None,
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
            incoming_wait_interrupt: Arc::default(),
            hint_to_main_in_flight: Arc::default(),
            send_cancellation: None,
            #[cfg(any(test, feature = "test-support"))]
            test_drain_counter: None,
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

    fn deliver_local_write_response(
        &self,
        message: &DfsRoutedMessage,
    ) -> Result<bool, NetworkError> {
        if !matches!(
            &message.message,
            DfsNetworkMessage::DistributedWriteResponse(_)
        ) {
            return Ok(false);
        }
        let Some(owner) = self.response_delivery.as_ref() else {
            return Ok(false);
        };
        // Charge the same actual ordinary command envelope as a native sender.
        // This transfers inbox custody only; its original caller validates the
        // response identity, authority and durable outcome after receiving it.
        let command = DfsMessageCommand {
            request_id: format!("dfs_message_{}", message.frame_id),
            routed_message: message.clone(),
        };
        let encoded_value_bytes = dfs_message_rpc_request(&command)?.body.len();
        owner.try_deliver_with_size(message, encoded_value_bytes)
    }

    fn routes_local_request_to_main(&self, target: &NodeId, message: &DfsNetworkMessage) -> bool {
        target == &self.local_node_id
            && self.local_namespace_dispatch == LocalNamespaceDispatch::RegisteredMain
            && matches!(
                message,
                DfsNetworkMessage::NamespaceRequest(_)
                    | DfsNetworkMessage::DistributedWriteRequest(_)
                    | DfsNetworkMessage::RetainedWriteRequest(_)
                    | DfsNetworkMessage::ExclusiveCreateRequest(_)
                    | DfsNetworkMessage::StableWriteReconciliationRequest(_)
                    | DfsNetworkMessage::StableNamespaceReconciliationRequest(_)
                    | DfsNetworkMessage::PrimaryVerificationRequest(_)
            )
    }

    fn send_routed_message_to_peer(
        &mut self,
        target_node_id: &NodeId,
        routed_message: DfsRoutedMessage,
        require_local_source: bool,
    ) -> Result<(), NetworkError> {
        let _send_cancellation = DfsSendCancellationScope::enter(self.send_cancellation.clone());
        let mut pending =
            VecDeque::from([(target_node_id.clone(), routed_message, require_local_source)]);
        while let Some((target_node_id, routed_message, require_local_source)) = pending.pop_front()
        {
            if target_node_id == self.local_node_id {
                if self.deliver_local_write_response(&routed_message)? {
                    continue;
                }
                if self.routes_local_request_to_main(&target_node_id, &routed_message.message) {
                    let sender = self
                        .sender
                        .as_ref()
                        .cloned()
                        .ok_or_else(|| NetworkError::Io {
                            operation: format!(
                                "delegate local DFS request to Main {target_node_id}"
                            ),
                            detail: "registered Main request has no actual sender".into(),
                        })?;
                    self.validate_routed_message_target(
                        &target_node_id,
                        &routed_message,
                        require_local_source,
                    )?;
                    let options = self.current_send_options(format!(
                        "delegate local DFS namespace to Main {target_node_id}"
                    ))?;
                    let returned = send_dfs_routed_message_to_peer(
                        sender,
                        options,
                        target_node_id,
                        routed_message,
                    )?;
                    for reply in returned {
                        pending.push_back((reply.target_node_id.clone(), reply, false));
                    }
                } else {
                    self.incoming_messages.push_back(routed_message);
                }
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

    fn send_routed_attachment_call_to_peer(
        &mut self,
        target_node_id: &NodeId,
        call: DfsRoutedCall,
        require_local_source: bool,
        #[cfg(any(test, feature = "test-support"))] native_context: Option<
            crate::native_shard_trace::NativeRpcAttemptContext,
        >,
    ) -> Result<(), NetworkError> {
        if target_node_id == &self.local_node_id
            && !self.routes_local_request_to_main(target_node_id, &call.message.message)
        {
            let message = crate::materialize_dfs_routed_call_sync(call)?;
            self.incoming_messages.push_back(message);
            return Ok(());
        }
        self.validate_routed_message_target(target_node_id, &call.message, require_local_source)?;
        let Some(peer_transport) = self.peer_transport.as_ref().cloned() else {
            return Err(NetworkError::Io {
                operation: format!("send DFS attachment to {target_node_id}"),
                detail: "DFS peer transport does not support canonical request attachments"
                    .to_string(),
            });
        };
        let (command, call) = dfs_routed_call_rpc_call(call)?;
        let options =
            self.current_send_options(format!("send DFS attachment to {target_node_id}"))?;
        let call = dfs_rpc_call_with_request_budget(call, options);
        #[cfg(any(test, feature = "test-support"))]
        let mut native_attempt =
            native_context.and_then(|context| context.begin(target_node_id, &command));
        let _send_cancellation = DfsSendCancellationScope::enter(self.send_cancellation.clone());
        let result = peer_transport.call_object_rpc(
            target_node_id,
            call,
            options.with_observe_connectivity(!dfs_message_is_read_only_request(&command)),
        );
        #[cfg(any(test, feature = "test-support"))]
        if let (Some(attempt), Err(error)) = (native_attempt.as_mut(), &result) {
            attempt.returned(&Err(error.clone()));
        }
        let result = result.map_err(|source| {
            dfs_peer_transport_network_error(
                format!("send DFS attachment to {target_node_id}"),
                source,
            )
        })?;
        let returned = dfs_message_response_from_rpc_result_sync(result);
        #[cfg(any(test, feature = "test-support"))]
        if let Some(attempt) = native_attempt.as_mut() {
            attempt.returned(&returned);
        }
        let returned = returned.map_err(|source| {
            dfs_applied_request_response_error(
                format!("decode DFS attachment response from {target_node_id}"),
                source,
            )
        })?;
        for message in returned.outbound_messages {
            let target = message.target_node_id.clone();
            self.send_routed_message_to_peer(&target, message, false)
                .map_err(|source| {
                    dfs_applied_request_response_error(
                        format!("deliver DFS attachment response from {target_node_id}"),
                        source,
                    )
                })?;
        }
        Ok(())
    }

    fn send_routed_call_to_peer(
        &mut self,
        target_node_id: &NodeId,
        call: DfsRoutedCall,
        require_local_source: bool,
        #[cfg(any(test, feature = "test-support"))] native_context: Option<
            crate::native_shard_trace::NativeRpcAttemptContext,
        >,
    ) -> Result<(), NetworkError> {
        if call.attachment.is_some() {
            self.send_routed_attachment_call_to_peer(
                target_node_id,
                call,
                require_local_source,
                #[cfg(any(test, feature = "test-support"))]
                native_context.clone(),
            )
        } else {
            self.send_routed_message_to_peer(target_node_id, call.message, require_local_source)
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
                .any(|send| send.eligible_for_target_policy(deferred_target_node_ids))
                .then_some(*at)
        });
        let ready = outbox
            .ready_sends
            .iter()
            .any(|send| send.eligible_for_target_policy(deferred_target_node_ids))
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
            .partition(|send| send.eligible_for_target_policy(deferred_target_node_ids));
        outbox.ready_sends = deferred;
        Ok(eligible)
    }

    fn send_routed_message_to_peer_detached(
        &mut self,
        target_node_id: &NodeId,
        routed_message: DfsRoutedMessage,
        require_local_source: bool,
    ) -> Result<(), NetworkError> {
        if target_node_id == &self.local_node_id
            && !self.routes_local_request_to_main(target_node_id, &routed_message.message)
        {
            if self.deliver_local_write_response(&routed_message)? {
                return Ok(());
            }
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
            sender: Some(sender),
            options,
            operation_deadline,
            target_node_id: target_node_id.clone(),
            routed_message,
            promotion_sequence: None,
            pending_local_responses: VecDeque::new(),
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
        self.flush_detached_peer_sends_excluding_targets(&BTreeSet::new(), None)
    }

    fn flush_detached_peer_sends_excluding_targets(
        &mut self,
        deferred_target_node_ids: &BTreeSet<NodeId>,
        response_handler: Option<
            &(
                 dyn Fn(DfsRoutedMessage, Option<&DfsRoutedMessage>) -> Result<(), DfsError>
                     + Send
                     + Sync
             ),
        >,
    ) -> Result<(), NetworkError> {
        self.flush_detached_peer_sends_with_now_mode(
            &Instant::now,
            false,
            deferred_target_node_ids,
            response_handler,
        )
    }

    fn flush_detached_peer_sends_exclusive_excluding_targets(
        &mut self,
        deferred_target_node_ids: &BTreeSet<NodeId>,
        response_handler: Option<
            &(
                 dyn Fn(DfsRoutedMessage, Option<&DfsRoutedMessage>) -> Result<(), DfsError>
                     + Send
                     + Sync
             ),
        >,
    ) -> Result<(), NetworkError> {
        self.flush_detached_peer_sends_with_now_mode(
            &Instant::now,
            true,
            deferred_target_node_ids,
            response_handler,
        )
    }

    #[cfg(test)]
    fn flush_detached_peer_sends_with_now<F>(&mut self, now: &F) -> Result<(), NetworkError>
    where
        F: Fn() -> Instant + ?Sized,
    {
        self.flush_detached_peer_sends_with_now_mode(now, false, &BTreeSet::new(), None)
    }

    fn flush_detached_peer_sends_with_now_mode<F>(
        &mut self,
        now: &F,
        wait_for_drain_gate: bool,
        deferred_target_node_ids: &BTreeSet<NodeId>,
        response_handler: Option<
            &(
                 dyn Fn(DfsRoutedMessage, Option<&DfsRoutedMessage>) -> Result<(), DfsError>
                     + Send
                     + Sync
             ),
        >,
    ) -> Result<(), NetworkError>
    where
        F: Fn() -> Instant + ?Sized,
    {
        // The Quint fan-out cap applies to the shared outbox, not to each
        // transport clone independently. One worker owns ready work while
        // retaining cap-two concurrency inside its drain. A competing worker
        // never blocks a Tokio blocking-pool thread behind the global gate.
        let _send_cancellation = DfsSendCancellationScope::enter(self.send_cancellation.clone());
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
                                scope.spawn(sinomo_core::service::observed_blocking_task(
                                    "dfs.promotion",
                                    move || {
                                        Self::send_detached_peer_message(
                                            send,
                                            current_operation_deadline,
                                        )
                                    },
                                ))
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

                let mut send = sends.pop_front().expect("detached send front");
                let attempted_replay = send
                    .is_replay_safe_completed_write_response()
                    .then(|| send.clone());
                let current_operation_deadline = if send.uses_fresh_detached_budget() {
                    None
                } else {
                    self.operation_deadline
                };
                let local_response_handler = (matches!(
                    &send.routed_message.message,
                    DfsNetworkMessage::FetchBlob(_)
                ) || !send.pending_local_responses.is_empty())
                .then_some(response_handler)
                .flatten();
                let result = if let Some(handler) = local_response_handler {
                    self.deliver_detached_local_responses(
                        &mut send,
                        current_operation_deadline,
                        handler,
                    )
                } else if !send.pending_local_responses.is_empty() {
                    Err(NetworkError::Io {
                        operation: "resume detached DFS local response delivery".into(),
                        detail: "retained response requires its admitted local handler".into(),
                    })
                } else {
                    Self::send_detached_peer_message(send.clone(), current_operation_deadline)
                        .and_then(|returned_messages| {
                            self.route_detached_peer_responses(returned_messages)
                        })
                };
                if let Err(error) = result {
                    if !send.pending_local_responses.is_empty() {
                        // The network call finished. Retry only its exact local
                        // delivery, never a fresh mutation or a fabricated ACK.
                        // Use the existing shared cooling queue and retry tiers.
                        replay_retries.push_back(send);
                        if first_replay_error.is_none() {
                            first_replay_error = Some(error);
                        }
                        continue;
                    }
                    if let Some(attempted_replay) = attempted_replay {
                        replay_retries.push_back(attempted_replay);
                        if first_replay_error.is_none() {
                            first_replay_error = Some(error);
                        }
                        continue;
                    }
                    // A fetch that returned nothing deliverable is not
                    // retried here; its repair attempt ends as a failure.
                    report_background_repair_send_failure(
                        &self.local_node_id,
                        &send.routed_message,
                        &error.to_string(),
                    );
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
            send.sender.ok_or_else(|| NetworkError::Io {
                operation: "send detached DFS message".into(),
                detail: "already-returned repair has no outbound sender or attachment".into(),
            })?,
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

    fn retain_background_replica_completion(
        &mut self,
        original: DfsRoutedMessage,
        response: DfsRoutedMessage,
    ) -> Result<(), NetworkError> {
        if !all_background_replica_response_identity_matches(
            &self.local_node_id,
            &original,
            &response,
        ) {
            return Err(NetworkError::Serialization {
                detail: "retained background repair response changed original identity".into(),
            });
        }
        let completion = DeferredDfsPeerSend {
            sender: None,
            options: DfsObjectRpcSendOptions::new(self.connect_timeout),
            operation_deadline: None,
            target_node_id: original.target_node_id.clone(),
            routed_message: original,
            promotion_sequence: None,
            pending_local_responses: VecDeque::from([response]),
            retry_attempts: 0,
        };
        // The first local attempt already failed. Reuse the existing shared
        // backoff, worker exclusion and target policy; no new remote call exists.
        self.restore_detached_peer_send_work(
            VecDeque::new(),
            VecDeque::from([completion]),
            Instant::now(),
        )
    }

    fn deliver_detached_local_responses(
        &self,
        send: &mut DeferredDfsPeerSend,
        operation_deadline: Option<Instant>,
        handler: &(
             dyn Fn(DfsRoutedMessage, Option<&DfsRoutedMessage>) -> Result<(), DfsError>
                 + Send
                 + Sync
         ),
    ) -> Result<(), NetworkError> {
        if send.pending_local_responses.is_empty() {
            let DfsNetworkMessage::FetchBlob(command) = &send.routed_message.message else {
                return Err(NetworkError::Serialization {
                    detail: "retained repair completion has no original returned frame".into(),
                });
            };
            let returned = Self::send_detached_peer_message(send.clone(), operation_deadline)?;
            for message in &returned {
                let original = &send.routed_message;
                let matching = matches!(&message.message,
                DfsNetworkMessage::FetchBlobResponse(response)
                    if original.source_node_id == self.local_node_id
                        && message.source_node_id == original.target_node_id
                        && message.target_node_id == original.source_node_id
                        && response.source_node_id == original.target_node_id
                        && response.request_id == command.request_id
                        && response.intent_id == command.intent_id
                        && response.object_id == command.object_id
                        && response.version == command.version
                        && match &response.result {
                            crate::DfsFetchBlobResult::Found { content_hash, .. } => content_hash == &command.content_hash,
                            _ => true,
                        });
                if !matching {
                    return Err(NetworkError::Serialization {
                        detail: "detached DFS fetch returned an unmatched response identity".into(),
                    });
                }
            }
            send.pending_local_responses = returned.into();
        }
        while let Some(response) = send.pending_local_responses.front() {
            let original = if matches!(
                &send.routed_message.message,
                DfsNetworkMessage::RepairReplica(_) | DfsNetworkMessage::RepairRetainedBlob(_)
            ) {
                if send.sender.is_some()
                    || !all_background_replica_response_identity_matches(
                        &self.local_node_id,
                        &send.routed_message,
                        response,
                    )
                {
                    return Err(NetworkError::Serialization {
                        detail: "retained repair completion changed original call custody".into(),
                    });
                }
                Some(&send.routed_message)
            } else {
                None
            };
            // Remove only after local processing succeeds. A failed handler can
            // already have durable effects; its next call must see the SAME
            // frame and validate the original native intent/content again.
            handler(response.clone(), original).map_err(|error| NetworkError::Io {
                operation: "deliver detached DFS response to Main".into(),
                detail: error.to_string(),
            })?;
            send.pending_local_responses.pop_front();
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
        let registered_local_request =
            self.routes_local_request_to_main(target_node_id, &routed_message.message);
        if !registered_local_request && !self.known_remote_nodes.contains(target_node_id) {
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
                if self.deliver_local_write_response(&routed_message)? {
                    continue;
                }
                if self.routes_local_request_to_main(&target_node_id, &routed_message.message) {
                    let sender =
                        self.async_sender
                            .as_ref()
                            .cloned()
                            .ok_or_else(|| NetworkError::Io {
                                operation: format!(
                                    "delegate async local DFS request to Main {target_node_id}"
                                ),
                                detail: "registered Main request has no actual sender".into(),
                            })?;
                    self.validate_routed_message_target(
                        &target_node_id,
                        &routed_message,
                        require_local_source,
                    )?;
                    let options = self.current_send_options(format!(
                        "delegate async local DFS namespace to Main {target_node_id}"
                    ))?;
                    let returned = send_dfs_routed_message_to_peer_async(
                        sender,
                        options,
                        target_node_id,
                        routed_message,
                    )
                    .await?;
                    for reply in returned {
                        pending.push_back((reply.target_node_id.clone(), reply, false));
                    }
                } else {
                    self.incoming_messages.push_back(routed_message);
                }
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

    async fn send_routed_attachment_call_to_peer_async(
        &mut self,
        target_node_id: &NodeId,
        call: DfsRoutedCall,
        require_local_source: bool,
        #[cfg(any(test, feature = "test-support"))] native_context: Option<
            crate::native_shard_trace::NativeRpcAttemptContext,
        >,
    ) -> Result<(), NetworkError> {
        if target_node_id == &self.local_node_id
            && !self.routes_local_request_to_main(target_node_id, &call.message.message)
        {
            let message = crate::message_transport::materialize_dfs_routed_call(call).await?;
            self.incoming_messages.push_back(message);
            return Ok(());
        }
        self.validate_routed_message_target(target_node_id, &call.message, require_local_source)?;
        let Some(peer_transport) = self.async_peer_transport.as_ref().cloned() else {
            return Err(NetworkError::Io {
                operation: format!("send DFS attachment to {target_node_id}"),
                detail: "async DFS peer transport does not support canonical request attachments"
                    .to_string(),
            });
        };
        let (command, call) = dfs_routed_call_rpc_call(call)?;
        let options =
            self.current_send_options(format!("send DFS attachment to {target_node_id}"))?;
        let call = dfs_rpc_call_with_request_budget(call, options);
        #[cfg(any(test, feature = "test-support"))]
        let mut native_attempt =
            native_context.and_then(|context| context.begin(target_node_id, &command));
        let result = peer_transport
            .call_object_rpc_async(
                target_node_id,
                call,
                options.with_observe_connectivity(!dfs_message_is_read_only_request(&command)),
            )
            .await;
        #[cfg(any(test, feature = "test-support"))]
        if let (Some(attempt), Err(error)) = (native_attempt.as_mut(), &result) {
            attempt.returned(&Err(error.clone()));
        }
        let result = result.map_err(|source| {
            dfs_peer_transport_network_error(
                format!("send DFS attachment to {target_node_id}"),
                source,
            )
        })?;
        let returned = dfs_message_response_from_rpc_result(result).await;
        #[cfg(any(test, feature = "test-support"))]
        if let Some(attempt) = native_attempt.as_mut() {
            attempt.returned(&returned);
        }
        let returned = returned.map_err(|source| {
            dfs_applied_request_response_error(
                format!("decode DFS attachment response from {target_node_id}"),
                source,
            )
        })?;
        for message in returned.outbound_messages {
            let target = message.target_node_id.clone();
            self.send_routed_message_to_peer_async(&target, message, false)
                .await
                .map_err(|source| {
                    dfs_applied_request_response_error(
                        format!("deliver DFS attachment response from {target_node_id}"),
                        source,
                    )
                })?;
        }
        Ok(())
    }

    async fn send_routed_call_to_peer_async(
        &mut self,
        target_node_id: &NodeId,
        call: DfsRoutedCall,
        require_local_source: bool,
        #[cfg(any(test, feature = "test-support"))] native_context: Option<
            crate::native_shard_trace::NativeRpcAttemptContext,
        >,
    ) -> Result<(), NetworkError> {
        if call.attachment.is_some() {
            self.send_routed_attachment_call_to_peer_async(
                target_node_id,
                call,
                require_local_source,
                #[cfg(any(test, feature = "test-support"))]
                native_context.clone(),
            )
            .await
        } else {
            self.send_routed_message_to_peer_async(
                target_node_id,
                call.message,
                require_local_source,
            )
            .await
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

/// The first attempt of a response-retry-safe request carries the caller's
/// whole remaining budget, and so does the request it stamps for the serving
/// node. An attempt whose response is lost before that budget ends lost it to
/// a broken stream: the serving node sees the stream end, which ends that
/// attempt's served wait and releases its admission, and the identical request
/// is replayed in what remains. Two attempts therefore never hold admission
/// together. A request without a budget is not replayed here.
fn dfs_exact_response_replay_attempt_options(
    message: &DfsNetworkMessage,
    options: DfsObjectRpcSendOptions,
) -> (DfsObjectRpcSendOptions, Option<Duration>) {
    if !crate::routes::dfs_network_message_response_retry_safe(message) {
        return (options, None);
    }
    (options, options.overall_timeout)
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

/// A response decoder or a forwarded reply can fail after the original peer
/// handled the request. Delivery classification for that reply cannot prove
/// that the original mutation was not sent.
fn dfs_applied_request_response_error(
    operation: String,
    error: impl std::fmt::Display,
) -> NetworkError {
    NetworkError::delivery_failed(
        operation,
        sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
        error.to_string(),
    )
}

fn dfs_exact_response_replay_exhausted(
    operation: String,
    first_error: DfsPeerTransportError,
    retry_error: DfsPeerTransportError,
) -> NetworkError {
    NetworkError::RetryExhausted {
        operation,
        attempts: 2,
        // The initial attempt may already have applied. A retry that was not
        // sent cannot erase that uncertainty about the same operation.
        delivery_state: sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
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
        #[cfg(any(test, feature = "test-support"))]
        let native_context = crate::native_shard_trace::rpc_attempt_context();
        if self.served_message.is_some() {
            return self.send_served_routed_call_async(
                target_node_id,
                call,
                #[cfg(any(test, feature = "test-support"))]
                native_context.clone(),
            );
        }
        Box::pin(async move {
            self.send_routed_call_to_peer_async(
                target_node_id,
                call,
                true,
                #[cfg(any(test, feature = "test-support"))]
                native_context.clone(),
            )
            .await
        })
    }
    fn send_routed_calls_bounded_async<'a>(
        &'a mut self,
        requests: Vec<DfsRoutedSend>,
        max_inflight: usize,
    ) -> DfsMessageTransportFuture<'a, Result<Vec<DfsRoutedSendOutcome>, NetworkError>> {
        #[cfg(any(test, feature = "test-support"))]
        let native_context = crate::native_shard_trace::rpc_attempt_context();
        if self.served_message.is_some() {
            self.send_served_calls_bounded_async(
                requests,
                max_inflight,
                #[cfg(any(test, feature = "test-support"))]
                native_context.clone(),
            )
        } else {
            self.send_peer_calls_bounded_async(
                requests,
                max_inflight,
                #[cfg(any(test, feature = "test-support"))]
                native_context.clone(),
            )
        }
    }
    fn drain_incoming_messages_async<'a>(
        &'a mut self,
    ) -> DfsMessageTransportFuture<'a, Result<Vec<DfsRoutedMessage>, NetworkError>> {
        Box::pin(async move { DfsMessageTransport::drain_incoming_messages(self) })
    }
    fn pending_incoming_count_async<'a>(
        &'a mut self,
    ) -> DfsMessageTransportFuture<'a, Result<Option<usize>, NetworkError>> {
        Box::pin(async move { DfsMessageTransport::pending_incoming_count(self) })
    }
    fn wait_for_incoming_async<'a>(
        &'a mut self,
        timeout: Duration,
    ) -> DfsMessageTransportFuture<'a, Result<bool, NetworkError>> {
        Box::pin(async move {
            if self.served_message.is_some() {
                // Preserve the former served transport's default async wait.
                if DfsMessageTransport::pending_incoming_count(self)?.is_some_and(|count| count > 0)
                {
                    return Ok(true);
                }
                if !timeout.is_zero() {
                    self.incoming_wait_interrupt.wait_async(timeout).await;
                }
                return Ok(DfsMessageTransport::pending_incoming_count(self)?
                    .is_some_and(|count| count > 0));
            }
            if self.owned_incoming_count()? > 0 {
                return Ok(true);
            }
            if let Some(inbox) = &self.response_inbox {
                return inbox.wait_async(timeout).await;
            }
            if !timeout.is_zero() {
                self.incoming_wait_interrupt.wait_async(timeout).await;
            }
            Ok(self.owned_incoming_count()? > 0)
        })
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

/// Perform one prepared direct metadata journal call and decode its typed reply.
async fn call_direct_metadata_journal_rpc_async(
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    prepared: DfsPreparedMetadataJournalBatchRpc,
) -> Result<(NodeId, DfsMetadataJournalBatchResponse), NetworkError> {
    let trace_context = prepared.trace_context();
    let outcome = match prepared.call() {
        Ok(call) => {
            #[cfg(any(test, feature = "test-support"))]
            let native_attempt = prepared.begin_native_attempt(&call.request.route);
            let rpc_response = peer_transport
                .call_object_rpc_async(&prepared.target_node_id, call, prepared.options)
                .await;
            prepared.finish(
                rpc_response,
                #[cfg(any(test, feature = "test-support"))]
                native_attempt,
            )
        }
        Err(error) => Err(error),
    };
    trace_direct_metadata_journal_fanout_call(trace_context, &outcome);
    outcome
}

fn call_direct_metadata_journal_rpc_blocking(
    peer_transport: &dyn DfsPeerTransport,
    prepared: DfsPreparedMetadataJournalBatchRpc,
) -> Result<(NodeId, DfsMetadataJournalBatchResponse), NetworkError> {
    let trace_context = prepared.trace_context();
    let outcome = match prepared.call() {
        Ok(call) => {
            #[cfg(any(test, feature = "test-support"))]
            let native_attempt = prepared.begin_native_attempt(&call.request.route);
            let rpc_response =
                peer_transport.call_object_rpc(&prepared.target_node_id, call, prepared.options);
            prepared.finish(
                rpc_response,
                #[cfg(any(test, feature = "test-support"))]
                native_attempt,
            )
        }
        Err(error) => Err(error),
    };
    trace_direct_metadata_journal_fanout_call(trace_context, &outcome);
    outcome
}

type DfsDirectMetadataJournalTraceContext =
    Option<(Instant, NodeId, crate::DfsMetadataShardId, u64, bool)>;

fn trace_direct_metadata_journal_fanout_call(
    trace_context: DfsDirectMetadataJournalTraceContext,
    outcome: &Result<(NodeId, DfsMetadataJournalBatchResponse), NetworkError>,
) {
    if let Some((started_at, target_node_id, shard_id, sequence, authority)) = trace_context {
        trace_direct_metadata_journal_fanout_peer_outcome(
            &target_node_id,
            &shard_id,
            sequence,
            authority,
            started_at,
            outcome,
        );
    }
}

/// One detached blocking metadata fan-out thread. The count is shared by every
/// transport clone, so stalled peers cannot accumulate unbounded threads.
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
            workers.spawn(
                call_direct_metadata_journal_rpc_async(peer_transport, prepared).in_current_span(),
            );
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
        let task = sinomo_core::service::observed_blocking_task("dfs.metadata.fanout", move || {
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
                let outcome = call_direct_metadata_journal_rpc_blocking(&*peer_transport, prepared);
                if canceled.load(AtomicOrdering::Acquire) || Instant::now() >= deadline {
                    break;
                }
                if completed_tx.send(outcome).is_err() {
                    break;
                }
            }
        });
        if std::thread::Builder::new()
            .name(format!("dfs-metadata-fanout-{worker_index}"))
            .spawn(move || {
                let _worker_slot = worker_slot;
                task();
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

/// Workers behind one in-flight metadata authority fan-out.
///
/// Async calls run as runtime tasks and end with the fan-out. A blocking call
/// cannot be interrupted: it keeps one shared worker slot until it returns,
/// which its network deadline (the fan-out deadline) bounds.
struct DfsNodeMetadataAuthorityFanoutWorkers {
    connect_timeout: Duration,
    mode: DfsNodeMetadataAuthorityFanoutMode,
}

enum DfsNodeMetadataAuthorityFanoutMode {
    Async {
        runtime: tokio::runtime::Handle,
        peer_transport: Arc<dyn AsyncDfsPeerTransport>,
        sender: Option<AsyncDfsMessageSender>,
        direct_slots: Arc<tokio::sync::Semaphore>,
        tasks: Vec<tokio::task::AbortHandle>,
    },
    Blocking {
        peer_transport: Arc<dyn DfsPeerTransport>,
        sender: Option<DfsMessageSender>,
        worker_slots: Arc<StdAtomicUsize>,
    },
}

/// Reports exactly one fan-out event per started call, including a call whose
/// worker ends without producing a result.
struct DfsMetadataAuthorityFanoutCompletion {
    events: Option<std::sync::mpsc::Sender<DfsMetadataAuthorityFanoutSignal>>,
    target_node_id: NodeId,
    routed: bool,
}

impl DfsMetadataAuthorityFanoutCompletion {
    fn new(
        events: &std::sync::mpsc::Sender<DfsMetadataAuthorityFanoutSignal>,
        target_node_id: NodeId,
        routed: bool,
    ) -> Self {
        Self {
            events: Some(events.clone()),
            target_node_id,
            routed,
        }
    }

    fn direct(mut self, result: Result<DfsMetadataJournalBatchResponse, NetworkError>) {
        if let Some(events) = self.events.take() {
            let _ = events.send(Some(DfsMetadataAuthorityFanoutEvent::Direct {
                target_node_id: self.target_node_id.clone(),
                result,
            }));
        }
    }

    fn routed(mut self, result: Result<Vec<DfsRoutedMessage>, NetworkError>) {
        if let Some(events) = self.events.take() {
            let _ = events.send(Some(DfsMetadataAuthorityFanoutEvent::Routed {
                target_node_id: self.target_node_id.clone(),
                result,
            }));
        }
    }
}

impl Drop for DfsMetadataAuthorityFanoutCompletion {
    fn drop(&mut self) {
        let Some(events) = self.events.take() else {
            return;
        };
        let target_node_id = self.target_node_id.clone();
        let error = NetworkError::Io {
            operation: format!("DFS metadata authority call to {target_node_id}"),
            detail: "fan-out worker ended without a result".to_string(),
        };
        let _ = events.send(Some(if self.routed {
            DfsMetadataAuthorityFanoutEvent::Routed {
                target_node_id,
                result: Err(error),
            }
        } else {
            DfsMetadataAuthorityFanoutEvent::Direct {
                target_node_id,
                result: Err(error),
            }
        }));
    }
}

fn metadata_authority_fanout_deadline_error(target_node_id: &NodeId) -> NetworkError {
    NetworkError::Io {
        operation: format!("DFS metadata authority call to {target_node_id}"),
        detail: "DFS operation deadline expired".to_string(),
    }
}

fn spawn_metadata_authority_fanout_thread(task: impl FnOnce() + Send + 'static) {
    let task = sinomo_core::service::observed_blocking_task("dfs.metadata.fanout", task);
    // A failed spawn drops the task; its completion reports the failure.
    let _ = std::thread::Builder::new()
        .name("dfs-metadata-authority-fanout".to_string())
        .spawn(task);
}

impl DfsNodeMetadataAuthorityFanoutWorkers {
    /// Start one direct call. Returns `false` when no blocking worker slot is
    /// free, leaving that target without a direct route for this round.
    /// Start one direct call. With a caller deadline the call's whole network
    /// time is bounded by it; without one only the network layer's own
    /// connect and no-progress limits apply.
    fn start_direct(
        &mut self,
        mut prepared: DfsPreparedMetadataJournalBatchRpc,
        deadline: Option<Instant>,
        events: &std::sync::mpsc::Sender<DfsMetadataAuthorityFanoutSignal>,
    ) -> bool {
        let target_node_id = prepared.target_node_id.clone();
        match &mut self.mode {
            DfsNodeMetadataAuthorityFanoutMode::Async {
                runtime,
                peer_transport,
                direct_slots,
                tasks,
                ..
            } => {
                let completion =
                    DfsMetadataAuthorityFanoutCompletion::new(events, target_node_id, false);
                let peer_transport = Arc::clone(peer_transport);
                let direct_slots = Arc::clone(direct_slots);
                let task = runtime.spawn(
                    async move {
                        let call = async {
                            let _slot = direct_slots.acquire_owned().await.map_err(|_| {
                                metadata_authority_fanout_deadline_error(&prepared.target_node_id)
                            })?;
                            if let Some(deadline) = deadline {
                                let remaining = deadline
                                    .checked_duration_since(Instant::now())
                                    .filter(|remaining| !remaining.is_zero())
                                    .ok_or_else(|| {
                                        metadata_authority_fanout_deadline_error(
                                            &prepared.target_node_id,
                                        )
                                    })?;
                                prepared.options.overall_timeout = Some(remaining);
                            }
                            call_direct_metadata_journal_rpc_async(peer_transport, prepared).await
                        };
                        let result = match deadline {
                            Some(deadline) => tokio::time::timeout_at(
                                tokio::time::Instant::from_std(deadline),
                                call,
                            )
                            .await
                            .unwrap_or_else(|_| {
                                Err(metadata_authority_fanout_deadline_error(
                                    &completion.target_node_id,
                                ))
                            }),
                            None => call.await,
                        };
                        completion.direct(result.map(|(_, response)| response));
                    }
                    .in_current_span(),
                );
                tasks.push(task.abort_handle());
                true
            }
            DfsNodeMetadataAuthorityFanoutMode::Blocking {
                peer_transport,
                worker_slots,
                ..
            } => {
                let Some(worker_slot) = BlockingFanoutWorkerSlot::try_acquire(
                    Arc::clone(worker_slots),
                    DFS_METADATA_AUTHORITY_DIRECT_MAX_INFLIGHT,
                ) else {
                    dfs_perf_trace(format_args!(
                        "direct-metadata-journal-fanout-blocking-worker-cap active={}",
                        worker_slots.load(AtomicOrdering::Acquire)
                    ));
                    return false;
                };
                let completion =
                    DfsMetadataAuthorityFanoutCompletion::new(events, target_node_id, false);
                let peer_transport = Arc::clone(peer_transport);
                spawn_metadata_authority_fanout_thread(move || {
                    let _worker_slot = worker_slot;
                    let remaining = deadline.map(|deadline| {
                        deadline
                            .checked_duration_since(Instant::now())
                            .filter(|remaining| !remaining.is_zero())
                    });
                    let result = match remaining {
                        Some(None) => Err(metadata_authority_fanout_deadline_error(
                            &prepared.target_node_id,
                        )),
                        Some(Some(remaining)) => {
                            prepared.options.overall_timeout = Some(remaining);
                            call_direct_metadata_journal_rpc_blocking(&*peer_transport, prepared)
                        }
                        None => {
                            call_direct_metadata_journal_rpc_blocking(&*peer_transport, prepared)
                        }
                    };
                    completion.direct(result.map(|(_, response)| response));
                });
                true
            }
        }
    }
}

impl DfsMetadataAuthorityFanoutWorkers for DfsNodeMetadataAuthorityFanoutWorkers {
    fn start_routed(
        &mut self,
        message: DfsRoutedMessage,
        deadline: Option<Instant>,
        events: &std::sync::mpsc::Sender<DfsMetadataAuthorityFanoutSignal>,
    ) -> Result<(), DfsRoutedMessage> {
        let target_node_id = message.target_node_id.clone();
        let operation = format!("send DFS message to {target_node_id}");
        let options = DfsObjectRpcSendOptions::new(self.connect_timeout);
        match &mut self.mode {
            DfsNodeMetadataAuthorityFanoutMode::Async {
                runtime,
                sender,
                tasks,
                ..
            } => {
                let Some(sender) = sender.clone() else {
                    return Err(message);
                };
                let completion =
                    DfsMetadataAuthorityFanoutCompletion::new(events, target_node_id, true);
                let task = runtime.spawn(
                    async move {
                        let call = async {
                            let options =
                                DfsNodeTransport::clamp_send_options_to_operation_deadline(
                                    options, deadline, operation,
                                )?;
                            send_dfs_routed_message_to_peer_async(
                                sender,
                                options,
                                message.target_node_id.clone(),
                                message,
                            )
                            .await
                        };
                        let result = match deadline {
                            Some(deadline) => tokio::time::timeout_at(
                                tokio::time::Instant::from_std(deadline),
                                call,
                            )
                            .await
                            .unwrap_or_else(|_| {
                                Err(metadata_authority_fanout_deadline_error(
                                    &completion.target_node_id,
                                ))
                            }),
                            None => call.await,
                        };
                        completion.routed(result);
                    }
                    .in_current_span(),
                );
                tasks.push(task.abort_handle());
                Ok(())
            }
            DfsNodeMetadataAuthorityFanoutMode::Blocking {
                sender,
                worker_slots,
                ..
            } => {
                let Some(sender) = sender.clone() else {
                    return Err(message);
                };
                let Some(worker_slot) = BlockingFanoutWorkerSlot::try_acquire(
                    Arc::clone(worker_slots),
                    DFS_METADATA_AUTHORITY_DIRECT_MAX_INFLIGHT,
                ) else {
                    dfs_perf_trace(format_args!(
                        "direct-metadata-journal-fanout-blocking-worker-cap active={}",
                        worker_slots.load(AtomicOrdering::Acquire)
                    ));
                    return Err(message);
                };
                let completion =
                    DfsMetadataAuthorityFanoutCompletion::new(events, target_node_id, true);
                // The routed sender records native attempts from its calling
                // thread; carry the caller's observation interest to the worker.
                #[cfg(any(test, feature = "test-support"))]
                let native_context = crate::native_shard_trace::rpc_attempt_context();
                spawn_metadata_authority_fanout_thread(move || {
                    let _worker_slot = worker_slot;
                    #[cfg(any(test, feature = "test-support"))]
                    let _native_context =
                        crate::native_shard_trace::enter_rpc_attempt_context(native_context);
                    let result = DfsNodeTransport::clamp_send_options_to_operation_deadline(
                        options, deadline, operation,
                    )
                    .and_then(|options| {
                        send_dfs_routed_message_to_peer(
                            sender,
                            options,
                            message.target_node_id.clone(),
                            message,
                        )
                    });
                    completion.routed(result);
                });
                Ok(())
            }
        }
    }

    fn abort(&mut self) {
        if let DfsNodeMetadataAuthorityFanoutMode::Async { tasks, .. } = &mut self.mode {
            for task in tasks.drain(..) {
                task.abort();
            }
        }
    }
}

impl DfsNodeTransport {
    /// One explicit round over an already initialized three-member scope.
    /// Both commands precede dispatch; every invoked peer gets exactly one R63
    /// call under the SAME absolute deadline. No input partial can be adopted.
    #[cfg(test)]
    pub(crate) fn capture_metadata_prefix_full_assignment_round(
        &mut self,
        target: &crate::DfsMetadataQuorumSyncTarget,
    ) -> Result<OwnedNativeMetadataPrefixRound, NetworkError> {
        let invalid = |detail: &str| NetworkError::Serialization {
            detail: format!("native prefix round: {detail}"),
        };
        let assignment = target
            .expected_shard
            .as_ref()
            .ok_or_else(|| invalid("missing exact assignment"))?;
        let expected = assignment
            .replica_node_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let supplied = target
            .replica_node_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        if target.primary_node_id != self.local_node_id
            || assignment.primary_node_id != self.local_node_id
            || assignment.file_system_id != target.file_system_id
            || assignment.shard_id != target.shard_id.0
            || assignment.epoch == 0
            || target.authority_epoch != Some(assignment.epoch)
            || target.barrier_id.is_empty()
            || target.state_fingerprint.is_empty()
            || target.checkpoint_snapshot.is_some()
            || assignment.replica_node_ids.len() != 2
            || target.replica_node_ids.len() != 2
            || expected.len() != 2
            || supplied != expected
            || expected.contains(&self.local_node_id)
        {
            return Err(invalid(
                "unsupported, duplicate, subset or inconsistent three-member target",
            ));
        }
        for peer in &expected {
            if !self.known_remote_nodes.contains(peer) {
                return Err(NetworkError::UnknownNode {
                    node_id: peer.clone(),
                });
            }
        }
        let deadline = self
            .operation_deadline
            .filter(|deadline| *deadline > Instant::now())
            .ok_or_else(|| invalid("missing or expired original operation deadline"))?;
        let round_id = sinomo_core::Id::generate().to_string();
        let peers = expected
            .iter()
            .enumerate()
            .map(|(index, peer)| DfsNativePrefixRoundPeer {
                peer: peer.clone(),
                command: DfsMetadataJournalBatchCommand {
                    request_id: format!("dfs_native_prefix_round_{round_id}_{index}"),
                    source_node_id: Some(self.local_node_id.clone()),
                    file_system_id: Some(target.file_system_id.clone()),
                    authority_epoch: target.authority_epoch,
                    verify_prefix_only: true,
                    prefix_probe: Some(crate::DfsMetadataPrefixProbe {
                        barrier_id: format!("{}-{round_id}", target.barrier_id),
                        file_system_id: Some(target.file_system_id.clone()),
                        authority_epoch: target.authority_epoch,
                        shard_id: target.shard_id.clone(),
                        journal_sequence: target.journal_sequence,
                        state_fingerprint: target.state_fingerprint.clone(),
                        primary_handoff_id: None,
                        write_set_generation: None,
                        epoch_start_alignment: None,
                    }),
                    authority_probe: None,
                    durable_replication: None,
                    batch: crate::DfsMetadataJournalFrameBatch {
                        shard_id: target.shard_id.clone(),
                        journal_frames: Vec::new(),
                        next_after_sequence: target.journal_sequence,
                    },
                },
                status: DfsNativePrefixRoundPeerStatus::Unattempted,
                attempted: false,
                response: None,
                native_return: None,
                error: None,
                unattempted_reason: Some("peer invocation has not begun".into()),
            })
            .collect::<Vec<_>>();
        let mut round = OwnedNativeMetadataPrefixRound {
            round_id,
            target: target.clone(),
            deadline,
            peers,
            common_source: None,
            finalization_error: None,
        };
        let mut native_attempts = BTreeSet::new();
        for slot in &mut round.peers {
            if Instant::now() >= deadline {
                slot.unattempted_reason =
                    Some("original round deadline expired before this peer invocation".into());
                continue;
            }
            slot.attempted = true;
            slot.unattempted_reason = None;
            match self.capture_metadata_prefix_probe_direct(&slot.peer, &slot.command) {
                Err(error) => {
                    slot.status = DfsNativePrefixRoundPeerStatus::Failed;
                    slot.error = Some(error);
                }
                Ok(returned) => {
                    slot.response = Some(returned.response);
                    match returned.evidence {
                        DfsMetadataPrefixEvidence::NoNativeProof => {
                            slot.status = DfsNativePrefixRoundPeerStatus::NoNativeProof
                        }
                        DfsMetadataPrefixEvidence::Native(value) => {
                            let valid = value.peer() == &slot.peer
                                && value.command() == &slot.command
                                && value.response()
                                    == slot.response.as_ref().expect("actual response retained")
                                && value.source_assignment() == assignment
                                && value.source_prefix().is_well_formed()
                                && value.source_prefix().journal_sequence
                                    == target.journal_sequence
                                && value.source_prefix().digest == target.state_fingerprint
                                && native_attempts.insert(value.attempt_id().to_owned());
                            slot.status = if valid {
                                DfsNativePrefixRoundPeerStatus::NativeCaptured
                            } else {
                                DfsNativePrefixRoundPeerStatus::NativeBindingRejected
                            };
                            if !valid {
                                slot.error = Some(invalid(
                                    "native value differs from its fixed peer/command/source or duplicates another attempt",
                                ));
                            }
                            // Preserve genuine captured DATA even if it cannot
                            // qualify this slot or a later whole-round check.
                            slot.native_return = Some(value);
                        }
                    }
                }
            }
        }
        let completed = (|| -> Result<DfsNativePrefixRoundSourceObservation, NetworkError> {
            if Instant::now() >= deadline {
                return Err(invalid(
                    "original whole-round deadline expired; observations retained",
                ));
            }
            let actual = round
                .peers
                .iter()
                .map(|slot| slot.peer.clone())
                .collect::<BTreeSet<_>>();
            if actual != expected
                || round.peers.len() != expected.len()
                || round.peers.iter().any(|slot| {
                    slot.status != DfsNativePrefixRoundPeerStatus::NativeCaptured
                        || slot.error.is_some()
                })
            {
                return Err(invalid(
                    "not every configured remote peer supplied qualifying native evidence in this round",
                ));
            }
            let first = round.peers[0]
                .native_return
                .as_ref()
                .expect("qualified native slot");
            let common = DfsNativePrefixRoundSourceObservation {
                assignment: first.source_assignment().clone(),
                prefix: first.source_prefix().clone(),
                physical: first.local_physical_owner().clone(),
            };
            for slot in &round.peers {
                let value = slot.native_return.as_ref().expect("qualified native slot");
                if value.source_assignment() != &common.assignment
                    || value.source_prefix() != &common.prefix
                    || value.local_physical_owner() != &common.physical
                {
                    return Err(invalid(
                        "native peer returns have mixed source assignment/prefix/physical observations",
                    ));
                }
                value.validate_binding(&slot.peer, &slot.command, &common.physical.data_root)?;
            }
            if Instant::now() >= deadline {
                return Err(invalid(
                    "final binding checks crossed the original whole-round deadline",
                ));
            }
            Ok(common)
        })();
        match completed {
            Ok(common) => round.common_source = Some(common),
            Err(error) => round.finalization_error = Some(error),
        }
        Ok(round)
    }

    /// Opt-in, one-call direct PREFIX producer. Routed/fanout output remains
    /// DATA; no second RPC is issued to obtain provenance for an earlier call.
    #[cfg(test)]
    pub(crate) fn capture_metadata_prefix_probe_direct(
        &mut self,
        peer: &NodeId,
        command: &DfsMetadataJournalBatchCommand,
    ) -> Result<DfsMetadataPrefixProbeReturn, NetworkError> {
        validate_ordinary_metadata_prefix_command(command).map_err(|error| {
            NetworkError::Serialization {
                detail: error.to_string(),
            }
        })?;
        if peer == &self.local_node_id
            || !self.known_remote_nodes.contains(peer)
            || command.source_node_id.as_ref() != Some(&self.local_node_id)
        {
            return Err(NetworkError::Serialization {
                detail: "metadata prefix attempt has a foreign local source or target peer".into(),
            });
        }
        let deadline = self
            .operation_deadline
            .filter(|deadline| *deadline > Instant::now())
            .ok_or_else(|| NetworkError::Serialization {
                detail: "metadata prefix capture requires its existing operation deadline".into(),
            })?;
        let options = self.current_send_options("capture one native metadata prefix return")?;
        let transport =
            self.peer_transport
                .as_ref()
                .ok_or_else(|| NetworkError::Serialization {
                    detail: "metadata prefix capture has no direct peer transport".into(),
                })?;
        let attempt = DfsMetadataPrefixProbeAttempt {
            id: sinomo_core::Id::generate().to_string(),
            peer: peer.clone(),
            command: command.clone(),
            deadline,
        };
        let result = transport
            .call_metadata_prefix_probe(&attempt, options)
            .map_err(|error| match error.delivery_state() {
                Some(state) => NetworkError::delivery_failed(
                    "capture direct metadata prefix return",
                    state,
                    error.to_string(),
                ),
                None => NetworkError::Io {
                    operation: "capture direct metadata prefix return".into(),
                    detail: error.to_string(),
                },
            })?;
        if Instant::now() >= deadline {
            return Err(NetworkError::delivery_failed(
                "capture direct metadata prefix return",
                sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
                "metadata prefix capture completed after its original deadline",
            ));
        }
        if result.response.request_id != command.request_id
            || result.response.shard_id != command.batch.shard_id
            || result.response.next_after_sequence != command.batch.next_after_sequence
        {
            return Err(NetworkError::Serialization {
                detail: "metadata prefix producer received response DATA for another attempt"
                    .into(),
            });
        }
        if let DfsMetadataPrefixEvidence::Native(value) = &result.evidence {
            value.validate_attempt_binding(&attempt, &result.response)?;
        }
        Ok(result)
    }

    fn send_metadata_journal_batches_direct_inner(
        &mut self,
        requests: &[(NodeId, DfsMetadataJournalBatchCommand)],
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
        self.send_borrowed_metadata_journal_batch_groups_direct_inner(&groups)
    }

    /// Validate every target and encode each group once. Returns `None` when
    /// `deadline`, if any, expires before every call is prepared.
    fn prepare_metadata_journal_batch_rpcs(
        &self,
        groups: &[DfsBorrowedMetadataJournalBatchDirectGroup<'_>],
        deadline: Option<Instant>,
    ) -> Result<Option<Vec<DfsPreparedMetadataJournalBatchRpc>>, NetworkError> {
        let expired = || deadline.is_some_and(|deadline| Instant::now() >= deadline);
        let request_count = groups
            .iter()
            .map(|group| group.target_node_ids.len())
            .sum::<usize>();
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
                    dfs_perf_trace(format_args!(
                        "direct-metadata-journal-target-unavailable local={} target={} known={:?}",
                        self.local_node_id, target_node_id, self.known_remote_nodes,
                    ));
                    return Err(NetworkError::UnknownNode {
                        node_id: (*target_node_id).clone(),
                    });
                }
                if expired() {
                    return Ok(None);
                }
            }
            #[cfg(test)]
            {
                let delay = take_metadata_direct_encoding_test_delay();
                if !delay.is_zero() {
                    std::thread::sleep(delay);
                }
            }
            if expired() {
                return Ok(None);
            }
            let replayable_call = Arc::new(dfs_metadata_journal_batch_replayable_rpc_call(
                group.command,
            )?);
            #[cfg(any(test, feature = "test-support"))]
            let native_attempt_context = crate::native_shard_trace::rpc_attempt_context();
            #[cfg(any(test, feature = "test-support"))]
            let native_command = native_attempt_context
                .as_ref()
                .map(|_| Arc::new(group.command.clone()));
            distinct_value_count = distinct_value_count.saturating_add(1);
            encoded_value_bytes =
                encoded_value_bytes.saturating_add(replayable_call.encoded_value_bytes());
            for target_node_id in &group.target_node_ids {
                if expired() {
                    return Ok(None);
                }
                let options = self.current_send_options(format!(
                    "send direct DFS metadata journal batch to {target_node_id}"
                ))?;
                prepared.push(DfsPreparedMetadataJournalBatchRpc {
                    #[cfg(any(test, feature = "test-support"))]
                    native_attempt_context: native_attempt_context.clone(),
                    #[cfg(any(test, feature = "test-support"))]
                    native_command: native_command.clone(),
                    #[cfg(test)]
                    test_probe: self.test_metadata_direct_probe.clone(),
                    target_node_id: (*target_node_id).clone(),
                    request_id: group.command.request_id.clone(),
                    shard_id: group.command.batch.shard_id.clone(),
                    next_after_sequence: group.command.batch.next_after_sequence,
                    authority_fanout: group.command.authority_probe.is_some(),
                    replayable_call: Arc::clone(&replayable_call),
                    upload_progress: None,
                    options,
                });
            }
        }
        dfs_perf_trace(format_args!(
            "direct-metadata-journal-fanout-encoding authority={} requests={} distinct_values={} encoded_bytes={}",
            !prepared.is_empty() && prepared.iter().all(|request| request.authority_fanout),
            prepared.len(),
            distinct_value_count,
            encoded_value_bytes,
        ));
        Ok(Some(prepared))
    }

    fn send_borrowed_metadata_journal_batch_groups_direct_inner(
        &mut self,
        groups: &[DfsBorrowedMetadataJournalBatchDirectGroup<'_>],
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
        // This best-effort batch API keeps one whole-method budget covering
        // local preparation and the peer replies. An installed operation
        // deadline can only shorten it.
        let fanout_deadline = Instant::now()
            .checked_add(fanout_budget)
            .unwrap_or_else(Instant::now);
        let deadline = self
            .operation_deadline
            .map_or(fanout_deadline, |deadline| deadline.min(fanout_deadline));
        let Some(mut prepared) =
            self.prepare_metadata_journal_batch_rpcs(groups, Some(deadline))?
        else {
            return Ok(Some(Vec::new()));
        };
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

    fn start_metadata_authority_fanout_inner(
        &mut self,
        groups: &[DfsMetadataJournalBatchDirectGroup],
        deadline: Option<Instant>,
    ) -> Result<Option<DfsMetadataAuthorityFanout>, NetworkError> {
        let mode = match (
            self.async_peer_transport.as_ref(),
            tokio::runtime::Handle::try_current(),
        ) {
            (Some(peer_transport), Ok(runtime))
                if matches!(
                    runtime.runtime_flavor(),
                    tokio::runtime::RuntimeFlavor::MultiThread
                ) =>
            {
                DfsNodeMetadataAuthorityFanoutMode::Async {
                    runtime,
                    peer_transport: Arc::clone(peer_transport),
                    sender: self.async_sender.clone(),
                    direct_slots: Arc::new(tokio::sync::Semaphore::new(
                        DFS_METADATA_AUTHORITY_DIRECT_MAX_INFLIGHT,
                    )),
                    tasks: Vec::new(),
                }
            }
            _ => {
                let Some(peer_transport) = self.peer_transport.as_ref() else {
                    return Ok(None);
                };
                DfsNodeMetadataAuthorityFanoutMode::Blocking {
                    peer_transport: Arc::clone(peer_transport),
                    sender: self.sender.clone(),
                    worker_slots: Arc::clone(&self.blocking_metadata_fanout_workers),
                }
            }
        };
        let groups = groups
            .iter()
            .map(|group| DfsBorrowedMetadataJournalBatchDirectGroup {
                target_node_ids: group.target_node_ids.iter().collect(),
                command: &group.command,
            })
            .collect::<Vec<_>>();
        let prepared = self.prepare_metadata_journal_batch_rpcs(&groups, deadline)?;
        let (events_tx, events) = std::sync::mpsc::channel();
        let mut workers = DfsNodeMetadataAuthorityFanoutWorkers {
            connect_timeout: self.connect_timeout,
            mode,
        };
        let mut direct_calls = BTreeMap::new();
        // An expired preparation starts no call; the caller's deadline check
        // then ends the round.
        for mut prepared in prepared.into_iter().flatten() {
            let target_node_id = prepared.target_node_id.clone();
            let upload_progress = sinomo_core::network::ObjectRpcBodyProgress::new();
            prepared.upload_progress = Some(upload_progress.clone());
            if workers.start_direct(prepared, deadline, &events_tx) {
                direct_calls.insert(target_node_id, upload_progress);
            }
        }
        Ok(Some(DfsMetadataAuthorityFanout::new(
            deadline,
            direct_calls,
            events_tx,
            events,
            Box::new(workers),
        )))
    }
}

impl DfsMessageTransport for DfsNodeTransport {
    fn send_local_metadata_query_to_main(
        &mut self,
        message: DfsRoutedMessage,
    ) -> Result<(), NetworkError> {
        if message.source_node_id != self.local_node_id
            || message.target_node_id != self.local_node_id
            || !matches!(&message.message,
                DfsNetworkMessage::NamespaceRequest(command)
                if command.requesting_node_id == self.local_node_id
                    && !command.include_metadata_journal_batches
                    && command.metadata_journal_after_sequences.is_empty()
                    && matches!(command.operation, crate::DfsNamespaceOperation::MetadataQuery { .. }))
        {
            return Err(NetworkError::Io {
                operation: "local metadata query to Main".into(),
                detail: "only an exact local read-only metadata query may use Main dispatch".into(),
            });
        }
        let sender = self
            .sender
            .as_ref()
            .cloned()
            .ok_or_else(|| NetworkError::Io {
                operation: "local metadata query to Main".into(),
                detail: "registered Main query has no authenticated sender".into(),
            })?;
        // The explicit self-source/self-target checks above select registered
        // Main. A remote-peer membership check cannot validate this route:
        // known_remote_nodes deliberately excludes the local endpoint.
        let options = self.current_send_options("local metadata query to Main")?;
        let replies =
            send_dfs_routed_message_to_peer(sender, options, self.local_node_id.clone(), message)?;
        // Preserve original frame/source custody. The query waiter checks the
        // exact request/source/destination and current authority as usual.
        self.incoming_messages.extend(replies);
        Ok(())
    }

    fn delegates_local_namespace_authority(&self) -> bool {
        self.served_message.is_none()
            && self.local_namespace_dispatch == LocalNamespaceDispatch::RegisteredMain
    }
    fn take_namespace_response(
        &mut self,
        request_id: &str,
    ) -> Result<Option<DfsRoutedMessage>, NetworkError> {
        if let Some(served) = self.served_message.as_mut()
            && let Some(position) = served.inbound_messages.iter().position(|message| {
                matches!(&message.message, DfsNetworkMessage::NamespaceResponse(response)
                    if response.request_id == request_id)
            })
        {
            return Ok(served.inbound_messages.remove(position));
        }
        if let Some(inbox) = &self.response_inbox {
            self.incoming_messages.extend(inbox.drain()?);
        }
        let position = self.incoming_messages.iter().position(|message| {
            matches!(&message.message, DfsNetworkMessage::NamespaceResponse(response)
                if response.request_id == request_id)
        });
        Ok(position.and_then(|position| self.incoming_messages.remove(position)))
    }
    fn register_routed_write_response_owner(
        &mut self,
        canonical: &str,
        object_id: &crate::DfsObjectId,
        aliases: &[String],
    ) -> Result<(), NetworkError> {
        if self.served_message.is_some() {
            return Ok(());
        }
        let (Some(owner), Some(inbox)) = (&self.response_delivery, &self.response_inbox) else {
            return Ok(());
        };
        if let Some(lease) = owner.register(inbox, canonical, object_id, aliases)? {
            self.response_registrations
                .entry(canonical.to_owned())
                .or_default()
                .push(lease);
        }
        Ok(())
    }
    fn release_routed_write_response_owner(&mut self, canonical: &str) {
        if self.served_message.is_some() {
            return;
        }
        self.response_registrations.remove(canonical);
    }
    fn routed_write_response_registration_checkpoint(&self, canonical: &str) -> usize {
        if self.served_message.is_some() {
            return 0;
        }
        self.response_registrations
            .get(canonical)
            .map_or(0, Vec::len)
    }
    fn rollback_routed_write_response_registration(&mut self, canonical: &str, checkpoint: usize) {
        if self.served_message.is_some() {
            return;
        }
        if let Some(leases) = self.response_registrations.get_mut(canonical) {
            leases.truncate(checkpoint);
        }
        if self
            .response_registrations
            .get(canonical)
            .is_some_and(Vec::is_empty)
        {
            self.response_registrations.remove(canonical);
        }
    }
    fn incoming_wait_waker(&self) -> Option<crate::service_state::DfsCancellationWaker> {
        let interrupt = Arc::downgrade(&self.incoming_wait_interrupt);
        let inbox = self
            .response_inbox
            .as_ref()
            .map(|inbox| inbox.interrupt_waker());
        Some(Arc::new(move || {
            if let Some(interrupt) = interrupt.upgrade() {
                interrupt.interrupt();
            }
            if let Some(inbox) = &inbox {
                inbox();
            }
        }))
    }
    fn set_incoming_arrival_waker(
        &mut self,
        waker: Option<crate::service_state::DfsCancellationWaker>,
    ) {
        // Only the response inbox receives from another thread; every other
        // message of this transport arrives through its own sends.
        if let Some(inbox) = &self.response_inbox {
            inbox.set_arrival_waker(waker);
        }
    }
    fn post_hint_to_local_main(&mut self, message: DfsRoutedMessage) -> Result<(), NetworkError> {
        let target_node_id = self.local_node_id.clone();
        if self.served_message.is_some()
            || !self.routes_local_request_to_main(&target_node_id, &message.message)
        {
            return self.send_routed_call(&target_node_id, DfsRoutedCall::value(message));
        }
        self.validate_routed_message_target(&target_node_id, &message, true)?;
        let sender = self
            .sender
            .as_ref()
            .cloned()
            .ok_or_else(|| NetworkError::Io {
                operation: format!("post DFS hint to Main {target_node_id}"),
                detail: "registered Main request has no actual sender".into(),
            })?;
        if self
            .hint_to_main_in_flight
            .swap(true, AtomicOrdering::AcqRel)
        {
            return Ok(());
        }
        // The hint is not part of its sender's call: it carries neither that
        // call's deadline nor its cancellation, and its answer is dropped.
        let options = DfsObjectRpcSendOptions::new(self.connect_timeout);
        let in_flight = Arc::clone(&self.hint_to_main_in_flight);
        let posted = std::thread::Builder::new()
            .name("sinomo-dfs-hint".into())
            .spawn({
                let in_flight = Arc::clone(&in_flight);
                move || {
                    let _ =
                        send_dfs_routed_message_to_peer(sender, options, target_node_id, message);
                    in_flight.store(false, AtomicOrdering::Release);
                }
            });
        if let Err(error) = posted {
            in_flight.store(false, AtomicOrdering::Release);
            return Err(NetworkError::Io {
                operation: "post DFS hint to Main".into(),
                detail: error.to_string(),
            });
        }
        Ok(())
    }
    fn wait_for_incoming(&self, timeout: Duration) -> Result<bool, NetworkError> {
        if self.served_message.is_some() {
            // Preserve the existing served transport's default wait contract.
            if DfsMessageTransport::pending_incoming_count(self)?.is_some_and(|count| count > 0) {
                return Ok(true);
            }
            if !timeout.is_zero() {
                self.incoming_wait_interrupt.wait(timeout);
            }
            return Ok(
                DfsMessageTransport::pending_incoming_count(self)?.is_some_and(|count| count > 0)
            );
        }
        if self.owned_incoming_count()? > 0 {
            return Ok(true);
        }
        if let Some(inbox) = &self.response_inbox {
            return inbox.wait(timeout);
        }
        if !timeout.is_zero() {
            self.incoming_wait_interrupt.wait(timeout);
        }
        Ok(self.owned_incoming_count()? > 0)
    }
    fn send_routed_call(
        &mut self,
        target_node_id: &NodeId,
        call: DfsRoutedCall,
    ) -> Result<(), NetworkError> {
        #[cfg(test)]
        if let Some(attempts) = &self.test_routed_call_attempts {
            attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        if self.served_message.is_some() {
            return self.send_served_routed_call(target_node_id, call);
        }
        self.send_routed_call_to_peer(
            target_node_id,
            call,
            true,
            #[cfg(any(test, feature = "test-support"))]
            crate::native_shard_trace::rpc_attempt_context(),
        )
    }
    fn send_routed_calls_bounded(
        &mut self,
        requests: Vec<DfsRoutedSend>,
        max_inflight: usize,
    ) -> Result<Vec<DfsRoutedSendOutcome>, NetworkError> {
        if self.served_message.is_some() {
            self.send_served_calls_bounded(requests, max_inflight)
        } else {
            self.send_peer_calls_bounded(
                requests,
                max_inflight,
                #[cfg(any(test, feature = "test-support"))]
                crate::native_shard_trace::rpc_attempt_context(),
            )
        }
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
        #[cfg(any(test, feature = "test-support"))]
        let mut native_attempt =
            crate::native_shard_trace::rpc_attempt_context().and_then(|context| {
                context.begin_direct_journal(
                    target_node_id,
                    &call.request.route,
                    Arc::new(command.clone()),
                )
            });
        let response = peer_transport
            .call_object_rpc(target_node_id, call, options)
            .map_err(|source| {
                #[cfg(any(test, feature = "test-support"))]
                if let Some(attempt) = native_attempt.as_mut() {
                    attempt.returned_direct_journal(
                        None,
                        Some(source.to_string()),
                        source.delivery_state(),
                    );
                }
                NetworkError::Io {
                    operation: format!(
                        "send direct DFS metadata journal batch to {target_node_id}"
                    ),
                    detail: source.to_string(),
                }
            })?;
        let response = value_response_from_rpc_result(response).map_err(|source| {
            #[cfg(any(test, feature = "test-support"))]
            if let Some(attempt) = native_attempt.as_mut() {
                attempt.returned_direct_journal(
                    None,
                    Some(source.to_string()),
                    source.delivery_state(),
                );
            }
            NetworkError::Io {
                operation: format!(
                    "decode direct DFS metadata journal response from {target_node_id}"
                ),
                detail: source.to_string(),
            }
        })?;
        let response = decode_dfs_metadata_journal_batch_rpc_response(&response);
        #[cfg(any(test, feature = "test-support"))]
        if let Err(source) = &response
            && let Some(attempt) = native_attempt.as_mut()
        {
            attempt.returned_direct_journal(
                None,
                Some(source.to_string()),
                source.delivery_state(),
            );
        }
        let response = response?;
        #[cfg(any(test, feature = "test-support"))]
        if let Some(attempt) = native_attempt.as_mut() {
            attempt.returned_direct_journal(Some(&response), None, None);
        }
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
        self.send_metadata_journal_batches_direct_inner(requests)
    }
    fn start_metadata_authority_fanout(
        &mut self,
        groups: &[DfsMetadataJournalBatchDirectGroup],
        deadline: Option<Instant>,
    ) -> Result<Option<DfsMetadataAuthorityFanout>, NetworkError> {
        self.start_metadata_authority_fanout_inner(groups, deadline)
    }
    fn send_routed_message_on_metadata_authority_fanout(
        &mut self,
        fanout: &mut DfsMetadataAuthorityFanout,
        message: DfsRoutedMessage,
    ) -> Result<Option<DfsRoutedMessage>, NetworkError> {
        let target_node_id = message.target_node_id.clone();
        // Local delivery and served-caller returns keep their ordinary send.
        if target_node_id == self.local_node_id
            || self.served_message.as_ref().is_some_and(|served| {
                served.return_node_id.as_ref() == Some(&target_node_id)
                    && dfs_network_message_can_return_to_served_caller(
                        &message.message,
                        served.caller_fetch_before_reply,
                    )
            })
        {
            return Ok(Some(message));
        }
        self.validate_routed_message_target(&target_node_id, &message, true)?;
        #[cfg(test)]
        let observed = match &message.message {
            DfsNetworkMessage::MetadataJournalBatch(command) => self
                .test_metadata_authority_routed_sends
                .clone()
                .map(|sends| (sends, command.batch.journal_frames.len())),
            _ => None,
        };
        let returned = fanout.start_routed(message).err();
        #[cfg(test)]
        if returned.is_none()
            && let Some((sends, frames)) = observed
        {
            sends
                .lock()
                .expect("authority routed send observations")
                .push((target_node_id, frames));
        }
        Ok(returned)
    }
    fn deliver_metadata_authority_fanout_replies(
        &mut self,
        messages: Vec<DfsRoutedMessage>,
    ) -> Result<(), NetworkError> {
        // Same custody as the replies returned by an ordinary routed send.
        for message in messages {
            let target_node_id = message.target_node_id.clone();
            self.send_routed_message_to_peer(&target_node_id, message, false)?;
        }
        Ok(())
    }
    fn drain_incoming_messages(&mut self) -> Result<Vec<DfsRoutedMessage>, NetworkError> {
        let mut messages = self
            .served_message
            .as_mut()
            .map(|served| served.inbound_messages.drain(..).collect::<Vec<_>>())
            .unwrap_or_default();
        messages.extend(self.drain_owned_incoming()?);
        Ok(messages)
    }
    fn flush_detached_sends(&mut self) -> Result<(), NetworkError> {
        self.flush_detached_peer_sends()
    }
    fn can_send_synchronously(&self) -> bool {
        self.served_message.is_some() || self.sender.is_some()
    }
    fn pending_incoming_count(&self) -> Result<Option<usize>, NetworkError> {
        let served = self
            .served_message
            .as_ref()
            .map_or(0, |state| state.inbound_messages.len());
        Ok(Some(served.saturating_add(self.owned_incoming_count()?)))
    }
    fn set_operation_deadline(&mut self, deadline: Option<Instant>) -> Option<Instant> {
        DfsNodeTransport::set_operation_deadline(self, deadline)
    }
    fn set_send_cancellation(&mut self, cancellation: Option<crate::DfsCancellationFlag>) {
        self.send_cancellation = cancellation;
    }
    fn operation_deadline(&self) -> Option<Instant> {
        DfsNodeTransport::operation_deadline(self)
    }
    fn supports_background_replica_dispatch(&self) -> bool {
        self.served_message
            .as_ref()
            .is_some_and(|state| state.background_replica_dispatch)
    }
    fn prepare_background_replica_dispatch(
        &mut self,
        requests: Vec<DfsRoutedSend>,
    ) -> Result<(), NetworkError> {
        if self.served_message.is_none() {
            return Err(NetworkError::Io {
                operation: "prepare background DFS replica dispatch".into(),
                detail: "transport has no detached replica dispatch owner".into(),
            });
        }
        for request in &requests {
            if !matches!(&request.call.message.message,
                DfsNetworkMessage::RepairReplica(command) if command.background_repair)
                && !matches!(
                    &request.call.message.message,
                    DfsNetworkMessage::RepairRetainedBlob(_)
                )
            {
                return Err(NetworkError::Serialization {
                    detail: "detached replica owner carries only executor repairs".to_string(),
                });
            }
            self.validate_routed_message_target(
                &request.target_node_id,
                &request.call.message,
                true,
            )?;
        }
        // These owned readers are private to the admitted producer. An already
        // active outbox watcher cannot observe them before the host detaches
        // this owner and releases mutation admission.
        self.served_mut().prepared_replica_calls.extend(
            requests
                .into_iter()
                .map(|request| DfsCheckpointedReplicaCall { request }),
        );
        Ok(())
    }
    fn checkpoint_background_replica_dispatch(&mut self) -> Result<(), NetworkError> {
        if self.served_message.is_none() {
            return Ok(());
        }
        let checkpoint_owner = Arc::clone(&self.served().checkpointed_replica_calls);
        let mut checkpointed = checkpoint_owner.lock().map_err(|_| NetworkError::Io {
            operation: "checkpoint background DFS replica dispatch".to_string(),
            detail: "checkpointed payload owner is poisoned".to_string(),
        })?;
        checkpointed.extend(std::mem::take(
            &mut self.served_mut().prepared_replica_calls,
        ));
        Ok(())
    }
    fn can_return_committed_foreground_response_before_replica_repair(&self) -> bool {
        self.served_message.is_some()
    }
    fn serves_routed_request(&self) -> bool {
        self.served_message.is_some()
    }
    fn detaches_executor_sends(&self) -> bool {
        // Every admitted lane hands its checkpointed calls and detached
        // outbox to the worker when admission ends, whichever maintenance
        // kind it runs.
        self.served_message.is_some()
    }
    fn should_detach_completed_response(&self, target_node_id: &NodeId) -> bool {
        self.served_message
            .as_ref()
            .and_then(|state| state.return_node_id.as_ref())
            .is_some_and(|return_node_id| return_node_id != target_node_id)
    }
}

impl NetworkedDfsService<DfsNodeTransport> {
    pub fn refresh_peer_targets(&mut self, peer_node_ids: Vec<NodeId>) {
        self.transport.replace_peer_nodes(peer_node_ids);
    }
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    /// The operation deadline its caller installed, if any.
    pub fn transport_operation_deadline(&self) -> Option<Instant> {
        self.transport.operation_deadline()
    }

    /// Install the caller's operation deadline, returning the previous one.
    pub fn set_transport_operation_deadline(
        &mut self,
        deadline: Option<Instant>,
    ) -> Option<Instant> {
        self.transport.set_operation_deadline(deadline)
    }
}

#[cfg(test)]
#[path = "node_transport/native_promotion_tests.rs"]
mod native_promotion_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DFS_CONTENT_ROOT_CATALOG_PREFIX_PROOF_VERSION, DfsContentRootCatalogCoverage,
        DfsContentRootCatalogPage, DfsContentRootCatalogPageRequest,
        DfsContentRootCatalogShardIdentity, DfsDistributedWriteCommand, DfsMetadataShardAssignment,
        DfsMetadataShardConfig, DfsNamespaceCommand, DfsNamespaceOperation, DfsObjectId,
        DfsReadResult, DfsReplicationPolicy, DfsStageBlobCommand, DfsVersion,
        dfs_content_root_catalog_topology_digest, sifs_file_system_id,
    };
    use sinomo_core::{DfsObjectPolicy, DfsWriteRequest};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex, mpsc};

    #[test]
    fn a_mounted_read_budget_includes_waiting_for_streamed_bytes() {
        let (_sender, body) = ObjectRpcIncomingBody::test_channel(Some(1), 1);
        let deadline = Instant::now() + Duration::from_millis(25);
        let _call = crate::MountedSifsCall::enter(Some(deadline));
        let error = collect_dfs_incoming_body_sync(body, 1).expect_err("the body is still open");
        assert_eq!(
            error.delivery_state(),
            Some(sinomo_core::network::NetworkDeliveryState::PossiblyApplied)
        );
    }

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
        let mut transport = DfsNodeTransport::new_with_message_sender_for_nodes(
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
        let mut transport = DfsNodeTransport::new_with_peer_transports_for_nodes(
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

    // Routing/codec controls only: this existing recording peer supplies no
    // content, replica receipt, or durability certificate. The native combined
    // FileSync control owns end-to-end content and authority qualification.
    fn served_file_sync_fetch_fixture() -> (
        DfsNodeTransport,
        Arc<RecordingAsyncMessagePeerTransport>,
        DfsRoutedMessage,
        DfsRoutedMessage,
    ) {
        let local = node_id("node_served_file_sync_authority");
        let caller = node_id("node_served_file_sync_caller");
        let object = DfsObjectId::from_legacy_named("served_file_sync_object").unwrap();
        let version = DfsVersion::from_legacy_named("served_file_sync_version").unwrap();
        let hash = crate::content_hash(b"transport routing bytes only");
        let mut served = test_namespace_message(&caller, &local, 1);
        let DfsNetworkMessage::NamespaceRequest(command) = &mut served.message else {
            unreachable!()
        };
        command.operation = DfsNamespaceOperation::FileSync {
            path: "/services/served-file-sync.bin".into(),
            target: crate::DfsPosixFileSyncTarget {
                kind: crate::DfsPosixSyncKind::File,
                file_system_id: sifs_file_system_id(),
                object_id: object.clone(),
                path: "/services/served-file-sync.bin".into(),
                version: version.clone(),
                content_hash: hash.clone(),
            },
        };
        let peer = Arc::new(RecordingAsyncMessagePeerTransport::default());
        let sync_peer: Arc<dyn DfsPeerTransport> = peer.clone();
        let async_peer: Arc<dyn AsyncDfsPeerTransport> = peer.clone();
        let mut transport = DfsNodeTransport::new_with_peer_transports_for_nodes(
            local.clone(),
            [caller.clone()],
            sync_peer,
            async_peer,
            Duration::from_secs(1),
        );
        transport.begin_message(&served).unwrap();
        let fetch = DfsRoutedMessage {
            source_node_id: local.clone(),
            target_node_id: caller.clone(),
            frame_id: 2,
            message_id: 2,
            message: DfsNetworkMessage::FetchBlob(crate::DfsFetchBlobCommand {
                request_id: "served-file-sync-fetch".into(),
                intent_id: "served-file-sync-intent".into(),
                object_id: object,
                version,
                content_hash: hash,
                authority_shard: None,
            }),
        };
        let response = DfsRoutedMessage {
            source_node_id: local,
            target_node_id: caller,
            frame_id: 3,
            message_id: 3,
            message: DfsNetworkMessage::NamespaceResponse(crate::DfsNamespaceResponse {
                request_id: "namespace_request_1".into(),
                committed_work: Default::default(),
                result: crate::DfsNamespaceResult::Rejected {
                    reason: "routing-only control, no durability evidence".into(),
                    posix_error: None,
                    current_primary_node_id: None,
                },
                synced_path_object: None,
                synced_path_objects: Vec::new(),
                metadata_journal_batches: Vec::new(),
                metadata_journal_has_more: false,
                posix_coordination_result: None,
            }),
        };
        (transport, peer, fetch, response)
    }

    #[test]
    fn file_sync_caller_fetch_sync_single_reaches_peer_before_reply() {
        let (mut transport, peer, fetch, response) = served_file_sync_fetch_fixture();
        let caller = fetch.target_node_id.clone();
        transport
            .send_routed_call(&caller, DfsRoutedCall::value(fetch.clone()))
            .unwrap();
        assert_eq!(
            peer.calls.load(Ordering::SeqCst),
            1,
            "FileSync must dispatch its required caller Fetch before finish_message"
        );
        transport
            .send_routed_call(&caller, DfsRoutedCall::value(response.clone()))
            .unwrap();
        assert_eq!(
            peer.calls.load(Ordering::SeqCst),
            1,
            "NamespaceResponse remains reply-owned"
        );
        assert_eq!(transport.finish_message(), vec![response]);
        transport
            .begin_message(&test_distributed_write_message(
                &caller,
                &fetch.source_node_id,
                10,
            ))
            .unwrap();
        transport
            .send_routed_call(&caller, DfsRoutedCall::value(fetch.clone()))
            .unwrap();
        assert_eq!(
            peer.calls.load(Ordering::SeqCst),
            1,
            "ordinary write retains Fetch piggyback"
        );
        assert_eq!(transport.finish_message(), vec![fetch]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn file_sync_caller_fetch_async_single_reaches_peer_before_reply() {
        let (mut transport, peer, fetch, response) = served_file_sync_fetch_fixture();
        let caller = fetch.target_node_id.clone();
        transport
            .send_routed_call_async(&caller, DfsRoutedCall::value(fetch.clone()))
            .await
            .unwrap();
        assert_eq!(
            peer.calls.load(Ordering::SeqCst),
            1,
            "FileSync must dispatch its required caller Fetch before finish_message"
        );
        transport
            .send_routed_call_async(&caller, DfsRoutedCall::value(response.clone()))
            .await
            .unwrap();
        assert_eq!(
            peer.calls.load(Ordering::SeqCst),
            1,
            "NamespaceResponse remains reply-owned"
        );
        assert_eq!(transport.finish_message(), vec![response]);
        transport
            .begin_message(&test_distributed_write_message(
                &caller,
                &fetch.source_node_id,
                10,
            ))
            .unwrap();
        transport
            .send_routed_call_async(&caller, DfsRoutedCall::value(fetch.clone()))
            .await
            .unwrap();
        assert_eq!(
            peer.calls.load(Ordering::SeqCst),
            1,
            "ordinary write retains Fetch piggyback"
        );
        assert_eq!(transport.finish_message(), vec![fetch]);
    }

    #[test]
    fn file_sync_caller_fetch_sync_fanout_reaches_peer_before_reply() {
        let (mut transport, peer, fetch, response) = served_file_sync_fetch_fixture();
        let caller = fetch.target_node_id.clone();
        let outcomes = transport
            .send_routed_calls_bounded(
                vec![
                    DfsRoutedSend::new(caller.clone(), DfsRoutedCall::value(fetch.clone())),
                    DfsRoutedSend::new(caller.clone(), DfsRoutedCall::value(response.clone())),
                ],
                2,
            )
            .unwrap();
        assert_eq!(outcomes.len(), 2);
        assert!(
            outcomes
                .iter()
                .all(|outcome| outcome.target_node_id == caller && outcome.result.is_ok())
        );
        assert_eq!(
            peer.calls.load(Ordering::SeqCst),
            1,
            "only the required Fetch crosses the peer before finish_message"
        );
        assert_eq!(transport.finish_message(), vec![response]);
        transport
            .begin_message(&test_distributed_write_message(
                &caller,
                &fetch.source_node_id,
                10,
            ))
            .unwrap();
        let outcomes = transport
            .send_routed_calls_bounded(
                vec![DfsRoutedSend::new(
                    caller.clone(),
                    DfsRoutedCall::value(fetch.clone()),
                )],
                2,
            )
            .unwrap();
        assert_eq!(outcomes.len(), 1);
        assert!(outcomes[0].result.is_ok());
        assert_eq!(
            peer.calls.load(Ordering::SeqCst),
            1,
            "ordinary write retains Fetch piggyback"
        );
        assert_eq!(transport.finish_message(), vec![fetch]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn file_sync_caller_fetch_async_fanout_reaches_peer_before_reply() {
        let (mut transport, peer, fetch, response) = served_file_sync_fetch_fixture();
        let caller = fetch.target_node_id.clone();
        let outcomes = transport
            .send_routed_calls_bounded_async(
                vec![
                    DfsRoutedSend::new(caller.clone(), DfsRoutedCall::value(fetch.clone())),
                    DfsRoutedSend::new(caller.clone(), DfsRoutedCall::value(response.clone())),
                ],
                2,
            )
            .await
            .unwrap();
        assert_eq!(outcomes.len(), 2);
        assert!(
            outcomes
                .iter()
                .all(|outcome| outcome.target_node_id == caller && outcome.result.is_ok())
        );
        assert_eq!(
            peer.calls.load(Ordering::SeqCst),
            1,
            "only the required Fetch crosses the peer before finish_message"
        );
        assert_eq!(transport.finish_message(), vec![response]);
        transport
            .begin_message(&test_distributed_write_message(
                &caller,
                &fetch.source_node_id,
                10,
            ))
            .unwrap();
        let outcomes = transport
            .send_routed_calls_bounded_async(
                vec![DfsRoutedSend::new(
                    caller.clone(),
                    DfsRoutedCall::value(fetch.clone()),
                )],
                2,
            )
            .await
            .unwrap();
        assert_eq!(outcomes.len(), 1);
        assert!(outcomes[0].result.is_ok());
        assert_eq!(
            peer.calls.load(Ordering::SeqCst),
            1,
            "ordinary write retains Fetch piggyback"
        );
        assert_eq!(transport.finish_message(), vec![fetch]);
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
                storage_revision: None,
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
                storage_revision: None,
                publication: Default::default(),
                patch: None,
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
                wait_for_commit: false,
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
                        receipt_generation: 0,
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
                background_repair: false,
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
                receipt_generation: 0,
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

    struct AttachmentDeliveryFailurePeer {
        delivery_state: Option<sinomo_core::network::NetworkDeliveryState>,
        invalid_response: bool,
        calls: AtomicUsize,
    }

    impl AttachmentDeliveryFailurePeer {
        fn respond(&self, call: ObjectRpcCall) -> Result<ObjectRpcResult, DfsPeerTransportError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            assert!(
                !call.body.is_empty(),
                "exercise the semantic attachment adapter"
            );
            if self.invalid_response {
                return ObjectRpcResponse::encode_body(
                    &call.request.route,
                    &"invalid DFS response",
                )
                .map(ObjectRpcResult::value)
                .map_err(|error| DfsPeerTransportError::new(error.to_string()));
            }
            Err(DfsPeerTransportError::with_delivery_state(
                "injected attachment send/response failure",
                self.delivery_state,
            ))
        }
    }

    impl DfsPeerTransport for AttachmentDeliveryFailurePeer {
        fn call_object_rpc(
            &self,
            _target: &NodeId,
            call: ObjectRpcCall,
            _options: DfsObjectRpcSendOptions,
        ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
            self.respond(call)
        }
    }

    impl AsyncDfsPeerTransport for AttachmentDeliveryFailurePeer {
        fn call_object_rpc_async<'a>(
            &'a self,
            _target: &'a NodeId,
            call: ObjectRpcCall,
            _options: DfsObjectRpcSendOptions,
        ) -> DfsPeerTransportFuture<'a, Result<ObjectRpcResult, DfsPeerTransportError>> {
            Box::pin(async move { self.respond(call) })
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn attachment_adapters_preserve_delivery_and_classify_lost_decoded_response() {
        use sinomo_core::network::NetworkDeliveryState;

        for (state, invalid_response, expected) in [
            (
                Some(NetworkDeliveryState::DefinitelyNotSent),
                false,
                NetworkDeliveryState::DefinitelyNotSent,
            ),
            (
                Some(NetworkDeliveryState::PossiblyApplied),
                false,
                NetworkDeliveryState::PossiblyApplied,
            ),
            (None, true, NetworkDeliveryState::PossiblyApplied),
        ] {
            let local = node_id("attachment-delivery-local");
            let remote = node_id("attachment-delivery-remote");
            let peer = Arc::new(AttachmentDeliveryFailurePeer {
                delivery_state: state,
                invalid_response,
                calls: AtomicUsize::new(0),
            });
            let sync_peer: Arc<dyn DfsPeerTransport> = peer.clone();
            let async_peer: Arc<dyn AsyncDfsPeerTransport> = peer.clone();
            let mut transport = DfsNodeTransport::with_peer_transports_for_nodes(
                local.clone(),
                [remote.clone()],
                Duration::from_secs(1),
                sync_peer,
                async_peer,
            );
            let error = transport
                .send_routed_call(
                    &remote,
                    test_stage_attachment_send(&local, &remote, 1, "sync").call,
                )
                .unwrap_err();
            assert_eq!(error.delivery_state(), Some(expected), "sync: {error}");
            let error = transport
                .send_routed_call_async(
                    &remote,
                    test_stage_attachment_send(&local, &remote, 2, "async").call,
                )
                .await
                .unwrap_err();
            assert_eq!(error.delivery_state(), Some(expected), "async: {error}");
            assert_eq!(
                peer.calls.load(Ordering::SeqCst),
                2,
                "an unknown attachment outcome must not cause a fresh body replay"
            );
        }
    }

    #[test]
    fn exact_response_replay_unsent_retry_preserves_initial_ambiguity() {
        use sinomo_core::network::NetworkDeliveryState;

        let error = dfs_exact_response_replay_exhausted(
            "exact routed operation".into(),
            DfsPeerTransportError::with_delivery_state(
                "first response lost",
                Some(NetworkDeliveryState::PossiblyApplied),
            ),
            DfsPeerTransportError::with_delivery_state(
                "retry could not connect",
                Some(NetworkDeliveryState::DefinitelyNotSent),
            ),
        );
        assert_eq!(
            error.delivery_state(),
            Some(NetworkDeliveryState::PossiblyApplied)
        );
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
                    applied_entries: command.batch.journal_frames.len(),
                    verified_prefix_only: command.verify_prefix_only,
                    verified_authority_probe_id: None,
                    result: crate::DfsMetadataJournalBatchResult::Applied,
                    member_view: None,
                    member_record: None,
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
                        batch: crate::DfsMetadataJournalFrameBatch {
                            shard_id: shard_id.clone(),
                            journal_frames: Vec::new(),
                            next_after_sequence: 7,
                        },
                    },
                )
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
        let mut transport = DfsNodeTransport::new_with_peer_transport_for_nodes(
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
    fn blocking_metadata_fanout_honors_operation_deadline() {
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
        let mut transport = DfsNodeTransport::new_with_peer_transport_for_nodes(
            local.clone(),
            targets.clone(),
            sync_peer,
            Duration::from_secs(2),
        );
        transport.set_operation_deadline(Some(Instant::now() + Duration::from_millis(50)));

        let started = Instant::now();
        let responses = transport
            .send_metadata_journal_batches_direct(&test_metadata_fanout_requests(&local, &targets))
            .expect("deadline-bounded metadata authority fan-out")
            .expect("blocking peer retained");

        assert!(responses.is_empty());
        assert!(
            started.elapsed() < Duration::from_millis(250),
            "ordinary authority fan-out must retain the caller's shorter deadline"
        );
    }

    /// Direct metadata and routed DFS message peer. Only the direct metadata
    /// route is slowed; routed calls answer at once with an applied reply.
    struct AuthorityFanoutPeer {
        direct_delays: BTreeMap<NodeId, Duration>,
        failed_direct: BTreeSet<NodeId>,
        active: AtomicUsize,
        direct_timeouts: Mutex<Vec<(NodeId, Option<Duration>)>>,
        routed_targets: Mutex<Vec<NodeId>>,
    }

    impl AuthorityFanoutPeer {
        fn new(direct_delays: Vec<(NodeId, Duration)>, failed_direct: Vec<NodeId>) -> Arc<Self> {
            Arc::new(Self {
                direct_delays: direct_delays.into_iter().collect(),
                failed_direct: failed_direct.into_iter().collect(),
                active: AtomicUsize::new(0),
                direct_timeouts: Mutex::new(Vec::new()),
                routed_targets: Mutex::new(Vec::new()),
            })
        }

        fn applied(command: &DfsMetadataJournalBatchCommand) -> DfsMetadataJournalBatchResponse {
            DfsMetadataJournalBatchResponse {
                request_id: command.request_id.clone(),
                shard_id: command.batch.shard_id.clone(),
                next_after_sequence: command.batch.next_after_sequence,
                applied_entries: command.batch.journal_frames.len(),
                verified_prefix_only: false,
                verified_authority_probe_id: None,
                result: crate::DfsMetadataJournalBatchResult::Applied,
                member_view: None,
                member_record: None,
            }
        }

        fn delay(&self, target_node_id: &NodeId, call: &ObjectRpcCall) -> Duration {
            if call.request.route == crate::DFS_METADATA_JOURNAL_BATCH_ROUTE {
                self.direct_delays
                    .get(target_node_id)
                    .copied()
                    .unwrap_or_default()
            } else {
                Duration::ZERO
            }
        }

        fn respond(
            &self,
            target_node_id: &NodeId,
            call: ObjectRpcCall,
            options: DfsObjectRpcSendOptions,
        ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
            let error = |source: NetworkError| DfsPeerTransportError::new(source.to_string());
            let request = materialize_dfs_value_call_sync(call, usize::MAX)?;
            if request.route == crate::DFS_METADATA_JOURNAL_BATCH_ROUTE {
                self.direct_timeouts
                    .lock()
                    .expect("direct timeouts")
                    .push((target_node_id.clone(), options.overall_timeout));
                if self.failed_direct.contains(target_node_id) {
                    return Err(DfsPeerTransportError::new(format!(
                        "injected direct failure for {target_node_id}"
                    )));
                }
                let command: DfsMetadataJournalBatchCommand =
                    request.decode_body().map_err(error)?;
                return ObjectRpcResponse::encode_body(&request.route, &Self::applied(&command))
                    .map(ObjectRpcResult::value)
                    .map_err(error);
            }
            self.routed_targets
                .lock()
                .expect("routed targets")
                .push(target_node_id.clone());
            let command: DfsMessageCommand = request.decode_body().map_err(error)?;
            let DfsNetworkMessage::MetadataJournalBatch(batch) = &command.routed_message.message
            else {
                return Err(DfsPeerTransportError::new("unexpected routed message"));
            };
            let reply = DfsRoutedMessage {
                source_node_id: target_node_id.clone(),
                target_node_id: command.routed_message.source_node_id.clone(),
                frame_id: command.routed_message.frame_id,
                message_id: command.routed_message.message_id,
                message: DfsNetworkMessage::MetadataJournalBatchResponse(Self::applied(batch)),
            };
            ObjectRpcResponse::encode_body(
                &request.route,
                &DfsMessageResponse {
                    request_id: command.request_id,
                    outbound_messages: vec![reply],
                },
            )
            .map(ObjectRpcResult::value)
            .map_err(error)
        }
    }

    impl DfsPeerTransport for AuthorityFanoutPeer {
        fn call_object_rpc(
            &self,
            target_node_id: &NodeId,
            call: ObjectRpcCall,
            options: DfsObjectRpcSendOptions,
        ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
            self.active.fetch_add(1, Ordering::SeqCst);
            let _active = MetadataFanoutActiveGuard(&self.active);
            std::thread::sleep(self.delay(target_node_id, &call));
            self.respond(target_node_id, call, options)
        }
    }

    impl AsyncDfsPeerTransport for AuthorityFanoutPeer {
        fn call_object_rpc_async<'a>(
            &'a self,
            target_node_id: &'a NodeId,
            call: ObjectRpcCall,
            options: DfsObjectRpcSendOptions,
        ) -> DfsPeerTransportFuture<'a, Result<ObjectRpcResult, DfsPeerTransportError>> {
            Box::pin(async move {
                self.active.fetch_add(1, Ordering::SeqCst);
                let _active = MetadataFanoutActiveGuard(&self.active);
                tokio::time::sleep(self.delay(target_node_id, &call)).await;
                self.respond(target_node_id, call, options)
            })
        }
    }

    fn authority_fanout_groups(
        local_node_id: &NodeId,
        targets: &[NodeId],
    ) -> Vec<DfsMetadataJournalBatchDirectGroup> {
        let (_, command) = test_metadata_fanout_requests(local_node_id, targets)
            .into_iter()
            .next()
            .expect("one shared command");
        vec![DfsMetadataJournalBatchDirectGroup::new(
            targets.to_vec(),
            command,
        )]
    }

    /// Collect every direct reply with its arrival time after `started`.
    fn authority_fanout_direct_replies(
        fanout: &mut DfsMetadataAuthorityFanout,
        started: Instant,
    ) -> Vec<(
        NodeId,
        Result<DfsMetadataJournalBatchResponse, NetworkError>,
        Duration,
    )> {
        let mut replies = Vec::new();
        while let Some(event) = fanout.next_event(fanout.deadline()) {
            let DfsMetadataAuthorityFanoutEvent::Direct {
                target_node_id,
                result,
            } = event
            else {
                panic!("only direct calls were started: {event:?}");
            };
            replies.push((target_node_id, result, started.elapsed()));
        }
        replies
    }

    fn assert_slow_direct_call_outlives_hedge_interval(
        peer: &AuthorityFanoutPeer,
        replies: &[(
            NodeId,
            Result<DfsMetadataJournalBatchResponse, NetworkError>,
            Duration,
        )],
        slow: &NodeId,
    ) {
        assert_eq!(replies.len(), 2, "{replies:?}");
        assert!(
            replies.iter().all(|(_, result, _)| result.is_ok()),
            "{replies:?}"
        );
        let (_, _, slow_elapsed) = replies
            .iter()
            .find(|(target, _, _)| target == slow)
            .expect("slow direct reply");
        assert!(
            *slow_elapsed >= Duration::from_millis(600),
            "a direct call slower than the hedge interval must still complete"
        );
        for (target, timeout) in peer.direct_timeouts.lock().unwrap().iter() {
            assert!(
                timeout.is_some_and(|timeout| timeout >= Duration::from_secs(4)),
                "{target} network deadline must be the operation deadline: {timeout:?}"
            );
        }
    }

    #[test]
    fn blocking_authority_fanout_keeps_slow_direct_calls_until_the_operation_deadline() {
        let local = node_id("node_authority_fanout_blocking_n1");
        let fast = node_id("node_authority_fanout_blocking_n2");
        let slow = node_id("node_authority_fanout_blocking_n3");
        let peer =
            AuthorityFanoutPeer::new(vec![(slow.clone(), Duration::from_millis(600))], vec![]);
        let sync_peer: Arc<dyn DfsPeerTransport> = peer.clone();
        let mut transport = DfsNodeTransport::new_with_peer_transport_for_nodes(
            local.clone(),
            [fast.clone(), slow.clone()],
            sync_peer,
            Duration::from_secs(2),
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        transport.set_operation_deadline(Some(deadline));
        let started = Instant::now();

        let mut fanout = transport
            .start_metadata_authority_fanout(
                &authority_fanout_groups(&local, &[fast.clone(), slow.clone()]),
                Some(deadline),
            )
            .expect("authority fan-out")
            .expect("blocking peer retained");
        assert!(fanout.has_direct_call(&fast) && fanout.has_direct_call(&slow));
        let replies = authority_fanout_direct_replies(&mut fanout, started);

        assert_eq!(fanout.in_flight(), 0);
        assert_slow_direct_call_outlives_hedge_interval(&peer, &replies, &slow);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn async_authority_fanout_keeps_slow_direct_calls_until_the_operation_deadline() {
        let local = node_id("node_authority_fanout_async_n1");
        let fast = node_id("node_authority_fanout_async_n2");
        let slow = node_id("node_authority_fanout_async_n3");
        let peer =
            AuthorityFanoutPeer::new(vec![(slow.clone(), Duration::from_millis(600))], vec![]);
        let sync_peer: Arc<dyn DfsPeerTransport> = peer.clone();
        let async_peer: Arc<dyn AsyncDfsPeerTransport> = peer.clone();
        let mut transport = DfsNodeTransport::new_with_peer_transports_for_nodes(
            local.clone(),
            [fast.clone(), slow.clone()],
            sync_peer,
            async_peer,
            Duration::from_secs(2),
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        transport.set_operation_deadline(Some(deadline));
        let started = Instant::now();

        let mut fanout = transport
            .start_metadata_authority_fanout(
                &authority_fanout_groups(&local, &[fast.clone(), slow.clone()]),
                Some(deadline),
            )
            .expect("authority fan-out")
            .expect("async peer retained");
        let replies = authority_fanout_direct_replies(&mut fanout, started);

        assert_slow_direct_call_outlives_hedge_interval(&peer, &replies, &slow);
    }

    #[test]
    fn authority_network_deadline_is_not_restarted_after_replayable_encoding() {
        let local = node_id("node_metadata_authority_encode_budget_n1");
        let target = node_id("node_metadata_authority_encode_budget_n2");
        let peer = AuthorityFanoutPeer::new(Vec::new(), Vec::new());
        let sync_peer: Arc<dyn DfsPeerTransport> = peer.clone();
        let mut transport = DfsNodeTransport::new_with_peer_transport_for_nodes(
            local.clone(),
            [target.clone()],
            sync_peer,
            Duration::from_secs(2),
        );
        let operation_deadline = Instant::now() + Duration::from_millis(1_500);
        transport.set_operation_deadline(Some(operation_deadline));
        DFS_METADATA_DIRECT_ENCODING_TEST_DELAY.with(|delay| delay.set(Duration::from_millis(75)));

        let started = Instant::now();
        let mut fanout = transport
            .start_metadata_authority_fanout(
                &authority_fanout_groups(&local, std::slice::from_ref(&target)),
                Some(operation_deadline),
            )
            .expect("authority fan-out")
            .expect("blocking peer retained");
        assert!(
            started.elapsed() >= Duration::from_millis(75),
            "the test must exercise local encoding before dispatch"
        );
        let replies = authority_fanout_direct_replies(&mut fanout, started);

        assert!(matches!(replies.as_slice(), [(_, Ok(_), _)]), "{replies:?}");
        let (_, observed) = peer.direct_timeouts.lock().unwrap()[0].clone();
        let observed = observed.expect("authority calls carry a network deadline");
        assert!(
            observed <= Duration::from_millis(1_425),
            "encoding must not restart the operation's network deadline: {observed:?}"
        );
        assert!(
            observed >= Duration::from_secs(1),
            "no shorter network deadline may replace the operation deadline: {observed:?}"
        );
        assert_eq!(transport.operation_deadline(), Some(operation_deadline));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_authority_fanout_ends_calls_at_a_shorter_caller_deadline() {
        let local = node_id("node_authority_fanout_caller_deadline_n1");
        let target = node_id("node_authority_fanout_caller_deadline_n2");
        let peer = AuthorityFanoutPeer::new(vec![(target.clone(), Duration::from_secs(5))], vec![]);
        let sync_peer: Arc<dyn DfsPeerTransport> = peer.clone();
        let async_peer: Arc<dyn AsyncDfsPeerTransport> = peer.clone();
        let mut transport = DfsNodeTransport::new_with_peer_transports_for_nodes(
            local.clone(),
            [target.clone()],
            sync_peer,
            async_peer,
            Duration::from_secs(2),
        );
        let deadline = Instant::now() + Duration::from_millis(300);
        transport.set_operation_deadline(Some(deadline));
        let started = Instant::now();

        let mut fanout = transport
            .start_metadata_authority_fanout(
                &authority_fanout_groups(&local, std::slice::from_ref(&target)),
                Some(deadline),
            )
            .expect("authority fan-out")
            .expect("async peer retained");
        let event = fanout.next_event(Some(deadline + Duration::from_secs(2)));

        assert!(
            matches!(
                &event,
                Some(DfsMetadataAuthorityFanoutEvent::Direct { result: Err(_), .. })
            ),
            "{event:?}"
        );
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(
            peer.direct_timeouts
                .lock()
                .unwrap()
                .iter()
                .all(|(_, timeout)| {
                    timeout.is_some_and(|timeout| timeout <= Duration::from_millis(300))
                }),
            "the caller's shorter deadline wins"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropping_an_async_authority_fanout_abandons_its_calls() {
        let local = node_id("node_authority_fanout_drop_n1");
        let target = node_id("node_authority_fanout_drop_n2");
        let peer = AuthorityFanoutPeer::new(vec![(target.clone(), Duration::from_secs(5))], vec![]);
        let sync_peer: Arc<dyn DfsPeerTransport> = peer.clone();
        let async_peer: Arc<dyn AsyncDfsPeerTransport> = peer.clone();
        let mut transport = DfsNodeTransport::new_with_peer_transports_for_nodes(
            local.clone(),
            [target.clone()],
            sync_peer,
            async_peer,
            Duration::from_secs(2),
        );
        let deadline = Instant::now() + Duration::from_secs(10);
        transport.set_operation_deadline(Some(deadline));
        let fanout = transport
            .start_metadata_authority_fanout(
                &authority_fanout_groups(&local, std::slice::from_ref(&target)),
                Some(deadline),
            )
            .expect("authority fan-out")
            .expect("async peer retained");
        let entered = Instant::now() + Duration::from_secs(2);
        while peer.active.load(Ordering::SeqCst) == 0 {
            assert!(Instant::now() < entered, "direct call did not start");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        drop(fanout);

        let abandoned = Instant::now() + Duration::from_secs(2);
        while peer.active.load(Ordering::SeqCst) != 0 {
            assert!(
                Instant::now() < abandoned,
                "dropping the fan-out must abandon its in-flight call"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    #[test]
    fn authority_fanout_routed_call_reports_its_replies_as_one_event() {
        let local = node_id("node_authority_fanout_routed_n1");
        let target = node_id("node_authority_fanout_routed_n2");
        let peer =
            AuthorityFanoutPeer::new(vec![(target.clone(), Duration::from_millis(600))], vec![]);
        let sync_peer: Arc<dyn DfsPeerTransport> = peer.clone();
        let mut transport = DfsNodeTransport::new_with_peer_transport_for_nodes(
            local.clone(),
            [target.clone()],
            sync_peer,
            Duration::from_secs(2),
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        transport.set_operation_deadline(Some(deadline));
        let groups = authority_fanout_groups(&local, std::slice::from_ref(&target));
        let mut fanout = transport
            .start_metadata_authority_fanout(&groups, Some(deadline))
            .expect("authority fan-out")
            .expect("blocking peer retained");
        let mut command = groups[0].command.clone();
        command.request_id = "authority-fanout-routed-hedge".into();
        let message = DfsRoutedMessage {
            source_node_id: local.clone(),
            target_node_id: target.clone(),
            frame_id: 41,
            message_id: 41,
            message: DfsNetworkMessage::MetadataJournalBatch(command),
        };

        assert!(
            transport
                .send_routed_message_on_metadata_authority_fanout(&mut fanout, message)
                .expect("routed fan-out call")
                .is_none(),
            "the routed call runs beside the slow direct call"
        );
        assert_eq!(fanout.in_flight(), 2);
        let Some(DfsMetadataAuthorityFanoutEvent::Routed {
            target_node_id,
            result,
        }) = fanout.next_event(Some(deadline))
        else {
            panic!("the routed reply arrives before the slow direct reply");
        };
        assert_eq!(target_node_id, target);
        transport
            .deliver_metadata_authority_fanout_replies(result.expect("routed reply"))
            .expect("ordinary reply custody");
        let delivered = transport.drain_incoming_messages().unwrap();
        assert!(
            matches!(delivered.as_slice(), [DfsRoutedMessage {
                message: DfsNetworkMessage::MetadataJournalBatchResponse(response),
                ..
            }] if response.request_id == "authority-fanout-routed-hedge"),
            "{delivered:?}"
        );
        assert!(matches!(
            fanout.next_event(Some(deadline)),
            Some(DfsMetadataAuthorityFanoutEvent::Direct { result: Ok(_), .. })
        ));
        assert_eq!(*peer.routed_targets.lock().unwrap(), vec![target]);
    }

    /// Receives a direct metadata prepare one 16 KiB chunk at a time.
    struct SlowUploadPeer {
        chunk_delay: Duration,
    }

    impl DfsPeerTransport for SlowUploadPeer {
        fn call_object_rpc(
            &self,
            _target_node_id: &NodeId,
            _call: ObjectRpcCall,
            _options: DfsObjectRpcSendOptions,
        ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
            Err(DfsPeerTransportError::new("slow upload peer is async only"))
        }
    }

    impl AsyncDfsPeerTransport for SlowUploadPeer {
        fn call_object_rpc_async<'a>(
            &'a self,
            _target_node_id: &'a NodeId,
            call: ObjectRpcCall,
            _options: DfsObjectRpcSendOptions,
        ) -> DfsPeerTransportFuture<'a, Result<ObjectRpcResult, DfsPeerTransportError>> {
            Box::pin(async move {
                let error = |source: std::io::Error| DfsPeerTransportError::new(source.to_string());
                let (mut incoming, producer) = call
                    .into_test_incoming(&tokio::runtime::Handle::current())
                    .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
                let mut body = Vec::new();
                while let Some(chunk) = incoming.body.next_chunk().await.map_err(error)? {
                    body.extend_from_slice(&chunk);
                    drop(chunk);
                    tokio::time::sleep(self.chunk_delay).await;
                }
                let _ = producer.await;
                let command: DfsMetadataJournalBatchCommand = SinomoObjectCodec::network_frame()
                    .decode(&body)
                    .map_err(|source| DfsPeerTransportError::new(source.to_string()))?;
                ObjectRpcResponse::encode_body(
                    &incoming.request.route,
                    &AuthorityFanoutPeer::applied(&command),
                )
                .map(ObjectRpcResult::value)
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))
            })
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_slow_direct_upload_reports_advancing_progress_between_hedges() {
        let local = node_id("node_authority_fanout_slow_upload_n1");
        let target = node_id("node_authority_fanout_slow_upload_n2");
        let peer = Arc::new(SlowUploadPeer {
            chunk_delay: Duration::from_millis(40),
        });
        let sync_peer: Arc<dyn DfsPeerTransport> = peer.clone();
        let async_peer: Arc<dyn AsyncDfsPeerTransport> = peer;
        let mut transport = DfsNodeTransport::new_with_peer_transports_for_nodes(
            local.clone(),
            [target.clone()],
            sync_peer,
            async_peer,
            Duration::from_secs(2),
        );
        let deadline = Instant::now() + Duration::from_secs(10);
        transport.set_operation_deadline(Some(deadline));
        let mut groups = authority_fanout_groups(&local, std::slice::from_ref(&target));
        groups[0].command.batch.journal_frames = (4..=7)
            .map(|sequence| crate::DfsMetadataJournalFrameRecord {
                sequence,
                exact_bytes: bytes::Bytes::from(vec![sequence as u8; 64 * 1024]),
            })
            .collect();
        let mut fanout = transport
            .start_metadata_authority_fanout(&groups, Some(deadline))
            .expect("authority fan-out")
            .expect("async peer retained");

        // Sample progress twice, one hedge interval apart, while the upload of
        // a 256 KiB prepare runs at about 400 KiB/s.
        tokio::time::sleep(Duration::from_millis(150)).await;
        let earlier = fanout.direct_upload_bytes(&target).unwrap();
        tokio::time::sleep(Duration::from_millis(250)).await;
        let later = fanout.direct_upload_bytes(&target).unwrap();
        assert!(earlier > 0, "the upload has started: {earlier}");
        assert!(
            later > earlier,
            "a slow but progressing upload advances between hedges: {earlier} -> {later}"
        );
        assert!(matches!(
            fanout.next_event(Some(deadline)),
            Some(DfsMetadataAuthorityFanoutEvent::Direct { result: Ok(_), .. })
        ));
        assert!(fanout.direct_upload_bytes(&target).unwrap() >= 256 * 1024);
    }

    #[test]
    fn saturated_blocking_workers_leave_authority_targets_without_a_direct_call() {
        let local = node_id("node_authority_fanout_saturated_n1");
        let target = node_id("node_authority_fanout_saturated_n2");
        let peer = AuthorityFanoutPeer::new(Vec::new(), Vec::new());
        let sync_peer: Arc<dyn DfsPeerTransport> = peer.clone();
        let mut transport = DfsNodeTransport::new_with_peer_transport_for_nodes(
            local.clone(),
            [target.clone()],
            sync_peer,
            Duration::from_secs(2),
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        transport.set_operation_deadline(Some(deadline));
        transport.blocking_metadata_fanout_workers.store(
            DFS_METADATA_AUTHORITY_DIRECT_MAX_INFLIGHT,
            AtomicOrdering::Release,
        );
        let groups = authority_fanout_groups(&local, std::slice::from_ref(&target));

        let mut fanout = transport
            .start_metadata_authority_fanout(&groups, Some(deadline))
            .expect("authority fan-out")
            .expect("blocking peer retained");
        let message = DfsRoutedMessage {
            source_node_id: local.clone(),
            target_node_id: target.clone(),
            frame_id: 42,
            message_id: 42,
            message: DfsNetworkMessage::MetadataJournalBatch(groups[0].command.clone()),
        };
        let returned = transport
            .send_routed_message_on_metadata_authority_fanout(&mut fanout, message)
            .expect("routed fan-out call");
        transport
            .blocking_metadata_fanout_workers
            .store(0, AtomicOrdering::Release);

        assert!(!fanout.has_direct_call(&target));
        assert_eq!(fanout.in_flight(), 0);
        assert!(
            returned.is_some(),
            "without a worker the caller keeps its ordinary routed send"
        );
        assert!(peer.direct_timeouts.lock().unwrap().is_empty());
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
        let mut transport = DfsNodeTransport::new_with_peer_transport_for_nodes(
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
        let mut transport = DfsNodeTransport::new_with_peer_transports_for_nodes(
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
            replay_budget <= first_budget
                && first_budget <= Duration::from_secs(2)
                && first_budget > Duration::from_millis(1_900),
            "the first attempt carries the whole budget and the replay what remains: first={first_budget:?}, replay={replay_budget:?}"
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
        let mut transport = DfsNodeTransport::new_with_message_sender_for_nodes(
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
    fn exclusive_detached_worker_waits_for_the_shared_drain_gate() {
        let local = node_id("node_exclusive_detached_worker_local");
        let detached_send_state = DfsNodeDetachedSendState::new();
        let sender: DfsMessageSender = Arc::new(|_target, command, _options| {
            Ok(DfsMessageResponse {
                request_id: command.request_id.clone(),
                outbound_messages: Vec::new(),
            })
        });
        let transport = DfsNodeTransport::new_with_message_sender_and_detached_state(
            local,
            Vec::new(),
            sender,
            Duration::from_secs(1),
            detached_send_state.clone(),
        );
        let mut worker = transport.fork_detached_send_worker();
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

/// Request-local custody owned by the same ordinary node transport.
#[derive(Debug, Default)]
struct DfsServedMessageState {
    return_node_id: Option<NodeId>,
    // FileSync may need the caller's content before it can construct its reply.
    caller_fetch_before_reply: bool,
    inbound_messages: VecDeque<DfsRoutedMessage>,
    outbound_messages: Vec<DfsRoutedMessage>,
    outbound_attachment: Option<DfsOutboundAttachment>,
    prepared_replica_calls: Vec<DfsCheckpointedReplicaCall>,
    checkpointed_replica_calls: Arc<std::sync::Mutex<Vec<DfsCheckpointedReplicaCall>>>,
    background_replica_dispatch: bool,
}
impl DfsNodeTransport {
    fn served(&self) -> &DfsServedMessageState {
        self.served_message
            .as_deref()
            .expect("served request transport")
    }
    fn served_mut(&mut self) -> &mut DfsServedMessageState {
        self.served_message
            .as_deref_mut()
            .expect("served request transport")
    }
    fn send_peer_calls_bounded(
        &mut self,
        requests: Vec<DfsRoutedSend>,
        max_inflight: usize,
        #[cfg(any(test, feature = "test-support"))] native_context: Option<
            crate::native_shard_trace::NativeRpcAttemptContext,
        >,
    ) -> Result<Vec<DfsRoutedSendOutcome>, NetworkError> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        if requests.len() == 1 {
            let request = requests.into_iter().next().expect("one routed call");
            let result = self.send_routed_call_to_peer(
                &request.target_node_id,
                request.call,
                true,
                #[cfg(any(test, feature = "test-support"))]
                native_context.clone(),
            );
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
                #[cfg(any(test, feature = "test-support"))]
                let native_context = native_context.clone();
                let pending = &pending;
                let completed = &completed;
                let mut transport = self.fork_replica_call_send();
                workers.push(scope.spawn(sinomo_core::service::observed_blocking_task(
                    "dfs.routed.fanout",
                    move || {
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
                                #[cfg(any(test, feature = "test-support"))]
                                native_context.clone(),
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
                    },
                )));
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
    fn send_peer_calls_bounded_async<'a>(
        &'a mut self,
        requests: Vec<DfsRoutedSend>,
        max_inflight: usize,
        #[cfg(any(test, feature = "test-support"))] native_context: Option<
            crate::native_shard_trace::NativeRpcAttemptContext,
        >,
    ) -> DfsMessageTransportFuture<'a, Result<Vec<DfsRoutedSendOutcome>, NetworkError>> {
        Box::pin(async move {
            if requests.is_empty() {
                return Ok(Vec::new());
            }
            if requests.len() == 1 {
                let request = requests.into_iter().next().expect("one routed call");
                let result = self
                    .send_routed_call_to_peer_async(
                        &request.target_node_id,
                        request.call,
                        true,
                        #[cfg(any(test, feature = "test-support"))]
                        native_context.clone(),
                    )
                    .await;
                return Ok(vec![DfsRoutedSendOutcome {
                    target_node_id: request.target_node_id,
                    result,
                }]);
            }
            let limiter = Arc::new(tokio::sync::Semaphore::new(max_inflight.max(1)));
            let mut workers = tokio::task::JoinSet::new();
            for (index, request) in requests.into_iter().enumerate() {
                #[cfg(any(test, feature = "test-support"))]
                let native_context = native_context.clone();
                let limiter = Arc::clone(&limiter);
                let mut transport = self.fork_replica_call_send();
                workers.spawn(
                    async move {
                        let _permit =
                            limiter
                                .acquire_owned()
                                .await
                                .map_err(|_| NetworkError::Io {
                                    operation: "acquire DFS call fan-out permit".to_string(),
                                    detail: "DFS call fan-out limiter closed unexpectedly"
                                        .to_string(),
                                })?;
                        let result = transport
                            .send_routed_call_to_peer_async(
                                &request.target_node_id,
                                request.call,
                                true,
                                #[cfg(any(test, feature = "test-support"))]
                                native_context.clone(),
                            )
                            .await;
                        Ok::<_, NetworkError>((
                            index,
                            DfsRoutedSendOutcome {
                                target_node_id: request.target_node_id,
                                result,
                            },
                            transport.incoming_messages.drain(..).collect::<Vec<_>>(),
                        ))
                    }
                    .in_current_span(),
                );
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
    fn send_served_routed_call(
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
        if self
            .served()
            .return_node_id
            .as_ref()
            .is_some_and(|return_node_id| target_node_id == return_node_id)
            && dfs_network_message_can_return_to_served_caller(
                &message.message,
                self.served().caller_fetch_before_reply,
            )
        {
            if attachment.is_some() && self.served().outbound_attachment.is_some() {
                return Err(NetworkError::Serialization {
                    detail: "DFS served response attempted to return more than one attachment"
                        .to_string(),
                });
            }
            self.served_mut().outbound_messages.push(message);
            if let Some(attachment) = attachment {
                self.served_mut().outbound_attachment = Some(attachment);
            }
            return Ok(());
        }
        if target_node_id == &self.local_node_id {
            if attachment.is_none() && self.deliver_local_write_response(&message)? {
                return Ok(());
            }
            let message = crate::materialize_dfs_routed_call_sync(DfsRoutedCall {
                message,
                attachment,
            })?;
            self.served_mut().inbound_messages.push_back(message);
            return Ok(());
        }
        if attachment.is_none() && dfs_network_message_is_committed_write_response(&message.message)
        {
            // The caller's own response was retained above. A different
            // requester's completed write is replay-safe delivery work; it
            // must not hold this served lane or maintenance admission over I/O.
            return self.send_routed_message_to_peer_detached(target_node_id, message, true);
        }
        self.send_routed_call_to_peer(
            target_node_id,
            DfsRoutedCall {
                message,
                attachment,
            },
            true,
            #[cfg(any(test, feature = "test-support"))]
            crate::native_shard_trace::rpc_attempt_context(),
        )
    }
    fn send_served_routed_call_async<'a>(
        &'a mut self,
        target_node_id: &'a NodeId,
        call: DfsRoutedCall,
        #[cfg(any(test, feature = "test-support"))] native_context: Option<
            crate::native_shard_trace::NativeRpcAttemptContext,
        >,
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
            if self
                .served()
                .return_node_id
                .as_ref()
                .is_some_and(|return_node_id| target_node_id == return_node_id)
                && dfs_network_message_can_return_to_served_caller(
                    &message.message,
                    self.served().caller_fetch_before_reply,
                )
            {
                if attachment.is_some() && self.served().outbound_attachment.is_some() {
                    return Err(NetworkError::Serialization {
                        detail: "DFS served response attempted to return more than one attachment"
                            .to_string(),
                    });
                }
                self.served_mut().outbound_messages.push(message);
                if let Some(attachment) = attachment {
                    self.served_mut().outbound_attachment = Some(attachment);
                }
                return Ok(());
            }
            if target_node_id == &self.local_node_id {
                if attachment.is_none() && self.deliver_local_write_response(&message)? {
                    return Ok(());
                }
                let message =
                    crate::message_transport::materialize_dfs_routed_call(DfsRoutedCall {
                        message,
                        attachment,
                    })
                    .await?;
                self.served_mut().inbound_messages.push_back(message);
                return Ok(());
            }
            if attachment.is_none()
                && dfs_network_message_is_committed_write_response(&message.message)
            {
                return self.send_routed_message_to_peer_detached(target_node_id, message, true);
            }
            self.send_routed_call_to_peer_async(
                target_node_id,
                DfsRoutedCall {
                    message,
                    attachment,
                },
                true,
                #[cfg(any(test, feature = "test-support"))]
                native_context.clone(),
            )
            .await
        })
    }
    fn send_served_calls_bounded(
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
                .served()
                .return_node_id
                .as_ref()
                .is_some_and(|return_node_id| &request.target_node_id == return_node_id)
                && dfs_network_message_can_return_to_served_caller(
                    &request.call.message.message,
                    self.served().caller_fetch_before_reply,
                );
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
            let peer_outcomes = self.send_peer_calls_bounded(
                peer_requests
                    .into_iter()
                    .map(|(_, request)| request)
                    .collect(),
                max_inflight,
                #[cfg(any(test, feature = "test-support"))]
                crate::native_shard_trace::rpc_attempt_context(),
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
    fn send_served_calls_bounded_async<'a>(
        &'a mut self,
        requests: Vec<DfsRoutedSend>,
        max_inflight: usize,
        #[cfg(any(test, feature = "test-support"))] native_context: Option<
            crate::native_shard_trace::NativeRpcAttemptContext,
        >,
    ) -> DfsMessageTransportFuture<'a, Result<Vec<DfsRoutedSendOutcome>, NetworkError>> {
        Box::pin(async move {
            let request_count = requests.len();
            let mut outcomes = std::iter::repeat_with(|| None)
                .take(request_count)
                .collect::<Vec<Option<DfsRoutedSendOutcome>>>();
            let mut peer_requests = Vec::new();

            for (index, request) in requests.into_iter().enumerate() {
                let handled_by_served_lane = request.target_node_id == self.local_node_id
                    || (self
                        .served()
                        .return_node_id
                        .as_ref()
                        .is_some_and(|return_node_id| &request.target_node_id == return_node_id)
                        && dfs_network_message_can_return_to_served_caller(
                            &request.call.message.message,
                            self.served().caller_fetch_before_reply,
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
                    .send_peer_calls_bounded_async(
                        peer_requests
                            .into_iter()
                            .map(|(_, request)| request)
                            .collect(),
                        max_inflight,
                        #[cfg(any(test, feature = "test-support"))]
                        native_context.clone(),
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
}

impl DfsNodeTransport {
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
        let mut transport = DfsNodeTransport::with_senders_for_nodes_and_detached_state(
            local_node_id,
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
        transport.served_message = Some(Box::default());
        transport
    }

    /// Begin one served request using its explicit constructor-owned custody.
    /// An ordinary caller is rejected before request processing.
    pub fn begin_message(&mut self, message: &DfsRoutedMessage) -> Result<(), NetworkError> {
        if self.served_message.is_none() {
            return Err(NetworkError::Serialization {
                detail: "DFS client transport has no served request owner".into(),
            });
        }
        if message.target_node_id != self.local_node_id {
            return Err(NetworkError::FrameTargetMismatch {
                requested_target_node_id: self.local_node_id.clone(),
                frame_target_node_id: message.target_node_id.clone(),
            });
        }
        self.served_mut().return_node_id = Some(message.source_node_id.clone());
        self.served_mut().caller_fetch_before_reply = matches!(
            &message.message,
            DfsNetworkMessage::NamespaceRequest(command)
                if matches!(&command.operation, crate::DfsNamespaceOperation::FileSync { .. })
        );
        Ok(())
    }

    pub fn finish_message(&mut self) -> Vec<DfsRoutedMessage> {
        self.finish_message_with_attachment().0
    }

    /// Return only the exact served output and attachment. An ordinary caller
    /// has no served output and retains its original inbox.
    pub fn finish_message_with_attachment(
        &mut self,
    ) -> (Vec<DfsRoutedMessage>, Option<DfsOutboundAttachment>) {
        if self.served_message.is_none() {
            return (Vec::new(), None);
        }
        self.served_mut().return_node_id = None;
        self.served_mut().caller_fetch_before_reply = false;
        self.served_mut().inbound_messages.clear();
        self.incoming_messages.clear();
        (
            std::mem::take(&mut self.served_mut().outbound_messages),
            self.served_mut().outbound_attachment.take(),
        )
    }

    fn drain_queued_messages(&mut self) -> Vec<DfsRoutedMessage> {
        self.served_message
            .as_mut()
            .map(|state| state.inbound_messages.drain(..).collect())
            .unwrap_or_default()
    }
}

fn dfs_network_message_is_committed_write_response(message: &DfsNetworkMessage) -> bool {
    matches!(message, DfsNetworkMessage::DistributedWriteResponse(response)
    if matches!(&response.result, crate::DfsDistributedWriteResult::Outcome {
        outcome: crate::NetworkedDfsWriteOutcome::Committed { .. }
    }))
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

fn dfs_network_message_can_return_to_served_caller(
    message: &DfsNetworkMessage,
    caller_fetch_before_reply: bool,
) -> bool {
    dfs_network_message_is_served_call_response(message)
        || (!caller_fetch_before_reply && matches!(message, DfsNetworkMessage::FetchBlob(_)))
}

impl NetworkedDfsService<DfsNodeTransport> {

    pub(crate) fn set_background_replica_dispatch(&mut self, enabled: bool) {
        let Some(served) = self.transport.served_message.as_deref_mut() else {
            return;
        };
        if enabled {
            // Each admitted pass owns a fresh holder. Old workers cannot see
            // this generation before the producer releases its actual guard.
            served.checkpointed_replica_calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        } else {
            served.prepared_replica_calls.clear();
        }
        served.background_replica_dispatch = enabled;
    }

    pub(crate) fn prepare_background_replica_worker(&self) -> DfsNodeDetachedSendWorker {
        let mut worker = self.transport.fork_detached_send_worker();
        if let Some(served) = self.transport.served_message.as_ref() {
            worker.prepared_replica_calls = Arc::clone(&served.checkpointed_replica_calls);
        }
        worker
    }

    pub fn refresh_message_peer_targets(&mut self, peer_node_ids: Vec<NodeId>) {
        self.transport.replace_peer_nodes(peer_node_ids);
    }

    pub fn prepare_message_deferred_work(&mut self) -> DfsNodeDetachedSendWorker {
        if self.transport.served_message.is_some() {
            self.drain_deferred_routed_create_parent_touches();
        }
        self.prepare_background_replica_worker()
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
        if self.transport.served_message.is_some() {
            while !self.writes.deferred_routed_create_parent_touches.is_empty() {
                self.drain_deferred_routed_create_parent_touches();
            }
        }
        self.prepare_message_deferred_work()
    }

    pub fn message_deferred_work_pending(&self) -> Result<bool, DfsError> {
        if let Some(served) = self.transport.served_message.as_ref()
            && (!self.writes.deferred_routed_create_parent_touches.is_empty()
                || !served.prepared_replica_calls.is_empty()
                || !served
                    .checkpointed_replica_calls
                    .lock()
                    .map_err(|_| DfsError::Network {
                        message: "checkpointed background repair payload owner is poisoned"
                            .to_string(),
                    })?
                    .is_empty())
        {
            return Ok(true);
        }
        self.transport.fork_detached_send_worker().has_pending()
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
        self.process_message_command_report_with_native_return(command, preingested_blob)
    }

    pub(crate) fn process_message_command_report_with_native_return(
        &mut self,
        command: DfsMessageCommand,
        preingested_blob: Option<ContentStoreQuarantinedBlob>,
    ) -> DfsMessageProcessReport {
        self.process_message_command_report_with_continuation(command, preingested_blob, None)
    }

    pub(crate) fn process_message_command_report_with_continuation(
        &mut self,
        command: DfsMessageCommand,
        preingested_blob: Option<ContentStoreQuarantinedBlob>,
        prepared: Option<&crate::DfsPreparedOperationDigest>,
    ) -> DfsMessageProcessReport {
        self.begin_message_committed_work();
        let outcome = self.process_message_command_inner(command, preingested_blob, prepared);
        // A completed repair wait's deadline belongs to this re-run only,
        // whether or not it reached the file sync, as after a redirect.
        self.file_sync_wait_deadline = None;
        let committed_work = self.take_message_committed_work();
        DfsMessageProcessReport {
            outcome,
            committed_work,
        }
    }

    /// Serve only the sealed immutable request. Draining incoming callbacks
    /// here could execute a metadata mutation under content-read admission.
    pub(crate) fn process_verified_blob_fetch_report(
        &mut self,
        command: DfsMessageCommand,
        verified: crate::service_fetch_transfer::VerifiedDfsBlobFetch,
    ) -> DfsMessageProcessReport {
        self.begin_message_committed_work();
        let outcome = (|| {
            let routed = command.routed_message;
            if routed.target_node_id != self.node_id {
                return Err(DfsError::Network {
                    message: "immutable fetch target is not local".to_string(),
                });
            }
            self.transport
                .begin_message(&routed)
                .map_err(DfsError::from_network)?;
            let result = match routed.message {
                DfsNetworkMessage::FetchBlob(fetch) => {
                    self.handle_verified_blob_fetch(routed.source_node_id, fetch, verified)
                }
                _ => Err(DfsError::Distributed {
                    message: "immutable fetch proof used for a different message kind".to_string(),
                }),
            };
            let event = match result {
                Ok(event) => event,
                Err(error) => {
                    self.transport.finish_message();
                    return Err(error);
                }
            };
            let events = vec![event];
            self.last_events = retained_networked_dfs_service_events(&events);
            let (outbound_messages, attachment) = self.transport.finish_message_with_attachment();
            Ok(DfsMessageProcessResult {
                response: DfsMessageResponse {
                    request_id: command.request_id,
                    outbound_messages,
                },
                attachment,
                events,
            })
        })();
        DfsMessageProcessReport {
            outcome,
            committed_work: self.take_message_committed_work(),
        }
    }

    fn process_message_command_inner(
        &mut self,
        command: DfsMessageCommand,
        preingested_blob: Option<ContentStoreQuarantinedBlob>,
        prepared: Option<&crate::DfsPreparedOperationDigest>,
    ) -> Result<DfsMessageProcessResult, DfsError> {
        if !matches!(
            &command.routed_message.message,
            DfsNetworkMessage::StageBlob(_)
        ) {
            return self.process_message_with_current_callbacks(
                command,
                preingested_blob,
                prepared,
            );
        }
        // A prior Routine turn can retain a Fetch response whose receipt needs
        // authority from this Stage sender. It belongs to the same lane's next
        // Routine admission, rather than to the Stage's current response drain.
        let incoming = std::mem::take(&mut self.transport.incoming_messages);
        let inbound = self.transport.served_message.as_mut()
            .map(|served| std::mem::take(&mut served.inbound_messages));
        let inbox = self.transport.response_inbox.take();
        // This synchronous Result boundary includes every typed failure and
        // cancellation. Only current-Stage callbacks enter the ordinary drain
        // and finish; restore the preexisting custody before returning either.
        let result =
            self.process_message_with_current_callbacks(command, preingested_blob, prepared);
        self.transport.incoming_messages.extend(incoming);
        if let Some(inbound) = inbound {
            self.transport.served_mut().inbound_messages.extend(inbound);
        }
        debug_assert!(self.transport.response_inbox.is_none(),
            "Stage processing does not register a new routed-write response owner");
        self.transport.response_inbox = inbox;
        if self.transport.pending_incoming_count()
            .map_or(true, |pending| pending != Some(0))
        {
            // Preserve the existing later owner/wake even with no repair row.
            // This requests validation; it grants no publication authority.
            self.record_message_committed_work(crate::DfsMessageCommittedWork {
                repair_maintenance: true,
                ..Default::default()
            });
        }
        result
    }

    fn process_message_with_current_callbacks(
        &mut self,
        command: DfsMessageCommand,
        preingested_blob: Option<ContentStoreQuarantinedBlob>,
        prepared: Option<&crate::DfsPreparedOperationDigest>,
    ) -> Result<DfsMessageProcessResult, DfsError> {
        let request_id = command.request_id;
        self.transport
            .begin_message(&command.routed_message)
            .map_err(DfsError::from_network)?;
        let initial = if let Some(prepared) = prepared {
            match command.routed_message.message {
                DfsNetworkMessage::DistributedWriteRequest(write)
                | DfsNetworkMessage::ExclusiveCreateRequest(write)
                    if preingested_blob.is_none() =>
                {
                    self.resume_prepared_routed_write(
                        command.routed_message.source_node_id,
                        write,
                        prepared,
                    )
                    .map(|event| vec![event])
                }
                DfsNetworkMessage::NamespaceRequest(ns)
                    if matches!(ns.operation, crate::DfsNamespaceOperation::WriteFile { .. })
                        && preingested_blob.is_none() =>
                {
                    self.resume_browser_write_command(
                        command.routed_message.source_node_id,
                        ns,
                        prepared,
                    )
                    .map(|event| vec![event])
                }
                _ => Err(DfsError::Metadata {
                    message: "prepared write continuation used for another message".into(),
                }),
            }
        } else if let Some(blob) = preingested_blob {
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
                    DfsNetworkMessage::NamespaceRequest(ns)
                        if matches!(
                            ns.operation,
                            crate::DfsNamespaceOperation::WriteFile { .. }
                        ) =>
                    {
                        self.handle_staged_browser_write(routed.source_node_id, ns, blob)
                            .map(|event| vec![event])
                    }
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
            if pending == Some(0) {
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
    let transport = DfsNodeTransport::new_with_message_sender(
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
mod response_delivery_tests;

#[cfg(test)]
impl DfsNodeTransport {
    pub(crate) fn observe_routed_calls_for_test(&mut self) -> Arc<StdAtomicUsize> {
        let attempts = Arc::new(StdAtomicUsize::new(0));
        self.test_routed_call_attempts = Some(Arc::clone(&attempts));
        attempts
    }

    /// Record `(target, journal frame count)` for each metadata journal batch
    /// started on an authority fan-out's routed path.
    pub(crate) fn observe_metadata_authority_routed_sends_for_test(
        &mut self,
    ) -> Arc<std::sync::Mutex<Vec<(NodeId, usize)>>> {
        let sends = Arc::new(std::sync::Mutex::new(Vec::new()));
        self.test_metadata_authority_routed_sends = Some(Arc::clone(&sends));
        sends
    }
}
