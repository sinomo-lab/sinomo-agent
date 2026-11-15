//! Shared unit-test host for the actual embedded DFS component.
//! Clients always use DfsNodeTransport; healthy replies come from native ingress.
use crate::*;
use sinomo_core::cluster::ClusterKvPathsExt;
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::{
    ClusterObjectRpcOptions, ClusterPaths, NetworkDeliveryState, NetworkError, NetworkTimeoutPhase,
    ObjectRpcCall, ObjectRpcOutgoingResponse, ObjectRpcRequest, ObjectRpcResponse, ObjectRpcResult,
};
use sinomo_core::node::{ClusterId, NodeId};
use sinomo_core::object::serde_ext as sinomo_object_serde;
use sinomo_core::service::{
    ClusterAccess, ResponseSink, ServiceContext, ServiceError, ServiceRegistry, ServiceSigner,
};
use sinomo_module::{
    ComponentContext, ComponentEventBus, ComponentHostRole, ComponentObjectRpcTransport,
    ComponentReconciler,
};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use time::OffsetDateTime;
use tokio::sync::{Notify, Semaphore};

const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_OBSERVATIONS: usize = 4096;
type RpcFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ObjectRpcResult, NetworkError>> + Send + 'a>>;
type UnitFuture<'a> = Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

fn initialize_opt_in_trace() {
    if std::env::var("SINOMO_DFS_TEST_TRACE").as_deref() != Ok("1") {
        return;
    }
    static TRACE: std::sync::Once = std::sync::Once::new();
    TRACE.call_once(|| {
        let installed = tracing_subscriber::fmt()
            .with_env_filter("debug,sinomo.perf=trace,sinomo.dfs.perf=trace,sinomo.dfs.reconciliation=trace")
            .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
            .with_test_writer()
            .try_init();
        assert!(installed.is_ok() || tracing::enabled!(target: "sinomo.dfs.perf", tracing::Level::TRACE),
            "opt-in DFS fixture trace could not enable the existing performance target: {installed:?}");
    });
    assert!(
        tracing::enabled!(target: "sinomo.dfs.perf", tracing::Level::TRACE),
        "opt-in DFS fixture performance trace is disabled"
    );
}

pub(crate) fn executor() -> &'static tokio::runtime::Runtime {
    initialize_opt_in_trace();
    static EXECUTOR: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    EXECUTOR.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .thread_name("dfs-shared-test-runtime")
            .enable_all()
            .build()
            .expect("shared DFS test executor")
    })
}
fn run_rpc(future: RpcFuture<'_>) -> Result<ObjectRpcResult, NetworkError> {
    if let Ok(current) = tokio::runtime::Handle::try_current() {
        assert!(
            matches!(
                current.runtime_flavor(),
                tokio::runtime::RuntimeFlavor::MultiThread
            ),
            "synchronous DFS test RPC cannot block a current-thread executor"
        );
        tokio::task::block_in_place(|| executor().block_on(future))
    } else {
        executor().block_on(future)
    }
}
fn run_unit(future: UnitFuture<'_>) -> Result<(), String> {
    if let Ok(current) = tokio::runtime::Handle::try_current() {
        if !matches!(
            current.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            return Err(
                "synchronous DFS test lifecycle cannot block a current-thread executor".into(),
            );
        }
        tokio::task::block_in_place(|| executor().block_on(future))
    } else {
        executor().block_on(future)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RpcOrigin {
    RegisteredHost,
    ExplicitFixtureRpc,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RpcPhase {
    Constructed,
    BeforeDispatch,
    AfterDispatch,
    /// Normal Hosted decoder returned an actual supported typed header.
    Decoded,
    Completed,
    EndedWithoutResponse,
}
#[derive(Clone, Debug)]
pub struct RpcObservation {
    pub source: NodeId,
    pub target: NodeId,
    pub call_id: u64,
    /// Actual inline repair or normally decoded typed head; observation never reads a body.
    pub dfs_frame_id: Option<u64>,
    pub dfs_intent_id: Option<String>,
    pub dfs_request_id: Option<String>,
    pub dfs_file_system_id: Option<crate::DfsFileSystemId>,
    pub dfs_object_id: Option<crate::DfsObjectId>,
    pub dfs_version: Option<crate::DfsVersion>,
    pub route: String,
    pub phase: RpcPhase,
    /// Actual returned transport error, attached only to a retained terminal
    /// observation. Cancellation without a returned error leaves this absent.
    pub outcome_error: Option<String>,
    /// Actual receiver context, recorded only after source custody is validated.
    pub signer: Option<ServiceSigner>,
    pub origin: RpcOrigin,
    pub options: ClusterObjectRpcOptions,
    pub observe_connectivity: bool,
    pub streamed_value: bool,
    pub body_bytes: Option<u64>,
}
// The context carries only one real carrier invocation's observation identity.
// It wraps the unchanged registry future and never authorizes or modifies work.
#[derive(Clone)]
struct NativeRpcCallContext {
    fabric: std::sync::Weak<Fabric>,
    observation: RpcObservation,
    write_reply_capture: Option<Arc<Mutex<DecodedWriteReplyCapture>>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CapturedWriteReplyKind {
    Write,
    Distributed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DecodedWriteReplyIdentity {
    call_id: u64,
    source: NodeId,
    target: NodeId,
    route: String,
    outer_request_id: String,
    frame_id: u64,
    message_id: u64,
    request_id: String,
    object_id: DfsObjectId,
    kind: CapturedWriteReplyKind,
}

#[derive(Default)]
struct DecodedWriteReplyCapture {
    identity: Option<DecodedWriteReplyIdentity>,
    invalid: bool,
}

tokio::task_local! {
    static NATIVE_RPC_CALL_CONTEXT: NativeRpcCallContext;
}

/// Observe supported typed identities after normal decoding and signer validation.
/// Streamed value bodies have already been consumed by that ordinary decoder;
/// this observer copies only existing scalar identity fields.
pub(crate) fn observe_decoded_namespace_call(command: &crate::DfsMessageCommand) {
    let head = &command.routed_message;
    let _ = NATIVE_RPC_CALL_CONTEXT.try_with(|context| {
        if context.observation.source != head.source_node_id
            || context.observation.target != head.target_node_id
        {
            return;
        }
        if let Some(capture) = &context.write_reply_capture {
            // Only the normal authenticated decoder sees the request. Copy
            // scalar identity here; never poll/clone the request body again.
            let identity = match &head.message {
                DfsNetworkMessage::WriteRequest(write)
                    if context.observation.route == DFS_DATA_WRITE_ROUTE =>
                {
                    Some((
                        &write.request_id,
                        &write.request.object_id,
                        CapturedWriteReplyKind::Write,
                    ))
                }
                DfsNetworkMessage::DistributedWriteRequest(write)
                    if context.observation.route == DFS_DATA_DISTRIBUTED_WRITE_ROUTE =>
                {
                    Some((
                        &write.request_id,
                        &write.request.object_id,
                        CapturedWriteReplyKind::Distributed,
                    ))
                }
                _ => None,
            };
            if let Ok(mut capture) = capture.lock() {
                if let Some((request_id, object_id, kind)) = identity {
                    let identity = DecodedWriteReplyIdentity {
                        call_id: context.observation.call_id,
                        source: head.source_node_id.clone(),
                        target: head.target_node_id.clone(),
                        route: context.observation.route.clone(),
                        outer_request_id: command.request_id.clone(),
                        frame_id: head.frame_id,
                        message_id: head.message_id,
                        request_id: request_id.clone(),
                        object_id: object_id.clone(),
                        kind,
                    };
                    if capture
                        .identity
                        .as_ref()
                        .is_some_and(|previous| previous != &identity)
                    {
                        capture.invalid = true;
                    } else {
                        if capture.identity.is_none()
                            && let Some(fabric) = context.fabric.upgrade()
                        {
                            let mut observation = context.observation.clone();
                            observation.phase = RpcPhase::Decoded;
                            observation.dfs_frame_id = Some(identity.frame_id);
                            observation.dfs_request_id = Some(identity.request_id.clone());
                            observation.dfs_object_id = Some(identity.object_id.clone());
                            fabric.record(observation);
                        }
                        capture.identity = Some(identity);
                    }
                } else {
                    capture.invalid = true;
                }
            }
        }
        let Some(fabric) = context.fabric.upgrade() else {
            return;
        };
        let mut observation = context.observation.clone();
        match &head.message {
            crate::DfsNetworkMessage::WriteRequest(request)
                if observation.route == crate::DFS_DATA_WRITE_ROUTE
                    && context.write_reply_capture.is_none() =>
            {
                observation.dfs_request_id = Some(request.request_id.clone());
                observation.dfs_file_system_id = Some(request.request.file_system_id.clone());
                observation.dfs_object_id = Some(request.request.object_id.clone());
                observation.dfs_version = Some(request.request.target_version.clone());
            }
            crate::DfsNetworkMessage::DistributedWriteRequest(request)
            | crate::DfsNetworkMessage::ExclusiveCreateRequest(request)
                if observation.route == crate::DFS_DATA_DISTRIBUTED_WRITE_ROUTE
                    && context.write_reply_capture.is_none() =>
            {
                observation.dfs_request_id = Some(request.request_id.clone());
                observation.dfs_file_system_id = Some(request.request.file_system_id.clone());
                observation.dfs_object_id = Some(request.request.object_id.clone());
                observation.dfs_version = Some(request.request.target_version.clone());
            }
            crate::DfsNetworkMessage::FetchBlob(request)
                if observation.route == crate::DFS_DATA_FETCH_BLOB_ROUTE =>
            {
                observation.dfs_request_id = Some(request.request_id.clone());
                observation.dfs_intent_id = Some(request.intent_id.clone());
                observation.dfs_object_id = Some(request.object_id.clone());
                observation.dfs_version = Some(request.version.clone());
            }
            crate::DfsNetworkMessage::NamespaceRequest(request)
                if observation.route == crate::DFS_DATA_NAMESPACE_ROUTE =>
            {
                observation.dfs_request_id = Some(request.request_id.clone());
                observation.dfs_file_system_id = Some(request.file_system_id.clone());
            }
            crate::DfsNetworkMessage::DistributedWriteResponse(response)
                if observation.route == crate::DFS_DATA_DISTRIBUTED_WRITE_RESPONSE_ROUTE =>
            {
                observation.dfs_request_id = Some(response.request_id.clone());
                observation.dfs_object_id = Some(response.object_id.clone());
                if let crate::DfsDistributedWriteResult::Outcome {
                    outcome:
                        crate::NetworkedDfsWriteOutcome::Committed {
                            intent_id, version, ..
                        },
                } = &response.result
                {
                    observation.dfs_intent_id = Some(intent_id.clone());
                    observation.dfs_version = Some(version.clone());
                }
            }
            crate::DfsNetworkMessage::RepairReplicaResponse(response)
                if observation.route == crate::DFS_DATA_REPAIR_REPLICA_RESPONSE_ROUTE =>
            {
                observation.dfs_request_id = Some(response.request_id.clone());
                observation.dfs_intent_id = Some(response.intent_id.clone());
                observation.dfs_file_system_id = Some(response.file_system_id.clone());
                observation.dfs_object_id = Some(response.object_id.clone());
                observation.dfs_version = Some(response.version.clone());
            }
            _ => return,
        }
        observation.phase = RpcPhase::Decoded;
        observation.dfs_frame_id = Some(head.frame_id);
        fabric.record(observation);
    });
}

/// Validate only correlation of a supplied completed response. This utility
/// creates no receiver result or authority; the caller owns actual dispatch.
fn captured_write_reply(
    expected: &DecodedWriteReplyIdentity,
    observation: &RpcObservation,
    response: DfsMessageResponse,
) -> Result<DfsRoutedMessage, NetworkError> {
    let invalid = || NetworkError::Serialization {
        detail: "completed write capture differs from its actual decoded call".into(),
    };
    if expected.call_id != observation.call_id
        || expected.source != observation.source
        || expected.target != observation.target
        || expected.route != observation.route
        || !observation
            .signer
            .as_ref()
            .and_then(ServiceSigner::cluster_identity)
            .is_some_and(|(node, _)| node == expected.source.as_str())
        || response.request_id != expected.outer_request_id
        || expected.outer_request_id != format!("dfs_message_{}", expected.frame_id)
        || response.outbound_messages.len() != 1
    {
        return Err(invalid());
    }
    let routed = response
        .outbound_messages
        .into_iter()
        .next()
        .ok_or_else(invalid)?;
    if routed.source_node_id != expected.target || routed.target_node_id != expected.source {
        return Err(invalid());
    }
    let matched = match (&routed.message, expected.kind) {
        (DfsNetworkMessage::WriteResponse(reply), CapturedWriteReplyKind::Write) => {
            expected.route == DFS_DATA_WRITE_ROUTE
                && reply.request_id == expected.request_id
                && reply.object_id == expected.object_id
        }
        (
            DfsNetworkMessage::DistributedWriteResponse(reply),
            CapturedWriteReplyKind::Distributed,
        ) => {
            expected.route == DFS_DATA_DISTRIBUTED_WRITE_ROUTE
                && reply.request_id == expected.request_id
                && reply.object_id == expected.object_id
        }
        _ => false,
    };
    if !matched {
        return Err(invalid());
    }
    Ok(routed)
}

/// Observe the normal decoded command after source authentication. No stream
/// is consumed twice, no body is reconstructed, and no reply is substituted.
pub(crate) async fn pause_decoded_write_commit_publication(
    command: &crate::DfsMetadataJournalBatchCommand,
) -> Result<(), ServiceError> {
    let selected = NATIVE_RPC_CALL_CONTEXT.try_with(|context| {
        let Some(fabric) = context.fabric.upgrade() else { return Ok(None); };
        let observation = &context.observation;
        let mut rules = fabric.faults.lock().unwrap();
        let Some(rule) = rules.iter_mut().find(|rule| {
            (matches!(rule.action, FaultAction::PauseWriteCommitPublication { .. })
                || matches!(&rule.action, FaultAction::PauseExactWriteCommitPublication { operation_id, .. }
                    if operation_id.starts_with("write:")
                        && observation.route == crate::DFS_METADATA_JOURNAL_BATCH_ROUTE
                        && observation.signer.as_ref().and_then(ServiceSigner::cluster_identity)
                            .is_some_and(|(node, _)| node == observation.source.as_str())
                        && command.source_node_id.as_ref() == Some(&observation.source)
                        && command.durable_replication.as_ref().is_some_and(|purpose| purpose.background_publication)
                        && command.batch.decoded().is_ok_and(|batch| batch.entries.iter().any(|entry|
                            matches!(&entry.mutation, crate::DfsMetadataMutation::CommitPreparedOperationV7 { operation_id: actual, digest, .. }
                                if actual == operation_id && &digest.operation_id == operation_id)))))
                && rule.source.as_ref().is_none_or(|id| id == &observation.source)
                && rule.target.as_ref().is_none_or(|id| id == &observation.target)
                && rule.route.as_ref().is_none_or(|route| route == &observation.route)
                && rule.origin.is_none_or(|origin| origin == observation.origin)
                && rule.call_id.is_none_or(|id| id == observation.call_id)
        }) else { return Ok(None); };
        if !command.durable_replication.as_ref().is_some_and(|purpose| purpose.background_publication)
            || !command.batch.decoded().is_ok_and(|batch| batch.entries.iter().any(|entry| {
                matches!(&entry.mutation, crate::DfsMetadataMutation::CommitPreparedOperationV7 { digest, .. }
                    if digest.operation_id.starts_with("write:"))
            })) { return Ok(None); }
        if rule.remaining_hits == 0 {
            fabric.exhausted_budgets.fetch_add(1, Ordering::Relaxed);
            return Err(ServiceError::Service("bounded native publication pause exhausted".into()));
        }
        rule.remaining_hits -= 1;
        let (gate, captured) = match &rule.action {
            FaultAction::PauseWriteCommitPublication { gate, captured } => (gate, captured),
            FaultAction::PauseExactWriteCommitPublication { gate, captured, .. } => {
                if captured.lock().unwrap().len() >= 32 { return Err(ServiceError::Service("bounded exact write publication capture exhausted".into())); }
                (gate, captured)
            }
            _ => unreachable!(),
        };
        captured.lock().unwrap().push(command.clone());
        Ok(Some(Arc::clone(gate)))
    }).unwrap_or(Ok(None))?;
    if let Some(gate) = selected {
        let permit = gate
            .acquire()
            .await
            .map_err(|_| ServiceError::Service("native publication pause closed".into()))?;
        permit.forget();
    }
    Ok(())
}

/// Bounded negative changes to replies already produced by native read RPC.
#[derive(Clone, Copy)]
pub enum ReadReplyFault {
    NotFound,
    StaleLocation,
    WrongEnvelopeObject,
    WrongPayloadObject,
    WrongContentHash,
}

#[derive(Clone, Copy)]
pub enum CommitAckFault {
    Missing,
    Durable,
    WrongContent,
    WrongObject,
    WrongSender,
}

#[derive(Clone, Copy)]
pub enum SourceAdoptionFaultPhase {
    Release,
    InspectOutcome,
}

/// Only negative changes to a response captured from a genuine Fetch receiver.
#[derive(Clone, Copy)]
pub enum FetchReplyFault {
    Missing,
    NotFound,
    CorruptPayload,
    CorruptPayloadAndLength,
}
#[derive(Clone, Debug)]
pub struct CapturedFetchReply {
    pub rpc_request_id: String,
    pub routed: crate::DfsRoutedMessage,
}

/// Original DATA observed at the actual authenticated native Stage receiver.
/// The body remains owned by that receiver; only its immutable head/descriptor
/// and actual completed reply are copied for negative replay tests.
#[derive(Clone, Debug)]
pub struct CapturedStageExchange {
    pub source_node_id: NodeId,
    pub target_node_id: NodeId,
    pub signer: ServiceSigner,
    pub request: ObjectRpcRequest,
    pub command: DfsMessageCommand,
    pub attachment: Option<DfsRpcAttachmentDescriptor>,
    pub body_bytes: Option<u64>,
    pub observation: RpcObservation,
    pub response_request_id: String,
    pub response_outbound_message_count: usize,
    pub response: DfsRoutedMessage,
}

#[derive(Default, Debug)]
pub struct StageExchangeCapture {
    pub exchanges: Vec<CapturedStageExchange>,
    pub dropped_count: usize,
}

#[derive(Clone, Debug)]
pub enum StatReplyFault {
    Unsupported,
    Incomplete,
    WrongRequest,
    WrongEpoch,
    WrongSource,
    WrongTarget,
    MalformedPrefix,
    StalePrefix,
    /// Alter only an actual canonical projection page, after its path view.
    SameVersionHashChanged,
}

#[derive(Clone)]
pub enum FaultAction {
    /// Negative-only mutation of a real completed stat query response.
    AlterStatReplyAfterDispatch {
        change: StatReplyFault,
        captured: Arc<Mutex<Vec<crate::DfsRoutedMessage>>>,
    },
    /// Pause an actual completed initial stat projection without changing its
    /// response bytes. Observation/fault DATA cannot supply read authority.
    PauseStatProjectionReplyAfterDispatch {
        object_id: crate::DfsObjectId,
        gate: Arc<Semaphore>,
        captured: Arc<Mutex<Vec<crate::DfsRoutedMessage>>>,
    },
    PauseBeforeDispatch(Arc<Semaphore>),
    /// Hold a real committed-write journal publication before receiver ingress.
    /// Prepare/authority RPCs remain untouched; no successful reply is supplied.
    PauseWriteCommitPublication {
        gate: Arc<Semaphore>,
        captured: Arc<Mutex<Vec<crate::DfsMetadataJournalBatchCommand>>>,
    },
    /// Negative-only hold of an actual decoded background write commit for
    /// exactly this V7 operation. Unrelated writes/prepares spend no hits.
    PauseExactWriteCommitPublication {
        operation_id: String,
        gate: Arc<Semaphore>,
        captured: Arc<Mutex<Vec<crate::DfsMetadataJournalBatchCommand>>>,
    },
    /// Hold the actual receiver's response after its native execution completes.
    PauseAfterDispatch(Arc<Semaphore>),
    /// Retain the actual native read head while pausing its original body source.
    PauseNativeReadBody {
        gate: Arc<Semaphore>,
        started: Arc<AtomicBool>,
        forwarded_bytes: Arc<AtomicUsize>,
        relay_hops: usize,
    },
    /// Capture and negatively alter only an actual native Fetch response.
    AlterFetchReplyAfterDispatch {
        change: FetchReplyFault,
        captured: Arc<Mutex<Vec<CapturedFetchReply>>>,
    },
    BudgetExhausted,
    /// Bind the negative fault to the first matching physical call identity.
    /// Other calls remain outside its scope; repeated delivery of that exact
    /// call still obeys the ordinary fail-closed exhausted-budget behavior.
    BudgetExhaustedForFirstCall,
    /// Fail this real call with the normal transient route error category.
    TransientRouteFailureBeforeDispatch,
    /// Reject only a decoded actual source RPC subtype before dispatch.
    BudgetExhaustedForSourcePhase(SourceAdoptionFaultPhase),
    /// Execute the real receiver and discard only its completed response.
    LoseReplyAfterDispatch,
    /// Retain real native Stage or Repair replies, then discard their delivery.
    CaptureTransferReplyAndLoseAfterDispatch {
        /// Match only this actual inline Repair intent; other calls stay normal.
        intent_id: Option<String>,
        captured: Arc<Mutex<Vec<crate::DfsRoutedMessage>>>,
    },
    /// Capture the real completed client-facing write response, then lose its
    /// delivery. The actual post-auth decoded request supplies correlation.
    CaptureWriteReplyAndLoseAfterDispatch(Arc<Mutex<Vec<crate::DfsRoutedMessage>>>),
    /// Observe the real Stage head/reply and lose only its completed delivery.
    CaptureStageExchangeAndLoseAfterDispatch(Arc<Mutex<StageExchangeCapture>>),
    /// Observe the actual completed CommitVersion reply without changing it.
    CaptureCommitReplyAfterDispatch(Arc<Mutex<Vec<crate::DfsRoutedMessage>>>),
    /// Corrupt a captured native acknowledgement after the receiver executes.
    AlterCommitAckAfterDispatch(CommitAckFault),
}
#[derive(Clone)]
pub struct FaultRule {
    pub source: Option<NodeId>,
    pub target: Option<NodeId>,
    pub route: Option<String>,
    pub call_id: Option<u64>,
    pub origin: Option<RpcOrigin>,
    pub remaining_hits: usize,
    pub action: FaultAction,
}
struct Endpoint {
    // One actual mounted host lifetime; component-only reloads keep this fence.
    host_generation: Arc<()>,
    data_dir: Arc<SinomoDataDir>,
    cluster: Arc<dyn ClusterAccess>,
    registry: ServiceRegistry,
    kv_available: Arc<AtomicBool>,
    accepting: AtomicBool,
    // The mounted host's own calls; see [`NativeHostIdentity::sending`].
    sending: Arc<AtomicBool>,
    active: Arc<AtomicUsize>,
    idle: Arc<Notify>,
}
impl Endpoint {
    fn admits_route(&self, route: &str) -> bool {
        if sinomo_core::kv::CLUSTER_KV_RPC_ROUTES.contains(&route) {
            self.kv_available.load(Ordering::Acquire) && self.registry.owns(route)
        } else {
            self.accepting.load(Ordering::Acquire)
        }
    }
}

/// The error for a target that does not admit a route. A call to a
/// configured member that is not running, on a KV or DFS route alike, fails
/// as a routed call to a dead peer does. A running node that does not admit
/// the route, and a node outside the membership, stay unknown.
fn unavailable_target(source_data: &SinomoDataDir, target: &NodeId, stopped: bool) -> NetworkError {
    if stopped
        && let Ok(state) =
            sinomo_core::cluster::read_cluster_state_without_kv(&ClusterPaths::new(source_data))
    {
        return crate::runtime_host::unavailable_test_kv_peer(&state, target);
    }
    NetworkError::UnknownNode {
        node_id: target.clone(),
    }
}

struct Transfer {
    source: NodeId,
    target: NodeId,
    task: tokio::task::JoinHandle<Result<(), NetworkError>>,
}
struct RpcObservationCheckpoint {
    path: std::path::PathBuf,
    records: usize,
    digest: blake3::Hash,
}
impl RpcObservationCheckpoint {
    fn verify(&self) -> Result<(), String> {
        let bytes = std::fs::read(&self.path).map_err(|error| error.to_string())?;
        let records: Vec<String> =
            serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
        if blake3::hash(&bytes) != self.digest || records.len() != self.records {
            return Err(format!(
                "RPC observation checkpoint changed: {}",
                self.path.display()
            ));
        }
        Ok(())
    }
}
struct Fabric {
    dropped_observations: AtomicUsize,
    exhausted_budgets: AtomicUsize,
    next_call: AtomicU64,
    nodes: Mutex<BTreeMap<NodeId, Arc<Endpoint>>>,
    faults: Mutex<Vec<FaultRule>>,
    observations: Mutex<Vec<RpcObservation>>,
    observation_checkpoints: Mutex<Vec<RpcObservationCheckpoint>>,
    transfers: Mutex<Vec<Transfer>>,
    partitions: Mutex<BTreeMap<NodeId, Partition>>,
}
/// The traffic a partition cuts a node off from, both ways, with every
/// other node: its cluster-KV calls, its DFS calls, or both. A node's calls
/// to itself are never cut.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Partition {
    pub kv: bool,
    pub dfs: bool,
}
impl Fabric {
    fn partitioned(&self, source: &NodeId, target: &NodeId, route: &str) -> bool {
        if source == target {
            return false;
        }
        let kv = sinomo_core::kv::CLUSTER_KV_RPC_ROUTES.contains(&route);
        let partitions = self.partitions.lock().expect("DFS test partitions");
        [source, target].into_iter().any(|node| {
            partitions
                .get(node)
                .is_some_and(|cut| if kv { cut.kv } else { cut.dfs })
        })
    }
    fn close_paused_faults_for_node(&self, node: &NodeId) {
        for rule in self.faults.lock().unwrap().iter() {
            if rule.target.as_ref().is_none_or(|id| id == node)
                || rule.source.as_ref().is_none_or(|id| id == node)
            {
                if let FaultAction::PauseBeforeDispatch(gate)
                | FaultAction::PauseWriteCommitPublication { gate, .. }
                | FaultAction::PauseExactWriteCommitPublication { gate, .. }
                | FaultAction::PauseAfterDispatch(gate)
                | FaultAction::PauseStatProjectionReplyAfterDispatch { gate, .. }
                | FaultAction::PauseNativeReadBody { gate, .. } = &rule.action
                {
                    gate.close();
                }
            }
        }
    }

    fn record_returned_error(&self, call_id: u64, error: &NetworkError) {
        let detail = format!("{error:?}");
        let mut bounded = detail.chars().take(2048).collect::<String>();
        if bounded.len() < detail.len() {
            bounded.push_str(" [diagnostic truncated]");
        }
        if let Ok(mut records) = self.observations.lock()
            && let Some(observation) = records
                .iter_mut()
                .rev()
                .find(|row| row.call_id == call_id && row.phase == RpcPhase::EndedWithoutResponse)
        {
            observation.outcome_error = Some(bounded);
        }
        // A checkpointed/evicted event remains immutable. Do not create a new
        // event or conceal incomplete coverage by reconstructing a lost one.
    }
    fn record(&self, observation: RpcObservation) {
        if let Ok(mut records) = self.observations.lock() {
            if records.len() < MAX_OBSERVATIONS {
                records.push(observation);
                return;
            }
        }
        self.dropped_observations.fetch_add(1, Ordering::Relaxed);
    }
    fn fault(
        &self,
        observation: &RpcObservation,
        request: &ObjectRpcRequest,
    ) -> Option<FaultAction> {
        let mut rules = self.faults.lock().expect("DFS test fault rules");
        let rule = rules.iter_mut().find(|rule| {
            rule.source
                .as_ref()
                .is_none_or(|id| id == &observation.source)
                && rule
                    .target
                    .as_ref()
                    .is_none_or(|id| id == &observation.target)
                && rule
                    .route
                    .as_ref()
                    .is_none_or(|route| route == &observation.route)
                && rule
                    .origin
                    .is_none_or(|origin| origin == observation.origin)
                && rule.call_id.is_none_or(|id| id == observation.call_id)
                && match &rule.action {
                    // The ordinary request value may be streamed. Select this
                    // negative fault from the real completed typed reply,
                    // without consuming or reconstructing the request body.
                    FaultAction::AlterStatReplyAfterDispatch { .. }
                    | FaultAction::PauseStatProjectionReplyAfterDispatch { .. } => false,
                    // Metadata journal values may be streamed. Select only
                    // after the ordinary authenticated receiver decodes them.
                    FaultAction::PauseWriteCommitPublication { .. }
                    | FaultAction::PauseExactWriteCommitPublication { .. } => false,
                    FaultAction::CaptureTransferReplyAndLoseAfterDispatch { intent_id, .. } => {
                        intent_id.as_ref().is_none_or(|expected| {
                            observation.dfs_intent_id.as_ref() == Some(expected)
                                && observation.dfs_request_id.is_some()
                        })
                    }
                    FaultAction::BudgetExhaustedForSourcePhase(phase) => {
                        if request.route != crate::source_adoption::SOURCE_ADOPTION_ROUTE { false }
                        else { sinomo_object_serde::from_bytes::<crate::source_adoption::DfsSourceAdoptionRequest>(&request.body).is_ok_and(|actual| match phase {
                            SourceAdoptionFaultPhase::Release => matches!(actual, crate::source_adoption::DfsSourceAdoptionRequest::Release { .. }),
                            SourceAdoptionFaultPhase::InspectOutcome => matches!(actual, crate::source_adoption::DfsSourceAdoptionRequest::InspectOutcome { .. }),
                        }) }
                    }
                    _ => true,
                }
        })?;
        if rule.remaining_hits == 0 {
            self.exhausted_budgets.fetch_add(1, Ordering::Relaxed);
            return Some(FaultAction::BudgetExhausted);
        }
        if matches!(rule.action, FaultAction::BudgetExhaustedForFirstCall
            | FaultAction::CaptureWriteReplyAndLoseAfterDispatch(_))
        {
            // Bind this completed-reply loss to one actual carrier. An exact
            // operation retry on a distinct call must not consume this rule.
            rule.call_id.get_or_insert(observation.call_id);
        }
        rule.remaining_hits -= 1;
        Some(match &rule.action {
            FaultAction::BudgetExhaustedForSourcePhase(_)
            | FaultAction::BudgetExhaustedForFirstCall => FaultAction::BudgetExhausted,
            _ => rule.action.clone(),
        })
    }
    fn transfer(
        &self,
        source: NodeId,
        target: NodeId,
        task: tokio::task::JoinHandle<Result<(), NetworkError>>,
    ) {
        self.transfers
            .lock()
            .expect("DFS test body transfers")
            .push(Transfer {
                source,
                target,
                task,
            });
    }
}
struct Invocation {
    active: Arc<AtomicUsize>,
    idle: Arc<Notify>,
    fabric: Arc<Fabric>,
    observation: RpcObservation,
    completed: bool,
}
impl Drop for Invocation {
    fn drop(&mut self) {
        if !self.completed {
            self.observation.phase = RpcPhase::EndedWithoutResponse;
            self.fabric.record(self.observation.clone());
        }
        self.active.fetch_sub(1, Ordering::AcqRel);
        self.idle.notify_waiters();
    }
}
#[derive(Clone)]
struct NativeHostIdentity {
    generation: Arc<()>,
    cluster_id: ClusterId,
    data_dir: Arc<SinomoDataDir>,
    /// Whether this mounted host sends. It does from its mount until its stop
    /// begins, whether or not it accepts inbound calls yet, as a daemon sends
    /// before its own ingress is ready.
    sending: Arc<AtomicBool>,
}

#[derive(Clone)]
struct Carrier {
    // Only mount_node grants a host binding. Raw fixture RPCs remain remote.
    host_identity: Option<NativeHostIdentity>,
    source: NodeId,
    fabric: Arc<Fabric>,
    origin: RpcOrigin,
}
impl Carrier {
    /// The source's data directory if it may send `route` now. A mounted host
    /// sends while its [`NativeHostIdentity::sending`] holds; a retired host
    /// generation never borrows its successor's. A raw fixture call keeps the
    /// source endpoint's own route admission.
    fn sending_source(
        &self,
        nodes: &BTreeMap<NodeId, Arc<Endpoint>>,
        route: &str,
    ) -> Result<Arc<SinomoDataDir>, NetworkError> {
        let Some(host) = &self.host_identity else {
            let source = nodes
                .get(&self.source)
                .filter(|source| source.admits_route(route))
                .ok_or_else(|| NetworkError::UnknownNode {
                    node_id: self.source.clone(),
                })?;
            return Ok(Arc::clone(&source.data_dir));
        };
        if nodes
            .get(&self.source)
            .is_some_and(|source| !Arc::ptr_eq(&host.generation, &source.host_generation))
        {
            return Err(NetworkError::delivery_failed(
                "native host generation admission",
                NetworkDeliveryState::DefinitelyNotSent,
                "retired native host generation cannot borrow its successor's authority",
            ));
        }
        if !host.sending.load(Ordering::Acquire) {
            return Err(NetworkError::UnknownNode {
                node_id: self.source.clone(),
            });
        }
        Ok(Arc::clone(&host.data_dir))
    }
    fn observation(
        &self,
        target: &NodeId,
        call: &ObjectRpcCall,
        options: ClusterObjectRpcOptions,
        observe_connectivity: bool,
    ) -> RpcObservation {
        // Ordinary attached repairs encode only their typed small head inline.
        // Never consume a streamed value or open/poll/read the attachment body.
        let repair = if call.request.route == crate::DFS_DATA_REPAIR_REPLICA_ROUTE
            && call.request.body.len() <= 32 * 1024
        {
            call.request
                .decode_body::<crate::body_rpc::DfsAttachmentMessageCommand>()
                .map(|head| head.command)
                .or_else(|_| crate::decode_dfs_message_rpc_request(&call.request))
                .ok()
                .and_then(|command| {
                    if command.routed_message.source_node_id != self.source
                        || &command.routed_message.target_node_id != target
                    {
                        return None;
                    }
                    match command.routed_message.message {
                        crate::DfsNetworkMessage::RepairReplica(repair) => Some((
                            command.routed_message.frame_id,
                            repair.intent_id,
                            repair.file_system_id,
                            repair.object_id,
                            repair.version,
                            repair.request_id,
                        )),
                        crate::DfsNetworkMessage::RepairRetainedBlob(repair) => Some((
                            command.routed_message.frame_id,
                            repair.intent_id,
                            repair.file_system_id,
                            repair.object_id,
                            repair.version,
                            repair.request_id,
                        )),
                        _ => None,
                    }
                })
        } else {
            None
        };
        RpcObservation {
            source: self.source.clone(),
            target: target.clone(),
            call_id: self.fabric.next_call.fetch_add(1, Ordering::Relaxed),
            dfs_frame_id: repair.as_ref().map(|(frame, _, _, _, _, _)| *frame),
            dfs_intent_id: repair
                .as_ref()
                .map(|(_, intent, _, _, _, _)| intent.clone()),
            dfs_request_id: repair
                .as_ref()
                .map(|(_, _, _, _, _, request)| request.clone()),
            dfs_file_system_id: repair.as_ref().map(|(_, _, fs, _, _, _)| fs.clone()),
            dfs_object_id: repair
                .as_ref()
                .map(|(_, _, _, object, _, _)| object.clone()),
            dfs_version: repair.map(|(_, _, _, _, version, _)| version),
            route: call.request.route.clone(),
            phase: RpcPhase::Constructed,
            outcome_error: None,
            signer: None,
            origin: self.origin,
            options,
            observe_connectivity,
            streamed_value: call.has_streamed_value_body(),
            body_bytes: call.body.total_bytes(),
        }
    }
    async fn deliver(
        &self,
        target: &NodeId,
        call: ObjectRpcCall,
        mut observation: RpcObservation,
        dispatch_started: Arc<AtomicBool>,
    ) -> Result<ObjectRpcResult, NetworkError> {
        // Drop order is part of the drain contract: every service-bearing
        // endpoint/reference below must end before this counter-only guard
        // publishes idle. Its notification cannot itself pin the old owner.
        let mut _invocation;
        let (source_data, endpoint) = {
            let nodes = self.fabric.nodes.lock().expect("DFS test nodes");
            let source_data = self.sending_source(&nodes, &call.request.route)?;
            if self
                .fabric
                .partitioned(&self.source, target, &call.request.route)
            {
                return Err(crate::runtime_host::partitioned_test_peer(target));
            }
            let Some(endpoint) = nodes
                .get(target)
                .filter(|endpoint| endpoint.admits_route(&call.request.route))
                .cloned()
            else {
                let stopped = nodes
                    .get(target)
                    .is_none_or(|endpoint| !endpoint.kv_available.load(Ordering::Acquire));
                drop(nodes);
                return Err(unavailable_target(&source_data, target, stopped));
            };
            endpoint.active.fetch_add(1, Ordering::AcqRel);
            (source_data, endpoint)
        };
        _invocation = Invocation {
            active: Arc::clone(&endpoint.active),
            idle: Arc::clone(&endpoint.idle),
            fabric: self.fabric.clone(),
            observation: observation.clone(),
            completed: false,
        };
        if !endpoint.admits_route(&call.request.route) {
            let stopped = !endpoint.kv_available.load(Ordering::Acquire);
            return Err(unavailable_target(&source_data, target, stopped));
        }
        drop(source_data);
        observation.phase = RpcPhase::BeforeDispatch;
        self.fabric.record(observation.clone());
        let fault = self.fabric.fault(&observation, &call.request);
        match &fault {
            Some(FaultAction::CaptureWriteReplyAndLoseAfterDispatch(_))
                if observation.route != DFS_DATA_WRITE_ROUTE && observation.route != DFS_DATA_DISTRIBUTED_WRITE_ROUTE =>
            {
                return Err(NetworkError::delivery_failed(
                    "shared DFS write capture scope", NetworkDeliveryState::DefinitelyNotSent,
                    "write reply capture requires an actual write request route",
                ));
            }
            Some(FaultAction::BudgetExhausted) => {
                return Err(NetworkError::delivery_failed(
                    "shared DFS test before-dispatch fault",
                    NetworkDeliveryState::DefinitelyNotSent,
                    "bounded injected carrier failure",
                ));
            }
            Some(FaultAction::TransientRouteFailureBeforeDispatch) => {
                return Err(NetworkError::delivery_failed(
                    "cluster target rpc",
                    NetworkDeliveryState::DefinitelyNotSent,
                    "bounded injected no usable route before dispatch",
                ));
            }
            Some(FaultAction::PauseBeforeDispatch(gate)) => {
                let permit = gate.acquire().await.map_err(|_| {
                    NetworkError::delivery_failed(
                        "shared DFS test dispatch gate",
                        NetworkDeliveryState::DefinitelyNotSent,
                        "case or node closed",
                    )
                })?;
                permit.forget();
            }
            _ => {}
        }
        if !endpoint.admits_route(&call.request.route) {
            return Err(NetworkError::delivery_failed(
                "shared DFS test dispatch admission",
                NetworkDeliveryState::DefinitelyNotSent,
                "node closed before native dispatch",
            ));
        }
        let (incoming, request_transfer) = call.into_test_incoming(executor().handle())?;
        self.fabric
            .transfer(self.source.clone(), target.clone(), request_transfer);
        let captured_stage_head = if matches!(
            &fault,
            Some(FaultAction::CaptureStageExchangeAndLoseAfterDispatch(_))
        ) {
            // Reader-backed Stage calls keep the typed command in the ordinary
            // attachment head. Do not poll or copy the original incoming body.
            let (command, attachment) = if incoming.body.is_attachment() {
                let head: DfsAttachmentMessageCommand = incoming.request.decode_body()?;
                if incoming
                    .body
                    .total_bytes()
                    .is_some_and(|size| size != head.attachment.total_size_bytes)
                {
                    return Err(NetworkError::Serialization {
                        detail:
                            "actual Stage capture attachment length differs from its received head"
                                .into(),
                    });
                }
                (head.command, Some(head.attachment))
            } else {
                if incoming.body.is_streamed_value() {
                    return Err(NetworkError::Serialization {
                        detail: "Stage head capture cannot consume an ordinary streamed value body"
                            .into(),
                    });
                }
                (
                    crate::decode_dfs_message_rpc_request(&incoming.request)?,
                    None,
                )
            };
            if !matches!(
                &command.routed_message.message,
                DfsNetworkMessage::StageBlob(_) | DfsNetworkMessage::StageRetainedBlob(_)
            ) || command.routed_message.source_node_id != self.source
                || command.routed_message.target_node_id != *target
                || incoming.request.route != crate::DFS_DATA_STAGE_BLOB_ROUTE
            {
                return Err(NetworkError::Serialization {
                    detail: "actual Stage capture does not match its native route/source/target"
                        .into(),
                });
            }
            Some((
                incoming.request.clone(),
                command,
                attachment,
                incoming.body.total_bytes(),
            ))
        } else {
            None
        };
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        let source_data = self.sending_source(
            &self.fabric.nodes.lock().expect("DFS test source identity"),
            &incoming.request.route,
        )?;
        let source_state =
            sinomo_core::cluster::read_cluster_state_without_kv(&ClusterPaths::new(&source_data))?;
        let public_key = source_state.members.iter()
            .find(|member| member.node_id == self.source)
            .ok_or_else(|| NetworkError::UnknownNode { node_id: self.source.clone() })?
            .node_public_key.clone();
        if let Some(host) = &self.host_identity {
            if source_state.local_node_id != self.source || source_state.cluster_id != host.cluster_id {
                return Err(NetworkError::delivery_failed(
                    "native host identity admission", NetworkDeliveryState::DefinitelyNotSent,
                    "configured node/cluster identity changed under the mounted native host",
                ));
            }
        }
        // Mirror authenticated local Main ingress only for the SAME actual
        // registered host generation. Equal node IDs alone do not establish
        // locality: raw protocol frames and calls across nodes stay Remote.
        let signer = if self.source == *target && self.host_identity.is_some() {
            ServiceSigner::LocalNode { node_id: self.source.to_string(), public_key }
        } else {
            ServiceSigner::Remote { node_id: self.source.to_string(), public_key }
        };
        observation.signer = Some(signer.clone());
        _invocation.observation.signer = Some(signer.clone());
        let context = ServiceContext::new(
            endpoint.data_dir.clone(),
            Some(endpoint.cluster.clone()),
            signer.clone(),
            incoming.body,
            Box::new(ReplySink(response_tx)),
        );
        dispatch_started.store(true, Ordering::Release);
        let write_reply_capture = matches!(&fault, Some(FaultAction::CaptureWriteReplyAndLoseAfterDispatch(_)))
            .then(|| Arc::new(Mutex::new(DecodedWriteReplyCapture::default())));
        let native_call = NativeRpcCallContext {
            fabric: Arc::downgrade(&self.fabric),
            observation: observation.clone(),
            write_reply_capture: write_reply_capture.clone(),
        };
        let dispatched = NATIVE_RPC_CALL_CONTEXT
            .scope(
                native_call,
                endpoint.registry.dispatch(context, incoming.request),
            )
            .await;
        let mut outgoing = match dispatched {
            // A KV handler's refusal reaches the caller as the daemon sends
            // it: an error response the caller types again, so a busy or
            // non-leader authority is recognized as one.
            Err(error)
                if sinomo_core::kv::CLUSTER_KV_RPC_ROUTES.contains(&observation.route.as_str()) =>
            {
                ObjectRpcOutgoingResponse::value(ObjectRpcResponse::encode_error(
                    observation.route.clone(),
                    error.to_string(),
                )?)
            }
            Err(error) => {
                return Err(NetworkError::Io {
                    operation: "shared DFS native service dispatch".into(),
                    detail: error.to_string(),
                });
            }
            Ok(_) => response_rx.await.map_err(|error| NetworkError::Io {
                operation: "receive shared DFS native service response".into(),
                detail: error.to_string(),
            })?,
        };
        observation.phase = RpcPhase::AfterDispatch;
        self.fabric.record(observation.clone());
        if let Some(FaultAction::PauseAfterDispatch(gate)) = &fault {
            let permit = gate.acquire().await.map_err(|_| {
                NetworkError::delivery_failed(
                    "shared DFS test completed-response gate",
                    NetworkDeliveryState::PossiblyApplied,
                    "native receiver completed before case or node closed",
                )
            })?;
            permit.forget();
        }
        if let Some(FaultAction::AlterFetchReplyAfterDispatch { change, captured }) = &fault {
            let mut response = if !outgoing.body.is_empty() && !outgoing.response.body.is_empty() {
                let head = crate::body_rpc::decode_dfs_attachment_message_rpc_response(
                    &outgoing.response,
                )?;
                let bytes = outgoing.body.collect_to_vec(4 * 1024 * 1024).await?;
                crate::body_rpc::attach_dfs_message_response_attachment(head, bytes)?
            } else {
                let value = if outgoing.response.body.is_empty() {
                    outgoing.body.collect_to_vec(4 * 1024 * 1024).await?
                } else {
                    outgoing.response.body.clone()
                };
                sinomo_object_serde::from_bytes::<crate::DfsMessageResponse>(&value).map_err(
                    |error| NetworkError::Io {
                        operation: "decode captured native Fetch response".into(),
                        detail: error.to_string(),
                    },
                )?
            };
            let index = response
                .outbound_messages
                .iter()
                .position(|routed| {
                    matches!(
                        routed.message,
                        crate::DfsNetworkMessage::FetchBlobResponse(_)
                    )
                })
                .ok_or_else(|| NetworkError::Serialization {
                    detail: "negative Fetch fault did not capture a native Fetch reply".into(),
                })?;
            let actual = response.outbound_messages[index].clone();
            if actual.source_node_id != *target || actual.target_node_id != self.source {
                return Err(NetworkError::Serialization {
                    detail: "captured native Fetch reply has another source or destination".into(),
                });
            }
            {
                let mut records = captured.lock().expect("captured native Fetch replies");
                assert!(
                    records.len() < 32,
                    "bounded Fetch response capture required"
                );
                records.push(CapturedFetchReply {
                    rpc_request_id: response.request_id.clone(),
                    routed: actual,
                });
            }
            match change {
                FetchReplyFault::Missing => {
                    response.outbound_messages.remove(index);
                }
                FetchReplyFault::NotFound => {
                    let crate::DfsNetworkMessage::FetchBlobResponse(fetch) =
                        &mut response.outbound_messages[index].message
                    else {
                        unreachable!()
                    };
                    fetch.result = crate::DfsFetchBlobResult::NotFound;
                }
                FetchReplyFault::CorruptPayload | FetchReplyFault::CorruptPayloadAndLength => {
                    let crate::DfsNetworkMessage::FetchBlobResponse(fetch) =
                        &mut response.outbound_messages[index].message
                    else {
                        unreachable!()
                    };
                    let crate::DfsFetchBlobResult::Found { bytes, .. } = &mut fetch.result else {
                        return Err(NetworkError::Serialization {
                            detail: "negative payload fault requires captured Found bytes".into(),
                        });
                    };
                    let first = bytes
                        .first_mut()
                        .ok_or_else(|| NetworkError::Serialization {
                            detail: "negative payload fault requires nonempty captured bytes"
                                .into(),
                        })?;
                    *first ^= 0xff;
                    if matches!(change, FetchReplyFault::CorruptPayloadAndLength) {
                        bytes.push(0xff);
                    }
                }
            }
            let mut rewritten =
                ObjectRpcOutgoingResponse::encode_value(outgoing.response.route.clone(), response)?;
            rewritten.response.metadata = std::mem::take(&mut outgoing.response.metadata);
            outgoing = rewritten;
        }
        if let Some(FaultAction::CaptureStageExchangeAndLoseAfterDispatch(captured)) = &fault {
            let value = if outgoing.response.body.is_empty() {
                outgoing.body.collect_to_vec(64 * 1024).await?
            } else {
                outgoing.response.body.clone()
            };
            let response: DfsMessageResponse =
                sinomo_object_serde::from_bytes(&value).map_err(|error| {
                    NetworkError::Serialization {
                        detail: error.to_string(),
                    }
                })?;
            let (request, command, attachment, body_bytes) = captured_stage_head
                .expect("actual Stage exchange fault captured its received head");
            let stage = match &command.routed_message.message {
                DfsNetworkMessage::StageBlob(stage)
                | DfsNetworkMessage::StageRetainedBlob(stage) => stage,
                _ => unreachable!("Stage capture input checked before dispatch"),
            };
            let response_request_id = response.request_id.clone();
            let response_outbound_message_count = response.outbound_messages.len();
            let mut found = false;
            let mut records = captured.lock().expect("actual Stage exchange capture");
            for routed in response.outbound_messages {
                let DfsNetworkMessage::StageBlobResponse(reply) = &routed.message else {
                    continue;
                };
                if routed.source_node_id != *target
                    || routed.target_node_id != self.source
                    || reply.request_id != stage.request_id
                    || reply.intent_id != stage.intent_id
                    || reply.file_system_id != stage.file_system_id
                    || reply.object_id != stage.object_id
                    || reply.version != stage.version
                    || reply.replica_node_id != *target
                {
                    return Err(NetworkError::Serialization {
                        detail:
                            "actual Stage reply does not correlate with its captured native command"
                                .into(),
                    });
                }
                found = true;
                if records.exchanges.len() == 32 {
                    records.dropped_count = records.dropped_count.saturating_add(1);
                } else {
                    records.exchanges.push(CapturedStageExchange {
                        source_node_id: self.source.clone(),
                        target_node_id: target.clone(),
                        signer: signer.clone(),
                        request: request.clone(),
                        command: command.clone(),
                        attachment: attachment.clone(),
                        body_bytes,
                        observation: observation.clone(),
                        response_request_id: response_request_id.clone(),
                        response_outbound_message_count,
                        response: routed,
                    });
                }
            }
            if !found {
                return Err(NetworkError::Serialization {
                    detail: "actual native Stage exchange has no completed Stage reply".into(),
                });
            }
            return Err(NetworkError::delivery_failed(
                "shared DFS test lost captured Stage exchange response",
                NetworkDeliveryState::PossiblyApplied,
                "native Stage receiver completed before its original response delivery was discarded",
            ));
        }
        if let Some(FaultAction::CaptureWriteReplyAndLoseAfterDispatch(captured)) = &fault {
            if outgoing.response.route != observation.route
                || (!outgoing.response.body.is_empty() && !outgoing.body.is_empty())
            {
                return Err(NetworkError::Serialization { detail: "write capture requires its exact completed value response route, without a semantic attachment".into() });
            }
            let expected = {
                let state = write_reply_capture.as_ref().expect("selected write capture")
                    .lock().map_err(|_| NetworkError::Serialization { detail: "decoded write capture poisoned".into() })?;
                if state.invalid { return Err(NetworkError::Serialization { detail: "write capture has conflicting or unsupported decoded request".into() }); }
                state.identity.clone().ok_or_else(|| NetworkError::Serialization { detail: "write capture has no actual authenticated decoded request".into() })?
            };
            // The completed output is consumed exactly once and deliberately
            // never forwarded or re-encoded: the selected fault loses it.
            let value = if outgoing.response.body.is_empty() {
                outgoing.body.collect_to_vec(64 * 1024).await?
            } else {
                if outgoing.response.body.len() > 64 * 1024 { return Err(NetworkError::Serialization { detail: "bounded completed write capture exceeded".into() }); }
                outgoing.response.body.clone()
            };
            let response: DfsMessageResponse = sinomo_object_serde::from_bytes(&value)
                .map_err(|error| NetworkError::Serialization { detail: error.to_string() })?;
            let routed = captured_write_reply(&expected, &observation, response)?;
            let mut records = captured.lock().map_err(|_| NetworkError::Serialization { detail: "completed write capture poisoned".into() })?;
            if records.len() >= 32 { return Err(NetworkError::Serialization { detail: "bounded completed write capture count exceeded".into() }); }
            records.push(routed);
            return Err(NetworkError::delivery_failed(
                "shared DFS test lost captured write response", NetworkDeliveryState::PossiblyApplied,
                "native write receiver completed before its original response delivery was discarded",
            ));
        }
        if let Some(FaultAction::CaptureTransferReplyAndLoseAfterDispatch {
            intent_id,
            captured,
        }) = &fault
        {
            let value = if outgoing.response.body.is_empty() {
                outgoing.body.collect_to_vec(64 * 1024).await?
            } else {
                outgoing.response.body.clone()
            };
            let response: crate::DfsMessageResponse = sinomo_object_serde::from_bytes(&value)
                .map_err(|error| NetworkError::Serialization {
                    detail: error.to_string(),
                })?;
            let mut records = captured.lock().expect("captured native transfer replies");
            let mut found = false;
            for routed in response.outbound_messages {
                if !matches!(
                    &routed.message,
                    crate::DfsNetworkMessage::StageBlobResponse(_)
                        | crate::DfsNetworkMessage::RepairReplicaResponse(_)
                ) {
                    continue;
                }
                assert!(
                    records.len() < 32,
                    "bounded actual transfer capture required"
                );
                assert_eq!(routed.source_node_id, *target);
                assert_eq!(routed.target_node_id, self.source);
                if let Some(expected) = intent_id {
                    let crate::DfsNetworkMessage::RepairReplicaResponse(reply) = &routed.message
                    else {
                        panic!("filtered Repair loss requires an actual Repair reply");
                    };
                    assert_eq!(&reply.intent_id, expected);
                    assert_eq!(Some(&reply.request_id), observation.dfs_request_id.as_ref());
                    assert_eq!(
                        Some(&reply.file_system_id),
                        observation.dfs_file_system_id.as_ref()
                    );
                    assert_eq!(Some(&reply.object_id), observation.dfs_object_id.as_ref());
                    assert_eq!(Some(&reply.version), observation.dfs_version.as_ref());
                    assert_eq!(&reply.replica_node_id, target);
                }
                records.push(routed);
                found = true;
            }
            assert!(
                found,
                "transfer reply loss requires the actual native receiver's transfer frame"
            );
            return Err(NetworkError::delivery_failed(
                "shared DFS test lost captured transfer response",
                NetworkDeliveryState::PossiblyApplied,
                "native transfer receiver completed before its response was discarded",
            ));
        }
        if matches!(fault, Some(FaultAction::LoseReplyAfterDispatch)) {
            return Err(NetworkError::delivery_failed(
                "shared DFS test lost response",
                NetworkDeliveryState::PossiblyApplied,
                "native receiver completed before its response was discarded",
            ));
        }
        if let Some(FaultAction::CaptureCommitReplyAfterDispatch(captured)) = &fault {
            let value = if outgoing.response.body.is_empty() {
                outgoing.body.collect_to_vec(64 * 1024).await?
            } else {
                outgoing.response.body.clone()
            };
            let response: crate::DfsMessageResponse = sinomo_object_serde::from_bytes(&value)
                .map_err(|error| NetworkError::Serialization {
                    detail: error.to_string(),
                })?;
            let mut records = captured
                .lock()
                .expect("captured native CommitVersion replies");
            for routed in &response.outbound_messages {
                if matches!(
                    &routed.message,
                    crate::DfsNetworkMessage::CommitVersionResponse(_)
                ) {
                    assert!(
                        records.len() < 32,
                        "bounded native CommitVersion capture required"
                    );
                    assert_eq!(routed.source_node_id, *target);
                    assert_eq!(routed.target_node_id, self.source);
                    records.push(routed.clone());
                }
            }
            drop(records);
            let mut rewritten =
                ObjectRpcOutgoingResponse::encode_value(outgoing.response.route.clone(), response)?;
            rewritten.response.metadata = std::mem::take(&mut outgoing.response.metadata);
            outgoing = rewritten;
        }
        // Select only after the ordinary authenticated receiver completed and
        // released its execution/admission. A controller can now perform a real
        // normal publication without nesting a writer under that read owner.
        let stat_pause_rules = if observation.route == crate::DFS_DATA_NAMESPACE_ROUTE {
            self.fabric.faults.lock().unwrap().iter().filter_map(|rule| {
                let FaultAction::PauseStatProjectionReplyAfterDispatch {
                    object_id, gate, captured,
                } = &rule.action else { return None; };
                (rule.source.as_ref().is_none_or(|id| id == &observation.source)
                    && rule.target.as_ref().is_none_or(|id| id == &observation.target)
                    && rule.route.as_ref().is_none_or(|route| route == &observation.route)
                    && rule.origin.is_none_or(|origin| origin == observation.origin)
                    && rule.call_id.is_none_or(|id| id == observation.call_id))
                    .then(|| (object_id.clone(), Arc::clone(gate), Arc::clone(captured)))
            }).collect::<Vec<_>>()
        } else { Vec::new() };
        if !stat_pause_rules.is_empty() {
            let value = if outgoing.response.body.is_empty() {
                let value = outgoing.body.collect_to_vec(64 * 1024).await?;
                // Forward the exact collected bytes through the existing value
                // owner; bytes/shared_bytes alone would label them Attachment.
                let mut head = outgoing.response;
                head.body = value.clone();
                outgoing = ObjectRpcOutgoingResponse::value(head);
                value
            } else { outgoing.response.body.clone() };
            let response: crate::DfsMessageResponse = sinomo_object_serde::from_bytes(&value)
                .map_err(|error| NetworkError::Serialization { detail: error.to_string() })?;
            for routed in &response.outbound_messages {
                let crate::DfsNetworkMessage::NamespaceResponse(reply) = &routed.message else { continue; };
                let Some(crate::DfsPosixCoordinationResult::MetadataQuery(query)) = &reply.posix_coordination_result else { continue; };
                let (request, initial_page) = match &query.result {
                    crate::DfsMetadataQueryResult::StatProjectionV1 { page } =>
                        (&page.request, !page.request.fence_only && page.request.expected_prefix.is_none()),
                    crate::DfsMetadataQueryResult::StatProjectionChangedV1 { request, .. } =>
                        (request.as_ref(), false),
                    _ => continue,
                };
                for (object_id, gate, captured) in &stat_pause_rules {
                    if request.object_id != *object_id { continue; }
                    {
                        let mut observed = captured.lock().unwrap();
                        assert!(observed.len() < 32, "bounded actual stat capture");
                        observed.push(routed.clone());
                    }
                    let pause = if initial_page {
                        let mut rules = self.fabric.faults.lock().unwrap();
                        rules.iter_mut().find(|rule| {
                            matches!(&rule.action, FaultAction::PauseStatProjectionReplyAfterDispatch {
                                object_id: actual, gate: owned, captured: observed,
                            } if actual == object_id && Arc::ptr_eq(owned, gate) && Arc::ptr_eq(observed, captured))
                                && rule.remaining_hits > 0
                        }).is_some_and(|rule| { rule.remaining_hits -= 1; true })
                    } else { false };
                    if pause {
                        let permit = gate.acquire().await.map_err(|_| NetworkError::delivery_failed(
                            "native stat completed-response pause", NetworkDeliveryState::PossiblyApplied,
                            "case or node closed before original stat response release"))?;
                        permit.forget();
                    }
                }
            }
        }
        let stat_fault = if observation.route == crate::DFS_DATA_NAMESPACE_ROUTE {
            self.fabric
                .faults
                .lock()
                .unwrap()
                .iter()
                .enumerate()
                .find(|(_, rule)| {
                    matches!(rule.action, FaultAction::AlterStatReplyAfterDispatch { .. })
                        && rule
                            .source
                            .as_ref()
                            .is_none_or(|id| id == &observation.source)
                        && rule
                            .target
                            .as_ref()
                            .is_none_or(|id| id == &observation.target)
                        && rule
                            .route
                            .as_ref()
                            .is_none_or(|route| route == &observation.route)
                        && rule
                            .origin
                            .is_none_or(|origin| origin == observation.origin)
                        && rule.call_id.is_none_or(|id| id == observation.call_id)
                })
                .map(|(index, rule)| (index, rule.action.clone()))
        } else {
            None
        };
        if let Some((rule_index, FaultAction::AlterStatReplyAfterDispatch { change, captured })) =
            stat_fault
        {
            let value = if outgoing.response.body.is_empty() {
                outgoing.body.collect_to_vec(64 * 1024).await?
            } else {
                outgoing.response.body.clone()
            };
            let mut response: crate::DfsMessageResponse = sinomo_object_serde::from_bytes(&value)
                .map_err(|e| {
                NetworkError::Serialization {
                    detail: e.to_string(),
                }
            })?;
            for routed in &mut response.outbound_messages {
                let crate::DfsNetworkMessage::NamespaceResponse(reply) = &mut routed.message else {
                    continue;
                };
                let Some(crate::DfsPosixCoordinationResult::MetadataQuery(query)) =
                    &mut reply.posix_coordination_result
                else {
                    continue;
                };
                if !matches!(
                    query.result,
                    crate::DfsMetadataQueryResult::StatProjectionV1 { .. }
                        | crate::DfsMetadataQueryResult::StatAtPathV1 { .. }
                ) {
                    continue;
                }
                if matches!(change, StatReplyFault::SameVersionHashChanged)
                    && !matches!(&query.result,
                        crate::DfsMetadataQueryResult::StatProjectionV1 { page }
                            if page.object.is_some())
                {
                    continue;
                }
                {
                    let mut rules = self.fabric.faults.lock().unwrap();
                    let rule = &mut rules[rule_index];
                    // Bind to this actual call. Later collection/fence calls
                    // must execute normally so their checks, not fault-budget
                    // exhaustion, establish rejection of stale evidence.
                    rule.call_id.get_or_insert(observation.call_id);
                    if rule.remaining_hits == 0 {
                        self.fabric
                            .exhausted_budgets
                            .fetch_add(1, Ordering::Relaxed);
                        return Err(NetworkError::Io {
                            operation: "bounded stat negative reply".into(),
                            detail: "stat negative reply fault budget exhausted".into(),
                        });
                    }
                    rule.remaining_hits -= 1;
                }
                // Save the actual receiver-produced response before the negative edit.
                captured.lock().unwrap().push(crate::DfsRoutedMessage {
                    source_node_id: routed.source_node_id.clone(),
                    target_node_id: routed.target_node_id.clone(),
                    frame_id: routed.frame_id,
                    message_id: routed.message_id,
                    message: crate::DfsNetworkMessage::NamespaceResponse(reply.clone()),
                });
                let Some(crate::DfsPosixCoordinationResult::MetadataQuery(query)) =
                    &mut reply.posix_coordination_result
                else {
                    unreachable!()
                };
                match &change {
                    StatReplyFault::Unsupported => {
                        query.result = crate::DfsMetadataQueryResult::CurrentObject { object: None }
                    }
                    StatReplyFault::WrongEpoch => query.epoch += 1,
                    StatReplyFault::WrongSource => {
                        routed.source_node_id =
                            NodeId::from_legacy_named("forged-stat-source").unwrap()
                    }
                    StatReplyFault::WrongTarget => {
                        routed.target_node_id =
                            NodeId::from_legacy_named("forged-stat-target").unwrap()
                    }
                    _ => {
                        if let crate::DfsMetadataQueryResult::StatAtPathV1 { view } =
                            &mut query.result
                        {
                            match &change {
                                StatReplyFault::Incomplete => view.complete = !view.complete,
                                StatReplyFault::WrongRequest => {
                                    view.request.read_id = crate::DfsVersion::generate().to_string()
                                }
                                StatReplyFault::MalformedPrefix => {
                                    view.prefixes[0].prefix.digest = "invalid".into()
                                }
                                StatReplyFault::StalePrefix => {
                                    view.prefixes[0].prefix.journal_sequence += 1
                                }
                                _ => unreachable!(),
                            }
                            continue;
                        }
                        let crate::DfsMetadataQueryResult::StatProjectionV1 { page } =
                            &mut query.result
                        else {
                            unreachable!()
                        };
                        match &change {
                            StatReplyFault::Incomplete => page.complete = !page.complete,
                            StatReplyFault::WrongRequest => {
                                page.request.object_version = crate::DfsVersion::generate()
                            }
                            StatReplyFault::MalformedPrefix => {
                                page.prefix.digest = "invalid".into()
                            }
                            StatReplyFault::StalePrefix => {
                                page.prefix.journal_sequence =
                                    page.prefix.journal_sequence.saturating_add(1)
                            }
                            StatReplyFault::SameVersionHashChanged => {
                                let object = page.object.as_mut().expect("selected canonical page");
                                object.content_hash = Some(format!(
                                    "blake3:{}",
                                    blake3::hash(b"negative stat same-version hash corruption")
                                ));
                            }
                            _ => unreachable!(),
                        }
                    }
                }
            }
            let mut rewritten =
                ObjectRpcOutgoingResponse::encode_value(outgoing.response.route.clone(), response)?;
            rewritten.response.metadata = std::mem::take(&mut outgoing.response.metadata);
            outgoing = rewritten;
        }
        if let Some(FaultAction::AlterCommitAckAfterDispatch(change)) = fault {
            let value = if outgoing.response.body.is_empty() {
                outgoing.body.collect_to_vec(64 * 1024).await?
            } else {
                outgoing.response.body.clone()
            };
            let mut response: crate::DfsMessageResponse = sinomo_object_serde::from_bytes(&value)
                .map_err(|error| NetworkError::Io {
                operation: "decode captured native acknowledgement".into(),
                detail: error.to_string(),
            })?;
            if matches!(change, CommitAckFault::Missing) {
                response.outbound_messages.clear();
            } else {
                for message in &mut response.outbound_messages {
                    let DfsNetworkMessage::CommitVersionResponse(ack) = &mut message.message else {
                        continue;
                    };
                    match change {
                        CommitAckFault::Durable => {
                            let crate::DfsBlobTransferResult::Committed { content_hash } =
                                &ack.result
                            else {
                                continue;
                            };
                            ack.result = crate::DfsBlobTransferResult::Durable {
                                content_hash: content_hash.clone(),
                            };
                        }
                        CommitAckFault::WrongContent => {
                            if let crate::DfsBlobTransferResult::Committed { content_hash } =
                                &mut ack.result
                            {
                                *content_hash =
                                    crate::content_hash(b"corrupted native acknowledgement");
                            }
                        }
                        CommitAckFault::WrongObject => {
                            ack.object_id = crate::DfsObjectId::generate()
                        }
                        CommitAckFault::WrongSender => {
                            message.source_node_id = message.target_node_id.clone()
                        }
                        CommitAckFault::Missing => unreachable!(),
                    }
                }
            }
            let mut rewritten =
                ObjectRpcOutgoingResponse::encode_value(outgoing.response.route.clone(), response)?;
            rewritten.response.metadata = std::mem::take(&mut outgoing.response.metadata);
            outgoing = rewritten;
        }
        if let Some(FaultAction::PauseNativeReadBody {
            gate,
            started,
            forwarded_bytes,
            relay_hops,
        }) = &fault
        {
            assert!(
                *relay_hops <= 1,
                "bounded native read body forwarding required"
            );
            if outgoing.response.route != crate::DFS_DIRECT_READ_ROUTE || outgoing.body.is_empty() {
                return Err(NetworkError::Serialization {
                    detail: "native body pause requires the real direct-read attachment response"
                        .into(),
                });
            }
            let total_bytes = outgoing.body.total_bytes();
            let paused = PausedNativeReadBody {
                head: outgoing.response.clone(),
                source: Some(outgoing.body),
                incoming: None,
                total_bytes,
                gate: Arc::clone(gate),
                acquiring: None,
                permit: None,
                started: Arc::clone(started),
                forwarded_bytes: Arc::clone(forwarded_bytes),
                relay_hops: *relay_hops,
                fabric: Arc::clone(&self.fabric),
                source_node: target.clone(),
                target_node: self.source.clone(),
            };
            outgoing = ObjectRpcOutgoingResponse::new(
                outgoing.response,
                sinomo_core::network::ObjectRpcBodySource::async_reader(paused, total_bytes),
            );
        }
        let (result, response_transfer) = outgoing.into_test_result(executor().handle())?;
        self.fabric
            .transfer(target.clone(), self.source.clone(), response_transfer);
        observation.phase = RpcPhase::Completed;
        self.fabric.record(observation);
        _invocation.completed = true;
        Ok(result)
    }
    async fn invoke(
        &self,
        target: &NodeId,
        call: ObjectRpcCall,
        observation: RpcObservation,
    ) -> Result<ObjectRpcResult, NetworkError> {
        let dispatch_started = Arc::new(AtomicBool::new(false));
        let call_id = observation.call_id;
        let timeout = observation.options.overall_timeout();
        let kv_route =
            sinomo_core::kv::CLUSTER_KV_RPC_ROUTES.contains(&call.request.route.as_str());
        let operation = self.deliver(target, call, observation, dispatch_started.clone());
        let result = if let Some(timeout) = timeout {
            tokio::time::timeout(timeout, operation)
                .await
                .map_err(|_| {
                    NetworkError::timeout(
                        "shared DFS native RPC overall deadline",
                        NetworkTimeoutPhase::Overall,
                        timeout,
                        if dispatch_started.load(Ordering::Acquire) {
                            NetworkDeliveryState::PossiblyApplied
                        } else {
                            NetworkDeliveryState::DefinitelyNotSent
                        },
                    )
                })
                .and_then(std::convert::identity)
        } else {
            operation.await
        };
        if let Err(error) = &result {
            self.fabric.record_returned_error(call_id, error);
        }
        if kv_route {
            self.record_kv_target_outcome(
                target,
                &result,
                dispatch_started.load(Ordering::Acquire),
            );
        }
        result
    }

    /// Report a target-bound KV call as the routed client does after every
    /// call, so a dead KV leader's liveness evidence can unblock the
    /// request-owned election. A reply from the target's own handler, a
    /// refusal included, is an authenticated response.
    fn record_kv_target_outcome(
        &self,
        target: &NodeId,
        result: &Result<ObjectRpcResult, NetworkError>,
        reached_target: bool,
    ) {
        let source_data = self
            .fabric
            .nodes
            .lock()
            .expect("DFS test nodes")
            .get(&self.source)
            .map(|endpoint| Arc::clone(&endpoint.data_dir));
        let Some(source_data) = source_data else {
            return;
        };
        let paths = ClusterPaths::new(&source_data);
        let Ok(state) = sinomo_core::cluster::read_cluster_state_without_kv(&paths) else {
            return;
        };
        let error = match result {
            Err(error) if !(reached_target && error.delivery_state().is_none()) => Some(error),
            _ => None,
        };
        sinomo_core::network::record_cluster_target_rpc_outcome_for_test_carrier(
            &paths, &state, target, error,
        );
    }
}
/// Negative body-only pause over the receiver's actual source. Its native head
/// and all bytes are retained; SDK ownership rings and framing produce delivery.
struct PausedNativeReadBody {
    head: ObjectRpcResponse,
    source: Option<sinomo_core::network::ObjectRpcBodySource>,
    incoming: Option<sinomo_core::network::ObjectRpcIncomingBody>,
    total_bytes: Option<u64>,
    gate: Arc<Semaphore>,
    acquiring: Option<
        Pin<
            Box<
                dyn Future<
                        Output = Result<
                            tokio::sync::OwnedSemaphorePermit,
                            tokio::sync::AcquireError,
                        >,
                    > + Send,
            >,
        >,
    >,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    started: Arc<AtomicBool>,
    forwarded_bytes: Arc<AtomicUsize>,
    relay_hops: usize,
    fabric: Arc<Fabric>,
    source_node: NodeId,
    target_node: NodeId,
}
impl tokio::io::AsyncRead for PausedNativeReadBody {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
        output: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if output.remaining() == 0 {
            return std::task::Poll::Ready(Ok(()));
        }
        let finished_bytes = this
            .total_bytes
            .is_some_and(|total| this.forwarded_bytes.load(Ordering::Acquire) as u64 >= total);
        if !finished_bytes && this.permit.is_none() {
            if this.acquiring.is_none() {
                this.acquiring = Some(Box::pin(Arc::clone(&this.gate).acquire_owned()));
            }
            match this.acquiring.as_mut().unwrap().as_mut().poll(context) {
                std::task::Poll::Pending => return std::task::Poll::Pending,
                std::task::Poll::Ready(Err(error)) => {
                    return std::task::Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::Interrupted,
                        format!("native negative read-body gate closed: {error}"),
                    )));
                }
                std::task::Poll::Ready(Ok(permit)) => {
                    this.permit = Some(permit);
                    this.acquiring = None;
                }
            }
        }
        if this.incoming.is_none() {
            let source = this
                .source
                .take()
                .expect("actual native source consumed once");
            let outgoing = ObjectRpcOutgoingResponse::new(this.head.clone(), source);
            let (result, task) = match outgoing.into_test_result(executor().handle()) {
                Ok(result) => result,
                Err(error) => {
                    return std::task::Poll::Ready(Err(std::io::Error::other(error.to_string())));
                }
            };
            this.fabric
                .transfer(this.source_node.clone(), this.target_node.clone(), task);
            let (_, mut body) = result.into_parts();
            for _ in 0..this.relay_hops {
                let forwarded = ObjectRpcOutgoingResponse::new(
                    this.head.clone(),
                    sinomo_core::network::ObjectRpcBodySource::from_incoming_body(body),
                );
                let (result, task) = match forwarded.into_test_result(executor().handle()) {
                    Ok(result) => result,
                    Err(error) => {
                        return std::task::Poll::Ready(Err(std::io::Error::other(
                            error.to_string(),
                        )));
                    }
                };
                this.fabric
                    .transfer(this.source_node.clone(), this.target_node.clone(), task);
                body = result.into_parts().1;
            }
            this.incoming = Some(body);
            this.started.store(true, Ordering::Release);
        }
        let limit = output.remaining().min(32 * 1024);
        let mut bounded = tokio::io::ReadBuf::new(&mut output.initialize_unfilled()[..limit]);
        match tokio::io::AsyncRead::poll_read(
            Pin::new(this.incoming.as_mut().unwrap()),
            context,
            &mut bounded,
        ) {
            std::task::Poll::Pending => std::task::Poll::Pending,
            std::task::Poll::Ready(Err(error)) => std::task::Poll::Ready(Err(error)),
            std::task::Poll::Ready(Ok(())) => {
                let received = bounded.filled().len();
                output.advance(received);
                if received > 0 {
                    if let Some(permit) = this.permit.take() {
                        permit.forget();
                    }
                    this.forwarded_bytes.fetch_add(received, Ordering::AcqRel);
                }
                std::task::Poll::Ready(Ok(()))
            }
        }
    }
}

struct ReplySink(tokio::sync::oneshot::Sender<ObjectRpcOutgoingResponse>);
impl ResponseSink for ReplySink {
    fn send(self: Box<Self>, response: ObjectRpcOutgoingResponse) -> Result<(), ServiceError> {
        self.0
            .send(response)
            .map_err(|_| ServiceError::Service("shared DFS caller cancelled".into()))
    }
}
impl ComponentObjectRpcTransport for Carrier {
    fn availability(&self, target: &NodeId) -> sinomo_core::network::RoutedAvailability {
        let nodes = self
            .fabric
            .nodes
            .lock()
            .expect("DFS native callable endpoints");
        let callable = |node: &NodeId| {
            nodes
                .get(node)
                .is_some_and(|endpoint| endpoint.accepting.load(Ordering::Acquire))
        };
        // A mounted host has a path while it sends, ready or not; the
        // target must accept the call.
        let sends = match &self.host_identity {
            Some(host) => {
                host.sending.load(Ordering::Acquire)
                    && nodes
                        .get(&self.source)
                        .is_none_or(|source| Arc::ptr_eq(&host.generation, &source.host_generation))
            }
            None => callable(&self.source),
        };
        if sends && callable(target) {
            sinomo_core::network::RoutedAvailability::Candidate
        } else {
            sinomo_core::network::RoutedAvailability::NoPath
        }
    }

    fn call(
        &self,
        target: &NodeId,
        call: ObjectRpcCall,
        options: ClusterObjectRpcOptions,
        observe_connectivity: bool,
    ) -> Result<ObjectRpcResult, NetworkError> {
        let observation = self.observation(target, &call, options, observe_connectivity);
        self.fabric.record(observation.clone());
        run_rpc(Box::pin(self.invoke(target, call, observation)))
    }
    fn call_async<'a>(
        &'a self,
        target: &'a NodeId,
        call: ObjectRpcCall,
        options: ClusterObjectRpcOptions,
        observe_connectivity: bool,
    ) -> RpcFuture<'a> {
        let observation = self.observation(target, &call, options, observe_connectivity);
        self.fabric.record(observation.clone());
        Box::pin(async move { self.invoke(target, call, observation).await })
    }
}
struct Node {
    data_dir: Arc<SinomoDataDir>,
    kv_available: Arc<AtomicBool>,
    resources: sinomo_device::NodeResourceMonitor,
    reconciler: Option<ComponentReconciler>,
    mounted_context: Option<ComponentContext>,
    selected_original_fixture: Option<crate::runtime_host::DfsSelectedOriginalDataFixture>,
}

/// Read-only copies of actual namespace read heads at native Main ingress.
/// This owns no sender, reply, mutable body or publication capability.
pub struct NativeNamespaceReadCapture {
    records: Arc<Mutex<Vec<crate::DfsRoutedMessage>>>,
    omitted: Arc<AtomicUsize>,
    sealed: Arc<AtomicBool>,
    fabric: std::sync::Weak<Fabric>,
    _guard: crate::runtime_host::DaemonDfsPreDispatchTestHookGuard,
    _owner: Arc<crate::runtime_host::DfsDataOwnerLock>,
}
/// Actual collector DATA only; this supplies no execution or effect authority.
struct NamespaceCaptureRequirement {
    records: Vec<crate::DfsRoutedMessage>,
    omitted: usize,
}

#[derive(Debug, serde::Serialize)]
pub struct DfsTestObservationCoverageReport {
    pub namespace_records: Option<usize>,
    pub namespace_omitted: Option<usize>,
    pub lifetime_records: usize,
    pub lifetime_omitted: usize,
    pub full_lifetime_coverage: &'static str,
}

impl NativeNamespaceReadCapture {
    fn seal(self, runtime_fabric: &Arc<Fabric>) -> Result<NamespaceCaptureRequirement, String> {
        let Self {
            records,
            omitted,
            sealed,
            fabric,
            _guard,
            _owner,
        } = self;
        let requirement = {
            let captured = records.lock().map_err(|_| "namespace capture poisoned")?;
            // The same mutex orders closure against every actual copied head.
            // Later callbacks are outside this explicitly sealed observation window.
            sealed.store(true, Ordering::Release);
            NamespaceCaptureRequirement {
                records: captured.clone(),
                omitted: omitted.load(Ordering::Acquire),
            }
        };
        // The observation must retain no hook or data-owner borrow during shutdown.
        drop(_guard);
        drop(_owner);
        if fabric
            .upgrade()
            .is_none_or(|actual| !Arc::ptr_eq(&actual, runtime_fabric))
            || requirement.omitted != 0
            || requirement.records.is_empty()
        {
            return Err(
                "namespace close requires a nonempty uncensored capture from this actual runtime"
                    .into(),
            );
        }
        Ok(requirement)
    }

    pub fn records(&self) -> Vec<crate::DfsRoutedMessage> {
        let records = self.records.lock().expect("actual namespace read heads");
        assert_eq!(
            self.omitted.load(Ordering::Acquire),
            0,
            "native namespace read capture overflowed"
        );
        records.clone()
    }
}

/// Bounded observations of one ordinary served maintenance invocation. These
/// are returned native outcomes/events and the node's repair executor facts,
/// never ACKs or a prediction of repair completion.
#[derive(Debug)]
pub struct DfsTestMaintenanceObservation {
    pub outcome: Result<DfsMaintenanceTickOutcome, String>,
    /// Needs the node's repair executor tracked before and after the tick.
    pub repair_tracked_before: usize,
    pub repair_tracked_after: usize,
    pub committed_kv_events: Vec<sinomo_core::kv::KvEvent>,
    pub omitted_kv_event_count: usize,
}
#[cfg(test)]
#[derive(Clone, Debug)]
pub(crate) struct CapturedServedMessageWork {
    pub command: crate::DfsMessageCommand,
    pub committed_work: crate::DfsMessageCommittedWork,
    pub succeeded: bool,
}
#[cfg(test)]
pub(crate) struct NativeServedWorkCapture {
    records: Arc<Mutex<Vec<CapturedServedMessageWork>>>,
    omitted: Arc<AtomicUsize>,
    _guard: crate::runtime_host::DaemonDfsPreDispatchTestHookGuard,
}
#[cfg(test)]
impl NativeServedWorkCapture {
    pub fn records(&self) -> Vec<CapturedServedMessageWork> {
        assert_eq!(
            self.omitted.load(Ordering::Acquire),
            0,
            "bounded Main report capture overflowed"
        );
        self.records
            .lock()
            .expect("actual Main report capture")
            .clone()
    }
}

// These records are observations of an existing owner/permit, never authority.
#[cfg(test)]
mod placement_authority_observation {
    use super::*;

    #[derive(Clone, Debug)]
    pub(crate) struct PlacementAuthorityCall {
        pub job_id: String,
        /// Copied from the existing acquisition caller: None covers the whole
        /// job, while Some identifies an individual object's validation.
        pub path: Option<String>,
        pub holder_id: Option<String>,
        pub job_state: Option<crate::DfsPlacementReconciliationState>,
        pub released: Option<bool>,
        pub finished: bool,
    }

    struct Observations {
        owner: std::sync::Weak<crate::runtime_host::DfsDataOwnerLock>,
        calls: Mutex<Vec<PlacementAuthorityCall>>,
        omitted: AtomicUsize,
    }

    static OBSERVERS: OnceLock<Mutex<BTreeMap<std::path::PathBuf, std::sync::Weak<Observations>>>> =
        OnceLock::new();

    #[derive(Clone)]
    pub(crate) struct NativePlacementAuthorityReader(Arc<Observations>);
    impl NativePlacementAuthorityReader {
        pub(crate) fn records(&self) -> Vec<PlacementAuthorityCall> {
            assert_eq!(
                self.0.omitted.load(Ordering::Acquire),
                0,
                "placement authority observations must be complete"
            );
            self.0
                .calls
                .lock()
                .expect("placement authority observations")
                .clone()
        }
    }

    pub(crate) struct NativePlacementAuthorityCapture {
        root: std::path::PathBuf,
        reader: NativePlacementAuthorityReader,
    }
    impl NativePlacementAuthorityCapture {
        pub(crate) fn reader(&self) -> NativePlacementAuthorityReader {
            self.reader.clone()
        }
    }
    impl Drop for NativePlacementAuthorityCapture {
        fn drop(&mut self) {
            let mut observers = OBSERVERS
                .get()
                .unwrap()
                .lock()
                .expect("placement observers");
            if observers
                .get(&self.root)
                .and_then(std::sync::Weak::upgrade)
                .is_some_and(|actual| Arc::ptr_eq(&actual, &self.reader.0))
            {
                observers.remove(&self.root);
            }
        }
    }

    pub(crate) struct PlacementAuthorityCallObservation {
        observed: Arc<Observations>,
        index: usize,
    }
    impl std::fmt::Debug for PlacementAuthorityCallObservation {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("PlacementAuthorityCallObservation")
                .field("index", &self.index)
                .finish_non_exhaustive()
        }
    }
    impl PlacementAuthorityCallObservation {
        pub(crate) fn acquired(
            &mut self,
            holder_id: &str,
            state: crate::DfsPlacementReconciliationState,
        ) {
            let mut calls = self
                .observed
                .calls
                .lock()
                .expect("placement authority observations");
            calls[self.index].holder_id = Some(holder_id.into());
            calls[self.index].job_state = Some(state);
        }
        pub(crate) fn released(&mut self, succeeded: bool) {
            self.observed
                .calls
                .lock()
                .expect("placement authority observations")[self.index]
                .released = Some(succeeded);
        }
    }
    impl Drop for PlacementAuthorityCallObservation {
        fn drop(&mut self) {
            self.observed
                .calls
                .lock()
                .expect("placement authority observations")[self.index]
                .finished = true;
        }
    }

    pub(crate) fn observe_placement_authority_call(
        root: &std::path::Path,
        owner: &Arc<crate::runtime_host::DfsDataOwnerLock>,
        job_id: &str,
        path: Option<&str>,
    ) -> Option<PlacementAuthorityCallObservation> {
        let observed = OBSERVERS
            .get()?
            .lock()
            .expect("placement observers")
            .get(root)?
            .upgrade()?;
        if !std::sync::Weak::ptr_eq(&observed.owner, &Arc::downgrade(owner)) {
            return None;
        }
        let index = {
            let mut calls = observed
                .calls
                .lock()
                .expect("placement authority observations");
            if calls.len() == MAX_OBSERVATIONS {
                observed.omitted.fetch_add(1, Ordering::Release);
                return None;
            }
            let index = calls.len();
            calls.push(PlacementAuthorityCall {
                job_id: job_id.into(),
                path: path.map(str::to_owned),
                holder_id: None,
                job_state: None,
                released: None,
                finished: false,
            });
            index
        };
        Some(PlacementAuthorityCallObservation { observed, index })
    }

    impl DfsTestRuntime {
        pub(crate) fn capture_placement_authority_calls(
            &self,
            node: &NodeId,
        ) -> Result<NativePlacementAuthorityCapture, String> {
            let context = self.component_context(node)?;
            let owner = crate::runtime_host::retain_component_data_owner(&context)
                .map_err(|error| error.to_string())?;
            let root = context.data_dir().root();
            let observed = Arc::new(Observations {
                owner: Arc::downgrade(&owner),
                calls: Mutex::new(Vec::new()),
                omitted: AtomicUsize::new(0),
            });
            let mut observers = OBSERVERS
                .get_or_init(Default::default)
                .lock()
                .map_err(|_| "placement observer registry poisoned")?;
            if observers
                .get(&root)
                .and_then(std::sync::Weak::upgrade)
                .is_some()
            {
                return Err("actual owner already has a placement authority observer".into());
            }
            observers.insert(root.clone(), Arc::downgrade(&observed));
            Ok(NativePlacementAuthorityCapture {
                root,
                reader: NativePlacementAuthorityReader(observed),
            })
        }
    }
}
#[cfg(test)]
pub(crate) use placement_authority_observation::{
    PlacementAuthorityCallObservation, observe_placement_authority_call,
};

#[cfg(test)]
#[derive(Clone, Debug)]
pub(crate) struct CapturedCheckpointedBackgroundRepair {
    pub(crate) message: crate::DfsRoutedMessage,
    pub(crate) descriptor: crate::DfsRpcAttachmentDescriptor,
}

#[cfg(test)]
struct CheckpointedBackgroundRepairObservations {
    node: NodeId,
    limit: usize,
    records: Mutex<Vec<CapturedCheckpointedBackgroundRepair>>,
    omitted: AtomicUsize,
}

#[cfg(test)]
static CHECKPOINTED_BACKGROUND_REPAIR_OBSERVERS: OnceLock<
    Mutex<BTreeMap<std::path::PathBuf, std::sync::Weak<CheckpointedBackgroundRepairObservations>>>,
> = OnceLock::new();

#[cfg(test)]
pub(crate) struct NativeCheckpointedBackgroundRepairCapture {
    root: std::path::PathBuf,
    observed: Arc<CheckpointedBackgroundRepairObservations>,
    _owner: Arc<crate::runtime_host::DfsDataOwnerLock>,
}

#[cfg(test)]
impl NativeCheckpointedBackgroundRepairCapture {
    pub(crate) fn records(&self) -> Vec<CapturedCheckpointedBackgroundRepair> {
        self.observed
            .records
            .lock()
            .expect("checkpointed repair observations")
            .clone()
    }

    pub(crate) fn omitted_count(&self) -> usize {
        self.observed.omitted.load(Ordering::Acquire)
    }
}

#[cfg(test)]
impl Drop for NativeCheckpointedBackgroundRepairCapture {
    fn drop(&mut self) {
        let mut observers = CHECKPOINTED_BACKGROUND_REPAIR_OBSERVERS
            .get()
            .expect("registered checkpointed repair observer")
            .lock()
            .expect("checkpointed repair observers");
        if observers
            .get(&self.root)
            .and_then(std::sync::Weak::upgrade)
            .is_some_and(|current| Arc::ptr_eq(&current, &self.observed))
        {
            observers.remove(&self.root);
        }
    }
}

/// Observe actual prepared calls only after the normal host has checkpointed
/// repair attempts and released admission. No header, receipt or route fact is
/// built here, and no call reader is read, removed, drained or unparked.
#[cfg(test)]
pub(crate) fn observe_checkpointed_background_repairs(
    data_dir: &SinomoDataDir,
    worker: &crate::DfsNodeDetachedSendWorker,
) {
    let observed = CHECKPOINTED_BACKGROUND_REPAIR_OBSERVERS
        .get()
        .and_then(|observers| {
            observers
                .lock()
                .expect("checkpointed repair observers")
                .get(&data_dir.root())
                .and_then(std::sync::Weak::upgrade)
        });
    let Some(observed) = observed else {
        return;
    };
    let mut records = observed
        .records
        .lock()
        .expect("checkpointed repair observations");
    let (prepared, omitted) = worker.checkpointed_replica_messages(observed.limit);
    observed.omitted.fetch_add(omitted, Ordering::Release);
    for (message, descriptor) in prepared {
        assert_eq!(message.source_node_id, observed.node);
        assert!(matches!(&message.message,
            crate::DfsNetworkMessage::RepairReplica(command) if command.background_repair)
            || matches!(&message.message, crate::DfsNetworkMessage::RepairRetainedBlob(_)));
        if let Some(previous) = records.iter().find(|record| {
            record.message.source_node_id == message.source_node_id
                && record.message.target_node_id == message.target_node_id
                && record.message.frame_id == message.frame_id
                && record.message.message_id == message.message_id
        }) {
            assert_eq!(
                serde_json::to_value(&previous.message).unwrap(),
                serde_json::to_value(&message).unwrap(),
                "duplicate actual routing identity changes its complete native header",
            );
            assert_eq!(
                serde_json::to_value(&previous.descriptor).unwrap(),
                serde_json::to_value(&descriptor).unwrap(),
                "duplicate actual routing identity changes its native body descriptor",
            );
            continue;
        }
        if records.len() == observed.limit {
            observed.omitted.fetch_add(1, Ordering::Release);
        } else {
            records.push(CapturedCheckpointedBackgroundRepair {
                message,
                descriptor,
            });
        }
    }
}

/// A negative completion scope and the original native DATA it actually saw.
/// No scheduler guard, native DATA-owner Arc or publication permit is retained.
#[cfg(test)]
pub(crate) struct DeferredNativeGenesis {
    data_root: std::path::PathBuf,
    shard: DfsMetadataShardConfig,
    original_record: DfsPreparedOperationRecord,
    deferral: crate::default_cluster::genesis::NativeGenesisCompletionDeferral,
}

#[cfg(test)]
impl DeferredNativeGenesis {
    pub(crate) fn shard(&self) -> &DfsMetadataShardConfig {
        &self.shard
    }
    pub(crate) fn original_record(&self) -> &DfsPreparedOperationRecord {
        &self.original_record
    }
    pub(crate) fn deferral_hits(&self) -> usize {
        self.deferral.hits()
    }
}

#[cfg(all(test, unix))]
mod retained_fixture;
#[cfg(all(test, unix))]
pub(crate) use retained_fixture::RetainedNativeHealthyFixture;

#[cfg(all(test, unix))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetainedNativeGenesisNode {
    pub(crate) node_id: NodeId,
    pub(crate) data_root: std::path::PathBuf,
    pub(crate) data_identity: (u64, u64),
    pub(crate) owner_identity: (u64, u64),
    pub(crate) raft_root_identity: (u64, u64),
    pub(crate) raft_lock_identity: (u64, u64),
    pub(crate) cluster_state: sinomo_core::ClusterBootstrapState,
    pub(crate) raft_term: u64,
    pub(crate) raft_commit_index: u64,
}

/// Test-owned expectations captured before process death; not a native owner,
/// transport signer, configuration proof or authorization to adopt old DATA.
#[cfg(all(test, unix))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetainedNativeGenesisFixture {
    pub(crate) format_version: u32,
    pub(crate) fixture_root: std::path::PathBuf,
    pub(crate) fixture_identity: (u64, u64),
    pub(crate) primary: NodeId,
    pub(crate) nodes: Vec<RetainedNativeGenesisNode>,
    pub(crate) shard: DfsMetadataShardConfig,
    pub(crate) shard_identity: (u64, u64),
    pub(crate) original_record: DfsPreparedOperationRecord,
    pub(crate) original_frame: Vec<u8>,
}

#[cfg(all(test, unix))]
fn retained_native_path_identity(
    path: &std::path::Path,
    directory: bool,
) -> Result<(u64, u64), String> {
    use std::os::unix::fs::MetadataExt;
    let before = std::fs::symlink_metadata(path).map_err(|e| e.to_string())?;
    if !path.is_absolute()
        || path.canonicalize().map_err(|e| e.to_string())?.as_path() != path
        || before.file_type().is_symlink()
        || before.is_dir() != directory
        || (!directory && !before.is_file())
    {
        return Err(format!(
            "retained native source is missing, aliased or has the wrong physical type: {}",
            path.display()
        ));
    }
    let file = std::fs::File::open(path).map_err(|e| e.to_string())?;
    let opened = file.metadata().map_err(|e| e.to_string())?;
    let after = std::fs::symlink_metadata(path).map_err(|e| e.to_string())?;
    let expected = (opened.dev(), opened.ino());
    if (before.dev(), before.ino()) != expected || (after.dev(), after.ino()) != expected {
        return Err("retained native source changed while reading physical identity".into());
    }
    Ok(expected)
}

#[cfg(all(test, unix))]
fn retained_native_original_data(
    data: &SinomoDataDir,
    shard: &DfsMetadataShardConfig,
    expected: &DfsPreparedOperationRecord,
) -> Result<Vec<u8>, String> {
    expected.validate().map_err(|e| e.to_string())?;
    if shard.epoch != 1
        || !shard.replica_node_ids.is_empty()
        || expected.status != DfsPreparedOperationStatus::Prepared
        || expected.committed_entry.is_some()
        || expected.digest.file_system_id != shard.file_system_id
        || expected.digest.shard_id.0 != shard.shard_id
        || expected.digest.owner_epoch != 1
        || expected.digest.operation_id
            != crate::default_cluster::genesis::genesis_operation_id(shard)
    {
        return Err(
            "retained original is not the exact supported incomplete native singleton".into(),
        );
    }
    let id = DfsMetadataShardId::new(&shard.shard_id);
    let root = crate::local_dfs_metadata_shard_path(data, &id);
    retained_native_path_identity(&root, true)?;
    crate::selected_fixture::SelectedOriginalFixtureIdentity::reject_default_root(&root)
        .map_err(|e| e.to_string())?;
    if crate::metadata_api::read_handoff_freeze_with_layout(
        &root.join("journal/membership-handoff-write-freeze.sobj"),
        None,
    )
    .map_err(|e| e.to_string())?
    .is_some()
    {
        return Err("retained incomplete singleton has a cooperative or recovery marker".into());
    }
    let _guard = crate::metadata_lock::try_metadata_inspection_lock(
        root.clone(),
        crate::metadata_cursor::journal_write_lock_path(&root),
    )
    .map_err(|e| e.to_string())?
    .ok_or("retained native original is busy or lacks its actual writer-lock file")?;
    let backend = crate::LocalSobjMetadataBackend::open_for_inspection(id.clone(), root.clone())
        .map_err(|e| e.to_string())?;
    let actual = backend
        .prepared_operation_record(&expected.digest.operation_id)
        .map_err(|e| e.to_string())?
        .ok_or("retained native original body is unavailable")?;
    if actual != *expected {
        return Err(
            "retained native original body/digest/status differs from precrash DATA".into(),
        );
    }
    let frames = crate::metadata_journal::read_segmented_journal_frames_at_sequences(
        &root,
        &[expected.digest.prepare_sequence],
    )
    .map_err(|e| e.to_string())?;
    let frame = frames
        .into_iter()
        .next()
        .flatten()
        .ok_or("retained native original exact prepare frame is unavailable")?;
    if !matches!(&frame.entry().mutation,DfsMetadataMutation::PrepareOperationV7 { record,.. } if record.as_ref()==expected)
    {
        return Err(
            "retained native original prepare frame differs from its materialized body".into(),
        );
    }
    Ok(frame.exact_bytes().to_vec())
}

#[cfg(all(test, unix))]
impl DfsTestRuntime {
    pub(crate) fn capture_retained_genesis_fixture(
        &self,
        phase: &DeferredNativeGenesis,
    ) -> Result<RetainedNativeGenesisFixture, String> {
        if self.nodes.len() != 3
            || !self.component_factories.is_empty()
            || self
                .nodes
                .values()
                .any(|node| node.selected_original_fixture.is_some() || node.reconciler.is_none())
        {
            return Err(
                "retained restart captures only the actual three-node default P0 fixture".into(),
            );
        }
        if self.read_deferred_native_genesis(phase)? != phase.original_record {
            return Err("precrash native original changed".into());
        }
        let (fixture_root, nodes) = self.capture_retained_native_nodes()?;
        let data = self.data_dir(&phase.shard.primary_node_id);
        let result = RetainedNativeGenesisFixture {
            format_version: 1,
            fixture_identity: retained_native_path_identity(&fixture_root, true)?,
            fixture_root,
            primary: self.primary.clone(),
            nodes,
            shard: phase.shard.clone(),
            shard_identity: retained_native_path_identity(
                &crate::local_dfs_metadata_shard_path(
                    data,
                    &DfsMetadataShardId::new(&phase.shard.shard_id),
                ),
                true,
            )?,
            original_frame: retained_native_original_data(
                data,
                &phase.shard,
                &phase.original_record,
            )?,
            original_record: phase.original_record.clone(),
        };
        if serde_json::to_vec(&result)
            .map_err(|e| e.to_string())?
            .len()
            > 1024 * 1024
        {
            return Err("retained fixture expectation exceeds 1MiB".into());
        }
        Ok(result)
    }

    /// Validate expected DATA before a writable native opener. Parent has
    /// already verified its own child's death/reap and saved raw files.
    pub(crate) fn validate_retained_genesis_fixture(
        expected: &RetainedNativeGenesisFixture,
    ) -> Result<(), String> {
        if expected.format_version != 1
            || expected.nodes.len() != 3
            || serde_json::to_vec(expected)
                .map_err(|e| e.to_string())?
                .len()
                > 1024 * 1024
            || retained_native_path_identity(&expected.fixture_root, true)?
                != expected.fixture_identity
        {
            return Err("retained native fixture version/bounds/root identity differs".into());
        }
        if expected.shard.primary_node_id != expected.primary {
            return Err("retained native fixture roster or original primary differs".into());
        }
        let _locks = Self::validate_retained_native_nodes(
            &expected.fixture_root,
            &expected.primary,
            &expected.nodes,
        )?;
        let primary = expected
            .nodes
            .iter()
            .find(|n| n.node_id == expected.primary)
            .ok_or("retained primary missing")?;
        let data = SinomoDataDir::new(&primary.data_root);
        if retained_native_path_identity(
            &crate::local_dfs_metadata_shard_path(
                &data,
                &DfsMetadataShardId::new(&expected.shard.shard_id),
            ),
            true,
        )? != expected.shard_identity
            || retained_native_original_data(&data, &expected.shard, &expected.original_record)?
                != expected.original_frame
        {
            return Err("retained native genesis physical root or exact frame changed".into());
        }
        if retained_native_path_identity(
            &crate::local_dfs_metadata_shard_path(
                &data,
                &DfsMetadataShardId::new(&expected.shard.shard_id),
            ),
            true,
        )? != expected.shard_identity
        {
            return Err("retained native genesis root changed across original validation".into());
        }
        Ok(())
    }

    pub(crate) fn reopen_retained_genesis_fixture(
        expected: RetainedNativeGenesisFixture,
    ) -> Result<(Self, DeferredNativeGenesis, Result<(), String>), String> {
        Self::validate_retained_genesis_fixture(&expected)?;
        let (mut actual, primary_data) = Self::reconstruct_retained_native_fixture(
            &expected.fixture_root,
            &expected.primary,
            &expected.nodes,
        )?;
        let primary = expected
            .nodes
            .iter()
            .find(|n| n.node_id == expected.primary)
            .expect("validated primary");
        let deferral =
            crate::default_cluster::genesis::NativeGenesisCompletionDeferral::restore_before_mount(
                &primary_data,
                &expected.shard,
                &expected.original_record,
                primary.data_identity,
                primary.owner_identity,
            )
            .map_err(|e| e.to_string())?;
        let phase = DeferredNativeGenesis {
            data_root: primary_data.root(),
            shard: expected.shard.clone(),
            original_record: expected.original_record.clone(),
            deferral,
        };
        actual.open_retained_native_raft(&expected.nodes)?;
        let keys = vec![
            crate::metadata_shard_key(&expected.shard.file_system_id, &expected.shard.shard_id),
            crate::metadata_shard_membership_handoff_key(
                &expected.shard.file_system_id,
                &expected.shard.shard_id,
            ),
        ];
        run_unit(Box::pin(async {
            let read = tokio::time::timeout(
                sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
                actual.authority.kv_get_authoritative_exact_set_async(keys),
            )
            .await
            .map_err(|_| {
                "retained fixture real ReadIndex reached its original five-second deadline"
                    .to_string()
            })?
            .map_err(|e| e.to_string())?;
            let [Some(assignment), None] = read.value.as_slice() else {
                return Err(
                    "retained fixture ReadIndex has no exact unhanded singleton assignment".into(),
                );
            };
            if assignment
                .decode::<DfsMetadataShardConfig>()
                .map_err(|e| e.to_string())?
                != expected.shard
            {
                return Err("retained fixture authoritative assignment changed".into());
            }
            Ok(())
        }))?;
        let ids = expected.nodes.iter().map(|n| n.node_id.clone()).collect();
        let startup = actual.start_nodes_after_projection(
            ids,
            crate::service_trace::DfsPhaseTimer::new("retained-native-genesis-restart"),
        );
        if startup.is_ok() {
            let readback = actual.read_deferred_native_genesis(&phase).and_then(|record| {
                if record == expected.original_record { Ok(()) } else {
                    Err("retained native startup changed original genesis before explicit completion".into())
                }
            });
            if let Err(error) = readback {
                // Keep the negative hold alive through normal cleanup; an
                // error must not release it first while mounted owners run.
                let cleanup = actual.close_inner(None);
                return Err(format!("{error}; native cleanup: {cleanup:?}"));
            }
        }
        Ok((actual, phase, startup))
    }
}

pub struct DfsTestRuntime {
    root: Option<sinomo_testdir::TestDir>,
    #[cfg(test)]
    retained_fixture_root: Option<std::path::PathBuf>,
    state: sinomo_core::ClusterBootstrapState,
    primary: NodeId,
    authority: Arc<dyn ClusterAccess>,
    kv_peers: Arc<crate::runtime_host::DfsTestClusterPeerBridge>,
    kv_bootstrapped: bool,
    /// Every node is a cluster-KV Raft voter, promoted through the production
    /// lifecycle at startup. Otherwise the bootstrap node is the sole voter
    /// and every other node a learner.
    kv_voters: bool,
    resident_pins: BTreeMap<NodeId, sinomo_core::kv::ResidentRaftNodeRegistryPin>,
    fabric: Arc<Fabric>,
    nodes: BTreeMap<NodeId, Node>,
    closed: bool,
    component_factories: Vec<Arc<dyn sinomo_module::ComponentFactory>>,
}
impl DfsTestRuntime {

    /// Read-only observation of actual Main background preparation. This does
    /// not publish network health or change the producer/dispatch policy.
    #[cfg(test)]
    pub(crate) fn capture_checkpointed_background_repairs(
        &self,
        node: &NodeId,
        max_records: usize,
    ) -> Result<NativeCheckpointedBackgroundRepairCapture, String> {
        assert!(
            (1..=32).contains(&max_records),
            "bounded repair observation required"
        );
        let context = self.component_context(node)?;
        let owner = crate::runtime_host::retain_component_data_owner(&context)
            .map_err(|error| error.to_string())?;
        let observed = Arc::new(CheckpointedBackgroundRepairObservations {
            node: node.clone(),
            limit: max_records,
            records: Mutex::new(Vec::new()),
            omitted: AtomicUsize::new(0),
        });
        let root = context.data_dir().root();
        let mut observers = CHECKPOINTED_BACKGROUND_REPAIR_OBSERVERS
            .get_or_init(|| Mutex::new(BTreeMap::new()))
            .lock()
            .map_err(|_| "checkpointed repair observer registry poisoned")?;
        if observers
            .get(&root)
            .and_then(std::sync::Weak::upgrade)
            .is_some()
        {
            return Err("actual owner already has a checkpointed repair observer".into());
        }
        observers.insert(root.clone(), Arc::downgrade(&observed));
        Ok(NativeCheckpointedBackgroundRepairCapture {
            root,
            observed,
            _owner: owner,
        })
    }

    /// Observe the SAME decoded lookup/poll request before normal Main work.
    /// No body is consumed, reply manufactured, or authority state changed.
    pub fn capture_namespace_reads(
        &self,
        node: &NodeId,
        max_records: usize,
    ) -> Result<NativeNamespaceReadCapture, String> {
        self.capture_namespace_requests_matching(node, max_records, |operation| {
            matches!(
                operation,
                crate::DfsNamespaceOperation::LookupPath { .. }
                    | crate::DfsNamespaceOperation::PollNamespaceChanges { .. }
            )
        })
    }

    /// Copy actual decoded namespace heads at the same normal Main boundary.
    /// This bounded window neither resets global observations nor changes work.
    pub fn capture_namespace_requests(
        &self,
        node: &NodeId,
        max_records: usize,
    ) -> Result<NativeNamespaceReadCapture, String> {
        self.capture_namespace_requests_matching(node, max_records, |_| true)
    }

    fn capture_namespace_requests_matching(
        &self,
        node: &NodeId,
        max_records: usize,
        matches_operation: fn(&crate::DfsNamespaceOperation) -> bool,
    ) -> Result<NativeNamespaceReadCapture, String> {
        assert!(
            (1..=32).contains(&max_records),
            "bounded namespace read capture required"
        );
        let context = self.component_context(node)?;
        let owner = crate::runtime_host::retain_component_data_owner(&context)
            .map_err(|error| error.to_string())?;
        let records = Arc::new(Mutex::new(Vec::new()));
        let omitted = Arc::new(AtomicUsize::new(0));
        let sealed = Arc::new(AtomicBool::new(false));
        let window_sealed = Arc::clone(&sealed);
        let observed = Arc::clone(&records);
        let dropped = Arc::clone(&omitted);
        let expected = node.clone();
        let hook =
            Arc::new(
                move |point: crate::runtime_host::DaemonDfsPreDispatchTestPoint<'_>,
                      command: Option<&crate::DfsMessageCommand>,
                      _preflight: Option<
                    &crate::runtime_host::DaemonDfsMetadataAuthorityPreflight,
                >| {
                    if !matches!(
                        point,
                        crate::runtime_host::DaemonDfsPreDispatchTestPoint::BeforeExecute { .. }
                    ) {
                        return Ok(());
                    }
                    let Some(command) = command else {
                        return Ok(());
                    };
                    let crate::DfsNetworkMessage::NamespaceRequest(request) =
                        &command.routed_message.message
                    else {
                        return Ok(());
                    };
                    if !matches_operation(&request.operation) {
                        return Ok(());
                    }
                    assert_eq!(command.routed_message.target_node_id, expected);
                    let mut captured = observed.lock().expect("actual namespace read capture");
                    if window_sealed.load(Ordering::Acquire) {
                        return Ok(());
                    }
                    if captured.len() == max_records {
                        dropped.fetch_add(1, Ordering::Release);
                    } else {
                        captured.push(command.routed_message.clone());
                    }
                    Ok(())
                },
            );
        let guard = crate::runtime_host::install_daemon_dfs_pre_dispatch_test_hook(
            context.data_dir(),
            hook,
        );
        Ok(NativeNamespaceReadCapture {
            records,
            omitted,
            sealed,
            fabric: Arc::downgrade(&self.fabric),
            _guard: guard,
            _owner: owner,
        })
    }

    /// Borrow the executor already used by the normal embedded fixture.
    pub fn runtime_handle(&self) -> tokio::runtime::Handle {
        executor().handle().clone()
    }

    /// Define the complete roster and start the normal embedded owners.
    pub fn start_cluster(name: &str, node_count: usize) -> Result<(Self, Vec<NodeId>), String> {
        if node_count == 0 {
            return Err("native DFS cluster requires at least one node".into());
        }
        let nodes = Self::ordered_node_ids(name, node_count);
        let mut runtime = Self::new(name, nodes[0].clone())?;
        for node in &nodes[1..] {
            runtime.define_node(node.clone())?;
        }
        runtime.start_nodes()?;
        Ok((runtime, nodes))
    }

    /// [`Self::start_cluster`] with every node a cluster-KV Raft voter, so
    /// losing any one node, the KV leader included, leaves a voter quorum.
    pub fn start_cluster_with_voters(
        name: &str,
        node_count: usize,
    ) -> Result<(Self, Vec<NodeId>), String> {
        if node_count == 0 {
            return Err("native DFS cluster requires at least one node".into());
        }
        let nodes = Self::ordered_node_ids(name, node_count);
        let mut runtime = Self::new(name, nodes[0].clone())?;
        runtime.use_kv_voters()?;
        for node in &nodes[1..] {
            runtime.define_node(node.clone())?;
        }
        runtime.start_nodes()?;
        Ok((runtime, nodes))
    }

    /// Make every node a cluster-KV Raft voter. Nodes still join as learners;
    /// startup promotes each through the production state-authority
    /// lifecycle (catch-up proof, then the quorum-backed configuration
    /// change). Must precede startup.
    pub fn use_kv_voters(&mut self) -> Result<(), String> {
        if self.kv_bootstrapped || self.nodes.values().any(|node| node.reconciler.is_some()) {
            return Err("choose the KV voter topology before native startup".into());
        }
        self.kv_voters = true;
        Ok(())
    }

    /// Whether every node is a cluster-KV Raft voter.
    pub fn kv_voters(&self) -> bool {
        self.kv_voters
    }

    /// Start the SAME normal embedded owner for an explicitly supplied node.
    /// The returned client uses the real hosted transport/backend/context; the
    /// caller drops its filesystem users before ordinary runtime shutdown.
    pub fn start_single_node(
        name: &str,
        node: NodeId,
    ) -> Result<(Self, NetworkedDfsService<DfsNodeTransport>), String> {
        let mut runtime = Self::new(name, node.clone())?;
        runtime.start_nodes()?;
        let client = runtime.client(&node)?;
        Ok((runtime, client))
    }

    /// Total node count; the normal planner's root owner is the first sorted ID.
    pub fn ordered_node_ids(prefix: &str, total_number_nodes: usize) -> Vec<NodeId> {
        assert!(total_number_nodes > 0);
        let mut nodes = (0..total_number_nodes)
            .map(|index| {
                NodeId::from_legacy_named(&format!("{prefix}-{index}"))
                    .expect("normal fixture node ID")
            })
            .collect::<Vec<_>>();
        nodes.sort();
        nodes
    }
    /// Supply a deliberate protocol frame without adding a service transport type.
    /// Healthy journal/snapshot ACKs must come from the real hosted receiver.
    pub fn queue_protocol_message(
        transport: &mut DfsNodeTransport,
        source: NodeId,
        target: NodeId,
        message: DfsNetworkMessage,
    ) {
        assert!(
            matches!(
                &message,
                DfsNetworkMessage::ReadResponse(_) | DfsNetworkMessage::CommitVersion(_)
            ),
            "this shared queue hook is only for the two explicit protocol regressions"
        );
        transport.test_enqueue_protocol_message(build_dfs_routed_message(
            &source, &target, 1, 1, message,
        ));
    }

    /// Preserve the actual decoded routing/request/content tuple before one
    /// negative inbox alteration. This utility can never enqueue a reply.
    pub fn alter_queued_read_replies(
        &self,
        transport: &mut DfsNodeTransport,
        change: ReadReplyFault,
        max_replies: usize,
    ) -> Vec<crate::DfsRoutedMessage> {
        let captured = transport.test_alter_queued_read_replies(change, max_replies);
        let observed = self.rpc_observations();
        for original in &captured {
            assert!(
                observed
                    .iter()
                    .any(|event| event.source == original.target_node_id
                        && event.target == original.source_node_id
                        && [crate::DFS_DIRECT_READ_ROUTE, crate::DFS_DATA_READ_ROUTE]
                            .contains(&event.route.as_str())
                        && event.phase == RpcPhase::Completed
                        && event.origin == RpcOrigin::RegisteredHost),
                "a queued negative read control requires the native receiver's completed real RPC"
            );
        }
        captured
    }

    /// Negative time control only: make an actual outstanding read eligible for
    /// pruning, after binding it to its native completed queued reply. It never
    /// inserts state, changes an identity, completes work or grants authority.
    pub fn expire_queued_native_read(
        &self,
        service: &mut NetworkedDfsService<DfsNodeTransport>,
        request_id: &str,
    ) -> std::time::Instant {
        let original = service
            .transport
            .test_queued_read_reply(request_id)
            .expect("negative time control requires an actual queued read reply");
        let DfsNetworkMessage::ReadResponse(response) = &original.message else {
            unreachable!()
        };
        let pending = service
            .reads
            .pending_reads
            .get(request_id)
            .expect("actual generated pending read");
        assert_eq!(pending.object_id, response.object_id);
        assert_eq!(pending.target_node_id, original.source_node_id);
        let crate::DfsReadResult::Found(data) = &response.result else {
            panic!("actual native Found reply required")
        };
        assert_eq!(pending.object_id, data.object_id);
        assert_eq!(pending.version, data.version);
        assert_eq!(pending.content_hash, data.content_hash);
        assert_eq!(pending.content_size_bytes, Some(data.bytes.len() as u64));
        assert!(self.rpc_observations().iter().any(|event| event.source
            == original.target_node_id
            && event.target == original.source_node_id
            && event.route == crate::DFS_DIRECT_READ_ROUTE
            && event.phase == RpcPhase::Completed
            && event.origin == RpcOrigin::RegisteredHost));
        let old = pending.next_retry_at;
        service
            .reads
            .pending_reads
            .get_mut(request_id)
            .unwrap()
            .next_retry_at = std::time::Instant::now()
            - crate::DFS_PENDING_TRANSFER_STALE_AFTER
            - Duration::from_millis(1);
        old
    }

    pub fn local_transport_with_drain_counter(
        local: NodeId,
        counter: Arc<std::sync::Mutex<usize>>,
    ) -> DfsNodeTransport {
        Self::local_transport(local).test_with_drain_counter(counter)
    }

    pub fn local_transport(local: NodeId) -> DfsNodeTransport {
        crate::runtime_host::daemon_dfs_node_transport(local, Vec::<NodeId>::new())
    }
    pub fn new(name: &str, primary: NodeId) -> Result<Self, String> {
        Self::new_with_path_alias(name, primary, false)
    }
    /// The SAME normal mounted fixture at an aliased physical data directory.
    /// Only source path identity varies; no host/authority/runtime path changes.
    #[cfg(unix)]
    pub fn new_with_data_dir_alias(name: &str, primary: NodeId) -> Result<Self, String> {
        Self::new_with_path_alias(name, primary, true)
    }
    fn new_with_path_alias(name: &str, primary: NodeId, alias: bool) -> Result<Self, String> {
        initialize_opt_in_trace();
        let root = sinomo_testdir::TestDir::new(name);
        let physical = root.join(primary.as_str());
        let data_root = if alias {
            #[cfg(unix)]
            {
                std::fs::create_dir_all(&physical).map_err(|error| error.to_string())?;
                let path = root.join("actual-data-dir-alias");
                std::os::unix::fs::symlink(&physical, &path).map_err(|error| error.to_string())?;
                path
            }
            #[cfg(not(unix))]
            {
                return Err("path alias fixture requires platform symlink support".into());
            }
        } else {
            physical
        };
        Self::initialize_in_data_dir(name, primary, root, SinomoDataDir::new(data_root))
    }

    fn initialize_in_data_dir(
        name: &str,
        primary: NodeId,
        root: sinomo_testdir::TestDir,
        data_dir: SinomoDataDir,
    ) -> Result<Self, String> {
        data_dir.ensure_root_layout().map_err(|e| e.to_string())?;
        let mut state = sinomo_core::ClusterBootstrapState::initialized_locally(
            ClusterId::from_legacy_named(name).map_err(|e| e.to_string())?,
            name,
            primary.clone(),
            name,
            format!("pk-{primary}"),
            OffsetDateTime::now_utc(),
        );
        for member in &mut state.members {
            member.capabilities = sinomo_core::node::main_daemon_capabilities(&member.role);
            member.allowed_capabilities = member.capabilities.clone();
        }
        let paths = ClusterPaths::new(&data_dir);
        sinomo_core::cluster::write_cluster_state(&paths, &state).map_err(|e| e.to_string())?;
        let kv_peers = Arc::new(crate::runtime_host::DfsTestClusterPeerBridge::default());
        let authority = crate::runtime_host::test_cluster_access_with_peers_for_runtime(
            &data_dir,
            Some(Arc::clone(&kv_peers)),
        );
        let fabric = Arc::new(Fabric {
            dropped_observations: AtomicUsize::new(0),
            exhausted_budgets: AtomicUsize::new(0),
            next_call: AtomicU64::new(1),
            nodes: Mutex::new(BTreeMap::new()),
            faults: Mutex::new(Vec::new()),
            observations: Mutex::new(Vec::new()),
            observation_checkpoints: Mutex::new(Vec::new()),
            transfers: Mutex::new(Vec::new()),
            partitions: Mutex::new(BTreeMap::new()),
        });
        let mut nodes = BTreeMap::new();
        nodes.insert(
            primary.clone(),
            Node {
                data_dir: Arc::new(data_dir),
                kv_available: Arc::new(AtomicBool::new(false)),
                resources: sinomo_device::NodeResourceMonitor::default(),
                reconciler: None,
                mounted_context: None,
                selected_original_fixture: None,
            },
        );
        Ok(Self {
            root: Some(root),
            #[cfg(test)]
            retained_fixture_root: None,
            state,
            primary,
            authority,
            kv_peers,
            kv_bootstrapped: false,
            kv_voters: false,
            resident_pins: BTreeMap::new(),
            fabric,
            nodes,
            closed: false,
            component_factories: Vec::new(),
        })
    }
    /// Use the SAME normal mounted runtime at a caller-owned fresh data path.
    /// Closing leaves the actual native genesis, receipts and checkpoints there
    /// for observational application tests; this does not copy or seed tables.
    pub fn start_single_node_in_data_dir(
        name: &str,
        data_dir: SinomoDataDir,
    ) -> Result<(Self, NodeId), String> {
        let node = Self::ordered_node_ids(name, 1).remove(0);
        if ClusterPaths::new(&data_dir).state.exists() {
            return Err("current DFS fixture requires a fresh initialized owner path".into());
        }
        initialize_opt_in_trace();
        let root = sinomo_testdir::TestDir::new(name);
        let mut runtime = Self::initialize_in_data_dir(name, node.clone(), root, data_dir)?;
        runtime.start_nodes()?;
        Ok((runtime, node))
    }

    /// Supply ordinary component composition before the SAME native startup.
    /// Explicit factories replace a default with the same component identity;
    /// other factories mount through the same reconciler, scope and registry.
    pub fn configure_component_factories(
        &mut self,
        factories: Vec<Arc<dyn sinomo_module::ComponentFactory>>,
    ) -> Result<(), String> {
        if self.kv_bootstrapped || self.nodes.values().any(|node| node.reconciler.is_some()) {
            return Err("component composition must precede ordinary native startup".into());
        }
        self.component_factories = factories;
        Ok(())
    }

    pub fn define_node(&mut self, node: NodeId) -> Result<(), String> {
        if self.kv_bootstrapped {
            return Err("define the entire node roster before native KV bootstrap".into());
        }
        if self.nodes.values().any(|node| node.reconciler.is_some()) {
            return Err("define the case topology before mounting nodes".into());
        }
        if self.nodes.contains_key(&node) {
            return Err("duplicate DFS case identity".into());
        }
        let mut member = sinomo_core::ClusterMemberRecord::pending_learner(
            self.state.cluster_id.clone(),
            node.clone(),
            node.to_string(),
            format!("pk-{node}"),
            OffsetDateTime::now_utc(),
        );
        sinomo_core::node::apply_cluster_raft_role(
            &mut member,
            sinomo_core::ClusterMemberRole::Learner,
            OffsetDateTime::now_utc(),
        );
        self.state.members.push(member.clone());
        let primary_paths = ClusterPaths::new(self.data_dir(&self.primary));
        sinomo_core::cluster::write_cluster_state(&primary_paths, &self.state)
            .map_err(|e| e.to_string())?;
        let data_dir = Arc::new(SinomoDataDir::new(
            self.root.as_ref().unwrap().join(node.as_str()),
        ));
        data_dir.ensure_root_layout().map_err(|e| e.to_string())?;
        self.nodes.insert(
            node,
            Node {
                data_dir,
                kv_available: Arc::new(AtomicBool::new(false)),
                resources: sinomo_device::NodeResourceMonitor::default(),
                reconciler: None,
                mounted_context: None,
                selected_original_fixture: None,
            },
        );
        Ok(())
    }
    fn prepare_projection(&mut self, node: &NodeId) -> Result<(), String> {
        let primary_paths = ClusterPaths::new(self.data_dir(&self.primary));
        let primary_state = sinomo_core::cluster::read_cluster_state_without_kv(&primary_paths)
            .map_err(|error| error.to_string())?;
        let paths = ClusterPaths::new(self.data_dir(node));
        let initialize_resident = !self.resident_pins.contains_key(node);
        // A voter that already holds Raft state restarts from it, and the
        // current leader catches it up, as for a restarted daemon. Copying the
        // bootstrap node's log could replay a deposed leader's view.
        let restarts_from_own_state = self.kv_voters && !initialize_resident;
        if node != &self.primary && !restarts_from_own_state {
            let mut local_state = primary_state.clone();
            local_state.local_node_id = node.clone();
            sinomo_core::cluster::write_cluster_state(&paths, &local_state)
                .map_err(|error| error.to_string())?;
            let primary = primary_state
                .members
                .iter()
                .find(|member| member.node_id == self.primary)
                .ok_or("primary membership is missing")?;
            let authenticated_primary = sinomo_core::cluster::ClusterAuthenticatedPeer {
                node_id: self.primary.to_string(),
                public_key: primary.node_public_key.clone(),
            };
            run_unit(Box::pin(async {
                tokio::time::timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT, async {
                    let request = primary_paths
                        .kv_paths()
                        .locked_build_raft_append_request_from_next_index_async(&primary_state, 1)
                        .await
                        .map_err(|error| error.to_string())?;
                    let response = sinomo_core::cluster::handle_cluster_raft_append_request_async(
                        executor().handle(),
                        &paths,
                        request,
                        &authenticated_primary,
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                    if !response.accepted {
                        return Err(response.detail);
                    }
                    if initialize_resident {
                        paths
                            .kv_paths()
                            .locked_initialize_raft_resident(node)
                            .map_err(|error| error.to_string())?;
                    }
                    Ok(())
                })
                .await
                .map_err(|_| {
                    "real replica KV bootstrap reached its existing network deadline".to_string()
                })?
            }))?;
        } else if initialize_resident {
            paths
                .kv_paths()
                .locked_initialize_raft_resident(node)
                .map_err(|error| error.to_string())?;
        }
        if !self.resident_pins.contains_key(node) {
            let pin = sinomo_core::kv::pin_resident_raft_node_registry_for_test(
                &paths.kv_paths().raft_wal_paths(node),
            )
            .map_err(|error| error.to_string())?;
            self.resident_pins.insert(node.clone(), pin);
        }
        let node_state = &self.nodes[node];
        sinomo_core::cluster::mark_cluster_kv_node_started(&paths);
        node_state.kv_available.store(true, Ordering::Release);
        self.kv_peers.register(
            node.clone(),
            (*node_state.data_dir).clone(),
            Arc::clone(&node_state.kv_available),
        );
        Ok(())
    }
    fn wait_native_authority(&self, node: &NodeId) -> Result<(), String> {
        self.wait_native_authority_before(
            node,
            tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        )
    }
    fn wait_native_authority_before(
        &self,
        node: &NodeId,
        deadline: tokio::time::Instant,
    ) -> Result<(), String> {
        let paths = ClusterPaths::new(self.data_dir(node));
        let state = sinomo_core::cluster::read_cluster_state_without_kv(&paths)
            .map_err(|error| error.to_string())?;
        let authority = crate::runtime_host::daemon_dfs_metadata_authority_generation_for_identity(
            &paths,
            &state.cluster_id,
            &state.local_node_id,
            Some(executor().handle()),
        )
        .map_err(|error| error.to_string())?;
        run_unit(Box::pin(async {
            tokio::time::timeout_at(deadline, authority.ensure_write_configuration_ready_async())
                .await
                .map_err(|_| {
                    "native DFS configuration readiness reached its existing network deadline"
                        .to_string()
                })?
                .map_err(|error| error.to_string())
        }))
    }
    /// Drive `node`'s published configuration up to every commit made before
    /// this call: one ReadIndex fixes the committed revision, then the
    /// node's publication is awaited (a watch, not a re-read) until it
    /// reaches that revision. Out-of-process clients read this publication.
    pub fn wait_published_configuration_current(&self, node: &NodeId) -> Result<(), String> {
        let deadline = tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        let paths = ClusterPaths::new(self.data_dir(node));
        let state = sinomo_core::cluster::read_cluster_state_without_kv(&paths)
            .map_err(|error| error.to_string())?;
        let authority = crate::runtime_host::daemon_dfs_metadata_authority_generation_for_identity(
            &paths,
            &state.cluster_id,
            &state.local_node_id,
            Some(executor().handle()),
        )
        .map_err(|error| error.to_string())?;
        let cluster = self.kv_read_authority();
        run_unit(Box::pin(async move {
            let committed = tokio::time::timeout_at(
                deadline,
                cluster.kv_range_prefix_authoritative_async("/dfs/filesystems/".into()),
            )
            .await
            .map_err(|_| "committed-revision ReadIndex reached its network deadline".to_string())?
            .map_err(|error| error.to_string())?
            .authority
            .revision;
            let mut published = authority.subscribe_configuration_snapshot();
            tokio::time::timeout_at(
                deadline,
                published.wait_for(|snapshot| {
                    snapshot.revision().is_some_and(|revision| revision >= committed)
                }),
            )
            .await
            .map_err(|_| format!("published configuration did not reach revision {}", committed.0))?
            .map(|_| ())
            .map_err(|error| error.to_string())
        }))
    }

    fn wait_startup_completion(&self) -> Result<(), String> {
        self.wait_startup_completion_with_budget(None, None)
    }
    fn wait_startup_completion_with_budget(
        &self,
        shared_deadline: Option<tokio::time::Instant>,
        selected_node: Option<&NodeId>,
    ) -> Result<(), String> {
        let deadline = shared_deadline.unwrap_or_else(|| {
            tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT
        });
        if shared_deadline.is_some() && tokio::time::Instant::now() > deadline {
            return Err("native recovery startup reached the existing network deadline".into());
        }
        let selected_owner =
            |node_id: &NodeId| selected_node.is_none_or(|selected| selected == node_id);
        if selected_node.is_some_and(|selected| {
            self.nodes
                .get(selected)
                .is_none_or(|node| node.reconciler.is_none())
        }) {
            return Err("requested native startup owner is not mounted".into());
        }
        let mut owners = self
            .nodes
            .iter()
            .filter(|(node_id, node)| selected_owner(node_id) && node.reconciler.is_some())
            .map(|(_, node)| {
                crate::default_reconciliation::subscribe_default_dfs_reconciliation_completion(
                    &ClusterPaths::new(&node.data_dir),
                )
                .map_err(|error| error.to_string())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let completion = run_unit(Box::pin(async {
            tokio::time::timeout_at(deadline, async {
                loop {
                    for owner in &mut owners {
                        owner.wait_current_complete().await.map_err(|error| error.to_string())?;
                    }
                    if owners.iter().map(|owner| owner.is_current_complete())
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|error| error.to_string())?
                        .into_iter().all(|complete| complete) {
                        return Ok(());
                    }
                }
            }).await.map_err(|_| {
                "mounted native DFS owners did not complete their current unparked startup generation before the existing network deadline".to_string()
            })?
        }));
        if let Err(error) = completion {
            eprintln!("native DFS startup completion failed: {error}; owners={owners:?}");
            return Err(error);
        }
        for (node_id, node) in &self.nodes {
            if selected_owner(node_id) && node.reconciler.is_some() {
                if let Some(deadline) = shared_deadline {
                    self.wait_native_authority_before(node_id, deadline)?;
                } else {
                    // Ordinary lifecycle retains each existing authority budget.
                    self.wait_native_authority(node_id)?;
                }
            }
        }
        if shared_deadline.is_some() && tokio::time::Instant::now() > deadline {
            return Err("native recovery startup reached the existing network deadline".into());
        }
        Ok(())
    }
    fn node_component_factories(
        &self,
        dfs_enabled: bool,
        node: &NodeId,
    ) -> Result<Vec<Arc<dyn sinomo_module::ComponentFactory>>, String> {
        let dfs: Arc<dyn sinomo_module::ComponentFactory> = if let Some(fixture) = &self.nodes[node].selected_original_fixture {
            Arc::new(crate::service::component_factory_with_selected_original_data_fixture(fixture.clone())
                .map_err(|error| error.to_string())?)
        } else {
            Arc::new(crate::component_factory().map_err(|error| error.to_string())?)
        };
        let dfs_id = dfs.descriptor().component_id.clone();
        let catalog: Arc<dyn sinomo_module::ComponentFactory> = Arc::new(
            sinomo_module::cluster_service_catalog_publisher_component_factory()
                .map_err(|error| error.to_string())?,
        );
        let hosted: Arc<dyn sinomo_module::ComponentFactory> = Arc::new(
            crate::runtime_host::hosted_cluster_kv_test_factory(Arc::clone(&self.kv_peers))
                .map_err(|error| error.to_string())?,
        );
        let mut factories = vec![hosted, catalog];
        if dfs_enabled {
            factories.insert(0, dfs);
        }
        for supplied in &self.component_factories {
            if self.nodes[node].selected_original_fixture.is_some()
                && supplied.descriptor().component_id == dfs_id
            {
                return Err("selected native fixture cannot be replaced by a supplied DFS factory".into());
            }
            if !dfs_enabled && supplied.descriptor().component_id == dfs_id {
                continue;
            }
            if let Some(index) = factories.iter().position(|factory| {
                factory.descriptor().component_id == supplied.descriptor().component_id
            }) {
                factories[index] = Arc::clone(supplied);
            } else {
                factories.push(Arc::clone(supplied));
            }
        }
        Ok(factories)
    }

    /// Reconcile actual node-local component enablement in the same mounted
    /// owner. Membership remains the committed bootstrap roster; the ordinary
    /// retained catalog publisher alone removes or adds its real DFS offer.
    #[cfg(test)]
    pub(crate) fn set_node_dfs_component_enabled(
        &mut self,
        node: &NodeId,
        enabled: bool,
    ) -> Result<(), String> {
        let factories = self.node_component_factories(enabled, node)?;
        let endpoint = self
            .fabric
            .nodes
            .lock()
            .map_err(|_| "native endpoint registry poisoned")?
            .get(node)
            .cloned()
            .ok_or("actual mounted component endpoint is absent")?;
        // Invalidate the exported context borrow at the same boundary as an
        // ordinary stop. A fresh borrow follows only successful real remount.
        self.nodes.get_mut(node).unwrap().mounted_context.take();
        endpoint.accepting.store(false, Ordering::Release);
        run_unit(Box::pin(async {
            tokio::time::timeout(CLOSE_TIMEOUT, async {
                while endpoint.active.load(Ordering::Acquire) != 0 {
                    let notified = endpoint.idle.notified();
                    if endpoint.active.load(Ordering::Acquire) == 0 {
                        break;
                    }
                    notified.await;
                }
            })
            .await
            .map_err(|_| {
                "DFS native ingress did not drain for component reconciliation".to_string()
            })
        }))?;
        self.join_transfers(Some(node))?;
        let catalog_changed = Arc::new(Notify::new());
        let catalog_wake = Arc::clone(&catalog_changed);
        // Wake on the commits applied by the node the catalog is read
        // through: a stopped bootstrap node applies none.
        let _catalog_subscription =
            sinomo_core::cluster::subscribe_cluster_kv_committed_observations(
                &ClusterPaths::new(self.data_dir(&self.kv_read_node())),
                move |_| catalog_wake.notify_one(),
            );
        let reconciler = self
            .nodes
            .get_mut(node)
            .and_then(|owned| owned.reconciler.as_mut())
            .ok_or("actual native component reconciler is absent")?;
        let report = reconciler
            .reconcile_staged_with_base_registry(ServiceRegistry::default(), &factories)
            .map_err(|error| error.to_string())?;
        let next_endpoint = Arc::new(Endpoint {
            host_generation: Arc::clone(&endpoint.host_generation),
            data_dir: Arc::clone(&endpoint.data_dir),
            cluster: Arc::clone(&endpoint.cluster),
            registry: reconciler
                .service_registry()
                .active_snapshot()
                .registry()
                .clone(),
            kv_available: Arc::clone(&endpoint.kv_available),
            accepting: AtomicBool::new(false),
            sending: Arc::clone(&endpoint.sending),
            active: Arc::new(AtomicUsize::new(0)),
            idle: Arc::new(Notify::new()),
        });
        self.fabric
            .nodes
            .lock()
            .unwrap()
            .insert(node.clone(), next_endpoint);
        // Release the withdrawn registry before joining its actual scopes so
        // the old service reference cannot retain the storage owner at remount.
        drop(endpoint);
        run_unit(Box::pin(async {
            reconciler
                .finish_retiring_and_wait(
                    &report.stopped,
                    tokio::time::Instant::now() + CLOSE_TIMEOUT,
                )
                .await
                .map_err(|error| error.to_string())
        }))?;
        run_unit(Box::pin(self.kv_peers.join(CLOSE_TIMEOUT)))?;
        let authority = self.kv_read_authority();
        run_unit(Box::pin(async {
            tokio::time::timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT, async {
                loop {
                    let read = authority
                        .kv_range_prefix_authoritative_async(sinomo_core::service::CLUSTER_SERVICES_KV_PREFIX.into())
                        .await.map_err(|error| error.to_string())?;
                    let providers = crate::cluster_service::dfs_provider_node_ids_from_current_entries(
                        &self.state, &self.state.members, &read.value,
                    ).map_err(|error| error.to_string())?;
                    if providers.contains(node) == enabled {
                        return Ok(());
                    }
                    catalog_changed.notified().await;
                }
            }).await.map_err(|_| "ordinary catalog publisher did not commit actual component enablement before the existing network deadline".to_string())?
        }))?;
        run_unit(Box::pin(self.kv_peers.join(CLOSE_TIMEOUT)))?;
        if enabled {
            self.wait_native_authority(node)?;
            self.ready_node(node)?;
            self.wait_startup_completion()?;
            let context = self.nodes[node]
                .reconciler
                .as_ref()
                .unwrap()
                .context()
                .clone();
            self.nodes.get_mut(node).unwrap().mounted_context = Some(context);
        } else {
            self.fabric.nodes.lock().unwrap()[node]
                .accepting
                .store(true, Ordering::Release);
        }
        Ok(())
    }

    /// Explicit fresh physical representation before native mounting. Ordinary
    /// fixtures keep their current factory and readers without modification.
    pub(crate) fn select_original_data_fixture_before_start(&mut self, node: &NodeId) -> Result<(), String> {
        let native = self.nodes.get_mut(node).ok_or("unknown selected fixture node")?;
        if native.reconciler.is_some() || native.mounted_context.is_some() || native.selected_original_fixture.is_some() {
            return Err("selected representation must be supplied once before native mounting".into());
        }
        native.selected_original_fixture = Some(crate::runtime_host::DfsSelectedOriginalDataFixture::default());
        Ok(())
    }

    pub(crate) fn selected_original_data_layout(&self, node: &NodeId) -> Result<Arc<crate::DfsSelectedOriginalDataLayout>, String> {
        self.nodes[node].selected_original_fixture.as_ref()
            .ok_or("native fixture did not select original DATA representation")?
            .layout().map_err(|error| error.to_string())
    }

    fn mount_node(&mut self, node: &NodeId) -> Result<(), String> {
        if self.nodes[node].reconciler.is_some() {
            return Err("duplicate live DFS test node".into());
        }
        let data_dir = self.nodes[node].data_dir.clone();
        crate::runtime_host::suspend_write_set_keeping_unless_enabled(data_dir.root());
        let host_generation = Arc::new(());
        let sending = Arc::new(AtomicBool::new(true));
        let carrier = Arc::new(Carrier {
            host_identity: Some(NativeHostIdentity {
                generation: Arc::clone(&host_generation),
                cluster_id: self.state.cluster_id.clone(),
                data_dir: data_dir.clone(),
                sending: Arc::clone(&sending),
            }),
            source: node.clone(),
            fabric: self.fabric.clone(),
            origin: RpcOrigin::RegisteredHost,
        });
        let cluster = if self.kv_voters {
            crate::runtime_host::test_cluster_access_for_voter_with_rpc(
                &data_dir,
                Some(Arc::clone(&self.kv_peers)),
                Some(carrier.clone()),
            )
        } else {
            crate::runtime_host::test_cluster_access_for_node_on_voter_with_rpc(
                &data_dir,
                self.data_dir(&self.primary),
                Some(Arc::clone(&self.kv_peers)),
                Some(carrier.clone()),
            )
        };
        // Mounted clients opened on this node subscribe to the component's own
        // bus, as they do to the daemon event stream it is bridged to.
        let event_bus = ComponentEventBus::default();
        crate::mounted_sifs::register_mounted_sifs_test_event_bus(&data_dir, Some(event_bus.clone()));
        let context = ComponentContext::new(data_dir.clone())
            .with_host_role(ComponentHostRole::Embedded)
            .with_runtime(executor().handle().clone())
            .with_node_resources(self.nodes[node].resources.clone())
            .with_cluster_access(Arc::clone(&cluster))
            .with_object_rpc_transport(carrier)
            .with_event_bus(event_bus);
        let mut reconciler = ComponentReconciler::new(context, ServiceRegistry::default());
        // A host that fails to mount never sends: no stop will end it.
        let mounted = self
            .node_component_factories(true, node)
            .and_then(|factories| reconciler.reconcile(&factories).map_err(|e| e.to_string()))
            .and_then(|_| {
                if reconciler.active_component_ids().is_empty() {
                    Err("embedded DFS component did not activate".to_string())
                } else {
                    Ok(())
                }
            });
        if let Err(error) = mounted {
            sending.store(false, Ordering::Release);
            return Err(error);
        }
        let endpoint = Arc::new(Endpoint {
            host_generation,
            data_dir,
            cluster,
            registry: reconciler
                .service_registry()
                .active_snapshot()
                .registry()
                .clone(),
            kv_available: Arc::clone(&self.nodes[node].kv_available),
            accepting: AtomicBool::new(false),
            sending,
            active: Arc::new(AtomicUsize::new(0)),
            idle: Arc::new(Notify::new()),
        });
        self.fabric
            .nodes
            .lock()
            .unwrap()
            .insert(node.clone(), endpoint);
        let context = reconciler.context().clone();
        let mounted = self.nodes.get_mut(node).unwrap();
        mounted.mounted_context = Some(context);
        mounted.reconciler = Some(reconciler);
        Ok(())
    }
    fn ready_node(&mut self, node: &NodeId) -> Result<(), String> {
        let reconciler = self
            .nodes
            .get_mut(node)
            .unwrap()
            .reconciler
            .as_mut()
            .unwrap();
        reconciler.notify_host_start();
        reconciler
            .prepare_host_on_runtime(Some(executor().handle()))
            .map_err(|e| e.to_string())?;
        reconciler.notify_host_ready();
        self.fabric.nodes.lock().unwrap()[node]
            .accepting
            .store(true, Ordering::Release);
        Ok(())
    }
    fn bootstrap_planned_cluster(&mut self) -> Result<(), String> {
        if self.kv_bootstrapped {
            return Ok(());
        }
        let paths = ClusterPaths::new(self.data_dir(&self.primary));
        // Match product initialization: declare the full initial partition,
        // establish native initial leadership, then commit its checked seed.
        sinomo_core::cluster::write_cluster_state(&paths, &self.state)
            .map_err(|error| error.to_string())?;
        paths
            .kv_paths()
            .locked_set_raft_leader_compact(&self.primary, self.state.term)
            .map_err(|error| error.to_string())?;
        sinomo_core::cluster::bootstrap_single_voter_cluster_state_to_local_kv(&paths, &self.state)
            .map_err(|error| error.to_string())?;
        self.kv_bootstrapped = true;
        Ok(())
    }

    pub fn start_nodes(&mut self) -> Result<(), String> {
        let operation = if crate::service_trace::dfs_perf_trace_enabled() {
            self.data_dir(&self.primary).root().display().to_string()
        } else {
            String::new()
        };
        let mut timing = crate::service_trace::DfsPhaseTimer::new(&operation);
        timing.mark("native-startup-began");
        self.bootstrap_planned_cluster()?;
        timing.mark("native-initial-kv-bootstrap-completed");
        let ids: Vec<_> = self.nodes.keys().cloned().collect();
        self.prepare_projection(&self.primary.clone())?;
        timing.mark("native-primary-projection-completed");
        // Establish the primary's actual current-term ReadIndex before copying
        // its accepted entries to non-voters; this is not a synthetic certificate.
        run_unit(Box::pin(async {
            tokio::time::timeout(
                sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
                self.authority
                    .kv_range_prefix_authoritative_async("/dfs/filesystems/".into()),
            )
            .await
            .map_err(|_| {
                "initial real ReadIndex reached its existing network deadline".to_string()
            })?
            .map(|_| ())
            .map_err(|error| error.to_string())
        }))?;
        timing.mark("native-initial-read-index-completed");
        for id in &ids {
            if id != &self.primary {
                crate::dfs_perf_trace(format_args!(
                    "native-startup-node owner={} node={} phase=projection",
                    self.primary, id
                ));
                timing.mark("native-replica-projection-started");
                self.prepare_projection(id)?;
                timing.mark("native-replica-projection-completed");
            }
        }
        if self.kv_voters {
            // Each caught-up learner becomes a voter, one quorum-backed
            // configuration change at a time, as `sinomo cluster promote` does.
            for id in ids.iter().filter(|id| **id != self.primary) {
                crate::runtime_host::promote_test_cluster_kv_voter(
                    executor().handle(),
                    self.data_dir(&self.primary),
                    &self.kv_peers,
                    id,
                )
                .map_err(|error| format!("promote {id} to a KV voter: {error}"))?;
            }
            timing.mark("native-kv-voter-promotion-completed");
        }
        self.start_nodes_after_projection(ids, timing)
    }

    fn start_nodes_after_projection(
        &mut self,
        ids: Vec<NodeId>,
        mut timing: crate::service_trace::DfsPhaseTimer,
    ) -> Result<(), String> {
        for id in &ids {
            crate::dfs_perf_trace(format_args!(
                "native-startup-node owner={} node={} phase=mount",
                self.primary, id
            ));
            timing.mark("native-mount-started");
            self.mount_node(id)?;
            timing.mark("native-mount-completed");
        }
        // Initial replicas already own native resident Raft nodes. Let the
        // actual committed fanout deliver the mounted catalog entries, without
        // racing a second full-log bootstrap/resident initialization against it.
        timing.mark("native-kv-fanout-join-started");
        run_unit(Box::pin(
            self.kv_peers
                .join(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT),
        ))?;
        timing.mark("native-kv-fanout-join-completed");
        // Join native cold authority while component background work is still
        // behind the normal host-prepare edge, then publish normal readiness.
        for id in &ids {
            crate::dfs_perf_trace(format_args!(
                "native-startup-node owner={} node={} phase=authority",
                self.primary, id
            ));
            timing.mark("native-authority-wait-started");
            self.wait_native_authority(id)?;
            timing.mark("native-authority-wait-completed");
        }
        let expected = ids
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        let catalog_changed = Arc::new(Notify::new());
        let catalog_wake = Arc::clone(&catalog_changed);
        let _catalog_subscription =
            sinomo_core::cluster::subscribe_cluster_kv_committed_observations(
                &ClusterPaths::new(self.data_dir(&self.primary)),
                move |_| catalog_wake.notify_one(),
            );
        timing.mark("native-catalog-wait-started");
        let authority = self.kv_read_authority();
        run_unit(Box::pin(async {
            tokio::time::timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT, async {
                loop {
                    let read = authority
                        .kv_range_prefix_authoritative_async(sinomo_core::service::CLUSTER_SERVICES_KV_PREFIX.into())
                        .await.map_err(|error| error.to_string())?;
                    let providers = crate::cluster_service::dfs_provider_node_ids_from_current_entries(
                        &self.state, &self.state.members, &read.value,
                    ).map_err(|error| error.to_string())?
                        .into_iter().collect::<std::collections::BTreeSet<_>>();
                    if providers == expected { return Ok(()); }
                    catalog_changed.notified().await;
                }
            }).await.map_err(|_| "normal component service catalog did not publish the actual mounted DFS providers before the existing network deadline".to_string())?
        }))?;
        timing.mark("native-catalog-wait-completed");
        // All actual providers and their mounted endpoints are present. Prepare
        // the independent native owners concurrently on the SAME runtime, as
        // separate product nodes do. An endpoint accepts requests only after
        // its own genuine component preparation succeeds.
        timing.mark("native-host-prepare-started");
        let mut preparations = Vec::new();
        for id in &ids {
            crate::dfs_perf_trace(format_args!(
                "native-startup-node owner={} node={} phase=host-ready",
                self.primary, id
            ));
            let reconciler = self.nodes.get_mut(id).unwrap().reconciler.take().unwrap();
            let endpoint = Arc::clone(&self.fabric.nodes.lock().unwrap()[id]);
            let runtime = executor().handle().clone();
            let preparation = executor().spawn_blocking(move || {
                reconciler.notify_host_start();
                let result = reconciler
                    .prepare_host_on_runtime(Some(&runtime))
                    .map_err(|error| error.to_string());
                if result.is_ok() {
                    reconciler.notify_host_ready();
                    endpoint.accepting.store(true, Ordering::Release);
                }
                (reconciler, result)
            });
            preparations.push((id.clone(), preparation));
        }
        let mut completed = Vec::new();
        run_unit(Box::pin(async {
            for (id, preparation) in preparations {
                completed.push((id, preparation.await));
            }
            Ok(())
        }))?;
        // Join every started owner before returning an error and restore every
        // returned real scope so normal shutdown retains ownership of its tasks.
        let mut failure = None;
        for (id, result) in completed {
            match result {
                Ok((reconciler, result)) => {
                    self.nodes.get_mut(&id).unwrap().reconciler = Some(reconciler);
                    if let Err(error) = result {
                        failure.get_or_insert(error);
                    }
                }
                Err(error) => {
                    failure
                        .get_or_insert_with(|| format!("native host preparation failed: {error}"));
                }
            }
        }
        if let Some(error) = failure {
            return Err(error);
        }
        timing.mark("native-host-prepare-completed");
        timing.mark("native-startup-completion-wait-started");
        let completion = self.wait_startup_completion();
        timing.mark("native-startup-completion-wait-returned");
        completion
    }
    /// Inspect the registry mounted by the real embedded component reconciler.
    #[cfg(test)]
    pub(crate) fn service_registry(&self, node: &NodeId) -> Result<ServiceRegistry, String> {
        let nodes = self
            .fabric
            .nodes
            .lock()
            .map_err(|_| "native endpoint registry poisoned")?;
        let endpoint = nodes
            .get(node)
            .ok_or("native component endpoint is absent")?;
        if !endpoint.accepting.load(Ordering::Acquire) {
            return Err("native component endpoint is closed".into());
        }
        Ok(endpoint.registry.clone())
    }

    /// Borrow the SAME mounted host mechanisms for another real component.
    /// This supplies transport/registry context, never an authority capability.
    pub fn component_context(&self, node: &NodeId) -> Result<ComponentContext, String> {
        let mounted = self.nodes.get(node).ok_or("unknown native fixture node")?;
        if mounted.reconciler.is_none()
            || !self
                .fabric
                .nodes
                .lock()
                .map_err(|_| "native endpoint registry poisoned")?
                .get(node)
                .is_some_and(|endpoint| endpoint.accepting.load(Ordering::Acquire))
        {
            return Err("native component context is not live".into());
        }
        mounted
            .mounted_context
            .clone()
            .ok_or_else(|| "native mounted context is absent".into())
    }

    /// Negative-only hold of the SAME mounted Main scheduler admission.
    /// No queue row, authority proof, effect, readiness bit or owner is invented.
    pub fn hold_main_mutation_admission(&self, node: &NodeId) -> Result<impl Send + use<>, String> {
        self.hold_main_admission(node, &DfsMessageExecutionScope::GlobalExclusive)
    }

    /// Hold one exact lane of the SAME mounted Main scheduler, as local work
    /// on `shard_id` holds it while it waits on a peer: a global writer then
    /// waits for this hold to end. Negative-only, like
    /// [`Self::hold_main_mutation_admission`].
    pub fn hold_main_exact_admission(
        &self,
        node: &NodeId,
        file_system_id: &crate::DfsFileSystemId,
        shard_id: &str,
    ) -> Result<impl Send + use<>, String> {
        self.hold_main_admission(
            node,
            &DfsMessageExecutionScope::ExactShard {
                file_system_id: file_system_id.clone(),
                shard_id: crate::DfsMetadataShardId::new(shard_id),
            },
        )
    }

    fn hold_main_admission(
        &self,
        node: &NodeId,
        scope: &DfsMessageExecutionScope,
    ) -> Result<impl Send + use<>, String> {
        let context = self.component_context(node)?;
        let owner = crate::runtime_host::retain_component_data_owner(&context)
            .map_err(|error| error.to_string())?;
        let scheduler = crate::runtime_host::daemon_dfs_mutation_scheduler(context.data_dir())
            .map_err(|error| error.to_string())?;
        let mut captured = None;
        run_unit(Box::pin(async {
            let admission = tokio::time::timeout(
                sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
                scheduler.admit(scope),
            )
            .await
            .map_err(|_| "actual Main admission hold reached its existing deadline".to_string())?
            .map_err(|error| error.to_string())?;
            captured = Some(admission);
            Ok(())
        }))?;
        Ok((
            captured.expect("actual scheduler returned its admission"),
            owner,
        ))
    }

    /// Observe the existing native Main report only. Captured bits and queue
    /// facts come from its normal command execution; this installs no owner,
    /// pending state, receipt, work flag or response.
    #[cfg(test)]
    pub(crate) fn capture_served_work(
        &self,
        node: &NodeId,
        max_reports: usize,
    ) -> Result<NativeServedWorkCapture, String> {
        assert!(
            (1..=32).contains(&max_reports),
            "bounded native report capture required"
        );
        let context = self.component_context(node)?;
        let records = Arc::new(Mutex::new(Vec::new()));
        let omitted = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&records);
        let dropped = Arc::clone(&omitted);
        let expected_node = node.clone();
        let hook =
            Arc::new(
                move |point: crate::runtime_host::DaemonDfsPreDispatchTestPoint<'_>,
                      command: Option<&crate::DfsMessageCommand>,
                      _preflight: Option<
                    &crate::runtime_host::DaemonDfsMetadataAuthorityPreflight,
                >| {
                    let crate::runtime_host::DaemonDfsPreDispatchTestPoint::AfterReport {
                        committed_work,
                        succeeded,
                    } = point
                    else {
                        return Ok(());
                    };
                    let command =
                        command.expect("actual Main report retains its same native command");
                    assert_eq!(command.routed_message.target_node_id, expected_node);
                    let mut captured = observed.lock().expect("actual Main report capture");
                    if captured.len() == max_reports {
                        dropped.fetch_add(1, Ordering::Release);
                    } else {
                        captured.push(CapturedServedMessageWork {
                            command: command.clone(),
                            committed_work,
                            succeeded,
                        });
                    }
                    Ok(())
                },
            );
        let guard = crate::runtime_host::install_daemon_dfs_pre_dispatch_test_hook(
            context.data_dir(),
            hook,
        );
        Ok(NativeServedWorkCapture {
            records,
            omitted,
            _guard: guard,
        })
    }

    /// Commit an ordinary placement Apply from the actual current configuration
    /// and native candidate observations. The caller opens its writer afterward
    /// so generic filesystem publication cannot conceal the authoritative rule.
    pub fn apply_placement_policy(
        &self,
        actor: &NodeId,
        path: &str,
        scope: DfsPlacementScope,
        policy: DfsPlacementPolicy,
    ) -> Result<DfsPlacementReconciliationJob, String> {
        let rule = crate::normalize_placement_rule(path, scope, policy)
            .map_err(|error| error.to_string())?;
        let control = self.shared_control_plane()?;
        let expected = control
            .with_client(|kv| {
                DfsControlPlaneStore::new(kv.as_mut())
                    .get_file_system_config(&crate::sifs_file_system_id())
            })
            .map_err(|error| error.to_string())?
            .ok_or("normal native fixture filesystem configuration is absent")?;
        let mut planner = self.client(actor)?;
        let candidates = planner
            .placement_content_candidates()
            .map_err(|error| error.to_string())?;
        let snapshot = planner
            .placement_snapshot_cache
            .snapshot()
            .map(|snapshot| (*snapshot).clone());
        drop(planner);
        let mut desired = expected.clone();
        crate::upsert_placement_rule(&mut desired.subtree_policies, rule.clone());
        let job = crate::control_plane::build_placement_reconciliation_job(
            sinomo_core::Id::generate().to_string(),
            &desired,
            rule.path_prefix,
            scope,
            BTreeSet::new(),
            candidates,
            snapshot,
            crate::metadata_records::dfs_now_unix_nanos(),
        )
        .map_err(|error| error.to_string())?;
        let committed = control
            .with_client(|kv| {
                DfsControlPlaneStore::new(kv.as_mut())
                    .compare_and_put_placement_update(&expected, &desired, &job)
            })
            .map_err(|error| error.to_string())?;
        if !committed {
            return Err("normal placement Apply rejected a changed current configuration".into());
        }
        Ok(job)
    }

    pub fn data_dir(&self, node: &NodeId) -> &SinomoDataDir {
        &self.nodes[node].data_dir
    }

    /// The node whose committed KV [`Self::kv_read_authority`] reads.
    fn kv_read_node(&self) -> NodeId {
        if self.kv_voters {
            let nodes = self.fabric.nodes.lock().expect("DFS test nodes");
            if !nodes.contains_key(&self.primary)
                && let Some(node) = nodes.keys().next()
            {
                return node.clone();
            }
        }
        self.primary.clone()
    }
    /// The access a fixture-level authoritative read runs through. With KV
    /// voters it is a running node's own access, so a stopped KV leader never
    /// answers; otherwise it is the sole voter's.
    fn kv_read_authority(&self) -> Arc<dyn ClusterAccess> {
        if self.kv_voters {
            let nodes = self.fabric.nodes.lock().expect("DFS test nodes");
            if let Some(endpoint) = nodes.get(&self.primary).or_else(|| nodes.values().next()) {
                return Arc::clone(&endpoint.cluster);
            }
        }
        Arc::clone(&self.authority)
    }
    pub fn file_system(&self) -> DfsFileSystem {
        default_cluster_sifs(&self.state)
    }
    /// A read-only view of `node`'s local metadata store under the current
    /// shard configuration. Clients delegate to Main and keep no store, so
    /// tests inspect a node's durable metadata here.
    pub fn node_metadata(
        &self,
        node: &NodeId,
    ) -> Result<crate::LocalShardedMetadataBackend, String> {
        crate::local_state::open_local_sharded_metadata_backend_for_inspection_at_node(
            crate::default_dfs_metadata_shards_root(self.data_dir(node)),
            self.metadata_shards()?,
            node,
        )
        .map_err(|error| error.to_string())
    }

    pub fn metadata_shards(&self) -> Result<Vec<DfsMetadataShardConfig>, String> {
        let paths = ClusterPaths::new(self.data_dir(&self.primary));
        let mut control = crate::runtime_host::DaemonDfsControlPlaneKvClient::new_on_runtime(
            &paths,
            executor().handle(),
        );
        DfsControlPlaneStore::new(&mut control)
            .list_metadata_shards(&sifs_file_system_id())
            .map_err(|error| error.to_string())
    }

    /// Borrow the ordinary Core effect opener for the actual owning shard.
    /// Its native owner, configuration projection and publication provider are
    /// the same ones used by maintenance and storage-import effects.
    pub fn metadata_primary_service(
        &self,
        path: &str,
    ) -> Result<NetworkedDfsService<DfsNodeTransport>, String> {
        let shards = self.metadata_shards()?;
        let shard = crate::select_metadata_shard_config_for_path(
            &self.file_system().file_system_id,
            &shards,
            path,
        )
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "normal runtime has no actual metadata owner for path".to_string())?;
        crate::runtime_host::open_dfs_runtime_service_on_shard(
            self.data_dir(&shard.primary_node_id),
            Some(executor().handle()),
            &shard,
        )
        .map_err(|error| error.to_string())
    }
    /// Run `operation` on `node`'s resident Main maintenance service, which
    /// holds every shard copy on the node, under ordinary maintenance
    /// admission. Use it for Main-side work that spans the node's shards.
    pub fn with_main_maintenance_service<R>(
        &self,
        node: &NodeId,
        operation: impl FnOnce(&mut NetworkedDfsService<DfsNodeTransport>) -> R,
    ) -> Result<R, String> {
        crate::runtime_host::with_daemon_dfs_main_maintenance_service(
            self.data_dir(node),
            executor().handle(),
            operation,
        )
        .map_err(|error| error.to_string())
    }
    /// Open `node`'s own copy of `shard`, as Main does for selected local
    /// recovery. The node holds the shard as its primary or a replica.
    pub fn local_shard_copy_service(
        &self,
        node: &NodeId,
        shard: &crate::DfsMetadataShardConfig,
    ) -> Result<NetworkedDfsService<DfsNodeTransport>, String> {
        crate::runtime_host::open_dfs_runtime_service_on_local_shard_copy(
            self.data_dir(node),
            Some(executor().handle()),
            shard,
        )
        .map_err(|error| error.to_string())
    }
    /// Run history collection through each ordinary current owning-shard opener.
    pub fn gc_metadata_history_on_current_primaries(
        &self,
        options: crate::DfsMetadataHistoryGcOptions,
    ) -> Result<(), String> {
        for shard in self.metadata_shards()? {
            let mut primary = crate::runtime_host::open_dfs_runtime_service_on_shard(
                self.data_dir(&shard.primary_node_id),
                Some(executor().handle()),
                &shard,
            )
            .map_err(|error| {
                format!(
                    "open current GC primary {} for {}/{} at epoch {}: {error}",
                    shard.primary_node_id, shard.file_system_id, shard.shard_id, shard.epoch
                )
            })?;
            primary
                .gc_metadata_history(options.clone())
                .map_err(|error| {
                    format!(
                        "GC on current primary {} for {}/{} at epoch {}: {error}",
                        shard.primary_node_id, shard.file_system_id, shard.shard_id, shard.epoch
                    )
                })?;
        }
        Ok(())
    }

    pub fn root_shard(&self) -> Result<DfsMetadataShardConfig, String> {
        self.metadata_shards()?
            .into_iter()
            .find(|shard| shard.shard_id == "root")
            .ok_or_else(|| "normal initial planner did not commit a root shard".into())
    }
    pub fn client(&self, node: &NodeId) -> Result<NetworkedDfsService<DfsNodeTransport>, String> {
        if self.nodes[node].reconciler.is_none() {
            return Err("DFS case node is stopped".into());
        }
        self.wait_startup_completion()?;
        crate::runtime_host::open_dfs_runtime_service(
            self.data_dir(node),
            Some(executor().handle()),
        )
        .map_err(|e| e.to_string())
    }
    /// One ordinary client-open attempt with the caller's startup deadline and
    /// typed Pending result intact. No admission is retained on a yielded open;
    /// recovery callers can use their existing exact-root continuation path.
    pub fn client_with_startup_deadline_once(
        &self,
        node: &NodeId,
        deadline: tokio::time::Instant,
    ) -> Result<NetworkedDfsService<DfsNodeTransport>, crate::runtime_host::DfsRuntimeError> {
        use crate::runtime_host::DfsRuntimeError;
        if self.nodes[node].reconciler.is_none() {
            return Err(DfsRuntimeError::Parse { detail: "DFS case node is stopped".into() });
        }
        self.wait_startup_completion_with_budget(Some(deadline), None)
            .map_err(|detail| DfsRuntimeError::Parse { detail })?;
        let client = crate::runtime_host::open_dfs_runtime_service(
            self.data_dir(node), Some(executor().handle()),
        )?;
        if tokio::time::Instant::now() >= deadline {
            return Err(DfsRuntimeError::Parse {
                detail: "native client preparation completed after its original startup deadline".into(),
            });
        }
        Ok(client)
    }

    pub fn pause_replica_materialization(
        &self,
        node: &NodeId,
        shard: &DfsMetadataShardId,
    ) -> Result<crate::runtime_host::DfsTestMaterializationPause, String> {
        let owned = self.nodes.get(node).ok_or("unknown native fixture node")?;
        if owned.reconciler.is_none() {
            return Err("native fixture node is stopped".into());
        }
        crate::runtime_host::materialization_test::install(
            &crate::default_dfs_metadata_shards_root(&owned.data_dir).join(&shard.0),
        )
    }
    /// Pause only the first real received deferred reply while its native
    /// completion still retains the original data owner. No reply is supplied.
    pub fn pause_deferred_response(
        &self,
        node: &NodeId,
    ) -> Result<crate::runtime_host::DfsTestDeferredResponsePause, String> {
        if self
            .nodes
            .get(node)
            .is_none_or(|owned| owned.reconciler.is_none())
        {
            return Err("native deferred-response owner is not mounted".into());
        }
        crate::runtime_host::deferred_work_test::install(self.data_dir(node))
    }
    pub fn wait_paused_materialization(
        &self,
        pause: &crate::runtime_host::DfsTestMaterializationPause,
        budget: Duration,
    ) -> Result<crate::runtime_host::DfsTestMaterializationCapture, String> {
        if let Ok(current) = tokio::runtime::Handle::try_current() {
            if !matches!(
                current.runtime_flavor(),
                tokio::runtime::RuntimeFlavor::MultiThread
            ) {
                return Err(
                    "materialization observation cannot block a current-thread executor".into(),
                );
            }
            tokio::task::block_in_place(|| executor().block_on(pause.wait(budget)))
        } else {
            executor().block_on(pause.wait(budget))
        }
    }

    /// Hold the existing normal current placement producer's exact permit.
    /// Tests supply no holder identity, receipt, or effect authorization.
    pub fn with_current_placement_permit<R>(
        &self,
        node: &NodeId,
        file_system_id: &DfsFileSystemId,
        job_id: &str,
        work: impl FnOnce() -> R,
    ) -> Result<R, String> {
        let context = self.component_context(node)?;
        let owner = crate::runtime_host::retain_component_data_owner(&context)
            .map_err(|error| error.to_string())?;
        let mut service = self.client(node)?;
        let (_, _, mut permit) = service
            .acquire_authoritative_placement_reconciliation(file_system_id, job_id, None)
            .map_err(|error| error.to_string())?
            .ok_or("actual current placement job changed before normal admission")?;
        let result = work();
        DfsControlPlaneStore::new(
            service
                .control_plane
                .as_mut()
                .ok_or("native client lost its control plane")?
                .as_mut(),
        )
        .release_placement_authority_permit(&mut permit)
        .map_err(|error| error.to_string())?;
        drop(owner);
        Ok(result)
    }

    /// Return the control client from the same ordinary registered DFS opener.
    /// No transport adapter, authority registration or policy is constructed.
    pub fn control_plane_client(&self) -> Result<Box<dyn DfsControlPlaneKvClient>, String> {
        let mut client = self.client(&self.primary)?;
        client
            .control_plane
            .take()
            .ok_or_else(|| "normal embedded client has no control plane".into())
    }

    /// The existing native KV settings producer, independent of the DFS
    /// projection a configuration-gap control deliberately withholds.
    pub fn configuration_publisher(&self, node: &NodeId) -> Box<dyn DfsControlPlaneKvClient> {
        Box::new(
            crate::runtime_host::DaemonDfsControlPlaneKvClient::new_on_runtime(
                &ClusterPaths::new(self.data_dir(node)),
                executor().handle(),
            ),
        )
    }

    pub fn shared_control_plane(
        &self,
    ) -> Result<SharedDfsControlPlaneKvClient<Box<dyn DfsControlPlaneKvClient>>, String> {
        Ok(SharedDfsControlPlaneKvClient::new(
            self.control_plane_client()?,
        ))
    }
    /// Reuse the normal registered host's actual peer adapters for shared
    /// instrumentation; successful replies still come from native ingress.
    pub fn peer_transports(
        &self,
        node: &NodeId,
    ) -> Result<(Arc<dyn DfsPeerTransport>, Arc<dyn AsyncDfsPeerTransport>), String> {
        let owned = self.nodes.get(node).ok_or("unknown native fixture node")?;
        if owned.reconciler.is_none() {
            return Err("native fixture node has no live registered component".into());
        }
        Ok(crate::runtime_host::daemon_dfs_peer_transports_on_runtime(
            &owned.data_dir,
            executor().handle(),
        ))
    }
    /// Add an empty current scope through the ordinary native bootstrap host.
    /// Hidden complete genesis precedes its assignment CAS and guarded publish.
    pub fn bootstrap_empty_metadata_prefix(
        &self,
        shard_id: &str,
        prefix: &str,
    ) -> Result<DfsMetadataShardConfig, String> {
        self.wait_startup_completion()?;
        let file_system = self.file_system();
        let mut shards = self.metadata_shards()?;
        if shards.iter().any(|cfg| cfg.shard_id == shard_id) {
            return Err("native scope already exists".into());
        }
        let cfg = DfsMetadataShardConfig {
            file_system_id: file_system.file_system_id.clone(),
            shard_id: shard_id.to_owned(),
            primary_node_id: self.primary.clone(),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::PathPrefix {
                prefix: crate::posix_path::normalize_posix_path(prefix)
                    .map_err(|e| e.to_string())?,
            },
            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        };
        shards.push(cfg.clone());
        crate::control_plane::DfsMetadataShardRoutingIndex::new(
            file_system.file_system_id.clone(),
            &shards,
        )
        .map_err(|e| e.to_string())?;
        let data_dir = self.data_dir(&self.primary).clone();
        let paths = ClusterPaths::new(&data_dir);
        let primary = self.primary.clone();
        let control = crate::runtime_host::DaemonDfsControlPlaneKvClient::new_on_runtime(
            &paths,
            executor().handle(),
        );
        let owner = crate::runtime_host::require_daemon_dfs_data_owner_lock(
            &crate::runtime_host::DfsRuntimePaths::new(&data_dir),
        )
        .map_err(|e| e.to_string())?;
        let scheduler = crate::runtime_host::daemon_dfs_mutation_scheduler(&data_dir)
            .map_err(|e| e.to_string())?;
        let bootstrap_nodes = self.nodes.keys().cloned().collect::<Vec<_>>();
        let created = cfg.clone();
        run_unit(Box::pin(async move {
            tokio::time::timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT, async move {
                let scope = DfsMessageExecutionScope::GlobalExclusive;
                let admission = scheduler.admit(&scope).await.map_err(|e|e.to_string())?;
                let owners = crate::DfsPublicationExecutionOwners::from_runtime(data_dir.root(), scope, admission, owner);
                let mut host = DfsControlPlaneDefaultClusterAsyncBootstrapHost::new_with_resident_metadata_backend(data_dir, paths, primary.clone(), Box::new(control), None, owners);
                host.prepare_native_genesis_async(file_system.clone(), shards.clone(), bootstrap_nodes.clone(), primary.clone()).await.map_err(|e|e.to_string())?;
                host.put_metadata_shard_async(created).await.map_err(|e|e.to_string())?;
                host.materialize_default_sifs_metadata_on_shards_async(file_system, shards, bootstrap_nodes, primary).await.map_err(|e|e.to_string())
            }).await.map_err(|_|"normal empty-scope bootstrap reached its existing network deadline".to_string())?
        }))?;
        for node in self.nodes.keys() {
            if self.nodes[node].reconciler.is_some() {
                self.wait_native_authority(node)?;
            }
        }
        let actual = self
            .metadata_shards()?
            .into_iter()
            .find(|actual| actual.shard_id == shard_id)
            .ok_or("ordinary scope bootstrap did not publish its assignment")?;
        if actual != cfg {
            return Err("native empty-scope assignment changed while bootstrap completed".into());
        }
        Ok(actual)
    }

    /// Run default setup's reconciliation of one shard toward `desired` on
    /// `node`, under the global admission a setup pass holds, as a pass whose
    /// discovery computed `desired` would.
    #[cfg(test)]
    pub(crate) fn reconcile_default_metadata_shard(
        &self,
        node: &NodeId,
        desired: DfsMetadataShardConfig,
    ) -> Result<(), String> {
        let data_dir = self.data_dir(node).clone();
        let paths = ClusterPaths::new(&data_dir);
        let control = crate::runtime_host::DaemonDfsControlPlaneKvClient::new_on_runtime(
            &paths,
            executor().handle(),
        );
        let owner = crate::runtime_host::require_daemon_dfs_data_owner_lock(
            &crate::runtime_host::DfsRuntimePaths::new(&data_dir),
        )
        .map_err(|e| e.to_string())?;
        let scheduler = crate::runtime_host::daemon_dfs_mutation_scheduler(&data_dir)
            .map_err(|e| e.to_string())?;
        let node = node.clone();
        run_unit(Box::pin(async move {
            tokio::time::timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT, async move {
                let scope = DfsMessageExecutionScope::GlobalExclusive;
                let admission = scheduler.admit(&scope).await.map_err(|e| e.to_string())?;
                let owners = crate::DfsPublicationExecutionOwners::from_runtime(
                    data_dir.root(),
                    scope,
                    admission,
                    owner,
                );
                let mut host =
                    DfsControlPlaneDefaultClusterAsyncBootstrapHost::new_with_resident_metadata_backend(
                        data_dir,
                        paths,
                        node,
                        Box::new(control),
                        None,
                        owners,
                    );
                host.put_metadata_shard_async(desired)
                    .await
                    .map_err(|e| e.to_string())
            })
            .await
            .map_err(|_| "default setup reconciliation reached its network deadline".to_string())?
        }))
    }

    /// Use the unchanged supported singleton bootstrap, but return only after
    /// its actual prepare/CAS owner has deferred completion and released locks.
    #[cfg(test)]
    pub(crate) fn begin_deferred_native_genesis(
        &self,
        shard_id: &str,
        prefix: &str,
    ) -> Result<DeferredNativeGenesis, String> {
        let shard = DfsMetadataShardConfig {
            file_system_id: self.file_system().file_system_id,
            shard_id: shard_id.to_owned(),
            primary_node_id: self.primary.clone(),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment: DfsMetadataShardAssignment::PathPrefix {
                prefix: crate::posix_path::normalize_posix_path(prefix)
                    .map_err(|e| e.to_string())?,
            },
            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        };
        let data = self.data_dir(&self.primary);
        let deferral =
            crate::default_cluster::genesis::NativeGenesisCompletionDeferral::install(data, &shard)
                .map_err(|e| e.to_string())?;
        let outcome = self.bootstrap_empty_metadata_prefix(shard_id, prefix);
        let error = outcome
            .err()
            .ok_or("native genesis unexpectedly completed instead of reaching the selected cut")?;
        if !error.contains(crate::default_cluster::genesis::NATIVE_GENESIS_COMPLETION_DEFERRED) {
            return Err(error);
        }
        let original_record = deferral
            .original_record()
            .map_err(|e| e.to_string())?
            .ok_or("native genesis deferral did not observe an original Prepared record")?;
        original_record.validate().map_err(|e| e.to_string())?;
        if original_record.status != DfsPreparedOperationStatus::Prepared
            || original_record.committed_entry.is_some()
            || deferral.hits() == 0
        {
            return Err("native genesis cut is not the actual incomplete original".into());
        }
        // Acquiring and releasing the SAME normal scheduler owner proves that
        // the selected deferral did not park global admission across probes.
        let scheduler =
            crate::runtime_host::daemon_dfs_mutation_scheduler(data).map_err(|e| e.to_string())?;
        run_unit(Box::pin(async move {
            tokio::time::timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT, async {
                let guard = scheduler.admit(&DfsMessageExecutionScope::GlobalExclusive).await.map_err(|e|e.to_string())?;
                drop(guard); Ok(())
            }).await.map_err(|_| "native genesis deferral did not release ordinary admission before its existing deadline".to_string())?
        }))?;
        for node in self.nodes.keys() {
            if self.nodes[node].reconciler.is_some() {
                self.wait_native_authority(node)?;
            }
        }
        if self
            .metadata_shards()?
            .into_iter()
            .find(|actual| actual.shard_id == shard_id)
            .as_ref()
            != Some(&shard)
        {
            return Err(
                "native genesis assignment CAS does not retain the exact singleton scope".into(),
            );
        }
        let phase = DeferredNativeGenesis {
            data_root: data.root(),
            shard,
            original_record,
            deferral,
        };
        if self.read_deferred_native_genesis(&phase)? != phase.original_record {
            return Err(
                "native genesis readback differs from its actual pre-commit original".into(),
            );
        }
        Ok(phase)
    }

    /// Historical read only through the existing inspection owner. A live
    /// matching DATA owner is required; this does not fake stopped-node readiness.
    #[cfg(test)]
    pub(crate) fn read_deferred_native_genesis(
        &self,
        phase: &DeferredNativeGenesis,
    ) -> Result<DfsPreparedOperationRecord, String> {
        let data = self.data_dir(&phase.shard.primary_node_id);
        if data.root() != phase.data_root {
            return Err("deferred genesis names another runtime root".into());
        }
        let owner = crate::runtime_host::require_daemon_dfs_data_owner_lock(
            &crate::runtime_host::DfsRuntimePaths::new(data),
        )
        .map_err(|e| e.to_string())?;
        let layout = owner
            .selected_original_data_layout()
            .map_err(|e| e.to_string())?;
        let shard = DfsMetadataShardId::new(&phase.shard.shard_id);
        let root = crate::local_dfs_metadata_shard_path(data, &shard);
        if let Some(layout) = &layout {
            layout
                .stage(&phase.shard.file_system_id, &shard, &root)
                .map_err(|e| e.to_string())?;
        }
        let deadline = std::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        loop {
            if std::time::Instant::now() >= deadline {
                return Err(
                    "deferred genesis inspection remains unavailable at its existing deadline"
                        .into(),
                );
            }
            if let Some(_guard) = crate::metadata_lock::try_metadata_inspection_lock(
                root.clone(),
                crate::metadata_cursor::journal_write_lock_path(&root),
            )
            .map_err(|e| e.to_string())?
            {
                let backend = crate::LocalSobjMetadataBackend::open_for_inspection_with_layout(
                    shard.clone(),
                    root.clone(),
                    layout
                        .clone()
                        .map(|layout| (phase.shard.file_system_id.clone(), layout)),
                )
                .map_err(|e| e.to_string())?;
                let record = backend
                    .prepared_operation_record(&phase.original_record.digest.operation_id)
                    .map_err(|e| e.to_string())?
                    .ok_or("original native genesis record is unavailable")?;
                if record.digest != phase.original_record.digest
                    || record.payload != phase.original_record.payload
                {
                    return Err(
                        "retained native genesis changed its original digest or body".into(),
                    );
                }
                if std::time::Instant::now() >= deadline {
                    return Err("deferred genesis inspection crossed its original deadline".into());
                }
                return Ok(record);
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// One explicit owner completion attempt, not a user-write readiness probe.
    #[cfg(test)]
    pub(crate) fn complete_deferred_native_genesis(
        &self,
        phase: &DeferredNativeGenesis,
    ) -> Result<DfsPreparedOperationRecord, String> {
        let shards = self.metadata_shards()?;
        if shards.iter().find(|s| s.shard_id == phase.shard.shard_id) != Some(&phase.shard) {
            return Err("deferred genesis assignment changed before explicit completion".into());
        }
        self.read_deferred_native_genesis(phase)?;
        let data = self.data_dir(&phase.shard.primary_node_id).clone();
        let paths = ClusterPaths::new(&data);
        let primary = phase.shard.primary_node_id.clone();
        let file_system = self.file_system();
        let nodes = self.nodes.keys().cloned().collect::<Vec<_>>();
        let control = crate::runtime_host::DaemonDfsControlPlaneKvClient::new_on_runtime(
            &paths,
            executor().handle(),
        );
        let owner = crate::runtime_host::require_daemon_dfs_data_owner_lock(
            &crate::runtime_host::DfsRuntimePaths::new(&data),
        )
        .map_err(|e| e.to_string())?;
        let scheduler =
            crate::runtime_host::daemon_dfs_mutation_scheduler(&data).map_err(|e| e.to_string())?;
        run_unit(Box::pin(async move {
            tokio::time::timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT, async {
                let scope = DfsMessageExecutionScope::GlobalExclusive;
                let admission = scheduler.admit(&scope).await.map_err(|e|e.to_string())?;
                let owners = crate::DfsPublicationExecutionOwners::from_runtime(data.root(), scope, admission, owner);
                let mut host = DfsControlPlaneDefaultClusterAsyncBootstrapHost::new_with_resident_metadata_backend(
                    data, paths, primary.clone(), Box::new(control), None, owners,
                );
                // Release only while this actual normal owner holds admission;
                // no competing completion wins a hidden test-only bypass.
                phase.deferral.release();
                host.materialize_default_sifs_metadata_on_shards_async(file_system, shards, nodes, primary)
                    .await.map_err(|e|e.to_string())
            }).await.map_err(|_| "explicit native genesis completion reached its existing network deadline".to_string())?
        }))?;
        let record = self.read_deferred_native_genesis(phase)?;
        if record.status != DfsPreparedOperationStatus::Committed
            || record.committed_entry.is_none()
        {
            return Err("normal genesis owner did not complete the same original record".into());
        }
        Ok(record)
    }

    /// A separate explicit OrdinaryDurability operation for the exact actual
    /// singleton assignment. Three mounted nodes do not turn {A} into fixed3.
    #[cfg(test)]
    pub(crate) fn sync_completed_native_genesis(
        &self,
        phase: &DeferredNativeGenesis,
        actual_bootstrap_object_path: &str,
    ) -> Result<DfsMetadataQuorumSyncCertificate, String> {
        let record = self.read_deferred_native_genesis(phase)?;
        if record.status != DfsPreparedOperationStatus::Committed
            || record.committed_entry.is_none()
        {
            return Err("cannot sync an incomplete native genesis as committed".into());
        }
        let mut service = crate::runtime_host::open_dfs_runtime_service_on_shard(
            self.data_dir(&phase.shard.primary_node_id),
            Some(executor().handle()),
            &phase.shard,
        )
        .map_err(|e| e.to_string())?;
        let object = service
            .metadata_backend()
            .ok_or("normal genesis service has no backend")?
            .get_object_by_path(&phase.shard.file_system_id, actual_bootstrap_object_path)
            .map_err(|e| e.to_string())?
            .ok_or("normal genesis did not create the requested bootstrap object")?;
        let target = service
            .capture_metadata_quorum_sync_target(&object)
            .map_err(|e| e.to_string())?;
        if target.expected_shard.as_ref() != Some(&phase.shard)
            || target.authority_epoch != Some(1)
            || !target.replica_node_ids.is_empty()
        {
            return Err(
                "genesis durability target differs from the actual singleton assignment".into(),
            );
        }
        let certificate = service
            .sync_metadata_shard_to_quorum(&target, sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
            .map_err(|e| e.to_string())?;
        if certificate.target != target
            || certificate.quorum_size != 1
            || certificate.acknowledged_node_ids != vec![phase.shard.primary_node_id.clone()]
        {
            return Err("genesis OrdinaryDurability result does not cover the exact actual assigned set {A}".into());
        }
        Ok(certificate)
    }

    /// Observe only the exact original completed native handoff, including
    /// its full proof and the retained retiring owner's activated freeze.
    /// Reuse an already-open control client so a deliberately withheld peer
    /// projection cannot turn this read into unrelated startup admission.
    pub(crate) fn observe_completed_native_handoff(
        &self,
        control: &SharedDfsControlPlaneKvClient<Box<dyn DfsControlPlaneKvClient>>,
        handoff: &DfsMetadataShardMembershipHandoff,
        phase_deadline: std::time::Instant,
    ) -> Result<Option<DfsMetadataShardConfig>, crate::DfsError> {
        let file_system_id = &handoff.retiring_shard.file_system_id;
        let shard_id = handoff.retiring_shard.shard_id.as_str();

        let keys = [
            crate::metadata_shard_key(file_system_id, shard_id),
            crate::metadata_shard_membership_handoff_key(file_system_id, shard_id),
            crate::metadata_shard_membership_handoff_proof_key(file_system_id, shard_id),
        ];
        let entries = control
            .with_client(|kv| kv.get_authoritative_consistent_before(&keys, phase_deadline))?;
        let [Some(active), None, Some(proof)] = entries.as_slice() else {
            return Ok(None);
        };
        if entries
            .iter()
            .zip(&keys)
            .any(|(entry, key)| entry.as_ref().is_some_and(|entry| &entry.key != key))
        {
            return Ok(None);
        }
        let active = active.decode::<DfsMetadataShardConfig>()?;
        let proof = proof.decode::<crate::DfsMetadataShardMembershipHandoffProof>()?;
        let Some(head) = handoff.required_head_watermark.as_ref() else {
            return Ok(None);
        };
        if active != handoff.desired_shard
            || proof.handoff_id != handoff.handoff_id
            || proof.cause != handoff.cause
            || proof.retiring_shard != handoff.retiring_shard
            || proof.desired_shard != handoff.desired_shard
            || head.file_system_id != handoff.retiring_shard.file_system_id
            || head.shard_id != handoff.retiring_shard.shard_id
            || head.primary_node_id != handoff.retiring_shard.primary_node_id
            || head.epoch != handoff.retiring_shard.epoch
            || proof.journal_sequence < head.journal_sequence
            || proof.journal_sequence < handoff.required_prepared_sequence
            || proof.state_fingerprint.is_empty()
            || !crate::control_plane::metadata_shard_membership_handoff_acknowledgements_are_safe(
                &proof.retiring_shard,
                &proof.desired_shard,
                &proof.acknowledged_node_ids.iter().cloned().collect(),
            )
        {
            return Ok(None);
        }
        // The cold opener may reject after this exact original
        // handoff activates. Observe its marker without reopening
        // a metadata backend or acquiring a publication lane.
        let context = self
            .component_context(&handoff.retiring_shard.primary_node_id)
            .map_err(|message| crate::DfsError::Metadata { message })?;
        let owner =
            crate::runtime_host::retain_component_data_owner(&context).map_err(|error| {
                crate::DfsError::Metadata {
                    message: error.to_string(),
                }
            })?;
        let shard = DfsMetadataShardId::new(shard_id);
        let root = crate::local_dfs_metadata_shard_path(context.data_dir(), &shard)
            .canonicalize()
            .map_err(|error| crate::DfsError::Metadata {
                message: error.to_string(),
            })?;
        if context.data_dir().root()
            != self
                .data_dir(&handoff.retiring_shard.primary_node_id)
                .root()
            || !owner.owns_backend_root(&root)
        {
            return Ok(None);
        }
        // Carry only the explicit reader capability of this retained native
        // owner. A marker/path must never opt a default observer into it.
        let selected_layout = owner.selected_original_data_layout().map_err(|error| {
            crate::DfsError::Metadata { message: error.to_string() }
        })?;
        let Some(marker) =
            crate::local_state::read_local_dfs_metadata_shard_write_freeze_with_layout(
                context.data_dir(), &shard, selected_layout.as_deref(),
            )?
        else {
            return Ok(None);
        };
        if marker.handoff_id != handoff.handoff_id
            || marker.retiring_epoch != handoff.retiring_shard.epoch
            || marker.activated_epoch != Some(handoff.desired_shard.epoch)
            || marker.journal_sequence != proof.journal_sequence
            || marker.state_fingerprint != proof.state_fingerprint
            || std::time::Instant::now() > phase_deadline
        {
            return Ok(None);
        }
        // The request's published watermark can precede the actual freeze.
        // Authenticate every intervening immutable native frame; sequence
        // advancement alone cannot prove that the frozen head extends it.
        let Some(mut prefix) = DfsMetadataPrefixProof::from_digest(
            head.journal_sequence,
            head.state_fingerprint.clone(),
        ) else {
            return Ok(None);
        };
        while prefix.journal_sequence < proof.journal_sequence {
            if std::time::Instant::now() > phase_deadline {
                return Ok(None);
            }
            let end = proof
                .journal_sequence
                .min(prefix.journal_sequence.saturating_add(64));
            let sequences = (prefix.journal_sequence + 1..=end).collect::<Vec<_>>();
            for frame in crate::metadata_journal::read_segmented_journal_frames_at_sequences(
                &root, &sequences,
            )? {
                let Some(frame) = frame else {
                    return Ok(None);
                };
                prefix = crate::metadata_journal::metadata_prefix_proof_after_exact_frame(
                    &prefix,
                    frame.entry(),
                    &shard,
                    frame.exact_bytes(),
                )?;
            }
        }
        if prefix.digest != proof.state_fingerprint || std::time::Instant::now() > phase_deadline {
            return Ok(None);
        }
        Ok(Some(active))
    }

    /// Move an actual native assignment through ordinary freeze, receiver
    /// acknowledgement and joint activation. This does not seed configurations.
    pub fn reassign_metadata_shard(
        &self,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
        primary: NodeId,
        mut replicas: Vec<NodeId>,
    ) -> Result<DfsMetadataShardConfig, String> {
        replicas.sort();
        replicas.dedup();
        if replicas.contains(&primary) {
            return Err("requested primary also appears in its replica assignment".into());
        }
        for node in std::iter::once(&primary).chain(&replicas) {
            if self
                .nodes
                .get(node)
                .is_none_or(|node| node.reconciler.is_none())
            {
                return Err(
                    "requested assignment includes a node without a live normal component".into(),
                );
            }
        }
        let wanted_members = std::iter::once(primary.clone())
            .chain(replicas.iter().cloned())
            .collect::<BTreeSet<_>>();
        let members = |config: &DfsMetadataShardConfig| {
            std::iter::once(config.primary_node_id.clone())
                .chain(config.replica_node_ids.iter().cloned())
                .collect::<BTreeSet<_>>()
        };
        let control = self.shared_control_plane()?;
        let mut completed = None;
        // Each phase is an ordinary exact-prefix handoff. A new primary must
        // first belong to the actual member ring, and the former primary stays
        // in that ring until the role transfer has really activated.
        for _ in 0..3 {
            let configs = control
                .with_client(|kv| {
                    DfsControlPlaneStore::new(kv.as_mut()).list_metadata_shards(file_system_id)
                })
                .map_err(|error| error.to_string())?;
            let actual = configs
                .iter()
                .find(|config| {
                    config.file_system_id == *file_system_id && config.shard_id == shard_id
                })
                .ok_or("requested filesystem/shard has no actual native assignment")?
                .clone();
            if completed
                .as_ref()
                .is_some_and(|previous| previous != &actual)
            {
                return Err("native assignment changed between completed handoff phases".into());
            }
            if actual.primary_node_id == primary && members(&actual) == wanted_members {
                return Ok(actual);
            }
            let mut joint_members = wanted_members.clone();
            joint_members.insert(actual.primary_node_id.clone());
            let role_only = actual.primary_node_id != primary && members(&actual) == joint_members;
            let handoff = control
                .with_client(|kv| {
                    let mut store = DfsControlPlaneStore::new(kv.as_mut());
                    if role_only {
                        // Preserve the exact actual ordered ring through the same
                        // product primary-transfer producer used by the CLI.
                        store.request_metadata_shard_primary_handoff(&actual, &primary)
                    } else {
                        let desired_members = if actual.primary_node_id == primary {
                            &wanted_members
                        } else {
                            &joint_members
                        };
                        let mut desired = actual.clone();
                        desired.replica_node_ids = desired_members
                            .iter()
                            .filter(|node| **node != actual.primary_node_id)
                            .cloned()
                            .collect();
                        desired.epoch = actual.epoch.checked_add(1).ok_or_else(|| {
                            sinomo_core::kv::KvError::Serialization {
                                detail: "metadata authority epoch exhausted".into(),
                            }
                        })?;
                        store.begin_metadata_shard_membership_handoff(&actual, &desired)
                    }
                })
                .map_err(|error| error.to_string())?
                .ok_or("native source assignment changed before handoff")?;
            let opener_deadline = std::time::Instant::now() + CLOSE_TIMEOUT;
            let (reconciliation, phase_deadline) = match self.client(&actual.primary_node_id) {
                Ok(mut source) => {
                    let phase_deadline = std::time::Instant::now() + CLOSE_TIMEOUT;
                    let reconciliation = source
                        .reconcile_pending_metadata_shard_membership_handoffs(
                            &configs,
                            CLOSE_TIMEOUT,
                        )
                        .map_err(|error| error.to_string());
                    drop(source);
                    (reconciliation, phase_deadline)
                }
                Err(error) => (Err(error), opener_deadline),
            };
            // The caller's attempt is advisory. A caller service holds no
            // metadata backend and cannot fence the retiring shard: it fails
            // on a pending handoff it sees, and completes nothing when it sees
            // none. The normal Main owner completes this original request.
            // Whatever the caller returned, wait for that completion through
            // authoritative reads within the phase budget: Main can finish
            // before or after the caller returns, and neither result says
            // the desired assignment is active. No new effect is authorized.
            let observe_deadline = phase_deadline.max(std::time::Instant::now() + CLOSE_TIMEOUT);
            let observed = loop {
                match self.observe_completed_native_handoff(&control, &handoff, observe_deadline) {
                    Ok(Some(active)) => break Some(active),
                    Ok(None) | Err(_) if std::time::Instant::now() < observe_deadline => {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Ok(None) | Err(_) => break None,
                }
            };
            let installed = match (observed, reconciliation) {
                (Some(active), _) => active,
                (None, Err(error)) => return Err(error),
                (None, Ok(count)) => {
                    return Err(format!(
                        "native handoff {} did not complete within its phase budget; the caller completed {count}",
                        handoff.handoff_id
                    ));
                }
            };
            if installed != handoff.desired_shard {
                return Err(format!(
                    "native handoff did not activate the exact requested assignment: handoff {} observed {:?} desired {:?}",
                    handoff.handoff_id, installed, handoff.desired_shard,
                ));
            }
            if installed.primary_node_id == primary && members(&installed) == wanted_members {
                return Ok(installed);
            }
            completed = Some(installed);
        }
        Err("native assignment requires another explicit handoff after its completed phases".into())
    }
    pub fn rpc_request(
        &self,
        source: &NodeId,
        target: &NodeId,
        request: ObjectRpcRequest,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, String> {
        self.rpc_call(source, target, ObjectRpcCall::value(request), timeout)
    }
    /// Dispatch the actual prepared call, including its owned native attachment,
    /// through the same authenticated Main endpoint and original caller budget.
    /// Preserve the actual semantic attachment so protocol tests can consume
    /// it through the production decoder instead of a value-only adapter.
    pub fn rpc_result(
        &self,
        source: &NodeId,
        target: &NodeId,
        call: ObjectRpcCall,
        timeout: Duration,
    ) -> Result<ObjectRpcResult, String> {
        let carrier = Carrier {
            host_identity: None,
            source: source.clone(),
            fabric: self.fabric.clone(),
            origin: RpcOrigin::ExplicitFixtureRpc,
        };
        carrier
            .call(
                target,
                call,
                DfsObjectRpcSendOptions::default()
                    .with_overall_timeout(timeout)
                    .cluster_rpc_options(),
                true,
            )
            .map_err(|e| e.to_string())
    }
    /// The same authenticated native carrier as rpc_result, with its caller
    /// future exposed for cancellation checks. No runtime/owner is synthesized.
    pub fn rpc_result_future(
        &self,
        source: &NodeId,
        target: &NodeId,
        call: ObjectRpcCall,
        timeout: Duration,
    ) -> impl std::future::Future<Output = Result<ObjectRpcResult, String>> + Send + use<> {
        let carrier = Carrier {
            host_identity: None,
            source: source.clone(),
            fabric: Arc::clone(&self.fabric),
            origin: RpcOrigin::ExplicitFixtureRpc,
        };
        let target = target.clone();
        async move {
            carrier
                .call_async(
                    &target,
                    call,
                    DfsObjectRpcSendOptions::default()
                        .with_overall_timeout(timeout)
                        .cluster_rpc_options(),
                    true,
                )
                .await
                .map_err(|error| error.to_string())
        }
    }

    pub fn rpc_call(
        &self,
        source: &NodeId,
        target: &NodeId,
        call: ObjectRpcCall,
        timeout: Duration,
    ) -> Result<ObjectRpcResponse, String> {
        let deadline = std::time::Instant::now() + timeout;
        let result = self.rpc_result(source, target, call, timeout)?;
        run_rpc(Box::pin(async {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            let response = tokio::time::timeout(
                remaining,
                result.into_materialized_value_response(16 * 1024 * 1024),
            )
            .await
            .map_err(|_| {
                NetworkError::timeout(
                    "shared DFS fixture response body deadline",
                    NetworkTimeoutPhase::Overall,
                    timeout,
                    NetworkDeliveryState::PossiblyApplied,
                )
            })??;
            Ok(ObjectRpcResult::value(response))
        }))
        .map(|result| result.response)
        .map_err(|e| e.to_string())
    }
    pub fn pause_source_preparation(
        &self,
        coordinator: &NodeId,
        point: crate::source_adoption_test::DfsSourcePausePoint,
    ) -> Result<crate::source_adoption_test::DfsSourcePhasePause, String> {
        crate::source_adoption_test::DfsSourcePhasePause::install_at(
            self.data_dir(coordinator).root().to_owned(),
            point,
        )
    }
    pub fn pause_remote_namespace_phase(
        &self,
        source: &NodeId,
    ) -> Result<crate::DfsSourcePhasePause, String> {
        crate::source_adoption_test::DfsSourcePhasePause::install(
            self.data_dir(source).root().to_owned(),
        )
    }
    pub fn install_fault(&self, rule: FaultRule) {
        if matches!(rule.action, FaultAction::BudgetExhaustedForFirstCall) {
            assert_eq!(
                rule.remaining_hits, 1,
                "first-call fault has one exact physical-call budget"
            );
            assert!(
                rule.call_id.is_none(),
                "the first actual matching call supplies its identity"
            );
        }
        assert!(
            (1..=32).contains(&rule.remaining_hits),
            "bounded fault rule required"
        );
        let mut rules = self.fabric.faults.lock().unwrap();
        assert!(rules.len() < 64);
        rules.push(rule)
    }
    /// Remove only the caller's exact configured negative transport rules.
    /// Already-running calls retain their cloned actions; native state, replies,
    /// other rules and sticky exhaustion accounting remain unchanged.
    #[cfg(test)]
    pub(crate) fn remove_faults_matching(
        &self,
        mut selected: impl FnMut(&FaultRule) -> bool,
    ) -> usize {
        let mut rules = self
            .fabric
            .faults
            .lock()
            .expect("native fixture fault rules");
        let before = rules.len();
        rules.retain(|rule| !selected(rule));
        before - rules.len()
    }

    /// Heal only configured transport faults. This neither acknowledges work
    /// nor changes persisted state; already-paused calls still own their gates.
    pub fn clear_faults(&self) {
        self.fabric
            .faults
            .lock()
            .expect("native fixture fault rules")
            .clear();
    }
    pub fn assert_fault_budgets(&self) -> Result<(), String> {
        if self.fabric.exhausted_budgets.load(Ordering::Relaxed) == 0 {
            Ok(())
        } else {
            Err(
                "bounded DFS test fault budget exhausted; matching traffic stayed failed closed"
                    .into(),
            )
        }
    }
    pub fn observation_dropped_count(&self) -> usize {
        self.fabric.dropped_observations.load(Ordering::Relaxed)
    }

    /// Seal a complete observation window to the fixture's disk before starting
    /// another corpus batch. The live buffer remains bounded by MAX_OBSERVATIONS;
    /// no event or omission counter is discarded. Normal close verifies every
    /// complete segment, including its count and digest, before claiming coverage.
    /// Observation readers subsequently see the new, explicitly selected window.
    pub fn checkpoint_rpc_observation_window(&self) -> Result<usize, String> {
        use std::io::Write;
        let mut records = self
            .fabric
            .observations
            .lock()
            .map_err(|_| "observations poisoned")?;
        if self.observation_dropped_count() != 0 {
            return Err("cannot checkpoint a censored RPC observation window".into());
        }
        if records.is_empty() {
            return Ok(0);
        }
        let mut checkpoints = self
            .fabric
            .observation_checkpoints
            .lock()
            .map_err(|_| "observation checkpoints poisoned")?;
        let fixture_root = self.root.as_ref().map(|root| root.path().to_path_buf());
        #[cfg(test)]
        let fixture_root = fixture_root.or_else(|| self.retained_fixture_root.clone());
        let checkpoint_name = format!("rpc-observations-{:06}.json", checkpoints.len());
        #[cfg(test)]
        let checkpoint_name = if self.retained_fixture_root.is_some() {
            format!("rpc-observations-reopen-{}-{:06}.json", std::process::id(), checkpoints.len())
        } else { checkpoint_name };
        let path = fixture_root.ok_or("fixture root was retained")?
            .join(checkpoint_name);
        // Debug includes every field, including the carrier's private timeout
        // and protection options. Do not replace it with a lossy route summary.
        let bytes = serde_json::to_vec(
            &records
                .iter()
                .map(|event| format!("{event:?}"))
                .collect::<Vec<_>>(),
        )
        .map_err(|error| error.to_string())?;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|error| error.to_string())?;
        file.write_all(&bytes)
            .and_then(|_| file.sync_all())
            .map_err(|error| error.to_string())?;
        let checkpoint = RpcObservationCheckpoint {
            path,
            records: records.len(),
            digest: blake3::hash(&bytes),
        };
        checkpoint.verify()?;
        let count = records.len();
        checkpoints.push(checkpoint);
        records.clear();
        Ok(count)
    }

    /// Export the latest sealed observation window before fixture shutdown.
    /// Capture and coverage remain owned by the ordinary checkpoint/close path.
    pub fn export_latest_rpc_observation_checkpoint(
        &self,
        output: &std::path::Path,
    ) -> Result<usize, String> {
        use std::io::Write;
        let checkpoints = self
            .fabric
            .observation_checkpoints
            .lock()
            .map_err(|_| "observation checkpoints poisoned")?;
        let checkpoint = checkpoints
            .last()
            .ok_or("no sealed RPC observation window exists")?;
        checkpoint.verify()?;
        let bytes = std::fs::read(&checkpoint.path).map_err(|error| error.to_string())?;
        if blake3::hash(&bytes) != checkpoint.digest {
            return Err("sealed RPC observation window changed before export".into());
        }
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(output)
            .map_err(|error| error.to_string())?;
        file.write_all(&bytes)
            .and_then(|_| file.sync_all())
            .map_err(|error| error.to_string())?;
        let exported = std::fs::read(output).map_err(|error| error.to_string())?;
        if exported != bytes || blake3::hash(&exported) != checkpoint.digest {
            return Err("exported RPC observation window differs from its sealed source".into());
        }
        Ok(checkpoint.records)
    }

    /// Actual adapter events in the current window. Only an explicit successful
    /// checkpoint begins a new window; ordinary fixtures retain their lifetime.
    pub fn rpc_observations(&self) -> Vec<RpcObservation> {
        self.fabric
            .observations
            .lock()
            .expect("native RPC observations")
            .clone()
    }
    /// Read the same bounded carrier observations while its component-owning
    /// runtime is moved into a normal maintenance caller. This retains no
    /// service admission or mutation capability.
    pub fn rpc_observation_reader(
        &self,
    ) -> impl Fn() -> Vec<RpcObservation> + Send + Sync + 'static + use<> {
        let fabric = Arc::clone(&self.fabric);
        move || {
            fabric
                .observations
                .lock()
                .expect("native RPC observations")
                .clone()
        }
    }
    /// Hold every background replica transfer of this mounted owner: its
    /// repair executor dispatches nothing, and a write's background stage
    /// sends are denied at their claim boundary. Dropping the hold wakes the
    /// executor, which serves everything that changed meanwhile.
    pub fn deny_background_replica_before_claim(
        &self,
        node: &NodeId,
    ) -> Result<impl Send + use<>, String> {
        let context = self.component_context(node)?;
        let executor = crate::repair_executor::hold_repair_executor_for_test(&context.data_dir().root());
        Ok((
            executor,
            crate::runtime_host::install_daemon_dfs_pre_dispatch_test_hook(
                context.data_dir(),
                Arc::new(|point, command, context| {
                    if matches!(point, crate::runtime_host::DaemonDfsPreDispatchTestPoint::BeforeReplicaTransferClaim {
                    background_repair: true, .. }) {
                    assert!(command.is_none() && context.is_none());
                    return Err(DfsError::Cancelled { operation: "negative native background before transfer claim" });
                }
                    Ok(())
                }),
            ),
        ))
    }
    /// The node repair executor's single timer: its earliest backoff end or
    /// in-flight re-check, if any.
    pub fn repair_executor_next_due_unix_nanos(&self, node: &NodeId) -> Result<Option<u64>, String> {
        let context = self.component_context(node)?;
        Ok(crate::repair_executor::repair_executor_work(&context.data_dir().root())
            .next_due_unix_nanos)
    }
    /// The node repair executor's progress mark: index changes of its
    /// primary shards and ended attempts advance it.
    pub fn repair_executor_progress(&self, node: &NodeId) -> Option<u64> {
        crate::repair_executor::repair_executor_progress(node)
    }
    /// Block on the node repair executor's progress edge until it passes
    /// `seen` or `deadline`; returns whether it advanced.
    pub fn wait_for_repair_executor_progress(
        &self,
        node: &NodeId,
        seen: u64,
        deadline: std::time::Instant,
    ) -> bool {
        crate::repair_executor::repair_executor_wait_for_progress(node, seen, deadline)
    }
    /// Hold this node's repair executor only; see
    /// [`crate::DfsRepairExecutorTestHold`].
    pub fn hold_repair_executor(
        &self,
        node: &NodeId,
    ) -> Result<crate::DfsRepairExecutorTestHold, String> {
        let context = self.component_context(node)?;
        Ok(crate::repair_executor::hold_repair_executor_for_test(
            &context.data_dir().root(),
        ))
    }

    /// Ask this live normal Main owner to reconstruct its existing durable
    /// reconciliation work. The request schedules the ordinary maintenance
    /// path; it neither performs hydration nor supplies repair state.
    pub fn request_repair_reconciliation(&self, node: &NodeId) -> Result<u64, String> {
        let context = self.component_context(node)?;
        let paths = crate::runtime_host::DfsRuntimePaths::for_host_role(
            context.data_dir(),
            context.host_role(),
        );
        let _owner = crate::runtime_host::require_daemon_dfs_data_owner_lock(&paths)
            .map_err(|error| error.to_string())?;
        crate::runtime_host::request_daemon_dfs_maintenance_reconciliation(&paths)
            .map_err(|error| error.to_string())
    }

    /// Run the same complete served owner as the normal component worker. It
    /// acquires actual native admission, checks its retained authority, takes
    /// the resident served lane and checkpoints repairs before detached sends.
    /// `budget` cancels this invocation only; it grants no component identity.
    pub fn maintenance_tick(
        &self,
        node: &NodeId,
        budget: Duration,
    ) -> Result<DfsTestMaintenanceObservation, String> {
        if budget.is_zero() || budget > sinomo_core::network::DEFAULT_NETWORK_TIMEOUT {
            return Err("maintenance invocation needs an existing bounded network budget".into());
        }
        let owned = self.nodes.get(node).ok_or("unknown native fixture node")?;
        if owned.reconciler.is_none() {
            return Err("native fixture node is stopped".into());
        }
        let data_dir = owned.data_dir.as_ref().clone();
        let paths = crate::runtime_host::DfsRuntimePaths::new(&data_dir);
        let _owner = crate::runtime_host::require_daemon_dfs_data_owner_lock(&paths)
            .map_err(|error| error.to_string())?;
        let repair_tracked_before =
            crate::repair_executor::repair_executor_work(&data_dir.root()).tracked_need_count;
        let records = Arc::new(Mutex::new((Vec::new(), 0usize)));
        let omitted = Arc::new(AtomicUsize::new(0));
        let captured = Arc::clone(&records);
        let dropped = Arc::clone(&omitted);
        let _subscription = sinomo_core::cluster::subscribe_cluster_kv_committed_deltas(
            &ClusterPaths::new(&data_dir),
            move |delta| {
                let mut captured = captured.lock().unwrap_or_else(|error| error.into_inner());
                for event in delta.events.iter() {
                    let bytes = event
                        .key
                        .len()
                        .saturating_add(match &event.kind {
                            sinomo_core::kv::KvEventKind::Put { value } => value.len(),
                            sinomo_core::kv::KvEventKind::Delete => 0,
                        })
                        .saturating_add(std::mem::size_of::<sinomo_core::kv::KvEvent>());
                    if captured.0.len() == MAX_OBSERVATIONS
                        || captured.1.saturating_add(bytes) > 8 * 1024 * 1024
                    {
                        dropped.fetch_add(1, Ordering::Relaxed);
                    } else {
                        captured.1 = captured.1.saturating_add(bytes);
                        captured.0.push(event.clone());
                    }
                }
            },
        );
        let resources = owned.resources.latest();
        let runtime = executor().handle().clone();
        let outcome = executor().block_on(async {
            let (cancel, cancelled) = tokio::sync::watch::channel(false);
            let mut invocation = Box::pin(
                crate::runtime_host::run_daemon_dfs_maintenance_tick_in_blocking_task(
                    paths,
                    runtime,
                    resources,
                    cancelled,
                ),
            );
            match tokio::time::timeout(budget, invocation.as_mut()).await {
                Ok(result) => result.map_err(|error| error.to_string()),
                Err(_) => {
                    cancel.send_replace(true);
                    // Retain and join the original future after cancellation;
                    // dropping a blocking-task handle would lose real custody.
                    let _ = invocation.await;
                    Err("ordinary maintenance invocation reached its caller budget and drained after cancellation".into())
                }
            }
        });
        let repair_tracked_after =
            crate::repair_executor::repair_executor_work(&data_dir.root()).tracked_need_count;
        let committed_kv_events =
            std::mem::take(&mut records.lock().unwrap_or_else(|error| error.into_inner()).0);
        Ok(DfsTestMaintenanceObservation {
            outcome,
            repair_tracked_before,
            repair_tracked_after,
            committed_kv_events,
            omitted_kv_event_count: omitted.load(Ordering::Relaxed),
        })
    }
    pub fn stop_node(&mut self, node: &NodeId) -> Result<(), String> {
        let owner_paths = self.nodes.get(node).and_then(|state| {
            state.mounted_context.as_ref().map(|context| {
                crate::runtime_host::DfsRuntimePaths::for_host_role(
                    context.data_dir(),
                    context.host_role(),
                )
            })
        });
        run_unit(Box::pin(self.kv_peers.join(CLOSE_TIMEOUT)))?;
        let result = self.stop_node_inner(node);
        if let Some(paths) = owner_paths.as_ref() {
            crate::runtime_host::trace_dfs_data_owner_references(paths, "stop-inner-returned");
        }
        result
    }
    fn stop_node_inner(&mut self, node: &NodeId) -> Result<(), String> {
        let owner_paths = self.nodes[node].mounted_context.as_ref().map(|context| {
            crate::runtime_host::DfsRuntimePaths::for_host_role(
                context.data_dir(),
                context.host_role(),
            )
        });
        let observe_owner = |checkpoint| {
            if let Some(paths) = owner_paths.as_ref() {
                crate::runtime_host::trace_dfs_data_owner_references(paths, checkpoint);
            }
        };
        observe_owner("before-mounted-context-drop");
        // Stop invalidates this borrow before endpoint/lifecycle withdrawal.
        // Reopen constructs a new context from that actual mounted generation.
        if let Some(context) = self.nodes.get_mut(node).unwrap().mounted_context.take() {
            crate::mounted_sifs::register_mounted_sifs_test_event_bus(context.data_dir(), None);
        }
        observe_owner("after-mounted-context-drop");
        // The node's own calls end as its stop begins, before its KV and
        // ingress are withdrawn, as a stopping daemon's do.
        if let Some(endpoint) = self.fabric.nodes.lock().unwrap().get(node) {
            endpoint.sending.store(false, Ordering::Release);
        }
        self.nodes[node]
            .kv_available
            .store(false, Ordering::Release);
        // A voter node's in-flight KV work ends now, before its ingress and
        // tasks are drained. A single-voter fixture's nodes all use the sole
        // voter's store, which stays open for the live nodes when it stops.
        if self.kv_voters {
            sinomo_core::cluster::mark_cluster_kv_node_stopping(&ClusterPaths::new(
                self.nodes[node].data_dir.as_ref(),
            ));
        }
        let endpoint = {
            let nodes = self.fabric.nodes.lock().unwrap();
            let endpoint = nodes
                .get(node)
                .cloned()
                .ok_or_else(|| "unknown live DFS test node".to_string())?;
            endpoint.accepting.store(false, Ordering::Release);
            endpoint
        };
        self.fabric.close_paused_faults_for_node(node);
        let node_state = self
            .nodes
            .get_mut(node)
            .ok_or("missing owned DFS test node")?;
        run_unit(Box::pin(async {
            tokio::time::timeout(CLOSE_TIMEOUT, async {
                while endpoint.active.load(Ordering::Acquire) != 0 {
                    let notified = endpoint.idle.notified();
                    if endpoint.active.load(Ordering::Acquire) == 0 {
                        break;
                    }
                    notified.await;
                }
            })
            .await
            .map_err(|_| "DFS native ingress did not drain".to_string())?;
            observe_owner("after-ingress-drain");
            node_state
                .reconciler
                .as_mut()
                .unwrap()
                .stop_tasks_and_wait(CLOSE_TIMEOUT)
                .await
                .map_err(|e| e.to_string())?;
            observe_owner("after-component-task-drain");
            Ok(())
        }))?;
        observe_owner("after-registered-host-task-drain");
        self.join_transfers(Some(node))?;
        observe_owner("after-transfer-drain");
        run_unit(Box::pin(async {
            self.nodes
                .get_mut(node)
                .unwrap()
                .reconciler
                .as_mut()
                .unwrap()
                .shutdown_and_wait(CLOSE_TIMEOUT)
                .await
                .map_err(|e| e.to_string())
        }))?;
        observe_owner("after-component-scope-shutdown");
        self.fabric.nodes.lock().unwrap().remove(node);
        observe_owner("after-endpoint-registry-removal");
        // The drained endpoint owns a clone of the actual mounted registry.
        // Release that final local service reference before observing storage.
        drop(endpoint);
        observe_owner("after-drained-endpoint-owner-drop");
        self.nodes.get_mut(node).unwrap().reconciler.take();
        observe_owner("after-reconciler-drop");
        // Every source component/host producer for this node is stopped. Join
        // any wave queued after the pre-stop drain before exposing its storage.
        run_unit(Box::pin(self.kv_peers.join(CLOSE_TIMEOUT)))?;
        observe_owner("after-final-kv-task-drain");
        Ok(())
    }
    pub fn reopen_node(&mut self, node: NodeId) -> Result<(), String> {
        self.reopen_node_with_action(node, None)
    }
    /// Reopen a production-shape fixture under its actual browser deadline.
    /// Existing ordinary reopen calls retain DEFAULT_NETWORK_TIMEOUT. This
    /// explicit caller deadline covers mounting, authority and completion.
    pub fn reopen_node_with_startup_deadline(
        &mut self,
        node: NodeId,
        deadline: tokio::time::Instant,
    ) -> Result<(), String> {
        if tokio::time::Instant::now() >= deadline {
            return Err("native recovery startup reached the caller deadline".into());
        }
        self.prepare_projection(&node)?;
        self.mount_node(&node)?;
        self.wait_native_authority_before(
            &node,
            deadline
                .min(tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT),
        )?;
        self.ready_node(&node)?;
        self.wait_startup_completion_with_budget(Some(deadline), Some(&node))
    }

    /// Run an ordinary RPC action once the real mounted host admits it, before
    /// waiting for its current startup generation. Recovery can enable that
    /// generation; action and completion share the original network budget.
    pub(crate) fn reopen_node_with_startup_action(
        &mut self,
        node: NodeId,
        action: Box<dyn FnOnce(Duration) -> Result<(), String> + '_>,
    ) -> Result<(), String> {
        self.reopen_node_with_action(node, Some(action))
    }


    fn reopen_node_with_action(
        &mut self,
        node: NodeId,
        action: Option<Box<dyn FnOnce(Duration) -> Result<(), String> + '_>>,
    ) -> Result<(), String> {
        self.prepare_projection(&node)?;
        self.mount_node(&node)?;
        self.wait_native_authority(&node)?;
        self.ready_node(&node)?;
        if let Some(action) = action {
            let deadline =
                tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err("native startup action reached the existing network deadline".into());
            }
            action(remaining)?;
            if tokio::time::Instant::now() > deadline {
                return Err("native startup action reached the existing network deadline".into());
            }
            self.wait_startup_completion_with_budget(Some(deadline), Some(&node))
        } else {
            self.wait_startup_completion_with_budget(None, Some(&node))
        }
    }
    pub fn close(mut self) -> Result<(), String> {
        self.close_inner(None).map(|_| ())
    }

    /// Require complete coverage for one real sealed namespace window while
    /// explicitly reporting any censored lifetime transport observations.
    /// All normal task, fault, custody and storage cleanup checks still run.
    pub fn close_with_namespace_capture(
        mut self,
        capture: NativeNamespaceReadCapture,
    ) -> Result<DfsTestObservationCoverageReport, String> {
        let requirement = capture.seal(&self.fabric)?;
        self.close_inner(Some(&requirement))
    }

    fn join_transfers(&self, node: Option<&NodeId>) -> Result<(), String> {
        let all = std::mem::take(&mut *self.fabric.transfers.lock().unwrap());
        let (mut transfers, other): (Vec<_>, Vec<_>) = all.into_iter().partition(|transfer| {
            node.is_none_or(|id| &transfer.source == id || &transfer.target == id)
        });
        let result = run_unit(Box::pin(async {
            while let Some(task) = transfers.last_mut() {
                let result = tokio::time::timeout(CLOSE_TIMEOUT, &mut task.task).await;
                match result {
                    Ok(Ok(Ok(()))) => {
                        transfers.pop();
                    }
                    Ok(Ok(Err(error))) => {
                        transfers.pop();
                        return Err(error.to_string());
                    }
                    Ok(Err(error)) if error.is_cancelled() => {
                        transfers.pop();
                    }
                    Ok(Err(error)) => {
                        transfers.pop();
                        return Err(error.to_string());
                    }
                    Err(_) => {
                        for transfer in &transfers {
                            transfer.task.abort()
                        }
                        return Err("DFS body transfer did not finish".into());
                    }
                }
            }
            Ok(())
        }));
        let mut owned = self.fabric.transfers.lock().unwrap();
        owned.extend(other);
        owned.extend(transfers);
        drop(owned);
        result
    }
    fn close_inner(
        &mut self,
        namespace: Option<&NamespaceCaptureRequirement>,
    ) -> Result<DfsTestObservationCoverageReport, String> {
        run_unit(Box::pin(self.kv_peers.join(CLOSE_TIMEOUT)))?;
        let ids: Vec<_> = self
            .nodes
            .iter()
            .filter(|(_, node)| node.reconciler.is_some())
            .map(|(id, _)| id.clone())
            .collect();
        for id in ids {
            self.stop_node_inner(&id)?
        }
        // No live component/host producer remains to enqueue a later wave.
        run_unit(Box::pin(self.kv_peers.join(CLOSE_TIMEOUT)))?;
        self.join_transfers(None)?;
        self.assert_fault_budgets()?;
        let checkpoints = self
            .fabric
            .observation_checkpoints
            .lock()
            .map_err(|_| "observation checkpoints poisoned")?;
        for checkpoint in checkpoints.iter() {
            checkpoint.verify()?;
        }
        let checkpointed_records: usize = checkpoints.iter().map(|entry| entry.records).sum();
        drop(checkpoints);
        match namespace {
            Some(required) if required.omitted != 0 || required.records.is_empty() => {
                return Err("DFS namespace observation window was empty or censored".into());
            }
            None if self.fabric.dropped_observations.load(Ordering::Relaxed) != 0 => {
                let records = self.rpc_observations();
                let mut observed_routes = BTreeMap::<String, usize>::new();
                for event in &records {
                    *observed_routes
                        .entry(format!(
                            "{} {:?} {:?}",
                            event.route, event.origin, event.phase
                        ))
                        .or_default() += 1;
                }
                return Err(format!(
                    "DFS test observations were censored: retained={}, omitted={}, retained_routes={observed_routes:?}",
                    records.len(),
                    self.observation_dropped_count(),
                ));
            }
            _ => {}
        }
        let lifetime_records = self
            .fabric
            .observations
            .lock()
            .map_err(|_| "lifetime observations poisoned")?
            .len()
            + checkpointed_records;
        let lifetime_omitted = self.fabric.dropped_observations.load(Ordering::Acquire);
        let coverage = DfsTestObservationCoverageReport {
            namespace_records: namespace.map(|required| required.records.len()),
            namespace_omitted: namespace.map(|required| required.omitted),
            lifetime_records,
            lifetime_omitted,
            full_lifetime_coverage: if lifetime_omitted == 0 {
                "available"
            } else {
                "unavailable: censored"
            },
        };
        // Joined tasks can transfer unresolved effects into the same real
        // retained actor/FD registry. That is a valid node stop, but final
        // utility cleanup must preserve its storage and recovery inputs.
        for node in self.nodes.values() {
            if crate::source_adoption::retained_custody_blocks_cleanup(&node.data_dir.root())
                .map_err(|error| error.to_string())?
            {
                // Leave closed false so the existing Drop error path keeps
                // this TestDir. Never release the actual retained owner here.
                return Err(format!(
                    "DFS test storage remains owned by native source custody at {}",
                    node.data_dir.root().display(),
                ));
            }
        }
        self.closed = true;
        Ok(coverage)
    }
}
impl Drop for DfsTestRuntime {
    fn drop(&mut self) {
        #[cfg(test)]
        let preserve_panic = std::thread::panicking()
            && std::env::var("SINOMO_DFS_TEST_PANIC_CAPTURE").is_ok_and(|value| value == "1");
        #[cfg(test)]
        if preserve_panic && let Some(root) = self.root.as_ref() {
            let observations = match self.fabric.observations.try_lock() {
                Ok(records) => format!("{records:#?}"),
                Err(error) => format!("observations unavailable: {error:?}"),
            };
            let capture = format!(
                "thread={:?}\nprimary={}\nomitted={}\nRPC observations BEFORE ordinary cleanup:\n{}",
                std::thread::current().name(),
                self.primary,
                self.observation_dropped_count(),
                observations
            );
            let result = std::fs::write(
                root.path().join("native-panic-rpc-observations.txt"),
                capture,
            );
            eprintln!(
                "NATIVE_PANIC_CAPTURE root={:?} write={result:?}",
                root.path()
            );
        }
        if !self.closed {
            if let Err(error) = self.close_inner(None) {
                eprintln!("shared DFS test cleanup failed: {error}");
                self.kv_peers.retain_after_failed_shutdown();
                for transfer in self.fabric.transfers.lock().unwrap().drain(..) {
                    transfer.task.abort()
                }
                if let Some(root) = self.root.take() {
                    eprintln!("retained DFS test root: {}", root.keep().display())
                }
            }
        }
        #[cfg(test)]
        if preserve_panic && let Some(root) = self.root.take() {
            eprintln!(
                "NATIVE_PANIC_RETAINED closed={} root={:?}",
                self.closed,
                root.keep()
            );
        }
    }
}

// Counts this client's synchronous txn API calls, not other runtime clients.
#[derive(Clone, Debug, Default)]
pub struct DfsTestControlPlaneProbe {
    calls: Arc<AtomicUsize>,
    point_reads: Arc<AtomicUsize>,
    point_read_keys: Arc<Mutex<Vec<String>>>,
    prefix_reads: Arc<AtomicUsize>,
}
impl DfsTestControlPlaneProbe {
    pub fn txn_calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
    /// Counts only this native client's synchronous point-read API calls.
    pub fn point_read_calls(&self) -> usize {
        self.point_reads.load(Ordering::SeqCst)
    }
    /// Keys requested by this native client's synchronous point reads.
    pub fn point_read_keys(&self) -> Vec<String> {
        self.point_read_keys
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }
    fn record_point_read<'a>(&self, keys: impl IntoIterator<Item = &'a str>) {
        self.point_reads.fetch_add(1, Ordering::SeqCst);
        self.point_read_keys
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .extend(keys.into_iter().map(str::to_string));
    }
    /// Counts only this native client's synchronous prefix-read API calls.
    pub fn prefix_read_calls(&self) -> usize {
        self.prefix_reads.load(Ordering::SeqCst)
    }
}

#[derive(Debug)]
struct ProbedNormalControlPlane {
    inner: Box<dyn DfsControlPlaneKvClient>,
    probe: DfsTestControlPlaneProbe,
}
impl DfsControlPlaneKvClient for ProbedNormalControlPlane {
    fn async_client(&mut self) -> Option<&mut dyn AsyncDfsControlPlaneKvClient> {
        // Preserve the actual native async view/capability and its futures.
        // Async transactions are deliberately outside this sync probe's count.
        self.inner.async_client()
    }
    fn configuration_snapshot(
        &self,
    ) -> Result<Option<Arc<crate::DfsConfigurationSnapshot>>, sinomo_core::kv::KvError> {
        self.inner.configuration_snapshot()
    }
    fn write_configuration_projection(
        &self,
    ) -> Result<Option<Arc<crate::DfsWriteConfigurationProjection>>, sinomo_core::kv::KvError> {
        self.inner.write_configuration_projection()
    }
    fn write_configuration_readiness(
        &self,
    ) -> Result<crate::DfsWriteConfigurationReadiness, sinomo_core::kv::KvError> {
        self.inner.write_configuration_readiness()
    }
    fn refresh_latest(&mut self) -> Result<(), sinomo_core::kv::KvError> {
        self.inner.refresh_latest()
    }
    fn placement_authority_process_identity(&self) -> Option<DfsPlacementAuthorityProcessIdentity> {
        self.inner.placement_authority_process_identity()
    }
    fn authorizes_prior_process_placement_authority_recovery(
        &self,
        local_node_id: &NodeId,
        current_process: &DfsPlacementAuthorityProcessIdentity,
    ) -> bool {
        self.inner
            .authorizes_prior_process_placement_authority_recovery(local_node_id, current_process)
    }
    fn get(&self, key: &str) -> Result<Option<sinomo_core::kv::KvEntry>, sinomo_core::kv::KvError> {
        self.probe.record_point_read([key]);
        self.inner.get(key)
    }
    fn get_consistent(
        &self,
        keys: &[String],
    ) -> Result<Vec<Option<sinomo_core::kv::KvEntry>>, sinomo_core::kv::KvError> {
        self.probe.record_point_read(keys.iter().map(String::as_str));
        self.inner.get_consistent(keys)
    }
    fn get_authoritative_consistent(
        &mut self,
        keys: &[String],
    ) -> Result<Vec<Option<sinomo_core::kv::KvEntry>>, sinomo_core::kv::KvError> {
        self.probe.record_point_read(keys.iter().map(String::as_str));
        self.inner.get_authoritative_consistent(keys)
    }
    fn get_authoritative_consistent_before(
        &mut self,
        keys: &[String],
        deadline: std::time::Instant,
    ) -> Result<Vec<Option<sinomo_core::kv::KvEntry>>, sinomo_core::kv::KvError> {
        self.probe.record_point_read(keys.iter().map(String::as_str));
        self.inner
            .get_authoritative_consistent_before(keys, deadline)
    }
    fn range_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<sinomo_core::kv::KvEntry>, sinomo_core::kv::KvError> {
        self.probe.prefix_reads.fetch_add(1, Ordering::SeqCst);
        self.inner.range_prefix(prefix)
    }
    fn cached_metadata_shards(
        &self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<Vec<DfsMetadataShardConfig>>, sinomo_core::kv::KvError> {
        self.inner.cached_metadata_shards(file_system_id)
    }
    fn range_prefix_authoritative(
        &mut self,
        prefix: &str,
    ) -> Result<crate::control_plane::DfsAuthoritativePrefixProjection, sinomo_core::kv::KvError>
    {
        self.probe.prefix_reads.fetch_add(1, Ordering::SeqCst);
        self.inner.range_prefix_authoritative(prefix)
    }
    fn put(
        &mut self,
        key: String,
        value: Vec<u8>,
    ) -> Result<sinomo_core::kv::KvRevision, sinomo_core::kv::KvError> {
        self.inner.put(key, value)
    }
    fn txn(
        &mut self,
        txn: sinomo_core::kv::KvTxn,
    ) -> Result<sinomo_core::kv::KvTxnResponse, sinomo_core::kv::KvError> {
        self.probe.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.txn(txn)
    }
    fn txn_with_native_authority(
        &mut self,
        txn: sinomo_core::kv::KvTxn,
    ) -> Result<
        (
            sinomo_core::kv::KvTxnResponse,
            Option<crate::DfsNativeTxnCommit>,
        ),
        sinomo_core::kv::KvError,
    > {
        self.probe.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.txn_with_native_authority(txn)
    }
    // The actual normal DaemonDfsControlPlaneKvClient (including its gated
    // normal view) uses the trait default linearizable_compare_value. Retain
    // that identical default so its one fresh barrier goes through counted txn.
}

impl DfsTestRuntime {
    pub fn client_with_control_plane_probe(
        &self,
        node: &NodeId,
    ) -> Result<
        (
            NetworkedDfsService<DfsNodeTransport>,
            DfsTestControlPlaneProbe,
        ),
        String,
    > {
        Self::probe_actual_control_plane(self.client(node)?)
    }

    pub fn metadata_primary_service_with_control_plane_probe(
        &self,
        path: &str,
    ) -> Result<
        (
            NetworkedDfsService<DfsNodeTransport>,
            DfsTestControlPlaneProbe,
        ),
        String,
    > {
        Self::probe_actual_control_plane(self.metadata_primary_service(path)?)
    }

    fn probe_actual_control_plane(
        mut client: NetworkedDfsService<DfsNodeTransport>,
    ) -> Result<
        (
            NetworkedDfsService<DfsNodeTransport>,
            DfsTestControlPlaneProbe,
        ),
        String,
    > {
        let inner = client
            .control_plane
            .take()
            .ok_or("normal client has no control plane")?;
        let probe = DfsTestControlPlaneProbe::default();
        client.control_plane = Some(Box::new(ProbedNormalControlPlane {
            inner,
            probe: probe.clone(),
        }));
        Ok((client, probe))
    }
}

#[cfg(test)]
mod pilot;

#[cfg(test)]
mod kv_voter_tests;

// Rooted shared native request-boundary instrumentation. Successful calls
// always continue through the same actual Raft implementation.
type NativeKvHook =
    dyn Fn(&sinomo_core::kv::KvTxnRequest) -> Result<(), ServiceError> + Send + Sync;
struct NativeKvHookEntry {
    root: std::path::PathBuf,
    id: u64,
    callback: Arc<NativeKvHook>,
    configuration: Option<Arc<NativeConfigurationFault>>,
}
static NATIVE_KV_HOOKS: OnceLock<
    Mutex<BTreeMap<std::path::PathBuf, std::sync::Weak<NativeKvHookEntry>>>,
> = OnceLock::new();
static NEXT_NATIVE_KV_HOOK: AtomicU64 = AtomicU64::new(1);
pub struct DfsTestNativeKvHookRegistration(Arc<NativeKvHookEntry>);
impl Drop for DfsTestNativeKvHookRegistration {
    fn drop(&mut self) {
        if let Some(registry) = NATIVE_KV_HOOKS.get() {
            let mut registry = registry.lock().unwrap_or_else(|error| error.into_inner());
            if registry
                .get(&self.0.root)
                .and_then(std::sync::Weak::upgrade)
                .is_some_and(|entry| entry.id == self.0.id)
            {
                registry.remove(&self.0.root);
            }
        }
    }
}
pub(crate) fn observe_native_kv_request(
    root: &std::path::Path,
    request: &sinomo_core::kv::KvTxnRequest,
) -> Result<(), ServiceError> {
    let callback = NATIVE_KV_HOOKS.get().and_then(|registry| {
        registry
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(root)
            .and_then(std::sync::Weak::upgrade)
    });
    if let Some(entry) = callback {
        (entry.callback)(request)?;
    }
    Ok(())
}

impl DfsTestRuntime {
    /// Observe/fail an actual native KV request before its ordinary admission.
    /// This hook can never supply a positive response, receipt or authority.
    pub fn native_kv_request_hook(
        &self,
        node: &NodeId,
        callback: impl Fn(&sinomo_core::kv::KvTxnRequest) -> Result<(), ServiceError>
        + Send
        + Sync
        + 'static,
    ) -> Result<DfsTestNativeKvHookRegistration, String> {
        let owned = self.nodes.get(node).ok_or("unknown native fixture node")?;
        if owned.reconciler.is_none() {
            return Err("native fixture node is stopped".into());
        }
        let root = owned.data_dir.root();
        let registry = NATIVE_KV_HOOKS.get_or_init(Default::default);
        let mut registry = registry.lock().unwrap_or_else(|error| error.into_inner());
        registry.retain(|_, value| value.strong_count() != 0);
        if registry.contains_key(&root) {
            return Err("native request hook already owned for this root".into());
        }
        let entry = Arc::new(NativeKvHookEntry {
            root: root.clone(),
            id: NEXT_NATIVE_KV_HOOK.fetch_add(1, Ordering::Relaxed),
            callback: Arc::new(callback),
            configuration: None,
        });
        registry.insert(root, Arc::downgrade(&entry));
        Ok(DfsTestNativeKvHookRegistration(entry))
    }
}

impl DfsTestRuntime {
    /// Resume the write-set keeper of `node`, which the test runtime mounts
    /// suspended so that tests driving write-set transactions by hand keep
    /// that control. It stays resumed across the node's restarts.
    pub fn enable_write_set_keeping(&self, node: &NodeId) {
        crate::runtime_host::set_write_set_keeping_suspended(self.data_dir(node).root(), false);
    }

    /// Set the network verdict `observer`'s keeper sees for `peer`. Once a
    /// test sets one, that keeper reads only verdicts the test sets.
    pub fn set_peer_verdict(
        &self,
        observer: &NodeId,
        peer: &NodeId,
        verdict: sinomo_core::network::PeerPathVerdict,
        verifying: bool,
    ) {
        crate::runtime_host::set_test_peer_verdict(
            self.data_dir(observer).root(),
            peer.clone(),
            verdict,
            verifying,
        );
    }

    /// Peers `observer`'s keeper asked the network to verify.
    pub fn peer_verification_requests(&self, observer: &NodeId) -> Vec<NodeId> {
        crate::runtime_host::test_verification_requests(&self.data_dir(observer).root())
    }

    /// Write-set events `node`'s keeper published, with their names.
    pub fn write_set_keeping_events(
        &self,
        node: &NodeId,
    ) -> Vec<(String, crate::DfsMetadataWriteSetEvent)> {
        crate::runtime_host::keeping_events_for_test(&self.data_dir(node).root())
    }

    /// Takeover events `node`'s candidate role published, with their names.
    pub fn write_set_takeover_events(
        &self,
        node: &NodeId,
    ) -> Vec<(String, crate::DfsMetadataTakeoverEvent)> {
        crate::runtime_host::takeover_events_for_test(&self.data_dir(node).root())
    }

    /// What `node`'s candidate role did, for diagnostics.
    pub fn write_set_takeover_trace(&self, node: &NodeId) -> Vec<String> {
        crate::runtime_host::takeover_trace_for_test(&self.data_dir(node).root())
    }

    /// Cut `node` off from every other node, both ways, on the traffic
    /// `partition` names, as a network partition does: a call across it
    /// fails as a call to an unreachable peer does, with nothing sent, and
    /// the node keeps running. An empty partition heals it.
    pub fn partition_node(&self, node: &NodeId, partition: Partition) {
        self.kv_peers.set_kv_partitioned(node, partition.kv);
        let mut partitions = self.fabric.partitions.lock().expect("DFS test partitions");
        if partition == Partition::default() {
            partitions.remove(node);
        } else {
            partitions.insert(node.clone(), partition);
        }
    }

    /// Stop `node`'s takeover attempts between the durable promise and the
    /// swap, as a crash there would.
    pub fn stop_takeover_after_promise(&self, node: &NodeId, stop: bool) {
        crate::runtime_host::stop_takeover_after_promise_for_test(
            self.data_dir(node).root(),
            stop,
        );
    }

    /// Hold the first write-set join each of `nodes` admits until every one
    /// of them has admitted one, so their joins overlap. Dropping the guard
    /// lets a join still held go.
    #[cfg(test)]
    pub(crate) fn hold_write_set_joins_until_all_admitted(
        &self,
        nodes: &[&NodeId],
    ) -> crate::runtime_host::JoinRendezvousGuard {
        crate::runtime_host::hold_joins_until_all_admitted_for_test(
            nodes.iter().map(|node| self.data_dir(node).root()).collect(),
        )
    }

    /// Hold `node`'s activation as a new primary until released.
    pub fn hold_takeover_activation(&self, node: &NodeId, hold: bool) {
        crate::runtime_host::hold_takeover_activation_for_test(self.data_dir(node).root(), hold);
    }

    /// Whether `node`'s keeper held a network reachability subscription at
    /// its last evaluation; `None` before it first evaluated.
    pub fn write_set_keeping_holds_reachability(&self, node: &NodeId) -> Option<bool> {
        crate::runtime_host::keeping_reachability_interest_for_test(&self.data_dir(node).root())
    }

    /// Steps `node`'s keeper could not complete, for diagnostics.
    pub fn write_set_keeping_failures(&self, node: &NodeId) -> Vec<String> {
        crate::runtime_host::keeping_failures_for_test(&self.data_dir(node).root())
    }

    /// Commit `node`'s member record with `role`, as the cluster member store
    /// does when the node's role changes.
    pub fn set_member_node_role(
        &self,
        node: &NodeId,
        role: sinomo_core::ClusterNodeRole,
    ) -> Result<(), String> {
        let member_key = sinomo_core::node::cluster_member_key(node);
        let error = |error: sinomo_core::kv::KvError| error.to_string();
        let mut client = self.configuration_publisher(node);
        for _ in 0..8 {
            let entry = client
                .get_authoritative_consistent(std::slice::from_ref(&member_key))
                .map_err(error)?
                .remove(0)
                .ok_or("native fixture node has no member record")?;
            let mut member = entry
                .decode::<sinomo_core::ClusterMemberRecord>()
                .map_err(error)?;
            member.node_role = role.clone();
            let response = client
                .txn(sinomo_core::kv::KvTxn {
                    compares: vec![sinomo_core::kv::KvCompare::ValueEquals {
                        key: member_key.clone(),
                        value: entry.value,
                    }],
                    success: vec![
                        sinomo_core::kv::KvOp::put_serde(member_key.clone(), &member, None)
                            .map_err(error)?,
                    ],
                    failure: Vec::new(),
                })
                .map_err(error)?;
            if response.succeeded {
                return Ok(());
            }
        }
        Err("member record kept changing while setting the role".into())
    }

    /// The test-support switch for the dormant write-set capability: commit
    /// `node`'s actual cluster member record with the capability advertised,
    /// and its derived capability policy record with it, as the cluster
    /// member store does when a node's binary advertises a capability.
    ///
    /// A node owns its own member record: its local cluster state carries
    /// what it advertises, and every local state projection keeps that copy
    /// over the committed one. So once the committed record advertises the
    /// capability, it goes into `node`'s local state too; that state is then
    /// behind the commit, so no authoritative read compares the two at an
    /// equal revision before both carry it.
    pub fn advertise_metadata_write_set_capability(&self, node: &NodeId) -> Result<(), String> {
        let member_key = sinomo_core::node::cluster_member_key(node);
        let policy_key = sinomo_core::node::cluster_capability_policy_key(node);
        let error = |error: sinomo_core::kv::KvError| error.to_string();
        let capability = sinomo_core::CapabilityName::new(
            sinomo_core::node::DFS_METADATA_WRITE_SET_V1_CAPABILITY,
        )
        .map_err(|error| error.to_string())?;
        let mut client = self.configuration_publisher(node);
        for _ in 0..8 {
            let entries = client
                .get_authoritative_consistent(&[member_key.clone(), policy_key.clone()])
                .map_err(error)?;
            let member_entry = entries[0]
                .clone()
                .ok_or("native fixture node has no member record")?;
            let mut member = member_entry
                .decode::<sinomo_core::ClusterMemberRecord>()
                .map_err(error)?;
            if !member.capabilities.contains(&capability) {
                member.capabilities.push(capability.clone());
            }
            if !member.allowed_capabilities.contains(&capability) {
                member.allowed_capabilities.push(capability.clone());
            }
            let policy = sinomo_core::node::ClusterCapabilityPolicyRecord::from(&member);
            let policy_compare = match entries[1].clone() {
                Some(entry) => sinomo_core::kv::KvCompare::ValueEquals {
                    key: policy_key.clone(),
                    value: entry.value,
                },
                None => sinomo_core::kv::KvCompare::Missing {
                    key: policy_key.clone(),
                },
            };
            let response = client
                .txn(sinomo_core::kv::KvTxn {
                    compares: vec![
                        sinomo_core::kv::KvCompare::ValueEquals {
                            key: member_key.clone(),
                            value: member_entry.value,
                        },
                        policy_compare,
                    ],
                    success: vec![
                        sinomo_core::kv::KvOp::put_serde(member_key.clone(), &member, None)
                            .map_err(error)?,
                        sinomo_core::kv::KvOp::put_serde(policy_key.clone(), &policy, None)
                            .map_err(error)?,
                    ],
                    failure: Vec::new(),
                })
                .map_err(error)?;
            if response.succeeded {
                return self.advertise_in_local_state(node, &capability);
            }
        }
        Err("member record kept changing while advertising the capability".into())
    }

    /// Add `capability` to `node`'s own member record in its local cluster
    /// state, until a re-read shows it there.
    fn advertise_in_local_state(
        &self,
        node: &NodeId,
        capability: &sinomo_core::CapabilityName,
    ) -> Result<(), String> {
        let paths = ClusterPaths::new(self.data_dir(node));
        let advertised = |state: &sinomo_core::ClusterBootstrapState| {
            state.members.iter().any(|member| {
                member.node_id == *node
                    && member.capabilities.contains(capability)
                    && member.allowed_capabilities.contains(capability)
            })
        };
        for _ in 0..8 {
            let mut state = sinomo_core::cluster::read_cluster_state_without_kv(&paths)
                .map_err(|error| error.to_string())?;
            if advertised(&state) {
                return Ok(());
            }
            let member = state
                .members
                .iter_mut()
                .find(|member| member.node_id == *node)
                .ok_or("native fixture node has no local member record")?;
            if !member.capabilities.contains(capability) {
                member.capabilities.push(capability.clone());
            }
            if !member.allowed_capabilities.contains(capability) {
                member.allowed_capabilities.push(capability.clone());
            }
            sinomo_core::cluster::write_cluster_state(&paths, &state)
                .map_err(|error| error.to_string())?;
        }
        Err("local cluster state kept changing while advertising the capability".into())
    }
}

// Post-commit reply loss at the native KV boundary. The request has already
// committed through the actual Raft implementation when the callback runs; an
// error it returns replaces only the reply the caller sees.
type NativeKvReplyHook =
    dyn Fn(&sinomo_core::kv::KvTxnRequest) -> Result<(), ServiceError> + Send + Sync;
struct NativeKvReplyHookEntry {
    root: std::path::PathBuf,
    id: u64,
    callback: Arc<NativeKvReplyHook>,
}
static NATIVE_KV_REPLY_HOOKS: OnceLock<
    Mutex<BTreeMap<std::path::PathBuf, std::sync::Weak<NativeKvReplyHookEntry>>>,
> = OnceLock::new();
pub struct DfsTestNativeKvReplyHookRegistration(Arc<NativeKvReplyHookEntry>);
impl Drop for DfsTestNativeKvReplyHookRegistration {
    fn drop(&mut self) {
        if let Some(registry) = NATIVE_KV_REPLY_HOOKS.get() {
            let mut registry = registry.lock().unwrap_or_else(|error| error.into_inner());
            if registry
                .get(&self.0.root)
                .and_then(std::sync::Weak::upgrade)
                .is_some_and(|entry| entry.id == self.0.id)
            {
                registry.remove(&self.0.root);
            }
        }
    }
}
pub(crate) fn observe_native_kv_reply(
    root: &std::path::Path,
    request: &sinomo_core::kv::KvTxnRequest,
) -> Result<(), ServiceError> {
    let callback = NATIVE_KV_REPLY_HOOKS.get().and_then(|registry| {
        registry
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(root)
            .and_then(std::sync::Weak::upgrade)
    });
    if let Some(entry) = callback {
        (entry.callback)(request)?;
    }
    Ok(())
}

impl DfsTestRuntime {
    /// Lose the reply of an actual native KV request after it committed.
    /// The callback decides per request; `Ok` delivers the reply unchanged.
    pub fn native_kv_reply_hook(
        &self,
        node: &NodeId,
        callback: impl Fn(&sinomo_core::kv::KvTxnRequest) -> Result<(), ServiceError>
        + Send
        + Sync
        + 'static,
    ) -> Result<DfsTestNativeKvReplyHookRegistration, String> {
        let owned = self.nodes.get(node).ok_or("unknown native fixture node")?;
        let root = owned.data_dir.root();
        let registry = NATIVE_KV_REPLY_HOOKS.get_or_init(Default::default);
        let mut registry = registry.lock().unwrap_or_else(|error| error.into_inner());
        registry.retain(|_, value| value.strong_count() != 0);
        if registry.contains_key(&root) {
            return Err("native reply hook already owned for this root".into());
        }
        let entry = Arc::new(NativeKvReplyHookEntry {
            root: root.clone(),
            id: NEXT_NATIVE_KV_HOOK.fetch_add(1, Ordering::Relaxed),
            callback: Arc::new(callback),
        });
        registry.insert(root, Arc::downgrade(&entry));
        Ok(DfsTestNativeKvReplyHookRegistration(entry))
    }
}

// Shared negative-only configuration faults, rooted in the existing native KV hook.
struct NativeConfigurationFault {
    authority: std::sync::Weak<crate::DfsMetadataAuthorityRuntime>,
    lose_delta_key: Option<String>,
    remaining_delta_loss: AtomicUsize,
    dropped_delta: Mutex<Option<sinomo_core::kv::ClusterKvCommittedDelta>>,
    pause_rebuild: AtomicBool,
    pause_used: AtomicBool,
    entered: Mutex<bool>,
    reached: std::sync::Condvar,
    release: tokio::sync::Semaphore,
    deny_floor: bool,
    deny_browser_content_preflight: AtomicBool,
    denied_browser_content_preflights: AtomicUsize,
    deny_operation_floor_key: Mutex<Option<String>>,
    denied_operation_floor_reads: AtomicUsize,
}

pub struct DfsTestNativeConfigurationFault {
    _registration: DfsTestNativeKvHookRegistration,
    fault: Arc<NativeConfigurationFault>,
}
impl DfsTestNativeConfigurationFault {
    pub fn denied_browser_content_preflights(&self) -> usize {
        self.fault
            .denied_browser_content_preflights
            .load(Ordering::Acquire)
    }

    /// Arm only a negative read return for the exact current floor key.
    /// The caller can retain this trigger at an existing normal preparation hook.
    pub fn operation_floor_denial_trigger(&self, key: String) -> impl FnOnce() + 'static + use<> {
        let fault = Arc::clone(&self.fault);
        move || {
            *fault
                .deny_operation_floor_key
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = Some(key);
        }
    }
    pub fn denied_operation_floor_reads(&self) -> usize {
        self.fault
            .denied_operation_floor_reads
            .load(Ordering::Acquire)
    }
    pub fn wait_rebuild(&self, budget: Duration) -> Result<(), String> {
        let deadline = std::time::Instant::now() + budget;
        let mut entered = self
            .fault
            .entered
            .lock()
            .map_err(|_| "native rebuild pause poisoned")?;
        while !*entered {
            let left = deadline
                .checked_duration_since(std::time::Instant::now())
                .ok_or("normal configuration rebuild did not reach its read boundary")?;
            entered = self
                .fault
                .reached
                .wait_timeout(entered, left)
                .map_err(|_| "native rebuild pause poisoned")?
                .0;
        }
        Ok(())
    }
    pub fn dropped_delta(&self) -> Option<sinomo_core::kv::ClusterKvCommittedDelta> {
        self.fault
            .dropped_delta
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
    pub fn release(&self) {
        self.fault.release.close();
    }
}
impl Drop for DfsTestNativeConfigurationFault {
    fn drop(&mut self) {
        self.fault.release.close();
    }
}

fn configuration_fault(
    authority: &crate::DfsMetadataAuthorityRuntime,
) -> Option<Arc<NativeConfigurationFault>> {
    let registry = NATIVE_KV_HOOKS.get()?;
    let retained = registry.lock().unwrap_or_else(|e| e.into_inner());
    retained
        .values()
        .filter_map(std::sync::Weak::upgrade)
        .filter_map(|entry| entry.configuration.clone())
        .find(|fault| {
            fault
                .authority
                .upgrade()
                .is_some_and(|actual| std::ptr::eq(actual.as_ref(), authority))
        })
}

pub(crate) fn withhold_native_configuration_delta(
    authority: &crate::DfsMetadataAuthorityRuntime,
    delta: &sinomo_core::kv::ClusterKvCommittedDelta,
) -> bool {
    let Some(fault) = configuration_fault(authority) else {
        return false;
    };
    let Some(key) = &fault.lose_delta_key else {
        return false;
    };
    if !delta.events.iter().any(|event| event.key == *key)
        || fault
            .remaining_delta_loss
            .compare_exchange(1, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
    {
        return false;
    }
    *fault
        .dropped_delta
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = Some(delta.clone());
    true
}

pub(crate) async fn pause_native_configuration_rebuild(
    authority: &crate::DfsMetadataAuthorityRuntime,
) {
    let Some(fault) = configuration_fault(authority) else {
        return;
    };
    if !fault.pause_rebuild.load(Ordering::Acquire)
        || fault.remaining_delta_loss.load(Ordering::Acquire) != 0
        || fault.pause_used.swap(true, Ordering::AcqRel)
    {
        return;
    }
    *fault.entered.lock().unwrap_or_else(|e| e.into_inner()) = true;
    fault.reached.notify_all();
    let _ = fault.release.acquire().await;
}

pub(crate) fn native_operation_floor_read_is_denied(
    authority: &crate::DfsMetadataAuthorityRuntime,
    key: &str,
) -> bool {
    if !key.contains("/metadata-operation-floors/") {
        return false;
    }
    let Some(fault) = configuration_fault(authority) else {
        return false;
    };
    if fault
        .deny_operation_floor_key
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .as_deref()
        != Some(key)
    {
        return false;
    }
    fault
        .denied_operation_floor_reads
        .fetch_add(1, Ordering::AcqRel);
    true
}

pub(crate) fn native_configuration_floor_is_denied(
    authority: &crate::DfsMetadataAuthorityRuntime,
) -> bool {
    configuration_fault(authority).is_some_and(|fault| fault.deny_floor)
}

pub(crate) fn browser_content_preflight_is_denied(
    authority: &crate::DfsMetadataAuthorityRuntime,
) -> bool {
    let Some(fault) = configuration_fault(authority) else {
        return false;
    };
    if !fault.deny_browser_content_preflight.load(Ordering::Acquire) {
        return false;
    }
    fault
        .denied_browser_content_preflights
        .fetch_add(1, Ordering::AcqRel);
    true
}

impl DfsTestRuntime {
    /// Own an initially unarmed negative operation-floor read fault on this
    /// actual mounted authority. It supplies no floor, result or permission.
    pub fn metadata_operation_floor_fault(
        &self,
        node: &NodeId,
    ) -> Result<DfsTestNativeConfigurationFault, String> {
        self.install_configuration_fault(node, None, false)
    }
    /// Withhold one real committed filesystem delta from this actual mounted
    /// authority and pause its normal repair read. Every KV commit still runs
    /// through the existing Raft owner; no replacement proof is provided.
    pub fn lose_configuration_delta_and_pause_rebuild(
        &self,
        node: &NodeId,
        key: String,
    ) -> Result<DfsTestNativeConfigurationFault, String> {
        self.install_configuration_fault(node, Some(key), false)
    }
    /// Pause only the next actual configuration rebuild. The triggering commit
    /// and all native observations remain intact; releasing this fault resumes
    /// the same owner without supplying a projection or authority.
    pub fn pause_next_configuration_rebuild(
        &self,
        node: &NodeId,
    ) -> Result<DfsTestNativeConfigurationFault, String> {
        let fault = self.install_configuration_fault(node, None, false)?;
        fault.fault.pause_rebuild.store(true, Ordering::Release);
        Ok(fault)
    }
    /// Deny this actual authority's floor-read return only. No synthetic floor
    /// or replacement positive result can be supplied by the hook.
    pub fn deny_configuration_floor(
        &self,
        node: &NodeId,
    ) -> Result<DfsTestNativeConfigurationFault, String> {
        self.install_configuration_fault(node, None, true)
    }

    /// Reject only the browser's content-planning preflight. Native metadata
    /// authority and namespace admission keep their ordinary configuration.
    pub fn deny_browser_content_preflight(
        &self,
        node: &NodeId,
    ) -> Result<DfsTestNativeConfigurationFault, String> {
        let fault = self.install_configuration_fault(node, None, false)?;
        fault
            .fault
            .deny_browser_content_preflight
            .store(true, Ordering::Release);
        Ok(fault)
    }
    fn install_configuration_fault(
        &self,
        node: &NodeId,
        lose_delta_key: Option<String>,
        deny_floor: bool,
    ) -> Result<DfsTestNativeConfigurationFault, String> {
        let owned = self.nodes.get(node).ok_or("unknown native fixture node")?;
        if owned.reconciler.is_none() {
            return Err("native fixture node is stopped".into());
        }
        let host = crate::dfs_browser_component_host(&owned.data_dir).map_err(|e| e.to_string())?;
        let _actual_owner = host
            .operation_guard()
            .map_err(|e| e.to_string())?
            .ok_or("native component owner is unavailable")?;
        let paths = ClusterPaths::new(&owned.data_dir);
        let state = sinomo_core::cluster::read_cluster_state_without_kv(&paths)
            .map_err(|e| e.to_string())?;
        let authority = crate::runtime_host::daemon_dfs_metadata_authority_generation_for_identity(
            &paths,
            &state.cluster_id,
            &state.local_node_id,
            Some(executor().handle()),
        )
        .map_err(|e| e.to_string())?;
        crate::runtime_host::require_daemon_dfs_data_owner_lock(
            &crate::runtime_host::DfsRuntimePaths::new(&owned.data_dir),
        )
        .map_err(|e| e.to_string())?;
        let root = owned.data_dir.root();
        let registry = NATIVE_KV_HOOKS.get_or_init(Default::default);
        let mut registry = registry.lock().unwrap_or_else(|e| e.into_inner());
        registry.retain(|_, value| value.strong_count() != 0);
        if registry.contains_key(&root) {
            return Err("native request hook already owned for this root".into());
        }
        let fault = Arc::new(NativeConfigurationFault {
            authority: Arc::downgrade(&authority),
            pause_rebuild: AtomicBool::new(lose_delta_key.is_some()),
            remaining_delta_loss: AtomicUsize::new(usize::from(lose_delta_key.is_some())),
            lose_delta_key,
            dropped_delta: Mutex::new(None),
            pause_used: AtomicBool::new(false),
            entered: Mutex::new(false),
            reached: std::sync::Condvar::new(),
            release: tokio::sync::Semaphore::new(0),
            deny_floor,
            deny_browser_content_preflight: AtomicBool::new(false),
            denied_browser_content_preflights: AtomicUsize::new(0),
            deny_operation_floor_key: Mutex::new(None),
            denied_operation_floor_reads: AtomicUsize::new(0),
        });
        let entry = Arc::new(NativeKvHookEntry {
            root: root.clone(),
            id: NEXT_NATIVE_KV_HOOK.fetch_add(1, Ordering::Relaxed),
            callback: Arc::new(|_| Ok(())),
            configuration: Some(Arc::clone(&fault)),
        });
        registry.insert(root, Arc::downgrade(&entry));
        Ok(DfsTestNativeConfigurationFault {
            _registration: DfsTestNativeKvHookRegistration(entry),
            fault,
        })
    }
}

#[cfg(test)]
mod normal_native_kv_admission_tests {
    use super::*;
    use sinomo_core::kv::{KvOp, KvTxn, KvTxnRequest};

    async fn observe_mounted_native_outcome(
        cluster: &dyn sinomo_core::service::ClusterAccess,
        request_id: &sinomo_core::kv::KvRequestId,
        on_busy: impl FnMut(),
    ) -> Result<sinomo_core::kv::ClusterKvRequestOutcomeRead, sinomo_core::service::ServiceError>
    {
        crate::runtime_host::native_request_outcome_test::admitted_native_request_outcome_observing(
            cluster, request_id, on_busy,
        )
        .await
    }

    #[test]
    fn mounted_native_outcome_observation_keeps_original_identity_across_real_busy_admission() {
        let (mut native, nodes) =
            DfsTestRuntime::start_cluster("native-original-outcome-busy", 1).unwrap();
        let context = native.component_context(&nodes[0]).unwrap();
        let cluster = context.cluster_access().unwrap();
        let request = KvTxnRequest::generated(KvTxn {
            compares: Vec::new(),
            success: vec![KvOp::Put {
                key: "/test/native-original-outcome-busy".into(),
                value: b"original body".to_vec(),
                lease_id: None,
            }],
            failure: Vec::new(),
        });
        let committed = executor()
            .block_on(cluster.kv_commit_compact_async(request.clone()))
            .unwrap();
        assert!(committed.txn_response.as_ref().unwrap().succeeded);
        let paths = ClusterPaths::new(native.data_dir(&nodes[0]));
        let held =
            sinomo_core::kv::lock_cluster_kv_raft_operation_for_paths(&paths.kv_paths()).unwrap();
        // An operation permit alone leaves an idle canonical resident readable.
        // This negative control must hold the actual mutable native checkout.
        let resident = sinomo_core::cluster::open_cluster_raft_kv_node(&paths, &nodes[0])
            .expect("hold actual native mutable resident");
        let (release, released) = std::sync::mpsc::channel();
        let budget = sinomo_core::cluster::ClusterControlPlaneTimeouts::default_daemon_transport()
            .kv_txn_quorum_options(true)
            .raft_append_timeout;
        let holder = std::thread::spawn(move || {
            let observed_busy = released.recv_timeout(budget);
            drop(resident);
            drop(held);
            observed_busy.expect("the real native read must encounter the owned admission");
        });
        let mut observed_busy = 0;
        let result = executor().block_on(observe_mounted_native_outcome(
            cluster.as_ref(),
            &request.request_id,
            || {
                observed_busy += 1;
                if observed_busy == 1 {
                    release.send(()).unwrap();
                }
            },
        ));
        // Release and join even if an unexpected error precedes the busy branch.
        let _ = release.send(());
        holder.join().unwrap();
        assert!(
            observed_busy > 0,
            "negative control must observe actual native contention"
        );
        let outcome =
            result.expect("the same original observation must finish after admission is released");
        sinomo_core::kv::validate_cluster_kv_request_outcome_read(&outcome).unwrap();
        let sinomo_core::kv::KvRequestOutcome::Committed(record) = outcome.outcome else {
            panic!("the original native committed receipt must remain available");
        };
        assert_eq!(record.request_id, request.request_id);
        assert_eq!(record.command_digest, committed.command_digest);
        assert_eq!(record.txn_response, committed.txn_response);
        drop(cluster);
        drop(context);
        native.close().unwrap();
    }

    #[test]
    fn mounted_native_kv_routes_work_before_dfs_preparation_and_stop_removes_admission() {
        let name = "native-core-kv-before-dfs-prepared";
        let node = DfsTestRuntime::ordered_node_ids(name, 1).remove(0);
        let mut native = DfsTestRuntime::new(name, node.clone()).unwrap();
        native.bootstrap_planned_cluster().unwrap();
        native.prepare_projection(&node).unwrap();
        let carrier = Carrier {
            host_identity: None,
            source: node.clone(),
            fabric: Arc::clone(&native.fabric),
            origin: RpcOrigin::RegisteredHost,
        };
        let request = KvTxnRequest::generated(KvTxn {
            compares: Vec::new(),
            success: vec![KvOp::Put {
                key: "/test/core-kv-before-dfs-ready".into(),
                value: b"actual native control before DFS preparation".to_vec(),
                lease_id: None,
            }],
            failure: Vec::new(),
        });
        let payload = sinomo_core::kv::cluster_kv_txn_rpc_payload(&request).unwrap();
        let call = || {
            ObjectRpcCall::value(ObjectRpcRequest::from_encoded_body(
                payload.route,
                payload.category,
                payload.body.clone(),
            ))
        };
        assert!(
            run_rpc(carrier.call_async(&node, call(), ClusterObjectRpcOptions::default(), true))
                .is_err(),
            "no mounted endpoint can admit control work"
        );
        native.mount_node(&node).unwrap();
        let endpoint = Arc::clone(&native.fabric.nodes.lock().unwrap()[&node]);
        assert!(
            !endpoint.accepting.load(Ordering::Acquire),
            "normal DFS preparation has not run"
        );
        assert!(endpoint.kv_available.load(Ordering::Acquire));
        assert!(endpoint.admits_route(sinomo_core::kv::CLUSTER_KV_TXN_ROUTE));
        assert!(!endpoint.admits_route(crate::DFS_DIRECT_READ_ROUTE));
        let response =
            run_rpc(carrier.call_async(&node, call(), ClusterObjectRpcOptions::default(), true))
                .unwrap();
        let response = executor()
            .block_on(response.into_materialized_value_response(64 * 1024))
            .unwrap();
        let response: sinomo_core::kv::ClusterKvTxnRpcResponse = response.decode_body().unwrap();
        assert!(response.txn_response.as_ref().unwrap().succeeded);
        let outcome = executor()
            .block_on(observe_mounted_native_outcome(
                endpoint.cluster.as_ref(),
                &request.request_id,
                || {},
            ))
            .unwrap();
        sinomo_core::kv::validate_cluster_kv_request_outcome_read(&outcome).unwrap();
        let sinomo_core::kv::KvRequestOutcome::Committed(record) = outcome.outcome else {
            panic!("normal Core route must have its actual committed native receipt");
        };
        assert_eq!(record.request_id, request.request_id);
        assert_eq!(record.command_digest, response.command_digest);
        assert_eq!(record.txn_response, response.txn_response);
        let dfs = ObjectRpcCall::value(
            ObjectRpcRequest::encode_body(crate::DFS_DIRECT_READ_ROUTE, "dfs", &()).unwrap(),
        );
        assert!(
            run_rpc(carrier.call_async(&node, dfs, ClusterObjectRpcOptions::default(), true))
                .is_err()
        );
        assert!(!endpoint.accepting.load(Ordering::Acquire));
        // Negative control only: even a prepared DFS admission cannot replace
        // the actual native KV owner's live flag. No request is dispatched.
        endpoint.accepting.store(true, Ordering::Release);
        endpoint.kv_available.store(false, Ordering::Release);
        assert!(!endpoint.admits_route(sinomo_core::kv::CLUSTER_KV_TXN_ROUTE));
        endpoint.accepting.store(false, Ordering::Release);
        endpoint.kv_available.store(true, Ordering::Release);
        let kv_available = Arc::clone(&endpoint.kv_available);
        drop(endpoint); // Do not retain mounted DFS services/data owners over stop.
        native.stop_node(&node).unwrap();
        assert!(!kv_available.load(Ordering::Acquire));
        assert!(!native.fabric.nodes.lock().unwrap().contains_key(&node));
        let paths = ClusterPaths::new(native.data_dir(&node));
        let wal = paths.kv_paths().raft_wal_paths(&node).wal;
        let before = std::fs::read(&wal).unwrap();
        assert!(
            run_rpc(carrier.call_async(&node, call(), ClusterObjectRpcOptions::default(), true))
                .is_err()
        );
        assert_eq!(std::fs::read(&wal).unwrap(), before);
        native.close().unwrap();
    }

    /// A reopened node sends before it is ready, as a daemon sends before its
    /// own ingress is: its calls pass the fabric while calls to it are still
    /// refused. Its stop ends its own calls, and a call to the stopped member
    /// still fails as a call to a dead peer does.
    #[test]
    fn reopened_host_sends_before_it_is_ready_and_stops_sending_when_it_stops() {
        let name = "native-host-sends-before-ready";
        let (mut native, nodes) = DfsTestRuntime::start_cluster(name, 2).unwrap();
        let (a, b) = (nodes[0].clone(), nodes[1].clone());
        let transport = |native: &DfsTestRuntime, node: &NodeId| {
            native.nodes[node]
                .mounted_context
                .as_ref()
                .unwrap()
                .object_rpc_transport()
                .unwrap()
        };
        let dfs_call = || {
            ObjectRpcCall::value(
                ObjectRpcRequest::encode_body(crate::DFS_DIRECT_READ_ROUTE, "dfs", &()).unwrap(),
            )
        };
        let request = KvTxnRequest::generated(KvTxn {
            compares: Vec::new(),
            success: Vec::new(),
            failure: Vec::new(),
        });
        let payload = sinomo_core::kv::cluster_kv_txn_rpc_payload(&request).unwrap();
        let kv_call = || {
            ObjectRpcCall::value(ObjectRpcRequest::from_encoded_body(
                payload.route,
                payload.category,
                payload.body.clone(),
            ))
        };
        let options = ClusterObjectRpcOptions::default;
        native.stop_node(&b).unwrap();
        native.prepare_projection(&b).unwrap();
        native.mount_node(&b).unwrap();
        assert!(
            !native.fabric.nodes.lock().unwrap()[&b]
                .accepting
                .load(Ordering::Acquire),
            "B is mounted but not ready"
        );
        let from_b = transport(&native, &b);
        let from_a = transport(&native, &a);
        // B's call passes the fabric's source and target admission and
        // reaches dispatch, where this rule ends it without effect.
        native.install_fault(FaultRule {
            source: Some(b.clone()),
            target: Some(a.clone()),
            route: Some(crate::DFS_DIRECT_READ_ROUTE.to_string()),
            call_id: None,
            origin: None,
            remaining_hits: 1,
            action: FaultAction::TransientRouteFailureBeforeDispatch,
        });
        let sent = run_rpc(from_b.call_async(&a, dfs_call(), options(), true));
        native.clear_faults();
        assert!(
            matches!(&sent, Err(NetworkError::DeliveryFailed { detail, .. })
                if detail == "bounded injected no usable route before dispatch"),
            "{sent:?}"
        );
        assert_eq!(
            from_b.availability(&a),
            sinomo_core::network::RoutedAvailability::Candidate
        );
        // A call to B is refused until B is ready.
        let refused = run_rpc(from_a.call_async(&b, dfs_call(), options(), true));
        assert!(
            matches!(&refused, Err(NetworkError::UnknownNode { node_id }) if *node_id == b),
            "{refused:?}"
        );
        assert_eq!(
            from_a.availability(&b),
            sinomo_core::network::RoutedAvailability::NoPath
        );
        native.wait_native_authority(&b).unwrap();
        native.ready_node(&b).unwrap();
        native
            .wait_startup_completion_with_budget(None, Some(&b))
            .unwrap();
        native.stop_node(&b).unwrap();
        // The stopped host's own calls end with its stop.
        let after_stop = run_rpc(from_b.call_async(&a, dfs_call(), options(), true));
        assert!(
            matches!(&after_stop, Err(NetworkError::UnknownNode { node_id }) if *node_id == b),
            "{after_stop:?}"
        );
        // A call to the stopped member, on a KV or DFS route, fails as a call
        // to a dead peer does.
        for call in [kv_call(), dfs_call()] {
            let dead = run_rpc(from_a.call_async(&b, call, options(), true));
            assert!(
                matches!(
                    &dead,
                    Err(NetworkError::RetryExhausted {
                        delivery_state: NetworkDeliveryState::DefinitelyNotSent,
                        ..
                    })
                ),
                "{dead:?}"
            );
        }
        drop((from_a, from_b));
        native.close().unwrap();
    }
}

#[cfg(test)]
mod write_fault_filter_tests {
    use super::*;
    use std::task::{Context, Poll, Waker};

    struct FilterFixture {
        fabric: Arc<Fabric>,
        context: NativeRpcCallContext,
        command: DfsMetadataJournalBatchCommand,
        gate: Arc<Semaphore>,
        captured: Arc<Mutex<Vec<DfsMetadataJournalBatchCommand>>>,
    }

    fn filter_fixture() -> FilterFixture {
        // Typed filter inputs only. No endpoint or receiver is mounted, and
        // the batch carries no executable frame until a test adds one.
        let source = NodeId::from_legacy_named("write-filter-source").unwrap();
        let target = NodeId::from_legacy_named("write-filter-target").unwrap();
        let command = DfsMetadataJournalBatchCommand {
            request_id: "write-filter-publication".into(),
            source_node_id: Some(source.clone()),
            file_system_id: Some(crate::sifs_file_system_id()),
            authority_epoch: Some(1),
            verify_prefix_only: false,
            prefix_probe: None,
            authority_probe: None,
            durable_replication: Some(DfsMetadataDurableReplication {
                confirmation_id: "filter-confirmation".into(),
                background_publication: true,
            }),
            batch: DfsMetadataJournalFrameBatch {
                shard_id: DfsMetadataShardId::new("services"),
                journal_frames: Vec::new(),
                next_after_sequence: 1,
            },
        };
        let gate = Arc::new(Semaphore::new(0));
        let captured = Arc::new(Mutex::new(Vec::new()));
        let fabric = Arc::new(Fabric {
            dropped_observations: AtomicUsize::new(0),
            exhausted_budgets: AtomicUsize::new(0),
            next_call: AtomicU64::new(1),
            nodes: Mutex::new(BTreeMap::new()),
            faults: Mutex::new(vec![FaultRule {
                source: Some(source.clone()),
                target: Some(target.clone()),
                route: Some(crate::DFS_METADATA_JOURNAL_BATCH_ROUTE.into()),
                call_id: None,
                origin: Some(RpcOrigin::RegisteredHost),
                remaining_hits: 1,
                action: FaultAction::PauseExactWriteCommitPublication {
                    operation_id: "write:filter-placeholder".into(),
                    gate: Arc::clone(&gate),
                    captured: Arc::clone(&captured),
                },
            }]),
            observations: Mutex::new(Vec::new()),
            observation_checkpoints: Mutex::new(Vec::new()),
            transfers: Mutex::new(Vec::new()),
            partitions: Mutex::new(BTreeMap::new()),
        });
        let carrier = Carrier {
            host_identity: None,
            source,
            fabric: Arc::clone(&fabric),
            origin: RpcOrigin::RegisteredHost,
        };
        let call = ObjectRpcCall::value(
            ObjectRpcRequest::encode_body(
                crate::DFS_METADATA_JOURNAL_BATCH_ROUTE,
                crate::DFS_RPC_CATEGORY,
                &command,
            )
            .unwrap(),
        );
        let observation =
            carrier.observation(&target, &call, ClusterObjectRpcOptions::default(), true);
        assert!(
            fabric.fault(&observation, &call.request).is_none(),
            "the generic predecode fault path cannot consume this decoded-commit rule"
        );
        let context = NativeRpcCallContext {
            fabric: Arc::downgrade(&fabric),
            observation,
            write_reply_capture: None,
        };
        FilterFixture {
            fabric,
            context,
            command,
            gate,
            captured,
        }
    }

    #[test]
    fn exact_write_publication_pause_filters_operation_and_stops_without_spending_other_hits() {
        // Codec/filter inputs only: these synthetic entries are never applied
        // and establish no native commit, acknowledgement or authority.
        let mut fixture = filter_fixture();
        fixture.context.observation.signer = Some(ServiceSigner::Remote {
            node_id: fixture.context.observation.source.to_string(),
            public_key: "filter-input-only".into(),
        });
        let operation_id = "write:exact-filter-operation".to_owned();
        fixture.fabric.faults.lock().unwrap()[0].action =
            FaultAction::PauseExactWriteCommitPublication {
                operation_id: operation_id.clone(),
                gate: Arc::clone(&fixture.gate),
                captured: Arc::clone(&fixture.captured),
            };
        fixture.command.verify_prefix_only = false;
        fixture.command.prefix_probe = None;
        fixture.command.durable_replication = Some(DfsMetadataDurableReplication {
            confirmation_id: "filter-confirmation".into(),
            background_publication: true,
        });
        let shard_id = fixture.command.batch.shard_id.clone();
        let make_frame = |operation: &str| {
            let digest = DfsPreparedOperationDigest {
                protocol_version: 7,
                file_system_id: crate::sifs_file_system_id(),
                shard_id: shard_id.clone(),
                owner_epoch: 1,
                operation_id: operation.into(),
                payload_digest: "0".repeat(64),
                prepare_sequence: 1,
            };
            crate::metadata_journal::encode_metadata_journal_frame_record(
                &DfsMetadataJournalEntry {
                    shard_id: digest.shard_id.clone(),
                    sequence: 2,
                    created_at: OffsetDateTime::UNIX_EPOCH,
                    mutation: DfsMetadataMutation::CommitPreparedOperationV7 {
                        file_system_id: digest.file_system_id.clone(),
                        operation_id: operation.into(),
                        digest,
                    },
                },
            )
            .unwrap()
        };
        fixture.command.batch.journal_frames = vec![make_frame(&operation_id)];
        fixture.command.batch.next_after_sequence = 2;
        let mut other = fixture.command.clone();
        other.batch.journal_frames = vec![make_frame("write:unrelated")];
        let mut foreground = fixture.command.clone();
        foreground
            .durable_replication
            .as_mut()
            .unwrap()
            .background_publication = false;
        let mut source = fixture.command.clone();
        source.source_node_id = Some(fixture.context.observation.target.clone());
        let mut no_commit = fixture.command.clone();
        no_commit.batch.journal_frames.clear();
        let mut context = Context::from_waker(Waker::noop());
        for command in [&other, &foreground, &source, &no_commit] {
            let mut pause = Box::pin(NATIVE_RPC_CALL_CONTEXT.scope(
                fixture.context.clone(),
                pause_decoded_write_commit_publication(command),
            ));
            assert!(matches!(
                pause.as_mut().poll(&mut context),
                Poll::Ready(Ok(()))
            ));
        }
        assert_eq!(fixture.fabric.faults.lock().unwrap()[0].remaining_hits, 1);
        assert!(fixture.captured.lock().unwrap().is_empty());
        let mut pause = Box::pin(NATIVE_RPC_CALL_CONTEXT.scope(
            fixture.context.clone(),
            pause_decoded_write_commit_publication(&fixture.command),
        ));
        assert!(matches!(pause.as_mut().poll(&mut context), Poll::Pending));
        assert_eq!(fixture.fabric.faults.lock().unwrap()[0].remaining_hits, 0);
        assert_eq!(
            *fixture.captured.lock().unwrap(),
            vec![fixture.command.clone()]
        );
        fixture
            .fabric
            .close_paused_faults_for_node(&NodeId::generate());
        assert!(!fixture.gate.is_closed());
        fixture
            .fabric
            .close_paused_faults_for_node(&fixture.context.observation.target);
        assert!(matches!(
            pause.as_mut().poll(&mut context),
            Poll::Ready(Err(_))
        ));
        assert_eq!(fixture.fabric.exhausted_budgets.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn write_reply_capture_correlates_only_exact_completed_response_data() {
        // Rejected response DATA exercises the negative helper's correlation;
        // no actual committed result or native receiver is manufactured.
        let fixture = filter_fixture();
        for kind in [
            CapturedWriteReplyKind::Write,
            CapturedWriteReplyKind::Distributed,
        ] {
            let route = match kind {
                CapturedWriteReplyKind::Write => DFS_DATA_WRITE_ROUTE,
                CapturedWriteReplyKind::Distributed => DFS_DATA_DISTRIBUTED_WRITE_ROUTE,
            };
            let object_id = DfsObjectId::generate();
            let expected = DecodedWriteReplyIdentity {
                call_id: fixture.context.observation.call_id,
                source: fixture.context.observation.source.clone(),
                target: fixture.context.observation.target.clone(),
                route: route.into(),
                outer_request_id: "dfs_message_11".into(),
                frame_id: 11,
                message_id: 22,
                request_id: "actual-decoded-write-request".into(),
                object_id: object_id.clone(),
                kind,
            };
            let mut observation = fixture.context.observation.clone();
            observation.route = route.into();
            observation.signer = Some(ServiceSigner::Remote {
                node_id: expected.source.to_string(),
                public_key: "correlation-only".into(),
            });
            let message = match kind {
                CapturedWriteReplyKind::Write => {
                    DfsNetworkMessage::WriteResponse(DfsWriteResponse {
                        request_id: expected.request_id.clone(),
                        object_id: object_id.clone(),
                        result: DfsWriteResult::Rejected {
                            reason: "correlation-only".into(),
                        },
                    })
                }
                CapturedWriteReplyKind::Distributed => {
                    DfsNetworkMessage::DistributedWriteResponse(DfsDistributedWriteResponse {
                        request_id: expected.request_id.clone(),
                        object_id: object_id.clone(),
                        result: DfsDistributedWriteResult::Rejected {
                            reason: "correlation-only".into(),
                            current_primary_node_id: None,
                        },
                        metadata_journal_batches: Vec::new(),
                    })
                }
            };
            let routed = DfsRoutedMessage {
                source_node_id: expected.target.clone(),
                target_node_id: expected.source.clone(),
                frame_id: 901,
                message_id: 902,
                message,
            };
            let response = DfsMessageResponse {
                request_id: expected.outer_request_id.clone(),
                outbound_messages: vec![routed.clone()],
            };
            assert_eq!(
                captured_write_reply(&expected, &observation, response.clone()).unwrap(),
                routed
            );
            let mut wrong = response.clone();
            wrong.request_id.push_str("-other");
            assert!(captured_write_reply(&expected, &observation, wrong).is_err());
            let mut wrong = response.clone();
            wrong.outbound_messages.push(routed.clone());
            assert!(captured_write_reply(&expected, &observation, wrong).is_err());
            let mut wrong = response.clone();
            wrong.outbound_messages[0].source_node_id = expected.source.clone();
            assert!(captured_write_reply(&expected, &observation, wrong).is_err());
            let mut wrong = response.clone();
            wrong.outbound_messages[0].message =
                DfsNetworkMessage::RepairReplicaResponse(DfsRepairReplicaResponse {
                    request_id: expected.request_id.clone(),
                    intent_id: "not-a-write-result".into(),
                    file_system_id: crate::sifs_file_system_id(),
                    object_id: object_id.clone(),
                    version: DfsVersion::generate(),
                    replica_node_id: expected.target.clone(),
                    result: DfsBlobTransferResult::Rejected {
                        reason: "wrong-family".into(),
                    },
                    receipt_generation: 0,
                });
            assert!(captured_write_reply(&expected, &observation, wrong).is_err());
            let mut foreign_call = observation.clone();
            foreign_call.call_id += 1;
            assert!(captured_write_reply(&expected, &foreign_call, response.clone()).is_err());
            let mut foreign_request = expected.clone();
            foreign_request.request_id.push_str("-other");
            assert!(captured_write_reply(&foreign_request, &observation, response).is_err());
        }
    }

    #[test]
    fn write_reply_loss_binds_first_call_without_waiving_same_call_budget() {
        // Selector/lifetime DATA only: no receiver or completed reply is
        // manufactured, and the deliberate exhausted hit is asserted below.
        let fixture = filter_fixture();
        let captured = Arc::new(Mutex::new(Vec::<DfsRoutedMessage>::new()));
        let mut observation = fixture.context.observation.clone();
        observation.route = DFS_DATA_DISTRIBUTED_WRITE_ROUTE.into();
        observation.call_id = 41;
        let request = ObjectRpcRequest::encode_body(
            DFS_DATA_DISTRIBUTED_WRITE_ROUTE,
            crate::DFS_RPC_CATEGORY,
            &"selector-only",
        )
        .unwrap();
        *fixture.fabric.faults.lock().unwrap() = vec![FaultRule {
            source: Some(observation.source.clone()),
            target: Some(observation.target.clone()),
            route: Some(observation.route.clone()),
            call_id: None,
            origin: Some(observation.origin),
            remaining_hits: 1,
            action: FaultAction::CaptureWriteReplyAndLoseAfterDispatch(Arc::clone(&captured)),
        }];
        let mut unrelated = observation.clone();
        unrelated.route = DFS_DATA_READ_ROUTE.into();
        assert!(fixture.fabric.fault(&unrelated, &request).is_none());
        {
            let rules = fixture.fabric.faults.lock().unwrap();
            assert_eq!(rules[0].call_id, None);
            assert_eq!(rules[0].remaining_hits, 1);
        }
        assert!(matches!(fixture.fabric.fault(&observation, &request),
            Some(FaultAction::CaptureWriteReplyAndLoseAfterDispatch(actual)) if Arc::ptr_eq(&actual, &captured)));
        {
            let rules = fixture.fabric.faults.lock().unwrap();
            assert_eq!(rules[0].call_id, Some(41));
            assert_eq!(rules[0].remaining_hits, 0);
        }
        let mut retry = observation.clone();
        retry.call_id = 42;
        assert!(fixture.fabric.fault(&retry, &request).is_none());
        assert_eq!(fixture.fabric.exhausted_budgets.load(Ordering::Relaxed), 0);
        assert!(matches!(
            fixture.fabric.fault(&observation, &request),
            Some(FaultAction::BudgetExhausted)
        ));
        assert_eq!(fixture.fabric.exhausted_budgets.load(Ordering::Relaxed), 1);
        retry.call_id = 43;
        assert!(fixture.fabric.fault(&retry, &request).is_none());
        assert_eq!(fixture.fabric.exhausted_budgets.load(Ordering::Relaxed), 1);
        assert!(
            captured.lock().unwrap().is_empty(),
            "selection cannot manufacture a completed reply"
        );
        fixture.fabric.faults.lock().unwrap().retain(|rule| !matches!(
            &rule.action, FaultAction::CaptureWriteReplyAndLoseAfterDispatch(actual) if Arc::ptr_eq(actual, &captured)
        ));
        assert!(fixture.fabric.faults.lock().unwrap().is_empty());
        assert_eq!(
            Arc::strong_count(&captured),
            1,
            "removing the exact rule releases its capture owner"
        );
    }
}

#[cfg(test)]
mod owned_fault_cleanup_tests {
    use super::*;

    #[test]
    fn removing_owned_capture_rule_preserves_an_unknown_capture_with_the_same_selector() {
        // Pure negative-rule configuration; no receiver, ACK, quorum or Ready
        // outcome is manufactured or exercised by this utility test.
        let name = "native-owned-fault-rule-cleanup-data";
        let nodes = DfsTestRuntime::ordered_node_ids(name, 2);
        let mut runtime = DfsTestRuntime::new(name, nodes[0].clone()).unwrap();
        runtime.define_node(nodes[1].clone()).unwrap();
        let owned = Arc::new(Mutex::new(StageExchangeCapture::default()));
        let unknown = Arc::new(Mutex::new(StageExchangeCapture::default()));
        let configured = |capture| FaultRule {
            source: Some(nodes[0].clone()),
            target: Some(nodes[1].clone()),
            route: Some(crate::DFS_DATA_STAGE_BLOB_ROUTE.into()),
            call_id: None,
            origin: Some(RpcOrigin::RegisteredHost),
            remaining_hits: 1,
            action: FaultAction::CaptureStageExchangeAndLoseAfterDispatch(capture),
        };
        runtime.install_fault(configured(Arc::clone(&owned)));
        runtime.install_fault(configured(Arc::clone(&unknown)));
        let removed = runtime.remove_faults_matching(|rule| {
            matches!(&rule.action, FaultAction::CaptureStageExchangeAndLoseAfterDispatch(capture)
                if Arc::ptr_eq(capture, &owned))
        });
        assert_eq!(removed, 1);
        assert_eq!(runtime.remove_faults_matching(|rule| {
            matches!(&rule.action, FaultAction::CaptureStageExchangeAndLoseAfterDispatch(capture)
                if Arc::ptr_eq(capture, &owned))
        }), 0);
        {
            let remaining = runtime.fabric.faults.lock().unwrap();
            assert_eq!(remaining.len(), 1);
            assert_eq!(remaining[0].remaining_hits, 1);
            assert!(matches!(&remaining[0].action,
                FaultAction::CaptureStageExchangeAndLoseAfterDispatch(capture)
                    if Arc::ptr_eq(capture, &unknown)));
            assert_eq!(runtime.fabric.exhausted_budgets.load(Ordering::Relaxed), 0);
        }
        runtime.remove_faults_matching(|rule| {
            matches!(&rule.action, FaultAction::CaptureStageExchangeAndLoseAfterDispatch(capture)
                if Arc::ptr_eq(capture, &unknown))
        });
        runtime.close().unwrap();
    }
    #[test]
    fn native_cancelled_invocation_releases_endpoint_before_idle_notification() {
        use std::task::{Context, Poll, Wake, Waker};
        struct DrainObservation {
            endpoint: std::sync::Weak<Endpoint>,
            observed: Mutex<Option<(usize, usize)>>,
        }
        impl DrainObservation {
            fn observe(&self) {
                let references = self.endpoint.strong_count();
                let active = self
                    .endpoint
                    .upgrade()
                    .unwrap()
                    .active
                    .load(Ordering::Acquire);
                *self.observed.lock().unwrap() = Some((active, references));
            }
        }
        impl Wake for DrainObservation {
            fn wake(self: Arc<Self>) {
                self.observe();
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.observe();
            }
        }
        let (native, nodes) =
            DfsTestRuntime::start_cluster("native-invocation-idle-owner-order", 1).unwrap();
        let node = nodes[0].clone();
        let endpoint = Arc::clone(&native.fabric.nodes.lock().unwrap()[&node]);
        assert!(endpoint.registry.owns(crate::DFS_STATUS_ROUTE));
        let carrier = Carrier {
            host_identity: None,
            source: node.clone(),
            fabric: Arc::clone(&native.fabric),
            origin: RpcOrigin::RegisteredHost,
        };
        let call = ObjectRpcCall::value(
            ObjectRpcRequest::encode_body(crate::DFS_STATUS_ROUTE, crate::DFS_RPC_CATEGORY, &())
                .unwrap(),
        );
        let observation =
            carrier.observation(&node, &call, ClusterObjectRpcOptions::default(), true);
        native.install_fault(FaultRule {
            source: Some(node.clone()),
            target: Some(node.clone()),
            route: Some(crate::DFS_STATUS_ROUTE.into()),
            call_id: Some(observation.call_id),
            origin: Some(RpcOrigin::RegisteredHost),
            remaining_hits: 1,
            action: FaultAction::PauseBeforeDispatch(Arc::new(Semaphore::new(0))),
        });
        let mut call =
            Box::pin(carrier.deliver(&node, call, observation, Arc::new(AtomicBool::new(false))));
        let mut pending_context = Context::from_waker(Waker::noop());
        assert!(matches!(
            call.as_mut().poll(&mut pending_context),
            Poll::Pending
        ));
        // Withdraw the real native endpoint exactly as stop does. No new peer
        // invocation can obscure the one admitted, paused call's drain edge.
        endpoint.accepting.store(false, Ordering::Release);
        endpoint.kv_available.store(false, Ordering::Release);
        run_unit(Box::pin(native.kv_peers.join(CLOSE_TIMEOUT))).unwrap();
        native.join_transfers(Some(&node)).unwrap();
        let deadline = std::time::Instant::now() + CLOSE_TIMEOUT;
        while endpoint.active.load(Ordering::Acquire) != 1 {
            assert!(
                std::time::Instant::now() < deadline,
                "other native ingress did not drain"
            );
            std::thread::yield_now();
        }
        let observer = Arc::new(DrainObservation {
            endpoint: Arc::downgrade(&endpoint),
            observed: Mutex::new(None),
        });
        let waker = Waker::from(Arc::clone(&observer));
        let mut context = Context::from_waker(&waker);
        let mut idle = Box::pin(endpoint.idle.notified());
        assert!(matches!(idle.as_mut().poll(&mut context), Poll::Pending));
        drop(call);
        let actual = *observer.observed.lock().unwrap();
        drop(idle);
        drop(endpoint);
        drop(carrier);
        native.clear_faults();
        native.close().unwrap();
        // Only the mounted registry and this test's observer reference may
        // remain when idle is published. No call may still own a service lease.
        assert_eq!(
            actual,
            Some((0, 2)),
            "native idle notification retained a per-call endpoint capability"
        );
    }
}
