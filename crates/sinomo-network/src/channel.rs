//! Bounded, persistent multiplexing for canonical object-RPC calls.
//!
//! This module deliberately stops below authentication and physical framing.
//! A tunnel, local daemon transport, or direct authenticated connection carries
//! [`ObjectRpcChannelWireFrame`] values and authenticates the physical channel
//! once. Logical calls then use call ids, ordered offsets, declared lengths,
//! and bounded flow control without signing or hashing data quanta.

use crate::channel_wire::{
    ObjectRpcChannelDataFrame, ObjectRpcChannelDataHeader, ObjectRpcChannelTransportFrame,
};
use crate::session::{BodyProducer, ObjectRpcChannelBodySender};
use crate::{
    NetworkDeliveryState, NetworkError, NetworkTimelineEvent, NetworkTimelineTraceContext,
    ObjectFrameReadTiming, ObjectFrameWriteTiming, ObjectRpcBodySource, ObjectRpcIncomingBody,
    ObjectRpcRequest, ObjectRpcResponse, ObjectRpcSessionCall, ObjectRpcSessionRequest,
    ObjectRpcSessionResponse, ObjectRpcSessionResult, network_timeline_flag,
    network_timeline_monotonic_ns, network_timeline_stage,
    network_timeline_trace_context_for_request, record_network_timeline_event,
    record_network_timeline_event_now,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use tokio::io::AsyncReadExt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};

/// Hard channel-wide logical-call limit.
pub const OBJECT_RPC_CHANNEL_MAX_CALLS: usize = 64;
/// Hard aggregate payload-byte bound for the outbound Data scheduler.
pub const OBJECT_RPC_CHANNEL_MAX_QUEUED_BYTES: usize = 8 * 1024 * 1024;
/// Backward-compatible name retained for the Data configuration field.
pub const OBJECT_RPC_CHANNEL_MAX_QUEUED_DATA_BYTES: usize = OBJECT_RPC_CHANNEL_MAX_QUEUED_BYTES;
/// Control frames use an independent reservation so a full data window cannot
/// prevent credit, cancellation, reset, or completion from reaching the wire.
const OBJECT_RPC_CHANNEL_MAX_QUEUED_CONTROL_BYTES: usize = 1024 * 1024;
/// Raw body frames preserve multiplexing fairness without forcing one RTT per
/// small transport quantum.
pub const OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES: usize = 256 * 1024;
/// Each logical body may pipeline this many unacknowledged bytes.
pub const OBJECT_RPC_CHANNEL_CALL_WINDOW_BYTES: usize = 8 * 1024 * 1024;
/// Receivers coalesce cumulative credit until this much body progress accrues.
const OBJECT_RPC_CHANNEL_WINDOW_UPDATE_BYTES: usize = 1024 * 1024;
/// A low-rate body still returns cumulative credit without waiting to fill the
/// byte threshold.
const OBJECT_RPC_CHANNEL_WINDOW_UPDATE_INTERVAL: std::time::Duration =
    std::time::Duration::from_millis(2);
/// One queue-budget reservation covers several independently scheduled frames.
const OBJECT_RPC_CHANNEL_QUEUE_BATCH_BYTES: usize = 1024 * 1024;
/// Values at or below this size stay in the Request/Response control exchange.
pub const OBJECT_RPC_CHANNEL_INLINE_VALUE_BYTES: usize = 64 * 1024;
/// Materialized typed values are bounded; larger payloads must use attachments.
pub const OBJECT_RPC_CHANNEL_MAX_MATERIALIZED_VALUE_BYTES: usize = 8 * 1024 * 1024;
/// Aggregate typed-value materialization budget across both directions.
pub const OBJECT_RPC_CHANNEL_MAX_AGGREGATE_MATERIALIZED_VALUE_BYTES: usize =
    OBJECT_RPC_CHANNEL_MAX_MATERIALIZED_VALUE_BYTES;

const DEFAULT_CONTROL_QUEUE_DEPTH: usize = 256;
const DEFAULT_DATA_QUEUE_DEPTH: usize = 256;
const MAX_CONTROL_QUEUE_DEPTH: usize = 256;
const MAX_DATA_QUEUE_DEPTH: usize = 256;
const CONTROL_BURST_BEFORE_DATA: usize = 8;
const INBOUND_BODY_EVENT_DEPTH: usize =
    OBJECT_RPC_CHANNEL_CALL_WINDOW_BYTES / OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES + 1;
const MAX_CONTROL_HEAD_BYTES: usize = 256 * 1024;
const MAX_COMPLETED_CALL_TOMBSTONES: usize = OBJECT_RPC_CHANNEL_MAX_CALLS * 4;
static NEXT_OBJECT_RPC_CHANNEL_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
pub struct ObjectRpcChannelConfig {
    pub max_calls: usize,
    /// Aggregate payload bytes across all Data lanes. Control traffic has a
    /// separate fixed reserve so a full Data queue cannot deadlock the call.
    pub max_queued_data_bytes: usize,
    pub control_queue_depth: usize,
    pub data_queue_depth: usize,
}

impl Default for ObjectRpcChannelConfig {
    fn default() -> Self {
        Self {
            max_calls: OBJECT_RPC_CHANNEL_MAX_CALLS,
            max_queued_data_bytes: OBJECT_RPC_CHANNEL_MAX_QUEUED_DATA_BYTES,
            control_queue_depth: DEFAULT_CONTROL_QUEUE_DEPTH,
            data_queue_depth: DEFAULT_DATA_QUEUE_DEPTH,
        }
    }
}

impl ObjectRpcChannelConfig {
    fn validate(&self) -> Result<(), NetworkError> {
        if self.max_calls == 0 || self.max_calls > OBJECT_RPC_CHANNEL_MAX_CALLS {
            return Err(channel_resource_error(
                "configure multiplexed object RPC call limit",
                self.max_calls,
            ));
        }
        if self.max_queued_data_bytes < OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES
            || self.max_queued_data_bytes > OBJECT_RPC_CHANNEL_MAX_QUEUED_DATA_BYTES
        {
            return Err(channel_resource_error(
                "configure multiplexed object RPC queued bytes",
                self.max_queued_data_bytes,
            ));
        }
        if self.control_queue_depth == 0
            || self.control_queue_depth > MAX_CONTROL_QUEUE_DEPTH
            || self.data_queue_depth == 0
            || self.data_queue_depth > MAX_DATA_QUEUE_DEPTH
        {
            return Err(channel_resource_error(
                "configure multiplexed object RPC queue depth",
                self.control_queue_depth.max(self.data_queue_depth),
            ));
        }
        Ok(())
    }
}

/// Assigns disjoint call-id parity to the two physical-channel endpoints.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObjectRpcChannelRole {
    Dialer,
    Acceptor,
}

impl ObjectRpcChannelRole {
    const fn first_call_id(self) -> u64 {
        match self {
            Self::Dialer => 1,
            Self::Acceptor => 2,
        }
    }

    const fn owns_call_id(self, call_id: ObjectRpcChannelCallId) -> bool {
        call_id.0 != 0 && call_id.0 % 2 == self.first_call_id() % 2
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ObjectRpcChannelCallId(pub u64);

impl fmt::Display for ObjectRpcChannelCallId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Correlation retained by the egress while a sampled frame is being written.
/// Physical carriers use this to time the ordinary frame writer without a
/// separate profiling transport or message API.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObjectRpcChannelWriteTimeline {
    pub trace: NetworkTimelineTraceContext,
    pub connection_id: u64,
    pub call_id: ObjectRpcChannelCallId,
    pub frame_id: u64,
    pub flags: u16,
    pub bytes: u32,
}

impl ObjectRpcChannelWriteTimeline {
    pub fn record_frame_write(self, duration_ns: u64, success: bool) {
        let flags = self.flags
            | if success {
                network_timeline_flag::SUCCESS
            } else {
                0
            };
        record_network_timeline_event_now(NetworkTimelineEvent::new(
            self.trace,
            network_timeline_stage::FRAME_WRITE,
            flags,
            self.connection_id,
            self.call_id.0,
            self.frame_id,
            self.bytes,
            0,
            duration_ns,
        ));
    }

    /// Records SOBJ encode/allocation separately from the subsequent physical
    /// socket write. An absent timing means encoding failed before a complete
    /// frame existed.
    pub fn record_frame_write_timing(self, timing: Option<ObjectFrameWriteTiming>, success: bool) {
        let Some(timing) = timing else {
            record_network_timeline_event_now(NetworkTimelineEvent::new(
                self.trace,
                network_timeline_stage::FRAME_ENCODE,
                self.flags,
                self.connection_id,
                self.call_id.0,
                self.frame_id,
                self.bytes,
                0,
                0,
            ));
            self.record_frame_write(0, false);
            return;
        };
        record_channel_timeline_event_at(
            self.trace,
            network_timeline_stage::FRAME_ENCODE,
            self.flags | network_timeline_flag::SUCCESS,
            self.connection_id,
            self.call_id,
            self.frame_id,
            timing.wire_bytes,
            0,
            timing.encode_completed_monotonic_ns,
            timing
                .encode_completed_monotonic_ns
                .saturating_sub(timing.encode_started_monotonic_ns),
        );
        record_channel_timeline_event_at(
            self.trace,
            network_timeline_stage::FRAME_SOCKET_WRITE,
            self.flags
                | if success {
                    network_timeline_flag::SUCCESS
                } else {
                    0
                },
            self.connection_id,
            self.call_id,
            self.frame_id,
            timing.wire_bytes,
            0,
            timing.socket_write_completed_monotonic_ns,
            timing
                .socket_write_completed_monotonic_ns
                .saturating_sub(timing.encode_completed_monotonic_ns),
        );
        record_channel_timeline_event_at(
            self.trace,
            network_timeline_stage::FRAME_WRITE,
            self.flags
                | if success {
                    network_timeline_flag::SUCCESS
                } else {
                    0
                },
            self.connection_id,
            self.call_id,
            self.frame_id,
            timing.wire_bytes,
            0,
            timing.socket_write_completed_monotonic_ns,
            timing
                .socket_write_completed_monotonic_ns
                .saturating_sub(timing.encode_started_monotonic_ns),
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn record_channel_timeline_event_at(
    trace: NetworkTimelineTraceContext,
    stage: u16,
    flags: u16,
    connection_id: u64,
    call_id: ObjectRpcChannelCallId,
    frame_id: u64,
    bytes: u32,
    aux: u32,
    monotonic_ns: u64,
    duration_ns: u64,
) {
    let mut event = NetworkTimelineEvent::new(
        trace,
        stage,
        flags,
        connection_id,
        call_id.0,
        frame_id,
        bytes,
        aux,
        duration_ns,
    );
    event.monotonic_ns = monotonic_ns;
    record_network_timeline_event(event);
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ObjectRpcChannelBodyDirection {
    Request,
    Response,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ObjectRpcChannelBodyKind {
    Value,
    Attachment,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectRpcChannelBodyDescriptor {
    pub kind: ObjectRpcChannelBodyKind,
    pub total_bytes: Option<u64>,
}

/// Serde-owned logical wire contract. Physical transports provide length
/// framing and one authenticated connection; these frames contain no crypto.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum ObjectRpcChannelWireFrame {
    Request {
        call_id: ObjectRpcChannelCallId,
        request: ObjectRpcRequest,
        body: Option<ObjectRpcChannelBodyDescriptor>,
    },
    RequestAccepted {
        call_id: ObjectRpcChannelCallId,
        credit_bytes: u32,
    },
    Response {
        call_id: ObjectRpcChannelCallId,
        response: ObjectRpcResponse,
        body: Option<ObjectRpcChannelBodyDescriptor>,
        initial_credit_bytes: u32,
    },
    Data {
        call_id: ObjectRpcChannelCallId,
        direction: ObjectRpcChannelBodyDirection,
        offset: u64,
        #[serde(
            serialize_with = "crate::fallible_serde::serialize_bytes",
            deserialize_with = "crate::fallible_serde::deserialize_bytes"
        )]
        bytes: Vec<u8>,
    },
    WindowUpdate {
        call_id: ObjectRpcChannelCallId,
        direction: ObjectRpcChannelBodyDirection,
        received_offset: u64,
    },
    BodyEnd {
        call_id: ObjectRpcChannelCallId,
        direction: ObjectRpcChannelBodyDirection,
        total_bytes: u64,
    },
    Cancel {
        call_id: ObjectRpcChannelCallId,
        reason: String,
    },
    Reset {
        call_id: ObjectRpcChannelCallId,
        reason: String,
    },
}

impl ObjectRpcChannelWireFrame {
    pub const fn call_id(&self) -> ObjectRpcChannelCallId {
        match self {
            Self::Request { call_id, .. }
            | Self::RequestAccepted { call_id, .. }
            | Self::Response { call_id, .. }
            | Self::Data { call_id, .. }
            | Self::WindowUpdate { call_id, .. }
            | Self::BodyEnd { call_id, .. }
            | Self::Cancel { call_id, .. }
            | Self::Reset { call_id, .. } => *call_id,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ObjectRpcChannelQueueSnapshot {
    pub active_calls: usize,
    pub queued_control_frames: usize,
    pub queued_bytes: usize,
    pub queued_data_bytes: usize,
}

pub struct ObjectRpcChannelEndpoint {
    pub client: ObjectRpcChannelClient,
    pub server: ObjectRpcChannelServer,
    pub ingress: ObjectRpcChannelIngress,
    pub egress: ObjectRpcChannelEgress,
}

impl ObjectRpcChannelEndpoint {
    pub fn new(
        role: ObjectRpcChannelRole,
        config: ObjectRpcChannelConfig,
    ) -> Result<Self, NetworkError> {
        config.validate()?;
        let connection_id = NEXT_OBJECT_RPC_CHANNEL_CONNECTION_ID
            .fetch_add(1, Ordering::Relaxed)
            .max(1);
        let (control_sender, control_receiver) = mpsc::channel(config.control_queue_depth);
        let (data_sender, data_receiver) = mpsc::channel(config.data_queue_depth);
        let (inbound_sender, inbound_receiver) = mpsc::channel(config.max_calls);
        let (closed_signal, closed) = watch::channel(false);
        let outbound = Arc::new(OutboundQueues {
            connection_id,
            control_sender,
            data_sender,
            control_budget: Arc::new(Semaphore::new(OBJECT_RPC_CHANNEL_MAX_QUEUED_CONTROL_BYTES)),
            data_budget: Arc::new(Semaphore::new(config.max_queued_data_bytes)),
            data_frame_budget: Arc::new(Semaphore::new(config.data_queue_depth)),
            data_batch_bytes: config
                .max_queued_data_bytes
                .min(OBJECT_RPC_CHANNEL_QUEUE_BATCH_BYTES),
            data_batch_frames: config
                .data_queue_depth
                .min(OBJECT_RPC_CHANNEL_QUEUE_BATCH_BYTES / OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES),
            queued_control_frames: Arc::new(AtomicUsize::new(0)),
            queued_bytes: Arc::new(AtomicUsize::new(0)),
            queued_data_bytes: Arc::new(AtomicUsize::new(0)),
        });
        let shared = Arc::new(ChannelShared {
            connection_id,
            role,
            next_call_id: AtomicU64::new(role.first_call_id()),
            request_admission: Mutex::new(()),
            call_permits: Arc::new(Semaphore::new(config.max_calls)),
            max_calls: config.max_calls,
            outbound,
            inbound_sender,
            materialized_value_budget: Arc::new(Semaphore::new(
                OBJECT_RPC_CHANNEL_MAX_AGGREGATE_MATERIALIZED_VALUE_BYTES,
            )),
            pending: Mutex::new(HashMap::new()),
            inbound: Mutex::new(HashMap::new()),
            tombstones: Mutex::new(CallTombstones::default()),
            retired_local_high_watermark: AtomicU64::new(0),
            retired_peer_high_watermark: AtomicU64::new(0),
            closed_fast: AtomicBool::new(false),
            closed: Mutex::new(None),
            closed_signal,
        });
        Ok(Self {
            client: ObjectRpcChannelClient {
                shared: Arc::clone(&shared),
            },
            server: ObjectRpcChannelServer {
                receiver: inbound_receiver,
                closed: closed.clone(),
            },
            ingress: ObjectRpcChannelIngress {
                shared: Arc::clone(&shared),
            },
            egress: ObjectRpcChannelEgress {
                shared,
                control_receiver,
                data_receiver,
                data_lanes: HashMap::new(),
                ready_data_lanes: VecDeque::new(),
                control_burst: 0,
                closed,
                pending_write: None,
            },
        })
    }
}

#[derive(Clone)]
pub struct ObjectRpcChannelClient {
    shared: Arc<ChannelShared>,
}

impl ObjectRpcChannelClient {
    pub fn is_closed(&self) -> bool {
        self.shared.is_closed()
    }

    /// Stable process-local identity for correlating route lookups, carrier
    /// frames, and lifecycle events in a bounded network timeline.
    pub fn connection_id(&self) -> u64 {
        self.shared.connection_id
    }

    /// Returns the physical-channel close reason on the cold diagnostic path.
    pub fn close_detail(&self) -> Option<String> {
        self.shared.close_detail()
    }

    pub async fn call(
        &self,
        call: ObjectRpcSessionCall,
    ) -> Result<ObjectRpcChannelPendingCall, NetworkError> {
        let timeline = network_timeline_trace_context_for_request(&call.request);
        let call_started_ns = timeline
            .map(|_| network_timeline_monotonic_ns())
            .unwrap_or(0);
        self.shared.ensure_open()?;
        let permit = Arc::clone(&self.shared.call_permits)
            .try_acquire_owned()
            .map_err(|_| channel_resource_error("open multiplexed object RPC call", 1))?;
        let (request, outgoing) = prepare_channel_call(call)?;
        validate_request_head(&request)?;
        let descriptor = outgoing
            .as_ref()
            .map(|outgoing| outgoing.descriptor.clone());
        let outgoing_value_permit = match descriptor.as_ref() {
            Some(descriptor) => self
                .shared
                .try_reserve_materialized_value(descriptor)?
                .map(|(_, permit)| permit),
            None => None,
        };
        if let Some(trace) = timeline {
            record_network_timeline_event_now(NetworkTimelineEvent::new(
                trace,
                network_timeline_stage::CALL_PREPARE,
                network_timeline_flag::REQUEST,
                self.shared.connection_id,
                0,
                0,
                u32::try_from(request.body.len()).unwrap_or(u32::MAX),
                0,
                network_timeline_monotonic_ns().saturating_sub(call_started_ns),
            ));
        }
        // IDs and their Request frames must enter the FIFO control queue in
        // the same order. Without this short admission critical section, two
        // runtime workers can allocate ids 1 and 3, enqueue id 3 first, and
        // let its completion advance the retired high-water mark before the
        // peer has seen id 1. Preparation stays outside the lock and no await
        // or body production occurs while it is held.
        let admission_started_ns = timeline
            .map(|_| network_timeline_monotonic_ns())
            .unwrap_or(0);
        let request_admission = self
            .shared
            .request_admission
            .lock()
            .map_err(|_| channel_lock_error())?;
        let call_id = self.shared.next_local_call_id()?;
        let (result_sender, result_receiver) = oneshot::channel();
        let lifetime = ChannelCallLifetime::new(
            permit,
            Arc::downgrade(&self.shared),
            ChannelCallSide::Client,
            call_id,
            timeline,
        );
        let progress = lifetime.progress();
        let request_window = outgoing.as_ref().map(|_| Arc::new(SendWindow::new(0)));
        {
            let mut pending = self
                .shared
                .pending
                .lock()
                .map_err(|_| channel_lock_error())?;
            pending.insert(
                call_id,
                PendingCallState {
                    result_sender: Some(result_sender),
                    request_window: request_window.as_ref().map(Arc::clone),
                    response_ingress: None,
                    lifetime: Arc::clone(&lifetime),
                },
            );
        }
        if let Err(error) = self.shared.ensure_open() {
            self.shared.abort_client(call_id, error.to_string(), false);
            return Err(error);
        }

        let frame = ObjectRpcChannelWireFrame::Request {
            call_id,
            request,
            body: descriptor,
        };
        let outgoing_task = if let Some(outgoing) = outgoing {
            let emitted = match self
                .shared
                .outbound
                .try_control_with_ack_timeline(frame, timeline)
            {
                Ok(emitted) => emitted,
                Err(error) => {
                    self.shared.abort_client(call_id, error.to_string(), false);
                    return Err(error);
                }
            };
            let request_window = request_window.expect("outgoing request body has a send window");
            Some((outgoing, emitted, request_window))
        } else {
            if let Err(error) = self
                .shared
                .outbound
                .try_bodyless_control(frame, Arc::clone(&lifetime))
            {
                self.shared.abort_client(call_id, error.to_string(), false);
                return Err(error);
            }
            None
        };
        drop(request_admission);
        if let Some(trace) = timeline {
            record_network_timeline_event_now(NetworkTimelineEvent::new(
                trace,
                network_timeline_stage::CALL_ADMISSION,
                network_timeline_flag::REQUEST,
                self.shared.connection_id,
                call_id.0,
                0,
                0,
                0,
                network_timeline_monotonic_ns().saturating_sub(admission_started_ns),
            ));
        }

        if let Some((outgoing, emitted, request_window)) = outgoing_task {
            let shared = Arc::clone(&self.shared);
            tokio::spawn(async move {
                if let Err(error) = run_outgoing_body(
                    Arc::clone(&shared),
                    call_id,
                    ObjectRpcChannelBodyDirection::Request,
                    outgoing.source,
                    request_window,
                    lifetime,
                    Some(emitted),
                    outgoing_value_permit,
                )
                .await
                {
                    shared.abort_client(call_id, error.to_string(), true);
                }
            });
        }

        Ok(ObjectRpcChannelPendingCall {
            call_id,
            shared: Arc::clone(&self.shared),
            result_receiver: Some(result_receiver),
            progress,
            completed: false,
            timeline,
            call_started_ns,
        })
    }

    pub fn queue_snapshot(&self) -> ObjectRpcChannelQueueSnapshot {
        self.shared.queue_snapshot()
    }
}

pub struct ObjectRpcChannelPendingCall {
    call_id: ObjectRpcChannelCallId,
    shared: Arc<ChannelShared>,
    result_receiver: Option<oneshot::Receiver<Result<ObjectRpcSessionResult, NetworkError>>>,
    progress: watch::Receiver<u64>,
    completed: bool,
    timeline: Option<NetworkTimelineTraceContext>,
    call_started_ns: u64,
}

impl ObjectRpcChannelPendingCall {
    pub const fn call_id(&self) -> ObjectRpcChannelCallId {
        self.call_id
    }

    /// Returns a call-local monotonic progress signal. The receiver owns no
    /// call permit or channel state, so retaining it cannot keep the call
    /// alive after the result future completes or is dropped.
    pub fn progress(&self) -> watch::Receiver<u64> {
        self.progress.clone()
    }

    pub async fn result(mut self) -> Result<ObjectRpcSessionResult, NetworkError> {
        let receiver = self
            .result_receiver
            .take()
            .ok_or_else(|| NetworkError::Io {
                operation: "receive multiplexed object RPC result".to_string(),
                detail: "result was already consumed".to_string(),
            })?;
        let result = receiver.await.map_err(|_| NetworkError::Io {
            operation: "receive multiplexed object RPC result".to_string(),
            detail: "channel closed before a response arrived".to_string(),
        })?;
        self.completed = true;
        if let Some(trace) = self.timeline {
            let success = result.is_ok();
            record_network_timeline_event_now(NetworkTimelineEvent::new(
                trace,
                network_timeline_stage::CALL_COMPLETE,
                network_timeline_flag::RESPONSE
                    | if success {
                        network_timeline_flag::SUCCESS
                    } else {
                        0
                    },
                self.shared.connection_id,
                self.call_id.0,
                0,
                0,
                0,
                network_timeline_monotonic_ns().saturating_sub(self.call_started_ns),
            ));
        }
        result
    }

    pub fn cancel(&mut self, reason: impl Into<String>) {
        if self.completed {
            return;
        }
        self.completed = true;
        self.shared.cancel_client(self.call_id, reason.into());
    }
}

impl Drop for ObjectRpcChannelPendingCall {
    fn drop(&mut self) {
        if !self.completed {
            self.shared
                .cancel_client(self.call_id, "pending call handle dropped".to_string());
        }
    }
}

pub struct ObjectRpcChannelServer {
    receiver: mpsc::Receiver<ObjectRpcChannelInboundRequest>,
    closed: watch::Receiver<bool>,
}

impl ObjectRpcChannelServer {
    pub async fn accept(&mut self) -> Option<ObjectRpcChannelInboundRequest> {
        if *self.closed.borrow() {
            return None;
        }
        let request = tokio::select! {
            biased;
            changed = self.closed.changed() => {
                let _ = changed;
                None
            }
            request = self.receiver.recv() => request,
        };
        if let Some(request) = request.as_ref() {
            request.record_server_dequeue();
        }
        request
    }
}

pub struct ObjectRpcChannelInboundRequest {
    pub request: ObjectRpcSessionRequest,
    responder: ObjectRpcChannelResponder,
    timeline: Option<NetworkTimelineTraceContext>,
    timeline_queued_ns: u64,
    connection_id: u64,
    call_id: ObjectRpcChannelCallId,
}

impl ObjectRpcChannelInboundRequest {
    fn record_server_dequeue(&self) {
        let Some(trace) = self.timeline else {
            return;
        };
        record_network_timeline_event_now(NetworkTimelineEvent::new(
            trace,
            network_timeline_stage::SERVER_QUEUE_WAIT,
            network_timeline_flag::REQUEST,
            self.connection_id,
            self.call_id.0,
            0,
            0,
            0,
            network_timeline_monotonic_ns().saturating_sub(self.timeline_queued_ns),
        ));
    }

    pub fn responder(&self) -> ObjectRpcChannelResponder {
        self.responder.clone()
    }

    pub const fn call_id(&self) -> ObjectRpcChannelCallId {
        self.call_id
    }

    pub const fn connection_id(&self) -> u64 {
        self.connection_id
    }

    pub const fn timeline_context(&self) -> Option<NetworkTimelineTraceContext> {
        self.timeline
    }

    pub async fn respond(&self, response: ObjectRpcSessionResponse) -> Result<(), NetworkError> {
        self.responder.respond(response).await
    }

    pub fn cancellation(&self) -> watch::Receiver<Option<String>> {
        self.responder.cancellation.clone()
    }

    pub fn reset(&self, reason: impl Into<String>) {
        self.responder.reset(reason);
    }
}

#[derive(Clone)]
pub struct ObjectRpcChannelResponder {
    shared: Arc<ChannelShared>,
    call_id: ObjectRpcChannelCallId,
    cancellation: watch::Receiver<Option<String>>,
}

impl ObjectRpcChannelResponder {
    /// Attempts exactly one terminal response for this call. Preparation,
    /// validation, and resource-admission failures reset the call; callers
    /// cannot retry with a different response after an error.
    pub async fn respond(&self, response: ObjectRpcSessionResponse) -> Result<(), NetworkError> {
        self.shared
            .respond(self.call_id, response)
            .await
            .map(|_| ())
    }

    /// Queues the response and waits until its complete logical body has been
    /// written and flushed by the physical carrier. Daemon stop/restart paths
    /// use this before retiring the authenticated connection.
    pub async fn respond_and_wait_written(
        &self,
        response: ObjectRpcSessionResponse,
    ) -> Result<(), NetworkError> {
        let lifetime = self.shared.respond(self.call_id, response).await?;
        lifetime.wait_outgoing_done().await
    }

    pub fn cancellation(&self) -> watch::Receiver<Option<String>> {
        self.cancellation.clone()
    }

    pub fn reset(&self, reason: impl Into<String>) {
        self.shared.reset_inbound(self.call_id, reason.into(), true);
    }
}

#[derive(Clone)]
pub struct ObjectRpcChannelIngress {
    shared: Arc<ChannelShared>,
}

impl ObjectRpcChannelIngress {
    /// Ingests one already-authenticated wire frame. This method never awaits
    /// an application request or body queue.
    pub fn receive(&self, frame: ObjectRpcChannelWireFrame) -> Result<(), NetworkError> {
        let call_id = frame.call_id();
        self.shared.ensure_open()?;
        self.shared.receive(frame)?;
        if let Err(error) = self.shared.ensure_open() {
            if self.shared.role.owns_call_id(call_id) {
                self.shared.abort_client(call_id, error.to_string(), false);
            } else {
                self.shared.reset_inbound(call_id, error.to_string(), false);
            }
            return Err(error);
        }
        Ok(())
    }

    /// Ingests an SOBJ control frame. Raw Data is deliberately rejected at
    /// this boundary so current carriers cannot accidentally serialize body
    /// payloads through the control codec.
    #[doc(hidden)]
    pub fn receive_control(&self, frame: ObjectRpcChannelWireFrame) -> Result<(), NetworkError> {
        if matches!(frame, ObjectRpcChannelWireFrame::Data { .. }) {
            return Err(NetworkError::Serialization {
                detail: "multiplexed Data must use the fixed channel wire header".to_string(),
            });
        }
        self.receive(frame)
    }

    /// Validates call state, ordering, declared length, and receiver credit
    /// from a fixed Data header before the carrier acquires payload storage.
    #[doc(hidden)]
    pub fn validate_data_header(
        &self,
        header: &ObjectRpcChannelDataHeader,
    ) -> Result<(), NetworkError> {
        self.shared.ensure_open()?;
        let result = self
            .shared
            .body_ingress(header.call_id, header.direction)
            .and_then(|ingress| ingress.validate_data(header.offset, header.payload_len as usize));
        match result {
            Err(_)
                if !self.shared.is_active_call(header.call_id)?
                    && self.shared.is_retired_call(header.call_id) =>
            {
                Ok(())
            }
            result => result,
        }
    }

    /// Revalidates and moves an ownership-bearing raw Data payload into the
    /// logical body queue. Cancellation may race the payload read, which is
    /// why this repeats the header-state validation performed before acquire.
    #[doc(hidden)]
    pub fn receive_data(&self, frame: ObjectRpcChannelDataFrame) -> Result<(), NetworkError> {
        if frame.bytes.len() != frame.header.payload_len as usize {
            return Err(NetworkError::Serialization {
                detail: "fixed channel Data payload length mismatch".to_string(),
            });
        }
        let call_id = frame.header.call_id;
        self.shared.ensure_open()?;
        self.shared.receive_data(frame)?;
        if let Err(error) = self.shared.ensure_open() {
            if self.shared.role.owns_call_id(call_id) {
                self.shared.abort_client(call_id, error.to_string(), false);
            } else {
                self.shared.reset_inbound(call_id, error.to_string(), false);
            }
            return Err(error);
        }
        Ok(())
    }

    /// Records the physical receiver boundary for any decoded frame belonging
    /// to a sampled call, including streamed data and flow-control frames.
    /// Carriers call this immediately after object-frame decode and before
    /// ordinary ingress processing. Timeline-disabled reads pass `None` and
    /// return without a lock or allocation.
    pub fn record_frame_read_timeline(
        &self,
        frame: &ObjectRpcChannelWireFrame,
        timing: Option<ObjectFrameReadTiming>,
        carrier_flag: u16,
    ) {
        let Some(timing) = timing else {
            return;
        };
        if timing.capture_generation != 0
            && crate::timeline::network_timeline_capture_generation() != timing.capture_generation
        {
            return;
        }
        let Some(trace) = self.shared.frame_timeline(frame) else {
            return;
        };
        let flags =
            channel_frame_timeline_flags(frame) | carrier_flag | network_timeline_flag::SUCCESS;
        let call_id = frame.call_id();
        let frame_id = channel_frame_timeline_id(frame);
        if timing.ready_monotonic_ns != 0
            && timing.read_poll_monotonic_ns >= timing.ready_monotonic_ns
        {
            record_channel_timeline_event_at(
                trace,
                network_timeline_stage::FRAME_READY_TO_POLL,
                flags,
                self.shared.connection_id,
                call_id,
                frame_id,
                timing.wire_bytes,
                timing.first_read_bytes,
                timing.read_poll_monotonic_ns,
                timing
                    .read_poll_monotonic_ns
                    .saturating_sub(timing.ready_monotonic_ns),
            );
        }
        record_channel_timeline_event_at(
            trace,
            network_timeline_stage::FRAME_FIRST_READ,
            flags,
            self.shared.connection_id,
            call_id,
            frame_id,
            timing.wire_bytes,
            timing.first_read_bytes,
            timing.first_read_monotonic_ns,
            0,
        );
        record_channel_timeline_event_at(
            trace,
            network_timeline_stage::FRAME_READ,
            flags,
            self.shared.connection_id,
            call_id,
            frame_id,
            timing.wire_bytes,
            timing.first_read_bytes,
            timing.body_read_completed_monotonic_ns,
            timing
                .body_read_completed_monotonic_ns
                .saturating_sub(timing.first_read_monotonic_ns),
        );
        record_channel_timeline_event_at(
            trace,
            network_timeline_stage::FRAME_DECODE,
            flags,
            self.shared.connection_id,
            call_id,
            frame_id,
            timing.wire_bytes,
            timing.first_read_bytes,
            timing.decoded_monotonic_ns,
            timing
                .decoded_monotonic_ns
                .saturating_sub(timing.decode_started_monotonic_ns),
        );
    }

    /// Idempotently closes the logical channel after its physical carrier
    /// stops. Pending clients fail, inbound handlers are cancelled, the server
    /// accept loop ends, and later calls or frames are rejected.
    pub fn close(&self, detail: impl Into<String>) {
        self.shared.close(detail.into());
    }

    pub fn queue_snapshot(&self) -> ObjectRpcChannelQueueSnapshot {
        self.shared.queue_snapshot()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct DataLaneKey {
    call_id: ObjectRpcChannelCallId,
    direction: ObjectRpcChannelBodyDirection,
}

pub struct ObjectRpcChannelEgress {
    shared: Arc<ChannelShared>,
    control_receiver: mpsc::Receiver<QueuedControl>,
    data_receiver: mpsc::Receiver<QueuedData>,
    data_lanes: HashMap<DataLaneKey, VecDeque<QueuedData>>,
    ready_data_lanes: VecDeque<DataLaneKey>,
    control_burst: usize,
    closed: watch::Receiver<bool>,
    pending_write: Option<PendingChannelWrite>,
}

impl ObjectRpcChannelEgress {
    /// Returns the next logical frame with bounded control priority. At most
    /// eight controls bypass an already-queued data frame. This convenience
    /// method treats the returned frame as written immediately; physical
    /// transports must use [`Self::next_transport_frame`] and report the
    /// actual socket-write result with [`Self::complete_frame_write`].
    pub async fn next_frame(&mut self) -> Option<ObjectRpcChannelWireFrame> {
        let frame = self.next_frame_for_transport().await?;
        self.complete_frame_write(true);
        Some(frame)
    }

    /// Compatibility adapter for low-level carriers using the legacy logical
    /// frame enum. Current physical carriers use [`Self::next_transport_frame`]
    /// so raw Data retains its `Bytes` allocation.
    pub async fn next_frame_for_transport(&mut self) -> Option<ObjectRpcChannelWireFrame> {
        let frame = self.next_transport_frame().await?;
        Some(match frame {
            ObjectRpcChannelTransportFrame::Control(frame) => frame,
            ObjectRpcChannelTransportFrame::Data(frame) => ObjectRpcChannelWireFrame::Data {
                call_id: frame.header.call_id,
                direction: frame.header.direction,
                offset: frame.header.offset,
                bytes: frame.bytes.to_vec(),
            },
        })
    }

    /// Dequeues one frame while retaining its call completion and byte-budget
    /// accounting until the physical transport reports whether the encoded
    /// frame was written and flushed.
    #[doc(hidden)]
    pub async fn next_transport_frame(&mut self) -> Option<ObjectRpcChannelTransportFrame> {
        if self.pending_write.take().is_some() {
            self.shared.close(
                "physical transport requested another frame before completing the prior write"
                    .to_string(),
            );
            return None;
        }
        loop {
            if *self.closed.borrow() {
                return None;
            }
            self.drain_data_receiver();
            if self.control_burst >= CONTROL_BURST_BEFORE_DATA
                && let Some(data) = self.pop_data_lane()
            {
                self.control_burst = 0;
                return Some(self.begin_write(PendingChannelWrite::Data(data)));
            }
            if self.control_burst < CONTROL_BURST_BEFORE_DATA
                && let Ok(control) = self.control_receiver.try_recv()
            {
                self.control_burst += 1;
                return Some(self.begin_write(PendingChannelWrite::Control(control)));
            }
            if let Some(data) = self.pop_data_lane() {
                self.control_burst = 0;
                return Some(self.begin_write(PendingChannelWrite::Data(data)));
            }
            if let Ok(control) = self.control_receiver.try_recv() {
                self.control_burst += 1;
                return Some(self.begin_write(PendingChannelWrite::Control(control)));
            }

            let pending = if self.control_burst >= CONTROL_BURST_BEFORE_DATA {
                tokio::select! {
                    biased;
                    changed = self.closed.changed() => {
                        let _ = changed;
                        return None;
                    }
                    data = self.data_receiver.recv() => {
                        let data = data?;
                        self.push_data_lane(data);
                        self.control_burst = 0;
                        self.pop_data_lane().map(PendingChannelWrite::Data)
                    }
                    control = self.control_receiver.recv() => {
                        let control = control?;
                        self.control_burst += 1;
                        Some(PendingChannelWrite::Control(control))
                    }
                }
            } else {
                tokio::select! {
                    biased;
                    changed = self.closed.changed() => {
                        let _ = changed;
                        return None;
                    }
                    control = self.control_receiver.recv() => {
                        let control = control?;
                        self.control_burst += 1;
                        Some(PendingChannelWrite::Control(control))
                    }
                    data = self.data_receiver.recv() => {
                        let data = data?;
                        self.push_data_lane(data);
                        self.control_burst = 0;
                        self.pop_data_lane().map(PendingChannelWrite::Data)
                    }
                }
            };
            if let Some(pending) = pending {
                return Some(self.begin_write(pending));
            }
            // A cancelled lane may be pruned after the receiver wakes. Keep
            // waiting; an empty pruned lane is not physical-channel EOF.
        }
    }

    /// Completes the frame most recently returned by
    /// [`Self::next_transport_frame`]. A failed write drops acknowledgements
    /// and cancels bodyless call lifetime state; a successful write advances
    /// progress and releases the retained queue budget.
    pub fn complete_frame_write(&mut self, written: bool) {
        let Some(mut pending) = self.pending_write.take() else {
            return;
        };
        if written {
            pending.mark_written();
        }
    }

    /// Returns correlation for the frame currently owned by the physical
    /// carrier. It is available after `next_transport_frame` and before
    /// `complete_frame_write`.
    pub fn pending_write_timeline(&self) -> Option<ObjectRpcChannelWriteTimeline> {
        self.pending_write
            .as_ref()
            .and_then(PendingChannelWrite::write_timeline)
    }

    fn begin_write(&mut self, mut pending: PendingChannelWrite) -> ObjectRpcChannelTransportFrame {
        let frame = pending.take_frame();
        debug_assert!(self.pending_write.is_none());
        self.pending_write = Some(pending);
        frame
    }

    fn drain_data_receiver(&mut self) {
        while let Ok(data) = self.data_receiver.try_recv() {
            self.push_data_lane(data);
        }
    }

    fn push_data_lane(&mut self, data: QueuedData) {
        if data.prune_if_inactive
            && !self
                .shared
                .is_active_call(data.lane.call_id)
                .unwrap_or(false)
        {
            return;
        }
        let lane = data.lane;
        match self.data_lanes.get_mut(&lane) {
            Some(queue) => queue.push_back(data),
            None => {
                self.data_lanes.insert(lane, VecDeque::from([data]));
                self.ready_data_lanes.push_back(lane);
            }
        }
    }

    fn pop_data_lane(&mut self) -> Option<QueuedData> {
        while let Some(lane) = self.ready_data_lanes.pop_front() {
            let lane_inactive = !self.shared.is_active_call(lane.call_id).unwrap_or(false);
            let Some(queue) = self.data_lanes.get_mut(&lane) else {
                continue;
            };
            while queue
                .front()
                .is_some_and(|data| data.prune_if_inactive && lane_inactive)
            {
                queue.pop_front();
            }
            let data = queue.pop_front();
            if queue.is_empty() {
                self.data_lanes.remove(&lane);
            } else {
                self.ready_data_lanes.push_back(lane);
            }
            if data.is_some() {
                return data;
            }
        }
        None
    }

    pub fn queue_snapshot(&self) -> ObjectRpcChannelQueueSnapshot {
        self.shared.queue_snapshot()
    }
}

struct ChannelShared {
    connection_id: u64,
    role: ObjectRpcChannelRole,
    next_call_id: AtomicU64,
    /// Serializes only id assignment through Request control-queue admission.
    /// The physical carrier is ordered, so this preserves the invariant used
    /// by the bounded retired-call high-water marks without serializing call
    /// execution or body streaming.
    request_admission: Mutex<()>,
    call_permits: Arc<Semaphore>,
    max_calls: usize,
    outbound: Arc<OutboundQueues>,
    inbound_sender: mpsc::Sender<ObjectRpcChannelInboundRequest>,
    materialized_value_budget: Arc<Semaphore>,
    pending: Mutex<HashMap<ObjectRpcChannelCallId, PendingCallState>>,
    inbound: Mutex<HashMap<ObjectRpcChannelCallId, InboundCallState>>,
    tombstones: Mutex<CallTombstones>,
    retired_local_high_watermark: AtomicU64,
    retired_peer_high_watermark: AtomicU64,
    /// Healthy calls are overwhelmingly more common than closure. Keep the
    /// error detail behind the mutex, but avoid taking that mutex at every
    /// request/ingress boundary until closure has actually begun.
    closed_fast: AtomicBool,
    closed: Mutex<Option<String>>,
    closed_signal: watch::Sender<bool>,
}

#[derive(Default)]
struct CallTombstones {
    order: VecDeque<ObjectRpcChannelCallId>,
    members: HashSet<ObjectRpcChannelCallId>,
}

impl CallTombstones {
    fn insert(&mut self, call_id: ObjectRpcChannelCallId) {
        if !self.members.insert(call_id) {
            return;
        }
        self.order.push_back(call_id);
        while self.order.len() > MAX_COMPLETED_CALL_TOMBSTONES {
            if let Some(expired) = self.order.pop_front() {
                self.members.remove(&expired);
            }
        }
    }

    fn contains(&self, call_id: ObjectRpcChannelCallId) -> bool {
        self.members.contains(&call_id)
    }
}

impl ChannelShared {
    fn is_closed(&self) -> bool {
        self.closed_fast.load(Ordering::Acquire) || self.closed.is_poisoned()
    }

    fn ensure_open(&self) -> Result<(), NetworkError> {
        if !self.closed_fast.load(Ordering::Acquire) && !self.closed.is_poisoned() {
            return Ok(());
        }
        let detail = self.closed.lock().map_err(|_| channel_lock_error())?;
        match detail.as_deref() {
            Some(detail) => Err(channel_delivery_error(&format!(
                "physical channel is closed: {detail}"
            ))),
            None => Ok(()),
        }
    }

    fn close_detail(&self) -> Option<String> {
        if !self.closed_fast.load(Ordering::Acquire) && !self.closed.is_poisoned() {
            return None;
        }
        self.closed
            .lock()
            .ok()
            .and_then(|detail| detail.as_ref().cloned())
    }

    fn frame_timeline(
        &self,
        frame: &ObjectRpcChannelWireFrame,
    ) -> Option<NetworkTimelineTraceContext> {
        match frame {
            ObjectRpcChannelWireFrame::Request { request, .. } => {
                network_timeline_trace_context_for_request(request)
            }
            _ => self.call_timeline(frame.call_id()),
        }
    }

    fn call_timeline(
        &self,
        call_id: ObjectRpcChannelCallId,
    ) -> Option<NetworkTimelineTraceContext> {
        if self.role.owns_call_id(call_id) {
            self.pending
                .lock()
                .ok()?
                .get(&call_id)
                .and_then(|pending| pending.lifetime.timeline)
        } else {
            self.inbound
                .lock()
                .ok()?
                .get(&call_id)
                .and_then(|inbound| inbound.lifetime.timeline)
        }
    }

    fn close(&self, detail: String) {
        let should_close = self
            .closed
            .lock()
            .map(|mut closed| {
                if closed.is_some() {
                    false
                } else {
                    *closed = Some(detail.clone());
                    // Publish closure while the detail lock is still held. An
                    // observer that sees the flag will then either read the
                    // complete detail or wait for this critical section; it
                    // can never slip through the healthy fast path after the
                    // close state has been committed.
                    self.closed_fast.store(true, Ordering::Release);
                    true
                }
            })
            .unwrap_or(false);
        if !should_close {
            return;
        }
        // A physical close is rare and must remain visible even when it does
        // not coincide with a sampled request. The zero trace/span IDs mark a
        // process-scoped lifecycle record; connection_id joins it to the
        // preceding request/frame spans. Queue counts make resource-driven
        // retirement distinguishable without retaining an unbounded log.
        let queue = self.queue_snapshot();
        record_network_timeline_event_now(NetworkTimelineEvent::new(
            NetworkTimelineTraceContext::default(),
            network_timeline_stage::CHANNEL_CLOSE,
            0,
            self.connection_id,
            0,
            0,
            u32::try_from(queue.queued_bytes).unwrap_or(u32::MAX),
            u32::try_from(queue.active_calls).unwrap_or(u32::MAX),
            0,
        ));
        self.closed_signal.send_replace(true);

        let pending = self
            .pending
            .lock()
            .map(|mut pending| std::mem::take(&mut *pending))
            .unwrap_or_default();
        for mut state in pending.into_values() {
            if let Some(window) = state.request_window.as_ref() {
                window.cancel();
            }
            if let Some(ingress) = state.response_ingress.take() {
                ingress.cancel();
            }
            if let Some(sender) = state.result_sender.take() {
                let _ = sender.send(Err(channel_delivery_error(&detail)));
            }
            state.lifetime.cancel();
        }

        let inbound = self
            .inbound
            .lock()
            .map(|mut inbound| std::mem::take(&mut *inbound))
            .unwrap_or_default();
        for mut state in inbound.into_values() {
            if let Some(window) = state.response_window.as_ref() {
                window.cancel();
            }
            if let Some(ingress) = state.request_ingress.take() {
                ingress.cancel();
            }
            let _ = state.cancellation_sender.send(Some(detail.clone()));
            state.lifetime.cancel();
        }
    }

    fn next_local_call_id(&self) -> Result<ObjectRpcChannelCallId, NetworkError> {
        let value = self.next_call_id.fetch_add(2, Ordering::Relaxed);
        if value == 0 || value > u64::MAX - 2 {
            return Err(NetworkError::Serialization {
                detail: "multiplexed object RPC call id overflow".to_string(),
            });
        }
        Ok(ObjectRpcChannelCallId(value))
    }

    fn queue_snapshot(&self) -> ObjectRpcChannelQueueSnapshot {
        ObjectRpcChannelQueueSnapshot {
            active_calls: self
                .max_calls
                .saturating_sub(self.call_permits.available_permits()),
            queued_control_frames: self.outbound.queued_control_frames.load(Ordering::Relaxed),
            queued_bytes: self.outbound.queued_bytes.load(Ordering::Relaxed),
            queued_data_bytes: self.outbound.queued_data_bytes.load(Ordering::Relaxed),
        }
    }

    fn record_tombstone(&self, call_id: ObjectRpcChannelCallId) {
        let high_watermark = if self.role.owns_call_id(call_id) {
            &self.retired_local_high_watermark
        } else {
            &self.retired_peer_high_watermark
        };
        high_watermark.fetch_max(call_id.0, Ordering::Release);
        if let Ok(mut tombstones) = self.tombstones.lock() {
            tombstones.insert(call_id);
        }
    }

    fn is_retired_call(&self, call_id: ObjectRpcChannelCallId) -> bool {
        let high_watermark = if self.role.owns_call_id(call_id) {
            self.retired_local_high_watermark.load(Ordering::Acquire)
        } else {
            self.retired_peer_high_watermark.load(Ordering::Acquire)
        };
        call_id.0 <= high_watermark
            || self
                .tombstones
                .lock()
                .map(|tombstones| tombstones.contains(call_id))
                .unwrap_or(false)
    }

    fn is_active_call(&self, call_id: ObjectRpcChannelCallId) -> Result<bool, NetworkError> {
        if self.role.owns_call_id(call_id) {
            self.pending
                .lock()
                .map(|pending| pending.contains_key(&call_id))
                .map_err(|_| channel_lock_error())
        } else {
            self.inbound
                .lock()
                .map(|inbound| inbound.contains_key(&call_id))
                .map_err(|_| channel_lock_error())
        }
    }

    fn has_active_inbound_call(
        &self,
        call_id: ObjectRpcChannelCallId,
    ) -> Result<bool, NetworkError> {
        self.inbound
            .lock()
            .map(|inbound| inbound.contains_key(&call_id))
            .map_err(|_| channel_lock_error())
    }

    fn try_reserve_materialized_value(
        &self,
        descriptor: &ObjectRpcChannelBodyDescriptor,
    ) -> Result<Option<(usize, OwnedSemaphorePermit)>, NetworkError> {
        if descriptor.kind != ObjectRpcChannelBodyKind::Value {
            return Ok(None);
        }
        let declared = descriptor
            .total_bytes
            .and_then(|bytes| usize::try_from(bytes).ok())
            .ok_or_else(|| {
                channel_resource_error("reserve multiplexed materialized value", usize::MAX)
            })?;
        let permits = u32::try_from(declared).map_err(|_| {
            channel_resource_error("reserve multiplexed materialized value", declared)
        })?;
        let permit = Arc::clone(&self.materialized_value_budget)
            .try_acquire_many_owned(permits)
            .map_err(|_| {
                channel_resource_error("reserve multiplexed materialized value", declared)
            })?;
        Ok(Some((declared, permit)))
    }

    fn reject_incoming_call(
        &self,
        call_id: ObjectRpcChannelCallId,
        reason: String,
        timeline: Option<NetworkTimelineTraceContext>,
    ) -> Result<(), NetworkError> {
        self.record_tombstone(call_id);
        self.outbound.try_control_with_timeline(
            ObjectRpcChannelWireFrame::Reset { call_id, reason },
            timeline,
        )
    }

    fn remove_call(&self, side: ChannelCallSide, call_id: ObjectRpcChannelCallId) {
        self.record_tombstone(call_id);
        match side {
            ChannelCallSide::Client => {
                if let Ok(mut pending) = self.pending.lock() {
                    pending.remove(&call_id);
                }
            }
            ChannelCallSide::Server => {
                if let Ok(mut inbound) = self.inbound.lock() {
                    inbound.remove(&call_id);
                }
            }
        }
    }

    fn receive(self: &Arc<Self>, frame: ObjectRpcChannelWireFrame) -> Result<(), NetworkError> {
        validate_wire_frame(&frame)?;
        let call_id = frame.call_id();
        let result = (|| match frame {
            ObjectRpcChannelWireFrame::Request {
                call_id,
                request,
                body,
            } => {
                if self.has_active_inbound_call(call_id)? {
                    Err(NetworkError::Serialization {
                        detail: format!("duplicate multiplexed object RPC call id {call_id}"),
                    })
                } else if self.is_retired_call(call_id) {
                    Ok(())
                } else {
                    self.receive_request(call_id, request, body)
                }
            }
            ObjectRpcChannelWireFrame::RequestAccepted {
                call_id,
                credit_bytes,
            } => {
                let (window, lifetime) = self
                    .pending
                    .lock()
                    .map_err(|_| channel_lock_error())?
                    .get(&call_id)
                    .map(|pending| {
                        (
                            pending.request_window.as_ref().map(Arc::clone),
                            Arc::clone(&pending.lifetime),
                        )
                    })
                    .ok_or_else(|| unknown_call_error(call_id))?;
                let window = window.ok_or_else(|| NetworkError::Serialization {
                    detail: "bodyless multiplexed request received body credit".to_string(),
                })?;
                window.open(credit_bytes as usize)?;
                lifetime.mark_progress();
                Ok(())
            }
            ObjectRpcChannelWireFrame::Response {
                call_id,
                response,
                body,
                initial_credit_bytes,
            } => self.receive_response(call_id, response, body, initial_credit_bytes),
            ObjectRpcChannelWireFrame::Data {
                call_id,
                direction,
                offset,
                bytes,
            } => {
                let ingress = self.body_ingress(call_id, direction)?;
                ingress.accept_data(offset, Bytes::from(bytes))
            }
            ObjectRpcChannelWireFrame::WindowUpdate {
                call_id,
                direction,
                received_offset,
            } => {
                let (window, lifetime) = self.send_window(call_id, direction)?;
                window.acknowledge(received_offset)?;
                lifetime.mark_progress();
                Ok(())
            }
            ObjectRpcChannelWireFrame::BodyEnd {
                call_id,
                direction,
                total_bytes,
            } => self
                .body_ingress(call_id, direction)?
                .accept_end(total_bytes),
            ObjectRpcChannelWireFrame::Cancel { call_id, reason } => {
                if self.role.owns_call_id(call_id)
                    || !self.try_reset_inbound(call_id, reason, false)
                {
                    Err(unknown_call_error(call_id))
                } else {
                    Ok(())
                }
            }
            ObjectRpcChannelWireFrame::Reset { call_id, reason } => {
                let removed = if self.role.owns_call_id(call_id) {
                    self.try_abort_client(call_id, reason, false)
                } else {
                    self.try_reset_inbound(call_id, reason, false)
                };
                if removed {
                    Ok(())
                } else {
                    Err(unknown_call_error(call_id))
                }
            }
        })();
        match result {
            Err(_) if !self.is_active_call(call_id)? && self.is_retired_call(call_id) => Ok(()),
            result => result,
        }
    }

    fn receive_data(
        self: &Arc<Self>,
        frame: ObjectRpcChannelDataFrame,
    ) -> Result<(), NetworkError> {
        frame.validate()?;
        let call_id = frame.header.call_id;
        let result = self
            .body_ingress(call_id, frame.header.direction)
            .and_then(|ingress| ingress.accept_data(frame.header.offset, frame.bytes));
        match result {
            Err(_) if !self.is_active_call(call_id)? && self.is_retired_call(call_id) => Ok(()),
            result => result,
        }
    }

    fn receive_request(
        self: &Arc<Self>,
        call_id: ObjectRpcChannelCallId,
        request: ObjectRpcRequest,
        body_descriptor: Option<ObjectRpcChannelBodyDescriptor>,
    ) -> Result<(), NetworkError> {
        let timeline = network_timeline_trace_context_for_request(&request);
        let ingress_started_ns = timeline
            .map(|_| network_timeline_monotonic_ns())
            .unwrap_or(0);
        let request_bytes = u32::try_from(request.body.len()).unwrap_or(u32::MAX);
        if self.role.owns_call_id(call_id) {
            return Err(NetworkError::Serialization {
                detail: format!("peer used local multiplexed call id {call_id}"),
            });
        }
        validate_request_head(&request)?;
        let materialized_value_reservation = if let Some(descriptor) = body_descriptor.as_ref() {
            validate_body_descriptor(descriptor)?;
            match self.try_reserve_materialized_value(descriptor) {
                Ok(reservation) => reservation,
                Err(error) => {
                    return self.reject_incoming_call(call_id, error.to_string(), timeline);
                }
            }
        } else {
            None
        };
        let permit = match Arc::clone(&self.call_permits).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                return self.reject_incoming_call(
                    call_id,
                    channel_resource_error("accept multiplexed object RPC call", 1).to_string(),
                    timeline,
                );
            }
        };
        let lifetime = ChannelCallLifetime::new(
            permit,
            Arc::downgrade(self),
            ChannelCallSide::Server,
            call_id,
            timeline,
        );
        let (cancellation_sender, cancellation) = watch::channel(None::<String>);
        let (request_ingress, incoming_body) = match body_descriptor.as_ref() {
            Some(descriptor) => {
                let (ingress, body) = start_body_ingress(
                    Arc::clone(self),
                    call_id,
                    ObjectRpcChannelBodyDirection::Request,
                    descriptor.clone(),
                    Arc::clone(&lifetime),
                );
                (Some(ingress), body)
            }
            None => (None, ObjectRpcIncomingBody::empty()),
        };

        {
            let mut inbound = self.inbound.lock().map_err(|_| channel_lock_error())?;
            if inbound.contains_key(&call_id) {
                return Err(NetworkError::Serialization {
                    detail: format!("duplicate multiplexed object RPC call id {call_id}"),
                });
            }
            inbound.insert(
                call_id,
                InboundCallState {
                    response_window: None,
                    request_ingress,
                    lifetime: Arc::clone(&lifetime),
                    cancellation_sender,
                    responded: false,
                },
            );
        }

        let responder = ObjectRpcChannelResponder {
            shared: Arc::clone(self),
            call_id,
            cancellation,
        };
        if let Some(descriptor) = body_descriptor {
            if let Err(error) = self.outbound.try_control_with_timeline(
                ObjectRpcChannelWireFrame::RequestAccepted {
                    call_id,
                    credit_bytes: OBJECT_RPC_CHANNEL_CALL_WINDOW_BYTES as u32,
                },
                timeline,
            ) {
                self.reset_inbound(call_id, error.to_string(), false);
                return Err(error);
            }
            let incoming = ObjectRpcSessionRequest::new(request, incoming_body);
            match descriptor.kind {
                ObjectRpcChannelBodyKind::Attachment => {
                    if let Err(error) = self.try_deliver_inbound(ObjectRpcChannelInboundRequest {
                        request: incoming,
                        responder,
                        timeline,
                        timeline_queued_ns: timeline
                            .map(|_| network_timeline_monotonic_ns())
                            .unwrap_or(0),
                        connection_id: self.connection_id,
                        call_id,
                    }) {
                        self.reset_inbound(call_id, error.to_string(), true);
                        return Err(error);
                    }
                }
                ObjectRpcChannelBodyKind::Value => {
                    let (declared_bytes, materialized_value_permit) =
                        materialized_value_reservation
                            .expect("value descriptor reserves materialization bytes");
                    let sender = self.inbound_sender.clone();
                    let shared = Arc::clone(self);
                    tokio::spawn(async move {
                        let mut incoming = incoming;
                        let result = read_materialized_value_exact(
                            &mut incoming.body,
                            declared_bytes,
                            "materialize multiplexed object RPC request value",
                        )
                        .await;
                        match result {
                            Ok(value) => {
                                incoming.request.body = value;
                                incoming.body = ObjectRpcIncomingBody::empty();
                                if sender
                                    .send(ObjectRpcChannelInboundRequest {
                                        request: incoming,
                                        responder,
                                        timeline,
                                        timeline_queued_ns: timeline
                                            .map(|_| network_timeline_monotonic_ns())
                                            .unwrap_or(0),
                                        connection_id: shared.connection_id,
                                        call_id,
                                    })
                                    .await
                                    .is_err()
                                {
                                    shared.reset_inbound(
                                        call_id,
                                        "multiplexed object RPC server closed".to_string(),
                                        true,
                                    );
                                }
                            }
                            Err(error) => shared.reset_inbound(call_id, error.to_string(), true),
                        }
                        drop(materialized_value_permit);
                    });
                }
            }
        } else {
            if let Err(error) = self.try_deliver_inbound(ObjectRpcChannelInboundRequest {
                request: ObjectRpcSessionRequest::value(request),
                responder,
                timeline,
                timeline_queued_ns: timeline
                    .map(|_| network_timeline_monotonic_ns())
                    .unwrap_or(0),
                connection_id: self.connection_id,
                call_id,
            }) {
                self.reset_inbound(call_id, error.to_string(), true);
                return Err(error);
            }
            lifetime.mark_incoming_done();
        }
        if let Some(trace) = timeline {
            record_network_timeline_event_now(NetworkTimelineEvent::new(
                trace,
                network_timeline_stage::FRAME_INGRESS,
                network_timeline_flag::REQUEST | network_timeline_flag::SUCCESS,
                self.connection_id,
                call_id.0,
                0,
                request_bytes,
                0,
                network_timeline_monotonic_ns().saturating_sub(ingress_started_ns),
            ));
        }
        Ok(())
    }

    fn try_deliver_inbound(
        &self,
        request: ObjectRpcChannelInboundRequest,
    ) -> Result<(), NetworkError> {
        self.inbound_sender.try_send(request).map_err(|error| {
            channel_resource_error(
                "queue multiplexed object RPC server request",
                match error {
                    mpsc::error::TrySendError::Full(_) => 1,
                    mpsc::error::TrySendError::Closed(_) => 0,
                },
            )
        })
    }

    fn receive_response(
        self: &Arc<Self>,
        call_id: ObjectRpcChannelCallId,
        response: ObjectRpcResponse,
        body_descriptor: Option<ObjectRpcChannelBodyDescriptor>,
        initial_credit_bytes: u32,
    ) -> Result<(), NetworkError> {
        let response_bytes = u32::try_from(response.body.len()).unwrap_or(u32::MAX);
        if !self.role.owns_call_id(call_id) {
            return Err(unknown_call_error(call_id));
        }
        validate_response_head(&response)?;
        let materialized_value_reservation = if let Some(descriptor) = body_descriptor.as_ref() {
            validate_body_descriptor(descriptor)?;
            if initial_credit_bytes as usize != OBJECT_RPC_CHANNEL_CALL_WINDOW_BYTES {
                return Err(NetworkError::Serialization {
                    detail: "invalid multiplexed response initial credit".to_string(),
                });
            }
            match self.try_reserve_materialized_value(descriptor) {
                Ok(reservation) => reservation,
                Err(error) => {
                    if self.try_abort_client(call_id, error.to_string(), true) {
                        return Ok(());
                    }
                    return Err(unknown_call_error(call_id));
                }
            }
        } else if initial_credit_bytes != 0 {
            return Err(NetworkError::Serialization {
                detail: "bodyless multiplexed response granted body credit".to_string(),
            });
        } else {
            None
        };
        let (result_sender, lifetime) = {
            let mut pending = self.pending.lock().map_err(|_| channel_lock_error())?;
            let pending = pending
                .get_mut(&call_id)
                .ok_or_else(|| unknown_call_error(call_id))?;
            (
                pending
                    .result_sender
                    .take()
                    .ok_or_else(|| NetworkError::Serialization {
                        detail: format!("duplicate response for multiplexed call {call_id}"),
                    })?,
                Arc::clone(&pending.lifetime),
            )
        };
        let timeline = lifetime.timeline;
        let resolve_started_ns = timeline
            .map(|_| network_timeline_monotonic_ns())
            .unwrap_or(0);
        lifetime.mark_progress();

        if let Some(descriptor) = body_descriptor {
            let (ingress, incoming_body) = start_body_ingress(
                Arc::clone(self),
                call_id,
                ObjectRpcChannelBodyDirection::Response,
                descriptor.clone(),
                lifetime,
            );
            self.pending
                .lock()
                .map_err(|_| channel_lock_error())?
                .get_mut(&call_id)
                .ok_or_else(|| unknown_call_error(call_id))?
                .response_ingress = Some(ingress);
            match descriptor.kind {
                ObjectRpcChannelBodyKind::Attachment => {
                    let _ = result_sender
                        .send(Ok(ObjectRpcSessionResult::new(response, incoming_body)));
                }
                ObjectRpcChannelBodyKind::Value => {
                    let total_bytes = descriptor.total_bytes;
                    let (declared_bytes, materialized_value_permit) =
                        materialized_value_reservation
                            .expect("value descriptor reserves materialization bytes");
                    tokio::spawn(async move {
                        let mut body = incoming_body;
                        let result = read_materialized_value_exact(
                            &mut body,
                            declared_bytes,
                            "materialize multiplexed object RPC response value",
                        )
                        .await;
                        match result {
                            Ok(value) => {
                                let mut response = response;
                                response.body = value;
                                let _ = result_sender.send(Ok(
                                    ObjectRpcSessionResult::materialized_channel_value(
                                        response,
                                        total_bytes,
                                    ),
                                ));
                            }
                            Err(error) => {
                                let _ = result_sender.send(Err(error));
                            }
                        }
                        drop(materialized_value_permit);
                    });
                }
            }
        } else {
            let _ = result_sender.send(Ok(ObjectRpcSessionResult::value(response)));
            lifetime.mark_incoming_done();
        }
        if let Some(trace) = timeline {
            record_network_timeline_event_now(NetworkTimelineEvent::new(
                trace,
                network_timeline_stage::PENDING_RESOLVE,
                network_timeline_flag::RESPONSE | network_timeline_flag::SUCCESS,
                self.connection_id,
                call_id.0,
                0,
                response_bytes,
                0,
                network_timeline_monotonic_ns().saturating_sub(resolve_started_ns),
            ));
        }
        Ok(())
    }

    fn body_ingress(
        &self,
        call_id: ObjectRpcChannelCallId,
        direction: ObjectRpcChannelBodyDirection,
    ) -> Result<Arc<BodyIngress>, NetworkError> {
        match direction {
            ObjectRpcChannelBodyDirection::Request => self
                .inbound
                .lock()
                .map_err(|_| channel_lock_error())?
                .get(&call_id)
                .and_then(|call| call.request_ingress.as_ref().map(Arc::clone))
                .ok_or_else(|| unknown_call_error(call_id)),
            ObjectRpcChannelBodyDirection::Response => self
                .pending
                .lock()
                .map_err(|_| channel_lock_error())?
                .get(&call_id)
                .and_then(|call| call.response_ingress.as_ref().map(Arc::clone))
                .ok_or_else(|| unknown_call_error(call_id)),
        }
    }

    fn send_window(
        &self,
        call_id: ObjectRpcChannelCallId,
        direction: ObjectRpcChannelBodyDirection,
    ) -> Result<(Arc<SendWindow>, Arc<ChannelCallLifetime>), NetworkError> {
        match direction {
            ObjectRpcChannelBodyDirection::Request => self
                .pending
                .lock()
                .map_err(|_| channel_lock_error())?
                .get(&call_id)
                .and_then(|call| {
                    call.request_window
                        .as_ref()
                        .map(|window| (Arc::clone(window), Arc::clone(&call.lifetime)))
                })
                .ok_or_else(|| unknown_call_error(call_id)),
            ObjectRpcChannelBodyDirection::Response => self
                .inbound
                .lock()
                .map_err(|_| channel_lock_error())?
                .get(&call_id)
                .and_then(|call| {
                    call.response_window
                        .as_ref()
                        .map(|window| (Arc::clone(window), Arc::clone(&call.lifetime)))
                })
                .ok_or_else(|| unknown_call_error(call_id)),
        }
    }

    async fn respond(
        self: &Arc<Self>,
        call_id: ObjectRpcChannelCallId,
        response: ObjectRpcSessionResponse,
    ) -> Result<Arc<ChannelCallLifetime>, NetworkError> {
        self.ensure_open()?;
        // Claim the one public response attempt before doing fallible
        // preparation. This both rejects concurrent responders deterministically
        // and makes failures terminal instead of leaving an implicit retry
        // state that no API contract promises.
        let timeline = {
            let mut inbound = self.inbound.lock().map_err(|_| channel_lock_error())?;
            let call = inbound
                .get_mut(&call_id)
                .ok_or_else(|| unknown_call_error(call_id))?;
            if call.responded {
                return Err(NetworkError::Serialization {
                    detail: format!("multiplexed call {call_id} already has a response"),
                });
            }
            call.responded = true;
            call.lifetime.timeline
        };
        let response_started_ns = timeline
            .map(|_| network_timeline_monotonic_ns())
            .unwrap_or(0);

        let (response, outgoing) = prepare_channel_response(response)
            .map_err(|error| self.terminal_response_error(call_id, error))?;
        let response_bytes = u32::try_from(response.body.len()).unwrap_or(u32::MAX);
        validate_response_head(&response)
            .map_err(|error| self.terminal_response_error(call_id, error))?;
        let descriptor = outgoing
            .as_ref()
            .map(|outgoing| outgoing.descriptor.clone());
        let outgoing_value_permit = match descriptor.as_ref() {
            Some(descriptor) => self
                .try_reserve_materialized_value(descriptor)
                .map_err(|error| self.terminal_response_error(call_id, error))?
                .map(|(_, permit)| permit),
            None => None,
        };
        let (lifetime, response_window) = {
            let mut inbound = self.inbound.lock().map_err(|_| channel_lock_error())?;
            let call = inbound
                .get_mut(&call_id)
                .ok_or_else(|| unknown_call_error(call_id))?;
            let window = outgoing
                .as_ref()
                .map(|_| Arc::new(SendWindow::new(OBJECT_RPC_CHANNEL_CALL_WINDOW_BYTES)));
            call.response_window = window.as_ref().map(Arc::clone);
            (Arc::clone(&call.lifetime), window)
        };

        let frame = ObjectRpcChannelWireFrame::Response {
            call_id,
            response,
            body: descriptor,
            initial_credit_bytes: if outgoing.is_some() {
                OBJECT_RPC_CHANNEL_CALL_WINDOW_BYTES as u32
            } else {
                0
            },
        };
        if let Some(outgoing) = outgoing {
            let emitted = match self
                .outbound
                .try_control_with_ack_timeline(frame, lifetime.timeline)
            {
                Ok(emitted) => emitted,
                Err(error) => {
                    self.reset_inbound(call_id, error.to_string(), false);
                    return Err(error);
                }
            };
            let shared = Arc::clone(self);
            let response_window =
                response_window.expect("outgoing response body has a send window");
            let body_lifetime = Arc::clone(&lifetime);
            tokio::spawn(async move {
                if let Err(error) = run_outgoing_body(
                    Arc::clone(&shared),
                    call_id,
                    ObjectRpcChannelBodyDirection::Response,
                    outgoing.source,
                    response_window,
                    body_lifetime,
                    Some(emitted),
                    outgoing_value_permit,
                )
                .await
                {
                    shared.reset_inbound(call_id, error.to_string(), true);
                }
            });
        } else {
            if let Err(error) = self
                .outbound
                .try_bodyless_control(frame, Arc::clone(&lifetime))
            {
                self.reset_inbound(call_id, error.to_string(), false);
                return Err(error);
            }
        }
        if let Some(trace) = timeline {
            record_network_timeline_event_now(NetworkTimelineEvent::new(
                trace,
                network_timeline_stage::RESPONSE_PREPARE,
                network_timeline_flag::RESPONSE,
                self.connection_id,
                call_id.0,
                0,
                response_bytes,
                0,
                network_timeline_monotonic_ns().saturating_sub(response_started_ns),
            ));
        }
        Ok(lifetime)
    }

    fn terminal_response_error(
        &self,
        call_id: ObjectRpcChannelCallId,
        error: NetworkError,
    ) -> NetworkError {
        self.reset_inbound(call_id, error.to_string(), true);
        error
    }

    fn cancel_client(&self, call_id: ObjectRpcChannelCallId, reason: String) {
        let timeline = self.call_timeline(call_id);
        let _ = self.outbound.try_control_with_timeline(
            ObjectRpcChannelWireFrame::Cancel {
                call_id,
                reason: reason.clone(),
            },
            timeline,
        );
        self.abort_client(call_id, reason, false);
    }

    fn abort_client(&self, call_id: ObjectRpcChannelCallId, reason: String, send_reset: bool) {
        let _ = self.try_abort_client(call_id, reason, send_reset);
    }

    fn try_abort_client(
        &self,
        call_id: ObjectRpcChannelCallId,
        reason: String,
        send_reset: bool,
    ) -> bool {
        let state = self
            .pending
            .lock()
            .ok()
            .and_then(|mut pending| pending.remove(&call_id));
        let removed = state.is_some();
        let timeline = state.as_ref().and_then(|state| state.lifetime.timeline);
        if let Some(mut state) = state {
            self.record_tombstone(call_id);
            if let Some(window) = state.request_window.as_ref() {
                window.cancel();
            }
            if let Some(ingress) = state.response_ingress.take() {
                ingress.cancel();
            }
            if let Some(sender) = state.result_sender.take() {
                let _ = sender.send(Err(channel_delivery_error(&reason)));
            }
            state.lifetime.cancel();
        }
        if send_reset && removed {
            let _ = self.outbound.try_control_with_timeline(
                ObjectRpcChannelWireFrame::Reset { call_id, reason },
                timeline,
            );
        }
        removed
    }

    fn reset_inbound(&self, call_id: ObjectRpcChannelCallId, reason: String, send_reset: bool) {
        let _ = self.try_reset_inbound(call_id, reason, send_reset);
    }

    fn try_reset_inbound(
        &self,
        call_id: ObjectRpcChannelCallId,
        reason: String,
        send_reset: bool,
    ) -> bool {
        let state = self
            .inbound
            .lock()
            .ok()
            .and_then(|mut inbound| inbound.remove(&call_id));
        let removed = state.is_some();
        let timeline = state.as_ref().and_then(|state| state.lifetime.timeline);
        if let Some(mut state) = state {
            self.record_tombstone(call_id);
            if let Some(window) = state.response_window.as_ref() {
                window.cancel();
            }
            if let Some(ingress) = state.request_ingress.take() {
                ingress.cancel();
            }
            let _ = state.cancellation_sender.send(Some(reason.clone()));
            state.lifetime.cancel();
        }
        if send_reset && removed {
            let _ = self.outbound.try_control_with_timeline(
                ObjectRpcChannelWireFrame::Reset { call_id, reason },
                timeline,
            );
        }
        removed
    }
}

struct PendingCallState {
    result_sender: Option<oneshot::Sender<Result<ObjectRpcSessionResult, NetworkError>>>,
    request_window: Option<Arc<SendWindow>>,
    response_ingress: Option<Arc<BodyIngress>>,
    lifetime: Arc<ChannelCallLifetime>,
}

struct InboundCallState {
    response_window: Option<Arc<SendWindow>>,
    request_ingress: Option<Arc<BodyIngress>>,
    lifetime: Arc<ChannelCallLifetime>,
    cancellation_sender: watch::Sender<Option<String>>,
    responded: bool,
}

#[derive(Clone, Copy)]
enum ChannelCallSide {
    Client,
    Server,
}

struct ChannelCallLifetime {
    owner: Weak<ChannelShared>,
    side: ChannelCallSide,
    call_id: ObjectRpcChannelCallId,
    permit: Mutex<Option<OwnedSemaphorePermit>>,
    outgoing_done: AtomicBool,
    incoming_done: AtomicBool,
    cancelled: AtomicBool,
    cancel_notify: tokio::sync::Notify,
    progress: watch::Sender<u64>,
    timeline: Option<NetworkTimelineTraceContext>,
}

impl ChannelCallLifetime {
    fn new(
        permit: OwnedSemaphorePermit,
        owner: Weak<ChannelShared>,
        side: ChannelCallSide,
        call_id: ObjectRpcChannelCallId,
        timeline: Option<NetworkTimelineTraceContext>,
    ) -> Arc<Self> {
        let (progress, _) = watch::channel(0);
        Arc::new(Self {
            owner,
            side,
            call_id,
            permit: Mutex::new(Some(permit)),
            outgoing_done: AtomicBool::new(false),
            incoming_done: AtomicBool::new(false),
            cancelled: AtomicBool::new(false),
            cancel_notify: tokio::sync::Notify::new(),
            progress,
            timeline,
        })
    }

    fn progress(&self) -> watch::Receiver<u64> {
        self.progress.subscribe()
    }

    fn progress_sender(&self) -> watch::Sender<u64> {
        self.progress.clone()
    }

    fn mark_progress(&self) {
        advance_progress(&self.progress);
    }

    fn mark_outgoing_done(&self) {
        self.outgoing_done.store(true, Ordering::Release);
        self.cancel_notify.notify_waiters();
        self.finish_if_complete();
    }

    fn mark_incoming_done(&self) {
        self.incoming_done.store(true, Ordering::Release);
        self.finish_if_complete();
    }

    fn finish_if_complete(&self) {
        if self.outgoing_done.load(Ordering::Acquire) && self.incoming_done.load(Ordering::Acquire)
        {
            self.release();
        }
    }

    fn cancel(&self) {
        if !self.cancelled.swap(true, Ordering::AcqRel) {
            self.mark_progress();
            self.cancel_notify.notify_waiters();
        }
        self.release();
    }

    async fn cancelled(&self) {
        loop {
            if self.cancelled.load(Ordering::Acquire) {
                return;
            }
            let notified = self.cancel_notify.notified();
            if self.cancelled.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    async fn wait_outgoing_done(&self) -> Result<(), NetworkError> {
        loop {
            if self.outgoing_done.load(Ordering::Acquire) {
                return Ok(());
            }
            if self.cancelled.load(Ordering::Acquire) {
                return Err(channel_delivery_error(
                    "physical channel closed before response write completed",
                ));
            }
            let notified = self.cancel_notify.notified();
            if self.outgoing_done.load(Ordering::Acquire) {
                return Ok(());
            }
            if self.cancelled.load(Ordering::Acquire) {
                return Err(channel_delivery_error(
                    "physical channel closed before response write completed",
                ));
            }
            notified.await;
        }
    }

    fn release(&self) {
        let released = self
            .permit
            .lock()
            .ok()
            .and_then(|mut permit| permit.take())
            .is_some();
        if released && let Some(owner) = self.owner.upgrade() {
            owner.remove_call(self.side, self.call_id);
        }
    }
}

fn advance_progress(progress: &watch::Sender<u64>) {
    progress.send_modify(|generation| {
        *generation = generation.saturating_add(1);
    });
}

struct SendWindow {
    window_bytes: AtomicUsize,
    sent_offset: AtomicU64,
    acknowledged_offset: AtomicU64,
    cancelled: AtomicBool,
    notify: tokio::sync::Notify,
}

impl SendWindow {
    fn new(window_bytes: usize) -> Self {
        Self {
            window_bytes: AtomicUsize::new(window_bytes),
            sent_offset: AtomicU64::new(0),
            acknowledged_offset: AtomicU64::new(0),
            cancelled: AtomicBool::new(false),
            notify: tokio::sync::Notify::new(),
        }
    }

    fn open(&self, window_bytes: usize) -> Result<(), NetworkError> {
        if window_bytes != OBJECT_RPC_CHANNEL_CALL_WINDOW_BYTES {
            return Err(NetworkError::Serialization {
                detail: format!("invalid multiplexed object RPC window {window_bytes}"),
            });
        }
        self.window_bytes
            .compare_exchange(0, window_bytes, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| NetworkError::Serialization {
                detail: "multiplexed object RPC window was opened more than once".to_string(),
            })?;
        self.notify.notify_one();
        Ok(())
    }

    fn acknowledge(&self, received_offset: u64) -> Result<(), NetworkError> {
        let sent_offset = self.sent_offset.load(Ordering::Acquire);
        let previous = self.acknowledged_offset.load(Ordering::Acquire);
        if received_offset < previous || received_offset > sent_offset {
            return Err(NetworkError::Serialization {
                detail: format!(
                    "invalid cumulative multiplexed credit offset {received_offset}; acknowledged={previous} sent={sent_offset}"
                ),
            });
        }
        self.acknowledged_offset
            .store(received_offset, Ordering::Release);
        self.notify.notify_one();
        Ok(())
    }

    async fn wait_available(&self) -> Result<usize, NetworkError> {
        loop {
            if self.cancelled.load(Ordering::Acquire) {
                return Err(channel_delivery_error("multiplexed call was cancelled"));
            }
            let window_bytes = self.window_bytes.load(Ordering::Acquire);
            let sent_offset = self.sent_offset.load(Ordering::Acquire);
            let acknowledged_offset = self.acknowledged_offset.load(Ordering::Acquire);
            let available = window_bytes.saturating_sub(
                usize::try_from(sent_offset.saturating_sub(acknowledged_offset))
                    .unwrap_or(usize::MAX),
            );
            if available > 0 {
                return Ok(available.min(OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES));
            }
            let notified = self.notify.notified();
            if self.cancelled.load(Ordering::Acquire) {
                return Err(channel_delivery_error("multiplexed call was cancelled"));
            }
            let window_bytes = self.window_bytes.load(Ordering::Acquire);
            let sent_offset = self.sent_offset.load(Ordering::Acquire);
            let acknowledged_offset = self.acknowledged_offset.load(Ordering::Acquire);
            if sent_offset.saturating_sub(acknowledged_offset)
                < u64::try_from(window_bytes).unwrap_or(u64::MAX)
            {
                continue;
            }
            if self.cancelled.load(Ordering::Acquire) {
                return Err(channel_delivery_error("multiplexed call was cancelled"));
            }
            notified.await;
        }
    }

    fn consume(&self, bytes: usize) -> Result<(), NetworkError> {
        let bytes = u64::try_from(bytes).map_err(|_| NetworkError::Serialization {
            detail: "multiplexed object RPC body offset overflow".to_string(),
        })?;
        let sent_offset = self.sent_offset.load(Ordering::Acquire);
        let acknowledged_offset = self.acknowledged_offset.load(Ordering::Acquire);
        let next_offset =
            sent_offset
                .checked_add(bytes)
                .ok_or_else(|| NetworkError::Serialization {
                    detail: "multiplexed object RPC body offset overflow".to_string(),
                })?;
        let window_bytes =
            u64::try_from(self.window_bytes.load(Ordering::Acquire)).unwrap_or(u64::MAX);
        if next_offset.saturating_sub(acknowledged_offset) > window_bytes {
            return Err(NetworkError::Serialization {
                detail: "multiplexed object RPC sender exceeded granted window".to_string(),
            });
        }
        self.sent_offset.store(next_offset, Ordering::Release);
        Ok(())
    }

    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        // One sender task waits on each logical body window. `notify_one`
        // retains a permit when cancellation races waiter registration.
        self.notify.notify_one();
    }
}

enum BodyIngressEvent {
    Data(Bytes),
    End { total_bytes: u64 },
    Cancel,
}

struct BodyIngress {
    descriptor: ObjectRpcChannelBodyDescriptor,
    state: Mutex<BodyIngressState>,
    sender: mpsc::Sender<BodyIngressEvent>,
    progress: watch::Sender<u64>,
}

struct BodyIngressState {
    next_offset: u64,
    acknowledged_offset: u64,
    ended: bool,
}

impl BodyIngress {
    fn validate_data(&self, offset: u64, bytes: usize) -> Result<(), NetworkError> {
        let state = self.state.lock().map_err(|_| channel_lock_error())?;
        self.validate_data_locked(&state, offset, bytes).map(|_| ())
    }

    fn validate_data_locked(
        &self,
        state: &BodyIngressState,
        offset: u64,
        bytes: usize,
    ) -> Result<u64, NetworkError> {
        if bytes == 0 || bytes > OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES {
            return Err(NetworkError::Serialization {
                detail: format!("invalid multiplexed data quantum {bytes}"),
            });
        }
        if state.ended || offset != state.next_offset {
            return Err(NetworkError::Serialization {
                detail: "invalid multiplexed body offset or flow-control credit".to_string(),
            });
        }
        let next_offset = state.next_offset.checked_add(bytes as u64).ok_or_else(|| {
            NetworkError::Serialization {
                detail: "multiplexed body offset overflow".to_string(),
            }
        })?;
        if next_offset.saturating_sub(state.acknowledged_offset)
            > OBJECT_RPC_CHANNEL_CALL_WINDOW_BYTES as u64
        {
            return Err(NetworkError::Serialization {
                detail: "multiplexed sender exceeded receiver window".to_string(),
            });
        }
        if self
            .descriptor
            .total_bytes
            .is_some_and(|total_bytes| next_offset > total_bytes)
        {
            return Err(NetworkError::Serialization {
                detail: "multiplexed body exceeded its declared length".to_string(),
            });
        }
        Ok(next_offset)
    }

    fn accept_data(&self, offset: u64, bytes: Bytes) -> Result<(), NetworkError> {
        let mut state = self.state.lock().map_err(|_| channel_lock_error())?;
        let next_offset = self.validate_data_locked(&state, offset, bytes.len())?;
        self.sender
            .try_send(BodyIngressEvent::Data(bytes))
            .map_err(|_| channel_resource_error("queue multiplexed inbound body quantum", 1))?;
        state.next_offset = next_offset;
        advance_progress(&self.progress);
        Ok(())
    }

    fn accept_end(&self, total_bytes: u64) -> Result<(), NetworkError> {
        let mut state = self.state.lock().map_err(|_| channel_lock_error())?;
        if state.ended
            || total_bytes != state.next_offset
            || self
                .descriptor
                .total_bytes
                .is_some_and(|expected| expected != total_bytes)
        {
            return Err(NetworkError::Serialization {
                detail: "invalid multiplexed body completion".to_string(),
            });
        }
        self.sender
            .try_send(BodyIngressEvent::End { total_bytes })
            .map_err(|_| channel_resource_error("queue multiplexed body completion", 1))?;
        state.ended = true;
        advance_progress(&self.progress);
        Ok(())
    }

    fn acknowledge(&self, received_offset: u64) -> Result<(), NetworkError> {
        let mut state = self.state.lock().map_err(|_| channel_lock_error())?;
        if received_offset < state.acknowledged_offset || received_offset > state.next_offset {
            return Err(NetworkError::Serialization {
                detail: "invalid multiplexed receiver cumulative offset".to_string(),
            });
        }
        state.acknowledged_offset = received_offset;
        Ok(())
    }

    fn cancel(&self) {
        let _ = self.sender.try_send(BodyIngressEvent::Cancel);
    }
}

fn start_body_ingress(
    shared: Arc<ChannelShared>,
    call_id: ObjectRpcChannelCallId,
    direction: ObjectRpcChannelBodyDirection,
    descriptor: ObjectRpcChannelBodyDescriptor,
    lifetime: Arc<ChannelCallLifetime>,
) -> (Arc<BodyIngress>, ObjectRpcIncomingBody) {
    let (event_sender, event_receiver) = mpsc::channel(INBOUND_BODY_EVENT_DEPTH);
    let ingress = Arc::new(BodyIngress {
        descriptor,
        state: Mutex::new(BodyIngressState {
            next_offset: 0,
            acknowledged_offset: 0,
            ended: false,
        }),
        sender: event_sender,
        progress: lifetime.progress_sender(),
    });
    let (body_sender, body) =
        ObjectRpcIncomingBody::multiplexed_channel(ingress.descriptor.total_bytes);
    tokio::spawn(run_body_ingress(
        shared,
        call_id,
        direction,
        Arc::clone(&ingress),
        event_receiver,
        body_sender,
        lifetime,
    ));
    (ingress, body)
}

async fn run_body_ingress(
    shared: Arc<ChannelShared>,
    call_id: ObjectRpcChannelCallId,
    direction: ObjectRpcChannelBodyDirection,
    ingress: Arc<BodyIngress>,
    mut receiver: mpsc::Receiver<BodyIngressEvent>,
    mut body_sender: ObjectRpcChannelBodySender,
    lifetime: Arc<ChannelCallLifetime>,
) {
    let mut total_bytes = 0_u64;
    let mut acknowledged_bytes = 0_u64;
    let mut credit_timer: Option<Pin<Box<tokio::time::Sleep>>> = None;
    let mut consumer_open = true;
    loop {
        let event = if let Some(timer) = credit_timer.as_mut() {
            tokio::select! {
                biased;
                _ = lifetime.cancelled() => {
                    body_sender.fail("multiplexed object RPC body cancelled");
                    return;
                }
                _ = timer => {
                    if let Err(error) = publish_channel_window_update(
                        &shared,
                        &ingress,
                        call_id,
                        direction,
                        total_bytes,
                        lifetime.timeline,
                    ) {
                        let reason = format!("multiplexed body credit failed: {error}");
                        body_sender.fail(reason.clone());
                        fail_incoming_body_call(&shared, call_id, direction, reason, true);
                        return;
                    }
                    acknowledged_bytes = total_bytes;
                    credit_timer = None;
                    continue;
                }
                event = receiver.recv() => event,
            }
        } else {
            tokio::select! {
                biased;
                _ = lifetime.cancelled() => {
                    body_sender.fail("multiplexed object RPC body cancelled");
                    return;
                }
                event = receiver.recv() => event,
            }
        };
        let Some(event) = event else {
            let reason = "multiplexed object RPC body ended before completion".to_string();
            body_sender.fail(reason.clone());
            fail_incoming_body_call(&shared, call_id, direction, reason, true);
            return;
        };
        match event {
            BodyIngressEvent::Data(bytes) => {
                total_bytes = total_bytes.saturating_add(bytes.len() as u64);
                if consumer_open {
                    consumer_open = tokio::select! {
                        biased;
                        _ = lifetime.cancelled() => {
                            body_sender.fail("multiplexed object RPC body cancelled");
                            return;
                        }
                        result = body_sender.send(bytes) => result.is_ok(),
                    };
                    if !consumer_open && direction == ObjectRpcChannelBodyDirection::Response {
                        fail_incoming_body_call(
                            &shared,
                            call_id,
                            direction,
                            "multiplexed response body consumer closed".to_string(),
                            true,
                        );
                        return;
                    }
                }
                if total_bytes.saturating_sub(acknowledged_bytes)
                    >= OBJECT_RPC_CHANNEL_WINDOW_UPDATE_BYTES as u64
                {
                    if let Err(error) = publish_channel_window_update(
                        &shared,
                        &ingress,
                        call_id,
                        direction,
                        total_bytes,
                        lifetime.timeline,
                    ) {
                        let reason = format!("multiplexed body credit failed: {error}");
                        body_sender.fail(reason.clone());
                        fail_incoming_body_call(&shared, call_id, direction, reason, true);
                        return;
                    }
                    acknowledged_bytes = total_bytes;
                    credit_timer = None;
                } else if total_bytes > acknowledged_bytes && credit_timer.is_none() {
                    credit_timer = Some(Box::pin(tokio::time::sleep(
                        OBJECT_RPC_CHANNEL_WINDOW_UPDATE_INTERVAL,
                    )));
                }
            }
            BodyIngressEvent::End {
                total_bytes: declared,
            } => {
                if declared != total_bytes {
                    let reason = "multiplexed object RPC body length mismatch".to_string();
                    body_sender.fail(reason.clone());
                    fail_incoming_body_call(&shared, call_id, direction, reason, true);
                    return;
                }
                // BodyEnd is terminal: the sender already waits for all Data
                // writes before emitting it, so a final credit frame cannot
                // unblock useful work. Omitting it also avoids turning a
                // successfully delivered EOF into a control-queue failure.
                if let Err(error) = body_sender.finish(total_bytes).await {
                    fail_incoming_body_call(&shared, call_id, direction, error.to_string(), true);
                    return;
                }
                lifetime.mark_incoming_done();
                return;
            }
            BodyIngressEvent::Cancel => {
                body_sender.fail("multiplexed object RPC body cancelled");
                return;
            }
        }
    }
}

fn publish_channel_window_update(
    shared: &ChannelShared,
    ingress: &BodyIngress,
    call_id: ObjectRpcChannelCallId,
    direction: ObjectRpcChannelBodyDirection,
    received_offset: u64,
    timeline: Option<NetworkTimelineTraceContext>,
) -> Result<(), NetworkError> {
    // Commit the local receiver window before exposing its cumulative offset;
    // the peer may consume the queued control on another task immediately.
    ingress.acknowledge(received_offset)?;
    shared.outbound.try_control_with_timeline(
        ObjectRpcChannelWireFrame::WindowUpdate {
            call_id,
            direction,
            received_offset,
        },
        timeline,
    )
}

fn fail_incoming_body_call(
    shared: &ChannelShared,
    call_id: ObjectRpcChannelCallId,
    direction: ObjectRpcChannelBodyDirection,
    reason: String,
    send_reset: bool,
) {
    match direction {
        ObjectRpcChannelBodyDirection::Request => shared.reset_inbound(call_id, reason, send_reset),
        ObjectRpcChannelBodyDirection::Response => shared.abort_client(call_id, reason, send_reset),
    }
}

struct PreparedChannelBody {
    descriptor: ObjectRpcChannelBodyDescriptor,
    source: ObjectRpcBodySource,
}

async fn read_materialized_value_exact(
    body: &mut ObjectRpcIncomingBody,
    declared_bytes: usize,
    operation: &str,
) -> Result<Vec<u8>, NetworkError> {
    let mut value = Vec::new();
    value
        .try_reserve_exact(declared_bytes)
        .map_err(|_| channel_resource_error(operation, declared_bytes))?;
    value.resize(declared_bytes, 0);
    body.read_exact(&mut value)
        .await
        .map_err(|source| NetworkError::Io {
            operation: operation.to_string(),
            detail: source.to_string(),
        })?;
    let mut extra = [0_u8; 1];
    let extra_bytes = body
        .read(&mut extra)
        .await
        .map_err(|source| NetworkError::Io {
            operation: operation.to_string(),
            detail: source.to_string(),
        })?;
    if extra_bytes != 0 {
        return Err(NetworkError::Serialization {
            detail: format!(
                "multiplexed materialized value exceeded declared length {declared_bytes}"
            ),
        });
    }
    Ok(value)
}

fn prepare_channel_call(
    call: ObjectRpcSessionCall,
) -> Result<(ObjectRpcRequest, Option<PreparedChannelBody>), NetworkError> {
    let ObjectRpcSessionCall {
        mut request,
        mut body,
    } = call;
    if body.has_value_intent() {
        if request.body.len() <= OBJECT_RPC_CHANNEL_INLINE_VALUE_BYTES {
            return Ok((request, None));
        }
        if request.body.len() > OBJECT_RPC_CHANNEL_MAX_MATERIALIZED_VALUE_BYTES {
            return Err(channel_resource_error(
                "prepare multiplexed request value",
                request.body.len(),
            ));
        }
        let value = std::mem::take(&mut request.body);
        body = ObjectRpcBodySource::bytes(value);
        let descriptor = ObjectRpcChannelBodyDescriptor {
            kind: ObjectRpcChannelBodyKind::Value,
            total_bytes: body.total_bytes(),
        };
        return Ok((
            request,
            Some(PreparedChannelBody {
                descriptor,
                source: body,
            }),
        ));
    }
    if body.is_empty() {
        return Ok((request, None));
    }
    let descriptor = ObjectRpcChannelBodyDescriptor {
        kind: ObjectRpcChannelBodyKind::Attachment,
        total_bytes: body.total_bytes(),
    };
    Ok((
        request,
        Some(PreparedChannelBody {
            descriptor,
            source: body,
        }),
    ))
}

fn prepare_channel_response(
    response: ObjectRpcSessionResponse,
) -> Result<(ObjectRpcResponse, Option<PreparedChannelBody>), NetworkError> {
    let ObjectRpcSessionResponse {
        mut response,
        mut body,
    } = response;
    if body.has_value_intent() {
        if !response.body.is_empty() {
            // Preserve the public literal-construction behavior as well as the
            // normal `ObjectRpcSessionResponse::value` path: a typed value in
            // the head takes precedence over a marker body source.
            if response.body.len() <= OBJECT_RPC_CHANNEL_INLINE_VALUE_BYTES {
                return Ok((response, None));
            }
            if response.body.len() > OBJECT_RPC_CHANNEL_MAX_MATERIALIZED_VALUE_BYTES {
                return Err(channel_resource_error(
                    "prepare multiplexed response value",
                    response.body.len(),
                ));
            }
            let value = std::mem::take(&mut response.body);
            body = ObjectRpcBodySource::bytes(value);
            let descriptor = ObjectRpcChannelBodyDescriptor {
                kind: ObjectRpcChannelBodyKind::Value,
                total_bytes: body.total_bytes(),
            };
            return Ok((
                response,
                Some(PreparedChannelBody {
                    descriptor,
                    source: body,
                }),
            ));
        }
        // A forwarded materialized value already lives in the body source:
        // `ObjectRpcSessionResponse::new` deliberately removes it from the
        // typed head so a subsequent carrier can stream it again. Preserve
        // that value role instead of mistaking the now-empty head for an
        // inline empty value.
        if !body.is_empty() {
            let total_bytes = body.total_bytes();
            if total_bytes.is_none_or(|bytes| {
                usize::try_from(bytes).map_or(true, |bytes| {
                    bytes > OBJECT_RPC_CHANNEL_MAX_MATERIALIZED_VALUE_BYTES
                })
            }) {
                return Err(channel_resource_error(
                    "prepare multiplexed response value",
                    total_bytes
                        .and_then(|bytes| usize::try_from(bytes).ok())
                        .unwrap_or(usize::MAX),
                ));
            }
            let descriptor = ObjectRpcChannelBodyDescriptor {
                kind: ObjectRpcChannelBodyKind::Value,
                total_bytes,
            };
            return Ok((
                response,
                Some(PreparedChannelBody {
                    descriptor,
                    source: body,
                }),
            ));
        }
        return Ok((response, None));
    }
    if body.is_empty() {
        return Ok((response, None));
    }
    let descriptor = ObjectRpcChannelBodyDescriptor {
        kind: ObjectRpcChannelBodyKind::Attachment,
        total_bytes: body.total_bytes(),
    };
    Ok((
        response,
        Some(PreparedChannelBody {
            descriptor,
            source: body,
        }),
    ))
}

async fn run_outgoing_body(
    shared: Arc<ChannelShared>,
    call_id: ObjectRpcChannelCallId,
    direction: ObjectRpcChannelBodyDirection,
    source: ObjectRpcBodySource,
    window: Arc<SendWindow>,
    lifetime: Arc<ChannelCallLifetime>,
    head_emitted: Option<oneshot::Receiver<()>>,
    mut materialized_value_permit: Option<OwnedSemaphorePermit>,
) -> Result<(), NetworkError> {
    if let Some(head_emitted) = head_emitted {
        tokio::select! {
            biased;
            _ = lifetime.cancelled() => {
                return Err(channel_delivery_error("multiplexed body send cancelled"));
            }
            emitted = head_emitted => {
                emitted.map_err(|_| {
                    channel_delivery_error("physical channel closed before call head emission")
                })?;
                lifetime.mark_progress();
            }
        }
    }
    // A request-side producer is not even opened until RequestAccepted grants
    // the first credit. This is important for one-pass readers: a rejected
    // request must remain completely unconsumed.
    let mut producer: Option<BodyProducer> =
        Some(source.producer(OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES)?);
    drop(source);
    let mut offset = 0_u64;
    let write_tracker = Arc::new(BodyWriteTracker::new());
    let mut written = write_tracker.subscribe();
    let mut pending_bytes: Option<Bytes> = None;
    let mut producer_finished = false;
    loop {
        let mut batch = Vec::with_capacity(shared.outbound.data_batch_frames);
        let mut batch_bytes = 0_usize;
        while batch_bytes < shared.outbound.data_batch_bytes
            && batch.len() < shared.outbound.data_batch_frames
            && !producer_finished
        {
            let available = window.wait_available().await?;
            let next = if let Some(bytes) = pending_bytes.take() {
                Some(bytes)
            } else if batch.is_empty() {
                tokio::select! {
                    biased;
                    _ = lifetime.cancelled() => {
                        return Err(channel_delivery_error("multiplexed body send cancelled"));
                    }
                    next = producer.as_mut().expect("body producer is active").next() => next?,
                }
            } else {
                tokio::select! {
                    biased;
                    _ = lifetime.cancelled() => {
                        return Err(channel_delivery_error("multiplexed body send cancelled"));
                    }
                    _ = tokio::time::sleep(OBJECT_RPC_CHANNEL_WINDOW_UPDATE_INTERVAL) => break,
                    next = producer.as_mut().expect("body producer is active").next() => next?,
                }
            };
            let Some(mut bytes) = next else {
                tokio::select! {
                    biased;
                    _ = lifetime.cancelled() => {
                        return Err(channel_delivery_error("multiplexed body send cancelled"));
                    }
                    result = producer.take().expect("body producer is active").finish() => result?,
                }
                producer_finished = true;
                break;
            };
            if bytes.is_empty() || bytes.len() > OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES {
                return Err(NetworkError::Serialization {
                    detail: format!(
                        "body producer emitted invalid multiplexed channel quantum {}",
                        bytes.len()
                    ),
                });
            }
            let remaining_batch = shared.outbound.data_batch_bytes - batch_bytes;
            let send_bytes = available.min(remaining_batch).min(bytes.len());
            if send_bytes < bytes.len() {
                pending_bytes = Some(bytes.split_off(send_bytes));
            }
            window.consume(bytes.len())?;
            let payload_len =
                u32::try_from(bytes.len()).map_err(|_| NetworkError::Serialization {
                    detail: "multiplexed Data payload length overflow".to_string(),
                })?;
            let next_offset = offset.checked_add(u64::from(payload_len)).ok_or_else(|| {
                NetworkError::Serialization {
                    detail: "multiplexed outgoing body offset overflow".to_string(),
                }
            })?;
            batch.push(ObjectRpcChannelDataFrame {
                header: ObjectRpcChannelDataHeader {
                    call_id,
                    direction,
                    offset,
                    payload_len,
                },
                bytes,
            });
            batch_bytes = batch_bytes.saturating_add(payload_len as usize);
            offset = next_offset;
        }

        if !batch.is_empty() {
            tokio::select! {
                biased;
                _ = lifetime.cancelled() => {
                    return Err(channel_delivery_error("multiplexed body send cancelled"));
                }
                result = shared.outbound.queue_data_batch(
                    batch,
                    Arc::clone(&write_tracker),
                    lifetime.progress_sender(),
                    lifetime.timeline,
                    true,
                ) => result?,
            }
        }

        if producer_finished {
            tokio::select! {
                biased;
                _ = lifetime.cancelled() => {
                    return Err(channel_delivery_error("multiplexed body send cancelled"));
                }
                result = BodyWriteTracker::wait_for(&mut written, offset) => result?,
            }
            drop(materialized_value_permit.take());
            let emitted = shared.outbound.try_control_with_ack_timeline(
                ObjectRpcChannelWireFrame::BodyEnd {
                    call_id,
                    direction,
                    total_bytes: offset,
                },
                lifetime.timeline,
            )?;
            tokio::select! {
                biased;
                _ = lifetime.cancelled() => {
                    return Err(channel_delivery_error("multiplexed body send cancelled"));
                }
                result = emitted => {
                    result.map_err(|_| {
                        channel_delivery_error(
                            "physical channel closed before body completion emission",
                        )
                    })?;
                    lifetime.mark_progress();
                }
            }
            lifetime.mark_outgoing_done();
            return Ok(());
        }
        if lifetime.cancelled.load(Ordering::Acquire) {
            return Err(channel_delivery_error("multiplexed body send cancelled"));
        }
    }
}

struct OutboundQueues {
    connection_id: u64,
    control_sender: mpsc::Sender<QueuedControl>,
    data_sender: mpsc::Sender<QueuedData>,
    control_budget: Arc<Semaphore>,
    data_budget: Arc<Semaphore>,
    data_frame_budget: Arc<Semaphore>,
    data_batch_bytes: usize,
    data_batch_frames: usize,
    queued_control_frames: Arc<AtomicUsize>,
    queued_bytes: Arc<AtomicUsize>,
    queued_data_bytes: Arc<AtomicUsize>,
}

impl OutboundQueues {
    #[cfg(test)]
    fn try_control(&self, frame: ObjectRpcChannelWireFrame) -> Result<(), NetworkError> {
        self.try_control_with_timeline(frame, None)
    }

    fn try_control_with_timeline(
        &self,
        frame: ObjectRpcChannelWireFrame,
        timeline: Option<NetworkTimelineTraceContext>,
    ) -> Result<(), NetworkError> {
        self.try_control_inner(frame, None, timeline).map(|_| ())
    }

    fn try_control_with_ack_timeline(
        &self,
        frame: ObjectRpcChannelWireFrame,
        timeline: Option<NetworkTimelineTraceContext>,
    ) -> Result<oneshot::Receiver<()>, NetworkError> {
        let (sender, receiver) = oneshot::channel();
        self.try_control_inner(frame, Some(QueuedControlCompletion::Ack(sender)), timeline)?;
        Ok(receiver)
    }

    fn try_bodyless_control(
        &self,
        frame: ObjectRpcChannelWireFrame,
        lifetime: Arc<ChannelCallLifetime>,
    ) -> Result<(), NetworkError> {
        let timeline = lifetime.timeline;
        self.try_control_inner(
            frame,
            Some(QueuedControlCompletion::BodylessCall(lifetime)),
            timeline,
        )
    }

    fn try_control_inner(
        &self,
        frame: ObjectRpcChannelWireFrame,
        completion: Option<QueuedControlCompletion>,
        explicit_timeline: Option<NetworkTimelineTraceContext>,
    ) -> Result<(), NetworkError> {
        let call_id = frame.call_id();
        let timeline = explicit_timeline.or_else(|| match &frame {
            ObjectRpcChannelWireFrame::Request { request, .. } => {
                network_timeline_trace_context_for_request(request)
            }
            _ => None,
        });
        let timeline_queued_ns = timeline
            .map(|_| network_timeline_monotonic_ns())
            .unwrap_or(0);
        let timeline_flags = channel_frame_timeline_flags(&frame);
        let frame_id = timeline
            .map(|_| channel_frame_timeline_id(&frame))
            .unwrap_or(0);
        let bytes = queued_frame_bytes(&frame);
        let permits = u32::try_from(bytes)
            .map_err(|_| channel_resource_error("reserve multiplexed queued bytes", bytes))?;
        let permit = Arc::clone(&self.control_budget)
            .try_acquire_many_owned(permits)
            .map_err(|_| {
                channel_resource_error("reserve multiplexed queued control bytes", bytes)
            })?;
        self.queued_control_frames.fetch_add(1, Ordering::Relaxed);
        self.queued_bytes.fetch_add(bytes, Ordering::Relaxed);
        let queued = QueuedControl {
            frame: Some(frame),
            completion,
            bytes,
            timeline,
            timeline_queued_ns,
            timeline_flags,
            connection_id: self.connection_id,
            call_id,
            frame_id,
            permit,
            queued_control_frames: Arc::clone(&self.queued_control_frames),
            queued_bytes: Arc::clone(&self.queued_bytes),
        };
        match self.control_sender.try_send(queued) {
            Ok(()) => Ok(()),
            Err(error) => {
                // Queue admission failed, so the caller still owns failure
                // handling and its reason. Disarm the queued-drop completion
                // before `QueuedControl::drop` releases the byte accounting.
                let mut queued = error.into_inner();
                queued.completion.take();
                Err(channel_resource_error(
                    "queue multiplexed control frame",
                    bytes,
                ))
            }
        }
    }

    #[cfg(test)]
    async fn queue_data(
        &self,
        frame: ObjectRpcChannelWireFrame,
    ) -> Result<oneshot::Receiver<()>, NetworkError> {
        self.queue_call_data(frame, watch::channel(0).0, None).await
    }

    #[cfg(test)]
    async fn queue_call_data(
        &self,
        frame: ObjectRpcChannelWireFrame,
        progress: watch::Sender<u64>,
        timeline: Option<NetworkTimelineTraceContext>,
    ) -> Result<oneshot::Receiver<()>, NetworkError> {
        let ObjectRpcChannelWireFrame::Data {
            call_id,
            direction,
            offset,
            bytes,
        } = frame
        else {
            return Err(NetworkError::Serialization {
                detail: "data scheduler accepted a control frame".to_string(),
            });
        };
        let payload_len = u32::try_from(bytes.len()).map_err(|_| NetworkError::Serialization {
            detail: "multiplexed Data payload length overflow".to_string(),
        })?;
        let frame = ObjectRpcChannelDataFrame {
            header: ObjectRpcChannelDataHeader {
                call_id,
                direction,
                offset,
                payload_len,
            },
            bytes: Bytes::from(bytes),
        };
        let target = offset.saturating_add(u64::from(payload_len));
        let tracker = Arc::new(BodyWriteTracker::new());
        let mut written = tracker.subscribe();
        self.queue_data_batch(vec![frame], Arc::clone(&tracker), progress, timeline, false)
            .await?;
        let (emitted, emission) = oneshot::channel();
        tokio::spawn(async move {
            if BodyWriteTracker::wait_for(&mut written, target)
                .await
                .is_ok()
            {
                let _ = emitted.send(());
            }
        });
        Ok(emission)
    }

    async fn queue_data_batch(
        &self,
        frames: Vec<ObjectRpcChannelDataFrame>,
        tracker: Arc<BodyWriteTracker>,
        progress: watch::Sender<u64>,
        timeline: Option<NetworkTimelineTraceContext>,
        prune_if_inactive: bool,
    ) -> Result<(), NetworkError> {
        if frames.is_empty() || frames.len() > self.data_batch_frames {
            return Err(NetworkError::Serialization {
                detail: "invalid multiplexed queued Data batch frame count".to_string(),
            });
        }
        let batch_bytes = frames.iter().try_fold(0_usize, |total, frame| {
            frame.validate()?;
            total
                .checked_add(frame.bytes.len())
                .ok_or_else(|| NetworkError::Serialization {
                    detail: "multiplexed queued Data batch length overflow".to_string(),
                })
        })?;
        if batch_bytes == 0 || batch_bytes > self.data_batch_bytes {
            return Err(NetworkError::Serialization {
                detail: format!("invalid multiplexed queued Data batch size {batch_bytes}"),
            });
        }
        let byte_permits = u32::try_from(batch_bytes).map_err(|_| {
            channel_resource_error("reserve multiplexed queued data batch", batch_bytes)
        })?;
        let frame_permits = u32::try_from(frames.len()).map_err(|_| {
            channel_resource_error("reserve multiplexed queued data frames", frames.len())
        })?;
        let byte_permit = Arc::clone(&self.data_budget)
            .acquire_many_owned(byte_permits)
            .await
            .map_err(|_| channel_delivery_error("multiplexed data queue closed"))?;
        let frame_permit = Arc::clone(&self.data_frame_budget)
            .acquire_many_owned(frame_permits)
            .await
            .map_err(|_| channel_delivery_error("multiplexed data frame queue closed"))?;
        let reservation = Arc::new(QueuedDataBatchReservation {
            _byte_permit: byte_permit,
            _frame_permit: frame_permit,
        });
        for frame in frames {
            let call_id = frame.header.call_id;
            let lane = DataLaneKey {
                call_id,
                direction: frame.header.direction,
            };
            let timeline_flags = channel_data_timeline_flags(frame.header.direction);
            let frame_id = timeline
                .map(|_| channel_data_timeline_id(&frame.header))
                .unwrap_or(0);
            let timeline_queued_ns = timeline
                .map(|_| network_timeline_monotonic_ns())
                .unwrap_or(0);
            let bytes = frame.bytes.len();
            let end_offset = frame.header.offset + bytes as u64;
            self.queued_bytes.fetch_add(bytes, Ordering::Relaxed);
            self.queued_data_bytes.fetch_add(bytes, Ordering::Relaxed);
            let queued = QueuedData {
                frame: Some(frame),
                lane,
                bytes,
                _reservation: Arc::clone(&reservation),
                queued_bytes: Arc::clone(&self.queued_bytes),
                queued_data_bytes: Arc::clone(&self.queued_data_bytes),
                tracker: Arc::clone(&tracker),
                end_offset,
                written: false,
                prune_if_inactive,
                progress: Some(progress.clone()),
                timeline,
                timeline_queued_ns,
                timeline_flags,
                connection_id: self.connection_id,
                call_id,
                frame_id,
            };
            self.data_sender.send(queued).await.map_err(|error| {
                channel_delivery_error(&format!("multiplexed data scheduler closed: {error}"))
            })?;
        }
        Ok(())
    }
}

enum QueuedControlCompletion {
    Ack(oneshot::Sender<()>),
    BodylessCall(Arc<ChannelCallLifetime>),
}

enum PendingChannelWrite {
    Control(QueuedControl),
    Data(QueuedData),
}

impl PendingChannelWrite {
    fn take_frame(&mut self) -> ObjectRpcChannelTransportFrame {
        match self {
            Self::Control(queued) => ObjectRpcChannelTransportFrame::Control(queued.take_frame()),
            Self::Data(queued) => ObjectRpcChannelTransportFrame::Data(queued.take_frame()),
        }
    }

    fn mark_written(&mut self) {
        match self {
            Self::Control(queued) => queued.mark_written(),
            Self::Data(queued) => queued.mark_written(),
        }
    }

    fn write_timeline(&self) -> Option<ObjectRpcChannelWriteTimeline> {
        match self {
            Self::Control(queued) => queued.write_timeline(),
            Self::Data(queued) => queued.write_timeline(),
        }
    }
}

struct QueuedControl {
    frame: Option<ObjectRpcChannelWireFrame>,
    completion: Option<QueuedControlCompletion>,
    bytes: usize,
    timeline: Option<NetworkTimelineTraceContext>,
    timeline_queued_ns: u64,
    timeline_flags: u16,
    connection_id: u64,
    call_id: ObjectRpcChannelCallId,
    frame_id: u64,
    permit: OwnedSemaphorePermit,
    queued_control_frames: Arc<AtomicUsize>,
    queued_bytes: Arc<AtomicUsize>,
}

impl QueuedControl {
    fn take_frame(&mut self) -> ObjectRpcChannelWireFrame {
        if let Some(trace) = self.timeline {
            record_network_timeline_event_now(NetworkTimelineEvent::new(
                trace,
                network_timeline_stage::TX_QUEUE_WAIT,
                self.timeline_flags,
                self.connection_id,
                self.call_id.0,
                self.frame_id,
                u32::try_from(self.bytes).unwrap_or(u32::MAX),
                0,
                network_timeline_monotonic_ns().saturating_sub(self.timeline_queued_ns),
            ));
        }
        self.frame
            .take()
            .expect("queued multiplexed control frame is present")
    }

    fn write_timeline(&self) -> Option<ObjectRpcChannelWriteTimeline> {
        Some(ObjectRpcChannelWriteTimeline {
            trace: self.timeline?,
            connection_id: self.connection_id,
            call_id: self.call_id,
            frame_id: self.frame_id,
            flags: self.timeline_flags,
            bytes: u32::try_from(self.bytes).unwrap_or(u32::MAX),
        })
    }

    fn mark_written(&mut self) {
        if let Some(completion) = self.completion.take() {
            match completion {
                QueuedControlCompletion::Ack(emitted) => {
                    let _ = emitted.send(());
                }
                QueuedControlCompletion::BodylessCall(lifetime) => {
                    lifetime.mark_progress();
                    lifetime.mark_outgoing_done();
                }
            }
        }
    }
}

impl Drop for QueuedControl {
    fn drop(&mut self) {
        if let Some(QueuedControlCompletion::BodylessCall(lifetime)) = self.completion.take() {
            lifetime.cancel();
        }
        self.queued_control_frames.fetch_sub(1, Ordering::Relaxed);
        self.queued_bytes.fetch_sub(self.bytes, Ordering::Relaxed);
        let _ = &self.permit;
    }
}

struct QueuedDataBatchReservation {
    _byte_permit: OwnedSemaphorePermit,
    _frame_permit: OwnedSemaphorePermit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BodyWriteState {
    Written(u64),
    Failed,
}

struct BodyWriteTracker {
    state: watch::Sender<BodyWriteState>,
}

impl BodyWriteTracker {
    fn new() -> Self {
        Self {
            state: watch::channel(BodyWriteState::Written(0)).0,
        }
    }

    fn subscribe(&self) -> watch::Receiver<BodyWriteState> {
        self.state.subscribe()
    }

    fn mark_written(&self, end_offset: u64) {
        self.state.send_if_modified(|state| match state {
            BodyWriteState::Written(written) if *written < end_offset => {
                *written = end_offset;
                true
            }
            _ => false,
        });
    }

    fn fail(&self) {
        self.state.send_replace(BodyWriteState::Failed);
    }

    async fn wait_for(
        state: &mut watch::Receiver<BodyWriteState>,
        target: u64,
    ) -> Result<(), NetworkError> {
        loop {
            match *state.borrow_and_update() {
                BodyWriteState::Written(written) if written >= target => return Ok(()),
                BodyWriteState::Failed => {
                    return Err(channel_delivery_error(
                        "physical channel closed before final body data emission",
                    ));
                }
                BodyWriteState::Written(_) => {}
            }
            state
                .changed()
                .await
                .map_err(|_| channel_delivery_error("multiplexed body write progress stopped"))?;
        }
    }
}

struct QueuedData {
    frame: Option<ObjectRpcChannelDataFrame>,
    lane: DataLaneKey,
    bytes: usize,
    _reservation: Arc<QueuedDataBatchReservation>,
    queued_bytes: Arc<AtomicUsize>,
    queued_data_bytes: Arc<AtomicUsize>,
    tracker: Arc<BodyWriteTracker>,
    end_offset: u64,
    written: bool,
    prune_if_inactive: bool,
    progress: Option<watch::Sender<u64>>,
    timeline: Option<NetworkTimelineTraceContext>,
    timeline_queued_ns: u64,
    timeline_flags: u16,
    connection_id: u64,
    call_id: ObjectRpcChannelCallId,
    frame_id: u64,
}

impl QueuedData {
    fn take_frame(&mut self) -> ObjectRpcChannelDataFrame {
        if let Some(trace) = self.timeline {
            record_network_timeline_event_now(NetworkTimelineEvent::new(
                trace,
                network_timeline_stage::TX_QUEUE_WAIT,
                self.timeline_flags,
                self.connection_id,
                self.call_id.0,
                self.frame_id,
                u32::try_from(self.bytes).unwrap_or(u32::MAX),
                0,
                network_timeline_monotonic_ns().saturating_sub(self.timeline_queued_ns),
            ));
        }
        self.frame
            .take()
            .expect("queued multiplexed data frame is present")
    }

    fn write_timeline(&self) -> Option<ObjectRpcChannelWriteTimeline> {
        Some(ObjectRpcChannelWriteTimeline {
            trace: self.timeline?,
            connection_id: self.connection_id,
            call_id: self.call_id,
            frame_id: self.frame_id,
            flags: self.timeline_flags,
            bytes: u32::try_from(self.bytes).unwrap_or(u32::MAX),
        })
    }

    fn mark_written(&mut self) {
        self.written = true;
        self.tracker.mark_written(self.end_offset);
        if let Some(progress) = self.progress.take() {
            advance_progress(&progress);
        }
    }
}

impl Drop for QueuedData {
    fn drop(&mut self) {
        if !self.written {
            self.tracker.fail();
        }
        self.queued_bytes.fetch_sub(self.bytes, Ordering::Relaxed);
        self.queued_data_bytes
            .fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

const QUEUED_CONTROL_FRAME_OVERHEAD_BYTES: usize = 128;

fn channel_frame_timeline_flags(frame: &ObjectRpcChannelWireFrame) -> u16 {
    let direction = match frame {
        ObjectRpcChannelWireFrame::Request { .. }
        | ObjectRpcChannelWireFrame::RequestAccepted { .. } => network_timeline_flag::REQUEST,
        ObjectRpcChannelWireFrame::Response { .. } => network_timeline_flag::RESPONSE,
        ObjectRpcChannelWireFrame::Data { direction, .. }
        | ObjectRpcChannelWireFrame::WindowUpdate { direction, .. }
        | ObjectRpcChannelWireFrame::BodyEnd { direction, .. } => match direction {
            ObjectRpcChannelBodyDirection::Request => network_timeline_flag::REQUEST,
            ObjectRpcChannelBodyDirection::Response => network_timeline_flag::RESPONSE,
        },
        ObjectRpcChannelWireFrame::Cancel { .. } | ObjectRpcChannelWireFrame::Reset { .. } => 0,
    };
    direction
        | if matches!(frame, ObjectRpcChannelWireFrame::Data { .. }) {
            network_timeline_flag::DATA_FRAME
        } else {
            network_timeline_flag::CONTROL_FRAME
        }
}

/// Stable within the logical wire protocol, so the sender and receiver can
/// pair a sampled frame without adding a profiling-only field to the frame.
/// TCP ordering disambiguates the rare repeated WindowUpdate/Cancel values;
/// Data and BodyEnd include their logical offsets.
fn channel_frame_timeline_id(frame: &ObjectRpcChannelWireFrame) -> u64 {
    let (kind, direction, detail) = match frame {
        ObjectRpcChannelWireFrame::Request { .. } => (1_u64, 0_u64, 0_u64),
        ObjectRpcChannelWireFrame::RequestAccepted { credit_bytes, .. } => {
            (2, 0, u64::from(*credit_bytes))
        }
        ObjectRpcChannelWireFrame::Response {
            initial_credit_bytes,
            ..
        } => (3, 1, u64::from(*initial_credit_bytes)),
        ObjectRpcChannelWireFrame::Data {
            direction, offset, ..
        } => (4, channel_body_direction_bit(*direction), *offset),
        ObjectRpcChannelWireFrame::WindowUpdate {
            direction,
            received_offset,
            ..
        } => (5, channel_body_direction_bit(*direction), *received_offset),
        ObjectRpcChannelWireFrame::BodyEnd {
            direction,
            total_bytes,
            ..
        } => (6, channel_body_direction_bit(*direction), *total_bytes),
        ObjectRpcChannelWireFrame::Cancel { .. } => (7, 0, 0),
        ObjectRpcChannelWireFrame::Reset { .. } => (8, 0, 0),
    };
    let id = frame.call_id().0.wrapping_mul(0x9e37_79b9_7f4a_7c15)
        ^ kind.wrapping_mul(0xbf58_476d_1ce4_e5b9)
        ^ direction.wrapping_mul(0x94d0_49bb_1331_11eb)
        ^ detail.rotate_left(23);
    id.max(1)
}

const fn channel_body_direction_bit(direction: ObjectRpcChannelBodyDirection) -> u64 {
    match direction {
        ObjectRpcChannelBodyDirection::Request => 0,
        ObjectRpcChannelBodyDirection::Response => 1,
    }
}

fn channel_data_timeline_flags(direction: ObjectRpcChannelBodyDirection) -> u16 {
    let direction = match direction {
        ObjectRpcChannelBodyDirection::Request => network_timeline_flag::REQUEST,
        ObjectRpcChannelBodyDirection::Response => network_timeline_flag::RESPONSE,
    };
    direction | network_timeline_flag::DATA_FRAME
}

fn channel_data_timeline_id(header: &ObjectRpcChannelDataHeader) -> u64 {
    let id = header.call_id.0.wrapping_mul(0x9e37_79b9_7f4a_7c15)
        ^ 4_u64.wrapping_mul(0xbf58_476d_1ce4_e5b9)
        ^ channel_body_direction_bit(header.direction).wrapping_mul(0x94d0_49bb_1331_11eb)
        ^ header.offset.rotate_left(23);
    id.max(1)
}

fn queued_frame_bytes(frame: &ObjectRpcChannelWireFrame) -> usize {
    match frame {
        ObjectRpcChannelWireFrame::Request { request, .. } => {
            request_head_bytes(request).saturating_add(QUEUED_CONTROL_FRAME_OVERHEAD_BYTES)
        }
        ObjectRpcChannelWireFrame::Response { response, .. } => {
            response_head_bytes(response).saturating_add(QUEUED_CONTROL_FRAME_OVERHEAD_BYTES)
        }
        ObjectRpcChannelWireFrame::Data { bytes, .. } => bytes.len(),
        ObjectRpcChannelWireFrame::Cancel { reason, .. }
        | ObjectRpcChannelWireFrame::Reset { reason, .. } => reason
            .len()
            .saturating_add(QUEUED_CONTROL_FRAME_OVERHEAD_BYTES),
        ObjectRpcChannelWireFrame::RequestAccepted { .. }
        | ObjectRpcChannelWireFrame::WindowUpdate { .. }
        | ObjectRpcChannelWireFrame::BodyEnd { .. } => QUEUED_CONTROL_FRAME_OVERHEAD_BYTES,
    }
}

fn request_head_bytes(request: &ObjectRpcRequest) -> usize {
    request
        .route
        .len()
        .saturating_add(request.category.len())
        .saturating_add(request.body.len())
        .saturating_add(request.auth.as_ref().map_or(0, Vec::len))
        .saturating_add(
            request
                .metadata
                .iter()
                .fold(0_usize, |total, (key, value)| {
                    total.saturating_add(key.len().saturating_add(value.len()).saturating_add(32))
                }),
        )
}

fn response_head_bytes(response: &ObjectRpcResponse) -> usize {
    response
        .route
        .len()
        .saturating_add(response.body.len())
        .saturating_add(
            response
                .metadata
                .iter()
                .fold(0_usize, |total, (key, value)| {
                    total.saturating_add(key.len().saturating_add(value.len()).saturating_add(32))
                }),
        )
}

fn validate_wire_frame(frame: &ObjectRpcChannelWireFrame) -> Result<(), NetworkError> {
    if frame.call_id().0 == 0 {
        return Err(NetworkError::Serialization {
            detail: "multiplexed object RPC call id cannot be zero".to_string(),
        });
    }
    match frame {
        ObjectRpcChannelWireFrame::Data { bytes, .. } => {
            if bytes.is_empty() || bytes.len() > OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES {
                return Err(NetworkError::Serialization {
                    detail: format!("invalid multiplexed data quantum {}", bytes.len()),
                });
            }
        }
        ObjectRpcChannelWireFrame::RequestAccepted { credit_bytes, .. } => {
            if *credit_bytes as usize != OBJECT_RPC_CHANNEL_CALL_WINDOW_BYTES {
                return Err(NetworkError::Serialization {
                    detail: "invalid multiplexed initial flow-control window".to_string(),
                });
            }
        }
        ObjectRpcChannelWireFrame::Cancel { reason, .. }
        | ObjectRpcChannelWireFrame::Reset { reason, .. }
            if reason.len() > MAX_CONTROL_HEAD_BYTES =>
        {
            return Err(channel_resource_error(
                "decode multiplexed cancellation detail",
                reason.len(),
            ));
        }
        _ => {}
    }
    Ok(())
}

fn validate_body_descriptor(
    descriptor: &ObjectRpcChannelBodyDescriptor,
) -> Result<(), NetworkError> {
    if descriptor.total_bytes == Some(0) {
        return Err(NetworkError::Serialization {
            detail: "multiplexed body descriptor cannot describe an empty body".to_string(),
        });
    }
    if descriptor.kind == ObjectRpcChannelBodyKind::Value
        && descriptor.total_bytes.is_none_or(|bytes| {
            usize::try_from(bytes).map_or(true, |bytes| {
                bytes > OBJECT_RPC_CHANNEL_MAX_MATERIALIZED_VALUE_BYTES
            })
        })
    {
        return Err(channel_resource_error(
            "accept multiplexed materialized value",
            descriptor
                .total_bytes
                .and_then(|bytes| usize::try_from(bytes).ok())
                .unwrap_or(usize::MAX),
        ));
    }
    Ok(())
}

fn validate_request_head(request: &ObjectRpcRequest) -> Result<(), NetworkError> {
    let bytes = request_head_bytes(request);
    if bytes > MAX_CONTROL_HEAD_BYTES {
        return Err(channel_resource_error(
            "encode multiplexed object RPC request head",
            bytes,
        ));
    }
    Ok(())
}

fn validate_response_head(response: &ObjectRpcResponse) -> Result<(), NetworkError> {
    let bytes = response_head_bytes(response);
    if bytes > MAX_CONTROL_HEAD_BYTES {
        return Err(channel_resource_error(
            "encode multiplexed object RPC response head",
            bytes,
        ));
    }
    Ok(())
}

fn channel_resource_error(operation: &str, requested_bytes: usize) -> NetworkError {
    NetworkError::ResourceExhausted {
        operation: operation.to_string(),
        requested_bytes,
    }
}

fn channel_delivery_error(detail: &str) -> NetworkError {
    NetworkError::delivery_failed(
        "multiplexed object RPC channel",
        NetworkDeliveryState::PossiblyApplied,
        detail,
    )
}

fn channel_lock_error() -> NetworkError {
    NetworkError::Io {
        operation: "access multiplexed object RPC channel state".to_string(),
        detail: "channel state lock poisoned".to_string(),
    }
}

fn unknown_call_error(call_id: ObjectRpcChannelCallId) -> NetworkError {
    NetworkError::Serialization {
        detail: format!("unknown multiplexed object RPC call {call_id}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::task::{Context, Poll};
    use std::time::Duration;
    use tokio::io::ReadBuf;
    use tokio::time::timeout;

    #[tokio::test]
    async fn streamed_data_and_body_end_retain_exact_write_timeline_ids() {
        let endpoint = ObjectRpcChannelEndpoint::new(
            ObjectRpcChannelRole::Dialer,
            ObjectRpcChannelConfig::default(),
        )
        .expect("endpoint");
        let trace = NetworkTimelineTraceContext {
            trace_id: [0x41; 16],
            span_id: [0x42; 8],
            trace_flags: 1,
        };
        let call_id = ObjectRpcChannelCallId(17);
        let data = ObjectRpcChannelWireFrame::Data {
            call_id,
            direction: ObjectRpcChannelBodyDirection::Request,
            offset: 4096,
            bytes: vec![7; 32],
        };
        let data_frame_id = channel_frame_timeline_id(&data);
        let (progress, _) = watch::channel(0);
        let emitted = endpoint
            .client
            .shared
            .outbound
            .queue_call_data(data.clone(), progress, Some(trace))
            .await
            .expect("queue data");
        let mut egress = endpoint.egress;
        assert_eq!(
            egress.next_transport_frame().await,
            Some(ObjectRpcChannelTransportFrame::Data(
                ObjectRpcChannelDataFrame::new(
                    call_id,
                    ObjectRpcChannelBodyDirection::Request,
                    4096,
                    Bytes::from(vec![7; 32]),
                )
                .expect("raw data frame"),
            ))
        );
        let timeline = egress.pending_write_timeline().expect("data timeline");
        assert_eq!(timeline.trace, trace);
        assert_eq!(timeline.call_id, call_id);
        assert_eq!(timeline.frame_id, data_frame_id);
        assert_ne!(timeline.frame_id, 0);
        assert_ne!(timeline.flags & network_timeline_flag::DATA_FRAME, 0);
        egress.complete_frame_write(true);
        emitted.await.expect("data written");

        let body_end = ObjectRpcChannelWireFrame::BodyEnd {
            call_id,
            direction: ObjectRpcChannelBodyDirection::Request,
            total_bytes: 4128,
        };
        let body_end_frame_id = channel_frame_timeline_id(&body_end);
        let acknowledged = endpoint
            .client
            .shared
            .outbound
            .try_control_with_ack_timeline(body_end.clone(), Some(trace))
            .expect("queue body end");
        assert_eq!(
            egress.next_transport_frame().await,
            Some(ObjectRpcChannelTransportFrame::Control(body_end))
        );
        let timeline = egress.pending_write_timeline().expect("body-end timeline");
        assert_eq!(timeline.frame_id, body_end_frame_id);
        assert_ne!(timeline.frame_id, data_frame_id);
        assert_ne!(timeline.flags & network_timeline_flag::CONTROL_FRAME, 0);
        egress.complete_frame_write(true);
        acknowledged.await.expect("body end written");
    }

    #[tokio::test]
    async fn send_window_uses_monotonic_cumulative_acknowledgements() {
        let window = SendWindow::new(OBJECT_RPC_CHANNEL_CALL_WINDOW_BYTES);
        let frame_count =
            OBJECT_RPC_CHANNEL_CALL_WINDOW_BYTES / OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES;
        for _ in 0..frame_count {
            assert_eq!(
                window.wait_available().await.expect("window capacity"),
                OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES
            );
            window
                .consume(OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES)
                .expect("consume window capacity");
        }
        assert!(
            timeout(Duration::from_millis(10), window.wait_available())
                .await
                .is_err(),
            "a full call window must block"
        );

        let cumulative_ack = 2 * OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES as u64;
        window
            .acknowledge(cumulative_ack)
            .expect("cumulative acknowledgement");
        assert_eq!(
            window
                .wait_available()
                .await
                .expect("capacity restored by cumulative acknowledgement"),
            OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES
        );
        window
            .acknowledge(cumulative_ack)
            .expect("duplicate cumulative acknowledgement is idempotent");
        assert!(
            window
                .acknowledge(OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES as u64)
                .is_err(),
            "acknowledgements must not move backwards"
        );
        assert!(
            window
                .acknowledge(OBJECT_RPC_CHANNEL_CALL_WINDOW_BYTES as u64 + 1)
                .is_err(),
            "acknowledgements must not pass the sent offset"
        );
    }

    #[tokio::test]
    async fn receive_window_coalesces_four_frames_into_one_cumulative_update() {
        let ObjectRpcChannelEndpoint {
            client: _,
            mut server,
            ingress,
            mut egress,
        } = ObjectRpcChannelEndpoint::new(
            ObjectRpcChannelRole::Acceptor,
            ObjectRpcChannelConfig::default(),
        )
        .expect("endpoint");
        let call_id = ObjectRpcChannelCallId(1);
        let total_bytes = OBJECT_RPC_CHANNEL_WINDOW_UPDATE_BYTES;
        ingress
            .receive(ObjectRpcChannelWireFrame::Request {
                call_id,
                request: request("/channel/cumulative-window", Vec::new()),
                body: Some(ObjectRpcChannelBodyDescriptor {
                    kind: ObjectRpcChannelBodyKind::Attachment,
                    total_bytes: Some(total_bytes as u64),
                }),
            })
            .expect("request ingress");
        assert!(matches!(
            egress.next_frame().await,
            Some(ObjectRpcChannelWireFrame::RequestAccepted {
                call_id: accepted,
                credit_bytes,
            }) if accepted == call_id
                && credit_bytes == OBJECT_RPC_CHANNEL_CALL_WINDOW_BYTES as u32
        ));
        let mut incoming = server.accept().await.expect("incoming request");
        let mut body = std::mem::take(&mut incoming.request.body);
        let reader = tokio::spawn(async move {
            let mut received = Vec::new();
            body.read_to_end(&mut received).await.map(|_| received)
        });

        for frame_index in 0..4 {
            ingress
                .receive(ObjectRpcChannelWireFrame::Data {
                    call_id,
                    direction: ObjectRpcChannelBodyDirection::Request,
                    offset: (frame_index * OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES) as u64,
                    bytes: vec![frame_index as u8; OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES],
                })
                .expect("request data");
        }
        ingress
            .receive(ObjectRpcChannelWireFrame::BodyEnd {
                call_id,
                direction: ObjectRpcChannelBodyDirection::Request,
                total_bytes: total_bytes as u64,
            })
            .expect("request body end");

        assert!(matches!(
            timeout(Duration::from_secs(1), egress.next_frame())
                .await
                .expect("window update timeout"),
            Some(ObjectRpcChannelWireFrame::WindowUpdate {
                call_id: updated,
                direction: ObjectRpcChannelBodyDirection::Request,
                received_offset,
            }) if updated == call_id && received_offset == total_bytes as u64
        ));
        assert_eq!(
            reader
                .await
                .expect("body reader task")
                .expect("read request body")
                .len(),
            total_bytes
        );
        assert!(
            timeout(Duration::from_millis(10), egress.next_frame())
                .await
                .is_err(),
            "the four-frame batch must not emit duplicate credit"
        );
    }

    #[derive(Default)]
    struct FrameCounts {
        requests: AtomicUsize,
        request_accepted: AtomicUsize,
        responses: AtomicUsize,
        data: AtomicUsize,
        window_updates: AtomicUsize,
        body_ends: AtomicUsize,
        cancels: AtomicUsize,
        resets: AtomicUsize,
    }

    impl FrameCounts {
        fn record(&self, frame: &ObjectRpcChannelWireFrame) {
            let counter = match frame {
                ObjectRpcChannelWireFrame::Request { .. } => &self.requests,
                ObjectRpcChannelWireFrame::RequestAccepted { .. } => &self.request_accepted,
                ObjectRpcChannelWireFrame::Response { .. } => &self.responses,
                ObjectRpcChannelWireFrame::Data { .. } => &self.data,
                ObjectRpcChannelWireFrame::WindowUpdate { .. } => &self.window_updates,
                ObjectRpcChannelWireFrame::BodyEnd { .. } => &self.body_ends,
                ObjectRpcChannelWireFrame::Cancel { .. } => &self.cancels,
                ObjectRpcChannelWireFrame::Reset { .. } => &self.resets,
            };
            counter.fetch_add(1, Ordering::Relaxed);
        }

        fn load(counter: &AtomicUsize) -> usize {
            counter.load(Ordering::Relaxed)
        }
    }

    struct LinkedEndpoints {
        left_client: ObjectRpcChannelClient,
        left_server: ObjectRpcChannelServer,
        right_client: ObjectRpcChannelClient,
        right_server: ObjectRpcChannelServer,
        counts: Arc<FrameCounts>,
        pumps: Vec<tokio::task::JoinHandle<Result<(), NetworkError>>>,
    }

    impl Drop for LinkedEndpoints {
        fn drop(&mut self) {
            for pump in &self.pumps {
                pump.abort();
            }
        }
    }

    fn linked_endpoints(config: ObjectRpcChannelConfig) -> LinkedEndpoints {
        let left = ObjectRpcChannelEndpoint::new(ObjectRpcChannelRole::Dialer, config.clone())
            .expect("left endpoint");
        let right = ObjectRpcChannelEndpoint::new(ObjectRpcChannelRole::Acceptor, config)
            .expect("right endpoint");
        let ObjectRpcChannelEndpoint {
            client: left_client,
            server: left_server,
            ingress: left_ingress,
            egress: mut left_egress,
        } = left;
        let ObjectRpcChannelEndpoint {
            client: right_client,
            server: right_server,
            ingress: right_ingress,
            egress: mut right_egress,
        } = right;
        let counts = Arc::new(FrameCounts::default());
        let left_counts = Arc::clone(&counts);
        let left_pump = tokio::spawn(async move {
            while let Some(frame) = left_egress.next_frame().await {
                left_counts.record(&frame);
                right_ingress.receive(frame)?;
            }
            Ok(())
        });
        let right_counts = Arc::clone(&counts);
        let right_pump = tokio::spawn(async move {
            while let Some(frame) = right_egress.next_frame().await {
                right_counts.record(&frame);
                left_ingress.receive(frame)?;
            }
            Ok(())
        });
        LinkedEndpoints {
            left_client,
            left_server,
            right_client,
            right_server,
            counts,
            pumps: vec![left_pump, right_pump],
        }
    }

    fn request(route: &str, body: Vec<u8>) -> ObjectRpcRequest {
        ObjectRpcRequest::from_encoded_body(route, "test", body)
    }

    fn response(route: &str, body: Vec<u8>) -> ObjectRpcResponse {
        ObjectRpcResponse::from_encoded_body(route, body)
    }

    fn patterned_body(bytes: usize, seed: u8) -> Vec<u8> {
        (0..bytes)
            .map(|index| seed.wrapping_add((index % 251) as u8))
            .collect()
    }

    fn assert_pattern(bytes: &[u8], seed: u8) {
        assert!(
            bytes
                .iter()
                .enumerate()
                .all(|(index, byte)| { *byte == seed.wrapping_add((index % 251) as u8) })
        );
    }

    struct ShortThenFullSyncReader {
        bytes: Vec<u8>,
        offset: usize,
        reads: usize,
    }

    impl Read for ShortThenFullSyncReader {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            if self.offset == self.bytes.len() {
                return Ok(0);
            }
            let limit = if self.reads == 0 {
                100
            } else {
                OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES
            };
            self.reads += 1;
            let read = limit.min(buffer.len()).min(self.bytes.len() - self.offset);
            buffer[..read].copy_from_slice(&self.bytes[self.offset..self.offset + read]);
            self.offset += read;
            Ok(read)
        }
    }

    struct ShortThenFullAsyncReader {
        bytes: Vec<u8>,
        offset: usize,
        reads: usize,
    }

    impl tokio::io::AsyncRead for ShortThenFullAsyncReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            buffer: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            if self.offset == self.bytes.len() {
                return Poll::Ready(Ok(()));
            }
            let limit = if self.reads == 0 {
                100
            } else {
                OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES
            };
            self.reads += 1;
            let read = limit
                .min(buffer.remaining())
                .min(self.bytes.len() - self.offset);
            buffer.put_slice(&self.bytes[self.offset..self.offset + read]);
            self.offset += read;
            Poll::Ready(Ok(()))
        }
    }

    struct PendingAsyncReader {
        polled: Arc<AtomicBool>,
        dropped: Arc<AtomicBool>,
    }

    impl tokio::io::AsyncRead for PendingAsyncReader {
        fn poll_read(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            _buffer: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            self.polled.store(true, Ordering::Release);
            Poll::Pending
        }
    }

    impl Drop for PendingAsyncReader {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Release);
        }
    }

    async fn wait_for_flag(flag: &AtomicBool, operation: &str) {
        timeout(Duration::from_secs(1), async {
            while !flag.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{operation} timeout"));
    }

    async fn wait_for_available_materialized_bytes(
        client: &ObjectRpcChannelClient,
        expected: usize,
    ) {
        timeout(Duration::from_secs(1), async {
            while client.shared.materialized_value_budget.available_permits() != expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("materialized-value permit release timeout");
    }

    async fn expect_call_progress(
        progress: &mut watch::Receiver<u64>,
        previous: u64,
        operation: &str,
    ) -> u64 {
        timeout(Duration::from_secs(1), async {
            loop {
                let generation = *progress.borrow_and_update();
                if generation > previous {
                    return generation;
                }
                progress
                    .changed()
                    .await
                    .unwrap_or_else(|_| panic!("{operation} progress sender closed"));
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{operation} progress timeout"))
    }

    async fn assert_no_call_progress(progress: &mut watch::Receiver<u64>, operation: &str) {
        match timeout(Duration::from_millis(25), progress.changed()).await {
            Err(_) => {}
            Ok(Ok(())) => panic!("{operation} unexpectedly advanced progress"),
            Ok(Err(_)) => panic!("{operation} progress sender unexpectedly closed"),
        }
    }

    async fn wait_for_active_calls(client: &ObjectRpcChannelClient, expected: usize) {
        timeout(Duration::from_secs(1), async {
            while client.queue_snapshot().active_calls != expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("active call count timeout");
    }

    async fn assert_linked_channel_accepts_replacement(
        endpoints: &mut LinkedEndpoints,
        route: &str,
    ) {
        let pending = endpoints
            .left_client
            .call(ObjectRpcSessionCall::value(request(route, Vec::new())))
            .await
            .expect("replacement call");
        let inbound = timeout(Duration::from_secs(1), endpoints.right_server.accept())
            .await
            .expect("replacement request timeout")
            .expect("replacement request");
        inbound
            .respond(ObjectRpcSessionResponse::value(response(
                route,
                b"ok".to_vec(),
            )))
            .await
            .expect("replacement response");
        let result = timeout(Duration::from_secs(1), pending.result())
            .await
            .expect("replacement result timeout")
            .expect("replacement result");
        assert_eq!(result.response.body, b"ok");
        wait_for_active_calls(&endpoints.left_client, 0).await;
        wait_for_active_calls(&endpoints.right_client, 0).await;
    }

    async fn assert_body_bytes_then_error(
        body: &mut ObjectRpcIncomingBody,
        expected: &[u8],
        expected_error: &str,
    ) {
        let mut delivered = vec![0_u8; expected.len()];
        timeout(Duration::from_secs(1), body.read_exact(&mut delivered))
            .await
            .expect("queued body read timeout")
            .expect("queued body bytes");
        assert_eq!(delivered, expected);
        let error = timeout(Duration::from_secs(1), body.read_u8())
            .await
            .expect("terminal body error timeout")
            .expect_err("terminal body failure must not become clean EOF");
        assert!(
            error.to_string().contains(expected_error),
            "unexpected terminal body error: {error}"
        );
    }

    async fn assert_short_reader_transfers_exactly(source: ObjectRpcBodySource, expected: Vec<u8>) {
        let mut endpoints = linked_endpoints(ObjectRpcChannelConfig::default());
        let pending = endpoints
            .left_client
            .call(ObjectRpcSessionCall::new(
                request("/channel/short-reader", Vec::new()),
                source,
            ))
            .await
            .expect("short-reader call");
        let mut incoming = timeout(Duration::from_secs(1), endpoints.right_server.accept())
            .await
            .expect("short-reader accept timeout")
            .expect("short-reader request");
        let mut received = Vec::new();
        timeout(
            Duration::from_secs(2),
            incoming.request.body.read_to_end(&mut received),
        )
        .await
        .expect("short-reader body timeout")
        .expect("short-reader body");
        assert_eq!(received, expected);
        incoming
            .respond(ObjectRpcSessionResponse::value(response(
                "/channel/short-reader",
                b"ok".to_vec(),
            )))
            .await
            .expect("short-reader response");
        let result = timeout(Duration::from_secs(1), pending.result())
            .await
            .expect("short-reader result timeout")
            .expect("short-reader result");
        assert_eq!(result.response.body, b"ok");
    }

    #[tokio::test]
    async fn inline_full_duplex_uses_one_request_and_response_each() {
        let mut endpoints = linked_endpoints(ObjectRpcChannelConfig::default());
        let left_call = endpoints
            .left_client
            .call(ObjectRpcSessionCall::value(request(
                "/channel/left",
                b"left".to_vec(),
            )))
            .await
            .expect("queue left call");
        let right_call = endpoints
            .right_client
            .call(ObjectRpcSessionCall::value(request(
                "/channel/right",
                b"right".to_vec(),
            )))
            .await
            .expect("queue right call");

        let left_request = timeout(Duration::from_secs(1), endpoints.left_server.accept())
            .await
            .expect("left server timeout")
            .expect("left server request");
        assert_eq!(left_request.request.request.body, b"right");
        left_request
            .respond(ObjectRpcSessionResponse::value(response(
                "/channel/right",
                b"from-left".to_vec(),
            )))
            .await
            .expect("left response");

        let right_request = timeout(Duration::from_secs(1), endpoints.right_server.accept())
            .await
            .expect("right server timeout")
            .expect("right server request");
        assert_eq!(right_request.request.request.body, b"left");
        right_request
            .respond(ObjectRpcSessionResponse::value(response(
                "/channel/left",
                b"from-right".to_vec(),
            )))
            .await
            .expect("right response");

        let left_result = timeout(Duration::from_secs(1), left_call.result())
            .await
            .expect("left result timeout")
            .expect("left result");
        let right_result = timeout(Duration::from_secs(1), right_call.result())
            .await
            .expect("right result timeout")
            .expect("right result");
        assert_eq!(left_result.response.body, b"from-right");
        assert_eq!(right_result.response.body, b"from-left");
        assert!(left_result.body.is_empty());
        assert!(right_result.body.is_empty());

        assert_eq!(FrameCounts::load(&endpoints.counts.requests), 2);
        assert_eq!(FrameCounts::load(&endpoints.counts.responses), 2);
        assert_eq!(FrameCounts::load(&endpoints.counts.request_accepted), 0);
        assert_eq!(FrameCounts::load(&endpoints.counts.data), 0);
        assert_eq!(FrameCounts::load(&endpoints.counts.window_updates), 0);
        assert_eq!(FrameCounts::load(&endpoints.counts.body_ends), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_call_admission_preserves_request_id_order_for_both_roles() {
        for role in [ObjectRpcChannelRole::Dialer, ObjectRpcChannelRole::Acceptor] {
            let ObjectRpcChannelEndpoint {
                client,
                server: _server,
                ingress: _ingress,
                mut egress,
            } = ObjectRpcChannelEndpoint::new(role, ObjectRpcChannelConfig::default())
                .expect("channel endpoint");
            let barrier = Arc::new(tokio::sync::Barrier::new(OBJECT_RPC_CHANNEL_MAX_CALLS + 1));
            let mut tasks = tokio::task::JoinSet::new();
            for index in 0..OBJECT_RPC_CHANNEL_MAX_CALLS {
                let client = client.clone();
                let barrier = Arc::clone(&barrier);
                tasks.spawn(async move {
                    barrier.wait().await;
                    client
                        .call(ObjectRpcSessionCall::value(request(
                            &format!("/channel/concurrent-admission/{index}"),
                            Vec::new(),
                        )))
                        .await
                });
            }
            barrier.wait().await;

            // Retain every pending call until all Request heads are drained;
            // dropping one would enqueue a Cancel control frame into the same
            // lane and obscure the admission-order assertion.
            let mut pending = Vec::with_capacity(OBJECT_RPC_CHANNEL_MAX_CALLS);
            while let Some(joined) = tasks.join_next().await {
                pending.push(joined.expect("call task").expect("open concurrent call"));
            }

            let mut expected = role.first_call_id();
            for _ in 0..OBJECT_RPC_CHANNEL_MAX_CALLS {
                let call_id = match egress.next_frame().await.expect("request frame") {
                    ObjectRpcChannelWireFrame::Request { call_id, .. } => call_id,
                    frame => panic!("expected request, got {frame:?}"),
                };
                assert_eq!(call_id, ObjectRpcChannelCallId(expected));
                expected = expected.checked_add(2).expect("call id sequence");
            }
            drop(pending);
        }
    }

    #[tokio::test]
    async fn higher_call_can_retire_before_an_active_lower_call_completes() {
        let ObjectRpcChannelEndpoint {
            client,
            server: _server,
            ingress,
            mut egress,
        } = ObjectRpcChannelEndpoint::new(
            ObjectRpcChannelRole::Dialer,
            ObjectRpcChannelConfig::default(),
        )
        .expect("endpoint");

        let lower = client
            .call(ObjectRpcSessionCall::value(request(
                "/channel/out-of-order/lower",
                Vec::new(),
            )))
            .await
            .expect("lower call");
        let lower_id = match egress.next_frame().await.expect("lower request") {
            ObjectRpcChannelWireFrame::Request { call_id, .. } => call_id,
            frame => panic!("expected lower request, got {frame:?}"),
        };
        let higher = client
            .call(ObjectRpcSessionCall::value(request(
                "/channel/out-of-order/higher",
                Vec::new(),
            )))
            .await
            .expect("higher call");
        let higher_id = match egress.next_frame().await.expect("higher request") {
            ObjectRpcChannelWireFrame::Request { call_id, .. } => call_id,
            frame => panic!("expected higher request, got {frame:?}"),
        };

        ingress
            .receive(ObjectRpcChannelWireFrame::Response {
                call_id: higher_id,
                response: response("/channel/out-of-order/higher", b"higher".to_vec()),
                body: None,
                initial_credit_bytes: 0,
            })
            .expect("higher response");
        assert_eq!(
            higher.result().await.expect("higher result").response.body,
            b"higher"
        );
        assert!(
            client
                .shared
                .retired_local_high_watermark
                .load(Ordering::Acquire)
                >= higher_id.0
        );

        ingress
            .receive(ObjectRpcChannelWireFrame::Response {
                call_id: lower_id,
                response: response("/channel/out-of-order/lower", b"lower".to_vec()),
                body: None,
                initial_credit_bytes: 0,
            })
            .expect("active lower response");
        assert_eq!(
            lower.result().await.expect("lower result").response.body,
            b"lower"
        );
        assert!(!client.is_closed());
    }

    #[tokio::test]
    async fn progress_is_isolated_between_logical_calls() {
        let ObjectRpcChannelEndpoint {
            client,
            server: _server,
            ingress,
            mut egress,
        } = ObjectRpcChannelEndpoint::new(
            ObjectRpcChannelRole::Dialer,
            ObjectRpcChannelConfig::default(),
        )
        .expect("endpoint");

        let mut call_a = client
            .call(ObjectRpcSessionCall::value(request(
                "/channel/progress/a",
                Vec::new(),
            )))
            .await
            .expect("call a");
        let mut progress_a = call_a.progress();
        let initial_a = *progress_a.borrow_and_update();
        let call_a_id = match egress.next_frame().await.expect("call a request") {
            ObjectRpcChannelWireFrame::Request { call_id, .. } => call_id,
            frame => panic!("expected call a request, got {frame:?}"),
        };
        let generation_a =
            expect_call_progress(&mut progress_a, initial_a, "call a request head").await;

        let call_b = client
            .call(ObjectRpcSessionCall::value(request(
                "/channel/progress/b",
                Vec::new(),
            )))
            .await
            .expect("call b");
        let mut progress_b = call_b.progress();
        let initial_b = *progress_b.borrow_and_update();
        let call_b_id = match egress.next_frame().await.expect("call b request") {
            ObjectRpcChannelWireFrame::Request { call_id, .. } => call_id,
            frame => panic!("expected call b request, got {frame:?}"),
        };
        let generation_b =
            expect_call_progress(&mut progress_b, initial_b, "call b request head").await;
        assert_no_call_progress(&mut progress_a, "call b request head on call a").await;

        ingress
            .receive(ObjectRpcChannelWireFrame::Response {
                call_id: call_b_id,
                response: response("/channel/progress/b", b"ok".to_vec()),
                body: None,
                initial_credit_bytes: 0,
            })
            .expect("call b response");
        let _ = expect_call_progress(&mut progress_b, generation_b, "call b response head").await;
        assert_no_call_progress(&mut progress_a, "call b response head on call a").await;
        assert_eq!(
            call_b.result().await.expect("call b result").response.body,
            b"ok"
        );

        call_a.cancel("isolation test complete");
        let _ = expect_call_progress(&mut progress_a, generation_a, "call a cancellation").await;
        assert!(matches!(
            egress.next_frame().await.expect("call a cancel"),
            ObjectRpcChannelWireFrame::Cancel { call_id, .. } if call_id == call_a_id
        ));
    }

    #[tokio::test]
    async fn streaming_body_frames_advance_call_progress() {
        let ObjectRpcChannelEndpoint {
            client,
            server: _server,
            ingress,
            mut egress,
        } = ObjectRpcChannelEndpoint::new(
            ObjectRpcChannelRole::Dialer,
            ObjectRpcChannelConfig::default(),
        )
        .expect("endpoint");
        let body_bytes = 2 * OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES + 7;
        let mut pending = client
            .call(ObjectRpcSessionCall::new(
                request("/channel/progress/stream", Vec::new()),
                ObjectRpcBodySource::bytes(vec![3; body_bytes]),
            ))
            .await
            .expect("streaming call");
        let mut progress = pending.progress();
        let mut generation = *progress.borrow_and_update();
        let call_id = match egress.next_frame().await.expect("request head") {
            ObjectRpcChannelWireFrame::Request { call_id, .. } => call_id,
            frame => panic!("expected request head, got {frame:?}"),
        };
        generation = expect_call_progress(&mut progress, generation, "request head").await;

        ingress
            .receive(ObjectRpcChannelWireFrame::RequestAccepted {
                call_id,
                credit_bytes: OBJECT_RPC_CHANNEL_CALL_WINDOW_BYTES as u32,
            })
            .expect("request accepted");
        generation = expect_call_progress(&mut progress, generation, "request accepted").await;

        for offset in [0, OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES] {
            assert!(matches!(
                egress.next_frame().await.expect("request data"),
                ObjectRpcChannelWireFrame::Data {
                    call_id: frame_call_id,
                    direction: ObjectRpcChannelBodyDirection::Request,
                    offset: frame_offset,
                    ..
                } if frame_call_id == call_id && frame_offset == offset as u64
            ));
            generation = expect_call_progress(&mut progress, generation, "request data").await;
            ingress
                .receive(ObjectRpcChannelWireFrame::WindowUpdate {
                    call_id,
                    direction: ObjectRpcChannelBodyDirection::Request,
                    received_offset: (offset + OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES) as u64,
                })
                .expect("request window update");
            generation =
                expect_call_progress(&mut progress, generation, "request window update").await;
        }

        assert!(matches!(
            egress.next_frame().await.expect("final request data"),
            ObjectRpcChannelWireFrame::Data {
                call_id: frame_call_id,
                direction: ObjectRpcChannelBodyDirection::Request,
                offset,
                bytes,
            } if frame_call_id == call_id
                && offset == (2 * OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES) as u64
                && bytes.len() == 7
        ));
        generation = expect_call_progress(&mut progress, generation, "final request data").await;
        assert!(matches!(
            egress.next_frame().await.expect("request body end"),
            ObjectRpcChannelWireFrame::BodyEnd {
                call_id: frame_call_id,
                direction: ObjectRpcChannelBodyDirection::Request,
                total_bytes,
                ..
            } if frame_call_id == call_id && total_bytes == body_bytes as u64
        ));
        generation = expect_call_progress(&mut progress, generation, "request body end").await;

        pending.cancel("streaming progress test complete");
        let _ =
            expect_call_progress(&mut progress, generation, "streaming call cancellation").await;
    }

    #[tokio::test]
    async fn full_body_queue_cancellation_is_nonblocking_and_not_clean_eof() {
        let ObjectRpcChannelEndpoint {
            client,
            mut server,
            ingress,
            mut egress,
        } = ObjectRpcChannelEndpoint::new(
            ObjectRpcChannelRole::Dialer,
            ObjectRpcChannelConfig::default(),
        )
        .expect("endpoint");
        let call_id = ObjectRpcChannelCallId(2);
        let payload = b"queued".to_vec();
        ingress
            .receive(ObjectRpcChannelWireFrame::Request {
                call_id,
                request: request("/channel/full-queue/cancel", Vec::new()),
                body: Some(ObjectRpcChannelBodyDescriptor {
                    kind: ObjectRpcChannelBodyKind::Attachment,
                    total_bytes: Some(payload.len() as u64),
                }),
            })
            .expect("request ingress");
        assert!(matches!(
            egress.next_frame().await.expect("request accepted"),
            ObjectRpcChannelWireFrame::RequestAccepted { call_id: accepted, .. }
                if accepted == call_id
        ));
        let mut incoming = server.accept().await.expect("inbound request");
        let mut body = std::mem::take(&mut incoming.request.body);

        ingress
            .receive(ObjectRpcChannelWireFrame::Data {
                call_id,
                direction: ObjectRpcChannelBodyDirection::Request,
                offset: 0,
                bytes: payload.clone(),
            })
            .expect("request data");
        assert!(matches!(
            egress.next_frame().await.expect("request window update"),
            ObjectRpcChannelWireFrame::WindowUpdate { call_id: updated, .. }
                if updated == call_id
        ));

        ingress
            .receive(ObjectRpcChannelWireFrame::Cancel {
                call_id,
                reason: "forced cancellation".to_string(),
            })
            .expect("cancel full queued body");
        wait_for_active_calls(&client, 0).await;
        assert_body_bytes_then_error(&mut body, &payload, "body cancelled").await;
    }

    #[tokio::test]
    async fn response_window_update_queue_failure_aborts_client_call_only() {
        let config = ObjectRpcChannelConfig {
            control_queue_depth: 1,
            ..ObjectRpcChannelConfig::default()
        };
        let ObjectRpcChannelEndpoint {
            client,
            server: _server,
            ingress,
            mut egress,
        } = ObjectRpcChannelEndpoint::new(ObjectRpcChannelRole::Dialer, config).expect("endpoint");
        let pending = client
            .call(ObjectRpcSessionCall::value(request(
                "/channel/response-credit-failure",
                Vec::new(),
            )))
            .await
            .expect("client call");
        let call_id = match egress.next_frame().await.expect("request head") {
            ObjectRpcChannelWireFrame::Request { call_id, .. } => call_id,
            frame => panic!("expected request head, got {frame:?}"),
        };
        let payload = b"response".to_vec();
        ingress
            .receive(ObjectRpcChannelWireFrame::Response {
                call_id,
                response: response("/channel/response-credit-failure", Vec::new()),
                body: Some(ObjectRpcChannelBodyDescriptor {
                    kind: ObjectRpcChannelBodyKind::Attachment,
                    total_bytes: Some(payload.len() as u64),
                }),
                initial_credit_bytes: OBJECT_RPC_CHANNEL_CALL_WINDOW_BYTES as u32,
            })
            .expect("response head");
        let mut result = pending.result().await.expect("response result");

        let filler_id = ObjectRpcChannelCallId(u64::MAX - 1);
        egress
            .shared
            .outbound
            .try_control(ObjectRpcChannelWireFrame::Cancel {
                call_id: filler_id,
                reason: "fill control queue".to_string(),
            })
            .expect("fill control queue");
        ingress
            .receive(ObjectRpcChannelWireFrame::Data {
                call_id,
                direction: ObjectRpcChannelBodyDirection::Response,
                offset: 0,
                bytes: payload.clone(),
            })
            .expect("response data");

        wait_for_active_calls(&client, 0).await;
        assert!(!client.is_closed());
        assert_body_bytes_then_error(&mut result.body, &payload, "body credit failed").await;
        assert!(matches!(
            egress.next_frame().await.expect("control queue filler"),
            ObjectRpcChannelWireFrame::Cancel { call_id, .. } if call_id == filler_id
        ));
    }

    #[tokio::test]
    async fn short_then_full_sync_reader_respects_remaining_credit() {
        let expected = patterned_body(100 + OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES + 257, 31);
        let source = ObjectRpcBodySource::from_reader(
            ShortThenFullSyncReader {
                bytes: expected.clone(),
                offset: 0,
                reads: 0,
            },
            Some(expected.len() as u64),
        );
        assert_short_reader_transfers_exactly(source, expected).await;
    }

    #[tokio::test]
    async fn short_then_full_async_reader_respects_remaining_credit() {
        let expected = patterned_body(100 + OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES + 257, 47);
        let source = ObjectRpcBodySource::async_reader(
            ShortThenFullAsyncReader {
                bytes: expected.clone(),
                offset: 0,
                reads: 0,
            },
            Some(expected.len() as u64),
        );
        assert_short_reader_transfers_exactly(source, expected).await;
    }

    #[tokio::test]
    async fn cancelling_call_drops_permanently_pending_async_reader() {
        let mut endpoints = linked_endpoints(ObjectRpcChannelConfig::default());
        let polled = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicBool::new(false));
        let mut pending = endpoints
            .left_client
            .call(ObjectRpcSessionCall::new(
                request("/channel/pending-reader", Vec::new()),
                ObjectRpcBodySource::async_reader(
                    PendingAsyncReader {
                        polled: Arc::clone(&polled),
                        dropped: Arc::clone(&dropped),
                    },
                    None,
                ),
            ))
            .await
            .expect("pending-reader call");
        let inbound = timeout(Duration::from_secs(1), endpoints.right_server.accept())
            .await
            .expect("pending-reader accept timeout")
            .expect("pending-reader request");
        let mut cancellation = inbound.cancellation();
        wait_for_flag(&polled, "poll pending reader").await;

        pending.cancel("cancel pending reader");

        wait_for_flag(&dropped, "drop pending reader").await;
        if cancellation.borrow().is_none() {
            timeout(Duration::from_secs(1), cancellation.changed())
                .await
                .expect("pending-reader cancellation timeout")
                .expect("pending-reader cancellation sender");
        }
        assert_eq!(
            cancellation.borrow().as_deref(),
            Some("cancel pending reader")
        );
        assert_eq!(endpoints.left_client.queue_snapshot().active_calls, 0);
        assert_eq!(endpoints.right_client.queue_snapshot().active_calls, 0);
    }

    #[tokio::test]
    async fn streams_more_than_one_mebibyte_in_both_directions() {
        let mut endpoints = linked_endpoints(ObjectRpcChannelConfig::default());
        let request_bytes = 1024 * 1024 + 19_337;
        let response_bytes = 1024 * 1024 + 41_111;
        let pending = endpoints
            .left_client
            .call(ObjectRpcSessionCall::new(
                request("/channel/stream", Vec::new()),
                ObjectRpcBodySource::bytes(patterned_body(request_bytes, 17)),
            ))
            .await
            .expect("queue streaming call");

        let mut incoming = timeout(Duration::from_secs(2), endpoints.right_server.accept())
            .await
            .expect("streaming request timeout")
            .expect("streaming request");
        let mut received_request = Vec::new();
        let request_read = timeout(
            Duration::from_secs(5),
            incoming.request.body.read_to_end(&mut received_request),
        )
        .await;
        if request_read.is_err() {
            panic!(
                "request body timeout after {} bytes; requests={} accepted={} data={} windows={} ends={} pumps_finished={:?}",
                received_request.len(),
                FrameCounts::load(&endpoints.counts.requests),
                FrameCounts::load(&endpoints.counts.request_accepted),
                FrameCounts::load(&endpoints.counts.data),
                FrameCounts::load(&endpoints.counts.window_updates),
                FrameCounts::load(&endpoints.counts.body_ends),
                endpoints
                    .pumps
                    .iter()
                    .map(tokio::task::JoinHandle::is_finished)
                    .collect::<Vec<_>>()
            );
        }
        request_read
            .expect("checked timeout above")
            .expect("read request body");
        assert_eq!(received_request.len(), request_bytes);
        assert_pattern(&received_request, 17);

        incoming
            .respond(ObjectRpcSessionResponse::new(
                response("/channel/stream", Vec::new()),
                ObjectRpcBodySource::bytes(patterned_body(response_bytes, 93)),
            ))
            .await
            .expect("streaming response");
        let mut result = timeout(Duration::from_secs(5), pending.result())
            .await
            .expect("streaming result timeout")
            .expect("streaming result");
        let mut received_response = Vec::new();
        timeout(
            Duration::from_secs(5),
            result.body.read_to_end(&mut received_response),
        )
        .await
        .expect("response body timeout")
        .expect("read response body");
        assert_eq!(received_response.len(), response_bytes);
        assert_pattern(&received_response, 93);
        assert_eq!(FrameCounts::load(&endpoints.counts.requests), 1);
        assert_eq!(FrameCounts::load(&endpoints.counts.request_accepted), 1);
        assert_eq!(FrameCounts::load(&endpoints.counts.responses), 1);
        assert_eq!(FrameCounts::load(&endpoints.counts.body_ends), 2);
        let expected_data_frames = request_bytes.div_ceil(OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES)
            + response_bytes.div_ceil(OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES);
        assert_eq!(
            FrameCounts::load(&endpoints.counts.data),
            expected_data_frames
        );
        let window_updates = FrameCounts::load(&endpoints.counts.window_updates);
        assert!(window_updates > 0);
        assert!(window_updates <= FrameCounts::load(&endpoints.counts.data));
    }

    #[tokio::test]
    async fn single_quantum_queue_streams_multi_quantum_body_without_batch_deadlock() {
        let config = ObjectRpcChannelConfig {
            max_calls: OBJECT_RPC_CHANNEL_MAX_CALLS,
            max_queued_data_bytes: OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES,
            control_queue_depth: DEFAULT_CONTROL_QUEUE_DEPTH,
            data_queue_depth: 1,
        };
        let mut endpoints = linked_endpoints(config);
        let request_bytes = 3 * OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES + 17;
        let expected = patterned_body(request_bytes, 29);
        let pending = endpoints
            .left_client
            .call(ObjectRpcSessionCall::new(
                request("/channel/single-quantum-queue", Vec::new()),
                ObjectRpcBodySource::bytes(expected.clone()),
            ))
            .await
            .expect("queue constrained streaming call");

        let mut incoming = timeout(Duration::from_secs(1), endpoints.right_server.accept())
            .await
            .expect("constrained streaming request timeout")
            .expect("constrained streaming request");
        let mut received = Vec::new();
        timeout(
            Duration::from_secs(5),
            incoming.request.body.read_to_end(&mut received),
        )
        .await
        .expect("constrained request body timeout")
        .expect("read constrained request body");
        assert_eq!(received, expected);

        incoming
            .respond(ObjectRpcSessionResponse::value(response(
                "/channel/single-quantum-queue",
                b"ok".to_vec(),
            )))
            .await
            .expect("respond to constrained streaming call");
        let result = timeout(Duration::from_secs(1), pending.result())
            .await
            .expect("constrained streaming result timeout")
            .expect("constrained streaming result");
        assert_eq!(result.response.body, b"ok");

        assert_eq!(
            FrameCounts::load(&endpoints.counts.data),
            request_bytes.div_ceil(OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES)
        );
        wait_for_active_calls(&endpoints.left_client, 0).await;
        wait_for_active_calls(&endpoints.right_client, 0).await;
        assert_eq!(endpoints.left_client.queue_snapshot().queued_data_bytes, 0);
    }

    #[tokio::test]
    async fn materialized_response_value_is_restreamed_across_a_second_channel() {
        let route = "/channel/materialized-response-forward";
        let expected = patterned_body(1024 * 1024 + 257, 37);
        let mut target_hop = linked_endpoints(ObjectRpcChannelConfig::default());
        let target_pending = target_hop
            .left_client
            .call(ObjectRpcSessionCall::value(request(route, Vec::new())))
            .await
            .expect("queue target-hop call");
        let target_request = timeout(Duration::from_secs(1), target_hop.right_server.accept())
            .await
            .expect("target-hop request timeout")
            .expect("target-hop request");
        target_request
            .respond(ObjectRpcSessionResponse::value(response(
                route,
                expected.clone(),
            )))
            .await
            .expect("stream target-hop value response");
        let target_result = timeout(Duration::from_secs(5), target_pending.result())
            .await
            .expect("target-hop result timeout")
            .expect("target-hop result");
        assert!(
            target_result.response.body.as_slice() == expected.as_slice(),
            "target hop returned a mismatched materialized value"
        );

        let mut origin_hop = linked_endpoints(ObjectRpcChannelConfig::default());
        let origin_pending = origin_hop
            .left_client
            .call(ObjectRpcSessionCall::value(request(route, Vec::new())))
            .await
            .expect("queue origin-hop call");
        let origin_request = timeout(Duration::from_secs(1), origin_hop.right_server.accept())
            .await
            .expect("origin-hop request timeout")
            .expect("origin-hop request");
        origin_request
            .respond(ObjectRpcSessionResponse::new(
                target_result.response,
                ObjectRpcBodySource::from_incoming_body(target_result.body),
            ))
            .await
            .expect("re-stream materialized response across origin hop");
        let origin_result = timeout(Duration::from_secs(5), origin_pending.result())
            .await
            .expect("origin-hop result timeout")
            .expect("origin-hop result");
        assert!(
            origin_result.response.body.as_slice() == expected.as_slice(),
            "origin hop returned a mismatched re-streamed value"
        );
        assert!(FrameCounts::load(&target_hop.counts.data) > 0);
        assert!(FrameCounts::load(&origin_hop.counts.data) > 0);
    }

    #[tokio::test]
    async fn literal_materialized_response_prefers_the_restored_typed_value() {
        let route = "/channel/materialized-value-literal";
        let expected = patterned_body(96 * 1024 + 17, 43);
        let materialized = ObjectRpcSessionResult::materialized_channel_value(
            response(route, expected.clone()),
            u64::try_from(expected.len()).ok(),
        );

        // `ObjectRpcSessionResponse` has public fields for compatibility, so
        // callers can bypass its normalizing constructor. The restored typed
        // head must still take precedence over the empty forwarding marker.
        let (head, outgoing) = prepare_channel_response(ObjectRpcSessionResponse {
            response: materialized.response,
            body: ObjectRpcBodySource::from_incoming_body(materialized.body),
        })
        .expect("prepare literal materialized response");
        assert!(head.body.is_empty());
        let outgoing = outgoing.expect("large typed value should be streamed");
        assert!(matches!(
            outgoing.descriptor.kind,
            ObjectRpcChannelBodyKind::Value
        ));
        assert_eq!(
            outgoing.descriptor.total_bytes,
            u64::try_from(expected.len()).ok()
        );
        let restored = outgoing
            .source
            .collect_to_vec(expected.len())
            .await
            .expect("collect re-streamed literal value");
        assert!(
            restored.as_slice() == expected.as_slice(),
            "literal response prepared a mismatched value"
        );
    }

    #[tokio::test]
    async fn early_response_discards_unread_request_tail() {
        let mut endpoints = linked_endpoints(ObjectRpcChannelConfig::default());
        let request_bytes = 3 * OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES + 17;
        let pending = endpoints
            .left_client
            .call(ObjectRpcSessionCall::new(
                request("/channel/early-response", Vec::new()),
                ObjectRpcBodySource::bytes(patterned_body(request_bytes, 61)),
            ))
            .await
            .expect("queue early-response call");
        let incoming = timeout(Duration::from_secs(1), endpoints.right_server.accept())
            .await
            .expect("early-response request timeout")
            .expect("early-response request");
        incoming
            .respond(ObjectRpcSessionResponse::value(response(
                "/channel/early-response",
                b"accepted".to_vec(),
            )))
            .await
            .expect("early response");
        drop(incoming);

        let result = timeout(Duration::from_secs(2), pending.result())
            .await
            .expect("early response result timeout")
            .expect("early response result");
        assert_eq!(result.response.body, b"accepted");
        wait_for_active_calls(&endpoints.left_client, 0).await;
        wait_for_active_calls(&endpoints.right_client, 0).await;
        let expected_quanta = request_bytes.div_ceil(OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES);
        assert_eq!(FrameCounts::load(&endpoints.counts.data), expected_quanta);
        let window_updates = FrameCounts::load(&endpoints.counts.window_updates);
        assert!(window_updates <= expected_quanta);
        assert_eq!(FrameCounts::load(&endpoints.counts.body_ends), 1);

        let final_call = endpoints
            .left_client
            .call(ObjectRpcSessionCall::value(request(
                "/channel/early-response/final",
                Vec::new(),
            )))
            .await
            .expect("carrier remains usable");
        let final_request = endpoints
            .right_server
            .accept()
            .await
            .expect("final request");
        final_request
            .respond(ObjectRpcSessionResponse::value(response(
                "/channel/early-response/final",
                b"ok".to_vec(),
            )))
            .await
            .expect("final response");
        assert_eq!(
            final_call
                .result()
                .await
                .expect("final result")
                .response
                .body,
            b"ok"
        );
    }

    #[tokio::test]
    async fn channel_rejects_the_sixty_fifth_active_call() {
        let endpoint = ObjectRpcChannelEndpoint::new(
            ObjectRpcChannelRole::Dialer,
            ObjectRpcChannelConfig::default(),
        )
        .expect("endpoint");
        let mut pending = Vec::with_capacity(OBJECT_RPC_CHANNEL_MAX_CALLS);
        for index in 0..OBJECT_RPC_CHANNEL_MAX_CALLS {
            pending.push(
                endpoint
                    .client
                    .call(ObjectRpcSessionCall::value(request(
                        &format!("/channel/bound/{index}"),
                        Vec::new(),
                    )))
                    .await
                    .expect("call within bound"),
            );
        }
        let overflow = endpoint
            .client
            .call(ObjectRpcSessionCall::value(request(
                "/channel/bound/overflow",
                Vec::new(),
            )))
            .await;
        assert!(matches!(
            overflow,
            Err(NetworkError::ResourceExhausted { .. })
        ));
        assert_eq!(
            endpoint.client.queue_snapshot().active_calls,
            OBJECT_RPC_CHANNEL_MAX_CALLS
        );
        drop(pending);
    }

    #[tokio::test]
    async fn sixty_four_timed_out_results_cancel_and_release_every_slot() {
        let mut endpoints = linked_endpoints(ObjectRpcChannelConfig::default());
        for index in 0..OBJECT_RPC_CHANNEL_MAX_CALLS {
            let pending = endpoints
                .left_client
                .call(ObjectRpcSessionCall::value(request(
                    &format!("/channel/timeout/{index}"),
                    Vec::new(),
                )))
                .await
                .expect("call within bound");
            let inbound = timeout(Duration::from_secs(1), endpoints.right_server.accept())
                .await
                .expect("server accept timeout")
                .expect("server request");
            let mut cancellation = inbound.cancellation();
            timeout(Duration::from_millis(1), pending.result())
                .await
                .expect_err("result must time out");
            if cancellation.borrow().is_none() {
                timeout(Duration::from_secs(1), cancellation.changed())
                    .await
                    .expect("cancellation timeout")
                    .expect("cancellation sender");
            }
            assert!(cancellation.borrow().is_some());
            drop(inbound);
            assert_eq!(endpoints.left_client.queue_snapshot().active_calls, 0);
            assert_eq!(endpoints.right_client.queue_snapshot().active_calls, 0);
        }

        let pending = endpoints
            .left_client
            .call(ObjectRpcSessionCall::value(request(
                "/channel/after-timeouts",
                b"still-open".to_vec(),
            )))
            .await
            .expect("slot after cancellations");
        let inbound = timeout(Duration::from_secs(1), endpoints.right_server.accept())
            .await
            .expect("final server accept timeout")
            .expect("final server request");
        inbound
            .respond(ObjectRpcSessionResponse::value(response(
                "/channel/after-timeouts",
                b"ok".to_vec(),
            )))
            .await
            .expect("final response");
        let result = timeout(Duration::from_secs(1), pending.result())
            .await
            .expect("final result timeout")
            .expect("final result");
        assert_eq!(result.response.body, b"ok");
    }

    #[tokio::test]
    async fn late_frames_for_cancelled_call_are_benign_and_unrelated_call_completes() {
        let ObjectRpcChannelEndpoint {
            client,
            server: _server,
            ingress,
            mut egress,
        } = ObjectRpcChannelEndpoint::new(
            ObjectRpcChannelRole::Dialer,
            ObjectRpcChannelConfig::default(),
        )
        .expect("endpoint");

        let cancelled = client
            .call(ObjectRpcSessionCall::value(request(
                "/channel/cancelled",
                Vec::new(),
            )))
            .await
            .expect("cancelled call");
        let cancelled_id = match egress.next_frame().await.expect("cancelled request frame") {
            ObjectRpcChannelWireFrame::Request { call_id, .. } => call_id,
            frame => panic!("expected request, got {frame:?}"),
        };
        timeout(Duration::from_millis(1), cancelled.result())
            .await
            .expect_err("cancelled result timeout");
        assert!(matches!(
            egress.next_frame().await.expect("cancel frame"),
            ObjectRpcChannelWireFrame::Cancel { call_id, .. } if call_id == cancelled_id
        ));

        let unrelated = client
            .call(ObjectRpcSessionCall::value(request(
                "/channel/unrelated",
                Vec::new(),
            )))
            .await
            .expect("unrelated call");
        let unrelated_id = match egress.next_frame().await.expect("unrelated request frame") {
            ObjectRpcChannelWireFrame::Request { call_id, .. } => call_id,
            frame => panic!("expected request, got {frame:?}"),
        };

        let late_frames = [
            ObjectRpcChannelWireFrame::RequestAccepted {
                call_id: cancelled_id,
                credit_bytes: OBJECT_RPC_CHANNEL_CALL_WINDOW_BYTES as u32,
            },
            ObjectRpcChannelWireFrame::Response {
                call_id: cancelled_id,
                response: response("/channel/cancelled", Vec::new()),
                body: None,
                initial_credit_bytes: 0,
            },
            ObjectRpcChannelWireFrame::Data {
                call_id: cancelled_id,
                direction: ObjectRpcChannelBodyDirection::Response,
                offset: 0,
                bytes: vec![1],
            },
            ObjectRpcChannelWireFrame::WindowUpdate {
                call_id: cancelled_id,
                direction: ObjectRpcChannelBodyDirection::Request,
                received_offset: OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES as u64,
            },
            ObjectRpcChannelWireFrame::BodyEnd {
                call_id: cancelled_id,
                direction: ObjectRpcChannelBodyDirection::Response,
                total_bytes: 0,
            },
            ObjectRpcChannelWireFrame::Reset {
                call_id: cancelled_id,
                reason: "late reset".to_string(),
            },
        ];
        for frame in late_frames {
            ingress.receive(frame).expect("late tombstoned frame");
        }

        ingress
            .receive(ObjectRpcChannelWireFrame::Response {
                call_id: unrelated_id,
                response: response("/channel/unrelated", b"ok".to_vec()),
                body: None,
                initial_credit_bytes: 0,
            })
            .expect("unrelated response");
        let result = unrelated.result().await.expect("unrelated result");
        assert_eq!(result.response.body, b"ok");

        let never_known = ObjectRpcChannelCallId(cancelled_id.0 + 10_000);
        assert!(matches!(
            ingress.receive(ObjectRpcChannelWireFrame::Response {
                call_id: never_known,
                response: response("/channel/never-known", Vec::new()),
                body: None,
                initial_credit_bytes: 0,
            }),
            Err(NetworkError::Serialization { .. })
        ));
    }

    #[tokio::test]
    async fn retired_high_watermark_survives_tombstone_churn_and_stale_data() {
        const CANCELLED_CALLS: usize = MAX_COMPLETED_CALL_TOMBSTONES + 32;
        let ObjectRpcChannelEndpoint {
            client,
            server: _server,
            ingress,
            mut egress,
        } = ObjectRpcChannelEndpoint::new(
            ObjectRpcChannelRole::Dialer,
            ObjectRpcChannelConfig::default(),
        )
        .expect("endpoint");
        let mut retired = Vec::with_capacity(CANCELLED_CALLS);
        for index in 0..CANCELLED_CALLS {
            let mut pending = client
                .call(ObjectRpcSessionCall::value(request(
                    &format!("/channel/retired/{index}"),
                    Vec::new(),
                )))
                .await
                .expect("retired call");
            let call_id = match egress.next_frame().await.expect("retired request") {
                ObjectRpcChannelWireFrame::Request { call_id, .. } => call_id,
                frame => panic!("expected retired request, got {frame:?}"),
            };
            pending.cancel("retire call");
            assert!(matches!(
                egress.next_frame().await.expect("retired cancel"),
                ObjectRpcChannelWireFrame::Cancel { call_id: cancelled, .. }
                    if cancelled == call_id
            ));
            retired.push(call_id);
        }
        assert_eq!(client.queue_snapshot().active_calls, 0);
        assert!(
            !client
                .shared
                .tombstones
                .lock()
                .expect("tombstones")
                .contains(retired[0])
        );

        for call_id in &retired {
            ingress
                .receive(ObjectRpcChannelWireFrame::Data {
                    call_id: *call_id,
                    direction: ObjectRpcChannelBodyDirection::Response,
                    offset: 0,
                    bytes: vec![1],
                })
                .expect("stale data after tombstone eviction");
        }
        let never_issued = ObjectRpcChannelCallId(retired.last().expect("retired id").0 + 100);
        assert!(matches!(
            ingress.receive(ObjectRpcChannelWireFrame::Response {
                call_id: never_issued,
                response: response("/channel/never-issued", Vec::new()),
                body: None,
                initial_credit_bytes: 0,
            }),
            Err(NetworkError::Serialization { .. })
        ));

        let unrelated = client
            .call(ObjectRpcSessionCall::value(request(
                "/channel/retired/final",
                Vec::new(),
            )))
            .await
            .expect("carrier remains usable");
        let unrelated_id = match egress.next_frame().await.expect("final request") {
            ObjectRpcChannelWireFrame::Request { call_id, .. } => call_id,
            frame => panic!("expected final request, got {frame:?}"),
        };
        ingress
            .receive(ObjectRpcChannelWireFrame::Response {
                call_id: unrelated_id,
                response: response("/channel/retired/final", b"ok".to_vec()),
                body: None,
                initial_credit_bytes: 0,
            })
            .expect("final response");
        assert_eq!(
            unrelated
                .result()
                .await
                .expect("final result")
                .response
                .body,
            b"ok"
        );
        assert!(!client.is_closed());
    }

    #[test]
    fn completed_call_tombstones_are_bounded() {
        let mut tombstones = CallTombstones::default();
        for value in 1..=(MAX_COMPLETED_CALL_TOMBSTONES as u64 + 32) {
            tombstones.insert(ObjectRpcChannelCallId(value));
        }
        assert_eq!(tombstones.order.len(), MAX_COMPLETED_CALL_TOMBSTONES);
        assert_eq!(tombstones.members.len(), MAX_COMPLETED_CALL_TOMBSTONES);
        assert!(!tombstones.contains(ObjectRpcChannelCallId(1)));
        assert!(tombstones.contains(ObjectRpcChannelCallId(
            MAX_COMPLETED_CALL_TOMBSTONES as u64 + 32
        )));
    }

    #[test]
    fn excessive_public_queue_depths_are_rejected() {
        for config in [
            ObjectRpcChannelConfig {
                control_queue_depth: MAX_CONTROL_QUEUE_DEPTH + 1,
                ..ObjectRpcChannelConfig::default()
            },
            ObjectRpcChannelConfig {
                data_queue_depth: MAX_DATA_QUEUE_DEPTH + 1,
                ..ObjectRpcChannelConfig::default()
            },
        ] {
            assert!(matches!(
                ObjectRpcChannelEndpoint::new(ObjectRpcChannelRole::Dialer, config),
                Err(NetworkError::ResourceExhausted { .. })
            ));
        }
    }

    #[tokio::test]
    async fn terminal_response_preparation_and_head_failures_reset_call_and_release_permit() {
        let oversized_value = ObjectRpcSessionResponse::value(response(
            "/channel/response/oversized-value",
            vec![0; OBJECT_RPC_CHANNEL_MAX_MATERIALIZED_VALUE_BYTES + 1],
        ));
        let oversized_head = ObjectRpcSessionResponse::value(response(
            &"x".repeat(MAX_CONTROL_HEAD_BYTES + 1),
            Vec::new(),
        ));

        for (route, failed_response, expected_error) in [
            (
                "/channel/response/oversized-value",
                oversized_value,
                "prepare multiplexed response value",
            ),
            (
                "/channel/response/oversized-head",
                oversized_head,
                "encode multiplexed object RPC response head",
            ),
        ] {
            let mut endpoints = linked_endpoints(ObjectRpcChannelConfig {
                max_calls: 1,
                ..ObjectRpcChannelConfig::default()
            });
            let pending = endpoints
                .left_client
                .call(ObjectRpcSessionCall::value(request(route, Vec::new())))
                .await
                .expect("failing response call");
            let inbound = timeout(Duration::from_secs(1), endpoints.right_server.accept())
                .await
                .expect("failing response request timeout")
                .expect("failing response request");

            let response_error = inbound
                .respond(failed_response)
                .await
                .expect_err("terminal response validation must fail");
            assert!(
                response_error.to_string().contains(expected_error),
                "unexpected responder error: {response_error}"
            );
            let retry_error = inbound
                .respond(ObjectRpcSessionResponse::value(response(
                    route,
                    b"retry".to_vec(),
                )))
                .await
                .expect_err("terminal response failure must not remain retryable");
            assert!(
                retry_error
                    .to_string()
                    .contains("unknown multiplexed object RPC call"),
                "unexpected response retry error: {retry_error}"
            );
            let client_error = timeout(Duration::from_secs(1), pending.result())
                .await
                .expect("client terminal response timeout")
                .expect_err("client must receive terminal response reset");
            assert!(
                client_error.to_string().contains(expected_error),
                "unexpected client error: {client_error}"
            );
            wait_for_active_calls(&endpoints.left_client, 0).await;
            wait_for_active_calls(&endpoints.right_client, 0).await;
            assert_eq!(FrameCounts::load(&endpoints.counts.resets), 1);

            assert_linked_channel_accepts_replacement(
                &mut endpoints,
                &format!("{route}/replacement"),
            )
            .await;
        }
    }

    #[tokio::test]
    async fn terminal_response_budget_failure_resets_call_and_recovers_all_permits() {
        let mut endpoints = linked_endpoints(ObjectRpcChannelConfig {
            max_calls: 1,
            ..ObjectRpcChannelConfig::default()
        });
        let remaining_bytes = OBJECT_RPC_CHANNEL_INLINE_VALUE_BYTES;
        let held_bytes = OBJECT_RPC_CHANNEL_MAX_AGGREGATE_MATERIALIZED_VALUE_BYTES
            .checked_sub(remaining_bytes)
            .expect("aggregate budget exceeds inline threshold");
        let held_budget = Arc::clone(&endpoints.right_client.shared.materialized_value_budget)
            .try_acquire_many_owned(u32::try_from(held_bytes).expect("held budget fits u32"))
            .expect("reserve deterministic aggregate pressure");
        assert_eq!(
            endpoints
                .right_client
                .shared
                .materialized_value_budget
                .available_permits(),
            remaining_bytes
        );

        let route = "/channel/response/budget-exhausted";
        let pending = endpoints
            .left_client
            .call(ObjectRpcSessionCall::value(request(route, Vec::new())))
            .await
            .expect("budget-failing response call");
        let inbound = timeout(Duration::from_secs(1), endpoints.right_server.accept())
            .await
            .expect("budget-failing response request timeout")
            .expect("budget-failing response request");
        let response_error = inbound
            .respond(ObjectRpcSessionResponse::value(response(
                route,
                vec![0; OBJECT_RPC_CHANNEL_INLINE_VALUE_BYTES + 1],
            )))
            .await
            .expect_err("aggregate response budget must fail");
        assert!(
            response_error
                .to_string()
                .contains("reserve multiplexed materialized value"),
            "unexpected responder error: {response_error}"
        );
        let client_error = timeout(Duration::from_secs(1), pending.result())
            .await
            .expect("client budget response timeout")
            .expect_err("client must receive budget response reset");
        assert!(
            client_error
                .to_string()
                .contains("reserve multiplexed materialized value"),
            "unexpected client error: {client_error}"
        );
        wait_for_active_calls(&endpoints.left_client, 0).await;
        wait_for_active_calls(&endpoints.right_client, 0).await;
        assert_eq!(FrameCounts::load(&endpoints.counts.resets), 1);

        // The failed call's channel permit is reusable even while the test's
        // independent aggregate-memory reservation remains held.
        assert_linked_channel_accepts_replacement(
            &mut endpoints,
            "/channel/response/budget-replacement",
        )
        .await;
        drop(held_budget);
        wait_for_available_materialized_bytes(
            &endpoints.right_client,
            OBJECT_RPC_CHANNEL_MAX_AGGREGATE_MATERIALIZED_VALUE_BYTES,
        )
        .await;
    }

    #[tokio::test]
    async fn materialized_values_share_one_budget_and_attachments_do_not_consume_it() {
        let ObjectRpcChannelEndpoint {
            client,
            server: _server,
            ingress,
            mut egress,
        } = ObjectRpcChannelEndpoint::new(
            ObjectRpcChannelRole::Dialer,
            ObjectRpcChannelConfig::default(),
        )
        .expect("endpoint");
        let value_bytes = 5 * 1024 * 1024;
        let value_descriptor = ObjectRpcChannelBodyDescriptor {
            kind: ObjectRpcChannelBodyKind::Value,
            total_bytes: Some(value_bytes as u64),
        };
        let attachment_descriptor = ObjectRpcChannelBodyDescriptor {
            kind: ObjectRpcChannelBodyKind::Attachment,
            total_bytes: Some(64 * 1024 * 1024),
        };

        ingress
            .receive(ObjectRpcChannelWireFrame::Request {
                call_id: ObjectRpcChannelCallId(2),
                request: request("/channel/value-one", Vec::new()),
                body: Some(value_descriptor.clone()),
            })
            .expect("first value admission");
        assert!(matches!(
            egress.next_frame().await.expect("first value accepted"),
            ObjectRpcChannelWireFrame::RequestAccepted {
                call_id: ObjectRpcChannelCallId(2),
                ..
            }
        ));
        assert_eq!(
            client.shared.materialized_value_budget.available_permits(),
            OBJECT_RPC_CHANNEL_MAX_AGGREGATE_MATERIALIZED_VALUE_BYTES - value_bytes
        );

        ingress
            .receive(ObjectRpcChannelWireFrame::Request {
                call_id: ObjectRpcChannelCallId(4),
                request: request("/channel/large-attachment", Vec::new()),
                body: Some(attachment_descriptor),
            })
            .expect("attachment admission");
        assert!(matches!(
            egress.next_frame().await.expect("attachment accepted"),
            ObjectRpcChannelWireFrame::RequestAccepted {
                call_id: ObjectRpcChannelCallId(4),
                ..
            }
        ));
        assert_eq!(
            client.shared.materialized_value_budget.available_permits(),
            OBJECT_RPC_CHANNEL_MAX_AGGREGATE_MATERIALIZED_VALUE_BYTES - value_bytes,
            "attachments must not reserve typed-value memory"
        );

        ingress
            .receive(ObjectRpcChannelWireFrame::Request {
                call_id: ObjectRpcChannelCallId(6),
                request: request("/channel/value-excess", Vec::new()),
                body: Some(value_descriptor.clone()),
            })
            .expect("excess value must reset without closing the carrier");
        assert!(matches!(
            egress.next_frame().await.expect("excess value reset"),
            ObjectRpcChannelWireFrame::Reset {
                call_id: ObjectRpcChannelCallId(6),
                ..
            }
        ));

        ingress
            .receive(ObjectRpcChannelWireFrame::Cancel {
                call_id: ObjectRpcChannelCallId(2),
                reason: "release value budget".to_string(),
            })
            .expect("cancel first value");
        ingress
            .receive(ObjectRpcChannelWireFrame::Cancel {
                call_id: ObjectRpcChannelCallId(4),
                reason: "release attachment".to_string(),
            })
            .expect("cancel attachment");
        wait_for_available_materialized_bytes(
            &client,
            OBJECT_RPC_CHANNEL_MAX_AGGREGATE_MATERIALIZED_VALUE_BYTES,
        )
        .await;

        ingress
            .receive(ObjectRpcChannelWireFrame::Request {
                call_id: ObjectRpcChannelCallId(8),
                request: request("/channel/value-after-release", Vec::new()),
                body: Some(value_descriptor),
            })
            .expect("value admission after release");
        assert!(matches!(
            egress.next_frame().await.expect("released value accepted"),
            ObjectRpcChannelWireFrame::RequestAccepted {
                call_id: ObjectRpcChannelCallId(8),
                ..
            }
        ));
        ingress
            .receive(ObjectRpcChannelWireFrame::Cancel {
                call_id: ObjectRpcChannelCallId(8),
                reason: "test complete".to_string(),
            })
            .expect("cancel released value");
        wait_for_available_materialized_bytes(
            &client,
            OBJECT_RPC_CHANNEL_MAX_AGGREGATE_MATERIALIZED_VALUE_BYTES,
        )
        .await;

        let mut outgoing_value = client
            .call(ObjectRpcSessionCall::value(request(
                "/channel/outgoing-value-one",
                vec![9_u8; value_bytes],
            )))
            .await
            .expect("first outgoing value admission");
        assert_eq!(
            client.shared.materialized_value_budget.available_permits(),
            OBJECT_RPC_CHANNEL_MAX_AGGREGATE_MATERIALIZED_VALUE_BYTES - value_bytes
        );
        assert!(matches!(
            client
                .call(ObjectRpcSessionCall::value(request(
                    "/channel/outgoing-value-excess",
                    vec![7_u8; value_bytes],
                )))
                .await,
            Err(NetworkError::ResourceExhausted { .. })
        ));
        let mut outgoing_attachment = client
            .call(ObjectRpcSessionCall::new(
                request("/channel/outgoing-attachment", Vec::new()),
                ObjectRpcBodySource::from_reader(std::io::empty(), Some(64 * 1024 * 1024)),
            ))
            .await
            .expect("outgoing attachment bypasses value budget");
        outgoing_value.cancel("release outgoing value budget");
        outgoing_attachment.cancel("release outgoing attachment");
        wait_for_available_materialized_bytes(
            &client,
            OBJECT_RPC_CHANNEL_MAX_AGGREGATE_MATERIALIZED_VALUE_BYTES,
        )
        .await;
    }

    #[tokio::test]
    async fn simultaneous_forty_by_forty_saturation_resets_only_excess_calls() {
        const CALLS_PER_SIDE: usize = 40;
        const ACCEPTED_PER_SIDE: usize = OBJECT_RPC_CHANNEL_MAX_CALLS - CALLS_PER_SIDE;
        const RESET_PER_SIDE: usize = CALLS_PER_SIDE - ACCEPTED_PER_SIDE;

        let left = ObjectRpcChannelEndpoint::new(
            ObjectRpcChannelRole::Dialer,
            ObjectRpcChannelConfig::default(),
        )
        .expect("left endpoint");
        let right = ObjectRpcChannelEndpoint::new(
            ObjectRpcChannelRole::Acceptor,
            ObjectRpcChannelConfig::default(),
        )
        .expect("right endpoint");
        let ObjectRpcChannelEndpoint {
            client: left_client,
            server: mut left_server,
            ingress: left_ingress,
            egress: mut left_egress,
        } = left;
        let ObjectRpcChannelEndpoint {
            client: right_client,
            server: mut right_server,
            ingress: right_ingress,
            egress: mut right_egress,
        } = right;

        let mut left_pending = Vec::with_capacity(CALLS_PER_SIDE);
        let mut right_pending = Vec::with_capacity(CALLS_PER_SIDE);
        for index in 0..CALLS_PER_SIDE {
            left_pending.push(
                left_client
                    .call(ObjectRpcSessionCall::value(request(
                        &format!("/channel/saturate/left/{index}"),
                        Vec::new(),
                    )))
                    .await
                    .expect("left local call"),
            );
            right_pending.push(
                right_client
                    .call(ObjectRpcSessionCall::value(request(
                        &format!("/channel/saturate/right/{index}"),
                        Vec::new(),
                    )))
                    .await
                    .expect("right local call"),
            );
        }

        // Deliver every Request before either direction's Reset frames. This
        // fixes the simultaneous-admission point deterministically at 40
        // local plus 24 incoming calls on each endpoint.
        for _ in 0..CALLS_PER_SIDE {
            let frame = left_egress.next_frame().await.expect("left request frame");
            assert!(matches!(frame, ObjectRpcChannelWireFrame::Request { .. }));
            right_ingress.receive(frame).expect("right request ingress");
        }
        for _ in 0..CALLS_PER_SIDE {
            let frame = right_egress
                .next_frame()
                .await
                .expect("right request frame");
            assert!(matches!(frame, ObjectRpcChannelWireFrame::Request { .. }));
            left_ingress.receive(frame).expect("left request ingress");
        }
        assert_eq!(
            left_client.queue_snapshot().active_calls,
            OBJECT_RPC_CHANNEL_MAX_CALLS
        );
        assert_eq!(
            right_client.queue_snapshot().active_calls,
            OBJECT_RPC_CHANNEL_MAX_CALLS
        );

        for _ in 0..RESET_PER_SIDE {
            let frame = left_egress.next_frame().await.expect("left reset frame");
            assert!(matches!(frame, ObjectRpcChannelWireFrame::Reset { .. }));
            right_ingress
                .receive(frame)
                .expect("right reset must not close carrier");
        }
        for _ in 0..RESET_PER_SIDE {
            let frame = right_egress.next_frame().await.expect("right reset frame");
            assert!(matches!(frame, ObjectRpcChannelWireFrame::Reset { .. }));
            left_ingress
                .receive(frame)
                .expect("left reset must not close carrier");
        }

        for _ in 0..ACCEPTED_PER_SIDE {
            let inbound = left_server.accept().await.expect("left accepted request");
            let route = inbound.request.request.route.clone();
            inbound
                .respond(ObjectRpcSessionResponse::value(response(
                    &route,
                    b"left-ok".to_vec(),
                )))
                .await
                .expect("left response");
        }
        for _ in 0..ACCEPTED_PER_SIDE {
            let inbound = right_server.accept().await.expect("right accepted request");
            let route = inbound.request.request.route.clone();
            inbound
                .respond(ObjectRpcSessionResponse::value(response(
                    &route,
                    b"right-ok".to_vec(),
                )))
                .await
                .expect("right response");
        }
        for _ in 0..ACCEPTED_PER_SIDE {
            let frame = left_egress.next_frame().await.expect("left response frame");
            assert!(matches!(frame, ObjectRpcChannelWireFrame::Response { .. }));
            right_ingress
                .receive(frame)
                .expect("right response ingress");
        }
        for _ in 0..ACCEPTED_PER_SIDE {
            let frame = right_egress
                .next_frame()
                .await
                .expect("right response frame");
            assert!(matches!(frame, ObjectRpcChannelWireFrame::Response { .. }));
            left_ingress.receive(frame).expect("left response ingress");
        }

        let mut left_ok = 0;
        let mut left_reset = 0;
        for pending in left_pending {
            match pending.result().await {
                Ok(_) => left_ok += 1,
                Err(_) => left_reset += 1,
            }
        }
        let mut right_ok = 0;
        let mut right_reset = 0;
        for pending in right_pending {
            match pending.result().await {
                Ok(_) => right_ok += 1,
                Err(_) => right_reset += 1,
            }
        }
        assert_eq!((left_ok, left_reset), (ACCEPTED_PER_SIDE, RESET_PER_SIDE));
        assert_eq!((right_ok, right_reset), (ACCEPTED_PER_SIDE, RESET_PER_SIDE));

        timeout(Duration::from_secs(1), async {
            while left_client.queue_snapshot().active_calls != 0
                || right_client.queue_snapshot().active_calls != 0
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("saturated call release timeout");

        let final_call = left_client
            .call(ObjectRpcSessionCall::value(request(
                "/channel/saturate/final",
                Vec::new(),
            )))
            .await
            .expect("carrier remains usable");
        let frame = left_egress.next_frame().await.expect("final request frame");
        right_ingress.receive(frame).expect("final request ingress");
        let inbound = right_server.accept().await.expect("final accepted request");
        inbound
            .respond(ObjectRpcSessionResponse::value(response(
                "/channel/saturate/final",
                b"ok".to_vec(),
            )))
            .await
            .expect("final response");
        let frame = right_egress
            .next_frame()
            .await
            .expect("final response frame");
        left_ingress.receive(frame).expect("final response ingress");
        assert_eq!(
            final_call
                .result()
                .await
                .expect("final result")
                .response
                .body,
            b"ok"
        );
    }

    #[tokio::test]
    async fn stalled_stream_does_not_block_an_unrelated_inline_call() {
        let mut endpoints = linked_endpoints(ObjectRpcChannelConfig::default());
        let mut stalled_call = endpoints
            .left_client
            .call(ObjectRpcSessionCall::new(
                request("/channel/stalled", Vec::new()),
                ObjectRpcBodySource::bytes(vec![7; 2 * 1024 * 1024]),
            ))
            .await
            .expect("stalled call");
        let stalled_request = timeout(Duration::from_secs(1), endpoints.right_server.accept())
            .await
            .expect("stalled request timeout")
            .expect("stalled request");
        let mut cancellation = stalled_request.cancellation();

        let quick_call = endpoints
            .left_client
            .call(ObjectRpcSessionCall::value(request(
                "/channel/quick",
                b"ping".to_vec(),
            )))
            .await
            .expect("quick call");
        let quick_request = timeout(Duration::from_secs(1), endpoints.right_server.accept())
            .await
            .expect("quick request timeout")
            .expect("quick request");
        quick_request
            .respond(ObjectRpcSessionResponse::value(response(
                "/channel/quick",
                b"pong".to_vec(),
            )))
            .await
            .expect("quick response");
        let quick_result = timeout(Duration::from_secs(1), quick_call.result())
            .await
            .expect("quick result timeout")
            .expect("quick result");
        assert_eq!(quick_result.response.body, b"pong");

        stalled_call.cancel("test finished");
        timeout(Duration::from_secs(1), cancellation.changed())
            .await
            .expect("cancellation timeout")
            .expect("cancellation sender");
        assert_eq!(cancellation.borrow().as_deref(), Some("test finished"));
        drop(stalled_request);
    }

    #[tokio::test]
    async fn data_and_control_queues_remain_bounded() {
        let config = ObjectRpcChannelConfig {
            max_calls: OBJECT_RPC_CHANNEL_MAX_CALLS,
            max_queued_data_bytes: OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES,
            control_queue_depth: 2,
            data_queue_depth: 1,
        };
        let endpoint = ObjectRpcChannelEndpoint::new(ObjectRpcChannelRole::Dialer, config.clone())
            .expect("data endpoint");
        let ObjectRpcChannelEndpoint {
            client: _,
            server: _,
            ingress: _,
            mut egress,
        } = endpoint;
        let outbound = Arc::clone(&egress.shared.outbound);
        let data_frame = |call_id| ObjectRpcChannelWireFrame::Data {
            call_id: ObjectRpcChannelCallId(call_id),
            direction: ObjectRpcChannelBodyDirection::Request,
            offset: 0,
            bytes: vec![1; OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES],
        };
        outbound
            .queue_data(data_frame(1))
            .await
            .expect("first data quantum");
        assert_eq!(
            egress.queue_snapshot().queued_data_bytes,
            OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES
        );
        assert_eq!(
            egress.queue_snapshot().queued_bytes,
            OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES
        );
        let second_outbound = Arc::clone(&outbound);
        let mut second =
            tokio::spawn(async move { second_outbound.queue_data(data_frame(3)).await });
        assert!(
            timeout(Duration::from_millis(25), &mut second)
                .await
                .is_err()
        );
        assert_eq!(
            egress.queue_snapshot().queued_data_bytes,
            OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES
        );
        assert_eq!(
            egress.queue_snapshot().queued_bytes,
            OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES
        );
        assert!(matches!(
            egress.next_frame().await,
            Some(ObjectRpcChannelWireFrame::Data { call_id, .. })
                if call_id == ObjectRpcChannelCallId(1)
        ));
        timeout(Duration::from_secs(1), &mut second)
            .await
            .expect("second quantum admission timeout")
            .expect("second data task")
            .expect("second data quantum");
        assert_eq!(
            egress.queue_snapshot().queued_data_bytes,
            OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES
        );
        let _ = egress.next_frame().await.expect("second data frame");
        assert_eq!(egress.queue_snapshot().queued_data_bytes, 0);
        assert_eq!(egress.queue_snapshot().queued_bytes, 0);

        let control_endpoint = ObjectRpcChannelEndpoint::new(ObjectRpcChannelRole::Dialer, config)
            .expect("control endpoint");
        let first = control_endpoint
            .client
            .call(ObjectRpcSessionCall::value(request(
                "/channel/control/one",
                Vec::new(),
            )))
            .await
            .expect("first control");
        let second = control_endpoint
            .client
            .call(ObjectRpcSessionCall::value(request(
                "/channel/control/two",
                Vec::new(),
            )))
            .await
            .expect("second control");
        let overflow = control_endpoint
            .client
            .call(ObjectRpcSessionCall::value(request(
                "/channel/control/three",
                Vec::new(),
            )))
            .await;
        assert!(matches!(
            overflow,
            Err(NetworkError::ResourceExhausted { .. })
        ));
        assert_eq!(
            control_endpoint
                .client
                .queue_snapshot()
                .queued_control_frames,
            2
        );
        assert!(
            control_endpoint.client.queue_snapshot().queued_bytes
                <= OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES
        );
        drop((first, second));
    }

    #[tokio::test]
    async fn control_priority_yields_to_queued_data_after_a_bounded_burst() {
        let endpoint = ObjectRpcChannelEndpoint::new(
            ObjectRpcChannelRole::Dialer,
            ObjectRpcChannelConfig::default(),
        )
        .expect("endpoint");
        let ObjectRpcChannelEndpoint {
            client: _,
            server: _,
            ingress: _,
            mut egress,
        } = endpoint;
        let outbound = Arc::clone(&egress.shared.outbound);
        outbound
            .queue_data(ObjectRpcChannelWireFrame::Data {
                call_id: ObjectRpcChannelCallId(1),
                direction: ObjectRpcChannelBodyDirection::Request,
                offset: 0,
                bytes: vec![1],
            })
            .await
            .expect("queue data");
        for call_id in 1..=CONTROL_BURST_BEFORE_DATA + 1 {
            outbound
                .try_control(ObjectRpcChannelWireFrame::Cancel {
                    call_id: ObjectRpcChannelCallId((call_id as u64) * 2 + 1),
                    reason: "fairness test".to_string(),
                })
                .expect("queue control");
        }
        for _ in 0..CONTROL_BURST_BEFORE_DATA {
            assert!(matches!(
                egress.next_frame().await,
                Some(ObjectRpcChannelWireFrame::Cancel { .. })
            ));
        }
        assert!(matches!(
            egress.next_frame().await,
            Some(ObjectRpcChannelWireFrame::Data { .. })
        ));
    }

    #[tokio::test]
    async fn bodyless_call_retires_only_after_queued_head_reaches_egress() {
        let endpoint = ObjectRpcChannelEndpoint::new(
            ObjectRpcChannelRole::Dialer,
            ObjectRpcChannelConfig {
                max_calls: 1,
                ..ObjectRpcChannelConfig::default()
            },
        )
        .expect("endpoint");
        let client = endpoint.client;
        let ingress = endpoint.ingress;
        let mut egress = endpoint.egress;
        let pending = client
            .call(ObjectRpcSessionCall::value(request(
                "/channel/bodyless/emission",
                Vec::new(),
            )))
            .await
            .expect("bodyless call");
        let call_id = pending.call_id();

        // Injecting the response before polling egress makes the distinction
        // between queue admission and emission deterministic. Incoming
        // completion alone must not release the call permit or tombstone it.
        ingress
            .receive(ObjectRpcChannelWireFrame::Response {
                call_id,
                response: response("/channel/bodyless/emission", b"ok".to_vec()),
                body: None,
                initial_credit_bytes: 0,
            })
            .expect("bodyless response");
        assert_eq!(
            pending
                .result()
                .await
                .expect("bodyless result")
                .response
                .body,
            b"ok"
        );
        assert_eq!(client.queue_snapshot().active_calls, 1);
        assert!(
            client
                .shared
                .pending
                .lock()
                .expect("pending calls")
                .contains_key(&call_id)
        );
        assert!(
            !client
                .shared
                .tombstones
                .lock()
                .expect("tombstones")
                .contains(call_id)
        );

        assert!(matches!(
            egress.next_frame().await,
            Some(ObjectRpcChannelWireFrame::Request {
                call_id: emitted,
                ..
            }) if emitted == call_id
        ));
        assert_eq!(client.queue_snapshot().active_calls, 0);
        assert!(
            !client
                .shared
                .pending
                .lock()
                .expect("pending calls")
                .contains_key(&call_id)
        );
        assert!(
            client
                .shared
                .tombstones
                .lock()
                .expect("tombstones")
                .contains(call_id)
        );

        let replacement = client
            .call(ObjectRpcSessionCall::value(request(
                "/channel/bodyless/replacement",
                Vec::new(),
            )))
            .await
            .expect("released permit accepts replacement call");
        drop(replacement);
    }

    #[tokio::test]
    async fn physical_close_fails_pending_work_and_rejects_later_io() {
        let endpoint = ObjectRpcChannelEndpoint::new(
            ObjectRpcChannelRole::Dialer,
            ObjectRpcChannelConfig::default(),
        )
        .expect("endpoint");
        let client = endpoint.client.clone();
        let mut server = endpoint.server;
        let ingress = endpoint.ingress;
        let mut egress = endpoint.egress;
        let pending = client
            .call(ObjectRpcSessionCall::value(request(
                "/channel/close/pending",
                Vec::new(),
            )))
            .await
            .expect("pending call");
        let call_id = pending.call_id();
        assert_eq!(client.queue_snapshot().active_calls, 1);
        assert_eq!(client.queue_snapshot().queued_control_frames, 1);

        ingress.close("test carrier stopped");
        ingress.close("ignored duplicate close");

        assert!(client.is_closed());
        assert_eq!(client.queue_snapshot().active_calls, 0);
        assert!(
            client
                .shared
                .tombstones
                .lock()
                .expect("tombstones")
                .contains(call_id)
        );
        // Closing the carrier releases the call permit immediately, while the
        // unsent head remains accounted until its physical egress is dropped.
        assert_eq!(client.queue_snapshot().queued_control_frames, 1);
        assert!(
            timeout(Duration::from_secs(1), egress.next_frame())
                .await
                .expect("egress close timeout")
                .is_none()
        );
        assert_eq!(client.queue_snapshot().queued_control_frames, 1);
        drop(egress);
        assert_eq!(client.queue_snapshot().queued_control_frames, 0);
        assert_eq!(client.queue_snapshot().queued_bytes, 0);
        let pending_error = pending.result().await.expect_err("pending call must fail");
        assert!(pending_error.to_string().contains("test carrier stopped"));
        let later_call = client
            .call(ObjectRpcSessionCall::value(request(
                "/channel/close/later",
                Vec::new(),
            )))
            .await;
        let Err(later_call) = later_call else {
            panic!("later call must fail");
        };
        assert!(later_call.to_string().contains("test carrier stopped"));
        let later_frame = ingress
            .receive(ObjectRpcChannelWireFrame::Reset {
                call_id: ObjectRpcChannelCallId(2),
                reason: "late frame".to_string(),
            })
            .expect_err("later frame must fail");
        assert!(later_frame.to_string().contains("test carrier stopped"));
        assert!(
            timeout(Duration::from_secs(1), server.accept())
                .await
                .expect("server close timeout")
                .is_none()
        );
    }

    #[tokio::test]
    async fn response_write_waits_for_physical_transport_completion() {
        let endpoint = ObjectRpcChannelEndpoint::new(
            ObjectRpcChannelRole::Dialer,
            ObjectRpcChannelConfig::default(),
        )
        .expect("endpoint");
        let mut server = endpoint.server;
        let ingress = endpoint.ingress;
        let mut egress = endpoint.egress;
        let call_id = ObjectRpcChannelCallId(2);
        ingress
            .receive(ObjectRpcChannelWireFrame::Request {
                call_id,
                request: request("/channel/write-completion", Vec::new()),
                body: None,
            })
            .expect("receive bodyless request");
        let inbound = server.accept().await.expect("accepted request");
        let responder = inbound.responder();
        let response_task = tokio::spawn(async move {
            responder
                .respond_and_wait_written(ObjectRpcSessionResponse::value(response(
                    "/channel/write-completion",
                    b"ok".to_vec(),
                )))
                .await
        });

        let frame = egress
            .next_transport_frame()
            .await
            .expect("queued response frame");
        assert!(matches!(
            frame,
            ObjectRpcChannelTransportFrame::Control(
                ObjectRpcChannelWireFrame::Response {
                    call_id: response_call_id,
                    ..
                }
            ) if response_call_id == call_id
        ));
        tokio::task::yield_now().await;
        assert!(
            !response_task.is_finished(),
            "dequeue alone must not claim physical response delivery"
        );

        egress.complete_frame_write(true);
        timeout(Duration::from_secs(1), response_task)
            .await
            .expect("response write completion timeout")
            .expect("response task")
            .expect("response write completion");
    }

    #[tokio::test]
    async fn failed_physical_response_write_cancels_write_waiter() {
        let endpoint = ObjectRpcChannelEndpoint::new(
            ObjectRpcChannelRole::Dialer,
            ObjectRpcChannelConfig::default(),
        )
        .expect("endpoint");
        let mut server = endpoint.server;
        let ingress = endpoint.ingress;
        let mut egress = endpoint.egress;
        ingress
            .receive(ObjectRpcChannelWireFrame::Request {
                call_id: ObjectRpcChannelCallId(2),
                request: request("/channel/write-failure", Vec::new()),
                body: None,
            })
            .expect("receive bodyless request");
        let responder = server.accept().await.expect("accepted request").responder();
        let response_task = tokio::spawn(async move {
            responder
                .respond_and_wait_written(ObjectRpcSessionResponse::value(response(
                    "/channel/write-failure",
                    b"not-written".to_vec(),
                )))
                .await
        });
        let _frame = egress
            .next_transport_frame()
            .await
            .expect("queued response frame");
        egress.complete_frame_write(false);

        let error = timeout(Duration::from_secs(1), response_task)
            .await
            .expect("failed-write waiter timeout")
            .expect("response task")
            .expect_err("failed physical write must fail the waiter");
        assert!(error.to_string().contains("response write completed"));
    }

    #[tokio::test]
    async fn data_scheduler_round_robins_one_frame_per_lane() {
        let endpoint = ObjectRpcChannelEndpoint::new(
            ObjectRpcChannelRole::Dialer,
            ObjectRpcChannelConfig::default(),
        )
        .expect("endpoint");
        let outbound = Arc::clone(&endpoint.client.shared.outbound);
        let mut egress = endpoint.egress;
        let (progress, _) = watch::channel(0);

        for call_id in [ObjectRpcChannelCallId(1), ObjectRpcChannelCallId(3)] {
            let frames = [0_u64, 1]
                .into_iter()
                .map(|offset| {
                    ObjectRpcChannelDataFrame::new(
                        call_id,
                        ObjectRpcChannelBodyDirection::Request,
                        offset,
                        Bytes::from_static(&[1]),
                    )
                    .expect("data frame")
                })
                .collect();
            outbound
                .queue_data_batch(
                    frames,
                    Arc::new(BodyWriteTracker::new()),
                    progress.clone(),
                    None,
                    false,
                )
                .await
                .expect("queue lane batch");
        }

        let mut order = Vec::new();
        for _ in 0..4 {
            let frame = egress
                .next_transport_frame()
                .await
                .expect("scheduled data frame");
            let ObjectRpcChannelTransportFrame::Data(frame) = frame else {
                panic!("data scheduler emitted a control frame");
            };
            order.push(frame.header.call_id);
            egress.complete_frame_write(true);
        }
        assert_eq!(
            order,
            [
                ObjectRpcChannelCallId(1),
                ObjectRpcChannelCallId(3),
                ObjectRpcChannelCallId(1),
                ObjectRpcChannelCallId(3),
            ]
        );
    }

    #[tokio::test]
    async fn data_batch_holds_one_exact_reservation_until_its_last_frame_is_written() {
        let endpoint = ObjectRpcChannelEndpoint::new(
            ObjectRpcChannelRole::Dialer,
            ObjectRpcChannelConfig::default(),
        )
        .expect("endpoint");
        let outbound = Arc::clone(&endpoint.client.shared.outbound);
        let mut egress = endpoint.egress;
        let initial_byte_permits = outbound.data_budget.available_permits();
        let initial_frame_permits = outbound.data_frame_budget.available_permits();
        let frame_lengths = [10_usize, 20, 30, 40];
        let batch_bytes = frame_lengths.into_iter().sum::<usize>();
        let mut offset = 0_u64;
        let frames = frame_lengths
            .into_iter()
            .map(|len| {
                let frame = ObjectRpcChannelDataFrame::new(
                    ObjectRpcChannelCallId(1),
                    ObjectRpcChannelBodyDirection::Request,
                    offset,
                    Bytes::from(vec![7; len]),
                )
                .expect("data frame");
                offset += len as u64;
                frame
            })
            .collect();
        let (progress, _) = watch::channel(0);

        outbound
            .queue_data_batch(
                frames,
                Arc::new(BodyWriteTracker::new()),
                progress,
                None,
                false,
            )
            .await
            .expect("queue data batch");
        assert_eq!(
            outbound.data_budget.available_permits(),
            initial_byte_permits - batch_bytes
        );
        assert_eq!(
            outbound.data_frame_budget.available_permits(),
            initial_frame_permits - frame_lengths.len()
        );

        for remaining_frames in (1..=frame_lengths.len()).rev() {
            assert!(matches!(
                egress.next_transport_frame().await,
                Some(ObjectRpcChannelTransportFrame::Data(_))
            ));
            egress.complete_frame_write(true);
            if remaining_frames > 1 {
                assert_eq!(
                    outbound.data_budget.available_permits(),
                    initial_byte_permits - batch_bytes,
                    "a shared batch reservation must survive intermediate frame writes"
                );
                assert_eq!(
                    outbound.data_frame_budget.available_permits(),
                    initial_frame_permits - frame_lengths.len(),
                    "all batch frame slots must remain reserved until the batch drains"
                );
            }
        }
        assert_eq!(
            outbound.data_budget.available_permits(),
            initial_byte_permits
        );
        assert_eq!(
            outbound.data_frame_budget.available_permits(),
            initial_frame_permits
        );
        assert_eq!(egress.queue_snapshot().queued_data_bytes, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn low_rate_receive_window_publishes_cumulative_credit_on_timer() {
        let ObjectRpcChannelEndpoint {
            client: _,
            mut server,
            ingress,
            mut egress,
        } = ObjectRpcChannelEndpoint::new(
            ObjectRpcChannelRole::Acceptor,
            ObjectRpcChannelConfig::default(),
        )
        .expect("endpoint");
        let call_id = ObjectRpcChannelCallId(1);
        let payload_bytes = OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES / 2;
        ingress
            .receive(ObjectRpcChannelWireFrame::Request {
                call_id,
                request: request("/channel/timed-window", Vec::new()),
                body: Some(ObjectRpcChannelBodyDescriptor {
                    kind: ObjectRpcChannelBodyKind::Attachment,
                    total_bytes: Some(payload_bytes as u64),
                }),
            })
            .expect("request ingress");
        assert!(matches!(
            egress.next_frame().await,
            Some(ObjectRpcChannelWireFrame::RequestAccepted { .. })
        ));
        let incoming = server.accept().await.expect("incoming request");
        ingress
            .receive(ObjectRpcChannelWireFrame::Data {
                call_id,
                direction: ObjectRpcChannelBodyDirection::Request,
                offset: 0,
                bytes: vec![3; payload_bytes],
            })
            .expect("request data");
        tokio::task::yield_now().await;

        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert!(matches!(
            egress.control_receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));

        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert!(matches!(
            timeout(Duration::from_millis(1), egress.next_frame())
                .await
                .expect("timed cumulative update"),
            Some(ObjectRpcChannelWireFrame::WindowUpdate {
                call_id: updated,
                direction: ObjectRpcChannelBodyDirection::Request,
                received_offset,
            }) if updated == call_id && received_offset == payload_bytes as u64
        ));

        ingress
            .receive(ObjectRpcChannelWireFrame::Cancel {
                call_id,
                reason: "timer test complete".to_string(),
            })
            .expect("cancel timed-window call");
        drop(incoming);
    }

    #[tokio::test]
    async fn cancelling_a_full_send_window_wakes_its_waiter() {
        let window = Arc::new(SendWindow::new(OBJECT_RPC_CHANNEL_CALL_WINDOW_BYTES));
        for _ in 0..OBJECT_RPC_CHANNEL_CALL_WINDOW_BYTES / OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES {
            window
                .consume(OBJECT_RPC_CHANNEL_DATA_QUANTUM_BYTES)
                .expect("fill send window");
        }
        let waiting_window = Arc::clone(&window);
        let waiter = tokio::spawn(async move { waiting_window.wait_available().await });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished(), "a full window must park its waiter");

        window.cancel();
        let result = timeout(Duration::from_secs(1), waiter)
            .await
            .expect("cancelled window waiter timeout")
            .expect("window waiter task");
        assert!(result.is_err(), "cancellation must fail the window waiter");
    }

    #[tokio::test]
    async fn pruning_data_that_woke_egress_does_not_signal_channel_eof() {
        let endpoint = ObjectRpcChannelEndpoint::new(
            ObjectRpcChannelRole::Dialer,
            ObjectRpcChannelConfig::default(),
        )
        .expect("endpoint");
        let outbound = Arc::clone(&endpoint.client.shared.outbound);
        let mut egress = endpoint.egress;
        let mut waiting = tokio::spawn(async move {
            let frame = egress.next_transport_frame().await;
            (frame, egress)
        });
        tokio::task::yield_now().await;

        let stale = ObjectRpcChannelDataFrame::new(
            ObjectRpcChannelCallId(1),
            ObjectRpcChannelBodyDirection::Request,
            0,
            Bytes::from_static(&[1]),
        )
        .expect("stale data frame");
        let (progress, _) = watch::channel(0);
        outbound
            .queue_data_batch(
                vec![stale],
                Arc::new(BodyWriteTracker::new()),
                progress,
                None,
                true,
            )
            .await
            .expect("queue stale data");
        tokio::task::yield_now().await;
        assert!(
            !waiting.is_finished(),
            "pruning an inactive lane must not end channel egress"
        );

        outbound
            .try_control(ObjectRpcChannelWireFrame::Cancel {
                call_id: ObjectRpcChannelCallId(3),
                reason: "egress remains live".to_string(),
            })
            .expect("queue follow-up control");
        let (frame, mut egress) = timeout(Duration::from_secs(1), &mut waiting)
            .await
            .expect("live egress timeout")
            .expect("egress task");
        assert!(matches!(
            frame,
            Some(ObjectRpcChannelTransportFrame::Control(
                ObjectRpcChannelWireFrame::Cancel { call_id, .. }
            )) if call_id == ObjectRpcChannelCallId(3)
        ));
        egress.complete_frame_write(true);
    }
}
