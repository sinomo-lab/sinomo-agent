use serde::{Deserialize, Serialize};
use sinomo_base::{ClusterEndpointRecord, ClusterTransportKind};
use std::fmt;
use std::mem::size_of;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock, TryLockError};
use std::time::Instant;

const CAPTURE_DISABLED: u8 = 0;
const CAPTURE_ACTIVE: u8 = 1;
const CAPTURE_STOPPING: u8 = 2;

/// Hard bound for event storage owned by one in-process timeline capture.
pub const NETWORK_TIMELINE_MAX_CAPTURE_BYTES: usize = 16 * 1024 * 1024;
/// A bounded default suitable for short diagnostic captures.
pub const NETWORK_TIMELINE_DEFAULT_MAX_EVENTS: usize = 32 * 1024;
/// Endpoint identities retained alongside one bounded diagnostic capture.
///
/// Timeline recording remains event-bounded; this second, small bound lets a
/// short-lived bootstrap client expose the exact endpoint behind a compact
/// event correlation token without putting endpoint strings in every event.
pub const NETWORK_TIMELINE_MAX_ENDPOINT_IDENTITIES: usize = 256;

/// Stable numeric stage identifiers used by the zero-allocation recorder and
/// translated to names only after a capture has stopped.
pub mod network_timeline_stage {
    pub const CALL_PREPARE: u16 = 1;
    pub const CALL_ADMISSION: u16 = 2;
    pub const TX_QUEUE_WAIT: u16 = 3;
    pub const FRAME_WRITE: u16 = 4;
    pub const FRAME_INGRESS: u16 = 5;
    pub const SERVER_QUEUE_WAIT: u16 = 6;
    pub const PRINCIPAL_VALIDATE: u16 = 7;
    pub const ROUTE_AUTHORIZE: u16 = 8;
    pub const HANDLER_DISPATCH: u16 = 9;
    pub const RESPONSE_PREPARE: u16 = 10;
    pub const PENDING_RESOLVE: u16 = 11;
    pub const CALL_COMPLETE: u16 = 12;
    pub const ROUTE_PLAN: u16 = 13;
    pub const CHANNEL_LOOKUP: u16 = 14;
    pub const FORWARD_DECODE: u16 = 15;
    pub const FORWARD_ENCODE: u16 = 16;
    pub const ROUTE_NEXT_HOP_KEY: u16 = 17;
    pub const ROUTE_TARGET_HINTS: u16 = 18;
    pub const REQUEST_SIGN: u16 = 19;
    pub const RELAY_PREPARE: u16 = 20;
    pub const ENDPOINT_OPEN: u16 = 21;
    pub const REQUEST_VERIFY: u16 = 22;
    pub const BODY_BIND: u16 = 23;
    pub const ROUTE_POLICY: u16 = 24;
    pub const CHANNEL_CLOSE: u16 = 25;
    /// Timestamp marker at the receiver's first successful physical read for
    /// a frame. Its duration is always zero; pair it with the sender's socket
    /// write completion to isolate kernel delivery and runtime wakeup.
    pub const FRAME_FIRST_READ: u16 = 26;
    /// Time from the first successful physical read through complete object
    /// frame body read and its bounded allocation.
    pub const FRAME_READ: u16 = 27;
    /// SOBJ frame encoding and allocation before the physical write begins.
    pub const FRAME_ENCODE: u16 = 28;
    /// Physical frame write/flush after SOBJ encoding has completed.
    pub const FRAME_SOCKET_WRITE: u16 = 29;
    /// SOBJ decode after the complete encoded frame body has been read.
    pub const FRAME_DECODE: u16 = 30;
    /// Time between Tokio's I/O readiness Waker firing for the persistent
    /// carrier and the carrier task next polling its first frame read. This is
    /// the receiver-local runnable scheduling delay; it deliberately excludes
    /// kernel delivery and the subsequent read syscall.
    pub const FRAME_READY_TO_POLL: u16 = 31;
    /// Synchronous routing-table cache lookup, including validation of a
    /// cached entry. `aux == 1` denotes a cache hit and `aux == 0` a miss.
    pub const ROUTE_CACHE_LOOKUP: u16 = 32;
    /// Time from submitting an uncached route calculation to Tokio's blocking
    /// pool until the blocking worker starts running it.
    pub const ROUTE_BLOCKING_QUEUE: u16 = 33;
    /// Wall time spent rebuilding an uncached cluster delivery routing table
    /// on a Tokio blocking worker.
    pub const ROUTE_TABLE_BUILD: u16 = 34;
    /// Wall time spent selecting a route from a freshly built routing table
    /// on a Tokio blocking worker.
    pub const ROUTE_TABLE_PLAN: u16 = 35;
    /// Total wall time spent attempting to reuse the relay envelope's planned
    /// path. `aux == 1` denotes a usable plan and `aux == 0` a fallback.
    pub const ROUTE_RELAY_PLANNED_PATH: u16 = 36;
    /// Wall time spent reading the local connectivity graph while selecting
    /// relay endpoints from a planned path or target hint.
    pub const ROUTE_RELAY_CONNECTIVITY_READ: u16 = 37;
    /// Total wall time spent attempting the dynamic routing-table fallback.
    /// `aux == 1` denotes a usable plan and `aux == 0` a further fallback.
    pub const ROUTE_RELAY_DYNAMIC_FALLBACK: u16 = 38;
    /// Total wall time spent attempting the relay envelope's target endpoint
    /// hints. `aux == 1` denotes a usable plan and `aux == 0` no route.
    pub const ROUTE_RELAY_TARGET_HINT_FALLBACK: u16 = 39;
}

pub mod network_timeline_flag {
    pub const REQUEST: u16 = 1 << 0;
    pub const RESPONSE: u16 = 1 << 1;
    pub const LOCAL_CARRIER: u16 = 1 << 2;
    pub const TUNNEL_CARRIER: u16 = 1 << 3;
    pub const SUCCESS: u16 = 1 << 4;
    pub const CONTROL_FRAME: u16 = 1 << 5;
    pub const DATA_FRAME: u16 = 1 << 6;
}

/// Stable `aux` values for `route.channel-lookup` events. The event's
/// `connection_id` identifies an available/closed logical channel and its
/// `bytes` field retains the call-site attempt/context counter.
pub mod network_timeline_channel_lookup_outcome {
    pub const INCOMPATIBLE: u32 = 0;
    pub const AVAILABLE: u32 = 1;
    pub const ABSENT: u32 = 2;
    pub const CLOSED: u32 = 3;
    pub const ERROR: u32 = 4;
}

pub const fn network_timeline_stage_name(stage: u16) -> &'static str {
    match stage {
        network_timeline_stage::CALL_PREPARE => "call.prepare",
        network_timeline_stage::CALL_ADMISSION => "call.admission",
        network_timeline_stage::TX_QUEUE_WAIT => "channel.tx-queue-wait",
        network_timeline_stage::FRAME_WRITE => "frame.write",
        network_timeline_stage::FRAME_INGRESS => "frame.ingress",
        network_timeline_stage::SERVER_QUEUE_WAIT => "server.queue-wait",
        network_timeline_stage::PRINCIPAL_VALIDATE => "auth.principal-validate",
        network_timeline_stage::ROUTE_AUTHORIZE => "auth.route-authorize",
        network_timeline_stage::HANDLER_DISPATCH => "handler.dispatch",
        network_timeline_stage::RESPONSE_PREPARE => "response.prepare",
        network_timeline_stage::PENDING_RESOLVE => "pending.resolve",
        network_timeline_stage::CALL_COMPLETE => "call.complete",
        network_timeline_stage::ROUTE_PLAN => "route.plan",
        network_timeline_stage::CHANNEL_LOOKUP => "route.channel-lookup",
        network_timeline_stage::FORWARD_DECODE => "forward.decode",
        network_timeline_stage::FORWARD_ENCODE => "forward.encode",
        network_timeline_stage::ROUTE_NEXT_HOP_KEY => "route.next-hop-key",
        network_timeline_stage::ROUTE_TARGET_HINTS => "route.target-hints",
        network_timeline_stage::REQUEST_SIGN => "auth.request-sign",
        network_timeline_stage::RELAY_PREPARE => "forward.prepare",
        network_timeline_stage::ENDPOINT_OPEN => "route.endpoint-open",
        network_timeline_stage::REQUEST_VERIFY => "auth.request-verify",
        network_timeline_stage::BODY_BIND => "body.bind-signed-inner",
        network_timeline_stage::ROUTE_POLICY => "auth.route-policy",
        network_timeline_stage::CHANNEL_CLOSE => "channel.close",
        network_timeline_stage::FRAME_FIRST_READ => "frame.first-read",
        network_timeline_stage::FRAME_READ => "frame.read",
        network_timeline_stage::FRAME_ENCODE => "frame.encode",
        network_timeline_stage::FRAME_SOCKET_WRITE => "frame.socket-write",
        network_timeline_stage::FRAME_DECODE => "frame.decode",
        network_timeline_stage::FRAME_READY_TO_POLL => "frame.ready-to-poll",
        network_timeline_stage::ROUTE_CACHE_LOOKUP => "route.cache-lookup",
        network_timeline_stage::ROUTE_BLOCKING_QUEUE => "route.blocking-queue",
        network_timeline_stage::ROUTE_TABLE_BUILD => "route.table-build",
        network_timeline_stage::ROUTE_TABLE_PLAN => "route.table-plan",
        network_timeline_stage::ROUTE_RELAY_PLANNED_PATH => "route.relay-planned-path",
        network_timeline_stage::ROUTE_RELAY_CONNECTIVITY_READ => "route.relay-connectivity-read",
        network_timeline_stage::ROUTE_RELAY_DYNAMIC_FALLBACK => "route.relay-dynamic-fallback",
        network_timeline_stage::ROUTE_RELAY_TARGET_HINT_FALLBACK => {
            "route.relay-target-hint-fallback"
        }
        _ => "unknown",
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NetworkTimelineTraceContext {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub trace_flags: u8,
}

/// One allocation-free timeline record. Stages and flags are deliberately
/// numeric so hot paths do not allocate or format strings.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NetworkTimelineEvent {
    pub sequence: u64,
    pub monotonic_ns: u64,
    pub duration_ns: u64,
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub connection_id: u64,
    /// Stable capture-local link to [`NetworkTimelineEndpointIdentity`].
    ///
    /// This is separate from `connection_id`: bootstrap transports may open a
    /// one-shot stream without creating a logical channel connection ID.
    pub endpoint_correlation_id: u64,
    pub call_id: u64,
    pub frame_id: u64,
    pub stage: u16,
    pub flags: u16,
    pub bytes: u32,
    pub aux: u32,
}

impl NetworkTimelineEvent {
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        trace: NetworkTimelineTraceContext,
        stage: u16,
        flags: u16,
        connection_id: u64,
        call_id: u64,
        frame_id: u64,
        bytes: u32,
        aux: u32,
        duration_ns: u64,
    ) -> Self {
        Self {
            sequence: 0,
            monotonic_ns: 0,
            duration_ns,
            trace_id: trace.trace_id,
            span_id: trace.span_id,
            connection_id,
            endpoint_correlation_id: 0,
            call_id,
            frame_id,
            stage,
            flags,
            bytes,
            aux,
        }
    }

    pub const fn with_endpoint_correlation_id(mut self, endpoint_correlation_id: u64) -> Self {
        self.endpoint_correlation_id = endpoint_correlation_id;
        self
    }
}

pub const NETWORK_TIMELINE_EVENT_BYTES: usize = size_of::<NetworkTimelineEvent>();

/// Exact endpoint identity associated with compact timeline events.
///
/// `correlation_id` is derived from the durable endpoint ID, so independent
/// process captures can be joined without treating an address or transient
/// socket connection as identity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NetworkTimelineEndpointIdentity {
    pub correlation_id: u64,
    pub endpoint_id: String,
    pub node_id: String,
    pub transport: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NetworkTimelineSnapshot {
    pub started_monotonic_ns: u64,
    pub captured_monotonic_ns: u64,
    pub max_events: usize,
    pub events: Vec<NetworkTimelineEvent>,
    pub endpoint_identities: Vec<NetworkTimelineEndpointIdentity>,
    pub dropped_events: u64,
    pub overflowed: bool,
    pub active: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NetworkTimelineCaptureError {
    InvalidCapacity,
    CaptureMemoryLimitExceeded {
        requested_bytes: usize,
        max_bytes: usize,
    },
    AllocationFailed,
    AlreadyActive,
    NotActive,
}

impl fmt::Display for NetworkTimelineCaptureError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCapacity => formatter.write_str("timeline capacity must be non-zero"),
            Self::CaptureMemoryLimitExceeded {
                requested_bytes,
                max_bytes,
            } => write!(
                formatter,
                "timeline capture requires {requested_bytes} bytes; limit is {max_bytes} bytes"
            ),
            Self::AllocationFailed => formatter.write_str("timeline capture allocation failed"),
            Self::AlreadyActive => formatter.write_str("timeline capture is already active"),
            Self::NotActive => formatter.write_str("timeline capture is not active"),
        }
    }
}

impl std::error::Error for NetworkTimelineCaptureError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NetworkTimelineTraceparentError {
    InvalidLength,
    InvalidDelimiter,
    UnsupportedVersion,
    InvalidHex,
    ZeroTraceId,
    ZeroSpanId,
}

impl fmt::Display for NetworkTimelineTraceparentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidLength => "traceparent has an invalid length",
            Self::InvalidDelimiter => "traceparent has an invalid delimiter",
            Self::UnsupportedVersion => "traceparent version is not supported",
            Self::InvalidHex => "traceparent contains invalid hexadecimal digits",
            Self::ZeroTraceId => "traceparent trace id must be non-zero",
            Self::ZeroSpanId => "traceparent span id must be non-zero",
        })
    }
}

impl std::error::Error for NetworkTimelineTraceparentError {}

struct TimelineCapture {
    started_monotonic_ns: u64,
    max_events: usize,
    next_sequence: u64,
    events: Vec<NetworkTimelineEvent>,
    endpoint_identities: Vec<NetworkTimelineEndpointIdentity>,
}

static CAPTURE_STATE: AtomicU8 = AtomicU8::new(CAPTURE_DISABLED);
static CAPTURE_GENERATION: AtomicU64 = AtomicU64::new(0);
static CAPTURE_DROPPED_EVENTS: AtomicU64 = AtomicU64::new(0);
static CAPTURE_OVERFLOWED: AtomicBool = AtomicBool::new(false);
static CAPTURE: OnceLock<Mutex<Option<TimelineCapture>>> = OnceLock::new();
static MONOTONIC_ORIGIN: OnceLock<Instant> = OnceLock::new();
#[cfg(test)]
static TEST_CAPTURE: Mutex<()> = Mutex::new(());

#[cfg(test)]
pub(crate) fn network_timeline_test_capture_guard() -> std::sync::MutexGuard<'static, ()> {
    TEST_CAPTURE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub fn parse_network_timeline_traceparent(
    traceparent: &str,
) -> Result<NetworkTimelineTraceContext, NetworkTimelineTraceparentError> {
    let encoded = traceparent.as_bytes();
    if encoded.len() != 55 {
        return Err(NetworkTimelineTraceparentError::InvalidLength);
    }
    if encoded[2] != b'-' || encoded[35] != b'-' || encoded[52] != b'-' {
        return Err(NetworkTimelineTraceparentError::InvalidDelimiter);
    }
    if &encoded[..2] != b"00" {
        return Err(NetworkTimelineTraceparentError::UnsupportedVersion);
    }

    let mut trace_id = [0_u8; 16];
    let mut span_id = [0_u8; 8];
    decode_hex(&encoded[3..35], &mut trace_id)?;
    decode_hex(&encoded[36..52], &mut span_id)?;
    let mut trace_flags = [0_u8; 1];
    decode_hex(&encoded[53..55], &mut trace_flags)?;

    if trace_id.iter().all(|byte| *byte == 0) {
        return Err(NetworkTimelineTraceparentError::ZeroTraceId);
    }
    if span_id.iter().all(|byte| *byte == 0) {
        return Err(NetworkTimelineTraceparentError::ZeroSpanId);
    }

    Ok(NetworkTimelineTraceContext {
        trace_id,
        span_id,
        trace_flags: trace_flags[0],
    })
}

/// Returns a compact trace token only for an explicitly sampled request while
/// a bounded capture is active. Ordinary requests therefore pay one relaxed
/// atomic load and branch, without inspecting metadata or reading a clock.
#[inline]
pub fn network_timeline_trace_context_for_request(
    request: &crate::ObjectRpcRequest,
) -> Option<NetworkTimelineTraceContext> {
    if !network_timeline_capture_is_active() || !request.observe_sampled() {
        return None;
    }
    request
        .traceparent()
        .and_then(|traceparent| parse_network_timeline_traceparent(traceparent).ok())
}

fn decode_hex(encoded: &[u8], decoded: &mut [u8]) -> Result<(), NetworkTimelineTraceparentError> {
    if encoded.len() != decoded.len() * 2 {
        return Err(NetworkTimelineTraceparentError::InvalidLength);
    }
    for (output, pair) in decoded.iter_mut().zip(encoded.chunks_exact(2)) {
        let high = decode_hex_digit(pair[0])?;
        let low = decode_hex_digit(pair[1])?;
        *output = (high << 4) | low;
    }
    Ok(())
}

fn decode_hex_digit(digit: u8) -> Result<u8, NetworkTimelineTraceparentError> {
    match digit {
        b'0'..=b'9' => Ok(digit - b'0'),
        b'a'..=b'f' => Ok(digit - b'a' + 10),
        b'A'..=b'F' => Ok(digit - b'A' + 10),
        _ => Err(NetworkTimelineTraceparentError::InvalidHex),
    }
}

pub fn network_timeline_monotonic_ns() -> u64 {
    let elapsed = MONOTONIC_ORIGIN
        .get_or_init(Instant::now)
        .elapsed()
        .as_nanos();
    elapsed.min(u128::from(u64::MAX)) as u64
}

/// Registers an endpoint for a currently active bounded capture and returns a
/// stable numeric token suitable for timeline events.
///
/// This is a cold diagnostic path. Ordinary forwarding never calls it unless
/// a timeline is explicitly active, and the exact identity table is bounded
/// independently from event storage.
pub fn register_network_timeline_endpoint(endpoint: &ClusterEndpointRecord) -> Option<u64> {
    if CAPTURE_STATE.load(Ordering::Relaxed) != CAPTURE_ACTIVE {
        return None;
    }
    let storage = CAPTURE.get()?;
    let mut slot = storage
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if CAPTURE_STATE.load(Ordering::Relaxed) != CAPTURE_ACTIVE {
        return None;
    }
    let capture = slot.as_mut()?;
    let endpoint_id = endpoint.endpoint_id.as_str();
    if let Some(identity) = capture
        .endpoint_identities
        .iter()
        .find(|identity| identity.endpoint_id == endpoint_id)
    {
        return Some(identity.correlation_id);
    }
    if capture.endpoint_identities.len() >= NETWORK_TIMELINE_MAX_ENDPOINT_IDENTITIES {
        return None;
    }

    let mut correlation_id = stable_endpoint_correlation_id(endpoint_id);
    while capture.endpoint_identities.iter().any(|identity| {
        identity.correlation_id == correlation_id && identity.endpoint_id != endpoint_id
    }) {
        correlation_id = correlation_id.wrapping_add(1).max(1);
    }
    capture.endpoint_identities.try_reserve(1).ok()?;
    capture
        .endpoint_identities
        .push(NetworkTimelineEndpointIdentity {
            correlation_id,
            endpoint_id: endpoint_id.to_string(),
            node_id: endpoint.node_id.to_string(),
            transport: network_timeline_transport_name(&endpoint.transport).to_string(),
        });
    Some(correlation_id)
}

fn stable_endpoint_correlation_id(endpoint_id: &str) -> u64 {
    // FNV-1a is deliberately fixed here rather than using DefaultHasher,
    // whose output is not a cross-process persistence contract.
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in b"sinomo.network.endpoint.v1\0"
        .iter()
        .chain(endpoint_id.as_bytes())
    {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash.max(1)
}

fn network_timeline_transport_name(transport: &ClusterTransportKind) -> &'static str {
    match transport {
        ClusterTransportKind::InProcess => "in-process",
        ClusterTransportKind::Loopback => "loopback",
        ClusterTransportKind::LanTcp => "lan-tcp",
        ClusterTransportKind::LanQuic => "lan-quic",
        ClusterTransportKind::Iroh => "iroh",
        ClusterTransportKind::WebRtc => "web-rtc",
        ClusterTransportKind::TurnRelay => "turn-relay",
        ClusterTransportKind::HttpsRelay => "https-relay",
    }
}

#[inline]
pub fn network_timeline_capture_is_active() -> bool {
    CAPTURE_STATE.load(Ordering::Relaxed) == CAPTURE_ACTIVE
}

/// Returns a non-zero token for the currently active capture. Readiness probes
/// retain this token across a socket wake so a stopped capture cannot leak a
/// timestamp into a later capture.
#[inline]
pub(crate) fn network_timeline_capture_generation() -> u64 {
    if CAPTURE_STATE.load(Ordering::Relaxed) != CAPTURE_ACTIVE {
        return 0;
    }
    CAPTURE_GENERATION.load(Ordering::Acquire)
}

pub fn start_network_timeline_capture(
    max_events: usize,
) -> Result<(), NetworkTimelineCaptureError> {
    let requested_bytes = capture_bytes(max_events)?;
    if requested_bytes > NETWORK_TIMELINE_MAX_CAPTURE_BYTES {
        return Err(NetworkTimelineCaptureError::CaptureMemoryLimitExceeded {
            requested_bytes,
            max_bytes: NETWORK_TIMELINE_MAX_CAPTURE_BYTES,
        });
    }

    let storage = CAPTURE.get_or_init(|| Mutex::new(None));
    let mut slot = storage
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if CAPTURE_STATE.load(Ordering::Relaxed) != CAPTURE_DISABLED || slot.is_some() {
        return Err(NetworkTimelineCaptureError::AlreadyActive);
    }

    let mut events = Vec::new();
    events
        .try_reserve_exact(max_events)
        .map_err(|_| NetworkTimelineCaptureError::AllocationFailed)?;
    *slot = Some(TimelineCapture {
        started_monotonic_ns: network_timeline_monotonic_ns(),
        max_events,
        next_sequence: 1,
        events,
        endpoint_identities: Vec::new(),
    });
    CAPTURE_DROPPED_EVENTS.store(0, Ordering::Relaxed);
    CAPTURE_OVERFLOWED.store(false, Ordering::Relaxed);
    let next_generation = CAPTURE_GENERATION
        .load(Ordering::Relaxed)
        .wrapping_add(1)
        .max(1);
    CAPTURE_GENERATION.store(next_generation, Ordering::Release);
    CAPTURE_STATE.store(CAPTURE_ACTIVE, Ordering::Release);
    Ok(())
}

pub fn snapshot_network_timeline_capture()
-> Result<NetworkTimelineSnapshot, NetworkTimelineCaptureError> {
    if CAPTURE_STATE.load(Ordering::Acquire) != CAPTURE_ACTIVE {
        return Err(NetworkTimelineCaptureError::NotActive);
    }
    let storage = CAPTURE
        .get()
        .ok_or(NetworkTimelineCaptureError::NotActive)?;
    let slot = storage
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if CAPTURE_STATE.load(Ordering::Relaxed) != CAPTURE_ACTIVE {
        return Err(NetworkTimelineCaptureError::NotActive);
    }
    let capture = slot
        .as_ref()
        .ok_or(NetworkTimelineCaptureError::NotActive)?;
    let mut events = Vec::new();
    events
        .try_reserve_exact(capture.events.len())
        .map_err(|_| NetworkTimelineCaptureError::AllocationFailed)?;
    events.extend_from_slice(&capture.events);
    let mut endpoint_identities = Vec::new();
    endpoint_identities
        .try_reserve_exact(capture.endpoint_identities.len())
        .map_err(|_| NetworkTimelineCaptureError::AllocationFailed)?;
    endpoint_identities.extend(capture.endpoint_identities.iter().cloned());
    Ok(NetworkTimelineSnapshot {
        started_monotonic_ns: capture.started_monotonic_ns,
        captured_monotonic_ns: network_timeline_monotonic_ns(),
        max_events: capture.max_events,
        events,
        endpoint_identities,
        dropped_events: CAPTURE_DROPPED_EVENTS.load(Ordering::Relaxed),
        overflowed: CAPTURE_OVERFLOWED.load(Ordering::Relaxed),
        active: true,
    })
}

pub fn stop_network_timeline_capture()
-> Result<NetworkTimelineSnapshot, NetworkTimelineCaptureError> {
    CAPTURE_STATE
        .compare_exchange(
            CAPTURE_ACTIVE,
            CAPTURE_STOPPING,
            Ordering::AcqRel,
            Ordering::Acquire,
        )
        .map_err(|_| NetworkTimelineCaptureError::NotActive)?;

    let storage = match CAPTURE.get() {
        Some(storage) => storage,
        None => {
            CAPTURE_STATE.store(CAPTURE_DISABLED, Ordering::Release);
            return Err(NetworkTimelineCaptureError::NotActive);
        }
    };
    let mut slot = storage
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let capture = match slot.take() {
        Some(capture) => capture,
        None => {
            CAPTURE_STATE.store(CAPTURE_DISABLED, Ordering::Release);
            return Err(NetworkTimelineCaptureError::NotActive);
        }
    };
    let snapshot = NetworkTimelineSnapshot {
        started_monotonic_ns: capture.started_monotonic_ns,
        captured_monotonic_ns: network_timeline_monotonic_ns(),
        max_events: capture.max_events,
        events: capture.events,
        endpoint_identities: capture.endpoint_identities,
        dropped_events: CAPTURE_DROPPED_EVENTS.load(Ordering::Relaxed),
        overflowed: CAPTURE_OVERFLOWED.load(Ordering::Relaxed),
        active: false,
    };
    CAPTURE_STATE.store(CAPTURE_DISABLED, Ordering::Release);
    Ok(snapshot)
}

/// Records an event with a caller-supplied monotonic timestamp. When capture is
/// disabled this performs one relaxed atomic load and branch, then returns.
#[inline]
pub fn record_network_timeline_event(event: NetworkTimelineEvent) {
    if CAPTURE_STATE.load(Ordering::Relaxed) != CAPTURE_ACTIVE {
        return;
    }
    record_network_timeline_event_enabled(event);
}

/// Records an event stamped at the recording point. Timestamp acquisition is
/// intentionally after the disabled-path branch.
#[inline]
pub fn record_network_timeline_event_now(mut event: NetworkTimelineEvent) {
    if CAPTURE_STATE.load(Ordering::Relaxed) != CAPTURE_ACTIVE {
        return;
    }
    event.monotonic_ns = network_timeline_monotonic_ns();
    record_network_timeline_event_enabled(event);
}

#[cold]
fn record_network_timeline_event_enabled(mut event: NetworkTimelineEvent) {
    let Some(storage) = CAPTURE.get() else {
        CAPTURE_DROPPED_EVENTS.fetch_add(1, Ordering::Relaxed);
        return;
    };
    let mut slot = match storage.try_lock() {
        Ok(slot) => slot,
        Err(TryLockError::WouldBlock) | Err(TryLockError::Poisoned(_)) => {
            CAPTURE_DROPPED_EVENTS.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };
    if CAPTURE_STATE.load(Ordering::Relaxed) != CAPTURE_ACTIVE {
        return;
    }
    let Some(capture) = slot.as_mut() else {
        CAPTURE_DROPPED_EVENTS.fetch_add(1, Ordering::Relaxed);
        return;
    };
    if capture.events.len() == capture.max_events {
        CAPTURE_OVERFLOWED.store(true, Ordering::Relaxed);
        CAPTURE_DROPPED_EVENTS.fetch_add(1, Ordering::Relaxed);
        return;
    }
    event.sequence = capture.next_sequence;
    capture.next_sequence = capture.next_sequence.saturating_add(1);
    capture.events.push(event);
}

fn capture_bytes(max_events: usize) -> Result<usize, NetworkTimelineCaptureError> {
    if max_events == 0 {
        return Err(NetworkTimelineCaptureError::InvalidCapacity);
    }
    max_events.checked_mul(NETWORK_TIMELINE_EVENT_BYTES).ok_or(
        NetworkTimelineCaptureError::CaptureMemoryLimitExceeded {
            requested_bytes: usize::MAX,
            max_bytes: NETWORK_TIMELINE_MAX_CAPTURE_BYTES,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn serial_capture_test() -> std::sync::MutexGuard<'static, ()> {
        network_timeline_test_capture_guard()
    }

    fn ensure_capture_stopped() {
        if network_timeline_capture_is_active() {
            let _ = stop_network_timeline_capture();
        }
    }

    fn test_event(stage: u16) -> NetworkTimelineEvent {
        NetworkTimelineEvent::new(
            NetworkTimelineTraceContext {
                trace_id: [1; 16],
                span_id: [2; 8],
                trace_flags: 1,
            },
            stage,
            0,
            3,
            4,
            5,
            6,
            7,
            8,
        )
    }

    #[test]
    fn traceparent_parser_extracts_ids_and_rejects_invalid_ids() {
        assert_eq!(
            network_timeline_stage_name(network_timeline_stage::FRAME_FIRST_READ),
            "frame.first-read"
        );
        assert_eq!(
            network_timeline_stage_name(network_timeline_stage::FRAME_READ),
            "frame.read"
        );
        assert_eq!(
            network_timeline_stage_name(network_timeline_stage::FRAME_ENCODE),
            "frame.encode"
        );
        assert_eq!(
            network_timeline_stage_name(network_timeline_stage::FRAME_SOCKET_WRITE),
            "frame.socket-write"
        );
        assert_eq!(
            network_timeline_stage_name(network_timeline_stage::FRAME_DECODE),
            "frame.decode"
        );
        assert_eq!(
            network_timeline_stage_name(network_timeline_stage::FRAME_READY_TO_POLL),
            "frame.ready-to-poll"
        );
        assert_eq!(
            network_timeline_stage_name(network_timeline_stage::ROUTE_CACHE_LOOKUP),
            "route.cache-lookup"
        );
        assert_eq!(
            network_timeline_stage_name(network_timeline_stage::ROUTE_BLOCKING_QUEUE),
            "route.blocking-queue"
        );
        assert_eq!(
            network_timeline_stage_name(network_timeline_stage::ROUTE_TABLE_BUILD),
            "route.table-build"
        );
        assert_eq!(
            network_timeline_stage_name(network_timeline_stage::ROUTE_TABLE_PLAN),
            "route.table-plan"
        );
        assert_eq!(
            network_timeline_stage_name(network_timeline_stage::ROUTE_RELAY_PLANNED_PATH),
            "route.relay-planned-path"
        );
        assert_eq!(
            network_timeline_stage_name(network_timeline_stage::ROUTE_RELAY_CONNECTIVITY_READ),
            "route.relay-connectivity-read"
        );
        assert_eq!(
            network_timeline_stage_name(network_timeline_stage::ROUTE_RELAY_DYNAMIC_FALLBACK),
            "route.relay-dynamic-fallback"
        );
        assert_eq!(
            network_timeline_stage_name(network_timeline_stage::ROUTE_RELAY_TARGET_HINT_FALLBACK),
            "route.relay-target-hint-fallback"
        );
        let parsed = parse_network_timeline_traceparent(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        )
        .expect("parse traceparent");
        assert_eq!(parsed.trace_id[0..4], [0x4b, 0xf9, 0x2f, 0x35]);
        assert_eq!(parsed.span_id[0..4], [0x00, 0xf0, 0x67, 0xaa]);
        assert_eq!(parsed.trace_flags, 1);

        assert_eq!(
            parse_network_timeline_traceparent(
                "00-00000000000000000000000000000000-00f067aa0ba902b7-01"
            ),
            Err(NetworkTimelineTraceparentError::ZeroTraceId)
        );
        assert_eq!(
            parse_network_timeline_traceparent(
                "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01"
            ),
            Err(NetworkTimelineTraceparentError::ZeroSpanId)
        );
        assert_eq!(
            parse_network_timeline_traceparent(
                "00-4bf92f3577b34da6a3ce929d0e0e473x-00f067aa0ba902b7-01"
            ),
            Err(NetworkTimelineTraceparentError::InvalidHex)
        );
    }

    #[test]
    fn capture_validates_memory_cap_and_preallocates() {
        let _serial = serial_capture_test();
        ensure_capture_stopped();
        assert_eq!(NETWORK_TIMELINE_EVENT_BYTES, 96);
        assert_eq!(
            start_network_timeline_capture(0),
            Err(NetworkTimelineCaptureError::InvalidCapacity)
        );

        let over_limit = NETWORK_TIMELINE_MAX_CAPTURE_BYTES / NETWORK_TIMELINE_EVENT_BYTES + 1;
        assert!(matches!(
            start_network_timeline_capture(over_limit),
            Err(NetworkTimelineCaptureError::CaptureMemoryLimitExceeded { .. })
        ));
        assert!(!network_timeline_capture_is_active());

        start_network_timeline_capture(7).expect("start bounded capture");
        let storage = CAPTURE.get().expect("capture storage");
        let slot = storage
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(slot.as_ref().expect("active capture").events.capacity() >= 7);
        drop(slot);
        stop_network_timeline_capture().expect("stop bounded capture");
    }

    #[test]
    fn endpoint_identity_is_exact_bounded_and_stable_across_captures() {
        let _serial = serial_capture_test();
        ensure_capture_stopped();
        let mut endpoint = ClusterEndpointRecord::loopback(
            sinomo_base::Id::derived("timeline-test-node", b"node-a"),
            "node-a:21353",
        );
        endpoint.endpoint_id = sinomo_base::Id::derived("timeline-test-endpoint", b"node-a:21353");
        endpoint.transport = ClusterTransportKind::LanTcp;

        start_network_timeline_capture(4).expect("start first capture");
        let first = register_network_timeline_endpoint(&endpoint).expect("register endpoint");
        assert_eq!(
            register_network_timeline_endpoint(&endpoint),
            Some(first),
            "one capture must reuse the same correlation token"
        );
        let first_snapshot = stop_network_timeline_capture().expect("stop first capture");
        assert_eq!(first_snapshot.endpoint_identities.len(), 1);
        assert_eq!(
            first_snapshot.endpoint_identities[0],
            NetworkTimelineEndpointIdentity {
                correlation_id: first,
                endpoint_id: endpoint.endpoint_id.to_string(),
                node_id: endpoint.node_id.to_string(),
                transport: "lan-tcp".to_string(),
            }
        );

        start_network_timeline_capture(4).expect("start second capture");
        let second = register_network_timeline_endpoint(&endpoint).expect("register endpoint");
        stop_network_timeline_capture().expect("stop second capture");
        assert_eq!(first, second, "endpoint token must be process-independent");
    }

    #[test]
    fn bounded_capture_reports_overflow_and_drops() {
        let _serial = serial_capture_test();
        ensure_capture_stopped();
        start_network_timeline_capture(2).expect("start bounded capture");
        record_network_timeline_event_now(test_event(10));
        record_network_timeline_event_now(test_event(11));
        record_network_timeline_event_now(test_event(12));

        let snapshot = stop_network_timeline_capture().expect("stop bounded capture");
        assert_eq!(snapshot.events.len(), 2);
        assert_eq!(snapshot.events[0].sequence, 1);
        assert_eq!(snapshot.events[1].sequence, 2);
        assert_eq!(snapshot.events[0].stage, 10);
        assert_eq!(snapshot.events[1].stage, 11);
        assert_eq!(snapshot.dropped_events, 1);
        assert!(snapshot.overflowed);
        assert!(!snapshot.active);
    }

    #[test]
    fn disabled_recording_does_not_initialize_or_mutate_capture() {
        let _serial = serial_capture_test();
        ensure_capture_stopped();
        let dropped_before = CAPTURE_DROPPED_EVENTS.load(Ordering::Relaxed);
        record_network_timeline_event_now(test_event(20));
        assert_eq!(
            CAPTURE_DROPPED_EVENTS.load(Ordering::Relaxed),
            dropped_before
        );
        assert_eq!(
            snapshot_network_timeline_capture(),
            Err(NetworkTimelineCaptureError::NotActive)
        );
    }
}
