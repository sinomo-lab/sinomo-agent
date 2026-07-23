//! Stream-native SOBJ object RPC.
//!
//! One authenticated duplex byte stream carries exactly one request direction
//! and one response direction. This module deliberately knows nothing about
//! route selection, relay topology, endpoint discovery, or service catalogs.
//! The authenticated stream supplies peer identity and a unique channel
//! binding; this protocol binds its request, body, and response transcripts to
//! that context.

use crate::error::NetworkError;
use crate::object::{ObjectFrameIoOptions, SinomoObjectCodec};
use crate::owned_chunk::{
    AuthenticatedChunkReader, AuthenticatedChunkStream, AuthenticatedChunkWriter, ChunkAllocator,
    OwnedChunk, RPC_CONTROL_PAYLOAD_MAX, observe_chunk_identity,
};
use crate::rpc_body::{
    ObjectRpcBodyChunk, ObjectRpcBodyReceiver, ObjectRpcBodyReceiverFuture, ObjectRpcBodySource,
    ObjectRpcIncomingBody,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sinomo_base::Id;
use sinomo_node::NodeId;
use std::collections::BTreeMap;
use std::future::Future;
use std::io::{self, IoSlice};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::sync::watch;

pub const OBJECT_RPC_STREAM_MAX_HEAD_BYTES: usize = RPC_CONTROL_PAYLOAD_MAX;
pub const OBJECT_RPC_STREAM_INLINE_BODY_BYTES: usize = 8 * 1024;
pub const OBJECT_RPC_STREAM_MAX_DATA_FRAME_BYTES: usize = 64 * 1024;
#[cfg(test)]
pub const OBJECT_RPC_STREAM_RECEIVE_WINDOW_BYTES: usize = 256 * 1024;

const OBJECT_RPC_STREAM_PROTOCOL: &str = "sinomo.object-rpc.stream.v2";
const OBJECT_RPC_STREAM_WIRE_MAGIC: [u8; 4] = *b"SOR2";
const OBJECT_RPC_STREAM_WIRE_PREFIX_BYTES: usize = 9;
const OBJECT_RPC_STREAM_MAX_TERMINAL_BYTES: usize = 16 * 1024;
const OBJECT_RPC_STREAM_MAX_HEAD_CLOCK_SKEW_MILLIS: u64 = 5 * 60 * 1000;
const OBJECT_RPC_STREAM_MIN_PROGRESS_INTERVAL: Duration = Duration::from_millis(10);
const AUTHENTICATED_CONTROL_TAG_BYTES: usize = 32;

const FRAME_REQUEST_HEAD: u8 = 1;
const FRAME_REQUEST_DATA: u8 = 2;
const FRAME_REQUEST_FINISH: u8 = 3;
const FRAME_RESPONSE_HEAD: u8 = 4;
const FRAME_RESPONSE_DATA: u8 = 5;
const FRAME_RESPONSE_FINISH: u8 = 6;
const FRAME_REQUEST_PROGRESS: u8 = 7;
const FRAME_RESPONSE_PROGRESS: u8 = 8;

const AUTH_REQUEST_HEAD: &[u8] = b"sinomo-object-rpc-request-head-v2";
const AUTH_REQUEST_FINISH: &[u8] = b"sinomo-object-rpc-request-finish-v2";
const AUTH_RESPONSE_HEAD: &[u8] = b"sinomo-object-rpc-response-head-v2";
const AUTH_RESPONSE_FINISH: &[u8] = b"sinomo-object-rpc-response-finish-v2";
const AUTH_REQUEST_PROGRESS: &[u8] = b"sinomo-object-rpc-request-progress-v2";
const AUTH_RESPONSE_PROGRESS: &[u8] = b"sinomo-object-rpc-response-progress-v2";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RpcPeerIdentity {
    Authenticated {
        local_node_id: NodeId,
        peer_node_id: NodeId,
    },
    UnauthenticatedLocal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RpcPeerContext {
    pub identity: RpcPeerIdentity,
    pub session_id: Id,
    pub channel_binding: [u8; 32],
}

impl RpcPeerContext {
    pub fn authenticated(
        local_node_id: NodeId,
        peer_node_id: NodeId,
        session_id: Id,
        channel_binding: [u8; 32],
    ) -> Result<Self, NetworkError> {
        if local_node_id == peer_node_id {
            return Err(serialization_error(
                "authenticated RPC peers must have distinct node identities",
            ));
        }
        Ok(Self {
            identity: RpcPeerIdentity::Authenticated {
                local_node_id,
                peer_node_id,
            },
            session_id,
            channel_binding,
        })
    }

    pub fn unauthenticated_local(session_id: Id, channel_binding: [u8; 32]) -> Self {
        Self {
            identity: RpcPeerIdentity::UnauthenticatedLocal,
            session_id,
            channel_binding,
        }
    }

    fn wire_endpoints(&self) -> (Option<NodeId>, Option<NodeId>) {
        match &self.identity {
            RpcPeerIdentity::Authenticated {
                local_node_id,
                peer_node_id,
            } => (Some(local_node_id.clone()), Some(peer_node_id.clone())),
            RpcPeerIdentity::UnauthenticatedLocal => (None, None),
        }
    }

    fn expected_incoming_endpoints(&self) -> (Option<&NodeId>, Option<&NodeId>) {
        match &self.identity {
            RpcPeerIdentity::Authenticated {
                local_node_id,
                peer_node_id,
            } => (Some(peer_node_id), Some(local_node_id)),
            RpcPeerIdentity::UnauthenticatedLocal => (None, None),
        }
    }
}

/// A request or response body supplied through the same API whether its final
/// length and digest are known at head time or deferred to the finish record.
///
/// Replayable sources are hashed in a bounded-memory pre-pass so their length
/// and digest are authenticated in the head. One-pass sources are streamed
/// once and authenticate their observed length and digest in the finish record.
pub struct ObjectRpcStreamBody {
    kind: OutgoingBodyKind,
    supplied_digest: Option<[u8; 32]>,
}

enum OutgoingBodyKind {
    Inline(Bytes),
    Source(ObjectRpcBodySource),
}

impl std::fmt::Debug for ObjectRpcStreamBody {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (kind, total_bytes) = match &self.kind {
            OutgoingBodyKind::Inline(bytes) => ("inline", Some(bytes.len() as u64)),
            OutgoingBodyKind::Source(source) => ("source", source.total_bytes()),
        };
        formatter
            .debug_struct("ObjectRpcStreamBody")
            .field("kind", &kind)
            .field("total_bytes", &total_bytes)
            .field("has_supplied_digest", &self.supplied_digest.is_some())
            .finish()
    }
}

impl Default for ObjectRpcStreamBody {
    fn default() -> Self {
        Self::bytes(Vec::new())
    }
}

impl ObjectRpcStreamBody {
    pub fn bytes(bytes: impl Into<Vec<u8>>) -> Self {
        let bytes = bytes.into();
        if bytes.len() <= OBJECT_RPC_STREAM_INLINE_BODY_BYTES {
            return Self {
                kind: OutgoingBodyKind::Inline(Bytes::from(bytes)),
                supplied_digest: None,
            };
        }
        Self {
            kind: OutgoingBodyKind::Source(ObjectRpcBodySource::bytes(bytes)),
            supplied_digest: None,
        }
    }

    pub fn source(source: ObjectRpcBodySource) -> Self {
        Self {
            kind: OutgoingBodyKind::Source(source),
            supplied_digest: None,
        }
    }

    /// Supplies a producer-computed digest for a source with a known length.
    /// This avoids a replayable pre-pass and lets a one-pass producer use a
    /// known descriptor when its domain already owns a stable content digest.
    #[cfg(test)]
    pub(crate) fn known(
        source: ObjectRpcBodySource,
        digest: [u8; 32],
    ) -> Result<Self, NetworkError> {
        if source.total_bytes().is_none() {
            return Err(serialization_error(
                "a known object RPC stream body digest requires a known length",
            ));
        }
        Ok(Self {
            kind: OutgoingBodyKind::Source(source),
            supplied_digest: Some(digest),
        })
    }

    #[cfg(test)]
    fn one_pass_reader<R>(reader: R, total_bytes: Option<u64>) -> Self
    where
        R: io::Read + Send + 'static,
    {
        Self::source(ObjectRpcBodySource::from_reader(reader, total_bytes))
    }

    #[cfg(test)]
    fn one_pass_async_reader<R>(reader: R, total_bytes: Option<u64>) -> Self
    where
        R: AsyncRead + Send + 'static,
    {
        Self::source(ObjectRpcBodySource::async_reader(reader, total_bytes))
    }
}

#[derive(Debug)]
pub struct ObjectRpcStreamCall {
    pub request_id: Id,
    pub method: String,
    pub operation_id: Option<String>,
    pub metadata: BTreeMap<String, String>,
    /// Opaque, bounded service head authenticated with the RPC head.
    ///
    /// Object RPC does not interpret this value. Service adapters use it for
    /// their typed request head while the body remains incrementally framed.
    pub application_head: Vec<u8>,
    pub body: ObjectRpcStreamBody,
    /// Capture-local diagnostic context. This is never serialized on the
    /// object-RPC wire and is `None` outside an explicitly sampled timeline.
    #[doc(hidden)]
    pub timeline_trace: Option<crate::NetworkTimelineTraceContext>,
}

impl ObjectRpcStreamCall {
    #[cfg(test)]
    fn new(method: impl Into<String>, body: ObjectRpcStreamBody) -> Self {
        Self {
            request_id: Id::generate(),
            method: method.into(),
            operation_id: None,
            metadata: BTreeMap::new(),
            application_head: Vec::new(),
            body,
            timeline_trace: None,
        }
    }

    fn validate(&self) -> Result<(), NetworkError> {
        if self.method.trim().is_empty() {
            return Err(serialization_error(
                "object RPC service method cannot be empty",
            ));
        }
        Ok(())
    }
}

/// Server-visible request head and incrementally readable body.
#[derive(Debug)]
pub struct ObjectRpcStreamIncomingCall {
    pub request_id: Id,
    pub method: String,
    pub operation_id: Option<String>,
    pub metadata: BTreeMap<String, String>,
    pub application_head: Vec<u8>,
    pub body: ObjectRpcStreamIncomingBody,
    /// Time from the server record reader starting until the authenticated
    /// request head and body descriptor are ready for the service adapter.
    #[doc(hidden)]
    pub request_ingress_duration_ns: u64,
}

/// Server-produced response head and streaming body source.
#[derive(Debug)]
pub struct ObjectRpcStreamReply {
    pub request_id: Id,
    pub metadata: BTreeMap<String, String>,
    pub application_head: Vec<u8>,
    pub body: ObjectRpcStreamBody,
    /// Capture-local diagnostic context. This is never serialized.
    #[doc(hidden)]
    pub timeline_trace: Option<crate::NetworkTimelineTraceContext>,
}

impl ObjectRpcStreamReply {
    #[cfg(test)]
    fn new(request_id: Id, body: ObjectRpcStreamBody) -> Self {
        Self {
            request_id,
            metadata: BTreeMap::new(),
            application_head: Vec::new(),
            body,
            timeline_trace: None,
        }
    }
}

/// Client-visible authenticated response head and incremental body.
#[derive(Debug)]
pub struct ObjectRpcStreamResponse {
    pub metadata: BTreeMap<String, String>,
    pub application_head: Vec<u8>,
    pub body: ObjectRpcStreamIncomingBody,
}

#[derive(Debug)]
pub enum RpcDeliveryOutcome<T> {
    NotSent { error: NetworkError },
    Rejected { detail: String },
    PossiblyApplied { error: NetworkError },
    Completed(T),
}

pub(crate) enum RpcResponseConsumption<T> {
    Complete(T),
    Streaming(T),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum BodyDescriptor {
    Inline {
        length: u64,
        digest: [u8; 32],
    },
    Known {
        length: u64,
        digest: [u8; 32],
    },
    /// A one-pass producer whose length is known before the stream begins.
    ///
    /// The authenticated head commits to the length while the authenticated
    /// finish record commits to the digest observed during the single pass.
    /// This is distinct from `Deferred`: trusted proxy hops can preserve an
    /// incoming attachment's declared length without buffering or hashing it
    /// before forwarding.
    KnownLength {
        length: u64,
    },
    Deferred,
}

// Inline bytes are stored separately in the authenticated head. Keeping the
// descriptor shape free of a Vec makes accidental unbounded descriptor use
// impossible; the head's explicit allocation bound remains authoritative.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct InlineBody {
    #[serde(with = "serde_bytes")]
    bytes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct RequestHeadUnsigned {
    protocol: String,
    origin_node_id: Option<NodeId>,
    destination_node_id: Option<NodeId>,
    session_id: Id,
    channel_binding: [u8; 32],
    request_id: Id,
    method: String,
    operation_id: Option<String>,
    metadata: BTreeMap<String, String>,
    #[serde(with = "serde_bytes")]
    application_head: Vec<u8>,
    issued_at_unix_millis: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    response_progress_interval_millis: Option<u64>,
    descriptor: BodyDescriptor,
    inline_body: Option<InlineBody>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum ResponseDisposition {
    Accepted {
        metadata: BTreeMap<String, String>,
        #[serde(with = "serde_bytes")]
        application_head: Vec<u8>,
        descriptor: BodyDescriptor,
        inline_body: Option<InlineBody>,
    },
    Rejected {
        detail: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ResponseHeadUnsigned {
    protocol: String,
    origin_node_id: Option<NodeId>,
    destination_node_id: Option<NodeId>,
    session_id: Id,
    channel_binding: [u8; 32],
    request_id: Id,
    request_head_digest: [u8; 32],
    disposition: ResponseDisposition,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct StreamProgressUnsigned {
    protocol: String,
    origin_node_id: Option<NodeId>,
    destination_node_id: Option<NodeId>,
    session_id: Id,
    channel_binding: [u8; 32],
    request_id: Id,
    head_digest: [u8; 32],
    nonce: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum FinishStatus {
    Completed,
    Rejected { detail: String },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct BodyFinishUnsigned {
    request_id: Id,
    session_id: Id,
    channel_binding: [u8; 32],
    origin_node_id: Option<NodeId>,
    destination_node_id: Option<NodeId>,
    head_digest: [u8; 32],
    length: u64,
    content_digest: [u8; 32],
    status: FinishStatus,
}

struct PreparedOutgoingBody {
    descriptor: BodyDescriptor,
    inline_body: Option<InlineBody>,
    source: Option<ObjectRpcBodySource>,
}

struct IncomingBodySpec {
    direction: IncomingDirection,
    request_id: Id,
    head_digest: [u8; 32],
    progress_head_digest: [u8; 32],
    last_progress_nonce: u64,
    descriptor: BodyDescriptor,
    inline_body: Option<InlineBody>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectRpcBodyVerification {
    pub length: u64,
    pub digest: [u8; 32],
}

#[derive(Clone, Debug)]
pub struct ObjectRpcStreamBodyVerifier {
    receiver: watch::Receiver<Option<Result<IncomingTerminal, NetworkError>>>,
}

impl ObjectRpcStreamBodyVerifier {
    pub async fn verify(mut self) -> Result<ObjectRpcBodyVerification, NetworkError> {
        loop {
            if let Some(result) = self.receiver.borrow().clone() {
                return incoming_body_verification(&result);
            }
            self.receiver.changed().await.map_err(|_| {
                delivery_error(
                    "verify object RPC stream body",
                    "body reader ended without a verification result",
                )
            })?;
        }
    }
}

/// Incremental incoming body. Natural backpressure is bounded to one receive
/// window. Dropping the reader makes the protocol pump discard further data
/// while it still authenticates the finish record.
#[derive(Debug)]
pub struct ObjectRpcStreamIncomingBody {
    body: ObjectRpcIncomingBody,
    verifier: ObjectRpcStreamBodyVerifier,
}

impl ObjectRpcStreamIncomingBody {
    pub fn total_bytes(&self) -> Option<u64> {
        self.body.total_bytes()
    }

    /// Materializes the complete authenticated body without imposing a size
    /// policy. Allocation remains fallible, so a body larger than the process
    /// can represent or reserve returns `ResourceExhausted` instead of
    /// aborting the daemon.
    pub async fn materialize_to_vec(self) -> Result<Vec<u8>, NetworkError> {
        self.collect_to_vec(usize::MAX).await
    }

    pub async fn collect_to_vec(self, max_bytes: usize) -> Result<Vec<u8>, NetworkError> {
        let verifier = self.verifier;
        let output = self.body.collect_to_vec(max_bytes).await?;
        verifier.verify().await?;
        Ok(output)
    }

    pub(crate) fn into_application_body(self) -> ObjectRpcIncomingBody {
        self.body
    }
}

impl AsyncRead for ObjectRpcStreamIncomingBody {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.body).poll_read(context, buffer)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IncomingDirection {
    Request,
    Response,
}

impl IncomingDirection {
    fn data_kind(self) -> u8 {
        match self {
            Self::Request => FRAME_REQUEST_DATA,
            Self::Response => FRAME_RESPONSE_DATA,
        }
    }

    fn finish_kind(self) -> u8 {
        match self {
            Self::Request => FRAME_REQUEST_FINISH,
            Self::Response => FRAME_RESPONSE_FINISH,
        }
    }

    fn finish_domain(self) -> &'static [u8] {
        match self {
            Self::Request => AUTH_REQUEST_FINISH,
            Self::Response => AUTH_RESPONSE_FINISH,
        }
    }

    fn progress_kind(self) -> u8 {
        match self {
            Self::Request => FRAME_REQUEST_PROGRESS,
            Self::Response => FRAME_RESPONSE_PROGRESS,
        }
    }

    fn progress_domain(self) -> &'static [u8] {
        match self {
            Self::Request => AUTH_REQUEST_PROGRESS,
            Self::Response => AUTH_RESPONSE_PROGRESS,
        }
    }
}

#[derive(Clone, Debug)]
enum IncomingTerminal {
    Completed(ObjectRpcBodyVerification),
    Rejected(String),
}

#[derive(Debug)]
struct SendFailure {
    error: NetworkError,
    wrote_any: bool,
}

fn serialization_error(detail: impl Into<String>) -> NetworkError {
    NetworkError::Serialization {
        detail: detail.into(),
    }
}

fn delivery_error(operation: &str, detail: impl Into<String>) -> NetworkError {
    NetworkError::Io {
        operation: operation.to_string(),
        detail: detail.into(),
    }
}

#[inline]
fn timeline_started_ns(trace: Option<crate::NetworkTimelineTraceContext>) -> u64 {
    trace
        .map(|_| crate::network_timeline_monotonic_ns())
        .unwrap_or(0)
}

#[inline]
fn record_rpc_timeline_duration(
    trace: Option<crate::NetworkTimelineTraceContext>,
    stage: u16,
    flags: u16,
    started_ns: u64,
    success: bool,
) {
    let Some(trace) = trace else {
        return;
    };
    crate::record_network_timeline_event_now(crate::NetworkTimelineEvent::new(
        trace,
        stage,
        flags
            | if success {
                crate::network_timeline_flag::SUCCESS
            } else {
                0
            },
        0,
        0,
        0,
        0,
        0,
        crate::network_timeline_monotonic_ns().saturating_sub(started_ns),
    ));
}

struct RpcTimelineStageGuard {
    trace: Option<crate::NetworkTimelineTraceContext>,
    stage: u16,
    flags: u16,
    started_ns: u64,
    success: bool,
}

impl RpcTimelineStageGuard {
    fn new(trace: Option<crate::NetworkTimelineTraceContext>, stage: u16, flags: u16) -> Self {
        Self {
            trace,
            stage,
            flags,
            started_ns: timeline_started_ns(trace),
            success: false,
        }
    }

    fn mark_success(&mut self) {
        self.success = true;
    }
}

impl Drop for RpcTimelineStageGuard {
    fn drop(&mut self) {
        record_rpc_timeline_duration(
            self.trace,
            self.stage,
            self.flags,
            self.started_ns,
            self.success,
        );
    }
}

fn now_unix_millis() -> Result<u64, NetworkError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            serialization_error(format!("system clock precedes Unix epoch: {error}"))
        })?;
    u64::try_from(duration.as_millis())
        .map_err(|_| serialization_error("current Unix time does not fit u64 milliseconds"))
}

fn decode_control<T: serde::de::DeserializeOwned>(payload: &[u8]) -> Result<T, NetworkError> {
    SinomoObjectCodec::network_frame().decode(payload)
}

fn authenticated_control_tag(
    channel_binding: &[u8; 32],
    domain: &[u8],
    unsigned_payload: &[u8],
) -> blake3::Hash {
    let mut hasher = blake3::Hasher::new_keyed(channel_binding);
    hasher.update(domain);
    hasher.update(unsigned_payload);
    hasher.finalize()
}

/// Encodes an authenticated control as `tag || canonical unsigned SOBJ`.
///
/// The unsigned value is deliberately serialized exactly once. The receiver
/// authenticates those same wire bytes before asking SOBJ to decode them, so
/// neither side has to reconstruct the authentication transcript.
fn encode_authenticated_control<T: Serialize>(
    channel_binding: &[u8; 32],
    domain: &[u8],
    value: &T,
    max_bytes: usize,
) -> Result<Vec<u8>, NetworkError> {
    let unsigned_payload = SinomoObjectCodec::network_frame().encode(value)?;
    let payload_len = AUTHENTICATED_CONTROL_TAG_BYTES
        .checked_add(unsigned_payload.len())
        .ok_or(NetworkError::FrameTooLarge {
            actual_bytes: usize::MAX,
            max_bytes,
        })?;
    if payload_len > max_bytes {
        return Err(NetworkError::FrameTooLarge {
            actual_bytes: payload_len,
            max_bytes,
        });
    }
    let authentication_tag = authenticated_control_tag(channel_binding, domain, &unsigned_payload);
    let mut payload = Vec::with_capacity(payload_len);
    payload.extend_from_slice(authentication_tag.as_bytes());
    payload.extend_from_slice(&unsigned_payload);
    Ok(payload)
}

fn decode_authenticated_control<T: serde::de::DeserializeOwned>(
    channel_binding: &[u8; 32],
    domain: &[u8],
    payload: &[u8],
) -> Result<T, NetworkError> {
    if payload.len() < AUTHENTICATED_CONTROL_TAG_BYTES {
        return Err(serialization_error(
            "authenticated object RPC control is shorter than its tag",
        ));
    }
    let (actual_tag, unsigned_payload) = payload.split_at(AUTHENTICATED_CONTROL_TAG_BYTES);
    let expected_tag = authenticated_control_tag(channel_binding, domain, unsigned_payload);
    // `blake3::Hash` compares byte slices in constant time.
    if !expected_tag.eq(actual_tag) {
        return Err(NetworkError::AuthDenied);
    }
    decode_control(unsigned_payload)
}

async fn write_wire_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    kind: u8,
    payload: &[u8],
    operation: &str,
) -> Result<(), SendFailure> {
    let length = u32::try_from(payload.len()).map_err(|_| SendFailure {
        error: NetworkError::FrameTooLarge {
            actual_bytes: payload.len(),
            max_bytes: u32::MAX as usize,
        },
        wrote_any: false,
    })?;
    let mut prefix = [0_u8; OBJECT_RPC_STREAM_WIRE_PREFIX_BYTES];
    prefix[..4].copy_from_slice(&OBJECT_RPC_STREAM_WIRE_MAGIC);
    prefix[4] = kind;
    prefix[5..].copy_from_slice(&length.to_be_bytes());

    // Keep the producer-owned body separate from its small wire prefix. The
    // logical-stream writer accepts this as one vectored record and performs
    // the single normalization copy required by the byte-stream adapter.
    let total = prefix.len().saturating_add(payload.len());
    let mut written = 0_usize;
    while written < total {
        let result = if written < prefix.len() {
            let slices = [IoSlice::new(&prefix[written..]), IoSlice::new(payload)];
            writer.write_vectored(&slices).await
        } else {
            writer.write(&payload[written - prefix.len()..]).await
        };
        match result {
            Ok(0) => {
                return Err(SendFailure {
                    error: delivery_error(
                        operation,
                        format!("peer stopped accepting bytes after {written} bytes"),
                    ),
                    wrote_any: written != 0,
                });
            }
            Ok(bytes_written) => written = written.saturating_add(bytes_written),
            Err(error) => {
                return Err(SendFailure {
                    error: delivery_error(operation, error.to_string()),
                    wrote_any: written != 0,
                });
            }
        }
    }
    Ok(())
}

async fn read_exact_idle<R: AsyncRead + Unpin>(
    reader: &mut R,
    bytes: &mut [u8],
    options: ObjectFrameIoOptions,
    operation: &str,
) -> Result<(), NetworkError> {
    if let Some(timeout) = options.idle_timeout {
        tokio::time::timeout(timeout, reader.read_exact(bytes))
            .await
            .map_err(|_| delivery_error(operation, format!("idle timeout after {timeout:?}")))?
            .map_err(|error| delivery_error(operation, error.to_string()))?;
    } else {
        reader
            .read_exact(bytes)
            .await
            .map_err(|error| delivery_error(operation, error.to_string()))?;
    }
    Ok(())
}

fn max_payload_for_kind(kind: u8) -> Result<usize, NetworkError> {
    match kind {
        FRAME_REQUEST_HEAD | FRAME_RESPONSE_HEAD => Ok(OBJECT_RPC_STREAM_MAX_HEAD_BYTES),
        FRAME_REQUEST_DATA | FRAME_RESPONSE_DATA => Ok(OBJECT_RPC_STREAM_MAX_DATA_FRAME_BYTES),
        FRAME_REQUEST_FINISH
        | FRAME_RESPONSE_FINISH
        | FRAME_REQUEST_PROGRESS
        | FRAME_RESPONSE_PROGRESS => Ok(OBJECT_RPC_STREAM_MAX_TERMINAL_BYTES),
        _ => Err(serialization_error(format!(
            "unknown object RPC stream frame kind {kind}"
        ))),
    }
}

async fn read_wire_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    options: ObjectFrameIoOptions,
) -> Result<(u8, Vec<u8>), NetworkError> {
    let mut prefix = [0_u8; OBJECT_RPC_STREAM_WIRE_PREFIX_BYTES];
    read_exact_idle(
        reader,
        &mut prefix,
        options,
        "read object RPC stream frame prefix",
    )
    .await?;
    if prefix[..4] != OBJECT_RPC_STREAM_WIRE_MAGIC {
        return Err(serialization_error("invalid object RPC stream wire magic"));
    }
    let kind = prefix[4];
    let max_bytes = max_payload_for_kind(kind)?;
    let length = u32::from_be_bytes(prefix[5..].try_into().expect("four-byte length")) as usize;
    if length > max_bytes {
        return Err(NetworkError::FrameTooLarge {
            actual_bytes: length,
            max_bytes,
        });
    }
    let mut payload = Vec::new();
    payload
        .try_reserve_exact(length)
        .map_err(|_| NetworkError::ResourceExhausted {
            operation: "allocate object RPC stream frame".to_string(),
            requested_bytes: length,
        })?;
    payload.resize(length, 0);
    read_exact_idle(
        reader,
        &mut payload,
        options,
        "read object RPC stream frame payload",
    )
    .await?;
    Ok((kind, payload))
}

type RpcRecordFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, NetworkError>> + Send + 'a>>;
type RpcSendFuture<'a> = Pin<Box<dyn Future<Output = Result<(), SendFailure>> + Send + 'a>>;

struct RpcRecord {
    kind: u8,
    payload: ObjectRpcBodyChunk,
}

impl RpcRecord {
    fn from_bytes(kind: u8, payload: Vec<u8>) -> Self {
        Self {
            kind,
            payload: ObjectRpcBodyChunk::bytes(Bytes::from(payload)),
        }
    }
}

trait RpcRecordReader: Send + 'static {
    fn read_record(&mut self, options: ObjectFrameIoOptions) -> RpcRecordFuture<'_, RpcRecord>;
}

trait RpcRecordWriter: Send + 'static {
    fn chunk_allocator(&self) -> Option<&ChunkAllocator>;

    fn write_record<'a>(
        &'a mut self,
        kind: u8,
        payload: &'a [u8],
        operation: &'a str,
    ) -> RpcSendFuture<'a>;

    fn write_owned_record<'a>(
        &'a mut self,
        kind: u8,
        chunk: OwnedChunk,
        operation: &'a str,
    ) -> RpcSendFuture<'a>;

    fn flush_record(&mut self) -> RpcSendFuture<'_>;

    fn finish_records(&mut self) -> RpcSendFuture<'_>;
}

struct ByteRpcRecordReader {
    reader: Box<dyn AsyncRead + Unpin + Send>,
}

impl RpcRecordReader for ByteRpcRecordReader {
    fn read_record(&mut self, options: ObjectFrameIoOptions) -> RpcRecordFuture<'_, RpcRecord> {
        Box::pin(async move {
            let (kind, payload) = read_wire_frame(&mut self.reader, options).await?;
            Ok(RpcRecord::from_bytes(kind, payload))
        })
    }
}

struct ByteRpcRecordWriter {
    writer: Box<dyn AsyncWrite + Unpin + Send>,
}

impl RpcRecordWriter for ByteRpcRecordWriter {
    fn chunk_allocator(&self) -> Option<&ChunkAllocator> {
        None
    }

    fn write_record<'a>(
        &'a mut self,
        kind: u8,
        payload: &'a [u8],
        operation: &'a str,
    ) -> RpcSendFuture<'a> {
        Box::pin(write_wire_frame(&mut self.writer, kind, payload, operation))
    }

    fn write_owned_record<'a>(
        &'a mut self,
        kind: u8,
        chunk: OwnedChunk,
        operation: &'a str,
    ) -> RpcSendFuture<'a> {
        Box::pin(async move {
            write_wire_frame(&mut self.writer, kind, chunk.as_slice(), operation).await
        })
    }

    fn flush_record(&mut self) -> RpcSendFuture<'_> {
        Box::pin(async move {
            self.writer.flush().await.map_err(|error| SendFailure {
                error: delivery_error("flush byte object RPC records", error.to_string()),
                wrote_any: true,
            })
        })
    }

    fn finish_records(&mut self) -> RpcSendFuture<'_> {
        Box::pin(async move {
            self.writer.shutdown().await.map_err(|error| SendFailure {
                error: delivery_error("finish byte object RPC records", error.to_string()),
                wrote_any: true,
            })
        })
    }
}

struct OwnedRpcRecordReader {
    reader: Box<dyn AuthenticatedChunkReader>,
}

impl RpcRecordReader for OwnedRpcRecordReader {
    fn read_record(&mut self, options: ObjectFrameIoOptions) -> RpcRecordFuture<'_, RpcRecord> {
        Box::pin(async move {
            let receive = self.reader.recv_chunk();
            let lease = if let Some(timeout) = options.idle_timeout {
                tokio::time::timeout(timeout, receive).await.map_err(|_| {
                    delivery_error(
                        "read owned object RPC record",
                        format!("idle timeout after {timeout:?}"),
                    )
                })??
            } else {
                receive.await?
            }
            .ok_or_else(|| {
                delivery_error(
                    "read owned object RPC record",
                    "peer finished before the next RPC record",
                )
            })?;
            let record = lease.as_slice();
            if record.len() < OBJECT_RPC_STREAM_WIRE_PREFIX_BYTES
                || record[..4] != OBJECT_RPC_STREAM_WIRE_MAGIC
            {
                return Err(serialization_error(
                    "invalid owned object RPC stream record prefix",
                ));
            }
            let kind = record[4];
            let max_bytes = max_payload_for_kind(kind)?;
            let length =
                u32::from_be_bytes(record[5..9].try_into().expect("four-byte length")) as usize;
            if length > max_bytes {
                return Err(NetworkError::FrameTooLarge {
                    actual_bytes: length,
                    max_bytes,
                });
            }
            if record.len() != OBJECT_RPC_STREAM_WIRE_PREFIX_BYTES + length {
                return Err(serialization_error(format!(
                    "owned object RPC record declared {length} payload bytes but carried {}",
                    record
                        .len()
                        .saturating_sub(OBJECT_RPC_STREAM_WIRE_PREFIX_BYTES)
                )));
            }
            let payload = lease.slice(OBJECT_RPC_STREAM_WIRE_PREFIX_BYTES, record.len())?;
            observe_chunk_identity(payload.slab_id(), "rpc-record-received");
            Ok(RpcRecord {
                kind,
                payload: ObjectRpcBodyChunk::lease(payload),
            })
        })
    }
}

struct OwnedRpcRecordWriter {
    writer: Box<dyn AuthenticatedChunkWriter>,
    allocator: ChunkAllocator,
}

impl OwnedRpcRecordWriter {
    fn rpc_prefix(kind: u8, payload_bytes: usize) -> Result<[u8; 9], SendFailure> {
        let max_bytes = max_payload_for_kind(kind).map_err(|error| SendFailure {
            error,
            wrote_any: false,
        })?;
        if payload_bytes > max_bytes {
            return Err(SendFailure {
                error: NetworkError::FrameTooLarge {
                    actual_bytes: payload_bytes,
                    max_bytes,
                },
                wrote_any: false,
            });
        }
        let length = u32::try_from(payload_bytes).map_err(|_| SendFailure {
            error: NetworkError::FrameTooLarge {
                actual_bytes: payload_bytes,
                max_bytes: u32::MAX as usize,
            },
            wrote_any: false,
        })?;
        let mut prefix = [0_u8; OBJECT_RPC_STREAM_WIRE_PREFIX_BYTES];
        prefix[..4].copy_from_slice(&OBJECT_RPC_STREAM_WIRE_MAGIC);
        prefix[4] = kind;
        prefix[5..].copy_from_slice(&length.to_be_bytes());
        Ok(prefix)
    }
}

impl RpcRecordWriter for OwnedRpcRecordWriter {
    fn chunk_allocator(&self) -> Option<&ChunkAllocator> {
        Some(&self.allocator)
    }

    fn write_record<'a>(
        &'a mut self,
        kind: u8,
        payload: &'a [u8],
        operation: &'a str,
    ) -> RpcSendFuture<'a> {
        Box::pin(async move {
            let prefix = Self::rpc_prefix(kind, payload.len())?;
            let mut chunk = self
                .allocator
                .allocate_control(payload.len())
                .await
                .map_err(|error| SendFailure {
                    error,
                    wrote_any: false,
                })?;
            chunk.writable_payload()[..payload.len()].copy_from_slice(payload);
            chunk
                .commit_payload(payload.len())
                .and_then(|_| chunk.prepend(&prefix))
                .map_err(|error| SendFailure {
                    error,
                    wrote_any: false,
                })?;
            self.writer
                .send_chunk(chunk)
                .await
                .map_err(|error| SendFailure {
                    error: delivery_error(operation, error.to_string()),
                    wrote_any: true,
                })
        })
    }

    fn write_owned_record<'a>(
        &'a mut self,
        kind: u8,
        mut chunk: OwnedChunk,
        operation: &'a str,
    ) -> RpcSendFuture<'a> {
        Box::pin(async move {
            observe_chunk_identity(chunk.slab_id(), "rpc-body-produced");
            let prefix = Self::rpc_prefix(kind, chunk.len())?;
            chunk.prepend(&prefix).map_err(|error| SendFailure {
                error,
                wrote_any: false,
            })?;
            observe_chunk_identity(chunk.slab_id(), "rpc-record-framed");
            self.writer
                .send_chunk(chunk)
                .await
                .map_err(|error| SendFailure {
                    error: delivery_error(operation, error.to_string()),
                    wrote_any: true,
                })
        })
    }

    fn flush_record(&mut self) -> RpcSendFuture<'_> {
        Box::pin(async { Ok(()) })
    }

    fn finish_records(&mut self) -> RpcSendFuture<'_> {
        Box::pin(async move {
            self.writer.finish().await.map_err(|error| SendFailure {
                error,
                wrote_any: true,
            })
        })
    }
}

fn byte_record_halves<S>(stream: S) -> (ByteRpcRecordReader, ByteRpcRecordWriter)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (reader, writer) = tokio::io::split(stream);
    (
        ByteRpcRecordReader {
            reader: Box::new(reader),
        },
        ByteRpcRecordWriter {
            writer: Box::new(writer),
        },
    )
}

fn owned_record_halves(
    stream: AuthenticatedChunkStream,
) -> (OwnedRpcRecordReader, OwnedRpcRecordWriter) {
    let (reader, writer) = stream.into_halves();
    let allocator = writer.allocator().clone();
    (
        OwnedRpcRecordReader { reader },
        OwnedRpcRecordWriter { writer, allocator },
    )
}

async fn read_response_head_with_progress<R: RpcRecordReader>(
    reader: &mut R,
    options: ObjectFrameIoOptions,
    context: &RpcPeerContext,
    request_id: &Id,
    request_head_digest: [u8; 32],
    timeline_trace: Option<crate::NetworkTimelineTraceContext>,
) -> Result<(RpcRecord, u64), NetworkError> {
    let started_ns = timeline_started_ns(timeline_trace);
    let result = read_response_head_with_progress_inner(
        reader,
        options,
        context,
        request_id,
        request_head_digest,
    )
    .await;
    record_rpc_timeline_duration(
        timeline_trace,
        crate::network_timeline_stage::RPC_RESPONSE_WAIT,
        crate::network_timeline_flag::RESPONSE,
        started_ns,
        result.is_ok(),
    );
    result
}

async fn read_response_head_with_progress_inner<R: RpcRecordReader>(
    reader: &mut R,
    options: ObjectFrameIoOptions,
    context: &RpcPeerContext,
    request_id: &Id,
    request_head_digest: [u8; 32],
) -> Result<(RpcRecord, u64), NetworkError> {
    let mut last_progress_nonce = 0;
    loop {
        let frame = reader.read_record(options).await?;
        if frame.kind != FRAME_RESPONSE_PROGRESS {
            return Ok((frame, last_progress_nonce));
        }
        validate_stream_progress(
            context,
            IncomingDirection::Response,
            request_id,
            request_head_digest,
            &mut last_progress_nonce,
            &frame.payload,
        )?;
    }
}

async fn hash_source(source: ObjectRpcBodySource) -> Result<[u8; 32], NetworkError> {
    let mut producer = source.producer(OBJECT_RPC_STREAM_MAX_DATA_FRAME_BYTES)?;
    let mut hasher = blake3::Hasher::new();
    while let Some(chunk) = producer.next().await? {
        hasher.update(&chunk);
    }
    producer.finish().await?;
    Ok(*hasher.finalize().as_bytes())
}

async fn prepare_outgoing_body(
    body: ObjectRpcStreamBody,
) -> Result<PreparedOutgoingBody, NetworkError> {
    match body.kind {
        OutgoingBodyKind::Inline(bytes) => {
            let length = u64::try_from(bytes.len())
                .map_err(|_| serialization_error("inline body length does not fit u64"))?;
            let digest = *blake3::hash(&bytes).as_bytes();
            Ok(PreparedOutgoingBody {
                descriptor: BodyDescriptor::Inline { length, digest },
                inline_body: Some(InlineBody {
                    bytes: bytes.to_vec(),
                }),
                source: None,
            })
        }
        OutgoingBodyKind::Source(source) => {
            let total_bytes = source.total_bytes();
            let digest = if let Some(digest) = body.supplied_digest {
                Some(digest)
            } else if let Some(digest) = source.known_digest() {
                Some(digest)
            } else if let Some(replay) = source.try_clone_replayable() {
                Some(hash_source(replay).await?)
            } else {
                None
            };
            let descriptor = match (total_bytes, digest) {
                (Some(length), Some(digest)) => BodyDescriptor::Known { length, digest },
                (Some(length), None) => BodyDescriptor::KnownLength { length },
                _ => BodyDescriptor::Deferred,
            };
            Ok(PreparedOutgoingBody {
                descriptor,
                inline_body: None,
                source: Some(source),
            })
        }
    }
}

fn validate_descriptor_inline(
    descriptor: &BodyDescriptor,
    inline_body: &Option<InlineBody>,
) -> Result<(), NetworkError> {
    match (descriptor, inline_body) {
        (BodyDescriptor::Inline { length, digest, .. }, Some(InlineBody { bytes })) => {
            let actual_length = u64::try_from(bytes.len())
                .map_err(|_| serialization_error("inline body length does not fit u64"))?;
            if actual_length != *length || blake3::hash(bytes).as_bytes() != digest {
                return Err(NetworkError::AuthDenied);
            }
            if bytes.len() > OBJECT_RPC_STREAM_INLINE_BODY_BYTES {
                return Err(NetworkError::FrameTooLarge {
                    actual_bytes: bytes.len(),
                    max_bytes: OBJECT_RPC_STREAM_INLINE_BODY_BYTES,
                });
            }
            Ok(())
        }
        (BodyDescriptor::Inline { .. }, None) | (_, Some(_)) => Err(serialization_error(
            "object RPC inline body does not match its descriptor",
        )),
        _ => Ok(()),
    }
}

fn descriptor_total_bytes(descriptor: &BodyDescriptor) -> Option<u64> {
    match descriptor {
        BodyDescriptor::Inline { length, .. }
        | BodyDescriptor::Known { length, .. }
        | BodyDescriptor::KnownLength { length } => Some(*length),
        BodyDescriptor::Deferred => None,
    }
}

#[derive(Clone)]
struct IncomingBodyCompletion {
    receiver: watch::Receiver<Option<Result<IncomingTerminal, NetworkError>>>,
}

impl IncomingBodyCompletion {
    async fn join(mut self) -> Result<IncomingTerminal, NetworkError> {
        loop {
            if let Some(result) = self.receiver.borrow().clone() {
                return result;
            }
            self.receiver.changed().await.map_err(|_| {
                delivery_error(
                    "complete object RPC stream body",
                    "body receiver ended without a terminal result",
                )
            })?;
        }
    }

    #[cfg(test)]
    fn is_ready(&self) -> bool {
        self.receiver.borrow().is_some()
    }
}

fn incoming_body_verification(
    result: &Result<IncomingTerminal, NetworkError>,
) -> Result<ObjectRpcBodyVerification, NetworkError> {
    match result {
        Ok(IncomingTerminal::Completed(verification)) => Ok(verification.clone()),
        Ok(IncomingTerminal::Rejected(detail)) => Err(serialization_error(format!(
            "object RPC body rejected: {detail}"
        ))),
        Err(error) => Err(error.clone()),
    }
}

struct PullIncomingBodyState<R> {
    reader: R,
    options: ObjectFrameIoOptions,
    context: RpcPeerContext,
    direction: IncomingDirection,
    request_id: Id,
    head_digest: [u8; 32],
    progress_head_digest: [u8; 32],
    last_progress_nonce: u64,
    descriptor: BodyDescriptor,
    hasher: blake3::Hasher,
    observed_length: u64,
    pending_chunk: Option<ObjectRpcBodyChunk>,
    terminal_sender: watch::Sender<Option<Result<IncomingTerminal, NetworkError>>>,
    terminal_published: bool,
}

impl<R: RpcRecordReader> PullIncomingBodyState<R> {
    fn publish(&mut self, result: Result<IncomingTerminal, NetworkError>) {
        if self.terminal_published {
            return;
        }
        self.terminal_published = true;
        self.terminal_sender.send_replace(Some(result));
    }

    async fn recv_chunk(&mut self) -> Result<Option<ObjectRpcBodyChunk>, NetworkError> {
        if self.terminal_published {
            return Ok(None);
        }
        let result = self.recv_chunk_inner().await;
        if let Err(error) = &result
            && !self.terminal_published
        {
            self.publish(Err(error.clone()));
        }
        result
    }

    async fn recv_chunk_inner(&mut self) -> Result<Option<ObjectRpcBodyChunk>, NetworkError> {
        if let Some(chunk) = self.pending_chunk.take() {
            return Ok(Some(chunk));
        }
        loop {
            let RpcRecord { kind, payload } = self.reader.read_record(self.options).await?;
            if kind == self.direction.progress_kind() {
                validate_stream_progress(
                    &self.context,
                    self.direction,
                    &self.request_id,
                    self.progress_head_digest,
                    &mut self.last_progress_nonce,
                    &payload,
                )?;
                continue;
            }
            if kind == self.direction.data_kind() {
                if matches!(self.descriptor, BodyDescriptor::Inline { .. }) {
                    return Err(serialization_error(
                        "inline object RPC body was followed by a data frame",
                    ));
                }
                self.observed_length = self
                    .observed_length
                    .checked_add(u64::try_from(payload.len()).unwrap_or(u64::MAX))
                    .ok_or_else(|| serialization_error("object RPC body length overflow"))?;
                if let Some(declared) = descriptor_total_bytes(&self.descriptor)
                    && self.observed_length > declared
                {
                    return Err(serialization_error(format!(
                        "object RPC body exceeded declared length {declared}"
                    )));
                }
                self.hasher.update(&payload);
                if let BodyDescriptor::Known { length, digest } = self.descriptor
                    && self.observed_length == length
                {
                    let observed_digest = *self.hasher.finalize().as_bytes();
                    if observed_digest != digest {
                        return Err(NetworkError::AuthDenied);
                    }
                    self.publish(Ok(IncomingTerminal::Completed(ObjectRpcBodyVerification {
                        length: self.observed_length,
                        digest: observed_digest,
                    })));
                }
                if payload.is_empty() {
                    continue;
                }
                return Ok(Some(payload));
            }
            if kind != self.direction.finish_kind() {
                return Err(serialization_error(format!(
                    "unexpected object RPC stream frame kind {kind} in {:?} body",
                    self.direction
                )));
            }
            let finish: BodyFinishUnsigned = decode_authenticated_control(
                &self.context.channel_binding,
                self.direction.finish_domain(),
                &payload,
            )?;
            validate_incoming_finish(
                &self.context,
                &self.request_id,
                self.head_digest,
                self.observed_length,
                &self.hasher.finalize(),
                &self.descriptor,
                &finish,
            )?;
            let verification = ObjectRpcBodyVerification {
                length: self.observed_length,
                digest: finish.content_digest,
            };
            match finish.status {
                FinishStatus::Completed => {
                    self.publish(Ok(IncomingTerminal::Completed(verification)));
                    return Ok(None);
                }
                FinishStatus::Rejected { detail } => {
                    self.publish(Ok(IncomingTerminal::Rejected(detail.clone())));
                    return Err(serialization_error(format!(
                        "object RPC body rejected: {detail}"
                    )));
                }
            }
        }
    }

    async fn drain(mut self) {
        while matches!(self.recv_chunk().await, Ok(Some(_))) {}
    }
}

struct PullIncomingBodyReceiver<R: RpcRecordReader> {
    state: Option<PullIncomingBodyState<R>>,
}

impl<R: RpcRecordReader> std::fmt::Debug for PullIncomingBodyReceiver<R> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PullIncomingBodyReceiver")
            .field(
                "terminal",
                &self
                    .state
                    .as_ref()
                    .is_none_or(|state| state.terminal_published),
            )
            .finish()
    }
}

impl<R: RpcRecordReader> ObjectRpcBodyReceiver for PullIncomingBodyReceiver<R> {
    fn recv_chunk(&mut self) -> ObjectRpcBodyReceiverFuture<'_> {
        Box::pin(async move {
            let Some(state) = self.state.as_mut() else {
                return Ok(None);
            };
            state
                .recv_chunk()
                .await
                .map_err(|error| std::io::Error::other(error.to_string()))
        })
    }
}

impl<R: RpcRecordReader> Drop for PullIncomingBodyReceiver<R> {
    fn drop(&mut self) {
        let Some(mut state) = self.state.take() else {
            return;
        };
        if state.terminal_published {
            return;
        }
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn(state.drain());
            }
            Err(error) => state.publish(Err(delivery_error(
                "drain abandoned object RPC body",
                format!("Tokio runtime unavailable: {error}"),
            ))),
        }
    }
}

async fn make_incoming_body<R>(
    reader: R,
    options: ObjectFrameIoOptions,
    context: RpcPeerContext,
    spec: IncomingBodySpec,
    inline_completes_without_finish: bool,
) -> (
    ObjectRpcStreamIncomingBody,
    ObjectRpcStreamBodyVerifier,
    IncomingBodyCompletion,
)
where
    R: RpcRecordReader,
{
    let total_bytes = descriptor_total_bytes(&spec.descriptor);
    let (terminal_sender, terminal_receiver) = watch::channel(None);
    let verifier = ObjectRpcStreamBodyVerifier {
        receiver: terminal_receiver.clone(),
    };
    let completion = IncomingBodyCompletion {
        receiver: terminal_receiver,
    };
    let descriptor_validation = validate_descriptor_inline(&spec.descriptor, &spec.inline_body);
    let incoming_body = match (descriptor_validation, spec.descriptor, spec.inline_body) {
        (Ok(()), BodyDescriptor::Inline { length, digest }, Some(InlineBody { bytes }))
            if inline_completes_without_finish =>
        {
            terminal_sender.send_replace(Some(Ok(IncomingTerminal::Completed(
                ObjectRpcBodyVerification { length, digest },
            ))));
            ObjectRpcIncomingBody::bytes(bytes)
        }
        (Ok(()), descriptor @ BodyDescriptor::Inline { .. }, Some(InlineBody { bytes })) => {
            let observed_length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            let mut hasher = blake3::Hasher::new();
            hasher.update(&bytes);
            let pending_chunk =
                (!bytes.is_empty()).then(|| ObjectRpcBodyChunk::bytes(Bytes::from(bytes)));
            ObjectRpcIncomingBody::receiver(
                total_bytes,
                Box::new(PullIncomingBodyReceiver {
                    state: Some(PullIncomingBodyState {
                        reader,
                        options,
                        context,
                        direction: spec.direction,
                        request_id: spec.request_id,
                        head_digest: spec.head_digest,
                        progress_head_digest: spec.progress_head_digest,
                        last_progress_nonce: spec.last_progress_nonce,
                        descriptor,
                        hasher,
                        observed_length,
                        pending_chunk,
                        terminal_sender,
                        terminal_published: false,
                    }),
                }),
            )
        }
        (Ok(()), BodyDescriptor::Known { length: 0, digest }, None) => {
            let observed_digest = *blake3::hash(&[]).as_bytes();
            let result = if observed_digest == digest {
                Ok(IncomingTerminal::Completed(ObjectRpcBodyVerification {
                    length: 0,
                    digest: observed_digest,
                }))
            } else {
                Err(NetworkError::AuthDenied)
            };
            terminal_sender.send_replace(Some(result));
            ObjectRpcIncomingBody::empty()
        }
        (Ok(()), descriptor, None) => ObjectRpcIncomingBody::receiver(
            total_bytes,
            Box::new(PullIncomingBodyReceiver {
                state: Some(PullIncomingBodyState {
                    reader,
                    options,
                    context,
                    direction: spec.direction,
                    request_id: spec.request_id,
                    head_digest: spec.head_digest,
                    progress_head_digest: spec.progress_head_digest,
                    last_progress_nonce: spec.last_progress_nonce,
                    descriptor,
                    hasher: blake3::Hasher::new(),
                    observed_length: 0,
                    pending_chunk: None,
                    terminal_sender,
                    terminal_published: false,
                }),
            }),
        ),
        (Ok(()), _, Some(_)) => unreachable!("inline descriptor validation matched the body"),
        (Err(error), _, _) => {
            terminal_sender.send_replace(Some(Err(error)));
            ObjectRpcIncomingBody::empty()
        }
    };
    let body = ObjectRpcStreamIncomingBody {
        body: incoming_body,
        verifier: verifier.clone(),
    };
    (body, verifier, completion)
}

fn validate_incoming_finish(
    context: &RpcPeerContext,
    request_id: &Id,
    head_digest: [u8; 32],
    observed_length: u64,
    observed_digest: &blake3::Hash,
    descriptor: &BodyDescriptor,
    finish: &BodyFinishUnsigned,
) -> Result<(), NetworkError> {
    let (expected_origin, expected_destination) = context.expected_incoming_endpoints();
    if &finish.request_id != request_id
        || finish.session_id != context.session_id
        || finish.channel_binding != context.channel_binding
        || finish.origin_node_id.as_ref() != expected_origin
        || finish.destination_node_id.as_ref() != expected_destination
        || finish.head_digest != head_digest
        || finish.length != observed_length
        || finish.content_digest.as_slice() != observed_digest.as_bytes()
    {
        return Err(NetworkError::AuthDenied);
    }
    match descriptor {
        BodyDescriptor::Inline { length, digest, .. }
        | BodyDescriptor::Known { length, digest } => {
            if *length != observed_length || digest.as_slice() != observed_digest.as_bytes() {
                return Err(NetworkError::AuthDenied);
            }
        }
        BodyDescriptor::KnownLength { length } => {
            if *length != observed_length {
                return Err(NetworkError::AuthDenied);
            }
        }
        BodyDescriptor::Deferred => {}
    }
    Ok(())
}

enum OutgoingRpcData {
    Bytes(Bytes),
    Owned(OwnedChunk),
}

impl OutgoingRpcData {
    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Bytes(bytes) => bytes,
            Self::Owned(chunk) => chunk.as_slice(),
        }
    }
}

async fn next_outgoing_body_chunk(
    producer: &mut crate::rpc_body::BodyProducer,
    allocator: Option<&ChunkAllocator>,
) -> Result<Option<OutgoingRpcData>, NetworkError> {
    match allocator {
        Some(allocator) => producer
            .next_owned(allocator)
            .await
            .map(|chunk| chunk.map(OutgoingRpcData::Owned)),
        None => producer
            .next()
            .await
            .map(|chunk| chunk.map(OutgoingRpcData::Bytes)),
    }
}

async fn write_prepared_body_data<W: RpcRecordWriter>(
    writer: &mut W,
    context: &RpcPeerContext,
    direction: IncomingDirection,
    request_id: &Id,
    progress_head_digest: [u8; 32],
    progress_interval: Option<Duration>,
    progress_nonce: &mut u64,
    data_kind: u8,
    body: &mut PreparedOutgoingBody,
) -> Result<ObjectRpcBodyVerification, SendFailure> {
    let mut hasher = blake3::Hasher::new();
    let mut length = 0_u64;
    if let Some(inline) = body.inline_body.as_ref() {
        hasher.update(&inline.bytes);
        length = inline.bytes.len() as u64;
    }
    if let Some(source) = body.source.as_ref() {
        let mut producer = source
            .producer(OBJECT_RPC_STREAM_MAX_DATA_FRAME_BYTES)
            .map_err(|error| SendFailure {
                error,
                wrote_any: false,
            })?;
        loop {
            let allocator = writer.chunk_allocator().cloned();
            let mut next_chunk =
                std::pin::pin!(next_outgoing_body_chunk(&mut producer, allocator.as_ref()));
            let next = await_with_stream_progress(
                writer,
                context,
                direction,
                request_id,
                progress_head_digest,
                progress_interval,
                progress_nonce,
                next_chunk.as_mut(),
            )
            .await?
            .map_err(|error| SendFailure {
                error,
                wrote_any: length != 0,
            })?;
            let Some(chunk) = next else {
                break;
            };
            if chunk.as_slice().len() > OBJECT_RPC_STREAM_MAX_DATA_FRAME_BYTES {
                return Err(SendFailure {
                    error: NetworkError::FrameTooLarge {
                        actual_bytes: chunk.as_slice().len(),
                        max_bytes: OBJECT_RPC_STREAM_MAX_DATA_FRAME_BYTES,
                    },
                    wrote_any: length != 0,
                });
            }
            length = length
                .checked_add(chunk.as_slice().len() as u64)
                .ok_or_else(|| SendFailure {
                    error: serialization_error("object RPC body length overflow"),
                    wrote_any: true,
                })?;
            hasher.update(chunk.as_slice());
            match chunk {
                OutgoingRpcData::Bytes(bytes) => {
                    writer
                        .write_record(
                            data_kind,
                            &bytes,
                            "write byte-compatible object RPC body data",
                        )
                        .await?;
                }
                OutgoingRpcData::Owned(chunk) => {
                    writer
                        .write_owned_record(data_kind, chunk, "write owned object RPC body data")
                        .await?;
                }
            }
        }
        let mut finish_producer = std::pin::pin!(producer.finish());
        await_with_stream_progress(
            writer,
            context,
            direction,
            request_id,
            progress_head_digest,
            progress_interval,
            progress_nonce,
            finish_producer.as_mut(),
        )
        .await?
        .map_err(|error| SendFailure {
            error,
            wrote_any: length != 0,
        })?;
    }
    let digest = *hasher.finalize().as_bytes();
    match body.descriptor {
        BodyDescriptor::Inline {
            length: expected_length,
            digest: expected_digest,
        }
        | BodyDescriptor::Known {
            length: expected_length,
            digest: expected_digest,
        } => {
            if length != expected_length || digest != expected_digest {
                return Err(SendFailure {
                    error: serialization_error(
                        "object RPC body changed after its authenticated descriptor was prepared",
                    ),
                    wrote_any: length != 0,
                });
            }
        }
        BodyDescriptor::KnownLength {
            length: expected_length,
        } => {
            if length != expected_length {
                return Err(SendFailure {
                    error: serialization_error(
                        "object RPC body length changed after its authenticated descriptor was prepared",
                    ),
                    wrote_any: length != 0,
                });
            }
        }
        BodyDescriptor::Deferred => {}
    }
    Ok(ObjectRpcBodyVerification { length, digest })
}

fn make_finish(
    context: &RpcPeerContext,
    request_id: Id,
    head_digest: [u8; 32],
    verification: ObjectRpcBodyVerification,
    status: FinishStatus,
) -> BodyFinishUnsigned {
    let (origin_node_id, destination_node_id) = context.wire_endpoints();
    BodyFinishUnsigned {
        request_id,
        session_id: context.session_id.clone(),
        channel_binding: context.channel_binding,
        origin_node_id,
        destination_node_id,
        head_digest,
        length: verification.length,
        content_digest: verification.digest,
        status,
    }
}

async fn write_finish<W: RpcRecordWriter>(
    writer: &mut W,
    context: &RpcPeerContext,
    kind: u8,
    domain: &[u8],
    finish: &BodyFinishUnsigned,
) -> Result<(), SendFailure> {
    let payload = encode_authenticated_control(
        &context.channel_binding,
        domain,
        finish,
        OBJECT_RPC_STREAM_MAX_TERMINAL_BYTES,
    )
    .map_err(|error| SendFailure {
        error,
        wrote_any: false,
    })?;
    writer
        .write_record(kind, &payload, "write object RPC body finish")
        .await
}

fn request_head(
    context: &RpcPeerContext,
    request_id: Id,
    method: String,
    operation_id: Option<String>,
    metadata: BTreeMap<String, String>,
    application_head: Vec<u8>,
    options: ObjectFrameIoOptions,
    body: &PreparedOutgoingBody,
) -> Result<(RequestHeadUnsigned, Vec<u8>), NetworkError> {
    let (origin_node_id, destination_node_id) = context.wire_endpoints();
    let unsigned = RequestHeadUnsigned {
        protocol: OBJECT_RPC_STREAM_PROTOCOL.to_string(),
        origin_node_id,
        destination_node_id,
        session_id: context.session_id.clone(),
        channel_binding: context.channel_binding,
        request_id,
        method,
        operation_id,
        metadata,
        application_head,
        issued_at_unix_millis: now_unix_millis()?,
        response_progress_interval_millis: advertised_progress_interval_millis(options),
        descriptor: body.descriptor,
        inline_body: body.inline_body.clone(),
    };
    let payload = encode_authenticated_control(
        &context.channel_binding,
        AUTH_REQUEST_HEAD,
        &unsigned,
        OBJECT_RPC_STREAM_MAX_HEAD_BYTES,
    )?;
    Ok((unsigned, payload))
}

fn validate_request_head(
    context: &RpcPeerContext,
    unsigned: &RequestHeadUnsigned,
) -> Result<(), NetworkError> {
    let (expected_origin, expected_destination) = context.expected_incoming_endpoints();
    if unsigned.protocol != OBJECT_RPC_STREAM_PROTOCOL
        || unsigned.origin_node_id.as_ref() != expected_origin
        || unsigned.destination_node_id.as_ref() != expected_destination
        || unsigned.session_id != context.session_id
        || unsigned.channel_binding != context.channel_binding
        || unsigned.method.trim().is_empty()
    {
        return Err(NetworkError::AuthDenied);
    }
    validate_descriptor_inline(&unsigned.descriptor, &unsigned.inline_body)?;
    let now = now_unix_millis()?;
    if unsigned.issued_at_unix_millis
        > now.saturating_add(OBJECT_RPC_STREAM_MAX_HEAD_CLOCK_SKEW_MILLIS)
        || now
            > unsigned
                .issued_at_unix_millis
                .saturating_add(OBJECT_RPC_STREAM_MAX_HEAD_CLOCK_SKEW_MILLIS)
    {
        return Err(NetworkError::AuthExpired);
    }
    Ok(())
}

fn response_head(
    context: &RpcPeerContext,
    request_id: Id,
    request_head_digest: [u8; 32],
    disposition: ResponseDisposition,
) -> Result<(ResponseHeadUnsigned, Vec<u8>), NetworkError> {
    let (origin_node_id, destination_node_id) = context.wire_endpoints();
    let unsigned = ResponseHeadUnsigned {
        protocol: OBJECT_RPC_STREAM_PROTOCOL.to_string(),
        origin_node_id,
        destination_node_id,
        session_id: context.session_id.clone(),
        channel_binding: context.channel_binding,
        request_id,
        request_head_digest,
        disposition,
    };
    let payload = encode_authenticated_control(
        &context.channel_binding,
        AUTH_RESPONSE_HEAD,
        &unsigned,
        OBJECT_RPC_STREAM_MAX_HEAD_BYTES,
    )?;
    Ok((unsigned, payload))
}

fn validate_response_head(
    context: &RpcPeerContext,
    request_id: &Id,
    request_head_digest: [u8; 32],
    unsigned: &ResponseHeadUnsigned,
) -> Result<(), NetworkError> {
    let (expected_origin, expected_destination) = context.expected_incoming_endpoints();
    if unsigned.protocol != OBJECT_RPC_STREAM_PROTOCOL
        || unsigned.origin_node_id.as_ref() != expected_origin
        || unsigned.destination_node_id.as_ref() != expected_destination
        || unsigned.session_id != context.session_id
        || unsigned.channel_binding != context.channel_binding
        || &unsigned.request_id != request_id
        || unsigned.request_head_digest != request_head_digest
    {
        return Err(NetworkError::AuthDenied);
    }
    if let ResponseDisposition::Accepted {
        descriptor,
        inline_body,
        ..
    } = &unsigned.disposition
    {
        validate_descriptor_inline(descriptor, inline_body)?;
    }
    Ok(())
}

fn stream_progress(
    context: &RpcPeerContext,
    direction: IncomingDirection,
    request_id: Id,
    head_digest: [u8; 32],
    nonce: u64,
) -> Result<Vec<u8>, NetworkError> {
    let (origin_node_id, destination_node_id) = context.wire_endpoints();
    let unsigned = StreamProgressUnsigned {
        protocol: OBJECT_RPC_STREAM_PROTOCOL.to_string(),
        origin_node_id,
        destination_node_id,
        session_id: context.session_id.clone(),
        channel_binding: context.channel_binding,
        request_id,
        head_digest,
        nonce,
    };
    encode_authenticated_control(
        &context.channel_binding,
        direction.progress_domain(),
        &unsigned,
        OBJECT_RPC_STREAM_MAX_TERMINAL_BYTES,
    )
}

fn validate_stream_progress(
    context: &RpcPeerContext,
    direction: IncomingDirection,
    request_id: &Id,
    head_digest: [u8; 32],
    last_nonce: &mut u64,
    payload: &[u8],
) -> Result<(), NetworkError> {
    let unsigned: StreamProgressUnsigned = decode_authenticated_control(
        &context.channel_binding,
        direction.progress_domain(),
        payload,
    )?;
    let (expected_origin, expected_destination) = context.expected_incoming_endpoints();
    if unsigned.protocol != OBJECT_RPC_STREAM_PROTOCOL
        || unsigned.origin_node_id.as_ref() != expected_origin
        || unsigned.destination_node_id.as_ref() != expected_destination
        || unsigned.session_id != context.session_id
        || unsigned.channel_binding != context.channel_binding
        || &unsigned.request_id != request_id
        || unsigned.head_digest != head_digest
        || unsigned.nonce == 0
        || unsigned.nonce <= *last_nonce
    {
        return Err(NetworkError::AuthDenied);
    }
    *last_nonce = unsigned.nonce;
    Ok(())
}

fn stream_progress_interval(options: ObjectFrameIoOptions) -> Option<Duration> {
    options.idle_timeout.map(|timeout| {
        timeout
            .checked_div(2)
            .unwrap_or(timeout)
            .max(OBJECT_RPC_STREAM_MIN_PROGRESS_INTERVAL)
    })
}

fn advertised_progress_interval_millis(options: ObjectFrameIoOptions) -> Option<u64> {
    stream_progress_interval(options).map(|interval| {
        let rounded_up = interval.as_micros().saturating_add(999) / 1_000;
        u64::try_from(rounded_up.max(1)).unwrap_or(u64::MAX)
    })
}

fn response_progress_interval(
    server_options: ObjectFrameIoOptions,
    requested_millis: Option<u64>,
) -> Option<Duration> {
    let requested = requested_millis
        .filter(|millis| *millis != 0)
        .map(Duration::from_millis)
        .map(|interval| interval.max(OBJECT_RPC_STREAM_MIN_PROGRESS_INTERVAL));
    match (stream_progress_interval(server_options), requested) {
        (Some(server), Some(requested)) => Some(server.min(requested)),
        (Some(server), None) => Some(server),
        (None, requested) => requested,
    }
}

async fn write_stream_progress<W: RpcRecordWriter>(
    writer: &mut W,
    context: &RpcPeerContext,
    direction: IncomingDirection,
    request_id: &Id,
    head_digest: [u8; 32],
    nonce: u64,
) -> Result<(), SendFailure> {
    let payload = stream_progress(context, direction, request_id.clone(), head_digest, nonce)
        .map_err(|error| SendFailure {
            error,
            wrote_any: false,
        })?;
    writer
        .write_record(
            direction.progress_kind(),
            &payload,
            "write object RPC stream progress",
        )
        .await?;
    writer.flush_record().await
}

async fn await_with_stream_progress<W, F, T>(
    writer: &mut W,
    context: &RpcPeerContext,
    direction: IncomingDirection,
    request_id: &Id,
    head_digest: [u8; 32],
    progress_interval: Option<Duration>,
    next_nonce: &mut u64,
    mut future: Pin<&mut F>,
) -> Result<T, SendFailure>
where
    W: RpcRecordWriter,
    F: Future<Output = T> + ?Sized,
{
    let Some(interval) = progress_interval else {
        return Ok(future.await);
    };
    loop {
        tokio::select! {
            biased;
            result = future.as_mut() => return Ok(result),
            () = tokio::time::sleep(interval) => {
                write_stream_progress(
                    writer,
                    context,
                    direction,
                    request_id,
                    head_digest,
                    *next_nonce,
                )
                .await?;
                *next_nonce = next_nonce.wrapping_add(1).max(1);
            }
        }
    }
}

async fn send_request<W: RpcRecordWriter>(
    mut writer: W,
    context: RpcPeerContext,
    request_id: Id,
    request_head_digest: [u8; 32],
    head_payload: Vec<u8>,
    mut body: PreparedOutgoingBody,
    options: ObjectFrameIoOptions,
    timeline_trace: Option<crate::NetworkTimelineTraceContext>,
) -> Result<(), SendFailure> {
    let started_ns = timeline_started_ns(timeline_trace);
    let result = async {
        let result = write_request_frames(
            &mut writer,
            &context,
            request_id,
            request_head_digest,
            &head_payload,
            &mut body,
            options,
        )
        .await;
        if result.is_ok() {
            writer.flush_record().await?;
            writer.finish_records().await?;
        } else {
            let _ = writer.finish_records().await;
        }
        result
    }
    .await;
    record_rpc_timeline_duration(
        timeline_trace,
        crate::network_timeline_stage::RPC_REQUEST_STAGE,
        crate::network_timeline_flag::REQUEST,
        started_ns,
        result.is_ok(),
    );
    result
}

async fn write_request_frames<W: RpcRecordWriter>(
    writer: &mut W,
    context: &RpcPeerContext,
    request_id: Id,
    request_head_digest: [u8; 32],
    head_payload: &[u8],
    body: &mut PreparedOutgoingBody,
    options: ObjectFrameIoOptions,
) -> Result<(), SendFailure> {
    writer
        .write_record(
            FRAME_REQUEST_HEAD,
            head_payload,
            "write object RPC request head",
        )
        .await?;
    if matches!(body.descriptor, BodyDescriptor::Inline { .. }) {
        return Ok(());
    }
    let mut progress_nonce = 1;
    let verification = write_prepared_body_data(
        writer,
        context,
        IncomingDirection::Request,
        &request_id,
        request_head_digest,
        stream_progress_interval(options),
        &mut progress_nonce,
        FRAME_REQUEST_DATA,
        body,
    )
    .await
    .map_err(|failure| SendFailure {
        error: failure.error,
        wrote_any: true,
    })?;
    // A Known descriptor is already authenticated in the request head and
    // commits to both the exact length and digest. The final data record
    // therefore closes the body at its declared byte boundary; the logical
    // mux can combine that record with stream FINISH. Length-only and deferred
    // producers still need the authenticated finish to publish their digest.
    if matches!(body.descriptor, BodyDescriptor::Known { .. }) {
        return Ok(());
    }
    let finish = make_finish(
        context,
        request_id,
        request_head_digest,
        verification,
        FinishStatus::Completed,
    );
    write_finish(
        writer,
        context,
        FRAME_REQUEST_FINISH,
        AUTH_REQUEST_FINISH,
        &finish,
    )
    .await
    .map_err(|failure| SendFailure {
        error: failure.error,
        wrote_any: true,
    })?;
    Ok(())
}

/// Executes one call over one authenticated duplex stream.
///
/// A complete consumer keeps the delivery outcome pending until the
/// authenticated response finish verifies. A streaming consumer transfers the
/// incremental body to its caller immediately; the body retains the verifier,
/// and any terminal authentication or transport failure is delivered by its
/// read path. This function never reconnects, reroutes, or automatically
/// resends a possibly applied call.
async fn call_object_rpc_records_with_consumption<R, W, F, Fut, T>(
    mut reader: R,
    writer: W,
    context: &RpcPeerContext,
    call: ObjectRpcStreamCall,
    options: ObjectFrameIoOptions,
    response_consumer: F,
) -> RpcDeliveryOutcome<T>
where
    R: RpcRecordReader,
    W: RpcRecordWriter,
    F: FnOnce(ObjectRpcStreamResponse) -> Fut,
    Fut: Future<Output = Result<RpcResponseConsumption<T>, NetworkError>>,
{
    if let Err(error) = call.validate() {
        return RpcDeliveryOutcome::NotSent { error };
    }
    let ObjectRpcStreamCall {
        request_id,
        method,
        operation_id,
        metadata,
        application_head,
        body,
        timeline_trace,
    } = call;
    let prepare_started_ns = timeline_started_ns(timeline_trace);
    let body = match prepare_outgoing_body(body).await {
        Ok(body) => body,
        Err(error) => return RpcDeliveryOutcome::NotSent { error },
    };
    let (_head, head_payload) = match request_head(
        context,
        request_id.clone(),
        method,
        operation_id,
        metadata,
        application_head,
        options,
        &body,
    ) {
        Ok(head) => head,
        Err(error) => return RpcDeliveryOutcome::NotSent { error },
    };
    let request_head_digest = *blake3::hash(&head_payload).as_bytes();
    record_rpc_timeline_duration(
        timeline_trace,
        crate::network_timeline_stage::RPC_PREPARE,
        crate::network_timeline_flag::REQUEST,
        prepare_started_ns,
        true,
    );
    let request_is_inline = body.source.is_none();
    let (response_frame, last_response_progress_nonce, request_send_completed, mut send_task) =
        if request_is_inline {
            match send_request(
                writer,
                context.clone(),
                request_id.clone(),
                request_head_digest,
                head_payload,
                body,
                options,
                timeline_trace,
            )
            .await
            {
                Ok(()) => {}
                Err(failure) => {
                    return if failure.wrote_any {
                        RpcDeliveryOutcome::PossiblyApplied {
                            error: failure.error,
                        }
                    } else {
                        RpcDeliveryOutcome::NotSent {
                            error: failure.error,
                        }
                    };
                }
            }
            match read_response_head_with_progress(
                &mut reader,
                options,
                context,
                &request_id,
                request_head_digest,
                timeline_trace,
            )
            .await
            {
                Ok((frame, last_nonce)) => (frame, last_nonce, true, None),
                Err(error) => return RpcDeliveryOutcome::PossiblyApplied { error },
            }
        } else {
            let mut task = tokio::spawn(send_request(
                writer,
                context.clone(),
                request_id.clone(),
                request_head_digest,
                head_payload,
                body,
                options,
                timeline_trace,
            ));
            let (frame, last_nonce, completed) = tokio::select! {
                send_result = &mut task => {
                    match send_result {
                        Ok(Ok(())) => match read_response_head_with_progress(
                            &mut reader,
                            options,
                            context,
                            &request_id,
                            request_head_digest,
                            timeline_trace,
                        ).await {
                            Ok((frame, last_nonce)) => (frame, last_nonce, true),
                            Err(error) => return RpcDeliveryOutcome::PossiblyApplied { error },
                        },
                        Ok(Err(failure)) => return if failure.wrote_any {
                            RpcDeliveryOutcome::PossiblyApplied { error: failure.error }
                        } else {
                            RpcDeliveryOutcome::NotSent { error: failure.error }
                        },
                        Err(error) => return RpcDeliveryOutcome::PossiblyApplied {
                            error: delivery_error("join object RPC request writer", error.to_string()),
                        },
                    }
                }
                response = read_response_head_with_progress(
                    &mut reader,
                    options,
                    context,
                    &request_id,
                    request_head_digest,
                    timeline_trace,
                ) => {
                    match response {
                        Ok((frame, last_nonce)) => (frame, last_nonce, false),
                        Err(error) => {
                            let send_result = task.await;
                            return match send_result {
                                Ok(Err(failure)) if !failure.wrote_any => {
                                    RpcDeliveryOutcome::NotSent { error: failure.error }
                                }
                                _ => RpcDeliveryOutcome::PossiblyApplied { error },
                            };
                        }
                    }
                }
            };
            (frame, last_nonce, completed, Some(task))
        };
    if response_frame.kind != FRAME_RESPONSE_HEAD {
        if let Some(task) = &send_task {
            task.abort();
        }
        return RpcDeliveryOutcome::PossiblyApplied {
            error: serialization_error("expected object RPC response head"),
        };
    }
    let response_head: ResponseHeadUnsigned = match decode_authenticated_control(
        &context.channel_binding,
        AUTH_RESPONSE_HEAD,
        &response_frame.payload,
    ) {
        Ok(head) => head,
        Err(error) => {
            if let Some(task) = &send_task {
                task.abort();
            }
            return RpcDeliveryOutcome::PossiblyApplied { error };
        }
    };
    if let Err(error) =
        validate_response_head(context, &request_id, request_head_digest, &response_head)
    {
        if let Some(task) = &send_task {
            task.abort();
        }
        return RpcDeliveryOutcome::PossiblyApplied { error };
    }
    let response_head_digest = *blake3::hash(&response_frame.payload).as_bytes();
    let (metadata, application_head, descriptor, inline_body) = match response_head.disposition {
        ResponseDisposition::Accepted {
            metadata,
            application_head,
            descriptor,
            inline_body,
        } => (metadata, application_head, descriptor, inline_body),
        ResponseDisposition::Rejected { detail } => {
            if let Some(task) = &send_task {
                task.abort();
            }
            return RpcDeliveryOutcome::Rejected { detail };
        }
    };
    let (body, _verifier, receive_completion) = make_incoming_body(
        reader,
        options,
        context.clone(),
        IncomingBodySpec {
            direction: IncomingDirection::Response,
            request_id: request_id.clone(),
            head_digest: response_head_digest,
            progress_head_digest: request_head_digest,
            last_progress_nonce: last_response_progress_nonce,
            descriptor,
            inline_body,
        },
        request_is_inline,
    )
    .await;
    let consumer_result = match response_consumer(ObjectRpcStreamResponse {
        metadata,
        application_head,
        body,
    })
    .await
    {
        Ok(RpcResponseConsumption::Complete(value)) => Ok(value),
        Ok(RpcResponseConsumption::Streaming(value)) => {
            // The returned application body now owns the authenticated record
            // reader and drives it directly. Only an independently running
            // request producer still needs to be detached here.
            if !request_send_completed {
                tokio::spawn(async move {
                    let _ = send_task
                        .expect("an incomplete object RPC request has a writer task")
                        .await;
                });
            }
            drop(receive_completion);
            return RpcDeliveryOutcome::Completed(value);
        }
        Err(error) => Err(error),
    };
    let terminal = match receive_completion.join().await {
        Ok(terminal) => terminal,
        Err(error) => return RpcDeliveryOutcome::PossiblyApplied { error },
    };
    if !request_send_completed {
        match send_task
            .take()
            .expect("an incomplete object RPC request has a writer task")
            .await
        {
            Ok(Ok(())) => {}
            Ok(Err(failure)) => {
                return RpcDeliveryOutcome::PossiblyApplied {
                    error: failure.error,
                };
            }
            Err(error) => {
                return RpcDeliveryOutcome::PossiblyApplied {
                    error: delivery_error("join object RPC request writer", error.to_string()),
                };
            }
        }
    }
    match terminal {
        IncomingTerminal::Rejected(detail) => RpcDeliveryOutcome::Rejected { detail },
        IncomingTerminal::Completed(_) => match consumer_result {
            Ok(value) => RpcDeliveryOutcome::Completed(value),
            Err(error) => RpcDeliveryOutcome::PossiblyApplied { error },
        },
    }
}

#[cfg(test)]
pub(crate) async fn call_object_rpc_stream_with_consumption<S, F, Fut, T>(
    stream: S,
    context: &RpcPeerContext,
    call: ObjectRpcStreamCall,
    options: ObjectFrameIoOptions,
    response_consumer: F,
) -> RpcDeliveryOutcome<T>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    F: FnOnce(ObjectRpcStreamResponse) -> Fut,
    Fut: Future<Output = Result<RpcResponseConsumption<T>, NetworkError>>,
{
    let (reader, writer) = byte_record_halves(stream);
    call_object_rpc_records_with_consumption(
        reader,
        writer,
        context,
        call,
        options,
        response_consumer,
    )
    .await
}

pub(crate) async fn call_object_rpc_chunk_stream_with_consumption<F, Fut, T>(
    stream: AuthenticatedChunkStream,
    context: &RpcPeerContext,
    call: ObjectRpcStreamCall,
    options: ObjectFrameIoOptions,
    response_consumer: F,
) -> RpcDeliveryOutcome<T>
where
    F: FnOnce(ObjectRpcStreamResponse) -> Fut,
    Fut: Future<Output = Result<RpcResponseConsumption<T>, NetworkError>>,
{
    let (reader, writer) = owned_record_halves(stream);
    call_object_rpc_records_with_consumption(
        reader,
        writer,
        context,
        call,
        options,
        response_consumer,
    )
    .await
}

#[cfg(test)]
async fn call_object_rpc_stream<S, F, Fut, T>(
    stream: S,
    context: &RpcPeerContext,
    call: ObjectRpcStreamCall,
    options: ObjectFrameIoOptions,
    response_consumer: F,
) -> RpcDeliveryOutcome<T>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    F: FnOnce(ObjectRpcStreamResponse) -> Fut,
    Fut: Future<Output = Result<T, NetworkError>>,
{
    call_object_rpc_stream_with_consumption(
        stream,
        context,
        call,
        options,
        move |response| async move {
            response_consumer(response)
                .await
                .map(RpcResponseConsumption::Complete)
        },
    )
    .await
}

async fn write_rejection<W: RpcRecordWriter>(
    writer: &mut W,
    context: &RpcPeerContext,
    request_id: Id,
    request_head_digest: [u8; 32],
    detail: String,
) -> Result<(), NetworkError> {
    let (_, payload) = response_head(
        context,
        request_id,
        request_head_digest,
        ResponseDisposition::Rejected { detail },
    )?;
    writer
        .write_record(FRAME_RESPONSE_HEAD, &payload, "write object RPC rejection")
        .await
        .map_err(|failure| failure.error)?;
    writer
        .finish_records()
        .await
        .map_err(|failure| failure.error)
}

/// Serves exactly one call from one authenticated duplex stream. The handler
/// starts after the bounded request head is authenticated, while the body pump
/// continues independently. Response data may therefore start before the full
/// request is consumed, but a successful response finish is withheld until the
/// request's content proof verifies.
async fn serve_object_rpc_records<R, W, F, Fut>(
    mut reader: R,
    mut writer: W,
    context: &RpcPeerContext,
    options: ObjectFrameIoOptions,
    handler: F,
) -> Result<(), NetworkError>
where
    R: RpcRecordReader,
    W: RpcRecordWriter,
    F: FnOnce(ObjectRpcStreamIncomingCall, RpcPeerContext) -> Fut,
    Fut: Future<Output = Result<ObjectRpcStreamReply, NetworkError>>,
{
    let request_ingress_started_ns = if crate::network_timeline_capture_is_active() {
        crate::network_timeline_monotonic_ns()
    } else {
        0
    };
    let RpcRecord {
        kind,
        payload: head_payload,
    } = reader.read_record(options).await?;
    if kind != FRAME_REQUEST_HEAD {
        return Err(serialization_error("expected object RPC request head"));
    }
    let head: RequestHeadUnsigned =
        decode_authenticated_control(&context.channel_binding, AUTH_REQUEST_HEAD, &head_payload)?;
    validate_request_head(context, &head)?;
    let request_is_inline = matches!(head.descriptor, BodyDescriptor::Inline { .. });
    let request_head_digest = *blake3::hash(&head_payload).as_bytes();
    let request_id = head.request_id.clone();
    let negotiated_response_progress_interval =
        response_progress_interval(options, head.response_progress_interval_millis);
    let (body, request_verifier, request_completion) = make_incoming_body(
        reader,
        options,
        context.clone(),
        IncomingBodySpec {
            direction: IncomingDirection::Request,
            request_id: request_id.clone(),
            head_digest: request_head_digest,
            progress_head_digest: request_head_digest,
            last_progress_nonce: 0,
            descriptor: head.descriptor,
            inline_body: head.inline_body,
        },
        true,
    )
    .await;
    let request_ingress_duration_ns = if request_ingress_started_ns == 0 {
        0
    } else {
        crate::network_timeline_monotonic_ns().saturating_sub(request_ingress_started_ns)
    };
    let incoming = ObjectRpcStreamIncomingCall {
        request_id: request_id.clone(),
        method: head.method,
        operation_id: head.operation_id,
        metadata: head.metadata,
        application_head: head.application_head,
        body,
        request_ingress_duration_ns,
    };
    let mut response_progress_nonce = 1;
    // Keep one handler future in the caller's state and lend a pinned view to
    // the progress driver. Passing it by value makes an async state machine
    // retain multiple copies of a potentially large service future, which can
    // overflow an otherwise-idle Tokio worker stack in debug builds.
    let handler_result = {
        let mut handler_future = std::pin::pin!(handler(incoming, context.clone()));
        match await_with_stream_progress(
            &mut writer,
            context,
            IncomingDirection::Response,
            &request_id,
            request_head_digest,
            negotiated_response_progress_interval,
            &mut response_progress_nonce,
            handler_future.as_mut(),
        )
        .await
        {
            Ok(result) => result,
            Err(failure) => return Err(failure.error),
        }
    };
    let reply = match handler_result {
        Ok(reply) if reply.request_id == request_id => reply,
        Ok(_) => {
            return write_rejection(
                &mut writer,
                context,
                request_id,
                request_head_digest,
                "object RPC handler response used the wrong request id".to_string(),
            )
            .await;
        }
        Err(error) => {
            return write_rejection(
                &mut writer,
                context,
                request_id,
                request_head_digest,
                error.to_string(),
            )
            .await;
        }
    };
    let mut response_stage = RpcTimelineStageGuard::new(
        reply.timeline_trace,
        crate::network_timeline_stage::RPC_RESPONSE_STAGE,
        crate::network_timeline_flag::RESPONSE,
    );
    let mut prepare_response_body = std::pin::pin!(prepare_outgoing_body(reply.body));
    let prepared_response_body = match await_with_stream_progress(
        &mut writer,
        context,
        IncomingDirection::Response,
        &request_id,
        request_head_digest,
        negotiated_response_progress_interval,
        &mut response_progress_nonce,
        prepare_response_body.as_mut(),
    )
    .await
    {
        Ok(result) => result,
        Err(failure) => return Err(failure.error),
    };
    let mut response_body = match prepared_response_body {
        Ok(body) => body,
        Err(error) => {
            return write_rejection(
                &mut writer,
                context,
                request_id,
                request_head_digest,
                error.to_string(),
            )
            .await;
        }
    };
    let response_completes_at_known_boundary =
        matches!(response_body.descriptor, BodyDescriptor::Known { .. });
    if response_completes_at_known_boundary {
        // A known response needs no terminal record, so establish the request
        // body's verified outcome before publishing the accepted response
        // head. Unknown/length-only responses keep the terminal status record
        // and may still begin while a long request is being verified.
        let mut verify_request = std::pin::pin!(request_verifier.clone().verify());
        let request_result = await_with_stream_progress(
            &mut writer,
            context,
            IncomingDirection::Response,
            &request_id,
            request_head_digest,
            negotiated_response_progress_interval,
            &mut response_progress_nonce,
            verify_request.as_mut(),
        )
        .await
        .map_err(|failure| failure.error)?;
        if let Err(error) = request_result {
            return write_rejection(
                &mut writer,
                context,
                request_id,
                request_head_digest,
                format!("request body verification failed: {error}"),
            )
            .await;
        }
    }
    let buffer_entire_response = request_is_inline && response_body.source.is_none();
    let (_, response_payload) = response_head(
        context,
        request_id.clone(),
        request_head_digest,
        ResponseDisposition::Accepted {
            metadata: reply.metadata,
            application_head: reply.application_head,
            descriptor: response_body.descriptor,
            inline_body: response_body.inline_body.clone(),
        },
    )?;
    let response_head_digest = *blake3::hash(&response_payload).as_bytes();
    writer
        .write_record(
            FRAME_RESPONSE_HEAD,
            &response_payload,
            "write object RPC response head",
        )
        .await
        .map_err(|failure| failure.error)?;
    if buffer_entire_response {
        writer
            .finish_records()
            .await
            .map_err(|failure| failure.error)?;
        let result = match request_completion.join().await {
            Ok(IncomingTerminal::Completed(_)) => Ok(()),
            Ok(IncomingTerminal::Rejected(detail)) => Err(serialization_error(format!(
                "request body rejected: {detail}"
            ))),
            Err(error) => Err(error),
        };
        if result.is_ok() {
            response_stage.mark_success();
        }
        return result;
    }
    writer
        .flush_record()
        .await
        .map_err(|failure| failure.error)?;
    let response_verification = write_prepared_body_data(
        &mut writer,
        context,
        IncomingDirection::Response,
        &request_id,
        request_head_digest,
        negotiated_response_progress_interval,
        &mut response_progress_nonce,
        FRAME_RESPONSE_DATA,
        &mut response_body,
    )
    .await
    .map_err(|failure| failure.error)?;
    if response_completes_at_known_boundary {
        writer
            .finish_records()
            .await
            .map_err(|failure| failure.error)?;
        let result = match request_completion.join().await {
            Ok(IncomingTerminal::Completed(_)) => Ok(()),
            Ok(IncomingTerminal::Rejected(detail)) => Err(serialization_error(format!(
                "request body rejected: {detail}"
            ))),
            Err(error) => Err(error),
        };
        if result.is_ok() {
            response_stage.mark_success();
        }
        return result;
    }
    let mut verify_request = std::pin::pin!(request_verifier.verify());
    let request_result = await_with_stream_progress(
        &mut writer,
        context,
        IncomingDirection::Response,
        &request_id,
        request_head_digest,
        negotiated_response_progress_interval,
        &mut response_progress_nonce,
        verify_request.as_mut(),
    )
    .await
    .map_err(|failure| failure.error)?;
    let finish_status = match &request_result {
        Ok(_) => FinishStatus::Completed,
        Err(error) => FinishStatus::Rejected {
            detail: format!("request body verification failed: {error}"),
        },
    };
    let finish = make_finish(
        context,
        request_id,
        response_head_digest,
        response_verification,
        finish_status,
    );
    write_finish(
        &mut writer,
        context,
        FRAME_RESPONSE_FINISH,
        AUTH_RESPONSE_FINISH,
        &finish,
    )
    .await
    .map_err(|failure| failure.error)?;
    writer
        .finish_records()
        .await
        .map_err(|failure| failure.error)?;
    let result = match request_completion.join().await {
        Ok(IncomingTerminal::Completed(_)) if request_result.is_ok() => Ok(()),
        Ok(IncomingTerminal::Rejected(detail)) => Err(serialization_error(format!(
            "request body rejected: {detail}"
        ))),
        Err(error) => Err(error),
        _ => request_result.map(|_| ()),
    };
    if result.is_ok() {
        response_stage.mark_success();
    }
    result
}

pub async fn serve_object_rpc_stream<S, F, Fut>(
    stream: S,
    context: &RpcPeerContext,
    options: ObjectFrameIoOptions,
    handler: F,
) -> Result<(), NetworkError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    F: FnOnce(ObjectRpcStreamIncomingCall, RpcPeerContext) -> Fut,
    Fut: Future<Output = Result<ObjectRpcStreamReply, NetworkError>>,
{
    let (reader, writer) = byte_record_halves(stream);
    serve_object_rpc_records(reader, writer, context, options, handler).await
}

pub(crate) async fn serve_object_rpc_chunk_stream<F, Fut>(
    stream: AuthenticatedChunkStream,
    context: &RpcPeerContext,
    options: ObjectFrameIoOptions,
    handler: F,
) -> Result<(), NetworkError>
where
    F: FnOnce(ObjectRpcStreamIncomingCall, RpcPeerContext) -> Fut,
    Fut: Future<Output = Result<ObjectRpcStreamReply, NetworkError>>,
{
    let (reader, writer) = owned_record_halves(stream);
    serve_object_rpc_records(reader, writer, context, options, handler).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::owned_chunk::{
        AuthenticatedChunkReader, AuthenticatedChunkWriter, ChunkClass, ChunkLease,
        ChunkStreamFuture, RPC_RECORD_MAX, ResetReason,
    };
    use std::collections::VecDeque;
    use std::io::Cursor;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex as StdMutex};

    struct TestChunkReader {
        receiver: tokio::sync::mpsc::Receiver<Option<ChunkLease>>,
        allocator: ChunkAllocator,
    }

    impl AuthenticatedChunkReader for TestChunkReader {
        fn allocator(&self) -> &ChunkAllocator {
            &self.allocator
        }

        fn max_payload_bytes(&self) -> usize {
            RPC_RECORD_MAX
        }

        fn recv_chunk(&mut self) -> ChunkStreamFuture<'_, Option<ChunkLease>> {
            Box::pin(async move {
                self.receiver.recv().await.ok_or_else(|| NetworkError::Io {
                    operation: "receive test owned object RPC record".to_string(),
                    detail: "test channel closed without a finish record".to_string(),
                })
            })
        }
    }

    struct TestChunkWriter {
        sender: tokio::sync::mpsc::Sender<Option<ChunkLease>>,
        allocator: ChunkAllocator,
        data_slab_ids: Arc<StdMutex<Vec<u64>>>,
        record_kinds: Arc<StdMutex<Vec<u8>>>,
    }

    impl AuthenticatedChunkWriter for TestChunkWriter {
        fn max_payload_bytes(&self) -> usize {
            RPC_RECORD_MAX
        }

        fn allocator(&self) -> &ChunkAllocator {
            &self.allocator
        }

        fn send_chunk(&mut self, chunk: OwnedChunk) -> ChunkStreamFuture<'_, ()> {
            Box::pin(async move {
                self.record_kinds
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .push(chunk.as_slice()[4]);
                if chunk.class() == ChunkClass::Data {
                    self.data_slab_ids
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .push(chunk.slab_id());
                }
                self.sender
                    .send(Some(chunk.into_lease()))
                    .await
                    .map_err(|_| NetworkError::Io {
                        operation: "send test owned object RPC record".to_string(),
                        detail: "test receiver closed".to_string(),
                    })
            })
        }

        fn finish(&mut self) -> ChunkStreamFuture<'_, ()> {
            Box::pin(async move {
                self.sender.send(None).await.map_err(|_| NetworkError::Io {
                    operation: "finish test owned object RPC records".to_string(),
                    detail: "test receiver closed".to_string(),
                })
            })
        }

        fn reset(&mut self, _reason: ResetReason) -> ChunkStreamFuture<'_, ()> {
            self.finish()
        }
    }

    struct ReadyRecordReader {
        records: VecDeque<RpcRecord>,
    }

    impl RpcRecordReader for ReadyRecordReader {
        fn read_record(
            &mut self,
            _options: ObjectFrameIoOptions,
        ) -> RpcRecordFuture<'_, RpcRecord> {
            Box::pin(async move {
                self.records.pop_front().ok_or_else(|| NetworkError::Io {
                    operation: "read ready test object RPC record".to_string(),
                    detail: "no buffered record remains".to_string(),
                })
            })
        }
    }

    fn test_owned_chunk_pair() -> (
        AuthenticatedChunkStream,
        AuthenticatedChunkStream,
        Arc<StdMutex<Vec<u64>>>,
        Arc<StdMutex<Vec<u8>>>,
        Arc<StdMutex<Vec<u8>>>,
    ) {
        let (client_sender, server_receiver) = tokio::sync::mpsc::channel(8);
        let (server_sender, client_receiver) = tokio::sync::mpsc::channel(8);
        let client_allocator = ChunkAllocator::default();
        let server_allocator = ChunkAllocator::default();
        let client_data_slab_ids = Arc::new(StdMutex::new(Vec::new()));
        let server_data_slab_ids = Arc::new(StdMutex::new(Vec::new()));
        let client_record_kinds = Arc::new(StdMutex::new(Vec::new()));
        let server_record_kinds = Arc::new(StdMutex::new(Vec::new()));
        let client = AuthenticatedChunkStream::new(
            Box::new(TestChunkReader {
                receiver: client_receiver,
                allocator: client_allocator.clone(),
            }),
            Box::new(TestChunkWriter {
                sender: client_sender,
                allocator: client_allocator,
                data_slab_ids: Arc::clone(&client_data_slab_ids),
                record_kinds: Arc::clone(&client_record_kinds),
            }),
        )
        .expect("client test chunk stream");
        let server = AuthenticatedChunkStream::new(
            Box::new(TestChunkReader {
                receiver: server_receiver,
                allocator: server_allocator.clone(),
            }),
            Box::new(TestChunkWriter {
                sender: server_sender,
                allocator: server_allocator,
                data_slab_ids: server_data_slab_ids,
                record_kinds: Arc::clone(&server_record_kinds),
            }),
        )
        .expect("server test chunk stream");
        (
            client,
            server,
            client_data_slab_ids,
            client_record_kinds,
            server_record_kinds,
        )
    }

    fn node(name: &str) -> NodeId {
        NodeId::from_legacy_named(name).expect("node id")
    }

    fn peer_contexts() -> (RpcPeerContext, RpcPeerContext) {
        let client = node("node_rpc_stream_client");
        let server = node("node_rpc_stream_server");
        let session_id = Id::from_legacy_named("session_rpc_stream").expect("session id");
        let binding = *blake3::hash(b"rpc-stream-test-binding").as_bytes();
        (
            RpcPeerContext::authenticated(
                client.clone(),
                server.clone(),
                session_id.clone(),
                binding,
            )
            .expect("client context"),
            RpcPeerContext::authenticated(server, client, session_id, binding)
                .expect("server context"),
        )
    }

    #[derive(Default)]
    struct VectoredOnlyWriter {
        bytes: Vec<u8>,
        vectored_calls: usize,
    }

    impl AsyncWrite for VectoredOnlyWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            _buffer: &[u8],
        ) -> Poll<io::Result<usize>> {
            panic!("object frame unexpectedly used the scalar copy path")
        }

        fn poll_write_vectored(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            buffers: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            self.vectored_calls += 1;
            let written = buffers.iter().map(|buffer| buffer.len()).sum();
            for buffer in buffers {
                self.bytes.extend_from_slice(buffer);
            }
            Poll::Ready(Ok(written))
        }

        fn is_write_vectored(&self) -> bool {
            true
        }

        fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn object_frame_moves_prefix_and_payload_through_one_vectored_write() {
        let payload = body_bytes(OBJECT_RPC_STREAM_MAX_DATA_FRAME_BYTES, 0x51);
        let mut writer = VectoredOnlyWriter::default();

        write_wire_frame(
            &mut writer,
            FRAME_REQUEST_DATA,
            &payload,
            "test object frame vectored write",
        )
        .await
        .expect("write frame");

        assert_eq!(writer.vectored_calls, 1);
        assert_eq!(
            writer.bytes.len(),
            payload.len() + OBJECT_RPC_STREAM_WIRE_PREFIX_BYTES
        );
        assert_eq!(&writer.bytes[..4], &OBJECT_RPC_STREAM_WIRE_MAGIC);
        assert_eq!(writer.bytes[4], FRAME_REQUEST_DATA);
        assert_eq!(
            &writer.bytes[OBJECT_RPC_STREAM_WIRE_PREFIX_BYTES..],
            payload
        );
    }

    #[tokio::test]
    async fn application_body_adapter_preserves_received_chunk_ownership() {
        let expected = Bytes::from(vec![0x6d; OBJECT_RPC_STREAM_MAX_DATA_FRAME_BYTES]);
        let expected_pointer = expected.as_ptr();
        let (mut sender, incoming) = ObjectRpcIncomingBody::multiplexed_chunk_ring_with_depth(
            Some(expected.len() as u64),
            1,
        );
        assert!(sender.send(expected).await.expect("send body chunk"));
        sender
            .finish(OBJECT_RPC_STREAM_MAX_DATA_FRAME_BYTES as u64)
            .await
            .expect("finish body");
        drop(sender);
        let (_verification_sender, verification_receiver) = watch::channel(None);
        let stream_body = ObjectRpcStreamIncomingBody {
            body: incoming,
            verifier: ObjectRpcStreamBodyVerifier {
                receiver: verification_receiver,
            },
        };

        let mut chunks = stream_body.into_application_body().into_chunk_stream();
        let actual = chunks
            .next_chunk(usize::MAX)
            .await
            .expect("read body chunk")
            .expect("body chunk");

        assert_eq!(actual.as_ptr(), expected_pointer);
        assert_eq!(actual.len(), OBJECT_RPC_STREAM_MAX_DATA_FRAME_BYTES);
    }

    #[tokio::test]
    async fn known_body_is_pulled_by_the_application_without_a_receive_task() {
        let expected = Bytes::from_static(b"already buffered typed body");
        let digest = *blake3::hash(&expected).as_bytes();
        let (_, server_context) = peer_contexts();
        let request_id = Id::from_legacy_named("request_buffered_known_body").expect("request id");
        let reader = ReadyRecordReader {
            records: VecDeque::from([RpcRecord {
                kind: FRAME_REQUEST_DATA,
                payload: ObjectRpcBodyChunk::bytes(expected.clone()),
            }]),
        };

        let (body, verifier, completion) = make_incoming_body(
            reader,
            ObjectFrameIoOptions::default(),
            server_context,
            IncomingBodySpec {
                direction: IncomingDirection::Request,
                request_id,
                head_digest: [0x31; 32],
                progress_head_digest: [0x31; 32],
                last_progress_nonce: 0,
                descriptor: BodyDescriptor::Known {
                    length: expected.len() as u64,
                    digest,
                },
                inline_body: None,
            },
            false,
        )
        .await;

        assert!(
            !completion.is_ready(),
            "constructing the body must not run or spawn its record reader"
        );
        assert_eq!(
            body.materialize_to_vec().await.expect("materialize body"),
            expected
        );
        assert!(
            completion.is_ready(),
            "the application pull must publish terminal verification"
        );
        assert_eq!(
            verifier.verify().await.expect("verify buffered body"),
            ObjectRpcBodyVerification {
                length: expected.len() as u64,
                digest,
            }
        );
        assert!(matches!(
            completion.join().await.expect("complete pulled body"),
            IncomingTerminal::Completed(_)
        ));
    }

    #[tokio::test]
    async fn owned_object_rpc_preserves_body_slab_through_delivery() {
        let (client, server, transmitted_data_slab_ids, _, _) = test_owned_chunk_pair();
        let (client_context, server_context) = peer_contexts();
        let payload = vec![0x7a; OBJECT_RPC_STREAM_INLINE_BODY_BYTES + 1024];
        let expected = payload.clone();
        let (delivered_id_sender, delivered_id_receiver) = tokio::sync::oneshot::channel();
        let server_task = tokio::spawn(async move {
            serve_object_rpc_chunk_stream(
                server,
                &server_context,
                ObjectFrameIoOptions::default(),
                move |incoming, _| async move {
                    let request_id = incoming.request_id.clone();
                    let mut body = incoming.body.into_application_body();
                    let chunk = body
                        .next_chunk()
                        .await
                        .expect("read owned request body")
                        .expect("request body chunk");
                    assert_eq!(&*chunk, &expected);
                    delivered_id_sender
                        .send(chunk.pooled_slab_id().expect("pooled request body"))
                        .expect("record delivered slab id");
                    assert!(
                        body.next_chunk()
                            .await
                            .expect("finish owned request body")
                            .is_none()
                    );
                    Ok(ObjectRpcStreamReply::new(
                        request_id,
                        ObjectRpcStreamBody::bytes(Vec::new()),
                    ))
                },
            )
            .await
            .expect("serve owned object RPC");
        });

        let result = call_object_rpc_chunk_stream_with_consumption(
            client,
            &client_context,
            ObjectRpcStreamCall::new("test.owned", ObjectRpcStreamBody::bytes(payload)),
            ObjectFrameIoOptions::default(),
            |response| async move {
                assert!(response.body.materialize_to_vec().await?.is_empty());
                Ok(RpcResponseConsumption::Complete(()))
            },
        )
        .await;
        assert!(matches!(result, RpcDeliveryOutcome::Completed(())));
        server_task.await.expect("join owned object RPC server");
        let delivered_id = delivered_id_receiver.await.expect("delivered slab id");
        let transmitted = transmitted_data_slab_ids
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        assert_eq!(transmitted.as_slice(), &[delivered_id]);
    }

    #[tokio::test]
    async fn small_known_owned_rpc_uses_head_and_data_without_redundant_finish() {
        let (client, server, _, client_record_kinds, server_record_kinds) = test_owned_chunk_pair();
        let (client_context, server_context) = peer_contexts();
        let request = "small owned request".to_string();
        let expected_request = request.clone();
        let response = "small owned response".to_string();
        let expected_response = response.clone();
        let server_task = tokio::spawn(async move {
            serve_object_rpc_chunk_stream(
                server,
                &server_context,
                ObjectFrameIoOptions::default(),
                move |incoming, _| {
                    let response = response.clone();
                    let expected_request = expected_request.clone();
                    async move {
                        let request_id = incoming.request_id.clone();
                        let encoded = incoming.body.materialize_to_vec().await?;
                        let actual: String = SinomoObjectCodec::network_frame().decode(&encoded)?;
                        assert_eq!(actual, expected_request);
                        Ok(ObjectRpcStreamReply::new(
                            request_id,
                            ObjectRpcStreamBody::source(ObjectRpcBodySource::sobj_value(response)?),
                        ))
                    }
                },
            )
            .await
            .expect("serve small owned object RPC");
        });

        let outcome = call_object_rpc_chunk_stream_with_consumption(
            client,
            &client_context,
            ObjectRpcStreamCall::new(
                "test.small-owned",
                ObjectRpcStreamBody::source(
                    ObjectRpcBodySource::sobj_value(request).expect("encode request"),
                ),
            ),
            ObjectFrameIoOptions::default(),
            |response| async move {
                let encoded = response.body.materialize_to_vec().await?;
                let actual: String = SinomoObjectCodec::network_frame().decode(&encoded)?;
                assert_eq!(actual, expected_response);
                Ok(RpcResponseConsumption::Complete(()))
            },
        )
        .await;
        assert!(matches!(outcome, RpcDeliveryOutcome::Completed(())));
        server_task.await.expect("join small owned RPC server");

        assert_eq!(
            client_record_kinds
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .as_slice(),
            &[FRAME_REQUEST_HEAD, FRAME_REQUEST_DATA]
        );
        assert_eq!(
            server_record_kinds
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .as_slice(),
            &[FRAME_RESPONSE_HEAD, FRAME_RESPONSE_DATA]
        );
    }

    #[test]
    fn authenticated_control_serializes_once_and_authenticates_wire_bytes() {
        struct Counted<'a> {
            value: &'a StreamProgressUnsigned,
            serializations: &'a AtomicUsize,
        }

        impl Serialize for Counted<'_> {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                self.serializations.fetch_add(1, Ordering::Relaxed);
                self.value.serialize(serializer)
            }
        }

        let (client_context, _) = peer_contexts();
        let (origin_node_id, destination_node_id) = client_context.wire_endpoints();
        let unsigned = StreamProgressUnsigned {
            protocol: OBJECT_RPC_STREAM_PROTOCOL.to_string(),
            origin_node_id,
            destination_node_id,
            session_id: client_context.session_id.clone(),
            channel_binding: client_context.channel_binding,
            request_id: Id::from_legacy_named("request_authenticated_control").expect("request id"),
            head_digest: [0x41; 32],
            nonce: 1,
        };
        let serializations = AtomicUsize::new(0);
        let payload = encode_authenticated_control(
            &client_context.channel_binding,
            AUTH_REQUEST_PROGRESS,
            &Counted {
                value: &unsigned,
                serializations: &serializations,
            },
            OBJECT_RPC_STREAM_MAX_TERMINAL_BYTES,
        )
        .expect("authenticated control");

        assert_eq!(serializations.load(Ordering::Relaxed), 1);
        let canonical_unsigned = SinomoObjectCodec::network_frame()
            .encode(&unsigned)
            .expect("canonical unsigned control");
        assert_eq!(
            &payload[AUTHENTICATED_CONTROL_TAG_BYTES..],
            canonical_unsigned
        );
        assert_eq!(
            &payload[..AUTHENTICATED_CONTROL_TAG_BYTES],
            authenticated_control_tag(
                &client_context.channel_binding,
                AUTH_REQUEST_PROGRESS,
                &canonical_unsigned,
            )
            .as_bytes()
        );
        let decoded: StreamProgressUnsigned = decode_authenticated_control(
            &client_context.channel_binding,
            AUTH_REQUEST_PROGRESS,
            &payload,
        )
        .expect("verified control");
        assert_eq!(decoded, unsigned);

        let mut tampered_tag = payload.clone();
        tampered_tag[0] ^= 1;
        assert_eq!(
            decode_authenticated_control::<StreamProgressUnsigned>(
                &client_context.channel_binding,
                AUTH_REQUEST_PROGRESS,
                &tampered_tag,
            ),
            Err(NetworkError::AuthDenied)
        );
        let mut tampered_unsigned = payload;
        tampered_unsigned[AUTHENTICATED_CONTROL_TAG_BYTES] ^= 1;
        assert_eq!(
            decode_authenticated_control::<StreamProgressUnsigned>(
                &client_context.channel_binding,
                AUTH_REQUEST_PROGRESS,
                &tampered_unsigned,
            ),
            Err(NetworkError::AuthDenied)
        );
    }

    #[tokio::test]
    async fn unlimited_materialization_reports_unrepresentable_allocation() {
        let (_sender, incoming) =
            ObjectRpcIncomingBody::multiplexed_chunk_ring_with_depth(Some(u64::MAX), 1);
        let (_verification_sender, verification_receiver) = watch::channel(None);
        let body = ObjectRpcStreamIncomingBody {
            body: incoming,
            verifier: ObjectRpcStreamBodyVerifier {
                receiver: verification_receiver,
            },
        };

        let error = body
            .materialize_to_vec()
            .await
            .expect_err("an unrepresentable body must fail without aborting");

        assert!(
            matches!(error, NetworkError::ResourceExhausted { .. }),
            "unexpected materialization error: {error:?}"
        );
    }

    fn body_bytes(size: usize, salt: u8) -> Vec<u8> {
        (0..size)
            .map(|offset| (offset as u8).wrapping_mul(31).wrapping_add(salt))
            .collect()
    }

    async fn round_trip(request: Vec<u8>, response: Vec<u8>) {
        let capacity = OBJECT_RPC_STREAM_RECEIVE_WINDOW_BYTES;
        let (client, server) = tokio::io::duplex(capacity);
        let (client_context, server_context) = peer_contexts();
        let expected_request = request.clone();
        let expected_response = response.clone();
        let server_task = tokio::spawn(async move {
            serve_object_rpc_stream(
                server,
                &server_context,
                ObjectFrameIoOptions { idle_timeout: None },
                move |call, _context| {
                    let expected_request = expected_request.clone();
                    let response = response.clone();
                    async move {
                        let request_id = call.request_id.clone();
                        let actual = call
                            .body
                            .collect_to_vec(expected_request.len().saturating_add(1))
                            .await?;
                        assert_eq!(actual, expected_request);
                        Ok(ObjectRpcStreamReply::new(
                            request_id,
                            ObjectRpcStreamBody::bytes(response),
                        ))
                    }
                },
            )
            .await
        });
        let outcome = call_object_rpc_stream(
            client,
            &client_context,
            ObjectRpcStreamCall::new("test.echo", ObjectRpcStreamBody::bytes(request)),
            ObjectFrameIoOptions { idle_timeout: None },
            move |response| {
                let expected_response = expected_response.clone();
                async move {
                    let actual = response
                        .body
                        .collect_to_vec(expected_response.len().saturating_add(1))
                        .await?;
                    assert_eq!(actual, expected_response);
                    Ok(())
                }
            },
        )
        .await;
        assert!(matches!(outcome, RpcDeliveryOutcome::Completed(())));
        server_task.await.expect("server task").expect("serve RPC");
    }

    #[tokio::test]
    async fn complete_request_response_payload_boundary_matrix_uses_one_protocol() {
        let inline = OBJECT_RPC_STREAM_INLINE_BODY_BYTES;
        let frame = OBJECT_RPC_STREAM_MAX_DATA_FRAME_BYTES;
        let window = OBJECT_RPC_STREAM_RECEIVE_WINDOW_BYTES;
        let sizes = [
            0,
            1,
            inline - 1,
            inline,
            inline + 1,
            frame - 1,
            frame,
            frame + 1,
            window - 1,
            window,
            window + 1,
            window * 4 + 17,
        ];
        for (request_index, request_size) in sizes.into_iter().enumerate() {
            for (response_index, response_size) in sizes.into_iter().enumerate() {
                round_trip(
                    body_bytes(request_size, request_index as u8),
                    body_bytes(response_size, response_index as u8),
                )
                .await;
            }
        }
    }

    #[tokio::test]
    async fn known_and_deferred_bodies_share_the_same_call_api() {
        for body in [
            ObjectRpcStreamBody::bytes(b"known".to_vec()),
            ObjectRpcStreamBody::one_pass_reader(Cursor::new(b"deferred".to_vec()), None),
        ] {
            let (client, server) = tokio::io::duplex(4096);
            let (client_context, server_context) = peer_contexts();
            let server_task = tokio::spawn(async move {
                serve_object_rpc_stream(
                    server,
                    &server_context,
                    ObjectFrameIoOptions { idle_timeout: None },
                    |call, _| async move {
                        let request_id = call.request_id.clone();
                        call.body.collect_to_vec(64).await?;
                        Ok(ObjectRpcStreamReply::new(
                            request_id,
                            ObjectRpcStreamBody::bytes(Vec::new()),
                        ))
                    },
                )
                .await
            });
            let outcome = call_object_rpc_stream(
                client,
                &client_context,
                ObjectRpcStreamCall::new("test.body", body),
                ObjectFrameIoOptions { idle_timeout: None },
                |response| async move {
                    response.body.collect_to_vec(1).await?;
                    Ok(())
                },
            )
            .await;
            assert!(matches!(outcome, RpcDeliveryOutcome::Completed(())));
            server_task.await.expect("server task").expect("serve RPC");
        }
    }

    #[tokio::test]
    async fn authenticated_stale_request_head_is_expired() {
        let (client_context, server_context) = peer_contexts();
        let body = prepare_outgoing_body(ObjectRpcStreamBody::bytes(Vec::new()))
            .await
            .expect("body");
        let (mut head, _) = request_head(
            &client_context,
            Id::generate(),
            "test.stale".to_string(),
            None,
            BTreeMap::new(),
            Vec::new(),
            ObjectFrameIoOptions::none(),
            &body,
        )
        .expect("head");
        head.issued_at_unix_millis = 0;
        let payload = encode_authenticated_control(
            &client_context.channel_binding,
            AUTH_REQUEST_HEAD,
            &head,
            OBJECT_RPC_STREAM_MAX_HEAD_BYTES,
        )
        .expect("authenticated head");
        let decoded: RequestHeadUnsigned = decode_authenticated_control(
            &server_context.channel_binding,
            AUTH_REQUEST_HEAD,
            &payload,
        )
        .expect("verified head");
        assert_eq!(
            validate_request_head(&server_context, &decoded),
            Err(NetworkError::AuthExpired)
        );
    }

    #[tokio::test]
    async fn authenticated_handler_rejection_is_not_ambiguous_delivery() {
        let (client, server) = tokio::io::duplex(4096);
        let (client_context, server_context) = peer_contexts();
        let server_task = tokio::spawn(async move {
            serve_object_rpc_stream(
                server,
                &server_context,
                ObjectFrameIoOptions { idle_timeout: None },
                |_call, _| async { Err(NetworkError::AuthDenied) },
            )
            .await
        });
        let outcome = call_object_rpc_stream(
            client,
            &client_context,
            ObjectRpcStreamCall::new("test.reject", ObjectRpcStreamBody::bytes(Vec::new())),
            ObjectFrameIoOptions { idle_timeout: None },
            |_response| async { Ok(()) },
        )
        .await;
        assert!(matches!(
            outcome,
            RpcDeliveryOutcome::Rejected { detail }
                if detail == "network authorization was denied"
        ));
        server_task.await.expect("server task").expect("serve RPC");
    }

    #[tokio::test(start_paused = true)]
    async fn authenticated_progress_preserves_a_healthy_long_running_handler() {
        const IDLE_TIMEOUT: Duration = Duration::from_millis(20);
        const HANDLER_DELAY: Duration = Duration::from_millis(100);

        let (client, server) = tokio::io::duplex(4096);
        let (client_context, server_context) = peer_contexts();
        let options = ObjectFrameIoOptions::idle_timeout(IDLE_TIMEOUT);
        let server_task = tokio::spawn(async move {
            serve_object_rpc_stream(server, &server_context, options, |call, _| async move {
                let request_id = call.request_id.clone();
                tokio::time::sleep(HANDLER_DELAY).await;
                Ok(ObjectRpcStreamReply::new(
                    request_id,
                    ObjectRpcStreamBody::bytes(Vec::new()),
                ))
            })
            .await
        });
        let started_at = tokio::time::Instant::now();
        let outcome = call_object_rpc_stream(
            client,
            &client_context,
            ObjectRpcStreamCall::new("test.long-handler", ObjectRpcStreamBody::bytes(Vec::new())),
            options,
            |response| async move {
                response.body.collect_to_vec(1).await?;
                Ok(())
            },
        )
        .await;

        assert!(matches!(outcome, RpcDeliveryOutcome::Completed(())));
        assert!(tokio::time::Instant::now().duration_since(started_at) >= HANDLER_DELAY);
        server_task.await.expect("server task").expect("serve RPC");
    }

    #[test]
    fn authenticated_progress_rejects_a_replayed_nonce() {
        let (client_context, server_context) = peer_contexts();
        let request_id = Id::generate();
        let head_digest = *blake3::hash(b"progress-head").as_bytes();
        let payload = stream_progress(
            &client_context,
            IncomingDirection::Request,
            request_id.clone(),
            head_digest,
            1,
        )
        .expect("progress frame");
        let mut last_nonce = 0;

        validate_stream_progress(
            &server_context,
            IncomingDirection::Request,
            &request_id,
            head_digest,
            &mut last_nonce,
            &payload,
        )
        .expect("first authenticated progress frame");
        assert_eq!(last_nonce, 1);
        assert_eq!(
            validate_stream_progress(
                &server_context,
                IncomingDirection::Request,
                &request_id,
                head_digest,
                &mut last_nonce,
                &payload,
            ),
            Err(NetworkError::AuthDenied)
        );
    }

    #[derive(Debug)]
    struct DelayedReader {
        inner: Cursor<Vec<u8>>,
        delay: Duration,
        waiting: Option<Pin<Box<tokio::time::Sleep>>>,
    }

    impl DelayedReader {
        fn new(bytes: Vec<u8>, delay: Duration) -> Self {
            Self {
                inner: Cursor::new(bytes),
                delay,
                waiting: None,
            }
        }
    }

    impl AsyncRead for DelayedReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
            buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            if self.waiting.is_none() {
                self.waiting = Some(Box::pin(tokio::time::sleep(self.delay)));
            }
            if self
                .waiting
                .as_mut()
                .expect("delayed read timer")
                .as_mut()
                .poll(context)
                .is_pending()
            {
                return Poll::Pending;
            }
            self.waiting = None;

            let position = usize::try_from(self.inner.position()).unwrap_or(usize::MAX);
            let remaining = self.inner.get_ref().len().saturating_sub(position);
            let copy_bytes = remaining.min(buffer.remaining());
            if copy_bytes != 0 {
                buffer.put_slice(&self.inner.get_ref()[position..position + copy_bytes]);
                self.inner
                    .set_position((position.saturating_add(copy_bytes)) as u64);
            }
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test(start_paused = true)]
    async fn authenticated_progress_covers_slow_one_pass_body_producers() {
        const IDLE_TIMEOUT: Duration = Duration::from_millis(20);
        const PRODUCER_DELAY: Duration = Duration::from_millis(100);
        let request_bytes = b"slow request body".to_vec();
        let response_bytes = b"slow response body".to_vec();
        let expected_request = request_bytes.clone();
        let expected_response = response_bytes.clone();
        let (client, server) = tokio::io::duplex(4096);
        let (client_context, server_context) = peer_contexts();
        let options = ObjectFrameIoOptions::idle_timeout(IDLE_TIMEOUT);
        let server_task = tokio::spawn(async move {
            serve_object_rpc_stream(server, &server_context, options, move |call, _| {
                let expected_request = expected_request.clone();
                let response_bytes = response_bytes.clone();
                async move {
                    let request_id = call.request_id.clone();
                    assert_eq!(call.body.collect_to_vec(1024).await?, expected_request);
                    let response_len = response_bytes.len() as u64;
                    Ok(ObjectRpcStreamReply::new(
                        request_id,
                        ObjectRpcStreamBody::one_pass_async_reader(
                            DelayedReader::new(response_bytes, PRODUCER_DELAY),
                            Some(response_len),
                        ),
                    ))
                }
            })
            .await
        });
        let request_len = request_bytes.len() as u64;
        let outcome = call_object_rpc_stream(
            client,
            &client_context,
            ObjectRpcStreamCall::new(
                "test.slow-producers",
                ObjectRpcStreamBody::one_pass_async_reader(
                    DelayedReader::new(request_bytes, PRODUCER_DELAY),
                    Some(request_len),
                ),
            ),
            options,
            move |response| async move {
                let actual = response.body.collect_to_vec(1024).await?;
                assert_eq!(actual, expected_response);
                Ok(())
            },
        )
        .await;

        assert!(
            matches!(outcome, RpcDeliveryOutcome::Completed(())),
            "unexpected slow-producer outcome: {outcome:?}"
        );
        server_task.await.expect("server task").expect("serve RPC");
    }

    #[tokio::test(start_paused = true)]
    async fn authenticated_progress_keeps_an_early_response_alive_until_request_verification() {
        const IDLE_TIMEOUT: Duration = Duration::from_millis(20);
        const PRODUCER_DELAY: Duration = Duration::from_millis(100);
        let request_bytes = b"slow request behind an early response".to_vec();
        let request_len = request_bytes.len() as u64;
        let (client, server) = tokio::io::duplex(4096);
        let (client_context, server_context) = peer_contexts();
        let options = ObjectFrameIoOptions::idle_timeout(IDLE_TIMEOUT);
        let server_task = tokio::spawn(async move {
            serve_object_rpc_stream(
                server,
                &server_context,
                options,
                move |call, _| async move {
                    let request_id = call.request_id.clone();
                    assert_eq!(call.body.total_bytes(), Some(request_len));
                    drop(call.body);
                    Ok(ObjectRpcStreamReply::new(
                        request_id,
                        ObjectRpcStreamBody::bytes(Vec::new()),
                    ))
                },
            )
            .await
        });
        let outcome = call_object_rpc_stream(
            client,
            &client_context,
            ObjectRpcStreamCall::new(
                "test.early-response-slow-request",
                ObjectRpcStreamBody::one_pass_async_reader(
                    DelayedReader::new(request_bytes, PRODUCER_DELAY),
                    Some(request_len),
                ),
            ),
            options,
            |response| async move {
                response.body.collect_to_vec(1).await?;
                Ok(())
            },
        )
        .await;

        assert!(
            matches!(outcome, RpcDeliveryOutcome::Completed(())),
            "unexpected early-response outcome: {outcome:?}"
        );
        server_task.await.expect("server task").expect("serve RPC");
    }

    #[derive(Debug)]
    struct CountingReader {
        remaining: u64,
        observed: Arc<AtomicU64>,
    }

    #[derive(Debug)]
    struct FailingReader {
        phase: u8,
        polls: Arc<AtomicU64>,
    }

    impl AsyncRead for FailingReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            match self.phase {
                0 => {
                    self.phase = 1;
                    buffer.put_slice(b"partially-sent");
                    Poll::Ready(Ok(()))
                }
                1 => {
                    self.phase = 2;
                    Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "synthetic one-pass producer failure",
                    )))
                }
                _ => panic!("a possibly applied one-pass body must never be read again"),
            }
        }
    }

    impl AsyncRead for CountingReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            if self.remaining == 0 {
                return Poll::Ready(Ok(()));
            }
            let bytes = usize::try_from(self.remaining)
                .unwrap_or(usize::MAX)
                .min(buffer.remaining())
                .min(4096);
            let chunk = vec![0x5a; bytes];
            buffer.put_slice(&chunk);
            self.remaining -= bytes as u64;
            self.observed.fetch_add(bytes as u64, Ordering::SeqCst);
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn response_head_can_start_before_request_body_is_consumed() {
        let total = (OBJECT_RPC_STREAM_RECEIVE_WINDOW_BYTES * 8) as u64;
        let observed = Arc::new(AtomicU64::new(0));
        let source = CountingReader {
            remaining: total,
            observed: Arc::clone(&observed),
        };
        let (client, server) = tokio::io::duplex(32 * 1024);
        let (client_context, server_context) = peer_contexts();
        let server_task = tokio::spawn(async move {
            serve_object_rpc_stream(
                server,
                &server_context,
                ObjectFrameIoOptions { idle_timeout: None },
                |call, _| async move {
                    Ok(ObjectRpcStreamReply::new(
                        call.request_id,
                        ObjectRpcStreamBody::bytes(b"early".to_vec()),
                    ))
                },
            )
            .await
        });
        let consumer_observed = Arc::clone(&observed);
        let outcome = call_object_rpc_stream(
            client,
            &client_context,
            ObjectRpcStreamCall::new(
                "test.early",
                ObjectRpcStreamBody::one_pass_async_reader(source, None),
            ),
            ObjectFrameIoOptions { idle_timeout: None },
            move |response| async move {
                assert!(
                    consumer_observed.load(Ordering::SeqCst) < total,
                    "response must be visible before the entire request is produced"
                );
                assert_eq!(response.body.collect_to_vec(16).await?, b"early");
                Ok(())
            },
        )
        .await;
        assert!(matches!(outcome, RpcDeliveryOutcome::Completed(())));
        server_task.await.expect("server task").expect("serve RPC");
    }

    #[tokio::test]
    async fn corrupted_known_body_is_rejected_at_completion() {
        let bytes = body_bytes(OBJECT_RPC_STREAM_INLINE_BODY_BYTES + 1, 9);
        let source = ObjectRpcBodySource::bytes(bytes);
        let wrong_digest = [0x42; 32];
        let body = ObjectRpcStreamBody::known(source, wrong_digest).expect("known body");
        let (client, server) = tokio::io::duplex(64 * 1024);
        let (client_context, server_context) = peer_contexts();
        let server_task = tokio::spawn(async move {
            serve_object_rpc_stream(
                server,
                &server_context,
                ObjectFrameIoOptions { idle_timeout: None },
                |call, _| async move {
                    Ok(ObjectRpcStreamReply::new(
                        call.request_id,
                        ObjectRpcStreamBody::bytes(b"speculative".to_vec()),
                    ))
                },
            )
            .await
        });
        let outcome = call_object_rpc_stream(
            client,
            &client_context,
            ObjectRpcStreamCall::new("test.corrupt", body),
            ObjectFrameIoOptions { idle_timeout: None },
            |response| async move {
                response.body.collect_to_vec(64).await?;
                Ok(())
            },
        )
        .await;
        assert!(matches!(
            outcome,
            RpcDeliveryOutcome::PossiblyApplied { .. }
        ));
        assert!(server_task.await.expect("server task").is_err());
    }

    #[tokio::test]
    async fn possibly_applied_one_pass_request_is_never_resent() {
        let polls = Arc::new(AtomicU64::new(0));
        let reader = FailingReader {
            phase: 0,
            polls: Arc::clone(&polls),
        };
        let (client, server) = tokio::io::duplex(4096);
        let (client_context, server_context) = peer_contexts();
        let server_task = tokio::spawn(async move {
            serve_object_rpc_stream(
                server,
                &server_context,
                ObjectFrameIoOptions { idle_timeout: None },
                |call, _| async move {
                    Ok(ObjectRpcStreamReply::new(
                        call.request_id,
                        ObjectRpcStreamBody::bytes(b"speculative".to_vec()),
                    ))
                },
            )
            .await
        });
        let outcome = call_object_rpc_stream(
            client,
            &client_context,
            ObjectRpcStreamCall::new(
                "test.no-retry",
                ObjectRpcStreamBody::one_pass_async_reader(reader, None),
            ),
            ObjectFrameIoOptions { idle_timeout: None },
            |response| async move {
                response.body.collect_to_vec(64).await?;
                Ok(())
            },
        )
        .await;
        assert!(matches!(
            outcome,
            RpcDeliveryOutcome::PossiblyApplied { .. }
        ));
        assert_eq!(polls.load(Ordering::SeqCst), 2);
        assert!(server_task.await.expect("server task").is_err());
    }

    #[tokio::test]
    async fn oversized_head_fails_from_prefix_before_payload_allocation() {
        let (mut client, server) = tokio::io::duplex(64);
        let (_, server_context) = peer_contexts();
        let server_task = tokio::spawn(async move {
            serve_object_rpc_stream(
                server,
                &server_context,
                ObjectFrameIoOptions { idle_timeout: None },
                |_call, _| async { unreachable!("oversized head must not reach handler") },
            )
            .await
        });
        let mut prefix = [0_u8; OBJECT_RPC_STREAM_WIRE_PREFIX_BYTES];
        prefix[..4].copy_from_slice(&OBJECT_RPC_STREAM_WIRE_MAGIC);
        prefix[4] = FRAME_REQUEST_HEAD;
        prefix[5..].copy_from_slice(&((OBJECT_RPC_STREAM_MAX_HEAD_BYTES + 1) as u32).to_be_bytes());
        client.write_all(&prefix).await.expect("write prefix");
        let error = server_task
            .await
            .expect("server task")
            .expect_err("oversized head");
        assert_eq!(
            error,
            NetworkError::FrameTooLarge {
                actual_bytes: OBJECT_RPC_STREAM_MAX_HEAD_BYTES + 1,
                max_bytes: OBJECT_RPC_STREAM_MAX_HEAD_BYTES,
            }
        );
    }

    #[tokio::test]
    async fn destination_or_channel_binding_mismatch_is_denied() {
        let (client, server) = tokio::io::duplex(4096);
        let (client_context, mut server_context) = peer_contexts();
        server_context.channel_binding = [0x77; 32];
        let server_task = tokio::spawn(async move {
            serve_object_rpc_stream(
                server,
                &server_context,
                ObjectFrameIoOptions { idle_timeout: None },
                |_call, _| async { unreachable!("mismatched context must not reach handler") },
            )
            .await
        });
        let outcome = call_object_rpc_stream(
            client,
            &client_context,
            ObjectRpcStreamCall::new("test.binding", ObjectRpcStreamBody::bytes(Vec::new())),
            ObjectFrameIoOptions { idle_timeout: None },
            |_response| async { Ok(()) },
        )
        .await;
        assert!(matches!(
            outcome,
            RpcDeliveryOutcome::PossiblyApplied { .. }
        ));
        assert_eq!(
            server_task.await.expect("server task"),
            Err(NetworkError::AuthDenied)
        );
    }

    #[tokio::test]
    async fn deferred_finish_is_bound_to_request_destination_and_channel() {
        let (client_context, _) = peer_contexts();
        let verification = ObjectRpcBodyVerification {
            length: 3,
            digest: *blake3::hash(b"abc").as_bytes(),
        };
        let finish = make_finish(
            &client_context,
            Id::from_legacy_named("request_finish_binding").expect("request id"),
            [0x11; 32],
            verification,
            FinishStatus::Completed,
        );
        let payload = encode_authenticated_control(
            &client_context.channel_binding,
            AUTH_REQUEST_FINISH,
            &finish,
            OBJECT_RPC_STREAM_MAX_TERMINAL_BYTES,
        )
        .expect("finish payload");
        let mut tampered = finish.clone();
        tampered.destination_node_id = Some(node("node_wrong_destination"));
        let mut tampered_payload = payload[..AUTHENTICATED_CONTROL_TAG_BYTES].to_vec();
        tampered_payload.extend(
            SinomoObjectCodec::network_frame()
                .encode(&tampered)
                .expect("tampered finish"),
        );
        assert_eq!(
            decode_authenticated_control::<BodyFinishUnsigned>(
                &client_context.channel_binding,
                AUTH_REQUEST_FINISH,
                &tampered_payload,
            ),
            Err(NetworkError::AuthDenied)
        );
        let mut tampered = finish;
        tampered.channel_binding = [0x99; 32];
        let mut tampered_payload = payload[..AUTHENTICATED_CONTROL_TAG_BYTES].to_vec();
        tampered_payload.extend(
            SinomoObjectCodec::network_frame()
                .encode(&tampered)
                .expect("tampered finish"),
        );
        assert_eq!(
            decode_authenticated_control::<BodyFinishUnsigned>(
                &client_context.channel_binding,
                AUTH_REQUEST_FINISH,
                &tampered_payload,
            ),
            Err(NetworkError::AuthDenied)
        );
    }

    #[tokio::test]
    async fn invalid_call_is_not_sent() {
        let (client, _server) = tokio::io::duplex(64);
        let (client_context, _) = peer_contexts();
        let call = ObjectRpcStreamCall::new(" ", ObjectRpcStreamBody::bytes(Vec::new()));
        assert!(matches!(
            call_object_rpc_stream(
                client,
                &client_context,
                call,
                ObjectFrameIoOptions { idle_timeout: None },
                |_response| async { Ok(()) },
            )
            .await,
            RpcDeliveryOutcome::NotSent { .. }
        ));
    }
}
