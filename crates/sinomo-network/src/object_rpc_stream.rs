//! Stream-native SOBJ object RPC.
//!
//! One established duplex byte stream carries exactly one request direction
//! and one response direction. This module deliberately knows nothing about
//! route selection, relay topology, endpoint discovery, or service catalogs.
//! Peer identity belongs to the transport. Object RPC adds only bounded
//! request/response framing and length-based stream completion.

use crate::error::NetworkError;
use crate::object::{ObjectFrameIoOptions, SinomoObjectCodec};
use crate::owned_chunk::{
    AuthenticatedChunkReader, AuthenticatedChunkStream, AuthenticatedChunkWriter, BODY_CHUNK_MAX,
    ChunkAllocator, OwnedChunk, RPC_CONTROL_PAYLOAD_MAX, observe_chunk_identity,
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
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::sync::watch;

pub const OBJECT_RPC_STREAM_MAX_HEAD_BYTES: usize = RPC_CONTROL_PAYLOAD_MAX;
pub const OBJECT_RPC_STREAM_INLINE_BODY_BYTES: usize = 8 * 1024;
pub const OBJECT_RPC_STREAM_MAX_DATA_FRAME_BYTES: usize = BODY_CHUNK_MAX;
#[cfg(test)]
pub const OBJECT_RPC_STREAM_RECEIVE_WINDOW_BYTES: usize = 256 * 1024;

const OBJECT_RPC_STREAM_PROTOCOL: &str = "sinomo.object-rpc.stream.v2";
const OBJECT_RPC_STREAM_WIRE_MAGIC: [u8; 4] = *b"SOR2";
const OBJECT_RPC_STREAM_WIRE_PREFIX_BYTES: usize = 9;
const OBJECT_RPC_STREAM_MAX_TERMINAL_BYTES: usize = 16 * 1024;
const OBJECT_RPC_STREAM_MIN_PROGRESS_INTERVAL: Duration = Duration::from_millis(10);

const FRAME_REQUEST_HEAD: u8 = 1;
const FRAME_REQUEST_DATA: u8 = 2;
const FRAME_REQUEST_FINISH: u8 = 3;
const FRAME_RESPONSE_HEAD: u8 = 4;
const FRAME_RESPONSE_DATA: u8 = 5;
const FRAME_RESPONSE_FINISH: u8 = 6;
const FRAME_REQUEST_PROGRESS: u8 = 7;
const FRAME_RESPONSE_PROGRESS: u8 = 8;

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
}

/// A request or response body supplied through the same API whether its final
/// length is known at head time or supplied by the finish record.
pub struct ObjectRpcStreamBody {
    kind: OutgoingBodyKind,
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
            };
        }
        Self {
            kind: OutgoingBodyKind::Source(ObjectRpcBodySource::bytes(bytes)),
        }
    }

    pub fn source(source: ObjectRpcBodySource) -> Self {
        Self {
            kind: OutgoingBodyKind::Source(source),
        }
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
    /// Opaque, bounded service head carried with the RPC head.
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
    /// Time from the server record reader starting until the
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

/// Client-visible response head and incremental body.
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
    },
    /// A one-pass producer whose length is known before the stream begins.
    KnownLength {
        length: u64,
    },
    Deferred,
}

// Inline bytes are stored separately in the bounded head. Keeping the
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
    request_id: Id,
    method: String,
    operation_id: Option<String>,
    metadata: BTreeMap<String, String>,
    #[serde(with = "serde_bytes")]
    application_head: Vec<u8>,
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
    request_id: Id,
    disposition: ResponseDisposition,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct StreamProgressUnsigned {
    protocol: String,
    request_id: Id,
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
    length: u64,
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
    last_progress_nonce: u64,
    descriptor: BodyDescriptor,
    inline_body: Option<InlineBody>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectRpcBodyCompletion {
    pub length: u64,
}

#[derive(Clone, Debug)]
pub struct ObjectRpcStreamBodyCompletion {
    receiver: watch::Receiver<Option<Result<IncomingTerminal, NetworkError>>>,
}

impl ObjectRpcStreamBodyCompletion {
    pub async fn wait(mut self) -> Result<ObjectRpcBodyCompletion, NetworkError> {
        loop {
            if let Some(result) = self.receiver.borrow().clone() {
                return incoming_body_completion(&result);
            }
            self.receiver.changed().await.map_err(|_| {
                delivery_error(
                    "wait for object RPC stream body",
                    "body reader ended without a completion result",
                )
            })?;
        }
    }
}

/// Incremental incoming body. Natural backpressure is bounded to one receive
/// window. Dropping the reader makes the protocol pump discard further data
/// while it still validates the terminal length record.
#[derive(Debug)]
pub struct ObjectRpcStreamIncomingBody {
    body: ObjectRpcIncomingBody,
    completion: ObjectRpcStreamBodyCompletion,
}

impl ObjectRpcStreamIncomingBody {
    pub fn total_bytes(&self) -> Option<u64> {
        self.body.total_bytes()
    }

    /// Materializes the complete body without imposing a size
    /// policy. Allocation remains fallible, so a body larger than the process
    /// can represent or reserve returns `ResourceExhausted` instead of
    /// aborting the daemon.
    pub async fn materialize_to_vec(self) -> Result<Vec<u8>, NetworkError> {
        self.collect_to_vec(usize::MAX).await
    }

    pub async fn collect_to_vec(self, max_bytes: usize) -> Result<Vec<u8>, NetworkError> {
        let completion = self.completion;
        let output = self.body.collect_to_vec(max_bytes).await?;
        completion.wait().await?;
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

    fn progress_kind(self) -> u8 {
        match self {
            Self::Request => FRAME_REQUEST_PROGRESS,
            Self::Response => FRAME_RESPONSE_PROGRESS,
        }
    }
}

#[derive(Clone, Debug)]
enum IncomingTerminal {
    Completed(ObjectRpcBodyCompletion),
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

fn decode_control<T: serde::de::DeserializeOwned>(payload: &[u8]) -> Result<T, NetworkError> {
    SinomoObjectCodec::network_frame().decode(payload)
}

/// Encodes one bounded RPC control record. The reliable transport owns byte
/// delivery and any optional channel protection, so RPC does not add a tag.
fn encode_rpc_control<T: Serialize>(value: &T, max_bytes: usize) -> Result<Vec<u8>, NetworkError> {
    let payload = SinomoObjectCodec::network_frame().encode(value)?;
    if payload.len() > max_bytes {
        return Err(NetworkError::FrameTooLarge {
            actual_bytes: payload.len(),
            max_bytes,
        });
    }
    Ok(payload)
}

fn decode_rpc_control<T: serde::de::DeserializeOwned>(payload: &[u8]) -> Result<T, NetworkError> {
    decode_control(payload)
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
    request_id: &Id,
    timeline_trace: Option<crate::NetworkTimelineTraceContext>,
) -> Result<(RpcRecord, u64), NetworkError> {
    let started_ns = timeline_started_ns(timeline_trace);
    let result = read_response_head_with_progress_inner(reader, options, request_id).await;
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
    request_id: &Id,
) -> Result<(RpcRecord, u64), NetworkError> {
    let mut last_progress_nonce = 0;
    loop {
        let frame = reader.read_record(options).await?;
        if frame.kind != FRAME_RESPONSE_PROGRESS {
            return Ok((frame, last_progress_nonce));
        }
        validate_stream_progress(request_id, &mut last_progress_nonce, &frame.payload)?;
    }
}

async fn prepare_outgoing_body(
    body: ObjectRpcStreamBody,
) -> Result<PreparedOutgoingBody, NetworkError> {
    match body.kind {
        OutgoingBodyKind::Inline(bytes) => {
            let length = u64::try_from(bytes.len())
                .map_err(|_| serialization_error("inline body length does not fit u64"))?;
            Ok(PreparedOutgoingBody {
                descriptor: BodyDescriptor::Inline { length },
                inline_body: Some(InlineBody {
                    bytes: bytes.to_vec(),
                }),
                source: None,
            })
        }
        OutgoingBodyKind::Source(source) => {
            let descriptor = match source.total_bytes() {
                Some(length) => BodyDescriptor::KnownLength { length },
                None => BodyDescriptor::Deferred,
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
        (BodyDescriptor::Inline { length }, Some(InlineBody { bytes })) => {
            let actual_length = u64::try_from(bytes.len())
                .map_err(|_| serialization_error("inline body length does not fit u64"))?;
            if actual_length != *length {
                return Err(serialization_error(
                    "object RPC inline body length does not match its descriptor",
                ));
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
        BodyDescriptor::Inline { length } | BodyDescriptor::KnownLength { length } => Some(*length),
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

fn incoming_body_completion(
    result: &Result<IncomingTerminal, NetworkError>,
) -> Result<ObjectRpcBodyCompletion, NetworkError> {
    match result {
        Ok(IncomingTerminal::Completed(completion)) => Ok(completion.clone()),
        Ok(IncomingTerminal::Rejected(detail)) => Err(serialization_error(format!(
            "object RPC body rejected: {detail}"
        ))),
        Err(error) => Err(error.clone()),
    }
}

struct PullIncomingBodyState<R> {
    reader: R,
    options: ObjectFrameIoOptions,
    direction: IncomingDirection,
    request_id: Id,
    last_progress_nonce: u64,
    descriptor: BodyDescriptor,
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
                    &self.request_id,
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
                if let BodyDescriptor::KnownLength { length } = self.descriptor
                    && self.observed_length == length
                {
                    self.publish(Ok(IncomingTerminal::Completed(ObjectRpcBodyCompletion {
                        length: self.observed_length,
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
            let finish: BodyFinishUnsigned = decode_rpc_control(&payload)?;
            validate_incoming_finish(
                &self.request_id,
                self.observed_length,
                &self.descriptor,
                &finish,
            )?;
            let completion = ObjectRpcBodyCompletion {
                length: self.observed_length,
            };
            match finish.status {
                FinishStatus::Completed => {
                    self.publish(Ok(IncomingTerminal::Completed(completion)));
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
    spec: IncomingBodySpec,
    inline_completes_without_finish: bool,
) -> (
    ObjectRpcStreamIncomingBody,
    ObjectRpcStreamBodyCompletion,
    IncomingBodyCompletion,
)
where
    R: RpcRecordReader,
{
    let total_bytes = descriptor_total_bytes(&spec.descriptor);
    let (terminal_sender, terminal_receiver) = watch::channel(None);
    let body_completion = ObjectRpcStreamBodyCompletion {
        receiver: terminal_receiver.clone(),
    };
    let receive_completion = IncomingBodyCompletion {
        receiver: terminal_receiver,
    };
    let descriptor_validation = validate_descriptor_inline(&spec.descriptor, &spec.inline_body);
    let incoming_body = match (descriptor_validation, spec.descriptor, spec.inline_body) {
        (Ok(()), BodyDescriptor::Inline { length }, Some(InlineBody { bytes }))
            if inline_completes_without_finish =>
        {
            terminal_sender.send_replace(Some(Ok(IncomingTerminal::Completed(
                ObjectRpcBodyCompletion { length },
            ))));
            ObjectRpcIncomingBody::bytes(bytes)
        }
        (Ok(()), descriptor @ BodyDescriptor::Inline { .. }, Some(InlineBody { bytes })) => {
            let observed_length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            let pending_chunk =
                (!bytes.is_empty()).then(|| ObjectRpcBodyChunk::bytes(Bytes::from(bytes)));
            ObjectRpcIncomingBody::receiver(
                total_bytes,
                Box::new(PullIncomingBodyReceiver {
                    state: Some(PullIncomingBodyState {
                        reader,
                        options,
                        direction: spec.direction,
                        request_id: spec.request_id,
                        last_progress_nonce: spec.last_progress_nonce,
                        descriptor,
                        observed_length,
                        pending_chunk,
                        terminal_sender,
                        terminal_published: false,
                    }),
                }),
            )
        }
        (Ok(()), BodyDescriptor::KnownLength { length: 0 }, None) => {
            terminal_sender.send_replace(Some(Ok(IncomingTerminal::Completed(
                ObjectRpcBodyCompletion { length: 0 },
            ))));
            ObjectRpcIncomingBody::empty()
        }
        (Ok(()), descriptor, None) => ObjectRpcIncomingBody::receiver(
            total_bytes,
            Box::new(PullIncomingBodyReceiver {
                state: Some(PullIncomingBodyState {
                    reader,
                    options,
                    direction: spec.direction,
                    request_id: spec.request_id,
                    last_progress_nonce: spec.last_progress_nonce,
                    descriptor,
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
        completion: body_completion.clone(),
    };
    (body, body_completion, receive_completion)
}

fn validate_incoming_finish(
    request_id: &Id,
    observed_length: u64,
    descriptor: &BodyDescriptor,
    finish: &BodyFinishUnsigned,
) -> Result<(), NetworkError> {
    if &finish.request_id != request_id || finish.length != observed_length {
        return Err(serialization_error(
            "object RPC finish does not match the active request or observed length",
        ));
    }
    match descriptor {
        BodyDescriptor::Inline { length } | BodyDescriptor::KnownLength { length } => {
            if *length != observed_length {
                return Err(serialization_error(
                    "object RPC body length does not match its descriptor",
                ));
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
    direction: IncomingDirection,
    request_id: &Id,
    progress_interval: Option<Duration>,
    progress_nonce: &mut u64,
    data_kind: u8,
    body: &mut PreparedOutgoingBody,
) -> Result<ObjectRpcBodyCompletion, SendFailure> {
    let mut length = 0_u64;
    if let Some(inline) = body.inline_body.as_ref() {
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
                direction,
                request_id,
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
            let reached_declared_end = matches!(
                body.descriptor,
                BodyDescriptor::KnownLength {
                    length: expected_length
                } if length == expected_length
            );
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
            if reached_declared_end {
                break;
            }
        }
        let mut finish_producer = std::pin::pin!(producer.finish());
        await_with_stream_progress(
            writer,
            direction,
            request_id,
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
    match body.descriptor {
        BodyDescriptor::Inline {
            length: expected_length,
        }
        | BodyDescriptor::KnownLength {
            length: expected_length,
        } => {
            if length != expected_length {
                return Err(SendFailure {
                    error: serialization_error(
                        "object RPC body length changed after its descriptor was prepared",
                    ),
                    wrote_any: length != 0,
                });
            }
        }
        BodyDescriptor::Deferred => {}
    }
    Ok(ObjectRpcBodyCompletion { length })
}

fn make_finish(
    request_id: Id,
    completion: ObjectRpcBodyCompletion,
    status: FinishStatus,
) -> BodyFinishUnsigned {
    BodyFinishUnsigned {
        request_id,
        length: completion.length,
        status,
    }
}

async fn write_finish<W: RpcRecordWriter>(
    writer: &mut W,
    kind: u8,
    finish: &BodyFinishUnsigned,
) -> Result<(), SendFailure> {
    let payload =
        encode_rpc_control(finish, OBJECT_RPC_STREAM_MAX_TERMINAL_BYTES).map_err(|error| {
            SendFailure {
                error,
                wrote_any: false,
            }
        })?;
    writer
        .write_record(kind, &payload, "write object RPC body finish")
        .await
}

fn request_head(
    request_id: Id,
    method: String,
    operation_id: Option<String>,
    metadata: BTreeMap<String, String>,
    application_head: Vec<u8>,
    options: ObjectFrameIoOptions,
    body: &PreparedOutgoingBody,
) -> Result<(RequestHeadUnsigned, Vec<u8>), NetworkError> {
    let unsigned = RequestHeadUnsigned {
        protocol: OBJECT_RPC_STREAM_PROTOCOL.to_string(),
        request_id,
        method,
        operation_id,
        metadata,
        application_head,
        response_progress_interval_millis: advertised_progress_interval_millis(options),
        descriptor: body.descriptor,
        inline_body: body.inline_body.clone(),
    };
    let payload = encode_rpc_control(&unsigned, OBJECT_RPC_STREAM_MAX_HEAD_BYTES)?;
    Ok((unsigned, payload))
}

fn validate_request_head(unsigned: &RequestHeadUnsigned) -> Result<(), NetworkError> {
    if unsigned.protocol != OBJECT_RPC_STREAM_PROTOCOL || unsigned.method.trim().is_empty() {
        return Err(serialization_error("invalid object RPC request head"));
    }
    validate_descriptor_inline(&unsigned.descriptor, &unsigned.inline_body)?;
    Ok(())
}

fn response_head(
    request_id: Id,
    disposition: ResponseDisposition,
) -> Result<(ResponseHeadUnsigned, Vec<u8>), NetworkError> {
    let unsigned = ResponseHeadUnsigned {
        protocol: OBJECT_RPC_STREAM_PROTOCOL.to_string(),
        request_id,
        disposition,
    };
    let payload = encode_rpc_control(&unsigned, OBJECT_RPC_STREAM_MAX_HEAD_BYTES)?;
    Ok((unsigned, payload))
}

fn validate_response_head(
    request_id: &Id,
    unsigned: &ResponseHeadUnsigned,
) -> Result<(), NetworkError> {
    if unsigned.protocol != OBJECT_RPC_STREAM_PROTOCOL || &unsigned.request_id != request_id {
        return Err(serialization_error("invalid object RPC response head"));
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

fn stream_progress(request_id: Id, nonce: u64) -> Result<Vec<u8>, NetworkError> {
    let unsigned = StreamProgressUnsigned {
        protocol: OBJECT_RPC_STREAM_PROTOCOL.to_string(),
        request_id,
        nonce,
    };
    encode_rpc_control(&unsigned, OBJECT_RPC_STREAM_MAX_TERMINAL_BYTES)
}

fn validate_stream_progress(
    request_id: &Id,
    last_nonce: &mut u64,
    payload: &[u8],
) -> Result<(), NetworkError> {
    let unsigned: StreamProgressUnsigned = decode_rpc_control(payload)?;
    if unsigned.protocol != OBJECT_RPC_STREAM_PROTOCOL
        || &unsigned.request_id != request_id
        || unsigned.nonce == 0
        || unsigned.nonce <= *last_nonce
    {
        return Err(serialization_error("invalid object RPC progress record"));
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
    direction: IncomingDirection,
    request_id: &Id,
    nonce: u64,
) -> Result<(), SendFailure> {
    let payload = stream_progress(request_id.clone(), nonce).map_err(|error| SendFailure {
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
    direction: IncomingDirection,
    request_id: &Id,
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
                    direction,
                    request_id,
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
    request_id: Id,
    head_payload: Vec<u8>,
    mut body: PreparedOutgoingBody,
    options: ObjectFrameIoOptions,
    timeline_trace: Option<crate::NetworkTimelineTraceContext>,
) -> Result<(), SendFailure> {
    let started_ns = timeline_started_ns(timeline_trace);
    let result = async {
        let result =
            write_request_frames(&mut writer, request_id, &head_payload, &mut body, options).await;
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
    request_id: Id,
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
    let completion = write_prepared_body_data(
        writer,
        IncomingDirection::Request,
        &request_id,
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
    if matches!(body.descriptor, BodyDescriptor::KnownLength { .. }) {
        return Ok(());
    }
    let finish = make_finish(request_id, completion, FinishStatus::Completed);
    write_finish(writer, FRAME_REQUEST_FINISH, &finish)
        .await
        .map_err(|failure| SendFailure {
            error: failure.error,
            wrote_any: true,
        })?;
    Ok(())
}

/// Executes one call over one peer-bound duplex stream.
///
/// A complete consumer keeps the delivery outcome pending until the
/// response terminal length arrives. A streaming consumer transfers the
/// incremental body to its caller immediately; the body retains the completion,
/// and any terminal framing or transport failure is delivered by its
/// read path. This function never reconnects, reroutes, or automatically
/// resends a possibly applied call.
async fn call_object_rpc_records_with_consumption<R, W, F, Fut, T>(
    mut reader: R,
    writer: W,
    _context: &RpcPeerContext,
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
                request_id.clone(),
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
                &request_id,
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
                request_id.clone(),
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
                            &request_id,
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
                    &request_id,
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
    let response_head: ResponseHeadUnsigned = match decode_rpc_control(&response_frame.payload) {
        Ok(head) => head,
        Err(error) => {
            if let Some(task) = &send_task {
                task.abort();
            }
            return RpcDeliveryOutcome::PossiblyApplied { error };
        }
    };
    if let Err(error) = validate_response_head(&request_id, &response_head) {
        if let Some(task) = &send_task {
            task.abort();
        }
        return RpcDeliveryOutcome::PossiblyApplied { error };
    }
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
    let (body, _body_completion, receive_completion) = make_incoming_body(
        reader,
        options,
        IncomingBodySpec {
            direction: IncomingDirection::Response,
            request_id: request_id.clone(),
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
            // The returned application body now owns the record
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
    request_id: Id,
    detail: String,
) -> Result<(), NetworkError> {
    let (_, payload) = response_head(request_id, ResponseDisposition::Rejected { detail })?;
    writer
        .write_record(FRAME_RESPONSE_HEAD, &payload, "write object RPC rejection")
        .await
        .map_err(|failure| failure.error)?;
    writer
        .finish_records()
        .await
        .map_err(|failure| failure.error)
}

/// Serves exactly one call from one peer-bound duplex stream. The handler
/// starts after the bounded request head is decoded, while the body pump
/// continues independently. Response data may therefore start before the full
/// request is consumed, but a successful response finish is withheld until
/// request framing reaches a valid terminal length.
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
    let head: RequestHeadUnsigned = decode_rpc_control(&head_payload)?;
    validate_request_head(&head)?;
    let request_is_inline = matches!(head.descriptor, BodyDescriptor::Inline { .. });
    let request_id = head.request_id.clone();
    let negotiated_response_progress_interval =
        response_progress_interval(options, head.response_progress_interval_millis);
    let (body, request_body_completion, request_receive_completion) = make_incoming_body(
        reader,
        options,
        IncomingBodySpec {
            direction: IncomingDirection::Request,
            request_id: request_id.clone(),
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
            IncomingDirection::Response,
            &request_id,
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
                request_id,
                "object RPC handler response used the wrong request id".to_string(),
            )
            .await;
        }
        Err(error) => {
            return write_rejection(&mut writer, request_id, error.to_string()).await;
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
        IncomingDirection::Response,
        &request_id,
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
            return write_rejection(&mut writer, request_id, error.to_string()).await;
        }
    };
    let response_completes_at_known_boundary =
        matches!(response_body.descriptor, BodyDescriptor::KnownLength { .. });
    if response_completes_at_known_boundary {
        // A known response needs no terminal record, so establish the request
        // body's terminal outcome before publishing the accepted response
        // head. Unknown/length-only responses keep the terminal status record
        // and may still begin while a long request is being consumed.
        let mut wait_request = std::pin::pin!(request_body_completion.clone().wait());
        let request_result = await_with_stream_progress(
            &mut writer,
            IncomingDirection::Response,
            &request_id,
            negotiated_response_progress_interval,
            &mut response_progress_nonce,
            wait_request.as_mut(),
        )
        .await
        .map_err(|failure| failure.error)?;
        if let Err(error) = request_result {
            return write_rejection(
                &mut writer,
                request_id,
                format!("request body completion failed: {error}"),
            )
            .await;
        }
    }
    let buffer_entire_response = request_is_inline && response_body.source.is_none();
    let (_, response_payload) = response_head(
        request_id.clone(),
        ResponseDisposition::Accepted {
            metadata: reply.metadata,
            application_head: reply.application_head,
            descriptor: response_body.descriptor,
            inline_body: response_body.inline_body.clone(),
        },
    )?;
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
        let result = match request_receive_completion.join().await {
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
    let response_completion = write_prepared_body_data(
        &mut writer,
        IncomingDirection::Response,
        &request_id,
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
        let result = match request_receive_completion.join().await {
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
    let mut wait_request = std::pin::pin!(request_body_completion.wait());
    let request_result = await_with_stream_progress(
        &mut writer,
        IncomingDirection::Response,
        &request_id,
        negotiated_response_progress_interval,
        &mut response_progress_nonce,
        wait_request.as_mut(),
    )
    .await
    .map_err(|failure| failure.error)?;
    let finish_status = match &request_result {
        Ok(_) => FinishStatus::Completed,
        Err(error) => FinishStatus::Rejected {
            detail: format!("request body completion failed: {error}"),
        },
    };
    let finish = make_finish(request_id, response_completion, finish_status);
    write_finish(&mut writer, FRAME_RESPONSE_FINISH, &finish)
        .await
        .map_err(|failure| failure.error)?;
    writer
        .finish_records()
        .await
        .map_err(|failure| failure.error)?;
    let result = match request_receive_completion.join().await {
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
        let (_completion_sender, completion_receiver) = watch::channel(None);
        let stream_body = ObjectRpcStreamIncomingBody {
            body: incoming,
            completion: ObjectRpcStreamBodyCompletion {
                receiver: completion_receiver,
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
    async fn known_length_body_is_pulled_by_the_application_without_a_receive_task() {
        let expected = Bytes::from_static(b"already buffered typed body");
        let request_id = Id::from_legacy_named("request_buffered_known_body").expect("request id");
        let reader = ReadyRecordReader {
            records: VecDeque::from([RpcRecord {
                kind: FRAME_REQUEST_DATA,
                payload: ObjectRpcBodyChunk::bytes(expected.clone()),
            }]),
        };

        let (body, body_completion, receive_completion) = make_incoming_body(
            reader,
            ObjectFrameIoOptions::default(),
            IncomingBodySpec {
                direction: IncomingDirection::Request,
                request_id,
                last_progress_nonce: 0,
                descriptor: BodyDescriptor::KnownLength {
                    length: expected.len() as u64,
                },
                inline_body: None,
            },
            false,
        )
        .await;

        assert!(
            !receive_completion.is_ready(),
            "constructing the body must not run or spawn its record reader"
        );
        assert_eq!(
            body.materialize_to_vec().await.expect("materialize body"),
            expected
        );
        assert!(
            receive_completion.is_ready(),
            "the application pull must publish terminal completion"
        );
        assert_eq!(
            body_completion
                .wait()
                .await
                .expect("wait for buffered body"),
            ObjectRpcBodyCompletion {
                length: expected.len() as u64,
            }
        );
        assert!(matches!(
            receive_completion
                .join()
                .await
                .expect("complete pulled body"),
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
    fn rpc_control_serializes_once_without_a_transport_redundant_tag() {
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

        let unsigned = StreamProgressUnsigned {
            protocol: OBJECT_RPC_STREAM_PROTOCOL.to_string(),
            request_id: Id::from_legacy_named("request_control").expect("request id"),
            nonce: 1,
        };
        let serializations = AtomicUsize::new(0);
        let payload = encode_rpc_control(
            &Counted {
                value: &unsigned,
                serializations: &serializations,
            },
            OBJECT_RPC_STREAM_MAX_TERMINAL_BYTES,
        )
        .expect("RPC control");

        assert_eq!(serializations.load(Ordering::Relaxed), 1);
        let canonical_unsigned = SinomoObjectCodec::network_frame()
            .encode(&unsigned)
            .expect("canonical unsigned control");
        assert_eq!(payload, canonical_unsigned);
        let decoded: StreamProgressUnsigned =
            decode_rpc_control(&payload).expect("decoded control");
        assert_eq!(decoded, unsigned);
    }

    #[tokio::test]
    async fn unlimited_materialization_reports_unrepresentable_allocation() {
        let (_sender, incoming) =
            ObjectRpcIncomingBody::multiplexed_chunk_ring_with_depth(Some(u64::MAX), 1);
        let (_completion_sender, completion_receiver) = watch::channel(None);
        let body = ObjectRpcStreamIncomingBody {
            body: incoming,
            completion: ObjectRpcStreamBodyCompletion {
                receiver: completion_receiver,
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
    async fn handler_rejection_is_not_ambiguous_delivery() {
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
    async fn progress_preserves_a_healthy_long_running_handler() {
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
    fn progress_rejects_a_replayed_nonce() {
        let request_id = Id::generate();
        let payload = stream_progress(request_id.clone(), 1).expect("progress frame");
        let mut last_nonce = 0;

        validate_stream_progress(&request_id, &mut last_nonce, &payload)
            .expect("first progress frame");
        assert_eq!(last_nonce, 1);
        assert_eq!(
            validate_stream_progress(&request_id, &mut last_nonce, &payload,),
            Err(serialization_error("invalid object RPC progress record"))
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
    async fn progress_covers_slow_one_pass_body_producers() {
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
    async fn progress_keeps_an_early_response_alive_until_request_completion() {
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
    async fn known_length_mismatch_is_rejected_at_completion() {
        let bytes = body_bytes(OBJECT_RPC_STREAM_INLINE_BODY_BYTES + 1, 9);
        let body = ObjectRpcStreamBody::one_pass_reader(
            io::Cursor::new(bytes.clone()),
            Some(bytes.len() as u64 + 1),
        );
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
    async fn rpc_does_not_reauthenticate_transport_context() {
        let (client, server) = tokio::io::duplex(4096);
        let (client_context, mut server_context) = peer_contexts();
        server_context.channel_binding = [0x77; 32];
        let server_task = tokio::spawn(async move {
            serve_object_rpc_stream(
                server,
                &server_context,
                ObjectFrameIoOptions { idle_timeout: None },
                |call, _| async move {
                    Ok(ObjectRpcStreamReply::new(
                        call.request_id,
                        ObjectRpcStreamBody::bytes(b"ok".to_vec()),
                    ))
                },
            )
            .await
        });
        let outcome = call_object_rpc_stream(
            client,
            &client_context,
            ObjectRpcStreamCall::new("test.binding", ObjectRpcStreamBody::bytes(Vec::new())),
            ObjectFrameIoOptions { idle_timeout: None },
            |response| async move {
                assert_eq!(response.body.collect_to_vec(2).await?, b"ok");
                Ok(())
            },
        )
        .await;
        assert!(matches!(outcome, RpcDeliveryOutcome::Completed(())));
        server_task.await.expect("server task").expect("serve RPC");
    }

    #[test]
    fn deferred_finish_validates_request_and_observed_length() {
        let request_id = Id::from_legacy_named("request_finish_length").expect("request id");
        let finish = make_finish(
            request_id.clone(),
            ObjectRpcBodyCompletion { length: 3 },
            FinishStatus::Completed,
        );
        assert!(
            validate_incoming_finish(&request_id, 3, &BodyDescriptor::Deferred, &finish,).is_ok()
        );
        assert!(
            validate_incoming_finish(&request_id, 4, &BodyDescriptor::Deferred, &finish,).is_err()
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
