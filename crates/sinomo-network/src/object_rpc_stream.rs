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
use crate::session::ObjectRpcBodySource;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sinomo_base::Id;
use sinomo_node::NodeId;
use std::collections::BTreeMap;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::sync::{mpsc, watch};

pub const OBJECT_RPC_STREAM_MAX_HEAD_BYTES: usize = 256 * 1024;
pub const OBJECT_RPC_STREAM_INLINE_BODY_BYTES: usize = 8 * 1024;
pub const OBJECT_RPC_STREAM_MAX_DATA_FRAME_BYTES: usize = 64 * 1024;
pub const OBJECT_RPC_STREAM_RECEIVE_WINDOW_BYTES: usize = 256 * 1024;

const OBJECT_RPC_STREAM_PROTOCOL: &str = "sinomo.object-rpc.stream.v1";
const OBJECT_RPC_STREAM_WIRE_MAGIC: [u8; 4] = *b"SOR1";
const OBJECT_RPC_STREAM_WIRE_PREFIX_BYTES: usize = 9;
const OBJECT_RPC_STREAM_MAX_TERMINAL_BYTES: usize = 16 * 1024;
const OBJECT_RPC_STREAM_BODY_CHANNEL_DEPTH: usize =
    OBJECT_RPC_STREAM_RECEIVE_WINDOW_BYTES / OBJECT_RPC_STREAM_MAX_DATA_FRAME_BYTES;
const OBJECT_RPC_STREAM_MAX_HEAD_CLOCK_SKEW_MILLIS: u64 = 5 * 60 * 1000;

const FRAME_REQUEST_HEAD: u8 = 1;
const FRAME_REQUEST_DATA: u8 = 2;
const FRAME_REQUEST_FINISH: u8 = 3;
const FRAME_RESPONSE_HEAD: u8 = 4;
const FRAME_RESPONSE_DATA: u8 = 5;
const FRAME_RESPONSE_FINISH: u8 = 6;

const AUTH_REQUEST_HEAD: &[u8] = b"sinomo-object-rpc-request-head-v1";
const AUTH_REQUEST_FINISH: &[u8] = b"sinomo-object-rpc-request-finish-v1";
const AUTH_RESPONSE_HEAD: &[u8] = b"sinomo-object-rpc-response-head-v1";
const AUTH_RESPONSE_FINISH: &[u8] = b"sinomo-object-rpc-response-finish-v1";

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
    pub fn known(source: ObjectRpcBodySource, digest: [u8; 32]) -> Result<Self, NetworkError> {
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

    pub fn one_pass_reader<R>(reader: R, total_bytes: Option<u64>) -> Self
    where
        R: io::Read + Send + 'static,
    {
        Self::source(ObjectRpcBodySource::from_reader(reader, total_bytes))
    }

    pub fn one_pass_async_reader<R>(reader: R, total_bytes: Option<u64>) -> Self
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
    pub body: ObjectRpcStreamBody,
}

impl ObjectRpcStreamCall {
    pub fn new(method: impl Into<String>, body: ObjectRpcStreamBody) -> Self {
        Self {
            request_id: Id::generate(),
            method: method.into(),
            operation_id: None,
            metadata: BTreeMap::new(),
            body,
        }
    }

    pub fn encode_body<T: Serialize + ?Sized>(
        method: impl Into<String>,
        operation_id: Option<String>,
        body: &T,
    ) -> Result<Self, NetworkError> {
        Ok(Self {
            request_id: Id::generate(),
            method: method.into(),
            operation_id,
            metadata: BTreeMap::new(),
            body: ObjectRpcStreamBody::bytes(SinomoObjectCodec::network_frame().encode(body)?),
        })
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
    pub body: ObjectRpcStreamIncomingBody,
}

impl ObjectRpcStreamIncomingCall {
    pub async fn decode_body<T: serde::de::DeserializeOwned>(
        self,
        max_bytes: usize,
    ) -> Result<T, NetworkError> {
        let bytes = self.body.collect_to_vec(max_bytes).await?;
        SinomoObjectCodec::network_frame().decode(&bytes)
    }
}

/// Server-produced response head and streaming body source.
#[derive(Debug)]
pub struct ObjectRpcStreamReply {
    pub request_id: Id,
    pub metadata: BTreeMap<String, String>,
    pub body: ObjectRpcStreamBody,
}

impl ObjectRpcStreamReply {
    pub fn new(request_id: Id, body: ObjectRpcStreamBody) -> Self {
        Self {
            request_id,
            metadata: BTreeMap::new(),
            body,
        }
    }

    pub fn encode_body<T: Serialize + ?Sized>(
        request_id: Id,
        body: &T,
    ) -> Result<Self, NetworkError> {
        Ok(Self::new(
            request_id,
            ObjectRpcStreamBody::bytes(SinomoObjectCodec::network_frame().encode(body)?),
        ))
    }
}

/// Client-visible authenticated response head and incremental body.
#[derive(Debug)]
pub struct ObjectRpcStreamResponse {
    pub request_id: Id,
    pub metadata: BTreeMap<String, String>,
    pub body: ObjectRpcStreamIncomingBody,
}

impl ObjectRpcStreamResponse {
    pub async fn decode_body<T: serde::de::DeserializeOwned>(
        self,
        max_bytes: usize,
    ) -> Result<T, NetworkError> {
        let bytes = self.body.collect_to_vec(max_bytes).await?;
        SinomoObjectCodec::network_frame().decode(&bytes)
    }
}

#[derive(Debug)]
pub enum RpcDeliveryOutcome<T> {
    NotSent { error: NetworkError },
    Rejected { detail: String },
    PossiblyApplied { error: NetworkError },
    Completed(T),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum BodyDescriptor {
    Inline { length: u64, digest: [u8; 32] },
    Known { length: u64, digest: [u8; 32] },
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
    issued_at_unix_millis: u64,
    descriptor: BodyDescriptor,
    inline_body: Option<InlineBody>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct RequestHead {
    unsigned: RequestHeadUnsigned,
    authentication_tag: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum ResponseDisposition {
    Accepted {
        metadata: BTreeMap<String, String>,
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
struct ResponseHead {
    unsigned: ResponseHeadUnsigned,
    authentication_tag: [u8; 32],
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct BodyFinish {
    unsigned: BodyFinishUnsigned,
    authentication_tag: [u8; 32],
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
    receiver: watch::Receiver<Option<Result<ObjectRpcBodyVerification, NetworkError>>>,
}

impl ObjectRpcStreamBodyVerifier {
    pub async fn verify(mut self) -> Result<ObjectRpcBodyVerification, NetworkError> {
        loop {
            if let Some(result) = self.receiver.borrow().clone() {
                return result;
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
    receiver: mpsc::Receiver<Bytes>,
    current: Option<Bytes>,
    total_bytes: Option<u64>,
    verifier: ObjectRpcStreamBodyVerifier,
}

impl ObjectRpcStreamIncomingBody {
    pub const fn total_bytes(&self) -> Option<u64> {
        self.total_bytes
    }

    /// A handler that will commit an irreversible effect must await this proof
    /// after consuming any speculative body bytes.
    pub fn verifier(&self) -> ObjectRpcStreamBodyVerifier {
        self.verifier.clone()
    }

    pub async fn collect_to_vec(mut self, max_bytes: usize) -> Result<Vec<u8>, NetworkError> {
        if let Some(total_bytes) = self.total_bytes {
            let declared = usize::try_from(total_bytes).unwrap_or(usize::MAX);
            if declared > max_bytes {
                return Err(NetworkError::FrameTooLarge {
                    actual_bytes: declared,
                    max_bytes,
                });
            }
        }
        let mut output = Vec::new();
        let mut chunk = vec![0_u8; OBJECT_RPC_STREAM_MAX_DATA_FRAME_BYTES];
        loop {
            let read = self.read(&mut chunk).await.map_err(|error| {
                delivery_error("read object RPC stream body", error.to_string())
            })?;
            if read == 0 {
                break;
            }
            if output.len().saturating_add(read) > max_bytes {
                return Err(NetworkError::FrameTooLarge {
                    actual_bytes: output.len().saturating_add(read),
                    max_bytes,
                });
            }
            output.extend_from_slice(&chunk[..read]);
        }
        self.verifier.verify().await?;
        Ok(output)
    }
}

impl AsyncRead for ObjectRpcStreamIncomingBody {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            if let Some(mut current) = self.current.take() {
                let copy_bytes = current.len().min(buffer.remaining());
                buffer.put_slice(&current[..copy_bytes]);
                if copy_bytes < current.len() {
                    current = current.slice(copy_bytes..);
                    self.current = Some(current);
                }
                return Poll::Ready(Ok(()));
            }
            match self.receiver.poll_recv(context) {
                Poll::Ready(Some(bytes)) if bytes.is_empty() => continue,
                Poll::Ready(Some(bytes)) => self.current = Some(bytes),
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Pending => return Poll::Pending,
            }
        }
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
}

#[derive(Debug)]
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

fn now_unix_millis() -> Result<u64, NetworkError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            serialization_error(format!("system clock precedes Unix epoch: {error}"))
        })?;
    u64::try_from(duration.as_millis())
        .map_err(|_| serialization_error("current Unix time does not fit u64 milliseconds"))
}

fn encode_control<T: Serialize>(value: &T, max_bytes: usize) -> Result<Vec<u8>, NetworkError> {
    let payload = SinomoObjectCodec::network_frame().encode(value)?;
    if payload.len() > max_bytes {
        return Err(NetworkError::FrameTooLarge {
            actual_bytes: payload.len(),
            max_bytes,
        });
    }
    Ok(payload)
}

fn decode_control<T: serde::de::DeserializeOwned>(payload: &[u8]) -> Result<T, NetworkError> {
    SinomoObjectCodec::network_frame().decode(payload)
}

fn authenticated_tag<T: Serialize>(
    channel_binding: &[u8; 32],
    domain: &[u8],
    value: &T,
) -> Result<[u8; 32], NetworkError> {
    let encoded = SinomoObjectCodec::network_frame().encode(value)?;
    let mut transcript = Vec::with_capacity(domain.len().saturating_add(encoded.len()));
    transcript.extend_from_slice(domain);
    transcript.extend_from_slice(&encoded);
    Ok(*blake3::keyed_hash(channel_binding, &transcript).as_bytes())
}

fn authenticate<T: Serialize>(
    channel_binding: &[u8; 32],
    domain: &[u8],
    value: &T,
    actual: &[u8; 32],
) -> Result<(), NetworkError> {
    let expected = authenticated_tag(channel_binding, domain, value)?;
    if expected != *actual {
        return Err(NetworkError::AuthDenied);
    }
    Ok(())
}

async fn write_all_counted<W: AsyncWrite + Unpin>(
    writer: &mut W,
    bytes: &[u8],
    operation: &str,
) -> Result<(), SendFailure> {
    let mut written = 0_usize;
    while written < bytes.len() {
        match writer.write(&bytes[written..]).await {
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
    write_all_counted(writer, &prefix, operation).await?;
    write_all_counted(writer, payload, operation)
        .await
        .map_err(|failure| SendFailure {
            error: failure.error,
            wrote_any: true,
        })
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
        FRAME_REQUEST_FINISH | FRAME_RESPONSE_FINISH => Ok(OBJECT_RPC_STREAM_MAX_TERMINAL_BYTES),
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
            } else if let Some(replay) = source.try_clone_replayable() {
                Some(hash_source(replay).await?)
            } else {
                None
            };
            let descriptor = match (total_bytes, digest) {
                (Some(length), Some(digest)) => BodyDescriptor::Known { length, digest },
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
        BodyDescriptor::Inline { length, .. } | BodyDescriptor::Known { length, .. } => {
            Some(*length)
        }
        BodyDescriptor::Deferred => None,
    }
}

fn make_incoming_body<R>(
    reader: R,
    options: ObjectFrameIoOptions,
    context: RpcPeerContext,
    spec: IncomingBodySpec,
) -> (
    ObjectRpcStreamIncomingBody,
    ObjectRpcStreamBodyVerifier,
    tokio::task::JoinHandle<Result<IncomingTerminal, NetworkError>>,
)
where
    R: AsyncRead + Unpin + Send + 'static,
{
    let total_bytes = descriptor_total_bytes(&spec.descriptor);
    let (sender, receiver) = mpsc::channel(OBJECT_RPC_STREAM_BODY_CHANNEL_DEPTH.max(1));
    let (verification_sender, verification_receiver) = watch::channel(None);
    let verifier = ObjectRpcStreamBodyVerifier {
        receiver: verification_receiver,
    };
    let body = ObjectRpcStreamIncomingBody {
        receiver,
        current: None,
        total_bytes,
        verifier: verifier.clone(),
    };
    let task_verifier = verifier.clone();
    let task = tokio::spawn(async move {
        let result = pump_incoming_body(
            reader,
            options,
            &context,
            spec.direction,
            &spec.request_id,
            spec.head_digest,
            spec.descriptor,
            spec.inline_body,
            sender,
        )
        .await;
        let verification = match &result {
            Ok(IncomingTerminal::Completed(verification)) => Ok(verification.clone()),
            Ok(IncomingTerminal::Rejected(detail)) => Err(serialization_error(format!(
                "object RPC body rejected: {detail}"
            ))),
            Err(error) => Err(error.clone()),
        };
        verification_sender.send_replace(Some(verification));
        result
    });
    (body, task_verifier, task)
}

#[allow(clippy::too_many_arguments)]
async fn pump_incoming_body<R: AsyncRead + Unpin>(
    mut reader: R,
    options: ObjectFrameIoOptions,
    context: &RpcPeerContext,
    direction: IncomingDirection,
    request_id: &Id,
    head_digest: [u8; 32],
    descriptor: BodyDescriptor,
    inline_body: Option<InlineBody>,
    sender: mpsc::Sender<Bytes>,
) -> Result<IncomingTerminal, NetworkError> {
    validate_descriptor_inline(&descriptor, &inline_body)?;
    let mut sender = Some(sender);
    let mut hasher = blake3::Hasher::new();
    let mut observed_length = 0_u64;
    if let Some(inline) = inline_body {
        observed_length = u64::try_from(inline.bytes.len())
            .map_err(|_| serialization_error("inline body length does not fit u64"))?;
        hasher.update(&inline.bytes);
        if !inline.bytes.is_empty()
            && let Some(active_sender) = sender.as_ref()
            && active_sender.send(Bytes::from(inline.bytes)).await.is_err()
        {
            sender = None;
        }
    }
    loop {
        let (kind, payload) = read_wire_frame(&mut reader, options).await?;
        if kind == direction.data_kind() {
            if matches!(descriptor, BodyDescriptor::Inline { .. }) {
                return Err(serialization_error(
                    "inline object RPC body was followed by a data frame",
                ));
            }
            observed_length = observed_length
                .checked_add(payload.len() as u64)
                .ok_or_else(|| serialization_error("object RPC body length overflow"))?;
            if let Some(declared) = descriptor_total_bytes(&descriptor)
                && observed_length > declared
            {
                return Err(serialization_error(format!(
                    "object RPC body exceeded declared length {declared}"
                )));
            }
            hasher.update(&payload);
            if !payload.is_empty()
                && let Some(active_sender) = sender.as_ref()
                && active_sender.send(Bytes::from(payload)).await.is_err()
            {
                sender = None;
            }
            continue;
        }
        if kind != direction.finish_kind() {
            return Err(serialization_error(format!(
                "unexpected object RPC stream frame kind {kind} in {direction:?} body"
            )));
        }
        let finish: BodyFinish = decode_control(&payload)?;
        validate_incoming_finish(
            context,
            direction,
            request_id,
            head_digest,
            observed_length,
            &hasher.finalize(),
            &descriptor,
            &finish,
        )?;
        drop(sender);
        let verification = ObjectRpcBodyVerification {
            length: observed_length,
            digest: finish.unsigned.content_digest,
        };
        return Ok(match finish.unsigned.status {
            FinishStatus::Completed => IncomingTerminal::Completed(verification),
            FinishStatus::Rejected { detail } => IncomingTerminal::Rejected(detail),
        });
    }
}

#[allow(clippy::too_many_arguments)]
fn validate_incoming_finish(
    context: &RpcPeerContext,
    direction: IncomingDirection,
    request_id: &Id,
    head_digest: [u8; 32],
    observed_length: u64,
    observed_digest: &blake3::Hash,
    descriptor: &BodyDescriptor,
    finish: &BodyFinish,
) -> Result<(), NetworkError> {
    let (expected_origin, expected_destination) = context.expected_incoming_endpoints();
    let unsigned = &finish.unsigned;
    if &unsigned.request_id != request_id
        || unsigned.session_id != context.session_id
        || unsigned.channel_binding != context.channel_binding
        || unsigned.origin_node_id.as_ref() != expected_origin
        || unsigned.destination_node_id.as_ref() != expected_destination
        || unsigned.head_digest != head_digest
        || unsigned.length != observed_length
        || unsigned.content_digest.as_slice() != observed_digest.as_bytes()
    {
        return Err(NetworkError::AuthDenied);
    }
    authenticate(
        &context.channel_binding,
        direction.finish_domain(),
        unsigned,
        &finish.authentication_tag,
    )?;
    match descriptor {
        BodyDescriptor::Inline { length, digest, .. }
        | BodyDescriptor::Known { length, digest } => {
            if *length != observed_length || digest.as_slice() != observed_digest.as_bytes() {
                return Err(NetworkError::AuthDenied);
            }
        }
        BodyDescriptor::Deferred => {}
    }
    Ok(())
}

async fn write_prepared_body_data<W: AsyncWrite + Unpin>(
    writer: &mut W,
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
        while let Some(chunk) = producer.next().await.map_err(|error| SendFailure {
            error,
            wrote_any: length != 0,
        })? {
            if chunk.len() > OBJECT_RPC_STREAM_MAX_DATA_FRAME_BYTES {
                return Err(SendFailure {
                    error: NetworkError::FrameTooLarge {
                        actual_bytes: chunk.len(),
                        max_bytes: OBJECT_RPC_STREAM_MAX_DATA_FRAME_BYTES,
                    },
                    wrote_any: length != 0,
                });
            }
            length = length
                .checked_add(chunk.len() as u64)
                .ok_or_else(|| SendFailure {
                    error: serialization_error("object RPC body length overflow"),
                    wrote_any: true,
                })?;
            hasher.update(&chunk);
            write_wire_frame(writer, data_kind, &chunk, "write object RPC body data").await?;
        }
        producer.finish().await.map_err(|error| SendFailure {
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
        BodyDescriptor::Deferred => {}
    }
    Ok(ObjectRpcBodyVerification { length, digest })
}

#[allow(clippy::too_many_arguments)]
fn make_finish(
    context: &RpcPeerContext,
    request_id: Id,
    head_digest: [u8; 32],
    verification: ObjectRpcBodyVerification,
    status: FinishStatus,
    domain: &[u8],
) -> Result<BodyFinish, NetworkError> {
    let (origin_node_id, destination_node_id) = context.wire_endpoints();
    let unsigned = BodyFinishUnsigned {
        request_id,
        session_id: context.session_id.clone(),
        channel_binding: context.channel_binding,
        origin_node_id,
        destination_node_id,
        head_digest,
        length: verification.length,
        content_digest: verification.digest,
        status,
    };
    let authentication_tag = authenticated_tag(&context.channel_binding, domain, &unsigned)?;
    Ok(BodyFinish {
        unsigned,
        authentication_tag,
    })
}

async fn write_finish<W: AsyncWrite + Unpin>(
    writer: &mut W,
    kind: u8,
    finish: &BodyFinish,
) -> Result<(), SendFailure> {
    let payload =
        encode_control(finish, OBJECT_RPC_STREAM_MAX_TERMINAL_BYTES).map_err(|error| {
            SendFailure {
                error,
                wrote_any: false,
            }
        })?;
    write_wire_frame(writer, kind, &payload, "write object RPC body finish").await
}

fn request_head(
    context: &RpcPeerContext,
    request_id: Id,
    method: String,
    operation_id: Option<String>,
    metadata: BTreeMap<String, String>,
    body: &PreparedOutgoingBody,
) -> Result<(RequestHead, Vec<u8>), NetworkError> {
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
        issued_at_unix_millis: now_unix_millis()?,
        descriptor: body.descriptor,
        inline_body: body.inline_body.clone(),
    };
    let authentication_tag =
        authenticated_tag(&context.channel_binding, AUTH_REQUEST_HEAD, &unsigned)?;
    let head = RequestHead {
        unsigned,
        authentication_tag,
    };
    let payload = encode_control(&head, OBJECT_RPC_STREAM_MAX_HEAD_BYTES)?;
    Ok((head, payload))
}

fn validate_request_head(context: &RpcPeerContext, head: &RequestHead) -> Result<(), NetworkError> {
    let unsigned = &head.unsigned;
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
    authenticate(
        &context.channel_binding,
        AUTH_REQUEST_HEAD,
        unsigned,
        &head.authentication_tag,
    )?;
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
) -> Result<(ResponseHead, Vec<u8>), NetworkError> {
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
    let authentication_tag =
        authenticated_tag(&context.channel_binding, AUTH_RESPONSE_HEAD, &unsigned)?;
    let head = ResponseHead {
        unsigned,
        authentication_tag,
    };
    let payload = encode_control(&head, OBJECT_RPC_STREAM_MAX_HEAD_BYTES)?;
    Ok((head, payload))
}

fn validate_response_head(
    context: &RpcPeerContext,
    request_id: &Id,
    request_head_digest: [u8; 32],
    head: &ResponseHead,
) -> Result<(), NetworkError> {
    let unsigned = &head.unsigned;
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
    authenticate(
        &context.channel_binding,
        AUTH_RESPONSE_HEAD,
        unsigned,
        &head.authentication_tag,
    )
}

async fn send_request<W: AsyncWrite + Unpin>(
    mut writer: W,
    context: RpcPeerContext,
    request_id: Id,
    request_head_digest: [u8; 32],
    head_payload: Vec<u8>,
    mut body: PreparedOutgoingBody,
) -> Result<(), SendFailure> {
    let result = async {
        write_wire_frame(
            &mut writer,
            FRAME_REQUEST_HEAD,
            &head_payload,
            "write object RPC request head",
        )
        .await?;
        let verification = write_prepared_body_data(&mut writer, FRAME_REQUEST_DATA, &mut body)
            .await
            .map_err(|failure| SendFailure {
                error: failure.error,
                wrote_any: true,
            })?;
        let finish = make_finish(
            &context,
            request_id,
            request_head_digest,
            verification,
            FinishStatus::Completed,
            AUTH_REQUEST_FINISH,
        )
        .map_err(|error| SendFailure {
            error,
            wrote_any: true,
        })?;
        write_finish(&mut writer, FRAME_REQUEST_FINISH, &finish)
            .await
            .map_err(|failure| SendFailure {
                error: failure.error,
                wrote_any: true,
            })?;
        writer.flush().await.map_err(|error| SendFailure {
            error: delivery_error("flush object RPC request", error.to_string()),
            wrote_any: true,
        })?;
        writer.shutdown().await.map_err(|error| SendFailure {
            error: delivery_error("finish object RPC request direction", error.to_string()),
            wrote_any: true,
        })
    }
    .await;
    if result.is_err() {
        let _ = writer.shutdown().await;
    }
    result
}

/// Executes one call over one authenticated duplex stream. The response
/// consumer receives an incremental body; the function returns `Completed`
/// only after the authenticated response finish record has verified. It never
/// reconnects, reroutes, or automatically resends a possibly applied call.
pub async fn call_object_rpc_stream<S, F, Fut, T>(
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
    if let Err(error) = call.validate() {
        return RpcDeliveryOutcome::NotSent { error };
    }
    let ObjectRpcStreamCall {
        request_id,
        method,
        operation_id,
        metadata,
        body,
    } = call;
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
        &body,
    ) {
        Ok(head) => head,
        Err(error) => return RpcDeliveryOutcome::NotSent { error },
    };
    let request_head_digest = *blake3::hash(&head_payload).as_bytes();
    let (mut reader, writer) = tokio::io::split(stream);
    let mut send_task = tokio::spawn(send_request(
        writer,
        context.clone(),
        request_id.clone(),
        request_head_digest,
        head_payload,
        body,
    ));

    let (response_frame, request_send_completed) = tokio::select! {
        send_result = &mut send_task => {
            match send_result {
                Ok(Ok(())) => match read_wire_frame(&mut reader, options).await {
                    Ok(frame) => (frame, true),
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
        response = read_wire_frame(&mut reader, options) => {
            match response {
                Ok(frame) => (frame, false),
                Err(error) => {
                    let send_result = send_task.await;
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
    if response_frame.0 != FRAME_RESPONSE_HEAD {
        send_task.abort();
        return RpcDeliveryOutcome::PossiblyApplied {
            error: serialization_error("expected object RPC response head"),
        };
    }
    let response_head: ResponseHead = match decode_control(&response_frame.1) {
        Ok(head) => head,
        Err(error) => {
            send_task.abort();
            return RpcDeliveryOutcome::PossiblyApplied { error };
        }
    };
    if let Err(error) =
        validate_response_head(context, &request_id, request_head_digest, &response_head)
    {
        send_task.abort();
        return RpcDeliveryOutcome::PossiblyApplied { error };
    }
    let response_head_digest = *blake3::hash(&response_frame.1).as_bytes();
    let (metadata, descriptor, inline_body) = match response_head.unsigned.disposition {
        ResponseDisposition::Accepted {
            metadata,
            descriptor,
            inline_body,
        } => (metadata, descriptor, inline_body),
        ResponseDisposition::Rejected { detail } => {
            send_task.abort();
            return RpcDeliveryOutcome::Rejected { detail };
        }
    };
    let (body, _verifier, receive_task) = make_incoming_body(
        reader,
        options,
        context.clone(),
        IncomingBodySpec {
            direction: IncomingDirection::Response,
            request_id: request_id.clone(),
            head_digest: response_head_digest,
            descriptor,
            inline_body,
        },
    );
    let consumer_result = response_consumer(ObjectRpcStreamResponse {
        request_id,
        metadata,
        body,
    })
    .await;
    let terminal = match receive_task.await {
        Ok(Ok(terminal)) => terminal,
        Ok(Err(error)) => return RpcDeliveryOutcome::PossiblyApplied { error },
        Err(error) => {
            return RpcDeliveryOutcome::PossiblyApplied {
                error: delivery_error("join object RPC response reader", error.to_string()),
            };
        }
    };
    if !request_send_completed {
        match send_task.await {
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

async fn write_rejection<W: AsyncWrite + Unpin>(
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
    write_wire_frame(
        writer,
        FRAME_RESPONSE_HEAD,
        &payload,
        "write object RPC rejection",
    )
    .await
    .map_err(|failure| failure.error)?;
    writer
        .shutdown()
        .await
        .map_err(|error| delivery_error("finish object RPC rejection", error.to_string()))
}

/// Serves exactly one call from one authenticated duplex stream. The handler
/// starts after the bounded request head is authenticated, while the body pump
/// continues independently. Response data may therefore start before the full
/// request is consumed, but a successful response finish is withheld until the
/// request's content proof verifies.
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
    let (mut reader, mut writer) = tokio::io::split(stream);
    let (kind, head_payload) = read_wire_frame(&mut reader, options).await?;
    if kind != FRAME_REQUEST_HEAD {
        return Err(serialization_error("expected object RPC request head"));
    }
    let head: RequestHead = decode_control(&head_payload)?;
    validate_request_head(context, &head)?;
    let request_head_digest = *blake3::hash(&head_payload).as_bytes();
    let request_id = head.unsigned.request_id.clone();
    let (body, request_verifier, request_task) = make_incoming_body(
        reader,
        options,
        context.clone(),
        IncomingBodySpec {
            direction: IncomingDirection::Request,
            request_id: request_id.clone(),
            head_digest: request_head_digest,
            descriptor: head.unsigned.descriptor,
            inline_body: head.unsigned.inline_body,
        },
    );
    let incoming = ObjectRpcStreamIncomingCall {
        request_id: request_id.clone(),
        method: head.unsigned.method,
        operation_id: head.unsigned.operation_id,
        metadata: head.unsigned.metadata,
        body,
    };
    let reply = match handler(incoming, context.clone()).await {
        Ok(reply) if reply.request_id == request_id => reply,
        Ok(_) => {
            request_task.abort();
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
            request_task.abort();
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
    let mut response_body = match prepare_outgoing_body(reply.body).await {
        Ok(body) => body,
        Err(error) => {
            request_task.abort();
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
    let (_, response_payload) = response_head(
        context,
        request_id.clone(),
        request_head_digest,
        ResponseDisposition::Accepted {
            metadata: reply.metadata,
            descriptor: response_body.descriptor,
            inline_body: response_body.inline_body.clone(),
        },
    )?;
    let response_head_digest = *blake3::hash(&response_payload).as_bytes();
    write_wire_frame(
        &mut writer,
        FRAME_RESPONSE_HEAD,
        &response_payload,
        "write object RPC response head",
    )
    .await
    .map_err(|failure| failure.error)?;
    let response_verification =
        write_prepared_body_data(&mut writer, FRAME_RESPONSE_DATA, &mut response_body)
            .await
            .map_err(|failure| failure.error)?;
    let request_result = request_verifier.verify().await;
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
        AUTH_RESPONSE_FINISH,
    )?;
    write_finish(&mut writer, FRAME_RESPONSE_FINISH, &finish)
        .await
        .map_err(|failure| failure.error)?;
    writer.shutdown().await.map_err(|error| {
        delivery_error("finish object RPC response direction", error.to_string())
    })?;
    match request_task.await {
        Ok(Ok(IncomingTerminal::Completed(_))) if request_result.is_ok() => Ok(()),
        Ok(Ok(IncomingTerminal::Rejected(detail))) => Err(serialization_error(format!(
            "request body rejected: {detail}"
        ))),
        Ok(Err(error)) => Err(error),
        Err(error) => Err(delivery_error(
            "join object RPC request reader",
            error.to_string(),
        )),
        _ => request_result.map(|_| ()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

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
            &body,
        )
        .expect("head");
        head.unsigned.issued_at_unix_millis = 0;
        head.authentication_tag = authenticated_tag(
            &client_context.channel_binding,
            AUTH_REQUEST_HEAD,
            &head.unsigned,
        )
        .expect("tag");
        assert_eq!(
            validate_request_head(&server_context, &head),
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
            AUTH_REQUEST_FINISH,
        )
        .expect("finish");
        let mut tampered = finish.clone();
        tampered.unsigned.destination_node_id = Some(node("node_wrong_destination"));
        assert_eq!(
            authenticate(
                &client_context.channel_binding,
                AUTH_REQUEST_FINISH,
                &tampered.unsigned,
                &tampered.authentication_tag,
            ),
            Err(NetworkError::AuthDenied)
        );
        let mut tampered = finish;
        tampered.unsigned.channel_binding = [0x99; 32];
        assert_eq!(
            authenticate(
                &client_context.channel_binding,
                AUTH_REQUEST_FINISH,
                &tampered.unsigned,
                &tampered.authentication_tag,
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
