//! Transport-neutral service-RPC bodies and application-facing call values.
//!
//! Wire framing, authentication, routing, and flow control live in the
//! stream-native object-RPC and routing modules.

use crate::{NetworkDeliveryState, NetworkError};
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::fmt;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt};

const BODY_READER_BUFFER_BYTES: usize =
    crate::object_rpc_stream::OBJECT_RPC_STREAM_MAX_DATA_FRAME_BYTES;
const BODY_READER_BUFFER_DEPTH: usize = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum ObjectRpcBodyRole {
    None,
    Value,
    Attachment,
}

#[derive(Clone, Debug)]
struct ReplayableBytes {
    bytes: Bytes,
}

#[derive(Clone, Debug)]
struct ReplayableRepeatedBytes {
    pattern: Bytes,
    total_bytes: u64,
}

#[derive(Clone, Debug)]
struct ReplayableFile {
    path: PathBuf,
    total_bytes: u64,
}

struct OnePassReader {
    reader: Option<Box<dyn Read + Send>>,
    total_bytes: Option<u64>,
}

struct OnePassAsyncReader {
    reader: Option<Pin<Box<dyn AsyncRead + Send>>>,
    total_bytes: Option<u64>,
}

#[derive(Debug)]
struct OnePassIncomingChunkStream {
    stream: Option<ObjectRpcIncomingBodyChunkStream>,
    total_bytes: Option<u64>,
}

impl fmt::Debug for OnePassAsyncReader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OnePassAsyncReader")
            .field("available", &self.reader.is_some())
            .field("total_bytes", &self.total_bytes)
            .finish()
    }
}

impl fmt::Debug for OnePassReader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OnePassReader")
            .field("available", &self.reader.is_some())
            .field("total_bytes", &self.total_bytes)
            .finish()
    }
}

#[derive(Debug)]
enum ObjectRpcBodySourceKind {
    Empty,
    Bytes(ReplayableBytes),
    RepeatedBytes(ReplayableRepeatedBytes),
    File(ReplayableFile),
    Reader(Mutex<OnePassReader>),
    AsyncReader(Mutex<OnePassAsyncReader>),
    IncomingChunkStream(Mutex<OnePassIncomingChunkStream>),
}

/// A whole RPC attachment source.
///
/// `Bytes` and `File` sources can be reopened for the bounded digest pre-pass
/// performed before this call writes its authenticated head. A one-pass reader
/// is consumed exactly once and reports an explicit reuse error instead of
/// silently buffering the complete input. Reconnect and replay are outside
/// this contract.
pub struct ObjectRpcBodySource {
    kind: ObjectRpcBodySourceKind,
    role: ObjectRpcBodyRole,
    lifetime_guards: Arc<Mutex<Vec<Box<dyn Any + Send>>>>,
}

impl fmt::Debug for ObjectRpcBodySource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ObjectRpcBodySource")
            .field("kind", &self.kind)
            .field("role", &self.role)
            .field(
                "lifetime_guard_count",
                &self
                    .lifetime_guards
                    .lock()
                    .map(|guards| guards.len())
                    .unwrap_or_default(),
            )
            .finish()
    }
}

impl Default for ObjectRpcBodySource {
    fn default() -> Self {
        Self::empty()
    }
}

impl ObjectRpcBodySource {
    pub fn empty() -> Self {
        Self {
            kind: ObjectRpcBodySourceKind::Empty,
            role: ObjectRpcBodyRole::None,
            lifetime_guards: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn bytes(bytes: impl Into<Vec<u8>>) -> Self {
        let bytes = bytes.into();
        if bytes.is_empty() {
            return Self::empty();
        }
        Self {
            kind: ObjectRpcBodySourceKind::Bytes(ReplayableBytes {
                bytes: Bytes::from(bytes),
            }),
            role: ObjectRpcBodyRole::Attachment,
            lifetime_guards: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Builds a replayable, bounded-memory zero-filled attachment source.
    ///
    /// Every producer shares one immutable transfer-sized backing buffer and
    /// emits zero-copy `Bytes` slices from it until `total_bytes` is reached.
    /// This is useful for generated payloads and benchmarks that must measure
    /// transport work rather than an `AsyncRead` fill loop.
    pub fn zeroes(total_bytes: u64) -> Self {
        if total_bytes == 0 {
            return Self::empty();
        }
        static ZERO_PATTERN: OnceLock<Bytes> = OnceLock::new();
        let pattern = ZERO_PATTERN
            .get_or_init(|| Bytes::from(vec![0_u8; BODY_READER_BUFFER_BYTES]))
            .clone();
        Self {
            kind: ObjectRpcBodySourceKind::RepeatedBytes(ReplayableRepeatedBytes {
                pattern,
                total_bytes,
            }),
            role: ObjectRpcBodyRole::Attachment,
            lifetime_guards: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn file(path: impl Into<PathBuf>) -> Result<Self, NetworkError> {
        let path = path.into();
        let metadata = std::fs::metadata(&path).map_err(|source| NetworkError::Io {
            operation: format!("inspect object RPC body file {}", path.display()),
            detail: source.to_string(),
        })?;
        if !metadata.is_file() {
            return Err(NetworkError::Io {
                operation: format!("inspect object RPC body file {}", path.display()),
                detail: "body source is not a regular file".to_string(),
            });
        }
        Ok(Self {
            kind: ObjectRpcBodySourceKind::File(ReplayableFile {
                path,
                total_bytes: metadata.len(),
            }),
            role: if metadata.len() == 0 {
                ObjectRpcBodyRole::None
            } else {
                ObjectRpcBodyRole::Attachment
            },
            lifetime_guards: Arc::new(Mutex::new(Vec::new())),
        })
    }

    pub fn reader(reader: Box<dyn Read + Send>, total_bytes: Option<u64>) -> Self {
        Self {
            kind: ObjectRpcBodySourceKind::Reader(Mutex::new(OnePassReader {
                reader: Some(reader),
                total_bytes,
            })),
            role: if total_bytes == Some(0) {
                ObjectRpcBodyRole::None
            } else {
                ObjectRpcBodyRole::Attachment
            },
            lifetime_guards: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn from_reader<R>(reader: R, total_bytes: Option<u64>) -> Self
    where
        R: Read + Send + 'static,
    {
        Self::reader(Box::new(reader), total_bytes)
    }

    pub fn async_reader<R>(reader: R, total_bytes: Option<u64>) -> Self
    where
        R: AsyncRead + Send + 'static,
    {
        Self {
            kind: ObjectRpcBodySourceKind::AsyncReader(Mutex::new(OnePassAsyncReader {
                reader: Some(Box::pin(reader)),
                total_bytes,
            })),
            role: if total_bytes == Some(0) {
                ObjectRpcBodyRole::None
            } else {
                ObjectRpcBodyRole::Attachment
            },
            lifetime_guards: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn from_incoming_body(body: ObjectRpcIncomingBody) -> Self {
        let total_bytes = body.total_bytes();
        Self::from_incoming_body_with_total(body, total_bytes)
    }

    pub fn from_incoming_body_with_total(
        body: ObjectRpcIncomingBody,
        total_bytes: Option<u64>,
    ) -> Self {
        let role = body.role;
        Self {
            kind: ObjectRpcBodySourceKind::AsyncReader(Mutex::new(OnePassAsyncReader {
                reader: Some(Box::pin(body)),
                total_bytes,
            })),
            role,
            lifetime_guards: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Opens the private ownership-moving adapter used by relay forwarding.
    ///
    /// Unlike [`Self::from_incoming_body`], this path does not expose the
    /// incoming body through `AsyncRead`: received `Bytes` chunks move into the
    /// outgoing producer and are split into zero-copy slices only as required
    /// by the downstream stream writer.
    pub(crate) fn from_incoming_chunk_stream(body: ObjectRpcIncomingBody) -> Self {
        let total_bytes = body.total_bytes();
        let role = body.role;
        Self {
            kind: ObjectRpcBodySourceKind::IncomingChunkStream(Mutex::new(
                OnePassIncomingChunkStream {
                    stream: Some(body.into_chunk_stream()),
                    total_bytes,
                },
            )),
            role,
            lifetime_guards: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn role(&self) -> ObjectRpcBodyRole {
        if self.is_empty() {
            ObjectRpcBodyRole::None
        } else {
            self.role
        }
    }

    fn with_role(mut self, role: ObjectRpcBodyRole) -> Self {
        self.role = if self.is_empty() {
            ObjectRpcBodyRole::None
        } else {
            role
        };
        self
    }

    fn value_intent() -> Self {
        let mut source = Self::empty();
        source.role = ObjectRpcBodyRole::Value;
        source
    }

    pub(crate) fn has_value_intent(&self) -> bool {
        self.role == ObjectRpcBodyRole::Value
    }

    pub fn is_empty(&self) -> bool {
        matches!(self.kind, ObjectRpcBodySourceKind::Empty)
            || matches!(&self.kind, ObjectRpcBodySourceKind::Bytes(source) if source.bytes.is_empty())
            || matches!(&self.kind, ObjectRpcBodySourceKind::RepeatedBytes(source)
                if source.total_bytes == 0)
            || matches!(&self.kind, ObjectRpcBodySourceKind::File(source) if source.total_bytes == 0)
            || matches!(&self.kind, ObjectRpcBodySourceKind::Reader(source)
                if source.lock().ok().and_then(|source| source.total_bytes) == Some(0))
            || matches!(&self.kind, ObjectRpcBodySourceKind::AsyncReader(source)
                if source.lock().ok().and_then(|source| source.total_bytes) == Some(0))
            || matches!(&self.kind, ObjectRpcBodySourceKind::IncomingChunkStream(source)
                if source.lock().ok().and_then(|source| source.total_bytes) == Some(0))
    }

    pub fn total_bytes(&self) -> Option<u64> {
        match &self.kind {
            ObjectRpcBodySourceKind::Empty => Some(0),
            ObjectRpcBodySourceKind::Bytes(source) => u64::try_from(source.bytes.len()).ok(),
            ObjectRpcBodySourceKind::RepeatedBytes(source) => Some(source.total_bytes),
            ObjectRpcBodySourceKind::File(source) => Some(source.total_bytes),
            ObjectRpcBodySourceKind::Reader(source) => {
                source.lock().ok().and_then(|source| source.total_bytes)
            }
            ObjectRpcBodySourceKind::AsyncReader(source) => {
                source.lock().ok().and_then(|source| source.total_bytes)
            }
            ObjectRpcBodySourceKind::IncomingChunkStream(source) => {
                source.lock().ok().and_then(|source| source.total_bytes)
            }
        }
    }

    /// Keeps an opaque route/concurrency/resource guard alive until this body
    /// source and every producer opened from it have finished or been dropped.
    pub fn hold_lifetime_guard<T>(&mut self, guard: T) -> Result<(), NetworkError>
    where
        T: Any + Send,
    {
        self.lifetime_guards
            .lock()
            .map_err(|_| NetworkError::Io {
                operation: "attach object RPC body lifetime guard".to_string(),
                detail: "body lifetime guard lock poisoned".to_string(),
            })?
            .push(Box::new(guard));
        Ok(())
    }

    pub fn with_lifetime_guard<T>(mut self, guard: T) -> Result<Self, NetworkError>
    where
        T: Any + Send,
    {
        self.hold_lifetime_guard(guard)?;
        Ok(self)
    }

    fn open(&self) -> Result<Box<dyn Read + Send>, NetworkError> {
        match &self.kind {
            ObjectRpcBodySourceKind::Empty => Ok(Box::new(std::io::empty())),
            ObjectRpcBodySourceKind::Bytes(source) => {
                Ok(Box::new(BytesReader::new(source.bytes.clone())))
            }
            ObjectRpcBodySourceKind::RepeatedBytes(source) => Ok(Box::new(
                RepeatedBytesReader::new(source.pattern.clone(), source.total_bytes),
            )),
            ObjectRpcBodySourceKind::File(source) => {
                let file = File::open(&source.path).map_err(|error| NetworkError::Io {
                    operation: format!("open object RPC body file {}", source.path.display()),
                    detail: error.to_string(),
                })?;
                Ok(Box::new(file))
            }
            ObjectRpcBodySourceKind::Reader(source) => source
                .lock()
                .map_err(|_| NetworkError::Io {
                    operation: "open one-pass object RPC body reader".to_string(),
                    detail: "reader lock poisoned".to_string(),
                })?
                .reader
                .take()
                .ok_or_else(|| body_consumed_error("one-pass reader was already consumed")),
            ObjectRpcBodySourceKind::AsyncReader(_) => Err(body_consumed_error(
                "asynchronous reader requires the async producer path",
            )),
            ObjectRpcBodySourceKind::IncomingChunkStream(_) => Err(body_consumed_error(
                "incoming chunk stream requires the ownership-moving producer path",
            )),
        }
    }

    /// Materializes this source under an explicit caller-owned resource bound.
    /// This does not change the network protocol or select a transfer path.
    pub async fn collect_to_vec(self, max_bytes: usize) -> Result<Vec<u8>, NetworkError> {
        if let ObjectRpcBodySourceKind::IncomingChunkStream(source) = &self.kind {
            let mut stream = source
                .lock()
                .map_err(|_| NetworkError::Io {
                    operation: "open incoming object RPC chunk stream".to_string(),
                    detail: "incoming chunk stream lock poisoned".to_string(),
                })?
                .stream
                .take()
                .ok_or_else(|| body_consumed_error("incoming chunk stream was already consumed"))?;
            return collect_incoming_chunks_bounded(&mut stream, max_bytes).await;
        }
        if let ObjectRpcBodySourceKind::AsyncReader(source) = &self.kind {
            let mut reader = source
                .lock()
                .map_err(|_| NetworkError::Io {
                    operation: "open asynchronous object RPC body reader".to_string(),
                    detail: "reader lock poisoned".to_string(),
                })?
                .reader
                .take()
                .ok_or_else(|| body_consumed_error("asynchronous reader was already consumed"))?;
            return read_async_body_bounded(&mut reader, max_bytes).await;
        }
        tokio::task::spawn_blocking(move || {
            let mut reader = self.open()?;
            read_sync_body_bounded(&mut reader, max_bytes)
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "join object RPC body collector".to_string(),
            detail: source.to_string(),
        })?
    }

    pub(crate) fn producer(&self, max_chunk_bytes: usize) -> Result<BodyProducer, NetworkError> {
        if matches!(self.kind, ObjectRpcBodySourceKind::Empty) {
            return Ok(BodyProducer::Bytes(BytesBodyProducer {
                bytes: Bytes::new(),
                offset: 0,
                chunk_bytes: max_chunk_bytes.max(1),
                _lifetime_guards: Arc::clone(&self.lifetime_guards),
            }));
        }
        if let ObjectRpcBodySourceKind::Bytes(source) = &self.kind {
            return Ok(BodyProducer::Bytes(BytesBodyProducer {
                bytes: source.bytes.clone(),
                offset: 0,
                chunk_bytes: max_chunk_bytes.max(1),
                _lifetime_guards: Arc::clone(&self.lifetime_guards),
            }));
        }
        if let ObjectRpcBodySourceKind::RepeatedBytes(source) = &self.kind {
            return Ok(BodyProducer::RepeatedBytes(RepeatedBytesBodyProducer {
                pattern: source.pattern.clone(),
                pattern_offset: 0,
                remaining_bytes: source.total_bytes,
                chunk_bytes: max_chunk_bytes.max(1),
                _lifetime_guards: Arc::clone(&self.lifetime_guards),
            }));
        }
        if let ObjectRpcBodySourceKind::AsyncReader(source) = &self.kind {
            let reader = source
                .lock()
                .map_err(|_| NetworkError::Io {
                    operation: "open asynchronous object RPC body reader".to_string(),
                    detail: "reader lock poisoned".to_string(),
                })?
                .reader
                .take()
                .ok_or_else(|| body_consumed_error("asynchronous reader was already consumed"))?;
            return Ok(BodyProducer::Async(AsyncBodyProducer {
                reader,
                chunk_bytes: max_chunk_bytes.max(1),
                _lifetime_guards: Arc::clone(&self.lifetime_guards),
            }));
        }
        if let ObjectRpcBodySourceKind::IncomingChunkStream(source) = &self.kind {
            let stream = source
                .lock()
                .map_err(|_| NetworkError::Io {
                    operation: "open incoming object RPC chunk stream".to_string(),
                    detail: "incoming chunk stream lock poisoned".to_string(),
                })?
                .stream
                .take()
                .ok_or_else(|| body_consumed_error("incoming chunk stream was already consumed"))?;
            return Ok(BodyProducer::Incoming(IncomingBodyProducer {
                stream,
                chunk_bytes: max_chunk_bytes.max(1),
                _lifetime_guards: Arc::clone(&self.lifetime_guards),
            }));
        }
        let reader = self.open()?;
        SyncBodyProducer::spawn(reader, max_chunk_bytes, Arc::clone(&self.lifetime_guards))
            .map(BodyProducer::Sync)
    }

    pub(crate) fn try_clone_replayable(&self) -> Option<Self> {
        let kind = match &self.kind {
            ObjectRpcBodySourceKind::Empty => ObjectRpcBodySourceKind::Empty,
            ObjectRpcBodySourceKind::Bytes(source) => {
                ObjectRpcBodySourceKind::Bytes(source.clone())
            }
            ObjectRpcBodySourceKind::RepeatedBytes(source) => {
                ObjectRpcBodySourceKind::RepeatedBytes(source.clone())
            }
            ObjectRpcBodySourceKind::File(source) => ObjectRpcBodySourceKind::File(source.clone()),
            ObjectRpcBodySourceKind::Reader(_)
            | ObjectRpcBodySourceKind::AsyncReader(_)
            | ObjectRpcBodySourceKind::IncomingChunkStream(_) => {
                return None;
            }
        };
        Some(Self {
            kind,
            role: self.role,
            lifetime_guards: Arc::clone(&self.lifetime_guards),
        })
    }
}

fn body_consumed_error(detail: &str) -> NetworkError {
    NetworkError::delivery_failed(
        "reuse object RPC body source",
        NetworkDeliveryState::DefinitelyNotSent,
        detail,
    )
}

#[derive(Debug)]
struct BytesReader {
    bytes: Bytes,
    offset: usize,
}

#[derive(Debug)]
struct RepeatedBytesReader {
    pattern: Bytes,
    pattern_offset: usize,
    remaining_bytes: u64,
}

impl RepeatedBytesReader {
    fn new(pattern: Bytes, total_bytes: u64) -> Self {
        debug_assert!(!pattern.is_empty());
        Self {
            pattern,
            pattern_offset: 0,
            remaining_bytes: total_bytes,
        }
    }
}

impl Read for RepeatedBytesReader {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        if output.is_empty() || self.remaining_bytes == 0 {
            return Ok(0);
        }
        let wanted = output
            .len()
            .min(usize::try_from(self.remaining_bytes).unwrap_or(usize::MAX));
        let mut written = 0;
        while written < wanted {
            let available = self.pattern.len() - self.pattern_offset;
            let copied = available.min(wanted - written);
            output[written..written + copied]
                .copy_from_slice(&self.pattern[self.pattern_offset..self.pattern_offset + copied]);
            written += copied;
            self.pattern_offset += copied;
            if self.pattern_offset == self.pattern.len() {
                self.pattern_offset = 0;
            }
        }
        self.remaining_bytes -= written as u64;
        Ok(written)
    }
}

impl BytesReader {
    fn new(bytes: Bytes) -> Self {
        Self { bytes, offset: 0 }
    }
}

impl Read for BytesReader {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        let remaining = &self.bytes[self.offset..];
        let read = remaining.len().min(output.len());
        output[..read].copy_from_slice(&remaining[..read]);
        self.offset += read;
        Ok(read)
    }
}

#[derive(Debug)]
enum IncomingBodyItem {
    Bytes(Bytes),
    Error(std::io::Error),
}

#[derive(Debug)]
enum MultiplexedBodyTerminal {
    Pending,
    Complete,
    Failed(String),
}

/// Bounded producer for one stream-native incoming body. Backpressure is
/// propagated to the protocol pump; application bytes are never spooled to disk.
#[derive(Debug)]
pub(crate) struct ObjectRpcBodySender {
    sender: tokio::sync::mpsc::Sender<IncomingBodyItem>,
    terminal: Arc<Mutex<MultiplexedBodyTerminal>>,
    received_bytes: u64,
}

impl ObjectRpcBodySender {
    pub(crate) async fn send(&mut self, bytes: impl Into<Bytes>) -> Result<bool, NetworkError> {
        let bytes = bytes.into();
        self.received_bytes = self
            .received_bytes
            .checked_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX))
            .ok_or_else(|| NetworkError::Serialization {
                detail: "stream-native object RPC body offset overflow".to_string(),
            })?;
        Ok(self
            .sender
            .send(IncomingBodyItem::Bytes(bytes))
            .await
            .is_ok())
    }

    pub(crate) fn fail(&mut self, detail: impl Into<String>) {
        let detail = detail.into();
        if let Ok(mut terminal) = self.terminal.lock() {
            *terminal = MultiplexedBodyTerminal::Failed(detail.clone());
        }
        let _ = self
            .sender
            .try_send(IncomingBodyItem::Error(std::io::Error::other(detail)));
    }

    pub(crate) async fn finish(&mut self, total_bytes: u64) -> Result<(), NetworkError> {
        if total_bytes != self.received_bytes {
            return Err(NetworkError::Serialization {
                detail: format!(
                    "stream-native object RPC body completed at {total_bytes} after delivering {} bytes",
                    self.received_bytes
                ),
            });
        }
        let mut terminal = self.terminal.lock().map_err(|_| NetworkError::Io {
            operation: "complete stream-native object RPC body".to_string(),
            detail: "body terminal state lock poisoned".to_string(),
        })?;
        match &*terminal {
            MultiplexedBodyTerminal::Pending => {
                *terminal = MultiplexedBodyTerminal::Complete;
                Ok(())
            }
            MultiplexedBodyTerminal::Complete => Ok(()),
            MultiplexedBodyTerminal::Failed(detail) => Err(NetworkError::Io {
                operation: "complete stream-native object RPC body".to_string(),
                detail: detail.clone(),
            }),
        }
    }
}

#[derive(Debug)]
enum ObjectRpcIncomingBodyInner {
    Empty,
    Bytes(std::io::Cursor<Bytes>),
    Channel {
        receiver: tokio::sync::mpsc::Receiver<IncomingBodyItem>,
        current: Option<std::io::Cursor<Bytes>>,
    },
}

/// A received RPC attachment. It exposes ordinary asynchronous byte reads;
/// transport frames and chunk sizes remain private. Ordered frames move
/// through a bounded in-memory channel whose pressure propagates to the stream.
#[derive(Debug)]
pub struct ObjectRpcIncomingBody {
    inner: ObjectRpcIncomingBodyInner,
    // Natural EOF clears these handles, detaching still-needed RPC work; an
    // early application drop leaves them here so Drop cancels that work.
    total_bytes: Option<u64>,
    role: ObjectRpcBodyRole,
    multiplexed_terminal: Option<Arc<Mutex<MultiplexedBodyTerminal>>>,
}

impl Default for ObjectRpcIncomingBody {
    fn default() -> Self {
        Self::empty()
    }
}

impl ObjectRpcIncomingBody {
    pub const fn empty() -> Self {
        Self {
            inner: ObjectRpcIncomingBodyInner::Empty,
            total_bytes: Some(0),
            role: ObjectRpcBodyRole::None,
            multiplexed_terminal: None,
        }
    }

    pub fn bytes(bytes: Vec<u8>) -> Self {
        if bytes.is_empty() {
            return Self::empty();
        }
        let total_bytes = u64::try_from(bytes.len()).ok();
        Self {
            inner: ObjectRpcIncomingBodyInner::Bytes(std::io::Cursor::new(Bytes::from(bytes))),
            total_bytes,
            role: ObjectRpcBodyRole::Attachment,
            multiplexed_terminal: None,
        }
    }

    pub(crate) fn multiplexed_channel(total_bytes: Option<u64>) -> (ObjectRpcBodySender, Self) {
        let terminal = Arc::new(Mutex::new(MultiplexedBodyTerminal::Pending));
        let (sender, receiver) = tokio::sync::mpsc::channel(BODY_READER_BUFFER_DEPTH);
        (
            ObjectRpcBodySender {
                sender,
                terminal: Arc::clone(&terminal),
                received_bytes: 0,
            },
            Self {
                inner: ObjectRpcIncomingBodyInner::Channel {
                    receiver,
                    current: None,
                },
                total_bytes,
                role: ObjectRpcBodyRole::Attachment,
                multiplexed_terminal: Some(terminal),
            },
        )
    }

    pub fn is_empty(&self) -> bool {
        matches!(self.inner, ObjectRpcIncomingBodyInner::Empty) || self.total_bytes == Some(0)
    }

    pub const fn total_bytes(&self) -> Option<u64> {
        self.total_bytes
    }

    pub async fn collect_to_vec(mut self, max_bytes: usize) -> Result<Vec<u8>, NetworkError> {
        let mut output = Vec::new();
        if let Some(total_bytes) = self.total_bytes {
            let declared_bytes = usize::try_from(total_bytes).unwrap_or(usize::MAX);
            if declared_bytes > max_bytes {
                return Err(NetworkError::FrameTooLarge {
                    actual_bytes: declared_bytes,
                    max_bytes,
                });
            }
            output.try_reserve_exact(declared_bytes).map_err(|_| {
                NetworkError::ResourceExhausted {
                    operation: "reserve object RPC incoming body".to_string(),
                    requested_bytes: declared_bytes,
                }
            })?;
        }
        let mut buffer = vec![0_u8; BODY_READER_BUFFER_BYTES];
        loop {
            let read = self
                .read(&mut buffer)
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "read object RPC incoming body".to_string(),
                    detail: source.to_string(),
                })?;
            if read == 0 {
                return Ok(output);
            }
            let next = output
                .len()
                .checked_add(read)
                .ok_or(NetworkError::FrameTooLarge {
                    actual_bytes: usize::MAX,
                    max_bytes,
                })?;
            if next > max_bytes {
                return Err(NetworkError::FrameTooLarge {
                    actual_bytes: next,
                    max_bytes,
                });
            }
            output
                .try_reserve(read)
                .map_err(|_| NetworkError::ResourceExhausted {
                    operation: "collect object RPC incoming body".to_string(),
                    requested_bytes: next,
                })?;
            output.extend_from_slice(&buffer[..read]);
        }
    }

    /// Consumes and counts an incoming body without copying its chunks through
    /// the public `AsyncRead` adapter.
    ///
    /// This is an internal relay/control-plane escape hatch. Natural EOF still
    /// performs the same terminal validation and completion-task detachment as
    /// the application reader, while a transport failure remains an error.
    #[doc(hidden)]
    pub async fn discard_owned_chunks(self) -> Result<u64, NetworkError> {
        const OPERATION: &str = "discard object RPC incoming body";

        let mut stream = self.into_chunk_stream();
        let mut discarded_bytes = 0_u64;
        loop {
            let Some(bytes) =
                stream
                    .next_chunk(usize::MAX)
                    .await
                    .map_err(|source| NetworkError::Io {
                        operation: OPERATION.to_string(),
                        detail: source.to_string(),
                    })?
            else {
                return Ok(discarded_bytes);
            };
            let chunk_bytes =
                u64::try_from(bytes.len()).map_err(|_| NetworkError::Serialization {
                    detail: "object RPC incoming body byte count overflow while discarding"
                        .to_string(),
                })?;
            discarded_bytes = discarded_bytes.checked_add(chunk_bytes).ok_or_else(|| {
                NetworkError::Serialization {
                    detail: "object RPC incoming body byte count overflow while discarding"
                        .to_string(),
                }
            })?;
        }
    }

    /// Consumes the application adapter and exposes the received transport
    /// chunks to crate-internal relays without copying them through
    /// `AsyncRead` buffers.
    pub(crate) fn into_chunk_stream(self) -> ObjectRpcIncomingBodyChunkStream {
        ObjectRpcIncomingBodyChunkStream { body: self }
    }
}

/// Crate-private ownership-moving view of an incoming body.
///
/// This deliberately does not implement a public stream trait: transport
/// chunk boundaries stay private, while relay producers can forward the
/// underlying `Bytes` allocation directly.
#[derive(Debug)]
pub(crate) struct ObjectRpcIncomingBodyChunkStream {
    body: ObjectRpcIncomingBody,
}

impl ObjectRpcIncomingBodyChunkStream {
    async fn next_chunk(&mut self, max_chunk_bytes: usize) -> std::io::Result<Option<Bytes>> {
        let max_chunk_bytes = max_chunk_bytes.max(1);
        loop {
            match &mut self.body.inner {
                ObjectRpcIncomingBodyInner::Empty => {
                    return Ok(None);
                }
                ObjectRpcIncomingBodyInner::Bytes(bytes) => {
                    let chunk = take_owned_cursor_chunk(bytes, max_chunk_bytes);
                    if chunk.is_none() {}
                    return Ok(chunk);
                }
                ObjectRpcIncomingBodyInner::Channel { receiver, current } => {
                    if let Some(bytes) = take_owned_current_chunk(current, max_chunk_bytes) {
                        return Ok(Some(bytes));
                    }
                    match receiver.recv().await {
                        Some(IncomingBodyItem::Bytes(bytes)) if bytes.is_empty() => continue,
                        Some(IncomingBodyItem::Bytes(bytes)) if bytes.len() <= max_chunk_bytes => {
                            return Ok(Some(bytes));
                        }
                        Some(IncomingBodyItem::Bytes(bytes)) => {
                            *current = Some(std::io::Cursor::new(bytes));
                        }
                        Some(IncomingBodyItem::Error(error)) => {
                            mark_multiplexed_body_terminal_error_delivered(
                                self.body.multiplexed_terminal.as_ref(),
                            );
                            return Err(error);
                        }
                        None => {
                            validate_multiplexed_body_eof(self.body.multiplexed_terminal.as_ref())?;
                            return Ok(None);
                        }
                    }
                }
            }
        }
    }
}

fn take_owned_current_chunk(
    current: &mut Option<std::io::Cursor<Bytes>>,
    max_chunk_bytes: usize,
) -> Option<Bytes> {
    let mut cursor = current.take()?;
    let chunk = take_owned_cursor_chunk(&mut cursor, max_chunk_bytes);
    if !cursor.get_ref().is_empty() {
        *current = Some(cursor);
    }
    chunk
}

fn take_owned_cursor_chunk(
    cursor: &mut std::io::Cursor<Bytes>,
    max_chunk_bytes: usize,
) -> Option<Bytes> {
    let owned = std::mem::replace(cursor, std::io::Cursor::new(Bytes::new()));
    let position = usize::try_from(owned.position()).unwrap_or(usize::MAX);
    let mut remaining = owned.into_inner();
    let consumed = position.min(remaining.len());
    if consumed != 0 {
        drop(remaining.split_to(consumed));
    }
    if remaining.is_empty() {
        return None;
    }
    let take = remaining.len().min(max_chunk_bytes.max(1));
    let chunk = remaining.split_to(take);
    *cursor = std::io::Cursor::new(remaining);
    Some(chunk)
}

fn validate_multiplexed_body_eof(
    terminal: Option<&Arc<Mutex<MultiplexedBodyTerminal>>>,
) -> std::io::Result<()> {
    let Some(terminal) = terminal else {
        return Ok(());
    };
    match terminal.lock() {
        Ok(mut terminal) => {
            match std::mem::replace(&mut *terminal, MultiplexedBodyTerminal::Complete) {
                MultiplexedBodyTerminal::Pending => Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "multiplexed body ended before declared completion",
                )),
                MultiplexedBodyTerminal::Complete => Ok(()),
                MultiplexedBodyTerminal::Failed(detail) => Err(std::io::Error::other(detail)),
            }
        }
        Err(_) => Err(std::io::Error::other(
            "multiplexed body terminal state lock poisoned",
        )),
    }
}

fn mark_multiplexed_body_terminal_error_delivered(
    terminal: Option<&Arc<Mutex<MultiplexedBodyTerminal>>>,
) {
    let Some(terminal) = terminal else {
        return;
    };
    if let Ok(mut terminal) = terminal.lock()
        && matches!(&*terminal, MultiplexedBodyTerminal::Failed(_))
    {
        // The queued error is the single terminal-failure delivery. Mark it
        // consumed so later reads observe ordinary closed-stream EOF instead
        // of replaying the same failure indefinitely.
        *terminal = MultiplexedBodyTerminal::Complete;
    }
}

async fn collect_incoming_chunks_bounded(
    stream: &mut ObjectRpcIncomingBodyChunkStream,
    max_bytes: usize,
) -> Result<Vec<u8>, NetworkError> {
    let mut output = Vec::new();
    loop {
        let Some(bytes) = stream
            .next_chunk(BODY_READER_BUFFER_BYTES)
            .await
            .map_err(|source| NetworkError::Io {
                operation: "read incoming object RPC chunk stream".to_string(),
                detail: source.to_string(),
            })?
        else {
            return Ok(output);
        };
        let next = output
            .len()
            .checked_add(bytes.len())
            .ok_or(NetworkError::FrameTooLarge {
                actual_bytes: usize::MAX,
                max_bytes,
            })?;
        if next > max_bytes {
            return Err(NetworkError::FrameTooLarge {
                actual_bytes: next,
                max_bytes,
            });
        }
        output
            .try_reserve(bytes.len())
            .map_err(|_| NetworkError::ResourceExhausted {
                operation: "collect incoming object RPC chunk stream".to_string(),
                requested_bytes: next,
            })?;
        output.extend_from_slice(&bytes);
    }
}

impl AsyncRead for ObjectRpcIncomingBody {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        output: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        loop {
            match &mut this.inner {
                ObjectRpcIncomingBodyInner::Empty => {
                    return Poll::Ready(Ok(()));
                }
                ObjectRpcIncomingBodyInner::Bytes(bytes) => {
                    let position = usize::try_from(bytes.position()).unwrap_or(usize::MAX);
                    if position >= bytes.get_ref().len() {
                        return Poll::Ready(Ok(()));
                    }
                    let read = output.remaining().min(bytes.get_ref().len() - position);
                    output.put_slice(&bytes.get_ref()[position..position + read]);
                    bytes.set_position((position + read) as u64);
                    return Poll::Ready(Ok(()));
                }
                ObjectRpcIncomingBodyInner::Channel { receiver, current } => {
                    if let Some(chunk) = current {
                        let position = usize::try_from(chunk.position()).unwrap_or(usize::MAX);
                        let bytes = chunk.get_ref();
                        if position < bytes.len() {
                            let read = output.remaining().min(bytes.len() - position);
                            output.put_slice(&bytes[position..position + read]);
                            chunk.set_position((position + read) as u64);
                            return Poll::Ready(Ok(()));
                        }
                        *current = None;
                    }
                    match receiver.poll_recv(context) {
                        Poll::Ready(Some(IncomingBodyItem::Bytes(bytes))) if bytes.is_empty() => {
                            continue;
                        }
                        Poll::Ready(Some(IncomingBodyItem::Bytes(bytes))) => {
                            *current = Some(std::io::Cursor::new(bytes));
                        }
                        Poll::Ready(Some(IncomingBodyItem::Error(error))) => {
                            mark_multiplexed_body_terminal_error_delivered(
                                this.multiplexed_terminal.as_ref(),
                            );
                            return Poll::Ready(Err(error));
                        }
                        Poll::Ready(None) => {
                            if let Err(error) =
                                validate_multiplexed_body_eof(this.multiplexed_terminal.as_ref())
                            {
                                return Poll::Ready(Err(error));
                            }
                            return Poll::Ready(Ok(()));
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
            }
        }
    }
}

/// Client-side outgoing request head and optional body.
#[derive(Debug)]
pub struct ObjectRpcCall {
    pub request: crate::ObjectRpcRequest,
    pub body: ObjectRpcBodySource,
}

impl ObjectRpcCall {
    pub fn new(request: crate::ObjectRpcRequest, body: ObjectRpcBodySource) -> Self {
        Self {
            request,
            body: body.with_role(ObjectRpcBodyRole::Attachment),
        }
    }

    pub fn value(request: crate::ObjectRpcRequest) -> Self {
        Self {
            request,
            body: ObjectRpcBodySource::value_intent(),
        }
    }

    /// Rebinds a received request body for a trusted internal proxy hop.
    ///
    /// Non-empty attachment chunks retain ownership as they enter the outgoing
    /// producer. Empty bodies retain ordinary value semantics so large typed
    /// request values can still be moved out of the head during preparation.
    /// The prior hop's signed transfer descriptor is not reused: normal call
    /// preparation creates and signs a fresh descriptor for the next hop.
    #[doc(hidden)]
    pub fn from_forwarded_body(
        request: crate::ObjectRpcRequest,
        body: ObjectRpcIncomingBody,
    ) -> Self {
        if body.is_empty() {
            Self::value(request)
        } else {
            Self {
                request,
                body: ObjectRpcBodySource::from_incoming_chunk_stream(body),
            }
        }
    }

    /// Splits the service head from the one stream-native body without adding
    /// a separate transport descriptor. Value calls move their encoded SOBJ
    /// value out of the application head at every size; attachment calls keep
    /// the typed head intact.
    pub(crate) fn into_stream_native_parts(
        mut self,
    ) -> (
        crate::ObjectRpcRequest,
        ObjectRpcBodyRole,
        ObjectRpcBodySource,
    ) {
        if self.body.has_value_intent() {
            let value = std::mem::take(&mut self.request.body);
            if value.is_empty() {
                return (
                    self.request,
                    ObjectRpcBodyRole::None,
                    ObjectRpcBodySource::empty(),
                );
            }
            return (
                self.request,
                ObjectRpcBodyRole::Value,
                ObjectRpcBodySource::bytes(value).with_role(ObjectRpcBodyRole::Value),
            );
        }
        let role = self.body.role();
        (self.request, role, self.body)
    }
}

/// Server-side authenticated request head and received body.
#[derive(Debug)]
pub struct ObjectRpcIncomingRequest {
    pub request: crate::ObjectRpcRequest,
    pub body: ObjectRpcIncomingBody,
}

impl ObjectRpcIncomingRequest {
    pub fn new(request: crate::ObjectRpcRequest, body: ObjectRpcIncomingBody) -> Self {
        Self { request, body }
    }

    pub fn value(request: crate::ObjectRpcRequest) -> Self {
        Self::new(request, ObjectRpcIncomingBody::empty())
    }
}

/// Server-side response head and optional outgoing body.
#[derive(Debug)]
pub struct ObjectRpcOutgoingResponse {
    pub response: crate::ObjectRpcResponse,
    pub body: ObjectRpcBodySource,
}

impl ObjectRpcOutgoingResponse {
    pub fn new(response: crate::ObjectRpcResponse, body: ObjectRpcBodySource) -> Self {
        Self {
            response,
            body: body.with_role(ObjectRpcBodyRole::Attachment),
        }
    }

    pub fn value(response: crate::ObjectRpcResponse) -> Self {
        Self {
            response,
            body: ObjectRpcBodySource::value_intent(),
        }
    }

    pub(crate) fn into_stream_native_parts(
        mut self,
    ) -> (
        crate::ObjectRpcResponse,
        ObjectRpcBodyRole,
        ObjectRpcBodySource,
    ) {
        if self.body.has_value_intent() {
            let value = std::mem::take(&mut self.response.body);
            if value.is_empty() {
                return (
                    self.response,
                    ObjectRpcBodyRole::None,
                    ObjectRpcBodySource::empty(),
                );
            }
            return (
                self.response,
                ObjectRpcBodyRole::Value,
                ObjectRpcBodySource::bytes(value).with_role(ObjectRpcBodyRole::Value),
            );
        }
        let role = self.body.role();
        (self.response, role, self.body)
    }
}

/// Client-side received response head and body.
#[derive(Debug)]
pub struct ObjectRpcResult {
    pub response: crate::ObjectRpcResponse,
    pub body: ObjectRpcIncomingBody,
}

impl ObjectRpcResult {
    pub fn new(response: crate::ObjectRpcResponse, body: ObjectRpcIncomingBody) -> Self {
        Self { response, body }
    }

    pub fn value(response: crate::ObjectRpcResponse) -> Self {
        Self::new(response, ObjectRpcIncomingBody::empty())
    }

    pub fn into_parts(self) -> (crate::ObjectRpcResponse, ObjectRpcIncomingBody) {
        (self.response, self.body)
    }

    #[doc(hidden)]
    pub fn into_forwarded_response(self) -> Result<ObjectRpcOutgoingResponse, NetworkError> {
        Ok(ObjectRpcOutgoingResponse::new(
            self.response,
            ObjectRpcBodySource::from_incoming_chunk_stream(self.body),
        ))
    }
}

pub(crate) enum BodyProducer {
    Bytes(BytesBodyProducer),
    RepeatedBytes(RepeatedBytesBodyProducer),
    Sync(SyncBodyProducer),
    Async(AsyncBodyProducer),
    Incoming(IncomingBodyProducer),
}

impl BodyProducer {
    pub(crate) async fn next(&mut self) -> Result<Option<Bytes>, NetworkError> {
        match self {
            Self::Bytes(producer) => producer.next(),
            Self::RepeatedBytes(producer) => producer.next(),
            Self::Sync(producer) => producer.next().await,
            Self::Async(producer) => producer.next().await,
            Self::Incoming(producer) => producer.next().await,
        }
    }

    pub(crate) async fn finish(self) -> Result<(), NetworkError> {
        match self {
            Self::Bytes(_) => Ok(()),
            Self::RepeatedBytes(_) => Ok(()),
            Self::Sync(producer) => producer.finish().await,
            Self::Async(_) => Ok(()),
            Self::Incoming(_) => Ok(()),
        }
    }
}

pub(crate) struct BytesBodyProducer {
    bytes: Bytes,
    offset: usize,
    chunk_bytes: usize,
    _lifetime_guards: Arc<Mutex<Vec<Box<dyn Any + Send>>>>,
}

impl BytesBodyProducer {
    fn next(&mut self) -> Result<Option<Bytes>, NetworkError> {
        if self.offset >= self.bytes.len() {
            return Ok(None);
        }
        let end = self
            .offset
            .saturating_add(self.chunk_bytes)
            .min(self.bytes.len());
        let bytes = self.bytes.slice(self.offset..end);
        self.offset = end;
        Ok(Some(bytes))
    }
}

pub(crate) struct RepeatedBytesBodyProducer {
    pattern: Bytes,
    pattern_offset: usize,
    remaining_bytes: u64,
    chunk_bytes: usize,
    _lifetime_guards: Arc<Mutex<Vec<Box<dyn Any + Send>>>>,
}

impl RepeatedBytesBodyProducer {
    fn next(&mut self) -> Result<Option<Bytes>, NetworkError> {
        if self.remaining_bytes == 0 {
            return Ok(None);
        }
        let available = self.pattern.len() - self.pattern_offset;
        let bytes = self
            .chunk_bytes
            .min(available)
            .min(usize::try_from(self.remaining_bytes).unwrap_or(usize::MAX));
        let chunk = self
            .pattern
            .slice(self.pattern_offset..self.pattern_offset + bytes);
        self.pattern_offset += bytes;
        if self.pattern_offset == self.pattern.len() {
            self.pattern_offset = 0;
        }
        self.remaining_bytes -= bytes as u64;
        Ok(Some(chunk))
    }
}

pub(crate) struct AsyncBodyProducer {
    reader: Pin<Box<dyn AsyncRead + Send>>,
    chunk_bytes: usize,
    _lifetime_guards: Arc<Mutex<Vec<Box<dyn Any + Send>>>>,
}

impl AsyncBodyProducer {
    async fn next(&mut self) -> Result<Option<Bytes>, NetworkError> {
        let mut bytes = pooled_body_buffer(self.chunk_bytes);
        let read = match self.reader.read(&mut bytes).await {
            Ok(read) => read,
            Err(source) => {
                drop(pooled_body_bytes(bytes));
                return Err(NetworkError::Io {
                    operation: "read asynchronous object RPC body source".to_string(),
                    detail: source.to_string(),
                });
            }
        };
        if read == 0 {
            drop(pooled_body_bytes(bytes));
            return Ok(None);
        }
        bytes.truncate(read);
        Ok(Some(pooled_body_bytes(bytes)))
    }
}

pub(crate) struct IncomingBodyProducer {
    stream: ObjectRpcIncomingBodyChunkStream,
    chunk_bytes: usize,
    _lifetime_guards: Arc<Mutex<Vec<Box<dyn Any + Send>>>>,
}

impl IncomingBodyProducer {
    async fn next(&mut self) -> Result<Option<Bytes>, NetworkError> {
        self.stream
            .next_chunk(self.chunk_bytes)
            .await
            .map_err(|source| NetworkError::Io {
                operation: "read ownership-moving object RPC body source".to_string(),
                detail: source.to_string(),
            })
    }
}

pub(crate) struct SyncBodyProducer {
    receiver: tokio::sync::mpsc::Receiver<Result<Bytes, NetworkError>>,
    task: tokio::task::JoinHandle<()>,
    _lifetime_guards: Arc<Mutex<Vec<Box<dyn Any + Send>>>>,
}

impl SyncBodyProducer {
    fn spawn(
        mut reader: Box<dyn Read + Send>,
        max_chunk_bytes: usize,
        lifetime_guards: Arc<Mutex<Vec<Box<dyn Any + Send>>>>,
    ) -> Result<Self, NetworkError> {
        let chunk_bytes = max_chunk_bytes.max(1);
        let (sender, receiver) = tokio::sync::mpsc::channel(BODY_READER_BUFFER_DEPTH);
        let task_lifetime_guards = Arc::clone(&lifetime_guards);
        let task = tokio::task::spawn_blocking(move || {
            // A cancelled consumer drops `SyncBodyProducer` without joining
            // this blocking task. Keep staged-file cleanup guards alive until
            // the reader itself closes, which is required on Windows where an
            // open file cannot be unlinked reliably.
            let _task_lifetime_guards = task_lifetime_guards;
            loop {
                let mut bytes = pooled_body_buffer(chunk_bytes);
                let read = match reader.read(&mut bytes) {
                    Ok(read) => read,
                    Err(source) if source.kind() == std::io::ErrorKind::Interrupted => {
                        drop(pooled_body_bytes(bytes));
                        continue;
                    }
                    Err(source) => {
                        drop(pooled_body_bytes(bytes));
                        let _ = sender.blocking_send(Err(NetworkError::Io {
                            operation: "read synchronous object RPC body source".to_string(),
                            detail: source.to_string(),
                        }));
                        return;
                    }
                };
                bytes.truncate(read);
                if sender.blocking_send(Ok(pooled_body_bytes(bytes))).is_err() || read == 0 {
                    return;
                }
            }
        });
        Ok(Self {
            receiver,
            task,
            _lifetime_guards: lifetime_guards,
        })
    }

    async fn next(&mut self) -> Result<Option<Bytes>, NetworkError> {
        match self.receiver.recv().await {
            Some(Ok(bytes)) if bytes.is_empty() => Ok(None),
            Some(Ok(bytes)) => Ok(Some(bytes)),
            Some(Err(error)) => Err(error),
            None => Ok(None),
        }
    }

    async fn finish(self) -> Result<(), NetworkError> {
        self.task.await.map_err(|source| NetworkError::Io {
            operation: "join object RPC body producer".to_string(),
            detail: source.to_string(),
        })
    }
}

fn pooled_body_buffer(len: usize) -> BytesMut {
    let mut buffer = BytesMut::with_capacity(len);
    buffer.resize(len, 0);
    buffer
}

fn pooled_body_bytes(bytes: BytesMut) -> Bytes {
    bytes.freeze()
}

fn read_sync_body_bounded(
    reader: &mut Box<dyn Read + Send>,
    max_bytes: usize,
) -> Result<Vec<u8>, NetworkError> {
    let mut output = Vec::new();
    let mut buffer = [0_u8; BODY_READER_BUFFER_BYTES];
    loop {
        let read = reader
            .read(&mut buffer)
            .map_err(|source| NetworkError::Io {
                operation: "collect synchronous object RPC body source".to_string(),
                detail: source.to_string(),
            })?;
        if read == 0 {
            return Ok(output);
        }
        let next = output
            .len()
            .checked_add(read)
            .ok_or(NetworkError::FrameTooLarge {
                actual_bytes: usize::MAX,
                max_bytes,
            })?;
        if next > max_bytes {
            return Err(NetworkError::FrameTooLarge {
                actual_bytes: next,
                max_bytes,
            });
        }
        output
            .try_reserve(read)
            .map_err(|_| NetworkError::ResourceExhausted {
                operation: "collect synchronous object RPC body source".to_string(),
                requested_bytes: next,
            })?;
        output.extend_from_slice(&buffer[..read]);
    }
}

async fn read_async_body_bounded(
    reader: &mut Pin<Box<dyn AsyncRead + Send>>,
    max_bytes: usize,
) -> Result<Vec<u8>, NetworkError> {
    let mut output = Vec::new();
    let mut buffer = vec![0_u8; BODY_READER_BUFFER_BYTES];
    loop {
        let read = reader
            .read(&mut buffer)
            .await
            .map_err(|source| NetworkError::Io {
                operation: "collect asynchronous object RPC body source".to_string(),
                detail: source.to_string(),
            })?;
        if read == 0 {
            return Ok(output);
        }
        let next = output
            .len()
            .checked_add(read)
            .ok_or(NetworkError::FrameTooLarge {
                actual_bytes: usize::MAX,
                max_bytes,
            })?;
        if next > max_bytes {
            return Err(NetworkError::FrameTooLarge {
                actual_bytes: next,
                max_bytes,
            });
        }
        output
            .try_reserve(read)
            .map_err(|_| NetworkError::ResourceExhausted {
                operation: "collect asynchronous object RPC body source".to_string(),
                requested_bytes: next,
            })?;
        output.extend_from_slice(&buffer[..read]);
    }
}
