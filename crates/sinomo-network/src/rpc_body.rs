//! Transport-neutral service-RPC bodies and application-facing call values.
//!
//! Wire framing, authentication, routing, and flow control live in the
//! stream-native object-RPC and routing modules.

use crate::owned_chunk::{
    BODY_CHUNK_MAX, CHUNK_HEADROOM_BYTES, ChunkAllocator, ChunkLease, OwnedChunk,
    PayloadCopyReason, SECURE_WIRE_MAX, observe_chunk_identity, record_shared_payload_copy,
};
use crate::{NetworkDeliveryState, NetworkError, SinomoObjectCodec};
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use sinomo_object::serde_ext::SobjOutput;
use std::any::Any;
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::fs::File;
use std::future::Future;
use std::io::Read;
use std::ops::Range;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt};

const BODY_READER_BUFFER_BYTES: usize =
    crate::object_rpc_stream::OBJECT_RPC_STREAM_MAX_DATA_FRAME_BYTES;
const BODY_READER_BUFFER_DEPTH: usize = 2;

/// One ownership-preserving application body chunk.
///
/// Chunks received through the routed owned-record path retain their pooled
/// transport slab and receive-credit lease. Byte-oriented and locally-created
/// bodies use the compatibility `Bytes` representation. Consumers that only
/// inspect the payload do not need to distinguish the two.
pub struct ObjectRpcBodyChunk {
    inner: ObjectRpcBodyChunkInner,
}

enum ObjectRpcBodyChunkInner {
    Bytes(Bytes),
    Lease(ChunkLease),
}

impl fmt::Debug for ObjectRpcBodyChunk {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ObjectRpcBodyChunk")
            .field("len", &self.len())
            .field(
                "pooled",
                &matches!(self.inner, ObjectRpcBodyChunkInner::Lease(_)),
            )
            .finish()
    }
}

impl ObjectRpcBodyChunk {
    pub(crate) fn bytes(bytes: Bytes) -> Self {
        Self {
            inner: ObjectRpcBodyChunkInner::Bytes(bytes),
        }
    }

    pub(crate) fn lease(lease: ChunkLease) -> Self {
        Self {
            inner: ObjectRpcBodyChunkInner::Lease(lease),
        }
    }

    pub fn len(&self) -> usize {
        self.as_ref().len()
    }

    pub fn is_empty(&self) -> bool {
        self.as_ref().is_empty()
    }

    /// Returns the stable pooled-slab identity when this chunk came from the
    /// owned transport path. This is primarily useful for copy-budget
    /// instrumentation and identity tests.
    #[doc(hidden)]
    pub fn pooled_slab_id(&self) -> Option<u64> {
        match &self.inner {
            ObjectRpcBodyChunkInner::Bytes(_) => None,
            ObjectRpcBodyChunkInner::Lease(lease) => Some(lease.slab_id()),
        }
    }

    pub(crate) async fn into_owned(
        self,
        allocator: &ChunkAllocator,
    ) -> Result<OwnedChunk, NetworkError> {
        let source = match self.inner {
            ObjectRpcBodyChunkInner::Lease(lease) => match lease.try_into_owned() {
                Ok(chunk) => return Ok(chunk),
                Err(lease) => ObjectRpcBodyChunkInner::Lease(lease),
            },
            bytes @ ObjectRpcBodyChunkInner::Bytes(_) => bytes,
        };
        let bytes = match &source {
            ObjectRpcBodyChunkInner::Bytes(bytes) => bytes.as_ref(),
            ObjectRpcBodyChunkInner::Lease(lease) => lease.as_slice(),
        };
        let mut chunk = allocator.allocate_data(bytes.len()).await?;
        chunk.copy_payload_from(bytes, PayloadCopyReason::SharedSourceNormalization)?;
        Ok(chunk)
    }

    fn into_bytes(self) -> Bytes {
        match self.inner {
            ObjectRpcBodyChunkInner::Bytes(bytes) => bytes,
            ObjectRpcBodyChunkInner::Lease(lease) => {
                record_shared_payload_copy(lease.len(), PayloadCopyReason::ByteStreamWriteAdapter);
                Bytes::copy_from_slice(lease.as_slice())
            }
        }
    }

    fn slice(&self, start: usize, end: usize) -> std::io::Result<Self> {
        if start > end || end > self.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid object RPC body chunk slice",
            ));
        }
        match &self.inner {
            ObjectRpcBodyChunkInner::Bytes(bytes) => Ok(Self::bytes(bytes.slice(start..end))),
            ObjectRpcBodyChunkInner::Lease(lease) => lease
                .slice(start, end)
                .map(Self::lease)
                .map_err(|error| std::io::Error::other(error.to_string())),
        }
    }
}

impl AsRef<[u8]> for ObjectRpcBodyChunk {
    fn as_ref(&self) -> &[u8] {
        match &self.inner {
            ObjectRpcBodyChunkInner::Bytes(bytes) => bytes,
            ObjectRpcBodyChunkInner::Lease(lease) => lease.as_slice(),
        }
    }
}

impl std::ops::Deref for ObjectRpcBodyChunk {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

pub(crate) type ObjectRpcBodyReceiverFuture<'a> =
    Pin<Box<dyn Future<Output = std::io::Result<Option<ObjectRpcBodyChunk>>> + Send + 'a>>;

/// Pull-native source for one authenticated object-RPC body.
///
/// The canonical routed path installs the RPC record reader directly here, so
/// application reads consume the mux slot's ownership-bearing chunks without a
/// second queue or a per-call feeder task. Implementations remain responsible
/// for draining and authenticating an abandoned body from their `Drop`
/// implementation.
pub(crate) trait ObjectRpcBodyReceiver: fmt::Debug + Send {
    fn recv_chunk(&mut self) -> ObjectRpcBodyReceiverFuture<'_>;
}

#[derive(Debug)]
struct BodyChunkCursor {
    chunk: Option<ObjectRpcBodyChunk>,
    offset: usize,
}

impl BodyChunkCursor {
    fn new(chunk: ObjectRpcBodyChunk) -> Self {
        Self {
            chunk: Some(chunk),
            offset: 0,
        }
    }

    fn remaining(&self) -> &[u8] {
        let Some(chunk) = &self.chunk else {
            return &[];
        };
        &chunk.as_ref()[self.offset.min(chunk.len())..]
    }

    fn take(&mut self, max_bytes: usize) -> std::io::Result<Option<ObjectRpcBodyChunk>> {
        let Some(chunk) = self.chunk.as_ref() else {
            return Ok(None);
        };
        if self.offset >= chunk.len() {
            self.chunk = None;
            return Ok(None);
        }
        let remaining = chunk.len() - self.offset;
        let take = remaining.min(max_bytes.max(1));
        if self.offset == 0 && take == chunk.len() {
            return Ok(self.chunk.take());
        }
        let output = chunk.slice(self.offset, self.offset + take)?;
        self.offset += take;
        if self.offset == chunk.len() {
            self.chunk = None;
        }
        Ok(Some(output))
    }

    fn advance(&mut self, bytes: usize) {
        let len = self.chunk.as_ref().map_or(0, ObjectRpcBodyChunk::len);
        self.offset = self.offset.saturating_add(bytes).min(len);
        if self.offset == len {
            self.chunk = None;
        }
    }
}

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
    forwarded_request_digest: Option<ForwardedRequestDigest>,
}

#[derive(Clone, Debug)]
struct ForwardedRequestDigest {
    digest: [u8; 32],
    metadata: BTreeMap<String, String>,
}

struct AdoptableSobjBuffer {
    storage: Vec<u8>,
    start: usize,
    end: usize,
}

struct AdoptableSobjSource {
    buffers: Mutex<Option<VecDeque<AdoptableSobjBuffer>>>,
    total_bytes: u64,
    digest: [u8; 32],
}

impl fmt::Debug for AdoptableSobjSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdoptableSobjSource")
            .field(
                "available",
                &self.buffers.lock().is_ok_and(|buffers| buffers.is_some()),
            )
            .field("total_bytes", &self.total_bytes)
            .finish_non_exhaustive()
    }
}

trait DeferredSobjValue: Send {
    fn encode_into(&self, output: &mut dyn SobjOutput) -> Result<Range<usize>, NetworkError>;
}

impl<T> DeferredSobjValue for T
where
    T: Serialize + Send + 'static,
{
    fn encode_into(&self, output: &mut dyn SobjOutput) -> Result<Range<usize>, NetworkError> {
        SinomoObjectCodec::network_frame().append_to_output(output, self)
    }
}

struct SharedDeferredSobjValue<T> {
    value: Arc<T>,
}

impl<T> DeferredSobjValue for SharedDeferredSobjValue<T>
where
    T: Serialize + Send + Sync + 'static,
{
    fn encode_into(&self, output: &mut dyn SobjOutput) -> Result<Range<usize>, NetworkError> {
        SinomoObjectCodec::network_frame().append_to_output(output, self.value.as_ref())
    }
}

#[derive(Clone, Debug)]
struct SobjPatch {
    offset: usize,
    bytes: Box<[u8]>,
}

#[derive(Clone, Debug)]
struct SobjEncodingPlan {
    total_bytes: usize,
    patches: Arc<[SobjPatch]>,
    digest: [u8; 32],
}

struct DeferredSobjSource {
    value: Mutex<Option<Box<dyn DeferredSobjValue>>>,
    plan: Arc<SobjEncodingPlan>,
}

impl fmt::Debug for DeferredSobjSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeferredSobjSource")
            .field(
                "available",
                &self.value.lock().is_ok_and(|value| value.is_some()),
            )
            .field("total_bytes", &self.plan.total_bytes)
            .field("patches", &self.plan.patches.len())
            .finish_non_exhaustive()
    }
}

#[derive(Default)]
struct SobjPlanningOutput {
    len: usize,
    patches: BTreeMap<usize, Box<[u8]>>,
}

impl SobjPlanningOutput {
    fn finish(self) -> SobjEncodingPlan {
        SobjEncodingPlan {
            total_bytes: self.len,
            patches: self
                .patches
                .into_iter()
                .map(|(offset, bytes)| SobjPatch { offset, bytes })
                .collect(),
            digest: [0; 32],
        }
    }
}

impl SobjOutput for SobjPlanningOutput {
    fn len(&self) -> usize {
        self.len
    }

    fn append(&mut self, bytes: &[u8], _context: &'static str) -> sinomo_object::Result<()> {
        self.len = self
            .len
            .checked_add(bytes.len())
            .ok_or(sinomo_object::Error::SizeOverflow)?;
        Ok(())
    }

    fn patch(&mut self, offset: usize, bytes: &[u8]) -> sinomo_object::Result<()> {
        let end = offset
            .checked_add(bytes.len())
            .ok_or(sinomo_object::Error::SizeOverflow)?;
        if end > self.len {
            return Err(sinomo_object::Error::UnexpectedEof);
        }
        if let Some((previous_offset, previous)) = self.patches.range(..=offset).next_back() {
            let previous_end = previous_offset
                .checked_add(previous.len())
                .ok_or(sinomo_object::Error::SizeOverflow)?;
            if previous_end > offset {
                if *previous_offset == offset && previous.as_ref() == bytes {
                    return Ok(());
                }
                return Err(sinomo_object::Error::Serde(
                    "overlapping SOBJ backpatches are not streamable".to_string(),
                ));
            }
        }
        if let Some((next_offset, _)) = self.patches.range(offset..).next()
            && *next_offset < end
        {
            return Err(sinomo_object::Error::Serde(
                "overlapping SOBJ backpatches are not streamable".to_string(),
            ));
        }
        self.patches.insert(offset, bytes.into());
        Ok(())
    }

    fn truncate(&mut self, len: usize) {
        self.len = self.len.min(len);
        self.patches.retain(|offset, bytes| {
            offset
                .checked_add(bytes.len())
                .is_some_and(|end| end <= self.len)
        });
    }
}

struct PlannedSobjOutput<'a, O: SobjOutput + ?Sized> {
    output: &'a mut O,
    plan: &'a SobjEncodingPlan,
    position: usize,
    patch_index: usize,
    validated_patches: Vec<bool>,
}

impl<'a, O: SobjOutput + ?Sized> PlannedSobjOutput<'a, O> {
    fn new(output: &'a mut O, plan: &'a SobjEncodingPlan) -> Self {
        Self {
            output,
            plan,
            position: 0,
            patch_index: 0,
            validated_patches: vec![false; plan.patches.len()],
        }
    }

    fn finish(self) -> Result<(), NetworkError> {
        if self.position != self.plan.total_bytes {
            return Err(NetworkError::Serialization {
                detail: format!(
                    "deferred SOBJ encoder produced {} bytes after planning {}",
                    self.position, self.plan.total_bytes
                ),
            });
        }
        if let Some((index, _)) = self
            .validated_patches
            .iter()
            .enumerate()
            .find(|(_, validated)| !**validated)
        {
            return Err(NetworkError::Serialization {
                detail: format!(
                    "deferred SOBJ encoder omitted planned backpatch at offset {}",
                    self.plan.patches[index].offset
                ),
            });
        }
        Ok(())
    }
}

impl<O: SobjOutput + ?Sized> SobjOutput for PlannedSobjOutput<'_, O> {
    fn len(&self) -> usize {
        self.position
    }

    fn append(&mut self, bytes: &[u8], context: &'static str) -> sinomo_object::Result<()> {
        let append_start = self.position;
        let append_end = append_start
            .checked_add(bytes.len())
            .ok_or(sinomo_object::Error::SizeOverflow)?;
        if append_end > self.plan.total_bytes {
            return Err(sinomo_object::Error::Serde(format!(
                "deferred SOBJ encoder exceeded planned length {}",
                self.plan.total_bytes
            )));
        }

        let mut logical = append_start;
        while logical < append_end {
            while self
                .plan
                .patches
                .get(self.patch_index)
                .is_some_and(|patch| patch.offset + patch.bytes.len() <= logical)
            {
                self.patch_index += 1;
            }
            let Some(patch) = self.plan.patches.get(self.patch_index) else {
                self.output
                    .append(&bytes[logical - append_start..], context)?;
                logical = append_end;
                continue;
            };
            if patch.offset >= append_end {
                self.output
                    .append(&bytes[logical - append_start..], context)?;
                logical = append_end;
                continue;
            }
            if logical < patch.offset {
                let ordinary_end = patch.offset.min(append_end);
                self.output.append(
                    &bytes[logical - append_start..ordinary_end - append_start],
                    context,
                )?;
                logical = ordinary_end;
                continue;
            }
            let patch_end = patch.offset + patch.bytes.len();
            let overlap_end = patch_end.min(append_end);
            self.output.append(
                &patch.bytes[logical - patch.offset..overlap_end - patch.offset],
                "planned SOBJ backpatch",
            )?;
            logical = overlap_end;
        }
        self.position = append_end;
        Ok(())
    }

    fn patch(&mut self, offset: usize, bytes: &[u8]) -> sinomo_object::Result<()> {
        let index = self
            .plan
            .patches
            .binary_search_by_key(&offset, |patch| patch.offset)
            .map_err(|_| {
                sinomo_object::Error::Serde(format!(
                    "deferred SOBJ encoder introduced an unplanned backpatch at offset {offset}"
                ))
            })?;
        if self.plan.patches[index].bytes.as_ref() != bytes {
            return Err(sinomo_object::Error::Serde(format!(
                "deferred SOBJ backpatch changed at offset {offset}"
            )));
        }
        self.validated_patches[index] = true;
        Ok(())
    }

    fn truncate(&mut self, len: usize) {
        let len = len.min(self.position);
        let removed = self.position - len;
        let output_len = self.output.len();
        self.output.truncate(output_len.saturating_sub(removed));
        self.position = len;
    }
}

#[derive(Default)]
struct HashingSobjOutput {
    len: usize,
    hasher: blake3::Hasher,
    truncated: bool,
}

impl HashingSobjOutput {
    fn finish(self) -> Result<[u8; 32], NetworkError> {
        if self.truncated {
            return Err(NetworkError::Serialization {
                detail: "deferred SOBJ hashing pass attempted to roll back emitted bytes"
                    .to_string(),
            });
        }
        Ok(*self.hasher.finalize().as_bytes())
    }
}

impl SobjOutput for HashingSobjOutput {
    fn len(&self) -> usize {
        self.len
    }

    fn append(&mut self, bytes: &[u8], _context: &'static str) -> sinomo_object::Result<()> {
        self.len = self
            .len
            .checked_add(bytes.len())
            .ok_or(sinomo_object::Error::SizeOverflow)?;
        self.hasher.update(bytes);
        Ok(())
    }

    fn patch(&mut self, _offset: usize, _bytes: &[u8]) -> sinomo_object::Result<()> {
        Err(sinomo_object::Error::Serde(
            "planned SOBJ output forwarded a backpatch".to_string(),
        ))
    }

    fn truncate(&mut self, _len: usize) {
        self.truncated = true;
    }
}

struct VisitingSobjOutput<'a> {
    len: usize,
    visitor: &'a mut dyn FnMut(AuthValuePayloadPart<'_>),
    truncated: bool,
}

impl SobjOutput for VisitingSobjOutput<'_> {
    fn len(&self) -> usize {
        self.len
    }

    fn append(&mut self, bytes: &[u8], _context: &'static str) -> sinomo_object::Result<()> {
        self.len = self
            .len
            .checked_add(bytes.len())
            .ok_or(sinomo_object::Error::SizeOverflow)?;
        if !bytes.is_empty() {
            (self.visitor)(AuthValuePayloadPart::Bytes(bytes));
        }
        Ok(())
    }

    fn patch(&mut self, _offset: usize, _bytes: &[u8]) -> sinomo_object::Result<()> {
        Err(sinomo_object::Error::Serde(
            "planned SOBJ visitor received a backpatch".to_string(),
        ))
    }

    fn truncate(&mut self, _len: usize) {
        self.truncated = true;
    }
}

fn plan_deferred_sobj(
    value: &dyn DeferredSobjValue,
) -> Result<Arc<SobjEncodingPlan>, NetworkError> {
    let mut planning = SobjPlanningOutput::default();
    let range = value.encode_into(&mut planning)?;
    if range.start != 0 || range.end != planning.len() {
        return Err(NetworkError::Serialization {
            detail: "deferred SOBJ plan did not describe one complete document".to_string(),
        });
    }
    let mut plan = planning.finish();
    let mut hashing = HashingSobjOutput::default();
    let mut planned = PlannedSobjOutput::new(&mut hashing, &plan);
    let range = value.encode_into(&mut planned)?;
    if range != (0..plan.total_bytes) {
        return Err(NetworkError::Serialization {
            detail: "deferred SOBJ hashing pass changed the document range".to_string(),
        });
    }
    planned.finish()?;
    if hashing.len != plan.total_bytes {
        return Err(NetworkError::Serialization {
            detail: "deferred SOBJ hashing pass changed the document length".to_string(),
        });
    }
    plan.digest = hashing.finish()?;
    Ok(Arc::new(plan))
}

#[derive(Default)]
struct SegmentedSobjOutput {
    buffers: Vec<Vec<u8>>,
    len: usize,
    max_payload_bytes: Option<usize>,
    overflowed: bool,
}

impl SegmentedSobjOutput {
    fn bounded(max_payload_bytes: usize) -> Self {
        Self {
            max_payload_bytes: Some(max_payload_bytes),
            ..Self::default()
        }
    }

    fn push_buffer(&mut self) -> sinomo_object::Result<()> {
        let mut storage = Vec::new();
        storage.try_reserve_exact(SECURE_WIRE_MAX).map_err(|_| {
            sinomo_object::Error::ResourceExhausted {
                context: "transport-shaped SOBJ segment",
                requested_bytes: SECURE_WIRE_MAX,
            }
        })?;
        storage.resize(CHUNK_HEADROOM_BYTES, 0);
        self.buffers.push(storage);
        Ok(())
    }

    fn finish(self) -> Result<VecDeque<AdoptableSobjBuffer>, NetworkError> {
        let mut buffers = VecDeque::with_capacity(self.buffers.len());
        for storage in self.buffers {
            let end = storage.len();
            if end <= CHUNK_HEADROOM_BYTES || end > CHUNK_HEADROOM_BYTES + BODY_CHUNK_MAX {
                return Err(NetworkError::Serialization {
                    detail: "segmented SOBJ encoder produced an invalid payload range".to_string(),
                });
            }
            buffers.push_back(AdoptableSobjBuffer {
                storage,
                start: CHUNK_HEADROOM_BYTES,
                end,
            });
        }
        Ok(buffers)
    }
}

impl sinomo_object::serde_ext::SobjOutput for SegmentedSobjOutput {
    fn len(&self) -> usize {
        self.len
    }

    fn append(&mut self, mut bytes: &[u8], _context: &'static str) -> sinomo_object::Result<()> {
        let next_len = self
            .len
            .checked_add(bytes.len())
            .ok_or(sinomo_object::Error::SizeOverflow)?;
        if self
            .max_payload_bytes
            .is_some_and(|max_payload_bytes| next_len > max_payload_bytes)
        {
            self.overflowed = true;
            return Err(sinomo_object::Error::ResourceExhausted {
                context: "bounded transport-shaped SOBJ document",
                requested_bytes: next_len,
            });
        }
        while !bytes.is_empty() {
            if self
                .buffers
                .last()
                .is_none_or(|buffer| buffer.len() == CHUNK_HEADROOM_BYTES + BODY_CHUNK_MAX)
            {
                self.push_buffer()?;
            }
            let buffer = self.buffers.last_mut().expect("ensured SOBJ segment");
            let available = CHUNK_HEADROOM_BYTES + BODY_CHUNK_MAX - buffer.len();
            let written = available.min(bytes.len());
            buffer.extend_from_slice(&bytes[..written]);
            self.len = self
                .len
                .checked_add(written)
                .ok_or(sinomo_object::Error::SizeOverflow)?;
            bytes = &bytes[written..];
        }
        Ok(())
    }

    fn patch(&mut self, mut offset: usize, mut bytes: &[u8]) -> sinomo_object::Result<()> {
        let end = offset
            .checked_add(bytes.len())
            .ok_or(sinomo_object::Error::SizeOverflow)?;
        if end > self.len {
            return Err(sinomo_object::Error::UnexpectedEof);
        }
        while !bytes.is_empty() {
            let segment_index = offset / BODY_CHUNK_MAX;
            let segment_offset = offset % BODY_CHUNK_MAX;
            let buffer = self
                .buffers
                .get_mut(segment_index)
                .ok_or(sinomo_object::Error::UnexpectedEof)?;
            let available = buffer
                .len()
                .saturating_sub(CHUNK_HEADROOM_BYTES + segment_offset);
            let patched = available.min(bytes.len());
            if patched == 0 {
                return Err(sinomo_object::Error::UnexpectedEof);
            }
            let start = CHUNK_HEADROOM_BYTES + segment_offset;
            buffer[start..start + patched].copy_from_slice(&bytes[..patched]);
            offset += patched;
            bytes = &bytes[patched..];
        }
        Ok(())
    }

    fn truncate(&mut self, len: usize) {
        let len = len.min(self.len);
        if len == 0 {
            self.buffers.clear();
            self.len = 0;
            return;
        }
        let complete = len / BODY_CHUNK_MAX;
        let partial = len % BODY_CHUNK_MAX;
        let retained = complete + usize::from(partial != 0);
        self.buffers.truncate(retained);
        if partial != 0
            && let Some(buffer) = self.buffers.last_mut()
        {
            buffer.truncate(CHUNK_HEADROOM_BYTES + partial);
        }
        self.len = len;
    }
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
    AdoptableSobj(AdoptableSobjSource),
    DeferredSobj(DeferredSobjSource),
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

pub(crate) enum AuthValuePayloadPart<'a> {
    Length(usize),
    Bytes(&'a [u8]),
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
    fn bounded_adoptable_sobj<T>(value: &T) -> Result<Option<AdoptableSobjSource>, NetworkError>
    where
        T: Serialize + ?Sized,
    {
        let mut output = SegmentedSobjOutput::bounded(BODY_CHUNK_MAX);
        match SinomoObjectCodec::network_frame().append_to_output(&mut output, value) {
            Ok(range) => {
                let payload_bytes = range.len();
                let buffers = output.finish()?;
                let mut hasher = blake3::Hasher::new();
                for buffer in &buffers {
                    hasher.update(&buffer.storage[buffer.start..buffer.end]);
                }
                Ok(Some(AdoptableSobjSource {
                    buffers: Mutex::new(Some(buffers)),
                    total_bytes: payload_bytes as u64,
                    digest: *hasher.finalize().as_bytes(),
                }))
            }
            Err(_) if output.overflowed => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn deferred_sobj_value(value: Box<dyn DeferredSobjValue>) -> Result<Self, NetworkError> {
        let plan = plan_deferred_sobj(value.as_ref())?;
        Ok(Self {
            kind: ObjectRpcBodySourceKind::DeferredSobj(DeferredSobjSource {
                value: Mutex::new(Some(value)),
                plan,
            }),
            role: ObjectRpcBodyRole::Value,
            lifetime_guards: Arc::new(Mutex::new(Vec::new())),
        })
    }

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

    /// Encodes one ordinary SOBJ value directly into transport-shaped
    /// segments. Every segment reserves the complete RPC/mux/secure headroom
    /// and AEAD tailroom and can therefore be adopted by the owned-record path
    /// without a normalization copy, regardless of total value size.
    pub fn sobj_borrowed_value<T: Serialize + ?Sized>(value: &T) -> Result<Self, NetworkError> {
        let mut output = SegmentedSobjOutput::default();
        let range = SinomoObjectCodec::network_frame().append_to_output(&mut output, value)?;
        let payload_bytes = range.len();
        let buffers = output.finish()?;
        let mut hasher = blake3::Hasher::new();
        for buffer in &buffers {
            hasher.update(&buffer.storage[buffer.start..buffer.end]);
        }
        Ok(Self {
            kind: ObjectRpcBodySourceKind::AdoptableSobj(AdoptableSobjSource {
                buffers: Mutex::new(Some(buffers)),
                total_bytes: payload_bytes as u64,
                digest: *hasher.finalize().as_bytes(),
            }),
            role: ObjectRpcBodyRole::Value,
            lifetime_guards: Arc::new(Mutex::new(Vec::new())),
        })
    }

    /// Retains an owned typed value and encodes it under transport allocation
    /// credit when it exceeds one body record.
    ///
    /// A one-record document keeps the low-latency adoptable representation.
    /// Larger documents retain only the Rust value, a compact backpatch plan,
    /// and their digest until the body producer acquires each destination slab.
    /// The encoder then fills those slabs directly and hands ownership to the
    /// object-RPC writer through a bounded one-chunk queue.
    pub fn sobj_value<T>(value: T) -> Result<Self, NetworkError>
    where
        T: Serialize + Send + 'static,
    {
        if let Some(source) = Self::bounded_adoptable_sobj(&value)? {
            return Ok(Self {
                kind: ObjectRpcBodySourceKind::AdoptableSobj(source),
                role: ObjectRpcBodyRole::Value,
                lifetime_guards: Arc::new(Mutex::new(Vec::new())),
            });
        }
        Self::deferred_sobj_value(Box::new(value))
    }

    /// Encodes a shared immutable value into independently owned transport
    /// chunks without cloning the semantic value for each call.
    ///
    /// This is intended for sequential or concurrent fan-out. Each body source
    /// owns one cheap [`Arc`] reference and performs its own bounded encoding;
    /// large byte fields are never duplicated merely to satisfy the encoder's
    /// `'static` lifetime.
    pub fn sobj_shared_value<T>(value: Arc<T>) -> Result<Self, NetworkError>
    where
        T: Serialize + Send + Sync + 'static,
    {
        if let Some(source) = Self::bounded_adoptable_sobj(value.as_ref())? {
            return Ok(Self {
                kind: ObjectRpcBodySourceKind::AdoptableSobj(source),
                role: ObjectRpcBodyRole::Value,
                lifetime_guards: Arc::new(Mutex::new(Vec::new())),
            });
        }
        Self::deferred_sobj_value(Box::new(SharedDeferredSobjValue { value }))
    }

    #[doc(hidden)]
    pub fn sobj_owned<T>(value: T) -> Result<Self, NetworkError>
    where
        T: Serialize + Send + 'static,
    {
        Self::sobj_value(value)
    }

    /// Builds a replayable, bounded-memory zero-filled attachment source.
    ///
    /// Every producer shares one immutable transfer-sized pattern. The
    /// canonical owned path fills transport slabs from that pattern directly;
    /// the byte compatibility path emits `Bytes` slices. This is useful for
    /// generated payloads and benchmarks that must measure transport work
    /// rather than an `AsyncRead` fill loop.
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
            kind: ObjectRpcBodySourceKind::IncomingChunkStream(Mutex::new(
                OnePassIncomingChunkStream {
                    stream: Some(body.into_chunk_stream()),
                    total_bytes,
                    forwarded_request_digest: None,
                },
            )),
            role,
            lifetime_guards: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn from_forwarded_request_body(
        body: ObjectRpcIncomingBody,
        metadata: &BTreeMap<String, String>,
    ) -> Self {
        let total_bytes = body.total_bytes();
        let role = body.role;
        let forwarded_request_digest =
            body.value_auth_digest_sha256()
                .copied()
                .map(|digest| ForwardedRequestDigest {
                    digest,
                    metadata: metadata.clone(),
                });
        Self {
            kind: ObjectRpcBodySourceKind::IncomingChunkStream(Mutex::new(
                OnePassIncomingChunkStream {
                    stream: Some(body.into_chunk_stream()),
                    total_bytes,
                    forwarded_request_digest,
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

    pub(crate) fn uses_head_value_intent(&self) -> bool {
        self.role == ObjectRpcBodyRole::Value && matches!(self.kind, ObjectRpcBodySourceKind::Empty)
    }

    pub fn is_empty(&self) -> bool {
        matches!(self.kind, ObjectRpcBodySourceKind::Empty)
            || matches!(&self.kind, ObjectRpcBodySourceKind::Bytes(source) if source.bytes.is_empty())
            || matches!(&self.kind, ObjectRpcBodySourceKind::AdoptableSobj(source)
                if source.total_bytes == 0)
            || matches!(&self.kind, ObjectRpcBodySourceKind::DeferredSobj(source)
                if source.plan.total_bytes == 0)
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
            ObjectRpcBodySourceKind::AdoptableSobj(source) => Some(source.total_bytes),
            ObjectRpcBodySourceKind::DeferredSobj(source) => {
                u64::try_from(source.plan.total_bytes).ok()
            }
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
            ObjectRpcBodySourceKind::AdoptableSobj(source) => {
                let buffers = source
                    .buffers
                    .lock()
                    .map_err(|_| NetworkError::Io {
                        operation: "open transport-shaped SOBJ value".to_string(),
                        detail: "SOBJ value lock poisoned".to_string(),
                    })?
                    .take()
                    .ok_or_else(|| {
                        body_consumed_error("transport-shaped SOBJ value was already consumed")
                    })?;
                Ok(Box::new(AdoptableSobjReader { buffers, offset: 0 }))
            }
            ObjectRpcBodySourceKind::DeferredSobj(_) => Err(body_consumed_error(
                "deferred SOBJ value requires the owned producer path",
            )),
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
        if let ObjectRpcBodySourceKind::DeferredSobj(source) = &self.kind {
            if source.plan.total_bytes > max_bytes {
                return Err(NetworkError::FrameTooLarge {
                    actual_bytes: source.plan.total_bytes,
                    max_bytes,
                });
            }
            let value = source
                .value
                .lock()
                .map_err(|_| NetworkError::Io {
                    operation: "materialize deferred SOBJ value".to_string(),
                    detail: "deferred SOBJ value lock poisoned".to_string(),
                })?
                .take()
                .ok_or_else(|| body_consumed_error("deferred SOBJ value was already consumed"))?;
            let plan = Arc::clone(&source.plan);
            return tokio::task::spawn_blocking(move || {
                let mut output = Vec::new();
                output.try_reserve_exact(plan.total_bytes).map_err(|_| {
                    NetworkError::ResourceExhausted {
                        operation: "reserve deferred SOBJ materialization".to_string(),
                        requested_bytes: plan.total_bytes,
                    }
                })?;
                let mut planned = PlannedSobjOutput::new(&mut output, &plan);
                let range = value.encode_into(&mut planned)?;
                if range != (0..plan.total_bytes) {
                    return Err(NetworkError::Serialization {
                        detail: "materialized deferred SOBJ changed its planned range".to_string(),
                    });
                }
                planned.finish()?;
                if output.len() != plan.total_bytes
                    || blake3::hash(&output).as_bytes() != &plan.digest
                {
                    return Err(NetworkError::Serialization {
                        detail: "materialized deferred SOBJ changed after authentication planning"
                            .to_string(),
                    });
                }
                Ok(output)
            })
            .await
            .map_err(|source| NetworkError::Io {
                operation: "join deferred SOBJ materializer".to_string(),
                detail: source.to_string(),
            })?;
        }
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
            return collect_incoming_chunks_bounded(&mut stream, max_bytes, Vec::new()).await;
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
        if let ObjectRpcBodySourceKind::AdoptableSobj(source) = &self.kind {
            let buffers = source
                .buffers
                .lock()
                .map_err(|_| NetworkError::Io {
                    operation: "open transport-shaped SOBJ producer".to_string(),
                    detail: "SOBJ value lock poisoned".to_string(),
                })?
                .take()
                .ok_or_else(|| {
                    body_consumed_error("transport-shaped SOBJ value was already consumed")
                })?;
            return Ok(BodyProducer::Adoptable(AdoptableSobjProducer { buffers }));
        }
        if let ObjectRpcBodySourceKind::DeferredSobj(source) = &self.kind {
            let value = source
                .value
                .lock()
                .map_err(|_| NetworkError::Io {
                    operation: "open deferred SOBJ producer".to_string(),
                    detail: "deferred SOBJ value lock poisoned".to_string(),
                })?
                .take()
                .ok_or_else(|| body_consumed_error("deferred SOBJ value was already consumed"))?;
            return Ok(BodyProducer::Deferred(DeferredSobjProducer::new(
                value,
                Arc::clone(&source.plan),
                max_chunk_bytes,
            )));
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
            ObjectRpcBodySourceKind::AdoptableSobj(_) => return None,
            ObjectRpcBodySourceKind::DeferredSobj(_) => return None,
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

    /// Returns whether request metadata can still be changed without
    /// invalidating an authenticated one-pass body proof.
    ///
    /// A trusted proxy can forward a streamed typed value only by retaining
    /// the digest verified on the previous hop. That digest binds the exact
    /// metadata map, so diagnostic or routing layers must not append metadata
    /// after adopting the stream.
    pub(crate) fn allows_request_metadata_mutation(&self) -> bool {
        let ObjectRpcBodySourceKind::IncomingChunkStream(source) = &self.kind else {
            return true;
        };
        source
            .lock()
            .map(|source| source.forwarded_request_digest.is_none())
            .unwrap_or(false)
    }

    pub(crate) fn known_digest(&self) -> Option<[u8; 32]> {
        match &self.kind {
            ObjectRpcBodySourceKind::AdoptableSobj(source) => Some(source.digest),
            ObjectRpcBodySourceKind::DeferredSobj(source) => Some(source.plan.digest),
            _ => None,
        }
    }

    pub(crate) fn forwarded_request_digest(
        &self,
        metadata: &BTreeMap<String, String>,
    ) -> Result<Option<[u8; 32]>, NetworkError> {
        if self.role != ObjectRpcBodyRole::Value {
            return Ok(None);
        }
        let ObjectRpcBodySourceKind::IncomingChunkStream(source) = &self.kind else {
            return Ok(None);
        };
        let source = source.lock().map_err(|_| NetworkError::Io {
            operation: "read forwarded request authentication digest".to_string(),
            detail: "incoming chunk stream lock poisoned".to_string(),
        })?;
        let proof = source.forwarded_request_digest.as_ref().ok_or_else(|| {
            NetworkError::Serialization {
                detail: "cannot authenticate a forwarded streamed value without its verified request digest"
                    .to_string(),
            }
        })?;
        if &proof.metadata != metadata {
            return Err(NetworkError::Serialization {
                detail: "cannot mutate request metadata after adopting a forwarded streamed value"
                    .to_string(),
            });
        }
        Ok(Some(proof.digest))
    }

    pub(crate) fn visit_auth_value_payload(
        &self,
        visitor: &mut dyn FnMut(AuthValuePayloadPart<'_>),
    ) -> Result<bool, NetworkError> {
        if self.role != ObjectRpcBodyRole::Value {
            return Ok(false);
        }
        match &self.kind {
            ObjectRpcBodySourceKind::Empty => Ok(false),
            ObjectRpcBodySourceKind::Bytes(source) => {
                visitor(AuthValuePayloadPart::Length(source.bytes.len()));
                visitor(AuthValuePayloadPart::Bytes(&source.bytes));
                Ok(true)
            }
            ObjectRpcBodySourceKind::AdoptableSobj(source) => {
                let total_bytes = usize::try_from(source.total_bytes).map_err(|_| {
                    NetworkError::FrameTooLarge {
                        actual_bytes: usize::MAX,
                        max_bytes: usize::MAX,
                    }
                })?;
                let buffers = source.buffers.lock().map_err(|_| NetworkError::Io {
                    operation: "hash transport-shaped SOBJ request".to_string(),
                    detail: "SOBJ value lock poisoned".to_string(),
                })?;
                let buffers = buffers.as_ref().ok_or_else(|| {
                    body_consumed_error(
                        "transport-shaped SOBJ value was consumed before authentication",
                    )
                })?;
                visitor(AuthValuePayloadPart::Length(total_bytes));
                let mut observed_bytes = 0_usize;
                for buffer in buffers {
                    let bytes = &buffer.storage[buffer.start..buffer.end];
                    observed_bytes = observed_bytes.checked_add(bytes.len()).ok_or_else(|| {
                        NetworkError::Serialization {
                            detail: "transport-shaped SOBJ length overflow".to_string(),
                        }
                    })?;
                    visitor(AuthValuePayloadPart::Bytes(bytes));
                }
                if observed_bytes != total_bytes {
                    return Err(NetworkError::Serialization {
                        detail: format!(
                            "transport-shaped SOBJ declared {total_bytes} bytes but retained {observed_bytes}"
                        ),
                    });
                }
                Ok(true)
            }
            ObjectRpcBodySourceKind::DeferredSobj(source) => {
                let total_bytes = source.plan.total_bytes;
                let value = source.value.lock().map_err(|_| NetworkError::Io {
                    operation: "hash deferred SOBJ request".to_string(),
                    detail: "deferred SOBJ value lock poisoned".to_string(),
                })?;
                let value = value.as_ref().ok_or_else(|| {
                    body_consumed_error("deferred SOBJ value was consumed before authentication")
                })?;
                visitor(AuthValuePayloadPart::Length(total_bytes));
                let mut output = VisitingSobjOutput {
                    len: 0,
                    visitor,
                    truncated: false,
                };
                let mut planned = PlannedSobjOutput::new(&mut output, &source.plan);
                let range = value.encode_into(&mut planned)?;
                if range != (0..total_bytes) {
                    return Err(NetworkError::Serialization {
                        detail: "deferred SOBJ authentication pass changed its planned range"
                            .to_string(),
                    });
                }
                planned.finish()?;
                if output.truncated || output.len != total_bytes {
                    return Err(NetworkError::Serialization {
                        detail: "deferred SOBJ authentication pass changed its document length"
                            .to_string(),
                    });
                }
                Ok(true)
            }
            ObjectRpcBodySourceKind::IncomingChunkStream(_) => Err(NetworkError::Serialization {
                detail:
                    "forwarded streamed request authentication must use its verified digest proof"
                        .to_string(),
            }),
            ObjectRpcBodySourceKind::RepeatedBytes(_)
            | ObjectRpcBodySourceKind::File(_)
            | ObjectRpcBodySourceKind::Reader(_)
            | ObjectRpcBodySourceKind::AsyncReader(_) => Err(NetworkError::Serialization {
                detail: "one-pass typed request values require a pre-authenticated owned producer"
                    .to_string(),
            }),
        }
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

struct AdoptableSobjReader {
    buffers: VecDeque<AdoptableSobjBuffer>,
    offset: usize,
}

impl Read for AdoptableSobjReader {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        let mut written = 0;
        while written < output.len() {
            let Some(buffer) = self.buffers.front() else {
                break;
            };
            let payload = &buffer.storage[buffer.start..buffer.end];
            if self.offset == payload.len() {
                self.buffers.pop_front();
                self.offset = 0;
                continue;
            }
            let copied = (output.len() - written).min(payload.len() - self.offset);
            output[written..written + copied]
                .copy_from_slice(&payload[self.offset..self.offset + copied]);
            self.offset += copied;
            written += copied;
        }
        record_shared_payload_copy(written, PayloadCopyReason::ByteStreamReadAdapter);
        Ok(written)
    }
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
    Chunk(ObjectRpcBodyChunk),
    Error(std::io::Error),
}

#[derive(Debug)]
enum MultiplexedBodyTerminal {
    Pending,
    Complete,
    Failed(String),
}

#[derive(Debug)]
struct BodyChunkRingState {
    items: VecDeque<IncomingBodyItem>,
    capacity: usize,
    producer_open: bool,
    consumer_open: bool,
    reader_waker: Option<std::task::Waker>,
}

/// One bounded ownership queue between the object-record reader and its
/// application consumer.
///
/// Unlike a Tokio `mpsc`, this ring does not allocate a channel node or
/// schedule an intermediate receiver task for each body record. The queued
/// value is the ownership-bearing chunk itself, including its receive-credit
/// lease.
#[derive(Debug)]
struct BodyChunkRing {
    state: Mutex<BodyChunkRingState>,
    writable: tokio::sync::Notify,
}

impl BodyChunkRing {
    fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(BodyChunkRingState {
                items: VecDeque::with_capacity(capacity.max(1)),
                capacity: capacity.max(1),
                producer_open: true,
                consumer_open: true,
                reader_waker: None,
            }),
            writable: tokio::sync::Notify::new(),
        })
    }

    async fn send(&self, item: IncomingBodyItem) -> Result<bool, NetworkError> {
        let mut item = Some(item);
        loop {
            let writable = self.writable.notified();
            let reader_waker = {
                let mut state = self.state.lock().map_err(|_| NetworkError::Io {
                    operation: "queue stream-native object RPC body chunk".to_string(),
                    detail: "body chunk ring lock poisoned".to_string(),
                })?;
                if !state.consumer_open {
                    return Ok(false);
                }
                if state.items.len() >= state.capacity {
                    None
                } else {
                    state
                        .items
                        .push_back(item.take().expect("body ring item is queued once"));
                    state.reader_waker.take()
                }
            };
            if item.is_none() {
                if let Some(waker) = reader_waker {
                    waker.wake();
                }
                return Ok(true);
            }
            writable.await;
        }
    }

    fn try_send(&self, item: IncomingBodyItem) -> bool {
        let reader_waker = {
            let Ok(mut state) = self.state.lock() else {
                return false;
            };
            if !state.consumer_open || state.items.len() >= state.capacity {
                return false;
            }
            state.items.push_back(item);
            state.reader_waker.take()
        };
        if let Some(waker) = reader_waker {
            waker.wake();
        }
        true
    }

    fn poll_recv(&self, context: &mut Context<'_>) -> Poll<Option<IncomingBodyItem>> {
        let item = {
            let Ok(mut state) = self.state.lock() else {
                return Poll::Ready(Some(IncomingBodyItem::Error(std::io::Error::other(
                    "body chunk ring lock poisoned",
                ))));
            };
            if let Some(item) = state.items.pop_front() {
                Some(Some(item))
            } else if !state.producer_open {
                Some(None)
            } else {
                state.reader_waker = Some(context.waker().clone());
                None
            }
        };
        match item {
            Some(item) => {
                self.writable.notify_one();
                Poll::Ready(item)
            }
            None => Poll::Pending,
        }
    }

    async fn recv(&self) -> Option<IncomingBodyItem> {
        std::future::poll_fn(|context| self.poll_recv(context)).await
    }

    fn close_producer(&self) {
        let reader_waker = self.state.lock().ok().and_then(|mut state| {
            state.producer_open = false;
            state.reader_waker.take()
        });
        if let Some(waker) = reader_waker {
            waker.wake();
        }
    }

    fn close_consumer(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.consumer_open = false;
            state.items.clear();
            state.reader_waker = None;
        }
        self.writable.notify_waiters();
    }
}

#[derive(Debug)]
struct BodyChunkRingReader {
    ring: Arc<BodyChunkRing>,
}

impl Drop for BodyChunkRingReader {
    fn drop(&mut self) {
        self.ring.close_consumer();
    }
}

/// Bounded producer for one stream-native incoming body. Backpressure is
/// propagated to the protocol pump; application bytes are never spooled to disk.
#[derive(Debug)]
pub(crate) struct ObjectRpcBodySender {
    ring: Arc<BodyChunkRing>,
    terminal: Arc<Mutex<MultiplexedBodyTerminal>>,
    received_bytes: u64,
}

impl Drop for ObjectRpcBodySender {
    fn drop(&mut self) {
        self.ring.close_producer();
    }
}

impl ObjectRpcBodySender {
    #[cfg(test)]
    pub(crate) async fn send(&mut self, bytes: impl Into<Bytes>) -> Result<bool, NetworkError> {
        self.send_chunk(ObjectRpcBodyChunk::bytes(bytes.into()))
            .await
    }

    pub(crate) async fn send_chunk(
        &mut self,
        chunk: ObjectRpcBodyChunk,
    ) -> Result<bool, NetworkError> {
        self.received_bytes = self
            .received_bytes
            .checked_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX))
            .ok_or_else(|| NetworkError::Serialization {
                detail: "stream-native object RPC body offset overflow".to_string(),
            })?;
        self.ring.send(IncomingBodyItem::Chunk(chunk)).await
    }

    pub(crate) fn fail(&mut self, detail: impl Into<String>) {
        let detail = detail.into();
        if let Ok(mut terminal) = self.terminal.lock() {
            *terminal = MultiplexedBodyTerminal::Failed(detail.clone());
        }
        let _ = self
            .ring
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
    Chunk(BodyChunkCursor),
    Receiver {
        receiver: Box<dyn ObjectRpcBodyReceiver>,
        current: Option<BodyChunkCursor>,
    },
    Ring {
        receiver: BodyChunkRingReader,
        current: Option<BodyChunkCursor>,
    },
}

/// A received RPC attachment. It exposes ordinary asynchronous byte reads;
/// transport frames and chunk sizes remain private. Ordered frames move
/// through a bounded ownership ring whose pressure propagates to the stream.
#[derive(Debug)]
pub struct ObjectRpcIncomingBody {
    inner: ObjectRpcIncomingBodyInner,
    // Natural EOF clears these handles, detaching still-needed RPC work; an
    // early application drop leaves them here so Drop cancels that work.
    total_bytes: Option<u64>,
    role: ObjectRpcBodyRole,
    /// Digest of the complete request value plus request metadata before the
    /// value moved out of the service head. The enclosing object-RPC head
    /// authenticates this claim together with the transport body descriptor.
    value_auth_digest_sha256: Option<[u8; 32]>,
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
            value_auth_digest_sha256: None,
            multiplexed_terminal: None,
        }
    }

    pub fn bytes(bytes: Vec<u8>) -> Self {
        if bytes.is_empty() {
            return Self::empty();
        }
        let total_bytes = u64::try_from(bytes.len()).ok();
        Self {
            inner: ObjectRpcIncomingBodyInner::Chunk(BodyChunkCursor::new(
                ObjectRpcBodyChunk::bytes(Bytes::from(bytes)),
            )),
            total_bytes,
            role: ObjectRpcBodyRole::Attachment,
            value_auth_digest_sha256: None,
            multiplexed_terminal: None,
        }
    }

    pub(crate) fn multiplexed_chunk_ring_with_depth(
        total_bytes: Option<u64>,
        depth: usize,
    ) -> (ObjectRpcBodySender, Self) {
        let terminal = Arc::new(Mutex::new(MultiplexedBodyTerminal::Pending));
        let ring = BodyChunkRing::new(depth);
        (
            ObjectRpcBodySender {
                ring: Arc::clone(&ring),
                terminal: Arc::clone(&terminal),
                received_bytes: 0,
            },
            Self {
                inner: ObjectRpcIncomingBodyInner::Ring {
                    receiver: BodyChunkRingReader { ring },
                    current: None,
                },
                total_bytes,
                role: ObjectRpcBodyRole::Attachment,
                value_auth_digest_sha256: None,
                multiplexed_terminal: Some(terminal),
            },
        )
    }

    pub(crate) fn receiver(
        total_bytes: Option<u64>,
        receiver: Box<dyn ObjectRpcBodyReceiver>,
    ) -> Self {
        Self {
            inner: ObjectRpcIncomingBodyInner::Receiver {
                receiver,
                current: None,
            },
            total_bytes,
            role: ObjectRpcBodyRole::Attachment,
            value_auth_digest_sha256: None,
            multiplexed_terminal: None,
        }
    }

    pub fn is_empty(&self) -> bool {
        matches!(self.inner, ObjectRpcIncomingBodyInner::Empty) || self.total_bytes == Some(0)
    }

    pub const fn total_bytes(&self) -> Option<u64> {
        self.total_bytes
    }

    pub fn is_streamed_value(&self) -> bool {
        self.role == ObjectRpcBodyRole::Value && !self.is_empty()
    }

    pub fn is_attachment(&self) -> bool {
        self.role == ObjectRpcBodyRole::Attachment && !self.is_empty()
    }

    pub fn value_auth_digest_sha256(&self) -> Option<&[u8; 32]> {
        self.value_auth_digest_sha256.as_ref()
    }

    pub(crate) fn with_role(mut self, role: ObjectRpcBodyRole) -> Self {
        self.role = if self.is_empty() {
            ObjectRpcBodyRole::None
        } else {
            role
        };
        self
    }

    pub(crate) fn with_value_auth_digest_sha256(mut self, digest: Option<[u8; 32]>) -> Self {
        self.value_auth_digest_sha256 = if self.role == ObjectRpcBodyRole::Value {
            digest
        } else {
            None
        };
        self
    }

    /// Incrementally decodes an owned SOBJ value while retaining at most a
    /// bounded number of transport chunks.
    ///
    /// The synchronous serde visitor runs on a blocking worker and pulls
    /// directly from the ownership ring through the runtime handle. There is
    /// no second per-chunk channel or async feeder task; once the decoder
    /// advances, each receive-credit lease is dropped rather than retaining
    /// the complete serialized document.
    pub async fn decode_sobj<T>(mut self) -> Result<T, NetworkError>
    where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        let total_bytes = self
            .total_bytes
            .ok_or_else(|| NetworkError::Serialization {
                detail: "incremental SOBJ decoding requires a declared body length".to_string(),
            })?;
        let document_bytes =
            usize::try_from(total_bytes).map_err(|_| NetworkError::FrameTooLarge {
                actual_bytes: usize::MAX,
                max_bytes: usize::MAX,
            })?;
        let first_chunk = self.next_chunk().await.map_err(|source| NetworkError::Io {
            operation: "read first incremental SOBJ chunk".to_string(),
            detail: source.to_string(),
        })?;
        if first_chunk
            .as_ref()
            .is_some_and(|chunk| chunk.len() == document_bytes)
        {
            let chunk = first_chunk.expect("single-chunk length was checked");
            if self
                .next_chunk()
                .await
                .map_err(|source| NetworkError::Io {
                    operation: "verify single-chunk SOBJ completion".to_string(),
                    detail: source.to_string(),
                })?
                .is_some()
            {
                return Err(NetworkError::Serialization {
                    detail:
                        "single-record SOBJ body carried data beyond its declared document length"
                            .to_string(),
                });
            }
            // One transport record is already a bounded chunk view. Decode it
            // in the caller task so ordinary typed RPCs do not pay a blocking
            // worker handoff. This is the same record protocol and ownership
            // path as a large value; only the naturally complete decoder input
            // avoids suspension.
            return crate::SinomoObjectCodec::network_frame().decode(chunk.as_ref());
        }
        let runtime = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || {
            let mut first_chunk = first_chunk;
            let mut complete = false;
            crate::SinomoObjectCodec::network_frame().decode_chunks(
                std::iter::from_fn(move || {
                    if complete {
                        return None;
                    }
                    if let Some(chunk) = first_chunk.take() {
                        return Some(Ok(chunk));
                    }
                    match runtime.block_on(self.next_chunk()) {
                        Ok(Some(chunk)) => Some(Ok(chunk)),
                        Ok(None) => {
                            complete = true;
                            None
                        }
                        Err(source) => {
                            complete = true;
                            Some(Err(sinomo_object::Error::Serde(source.to_string())))
                        }
                    }
                }),
                document_bytes,
            )
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "join incremental SOBJ decoder".to_string(),
            detail: source.to_string(),
        })?
    }

    /// Returns the next transport-owned body chunk without copying it through
    /// an `AsyncRead` buffer. Dropping the returned chunk releases its receive
    /// credit; forwarding it can transfer the same slab into another RPC.
    pub async fn next_chunk(&mut self) -> std::io::Result<Option<ObjectRpcBodyChunk>> {
        let chunk = next_incoming_chunk(self, usize::MAX).await?;
        if let Some(chunk) = &chunk
            && let Some(slab_id) = chunk.pooled_slab_id()
        {
            observe_chunk_identity(slab_id, "application-body-delivery");
        }
        Ok(chunk)
    }

    pub async fn collect_to_vec(self, max_bytes: usize) -> Result<Vec<u8>, NetworkError> {
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
        let mut stream = self.into_chunk_stream();
        collect_incoming_chunks_bounded(&mut stream, max_bytes, output).await
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
    pub(crate) async fn next_chunk(
        &mut self,
        max_chunk_bytes: usize,
    ) -> std::io::Result<Option<ObjectRpcBodyChunk>> {
        next_incoming_chunk(&mut self.body, max_chunk_bytes).await
    }
}

async fn next_incoming_chunk(
    body: &mut ObjectRpcIncomingBody,
    max_chunk_bytes: usize,
) -> std::io::Result<Option<ObjectRpcBodyChunk>> {
    let max_chunk_bytes = max_chunk_bytes.max(1);
    loop {
        match &mut body.inner {
            ObjectRpcIncomingBodyInner::Empty => return Ok(None),
            ObjectRpcIncomingBodyInner::Chunk(cursor) => return cursor.take(max_chunk_bytes),
            ObjectRpcIncomingBodyInner::Receiver { receiver, current } => {
                if let Some(cursor) = current {
                    if let Some(chunk) = cursor.take(max_chunk_bytes)? {
                        return Ok(Some(chunk));
                    }
                    *current = None;
                }
                match receiver.recv_chunk().await? {
                    Some(chunk) if chunk.is_empty() => continue,
                    Some(chunk) if chunk.len() <= max_chunk_bytes => return Ok(Some(chunk)),
                    Some(chunk) => {
                        *current = Some(BodyChunkCursor::new(chunk));
                    }
                    None => return Ok(None),
                }
            }
            ObjectRpcIncomingBodyInner::Ring { receiver, current } => {
                if let Some(cursor) = current {
                    if let Some(chunk) = cursor.take(max_chunk_bytes)? {
                        return Ok(Some(chunk));
                    }
                    *current = None;
                }
                match receiver.ring.recv().await {
                    Some(IncomingBodyItem::Chunk(chunk)) if chunk.is_empty() => continue,
                    Some(IncomingBodyItem::Chunk(chunk)) if chunk.len() <= max_chunk_bytes => {
                        return Ok(Some(chunk));
                    }
                    Some(IncomingBodyItem::Chunk(chunk)) => {
                        *current = Some(BodyChunkCursor::new(chunk));
                    }
                    Some(IncomingBodyItem::Error(error)) => {
                        mark_multiplexed_body_terminal_error_delivered(
                            body.multiplexed_terminal.as_ref(),
                        );
                        return Err(error);
                    }
                    None => {
                        validate_multiplexed_body_eof(body.multiplexed_terminal.as_ref())?;
                        return Ok(None);
                    }
                }
            }
        }
    }
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
    mut output: Vec<u8>,
) -> Result<Vec<u8>, NetworkError> {
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
        record_shared_payload_copy(
            bytes.len(),
            PayloadCopyReason::ContiguousBodyMaterialization,
        );
    }
}

async fn bridge_body_receiver_to_ring(
    mut receiver: Box<dyn ObjectRpcBodyReceiver>,
    mut current: Option<BodyChunkCursor>,
    mut sender: ObjectRpcBodySender,
) {
    let result: std::io::Result<u64> = async {
        let mut received_bytes = 0_u64;
        if let Some(cursor) = current.as_mut()
            && let Some(chunk) = cursor.take(usize::MAX)?
        {
            received_bytes = received_bytes
                .checked_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX))
                .ok_or_else(|| std::io::Error::other("object RPC body length overflow"))?;
            let _consumer_open = sender
                .send_chunk(chunk)
                .await
                .map_err(|error| std::io::Error::other(error.to_string()))?;
        }
        loop {
            let Some(chunk) = receiver.recv_chunk().await? else {
                return Ok(received_bytes);
            };
            received_bytes = received_bytes
                .checked_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX))
                .ok_or_else(|| std::io::Error::other("object RPC body length overflow"))?;
            let _consumer_open = sender
                .send_chunk(chunk)
                .await
                .map_err(|error| std::io::Error::other(error.to_string()))?;
        }
    }
    .await;
    match result {
        Ok(received_bytes) => {
            if let Err(error) = sender.finish(received_bytes).await {
                sender.fail(error.to_string());
            }
        }
        Err(error) => sender.fail(error.to_string()),
    }
}

impl ObjectRpcIncomingBody {
    fn bridge_receiver_for_async_read(&mut self) -> std::io::Result<()> {
        let inner = std::mem::replace(&mut self.inner, ObjectRpcIncomingBodyInner::Empty);
        let ObjectRpcIncomingBodyInner::Receiver { receiver, current } = inner else {
            self.inner = inner;
            return Ok(());
        };
        let (sender, bridged) =
            Self::multiplexed_chunk_ring_with_depth(self.total_bytes, BODY_READER_BUFFER_DEPTH);
        self.inner = bridged.inner;
        self.multiplexed_terminal = bridged.multiplexed_terminal;
        tokio::runtime::Handle::try_current()
            .map_err(|error| {
                std::io::Error::other(format!(
                    "object RPC AsyncRead compatibility requires a Tokio runtime: {error}"
                ))
            })?
            .spawn(bridge_body_receiver_to_ring(receiver, current, sender));
        Ok(())
    }
}

impl AsyncRead for ObjectRpcIncomingBody {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        output: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if matches!(this.inner, ObjectRpcIncomingBodyInner::Receiver { .. })
            && let Err(error) = this.bridge_receiver_for_async_read()
        {
            return Poll::Ready(Err(error));
        }
        loop {
            match &mut this.inner {
                ObjectRpcIncomingBodyInner::Empty => {
                    return Poll::Ready(Ok(()));
                }
                ObjectRpcIncomingBodyInner::Chunk(cursor) => {
                    if cursor.remaining().is_empty() {
                        return Poll::Ready(Ok(()));
                    }
                    let read = output.remaining().min(cursor.remaining().len());
                    output.put_slice(&cursor.remaining()[..read]);
                    cursor.advance(read);
                    record_shared_payload_copy(read, PayloadCopyReason::ByteStreamReadAdapter);
                    return Poll::Ready(Ok(()));
                }
                ObjectRpcIncomingBodyInner::Receiver { .. } => {
                    unreachable!("pull receiver was bridged before byte-oriented reading")
                }
                ObjectRpcIncomingBodyInner::Ring { receiver, current } => {
                    if let Some(chunk) = current {
                        if !chunk.remaining().is_empty() {
                            let read = output.remaining().min(chunk.remaining().len());
                            output.put_slice(&chunk.remaining()[..read]);
                            chunk.advance(read);
                            record_shared_payload_copy(
                                read,
                                PayloadCopyReason::ByteStreamReadAdapter,
                            );
                            return Poll::Ready(Ok(()));
                        }
                        *current = None;
                    }
                    match receiver.ring.poll_recv(context) {
                        Poll::Ready(Some(IncomingBodyItem::Chunk(chunk))) if chunk.is_empty() => {
                            continue;
                        }
                        Poll::Ready(Some(IncomingBodyItem::Chunk(chunk))) => {
                            *current = Some(BodyChunkCursor::new(chunk));
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

    /// Encodes an ordinary typed request directly into transport-shaped
    /// segments. The request head remains small and authentication hashes the
    /// segmented value in place before the first record is sent.
    pub fn encode_borrowed_value<T: Serialize + ?Sized>(
        route: impl Into<String>,
        category: impl Into<String>,
        value: &T,
    ) -> Result<Self, NetworkError> {
        Ok(Self {
            request: crate::ObjectRpcRequest::from_encoded_body(route, category, Vec::new()),
            body: ObjectRpcBodySource::sobj_borrowed_value(value)?,
        })
    }

    /// Encodes an owned ordinary value directly into allocator-controlled
    /// chunks when it exceeds one transport record.
    pub fn encode_value<T>(
        route: impl Into<String>,
        category: impl Into<String>,
        value: T,
    ) -> Result<Self, NetworkError>
    where
        T: Serialize + Send + 'static,
    {
        Ok(Self {
            request: crate::ObjectRpcRequest::from_encoded_body(route, category, Vec::new()),
            body: ObjectRpcBodySource::sobj_value(value)?,
        })
    }

    /// Encodes one shared immutable request without cloning its semantic
    /// payload. Every call receives an independent credit-bounded body source.
    pub fn encode_shared_value<T>(
        route: impl Into<String>,
        category: impl Into<String>,
        value: Arc<T>,
    ) -> Result<Self, NetworkError>
    where
        T: Serialize + Send + Sync + 'static,
    {
        Ok(Self {
            request: crate::ObjectRpcRequest::from_encoded_body(route, category, Vec::new()),
            body: ObjectRpcBodySource::sobj_shared_value(value)?,
        })
    }

    #[doc(hidden)]
    pub fn encode_owned_value<T>(
        route: impl Into<String>,
        category: impl Into<String>,
        value: T,
    ) -> Result<Self, NetworkError>
    where
        T: Serialize + Send + 'static,
    {
        Self::encode_value(route, category, value)
    }

    /// Whether this call carries its ordinary typed value in the owned body
    /// stream instead of the request head.
    pub fn has_streamed_value_body(&self) -> bool {
        self.body.role() == ObjectRpcBodyRole::Value && !self.body.is_empty()
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
            let body = ObjectRpcBodySource::from_forwarded_request_body(body, &request.metadata);
            Self { request, body }
        }
    }

    /// Restores a one-pass typed request body at a trusted proxy boundary.
    ///
    /// The supplied digest must have been authenticated as part of the proxy
    /// envelope. It is reused when the outgoing request is signed because the
    /// same owned chunks cannot be consumed once for hashing and again for
    /// transmission.
    #[doc(hidden)]
    pub fn from_forwarded_streamed_value(
        request: crate::ObjectRpcRequest,
        body: ObjectRpcIncomingBody,
        request_digest_sha256: [u8; 32],
    ) -> Result<Self, NetworkError> {
        if body.is_empty() {
            return Err(NetworkError::Serialization {
                detail: "forwarded streamed request value had no body".to_string(),
            });
        }
        let body = body
            .with_role(ObjectRpcBodyRole::Value)
            .with_value_auth_digest_sha256(Some(request_digest_sha256));
        let body = ObjectRpcBodySource::from_forwarded_request_body(body, &request.metadata);
        Ok(Self { request, body })
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
        if self.body.uses_head_value_intent() {
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

    pub(crate) fn streamed_value(
        request: crate::ObjectRpcRequest,
        body: ObjectRpcIncomingBody,
        value_auth_digest_sha256: Option<[u8; 32]>,
    ) -> Self {
        Self::new(
            request,
            body.with_role(ObjectRpcBodyRole::Value)
                .with_value_auth_digest_sha256(value_auth_digest_sha256),
        )
    }

    pub fn has_streamed_value(&self) -> bool {
        self.body.is_streamed_value()
    }

    pub fn value_auth_digest_sha256(&self) -> Option<&[u8; 32]> {
        self.body.value_auth_digest_sha256()
    }

    /// Decodes the ordinary typed request value without first gathering a
    /// stream-native body into a second full-size byte vector.
    pub async fn decode_body<T>(self) -> Result<T, NetworkError>
    where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        if self.body.is_streamed_value() {
            return self.body.decode_sobj().await;
        }
        if self.body.is_attachment() {
            return Err(NetworkError::Serialization {
                detail: "cannot decode a semantic attachment as the request value".to_string(),
            });
        }
        self.request.decode_body()
    }

    /// Compatibility boundary for services that still require a contiguous
    /// encoded request. Only streamed value bodies are materialized;
    /// attachments remain ownership-native.
    pub async fn materialize_value_body(&mut self, max_bytes: usize) -> Result<(), NetworkError> {
        if !self.body.is_streamed_value() {
            return Ok(());
        }
        let body = std::mem::take(&mut self.body);
        self.request.body = body.collect_to_vec(max_bytes).await?;
        Ok(())
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

    /// Builds a typed value response whose encoded bytes are already laid out
    /// for direct adoption by the owned transport.
    pub fn encode_borrowed_value<T: Serialize + ?Sized>(
        route: impl Into<String>,
        value: &T,
    ) -> Result<Self, NetworkError> {
        let route = route.into();
        Ok(Self {
            response: crate::ObjectRpcResponse::from_encoded_body(route, Vec::new()),
            body: ObjectRpcBodySource::sobj_borrowed_value(value)?,
        })
    }

    /// Builds an owned typed response whose large-value encoder acquires one
    /// transport slab at a time.
    pub fn encode_value<T>(route: impl Into<String>, value: T) -> Result<Self, NetworkError>
    where
        T: Serialize + Send + 'static,
    {
        let route = route.into();
        Ok(Self {
            response: crate::ObjectRpcResponse::from_encoded_body(route, Vec::new()),
            body: ObjectRpcBodySource::sobj_value(value)?,
        })
    }

    /// Builds a typed response from a shared immutable value without cloning
    /// its semantic payload. Each response owns an independent bounded encoder
    /// while retaining only one cheap [`Arc`] reference to the value.
    pub fn encode_shared_value<T>(
        route: impl Into<String>,
        value: Arc<T>,
    ) -> Result<Self, NetworkError>
    where
        T: Serialize + Send + Sync + 'static,
    {
        let route = route.into();
        Ok(Self {
            response: crate::ObjectRpcResponse::from_encoded_body(route, Vec::new()),
            body: ObjectRpcBodySource::sobj_shared_value(value)?,
        })
    }

    #[doc(hidden)]
    pub fn encode_owned_value<T>(route: impl Into<String>, value: T) -> Result<Self, NetworkError>
    where
        T: Serialize + Send + 'static,
    {
        Self::encode_value(route, value)
    }

    /// Explicit compatibility boundary for callers whose contract still
    /// returns one contiguous [`crate::ObjectRpcResponse`].
    pub async fn into_materialized_value_response(
        mut self,
        max_bytes: usize,
    ) -> Result<crate::ObjectRpcResponse, NetworkError> {
        if self.body.is_empty() {
            return Ok(self.response);
        }
        if self.body.role() != ObjectRpcBodyRole::Value {
            return Err(NetworkError::Serialization {
                detail: format!(
                    "object RPC value route {} returned a semantic attachment",
                    self.response.route
                ),
            });
        }
        self.response.body = self.body.collect_to_vec(max_bytes).await?;
        Ok(self.response)
    }

    pub(crate) fn into_stream_native_parts(
        mut self,
    ) -> (
        crate::ObjectRpcResponse,
        ObjectRpcBodyRole,
        ObjectRpcBodySource,
    ) {
        if self.body.uses_head_value_intent() {
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

    pub(crate) fn streamed_value(
        response: crate::ObjectRpcResponse,
        body: ObjectRpcIncomingBody,
    ) -> Self {
        Self::new(response, body.with_role(ObjectRpcBodyRole::Value))
    }

    pub fn has_streamed_value(&self) -> bool {
        self.body.is_streamed_value()
    }

    /// Restores the semantic role of a response body transported through a
    /// trusted proxy envelope.
    #[doc(hidden)]
    pub fn from_forwarded_body(
        response: crate::ObjectRpcResponse,
        body: ObjectRpcIncomingBody,
        streamed_value: bool,
    ) -> Result<Self, NetworkError> {
        if streamed_value {
            if body.is_empty() {
                return Err(NetworkError::Serialization {
                    detail: "forwarded streamed response value had no body".to_string(),
                });
            }
            return Ok(Self::streamed_value(response, body));
        }
        Ok(Self::new(response, body))
    }

    /// Decodes a normal response incrementally when it arrived through the
    /// owned-record path. Error metadata is checked before consuming a body.
    pub async fn decode_body<T>(self) -> Result<T, NetworkError>
    where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        self.response.error_detail()?;
        if self.body.is_streamed_value() {
            return self.body.decode_sobj().await;
        }
        if self.body.is_attachment() {
            return Err(NetworkError::Serialization {
                detail: "cannot decode a semantic attachment as the response value".to_string(),
            });
        }
        self.response.decode_body()
    }

    pub async fn materialize_value_body(&mut self, max_bytes: usize) -> Result<(), NetworkError> {
        if !self.body.is_streamed_value() {
            return Ok(());
        }
        let body = std::mem::take(&mut self.body);
        self.response.body = body.collect_to_vec(max_bytes).await?;
        Ok(())
    }

    /// Explicit compatibility boundary for callers whose public contract still
    /// returns one contiguous [`crate::ObjectRpcResponse`].
    ///
    /// The canonical owned-record path keeps an ordinary typed value in
    /// `self.body`; semantic attachments remain distinct and are never
    /// silently folded into the response value.
    pub async fn into_materialized_value_response(
        mut self,
        max_bytes: usize,
    ) -> Result<crate::ObjectRpcResponse, NetworkError> {
        self.response.error_detail()?;
        if self.body.is_attachment() {
            return Err(NetworkError::Serialization {
                detail: format!(
                    "object RPC value route {} returned a semantic attachment",
                    self.response.route
                ),
            });
        }
        self.materialize_value_body(max_bytes).await?;
        Ok(self.response)
    }

    pub fn into_parts(self) -> (crate::ObjectRpcResponse, ObjectRpcIncomingBody) {
        (self.response, self.body)
    }

    #[doc(hidden)]
    pub fn into_forwarded_response(self) -> Result<ObjectRpcOutgoingResponse, NetworkError> {
        // Preserve the semantic role. `ObjectRpcOutgoingResponse::new` is the
        // public attachment constructor and would relabel an ordinary streamed
        // value while proxying it through another owned RPC.
        Ok(ObjectRpcOutgoingResponse {
            response: self.response,
            body: ObjectRpcBodySource::from_incoming_body(self.body),
        })
    }
}

pub(crate) enum BodyProducer {
    Bytes(BytesBodyProducer),
    Adoptable(AdoptableSobjProducer),
    Deferred(DeferredSobjProducer),
    RepeatedBytes(RepeatedBytesBodyProducer),
    Sync(SyncBodyProducer),
    Async(AsyncBodyProducer),
    Incoming(IncomingBodyProducer),
}

impl BodyProducer {
    pub(crate) async fn next(&mut self) -> Result<Option<Bytes>, NetworkError> {
        match self {
            Self::Bytes(producer) => producer.next(),
            Self::Adoptable(producer) => producer.next(),
            Self::Deferred(_) => Err(NetworkError::Serialization {
                detail: "credit-bounded SOBJ values require the canonical owned-chunk transport"
                    .to_string(),
            }),
            Self::RepeatedBytes(producer) => producer.next(),
            Self::Sync(producer) => producer.next().await,
            Self::Async(producer) => producer.next().await,
            Self::Incoming(producer) => producer.next().await,
        }
    }

    pub(crate) async fn next_owned(
        &mut self,
        allocator: &ChunkAllocator,
    ) -> Result<Option<OwnedChunk>, NetworkError> {
        match self {
            Self::Adoptable(producer) => producer.next_owned(allocator).await,
            Self::Deferred(producer) => producer.next_owned(allocator).await,
            Self::Async(producer) => producer.next_owned(allocator).await,
            Self::Incoming(producer) => producer.next_owned(allocator).await,
            Self::Bytes(producer) => copy_source_chunk_to_owned(producer.next()?, allocator).await,
            Self::RepeatedBytes(producer) => producer.next_owned(allocator).await,
            Self::Sync(producer) => producer.next_owned(allocator).await,
        }
    }

    pub(crate) async fn finish(self) -> Result<(), NetworkError> {
        match self {
            Self::Bytes(_) => Ok(()),
            Self::Adoptable(_) => Ok(()),
            Self::Deferred(producer) => producer.finish().await,
            Self::RepeatedBytes(_) => Ok(()),
            Self::Sync(producer) => producer.finish().await,
            Self::Async(_) => Ok(()),
            Self::Incoming(_) => Ok(()),
        }
    }
}

pub(crate) struct AdoptableSobjProducer {
    buffers: VecDeque<AdoptableSobjBuffer>,
}

impl AdoptableSobjProducer {
    fn next(&mut self) -> Result<Option<Bytes>, NetworkError> {
        let Some(buffer) = self.buffers.pop_front() else {
            return Ok(None);
        };
        let payload = Bytes::copy_from_slice(&buffer.storage[buffer.start..buffer.end]);
        record_shared_payload_copy(payload.len(), PayloadCopyReason::ByteStreamWriteAdapter);
        Ok(Some(payload))
    }

    async fn next_owned(
        &mut self,
        allocator: &ChunkAllocator,
    ) -> Result<Option<OwnedChunk>, NetworkError> {
        let Some(buffer) = self.buffers.pop_front() else {
            return Ok(None);
        };
        allocator
            .adopt_data(buffer.storage, buffer.start, buffer.end)
            .await
            .map(Some)
    }
}

pub(crate) struct DeferredSobjProducer {
    value: Option<Box<dyn DeferredSobjValue>>,
    plan: Arc<SobjEncodingPlan>,
    chunk_bytes: usize,
    receiver: Option<tokio::sync::mpsc::Receiver<Result<OwnedChunk, NetworkError>>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl DeferredSobjProducer {
    fn new(
        value: Box<dyn DeferredSobjValue>,
        plan: Arc<SobjEncodingPlan>,
        max_chunk_bytes: usize,
    ) -> Self {
        Self {
            value: Some(value),
            plan,
            chunk_bytes: max_chunk_bytes.clamp(1, BODY_CHUNK_MAX),
            receiver: None,
            task: None,
        }
    }

    fn start(&mut self, allocator: &ChunkAllocator) -> Result<(), NetworkError> {
        if self.receiver.is_some() {
            return Ok(());
        }
        let value = self
            .value
            .take()
            .ok_or_else(|| body_consumed_error("deferred SOBJ producer was already started"))?;
        let plan = Arc::clone(&self.plan);
        let allocator = allocator.clone();
        let runtime = tokio::runtime::Handle::current();
        let chunk_bytes = self.chunk_bytes;
        // One queued slab plus the encoder's current slab is the complete
        // producer-side residency. The allocator independently enforces the
        // per-connection and process byte ceilings.
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let worker_sender = sender.clone();
        let task = tokio::task::spawn_blocking(move || {
            let mut output =
                OwnedChunkSobjOutput::new(allocator, runtime, chunk_bytes, worker_sender.clone());
            let result = (|| {
                let mut planned = PlannedSobjOutput::new(&mut output, &plan);
                let range = value.encode_into(&mut planned)?;
                if range != (0..plan.total_bytes) {
                    return Err(NetworkError::Serialization {
                        detail: "credit-bounded SOBJ encoder changed its planned range".to_string(),
                    });
                }
                planned.finish()?;
                output.finish(&plan)
            })();
            if let Err(error) = result {
                let error = output.take_error().unwrap_or(error);
                let _ = worker_sender.blocking_send(Err(error));
            }
        });
        drop(sender);
        self.receiver = Some(receiver);
        self.task = Some(task);
        Ok(())
    }

    async fn next_owned(
        &mut self,
        allocator: &ChunkAllocator,
    ) -> Result<Option<OwnedChunk>, NetworkError> {
        self.start(allocator)?;
        self.receiver
            .as_mut()
            .expect("deferred SOBJ receiver was started")
            .recv()
            .await
            .transpose()
    }

    async fn finish(mut self) -> Result<(), NetworkError> {
        drop(self.receiver.take());
        if let Some(task) = self.task.take() {
            task.await.map_err(|source| NetworkError::Io {
                operation: "join credit-bounded SOBJ producer".to_string(),
                detail: source.to_string(),
            })?;
        }
        Ok(())
    }
}

struct OwnedChunkSobjOutput {
    allocator: ChunkAllocator,
    runtime: tokio::runtime::Handle,
    chunk_bytes: usize,
    sender: tokio::sync::mpsc::Sender<Result<OwnedChunk, NetworkError>>,
    current: Option<OwnedChunk>,
    len: usize,
    hasher: blake3::Hasher,
    error: Option<NetworkError>,
}

impl OwnedChunkSobjOutput {
    fn new(
        allocator: ChunkAllocator,
        runtime: tokio::runtime::Handle,
        chunk_bytes: usize,
        sender: tokio::sync::mpsc::Sender<Result<OwnedChunk, NetworkError>>,
    ) -> Self {
        Self {
            allocator,
            runtime,
            chunk_bytes,
            sender,
            current: None,
            len: 0,
            hasher: blake3::Hasher::new(),
            error: None,
        }
    }

    fn fail(&mut self, error: NetworkError) -> sinomo_object::Error {
        let detail = error.to_string();
        self.error = Some(error);
        sinomo_object::Error::Serde(detail)
    }

    fn ensure_chunk(&mut self) -> sinomo_object::Result<()> {
        if self.current.is_none() {
            let allocator = self.allocator.clone();
            let chunk_bytes = self.chunk_bytes;
            let chunk = self
                .runtime
                .block_on(allocator.allocate_data(chunk_bytes))
                .map_err(|error| self.fail(error))?;
            self.current = Some(chunk);
        }
        Ok(())
    }

    fn flush(&mut self) -> sinomo_object::Result<()> {
        let Some(chunk) = self.current.take() else {
            return Ok(());
        };
        if chunk.len() == 0 {
            return Ok(());
        }
        self.sender.blocking_send(Ok(chunk)).map_err(|_| {
            self.fail(NetworkError::delivery_failed(
                "emit credit-bounded SOBJ chunk",
                NetworkDeliveryState::PossiblyApplied,
                "owned body consumer closed",
            ))
        })
    }

    fn finish(&mut self, plan: &SobjEncodingPlan) -> Result<(), NetworkError> {
        self.flush().map_err(NetworkError::object)?;
        if self.len != plan.total_bytes || self.hasher.finalize().as_bytes() != &plan.digest {
            return Err(NetworkError::Serialization {
                detail: "credit-bounded SOBJ bytes changed after authentication planning"
                    .to_string(),
            });
        }
        Ok(())
    }

    fn take_error(&mut self) -> Option<NetworkError> {
        self.error.take()
    }
}

impl SobjOutput for OwnedChunkSobjOutput {
    fn len(&self) -> usize {
        self.len
    }

    fn append(&mut self, mut bytes: &[u8], _context: &'static str) -> sinomo_object::Result<()> {
        while !bytes.is_empty() {
            self.ensure_chunk()?;
            let chunk = self.current.as_mut().expect("SOBJ output chunk exists");
            let written = chunk.writable_payload().len().min(bytes.len());
            chunk.writable_payload()[..written].copy_from_slice(&bytes[..written]);
            chunk
                .commit_payload(written)
                .map_err(|error| self.fail(error))?;
            self.hasher.update(&bytes[..written]);
            self.len = self
                .len
                .checked_add(written)
                .ok_or(sinomo_object::Error::SizeOverflow)?;
            bytes = &bytes[written..];
            if self
                .current
                .as_mut()
                .is_some_and(|chunk| chunk.writable_payload().is_empty())
            {
                self.flush()?;
            }
        }
        Ok(())
    }

    fn patch(&mut self, _offset: usize, _bytes: &[u8]) -> sinomo_object::Result<()> {
        Err(sinomo_object::Error::Serde(
            "credit-bounded SOBJ output received a backpatch".to_string(),
        ))
    }

    fn truncate(&mut self, _len: usize) {
        self.error
            .get_or_insert_with(|| NetworkError::Serialization {
                detail: "credit-bounded SOBJ encoder attempted to roll back already emitted chunks"
                    .to_string(),
            });
        self.current = None;
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

    async fn next_owned(
        &mut self,
        allocator: &ChunkAllocator,
    ) -> Result<Option<OwnedChunk>, NetworkError> {
        if self.remaining_bytes == 0 {
            return Ok(None);
        }
        let bytes = self
            .chunk_bytes
            .min(usize::try_from(self.remaining_bytes).unwrap_or(usize::MAX));
        let mut chunk = allocator.allocate_data(bytes).await?;
        let mut written = 0;
        while written < bytes {
            let available = self.pattern.len() - self.pattern_offset;
            let copied = available.min(bytes - written);
            chunk.writable_payload()[written..written + copied]
                .copy_from_slice(&self.pattern[self.pattern_offset..self.pattern_offset + copied]);
            written += copied;
            self.pattern_offset += copied;
            if self.pattern_offset == self.pattern.len() {
                self.pattern_offset = 0;
            }
        }
        chunk.commit_payload(bytes)?;
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

    async fn next_owned(
        &mut self,
        allocator: &ChunkAllocator,
    ) -> Result<Option<OwnedChunk>, NetworkError> {
        let mut chunk = allocator.allocate_data(self.chunk_bytes).await?;
        let read = self
            .reader
            .read(chunk.writable_payload())
            .await
            .map_err(|source| NetworkError::Io {
                operation: "read asynchronous object RPC body source into pooled slab".to_string(),
                detail: source.to_string(),
            })?;
        if read == 0 {
            return Ok(None);
        }
        chunk.commit_payload(read)?;
        Ok(Some(chunk))
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
            .map(|chunk| chunk.map(ObjectRpcBodyChunk::into_bytes))
            .map_err(|source| NetworkError::Io {
                operation: "read ownership-moving object RPC body source".to_string(),
                detail: source.to_string(),
            })
    }

    async fn next_owned(
        &mut self,
        allocator: &ChunkAllocator,
    ) -> Result<Option<OwnedChunk>, NetworkError> {
        let chunk = self
            .stream
            .next_chunk(self.chunk_bytes)
            .await
            .map_err(|source| NetworkError::Io {
                operation: "read owned object RPC body source".to_string(),
                detail: source.to_string(),
            })?;
        match chunk {
            Some(chunk) => {
                if let Some(slab_id) = chunk.pooled_slab_id() {
                    observe_chunk_identity(slab_id, "application-body-forwarding");
                }
                chunk.into_owned(allocator).await.map(Some)
            }
            None => Ok(None),
        }
    }
}

async fn copy_source_chunk_to_owned(
    bytes: Option<Bytes>,
    allocator: &ChunkAllocator,
) -> Result<Option<OwnedChunk>, NetworkError> {
    let Some(bytes) = bytes else {
        return Ok(None);
    };
    let mut chunk = allocator.allocate_data(bytes.len()).await?;
    chunk.copy_payload_from(&bytes, PayloadCopyReason::SharedSourceNormalization)?;
    Ok(Some(chunk))
}

pub(crate) struct SyncBodyProducer {
    state: SyncBodyProducerState,
    chunk_bytes: usize,
    _lifetime_guards: Arc<Mutex<Vec<Box<dyn Any + Send>>>>,
}

enum SyncBodyProducerState {
    Idle(Option<Box<dyn Read + Send>>),
    Bytes {
        receiver: tokio::sync::mpsc::Receiver<Result<Bytes, NetworkError>>,
        task: tokio::task::JoinHandle<()>,
    },
    Owned {
        receiver: tokio::sync::mpsc::Receiver<Result<OwnedChunk, NetworkError>>,
        task: tokio::task::JoinHandle<()>,
    },
    Finished,
}

impl SyncBodyProducer {
    fn spawn(
        reader: Box<dyn Read + Send>,
        max_chunk_bytes: usize,
        lifetime_guards: Arc<Mutex<Vec<Box<dyn Any + Send>>>>,
    ) -> Result<Self, NetworkError> {
        Ok(Self {
            state: SyncBodyProducerState::Idle(Some(reader)),
            chunk_bytes: max_chunk_bytes.max(1),
            _lifetime_guards: lifetime_guards,
        })
    }

    async fn next(&mut self) -> Result<Option<Bytes>, NetworkError> {
        if matches!(self.state, SyncBodyProducerState::Idle(_)) {
            self.start_bytes_worker()?;
        }
        match &mut self.state {
            SyncBodyProducerState::Bytes { receiver, .. } => receiver.recv().await.transpose(),
            SyncBodyProducerState::Owned { .. } => Err(NetworkError::Serialization {
                detail: "synchronous body producer cannot switch from owned chunks to byte buffers"
                    .to_string(),
            }),
            SyncBodyProducerState::Finished => Ok(None),
            SyncBodyProducerState::Idle(_) => unreachable!("byte worker was started"),
        }
    }

    async fn next_owned(
        &mut self,
        allocator: &ChunkAllocator,
    ) -> Result<Option<OwnedChunk>, NetworkError> {
        if matches!(self.state, SyncBodyProducerState::Idle(_)) {
            self.start_owned_worker(allocator)?;
        }
        match &mut self.state {
            SyncBodyProducerState::Owned { receiver, .. } => receiver.recv().await.transpose(),
            SyncBodyProducerState::Bytes { .. } => Err(NetworkError::Serialization {
                detail: "synchronous body producer cannot switch from byte buffers to owned chunks"
                    .to_string(),
            }),
            SyncBodyProducerState::Finished => Ok(None),
            SyncBodyProducerState::Idle(_) => unreachable!("owned worker was started"),
        }
    }

    fn take_reader(&mut self) -> Result<Box<dyn Read + Send>, NetworkError> {
        let SyncBodyProducerState::Idle(reader) = &mut self.state else {
            return Err(NetworkError::Serialization {
                detail: "synchronous body reader worker was already started".to_string(),
            });
        };
        reader
            .take()
            .ok_or_else(|| body_consumed_error("synchronous body reader was already consumed"))
    }

    fn start_bytes_worker(&mut self) -> Result<(), NetworkError> {
        let mut reader = self.take_reader()?;
        let chunk_bytes = self.chunk_bytes;
        let lifetime_guards = Arc::clone(&self._lifetime_guards);
        let (sender, receiver) = tokio::sync::mpsc::channel(BODY_READER_BUFFER_DEPTH);
        let task = tokio::task::spawn_blocking(move || {
            let _lifetime_guards = lifetime_guards;
            loop {
                let mut bytes = pooled_body_buffer(chunk_bytes);
                let read = match read_sync_body_chunk(&mut reader, &mut bytes) {
                    Ok(0) => return,
                    Ok(read) => read,
                    Err(error) => {
                        let _ = sender.blocking_send(Err(error));
                        return;
                    }
                };
                bytes.truncate(read);
                if sender.blocking_send(Ok(pooled_body_bytes(bytes))).is_err() {
                    return;
                }
            }
        });
        self.state = SyncBodyProducerState::Bytes { receiver, task };
        Ok(())
    }

    fn start_owned_worker(&mut self, allocator: &ChunkAllocator) -> Result<(), NetworkError> {
        let mut reader = self.take_reader()?;
        let allocator = allocator.clone();
        let runtime = tokio::runtime::Handle::current();
        let chunk_bytes = self.chunk_bytes;
        let lifetime_guards = Arc::clone(&self._lifetime_guards);
        let (sender, receiver) = tokio::sync::mpsc::channel(BODY_READER_BUFFER_DEPTH);
        let task = tokio::task::spawn_blocking(move || {
            let _lifetime_guards = lifetime_guards;
            loop {
                let mut chunk = match runtime.block_on(allocator.allocate_data(chunk_bytes)) {
                    Ok(chunk) => chunk,
                    Err(error) => {
                        let _ = sender.blocking_send(Err(error));
                        return;
                    }
                };
                let read = match read_sync_body_chunk(&mut reader, chunk.writable_payload()) {
                    Ok(0) => return,
                    Ok(read) => read,
                    Err(error) => {
                        let _ = sender.blocking_send(Err(error));
                        return;
                    }
                };
                if let Err(error) = chunk.commit_payload(read) {
                    let _ = sender.blocking_send(Err(error));
                    return;
                }
                if sender.blocking_send(Ok(chunk)).is_err() {
                    return;
                }
            }
        });
        self.state = SyncBodyProducerState::Owned { receiver, task };
        Ok(())
    }

    async fn finish(mut self) -> Result<(), NetworkError> {
        let state = std::mem::replace(&mut self.state, SyncBodyProducerState::Finished);
        let task = match state {
            SyncBodyProducerState::Bytes { receiver, task } => {
                drop(receiver);
                Some(task)
            }
            SyncBodyProducerState::Owned { receiver, task } => {
                drop(receiver);
                Some(task)
            }
            SyncBodyProducerState::Idle(_) | SyncBodyProducerState::Finished => None,
        };
        if let Some(task) = task {
            task.await.map_err(|source| NetworkError::Io {
                operation: "join object RPC body producer".to_string(),
                detail: source.to_string(),
            })?;
        }
        Ok(())
    }
}

fn read_sync_body_chunk(
    reader: &mut Box<dyn Read + Send>,
    buffer: &mut [u8],
) -> Result<usize, NetworkError> {
    loop {
        match reader.read(buffer) {
            Ok(read) => return Ok(read),
            Err(source) if source.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(source) => {
                return Err(NetworkError::Io {
                    operation: "read synchronous object RPC body source".to_string(),
                    detail: source.to_string(),
                });
            }
        }
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
        record_shared_payload_copy(read, PayloadCopyReason::ContiguousBodyMaterialization);
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
        record_shared_payload_copy(read, PayloadCopyReason::ContiguousBodyMaterialization);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::ser::SerializeMap;

    #[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
    struct StreamedFixture {
        label: String,
        payload: Vec<u8>,
        values: Vec<u64>,
    }

    #[derive(Debug)]
    struct DecodeThreadProbe(std::thread::ThreadId);

    impl<'de> Deserialize<'de> for DecodeThreadProbe {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            let _value = String::deserialize(deserializer)?;
            Ok(Self(std::thread::current().id()))
        }
    }

    #[test]
    fn small_sobj_source_leaves_unused_transport_tail_uninitialized() {
        let source =
            ObjectRpcBodySource::sobj_value(&"small typed value").expect("encode SOBJ source");
        let ObjectRpcBodySourceKind::AdoptableSobj(source) = &source.kind else {
            panic!("typed SOBJ source was not adoptable");
        };
        let buffers = source.buffers.lock().expect("SOBJ buffer lock");
        let buffers = buffers.as_ref().expect("SOBJ buffers");
        assert_eq!(buffers.len(), 1);
        let buffer = buffers.front().expect("SOBJ buffer");

        assert_eq!(buffer.storage.len(), buffer.end);
        assert!(buffer.storage.capacity() >= SECURE_WIRE_MAX);
        assert!(buffer.storage.len() < SECURE_WIRE_MAX);
        assert_eq!(buffer.start, CHUNK_HEADROOM_BYTES);
    }

    #[test]
    fn small_owned_sobj_keeps_the_single_record_adoptable_path() {
        let source = ObjectRpcBodySource::sobj_owned("small owned value".to_string())
            .expect("encode owned SOBJ source");
        assert!(matches!(
            &source.kind,
            ObjectRpcBodySourceKind::AdoptableSobj(_)
        ));
    }

    struct UnknownLengthMap {
        payload: Vec<u8>,
    }

    impl Serialize for UnknownLengthMap {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            let mut map = serializer.serialize_map(None)?;
            map.serialize_entry("payload", &self.payload)?;
            map.end()
        }
    }

    #[tokio::test]
    async fn large_owned_sobj_fills_credit_bounded_slabs_without_payload_copies() {
        let payload = (0..(BODY_CHUNK_MAX * 4 + 311))
            .map(|offset| (offset % 251) as u8)
            .collect::<Vec<_>>();
        let expected = crate::SinomoObjectCodec::network_frame()
            .encode(&UnknownLengthMap {
                payload: payload.clone(),
            })
            .expect("encode contiguous reference");
        let source = ObjectRpcBodySource::sobj_owned(UnknownLengthMap { payload })
            .expect("plan deferred SOBJ source");
        assert!(matches!(
            &source.kind,
            ObjectRpcBodySourceKind::DeferredSobj(_)
        ));
        assert_eq!(source.total_bytes(), Some(expected.len() as u64));
        assert_eq!(
            source.known_digest(),
            Some(*blake3::hash(&expected).as_bytes())
        );

        let pool = crate::owned_chunk::ChunkPool::new(8 * SECURE_WIRE_MAX, SECURE_WIRE_MAX, 0, 0);
        let allocator = ChunkAllocator::new(pool, 2 * SECURE_WIRE_MAX, SECURE_WIRE_MAX);
        let mut producer = source.producer(BODY_CHUNK_MAX).expect("open producer");
        let mut actual = Vec::with_capacity(expected.len());
        let mut chunks = 0;
        while let Some(chunk) = producer
            .next_owned(&allocator)
            .await
            .expect("encode owned SOBJ chunk")
        {
            chunks += 1;
            actual.extend_from_slice(chunk.as_slice());
        }
        producer.finish().await.expect("finish producer");

        assert!(chunks > 2);
        assert_eq!(actual, expected);
        assert_eq!(allocator.metrics().payload_copy_bytes, 0);
        assert!(
            allocator.metrics().peak_active_bytes <= 2 * SECURE_WIRE_MAX,
            "producer exceeded its current-plus-queued slab budget: {:?}",
            allocator.metrics()
        );
        let decoded: BTreeMap<String, Vec<u8>> = crate::SinomoObjectCodec::network_frame()
            .decode(&actual)
            .expect("decode streamed SOBJ");
        assert_eq!(
            decoded.get("payload").map(Vec::len),
            Some(BODY_CHUNK_MAX * 4 + 311)
        );
    }

    #[tokio::test]
    async fn shared_large_sobj_fanout_retains_one_value_and_encodes_without_payload_copies() {
        let value = Arc::new(UnknownLengthMap {
            payload: (0..(BODY_CHUNK_MAX * 3 + 173))
                .map(|offset| (offset % 239) as u8)
                .collect(),
        });
        let expected = crate::SinomoObjectCodec::network_frame()
            .encode(value.as_ref())
            .expect("encode contiguous reference");
        let call_a = ObjectRpcCall::encode_shared_value(
            "/test/shared-fanout-a",
            "test.shared-fanout",
            Arc::clone(&value),
        )
        .expect("build first shared call");
        let response_b = ObjectRpcOutgoingResponse::encode_shared_value(
            "/test/shared-fanout-b",
            Arc::clone(&value),
        )
        .expect("build second shared response");
        assert_eq!(
            Arc::strong_count(&value),
            3,
            "each body source should retain only its Arc reference"
        );
        assert!(matches!(
            &call_a.body.kind,
            ObjectRpcBodySourceKind::DeferredSobj(_)
        ));
        assert!(matches!(
            &response_b.body.kind,
            ObjectRpcBodySourceKind::DeferredSobj(_)
        ));

        let pool = crate::owned_chunk::ChunkPool::new(8 * SECURE_WIRE_MAX, SECURE_WIRE_MAX, 0, 0);
        let allocator = ChunkAllocator::new(pool, 2 * SECURE_WIRE_MAX, SECURE_WIRE_MAX);
        for source in [call_a.body, response_b.body] {
            let mut producer = source.producer(BODY_CHUNK_MAX).expect("open producer");
            let mut actual = Vec::with_capacity(expected.len());
            while let Some(chunk) = producer
                .next_owned(&allocator)
                .await
                .expect("encode shared SOBJ chunk")
            {
                actual.extend_from_slice(chunk.as_slice());
            }
            producer.finish().await.expect("finish shared producer");
            assert_eq!(actual, expected);
        }

        assert_eq!(Arc::strong_count(&value), 1);
        assert_eq!(allocator.metrics().payload_copy_bytes, 0);
        assert!(
            allocator.metrics().peak_active_bytes <= 2 * SECURE_WIRE_MAX,
            "shared producer exceeded its current-plus-queued slab budget: {:?}",
            allocator.metrics()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn single_record_sobj_decodes_in_the_caller_task() {
        let encoded = crate::SinomoObjectCodec::network_frame()
            .encode(&"single record")
            .expect("encode probe");
        let caller_thread = std::thread::current().id();
        let decoded = ObjectRpcIncomingBody::bytes(encoded)
            .decode_sobj::<DecodeThreadProbe>()
            .await
            .expect("decode probe");

        assert_eq!(decoded.0, caller_thread);
    }

    #[tokio::test]
    async fn incremental_sobj_decode_releases_bounded_chunk_ring_credit() {
        let expected = StreamedFixture {
            label: "chunk-crossing".repeat(31),
            payload: (0..(512 * 1024 + 73))
                .map(|index| (index % 251) as u8)
                .collect(),
            values: (0..4097).collect(),
        };
        let encoded = crate::SinomoObjectCodec::network_frame()
            .encode(&expected)
            .expect("encode fixture");
        let total_bytes = encoded.len() as u64;
        let (mut sender, body) =
            ObjectRpcIncomingBody::multiplexed_chunk_ring_with_depth(Some(total_bytes), 1);
        let producer = tokio::spawn(async move {
            for chunk in encoded.chunks(997) {
                assert!(sender.send(chunk.to_vec()).await.expect("send chunk"));
            }
            sender.finish(total_bytes).await.expect("finish body");
        });

        let decoded = body
            .with_role(ObjectRpcBodyRole::Value)
            .decode_sobj::<StreamedFixture>()
            .await
            .expect("decode streamed fixture");
        producer.await.expect("producer task");
        assert_eq!(decoded, expected);
    }

    #[tokio::test]
    async fn exclusive_incoming_chunk_transfers_to_outgoing_producer_without_copy() {
        let allocator = ChunkAllocator::default();
        let payload = vec![0xa5; 32 * 1024 + 17];
        let mut chunk = allocator
            .allocate_data(payload.len())
            .await
            .expect("allocate incoming chunk");
        chunk
            .copy_payload_from(&payload, PayloadCopyReason::SharedSourceNormalization)
            .expect("fill incoming chunk");
        let slab_id = chunk.slab_id();
        let incoming = ObjectRpcIncomingBody {
            inner: ObjectRpcIncomingBodyInner::Chunk(BodyChunkCursor::new(
                ObjectRpcBodyChunk::lease(chunk.into_lease()),
            )),
            total_bytes: Some(payload.len() as u64),
            role: ObjectRpcBodyRole::Attachment,
            value_auth_digest_sha256: None,
            multiplexed_terminal: None,
        };
        let source = ObjectRpcBodySource::from_incoming_body(incoming);
        let mut producer = source
            .producer(BODY_CHUNK_MAX)
            .expect("open forwarded producer");

        let forwarded = producer
            .next_owned(&allocator)
            .await
            .expect("read forwarded chunk")
            .expect("forwarded chunk");

        assert_eq!(forwarded.slab_id(), slab_id);
        assert_eq!(forwarded.as_slice(), payload);
        assert!(
            producer
                .next_owned(&allocator)
                .await
                .expect("finish forwarded body")
                .is_none()
        );
    }

    #[tokio::test]
    async fn synchronous_reader_prefetches_directly_into_owned_slabs_without_payload_copy() {
        let pool = crate::owned_chunk::ChunkPool::new(8 * SECURE_WIRE_MAX, SECURE_WIRE_MAX, 0, 0);
        let allocator = ChunkAllocator::new(pool, 2 * SECURE_WIRE_MAX, SECURE_WIRE_MAX);
        let expected = (0..(BODY_CHUNK_MAX * 3 + 97))
            .map(|offset| (offset % 251) as u8)
            .collect::<Vec<_>>();
        let source = ObjectRpcBodySource::from_reader(
            std::io::Cursor::new(expected.clone()),
            Some(expected.len() as u64),
        );
        let mut producer = source.producer(BODY_CHUNK_MAX).expect("open producer");
        let mut actual = Vec::with_capacity(expected.len());

        while let Some(chunk) = producer
            .next_owned(&allocator)
            .await
            .expect("read owned chunk")
        {
            actual.extend_from_slice(chunk.as_slice());
        }
        producer.finish().await.expect("finish producer");

        assert_eq!(actual, expected);
        assert_eq!(allocator.metrics().payload_copy_bytes, 0);
    }

    #[tokio::test]
    async fn generated_repeated_bytes_fill_owned_slabs_without_normalization_copy() {
        let pool = crate::owned_chunk::ChunkPool::new(8 * SECURE_WIRE_MAX, SECURE_WIRE_MAX, 0, 0);
        let allocator = ChunkAllocator::new(pool, 2 * SECURE_WIRE_MAX, SECURE_WIRE_MAX);
        let expected_bytes = BODY_CHUNK_MAX * 2 + 97;
        let source = ObjectRpcBodySource::zeroes(expected_bytes as u64);
        let mut producer = source.producer(BODY_CHUNK_MAX).expect("open producer");
        let mut actual_bytes = 0;

        while let Some(chunk) = producer
            .next_owned(&allocator)
            .await
            .expect("fill generated owned chunk")
        {
            assert!(chunk.as_slice().iter().all(|byte| *byte == 0));
            actual_bytes += chunk.len();
        }
        producer.finish().await.expect("finish producer");

        assert_eq!(actual_bytes, expected_bytes);
        assert_eq!(allocator.metrics().payload_copy_bytes, 0);
    }

    #[tokio::test]
    async fn shared_bytes_source_normalizes_into_owned_slabs_exactly_once() {
        let pool = crate::owned_chunk::ChunkPool::new(8 * SECURE_WIRE_MAX, SECURE_WIRE_MAX, 0, 0);
        let allocator = ChunkAllocator::new(pool, 2 * SECURE_WIRE_MAX, SECURE_WIRE_MAX);
        let expected = (0..(BODY_CHUNK_MAX + 97))
            .map(|offset| (offset % 251) as u8)
            .collect::<Vec<_>>();
        let source = ObjectRpcBodySource::bytes(expected.clone());
        let mut producer = source.producer(BODY_CHUNK_MAX).expect("open producer");
        let mut actual = Vec::with_capacity(expected.len());

        while let Some(chunk) = producer
            .next_owned(&allocator)
            .await
            .expect("read owned chunk")
        {
            actual.extend_from_slice(chunk.as_slice());
        }
        producer.finish().await.expect("finish producer");

        assert_eq!(actual, expected);
        assert_eq!(
            allocator.metrics().payload_copy_bytes,
            expected.len() as u64
        );
    }
}
