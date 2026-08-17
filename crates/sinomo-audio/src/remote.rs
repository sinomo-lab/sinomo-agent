//! Audio devices attached to another node, reached over Sinomo's object-RPC path.
//!
//! These implement the same [`RealtimeAudioSource`] and [`RealtimeAudioSink`] traits as local
//! devices, so a call path drives a microphone three rooms away exactly as it drives one on this
//! machine.
//!
//! Transport is injected rather than imported: this crate states what call it needs and an
//! application supplies a cluster-routed implementation. That keeps device access on the sanctioned
//! networking path without pulling daemon routing into an R2 crate.

use crate::service::{
    MEDIA_AUDIO_CAPTURE_ROUTE, MEDIA_AUDIO_PLAYBACK_ROUTE, MEDIA_RPC_CATEGORY,
    MediaAudioCaptureRequest, MediaAudioPlaybackRequest,
};
use crate::{
    NativeAudioChunk, NativeAudioError, NativeAudioSampleFormat, RealtimeAudioSink,
    RealtimeAudioSource,
};
use sinomo_runtime::network::{ObjectRpcBodySource, ObjectRpcCall, ObjectRpcRequest};
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, sync_channel};
use std::time::Duration;

/// How many chunks may queue between the network task and the call path before it applies
/// backpressure.
const REMOTE_QUEUE_CAPACITY: usize = 32;

pub type MediaTransportFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, String>> + Send + 'a>>;

/// One streamed object-RPC call issued to a named node.
///
/// The reply carries the response body incrementally so a capture stream stays one call for its
/// whole lifetime rather than a sequence of polls.
pub trait MediaObjectRpcTransport: Send + Sync {
    fn call_streaming<'a>(
        &'a self,
        node_id: &'a str,
        call: ObjectRpcCall,
    ) -> MediaTransportFuture<'a, MediaTransportReply>;
}

/// The reply half of a streamed media call.
pub struct MediaTransportReply {
    /// Yields the next body chunk, or `None` once the peer ends the stream.
    pub next_chunk: Box<dyn FnMut() -> MediaTransportFuture<'static, Option<Vec<u8>>> + Send>,
}

/// Bytes one capture chunk carries at a given format and duration.
pub fn capture_chunk_bytes(
    sample_rate_hz: u32,
    sample_format: NativeAudioSampleFormat,
    chunk_ms: u64,
) -> usize {
    let frames = (u64::from(sample_rate_hz).saturating_mul(chunk_ms) / 1_000).max(1);
    (frames as usize).saturating_mul(sample_format.bytes_per_sample())
}

/// Builds the capture call for a microphone attached to another node.
pub fn capture_call(request: &MediaAudioCaptureRequest) -> Result<ObjectRpcCall, NativeAudioError> {
    let request =
        ObjectRpcRequest::encode_body(MEDIA_AUDIO_CAPTURE_ROUTE, MEDIA_RPC_CATEGORY, request)
            .map_err(|error| NativeAudioError::AudioStream {
                detail: format!("encode capture request: {error}"),
            })?;
    Ok(ObjectRpcCall::value(request))
}

/// Builds the playback call for a speaker attached to another node.
///
/// The PCM travels as the request body, so the call is created with a streaming body source.
pub fn playback_call(
    request: &MediaAudioPlaybackRequest,
    body: ObjectRpcBodySource,
) -> Result<ObjectRpcCall, NativeAudioError> {
    let request =
        ObjectRpcRequest::encode_body(MEDIA_AUDIO_PLAYBACK_ROUTE, MEDIA_RPC_CATEGORY, request)
            .map_err(|error| NativeAudioError::AudioStream {
                detail: format!("encode playback request: {error}"),
            })?;
    Ok(ObjectRpcCall::new(request, body))
}

/// Reassembles a byte stream into fixed-size capture chunks.
///
/// The wire carries PCM without frame boundaries, so the negotiated chunk size is what turns it
/// back into the chunks a call path expects.
pub struct CaptureChunkAssembler {
    chunk_bytes: usize,
    sample_rate_hz: u32,
    sample_format: NativeAudioSampleFormat,
    pending: Vec<u8>,
    sequence: u64,
}

impl CaptureChunkAssembler {
    pub fn new(sample_rate_hz: u32, sample_format: NativeAudioSampleFormat, chunk_ms: u64) -> Self {
        Self {
            chunk_bytes: capture_chunk_bytes(sample_rate_hz, sample_format, chunk_ms),
            sample_rate_hz,
            sample_format,
            pending: Vec::new(),
            sequence: 0,
        }
    }

    /// Absorbs received bytes and returns every whole chunk they completed.
    pub fn push(&mut self, bytes: &[u8]) -> Vec<NativeAudioChunk> {
        self.pending.extend_from_slice(bytes);
        let mut chunks = Vec::new();
        while self.pending.len() >= self.chunk_bytes {
            let audio = self.pending.drain(..self.chunk_bytes).collect::<Vec<u8>>();
            chunks.push(self.chunk(audio));
        }
        chunks
    }

    /// Returns whatever remains once the stream ends, so a final short chunk is not dropped.
    pub fn flush(&mut self) -> Option<NativeAudioChunk> {
        if self.pending.is_empty() {
            return None;
        }
        let audio = std::mem::take(&mut self.pending);
        Some(self.chunk(audio))
    }

    fn chunk(&mut self, audio: Vec<u8>) -> NativeAudioChunk {
        let sequence = self.sequence;
        self.sequence = self.sequence.saturating_add(1);
        NativeAudioChunk {
            sequence,
            sample_rate_hz: self.sample_rate_hz,
            sample_format: self.sample_format,
            channels: 1,
            captured_at_unix_ms: 0,
            audio,
        }
    }
}

/// Streams queued PCM frames as a request body.
///
/// Playback sends media out as the request body of one long-lived call, so the frames a call path
/// writes have to appear to the RPC layer as a byte stream it can read incrementally.
pub struct PcmFrameReader {
    frames: tokio::sync::mpsc::Receiver<Vec<u8>>,
    pending: Vec<u8>,
    offset: usize,
}

impl PcmFrameReader {
    pub fn new(frames: tokio::sync::mpsc::Receiver<Vec<u8>>) -> Self {
        Self {
            frames,
            pending: Vec::new(),
            offset: 0,
        }
    }
}

impl tokio::io::AsyncRead for PcmFrameReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use std::task::Poll;
        loop {
            if self.offset < self.pending.len() {
                let take = (self.pending.len() - self.offset).min(buf.remaining());
                let offset = self.offset;
                let chunk = self.pending[offset..offset + take].to_vec();
                buf.put_slice(&chunk);
                self.offset += take;
                return Poll::Ready(Ok(()));
            }
            match self.frames.poll_recv(cx) {
                // A closed queue ends the body, which ends the playback call cleanly.
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Ready(Some(frame)) => {
                    self.pending = frame;
                    self.offset = 0;
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// A microphone attached to another node.
pub struct RemoteAudioSource {
    chunks: Mutex<Receiver<Result<NativeAudioChunk, String>>>,
}

impl RemoteAudioSource {
    /// Creates a source fed by an already-running receive task.
    ///
    /// The task is expected to send assembled chunks and drop the sender when the stream ends.
    pub fn from_channel(chunks: Receiver<Result<NativeAudioChunk, String>>) -> Self {
        Self {
            chunks: Mutex::new(chunks),
        }
    }

    /// Creates the source and the sender a receive task feeds.
    pub fn channel() -> (Self, SyncSender<Result<NativeAudioChunk, String>>) {
        let (sender, receiver) = sync_channel(REMOTE_QUEUE_CAPACITY);
        (Self::from_channel(receiver), sender)
    }
}

impl RealtimeAudioSource for RemoteAudioSource {
    fn recv_chunk(&self, timeout: Duration) -> Result<Option<NativeAudioChunk>, NativeAudioError> {
        let chunks = self
            .chunks
            .lock()
            .map_err(|_| NativeAudioError::AudioStream {
                detail: "remote capture receiver lock poisoned".to_string(),
            })?;
        match chunks.recv_timeout(timeout) {
            Ok(Ok(chunk)) => Ok(Some(chunk)),
            Ok(Err(detail)) => Err(NativeAudioError::AudioStream { detail }),
            Err(RecvTimeoutError::Timeout) => Ok(None),
            // A closed stream is not an error: the serving node ended the capture.
            Err(RecvTimeoutError::Disconnected) => Ok(None),
        }
    }
}

/// A speaker attached to another node.
pub struct RemoteAudioSink {
    frames: SyncSender<Vec<u8>>,
}

impl RemoteAudioSink {
    /// Creates a sink that hands frames to an already-running send task.
    pub fn from_channel(frames: SyncSender<Vec<u8>>) -> Self {
        Self { frames }
    }

    /// Creates the sink and the receiver a send task drains into the request body.
    pub fn channel() -> (Self, Receiver<Vec<u8>>) {
        let (sender, receiver) = sync_channel(REMOTE_QUEUE_CAPACITY);
        (Self::from_channel(sender), receiver)
    }
}

impl RealtimeAudioSink for RemoteAudioSink {
    fn write_chunk(&self, bytes: &[u8]) -> Result<(), NativeAudioError> {
        self.frames
            .send(bytes.to_vec())
            .map_err(|_| NativeAudioError::AudioStream {
                detail: "the remote speaker stream is closed".to_string(),
            })
    }
}

#[cfg(test)]
#[path = "remote_tests.rs"]
mod tests;
