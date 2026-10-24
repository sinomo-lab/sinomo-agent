//! Continuous video over Sinomo's streaming object-RPC media path. Both App Use
//! control modes use this framing and the same encoder as RealtimeSession.
use serde::{Deserialize, Serialize};
use sinomo_lm::{
    RealtimeEncodedVideoFrameInput, RealtimeVideoFrameFormat, RealtimeVideoFrameInput,
    RealtimeVideoInputConfig, RealtimeVideoPixelFormat,
};
use sinomo_runtime::device::desktop::DesktopFrameSource;
use sinomo_runtime::device::{NativeVideoSample, NativeVideoSampleFormat};
use sinomo_runtime::network::ObjectRpcBodySource;
use std::io;
use std::pin::Pin;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};

pub const VIDEO_STREAM_VERSION: u32 = 1;
pub const MAX_ENCODED_FRAME_BYTES: usize = 32 * 1024 * 1024;
pub const MAX_METADATA_BYTES: usize = 4096;

/// A record is u32 LE metadata length, JSON metadata, u32 LE image length,
/// encoded image. The outer object RPC owns routing, authorization and framing.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct VideoFrameMetadata {
    pub sequence: u64,
    pub timestamp_ns: u64,
    pub duration_ns: u64,
    pub captured_at_unix_ms: u128,
    pub width: u32,
    pub height: u32,
    pub source_width: u32,
    pub source_height: u32,
    pub format: RealtimeVideoFrameFormat,
    #[serde(default)]
    pub coordinate_grid: bool,
}

/// BGRA desktop textures go directly into the existing media encoder. Other
/// native sample formats use the existing video conversion path.
pub fn realtime_frame(sample: &NativeVideoSample) -> Result<RealtimeVideoFrameInput, String> {
    sample.validate().map_err(|e| e.to_string())?;
    let mut frame =
        if sample.format == NativeVideoSampleFormat::Bgra8 && sample.planes[0].offset == 0 {
            RealtimeVideoFrameInput::new(
                sample.data.clone(),
                RealtimeVideoPixelFormat::Bgra8,
                sample.width,
                sample.height,
                sample.planes[0].stride as usize,
                sample.sequence,
            )
        } else {
            let surface =
                crate::native_video_sample_to_surface_frame(sample).map_err(|e| e.to_string())?;
            RealtimeVideoFrameInput::new(
                surface.rgba,
                RealtimeVideoPixelFormat::Rgba8,
                surface.width,
                surface.height,
                surface.width as usize * 4,
                surface.sequence,
            )
        };
    frame.timestamp_ms = Some(sample.timestamp_ns / 1_000_000);
    Ok(frame)
}

pub fn encode_sample(
    sample: &NativeVideoSample,
    config: &RealtimeVideoInputConfig,
) -> Result<(VideoFrameMetadata, RealtimeEncodedVideoFrameInput), String> {
    let encoded = sinomo_lm::encode_realtime_video_frame(&realtime_frame(sample)?, config)
        .map_err(|e| e.to_string())?;
    if encoded.image.len() > MAX_ENCODED_FRAME_BYTES {
        return Err("encoded video frame exceeds limit".into());
    }
    let metadata = VideoFrameMetadata {
        sequence: sample.sequence,
        timestamp_ns: sample.timestamp_ns,
        duration_ns: sample.duration_ns,
        captured_at_unix_ms: sample.captured_at_unix_ms,
        width: encoded.width.unwrap_or(sample.width),
        height: encoded.height.unwrap_or(sample.height),
        source_width: sample.width,
        source_height: sample.height,
        format: encoded.format,
        coordinate_grid: config.coordinate_grid,
    };
    Ok((metadata, encoded))
}
fn record(metadata: &VideoFrameMetadata, image: &[u8]) -> Result<Vec<u8>, String> {
    let metadata = serde_json::to_vec(metadata).map_err(|e| e.to_string())?;
    if metadata.len() > MAX_METADATA_BYTES || image.len() > MAX_ENCODED_FRAME_BYTES {
        return Err("video record exceeds limit".into());
    }
    let mut bytes = Vec::with_capacity(8 + metadata.len() + image.len());
    bytes.extend_from_slice(&(metadata.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&metadata);
    bytes.extend_from_slice(&(image.len() as u32).to_le_bytes());
    bytes.extend_from_slice(image);
    Ok(bytes)
}

/// The source owns capture and stops when the reader drops, the component is
/// cancelled, local consent is revoked, or the explicit duration expires.
pub fn desktop_capture_body(
    capture: impl DesktopFrameSource + 'static,
    encoding: RealtimeVideoInputConfig,
    duration: Duration,
    cancelled: Arc<AtomicBool>,
) -> (ObjectRpcBodySource, tokio::task::JoinHandle<()>) {
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    let failure = Arc::new(Mutex::new(None));
    let terminal = failure.clone();
    let worker = tokio::task::spawn_blocking(move || {
        let deadline = Instant::now() + duration;
        let mut sequence = 0;
        let mut next_encode = Instant::now();
        let encode_interval = encoding.model_frame_interval().unwrap_or(Duration::ZERO);
        let result = (|| -> Result<(), String> {
            while Instant::now() < deadline
                && !cancelled.load(Ordering::Acquire)
                && !sender.is_closed()
            {
                // Native capture keeps its own latest-frame slot. Pull only
                // when we can encode/send: a provider boundary may copy a full
                // raw frame, so polling it while backpressured wastes bandwidth.
                if sender.capacity() == 0 {
                    std::thread::sleep(Duration::from_millis(50));
                    continue;
                }
                let wait = next_encode.saturating_duration_since(Instant::now());
                if !wait.is_zero() {
                    std::thread::sleep(wait.min(Duration::from_millis(100)));
                    continue;
                }
                let Some(sample) = capture
                    .next(sequence, Duration::from_millis(100))
                    .map_err(|e| e.to_string())?
                else {
                    continue;
                };
                sequence = sample.sequence;
                let (metadata, encoded) = encode_sample(&sample, &encoding)?;
                next_encode = Instant::now() + encode_interval;
                match sender.try_send(record(&metadata, &encoded.image)?) {
                    Ok(()) | Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {}
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => break,
                }
            }
            Ok(())
        })();
        if let Err(error) = result {
            *terminal.lock().unwrap_or_else(|e| e.into_inner()) = Some(error);
        }
        // Drop capture before channel EOF, so completion means native teardown.
        drop(capture);
    });
    let reader = VideoBodyReader {
        receiver,
        pending: Vec::new(),
        offset: 0,
        failure,
    };
    (ObjectRpcBodySource::async_reader(reader, None), worker)
}

struct VideoBodyReader {
    receiver: tokio::sync::mpsc::Receiver<Vec<u8>>,
    pending: Vec<u8>,
    offset: usize,
    failure: Arc<Mutex<Option<String>>>,
}
impl AsyncRead for VideoBodyReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        loop {
            if this.offset < this.pending.len() {
                let take = buf.remaining().min(this.pending.len() - this.offset);
                buf.put_slice(&this.pending[this.offset..this.offset + take]);
                this.offset += take;
                return Poll::Ready(Ok(()));
            }
            match this.receiver.poll_recv(cx) {
                Poll::Ready(Some(frame)) => {
                    this.pending = frame;
                    this.offset = 0;
                }
                Poll::Ready(None) => {
                    return Poll::Ready(
                        match this
                            .failure
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .take()
                        {
                            Some(error) => Err(io::Error::other(error)),
                            None => Ok(()),
                        },
                    );
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// Incremental reader usable with any routed object-RPC body. The returned frame
/// can be passed straight to RealtimeSession::send_encoded_video_frame.
pub async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> io::Result<Option<(VideoFrameMetadata, RealtimeEncodedVideoFrameInput)>> {
    let mut first = [0u8; 1];
    if reader.read(&mut first).await? == 0 {
        return Ok(None);
    }
    let mut length = [first[0], 0, 0, 0];
    reader.read_exact(&mut length[1..]).await?;
    let length = u32::from_le_bytes(length) as usize;
    if length == 0 || length > MAX_METADATA_BYTES {
        return Err(io::Error::other("invalid video metadata length"));
    }
    let mut metadata = vec![0; length];
    reader.read_exact(&mut metadata).await?;
    let metadata: VideoFrameMetadata =
        serde_json::from_slice(&metadata).map_err(io::Error::other)?;
    let length = reader.read_u32_le().await? as usize;
    if length == 0 || length > MAX_ENCODED_FRAME_BYTES {
        return Err(io::Error::other("invalid encoded video frame length"));
    }
    if metadata.width == 0
        || metadata.height == 0
        || u64::from(metadata.width) * u64::from(metadata.height) > 32 * 1024 * 1024
    {
        return Err(io::Error::other("invalid video dimensions"));
    }
    let mut image = vec![0; length];
    reader.read_exact(&mut image).await?;
    let mut frame = RealtimeEncodedVideoFrameInput::new(image, metadata.format, metadata.sequence);
    frame.width = Some(metadata.width);
    frame.height = Some(metadata.height);
    frame.timestamp_ms = Some(metadata.timestamp_ns / 1_000_000);
    Ok(Some((metadata, frame)))
}

/// Model transport remains owned by Sinomo's existing realtime session.
pub async fn forward_to_realtime<R: AsyncRead + Unpin>(
    reader: &mut R,
    session: &mut dyn sinomo_lm::RealtimeSession,
    max_frames: u64,
) -> Result<u64, String> {
    let mut sent = 0;
    while sent < max_frames {
        let Some((_, frame)) = read_frame(reader).await.map_err(|e| e.to_string())? else {
            break;
        };
        session
            .send_encoded_video_frame(&frame)
            .await
            .map_err(|e| e.to_string())?;
        sent += 1;
    }
    Ok(sent)
}

type ReceivedVideoFrame = (VideoFrameMetadata, RealtimeEncodedVideoFrameInput);
/// A bounded latest-frame consumer, suitable for duplex audio/video call loops.
/// Dropping it cancels only its logical media RPC, not the shared connection.
pub struct RemoteVideoSource {
    latest: Arc<Mutex<Option<ReceivedVideoFrame>>>,
    failure: Arc<Mutex<Option<String>>>,
    worker: tokio::task::JoinHandle<()>,
}
impl RemoteVideoSource {
    pub fn from_body(body: sinomo_runtime::network::ObjectRpcIncomingBody) -> Self {
        let latest = Arc::new(Mutex::new(None));
        let failure = Arc::new(Mutex::new(None));
        let output = latest.clone();
        let terminal = failure.clone();
        let worker = tokio::spawn(async move {
            let mut body = body.cancel_on_drop();
            let mut previous = 0;
            loop {
                match read_frame(&mut body).await {
                    Ok(Some(frame)) if frame.0.sequence > previous => {
                        previous = frame.0.sequence;
                        *output.lock().unwrap_or_else(|e| e.into_inner()) = Some(frame);
                    }
                    Ok(None) => break,
                    result => {
                        let error = match result {
                            Err(error) => error.to_string(),
                            _ => "video sequence did not advance".into(),
                        };
                        *output.lock().unwrap_or_else(|e| e.into_inner()) = None;
                        *terminal.lock().unwrap_or_else(|e| e.into_inner()) = Some(error);
                        break;
                    }
                }
            }
        });
        Self {
            latest,
            failure,
            worker,
        }
    }
    pub fn take_latest(&self) -> Result<Option<ReceivedVideoFrame>, String> {
        if let Some(error) = self
            .failure
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
        {
            return Err(error.clone());
        }
        Ok(self.latest.lock().unwrap_or_else(|e| e.into_inner()).take())
    }
}
impl Drop for RemoteVideoSource {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn backpressure_does_not_fetch_unconsumed_native_frames() {
        use std::sync::atomic::AtomicUsize;
        struct Source(Arc<AtomicUsize>);
        impl DesktopFrameSource for Source {
            fn next(
                &self,
                _: u64,
                _: Duration,
            ) -> sinomo_runtime::device::desktop::DesktopResult<Option<NativeVideoSample>>
            {
                let sequence = self.0.fetch_add(1, Ordering::AcqRel) as u64 + 1;
                Ok(Some(NativeVideoSample {
                    sequence,
                    format: NativeVideoSampleFormat::Bgra8,
                    width: 1,
                    height: 1,
                    timestamp_ns: sequence,
                    duration_ns: 1,
                    captured_at_unix_ms: 1,
                    data: Arc::from([0, 0, 0, 255]),
                    planes: Arc::from([sinomo_runtime::device::NativeVideoPlane {
                        offset: 0,
                        len: 4,
                        width: 1,
                        height: 1,
                        stride: 4,
                    }]),
                }))
            }
        }
        let reads = Arc::new(AtomicUsize::new(0));
        let (body, worker) = desktop_capture_body(
            Source(reads.clone()),
            RealtimeVideoInputConfig {
                frame_format: RealtimeVideoFrameFormat::Png,
                max_frame_rate_hz: None,
                ..Default::default()
            },
            Duration::from_secs(2),
            Arc::new(AtomicBool::new(false)),
        );
        let until = tokio::time::Instant::now() + Duration::from_secs(1);
        while reads.load(Ordering::Acquire) == 0 {
            assert!(tokio::time::Instant::now() < until);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tokio::time::sleep(Duration::from_millis(160)).await;
        assert_eq!(reads.load(Ordering::Acquire), 1);
        drop(body);
        tokio::time::timeout(Duration::from_secs(1), worker)
            .await
            .unwrap()
            .unwrap();
    }
    #[tokio::test]
    async fn bgra_video_uses_the_shared_encoder_and_keeps_capture_metadata() {
        let sample = NativeVideoSample {
            sequence: 7,
            format: NativeVideoSampleFormat::Bgra8,
            width: 2,
            height: 1,
            timestamp_ns: 50_000_000,
            duration_ns: 33_333_333,
            captured_at_unix_ms: 123,
            data: Arc::from([0u8, 0, 255, 255, 255, 0, 0, 255, 9, 9, 9, 9]),
            planes: Arc::from([sinomo_runtime::device::NativeVideoPlane {
                offset: 0,
                len: 12,
                width: 2,
                height: 1,
                stride: 12,
            }]),
        };
        let frame = realtime_frame(&sample).unwrap();
        assert!(Arc::ptr_eq(&frame.pixels, &sample.data));
        let config = RealtimeVideoInputConfig {
            frame_format: RealtimeVideoFrameFormat::Png,
            ..Default::default()
        };
        let (metadata, encoded) = encode_sample(&sample, &config).unwrap();
        let image = image::load_from_memory(&encoded.image).unwrap().to_rgba8();
        assert_eq!(image.as_raw(), &[255, 0, 0, 255, 0, 0, 255, 255]);
        let bytes = record(&metadata, &encoded.image).unwrap();
        let mut stream = [bytes.clone(), bytes].concat();
        let mut reader = stream.as_slice();
        for _ in 0..2 {
            let (received, frame) = read_frame(&mut reader).await.unwrap().unwrap();
            assert_eq!(received.sequence, 7);
            assert_eq!(received.captured_at_unix_ms, 123);
            assert_eq!(frame.image, encoded.image);
        }
        assert!(read_frame(&mut reader).await.unwrap().is_none());
        stream.clear();
    }
    #[tokio::test]
    async fn continuous_records_preserve_sequence_and_reject_truncation() {
        let metadata = VideoFrameMetadata {
            sequence: 5,
            timestamp_ns: 50_000_000,
            duration_ns: 10,
            captured_at_unix_ms: 42,
            width: 2,
            height: 1,
            source_width: 2,
            source_height: 1,
            format: RealtimeVideoFrameFormat::Png,
            coordinate_grid: true,
        };
        let bytes = record(&metadata, &[1, 2, 3]).unwrap();
        let mut reader = bytes.as_slice();
        let (decoded, frame) = read_frame(&mut reader).await.unwrap().unwrap();
        assert_eq!(decoded, metadata);
        assert_eq!(frame.image.as_ref(), &[1, 2, 3]);
        assert_eq!(frame.timestamp_ms, Some(50));
        assert!(read_frame(&mut reader).await.unwrap().is_none());
        assert!(read_frame(&mut &bytes[..bytes.len() - 1]).await.is_err());
        assert!(
            read_frame(&mut &(u32::MAX.to_le_bytes())[..])
                .await
                .is_err()
        );
    }
}
