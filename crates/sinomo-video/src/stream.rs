//! Continuous video over Sinomo's streaming object-RPC media path. Both App Use
//! control modes use this framing and the same encoder as RealtimeSession.
use serde::{Deserialize, Serialize};
use sinomo_lm::{
    RealtimeEncodedVideoFrameInput, RealtimeVideoFrameFormat, RealtimeVideoFrameInput,
    RealtimeVideoInputConfig, RealtimeVideoPixelFormat,
};
use sinomo_runtime::device::desktop::DesktopFrameSource;
use sinomo_runtime::device::desktop::surfaces::{DesktopCaptureIdentity, DesktopOverviewTile};
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
pub const H264_STREAM_VERSION: u32 = 2;
use crate::codec::VideoTransport;
pub const MAX_ENCODED_FRAME_BYTES: usize = 32 * 1024 * 1024;
pub const MAX_METADATA_BYTES: usize = 4096;

/// A record is u32 LE metadata length, JSON metadata, u32 LE image length,
/// encoded image. The outer object RPC owns routing, authorization and framing.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct VideoFrameMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<DesktopCaptureIdentity>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tiles: Vec<DesktopOverviewTile>,
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
    #[serde(default)]
    pub transport: VideoTransport,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub packet_sequence: Option<u64>,
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
        identity: None,
        tiles: Vec::new(),
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
        transport: VideoTransport::Image,
        packet_sequence: None,
    };
    Ok((metadata, encoded))
}

/// A bounded application overview, assembled from native samples without an
/// intermediate image codec. Tiles retain their original dimensions/timestamps;
/// callers select a full window stream before grounding input coordinates.
pub fn compose_desktop_overview(
    samples: &[(String, NativeVideoSample)],
    sequence: u64,
    timestamp_ns: u64,
) -> Result<(NativeVideoSample, Vec<DesktopOverviewTile>), String> {
    if samples.is_empty() || samples.len() > 4 {
        return Err("overview requires 1..4 samples".into());
    }
    let (width, height) = (1024u32, 768u32);
    let columns = if samples.len() == 1 { 1 } else { 2 };
    let rows = if samples.len() <= 2 { 1 } else { 2 };
    let (cell_w, cell_h) = (width / columns, height / rows);
    let mut pixels = [28u8, 20, 12, 255].repeat((width * height) as usize);
    let mut tiles = Vec::new();
    let mut captured_at = 0;
    for (index, (id, sample)) in samples.iter().enumerate() {
        sample.validate().map_err(|e| e.to_string())?;
        if sample.format != NativeVideoSampleFormat::Bgra8 {
            return Err("overview expects native BGRA samples".into());
        }
        let factor =
            (cell_w as f64 / sample.width as f64).min(cell_h as f64 / sample.height as f64);
        let w = (sample.width as f64 * factor).round().max(1.0) as u32;
        let h = (sample.height as f64 * factor).round().max(1.0) as u32;
        let left = index as u32 % columns * cell_w + (cell_w - w) / 2;
        let top = index as u32 / columns * cell_h + (cell_h - h) / 2;
        let plane = sample.planes[0];
        for y in 0..h {
            let source_y = u64::from(y) * u64::from(sample.height) / u64::from(h);
            for x in 0..w {
                let source_x = u64::from(x) * u64::from(sample.width) / u64::from(w);
                let src = plane.offset
                    + source_y as usize * plane.stride as usize
                    + source_x as usize * 4;
                let dst = (((top + y) * width + left + x) * 4) as usize;
                pixels[dst..dst + 4].copy_from_slice(&sample.data[src..src + 4]);
            }
        }
        captured_at = captured_at.max(sample.captured_at_unix_ms);
        tiles.push(DesktopOverviewTile {
            surface_id: id.clone(),
            bounds: [left, top, left + w, top + h],
            source_width: sample.width,
            source_height: sample.height,
            captured_at_unix_ms: sample.captured_at_unix_ms.min(u64::MAX as u128) as u64,
        });
    }
    let sample = NativeVideoSample {
        sequence,
        format: NativeVideoSampleFormat::Bgra8,
        width,
        height,
        timestamp_ns,
        duration_ns: 0,
        captured_at_unix_ms: captured_at,
        planes: Arc::from([sinomo_runtime::device::NativeVideoPlane {
            offset: 0,
            len: pixels.len(),
            width,
            height,
            stride: width * 4,
        }]),
        data: pixels.into(),
    };
    Ok((sample, tiles))
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
    desktop_capture_body_with_transport(
        capture,
        encoding,
        duration,
        cancelled,
        VideoTransport::Image,
    )
}

pub fn desktop_capture_body_with_transport(
    capture: impl DesktopFrameSource + 'static,
    encoding: RealtimeVideoInputConfig,
    duration: Duration,
    cancelled: Arc<AtomicBool>,
    transport: VideoTransport,
) -> (ObjectRpcBodySource, tokio::task::JoinHandle<()>) {
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    let failure = Arc::new(Mutex::new(None));
    let terminal = failure.clone();
    let worker = tokio::task::spawn_blocking(move || {
        let deadline = Instant::now() + duration;
        let mut sequence = 0;
        let mut packet_sequence = 0;
        let mut hardware = None;
        let mut output_size = None;
        let mut next_encode = Instant::now();
        let encode_interval = encoding.model_frame_interval().unwrap_or(Duration::ZERO);
        let result = (|| -> Result<(), String> {
            while Instant::now() < deadline
                && !cancelled.load(Ordering::Acquire)
                && !sender.is_closed()
            {
                capture.check_active().map_err(|error| error.to_string())?;
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
                let frame_started = Instant::now();
                let (mut metadata, bytes) = if transport == VideoTransport::H264 {
                    let frame = realtime_frame(&sample)?;
                    let max = encoding.max_dimension.unwrap_or(1920).clamp(2, 1920);
                    let scale = f64::from(max)
                        / f64::from(frame.width.max(frame.height)).max(f64::from(max));
                    let size = (
                        (((f64::from(frame.width) * scale).ceil() as u32 + 1) & !1).clamp(2, 1920),
                        (((f64::from(frame.height) * scale).ceil() as u32 + 1) & !1).clamp(2, 1920),
                    );
                    crate::codec::dimensions(size.0, size.1)?;
                    if output_size != Some(size) {
                        if output_size.is_some() {
                            return Err("H264 source geometry changed; reopen stream".into());
                        }
                        let fps = if encode_interval.is_zero() {
                            30
                        } else {
                            (1.0 / encode_interval.as_secs_f64())
                                .round()
                                .clamp(1.0, 60.0) as u32
                        };
                        hardware = Some(crate::codec::encoder(size.0, size.1, fps)?);
                        output_size = Some(size);
                    }
                    let bytes = hardware.as_mut().unwrap().encode(&frame)?;
                    (
                        VideoFrameMetadata {
                            identity: None,
                            tiles: Vec::new(),
                            sequence: sample.sequence,
                            timestamp_ns: sample.timestamp_ns,
                            duration_ns: sample.duration_ns,
                            captured_at_unix_ms: sample.captured_at_unix_ms,
                            width: size.0,
                            height: size.1,
                            source_width: sample.width,
                            source_height: sample.height,
                            format: encoding.frame_format,
                            coordinate_grid: false,
                            transport,
                            packet_sequence: Some(packet_sequence),
                        },
                        Arc::<[u8]>::from(bytes),
                    )
                } else {
                    let (metadata, encoded) = encode_sample(&sample, &encoding)?;
                    (metadata, encoded.image)
                };
                capture.check_active().map_err(|error| error.to_string())?;
                if cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
                    break;
                }
                packet_sequence += 1;
                metadata.identity = capture.identity();
                metadata.tiles = capture.tiles();
                next_encode = frame_started + encode_interval;
                match sender.try_send(record(&metadata, &bytes)?) {
                    Ok(()) => {}
                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                        return Err("video producer lost reserved queue capacity".into());
                    }
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
async fn read_packet<R: AsyncRead + Unpin>(
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

/// Legacy stateless image reader rejects dependent compressed video.
pub async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> io::Result<Option<(VideoFrameMetadata, RealtimeEncodedVideoFrameInput)>> {
    let frame = read_packet(reader).await?;
    if frame
        .as_ref()
        .is_some_and(|(m, _)| m.transport != VideoTransport::Image)
    {
        return Err(io::Error::other("use VideoStreamReader for H264 transport"));
    }
    Ok(frame)
}

/// A received frame stays in pixels after video decoding. Image conversion is
/// deferred until a model actually consumes it, not performed for dropped previews.
#[derive(Clone)]
pub enum ReceivedFrame {
    Image(RealtimeEncodedVideoFrameInput),
    Pixels(RealtimeVideoFrameInput),
}
impl ReceivedFrame {
    pub fn into_model_image(
        self,
        format: RealtimeVideoFrameFormat,
    ) -> io::Result<RealtimeEncodedVideoFrameInput> {
        match self {
            Self::Image(frame) => Ok(frame),
            Self::Pixels(frame) => sinomo_lm::encode_realtime_video_frame(
                &frame,
                &RealtimeVideoInputConfig {
                    max_dimension: None,
                    frame_format: format,
                    ..Default::default()
                },
            )
            .map_err(io::Error::other),
        }
    }
}

/// Owns reference frames across reads. Decode every access unit before publishing
/// a latest image; dropping compressed P frames would corrupt later frames.
#[derive(Default)]
pub struct VideoStreamReader {
    decoder: Option<openh264::decoder::Decoder>,
    previous: Option<u64>,
    geometry: Option<(u32, u32)>,
    transport: Option<VideoTransport>,
    next_packet: u64,
    pub last_wire_bytes: usize,
    pub last_decode_us: u128,
}
impl VideoStreamReader {
    pub async fn read<R: AsyncRead + Unpin>(
        &mut self,
        reader: &mut R,
    ) -> io::Result<Option<(VideoFrameMetadata, RealtimeEncodedVideoFrameInput)>> {
        let Some((metadata, frame)) = self.read_decoded(reader).await? else {
            return Ok(None);
        };
        let format = metadata.format;
        let frame = tokio::task::spawn_blocking(move || frame.into_model_image(format))
            .await
            .map_err(io::Error::other)??;
        Ok(Some((metadata, frame)))
    }
    pub async fn read_decoded<R: AsyncRead + Unpin>(
        &mut self,
        reader: &mut R,
    ) -> io::Result<Option<(VideoFrameMetadata, ReceivedFrame)>> {
        let Some((metadata, frame)) = read_packet(reader).await? else {
            return Ok(None);
        };
        let mut state = std::mem::take(self);
        let (state, result) = tokio::task::spawn_blocking(move || {
            let result = state.decode_packet(metadata, frame);
            (state, result)
        })
        .await
        .map_err(io::Error::other)?;
        *self = state;
        result.map(Some)
    }
    fn decode_packet(
        &mut self,
        metadata: VideoFrameMetadata,
        frame: RealtimeEncodedVideoFrameInput,
    ) -> io::Result<(VideoFrameMetadata, ReceivedFrame)> {
        self.last_wire_bytes = frame.image.len();
        let started = Instant::now();
        if self
            .previous
            .is_some_and(|previous| metadata.sequence <= previous)
        {
            return Err(io::Error::other("video sequence did not advance"));
        }
        if self
            .transport
            .is_some_and(|value| value != metadata.transport)
        {
            return Err(io::Error::other("video transport changed within a stream"));
        }
        self.transport = Some(metadata.transport);
        self.previous = Some(metadata.sequence);
        let frame = match metadata.transport {
            VideoTransport::Image => ReceivedFrame::Image(frame),
            VideoTransport::Auto => return Err(io::Error::other("unnegotiated video transport")),
            VideoTransport::H264 => {
                crate::codec::dimensions(metadata.width, metadata.height)
                    .map_err(io::Error::other)?;
                if self
                    .geometry
                    .is_some_and(|size| size != (metadata.width, metadata.height))
                {
                    return Err(io::Error::other("H264 geometry changed within a stream"));
                }
                self.geometry = Some((metadata.width, metadata.height));
                if metadata.packet_sequence != Some(self.next_packet) {
                    return Err(io::Error::other("H264 packet sequence gap"));
                }
                crate::codec::validate_access_unit(
                    &frame.image,
                    metadata.width,
                    metadata.height,
                    self.next_packet == 0,
                )
                .map_err(io::Error::other)?;
                self.next_packet += 1;
                if self.decoder.is_none() {
                    self.decoder =
                        Some(openh264::decoder::Decoder::new().map_err(io::Error::other)?);
                }
                use openh264::formats::YUVSource;
                let decoded = self
                    .decoder
                    .as_mut()
                    .unwrap()
                    .decode(&frame.image)
                    .map_err(io::Error::other)?
                    .ok_or_else(|| {
                        io::Error::other("H264 access unit did not produce one frame")
                    })?;
                if decoded.dimensions() != (metadata.width as usize, metadata.height as usize) {
                    return Err(io::Error::other(
                        "H264 decoded dimensions do not match metadata",
                    ));
                }
                let mut rgb = vec![0; metadata.width as usize * metadata.height as usize * 3];
                decoded.write_rgb8(&mut rgb);
                let mut input = RealtimeVideoFrameInput::new(
                    rgb.into(),
                    RealtimeVideoPixelFormat::Rgb8,
                    metadata.width,
                    metadata.height,
                    metadata.width as usize * 3,
                    metadata.sequence,
                );
                input.timestamp_ms = frame.timestamp_ms;
                ReceivedFrame::Pixels(input)
            }
        };
        self.last_decode_us = started.elapsed().as_micros();
        Ok((metadata, frame))
    }
}

/// Model transport remains owned by Sinomo's existing realtime session.
pub async fn forward_to_realtime<R: AsyncRead + Unpin>(
    reader: &mut R,
    session: &mut dyn sinomo_lm::RealtimeSession,
    max_frames: u64,
) -> Result<u64, String> {
    let mut sent = 0;
    let mut decoder = VideoStreamReader::default();
    while sent < max_frames {
        let Some((_, frame)) = decoder.read(reader).await.map_err(|e| e.to_string())? else {
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
type DecodedVideoFrame = (VideoFrameMetadata, ReceivedFrame);
/// A bounded latest-frame consumer, suitable for duplex audio/video call loops.
/// Dropping it cancels only its logical media RPC, not the shared connection.
pub struct RemoteVideoSource {
    latest: Arc<Mutex<Option<DecodedVideoFrame>>>,
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
            let mut previous = None;
            let mut decoder = VideoStreamReader::default();
            loop {
                match decoder.read_decoded(&mut body).await {
                    Ok(Some(frame))
                        if previous.is_none_or(|sequence| frame.0.sequence > sequence) =>
                    {
                        previous = Some(frame.0.sequence);
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
        let latest = self.latest.lock().unwrap_or_else(|e| e.into_inner()).take();
        latest
            .map(|(metadata, frame)| {
                let image = frame
                    .into_model_image(metadata.format)
                    .map_err(|e| e.to_string())?;
                Ok((metadata, image))
            })
            .transpose()
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
    fn h264_records() -> Vec<(VideoFrameMetadata, Vec<u8>)> {
        use openh264::{
            encoder::Encoder,
            formats::{RgbSliceU8, YUVBuffer},
        };
        let mut encoder = Encoder::new().unwrap();
        (0..4)
            .map(|index| {
                let rgb = [20 + index as u8 * 20, 100, 200].repeat(64 * 64);
                let yuv = YUVBuffer::from_rgb_source(RgbSliceU8::new(&rgb, (64, 64)));
                let bytes = encoder.encode(&yuv).unwrap().to_vec();
                (
                    VideoFrameMetadata {
                        identity: None,
                        tiles: Vec::new(),
                        sequence: index * 2 + 1,
                        timestamp_ns: index * 100_000_000,
                        duration_ns: 100_000_000,
                        captured_at_unix_ms: 1,
                        width: 64,
                        height: 64,
                        source_width: 64,
                        source_height: 64,
                        format: RealtimeVideoFrameFormat::Png,
                        coordinate_grid: false,
                        transport: VideoTransport::H264,
                        packet_sequence: Some(index),
                    },
                    bytes,
                )
            })
            .collect()
    }
    #[tokio::test]
    async fn h264_decodes_dependent_frames_before_latest_image_and_rejects_loss() {
        let frames = h264_records();
        let mut decoder = VideoStreamReader::default();
        for (metadata, bytes) in &frames {
            let record = record(metadata, bytes).unwrap();
            let (received, image) = decoder.read(&mut record.as_slice()).await.unwrap().unwrap();
            assert_eq!(received.sequence, metadata.sequence);
            assert_eq!(image::load_from_memory(&image.image).unwrap().width(), 64);
            assert_eq!(decoder.last_wire_bytes, bytes.len());
        }
        let mut decoder = VideoStreamReader::default();
        decoder
            .read(&mut record(&frames[0].0, &frames[0].1).unwrap().as_slice())
            .await
            .unwrap();
        assert!(
            decoder
                .read(&mut record(&frames[2].0, &frames[2].1).unwrap().as_slice())
                .await
                .unwrap_err()
                .to_string()
                .contains("sequence gap")
        );
        assert!(
            read_frame(&mut record(&frames[0].0, &frames[0].1).unwrap().as_slice())
                .await
                .is_err()
        );
    }
    #[tokio::test]
    async fn h264_rejects_missing_configuration_and_lying_geometry() {
        let frames = h264_records();
        let mut metadata = frames[1].0.clone();
        metadata.packet_sequence = Some(0);
        assert!(
            VideoStreamReader::default()
                .read(&mut record(&metadata, &frames[1].1).unwrap().as_slice())
                .await
                .is_err()
        );
        let mut metadata = frames[0].0.clone();
        metadata.width = 128;
        assert!(
            VideoStreamReader::default()
                .read(&mut record(&metadata, &frames[0].1).unwrap().as_slice())
                .await
                .unwrap_err()
                .to_string()
                .contains("SPS")
        );
    }
    #[tokio::test]
    async fn remote_source_accepts_a_native_camera_starting_at_sequence_zero() {
        let sample = NativeVideoSample {
            sequence: 0,
            format: NativeVideoSampleFormat::Bgra8,
            width: 1,
            height: 1,
            timestamp_ns: 0,
            duration_ns: 33_333_333,
            captured_at_unix_ms: 1,
            data: Arc::from([0, 0, 0, 255]),
            planes: Arc::from([sinomo_runtime::device::NativeVideoPlane {
                offset: 0,
                len: 4,
                width: 1,
                height: 1,
                stride: 4,
            }]),
        };
        let (metadata, encoded) =
            encode_sample(&sample, &RealtimeVideoInputConfig::default()).unwrap();
        let body = sinomo_runtime::network::ObjectRpcIncomingBody::bytes(
            record(&metadata, &encoded.image).unwrap(),
        );
        let source = RemoteVideoSource::from_body(body);
        let frame = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(frame) = source.take_latest().unwrap() {
                    break frame;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(frame.0.sequence, 0);
    }

    #[test]
    fn overview_tiles_preserve_surface_identity_and_source_geometry() {
        let make = |color: [u8; 4], width, height, time| NativeVideoSample {
            sequence: 1,
            format: NativeVideoSampleFormat::Bgra8,
            width,
            height,
            timestamp_ns: 1,
            duration_ns: 1,
            captured_at_unix_ms: time,
            data: color.repeat((width * height) as usize).into(),
            planes: Arc::from([sinomo_runtime::device::NativeVideoPlane {
                offset: 0,
                len: (width * height * 4) as usize,
                width,
                height,
                stride: width * 4,
            }]),
        };
        let (sample, tiles) = compose_desktop_overview(
            &[
                ("a".into(), make([0, 0, 255, 255], 2, 1, 10)),
                ("b".into(), make([255, 0, 0, 255], 1, 2, 20)),
            ],
            9,
            42,
        )
        .unwrap();
        assert_eq!(sample.sequence, 9);
        assert_eq!(sample.captured_at_unix_ms, 20);
        assert_eq!(tiles[0].surface_id, "a");
        assert_eq!((tiles[0].source_width, tiles[0].source_height), (2, 1));
        assert_eq!(tiles[1].surface_id, "b");
        assert_eq!(tiles[0].captured_at_unix_ms, 10);
        for (tile, color) in tiles.iter().zip([[0, 0, 255, 255], [255, 0, 0, 255]]) {
            let x = (tile.bounds[0] + tile.bounds[2]) / 2;
            let y = (tile.bounds[1] + tile.bounds[3]) / 2;
            let offset = ((y * sample.width + x) * 4) as usize;
            assert_eq!(&sample.data[offset..offset + 4], &color);
        }
    }
    #[tokio::test]
    async fn backpressure_does_not_fetch_frames_or_delay_revocation() {
        use std::sync::atomic::AtomicUsize;
        struct Source(Arc<AtomicUsize>, Arc<AtomicBool>);
        impl DesktopFrameSource for Source {
            fn check_active(&self) -> sinomo_runtime::device::desktop::DesktopResult<()> {
                if self.1.load(Ordering::Acquire) {
                    Err(
                        sinomo_runtime::device::desktop::DesktopError::PermissionDenied(
                            "revoked".into(),
                        ),
                    )
                } else {
                    Ok(())
                }
            }
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
        let revoked = Arc::new(AtomicBool::new(false));
        let (body, worker) = desktop_capture_body(
            Source(reads.clone(), revoked.clone()),
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
        revoked.store(true, Ordering::Release);
        // Keep the unread body alive: revocation must release capture even if
        // the peer never drains the already queued frame.
        tokio::time::timeout(Duration::from_secs(1), worker)
            .await
            .unwrap()
            .unwrap();
        drop(body);
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
            identity: None,
            tiles: Vec::new(),
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
            transport: VideoTransport::Image,
            packet_sequence: None,
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
