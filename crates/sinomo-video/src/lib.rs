//! Video and screen perception event producer skeleton.

use serde::{Deserialize, Serialize};
use sinomo_gateway::{InputChunk, InputModality};
use sinomo_runtime::core::{
    Confidence, EventEnvelope, EventKind, EventSpan, LatencyClass, PrivacyClass, RoutingMetadata,
    RoutingScope, Salience, Source,
};
use sinomo_runtime::device;
use sinomo_runtime::events::EventId;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use thiserror::Error;

pub use sinomo_runtime::device::{NativeVideoPlane, NativeVideoSample, NativeVideoSampleFormat};

#[derive(Clone, Debug, Default)]
pub struct VideoPerceptionWorker;

impl VideoPerceptionWorker {
    pub fn new() -> Self {
        Self
    }

    pub fn process_chunk(&self, chunk: &InputChunk) -> Vec<EventEnvelope> {
        if !matches!(chunk.modality, InputModality::Video | InputModality::Screen) {
            return Vec::new();
        }

        let description = chunk
            .payload
            .get("description_hint")
            .and_then(|value| value.as_str())
            .unwrap_or("screen state observed")
            .to_string();
        let span = chunk.span.unwrap_or(EventSpan::new(0, 0));

        vec![base_event(
            chunk,
            "vision.observation.screen",
            span,
            serde_json::json!({
                "description": description,
                "source_chunk_id": chunk.chunk_id
            }),
        )]
    }
}

fn base_event(
    chunk: &InputChunk,
    kind: &str,
    span: EventSpan,
    payload: serde_json::Value,
) -> EventEnvelope {
    let mut event = EventEnvelope::new(
        EventId::generate(),
        chunk.captured_at,
        chunk.origin_node_id.clone(),
        Source::new(format!("{}.perception", chunk.source.as_str())).expect("valid source"),
        EventKind::new(kind).expect("valid event kind"),
        Salience::new(0.58).expect("valid salience"),
        Confidence::new(0.88).expect("valid confidence"),
        RoutingMetadata::new(
            RoutingScope::Session,
            LatencyClass::Interactive,
            PrivacyClass::Derived,
        ),
    )
    .with_origin_device(chunk.origin_device_id.clone())
    .with_payload(payload)
    .with_session(
        chunk
            .session_id
            .clone()
            .expect("screen chunks carry sessions"),
    );
    event.span = Some(span);
    event
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NativeVideoCaptureConfig {
    pub device: Option<String>,
    pub width: u32,
    pub height: u32,
    pub frame_interval_ms: u64,
}

impl Default for NativeVideoCaptureConfig {
    fn default() -> Self {
        let raw = device::NativeCameraCaptureConfig::default();
        Self {
            device: raw.device,
            width: raw.width,
            height: raw.height,
            frame_interval_ms: raw.frame_interval_ms,
        }
    }
}

/// Capture cadence used for responsive node-local camera previews. Realtime
/// network senders should apply their own, usually lower, outbound cadence.
pub const NATIVE_VIDEO_PREVIEW_FRAME_INTERVAL_MS: u64 =
    sinomo_runtime::device::NATIVE_CAMERA_PREVIEW_FRAME_INTERVAL_MS;

/// Shared RGBA frame for stable application preview surfaces. Pixel storage is
/// immutable and reference-counted so preview and model consumers can share a
/// conversion result without copying it.
#[derive(Clone, Debug, PartialEq)]
pub struct NativeVideoSurfaceFrame {
    pub revision: u64,
    pub sequence: u64,
    pub width: u32,
    pub height: u32,
    pub timestamp_ns: u64,
    pub duration_ns: u64,
    pub captured_at_unix_ms: u128,
    pub rgba: Arc<[u8]>,
}

#[derive(Debug, Default)]
pub struct NativeVideoSurface {
    revision: AtomicU64,
    latest: Mutex<Option<Arc<NativeVideoSurfaceFrame>>>,
}

impl NativeVideoSurface {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn publish(&self, mut frame: NativeVideoSurfaceFrame) -> Arc<NativeVideoSurfaceFrame> {
        let mut latest = self
            .latest
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        frame.revision = self.advance_revision();
        let frame = Arc::new(frame);
        *latest = Some(frame.clone());
        frame
    }

    /// Convert a native sample off the render thread, publish it, and return
    /// the exact shared frame stored by this surface.
    pub fn publish_sample(
        &self,
        sample: &NativeVideoSample,
    ) -> Result<Arc<NativeVideoSurfaceFrame>, NativeVideoError> {
        native_video_sample_to_surface_frame(sample).map(|frame| self.publish(frame))
    }

    pub fn latest(&self) -> Option<Arc<NativeVideoSurfaceFrame>> {
        self.latest
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn clear(&self) -> u64 {
        let mut latest = self
            .latest
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let revision = self.advance_revision();
        *latest = None;
        revision
    }

    pub fn revision(&self) -> u64 {
        self.revision.load(Ordering::Acquire)
    }

    fn advance_revision(&self) -> u64 {
        let mut current = self.revision.load(Ordering::Acquire);
        loop {
            let next = current.saturating_add(1);
            match self.revision.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return next,
                Err(observed) => current = observed,
            }
        }
    }
}

pub struct NativeVideoCapture {
    raw: device::NativeCameraCapture,
}

impl NativeVideoCapture {
    /// Receive the newest native sample. Capture uses a single-slot handoff,
    /// replacing an unread sample instead of building latency in a queue.
    pub fn recv_sample_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Option<NativeVideoSample>, NativeVideoError> {
        self.raw
            .recv_sample_timeout(timeout)
            .map_err(NativeVideoError::Device)
    }
}

#[derive(Debug, Error, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub enum NativeVideoError {
    #[error("native video device error: {0}")]
    Device(#[from] device::NativeDeviceError),
    #[error("native video sample is invalid: {detail}")]
    InvalidSample { detail: String },
    #[error("native video sample decode failed: {detail}")]
    Decode { detail: String },
}

pub fn start_native_camera_capture(
    config: NativeVideoCaptureConfig,
) -> Result<NativeVideoCapture, NativeVideoError> {
    let raw = device::start_native_camera_capture(device::NativeCameraCaptureConfig {
        device: config.device,
        width: config.width,
        height: config.height,
        frame_interval_ms: config.frame_interval_ms,
    })?;
    Ok(NativeVideoCapture { raw })
}

/// Convert one raw or natively encoded camera sample into an immutable RGBA
/// surface frame. Call this on the media worker, never in a render callback.
pub fn native_video_sample_to_surface_frame(
    sample: &NativeVideoSample,
) -> Result<NativeVideoSurfaceFrame, NativeVideoError> {
    validate_native_video_sample(sample)?;

    let (width, height, rgba) = match sample.format {
        NativeVideoSampleFormat::Mjpeg => decode_mjpeg_sample(sample)?,
        NativeVideoSampleFormat::Nv12 => {
            (sample.width, sample.height, convert_nv12_to_rgba(sample)?)
        }
        NativeVideoSampleFormat::Yuyv => {
            (sample.width, sample.height, convert_yuyv_to_rgba(sample)?)
        }
        NativeVideoSampleFormat::Rgb8
        | NativeVideoSampleFormat::Bgr8
        | NativeVideoSampleFormat::Gray8 => {
            (sample.width, sample.height, convert_packed_to_rgba(sample)?)
        }
    };

    Ok(NativeVideoSurfaceFrame {
        revision: 0,
        sequence: sample.sequence,
        width,
        height,
        timestamp_ns: sample.timestamp_ns,
        duration_ns: sample.duration_ns,
        captured_at_unix_ms: sample.captured_at_unix_ms,
        rgba: rgba.into(),
    })
}

fn validate_native_video_sample(sample: &NativeVideoSample) -> Result<(), NativeVideoError> {
    sample
        .validate()
        .map_err(|error| NativeVideoError::InvalidSample {
            detail: error.to_string(),
        })
}

fn decode_mjpeg_sample(
    sample: &NativeVideoSample,
) -> Result<(u32, u32, Vec<u8>), NativeVideoError> {
    let bytes = sample_plane_data(sample, 0)?;
    let image = image::load_from_memory_with_format(bytes, image::ImageFormat::Jpeg)
        .map_err(|error| NativeVideoError::Decode {
            detail: error.to_string(),
        })?
        .to_rgba8();
    let (width, height) = image.dimensions();
    Ok((width, height, image.into_raw()))
}

fn convert_packed_to_rgba(sample: &NativeVideoSample) -> Result<Vec<u8>, NativeVideoError> {
    let plane = &sample.planes[0];
    let data = sample_plane_data(sample, 0)?;
    let mut rgba = Vec::with_capacity(pixel_buffer_len(sample.width, sample.height, 4)?);
    let stride = plane.stride as usize;
    for y in 0..sample.height as usize {
        let row = &data[y * stride..(y + 1) * stride];
        for x in 0..sample.width as usize {
            match sample.format {
                NativeVideoSampleFormat::Rgb8 => {
                    let offset = x * 3;
                    rgba.extend_from_slice(&[row[offset], row[offset + 1], row[offset + 2], 255]);
                }
                NativeVideoSampleFormat::Bgr8 => {
                    let offset = x * 3;
                    rgba.extend_from_slice(&[row[offset + 2], row[offset + 1], row[offset], 255]);
                }
                NativeVideoSampleFormat::Gray8 => {
                    let value = row[x];
                    rgba.extend_from_slice(&[value, value, value, 255]);
                }
                _ => unreachable!("packed conversion only receives RGB, BGR, or gray"),
            }
        }
    }
    Ok(rgba)
}

fn convert_yuyv_to_rgba(sample: &NativeVideoSample) -> Result<Vec<u8>, NativeVideoError> {
    let plane = &sample.planes[0];
    let data = sample_plane_data(sample, 0)?;
    let mut rgba = Vec::with_capacity(pixel_buffer_len(sample.width, sample.height, 4)?);
    let stride = plane.stride as usize;
    for y in 0..sample.height as usize {
        let row = &data[y * stride..(y + 1) * stride];
        for x in 0..sample.width as usize {
            let pair = (x / 2) * 4;
            let luma = row[pair + if x % 2 == 0 { 0 } else { 2 }];
            let [red, green, blue] = yuv_to_rgb(luma, row[pair + 1], row[pair + 3]);
            rgba.extend_from_slice(&[red, green, blue, 255]);
        }
    }
    Ok(rgba)
}

fn convert_nv12_to_rgba(sample: &NativeVideoSample) -> Result<Vec<u8>, NativeVideoError> {
    let y_plane = &sample.planes[0];
    let uv_plane = &sample.planes[1];
    let y_data = sample_plane_data(sample, 0)?;
    let uv_data = sample_plane_data(sample, 1)?;
    let y_stride = y_plane.stride as usize;
    let uv_stride = uv_plane.stride as usize;
    let mut rgba = Vec::with_capacity(pixel_buffer_len(sample.width, sample.height, 4)?);
    for y in 0..sample.height as usize {
        let y_row = &y_data[y * y_stride..(y + 1) * y_stride];
        let uv_row = &uv_data[(y / 2) * uv_stride..(y / 2 + 1) * uv_stride];
        for (x, &luma) in y_row.iter().enumerate().take(sample.width as usize) {
            let uv_offset = (x / 2) * 2;
            let [red, green, blue] = yuv_to_rgb(luma, uv_row[uv_offset], uv_row[uv_offset + 1]);
            rgba.extend_from_slice(&[red, green, blue, 255]);
        }
    }
    Ok(rgba)
}

fn sample_plane_data(sample: &NativeVideoSample, index: usize) -> Result<&[u8], NativeVideoError> {
    sample
        .plane_data(index)
        .ok_or_else(|| NativeVideoError::InvalidSample {
            detail: format!("native video sample is missing valid plane {index}"),
        })
}

fn yuv_to_rgb(y: u8, u: u8, v: u8) -> [u8; 3] {
    let c = (i32::from(y) - 16).max(0);
    let d = i32::from(u) - 128;
    let e = i32::from(v) - 128;
    let red = (298 * c + 409 * e + 128) >> 8;
    let green = (298 * c - 100 * d - 208 * e + 128) >> 8;
    let blue = (298 * c + 516 * d + 128) >> 8;
    [clamp_u8(red), clamp_u8(green), clamp_u8(blue)]
}

fn clamp_u8(value: i32) -> u8 {
    value.clamp(0, 255) as u8
}

fn pixel_buffer_len(width: u32, height: u32, channels: usize) -> Result<usize, NativeVideoError> {
    usize::try_from(width)
        .ok()
        .and_then(|width| width.checked_mul(height as usize))
        .and_then(|pixels| pixels.checked_mul(channels))
        .ok_or_else(|| NativeVideoError::InvalidSample {
            detail: "video pixel buffer length overflowed".to_string(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_gateway::{InputAdapter, SyntheticInputAdapter};
    use sinomo_runtime::device::DeviceProfile;
    use sinomo_runtime::events::SessionId;
    use sinomo_runtime::node::NodeId;

    #[test]
    fn video_worker_emits_screen_observation() {
        let device =
            DeviceProfile::mock_screen(NodeId::from_legacy_named("node_desktop").expect("node"));
        let session = SessionId::from_legacy_named("sess_video").expect("session");
        let mut adapter =
            SyntheticInputAdapter::screen_observation(&device, session, "alert dialog visible")
                .expect("adapter");
        let chunk = adapter.next_chunk().expect("chunk").expect("chunk");
        let events = VideoPerceptionWorker::new().process_chunk(&chunk);

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind.as_str(), "vision.observation.screen");
        assert_eq!(events[0].origin_device_id, Some(device.device_id));
    }

    #[test]
    fn native_video_capture_defaults_mirror_raw_camera_defaults() {
        let raw = sinomo_runtime::device::NativeCameraCaptureConfig::default();
        let wrapped = NativeVideoCaptureConfig::default();

        assert_eq!(wrapped.device, raw.device);
        assert_eq!(wrapped.width, raw.width);
        assert_eq!(wrapped.height, raw.height);
        assert_eq!(wrapped.frame_interval_ms, raw.frame_interval_ms);
        assert_eq!(
            wrapped.frame_interval_ms,
            NATIVE_VIDEO_PREVIEW_FRAME_INTERVAL_MS
        );
        assert_eq!(NATIVE_VIDEO_PREVIEW_FRAME_INTERVAL_MS, 33);
    }

    #[test]
    fn packed_native_samples_convert_stride_aware_to_rgba() {
        let rgb = native_sample(
            NativeVideoSampleFormat::Rgb8,
            2,
            1,
            vec![255, 0, 0, 0, 255, 0, 9, 9],
            vec![plane(0, 8, 2, 1, 8)],
        );
        let rgb_surface = native_video_sample_to_surface_frame(&rgb).expect("RGB surface");
        assert_eq!(&*rgb_surface.rgba, &[255, 0, 0, 255, 0, 255, 0, 255]);

        let bgr = native_sample(
            NativeVideoSampleFormat::Bgr8,
            1,
            1,
            vec![30, 20, 10],
            vec![plane(0, 3, 1, 1, 3)],
        );
        assert_eq!(
            &*native_video_sample_to_surface_frame(&bgr)
                .expect("BGR surface")
                .rgba,
            &[10, 20, 30, 255]
        );

        let gray = native_sample(
            NativeVideoSampleFormat::Gray8,
            2,
            1,
            vec![12, 240, 99],
            vec![plane(0, 3, 2, 1, 3)],
        );
        assert_eq!(
            &*native_video_sample_to_surface_frame(&gray)
                .expect("gray surface")
                .rgba,
            &[12, 12, 12, 255, 240, 240, 240, 255]
        );
    }

    #[test]
    fn yuyv_and_nv12_samples_convert_without_intermediate_jpeg() {
        let yuyv = native_sample(
            NativeVideoSampleFormat::Yuyv,
            2,
            1,
            vec![16, 128, 235, 128],
            vec![plane(0, 4, 2, 1, 4)],
        );
        assert_eq!(
            &*native_video_sample_to_surface_frame(&yuyv)
                .expect("YUYV surface")
                .rgba,
            &[0, 0, 0, 255, 255, 255, 255, 255]
        );

        let nv12 = native_sample(
            NativeVideoSampleFormat::Nv12,
            2,
            2,
            vec![16, 235, 81, 145, 128, 128],
            vec![plane(0, 4, 2, 2, 2), plane(4, 2, 1, 1, 2)],
        );
        let surface = native_video_sample_to_surface_frame(&nv12).expect("NV12 surface");
        assert_eq!(
            &*surface.rgba,
            &[
                0, 0, 0, 255, 255, 255, 255, 255, 76, 76, 76, 255, 150, 150, 150, 255,
            ]
        );
        assert_eq!(surface.timestamp_ns, 1_000);
        assert_eq!(surface.duration_ns, 33_333_333);
    }

    #[test]
    fn native_mjpeg_sample_decodes_to_rgba_surface() {
        let mut jpeg = Vec::new();
        image::codecs::jpeg::JpegEncoder::new_with_quality(&mut jpeg, 90)
            .encode(&[255, 0, 0], 1, 1, image::ExtendedColorType::Rgb8)
            .expect("encode jpeg");
        let sample = native_sample(
            NativeVideoSampleFormat::Mjpeg,
            1,
            1,
            jpeg.clone(),
            vec![plane(0, jpeg.len(), 1, 1, 0)],
        );

        let surface = native_video_sample_to_surface_frame(&sample).expect("decode sample");

        assert_eq!(surface.sequence, 7);
        assert_eq!((surface.width, surface.height), (1, 1));
        assert_eq!(surface.rgba.len(), 4);
        assert!(surface.rgba[0] > 200);
        assert_eq!(surface.rgba[3], 255);
    }

    #[test]
    fn native_video_surface_publishes_one_stable_arc_and_monotonic_revisions() {
        let sample = native_sample(
            NativeVideoSampleFormat::Gray8,
            1,
            1,
            vec![128],
            vec![plane(0, 1, 1, 1, 1)],
        );
        let surface = NativeVideoSurface::new();

        let first = surface.publish_sample(&sample).expect("publish sample");
        let latest = surface.latest().expect("latest frame");
        assert!(Arc::ptr_eq(&first, &latest));
        assert!(Arc::ptr_eq(&first.rgba, &latest.rgba));
        assert_eq!(first.revision, 1);
        assert_eq!(surface.revision(), 1);

        let second = surface.publish_sample(&sample).expect("republish sample");
        assert_eq!(second.revision, 2);
        assert_eq!(surface.clear(), 3);
        assert_eq!(surface.revision(), 3);
        assert!(surface.latest().is_none());
    }

    #[test]
    fn invalid_mjpeg_is_rejected_at_the_decode_boundary() {
        let sample = native_sample(
            NativeVideoSampleFormat::Mjpeg,
            1,
            1,
            vec![1, 2, 3],
            vec![plane(0, 3, 1, 1, 0)],
        );

        assert!(matches!(
            native_video_sample_to_surface_frame(&sample),
            Err(NativeVideoError::Decode { .. })
        ));
    }

    fn native_sample(
        format: NativeVideoSampleFormat,
        width: u32,
        height: u32,
        data: Vec<u8>,
        planes: Vec<NativeVideoPlane>,
    ) -> NativeVideoSample {
        let sample = NativeVideoSample {
            sequence: 7,
            format,
            width,
            height,
            timestamp_ns: 1_000,
            duration_ns: 33_333_333,
            captured_at_unix_ms: 42,
            data: data.into(),
            planes: planes.into(),
        };
        sample.validate().expect("valid test sample");
        sample
    }

    fn plane(offset: usize, len: usize, width: u32, height: u32, stride: u32) -> NativeVideoPlane {
        NativeVideoPlane {
            offset,
            len,
            width,
            height,
            stride,
        }
    }
}
