//! Video and screen perception event producer skeleton.

use serde::{Deserialize, Serialize};
use sinomo_gateway::{InputChunk, InputModality};
use sinomo_runtime::core::{
    Confidence, EventEnvelope, EventKind, EventSpan, LatencyClass, PrivacyClass, RoutingMetadata,
    RoutingScope, Salience, Source,
};
use sinomo_runtime::device;
use sinomo_runtime::events::EventId;
use std::time::Duration;
use thiserror::Error;

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

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum NativeVideoFrameFormat {
    Jpeg,
}

impl NativeVideoFrameFormat {
    pub fn mime_type(self) -> &'static str {
        match self {
            Self::Jpeg => "image/jpeg",
        }
    }
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct NativeVideoFrame {
    pub sequence: u64,
    pub format: NativeVideoFrameFormat,
    pub width: u32,
    pub height: u32,
    pub captured_at_unix_ms: u128,
    pub bytes: Vec<u8>,
}

pub struct NativeVideoCapture {
    raw: device::NativeCameraCapture,
}

impl NativeVideoCapture {
    pub fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Option<NativeVideoFrame>, NativeVideoError> {
        self.raw
            .recv_timeout(timeout)
            .map(|frame| frame.map(native_video_frame_from_device))
            .map_err(NativeVideoError::Device)
    }
}

#[derive(Debug, Error, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub enum NativeVideoError {
    #[error("native video device error: {0}")]
    Device(#[from] device::NativeDeviceError),
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

fn native_video_frame_from_device(frame: device::NativeVideoFrame) -> NativeVideoFrame {
    NativeVideoFrame {
        sequence: frame.sequence,
        format: match frame.format {
            device::NativeVideoFrameFormat::Jpeg => NativeVideoFrameFormat::Jpeg,
        },
        width: frame.width,
        height: frame.height,
        captured_at_unix_ms: frame.captured_at_unix_ms,
        bytes: frame.bytes,
    }
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
    fn native_video_frame_mapping_preserves_raw_device_metadata() {
        let frame = native_video_frame_from_device(sinomo_runtime::device::NativeVideoFrame {
            sequence: 7,
            format: sinomo_runtime::device::NativeVideoFrameFormat::Jpeg,
            width: 640,
            height: 480,
            captured_at_unix_ms: 42,
            bytes: vec![1, 2, 3],
        });

        assert_eq!(frame.sequence, 7);
        assert_eq!(frame.format, NativeVideoFrameFormat::Jpeg);
        assert_eq!(frame.format.mime_type(), "image/jpeg");
        assert_eq!(frame.width, 640);
        assert_eq!(frame.height, 480);
        assert_eq!(frame.bytes, vec![1, 2, 3]);
    }
}
