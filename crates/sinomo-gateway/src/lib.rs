//! Ingress adapter and normalized stream chunk skeleton.

use serde::{Deserialize, Serialize};
use sinomo_runtime::core::{EventSpan, Source};
use sinomo_runtime::device::{DeviceId, DeviceProfile};
use sinomo_runtime::events::SessionId;
use sinomo_runtime::node::NodeId;
use thiserror::Error;
use time::OffsetDateTime;

pub trait InputAdapter {
    fn next_chunk(&mut self) -> Result<Option<InputChunk>, GatewayError>;
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct InputChunk {
    pub chunk_id: String,
    pub modality: InputModality,
    pub origin_node_id: NodeId,
    pub origin_device_id: DeviceId,
    pub source: Source,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<SessionId>,
    #[serde(with = "time::serde::rfc3339")]
    pub captured_at: OffsetDateTime,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub span: Option<EventSpan>,
    pub payload: serde_json::Value,
}

impl InputChunk {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        chunk_id: impl Into<String>,
        modality: InputModality,
        device: &DeviceProfile,
        source: Source,
        session_id: Option<SessionId>,
        span: Option<EventSpan>,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            chunk_id: chunk_id.into(),
            modality,
            origin_node_id: device.node_id.clone(),
            origin_device_id: device.device_id.clone(),
            source,
            session_id,
            captured_at: OffsetDateTime::now_utc(),
            span,
            payload,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum InputModality {
    Text,
    Audio,
    Video,
    Screen,
    Telemetry,
    ExternalEvent,
}

#[derive(Clone, Debug)]
pub struct SyntheticInputAdapter {
    chunks: Vec<InputChunk>,
}

impl SyntheticInputAdapter {
    pub fn new(chunks: Vec<InputChunk>) -> Self {
        let mut chunks = chunks;
        chunks.reverse();
        Self { chunks }
    }

    pub fn text(
        device: &DeviceProfile,
        session_id: SessionId,
        text: impl Into<String>,
    ) -> Result<Self, GatewayError> {
        let chunk = InputChunk::new(
            "chunk_text_1",
            InputModality::Text,
            device,
            Source::new("text.synthetic.user")?,
            Some(session_id),
            None,
            serde_json::json!({ "text": text.into() }),
        );
        Ok(Self::new(vec![chunk]))
    }

    pub fn audio_transcript(
        device: &DeviceProfile,
        session_id: SessionId,
        text: impl Into<String>,
    ) -> Result<Self, GatewayError> {
        let chunk = InputChunk::new(
            "chunk_audio_1",
            InputModality::Audio,
            device,
            Source::new("audio.synthetic.microphone")?,
            Some(session_id),
            Some(EventSpan::new(0, 1_240)),
            serde_json::json!({
                "transcript_hint": text.into(),
                "vad": "speech",
            }),
        );
        Ok(Self::new(vec![chunk]))
    }

    pub fn screen_observation(
        device: &DeviceProfile,
        session_id: SessionId,
        description: impl Into<String>,
    ) -> Result<Self, GatewayError> {
        let chunk = InputChunk::new(
            "chunk_screen_1",
            InputModality::Screen,
            device,
            Source::new("screen.synthetic.main")?,
            Some(session_id),
            Some(EventSpan::new(0, 1_240)),
            serde_json::json!({
                "description_hint": description.into(),
                "frame_index": 1,
            }),
        );
        Ok(Self::new(vec![chunk]))
    }
}

impl InputAdapter for SyntheticInputAdapter {
    fn next_chunk(&mut self) -> Result<Option<InputChunk>, GatewayError> {
        Ok(self.chunks.pop())
    }
}

#[derive(Debug, Error)]
pub enum GatewayError {
    #[error(transparent)]
    Scalar(#[from] sinomo_runtime::core::ScalarError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_runtime::node::NodeId;

    #[test]
    fn synthetic_audio_chunk_carries_node_and_device_provenance() {
        let device =
            DeviceProfile::mock_microphone(NodeId::from_legacy_named("node_mobile").expect("node"));
        let session = SessionId::from_legacy_named("sess_ingest").expect("session");
        let mut adapter =
            SyntheticInputAdapter::audio_transcript(&device, session, "hello").expect("adapter");

        let chunk = adapter.next_chunk().expect("chunk").expect("some chunk");
        assert_eq!(chunk.origin_node_id, device.node_id);
        assert_eq!(chunk.origin_device_id, device.device_id);
        assert_eq!(chunk.modality, InputModality::Audio);
    }
}
