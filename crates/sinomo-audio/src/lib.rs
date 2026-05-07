//! Audio perception event producer skeleton.

use sinomo_gateway::{InputChunk, InputModality};
use sinomo_runtime::core::{
    Confidence, EventEnvelope, EventId, EventKind, EventSpan, LatencyClass, PrivacyClass,
    RoutingMetadata, RoutingScope, Salience, Source,
};

#[derive(Clone, Debug, Default)]
pub struct AudioPerceptionWorker;

impl AudioPerceptionWorker {
    pub fn new() -> Self {
        Self
    }

    pub fn process_chunk(&self, chunk: &InputChunk) -> Vec<EventEnvelope> {
        if chunk.modality != InputModality::Audio {
            return Vec::new();
        }

        let text = chunk
            .payload
            .get("transcript_hint")
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .to_string();

        let span = chunk.span.unwrap_or(EventSpan::new(0, 0));
        vec![
            base_event(
                chunk,
                "audio.vad.speech",
                span,
                serde_json::json!({ "state": "speech" }),
            ),
            base_event(
                chunk,
                "speech.transcript.final",
                span,
                serde_json::json!({ "text": text, "source_chunk_id": chunk.chunk_id }),
            ),
        ]
    }
}

fn base_event(
    chunk: &InputChunk,
    kind: &str,
    span: EventSpan,
    payload: serde_json::Value,
) -> EventEnvelope {
    EventEnvelope::new(
        EventId::generate(),
        chunk.captured_at,
        chunk.origin_node_id.clone(),
        Source::new(format!("{}.perception", chunk.source.as_str())).expect("valid source"),
        EventKind::new(kind).expect("valid event kind"),
        Salience::new(0.62).expect("valid salience"),
        Confidence::new(0.91).expect("valid confidence"),
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
            .expect("audio chunks carry sessions"),
    )
    .tap_span(span)
}

trait WithSpan {
    fn tap_span(self, span: EventSpan) -> Self;
}

impl WithSpan for EventEnvelope {
    fn tap_span(mut self, span: EventSpan) -> Self {
        self.span = Some(span);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_gateway::{InputAdapter, SyntheticInputAdapter};
    use sinomo_runtime::core::{NodeId, SessionId};
    use sinomo_runtime::device::DeviceProfile;

    #[test]
    fn audio_worker_emits_vad_and_transcript_events() {
        let device =
            DeviceProfile::mock_microphone(NodeId::from_legacy_named("node_mobile").expect("node"));
        let session = SessionId::from_legacy_named("sess_audio").expect("session");
        let mut adapter =
            SyntheticInputAdapter::audio_transcript(&device, session, "check the alert")
                .expect("adapter");
        let chunk = adapter.next_chunk().expect("chunk").expect("chunk");
        let events = AudioPerceptionWorker::new().process_chunk(&chunk);

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].origin_device_id, Some(device.device_id.clone()));
        assert_eq!(events[1].kind.as_str(), "speech.transcript.final");
    }
}
