//! Video and screen perception event producer skeleton.

use sinomo_gateway::{InputChunk, InputModality};
use sinomo_runtime::core::{
    Confidence, EventEnvelope, EventId, EventKind, EventSpan, LatencyClass, PrivacyClass,
    RoutingMetadata, RoutingScope, Salience, Source,
};

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

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_gateway::{InputAdapter, SyntheticInputAdapter};
    use sinomo_runtime::core::{NodeId, SessionId};
    use sinomo_runtime::device::DeviceProfile;

    #[test]
    fn video_worker_emits_screen_observation() {
        let device = DeviceProfile::mock_screen(NodeId::new("node_desktop").expect("node"));
        let session = SessionId::new("sess_video").expect("session");
        let mut adapter =
            SyntheticInputAdapter::screen_observation(&device, session, "alert dialog visible")
                .expect("adapter");
        let chunk = adapter.next_chunk().expect("chunk").expect("chunk");
        let events = VideoPerceptionWorker::new().process_chunk(&chunk);

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind.as_str(), "vision.observation.screen");
        assert_eq!(events[0].origin_device_id, Some(device.device_id));
    }
}
