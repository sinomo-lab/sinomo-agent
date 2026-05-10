//! Cross-modal alignment skeleton.

use sinomo_runtime::core::{
    Confidence, EventEnvelope, EventKind, EventLinks, LatencyClass, PrivacyClass, RoutingMetadata,
    RoutingScope, Salience, Source,
};
use sinomo_runtime::events::EventId;

#[derive(Clone, Debug, Default)]
pub struct FusionWorker;

impl FusionWorker {
    pub fn new() -> Self {
        Self
    }

    pub fn align(&self, observations: &[EventEnvelope]) -> Option<EventEnvelope> {
        if observations.len() < 2 {
            return None;
        }

        let first = observations.first()?;
        let session_id = first.session_id.clone()?;
        let mut linked_ids = Vec::new();
        let mut kinds = Vec::new();

        for observation in observations {
            linked_ids.push(observation.event_id.clone());
            kinds.push(observation.kind.as_str().to_string());
        }

        let mut event = EventEnvelope::new(
            EventId::generate(),
            first.time,
            first.origin_node_id.clone(),
            Source::new("fusion.synthetic.alignment").expect("valid source"),
            EventKind::new("fusion.observation.aligned").expect("valid kind"),
            Salience::new(0.76).expect("valid salience"),
            Confidence::new(0.84).expect("valid confidence"),
            RoutingMetadata::new(
                RoutingScope::Session,
                LatencyClass::Interactive,
                PrivacyClass::Derived,
            ),
        )
        .with_session(session_id)
        .with_payload(serde_json::json!({
            "aligned_event_kinds": kinds,
            "observation_count": observations.len()
        }));
        event.links = EventLinks {
            caused_by: linked_ids,
            related_to: Vec::new(),
            supersedes: Vec::new(),
        };
        event
            .routing
            .hints
            .push(sinomo_runtime::core::RoutingHint::new("requires:context-packet").expect("hint"));
        Some(event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_runtime::core::{
        Confidence, EventEnvelope, EventKind, LatencyClass, PrivacyClass, RoutingMetadata,
        RoutingScope, Salience, Source,
    };
    use sinomo_runtime::events::SessionId;
    use sinomo_runtime::node::NodeId;

    fn observation(id: &str, kind: &str) -> EventEnvelope {
        EventEnvelope::new(
            EventId::from_legacy_named(id).expect("event"),
            time::OffsetDateTime::now_utc(),
            NodeId::from_legacy_named("node_desktop").expect("node"),
            Source::new("test.observation").expect("source"),
            EventKind::new(kind).expect("kind"),
            Salience::new(0.5).expect("salience"),
            Confidence::new(0.9).expect("confidence"),
            RoutingMetadata::new(
                RoutingScope::Session,
                LatencyClass::Interactive,
                PrivacyClass::Derived,
            ),
        )
        .with_session(SessionId::from_legacy_named("sess_fusion").expect("session"))
    }

    #[test]
    fn fusion_links_observations() {
        let fused = FusionWorker::new()
            .align(&[
                observation("evt_audio", "speech.transcript.final"),
                observation("evt_video", "vision.observation.screen"),
            ])
            .expect("fused event");

        assert_eq!(fused.kind.as_str(), "fusion.observation.aligned");
        assert_eq!(fused.links.caused_by.len(), 2);
    }
}
