use sinomo_core::{
    CapabilityName, Confidence, DeviceCapability, EventEnvelope, EventId, EventKind, LatencyClass,
    NodeCapability, PrivacyClass, RoutingHint, RoutingMetadata, RoutingScope, Salience, SessionId,
    Source,
};
use sinomo_device::{DevicePermission, DeviceProfile};
use sinomo_events::{EventStore, InMemoryEventStore, ReplayQuery};
use sinomo_node::{HostedService, NodeProfile};
use time::OffsetDateTime;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let speech_capability = CapabilityName::new("input.audio.vad")?;
    let event_service_capability = CapabilityName::new("service.events.local")?;

    let node = NodeProfile::local("local workstation")?
        .with_capability(NodeCapability::new(event_service_capability.clone()))
        .with_service(HostedService::new(event_service_capability));

    let microphone = DeviceProfile::mock_microphone(node.node_id.clone())
        .with_capability(DeviceCapability::new(speech_capability))
        .with_permission(DevicePermission::Observe);

    let session_id = SessionId::generate();
    let event = EventEnvelope::new(
        EventId::generate(),
        OffsetDateTime::now_utc(),
        node.node_id.clone(),
        Source::new("audio.mock.microphone")?,
        EventKind::new("speech.transcript.final")?,
        Salience::new(0.74)?,
        Confidence::new(0.97)?,
        RoutingMetadata::new(
            RoutingScope::Session,
            LatencyClass::Interactive,
            PrivacyClass::Derived,
        )
        .with_hint(RoutingHint::new("target:local")?),
    )
    .with_origin_device(microphone.device_id.clone())
    .with_session(session_id.clone())
    .with_payload(serde_json::json!({
        "text": "Phase one event backbone smoke test"
    }));

    let mut store = InMemoryEventStore::new();
    let receipt = store.append(event)?;
    let replayed = store.replay(ReplayQuery::by_session(session_id))?;

    println!(
        "sinomo-app: appended {} sequence={} kind=speech.transcript.final",
        receipt.event_id, receipt.sequence.0
    );
    println!("sinomo-app: replayed {} event(s)", replayed.len());

    Ok(())
}
