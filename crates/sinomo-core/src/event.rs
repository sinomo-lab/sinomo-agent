use crate::{
    Confidence, DeviceId, EntityRef, EventId, EventKind, NodeId, RoutingHint, Salience, SessionId,
    Source,
};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EventEnvelope {
    pub event_id: EventId,
    #[serde(with = "time::serde::rfc3339")]
    pub time: OffsetDateTime,
    pub origin_node_id: NodeId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_device_id: Option<DeviceId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_node_id: Option<NodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_device_id: Option<DeviceId>,
    pub source: Source,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<SessionId>,
    pub kind: EventKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub span: Option<EventSpan>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entities: Vec<EntityRef>,
    pub salience: Salience,
    pub confidence: Confidence,
    #[serde(default)]
    pub payload: serde_json::Value,
    #[serde(default)]
    pub evidence: Evidence,
    #[serde(default)]
    pub links: EventLinks,
    pub routing: RoutingMetadata,
}

impl EventEnvelope {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        event_id: EventId,
        time: OffsetDateTime,
        origin_node_id: NodeId,
        source: Source,
        kind: EventKind,
        salience: Salience,
        confidence: Confidence,
        routing: RoutingMetadata,
    ) -> Self {
        Self {
            event_id,
            time,
            origin_node_id,
            origin_device_id: None,
            target_node_id: None,
            target_device_id: None,
            source,
            session_id: None,
            kind,
            span: None,
            entities: Vec::new(),
            salience,
            confidence,
            payload: serde_json::Value::Null,
            evidence: Evidence::default(),
            links: EventLinks::default(),
            routing,
        }
    }

    pub fn with_origin_device(mut self, device_id: DeviceId) -> Self {
        self.origin_device_id = Some(device_id);
        self
    }

    pub fn with_target_node(mut self, node_id: NodeId) -> Self {
        self.target_node_id = Some(node_id);
        self
    }

    pub fn with_target_device(mut self, device_id: DeviceId) -> Self {
        self.target_device_id = Some(device_id);
        self
    }

    pub fn with_session(mut self, session_id: SessionId) -> Self {
        self.session_id = Some(session_id);
        self
    }

    pub fn with_payload(mut self, payload: serde_json::Value) -> Self {
        self.payload = payload;
        self
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EventSpan {
    pub start_ms: u64,
    pub end_ms: u64,
}

impl EventSpan {
    pub fn new(start_ms: u64, end_ms: u64) -> Self {
        Self { start_ms, end_ms }
    }

    pub fn is_valid(self) -> bool {
        self.start_ms <= self.end_ms
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct Evidence {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub media_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub refs: Vec<EvidenceRef>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EvidenceRef {
    pub uri: String,
    pub kind: EvidenceKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
}

impl EvidenceRef {
    pub fn new(uri: impl Into<String>, kind: EvidenceKind) -> Self {
        Self {
            uri: uri.into(),
            kind,
            content_hash: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum EvidenceKind {
    Audio,
    Video,
    Image,
    Screen,
    Transcript,
    Document,
    Artifact,
    Other,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct EventLinks {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub caused_by: Vec<EventId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub related_to: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub supersedes: Vec<EventId>,
}

impl EventLinks {
    pub fn references_event(&self, event_id: &EventId) -> bool {
        self.caused_by.iter().any(|id| id == event_id)
            || self.supersedes.iter().any(|id| id == event_id)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RoutingMetadata {
    pub scope: RoutingScope,
    pub latency_class: LatencyClass,
    pub privacy: PrivacyClass,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hints: Vec<RoutingHint>,
}

impl RoutingMetadata {
    pub fn new(scope: RoutingScope, latency_class: LatencyClass, privacy: PrivacyClass) -> Self {
        Self {
            scope,
            latency_class,
            privacy,
            hints: Vec::new(),
        }
    }

    pub fn with_hint(mut self, hint: RoutingHint) -> Self {
        self.hints.push(hint);
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum RoutingScope {
    Local,
    Session,
    TrustedNetwork,
    DurableGlobal,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum LatencyClass {
    Reactive,
    Interactive,
    Background,
    Archive,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum PrivacyClass {
    Raw,
    Derived,
    Private,
    Redacted,
    Public,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CapabilityName, EventId, NodeId};

    #[test]
    fn event_json_matches_documented_shape() {
        let event = EventEnvelope::new(
            EventId::new("evt_example").expect("event id"),
            OffsetDateTime::from_unix_timestamp(1_776_809_414).expect("timestamp"),
            NodeId::new("node_mobile_phone").expect("node id"),
            Source::new("audio.mic.frontdesk").expect("source"),
            EventKind::new("speech.transcript.final").expect("kind"),
            Salience::new(0.81).expect("salience"),
            Confidence::new(0.96).expect("confidence"),
            RoutingMetadata::new(
                RoutingScope::Session,
                LatencyClass::Interactive,
                PrivacyClass::Derived,
            )
            .with_hint(RoutingHint::new("target:desktop").expect("hint")),
        )
        .with_session(SessionId::new("sess_example").expect("session"))
        .with_payload(serde_json::json!({
            "text": "Please check the alert and tell me if it is urgent"
        }));

        let json = serde_json::to_value(&event).expect("serialize event");
        assert_eq!(json["event_id"], "evt_example");
        assert_eq!(json["kind"], "speech.transcript.final");
        assert_eq!(json["routing"]["scope"], "session");
        assert_eq!(json["routing"]["latency_class"], "interactive");

        let round_tripped: EventEnvelope = serde_json::from_value(json).expect("deserialize event");
        assert_eq!(round_tripped.event_id.as_str(), "evt_example");
    }

    #[test]
    fn event_span_validates_order() {
        assert!(EventSpan::new(1, 2).is_valid());
        assert!(!EventSpan::new(2, 1).is_valid());
    }

    #[test]
    fn capability_names_are_dotted() {
        assert!(CapabilityName::new("input.audio.vad").is_ok());
        assert!(CapabilityName::new("input").is_err());
    }
}
