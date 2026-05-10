//! Rule-based trigger timing policy skeleton.

use sinomo_runtime::core::{
    Confidence, EventEnvelope, EventKind, LatencyClass, PrivacyClass, RoutingMetadata,
    RoutingScope, Salience, Source, TriggerAction, TriggerDecision,
};
use sinomo_runtime::events::{EventId, StoredEvent};

#[derive(Clone, Debug)]
pub struct TriggerPolicy {
    pub interrupt_salience_threshold: f32,
    pub escalate_salience_threshold: f32,
}

impl Default for TriggerPolicy {
    fn default() -> Self {
        Self {
            interrupt_salience_threshold: 0.9,
            escalate_salience_threshold: 0.8,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct RuleBasedTrigger {
    policy: TriggerPolicy,
}

impl RuleBasedTrigger {
    pub fn new(policy: TriggerPolicy) -> Self {
        Self { policy }
    }

    pub fn evaluate(&self, stored_event: &StoredEvent) -> TriggerDecision {
        let event = &stored_event.event;
        let payload_text = payload_text(event);
        let kind = event.kind.as_str();

        let (action, reason) = if kind.starts_with("safety.") {
            (
                TriggerAction::Alert,
                "safety event requires immediate alert".to_string(),
            )
        } else if payload_text.contains("urgent") || payload_text.contains("alert") {
            if event.salience.get() >= self.policy.interrupt_salience_threshold {
                (
                    TriggerAction::Interrupt,
                    "urgent high-salience cue should interrupt".to_string(),
                )
            } else {
                (
                    TriggerAction::Acknowledge,
                    "urgent cue should be acknowledged without interrupting".to_string(),
                )
            }
        } else if kind.starts_with("fusion.")
            && event.salience.get() >= self.policy.escalate_salience_threshold
        {
            (
                TriggerAction::Escalate,
                "salient fused observation should be escalated for interactive handling"
                    .to_string(),
            )
        } else {
            (
                TriggerAction::StaySilent,
                "event did not cross trigger thresholds".to_string(),
            )
        };

        TriggerDecision {
            trigger_event_id: event.event_id.clone(),
            action,
            reason,
        }
    }

    pub fn decision_event(
        &self,
        decision: &TriggerDecision,
        source_event: &EventEnvelope,
    ) -> EventEnvelope {
        let mut event = EventEnvelope::new(
            EventId::generate(),
            source_event.time,
            source_event.origin_node_id.clone(),
            Source::new("trigger.rule_based").expect("valid source"),
            EventKind::new("trigger.decision.created").expect("valid event kind"),
            Salience::new(0.5).expect("valid salience"),
            Confidence::new(1.0).expect("valid confidence"),
            RoutingMetadata::new(
                RoutingScope::Session,
                LatencyClass::Reactive,
                PrivacyClass::Derived,
            ),
        )
        .with_payload(serde_json::json!({
            "trigger_event_id": decision.trigger_event_id,
            "action": decision.action,
            "reason": decision.reason,
        }));
        if let Some(session_id) = source_event.session_id.clone() {
            event = event.with_session(session_id);
        }
        event.links.caused_by.push(source_event.event_id.clone());
        event
    }
}

fn payload_text(event: &EventEnvelope) -> String {
    let mut text = String::new();
    collect_json_text(&event.payload, &mut text);
    text.to_ascii_lowercase()
}

fn collect_json_text(value: &serde_json::Value, text: &mut String) {
    match value {
        serde_json::Value::String(value) => {
            text.push(' ');
            text.push_str(value);
        }
        serde_json::Value::Array(values) => {
            for value in values {
                collect_json_text(value, text);
            }
        }
        serde_json::Value::Object(map) => {
            for value in map.values() {
                collect_json_text(value, text);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_runtime::core::{
        Confidence, EventEnvelope, EventKind, LatencyClass, PrivacyClass, RoutingMetadata,
        RoutingScope, Salience, Source,
    };
    use sinomo_runtime::events::{EventSequence, SessionId};
    use sinomo_runtime::node::NodeId;

    fn stored(payload: serde_json::Value, salience: f32) -> StoredEvent {
        StoredEvent {
            sequence: EventSequence(1),
            appended_at: time::OffsetDateTime::now_utc(),
            event: EventEnvelope::new(
                EventId::from_legacy_named("evt_trigger").expect("event"),
                time::OffsetDateTime::now_utc(),
                NodeId::from_legacy_named("node_desktop").expect("node"),
                Source::new("test.trigger").expect("source"),
                EventKind::new("speech.transcript.final").expect("kind"),
                Salience::new(salience).expect("salience"),
                Confidence::new(0.95).expect("confidence"),
                RoutingMetadata::new(
                    RoutingScope::Session,
                    LatencyClass::Interactive,
                    PrivacyClass::Derived,
                ),
            )
            .with_session(SessionId::from_legacy_named("sess_trigger").expect("session"))
            .with_payload(payload),
        }
    }

    #[test]
    fn urgent_transcript_acknowledges() {
        let decision = RuleBasedTrigger::default().evaluate(&stored(
            serde_json::json!({ "text": "please check the alert" }),
            0.75,
        ));

        assert_eq!(decision.action, TriggerAction::Acknowledge);
    }

    #[test]
    fn high_salience_urgent_transcript_interrupts() {
        let decision = RuleBasedTrigger::default().evaluate(&stored(
            serde_json::json!({ "text": "urgent deployment alert" }),
            0.95,
        ));

        assert_eq!(decision.action, TriggerAction::Interrupt);
    }
}
