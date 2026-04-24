//! Bounded interactive controller skeleton.

use serde::{Deserialize, Serialize};
use sinomo_core::{
    ActionRequest, Confidence, ContextPacket, EntityId, EventEnvelope, EventId, EventKind,
    LatencyClass, NodeId, PrivacyClass, RoutingMetadata, RoutingScope, Salience, SessionId, Source,
    TaskId, TriggerAction, TriggerDecision,
};
use sinomo_device::{DeviceKind, DeviceProfile};
use sinomo_events::{EventStore, ReplayDirection, ReplayQuery, StoredEvent};
use thiserror::Error;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct BoundedContext {
    pub session_id: SessionId,
    pub events: Vec<StoredEvent>,
    pub packet: ContextPacket,
}

pub struct BoundedContextBuilder {
    event_limit: usize,
}

impl BoundedContextBuilder {
    pub fn new(event_limit: usize) -> Self {
        Self { event_limit }
    }

    pub fn build<S: EventStore>(
        &self,
        store: &S,
        session_id: SessionId,
    ) -> Result<BoundedContext, AgentError> {
        let mut events = store.replay(ReplayQuery {
            session_id: Some(session_id.clone()),
            kind_prefix: None,
            origin_node_id: None,
            after_sequence: None,
            start_time: None,
            end_time: None,
            direction: ReplayDirection::Reverse,
            limit: Some(self.event_limit),
        })?;
        events.reverse();
        let packet = ContextPacket {
            salient_events: events
                .iter()
                .filter(|event| event.event.salience.get() >= 0.7)
                .map(|event| event.event.event_id.clone())
                .collect(),
            memory_records: Vec::<EntityId>::new(),
        };
        Ok(BoundedContext {
            session_id,
            events,
            packet,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct InteractiveTurn {
    pub response: AgentResponse,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action_request: Option<ActionRequest>,
    pub trace_event: EventEnvelope,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentResponse {
    pub mode: AgentResponseMode,
    pub text: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum AgentResponseMode {
    Acknowledge,
    Clarify,
    Answer,
    Defer,
}

#[derive(Clone, Debug)]
pub struct InteractiveAgent {
    node_id: NodeId,
    action_targets: Vec<DeviceProfile>,
}

impl InteractiveAgent {
    pub fn new(node_id: NodeId, action_targets: Vec<DeviceProfile>) -> Self {
        Self {
            node_id,
            action_targets,
        }
    }

    pub fn handle_trigger(
        &self,
        decision: &TriggerDecision,
        context: &BoundedContext,
    ) -> InteractiveTurn {
        let context_text = context_payload_text(context);
        let action_target = self.screen_target();
        let action_request = if matches!(
            decision.action,
            TriggerAction::Acknowledge | TriggerAction::Interrupt | TriggerAction::Alert
        ) && context_text.contains("alert")
        {
            action_target.as_ref().map(|device| ActionRequest {
                request_id: TaskId::generate(),
                origin_node_id: self.node_id.clone(),
                target_node_id: Some(device.node_id.clone()),
                target_device_id: Some(device.device_id.clone()),
                kind: "ui.inspect.alert".to_string(),
                payload: serde_json::json!({
                    "reason": decision.reason,
                    "context_event_count": context.events.len()
                }),
            })
        } else {
            None
        };

        let response = match decision.action {
            TriggerAction::StaySilent => AgentResponse {
                mode: AgentResponseMode::Defer,
                text: "I will keep watching this context.".to_string(),
            },
            TriggerAction::Acknowledge => AgentResponse {
                mode: AgentResponseMode::Acknowledge,
                text: "I see the alert context and will check it.".to_string(),
            },
            TriggerAction::Interrupt => AgentResponse {
                mode: AgentResponseMode::Acknowledge,
                text: "This looks urgent; I am checking the visible alert now.".to_string(),
            },
            TriggerAction::Alert => AgentResponse {
                mode: AgentResponseMode::Acknowledge,
                text: "A safety alert needs attention.".to_string(),
            },
            TriggerAction::Escalate => AgentResponse {
                mode: AgentResponseMode::Clarify,
                text: "I found a salient cross-modal observation and need a little more context."
                    .to_string(),
            },
        };

        let trace_event = self.trace_event(decision, context, &response, action_request.as_ref());

        InteractiveTurn {
            response,
            action_request,
            trace_event,
        }
    }

    fn screen_target(&self) -> Option<DeviceProfile> {
        self.action_targets
            .iter()
            .find(|device| matches!(device.kind, DeviceKind::Screen))
            .cloned()
    }

    fn trace_event(
        &self,
        decision: &TriggerDecision,
        context: &BoundedContext,
        response: &AgentResponse,
        action_request: Option<&ActionRequest>,
    ) -> EventEnvelope {
        let mut event = EventEnvelope::new(
            EventId::generate(),
            time::OffsetDateTime::now_utc(),
            self.node_id.clone(),
            Source::new("agent.interactive").expect("valid source"),
            EventKind::new("agent.turn.completed").expect("valid kind"),
            Salience::new(0.64).expect("valid salience"),
            Confidence::new(0.88).expect("valid confidence"),
            RoutingMetadata::new(
                RoutingScope::Session,
                LatencyClass::Interactive,
                PrivacyClass::Derived,
            ),
        )
        .with_session(context.session_id.clone())
        .with_payload(serde_json::json!({
            "trigger_event_id": decision.trigger_event_id,
            "trigger_action": decision.action,
            "response": response,
            "action_request": action_request,
            "bounded_context_event_count": context.events.len(),
            "salient_events": context.packet.salient_events,
        }));
        event
            .links
            .caused_by
            .push(decision.trigger_event_id.clone());
        event
    }
}

fn context_payload_text(context: &BoundedContext) -> String {
    let mut text = String::new();
    for event in &context.events {
        collect_json_text(&event.event.payload, &mut text);
    }
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

#[derive(Debug, Error)]
pub enum AgentError {
    #[error(transparent)]
    EventStore(#[from] sinomo_events::EventStoreError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::{
        Confidence, DeviceId, EventEnvelope, EventId, EventKind, LatencyClass, NodeId,
        PrivacyClass, RoutingMetadata, RoutingScope, Salience, Source,
    };
    use sinomo_device::DeviceState;
    use sinomo_events::{EventStore, InMemoryEventStore};

    fn event(id: &str, text: &str, salience: f32) -> EventEnvelope {
        EventEnvelope::new(
            EventId::new(id).expect("event"),
            time::OffsetDateTime::now_utc(),
            NodeId::new("node_desktop").expect("node"),
            Source::new("test.agent").expect("source"),
            EventKind::new("speech.transcript.final").expect("kind"),
            Salience::new(salience).expect("salience"),
            Confidence::new(0.9).expect("confidence"),
            RoutingMetadata::new(
                RoutingScope::Session,
                LatencyClass::Interactive,
                PrivacyClass::Derived,
            ),
        )
        .with_session(SessionId::new("sess_agent").expect("session"))
        .with_payload(serde_json::json!({ "text": text }))
    }

    #[test]
    fn bounded_context_limits_recent_events() {
        let mut store = InMemoryEventStore::new();
        store.append(event("evt_1", "one", 0.1)).expect("append");
        store.append(event("evt_2", "two", 0.8)).expect("append");
        store.append(event("evt_3", "three", 0.9)).expect("append");

        let context = BoundedContextBuilder::new(2)
            .build(&store, SessionId::new("sess_agent").expect("session"))
            .expect("context");

        assert_eq!(context.events.len(), 2);
        assert_eq!(context.packet.salient_events.len(), 2);
    }

    #[test]
    fn agent_routes_alert_action_to_screen_owner() {
        let node = NodeId::new("node_desktop").expect("node");
        let screen = DeviceProfile {
            device_id: DeviceId::new("device_screen").expect("device"),
            node_id: node.clone(),
            display_name: "screen".to_string(),
            kind: DeviceKind::Screen,
            capabilities: Vec::new(),
            permissions: Vec::new(),
            state: DeviceState::available(),
        };
        let context = BoundedContext {
            session_id: SessionId::new("sess_agent").expect("session"),
            events: vec![StoredEvent {
                sequence: sinomo_events::EventSequence(1),
                appended_at: time::OffsetDateTime::now_utc(),
                event: event("evt_alert", "urgent alert", 0.9),
            }],
            packet: ContextPacket::default(),
        };
        let decision = TriggerDecision {
            trigger_event_id: EventId::new("evt_alert").expect("event"),
            action: TriggerAction::Acknowledge,
            reason: "alert".to_string(),
        };

        let turn =
            InteractiveAgent::new(node.clone(), vec![screen]).handle_trigger(&decision, &context);

        let action = turn.action_request.expect("action");
        assert_eq!(action.target_node_id, Some(node));
        assert_eq!(action.kind, "ui.inspect.alert");
    }
}
