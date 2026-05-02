//! Hot and warm memory policy skeleton.

use serde::{Deserialize, Serialize};
use sinomo_runtime::core::{ContextPacket, EntityId, MemoryRecord, SessionId, TrustDomain};
use sinomo_runtime::events::StoredEvent;
use std::collections::VecDeque;
use thiserror::Error;
use time::OffsetDateTime;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MemoryPolicy {
    pub hot_event_limit: usize,
    pub warm_salience_threshold: f32,
}

impl Default for MemoryPolicy {
    fn default() -> Self {
        Self {
            hot_event_limit: 32,
            warm_salience_threshold: 0.7,
        }
    }
}

#[derive(Clone, Debug)]
pub struct HotMemory {
    limit: usize,
    events: VecDeque<StoredEvent>,
}

impl HotMemory {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            events: VecDeque::new(),
        }
    }

    pub fn ingest(&mut self, event: StoredEvent) {
        self.events.push_back(event);
        while self.events.len() > self.limit {
            self.events.pop_front();
        }
    }

    pub fn recent(&self) -> Vec<StoredEvent> {
        self.events.iter().cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct WarmMemoryEntry {
    pub record: MemoryRecord,
    pub session_id: Option<SessionId>,
    pub reason: PromotionReason,
    #[serde(with = "time::serde::rfc3339")]
    pub promoted_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum PromotionReason {
    Salient,
    UserImportant,
    TaskOutcome,
    SafetyIncident,
    AgentTrace,
    DeliberationSummary,
}

#[derive(Clone, Debug, Default)]
pub struct WarmMemoryStore {
    entries: Vec<WarmMemoryEntry>,
}

impl WarmMemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn promote(&mut self, entry: WarmMemoryEntry) {
        self.entries.push(entry);
    }

    pub fn entries(&self) -> &[WarmMemoryEntry] {
        &self.entries
    }

    pub fn for_session(&self, session_id: &SessionId) -> Vec<WarmMemoryEntry> {
        self.entries
            .iter()
            .filter(|entry| entry.session_id.as_ref() == Some(session_id))
            .cloned()
            .collect()
    }
}

#[derive(Clone, Debug)]
pub struct MemoryManager {
    policy: MemoryPolicy,
    hot: HotMemory,
    warm: WarmMemoryStore,
}

impl MemoryManager {
    pub fn new(policy: MemoryPolicy) -> Self {
        Self {
            hot: HotMemory::new(policy.hot_event_limit),
            warm: WarmMemoryStore::new(),
            policy,
        }
    }

    pub fn ingest(&mut self, event: StoredEvent) -> Option<WarmMemoryEntry> {
        let promoted = self.maybe_promote(&event);
        self.hot.ingest(event);
        if let Some(entry) = promoted.clone() {
            self.warm.promote(entry);
        }
        promoted
    }

    pub fn hot(&self) -> &HotMemory {
        &self.hot
    }

    pub fn warm(&self) -> &WarmMemoryStore {
        &self.warm
    }

    pub fn context_packet(&self, session_id: &SessionId) -> ContextPacket {
        let salient_events = self
            .hot
            .events
            .iter()
            .filter(|stored| {
                stored.event.session_id.as_ref() == Some(session_id)
                    && stored.event.salience.get() >= self.policy.warm_salience_threshold
            })
            .map(|stored| stored.event.event_id.clone())
            .collect();
        let memory_records = self
            .warm
            .for_session(session_id)
            .into_iter()
            .map(|entry| entry.record.entity_id)
            .collect();

        ContextPacket {
            salient_events,
            memory_records,
        }
    }

    fn maybe_promote(&self, stored: &StoredEvent) -> Option<WarmMemoryEntry> {
        let event = &stored.event;
        let reason = promotion_reason(event)?;
        if event.salience.get() < self.policy.warm_salience_threshold
            && !matches!(
                reason,
                PromotionReason::SafetyIncident
                    | PromotionReason::AgentTrace
                    | PromotionReason::DeliberationSummary
            )
        {
            return None;
        }

        Some(WarmMemoryEntry {
            record: MemoryRecord {
                entity_id: EntityId::generate(),
                source_event_id: Some(event.event_id.clone()),
                summary: summarize_event(event),
                visibility: TrustDomain::new("trust.local").expect("valid trust domain"),
            },
            session_id: event.session_id.clone(),
            reason,
            promoted_at: OffsetDateTime::now_utc(),
        })
    }
}

fn promotion_reason(event: &sinomo_runtime::core::EventEnvelope) -> Option<PromotionReason> {
    let kind = event.kind.as_str();
    if kind.starts_with("safety.") {
        Some(PromotionReason::SafetyIncident)
    } else if kind == "agent.turn.completed" {
        Some(PromotionReason::AgentTrace)
    } else if kind == "deliberation.summary.completed" {
        Some(PromotionReason::DeliberationSummary)
    } else if kind.starts_with("speech.")
        || kind.starts_with("vision.")
        || kind.starts_with("fusion.")
    {
        Some(PromotionReason::Salient)
    } else {
        None
    }
}

fn summarize_event(event: &sinomo_runtime::core::EventEnvelope) -> String {
    let mut text = String::new();
    collect_text(&event.payload, &mut text);
    let text = text.trim();
    if text.is_empty() {
        format!("{} from {}", event.kind, event.source)
    } else {
        format!("{}: {}", event.kind, text)
    }
}

fn collect_text(value: &serde_json::Value, text: &mut String) {
    match value {
        serde_json::Value::String(value) => {
            text.push(' ');
            text.push_str(value);
        }
        serde_json::Value::Array(values) => {
            for value in values {
                collect_text(value, text);
            }
        }
        serde_json::Value::Object(map) => {
            for value in map.values() {
                collect_text(value, text);
            }
        }
        _ => {}
    }
}

#[derive(Debug, Error)]
pub enum MemoryError {
    #[error("memory context is empty")]
    EmptyContext,
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_runtime::core::{
        Confidence, EventEnvelope, EventId, EventKind, LatencyClass, NodeId, PrivacyClass,
        RoutingMetadata, RoutingScope, Salience, Source,
    };
    use sinomo_runtime::events::EventSequence;

    fn stored(id: &str, kind: &str, salience: f32) -> StoredEvent {
        StoredEvent {
            sequence: EventSequence(1),
            appended_at: OffsetDateTime::now_utc(),
            event: EventEnvelope::new(
                EventId::new(id).expect("event"),
                OffsetDateTime::now_utc(),
                NodeId::new("node_desktop").expect("node"),
                Source::new("test.memory").expect("source"),
                EventKind::new(kind).expect("kind"),
                Salience::new(salience).expect("salience"),
                Confidence::new(0.9).expect("confidence"),
                RoutingMetadata::new(
                    RoutingScope::Session,
                    LatencyClass::Interactive,
                    PrivacyClass::Derived,
                ),
            )
            .with_session(SessionId::new("sess_memory").expect("session"))
            .with_payload(serde_json::json!({ "text": "urgent alert" })),
        }
    }

    #[test]
    fn hot_memory_keeps_bounded_recent_events() {
        let mut hot = HotMemory::new(2);
        hot.ingest(stored("evt_1", "speech.transcript.final", 0.2));
        hot.ingest(stored("evt_2", "speech.transcript.final", 0.2));
        hot.ingest(stored("evt_3", "speech.transcript.final", 0.2));

        assert_eq!(hot.len(), 2);
        assert_eq!(hot.recent()[0].event.event_id.as_str(), "evt_2");
    }

    #[test]
    fn salient_events_promote_to_warm_memory_and_context_packet() {
        let mut manager = MemoryManager::new(MemoryPolicy {
            hot_event_limit: 4,
            warm_salience_threshold: 0.7,
        });
        manager.ingest(stored("evt_salient", "speech.transcript.final", 0.9));

        let packet = manager.context_packet(&SessionId::new("sess_memory").expect("session"));
        assert_eq!(manager.warm().entries().len(), 1);
        assert_eq!(packet.salient_events.len(), 1);
        assert_eq!(packet.memory_records.len(), 1);
    }
}
