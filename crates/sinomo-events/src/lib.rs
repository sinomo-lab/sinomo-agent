//! Event validation, append, and replay backbone.

use serde::{Deserialize, Serialize};
use sinomo_core::{EventEnvelope, EventId, EventKind, NodeId, SessionId};
use std::collections::HashSet;
use thiserror::Error;
use time::OffsetDateTime;

#[derive(Clone, Debug, Default)]
pub struct EventValidator;

impl EventValidator {
    pub fn validate(&self, event: &EventEnvelope) -> Result<(), EventValidationError> {
        if event.source.as_str().trim().is_empty() {
            return Err(EventValidationError::EmptySource);
        }

        if event.kind.as_str().trim().is_empty() {
            return Err(EventValidationError::EmptyKind);
        }

        if let Some(span) = event.span
            && !span.is_valid()
        {
            return Err(EventValidationError::InvalidSpan {
                start_ms: span.start_ms,
                end_ms: span.end_ms,
            });
        }

        if event.links.references_event(&event.event_id) {
            return Err(EventValidationError::SelfReferentialLink {
                event_id: event.event_id.clone(),
            });
        }

        Ok(())
    }
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum EventValidationError {
    #[error("event source cannot be empty")]
    EmptySource,
    #[error("event kind cannot be empty")]
    EmptyKind,
    #[error("event span start_ms {start_ms} cannot be greater than end_ms {end_ms}")]
    InvalidSpan { start_ms: u64, end_ms: u64 },
    #[error("event {event_id} cannot link to itself")]
    SelfReferentialLink { event_id: EventId },
}

pub trait EventStore {
    fn append(&mut self, event: EventEnvelope) -> Result<AppendReceipt, EventStoreError>;
    fn replay(&self, query: ReplayQuery) -> Result<Vec<StoredEvent>, EventStoreError>;
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AppendReceipt {
    pub event_id: EventId,
    pub sequence: EventSequence,
    #[serde(with = "time::serde::rfc3339")]
    pub appended_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct StoredEvent {
    pub sequence: EventSequence,
    #[serde(with = "time::serde::rfc3339")]
    pub appended_at: OffsetDateTime,
    pub event: EventEnvelope,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct EventSequence(pub u64);

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct EventCursor {
    pub sequence: EventSequence,
}

impl From<EventSequence> for EventCursor {
    fn from(sequence: EventSequence) -> Self {
        Self { sequence }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ReplayQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<SessionId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind_prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_node_id: Option<NodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_sequence: Option<EventSequence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "time::serde::rfc3339::option")]
    pub start_time: Option<OffsetDateTime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "time::serde::rfc3339::option")]
    pub end_time: Option<OffsetDateTime>,
    pub direction: ReplayDirection,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

impl ReplayQuery {
    pub fn all() -> Self {
        Self::default()
    }

    pub fn by_session(session_id: SessionId) -> Self {
        Self {
            session_id: Some(session_id),
            ..Self::default()
        }
    }

    pub fn by_kind_prefix(kind_prefix: impl Into<String>) -> Self {
        Self {
            kind_prefix: Some(kind_prefix.into()),
            ..Self::default()
        }
    }

    pub fn with_origin_node(mut self, node_id: NodeId) -> Self {
        self.origin_node_id = Some(node_id);
        self
    }

    pub fn after(mut self, sequence: EventSequence) -> Self {
        self.after_sequence = Some(sequence);
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
}

impl Default for ReplayQuery {
    fn default() -> Self {
        Self {
            session_id: None,
            kind_prefix: None,
            origin_node_id: None,
            after_sequence: None,
            start_time: None,
            end_time: None,
            direction: ReplayDirection::Forward,
            limit: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ReplayDirection {
    Forward,
    Reverse,
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum EventStoreError {
    #[error(transparent)]
    Validation(#[from] EventValidationError),
    #[error("event id already exists: {event_id}")]
    DuplicateEventId { event_id: EventId },
}

#[derive(Clone, Debug, Default)]
pub struct InMemoryEventStore {
    validator: EventValidator,
    events: Vec<StoredEvent>,
    event_ids: HashSet<EventId>,
}

impl InMemoryEventStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_validator(validator: EventValidator) -> Self {
        Self {
            validator,
            events: Vec::new(),
            event_ids: HashSet::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    fn matches_query(stored: &StoredEvent, query: &ReplayQuery) -> bool {
        if let Some(session_id) = &query.session_id
            && stored.event.session_id.as_ref() != Some(session_id)
        {
            return false;
        }

        if let Some(kind_prefix) = &query.kind_prefix
            && !stored.event.kind.as_str().starts_with(kind_prefix)
        {
            return false;
        }

        if let Some(origin_node_id) = &query.origin_node_id
            && &stored.event.origin_node_id != origin_node_id
        {
            return false;
        }

        if let Some(after_sequence) = query.after_sequence
            && stored.sequence <= after_sequence
        {
            return false;
        }

        if let Some(start_time) = query.start_time
            && stored.event.time < start_time
        {
            return false;
        }

        if let Some(end_time) = query.end_time
            && stored.event.time > end_time
        {
            return false;
        }

        true
    }
}

impl EventStore for InMemoryEventStore {
    fn append(&mut self, event: EventEnvelope) -> Result<AppendReceipt, EventStoreError> {
        self.validator.validate(&event)?;

        if self.event_ids.contains(&event.event_id) {
            return Err(EventStoreError::DuplicateEventId {
                event_id: event.event_id,
            });
        }

        let sequence = EventSequence((self.events.len() as u64) + 1);
        let appended_at = OffsetDateTime::now_utc();
        let event_id = event.event_id.clone();

        self.event_ids.insert(event_id.clone());
        self.events.push(StoredEvent {
            sequence,
            appended_at,
            event,
        });

        Ok(AppendReceipt {
            event_id,
            sequence,
            appended_at,
        })
    }

    fn replay(&self, query: ReplayQuery) -> Result<Vec<StoredEvent>, EventStoreError> {
        let mut events: Vec<StoredEvent> = self
            .events
            .iter()
            .filter(|stored| Self::matches_query(stored, &query))
            .cloned()
            .collect();

        if query.direction == ReplayDirection::Reverse {
            events.reverse();
        }

        if let Some(limit) = query.limit {
            events.truncate(limit);
        }

        Ok(events)
    }
}

pub fn kind_has_prefix(kind: &EventKind, prefix: &str) -> bool {
    kind.as_str().starts_with(prefix)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::{
        Confidence, EventEnvelope, EventId, EventKind, EventLinks, EventSpan, LatencyClass, NodeId,
        PrivacyClass, RoutingMetadata, RoutingScope, Salience, SessionId, Source,
    };

    fn fixture_event(id: &str, session_id: Option<&str>, kind: &str) -> EventEnvelope {
        let mut event = EventEnvelope::new(
            EventId::new(id).expect("event id"),
            OffsetDateTime::from_unix_timestamp(1_776_809_414).expect("timestamp"),
            NodeId::new("node_desktop").expect("node id"),
            Source::new("test.fixture").expect("source"),
            EventKind::new(kind).expect("kind"),
            Salience::new(0.5).expect("salience"),
            Confidence::new(0.9).expect("confidence"),
            RoutingMetadata::new(
                RoutingScope::Session,
                LatencyClass::Interactive,
                PrivacyClass::Derived,
            ),
        );

        if let Some(session_id) = session_id {
            event = event.with_session(SessionId::new(session_id).expect("session id"));
        }

        event
    }

    #[test]
    fn validator_rejects_invalid_span() {
        let mut event = fixture_event("evt_invalid_span", None, "speech.transcript.final");
        event.span = Some(EventSpan::new(10, 1));

        let error = EventValidator
            .validate(&event)
            .expect_err("invalid span should fail");
        assert!(matches!(error, EventValidationError::InvalidSpan { .. }));
    }

    #[test]
    fn validator_rejects_self_reference() {
        let mut event = fixture_event("evt_self_ref", None, "agent.trace.created");
        event.links = EventLinks {
            caused_by: vec![EventId::new("evt_self_ref").expect("event id")],
            related_to: Vec::new(),
            supersedes: Vec::new(),
        };

        let error = EventValidator
            .validate(&event)
            .expect_err("self reference should fail");
        assert!(matches!(
            error,
            EventValidationError::SelfReferentialLink { .. }
        ));
    }

    #[test]
    fn store_rejects_duplicate_event_ids() {
        let mut store = InMemoryEventStore::new();
        let event = fixture_event("evt_duplicate", None, "speech.transcript.final");

        store.append(event.clone()).expect("first append");
        let error = store.append(event).expect_err("duplicate append");

        assert!(matches!(error, EventStoreError::DuplicateEventId { .. }));
    }

    #[test]
    fn replay_filters_by_session_kind_and_cursor() {
        let mut store = InMemoryEventStore::new();
        store
            .append(fixture_event(
                "evt_one",
                Some("sess_main"),
                "speech.transcript.partial",
            ))
            .expect("append one");
        let second = store
            .append(fixture_event(
                "evt_two",
                Some("sess_main"),
                "speech.transcript.final",
            ))
            .expect("append two");
        store
            .append(fixture_event(
                "evt_three",
                Some("sess_other"),
                "agent.trace.created",
            ))
            .expect("append three");

        let session_events = store
            .replay(ReplayQuery::by_session(
                SessionId::new("sess_main").expect("session id"),
            ))
            .expect("replay by session");
        assert_eq!(session_events.len(), 2);

        let speech_events = store
            .replay(ReplayQuery::by_kind_prefix("speech."))
            .expect("replay by kind");
        assert_eq!(speech_events.len(), 2);

        let after_second = store
            .replay(ReplayQuery::all().after(second.sequence))
            .expect("replay after cursor");
        assert_eq!(after_second.len(), 1);
        assert_eq!(after_second[0].event.event_id.as_str(), "evt_three");
    }
}
