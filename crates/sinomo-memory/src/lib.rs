//! Hot memory, immutable memory records, and memory context retrieval.

use serde::{Deserialize, Serialize};
use sinomo_runtime::core::{
    ContextPacket, EventEnvelope, MemoryId, MemoryRecord, MemoryScope, MemorySourceRef, MemoryTier,
    TrustDomain,
};
use sinomo_runtime::events::{SessionId, StoredEvent};
use std::collections::VecDeque;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use thiserror::Error;
use time::OffsetDateTime;

const DEFAULT_RETRIEVAL_RECORD_LIMIT: usize = 8;
const DEFAULT_RETRIEVAL_BYTE_LIMIT: usize = 16 * 1024;

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
            .map(|entry| entry.record.memory_id)
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

        let promoted_at = OffsetDateTime::now_utc();
        let mut source_refs = vec![MemorySourceRef::new("event", event.event_id.to_string())];
        if let Some(session_id) = &event.session_id {
            source_refs.push(MemorySourceRef::new("session", session_id.to_string()));
        }

        Some(WarmMemoryEntry {
            record: MemoryRecord::new(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                summarize_event(event),
                source_refs,
                TrustDomain::new("trust.local").expect("valid trust domain"),
                promoted_at,
            ),
            session_id: event.session_id.clone(),
            reason,
            promoted_at,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<SessionId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_hint: Option<String>,
    pub max_records: usize,
    pub max_bytes: usize,
}

impl Default for MemoryQuery {
    fn default() -> Self {
        Self {
            session_id: None,
            agent_id: None,
            task_hint: None,
            max_records: DEFAULT_RETRIEVAL_RECORD_LIMIT,
            max_bytes: DEFAULT_RETRIEVAL_BYTE_LIMIT,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RetrievedMemoryRecord {
    pub record: MemoryRecord,
    pub reason: String,
    pub score: i32,
    pub bytes: usize,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryRetrievalReport {
    pub considered_records: usize,
    pub selected_records: usize,
    pub selected_bytes: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub omitted: Vec<MemoryRetrievalOmission>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryRetrievalOmission {
    pub memory_id: MemoryId,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryIndexEntry {
    pub memory_id: MemoryId,
    pub scope: MemoryScope,
    pub tier: MemoryTier,
    pub path: String,
    pub bytes: usize,
    #[serde(with = "time::serde::rfc3339")]
    pub indexed_at: OffsetDateTime,
}

pub trait MemoryContextProvider: Send + Sync {
    fn retrieve(&self, query: &MemoryQuery) -> Result<MemoryRetrievalResult, MemoryError>;
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryRetrievalResult {
    pub records: Vec<RetrievedMemoryRecord>,
    pub report: MemoryRetrievalReport,
}

#[derive(Clone, Debug)]
pub struct FlatFileMemoryStore {
    root: PathBuf,
}

impl FlatFileMemoryStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn write_record(&self, record: MemoryRecord) -> Result<MemoryRecord, MemoryError> {
        let expected = MemoryRecord::new(
            record.scope.clone(),
            record.tier.clone(),
            record.summary.clone(),
            record.source_refs.clone(),
            record.visibility.clone(),
            record.created_at,
        );
        if expected.memory_id != record.memory_id {
            return Err(MemoryError::NonCanonicalMemoryId {
                expected: expected.memory_id,
                actual: record.memory_id,
            });
        }

        let record_path = self.record_path(&record.scope, &record.memory_id);
        let record_existed = record_path.exists();
        if !record_existed {
            if let Some(parent) = record_path.parent() {
                fs::create_dir_all(parent).map_err(|source| MemoryError::Io {
                    path: parent.display().to_string(),
                    source,
                })?;
            }
            let bytes =
                serde_json::to_vec_pretty(&record).map_err(|source| MemoryError::Serialize {
                    path: record_path.display().to_string(),
                    source,
                })?;
            let tmp_path = record_path.with_extension(format!("json.tmp-{}", std::process::id()));
            fs::write(&tmp_path, bytes).map_err(|source| MemoryError::Io {
                path: tmp_path.display().to_string(),
                source,
            })?;
            fs::rename(&tmp_path, &record_path).map_err(|source| MemoryError::Io {
                path: record_path.display().to_string(),
                source,
            })?;
        }

        if !record_existed {
            self.append_index(&record, &record_path)?;
        }

        Ok(record)
    }

    pub fn put_record(
        &self,
        scope: MemoryScope,
        tier: MemoryTier,
        summary: impl Into<String>,
        source_refs: Vec<MemorySourceRef>,
        visibility: TrustDomain,
        created_at: OffsetDateTime,
    ) -> Result<MemoryRecord, MemoryError> {
        self.write_record(MemoryRecord::new(
            scope,
            tier,
            summary,
            source_refs,
            visibility,
            created_at,
        ))
    }

    pub fn records_for_scope(&self, scope: &MemoryScope) -> Result<Vec<MemoryRecord>, MemoryError> {
        let records_dir = self.scope_dir(scope).join("records");
        if !records_dir.exists() {
            return Ok(Vec::new());
        }
        let mut records = Vec::new();
        for entry in fs::read_dir(&records_dir).map_err(|source| MemoryError::Io {
            path: records_dir.display().to_string(),
            source,
        })? {
            let entry = entry.map_err(|source| MemoryError::Io {
                path: records_dir.display().to_string(),
                source,
            })?;
            let path = entry.path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
                continue;
            }
            let bytes = fs::read(&path).map_err(|source| MemoryError::Io {
                path: path.display().to_string(),
                source,
            })?;
            let record: MemoryRecord =
                serde_json::from_slice(&bytes).map_err(|source| MemoryError::Deserialize {
                    path: path.display().to_string(),
                    source,
                })?;
            records.push(record);
        }
        records.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.memory_id.cmp(&right.memory_id))
        });
        Ok(records)
    }

    fn retrieve_records(&self, query: &MemoryQuery) -> Result<MemoryRetrievalResult, MemoryError> {
        let mut candidates = Vec::new();
        candidates.extend(self.records_for_scope(&MemoryScope::Cluster)?);
        if let Some(agent_id) = &query.agent_id {
            candidates.extend(self.records_for_scope(&MemoryScope::Agent {
                agent_id: agent_id.clone(),
            })?);
        }

        let mut scored = candidates
            .into_iter()
            .map(|record| {
                let bytes = serde_json::to_vec(&record)
                    .map(|bytes| bytes.len())
                    .unwrap_or_default();
                let (score, reason) = score_record(&record, query);
                RetrievedMemoryRecord {
                    record,
                    reason,
                    score,
                    bytes,
                }
            })
            .collect::<Vec<_>>();
        scored.sort_by(|left, right| {
            right
                .score
                .cmp(&left.score)
                .then_with(|| {
                    memory_scope_rank(&left.record.scope)
                        .cmp(&memory_scope_rank(&right.record.scope))
                })
                .then_with(|| left.record.created_at.cmp(&right.record.created_at))
                .then_with(|| left.record.memory_id.cmp(&right.record.memory_id))
        });

        let mut selected = Vec::new();
        let mut omitted = Vec::new();
        let mut selected_bytes = 0usize;
        for candidate in scored {
            if selected.len() >= query.max_records {
                omitted.push(MemoryRetrievalOmission {
                    memory_id: candidate.record.memory_id,
                    reason: "record limit reached".to_string(),
                });
                continue;
            }
            if selected_bytes + candidate.bytes > query.max_bytes {
                omitted.push(MemoryRetrievalOmission {
                    memory_id: candidate.record.memory_id,
                    reason: "byte limit reached".to_string(),
                });
                continue;
            }
            selected_bytes += candidate.bytes;
            selected.push(candidate);
        }

        Ok(MemoryRetrievalResult {
            report: MemoryRetrievalReport {
                considered_records: selected.len() + omitted.len(),
                selected_records: selected.len(),
                selected_bytes,
                omitted,
            },
            records: selected,
        })
    }

    fn append_index(&self, record: &MemoryRecord, record_path: &Path) -> Result<(), MemoryError> {
        let scope_dir = self.scope_dir(&record.scope);
        fs::create_dir_all(&scope_dir).map_err(|source| MemoryError::Io {
            path: scope_dir.display().to_string(),
            source,
        })?;
        let index_path = scope_dir.join("index.jsonl");
        let relative_path = record_path
            .strip_prefix(&scope_dir)
            .unwrap_or(record_path)
            .display()
            .to_string();
        let bytes = fs::metadata(record_path)
            .map(|metadata| metadata.len() as usize)
            .unwrap_or_default();
        let entry = MemoryIndexEntry {
            memory_id: record.memory_id.clone(),
            scope: record.scope.clone(),
            tier: record.tier.clone(),
            path: relative_path,
            bytes,
            indexed_at: OffsetDateTime::now_utc(),
        };
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&index_path)
            .map_err(|source| MemoryError::Io {
                path: index_path.display().to_string(),
                source,
            })?;
        let line = serde_json::to_string(&entry).map_err(|source| MemoryError::Serialize {
            path: index_path.display().to_string(),
            source,
        })?;
        writeln!(file, "{line}").map_err(|source| MemoryError::Io {
            path: index_path.display().to_string(),
            source,
        })?;
        Ok(())
    }

    fn record_path(&self, scope: &MemoryScope, memory_id: &MemoryId) -> PathBuf {
        self.scope_dir(scope)
            .join("records")
            .join(format!("{memory_id}.json"))
    }

    fn scope_dir(&self, scope: &MemoryScope) -> PathBuf {
        match scope {
            MemoryScope::Cluster => self.root.join("cluster"),
            MemoryScope::Agent { agent_id } => {
                self.root.join("agents").join(safe_component(agent_id))
            }
        }
    }
}

impl MemoryContextProvider for FlatFileMemoryStore {
    fn retrieve(&self, query: &MemoryQuery) -> Result<MemoryRetrievalResult, MemoryError> {
        self.retrieve_records(query)
    }
}

fn memory_scope_rank(scope: &MemoryScope) -> u8 {
    match scope {
        MemoryScope::Cluster => 0,
        MemoryScope::Agent { .. } => 1,
    }
}

fn score_record(record: &MemoryRecord, query: &MemoryQuery) -> (i32, String) {
    let mut score = 0i32;
    let mut reasons = Vec::new();
    if let Some(session_id) = &query.session_id
        && record
            .source_refs
            .iter()
            .any(|source| source.kind == "session" && source.value == session_id.to_string())
    {
        score += 100;
        reasons.push("same session");
    }
    if let Some(agent_id) = &query.agent_id
        && matches!(&record.scope, MemoryScope::Agent { agent_id: scoped } if scoped == agent_id)
    {
        score += 20;
        reasons.push("same agent");
    }
    if let Some(task_hint) = &query.task_hint {
        let hint = task_hint.to_ascii_lowercase();
        if !hint.trim().is_empty()
            && record
                .summary
                .to_ascii_lowercase()
                .split_whitespace()
                .any(|word| hint.contains(word))
        {
            score += 10;
            reasons.push("task hint match");
        }
    }
    if reasons.is_empty() {
        reasons.push("available memory");
    }
    (score, reasons.join(", "))
}

fn safe_component(value: &str) -> String {
    let mut safe = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            safe.push(ch);
        } else {
            safe.push('_');
        }
    }
    if safe.is_empty() {
        "unnamed".to_string()
    } else {
        safe
    }
}

fn promotion_reason(event: &EventEnvelope) -> Option<PromotionReason> {
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

fn summarize_event(event: &EventEnvelope) -> String {
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
    #[error("memory id is not canonical for record content: expected {expected}, got {actual}")]
    NonCanonicalMemoryId {
        expected: MemoryId,
        actual: MemoryId,
    },
    #[error("memory I/O error at {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to serialize memory data for {path}: {source}")]
    Serialize {
        path: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to deserialize memory data from {path}: {source}")]
    Deserialize {
        path: String,
        #[source]
        source: serde_json::Error,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_runtime::core::{
        Confidence, EventEnvelope, EventKind, Id, LatencyClass, PrivacyClass, RoutingMetadata,
        RoutingScope, Salience, Source,
    };
    use sinomo_runtime::events::{EventId, EventSequence};
    use sinomo_runtime::node::NodeId;

    fn stored(id: &str, kind: &str, salience: f32) -> StoredEvent {
        StoredEvent {
            sequence: EventSequence(1),
            appended_at: OffsetDateTime::now_utc(),
            event: EventEnvelope::new(
                EventId::from_legacy_named(id).expect("event"),
                OffsetDateTime::now_utc(),
                NodeId::from_legacy_named("node_desktop").expect("node"),
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
            .with_session(SessionId::from_legacy_named("sess_memory").expect("session"))
            .with_payload(serde_json::json!({ "text": "urgent alert" })),
        }
    }

    fn temp_memory_root(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("{label}-{}", Id::generate()));
        fs::create_dir_all(&path).expect("temp memory root");
        path
    }

    #[test]
    fn hot_memory_keeps_bounded_recent_events() {
        let mut hot = HotMemory::new(2);
        hot.ingest(stored("evt_1", "speech.transcript.final", 0.2));
        hot.ingest(stored("evt_2", "speech.transcript.final", 0.2));
        hot.ingest(stored("evt_3", "speech.transcript.final", 0.2));

        assert_eq!(hot.len(), 2);
        assert_eq!(
            hot.recent()[0].event.event_id,
            EventId::from_legacy_named("evt_2").expect("event")
        );
    }

    #[test]
    fn salient_events_promote_to_warm_memory_and_context_packet() {
        let mut manager = MemoryManager::new(MemoryPolicy {
            hot_event_limit: 4,
            warm_salience_threshold: 0.7,
        });
        manager.ingest(stored("evt_salient", "speech.transcript.final", 0.9));

        let packet =
            manager.context_packet(&SessionId::from_legacy_named("sess_memory").expect("session"));
        assert_eq!(manager.warm().entries().len(), 1);
        assert_eq!(packet.salient_events.len(), 1);
        assert_eq!(packet.memory_records.len(), 1);
        assert_eq!(
            packet.memory_records[0],
            manager.warm().entries()[0].record.memory_id
        );
    }

    #[test]
    fn flat_file_store_writes_immutable_records_and_index() {
        let root = temp_memory_root("sinomo-memory-flat-file");
        let store = FlatFileMemoryStore::new(&root);
        let record = store
            .put_record(
                MemoryScope::Agent {
                    agent_id: "coding-agent".to_string(),
                },
                MemoryTier::Warm,
                "Prefer scoped patches for large files",
                vec![MemorySourceRef::new("session", "sess_memory")],
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("write record");

        let record_path = root
            .join("agents")
            .join("coding-agent")
            .join("records")
            .join(format!("{}.json", record.memory_id));
        assert!(record_path.exists());
        assert!(
            root.join("agents")
                .join("coding-agent")
                .join("index.jsonl")
                .exists()
        );
    }

    #[test]
    fn flat_file_store_retrieves_cluster_and_agent_memory() {
        let root = temp_memory_root("sinomo-memory-retrieve");
        let store = FlatFileMemoryStore::new(&root);
        store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Procedural,
                "Always preserve source evidence for user-visible claims",
                Vec::new(),
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("cluster record");
        store
            .put_record(
                MemoryScope::Agent {
                    agent_id: "coding-agent".to_string(),
                },
                MemoryTier::Warm,
                "Use small direct patches when broad context is unstable",
                vec![MemorySourceRef::new("session", "sess_memory")],
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("agent record");

        let result = store
            .retrieve(&MemoryQuery {
                session_id: Some(SessionId::from_legacy_named("sess_memory").expect("session")),
                agent_id: Some("coding-agent".to_string()),
                max_records: 8,
                max_bytes: 16 * 1024,
                ..MemoryQuery::default()
            })
            .expect("retrieve");

        assert_eq!(result.records.len(), 2);
        assert_eq!(result.report.considered_records, 2);
        assert!(
            result
                .records
                .iter()
                .any(|record| matches!(record.record.scope, MemoryScope::Cluster))
        );
    }
}
