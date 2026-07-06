//! Hot memory, immutable memory records, and memory context retrieval.

use serde::{Deserialize, Serialize};
use sinomo_runtime::core::{
    ContextPacket, EventEnvelope, Id, MemoryId, MemoryRecord, MemoryScope, MemorySourceRef,
    MemoryTier, TrustDomain,
};
use sinomo_runtime::events::{SessionId, StoredEvent};
use sinomo_runtime::object::{LengthMode, serde_ext};
use sinomo_runtime::vfs::{
    VfsAppendOptions, VfsError, VfsHandle, VfsObjectKind, VfsRemoveOptions, VfsWriteOptions,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use thiserror::Error;
use time::OffsetDateTime;

const DEFAULT_RETRIEVAL_RECORD_LIMIT: usize = 8;
const DEFAULT_RETRIEVAL_BYTE_LIMIT: usize = 16 * 1024;
const DEFAULT_MEMORY_RECORDS_PER_SEGMENT: u64 = 1024;
const MEMORY_STORAGE_SCHEMA_VERSION: u32 = 1;
const MEMORY_SOBJ_VERSION: u16 = 1;
const MAX_MEMORY_LOG_RECORD_BYTES: usize = 16 * 1024 * 1024;

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
        let promoted = self.promote_event(&event.event);
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

    pub fn promote_event(&self, event: &EventEnvelope) -> Option<WarmMemoryEntry> {
        let reason = promotion_reason(event)?;
        if event.salience.get() < self.policy.warm_salience_threshold
            && !matches!(
                reason,
                PromotionReason::SafetyIncident
                    | PromotionReason::UserImportant
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
    pub chat_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team_id: Option<String>,
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
            chat_id: None,
            channel_id: None,
            task_id: None,
            workspace_id: None,
            user_id: None,
            team_id: None,
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
    pub included: Vec<MemoryRetrievalTraceEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub omitted: Vec<MemoryRetrievalOmission>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub filtered: Vec<MemoryRetrievalFilter>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub superseded: Vec<MemorySupersededTrace>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryRetrievalOmission {
    pub memory_id: MemoryId,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryRetrievalTraceEntry {
    pub memory_id: MemoryId,
    pub reason: String,
    pub score: i32,
    pub bytes: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryRetrievalFilter {
    pub memory_id: MemoryId,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemorySupersededTrace {
    pub memory_id: MemoryId,
    pub superseded_by: MemoryId,
    pub supersession_id: MemorySupersessionId,
    pub reason: String,
}

pub type MemoryLinkId = Id;
pub type MemorySupersessionId = Id;
pub type MemoryConsolidationId = Id;
pub type MemoryConsolidationJobId = Id;
pub type MemoryConsolidationJobStatusId = Id;
pub type MemoryEmbeddingId = Id;
pub type MemoryAccessId = Id;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum MemoryLinkRelation {
    Supports,
    Contradicts,
    Elaborates,
    Caused,
    Supersedes,
    SameTask,
    SameEntity,
    Related,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryLinkRecord {
    pub link_id: MemoryLinkId,
    pub from_memory_id: MemoryId,
    pub to_memory_id: MemoryId,
    pub relation: MemoryLinkRelation,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_refs: Vec<MemorySourceRef>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

impl MemoryLinkRecord {
    pub fn new(
        from_memory_id: MemoryId,
        to_memory_id: MemoryId,
        relation: MemoryLinkRelation,
        source_refs: Vec<MemorySourceRef>,
        created_at: OffsetDateTime,
    ) -> Self {
        let link_id = derive_memory_link_id(
            &from_memory_id,
            &to_memory_id,
            &relation,
            &source_refs,
            created_at,
        );
        Self {
            link_id,
            from_memory_id,
            to_memory_id,
            relation,
            source_refs,
            created_at,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemorySupersessionRecord {
    pub supersession_id: MemorySupersessionId,
    pub superseded_memory_id: MemoryId,
    pub replacement_memory_id: MemoryId,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_refs: Vec<MemorySourceRef>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

impl MemorySupersessionRecord {
    pub fn new(
        superseded_memory_id: MemoryId,
        replacement_memory_id: MemoryId,
        reason: impl Into<String>,
        source_refs: Vec<MemorySourceRef>,
        created_at: OffsetDateTime,
    ) -> Self {
        let reason = reason.into();
        let supersession_id = derive_memory_supersession_id(
            &superseded_memory_id,
            &replacement_memory_id,
            &reason,
            &source_refs,
            created_at,
        );
        Self {
            supersession_id,
            superseded_memory_id,
            replacement_memory_id,
            reason,
            source_refs,
            created_at,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryConsolidationRecord {
    pub consolidation_id: MemoryConsolidationId,
    pub consolidated_memory_id: MemoryId,
    pub source_memory_ids: Vec<MemoryId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_refs: Vec<MemorySourceRef>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

impl MemoryConsolidationRecord {
    pub fn new(
        consolidated_memory_id: MemoryId,
        source_memory_ids: Vec<MemoryId>,
        source_refs: Vec<MemorySourceRef>,
        created_at: OffsetDateTime,
    ) -> Self {
        let consolidation_id = derive_memory_consolidation_id(
            &consolidated_memory_id,
            &source_memory_ids,
            &source_refs,
            created_at,
        );
        Self {
            consolidation_id,
            consolidated_memory_id,
            source_memory_ids,
            source_refs,
            created_at,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum MemoryConsolidationJobKind {
    SessionBoundaryRecap,
    ChatChannelRecap,
    TaskOutcome,
    PreferenceExtraction,
    IncidentLearning,
    Reflection,
    SkillPromotion,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryConsolidationJobRecord {
    pub job_id: MemoryConsolidationJobId,
    pub kind: MemoryConsolidationJobKind,
    pub target_scope: MemoryScope,
    pub target_tier: MemoryTier,
    pub summary_hint: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_memory_ids: Vec<MemoryId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_refs: Vec<MemorySourceRef>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

impl MemoryConsolidationJobRecord {
    pub fn new(
        kind: MemoryConsolidationJobKind,
        target_scope: MemoryScope,
        target_tier: MemoryTier,
        summary_hint: impl Into<String>,
        source_memory_ids: Vec<MemoryId>,
        source_refs: Vec<MemorySourceRef>,
        created_at: OffsetDateTime,
    ) -> Self {
        let summary_hint = summary_hint.into();
        let job_id = derive_memory_consolidation_job_id(
            &kind,
            &target_scope,
            &target_tier,
            &summary_hint,
            &source_memory_ids,
            &source_refs,
            created_at,
        );
        Self {
            job_id,
            kind,
            target_scope,
            target_tier,
            summary_hint,
            source_memory_ids,
            source_refs,
            created_at,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum MemoryConsolidationJobStatus {
    Scheduled,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryConsolidationJobStatusRecord {
    pub status_id: MemoryConsolidationJobStatusId,
    pub job_id: MemoryConsolidationJobId,
    pub status: MemoryConsolidationJobStatus,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consolidation_id: Option<MemoryConsolidationId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_refs: Vec<MemorySourceRef>,
    #[serde(with = "time::serde::rfc3339")]
    pub recorded_at: OffsetDateTime,
}

impl MemoryConsolidationJobStatusRecord {
    pub fn new(
        job_id: MemoryConsolidationJobId,
        status: MemoryConsolidationJobStatus,
        reason: impl Into<String>,
        consolidation_id: Option<MemoryConsolidationId>,
        source_refs: Vec<MemorySourceRef>,
        recorded_at: OffsetDateTime,
    ) -> Self {
        let reason = reason.into();
        let status_id = derive_memory_consolidation_job_status_id(
            &job_id,
            &status,
            &reason,
            consolidation_id.as_ref(),
            &source_refs,
            recorded_at,
        );
        Self {
            status_id,
            job_id,
            status,
            reason,
            consolidation_id,
            source_refs,
            recorded_at,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum MemoryAccessOperation {
    Write,
    Retrieval,
    Propagation,
    ContextInclusion,
    Supersession,
    Consolidation,
    Compaction,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryAccessRecord {
    pub access_id: MemoryAccessId,
    pub operation: MemoryAccessOperation,
    pub memory_ids: Vec<MemoryId>,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_refs: Vec<MemorySourceRef>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

impl MemoryAccessRecord {
    pub fn new(
        operation: MemoryAccessOperation,
        memory_ids: Vec<MemoryId>,
        reason: impl Into<String>,
        source_refs: Vec<MemorySourceRef>,
        created_at: OffsetDateTime,
    ) -> Self {
        let reason = reason.into();
        let access_id =
            derive_memory_access_id(&operation, &memory_ids, &reason, &source_refs, created_at);
        Self {
            access_id,
            operation,
            memory_ids,
            reason,
            source_refs,
            created_at,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryIndexEntry {
    pub memory_id: MemoryId,
    pub scope: MemoryScope,
    pub tier: MemoryTier,
    #[serde(default)]
    pub sequence: u64,
    pub path: String,
    pub bytes: usize,
    #[serde(with = "time::serde::rfc3339")]
    pub indexed_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryTokenIndexEntry {
    pub token: String,
    pub memory_ids: Vec<MemoryId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryEmbeddingRecord {
    pub embedding_id: MemoryEmbeddingId,
    pub memory_id: MemoryId,
    pub provider: String,
    pub model: String,
    pub dimensions: usize,
    pub vector: Vec<i32>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

impl MemoryEmbeddingRecord {
    pub fn new(
        memory_id: MemoryId,
        provider: impl Into<String>,
        model: impl Into<String>,
        dimensions: usize,
        vector: Vec<i32>,
        created_at: OffsetDateTime,
    ) -> Self {
        let provider = provider.into();
        let model = model.into();
        let embedding_id = derive_memory_embedding_id(
            &memory_id, &provider, &model, dimensions, &vector, created_at,
        );
        Self {
            embedding_id,
            memory_id,
            provider,
            model,
            dimensions,
            vector,
            created_at,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryEmbeddingIndexSnapshot {
    pub provider: String,
    pub model: String,
    pub dimensions: usize,
    #[serde(with = "time::serde::rfc3339")]
    pub rebuilt_at: OffsetDateTime,
    pub embeddings: Vec<MemoryEmbeddingRecord>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryEmbeddingSearchResult {
    pub memory_id: MemoryId,
    pub score: i64,
}

pub trait MemoryEmbeddingIndexProvider: Send + Sync {
    fn rebuild_embedding_index(
        &self,
        records: &[MemoryRecord],
        rebuilt_at: OffsetDateTime,
    ) -> Result<MemoryEmbeddingIndexSnapshot, MemoryError>;

    fn search_embedding_index(
        &self,
        index: &MemoryEmbeddingIndexSnapshot,
        query: &MemoryQuery,
        limit: usize,
    ) -> Vec<MemoryEmbeddingSearchResult>;
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryGraphNodeIndexEntry {
    pub memory_id: MemoryId,
    pub scope: MemoryScope,
    pub tier: MemoryTier,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryGraphEdgeIndexEntry {
    pub from_memory_id: MemoryId,
    pub to_memory_id: MemoryId,
    pub relation: String,
    pub derived_id: Id,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryGraphIndexSnapshot {
    pub nodes: Vec<MemoryGraphNodeIndexEntry>,
    pub edges: Vec<MemoryGraphEdgeIndexEntry>,
}

pub trait MemoryGraphIndexProvider: Send + Sync {
    fn rebuild_graph_index(
        &self,
        records: &[MemoryRecord],
        links: &[MemoryLinkRecord],
        supersessions: &[MemorySupersessionRecord],
        consolidations: &[MemoryConsolidationRecord],
    ) -> Result<MemoryGraphIndexSnapshot, MemoryError>;

    fn related_memory_ids(
        &self,
        index: &MemoryGraphIndexSnapshot,
        memory_id: &MemoryId,
    ) -> Vec<MemoryId>;
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct TokenHashEmbeddingIndexProvider {
    pub provider: String,
    pub model: String,
    pub dimensions: usize,
}

impl TokenHashEmbeddingIndexProvider {
    pub fn new(provider: impl Into<String>, model: impl Into<String>, dimensions: usize) -> Self {
        Self {
            provider: provider.into(),
            model: model.into(),
            dimensions: dimensions.max(1),
        }
    }
}

impl Default for TokenHashEmbeddingIndexProvider {
    fn default() -> Self {
        Self::new("sinomo-local", "token-hash-v1", 64)
    }
}

impl MemoryEmbeddingIndexProvider for TokenHashEmbeddingIndexProvider {
    fn rebuild_embedding_index(
        &self,
        records: &[MemoryRecord],
        rebuilt_at: OffsetDateTime,
    ) -> Result<MemoryEmbeddingIndexSnapshot, MemoryError> {
        let mut embeddings = records
            .iter()
            .map(|record| {
                MemoryEmbeddingRecord::new(
                    record.memory_id.clone(),
                    self.provider.clone(),
                    self.model.clone(),
                    self.dimensions,
                    token_hash_vector(memory_record_tokens(record), self.dimensions),
                    rebuilt_at,
                )
            })
            .collect::<Vec<_>>();
        embeddings.sort_by(|left, right| left.memory_id.cmp(&right.memory_id));
        Ok(MemoryEmbeddingIndexSnapshot {
            provider: self.provider.clone(),
            model: self.model.clone(),
            dimensions: self.dimensions,
            rebuilt_at,
            embeddings,
        })
    }

    fn search_embedding_index(
        &self,
        index: &MemoryEmbeddingIndexSnapshot,
        query: &MemoryQuery,
        limit: usize,
    ) -> Vec<MemoryEmbeddingSearchResult> {
        if limit == 0 || index.dimensions == 0 {
            return Vec::new();
        }
        let query_vector = token_hash_vector(memory_query_tokens(query), index.dimensions);
        let mut results = index
            .embeddings
            .iter()
            .map(|embedding| MemoryEmbeddingSearchResult {
                memory_id: embedding.memory_id.clone(),
                score: dot_product(&embedding.vector, &query_vector),
            })
            .filter(|result| result.score > 0)
            .collect::<Vec<_>>();
        results.sort_by(|left, right| {
            right
                .score
                .cmp(&left.score)
                .then_with(|| left.memory_id.cmp(&right.memory_id))
        });
        results.truncate(limit);
        results
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct LocalMemoryGraphIndexProvider;

impl MemoryGraphIndexProvider for LocalMemoryGraphIndexProvider {
    fn rebuild_graph_index(
        &self,
        records: &[MemoryRecord],
        links: &[MemoryLinkRecord],
        supersessions: &[MemorySupersessionRecord],
        consolidations: &[MemoryConsolidationRecord],
    ) -> Result<MemoryGraphIndexSnapshot, MemoryError> {
        Ok(build_memory_graph_index(
            records,
            links,
            supersessions,
            consolidations,
        ))
    }

    fn related_memory_ids(
        &self,
        index: &MemoryGraphIndexSnapshot,
        memory_id: &MemoryId,
    ) -> Vec<MemoryId> {
        let mut related = BTreeSet::new();
        for edge in &index.edges {
            if &edge.from_memory_id == memory_id {
                related.insert(edge.to_memory_id.clone());
            }
            if &edge.to_memory_id == memory_id {
                related.insert(edge.from_memory_id.clone());
            }
        }
        related.into_iter().collect()
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum MemoryReplicationMode {
    None,
    Snapshot,
    Records,
    SnapshotAndIndex,
    RecordsAndIndex,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum MemoryReplicationConsistency {
    LocalOnly,
    Eventual,
    Confirmed,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum MemoryReplicationArtifactKind {
    ScopeSnapshot,
    ScopeRecords,
    ScopeIndex,
}

impl MemoryReplicationArtifactKind {
    fn file_name(self) -> &'static str {
        match self {
            Self::ScopeSnapshot => "snapshot.sobj",
            Self::ScopeRecords => "records.sobjlog",
            Self::ScopeIndex => "index.sobj",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryScopeReplicationPolicy {
    pub scope: MemoryScope,
    pub mode: MemoryReplicationMode,
    pub consistency: MemoryReplicationConsistency,
    pub min_replicas: u8,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub target_node_ids: Vec<String>,
    pub sifs_path_prefix: String,
}

impl MemoryScopeReplicationPolicy {
    pub fn new(
        scope: MemoryScope,
        mode: MemoryReplicationMode,
        consistency: MemoryReplicationConsistency,
    ) -> Self {
        let min_replicas = match mode {
            MemoryReplicationMode::None => 0,
            _ => match consistency {
                MemoryReplicationConsistency::LocalOnly => 1,
                MemoryReplicationConsistency::Eventual => 1,
                MemoryReplicationConsistency::Confirmed => 2,
            },
        };
        let sifs_path_prefix = memory_scope_sifs_path(&scope);
        Self {
            scope,
            mode,
            consistency,
            min_replicas,
            target_node_ids: Vec::new(),
            sifs_path_prefix,
        }
    }

    pub fn with_min_replicas(mut self, min_replicas: u8) -> Self {
        self.min_replicas = min_replicas;
        self
    }

    pub fn with_target_node_ids(mut self, target_node_ids: Vec<String>) -> Self {
        self.target_node_ids = target_node_ids;
        self
    }

    pub fn with_sifs_path_prefix(mut self, sifs_path_prefix: impl Into<String>) -> Self {
        self.sifs_path_prefix = normalize_sifs_path_prefix(sifs_path_prefix.into(), &self.scope);
        self
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryReplicationPolicy {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scope_policies: Vec<MemoryScopeReplicationPolicy>,
}

impl MemoryReplicationPolicy {
    pub fn default_for_cluster() -> Self {
        Self {
            scope_policies: vec![MemoryScopeReplicationPolicy::new(
                MemoryScope::Cluster,
                MemoryReplicationMode::SnapshotAndIndex,
                MemoryReplicationConsistency::Confirmed,
            )],
        }
    }

    pub fn from_scope_policies(scope_policies: Vec<MemoryScopeReplicationPolicy>) -> Self {
        Self { scope_policies }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryReplicationPlan {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entries: Vec<MemoryReplicationPlanEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryReplicationPlanEntry {
    pub scope: MemoryScope,
    pub artifact: MemoryReplicationArtifactKind,
    pub source_ref: String,
    pub sifs_path: String,
    pub record_count: usize,
    pub consistency: MemoryReplicationConsistency,
    pub min_replicas: u8,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub target_node_ids: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryDerivedIndexEntry {
    pub derived_id: Id,
    pub kind: String,
    #[serde(default)]
    pub sequence: u64,
    pub path: String,
    pub bytes: usize,
    #[serde(with = "time::serde::rfc3339")]
    pub indexed_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryIndexSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub last_sequence: u64,
    pub entries: Vec<MemoryIndexEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub derived_entries: Vec<MemoryDerivedIndexEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub token_index: Vec<MemoryTokenIndexEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemorySnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub last_sequence: u64,
    pub records: Vec<MemoryRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub links: Vec<MemoryLinkRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub supersessions: Vec<MemorySupersessionRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub consolidations: Vec<MemoryConsolidationRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub consolidation_jobs: Vec<MemoryConsolidationJobRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub consolidation_job_statuses: Vec<MemoryConsolidationJobStatusRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub access_records: Vec<MemoryAccessRecord>,
}

pub trait MemoryContextProvider: Send + Sync {
    fn retrieve(&self, query: &MemoryQuery) -> Result<MemoryRetrievalResult, MemoryError>;
}

pub trait MemoryRecordStore: MemoryContextProvider {
    fn write_record(&self, record: MemoryRecord) -> Result<MemoryRecord, MemoryError>;

    fn promote_event(
        &self,
        event: &EventEnvelope,
        policy: &MemoryPolicy,
    ) -> Result<Option<MemoryRecord>, MemoryError>;
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryRetrievalResult {
    pub records: Vec<RetrievedMemoryRecord>,
    pub report: MemoryRetrievalReport,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryEvaluationSuite {
    pub cases: Vec<MemoryEvaluationCase>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryEvaluationCase {
    pub name: String,
    pub query: MemoryQuery,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub expected_memory_ids: Vec<MemoryId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub forbidden_memory_ids: Vec<MemoryId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub expected_superseded_memory_ids: Vec<MemoryId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_source_refs: Vec<MemorySourceRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub previous_included_memory_ids: Vec<MemoryId>,
}

impl MemoryEvaluationCase {
    pub fn new(name: impl Into<String>, query: MemoryQuery) -> Self {
        Self {
            name: name.into(),
            query,
            expected_memory_ids: Vec::new(),
            forbidden_memory_ids: Vec::new(),
            expected_superseded_memory_ids: Vec::new(),
            required_source_refs: Vec::new(),
            previous_included_memory_ids: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryEvaluationReport {
    pub summary: MemoryEvaluationSummary,
    pub cases: Vec<MemoryEvaluationCaseReport>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryEvaluationSummary {
    pub total_cases: usize,
    pub passed_cases: usize,
    pub expected_records: usize,
    pub recalled_records: usize,
    pub selected_records: usize,
    pub relevant_selected_records: usize,
    pub forbidden_selected_records: usize,
    pub missing_superseded_records: usize,
    pub missing_source_refs: usize,
    pub cache_reuse_prefix_records: usize,
    pub cache_churn_records: usize,
    pub max_latency_micros: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryEvaluationCaseReport {
    pub name: String,
    pub selected_memory_ids: Vec<MemoryId>,
    pub missing_expected_memory_ids: Vec<MemoryId>,
    pub unexpected_forbidden_memory_ids: Vec<MemoryId>,
    pub missing_superseded_memory_ids: Vec<MemoryId>,
    pub missing_source_refs: Vec<MemorySourceRef>,
    pub selected_superseded_memory_ids: Vec<MemoryId>,
    pub expected_records: usize,
    pub recalled_records: usize,
    pub selected_records: usize,
    pub relevant_selected_records: usize,
    pub cache_reuse_prefix_records: usize,
    pub cache_churn_records: usize,
    pub latency_micros: u64,
}

impl MemoryEvaluationCaseReport {
    pub fn passed(&self) -> bool {
        self.missing_expected_memory_ids.is_empty()
            && self.unexpected_forbidden_memory_ids.is_empty()
            && self.missing_superseded_memory_ids.is_empty()
            && self.missing_source_refs.is_empty()
            && self.selected_superseded_memory_ids.is_empty()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemoryConsolidationJobRun {
    pub job: MemoryConsolidationJobRecord,
    pub running_status: MemoryConsolidationJobStatusRecord,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consolidation: Option<MemoryConsolidationRecord>,
    pub final_status: MemoryConsolidationJobStatusRecord,
}

pub fn evaluate_memory_retrieval(
    provider: &dyn MemoryContextProvider,
    suite: &MemoryEvaluationSuite,
) -> Result<MemoryEvaluationReport, MemoryError> {
    let mut report = MemoryEvaluationReport::default();
    report.summary.total_cases = suite.cases.len();

    for case in &suite.cases {
        let started = Instant::now();
        let retrieval = provider.retrieve(&case.query)?;
        let latency_micros = duration_micros(started.elapsed());
        let case_report = evaluate_memory_retrieval_case(case, &retrieval, latency_micros);
        accumulate_memory_evaluation_summary(&mut report.summary, &case_report);
        report.cases.push(case_report);
    }

    Ok(report)
}

#[derive(Clone, Debug)]
pub struct DurableMemoryStore {
    root: PathBuf,
    vfs: VfsHandle,
    records_per_segment: u64,
    write_lock: Arc<Mutex<()>>,
}

impl DurableMemoryStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        let vfs = VfsHandle::local(root.clone()).expect("durable memory VFS root");
        Self {
            root,
            vfs,
            records_per_segment: DEFAULT_MEMORY_RECORDS_PER_SEGMENT,
            write_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn with_records_per_segment(mut self, records_per_segment: u64) -> Self {
        self.records_per_segment = records_per_segment.max(1);
        self
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn from_data_dir(data_dir: &sinomo_runtime::local::SinomoDataDir) -> Self {
        Self::new(data_dir.root().join("memory"))
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

        let _guard = self.write_guard()?;
        let mut state = self.replay_state()?;
        if state.records.contains_key(&record.memory_id) {
            return Ok(record);
        }
        let sequence = next_sequence(state.last_sequence)?;
        self.append_log_entry(&MemoryLogEntry::put(sequence, record.clone()))?;
        state.last_sequence = sequence;
        state
            .record_sequences
            .insert(record.memory_id.clone(), sequence);
        state
            .records
            .insert(record.memory_id.clone(), record.clone());
        let access = MemoryAccessRecord::new(
            MemoryAccessOperation::Write,
            vec![record.memory_id.clone()],
            "memory record write",
            record.source_refs.clone(),
            OffsetDateTime::now_utc(),
        );
        let access_sequence = next_sequence(state.last_sequence)?;
        self.append_log_entry(&MemoryLogEntry::put_access(access_sequence, access.clone()))?;
        state.last_sequence = access_sequence;
        state
            .access_sequences
            .insert(access.access_id.clone(), access_sequence);
        state
            .access_records
            .insert(access.access_id.clone(), access);
        self.write_index_snapshot(&state)?;

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
        let mut records = self
            .replay_state()?
            .records
            .into_values()
            .filter(|record| &record.scope == scope)
            .collect::<Vec<_>>();
        records.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.memory_id.cmp(&right.memory_id))
        });
        Ok(records)
    }

    pub fn write_link(&self, link: MemoryLinkRecord) -> Result<MemoryLinkRecord, MemoryError> {
        let expected = MemoryLinkRecord::new(
            link.from_memory_id.clone(),
            link.to_memory_id.clone(),
            link.relation.clone(),
            link.source_refs.clone(),
            link.created_at,
        );
        if expected.link_id != link.link_id {
            return Err(MemoryError::NonCanonicalDerivedMemoryId {
                kind: "memory-link",
                expected: expected.link_id,
                actual: link.link_id,
            });
        }

        let _guard = self.write_guard()?;
        let mut state = self.replay_state()?;
        if state.links.contains_key(&link.link_id) {
            return Ok(link);
        }
        ensure_memory_record_exists(&state, &link.from_memory_id, "memory-link")?;
        ensure_memory_record_exists(&state, &link.to_memory_id, "memory-link")?;
        let sequence = next_sequence(state.last_sequence)?;
        self.append_log_entry(&MemoryLogEntry::put_link(sequence, link.clone()))?;
        state.last_sequence = sequence;
        state.link_sequences.insert(link.link_id.clone(), sequence);
        state.links.insert(link.link_id.clone(), link.clone());
        self.write_index_snapshot(&state)?;
        Ok(link)
    }

    pub fn link_records(&self) -> Result<Vec<MemoryLinkRecord>, MemoryError> {
        let mut links = self.replay_state()?.links.into_values().collect::<Vec<_>>();
        links.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.link_id.cmp(&right.link_id))
        });
        Ok(links)
    }

    pub fn write_supersession(
        &self,
        supersession: MemorySupersessionRecord,
    ) -> Result<MemorySupersessionRecord, MemoryError> {
        let expected = MemorySupersessionRecord::new(
            supersession.superseded_memory_id.clone(),
            supersession.replacement_memory_id.clone(),
            supersession.reason.clone(),
            supersession.source_refs.clone(),
            supersession.created_at,
        );
        if expected.supersession_id != supersession.supersession_id {
            return Err(MemoryError::NonCanonicalDerivedMemoryId {
                kind: "memory-supersession",
                expected: expected.supersession_id,
                actual: supersession.supersession_id,
            });
        }

        let _guard = self.write_guard()?;
        let mut state = self.replay_state()?;
        if state
            .supersessions
            .contains_key(&supersession.supersession_id)
        {
            return Ok(supersession);
        }
        ensure_memory_record_exists(
            &state,
            &supersession.superseded_memory_id,
            "memory-supersession",
        )?;
        ensure_memory_record_exists(
            &state,
            &supersession.replacement_memory_id,
            "memory-supersession",
        )?;
        let sequence = next_sequence(state.last_sequence)?;
        self.append_log_entry(&MemoryLogEntry::put_supersession(
            sequence,
            supersession.clone(),
        ))?;
        state.last_sequence = sequence;
        state
            .supersession_sequences
            .insert(supersession.supersession_id.clone(), sequence);
        state
            .supersessions
            .insert(supersession.supersession_id.clone(), supersession.clone());
        self.write_index_snapshot(&state)?;
        Ok(supersession)
    }

    pub fn supersede_record(
        &self,
        superseded_memory_id: MemoryId,
        replacement_memory_id: MemoryId,
        reason: impl Into<String>,
        source_refs: Vec<MemorySourceRef>,
        created_at: OffsetDateTime,
    ) -> Result<MemorySupersessionRecord, MemoryError> {
        self.write_supersession(MemorySupersessionRecord::new(
            superseded_memory_id,
            replacement_memory_id,
            reason,
            source_refs,
            created_at,
        ))
    }

    pub fn supersession_records(&self) -> Result<Vec<MemorySupersessionRecord>, MemoryError> {
        let mut supersessions = self
            .replay_state()?
            .supersessions
            .into_values()
            .collect::<Vec<_>>();
        supersessions.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.supersession_id.cmp(&right.supersession_id))
        });
        Ok(supersessions)
    }

    pub fn write_consolidation(
        &self,
        consolidation: MemoryConsolidationRecord,
    ) -> Result<MemoryConsolidationRecord, MemoryError> {
        let expected = MemoryConsolidationRecord::new(
            consolidation.consolidated_memory_id.clone(),
            consolidation.source_memory_ids.clone(),
            consolidation.source_refs.clone(),
            consolidation.created_at,
        );
        if expected.consolidation_id != consolidation.consolidation_id {
            return Err(MemoryError::NonCanonicalDerivedMemoryId {
                kind: "memory-consolidation",
                expected: expected.consolidation_id,
                actual: consolidation.consolidation_id,
            });
        }

        let _guard = self.write_guard()?;
        let mut state = self.replay_state()?;
        if state
            .consolidations
            .contains_key(&consolidation.consolidation_id)
        {
            return Ok(consolidation);
        }
        ensure_memory_record_exists(
            &state,
            &consolidation.consolidated_memory_id,
            "memory-consolidation",
        )?;
        for memory_id in &consolidation.source_memory_ids {
            ensure_memory_record_exists(&state, memory_id, "memory-consolidation")?;
        }
        let sequence = next_sequence(state.last_sequence)?;
        self.append_log_entry(&MemoryLogEntry::put_consolidation(
            sequence,
            consolidation.clone(),
        ))?;
        state.last_sequence = sequence;
        state
            .consolidation_sequences
            .insert(consolidation.consolidation_id.clone(), sequence);
        state.consolidations.insert(
            consolidation.consolidation_id.clone(),
            consolidation.clone(),
        );
        self.write_index_snapshot(&state)?;
        Ok(consolidation)
    }

    pub fn consolidate_records(
        &self,
        scope: MemoryScope,
        tier: MemoryTier,
        summary: impl Into<String>,
        source_memory_ids: Vec<MemoryId>,
        source_refs: Vec<MemorySourceRef>,
        visibility: TrustDomain,
        created_at: OffsetDateTime,
    ) -> Result<(MemoryRecord, MemoryConsolidationRecord), MemoryError> {
        if source_memory_ids.is_empty() {
            return Err(MemoryError::EmptyConsolidationSources);
        }

        let mut record_source_refs =
            consolidated_memory_source_refs(source_refs.clone(), &source_memory_ids);
        let record = MemoryRecord::new(
            scope,
            tier,
            summary,
            std::mem::take(&mut record_source_refs),
            visibility,
            created_at,
        );
        let consolidation = MemoryConsolidationRecord::new(
            record.memory_id.clone(),
            source_memory_ids,
            source_refs,
            created_at,
        );

        let _guard = self.write_guard()?;
        let mut state = self.replay_state()?;
        for memory_id in &consolidation.source_memory_ids {
            ensure_memory_record_exists(&state, memory_id, "memory-consolidation")?;
        }

        if !state.records.contains_key(&record.memory_id) {
            let sequence = next_sequence(state.last_sequence)?;
            self.append_log_entry(&MemoryLogEntry::put(sequence, record.clone()))?;
            state.last_sequence = sequence;
            state
                .record_sequences
                .insert(record.memory_id.clone(), sequence);
            state
                .records
                .insert(record.memory_id.clone(), record.clone());
            let access = MemoryAccessRecord::new(
                MemoryAccessOperation::Consolidation,
                vec![record.memory_id.clone()],
                "memory consolidation write",
                record.source_refs.clone(),
                OffsetDateTime::now_utc(),
            );
            self.append_access_record_to_state(&mut state, access)?;
        }

        if !state
            .consolidations
            .contains_key(&consolidation.consolidation_id)
        {
            ensure_memory_record_exists(
                &state,
                &consolidation.consolidated_memory_id,
                "memory-consolidation",
            )?;
            let sequence = next_sequence(state.last_sequence)?;
            self.append_log_entry(&MemoryLogEntry::put_consolidation(
                sequence,
                consolidation.clone(),
            ))?;
            state.last_sequence = sequence;
            state
                .consolidation_sequences
                .insert(consolidation.consolidation_id.clone(), sequence);
            state.consolidations.insert(
                consolidation.consolidation_id.clone(),
                consolidation.clone(),
            );
        }
        self.write_index_snapshot(&state)?;

        Ok((record, consolidation))
    }

    pub fn consolidation_records(&self) -> Result<Vec<MemoryConsolidationRecord>, MemoryError> {
        let mut consolidations = self
            .replay_state()?
            .consolidations
            .into_values()
            .collect::<Vec<_>>();
        consolidations.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.consolidation_id.cmp(&right.consolidation_id))
        });
        Ok(consolidations)
    }

    pub fn schedule_consolidation_job(
        &self,
        kind: MemoryConsolidationJobKind,
        target_scope: MemoryScope,
        target_tier: MemoryTier,
        summary_hint: impl Into<String>,
        source_memory_ids: Vec<MemoryId>,
        source_refs: Vec<MemorySourceRef>,
        created_at: OffsetDateTime,
    ) -> Result<MemoryConsolidationJobRecord, MemoryError> {
        self.write_consolidation_job(MemoryConsolidationJobRecord::new(
            kind,
            target_scope,
            target_tier,
            summary_hint,
            source_memory_ids,
            source_refs,
            created_at,
        ))
    }

    pub fn write_consolidation_job(
        &self,
        job: MemoryConsolidationJobRecord,
    ) -> Result<MemoryConsolidationJobRecord, MemoryError> {
        let expected = MemoryConsolidationJobRecord::new(
            job.kind.clone(),
            job.target_scope.clone(),
            job.target_tier.clone(),
            job.summary_hint.clone(),
            job.source_memory_ids.clone(),
            job.source_refs.clone(),
            job.created_at,
        );
        if expected.job_id != job.job_id {
            return Err(MemoryError::NonCanonicalDerivedMemoryId {
                kind: "memory-consolidation-job",
                expected: expected.job_id,
                actual: job.job_id,
            });
        }
        if job.source_memory_ids.is_empty() && job.source_refs.is_empty() {
            return Err(MemoryError::EmptyConsolidationJobSources);
        }

        let _guard = self.write_guard()?;
        let mut state = self.replay_state()?;
        if state.consolidation_jobs.contains_key(&job.job_id) {
            return Ok(job);
        }
        for memory_id in &job.source_memory_ids {
            ensure_memory_record_exists(&state, memory_id, "memory-consolidation-job")?;
        }
        let sequence = next_sequence(state.last_sequence)?;
        self.append_log_entry(&MemoryLogEntry::put_consolidation_job(
            sequence,
            job.clone(),
        ))?;
        state.last_sequence = sequence;
        state
            .consolidation_job_sequences
            .insert(job.job_id.clone(), sequence);
        state
            .consolidation_jobs
            .insert(job.job_id.clone(), job.clone());
        let status = MemoryConsolidationJobStatusRecord::new(
            job.job_id.clone(),
            MemoryConsolidationJobStatus::Scheduled,
            "memory consolidation job scheduled",
            None,
            job.source_refs.clone(),
            OffsetDateTime::now_utc(),
        );
        self.append_consolidation_job_status_to_state(&mut state, status)?;
        self.write_index_snapshot(&state)?;
        Ok(job)
    }

    pub fn record_consolidation_job_status(
        &self,
        status: MemoryConsolidationJobStatusRecord,
    ) -> Result<MemoryConsolidationJobStatusRecord, MemoryError> {
        let expected = MemoryConsolidationJobStatusRecord::new(
            status.job_id.clone(),
            status.status.clone(),
            status.reason.clone(),
            status.consolidation_id.clone(),
            status.source_refs.clone(),
            status.recorded_at,
        );
        if expected.status_id != status.status_id {
            return Err(MemoryError::NonCanonicalDerivedMemoryId {
                kind: "memory-consolidation-job-status",
                expected: expected.status_id,
                actual: status.status_id,
            });
        }

        let _guard = self.write_guard()?;
        let mut state = self.replay_state()?;
        self.append_consolidation_job_status_to_state(&mut state, status.clone())?;
        self.write_index_snapshot(&state)?;
        Ok(status)
    }

    pub fn complete_consolidation_job(
        &self,
        job_id: MemoryConsolidationJobId,
        consolidation_id: MemoryConsolidationId,
        reason: impl Into<String>,
        source_refs: Vec<MemorySourceRef>,
        recorded_at: OffsetDateTime,
    ) -> Result<MemoryConsolidationJobStatusRecord, MemoryError> {
        self.record_consolidation_job_status(MemoryConsolidationJobStatusRecord::new(
            job_id,
            MemoryConsolidationJobStatus::Completed,
            reason,
            Some(consolidation_id),
            source_refs,
            recorded_at,
        ))
    }

    pub fn consolidation_job_records(
        &self,
    ) -> Result<Vec<MemoryConsolidationJobRecord>, MemoryError> {
        let mut jobs = self
            .replay_state()?
            .consolidation_jobs
            .into_values()
            .collect::<Vec<_>>();
        jobs.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.job_id.cmp(&right.job_id))
        });
        Ok(jobs)
    }

    pub fn consolidation_job_status_records(
        &self,
    ) -> Result<Vec<MemoryConsolidationJobStatusRecord>, MemoryError> {
        let mut statuses = self
            .replay_state()?
            .consolidation_job_statuses
            .into_values()
            .collect::<Vec<_>>();
        statuses.sort_by(|left, right| {
            left.recorded_at
                .cmp(&right.recorded_at)
                .then_with(|| left.status_id.cmp(&right.status_id))
        });
        Ok(statuses)
    }

    pub fn pending_consolidation_jobs(
        &self,
    ) -> Result<Vec<MemoryConsolidationJobRecord>, MemoryError> {
        let state = self.replay_state()?;
        let latest_statuses = latest_consolidation_job_statuses_by_job(&state);
        let mut jobs = state
            .consolidation_jobs
            .into_values()
            .filter(|job| {
                matches!(
                    latest_statuses
                        .get(&job.job_id)
                        .map(|status| &status.status),
                    None | Some(MemoryConsolidationJobStatus::Scheduled)
                        | Some(MemoryConsolidationJobStatus::Running)
                )
            })
            .collect::<Vec<_>>();
        jobs.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.job_id.cmp(&right.job_id))
        });
        Ok(jobs)
    }

    pub fn run_next_consolidation_job(
        &self,
        visibility: TrustDomain,
        now: OffsetDateTime,
    ) -> Result<Option<MemoryConsolidationJobRun>, MemoryError> {
        let Some(job) = self.pending_consolidation_jobs()?.into_iter().next() else {
            return Ok(None);
        };
        let running_status =
            self.record_consolidation_job_status(MemoryConsolidationJobStatusRecord::new(
                job.job_id.clone(),
                MemoryConsolidationJobStatus::Running,
                "memory consolidation job running",
                None,
                job.source_refs.clone(),
                now,
            ))?;

        if job.source_memory_ids.is_empty() {
            let final_status =
                self.record_consolidation_job_status(MemoryConsolidationJobStatusRecord::new(
                    job.job_id.clone(),
                    MemoryConsolidationJobStatus::Failed,
                    "deterministic worker requires source memory records",
                    None,
                    job.source_refs.clone(),
                    now + time::Duration::nanoseconds(1),
                ))?;
            return Ok(Some(MemoryConsolidationJobRun {
                job,
                running_status,
                consolidation: None,
                final_status,
            }));
        }

        let source_records = match self.records_by_id(&job.source_memory_ids) {
            Ok(records) => records,
            Err(source) => {
                let final_status =
                    self.record_consolidation_job_status(MemoryConsolidationJobStatusRecord::new(
                        job.job_id.clone(),
                        MemoryConsolidationJobStatus::Failed,
                        format!("failed to load source memories: {source}"),
                        None,
                        job.source_refs.clone(),
                        now + time::Duration::nanoseconds(1),
                    ))?;
                return Ok(Some(MemoryConsolidationJobRun {
                    job,
                    running_status,
                    consolidation: None,
                    final_status,
                }));
            }
        };
        let summary = deterministic_consolidation_summary(&job, &source_records);
        let (_record, consolidation) = self.consolidate_records(
            job.target_scope.clone(),
            job.target_tier.clone(),
            summary,
            job.source_memory_ids.clone(),
            job.source_refs.clone(),
            visibility,
            now + time::Duration::nanoseconds(1),
        )?;
        let final_status = self.complete_consolidation_job(
            job.job_id.clone(),
            consolidation.consolidation_id.clone(),
            "memory consolidation job completed",
            job.source_refs.clone(),
            now + time::Duration::nanoseconds(2),
        )?;

        Ok(Some(MemoryConsolidationJobRun {
            job,
            running_status,
            consolidation: Some(consolidation),
            final_status,
        }))
    }

    fn append_consolidation_job_status_to_state(
        &self,
        state: &mut MemoryReplayState,
        status: MemoryConsolidationJobStatusRecord,
    ) -> Result<(), MemoryError> {
        if state
            .consolidation_job_statuses
            .contains_key(&status.status_id)
        {
            return Ok(());
        }
        if !state.consolidation_jobs.contains_key(&status.job_id) {
            return Err(MemoryError::MissingConsolidationJob {
                job_id: status.job_id,
            });
        }
        if let Some(consolidation_id) = &status.consolidation_id
            && !state.consolidations.contains_key(consolidation_id)
        {
            return Err(MemoryError::MissingConsolidationRecord {
                consolidation_id: consolidation_id.clone(),
            });
        }
        let sequence = next_sequence(state.last_sequence)?;
        self.append_log_entry(&MemoryLogEntry::put_consolidation_job_status(
            sequence,
            status.clone(),
        ))?;
        state.last_sequence = sequence;
        state
            .consolidation_job_status_sequences
            .insert(status.status_id.clone(), sequence);
        state
            .consolidation_job_statuses
            .insert(status.status_id.clone(), status);
        Ok(())
    }

    fn records_by_id(&self, memory_ids: &[MemoryId]) -> Result<Vec<MemoryRecord>, MemoryError> {
        let state = self.replay_state()?;
        memory_ids
            .iter()
            .map(|memory_id| {
                state.records.get(memory_id).cloned().ok_or_else(|| {
                    MemoryError::MissingMemoryRecord {
                        kind: "memory-consolidation-job",
                        memory_id: memory_id.clone(),
                    }
                })
            })
            .collect()
    }

    pub fn write_access_record(
        &self,
        access: MemoryAccessRecord,
    ) -> Result<MemoryAccessRecord, MemoryError> {
        let expected = MemoryAccessRecord::new(
            access.operation.clone(),
            access.memory_ids.clone(),
            access.reason.clone(),
            access.source_refs.clone(),
            access.created_at,
        );
        if expected.access_id != access.access_id {
            return Err(MemoryError::NonCanonicalDerivedMemoryId {
                kind: "memory-access",
                expected: expected.access_id,
                actual: access.access_id,
            });
        }

        let _guard = self.write_guard()?;
        let mut state = self.replay_state()?;
        if state.access_records.contains_key(&access.access_id) {
            return Ok(access);
        }
        for memory_id in &access.memory_ids {
            ensure_memory_record_exists(&state, memory_id, "memory-access")?;
        }
        let sequence = next_sequence(state.last_sequence)?;
        self.append_log_entry(&MemoryLogEntry::put_access(sequence, access.clone()))?;
        state.last_sequence = sequence;
        state
            .access_sequences
            .insert(access.access_id.clone(), sequence);
        state
            .access_records
            .insert(access.access_id.clone(), access.clone());
        self.write_index_snapshot(&state)?;
        Ok(access)
    }

    fn append_access_record_to_state(
        &self,
        state: &mut MemoryReplayState,
        access: MemoryAccessRecord,
    ) -> Result<(), MemoryError> {
        if state.access_records.contains_key(&access.access_id) {
            return Ok(());
        }
        for memory_id in &access.memory_ids {
            ensure_memory_record_exists(state, memory_id, "memory-access")?;
        }
        let sequence = next_sequence(state.last_sequence)?;
        self.append_log_entry(&MemoryLogEntry::put_access(sequence, access.clone()))?;
        state.last_sequence = sequence;
        state
            .access_sequences
            .insert(access.access_id.clone(), sequence);
        state
            .access_records
            .insert(access.access_id.clone(), access);
        Ok(())
    }

    pub fn record_access(
        &self,
        operation: MemoryAccessOperation,
        memory_ids: Vec<MemoryId>,
        reason: impl Into<String>,
        source_refs: Vec<MemorySourceRef>,
        created_at: OffsetDateTime,
    ) -> Result<MemoryAccessRecord, MemoryError> {
        self.write_access_record(MemoryAccessRecord::new(
            operation,
            memory_ids,
            reason,
            source_refs,
            created_at,
        ))
    }

    pub fn access_records(&self) -> Result<Vec<MemoryAccessRecord>, MemoryError> {
        let mut access_records = self
            .replay_state()?
            .access_records
            .into_values()
            .collect::<Vec<_>>();
        access_records.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.access_id.cmp(&right.access_id))
        });
        Ok(access_records)
    }

    pub fn promote_event(
        &self,
        event: &EventEnvelope,
        policy: &MemoryPolicy,
    ) -> Result<Option<MemoryRecord>, MemoryError> {
        let manager = MemoryManager::new(policy.clone());
        let Some(entry) = manager.promote_event(event) else {
            return Ok(None);
        };
        self.write_record(entry.record).map(Some)
    }

    pub fn compact(&self) -> Result<MemorySnapshot, MemoryError> {
        let _guard = self.write_guard()?;
        let mut state = self.replay_state()?;
        let snapshot = MemorySnapshot {
            schema_version: MEMORY_STORAGE_SCHEMA_VERSION,
            last_sequence: state.last_sequence,
            records: state.records.values().cloned().collect(),
            links: state.links.values().cloned().collect(),
            supersessions: state.supersessions.values().cloned().collect(),
            consolidations: state.consolidations.values().cloned().collect(),
            consolidation_jobs: state.consolidation_jobs.values().cloned().collect(),
            consolidation_job_statuses: state
                .consolidation_job_statuses
                .values()
                .cloned()
                .collect(),
            access_records: state.access_records.values().cloned().collect(),
        };
        self.write_sobj_file(&self.snapshot_path(), &snapshot)?;
        state.snapshot_sequence = snapshot.last_sequence;
        self.write_index_snapshot(&state)?;
        self.clear_log_segments()?;
        Ok(snapshot)
    }

    pub fn load_snapshot(&self) -> Result<Option<MemorySnapshot>, MemoryError> {
        let path = self.snapshot_path();
        let Some(bytes) = self.read_optional_bytes(&path)? else {
            return Ok(None);
        };
        let snapshot = decode_sobj::<MemorySnapshot>(&path, &bytes)?;
        if snapshot.schema_version != MEMORY_STORAGE_SCHEMA_VERSION {
            return Err(MemoryError::UnsupportedSchema {
                path: path.display().to_string(),
                schema_version: snapshot.schema_version,
            });
        }
        Ok(Some(snapshot))
    }

    pub fn load_index(&self) -> Result<Option<MemoryIndexSnapshot>, MemoryError> {
        let path = self.index_path();
        let Some(bytes) = self.read_optional_bytes(&path)? else {
            return Ok(None);
        };
        let index = decode_sobj::<MemoryIndexSnapshot>(&path, &bytes)?;
        if index.schema_version != MEMORY_STORAGE_SCHEMA_VERSION {
            return Err(MemoryError::UnsupportedSchema {
                path: path.display().to_string(),
                schema_version: index.schema_version,
            });
        }
        Ok(Some(index))
    }

    fn read_optional_bytes(&self, path: &Path) -> Result<Option<Vec<u8>>, MemoryError> {
        let vfs_path = self.vfs_path_for(path)?;
        self.vfs
            .read_optional(&vfs_path)
            .map_err(|source| memory_vfs_error(path, source))
    }

    fn append_bytes(&self, path: &Path, bytes: &[u8]) -> Result<(), MemoryError> {
        let vfs_path = self.vfs_path_for(path)?;
        let parent = vfs_parent_path(&vfs_path).unwrap_or("");
        self.vfs
            .create_dir_all(parent)
            .map_err(|source| memory_vfs_error(path, source))?;
        self.vfs
            .append_with_options(&vfs_path, bytes, VfsAppendOptions::local())
            .map_err(|source| memory_vfs_error(path, source))
    }

    fn write_bytes(&self, path: &Path, bytes: &[u8]) -> Result<(), MemoryError> {
        let vfs_path = self.vfs_path_for(path)?;
        let parent = vfs_parent_path(&vfs_path).unwrap_or("");
        self.vfs
            .create_dir_all(parent)
            .map_err(|source| memory_vfs_error(path, source))?;
        self.vfs
            .write_with_options(&vfs_path, bytes, VfsWriteOptions::local_replace())
            .map_err(|source| memory_vfs_error(path, source))
    }

    fn remove_file_if_exists(&self, path: &Path) -> Result<(), MemoryError> {
        let vfs_path = self.vfs_path_for(path)?;
        self.vfs
            .remove_with_options(&vfs_path, VfsRemoveOptions::file().missing_ok())
            .map_err(|source| memory_vfs_error(path, source))
    }

    fn remove_empty_dir_if_exists(&self, path: &Path) -> Result<(), MemoryError> {
        let vfs_path = self.vfs_path_for(path)?;
        self.vfs
            .remove_with_options(&vfs_path, VfsRemoveOptions::directory().missing_ok())
            .map_err(|source| memory_vfs_error(path, source))
    }

    fn vfs_path_for(&self, path: &Path) -> Result<String, MemoryError> {
        let relative = path
            .strip_prefix(&self.root)
            .map_err(|source| MemoryError::Vfs {
                path: path.display().to_string(),
                detail: format!("path is outside memory store root: {source}"),
            })?;
        let mut segments = Vec::new();
        for component in relative.components() {
            match component {
                Component::Normal(segment) => {
                    let Some(segment) = segment.to_str() else {
                        return Err(MemoryError::Vfs {
                            path: path.display().to_string(),
                            detail: "path segment is not valid UTF-8".to_string(),
                        });
                    };
                    segments.push(segment.to_string());
                }
                Component::CurDir => {}
                _ => {
                    return Err(MemoryError::Vfs {
                        path: path.display().to_string(),
                        detail: "path contains unsupported component".to_string(),
                    });
                }
            }
        }
        Ok(segments.join("/"))
    }

    pub fn rebuild_embedding_index(
        &self,
        provider: &dyn MemoryEmbeddingIndexProvider,
        rebuilt_at: OffsetDateTime,
    ) -> Result<MemoryEmbeddingIndexSnapshot, MemoryError> {
        let state = self.replay_state()?;
        let records = state.records.values().cloned().collect::<Vec<_>>();
        provider.rebuild_embedding_index(&records, rebuilt_at)
    }

    pub fn rebuild_graph_index(
        &self,
        provider: &dyn MemoryGraphIndexProvider,
    ) -> Result<MemoryGraphIndexSnapshot, MemoryError> {
        let state = self.replay_state()?;
        let records = state.records.values().cloned().collect::<Vec<_>>();
        let links = state.links.values().cloned().collect::<Vec<_>>();
        let supersessions = state.supersessions.values().cloned().collect::<Vec<_>>();
        let consolidations = state.consolidations.values().cloned().collect::<Vec<_>>();
        provider.rebuild_graph_index(&records, &links, &supersessions, &consolidations)
    }

    pub fn plan_sifs_replication(
        &self,
        policy: &MemoryReplicationPolicy,
    ) -> Result<MemoryReplicationPlan, MemoryError> {
        let state = self.replay_state()?;
        let mut entries = Vec::new();
        for scope_policy in &policy.scope_policies {
            entries.extend(memory_replication_plan_entries(&state, scope_policy));
        }
        Ok(MemoryReplicationPlan { entries })
    }

    fn retrieve_records(&self, query: &MemoryQuery) -> Result<MemoryRetrievalResult, MemoryError> {
        let state = self.replay_state()?;
        let superseded_by = latest_supersessions_by_memory(&state);
        let mut candidates = Vec::new();
        let mut filtered = Vec::new();
        let mut superseded = Vec::new();
        for record in state.records.values() {
            if !memory_record_visible(record, query) {
                filtered.push(MemoryRetrievalFilter {
                    memory_id: record.memory_id.clone(),
                    reason: "scope not visible for query".to_string(),
                });
                continue;
            }
            if let Some(supersession) = superseded_by.get(&record.memory_id) {
                superseded.push(MemorySupersededTrace {
                    memory_id: record.memory_id.clone(),
                    superseded_by: supersession.replacement_memory_id.clone(),
                    supersession_id: supersession.supersession_id.clone(),
                    reason: supersession.reason.clone(),
                });
                continue;
            }
            candidates.push(record.clone());
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
        let mut included = Vec::new();
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
            included.push(MemoryRetrievalTraceEntry {
                memory_id: candidate.record.memory_id.clone(),
                reason: candidate.reason.clone(),
                score: candidate.score,
                bytes: candidate.bytes,
            });
            selected.push(candidate);
        }

        let selected_memory_ids = selected
            .iter()
            .map(|record| record.record.memory_id.clone())
            .collect::<Vec<_>>();
        let result = MemoryRetrievalResult {
            report: MemoryRetrievalReport {
                considered_records: selected.len() + omitted.len() + superseded.len(),
                selected_records: selected.len(),
                selected_bytes,
                included,
                omitted,
                filtered,
                superseded,
            },
            records: selected,
        };
        if !selected_memory_ids.is_empty() {
            self.record_access(
                MemoryAccessOperation::Retrieval,
                selected_memory_ids,
                "memory retrieval",
                memory_query_source_refs(query),
                OffsetDateTime::now_utc(),
            )?;
        }
        Ok(result)
    }

    fn replay_state(&self) -> Result<MemoryReplayState, MemoryError> {
        let mut state = MemoryReplayState::default();
        if let Some(snapshot) = self.load_snapshot()? {
            state.snapshot_sequence = snapshot.last_sequence;
            state.last_sequence = snapshot.last_sequence;
            for record in snapshot.records {
                state
                    .record_sequences
                    .insert(record.memory_id.clone(), snapshot.last_sequence);
                state.records.insert(record.memory_id.clone(), record);
            }
            for link in snapshot.links {
                state
                    .link_sequences
                    .insert(link.link_id.clone(), snapshot.last_sequence);
                state.links.insert(link.link_id.clone(), link);
            }
            for supersession in snapshot.supersessions {
                state
                    .supersession_sequences
                    .insert(supersession.supersession_id.clone(), snapshot.last_sequence);
                state
                    .supersessions
                    .insert(supersession.supersession_id.clone(), supersession);
            }
            for consolidation in snapshot.consolidations {
                state.consolidation_sequences.insert(
                    consolidation.consolidation_id.clone(),
                    snapshot.last_sequence,
                );
                state
                    .consolidations
                    .insert(consolidation.consolidation_id.clone(), consolidation);
            }
            for job in snapshot.consolidation_jobs {
                state
                    .consolidation_job_sequences
                    .insert(job.job_id.clone(), snapshot.last_sequence);
                state.consolidation_jobs.insert(job.job_id.clone(), job);
            }
            for status in snapshot.consolidation_job_statuses {
                state
                    .consolidation_job_status_sequences
                    .insert(status.status_id.clone(), snapshot.last_sequence);
                state
                    .consolidation_job_statuses
                    .insert(status.status_id.clone(), status);
            }
            for access in snapshot.access_records {
                state
                    .access_sequences
                    .insert(access.access_id.clone(), snapshot.last_sequence);
                state
                    .access_records
                    .insert(access.access_id.clone(), access);
            }
        }
        for entry in self.read_log_entries()? {
            let sequence = if entry.sequence == 0 {
                state
                    .last_sequence
                    .checked_add(1)
                    .ok_or(MemoryError::SequenceOverflow)?
            } else {
                entry.sequence
            };
            state.last_sequence = state.last_sequence.max(sequence);
            if sequence <= state.snapshot_sequence {
                continue;
            }
            match entry.op {
                MemoryLogOperation::PutRecord { record } => {
                    state
                        .record_sequences
                        .insert(record.memory_id.clone(), sequence);
                    state.records.insert(record.memory_id.clone(), record);
                }
                MemoryLogOperation::PutLink { link } => {
                    state.link_sequences.insert(link.link_id.clone(), sequence);
                    state.links.insert(link.link_id.clone(), link);
                }
                MemoryLogOperation::PutSupersession { supersession } => {
                    state
                        .supersession_sequences
                        .insert(supersession.supersession_id.clone(), sequence);
                    state
                        .supersessions
                        .insert(supersession.supersession_id.clone(), supersession);
                }
                MemoryLogOperation::PutConsolidation { consolidation } => {
                    state
                        .consolidation_sequences
                        .insert(consolidation.consolidation_id.clone(), sequence);
                    state
                        .consolidations
                        .insert(consolidation.consolidation_id.clone(), consolidation);
                }
                MemoryLogOperation::PutConsolidationJob { job } => {
                    state
                        .consolidation_job_sequences
                        .insert(job.job_id.clone(), sequence);
                    state.consolidation_jobs.insert(job.job_id.clone(), job);
                }
                MemoryLogOperation::PutConsolidationJobStatus { status } => {
                    state
                        .consolidation_job_status_sequences
                        .insert(status.status_id.clone(), sequence);
                    state
                        .consolidation_job_statuses
                        .insert(status.status_id.clone(), status);
                }
                MemoryLogOperation::PutAccess { access } => {
                    state
                        .access_sequences
                        .insert(access.access_id.clone(), sequence);
                    state
                        .access_records
                        .insert(access.access_id.clone(), access);
                }
            }
        }
        Ok(state)
    }

    fn read_log_entries(&self) -> Result<Vec<MemoryLogEntry>, MemoryError> {
        let mut entries = Vec::new();
        for path in self.log_paths()? {
            entries.extend(self.read_log_entries_from_path(&path)?);
        }
        entries.sort_by_key(|entry| entry.sequence);
        Ok(entries)
    }

    fn read_log_entries_from_path(&self, path: &Path) -> Result<Vec<MemoryLogEntry>, MemoryError> {
        let Some(bytes) = self.read_optional_bytes(path)? else {
            return Ok(Vec::new());
        };
        let mut entries = Vec::new();
        let mut offset = 0_usize;
        while offset < bytes.len() {
            if bytes.len().saturating_sub(offset) < 4 {
                break;
            }
            let length = u32::from_be_bytes([
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
            ]) as usize;
            if length > MAX_MEMORY_LOG_RECORD_BYTES {
                return Err(MemoryError::DurableFile {
                    path: path.display().to_string(),
                    detail: format!(
                        "length-prefixed record is {length} bytes; max is {MAX_MEMORY_LOG_RECORD_BYTES}"
                    ),
                });
            }
            let record_start = offset.saturating_add(4);
            let record_end = record_start.saturating_add(length);
            if record_end > bytes.len() {
                break;
            }
            let entry = decode_sobj::<MemoryLogEntry>(path, &bytes[record_start..record_end])?;
            if entry.schema_version != MEMORY_STORAGE_SCHEMA_VERSION {
                return Err(MemoryError::UnsupportedSchema {
                    path: path.display().to_string(),
                    schema_version: entry.schema_version,
                });
            }
            entries.push(entry);
            offset = record_end;
        }
        Ok(entries)
    }

    fn append_log_entry(&self, entry: &MemoryLogEntry) -> Result<(), MemoryError> {
        if entry.sequence == 0 {
            return Err(MemoryError::InvalidLogSequence { sequence: 0 });
        }
        let path = self.segment_path(entry.sequence);
        let bytes = encode_sobj(&path, entry)?;
        let length = u32::try_from(bytes.len()).map_err(|_| MemoryError::DurableFile {
            path: path.display().to_string(),
            detail: format!("length-prefixed record exceeds u32 length: {}", bytes.len()),
        })?;
        let mut framed = Vec::with_capacity(4 + bytes.len());
        framed.extend_from_slice(&length.to_be_bytes());
        framed.extend_from_slice(&bytes);
        self.append_bytes(&path, &framed)
    }

    fn write_index_snapshot(&self, state: &MemoryReplayState) -> Result<(), MemoryError> {
        let indexed_at = OffsetDateTime::now_utc();
        let entries = state
            .records
            .values()
            .map(|record| MemoryIndexEntry {
                memory_id: record.memory_id.clone(),
                scope: record.scope.clone(),
                tier: record.tier.clone(),
                sequence: state
                    .record_sequences
                    .get(&record.memory_id)
                    .copied()
                    .unwrap_or(state.snapshot_sequence),
                path: self.index_record_path(record, state),
                bytes: encode_sobj(&self.index_path(), record)
                    .map(|bytes| bytes.len())
                    .unwrap_or_default(),
                indexed_at,
            })
            .collect::<Vec<_>>();
        let mut derived_entries = Vec::new();
        for link in state.links.values() {
            derived_entries.push(
                self.derived_index_entry(
                    "memory-link",
                    &link.link_id,
                    state
                        .link_sequences
                        .get(&link.link_id)
                        .copied()
                        .unwrap_or(state.snapshot_sequence),
                    link,
                    indexed_at,
                    state,
                ),
            );
        }
        for supersession in state.supersessions.values() {
            derived_entries.push(
                self.derived_index_entry(
                    "memory-supersession",
                    &supersession.supersession_id,
                    state
                        .supersession_sequences
                        .get(&supersession.supersession_id)
                        .copied()
                        .unwrap_or(state.snapshot_sequence),
                    supersession,
                    indexed_at,
                    state,
                ),
            );
        }
        for consolidation in state.consolidations.values() {
            derived_entries.push(
                self.derived_index_entry(
                    "memory-consolidation",
                    &consolidation.consolidation_id,
                    state
                        .consolidation_sequences
                        .get(&consolidation.consolidation_id)
                        .copied()
                        .unwrap_or(state.snapshot_sequence),
                    consolidation,
                    indexed_at,
                    state,
                ),
            );
        }
        for job in state.consolidation_jobs.values() {
            derived_entries.push(
                self.derived_index_entry(
                    "memory-consolidation-job",
                    &job.job_id,
                    state
                        .consolidation_job_sequences
                        .get(&job.job_id)
                        .copied()
                        .unwrap_or(state.snapshot_sequence),
                    job,
                    indexed_at,
                    state,
                ),
            );
        }
        for status in state.consolidation_job_statuses.values() {
            derived_entries.push(
                self.derived_index_entry(
                    "memory-consolidation-job-status",
                    &status.status_id,
                    state
                        .consolidation_job_status_sequences
                        .get(&status.status_id)
                        .copied()
                        .unwrap_or(state.snapshot_sequence),
                    status,
                    indexed_at,
                    state,
                ),
            );
        }
        for access in state.access_records.values() {
            derived_entries.push(
                self.derived_index_entry(
                    "memory-access",
                    &access.access_id,
                    state
                        .access_sequences
                        .get(&access.access_id)
                        .copied()
                        .unwrap_or(state.snapshot_sequence),
                    access,
                    indexed_at,
                    state,
                ),
            );
        }
        let token_index = build_token_index(state.records.values());
        let index = MemoryIndexSnapshot {
            schema_version: MEMORY_STORAGE_SCHEMA_VERSION,
            last_sequence: state.last_sequence,
            entries,
            derived_entries,
            token_index,
        };
        self.write_sobj_file(&self.index_path(), &index)
    }

    fn index_record_path(&self, record: &MemoryRecord, state: &MemoryReplayState) -> String {
        let sequence = state
            .record_sequences
            .get(&record.memory_id)
            .copied()
            .unwrap_or(state.snapshot_sequence);
        if sequence == 0 || sequence <= state.snapshot_sequence {
            return format!("records/snapshot.sobj#{}", record.memory_id);
        }
        format!(
            "records/segments/{:016}.sobjlog#{}",
            self.segment_number_for_sequence(sequence),
            record.memory_id
        )
    }

    fn derived_index_entry<T: Serialize + ?Sized>(
        &self,
        kind: &str,
        derived_id: &Id,
        sequence: u64,
        value: &T,
        indexed_at: OffsetDateTime,
        state: &MemoryReplayState,
    ) -> MemoryDerivedIndexEntry {
        MemoryDerivedIndexEntry {
            derived_id: derived_id.clone(),
            kind: kind.to_string(),
            sequence,
            path: self.index_derived_path(kind, derived_id, sequence, state),
            bytes: encode_sobj(&self.index_path(), value)
                .map(|bytes| bytes.len())
                .unwrap_or_default(),
            indexed_at,
        }
    }

    fn index_derived_path(
        &self,
        kind: &str,
        derived_id: &Id,
        sequence: u64,
        state: &MemoryReplayState,
    ) -> String {
        if sequence == 0 || sequence <= state.snapshot_sequence {
            return format!("records/snapshot.sobj#{kind}/{derived_id}");
        }
        format!(
            "records/segments/{:016}.sobjlog#{kind}/{}",
            self.segment_number_for_sequence(sequence),
            derived_id
        )
    }

    fn log_paths(&self) -> Result<Vec<PathBuf>, MemoryError> {
        let mut paths = Vec::new();
        let segments_dir = self.segments_dir();
        let vfs_path = self.vfs_path_for(&segments_dir)?;
        for entry in self
            .vfs
            .list_if_exists(&vfs_path)
            .map_err(|source| memory_vfs_error(&segments_dir, source))?
        {
            if entry.kind == VfsObjectKind::File && entry.name.ends_with(".sobjlog") {
                paths.push(self.root.join(entry.path.trim_start_matches('/')));
            }
        }
        paths.sort();
        Ok(paths)
    }

    fn clear_log_segments(&self) -> Result<(), MemoryError> {
        for path in self.log_paths()? {
            self.remove_file_if_exists(&path)?;
        }
        self.remove_empty_dir_if_exists(&self.segments_dir())?;
        Ok(())
    }

    fn write_sobj_file<T: Serialize + ?Sized>(
        &self,
        path: &Path,
        value: &T,
    ) -> Result<(), MemoryError> {
        let bytes = encode_sobj(path, value)?;
        self.write_file_atomic(path, &bytes)
    }

    fn write_file_atomic(&self, path: &Path, bytes: &[u8]) -> Result<(), MemoryError> {
        self.write_bytes(path, bytes)
    }

    fn write_guard(&self) -> Result<std::sync::MutexGuard<'_, ()>, MemoryError> {
        self.write_lock
            .lock()
            .map_err(|source| MemoryError::LockPoisoned {
                detail: source.to_string(),
            })
    }

    fn segment_path(&self, sequence: u64) -> PathBuf {
        self.segments_dir().join(format!(
            "{:016}.sobjlog",
            self.segment_number_for_sequence(sequence)
        ))
    }

    fn segment_number_for_sequence(&self, sequence: u64) -> u64 {
        sequence
            .saturating_sub(1)
            .saturating_div(self.records_per_segment)
            .saturating_add(1)
    }

    fn segments_dir(&self) -> PathBuf {
        self.root.join("records").join("segments")
    }

    fn snapshot_path(&self) -> PathBuf {
        self.root.join("records").join("snapshot.sobj")
    }

    fn index_path(&self) -> PathBuf {
        self.root.join("index.sobj")
    }
}

impl MemoryContextProvider for DurableMemoryStore {
    fn retrieve(&self, query: &MemoryQuery) -> Result<MemoryRetrievalResult, MemoryError> {
        self.retrieve_records(query)
    }
}

impl MemoryRecordStore for DurableMemoryStore {
    fn write_record(&self, record: MemoryRecord) -> Result<MemoryRecord, MemoryError> {
        DurableMemoryStore::write_record(self, record)
    }

    fn promote_event(
        &self,
        event: &EventEnvelope,
        policy: &MemoryPolicy,
    ) -> Result<Option<MemoryRecord>, MemoryError> {
        DurableMemoryStore::promote_event(self, event, policy)
    }
}

pub type LogStructuredMemoryStore = DurableMemoryStore;

#[derive(Clone, Debug, Default)]
struct MemoryReplayState {
    snapshot_sequence: u64,
    last_sequence: u64,
    records: BTreeMap<MemoryId, MemoryRecord>,
    record_sequences: BTreeMap<MemoryId, u64>,
    links: BTreeMap<MemoryLinkId, MemoryLinkRecord>,
    link_sequences: BTreeMap<MemoryLinkId, u64>,
    supersessions: BTreeMap<MemorySupersessionId, MemorySupersessionRecord>,
    supersession_sequences: BTreeMap<MemorySupersessionId, u64>,
    consolidations: BTreeMap<MemoryConsolidationId, MemoryConsolidationRecord>,
    consolidation_sequences: BTreeMap<MemoryConsolidationId, u64>,
    consolidation_jobs: BTreeMap<MemoryConsolidationJobId, MemoryConsolidationJobRecord>,
    consolidation_job_sequences: BTreeMap<MemoryConsolidationJobId, u64>,
    consolidation_job_statuses:
        BTreeMap<MemoryConsolidationJobStatusId, MemoryConsolidationJobStatusRecord>,
    consolidation_job_status_sequences: BTreeMap<MemoryConsolidationJobStatusId, u64>,
    access_records: BTreeMap<MemoryAccessId, MemoryAccessRecord>,
    access_sequences: BTreeMap<MemoryAccessId, u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct MemoryLogEntry {
    schema_version: u32,
    #[serde(default)]
    sequence: u64,
    #[serde(with = "time::serde::rfc3339")]
    appended_at: OffsetDateTime,
    op: MemoryLogOperation,
}

impl MemoryLogEntry {
    fn put(sequence: u64, record: MemoryRecord) -> Self {
        Self {
            schema_version: MEMORY_STORAGE_SCHEMA_VERSION,
            sequence,
            appended_at: OffsetDateTime::now_utc(),
            op: MemoryLogOperation::PutRecord { record },
        }
    }

    fn put_link(sequence: u64, link: MemoryLinkRecord) -> Self {
        Self {
            schema_version: MEMORY_STORAGE_SCHEMA_VERSION,
            sequence,
            appended_at: OffsetDateTime::now_utc(),
            op: MemoryLogOperation::PutLink { link },
        }
    }

    fn put_supersession(sequence: u64, supersession: MemorySupersessionRecord) -> Self {
        Self {
            schema_version: MEMORY_STORAGE_SCHEMA_VERSION,
            sequence,
            appended_at: OffsetDateTime::now_utc(),
            op: MemoryLogOperation::PutSupersession { supersession },
        }
    }

    fn put_consolidation(sequence: u64, consolidation: MemoryConsolidationRecord) -> Self {
        Self {
            schema_version: MEMORY_STORAGE_SCHEMA_VERSION,
            sequence,
            appended_at: OffsetDateTime::now_utc(),
            op: MemoryLogOperation::PutConsolidation { consolidation },
        }
    }

    fn put_consolidation_job(sequence: u64, job: MemoryConsolidationJobRecord) -> Self {
        Self {
            schema_version: MEMORY_STORAGE_SCHEMA_VERSION,
            sequence,
            appended_at: OffsetDateTime::now_utc(),
            op: MemoryLogOperation::PutConsolidationJob { job },
        }
    }

    fn put_consolidation_job_status(
        sequence: u64,
        status: MemoryConsolidationJobStatusRecord,
    ) -> Self {
        Self {
            schema_version: MEMORY_STORAGE_SCHEMA_VERSION,
            sequence,
            appended_at: OffsetDateTime::now_utc(),
            op: MemoryLogOperation::PutConsolidationJobStatus { status },
        }
    }

    fn put_access(sequence: u64, access: MemoryAccessRecord) -> Self {
        Self {
            schema_version: MEMORY_STORAGE_SCHEMA_VERSION,
            sequence,
            appended_at: OffsetDateTime::now_utc(),
            op: MemoryLogOperation::PutAccess { access },
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
enum MemoryLogOperation {
    PutRecord {
        record: MemoryRecord,
    },
    PutLink {
        link: MemoryLinkRecord,
    },
    PutSupersession {
        supersession: MemorySupersessionRecord,
    },
    PutConsolidation {
        consolidation: MemoryConsolidationRecord,
    },
    PutConsolidationJob {
        job: MemoryConsolidationJobRecord,
    },
    PutConsolidationJobStatus {
        status: MemoryConsolidationJobStatusRecord,
    },
    PutAccess {
        access: MemoryAccessRecord,
    },
}

pub fn register_storage_paths(
    layout: &mut sinomo_runtime::storage::StorageLayout,
) -> Result<(), sinomo_runtime::storage::StorageContractError> {
    use sinomo_runtime::storage::{ObjectSchema, STORAGE_CONTRACT_OBJECT_VERSION, StorageRing};
    layout.register_escape_path(
        "sinomo-memory",
        StorageRing::R2,
        "memory/records/segments",
        "segmented length-prefixed SOBJ memory record logs",
    )?;
    layout.register_sinomo_object(
        "sinomo-memory",
        StorageRing::R2,
        "memory/records/snapshot.sobj",
        ObjectSchema::new("MemorySnapshot", STORAGE_CONTRACT_OBJECT_VERSION),
    )?;
    layout.register_sinomo_object(
        "sinomo-memory",
        StorageRing::R2,
        "memory/index.sobj",
        ObjectSchema::new("MemoryIndexSnapshot", STORAGE_CONTRACT_OBJECT_VERSION),
    )
}

pub fn memory_scope_sifs_path(scope: &MemoryScope) -> String {
    format!("/services/sinomo/memory/{}", memory_scope_export_key(scope))
}

fn memory_replication_plan_entries(
    state: &MemoryReplayState,
    policy: &MemoryScopeReplicationPolicy,
) -> Vec<MemoryReplicationPlanEntry> {
    let artifacts = memory_replication_artifacts(policy.mode);
    if artifacts.is_empty() {
        return Vec::new();
    }
    let record_count = state
        .records
        .values()
        .filter(|record| record.scope == policy.scope)
        .count();
    if record_count == 0 {
        return Vec::new();
    }

    let scope_key = memory_scope_export_key(&policy.scope);
    let sifs_path_prefix =
        normalize_sifs_path_prefix(policy.sifs_path_prefix.clone(), &policy.scope);
    artifacts
        .into_iter()
        .map(|artifact| {
            let file_name = artifact.file_name();
            MemoryReplicationPlanEntry {
                scope: policy.scope.clone(),
                artifact,
                source_ref: format!("memory-export://{scope_key}/{file_name}"),
                sifs_path: join_sifs_path(&sifs_path_prefix, file_name),
                record_count,
                consistency: policy.consistency,
                min_replicas: policy.min_replicas,
                target_node_ids: policy.target_node_ids.clone(),
            }
        })
        .collect()
}

fn memory_replication_artifacts(mode: MemoryReplicationMode) -> Vec<MemoryReplicationArtifactKind> {
    match mode {
        MemoryReplicationMode::None => Vec::new(),
        MemoryReplicationMode::Snapshot => vec![MemoryReplicationArtifactKind::ScopeSnapshot],
        MemoryReplicationMode::Records => vec![MemoryReplicationArtifactKind::ScopeRecords],
        MemoryReplicationMode::SnapshotAndIndex => vec![
            MemoryReplicationArtifactKind::ScopeSnapshot,
            MemoryReplicationArtifactKind::ScopeIndex,
        ],
        MemoryReplicationMode::RecordsAndIndex => vec![
            MemoryReplicationArtifactKind::ScopeRecords,
            MemoryReplicationArtifactKind::ScopeIndex,
        ],
    }
}

fn normalize_sifs_path_prefix(prefix: String, scope: &MemoryScope) -> String {
    let prefix = prefix.trim();
    if prefix.is_empty() {
        return memory_scope_sifs_path(scope);
    }
    if prefix == "/" {
        return "/".to_string();
    }
    let prefix = prefix.trim_end_matches('/');
    if prefix.starts_with('/') {
        prefix.to_string()
    } else {
        format!("/{prefix}")
    }
}

fn join_sifs_path(prefix: &str, file_name: &str) -> String {
    if prefix == "/" {
        format!("/{file_name}")
    } else {
        format!("{prefix}/{file_name}")
    }
}

fn memory_scope_export_key(scope: &MemoryScope) -> String {
    match scope {
        MemoryScope::Cluster => "cluster".to_string(),
        MemoryScope::Agent { agent_id } => {
            format!("agents/{}", sifs_path_segment(agent_id))
        }
        MemoryScope::Chat { chat_id } => {
            format!("chats/{}", sifs_path_segment(chat_id))
        }
        MemoryScope::Channel { channel_id } => {
            format!("channels/{}", sifs_path_segment(channel_id))
        }
        MemoryScope::Task { task_id } => {
            format!("tasks/{}", sifs_path_segment(task_id))
        }
        MemoryScope::Workspace { workspace_id } => {
            format!("workspaces/{}", sifs_path_segment(workspace_id))
        }
        MemoryScope::User { user_id } => {
            format!("users/{}", sifs_path_segment(user_id))
        }
        MemoryScope::Team { team_id } => {
            format!("teams/{}", sifs_path_segment(team_id))
        }
    }
}

fn sifs_path_segment(value: &str) -> String {
    let mut segment = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | ':') {
            segment.push(ch);
        } else {
            segment.push('_');
        }
    }
    if segment.is_empty() {
        "_".to_string()
    } else {
        segment
    }
}

fn derive_memory_link_id(
    from_memory_id: &MemoryId,
    to_memory_id: &MemoryId,
    relation: &MemoryLinkRelation,
    source_refs: &[MemorySourceRef],
    created_at: OffsetDateTime,
) -> MemoryLinkId {
    #[derive(Serialize)]
    struct LinkFingerprint<'a> {
        from_memory_id: &'a MemoryId,
        to_memory_id: &'a MemoryId,
        relation: &'a MemoryLinkRelation,
        source_refs: &'a [MemorySourceRef],
        #[serde(with = "time::serde::rfc3339")]
        created_at: OffsetDateTime,
    }

    let fingerprint = LinkFingerprint {
        from_memory_id,
        to_memory_id,
        relation,
        source_refs,
        created_at,
    };
    let bytes = serde_json::to_vec(&fingerprint).unwrap_or_default();
    Id::derived("memory-link", bytes)
}

fn derive_memory_supersession_id(
    superseded_memory_id: &MemoryId,
    replacement_memory_id: &MemoryId,
    reason: &str,
    source_refs: &[MemorySourceRef],
    created_at: OffsetDateTime,
) -> MemorySupersessionId {
    #[derive(Serialize)]
    struct SupersessionFingerprint<'a> {
        superseded_memory_id: &'a MemoryId,
        replacement_memory_id: &'a MemoryId,
        reason: &'a str,
        source_refs: &'a [MemorySourceRef],
        #[serde(with = "time::serde::rfc3339")]
        created_at: OffsetDateTime,
    }

    let fingerprint = SupersessionFingerprint {
        superseded_memory_id,
        replacement_memory_id,
        reason,
        source_refs,
        created_at,
    };
    let bytes = serde_json::to_vec(&fingerprint).unwrap_or_default();
    Id::derived("memory-supersession", bytes)
}

fn derive_memory_consolidation_id(
    consolidated_memory_id: &MemoryId,
    source_memory_ids: &[MemoryId],
    source_refs: &[MemorySourceRef],
    created_at: OffsetDateTime,
) -> MemoryConsolidationId {
    #[derive(Serialize)]
    struct ConsolidationFingerprint<'a> {
        consolidated_memory_id: &'a MemoryId,
        source_memory_ids: &'a [MemoryId],
        source_refs: &'a [MemorySourceRef],
        #[serde(with = "time::serde::rfc3339")]
        created_at: OffsetDateTime,
    }

    let fingerprint = ConsolidationFingerprint {
        consolidated_memory_id,
        source_memory_ids,
        source_refs,
        created_at,
    };
    let bytes = serde_json::to_vec(&fingerprint).unwrap_or_default();
    Id::derived("memory-consolidation", bytes)
}

fn derive_memory_consolidation_job_id(
    kind: &MemoryConsolidationJobKind,
    target_scope: &MemoryScope,
    target_tier: &MemoryTier,
    summary_hint: &str,
    source_memory_ids: &[MemoryId],
    source_refs: &[MemorySourceRef],
    created_at: OffsetDateTime,
) -> MemoryConsolidationJobId {
    #[derive(Serialize)]
    struct ConsolidationJobFingerprint<'a> {
        kind: &'a MemoryConsolidationJobKind,
        target_scope: &'a MemoryScope,
        target_tier: &'a MemoryTier,
        summary_hint: &'a str,
        source_memory_ids: &'a [MemoryId],
        source_refs: &'a [MemorySourceRef],
        #[serde(with = "time::serde::rfc3339")]
        created_at: OffsetDateTime,
    }

    let fingerprint = ConsolidationJobFingerprint {
        kind,
        target_scope,
        target_tier,
        summary_hint,
        source_memory_ids,
        source_refs,
        created_at,
    };
    let bytes = serde_json::to_vec(&fingerprint).unwrap_or_default();
    Id::derived("memory-consolidation-job", bytes)
}

fn derive_memory_consolidation_job_status_id(
    job_id: &MemoryConsolidationJobId,
    status: &MemoryConsolidationJobStatus,
    reason: &str,
    consolidation_id: Option<&MemoryConsolidationId>,
    source_refs: &[MemorySourceRef],
    recorded_at: OffsetDateTime,
) -> MemoryConsolidationJobStatusId {
    #[derive(Serialize)]
    struct ConsolidationJobStatusFingerprint<'a> {
        job_id: &'a MemoryConsolidationJobId,
        status: &'a MemoryConsolidationJobStatus,
        reason: &'a str,
        consolidation_id: Option<&'a MemoryConsolidationId>,
        source_refs: &'a [MemorySourceRef],
        #[serde(with = "time::serde::rfc3339")]
        recorded_at: OffsetDateTime,
    }

    let fingerprint = ConsolidationJobStatusFingerprint {
        job_id,
        status,
        reason,
        consolidation_id,
        source_refs,
        recorded_at,
    };
    let bytes = serde_json::to_vec(&fingerprint).unwrap_or_default();
    Id::derived("memory-consolidation-job-status", bytes)
}

fn derive_memory_embedding_id(
    memory_id: &MemoryId,
    provider: &str,
    model: &str,
    dimensions: usize,
    vector: &[i32],
    created_at: OffsetDateTime,
) -> MemoryEmbeddingId {
    #[derive(Serialize)]
    struct EmbeddingFingerprint<'a> {
        memory_id: &'a MemoryId,
        provider: &'a str,
        model: &'a str,
        dimensions: usize,
        vector: &'a [i32],
        #[serde(with = "time::serde::rfc3339")]
        created_at: OffsetDateTime,
    }

    let fingerprint = EmbeddingFingerprint {
        memory_id,
        provider,
        model,
        dimensions,
        vector,
        created_at,
    };
    let bytes = serde_json::to_vec(&fingerprint).unwrap_or_default();
    Id::derived("memory-embedding", bytes)
}

fn derive_memory_access_id(
    operation: &MemoryAccessOperation,
    memory_ids: &[MemoryId],
    reason: &str,
    source_refs: &[MemorySourceRef],
    created_at: OffsetDateTime,
) -> MemoryAccessId {
    #[derive(Serialize)]
    struct AccessFingerprint<'a> {
        operation: &'a MemoryAccessOperation,
        memory_ids: &'a [MemoryId],
        reason: &'a str,
        source_refs: &'a [MemorySourceRef],
        #[serde(with = "time::serde::rfc3339")]
        created_at: OffsetDateTime,
    }

    let fingerprint = AccessFingerprint {
        operation,
        memory_ids,
        reason,
        source_refs,
        created_at,
    };
    let bytes = serde_json::to_vec(&fingerprint).unwrap_or_default();
    Id::derived("memory-access", bytes)
}

fn next_sequence(last_sequence: u64) -> Result<u64, MemoryError> {
    last_sequence
        .checked_add(1)
        .ok_or(MemoryError::SequenceOverflow)
}

fn memory_record_visible(record: &MemoryRecord, query: &MemoryQuery) -> bool {
    match &record.scope {
        MemoryScope::Cluster => true,
        MemoryScope::Agent { agent_id } => query.agent_id.as_ref() == Some(agent_id),
        MemoryScope::Chat { chat_id } => query.chat_id.as_ref() == Some(chat_id),
        MemoryScope::Channel { channel_id } => query.channel_id.as_ref() == Some(channel_id),
        MemoryScope::Task { task_id } => query.task_id.as_ref() == Some(task_id),
        MemoryScope::Workspace { workspace_id } => {
            query.workspace_id.as_ref() == Some(workspace_id)
        }
        MemoryScope::User { user_id } => query.user_id.as_ref() == Some(user_id),
        MemoryScope::Team { team_id } => query.team_id.as_ref() == Some(team_id),
    }
}

fn memory_query_source_refs(query: &MemoryQuery) -> Vec<MemorySourceRef> {
    let mut source_refs = Vec::new();
    if let Some(session_id) = &query.session_id {
        push_source_ref_once(&mut source_refs, "session", session_id.to_string());
    }
    if let Some(agent_id) = &query.agent_id {
        push_source_ref_once(&mut source_refs, "agent", agent_id.clone());
    }
    if let Some(chat_id) = &query.chat_id {
        push_source_ref_once(&mut source_refs, "chat", chat_id.clone());
    }
    if let Some(channel_id) = &query.channel_id {
        push_source_ref_once(&mut source_refs, "channel", channel_id.clone());
    }
    if let Some(task_id) = &query.task_id {
        push_source_ref_once(&mut source_refs, "task", task_id.clone());
    }
    if let Some(workspace_id) = &query.workspace_id {
        push_source_ref_once(&mut source_refs, "workspace", workspace_id.clone());
    }
    if let Some(user_id) = &query.user_id {
        push_source_ref_once(&mut source_refs, "user", user_id.clone());
    }
    if let Some(team_id) = &query.team_id {
        push_source_ref_once(&mut source_refs, "team", team_id.clone());
    }
    if let Some(task_hint) = &query.task_hint {
        let task_hint = task_hint.trim();
        if !task_hint.is_empty() {
            push_source_ref_once(&mut source_refs, "task_hint", task_hint.to_string());
        }
    }
    source_refs
}

fn consolidated_memory_source_refs(
    mut source_refs: Vec<MemorySourceRef>,
    source_memory_ids: &[MemoryId],
) -> Vec<MemorySourceRef> {
    for memory_id in source_memory_ids {
        push_source_ref_once(&mut source_refs, "memory", memory_id.to_string());
    }
    source_refs
}

fn push_source_ref_once(
    source_refs: &mut Vec<MemorySourceRef>,
    kind: impl Into<String>,
    value: impl Into<String>,
) {
    let kind = kind.into();
    let value = value.into();
    if !source_refs
        .iter()
        .any(|source| source.kind == kind && source.value == value)
    {
        source_refs.push(MemorySourceRef::new(kind, value));
    }
}

fn ensure_memory_record_exists(
    state: &MemoryReplayState,
    memory_id: &MemoryId,
    kind: &'static str,
) -> Result<(), MemoryError> {
    if state.records.contains_key(memory_id) {
        Ok(())
    } else {
        Err(MemoryError::MissingMemoryRecord {
            kind,
            memory_id: memory_id.clone(),
        })
    }
}

fn latest_supersessions_by_memory(
    state: &MemoryReplayState,
) -> BTreeMap<MemoryId, MemorySupersessionRecord> {
    let mut superseded_by = BTreeMap::new();
    for supersession in state.supersessions.values() {
        let replace = superseded_by
            .get(&supersession.superseded_memory_id)
            .map(|existing: &MemorySupersessionRecord| {
                supersession.created_at > existing.created_at
                    || (supersession.created_at == existing.created_at
                        && supersession.supersession_id > existing.supersession_id)
            })
            .unwrap_or(true);
        if replace {
            superseded_by.insert(
                supersession.superseded_memory_id.clone(),
                supersession.clone(),
            );
        }
    }
    superseded_by
}

fn latest_consolidation_job_statuses_by_job(
    state: &MemoryReplayState,
) -> BTreeMap<MemoryConsolidationJobId, MemoryConsolidationJobStatusRecord> {
    let mut latest = BTreeMap::new();
    for status in state.consolidation_job_statuses.values() {
        let replace = latest
            .get(&status.job_id)
            .map(|existing: &MemoryConsolidationJobStatusRecord| {
                status.recorded_at > existing.recorded_at
                    || (status.recorded_at == existing.recorded_at
                        && status.status_id > existing.status_id)
            })
            .unwrap_or(true);
        if replace {
            latest.insert(status.job_id.clone(), status.clone());
        }
    }
    latest
}

fn deterministic_consolidation_summary(
    job: &MemoryConsolidationJobRecord,
    source_records: &[MemoryRecord],
) -> String {
    const MAX_SOURCE_SNIPPET_BYTES: usize = 512;
    let mut summary = String::new();
    summary.push_str(memory_consolidation_job_kind_label(&job.kind));
    if !job.summary_hint.trim().is_empty() {
        summary.push_str(": ");
        summary.push_str(job.summary_hint.trim());
    }
    for (index, record) in source_records.iter().enumerate() {
        summary.push('\n');
        summary.push_str("- source ");
        summary.push_str(&(index + 1).to_string());
        summary.push_str(" [");
        summary.push_str(&record.memory_id.to_string());
        summary.push_str("]: ");
        summary.push_str(&bounded_summary_snippet(
            &record.summary,
            MAX_SOURCE_SNIPPET_BYTES,
        ));
    }
    summary
}

fn memory_consolidation_job_kind_label(kind: &MemoryConsolidationJobKind) -> &'static str {
    match kind {
        MemoryConsolidationJobKind::SessionBoundaryRecap => "session boundary recap",
        MemoryConsolidationJobKind::ChatChannelRecap => "chat/channel recap",
        MemoryConsolidationJobKind::TaskOutcome => "task outcome",
        MemoryConsolidationJobKind::PreferenceExtraction => "preference extraction",
        MemoryConsolidationJobKind::IncidentLearning => "incident learning",
        MemoryConsolidationJobKind::Reflection => "reflection",
        MemoryConsolidationJobKind::SkillPromotion => "skill promotion",
    }
}

fn bounded_summary_snippet(text: &str, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text.to_string();
    }
    let mut end = max_bytes;
    while !text.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    let mut snippet = text[..end].trim_end().to_string();
    snippet.push_str("...");
    snippet
}

fn evaluate_memory_retrieval_case(
    case: &MemoryEvaluationCase,
    retrieval: &MemoryRetrievalResult,
    latency_micros: u64,
) -> MemoryEvaluationCaseReport {
    let selected_memory_ids = retrieval
        .records
        .iter()
        .map(|record| record.record.memory_id.clone())
        .collect::<Vec<_>>();
    let selected = selected_memory_ids.iter().cloned().collect::<BTreeSet<_>>();
    let expected = case
        .expected_memory_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let forbidden = case
        .forbidden_memory_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let superseded = retrieval
        .report
        .superseded
        .iter()
        .map(|trace| trace.memory_id.clone())
        .collect::<BTreeSet<_>>();

    let missing_expected_memory_ids = expected.difference(&selected).cloned().collect::<Vec<_>>();
    let unexpected_forbidden_memory_ids = selected
        .intersection(&forbidden)
        .cloned()
        .collect::<Vec<_>>();
    let missing_superseded_memory_ids = case
        .expected_superseded_memory_ids
        .iter()
        .filter(|memory_id| !superseded.contains(*memory_id))
        .cloned()
        .collect::<Vec<_>>();
    let selected_superseded_memory_ids = case
        .expected_superseded_memory_ids
        .iter()
        .filter(|memory_id| selected.contains(*memory_id))
        .cloned()
        .collect::<Vec<_>>();
    let missing_source_refs = case
        .required_source_refs
        .iter()
        .filter(|required| {
            !retrieval.records.iter().any(|record| {
                record
                    .record
                    .source_refs
                    .iter()
                    .any(|source| source == *required)
            })
        })
        .cloned()
        .collect::<Vec<_>>();
    let relevant_selected_records = selected.intersection(&expected).count();
    let cache_reuse_prefix_records =
        common_prefix_len(&case.previous_included_memory_ids, &selected_memory_ids);
    let cache_churn_records = case
        .previous_included_memory_ids
        .len()
        .saturating_sub(cache_reuse_prefix_records)
        + selected_memory_ids
            .len()
            .saturating_sub(cache_reuse_prefix_records);

    MemoryEvaluationCaseReport {
        name: case.name.clone(),
        selected_memory_ids,
        missing_expected_memory_ids,
        unexpected_forbidden_memory_ids,
        missing_superseded_memory_ids,
        missing_source_refs,
        selected_superseded_memory_ids,
        expected_records: case.expected_memory_ids.len(),
        recalled_records: relevant_selected_records,
        selected_records: retrieval.records.len(),
        relevant_selected_records,
        cache_reuse_prefix_records,
        cache_churn_records,
        latency_micros,
    }
}

fn accumulate_memory_evaluation_summary(
    summary: &mut MemoryEvaluationSummary,
    case: &MemoryEvaluationCaseReport,
) {
    if case.passed() {
        summary.passed_cases += 1;
    }
    summary.expected_records += case.expected_records;
    summary.recalled_records += case.recalled_records;
    summary.selected_records += case.selected_records;
    summary.relevant_selected_records += case.relevant_selected_records;
    summary.forbidden_selected_records += case.unexpected_forbidden_memory_ids.len();
    summary.missing_superseded_records += case.missing_superseded_memory_ids.len();
    summary.missing_source_refs += case.missing_source_refs.len();
    summary.cache_reuse_prefix_records += case.cache_reuse_prefix_records;
    summary.cache_churn_records += case.cache_churn_records;
    summary.max_latency_micros = summary.max_latency_micros.max(case.latency_micros);
}

fn common_prefix_len(left: &[MemoryId], right: &[MemoryId]) -> usize {
    left.iter()
        .zip(right.iter())
        .take_while(|(left, right)| left == right)
        .count()
}

fn duration_micros(duration: std::time::Duration) -> u64 {
    duration.as_micros().min(u128::from(u64::MAX)) as u64
}

fn memory_scope_rank(scope: &MemoryScope) -> u8 {
    match scope {
        MemoryScope::Cluster => 0,
        MemoryScope::User { .. } => 1,
        MemoryScope::Workspace { .. } => 2,
        MemoryScope::Team { .. } => 3,
        MemoryScope::Channel { .. } => 4,
        MemoryScope::Chat { .. } => 5,
        MemoryScope::Task { .. } => 6,
        MemoryScope::Agent { .. } => 7,
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
    if let Some(chat_id) = &query.chat_id
        && matches!(&record.scope, MemoryScope::Chat { chat_id: scoped } if scoped == chat_id)
    {
        score += 18;
        reasons.push("same chat");
    }
    if let Some(channel_id) = &query.channel_id
        && matches!(&record.scope, MemoryScope::Channel { channel_id: scoped } if scoped == channel_id)
    {
        score += 18;
        reasons.push("same channel");
    }
    if let Some(task_id) = &query.task_id
        && matches!(&record.scope, MemoryScope::Task { task_id: scoped } if scoped == task_id)
    {
        score += 22;
        reasons.push("same task");
    }
    if let Some(workspace_id) = &query.workspace_id
        && matches!(&record.scope, MemoryScope::Workspace { workspace_id: scoped } if scoped == workspace_id)
    {
        score += 12;
        reasons.push("same workspace");
    }
    if let Some(user_id) = &query.user_id
        && matches!(&record.scope, MemoryScope::User { user_id: scoped } if scoped == user_id)
    {
        score += 16;
        reasons.push("same user");
    }
    if let Some(team_id) = &query.team_id
        && matches!(&record.scope, MemoryScope::Team { team_id: scoped } if scoped == team_id)
    {
        score += 14;
        reasons.push("same team");
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

fn build_token_index<'a>(
    records: impl IntoIterator<Item = &'a MemoryRecord>,
) -> Vec<MemoryTokenIndexEntry> {
    let mut by_token = BTreeMap::<String, BTreeSet<MemoryId>>::new();
    for record in records {
        for token in memory_record_tokens(record) {
            by_token
                .entry(token)
                .or_default()
                .insert(record.memory_id.clone());
        }
    }
    by_token
        .into_iter()
        .map(|(token, memory_ids)| MemoryTokenIndexEntry {
            token,
            memory_ids: memory_ids.into_iter().collect(),
        })
        .collect()
}

fn memory_record_tokens(record: &MemoryRecord) -> BTreeSet<String> {
    let mut tokens = BTreeSet::new();
    collect_tokens(&record.summary, &mut tokens);
    for source in &record.source_refs {
        collect_tokens(&source.kind, &mut tokens);
        collect_tokens(&source.value, &mut tokens);
    }
    match &record.scope {
        MemoryScope::Cluster => {}
        MemoryScope::Agent { agent_id } => collect_tokens(agent_id, &mut tokens),
        MemoryScope::Chat { chat_id } => collect_tokens(chat_id, &mut tokens),
        MemoryScope::Channel { channel_id } => collect_tokens(channel_id, &mut tokens),
        MemoryScope::Task { task_id } => collect_tokens(task_id, &mut tokens),
        MemoryScope::Workspace { workspace_id } => collect_tokens(workspace_id, &mut tokens),
        MemoryScope::User { user_id } => collect_tokens(user_id, &mut tokens),
        MemoryScope::Team { team_id } => collect_tokens(team_id, &mut tokens),
    }
    tokens
}

fn collect_tokens(text: &str, tokens: &mut BTreeSet<String>) {
    for token in text
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .map(|token| token.trim().to_ascii_lowercase())
        .filter(|token| token.len() >= 2)
    {
        tokens.insert(token);
    }
}

fn memory_query_tokens(query: &MemoryQuery) -> BTreeSet<String> {
    let mut tokens = BTreeSet::new();
    if let Some(session_id) = &query.session_id {
        collect_tokens(&session_id.to_string(), &mut tokens);
    }
    if let Some(agent_id) = &query.agent_id {
        collect_tokens(agent_id, &mut tokens);
    }
    if let Some(chat_id) = &query.chat_id {
        collect_tokens(chat_id, &mut tokens);
    }
    if let Some(channel_id) = &query.channel_id {
        collect_tokens(channel_id, &mut tokens);
    }
    if let Some(task_id) = &query.task_id {
        collect_tokens(task_id, &mut tokens);
    }
    if let Some(workspace_id) = &query.workspace_id {
        collect_tokens(workspace_id, &mut tokens);
    }
    if let Some(user_id) = &query.user_id {
        collect_tokens(user_id, &mut tokens);
    }
    if let Some(team_id) = &query.team_id {
        collect_tokens(team_id, &mut tokens);
    }
    if let Some(task_hint) = &query.task_hint {
        collect_tokens(task_hint, &mut tokens);
    }
    tokens
}

fn token_hash_vector(tokens: BTreeSet<String>, dimensions: usize) -> Vec<i32> {
    let dimensions = dimensions.max(1);
    let mut vector = vec![0i32; dimensions];
    for token in tokens {
        let hash = stable_token_hash(&token);
        let slot = (hash as usize) % dimensions;
        let sign = if hash & 1 == 0 { 1 } else { -1 };
        vector[slot] += sign;
    }
    vector
}

fn stable_token_hash(token: &str) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in token.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn dot_product(left: &[i32], right: &[i32]) -> i64 {
    left.iter()
        .zip(right.iter())
        .map(|(left, right)| i64::from(*left) * i64::from(*right))
        .sum()
}

fn build_memory_graph_index(
    records: &[MemoryRecord],
    links: &[MemoryLinkRecord],
    supersessions: &[MemorySupersessionRecord],
    consolidations: &[MemoryConsolidationRecord],
) -> MemoryGraphIndexSnapshot {
    let known_memory_ids = records
        .iter()
        .map(|record| record.memory_id.clone())
        .collect::<BTreeSet<_>>();
    let mut nodes = records
        .iter()
        .map(|record| MemoryGraphNodeIndexEntry {
            memory_id: record.memory_id.clone(),
            scope: record.scope.clone(),
            tier: record.tier.clone(),
        })
        .collect::<Vec<_>>();
    nodes.sort_by(|left, right| left.memory_id.cmp(&right.memory_id));

    let mut edges = Vec::new();
    for link in links {
        if known_memory_ids.contains(&link.from_memory_id)
            && known_memory_ids.contains(&link.to_memory_id)
        {
            edges.push(MemoryGraphEdgeIndexEntry {
                from_memory_id: link.from_memory_id.clone(),
                to_memory_id: link.to_memory_id.clone(),
                relation: memory_link_relation_label(&link.relation).to_string(),
                derived_id: link.link_id.clone(),
            });
        }
    }
    for supersession in supersessions {
        if known_memory_ids.contains(&supersession.superseded_memory_id)
            && known_memory_ids.contains(&supersession.replacement_memory_id)
        {
            edges.push(MemoryGraphEdgeIndexEntry {
                from_memory_id: supersession.superseded_memory_id.clone(),
                to_memory_id: supersession.replacement_memory_id.clone(),
                relation: "superseded-by".to_string(),
                derived_id: supersession.supersession_id.clone(),
            });
        }
    }
    for consolidation in consolidations {
        if known_memory_ids.contains(&consolidation.consolidated_memory_id) {
            for source_memory_id in &consolidation.source_memory_ids {
                if known_memory_ids.contains(source_memory_id) {
                    edges.push(MemoryGraphEdgeIndexEntry {
                        from_memory_id: source_memory_id.clone(),
                        to_memory_id: consolidation.consolidated_memory_id.clone(),
                        relation: "consolidated-into".to_string(),
                        derived_id: consolidation.consolidation_id.clone(),
                    });
                }
            }
        }
    }
    edges.sort_by(|left, right| {
        left.from_memory_id
            .cmp(&right.from_memory_id)
            .then_with(|| left.to_memory_id.cmp(&right.to_memory_id))
            .then_with(|| left.relation.cmp(&right.relation))
            .then_with(|| left.derived_id.cmp(&right.derived_id))
    });

    MemoryGraphIndexSnapshot { nodes, edges }
}

fn memory_link_relation_label(relation: &MemoryLinkRelation) -> &'static str {
    match relation {
        MemoryLinkRelation::Supports => "supports",
        MemoryLinkRelation::Contradicts => "contradicts",
        MemoryLinkRelation::Elaborates => "elaborates",
        MemoryLinkRelation::Caused => "caused",
        MemoryLinkRelation::Supersedes => "supersedes",
        MemoryLinkRelation::SameTask => "same-task",
        MemoryLinkRelation::SameEntity => "same-entity",
        MemoryLinkRelation::Related => "related",
    }
}

fn encode_sobj<T: Serialize + ?Sized>(path: &Path, value: &T) -> Result<Vec<u8>, MemoryError> {
    serde_ext::to_bytes(value, MEMORY_SOBJ_VERSION, LengthMode::U32).map_err(|source| {
        MemoryError::SobjSerialize {
            path: path.display().to_string(),
            detail: source.to_string(),
        }
    })
}

fn decode_sobj<T: for<'de> Deserialize<'de>>(path: &Path, bytes: &[u8]) -> Result<T, MemoryError> {
    serde_ext::from_bytes(bytes).map_err(|source| MemoryError::SobjDeserialize {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

fn vfs_parent_path(path: &str) -> Option<&str> {
    path.rsplit_once('/').map(|(parent, _)| parent)
}

fn memory_vfs_error(path: &Path, source: VfsError) -> MemoryError {
    MemoryError::Vfs {
        path: path.display().to_string(),
        detail: source.to_string(),
    }
}

fn promotion_reason(event: &EventEnvelope) -> Option<PromotionReason> {
    let kind = event.kind.as_str();
    if kind.starts_with("safety.") {
        Some(PromotionReason::SafetyIncident)
    } else if kind.starts_with("memory.") {
        Some(PromotionReason::UserImportant)
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
    #[error("{kind} id is not canonical for record content: expected {expected}, got {actual}")]
    NonCanonicalDerivedMemoryId {
        kind: &'static str,
        expected: Id,
        actual: Id,
    },
    #[error("{kind} references missing memory record {memory_id}")]
    MissingMemoryRecord {
        kind: &'static str,
        memory_id: MemoryId,
    },
    #[error("memory consolidation requires at least one source memory record")]
    EmptyConsolidationSources,
    #[error("memory consolidation job requires source memory records or source references")]
    EmptyConsolidationJobSources,
    #[error("memory consolidation job status references missing job {job_id}")]
    MissingConsolidationJob { job_id: MemoryConsolidationJobId },
    #[error(
        "memory consolidation job status references missing consolidation record {consolidation_id}"
    )]
    MissingConsolidationRecord {
        consolidation_id: MemoryConsolidationId,
    },
    #[error("memory I/O error at {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("memory durable file error at {path}: {detail}")]
    DurableFile { path: String, detail: String },
    #[error("memory VFS error at {path}: {detail}")]
    Vfs { path: String, detail: String },
    #[error("failed to serialize memory SOBJ for {path}: {detail}")]
    SobjSerialize { path: String, detail: String },
    #[error("failed to deserialize memory SOBJ from {path}: {detail}")]
    SobjDeserialize { path: String, detail: String },
    #[error("unsupported memory storage schema {schema_version} at {path}")]
    UnsupportedSchema { path: String, schema_version: u32 },
    #[error("memory log sequence overflow")]
    SequenceOverflow,
    #[error("invalid memory log sequence {sequence}")]
    InvalidLogSequence { sequence: u64 },
    #[error("memory store lock poisoned: {detail}")]
    LockPoisoned { detail: String },
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
    use std::fs;

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

    fn trust_local() -> TrustDomain {
        TrustDomain::new("trust.local").expect("trust")
    }

    fn fixed_time(nanos: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("time")
            + time::Duration::nanoseconds(nanos)
    }

    fn selected_memory_ids(result: &MemoryRetrievalResult) -> Vec<MemoryId> {
        result
            .records
            .iter()
            .map(|record| record.record.memory_id.clone())
            .collect()
    }

    fn assert_source_ref(source_refs: &[MemorySourceRef], kind: &str, value: &str) {
        assert!(
            source_refs
                .iter()
                .any(|source| source.kind == kind && source.value == value),
            "missing source ref {kind}={value}"
        );
    }

    fn assert_noncanonical_derived(error: MemoryError, expected_kind: &'static str) {
        match error {
            MemoryError::NonCanonicalDerivedMemoryId { kind, .. } => {
                assert_eq!(kind, expected_kind)
            }
            other => panic!("expected non-canonical {expected_kind} id, got {other:?}"),
        }
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
    fn explicit_memory_events_promote_below_salience_threshold() {
        let manager = MemoryManager::new(MemoryPolicy {
            hot_event_limit: 4,
            warm_salience_threshold: 0.9,
        });
        let event = stored("evt_memory", "memory.user-important", 0.1).event;

        let promoted = manager
            .promote_event(&event)
            .expect("promoted memory event");

        assert_eq!(promoted.reason, PromotionReason::UserImportant);
        assert!(promoted.record.summary.contains("urgent alert"));
    }

    #[test]
    fn durable_store_writes_immutable_records_and_index() {
        let root = temp_memory_root("sinomo-memory-durable");
        let store = DurableMemoryStore::new(&root);
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

        assert!(
            root.join("records")
                .join("segments")
                .join("0000000000000001.sobjlog")
                .exists()
        );
        assert!(root.join("index.sobj").exists());
        let index = store
            .load_index()
            .expect("load index")
            .expect("index exists");
        assert_eq!(index.entries.len(), 1);
        assert_eq!(index.last_sequence, 2);
        assert_eq!(index.entries[0].memory_id, record.memory_id);
        assert_eq!(index.entries[0].sequence, 1);
        assert!(index.entries[0].path.starts_with("records/segments/"));
        assert!(index.derived_entries.iter().any(|entry| {
            entry.kind == "memory-access"
                && entry.sequence == 2
                && entry.path.starts_with("records/segments/")
        }));
        assert!(
            index
                .token_index
                .iter()
                .any(|entry| entry.token == "patches"
                    && entry.memory_ids.contains(&record.memory_id))
        );
    }

    #[test]
    fn durable_store_retrieves_cluster_and_agent_memory() {
        let root = temp_memory_root("sinomo-memory-retrieve");
        let store = DurableMemoryStore::new(&root);
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

    #[test]
    fn durable_store_retrieval_report_traces_included_filtered_and_omitted() {
        let root = temp_memory_root("sinomo-memory-trace");
        let store = DurableMemoryStore::new(&root);
        let cluster = store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "Cluster memory should remain available",
                Vec::new(),
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("cluster record");
        let agent = store
            .put_record(
                MemoryScope::Agent {
                    agent_id: "coding-agent".to_string(),
                },
                MemoryTier::Warm,
                "Agent memory should rank first for the same agent",
                Vec::new(),
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("agent record");
        let other_agent = store
            .put_record(
                MemoryScope::Agent {
                    agent_id: "review-agent".to_string(),
                },
                MemoryTier::Warm,
                "Other agent memory must not leak into this query",
                Vec::new(),
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("other agent record");

        let result = store
            .retrieve(&MemoryQuery {
                agent_id: Some("coding-agent".to_string()),
                max_records: 1,
                max_bytes: 16 * 1024,
                ..MemoryQuery::default()
            })
            .expect("retrieve");

        assert_eq!(result.records.len(), 1);
        assert_eq!(result.records[0].record.memory_id, agent.memory_id);
        assert_eq!(result.report.considered_records, 2);
        assert_eq!(result.report.selected_records, 1);
        assert_eq!(result.report.included.len(), 1);
        assert_eq!(result.report.included[0].memory_id, agent.memory_id);
        assert_eq!(result.report.omitted.len(), 1);
        assert_eq!(result.report.omitted[0].memory_id, cluster.memory_id);
        assert_eq!(result.report.omitted[0].reason, "record limit reached");
        assert_eq!(result.report.filtered.len(), 1);
        assert_eq!(result.report.filtered[0].memory_id, other_agent.memory_id);
        assert_eq!(
            result.report.filtered[0].reason,
            "scope not visible for query"
        );
        let retrieval_access = store
            .access_records()
            .expect("access records")
            .into_iter()
            .find(|record| record.operation == MemoryAccessOperation::Retrieval)
            .expect("retrieval audit");
        assert_eq!(retrieval_access.memory_ids, vec![agent.memory_id]);
        assert!(
            retrieval_access
                .source_refs
                .iter()
                .any(|source| source.kind == "agent" && source.value == "coding-agent")
        );
    }

    #[test]
    fn durable_store_gates_all_non_cluster_scopes_by_query_policy() {
        let root = temp_memory_root("sinomo-memory-scope-policy");
        let store = DurableMemoryStore::new(&root);
        let specs = [
            (MemoryScope::Cluster, "cluster memory"),
            (
                MemoryScope::Agent {
                    agent_id: "agent_a".to_string(),
                },
                "agent memory",
            ),
            (
                MemoryScope::Chat {
                    chat_id: "chat_a".to_string(),
                },
                "chat memory",
            ),
            (
                MemoryScope::Channel {
                    channel_id: "channel_a".to_string(),
                },
                "channel memory",
            ),
            (
                MemoryScope::Task {
                    task_id: "task_a".to_string(),
                },
                "task memory",
            ),
            (
                MemoryScope::Workspace {
                    workspace_id: "workspace_a".to_string(),
                },
                "workspace memory",
            ),
            (
                MemoryScope::User {
                    user_id: "user_a".to_string(),
                },
                "user memory",
            ),
            (
                MemoryScope::Team {
                    team_id: "team_a".to_string(),
                },
                "team memory",
            ),
        ];
        let records = specs
            .into_iter()
            .map(|(scope, summary)| {
                store
                    .put_record(
                        scope,
                        MemoryTier::Warm,
                        summary,
                        Vec::new(),
                        TrustDomain::new("trust.local").expect("trust"),
                        OffsetDateTime::now_utc(),
                    )
                    .expect("record")
            })
            .collect::<Vec<_>>();

        let authorized = store
            .retrieve(&MemoryQuery {
                agent_id: Some("agent_a".to_string()),
                chat_id: Some("chat_a".to_string()),
                channel_id: Some("channel_a".to_string()),
                task_id: Some("task_a".to_string()),
                workspace_id: Some("workspace_a".to_string()),
                user_id: Some("user_a".to_string()),
                team_id: Some("team_a".to_string()),
                max_records: 16,
                max_bytes: 64 * 1024,
                ..MemoryQuery::default()
            })
            .expect("authorized retrieve");
        assert_eq!(authorized.records.len(), records.len());
        for record in &records {
            assert!(
                authorized
                    .records
                    .iter()
                    .any(|retrieved| retrieved.record.memory_id == record.memory_id),
                "expected scope-visible record {}",
                record.summary
            );
        }
        assert!(authorized.report.filtered.is_empty());

        let unauthorized = store
            .retrieve(&MemoryQuery {
                agent_id: Some("agent_b".to_string()),
                chat_id: Some("chat_b".to_string()),
                channel_id: Some("channel_b".to_string()),
                task_id: Some("task_b".to_string()),
                workspace_id: Some("workspace_b".to_string()),
                user_id: Some("user_b".to_string()),
                team_id: Some("team_b".to_string()),
                max_records: 16,
                max_bytes: 64 * 1024,
                ..MemoryQuery::default()
            })
            .expect("unauthorized retrieve");
        assert_eq!(unauthorized.records.len(), 1);
        assert!(matches!(
            unauthorized.records[0].record.scope,
            MemoryScope::Cluster
        ));
        assert_eq!(unauthorized.report.filtered.len(), records.len() - 1);
    }

    #[test]
    fn durable_store_does_not_treat_session_as_visibility_scope() {
        let root = temp_memory_root("sinomo-memory-session-not-scope");
        let store = DurableMemoryStore::new(&root);
        let session_id = SessionId::from_legacy_named("sess_shared").expect("session");
        let session_ref = MemorySourceRef::new("session", session_id.to_string());
        let cluster = store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "Cluster-visible memory with session provenance",
                vec![session_ref.clone()],
                trust_local(),
                fixed_time(1),
            )
            .expect("cluster record");
        let hidden_agent = store
            .put_record(
                MemoryScope::Agent {
                    agent_id: "agent_a".to_string(),
                },
                MemoryTier::Warm,
                "Agent-private memory with matching session provenance",
                vec![session_ref.clone()],
                trust_local(),
                fixed_time(2),
            )
            .expect("agent record");
        let hidden_chat = store
            .put_record(
                MemoryScope::Chat {
                    chat_id: "chat_a".to_string(),
                },
                MemoryTier::Warm,
                "Chat memory with matching session provenance",
                vec![session_ref.clone()],
                trust_local(),
                fixed_time(3),
            )
            .expect("chat record");
        let hidden_task = store
            .put_record(
                MemoryScope::Task {
                    task_id: "task_a".to_string(),
                },
                MemoryTier::Warm,
                "Task memory with matching session provenance",
                vec![session_ref],
                trust_local(),
                fixed_time(4),
            )
            .expect("task record");

        let result = store
            .retrieve(&MemoryQuery {
                session_id: Some(session_id),
                max_records: 16,
                max_bytes: 64 * 1024,
                ..MemoryQuery::default()
            })
            .expect("retrieve");

        assert_eq!(selected_memory_ids(&result), vec![cluster.memory_id]);
        for hidden in [hidden_agent, hidden_chat, hidden_task] {
            assert!(
                result
                    .report
                    .filtered
                    .iter()
                    .any(|entry| entry.memory_id == hidden.memory_id),
                "expected session-provenance-only record to be filtered"
            );
        }
    }

    #[test]
    fn durable_store_retrieval_enforces_byte_limit_and_audits_only_selected() {
        let root = temp_memory_root("sinomo-memory-byte-limit");
        let store = DurableMemoryStore::new(&root);
        let small = store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "Small byte bounded memory",
                Vec::new(),
                trust_local(),
                fixed_time(1),
            )
            .expect("small record");
        let large = store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "Large byte bounded memory ".repeat(512),
                Vec::new(),
                trust_local(),
                fixed_time(2),
            )
            .expect("large record");
        let small_bytes = serde_json::to_vec(&small).expect("serialize").len();

        let result = store
            .retrieve(&MemoryQuery {
                max_records: 8,
                max_bytes: small_bytes,
                ..MemoryQuery::default()
            })
            .expect("retrieve");

        assert_eq!(selected_memory_ids(&result), vec![small.memory_id.clone()]);
        assert_eq!(result.report.selected_bytes, small_bytes);
        assert!(result.report.omitted.iter().any(|entry| {
            entry.memory_id == large.memory_id && entry.reason == "byte limit reached"
        }));
        let retrieval_accesses = store
            .access_records()
            .expect("access records")
            .into_iter()
            .filter(|record| record.operation == MemoryAccessOperation::Retrieval)
            .collect::<Vec<_>>();
        assert_eq!(retrieval_accesses.len(), 1);
        assert_eq!(retrieval_accesses[0].memory_ids, vec![small.memory_id]);
    }

    #[test]
    fn durable_store_retrieval_order_is_stable_after_irrelevant_append() {
        let root = temp_memory_root("sinomo-memory-cache-stability");
        let store = DurableMemoryStore::new(&root);
        let first = store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "Stable prefix memory one",
                Vec::new(),
                trust_local(),
                fixed_time(1),
            )
            .expect("first record");
        let second = store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "Stable prefix memory two",
                Vec::new(),
                trust_local(),
                fixed_time(2),
            )
            .expect("second record");
        let query = MemoryQuery {
            max_records: 2,
            max_bytes: 64 * 1024,
            ..MemoryQuery::default()
        };
        let before = selected_memory_ids(&store.retrieve(&query).expect("retrieve before"));

        let later = store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "Later appended cluster memory should not churn the selected prefix",
                Vec::new(),
                trust_local(),
                fixed_time(3),
            )
            .expect("later record");
        store
            .put_record(
                MemoryScope::Agent {
                    agent_id: "other-agent".to_string(),
                },
                MemoryTier::Warm,
                "Filtered agent-private memory should not churn the selected prefix",
                Vec::new(),
                trust_local(),
                fixed_time(4),
            )
            .expect("filtered record");

        assert_eq!(
            before,
            vec![first.memory_id.clone(), second.memory_id.clone()]
        );
        let after_result = store.retrieve(&query).expect("retrieve after");
        assert_eq!(selected_memory_ids(&after_result), before);
        assert!(after_result.report.omitted.iter().any(|entry| {
            entry.memory_id == later.memory_id && entry.reason == "record limit reached"
        }));

        let report = evaluate_memory_retrieval(
            &store,
            &MemoryEvaluationSuite {
                cases: vec![MemoryEvaluationCase {
                    name: "stable prefix after append".to_string(),
                    query,
                    expected_memory_ids: before.clone(),
                    forbidden_memory_ids: vec![later.memory_id],
                    expected_superseded_memory_ids: Vec::new(),
                    required_source_refs: Vec::new(),
                    previous_included_memory_ids: before,
                }],
            },
        )
        .expect("evaluate");
        assert_eq!(report.summary.cache_churn_records, 0);
        assert_eq!(report.summary.passed_cases, 1);
    }

    #[test]
    fn durable_store_records_retrieval_access_with_full_query_provenance() {
        let root = temp_memory_root("sinomo-memory-query-provenance");
        let store = DurableMemoryStore::new(&root);
        store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "Deployment recovery memory selected by task hint",
                Vec::new(),
                trust_local(),
                fixed_time(1),
            )
            .expect("record");
        let session_id = SessionId::from_legacy_named("sess_query_provenance").expect("session");

        store
            .retrieve(&MemoryQuery {
                session_id: Some(session_id.clone()),
                agent_id: Some("agent_a".to_string()),
                chat_id: Some("chat_a".to_string()),
                channel_id: Some("channel_a".to_string()),
                task_id: Some("task_a".to_string()),
                workspace_id: Some("workspace_a".to_string()),
                user_id: Some("user_a".to_string()),
                team_id: Some("team_a".to_string()),
                task_hint: Some("deployment recovery".to_string()),
                max_records: 8,
                max_bytes: 64 * 1024,
            })
            .expect("retrieve");

        let retrieval_access = store
            .access_records()
            .expect("access records")
            .into_iter()
            .find(|record| record.operation == MemoryAccessOperation::Retrieval)
            .expect("retrieval access");
        assert_source_ref(
            &retrieval_access.source_refs,
            "session",
            &session_id.to_string(),
        );
        assert_source_ref(&retrieval_access.source_refs, "agent", "agent_a");
        assert_source_ref(&retrieval_access.source_refs, "chat", "chat_a");
        assert_source_ref(&retrieval_access.source_refs, "channel", "channel_a");
        assert_source_ref(&retrieval_access.source_refs, "task", "task_a");
        assert_source_ref(&retrieval_access.source_refs, "workspace", "workspace_a");
        assert_source_ref(&retrieval_access.source_refs, "user", "user_a");
        assert_source_ref(&retrieval_access.source_refs, "team", "team_a");
        assert_source_ref(
            &retrieval_access.source_refs,
            "task_hint",
            "deployment recovery",
        );
    }

    #[test]
    fn durable_store_rejects_non_canonical_base_and_derived_ids() {
        let root = temp_memory_root("sinomo-memory-noncanonical");
        let store = DurableMemoryStore::new(&root);
        let mut bad_record = MemoryRecord::new(
            MemoryScope::Cluster,
            MemoryTier::Warm,
            "Non-canonical memory id",
            Vec::new(),
            trust_local(),
            fixed_time(1),
        );
        bad_record.memory_id = MemoryId::from_legacy_named("bad_memory_id").expect("id");
        assert!(matches!(
            store
                .write_record(bad_record)
                .expect_err("bad memory id should be rejected"),
            MemoryError::NonCanonicalMemoryId { .. }
        ));

        let first = store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "First canonical source",
                Vec::new(),
                trust_local(),
                fixed_time(2),
            )
            .expect("first");
        let second = store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "Second canonical source",
                Vec::new(),
                trust_local(),
                fixed_time(3),
            )
            .expect("second");

        let mut link = MemoryLinkRecord::new(
            first.memory_id.clone(),
            second.memory_id.clone(),
            MemoryLinkRelation::Related,
            Vec::new(),
            fixed_time(4),
        );
        link.link_id = Id::from_legacy_named("bad_link_id").expect("id");
        assert_noncanonical_derived(
            store
                .write_link(link)
                .expect_err("bad link id should be rejected"),
            "memory-link",
        );

        let mut supersession = MemorySupersessionRecord::new(
            first.memory_id.clone(),
            second.memory_id.clone(),
            "replacement",
            Vec::new(),
            fixed_time(5),
        );
        supersession.supersession_id = Id::from_legacy_named("bad_supersession_id").expect("id");
        assert_noncanonical_derived(
            store
                .write_supersession(supersession)
                .expect_err("bad supersession id should be rejected"),
            "memory-supersession",
        );

        let mut consolidation = MemoryConsolidationRecord::new(
            second.memory_id.clone(),
            vec![first.memory_id.clone(), second.memory_id.clone()],
            Vec::new(),
            fixed_time(6),
        );
        consolidation.consolidation_id = Id::from_legacy_named("bad_consolidation_id").expect("id");
        assert_noncanonical_derived(
            store
                .write_consolidation(consolidation)
                .expect_err("bad consolidation id should be rejected"),
            "memory-consolidation",
        );

        let mut job = MemoryConsolidationJobRecord::new(
            MemoryConsolidationJobKind::Reflection,
            MemoryScope::Cluster,
            MemoryTier::Cold,
            "Reflect on sources",
            vec![first.memory_id.clone()],
            Vec::new(),
            fixed_time(7),
        );
        job.job_id = Id::from_legacy_named("bad_job_id").expect("id");
        assert_noncanonical_derived(
            store
                .write_consolidation_job(job)
                .expect_err("bad job id should be rejected"),
            "memory-consolidation-job",
        );

        let job = store
            .schedule_consolidation_job(
                MemoryConsolidationJobKind::Reflection,
                MemoryScope::Cluster,
                MemoryTier::Cold,
                "Reflect on sources",
                vec![first.memory_id.clone()],
                Vec::new(),
                fixed_time(8),
            )
            .expect("job");
        let mut status = MemoryConsolidationJobStatusRecord::new(
            job.job_id,
            MemoryConsolidationJobStatus::Running,
            "running",
            None,
            Vec::new(),
            fixed_time(9),
        );
        status.status_id = Id::from_legacy_named("bad_status_id").expect("id");
        assert_noncanonical_derived(
            store
                .record_consolidation_job_status(status)
                .expect_err("bad status id should be rejected"),
            "memory-consolidation-job-status",
        );

        let mut access = MemoryAccessRecord::new(
            MemoryAccessOperation::ContextInclusion,
            vec![first.memory_id],
            "included",
            Vec::new(),
            fixed_time(10),
        );
        access.access_id = Id::from_legacy_named("bad_access_id").expect("id");
        assert_noncanonical_derived(
            store
                .write_access_record(access)
                .expect_err("bad access id should be rejected"),
            "memory-access",
        );
    }

    #[test]
    fn durable_store_duplicate_writes_are_idempotent() {
        let root = temp_memory_root("sinomo-memory-idempotent");
        let store = DurableMemoryStore::new(&root);
        let first = MemoryRecord::new(
            MemoryScope::Cluster,
            MemoryTier::Warm,
            "Idempotent first record",
            Vec::new(),
            trust_local(),
            fixed_time(1),
        );
        let second = MemoryRecord::new(
            MemoryScope::Cluster,
            MemoryTier::Warm,
            "Idempotent second record",
            Vec::new(),
            trust_local(),
            fixed_time(2),
        );
        store.write_record(first.clone()).expect("first write");
        store.write_record(first.clone()).expect("duplicate first");
        store.write_record(second.clone()).expect("second write");
        store
            .write_record(second.clone())
            .expect("duplicate second");

        let link = MemoryLinkRecord::new(
            first.memory_id.clone(),
            second.memory_id.clone(),
            MemoryLinkRelation::Related,
            Vec::new(),
            fixed_time(3),
        );
        store.write_link(link.clone()).expect("link");
        store.write_link(link.clone()).expect("duplicate link");

        let access = MemoryAccessRecord::new(
            MemoryAccessOperation::ContextInclusion,
            vec![first.memory_id.clone(), second.memory_id.clone()],
            "context inclusion",
            Vec::new(),
            fixed_time(4),
        );
        store
            .write_access_record(access.clone())
            .expect("access record");
        store
            .write_access_record(access.clone())
            .expect("duplicate access record");

        assert_eq!(
            store
                .records_for_scope(&MemoryScope::Cluster)
                .expect("records")
                .len(),
            2
        );
        assert_eq!(store.link_records().expect("links").len(), 1);
        assert_eq!(
            store
                .access_records()
                .expect("access")
                .iter()
                .filter(|record| record.access_id == access.access_id)
                .count(),
            1
        );
        let index = store
            .load_index()
            .expect("load index")
            .expect("index exists");
        assert_eq!(
            index
                .entries
                .iter()
                .filter(|entry| entry.memory_id == first.memory_id)
                .count(),
            1
        );
        assert_eq!(
            index
                .derived_entries
                .iter()
                .filter(|entry| entry.derived_id == link.link_id)
                .count(),
            1
        );
        assert_eq!(
            index
                .derived_entries
                .iter()
                .filter(|entry| entry.derived_id == access.access_id)
                .count(),
            1
        );
    }

    #[test]
    fn durable_store_replays_snapshot_plus_later_log_segments() {
        let root = temp_memory_root("sinomo-memory-snapshot-tail");
        let store = DurableMemoryStore::new(&root);
        let before_snapshot = store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "Record before compaction",
                Vec::new(),
                trust_local(),
                fixed_time(1),
            )
            .expect("before snapshot");
        store.compact().expect("compact");
        let after_snapshot = store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "Record after compaction",
                Vec::new(),
                trust_local(),
                fixed_time(2),
            )
            .expect("after snapshot");
        let link = store
            .write_link(MemoryLinkRecord::new(
                before_snapshot.memory_id.clone(),
                after_snapshot.memory_id.clone(),
                MemoryLinkRelation::Related,
                Vec::new(),
                fixed_time(3),
            ))
            .expect("link");

        let reopened = DurableMemoryStore::new(&root);
        let records = reopened
            .records_for_scope(&MemoryScope::Cluster)
            .expect("records");
        assert_eq!(records.len(), 2);
        assert_eq!(
            reopened.link_records().expect("links")[0].link_id,
            link.link_id
        );
        let index = reopened
            .load_index()
            .expect("load index")
            .expect("index exists");
        let before_entry = index
            .entries
            .iter()
            .find(|entry| entry.memory_id == before_snapshot.memory_id)
            .expect("before entry");
        assert!(before_entry.path.starts_with("records/snapshot.sobj#"));
        let after_entry = index
            .entries
            .iter()
            .find(|entry| entry.memory_id == after_snapshot.memory_id)
            .expect("after entry");
        assert!(after_entry.path.starts_with("records/segments/"));
        assert!(index.derived_entries.iter().any(|entry| {
            entry.derived_id == link.link_id && entry.path.starts_with("records/segments/")
        }));
    }

    #[test]
    fn durable_store_latest_supersession_wins_and_chains_filter_cleanly() {
        let root = temp_memory_root("sinomo-memory-supersession-chain");
        let store = DurableMemoryStore::new(&root);
        let original = store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "Original recovery note",
                Vec::new(),
                trust_local(),
                fixed_time(1),
            )
            .expect("original");
        let intermediate = store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "Intermediate recovery note",
                Vec::new(),
                trust_local(),
                fixed_time(2),
            )
            .expect("intermediate");
        let latest = store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "Latest recovery note",
                Vec::new(),
                trust_local(),
                fixed_time(3),
            )
            .expect("latest");
        store
            .supersede_record(
                original.memory_id.clone(),
                intermediate.memory_id.clone(),
                "intermediate replaced original",
                Vec::new(),
                fixed_time(4),
            )
            .expect("original to intermediate");
        let original_to_latest = store
            .supersede_record(
                original.memory_id.clone(),
                latest.memory_id.clone(),
                "latest replaced original",
                Vec::new(),
                fixed_time(5),
            )
            .expect("original to latest");
        store
            .supersede_record(
                intermediate.memory_id.clone(),
                latest.memory_id.clone(),
                "latest replaced intermediate",
                Vec::new(),
                fixed_time(6),
            )
            .expect("intermediate to latest");

        let result = store.retrieve(&MemoryQuery::default()).expect("retrieve");

        assert_eq!(selected_memory_ids(&result), vec![latest.memory_id.clone()]);
        assert!(
            !result
                .records
                .iter()
                .any(|record| record.record.memory_id == original.memory_id)
        );
        assert!(
            !result
                .records
                .iter()
                .any(|record| record.record.memory_id == intermediate.memory_id)
        );
        assert!(result.report.superseded.iter().any(|trace| {
            trace.memory_id == original.memory_id
                && trace.superseded_by == latest.memory_id
                && trace.supersession_id == original_to_latest.supersession_id
        }));
        assert!(result.report.superseded.iter().any(|trace| {
            trace.memory_id == intermediate.memory_id && trace.superseded_by == latest.memory_id
        }));
    }

    #[test]
    fn durable_store_consolidation_does_not_grant_source_scope_visibility() {
        let root = temp_memory_root("sinomo-memory-consolidation-visibility");
        let store = DurableMemoryStore::new(&root);
        let private_source = store
            .put_record(
                MemoryScope::Agent {
                    agent_id: "agent_private".to_string(),
                },
                MemoryTier::Warm,
                "Agent-private lesson source",
                Vec::new(),
                trust_local(),
                fixed_time(1),
            )
            .expect("private source");
        let (task_summary, consolidation) = store
            .consolidate_records(
                MemoryScope::Task {
                    task_id: "task_shared".to_string(),
                },
                MemoryTier::Cold,
                "Task-visible derived lesson",
                vec![private_source.memory_id.clone()],
                vec![MemorySourceRef::new("task", "task_shared")],
                trust_local(),
                fixed_time(2),
            )
            .expect("consolidation");

        let result = store
            .retrieve(&MemoryQuery {
                task_id: Some("task_shared".to_string()),
                max_records: 8,
                max_bytes: 64 * 1024,
                ..MemoryQuery::default()
            })
            .expect("retrieve");

        assert_eq!(selected_memory_ids(&result), vec![task_summary.memory_id]);
        assert!(result.report.filtered.iter().any(|entry| {
            entry.memory_id == private_source.memory_id
                && entry.reason == "scope not visible for query"
        }));
        assert_source_ref(
            &result.records[0].record.source_refs,
            "memory",
            &private_source.memory_id.to_string(),
        );
        assert_eq!(
            consolidation.source_memory_ids,
            vec![private_source.memory_id]
        );
    }

    #[test]
    fn durable_store_rebuilds_indexes_from_snapshot_plus_tail() {
        let root = temp_memory_root("sinomo-memory-index-snapshot-tail");
        let store = DurableMemoryStore::new(&root);
        let first = store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "alpha index source",
                Vec::new(),
                trust_local(),
                fixed_time(1),
            )
            .expect("first");
        let second = store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "beta index source",
                Vec::new(),
                trust_local(),
                fixed_time(2),
            )
            .expect("second");
        let compacted_link = store
            .write_link(MemoryLinkRecord::new(
                first.memory_id.clone(),
                second.memory_id.clone(),
                MemoryLinkRelation::Related,
                Vec::new(),
                fixed_time(3),
            ))
            .expect("compacted link");
        store.compact().expect("compact");
        let third = store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "gamma index source",
                Vec::new(),
                trust_local(),
                fixed_time(4),
            )
            .expect("third");
        let tail_link = store
            .write_link(MemoryLinkRecord::new(
                second.memory_id.clone(),
                third.memory_id.clone(),
                MemoryLinkRelation::Related,
                Vec::new(),
                fixed_time(5),
            ))
            .expect("tail link");
        let tail_supersession = store
            .supersede_record(
                first.memory_id.clone(),
                third.memory_id.clone(),
                "gamma replaces alpha",
                Vec::new(),
                fixed_time(6),
            )
            .expect("tail supersession");

        let embedding_index = store
            .rebuild_embedding_index(&TokenHashEmbeddingIndexProvider::default(), fixed_time(7))
            .expect("embedding index");
        assert_eq!(embedding_index.embeddings.len(), 3);
        for memory_id in [&first.memory_id, &second.memory_id, &third.memory_id] {
            assert!(
                embedding_index
                    .embeddings
                    .iter()
                    .any(|entry| &entry.memory_id == memory_id),
                "missing embedding for {memory_id}"
            );
        }

        let graph_index = store
            .rebuild_graph_index(&LocalMemoryGraphIndexProvider)
            .expect("graph index");
        assert_eq!(graph_index.nodes.len(), 3);
        assert!(graph_index.edges.iter().any(|edge| {
            edge.derived_id == compacted_link.link_id
                && edge.from_memory_id == first.memory_id
                && edge.to_memory_id == second.memory_id
        }));
        assert!(graph_index.edges.iter().any(|edge| {
            edge.derived_id == tail_link.link_id
                && edge.from_memory_id == second.memory_id
                && edge.to_memory_id == third.memory_id
        }));
        assert!(graph_index.edges.iter().any(|edge| {
            edge.derived_id == tail_supersession.supersession_id
                && edge.from_memory_id == first.memory_id
                && edge.to_memory_id == third.memory_id
                && edge.relation == "superseded-by"
        }));
    }

    #[test]
    fn durable_store_plans_scope_partitioned_sifs_replication() {
        let root = temp_memory_root("sinomo-memory-replication-plan");
        let store = DurableMemoryStore::new(&root);
        let _cluster = store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "Cluster incident lesson",
                Vec::new(),
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("cluster record");
        let _user = store
            .put_record(
                MemoryScope::User {
                    user_id: "user_a".to_string(),
                },
                MemoryTier::Warm,
                "User prefers concise status updates",
                Vec::new(),
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("user record");
        let _agent = store
            .put_record(
                MemoryScope::Agent {
                    agent_id: "agent_private".to_string(),
                },
                MemoryTier::Warm,
                "Agent-local scratch lesson",
                Vec::new(),
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("agent record");

        let policy = MemoryReplicationPolicy::from_scope_policies(vec![
            MemoryScopeReplicationPolicy::new(
                MemoryScope::Cluster,
                MemoryReplicationMode::SnapshotAndIndex,
                MemoryReplicationConsistency::Confirmed,
            ),
            MemoryScopeReplicationPolicy::new(
                MemoryScope::User {
                    user_id: "user_a".to_string(),
                },
                MemoryReplicationMode::RecordsAndIndex,
                MemoryReplicationConsistency::Confirmed,
            )
            .with_target_node_ids(vec!["node_a".to_string(), "node_b".to_string()]),
            MemoryScopeReplicationPolicy::new(
                MemoryScope::Agent {
                    agent_id: "agent_private".to_string(),
                },
                MemoryReplicationMode::None,
                MemoryReplicationConsistency::LocalOnly,
            ),
        ]);

        let plan = store
            .plan_sifs_replication(&policy)
            .expect("replication plan");

        assert_eq!(plan.entries.len(), 4);
        assert!(plan.entries.iter().any(|entry| {
            entry.scope == MemoryScope::Cluster
                && entry.artifact == MemoryReplicationArtifactKind::ScopeSnapshot
                && entry.sifs_path == "/services/sinomo/memory/cluster/snapshot.sobj"
                && entry.record_count == 1
                && entry.min_replicas == 2
        }));
        assert!(plan.entries.iter().any(|entry| {
            entry.scope
                == MemoryScope::User {
                    user_id: "user_a".to_string(),
                }
                && entry.artifact == MemoryReplicationArtifactKind::ScopeRecords
                && entry.sifs_path == "/services/sinomo/memory/users/user_a/records.sobjlog"
                && entry.source_ref == "memory-export://users/user_a/records.sobjlog"
                && entry.target_node_ids == vec!["node_a".to_string(), "node_b".to_string()]
        }));
        assert!(
            !plan
                .entries
                .iter()
                .any(|entry| matches!(entry.scope, MemoryScope::Agent { .. }))
        );
    }

    #[test]
    fn durable_store_sanitizes_replication_paths_and_skips_empty_scopes() {
        let root = temp_memory_root("sinomo-memory-replication-sanitize");
        let store = DurableMemoryStore::new(&root);
        store
            .put_record(
                MemoryScope::Chat {
                    chat_id: "room/a b".to_string(),
                },
                MemoryTier::Warm,
                "Chat decision to remember",
                Vec::new(),
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("chat record");

        let plan = store
            .plan_sifs_replication(&MemoryReplicationPolicy::from_scope_policies(vec![
                MemoryScopeReplicationPolicy::new(
                    MemoryScope::Chat {
                        chat_id: "room/a b".to_string(),
                    },
                    MemoryReplicationMode::Snapshot,
                    MemoryReplicationConsistency::Eventual,
                ),
                MemoryScopeReplicationPolicy::new(
                    MemoryScope::Workspace {
                        workspace_id: "missing/workspace".to_string(),
                    },
                    MemoryReplicationMode::Snapshot,
                    MemoryReplicationConsistency::Eventual,
                ),
            ]))
            .expect("replication plan");

        assert_eq!(plan.entries.len(), 1);
        assert_eq!(
            plan.entries[0].sifs_path,
            "/services/sinomo/memory/chats/room_a_b/snapshot.sobj"
        );
        assert_eq!(
            plan.entries[0].source_ref,
            "memory-export://chats/room_a_b/snapshot.sobj"
        );
    }

    #[test]
    fn memory_evaluation_reports_recall_leakage_supersession_provenance_and_cache_churn() {
        let root = temp_memory_root("sinomo-memory-eval-pass");
        let store = DurableMemoryStore::new(&root);
        let relevant = store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "Relevant task recovery memory",
                vec![MemorySourceRef::new("event", "evt_relevant")],
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("relevant");
        let old = store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "Old task recovery memory",
                vec![MemorySourceRef::new("event", "evt_old")],
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("old");
        let replacement = store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "Replacement task recovery memory",
                vec![MemorySourceRef::new("event", "evt_replacement")],
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("replacement");
        store
            .put_record(
                MemoryScope::Agent {
                    agent_id: "other-agent".to_string(),
                },
                MemoryTier::Warm,
                "Private memory for another agent",
                Vec::new(),
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("forbidden");
        store
            .supersede_record(
                old.memory_id.clone(),
                replacement.memory_id.clone(),
                "replacement is newer",
                vec![MemorySourceRef::new("event", "evt_supersede")],
                OffsetDateTime::now_utc(),
            )
            .expect("supersede");

        let query = MemoryQuery {
            agent_id: Some("main-agent".to_string()),
            max_records: 8,
            max_bytes: 64 * 1024,
            ..MemoryQuery::default()
        };
        let previous_included_memory_ids = store
            .retrieve(&query)
            .expect("previous retrieval")
            .records
            .iter()
            .map(|record| record.record.memory_id.clone())
            .collect::<Vec<_>>();
        let suite = MemoryEvaluationSuite {
            cases: vec![MemoryEvaluationCase {
                name: "task recovery retrieval".to_string(),
                query,
                expected_memory_ids: previous_included_memory_ids.clone(),
                forbidden_memory_ids: vec![
                    MemoryId::from_legacy_named("missing_forbidden").expect("id"),
                ],
                expected_superseded_memory_ids: vec![old.memory_id.clone()],
                required_source_refs: vec![MemorySourceRef::new("event", "evt_relevant")],
                previous_included_memory_ids,
            }],
        };

        let report = evaluate_memory_retrieval(&store, &suite).expect("evaluate");

        assert_eq!(report.summary.total_cases, 1);
        assert_eq!(report.summary.passed_cases, 1);
        assert_eq!(report.summary.expected_records, 2);
        assert_eq!(report.summary.recalled_records, 2);
        assert_eq!(report.summary.forbidden_selected_records, 0);
        assert_eq!(report.summary.missing_superseded_records, 0);
        assert_eq!(report.summary.missing_source_refs, 0);
        assert_eq!(report.summary.cache_churn_records, 0);
        assert!(report.cases[0].passed());
        assert!(
            report.cases[0]
                .selected_memory_ids
                .contains(&relevant.memory_id)
        );
        assert!(
            report.cases[0]
                .selected_memory_ids
                .contains(&replacement.memory_id)
        );
    }

    #[test]
    fn memory_evaluation_detects_missing_expected_and_forbidden_selected_records() {
        let root = temp_memory_root("sinomo-memory-eval-fail");
        let store = DurableMemoryStore::new(&root);
        let forbidden = store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "Visible record that should be forbidden by the case",
                Vec::new(),
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("forbidden");
        let missing = MemoryId::from_legacy_named("missing_expected_memory").expect("id");
        let suite = MemoryEvaluationSuite {
            cases: vec![MemoryEvaluationCase {
                name: "failing retrieval".to_string(),
                query: MemoryQuery {
                    max_records: 8,
                    max_bytes: 64 * 1024,
                    ..MemoryQuery::default()
                },
                expected_memory_ids: vec![missing.clone()],
                forbidden_memory_ids: vec![forbidden.memory_id.clone()],
                expected_superseded_memory_ids: Vec::new(),
                required_source_refs: vec![MemorySourceRef::new("event", "missing_event")],
                previous_included_memory_ids: Vec::new(),
            }],
        };

        let report = evaluate_memory_retrieval(&store, &suite).expect("evaluate");

        assert_eq!(report.summary.total_cases, 1);
        assert_eq!(report.summary.passed_cases, 0);
        assert_eq!(report.summary.expected_records, 1);
        assert_eq!(report.summary.recalled_records, 0);
        assert_eq!(report.summary.forbidden_selected_records, 1);
        assert_eq!(report.summary.missing_source_refs, 1);
        assert_eq!(report.cases[0].missing_expected_memory_ids, vec![missing]);
        assert_eq!(
            report.cases[0].unexpected_forbidden_memory_ids,
            vec![forbidden.memory_id]
        );
        assert!(!report.cases[0].passed());
    }

    #[test]
    fn token_hash_embedding_provider_rebuilds_and_searches_from_memory_records() {
        let root = temp_memory_root("sinomo-memory-embedding-index");
        let store = DurableMemoryStore::new(&root);
        let relevant = store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "alpha beta deployment recovery",
                Vec::new(),
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("relevant");
        store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "theta lambda unrelated note",
                Vec::new(),
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("unrelated");
        let provider = TokenHashEmbeddingIndexProvider::new("test", "token-hash-test", 128);
        let index = store
            .rebuild_embedding_index(&provider, OffsetDateTime::now_utc())
            .expect("embedding index");

        assert_eq!(index.embeddings.len(), 2);
        assert_eq!(index.dimensions, 128);
        assert!(index.embeddings.iter().all(|embedding| {
            embedding.provider == "test"
                && embedding.model == "token-hash-test"
                && embedding.vector.len() == 128
        }));

        let results = provider.search_embedding_index(
            &index,
            &MemoryQuery {
                task_hint: Some("alpha beta recovery".to_string()),
                max_records: 8,
                max_bytes: 64 * 1024,
                ..MemoryQuery::default()
            },
            1,
        );

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].memory_id, relevant.memory_id);
        assert!(results[0].score > 0);
    }

    #[test]
    fn local_graph_provider_rebuilds_edges_from_links_supersessions_and_consolidations() {
        let root = temp_memory_root("sinomo-memory-graph-index");
        let store = DurableMemoryStore::new(&root);
        let first = store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "First graph memory",
                Vec::new(),
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("first");
        let second = store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "Second graph memory",
                Vec::new(),
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("second");
        let replacement = store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "Replacement graph memory",
                Vec::new(),
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("replacement");
        let link = store
            .write_link(MemoryLinkRecord::new(
                first.memory_id.clone(),
                second.memory_id.clone(),
                MemoryLinkRelation::Related,
                Vec::new(),
                OffsetDateTime::now_utc(),
            ))
            .expect("link");
        let supersession = store
            .supersede_record(
                first.memory_id.clone(),
                replacement.memory_id.clone(),
                "replacement",
                Vec::new(),
                OffsetDateTime::now_utc(),
            )
            .expect("supersession");
        let (_summary, consolidation) = store
            .consolidate_records(
                MemoryScope::Cluster,
                MemoryTier::Cold,
                "Graph consolidation",
                vec![first.memory_id.clone(), second.memory_id.clone()],
                Vec::new(),
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("consolidation");
        let provider = LocalMemoryGraphIndexProvider;
        let index = store.rebuild_graph_index(&provider).expect("graph index");

        assert_eq!(index.nodes.len(), 4);
        assert!(index.edges.iter().any(|edge| {
            edge.derived_id == link.link_id
                && edge.from_memory_id == first.memory_id
                && edge.to_memory_id == second.memory_id
                && edge.relation == "related"
        }));
        assert!(index.edges.iter().any(|edge| {
            edge.derived_id == supersession.supersession_id
                && edge.from_memory_id == first.memory_id
                && edge.to_memory_id == replacement.memory_id
                && edge.relation == "superseded-by"
        }));
        assert!(index.edges.iter().any(|edge| {
            edge.derived_id == consolidation.consolidation_id
                && edge.from_memory_id == first.memory_id
                && edge.to_memory_id == consolidation.consolidated_memory_id
                && edge.relation == "consolidated-into"
        }));
        let related = provider.related_memory_ids(&index, &first.memory_id);
        assert!(related.contains(&second.memory_id));
        assert!(related.contains(&replacement.memory_id));
        assert!(related.contains(&consolidation.consolidated_memory_id));
    }

    #[test]
    fn durable_store_supersedes_records_and_filters_retrieval() {
        let root = temp_memory_root("sinomo-memory-supersession");
        let store = DurableMemoryStore::new(&root);
        let old = store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "Old deployment recovery note",
                vec![MemorySourceRef::new("event", "evt_old")],
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("old record");
        let replacement = store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "Replacement deployment recovery note",
                vec![MemorySourceRef::new("event", "evt_new")],
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("replacement record");
        let supersession = store
            .supersede_record(
                old.memory_id.clone(),
                replacement.memory_id.clone(),
                "newer evidence replaced the old deployment note",
                vec![MemorySourceRef::new("event", "evt_supersede")],
                OffsetDateTime::now_utc(),
            )
            .expect("supersession");

        let result = store.retrieve(&MemoryQuery::default()).expect("retrieve");
        assert_eq!(result.records.len(), 1);
        assert_eq!(result.records[0].record.memory_id, replacement.memory_id);
        assert!(
            !result
                .records
                .iter()
                .any(|record| record.record.memory_id == old.memory_id)
        );
        assert_eq!(result.report.superseded.len(), 1);
        assert_eq!(result.report.superseded[0].memory_id, old.memory_id);
        assert_eq!(
            result.report.superseded[0].superseded_by,
            replacement.memory_id
        );
        assert_eq!(
            result.report.superseded[0].supersession_id,
            supersession.supersession_id
        );

        let reopened = DurableMemoryStore::new(&root);
        assert_eq!(
            reopened
                .supersession_records()
                .expect("supersessions")
                .len(),
            1
        );
        let reopened_result = reopened
            .retrieve(&MemoryQuery::default())
            .expect("reopened retrieve");
        assert_eq!(reopened_result.records.len(), 1);
        assert_eq!(
            reopened_result.records[0].record.memory_id,
            replacement.memory_id
        );
    }

    #[test]
    fn durable_store_consolidates_source_records_with_provenance() {
        let root = temp_memory_root("sinomo-memory-consolidate");
        let store = DurableMemoryStore::new(&root);
        let first = store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "First task outcome",
                vec![MemorySourceRef::new("event", "evt_first")],
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("first record");
        let second = store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "Second task outcome",
                vec![MemorySourceRef::new("event", "evt_second")],
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("second record");

        let (record, consolidation) = store
            .consolidate_records(
                MemoryScope::Cluster,
                MemoryTier::Cold,
                "Consolidated task outcome summary",
                vec![first.memory_id.clone(), second.memory_id.clone()],
                vec![MemorySourceRef::new("event", "evt_consolidate")],
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("consolidate");

        assert_eq!(consolidation.consolidated_memory_id, record.memory_id);
        assert_eq!(
            consolidation.source_memory_ids,
            vec![first.memory_id.clone(), second.memory_id.clone()]
        );
        assert!(
            record.source_refs.iter().any(
                |source| source.kind == "memory" && source.value == first.memory_id.to_string()
            )
        );
        assert!(
            record
                .source_refs
                .iter()
                .any(|source| source.kind == "memory"
                    && source.value == second.memory_id.to_string())
        );

        let reopened = DurableMemoryStore::new(&root);
        assert_eq!(
            reopened.consolidation_records().expect("consolidations")[0].consolidation_id,
            consolidation.consolidation_id
        );
        assert!(
            reopened
                .access_records()
                .expect("access")
                .iter()
                .any(
                    |access| access.operation == MemoryAccessOperation::Consolidation
                        && access.memory_ids == vec![record.memory_id.clone()]
                )
        );
    }

    #[test]
    fn durable_store_schedules_completes_replays_and_compacts_consolidation_jobs() {
        let root = temp_memory_root("sinomo-memory-consolidation-job");
        let store = DurableMemoryStore::new(&root);
        let source = store
            .put_record(
                MemoryScope::Task {
                    task_id: "task_memory_job".to_string(),
                },
                MemoryTier::Warm,
                "Task result to consolidate",
                vec![MemorySourceRef::new("task", "task_memory_job")],
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("source record");
        let job = store
            .schedule_consolidation_job(
                MemoryConsolidationJobKind::TaskOutcome,
                MemoryScope::Task {
                    task_id: "task_memory_job".to_string(),
                },
                MemoryTier::Cold,
                "Summarize task outcome",
                vec![source.memory_id.clone()],
                vec![MemorySourceRef::new("task", "task_memory_job")],
                OffsetDateTime::now_utc(),
            )
            .expect("schedule job");

        let statuses = store
            .consolidation_job_status_records()
            .expect("job statuses");
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].status, MemoryConsolidationJobStatus::Scheduled);
        assert_eq!(
            store.pending_consolidation_jobs().expect("pending"),
            vec![job.clone()]
        );

        let reopened = DurableMemoryStore::new(&root);
        assert_eq!(
            reopened.consolidation_job_records().expect("jobs")[0].job_id,
            job.job_id
        );
        assert_eq!(
            reopened.pending_consolidation_jobs().expect("pending"),
            vec![job.clone()]
        );

        let (_summary, consolidation) = reopened
            .consolidate_records(
                job.target_scope.clone(),
                job.target_tier.clone(),
                "Task outcome summary",
                job.source_memory_ids.clone(),
                job.source_refs.clone(),
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("consolidate");
        let completed = reopened
            .complete_consolidation_job(
                job.job_id.clone(),
                consolidation.consolidation_id.clone(),
                "job completed",
                vec![MemorySourceRef::new("task", "task_memory_job")],
                OffsetDateTime::now_utc(),
            )
            .expect("complete job");
        assert_eq!(completed.status, MemoryConsolidationJobStatus::Completed);
        assert_eq!(
            reopened.pending_consolidation_jobs().expect("pending"),
            Vec::new()
        );
        let index = reopened
            .load_index()
            .expect("load index")
            .expect("index exists");
        assert!(index.derived_entries.iter().any(|entry| {
            entry.kind == "memory-consolidation-job" && entry.derived_id == job.job_id
        }));
        assert!(index.derived_entries.iter().any(|entry| {
            entry.kind == "memory-consolidation-job-status"
                && entry.derived_id == completed.status_id
        }));

        let snapshot = reopened.compact().expect("compact");
        assert_eq!(snapshot.consolidation_jobs.len(), 1);
        assert_eq!(snapshot.consolidation_job_statuses.len(), 2);

        let compacted = DurableMemoryStore::new(&root);
        assert_eq!(
            compacted
                .consolidation_job_status_records()
                .expect("statuses")
                .iter()
                .filter(|status| status.job_id == job.job_id)
                .count(),
            2
        );
        assert_eq!(
            compacted.pending_consolidation_jobs().expect("pending"),
            Vec::new()
        );
    }

    #[test]
    fn durable_store_runs_next_consolidation_job_with_deterministic_summary() {
        let root = temp_memory_root("sinomo-memory-run-consolidation-job");
        let store = DurableMemoryStore::new(&root);
        let first = store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "First incident lesson",
                vec![MemorySourceRef::new("event", "evt_incident_first")],
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("first source");
        let second = store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "Second incident lesson",
                vec![MemorySourceRef::new("event", "evt_incident_second")],
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("second source");
        let job = store
            .schedule_consolidation_job(
                MemoryConsolidationJobKind::IncidentLearning,
                MemoryScope::Cluster,
                MemoryTier::Cold,
                "Create an incident lesson",
                vec![first.memory_id.clone(), second.memory_id.clone()],
                vec![MemorySourceRef::new("event", "evt_incident_job")],
                OffsetDateTime::now_utc(),
            )
            .expect("schedule");

        let run = store
            .run_next_consolidation_job(
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("run")
            .expect("job run");

        assert_eq!(run.job.job_id, job.job_id);
        assert_eq!(
            run.running_status.status,
            MemoryConsolidationJobStatus::Running
        );
        assert_eq!(
            run.final_status.status,
            MemoryConsolidationJobStatus::Completed
        );
        let consolidation = run.consolidation.expect("consolidation");
        let records = store
            .records_for_scope(&MemoryScope::Cluster)
            .expect("cluster records");
        let consolidated = records
            .iter()
            .find(|record| record.memory_id == consolidation.consolidated_memory_id)
            .expect("consolidated record");
        assert!(consolidated.summary.contains("incident learning"));
        assert!(consolidated.summary.contains("First incident lesson"));
        assert!(consolidated.summary.contains("Second incident lesson"));
        assert_eq!(
            store.pending_consolidation_jobs().expect("pending"),
            Vec::new()
        );
        assert!(
            store
                .run_next_consolidation_job(
                    TrustDomain::new("trust.local").expect("trust"),
                    OffsetDateTime::now_utc(),
                )
                .expect("run")
                .is_none()
        );
    }

    #[test]
    fn durable_store_marks_source_ref_only_consolidation_job_failed_for_deterministic_worker() {
        let root = temp_memory_root("sinomo-memory-run-consolidation-job-failed");
        let store = DurableMemoryStore::new(&root);
        let job = store
            .schedule_consolidation_job(
                MemoryConsolidationJobKind::SessionBoundaryRecap,
                MemoryScope::Chat {
                    chat_id: "chat_source_ref_only".to_string(),
                },
                MemoryTier::Cold,
                "Summarize source refs later",
                Vec::new(),
                vec![MemorySourceRef::new("session", "sess_source_ref_only")],
                OffsetDateTime::now_utc(),
            )
            .expect("schedule");

        let run = store
            .run_next_consolidation_job(
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("run")
            .expect("job run");

        assert_eq!(run.job.job_id, job.job_id);
        assert!(run.consolidation.is_none());
        assert_eq!(
            run.final_status.status,
            MemoryConsolidationJobStatus::Failed
        );
        assert!(run.final_status.reason.contains("source memory records"));
        assert_eq!(
            store.pending_consolidation_jobs().expect("pending"),
            Vec::new()
        );
    }

    #[test]
    fn durable_store_rejects_invalid_consolidation_jobs_and_statuses() {
        let root = temp_memory_root("sinomo-memory-invalid-consolidation-job");
        let store = DurableMemoryStore::new(&root);
        let empty_error = store
            .schedule_consolidation_job(
                MemoryConsolidationJobKind::SessionBoundaryRecap,
                MemoryScope::Cluster,
                MemoryTier::Cold,
                "Empty job",
                Vec::new(),
                Vec::new(),
                OffsetDateTime::now_utc(),
            )
            .expect_err("empty job should be rejected");
        assert!(matches!(
            empty_error,
            MemoryError::EmptyConsolidationJobSources
        ));

        let missing_job_error = store
            .record_consolidation_job_status(MemoryConsolidationJobStatusRecord::new(
                MemoryConsolidationJobId::from_legacy_named("missing_job").expect("job id"),
                MemoryConsolidationJobStatus::Running,
                "missing job",
                None,
                Vec::new(),
                OffsetDateTime::now_utc(),
            ))
            .expect_err("missing job should be rejected");
        assert!(matches!(
            missing_job_error,
            MemoryError::MissingConsolidationJob { .. }
        ));
    }

    #[test]
    fn durable_store_replays_compacts_and_indexes_derived_records() {
        let root = temp_memory_root("sinomo-memory-derived");
        let store = DurableMemoryStore::new(&root);
        let first = store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "First related memory",
                Vec::new(),
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("first record");
        let second = store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "Second related memory",
                Vec::new(),
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("second record");
        let link = store
            .write_link(MemoryLinkRecord::new(
                first.memory_id.clone(),
                second.memory_id.clone(),
                MemoryLinkRelation::Related,
                vec![MemorySourceRef::new("event", "evt_link")],
                OffsetDateTime::now_utc(),
            ))
            .expect("link");
        let consolidation = store
            .write_consolidation(MemoryConsolidationRecord::new(
                second.memory_id.clone(),
                vec![first.memory_id.clone(), second.memory_id.clone()],
                vec![MemorySourceRef::new("event", "evt_consolidation")],
                OffsetDateTime::now_utc(),
            ))
            .expect("consolidation");
        let access = store
            .record_access(
                MemoryAccessOperation::Retrieval,
                vec![first.memory_id.clone(), second.memory_id.clone()],
                "retrieved for context assembly",
                vec![MemorySourceRef::new("session", "sess_memory")],
                OffsetDateTime::now_utc(),
            )
            .expect("access");

        let reopened = DurableMemoryStore::new(&root);
        assert_eq!(
            reopened.link_records().expect("links")[0].link_id,
            link.link_id
        );
        assert_eq!(
            reopened.consolidation_records().expect("consolidations")[0].consolidation_id,
            consolidation.consolidation_id
        );
        assert_eq!(
            reopened
                .access_records()
                .expect("access")
                .iter()
                .filter(|record| record.access_id == access.access_id)
                .count(),
            1
        );
        let index = reopened
            .load_index()
            .expect("load index")
            .expect("index exists");
        assert!(
            index
                .derived_entries
                .iter()
                .any(|entry| entry.kind == "memory-link" && entry.derived_id == link.link_id)
        );
        assert!(
            index
                .derived_entries
                .iter()
                .any(|entry| entry.kind == "memory-access" && entry.derived_id == access.access_id)
        );

        let snapshot = reopened.compact().expect("compact");
        assert_eq!(snapshot.links.len(), 1);
        assert_eq!(snapshot.consolidations.len(), 1);
        assert_eq!(snapshot.access_records.len(), 3);

        let compacted = DurableMemoryStore::new(&root);
        assert_eq!(
            compacted.link_records().expect("links")[0].link_id,
            link.link_id
        );
        assert_eq!(
            compacted
                .access_records()
                .expect("access")
                .iter()
                .filter(|record| record.access_id == access.access_id)
                .count(),
            1
        );
    }

    #[test]
    fn durable_store_rejects_derived_records_with_missing_base_memory() {
        let root = temp_memory_root("sinomo-memory-missing-derived");
        let store = DurableMemoryStore::new(&root);
        let missing_a = MemoryId::from_legacy_named("missing_a").expect("id");
        let missing_b = MemoryId::from_legacy_named("missing_b").expect("id");

        let error = store
            .write_link(MemoryLinkRecord::new(
                missing_a,
                missing_b,
                MemoryLinkRelation::Related,
                Vec::new(),
                OffsetDateTime::now_utc(),
            ))
            .expect_err("missing memories should be rejected");

        assert!(matches!(
            error,
            MemoryError::MissingMemoryRecord {
                kind: "memory-link",
                ..
            }
        ));
    }

    #[test]
    fn durable_store_rejects_empty_consolidation_sources() {
        let root = temp_memory_root("sinomo-memory-empty-consolidation");
        let store = DurableMemoryStore::new(&root);
        let error = store
            .consolidate_records(
                MemoryScope::Cluster,
                MemoryTier::Cold,
                "Empty consolidation",
                Vec::new(),
                Vec::new(),
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect_err("empty consolidation should be rejected");

        assert!(matches!(error, MemoryError::EmptyConsolidationSources));
    }

    #[test]
    fn durable_store_replays_records_after_restart() {
        let root = temp_memory_root("sinomo-memory-replay");
        let store = DurableMemoryStore::new(&root);
        let record = store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "Use durable replay for memory recovery",
                vec![MemorySourceRef::new("session", "sess_memory")],
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("write");

        let reopened = DurableMemoryStore::new(&root);
        let records = reopened
            .records_for_scope(&MemoryScope::Cluster)
            .expect("records");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].memory_id, record.memory_id);
    }

    #[test]
    fn durable_store_rotates_log_segments_and_replays_all_records() {
        let root = temp_memory_root("sinomo-memory-segments");
        let store = DurableMemoryStore::new(&root).with_records_per_segment(1);
        store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "First segmented memory record",
                Vec::new(),
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("first");
        store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "Second segmented memory record",
                Vec::new(),
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("second");

        assert!(
            root.join("records")
                .join("segments")
                .join("0000000000000001.sobjlog")
                .exists()
        );
        assert!(
            root.join("records")
                .join("segments")
                .join("0000000000000002.sobjlog")
                .exists()
        );

        let reopened = DurableMemoryStore::new(&root).with_records_per_segment(1);
        assert_eq!(
            reopened
                .records_for_scope(&MemoryScope::Cluster)
                .expect("records")
                .len(),
            2
        );
    }

    #[test]
    fn durable_store_compacts_to_snapshot_and_clears_segments() {
        let root = temp_memory_root("sinomo-memory-compact");
        let store = DurableMemoryStore::new(&root);
        store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "Compact memory snapshots from log records",
                Vec::new(),
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("write");

        let snapshot = store.compact().expect("compact");
        assert_eq!(snapshot.records.len(), 1);
        assert_eq!(snapshot.last_sequence, 2);
        assert_eq!(snapshot.access_records.len(), 1);
        assert!(
            root.join("records").join("snapshot.sobj").exists(),
            "snapshot should be written under records/"
        );
        assert!(
            !root
                .join("records")
                .join("segments")
                .join("0000000000000001.sobjlog")
                .exists(),
            "compaction should remove replayed segment files"
        );
        let reopened = DurableMemoryStore::new(&root);
        assert_eq!(
            reopened
                .records_for_scope(&MemoryScope::Cluster)
                .expect("records")
                .len(),
            1
        );
    }

    #[test]
    fn durable_store_ignores_trailing_partial_log_record() {
        let root = temp_memory_root("sinomo-memory-partial-log");
        let store = DurableMemoryStore::new(&root);
        let record = store
            .put_record(
                MemoryScope::Cluster,
                MemoryTier::Warm,
                "Ignore partial tail records",
                Vec::new(),
                TrustDomain::new("trust.local").expect("trust"),
                OffsetDateTime::now_utc(),
            )
            .expect("write");
        {
            use std::io::Write as _;
            let mut log = fs::OpenOptions::new()
                .append(true)
                .open(
                    root.join("records")
                        .join("segments")
                        .join("0000000000000001.sobjlog"),
                )
                .expect("open log");
            log.write_all(&16_u32.to_be_bytes()).expect("length");
            log.write_all(b"partial").expect("partial bytes");
        }

        let reopened = DurableMemoryStore::new(&root);
        let records = reopened
            .records_for_scope(&MemoryScope::Cluster)
            .expect("records");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].memory_id, record.memory_id);
    }
}
