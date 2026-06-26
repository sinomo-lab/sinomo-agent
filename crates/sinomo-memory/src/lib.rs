//! Hot memory, immutable memory records, and memory context retrieval.

use serde::{Deserialize, Serialize};
use sinomo_runtime::core::{
    ContextPacket, EventEnvelope, MemoryId, MemoryRecord, MemoryScope, MemorySourceRef, MemoryTier,
    TrustDomain,
};
use sinomo_runtime::events::{SessionId, StoredEvent};
use sinomo_runtime::object::{LengthMode, serde_ext};
use sinomo_runtime::storage::durable_file::{
    self, DurableAtomicFileOptions, DurableFileSync, DurableLengthEndian,
    DurableRecordAppendOptions, DurableRecordReadOptions,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
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
pub struct MemoryIndexSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub last_sequence: u64,
    pub entries: Vec<MemoryIndexEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub token_index: Vec<MemoryTokenIndexEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MemorySnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub last_sequence: u64,
    pub records: Vec<MemoryRecord>,
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

#[derive(Clone, Debug)]
pub struct DurableMemoryStore {
    root: PathBuf,
    records_per_segment: u64,
    write_lock: Arc<Mutex<()>>,
}

impl DurableMemoryStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
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
        let sequence = state
            .last_sequence
            .checked_add(1)
            .ok_or(MemoryError::SequenceOverflow)?;
        self.append_log_entry(&MemoryLogEntry::put(sequence, record.clone()))?;
        state.last_sequence = sequence;
        state
            .record_sequences
            .insert(record.memory_id.clone(), sequence);
        state
            .records
            .insert(record.memory_id.clone(), record.clone());
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
        };
        self.write_sobj_file(&self.snapshot_path(), &snapshot)?;
        state.snapshot_sequence = snapshot.last_sequence;
        self.write_index_snapshot(&state)?;
        self.clear_log_segments()?;
        Ok(snapshot)
    }

    pub fn load_snapshot(&self) -> Result<Option<MemorySnapshot>, MemoryError> {
        let path = self.snapshot_path();
        match durable_file::read_object_file_bytes(
            &path,
            durable_file::DEFAULT_OBJECT_FILE_MAX_READ_BYTES,
        ) {
            Ok(bytes) => {
                let snapshot = decode_sobj::<MemorySnapshot>(&path, &bytes)?;
                if snapshot.schema_version != MEMORY_STORAGE_SCHEMA_VERSION {
                    return Err(MemoryError::UnsupportedSchema {
                        path: path.display().to_string(),
                        schema_version: snapshot.schema_version,
                    });
                }
                Ok(Some(snapshot))
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(source) => Err(MemoryError::DurableFile {
                path: source.path().to_string(),
                detail: source.detail().to_string(),
            }),
        }
    }

    pub fn load_index(&self) -> Result<Option<MemoryIndexSnapshot>, MemoryError> {
        let path = self.index_path();
        match durable_file::read_object_file_bytes(
            &path,
            durable_file::DEFAULT_OBJECT_FILE_MAX_READ_BYTES,
        ) {
            Ok(bytes) => {
                let index = decode_sobj::<MemoryIndexSnapshot>(&path, &bytes)?;
                if index.schema_version != MEMORY_STORAGE_SCHEMA_VERSION {
                    return Err(MemoryError::UnsupportedSchema {
                        path: path.display().to_string(),
                        schema_version: index.schema_version,
                    });
                }
                Ok(Some(index))
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(source) => Err(MemoryError::DurableFile {
                path: source.path().to_string(),
                detail: source.detail().to_string(),
            }),
        }
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
        let mut file = match File::open(path) {
            Ok(file) => file,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(source) => {
                return Err(MemoryError::Io {
                    path: path.display().to_string(),
                    source,
                });
            }
        };
        let mut entries = Vec::new();
        let options = DurableRecordReadOptions::new(DurableLengthEndian::Big)
            .max_record_bytes(MAX_MEMORY_LOG_RECORD_BYTES);
        while let Some(record) = durable_file::read_length_prefixed_record(&mut file, path, options)
            .map_err(|source| MemoryError::DurableFile {
                path: source.path().to_string(),
                detail: source.detail().to_string(),
            })?
        {
            let entry = decode_sobj::<MemoryLogEntry>(path, &record.bytes)?;
            if entry.schema_version != MEMORY_STORAGE_SCHEMA_VERSION {
                return Err(MemoryError::UnsupportedSchema {
                    path: path.display().to_string(),
                    schema_version: entry.schema_version,
                });
            }
            entries.push(entry);
        }
        Ok(entries)
    }

    fn append_log_entry(&self, entry: &MemoryLogEntry) -> Result<(), MemoryError> {
        if entry.sequence == 0 {
            return Err(MemoryError::InvalidLogSequence { sequence: 0 });
        }
        let path = self.segment_path(entry.sequence);
        let bytes = encode_sobj(&path, entry)?;
        let mut append_file = durable_file::open_file_for_append(&path).map_err(|source| {
            MemoryError::DurableFile {
                path: source.path().to_string(),
                detail: source.detail().to_string(),
            }
        })?;
        durable_file::append_length_prefixed_records(
            &mut append_file.file,
            &path,
            std::iter::once(bytes),
            DurableRecordAppendOptions::new(DurableLengthEndian::Big).sync(DurableFileSync::Data),
        )
        .map_err(|source| MemoryError::DurableFile {
            path: source.path().to_string(),
            detail: source.detail().to_string(),
        })?;
        Ok(())
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
        let token_index = build_token_index(state.records.values());
        let index = MemoryIndexSnapshot {
            schema_version: MEMORY_STORAGE_SCHEMA_VERSION,
            last_sequence: state.last_sequence,
            entries,
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

    fn log_paths(&self) -> Result<Vec<PathBuf>, MemoryError> {
        let mut paths = Vec::new();
        let segments_dir = self.segments_dir();
        let entries = match fs::read_dir(&segments_dir) {
            Ok(entries) => entries,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                paths.sort();
                return Ok(paths);
            }
            Err(source) => {
                return Err(MemoryError::Io {
                    path: segments_dir.display().to_string(),
                    source,
                });
            }
        };
        for entry in entries {
            let entry = entry.map_err(|source| MemoryError::Io {
                path: segments_dir.display().to_string(),
                source,
            })?;
            let path = entry.path();
            if path.extension().and_then(|extension| extension.to_str()) == Some("sobjlog") {
                paths.push(path);
            }
        }
        paths.sort();
        Ok(paths)
    }

    fn clear_log_segments(&self) -> Result<(), MemoryError> {
        for path in self.log_paths()? {
            durable_file::remove_file_if_exists_and_sync_parent(&path).map_err(|source| {
                MemoryError::DurableFile {
                    path: source.path().to_string(),
                    detail: source.detail().to_string(),
                }
            })?;
        }
        durable_file::remove_empty_dir_if_exists_and_sync_parent(&self.segments_dir()).map_err(
            |source| MemoryError::DurableFile {
                path: source.path().to_string(),
                detail: source.detail().to_string(),
            },
        )?;
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
        durable_file::write_file_atomic(
            path,
            bytes,
            DurableAtomicFileOptions::new("sobj", "memory-tmp").sync_all(),
        )
        .map_err(|source| MemoryError::DurableFile {
            path: source.path().to_string(),
            detail: source.detail().to_string(),
        })
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
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
enum MemoryLogOperation {
    PutRecord { record: MemoryRecord },
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
    if let MemoryScope::Agent { agent_id } = &record.scope {
        collect_tokens(agent_id, &mut tokens);
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
    #[error("memory I/O error at {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("memory durable file error at {path}: {detail}")]
    DurableFile { path: String, detail: String },
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
        assert_eq!(index.last_sequence, 1);
        assert_eq!(index.entries[0].memory_id, record.memory_id);
        assert_eq!(index.entries[0].sequence, 1);
        assert!(index.entries[0].path.starts_with("records/segments/"));
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
        assert_eq!(snapshot.last_sequence, 1);
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
