//! Segmented journal persistence for local DFS metadata shards.

pub(crate) mod admission;

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fs;
use std::io::{Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Instant, SystemTime};

use serde::{Deserialize, Serialize};
use sinomo_core::object::{LengthMode, serde_ext as sinomo_object_serde};
use sinomo_core::storage::durable_file;

use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_io::{
    create_dir_all, metadata_durable_file_error, read_dir, read_sobj_optional,
    remove_file_if_exists, sync_dir, write_sobj_atomic,
};
use crate::metadata_records::{
    DfsMetadataJournalBatch, DfsMetadataJournalEntry, DfsMetadataMutation, DfsMetadataPrefixProof,
    DfsMetadataShardId, dfs_metadata_prefix_proof_v2_after_bytes,
};

pub(crate) const JOURNAL_SEGMENT_ENTRY_LIMIT: u64 = 8192;
const JOURNAL_SEGMENT_INDEX_CACHE_LIMIT: usize = 256;
// Point lookups may name multi-megabyte namespace records. Keep their decoded
// form under a deliberately small process-wide LRU so repeated table replay
// does not trade CPU amplification for unbounded resident memory.
const JOURNAL_SEGMENT_DECODED_CACHE_ENTRY_LIMIT: usize = 16;
const JOURNAL_SEGMENT_DECODED_CACHE_BYTE_LIMIT: u64 = 32 * 1024 * 1024;

/// One immutable, payload-complete journal entry and its exact SOBJ bytes.
///
/// Encoding borrows the caller's typed entry, while reading owns the decoded
/// entry. In both cases the byte allocation is immutable and can be reused by
/// durable append, byte-based prefix proofs, and a later wire integration
/// without serializing the entry again.
#[derive(Debug)]
pub(crate) struct DfsMetadataJournalFrame<'a> {
    entry: Cow<'a, DfsMetadataJournalEntry>,
    exact_bytes: Arc<[u8]>,
}

impl<'a> DfsMetadataJournalFrame<'a> {
    pub(crate) fn encode(entry: &'a DfsMetadataJournalEntry) -> Result<Self> {
        let exact_bytes = encode_metadata_journal_entry(entry)?;
        Ok(Self {
            entry: Cow::Borrowed(entry),
            exact_bytes,
        })
    }

    pub(crate) fn entry(&self) -> &DfsMetadataJournalEntry {
        self.entry.as_ref()
    }

    pub(crate) fn exact_bytes(&self) -> &[u8] {
        self.exact_bytes.as_ref()
    }

    pub(crate) fn exact_bytes_arc(&self) -> Arc<[u8]> {
        Arc::clone(&self.exact_bytes)
    }

    #[cfg(test)]
    pub(crate) fn encoded_len(&self) -> usize {
        self.exact_bytes.len()
    }

    #[cfg(test)]
    pub(crate) fn prefix_proof_after(
        &self,
        previous: &DfsMetadataPrefixProof,
    ) -> Result<DfsMetadataPrefixProof> {
        metadata_prefix_proof_after_exact_frame(
            previous,
            self.entry(),
            &self.entry().shard_id,
            self.exact_bytes(),
        )
    }

    pub(crate) fn into_entry(self) -> DfsMetadataJournalEntry {
        self.entry.into_owned()
    }
}

/// Extend one exact prefix using the immutable bytes that were or will be
/// appended for `entry`. Keeping this validation here gives local writers,
/// replica ingest, handoff verification, and checkpoint replay one canonical
/// byte-proof implementation.
pub(crate) fn metadata_prefix_proof_after_exact_frame(
    previous: &DfsMetadataPrefixProof,
    entry: &DfsMetadataJournalEntry,
    expected_shard_id: &DfsMetadataShardId,
    exact_bytes: &[u8],
) -> Result<DfsMetadataPrefixProof> {
    if !previous.is_well_formed()
        || previous.journal_sequence.saturating_add(1) != entry.sequence
        || &entry.shard_id != expected_shard_id
    {
        return Err(MetadataBackendError::Serde(format!(
            "metadata shard {} cannot extend exact-frame prefix {} with journal entry {}:{}",
            expected_shard_id.0, previous.journal_sequence, entry.shard_id.0, entry.sequence
        )));
    }
    Ok(dfs_metadata_prefix_proof_v2_after_bytes(
        previous,
        entry.sequence,
        exact_bytes,
    ))
}

impl DfsMetadataJournalFrame<'static> {
    pub(crate) fn local(
        shard_id: &DfsMetadataShardId,
        sequence: u64,
        mutation: DfsMetadataMutation,
    ) -> Result<Self> {
        let entry = DfsMetadataJournalEntry {
            shard_id: shard_id.clone(),
            sequence: sequence.max(1),
            created_at: time::OffsetDateTime::now_utc(),
            mutation,
        };
        let exact_bytes = encode_metadata_journal_entry(&entry)?;
        Ok(Self {
            entry: Cow::Owned(entry),
            exact_bytes,
        })
    }
    fn decode_exact(exact_bytes: Vec<u8>) -> std::result::Result<Self, sinomo_core::object::Error> {
        let entry = sinomo_object_serde::from_bytes(&exact_bytes)?;
        Ok(Self {
            entry: Cow::Owned(entry),
            exact_bytes: Arc::from(exact_bytes),
        })
    }
}

pub(crate) fn decode_metadata_journal_frame_record(
    record: &crate::DfsMetadataJournalFrameRecord,
) -> Result<DfsMetadataJournalEntry> {
    let entry: DfsMetadataJournalEntry = sinomo_object_serde::from_bytes(&record.exact_bytes)
        .map_err(|source| MetadataBackendError::Serde(source.to_string()))?;
    if entry.sequence != record.sequence {
        return Err(MetadataBackendError::Serde(format!(
            "metadata journal frame declares sequence {} but decodes as {}",
            record.sequence, entry.sequence
        )));
    }
    Ok(entry)
}

fn encode_metadata_journal_entry(entry: &DfsMetadataJournalEntry) -> Result<Arc<[u8]>> {
    let exact_bytes = sinomo_object_serde::to_bytes(entry, 1, LengthMode::U32)
        .map_err(|source| MetadataBackendError::Serde(source.to_string()))?;
    u32::try_from(exact_bytes.len()).map_err(|_| {
        MetadataBackendError::Serde(format!(
            "metadata journal entry {} is too large: {} bytes",
            entry.sequence,
            exact_bytes.len()
        ))
    })?;
    Ok(Arc::from(exact_bytes))
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn encode_metadata_journal_frame_record(
    entry: &DfsMetadataJournalEntry,
) -> Result<crate::DfsMetadataJournalFrameRecord> {
    let frame = DfsMetadataJournalFrame::encode(entry)?;
    Ok(crate::DfsMetadataJournalFrameRecord {
        sequence: entry.sequence,
        // Keep the journal encoder's immutable allocation as the wire owner.
        // `Bytes::from_owner` makes later per-replica clones O(1) without
        // copying a multi-megabyte prepare into a second allocation first.
        exact_bytes: bytes::Bytes::from_owner(frame.exact_bytes_arc()),
    })
}

#[cfg(test)]
thread_local! {
    static BEFORE_JOURNAL_PRUNE_HOOK: std::cell::RefCell<Option<(PathBuf, Box<dyn FnOnce()>)>> = const { std::cell::RefCell::new(None) };
    static METADATA_JOURNAL_APPEND_FAULT: std::cell::Cell<Option<JournalAppendFault>> = const { std::cell::Cell::new(None) };
    static METADATA_JOURNAL_APPEND_SYNC_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static METADATA_JOURNAL_CERTIFICATION_FAILURES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static METADATA_JOURNAL_SEGMENT_SCAN_CALLS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static METADATA_JOURNAL_DECODED_RECORDS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static METADATA_JOURNAL_DECODED_CACHE_HITS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static METADATA_JOURNAL_EXACT_INDEX_LOOKUPS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static METADATA_JOURNAL_EXACT_FRAME_DECODES: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static METADATA_JOURNAL_EXACT_SEGMENT_OPENS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static METADATA_JOURNAL_TAIL_RECOVERY_CALLS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static METADATA_JOURNAL_REPLAY_FRAME_SEGMENTS_BEFORE_FAILURE: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn run_before_journal_prune_hook_for_test(root: &Path) {
    let action = BEFORE_JOURNAL_PRUNE_HOOK.with(|hook| {
        let mut hook = hook.borrow_mut();
        if hook.as_ref().is_some_and(|(expected, _)| expected == root) {
            hook.take().map(|(_, action)| action)
        } else {
            None
        }
    });
    if let Some(action) = action {
        action();
    }
}

#[cfg(test)]
#[derive(Clone, Copy)]
pub(crate) struct JournalAppendFault {
    pub(crate) skip_appends: usize,
    pub(crate) complete_frames: usize,
    pub(crate) partial_frame_bytes: usize,
}

#[cfg(test)]
pub(crate) fn reset_journal_append_sync_calls_for_test() {
    METADATA_JOURNAL_APPEND_SYNC_CALLS.set(0);
}
#[cfg(test)]
pub(crate) fn journal_append_sync_calls_for_test() -> usize {
    METADATA_JOURNAL_APPEND_SYNC_CALLS.get()
}

fn append_journal_records(file: &mut fs::File, path: &Path, records: &[&[u8]]) -> Result<u64> {
    #[cfg(test)]
    if let Some(mut fault) = METADATA_JOURNAL_APPEND_FAULT.take() {
        if fault.skip_appends > 0 {
            fault.skip_appends -= 1;
            METADATA_JOURNAL_APPEND_FAULT.set(Some(fault));
        } else {
            use std::io::Write as _;
            for (index, bytes) in records.iter().enumerate() {
                let mut framed = (bytes.len() as u32).to_le_bytes().to_vec();
                framed.extend_from_slice(bytes);
                let keep = if index < fault.complete_frames {
                    framed.len()
                } else if index == fault.complete_frames {
                    fault.partial_frame_bytes.min(framed.len())
                } else {
                    0
                };
                file.write_all(&framed[..keep])
                    .map_err(|source| MetadataBackendError::Io {
                        path: path.to_owned(),
                        source,
                    })?;
            }
            return Err(MetadataBackendError::Serde(
                "injected authored journal append/sync failure".into(),
            ));
        }
    }
    #[cfg(test)]
    METADATA_JOURNAL_APPEND_SYNC_CALLS
        .set(METADATA_JOURNAL_APPEND_SYNC_CALLS.get().saturating_add(1));
    durable_file::append_length_prefixed_records(
        file,
        path,
        records.iter().copied(),
        durable_file::DurableRecordAppendOptions::new(durable_file::DurableLengthEndian::Little)
            .sync(durable_file::DurableFileSync::All),
    )
    .map_err(metadata_durable_file_error)
}

#[cfg(test)]
pub(crate) fn fail_journal_certifications_for_test(count: usize) {
    METADATA_JOURNAL_CERTIFICATION_FAILURES.set(count);
}

#[cfg(test)]
fn fail_journal_certification_for_test() -> Result<()> {
    let remaining = METADATA_JOURNAL_CERTIFICATION_FAILURES.get();
    if remaining > 0 {
        METADATA_JOURNAL_CERTIFICATION_FAILURES.set(remaining - 1);
        return Err(MetadataBackendError::Serde(
            "injected journal certification sync failure".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn metadata_journal_tail_recovery_calls() -> usize {
    METADATA_JOURNAL_TAIL_RECOVERY_CALLS.get()
}

#[cfg(test)]
fn fail_replayed_frame_append_after_segments_for_test(segment_count: usize) {
    METADATA_JOURNAL_REPLAY_FRAME_SEGMENTS_BEFORE_FAILURE.set(segment_count);
}

#[cfg(test)]
fn maybe_fail_replayed_frame_append_after_segment() -> Result<()> {
    METADATA_JOURNAL_REPLAY_FRAME_SEGMENTS_BEFORE_FAILURE.with(|remaining| {
        let count = remaining.get();
        if count == 0 {
            return Ok(());
        }
        if count == 1 {
            remaining.set(0);
            return Err(MetadataBackendError::Serde(
                "injected metadata replay frame append failure".to_string(),
            ));
        }
        remaining.set(count - 1);
        Ok(())
    })
}

#[cfg(test)]
fn reset_metadata_journal_decoded_records() {
    METADATA_JOURNAL_DECODED_RECORDS.set(0);
}

#[cfg(test)]
fn metadata_journal_decoded_records() -> usize {
    METADATA_JOURNAL_DECODED_RECORDS.get()
}

#[cfg(test)]
fn reset_metadata_journal_decoded_cache_hits() {
    METADATA_JOURNAL_DECODED_CACHE_HITS.set(0);
}

#[cfg(test)]
fn metadata_journal_decoded_cache_hits() -> usize {
    METADATA_JOURNAL_DECODED_CACHE_HITS.get()
}

#[cfg(test)]
pub(crate) fn reset_metadata_journal_exact_read_counters() {
    METADATA_JOURNAL_EXACT_INDEX_LOOKUPS.set(0);
    METADATA_JOURNAL_EXACT_FRAME_DECODES.set(0);
    METADATA_JOURNAL_EXACT_SEGMENT_OPENS.set(0);
}

#[cfg(test)]
fn metadata_journal_exact_index_lookups() -> usize {
    METADATA_JOURNAL_EXACT_INDEX_LOOKUPS.get()
}

#[cfg(test)]
pub(crate) fn metadata_journal_exact_frame_decodes() -> usize {
    METADATA_JOURNAL_EXACT_FRAME_DECODES.get()
}

#[cfg(test)]
pub(crate) fn metadata_journal_exact_segment_opens() -> usize {
    METADATA_JOURNAL_EXACT_SEGMENT_OPENS.get()
}

#[cfg(test)]
fn metadata_journal_decoded_cache_usage(path: &Path) -> (usize, u64) {
    let cache = journal_segment_index_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    cache
        .decoded_entries
        .iter()
        .filter(|(key, _)| key.path == path)
        .fold((0_usize, 0_u64), |(entries, bytes), (_, cached)| {
            (
                entries.saturating_add(1),
                bytes.saturating_add(cached.record_bytes),
            )
        })
}

#[cfg(test)]
fn metadata_journal_decoded_cache_total_usage() -> (usize, u64) {
    let cache = journal_segment_index_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    (cache.decoded_entries.len(), cache.decoded_entry_bytes)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct JournalSegmentFileIdentity {
    created: Option<SystemTime>,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(windows)]
    volume_serial_number: Option<u32>,
    #[cfg(windows)]
    file_index: Option<u64>,
}

impl JournalSegmentFileIdentity {
    fn from_file_metadata(file: &fs::File, metadata: &fs::Metadata) -> Self {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;

            let _ = file;
            Self {
                created: metadata.created().ok(),
                device: metadata.dev(),
                inode: metadata.ino(),
            }
        }
        #[cfg(windows)]
        {
            let identity = crate::metadata_io::windows_file_identity(file);
            Self {
                created: metadata.created().ok(),
                volume_serial_number: identity.map(|(volume, _)| volume),
                file_index: identity.map(|(_, index)| index),
            }
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = file;
            Self {
                created: metadata.created().ok(),
            }
        }
    }

    fn supports_append_extension(&self) -> bool {
        #[cfg(unix)]
        {
            true
        }
        #[cfg(windows)]
        {
            self.volume_serial_number.is_some() && self.file_index.is_some()
        }
        #[cfg(not(any(unix, windows)))]
        {
            false
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct JournalSegmentEntryOffset {
    sequence: u64,
    record_start: u64,
}

#[derive(Clone, Debug)]
struct JournalSegmentIndex {
    content_generation: u64,
    identity: JournalSegmentFileIdentity,
    observed_file_len: u64,
    modified: Option<SystemTime>,
    indexed_bytes: u64,
    // Readers retain immutable offset snapshots. Sharing the offsets keeps a
    // warm point lookup from copying the complete segment index. Append and
    // extension preserve retained snapshots with copy-on-write.
    entries: Arc<Vec<JournalSegmentEntryOffset>>,
    sequences_monotonic: bool,
}

impl JournalSegmentIndex {
    fn record_bytes(&self, expected: JournalSegmentEntryOffset) -> Option<u64> {
        let position = self
            .entries
            .binary_search_by_key(&expected.record_start, |entry| entry.record_start)
            .ok()?;
        (self.entries[position].sequence == expected.sequence)
            .then_some(())
            .and_then(|()| {
                self.entries
                    .get(position.saturating_add(1))
                    .map(|next| next.record_start)
                    .unwrap_or(self.indexed_bytes)
                    .checked_sub(expected.record_start)
            })
    }

    fn matches_file_snapshot(&self, snapshot: &JournalSegmentFileSnapshot) -> bool {
        self.identity == snapshot.identity
            && self.observed_file_len == snapshot.file_len
            && self.modified == snapshot.modified
            && self.indexed_bytes == snapshot.file_len
    }

    fn matches_cached_snapshot(&self, other: &Self) -> bool {
        self.content_generation == other.content_generation
            && self.identity == other.identity
            && self.observed_file_len == other.observed_file_len
            && self.modified == other.modified
            && self.indexed_bytes == other.indexed_bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct JournalSegmentFileSnapshot {
    identity: JournalSegmentFileIdentity,
    file_len: u64,
    modified: Option<SystemTime>,
}

impl JournalSegmentFileSnapshot {
    fn from_file(file: &fs::File) -> std::io::Result<Self> {
        let metadata = file.metadata()?;
        Ok(Self {
            identity: JournalSegmentFileIdentity::from_file_metadata(file, &metadata),
            file_len: metadata.len(),
            modified: metadata.modified().ok(),
        })
    }
}

#[derive(Clone, Debug)]
struct CachedJournalSegmentIndex {
    index: JournalSegmentIndex,
    last_used: u64,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct JournalSegmentDecodedEntryKey {
    path: PathBuf,
    content_generation: u64,
    record_start: u64,
}

#[derive(Clone, Debug)]
struct CachedJournalSegmentDecodedEntry {
    sequence: u64,
    record_bytes: u64,
    entry: Arc<DfsMetadataJournalEntry>,
    last_used: u64,
}

#[derive(Debug, Default)]
struct JournalSegmentIndexCache {
    generation: u64,
    entries: BTreeMap<PathBuf, CachedJournalSegmentIndex>,
    decoded_entry_bytes: u64,
    decoded_entries: BTreeMap<JournalSegmentDecodedEntryKey, CachedJournalSegmentDecodedEntry>,
}

fn journal_segment_index_cache() -> &'static Mutex<JournalSegmentIndexCache> {
    static CACHE: OnceLock<Mutex<JournalSegmentIndexCache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(JournalSegmentIndexCache::default()))
}

fn cached_journal_segment_index(path: &Path) -> Option<JournalSegmentIndex> {
    let mut cache = journal_segment_index_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    cache.generation = cache.generation.saturating_add(1);
    let generation = cache.generation;
    let cached = cache.entries.get_mut(path)?;
    cached.last_used = generation;
    Some(cached.index.clone())
}

fn remove_cached_journal_segment_decoded_entries(
    cache: &mut JournalSegmentIndexCache,
    path: &Path,
) {
    let mut removed_bytes = 0_u64;
    cache.decoded_entries.retain(|key, cached| {
        let retain = key.path != path;
        if !retain {
            removed_bytes = removed_bytes.saturating_add(cached.record_bytes);
        }
        retain
    });
    cache.decoded_entry_bytes = cache.decoded_entry_bytes.saturating_sub(removed_bytes);
}

fn remove_cached_journal_segment(
    cache: &mut JournalSegmentIndexCache,
    path: &Path,
) -> Option<CachedJournalSegmentIndex> {
    remove_cached_journal_segment_decoded_entries(cache, path);
    cache.entries.remove(path)
}

fn cache_journal_segment_index(
    path: PathBuf,
    mut index: JournalSegmentIndex,
) -> JournalSegmentIndex {
    let mut cache = journal_segment_index_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    cache.generation = cache.generation.saturating_add(1);
    let generation = cache.generation;
    if index.content_generation == 0 {
        index.content_generation = generation;
        remove_cached_journal_segment_decoded_entries(&mut cache, &path);
    } else if cache
        .entries
        .get(&path)
        .is_some_and(|cached| cached.index.content_generation != index.content_generation)
    {
        // A concurrent reader has already installed a newer view of this
        // path. Do not let a stale append-extension overwrite it or populate
        // decoded entries under its obsolete generation.
        return index;
    }
    if !cache.entries.contains_key(&path)
        && cache.entries.len() >= JOURNAL_SEGMENT_INDEX_CACHE_LIMIT
        && let Some(evicted) = cache
            .entries
            .iter()
            .min_by_key(|(_, cached)| cached.last_used)
            .map(|(path, _)| path.clone())
    {
        remove_cached_journal_segment(&mut cache, &evicted);
    }
    cache.entries.insert(
        path,
        CachedJournalSegmentIndex {
            index: index.clone(),
            last_used: generation,
        },
    );
    index
}

fn cached_journal_segment_decoded_entries(
    path: &Path,
    index: &JournalSegmentIndex,
    expected: &[JournalSegmentEntryOffset],
) -> Option<Vec<Arc<DfsMetadataJournalEntry>>> {
    let mut cache = journal_segment_index_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let cached_index = cache.entries.get(path).map(|cached| &cached.index)?;
    if !cached_index.matches_cached_snapshot(index)
        || !expected
            .iter()
            .all(|expected| cached_index.record_bytes(*expected).is_some())
    {
        return None;
    }
    let mut cached_entries = Vec::with_capacity(expected.len());
    let mut keys = Vec::with_capacity(expected.len());
    for expected in expected {
        let key = JournalSegmentDecodedEntryKey {
            path: path.to_owned(),
            content_generation: index.content_generation,
            record_start: expected.record_start,
        };
        let cached = cache.decoded_entries.get(&key)?;
        if cached.sequence != expected.sequence {
            return None;
        }
        cached_entries.push(Arc::clone(&cached.entry));
        keys.push(key);
    }
    cache.generation = cache.generation.saturating_add(1);
    let generation = cache.generation;
    for key in keys {
        if let Some(cached) = cache.decoded_entries.get_mut(&key) {
            cached.last_used = generation;
        }
    }
    Some(cached_entries)
}

fn cache_journal_segment_decoded_entry_locked(
    cache: &mut JournalSegmentIndexCache,
    path: &Path,
    index: &JournalSegmentIndex,
    expected: JournalSegmentEntryOffset,
    record_bytes: u64,
    entry: Arc<DfsMetadataJournalEntry>,
) {
    if record_bytes == 0 || record_bytes > JOURNAL_SEGMENT_DECODED_CACHE_BYTE_LIMIT {
        return;
    }
    if !cache.entries.get(path).is_some_and(|cached| {
        cached.index.matches_cached_snapshot(index)
            && cached.index.record_bytes(expected) == Some(record_bytes)
    }) {
        return;
    }
    cache.generation = cache.generation.saturating_add(1);
    let generation = cache.generation;
    let key = JournalSegmentDecodedEntryKey {
        path: path.to_owned(),
        content_generation: index.content_generation,
        record_start: expected.record_start,
    };
    if let Some(replaced) = cache.decoded_entries.remove(&key) {
        cache.decoded_entry_bytes = cache
            .decoded_entry_bytes
            .saturating_sub(replaced.record_bytes);
    }
    while cache.decoded_entries.len() >= JOURNAL_SEGMENT_DECODED_CACHE_ENTRY_LIMIT
        || cache.decoded_entry_bytes.saturating_add(record_bytes)
            > JOURNAL_SEGMENT_DECODED_CACHE_BYTE_LIMIT
    {
        let Some(evicted) = cache
            .decoded_entries
            .iter()
            .min_by_key(|(_, cached)| cached.last_used)
            .map(|(key, _)| key.clone())
        else {
            break;
        };
        if let Some(evicted) = cache.decoded_entries.remove(&evicted) {
            cache.decoded_entry_bytes = cache
                .decoded_entry_bytes
                .saturating_sub(evicted.record_bytes);
        }
    }
    cache.decoded_entry_bytes = cache.decoded_entry_bytes.saturating_add(record_bytes);
    cache.decoded_entries.insert(
        key,
        CachedJournalSegmentDecodedEntry {
            sequence: expected.sequence,
            record_bytes,
            entry,
            last_used: generation,
        },
    );
}

fn clone_bounded_decoded_journal_suffix(
    records: &[(&DfsMetadataJournalEntry, JournalSegmentEntryOffset, u64)],
) -> Vec<(JournalSegmentEntryOffset, u64, Arc<DfsMetadataJournalEntry>)> {
    let mut selected =
        Vec::with_capacity(records.len().min(JOURNAL_SEGMENT_DECODED_CACHE_ENTRY_LIMIT));
    let mut selected_bytes = 0_u64;
    for (entry, expected, record_bytes) in records.iter().rev().copied() {
        let Some(next_bytes) = selected_bytes.checked_add(record_bytes) else {
            break;
        };
        if record_bytes == 0
            || selected.len() >= JOURNAL_SEGMENT_DECODED_CACHE_ENTRY_LIMIT
            || next_bytes > JOURNAL_SEGMENT_DECODED_CACHE_BYTE_LIMIT
        {
            break;
        }
        selected.push((entry, expected, record_bytes));
        selected_bytes = next_bytes;
    }
    selected.reverse();
    selected
        .into_iter()
        .map(|(entry, expected, record_bytes)| (expected, record_bytes, Arc::new(entry.clone())))
        .collect()
}

fn cache_journal_segment_decoded_entries(
    path: &Path,
    index: &JournalSegmentIndex,
    expected: &[JournalSegmentEntryOffset],
    entries: &[DfsMetadataJournalEntry],
) {
    if expected.len() != entries.len() {
        return;
    }
    let mut cache_candidates = Vec::with_capacity(entries.len());
    for (expected, entry) in expected.iter().copied().zip(entries) {
        let Some(record_bytes) = index.record_bytes(expected) else {
            return;
        };
        cache_candidates.push((entry, expected, record_bytes));
    }
    let decoded = clone_bounded_decoded_journal_suffix(&cache_candidates);
    if decoded.is_empty() {
        return;
    }
    let mut cache = journal_segment_index_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if !cache
        .entries
        .get(path)
        .is_some_and(|cached| cached.index.matches_cached_snapshot(index))
    {
        return;
    }
    for (expected, record_bytes, entry) in decoded {
        cache_journal_segment_decoded_entry_locked(
            &mut cache,
            path,
            index,
            expected,
            record_bytes,
            entry,
        );
    }
}

fn cache_journal_segment_durable_append(
    path: &Path,
    created: bool,
    before: Option<JournalSegmentFileSnapshot>,
    after: Option<JournalSegmentFileSnapshot>,
    records: &[(&DfsMetadataJournalEntry, u64)],
) {
    // Metadata writers hold the shard's inter-process append lock across this
    // call. Under that invariant, exact pre/post snapshots distinguish our
    // append from a concurrent writer; pathname replacement or an unexpected
    // length change simply makes this optimization ineligible and the reader
    // falls back to its existing durable scan.
    let (Some(before), Some(after)) = (before, after) else {
        return;
    };
    let Some(appended_bytes) = records
        .iter()
        .try_fold(0_u64, |total, (_, bytes)| total.checked_add(*bytes))
    else {
        return;
    };
    if records.is_empty()
        || appended_bytes == 0
        || before.identity != after.identity
        || before.file_len.checked_add(appended_bytes) != Some(after.file_len)
    {
        return;
    }

    let mut offsets = Vec::with_capacity(records.len());
    let mut record_start = before.file_len;
    let mut previous_sequence = None;
    for (entry, record_bytes) in records {
        if *record_bytes == 0
            || previous_sequence.is_some_and(|sequence| entry.sequence <= sequence)
        {
            return;
        }
        offsets.push(JournalSegmentEntryOffset {
            sequence: entry.sequence,
            record_start,
        });
        let Some(next_record_start) = record_start.checked_add(*record_bytes) else {
            return;
        };
        record_start = next_record_start;
        previous_sequence = Some(entry.sequence);
    }
    if record_start != after.file_len {
        return;
    }

    let index = {
        let mut cache = journal_segment_index_cache()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        cache.generation = cache.generation.saturating_add(1);
        let generation = cache.generation;

        let cached_post_append = cache.entries.get(path).and_then(|cached| {
            if !cached.index.matches_file_snapshot(&after) {
                return None;
            }
            offsets
                .iter()
                .zip(records)
                .all(|(expected, (_, record_bytes))| {
                    cached.index.record_bytes(*expected) == Some(*record_bytes)
                })
                .then(|| cached.index.clone())
        });

        let index = if let Some(index) = cached_post_append {
            index
        } else if let Some(cached) = cache.entries.get(path)
            && cached.index.matches_file_snapshot(&before)
            && cached
                .index
                .entries
                .last()
                .is_none_or(|last| last.sequence < records[0].0.sequence)
        {
            let mut index = cached.index.clone();
            Arc::make_mut(&mut index.entries).extend(offsets.iter().copied());
            index.sequences_monotonic &= offsets
                .windows(2)
                .all(|pair| pair[0].sequence <= pair[1].sequence);
            index.identity = after.identity.clone();
            index.observed_file_len = after.file_len;
            index.modified = after.modified;
            index.indexed_bytes = after.file_len;
            index
        } else if created && before.file_len == 0 {
            remove_cached_journal_segment(&mut cache, path);
            JournalSegmentIndex {
                content_generation: generation,
                identity: after.identity.clone(),
                observed_file_len: after.file_len,
                modified: after.modified,
                indexed_bytes: after.file_len,
                entries: Arc::new(offsets.clone()),
                sequences_monotonic: offsets
                    .windows(2)
                    .all(|pair| pair[0].sequence <= pair[1].sequence),
            }
        } else {
            return;
        };

        if !cache.entries.contains_key(path)
            && cache.entries.len() >= JOURNAL_SEGMENT_INDEX_CACHE_LIMIT
            && let Some(evicted) = cache
                .entries
                .iter()
                .min_by_key(|(_, cached)| cached.last_used)
                .map(|(path, _)| path.clone())
        {
            remove_cached_journal_segment(&mut cache, &evicted);
        }
        cache.entries.insert(
            path.to_owned(),
            CachedJournalSegmentIndex {
                index: index.clone(),
                last_used: generation,
            },
        );
        index
    };

    // Namespace prepares can be several megabytes. Clone them outside the
    // process-wide cache lock; a concurrent append or replacement may make
    // this best-effort decoded fill ineligible, while the exact offset index
    // installed above remains useful and authoritative reads still scan disk.
    let cache_candidates = records
        .iter()
        .zip(&offsets)
        .map(|((entry, record_bytes), expected)| (*entry, *expected, *record_bytes))
        .collect::<Vec<_>>();
    let decoded = clone_bounded_decoded_journal_suffix(&cache_candidates);
    if decoded.is_empty() {
        return;
    }
    let mut cache = journal_segment_index_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    for (expected, record_bytes, entry) in decoded {
        cache_journal_segment_decoded_entry_locked(
            &mut cache,
            path,
            &index,
            expected,
            record_bytes,
            entry,
        );
    }
}

#[cfg(test)]
fn reset_journal_segment_index_cache() {
    let mut cache = journal_segment_index_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    cache.entries.clear();
    cache.decoded_entries.clear();
    cache.decoded_entry_bytes = 0;
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct DfsMetadataJournalHead {
    pub(crate) next_sequence: u64,
}

/// Validated O(1) locator for the durable tail record.
///
/// `head.sobj` remains a deferred sequence hint and is never trusted alone.
/// This sidecar is written only after a complete highest-segment scan. A later
/// open may trust the head without decoding the whole segment only when the
/// same highest segment still has the exact recorded length and the record at
/// `record_start` decodes as the final expected sequence. Any append, segment
/// rotation, replacement, or torn tail invalidates the locator and falls back
/// to the bounded scan.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct DfsMetadataJournalTailHint {
    next_sequence: u64,
    segment_id: u64,
    record_start: u64,
    file_len: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct DfsMetadataReplayCursor {
    pub(crate) shard_id: DfsMetadataShardId,
    pub(crate) last_sequence: u64,
}

pub(crate) fn recover_next_journal_sequence(root: &Path) -> Result<u64> {
    #[cfg(test)]
    METADATA_JOURNAL_TAIL_RECOVERY_CALLS
        .set(METADATA_JOURNAL_TAIL_RECOVERY_CALLS.get().saturating_add(1));
    let journal_dir = root.join("journal");
    if !journal_dir.exists() {
        return Ok(1);
    }
    let head_path = journal_dir.join("head.sobj");
    let head_next_sequence = journal_head_next_sequence(root)?;
    let mut max_sequence = head_next_sequence
        .map(|next_sequence| next_sequence.saturating_sub(1))
        .unwrap_or(0);
    let mut segmented_tail_hint = None;
    // `head.sobj` and the local materialization cursor are deferred hints.
    // They may agree while both lag a fully durable append. The tail locator
    // proves that no bytes follow the expected last record; otherwise inspect
    // the bounded highest segment and repair both hints before admitting a
    // sequence that could otherwise be reused.
    if !validated_journal_tail_hint(root, head_next_sequence)? {
        let (segmented_max_sequence, tail_hint) = max_segmented_journal_sequence(root)?;
        max_sequence = max_sequence.max(segmented_max_sequence);
        segmented_tail_hint = tail_hint;
    }

    let next_sequence = max_sequence.saturating_add(1).max(1);
    if head_next_sequence != Some(next_sequence) {
        write_sobj_atomic(&head_path, &DfsMetadataJournalHead { next_sequence })?;
    }
    if let Some(tail_hint) = segmented_tail_hint
        && tail_hint.next_sequence == next_sequence
    {
        // The locator is a derived optimization, not a durability boundary.
        // A failed write only makes the next open repeat the bounded scan.
        let _ = write_sobj_atomic(&journal_tail_hint_path(root), &tail_hint);
    }
    Ok(next_sequence)
}

pub(crate) fn read_next_journal_sequence_lenient(root: &Path) -> Result<u64> {
    if let Some(next_sequence) = journal_head_next_sequence(root)? {
        return Ok(next_sequence.max(1));
    }

    let (max_sequence, _) = max_segmented_journal_sequence(root)?;
    Ok(max_sequence.saturating_add(1).max(1))
}

fn highest_segmented_journal_path(root: &Path) -> Result<Option<(u64, PathBuf)>> {
    Ok(segmented_journal_paths_descending(root)?.into_iter().next())
}

fn segmented_journal_paths_descending(root: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let segments_dir = root.join("journal").join("segments");
    if !segments_dir.exists() {
        return Ok(Vec::new());
    }
    let mut paths = Vec::new();
    for entry in read_dir(&segments_dir)? {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("slog") {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };
        let Ok(segment_id) = stem.parse::<u64>() else {
            continue;
        };
        paths.push((segment_id, path));
    }
    paths.sort_by_key(|entry| std::cmp::Reverse(entry.0));
    Ok(paths)
}

fn journal_head_next_sequence(root: &Path) -> Result<Option<u64>> {
    match read_sobj_optional::<DfsMetadataJournalHead>(&root.join("journal").join("head.sobj")) {
        Ok(head) => {
            Ok(head.and_then(|head| (head.next_sequence > 0).then_some(head.next_sequence)))
        }
        // This file is a derived, atomically published hint. A crash may lose
        // its unsynced bytes; the journal itself was synced before publication.
        Err(MetadataBackendError::Serde(_)) => Ok(None),
        Err(error) => Err(error),
    }
}

fn journal_tail_hint_path(root: &Path) -> PathBuf {
    root.join("journal").join("tail.sobj")
}

fn validated_journal_tail_hint(root: &Path, head_next_sequence: Option<u64>) -> Result<bool> {
    let Some(head_next_sequence) = head_next_sequence.filter(|sequence| *sequence > 1) else {
        return Ok(false);
    };
    let Some(hint) =
        read_sobj_optional::<DfsMetadataJournalTailHint>(&journal_tail_hint_path(root))
            .unwrap_or(None)
    else {
        return Ok(false);
    };
    if hint.next_sequence != head_next_sequence
        || hint.file_len <= hint.record_start
        || journal_segment_id(head_next_sequence.saturating_sub(1)) != hint.segment_id
    {
        return Ok(false);
    }
    let Some((segment_id, path)) = highest_segmented_journal_path(root)? else {
        return Ok(false);
    };
    if segment_id != hint.segment_id {
        return Ok(false);
    }
    let mut file = fs::File::open(&path).map_err(|source| MetadataBackendError::Io {
        path: path.clone(),
        source,
    })?;
    let file_len = file
        .metadata()
        .map_err(|source| MetadataBackendError::Io {
            path: path.clone(),
            source,
        })?
        .len();
    if file_len != hint.file_len {
        return Ok(false);
    }
    file.seek(SeekFrom::Start(hint.record_start))
        .map_err(|source| MetadataBackendError::Io {
            path: path.clone(),
            source,
        })?;
    let Some(entry) = read_next_segmented_journal_entry(&mut file, &path)? else {
        return Ok(false);
    };
    let record_end = file
        .stream_position()
        .map_err(|source| MetadataBackendError::Io {
            path: path.clone(),
            source,
        })?;
    Ok(record_end == hint.file_len && entry.sequence.saturating_add(1) == head_next_sequence)
}

fn max_segmented_journal_sequence(
    root: &Path,
) -> Result<(u64, Option<DfsMetadataJournalTailHint>)> {
    let paths = segmented_journal_paths_descending(root)?;
    let Some(highest_segment_id) = paths.first().map(|(segment_id, _)| *segment_id) else {
        return Ok((0, None));
    };
    for (segment_id, path) in paths {
        let max_sequence = read_segmented_journal_file_since_limited(&path, 0, usize::MAX)?
            .into_iter()
            .map(|entry| entry.sequence)
            .max()
            .unwrap_or(0);
        if max_sequence == 0 {
            // A failed append may create the next segment before writing its
            // first complete record. Sequence recovery must continue into the
            // preceding segment instead of allowing that empty/torn pathname
            // to hide a fully durable tail.
            continue;
        }
        let tail_hint = (segment_id == highest_segment_id)
            .then(|| cached_journal_segment_index(&path))
            .flatten()
            .and_then(|index| {
                let tail = index.entries.last()?;
                (tail.sequence == max_sequence && index.indexed_bytes == index.observed_file_len)
                    .then_some(DfsMetadataJournalTailHint {
                        next_sequence: max_sequence.saturating_add(1).max(1),
                        segment_id,
                        record_start: tail.record_start,
                        file_len: index.observed_file_len,
                    })
            });
        return Ok((max_sequence, tail_hint));
    }
    Ok((0, None))
}

/// Repair the only bytes an interrupted append may safely discard, then
/// recover and persist the exact next sequence before the resident backend can
/// retry. Callers must hold the shard metadata writer lock.
#[cfg(test)]
pub(crate) fn recover_next_journal_sequence_after_append_error(root: &Path) -> Result<u64> {
    certify_journal_sources(root, None).map(|(next_sequence, _)| next_sequence)
}

fn certify_journal_sources(
    root: &Path,
    expected_shard: Option<&DfsMetadataShardId>,
) -> Result<(u64, Option<DfsMetadataPrefixProof>)> {
    // A pruned journal is rooted in this self-contained checkpoint. Certify its
    // actual bytes/name before accepting the sequence floor it contributes.
    let checkpoint =
        crate::metadata_table_store::read_metadata_table_snapshot_for_replica_repair(root)?;
    let checkpoint_sequence = checkpoint.materialized_through_sequence.unwrap_or(0);
    let mut exact_prefix = checkpoint.prefix_proof.clone();
    if exact_prefix.is_none() {
        if checkpoint_sequence != 0 || checkpoint.tables.has_materialized_state() {
            return Err(MetadataBackendError::replica_diverged(
                "current metadata checkpoint lacks its certified exact prefix".into(),
            ));
        }
        if let Some(shard) = expected_shard {
            exact_prefix = Some(crate::metadata_records::dfs_metadata_prefix_proof_genesis(
                shard,
            ));
        }
    }

    let snapshot_path = crate::metadata_table_store::metadata_table_snapshot_path(root);
    if snapshot_path.is_file() {
        let snapshot = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&snapshot_path)
            .map_err(|source| MetadataBackendError::Io {
                path: snapshot_path.clone(),
                source,
            })?;
        #[cfg(test)]
        fail_journal_certification_for_test()?;
        snapshot
            .sync_all()
            .map_err(|source| MetadataBackendError::Io {
                path: snapshot_path.clone(),
                source,
            })?;
        if let Some(parent) = snapshot_path.parent() {
            sync_dir(parent)?;
        }
    }
    let mut paths = segmented_journal_paths_descending(root)?;
    paths.reverse();
    let mut next_uncovered_sequence = checkpoint_sequence.saturating_add(1).max(1);
    let mut observed_shard = expected_shard.cloned();
    for (position, (segment_id, path)) in paths.iter().enumerate() {
        if segment_id.saturating_mul(JOURNAL_SEGMENT_ENTRY_LIMIT) <= checkpoint_sequence {
            // An exact self-contained checkpoint supersedes every possible
            // record in this segment. Its old bytes cannot constrain recovery
            // or require decoding after checkpoint publication/pruning.
            continue;
        }
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|source| MetadataBackendError::Io {
                path: path.clone(),
                source,
            })?;
        let file_len = file
            .metadata()
            .map_err(|source| MetadataBackendError::Io {
                path: path.clone(),
                source,
            })?
            .len();
        let mut complete_len = 0;
        let mut previous_sequence = None;
        // A retried batch can replay earlier records after later sequences.
        // Keep exact-frame digests for this segment's bounded sequence range,
        // so retries do not advance the logical prefix twice.
        let mut observed_frames = BTreeMap::new();
        while let Some(record) = durable_file::read_length_prefixed_record(
            &mut file,
            path,
            durable_file::DurableRecordReadOptions::new(durable_file::DurableLengthEndian::Little),
        )
        .map_err(metadata_durable_file_error)?
        {
            let frame = DfsMetadataJournalFrame::decode_exact(record.bytes).map_err(|error| {
                MetadataBackendError::replica_diverged(format!(
                    "complete journal frame at {} is malformed: {error}",
                    path.display()
                ))
            })?;
            let entry = frame.entry();
            if observed_shard
                .as_ref()
                .is_some_and(|shard| shard != &entry.shard_id)
            {
                return Err(MetadataBackendError::replica_diverged(format!(
                    "journal certification found a foreign shard at {}",
                    path.display()
                )));
            }
            observed_shard.get_or_insert_with(|| entry.shard_id.clone());
            let sequence = entry.sequence;
            if journal_segment_id(sequence) != *segment_id {
                return Err(MetadataBackendError::replica_diverged(format!(
                    "journal certification found a sequence discontinuity at {} sequence {sequence}",
                    path.display()
                )));
            }
            let digest = blake3::hash(frame.exact_bytes());
            if let Some(previous_digest) = observed_frames.get(&sequence) {
                if previous_digest != &digest {
                    return Err(MetadataBackendError::Serde(format!(
                        "metadata journal contains conflicting duplicate sequence {sequence} under {}",
                        root.display()
                    )));
                }
                complete_len = record.record_end;
                continue;
            }
            if previous_sequence.is_some_and(|previous: u64| sequence != previous.saturating_add(1))
            {
                return Err(MetadataBackendError::replica_diverged(format!(
                    "journal certification found a sequence discontinuity at {} sequence {sequence}",
                    path.display()
                )));
            }
            if sequence > checkpoint_sequence {
                if sequence != next_uncovered_sequence {
                    return Err(MetadataBackendError::replica_diverged(format!(
                        "journal certification requires sequence {next_uncovered_sequence}, found {sequence}"
                    )));
                }
                if let Some(previous) = exact_prefix.as_ref() {
                    exact_prefix = Some(metadata_prefix_proof_after_exact_frame(
                        previous,
                        entry,
                        &entry.shard_id,
                        frame.exact_bytes(),
                    )?);
                }
                next_uncovered_sequence = sequence.saturating_add(1);
            }
            previous_sequence = Some(sequence);
            observed_frames.insert(sequence, digest);
            complete_len = record.record_end;
        }
        if complete_len < file_len {
            if position + 1 != paths.len() {
                return Err(MetadataBackendError::replica_diverged(format!(
                    "journal certification found an incomplete non-tail segment {}",
                    path.display()
                )));
            }
            file.set_len(complete_len)
                .map_err(|source| MetadataBackendError::Io {
                    path: path.clone(),
                    source,
                })?;
        }
        #[cfg(test)]
        fail_journal_certification_for_test()?;
        file.sync_all().map_err(|source| MetadataBackendError::Io {
            path: path.clone(),
            source,
        })?;
        if let Some(parent) = path.parent() {
            sync_dir(parent)?;
        }
        let mut cache = journal_segment_index_cache()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        remove_cached_journal_segment(&mut cache, path);
    }
    let recovered = recover_next_journal_sequence(root)?;
    if recovered > next_uncovered_sequence {
        return Err(MetadataBackendError::replica_diverged(format!(
            "journal certification head hint {recovered} exceeds checkpoint/journal prefix {next_uncovered_sequence}"
        )));
    }
    let next_sequence = recovered.max(next_uncovered_sequence);
    if next_sequence != recovered {
        crate::metadata_cursor::write_journal_head_hint(root, next_sequence)?;
    }
    validate_certified_ack_floors(root, expected_shard, next_sequence, exact_prefix.as_ref())?;
    Ok((next_sequence, exact_prefix))
}

fn validate_certified_ack_floors(
    root: &Path,
    shard: Option<&DfsMetadataShardId>,
    next_sequence: u64,
    prefix: Option<&DfsMetadataPrefixProof>,
) -> Result<()> {
    let Some(shard) = shard else {
        return Ok(());
    };
    let prefix_floor = crate::metadata_cursor::metadata_prefix_ack_floor(root, shard)?;
    let authority_floor = crate::metadata_cursor::metadata_authority_ack_floor(root, shard)?;
    for (sequence, proof) in prefix_floor
        .as_ref()
        .map(|floor| (floor.journal_sequence, &floor.durable_prefix_proof))
        .into_iter()
        .chain(
            authority_floor
                .as_ref()
                .map(|floor| (floor.journal_sequence, &floor.durable_prefix_proof)),
        )
    {
        if next_sequence.saturating_sub(1) < sequence
            || (next_sequence.saturating_sub(1) == sequence && Some(proof) != prefix)
        {
            return Err(MetadataBackendError::replica_diverged(format!(
                "journal certification conflicts with retained acknowledgement floor for shard {} sequence {sequence}",
                shard.0
            )));
        }
    }
    if let Some(floor) = prefix_floor.as_ref()
        && floor.journal_sequence == next_sequence.saturating_sub(1)
        && !floor.state_fingerprint.is_empty()
    {
        let expected = DfsMetadataPrefixProof::from_digest(
            floor.journal_sequence,
            floor.state_fingerprint.clone(),
        )
        .ok_or_else(|| {
            MetadataBackendError::replica_diverged(
                "retained acknowledgement does not contain a current exact prefix".into(),
            )
        })?;
        if prefix != Some(&expected) {
            return Err(MetadataBackendError::replica_diverged(
                "journal certification differs from retained exact acknowledgement".into(),
            ));
        }
    }
    Ok(())
}

/// Append one already-encoded immutable frame without serializing its entry
/// again. The segment retains its existing little-endian length prefix; the
/// frame bytes are exactly the current SOBJ payload inside that envelope.
pub(crate) fn append_segmented_journal_frame_with_record_bytes(
    root: &Path,
    frame: &DfsMetadataJournalFrame<'_>,
) -> Result<u64> {
    append_segmented_journal_exact_frame_with_record_bytes(root, frame.entry(), frame.exact_bytes())
}

#[tracing::instrument(
    target = "sinomo.perf",
    level = "trace",
    name = "dfs.journal.append_one",
    skip_all,
    fields(stage = "dfs.journal.append_one")
)]
pub(crate) fn append_segmented_journal_exact_frame_with_record_bytes(
    root: &Path,
    entry: &DfsMetadataJournalEntry,
    exact_bytes: &[u8],
) -> Result<u64> {
    let segment_id = journal_segment_id(entry.sequence);
    let path = root
        .join("journal")
        .join("segments")
        .join(format!("{segment_id:020}.slog"));
    if let Some(parent) = path.parent() {
        create_dir_all(parent.to_owned())?;
    }
    let mut append = durable_file::open_file_for_append_with_options(
        &path,
        durable_file::DurableAppendFileOptions::new().without_parent_sync(),
    )
    .map_err(metadata_durable_file_error)?;
    let cache_before_append = JournalSegmentFileSnapshot::from_file(&append.file).ok();
    let record_bytes = append_journal_records(&mut append.file, &path, &[exact_bytes])?;
    if append.created
        && let Some(parent) = path.parent()
    {
        sync_dir(parent)?;
    }
    let cache_after_append = JournalSegmentFileSnapshot::from_file(&append.file).ok();
    cache_journal_segment_durable_append(
        &path,
        append.created,
        cache_before_append,
        cache_after_append,
        &[(entry, record_bytes)],
    );
    Ok(record_bytes)
}

#[tracing::instrument(
    target = "sinomo.perf",
    level = "trace",
    name = "dfs.journal.append",
    skip_all,
    fields(stage = "dfs.journal.append")
)]
pub(crate) fn append_segmented_journal_exact_frames(
    root: &Path,
    frames: &[(&DfsMetadataJournalEntry, &[u8])],
    encode_ms: u128,
) -> Result<()> {
    let Some((first, _)) = frames.first() else {
        return Ok(());
    };
    let trace_started = Instant::now();
    let segment_id = journal_segment_id(first.sequence);
    let path = root
        .join("journal")
        .join("segments")
        .join(format!("{segment_id:020}.slog"));
    let create_dir_started = Instant::now();
    if let Some(parent) = path.parent() {
        create_dir_all(parent.to_owned())?;
    }
    let create_dir_ms = create_dir_started.elapsed().as_millis();
    let mut record_lengths = Vec::with_capacity(frames.len());
    let mut records_len = 0_u64;
    for (entry, bytes) in frames {
        if journal_segment_id(entry.sequence) != segment_id {
            return Err(MetadataBackendError::Serde(format!(
                "metadata journal batch tried to append entry {} to segment {segment_id}",
                entry.sequence
            )));
        }
        let length = u32::try_from(bytes.len()).map_err(|_| {
            MetadataBackendError::Serde(format!(
                "metadata journal entry {} is too large: {} bytes",
                entry.sequence,
                bytes.len()
            ))
        })?;
        let record_len = 4_u64.saturating_add(u64::from(length));
        records_len = records_len.saturating_add(record_len);
        record_lengths.push(record_len);
    }
    let write_sync_started = Instant::now();
    let mut append = durable_file::open_file_for_append_with_options(
        &path,
        durable_file::DurableAppendFileOptions::new().without_parent_sync(),
    )
    .map_err(metadata_durable_file_error)?;
    let existed = !append.created;
    let cache_before_append = JournalSegmentFileSnapshot::from_file(&append.file).ok();
    let record_slices = frames.iter().map(|(_, bytes)| *bytes).collect::<Vec<_>>();
    let written_record_bytes = append_journal_records(&mut append.file, &path, &record_slices)?;
    let write_sync_ms = write_sync_started.elapsed().as_millis();
    let sync_dir_started = Instant::now();
    if append.created
        && let Some(parent) = path.parent()
    {
        sync_dir(parent)?;
    }
    let sync_dir_ms = sync_dir_started.elapsed().as_millis();
    let cache_after_append = JournalSegmentFileSnapshot::from_file(&append.file).ok();
    if written_record_bytes == records_len {
        let cache_records = frames
            .iter()
            .map(|(entry, _)| *entry)
            .zip(record_lengths)
            .collect::<Vec<_>>();
        cache_journal_segment_durable_append(
            &path,
            append.created,
            cache_before_append,
            cache_after_append,
            &cache_records,
        );
    }
    let traceparent = crate::current_dfs_traceparent().unwrap_or_default();
    tracing::trace!(
        target: "sinomo.dfs.perf",
        traceparent = %traceparent,
        entries = frames.len(),
        bytes = records_len,
        segment = segment_id,
        existed,
        create_dir_ms,
        encode_ms,
        write_sync_ms,
        sync_dir_ms,
        total_ms = trace_started.elapsed().as_millis(),
        "metadata-journal-segment"
    );
    Ok(())
}

pub(crate) fn write_local_journal_frame_with_bytes(
    root: &Path,
    next_sequence: &mut u64,
    frame: &DfsMetadataJournalFrame<'_>,
) -> Result<u64> {
    if frame.entry().sequence != (*next_sequence).max(1) {
        return Err(MetadataBackendError::Serde(
            "local journal frame lost its exact writer slot".into(),
        ));
    }
    let record_bytes = append_segmented_journal_frame_with_record_bytes(root, frame)?;
    *next_sequence = frame.entry().sequence.saturating_add(1);
    Ok(record_bytes)
}

#[cfg(test)]
pub(crate) fn persist_replayed_journal_entries(
    root: &Path,
    entries: &[&DfsMetadataJournalEntry],
    persisted_through_sequence: u64,
) -> Result<Option<u64>> {
    let mut entries_by_segment = BTreeMap::<u64, Vec<&DfsMetadataJournalEntry>>::new();
    let mut max_sequence = 0u64;
    for entry in entries {
        entries_by_segment
            .entry(journal_segment_id(entry.sequence))
            .or_default()
            .push(*entry);
        max_sequence = max_sequence.max(entry.sequence);
    }
    for entries in entries_by_segment.values_mut() {
        entries.sort_by_key(|entry| entry.sequence);
        let mut missing = Vec::new();
        for entry in entries.iter().copied() {
            // The caller holds the shard's inter-process metadata write lock
            // and has already refreshed and verified this exact durable
            // prefix. A strictly newer, contiguous entry therefore cannot
            // exist yet; checking it by loading the whole segment made every
            // steady-state replica ingest O(total journal bytes).
            if entry.sequence > persisted_through_sequence {
                missing.push(entry);
                continue;
            }
            if let Some(existing) = segmented_journal_entry_at_sequence(root, entry.sequence)? {
                if existing == *entry {
                    continue;
                }
                return Err(MetadataBackendError::replica_diverged(format!(
                    "metadata journal sequence {} already exists with a different entry while replaying shard {}",
                    entry.sequence, entry.shard_id.0
                )));
            }
            return Err(MetadataBackendError::replica_diverged(format!(
                "metadata journal sequence {} is within durable prefix {} but is missing while replaying shard {}",
                entry.sequence, persisted_through_sequence, entry.shard_id.0
            )));
        }
        tests::append_segmented_journal_entries(root, &missing)?;
    }
    Ok((max_sequence > 0).then_some(max_sequence))
}

pub(crate) fn persist_replayed_journal_frames(
    root: &Path,
    frames: &[(
        &DfsMetadataJournalEntry,
        &crate::DfsMetadataJournalFrameRecord,
    )],
    persisted_through_sequence: u64,
) -> Result<Option<u64>> {
    let mut frames_by_segment = BTreeMap::<
        u64,
        Vec<(
            &DfsMetadataJournalEntry,
            &crate::DfsMetadataJournalFrameRecord,
        )>,
    >::new();
    let mut max_sequence = 0u64;
    for (entry, frame) in frames {
        if entry.sequence != frame.sequence {
            return Err(MetadataBackendError::Serde(format!(
                "metadata journal frame sequence {} does not match decoded entry {}",
                frame.sequence, entry.sequence
            )));
        }
        frames_by_segment
            .entry(journal_segment_id(entry.sequence))
            .or_default()
            .push((*entry, *frame));
        max_sequence = max_sequence.max(entry.sequence);
    }
    for frames in frames_by_segment.values_mut() {
        frames.sort_by_key(|(entry, _)| entry.sequence);
        let mut missing = Vec::new();
        for (entry, frame) in frames.iter().copied() {
            if entry.sequence > persisted_through_sequence {
                missing.push((entry, frame.exact_bytes.as_ref()));
                continue;
            }
            let existing = read_segmented_journal_frame_at_sequence(root, entry.sequence)?
                .ok_or_else(|| {
                    MetadataBackendError::replica_diverged(format!(
                        "metadata journal sequence {} is within durable prefix {} but is missing while replaying shard {}",
                        entry.sequence, persisted_through_sequence, entry.shard_id.0
                    ))
                })?;
            if existing.exact_bytes() != frame.exact_bytes.as_ref() {
                return Err(MetadataBackendError::replica_diverged(format!(
                    "metadata journal sequence {} already exists with different exact bytes while replaying shard {}",
                    entry.sequence, entry.shard_id.0
                )));
            }
        }
        #[cfg(test)]
        let appended_frames = !missing.is_empty();
        append_segmented_journal_exact_frames(root, &missing, 0)?;
        #[cfg(test)]
        if appended_frames {
            maybe_fail_replayed_frame_append_after_segment()?;
        }
    }
    Ok((max_sequence > 0).then_some(max_sequence))
}

pub(crate) fn journal_entries_since_limited_with_next_sequence(
    root: &Path,
    after_sequence: u64,
    max_entries: usize,
    next_sequence: u64,
) -> Result<Vec<DfsMetadataJournalEntry>> {
    segmented_journal_entries_since_limited(root, after_sequence, max_entries, Some(next_sequence))
}

/// Read the durable segmented tail without trusting an in-memory or deferred
/// journal-head hint. Bounded migration uses this to replay object IDs changed
/// after its ordered catalog cursor advanced.
pub(crate) fn durable_journal_entries_since_limited(
    root: &Path,
    after_sequence: u64,
    max_entries: usize,
) -> Result<Vec<DfsMetadataJournalEntry>> {
    segmented_journal_entries_since_limited(root, after_sequence, max_entries, None)
}

pub(crate) fn bounded_metadata_journal_batch(
    shard_id: DfsMetadataShardId,
    after_sequence: u64,
    mut entries: Vec<DfsMetadataJournalEntry>,
    max_entries: usize,
    max_encoded_bytes: usize,
) -> Result<Option<DfsMetadataJournalBatch>> {
    if max_entries == 0 {
        return Ok(None);
    }
    entries.truncate(max_entries);
    if entries.is_empty() {
        return Ok(None);
    }

    let max_encoded_bytes = max_encoded_bytes.max(1);
    let selected_len = if entries.len() == 1 {
        1
    } else if metadata_journal_batch_prefix_encoded_len(&entries)? <= max_encoded_bytes {
        entries.len()
    } else {
        // The full prefix is too large. One entry is deliberately always
        // allowed so replication can make progress even when a single entry
        // exceeds the configured byte limit. Journal entries in a batch all
        // belong to one shard, so encoded prefix size grows monotonically.
        let mut accepted = 1usize;
        let mut rejected = entries.len();
        while accepted + 1 < rejected {
            let candidate_len = accepted + (rejected - accepted) / 2;
            if metadata_journal_batch_prefix_encoded_len(&entries[..candidate_len])?
                <= max_encoded_bytes
            {
                accepted = candidate_len;
            } else {
                rejected = candidate_len;
            }
        }
        accepted
    };

    entries.truncate(selected_len);
    let next_after_sequence = entries
        .last()
        .map(|entry| entry.sequence)
        .unwrap_or(after_sequence);
    Ok(Some(DfsMetadataJournalBatch {
        shard_id: entries
            .first()
            .map(|entry| entry.shard_id.clone())
            .unwrap_or(shard_id),
        entries,
        next_after_sequence,
    }))
}

#[derive(Serialize)]
struct BorrowedMetadataJournalBatch<'a> {
    shard_id: &'a DfsMetadataShardId,
    entries: &'a [DfsMetadataJournalEntry],
    next_after_sequence: u64,
}

fn metadata_journal_batch_prefix_encoded_len(entries: &[DfsMetadataJournalEntry]) -> Result<usize> {
    let last = entries
        .last()
        .expect("metadata journal prefix must contain an entry");
    sinomo_object_serde::to_bytes(
        &BorrowedMetadataJournalBatch {
            shard_id: &last.shard_id,
            entries,
            next_after_sequence: last.sequence,
        },
        1,
        LengthMode::U32,
    )
    .map(|bytes| bytes.len())
    .map_err(|source| MetadataBackendError::Serde(source.to_string()))
}

#[cfg(test)]
pub(crate) fn metadata_journal_batch_encoded_len(batch: &DfsMetadataJournalBatch) -> Result<usize> {
    sinomo_object_serde::to_bytes(batch, 1, LengthMode::U32)
        .map(|bytes| bytes.len())
        .map_err(|source| MetadataBackendError::Serde(source.to_string()))
}

pub(crate) fn segmented_journal_entry_at_sequence(
    root: &Path,
    sequence: u64,
) -> Result<Option<DfsMetadataJournalEntry>> {
    let segment_id = journal_segment_id(sequence);
    let path = root
        .join("journal")
        .join("segments")
        .join(format!("{segment_id:020}.slog"));
    if !path.exists() {
        return Ok(None);
    }
    read_segmented_journal_file_at_sequence(&path, sequence)
}

fn segmented_journal_entries_since_limited(
    root: &Path,
    after_sequence: u64,
    max_entries: usize,
    known_next_sequence: Option<u64>,
) -> Result<Vec<DfsMetadataJournalEntry>> {
    segmented_journal_entries_since_limited_with_byte_limit(
        root,
        after_sequence,
        max_entries,
        known_next_sequence,
        None,
    )
    .map(|batch| batch.entries)
}

#[derive(Default)]
pub(crate) struct DfsDurableJournalRead {
    pub(crate) entries: Vec<DfsMetadataJournalEntry>,
    pub(crate) encoded_bytes: u64,
    pub(crate) byte_limit_reached: bool,
}

struct JournalReadByteBudget {
    limit: u64,
    used: u64,
    stopped: bool,
}

fn segmented_journal_entries_since_limited_with_byte_limit(
    root: &Path,
    after_sequence: u64,
    max_entries: usize,
    known_next_sequence: Option<u64>,
    max_bytes: Option<usize>,
) -> Result<DfsDurableJournalRead> {
    let trace_started = Instant::now();
    if max_entries == 0 {
        return Ok(DfsDurableJournalRead::default());
    }
    let has_entries = if let Some(next_sequence) = known_next_sequence {
        after_sequence < next_sequence.saturating_sub(1)
    } else {
        // The durable-tail path is specifically used to observe appends made
        // by another retained backend instance. `head.sobj` is a deferred
        // hint and may lag a fully durable segment append, so it cannot prove
        // this scan is empty. Listing the sequence-selected segment is the
        // bounded source-of-truth check.
        true
    };
    if !has_entries {
        return Ok(DfsDurableJournalRead::default());
    }
    let segments_dir = root.join("journal").join("segments");
    if !segments_dir.exists() {
        return Ok(DfsDurableJournalRead::default());
    }
    let first_segment = journal_segment_id(after_sequence.saturating_add(1));
    let mut paths = BTreeMap::new();
    for entry in read_dir(&segments_dir)? {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("slog") {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };
        let Ok(segment_id) = stem.parse::<u64>() else {
            continue;
        };
        if segment_id >= first_segment {
            paths.insert(segment_id, path);
        }
    }
    let candidate_segments = paths.len();
    let mut opened_segments = 0_usize;
    let mut entries = Vec::new();
    let mut byte_budget = max_bytes.map(|limit| JournalReadByteBudget {
        limit: limit.max(1) as u64,
        used: 0,
        stopped: false,
    });
    for path in paths.into_values() {
        let remaining = max_entries.saturating_sub(entries.len());
        if remaining == 0 {
            break;
        }
        opened_segments = opened_segments.saturating_add(1);
        entries.extend(read_segmented_journal_file_since_limited_with_byte_budget(
            &path,
            after_sequence,
            remaining,
            byte_budget.as_mut(),
        )?);
        if entries.len() >= max_entries
            || byte_budget
                .as_ref()
                .is_some_and(|budget| budget.stopped || budget.used >= budget.limit)
        {
            break;
        }
    }
    entries.sort_by_key(|entry| entry.sequence);
    let mut distinct = Vec::<DfsMetadataJournalEntry>::with_capacity(entries.len());
    for entry in entries {
        if let Some(previous) = distinct.last()
            && previous.sequence == entry.sequence
        {
            if previous != &entry {
                return Err(MetadataBackendError::Serde(format!(
                    "metadata journal contains conflicting duplicate sequence {} under {}",
                    entry.sequence,
                    root.display()
                )));
            }
            continue;
        }
        distinct.push(entry);
    }
    distinct.truncate(max_entries);
    let traceparent = crate::current_dfs_traceparent().unwrap_or_default();
    tracing::trace!(
        target: "sinomo.dfs.perf",
        traceparent = %traceparent,
        after_sequence,
        max_entries,
        candidate_segments,
        opened_segments,
        returned_entries = distinct.len(),
        total_ms = trace_started.elapsed().as_millis(),
        "metadata-journal-suffix-scan"
    );
    Ok(DfsDurableJournalRead {
        entries: distinct,
        encoded_bytes: byte_budget.as_ref().map_or(0, |budget| budget.used),
        byte_limit_reached: byte_budget
            .as_ref()
            .is_some_and(|budget| budget.stopped || budget.used >= budget.limit),
    })
}

fn journal_segment_file_matches_index(
    file: &fs::File,
    path: &Path,
    index: &JournalSegmentIndex,
) -> Result<bool> {
    let metadata = file.metadata().map_err(|source| MetadataBackendError::Io {
        path: path.to_owned(),
        source,
    })?;
    Ok(index.content_generation != 0
        && index.identity == JournalSegmentFileIdentity::from_file_metadata(file, &metadata)
        && index.observed_file_len == metadata.len()
        && index.modified == metadata.modified().ok()
        && index.indexed_bytes == metadata.len())
}

struct IndexedJournalSegment {
    file: fs::File,
    file_bytes: u64,
    index: JournalSegmentIndex,
    refreshed_entries: BTreeMap<u64, DfsMetadataJournalEntry>,
    cache_hit: bool,
    index_extended: bool,
    index_rebuilt: bool,
    decoded_records: usize,
    scanned_bytes: u64,
}

/// Open one segment and return its reusable sequence-to-record-offset index.
///
/// Both typed suffix reads and exact-frame replication reads use this path so
/// append extension, pathname replacement, and torn journal tail handling have
/// one implementation. A warm exact-frame lookup therefore seeks straight to
/// the indexed length-prefixed record instead of decoding the segment prefix.
fn open_indexed_journal_segment(path: &Path) -> Result<IndexedJournalSegment> {
    let mut file = fs::File::open(path).map_err(|source| MetadataBackendError::Io {
        path: path.to_owned(),
        source,
    })?;
    let metadata = file.metadata().map_err(|source| MetadataBackendError::Io {
        path: path.to_owned(),
        source,
    })?;
    let file_bytes = metadata.len();
    let identity = JournalSegmentFileIdentity::from_file_metadata(&file, &metadata);
    let modified = metadata.modified().ok();
    let cached = cached_journal_segment_index(path);
    let cache_hit = cached.as_ref().is_some_and(|cached| {
        cached.identity == identity
            && cached.observed_file_len == file_bytes
            && cached.modified == modified
    });
    let index_extended = cached.as_ref().is_some_and(|cached| {
        // Unix inode/device and Windows volume/file-index pairs survive an
        // append while distinguishing replacement files. Other platforms only
        // expose the weaker creation timestamp, so retain the mtime fence
        // there instead of extending a possibly stale offset index.
        cached.identity == identity
            && (cached.identity.supports_append_extension() || cached.modified == modified)
            && cached.indexed_bytes == cached.observed_file_len
            && file_bytes > cached.observed_file_len
    });
    let mut index = if cache_hit || index_extended {
        cached.expect("cache hit or extension requires a cached index")
    } else {
        JournalSegmentIndex {
            content_generation: 0,
            identity: identity.clone(),
            observed_file_len: 0,
            modified: None,
            indexed_bytes: 0,
            entries: Arc::new(Vec::new()),
            sequences_monotonic: true,
        }
    };
    let index_rebuilt = !cache_hit && !index_extended;
    let index_scan_start = if cache_hit {
        None
    } else if index_extended {
        Some(index.indexed_bytes)
    } else {
        Some(0)
    };
    let mut refreshed_entries = BTreeMap::new();
    let mut decoded_records = 0_usize;
    let mut scanned_bytes = 0_u64;
    if let Some(index_scan_start) = index_scan_start {
        file.seek(SeekFrom::Start(index_scan_start))
            .map_err(|source| MetadataBackendError::Io {
                path: path.to_owned(),
                source,
            })?;
        loop {
            let record_start =
                file.stream_position()
                    .map_err(|source| MetadataBackendError::Io {
                        path: path.to_owned(),
                        source,
                    })?;
            let Some(entry) = read_next_segmented_journal_entry(&mut file, path)? else {
                index.indexed_bytes =
                    file.stream_position()
                        .map_err(|source| MetadataBackendError::Io {
                            path: path.to_owned(),
                            source,
                        })?;
                break;
            };
            decoded_records = decoded_records.saturating_add(1);
            #[cfg(test)]
            METADATA_JOURNAL_DECODED_RECORDS
                .set(METADATA_JOURNAL_DECODED_RECORDS.get().saturating_add(1));
            if index
                .entries
                .last()
                .is_some_and(|previous| previous.sequence > entry.sequence)
            {
                index.sequences_monotonic = false;
            }
            Arc::make_mut(&mut index.entries).push(JournalSegmentEntryOffset {
                sequence: entry.sequence,
                record_start,
            });
            refreshed_entries.insert(record_start, entry);
        }
        scanned_bytes = index.indexed_bytes.saturating_sub(index_scan_start);
        index.identity = identity;
        index.observed_file_len = file_bytes;
        index.modified = modified;
        index = cache_journal_segment_index(path.to_owned(), index);
    }

    Ok(IndexedJournalSegment {
        file,
        file_bytes,
        index,
        refreshed_entries,
        cache_hit,
        index_extended,
        index_rebuilt,
        decoded_records,
        scanned_bytes,
    })
}

pub(crate) fn read_segmented_journal_file_since_limited(
    path: &Path,
    after_sequence: u64,
    max_entries: usize,
) -> Result<Vec<DfsMetadataJournalEntry>> {
    read_segmented_journal_file_since_limited_with_byte_budget(
        path,
        after_sequence,
        max_entries,
        None,
    )
}

fn read_segmented_journal_file_since_limited_with_byte_budget(
    path: &Path,
    after_sequence: u64,
    max_entries: usize,
    byte_budget: Option<&mut JournalReadByteBudget>,
) -> Result<Vec<DfsMetadataJournalEntry>> {
    #[cfg(test)]
    METADATA_JOURNAL_SEGMENT_SCAN_CALLS
        .set(METADATA_JOURNAL_SEGMENT_SCAN_CALLS.get().saturating_add(1));
    let trace_started = Instant::now();
    let IndexedJournalSegment {
        mut file,
        file_bytes,
        index,
        mut refreshed_entries,
        cache_hit,
        index_extended,
        index_rebuilt,
        mut decoded_records,
        mut scanned_bytes,
    } = open_indexed_journal_segment(path)?;

    let selected_owned = (!index.sequences_monotonic).then(|| {
        let mut selected = index
            .entries
            .iter()
            .copied()
            .filter(|entry| entry.sequence > after_sequence)
            .collect::<Vec<_>>();
        selected.sort_by_key(|entry| (entry.sequence, entry.record_start));
        selected.truncate(max_entries);
        selected
    });
    let selected = selected_owned.as_deref().unwrap_or_else(|| {
        let first_entry = index
            .entries
            .partition_point(|entry| entry.sequence <= after_sequence);
        &index.entries[first_entry
            ..first_entry
                .saturating_add(max_entries)
                .min(index.entries.len())]
    });
    let selected = if let Some(budget) = byte_budget {
        let mut count = 0;
        for offset in selected {
            let bytes = index.record_bytes(*offset).ok_or_else(|| {
                MetadataBackendError::Serde(
                    "journal index has no complete byte range for selected record".into(),
                )
            })?;
            // One oversized entry remains permitted so a large namespace
            // transaction cannot permanently block replay.
            if budget.used > 0 && budget.used.saturating_add(bytes) > budget.limit {
                budget.stopped = true;
                break;
            }
            budget.used = budget.used.saturating_add(bytes);
            count += 1;
        }
        &selected[..count]
    } else {
        selected
    };
    let all_selected_were_refreshed = selected
        .iter()
        .all(|offset| refreshed_entries.contains_key(&offset.record_start));
    let use_decoded_cache =
        !selected.is_empty() && journal_segment_file_matches_index(&file, path, &index)?;
    let mut decoded_cache_hit = false;
    let entries = if all_selected_were_refreshed {
        let entries = selected
            .iter()
            .filter_map(|offset| refreshed_entries.remove(&offset.record_start))
            .collect::<Vec<_>>();
        if use_decoded_cache && journal_segment_file_matches_index(&file, path, &index)? {
            cache_journal_segment_decoded_entries(path, &index, selected, &entries);
        }
        entries
    } else if use_decoded_cache
        && let Some(cached) = cached_journal_segment_decoded_entries(path, &index, selected)
        && journal_segment_file_matches_index(&file, path, &index)?
    {
        decoded_cache_hit = true;
        #[cfg(test)]
        METADATA_JOURNAL_DECODED_CACHE_HITS
            .set(METADATA_JOURNAL_DECODED_CACHE_HITS.get().saturating_add(1));
        cached
            .into_iter()
            .map(|entry| entry.as_ref().clone())
            .collect()
    } else if !index.sequences_monotonic {
        let mut entries = Vec::with_capacity(selected.len());
        for expected in selected {
            file.seek(SeekFrom::Start(expected.record_start))
                .map_err(|source| MetadataBackendError::Io {
                    path: path.to_owned(),
                    source,
                })?;
            let Some(entry) = read_next_segmented_journal_entry(&mut file, path)? else {
                return Err(MetadataBackendError::Serde(format!(
                    "metadata journal entry {} disappeared while reading non-monotonic segment index at {}",
                    expected.sequence,
                    path.display()
                )));
            };
            decoded_records = decoded_records.saturating_add(1);
            #[cfg(test)]
            METADATA_JOURNAL_DECODED_RECORDS
                .set(METADATA_JOURNAL_DECODED_RECORDS.get().saturating_add(1));
            if entry.sequence != expected.sequence {
                return Err(MetadataBackendError::Serde(format!(
                    "non-monotonic metadata journal segment index at {} expected sequence {}, found {}",
                    path.display(),
                    expected.sequence,
                    entry.sequence
                )));
            }
            let record_end = file.stream_position().unwrap_or(expected.record_start);
            scanned_bytes =
                scanned_bytes.saturating_add(record_end.saturating_sub(expected.record_start));
            entries.push(entry);
        }
        if journal_segment_file_matches_index(&file, path, &index)? {
            cache_journal_segment_decoded_entries(path, &index, selected, &entries);
        }
        entries
    } else if let Some(first) = selected.first() {
        file.seek(SeekFrom::Start(first.record_start))
            .map_err(|source| MetadataBackendError::Io {
                path: path.to_owned(),
                source,
            })?;
        let selected_scan_start = first.record_start;
        let mut entries = Vec::with_capacity(selected.len());
        for expected in selected {
            let Some(entry) = read_next_segmented_journal_entry(&mut file, path)? else {
                return Err(MetadataBackendError::Serde(format!(
                    "metadata journal entry {} disappeared while reading cached segment index at {}",
                    expected.sequence,
                    path.display()
                )));
            };
            decoded_records = decoded_records.saturating_add(1);
            #[cfg(test)]
            METADATA_JOURNAL_DECODED_RECORDS
                .set(METADATA_JOURNAL_DECODED_RECORDS.get().saturating_add(1));
            if entry.sequence != expected.sequence {
                return Err(MetadataBackendError::Serde(format!(
                    "metadata journal segment index at {} expected sequence {}, found {}",
                    path.display(),
                    expected.sequence,
                    entry.sequence
                )));
            }
            entries.push(entry);
        }
        let selected_scan_end = file.stream_position().unwrap_or(selected_scan_start);
        scanned_bytes =
            scanned_bytes.saturating_add(selected_scan_end.saturating_sub(selected_scan_start));
        if journal_segment_file_matches_index(&file, path, &index)? {
            cache_journal_segment_decoded_entries(path, &index, selected, &entries);
        }
        entries
    } else {
        Vec::new()
    };
    let traceparent = crate::current_dfs_traceparent().unwrap_or_default();
    tracing::trace!(
        target: "sinomo.dfs.perf",
        traceparent = %traceparent,
        path = %path.display(),
        after_sequence,
        max_entries,
        file_bytes,
        scanned_bytes,
        decoded_records,
        returned_entries = entries.len(),
        cache_hit,
        decoded_cache_hit,
        index_extended,
        index_rebuilt,
        total_ms = trace_started.elapsed().as_millis(),
        "metadata-journal-segment-scan"
    );
    Ok(entries)
}

fn read_segmented_journal_file_at_sequence(
    path: &Path,
    sequence: u64,
) -> Result<Option<DfsMetadataJournalEntry>> {
    Ok(
        read_segmented_journal_file_since_limited(path, sequence.saturating_sub(1), 1)?
            .into_iter()
            .next()
            .filter(|entry| entry.sequence == sequence),
    )
}

/// Read exact immutable SOBJ frames for an arbitrary ordered set of journal
/// sequences while preserving the caller's order.
///
/// Requests are grouped by segment. Each present segment performs one indexed
/// range lookup, then decodes only requested length-prefixed records. This is
/// deliberately separate from the bounded typed-entry cache: exact replication
/// bytes must come from the durable record rather than a re-encoding.
pub(crate) fn read_segmented_journal_frames_at_sequences(
    root: &Path,
    sequences: &[u64],
) -> Result<Vec<Option<DfsMetadataJournalFrame<'static>>>> {
    let trace_started = Instant::now();
    let mut frames = std::iter::repeat_with(|| None)
        .take(sequences.len())
        .collect::<Vec<_>>();
    let mut requests_by_segment = BTreeMap::<u64, Vec<(usize, u64)>>::new();
    for (output_index, sequence) in sequences.iter().copied().enumerate() {
        requests_by_segment
            .entry(journal_segment_id(sequence))
            .or_default()
            .push((output_index, sequence));
    }

    let requested_segments = requests_by_segment.len();
    let mut opened_segments = 0_usize;
    let mut decoded_frames = 0_usize;
    let mut returned_frames = 0_usize;
    let mut scanned_bytes = 0_u64;
    for (segment_id, mut requests) in requests_by_segment {
        let path = root
            .join("journal")
            .join("segments")
            .join(format!("{segment_id:020}.slog"));
        if !path.is_file() {
            continue;
        }
        opened_segments = opened_segments.saturating_add(1);
        #[cfg(test)]
        METADATA_JOURNAL_EXACT_SEGMENT_OPENS
            .set(METADATA_JOURNAL_EXACT_SEGMENT_OPENS.get().saturating_add(1));
        requests.sort_by_key(|(_, sequence)| *sequence);

        let IndexedJournalSegment {
            mut file,
            index,
            cache_hit,
            index_extended,
            index_rebuilt,
            scanned_bytes: segment_scanned_bytes,
            ..
        } = open_indexed_journal_segment(&path)?;
        scanned_bytes = scanned_bytes.saturating_add(segment_scanned_bytes);
        let segment_requested_frames = requests.len();
        let first_requested_sequence = requests
            .first()
            .map(|(_, sequence)| *sequence)
            .expect("a grouped exact-frame request cannot be empty");
        #[cfg(test)]
        METADATA_JOURNAL_EXACT_INDEX_LOOKUPS
            .set(METADATA_JOURNAL_EXACT_INDEX_LOOKUPS.get().saturating_add(1));
        let mut indexed_position = index
            .entries
            .partition_point(|entry| entry.sequence < first_requested_sequence);
        let mut segment_decoded_frames = 0_usize;
        let mut segment_returned_frames = 0_usize;
        for (output_index, requested_sequence) in requests {
            while index
                .entries
                .get(indexed_position)
                .is_some_and(|entry| entry.sequence < requested_sequence)
            {
                indexed_position = indexed_position.saturating_add(1);
            }
            let Some(expected) = index
                .entries
                .get(indexed_position)
                .copied()
                .filter(|entry| entry.sequence == requested_sequence)
            else {
                continue;
            };
            file.seek(SeekFrom::Start(expected.record_start))
                .map_err(|source| MetadataBackendError::Io {
                    path: path.clone(),
                    source,
                })?;
            let Some(frame) = read_next_segmented_journal_frame(&mut file, &path)? else {
                return Err(MetadataBackendError::Serde(format!(
                    "metadata journal entry {} disappeared while reading exact frame from indexed segment at {}",
                    requested_sequence,
                    path.display()
                )));
            };
            segment_decoded_frames = segment_decoded_frames.saturating_add(1);
            decoded_frames = decoded_frames.saturating_add(1);
            #[cfg(test)]
            METADATA_JOURNAL_EXACT_FRAME_DECODES
                .set(METADATA_JOURNAL_EXACT_FRAME_DECODES.get().saturating_add(1));
            if frame.entry().sequence != requested_sequence {
                return Err(MetadataBackendError::Serde(format!(
                    "metadata journal segment index at {} expected exact frame sequence {}, found {}",
                    path.display(),
                    requested_sequence,
                    frame.entry().sequence
                )));
            }
            frames[output_index] = Some(frame);
            segment_returned_frames = segment_returned_frames.saturating_add(1);
            returned_frames = returned_frames.saturating_add(1);
        }
        let traceparent = crate::current_dfs_traceparent().unwrap_or_default();
        tracing::trace!(
            target: "sinomo.dfs.perf",
            traceparent = %traceparent,
            path = %path.display(),
            segment = segment_id,
            requested_frames = segment_requested_frames,
            decoded_frames = segment_decoded_frames,
            returned_frames = segment_returned_frames,
            scanned_bytes = segment_scanned_bytes,
            cache_hit,
            index_extended,
            index_rebuilt,
            "metadata-journal-exact-segment-read"
        );
    }
    let traceparent = crate::current_dfs_traceparent().unwrap_or_default();
    tracing::trace!(
        target: "sinomo.dfs.perf",
        traceparent = %traceparent,
        requested_frames = sequences.len(),
        requested_segments,
        opened_segments,
        decoded_frames,
        returned_frames,
        scanned_bytes,
        total_ms = trace_started.elapsed().as_millis(),
        "metadata-journal-exact-range-read"
    );
    Ok(frames)
}

/// Read one segmented entry while preserving the exact immutable SOBJ bytes
/// that were inside its durable length-prefixed record.
pub(crate) fn read_segmented_journal_frame_at_sequence(
    root: &Path,
    sequence: u64,
) -> Result<Option<DfsMetadataJournalFrame<'static>>> {
    Ok(
        read_segmented_journal_frames_at_sequences(root, &[sequence])?
            .pop()
            .flatten(),
    )
}

fn read_next_segmented_journal_entry(
    file: &mut fs::File,
    path: &Path,
) -> Result<Option<DfsMetadataJournalEntry>> {
    Ok(read_next_segmented_journal_frame(file, path)?.map(DfsMetadataJournalFrame::into_entry))
}

fn read_next_segmented_journal_frame(
    file: &mut fs::File,
    path: &Path,
) -> Result<Option<DfsMetadataJournalFrame<'static>>> {
    let Some(record) = durable_file::read_length_prefixed_record(
        file,
        path,
        durable_file::DurableRecordReadOptions::new(durable_file::DurableLengthEndian::Little),
    )
    .map_err(metadata_durable_file_error)?
    else {
        return Ok(None);
    };
    let frame = match DfsMetadataJournalFrame::decode_exact(record.bytes) {
        Ok(frame) => frame,
        Err(source) => {
            return Err(MetadataBackendError::replica_diverged(format!(
                "failed to decode segmented journal entry at {}: {source}",
                path.display()
            )));
        }
    };
    Ok(Some(frame))
}

pub(crate) fn journal_segment_id(sequence: u64) -> u64 {
    sequence
        .saturating_sub(1)
        .checked_div(JOURNAL_SEGMENT_ENTRY_LIMIT)
        .unwrap_or(0)
        .saturating_add(1)
}

/// Remove every journal record after `sequence`, for a write-set member
/// aligning with a new epoch's start. The segment holding the first later
/// record is cut just before it, and every later segment is removed. A
/// segment can repeat earlier records after later ones (a retried batch), so
/// the cut is at the first record past `sequence`, never after a later
/// duplicate. The caller holds the shard writer lock and has invalidated the
/// journal admission certificate.
pub(crate) fn truncate_segmented_journal_after(root: &Path, sequence: u64) -> Result<()> {
    for (segment_id, path) in segmented_journal_paths_descending(root)? {
        let first_sequence = segment_id
            .saturating_sub(1)
            .saturating_mul(JOURNAL_SEGMENT_ENTRY_LIMIT)
            .saturating_add(1);
        if first_sequence > sequence {
            remove_file_if_exists(&path)?;
            let mut cache = journal_segment_index_cache()
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            remove_cached_journal_segment(&mut cache, &path);
            continue;
        }
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|source| MetadataBackendError::Io {
                path: path.clone(),
                source,
            })?;
        let mut record_start = 0;
        let mut cut = None;
        while let Some(record) = durable_file::read_length_prefixed_record(
            &mut file,
            &path,
            durable_file::DurableRecordReadOptions::new(durable_file::DurableLengthEndian::Little),
        )
        .map_err(metadata_durable_file_error)?
        {
            let frame = DfsMetadataJournalFrame::decode_exact(record.bytes).map_err(|error| {
                MetadataBackendError::replica_diverged(format!(
                    "complete journal frame at {} is malformed: {error}",
                    path.display()
                ))
            })?;
            if frame.entry().sequence > sequence {
                cut = Some(record_start);
                break;
            }
            record_start = record.record_end;
        }
        if let Some(cut) = cut {
            file.set_len(cut).map_err(|source| MetadataBackendError::Io {
                path: path.clone(),
                source,
            })?;
            file.sync_all().map_err(|source| MetadataBackendError::Io {
                path: path.clone(),
                source,
            })?;
            if let Some(parent) = path.parent() {
                sync_dir(parent)?;
            }
        }
        let mut cache = journal_segment_index_cache()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        remove_cached_journal_segment(&mut cache, &path);
        // Earlier segments hold only earlier sequences.
        break;
    }
    Ok(())
}

/// Remove complete journal segments strictly older than an authoritative,
/// durable materialization checkpoint.
///
/// The segment containing `checkpoint_sequence` is deliberately retained even
/// when the checkpoint lands on its final entry. A replica whose cursor
/// predates the retained suffix will therefore receive a non-contiguous first
/// entry, reject it, and enter the existing authoritative snapshot-repair
/// path. Never pruning the checkpoint segment also keeps a bounded durable
/// witness for that gap when no newer writes exist.
pub(crate) fn prune_segmented_journal_before_checkpoint(
    root: &Path,
    checkpoint_sequence: u64,
) -> Result<usize> {
    #[cfg(test)]
    run_before_journal_prune_hook_for_test(root);
    if checkpoint_sequence == 0 {
        return Ok(0);
    }
    let retain_segment_id = journal_segment_id(checkpoint_sequence);
    if retain_segment_id <= 1 {
        return Ok(0);
    }
    let segments_dir = root.join("journal").join("segments");
    if !segments_dir.exists() {
        return Ok(0);
    }

    let mut removed_paths = Vec::new();
    for entry in read_dir(&segments_dir)? {
        let path = entry.path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("slog") {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };
        let Ok(segment_id) = stem.parse::<u64>() else {
            continue;
        };
        if segment_id >= retain_segment_id {
            continue;
        }
        remove_file_if_exists(&path)?;
        removed_paths.push(path);
    }
    if removed_paths.is_empty() {
        return Ok(0);
    }

    {
        let mut cache = journal_segment_index_cache()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for path in &removed_paths {
            remove_cached_journal_segment(&mut cache, path);
        }
    }
    sync_dir(&segments_dir)?;
    Ok(removed_paths.len())
}

#[cfg(test)]
mod tests {

    fn append_segmented_journal_entry(root: &Path, entry: &DfsMetadataJournalEntry) -> Result<()> {
        append_segmented_journal_entry_with_record_bytes(root, entry).map(|_| ())
    }
    /// Append and return the exact length-prefixed bytes written for this journal
    /// record. Callers may persist that already-measured size as rebuild replay
    /// pressure without encoding the authoritative mutation a second time.
    fn append_segmented_journal_entry_with_record_bytes(
        root: &Path,
        entry: &DfsMetadataJournalEntry,
    ) -> Result<u64> {
        let frame = DfsMetadataJournalFrame::encode(entry)?;
        append_segmented_journal_frame_with_record_bytes(root, &frame)
    }
    pub(super) fn append_segmented_journal_entries(
        root: &Path,
        entries: &[&DfsMetadataJournalEntry],
    ) -> Result<()> {
        let encode_started = Instant::now();
        let frames = entries
            .iter()
            .map(|entry| DfsMetadataJournalFrame::encode(entry))
            .collect::<Result<Vec<_>>>()?;
        let encode_ms = encode_started.elapsed().as_millis();
        let exact_frames = frames
            .iter()
            .map(|frame| (frame.entry(), frame.exact_bytes()))
            .collect::<Vec<_>>();
        append_segmented_journal_exact_frames(root, &exact_frames, encode_ms)
    }
    fn durable_journal_entries_since_limits(
        root: &Path,
        after_sequence: u64,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<DfsDurableJournalRead> {
        segmented_journal_entries_since_limited_with_byte_limit(
            root,
            after_sequence,
            max_entries,
            None,
            Some(max_bytes),
        )
    }

    use super::*;
    use std::io::Write as _;

    fn journal_cache_test_guard() -> std::sync::MutexGuard<'static, ()> {
        static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
        GUARD
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub(super) fn test_journal_entry(sequence: u64) -> DfsMetadataJournalEntry {
        let payload = crate::DfsPreparedOperationPayload::for_metadata_effects(
            crate::DfsPreparedOperationScope {
                file_system_id: crate::DfsFileSystemId::from_legacy_named(
                    "sifs-journal-suffix-test",
                )
                .expect("filesystem id"),
                shard_id: DfsMetadataShardId::new("services"),
                owner_epoch: 1,
                operation_id: format!("journal-test:{sequence}"),
            },
            Vec::new(),
            Vec::new(),
            Vec::new(),
            time::OffsetDateTime::UNIX_EPOCH,
        )
        .expect("complete current payload");
        let digest = crate::DfsPreparedOperationDigest::at_sequence(&payload, sequence)
            .expect("native digest");
        DfsMetadataJournalEntry {
            shard_id: digest.shard_id.clone(),
            sequence,
            created_at: time::OffsetDateTime::UNIX_EPOCH,
            mutation: DfsMetadataMutation::PrepareOperationV7 {
                file_system_id: digest.file_system_id.clone(),
                operation_id: digest.operation_id.clone(),
                record: Box::new(crate::DfsPreparedOperationRecord {
                    digest,
                    payload: Arc::new(payload),
                    status: crate::DfsPreparedOperationStatus::Prepared,
                    committed_entry: None,
                }),
            },
        }
    }

    #[test]
    fn immutable_journal_frame_roundtrips_exact_bytes_through_current_segment_format() {
        let _cache_guard = journal_cache_test_guard();
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-exact-journal-frame");
        let first = test_journal_entry(1);
        let first_frame = DfsMetadataJournalFrame::encode(&first).expect("encode first frame");
        let first_bytes = first_frame.exact_bytes().to_vec();
        let written = append_segmented_journal_frame_with_record_bytes(root.path(), &first_frame)
            .expect("append exact frame");
        assert_eq!(
            written,
            4_u64.saturating_add(first_frame.encoded_len() as u64),
            "the immutable frame remains inside the current little-endian length envelope"
        );

        let decoded = read_segmented_journal_frame_at_sequence(root.path(), 1)
            .expect("read exact frame")
            .expect("first frame");
        assert_eq!(decoded.entry(), &first);
        assert_eq!(decoded.exact_bytes(), first_bytes);
        assert_eq!(
            read_segmented_journal_file_since_limited(
                &root
                    .path()
                    .join("journal")
                    .join("segments")
                    .join(format!("{:020}.slog", 1)),
                0,
                1,
            )
            .expect("typed entry read"),
            vec![first],
            "existing typed readers must consume a raw-frame append"
        );

        let second = test_journal_entry(2);
        append_segmented_journal_entry(root.path(), &second).expect("typed entry append");
        let expected_second =
            DfsMetadataJournalFrame::encode(&second).expect("encode expected second frame");
        let decoded_second = read_segmented_journal_frame_at_sequence(root.path(), 2)
            .expect("read typed appended frame")
            .expect("second frame");
        assert_eq!(decoded_second.entry(), &second);
        assert_eq!(decoded_second.exact_bytes(), expected_second.exact_bytes());
    }

    #[test]
    fn durable_suffix_byte_budget_limits_records_and_permits_one_oversized_entry() {
        let _cache_guard = journal_cache_test_guard();
        let root = sinomo_testdir::TestDir::new("journal-suffix-byte-budget");
        let first = test_journal_entry(1);
        append_segmented_journal_entry(root.path(), &first).unwrap();
        let segment = root
            .path()
            .join("journal/segments/00000000000000000001.slog");
        let first_bytes = fs::metadata(&segment).unwrap().len();
        let second = test_journal_entry(2);
        let third = test_journal_entry(3);
        append_segmented_journal_entry(root.path(), &second).unwrap();
        append_segmented_journal_entry(root.path(), &third).unwrap();
        let batch =
            durable_journal_entries_since_limits(root.path(), 0, 10, first_bytes as usize).unwrap();
        assert_eq!(batch.entries, vec![first]);
        assert_eq!(batch.encoded_bytes, first_bytes);
        assert!(batch.byte_limit_reached);
        let oversized = durable_journal_entries_since_limits(root.path(), 1, 10, 1).unwrap();
        assert_eq!(oversized.entries, vec![second]);
        assert!(oversized.encoded_bytes > 1);
        assert!(oversized.byte_limit_reached);
        let rest = durable_journal_entries_since_limits(root.path(), 2, 10, 1024 * 1024).unwrap();
        assert_eq!(rest.entries, vec![third]);
        assert!(!rest.byte_limit_reached);
    }

    #[test]
    fn journal_certification_accepts_non_adjacent_exact_replays() {
        let _cache_guard = journal_cache_test_guard();
        let root = sinomo_testdir::TestDir::new("journal-certification-batch-replay");
        for sequence in [1, 2, 1, 2, 3] {
            append_segmented_journal_entry(root.path(), &test_journal_entry(sequence)).unwrap();
        }
        let path = root.join("journal/segments/00000000000000000001.slog");
        let before = fs::read(&path).unwrap();
        assert_eq!(certify_journal_sources(root.path(), None).unwrap().0, 4);
        assert_eq!(fs::read(&path).unwrap(), before);
    }

    #[test]
    fn journal_certification_rejects_non_adjacent_conflicting_replays() {
        let _cache_guard = journal_cache_test_guard();
        let root = sinomo_testdir::TestDir::new("journal-certification-conflicting-replay");
        for sequence in [1, 2] {
            append_segmented_journal_entry(root.path(), &test_journal_entry(sequence)).unwrap();
        }
        let mut conflicting = test_journal_entry(1);
        conflicting.created_at += time::Duration::seconds(1);
        append_segmented_journal_entry(root.path(), &conflicting).unwrap();
        let error = certify_journal_sources(root.path(), None).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("conflicting duplicate sequence 1")
        );
    }

    #[test]
    fn journal_certification_rejects_missing_unique_sequences_after_replay() {
        let _cache_guard = journal_cache_test_guard();
        let root = sinomo_testdir::TestDir::new("journal-certification-gap-after-replay");
        for sequence in [1, 2, 1, 4] {
            append_segmented_journal_entry(root.path(), &test_journal_entry(sequence)).unwrap();
        }
        let error = certify_journal_sources(root.path(), None).unwrap_err();
        assert!(error.to_string().contains("sequence discontinuity"));
    }

    #[test]
    fn durable_suffix_collapses_exact_duplicate_sequences() {
        let _cache_guard = journal_cache_test_guard();
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-duplicate-journal-sequence");
        let first = test_journal_entry(1);
        let second = test_journal_entry(2);
        append_segmented_journal_entry(root.path(), &first).expect("append first");
        append_segmented_journal_entry(root.path(), &first).expect("append exact retry");
        append_segmented_journal_entry(root.path(), &second).expect("append second");

        assert_eq!(
            durable_journal_entries_since_limited(root.path(), 0, 3)
                .expect("read deduplicated suffix"),
            vec![first, second]
        );
    }

    #[test]
    fn durable_suffix_filters_out_of_order_exact_retries_before_paging() {
        let _cache_guard = journal_cache_test_guard();
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-out-of-order-journal-retry");
        let first = test_journal_entry(1);
        let second = test_journal_entry(2);
        append_segmented_journal_entry(root.path(), &first).expect("append first");
        append_segmented_journal_entry(root.path(), &second).expect("append second");
        append_segmented_journal_entry(root.path(), &first).expect("append delayed exact retry");

        assert_eq!(
            durable_journal_entries_since_limited(root.path(), 0, 1)
                .expect("prime only the first decoded entry"),
            vec![first]
        );
        assert_eq!(
            durable_journal_entries_since_limited(root.path(), 1, 2)
                .expect("read suffix past physical sequence regression"),
            vec![second]
        );
    }

    #[test]
    fn durable_suffix_rejects_conflicting_duplicate_sequences() {
        let _cache_guard = journal_cache_test_guard();
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-conflicting-journal-sequence");
        let first = test_journal_entry(1);
        let mut conflicting = first.clone();
        conflicting.created_at += time::Duration::seconds(1);
        append_segmented_journal_entry(root.path(), &first).expect("append first");
        append_segmented_journal_entry(root.path(), &conflicting)
            .expect("append conflicting durable record");

        let error = durable_journal_entries_since_limited(root.path(), 0, 2)
            .expect_err("conflicting sequence must fail closed");
        assert!(
            error
                .to_string()
                .contains("conflicting duplicate sequence 1")
        );
    }

    #[test]
    fn indexed_exact_frame_range_seeks_once_and_decodes_only_requested_records() {
        let _cache_guard = journal_cache_test_guard();
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-indexed-exact-journal-range");
        for sequence in 1..=128 {
            append_segmented_journal_entry(root.path(), &test_journal_entry(sequence))
                .expect("append indexed journal entry");
        }
        let segment_path = root
            .join("journal")
            .join("segments")
            .join(format!("{:020}.slog", 1));

        // Rebuild once to make the warm-read contract independent of the
        // writer's append-index optimization.
        reset_journal_segment_index_cache();
        read_segmented_journal_file_since_limited(&segment_path, u64::MAX, 0)
            .expect("prime exact-frame offset index");
        reset_metadata_journal_decoded_records();
        reset_metadata_journal_exact_read_counters();

        let late = read_segmented_journal_frame_at_sequence(root.path(), 127)
            .expect("read warm late frame")
            .expect("late frame");
        assert_eq!(late.entry().sequence, 127);
        assert_eq!(metadata_journal_exact_segment_opens(), 1);
        assert_eq!(metadata_journal_exact_index_lookups(), 1);
        assert_eq!(metadata_journal_exact_frame_decodes(), 1);
        assert_eq!(
            metadata_journal_decoded_records(),
            0,
            "a warm late lookup must not decode the preceding 126 records"
        );

        reset_metadata_journal_exact_read_counters();
        let sequences = (121..=128).collect::<Vec<_>>();
        let frames = read_segmented_journal_frames_at_sequences(root.path(), &sequences)
            .expect("read warm exact range");
        assert_eq!(
            frames
                .iter()
                .map(|frame| frame.as_ref().map(|frame| frame.entry().sequence))
                .collect::<Vec<_>>(),
            sequences.into_iter().map(Some).collect::<Vec<_>>()
        );
        assert_eq!(
            metadata_journal_exact_segment_opens(),
            1,
            "one batch in one segment must reuse one file handle"
        );
        assert_eq!(
            metadata_journal_exact_index_lookups(),
            1,
            "one batch in one segment must select one indexed range"
        );
        assert_eq!(
            metadata_journal_exact_frame_decodes(),
            8,
            "exact decoding must scale with the selected batch, not its late offset"
        );
        assert_eq!(metadata_journal_decoded_records(), 0);
    }

    #[test]
    fn indexed_exact_frame_range_extends_after_current_append_and_groups_segments() {
        let _cache_guard = journal_cache_test_guard();
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-indexed-exact-journal-append");
        let first = test_journal_entry(1);
        append_segmented_journal_entry(root.path(), &first).expect("append first current entry");
        read_segmented_journal_frame_at_sequence(root.path(), 1)
            .expect("prime first exact frame")
            .expect("first exact frame");

        let second = test_journal_entry(2);
        append_segmented_journal_entry(root.path(), &second).expect("append current extension");
        let next_segment_sequence = JOURNAL_SEGMENT_ENTRY_LIMIT.saturating_add(1);
        let next_segment = test_journal_entry(next_segment_sequence);
        append_segmented_journal_entry(root.path(), &next_segment)
            .expect("append next segment entry");

        reset_metadata_journal_decoded_records();
        reset_metadata_journal_exact_read_counters();
        let sequences = [2, next_segment_sequence, 1];
        let frames = read_segmented_journal_frames_at_sequences(root.path(), &sequences)
            .expect("read grouped exact frames");
        assert_eq!(
            frames
                .iter()
                .map(|frame| frame.as_ref().map(|frame| frame.entry().sequence))
                .collect::<Vec<_>>(),
            sequences.into_iter().map(Some).collect::<Vec<_>>(),
            "the batched reader must preserve caller order across segment grouping"
        );
        assert_eq!(metadata_journal_exact_segment_opens(), 2);
        assert_eq!(metadata_journal_exact_index_lookups(), 2);
        assert_eq!(metadata_journal_exact_frame_decodes(), 3);
        assert_eq!(
            metadata_journal_decoded_records(),
            0,
            "locally appended current records must extend the warm index without a rescan"
        );
    }

    #[test]
    fn immutable_journal_frame_extends_v2_proof_from_its_exact_bytes() {
        let entry = test_journal_entry(1);
        let frame = DfsMetadataJournalFrame::encode(&entry).expect("encode frame");
        let seed = crate::metadata_records::dfs_metadata_prefix_proof_genesis(&entry.shard_id);
        let proof = frame.prefix_proof_after(&seed).expect("extend proof");
        assert_eq!(
            proof,
            dfs_metadata_prefix_proof_v2_after_bytes(&seed, entry.sequence, frame.exact_bytes())
        );
        assert!(proof.is_well_formed());

        let non_contiguous = test_journal_entry(3);
        assert!(
            DfsMetadataJournalFrame::encode(&non_contiguous)
                .expect("encode non-contiguous frame")
                .prefix_proof_after(&seed)
                .is_err(),
            "a frame helper must not create a proof across a sequence gap"
        );
    }

    #[test]
    fn journal_recovery_tolerates_lost_corrupt_and_stale_head_hints() {
        let _cache_guard = journal_cache_test_guard();
        let root = sinomo_testdir::TestDir::new("journal-derived-head-recovery");
        for sequence in 1..=3 {
            append_segmented_journal_entry(root.path(), &test_journal_entry(sequence)).unwrap();
        }
        let head = root.join("journal/head.sobj");
        for bytes in [Vec::new(), b"torn hint".to_vec()] {
            std::fs::write(&head, bytes).unwrap();
            reset_journal_segment_index_cache();
            assert_eq!(recover_next_journal_sequence(root.path()).unwrap(), 4);
        }
        std::fs::remove_file(&head).unwrap();
        assert_eq!(recover_next_journal_sequence(root.path()).unwrap(), 4);
        crate::metadata_cursor::write_journal_head_hint(root.path(), 2).unwrap();
        assert_eq!(recover_next_journal_sequence(root.path()).unwrap(), 4);
        assert_eq!(
            durable_journal_entries_since_limited(root.path(), 0, 64)
                .unwrap()
                .len(),
            3
        );
    }

    #[test]
    fn recovered_tail_hint_skips_unchanged_highest_segment_scan() {
        let _cache_guard = journal_cache_test_guard();
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-journal-tail-hint");
        for sequence in 1..=128 {
            append_segmented_journal_entry(root.path(), &test_journal_entry(sequence))
                .expect("append journal entry");
        }

        reset_journal_segment_index_cache();
        reset_metadata_journal_decoded_records();
        assert_eq!(
            recover_next_journal_sequence(root.path()).expect("recover initial tail"),
            129
        );
        assert_eq!(metadata_journal_decoded_records(), 128);
        assert!(journal_tail_hint_path(root.path()).is_file());

        reset_journal_segment_index_cache();
        reset_metadata_journal_decoded_records();
        assert_eq!(
            recover_next_journal_sequence(root.path()).expect("recover validated tail"),
            129
        );
        assert_eq!(
            metadata_journal_decoded_records(),
            0,
            "an unchanged durable tail must be validated from its final record"
        );

        append_segmented_journal_entry(root.path(), &test_journal_entry(129))
            .expect("append beyond validated tail");
        reset_journal_segment_index_cache();
        reset_metadata_journal_decoded_records();
        assert_eq!(
            recover_next_journal_sequence(root.path()).expect("recover appended tail"),
            130
        );
        assert!(
            metadata_journal_decoded_records() > 0,
            "a changed segment length must invalidate the tail hint"
        );
    }

    fn install_preceding_rollover_checkpoint(root: &Path) {
        let sequence = JOURNAL_SEGMENT_ENTRY_LIMIT - 1;
        let shard = test_journal_entry(JOURNAL_SEGMENT_ENTRY_LIMIT).shard_id;
        crate::metadata_candidate::stage_current_test_generation(root, &shard.0);
        let tables = crate::metadata_tables::DfsMetadataTables::default();
        let mut prefix = crate::metadata_records::dfs_metadata_prefix_proof_genesis(&shard);
        for journal_sequence in 1..=sequence {
            let entry = test_journal_entry(journal_sequence);
            prefix = DfsMetadataJournalFrame::encode(&entry)
                .expect("current frame")
                .prefix_proof_after(&prefix)
                .expect("actual preceding frame prefix");
        }
        crate::metadata_table_store::persist_metadata_table_snapshot(
            root,
            &tables,
            Some(sequence),
            Some(&prefix),
        )
        .expect("durable checkpoint before rollover suffix");
    }

    #[test]
    fn replay_frame_append_error_recovers_completed_segment_before_same_process_retry() {
        let _cache_guard = journal_cache_test_guard();
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-journal-partial-frame-batch");
        reset_journal_segment_index_cache();
        install_preceding_rollover_checkpoint(root.path());
        let entries = [
            test_journal_entry(JOURNAL_SEGMENT_ENTRY_LIMIT),
            test_journal_entry(JOURNAL_SEGMENT_ENTRY_LIMIT.saturating_add(1)),
        ];
        let frames = entries
            .iter()
            .map(encode_metadata_journal_frame_record)
            .collect::<Result<Vec<_>>>()
            .expect("encode exact replay frames");
        let pairs = entries.iter().zip(&frames).collect::<Vec<_>>();

        fail_replayed_frame_append_after_segments_for_test(1);
        assert!(
            persist_replayed_journal_frames(
                root.path(),
                &pairs,
                JOURNAL_SEGMENT_ENTRY_LIMIT.saturating_sub(1),
            )
            .expect_err("inject failure after the first durable segment")
            .to_string()
            .contains("injected metadata replay frame append failure")
        );
        assert_eq!(
            recover_next_journal_sequence_after_append_error(root.path())
                .expect("recover completed first segment"),
            JOURNAL_SEGMENT_ENTRY_LIMIT.saturating_add(1)
        );

        assert_eq!(
            persist_replayed_journal_frames(root.path(), &pairs, JOURNAL_SEGMENT_ENTRY_LIMIT)
                .expect("retry exact batch without duplicating first segment"),
            Some(JOURNAL_SEGMENT_ENTRY_LIMIT.saturating_add(1))
        );
        let recovered = read_segmented_journal_frames_at_sequences(
            root.path(),
            &[
                JOURNAL_SEGMENT_ENTRY_LIMIT,
                JOURNAL_SEGMENT_ENTRY_LIMIT.saturating_add(1),
            ],
        )
        .expect("read exact retried frames");
        assert_eq!(
            recovered
                .iter()
                .map(|frame| frame.as_ref().map(|frame| frame.entry().sequence))
                .collect::<Vec<_>>(),
            vec![
                Some(JOURNAL_SEGMENT_ENTRY_LIMIT),
                Some(JOURNAL_SEGMENT_ENTRY_LIMIT.saturating_add(1)),
            ]
        );
        let first_segment_path = root
            .join("journal")
            .join("segments")
            .join(format!("{:020}.slog", 1));
        assert_eq!(
            read_segmented_journal_file_since_limited(
                &first_segment_path,
                JOURNAL_SEGMENT_ENTRY_LIMIT.saturating_sub(1),
                usize::MAX,
            )
            .expect("read first segment after retry")
            .len(),
            1,
            "the same-process retry must not append a duplicate sequence"
        );
    }

    #[test]
    fn append_error_recovery_truncates_torn_empty_highest_segment() {
        let _cache_guard = journal_cache_test_guard();
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-journal-torn-highest-segment");
        reset_journal_segment_index_cache();
        install_preceding_rollover_checkpoint(root.path());
        append_segmented_journal_entry(
            root.path(),
            &test_journal_entry(JOURNAL_SEGMENT_ENTRY_LIMIT),
        )
        .expect("append complete preceding segment");

        let torn_path = root
            .join("journal")
            .join("segments")
            .join(format!("{:020}.slog", 2));
        let mut torn = fs::OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&torn_path)
            .expect("create torn next segment");
        torn.write_all(&16_u32.to_le_bytes())
            .and_then(|()| torn.write_all(b"torn"))
            .expect("write incomplete length-prefixed frame");
        torn.sync_all().expect("persist torn test bytes");
        drop(torn);

        assert_eq!(
            recover_next_journal_sequence_after_append_error(root.path())
                .expect("truncate torn tail and recover prior segment"),
            JOURNAL_SEGMENT_ENTRY_LIMIT.saturating_add(1)
        );
        assert_eq!(
            fs::metadata(&torn_path).expect("repaired segment").len(),
            0,
            "only the incomplete trailing record may be discarded"
        );
        assert_eq!(
            read_next_journal_sequence_lenient(root.path()).expect("repaired journal head"),
            JOURNAL_SEGMENT_ENTRY_LIMIT.saturating_add(1)
        );
    }

    fn journal_batch_for_prefix(
        entries: &[DfsMetadataJournalEntry],
        prefix_len: usize,
    ) -> DfsMetadataJournalBatch {
        let entries = entries[..prefix_len].to_vec();
        let last = entries.last().expect("non-empty journal prefix");
        DfsMetadataJournalBatch {
            shard_id: last.shard_id.clone(),
            next_after_sequence: last.sequence,
            entries,
        }
    }

    #[test]
    fn checkpoint_pruning_retains_a_gap_witness_for_snapshot_fallback() {
        let root = sinomo_testdir::TestDir::new("dfs-journal-checkpoint-pruning");
        let first_in_segment_two = JOURNAL_SEGMENT_ENTRY_LIMIT + 1;
        let first_in_segment_three = JOURNAL_SEGMENT_ENTRY_LIMIT * 2 + 1;
        for sequence in [1, first_in_segment_two, first_in_segment_three] {
            append_segmented_journal_entry(root.path(), &test_journal_entry(sequence))
                .expect("append checkpoint segment witness");
        }

        // Prime the per-process index so pruning must invalidate stale cache
        // entries as well as remove the durable files.
        assert_eq!(
            journal_entries_since_limited_with_next_sequence(
                root.path(),
                0,
                usize::MAX,
                first_in_segment_three + 1,
            )
            .expect("read unpruned segments")
            .len(),
            3
        );
        assert_eq!(
            prune_segmented_journal_before_checkpoint(root.path(), first_in_segment_three)
                .expect("prune checkpointed segments"),
            2
        );

        let segments = root.join("journal").join("segments");
        assert!(!segments.join(format!("{:020}.slog", 1)).exists());
        assert!(!segments.join(format!("{:020}.slog", 2)).exists());
        assert!(segments.join(format!("{:020}.slog", 3)).is_file());
        assert_eq!(
            journal_entries_since_limited_with_next_sequence(
                root.path(),
                0,
                usize::MAX,
                first_in_segment_three + 1,
            )
            .expect("read retained checkpoint segment")
            .into_iter()
            .map(|entry| entry.sequence)
            .collect::<Vec<_>>(),
            vec![first_in_segment_three],
            "a lagging replica must observe a non-contiguous retained entry and request a snapshot"
        );
    }

    fn linear_bounded_metadata_journal_batch(
        shard_id: DfsMetadataShardId,
        after_sequence: u64,
        entries: Vec<DfsMetadataJournalEntry>,
        max_entries: usize,
        max_encoded_bytes: usize,
    ) -> Result<Option<DfsMetadataJournalBatch>> {
        if max_entries == 0 {
            return Ok(None);
        }
        let max_encoded_bytes = max_encoded_bytes.max(1);
        let mut selected = Vec::new();
        let mut next_after_sequence = after_sequence;
        for entry in entries.into_iter().take(max_entries) {
            let candidate_sequence = entry.sequence;
            selected.push(entry);
            let candidate = journal_batch_for_prefix(&selected, selected.len());
            if selected.len() > 1
                && metadata_journal_batch_encoded_len(&candidate)? > max_encoded_bytes
            {
                selected.pop();
                break;
            }
            next_after_sequence = candidate_sequence;
        }
        if selected.is_empty() {
            return Ok(None);
        }
        Ok(Some(DfsMetadataJournalBatch {
            shard_id: selected
                .first()
                .map(|entry| entry.shard_id.clone())
                .unwrap_or(shard_id),
            entries: selected,
            next_after_sequence,
        }))
    }

    #[test]
    fn bounded_journal_batch_honors_exact_entry_and_byte_bounds() {
        let shard_id = DfsMetadataShardId::new("services");
        let entries = (1..=5).map(test_journal_entry).collect::<Vec<_>>();
        let two_entry_len =
            metadata_journal_batch_encoded_len(&journal_batch_for_prefix(&entries, 2))
                .expect("two-entry encoded length");
        let three_entry_len =
            metadata_journal_batch_encoded_len(&journal_batch_for_prefix(&entries, 3))
                .expect("three-entry encoded length");
        assert!(two_entry_len < three_entry_len);

        let entry_bounded =
            bounded_metadata_journal_batch(shard_id.clone(), 0, entries.clone(), 2, usize::MAX)
                .expect("entry-bounded batch")
                .expect("batch");
        assert_eq!(entry_bounded.entries.len(), 2);
        assert_eq!(entry_bounded.next_after_sequence, 2);

        let exactly_byte_bounded = bounded_metadata_journal_batch(
            shard_id.clone(),
            0,
            entries.clone(),
            entries.len(),
            three_entry_len,
        )
        .expect("exactly byte-bounded batch")
        .expect("batch");
        assert_eq!(exactly_byte_bounded.entries.len(), 3);
        assert_eq!(
            metadata_journal_batch_encoded_len(&exactly_byte_bounded)
                .expect("selected encoded length"),
            three_entry_len
        );

        let just_under_byte_bound = bounded_metadata_journal_batch(
            shard_id.clone(),
            0,
            entries.clone(),
            entries.len(),
            three_entry_len - 1,
        )
        .expect("just-under byte-bounded batch")
        .expect("batch");
        assert_eq!(just_under_byte_bound.entries.len(), 2);
        assert!(
            metadata_journal_batch_encoded_len(&just_under_byte_bound)
                .expect("selected encoded length")
                <= two_entry_len
        );

        let oversized_first =
            bounded_metadata_journal_batch(shard_id.clone(), 0, entries.clone(), 5, 0)
                .expect("oversized-first batch")
                .expect("batch");
        assert_eq!(oversized_first.entries.len(), 1);
        assert!(
            metadata_journal_batch_encoded_len(&oversized_first).expect("oversized encoded length")
                > 1
        );
        assert!(
            bounded_metadata_journal_batch(shard_id, 0, entries, 0, usize::MAX)
                .expect("zero-entry bound")
                .is_none()
        );
    }

    #[test]
    fn bounded_journal_batch_matches_linear_prefix_selection() {
        let shard_id = DfsMetadataShardId::new("services");
        let entries = (41..=47).map(test_journal_entry).collect::<Vec<_>>();
        let mut byte_limits = vec![0, 1, usize::MAX];
        for prefix_len in 1..=entries.len() {
            let encoded_len =
                metadata_journal_batch_encoded_len(&journal_batch_for_prefix(&entries, prefix_len))
                    .expect("prefix encoded length");
            assert_eq!(
                metadata_journal_batch_prefix_encoded_len(&entries[..prefix_len])
                    .expect("borrowed prefix encoded length"),
                encoded_len,
                "borrowed sizing view must preserve the emitted batch wire layout"
            );
            byte_limits.extend([
                encoded_len.saturating_sub(1),
                encoded_len,
                encoded_len.saturating_add(1),
            ]);
        }
        byte_limits.sort_unstable();
        byte_limits.dedup();

        for max_entries in [0, 1, 2, 3, entries.len(), entries.len() + 3] {
            for max_encoded_bytes in byte_limits.iter().copied() {
                let expected = linear_bounded_metadata_journal_batch(
                    shard_id.clone(),
                    40,
                    entries.clone(),
                    max_entries,
                    max_encoded_bytes,
                )
                .expect("linear selection");
                let actual = bounded_metadata_journal_batch(
                    shard_id.clone(),
                    40,
                    entries.clone(),
                    max_entries,
                    max_encoded_bytes,
                )
                .expect("binary selection");
                assert_eq!(
                    actual, expected,
                    "selection differs for max_entries={max_entries}, max_encoded_bytes={max_encoded_bytes}"
                );
            }
        }
    }

    #[test]
    fn durable_journal_tail_reuses_append_fed_offsets_and_decoded_entry() {
        let _cache_guard = journal_cache_test_guard();
        let root = std::env::temp_dir().join(format!(
            "sinomo-dfs-journal-index-{}-{}",
            std::process::id(),
            time::OffsetDateTime::now_utc().unix_timestamp_nanos()
        ));
        std::fs::remove_dir_all(&root).ok();
        for sequence in 1..=3 {
            append_segmented_journal_entry(&root, &test_journal_entry(sequence))
                .expect("append journal entry");
        }

        reset_journal_segment_index_cache();
        reset_metadata_journal_decoded_records();
        assert!(
            durable_journal_entries_since_limited(&root, 3, 2)
                .expect("first durable tail read")
                .is_empty()
        );
        assert_eq!(metadata_journal_decoded_records(), 3);

        reset_metadata_journal_decoded_records();
        assert!(
            durable_journal_entries_since_limited(&root, 3, 2)
                .expect("cached durable tail read")
                .is_empty()
        );
        assert_eq!(
            metadata_journal_decoded_records(),
            0,
            "an unchanged segment must not be decoded again"
        );

        append_segmented_journal_entry(&root, &test_journal_entry(4)).expect("append journal tail");
        reset_metadata_journal_decoded_records();
        reset_metadata_journal_decoded_cache_hits();
        assert_eq!(
            durable_journal_entries_since_limited(&root, 3, 2)
                .expect("extended durable tail read")
                .into_iter()
                .map(|entry| entry.sequence)
                .collect::<Vec<_>>(),
            vec![4]
        );
        assert_eq!(
            metadata_journal_decoded_records(),
            0,
            "the durable append must extend the exact cached index without a scan"
        );
        assert_eq!(metadata_journal_decoded_cache_hits(), 1);

        std::fs::remove_dir_all(root).expect("cleanup journal index test");
    }

    fn append_cached_two_entry_tail(
        root: &Path,
    ) -> (PathBuf, DfsMetadataJournalEntry, DfsMetadataJournalEntry) {
        append_segmented_journal_entry(root, &test_journal_entry(1))
            .expect("append seed journal entry");
        let segment_path = root
            .join("journal")
            .join("segments")
            .join(format!("{:020}.slog", 1));
        reset_journal_segment_index_cache();
        assert!(
            read_segmented_journal_file_since_limited(&segment_path, 1, 1)
                .expect("prime exact pre-append index")
                .is_empty()
        );
        let commit = test_journal_entry(2);
        let prepare = test_journal_entry(3);
        append_segmented_journal_entries(root, &[&commit, &prepare])
            .expect("append commit and prepare tail");
        (segment_path, commit, prepare)
    }

    #[test]
    fn batched_append_serves_commit_prepare_tail_without_decoding() {
        let _cache_guard = journal_cache_test_guard();
        let root = sinomo_testdir::TestDir::new("dfs-journal-append-fed-batch-cache");
        reset_journal_segment_index_cache();
        let (segment_path, commit, prepare) = append_cached_two_entry_tail(root.path());

        reset_metadata_journal_decoded_records();
        reset_metadata_journal_decoded_cache_hits();
        assert_eq!(
            read_segmented_journal_file_since_limited(&segment_path, 1, 2)
                .expect("read cached commit and prepare tail"),
            vec![commit, prepare]
        );
        assert_eq!(metadata_journal_decoded_records(), 0);
        assert_eq!(metadata_journal_decoded_cache_hits(), 1);
    }

    #[test]
    fn multi_entry_decoded_cache_requires_the_complete_selected_suffix() {
        let _cache_guard = journal_cache_test_guard();
        let root = sinomo_testdir::TestDir::new("dfs-journal-append-fed-partial-miss");
        reset_journal_segment_index_cache();
        let (segment_path, commit, prepare) = append_cached_two_entry_tail(root.path());

        {
            let mut cache = journal_segment_index_cache()
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let missing = cache
                .decoded_entries
                .iter()
                .find(|(key, cached)| {
                    key.path == segment_path && cached.sequence == prepare.sequence
                })
                .map(|(key, _)| key.clone())
                .expect("cached prepare entry");
            let removed = cache
                .decoded_entries
                .remove(&missing)
                .expect("remove one cached tail entry");
            cache.decoded_entry_bytes = cache
                .decoded_entry_bytes
                .saturating_sub(removed.record_bytes);
        }

        reset_metadata_journal_decoded_records();
        reset_metadata_journal_decoded_cache_hits();
        assert_eq!(
            read_segmented_journal_file_since_limited(&segment_path, 1, 2)
                .expect("scan complete tail after partial cache miss"),
            vec![commit.clone(), prepare.clone()]
        );
        assert_eq!(
            metadata_journal_decoded_records(),
            2,
            "a partial cache must never produce a mixed cached and disk result"
        );
        assert_eq!(metadata_journal_decoded_cache_hits(), 0);

        reset_metadata_journal_decoded_records();
        reset_metadata_journal_decoded_cache_hits();
        assert_eq!(
            read_segmented_journal_file_since_limited(&segment_path, 1, 2)
                .expect("read re-cached complete tail"),
            vec![commit, prepare]
        );
        assert_eq!(metadata_journal_decoded_records(), 0);
        assert_eq!(metadata_journal_decoded_cache_hits(), 1);
    }

    #[test]
    fn append_fed_decoded_cache_respects_process_bounds() {
        let _cache_guard = journal_cache_test_guard();
        let root = sinomo_testdir::TestDir::new("dfs-journal-append-fed-cache-bounds");
        reset_journal_segment_index_cache();
        let entries = (1..=u64::try_from(JOURNAL_SEGMENT_DECODED_CACHE_ENTRY_LIMIT + 1)
            .expect("test cache bound"))
            .map(test_journal_entry)
            .collect::<Vec<_>>();
        let references = entries.iter().collect::<Vec<_>>();
        append_segmented_journal_entries(root.path(), &references)
            .expect("append beyond decoded cache entry bound");
        let segment_path = root
            .join("journal")
            .join("segments")
            .join(format!("{:020}.slog", 1));

        let (cached_entries, _) = metadata_journal_decoded_cache_usage(&segment_path);
        let (total_entries, total_bytes) = metadata_journal_decoded_cache_total_usage();
        assert!(cached_entries > 0);
        assert!(total_entries <= JOURNAL_SEGMENT_DECODED_CACHE_ENTRY_LIMIT);
        assert!(total_bytes <= JOURNAL_SEGMENT_DECODED_CACHE_BYTE_LIMIT);
    }

    #[test]
    fn decoded_cache_clones_only_the_newest_bounded_suffix() {
        let entries = (1..=18).map(test_journal_entry).collect::<Vec<_>>();
        let candidates = entries
            .iter()
            .map(|entry| {
                (
                    entry,
                    JournalSegmentEntryOffset {
                        sequence: entry.sequence,
                        record_start: entry.sequence.saturating_mul(8),
                    },
                    8_u64,
                )
            })
            .collect::<Vec<_>>();
        let selected = clone_bounded_decoded_journal_suffix(&candidates);
        assert_eq!(selected.len(), JOURNAL_SEGMENT_DECODED_CACHE_ENTRY_LIMIT);
        assert_eq!(
            selected
                .iter()
                .map(|(_, _, entry)| entry.sequence)
                .collect::<Vec<_>>(),
            (3..=18).collect::<Vec<_>>()
        );

        let byte_bounded = [
            (&entries[0], candidates[0].1, 1_u64),
            (
                &entries[1],
                candidates[1].1,
                JOURNAL_SEGMENT_DECODED_CACHE_BYTE_LIMIT - 8,
            ),
            (&entries[2], candidates[2].1, 8_u64),
        ];
        assert_eq!(
            clone_bounded_decoded_journal_suffix(&byte_bounded)
                .iter()
                .map(|(_, _, entry)| entry.sequence)
                .collect::<Vec<_>>(),
            vec![2, 3],
            "an older record beyond the byte budget must stop suffix admission"
        );
    }

    #[test]
    fn multi_entry_cache_falls_back_after_reopen_and_segment_replacement() {
        let _cache_guard = journal_cache_test_guard();
        let root = sinomo_testdir::TestDir::new("dfs-journal-batch-cache-reopen-replace");
        reset_journal_segment_index_cache();
        let (segment_path, _, _) = append_cached_two_entry_tail(root.path());

        reset_journal_segment_index_cache();
        reset_metadata_journal_decoded_records();
        reset_metadata_journal_decoded_cache_hits();
        assert_eq!(
            read_segmented_journal_file_since_limited(&segment_path, 1, 2)
                .expect("scan tail after process-cache reopen")
                .into_iter()
                .map(|entry| entry.sequence)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert_eq!(metadata_journal_decoded_records(), 3);
        assert_eq!(metadata_journal_decoded_cache_hits(), 0);

        let replacements = (1..=3)
            .map(|sequence| {
                let mut entry = test_journal_entry(sequence);
                entry.shard_id = DfsMetadataShardId::new("replacement");
                entry
            })
            .collect::<Vec<_>>();
        let encoded = replacements
            .iter()
            .map(|entry| {
                sinomo_object_serde::to_bytes(entry, 1, LengthMode::U32)
                    .expect("encode replacement entry")
            })
            .collect::<Vec<_>>();
        remove_file_if_exists(&segment_path).expect("remove cached segment generation");
        let mut replacement =
            durable_file::open_file_for_append(&segment_path).expect("create replacement segment");
        durable_file::append_length_prefixed_records(
            &mut replacement.file,
            &segment_path,
            encoded.iter().map(Vec::as_slice),
            durable_file::DurableRecordAppendOptions::new(
                durable_file::DurableLengthEndian::Little,
            )
            .sync(durable_file::DurableFileSync::All),
        )
        .expect("persist replacement segment");

        reset_metadata_journal_decoded_records();
        reset_metadata_journal_decoded_cache_hits();
        assert_eq!(
            read_segmented_journal_file_since_limited(&segment_path, 1, 2)
                .expect("scan replacement tail"),
            replacements[1..].to_vec()
        );
        assert_eq!(metadata_journal_decoded_records(), 3);
        assert_eq!(metadata_journal_decoded_cache_hits(), 0);
    }

    #[test]
    fn retained_index_snapshot_survives_concurrent_append_truncate_and_replacement() {
        let _cache_guard = journal_cache_test_guard();
        let root = sinomo_testdir::TestDir::new("dfs-journal-retained-index-snapshot");
        for sequence in 1..=32 {
            append_segmented_journal_entry(root.path(), &test_journal_entry(sequence))
                .expect("append initial journal records");
        }
        let segment_path = root
            .join("journal")
            .join("segments")
            .join(format!("{:020}.slog", 1));
        let retained = open_indexed_journal_segment(&segment_path)
            .expect("retain the indexed reader before append");
        let original_offsets = retained
            .index
            .entries
            .iter()
            .map(|entry| (entry.sequence, entry.record_start))
            .collect::<Vec<_>>();
        let original_last = *retained.index.entries.last().expect("initial index tail");
        let original_last_bytes = retained.index.record_bytes(original_last);
        let first_record_end = retained.index.entries[1].record_start;

        let append_root = root.path().to_path_buf();
        std::thread::spawn(move || {
            append_segmented_journal_entry(&append_root, &test_journal_entry(33))
                .expect("append while another reader retains the old index");
        })
        .join()
        .expect("join durable append");
        assert_eq!(
            retained
                .index
                .entries
                .iter()
                .map(|entry| (entry.sequence, entry.record_start))
                .collect::<Vec<_>>(),
            original_offsets,
            "an append must not mutate an already-open reader's offset snapshot"
        );
        assert_eq!(
            retained.index.record_bytes(original_last),
            original_last_bytes
        );
        assert!(
            !journal_segment_file_matches_index(&retained.file, &segment_path, &retained.index)
                .expect("validate the retained pre-append reader"),
            "the old offsets must not certify the appended file length"
        );
        assert_eq!(
            segmented_journal_entry_at_sequence(root.path(), 33)
                .expect("lookup the newly appended record"),
            Some(test_journal_entry(33))
        );
        let appended = open_indexed_journal_segment(&segment_path)
            .expect("retain the complete appended index");
        assert_eq!(appended.index.entries.len(), 33);

        let truncate_path = segment_path.clone();
        std::thread::spawn(move || {
            let file = fs::OpenOptions::new()
                .write(true)
                .open(&truncate_path)
                .expect("open the same journal file for truncation");
            file.set_len(first_record_end)
                .expect("truncate at a complete record boundary");
            file.sync_all().expect("persist the truncated tail");
        })
        .join()
        .expect("join durable truncation");
        assert_eq!(appended.index.entries.len(), 33);
        assert_eq!(retained.index.entries.len(), 32);
        assert_eq!(
            retained.index.record_bytes(original_last),
            original_last_bytes
        );
        assert!(
            !journal_segment_file_matches_index(&appended.file, &segment_path, &appended.index)
                .expect("validate the retained pre-truncation reader")
        );
        assert!(
            segmented_journal_entry_at_sequence(root.path(), 33)
                .expect("lookup after truncation")
                .is_none(),
            "a fresh reader must reject the previous index's removed tail"
        );
        assert_eq!(
            segmented_journal_entry_at_sequence(root.path(), 1)
                .expect("lookup the surviving complete record"),
            Some(test_journal_entry(1))
        );
        let before_replacement = open_indexed_journal_segment(&segment_path)
            .expect("retain the surviving generation before replacement");
        let replacement_root = root.path().to_path_buf();
        let replacement_path = segment_path.clone();
        let mut replacement = test_journal_entry(1);
        replacement.shard_id = DfsMetadataShardId::new("retained-index-replacement");
        let replacement_for_writer = replacement.clone();
        std::thread::spawn(move || {
            remove_file_if_exists(&replacement_path).expect("replace the journal pathname");
            append_segmented_journal_entry(&replacement_root, &replacement_for_writer)
                .expect("persist the replacement record");
        })
        .join()
        .expect("join durable replacement");
        assert_eq!(before_replacement.index.entries.len(), 1);
        assert_eq!(
            segmented_journal_entry_at_sequence(root.path(), 1)
                .expect("lookup the replacement generation"),
            Some(replacement),
            "a fresh public lookup must not reuse the replaced file's decoded record"
        );
        let replaced =
            open_indexed_journal_segment(&segment_path).expect("open the replacement index");
        assert_ne!(
            replaced.index.content_generation, before_replacement.index.content_generation,
            "pathname replacement must establish a new content generation"
        );
    }

    #[test]
    fn point_lookup_reuses_only_the_current_segment_generation() {
        let _cache_guard = journal_cache_test_guard();
        let root = sinomo_testdir::TestDir::new("dfs-journal-decoded-point-cache");
        for sequence in 1..=3 {
            append_segmented_journal_entry(root.path(), &test_journal_entry(sequence))
                .expect("append journal entry");
        }
        let segment_path = root
            .join("journal")
            .join("segments")
            .join(format!("{:020}.slog", 1));

        reset_journal_segment_index_cache();
        reset_metadata_journal_decoded_records();
        reset_metadata_journal_decoded_cache_hits();
        let expected = segmented_journal_entry_at_sequence(root.path(), 2)
            .expect("initial point lookup")
            .expect("journal entry");
        assert_eq!(metadata_journal_decoded_records(), 3);
        assert_eq!(metadata_journal_decoded_cache_hits(), 0);

        reset_metadata_journal_decoded_records();
        reset_metadata_journal_decoded_cache_hits();
        assert_eq!(
            segmented_journal_entry_at_sequence(root.path(), 2)
                .expect("cached point lookup")
                .as_ref(),
            Some(&expected)
        );
        assert_eq!(
            metadata_journal_decoded_records(),
            0,
            "an unchanged point lookup must not deserialize the record again"
        );
        assert_eq!(metadata_journal_decoded_cache_hits(), 1);

        append_segmented_journal_entry(root.path(), &test_journal_entry(4))
            .expect("append journal tail");
        reset_metadata_journal_decoded_records();
        reset_metadata_journal_decoded_cache_hits();
        assert_eq!(
            segmented_journal_entry_at_sequence(root.path(), 2)
                .expect("point lookup after append")
                .as_ref(),
            Some(&expected)
        );
        assert_eq!(
            metadata_journal_decoded_records(),
            0,
            "a locally durable append must extend the exact cached index on every platform"
        );
        assert_eq!(metadata_journal_decoded_cache_hits(), 1);

        let mut replacement = test_journal_entry(2);
        replacement.shard_id = DfsMetadataShardId::new("replacement");
        remove_file_if_exists(&segment_path).expect("rotate segment");
        append_segmented_journal_entry(root.path(), &replacement).expect("write replacement");
        reset_metadata_journal_decoded_records();
        reset_metadata_journal_decoded_cache_hits();
        assert_eq!(
            segmented_journal_entry_at_sequence(root.path(), 2)
                .expect("replacement point lookup")
                .as_ref(),
            Some(&replacement),
            "a replacement file must never reuse the prior decoded generation"
        );
        assert_eq!(metadata_journal_decoded_records(), 0);
        assert_eq!(metadata_journal_decoded_cache_hits(), 1);

        fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&segment_path)
            .expect("truncate segment");
        append_segmented_journal_entry(root.path(), &test_journal_entry(1))
            .expect("write truncated generation");
        reset_metadata_journal_decoded_records();
        reset_metadata_journal_decoded_cache_hits();
        assert!(
            segmented_journal_entry_at_sequence(root.path(), 2)
                .expect("truncated point lookup")
                .is_none(),
            "a cached record beyond the new durable tail must disappear"
        );
        assert_eq!(metadata_journal_decoded_records(), 1);
        assert_eq!(metadata_journal_decoded_cache_hits(), 0);
    }

    #[test]
    fn point_lookup_cache_preserves_torn_tail_recovery() {
        use std::io::Write;

        let _cache_guard = journal_cache_test_guard();
        let root = sinomo_testdir::TestDir::new("dfs-journal-decoded-torn-tail");
        let first = test_journal_entry(1);
        let second = test_journal_entry(2);
        append_segmented_journal_entry(root.path(), &first).expect("append first entry");
        let segment_path = root
            .join("journal")
            .join("segments")
            .join(format!("{:020}.slog", 1));

        reset_journal_segment_index_cache();
        segmented_journal_entry_at_sequence(root.path(), 1)
            .expect("prime point cache")
            .expect("first entry");

        let second_bytes = sinomo_object_serde::to_bytes(&second, 1, LengthMode::U32)
            .expect("encode second entry");
        let second_len = u32::try_from(second_bytes.len()).expect("bounded test entry");
        let split = second_bytes.len() / 2;
        {
            let mut file = fs::OpenOptions::new()
                .append(true)
                .open(&segment_path)
                .expect("open torn tail");
            file.write_all(&second_len.to_le_bytes())
                .expect("write record length");
            file.write_all(&second_bytes[..split])
                .expect("write partial record");
            file.sync_all().expect("sync partial record");
        }

        reset_metadata_journal_decoded_records();
        reset_metadata_journal_decoded_cache_hits();
        assert_eq!(
            segmented_journal_entry_at_sequence(root.path(), 1)
                .expect("lookup before torn tail")
                .as_ref(),
            Some(&first)
        );
        assert_eq!(metadata_journal_decoded_records(), 1);
        assert_eq!(metadata_journal_decoded_cache_hits(), 0);

        {
            let mut file = fs::OpenOptions::new()
                .append(true)
                .open(&segment_path)
                .expect("open tail completion");
            file.write_all(&second_bytes[split..])
                .expect("complete record");
            file.sync_all().expect("sync complete record");
        }
        reset_metadata_journal_decoded_records();
        reset_metadata_journal_decoded_cache_hits();
        assert_eq!(
            segmented_journal_entry_at_sequence(root.path(), 2)
                .expect("lookup completed tail")
                .as_ref(),
            Some(&second),
            "completing a torn record must rebuild from the last complete boundary"
        );
        assert_eq!(metadata_journal_decoded_records(), 2);
        assert_eq!(metadata_journal_decoded_cache_hits(), 0);
    }

    #[test]
    fn decoded_point_cache_is_process_bounded() {
        let _cache_guard = journal_cache_test_guard();
        let root = sinomo_testdir::TestDir::new("dfs-journal-decoded-cache-bound");
        let entry_count = JOURNAL_SEGMENT_DECODED_CACHE_ENTRY_LIMIT + 1;
        for sequence in 1..=u64::try_from(entry_count).expect("test entry count") {
            append_segmented_journal_entry(root.path(), &test_journal_entry(sequence))
                .expect("append bounded-cache entry");
        }
        let segment_path = root
            .join("journal")
            .join("segments")
            .join(format!("{:020}.slog", 1));

        reset_journal_segment_index_cache();
        read_segmented_journal_file_since_limited(
            &segment_path,
            u64::try_from(entry_count).expect("test entry count"),
            1,
        )
        .expect("prime offset index");
        for sequence in 1..=u64::try_from(entry_count).expect("test entry count") {
            segmented_journal_entry_at_sequence(root.path(), sequence)
                .expect("bounded point lookup")
                .expect("journal entry");
        }
        let (cached_entries, cached_bytes) = metadata_journal_decoded_cache_usage(&segment_path);
        assert!(cached_entries <= JOURNAL_SEGMENT_DECODED_CACHE_ENTRY_LIMIT);
        assert!(cached_bytes <= JOURNAL_SEGMENT_DECODED_CACHE_BYTE_LIMIT);
    }

    #[test]
    fn replay_persists_new_suffix_without_decoding_existing_segment() {
        let _cache_guard = journal_cache_test_guard();
        let root = std::env::temp_dir().join(format!(
            "sinomo-dfs-journal-replay-suffix-{}-{}",
            std::process::id(),
            time::OffsetDateTime::now_utc().unix_timestamp_nanos()
        ));
        std::fs::remove_dir_all(&root).ok();
        for sequence in 1..=128 {
            append_segmented_journal_entry(&root, &test_journal_entry(sequence))
                .expect("append existing journal entry");
        }

        reset_journal_segment_index_cache();
        reset_metadata_journal_decoded_records();
        let entry = test_journal_entry(129);
        assert_eq!(
            persist_replayed_journal_entries(&root, &[&entry], 128)
                .expect("persist new replay suffix"),
            Some(129)
        );
        assert_eq!(
            metadata_journal_decoded_records(),
            0,
            "a verified contiguous suffix must not decode existing journal history"
        );
        assert_eq!(
            segmented_journal_entry_at_sequence(&root, 129)
                .expect("read replayed suffix")
                .map(|entry| entry.sequence),
            Some(129)
        );

        std::fs::remove_dir_all(root).expect("cleanup replay suffix test");
    }
}
