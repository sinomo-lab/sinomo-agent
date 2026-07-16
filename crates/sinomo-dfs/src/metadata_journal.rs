//! Segmented journal persistence for local DFS metadata shards.

use std::collections::BTreeMap;
use std::fs;
use std::io::{Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use sinomo_core::object::{LengthMode, serde_ext as sinomo_object_serde};
use sinomo_core::storage::durable_file;

use crate::metadata_error::{MetadataBackendError, Result};
use crate::metadata_io::{
    create_dir_all, metadata_durable_file_error, read_dir, read_sobj, read_sobj_optional,
    remove_file_if_exists, sync_dir, write_sobj_atomic,
};
use crate::metadata_records::{
    DfsMetadataJournalBatch, DfsMetadataJournalEntry, DfsMetadataMutation, DfsMetadataShardId,
};

pub(crate) const JOURNAL_SEGMENT_ENTRY_LIMIT: u64 = 8192;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct DfsMetadataJournalHead {
    pub(crate) next_sequence: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct DfsMetadataReplayCursor {
    pub(crate) shard_id: DfsMetadataShardId,
    pub(crate) last_sequence: u64,
}

pub(crate) fn recover_next_journal_sequence(root: &Path) -> Result<u64> {
    let journal_dir = root.join("journal");
    if !journal_dir.exists() {
        return Ok(1);
    }
    let head_path = journal_dir.join("head.sobj");
    let head_next_sequence =
        read_sobj_optional::<DfsMetadataJournalHead>(&head_path)?.map(|head| head.next_sequence);
    // `head.sobj` and the local materialization cursor are deferred hints.
    // They may agree while both lag a fully durable append, so agreement is
    // not proof that the highest journal segment contains no later entry.
    // Reopen is a cold path: inspect the bounded highest segment and repair
    // the hint before admitting a sequence that could otherwise be reused.
    let mut max_sequence = head_next_sequence
        .map(|next_sequence| next_sequence.saturating_sub(1))
        .unwrap_or(0);

    max_sequence = max_sequence.max(max_segmented_journal_sequence(root)?);
    for (sequence, _, _) in legacy_journal_entry_paths_since(root, 0)? {
        max_sequence = max_sequence.max(sequence);
    }

    let next_sequence = max_sequence.saturating_add(1).max(1);
    if head_next_sequence != Some(next_sequence) {
        write_sobj_atomic(&head_path, &DfsMetadataJournalHead { next_sequence })?;
    }
    Ok(next_sequence)
}

pub(crate) fn read_next_journal_sequence_lenient(root: &Path) -> Result<u64> {
    if let Some(next_sequence) = journal_head_next_sequence(root)? {
        return Ok(next_sequence.max(1));
    }

    let mut max_sequence = max_segmented_journal_sequence(root)?;
    for (sequence, _, _) in legacy_journal_entry_paths_since(root, 0)? {
        max_sequence = max_sequence.max(sequence);
    }
    Ok(max_sequence.saturating_add(1).max(1))
}

fn highest_segmented_journal_path(root: &Path) -> Result<Option<PathBuf>> {
    let segments_dir = root.join("journal").join("segments");
    if !segments_dir.exists() {
        return Ok(None);
    }
    let mut highest: Option<(u64, PathBuf)> = None;
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
        if highest
            .as_ref()
            .is_none_or(|(highest_segment_id, _)| segment_id > *highest_segment_id)
        {
            highest = Some((segment_id, path));
        }
    }
    Ok(highest.map(|(_, path)| path))
}

fn journal_head_next_sequence(root: &Path) -> Result<Option<u64>> {
    Ok(
        read_sobj_optional::<DfsMetadataJournalHead>(&root.join("journal").join("head.sobj"))?
            .and_then(|head| (head.next_sequence > 0).then_some(head.next_sequence)),
    )
}

fn journal_has_entries_after_head(root: &Path, after_sequence: u64) -> Result<bool> {
    let Some(next_sequence) = journal_head_next_sequence(root)? else {
        return Ok(true);
    };
    Ok(after_sequence < next_sequence.saturating_sub(1))
}

fn max_segmented_journal_sequence(root: &Path) -> Result<u64> {
    let Some(path) = highest_segmented_journal_path(root)? else {
        return Ok(0);
    };
    let mut max_sequence = 0;
    for entry in read_segmented_journal_file_since_limited(&path, 0, usize::MAX)? {
        max_sequence = max_sequence.max(entry.sequence);
    }
    Ok(max_sequence)
}

pub(crate) fn append_segmented_journal_entry(
    root: &Path,
    entry: &DfsMetadataJournalEntry,
) -> Result<()> {
    let bytes = sinomo_object_serde::to_bytes(entry, 1, LengthMode::U32)
        .map_err(|source| MetadataBackendError::Serde(source.to_string()))?;
    u32::try_from(bytes.len()).map_err(|_| {
        MetadataBackendError::Serde(format!(
            "metadata journal entry {} is too large: {} bytes",
            entry.sequence,
            bytes.len()
        ))
    })?;
    let segment_id = journal_segment_id(entry.sequence);
    let path = root
        .join("journal")
        .join("segments")
        .join(format!("{segment_id:020}.slog"));
    let existed = path.exists();
    if let Some(parent) = path.parent() {
        create_dir_all(parent.to_owned())?;
    }
    let mut append = durable_file::open_file_for_append_with_options(
        &path,
        durable_file::DurableAppendFileOptions::new().without_parent_sync(),
    )
    .map_err(metadata_durable_file_error)?;
    durable_file::append_length_prefixed_records(
        &mut append.file,
        &path,
        [bytes.as_slice()],
        durable_file::DurableRecordAppendOptions::new(durable_file::DurableLengthEndian::Little)
            .sync(durable_file::DurableFileSync::All),
    )
    .map_err(metadata_durable_file_error)?;
    if !existed && let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

pub(crate) fn append_segmented_journal_entries(
    root: &Path,
    entries: &[&DfsMetadataJournalEntry],
) -> Result<()> {
    let Some(first) = entries.first() else {
        return Ok(());
    };
    let trace_started = Instant::now();
    let segment_id = journal_segment_id(first.sequence);
    let path = root
        .join("journal")
        .join("segments")
        .join(format!("{segment_id:020}.slog"));
    let existed = path.exists();
    let create_dir_started = Instant::now();
    if let Some(parent) = path.parent() {
        create_dir_all(parent.to_owned())?;
    }
    let create_dir_ms = create_dir_started.elapsed().as_millis();
    let encode_started = Instant::now();
    let mut records = Vec::with_capacity(entries.len());
    let mut records_len = 0_u64;
    for entry in entries {
        if journal_segment_id(entry.sequence) != segment_id {
            return Err(MetadataBackendError::Serde(format!(
                "metadata journal batch tried to append entry {} to segment {segment_id}",
                entry.sequence
            )));
        }
        let bytes = sinomo_object_serde::to_bytes(entry, 1, LengthMode::U32)
            .map_err(|source| MetadataBackendError::Serde(source.to_string()))?;
        let length = u32::try_from(bytes.len()).map_err(|_| {
            MetadataBackendError::Serde(format!(
                "metadata journal entry {} is too large: {} bytes",
                entry.sequence,
                bytes.len()
            ))
        })?;
        records_len = records_len
            .saturating_add(4)
            .saturating_add(u64::from(length));
        records.push(bytes);
    }
    let encode_ms = encode_started.elapsed().as_millis();
    let write_sync_started = Instant::now();
    let mut append = durable_file::open_file_for_append_with_options(
        &path,
        durable_file::DurableAppendFileOptions::new().without_parent_sync(),
    )
    .map_err(metadata_durable_file_error)?;
    durable_file::append_length_prefixed_records(
        &mut append.file,
        &path,
        records.iter().map(Vec::as_slice),
        durable_file::DurableRecordAppendOptions::new(durable_file::DurableLengthEndian::Little)
            .sync(durable_file::DurableFileSync::All),
    )
    .map_err(metadata_durable_file_error)?;
    let write_sync_ms = write_sync_started.elapsed().as_millis();
    let sync_dir_started = Instant::now();
    if !existed && let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    let sync_dir_ms = sync_dir_started.elapsed().as_millis();
    let traceparent = crate::current_dfs_traceparent().unwrap_or_default();
    tracing::trace!(
        target: "sinomo.dfs.perf",
        traceparent = %traceparent,
        entries = entries.len(),
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

pub(crate) fn write_local_journal_entry(
    root: &Path,
    shard_id: &DfsMetadataShardId,
    next_sequence: &mut u64,
    mutation: DfsMetadataMutation,
) -> Result<DfsMetadataJournalEntry> {
    let sequence = (*next_sequence).max(1);
    let entry = DfsMetadataJournalEntry {
        shard_id: shard_id.clone(),
        sequence,
        created_at: time::OffsetDateTime::now_utc(),
        mutation,
    };
    append_segmented_journal_entry(root, &entry)?;
    *next_sequence = sequence.saturating_add(1);
    Ok(entry)
}

pub(crate) fn write_local_journal_entries(
    root: &Path,
    shard_id: &DfsMetadataShardId,
    next_sequence: &mut u64,
    mutations: Vec<DfsMetadataMutation>,
) -> Result<Vec<DfsMetadataJournalEntry>> {
    if mutations.is_empty() {
        return Ok(Vec::new());
    }
    let trace_started = Instant::now();
    let sequence_started = Instant::now();
    let mut candidate_sequence = (*next_sequence).max(1);
    let sequence_ms = sequence_started.elapsed().as_millis();
    let build_started = Instant::now();
    let mut entries = Vec::with_capacity(mutations.len());
    for mutation in mutations {
        entries.push(DfsMetadataJournalEntry {
            shard_id: shard_id.clone(),
            sequence: candidate_sequence,
            created_at: time::OffsetDateTime::now_utc(),
            mutation,
        });
        candidate_sequence = candidate_sequence.saturating_add(1);
    }
    let build_ms = build_started.elapsed().as_millis();

    let mut chunk_start = 0usize;
    let mut chunk_count = 0usize;
    let mut append_ms = 0u128;
    while chunk_start < entries.len() {
        let segment_id = journal_segment_id(entries[chunk_start].sequence);
        let mut chunk_end = chunk_start + 1;
        while chunk_end < entries.len()
            && journal_segment_id(entries[chunk_end].sequence) == segment_id
        {
            chunk_end += 1;
        }
        let chunk = entries[chunk_start..chunk_end].iter().collect::<Vec<_>>();
        let append_started = Instant::now();
        append_segmented_journal_entries(root, &chunk)?;
        append_ms = append_ms.saturating_add(append_started.elapsed().as_millis());
        *next_sequence = entries[chunk_end - 1].sequence.saturating_add(1);
        chunk_count += 1;
        chunk_start = chunk_end;
    }
    let traceparent = crate::current_dfs_traceparent().unwrap_or_default();
    tracing::trace!(
        target: "sinomo.dfs.perf",
        traceparent = %traceparent,
        shard = %shard_id.0,
        entries = entries.len(),
        chunks = chunk_count,
        sequence_ms,
        build_ms,
        append_ms,
        head_ms = 0,
        total_ms = trace_started.elapsed().as_millis(),
        "metadata-journal-entries"
    );
    Ok(entries)
}

#[cfg(test)]
pub(crate) fn persist_replayed_journal_entry(
    root: &Path,
    entry: &DfsMetadataJournalEntry,
) -> Result<Option<u64>> {
    if let Some(existing) = segmented_journal_entry_at_sequence(root, entry.sequence)? {
        if existing == *entry {
            return Ok(Some(entry.sequence));
        }
        return Err(MetadataBackendError::Serde(format!(
            "metadata journal sequence {} already exists with a different entry while replaying shard {}",
            entry.sequence, entry.shard_id.0
        )));
    }
    append_segmented_journal_entry(root, entry)?;
    Ok(Some(entry.sequence))
}

pub(crate) fn persist_replayed_journal_entries(
    root: &Path,
    entries: &[&DfsMetadataJournalEntry],
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
    for (segment_id, entries) in entries_by_segment.iter_mut() {
        entries.sort_by_key(|entry| entry.sequence);
        let path = root
            .join("journal")
            .join("segments")
            .join(format!("{segment_id:020}.slog"));
        let existing = if path.exists() {
            read_segmented_journal_file_since_limited(&path, 0, usize::MAX)?
                .into_iter()
                .map(|entry| (entry.sequence, entry))
                .collect::<BTreeMap<_, _>>()
        } else {
            BTreeMap::new()
        };
        let mut missing = Vec::new();
        for entry in entries.iter().copied() {
            if let Some(existing) = existing.get(&entry.sequence) {
                if existing == entry {
                    continue;
                }
                return Err(MetadataBackendError::Serde(format!(
                    "metadata journal sequence {} already exists with a different entry while replaying shard {}",
                    entry.sequence, entry.shard_id.0
                )));
            }
            missing.push(entry);
        }
        append_segmented_journal_entries(root, &missing)?;
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

pub(crate) fn migrate_legacy_journal_entries(root: &Path) -> Result<()> {
    let legacy_entries = legacy_journal_entry_paths_since(root, 0)?;
    if legacy_entries.is_empty() {
        return Ok(());
    }
    for (sequence, path, entry) in legacy_entries {
        if !segmented_journal_contains_sequence(root, sequence)? {
            append_segmented_journal_entry(root, &entry)?;
        }
        remove_file_if_exists(&path)?;
    }
    Ok(())
}

fn segmented_journal_contains_sequence(root: &Path, sequence: u64) -> Result<bool> {
    Ok(segmented_journal_entry_at_sequence(root, sequence)?.is_some())
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

fn legacy_journal_entry_paths_since(
    root: &Path,
    after_sequence: u64,
) -> Result<Vec<(u64, PathBuf, DfsMetadataJournalEntry)>> {
    let journal_dir = root.join("journal");
    if !journal_dir.exists() {
        return Ok(Vec::new());
    }
    let mut paths = BTreeMap::new();
    for entry in read_dir(&journal_dir)? {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("sobj")
            || path.file_name().and_then(|name| name.to_str()) == Some("head.sobj")
        {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };
        let Ok(sequence) = stem.parse::<u64>() else {
            continue;
        };
        if sequence > after_sequence {
            paths.insert(sequence, path);
        }
    }
    let mut entries = Vec::new();
    for path in paths.into_values() {
        let entry: DfsMetadataJournalEntry = read_sobj(&path)?;
        entries.push((entry.sequence, path, entry));
    }
    Ok(entries)
}

fn segmented_journal_entries_since_limited(
    root: &Path,
    after_sequence: u64,
    max_entries: usize,
    known_next_sequence: Option<u64>,
) -> Result<Vec<DfsMetadataJournalEntry>> {
    let trace_started = Instant::now();
    if max_entries == 0 {
        return Ok(Vec::new());
    }
    let has_entries = if let Some(next_sequence) = known_next_sequence {
        after_sequence < next_sequence.saturating_sub(1)
    } else {
        journal_has_entries_after_head(root, after_sequence)?
    };
    if !has_entries {
        return Ok(Vec::new());
    }
    let segments_dir = root.join("journal").join("segments");
    if !segments_dir.exists() {
        return Ok(Vec::new());
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
    for path in paths.into_values() {
        let remaining = max_entries.saturating_sub(entries.len());
        if remaining == 0 {
            break;
        }
        opened_segments = opened_segments.saturating_add(1);
        entries.extend(read_segmented_journal_file_since_limited(
            &path,
            after_sequence,
            remaining,
        )?);
        if entries.len() >= max_entries {
            break;
        }
    }
    entries.sort_by_key(|entry| entry.sequence);
    entries.truncate(max_entries);
    let traceparent = crate::current_dfs_traceparent().unwrap_or_default();
    tracing::trace!(
        target: "sinomo.dfs.perf",
        traceparent = %traceparent,
        after_sequence,
        max_entries,
        candidate_segments,
        opened_segments,
        returned_entries = entries.len(),
        total_ms = trace_started.elapsed().as_millis(),
        "metadata-journal-suffix-scan"
    );
    Ok(entries)
}

pub(crate) fn read_segmented_journal_file_since_limited(
    path: &Path,
    after_sequence: u64,
    max_entries: usize,
) -> Result<Vec<DfsMetadataJournalEntry>> {
    let trace_enabled = tracing::enabled!(target: "sinomo.dfs.perf", tracing::Level::TRACE);
    let trace_started = Instant::now();
    let mut file = fs::File::open(path).map_err(|source| MetadataBackendError::Io {
        path: path.to_owned(),
        source,
    })?;
    let file_bytes = if trace_enabled {
        file.metadata().map(|metadata| metadata.len()).unwrap_or(0)
    } else {
        0
    };
    let mut entries = Vec::new();
    let mut decoded_records = 0_usize;
    while let Some(entry) = read_next_segmented_journal_entry(&mut file, path)? {
        decoded_records = decoded_records.saturating_add(1);
        if entry.sequence > after_sequence {
            entries.push(entry);
            if entries.len() >= max_entries {
                break;
            }
        }
    }
    let scanned_bytes = if trace_enabled {
        file.stream_position().unwrap_or(0)
    } else {
        0
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
        total_ms = trace_started.elapsed().as_millis(),
        "metadata-journal-segment-scan"
    );
    Ok(entries)
}

fn read_segmented_journal_file_at_sequence(
    path: &Path,
    sequence: u64,
) -> Result<Option<DfsMetadataJournalEntry>> {
    let mut file = fs::File::open(path).map_err(|source| MetadataBackendError::Io {
        path: path.to_owned(),
        source,
    })?;
    while let Some(entry) = read_next_segmented_journal_entry(&mut file, path)? {
        if entry.sequence == sequence {
            return Ok(Some(entry));
        }
        if entry.sequence > sequence {
            break;
        }
    }
    Ok(None)
}

fn read_next_segmented_journal_entry(
    file: &mut fs::File,
    path: &Path,
) -> Result<Option<DfsMetadataJournalEntry>> {
    let Some(record) = durable_file::read_length_prefixed_record(
        file,
        path,
        durable_file::DurableRecordReadOptions::new(durable_file::DurableLengthEndian::Little),
    )
    .map_err(metadata_durable_file_error)?
    else {
        return Ok(None);
    };
    let entry = match sinomo_object_serde::from_bytes(&record.bytes) {
        Ok(entry) => entry,
        Err(_) if record.record_end >= record.file_len => {
            file.seek(SeekFrom::Start(record.record_start))
                .map_err(|source| MetadataBackendError::Io {
                    path: path.to_owned(),
                    source,
                })?;
            return Ok(None);
        }
        Err(source) => {
            return Err(MetadataBackendError::Serde(format!(
                "failed to decode segmented journal entry at {}: {source}",
                path.display()
            )));
        }
    };
    Ok(Some(entry))
}

pub(crate) fn journal_segment_id(sequence: u64) -> u64 {
    sequence
        .saturating_sub(1)
        .checked_div(JOURNAL_SEGMENT_ENTRY_LIMIT)
        .unwrap_or(0)
        .saturating_add(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_journal_entry(sequence: u64) -> DfsMetadataJournalEntry {
        DfsMetadataJournalEntry {
            shard_id: DfsMetadataShardId::new("services"),
            sequence,
            created_at: time::OffsetDateTime::UNIX_EPOCH,
            mutation: DfsMetadataMutation::PutFileSystem {
                file_system_id: crate::DfsFileSystemId::from_legacy_named(
                    "sifs-journal-suffix-test",
                )
                .expect("filesystem id"),
                file_system: None,
            },
        }
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
}
