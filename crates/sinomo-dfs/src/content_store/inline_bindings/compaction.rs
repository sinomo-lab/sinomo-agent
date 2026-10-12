//! Bounded, restartable generation compaction. The published catalog remains
//! unchanged through copy and verification. Each private copy segment is at
//! most one migration batch; verification re-reads one such bounded segment.
//! Existing readers pin their generation and are independent of map retirement.

use super::*;
use sinomo_core::storage::migration::{
    MigrationAdvance, MigrationBatch, MigrationBudget, MigrationDriver, MigrationIdentity,
    MigrationJob, MigrationJobStore, MigrationPhase, MigrationPublication, MigrationStep,
};

const COMPACTION_RECORDS: usize = 64;
const COMPACTION_BYTES: usize = 1024 * 1024;
const COMPACTION_JOB: &str = "compaction.sobj";
const COMPACTION_PLAN: &str = "compaction-plan.sobj";

#[derive(Clone, Serialize, Deserialize)]
struct CompactionSource {
    catalog: InlineCatalog,
    sequence: u64,
    active_end: u64,
}

#[derive(Clone, Serialize, Deserialize)]
struct CopySegment {
    segment: InlineSegment,
    first: u64,
    records: u64,
    bytes: usize,
    before: Option<CanonicalKey>,
    after: CanonicalKey,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
enum CanonicalKey {
    Blob(String),
    Version(VersionKey),
    Reservation(ContentStoreReservationIdentity),
    DeletedBlob(String),
}

#[derive(Clone, Serialize, Deserialize)]
struct CompactionPlan {
    source: CompactionSource,
    source_id: String,
    target: u64,
    total: u64,
    copied: u64,
    segments: Vec<CopySegment>,
}

#[derive(Serialize, Deserialize)]
struct PlanEnvelope {
    plan: CompactionPlan,
    checksum: String,
}

#[derive(Default)]
pub(in crate::content_store) struct InlineCompactionReport {
    pub(in crate::content_store) pending: u64,
    pub(in crate::content_store) copied_records: u64,
    pub(in crate::content_store) advanced_steps: u64,
    pub(in crate::content_store) reclaimed_bytes: u64,
}

fn source_id(source: &CompactionSource) -> Result<String> {
    Ok(blake3::hash(&encode_inline(source)?).to_hex().to_string())
}
fn plan_path(root: &Path) -> PathBuf {
    root.join(INLINE_BINDING_DIR).join(COMPACTION_PLAN)
}
fn job_path(root: &Path) -> PathBuf {
    root.join(INLINE_BINDING_DIR).join(COMPACTION_JOB)
}
fn read_plan(root: &Path) -> Result<Option<CompactionPlan>> {
    let path = plan_path(root);
    let bytes = match durable_file::read_object_file_bytes(&path, INLINE_CATALOG_MAX_BYTES) {
        Ok(bytes) => bytes,
        Err(error) => {
            if !path.try_exists().map_err(|source| ContentStoreError::Io {
                path: path.clone(),
                source,
            })? {
                return Ok(None);
            }
            return Err(content_durable_file_error(error));
        }
    };
    let envelope: PlanEnvelope = sinomo_object_serde::from_bytes(&bytes).map_err(inline_error)?;
    if envelope.checksum
        != blake3::hash(&encode_inline(&envelope.plan)?)
            .to_hex()
            .as_str()
        || source_id(&envelope.plan.source)? != envelope.plan.source_id
        || envelope.plan.target <= envelope.plan.source.catalog.generation
        || envelope.plan.copied > envelope.plan.total
    {
        return Err(inline_error("invalid content compaction plan"));
    }
    let mut next = 0;
    let mut after = None;
    for segment in &envelope.plan.segments {
        if segment.before != after
            || segment
                .before
                .as_ref()
                .is_some_and(|before| before >= &segment.after)
            || segment.first != next
            || segment.segment.id != next
            || segment.records == 0
            || segment.records > COMPACTION_RECORDS as u64
            || segment.bytes > COMPACTION_BYTES
            || segment.segment.sealed_end.is_none()
            || segment.segment.sealed_digest.is_none()
        {
            return Err(inline_error("invalid bounded compaction segment"));
        }
        after = Some(segment.after.clone());
        next = next
            .checked_add(segment.records)
            .ok_or_else(|| inline_error("compaction ordinal overflow"))?;
    }
    if next != envelope.plan.copied {
        return Err(inline_error(
            "compaction progress does not match its sealed segments",
        ));
    }
    Ok(Some(envelope.plan))
}
fn save_plan(root: &Path, plan: &CompactionPlan) -> Result<()> {
    let checksum = blake3::hash(&encode_inline(plan)?).to_hex().to_string();
    let bytes = encode_inline(&PlanEnvelope {
        plan: plan.clone(),
        checksum,
    })?;
    if bytes.len() as u64 > INLINE_CATALOG_MAX_BYTES {
        return Err(inline_error("compaction plan exceeds resource bound"));
    }
    write_file_atomic(&plan_path(root), &bytes)
}
fn canonical_record_count(view: &InlineBindingView) -> u64 {
    (view.blobs.len() as u64)
        .saturating_add(view.versions.len() as u64)
        .saturating_add(view.reservations.len() as u64)
        .saturating_add(view.deleted_blobs.len() as u64)
}
/// Each step seeks after the last exact key instead of rescanning an ordinal
/// prefix. Copy and verification therefore do O(log N) map work per record.
fn canonical_next<'a>(
    view: &'a InlineBindingView,
    after: Option<&CanonicalKey>,
) -> Option<(CanonicalKey, InlineOperation, Option<&'a InlineBlobEntry>)> {
    use std::ops::Bound::{Excluded, Unbounded};
    if after.is_none() || matches!(after, Some(CanonicalKey::Blob(_))) {
        let entry = match after {
            Some(CanonicalKey::Blob(hash)) => view
                .blobs
                .range::<str, _>((Excluded(hash.as_str()), Unbounded))
                .next(),
            _ => view.blobs.first_key_value(),
        };
        if let Some((hash, blob)) = entry {
            return Some((
                CanonicalKey::Blob(hash.clone()),
                InlineOperation::Blob {
                    content_hash: hash.clone(),
                    size_bytes: blob.size_bytes,
                    published_at_unix_nanos: blob.published_at_unix_nanos,
                },
                Some(blob),
            ));
        }
    }
    if after.is_none()
        || matches!(
            after,
            Some(CanonicalKey::Blob(_) | CanonicalKey::Version(_))
        )
    {
        let entry = match after {
            Some(CanonicalKey::Version(key)) => {
                view.versions.range((Excluded(key), Unbounded)).next()
            }
            _ => view.versions.first_key_value(),
        };
        if let Some((key, state)) = entry {
            let operation = match state {
                InlineVersionState::Live(entry) => InlineOperation::RestoreVersion {
                    manifest: entry.manifest.clone(),
                    published_at_unix_nanos: entry.published_at_unix_nanos,
                },
                InlineVersionState::Deleted { content_hash } => InlineOperation::DeleteVersion {
                    object_id: key.0.clone(),
                    version: key.1.clone(),
                    content_hash: content_hash.clone(),
                },
            };
            return Some((CanonicalKey::Version(key.clone()), operation, None));
        }
    }
    if !matches!(after, Some(CanonicalKey::DeletedBlob(_))) {
        let entry = match after {
            Some(CanonicalKey::Reservation(key)) => {
                view.reservations.range((Excluded(key), Unbounded)).next()
            }
            _ => view.reservations.first_key_value(),
        };
        if let Some((identity, state)) = entry {
            return Some((
                CanonicalKey::Reservation(identity.clone()),
                match state {
                    InlineReservationState::Live(record) => {
                        InlineOperation::Reserve(record.clone())
                    }
                    InlineReservationState::Released => InlineOperation::Release(identity.clone()),
                },
                None,
            ));
        }
    }
    let hash = match after {
        Some(CanonicalKey::DeletedBlob(hash)) => view
            .deleted_blobs
            .range::<str, _>((Excluded(hash.as_str()), Unbounded))
            .next(),
        _ => view.deleted_blobs.first(),
    }?;
    Some((
        CanonicalKey::DeletedBlob(hash.clone()),
        InlineOperation::RemoveBlob {
            content_hash: hash.clone(),
        },
        None,
    ))
}
fn next_record<'a>(
    plan: &CompactionPlan,
    view: &'a InlineBindingView,
    after: Option<&CanonicalKey>,
    ordinal: u64,
) -> Result<(CanonicalKey, InlineRecord, Option<&'a InlineBlobEntry>)> {
    let (key, operation, blob) = canonical_next(view, after)
        .ok_or_else(|| inline_error("compaction key cursor exceeds exact source snapshot"))?;
    Ok((
        key,
        InlineRecord {
            format_version: INLINE_FORMAT_VERSION,
            store_incarnation: plan.source.catalog.store_incarnation.clone(),
            generation: plan.target,
            sequence: ordinal
                .checked_add(1)
                .ok_or_else(|| inline_error("compaction sequence overflow"))?,
            operation,
        },
        blob,
    ))
}
fn cursor_ordinal(cursor: &[u8]) -> Result<u64> {
    if cursor.is_empty() {
        return Ok(0);
    }
    Ok(u64::from_le_bytes(
        cursor
            .try_into()
            .map_err(|_| inline_error("invalid compaction cursor"))?,
    ))
}
fn read_payload(blob: &InlineBlobEntry) -> Result<Vec<u8>> {
    if blob.size_bytes > INLINE_BLOB_SMALL_LIMIT_BYTES {
        return Err(inline_error("compaction payload exceeds inline bound"));
    }
    let mut file = fs::File::open(&blob.path).map_err(|source| ContentStoreError::Io {
        path: blob.path.clone(),
        source,
    })?;
    file.seek(SeekFrom::Start(blob.payload_offset))
        .map_err(|source| ContentStoreError::Io {
            path: blob.path.clone(),
            source,
        })?;
    let mut bytes = vec![0; blob.size_bytes as usize];
    file.read_exact(&mut bytes)
        .map_err(|source| ContentStoreError::Io {
            path: blob.path.clone(),
            source,
        })?;
    Ok(bytes)
}
fn make_generation(root: &Path, generation: u64) -> Result<()> {
    let path = generation_root(root, generation);
    durable_file::ensure_directory(&path).map_err(content_durable_file_error)?;
    let lock_path = path.join("readers.lock");
    let mut lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(|source| ContentStoreError::Io {
            path: lock_path.clone(),
            source,
        })?;
    durable_file::sync_open_file(&mut lock, &lock_path, durable_file::DurableFileSync::All)
        .map_err(content_durable_file_error)?;
    sync_dir(&path)?;
    sync_dir(path.parent().expect("generation parent"))
}

struct CompactionDriver<'a> {
    root: &'a Path,
    plan: CompactionPlan,
    current: CompactionSource,
    view: &'a InlineBindingView,
}
impl CompactionDriver<'_> {
    fn published(&self) -> bool {
        self.current.catalog.generation == self.plan.target
    }
    fn target_catalog(&self) -> Result<InlineCatalog> {
        if self.plan.copied != self.plan.total {
            return Err(inline_error("compaction is not completely copied"));
        }
        let mut segments = self
            .plan
            .segments
            .iter()
            .map(|entry| entry.segment.clone())
            .collect::<Vec<_>>();
        segments.push(InlineSegment {
            id: self
                .plan
                .total
                .checked_add(1)
                .ok_or_else(|| inline_error("compaction segment overflow"))?,
            sealed_end: None,
            sealed_digest: None,
        });
        Ok(InlineCatalog {
            format_version: INLINE_FORMAT_VERSION,
            store_incarnation: self.plan.source.catalog.store_incarnation.clone(),
            generation: self.plan.target,
            segments,
        })
    }
    fn verify_segment(&self, segment: &CopySegment) -> Result<()> {
        let path = segment_path(self.root, self.plan.target, segment.segment.id);
        let mut file = fs::File::open(&path).map_err(|source| ContentStoreError::Io {
            path: path.clone(),
            source,
        })?;
        let mut after = segment.before.clone();
        for ordinal in segment.first..segment.first + segment.records {
            let FrameRead::Complete(frame) =
                read_frame(&mut file, &path, frame_limits()).map_err(inline_error)?
            else {
                return Err(inline_error("compaction candidate lost a complete frame"));
            };
            let (key, expected, blob) =
                next_record(&self.plan, self.view, after.as_ref(), ordinal)?;
            after = Some(key);
            if frame.header() != encode_inline(&expected)?
                || frame.payload_bytes() != blob.map(|entry| entry.size_bytes).unwrap_or(0)
            {
                return Err(inline_error(
                    "compaction frame differs from exact source projection",
                ));
            }
            if let InlineOperation::Blob { content_hash, .. } = expected.operation {
                let actual = format!(
                    "{HASH_PREFIX}{}",
                    blake3::Hash::from_bytes(*frame.payload_checksum()).to_hex()
                );
                if actual != content_hash {
                    return Err(inline_error("compaction payload differs from source hash"));
                }
            }
        }
        if after.as_ref() != Some(&segment.after) {
            return Err(inline_error("compaction segment ended at a different key"));
        }
        if !matches!(
            read_frame(&mut file, &path, frame_limits()).map_err(inline_error)?,
            FrameRead::CleanEof { .. }
        ) {
            return Err(inline_error("unexpected compaction segment suffix"));
        }
        let (hash, bytes) =
            durable_file::file_blake3_hash_and_size(&path).map_err(content_durable_file_error)?;
        if segment.segment.sealed_end != Some(bytes)
            || segment.segment.sealed_digest.as_deref() != Some(hash.to_hex().as_str())
        {
            return Err(inline_error("compaction sealed segment changed"));
        }
        Ok(())
    }
}
impl MigrationDriver for CompactionDriver<'_> {
    fn source_is_current(&mut self, job: &MigrationJob) -> std::result::Result<bool, String> {
        if self.published() {
            return Ok(job.phase == MigrationPhase::Publishing);
        }
        source_id(&self.current)
            .map(|id| id == self.plan.source_id)
            .map_err(|error| error.to_string())
    }
    fn copy_batch(
        &mut self,
        job: &MigrationJob,
        step: &MigrationStep<'_>,
    ) -> std::result::Result<MigrationBatch, String> {
        (|| -> Result<MigrationBatch> {
            let start = cursor_ordinal(&job.cursor)?;
            if start > self.plan.copied {
                return Err(inline_error(
                    "migration checkpoint is ahead of copied content",
                ));
            }
            if start < self.plan.copied {
                let prior = self
                    .plan
                    .segments
                    .last()
                    .ok_or_else(|| inline_error("missing copied batch"))?;
                if prior.first != start {
                    return Err(inline_error(
                        "copy checkpoint lag exceeds one durable batch",
                    ));
                }
                self.verify_segment(prior)?;
                // Reestablish visible-but-uncertain progress namespace before
                // the shared migration runner acknowledges this cursor.
                save_plan(self.root, &self.plan)?;
                return Ok(MigrationBatch {
                    cursor: self.plan.copied.to_le_bytes().to_vec(),
                    records: prior.records as usize,
                    bytes: prior.bytes,
                    complete: self.plan.copied == self.plan.total,
                });
            }
            if start == self.plan.total {
                return Ok(MigrationBatch {
                    cursor: start.to_le_bytes().to_vec(),
                    records: 0,
                    bytes: 0,
                    complete: true,
                });
            }
            make_generation(self.root, self.plan.target)?;
            let path = segment_path(self.root, self.plan.target, start);
            // This private bounded segment is not in any published catalog.
            // Recreate a failed partial batch; no reader can own its bytes.
            let mut writer = FrameAppender::open_with_recovery(
                &path,
                frame_limits(),
                TailRecovery::TruncateIncomplete {
                    minimum_durable_end: 0,
                },
            )
            .map_err(inline_error)?;
            let mut old = fs::File::open(&path).map_err(|source| ContentStoreError::Io {
                path: path.clone(),
                source,
            })?;
            let prior_end = writer.durable_prefix().end();
            let step = step.after_preparation().map_err(inline_error)?;
            let before = self
                .plan
                .segments
                .last()
                .map(|segment| segment.after.clone());
            let mut after = before.clone();
            let mut ordinal = start;
            let mut bytes = 0usize;
            let mut pending_frames = Vec::<(Vec<u8>, Vec<u8>)>::new();
            while ordinal < self.plan.total
                && ordinal - start < step.budget.max_records as u64
                && (ordinal == start
                    || !step.should_yield()
                    || old
                        .stream_position()
                        .map_err(|source| ContentStoreError::Io {
                            path: path.clone(),
                            source,
                        })?
                        < prior_end)
            {
                let (key, record, blob) =
                    next_record(&self.plan, self.view, after.as_ref(), ordinal)?;
                let header = encode_inline(&record)?;
                let size = header.len()
                    + blob.map(|entry| entry.size_bytes as usize).unwrap_or(0)
                    + sinomo_core::storage::checksummed_frame::FRAME_OVERHEAD_BYTES as usize;
                if bytes + size > step.budget.max_bytes {
                    break;
                }
                match read_frame(&mut old, &path, frame_limits()).map_err(inline_error)? {
                    FrameRead::Complete(frame) => {
                        if frame.header() != header
                            || frame.payload_bytes()
                                != blob.map(|entry| entry.size_bytes).unwrap_or(0)
                        {
                            return Err(inline_error("private retry contains a conflicting frame"));
                        }
                        if let InlineOperation::Blob { content_hash, .. } = &record.operation
                            && *content_hash
                                != format!(
                                    "{HASH_PREFIX}{}",
                                    blake3::Hash::from_bytes(*frame.payload_checksum()).to_hex()
                                )
                        {
                            return Err(inline_error("private retry payload hash mismatch"));
                        }
                    }
                    FrameRead::CleanEof { .. } => {
                        let payload = blob.map(read_payload).transpose()?.unwrap_or_default();
                        if let InlineOperation::Blob { content_hash, .. } = &record.operation {
                            ensure_bytes_match_hash(content_hash, &payload)?;
                        }
                        pending_frames.push((header, payload));
                    }
                    _ => return Err(inline_error("private retry tail was not recovered")),
                }
                after = Some(key);
                ordinal += 1;
                bytes += size;
            }
            if ordinal == start {
                return Err(inline_error(
                    "compaction budget cannot fit one bounded record",
                ));
            }
            // A retry may have reached more records before a failed checkpoint;
            // keep the same bounded batch rather than publish a prefix of it.
            if !matches!(
                read_frame(&mut old, &path, frame_limits()).map_err(inline_error)?,
                FrameRead::CleanEof { .. }
            ) {
                return Err(inline_error(
                    "private retry has unconsumed complete records",
                ));
            }
            // Prepare all bounded records before the one final batch barrier.
            // Durability tokens and the copy cursor exist only after it passes.
            if !pending_frames.is_empty() {
                let batch = pending_frames
                    .iter()
                    .map(|(header, payload)| (header.as_slice(), payload.as_slice()))
                    .collect::<Vec<_>>();
                writer.append_batch_bytes(&batch).map_err(inline_error)?;
            }
            let (hash, length) = durable_file::file_blake3_hash_and_size(&path)
                .map_err(content_durable_file_error)?;
            self.plan.segments.push(CopySegment {
                segment: InlineSegment {
                    id: start,
                    sealed_end: Some(length),
                    sealed_digest: Some(hash.to_hex().to_string()),
                },
                first: start,
                records: ordinal - start,
                bytes,
                before,
                after: after.expect("nonempty bounded batch"),
            });
            self.plan.copied = ordinal;
            save_plan(self.root, &self.plan)?;
            Ok(MigrationBatch {
                cursor: ordinal.to_le_bytes().to_vec(),
                records: (ordinal - start) as usize,
                bytes,
                complete: ordinal == self.plan.total,
            })
        })()
        .map_err(|error| error.to_string())
    }
    fn verify_batch(
        &mut self,
        job: &MigrationJob,
        _: &MigrationStep<'_>,
    ) -> std::result::Result<MigrationBatch, String> {
        (|| -> Result<MigrationBatch> {
            let next = cursor_ordinal(&job.cursor)?;
            let Some(segment) = self.plan.segments.iter().find(|entry| entry.first == next) else {
                if next == self.plan.total {
                    return Ok(MigrationBatch {
                        cursor: next.to_le_bytes().to_vec(),
                        records: 0,
                        bytes: 0,
                        complete: true,
                    });
                }
                return Err(inline_error(
                    "verification cursor does not select a complete copy batch",
                ));
            };
            self.verify_segment(segment)?;
            let end = next + segment.records;
            Ok(MigrationBatch {
                cursor: end.to_le_bytes().to_vec(),
                records: segment.records as usize,
                bytes: segment.bytes,
                complete: end == self.plan.total,
            })
        })()
        .map_err(|error| error.to_string())
    }
    fn publish(&mut self, _: &MigrationJob) -> std::result::Result<MigrationPublication, String> {
        (|| -> Result<MigrationPublication> {
            if self.published() {
                certify_catalog(self.root, &self.current.catalog)?;
                return Ok(MigrationPublication::Published);
            }
            if source_id(&self.current)? != self.plan.source_id {
                return Ok(MigrationPublication::Superseded);
            }
            let catalog = self.target_catalog()?;
            make_generation(self.root, catalog.generation)?;
            let active = catalog.segments.last().expect("active segment");
            let writer = FrameAppender::open(
                &segment_path(self.root, catalog.generation, active.id),
                frame_limits(),
            )
            .map_err(inline_error)?;
            if writer.durable_prefix().end() != 0 {
                return Err(inline_error(
                    "unpublished compacted active segment contains bytes",
                ));
            }
            publish_catalog(self.root, &catalog)?;
            certify_catalog(self.root, &catalog)?;
            Ok(MigrationPublication::Published)
        })()
        .map_err(|error| error.to_string())
    }
    fn discard_staging(&mut self, _: &MigrationJob) -> std::result::Result<(), String> {
        (|| -> Result<()> {
            let current = read_catalog(self.root, &self.plan.source.catalog.store_incarnation)?;
            if current.generation == self.plan.target {
                return Err(inline_error(
                    "refusing to discard a published compaction generation",
                ));
            }
            reclaim_generation(self.root, self.plan.target)?;
            Ok(())
        })()
        .map_err(|error| error.to_string())
    }
}
fn reclaim_generation(root: &Path, generation: u64) -> Result<u64> {
    let path = generation_root(root, generation);
    if !path.try_exists().map_err(|source| ContentStoreError::Io {
        path: path.clone(),
        source,
    })? {
        return Ok(0);
    }
    let lock_path = path.join("readers.lock");
    let lock = match fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&lock_path)
    {
        Ok(lock) => lock,
        Err(source) if source.kind() == io::ErrorKind::NotFound => {
            let mut entries = fs::read_dir(&path).map_err(|source| ContentStoreError::Io {
                path: path.clone(),
                source,
            })?;
            if entries.next().is_some() {
                return Err(inline_error(
                    "retired generation lost its reader fence before payload reclamation",
                ));
            }
            // Retry of final empty-directory removal after its lock name was
            // removed. No payload or future lookup can belong to this retired
            // empty generation, so no reader fence needs to be reacquired.
            fs::remove_dir(&path).map_err(|source| ContentStoreError::Io {
                path: path.clone(),
                source,
            })?;
            sync_dir(path.parent().expect("generation parent"))?;
            return Ok(0);
        }
        Err(source) => {
            return Err(ContentStoreError::Io {
                path: lock_path,
                source,
            });
        }
    };
    match FileExt::try_lock_exclusive(&lock) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(0),
        Err(source) => return Err(ContentStoreError::Io { path, source }),
    }
    // Retired generations can be large. Reclaim at most one bounded set of
    // segment files per call, keeping the lock inode until every segment goes.
    let mut bytes = 0u64;
    let mut removed = 0usize;
    let entries = fs::read_dir(&path).map_err(|source| ContentStoreError::Io {
        path: path.clone(),
        source,
    })?;
    for entry in entries {
        let entry = entry.map_err(|source| ContentStoreError::Io {
            path: path.clone(),
            source,
        })?;
        let entry_path = entry.path();
        if entry.file_name() == "readers.lock" {
            continue;
        }
        if entry_path
            .extension()
            .and_then(|extension| extension.to_str())
            != Some("slog")
        {
            return Err(inline_error(
                "unexpected file in retired content generation",
            ));
        }
        let size = file_len_if_exists(&entry_path)?.unwrap_or(0);
        if remove_file_if_exists_without_sync(&entry_path)? {
            bytes = bytes.saturating_add(size);
            removed += 1;
        }
        if removed >= COMPACTION_RECORDS {
            break;
        }
    }
    sync_dir(&path)?;
    let mut remaining = false;
    for entry in fs::read_dir(&path).map_err(|source| ContentStoreError::Io {
        path: path.clone(),
        source,
    })? {
        let entry = entry.map_err(|source| ContentStoreError::Io {
            path: path.clone(),
            source,
        })?;
        if entry.file_name() != "readers.lock" {
            remaining = true;
            break;
        }
    }
    if !remaining {
        remove_file_if_exists_without_sync(&path.join("readers.lock"))?;
        fs::remove_dir(&path).map_err(|source| ContentStoreError::Io {
            path: path.clone(),
            source,
        })?;
        sync_dir(path.parent().expect("generation parent"))?;
    }
    Ok(bytes)
}

impl LocalContentStore {
    pub(in crate::content_store) fn advance_inline_compaction(
        &self,
        dry_run: bool,
        allow_work: bool,
    ) -> Result<InlineCompactionReport> {
        let _owner = self.content_mutation_guard()?;
        let Some(sentinel) = self.inline_sentinel_unlocked()? else {
            return Ok(InlineCompactionReport::default());
        };
        let root = &self.inline_store.owner.root;
        let mut state = self
            .inline_store
            .owner
            .state
            .lock()
            .map_err(|_| inline_error("resident binding mutex poisoned"))?;
        if dry_run {
            refresh_resident(root, &sentinel.store_incarnation, &mut state, false)?;
        } else {
            ensure_writer(root, &sentinel.store_incarnation, &mut state)?;
        }
        let current = CompactionSource {
            catalog: state.catalog.clone().expect("loaded catalog"),
            sequence: state.view.sequence,
            active_end: state.active_end,
        };
        let job_store = MigrationJobStore::new(job_path(root));
        let job = job_store.read().map_err(inline_error)?;
        let mut plan = read_plan(root)?;
        let mut report = InlineCompactionReport::default();
        let terminal = job.as_ref().is_some_and(|job| {
            matches!(
                job.phase,
                MigrationPhase::Complete | MigrationPhase::Superseded
            )
        });
        if terminal {
            if !dry_run && allow_work {
                // Plan is removed first: a leftover terminal job is safely
                // recognizable after a failed cleanup barrier.
                remove_file_if_exists(&plan_path(root))?;
                remove_file_if_exists(&job_path(root))?;
                plan = None;
            } else {
                report.pending += 1;
            }
        } else if plan.is_none() && job.is_some() {
            return Err(inline_error(
                "active compaction job has no exact source plan",
            ));
        }
        let generations = root.join(INLINE_BINDING_DIR).join("generations");
        let mut generation_ids = Vec::new();
        for entry in read_dir(&generations)? {
            if !entry.path().is_dir() {
                continue;
            }
            // Generation directories are generated by generation_root only.
            if let Some(id) = entry
                .file_name()
                .to_str()
                .and_then(|name| u64::from_str_radix(name, 16).ok())
            {
                generation_ids.push(id);
            }
        }
        generation_ids.sort_unstable();
        let pending_target = plan.as_ref().map(|plan| plan.target);
        let retired = generation_ids
            .iter()
            .copied()
            .filter(|generation| {
                *generation != current.catalog.generation && Some(*generation) != pending_target
            })
            .collect::<Vec<_>>();
        report.pending = report.pending.saturating_add(retired.len() as u64);
        if !dry_run
            && allow_work
            && report.reclaimed_bytes == 0
            && let Some(generation) = retired
                .iter()
                .copied()
                .find(|generation| *generation > state.retired_generation_cursor)
                .or_else(|| retired.first().copied())
        {
            // One bounded reclaim attempt per call, rotating past pinned old
            // generations so a long reader cannot starve other retired files.
            state.retired_generation_cursor = generation;
            report.reclaimed_bytes = report
                .reclaimed_bytes
                .saturating_add(reclaim_generation(root, generation)?);
        }
        let view = &state.view;
        if terminal && (dry_run || !allow_work) {
            return Ok(report);
        }
        if plan.is_none() {
            let physical = current
                .catalog
                .segments
                .iter()
                .map(|segment| segment.sealed_end.unwrap_or(current.active_end))
                .fold(0u64, u64::saturating_add);
            let count = canonical_record_count(view);
            // A canonical generation has one record per selected state. If
            // no extra history exists, a copy cannot reclaim obsolete records.
            // Check this cheap bound before any payload or inventory walk.
            if physical < INLINE_SEGMENT_MAX_BYTES || current.sequence <= count {
                return Ok(report);
            }
            report.pending += 1;
            if dry_run || !allow_work {
                return Ok(report);
            }
            let target = generation_ids
                .iter()
                .copied()
                .max()
                .unwrap_or(current.catalog.generation)
                .checked_add(1)
                .ok_or_else(|| inline_error("generation identity exhausted"))?;
            let created = CompactionPlan {
                source_id: source_id(&current)?,
                source: current.clone(),
                target,
                total: count,
                copied: 0,
                segments: Vec::new(),
            };
            save_plan(root, &created)?;
            plan = Some(created);
        } else {
            report.pending += 1;
        }
        if dry_run || !allow_work {
            return Ok(report);
        }
        let plan = plan.expect("existing compaction plan");
        let identity = MigrationIdentity {
            owner: "sinomo-dfs".into(),
            migration: "inline-content-compaction-v1".into(),
            scope: root.display().to_string(),
            source_version: 1,
            target_version: 1,
            source_generation: plan.source_id.clone(),
        };
        let before = plan.copied;
        let mut driver = CompactionDriver {
            root,
            plan,
            current,
            view,
        };
        let outcome = job_store
            .advance(
                &identity,
                MigrationBudget {
                    max_records: COMPACTION_RECORDS,
                    max_bytes: COMPACTION_BYTES,
                    max_duration: Duration::from_millis(50),
                },
                &AtomicBool::new(false),
                &mut driver,
            )
            .map_err(inline_error)?;
        report.copied_records = driver.plan.copied.saturating_sub(before);
        report.advanced_steps += u64::from(matches!(
            &outcome,
            MigrationAdvance::Progress(_)
                | MigrationAdvance::Complete(_)
                | MigrationAdvance::Superseded(_)
        ));
        if matches!(outcome, MigrationAdvance::Complete(_)) {
            drop(driver);
            // Catalog publication is durable. Drop the old retained appender;
            // its file descriptor must never receive a subsequent write.
            state.writer = None;
            state.uncertain = true;
            ensure_writer(root, &sentinel.store_incarnation, &mut state)?;
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::active_store;
    use super::*;

    fn force_plan(store: &LocalContentStore) {
        let _owner = store.content_mutation_guard().unwrap();
        let sentinel = store.inline_sentinel_unlocked().unwrap().unwrap();
        let mut state = store.inline_store.owner.state.lock().unwrap();
        ensure_writer(&store.root, &sentinel.store_incarnation, &mut state).unwrap();
        let source = CompactionSource {
            catalog: state.catalog.clone().unwrap(),
            sequence: state.view.sequence,
            active_end: state.active_end,
        };
        save_plan(
            &store.root,
            &CompactionPlan {
                source_id: source_id(&source).unwrap(),
                target: source.catalog.generation + 1,
                source,
                total: canonical_record_count(&state.view),
                copied: 0,
                segments: Vec::new(),
            },
        )
        .unwrap();
    }
    fn drive_to_publishing(store: &LocalContentStore) {
        let jobs = MigrationJobStore::new(job_path(&store.root));
        for _ in 0..256 {
            store.advance_inline_compaction(false, true).unwrap();
            let job = jobs.read().unwrap().unwrap();
            if job.phase == MigrationPhase::Publishing {
                return;
            }
            assert_ne!(job.phase, MigrationPhase::Superseded);
        }
        panic!("bounded copy/verify did not progress");
    }

    #[test]
    fn inline_compaction_preserves_ages_roots_and_reader_generation_across_reopen() {
        let (_root, _owner, store) = active_store("dfs-inline-compaction-reader");
        for index in 0..24 {
            store
                .put_object_version(
                    &format!("object-{index}"),
                    "v1",
                    format!("payload-{index}").as_bytes(),
                )
                .unwrap();
        }
        let old_view = store.inline_binding_view().unwrap().unwrap();
        let old_age = match old_view
            .versions
            .get(&("object-0".into(), "v1".into()))
            .unwrap()
        {
            InlineVersionState::Live(entry) => entry.published_at_unix_nanos,
            _ => panic!("live"),
        };
        let expected_stats = store.stats().unwrap();
        let (_, mut reader) = store.open_object_version_stream("object-0", "v1").unwrap();
        force_plan(&store);
        drive_to_publishing(&store);
        assert_eq!(
            store.inline_binding_view().unwrap().unwrap().sequence,
            old_view.sequence
        );
        store.advance_inline_compaction(false, true).unwrap();
        let sentinel = store.inline_sentinel_unlocked().unwrap().unwrap();
        assert_eq!(
            read_catalog(&store.root, &sentinel.store_incarnation)
                .unwrap()
                .generation,
            2
        );
        store.advance_inline_compaction(false, true).unwrap();
        assert!(
            generation_root(&store.root, 1).exists(),
            "open reader pins old segments"
        );
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"payload-0");
        drop(reader);
        for _ in 0..8 {
            store.advance_inline_compaction(false, true).unwrap();
        }
        assert!(!generation_root(&store.root, 1).exists());
        let root = store.root.clone();
        drop(store);
        let reopened = LocalContentStore::new(root).unwrap();
        assert_eq!(reopened.stats().unwrap(), expected_stats);
        assert_eq!(
            reopened
                .content_reservation_roots()
                .unwrap()
                .content_hashes
                .len(),
            24
        );
        let Some(InlineVersionState::Live(entry)) =
            reopened.inline_version_state("object-0", "v1").unwrap()
        else {
            panic!("live");
        };
        assert_eq!(entry.published_at_unix_nanos, old_age);
        assert_eq!(
            reopened.read_object_version("object-23", "v1").unwrap().1,
            b"payload-23"
        );
        assert!(reopened.scrub().unwrap().is_healthy());
    }

    #[test]
    fn expected_hash_inline_binding_survives_compaction_and_root_reopen() {
        let (_root, _owner, store) = active_store("dfs-inline-expected-hash-compaction");
        let bytes = vec![53; INLINE_BLOB_SMALL_LIMIT_BYTES as usize];
        let hash = content_hash(&bytes);
        let manifest = store
            .put_object_version_with_expected_hash("object", "v1", &hash, &bytes)
            .unwrap();
        let roots = store.content_reservation_roots().unwrap().content_hashes;
        let stats = store.stats().unwrap();
        let (_, mut reader) = store.open_object_version_stream("object", "v1").unwrap();
        force_plan(&store);
        drive_to_publishing(&store);
        store.advance_inline_compaction(false, true).unwrap();
        let sentinel = store.inline_sentinel_unlocked().unwrap().unwrap();
        assert_eq!(
            read_catalog(&store.root, &sentinel.store_incarnation)
                .unwrap()
                .generation,
            2
        );
        let mut pinned = Vec::new();
        reader.read_to_end(&mut pinned).unwrap();
        assert_eq!(pinned, bytes);
        drop(reader);
        for _ in 0..8 {
            store.advance_inline_compaction(false, true).unwrap();
        }
        assert!(!generation_root(&store.root, 1).exists());
        let root = store.root.clone();
        drop(store);
        let reopened = LocalContentStore::new(root).unwrap();
        assert_eq!(reopened.read_manifest("object", "v1").unwrap(), manifest);
        assert_eq!(
            reopened.read_object_version("object", "v1").unwrap().1,
            bytes
        );
        assert_eq!(
            reopened.content_reservation_roots().unwrap().content_hashes,
            roots
        );
        assert_eq!(reopened.stats().unwrap(), stats);
        assert!(reopened.scrub().unwrap().is_healthy());
    }

    #[test]
    fn inline_compaction_superseded_source_never_publishes_or_deletes_current_generation() {
        let (_root, _owner, store) = active_store("dfs-inline-compaction-superseded");
        store.put_object_version("object", "v1", b"first").unwrap();
        force_plan(&store);
        store.advance_inline_compaction(false, true).unwrap();
        store
            .put_object_version("object", "v2", b"concurrent durable update")
            .unwrap();
        store.advance_inline_compaction(false, true).unwrap();
        let job = MigrationJobStore::new(job_path(&store.root))
            .read()
            .unwrap()
            .unwrap();
        assert_eq!(job.phase, MigrationPhase::Superseded);
        let sentinel = store.inline_sentinel_unlocked().unwrap().unwrap();
        assert_eq!(
            read_catalog(&store.root, &sentinel.store_incarnation)
                .unwrap()
                .generation,
            1
        );
        assert_eq!(
            store.read_object_version("object", "v2").unwrap().1,
            b"concurrent durable update"
        );
        assert!(!generation_root(&store.root, 2).exists());
    }

    #[test]
    fn inline_compaction_lost_pointer_barrier_resumes_publication_after_fresh_owner_certifies() {
        let (_root, _owner, store) = active_store("dfs-inline-compaction-pointer-retry");
        store
            .put_object_version("object", "v1", b"before compaction")
            .unwrap();
        force_plan(&store);
        drive_to_publishing(&store);
        let root = store.root.clone();
        INLINE_FAULT.with(|fault| {
            *fault.borrow_mut() = Some((root.clone(), InlineFault::CatalogParentSync))
        });
        assert!(store.advance_inline_compaction(false, true).is_err());
        drop(store);
        let store = LocalContentStore::new(&root).unwrap();
        INLINE_FAULT.with(|fault| {
            *fault.borrow_mut() = Some((root.clone(), InlineFault::CatalogCertification))
        });
        assert!(
            store
                .put_object_version("object", "v2", b"no premature receipt")
                .is_err()
        );
        store.advance_inline_compaction(false, true).unwrap();
        assert_eq!(
            MigrationJobStore::new(job_path(&root))
                .read()
                .unwrap()
                .unwrap()
                .phase,
            MigrationPhase::Complete
        );
        store
            .put_object_version("object", "v2", b"after certification")
            .unwrap();
        drop(store);
        let reopened = LocalContentStore::new(root).unwrap();
        assert_eq!(
            reopened.read_object_version("object", "v1").unwrap().1,
            b"before compaction"
        );
        assert_eq!(
            reopened.read_object_version("object", "v2").unwrap().1,
            b"after certification"
        );
    }
    #[test]
    fn inline_compaction_waits_for_quiet_without_publishing_a_stale_snapshot() {
        let (_root, _owner, store) = active_store("dfs-inline-compaction-needs-quiet");
        store
            .put_object_version("object", "v0", b"initial")
            .unwrap();
        for version in 1..=3 {
            force_plan(&store);
            store.advance_inline_compaction(false, true).unwrap();
            store
                .put_object_version(
                    "object",
                    &format!("v{version}"),
                    format!("foreground-{version}").as_bytes(),
                )
                .unwrap();
            store.advance_inline_compaction(false, true).unwrap();
            assert_eq!(
                MigrationJobStore::new(job_path(&store.root))
                    .read()
                    .unwrap()
                    .unwrap()
                    .phase,
                MigrationPhase::Superseded
            );
            store.advance_inline_compaction(false, true).unwrap();
        }
        force_plan(&store);
        drive_to_publishing(&store);
        store.advance_inline_compaction(false, true).unwrap();
        for version in 1..=3 {
            assert_eq!(
                store
                    .read_object_version("object", &format!("v{version}"))
                    .unwrap()
                    .1,
                format!("foreground-{version}").as_bytes()
            );
        }
    }

    #[test]
    fn inline_empty_compaction_creates_a_reader_fenced_writable_generation() {
        let (_root, _owner, store) = active_store("dfs-inline-empty-compaction");
        force_plan(&store);
        drive_to_publishing(&store);
        store.advance_inline_compaction(false, true).unwrap();
        assert!(
            generation_root(&store.root, 2)
                .join("readers.lock")
                .is_file()
        );
        store
            .put_object_version("object", "v1", b"after empty compaction")
            .unwrap();
        let (_, mut reader) = store.open_object_version_stream("object", "v1").unwrap();
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"after empty compaction");
    }
    #[test]
    fn inline_compaction_replays_lost_plan_and_job_checkpoint_without_duplicate_frames() {
        let (_root, _owner, store) = active_store("dfs-inline-compaction-copy-retry");
        for index in 0..40 {
            store
                .put_object_version(
                    &format!("object-{index}"),
                    "v1",
                    format!("payload-{index}").as_bytes(),
                )
                .unwrap();
        }
        force_plan(&store);
        store.advance_inline_compaction(false, true).unwrap();
        let old_job = read_file(&job_path(&store.root)).unwrap();
        let old_plan = read_file(&plan_path(&store.root)).unwrap();
        assert_eq!(
            MigrationJobStore::new(job_path(&store.root))
                .read()
                .unwrap()
                .unwrap()
                .phase,
            MigrationPhase::Copying
        );
        store.advance_inline_compaction(false, true).unwrap();
        let progressed = read_plan(&store.root).unwrap().unwrap();
        let copied = progressed.copied;
        let segment = progressed.segments.last().unwrap();
        let path = segment_path(&store.root, progressed.target, segment.segment.id);
        let expected_bytes = fs::read(&path).unwrap();
        // Output survived while both progress checkpoints retained their old
        // versions. Reusing that private batch must validate and resync it,
        // not duplicate records or expose it through the current catalog.
        write_file_atomic(&plan_path(&store.root), &old_plan).unwrap();
        write_file_atomic(&job_path(&store.root), &old_job).unwrap();
        store.advance_inline_compaction(false, true).unwrap();
        assert!(read_plan(&store.root).unwrap().unwrap().copied >= copied);
        assert!(fs::read(path).unwrap().starts_with(&expected_bytes));
        drive_to_publishing(&store);
        store.advance_inline_compaction(false, true).unwrap();
        assert_eq!(store.stats().unwrap().manifest_count, 40);
        assert_eq!(
            store.read_object_version("object-39", "v1").unwrap().1,
            b"payload-39"
        );
    }
    #[test]
    fn inline_compaction_keeps_deleted_external_and_released_categories_across_batch_boundary() {
        let (_root, _owner, store) = active_store("dfs-inline-compaction-mixed-states");
        let mut retired = Vec::new();
        for index in 0..35 {
            let object = format!("object-{index:03}");
            let manifest = store
                .put_object_version(&object, "v1", format!("payload-{index}").as_bytes())
                .unwrap();
            if index < 14 {
                retired.push(manifest);
            }
        }
        for manifest in &retired {
            let identity = ContentStoreReservationIdentity {
                object_id: manifest.object_id.clone(),
                version: manifest.version.clone(),
                content_hash: manifest.content_hash.clone(),
            };
            let _owner = store.content_mutation_guard().unwrap();
            store
                .inline_delete_version_unlocked(
                    &manifest.object_id,
                    &manifest.version,
                    &manifest.content_hash,
                )
                .unwrap();
            {
                let _reservation = store.lock_content_reservations_for_write().unwrap();
                store
                    .inline_release_reservations_unlocked(&BTreeSet::from([identity]))
                    .unwrap();
            }
            store
                .inline_remove_blob_unlocked(&manifest.content_hash)
                .unwrap();
        }
        let large = vec![37u8; INLINE_BLOB_SMALL_LIMIT_BYTES as usize + 1];
        let legacy = store.put_object_version("legacy", "v1", &large).unwrap();
        {
            let _owner = store.content_mutation_guard().unwrap();
            store
                .inline_delete_version_unlocked("legacy", "v1", &legacy.content_hash)
                .unwrap();
        }
        store.put_object_version("legacy", "v1", &large).unwrap();
        assert!(matches!(
            store.inline_version_state("legacy", "v1").unwrap(),
            Some(InlineVersionState::Live(_))
        ));
        force_plan(&store);
        drive_to_publishing(&store);
        let plan = read_plan(&store.root).unwrap().unwrap();
        assert!(plan.segments.len() >= 2);
        assert!(matches!(
            plan.segments.last().unwrap().after,
            CanonicalKey::DeletedBlob(_)
        ));
        store.advance_inline_compaction(false, true).unwrap();
        for manifest in retired {
            assert!(matches!(
                store
                    .inline_version_state(&manifest.object_id, "v1")
                    .unwrap(),
                Some(InlineVersionState::Deleted { .. })
            ));
            assert!(matches!(
                store
                    .inline_reservation_state(&ContentStoreReservationIdentity {
                        object_id: manifest.object_id.clone(),
                        version: "v1".into(),
                        content_hash: manifest.content_hash.clone()
                    })
                    .unwrap(),
                Some(InlineReservationState::Released)
            ));
            assert!(!store.has_blob(&manifest.content_hash).unwrap());
        }
        assert_eq!(store.read_object_version("legacy", "v1").unwrap().1, large);
        assert_eq!(
            store.read_object_version("object-034", "v1").unwrap().1,
            b"payload-34"
        );
    }
    #[test]
    fn inline_compaction_restores_blob_only_roots_without_inventing_manifests() {
        let (_root, _owner, store) = active_store("dfs-inline-compaction-blob-only");
        let first = store
            .put_object_version_blob_only("object", "v1", b"first payload")
            .unwrap();
        let other = store
            .put_object_version_blob_only("object", "v1", b"other payload")
            .unwrap();
        store
            .put_object_version_blob_only("another", "v2", b"first payload")
            .unwrap();
        let roots = store.content_reservation_roots().unwrap().reservations;
        assert_eq!(roots.len(), 3);
        assert_eq!(store.stats().unwrap().manifest_count, 0);
        force_plan(&store);
        drive_to_publishing(&store);
        store.advance_inline_compaction(false, true).unwrap();
        let root = store.root.clone();
        drop(store);
        let reopened = LocalContentStore::new(root).unwrap();
        assert!(reopened.manifests_for_object("object").unwrap().is_empty());
        assert!(reopened.manifests_for_object("another").unwrap().is_empty());
        assert_eq!(
            reopened.content_reservation_roots().unwrap().reservations,
            roots
        );
        assert_eq!(
            reopened.read_blob(&first.content_hash).unwrap(),
            b"first payload"
        );
        assert_eq!(
            reopened.read_blob(&other.content_hash).unwrap(),
            b"other payload"
        );
        let stats = reopened.stats().unwrap();
        assert_eq!(
            (
                stats.blob_count,
                stats.manifest_count,
                stats.logical_object_bytes
            ),
            (2, 0, 0)
        );
        reopened
            .put_blob_only_manifest("object", "v1", &first.content_hash, first.size_bytes)
            .unwrap();
        assert_eq!(reopened.read_manifest("object", "v1").unwrap(), first);
        assert!(reopened.manifests_for_object("another").unwrap().is_empty());
    }
}
