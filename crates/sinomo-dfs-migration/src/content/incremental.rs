//! Bounded source discovery and replay into the normal current catalog.
use super::*;
use sinomo_core::storage::checksummed_frame::{FrameLimits, FrameRead, read_frame};
use sinomo_dfs::{ContentStoreStagingRow as Row, ContentStoreStagingWriter};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(super) enum Phase {
    Loose,
    Packed,
    Reservations,
    Inline,
    Manifests,
    Verify,
    Install,
    GcHints,
    Publish,
    Cleanup,
    Done,
}
#[derive(Serialize, Deserialize)]
pub(super) struct Progress {
    version: u32,
    pub(super) phase: Phase,
    lane: u8,
    snapshot: Option<snapshot_cursor::MapCursor>,
    snapshot_done: bool,
    log_offset: u64,
    inline_segment: usize,
    inline_offset: u64,
    inline_sequence: u64,
    verify_after: Option<(String, String)>,
    pub(super) publication_step: u8,
    reservation_batch: Option<PathBuf>,
    reservation_next: usize,
    #[serde(default)]
    gc_hints_complete: bool,
    #[serde(default)]
    gc_hint_scan_version: u8,
}
impl Default for Progress {
    fn default() -> Self {
        Self {
            version: 1,
            phase: Phase::Loose,
            lane: 0,
            snapshot: None,
            snapshot_done: false,
            log_offset: 0,
            inline_segment: 0,
            inline_offset: 0,
            inline_sequence: 0,
            verify_after: None,
            publication_step: 0,
            reservation_batch: None,
            reservation_next: 0,
            gc_hints_complete: false,
            gc_hint_scan_version: 0,
        }
    }
}
#[derive(Deserialize)]
struct PackedLocation {
    segment_id: String,
    offset: u64,
    size_bytes: u64,
}
#[derive(Deserialize)]
struct PackedRecord {
    version: u32,
    content_hash: String,
    location: Option<PackedLocation>,
}
#[derive(Deserialize, Serialize)]
struct Catalog {
    format_version: u32,
    store_incarnation: String,
    generation: u64,
    legacy_files_are_authoritative: bool,
    segments: Vec<Segment>,
}
#[derive(Deserialize, Serialize)]
struct Segment {
    id: u64,
    sealed_end: Option<u64>,
    sealed_digest: Option<String>,
}
#[derive(Deserialize)]
struct Envelope {
    catalog: Catalog,
    checksum: String,
}
#[derive(Deserialize)]
struct InlineRecord {
    format_version: u32,
    store_incarnation: String,
    generation: u64,
    sequence: u64,
    operation: Operation,
}
// Original producer 00f51166 uses externally tagged PascalCase variants.
#[derive(Deserialize)]
enum Operation {
    Blob {
        content_hash: String,
        size_bytes: u64,
        published_at_unix_nanos: u64,
    },
    StageBlob {
        size_bytes: u64,
        published_at_unix_nanos: u64,
        reservation: Reservation,
    },
    Bind {
        manifest: ContentManifest,
        published_at_unix_nanos: u64,
        reservation: Option<Reservation>,
    },
    Link {
        manifest: ContentManifest,
        published_at_unix_nanos: u64,
        reservation: Option<Reservation>,
    },
    RestoreVersion {
        manifest: ContentManifest,
        published_at_unix_nanos: u64,
    },
    Reserve(Reservation),
    Release(ContentStoreReservationIdentity),
    DeleteVersion {
        object_id: String,
        version: String,
        content_hash: String,
    },
    LegacyManifest {
        object_id: String,
        version: String,
        content_hash: String,
    },
    RemoveBlob {
        content_hash: String,
    },
}
#[derive(Deserialize)]
struct Reservation {
    identity: ContentStoreReservationIdentity,
    reserved_at_unix_nanos: u64,
}
#[derive(Deserialize)]
struct Single {
    format_version: u32,
    identity: ContentStoreReservationIdentity,
    reserved_at_unix_nanos: u64,
}
#[derive(Deserialize)]
struct Batch {
    format_version: u32,
    reservations: Vec<Reservation>,
}
#[derive(Serialize, Deserialize)]
struct DiscoveryReceipt {
    relative: PathBuf,
    size: u64,
    modified: Option<std::time::SystemTime>,
    directory: bool,
}
#[derive(Serialize, Deserialize)]
struct PayloadReceipt {
    source: String,
    offset: u64,
}
struct Walker {
    stack: Vec<fs::ReadDir>,
}
impl Walker {
    fn new(root: &Path) -> Result<Self> {
        Ok(Self {
            stack: if root.exists() {
                vec![fs::read_dir(root).map_err(|e| io(root, e))?]
            } else {
                Vec::new()
            },
        })
    }
    fn next(&mut self) -> Result<Option<PathBuf>> {
        loop {
            let Some(directory) = self.stack.last_mut() else {
                return Ok(None);
            };
            let Some(entry) = directory.next() else {
                self.stack.pop();
                continue;
            };
            let path = entry.map_err(error)?.path();
            let metadata = fs::symlink_metadata(&path).map_err(|e| io(&path, e))?;
            if metadata.file_type().is_symlink() {
                return Err(error("retained source contains a symlink"));
            }
            if metadata.is_dir() {
                self.stack
                    .push(fs::read_dir(&path).map_err(|e| io(&path, e))?);
            }
            return Ok(Some(path));
        }
    }
}
struct Session {
    root: PathBuf,
    work: PathBuf,
    target: PathBuf,
    progress: Progress,
    store: Option<ContentStoreStagingWriter>,
    walker: Option<Walker>,
    catalog: Option<Catalog>,
    _conversion: File,
    _owner: File,
    _reservation: File,
    _readers: Option<File>,
    current_only: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContentConversionInspection {
    pub current_format: bool,
    pub conversion_complete: bool,
    pub content_verified: bool,
    pub manifest_count: Option<u64>,
    pub retention_root_count: Option<u64>,
    pub phase: Option<String>,
}

/// Component-owned converter state. Each root has its own lock and actual
/// source/staged-store leases; there is no process-global preparation registry.
#[derive(Default)]
pub struct ContentConverter {
    sessions: Mutex<BTreeMap<PathBuf, Arc<Mutex<Option<Session>>>>>,
}
impl ContentConverter {
    /// Inspects the converter's durable progress and the actual current root.
    /// This is explicit operator inspection, not an authority or quorum proof.
    pub fn inspect(&self, root: &Path) -> Result<ContentConversionInspection> {
        let path = root.join(CONVERSION).join("progress.sobj");
        let phase = if path.exists() {
            Some(read::<Progress>(&path)?.phase)
        } else {
            None
        };
        let current_format = current(root)?;
        let conversion_complete = phase == Some(Phase::Done);
        let mut manifests = None;
        let mut roots = None;
        let mut content_verified = false;
        if current_format && (phase.is_none() || conversion_complete) {
            let inspected = LocalContentStore::open_for_inspection(root);
            let scrub = inspected.inspect_integrity().map_err(error)?;
            content_verified = scrub.issues.iter().all(|issue| {
                matches!(
                    issue,
                    sinomo_dfs::ContentStoreScrubIssue::OrphanBlob { .. }
                        | sinomo_dfs::ContentStoreScrubIssue::OrphanInlineBlob { .. }
                )
            });
            manifests = Some(scrub.stats.manifest_count);
            roots = Some(
                inspected
                    .content_reservation_roots()
                    .map_err(error)?
                    .reservation_count,
            );
        }
        Ok(ContentConversionInspection {
            current_format,
            conversion_complete,
            content_verified,
            manifest_count: manifests,
            retention_root_count: roots,
            phase: phase.map(|phase| {
                match phase {
                    Phase::Loose | Phase::Packed => "copying-content",
                    Phase::Reservations => "copying-retention-roots",
                    Phase::Inline | Phase::Manifests => "replaying-catalog",
                    Phase::Verify => "verifying-content",
                    Phase::Install | Phase::Publish => "publishing-current-root",
                    Phase::GcHints => "retiring-old-gc-observations",
                    Phase::Cleanup => "removing-old-copies",
                    Phase::Done => "ready",
                }
                .to_owned()
            }),
        })
    }

    pub fn prepare(&self, root: &Path, max_records: usize) -> Result<DfsStoragePreparation> {
        if !root.exists() {
            return Ok(DfsStoragePreparation::Ready);
        }
        let key = fs::canonicalize(root).map_err(|e| io(root, e))?;
        // The provider retains its ordinary role/data-owner checks. An exact
        // CURRENT initialization resumes in LocalContentStore::new, without
        // entering source-format discovery or conversion.
        if LocalContentStore::has_current_initialization(&key).map_err(error)? {
            return Ok(DfsStoragePreparation::Ready);
        }
        let progress = key.join(CONVERSION).join("progress.sobj");
        if current(&key)?
            && ((!progress.exists()
                && !key.join("orphan-observations").exists()
                && !key.join("manifest-absence-observations").exists()
                && !key.join("catalog/metadata-content-roots.sobj").exists())
                || progress.exists() && {
                    let saved = read::<Progress>(&progress)?;
                    saved.phase == Phase::Done
                        && saved.gc_hints_complete
                        && saved.gc_hint_scan_version == 1
                })
        {
            return Ok(DfsStoragePreparation::Ready);
        }
        if !progress.exists()
            && ![
                "objects",
                "blobs",
                "blob-packs",
                "inline-bindings",
                "content-reservations",
                "staging",
            ]
            .iter()
            .any(|name| key.join(name).exists())
        {
            return Ok(DfsStoragePreparation::Ready);
        }
        let session = {
            let mut sessions = self
                .sessions
                .lock()
                .map_err(|_| error("converter session mutex poisoned"))?;
            Arc::clone(
                sessions
                    .entry(key.clone())
                    .or_insert_with(|| Arc::new(Mutex::new(None))),
            )
        };
        let result = {
            let mut held = session
                .lock()
                .map_err(|_| error("content root session mutex poisoned"))?;
            if held.is_none() {
                *held = Some(Session::open(&key)?);
            }
            let result = held.as_mut().unwrap().advance(max_records.max(1));
            if matches!(&result, Ok(DfsStoragePreparation::Ready)) {
                *held = None;
            }
            result
        };
        if matches!(&result, Ok(DfsStoragePreparation::Ready)) {
            self.sessions
                .lock()
                .map_err(|_| error("converter session mutex poisoned"))?
                .remove(&key);
        }
        result
    }
}
impl Session {
    fn open(root: &Path) -> Result<Self> {
        let conversion = lock(&root.join("content-conversion.lock"))?;
        let owner = lock(&root.join("content-owner.lock"))?;
        let reservation = lock(&root.join("content-reservations/write.lock"))?;
        let readers = root
            .join("legacy-readers.lock")
            .exists()
            .then(|| lock(&root.join("legacy-readers.lock")))
            .transpose()?;
        let work = root.join(CONVERSION);
        durable_file::ensure_directory(&work).map_err(error)?;
        let progress_path = work.join("progress.sobj");
        let mut progress = if progress_path.exists() {
            read(&progress_path)?
        } else {
            Progress::default()
        };
        if progress.version != 1 {
            return Err(error("unsupported content conversion progress"));
        }
        let current_only =
            current(root)? && matches!(progress.phase, Phase::Loose | Phase::Done | Phase::GcHints);
        if current_only && progress.phase != Phase::GcHints {
            progress.phase = Phase::GcHints;
            progress.lane = 0;
            progress.gc_hints_complete = false;
            write(&progress_path, &progress)?;
        }
        let target = work.join("store");
        let store = if progress.phase != Phase::Publish
            && progress.phase != Phase::Cleanup
            && progress.phase != Phase::Done
            && progress.phase != Phase::GcHints
        {
            Some(ContentStoreStagingWriter::open_candidate(&target).map_err(error)?)
        } else {
            None
        };
        Ok(Self {
            root: root.to_owned(),
            work,
            target,
            progress,
            store,
            walker: None,
            catalog: None,
            _conversion: conversion,
            _owner: owner,
            _reservation: reservation,
            _readers: readers,
            current_only,
        })
    }
    fn save(&self) -> Result<()> {
        write(&self.work.join("progress.sobj"), &self.progress)
    }
    fn transition(&mut self, phase: Phase) -> Result<()> {
        self.progress.phase = phase;
        self.walker = None;
        self.save()
    }
    fn store(&self) -> Result<&ContentStoreStagingWriter> {
        self.store
            .as_ref()
            .ok_or_else(|| error("staged writer is absent"))
    }
    fn receipt_path(&self, kind: &str, path: &Path) -> Result<PathBuf> {
        let relative = path.strip_prefix(&self.root).map_err(error)?;
        Ok(self.work.join("discovery").join(kind).join(format!(
            "{}.sobj",
            blake3::hash(relative.to_string_lossy().as_bytes()).to_hex()
        )))
    }
    fn next_discovered(&mut self, kind: &str, source: &Path) -> Result<Option<(PathBuf, PathBuf)>> {
        if self.walker.is_none() {
            self.walker = Some(Walker::new(source)?);
        }
        loop {
            let Some(path) = self.walker.as_mut().unwrap().next()? else {
                return Ok(None);
            };
            let receipt = self.receipt_path(kind, &path)?;
            if receipt.exists() {
                let saved: DiscoveryReceipt = read(&receipt)?;
                let metadata = fs::symlink_metadata(&path).map_err(|e| io(&path, e))?;
                if saved.relative != path.strip_prefix(&self.root).map_err(error)?
                    || saved.size != metadata.len()
                    || saved.modified != metadata.modified().ok()
                    || saved.directory != metadata.is_dir()
                {
                    return Err(error("retained discovery source changed"));
                }
                continue;
            }
            return Ok(Some((path, receipt)));
        }
    }
    fn mark(&self, path: &Path, receipt: &Path) -> Result<()> {
        let metadata = fs::symlink_metadata(path).map_err(|e| io(path, e))?;
        write(
            receipt,
            &DiscoveryReceipt {
                relative: path.strip_prefix(&self.root).map_err(error)?.to_owned(),
                size: metadata.len(),
                modified: metadata.modified().ok(),
                directory: metadata.is_dir(),
            },
        )
    }
    fn payload_receipt(&self, hash: &str, origin: &str) -> Result<PathBuf> {
        let hex = hash_hex(hash)?;
        Ok(self
            .work
            .join("payload-receipts")
            .join(hex)
            .join(format!("{origin}.sobj")))
    }
    fn copy(&self, source: &BlobSource, origin: &str, offset: u64, snapshot: bool) -> Result<()> {
        let receipt = self.payload_receipt(&source.hash, origin)?;
        if snapshot && receipt.exists() {
            let previous: PayloadReceipt = read(&receipt)?;
            if previous.offset != offset {
                return Err(error("duplicate packed snapshot key"));
            }
        }
        copy_blob(&self.root, &self.target, source)?;
        write(
            &receipt,
            &PayloadReceipt {
                source: origin.into(),
                offset,
            },
        )
    }
    fn remove_packed(&self, hash: &str, origin: &str) -> Result<()> {
        let receipt = self.payload_receipt(hash, origin)?;
        durable_file::remove_file_if_exists_and_sync_parent(&receipt).map_err(error)?;
        let directory = receipt.parent().unwrap();
        if fs::read_dir(directory)
            .map_err(|e| io(directory, e))?
            .next()
            .is_none()
        {
            let hex = hash_hex(hash)?;
            durable_file::remove_file_if_exists_and_sync_parent(
                &self.target.join("blobs").join(&hex[..2]).join(hex),
            )
            .map_err(error)?;
        }
        Ok(())
    }
    fn packed_root(&self) -> PathBuf {
        if self.progress.lane == 0 {
            self.root.join("blob-packs")
        } else {
            self.root
                .join(format!("blob-packs/lanes/{:02x}", self.progress.lane))
        }
    }
    fn packed_row(
        &self,
        hash: String,
        location: Option<PackedLocation>,
        position: u64,
        snapshot: bool,
    ) -> Result<()> {
        let origin = format!("packed-{}", self.progress.lane);
        if let Some(location) = location {
            if location.segment_id.contains(['/', '\\']) || location.segment_id == ".." {
                return Err(error("packed locator escapes root"));
            }
            let path = self
                .packed_root()
                .join("segments")
                .join(location.segment_id);
            self.copy(
                &BlobSource {
                    hash,
                    path: path.strip_prefix(&self.root).map_err(error)?.to_owned(),
                    offset: location.offset,
                    size: location.size_bytes,
                },
                &origin,
                position,
                snapshot,
            )
        } else {
            self.remove_packed(&hash, &origin)
        }
    }
    fn packed_step(&mut self) -> Result<bool> {
        if self.progress.lane == 8 {
            self.transition(Phase::Reservations)?;
            return Ok(false);
        }
        let snapshot = self.packed_root().join("index.sobj");
        if !self.progress.snapshot_done {
            if snapshot.exists() {
                if self.progress.snapshot.is_none() {
                    self.progress.snapshot = Some(snapshot_cursor::open(&snapshot)?);
                }
                let mut cursor = self.progress.snapshot.clone().unwrap();
                if let Some((hash, location, offset)) =
                    snapshot_cursor::next::<PackedLocation>(&snapshot, &mut cursor)?
                {
                    self.packed_row(hash, Some(location), offset, true)?;
                    self.progress.snapshot = Some(cursor);
                    return Ok(true);
                }
            }
            self.progress.snapshot_done = true;
        }
        let path = self.packed_root().join("index.slog");
        if path.exists() {
            let mut file = File::open(&path).map_err(|e| io(&path, e))?;
            file.seek(SeekFrom::Start(self.progress.log_offset))
                .map_err(error)?;
            let mut length = [0; 4];
            let complete_length = match file.read_exact(&mut length) {
                Ok(()) => true,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => false,
                Err(e) => return Err(io(&path, e)),
            };
            if complete_length {
                let length = u32::from_le_bytes(length) as usize;
                if length == 0 || length > 64 * 1024 {
                    return Err(error("invalid packed index record bound"));
                }
                let mut bytes = vec![0; length];
                match file.read_exact(&mut bytes) {
                    Ok(()) => {
                        let record: PackedRecord = serde_ext::from_bytes(&bytes).map_err(error)?;
                        if record.version != 1 {
                            return Err(error("unsupported packed index log version"));
                        }
                        self.packed_row(
                            record.content_hash,
                            record.location,
                            self.progress.log_offset,
                            false,
                        )?;
                        self.progress.log_offset = file.stream_position().map_err(error)?;
                        return Ok(true);
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {}
                    Err(e) => return Err(io(&path, e)),
                }
            }
        }
        self.progress.lane += 1;
        self.progress.snapshot = None;
        self.progress.snapshot_done = false;
        self.progress.log_offset = 0;
        Ok(false)
    }
    fn reserve(&self, record: Reservation) -> Result<()> {
        if record.reserved_at_unix_nanos == 0 {
            return Err(error("retained reservation has zero timestamp"));
        }
        self.store()?
            .apply_storage_staging_row(Row::RetentionRoot {
                identity: record.identity,
                not_before: record.reserved_at_unix_nanos,
            })
            .map_err(error)
    }
    fn reservations_step(&mut self) -> Result<bool> {
        if let Some(relative) = self.progress.reservation_batch.clone() {
            let path = self.root.join(relative);
            let batch: Batch = read(&path)?;
            if ![1, 2].contains(&batch.format_version) || batch.reservations.len() > 256 {
                return Err(error("unsupported reservation batch"));
            }
            if let Some(record) = batch
                .reservations
                .into_iter()
                .nth(self.progress.reservation_next)
            {
                self.reserve(record)?;
                self.progress.reservation_next += 1;
                return Ok(true);
            }
            let receipt = self.receipt_path("reservations", &path)?;
            self.mark(&path, &receipt)?;
            self.progress.reservation_batch = None;
            self.progress.reservation_next = 0;
        }
        let root = self.root.join("content-reservations");
        let Some((path, receipt)) = self.next_discovered("reservations", &root)? else {
            self.transition(Phase::Inline)?;
            return Ok(false);
        };
        if path.is_file()
            && path.extension().and_then(|name| name.to_str()) == Some("sobj")
            && path != self.root.join(MARKER)
            && path.file_name().and_then(|name| name.to_str()) != Some("reconciliation-cursor.sobj")
        {
            if path.starts_with(root.join("batches")) {
                self.progress.reservation_batch =
                    Some(path.strip_prefix(&self.root).map_err(error)?.to_owned());
                self.progress.reservation_next = 0;
                self.save()?;
                return Ok(false);
            }
            let record: Single = read(&path)?;
            if ![1, 2].contains(&record.format_version) {
                return Err(error("unsupported retained reservation version"));
            }
            self.reserve(Reservation {
                identity: record.identity,
                reserved_at_unix_nanos: record.reserved_at_unix_nanos,
            })?;
        }
        self.mark(&path, &receipt)?;
        Ok(true)
    }
    fn catalog(&mut self) -> Result<bool> {
        if self.catalog.is_some() {
            return Ok(true);
        }
        let path = self.root.join("inline-bindings/current.sobj");
        if !path.exists() {
            return Ok(false);
        }
        let envelope: Envelope = read(&path)?;
        let catalog = envelope.catalog;
        if catalog.format_version != 1
            || !catalog.legacy_files_are_authoritative
            || catalog.generation == 0
            || catalog.segments.is_empty()
        {
            return Err(error("unsupported retained inline catalog"));
        }
        if blake3::hash(&serde_ext::to_bytes(&catalog, 1, LengthMode::U32).map_err(error)?)
            .to_hex()
            .as_str()
            != envelope.checksum
        {
            return Err(error("retained inline catalog checksum mismatch"));
        }
        self.catalog = Some(catalog);
        Ok(true)
    }
    fn inline_step(&mut self) -> Result<bool> {
        if !self.catalog()? {
            self.transition(Phase::Manifests)?;
            return Ok(false);
        }
        let catalog = self.catalog.as_ref().unwrap();
        let Some(segment) = catalog.segments.get(self.progress.inline_segment) else {
            self.transition(Phase::Manifests)?;
            return Ok(false);
        };
        let path = self.root.join(format!(
            "inline-bindings/generations/{:016x}/{:016x}.slog",
            catalog.generation, segment.id
        ));
        let sealed = self.progress.inline_segment + 1 != catalog.segments.len();
        let mut file = File::open(&path).map_err(|e| io(&path, e))?;
        file.seek(SeekFrom::Start(self.progress.inline_offset))
            .map_err(error)?;
        let end = match read_frame(
            &mut file,
            &path,
            FrameLimits::new(64 * 1024, 16 * 1024).map_err(error)?,
        )
        .map_err(error)?
        {
            FrameRead::Complete(frame) => {
                let record: InlineRecord = serde_ext::from_bytes(frame.header()).map_err(error)?;
                if record.format_version != 1
                    || record.store_incarnation != catalog.store_incarnation
                    || record.generation != catalog.generation
                    || record.sequence
                        != self
                            .progress
                            .inline_sequence
                            .checked_add(1)
                            .ok_or_else(|| error("inline sequence overflow"))?
                {
                    return Err(error(
                        "retained inline record identity or sequence mismatch",
                    ));
                }
                let hash = format!(
                    "blake3:{}",
                    blake3::Hash::from_bytes(*frame.payload_checksum()).to_hex()
                );
                let mut payload = vec![0; usize::try_from(frame.payload_bytes()).map_err(error)?];
                file.seek(SeekFrom::Start(frame.payload_offset()))
                    .map_err(error)?;
                file.read_exact(&mut payload).map_err(error)?;
                self.apply_inline(record.operation, &hash, payload)?;
                self.progress.inline_sequence = record.sequence;
                self.progress.inline_offset = frame.frame_end();
                return Ok(true);
            }
            FrameRead::CleanEof { offset } => offset,
            FrameRead::Incomplete(tail) if !sealed => tail.frame_start,
            FrameRead::Incomplete(_) => {
                return Err(error("sealed inline source has a partial frame"));
            }
            FrameRead::Corrupt(frame) => return Err(error(frame.reason)),
        };
        if sealed {
            let (hash, size) = durable_file::file_blake3_hash_and_size(&path).map_err(error)?;
            if size != end
                || segment.sealed_end != Some(end)
                || segment.sealed_digest.as_deref() != Some(hash.to_hex().as_str())
            {
                return Err(error("retained inline segment certificate mismatch"));
            }
        }
        self.progress.inline_segment += 1;
        self.progress.inline_offset = 0;
        Ok(false)
    }
    fn apply_inline(&self, operation: Operation, hash: &str, payload: Vec<u8>) -> Result<()> {
        let store = self.store()?;
        match operation {
            Operation::Blob {
                content_hash,
                size_bytes,
                published_at_unix_nanos,
            } => {
                if content_hash != hash || size_bytes != payload.len() as u64 {
                    return Err(error("inline source blob identity mismatch"));
                }
                store
                    .apply_storage_staging_row(Row::Blob {
                        content_hash,
                        bytes: payload,
                        published_at_unix_nanos,
                    })
                    .map_err(error)?;
            }
            Operation::StageBlob {
                size_bytes,
                published_at_unix_nanos,
                reservation,
            } => {
                if reservation.identity.content_hash != hash || size_bytes != payload.len() as u64 {
                    return Err(error("inline staged payload mismatch"));
                }
                store
                    .apply_storage_staging_row(Row::Blob {
                        content_hash: hash.into(),
                        bytes: payload,
                        published_at_unix_nanos,
                    })
                    .map_err(error)?;
                self.reserve(reservation)?;
            }
            Operation::Bind {
                manifest,
                published_at_unix_nanos,
                reservation,
            } => {
                if manifest.content_hash != hash || manifest.size_bytes != payload.len() as u64 {
                    return Err(error("inline source binding mismatch"));
                }
                if !payload.is_empty() {
                    store
                        .apply_storage_staging_row(Row::Blob {
                            content_hash: hash.into(),
                            bytes: payload,
                            published_at_unix_nanos,
                        })
                        .map_err(error)?;
                }
                store
                    .apply_storage_staging_row(Row::Manifest {
                        manifest,
                        published_at_unix_nanos,
                    })
                    .map_err(error)?;
                if let Some(record) = reservation {
                    self.reserve(record)?;
                }
            }
            Operation::Link {
                manifest,
                published_at_unix_nanos,
                reservation,
            } => {
                store
                    .apply_storage_staging_row(Row::Manifest {
                        manifest,
                        published_at_unix_nanos,
                    })
                    .map_err(error)?;
                if let Some(record) = reservation {
                    self.reserve(record)?;
                }
            }
            Operation::RestoreVersion {
                manifest,
                published_at_unix_nanos,
            } => store
                .apply_storage_staging_row(Row::Manifest {
                    manifest,
                    published_at_unix_nanos,
                })
                .map_err(error)?,
            Operation::Reserve(record) => self.reserve(record)?,
            Operation::Release(identity) => store
                .apply_storage_staging_row(Row::RetentionRelease(identity))
                .map_err(error)?,
            Operation::DeleteVersion {
                object_id,
                version,
                content_hash,
            } => store
                .apply_storage_staging_row(Row::VersionTombstone {
                    object_id,
                    version,
                    content_hash,
                })
                .map_err(error)?,
            Operation::LegacyManifest {
                object_id,
                version,
                content_hash,
            } => {
                let path = self
                    .root
                    .join("objects")
                    .join(&object_id)
                    .join(&version)
                    .join("manifest.sobj");
                let manifest: ContentManifest = read(&path)?;
                if manifest.object_id != object_id
                    || manifest.version != version
                    || manifest.content_hash != content_hash
                {
                    return Err(error("legacy binding has no exact file manifest"));
                }
                let published_at_unix_nanos = modified_nanos(&path)?;
                store
                    .apply_storage_staging_row(Row::Manifest {
                        manifest,
                        published_at_unix_nanos,
                    })
                    .map_err(error)?;
            }
            Operation::RemoveBlob { content_hash } => store
                .apply_storage_staging_row(Row::BlobTombstone { content_hash })
                .map_err(error)?,
        }
        Ok(())
    }
    fn advance(&mut self, budget: usize) -> Result<DfsStoragePreparation> {
        let mut processed = 0;
        while processed < budget {
            let worked = match self.progress.phase {
                Phase::Loose => {
                    let source = self.root.join("blobs");
                    if let Some((path, receipt)) = self.next_discovered("loose", &source)? {
                        if path.is_file() {
                            let hash = format!(
                                "blake3:{}",
                                path.file_name()
                                    .and_then(|name| name.to_str())
                                    .ok_or_else(|| error("invalid loose hash name"))?
                            );
                            let metadata = fs::metadata(&path).map_err(|e| io(&path, e))?;
                            self.copy(
                                &BlobSource {
                                    hash,
                                    path: path.strip_prefix(&self.root).map_err(error)?.to_owned(),
                                    offset: 0,
                                    size: metadata.len(),
                                },
                                "loose",
                                0,
                                false,
                            )?;
                        }
                        self.mark(&path, &receipt)?;
                        true
                    } else {
                        self.transition(Phase::Packed)?;
                        false
                    }
                }
                Phase::Packed => self.packed_step()?,
                Phase::Reservations => self.reservations_step()?,
                Phase::Inline => self.inline_step()?,
                Phase::Manifests => {
                    let source = self.root.join("objects");
                    if let Some((path, receipt)) = self.next_discovered("manifests", &source)? {
                        if path.file_name().and_then(|name| name.to_str()) == Some("manifest.sobj")
                        {
                            let manifest: ContentManifest = read(&path)?;
                            if path
                                .parent()
                                .and_then(Path::file_name)
                                .and_then(|name| name.to_str())
                                != Some(manifest.version.as_str())
                                || path
                                    .parent()
                                    .and_then(Path::parent)
                                    .and_then(Path::file_name)
                                    .and_then(|name| name.to_str())
                                    != Some(manifest.object_id.as_str())
                            {
                                return Err(error("source manifest path identity mismatch"));
                            }
                            if !self
                                .store()?
                                .storage_staging_version_owned(
                                    &manifest.object_id,
                                    &manifest.version,
                                )
                                .map_err(error)?
                            {
                                self.store()?
                                    .apply_storage_staging_row(Row::Manifest {
                                        manifest,
                                        published_at_unix_nanos: modified_nanos(&path)?,
                                    })
                                    .map_err(error)?;
                            }
                        }
                        self.mark(&path, &receipt)?;
                        true
                    } else {
                        self.transition(Phase::Verify)?;
                        false
                    }
                }
                Phase::Verify => {
                    let page = self
                        .store()?
                        .storage_staging_manifest_page(self.progress.verify_after.as_ref(), 1)
                        .map_err(error)?;
                    if let Some(manifest) = page.into_iter().next() {
                        let (_, mut reader) = self
                            .store()?
                            .open_object_version_stream(&manifest.object_id, &manifest.version)
                            .map_err(error)?;
                        let mut hash = blake3::Hasher::new();
                        let mut bytes = [0; 64 * 1024];
                        loop {
                            let count = reader.read(&mut bytes).map_err(error)?;
                            if count == 0 {
                                break;
                            }
                            hash.update(&bytes[..count]);
                        }
                        if format!("blake3:{}", hash.finalize().to_hex()) != manifest.content_hash {
                            return Err(error("candidate manifest logical digest mismatch"));
                        }
                        self.progress.verify_after = Some((manifest.object_id, manifest.version));
                        true
                    } else {
                        self.transition(Phase::Install)?;
                        false
                    }
                }
                Phase::Install => {
                    let source = self.target.join("blobs");
                    if self.walker.is_none() {
                        self.walker = Some(Walker::new(&source)?);
                    }
                    if let Some(path) = self.walker.as_mut().unwrap().next()? {
                        if path.is_file() {
                            let relative = path.strip_prefix(&self.target).map_err(error)?;
                            let hash = format!(
                                "blake3:{}",
                                path.file_name()
                                    .and_then(|name| name.to_str())
                                    .ok_or_else(|| error("invalid target hash"))?
                            );
                            let receipt = self
                                .work
                                .join("installed")
                                .join(format!("{}.sobj", hash_hex(&hash)?));
                            if !receipt.exists() {
                                install_blob(
                                    &self.root,
                                    &self.target,
                                    &BlobSource {
                                        hash,
                                        path: relative.to_owned(),
                                        offset: 0,
                                        size: fs::metadata(&path).map_err(error)?.len(),
                                    },
                                )?;
                                write(&receipt, &true)?;
                            }
                        }
                        true
                    } else {
                        self.store = None;
                        self.progress.lane = 0;
                        self.transition(Phase::GcHints)?;
                        false
                    }
                }
                Phase::GcHints => {
                    if self.gc_hint_step()? {
                        true
                    } else {
                        self.progress.gc_hints_complete = true;
                        self.progress.gc_hint_scan_version = 1;
                        self.transition(if self.current_only {
                            Phase::Done
                        } else {
                            Phase::Publish
                        })?;
                        false
                    }
                }
                Phase::Publish => {
                    self.publish()?;
                    self.progress.lane = 0;
                    self.transition(Phase::Cleanup)?;
                    false
                }
                Phase::Cleanup => {
                    if self.cleanup()? {
                        true
                    } else {
                        self.transition(Phase::Done)?;
                        false
                    }
                }
                Phase::Done => return Ok(DfsStoragePreparation::Ready),
            };
            if worked {
                processed += 1;
                self.save()?;
            }
        }
        Ok(DfsStoragePreparation::Pending {
            processed_records: processed,
        })
    }
    fn gc_hint_step(&mut self) -> Result<bool> {
        let roots = [
            "orphan-observations",
            "manifest-absence-observations",
            "catalog",
        ];
        if self.progress.lane >= roots.len() as u8 {
            return Ok(false);
        }
        let source = self.root.join(roots[self.progress.lane as usize]);
        if self.walker.is_none() {
            self.walker = Some(Walker::new(&source)?);
        }
        loop {
            let Some(path) = self.walker.as_mut().unwrap().next()? else {
                self.progress.lane += 1;
                self.walker = None;
                self.save()?;
                return Ok(true);
            };
            let receipt = self.receipt_path("gc-hints", &path)?;
            if path.is_dir() {
                if receipt.exists() {
                    continue;
                }
                self.mark(&path, &receipt)?;
                return Ok(true);
            }
            if !path.is_file() || path.extension().and_then(|value| value.to_str()) != Some("sobj")
            {
                continue;
            }
            if self.progress.lane == 2
                && path.file_name().and_then(|value| value.to_str())
                    != Some("metadata-content-roots.sobj")
            {
                continue;
            }
            let metadata = fs::metadata(&path).map_err(|e| io(&path, e))?;
            if receipt.exists() {
                let previous: DiscoveryReceipt = read(&receipt)?;
                if previous.size == metadata.len() && previous.modified == metadata.modified().ok()
                {
                    continue;
                }
            }
            let bytes = durable_file::read_object_file_bytes(&path, RECORD_LIMIT).map_err(error)?;
            let value = match crate::metadata::source_document::decode_original_document(&bytes) {
                Ok(value) => value,
                // A torn CURRENT relaxed marker remains the normal store's
                // conservative crash-recovery input, never legacy parsing here.
                Err(_) => {
                    self.mark(&path, &receipt)?;
                    return Ok(true);
                }
            };
            let Some(fields) = value.as_object() else {
                self.mark(&path, &receipt)?;
                return Ok(true);
            };
            let version = fields
                .get("format_version")
                .and_then(serde_json::Value::as_u64);
            let legacy = match self.progress.lane {
                0 => {
                    version == Some(1)
                        || version == Some(2)
                            && (!fields.contains_key("catalog_generation")
                                || !fields.contains_key("catalog_observed_at_unix_nanos"))
                }
                1 => {
                    version == Some(1)
                        && (!fields.contains_key("catalog_generation")
                            || !fields.contains_key("catalog_generated_at_unix_nanos"))
                }
                2 => retired_content_root_catalog(fields),
                _ => false,
            };
            if legacy {
                let relative = path.strip_prefix(&self.root).map_err(error)?;
                let archive = self.work.join("original-gc-hints").join(relative);
                if archive.exists() {
                    if durable_file::read_object_file_bytes(&archive, RECORD_LIMIT)
                        .map_err(error)?
                        != bytes
                    {
                        return Err(error("original GC observation archive changed"));
                    }
                } else {
                    let parent = archive
                        .parent()
                        .ok_or_else(|| error("GC archive has no parent"))?;
                    let relative = parent.strip_prefix(&self.work).map_err(error)?;
                    let mut directory = self.work.clone();
                    for component in relative.components() {
                        directory.push(component);
                        durable_file::ensure_directory(&directory).map_err(error)?;
                    }
                    durable_file::write_file_atomic(
                        &archive,
                        &bytes,
                        durable_file::DurableAtomicFileOptions::new("gc-source", "tmp").sync_all(),
                    )
                    .map_err(error)?;
                }
                // Hint retirement only extends grace. Actual blobs, manifests and
                // retention roots are never deleted or granted by this conversion.
                durable_file::remove_file_if_exists_and_sync_parent(&path).map_err(error)?;
            } else {
                self.mark(&path, &receipt)?;
            }
            return Ok(true);
        }
    }
    fn publish(&mut self) -> Result<()> {
        let current_catalog = self.root.join("inline-bindings");
        let backup = self.work.join("old-inline-bindings");
        if self.progress.publication_step == 0 {
            if current_catalog.exists()
                && !backup.exists()
                && self.target.join("inline-bindings").exists()
            {
                fs::rename(&current_catalog, &backup).map_err(|e| io(&current_catalog, e))?;
                durable_file::sync_directory(&self.root).map_err(error)?;
                durable_file::sync_directory(&self.work).map_err(error)?;
            }
            self.progress.publication_step = 1;
            self.save()?;
        }
        if self.progress.publication_step == 1 {
            if self.target.join("inline-bindings").exists() {
                fs::rename(self.target.join("inline-bindings"), &current_catalog)
                    .map_err(|e| io(&current_catalog, e))?;
                durable_file::sync_directory(&self.root).map_err(error)?;
                durable_file::sync_directory(&self.target).map_err(error)?;
            }
            self.progress.publication_step = 2;
            self.save()?;
        }
        let _readers = lock(&self.root.join("file-readers-v2.lock"))?;
        let bytes = durable_file::read_object_file_bytes(&self.target.join(MARKER), RECORD_LIMIT)
            .map_err(error)?;
        durable_file::write_file_atomic(
            &self.root.join(MARKER),
            &bytes,
            durable_file::DurableAtomicFileOptions::new("conversion", "tmp").sync_all(),
        )
        .map_err(error)?;
        durable_file::sync_directory(&self.root).map_err(error)?;
        self.progress.publication_step = 3;
        self.save()
    }
    fn cleanup(&mut self) -> Result<bool> {
        // Original names are no longer an authority source. Only obsolete
        // representations are removed; live loose blobs and hash-tree caches stay.
        let roots = [
            self.root.join("blob-packs"),
            self.work.join("old-inline-bindings"),
            self.root.join("objects"),
            self.root.join("content-reservations"),
        ];
        if self.progress.lane >= roots.len() as u8 {
            return Ok(false);
        }
        let path = &roots[self.progress.lane as usize];
        if self.walker.is_none() {
            self.walker = Some(Walker::new(path)?);
        }
        if let Some(entry) = self.walker.as_mut().unwrap().next()? {
            let removable = match self.progress.lane {
                0 | 1 => entry.is_file(),
                2 => {
                    entry.file_name().and_then(|name| name.to_str()) == Some("manifest.sobj")
                        || entry
                            .file_name()
                            .and_then(|name| name.to_str())
                            .is_some_and(|name| name.starts_with(".content-"))
                }
                3 => {
                    entry.is_file()
                        && entry.extension().and_then(|name| name.to_str()) == Some("sobj")
                        && entry != self.root.join(MARKER)
                }
                _ => false,
            };
            if removable {
                durable_file::remove_file_if_exists_and_sync_parent(&entry).map_err(error)?;
            }
            Ok(true)
        } else {
            self.walker = None;
            self.progress.lane += 1;
            Ok(true)
        }
    }
}
fn hash_hex(hash: &str) -> Result<&str> {
    hash.strip_prefix("blake3:")
        .filter(|hex| {
            hex.len() == 64
                && hex
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        })
        .ok_or_else(|| error("invalid content hash"))
}
fn modified_nanos(path: &Path) -> Result<u64> {
    Ok(fs::metadata(path)
        .map_err(|e| io(path, e))?
        .modified()
        .map_err(error)?
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u128::from(u64::MAX)) as u64)
}

fn retired_content_root_catalog(fields: &serde_json::Map<String, serde_json::Value>) -> bool {
    let recognized_catalog = |value: &serde_json::Value| {
        value
            .get("format_version")
            .and_then(serde_json::Value::as_u64)
            .is_some_and(|version| version == 1 || version == 2)
            && value
                .get("topology_digest")
                .is_some_and(serde_json::Value::is_string)
            && value
                .get("generated_at_unix_nanos")
                .and_then(serde_json::Value::as_u64)
                .is_some()
    };
    if !fields.contains_key("catalog_json") && !fields.contains_key("storage_format_version") {
        return recognized_catalog(&serde_json::Value::Object(fields.clone()));
    }
    let Some(json) = fields
        .get("catalog_json")
        .and_then(serde_json::Value::as_str)
    else {
        return false;
    };
    let Ok(payload) = serde_json::from_str::<serde_json::Value>(json) else {
        return false;
    };
    if !recognized_catalog(&payload) {
        return false;
    }
    if payload
        .get("format_version")
        .and_then(serde_json::Value::as_u64)
        == Some(1)
    {
        return true;
    }
    // Only a positively decoded historical proof family is retired. A torn or
    // malformed CURRENT envelope remains fail-closed normal store input.
    payload
        .get("fragments")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|fragments| {
            fragments.iter().any(|fragment| {
                fragment
                    .get("coverage")
                    .and_then(|coverage| coverage.get("complete"))
                    .and_then(|complete| complete.get("prefix_proof_version"))
                    .and_then(serde_json::Value::as_u64)
                    == Some(1)
            })
        })
}

#[cfg(test)]
mod original_inline_operation_tests {
    use super::*;
    #[derive(Serialize)]
    enum OriginalOperation {
        Bind {
            manifest: ContentManifest,
            published_at_unix_nanos: u64,
            reservation: Option<ReservationData>,
        },
    }
    #[derive(Serialize)]
    struct ReservationData {
        identity: ContentStoreReservationIdentity,
        reserved_at_unix_nanos: u64,
        causal_stamp: Option<OriginalCausalStamp>,
    }
    #[derive(Serialize)]
    struct OriginalCausalStamp {
        store_incarnation: String,
        catalog_scan_epoch: u64,
    }
    #[derive(Serialize)]
    struct OriginalRecord {
        format_version: u32,
        store_incarnation: String,
        generation: u64,
        sequence: u64,
        operation: OriginalOperation,
    }
    #[derive(Serialize)]
    struct OriginalEnvelope<'a> {
        catalog: &'a Catalog,
        checksum: String,
    }
    fn original_inline_source(root: &Path, bytes: &[u8]) -> ContentManifest {
        let manifest = ContentManifest {
            object_id: "original-inline-object".into(),
            version: "original-v1".into(),
            content_hash: format!("blake3:{}", blake3::hash(bytes).to_hex()),
            size_bytes: bytes.len() as u64,
            extents: Vec::new(),
        };
        let catalog = Catalog {
            format_version: 1,
            store_incarnation: "original-inline-incarnation".into(),
            generation: 1,
            legacy_files_are_authoritative: true,
            segments: vec![Segment {
                id: 0,
                sealed_end: None,
                sealed_digest: None,
            }],
        };
        let path = root.join("inline-bindings/generations/0000000000000001/0000000000000000.slog");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let record = OriginalRecord {
            format_version: 1,
            store_incarnation: catalog.store_incarnation.clone(),
            generation: 1,
            sequence: 1,
            operation: OriginalOperation::Bind {
                manifest: manifest.clone(),
                published_at_unix_nanos: 1234,
                reservation: Some(ReservationData {
                    identity: ContentStoreReservationIdentity {
                        object_id: manifest.object_id.clone(),
                        version: manifest.version.clone(),
                        content_hash: manifest.content_hash.clone(),
                    },
                    reserved_at_unix_nanos: 1234,
                    causal_stamp: Some(OriginalCausalStamp {
                        store_incarnation: catalog.store_incarnation.clone(),
                        catalog_scan_epoch: 0,
                    }),
                }),
            },
        };
        let encoded = serde_ext::to_bytes(&record, 1, LengthMode::U32).unwrap();
        let mut writer = sinomo_core::storage::checksummed_frame::FrameAppender::open(
            &path,
            FrameLimits::new(64 * 1024, 16 * 1024).unwrap(),
        )
        .unwrap();
        writer.append_bytes(&encoded, bytes).unwrap();
        drop(writer);
        let checksum = blake3::hash(&serde_ext::to_bytes(&catalog, 1, LengthMode::U32).unwrap())
            .to_hex()
            .to_string();
        write(
            &root.join("inline-bindings/current.sobj"),
            &OriginalEnvelope {
                catalog: &catalog,
                checksum,
            },
        )
        .unwrap();
        manifest
    }
    #[test]
    fn original_pascal_inline_bind_converts_real_framed_bytes_and_reopens_current() {
        // Original producer00f51166: externally tagged PascalCase, same fields.
        // This is source DATA; no metadata visibility/guard/quorum is installed.
        let root = sinomo_testdir::TestDir::new("original-inline-pascal-bind");
        let bytes = b"actual original inline payload";
        let manifest = original_inline_source(root.path(), bytes);
        let converter = ContentConverter::default();
        let mut ready = false;
        for _ in 0..1000 {
            match converter.prepare(root.path(), 1).unwrap() {
                DfsStoragePreparation::Ready => {
                    ready = true;
                    break;
                }
                DfsStoragePreparation::Pending { processed_records } => {
                    assert!(processed_records <= 1)
                }
            }
        }
        assert!(ready);
        drop(converter);
        let current = LocalContentStore::new(root.path()).unwrap();
        assert_eq!(
            current
                .read_manifest(&manifest.object_id, &manifest.version)
                .unwrap(),
            manifest
        );
        assert_eq!(
            current
                .read_object_version(&manifest.object_id, &manifest.version)
                .unwrap()
                .1,
            bytes
        );
        assert_eq!(current.read_blob(&manifest.content_hash).unwrap(), bytes);
        assert!(current.scrub().unwrap().is_healthy());
        drop(current);
        assert_eq!(
            LocalContentStore::new(root.path())
                .unwrap()
                .read_object_version(&manifest.object_id, &manifest.version)
                .unwrap()
                .1,
            bytes
        );
    }
    #[test]
    fn original_inline_decoder_rejects_invented_lowercase_and_unknown_variants() {
        let root = sinomo_testdir::TestDir::new("original-inline-unknown-negative");
        let manifest = original_inline_source(root.path(), b"actual bytes");
        for tag in ["bind", "UnknownOperation"] {
            let operation = serde_json::json!({(tag): {
                "manifest":manifest, "published_at_unix_nanos":1234, "reservation":null,
            }});
            let value = serde_json::json!({
                "format_version":1,"store_incarnation":"original-inline-incarnation",
                "generation":1,"sequence":1,"operation":operation,
            });
            let bytes = serde_ext::to_bytes(&value, 1, LengthMode::U32).unwrap();
            assert!(serde_ext::from_bytes::<InlineRecord>(&bytes).is_err());
        }
    }
    #[test]
    #[ignore = "requires exact copied original segment/catalog paths; no fabricated capture"]
    fn copied_original_inline_segment_decodes_every_checked_frame() {
        let segment = PathBuf::from(
            std::env::var("SINOMO_MIGRATION_ORIGINAL_INLINE_SEGMENT")
                .expect("actual copied segment"),
        );
        let catalog_path = PathBuf::from(
            std::env::var("SINOMO_MIGRATION_ORIGINAL_INLINE_CATALOG")
                .expect("actual copied catalog"),
        );
        let envelope: Envelope = read(&catalog_path).unwrap();
        let catalog = envelope.catalog;
        assert_eq!(catalog.format_version, 1);
        assert_eq!(catalog.generation, 1);
        assert!(catalog.legacy_files_are_authoritative);
        assert_eq!(
            blake3::hash(&serde_ext::to_bytes(&catalog, 1, LengthMode::U32).unwrap())
                .to_hex()
                .as_str(),
            envelope.checksum
        );
        let expected_segment = catalog.segments.first().expect("actual segment0");
        assert_eq!(expected_segment.id, 0);
        let (actual_digest, actual_bytes) =
            durable_file::file_blake3_hash_and_size(&segment).unwrap();
        assert_eq!(expected_segment.sealed_end, Some(actual_bytes));
        assert_eq!(
            expected_segment.sealed_digest.as_deref(),
            Some(actual_digest.to_hex().as_str())
        );
        let mut file = File::open(&segment).unwrap();
        let mut sequence = 0;
        loop {
            match read_frame(
                &mut file,
                &segment,
                FrameLimits::new(64 * 1024, 16 * 1024).unwrap(),
            )
            .unwrap()
            {
                FrameRead::Complete(frame) => {
                    let record: InlineRecord = serde_ext::from_bytes(frame.header()).unwrap();
                    assert_eq!(record.format_version, catalog.format_version);
                    assert_eq!(record.store_incarnation, catalog.store_incarnation);
                    assert_eq!(record.generation, catalog.generation);
                    assert_eq!(record.sequence, sequence + 1);
                    sequence = record.sequence;
                    match record.operation {
                        Operation::Blob {
                            content_hash,
                            size_bytes,
                            ..
                        } => {
                            assert_eq!(size_bytes, frame.payload_bytes());
                            assert_eq!(
                                content_hash,
                                format!(
                                    "blake3:{}",
                                    blake3::Hash::from_bytes(*frame.payload_checksum()).to_hex()
                                )
                            );
                        }
                        Operation::Bind { manifest, .. } => {
                            assert_eq!(manifest.size_bytes, frame.payload_bytes());
                            assert_eq!(
                                manifest.content_hash,
                                format!(
                                    "blake3:{}",
                                    blake3::Hash::from_bytes(*frame.payload_checksum()).to_hex()
                                )
                            );
                        }
                        Operation::Link { .. } | Operation::RestoreVersion { .. } => {
                            assert_eq!(frame.payload_bytes(), 0);
                        }
                        _ => {}
                    }
                    file.seek(SeekFrom::Start(frame.frame_end())).unwrap();
                }
                FrameRead::CleanEof { .. } => break,
                _ => panic!("actual copied sealed segment has a partial or corrupt frame"),
            }
        }
        assert!(sequence > 0);
    }
}
