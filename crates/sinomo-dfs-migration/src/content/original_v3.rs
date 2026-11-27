//! Read-only interpretation of the retired format-3 / inline-v2 source.
//! This driver owns no queue, runtime, or publication authority. Its caller
//! owns the inventory job, source/target paths, cancellation and target exposure.
use super::*;
use std::collections::BTreeMap;
use sinomo_core::storage::checksummed_frame::{FrameLimits, FrameRead, read_frame};
use sinomo_dfs::{ContentStoreStagingRow, ContentStoreStagingWriter};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

const HEADER_LIMIT: u64 = 64 * 1024;
const PAYLOAD_LIMIT: u64 = 16 * 1024;
const FRAME_WORK: u64 = HEADER_LIMIT + PAYLOAD_LIMIT + 256;
const CATALOG_LIMIT: u64 = 64 * 1024 * 1024;

/// Bounded original DATA identity, not a complete-frame or publication proof.
/// The owner holds its source custody lock across this observation and relocation.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct OriginalV3SourceIdentity {
    pub store_incarnation: String,
    pub catalog_generation: u64,
    pub marker_digest: String,
    pub catalog_digest: String,
    pub active_file_length: u64,
    pub active_file_identity_digest: String,
}
pub(crate) fn probe_original_v3(
    source: &Path,
    max_header_bytes: u64,
) -> Result<OriginalV3SourceIdentity> {
    probe_publication_source(source, max_header_bytes, None)
}

pub(crate) fn probe_publication_source(
    source: &Path,
    max_header_bytes: u64,
    recorded_device: Option<u64>,
) -> Result<OriginalV3SourceIdentity> {
    let marker_path = source.join(MARKER);
    let catalog_path = source.join("inline-bindings/current.sobj");
    let marker_len = fs::metadata(&marker_path)
        .map_err(|e| io(&marker_path, e))?
        .len();
    let catalog_len = fs::metadata(&catalog_path)
        .map_err(|e| io(&catalog_path, e))?
        .len();
    if marker_len.saturating_add(catalog_len) > max_header_bytes {
        return Err(error(
            "original-v3 identity header exceeds its explicit setup byte budget",
        ));
    }
    let (marker, bytes, catalog) = read_source(source)?;
    let active = segment_path(source, &catalog, catalog.segments.last().unwrap());
    let identity = file_identity(&active)?.in_checkpoint_namespace(source, recorded_device)?;
    Ok(OriginalV3SourceIdentity {
        store_incarnation: catalog.store_incarnation,
        catalog_generation: catalog.generation,
        marker_digest: blake3::hash(&marker).to_hex().to_string(),
        catalog_digest: blake3::hash(&bytes).to_hex().to_string(),
        active_file_length: identity.length,
        active_file_identity_digest: blake3::hash(&encode(&identity)?).to_hex().to_string(),
    })
}

#[derive(Clone, Debug)]
pub(crate) enum OriginalV3Selection {
    All,
    Object(String),
    Version { object_id: String, version: String },
    Blob(String),
}
pub(crate) struct OriginalV3Budget<'a> {
    pub max_records: usize,
    /// Input/verification bytes per step; bounded checkpoint/index writes are
    /// additional bookkeeping constrained by max_records and the source frame cap.
    pub max_bytes: u64,
    pub deadline: Instant,
    pub cancelled: &'a AtomicBool,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum OriginalV3UnitResult {
    PendingIndex,
    Pending,
    Covered,
    Absent,
    Blocked(String),
}
pub(crate) struct OriginalV3Step {
    pub processed_records: usize,
    /// Input and verification bytes; not a measurement of total filesystem I/O.
    pub processed_bytes: u64,
    pub coverage_complete: bool,
    pub unit_result: OriginalV3UnitResult,
}

#[derive(Deserialize)]
struct OriginalMarker {
    format_version: u32,
    marker: String,
    store_incarnation: String,
    catalog_scan_epoch: u64,
    content_binding_layout: String,
}
#[derive(Clone, Serialize, Deserialize)]
struct Catalog {
    format_version: u32,
    store_incarnation: String,
    generation: u64,
    segments: Vec<Segment>,
}
#[derive(Clone, Serialize, Deserialize)]
struct Segment {
    id: u64,
    sealed_end: Option<u64>,
    sealed_digest: Option<String>,
}
#[derive(Deserialize)]
#[cfg_attr(test, derive(Serialize))]
struct Envelope {
    catalog: Catalog,
    checksum: String,
}
#[derive(Deserialize)]
#[cfg_attr(test, derive(Serialize))]
struct Record {
    format_version: u32,
    store_incarnation: String,
    generation: u64,
    sequence: u64,
    operation: Operation,
}
#[derive(Deserialize)]
#[cfg_attr(test, derive(Serialize))]
struct Reservation {
    identity: ContentStoreReservationIdentity,
    reserved_at_unix_nanos: u64,
    causal_stamp: Stamp,
}
#[derive(Deserialize)]
#[cfg_attr(test, derive(Serialize))]
struct Stamp {
    store_incarnation: String,
    catalog_scan_epoch: u64,
}
#[derive(Deserialize)]
#[cfg_attr(test, derive(Serialize))]
struct RepairSubject {
    queue_incarnation: String,
    producer: String,
    target: String,
    intent_id: String,
    request_id: String,
}
#[derive(Deserialize)]
#[cfg_attr(test, derive(Serialize))]
enum RepairSource {
    Manifest(ContentManifest),
    Blob {
        content_hash: String,
        size_bytes: u64,
    },
}
#[derive(Deserialize)]
#[cfg_attr(test, derive(Serialize))]
struct RepairRoot {
    subject: RepairSubject,
    original_command_digest: String,
    store_incarnation: String,
    source: RepairSource,
}
#[derive(Deserialize)]
#[cfg_attr(test, derive(Serialize))]
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
    RetainRepairPayload(RepairRoot),
    ReleaseRepairPayload(RepairRoot),
    RestoreRepairPayload {
        root: RepairRoot,
        released: bool,
    },
    DeleteVersion {
        object_id: String,
        version: String,
        content_hash: String,
    },
    ExternalBinding {
        manifest: ContentManifest,
        published_at_unix_nanos: u64,
        reservation: Option<Reservation>,
    },
    RemoveBlob {
        content_hash: String,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
enum Key {
    Version(String, String),
    Blob(String),
    Reservation(ContentStoreReservationIdentity),
}
#[derive(Clone, Serialize, Deserialize)]
enum Value {
    Version {
        manifest: ContentManifest,
        published: u64,
    },
    DeletedVersion {
        object_id: String,
        version: String,
        hash: String,
    },
    Blob {
        source: BlobSource,
        published: u64,
    },
    DeletedBlob(String),
    Reservation {
        identity: ContentStoreReservationIdentity,
        not_before: u64,
    },
    Released(ContentStoreReservationIdentity),
}
#[derive(Serialize, Deserialize)]
struct Indexed {
    copy: Option<CopyWork>,
    unit_log_end: u64,
    key: Key,
    sequence: u64,
    value: Value,
}
#[derive(Serialize, Deserialize)]
struct Checked<T> {
    value: T,
    checksum: String,
}
#[derive(Serialize, Deserialize)]
struct Progress {
    inventory_id: String,
    source_root: PathBuf,
    target_root: PathBuf,
    target_incarnation: String,
    marker_digest: String,
    catalog_digest: String,
    active_len: u64,
    active_identity: FileIdentity,
    active_digest: Option<String>,
    validation_segment: usize,
    loose_bucket: u16,
    segment: usize,
    offset: u64,
    sequence: u64,
    segment_verified: bool,
    index_complete: bool,
    units_end: u64,
    import_offset: u64,
    complete: bool,
    priority: Option<Priority>,
}
#[derive(Serialize, Deserialize)]
struct CopyWork {
    hash: String,
    source: BlobSource,
    copied: u64,
    staged: PathBuf,
    existing: Option<PathBuf>,
}
#[derive(Serialize, Deserialize)]
struct Priority {
    selection: String,
    offset: u64,
    seen: bool,
}
#[derive(Serialize, Deserialize, Eq, PartialEq)]
struct FileIdentity {
    length: u64,
    modified: Option<std::time::SystemTime>,
    created: Option<std::time::SystemTime>,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    changed: (i64, i64),
}
impl FileIdentity {
    fn in_checkpoint_namespace(self, anchor: &Path, recorded_device: Option<u64>) -> Result<Self> {
        #[cfg(target_os = "linux")]
        if let Some(device) = recorded_device {
            if device != self.device && self.created.is_some()
                && checkpoint_device::can_rebind(anchor, device, self.device).map_err(|e| io(anchor, e))?
            {
                return Ok(Self { device, ..self });
            }
        }
        #[cfg(not(target_os = "linux"))]
        let _ = (anchor, recorded_device);
        Ok(self)
    }

    fn matches_checkpoint(&self, path: &Path, checkpoint: &Path) -> Result<bool> {
        #[cfg(unix)]
        let device = Some(self.device);
        #[cfg(not(unix))]
        let device = None;
        Ok(*self == file_identity(path)?.in_checkpoint_namespace(checkpoint, device)?)
    }
}
fn file_identity(path: &Path) -> Result<FileIdentity> {
    let meta = fs::symlink_metadata(path).map_err(|e| io(path, e))?;
    if !meta.is_file() {
        return Err(error("original-v3 source is not a regular retained file"));
    }
    #[cfg(unix)]
    use std::os::unix::fs::MetadataExt;
    Ok(FileIdentity {
        length: meta.len(),
        modified: meta.modified().ok(),
        created: meta.created().ok(),
        #[cfg(unix)]
        device: meta.dev(),
        #[cfg(unix)]
        inode: meta.ino(),
        #[cfg(unix)]
        changed: (meta.ctime(), meta.ctime_nsec()),
    })
}
enum Warm {
    Active {
        offset: u64,
        hasher: blake3::Hasher,
    },
    Segment {
        index: usize,
        verifier: sinomo_dfs::ContentImportBlobVerifier,
    },
}
#[derive(Eq, PartialEq)]
struct WarmDirectory {
    created: Option<std::time::SystemTime>,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}
fn directory_identity(path: &Path) -> Result<WarmDirectory> {
    let metadata = fs::symlink_metadata(path).map_err(|e| io(path, e))?;
    if !metadata.is_dir() {
        return Err(error("source3 work directory changed its physical kind"));
    }
    #[cfg(unix)]
    use std::os::unix::fs::MetadataExt;
    Ok(WarmDirectory {
        created: metadata.created().ok(),
        #[cfg(unix)]
        device: metadata.dev(),
        #[cfg(unix)]
        inode: metadata.ino(),
    })
}

#[derive(Default)]
pub(crate) struct OriginalV3Converter {
    job: Option<String>,
    warm: Option<Warm>,
    // Incomplete hash passes belong to their admitted blob. Keep a bounded set
    // so a small demanded blob can preempt a background verification without
    // resetting it or retaining an unbounded number of file handles.
    blob_verifiers: BTreeMap<String, sinomo_dfs::ContentImportBlobVerifier>,
    loose_reader: Option<(u16, fs::ReadDir)>,
    directories: Option<Vec<WarmDirectory>>,
    checkpoint_bytes: Option<Vec<u8>>,
    writer: Option<std::sync::Arc<ContentStoreStagingWriter>>,
}

struct SourceLock(File);
impl Drop for SourceLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.0);
    }
}
fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    serde_ext::to_bytes(value, 2, LengthMode::U32).map_err(error)
}
fn checked_write<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let bytes = encode(value)?;
    write(
        path,
        &Checked {
            value,
            checksum: blake3::hash(&bytes).to_hex().to_string(),
        },
    )
}
fn checked_read<T: Serialize + for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let bytes = durable_file::read_object_file_bytes(path, HEADER_LIMIT * 4).map_err(error)?;
    let checked: Checked<T> = serde_ext::from_bytes(&bytes).map_err(error)?;
    if blake3::hash(&encode(&checked.value)?).to_hex().as_str() != checked.checksum {
        return Err(error("original-v3 checkpoint checksum changed"));
    }
    Ok(checked.value)
}
fn hash_key(key: &Key) -> Result<String> {
    Ok(blake3::hash(&encode(key)?).to_hex().to_string())
}
fn row_path(work: &Path, key: &Key) -> Result<PathBuf> {
    Ok(work.join("rows").join(hash_key(key)?))
}
fn segment_path(root: &Path, catalog: &Catalog, segment: &Segment) -> PathBuf {
    root.join("inline-bindings/generations")
        .join(format!("{:016x}", catalog.generation))
        .join(format!("{:016x}.slog", segment.id))
}
pub(super) fn original_marker_incarnation(root: &Path) -> Result<String> {
    let bytes =
        durable_file::read_object_file_bytes(&root.join(MARKER), HEADER_LIMIT).map_err(error)?;
    let marker: OriginalMarker = serde_ext::from_bytes(&bytes).map_err(error)?;
    if marker.format_version != 3
        || marker.marker != "sinomo-dfs-content-reservation-batches-v1"
        || marker.content_binding_layout != "inline-bindings-v2"
        || marker.store_incarnation.parse::<sinomo_core::Id>().is_err()
    {
        return Err(error(
            "legacy staged target has no exact format3 incarnation",
        ));
    }
    Ok(marker.store_incarnation)
}

fn read_source(root: &Path) -> Result<(Vec<u8>, Vec<u8>, Catalog)> {
    let marker =
        durable_file::read_object_file_bytes(&root.join(MARKER), HEADER_LIMIT).map_err(error)?;
    let value: OriginalMarker = serde_ext::from_bytes(&marker).map_err(error)?;
    if value.marker != "sinomo-dfs-content-reservation-batches-v1"
        || value.format_version != 3
        || value.content_binding_layout != "inline-bindings-v2"
    {
        return Err(error(
            "original-v3 source requires exact format3 inline-v2 marker",
        ));
    }
    let bytes = durable_file::read_object_file_bytes(
        &root.join("inline-bindings/current.sobj"),
        CATALOG_LIMIT,
    )
    .map_err(error)?;
    let envelope: Envelope = serde_ext::from_bytes(&bytes).map_err(error)?;
    let catalog = envelope.catalog;
    if catalog.format_version != 2
        || catalog.generation == 0
        || catalog
            .store_incarnation
            .parse::<sinomo_core::Id>()
            .is_err()
        || catalog.segments.is_empty()
        || blake3::hash(&encode(&catalog)?).to_hex().as_str() != envelope.checksum
    {
        return Err(error("original-v3 catalog identity/checksum is invalid"));
    }
    if value.store_incarnation != catalog.store_incarnation {
        return Err(error("original-v3 marker/catalog incarnation differs"));
    }
    for (i, segment) in catalog.segments.iter().enumerate() {
        let sealed = i + 1 != catalog.segments.len();
        if (i > 0 && catalog.segments[i - 1].id >= segment.id)
            || sealed != segment.sealed_end.is_some()
            || sealed != segment.sealed_digest.is_some()
        {
            return Err(error("original-v3 catalog segment ordering/seals differ"));
        }
    }
    Ok((marker, bytes, catalog))
}
fn hash_valid(hash: &str) -> bool {
    hash.strip_prefix("blake3:").is_some_and(|h| {
        h.len() == 64
            && h.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    })
}
fn validate_identity(identity: &ContentStoreReservationIdentity) -> Result<()> {
    if [&identity.object_id, &identity.version].iter().any(|part| {
        part.is_empty()
            || part.as_str() == "."
            || part.as_str() == ".."
            || part.contains(['/', '\\', '\0'])
    }) || !hash_valid(&identity.content_hash)
    {
        return Err(error("original-v3 invalid immutable identity"));
    }
    Ok(())
}
fn reservation_value(record: Reservation, incarnation: &str) -> Result<(Key, Value)> {
    validate_identity(&record.identity)?;
    if record.causal_stamp.store_incarnation != incarnation {
        return Err(error("original-v3 reservation provenance is invalid"));
    }
    Ok((
        Key::Reservation(record.identity.clone()),
        Value::Reservation {
            identity: record.identity,
            not_before: record.reserved_at_unix_nanos,
        },
    ))
}

// Independent files in one source record need no ordering between their syncs.
// Join every bounded group before publishing names or advancing the source
// cursor. No worker, writable handle, or queued task outlives this call.
fn sync_index_group<T: Send, R: Send>(
    items: impl IntoIterator<Item = T>,
    sync: impl Fn(T) -> Result<R> + Sync,
) -> Result<Vec<R>> {
    let mut items = items.into_iter().peekable();
    let span = tracing::Span::current();
    let dispatch = tracing::dispatcher::get_default(Clone::clone);
    std::thread::scope(|scope| {
        let mut result = Vec::new();
        while items.peek().is_some() {
            let mut workers = Vec::new();
            for item in items.by_ref().take(8) {
                let sync = &sync;
                let span = span.clone();
                let dispatch = dispatch.clone();
                workers.push(
                    std::thread::Builder::new()
                        .name("dfs-index-sync".into())
                        .spawn_scoped(scope, move || {
                            tracing::dispatcher::with_default(&dispatch, || {
                                span.in_scope(|| sync(item))
                            })
                        })
                        .map_err(error)?,
                );
            }
            // Observe all outcomes, including when one barrier fails. The
            // owner cannot start replay while another worker still holds DATA.
            let outcomes = workers
                .into_iter()
                .map(|worker| {
                    worker
                        .join()
                        .map_err(|_| error("index synchronization worker panicked"))
                        .and_then(|result| result)
                })
                .collect::<Vec<_>>();
            result.extend(outcomes.into_iter().collect::<Result<Vec<_>>>()?);
        }
        Ok(result)
    })
}

// Publish one source record with a single barrier per shared log/directory. The
// source cursor remains the recovery owner and cannot advance until publication
// finishes. Temporary files stay outside refs: an interrupted private write must
// never count as a live content root.
#[derive(Default)]
struct IndexPublication {
    staging_root: PathBuf,
    logs: Vec<(PathBuf, File)>,
    staged: Vec<(PathBuf, PathBuf, Option<File>)>,
    removed: Vec<PathBuf>,
    directories: std::collections::BTreeSet<PathBuf>,
}

impl IndexPublication {
    fn append(&mut self, path: PathBuf, initial_end: u64, bytes: &[u8]) -> Result<u64> {
        let index = match self.logs.iter().position(|(held, _)| held == &path) {
            Some(index) => index,
            None => {
                let mut file = OpenOptions::new()
                    .create(true)
                    .read(true)
                    .write(true)
                    .truncate(false)
                    .open(&path)
                    .map_err(|e| io(&path, e))?;
                file.set_len(initial_end).map_err(|e| io(&path, e))?;
                file.seek(SeekFrom::Start(initial_end))
                    .map_err(|e| io(&path, e))?;
                self.directories
                    .insert(path.parent().expect("index log parent").to_owned());
                self.logs.push((path.clone(), file));
                self.logs.len() - 1
            }
        };
        let file = &mut self.logs[index].1;
        sinomo_core::storage::io_trace::write_all(file, &(bytes.len() as u32).to_le_bytes())
            .and_then(|_| sinomo_core::storage::io_trace::write_all(file, bytes))
            .map_err(|e| io(&path, e))?;
        file.stream_position().map_err(|e| io(&path, e))
    }

    fn write<T: Serialize>(&mut self, destination: &Path, value: &T) -> Result<()> {
        // A Bind contributes a reservation and a version to the same object
        // log. Retain only its final end marker; the log itself retains both.
        if let Some(index) = self
            .staged
            .iter()
            .position(|(_, path, _)| path == destination)
        {
            let (temporary, _, file) = self.staged.remove(index);
            drop(file);
            fs::remove_file(&temporary).map_err(|e| io(&temporary, e))?;
        }
        let value = Checked {
            value,
            checksum: blake3::hash(&encode(value)?).to_hex().to_string(),
        };
        let bytes = serde_ext::to_bytes(&value, 1, LengthMode::U32).map_err(error)?;
        let parent = destination
            .parent()
            .ok_or_else(|| error("index row has no parent"))?;
        let temporary = self.staging_root.join(format!(
            ".source3-index-{}.tmp",
            sinomo_core::Id::generate()
        ));
        let mut file = durable_file::create_private_staged_file(&temporary).map_err(error)?;
        self.staged.push((temporary, destination.to_owned(), None));
        sinomo_core::storage::io_trace::write_all(&mut file, &bytes)
            .map_err(|e| io(destination, e))?;
        self.staged.last_mut().expect("retained stage").2 = Some(file);
        self.directories.insert(parent.to_owned());
        self.directories.insert(self.staging_root.clone());
        Ok(())
    }

    fn directory(&mut self, path: &Path) -> Result<()> {
        match fs::create_dir(path) {
            Ok(()) => {}
            Err(failure) if failure.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(failure) => return Err(io(path, failure)),
        }
        directory_identity(path)?;
        self.directories.insert(path.to_owned());
        self.directories
            .insert(path.parent().expect("index directory parent").to_owned());
        Ok(())
    }

    fn remove(&mut self, path: PathBuf) {
        self.directories
            .insert(path.parent().expect("index reference parent").to_owned());
        self.removed.push(path);
    }

    fn publish(mut self) -> Result<()> {
        // All data barriers precede every rename, including the per-object
        // log end marker. A crash can expose only complete files over durable
        // log bytes; the unchanged source cursor then replays this same row.
        sync_index_group(&mut self.logs, |(path, file)| {
            durable_file::sync_open_file(file, path, durable_file::DurableFileSync::All)
                .map_err(error)
        })?;
        #[cfg(test)]
        self.interrupt("logs")?;
        #[cfg(test)]
        if self.logs.len() == 2
            && FAIL_INDEX_PUBLICATION.with(|fault| {
                if fault.get() == Some("missing-stage") {
                    fault.set(None);
                    true
                } else {
                    false
                }
            })
        {
            // A real failed path/handle identity check in one barrier must
            // prevent every install, while its sibling workers still drain.
            let path = &self.staged[0].0;
            fs::rename(path, path.with_extension("orphan")).map_err(|e| io(path, e))?;
        }
        let synced = sync_index_group(&mut self.staged, |(path, destination, file)| {
            Ok((
                durable_file::sync_private_staged_file(
                    path,
                    file.take().expect("index stage writer"),
                )
                .map_err(error)?,
                destination.clone(),
            ))
        })?;
        #[cfg(test)]
        self.interrupt("files")?;
        for (stage, destination) in &synced {
            // Index rows are replacements under the held original conversion
            // owner, not immutable no-clobber content publications.
            durable_file::replace_file(stage.path(), destination).map_err(error)?;
            #[cfg(test)]
            self.interrupt("first-install")?;
        }
        for path in &self.removed {
            match fs::remove_file(path) {
                Ok(()) => {}
                Err(failure) if failure.kind() == std::io::ErrorKind::NotFound => {}
                Err(failure) => return Err(io(path, failure)),
            }
        }
        #[cfg(test)]
        self.interrupt("before-directories")?;
        sync_index_group(&self.directories, |directory| {
            durable_file::sync_directory(directory).map_err(error)
        })?;
        #[cfg(test)]
        self.interrupt("directories")?;
        #[cfg(test)]
        if !self.logs.is_empty() && FAIL_AFTER_INDEX_ROW.with(|fault| fault.replace(false)) {
            return Err(error("injected index row before cursor publication"));
        }
        Ok(())
    }

    #[cfg(test)]
    fn interrupt(&mut self, phase: &str) -> Result<()> {
        if self.logs.len() == 2
            && FAIL_INDEX_PUBLICATION.with(|fault| {
                if fault.get() == Some(phase) {
                    fault.set(None);
                    true
                } else {
                    false
                }
            })
        {
            // Leave the same installed prefix and private files as process loss.
            // Close handles, but do not run the normal error-path unlink cleanup.
            self.staged.clear();
            return Err(error(format!("injected index publication at {phase}")));
        }
        Ok(())
    }
}

impl Drop for IndexPublication {
    fn drop(&mut self) {
        for (temporary, _, file) in &mut self.staged {
            file.take();
            let _ = fs::remove_file(temporary);
        }
    }
}
fn index_row(
    work: &Path,
    progress: &mut Progress,
    key: Key,
    value: Value,
    sequence: u64,
) -> Result<()> {
    let mut publication = IndexPublication::default();
    publication.staging_root = work.to_owned();
    stage_index_row(work, progress, key, value, sequence, &mut publication)?;
    publication.publish()
}

fn stage_index_row(
    work: &Path,
    progress: &mut Progress,
    key: Key,
    value: Value,
    sequence: u64,
    publication: &mut IndexPublication,
) -> Result<()> {
    // Duplicate log entries after replay are harmless: only the final indexed
    // row is imported and the target arbitrates current state.
    let key_bytes = encode(&Checked {
        checksum: blake3::hash(&encode(&key)?).to_hex().to_string(),
        value: &key,
    })?;
    let path = work.join("units");
    progress.units_end = publication.append(path, progress.units_end, &key_bytes)?;
    unit_log(work, &key, &key_bytes, publication)?;
    let row = row_path(work, &key)?;
    if row.exists() {
        let previous: Indexed = checked_read(&row)?;
        if previous.key != key {
            return Err(error("original-v3 index identity collision"));
        }
        if previous.sequence > sequence {
            return Err(error(
                "original-v3 index is ahead of its validated source cursor",
            ));
        }
        fn version_hash(value: &Value) -> Option<&str> {
            match value {
                Value::Version { manifest, .. } => Some(manifest.content_hash.as_str()),
                Value::DeletedVersion { hash, .. } => Some(hash.as_str()),
                _ => None,
            }
        }
        if version_hash(&previous.value)
            .zip(version_hash(&value))
            .is_some_and(|(old, new)| old != new)
        {
            return Err(error("original-v3 immutable version hash changed"));
        }
        if let (Value::Version { manifest: old, .. }, Value::Version { manifest: new, .. }) =
            (&previous.value, &value)
        {
            if old != new {
                return Err(error("original-v3 immutable version changed"));
            }
        }
    }
    let root_hash = match &value {
        Value::Version { manifest, .. } => Some((&manifest.content_hash, true)),
        Value::DeletedVersion { hash, .. } => Some((hash, false)),
        Value::Reservation { identity, .. } => Some((&identity.content_hash, true)),
        Value::Released(identity) => Some((&identity.content_hash, false)),
        _ => None,
    };
    if let Some((hash, live)) = root_hash {
        let directory = work.join("refs").join(&hash[7..]);
        publication.directory(&directory)?;
        let reference = directory.join(hash_key(&key)?);
        if live {
            publication.write(&reference, &key)?;
        } else {
            publication.remove(reference);
        }
    }
    publication.write(
        &row,
        &Indexed {
            copy: None,
            unit_log_end: progress.units_end,
            key,
            sequence,
            value,
        },
    )?;
    Ok(())
}
#[cfg(test)]
thread_local! { static FAIL_AFTER_INDEX_ROW: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
#[cfg(test)]
thread_local! { static FAIL_INDEX_PUBLICATION: std::cell::Cell<Option<&'static str>> = const { std::cell::Cell::new(None) }; }

fn project_record(
    work: &Path,
    progress: &mut Progress,
    catalog: &Catalog,
    record: Record,
    payload_hash: String,
    size: u64,
    locator: BlobSource,
) -> Result<()> {
    let seq = record.sequence;
    let mut rows = Vec::new();
    let blob = |hash: String, expected: u64, published: u64| -> Result<(Key, Value)> {
        if !hash_valid(&hash) || expected != size || hash != payload_hash {
            return Err(error("original-v3 inline payload hash/length differs"));
        }
        Ok((
            Key::Blob(hash.clone()),
            Value::Blob {
                source: BlobSource {
                    hash,
                    ..locator.clone()
                },
                published,
            },
        ))
    };
    let mut version = |manifest: ContentManifest,
                       published: u64,
                       reservation: Option<Reservation>,
                       strict_timestamp: bool|
     -> Result<()> {
        validate_identity(&ContentStoreReservationIdentity {
            object_id: manifest.object_id.clone(),
            version: manifest.version.clone(),
            content_hash: manifest.content_hash.clone(),
        })?;
        let previous_path = row_path(
            work,
            &Key::Version(manifest.object_id.clone(), manifest.version.clone()),
        )?;
        if strict_timestamp && previous_path.exists() {
            let previous: Indexed = checked_read(&previous_path)?;
            if matches!(previous.value,Value::Version{published:old,..} if old!=published) {
                return Err(error(
                    "original-v3 binding changed its durable publication time",
                ));
            }
        }
        if let Some(reservation) = reservation {
            if reservation.identity.object_id != manifest.object_id
                || reservation.identity.version != manifest.version
                || reservation.identity.content_hash != manifest.content_hash
            {
                return Err(error("original-v3 binding reservation differs"));
            }
            rows.push(reservation_value(reservation, &catalog.store_incarnation)?);
        }
        rows.push((
            Key::Version(manifest.object_id.clone(), manifest.version.clone()),
            Value::Version {
                manifest,
                published,
            },
        ));
        Ok(())
    };
    let mut blob_row = None;
    match record.operation {
        Operation::Blob {
            content_hash,
            size_bytes,
            published_at_unix_nanos,
        } => {
            blob_row = Some(blob(content_hash, size_bytes, published_at_unix_nanos)?);
        }
        Operation::StageBlob {
            size_bytes,
            published_at_unix_nanos,
            reservation,
        } => {
            if size_bytes == 0 {
                return Err(error("original-v3 staged blob cannot be empty"));
            }
            blob_row = Some(blob(
                reservation.identity.content_hash.clone(),
                size_bytes,
                published_at_unix_nanos,
            )?);
            rows.push(reservation_value(reservation, &catalog.store_incarnation)?);
        }
        Operation::Bind {
            manifest,
            published_at_unix_nanos,
            reservation,
        } => {
            if !manifest.extents.is_empty() {
                return Err(error("original-v3 inline bind has extents"));
            }
            if manifest.size_bytes != 0 {
                blob_row = Some(blob(
                    manifest.content_hash.clone(),
                    manifest.size_bytes,
                    published_at_unix_nanos,
                )?);
            } else if size != 0 || manifest.content_hash != payload_hash {
                return Err(error("original-v3 empty bind differs"));
            }
            version(manifest, published_at_unix_nanos, reservation, true)?;
        }
        Operation::Link {
            manifest,
            published_at_unix_nanos,
            reservation,
        } => {
            if size != 0 {
                return Err(error("original-v3 link contains payload"));
            }
            let row: Indexed =
                checked_read(&row_path(work, &Key::Blob(manifest.content_hash.clone()))?)?;
            if !matches!(row.value, Value::Blob { source, .. } if source.size == manifest.size_bytes)
            {
                return Err(error("original-v3 link lacks its exact prior blob"));
            }
            version(manifest, published_at_unix_nanos, reservation, true)?;
        }
        Operation::ExternalBinding {
            manifest,
            published_at_unix_nanos,
            reservation,
        } => {
            if size != 0 {
                return Err(error("original-v3 external binding contains payload"));
            }
            version(manifest, published_at_unix_nanos, reservation, true)?;
        }
        Operation::RestoreVersion {
            manifest,
            published_at_unix_nanos,
        } => {
            if size != 0 {
                return Err(error("original-v3 restored version contains payload"));
            }
            version(manifest, published_at_unix_nanos, None, false)?;
        }
        Operation::Reserve(record) => {
            if size != 0 {
                return Err(error("original-v3 reservation contains payload"));
            }
            rows.push(reservation_value(record, &catalog.store_incarnation)?);
        }
        Operation::Release(identity) => {
            if size != 0 {
                return Err(error("original-v3 release contains payload"));
            }
            validate_identity(&identity)?;
            rows.push((
                Key::Reservation(identity.clone()),
                Value::Released(identity),
            ));
        }
        Operation::DeleteVersion {
            object_id,
            version,
            content_hash,
        } => {
            if size != 0 {
                return Err(error("original-v3 delete contains payload"));
            }
            validate_identity(&ContentStoreReservationIdentity {
                object_id: object_id.clone(),
                version: version.clone(),
                content_hash: content_hash.clone(),
            })?;
            rows.push((
                Key::Version(object_id.clone(), version.clone()),
                Value::DeletedVersion {
                    object_id,
                    version,
                    hash: content_hash,
                },
            ));
        }
        Operation::RemoveBlob { content_hash } => {
            if size != 0 || !hash_valid(&content_hash) {
                return Err(error("original-v3 blob deletion differs"));
            }
            let references = work.join("refs").join(&content_hash[7..]);
            match fs::read_dir(&references) {
                Ok(mut entries) => {
                    if entries
                        .next()
                        .transpose()
                        .map_err(|e| io(&references, e))?
                        .is_some()
                    {
                        return Err(error("original-v3 removed a blob with live roots"));
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(io(&references, e)),
            }
            rows.push((
                Key::Blob(content_hash.clone()),
                Value::DeletedBlob(content_hash),
            ));
        }
        // The format-3 producer already treated this removed repair custody as
        // non-retaining. Preserve its raw source in the archive, not new roots.
        Operation::RetainRepairPayload(_)
        | Operation::ReleaseRepairPayload(_)
        | Operation::RestoreRepairPayload { .. } => {
            if size != 0 {
                return Err(error("original-v3 retired repair record contains payload"));
            }
        }
    }
    if let Some(row) = blob_row {
        rows.push(row);
    }
    let mut publication = IndexPublication::default();
    publication.staging_root = work.to_owned();
    for (key, value) in rows {
        stage_index_row(work, progress, key, value, seq, &mut publication)?;
    }
    publication.publish()
}

fn key_at(work: &Path, offset: u64, end: u64) -> Result<Option<(Key, u64)>> {
    if offset == end {
        return Ok(None);
    }
    let path = work.join("units");
    let mut file = File::open(&path).map_err(|e| io(&path, e))?;
    file.seek(SeekFrom::Start(offset))
        .map_err(|e| io(&path, e))?;
    let mut length = [0; 4];
    file.read_exact(&mut length).map_err(|e| io(&path, e))?;
    let length = u32::from_le_bytes(length) as u64;
    if length > HEADER_LIMIT || offset.checked_add(4 + length).is_none_or(|next| next > end) {
        return Err(error("original-v3 unit index exceeds its checkpoint"));
    }
    let mut bytes = vec![0; length as usize];
    file.read_exact(&mut bytes).map_err(|e| io(&path, e))?;
    let checked: Checked<Key> = serde_ext::from_bytes(&bytes).map_err(error)?;
    if blake3::hash(&encode(&checked.value)?).to_hex().as_str() != checked.checksum {
        return Err(error("original-v3 unit index checksum differs"));
    }
    Ok(Some((checked.value, offset + 4 + length)))
}

fn selected(selection: &OriginalV3Selection, key: &Key) -> bool {
    match (selection, key) {
        (OriginalV3Selection::All, _) => true,
        (OriginalV3Selection::Object(id), Key::Version(object, _)) => id == object,
        (OriginalV3Selection::Object(id), Key::Reservation(root)) => id == &root.object_id,
        (OriginalV3Selection::Version { object_id, version }, Key::Version(object, v)) => {
            object_id == object && version == v
        }
        (OriginalV3Selection::Blob(hash), Key::Blob(actual)) => hash == actual,
        _ => false,
    }
}

fn unit_log(work: &Path, key: &Key, bytes: &[u8], publication: &mut IndexPublication) -> Result<()> {
    let object = match key {
        Key::Version(object, _) => object,
        Key::Reservation(identity) => &identity.object_id,
        _ => return Ok(()),
    };
    let stem = blake3::hash(object.as_bytes()).to_hex().to_string();
    let end_path = work.join("objects").join(format!("{stem}.end"));
    let path = work.join("objects").join(stem);
    let end: u64 = if end_path.exists() {
        checked_read(&end_path)?
    } else {
        0
    };
    let end = publication.append(path, end, bytes)?;
    publication.write(&end_path, &end)
}
fn receipt_path(work: &Path, key: &Key) -> Result<PathBuf> {
    Ok(work.join("covered").join(hash_key(key)?))
}
fn covered(work: &Path, key: &Key) -> Result<bool> {
    let path = receipt_path(work, key)?;
    if !path.exists() {
        return Ok(false);
    }
    let actual: Key = checked_read(&path)?;
    if actual != *key {
        return Err(error("original-v3 coverage receipt identity differs"));
    }
    Ok(true)
}
fn read_key_log(path: &Path, offset: u64, end: u64) -> Result<Option<(Key, u64)>> {
    if offset == end {
        return Ok(None);
    }
    let mut file = File::open(path).map_err(|e| io(path, e))?;
    file.seek(SeekFrom::Start(offset))
        .map_err(|e| io(path, e))?;
    let mut len = [0; 4];
    file.read_exact(&mut len).map_err(|e| io(path, e))?;
    let len = u32::from_le_bytes(len) as u64;
    if len > HEADER_LIMIT * 2 || offset.checked_add(4 + len).is_none_or(|n| n > end) {
        return Err(error("original-v3 key log exceeds committed cursor"));
    }
    let mut bytes = vec![0; len as usize];
    file.read_exact(&mut bytes).map_err(|e| io(path, e))?;
    let value: Checked<Key> = serde_ext::from_bytes(&bytes).map_err(error)?;
    if blake3::hash(&encode(&value.value)?).to_hex().as_str() != value.checksum {
        return Err(error("original-v3 key log checksum differs"));
    }
    Ok(Some((value.value, offset + 4 + len)))
}

impl OriginalV3Converter {
    fn save_progress(&mut self, path: &Path, progress: &Progress) -> Result<()> {
        let encoded = encode(progress)?;
        if self.checkpoint_bytes.as_ref() == Some(&encoded) {
            return Ok(());
        }
        checked_write(path, progress)?;
        self.checkpoint_bytes = Some(encoded);
        Ok(())
    }
    pub(crate) fn checkpoint_path(
        source: &Path,
        target: &Path,
        inventory_id: &str,
    ) -> Result<PathBuf> {
        let source = fs::canonicalize(source).map_err(|e| io(source, e))?;
        let target = fs::canonicalize(target).map_err(|e| io(target, e))?;
        let job = blake3::hash(&encode(&(inventory_id, &source, &target))?)
            .to_hex()
            .to_string();
        Ok(source
            .join(CONVERSION)
            .join("original-v3")
            .join(job)
            .join("cursor.sobj"))
    }
    /// Observe the existing completed original-source lineage without advancing
    /// conversion, replaying payloads, opening a writer, or granting GC/Ready.
    pub(crate) fn completed_original_observation(source: &Path, target: &Path, inventory_id: &str) -> Result<bool> {
        let source = fs::canonicalize(source).map_err(|e| io(source, e))?;
        let target = fs::canonicalize(target).map_err(|e| io(target, e))?;
        let checkpoint = Self::checkpoint_path(&source, &target, inventory_id)?;
        if !checkpoint.try_exists().map_err(|e| io(&checkpoint, e))? { return Ok(false); }
        let before = durable_file::read_object_file_bytes(&checkpoint, HEADER_LIMIT * 4).map_err(error)?;
        let progress: Progress = checked_read(&checkpoint)?;
        let (marker, bytes, catalog) = read_source(&source)?;
        let active = segment_path(&source, &catalog, catalog.segments.last().unwrap());
        let store = LocalContentStore::open_for_inspection(&target);
        if progress.inventory_id != inventory_id || progress.source_root != source || progress.target_root != target
            || progress.target_incarnation != store.store_incarnation().map_err(error)?
            || progress.marker_digest != blake3::hash(&marker).to_hex().as_str()
            || progress.catalog_digest != blake3::hash(&bytes).to_hex().as_str()
            || progress.active_len != progress.active_identity.length || !progress.active_identity.matches_checkpoint(&active, &checkpoint)? {
            return Err(error("retained original-source completion changed its exact lineage"));
        }
        let complete = progress.complete && progress.index_complete && progress.loose_bucket == 256
            && progress.segment == catalog.segments.len() && progress.active_digest.is_some()
            && progress.import_offset == progress.units_end && progress.validation_segment == catalog.segments.len();
        if durable_file::read_object_file_bytes(&checkpoint, HEADER_LIMIT * 4).map_err(error)? != before {
            return Ok(false);
        }
        Ok(complete)
    }
    pub(crate) fn step(
        &mut self,
        source: &Path,
        target: &Path,
        inventory_id: &str,
        selection: &OriginalV3Selection,
        budget: &OriginalV3Budget<'_>,
    ) -> Result<OriginalV3Step> {
        let selection_kind = match selection {
            OriginalV3Selection::All => "inventory",
            OriginalV3Selection::Object(_) => "object",
            OriginalV3Selection::Version { .. } => "version",
            OriginalV3Selection::Blob(_) => "blob",
        };
        let span = tracing::trace_span!(target: "sinomo.perf", "dfs.migration.content",
            stage = "dfs.migration.content", selection = selection_kind,
            outcome = tracing::field::Empty, work_state = tracing::field::Empty,
            processed_records = tracing::field::Empty, processed_bytes = tracing::field::Empty);
        let _entered = span.enter();
        let result = self.step_inner(source, target, inventory_id, selection, budget);
        span.record("outcome", if result.is_ok() { "ok" } else { "error" });
        if let Ok(step) = &result {
            span.record("work_state", tracing::field::debug(&step.unit_result));
            span.record("processed_records", step.processed_records as u64);
            span.record("processed_bytes", step.processed_bytes);
        }
        if result.is_err() {
            self.warm = None;
            self.blob_verifiers.clear();
            self.loose_reader = None;
            self.directories = None;
            self.checkpoint_bytes = None;
            self.writer = None;
        }
        if result.as_ref().is_ok_and(|step| step.coverage_complete) {
            self.writer = None;
            self.blob_verifiers.clear();
        }
        result
    }
    fn step_inner(
        &mut self,
        source: &Path,
        target: &Path,
        inventory_id: &str,
        selection: &OriginalV3Selection,
        budget: &OriginalV3Budget<'_>,
    ) -> Result<OriginalV3Step> {
        let mut result = OriginalV3Step {
            processed_records: 0,
            processed_bytes: 0,
            coverage_complete: false,
            unit_result: OriginalV3UnitResult::Pending,
        };
        if inventory_id.is_empty() || inventory_id.len() > 1024 || inventory_id.contains('\0') {
            return Err(error("invalid original-v3 inventory identity"));
        }
        if budget.max_records == 0 || budget.max_bytes == 0 {
            result.unit_result =
                OriginalV3UnitResult::Blocked("nonzero record and byte budgets required".into());
            return Ok(result);
        }
        if budget.cancelled.load(Ordering::Acquire) || Instant::now() >= budget.deadline {
            return Ok(result);
        }
        let source = fs::canonicalize(source).map_err(|e| io(source, e))?;
        let target = fs::canonicalize(target).map_err(|e| io(target, e))?;
        if source == target {
            return Err(error(
                "original-v3 source must be retained separately from current target",
            ));
        }
        let job = blake3::hash(&encode(&(inventory_id, &source, &target))?)
            .to_hex()
            .to_string();
        if self.job.as_ref().is_some_and(|actual| actual != &job) {
            return Err(error("original-v3 driver changed its caller-owned job"));
        }
        self.job = Some(job.clone());
        let work = source.join(CONVERSION).join("original-v3").join(&job);
        let directories = [
            &source.join(CONVERSION),
            &source.join(CONVERSION).join("original-v3"),
            &work,
            &work.join("rows"),
            &work.join("objects"),
            &work.join("covered"),
            &work.join("refs"),
        ];
        if let Some(known) = &self.directories {
            let observed = directories
                .iter()
                .map(|path| directory_identity(path))
                .collect::<Result<Vec<_>>>()?;
            if known != &observed {
                return Err(error(
                    "source3 work directories changed within the retained owner",
                ));
            }
        } else {
            for directory in &directories {
                durable_file::ensure_directory(directory).map_err(error)?;
            }
            self.directories = Some(
                directories
                    .iter()
                    .map(|path| directory_identity(path))
                    .collect::<Result<Vec<_>>>()?,
            );
        }
        let _conversion = SourceLock(lock(&work.join("writer.lock"))?);
        let owner_path = source.join("content-owner.lock");
        let owner = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&owner_path)
            .map_err(|e| io(&owner_path, e))?;
        owner.try_lock_exclusive().map_err(|e| io(&owner_path, e))?;
        let _source = SourceLock(owner);
        let marker_len = fs::metadata(source.join(MARKER))
            .map_err(|e| io(&source.join(MARKER), e))?
            .len();
        let catalog_path = source.join("inline-bindings/current.sobj");
        let catalog_len = fs::metadata(&catalog_path)
            .map_err(|e| io(&catalog_path, e))?
            .len();
        if marker_len.saturating_add(catalog_len) > budget.max_bytes {
            result.unit_result = OriginalV3UnitResult::Blocked(
                "byte budget cannot hold the bounded original catalog header".into(),
            );
            return Ok(result);
        }
        let (marker, catalog_bytes, catalog) = read_source(&source)?;
        result.processed_bytes = marker.len() as u64 + catalog_bytes.len() as u64;
        let marker_digest = blake3::hash(&marker).to_hex().to_string();
        let catalog_digest = blake3::hash(&catalog_bytes).to_hex().to_string();
        let active = segment_path(&source, &catalog, catalog.segments.last().unwrap());
        let active_len = fs::metadata(&active).map_err(|e| io(&active, e))?.len();
        let target_inspection = LocalContentStore::open_for_inspection(&target);
        let target_incarnation = target_inspection.store_incarnation().map_err(error)?;
        let checkpoint = work.join("cursor.sobj");
        let mut progress = if checkpoint.exists() {
            checked_read::<Progress>(&checkpoint)?
        } else {
            Progress {
                inventory_id: inventory_id.into(),
                source_root: source.clone(),
                target_root: target.clone(),
                target_incarnation: target_incarnation.clone(),
                marker_digest: marker_digest.clone(),
                catalog_digest: catalog_digest.clone(),
                active_len,
                active_identity: file_identity(&active)?,
                active_digest: None,
                validation_segment: 0,
                loose_bucket: 0,
                segment: 0,
                offset: 0,
                sequence: 0,
                segment_verified: false,
                index_complete: false,
                units_end: 0,
                import_offset: 0,
                complete: false,
                priority: None,
            }
        };
        if progress.inventory_id != inventory_id
            || progress.source_root != source
            || progress.target_root != target
            || progress.target_incarnation != target_incarnation
            || progress.marker_digest != marker_digest
            || progress.catalog_digest != catalog_digest
            || progress.active_len != active_len
            || !progress.active_identity.matches_checkpoint(&active, &checkpoint)?
        {
            return Err(error(
                "original-v3 immutable source identity changed; retained job cannot restart from another cut",
            ));
        }
        if progress.loose_bucket > 256
            || progress.segment > catalog.segments.len()
            || progress.validation_segment > catalog.segments.len()
            || progress.import_offset > progress.units_end
            || (progress.index_complete
                && (progress.loose_bucket != 256
                    || progress.segment != catalog.segments.len()
                    || progress.active_digest.is_none()))
            || (progress.complete
                && (!progress.index_complete
                    || progress.import_offset != progress.units_end
                    || progress.validation_segment != catalog.segments.len()))
        {
            return Err(error("original-v3 cursor violates its original job bounds"));
        }
        let pending = target_inspection
            .inventory_import_pending()
            .map_err(error)?;
        if pending.as_deref() != Some(inventory_id)
            && !(progress.complete
                && target_inspection
                    .inventory_import_completed(inventory_id)
                    .map_err(error)?)
        {
            return Err(error("source3 target lost its exact inventory lineage"));
        }
        if progress.complete {
            result.coverage_complete = true;
            result.unit_result = OriginalV3UnitResult::Covered;
            return Ok(result);
        }
        self.save_progress(&checkpoint, &progress)?;
        // Format3 still stores large immutable blobs as loose content-addressed
        // files. Discover those once, before inline control records shadow them.
        // A restart resumes its current bounded bucket; ordinary demands retain
        // this same iterator/cursor rather than rescanning the source tree.
        while progress.loose_bucket < 256
            && result.processed_records < budget.max_records
            && !budget.cancelled.load(Ordering::Acquire)
            && Instant::now() < budget.deadline
        {
            result.unit_result = OriginalV3UnitResult::PendingIndex;
            let bucket = progress.loose_bucket;
            let directory = source.join("blobs").join(format!("{bucket:02x}"));
            if self
                .loose_reader
                .as_ref()
                .is_none_or(|(current, _)| *current != bucket)
            {
                match fs::read_dir(&directory) {
                    Ok(reader) => self.loose_reader = Some((bucket, reader)),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        progress.loose_bucket += 1;
                        result.processed_records += 1;
                        self.save_progress(&checkpoint, &progress)?;
                        continue;
                    }
                    Err(error) => return Err(io(&directory, error)),
                }
            }
            let entry = self.loose_reader.as_mut().unwrap().1.next();
            result.processed_records += 1;
            match entry {
                None => {
                    progress.loose_bucket += 1;
                    self.loose_reader = None;
                }
                Some(entry) => {
                    let entry = entry.map_err(|e| io(&directory, e))?;
                    let name = entry
                        .file_name()
                        .into_string()
                        .map_err(|_| error("original-v3 blob filename is not canonical"))?;
                    let hash = format!("blake3:{name}");
                    if !hash_valid(&hash)
                        || !name.starts_with(&format!("{bucket:02x}"))
                        || !entry
                            .file_type()
                            .map_err(|e| io(&entry.path(), e))?
                            .is_file()
                    {
                        return Err(error("original-v3 loose blob path is not canonical"));
                    }
                    let key = Key::Blob(hash.clone());
                    let row = row_path(&work, &key)?;
                    let indexed = if row.exists() {
                        Some(checked_read::<Indexed>(&row)?)
                    } else {
                        None
                    };
                    if indexed
                        .as_ref()
                        .is_some_and(|row| row.key != key || row.sequence != 0)
                    {
                        return Err(error(
                            "original-v3 loose inventory has an invalid indexed predecessor",
                        ));
                    }
                    if indexed
                        .as_ref()
                        .is_none_or(|row| row.unit_log_end > progress.units_end)
                    {
                        let size = entry.metadata().map_err(|e| io(&entry.path(), e))?.len();
                        let blob = BlobSource {
                            hash,
                            path: entry
                                .path()
                                .strip_prefix(&source)
                                .map_err(error)?
                                .to_owned(),
                            offset: 0,
                            size,
                        };
                        index_row(
                            &work,
                            &mut progress,
                            key,
                            Value::Blob {
                                source: blob,
                                published: 0,
                            },
                            0,
                        )?;
                    }
                }
            }
            self.save_progress(&checkpoint, &progress)?;
        }
        if progress.loose_bucket < 256 {
            return Ok(result);
        }
        while !progress.index_complete
            && result.processed_records < budget.max_records
            && !budget.cancelled.load(Ordering::Acquire)
            && Instant::now() < budget.deadline
        {
            result.unit_result = OriginalV3UnitResult::PendingIndex;
            if progress.segment == catalog.segments.len() {
                progress.index_complete = true;
                self.save_progress(&checkpoint, &progress)?;
                break;
            }
            let segment = &catalog.segments[progress.segment];
            let path = segment_path(&source, &catalog, segment);
            let length = fs::metadata(&path).map_err(|e| io(&path, e))?.len();
            if let Some(end) = segment.sealed_end {
                if length != end {
                    return Err(error("original-v3 sealed segment length differs"));
                }
                if !progress.segment_verified {
                    if !matches!(&self.warm,Some(Warm::Segment{index,..}) if *index==progress.segment)
                    {
                        self.warm = Some(Warm::Segment {
                            index: progress.segment,
                            verifier: sinomo_dfs::ContentImportBlobVerifier::existing(
                                &path,
                                format!("blake3:{}", segment.sealed_digest.as_ref().unwrap()),
                                end,
                            )
                            .map_err(error)?,
                        });
                    }
                    let remaining = budget.max_bytes.saturating_sub(result.processed_bytes);
                    if remaining == 0 {
                        break;
                    }
                    let Some(Warm::Segment { verifier, .. }) = &mut self.warm else {
                        unreachable!()
                    };
                    let before = verifier.verified_bytes();
                    let done = verifier
                        .step(remaining, budget.deadline, budget.cancelled)
                        .map_err(error)?;
                    result.processed_bytes += verifier.verified_bytes() - before;
                    if !done {
                        break;
                    }
                    self.warm = None;
                    progress.segment_verified = true;
                    self.save_progress(&checkpoint, &progress)?;
                }
            }
            if segment.sealed_end.is_none() && progress.active_digest.is_none() {
                if !matches!(self.warm, Some(Warm::Active { .. })) {
                    self.warm = Some(Warm::Active {
                        offset: 0,
                        hasher: blake3::Hasher::new(),
                    });
                }
                let Some(Warm::Active { offset, hasher }) = &mut self.warm else {
                    unreachable!()
                };
                let remaining = budget.max_bytes.saturating_sub(result.processed_bytes);
                if remaining == 0 {
                    break;
                }
                let count = (length - *offset).min(remaining).min(64 * 1024) as usize;
                let mut file = File::open(&path).map_err(|e| io(&path, e))?;
                file.seek(SeekFrom::Start(*offset))
                    .map_err(|e| io(&path, e))?;
                let mut bytes = vec![0; count];
                file.read_exact(&mut bytes).map_err(|e| io(&path, e))?;
                hasher.update(&bytes);
                *offset += count as u64;
                result.processed_bytes += count as u64;
                if *offset != length {
                    break;
                }
                progress.active_digest = Some(hasher.finalize().to_hex().to_string());
                self.warm = None;
                self.save_progress(&checkpoint, &progress)?;
            }
            if budget.max_bytes.saturating_sub(result.processed_bytes) < FRAME_WORK {
                if result.processed_records == 0
                    && result.processed_bytes == marker_len + catalog_len
                {
                    result.unit_result = OriginalV3UnitResult::Blocked(format!(
                        "source frame needs at least {FRAME_WORK} bytes of remaining budget"
                    ));
                }
                break;
            }
            let mut file = File::open(&path).map_err(|e| io(&path, e))?;
            file.seek(SeekFrom::Start(progress.offset))
                .map_err(|e| io(&path, e))?;
            match read_frame(
                &mut file,
                &path,
                FrameLimits::new(HEADER_LIMIT as u32, PAYLOAD_LIMIT).map_err(error)?,
            )
            .map_err(error)?
            {
                FrameRead::Complete(frame) => {
                    let record: Record = serde_ext::from_bytes(frame.header()).map_err(error)?;
                    if record.format_version != 2
                        || record.store_incarnation != catalog.store_incarnation
                        || record.generation != catalog.generation
                        || record.sequence
                            != progress
                                .sequence
                                .checked_add(1)
                                .ok_or_else(|| error("source sequence exhausted"))?
                    {
                        return Err(error("original-v3 source generation/sequence differs"));
                    }
                    let hash = format!(
                        "blake3:{}",
                        blake3::Hash::from_bytes(*frame.payload_checksum()).to_hex()
                    );
                    let locator = BlobSource {
                        hash: hash.clone(),
                        path: path.strip_prefix(&source).map_err(error)?.to_owned(),
                        offset: frame.payload_offset(),
                        size: frame.payload_bytes(),
                    };
                    project_record(
                        &work,
                        &mut progress,
                        &catalog,
                        record,
                        hash,
                        frame.payload_bytes(),
                        locator,
                    )?;
                    result.processed_bytes += frame.frame_end() - frame.frame_start();
                    result.processed_records += 1;
                    progress.offset = frame.frame_end();
                    progress.sequence += 1;
                }
                FrameRead::CleanEof { offset } => {
                    if offset != length {
                        return Err(error("source EOF differs"));
                    }
                    result.processed_records += 1;
                    progress.segment += 1;
                    progress.offset = 0;
                    progress.segment_verified = false;
                }
                FrameRead::Incomplete(_) if segment.sealed_end.is_none() => {
                    result.processed_records += 1;
                    progress.segment += 1;
                    progress.offset = 0;
                    progress.segment_verified = false;
                }
                FrameRead::Incomplete(_) => {
                    return Err(error("sealed source segment has incomplete tail"));
                }
                FrameRead::Corrupt(frame) => {
                    return Err(error(format!(
                        "corrupt original-v3 frame: {}",
                        frame.reason
                    )));
                }
            }
            self.save_progress(&checkpoint, &progress)?;
        }
        if !progress.index_complete {
            return Ok(result);
        }
        result.unit_result = OriginalV3UnitResult::Pending;
        if self.writer.is_none() {
            self.writer = Some(std::sync::Arc::new(
                ContentStoreStagingWriter::open_import_target(&target, inventory_id)
                    .map_err(error)?,
            ));
        }
        let writer = std::sync::Arc::clone(self.writer.as_ref().unwrap());
        while result.processed_records < budget.max_records
            && !budget.cancelled.load(Ordering::Acquire)
            && Instant::now() < budget.deadline
        {
            let next = match selection {
                OriginalV3Selection::All => {
                    key_at(&work, progress.import_offset, progress.units_end)?
                }
                OriginalV3Selection::Version { object_id, version } => {
                    let key = Key::Version(object_id.clone(), version.clone());
                    if !row_path(&work, &key)?.exists() {
                        result.unit_result = OriginalV3UnitResult::Absent;
                        break;
                    }
                    Some((key, 0))
                }
                OriginalV3Selection::Blob(hash) => {
                    let key = Key::Blob(hash.clone());
                    if !row_path(&work, &key)?.exists() {
                        result.unit_result = OriginalV3UnitResult::Absent;
                        break;
                    }
                    Some((key, 0))
                }
                OriginalV3Selection::Object(object) => {
                    let name = blake3::hash(object.as_bytes()).to_hex().to_string();
                    let end_path = work.join("objects").join(format!("{name}.end"));
                    if !end_path.exists() {
                        result.unit_result = OriginalV3UnitResult::Absent;
                        break;
                    }
                    let end: u64 = checked_read(&end_path)?;
                    if progress
                        .priority
                        .as_ref()
                        .is_none_or(|p| p.selection != name)
                    {
                        progress.priority = Some(Priority {
                            selection: name.clone(),
                            offset: 0,
                            seen: false,
                        });
                    }
                    read_key_log(
                        &work.join("objects").join(name),
                        progress.priority.as_ref().unwrap().offset,
                        end,
                    )?
                }
            };
            let Some((key, next)) = next else {
                if matches!(selection, OriginalV3Selection::All) {
                    // Source retirement also needs the final bounded original
                    // segment verification below; imported units alone are not coverage.
                }
                result.unit_result = OriginalV3UnitResult::Covered;
                break;
            };
            if !selected(selection, &key) {
                return Err(error("source unit priority index crossed identity"));
            }
            if !covered(&work, &key)?
                && !self.import_unit(
                    &source,
                    &target,
                    &work,
                    &writer,
                    &key,
                    &mut progress,
                    &mut result,
                    budget,
                )?
            {
                break;
            }
            result.processed_records += 1;
            match selection {
                OriginalV3Selection::All => progress.import_offset = next,
                OriginalV3Selection::Object(_) => {
                    let p = progress.priority.as_mut().unwrap();
                    p.offset = next;
                    p.seen = true;
                }
                _ => {
                    result.unit_result = OriginalV3UnitResult::Covered;
                    break;
                }
            }
            self.save_progress(&checkpoint, &progress)?;
        }
        if matches!(selection, OriginalV3Selection::All)
            && progress.import_offset == progress.units_end
        {
            while progress.validation_segment < catalog.segments.len()
                && result.processed_records < budget.max_records
                && !budget.cancelled.load(Ordering::Acquire)
                && Instant::now() < budget.deadline
            {
                let index = progress.validation_segment;
                let segment = &catalog.segments[index];
                let path = segment_path(&source, &catalog, segment);
                let expected = segment
                    .sealed_digest
                    .as_ref()
                    .or(progress.active_digest.as_ref())
                    .ok_or_else(|| error("source active digest is absent"))?;
                let length = segment.sealed_end.unwrap_or(progress.active_len);
                if !matches!(&self.warm,Some(Warm::Segment{index:actual,..}) if *actual==index) {
                    self.warm = Some(Warm::Segment {
                        index,
                        verifier: sinomo_dfs::ContentImportBlobVerifier::existing(
                            &path,
                            format!("blake3:{expected}"),
                            length,
                        )
                        .map_err(error)?,
                    });
                }
                let remaining = budget.max_bytes.saturating_sub(result.processed_bytes);
                if remaining == 0 {
                    break;
                }
                let Some(Warm::Segment { verifier, .. }) = &mut self.warm else {
                    unreachable!()
                };
                let before = verifier.verified_bytes();
                let done = verifier
                    .step(remaining, budget.deadline, budget.cancelled)
                    .map_err(error)?;
                result.processed_bytes += verifier.verified_bytes() - before;
                if !done {
                    break;
                }
                self.warm = None;
                progress.validation_segment += 1;
                result.processed_records += 1;
                self.save_progress(&checkpoint, &progress)?;
            }
            progress.complete = progress.validation_segment == catalog.segments.len();
            result.unit_result = if progress.complete {
                OriginalV3UnitResult::Covered
            } else {
                OriginalV3UnitResult::Pending
            };
        }
        self.save_progress(&checkpoint, &progress)?;
        result.coverage_complete = progress.complete;
        Ok(result)
    }
}

impl OriginalV3Converter {
    fn import_unit(
        &mut self,
        source: &Path,
        target: &Path,
        work: &Path,
        writer: &ContentStoreStagingWriter,
        key: &Key,
        progress: &mut Progress,
        result: &mut OriginalV3Step,
        budget: &OriginalV3Budget<'_>,
    ) -> Result<bool> {
        let indexed: Indexed = checked_read(&row_path(work, key)?)?;
        if indexed.key != *key
            || indexed.sequence > progress.sequence
            || indexed.unit_log_end > progress.units_end
        {
            return Err(error(
                "original-v3 unit row escaped its completed source cut",
            ));
        }
        let row = match indexed.value {
            Value::Blob {
                source: blob,
                published: _,
            } => {
                if !self.import_blob(
                    source, target, work, writer, &blob, progress, result, budget,
                )? {
                    return Ok(false);
                }
                checked_write(&receipt_path(work, key)?, key)?;
                return Ok(true);
            }
            Value::DeletedBlob(content_hash) => {
                ContentStoreStagingRow::BlobTombstone { content_hash }
            }
            Value::DeletedVersion {
                object_id,
                version,
                hash,
            } => ContentStoreStagingRow::VersionTombstone {
                object_id,
                version,
                content_hash: hash,
            },
            Value::Reservation {
                identity,
                not_before,
            } => ContentStoreStagingRow::RetentionRoot {
                identity,
                not_before,
            },
            Value::Released(identity) => ContentStoreStagingRow::RetentionRelease(identity),
            Value::Version {
                manifest,
                published,
            } => {
                // Existing current retirement wins in the atomic target API.
                // This observation only avoids copying unnecessary source bytes.
                let retired = writer
                    .object_version_is_deleted(&manifest.object_id, &manifest.version)
                    .map_err(error)?;
                if retired {
                    // A terminal current deletion ends this unit's need for a
                    // warm hash pass. Its durable source/copy DATA remains.
                    self.blob_verifiers.remove(&manifest.content_hash);
                    for extent in &manifest.extents { self.blob_verifiers.remove(&extent.content_hash); }
                } else {
                    let hashes = if manifest.extents.is_empty() {
                        if manifest.size_bytes == 0 {
                            Vec::new()
                        } else {
                            vec![(manifest.content_hash.clone(), manifest.size_bytes)]
                        }
                    } else {
                        manifest
                            .extents
                            .iter()
                            .map(|extent| {
                                extent
                                    .blob_offset
                                    .checked_add(extent.size_bytes)
                                    .map(|end| (extent.content_hash.clone(), end))
                                    .ok_or_else(|| error("original-v3 extent overflows"))
                            })
                            .collect::<Result<Vec<_>>>()?
                    };
                    for (hash, minimum) in hashes {
                        let blob_key = Key::Blob(hash.clone());
                        if covered(work, &blob_key)? {
                            continue;
                        }
                        let path = row_path(work, &blob_key)?;
                        let blob = if path.exists() {
                            let row: Indexed = checked_read(&path)?;
                            match row.value {
                                Value::Blob { source, .. } => source,
                                Value::DeletedBlob(_) => {
                                    return Err(error(
                                        "original-v3 live version references a retired blob",
                                    ));
                                }
                                _ => return Err(error("original-v3 blob index type differs")),
                            }
                        } else {
                            if !hash_valid(&hash) {
                                return Err(error("original-v3 external blob hash is invalid"));
                            }
                            let hex = &hash[7..];
                            let relative = PathBuf::from("blobs").join(&hex[..2]).join(hex);
                            let path = source.join(&relative);
                            let size = fs::metadata(&path).map_err(|e| io(&path, e))?.len();
                            BlobSource {
                                hash: hash.clone(),
                                path: relative,
                                offset: 0,
                                size,
                            }
                        };
                        if blob.size < minimum {
                            return Err(error(
                                "original-v3 source blob does not cover its manifest",
                            ));
                        }
                        if !self.import_blob(
                            source, target, work, writer, &blob, progress, result, budget,
                        )? {
                            return Ok(false);
                        }
                        checked_write(&receipt_path(work, &blob_key)?, &blob_key)?;
                    }
                }
                ContentStoreStagingRow::Manifest {
                    manifest,
                    published_at_unix_nanos: published,
                }
            }
        };
        if budget.cancelled.load(Ordering::Acquire) || Instant::now() >= budget.deadline {
            return Ok(false);
        }
        writer.apply_storage_staging_row(row).map_err(error)?;
        checked_write(&receipt_path(work, key)?, key)?;
        Ok(true)
    }

    fn import_blob(
        &mut self,
        source: &Path,
        target: &Path,
        work: &Path,
        writer: &ContentStoreStagingWriter,
        blob: &BlobSource,
        progress: &mut Progress,
        result: &mut OriginalV3Step,
        budget: &OriginalV3Budget<'_>,
    ) -> Result<bool> {
        let key = Key::Blob(blob.hash.clone());
        let path = row_path(work, &key)?;
        let mut indexed: Indexed = checked_read(&path)?;
        if indexed.key != key
            || indexed.sequence > progress.sequence
            || indexed.unit_log_end > progress.units_end
        {
            return Err(error(
                "original-v3 copy unit escaped its completed source index",
            ));
        }
        let Value::Blob {
            source: original, ..
        } = &indexed.value
        else {
            return Err(error(
                "original-v3 copy cursor is not attached to a live blob unit",
            ));
        };
        if original.hash != blob.hash
            || original.path != blob.path
            || original.offset != blob.offset
            || original.size != blob.size
            || indexed
                .copy
                .as_ref()
                .is_some_and(|copy| copy.copied > original.size)
        {
            return Err(error(
                "original-v3 copy unit changed its immutable source identity",
            ));
        }
        // Exactly one copy cursor lives in this indexed unit. The admitted
        // verifier also retains its progress until its owning unit completes.
        let done = self.advance_blob(
            source,
            target,
            work,
            writer,
            blob,
            &mut indexed.copy,
            result,
            budget,
        )?;
        checked_write(&path, &indexed)?;
        Ok(done)
    }

    fn advance_blob(
        &mut self,
        source: &Path,
        target: &Path,
        _work: &Path,
        writer: &ContentStoreStagingWriter,
        blob: &BlobSource,
        copy_slot: &mut Option<CopyWork>,
        result: &mut OriginalV3Step,
        budget: &OriginalV3Budget<'_>,
    ) -> Result<bool> {
        if blob.size == 0 {
            if blob.hash != format!("blake3:{}", blake3::hash(&[]).to_hex()) {
                return Err(error("empty source blob has another hash"));
            }
            return Ok(true); // intrinsic empty content needs no physical publication
        }
        if !hash_valid(&blob.hash)
            || blob.path.is_absolute()
            || blob
                .path
                .components()
                .any(|c| matches!(c, std::path::Component::ParentDir))
        {
            return Err(error("original-v3 blob locator escapes owned source"));
        }
        let path = source.join(&blob.path);
        let actual = fs::canonicalize(&path).map_err(|e| io(&path, e))?;
        if !actual.starts_with(source) {
            return Err(error("original-v3 blob resolves outside retained source"));
        }
        if !self.blob_verifiers.contains_key(&blob.hash) && self.blob_verifiers.len() >= 8 {
            // Admitted units remain runnable; their completion wakes this
            // selection. Capacity pressure must not evict unfinished work.
            return Ok(false);
        }
        if copy_slot
            .as_ref()
            .is_some_and(|copy| copy.hash != blob.hash)
        {
            return Err(error(
                "original-v3 caller changed an unfinished immutable unit dependency",
            ));
        }
        let stage_root = target
            .join(CONVERSION)
            .join("original-v3-staged")
            .join(self.job.as_ref().unwrap());
        durable_file::ensure_directory(&target.join(CONVERSION)).map_err(error)?;
        durable_file::ensure_directory(&target.join(CONVERSION).join("original-v3-staged"))
            .map_err(error)?;
        durable_file::ensure_directory(&stage_root).map_err(error)?;
        if copy_slot.is_none() {
            *copy_slot = Some(CopyWork {
                hash: blob.hash.clone(),
                source: blob.clone(),
                copied: 0,
                staged: stage_root.join(&blob.hash[7..]),
                existing: None,
            });
        }
        let copy = copy_slot.as_mut().unwrap();
        let expected_stage = stage_root.join(&blob.hash[7..]);
        let expected_current = target
            .join("blobs")
            .join(&blob.hash[7..9])
            .join(&blob.hash[7..]);
        if copy.staged != expected_stage
            || copy
                .existing
                .as_ref()
                .is_some_and(|path| *path != expected_current)
        {
            return Err(error(
                "original-v3 copy checkpoint escaped the owned target",
            ));
        }
        if copy.copied > 0 && copy.existing.is_none() && !copy.staged.exists() {
            // Publication may have committed before its coverage checkpoint.
            // Reobserve and verify the current inode; never assume absence or
            // a missing private pathname proves the effect did not happen.
            if expected_current.is_file() {
                copy.existing = Some(expected_current);
            } else {
                copy.copied = 0;
                self.blob_verifiers.remove(&blob.hash);
            }
        }

        if copy.source.path != blob.path
            || copy.source.offset != blob.offset
            || copy.source.size != blob.size
        {
            return Err(error(
                "original-v3 blob dependency changed its captured locator",
            ));
        }
        if copy.copied < blob.size && copy.existing.is_none() {
            let remaining = budget.max_bytes.saturating_sub(result.processed_bytes);
            if remaining == 0
                || budget.cancelled.load(Ordering::Acquire)
                || Instant::now() >= budget.deadline
            {
                return Ok(false);
            }
            let mut input = File::open(&actual).map_err(|e| io(&actual, e))?;
            let length = input.metadata().map_err(|e| io(&actual, e))?.len();
            if blob
                .offset
                .checked_add(blob.size)
                .is_none_or(|end| end > length)
            {
                return Err(error("original-v3 blob span exceeds retained file"));
            }
            input
                .seek(SeekFrom::Start(blob.offset + copy.copied))
                .map_err(|e| io(&actual, e))?;
            let mut output = if copy.staged.exists() {
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&copy.staged)
                    .map_err(|e| io(&copy.staged, e))?
            } else {
                if copy.copied != 0 {
                    return Err(error("original-v3 private copy disappeared"));
                }
                durable_file::create_private_staged_file(&copy.staged).map_err(error)?
            };
            output
                .set_len(copy.copied)
                .map_err(|e| io(&copy.staged, e))?;
            output
                .seek(SeekFrom::Start(copy.copied))
                .map_err(|e| io(&copy.staged, e))?;
            let count = (blob.size - copy.copied).min(remaining).min(64 * 1024) as usize;
            let mut bytes = vec![0; count];
            input.read_exact(&mut bytes).map_err(|e| io(&actual, e))?;
            output
                .write_all(&bytes)
                .and_then(|_| output.sync_all())
                .map_err(|e| io(&copy.staged, e))?;
            copy.copied += count as u64;
            result.processed_bytes += count as u64;
            if copy.copied < blob.size {
                return Ok(false);
            }
        }
        if !self.blob_verifiers.contains_key(&blob.hash) {
            let verifier = if let Some(existing) = &copy.existing {
                sinomo_dfs::ContentImportBlobVerifier::existing(
                    existing,
                    blob.hash.clone(),
                    blob.size,
                )
                .map_err(error)?
            } else {
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&copy.staged)
                    .map_err(|e| io(&copy.staged, e))?;
                let synced =
                    durable_file::sync_private_staged_file(&copy.staged, file).map_err(error)?;
                sinomo_dfs::ContentImportBlobVerifier::new(synced, blob.hash.clone(), blob.size)
                    .map_err(error)?
            };
            self.blob_verifiers.insert(blob.hash.clone(), verifier);
        }
        let remaining = budget.max_bytes.saturating_sub(result.processed_bytes);
        if remaining == 0 {
            return Ok(false);
        }
        let verifier = self.blob_verifiers.get_mut(&blob.hash).expect("admitted blob verifier");
        let before = verifier.verified_bytes();
        let done = verifier
            .step(remaining, budget.deadline, budget.cancelled)
            .map_err(error)?;
        result.processed_bytes += verifier.verified_bytes() - before;
        if !done {
            return Ok(false);
        }
        if budget.cancelled.load(Ordering::Acquire) || Instant::now() >= budget.deadline {
            return Ok(false);
        }
        let verifier = self.blob_verifiers.remove(&blob.hash).expect("completed blob verifier");
        match writer
            .import_verified_blob(verifier.finish().map_err(error)?)
            .map_err(error)?
        {
            sinomo_dfs::ContentImportBlobPublication::Published
            | sinomo_dfs::ContentImportBlobPublication::CurrentDeleted => {
                if copy.staged.exists() {
                    fs::remove_file(&copy.staged).map_err(|e| io(&copy.staged, e))?;
                    durable_file::sync_directory(&stage_root).map_err(error)?;
                }
                *copy_slot = None;
                Ok(true)
            }
            sinomo_dfs::ContentImportBlobPublication::VerifyCurrent { path } => {
                copy.existing = Some(path);
                Ok(false)
            }
        }
    }
}

/// Exact retired codec DATA for owner lifecycle tests; no native authority or
/// current storage activation is manufactured by this source writer.
#[cfg(test)]
pub(crate) fn write_original_v3_fixture_data(
    root: &Path,
    bytes: &[u8],
    deleted: bool,
) -> ContentManifest {
    tests::original(root, bytes, deleted)
}

#[cfg(test)]
pub(crate) fn write_original_v3_named_fixture_data(
    root: &Path,
    bytes: &[u8],
    object_id: &str,
    version: &str,
) -> ContentManifest {
    tests::original_named(root, bytes, false, object_id, version)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::storage::checksummed_frame::FrameAppender;
    use std::time::Duration;

    pub(super) fn original(root: &Path, bytes: &[u8], deleted: bool) -> ContentManifest {
        original_named(root, bytes, deleted, "opaque-object", "opaque-version")
    }
    pub(super) fn original_named(
        root: &Path,
        bytes: &[u8],
        deleted: bool,
        object_id: &str,
        version: &str,
    ) -> ContentManifest {
        #[derive(Serialize)]
        struct Marker<'a> {
            format_version: u32,
            marker: &'a str,
            store_incarnation: &'a str,
            catalog_scan_epoch: u64,
            content_binding_layout: &'a str,
        }
        fs::create_dir_all(root).unwrap();
        File::create(root.join("content-owner.lock"))
            .unwrap()
            .sync_all()
            .unwrap();
        let incarnation = sinomo_core::Id::generate().to_string();
        write(
            &root.join(MARKER),
            &Marker {
                format_version: 3,
                marker: "sinomo-dfs-content-reservation-batches-v1",
                store_incarnation: &incarnation,
                catalog_scan_epoch: 0,
                content_binding_layout: "inline-bindings-v2",
            },
        )
        .unwrap();
        let catalog = Catalog {
            format_version: 2,
            store_incarnation: incarnation.clone(),
            generation: 1,
            segments: vec![Segment {
                id: 1,
                sealed_end: None,
                sealed_digest: None,
            }],
        };
        let path = segment_path(root, &catalog, &catalog.segments[0]);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let manifest = ContentManifest {
            object_id: object_id.into(),
            version: version.into(),
            content_hash: format!("blake3:{}", blake3::hash(bytes).to_hex()),
            size_bytes: bytes.len() as u64,
            extents: Vec::new(),
        };
        let reservation = Reservation {
            identity: ContentStoreReservationIdentity {
                object_id: manifest.object_id.clone(),
                version: manifest.version.clone(),
                content_hash: manifest.content_hash.clone(),
            },
            reserved_at_unix_nanos: 123,
            causal_stamp: Stamp {
                store_incarnation: incarnation.clone(),
                catalog_scan_epoch: 0,
            },
        };
        let operation = if bytes.len() <= PAYLOAD_LIMIT as usize {
            Operation::Bind {
                manifest: manifest.clone(),
                published_at_unix_nanos: 123,
                reservation: Some(reservation),
            }
        } else {
            let hex = &manifest.content_hash[7..];
            let blob = root.join("blobs").join(&hex[..2]).join(hex);
            fs::create_dir_all(blob.parent().unwrap()).unwrap();
            fs::write(blob, bytes).unwrap();
            Operation::ExternalBinding {
                manifest: manifest.clone(),
                published_at_unix_nanos: 123,
                reservation: Some(reservation),
            }
        };
        let mut appender = FrameAppender::open(
            &path,
            FrameLimits::new(HEADER_LIMIT as u32, PAYLOAD_LIMIT).unwrap(),
        )
        .unwrap();
        let record = Record {
            format_version: 2,
            store_incarnation: incarnation.clone(),
            generation: 1,
            sequence: 1,
            operation,
        };
        appender
            .append_bytes(
                &encode(&record).unwrap(),
                if bytes.len() <= PAYLOAD_LIMIT as usize {
                    bytes
                } else {
                    &[]
                },
            )
            .unwrap();
        if deleted {
            let record = Record {
                format_version: 2,
                store_incarnation: incarnation,
                generation: 1,
                sequence: 2,
                operation: Operation::DeleteVersion {
                    object_id: manifest.object_id.clone(),
                    version: manifest.version.clone(),
                    content_hash: manifest.content_hash.clone(),
                },
            };
            appender
                .append_bytes(&encode(&record).unwrap(), &[])
                .unwrap();
        }
        drop(appender);
        let checksum = blake3::hash(&encode(&catalog).unwrap())
            .to_hex()
            .to_string();
        durable_file::write_file_atomic(
            &root.join("inline-bindings/current.sobj"),
            &encode(&Envelope { catalog, checksum }).unwrap(),
            durable_file::DurableAtomicFileOptions::new("original", "tmp").sync_all(),
        )
        .unwrap();
        manifest
    }
    fn budget(cancel: &AtomicBool) -> OriginalV3Budget<'_> {
        OriginalV3Budget {
            max_records: 32,
            max_bytes: 96 * 1024,
            deadline: Instant::now() + Duration::from_secs(5),
            cancelled: cancel,
        }
    }
    fn target(root: &Path) -> LocalContentStore {
        let target = LocalContentStore::new(root).unwrap();
        target.begin_inventory_import("inventory").unwrap();
        target
    }
    #[test]
    fn alternating_large_units_preserve_unfinished_verification() {
        let dir = sinomo_testdir::TestDir::new("original-v3-interleaved-verifiers");
        let source = dir.join("source");
        let target_path = dir.join("target");
        let first_bytes = vec![0x31; 2 * 1024 * 1024];
        let second_bytes = vec![0x72; 2 * 1024 * 1024];
        let first = original(&source, &first_bytes, false);
        let (_, _, catalog) = read_source(&source).unwrap();
        let second = ContentManifest { object_id: "second-object".into(), version: "second-version".into(),
            content_hash: format!("blake3:{}", blake3::hash(&second_bytes).to_hex()),
            size_bytes: second_bytes.len() as u64, extents: Vec::new() };
        let hex = &second.content_hash[7..];
        let path = source.join("blobs").join(&hex[..2]).join(hex);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, &second_bytes).unwrap();
        let segment = segment_path(&source, &catalog, &catalog.segments[0]);
        let mut writer = FrameAppender::open(&segment, FrameLimits::new(HEADER_LIMIT as u32, PAYLOAD_LIMIT).unwrap()).unwrap();
        writer.append_bytes(&encode(&Record {
            format_version: 2, store_incarnation: catalog.store_incarnation, generation: 1, sequence: 2,
            operation: Operation::ExternalBinding { manifest: second.clone(), published_at_unix_nanos: 124, reservation: None },
        }).unwrap(), &[]).unwrap();
        drop(writer);
        let current = target(&target_path);
        let cancel = AtomicBool::new(false);
        let mut converter = OriginalV3Converter::default();
        let mut complete = [false; 2];
        for _ in 0..200 {
            for (index, manifest) in [&first, &second].into_iter().enumerate() {
                if complete[index] { continue; }
                let limits = budget(&cancel);
                let result = converter.step(&source, &target_path, "inventory",
                    &OriginalV3Selection::Version { object_id: manifest.object_id.clone(), version: manifest.version.clone() },
                    &limits).unwrap();
                assert!(result.processed_bytes <= limits.max_bytes);
                complete[index] = result.unit_result == OriginalV3UnitResult::Covered;
            }
            if complete == [true, true] { break; }
        }
        assert_eq!(complete, [true, true], "interleaving must not restart each verifier indefinitely");
        for (manifest, expected) in [(&first, &first_bytes), (&second, &second_bytes)] {
            assert_eq!(current.read_object_version(&manifest.object_id, &manifest.version).unwrap().1, *expected);
        }
    }
    #[test]
    fn original_v3_large_priority_import_restarts_and_completes_without_rewriting_source() {
        let dir = sinomo_testdir::TestDir::new("original-v3-large");
        let source = dir.path().join("source");
        let target_path = dir.path().join("target");
        let bytes = vec![0x37; 256 * 1024];
        let manifest = original(&source, &bytes, false);
        let current = target(&target_path);
        let before = fs::read(source.join("inline-bindings/current.sobj")).unwrap();
        let cancel = AtomicBool::new(false);
        let selection = OriginalV3Selection::Version {
            object_id: manifest.object_id.clone(),
            version: manifest.version.clone(),
        };
        let mut converter = OriginalV3Converter::default();
        let mut restarted = false;
        for attempt in 0..200 {
            let budget = budget(&cancel);
            let result = converter
                .step(&source, &target_path, "inventory", &selection, &budget)
                .unwrap();
            assert!(result.processed_records <= budget.max_records);
            assert!(result.processed_bytes <= budget.max_bytes);
            if attempt == 10 {
                drop(converter);
                converter = OriginalV3Converter::default();
                restarted = true;
            }
            if result.unit_result == OriginalV3UnitResult::Covered {
                break;
            }
            assert!(
                attempt < 199,
                "bounded priority failed to finish: {:?}",
                result.unit_result
            );
        }
        assert!(restarted);
        assert_eq!(
            current
                .read_object_version(&manifest.object_id, &manifest.version)
                .unwrap()
                .1,
            bytes
        );
        for attempt in 0..200 {
            let result = converter
                .step(
                    &source,
                    &target_path,
                    "inventory",
                    &OriginalV3Selection::All,
                    &budget(&cancel),
                )
                .unwrap();
            if result.coverage_complete {
                break;
            }
            assert!(attempt < 199);
        }
        current.complete_inventory_import("inventory").unwrap();
        assert!(
            converter
                .step(
                    &source,
                    &target_path,
                    "inventory",
                    &OriginalV3Selection::All,
                    &budget(&cancel)
                )
                .unwrap()
                .coverage_complete
        );
        assert_eq!(
            fs::read(source.join("inline-bindings/current.sobj")).unwrap(),
            before
        );
    }
    #[test]
    fn original_v3_final_delete_precedes_priority_and_unknown_requires_complete_index() {
        let dir = sinomo_testdir::TestDir::new("original-v3-delete");
        let source = dir.path().join("source");
        let target_path = dir.path().join("target");
        let manifest = original(&source, b"old bytes", true);
        let current = target(&target_path);
        let cancel = AtomicBool::new(false);
        let mut converter = OriginalV3Converter::default();
        let selection = OriginalV3Selection::Object(manifest.object_id.clone());
        let mut complete = false;
        for _ in 0..100 {
            let result = converter
                .step(
                    &source,
                    &target_path,
                    "inventory",
                    &selection,
                    &budget(&cancel),
                )
                .unwrap();
            if result.unit_result == OriginalV3UnitResult::Covered {
                complete = true;
                break;
            }
        }
        assert!(complete);
        assert!(
            current
                .object_version_is_deleted(&manifest.object_id, &manifest.version)
                .unwrap()
        );
        assert_eq!(
            converter
                .step(
                    &source,
                    &target_path,
                    "inventory",
                    &OriginalV3Selection::Version {
                        object_id: "missing".into(),
                        version: "missing".into()
                    },
                    &budget(&cancel)
                )
                .unwrap()
                .unit_result,
            OriginalV3UnitResult::Absent
        );
    }
    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "requires TMPDIR on Btrfs"]
    fn btrfs_cursor_keeps_its_inventory_after_device_renumbering() {
        let dir = sinomo_testdir::TestDir::new("original-v3-device-renumber");
        let source = dir.join("source");
        let dest = dir.join("target");
        original(&source, b"same immutable source", false);
        let _store = target(&dest);
        let cancel = AtomicBool::new(false);
        let mut driver = OriginalV3Converter::default();
        let step = |driver: &mut OriginalV3Converter| {
            driver.step(&source, &dest, "inventory", &OriginalV3Selection::All, &budget(&cancel))
        };
        step(&mut driver).unwrap();
        let path = OriginalV3Converter::checkpoint_path(&source, &dest, "inventory").unwrap();
        let mut saved: Progress = checked_read(&path).unwrap();
        saved.active_identity.device ^= 1;
        checked_write(&path, &saved).unwrap();
        let mut resumed = OriginalV3Converter::default();
        assert!((0..200).any(|_| step(&mut resumed).unwrap().coverage_complete));
        assert!(OriginalV3Converter::completed_original_observation(&source, &dest, "inventory").unwrap());
        let mut changed: Progress = checked_read(&path).unwrap();
        changed.active_identity.inode ^= 1;
        checked_write(&path, &changed).unwrap();
        assert!(OriginalV3Converter::completed_original_observation(&source, &dest, "inventory").is_err());
    }
    #[test]
    fn original_v3_changed_source_and_replacement_target_refuse_original_checkpoint() {
        let dir = sinomo_testdir::TestDir::new("original-v3-identity");
        let source = dir.path().join("source");
        let target_path = dir.path().join("target");
        original(&source, b"source", false);
        let current = target(&target_path);
        let cancel = AtomicBool::new(false);
        let mut converter = OriginalV3Converter::default();
        converter
            .step(
                &source,
                &target_path,
                "inventory",
                &OriginalV3Selection::All,
                &budget(&cancel),
            )
            .unwrap();
        let marker = source.join(MARKER);
        let bytes = fs::read(&marker).unwrap();
        fs::write(&marker, b"changed source").unwrap();
        assert!(
            converter
                .step(
                    &source,
                    &target_path,
                    "inventory",
                    &OriginalV3Selection::All,
                    &budget(&cancel)
                )
                .is_err()
        );
        fs::write(&marker, bytes).unwrap();
        drop(current);
        fs::rename(&target_path, dir.path().join("retained-target")).unwrap();
        let _replacement = target(&target_path);
        assert!(
            converter
                .step(
                    &source,
                    &target_path,
                    "inventory",
                    &OriginalV3Selection::All,
                    &budget(&cancel)
                )
                .is_err()
        );
    }
    #[test]
    fn original_v3_retired_repair_records_do_not_retain_or_resurrect_current_content() {
        let dir = sinomo_testdir::TestDir::new("original-v3-retired-repair");
        let source = dir.path().join("source");
        let target_path = dir.path().join("target");
        let manifest = original(&source, b"retired payload", true);
        let (_, _, catalog) = read_source(&source).unwrap();
        let path = segment_path(&source, &catalog, &catalog.segments[0]);
        let mut appender = FrameAppender::open(
            &path,
            FrameLimits::new(HEADER_LIMIT as u32, PAYLOAD_LIMIT).unwrap(),
        )
        .unwrap();
        let identity = ContentStoreReservationIdentity {
            object_id: manifest.object_id.clone(),
            version: manifest.version.clone(),
            content_hash: manifest.content_hash.clone(),
        };
        let root = || RepairRoot {
            subject: RepairSubject {
                queue_incarnation: "old-queue".into(),
                producer: "old-producer".into(),
                target: "old-target".into(),
                intent_id: "old-intent".into(),
                request_id: "old-request".into(),
            },
            original_command_digest: "old-command".into(),
            store_incarnation: catalog.store_incarnation.clone(),
            source: RepairSource::Manifest(manifest.clone()),
        };
        let ops = [
            Operation::Release(identity),
            Operation::RetainRepairPayload(root()),
            Operation::ReleaseRepairPayload(root()),
            Operation::RestoreRepairPayload {
                root: root(),
                released: false,
            },
        ];
        for (offset, operation) in ops.into_iter().enumerate() {
            appender
                .append_bytes(
                    &encode(&Record {
                        format_version: 2,
                        store_incarnation: catalog.store_incarnation.clone(),
                        generation: catalog.generation,
                        sequence: 3 + offset as u64,
                        operation,
                    })
                    .unwrap(),
                    &[],
                )
                .unwrap();
        }
        drop(appender);
        let current = target(&target_path);
        let cancel = AtomicBool::new(false);
        let mut converter = OriginalV3Converter::default();
        let mut complete = false;
        for _ in 0..200 {
            if converter
                .step(
                    &source,
                    &target_path,
                    "inventory",
                    &OriginalV3Selection::All,
                    &budget(&cancel),
                )
                .unwrap()
                .coverage_complete
            {
                complete = true;
                break;
            }
        }
        assert!(complete);
        current.complete_inventory_import("inventory").unwrap();
        assert!(
            current
                .object_version_is_deleted(&manifest.object_id, &manifest.version)
                .unwrap()
        );
        assert_eq!(
            current
                .content_reservation_roots()
                .unwrap()
                .reservation_count,
            0
        );
    }

    #[test]
    fn current_content_decoder_refuses_retired_repair_variant_bytes() {
        let dir = sinomo_testdir::TestDir::new("current-refuses-original-repair");
        let current = LocalContentStore::new(dir.path()).unwrap();
        drop(current);
        let bytes = fs::read(dir.path().join("inline-bindings/current.sobj")).unwrap();
        let envelope: Envelope = serde_ext::from_bytes(&bytes).unwrap();
        let catalog = envelope.catalog;
        let path = segment_path(dir.path(), &catalog, catalog.segments.last().unwrap());
        let root = RepairRoot {
            subject: RepairSubject {
                queue_incarnation: "old-queue".into(),
                producer: "old-producer".into(),
                target: "old-target".into(),
                intent_id: "old-intent".into(),
                request_id: "old-request".into(),
            },
            original_command_digest: "old-command".into(),
            store_incarnation: catalog.store_incarnation.clone(),
            source: RepairSource::Blob {
                content_hash: format!("blake3:{}", blake3::hash(b"old").to_hex()),
                size_bytes: 3,
            },
        };
        let mut appender = FrameAppender::open(
            &path,
            FrameLimits::new(HEADER_LIMIT as u32, PAYLOAD_LIMIT).unwrap(),
        )
        .unwrap();
        appender
            .append_bytes(
                &encode(&Record {
                    format_version: 2,
                    store_incarnation: catalog.store_incarnation,
                    generation: catalog.generation,
                    sequence: 1,
                    operation: Operation::RetainRepairPayload(root),
                })
                .unwrap(),
                &[],
            )
            .unwrap();
        drop(appender);
        assert!(
            LocalContentStore::open_for_inspection(dir.path())
                .content_reservation_roots()
                .is_err(),
            "current4 never interprets the retired original variant"
        );
    }
    #[test]
    fn original_v3_partial_index_publication_reopens_without_advancing_source() {
        for phase in [
            "logs",
            "missing-stage",
            "files",
            "first-install",
            "before-directories",
            "directories",
        ] {
            let dir = sinomo_testdir::TestDir::new("original-v3-partial-index");
            let source = dir.join("source");
            let target_path = dir.join("target");
            let bytes = b"retained payload across partial index publication";
            let manifest = original(&source, bytes, false);
            let (_, _, catalog) = read_source(&source).unwrap();
            let active = segment_path(&source, &catalog, &catalog.segments[0]);
            let before = fs::read(&active).unwrap();
            let identity = probe_original_v3(&source, 128 * 1024).unwrap();
            let current = target(&target_path);
            let cancel = AtomicBool::new(false);
            let checkpoint =
                OriginalV3Converter::checkpoint_path(&source, &target_path, "inventory").unwrap();
            let mut converter = OriginalV3Converter::default();
            FAIL_INDEX_PUBLICATION.with(|fault| fault.set(Some(phase)));
            let mut failed = false;
            for _ in 0..200 {
                match converter.step(
                    &source,
                    &target_path,
                    "inventory",
                    &OriginalV3Selection::All,
                    &budget(&cancel),
                ) {
                    Err(error) => {
                        if phase == "missing-stage" {
                            assert!(error.to_string().contains(".source3-index-"), "{error}");
                        } else {
                            assert!(
                                error
                                    .to_string()
                                    .contains(&format!("injected index publication at {phase}")),
                                "{error}"
                            );
                        }
                        failed = true;
                        break;
                    }
                    Ok(step) => assert!(
                        !step.coverage_complete,
                        "fault must interrupt actual publication"
                    ),
                }
            }
            assert!(failed, "{phase}");
            let progress: Progress = checked_read(&checkpoint).unwrap();
            assert_eq!(
                progress.sequence, 0,
                "{phase}: cursor precedes unfinished first record"
            );
            assert_eq!(progress.offset, 0);
            assert!(!progress.index_complete);
            assert!(
                current
                    .read_object_version(&manifest.object_id, &manifest.version)
                    .is_err()
            );
            let work = checkpoint.parent().unwrap();
            if phase == "missing-stage" {
                assert!(
                    !row_path(
                        work,
                        &Key::Version(manifest.object_id.clone(), manifest.version.clone())
                    )
                    .unwrap()
                    .exists(),
                    "no key can install after a failed data barrier"
                );
                let stem = blake3::hash(manifest.object_id.as_bytes()).to_hex();
                assert!(!work.join("objects").join(format!("{stem}.end")).exists());
            }
            for entry in fs::read_dir(work.join("refs")).unwrap() {
                for reference in fs::read_dir(entry.unwrap().path()).unwrap() {
                    let name = reference.unwrap().file_name();
                    assert_eq!(
                        name.to_string_lossy().len(),
                        64,
                        "private files cannot become live roots"
                    );
                }
            }
            drop(converter);
            let mut converter = OriginalV3Converter::default();
            let mut object_complete = false;
            for _ in 0..200 {
                let step = converter
                    .step(
                        &source,
                        &target_path,
                        "inventory",
                        &OriginalV3Selection::Object(manifest.object_id.clone()),
                        &budget(&cancel),
                    )
                    .unwrap();
                if step.unit_result == OriginalV3UnitResult::Covered {
                    object_complete = true;
                    break;
                }
            }
            assert!(
                object_complete,
                "{phase}: priority object index must retain both keys"
            );
            assert!(
                covered(
                    work,
                    &Key::Version(manifest.object_id.clone(), manifest.version.clone())
                )
                .unwrap()
            );
            assert!(
                covered(
                    work,
                    &Key::Reservation(ContentStoreReservationIdentity {
                        object_id: manifest.object_id.clone(),
                        version: manifest.version.clone(),
                        content_hash: manifest.content_hash.clone(),
                    })
                )
                .unwrap()
            );
            let mut complete = false;
            for _ in 0..200 {
                let step = converter
                    .step(
                        &source,
                        &target_path,
                        "inventory",
                        &OriginalV3Selection::All,
                        &budget(&cancel),
                    )
                    .unwrap();
                if step.coverage_complete {
                    complete = true;
                    break;
                }
            }
            assert!(complete, "{phase}");
            assert_eq!(
                current
                    .read_object_version(&manifest.object_id, &manifest.version)
                    .unwrap()
                    .1,
                bytes
            );
            current.complete_inventory_import("inventory").unwrap();
            assert_eq!(fs::read(&active).unwrap(), before);
            assert_eq!(probe_original_v3(&source, 128 * 1024).unwrap(), identity);
        }
    }
    #[test]
    fn original_v3_row_before_cursor_failure_keeps_loose_unit_enumerated() {
        let dir = sinomo_testdir::TestDir::new("original-v3-row-before-cursor");
        let source = dir.path().join("source");
        let target_path = dir.path().join("target");
        let bytes = vec![0x53; 32 * 1024];
        let manifest = original(&source, &bytes, false);
        let current = target(&target_path);
        let cancel = AtomicBool::new(false);
        let mut converter = OriginalV3Converter::default();
        let mut failed = false;
        let mut complete = false;
        FAIL_AFTER_INDEX_ROW.with(|fault| fault.set(true));
        for _ in 0..200 {
            match converter.step(
                &source,
                &target_path,
                "inventory",
                &OriginalV3Selection::All,
                &budget(&cancel),
            ) {
                Err(error) => {
                    assert!(!failed);
                    assert!(error.to_string().contains("injected index row"));
                    failed = true;
                }
                Ok(step) => {
                    if step.coverage_complete {
                        complete = true;
                        break;
                    }
                }
            }
        }
        assert!(failed && complete);
        assert_eq!(current.read_blob(&manifest.content_hash).unwrap(), bytes);
        current.complete_inventory_import("inventory").unwrap();
    }
    #[test]
    fn original_v3_probe_is_bounded_and_survives_owner_relocation_without_certifying_frames() {
        let dir = sinomo_testdir::TestDir::new("original-v3-probe");
        let live = dir.path().join("live");
        original(&live, b"probe bytes", false);
        assert!(probe_original_v3(&live, 1).is_err());
        let before = probe_original_v3(&live, 128 * 1024).unwrap();
        let retained = dir.path().join("retained");
        fs::rename(&live, &retained).unwrap();
        assert_eq!(probe_original_v3(&retained, 128 * 1024).unwrap(), before);
        let (_, _, catalog) = read_source(&retained).unwrap();
        let active = segment_path(&retained, &catalog, catalog.segments.last().unwrap());
        let mut bytes = fs::read(&active).unwrap();
        bytes[0] ^= 1;
        fs::write(&active, bytes).unwrap();
        let changed = probe_original_v3(&retained, 128 * 1024).unwrap();
        assert_ne!(
            changed.active_file_identity_digest,
            before.active_file_identity_digest
        );
        // The probe intentionally does not decode/hash payload frames or claim readiness.
    }
    #[test]
    fn original_v3_small_demand_preempts_large_copy_and_original_unit_resumes() {
        small_demand_preemption_case(false);
    }
    #[test]
    fn original_v3_small_demand_preempts_large_verification_without_resetting_it() {
        small_demand_preemption_case(true);
    }
    fn small_demand_preemption_case(verifying: bool) {
        let dir = sinomo_testdir::TestDir::new(if verifying {
            "original-v3-verification-preemption"
        } else { "original-v3-priority-preemption" });
        let source = dir.path().join("source");
        let target_path = dir.path().join("target");
        let large_bytes = vec![0x27; 512 * 1024];
        let large = original(&source, &large_bytes, false);
        let (_, _, catalog) = read_source(&source).unwrap();
        let path = segment_path(&source, &catalog, catalog.segments.last().unwrap());
        let small_bytes = b"priority bytes";
        let small = ContentManifest {
            object_id: "priority-object".into(),
            version: "priority-version".into(),
            content_hash: format!("blake3:{}", blake3::hash(small_bytes).to_hex()),
            size_bytes: small_bytes.len() as u64,
            extents: Vec::new(),
        };
        let reservation = Reservation {
            identity: ContentStoreReservationIdentity {
                object_id: small.object_id.clone(),
                version: small.version.clone(),
                content_hash: small.content_hash.clone(),
            },
            reserved_at_unix_nanos: 456,
            causal_stamp: Stamp {
                store_incarnation: catalog.store_incarnation.clone(),
                catalog_scan_epoch: 0,
            },
        };
        let mut appender = FrameAppender::open(
            &path,
            FrameLimits::new(HEADER_LIMIT as u32, PAYLOAD_LIMIT).unwrap(),
        )
        .unwrap();
        appender
            .append_bytes(
                &encode(&Record {
                    format_version: 2,
                    store_incarnation: catalog.store_incarnation.clone(),
                    generation: catalog.generation,
                    sequence: 2,
                    operation: Operation::Bind {
                        manifest: small.clone(),
                        published_at_unix_nanos: 456,
                        reservation: Some(reservation),
                    },
                })
                .unwrap(),
                small_bytes,
            )
            .unwrap();
        drop(appender);
        let source_identity = probe_original_v3(&source, 128 * 1024).unwrap();
        let current = target(&target_path);
        let target_identity = current.store_incarnation().unwrap();
        let cancel = AtomicBool::new(false);
        let mut converter = OriginalV3Converter::default();
        let checkpoint =
            OriginalV3Converter::checkpoint_path(&source, &target_path, "inventory").unwrap();
        let work = checkpoint.parent().unwrap();
        let large_key = Key::Blob(large.content_hash.clone());
        let mut paused = None;
        for _ in 0..100 {
            converter
                .step(
                    &source,
                    &target_path,
                    "inventory",
                    &OriginalV3Selection::All,
                    &budget(&cancel),
                )
                .unwrap();
            let row = row_path(work, &large_key).unwrap();
            if row.exists() {
                let indexed: Indexed = checked_read(&row).unwrap();
                if let Some(copy) = indexed.copy {
                    if (verifying && copy.copied == large.size_bytes)
                        || (!verifying && copy.copied > 0 && copy.copied < large.size_bytes) {
                        paused = Some(copy);
                        break;
                    }
                }
            }
        }
        let paused =
            paused.expect("large background unit reached its durable bounded copy boundary");
        let selection = OriginalV3Selection::Version {
            object_id: small.object_id.clone(),
            version: small.version.clone(),
        };
        let mut small_ready = false;
        for _ in 0..20 {
            if converter
                .step(
                    &source,
                    &target_path,
                    "inventory",
                    &selection,
                    &budget(&cancel),
                )
                .unwrap()
                .unit_result
                == OriginalV3UnitResult::Covered
            {
                small_ready = true;
                break;
            }
        }
        assert!(small_ready);
        assert_eq!(
            current
                .read_object_version(&small.object_id, &small.version)
                .unwrap()
                .1,
            small_bytes
        );
        assert!(
            !current
                .blob_storage_candidate_exists(&large.content_hash)
                .unwrap(),
            "small demand must finish before large publication"
        );
        let still_paused: Indexed = checked_read(&row_path(work, &large_key).unwrap()).unwrap();
        let still_paused = still_paused.copy.unwrap();
        assert_eq!(still_paused.copied, paused.copied);
        assert_eq!(still_paused.staged, paused.staged);
        assert_eq!(still_paused.source.hash, paused.source.hash);
        let mut complete = false;
        for _ in 0..200 {
            if converter
                .step(
                    &source,
                    &target_path,
                    "inventory",
                    &OriginalV3Selection::All,
                    &budget(&cancel),
                )
                .unwrap()
                .coverage_complete
            {
                complete = true;
                break;
            }
        }
        assert!(complete);
        assert_eq!(
            current
                .read_object_version(&large.object_id, &large.version)
                .unwrap()
                .1,
            large_bytes
        );
        assert_eq!(current.store_incarnation().unwrap(), target_identity);
        assert_eq!(
            probe_original_v3(&source, 128 * 1024).unwrap(),
            source_identity
        );
    }
}
