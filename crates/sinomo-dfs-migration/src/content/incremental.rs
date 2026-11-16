//! Bounded source discovery and replay into the normal current catalog.
use super::*;
use sinomo_core::storage::checksummed_frame::{FrameLimits, FrameRead, read_frame};
use sinomo_dfs::{ContentStoreStagingRow as Row, ContentStoreStagingWriter};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
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
pub(super) fn published_three_checkpoint(root:&Path)->Result<bool> {
    let path=root.join(CONVERSION).join("progress.sobj");
    if !path.exists() || source_format(root)?!=Some(3){return Ok(false);}
    let bytes=durable_file::read_object_file_bytes(&path,1024*1024).map_err(error)?;
    let progress:Progress=serde_ext::from_bytes(&bytes).map_err(error)?;
    if progress.version!=1 {return Err(error("unsupported original legacy cursor"));}
    Ok(progress.publication_step==3 && matches!(progress.phase,Phase::Publish|Phase::Cleanup|Phase::Done))
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
    #[serde(default,skip_serializing_if="Option::is_none")]
    pending_copy: Option<super::legacy_blob::CopyCursor>,
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
    store: Option<Arc<ContentStoreStagingWriter>>,
    walker: Option<Walker>,
    catalog: Option<Catalog>,
    _conversion: File,
    _owner: Option<File>,
    _reservation: Option<File>,
    _readers: Option<File>,
    turn_deadline: std::time::Instant,
    turn_bytes: u64,
    turn_limit: u64,
    turn_advanced: bool,
    saved_progress: Option<Vec<u8>>,
    copy_warm: Option<super::legacy_blob::WarmCopy>,
    pending_discovery: Option<(String,PathBuf,PathBuf)>,
    pending_install: Option<PathBuf>,
    verify_warm: Option<(ContentManifest,sinomo_dfs::ObjectVersionReader,blake3::Hasher,u64)>,
    install_warm: Option<(PathBuf,sinomo_dfs::ContentImportBlobVerifier)>,
    inline_seal_warm: Option<(PathBuf,sinomo_dfs::ContentImportBlobVerifier)>,
    current_only: bool,
    job: super::LegacyContentJobReference,
    upgrade: Option<super::legacy_stage::Upgrade>,
    upgrade_driver: super::original_v3::OriginalV3Converter,
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
    /// Called by the sole component owner only after all conversion steps drain.
    /// Persisted source checkpoints remain; only warm leases/cursors are released.
    pub(crate) fn release_checkpointed_sessions(&self) -> Result<()> {
        let mut sessions = self.sessions.lock().map_err(error)?;
        for session in sessions.values() {
            if session.try_lock().is_err() { return Err(error("content source session is still active")); }
        }
        sessions.clear();
        Ok(())
    }
    /// Read only the actual existing parent reference. This does not create a
    /// session, take legacy source leases, advance conversion, or claim Ready.
    pub(crate) fn existing_job_reference(&self, root:&Path)->Result<Option<super::LegacyContentJobReference>> {
        Ok(self.observe_existing_job(root)?.map(|(reference,_)|reference))
    }
    pub(crate) fn existing_job_complete(&self, root:&Path)->Result<bool> {
        Ok(self.observe_existing_job(root)?.is_some_and(|(_,complete)|complete))
    }
    fn observe_existing_job(&self, root:&Path)->Result<Option<(super::LegacyContentJobReference,bool)>> {
        let path=root.join(CONVERSION).join("progress.sobj");
        if !path.try_exists().map_err(|e|io(&path,e))? {return Ok(None);}
        let before=durable_file::read_object_file_bytes(&path,1024*1024).map_err(error)?;
        let progress:Progress=serde_ext::from_bytes(&before).map_err(error)?;
        if progress.version!=1 {return Err(error("unsupported original legacy cursor"));}
        let reference=super::legacy_stage::reference(root)?;
        let current_root=source_format(root)?==Some(LocalContentStore::FORMAT_VERSION);
        if current_root {
            LocalContentStore::open_for_inspection(root).validate_current_layout().map_err(error)?;
            let target=root.join(CONVERSION).join("store");
            if target.join(MARKER).exists() {
                let expected=super::legacy_stage::target_incarnation(&target)?;
                let current=LocalContentStore::open_for_inspection(root).store_incarnation().map_err(error)?;
                if current!=expected {return Err(error("current root replaced an unfinished original legacy target"));}
            }
        }
        if durable_file::read_object_file_bytes(&path,1024*1024).map_err(error)?!=before {
            return Err(MetadataBackendError::StoragePreparationPending{root:root.to_owned(),processed_records:0});
        }
        let complete=current_root && progress.phase==Phase::Done && progress.gc_hints_complete && progress.gc_hint_scan_version==1;
        Ok(Some((reference,complete)))
    }

    /// Enroll the existing checkpoint before any public source mutation. Private
    /// current staging creation is owned by that same checkpoint/session.
    pub(crate) fn enroll(&self, root: &Path) -> Result<Option<super::LegacyContentJobReference>> {
        if !root.exists() { return Ok(None); }
        if !root.join(CONVERSION).join("progress.sobj").try_exists().map_err(|e| io(root, e))?
            && LocalContentStore::has_current_initialization(root).map_err(error)? {
            return Ok(None);
        }
        let key=fs::canonicalize(root).map_err(|e|io(root,e))?;
        let warm=self.sessions.lock().map_err(error)?.get(&key).cloned();
        if let Some(session)=warm {
            let held=session.lock().map_err(error)?;
            if let Some(active)=held.as_ref() {
                return Ok(Some(active.job.clone()));
            }
        }
        if current(root)? {return self.existing_job_reference(root);}
        let session={let mut sessions=self.sessions.lock().map_err(error)?;
            Arc::clone(sessions.entry(key.clone()).or_insert_with(||Arc::new(Mutex::new(None))))};
        let mut held=session.lock().map_err(error)?;
        if held.is_none(){*held=Some(Session::open(&key)?);}
        let active=held.as_ref().unwrap();
        Ok(Some(active.job.clone()))
    }
    /// Exact predecessor DATA for the single owner's already-published3 handoff.
    pub(crate) fn published_v3_successor(&self, root: &Path) -> Result<Option<super::LegacyPublishedV3>> {
        super::legacy_stage::published_v3(root)
    }

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
        self.prepare_bounded(root,max_records,8*1024*1024,std::time::Instant::now()+std::time::Duration::from_millis(50))
    }
    pub(crate) fn prepare_bounded(&self, root:&Path, max_records:usize, max_bytes:u64, deadline:std::time::Instant)->Result<DfsStoragePreparation> {
        if max_bytes==0 || std::time::Instant::now()>=deadline {return Ok(DfsStoragePreparation::Pending{processed_records:0});}
        if !root.exists() {
            return Ok(DfsStoragePreparation::Ready);
        }
        let key = fs::canonicalize(root).map_err(|e| io(root, e))?;
        // An existing conversion owns the original content/reservation leases.
        // After its marker publication, cold current-layout inspection would
        // reacquire that same owner and wait on ourselves. Resume the exact
        // checkpoint owner first; no caller or source identity is replaced.
        let warm = self.sessions.lock().map_err(|_| error("converter session mutex poisoned"))?
            .get(&key).cloned();
        if let Some(session) = warm {
            let result = {
                let mut held = session.lock().map_err(|_| error("content root session mutex poisoned"))?;
                if let Some(active) = held.as_mut() {
                    let result = active.advance(max_records.max(1),max_bytes,deadline);
                    if result.is_err(){active.discard_failed_warm_work();}
                    if matches!(&result, Ok(DfsStoragePreparation::Ready)) { active.release_source_leases()?; *held = None; }
                    Some(result)
                } else { None }
            };
            if let Some(result) = result {
                if matches!(&result, Ok(DfsStoragePreparation::Ready)) {
                    self.sessions.lock().map_err(|_| error("converter session mutex poisoned"))?.remove(&key);
                }
                return self.qualify_result(&key,result);
            }
        }
        let source_generation=source_format(&key)?;
        if source_generation==Some(3) && published_three_checkpoint(&key)? {
            // This exact predecessor is complete, but it is not current4.
            // The same owner resolves its validated successor without replaying
            // the old parent or pretending another record progressed.
            super::legacy_stage::published_v3(&key)?.ok_or_else(||error("published3 parent has no exact successor evidence"))?;
            return Ok(DfsStoragePreparation::Pending{processed_records:0});
        }
        if source_generation==Some(3) && !key.join(CONVERSION).join("progress.sobj").exists() {
            return Err(error("format3 belongs to its original-v3 unit transformer"));
        }
        if source_generation.is_some_and(|version| ![1,2,3,LocalContentStore::FORMAT_VERSION].contains(&version)) {
            return Err(error("unsupported original content generation; source preserved"));
        }
        // The provider retains its ordinary role/data-owner checks. An exact
        // CURRENT initialization resumes in LocalContentStore::new, without
        // entering source-format discovery or conversion.
        if !key.join(CONVERSION).join("progress.sobj").exists()
            && LocalContentStore::has_current_initialization(&key).map_err(error)? {
            return Ok(DfsStoragePreparation::Ready);
        }
        let progress = key.join(CONVERSION).join("progress.sobj");
        if progress.exists() && source_format(&key)?==Some(LocalContentStore::FORMAT_VERSION) {
            self.existing_job_reference(&key)?;
        }
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
            let result = held.as_mut().unwrap().advance(max_records.max(1),max_bytes,deadline);
            if result.is_err(){held.as_mut().unwrap().discard_failed_warm_work();}
            if matches!(&result, Ok(DfsStoragePreparation::Ready)) {
                held.as_mut().unwrap().release_source_leases()?;
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
        self.qualify_result(&key,result)
    }
    fn qualify_result(&self, root:&Path, result:Result<DfsStoragePreparation>)->Result<DfsStoragePreparation> {
        if matches!(&result,Ok(DfsStoragePreparation::Ready)) && !current(root)? {
            if super::legacy_stage::published_v3(root)?.is_some() {
                return Ok(DfsStoragePreparation::Pending{processed_records:1});
            }
            return Err(error("legacy completion has neither current4 output nor an exact published3 successor"));
        }
        result
    }
}
impl Session {
    fn open(root: &Path) -> Result<Self> {
        let conversion = lock(&root.join("content-conversion.lock"))?;
        // Format transitions belong to this converter lock. Validate current
        // layout before taking the legacy source leases; ordinary current
        // mutations cannot downgrade its format and hint cleanup grants no
        // content authority.
        let current_root = current(root)?;
        let current_incarnation = if current_root {Some(LocalContentStore::open_for_inspection(root).store_incarnation().map_err(error)?)} else {None};
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
            current_root && matches!(progress.phase, Phase::Loose | Phase::Done | Phase::GcHints);
        if current_only && progress.phase != Phase::GcHints {
            progress.phase = Phase::GcHints;
            progress.lane = 0;
            progress.gc_hints_complete = false;
            write(&progress_path, &progress)?;
        }
        if !progress_path.exists() {write(&progress_path,&progress)?;}
        let upgrade=if progress.publication_step==0 && !matches!(progress.phase,Phase::Cleanup|Phase::Done) {
            super::legacy_stage::Upgrade::prepare(root)?
        } else {None};
        let target = work.join("store");
        let store = if upgrade.as_ref().is_none_or(|upgrade|upgrade.published()) && progress.phase != Phase::Publish
            && progress.phase != Phase::Cleanup
            && progress.phase != Phase::Done
            && progress.phase != Phase::GcHints
        {
            Some(Arc::new(ContentStoreStagingWriter::open_candidate(&target).map_err(error)?))
        } else {
            None
        };
        let job=if let Some(upgrade)=&upgrade {upgrade.job.clone()}
            else if target.join(MARKER).exists() {super::legacy_stage::reference(root)?}
            else if let Some(incarnation)=&current_incarnation {super::legacy_stage::job(incarnation.clone())}
            else {return Err(error("legacy session has no original target identity"));};
        if let Some(current_incarnation)=&current_incarnation {
            if target.join(MARKER).exists() && super::legacy_stage::target_incarnation(&target)?!=*current_incarnation {
                return Err(error("legacy session found a replacement current root"));
            }
        }
        let mut session=Self {
            root: root.to_owned(),
            work,
            target,
            progress,
            store,
            walker: None,
            catalog: None,
            _conversion: conversion,
            _owner: Some(owner),
            _reservation: Some(reservation),
            _readers: readers,
            turn_deadline:std::time::Instant::now(),turn_bytes:0,turn_limit:0,turn_advanced:false,saved_progress:None,
            copy_warm:None,pending_discovery:None,pending_install:None,verify_warm:None,install_warm:None,inline_seal_warm:None,
            current_only,
            job,
            upgrade,
            upgrade_driver: Default::default(),
        };
        if current_root {session.release_source_leases()?;}
        Ok(session)
    }
    fn discard_failed_warm_work(&mut self) {
        self.saved_progress=None;
        self.copy_warm=None;self.verify_warm=None;self.install_warm=None;self.inline_seal_warm=None;
        self.walker=None;self.pending_discovery=None;self.pending_install=None;
    }
    fn release_source_leases(&mut self)->Result<()> {
        if let Some(file)=self._reservation.take(){FileExt::unlock(&file).map_err(error)?;}
        if let Some(file)=self._owner.take(){FileExt::unlock(&file).map_err(error)?;}
        Ok(())
    }
    fn save(&mut self) -> Result<()> {
        let encoded=serde_ext::to_bytes(&self.progress,1,LengthMode::U32).map_err(error)?;
        if self.saved_progress.as_ref()==Some(&encoded) {return Ok(());}
        write(&self.work.join("progress.sobj"), &self.progress)?;
        self.saved_progress=Some(encoded);
        Ok(())
    }
    fn transition(&mut self, phase: Phase) -> Result<()> {
        self.progress.phase = phase;
        self.walker = None;
        self.pending_discovery = None;
        self.pending_install = None;
        self.save()
    }
    fn store(&self) -> Result<&ContentStoreStagingWriter> {
        self.store
            .as_deref()
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
        if let Some((held_kind,path,receipt))=&self.pending_discovery {
            if held_kind!=kind {return Err(error("legacy source changed phase while a byte effect is pending"));}
            return Ok(Some((path.clone(),receipt.clone())));
        }
        if self.walker.is_none() {
            self.walker = Some(Walker::new(source)?);
        }
        loop {
            self.require_turn()?;
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
            self.pending_discovery=Some((kind.to_owned(),path.clone(),receipt.clone()));
            return Ok(Some((path, receipt)));
        }
    }
    fn mark(&mut self, path: &Path, receipt: &Path) -> Result<()> {
        let metadata = fs::symlink_metadata(path).map_err(|e| io(path, e))?;
        write(
            receipt,
            &DiscoveryReceipt {
                relative: path.strip_prefix(&self.root).map_err(error)?.to_owned(),
                size: metadata.len(),
                modified: metadata.modified().ok(),
                directory: metadata.is_dir(),
            },
        )?;
        if self.pending_discovery.as_ref().is_some_and(|(_,held,_)|held==path){self.pending_discovery=None;}
        Ok(())
    }
    fn payload_receipt(&self, hash: &str, origin: &str) -> Result<PathBuf> {
        let hex = hash_hex(hash)?;
        Ok(self
            .work
            .join("payload-receipts")
            .join(hex)
            .join(format!("{origin}.sobj")))
    }
    fn require_turn(&self)->Result<()> {
        if self.turn_bytes==0 || std::time::Instant::now()>=self.turn_deadline {
            return Err(MetadataBackendError::StoragePreparationPending{root:self.root.clone(),processed_records:usize::from(self.turn_advanced)});
        }
        Ok(())
    }
    fn source_bytes(&mut self,path:&Path)->Result<Vec<u8>> {
        self.require_turn()?;
        let length=fs::metadata(path).map_err(|e|io(path,e))?.len();
        if length>self.turn_limit {return Err(error(format!("source metadata record/header {} requires {length} bytes beyond configured {} byte turn; source retained",path.display(),self.turn_limit)));}
        if length>self.turn_bytes {return Err(MetadataBackendError::StoragePreparationPending{root:self.root.clone(),processed_records:0});}
        let bytes=durable_file::read_object_file_bytes(path,self.turn_limit).map_err(error)?;
        self.turn_bytes=self.turn_bytes.saturating_sub(bytes.len() as u64);Ok(bytes)
    }
    fn source_record<T:for<'de> Deserialize<'de>>(&mut self,path:&Path)->Result<T> {
        serde_ext::from_bytes(&self.source_bytes(path)?).map_err(error)
    }
    fn copy(&mut self, source:&BlobSource, origin:&str, offset:u64, snapshot:bool)->Result<()> {
        self.require_turn()?;
        let receipt=self.payload_receipt(&source.hash,origin)?;
        let mut retained=if receipt.exists(){read::<PayloadReceipt>(&receipt)?}else{PayloadReceipt{source:origin.into(),offset,pending_copy:None}};
        if snapshot && retained.offset!=offset {return Err(error("duplicate packed snapshot key"));}
        if retained.pending_copy.is_some() && (retained.offset!=offset || retained.source!=origin) {return Err(error("legacy byte continuation changed its original source record"));}
        retained.source=origin.into();retained.offset=offset;
        let writer=Arc::clone(self.store.as_ref().ok_or_else(||error("legacy copy has no current candidate owner"))?);
        let before=self.turn_bytes;
        let done=super::legacy_blob::copy_step(&self.root,&self.target,source,&receipt,&mut retained.pending_copy,&mut self.copy_warm,&writer,&mut self.turn_bytes,self.turn_deadline)?;
        self.turn_advanced |= self.turn_bytes<before;
        write(&receipt,&retained)?;
        if !done {return Err(MetadataBackendError::StoragePreparationPending{root:self.root.clone(),processed_records:usize::from(self.turn_advanced)});}
        Ok(())
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
        &mut self,
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
                    snapshot_cursor::next::<PackedLocation>(&snapshot, &mut cursor,self.turn_limit,self.turn_bytes)?
                {
                    self.turn_bytes=self.turn_bytes.saturating_sub(cursor.offset-offset);
                    if self.turn_bytes==0 {return Err(error("packed row metadata leaves no byte budget for its payload continuation"));}
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
                if length as u64>self.turn_limit {return Err(error("packed log record exceeds configured byte turn"));}
                if length as u64>self.turn_bytes {return Err(MetadataBackendError::StoragePreparationPending{root:self.root.clone(),processed_records:0});}
                let mut bytes = vec![0; length];
                match file.read_exact(&mut bytes) {
                    Ok(()) => {
                        self.turn_bytes-=bytes.len() as u64;
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
            let batch: Batch = self.source_record(&path)?;
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
            let record: Single = self.source_record(&path)?;
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
        let envelope: Envelope = self.source_record(&path)?;
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
        const INLINE_INPUT_BOUND:u64=96*1024+256;
        if self.turn_limit<INLINE_INPUT_BOUND {return Err(error("configured byte turn is below the bounded original inline record capacity"));}
        if self.turn_bytes<INLINE_INPUT_BOUND {return Err(MetadataBackendError::StoragePreparationPending{root:self.root.clone(),processed_records:0});}
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
                self.turn_bytes=self.turn_bytes.saturating_sub(frame.frame_end()-frame.frame_start()+frame.payload_bytes());
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
            if segment.sealed_end!=Some(end) {return Err(error("retained inline segment certificate length mismatch"));}
            let digest=segment.sealed_digest.as_ref().ok_or_else(||error("retained inline segment lacks its certificate"))?;
            self.require_turn()?;
            if self.inline_seal_warm.as_ref().is_none_or(|(held,_)|held!=&path) {
                self.inline_seal_warm=Some((path.clone(),sinomo_dfs::ContentImportBlobVerifier::existing(&path,format!("blake3:{digest}"),end).map_err(error)?));
            }
            let (_,verifier)=self.inline_seal_warm.as_mut().unwrap();let before=verifier.verified_bytes();
            let done=verifier.step(self.turn_bytes,self.turn_deadline,&std::sync::atomic::AtomicBool::new(false)).map_err(error)?;
            let advanced=verifier.verified_bytes()-before;self.turn_bytes-=advanced;self.turn_advanced|=advanced!=0;
            if !done {return Err(MetadataBackendError::StoragePreparationPending{root:self.root.clone(),processed_records:usize::from(self.turn_advanced)});}
            self.inline_seal_warm.take().unwrap().1.finish().map_err(error)?;
        }
        self.progress.inline_segment += 1;
        self.progress.inline_offset = 0;
        Ok(false)
    }
    fn apply_inline(&mut self, operation: Operation, hash: &str, payload: Vec<u8>) -> Result<()> {
        let store = Arc::clone(self.store.as_ref().ok_or_else(||error("staged writer is absent"))?);
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
                let manifest: ContentManifest = self.source_record(&path)?;
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
    fn verify_manifest(&mut self, manifest:&ContentManifest)->Result<bool> {
        self.require_turn()?;
        if self.verify_warm.as_ref().is_none_or(|(held,_,_,_)|held!=manifest) {
            let (actual,reader)=self.store()?.open_object_version_stream(&manifest.object_id,&manifest.version).map_err(error)?;
            if actual!=*manifest {return Err(error("candidate manifest changed before verification"));}
            self.verify_warm=Some((manifest.clone(),reader,blake3::Hasher::new(),0));
        }
        let (_,reader,hash,verified)=self.verify_warm.as_mut().unwrap();
        let mut buffer=[0;64*1024];
        while *verified<manifest.size_bytes && self.turn_bytes!=0 && std::time::Instant::now()<self.turn_deadline {
            let bound=(manifest.size_bytes-*verified).min(self.turn_bytes).min(buffer.len() as u64) as usize;
            let count=reader.read(&mut buffer[..bound]).map_err(error)?;
            if count==0 {return Err(error("candidate logical stream ended before its immutable length"));}
            hash.update(&buffer[..count]);*verified+=count as u64;self.turn_bytes-=count as u64;self.turn_advanced=true;
        }
        if *verified!=manifest.size_bytes {return Ok(false);}
        if format!("blake3:{}",hash.finalize().to_hex())!=manifest.content_hash {return Err(error("candidate manifest logical digest mismatch"));}
        self.verify_warm=None;Ok(true)
    }
    fn install_bounded(&mut self, source:&Path, hash:&str, size:u64)->Result<bool> {
        self.require_turn()?;
        let hex=hash_hex(hash)?;let destination=self.root.join("blobs").join(&hex[..2]).join(hex);
        if destination.try_exists().map_err(|e|io(&destination,e))? {
            if self.install_warm.as_ref().is_none_or(|(held,_)|held!=&destination) {
                self.install_warm=Some((destination.clone(),sinomo_dfs::ContentImportBlobVerifier::existing(&destination,hash.to_owned(),size).map_err(error)?));
            }
            let (_,verifier)=self.install_warm.as_mut().unwrap();let before=verifier.verified_bytes();
            let done=verifier.step(self.turn_bytes,self.turn_deadline,&std::sync::atomic::AtomicBool::new(false)).map_err(error)?;
            let advanced=verifier.verified_bytes()-before;self.turn_bytes-=advanced;self.turn_advanced|=advanced!=0;
            if !done {return Ok(false);}
            self.install_warm.take().unwrap().1.finish().map_err(error)?;
            return Ok(true);
        }
        durable_file::ensure_directory(&self.root.join("blobs")).map_err(error)?;
        durable_file::ensure_directory(destination.parent().unwrap()).map_err(error)?;
        match fs::hard_link(source,&destination) {
            Ok(())=>{},
            Err(failure) if failure.kind()==std::io::ErrorKind::AlreadyExists=>return Ok(false),
            Err(failure)=>return Err(io(&destination,failure)),
        }
        durable_file::sync_directory(destination.parent().unwrap()).map_err(error)?;
        durable_file::sync_directory(&self.root.join("blobs")).map_err(error)?;
        Ok(true)
    }

    fn advance(&mut self, budget: usize, max_bytes:u64, deadline:std::time::Instant) -> Result<DfsStoragePreparation> {
        // A failed checkpoint publication cannot leave a warm cursor ahead of
        // its acknowledged source. Byte continuations remain in their receipt.
        self.progress=read(&self.work.join("progress.sobj"))?;
        if self.progress.version!=1 {return Err(error("unsupported content conversion progress"));}
        self.turn_deadline=deadline;self.turn_bytes=max_bytes;self.turn_limit=max_bytes;self.turn_advanced=false;
        if std::time::Instant::now()>=deadline{return Ok(DfsStoragePreparation::Pending{processed_records:0});}
        if let Some(upgrade)=self.upgrade.as_mut() {
            if !upgrade.published() {
                let result=upgrade.advance(&self.root,&mut self.upgrade_driver,budget,max_bytes,deadline)?;
                if !matches!(result,DfsStoragePreparation::Ready) {return Ok(result);}
                if !matches!(self.progress.phase,Phase::Publish|Phase::Cleanup|Phase::Done|Phase::GcHints) {
                    self.store=Some(Arc::new(ContentStoreStagingWriter::open_candidate(&self.target).map_err(error)?));
                }
                return Ok(DfsStoragePreparation::Pending{processed_records:1});
            }
        }
        if matches!(self.progress.phase,Phase::Cleanup|Phase::Done) && source_format(&self.root)?==Some(3) {
            if self.progress.publication_step!=3 {return Err(error("legacy successor lacks its completed publication prefix"));}
            super::legacy_stage::record_published_v3(&self.root)?;
            return Ok(DfsStoragePreparation::Ready);
        }
        let mut processed = 0;
        while processed < budget && self.turn_bytes>0 && std::time::Instant::now()<self.turn_deadline {
            let outcome=(||->Result<bool>{Ok(match self.progress.phase {
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
                            let manifest: ContentManifest = self.source_record(&path)?;
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
                    let page=self.store()?.storage_staging_manifest_page(self.progress.verify_after.as_ref(),1).map_err(error)?;
                    if let Some(manifest)=page.into_iter().next() {
                        if !self.verify_manifest(&manifest)? {return Err(MetadataBackendError::StoragePreparationPending{root:self.root.clone(),processed_records:0});}
                        self.progress.verify_after=Some((manifest.object_id,manifest.version));true
                    }else{self.transition(Phase::Install)?;false}
                }
                Phase::Install => {
                    let source=self.target.join("blobs");
                    if self.walker.is_none(){self.walker=Some(Walker::new(&source)?);}
                    let next=if let Some(path)=&self.pending_install {Some(path.clone())}else{self.walker.as_mut().unwrap().next()?};
                    if let Some(path)=next {
                        self.pending_install=Some(path.clone());
                        if path.is_file(){
                            let hash=format!("blake3:{}",path.file_name().and_then(|name|name.to_str()).ok_or_else(||error("invalid target hash"))?);
                            let receipt=self.work.join("installed").join(format!("{}.sobj",hash_hex(&hash)?));
                            if !receipt.exists(){
                                let size=fs::metadata(&path).map_err(|e|io(&path,e))?.len();
                                if !self.install_bounded(&path,&hash,size)? {return Err(MetadataBackendError::StoragePreparationPending{root:self.root.clone(),processed_records:0});}
                                write(&receipt,&true)?;
                            }
                        }
                        self.pending_install=None;true
                    }else{self.store=None;self.progress.lane=0;self.transition(Phase::GcHints)?;false}
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
                    if source_format(&self.root)?==Some(3) {
                        super::legacy_stage::record_published_v3(&self.root)?;
                        return Ok(false);
                    }
                    self.release_source_leases()?;
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
                Phase::Done => return Ok(false),
            })})();
            let worked=match outcome {
                Ok(worked)=>worked,
                Err(MetadataBackendError::StoragePreparationPending{..})=>return Ok(DfsStoragePreparation::Pending{processed_records:(processed+usize::from(self.turn_advanced)).min(budget)}),
                Err(failure)=>return Err(failure),
            };
            if self.progress.phase==Phase::Done || (self.progress.phase==Phase::Publish && self.progress.publication_step==3 && source_format(&self.root)?==Some(3)) {return Ok(DfsStoragePreparation::Ready);}
            if worked {
                processed += 1;
                self.turn_advanced = false; // the completed record already accounts for its byte work
            }
            // Empty source lanes also advance the original cursor. Persist that
            // transition before yielding, even when it completed no record.
            self.save()?;
        }
        Ok(DfsStoragePreparation::Pending {
            processed_records: (processed+usize::from(self.turn_advanced && processed==0)).min(budget),
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
            self.require_turn()?;
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
            let bytes = self.source_bytes(&path)?;
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
                    if durable_file::read_object_file_bytes(&archive, bytes.len() as u64)
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
        if source_format(&self.root)?==Some(LocalContentStore::FORMAT_VERSION) {
            self.release_source_leases()?;
            let expected=super::legacy_stage::target_incarnation(&self.target)?;
            LocalContentStore::open_for_inspection(&self.root).certify_current_layout_durability(&expected).map_err(error)?;
            self.progress.publication_step=3;
            return self.save();
        }
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
        let bytes = durable_file::read_object_file_bytes(&self.target.join(MARKER), CONTROL_RECORD_LIMIT)
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
pub(super) fn hash_hex(hash: &str) -> Result<&str> {
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
