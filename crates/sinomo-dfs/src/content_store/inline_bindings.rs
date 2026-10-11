//! Authoritative small-content bindings, layered on shared storage frames.
//!
//! The published catalog owns every version and retention root. Small payloads
//! are framed inline; large and extent payloads use current content-addressed
//! files. The mandatory current marker rejects unsupported storage formats.

mod activation;
mod blob_stage;
mod compaction;
mod completion;
mod operations;
pub(super) use operations::open_verified_inline_payload;
#[cfg(feature = "storage-conversion")]
pub use operations::{ContentStoreStagingRow, ContentStoreStagingWriter};
mod read_snapshot;
mod root_identity;
#[cfg(test)]
mod tests;
pub(super) use read_snapshot::{ContentReadSnapshot, initialize_file_reclamation_lock};

use super::*;
use sinomo_core::storage::checksummed_frame::{
    FrameAppender, FrameLimits, FrameRead, TailRecovery, read_frame,
};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Weak;

pub(super) const INLINE_BINDING_LAYOUT: &str = "inline-bindings-v2";
pub(super) const INLINE_BINDING_DIR: &str = "inline-bindings";
const INLINE_FORMAT_VERSION: u32 = 2;
const INLINE_SEGMENT_MAX_BYTES: u64 = 4 * 1024 * 1024;
const INLINE_FRAME_HEADER_LIMIT: u32 = 64 * 1024;
const INLINE_OWNER_LOCK: &str = "content-owner.lock";
const INLINE_CURRENT: &str = "current.sobj";
const INLINE_CATALOG_MAX_BYTES: u64 = 64 * 1024 * 1024;

type VersionKey = (String, String);

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct InlineCatalog {
    format_version: u32,
    store_incarnation: String,
    generation: u64,
    /// The original file representation is an authoritative mixed baseline.
    /// Control records shadow exact identities; missing log keys do not.
    segments: Vec<InlineSegment>,
}

#[derive(Serialize, Deserialize)]
struct InlineCatalogEnvelope {
    catalog: InlineCatalog,
    checksum: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct InlineSegment {
    id: u64,
    sealed_end: Option<u64>,
    sealed_digest: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct InlineRecord {
    format_version: u32,
    store_incarnation: String,
    generation: u64,
    sequence: u64,
    operation: InlineOperation,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum InlineOperation {
    /// Compaction copies shared payloads once before restoring logical roots.
    Blob {
        content_hash: String,
        size_bytes: u64,
        published_at_unix_nanos: u64,
    },
    /// Bytes and an exact in-flight root, without a public version manifest.
    StageBlob {
        size_bytes: u64,
        published_at_unix_nanos: u64,
        reservation: ContentStoreReservationRecord,
    },
    Bind {
        manifest: ContentManifest,
        published_at_unix_nanos: u64,
        reservation: Option<ContentStoreReservationRecord>,
    },
    Link {
        manifest: ContentManifest,
        published_at_unix_nanos: u64,
        reservation: Option<ContentStoreReservationRecord>,
    },
    /// Restores a verified compaction snapshot. Unlike foreground Bind/Link,
    /// its reservation state is restored separately before pointer publication.
    RestoreVersion {
        manifest: ContentManifest,
        published_at_unix_nanos: u64,
    },
    Reserve(ContentStoreReservationRecord),
    Release(ContentStoreReservationIdentity),
    DeleteVersion {
        object_id: String,
        version: String,
        content_hash: String,
    },
    ExternalBinding {
        manifest: ContentManifest,
        published_at_unix_nanos: u64,
        reservation: Option<ContentStoreReservationRecord>,
    },
    RemoveBlob {
        content_hash: String,
    },
}

#[derive(Clone, Debug)]
pub(super) struct InlineVersionEntry {
    pub(super) manifest: ContentManifest,
    pub(super) published_at_unix_nanos: u64,
}

#[derive(Clone, Debug)]
pub(super) enum InlineVersionState {
    Live(InlineVersionEntry),
    Deleted { content_hash: String },
}

#[derive(Clone, Debug)]
pub(super) enum InlineReservationState {
    Live(ContentStoreReservationRecord),
    Released,
}

#[derive(Clone, Debug)]
pub(super) struct InlineBlobEntry {
    pub(super) size_bytes: u64,
    pub(super) published_at_unix_nanos: u64,
    path: PathBuf,
    payload_offset: u64,
    generation: u64,
}

impl InlineBlobEntry {
    pub(super) fn readable_size(&self) -> Result<Option<u64>> {
        let metadata = match fs::metadata(&self.path) {
            Ok(metadata) => metadata,
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(source) => {
                return Err(ContentStoreError::Io {
                    path: self.path.clone(),
                    source,
                });
            }
        };
        Ok((metadata.is_file()
            && self
                .payload_offset
                .checked_add(self.size_bytes)
                .is_some_and(|end| end <= metadata.len()))
        .then_some(self.size_bytes))
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct InlineBindingView {
    pub(super) versions: BTreeMap<VersionKey, InlineVersionState>,
    content_versions: BTreeMap<(String, String, u64), BTreeSet<String>>,
    pub(super) reservations: BTreeMap<ContentStoreReservationIdentity, InlineReservationState>,
    pub(super) blobs: BTreeMap<String, InlineBlobEntry>,
    pub(super) deleted_blobs: BTreeSet<String>,
    sequence: u64,
}

impl InlineBindingView {
    pub(super) fn live_manifest(&self, object_id: &str, version: &str) -> Option<&ContentManifest> {
        match self
            .versions
            .get(&(object_id.to_owned(), version.to_owned()))
        {
            Some(InlineVersionState::Live(entry)) => Some(&entry.manifest),
            Some(InlineVersionState::Deleted { .. }) | None => None,
        }
    }
}

#[derive(Debug, Default)]
struct InlineResidentState {
    catalog: Option<InlineCatalog>,
    view: InlineBindingView,
    active_end: u64,
    writer: Option<FrameAppender>,
    uncertain: bool,
    certified_active_end: u64,
    certified_catalog: Option<InlineCatalog>,
    active_identity: Option<InlineFileIdentity>,
    certified_identity: Option<InlineFileIdentity>,
    retired_generation_cursor: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct InlineFileIdentity {
    length: u64,
    modified: Option<SystemTime>,
    created: Option<SystemTime>,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    changed: (i64, i64),
}

impl InlineFileIdentity {
    fn read(path: &Path) -> Result<Self> {
        let metadata = fs::metadata(path).map_err(|source| ContentStoreError::Io {
            path: path.to_owned(),
            source,
        })?;
        if !metadata.is_file() {
            return Err(inline_error("active segment is not a regular file"));
        }
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt;
        Ok(Self {
            length: metadata.len(),
            modified: metadata.modified().ok(),
            created: metadata.created().ok(),
            #[cfg(unix)]
            device: metadata.dev(),
            #[cfg(unix)]
            inode: metadata.ino(),
            #[cfg(unix)]
            changed: (metadata.ctime(), metadata.ctime_nsec()),
        })
    }
    fn same_file(&self, other: &Self) -> bool {
        #[cfg(unix)]
        {
            self.device == other.device && self.inode == other.inode
        }
        #[cfg(not(unix))]
        {
            self.created == other.created
        }
    }
}

#[derive(Debug)]
struct InlineStoreOwner {
    root: PathBuf,
    handles: Mutex<Weak<()>>,
    state: Mutex<InlineResidentState>,
}

static INLINE_STORES: OnceLock<Mutex<BTreeMap<PathBuf, Weak<InlineStoreOwner>>>> = OnceLock::new();

/// Retained by store handles and every reader. Cold
/// activation must see only its own lease after exclusive daemon ownership.
#[derive(Clone, Debug)]
pub(super) struct InlineStoreLease {
    owner: Arc<InlineStoreOwner>,
    handle: Arc<()>,
    identity_error: Option<Arc<str>>,
}

impl InlineStoreLease {
    pub(super) fn root(&self) -> &Path {
        &self.owner.root
    }
    pub(super) fn new(root: &Path) -> Self {
        let (key, identity_error) = match root_identity::normalized_root_key(root) {
            Ok(key) => (key, None),
            Err(error) => (
                if root.is_absolute() {
                    root.to_owned()
                } else {
                    std::env::current_dir()
                        .unwrap_or_else(|_| PathBuf::from("."))
                        .join(root)
                },
                Some(Arc::<str>::from(error.to_string())),
            ),
        };
        let mut stores = INLINE_STORES
            .get_or_init(|| Mutex::new(BTreeMap::new()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let owner = stores.get(&key).and_then(Weak::upgrade).unwrap_or_else(|| {
            let owner = Arc::new(InlineStoreOwner {
                root: key.clone(),
                handles: Mutex::new(Weak::new()),
                state: Mutex::new(InlineResidentState::default()),
            });
            stores.insert(key, Arc::downgrade(&owner));
            owner
        });
        drop(stores);
        let mut handle_slot = owner
            .handles
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let handle = handle_slot.upgrade().unwrap_or_else(|| {
            let handle = Arc::new(());
            *handle_slot = Arc::downgrade(&handle);
            handle
        });
        drop(handle_slot);
        Self {
            owner,
            handle,
            identity_error,
        }
    }
}

thread_local! {
    static CONTENT_OWNER_LOCKS: RefCell<BTreeMap<PathBuf, (usize, Arc<fs::File>)>> = const { RefCell::new(BTreeMap::new()) };
}

/// Synchronous reentrancy is needed because mutation methods call reservation and
/// accounting helpers internally. The marker makes this guard !Send: a lock
/// depth cannot migrate to another worker. All active-layout mutation entry
/// points take this owner before the reservation flock or the stats lock.
pub(super) struct ContentMutationGuard {
    path: Option<PathBuf>,
    _thread_bound: std::marker::PhantomData<Rc<()>>,
}

impl ContentMutationGuard {
    fn inactive() -> Self {
        Self {
            path: None,
            _thread_bound: std::marker::PhantomData,
        }
    }

    pub(super) fn acquire(root: &Path, create: bool) -> Result<Self> {
        let canonical = root_identity::canonical_existing_root(root).map_err(|source| {
            ContentStoreError::Io {
                path: root.to_owned(),
                source,
            }
        })?;
        let path = canonical.join(INLINE_OWNER_LOCK);
        let reused = CONTENT_OWNER_LOCKS.with(|locks| {
            let mut locks = locks.borrow_mut();
            if let Some((depth, _)) = locks.get_mut(&path) {
                *depth += 1;
                true
            } else {
                false
            }
        });
        if reused {
            return Ok(Self {
                path: Some(path),
                _thread_bound: std::marker::PhantomData,
            });
        }
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(create)
            .truncate(false)
            .open(&path)
            .map_err(|source| ContentStoreError::Io {
                path: path.clone(),
                source,
            })?;
        FileExt::lock_exclusive(&file).map_err(|source| ContentStoreError::Io {
            path: path.clone(),
            source,
        })?;
        if create {
            sync_dir(root)?;
        }
        CONTENT_OWNER_LOCKS.with(|locks| {
            locks.borrow_mut().insert(path.clone(), (1, Arc::new(file)));
        });
        Ok(Self {
            path: Some(path),
            _thread_bound: std::marker::PhantomData,
        })
    }
}

impl Drop for ContentMutationGuard {
    fn drop(&mut self) {
        let Some(path) = self.path.as_ref() else {
            return;
        };
        CONTENT_OWNER_LOCKS.with(|locks| {
            let mut locks = locks.borrow_mut();
            if let Some((depth, _)) = locks.get_mut(path) {
                *depth -= 1;
                if *depth == 0
                    && let Some((_, file)) = locks.remove(path)
                {
                    let _ = FileExt::unlock(file.as_ref());
                }
            }
        });
    }
}

#[derive(Debug)]
pub(super) struct InlineReaderLease {
    _store: InlineStoreLease,
    generation_lock: fs::File,
}

impl Drop for InlineReaderLease {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.generation_lock);
    }
}

fn inline_error(detail: impl std::fmt::Display) -> ContentStoreError {
    ContentStoreError::InvalidContentLayout {
        message: format!("inline content bindings: {detail}"),
    }
}

fn frame_limits() -> FrameLimits {
    FrameLimits::new(INLINE_FRAME_HEADER_LIMIT, INLINE_BLOB_SMALL_LIMIT_BYTES)
        .expect("fixed inline frame limits")
}

fn generation_root(root: &Path, generation: u64) -> PathBuf {
    root.join(INLINE_BINDING_DIR)
        .join("generations")
        .join(format!("{generation:016x}"))
}

fn segment_path(root: &Path, generation: u64, segment: u64) -> PathBuf {
    generation_root(root, generation).join(format!("{segment:016x}.slog"))
}

fn catalog_path(root: &Path) -> PathBuf {
    root.join(INLINE_BINDING_DIR).join(INLINE_CURRENT)
}

fn encode_inline<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    sinomo_object_serde::to_bytes(value, INLINE_FORMAT_VERSION as u16, LengthMode::U32)
        .map_err(|error| ContentStoreError::Serde(error.to_string()))
}

fn read_catalog(root: &Path, incarnation: &str) -> Result<InlineCatalog> {
    let bytes = durable_file::read_object_file_bytes(&catalog_path(root), INLINE_CATALOG_MAX_BYTES)
        .map_err(content_durable_file_error)?;
    let envelope: InlineCatalogEnvelope = sinomo_object_serde::from_bytes(&bytes)
        .map_err(|error| ContentStoreError::Serde(error.to_string()))?;
    let catalog = envelope.catalog;
    if blake3::hash(&encode_inline(&catalog)?).to_hex().as_str() != envelope.checksum {
        return Err(inline_error("generation catalog checksum mismatch"));
    }
    if catalog.format_version != INLINE_FORMAT_VERSION
        || catalog.store_incarnation != incarnation
        || catalog.generation == 0
        || catalog.segments.is_empty()
        || catalog
            .segments
            .last()
            .is_some_and(|segment| segment.sealed_end.is_some() || segment.sealed_digest.is_some())
    {
        return Err(inline_error("unsupported or mismatched generation catalog"));
    }
    let mut previous = None;
    for (index, segment) in catalog.segments.iter().enumerate() {
        if previous.is_some_and(|id| segment.id <= id)
            || (index + 1 != catalog.segments.len()
                && (segment.sealed_end.is_none() || segment.sealed_digest.is_none()))
        {
            return Err(inline_error("invalid segment ordering/seal"));
        }
        previous = Some(segment.id);
    }
    Ok(catalog)
}

fn encode_catalog(catalog: &InlineCatalog) -> Result<Vec<u8>> {
    let checksum = blake3::hash(&encode_inline(catalog)?).to_hex().to_string();
    let bytes = encode_inline(&InlineCatalogEnvelope {
        catalog: catalog.clone(),
        checksum,
    })?;
    if bytes.len() as u64 > INLINE_CATALOG_MAX_BYTES {
        return Err(inline_error("generation catalog exceeds resource bound"));
    }
    Ok(bytes)
}

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum InlineFault {
    CatalogParentSync,
    CatalogCertification,
    LostAppendCompletion,
}
#[cfg(test)]
thread_local! { static INLINE_FAULT: RefCell<Option<(PathBuf, InlineFault)>> = const { RefCell::new(None) }; }
#[cfg(test)]
fn take_inline_fault(root: &Path, point: InlineFault) -> bool {
    INLINE_FAULT.with(|fault| {
        let mut fault = fault.borrow_mut();
        if fault
            .as_ref()
            .is_some_and(|(path, armed)| path == root && *armed == point)
        {
            fault.take();
            true
        } else {
            false
        }
    })
}
fn publish_catalog(root: &Path, catalog: &InlineCatalog) -> Result<()> {
    let bytes = encode_catalog(catalog)?;
    #[cfg(test)]
    if take_inline_fault(root, InlineFault::CatalogParentSync) {
        // Deterministic failure after the synchronized replacement has been
        // renamed, before its parent barrier. This is not a production path.
        let temporary = root.join(INLINE_BINDING_DIR).join("catalog-fault.tmp");
        fs::write(&temporary, &bytes).map_err(|source| ContentStoreError::Io {
            path: temporary.clone(),
            source,
        })?;
        fs::File::open(&temporary)
            .and_then(|file| file.sync_all())
            .map_err(|source| ContentStoreError::Io {
                path: temporary.clone(),
                source,
            })?;
        fs::rename(&temporary, catalog_path(root)).map_err(|source| ContentStoreError::Io {
            path: temporary,
            source,
        })?;
        return Err(inline_error("injected catalog parent sync failure"));
    }
    write_file_atomic(&catalog_path(root), &bytes)
}

fn certify_catalog(root: &Path, expected: &InlineCatalog) -> Result<()> {
    #[cfg(test)]
    if take_inline_fault(root, InlineFault::CatalogCertification) {
        return Err(inline_error("injected catalog recertification failure"));
    }
    let path = catalog_path(root);
    if read_catalog(root, &expected.store_incarnation)? != *expected {
        return Err(inline_error(
            "catalog changed before durability certification",
        ));
    }
    let mut file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .map_err(|source| ContentStoreError::Io {
            path: path.clone(),
            source,
        })?;
    durable_file::sync_open_file(&mut file, &path, durable_file::DurableFileSync::All)
        .map_err(content_durable_file_error)?;
    // Re-establish the full owned namespace chain after any interrupted mkdir
    // or pointer publication. Existence alone is not directory durability.
    sync_dir(&generation_root(root, expected.generation))?;
    sync_dir(&root.join(INLINE_BINDING_DIR).join("generations"))?;
    sync_dir(&root.join(INLINE_BINDING_DIR))?;
    sync_dir(root)?;
    if let Some(parent) = root.parent() {
        sync_dir(parent)?;
    }
    if read_catalog(root, &expected.store_incarnation)? != *expected {
        return Err(inline_error(
            "catalog changed during durability certification",
        ));
    }
    Ok(())
}

fn validate_key(object_id: &str, version: &str) -> Result<()> {
    if [object_id, version].into_iter().any(|part| {
        part.is_empty() || part == "." || part == ".." || part.contains(['/', '\\', '\0'])
    }) {
        return Err(inline_error("invalid object/version path component"));
    }
    Ok(())
}

fn version_hash(state: &InlineVersionState) -> &str {
    match state {
        InlineVersionState::Live(entry) => &entry.manifest.content_hash,
        InlineVersionState::Deleted { content_hash } => content_hash,
    }
}

fn validate_tuple(view: &InlineBindingView, manifest: &ContentManifest) -> Result<()> {
    validate_key(&manifest.object_id, &manifest.version)?;
    hash_hex(&manifest.content_hash)?;
    validate_and_order_stream_manifest(manifest.clone())?;
    if let Some(prior) = view
        .versions
        .get(&(manifest.object_id.clone(), manifest.version.clone()))
    {
        let existing_hash = version_hash(prior);
        if existing_hash != manifest.content_hash {
            return Err(ContentStoreError::VersionHashConflict {
                object_id: manifest.object_id.clone(),
                version: manifest.version.clone(),
                existing_hash: existing_hash.to_owned(),
                new_hash: manifest.content_hash.clone(),
            });
        }
        if let InlineVersionState::Live(entry) = prior
            && entry.manifest != *manifest
        {
            return Err(inline_error(
                "existing exact tuple has a different manifest",
            ));
        }
    }
    Ok(())
}

fn validate_inline_reservation(
    record: &ContentStoreReservationRecord,
    incarnation: &str,
) -> Result<()> {
    validate_key(&record.identity.object_id, &record.identity.version)?;
    hash_hex(&record.identity.content_hash)?;
    if record.causal_stamp.store_incarnation != incarnation {
        return Err(inline_error(
            "inline reservation lacks exact store provenance",
        ));
    }
    Ok(())
}

fn validate_operation(
    view: &InlineBindingView,
    operation: &InlineOperation,
    payload_hash: &str,
    payload_bytes: u64,
    incarnation: &str,
) -> Result<()> {
    match operation {
        InlineOperation::Blob {
            content_hash,
            size_bytes,
            ..
        } => {
            hash_hex(content_hash)?;
            if content_hash != payload_hash
                || *size_bytes != payload_bytes
                || *size_bytes > INLINE_BLOB_SMALL_LIMIT_BYTES
            {
                return Err(inline_error(
                    "inline payload does not match its declared hash/size",
                ));
            }
        }
        InlineOperation::StageBlob {
            size_bytes,
            reservation,
            ..
        } => {
            validate_inline_reservation(reservation, incarnation)?;
            if reservation.identity.content_hash != payload_hash
                || *size_bytes != payload_bytes
                || *size_bytes > INLINE_BLOB_SMALL_LIMIT_BYTES
                || *size_bytes == 0
            {
                return Err(inline_error(
                    "blob-only stage payload does not match its exact reservation",
                ));
            }
            if view
                .versions
                .get(&(
                    reservation.identity.object_id.clone(),
                    reservation.identity.version.clone(),
                ))
                .is_some_and(|prior| version_hash(prior) != reservation.identity.content_hash)
            {
                return Err(inline_error(
                    "blob-only stage conflicts with a published or retired tuple",
                ));
            }
        }
        InlineOperation::Bind {
            manifest,
            reservation,
            published_at_unix_nanos,
        }
        | InlineOperation::Link {
            manifest,
            reservation,
            published_at_unix_nanos,
        }
        | InlineOperation::ExternalBinding {
            manifest,
            reservation,
            published_at_unix_nanos,
        } => {
            validate_tuple(view, manifest)?;
            if let Some(InlineVersionState::Live(prior)) = view
                .versions
                .get(&(manifest.object_id.clone(), manifest.version.clone()))
                && prior.published_at_unix_nanos != *published_at_unix_nanos
            {
                return Err(inline_error(
                    "retry changed the durable version publication time",
                ));
            }
            match operation {
                InlineOperation::Bind { .. }
                    if manifest.extents.is_empty()
                        && manifest.size_bytes <= INLINE_BLOB_SMALL_LIMIT_BYTES
                        && manifest.content_hash == payload_hash
                        && manifest.size_bytes == payload_bytes => {}
                InlineOperation::Link { .. }
                    if payload_bytes == 0
                        && view
                            .blobs
                            .get(&manifest.content_hash)
                            .is_some_and(|entry| entry.size_bytes == manifest.size_bytes) => {}
                InlineOperation::ExternalBinding { .. } if payload_bytes == 0 => {}
                _ => return Err(inline_error("binding payload/reference is not exact")),
            }
            if manifest.content_hash == intrinsic_empty_content_hash() {
                if reservation.is_some() {
                    return Err(inline_error("intrinsic empty content has a reservation"));
                }
            } else {
                let reservation = reservation
                    .as_ref()
                    .ok_or_else(|| inline_error("binding has no durable reservation"))?;
                validate_inline_reservation(reservation, incarnation)?;
                if reservation.identity.object_id != manifest.object_id
                    || reservation.identity.version != manifest.version
                    || reservation.identity.content_hash != manifest.content_hash
                {
                    return Err(inline_error("binding reservation does not match manifest"));
                }
            }
        }
        InlineOperation::RestoreVersion { manifest, .. } => {
            validate_tuple(view, manifest)?;
            if payload_bytes != 0 {
                return Err(inline_error("compacted manifest has payload"));
            }
        }
        InlineOperation::Reserve(record) => {
            if payload_bytes != 0 {
                return Err(inline_error("control frame has payload"));
            }
            validate_inline_reservation(record, incarnation)?;
        }
        InlineOperation::Release(identity) => {
            if payload_bytes != 0 {
                return Err(inline_error("control frame has payload"));
            }
            validate_key(&identity.object_id, &identity.version)?;
            hash_hex(&identity.content_hash)?;
        }
        InlineOperation::DeleteVersion {
            object_id,
            version,
            content_hash,
        } => {
            if payload_bytes != 0 {
                return Err(inline_error("control frame has payload"));
            }
            validate_key(object_id, version)?;
            hash_hex(content_hash)?;
            if view
                .versions
                .get(&(object_id.clone(), version.clone()))
                .is_some_and(|prior| version_hash(prior) != content_hash)
            {
                return Err(inline_error(
                    "control frame changed an immutable tuple identity",
                ));
            }
        }
        InlineOperation::RemoveBlob { content_hash } => {
            if payload_bytes != 0 {
                return Err(inline_error("control frame has payload"));
            }
            hash_hex(content_hash)?;
            if view.versions.values().any(|entry| matches!(entry, InlineVersionState::Live(live) if live.manifest.content_hash == *content_hash))
                || view.reservations.values().any(|entry| matches!(entry, InlineReservationState::Live(live) if live.identity.content_hash == *content_hash))
            { return Err(inline_error("attempt to remove an inline payload with live roots")); }
        }
    }
    Ok(())
}

fn apply_operation(
    view: &mut InlineBindingView,
    operation: InlineOperation,
    location: InlineBlobEntry,
    sequence: u64,
) {
    let affected_version = match &operation {
        InlineOperation::Bind { manifest, .. }
        | InlineOperation::Link { manifest, .. }
        | InlineOperation::ExternalBinding { manifest, .. }
        | InlineOperation::RestoreVersion { manifest, .. } => {
            Some((manifest.object_id.clone(), manifest.version.clone()))
        }
        InlineOperation::DeleteVersion {
            object_id, version, ..
        } => Some((object_id.clone(), version.clone())),
        _ => None,
    };
    if let Some(key) = affected_version.as_ref()
        && let Some(InlineVersionState::Live(prior)) = view.versions.get(key)
    {
        let index_key = (
            prior.manifest.object_id.clone(),
            prior.manifest.content_hash.clone(),
            prior.manifest.size_bytes,
        );
        if let Some(versions) = view.content_versions.get_mut(&index_key) {
            versions.remove(&prior.manifest.version);
            if versions.is_empty() {
                view.content_versions.remove(&index_key);
            }
        }
    }
    match operation {
        InlineOperation::Blob { content_hash, .. } => {
            view.deleted_blobs.remove(&content_hash);
            view.blobs.insert(content_hash, location);
        }
        InlineOperation::StageBlob { reservation, .. } => {
            let hash = reservation.identity.content_hash.clone();
            view.deleted_blobs.remove(&hash);
            view.blobs.insert(hash, location);
            view.reservations.insert(
                reservation.identity.clone(),
                InlineReservationState::Live(reservation),
            );
        }
        InlineOperation::Bind {
            manifest,
            published_at_unix_nanos,
            reservation,
        } => {
            view.deleted_blobs.remove(&manifest.content_hash);
            if manifest.content_hash != intrinsic_empty_content_hash() {
                view.blobs.insert(manifest.content_hash.clone(), location);
            }
            let key = (manifest.object_id.clone(), manifest.version.clone());
            view.versions.insert(
                key,
                InlineVersionState::Live(InlineVersionEntry {
                    manifest,
                    published_at_unix_nanos,
                }),
            );
            if let Some(record) = reservation {
                view.reservations.insert(
                    record.identity.clone(),
                    InlineReservationState::Live(record),
                );
            }
        }
        InlineOperation::Link {
            manifest,
            published_at_unix_nanos,
            reservation,
        }
        | InlineOperation::ExternalBinding {
            manifest,
            published_at_unix_nanos,
            reservation,
        } => {
            let key = (manifest.object_id.clone(), manifest.version.clone());
            view.versions.insert(
                key,
                InlineVersionState::Live(InlineVersionEntry {
                    manifest,
                    published_at_unix_nanos,
                }),
            );
            if let Some(record) = reservation {
                view.reservations.insert(
                    record.identity.clone(),
                    InlineReservationState::Live(record),
                );
            }
        }
        InlineOperation::RestoreVersion {
            manifest,
            published_at_unix_nanos,
        } => {
            view.versions.insert(
                (manifest.object_id.clone(), manifest.version.clone()),
                InlineVersionState::Live(InlineVersionEntry {
                    manifest,
                    published_at_unix_nanos,
                }),
            );
        }
        InlineOperation::Reserve(record) => {
            view.reservations.insert(
                record.identity.clone(),
                InlineReservationState::Live(record),
            );
        }
        InlineOperation::Release(identity) => {
            view.reservations
                .insert(identity, InlineReservationState::Released);
        }
        InlineOperation::DeleteVersion {
            object_id,
            version,
            content_hash,
        } => {
            view.versions.insert(
                (object_id, version),
                InlineVersionState::Deleted { content_hash },
            );
        }
        InlineOperation::RemoveBlob { content_hash } => {
            view.blobs.remove(&content_hash);
            view.deleted_blobs.insert(content_hash);
        }
    }
    if let Some(key) = affected_version
        && let Some(InlineVersionState::Live(entry)) = view.versions.get(&key)
    {
        view.content_versions
            .entry((
                entry.manifest.object_id.clone(),
                entry.manifest.content_hash.clone(),
                entry.manifest.size_bytes,
            ))
            .or_default()
            .insert(entry.manifest.version.clone());
    }
    view.sequence = sequence;
}

fn replay_catalog(root: &Path, catalog: &InlineCatalog) -> Result<(InlineBindingView, u64)> {
    let mut view = InlineBindingView::default();
    let mut active_end = 0;
    for (index, segment) in catalog.segments.iter().enumerate() {
        let path = segment_path(root, catalog.generation, segment.id);
        let mut file = fs::File::open(&path).map_err(|source| ContentStoreError::Io {
            path: path.clone(),
            source,
        })?;
        let sealed = index + 1 != catalog.segments.len();
        loop {
            match read_frame(&mut file, &path, frame_limits()).map_err(inline_error)? {
                FrameRead::Complete(frame) => {
                    let record: InlineRecord = sinomo_object_serde::from_bytes(frame.header())
                        .map_err(|error| ContentStoreError::Serde(error.to_string()))?;
                    if record.format_version != INLINE_FORMAT_VERSION
                        || record.store_incarnation != catalog.store_incarnation
                        || record.generation != catalog.generation
                        || record.sequence
                            != view
                                .sequence
                                .checked_add(1)
                                .ok_or_else(|| inline_error("record sequence overflow"))?
                    {
                        return Err(inline_error("record generation/sequence mismatch"));
                    }
                    let payload_hash = format!(
                        "{HASH_PREFIX}{}",
                        blake3::Hash::from_bytes(*frame.payload_checksum()).to_hex()
                    );
                    validate_operation(
                        &view,
                        &record.operation,
                        &payload_hash,
                        frame.payload_bytes(),
                        &catalog.store_incarnation,
                    )?;
                    let published = match &record.operation {
                        InlineOperation::Blob {
                            published_at_unix_nanos,
                            ..
                        }
                        | InlineOperation::Bind {
                            published_at_unix_nanos,
                            ..
                        }
                        | InlineOperation::StageBlob {
                            published_at_unix_nanos,
                            ..
                        } => *published_at_unix_nanos,
                        _ => 0,
                    };
                    let location = InlineBlobEntry {
                        size_bytes: frame.payload_bytes(),
                        published_at_unix_nanos: published,
                        path: path.clone(),
                        payload_offset: frame.payload_offset(),
                        generation: catalog.generation,
                    };
                    apply_operation(&mut view, record.operation, location, record.sequence);
                }
                FrameRead::CleanEof { offset } => {
                    active_end = offset;
                    break;
                }
                FrameRead::Incomplete(tail) if !sealed => {
                    active_end = tail.frame_start;
                    break;
                }
                FrameRead::Incomplete(_) => {
                    return Err(inline_error("sealed segment has an incomplete frame"));
                }
                FrameRead::Corrupt(frame) => {
                    return Err(inline_error(format!(
                        "corrupt frame at {}: {}",
                        frame.frame_start, frame.reason
                    )));
                }
            }
        }
        if sealed {
            let (hash, bytes) = durable_file::file_blake3_hash_and_size(&path)
                .map_err(content_durable_file_error)?;
            if segment.sealed_end != Some(active_end)
                || bytes != active_end
                || segment.sealed_digest.as_deref() != Some(hash.to_hex().as_str())
            {
                return Err(inline_error(
                    "sealed segment does not match its durable catalog",
                ));
            }
        }
    }
    Ok((view, active_end))
}

fn refresh_resident(
    root: &Path,
    incarnation: &str,
    state: &mut InlineResidentState,
    force: bool,
) -> Result<()> {
    let catalog = read_catalog(root, incarnation)?;
    let active = catalog.segments.last().expect("validated nonempty catalog");
    let path = segment_path(root, catalog.generation, active.id);
    let identity = InlineFileIdentity::read(&path)?;
    let physical_end = identity.length;
    if state.certified_catalog.as_ref() == Some(&catalog)
        && state
            .certified_identity
            .as_ref()
            .is_some_and(|old| !old.same_file(&identity))
    {
        state.writer = None;
        state.catalog = None;
        state.view = InlineBindingView::default();
        state.uncertain = true;
        return Err(inline_error(
            "active segment pathname replaced within a certified generation",
        ));
    }
    if !force
        && state.catalog.as_ref() == Some(&catalog)
        && physical_end == state.active_end
        && state.active_identity.as_ref() == Some(&identity)
    {
        return Ok(());
    }
    let same_certified_generation = state.certified_catalog.as_ref() == Some(&catalog);
    let reusable_certification = same_certified_generation
        && !state.uncertain
        && state.active_identity.as_ref() == Some(&identity)
        && state.certified_active_end == physical_end;
    let previous_writer = state.writer.take();
    // A rejected replay must not leave a same-length cache eligible for the
    // next ordinary lookup. Keep independent certified minima for recovery,
    // but discard the logical projection before attempting any new adoption.
    state.catalog = None;
    state.view = InlineBindingView::default();
    state.active_end = 0;
    state.uncertain = true;
    let (view, end) = replay_catalog(root, &catalog)?;
    if InlineFileIdentity::read(&path)? != identity {
        return Err(inline_error("active segment changed during replay"));
    }
    state.catalog = Some(catalog);
    state.view = view;
    state.active_end = end;
    state.active_identity = Some(identity);
    if !same_certified_generation {
        state.certified_active_end = 0;
    }
    // A successful explicit scrub of the identical owner-certified file need
    // not throw away its existing durability capability. Any changed identity,
    // end, catalog, uncertain append or failed replay discards that capability.
    // Fresh owners still only have readable bytes and must synchronize them.
    state.writer = if reusable_certification && end == state.certified_active_end {
        previous_writer
    } else {
        None
    };
    state.uncertain = state.writer.is_none();
    Ok(())
}

fn ensure_writer(root: &Path, incarnation: &str, state: &mut InlineResidentState) -> Result<()> {
    refresh_resident(root, incarnation, state, false)?;
    if state.writer.is_some() && !state.uncertain {
        return Ok(());
    }
    let catalog = state.catalog.as_ref().expect("loaded catalog").clone();
    let active = catalog.segments.last().expect("active segment");
    let path = segment_path(root, catalog.generation, active.id);
    // Normal fsync crash model: acknowledged bytes survive; a crash-torn active
    // suffix is unacknowledged. Keep resident certified minima and catalog seals.
    // Arbitrary post-ACK media rollback is outside tail-recovery guarantees.
    // Fresh owners must resync observed complete frames before certifying them.
    let writer = FrameAppender::open_with_recovery(
        &path,
        frame_limits(),
        TailRecovery::TruncateIncomplete {
            minimum_durable_end: state.certified_active_end,
        },
    )
    .map_err(inline_error)?;
    // A previous owner may have renamed current.sobj but failed its parent
    // sync. A readable pointer is not proof that a crash will retain it.
    certify_catalog(root, &catalog)?;
    let (view, end) = replay_catalog(root, &catalog)?;
    if writer.durable_prefix().end() != end {
        return Err(inline_error("replay changed after required content sync"));
    }
    state.view = view;
    state.active_end = end;
    state.certified_active_end = end;
    state.certified_catalog = Some(catalog);
    let identity = InlineFileIdentity::read(&path)?;
    state.active_identity = Some(identity.clone());
    state.certified_identity = Some(identity);
    state.writer = Some(writer);
    state.uncertain = false;
    Ok(())
}

fn append_operation(
    root: &Path,
    sentinel: &ContentStoreReservationBatchSentinel,
    state: &mut InlineResidentState,
    operation: InlineOperation,
    payload: &[u8],
) -> Result<()> {
    ensure_writer(root, &sentinel.store_incarnation, state)?;
    validate_operation(
        &state.view,
        &operation,
        &content_hash(payload),
        payload.len() as u64,
        &sentinel.store_incarnation,
    )?;
    let mut catalog = state.catalog.as_ref().expect("loaded catalog").clone();
    let sequence = state
        .view
        .sequence
        .checked_add(1)
        .ok_or_else(|| inline_error("record sequence exhausted"))?;
    let record = InlineRecord {
        format_version: INLINE_FORMAT_VERSION,
        store_incarnation: sentinel.store_incarnation.clone(),
        generation: catalog.generation,
        sequence,
        operation,
    };
    let bytes = encode_inline(&record)?;
    if bytes.len() > INLINE_FRAME_HEADER_LIMIT as usize {
        return Err(inline_error(
            "binding header exceeds the bounded frame limit",
        ));
    }
    let frame_bytes = sinomo_core::storage::checksummed_frame::FRAME_OVERHEAD_BYTES
        + bytes.len() as u64
        + payload.len() as u64;
    if state.active_end != 0
        && state.active_end.saturating_add(frame_bytes) > INLINE_SEGMENT_MAX_BYTES
    {
        let active = catalog.segments.last_mut().expect("active segment");
        let old_path = segment_path(root, catalog.generation, active.id);
        let (hash, length) = durable_file::file_blake3_hash_and_size(&old_path)
            .map_err(content_durable_file_error)?;
        if length != state.active_end {
            return Err(inline_error("active segment changed before rotation"));
        }
        active.sealed_end = Some(length);
        active.sealed_digest = Some(hash.to_hex().to_string());
        let next_id = active
            .id
            .checked_add(1)
            .ok_or_else(|| inline_error("segment identity exhausted"))?;
        let new_path = segment_path(root, catalog.generation, next_id);
        let writer = FrameAppender::open(&new_path, frame_limits()).map_err(inline_error)?;
        if writer.durable_prefix().end() != 0 {
            return Err(inline_error("unpublished rotation segment is not empty"));
        }
        catalog.segments.push(InlineSegment {
            id: next_id,
            sealed_end: None,
            sealed_digest: None,
        });
        state.uncertain = true;
        publish_catalog(root, &catalog)?;
        state.writer = Some(writer);
        state.catalog = Some(catalog.clone());
        state.active_end = 0;
        state.certified_active_end = 0;
        state.certified_catalog = Some(catalog.clone());
        let identity = InlineFileIdentity::read(&new_path)?;
        state.active_identity = Some(identity.clone());
        state.certified_identity = Some(identity);
    }
    let active = catalog.segments.last().expect("active segment");
    let path = segment_path(root, catalog.generation, active.id);
    state.uncertain = true;
    let token = state
        .writer
        .as_mut()
        .expect("certified writer")
        .append_bytes(&bytes, payload)
        .map_err(inline_error)?;
    #[cfg(test)]
    if take_inline_fault(root, InlineFault::LostAppendCompletion) {
        // The durable bytes survived but the domain owner did not receive the
        // completion. Preserve uncertainty until a later owner recertifies.
        // Actual write/sync failures are injected by the shared R0 tests.
        return Err(inline_error("injected lost append completion"));
    }
    let identity = InlineFileIdentity::read(&path)?;
    if state
        .certified_identity
        .as_ref()
        .is_some_and(|old| !old.same_file(&identity))
    {
        return Err(inline_error(
            "active segment replaced during a synchronized append",
        ));
    }
    let published = match &record.operation {
        InlineOperation::Blob {
            published_at_unix_nanos,
            ..
        }
        | InlineOperation::Bind {
            published_at_unix_nanos,
            ..
        }
        | InlineOperation::StageBlob {
            published_at_unix_nanos,
            ..
        } => *published_at_unix_nanos,
        _ => 0,
    };
    let location = InlineBlobEntry {
        size_bytes: payload.len() as u64,
        published_at_unix_nanos: published,
        path,
        payload_offset: token.frame().payload_offset(),
        generation: catalog.generation,
    };
    apply_operation(&mut state.view, record.operation, location, sequence);
    state.active_end = token.prefix().end();
    state.certified_active_end = state.active_end;
    state.active_identity = Some(identity.clone());
    state.certified_identity = Some(identity);
    state.uncertain = false;
    Ok(())
}

impl LocalContentStore {
    /// The cold activation path creates the owner lock before enabling the
    /// mandatory layout gate. Observational opens remain non-creating.
    pub(super) fn content_mutation_guard(&self) -> Result<ContentMutationGuard> {
        if let Some(error) = self.inline_store.identity_error.as_ref() {
            return Err(inline_error(format!(
                "content root identity failed: {error}"
            )));
        }
        let path = self.content_reservation_batch_sentinel_path();
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(source)
                if source.kind() == io::ErrorKind::NotFound
                    && !self.content_reservation_writer_initialized =>
            {
                return Ok(ContentMutationGuard::inactive());
            }
            Err(source) => return Err(ContentStoreError::Io { path, source }),
        };
        let sentinel = decode_content_reservation_batch_sentinel(&bytes)?;
        validate_content_reservation_batch_sentinel(&sentinel)?;
        if sentinel.content_binding_layout == INLINE_BINDING_LAYOUT {
            ContentMutationGuard::acquire(&self.inline_store.owner.root, false)
        } else {
            Ok(ContentMutationGuard::inactive())
        }
    }
}
