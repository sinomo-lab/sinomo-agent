//! Durable local content storage for DFS blobs.

mod accounting;
mod fs_ops;
mod packed;
mod readers;

pub use accounting::{
    ContentStoreQuota, ContentStoreQuotaResource, ContentStoreQuotaUsage, ContentStoreStats,
};
use accounting::{ContentStoreQuotaDelta, ContentStoreStatsDelta, ContentStoreStatsSnapshot};
use blake3::hazmat::{
    ChainingValue, HasherExt, Mode, left_subtree_len, merge_subtrees_non_root, merge_subtrees_root,
};
#[cfg(test)]
use fs_ops::write_file_atomic;
use fs_ops::{
    content_durable_file_error, create_dir_all, duration_nanos_saturating, file_age_nanos,
    file_len_if_exists, projected_file_growth, read_dir, read_file, recursive_file_count_and_bytes,
    remove_empty_dir_if_exists_and_sync_parent, remove_file_if_exists,
    remove_file_if_exists_and_sync_parent, sync_dir, system_time_nanos_since_epoch_saturating,
    temporary_path_for, write_file_create_new_atomic, write_files_create_new_relaxed,
    write_reader_to_file,
};
#[cfg(test)]
use packed::PACKED_BLOB_LOCK_FILE;
use packed::{
    PACKED_BLOB_DIR, PACKED_BLOB_SEGMENT_DIR, PACKED_BLOB_SMALL_LIMIT_BYTES, PackedBlobIndexCache,
};
pub use readers::{BlobRangeReader, BlobReader, ObjectVersionReader};
use serde::{Deserialize, Serialize};
use sinomo_core::object::{LengthMode, serde_ext as sinomo_object_serde};
use sinomo_core::storage::durable_file;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::AtomicU64;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant, SystemTime};
use thiserror::Error;

const HASH_PREFIX: &str = "blake3:";
const BLAKE3_HEX_LEN: usize = 64;
const CONTENT_STORE_STATS_VERSION: u32 = 1;
const CONTENT_STORE_STATS_FILE: &str = "stats.sobj";
const CONTENT_STORE_STATS_LOCK_FILE: &str = "stats.lock";
const STREAMED_OBJECT_WRITE_BUFFER_BYTES: usize = 1024 * 1024;
const STREAMED_BLOB_ONLY_MIN_BYTES: usize = 1024 * 1024;
const CONTENT_HASH_TREE_VERSION: u32 = 1;
const CONTENT_HASH_TREE_LEAF_BYTES: u64 = 1024 * 1024;
const STREAM_MANIFEST_CACHE_MAX_ENTRIES: usize = 1024;

fn content_store_perf_trace(args: std::fmt::Arguments<'_>) {
    tracing::trace!(target: "sinomo.dfs.perf", detail = %args, "dfs content store perf");
}

#[derive(Debug, Error)]
pub enum ContentStoreError {
    #[error("io error at {path}: {source}")]
    Io { path: PathBuf, source: io::Error },
    #[error("manifest serialization error: {0}")]
    Serde(String),
    #[error("invalid content hash: {0}")]
    InvalidHash(String),
    #[error("staged content not found for intent: {0}")]
    MissingStaged(String),
    #[error("content-addressed blob hash mismatch: expected={expected}, actual={actual}")]
    BlobHashMismatch { expected: String, actual: String },
    #[error("loose content-addressed blob not found: {hash}")]
    MissingLooseBlob { hash: String },
    #[error("content-addressed blob is packed, not loose: {hash}")]
    PackedBlob { hash: String },
    #[error("invalid content layout: {message}")]
    InvalidContentLayout { message: String },
    #[error(
        "object version manifest already exists with different content: object={object_id}, version={version}, existing={existing_hash}, new={new_hash}"
    )]
    VersionHashConflict {
        object_id: String,
        version: String,
        existing_hash: String,
        new_hash: String,
    },
    #[error(
        "content store quota exceeded for {resource:?}: limit={limit_bytes}, current={current_bytes}, additional={additional_bytes}"
    )]
    QuotaExceeded {
        resource: ContentStoreQuotaResource,
        limit_bytes: u64,
        current_bytes: u64,
        additional_bytes: u64,
    },
}

pub type Result<T> = std::result::Result<T, ContentStoreError>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentManifest {
    pub object_id: String,
    pub version: String,
    pub content_hash: String,
    pub size_bytes: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extents: Vec<ContentExtent>,
}

/// Finalized streamed content that is not yet bound to an object version.
///
/// The quarantine file is durable and content-addressed by `content_hash`, but
/// it does not reserve an `(object_id, version)` manifest. Dropping an unbound
/// value removes the quarantine file.
#[derive(Debug)]
pub struct ContentStoreQuarantinedBlob {
    store_root: PathBuf,
    temporary_path: PathBuf,
    pub content_hash: String,
    pub size_bytes: u64,
}

impl Drop for ContentStoreQuarantinedBlob {
    fn drop(&mut self) {
        remove_file_if_exists(&self.temporary_path).ok();
    }
}

/// One-pass object-version ingest into transport-independent content storage.
///
/// Callers write each borrowed chunk directly while they own it. The ingest
/// hashes and writes that range once. It may either finalize into an anonymous
/// quarantine blob for later authorization, or atomically publish the finalized
/// blob and manifest at EOF. Dropping an unfinished ingest removes its temporary
/// file.
pub struct ContentStoreObjectIngest<'a> {
    store: &'a LocalContentStore,
    object_id: String,
    version: String,
    expected_size_bytes: u64,
    expected_hash: Option<String>,
    received_size_bytes: u64,
    hasher: blake3::Hasher,
    file: Option<fs::File>,
    temporary_path: PathBuf,
    finished: bool,
}

impl std::fmt::Debug for ContentStoreObjectIngest<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ContentStoreObjectIngest")
            .field("object_id", &self.object_id)
            .field("version", &self.version)
            .field("expected_size_bytes", &self.expected_size_bytes)
            .field("received_size_bytes", &self.received_size_bytes)
            .finish_non_exhaustive()
    }
}

impl ContentStoreObjectIngest<'_> {
    /// Appends one producer-owned range without retaining or copying it.
    pub fn write_chunk(&mut self, bytes: &[u8]) -> Result<()> {
        let chunk_bytes =
            u64::try_from(bytes.len()).map_err(|_| ContentStoreError::InvalidContentLayout {
                message: "content ingest chunk length does not fit u64".to_string(),
            })?;
        let next_size = self
            .received_size_bytes
            .checked_add(chunk_bytes)
            .ok_or_else(|| ContentStoreError::InvalidContentLayout {
                message: "content ingest length overflow".to_string(),
            })?;
        if next_size > self.expected_size_bytes {
            return Err(ContentStoreError::InvalidContentLayout {
                message: format!(
                    "content ingest exceeded declared length: expected {}, received at least {next_size}",
                    self.expected_size_bytes
                ),
            });
        }
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| ContentStoreError::InvalidContentLayout {
                message: "content ingest was already finished".to_string(),
            })?;
        file.write_all(bytes)
            .map_err(|source| ContentStoreError::Io {
                path: self.temporary_path.clone(),
                source,
            })?;
        self.hasher.update(bytes);
        self.received_size_bytes = next_size;
        Ok(())
    }

    /// Finalizes the one application digest without reserving an object version.
    pub fn finish_quarantined(mut self) -> Result<ContentStoreQuarantinedBlob> {
        if self.received_size_bytes != self.expected_size_bytes {
            return Err(ContentStoreError::InvalidContentLayout {
                message: format!(
                    "content ingest length mismatch: expected {}, received {}",
                    self.expected_size_bytes, self.received_size_bytes
                ),
            });
        }
        let hash = format!("{HASH_PREFIX}{}", self.hasher.finalize().to_hex());
        if let Some(expected_hash) = self.expected_hash.as_deref()
            && expected_hash != hash
        {
            return Err(ContentStoreError::BlobHashMismatch {
                expected: expected_hash.to_string(),
                actual: hash,
            });
        }
        let file = self
            .file
            .take()
            .ok_or_else(|| ContentStoreError::InvalidContentLayout {
                message: "content ingest was already finished".to_string(),
            })?;
        file.sync_all().map_err(|source| ContentStoreError::Io {
            path: self.temporary_path.clone(),
            source,
        })?;
        drop(file);
        self.finished = true;
        Ok(ContentStoreQuarantinedBlob {
            store_root: self.store.root.clone(),
            temporary_path: self.temporary_path.clone(),
            content_hash: hash,
            size_bytes: self.received_size_bytes,
        })
    }

    /// Finalizes the one application digest and publishes the blob and manifest.
    pub fn finish(self) -> Result<ContentManifest> {
        let store = self.store;
        let object_id = self.object_id.clone();
        let version = self.version.clone();
        let blob = self.finish_quarantined()?;
        store.bind_quarantined_blob(&object_id, &version, blob)
    }
}

impl Drop for ContentStoreObjectIngest<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.file.take();
            remove_file_if_exists(&self.temporary_path).ok();
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentPatchManifest {
    pub manifest: ContentManifest,
    pub patch_hash: Option<String>,
    pub patch_size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentVersionIdentity {
    pub content_hash: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ContentHashTree {
    version: u32,
    object_id: String,
    object_version: String,
    size_bytes: u64,
    content_hash: String,
    leaf_bytes: u64,
    leaves: Vec<ContentHashTreeLeaf>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ContentHashTreeLeaf {
    logical_offset: u64,
    size_bytes: u64,
    chaining_value: ChainingValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentExtent {
    pub logical_offset: u64,
    pub size_bytes: u64,
    pub content_hash: String,
    pub blob_offset: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentStoreScrubReport {
    pub stats: ContentStoreStats,
    pub referenced_blob_count: u64,
    pub referenced_blob_bytes: u64,
    pub orphan_blob_count: u64,
    pub orphan_blob_bytes: u64,
    pub issues: Vec<ContentStoreScrubIssue>,
}

impl ContentStoreScrubReport {
    pub fn is_healthy(&self) -> bool {
        self.issues.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum ContentStoreScrubIssue {
    InvalidManifest {
        path: PathBuf,
        message: String,
    },
    MissingBlob {
        object_id: String,
        version: String,
        content_hash: String,
        path: PathBuf,
    },
    CorruptBlob {
        object_id: String,
        version: String,
        content_hash: String,
        path: PathBuf,
        actual_hash: String,
    },
    BlobSizeMismatch {
        object_id: String,
        version: String,
        content_hash: String,
        path: PathBuf,
        expected_size_bytes: u64,
        actual_size_bytes: u64,
    },
    BlobRangeOutOfBounds {
        object_id: String,
        version: String,
        content_hash: String,
        path: PathBuf,
        blob_offset: u64,
        size_bytes: u64,
        blob_size_bytes: u64,
    },
    OrphanBlob {
        content_hash: String,
        path: PathBuf,
        size_bytes: u64,
    },
    OrphanPackedBlob {
        content_hash: String,
        segment_id: String,
        offset: u64,
        path: PathBuf,
        size_bytes: u64,
    },
    InvalidBlobPath {
        path: PathBuf,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentStoreGcOptions {
    pub dry_run: bool,
    pub min_orphan_age_nanos: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_deletions: Option<u64>,
}

impl ContentStoreGcOptions {
    pub fn dry_run(min_orphan_age: Duration) -> Self {
        Self {
            dry_run: true,
            min_orphan_age_nanos: duration_nanos_saturating(min_orphan_age),
            max_deletions: None,
        }
    }

    pub fn delete_eligible(min_orphan_age: Duration) -> Self {
        Self {
            dry_run: false,
            min_orphan_age_nanos: duration_nanos_saturating(min_orphan_age),
            max_deletions: None,
        }
    }

    pub fn with_max_deletions(mut self, max_deletions: u64) -> Self {
        self.max_deletions = Some(max_deletions);
        self
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentStoreGcReport {
    pub scrub: ContentStoreScrubReport,
    pub dry_run: bool,
    pub min_orphan_age_nanos: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_deletions: Option<u64>,
    pub blocked_by_scrub_issue_count: u64,
    pub eligible_orphan_count: u64,
    pub eligible_orphan_bytes: u64,
    pub deleted_orphan_count: u64,
    pub deleted_orphan_bytes: u64,
    pub retained_young_orphan_count: u64,
    pub retained_young_orphan_bytes: u64,
    pub retained_limit_orphan_count: u64,
    pub retained_limit_orphan_bytes: u64,
    pub max_deletions_reached: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentStoreStagingGcOptions {
    pub dry_run: bool,
    pub min_staging_age_nanos: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_deletions: Option<u64>,
}

impl ContentStoreStagingGcOptions {
    pub fn dry_run(min_staging_age: Duration) -> Self {
        Self {
            dry_run: true,
            min_staging_age_nanos: duration_nanos_saturating(min_staging_age),
            max_deletions: None,
        }
    }

    pub fn delete_eligible(min_staging_age: Duration) -> Self {
        Self {
            dry_run: false,
            min_staging_age_nanos: duration_nanos_saturating(min_staging_age),
            max_deletions: None,
        }
    }

    pub fn with_max_deletions(mut self, max_deletions: u64) -> Self {
        self.max_deletions = Some(max_deletions);
        self
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentStoreStagingGcReport {
    pub dry_run: bool,
    pub min_staging_age_nanos: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_deletions: Option<u64>,
    pub scanned_staging_count: u64,
    pub scanned_staging_bytes: u64,
    pub eligible_staging_count: u64,
    pub eligible_staging_bytes: u64,
    pub deleted_staging_count: u64,
    pub deleted_staging_bytes: u64,
    pub retained_young_staging_count: u64,
    pub retained_young_staging_bytes: u64,
    /// Earliest wall-clock instant at which a retained staging file reaches
    /// the configured age. Reactive callers can sleep until this deadline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_eligible_at_unix_nanos: Option<u64>,
    pub retained_limit_staging_count: u64,
    pub retained_limit_staging_bytes: u64,
    pub max_deletions_reached: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentStoreVersionGcOptions {
    pub dry_run: bool,
    pub min_version_age_nanos: u64,
    pub keep_latest_versions_per_object: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_deletions: Option<u64>,
}

impl ContentStoreVersionGcOptions {
    pub fn dry_run(min_version_age: Duration, keep_latest_versions_per_object: u64) -> Self {
        Self {
            dry_run: true,
            min_version_age_nanos: duration_nanos_saturating(min_version_age),
            keep_latest_versions_per_object: keep_latest_versions_per_object.max(1),
            max_deletions: None,
        }
    }

    pub fn delete_eligible(
        min_version_age: Duration,
        keep_latest_versions_per_object: u64,
    ) -> Self {
        Self {
            dry_run: false,
            min_version_age_nanos: duration_nanos_saturating(min_version_age),
            keep_latest_versions_per_object: keep_latest_versions_per_object.max(1),
            max_deletions: None,
        }
    }

    pub fn with_max_deletions(mut self, max_deletions: u64) -> Self {
        self.max_deletions = Some(max_deletions);
        self
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentStoreVersionGcReport {
    pub scrub: ContentStoreScrubReport,
    pub dry_run: bool,
    pub min_version_age_nanos: u64,
    pub keep_latest_versions_per_object: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_deletions: Option<u64>,
    pub blocked_by_scrub_issue_count: u64,
    pub scanned_object_count: u64,
    pub scanned_version_count: u64,
    pub retained_latest_version_count: u64,
    pub retained_latest_logical_bytes: u64,
    pub retained_young_version_count: u64,
    pub retained_young_logical_bytes: u64,
    pub retained_limit_version_count: u64,
    pub retained_limit_logical_bytes: u64,
    pub eligible_version_count: u64,
    pub eligible_logical_bytes: u64,
    pub eligible_manifest_bytes: u64,
    pub deleted_version_count: u64,
    pub deleted_logical_bytes: u64,
    pub deleted_manifest_bytes: u64,
    pub max_deletions_reached: bool,
}

#[derive(Debug, Clone)]
pub struct LocalContentStore {
    root: PathBuf,
    quota: Option<ContentStoreQuota>,
    quota_stats_initialized: Arc<AtomicBool>,
    packed_index_cache: Arc<Mutex<PackedBlobIndexCache>>,
    stream_manifest_cache: Arc<Mutex<StreamManifestCache>>,
    #[cfg(test)]
    blob_stream_open_count: Arc<AtomicU64>,
    #[cfg(test)]
    stream_manifest_file_read_count: Arc<AtomicU64>,
}

#[derive(Debug)]
pub(super) struct ValidatedContentManifest {
    manifest: ContentManifest,
}

impl ValidatedContentManifest {
    pub(super) fn manifest(&self) -> &ContentManifest {
        &self.manifest
    }
}

#[derive(Debug, Default)]
struct StreamManifestCache {
    entries: BTreeMap<PathBuf, Arc<ValidatedContentManifest>>,
    insertion_order: VecDeque<PathBuf>,
}

impl StreamManifestCache {
    fn get(&mut self, path: &Path) -> Option<Arc<ValidatedContentManifest>> {
        self.entries.get(path).map(Arc::clone)
    }

    fn insert(
        &mut self,
        path: PathBuf,
        manifest: Arc<ValidatedContentManifest>,
    ) -> Arc<ValidatedContentManifest> {
        if let Some(entry) = self.entries.get_mut(&path) {
            return Arc::clone(entry);
        }
        while self.entries.len() >= STREAM_MANIFEST_CACHE_MAX_ENTRIES {
            let Some(oldest_path) = self.insertion_order.pop_front() else {
                break;
            };
            self.entries.remove(&oldest_path);
        }
        self.insertion_order.push_back(path.clone());
        self.entries.insert(path, Arc::clone(&manifest));
        manifest
    }

    fn remove(&mut self, path: &Path) {
        self.entries.remove(path);
        self.insertion_order.retain(|cached| cached != path);
    }
}

#[derive(Debug, Clone)]
struct ScrubbedBlob {
    actual_hash: String,
    size_bytes: u64,
}

#[derive(Debug, Clone)]
enum OrphanBlobGcCandidate {
    Loose {
        path: PathBuf,
        size_bytes: u64,
    },
    Packed {
        content_hash: String,
        path: PathBuf,
        size_bytes: u64,
    },
}

impl OrphanBlobGcCandidate {
    fn path(&self) -> PathBuf {
        match self {
            Self::Loose { path, .. } | Self::Packed { path, .. } => path.clone(),
        }
    }

    fn size_bytes(&self) -> u64 {
        match self {
            Self::Loose { size_bytes, .. } | Self::Packed { size_bytes, .. } => *size_bytes,
        }
    }
}

#[derive(Debug, Clone)]
struct StagingGcCandidate {
    path: PathBuf,
    size_bytes: u64,
    modified_at: SystemTime,
}

#[derive(Debug, Clone)]
struct VersionManifestEntry {
    object_id: String,
    version: String,
    content_hash: String,
    path: PathBuf,
    version_dir: PathBuf,
    object_dir: PathBuf,
    modified_nanos: u64,
    manifest_bytes: u64,
    logical_bytes: u64,
    extent_count: u64,
}

struct PreparedSmallObjectVersion<'a> {
    bytes: &'a [u8],
    blob_path: PathBuf,
    manifest: ContentManifest,
    manifest_bytes: Vec<u8>,
    manifest_path: PathBuf,
}

struct StreamedObjectVersionFile {
    path: PathBuf,
    hash: String,
    size_bytes: u64,
    captured_bytes: Option<Vec<u8>>,
}

impl LocalContentStore {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        let blobs_dir = root.join("blobs");
        create_dir_all(blobs_dir.clone())?;
        for shard in 0_u8..=u8::MAX {
            create_dir_all(blobs_dir.join(format!("{shard:02x}")))?;
        }
        create_dir_all(root.join(PACKED_BLOB_DIR).join(PACKED_BLOB_SEGMENT_DIR))?;
        create_dir_all(root.join("objects"))?;
        create_dir_all(root.join("staging"))?;
        Ok(Self::open_for_inspection(root))
    }

    /// Opens a read-only view without initializing the content-store layout.
    ///
    /// This constructor is intended for advisory inspection of an existing
    /// store. Code that may write must use [`Self::new`] first.
    pub fn open_for_inspection(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            quota: None,
            // A persisted stats snapshot is derived observability state. It is
            // not trusted for quota admission until this process has rebuilt
            // it from the durable store while quota mode is active.
            quota_stats_initialized: Arc::new(AtomicBool::new(false)),
            packed_index_cache: Arc::new(Mutex::new(PackedBlobIndexCache::default())),
            stream_manifest_cache: Arc::new(Mutex::new(StreamManifestCache::default())),
            #[cfg(test)]
            blob_stream_open_count: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            stream_manifest_file_read_count: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn with_quota(mut self, quota: ContentStoreQuota) -> Self {
        self.quota = Some(quota);
        // The snapshot may have been produced by an earlier no-quota process,
        // whose mutations intentionally do not persist deltas. Force one
        // durable scan before using it as authoritative quota state.
        self.quota_stats_initialized.store(false, Ordering::Release);
        self
    }

    pub fn without_quota(mut self) -> Self {
        self.quota = None;
        self.quota_stats_initialized.store(false, Ordering::Release);
        self
    }

    pub fn quota(&self) -> Option<&ContentStoreQuota> {
        self.quota.as_ref()
    }

    pub fn stage_bytes(&self, intent_id: &str, bytes: &[u8]) -> Result<String> {
        let hash = content_hash(bytes);
        let staging_path = self.staging_path(intent_id);
        self.ensure_quota_delta(ContentStoreQuotaDelta {
            staging_bytes: projected_file_growth(&staging_path, bytes.len() as u64)?,
            ..ContentStoreQuotaDelta::default()
        })?;
        self.write_tracked_staging_bytes(&staging_path, bytes)?;
        Ok(hash)
    }

    pub fn commit_staged(
        &self,
        intent_id: &str,
        object_id: &str,
        version: &str,
    ) -> Result<ContentManifest> {
        let staging_path = self.staging_path(intent_id);
        if !staging_path.exists() {
            return Err(ContentStoreError::MissingStaged(intent_id.to_owned()));
        }

        let manifest = self.write_object_version_from_file(object_id, version, &staging_path)?;
        self.remove_tracked_staging_file(&staging_path)?;
        Ok(manifest)
    }

    pub fn put_object_version(
        &self,
        object_id: &str,
        version: &str,
        bytes: &[u8],
    ) -> Result<ContentManifest> {
        self.write_object_version(object_id, version, bytes)
    }

    /// Starts a one-pass streamed ingest for an object version.
    ///
    /// `expected_hash` is an application-level DFS identity, not a transport
    /// checksum. The final comparison happens once at EOF using the same digest
    /// that names the published content-addressed blob.
    pub fn begin_object_version_ingest(
        &self,
        object_id: &str,
        version: &str,
        expected_size_bytes: u64,
        expected_hash: Option<&str>,
    ) -> Result<ContentStoreObjectIngest<'_>> {
        self.begin_content_ingest(object_id, version, expected_size_bytes, expected_hash)
    }

    /// Starts a streamed ingest whose finalized bytes do not reserve an object
    /// version until `bind_quarantined_blob` is called after authorization.
    pub fn begin_quarantined_blob_ingest(
        &self,
        expected_size_bytes: u64,
        expected_hash: Option<&str>,
    ) -> Result<ContentStoreObjectIngest<'_>> {
        self.begin_content_ingest("", "", expected_size_bytes, expected_hash)
    }

    fn begin_content_ingest(
        &self,
        object_id: &str,
        version: &str,
        expected_size_bytes: u64,
        expected_hash: Option<&str>,
    ) -> Result<ContentStoreObjectIngest<'_>> {
        let temporary_anchor = self.root.join("blob-tmp").join("stream-ingest");
        let temporary_path = temporary_path_for(&temporary_anchor);
        if let Some(parent) = temporary_path.parent() {
            create_dir_all(parent.to_owned())?;
        }
        let file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary_path)
            .map_err(|source| ContentStoreError::Io {
                path: temporary_path.clone(),
                source,
            })?;
        Ok(ContentStoreObjectIngest {
            store: self,
            object_id: object_id.to_string(),
            version: version.to_string(),
            expected_size_bytes,
            expected_hash: expected_hash.map(ToString::to_string),
            received_size_bytes: 0,
            hasher: blake3::Hasher::new(),
            file: Some(file),
            temporary_path,
            finished: false,
        })
    }

    /// Atomically binds a finalized quarantine blob to one object version.
    ///
    /// All admission, authority, base-version, and route-rebase checks must
    /// complete before calling this method.
    pub fn bind_quarantined_blob(
        &self,
        object_id: &str,
        version: &str,
        blob: ContentStoreQuarantinedBlob,
    ) -> Result<ContentManifest> {
        if blob.store_root != self.root {
            return Err(ContentStoreError::InvalidContentLayout {
                message: format!(
                    "quarantined blob belongs to {}, not content store {}",
                    blob.store_root.display(),
                    self.root.display()
                ),
            });
        }
        self.publish_prehashed_object_version_file(
            object_id,
            version,
            blob.content_hash.clone(),
            blob.size_bytes,
            &blob.temporary_path,
        )
    }

    /// Stores one already-materialized DFS value while computing its
    /// application digest exactly once and publishing only after it matches.
    pub fn put_object_version_with_expected_hash(
        &self,
        object_id: &str,
        version: &str,
        expected_hash: &str,
        bytes: &[u8],
    ) -> Result<ContentManifest> {
        let mut ingest = self.begin_object_version_ingest(
            object_id,
            version,
            u64::try_from(bytes.len()).map_err(|_| ContentStoreError::InvalidContentLayout {
                message: "content length does not fit u64".to_string(),
            })?,
            Some(expected_hash),
        )?;
        ingest.write_chunk(bytes)?;
        ingest.finish()
    }

    pub fn put_object_versions<'a, I>(&self, versions: I) -> Result<Vec<ContentManifest>>
    where
        I: IntoIterator<Item = (&'a str, &'a str, &'a [u8])>,
    {
        let versions = versions.into_iter().collect::<Vec<_>>();
        if versions.is_empty() {
            return Ok(Vec::new());
        }
        if versions
            .iter()
            .any(|(_, _, bytes)| bytes.len() as u64 > PACKED_BLOB_SMALL_LIMIT_BYTES)
        {
            return versions
                .into_iter()
                .map(|(object_id, version, bytes)| {
                    self.write_object_version(object_id, version, bytes)
                })
                .collect();
        }
        self.write_small_object_versions_batch(&versions)
    }

    /// Durably stores content-addressed blobs without publishing per-object
    /// manifests. Callers may publish visible metadata immediately after this
    /// method returns.
    pub fn put_object_version_blobs_only<'a, I>(&self, versions: I) -> Result<Vec<ContentManifest>>
    where
        I: IntoIterator<Item = (&'a str, &'a str, &'a [u8])>,
    {
        let versions = versions.into_iter().collect::<Vec<_>>();
        if versions.is_empty() {
            return Ok(Vec::new());
        }
        if versions
            .iter()
            .any(|(_, _, bytes)| bytes.len() as u64 > PACKED_BLOB_SMALL_LIMIT_BYTES)
        {
            return versions
                .into_iter()
                .map(|(object_id, version, bytes)| {
                    self.write_object_version_blob_only(object_id, version, bytes)
                })
                .collect();
        }

        let mut manifests = Vec::with_capacity(versions.len());
        let mut blob_writes = Vec::new();
        let mut quota_delta = ContentStoreQuotaDelta::default();
        {
            let _lock = self.lock_packed_blobs()?;
            let mut index = self.read_packed_blob_index_for_update_unlocked()?;
            for (object_id, version, bytes) in &versions {
                let hash = content_hash(bytes);
                let manifest_path = self.manifest_path(object_id, version);
                if manifest_path.is_file() {
                    let existing = decode_manifest(&read_file(&manifest_path)?)?;
                    if existing.content_hash != hash {
                        return Err(ContentStoreError::VersionHashConflict {
                            object_id: (*object_id).to_owned(),
                            version: (*version).to_owned(),
                            existing_hash: existing.content_hash,
                            new_hash: hash,
                        });
                    }
                    self.ensure_content_manifest_index_best_effort(&existing);
                    manifests.push(existing);
                    continue;
                }

                let blob_path = self.blob_path(&hash)?;
                let manifest = ContentManifest {
                    object_id: (*object_id).to_owned(),
                    version: (*version).to_owned(),
                    content_hash: hash,
                    size_bytes: bytes.len() as u64,
                    extents: Vec::new(),
                };
                let loose_readable = blob_path.is_file()
                    && file_content_hash(&blob_path)
                        .is_ok_and(|actual| actual == manifest.content_hash);
                let packed_readable = match index.entries.get(&manifest.content_hash) {
                    Some(location) => self.packed_blob_location_readable(location)?,
                    None => false,
                };
                if !loose_readable && !packed_readable {
                    quota_delta.blob_bytes =
                        quota_delta.blob_bytes.saturating_add(bytes.len() as u64);
                    blob_writes.push((manifest.content_hash.clone(), *bytes));
                }
                manifests.push(manifest);
            }
            self.ensure_quota_delta(quota_delta)?;
            let blob_delta = self.write_packed_blobs_unlocked(&mut index, &blob_writes)?;
            if !blob_delta.is_zero() {
                self.apply_stats_delta(blob_delta)?;
            }
        }
        Ok(manifests)
    }

    pub fn put_object_version_blob_only(
        &self,
        object_id: &str,
        version: &str,
        bytes: &[u8],
    ) -> Result<ContentManifest> {
        self.write_object_version_blob_only(object_id, version, bytes)
    }

    pub fn put_blob_only_manifest(
        &self,
        object_id: &str,
        version: &str,
        content_hash: &str,
        size_bytes: u64,
    ) -> Result<ContentManifest> {
        let reader = self.open_blob_stream(content_hash)?;
        if reader.size_bytes() != size_bytes {
            return Err(ContentStoreError::InvalidContentLayout {
                message: format!(
                    "blob-only manifest size does not match blob: object={object_id}, version={version}, manifest={size_bytes}, blob={}",
                    reader.size_bytes()
                ),
            });
        }
        self.write_manifest_create_new_or_existing(ContentManifest {
            object_id: object_id.to_owned(),
            version: version.to_owned(),
            content_hash: content_hash.to_owned(),
            size_bytes,
            extents: Vec::new(),
        })
    }

    pub fn put_object_version_from_reader<R: Read>(
        &self,
        object_id: &str,
        version: &str,
        reader: R,
    ) -> Result<ContentManifest> {
        self.put_object_version_from_reader_capture(object_id, version, reader, 0)
            .map(|(manifest, _)| manifest)
    }

    pub fn put_object_version_from_reader_capture<R: Read>(
        &self,
        object_id: &str,
        version: &str,
        mut reader: R,
        capture_limit_bytes: usize,
    ) -> Result<(ContentManifest, Option<Vec<u8>>)> {
        let internal_capture_limit =
            capture_limit_bytes.max(PACKED_BLOB_SMALL_LIMIT_BYTES as usize);

        // Keep small reader-backed writes in memory. The old path created,
        // wrote, fsynced, and removed a temporary file before writing the same
        // bytes into the packed-blob store. Small POSIX files are common and
        // already fit within the bounded capture budget, so probe one byte past
        // that budget and spill only when the input is actually larger.
        let probe_limit = u64::try_from(internal_capture_limit)
            .unwrap_or(u64::MAX)
            .saturating_add(1);
        let mut buffered = Vec::new();
        (&mut reader)
            .take(probe_limit)
            .read_to_end(&mut buffered)
            .map_err(|source| ContentStoreError::Io {
                path: self.root.join("blob-tmp").join("streamed-object-version"),
                source,
            })?;
        if buffered.len() <= internal_capture_limit {
            let size_bytes = buffered.len() as u64;
            let manifest = self.write_object_version(object_id, version, &buffered)?;
            let captured =
                capture_limit_includes_size(capture_limit_bytes, size_bytes).then_some(buffered);
            return Ok((manifest, captured));
        }

        let reader = std::io::Cursor::new(buffered).chain(reader);
        let mut streamed =
            self.write_streamed_object_version_file(reader, internal_capture_limit)?;
        if streamed.size_bytes <= PACKED_BLOB_SMALL_LIMIT_BYTES {
            let bytes = streamed
                .captured_bytes
                .take()
                .expect("small streamed object bytes are always captured");
            let captured = if capture_limit_includes_size(capture_limit_bytes, streamed.size_bytes)
            {
                Some(bytes.clone())
            } else {
                None
            };
            let result = self.write_object_version(object_id, version, &bytes);
            remove_file_if_exists(&streamed.path).ok();
            return result.map(|manifest| (manifest, captured));
        }

        let captured = if capture_limit_includes_size(capture_limit_bytes, streamed.size_bytes) {
            streamed.captured_bytes.take()
        } else {
            None
        };
        let result = self.write_object_version_from_verified_file(
            object_id,
            version,
            &streamed.path,
            streamed.hash,
            streamed.size_bytes,
        );
        remove_file_if_exists(&streamed.path).ok();
        result.map(|manifest| (manifest, captured))
    }

    pub fn read_blob(&self, hash: &str) -> Result<Vec<u8>> {
        let mut reader = self.open_blob_reader(hash)?;
        let mut bytes =
            Vec::with_capacity(usize::try_from(reader.size_bytes()).unwrap_or(usize::MAX));
        reader
            .read_to_end(&mut bytes)
            .map_err(|source| ContentStoreError::Io {
                path: self
                    .blob_path(hash)
                    .unwrap_or_else(|_| self.root.join("blobs")),
                source,
            })?;
        ensure_bytes_match_hash(hash, &bytes)?;
        Ok(bytes)
    }

    pub fn read_blob_range(&self, hash: &str, offset: u64, max_bytes: usize) -> Result<Vec<u8>> {
        let mut reader = self.open_blob_range_stream(hash, offset, max_bytes)?;
        let mut bytes =
            Vec::with_capacity(usize::try_from(reader.remaining_bytes()).unwrap_or(usize::MAX));
        reader
            .read_to_end(&mut bytes)
            .map_err(|source| ContentStoreError::Io {
                path: self
                    .blob_path(hash)
                    .unwrap_or_else(|_| self.root.join("blobs")),
                source,
            })?;
        Ok(bytes)
    }

    pub fn open_blob_reader(&self, hash: &str) -> Result<BlobReader> {
        let path = self.blob_path(hash)?;
        if path.is_file() {
            let (actual_hash, size_bytes) = file_content_hash_and_size(&path)?;
            if actual_hash != hash {
                return Err(ContentStoreError::BlobHashMismatch {
                    expected: hash.to_owned(),
                    actual: actual_hash,
                });
            }
            let file =
                fs::File::open(&path).map_err(|source| ContentStoreError::Io { path, source })?;
            return Ok(BlobReader::new(file, size_bytes));
        }
        let Some(location) = self.packed_blob_location(hash)? else {
            return Err(ContentStoreError::Io {
                path,
                source: io::Error::new(io::ErrorKind::NotFound, "blob not found"),
            });
        };
        let mut reader = self.open_packed_blob_reader(&location, 0, usize::MAX)?;
        let mut bytes =
            Vec::with_capacity(usize::try_from(reader.size_bytes()).unwrap_or(usize::MAX));
        reader
            .read_to_end(&mut bytes)
            .map_err(|source| ContentStoreError::Io {
                path: self.packed_blob_segment_path(&location.segment_id),
                source,
            })?;
        ensure_bytes_match_hash(hash, &bytes)?;
        self.open_packed_blob_reader(&location, 0, usize::MAX)
    }

    /// Opens a blob for streaming using a single read pass.
    ///
    /// Unlike [`Self::open_blob_reader`], this does not perform a separate
    /// full-file hash pass to verify the content address before returning. The
    /// blob size is taken from filesystem metadata. This is intended for serving
    /// content-addressed bytes to a remote peer that verifies the assembled hash
    /// itself (and rotates to another source on mismatch), where the extra read
    /// pass would simply double the I/O for large blobs.
    pub fn open_blob_stream(&self, hash: &str) -> Result<BlobReader> {
        #[cfg(test)]
        self.blob_stream_open_count.fetch_add(1, Ordering::Relaxed);
        let path = self.blob_path(hash)?;
        if path.is_file() {
            let file = fs::File::open(&path).map_err(|source| ContentStoreError::Io {
                path: path.clone(),
                source,
            })?;
            let size_bytes = file
                .metadata()
                .map_err(|source| ContentStoreError::Io { path, source })?
                .len();
            return Ok(BlobReader::new(file, size_bytes));
        }
        let Some(location) = self.packed_blob_location(hash)? else {
            return Err(ContentStoreError::Io {
                path,
                source: io::Error::new(io::ErrorKind::NotFound, "blob not found"),
            });
        };
        self.open_packed_blob_reader(&location, 0, usize::MAX)
    }

    /// Opens a bounded range of a content-addressed blob for streaming.
    ///
    /// This has the same trust model as [`Self::open_blob_stream`]: the path is
    /// derived from the validated content hash, and callers that need full
    /// integrity verification must validate the assembled object hash at a
    /// higher layer. The important property here is bounded memory and no
    /// full-blob read before serving a range.
    pub fn open_blob_range_stream(
        &self,
        hash: &str,
        offset: u64,
        max_bytes: usize,
    ) -> Result<BlobRangeReader> {
        #[cfg(test)]
        self.blob_stream_open_count.fetch_add(1, Ordering::Relaxed);
        let path = self.blob_path(hash)?;
        if !path.is_file() {
            let Some(location) = self.packed_blob_location(hash)? else {
                return Err(ContentStoreError::Io {
                    path,
                    source: io::Error::new(io::ErrorKind::NotFound, "blob not found"),
                });
            };
            return self.open_packed_blob_range_reader(&location, offset, max_bytes);
        }
        let mut file = fs::File::open(&path).map_err(|source| ContentStoreError::Io {
            path: path.clone(),
            source,
        })?;
        let size_bytes = file
            .metadata()
            .map_err(|source| ContentStoreError::Io {
                path: path.clone(),
                source,
            })?
            .len();
        let start = offset.min(size_bytes);
        file.seek(SeekFrom::Start(start))
            .map_err(|source| ContentStoreError::Io {
                path: path.clone(),
                source,
            })?;
        let remaining_bytes = size_bytes
            .saturating_sub(start)
            .min(u64::try_from(max_bytes).unwrap_or(u64::MAX));
        Ok(BlobRangeReader::new(
            file,
            start,
            size_bytes,
            remaining_bytes,
        ))
    }

    pub fn read_object_version_range(
        &self,
        object_id: &str,
        version: &str,
        offset: u64,
        max_bytes: usize,
    ) -> Result<(ContentManifest, Vec<u8>)> {
        let manifest = self.read_manifest(object_id, version)?;
        let bytes = self.read_manifest_range(&manifest, offset, max_bytes)?;
        Ok((manifest, bytes))
    }

    /// Read a range through the durable content-identity manifest index.
    ///
    /// Metadata-only DFS revisions intentionally keep the same immutable
    /// content hash while advancing the visible version. Patch writes may be
    /// represented by an extent manifest rather than a single blob named by
    /// the aggregate hash, so a hash-only blob lookup is insufficient. Every
    /// published version manifest is hard-linked into a deterministic
    /// per-object hash index; this keeps steady-state and post-restart lookup
    /// O(1) without copying the object or scanning all historical versions.
    pub fn read_object_content_range(
        &self,
        object_id: &str,
        content_hash: &str,
        size_bytes: u64,
        offset: u64,
        max_bytes: usize,
    ) -> Result<Option<(ContentManifest, Vec<u8>)>> {
        let Some(manifest) =
            self.manifest_for_object_content(object_id, content_hash, size_bytes)?
        else {
            return Ok(None);
        };
        let bytes = self.read_manifest_range(&manifest, offset, max_bytes)?;
        Ok(Some((manifest, bytes)))
    }

    pub fn read_object_version(
        &self,
        object_id: &str,
        version: &str,
    ) -> Result<(ContentManifest, Vec<u8>)> {
        let (manifest, mut reader) = self.open_object_version_stream(object_id, version)?;
        let capacity = usize::try_from(reader.size_bytes()).map_err(|_| {
            ContentStoreError::InvalidContentLayout {
                message: format!(
                    "object version is too large to buffer: object={object_id}, version={version}, size={}",
                    reader.size_bytes()
                ),
            }
        })?;
        let mut bytes = Vec::with_capacity(capacity);
        reader
            .read_to_end(&mut bytes)
            .map_err(|source| ContentStoreError::Io {
                path: self.manifest_path(object_id, version),
                source,
            })?;
        ensure_bytes_match_hash(&manifest.content_hash, &bytes)?;
        Ok((manifest, bytes))
    }

    pub fn open_object_version_stream(
        &self,
        object_id: &str,
        version: &str,
    ) -> Result<(ContentManifest, ObjectVersionReader)> {
        let manifest = self.read_manifest(object_id, version)?;
        if manifest.extents.is_empty() {
            let reader = self.open_blob_stream(&manifest.content_hash)?;
            if reader.size_bytes() != manifest.size_bytes {
                return Err(ContentStoreError::InvalidContentLayout {
                    message: format!(
                        "object version manifest size does not match blob size: object={object_id}, version={version}, manifest={}, blob={}",
                        manifest.size_bytes,
                        reader.size_bytes()
                    ),
                });
            }
            return Ok((manifest, ObjectVersionReader::from_blob(reader)));
        }

        let extents = self.logical_extents_for_manifest(&manifest)?;
        self.ensure_extents_readable(&extents)?;
        let reader = ObjectVersionReader::from_extents(self.clone(), manifest.size_bytes, extents);
        Ok((manifest, reader))
    }

    pub fn open_object_version_range_stream(
        &self,
        object_id: &str,
        version: &str,
        offset: u64,
        max_bytes: usize,
    ) -> Result<(ContentManifest, ObjectVersionReader)> {
        let (manifest, reader) =
            self.open_object_version_range_stream_shared(object_id, version, offset, max_bytes)?;
        Ok((manifest.manifest.clone(), reader))
    }

    pub(crate) fn open_object_version_range_stream_shared(
        &self,
        object_id: &str,
        version: &str,
        offset: u64,
        max_bytes: usize,
    ) -> Result<(Arc<ValidatedContentManifest>, ObjectVersionReader)> {
        let manifest = self.read_stream_manifest(object_id, version)?;
        let reader =
            self.open_validated_manifest_range_stream(Arc::clone(&manifest), offset, max_bytes)?;
        Ok((manifest, reader))
    }

    fn open_validated_manifest_range_stream(
        &self,
        manifest: Arc<ValidatedContentManifest>,
        offset: u64,
        max_bytes: usize,
    ) -> Result<ObjectVersionReader> {
        let manifest_fields = &manifest.manifest;
        let start = offset.min(manifest_fields.size_bytes);
        let max_bytes_u64 = u64::try_from(max_bytes).unwrap_or(u64::MAX);
        let end = start
            .saturating_add(max_bytes_u64)
            .min(manifest_fields.size_bytes);
        if manifest_fields.extents.is_empty() {
            let reader =
                self.open_blob_range_stream(&manifest_fields.content_hash, start, max_bytes)?;
            return Ok(ObjectVersionReader::from_blob_range(reader));
        }

        // Cached stream manifests have already been sorted and validated. Use
        // the monotonic logical offsets to select only the extents intersecting
        // this bounded range, without cloning or sorting the complete layout.
        let first_extent_index = manifest_fields.extents.partition_point(|extent| {
            extent.logical_offset.saturating_add(extent.size_bytes) <= start
        });
        let selected_extent_count = manifest_fields.extents[first_extent_index..]
            .partition_point(|extent| extent.logical_offset < end);
        let end_extent_index = first_extent_index.saturating_add(selected_extent_count);
        for extent in &manifest_fields.extents[first_extent_index..end_extent_index] {
            let extent_end = extent.logical_offset.saturating_add(extent.size_bytes);
            let range_start = start.max(extent.logical_offset);
            let range_end = end.min(extent_end);
            self.ensure_extent_subrange_readable(extent, range_start, range_end)?;
        }
        Ok(ObjectVersionReader::from_manifest_range(
            self.clone(),
            manifest,
            end.saturating_sub(start),
            first_extent_index,
            end_extent_index,
            start,
            end,
        ))
    }

    /// Open a bounded range stream for a content identity whose visible DFS
    /// metadata version may be newer than the stored extent manifest.
    pub fn open_object_content_range_stream(
        &self,
        object_id: &str,
        content_hash: &str,
        size_bytes: u64,
        offset: u64,
        max_bytes: usize,
    ) -> Result<Option<(ContentManifest, ObjectVersionReader)>> {
        let Some(manifest) =
            self.manifest_for_object_content(object_id, content_hash, size_bytes)?
        else {
            return Ok(None);
        };
        let manifest = self.cache_stream_manifest(manifest)?;
        let reader =
            self.open_validated_manifest_range_stream(Arc::clone(&manifest), offset, max_bytes)?;
        Ok(Some((manifest.manifest.clone(), reader)))
    }

    pub fn open_object_version_blob_stream(
        &self,
        object_id: &str,
        version: &str,
    ) -> Result<(ContentManifest, BlobReader)> {
        let manifest = self.read_manifest(object_id, version)?;
        let reader = self.open_blob_stream(&manifest.content_hash)?;
        if reader.size_bytes() != manifest.size_bytes {
            return Err(ContentStoreError::InvalidContentLayout {
                message: format!(
                    "object version manifest size does not match blob size: object={object_id}, version={version}, manifest={}, blob={}",
                    manifest.size_bytes,
                    reader.size_bytes()
                ),
            });
        }
        Ok((manifest, reader))
    }

    pub fn manifest_content_available(&self, manifest: &ContentManifest) -> Result<bool> {
        if manifest.extents.is_empty() {
            return Ok(self.has_blob(&manifest.content_hash)?);
        }
        let extents = self.logical_extents_for_manifest(manifest)?;
        self.extents_readable(&extents)
    }

    pub fn read_manifest(&self, object_id: &str, version: &str) -> Result<ContentManifest> {
        let path = self.manifest_path(object_id, version);
        let bytes = read_file(&path)?;
        decode_manifest(&bytes)
    }

    fn read_stream_manifest(
        &self,
        object_id: &str,
        version: &str,
    ) -> Result<Arc<ValidatedContentManifest>> {
        let path = self.manifest_path(object_id, version);
        let cached = {
            let mut cache = self
                .stream_manifest_cache
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            cache.get(&path)
        };
        if let Some(manifest) = cached {
            // Object-version manifests are immutable. The existence probe keeps
            // version collection visible while avoiding another file read,
            // decode, extent sort, and full layout validation on cache hits.
            if path.is_file() {
                return Ok(manifest);
            }
            self.evict_stream_manifest(&path);
        }

        #[cfg(test)]
        self.stream_manifest_file_read_count
            .fetch_add(1, Ordering::Relaxed);
        let manifest = decode_manifest(&read_file(&path)?)?;
        self.cache_stream_manifest_at(path, manifest)
    }

    fn cache_stream_manifest(
        &self,
        manifest: ContentManifest,
    ) -> Result<Arc<ValidatedContentManifest>> {
        let path = self.manifest_path(&manifest.object_id, &manifest.version);
        self.cache_stream_manifest_at(path, manifest)
    }

    fn cache_stream_manifest_at(
        &self,
        path: PathBuf,
        manifest: ContentManifest,
    ) -> Result<Arc<ValidatedContentManifest>> {
        let manifest = Arc::new(ValidatedContentManifest {
            manifest: validate_and_order_stream_manifest(manifest)?,
        });
        let mut cache = self
            .stream_manifest_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Ok(cache.insert(path, manifest))
    }

    fn evict_stream_manifest(&self, path: &Path) {
        self.stream_manifest_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(path);
    }

    pub fn manifests_for_object(&self, object_id: &str) -> Result<Vec<ContentManifest>> {
        let object_root = self.root.join("objects").join(object_id);
        if !object_root.exists() {
            return Ok(Vec::new());
        }

        let mut manifests: Vec<ContentManifest> = Vec::new();
        for entry in read_dir(&object_root)? {
            let path = entry.path().join("manifest.sobj");
            if path.is_file() {
                manifests.push(decode_manifest(&read_file(&path)?)?);
            }
        }
        manifests.sort_by(|left, right| left.version.cmp(&right.version));
        Ok(manifests)
    }

    fn manifest_for_object_content(
        &self,
        object_id: &str,
        content_hash: &str,
        size_bytes: u64,
    ) -> Result<Option<ContentManifest>> {
        let index_path = self.content_manifest_index_path(object_id, content_hash)?;
        if index_path.is_file() {
            match decode_manifest(&read_file(&index_path)?) {
                Ok(manifest)
                    if manifest.object_id == object_id
                        && manifest.content_hash == content_hash
                        && manifest.size_bytes == size_bytes =>
                {
                    return Ok(Some(manifest));
                }
                Ok(_) | Err(_) => {
                    // The index is derived cache state. Discard a corrupt or
                    // mismatched link and rebuild it from source manifests.
                    let _ = remove_file_if_exists(&index_path);
                }
            }
        }

        let manifest = self
            .manifests_for_object(object_id)?
            .into_iter()
            .rev()
            .find(|manifest| {
                manifest.content_hash == content_hash && manifest.size_bytes == size_bytes
            });
        if let Some(manifest) = manifest.as_ref() {
            self.ensure_content_manifest_index_best_effort(manifest);
        }
        Ok(manifest)
    }

    pub fn scrub(&self) -> Result<ContentStoreScrubReport> {
        let (blob_count, blob_bytes) = self.scan_blob_stats()?;
        let (staging_file_count, staging_bytes) =
            recursive_file_count_and_bytes(&self.root.join("staging"))?;
        let mut report = ContentStoreScrubReport {
            stats: ContentStoreStats {
                blob_count,
                blob_bytes,
                staging_file_count,
                staging_bytes,
                ..ContentStoreStats::default()
            },
            ..ContentStoreScrubReport::default()
        };
        let mut referenced_blobs = BTreeSet::new();
        let mut checked_blobs = BTreeMap::new();
        self.scrub_manifest_tree(
            &self.root.join("objects"),
            &mut report,
            &mut referenced_blobs,
            &mut checked_blobs,
        )?;
        self.collect_orphan_blob_issues(&self.root.join("blobs"), &referenced_blobs, &mut report)?;
        self.collect_orphan_packed_blob_issues(&referenced_blobs, &mut report)?;
        report.referenced_blob_count = referenced_blobs.len() as u64;
        report.referenced_blob_bytes = referenced_blobs
            .iter()
            .filter_map(|hash| checked_blobs.get(hash))
            .map(|blob| blob.size_bytes)
            .fold(0_u64, |total, size| total.saturating_add(size));
        self.replace_stats_snapshot(&report.stats)?;
        Ok(report)
    }

    pub fn collect_garbage(&self, options: ContentStoreGcOptions) -> Result<ContentStoreGcReport> {
        let scrub = self.scrub()?;
        let blocked_by_scrub_issue_count = scrub
            .issues
            .iter()
            .filter(|issue| {
                !matches!(
                    issue,
                    ContentStoreScrubIssue::OrphanBlob { .. }
                        | ContentStoreScrubIssue::OrphanPackedBlob { .. }
                )
            })
            .count() as u64;
        let mut report = ContentStoreGcReport {
            scrub,
            dry_run: options.dry_run,
            min_orphan_age_nanos: options.min_orphan_age_nanos,
            max_deletions: options.max_deletions,
            blocked_by_scrub_issue_count,
            ..ContentStoreGcReport::default()
        };
        if blocked_by_scrub_issue_count != 0 {
            return Ok(report);
        }

        let now = SystemTime::now();
        let orphan_blobs = report
            .scrub
            .issues
            .iter()
            .filter_map(|issue| match issue {
                ContentStoreScrubIssue::OrphanBlob {
                    path, size_bytes, ..
                } => Some(OrphanBlobGcCandidate::Loose {
                    path: path.clone(),
                    size_bytes: *size_bytes,
                }),
                ContentStoreScrubIssue::OrphanPackedBlob {
                    content_hash,
                    path,
                    size_bytes,
                    ..
                } => Some(OrphanBlobGcCandidate::Packed {
                    content_hash: content_hash.clone(),
                    path: path.clone(),
                    size_bytes: *size_bytes,
                }),
                _ => None,
            })
            .collect::<Vec<_>>();
        for orphan in orphan_blobs {
            let path = orphan.path();
            let size_bytes = orphan.size_bytes();
            let age_nanos = file_age_nanos(&path, now)?;
            if age_nanos < options.min_orphan_age_nanos {
                report.retained_young_orphan_count =
                    report.retained_young_orphan_count.saturating_add(1);
                report.retained_young_orphan_bytes = report
                    .retained_young_orphan_bytes
                    .saturating_add(size_bytes);
                continue;
            }

            report.eligible_orphan_count = report.eligible_orphan_count.saturating_add(1);
            report.eligible_orphan_bytes = report.eligible_orphan_bytes.saturating_add(size_bytes);
            if report
                .max_deletions
                .is_some_and(|max_deletions| report.deleted_orphan_count >= max_deletions)
            {
                report.max_deletions_reached = true;
                report.retained_limit_orphan_count =
                    report.retained_limit_orphan_count.saturating_add(1);
                report.retained_limit_orphan_bytes = report
                    .retained_limit_orphan_bytes
                    .saturating_add(size_bytes);
                continue;
            }
            if options.dry_run {
                continue;
            }
            match orphan {
                OrphanBlobGcCandidate::Loose { path, .. } => {
                    let before_len = file_len_if_exists(&path)?;
                    if remove_file_if_exists_and_sync_parent(&path)? {
                        report.deleted_orphan_count = report.deleted_orphan_count.saturating_add(1);
                        report.deleted_orphan_bytes =
                            report.deleted_orphan_bytes.saturating_add(size_bytes);
                        self.apply_stats_delta(ContentStoreStatsDelta::blob_file_change(
                            before_len, None,
                        ))?;
                    }
                }
                OrphanBlobGcCandidate::Packed { content_hash, .. } => {
                    if self.remove_packed_blob_index_entry(&content_hash)? {
                        report.deleted_orphan_count = report.deleted_orphan_count.saturating_add(1);
                        report.deleted_orphan_bytes =
                            report.deleted_orphan_bytes.saturating_add(size_bytes);
                        self.apply_stats_delta(ContentStoreStatsDelta {
                            blob_count: -1,
                            blob_bytes: -i128::from(size_bytes),
                            ..ContentStoreStatsDelta::default()
                        })?;
                    }
                }
            }
        }
        Ok(report)
    }

    pub fn collect_stale_staging(
        &self,
        options: ContentStoreStagingGcOptions,
    ) -> Result<ContentStoreStagingGcReport> {
        let now = SystemTime::now();
        let mut candidates = Vec::new();
        collect_staging_gc_candidates(&self.root.join("staging"), &mut candidates)?;
        candidates.sort_by(|left, right| left.path.cmp(&right.path));

        let mut report = ContentStoreStagingGcReport {
            dry_run: options.dry_run,
            min_staging_age_nanos: options.min_staging_age_nanos,
            max_deletions: options.max_deletions,
            ..ContentStoreStagingGcReport::default()
        };
        for candidate in candidates {
            report.scanned_staging_count = report.scanned_staging_count.saturating_add(1);
            report.scanned_staging_bytes = report
                .scanned_staging_bytes
                .saturating_add(candidate.size_bytes);
            let age_nanos = now
                .duration_since(candidate.modified_at)
                .map(duration_nanos_saturating)
                .unwrap_or(0);
            if age_nanos < options.min_staging_age_nanos {
                report.retained_young_staging_count =
                    report.retained_young_staging_count.saturating_add(1);
                report.retained_young_staging_bytes = report
                    .retained_young_staging_bytes
                    .saturating_add(candidate.size_bytes);
                let eligible_at = system_time_nanos_since_epoch_saturating(candidate.modified_at)
                    .saturating_add(options.min_staging_age_nanos);
                report.next_eligible_at_unix_nanos = Some(
                    report
                        .next_eligible_at_unix_nanos
                        .map(|current| current.min(eligible_at))
                        .unwrap_or(eligible_at),
                );
                continue;
            }
            report.eligible_staging_count = report.eligible_staging_count.saturating_add(1);
            report.eligible_staging_bytes = report
                .eligible_staging_bytes
                .saturating_add(candidate.size_bytes);
            if options
                .max_deletions
                .is_some_and(|max_deletions| report.deleted_staging_count >= max_deletions)
            {
                report.max_deletions_reached = true;
                report.retained_limit_staging_count =
                    report.retained_limit_staging_count.saturating_add(1);
                report.retained_limit_staging_bytes = report
                    .retained_limit_staging_bytes
                    .saturating_add(candidate.size_bytes);
                continue;
            }
            if !options.dry_run {
                self.remove_tracked_staging_file_if_exists(&candidate.path)?;
                report.deleted_staging_count = report.deleted_staging_count.saturating_add(1);
                report.deleted_staging_bytes = report
                    .deleted_staging_bytes
                    .saturating_add(candidate.size_bytes);
            }
        }
        Ok(report)
    }

    pub fn collect_versions(
        &self,
        options: ContentStoreVersionGcOptions,
    ) -> Result<ContentStoreVersionGcReport> {
        let scrub = self.scrub()?;
        let blocked_by_scrub_issue_count = scrub
            .issues
            .iter()
            .filter(|issue| {
                !matches!(
                    issue,
                    ContentStoreScrubIssue::OrphanBlob { .. }
                        | ContentStoreScrubIssue::OrphanPackedBlob { .. }
                )
            })
            .count() as u64;
        let keep_latest_versions_per_object = options.keep_latest_versions_per_object.max(1);
        let mut report = ContentStoreVersionGcReport {
            scrub,
            dry_run: options.dry_run,
            min_version_age_nanos: options.min_version_age_nanos,
            keep_latest_versions_per_object,
            max_deletions: options.max_deletions,
            blocked_by_scrub_issue_count,
            ..ContentStoreVersionGcReport::default()
        };
        if blocked_by_scrub_issue_count != 0 {
            return Ok(report);
        }

        let mut versions_by_object = BTreeMap::<String, Vec<VersionManifestEntry>>::new();
        self.collect_version_manifest_entries(&self.root.join("objects"), &mut versions_by_object)?;
        report.scanned_object_count = versions_by_object.len() as u64;
        report.scanned_version_count = versions_by_object
            .values()
            .map(|entries| entries.len() as u64)
            .fold(0_u64, |total, len| total.saturating_add(len));

        let now = SystemTime::now();
        let keep_latest_limit =
            usize::try_from(keep_latest_versions_per_object).unwrap_or(usize::MAX);
        for entries in versions_by_object.values_mut() {
            entries.sort_by(|left, right| {
                right
                    .modified_nanos
                    .cmp(&left.modified_nanos)
                    .then_with(|| right.version.cmp(&left.version))
                    .then_with(|| right.path.cmp(&left.path))
            });
            for (index, entry) in entries.iter().enumerate() {
                if index < keep_latest_limit {
                    report.retained_latest_version_count =
                        report.retained_latest_version_count.saturating_add(1);
                    report.retained_latest_logical_bytes = report
                        .retained_latest_logical_bytes
                        .saturating_add(entry.logical_bytes);
                    continue;
                }

                let age_nanos = file_age_nanos(&entry.path, now)?;
                if age_nanos < options.min_version_age_nanos {
                    report.retained_young_version_count =
                        report.retained_young_version_count.saturating_add(1);
                    report.retained_young_logical_bytes = report
                        .retained_young_logical_bytes
                        .saturating_add(entry.logical_bytes);
                    continue;
                }

                report.eligible_version_count = report.eligible_version_count.saturating_add(1);
                report.eligible_logical_bytes = report
                    .eligible_logical_bytes
                    .saturating_add(entry.logical_bytes);
                report.eligible_manifest_bytes = report
                    .eligible_manifest_bytes
                    .saturating_add(entry.manifest_bytes);
                if report
                    .max_deletions
                    .is_some_and(|max_deletions| report.deleted_version_count >= max_deletions)
                {
                    report.max_deletions_reached = true;
                    report.retained_limit_version_count =
                        report.retained_limit_version_count.saturating_add(1);
                    report.retained_limit_logical_bytes = report
                        .retained_limit_logical_bytes
                        .saturating_add(entry.logical_bytes);
                    continue;
                }
                if options.dry_run {
                    continue;
                }
                self.remove_content_manifest_index_for_version_if_current(entry)?;
                if remove_file_if_exists_and_sync_parent(&entry.path)? {
                    self.evict_stream_manifest(&entry.path);
                    report.deleted_version_count = report.deleted_version_count.saturating_add(1);
                    report.deleted_logical_bytes = report
                        .deleted_logical_bytes
                        .saturating_add(entry.logical_bytes);
                    report.deleted_manifest_bytes = report
                        .deleted_manifest_bytes
                        .saturating_add(entry.manifest_bytes);
                    self.apply_stats_delta(ContentStoreStatsDelta::manifest_deleted(entry))?;
                    remove_empty_dir_if_exists_and_sync_parent(&entry.version_dir)?;
                    remove_empty_dir_if_exists_and_sync_parent(&entry.object_dir)?;
                }
            }
        }
        Ok(report)
    }

    pub fn has_blob(&self, hash: &str) -> Result<bool> {
        let path = self.blob_path(hash)?;
        if path.is_file() {
            return Ok(file_content_hash(&path).is_ok_and(|actual| actual == hash));
        }
        let Some(location) = self.packed_blob_location(hash)? else {
            return Ok(false);
        };
        let mut reader = self.open_packed_blob_reader(&location, 0, usize::MAX)?;
        let mut bytes =
            Vec::with_capacity(usize::try_from(reader.size_bytes()).unwrap_or(usize::MAX));
        reader
            .read_to_end(&mut bytes)
            .map_err(|source| ContentStoreError::Io {
                path: self.packed_blob_segment_path(&location.segment_id),
                source,
            })?;
        Ok(content_hash(&bytes) == hash)
    }

    /// Returns whether local storage has an addressable candidate for `hash`.
    ///
    /// This is an advisory scheduling probe. It validates the hash syntax and
    /// the loose-file or packed-index bounds, but deliberately does not read or
    /// hash the blob bytes. Callers that consume the content remain responsible
    /// for end-to-end integrity validation.
    pub fn blob_storage_candidate_exists(&self, hash: &str) -> Result<bool> {
        let path = self.blob_path(hash)?;
        match fs::metadata(&path) {
            Ok(metadata) if metadata.is_file() => return Ok(true),
            Ok(_) => {}
            Err(source) if source.kind() == io::ErrorKind::NotFound => {}
            Err(source) => return Err(ContentStoreError::Io { path, source }),
        }
        let Some(location) = self.packed_blob_location(hash)? else {
            return Ok(false);
        };
        self.packed_blob_location_readable(&location)
    }

    /// Read-only form of [`Self::blob_storage_candidate_exists`] that does not
    /// initialize the content-store directory tree.
    pub fn blob_storage_candidate_exists_at(root: &Path, hash: &str) -> Result<bool> {
        Self::open_for_inspection(root).blob_storage_candidate_exists(hash)
    }

    /// Checks whether the exact object version has addressable local storage
    /// without reading or hashing its payload bytes.
    ///
    /// This mirrors the pending-write stream fallback: an exact manifest may
    /// reference either one logical blob or multiple extents; if the manifest
    /// is absent, the expected whole-content blob remains a candidate.
    pub fn object_version_storage_candidate_exists(
        &self,
        object_id: &str,
        version: &str,
        expected_hash: &str,
    ) -> Result<bool> {
        let manifest = match self.read_manifest(object_id, version) {
            Ok(manifest) => manifest,
            Err(error) if content_error_is_not_found(&error) => {
                return self.blob_storage_candidate_exists(expected_hash);
            }
            Err(error) => return Err(error),
        };
        if manifest.object_id != object_id
            || manifest.version != version
            || manifest.content_hash != expected_hash
        {
            return Err(ContentStoreError::InvalidContentLayout {
                message: format!(
                    "object version storage candidate does not match requested identity: object={object_id}, version={version}, expected_hash={expected_hash}, manifest_object={}, manifest_version={}, manifest_hash={}",
                    manifest.object_id, manifest.version, manifest.content_hash
                ),
            });
        }
        if manifest.extents.is_empty() {
            return match self.open_blob_stream(expected_hash) {
                Ok(reader) => Ok(reader.size_bytes() == manifest.size_bytes),
                Err(error) if content_error_is_not_found(&error) => Ok(false),
                Err(error) => Err(error),
            };
        }
        let extents = self.logical_extents_for_manifest(&manifest)?;
        self.extents_readable(&extents)
    }

    /// Test-only fault injection: remove a loose content-addressed blob.
    ///
    /// Packed blobs share segment files, so this deliberately refuses packed
    /// content. Cluster tests use payloads above the packed threshold when they
    /// need to simulate one replica losing local disk content.
    pub fn debug_remove_loose_blob(&self, hash: &str) -> Result<()> {
        hash_hex(hash)?;
        let blob_path = self.blob_path(hash)?;
        let before_len = file_len_if_exists(&blob_path)?;
        if before_len.is_some() {
            if remove_file_if_exists_and_sync_parent(&blob_path)? {
                self.apply_stats_delta(ContentStoreStatsDelta::blob_file_change(before_len, None))?;
            }
            return Ok(());
        }
        if self.packed_blob_location(hash)?.is_some() {
            return Err(ContentStoreError::PackedBlob {
                hash: hash.to_owned(),
            });
        }
        Err(ContentStoreError::MissingLooseBlob {
            hash: hash.to_owned(),
        })
    }

    pub fn put_object_version_patch(
        &self,
        object_id: &str,
        base_version: &str,
        target_version: &str,
        offset: u64,
        bytes: &[u8],
    ) -> Result<ContentManifest> {
        Ok(self
            .put_object_version_patch_with_info(
                object_id,
                base_version,
                target_version,
                offset,
                bytes,
            )?
            .manifest)
    }

    pub fn put_object_version_patch_with_info(
        &self,
        object_id: &str,
        base_version: &str,
        target_version: &str,
        offset: u64,
        bytes: &[u8],
    ) -> Result<ContentPatchManifest> {
        self.put_object_version_patch_with_info_and_base_identity(
            object_id,
            base_version,
            target_version,
            offset,
            bytes,
            None,
        )
    }

    pub fn put_object_version_patch_with_info_and_base_identity(
        &self,
        object_id: &str,
        base_version: &str,
        target_version: &str,
        offset: u64,
        bytes: &[u8],
        base_identity: Option<&ContentVersionIdentity>,
    ) -> Result<ContentPatchManifest> {
        self.validate_object_version_patch_source(
            object_id,
            base_version,
            offset,
            bytes.len() as u64,
            base_identity,
        )?;
        let (patch_hash, patch_size) = if bytes.is_empty() {
            (None, 0)
        } else {
            let (hash, size) = self.write_bytes_as_blob(bytes)?;
            (Some(hash), size)
        };
        self.put_object_version_patch_from_blob(
            object_id,
            base_version,
            target_version,
            offset,
            patch_hash,
            patch_size,
            base_identity,
        )
    }

    pub(crate) fn put_object_version_patch_with_trusted_content_hash_and_base_identity(
        &self,
        object_id: &str,
        base_version: &str,
        target_version: &str,
        offset: u64,
        bytes: &[u8],
        trusted_content_hash: &str,
        base_identity: Option<&ContentVersionIdentity>,
    ) -> Result<ContentPatchManifest> {
        hash_hex(trusted_content_hash)?;
        self.validate_object_version_patch_source(
            object_id,
            base_version,
            offset,
            bytes.len() as u64,
            base_identity,
        )?;
        let (patch_hash, patch_size) = if bytes.is_empty() {
            (None, 0)
        } else {
            let (hash, size) = self.write_bytes_as_blob(bytes)?;
            (Some(hash), size)
        };
        self.put_object_version_patch_from_blob_with_trusted_content_hash(
            object_id,
            base_version,
            target_version,
            offset,
            patch_hash,
            patch_size,
            Some(trusted_content_hash),
            base_identity,
        )
    }

    pub fn put_object_version_patch_from_reader_with_info<R: Read>(
        &self,
        object_id: &str,
        base_version: &str,
        target_version: &str,
        offset: u64,
        reader: R,
    ) -> Result<ContentPatchManifest> {
        self.put_object_version_patch_from_reader_impl(
            object_id,
            base_version,
            target_version,
            offset,
            reader,
            None,
        )
    }

    pub fn put_object_version_patch_from_reader_with_info_and_base_identity<R: Read>(
        &self,
        object_id: &str,
        base_version: &str,
        target_version: &str,
        offset: u64,
        reader: R,
        base_identity: Option<&ContentVersionIdentity>,
    ) -> Result<ContentPatchManifest> {
        self.put_object_version_patch_from_reader_impl(
            object_id,
            base_version,
            target_version,
            offset,
            reader,
            base_identity,
        )
    }

    pub fn put_object_version_patch_from_reader<R: Read>(
        &self,
        object_id: &str,
        base_version: &str,
        target_version: &str,
        offset: u64,
        reader: R,
    ) -> Result<ContentManifest> {
        Ok(self
            .put_object_version_patch_from_reader_impl(
                object_id,
                base_version,
                target_version,
                offset,
                reader,
                None,
            )?
            .manifest)
    }

    pub fn put_object_version_patch_from_reader_with_base_identity<R: Read>(
        &self,
        object_id: &str,
        base_version: &str,
        target_version: &str,
        offset: u64,
        reader: R,
        base_identity: Option<&ContentVersionIdentity>,
    ) -> Result<ContentManifest> {
        Ok(self
            .put_object_version_patch_from_reader_impl(
                object_id,
                base_version,
                target_version,
                offset,
                reader,
                base_identity,
            )?
            .manifest)
    }

    fn put_object_version_patch_from_reader_impl<R: Read>(
        &self,
        object_id: &str,
        base_version: &str,
        target_version: &str,
        offset: u64,
        mut reader: R,
        base_identity: Option<&ContentVersionIdentity>,
    ) -> Result<ContentPatchManifest> {
        let staging_anchor = self
            .root
            .join("staging")
            .join("streamed-object-version-patch");
        let tmp_path = temporary_path_for(&staging_anchor);
        if let Err(error) = write_reader_to_file(&tmp_path, &mut reader) {
            remove_file_if_exists(&tmp_path).ok();
            return Err(error);
        }
        if let Err(error) = self.track_staging_file_created(&tmp_path) {
            remove_file_if_exists(&tmp_path).ok();
            return Err(error);
        }
        if let Err(error) = self.ensure_quota_delta(ContentStoreQuotaDelta::default()) {
            self.remove_tracked_staging_file_if_exists(&tmp_path).ok();
            return Err(error);
        }
        let result = self.put_object_version_patch_from_file(
            object_id,
            base_version,
            target_version,
            offset,
            &tmp_path,
            base_identity,
        );
        self.remove_tracked_staging_file_if_exists(&tmp_path).ok();
        result
    }

    fn put_object_version_patch_from_file(
        &self,
        object_id: &str,
        base_version: &str,
        target_version: &str,
        offset: u64,
        source_path: &Path,
        base_identity: Option<&ContentVersionIdentity>,
    ) -> Result<ContentPatchManifest> {
        self.put_object_version_patch_from_file_with_trusted_content_hash(
            object_id,
            base_version,
            target_version,
            offset,
            source_path,
            None,
            base_identity,
        )
    }

    fn put_object_version_patch_from_file_with_trusted_content_hash(
        &self,
        object_id: &str,
        base_version: &str,
        target_version: &str,
        offset: u64,
        source_path: &Path,
        trusted_content_hash: Option<&str>,
        base_identity: Option<&ContentVersionIdentity>,
    ) -> Result<ContentPatchManifest> {
        if let Some(trusted_content_hash) = trusted_content_hash {
            hash_hex(trusted_content_hash)?;
        }
        let (patch_hash, patch_size) = file_content_hash_and_size(source_path)?;
        self.validate_object_version_patch_source(
            object_id,
            base_version,
            offset,
            patch_size,
            base_identity,
        )?;
        if patch_size == 0 {
            return self.put_object_version_patch_from_blob_with_trusted_content_hash(
                object_id,
                base_version,
                target_version,
                offset,
                None,
                0,
                trusted_content_hash,
                base_identity,
            );
        }
        let blob_path = self.blob_path(&patch_hash)?;
        if !self.has_blob(&patch_hash)? {
            self.ensure_quota_delta(ContentStoreQuotaDelta {
                blob_bytes: projected_file_growth(&blob_path, patch_size)?,
                ..ContentStoreQuotaDelta::default()
            })?;
            self.publish_tracked_file_as_blob(&patch_hash, source_path, &blob_path, &patch_hash)?;
        }
        self.put_object_version_patch_from_blob_with_trusted_content_hash(
            object_id,
            base_version,
            target_version,
            offset,
            Some(patch_hash),
            patch_size,
            trusted_content_hash,
            base_identity,
        )
    }

    fn patch_base_manifest(
        &self,
        object_id: &str,
        base_version: &str,
        base_identity: Option<&ContentVersionIdentity>,
    ) -> Result<ContentManifest> {
        match self.read_manifest(object_id, base_version) {
            Ok(manifest) => return Ok(manifest),
            Err(error) if !content_error_is_not_found(&error) => return Err(error),
            Err(error) if base_identity.is_none() => return Err(error),
            Err(_) => {}
        }
        let base_identity = base_identity.expect("checked above");
        hash_hex(&base_identity.content_hash)?;
        let reader = self.open_blob_stream(&base_identity.content_hash)?;
        if reader.size_bytes() != base_identity.size_bytes {
            return Err(ContentStoreError::InvalidContentLayout {
                message: format!(
                    "blob-only patch base has wrong size: object={object_id}, base={base_version}, hash={}, expected={}, actual={}",
                    base_identity.content_hash,
                    base_identity.size_bytes,
                    reader.size_bytes()
                ),
            });
        }
        Ok(ContentManifest {
            object_id: object_id.to_owned(),
            version: base_version.to_owned(),
            content_hash: base_identity.content_hash.clone(),
            size_bytes: base_identity.size_bytes,
            extents: Vec::new(),
        })
    }

    fn validate_object_version_patch_source(
        &self,
        object_id: &str,
        base_version: &str,
        offset: u64,
        patch_size: u64,
        base_identity: Option<&ContentVersionIdentity>,
    ) -> Result<()> {
        let base_manifest = self.patch_base_manifest(object_id, base_version, base_identity)?;
        let base_extents = self.logical_extents_for_manifest(&base_manifest)?;
        self.ensure_extents_readable(&base_extents)?;
        if offset > base_manifest.size_bytes {
            return Err(ContentStoreError::InvalidContentLayout {
                message: format!(
                    "patch offset leaves sparse gap: object={object_id}, base={base_version}, offset={offset}, size={}",
                    base_manifest.size_bytes
                ),
            });
        }
        offset
            .checked_add(patch_size)
            .ok_or_else(|| ContentStoreError::InvalidContentLayout {
                message: format!(
                    "patch range overflows object size: object={object_id}, offset={offset}, bytes={patch_size}"
                ),
            })?;
        Ok(())
    }

    fn put_object_version_patch_from_blob(
        &self,
        object_id: &str,
        base_version: &str,
        target_version: &str,
        offset: u64,
        patch_hash: Option<String>,
        patch_size: u64,
        base_identity: Option<&ContentVersionIdentity>,
    ) -> Result<ContentPatchManifest> {
        self.put_object_version_patch_from_blob_with_trusted_content_hash(
            object_id,
            base_version,
            target_version,
            offset,
            patch_hash,
            patch_size,
            None,
            base_identity,
        )
    }

    fn put_object_version_patch_from_blob_with_trusted_content_hash(
        &self,
        object_id: &str,
        base_version: &str,
        target_version: &str,
        offset: u64,
        patch_hash: Option<String>,
        patch_size: u64,
        trusted_content_hash: Option<&str>,
        base_identity: Option<&ContentVersionIdentity>,
    ) -> Result<ContentPatchManifest> {
        if let Some(trusted_content_hash) = trusted_content_hash {
            hash_hex(trusted_content_hash)?;
        }
        let base_manifest = self.patch_base_manifest(object_id, base_version, base_identity)?;
        let base_extents = self.logical_extents_for_manifest(&base_manifest)?;
        self.ensure_extents_readable(&base_extents)?;
        if offset > base_manifest.size_bytes {
            return Err(ContentStoreError::InvalidContentLayout {
                message: format!(
                    "patch offset leaves sparse gap: object={object_id}, base={base_version}, offset={offset}, size={}",
                    base_manifest.size_bytes
                ),
            });
        }

        let patch_end =
            offset
                .checked_add(patch_size)
                .ok_or_else(|| ContentStoreError::InvalidContentLayout {
                    message: format!(
                        "patch range overflows object size: object={object_id}, offset={offset}, bytes={patch_size}"
                    ),
                })?;
        let target_size = base_manifest.size_bytes.max(patch_end);
        let mut target_extents = Vec::new();

        for extent in base_extents {
            let extent_end = extent.logical_offset.checked_add(extent.size_bytes).ok_or_else(
                || ContentStoreError::InvalidContentLayout {
                    message: format!(
                        "extent range overflows object size: object={object_id}, version={base_version}"
                    ),
                },
            )?;
            if extent.logical_offset < offset {
                let prefix_end = extent_end.min(offset);
                if prefix_end > extent.logical_offset {
                    push_extent_coalescing(
                        &mut target_extents,
                        clip_extent(&extent, extent.logical_offset, prefix_end)?,
                    )?;
                }
            }
            if extent_end > patch_end {
                let suffix_start = extent.logical_offset.max(patch_end);
                if suffix_start < extent_end && suffix_start < target_size {
                    push_extent_coalescing(
                        &mut target_extents,
                        clip_extent(&extent, suffix_start, extent_end.min(target_size))?,
                    )?;
                }
            }
        }

        if let Some(patch_hash) = patch_hash.as_ref() {
            push_extent_coalescing(
                &mut target_extents,
                ContentExtent {
                    logical_offset: offset,
                    size_bytes: patch_size,
                    content_hash: patch_hash.clone(),
                    blob_offset: 0,
                },
            )?;
        }
        target_extents.sort_by(|left, right| {
            left.logical_offset
                .cmp(&right.logical_offset)
                .then_with(|| left.blob_offset.cmp(&right.blob_offset))
        });
        let target_extents = coalesce_extents(target_extents)?;
        validate_extents_cover_size(&target_extents, target_size, object_id, target_version)?;
        let placeholder_content_hash = trusted_content_hash.unwrap_or(&base_manifest.content_hash);
        let target_manifest_for_hash = ContentManifest {
            object_id: object_id.to_owned(),
            version: target_version.to_owned(),
            content_hash: placeholder_content_hash.to_owned(),
            size_bytes: target_size,
            extents: target_extents.clone(),
        };
        let hash_tree = self.patch_hash_tree_for_extents(
            &base_manifest,
            &target_manifest_for_hash,
            offset,
            patch_size,
            trusted_content_hash,
        )?;
        let content_hash = match (hash_tree.as_ref(), trusted_content_hash) {
            (Some(tree), _) => tree.content_hash.clone(),
            (None, Some(content_hash)) => content_hash.to_owned(),
            (None, None) => self.hash_extents(&target_extents)?,
        };
        let manifest = ContentManifest {
            object_id: object_id.to_owned(),
            version: target_version.to_owned(),
            content_hash,
            size_bytes: target_size,
            extents: target_extents,
        };
        let manifest = self.write_manifest_create_new_or_existing(manifest)?;
        if let Some(hash_tree) = hash_tree {
            self.write_hash_tree_create_new_or_existing(ContentHashTree {
                content_hash: manifest.content_hash.clone(),
                ..hash_tree
            })?;
        }
        Ok(ContentPatchManifest {
            manifest,
            patch_hash,
            patch_size_bytes: patch_size,
        })
    }

    fn write_object_version(
        &self,
        object_id: &str,
        version: &str,
        bytes: &[u8],
    ) -> Result<ContentManifest> {
        let hash = content_hash(bytes);
        let manifest_path = self.manifest_path(object_id, version);
        if manifest_path.is_file() {
            let existing = decode_manifest(&read_file(&manifest_path)?)?;
            if existing.content_hash != hash {
                return Err(ContentStoreError::VersionHashConflict {
                    object_id: object_id.to_owned(),
                    version: version.to_owned(),
                    existing_hash: existing.content_hash,
                    new_hash: hash,
                });
            }
            let blob_path = self.blob_path(&hash)?;
            if !self.has_blob(&hash)? {
                self.ensure_quota_delta(ContentStoreQuotaDelta {
                    blob_bytes: projected_file_growth(&blob_path, bytes.len() as u64)?,
                    ..ContentStoreQuotaDelta::default()
                })?;
                self.write_tracked_blob_bytes(&hash, &blob_path, bytes)?;
            }
            self.ensure_content_manifest_index_best_effort(&existing);
            return Ok(existing);
        }

        let blob_path = self.blob_path(&hash)?;
        let blob_needs_write = !self.has_blob(&hash)?;
        let manifest = ContentManifest {
            object_id: object_id.to_owned(),
            version: version.to_owned(),
            content_hash: hash,
            size_bytes: bytes.len() as u64,
            extents: Vec::new(),
        };
        let manifest_bytes = encode_manifest(&manifest)?;
        self.ensure_quota_delta(ContentStoreQuotaDelta {
            blob_bytes: if blob_needs_write {
                projected_file_growth(&blob_path, bytes.len() as u64)?
            } else {
                0
            },
            logical_object_bytes: manifest.size_bytes,
            manifest_bytes: manifest_bytes.len() as u64,
            ..ContentStoreQuotaDelta::default()
        })?;
        if blob_needs_write {
            self.write_tracked_blob_bytes(&manifest.content_hash, &blob_path, bytes)?;
        }

        if let Err(error) = write_file_create_new_atomic(&manifest_path, &manifest_bytes) {
            if let ContentStoreError::Io { source, .. } = &error
                && source.kind() == io::ErrorKind::AlreadyExists
            {
                let existing = decode_manifest(&read_file(&manifest_path)?)?;
                if existing.content_hash != manifest.content_hash {
                    return Err(ContentStoreError::VersionHashConflict {
                        object_id: object_id.to_owned(),
                        version: version.to_owned(),
                        existing_hash: existing.content_hash,
                        new_hash: manifest.content_hash,
                    });
                }
                self.ensure_content_manifest_index_best_effort(&existing);
                return Ok(existing);
            }
            return Err(error);
        }
        self.apply_stats_delta(ContentStoreStatsDelta::manifest_created(
            &manifest,
            manifest_bytes.len() as u64,
        ))?;
        self.ensure_content_manifest_index_best_effort(&manifest);
        Ok(manifest)
    }

    fn publish_prehashed_object_version_file(
        &self,
        object_id: &str,
        version: &str,
        hash: String,
        size_bytes: u64,
        temporary_path: &Path,
    ) -> Result<ContentManifest> {
        let manifest_path = self.manifest_path(object_id, version);
        if manifest_path.is_file() {
            let existing = decode_manifest(&read_file(&manifest_path)?)?;
            if existing.content_hash != hash {
                return Err(ContentStoreError::VersionHashConflict {
                    object_id: object_id.to_owned(),
                    version: version.to_owned(),
                    existing_hash: existing.content_hash,
                    new_hash: hash,
                });
            }
            if existing.size_bytes != size_bytes {
                return Err(ContentStoreError::InvalidContentLayout {
                    message: format!(
                        "object {object_id} version {version} manifest size {} does not match ingested size {size_bytes}",
                        existing.size_bytes
                    ),
                });
            }
            if !self.has_blob(&existing.content_hash)? {
                let blob_path = self.blob_path(&existing.content_hash)?;
                self.ensure_quota_delta(ContentStoreQuotaDelta {
                    blob_bytes: projected_file_growth(&blob_path, size_bytes)?,
                    ..ContentStoreQuotaDelta::default()
                })?;
                self.publish_prehashed_temporary_blob(temporary_path, &blob_path)?;
            } else {
                remove_file_if_exists(temporary_path)?;
            }
            self.ensure_content_manifest_index_best_effort(&existing);
            return Ok(existing);
        }

        let blob_path = self.blob_path(&hash)?;
        let blob_needs_write = !self.has_blob(&hash)?;
        let manifest = ContentManifest {
            object_id: object_id.to_owned(),
            version: version.to_owned(),
            content_hash: hash,
            size_bytes,
            extents: Vec::new(),
        };
        let manifest_bytes = encode_manifest(&manifest)?;
        self.ensure_quota_delta(ContentStoreQuotaDelta {
            blob_bytes: if blob_needs_write {
                projected_file_growth(&blob_path, size_bytes)?
            } else {
                0
            },
            logical_object_bytes: manifest.size_bytes,
            manifest_bytes: manifest_bytes.len() as u64,
            ..ContentStoreQuotaDelta::default()
        })?;
        if blob_needs_write {
            self.publish_prehashed_temporary_blob(temporary_path, &blob_path)?;
        } else {
            remove_file_if_exists(temporary_path)?;
        }

        if let Err(error) = write_file_create_new_atomic(&manifest_path, &manifest_bytes) {
            if let ContentStoreError::Io { source, .. } = &error
                && source.kind() == io::ErrorKind::AlreadyExists
            {
                let existing = decode_manifest(&read_file(&manifest_path)?)?;
                if existing.content_hash != manifest.content_hash {
                    return Err(ContentStoreError::VersionHashConflict {
                        object_id: object_id.to_owned(),
                        version: version.to_owned(),
                        existing_hash: existing.content_hash,
                        new_hash: manifest.content_hash,
                    });
                }
                self.ensure_content_manifest_index_best_effort(&existing);
                return Ok(existing);
            }
            return Err(error);
        }
        self.apply_stats_delta(ContentStoreStatsDelta::manifest_created(
            &manifest,
            manifest_bytes.len() as u64,
        ))?;
        self.ensure_content_manifest_index_best_effort(&manifest);
        Ok(manifest)
    }

    fn publish_prehashed_temporary_blob(
        &self,
        temporary_path: &Path,
        blob_path: &Path,
    ) -> Result<()> {
        if let Some(parent) = blob_path.parent() {
            create_dir_all(parent.to_owned())?;
        }
        let before_len = file_len_if_exists(blob_path)?;
        match fs::hard_link(temporary_path, blob_path) {
            Ok(()) => {
                remove_file_if_exists(temporary_path)?;
                if let Some(parent) = blob_path.parent() {
                    sync_dir(parent)?;
                }
            }
            Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {
                remove_file_if_exists(temporary_path)?;
            }
            Err(source) => {
                return Err(ContentStoreError::Io {
                    path: blob_path.to_owned(),
                    source,
                });
            }
        }
        let after_len = file_len_if_exists(blob_path)?;
        self.apply_stats_delta(ContentStoreStatsDelta::blob_file_change(
            before_len, after_len,
        ))
    }

    fn write_object_version_blob_only(
        &self,
        object_id: &str,
        version: &str,
        bytes: &[u8],
    ) -> Result<ContentManifest> {
        let trace_started = Instant::now();
        let manifest_check_started = Instant::now();
        let manifest_path = self.manifest_path(object_id, version);
        if manifest_path.is_file() {
            let hash_started = Instant::now();
            let hash = content_hash(bytes);
            let hash_ms = hash_started.elapsed().as_millis();
            let existing = decode_manifest(&read_file(&manifest_path)?)?;
            if existing.content_hash != hash {
                return Err(ContentStoreError::VersionHashConflict {
                    object_id: object_id.to_owned(),
                    version: version.to_owned(),
                    existing_hash: existing.content_hash,
                    new_hash: hash,
                });
            }
            content_store_perf_trace(format_args!(
                "blob-only-write object={} version={} bytes={} existing_manifest=true hash_ms={} manifest_check_ms={} total_ms={}",
                object_id,
                version,
                bytes.len(),
                hash_ms,
                manifest_check_started.elapsed().as_millis(),
                trace_started.elapsed().as_millis()
            ));
            self.ensure_content_manifest_index_best_effort(&existing);
            return Ok(existing);
        }
        let manifest_check_ms = manifest_check_started.elapsed().as_millis();

        if self.uses_streamed_blob_only_path(bytes.len()) {
            return self.write_object_version_blob_only_streamed_durable(
                object_id,
                version,
                bytes,
                manifest_check_ms,
                trace_started,
            );
        }

        let hash_started = Instant::now();
        let hash = content_hash(bytes);
        let hash_ms = hash_started.elapsed().as_millis();
        let blob_path = self.blob_path(&hash)?;
        let manifest = ContentManifest {
            object_id: object_id.to_owned(),
            version: version.to_owned(),
            content_hash: hash,
            size_bytes: bytes.len() as u64,
            extents: Vec::new(),
        };

        let has_blob_started = Instant::now();
        let blob_exists = self.has_blob(&manifest.content_hash)?;
        let has_blob_ms = has_blob_started.elapsed().as_millis();
        let mut quota_ms = 0;
        let mut blob_write_ms = 0;
        if !blob_exists {
            let quota_started = Instant::now();
            self.ensure_quota_delta(ContentStoreQuotaDelta {
                blob_bytes: projected_file_growth(&blob_path, manifest.size_bytes)?,
                ..ContentStoreQuotaDelta::default()
            })?;
            quota_ms = quota_started.elapsed().as_millis();
            let blob_write_started = Instant::now();
            self.write_tracked_blob_bytes_prehashed(&manifest.content_hash, &blob_path, bytes)?;
            blob_write_ms = blob_write_started.elapsed().as_millis();
        }
        content_store_perf_trace(format_args!(
            "blob-only-write object={} version={} bytes={} existing_manifest=false blob_exists={} hash_ms={} manifest_check_ms={} has_blob_ms={} quota_ms={} blob_write_ms={} total_ms={}",
            object_id,
            version,
            bytes.len(),
            blob_exists,
            hash_ms,
            manifest_check_ms,
            has_blob_ms,
            quota_ms,
            blob_write_ms,
            trace_started.elapsed().as_millis()
        ));
        Ok(manifest)
    }

    fn write_object_version_blob_only_streamed_durable(
        &self,
        object_id: &str,
        version: &str,
        bytes: &[u8],
        manifest_check_ms: u128,
        trace_started: Instant,
    ) -> Result<ContentManifest> {
        let tmp_anchor = self.root.join("blob-tmp").join("blob-only");
        let tmp_path = temporary_path_for(&tmp_anchor);
        let result = (|| {
            if let Some(parent) = tmp_path.parent() {
                create_dir_all(parent.to_owned())?;
            }
            let stream_started = Instant::now();
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&tmp_path)
                .map_err(|source| ContentStoreError::Io {
                    path: tmp_path.clone(),
                    source,
                })?;
            let mut hasher = blake3::Hasher::new();
            for chunk in bytes.chunks(STREAMED_OBJECT_WRITE_BUFFER_BYTES) {
                hasher.update(chunk);
                file.write_all(chunk)
                    .map_err(|source| ContentStoreError::Io {
                        path: tmp_path.clone(),
                        source,
                    })?;
            }
            file.sync_all().map_err(|source| ContentStoreError::Io {
                path: tmp_path.clone(),
                source,
            })?;
            drop(file);
            let stream_hash_write_ms = stream_started.elapsed().as_millis();
            let hash = format!("{HASH_PREFIX}{}", hasher.finalize().to_hex());
            let blob_path = self.blob_path(&hash)?;
            let manifest = ContentManifest {
                object_id: object_id.to_owned(),
                version: version.to_owned(),
                content_hash: hash,
                size_bytes: bytes.len() as u64,
                extents: Vec::new(),
            };

            let create_dir_started = Instant::now();
            if let Some(parent) = blob_path.parent() {
                create_dir_all(parent.to_owned())?;
            }
            let create_dir_ms = create_dir_started.elapsed().as_millis();

            let publish_started = Instant::now();
            let mut blob_exists = false;
            if blob_path.is_file() {
                if file_content_hash(&blob_path).is_ok_and(|actual| actual == manifest.content_hash)
                {
                    remove_file_if_exists(&tmp_path)?;
                    blob_exists = true;
                } else {
                    remove_file_if_exists(&blob_path)?;
                }
            }
            if !blob_exists {
                match fs::hard_link(&tmp_path, &blob_path) {
                    Ok(()) => {
                        remove_file_if_exists(&tmp_path)?;
                        if let Some(parent) = blob_path.parent() {
                            sync_dir(parent)?;
                        }
                    }
                    Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {
                        let actual = file_content_hash(&blob_path)?;
                        if actual != manifest.content_hash {
                            return Err(ContentStoreError::BlobHashMismatch {
                                expected: manifest.content_hash,
                                actual,
                            });
                        }
                        remove_file_if_exists(&tmp_path)?;
                        blob_exists = true;
                    }
                    Err(source) => {
                        return Err(ContentStoreError::Io {
                            path: blob_path.clone(),
                            source,
                        });
                    }
                }
            }
            let publish_ms = publish_started.elapsed().as_millis();
            content_store_perf_trace(format_args!(
                "blob-only-write-streamed object={} version={} bytes={} existing_manifest=false blob_exists={} manifest_check_ms={} stream_hash_write_ms={} create_dir_ms={} publish_ms={} total_ms={}",
                object_id,
                version,
                bytes.len(),
                blob_exists,
                manifest_check_ms,
                stream_hash_write_ms,
                create_dir_ms,
                publish_ms,
                trace_started.elapsed().as_millis()
            ));
            Ok(manifest)
        })();
        if result.is_err() {
            remove_file_if_exists(&tmp_path).ok();
        }
        result
    }

    fn uses_streamed_blob_only_path(&self, bytes_len: usize) -> bool {
        self.quota.is_none()
            && !self.tracks_stats_deltas()
            && bytes_len >= STREAMED_BLOB_ONLY_MIN_BYTES
    }

    fn write_small_object_versions_batch<'a>(
        &self,
        versions: &[(&'a str, &'a str, &'a [u8])],
    ) -> Result<Vec<ContentManifest>> {
        let mut prepared = Vec::with_capacity(versions.len());
        let mut quota_delta = ContentStoreQuotaDelta::default();
        for (object_id, version, bytes) in versions {
            let hash = content_hash(bytes);
            let manifest_path = self.manifest_path(object_id, version);
            if manifest_path.is_file() {
                return versions
                    .iter()
                    .map(|(object_id, version, bytes)| {
                        self.write_object_version(object_id, version, bytes)
                    })
                    .collect();
            }
            let blob_path = self.blob_path(&hash)?;
            let manifest = ContentManifest {
                object_id: (*object_id).to_owned(),
                version: (*version).to_owned(),
                content_hash: hash,
                size_bytes: bytes.len() as u64,
                extents: Vec::new(),
            };
            let manifest_bytes = encode_manifest(&manifest)?;
            quota_delta.logical_object_bytes = quota_delta
                .logical_object_bytes
                .saturating_add(manifest.size_bytes);
            quota_delta.manifest_bytes = quota_delta
                .manifest_bytes
                .saturating_add(manifest_bytes.len() as u64);
            prepared.push(PreparedSmallObjectVersion {
                bytes,
                blob_path,
                manifest,
                manifest_bytes,
                manifest_path,
            });
        }

        let mut blob_writes = Vec::new();
        let mut invalid_loose_blobs = Vec::new();
        {
            let _lock = self.lock_packed_blobs()?;
            let mut index = self.read_packed_blob_index_for_update_unlocked()?;
            for prepared in &prepared {
                let hash = prepared.manifest.content_hash.as_str();
                if prepared.blob_path.is_file() {
                    if file_content_hash(&prepared.blob_path).is_ok_and(|actual| actual == hash) {
                        continue;
                    }
                    invalid_loose_blobs.push(prepared.blob_path.clone());
                }
                if let Some(location) = index.entries.get(hash)
                    && self.packed_blob_location_readable(location)?
                {
                    continue;
                }
                quota_delta.blob_bytes = quota_delta
                    .blob_bytes
                    .saturating_add(prepared.bytes.len() as u64);
                blob_writes.push((prepared.manifest.content_hash.clone(), prepared.bytes));
            }
            self.ensure_quota_delta(quota_delta)?;
            let blob_delta = self.write_packed_blobs_unlocked(&mut index, &blob_writes)?;
            if !blob_delta.is_zero() {
                self.apply_stats_delta(blob_delta)?;
            }
        }

        for path in invalid_loose_blobs {
            remove_file_if_exists(&path)?;
        }

        let mut manifest_delta = ContentStoreStatsDelta::default();
        write_files_create_new_relaxed(prepared.iter().map(|prepared| {
            (
                prepared.manifest_path.clone(),
                prepared.manifest_bytes.as_slice(),
            )
        }))?;
        for prepared in &prepared {
            manifest_delta.add(ContentStoreStatsDelta::manifest_created(
                &prepared.manifest,
                prepared.manifest_bytes.len() as u64,
            ));
            self.ensure_content_manifest_index_best_effort(&prepared.manifest);
        }
        self.apply_stats_delta(manifest_delta)?;
        Ok(prepared
            .into_iter()
            .map(|prepared| prepared.manifest)
            .collect())
    }

    fn write_object_version_from_file(
        &self,
        object_id: &str,
        version: &str,
        source_path: &Path,
    ) -> Result<ContentManifest> {
        let (hash, size_bytes) = file_content_hash_and_size(source_path)?;
        self.write_object_version_from_verified_file(
            object_id,
            version,
            source_path,
            hash,
            size_bytes,
        )
    }

    fn write_streamed_object_version_file<R: Read>(
        &self,
        mut reader: R,
        capture_limit_bytes: usize,
    ) -> Result<StreamedObjectVersionFile> {
        let tmp_anchor = self.root.join("blob-tmp").join("streamed-object-version");
        let tmp_path = temporary_path_for(&tmp_anchor);
        let result = (|| {
            if let Some(parent) = tmp_path.parent() {
                create_dir_all(parent.to_owned())?;
            }
            let mut file = fs::File::create(&tmp_path).map_err(|source| ContentStoreError::Io {
                path: tmp_path.clone(),
                source,
            })?;
            let mut hasher = blake3::Hasher::new();
            let mut buffer = vec![0_u8; STREAMED_OBJECT_WRITE_BUFFER_BYTES];
            let mut size_bytes = 0_u64;
            let mut captured_bytes = if capture_limit_bytes > 0 {
                Some(Vec::new())
            } else {
                None
            };
            loop {
                let read = reader
                    .read(&mut buffer)
                    .map_err(|source| ContentStoreError::Io {
                        path: tmp_path.clone(),
                        source,
                    })?;
                if read == 0 {
                    break;
                }
                size_bytes =
                    size_bytes
                        .checked_add(read as u64)
                        .ok_or_else(|| ContentStoreError::Io {
                            path: tmp_path.clone(),
                            source: io::Error::new(
                                io::ErrorKind::InvalidData,
                                "content size overflow",
                            ),
                        })?;
                hasher.update(&buffer[..read]);
                file.write_all(&buffer[..read])
                    .map_err(|source| ContentStoreError::Io {
                        path: tmp_path.clone(),
                        source,
                    })?;
                if let Some(bytes) = captured_bytes.as_mut() {
                    if bytes.len().saturating_add(read) <= capture_limit_bytes {
                        bytes.extend_from_slice(&buffer[..read]);
                    } else {
                        captured_bytes = None;
                    }
                }
            }
            file.sync_data().map_err(|source| ContentStoreError::Io {
                path: tmp_path.clone(),
                source,
            })?;
            Ok(StreamedObjectVersionFile {
                path: tmp_path.clone(),
                hash: format!("{HASH_PREFIX}{}", hasher.finalize().to_hex()),
                size_bytes,
                captured_bytes,
            })
        })();
        if result.is_err() {
            remove_file_if_exists(&tmp_path).ok();
        }
        result
    }

    fn write_object_version_from_verified_file(
        &self,
        object_id: &str,
        version: &str,
        source_path: &Path,
        hash: String,
        size_bytes: u64,
    ) -> Result<ContentManifest> {
        let manifest_path = self.manifest_path(object_id, version);
        if manifest_path.is_file() {
            let existing = decode_manifest(&read_file(&manifest_path)?)?;
            if existing.content_hash != hash {
                return Err(ContentStoreError::VersionHashConflict {
                    object_id: object_id.to_owned(),
                    version: version.to_owned(),
                    existing_hash: existing.content_hash,
                    new_hash: hash,
                });
            }
            let blob_path = self.blob_path(&hash)?;
            if !self.has_blob(&hash)? {
                self.ensure_quota_delta(ContentStoreQuotaDelta {
                    blob_bytes: projected_file_growth(&blob_path, size_bytes)?,
                    ..ContentStoreQuotaDelta::default()
                })?;
                self.publish_tracked_file_as_blob(&hash, source_path, &blob_path, &hash)?;
            }
            self.ensure_content_manifest_index_best_effort(&existing);
            return Ok(existing);
        }

        let blob_path = self.blob_path(&hash)?;
        let blob_needs_publish = !self.has_blob(&hash)?;
        let manifest = ContentManifest {
            object_id: object_id.to_owned(),
            version: version.to_owned(),
            content_hash: hash,
            size_bytes,
            extents: Vec::new(),
        };
        let manifest_bytes = encode_manifest(&manifest)?;
        self.ensure_quota_delta(ContentStoreQuotaDelta {
            blob_bytes: if blob_needs_publish {
                projected_file_growth(&blob_path, size_bytes)?
            } else {
                0
            },
            logical_object_bytes: manifest.size_bytes,
            manifest_bytes: manifest_bytes.len() as u64,
            ..ContentStoreQuotaDelta::default()
        })?;
        // The quota check needs to know whether this publish grows durable blob
        // storage, so we preflight the content-addressed blob before linking it.
        if blob_needs_publish {
            self.publish_tracked_file_as_blob(
                &manifest.content_hash,
                source_path,
                &blob_path,
                &manifest.content_hash,
            )?;
        }
        if let Err(error) = write_file_create_new_atomic(&manifest_path, &manifest_bytes) {
            if let ContentStoreError::Io { source, .. } = &error
                && source.kind() == io::ErrorKind::AlreadyExists
            {
                let existing = decode_manifest(&read_file(&manifest_path)?)?;
                if existing.content_hash != manifest.content_hash {
                    return Err(ContentStoreError::VersionHashConflict {
                        object_id: object_id.to_owned(),
                        version: version.to_owned(),
                        existing_hash: existing.content_hash,
                        new_hash: manifest.content_hash,
                    });
                }
                self.ensure_content_manifest_index_best_effort(&existing);
                return Ok(existing);
            }
            return Err(error);
        }
        self.apply_stats_delta(ContentStoreStatsDelta::manifest_created(
            &manifest,
            manifest_bytes.len() as u64,
        ))?;
        self.ensure_content_manifest_index_best_effort(&manifest);
        Ok(manifest)
    }

    fn collect_version_manifest_entries(
        &self,
        path: &Path,
        versions_by_object: &mut BTreeMap<String, Vec<VersionManifestEntry>>,
    ) -> Result<()> {
        if !path.exists() {
            return Ok(());
        }
        for entry in read_dir(path)? {
            let entry_path = entry.path();
            if entry_path.is_dir() {
                self.collect_version_manifest_entries(&entry_path, versions_by_object)?;
                continue;
            }
            if entry_path.file_name().and_then(|name| name.to_str()) != Some("manifest.sobj") {
                continue;
            }
            let manifest_bytes = read_file(&entry_path)?;
            let manifest = decode_manifest(&manifest_bytes)?;
            let modified = entry_path
                .metadata()
                .and_then(|metadata| metadata.modified())
                .map_err(|source| ContentStoreError::Io {
                    path: entry_path.clone(),
                    source,
                })?;
            let version_dir = entry_path
                .parent()
                .map(Path::to_owned)
                .unwrap_or_else(|| entry_path.clone());
            let object_dir = version_dir
                .parent()
                .map(Path::to_owned)
                .unwrap_or_else(|| version_dir.clone());
            versions_by_object
                .entry(manifest.object_id.clone())
                .or_default()
                .push(VersionManifestEntry {
                    object_id: manifest.object_id,
                    version: manifest.version,
                    content_hash: manifest.content_hash,
                    path: entry_path,
                    version_dir,
                    object_dir,
                    modified_nanos: system_time_nanos_since_epoch_saturating(modified),
                    manifest_bytes: manifest_bytes.len() as u64,
                    logical_bytes: manifest.size_bytes,
                    extent_count: manifest.extents.len() as u64,
                });
        }
        Ok(())
    }

    fn scrub_manifest_tree(
        &self,
        path: &Path,
        report: &mut ContentStoreScrubReport,
        referenced_blobs: &mut BTreeSet<String>,
        checked_blobs: &mut BTreeMap<String, ScrubbedBlob>,
    ) -> Result<()> {
        if !path.exists() {
            return Ok(());
        }
        for entry in read_dir(path)? {
            let entry_path = entry.path();
            if entry_path.is_dir() {
                self.scrub_manifest_tree(&entry_path, report, referenced_blobs, checked_blobs)?;
                continue;
            }
            if entry_path.file_name().and_then(|name| name.to_str()) != Some("manifest.sobj") {
                continue;
            }
            self.scrub_manifest(&entry_path, report, referenced_blobs, checked_blobs)?;
        }
        Ok(())
    }

    fn scrub_manifest(
        &self,
        path: &Path,
        report: &mut ContentStoreScrubReport,
        referenced_blobs: &mut BTreeSet<String>,
        checked_blobs: &mut BTreeMap<String, ScrubbedBlob>,
    ) -> Result<()> {
        let manifest_bytes = match read_file(path) {
            Ok(bytes) => bytes,
            Err(error) => {
                report.issues.push(ContentStoreScrubIssue::InvalidManifest {
                    path: path.to_owned(),
                    message: error.to_string(),
                });
                return Ok(());
            }
        };
        report.stats.manifest_count = report.stats.manifest_count.saturating_add(1);
        report.stats.manifest_bytes = report
            .stats
            .manifest_bytes
            .saturating_add(manifest_bytes.len() as u64);
        let manifest = match decode_manifest(&manifest_bytes) {
            Ok(manifest) => manifest,
            Err(error) => {
                report.issues.push(ContentStoreScrubIssue::InvalidManifest {
                    path: path.to_owned(),
                    message: error.to_string(),
                });
                return Ok(());
            }
        };
        report.stats.logical_object_bytes = report
            .stats
            .logical_object_bytes
            .saturating_add(manifest.size_bytes);
        if manifest.extents.is_empty() {
            self.scrub_blob_reference(
                path,
                &manifest.object_id,
                &manifest.version,
                &manifest.content_hash,
                0,
                manifest.size_bytes,
                true,
                report,
                referenced_blobs,
                checked_blobs,
            )?;
            return Ok(());
        }

        report.stats.extent_manifest_count = report.stats.extent_manifest_count.saturating_add(1);
        report.stats.extent_count = report
            .stats
            .extent_count
            .saturating_add(manifest.extents.len() as u64);
        if let Err(error) = validate_extents_cover_size(
            &manifest.extents,
            manifest.size_bytes,
            &manifest.object_id,
            &manifest.version,
        ) {
            report.issues.push(ContentStoreScrubIssue::InvalidManifest {
                path: path.to_owned(),
                message: error.to_string(),
            });
        }
        for extent in &manifest.extents {
            self.scrub_blob_reference(
                path,
                &manifest.object_id,
                &manifest.version,
                &extent.content_hash,
                extent.blob_offset,
                extent.size_bytes,
                false,
                report,
                referenced_blobs,
                checked_blobs,
            )?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn scrub_blob_reference(
        &self,
        manifest_path: &Path,
        object_id: &str,
        version: &str,
        content_hash: &str,
        blob_offset: u64,
        size_bytes: u64,
        require_exact_size: bool,
        report: &mut ContentStoreScrubReport,
        referenced_blobs: &mut BTreeSet<String>,
        checked_blobs: &mut BTreeMap<String, ScrubbedBlob>,
    ) -> Result<()> {
        if let Err(error) = hash_hex(content_hash) {
            report.issues.push(ContentStoreScrubIssue::InvalidManifest {
                path: manifest_path.to_owned(),
                message: error.to_string(),
            });
            return Ok(());
        }
        referenced_blobs.insert(content_hash.to_owned());
        let blob_path = self.blob_path(content_hash)?;
        if !blob_path.is_file() {
            let Some(location) = self.packed_blob_location(content_hash)? else {
                report.issues.push(ContentStoreScrubIssue::MissingBlob {
                    object_id: object_id.to_owned(),
                    version: version.to_owned(),
                    content_hash: content_hash.to_owned(),
                    path: blob_path,
                });
                return Ok(());
            };
            if !checked_blobs.contains_key(content_hash) {
                let segment_path = self.packed_blob_segment_path(&location.segment_id);
                let mut reader = match self.open_packed_blob_reader(&location, 0, usize::MAX) {
                    Ok(reader) => reader,
                    Err(error) => {
                        report.issues.push(ContentStoreScrubIssue::MissingBlob {
                            object_id: object_id.to_owned(),
                            version: version.to_owned(),
                            content_hash: content_hash.to_owned(),
                            path: segment_path,
                        });
                        report.issues.push(ContentStoreScrubIssue::InvalidManifest {
                            path: manifest_path.to_owned(),
                            message: error.to_string(),
                        });
                        return Ok(());
                    }
                };
                let mut bytes =
                    Vec::with_capacity(usize::try_from(location.size_bytes).unwrap_or(usize::MAX));
                if let Err(source) = reader.read_to_end(&mut bytes) {
                    return Err(ContentStoreError::Io {
                        path: segment_path,
                        source,
                    });
                }
                checked_blobs.insert(
                    content_hash.to_owned(),
                    ScrubbedBlob {
                        actual_hash: crate::content_store::content_hash(&bytes),
                        size_bytes: bytes.len() as u64,
                    },
                );
            }
            let checked = checked_blobs
                .get(content_hash)
                .expect("checked packed blob inserted before lookup");
            if checked.actual_hash != content_hash {
                report.issues.push(ContentStoreScrubIssue::CorruptBlob {
                    object_id: object_id.to_owned(),
                    version: version.to_owned(),
                    content_hash: content_hash.to_owned(),
                    path: self.packed_blob_segment_path(&location.segment_id),
                    actual_hash: checked.actual_hash.clone(),
                });
                return Ok(());
            }
            if require_exact_size && checked.size_bytes != size_bytes {
                report
                    .issues
                    .push(ContentStoreScrubIssue::BlobSizeMismatch {
                        object_id: object_id.to_owned(),
                        version: version.to_owned(),
                        content_hash: content_hash.to_owned(),
                        path: self.packed_blob_segment_path(&location.segment_id),
                        expected_size_bytes: size_bytes,
                        actual_size_bytes: checked.size_bytes,
                    });
                return Ok(());
            }
            let extent_end = blob_offset.checked_add(size_bytes);
            if extent_end.is_none_or(|end| end > checked.size_bytes) {
                report
                    .issues
                    .push(ContentStoreScrubIssue::BlobRangeOutOfBounds {
                        object_id: object_id.to_owned(),
                        version: version.to_owned(),
                        content_hash: content_hash.to_owned(),
                        path: self.packed_blob_segment_path(&location.segment_id),
                        blob_offset,
                        size_bytes,
                        blob_size_bytes: checked.size_bytes,
                    });
            }
            return Ok(());
        }
        if !checked_blobs.contains_key(content_hash) {
            let (actual_hash, blob_size_bytes) = file_content_hash_and_size(&blob_path)?;
            checked_blobs.insert(
                content_hash.to_owned(),
                ScrubbedBlob {
                    actual_hash,
                    size_bytes: blob_size_bytes,
                },
            );
        }
        let checked = checked_blobs
            .get(content_hash)
            .expect("checked blob inserted before lookup");
        if checked.actual_hash != content_hash {
            report.issues.push(ContentStoreScrubIssue::CorruptBlob {
                object_id: object_id.to_owned(),
                version: version.to_owned(),
                content_hash: content_hash.to_owned(),
                path: blob_path,
                actual_hash: checked.actual_hash.clone(),
            });
            return Ok(());
        }
        if require_exact_size && checked.size_bytes != size_bytes {
            report
                .issues
                .push(ContentStoreScrubIssue::BlobSizeMismatch {
                    object_id: object_id.to_owned(),
                    version: version.to_owned(),
                    content_hash: content_hash.to_owned(),
                    path: blob_path,
                    expected_size_bytes: size_bytes,
                    actual_size_bytes: checked.size_bytes,
                });
            return Ok(());
        }
        let extent_end = blob_offset.checked_add(size_bytes);
        if extent_end.is_none_or(|end| end > checked.size_bytes) {
            report
                .issues
                .push(ContentStoreScrubIssue::BlobRangeOutOfBounds {
                    object_id: object_id.to_owned(),
                    version: version.to_owned(),
                    content_hash: content_hash.to_owned(),
                    path: blob_path,
                    blob_offset,
                    size_bytes,
                    blob_size_bytes: checked.size_bytes,
                });
        }
        Ok(())
    }

    fn collect_orphan_blob_issues(
        &self,
        path: &Path,
        referenced_blobs: &BTreeSet<String>,
        report: &mut ContentStoreScrubReport,
    ) -> Result<()> {
        if !path.exists() {
            return Ok(());
        }
        for entry in read_dir(path)? {
            let entry_path = entry.path();
            if entry_path.is_dir() {
                self.collect_orphan_blob_issues(&entry_path, referenced_blobs, report)?;
                continue;
            }
            if !entry_path.is_file() {
                continue;
            }
            let Some(hex) = entry_path.file_name().and_then(|name| name.to_str()) else {
                report
                    .issues
                    .push(ContentStoreScrubIssue::InvalidBlobPath { path: entry_path });
                continue;
            };
            let content_hash = format!("{HASH_PREFIX}{hex}");
            if hash_hex(&content_hash).is_err() {
                report
                    .issues
                    .push(ContentStoreScrubIssue::InvalidBlobPath { path: entry_path });
                continue;
            }
            if referenced_blobs.contains(&content_hash) {
                continue;
            }
            let size_bytes = entry_path
                .metadata()
                .map_err(|source| ContentStoreError::Io {
                    path: entry_path.clone(),
                    source,
                })?
                .len();
            report.orphan_blob_count = report.orphan_blob_count.saturating_add(1);
            report.orphan_blob_bytes = report.orphan_blob_bytes.saturating_add(size_bytes);
            report.issues.push(ContentStoreScrubIssue::OrphanBlob {
                content_hash,
                path: entry_path,
                size_bytes,
            });
        }
        Ok(())
    }

    fn collect_orphan_packed_blob_issues(
        &self,
        referenced_blobs: &BTreeSet<String>,
        report: &mut ContentStoreScrubReport,
    ) -> Result<()> {
        let index = self.read_packed_blob_index_unlocked()?;
        for (content_hash, location) in index.entries {
            if referenced_blobs.contains(&content_hash) {
                continue;
            }
            let path = self.packed_blob_segment_path(&location.segment_id);
            report.orphan_blob_count = report.orphan_blob_count.saturating_add(1);
            report.orphan_blob_bytes = report.orphan_blob_bytes.saturating_add(location.size_bytes);
            report
                .issues
                .push(ContentStoreScrubIssue::OrphanPackedBlob {
                    content_hash,
                    segment_id: location.segment_id,
                    offset: location.offset,
                    path,
                    size_bytes: location.size_bytes,
                });
        }
        Ok(())
    }

    fn read_manifest_range(
        &self,
        manifest: &ContentManifest,
        offset: u64,
        max_bytes: usize,
    ) -> Result<Vec<u8>> {
        if manifest.extents.is_empty() {
            return self.read_blob_range(&manifest.content_hash, offset, max_bytes);
        }

        let extents = self.logical_extents_for_manifest(manifest)?;
        let start = offset.min(manifest.size_bytes);
        let max_bytes = u64::try_from(max_bytes).unwrap_or(u64::MAX);
        let end = start.saturating_add(max_bytes).min(manifest.size_bytes);
        let capacity = usize::try_from(end.saturating_sub(start)).map_err(|_| {
            ContentStoreError::InvalidContentLayout {
                message: format!(
                    "object range is too large to buffer: object={}, version={}, range={}..{}",
                    manifest.object_id, manifest.version, start, end
                ),
            }
        })?;
        let mut bytes = Vec::with_capacity(capacity);
        for extent in extents {
            let extent_end = extent
                .logical_offset
                .checked_add(extent.size_bytes)
                .ok_or_else(|| ContentStoreError::InvalidContentLayout {
                    message: format!(
                        "extent range overflows object size: object={}, version={}",
                        manifest.object_id, manifest.version
                    ),
                })?;
            let range_start = start.max(extent.logical_offset);
            let range_end = end.min(extent_end);
            if range_start >= range_end {
                continue;
            }
            let extent_delta = range_start - extent.logical_offset;
            let blob_offset = extent
                .blob_offset
                .checked_add(extent_delta)
                .ok_or_else(|| ContentStoreError::InvalidContentLayout {
                    message: format!(
                        "extent blob offset overflows: object={}, version={}",
                        manifest.object_id, manifest.version
                    ),
                })?;
            let read_len = usize::try_from(range_end - range_start).map_err(|_| {
                ContentStoreError::InvalidContentLayout {
                    message: format!(
                        "extent range is too large to buffer: object={}, version={}",
                        manifest.object_id, manifest.version
                    ),
                }
            })?;
            let part = self.read_blob_range(&extent.content_hash, blob_offset, read_len)?;
            if part.len() != read_len {
                return Err(ContentStoreError::InvalidContentLayout {
                    message: format!(
                        "extent range shorter than manifest: object={}, version={}, expected={}, actual={}",
                        manifest.object_id,
                        manifest.version,
                        read_len,
                        part.len()
                    ),
                });
            }
            bytes.extend_from_slice(&part);
        }
        Ok(bytes)
    }

    fn logical_extents_for_manifest(
        &self,
        manifest: &ContentManifest,
    ) -> Result<Vec<ContentExtent>> {
        hash_hex(&manifest.content_hash)?;
        if manifest.extents.is_empty() {
            return Ok(vec![ContentExtent {
                logical_offset: 0,
                size_bytes: manifest.size_bytes,
                content_hash: manifest.content_hash.clone(),
                blob_offset: 0,
            }]);
        }
        let mut extents = manifest.extents.clone();
        extents.sort_by(|left, right| {
            left.logical_offset
                .cmp(&right.logical_offset)
                .then_with(|| left.blob_offset.cmp(&right.blob_offset))
        });
        validate_extents_cover_size(
            &extents,
            manifest.size_bytes,
            &manifest.object_id,
            &manifest.version,
        )?;
        Ok(extents)
    }

    fn ensure_extents_readable(&self, extents: &[ContentExtent]) -> Result<()> {
        if !self.extents_readable(extents)? {
            return Err(ContentStoreError::InvalidContentLayout {
                message: "manifest references missing or truncated content extents".to_string(),
            });
        }
        Ok(())
    }

    fn extents_readable(&self, extents: &[ContentExtent]) -> Result<bool> {
        for extent in extents {
            if !self.extent_range_readable(extent)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn extent_range_readable(&self, extent: &ContentExtent) -> Result<bool> {
        let required_end = extent
            .blob_offset
            .checked_add(extent.size_bytes)
            .ok_or_else(|| ContentStoreError::InvalidContentLayout {
                message: format!(
                    "extent blob range overflows for content hash {}",
                    extent.content_hash
                ),
            })?;
        Ok(self
            .readable_blob_size(&extent.content_hash)?
            .is_some_and(|size_bytes| required_end <= size_bytes))
    }

    fn ensure_extent_subrange_readable(
        &self,
        extent: &ContentExtent,
        logical_start: u64,
        logical_end: u64,
    ) -> Result<()> {
        let extent_delta = logical_start
            .checked_sub(extent.logical_offset)
            .ok_or_else(|| ContentStoreError::InvalidContentLayout {
                message: "extent range starts before its logical offset".to_string(),
            })?;
        let blob_start = extent
            .blob_offset
            .checked_add(extent_delta)
            .ok_or_else(|| ContentStoreError::InvalidContentLayout {
                message: format!(
                    "extent blob offset overflows for content hash {}",
                    extent.content_hash
                ),
            })?;
        let required_end = blob_start
            .checked_add(logical_end.saturating_sub(logical_start))
            .ok_or_else(|| ContentStoreError::InvalidContentLayout {
                message: format!(
                    "extent blob range overflows for content hash {}",
                    extent.content_hash
                ),
            })?;
        if !self
            .readable_blob_size(&extent.content_hash)?
            .is_some_and(|size_bytes| required_end <= size_bytes)
        {
            return Err(ContentStoreError::InvalidContentLayout {
                message: "manifest references missing or truncated content extents".to_string(),
            });
        }
        Ok(())
    }

    fn readable_blob_size(&self, hash: &str) -> Result<Option<u64>> {
        let path = self.blob_path(hash)?;
        if let Some(size_bytes) = file_len_if_exists(&path)? {
            return Ok(Some(size_bytes));
        }
        let Some(location) = self.packed_blob_location(hash)? else {
            return Ok(None);
        };
        if !self.packed_blob_location_readable(&location)? {
            return Ok(None);
        }
        Ok(Some(location.size_bytes))
    }

    fn read_hash_tree_optional(
        &self,
        object_id: &str,
        version: &str,
    ) -> Result<Option<ContentHashTree>> {
        let path = self.hash_tree_path(object_id, version);
        if !path.is_file() {
            return Ok(None);
        }
        Ok(Some(decode_hash_tree(&read_file(&path)?)?))
    }

    fn write_hash_tree_create_new_or_existing(
        &self,
        tree: ContentHashTree,
    ) -> Result<ContentHashTree> {
        let path = self.hash_tree_path(&tree.object_id, &tree.object_version);
        if path.is_file() {
            let existing = decode_hash_tree(&read_file(&path)?)?;
            self.validate_hash_tree_for_manifest_fields(
                &existing,
                &tree.object_id,
                &tree.object_version,
                tree.size_bytes,
                &tree.content_hash,
            )?;
            return Ok(existing);
        }
        if let Some(parent) = path.parent() {
            create_dir_all(parent.to_path_buf())?;
        }
        let bytes = encode_hash_tree(&tree)?;
        if let Err(error) = write_file_create_new_atomic(&path, &bytes) {
            if let ContentStoreError::Io { source, .. } = &error
                && source.kind() == io::ErrorKind::AlreadyExists
            {
                let existing = decode_hash_tree(&read_file(&path)?)?;
                self.validate_hash_tree_for_manifest_fields(
                    &existing,
                    &tree.object_id,
                    &tree.object_version,
                    tree.size_bytes,
                    &tree.content_hash,
                )?;
                return Ok(existing);
            }
            return Err(error);
        }
        Ok(tree)
    }

    fn validate_hash_tree_for_manifest_fields(
        &self,
        tree: &ContentHashTree,
        object_id: &str,
        version: &str,
        size_bytes: u64,
        content_hash: &str,
    ) -> Result<()> {
        hash_hex(&tree.content_hash)?;
        if tree.version != CONTENT_HASH_TREE_VERSION
            || tree.object_id != object_id
            || tree.object_version != version
            || tree.size_bytes != size_bytes
            || tree.content_hash != content_hash
            || tree.leaf_bytes != CONTENT_HASH_TREE_LEAF_BYTES
        {
            return Err(ContentStoreError::InvalidContentLayout {
                message: format!(
                    "content hash tree does not match manifest: object={object_id}, version={version}"
                ),
            });
        }
        let expected_leaves = content_hash_tree_leaf_count(size_bytes)?;
        if tree.leaves.len() != expected_leaves {
            return Err(ContentStoreError::InvalidContentLayout {
                message: format!(
                    "content hash tree leaf count mismatch: object={object_id}, version={version}, expected={expected_leaves}, actual={}",
                    tree.leaves.len()
                ),
            });
        }
        for (index, leaf) in tree.leaves.iter().enumerate() {
            let (expected_offset, expected_size) = content_hash_tree_leaf_range(size_bytes, index)?;
            if leaf.logical_offset != expected_offset || leaf.size_bytes != expected_size {
                return Err(ContentStoreError::InvalidContentLayout {
                    message: format!(
                        "content hash tree leaf shape mismatch: object={object_id}, version={version}, leaf={index}"
                    ),
                });
            }
        }
        Ok(())
    }

    fn hash_tree_for_manifest(&self, manifest: &ContentManifest) -> Result<ContentHashTree> {
        if let Some(tree) = self.read_hash_tree_optional(&manifest.object_id, &manifest.version)? {
            self.validate_hash_tree_for_manifest_fields(
                &tree,
                &manifest.object_id,
                &manifest.version,
                manifest.size_bytes,
                &manifest.content_hash,
            )?;
            return Ok(tree);
        }
        let tree = self.build_hash_tree_for_manifest(manifest, &manifest.content_hash)?;
        self.write_hash_tree_create_new_or_existing(tree)
    }

    fn build_hash_tree_for_manifest(
        &self,
        manifest: &ContentManifest,
        expected_content_hash: &str,
    ) -> Result<ContentHashTree> {
        hash_hex(expected_content_hash)?;
        let leaf_count = content_hash_tree_leaf_count(manifest.size_bytes)?;
        let mut leaves = Vec::with_capacity(leaf_count);
        for index in 0..leaf_count {
            let (logical_offset, size_bytes) =
                content_hash_tree_leaf_range(manifest.size_bytes, index)?;
            let bytes = self.read_manifest_range(
                manifest,
                logical_offset,
                usize::try_from(size_bytes).map_err(|_| {
                    ContentStoreError::InvalidContentLayout {
                        message: format!(
                            "content hash tree leaf is too large to buffer: object={}, version={}, bytes={size_bytes}",
                            manifest.object_id, manifest.version
                        ),
                    }
                })?,
            )?;
            if bytes.len() as u64 != size_bytes {
                return Err(ContentStoreError::InvalidContentLayout {
                    message: format!(
                        "content hash tree leaf read was short: object={}, version={}, expected={size_bytes}, actual={}",
                        manifest.object_id,
                        manifest.version,
                        bytes.len()
                    ),
                });
            }
            leaves.push(ContentHashTreeLeaf {
                logical_offset,
                size_bytes,
                chaining_value: hash_tree_leaf_chaining_value(logical_offset, &bytes),
            });
        }
        let content_hash = content_hash_from_hash_tree_leaves(manifest.size_bytes, &leaves)?;
        if content_hash != expected_content_hash {
            return Err(ContentStoreError::BlobHashMismatch {
                expected: expected_content_hash.to_owned(),
                actual: content_hash,
            });
        }
        Ok(ContentHashTree {
            version: CONTENT_HASH_TREE_VERSION,
            object_id: manifest.object_id.clone(),
            object_version: manifest.version.clone(),
            size_bytes: manifest.size_bytes,
            content_hash: expected_content_hash.to_owned(),
            leaf_bytes: CONTENT_HASH_TREE_LEAF_BYTES,
            leaves,
        })
    }

    fn patch_hash_tree_for_extents(
        &self,
        base_manifest: &ContentManifest,
        target_manifest: &ContentManifest,
        offset: u64,
        patch_size: u64,
        trusted_content_hash: Option<&str>,
    ) -> Result<Option<ContentHashTree>> {
        if target_manifest.size_bytes <= CONTENT_HASH_TREE_LEAF_BYTES {
            return Ok(None);
        }
        let base_tree = if base_manifest.size_bytes > CONTENT_HASH_TREE_LEAF_BYTES {
            Some(self.hash_tree_for_manifest(base_manifest)?)
        } else {
            None
        };
        let leaf_count = content_hash_tree_leaf_count(target_manifest.size_bytes)?;
        let mut leaves = Vec::with_capacity(leaf_count);
        let patch_end =
            offset
                .checked_add(patch_size)
                .ok_or_else(|| ContentStoreError::InvalidContentLayout {
                    message: format!(
                        "patch range overflows object size: object={}, offset={offset}, bytes={patch_size}",
                        target_manifest.object_id
                    ),
                })?;
        for index in 0..leaf_count {
            let (logical_offset, size_bytes) =
                content_hash_tree_leaf_range(target_manifest.size_bytes, index)?;
            let leaf_end = logical_offset.checked_add(size_bytes).ok_or_else(|| {
                ContentStoreError::InvalidContentLayout {
                    message: format!(
                        "content hash tree leaf range overflows: object={}, version={}",
                        target_manifest.object_id, target_manifest.version
                    ),
                }
            })?;
            let overlaps_patch = logical_offset < patch_end && leaf_end > offset;
            let reusable = base_tree.as_ref().and_then(|tree| {
                tree.leaves.get(index).filter(|leaf| {
                    !overlaps_patch
                        && leaf.logical_offset == logical_offset
                        && leaf.size_bytes == size_bytes
                })
            });
            if let Some(leaf) = reusable {
                leaves.push(leaf.clone());
                continue;
            }
            let bytes = self.read_manifest_range(
                target_manifest,
                logical_offset,
                usize::try_from(size_bytes).map_err(|_| {
                    ContentStoreError::InvalidContentLayout {
                        message: format!(
                            "content hash tree patch leaf is too large to buffer: object={}, version={}, bytes={size_bytes}",
                            target_manifest.object_id, target_manifest.version
                        ),
                    }
                })?,
            )?;
            if bytes.len() as u64 != size_bytes {
                return Err(ContentStoreError::InvalidContentLayout {
                    message: format!(
                        "content hash tree patch leaf read was short: object={}, version={}, expected={size_bytes}, actual={}",
                        target_manifest.object_id,
                        target_manifest.version,
                        bytes.len()
                    ),
                });
            }
            leaves.push(ContentHashTreeLeaf {
                logical_offset,
                size_bytes,
                chaining_value: hash_tree_leaf_chaining_value(logical_offset, &bytes),
            });
        }
        let content_hash = content_hash_from_hash_tree_leaves(target_manifest.size_bytes, &leaves)?;
        if let Some(trusted_content_hash) = trusted_content_hash
            && content_hash != trusted_content_hash
        {
            return Err(ContentStoreError::BlobHashMismatch {
                expected: trusted_content_hash.to_owned(),
                actual: content_hash,
            });
        }
        Ok(Some(ContentHashTree {
            version: CONTENT_HASH_TREE_VERSION,
            object_id: target_manifest.object_id.clone(),
            object_version: target_manifest.version.clone(),
            size_bytes: target_manifest.size_bytes,
            content_hash,
            leaf_bytes: CONTENT_HASH_TREE_LEAF_BYTES,
            leaves,
        }))
    }

    fn write_bytes_as_blob(&self, bytes: &[u8]) -> Result<(String, u64)> {
        let hash = content_hash(bytes);
        let blob_path = self.blob_path(&hash)?;
        let blob_needs_write = !self.has_blob(&hash)?;
        if blob_needs_write {
            self.ensure_quota_delta(ContentStoreQuotaDelta {
                blob_bytes: projected_file_growth(&blob_path, bytes.len() as u64)?,
                ..ContentStoreQuotaDelta::default()
            })?;
            self.write_tracked_blob_bytes(&hash, &blob_path, bytes)?;
        }
        Ok((hash, bytes.len() as u64))
    }

    fn hash_extents(&self, extents: &[ContentExtent]) -> Result<String> {
        let mut hasher = blake3::Hasher::new();
        let mut buffer = [0_u8; 64 * 1024];
        for extent in extents {
            let max_bytes = usize::try_from(extent.size_bytes).map_err(|_| {
                ContentStoreError::InvalidContentLayout {
                    message: format!(
                        "extent is too large to hash in this process: {} bytes",
                        extent.size_bytes
                    ),
                }
            })?;
            let mut reader =
                self.open_blob_range_stream(&extent.content_hash, extent.blob_offset, max_bytes)?;
            if reader.remaining_bytes() != extent.size_bytes {
                return Err(ContentStoreError::InvalidContentLayout {
                    message: format!(
                        "extent source is shorter than manifest: hash={}, expected={}, actual={}",
                        extent.content_hash,
                        extent.size_bytes,
                        reader.remaining_bytes()
                    ),
                });
            }
            let blob_path = self.blob_path(&extent.content_hash)?;
            loop {
                let read = reader
                    .read(&mut buffer)
                    .map_err(|source| ContentStoreError::Io {
                        path: blob_path.clone(),
                        source,
                    })?;
                if read == 0 {
                    break;
                }
                hasher.update(&buffer[..read]);
            }
        }
        Ok(format!("{HASH_PREFIX}{}", hasher.finalize().to_hex()))
    }

    fn write_manifest_create_new_or_existing(
        &self,
        manifest: ContentManifest,
    ) -> Result<ContentManifest> {
        let manifest_path = self.manifest_path(&manifest.object_id, &manifest.version);
        if manifest_path.is_file() {
            let existing = decode_manifest(&read_file(&manifest_path)?)?;
            if existing.content_hash != manifest.content_hash
                || existing.size_bytes != manifest.size_bytes
            {
                return Err(ContentStoreError::VersionHashConflict {
                    object_id: manifest.object_id,
                    version: manifest.version,
                    existing_hash: existing.content_hash,
                    new_hash: manifest.content_hash,
                });
            }
            self.ensure_content_manifest_index_best_effort(&existing);
            return Ok(existing);
        }
        let manifest_bytes = encode_manifest(&manifest)?;
        self.ensure_quota_delta(ContentStoreQuotaDelta {
            logical_object_bytes: manifest.size_bytes,
            manifest_bytes: manifest_bytes.len() as u64,
            ..ContentStoreQuotaDelta::default()
        })?;
        if let Err(error) = write_file_create_new_atomic(&manifest_path, &manifest_bytes) {
            if let ContentStoreError::Io { source, .. } = &error
                && source.kind() == io::ErrorKind::AlreadyExists
            {
                let existing = decode_manifest(&read_file(&manifest_path)?)?;
                if existing.content_hash != manifest.content_hash
                    || existing.size_bytes != manifest.size_bytes
                {
                    return Err(ContentStoreError::VersionHashConflict {
                        object_id: manifest.object_id,
                        version: manifest.version,
                        existing_hash: existing.content_hash,
                        new_hash: manifest.content_hash,
                    });
                }
                self.ensure_content_manifest_index_best_effort(&existing);
                return Ok(existing);
            }
            return Err(error);
        }
        self.apply_stats_delta(ContentStoreStatsDelta::manifest_created(
            &manifest,
            manifest_bytes.len() as u64,
        ))?;
        self.ensure_content_manifest_index_best_effort(&manifest);
        Ok(manifest)
    }

    fn ensure_content_manifest_index_best_effort(&self, manifest: &ContentManifest) {
        let result = (|| {
            let index_path =
                self.content_manifest_index_path(&manifest.object_id, &manifest.content_hash)?;
            if index_path.is_file() {
                return Ok(());
            }
            let manifest_path = self.manifest_path(&manifest.object_id, &manifest.version);
            match fs::hard_link(&manifest_path, &index_path) {
                Ok(()) => Ok(()),
                Err(source) if source.kind() == io::ErrorKind::AlreadyExists => Ok(()),
                Err(source) => Err(ContentStoreError::Io {
                    path: index_path,
                    source,
                }),
            }
        })();
        if let Err(error) = result {
            // The index is a rebuildable accelerator. The manifest remains
            // authoritative and the first identity lookup will fall back to
            // one version-directory scan if this best-effort link fails.
            content_store_perf_trace(format_args!(
                "content manifest index link skipped object={} version={} error={error}",
                manifest.object_id, manifest.version
            ));
        }
    }

    fn remove_content_manifest_index_for_version_if_current(
        &self,
        entry: &VersionManifestEntry,
    ) -> Result<()> {
        let index_path = self.content_manifest_index_path(&entry.object_id, &entry.content_hash)?;
        if !index_path.is_file() {
            return Ok(());
        }
        let Ok(indexed_manifest) = decode_manifest(&read_file(&index_path)?) else {
            // A derived index that does not decode cannot be proven to point
            // at the version being collected. Leave it for scrub/lookup repair.
            return Ok(());
        };
        if indexed_manifest.object_id == entry.object_id
            && indexed_manifest.version == entry.version
            && indexed_manifest.content_hash == entry.content_hash
        {
            remove_file_if_exists_and_sync_parent(&index_path)?;
        }
        Ok(())
    }

    fn blob_path(&self, hash: &str) -> Result<PathBuf> {
        let hex = hash_hex(hash)?;
        Ok(self.root.join("blobs").join(&hex[..2]).join(hex))
    }

    fn manifest_path(&self, object_id: &str, version: &str) -> PathBuf {
        self.root
            .join("objects")
            .join(object_id)
            .join(version)
            .join("manifest.sobj")
    }

    fn content_manifest_index_path(&self, object_id: &str, content_hash: &str) -> Result<PathBuf> {
        let hex = hash_hex(content_hash)?;
        Ok(self
            .root
            .join("objects")
            .join(object_id)
            .join(format!(".content-{hex}.sobj")))
    }

    fn hash_tree_path(&self, object_id: &str, version: &str) -> PathBuf {
        self.root
            .join("objects")
            .join(object_id)
            .join(version)
            .join("hash-tree.sobj")
    }

    fn staging_path(&self, intent_id: &str) -> PathBuf {
        self.root.join("staging").join(intent_id)
    }
}

fn encode_manifest(manifest: &ContentManifest) -> Result<Vec<u8>> {
    sinomo_object_serde::to_bytes(manifest, 1, LengthMode::U32)
        .map_err(|source| ContentStoreError::Serde(source.to_string()))
}

fn decode_manifest(bytes: &[u8]) -> Result<ContentManifest> {
    sinomo_object_serde::from_bytes(bytes)
        .map_err(|source| ContentStoreError::Serde(source.to_string()))
}

fn validate_and_order_stream_manifest(mut manifest: ContentManifest) -> Result<ContentManifest> {
    hash_hex(&manifest.content_hash)?;
    if manifest.extents.is_empty() {
        return Ok(manifest);
    }
    let already_ordered = manifest.extents.windows(2).all(|pair| {
        pair[0]
            .logical_offset
            .cmp(&pair[1].logical_offset)
            .then_with(|| pair[0].blob_offset.cmp(&pair[1].blob_offset))
            != std::cmp::Ordering::Greater
    });
    if !already_ordered {
        manifest.extents.sort_by(|left, right| {
            left.logical_offset
                .cmp(&right.logical_offset)
                .then_with(|| left.blob_offset.cmp(&right.blob_offset))
        });
    }
    validate_extents_cover_size(
        &manifest.extents,
        manifest.size_bytes,
        &manifest.object_id,
        &manifest.version,
    )?;
    Ok(manifest)
}

fn encode_hash_tree(tree: &ContentHashTree) -> Result<Vec<u8>> {
    sinomo_object_serde::to_bytes(tree, 1, LengthMode::U32)
        .map_err(|source| ContentStoreError::Serde(source.to_string()))
}

fn decode_hash_tree(bytes: &[u8]) -> Result<ContentHashTree> {
    sinomo_object_serde::from_bytes(bytes)
        .map_err(|source| ContentStoreError::Serde(source.to_string()))
}

fn encode_stats_snapshot(snapshot: &ContentStoreStatsSnapshot) -> Result<Vec<u8>> {
    sinomo_object_serde::to_bytes(snapshot, 1, LengthMode::U32)
        .map_err(|source| ContentStoreError::Serde(source.to_string()))
}

fn decode_stats_snapshot(bytes: &[u8]) -> Result<ContentStoreStatsSnapshot> {
    sinomo_object_serde::from_bytes(bytes)
        .map_err(|source| ContentStoreError::Serde(source.to_string()))
}

fn clip_extent(
    extent: &ContentExtent,
    logical_start: u64,
    logical_end: u64,
) -> Result<ContentExtent> {
    if logical_start < extent.logical_offset || logical_end < logical_start {
        return Err(ContentStoreError::InvalidContentLayout {
            message: "invalid extent clip range".to_string(),
        });
    }
    let extent_end = extent
        .logical_offset
        .checked_add(extent.size_bytes)
        .ok_or_else(|| ContentStoreError::InvalidContentLayout {
            message: "extent logical range overflow".to_string(),
        })?;
    if logical_end > extent_end {
        return Err(ContentStoreError::InvalidContentLayout {
            message: "extent clip exceeds source extent".to_string(),
        });
    }
    let delta = logical_start - extent.logical_offset;
    Ok(ContentExtent {
        logical_offset: logical_start,
        size_bytes: logical_end - logical_start,
        content_hash: extent.content_hash.clone(),
        blob_offset: extent.blob_offset.checked_add(delta).ok_or_else(|| {
            ContentStoreError::InvalidContentLayout {
                message: "extent blob offset overflow".to_string(),
            }
        })?,
    })
}

fn push_extent_coalescing(extents: &mut Vec<ContentExtent>, extent: ContentExtent) -> Result<()> {
    if extent.size_bytes == 0 {
        return Ok(());
    }
    hash_hex(&extent.content_hash)?;
    if let Some(last) = extents.last_mut() {
        let last_logical_end = last
            .logical_offset
            .checked_add(last.size_bytes)
            .ok_or_else(|| ContentStoreError::InvalidContentLayout {
                message: "extent logical range overflow".to_string(),
            })?;
        let last_blob_end = last
            .blob_offset
            .checked_add(last.size_bytes)
            .ok_or_else(|| ContentStoreError::InvalidContentLayout {
                message: "extent blob range overflow".to_string(),
            })?;
        if last.content_hash == extent.content_hash
            && last_logical_end == extent.logical_offset
            && last_blob_end == extent.blob_offset
        {
            last.size_bytes = last
                .size_bytes
                .checked_add(extent.size_bytes)
                .ok_or_else(|| ContentStoreError::InvalidContentLayout {
                    message: "coalesced extent size overflow".to_string(),
                })?;
            return Ok(());
        }
    }
    extents.push(extent);
    Ok(())
}

fn coalesce_extents(extents: Vec<ContentExtent>) -> Result<Vec<ContentExtent>> {
    let mut coalesced = Vec::with_capacity(extents.len());
    for extent in extents {
        push_extent_coalescing(&mut coalesced, extent)?;
    }
    Ok(coalesced)
}

fn validate_extents_cover_size(
    extents: &[ContentExtent],
    size_bytes: u64,
    object_id: &str,
    version: &str,
) -> Result<()> {
    if size_bytes == 0 {
        if extents.is_empty() {
            return Ok(());
        }
        return Err(ContentStoreError::InvalidContentLayout {
            message: format!("zero-sized object {object_id} {version} has non-empty extents"),
        });
    }
    let mut cursor = 0_u64;
    for extent in extents {
        hash_hex(&extent.content_hash)?;
        if extent.size_bytes == 0 {
            return Err(ContentStoreError::InvalidContentLayout {
                message: format!("zero-sized extent in object {object_id} {version}"),
            });
        }
        if extent.logical_offset != cursor {
            return Err(ContentStoreError::InvalidContentLayout {
                message: format!(
                    "manifest extents are not contiguous for object {object_id} {version}: expected offset {cursor}, actual {}",
                    extent.logical_offset
                ),
            });
        }
        let logical_end = extent
            .logical_offset
            .checked_add(extent.size_bytes)
            .ok_or_else(|| ContentStoreError::InvalidContentLayout {
                message: format!("extent logical range overflow in object {object_id} {version}"),
            })?;
        extent
            .blob_offset
            .checked_add(extent.size_bytes)
            .ok_or_else(|| ContentStoreError::InvalidContentLayout {
                message: format!("extent blob range overflow in object {object_id} {version}"),
            })?;
        cursor = logical_end;
    }
    if cursor != size_bytes {
        return Err(ContentStoreError::InvalidContentLayout {
            message: format!(
                "manifest extents do not cover object {object_id} {version}: covered {cursor}, expected {size_bytes}"
            ),
        });
    }
    Ok(())
}

pub(crate) fn content_hash(bytes: &[u8]) -> String {
    format!("{HASH_PREFIX}{}", blake3::hash(bytes).to_hex())
}

fn content_hash_tree_leaf_count(size_bytes: u64) -> Result<usize> {
    if size_bytes == 0 {
        return Ok(0);
    }
    usize::try_from(size_bytes.div_ceil(CONTENT_HASH_TREE_LEAF_BYTES)).map_err(|_| {
        ContentStoreError::InvalidContentLayout {
            message: format!("content hash tree has too many leaves: bytes={size_bytes}"),
        }
    })
}

fn content_hash_tree_leaf_range(size_bytes: u64, index: usize) -> Result<(u64, u64)> {
    let index = u64::try_from(index).map_err(|_| ContentStoreError::InvalidContentLayout {
        message: "content hash tree leaf index is too large".to_string(),
    })?;
    let logical_offset = index
        .checked_mul(CONTENT_HASH_TREE_LEAF_BYTES)
        .ok_or_else(|| ContentStoreError::InvalidContentLayout {
            message: format!("content hash tree leaf offset overflows: leaf={index}"),
        })?;
    if logical_offset >= size_bytes {
        return Err(ContentStoreError::InvalidContentLayout {
            message: format!(
                "content hash tree leaf starts beyond object: leaf={index}, offset={logical_offset}, size={size_bytes}"
            ),
        });
    }
    Ok((
        logical_offset,
        CONTENT_HASH_TREE_LEAF_BYTES.min(size_bytes - logical_offset),
    ))
}

fn hash_tree_leaf_chaining_value(logical_offset: u64, bytes: &[u8]) -> ChainingValue {
    let mut hasher = blake3::Hasher::new();
    if logical_offset > 0 {
        hasher.set_input_offset(logical_offset);
    }
    hasher.update(bytes);
    hasher.finalize_non_root()
}

fn content_hash_from_hash_tree_leaves(
    size_bytes: u64,
    leaves: &[ContentHashTreeLeaf],
) -> Result<String> {
    if size_bytes == 0 {
        return Ok(content_hash(&[]));
    }
    if size_bytes <= CONTENT_HASH_TREE_LEAF_BYTES || leaves.len() <= 1 {
        return Err(ContentStoreError::InvalidContentLayout {
            message: "content hash tree root requires at least two leaves".to_string(),
        });
    }
    let left_len = left_subtree_len(size_bytes);
    let left_leaf_count =
        usize::try_from(left_len / CONTENT_HASH_TREE_LEAF_BYTES).map_err(|_| {
            ContentStoreError::InvalidContentLayout {
                message: format!("content hash tree left subtree is too large: bytes={left_len}"),
            }
        })?;
    if left_leaf_count == 0 || left_leaf_count >= leaves.len() {
        return Err(ContentStoreError::InvalidContentLayout {
            message: format!(
                "content hash tree invalid root split: bytes={size_bytes}, left_leaves={left_leaf_count}, leaves={}",
                leaves.len()
            ),
        });
    }
    let left = content_hash_tree_subtree_cv(&leaves[..left_leaf_count], left_len)?;
    let right = content_hash_tree_subtree_cv(&leaves[left_leaf_count..], size_bytes - left_len)?;
    Ok(format!(
        "{HASH_PREFIX}{}",
        merge_subtrees_root(&left, &right, Mode::Hash).to_hex()
    ))
}

fn content_hash_tree_subtree_cv(
    leaves: &[ContentHashTreeLeaf],
    size_bytes: u64,
) -> Result<ChainingValue> {
    if leaves.is_empty() {
        return Err(ContentStoreError::InvalidContentLayout {
            message: "content hash tree subtree has no leaves".to_string(),
        });
    }
    if leaves.len() == 1 {
        if leaves[0].size_bytes != size_bytes {
            return Err(ContentStoreError::InvalidContentLayout {
                message: format!(
                    "content hash tree leaf size mismatch: expected={size_bytes}, actual={}",
                    leaves[0].size_bytes
                ),
            });
        }
        return Ok(leaves[0].chaining_value);
    }
    let left_len = left_subtree_len(size_bytes);
    let left_leaf_count =
        usize::try_from(left_len / CONTENT_HASH_TREE_LEAF_BYTES).map_err(|_| {
            ContentStoreError::InvalidContentLayout {
                message: format!("content hash tree subtree split is too large: bytes={left_len}"),
            }
        })?;
    if left_leaf_count == 0 || left_leaf_count >= leaves.len() {
        return Err(ContentStoreError::InvalidContentLayout {
            message: format!(
                "content hash tree invalid subtree split: bytes={size_bytes}, left_leaves={left_leaf_count}, leaves={}",
                leaves.len()
            ),
        });
    }
    let left = content_hash_tree_subtree_cv(&leaves[..left_leaf_count], left_len)?;
    let right = content_hash_tree_subtree_cv(&leaves[left_leaf_count..], size_bytes - left_len)?;
    Ok(merge_subtrees_non_root(&left, &right, Mode::Hash))
}

fn ensure_bytes_match_hash(hash: &str, bytes: &[u8]) -> Result<()> {
    let actual = content_hash(bytes);
    if actual == hash {
        return Ok(());
    }
    Err(ContentStoreError::BlobHashMismatch {
        expected: hash.to_owned(),
        actual,
    })
}

fn file_content_hash(path: &Path) -> Result<String> {
    file_content_hash_and_size(path).map(|(hash, _)| hash)
}

fn file_content_hash_and_size(path: &Path) -> Result<(String, u64)> {
    let (hash, size_bytes) =
        durable_file::file_blake3_hash_and_size(path).map_err(content_durable_file_error)?;
    Ok((format!("{HASH_PREFIX}{}", hash.to_hex()), size_bytes))
}

fn hash_hex(hash: &str) -> Result<&str> {
    let Some(hex) = hash.strip_prefix(HASH_PREFIX) else {
        return Err(ContentStoreError::InvalidHash(hash.to_owned()));
    };
    if hex.len() != BLAKE3_HEX_LEN || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ContentStoreError::InvalidHash(hash.to_owned()));
    }
    if hex.bytes().any(|byte| byte.is_ascii_uppercase()) {
        return Err(ContentStoreError::InvalidHash(hash.to_owned()));
    }
    Ok(hex)
}

fn content_error_is_not_found(error: &ContentStoreError) -> bool {
    matches!(
        error,
        ContentStoreError::Io { source, .. } if source.kind() == io::ErrorKind::NotFound
    )
}

fn collect_staging_gc_candidates(
    path: &Path,
    candidates: &mut Vec<StagingGcCandidate>,
) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    for entry in read_dir(path)? {
        let entry_path = entry.path();
        if entry_path.is_dir() {
            collect_staging_gc_candidates(&entry_path, candidates)?;
            continue;
        }
        if !entry_path.is_file() {
            continue;
        }
        let metadata = entry_path
            .metadata()
            .map_err(|source| ContentStoreError::Io {
                path: entry_path.clone(),
                source,
            })?;
        let modified_at = metadata
            .modified()
            .map_err(|source| ContentStoreError::Io {
                path: entry_path.clone(),
                source,
            })?;
        candidates.push(StagingGcCandidate {
            path: entry_path,
            size_bytes: metadata.len(),
            modified_at,
        });
    }
    Ok(())
}

fn capture_limit_includes_size(limit_bytes: usize, size_bytes: u64) -> bool {
    limit_bytes > 0 && size_bytes <= u64::try_from(limit_bytes).unwrap_or(u64::MAX)
}

fn publish_file_as_blob(source_path: &Path, blob_path: &Path, expected_hash: &str) -> Result<()> {
    if let Some(parent) = blob_path.parent() {
        create_dir_all(parent.to_owned())?;
    }
    if blob_path.exists() {
        if file_content_hash(blob_path).is_ok_and(|actual| actual == expected_hash) {
            return Ok(());
        }
        remove_file_if_exists(blob_path)?;
    }

    match fs::hard_link(source_path, blob_path) {
        Ok(()) => {}
        Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {
            if file_content_hash(blob_path).is_ok_and(|actual| actual == expected_hash) {
                return Ok(());
            }
            remove_file_if_exists(blob_path)?;
            copy_file_create_new_or_existing(source_path, blob_path, expected_hash)?;
        }
        Err(_) => {
            copy_file_create_new_or_existing(source_path, blob_path, expected_hash)?;
        }
    }
    if let Some(parent) = blob_path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

fn copy_file_create_new_or_existing(
    source_path: &Path,
    target_path: &Path,
    expected_hash: &str,
) -> Result<()> {
    match copy_file_create_new(source_path, target_path) {
        Ok(()) => Ok(()),
        Err(error) => {
            if let ContentStoreError::Io { source, .. } = &error
                && source.kind() == io::ErrorKind::AlreadyExists
                && file_content_hash(target_path).is_ok_and(|actual| actual == expected_hash)
            {
                return Ok(());
            }
            Err(error)
        }
    }
}

fn copy_file_create_new(source_path: &Path, target_path: &Path) -> Result<()> {
    let mut source_file = fs::File::open(source_path).map_err(|source| ContentStoreError::Io {
        path: source_path.to_owned(),
        source,
    })?;
    let mut target_file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(target_path)
        .map_err(|source| ContentStoreError::Io {
            path: target_path.to_owned(),
            source,
        })?;
    io::copy(&mut source_file, &mut target_file).map_err(|source| ContentStoreError::Io {
        path: target_path.to_owned(),
        source,
    })?;
    target_file
        .sync_all()
        .map_err(|source| ContentStoreError::Io {
            path: target_path.to_owned(),
            source,
        })
}

fn write_content_addressed_blob_bytes(hash: &str, blob_path: &Path, bytes: &[u8]) -> Result<()> {
    ensure_bytes_match_hash(hash, bytes)?;
    write_content_addressed_blob_bytes_prehashed(hash, blob_path, bytes)
}

fn write_content_addressed_blob_bytes_prehashed(
    hash: &str,
    blob_path: &Path,
    bytes: &[u8],
) -> Result<()> {
    if let Some(parent) =
        write_content_addressed_blob_bytes_prehashed_defer_dir_sync(hash, blob_path, bytes)?
    {
        sync_dir(&parent)?;
    }
    Ok(())
}

fn write_content_addressed_blob_bytes_prehashed_existing_parent(
    hash: &str,
    blob_path: &Path,
    bytes: &[u8],
) -> Result<()> {
    if let Some(parent) = write_content_addressed_blob_bytes_prehashed_defer_dir_sync_inner(
        hash, blob_path, bytes, false,
    )? {
        sync_dir(&parent)?;
    }
    Ok(())
}

fn write_content_addressed_blob_bytes_prehashed_defer_dir_sync(
    hash: &str,
    blob_path: &Path,
    bytes: &[u8],
) -> Result<Option<PathBuf>> {
    write_content_addressed_blob_bytes_prehashed_defer_dir_sync_inner(hash, blob_path, bytes, true)
}

fn write_content_addressed_blob_bytes_prehashed_defer_dir_sync_inner(
    hash: &str,
    blob_path: &Path,
    bytes: &[u8],
    ensure_parent: bool,
) -> Result<Option<PathBuf>> {
    let trace_started = Instant::now();
    let create_dir_started = Instant::now();
    if ensure_parent && let Some(parent) = blob_path.parent() {
        create_dir_all(parent.to_owned())?;
    }
    let create_dir_ms = create_dir_started.elapsed().as_millis();
    let existing_check_started = Instant::now();
    if blob_path.is_file() {
        if file_content_hash(blob_path).is_ok_and(|actual| actual == hash) {
            content_store_perf_trace(format_args!(
                "content-addressed-blob-write hash={} bytes={} existing=true create_dir_ms={} existing_check_ms={} total_ms={}",
                hash,
                bytes.len(),
                create_dir_ms,
                existing_check_started.elapsed().as_millis(),
                trace_started.elapsed().as_millis()
            ));
            return Ok(None);
        }
        remove_file_if_exists(blob_path)?;
    }
    let existing_check_ms = existing_check_started.elapsed().as_millis();

    let write_sync_started = Instant::now();
    let mut file = match fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(blob_path)
    {
        Ok(file) => file,
        Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {
            if file_content_hash(blob_path).is_ok_and(|actual| actual == hash) {
                content_store_perf_trace(format_args!(
                    "content-addressed-blob-write hash={} bytes={} existing_race=true create_dir_ms={} existing_check_ms={} total_ms={}",
                    hash,
                    bytes.len(),
                    create_dir_ms,
                    existing_check_ms,
                    trace_started.elapsed().as_millis()
                ));
                return Ok(None);
            }
            remove_file_if_exists(blob_path)?;
            fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(blob_path)
                .map_err(|source| ContentStoreError::Io {
                    path: blob_path.to_owned(),
                    source,
                })?
        }
        Err(source) => {
            return Err(ContentStoreError::Io {
                path: blob_path.to_owned(),
                source,
            });
        }
    };
    if let Err(source) = file.write_all(bytes).and_then(|()| file.sync_data()) {
        drop(file);
        remove_file_if_exists(blob_path).ok();
        return Err(ContentStoreError::Io {
            path: blob_path.to_owned(),
            source,
        });
    }
    drop(file);
    let write_sync_ms = write_sync_started.elapsed().as_millis();
    let deferred_sync_dir = blob_path.parent().map(Path::to_path_buf);
    content_store_perf_trace(format_args!(
        "content-addressed-blob-write hash={} bytes={} existing=false create_dir_ms={} existing_check_ms={} write_sync_ms={} sync_dir_ms=deferred total_ms={}",
        hash,
        bytes.len(),
        create_dir_ms,
        existing_check_ms,
        write_sync_ms,
        trace_started.elapsed().as_millis()
    ));
    Ok(deferred_sync_dir)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn streamed_object_ingest_hashes_and_publishes_incremental_chunks() {
        let root = temp_root("streamed_object_ingest_incremental");
        let store = LocalContentStore::new(&root).expect("create store");
        let bytes = (0..(3 * 64 * 1024 + 17))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let expected_hash = content_hash(&bytes);
        let mut ingest = store
            .begin_object_version_ingest(
                "stream-object",
                "v1",
                bytes.len() as u64,
                Some(&expected_hash),
            )
            .expect("begin ingest");
        for chunk in bytes.chunks(8191) {
            ingest.write_chunk(chunk).expect("write chunk");
        }
        let manifest = ingest.finish().expect("finish ingest");

        assert_eq!(manifest.content_hash, expected_hash);
        assert_eq!(manifest.size_bytes, bytes.len() as u64);
        let (_, stored_bytes) = store
            .read_object_version("stream-object", "v1")
            .expect("read ingested object");
        assert_eq!(stored_bytes, bytes);

        cleanup(&root);
    }

    #[test]
    fn quarantined_stream_ingest_does_not_reserve_version_until_bound() {
        let root = temp_root("quarantined_stream_ingest_bind");
        let store = LocalContentStore::new(&root).expect("create store");
        let bytes = b"validate this streamed value before publication";
        let expected_hash = content_hash(bytes);
        let mut ingest = store
            .begin_quarantined_blob_ingest(bytes.len() as u64, Some(&expected_hash))
            .expect("begin quarantined ingest");
        ingest.write_chunk(bytes).expect("write chunk");
        let blob = ingest.finish_quarantined().expect("finish quarantine");

        assert_eq!(blob.content_hash, expected_hash);
        assert_eq!(blob.size_bytes, bytes.len() as u64);
        assert!(
            store.read_manifest("stream-object", "v1").is_err(),
            "finishing quarantine must not reserve an object version"
        );

        let manifest = store
            .bind_quarantined_blob("stream-object", "v1", blob)
            .expect("bind validated blob");
        assert_eq!(manifest.content_hash, expected_hash);
        let (_, stored_bytes) = store
            .read_object_version("stream-object", "v1")
            .expect("read bound object");
        assert_eq!(stored_bytes, bytes);

        cleanup(&root);
    }

    #[test]
    fn streamed_object_ingest_rejects_application_hash_at_eof() {
        let root = temp_root("streamed_object_ingest_hash_mismatch");
        let store = LocalContentStore::new(&root).expect("create store");
        let bytes = b"streamed payload";
        let actual_hash = content_hash(bytes);
        let expected_hash = content_hash(b"different payload");
        let mut ingest = store
            .begin_object_version_ingest(
                "stream-object",
                "v-bad",
                bytes.len() as u64,
                Some(&expected_hash),
            )
            .expect("begin ingest");
        ingest.write_chunk(bytes).expect("write chunk");
        let error = ingest.finish().expect_err("hash mismatch");

        assert!(matches!(
            error,
            ContentStoreError::BlobHashMismatch {
                expected,
                actual,
            } if expected == expected_hash && actual == actual_hash
        ));
        assert!(
            !store.has_blob(&actual_hash).expect("blob check"),
            "a rejected ingest must not publish its temporary blob"
        );
        assert!(
            store.read_manifest("stream-object", "v-bad").is_err(),
            "a rejected ingest must not publish a manifest"
        );

        cleanup(&root);
    }

    #[test]
    fn blob_storage_candidate_is_presence_only_and_does_not_claim_integrity() {
        let root = temp_root("blob_storage_candidate_presence_only");
        let store = LocalContentStore::new(&root).expect("create store");
        let expected_hash = content_hash(b"expected bytes");
        let path = store.blob_path(&expected_hash).expect("blob path");
        fs::create_dir_all(path.parent().expect("blob parent")).expect("create blob parent");
        fs::write(&path, b"different bytes").expect("write mismatched candidate");

        assert!(
            store
                .blob_storage_candidate_exists(&expected_hash)
                .expect("candidate probe")
        );
        assert!(
            !store.has_blob(&expected_hash).expect("integrity probe"),
            "the advisory presence probe must not replace integrity validation"
        );

        cleanup(&root);
    }

    #[test]
    fn blob_storage_candidate_probe_does_not_initialize_missing_store() {
        let root = temp_root("blob_storage_candidate_missing_store");
        let content_root = root.join("content-not-created");
        let missing_hash = content_hash(b"missing bytes");

        assert!(
            !LocalContentStore::blob_storage_candidate_exists_at(&content_root, &missing_hash)
                .expect("candidate probe")
        );
        assert!(!content_root.exists());

        cleanup(&root);
    }

    #[test]
    fn young_staging_file_projects_exact_cleanup_deadline() {
        let root = temp_root("young-staging-cleanup-deadline");
        let store = LocalContentStore::new(&root).expect("create store");
        store
            .stage_bytes("intent-young", b"pending")
            .expect("stage bytes");
        let staging_path = root.join("staging").join("intent-young");
        let modified_at = staging_path
            .metadata()
            .expect("staging metadata")
            .modified()
            .expect("staging modified time");
        let min_age = Duration::from_secs(60 * 60);

        let projection = store
            .collect_stale_staging(ContentStoreStagingGcOptions::dry_run(min_age))
            .expect("staging projection");

        assert_eq!(projection.eligible_staging_count, 0);
        assert_eq!(projection.retained_young_staging_count, 1);
        assert_eq!(
            projection.next_eligible_at_unix_nanos,
            Some(
                system_time_nanos_since_epoch_saturating(modified_at)
                    .saturating_add(duration_nanos_saturating(min_age))
            )
        );
        cleanup(&root);
    }

    #[test]
    fn object_version_storage_candidate_accepts_extent_backed_version() {
        let root = temp_root("object_version_storage_candidate_extents");
        let store = LocalContentStore::new(&root).expect("create store");
        store
            .put_object_version("object-extents", "v1", b"abcdefgh")
            .expect("base version");
        let patched = store
            .put_object_version_patch("object-extents", "v1", "v2", 2, b"ZZ")
            .expect("patch version");

        assert!(!patched.extents.is_empty());
        assert!(
            !store
                .has_blob(&patched.content_hash)
                .expect("whole blob probe"),
            "the patch should remain represented by reusable extents"
        );
        assert!(
            store
                .object_version_storage_candidate_exists(
                    "object-extents",
                    "v2",
                    &patched.content_hash,
                )
                .expect("object-version candidate")
        );

        cleanup(&root);
    }

    #[test]
    fn put_object_version_writes_blob_and_manifest() {
        let root = temp_root("put_object_version_writes_blob_and_manifest");
        let store = LocalContentStore::new(&root).expect("create store");

        let manifest = store
            .put_object_version("object-a", "v1", b"hello dfs")
            .expect("put object version");

        assert_eq!(manifest.object_id, "object-a");
        assert_eq!(manifest.version, "v1");
        assert_eq!(manifest.size_bytes, 9);
        assert!(manifest.content_hash.starts_with(HASH_PREFIX));
        assert_eq!(
            manifest.content_hash.len(),
            HASH_PREFIX.len() + BLAKE3_HEX_LEN
        );
        assert!(store.has_blob(&manifest.content_hash).expect("has blob"));
        assert_eq!(
            store.read_blob(&manifest.content_hash).expect("read blob"),
            b"hello dfs"
        );
        assert_eq!(
            store
                .read_manifest("object-a", "v1")
                .expect("read manifest"),
            manifest
        );
        assert_eq!(
            store
                .manifests_for_object("object-a")
                .expect("object manifests"),
            vec![manifest]
        );

        cleanup(&root);
    }

    #[test]
    fn put_object_versions_batches_small_blobs_and_manifests() {
        let root = temp_root("put_object_versions_batches_small_blobs_and_manifests");
        let store = LocalContentStore::new(&root).expect("create store");

        let first = b"small batch object one";
        let second = b"small batch object two";
        let manifests = store
            .put_object_versions(vec![
                ("object-batch-a", "v1", first.as_slice()),
                ("object-batch-b", "v1", second.as_slice()),
            ])
            .expect("put object versions");

        assert_eq!(manifests.len(), 2);
        assert_eq!(
            store
                .read_blob(&manifests[0].content_hash)
                .expect("read first"),
            first
        );
        assert_eq!(
            store
                .read_blob(&manifests[1].content_hash)
                .expect("read second"),
            second
        );
        assert_eq!(
            store
                .read_manifest("object-batch-a", "v1")
                .expect("first manifest"),
            manifests[0]
        );
        assert_eq!(
            store
                .read_manifest("object-batch-b", "v1")
                .expect("second manifest"),
            manifests[1]
        );
        let stats = store.stats().expect("stats");
        assert_eq!(stats.blob_count, 2);
        assert_eq!(stats.manifest_count, 2);
        assert_eq!(
            stats.logical_object_bytes,
            (first.len() + second.len()) as u64
        );

        cleanup(&root);
    }

    #[test]
    fn put_object_version_blobs_only_batches_small_blobs_without_manifests() {
        let root = temp_root("put_object_version_blobs_only_batches_small_blobs_without_manifests");
        let store = LocalContentStore::new(&root).expect("create store");

        let first = b"blob only batch object one";
        let second = b"blob only batch object two";
        let manifests = store
            .put_object_version_blobs_only(vec![
                ("object-blob-only-a", "v1", first.as_slice()),
                ("object-blob-only-b", "v1", second.as_slice()),
            ])
            .expect("put blob-only object versions");

        assert_eq!(manifests.len(), 2);
        assert_eq!(
            store
                .read_blob(&manifests[0].content_hash)
                .expect("read first blob"),
            first
        );
        assert_eq!(
            store
                .read_blob(&manifests[1].content_hash)
                .expect("read second blob"),
            second
        );
        assert!(
            store.read_manifest("object-blob-only-a", "v1").is_err(),
            "blob-only batch writes should not create per-object manifests"
        );
        assert!(
            store.read_manifest("object-blob-only-b", "v1").is_err(),
            "blob-only batch writes should not create per-object manifests"
        );

        cleanup(&root);
    }

    #[test]
    fn put_object_version_blobs_only_deduplicates_same_batch_packed_hash() {
        let root = temp_root("put_object_version_blobs_only_deduplicates_same_batch_packed_hash");
        let store = LocalContentStore::new(&root).expect("create store");

        let bytes = b"same packed blob bytes";
        let manifests = store
            .put_object_version_blobs_only(vec![
                ("object-blob-only-duplicate-a", "v1", bytes.as_slice()),
                ("object-blob-only-duplicate-b", "v1", bytes.as_slice()),
            ])
            .expect("put duplicate blob-only object versions");

        assert_eq!(manifests.len(), 2);
        assert_eq!(manifests[0].content_hash, manifests[1].content_hash);
        assert_eq!(
            store
                .read_blob(&manifests[0].content_hash)
                .expect("read packed duplicate blob"),
            bytes
        );
        let index = store
            .read_packed_blob_index_unlocked()
            .expect("read packed index");
        assert_eq!(index.entries.len(), 1);
        let stats = store.stats().expect("stats");
        assert_eq!(stats.blob_count, 1);
        assert_eq!(stats.blob_bytes, bytes.len() as u64);

        cleanup(&root);
    }

    #[test]
    fn no_quota_status_snapshot_preserves_streamed_durable_blob_only_branch() {
        let root =
            temp_root("no_quota_status_snapshot_preserves_streamed_durable_blob_only_branch");
        let store = LocalContentStore::new(&root).expect("create store");
        let bytes = vec![0x71; STREAMED_BLOB_ONLY_MIN_BYTES + 4096];

        let empty = store.stats().expect("status materializes derived snapshot");
        assert_eq!(empty, ContentStoreStats::default());
        let snapshot_before_write = read_file(&store.stats_snapshot_path()).expect("read snapshot");
        assert!(!store.tracks_stats_deltas());
        assert!(store.uses_streamed_blob_only_path(bytes.len()));

        let manifest = store
            .put_object_version_blob_only("object-blob-only-large", "v1", &bytes)
            .expect("put blob-only large object");

        assert_eq!(
            read_file(&store.stats_snapshot_path()).expect("read unchanged snapshot"),
            snapshot_before_write,
            "a derived status snapshot must not activate synchronous write deltas"
        );
        assert_eq!(
            store
                .read_blob(&manifest.content_hash)
                .expect("read large blob"),
            bytes
        );
        assert!(
            store.read_manifest("object-blob-only-large", "v1").is_err(),
            "blob-only large writes should not create per-object manifests"
        );
        let rebuilt = store.stats().expect("derive stats from durable blob");
        assert_eq!(rebuilt.blob_count, 1);
        assert_eq!(rebuilt.blob_bytes, bytes.len() as u64);
        assert_eq!(rebuilt.manifest_count, 0);

        cleanup(&root);
    }

    #[test]
    fn put_object_version_blob_only_large_blob_skips_manifest() {
        let root = temp_root("put_object_version_blob_only_large_blob_skips_manifest");
        let store = LocalContentStore::new(&root).expect("create store");
        let bytes = vec![0x73; PACKED_BLOB_SMALL_LIMIT_BYTES as usize + 4096];

        let manifest = store
            .put_object_version_blob_only("object-blob-only-durable-large", "v1", &bytes)
            .expect("put durable blob-only large object");

        assert_eq!(
            store
                .read_blob(&manifest.content_hash)
                .expect("read durable large blob"),
            bytes
        );
        assert!(
            store
                .read_manifest("object-blob-only-durable-large", "v1")
                .is_err(),
            "durable blob-only large writes should not create per-object manifests"
        );

        cleanup(&root);
    }

    #[test]
    fn put_object_version_from_reader_streams_blob_and_manifest() {
        let root = temp_root("put_object_version_from_reader_streams_blob_and_manifest");
        let store = LocalContentStore::new(&root).expect("create store");
        let size = 192 * 1024 + 17;
        let reader = std::io::repeat(0x5a).take(size);

        let manifest = store
            .put_object_version_from_reader("object-large", "v1", reader)
            .expect("put object version from reader");

        assert_eq!(manifest.object_id, "object-large");
        assert_eq!(manifest.version, "v1");
        assert_eq!(manifest.size_bytes, size);
        assert!(store.has_blob(&manifest.content_hash).expect("has blob"));
        let bytes = store.read_blob(&manifest.content_hash).expect("read blob");
        assert_eq!(bytes.len() as u64, size);
        assert!(bytes.iter().all(|byte| *byte == 0x5a));
        assert_eq!(
            store
                .read_manifest("object-large", "v1")
                .expect("read manifest"),
            manifest
        );
        assert!(
            std::fs::read_dir(root.join("staging"))
                .expect("staging dir")
                .next()
                .is_none(),
            "streaming writes must not leave temporary staging files"
        );
        assert_eq!(store.stats().expect("stats").staging_bytes, 0);

        cleanup(&root);
    }

    #[test]
    fn put_small_object_version_from_reader_skips_temporary_blob_file() {
        let root = temp_root("put_small_object_version_from_reader_skips_temporary_blob_file");
        let store = LocalContentStore::new(&root).expect("create store");
        let bytes = b"small reader-backed DFS object";

        let manifest = store
            .put_object_version_from_reader("object-small-reader", "v1", bytes.as_slice())
            .expect("put small object version from reader");

        assert_eq!(manifest.size_bytes, bytes.len() as u64);
        assert_eq!(
            store.read_blob(&manifest.content_hash).expect("read blob"),
            bytes
        );
        assert!(
            !root.join("blob-tmp").exists(),
            "small reader-backed writes must stay within the bounded memory fast path"
        );

        cleanup(&root);
    }

    #[test]
    fn put_object_version_from_reader_capture_reuses_streamed_bytes() {
        let root = temp_root("put_object_version_from_reader_capture_reuses_streamed_bytes");
        let store = LocalContentStore::new(&root).expect("create store");
        let bytes = (0..192 * 1024 + 17)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();

        let (manifest, captured) = store
            .put_object_version_from_reader_capture(
                "object-large-capture",
                "v1",
                bytes.as_slice(),
                bytes.len(),
            )
            .expect("put object version from reader with capture");

        assert_eq!(captured.as_deref(), Some(bytes.as_slice()));
        assert_eq!(manifest.content_hash, content_hash(&bytes));
        assert_eq!(
            store
                .read_manifest("object-large-capture", "v1")
                .expect("read manifest"),
            manifest
        );
        assert_eq!(
            store.read_blob(&manifest.content_hash).expect("read blob"),
            bytes
        );
        assert!(
            std::fs::read_dir(root.join("staging"))
                .expect("staging dir")
                .next()
                .is_none(),
            "capture writes must not use the staged-write directory"
        );
        assert!(
            !root.join("blob-tmp").exists()
                || std::fs::read_dir(root.join("blob-tmp"))
                    .expect("blob tmp dir")
                    .next()
                    .is_none(),
            "capture writes must remove temporary blob files"
        );

        cleanup(&root);
    }

    #[test]
    fn open_blob_reader_validates_hash_and_streams_bytes() {
        let root = temp_root("open_blob_reader_validates_hash_and_streams_bytes");
        let store = LocalContentStore::new(&root).expect("create store");
        let manifest = store
            .put_object_version("object-reader", "v1", b"streamed reader bytes")
            .expect("put object version");

        let mut reader = store
            .open_blob_reader(&manifest.content_hash)
            .expect("open reader");
        assert_eq!(reader.size_bytes(), manifest.size_bytes);
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).expect("read blob reader");
        assert_eq!(bytes, b"streamed reader bytes");

        let blob_path = store.blob_path(&manifest.content_hash).expect("blob path");
        write_file_atomic(&blob_path, b"corrupt reader bytes").expect("corrupt blob");
        assert!(matches!(
            store.open_blob_reader(&manifest.content_hash),
            Err(ContentStoreError::BlobHashMismatch { .. })
        ));

        cleanup(&root);
    }

    #[test]
    fn open_blob_stream_streams_bytes_in_a_single_pass() {
        let root = temp_root("open_blob_stream_streams_bytes_in_a_single_pass");
        let store = LocalContentStore::new(&root).expect("create store");
        let payload = b"single pass streamed bytes";
        let manifest = store
            .put_object_version("object-stream", "v1", payload)
            .expect("put object version");

        let mut reader = store
            .open_blob_stream(&manifest.content_hash)
            .expect("open stream");
        assert_eq!(reader.size_bytes(), manifest.size_bytes);
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).expect("read blob stream");
        assert_eq!(bytes, payload);

        // The streaming reader does not verify the content address itself; that is
        // enforced end-to-end by the consuming peer. A missing blob still errors.
        assert!(matches!(
            store.open_blob_stream("blake3:not-a-real-hash"),
            Err(ContentStoreError::InvalidHash(_))
        ));

        cleanup(&root);
    }

    #[test]
    fn read_missing_blob_does_not_create_packed_blob_lock() {
        let root = temp_root("read_missing_blob_does_not_create_packed_blob_lock");
        let store = LocalContentStore::new(&root).expect("create store");
        let missing_hash = content_hash(b"missing read-only packed blob lookup");
        let lock_path = root.join(PACKED_BLOB_DIR).join(PACKED_BLOB_LOCK_FILE);

        assert!(!store.has_blob(&missing_hash).expect("missing blob check"));
        assert!(
            !lock_path.exists(),
            "read-side packed lookup must not create pack.lock"
        );
        assert!(matches!(
            store.open_blob_stream(&missing_hash),
            Err(ContentStoreError::Io { .. })
        ));
        assert!(
            !lock_path.exists(),
            "streaming read miss must not create pack.lock"
        );

        cleanup(&root);
    }

    #[test]
    fn blob_range_stream_reads_bounded_slice_without_full_blob() {
        let root = temp_root("blob_range_stream_reads_bounded_slice");
        let store = LocalContentStore::new(&root).expect("create store");
        let payload = b"0123456789abcdef";
        let manifest = store
            .put_object_version("object-range", "v1", payload)
            .expect("put object version");

        let mut reader = store
            .open_blob_range_stream(&manifest.content_hash, 4, 6)
            .expect("open range stream");
        assert_eq!(reader.offset(), 4);
        assert_eq!(reader.size_bytes(), payload.len() as u64);
        assert_eq!(reader.remaining_bytes(), 6);
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).expect("read range");
        assert_eq!(bytes, b"456789");

        assert_eq!(
            store
                .read_blob_range(&manifest.content_hash, payload.len() as u64 + 8, 16)
                .expect("range past eof"),
            Vec::<u8>::new()
        );
        let (range_manifest, range) = store
            .read_object_version_range("object-range", "v1", 10, 64)
            .expect("object range");
        assert_eq!(range_manifest, manifest);
        assert_eq!(range, b"abcdef");

        cleanup(&root);
    }

    #[test]
    fn extent_range_stream_reuses_manifest_layout_and_opens_each_extent_once() {
        let root = temp_root("extent_range_stream_reuses_manifest_layout");
        let store = LocalContentStore::new(&root).expect("create store");
        let base = b"0123456789abcdef";
        store
            .put_object_version("object-extent-range", "v1", base)
            .expect("put base version");
        let patched = store
            .put_object_version_patch("object-extent-range", "v1", "v2", 6, b"WXYZ")
            .expect("put extent-backed version");
        assert!(patched.extents.len() >= 3);

        let manifest_reads_before = store
            .stream_manifest_file_read_count
            .load(Ordering::Relaxed);
        let opens_before = store.blob_stream_open_count.load(Ordering::Relaxed);
        let (stream_manifest, mut reader) = store
            .open_object_version_range_stream("object-extent-range", "v2", 0, base.len())
            .expect("open extent-backed range");
        assert_eq!(
            store.blob_stream_open_count.load(Ordering::Relaxed),
            opens_before,
            "range construction should validate extent sizes without opening their blobs"
        );
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).expect("stream range");
        assert_eq!(bytes, b"012345WXYZabcdef");
        assert_eq!(
            store
                .blob_stream_open_count
                .load(Ordering::Relaxed)
                .saturating_sub(opens_before),
            stream_manifest.extents.len() as u64,
            "each selected extent should be opened exactly once"
        );
        assert_eq!(
            store
                .stream_manifest_file_read_count
                .load(Ordering::Relaxed),
            manifest_reads_before + 1
        );

        let (_, mut repeated) = store
            .open_object_version_range_stream("object-extent-range", "v2", 5, 6)
            .expect("open cached extent-backed range");
        let mut repeated_bytes = Vec::new();
        repeated
            .read_to_end(&mut repeated_bytes)
            .expect("stream cached range");
        assert_eq!(repeated_bytes, b"5WXYZa");
        assert_eq!(
            store
                .stream_manifest_file_read_count
                .load(Ordering::Relaxed),
            manifest_reads_before + 1,
            "repeated ranges should reuse the immutable validated manifest"
        );

        cleanup(&root);
    }

    #[test]
    fn small_object_versions_are_stored_in_packed_segments() {
        let root = temp_root("small_object_versions_are_stored_in_packed_segments");
        let store = LocalContentStore::new(&root).expect("create store");
        let first_bytes = b"tiny packed object alpha";
        let second_bytes = b"tiny packed object bravo";

        let first = store
            .put_object_version("object-packed-a", "v1", first_bytes)
            .expect("put first packed object");
        let second = store
            .put_object_version("object-packed-b", "v1", second_bytes)
            .expect("put second packed object");

        assert!(
            !store
                .blob_path(&first.content_hash)
                .expect("first loose blob path")
                .exists(),
            "small content should avoid one loose blob file per object"
        );
        assert!(
            !store
                .blob_path(&second.content_hash)
                .expect("second loose blob path")
                .exists(),
            "small content should avoid one loose blob file per object"
        );
        let segment_files = fs::read_dir(root.join(PACKED_BLOB_DIR).join(PACKED_BLOB_SEGMENT_DIR))
            .expect("pack segment dir")
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.path().is_file())
            .count();
        assert_eq!(segment_files, 1);

        assert_eq!(
            store
                .read_blob(&first.content_hash)
                .expect("read first blob"),
            first_bytes
        );
        assert_eq!(
            store
                .read_object_version("object-packed-b", "v1")
                .expect("read second object")
                .1,
            second_bytes
        );
        let mut stream = store
            .open_blob_stream(&first.content_hash)
            .expect("open packed stream");
        let mut streamed = Vec::new();
        stream.read_to_end(&mut streamed).expect("read stream");
        assert_eq!(streamed, first_bytes);
        assert_eq!(
            store
                .read_blob_range(&second.content_hash, 5, 6)
                .expect("read packed range"),
            &second_bytes[5..11]
        );

        let stats = store.stats().expect("packed stats");
        assert_eq!(stats.blob_count, 2);
        assert_eq!(
            stats.blob_bytes,
            first_bytes.len() as u64 + second_bytes.len() as u64
        );
        assert_eq!(stats.manifest_count, 2);
        let scrub = store.scrub().expect("packed scrub");
        assert!(scrub.is_healthy(), "packed scrub issues: {scrub:?}");
        assert_eq!(scrub.referenced_blob_count, 2);
        assert_eq!(scrub.orphan_blob_count, 0);
        assert_eq!(store.stats().expect("stats after scrub"), scrub.stats);

        cleanup(&root);
    }

    #[test]
    fn small_object_packed_index_uses_append_log_instead_of_per_write_snapshot() {
        let root =
            temp_root("small_object_packed_index_uses_append_log_instead_of_per_write_snapshot");
        let store = LocalContentStore::new(&root).expect("create store");
        let first_bytes = b"tiny packed index log alpha";
        let second_bytes = b"tiny packed index log bravo";

        let first = store
            .put_object_version("object-packed-index-log-a", "v1", first_bytes)
            .expect("put first packed object");
        let second = store
            .put_object_version("object-packed-index-log-b", "v1", second_bytes)
            .expect("put second packed object");

        assert!(
            store.packed_blob_index_log_path().is_file(),
            "small packed writes should append compact index records"
        );
        assert!(
            !store.packed_blob_index_path().exists(),
            "small packed writes should not rewrite the full index snapshot on every object"
        );

        let reopened = LocalContentStore::new(&root).expect("reopen store");
        assert_eq!(
            reopened
                .read_blob(&first.content_hash)
                .expect("read first packed blob after replay"),
            first_bytes
        );
        assert_eq!(
            reopened
                .read_blob(&second.content_hash)
                .expect("read second packed blob after replay"),
            second_bytes
        );
        let index = reopened
            .read_packed_blob_index_unlocked()
            .expect("replayed packed index");
        assert_eq!(index.entries.len(), 2);

        cleanup(&root);
    }

    #[test]
    fn packed_blob_index_cache_is_invalidated_after_local_write() {
        let root = temp_root("packed_blob_index_cache_is_invalidated_after_local_write");
        let store = LocalContentStore::new(&root).expect("create store");

        let first = store
            .put_object_version("object-packed-cache-a", "v1", b"packed cache alpha")
            .expect("put first packed object");
        assert!(
            store
                .packed_blob_location(&first.content_hash)
                .expect("populate packed cache")
                .is_some()
        );

        let second = store
            .put_object_version("object-packed-cache-b", "v1", b"packed cache bravo")
            .expect("put second packed object");
        assert!(
            store
                .packed_blob_location(&second.content_hash)
                .expect("read invalidated packed cache")
                .is_some(),
            "same-process packed writes must invalidate the cached index"
        );

        cleanup(&root);
    }

    #[test]
    fn packed_blob_index_cache_observes_external_writer() {
        let root = temp_root("packed_blob_index_cache_observes_external_writer");
        let reader = LocalContentStore::new(&root).expect("reader store");
        let writer = LocalContentStore::new(&root).expect("writer store");

        let first = writer
            .put_object_version("object-packed-external-a", "v1", b"packed external alpha")
            .expect("put first packed object");
        assert_eq!(
            reader
                .read_blob(&first.content_hash)
                .expect("read first packed blob"),
            b"packed external alpha"
        );

        let second = writer
            .put_object_version("object-packed-external-b", "v1", b"packed external bravo")
            .expect("put second packed object");
        assert_eq!(
            reader
                .read_blob(&second.content_hash)
                .expect("reader must observe externally updated packed index"),
            b"packed external bravo",
            "packed index cache must not hide blobs written by another process"
        );

        cleanup(&root);
    }

    #[test]
    fn content_writes_do_not_create_stats_snapshot_until_stats_are_requested() {
        let root =
            temp_root("content_writes_do_not_create_stats_snapshot_until_stats_are_requested");
        let store = LocalContentStore::new(&root).expect("create store");

        store
            .put_object_version("object-lazy-stats", "v1", b"lazy stats alpha")
            .expect("put first object");
        assert!(
            !store.stats_snapshot_path().exists(),
            "ordinary writes without quotas should not force a stats snapshot write"
        );
        assert!(
            !root.join(CONTENT_STORE_STATS_LOCK_FILE).exists(),
            "ordinary writes without quotas should not force a stats lock"
        );

        let initial = store.stats().expect("stats scan materializes snapshot");
        assert_eq!(initial.blob_count, 1);
        assert_eq!(initial.manifest_count, 1);
        assert!(store.stats_snapshot_path().exists());
        assert!(
            !store.tracks_stats_deltas(),
            "derived status state must not become synchronous write authority"
        );
        let snapshot_before_second_write =
            read_file(&store.stats_snapshot_path()).expect("read initial derived snapshot");

        store
            .put_object_version("object-lazy-stats", "v2", b"lazy stats bravo")
            .expect("put second object");
        assert_eq!(
            read_file(&store.stats_snapshot_path()).expect("read lagging derived snapshot"),
            snapshot_before_second_write,
            "no-quota writes must not rewrite a status snapshot"
        );
        let updated = store.stats().expect("derived stats rescan durable content");
        assert_eq!(updated.blob_count, 2);
        assert_eq!(updated.manifest_count, 2);
        assert_eq!(
            updated.logical_object_bytes,
            b"lazy stats alpha".len() as u64 + b"lazy stats bravo".len() as u64
        );

        cleanup(&root);
    }

    #[test]
    fn no_quota_reopen_ignores_legacy_stale_stats_snapshot() {
        let root = temp_root("no_quota_reopen_ignores_legacy_stale_stats_snapshot");
        let store = LocalContentStore::new(&root).expect("create store");
        store
            .put_object_version("object-legacy-a", "v1", b"legacy alpha")
            .expect("put first object");
        let initial = store.stats().expect("materialize legacy snapshot");
        assert_eq!(initial.blob_count, 1);
        assert_eq!(initial.manifest_count, 1);
        let stale_snapshot = read_file(&store.stats_snapshot_path()).expect("read legacy snapshot");

        store
            .put_object_version("object-legacy-b", "v1", b"legacy bravo")
            .expect("put second object without synchronous stats");
        assert_eq!(
            read_file(&store.stats_snapshot_path()).expect("read stale snapshot"),
            stale_snapshot
        );
        drop(store);

        let reopened = LocalContentStore::new(&root).expect("reopen store");
        assert!(!reopened.tracks_stats_deltas());
        let rebuilt = reopened.stats().expect("rebuild derived stats on reopen");
        assert_eq!(rebuilt.blob_count, 2);
        assert_eq!(rebuilt.manifest_count, 2);
        assert_eq!(
            rebuilt.logical_object_bytes,
            b"legacy alpha".len() as u64 + b"legacy bravo".len() as u64
        );

        cleanup(&root);
    }

    #[test]
    fn put_object_version_patch_reuses_base_extents_and_reads_logical_bytes() {
        let root = temp_root("put_object_version_patch_reuses_base_extents");
        let store = LocalContentStore::new(&root).expect("create store");
        let mut base = vec![0_u8; 192 * 1024 + 13];
        for (index, byte) in base.iter_mut().enumerate() {
            *byte = (index % 251) as u8;
        }
        let base_manifest = store
            .put_object_version("object-patch", "v1", &base)
            .expect("put base object");
        let patch = b"extent-patch";
        let patch_hash = content_hash(patch);
        let patch_offset = 96 * 1024 + 7;
        let mut expected = base.clone();
        expected[patch_offset..patch_offset + patch.len()].copy_from_slice(patch);

        let patched_info = store
            .put_object_version_patch_with_info(
                "object-patch",
                "v1",
                "v2",
                patch_offset as u64,
                patch,
            )
            .expect("put patched object");
        assert_eq!(
            patched_info.patch_hash.as_deref(),
            Some(patch_hash.as_str())
        );
        assert_eq!(patched_info.patch_size_bytes, patch.len() as u64);
        let patched = patched_info.manifest;

        assert_eq!(patched.size_bytes, expected.len() as u64);
        assert_eq!(patched.content_hash, content_hash(&expected));
        assert!(
            patched
                .extents
                .iter()
                .any(|extent| extent.content_hash == base_manifest.content_hash),
            "patched manifest should reference unchanged base content"
        );
        assert!(
            patched
                .extents
                .iter()
                .any(|extent| extent.content_hash == patch_hash),
            "patched manifest should reference only the changed bytes as a patch blob"
        );
        assert!(
            !store
                .has_blob(&patched.content_hash)
                .expect("logical blob check"),
            "patched versions should not rewrite unchanged bytes into a full logical blob"
        );
        let stats = store.stats().expect("content stats");
        assert_eq!(stats.blob_count, 2);
        assert_eq!(stats.manifest_count, 2);
        assert_eq!(stats.extent_manifest_count, 1);
        assert_eq!(stats.extent_count, patched.extents.len() as u64);
        assert!(stats.extent_count >= 3);
        assert_eq!(
            stats.logical_object_bytes,
            base.len() as u64 + expected.len() as u64
        );
        assert_eq!(stats.staging_file_count, 0);
        let scrub = store.scrub().expect("content scrub");
        assert!(scrub.is_healthy(), "unexpected scrub issues: {scrub:?}");
        assert_eq!(scrub.referenced_blob_count, 2);
        assert_eq!(
            scrub.referenced_blob_bytes,
            base.len() as u64 + patch.len() as u64
        );
        assert_eq!(scrub.orphan_blob_count, 0);
        assert_eq!(scrub.stats, stats);
        assert!(
            store
                .manifest_content_available(&patched)
                .expect("manifest availability")
        );

        let (read_manifest, read_bytes) = store
            .read_object_version("object-patch", "v2")
            .expect("read patched object");
        assert_eq!(read_manifest, patched);
        assert_eq!(read_bytes, expected);

        let range_offset = patch_offset as u64 - 8;
        let (_, range) = store
            .read_object_version_range("object-patch", "v2", range_offset, patch.len() + 16)
            .expect("read patched range");
        assert_eq!(
            range,
            expected[range_offset as usize..range_offset as usize + patch.len() + 16]
        );

        let (_, identity_range) = store
            .read_object_content_range(
                "object-patch",
                &patched.content_hash,
                patched.size_bytes,
                range_offset,
                patch.len() + 16,
            )
            .expect("read patched identity range")
            .expect("indexed patched identity");
        assert_eq!(identity_range, range);
        assert!(
            store
                .content_manifest_index_path("object-patch", &patched.content_hash)
                .expect("content manifest index path")
                .is_file(),
            "extent manifests must have a durable O(1) content-identity index"
        );

        drop(store);
        let reopened = LocalContentStore::new(&root).expect("reopen content store");
        let (_, reopened_range) = reopened
            .read_object_content_range(
                "object-patch",
                &patched.content_hash,
                patched.size_bytes,
                range_offset,
                patch.len() + 16,
            )
            .expect("read patched identity range after reopen")
            .expect("persisted patched identity index");
        assert_eq!(reopened_range, range);

        cleanup(&root);
    }

    #[test]
    fn scrub_reports_missing_corrupt_and_orphan_blobs() {
        let root = temp_root("scrub_reports_content_anomalies");
        let store = LocalContentStore::new(&root).expect("create store");
        let missing_bytes = vec![b'm'; PACKED_BLOB_SMALL_LIMIT_BYTES as usize + 1];
        let corrupt_bytes = vec![b'c'; PACKED_BLOB_SMALL_LIMIT_BYTES as usize + 2];
        let missing = store
            .put_object_version("object-missing", "v1", &missing_bytes)
            .expect("put missing object");
        let corrupt = store
            .put_object_version("object-corrupt", "v1", &corrupt_bytes)
            .expect("put corrupt object");
        fs::remove_file(
            store
                .blob_path(&missing.content_hash)
                .expect("missing blob path"),
        )
        .expect("remove referenced blob");
        write_file_atomic(
            &store
                .blob_path(&corrupt.content_hash)
                .expect("corrupt blob path"),
            b"not the bytes named by this content hash",
        )
        .expect("overwrite blob with corrupt bytes");
        let orphan_bytes = b"valid but unreferenced blob";
        let orphan_hash = content_hash(orphan_bytes);
        write_file_atomic(
            &store.blob_path(&orphan_hash).expect("orphan blob path"),
            orphan_bytes,
        )
        .expect("write orphan blob");

        let scrub = store.scrub().expect("content scrub");

        assert!(!scrub.is_healthy());
        assert_eq!(scrub.stats.manifest_count, 2);
        assert_eq!(scrub.referenced_blob_count, 2);
        assert_eq!(scrub.orphan_blob_count, 1);
        assert_eq!(scrub.orphan_blob_bytes, orphan_bytes.len() as u64);
        assert!(scrub.issues.iter().any(|issue| matches!(
            issue,
            ContentStoreScrubIssue::MissingBlob {
                object_id,
                content_hash,
                ..
            } if object_id == "object-missing" && content_hash == &missing.content_hash
        )));
        assert!(scrub.issues.iter().any(|issue| matches!(
            issue,
            ContentStoreScrubIssue::CorruptBlob {
                object_id,
                content_hash,
                ..
            } if object_id == "object-corrupt" && content_hash == &corrupt.content_hash
        )));
        assert!(scrub.issues.iter().any(|issue| matches!(
            issue,
            ContentStoreScrubIssue::OrphanBlob {
                content_hash,
                size_bytes,
                ..
            } if content_hash == &orphan_hash && *size_bytes == orphan_bytes.len() as u64
        )));

        cleanup(&root);
    }

    #[test]
    fn no_quota_stats_rescan_observes_out_of_band_durable_content() {
        let root = temp_root("no_quota_stats_rescan_observes_out_of_band_durable_content");
        let store = LocalContentStore::new(&root).expect("create store");
        store
            .put_object_version("object-live", "v1", b"live referenced blob")
            .expect("put live object");
        let cached = store.stats().expect("initial stats");
        assert_eq!(cached.blob_count, 1);
        assert_eq!(cached.manifest_count, 1);

        let orphan_bytes = b"out-of-band orphan blob";
        let orphan_hash = content_hash(orphan_bytes);
        write_file_atomic(
            &store.blob_path(&orphan_hash).expect("orphan blob path"),
            orphan_bytes,
        )
        .expect("write orphan outside store counters");

        let rescanned = store.stats().expect("rescan derived stats");
        assert_eq!(rescanned.blob_count, 2);
        assert_eq!(rescanned.manifest_count, cached.manifest_count);
        assert_eq!(
            rescanned.blob_bytes,
            cached.blob_bytes + orphan_bytes.len() as u64
        );

        let scrub = store.scrub().expect("scrub rebuilds stats");
        assert_eq!(scrub.orphan_blob_count, 1);
        assert_eq!(scrub.stats.blob_count, 2);
        assert_eq!(store.stats().expect("rebuilt stats"), scrub.stats);

        cleanup(&root);
    }

    #[test]
    fn quota_entry_rebuilds_stale_snapshot_then_updates_synchronously() {
        let root = temp_root("quota_entry_rebuilds_stale_snapshot_then_updates_synchronously");
        let store = LocalContentStore::new(&root).expect("create store");
        store
            .put_object_version("object-prequota-a", "v1", b"abcd")
            .expect("put first pre-quota object");
        let initial = store.stats().expect("materialize pre-quota snapshot");
        assert_eq!(initial.blob_bytes, 4);
        let stale_snapshot =
            read_file(&store.stats_snapshot_path()).expect("read pre-quota snapshot");
        store
            .put_object_version("object-prequota-b", "v1", b"efgh")
            .expect("put second pre-quota object");
        assert_eq!(
            read_file(&store.stats_snapshot_path()).expect("read stale pre-quota snapshot"),
            stale_snapshot
        );
        drop(store);

        let quota_store = LocalContentStore::new(&root)
            .expect("reopen quota store")
            .with_quota(ContentStoreQuota {
                max_blob_bytes: Some(9),
                ..ContentStoreQuota::default()
            });
        assert!(quota_store.tracks_stats_deltas());
        let error = quota_store
            .put_object_version("object-quota-reject", "v1", b"ij")
            .expect_err("durable eight-byte usage plus two bytes exceeds quota");
        assert!(matches!(
            error,
            ContentStoreError::QuotaExceeded {
                resource: ContentStoreQuotaResource::BlobBytes,
                limit_bytes: 9,
                current_bytes: 8,
                additional_bytes: 2,
            }
        ));

        let quota_store = quota_store.with_quota(ContentStoreQuota {
            max_blob_bytes: Some(10),
            ..ContentStoreQuota::default()
        });
        quota_store
            .put_object_version("object-quota-accepted", "v1", b"ij")
            .expect("exact quota boundary accepts");
        assert_eq!(quota_store.stats().expect("quota usage").blob_bytes, 10);

        let error = quota_store
            .put_object_version("object-quota-followup", "v1", b"k")
            .expect_err("synchronous quota delta blocks follow-up growth");
        assert!(matches!(
            error,
            ContentStoreError::QuotaExceeded {
                resource: ContentStoreQuotaResource::BlobBytes,
                limit_bytes: 10,
                current_bytes: 10,
                additional_bytes: 1,
            }
        ));

        cleanup(&root);
    }

    #[test]
    fn collect_garbage_deletes_only_retention_eligible_orphans() {
        let root = temp_root("collect_garbage_deletes_eligible_orphans");
        let store = LocalContentStore::new(&root).expect("create store");
        let live = store
            .put_object_version("object-live", "v1", b"live referenced blob")
            .expect("put live object");
        let orphan_a_bytes = b"orphan blob a";
        let orphan_a_hash = content_hash(orphan_a_bytes);
        write_file_atomic(
            &store.blob_path(&orphan_a_hash).expect("orphan a path"),
            orphan_a_bytes,
        )
        .expect("write orphan a");
        let orphan_b_bytes = b"orphan blob b";
        let orphan_b_hash = content_hash(orphan_b_bytes);
        write_file_atomic(
            &store.blob_path(&orphan_b_hash).expect("orphan b path"),
            orphan_b_bytes,
        )
        .expect("write orphan b");

        let retained = store
            .collect_garbage(ContentStoreGcOptions::delete_eligible(Duration::from_secs(
                24 * 60 * 60,
            )))
            .expect("retention gc");
        assert_eq!(retained.deleted_orphan_count, 0);
        assert_eq!(retained.eligible_orphan_count, 0);
        assert_eq!(retained.retained_young_orphan_count, 2);
        assert_eq!(
            retained.retained_young_orphan_bytes,
            orphan_a_bytes.len() as u64 + orphan_b_bytes.len() as u64
        );
        assert!(store.has_blob(&orphan_a_hash).expect("orphan a retained"));
        assert!(store.has_blob(&orphan_b_hash).expect("orphan b retained"));

        let limited = store
            .collect_garbage(
                ContentStoreGcOptions::delete_eligible(Duration::from_nanos(0))
                    .with_max_deletions(1),
            )
            .expect("limited gc");
        assert_eq!(limited.blocked_by_scrub_issue_count, 0);
        assert_eq!(limited.eligible_orphan_count, 2);
        assert_eq!(limited.deleted_orphan_count, 1);
        assert!(limited.max_deletions_reached);
        assert_eq!(limited.retained_limit_orphan_count, 1);
        assert!(store.has_blob(&live.content_hash).expect("live retained"));
        let remaining_orphans = [&orphan_a_hash, &orphan_b_hash]
            .into_iter()
            .filter(|hash| store.has_blob(hash).expect("orphan exists check"))
            .count();
        assert_eq!(remaining_orphans, 1);

        let final_gc = store
            .collect_garbage(ContentStoreGcOptions::delete_eligible(
                Duration::from_nanos(0),
            ))
            .expect("final gc");
        assert_eq!(final_gc.deleted_orphan_count, 1);
        assert!(
            store
                .has_blob(&live.content_hash)
                .expect("live still retained")
        );
        assert!(!store.has_blob(&orphan_a_hash).expect("orphan a gone"));
        assert!(!store.has_blob(&orphan_b_hash).expect("orphan b gone"));
        let scrub = store.scrub().expect("post-gc scrub");
        assert!(scrub.is_healthy(), "post-gc scrub issues: {scrub:?}");

        cleanup(&root);
    }

    #[test]
    fn collect_garbage_dry_run_and_blocks_on_non_orphan_scrub_issues() {
        let root = temp_root("collect_garbage_dry_run_blocks_on_corrupt");
        let store = LocalContentStore::new(&root).expect("create store");
        let live = store
            .put_object_version("object-live", "v1", b"live referenced blob")
            .expect("put live object");
        let orphan_bytes = b"dry run orphan blob";
        let orphan_hash = content_hash(orphan_bytes);
        write_file_atomic(
            &store.blob_path(&orphan_hash).expect("orphan path"),
            orphan_bytes,
        )
        .expect("write orphan");

        let dry_run = store
            .collect_garbage(ContentStoreGcOptions::dry_run(Duration::from_nanos(0)))
            .expect("dry-run gc");
        assert_eq!(dry_run.blocked_by_scrub_issue_count, 0);
        assert!(dry_run.dry_run);
        assert_eq!(dry_run.eligible_orphan_count, 1);
        assert_eq!(dry_run.deleted_orphan_count, 0);
        assert!(store.has_blob(&orphan_hash).expect("dry-run retained"));

        write_file_atomic(
            &store.blob_path(&live.content_hash).expect("live blob path"),
            b"corrupt live bytes",
        )
        .expect("corrupt referenced blob");
        let blocked = store
            .collect_garbage(ContentStoreGcOptions::delete_eligible(
                Duration::from_nanos(0),
            ))
            .expect("blocked gc");
        assert_eq!(blocked.blocked_by_scrub_issue_count, 1);
        assert_eq!(blocked.eligible_orphan_count, 0);
        assert_eq!(blocked.deleted_orphan_count, 0);
        assert!(
            store
                .has_blob(&orphan_hash)
                .expect("blocked gc retained orphan")
        );

        cleanup(&root);
    }

    #[test]
    fn collect_versions_prunes_old_manifests_then_blob_gc_reclaims_orphans() {
        let root = temp_root("collect_versions_prunes_old_manifests");
        let store = LocalContentStore::new(&root).expect("create store");
        let version_1 = store
            .put_object_version("object-history", "version_1", b"history bytes v1")
            .expect("put version 1");
        let version_2 = store
            .put_object_version("object-history", "version_2", b"history bytes v2")
            .expect("put version 2");
        let version_3 = store
            .put_object_version("object-history", "version_3", b"history bytes v3")
            .expect("put version 3");

        let dry_run = store
            .collect_versions(ContentStoreVersionGcOptions::dry_run(
                Duration::from_nanos(0),
                1,
            ))
            .expect("dry-run version gc");
        assert!(dry_run.dry_run);
        assert_eq!(dry_run.scanned_object_count, 1);
        assert_eq!(dry_run.scanned_version_count, 3);
        assert_eq!(dry_run.retained_latest_version_count, 1);
        assert_eq!(dry_run.eligible_version_count, 2);
        assert_eq!(dry_run.deleted_version_count, 0);
        assert_eq!(
            store
                .manifests_for_object("object-history")
                .expect("manifests after dry run")
                .len(),
            3
        );

        let retained = store
            .collect_versions(ContentStoreVersionGcOptions::delete_eligible(
                Duration::from_secs(24 * 60 * 60),
                1,
            ))
            .expect("retention version gc");
        assert_eq!(retained.eligible_version_count, 0);
        assert_eq!(retained.deleted_version_count, 0);
        assert_eq!(retained.retained_latest_version_count, 1);
        assert_eq!(retained.retained_young_version_count, 2);

        let limited = store
            .collect_versions(
                ContentStoreVersionGcOptions::delete_eligible(Duration::from_nanos(0), 1)
                    .with_max_deletions(1),
            )
            .expect("limited version gc");
        assert_eq!(limited.blocked_by_scrub_issue_count, 0);
        assert_eq!(limited.eligible_version_count, 2);
        assert_eq!(limited.deleted_version_count, 1);
        assert!(limited.max_deletions_reached);
        assert_eq!(limited.retained_limit_version_count, 1);
        assert_eq!(
            store
                .manifests_for_object("object-history")
                .expect("manifests after limited delete")
                .len(),
            2
        );

        let final_version_gc = store
            .collect_versions(ContentStoreVersionGcOptions::delete_eligible(
                Duration::from_nanos(0),
                1,
            ))
            .expect("final version gc");
        assert_eq!(final_version_gc.deleted_version_count, 1);
        let manifests = store
            .manifests_for_object("object-history")
            .expect("remaining manifests");
        assert_eq!(manifests.len(), 1);
        assert_eq!(manifests[0].version, version_3.version);
        assert_eq!(
            store
                .read_object_version("object-history", "version_3")
                .expect("latest remains")
                .1,
            b"history bytes v3"
        );

        let scrub = store.scrub().expect("scrub after version gc");
        assert_eq!(scrub.orphan_blob_count, 2);
        assert!(store.has_blob(&version_1.content_hash).expect("v1 orphan"));
        assert!(store.has_blob(&version_2.content_hash).expect("v2 orphan"));
        assert!(store.has_blob(&version_3.content_hash).expect("v3 live"));

        let blob_gc = store
            .collect_garbage(ContentStoreGcOptions::delete_eligible(
                Duration::from_nanos(0),
            ))
            .expect("blob gc after version gc");
        assert_eq!(blob_gc.deleted_orphan_count, 2);
        assert!(
            !store
                .has_blob(&version_1.content_hash)
                .expect("v1 reclaimed")
        );
        assert!(
            !store
                .has_blob(&version_2.content_hash)
                .expect("v2 reclaimed")
        );
        assert!(
            store
                .has_blob(&version_3.content_hash)
                .expect("v3 retained")
        );
        let final_scrub = store.scrub().expect("final scrub");
        assert!(
            final_scrub.is_healthy(),
            "final scrub issues: {final_scrub:?}"
        );

        cleanup(&root);
    }

    #[test]
    fn collect_versions_evicts_deleted_stream_manifest_cache_entry() {
        let root = temp_root("collect_versions_evicts_stream_manifest_cache");
        let store = LocalContentStore::new(&root).expect("create store");
        store
            .put_object_version("object-cache-gc", "version_1", b"old bytes")
            .expect("put old version");
        store
            .put_object_version("object-cache-gc", "version_2", b"new bytes")
            .expect("put latest version");
        let (_, mut old_reader) = store
            .open_object_version_range_stream("object-cache-gc", "version_1", 0, usize::MAX)
            .expect("cache old manifest");
        let mut old_bytes = Vec::new();
        old_reader
            .read_to_end(&mut old_bytes)
            .expect("read old version");
        assert_eq!(old_bytes, b"old bytes");

        let report = store
            .collect_versions(ContentStoreVersionGcOptions::delete_eligible(
                Duration::from_nanos(0),
                1,
            ))
            .expect("collect old version");
        assert_eq!(report.deleted_version_count, 1);
        assert!(
            store
                .open_object_version_range_stream("object-cache-gc", "version_1", 0, usize::MAX,)
                .is_err(),
            "a collected manifest must not remain readable through the stream cache"
        );

        cleanup(&root);
    }

    #[test]
    fn collect_versions_removes_deleted_manifest_index_and_rebuilds_from_survivor() {
        let root = temp_root("collect_versions_rebuilds_content_index");
        let store = LocalContentStore::new(&root).expect("create store");
        let version_1 = store
            .put_object_version("object-shared-content", "version_1", b"shared bytes")
            .expect("put version 1");
        let version_2 = store
            .put_object_version("object-shared-content", "version_2", b"shared bytes")
            .expect("put version 2");
        assert_eq!(version_1.content_hash, version_2.content_hash);

        let index_path = store
            .content_manifest_index_path("object-shared-content", &version_1.content_hash)
            .expect("content index path");
        let indexed = decode_manifest(&read_file(&index_path).expect("read initial index"))
            .expect("decode initial index");
        assert_eq!(indexed.version, version_1.version);

        let report = store
            .collect_versions(ContentStoreVersionGcOptions::delete_eligible(
                Duration::from_nanos(0),
                1,
            ))
            .expect("collect old version");
        assert_eq!(report.deleted_version_count, 1);
        assert!(
            !store
                .manifest_path("object-shared-content", &version_1.version)
                .is_file()
        );
        assert!(
            !index_path.is_file(),
            "the index hardlink must not retain a collected manifest"
        );

        let (_, bytes) = store
            .read_object_content_range(
                "object-shared-content",
                &version_2.content_hash,
                version_2.size_bytes,
                0,
                usize::MAX,
            )
            .expect("read surviving content identity")
            .expect("surviving content identity");
        assert_eq!(bytes, b"shared bytes");
        let rebuilt = decode_manifest(&read_file(&index_path).expect("read rebuilt index"))
            .expect("decode rebuilt index");
        assert_eq!(rebuilt.version, version_2.version);

        cleanup(&root);
    }

    #[test]
    fn collect_versions_blocks_on_non_orphan_scrub_issues() {
        let root = temp_root("collect_versions_blocks_on_corrupt");
        let store = LocalContentStore::new(&root).expect("create store");
        let version_1 = store
            .put_object_version("object-history", "version_1", b"history bytes v1")
            .expect("put version 1");
        store
            .put_object_version("object-history", "version_2", b"history bytes v2")
            .expect("put version 2");
        write_file_atomic(
            &store
                .blob_path(&version_1.content_hash)
                .expect("version 1 blob path"),
            b"corrupt old version bytes",
        )
        .expect("corrupt referenced old version");

        let blocked = store
            .collect_versions(ContentStoreVersionGcOptions::delete_eligible(
                Duration::from_nanos(0),
                1,
            ))
            .expect("blocked version gc");

        assert_eq!(blocked.blocked_by_scrub_issue_count, 1);
        assert_eq!(blocked.scanned_version_count, 0);
        assert_eq!(blocked.deleted_version_count, 0);
        assert_eq!(
            store
                .manifests_for_object("object-history")
                .expect("manifests after blocked gc")
                .len(),
            2
        );

        cleanup(&root);
    }

    #[test]
    fn quota_rejects_blob_logical_and_manifest_growth_without_side_effects() {
        let root = temp_root("quota-rejects-growth");
        let store = LocalContentStore::new(&root)
            .expect("create store")
            .with_quota(ContentStoreQuota {
                max_blob_bytes: Some(4),
                ..ContentStoreQuota::default()
            });

        let error = store
            .put_object_version("object-quota", "v1", b"abcde")
            .expect_err("blob quota should reject write");
        assert!(matches!(
            error,
            ContentStoreError::QuotaExceeded {
                resource: ContentStoreQuotaResource::BlobBytes,
                limit_bytes: 4,
                current_bytes: 0,
                additional_bytes: 5,
            }
        ));
        assert_eq!(
            store
                .manifests_for_object("object-quota")
                .expect("manifests after rejected blob write")
                .len(),
            0
        );
        assert_eq!(store.stats().expect("stats").blob_bytes, 0);

        let store = store.with_quota(ContentStoreQuota {
            max_blob_bytes: Some(128),
            max_logical_object_bytes: Some(8),
            ..ContentStoreQuota::default()
        });
        store
            .put_object_version("object-quota", "v1", b"12345")
            .expect("first logical write fits");
        let error = store
            .put_object_version("object-quota", "v2", b"abcdef")
            .expect_err("logical quota should reject second version");
        assert!(matches!(
            error,
            ContentStoreError::QuotaExceeded {
                resource: ContentStoreQuotaResource::LogicalObjectBytes,
                limit_bytes: 8,
                current_bytes: 5,
                additional_bytes: 6,
            }
        ));
        assert_eq!(
            store
                .manifests_for_object("object-quota")
                .expect("manifests after rejected logical write")
                .len(),
            1
        );

        let store = store.with_quota(ContentStoreQuota {
            max_blob_bytes: Some(128),
            max_logical_object_bytes: Some(128),
            max_manifest_bytes: Some(1),
            ..ContentStoreQuota::default()
        });
        let error = store
            .put_object_version("object-manifest-quota", "v1", b"small")
            .expect_err("manifest quota should reject write");
        assert!(matches!(
            error,
            ContentStoreError::QuotaExceeded {
                resource: ContentStoreQuotaResource::ManifestBytes,
                limit_bytes: 1,
                ..
            }
        ));
        assert_eq!(
            store
                .manifests_for_object("object-manifest-quota")
                .expect("manifests after rejected manifest write")
                .len(),
            0
        );
        assert!(
            !store
                .has_blob(&content_hash(b"small"))
                .expect("manifest quota should reject before blob write")
        );

        cleanup(&root);
    }

    #[test]
    fn quota_enforces_staging_and_patch_blob_growth() {
        let root = temp_root("quota-staging-and-patch");
        let store = LocalContentStore::new(&root)
            .expect("create store")
            .with_quota(ContentStoreQuota {
                max_staging_bytes: Some(4),
                ..ContentStoreQuota::default()
            });
        let error = store
            .stage_bytes("intent-too-large", b"12345")
            .expect_err("staging quota should reject direct stage");
        assert!(matches!(
            error,
            ContentStoreError::QuotaExceeded {
                resource: ContentStoreQuotaResource::StagingBytes,
                limit_bytes: 4,
                current_bytes: 0,
                additional_bytes: 5,
            }
        ));
        assert_eq!(store.stats().expect("stats").staging_bytes, 0);
        let store = store.with_quota(ContentStoreQuota {
            max_blob_bytes: Some(12),
            max_logical_object_bytes: Some(128),
            max_manifest_bytes: Some(4096),
            ..ContentStoreQuota::default()
        });
        store
            .put_object_version("object-patch-quota", "v1", b"abcdefgh")
            .expect("base write fits");
        let error = store
            .put_object_version_patch("object-patch-quota", "v1", "v2", 2, b"patch")
            .expect_err("patch blob should exceed blob quota");
        assert!(matches!(
            error,
            ContentStoreError::QuotaExceeded {
                resource: ContentStoreQuotaResource::BlobBytes,
                limit_bytes: 12,
                current_bytes: 8,
                additional_bytes: 5,
            }
        ));
        assert_eq!(
            store
                .manifests_for_object("object-patch-quota")
                .expect("manifests after rejected patch")
                .len(),
            1
        );
        assert!(
            !store
                .has_blob(&content_hash(b"patch"))
                .expect("rejected patch blob should not be written")
        );

        cleanup(&root);
    }

    #[test]
    fn put_object_version_patch_from_reader_streams_patch_blob() {
        struct ChunkedReader {
            bytes: Vec<u8>,
            offset: usize,
            max_chunk: usize,
            reads: std::rc::Rc<std::cell::Cell<usize>>,
        }

        impl Read for ChunkedReader {
            fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
                self.reads.set(self.reads.get().saturating_add(1));
                if self.offset >= self.bytes.len() || buffer.is_empty() {
                    return Ok(0);
                }
                let len = buffer
                    .len()
                    .min(self.max_chunk)
                    .min(self.bytes.len() - self.offset);
                buffer[..len].copy_from_slice(&self.bytes[self.offset..self.offset + len]);
                self.offset += len;
                Ok(len)
            }
        }

        let root = temp_root("put_object_version_patch_from_reader_streams_patch_blob");
        let store = LocalContentStore::new(&root).expect("create store");
        let mut base = vec![0_u8; 320 * 1024 + 29];
        for (index, byte) in base.iter_mut().enumerate() {
            *byte = (index % 251) as u8;
        }
        let base_manifest = store
            .put_object_version("object-stream-patch", "v1", &base)
            .expect("put base object");
        let patch = (0..96 * 1024 + 31)
            .map(|index| (index % 239) as u8)
            .collect::<Vec<_>>();
        let patch_hash = content_hash(&patch);
        let patch_offset = 144 * 1024 + 17;
        let mut expected = base.clone();
        expected[patch_offset..patch_offset + patch.len()].copy_from_slice(&patch);
        let reads = std::rc::Rc::new(std::cell::Cell::new(0));

        let patched_info = store
            .put_object_version_patch_from_reader_with_info(
                "object-stream-patch",
                "v1",
                "v2",
                patch_offset as u64,
                ChunkedReader {
                    bytes: patch.clone(),
                    offset: 0,
                    max_chunk: 4093,
                    reads: reads.clone(),
                },
            )
            .expect("put streamed patch object");
        assert_eq!(
            patched_info.patch_hash.as_deref(),
            Some(patch_hash.as_str())
        );
        assert_eq!(patched_info.patch_size_bytes, patch.len() as u64);
        let patched = patched_info.manifest;

        assert!(
            reads.get() > 1,
            "streamed patch reader should be consumed incrementally"
        );
        assert_eq!(patched.size_bytes, expected.len() as u64);
        assert_eq!(patched.content_hash, content_hash(&expected));
        assert!(
            patched
                .extents
                .iter()
                .any(|extent| extent.content_hash == base_manifest.content_hash)
        );
        assert!(
            patched
                .extents
                .iter()
                .any(|extent| extent.content_hash == patch_hash)
        );
        assert!(
            !store
                .has_blob(&patched.content_hash)
                .expect("logical blob check"),
            "streamed patch should not materialize the full logical blob"
        );
        let stats = store.stats().expect("content stats");
        assert_eq!(stats.staging_file_count, 0);
        assert_eq!(stats.blob_count, 2);
        assert!(stats.extent_count >= 3);
        let (_, range) = store
            .read_object_version_range(
                "object-stream-patch",
                "v2",
                patch_offset as u64 - 11,
                patch.len() + 22,
            )
            .expect("read streamed patch range");
        assert_eq!(
            range,
            expected[patch_offset - 11..patch_offset + patch.len() + 11]
        );

        cleanup(&root);
    }

    #[test]
    fn large_patch_uses_hash_tree_sidecar_without_materializing_logical_blob() {
        let root = temp_root("large-patch-hash-tree-sidecar");
        let store = LocalContentStore::new(&root).expect("create store");
        let base_len = (CONTENT_HASH_TREE_LEAF_BYTES * 3 + 17) as usize;
        let mut base = vec![0_u8; base_len];
        for (index, byte) in base.iter_mut().enumerate() {
            *byte = ((index * 31 + 7) % 251) as u8;
        }
        let base_manifest = store
            .put_object_version("object-large-patch", "v1", &base)
            .expect("put base object");

        let first_offset = CONTENT_HASH_TREE_LEAF_BYTES as usize + 123;
        let first_patch = (0..4097)
            .map(|index| ((index * 17 + 3) % 241) as u8)
            .collect::<Vec<_>>();
        let mut expected = base.clone();
        expected[first_offset..first_offset + first_patch.len()].copy_from_slice(&first_patch);
        let first = store
            .put_object_version_patch(
                "object-large-patch",
                "v1",
                "v2",
                first_offset as u64,
                &first_patch,
            )
            .expect("put first patch");
        assert_eq!(first.content_hash, content_hash(&expected));
        assert!(
            !store
                .has_blob(&first.content_hash)
                .expect("first logical blob"),
            "large patch should keep the logical object as extents"
        );

        let second_offset = (CONTENT_HASH_TREE_LEAF_BYTES * 2) as usize + 211;
        let second_patch = (0..8193)
            .map(|index| ((index * 13 + 11) % 239) as u8)
            .collect::<Vec<_>>();
        expected[second_offset..second_offset + second_patch.len()].copy_from_slice(&second_patch);
        let second = store
            .put_object_version_patch(
                "object-large-patch",
                "v2",
                "v3",
                second_offset as u64,
                &second_patch,
            )
            .expect("put second patch");
        assert_eq!(second.content_hash, content_hash(&expected));
        assert!(
            !store
                .has_blob(&second.content_hash)
                .expect("second logical blob"),
            "large patch should not materialize the patched logical blob"
        );
        assert!(
            store.hash_tree_path("object-large-patch", "v1").is_file(),
            "base hash tree should be cached for subsequent patches"
        );
        for manifest in [&base_manifest, &first, &second] {
            let tree = store
                .read_hash_tree_optional(&manifest.object_id, &manifest.version)
                .expect("read hash tree")
                .expect("hash tree exists");
            assert_eq!(tree.content_hash, manifest.content_hash);
            assert_eq!(tree.size_bytes, manifest.size_bytes);
        }

        cleanup(&root);
    }

    #[test]
    fn put_object_version_rejects_same_version_with_different_bytes() {
        let root = temp_root("put_object_version_rejects_same_version_with_different_bytes");
        let store = LocalContentStore::new(&root).expect("create store");

        let first = store
            .put_object_version("object-a", "v1", b"hello dfs")
            .expect("first version");
        let duplicate = store
            .put_object_version("object-a", "v1", b"hello dfs")
            .expect("duplicate version");
        assert_eq!(duplicate, first);

        let error = store
            .put_object_version("object-a", "v1", b"different bytes")
            .expect_err("different content for same version");
        assert!(matches!(
            error,
            ContentStoreError::VersionHashConflict {
                object_id,
                version,
                ..
            } if object_id == "object-a" && version == "v1"
        ));
        let rejected_hash = content_hash(b"different bytes");
        assert!(
            !store.has_blob(&rejected_hash).expect("rejected blob check"),
            "version conflicts must not leave unreferenced content-addressed blobs"
        );

        cleanup(&root);
    }

    #[test]
    fn create_new_atomic_does_not_overwrite_existing_file() {
        let root = temp_root("create_new_atomic_does_not_overwrite_existing_file");
        let path = root.join("published.sobj");
        write_file_create_new_atomic(&path, b"winner").expect("publish winner");

        let error = write_file_create_new_atomic(&path, b"loser")
            .expect_err("loser must not overwrite published file");
        assert!(matches!(
            error,
            ContentStoreError::Io { source, .. }
                if source.kind() == io::ErrorKind::AlreadyExists
        ));
        assert_eq!(read_file(&path).expect("published file"), b"winner");

        cleanup(&root);
    }

    #[test]
    fn read_blob_rejects_content_address_mismatch() {
        let root = temp_root("read_blob_rejects_content_address_mismatch");
        let store = LocalContentStore::new(&root).expect("create store");
        let manifest = store
            .put_object_version("object-a", "v1", b"correct bytes")
            .expect("put object version");
        let blob_path = store.blob_path(&manifest.content_hash).expect("blob path");
        write_file_atomic(&blob_path, b"corrupt bytes").expect("corrupt blob");

        let error = store
            .read_blob(&manifest.content_hash)
            .expect_err("corrupt blob must not be returned");
        assert!(matches!(
            error,
            ContentStoreError::BlobHashMismatch { expected, .. }
                if expected == manifest.content_hash
        ));
        assert!(
            !store.has_blob(&manifest.content_hash).expect("has blob"),
            "corrupt content-addressed blobs must not count as locally available"
        );

        cleanup(&root);
    }

    #[test]
    fn put_object_version_repairs_corrupt_content_addressed_blob() {
        let root = temp_root("put_object_version_repairs_corrupt_blob");
        let store = LocalContentStore::new(&root).expect("create store");
        let manifest = store
            .put_object_version("object-a", "v1", b"correct bytes")
            .expect("put object version");
        let blob_path = store.blob_path(&manifest.content_hash).expect("blob path");
        write_file_atomic(&blob_path, b"corrupt bytes").expect("corrupt blob");

        let repaired = store
            .put_object_version("object-a", "v1", b"correct bytes")
            .expect("repair same version blob");

        assert_eq!(repaired, manifest);
        assert_eq!(
            store
                .read_blob(&manifest.content_hash)
                .expect("read repaired"),
            b"correct bytes"
        );

        cleanup(&root);
    }

    #[test]
    fn stage_and_commit_moves_staged_bytes_into_object_version() {
        let root = temp_root("stage_and_commit_moves_staged_bytes_into_object_version");
        let store = LocalContentStore::new(&root).expect("create store");

        let staged_hash = store
            .stage_bytes("intent-1", b"pending content")
            .expect("stage bytes");
        let staging_path = root.join("staging").join("intent-1");
        assert!(staging_path.is_file());
        let staged_stats = store.stats().expect("staged stats");
        assert_eq!(staged_stats.staging_file_count, 1);
        assert_eq!(staged_stats.staging_bytes, b"pending content".len() as u64);

        let manifest = store
            .commit_staged("intent-1", "object-b", "v2")
            .expect("commit staged");

        assert_eq!(manifest.content_hash, staged_hash);
        assert!(!staging_path.exists());
        assert_eq!(
            store.read_blob(&manifest.content_hash).expect("read blob"),
            b"pending content"
        );
        let committed_stats = store.stats().expect("committed stats");
        assert_eq!(committed_stats.staging_file_count, 0);
        assert_eq!(committed_stats.staging_bytes, 0);
        assert_eq!(committed_stats.blob_count, 1);
        assert_eq!(committed_stats.manifest_count, 1);

        cleanup(&root);
    }

    #[test]
    fn rejects_invalid_hashes() {
        let root = temp_root("rejects_invalid_hashes");
        let store = LocalContentStore::new(&root).expect("create store");

        assert!(matches!(
            store.has_blob("sha256:abc"),
            Err(ContentStoreError::InvalidHash(_))
        ));
        assert!(matches!(
            store.read_blob(
                "blake3:AF1349B9F5F9A1A6A0404DEA36D2518CFBBAF2E24C9EAEF1F5D3B3F9B8A3E9F0"
            ),
            Err(ContentStoreError::InvalidHash(_))
        ));

        cleanup(&root);
    }

    #[test]
    fn concurrent_identical_object_version_writes_are_idempotent() {
        let root = temp_root("concurrent-identical-writes");
        let store = std::sync::Arc::new(LocalContentStore::new(&root).expect("store"));
        let bytes = b"the same content written by every racer".to_vec();
        let expected_hash = content_hash(&bytes);

        // Several threads write the identical (object, version, bytes) at once. Only
        // one wins the create-new manifest link; the rest hit AlreadyExists, re-read
        // the manifest, and must return the same content-addressed result (not error).
        let threads = 8usize;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(threads));
        let mut handles = Vec::new();
        for _ in 0..threads {
            let store = store.clone();
            let bytes = bytes.clone();
            let barrier = barrier.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                store
                    .put_object_version("obj_shared", "version_1", &bytes)
                    .expect("concurrent identical write must succeed")
            }));
        }
        for handle in handles {
            let manifest = handle.join().expect("writer thread");
            assert_eq!(
                manifest.content_hash, expected_hash,
                "every racer must observe the same content-addressed manifest"
            );
        }

        assert_eq!(
            store.read_blob(&expected_hash).expect("read blob"),
            bytes,
            "the stored blob must match the written content"
        );
        cleanup(&root);
    }

    fn temp_root(name: &str) -> sinomo_testdir::TestDir {
        sinomo_testdir::TestDir::new(&format!("sinomo-dfs-{name}"))
    }

    fn cleanup(root: &Path) {
        if root.exists() {
            fs::remove_dir_all(root).expect("cleanup temp dir");
        }
    }
}
