//! Durable local content storage for DFS blobs.

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sinomo_object::{LengthMode, serde_ext as sinomo_object_serde};
use sinomo_storage::durable_file;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, LazyLock, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Duration, Instant, SystemTime};
use thiserror::Error;

const HASH_PREFIX: &str = "blake3:";
const BLAKE3_HEX_LEN: usize = 64;
const CONTENT_STORE_STATS_VERSION: u32 = 1;
const CONTENT_STORE_STATS_FILE: &str = "stats.sobj";
const CONTENT_STORE_STATS_LOCK_FILE: &str = "stats.lock";
const PACKED_BLOB_INDEX_VERSION: u32 = 1;
const PACKED_BLOB_DIR: &str = "blob-packs";
const PACKED_BLOB_SEGMENT_DIR: &str = "segments";
const PACKED_BLOB_INDEX_FILE: &str = "index.sobj";
const PACKED_BLOB_INDEX_LOG_FILE: &str = "index.slog";
const PACKED_BLOB_LOCK_FILE: &str = "pack.lock";
const PACKED_BLOB_SMALL_LIMIT_BYTES: u64 = 16 * 1024;
const PACKED_BLOB_SEGMENT_MAX_BYTES: u64 = 8 * 1024 * 1024;
const STREAMED_OBJECT_WRITE_BUFFER_BYTES: usize = 1024 * 1024;

fn content_store_perf_trace_enabled() -> bool {
    static ENABLED: LazyLock<bool> = LazyLock::new(|| {
        std::env::var("SINOMO_DFS_PERF_TRACE")
            .ok()
            .is_some_and(|value| {
                value == "1"
                    || value.eq_ignore_ascii_case("true")
                    || value.eq_ignore_ascii_case("yes")
            })
    });
    *ENABLED
}

fn content_store_perf_trace(args: std::fmt::Arguments<'_>) {
    if content_store_perf_trace_enabled() {
        eprintln!("[sinomo-dfs-perf] {args}");
    }
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
    #[error(
        "chunked content hash mismatch: object={object_id}, version={version}, expected={expected_hash}, actual={actual_hash}"
    )]
    ChunkHashMismatch {
        object_id: String,
        version: String,
        expected_hash: String,
        actual_hash: String,
    },
    #[error("chunked content transfer incomplete: object={object_id}, version={version}")]
    IncompleteChunkedContent { object_id: String, version: String },
    #[error(
        "conflicting duplicate content chunk: object={object_id}, version={version}, chunk={chunk_index}"
    )]
    ConflictingChunk {
        object_id: String,
        version: String,
        chunk_index: u32,
    },
    #[error("invalid chunked content transfer: {message}")]
    InvalidChunkedContent { message: String },
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentPatchManifest {
    pub manifest: ContentManifest,
    pub patch_hash: Option<String>,
    pub patch_size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentExtent {
    pub logical_offset: u64,
    pub size_bytes: u64,
    pub content_hash: String,
    pub blob_offset: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentStoreStats {
    pub blob_count: u64,
    pub blob_bytes: u64,
    pub manifest_count: u64,
    pub manifest_bytes: u64,
    pub logical_object_bytes: u64,
    pub extent_manifest_count: u64,
    pub extent_count: u64,
    pub staging_file_count: u64,
    pub staging_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ContentStoreStatsSnapshot {
    version: u32,
    stats: ContentStoreStats,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct PackedBlobIndex {
    version: u32,
    next_segment_id: u64,
    #[serde(default)]
    entries: BTreeMap<String, PackedBlobLocation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PackedBlobLocation {
    segment_id: String,
    offset: u64,
    size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PackedBlobIndexLogEntry {
    version: u32,
    content_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    location: Option<PackedBlobLocation>,
    next_segment_id: u64,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ContentStoreQuotaResource {
    BlobBytes,
    LogicalObjectBytes,
    ManifestBytes,
    StagingBytes,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentStoreQuota {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_blob_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_logical_object_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_manifest_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_staging_bytes: Option<u64>,
}

impl ContentStoreQuota {
    pub fn no_limits() -> Self {
        Self::default()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentStoreQuotaUsage {
    pub stats: ContentStoreStats,
}

#[derive(Debug, Clone)]
pub struct LocalContentStore {
    root: PathBuf,
    quota: Option<ContentStoreQuota>,
    stats_snapshot_active: Arc<AtomicBool>,
    deferred_durable_blobs: Arc<Mutex<BTreeMap<PathBuf, PathBuf>>>,
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
struct VersionManifestEntry {
    version: String,
    path: PathBuf,
    version_dir: PathBuf,
    object_dir: PathBuf,
    modified_nanos: u64,
    manifest_bytes: u64,
    logical_bytes: u64,
    extent_count: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct ContentStoreQuotaDelta {
    blob_bytes: u64,
    logical_object_bytes: u64,
    manifest_bytes: u64,
    staging_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct ContentStoreStatsDelta {
    blob_count: i128,
    blob_bytes: i128,
    manifest_count: i128,
    manifest_bytes: i128,
    logical_object_bytes: i128,
    extent_manifest_count: i128,
    extent_count: i128,
    staging_file_count: i128,
    staging_bytes: i128,
}

impl ContentStoreStatsDelta {
    fn blob_file_change(before_len: Option<u64>, after_len: Option<u64>) -> Self {
        Self {
            blob_count: file_count_delta(before_len, after_len),
            blob_bytes: file_bytes_delta(before_len, after_len),
            ..Self::default()
        }
    }

    fn staging_file_change(before_len: Option<u64>, after_len: Option<u64>) -> Self {
        Self {
            staging_file_count: file_count_delta(before_len, after_len),
            staging_bytes: file_bytes_delta(before_len, after_len),
            ..Self::default()
        }
    }

    fn manifest_created(manifest: &ContentManifest, manifest_bytes: u64) -> Self {
        let extent_manifest_count = if manifest.extents.is_empty() { 0 } else { 1 };
        Self {
            manifest_count: 1,
            manifest_bytes: i128::from(manifest_bytes),
            logical_object_bytes: i128::from(manifest.size_bytes),
            extent_manifest_count,
            extent_count: manifest.extents.len() as i128,
            ..Self::default()
        }
    }

    fn manifest_deleted(entry: &VersionManifestEntry) -> Self {
        Self {
            manifest_count: -1,
            manifest_bytes: -i128::from(entry.manifest_bytes),
            logical_object_bytes: -i128::from(entry.logical_bytes),
            extent_manifest_count: if entry.extent_count == 0 { 0 } else { -1 },
            extent_count: -(entry.extent_count as i128),
            ..Self::default()
        }
    }

    fn apply_to(self, stats: &mut ContentStoreStats) {
        stats.blob_count = apply_signed_delta(stats.blob_count, self.blob_count);
        stats.blob_bytes = apply_signed_delta(stats.blob_bytes, self.blob_bytes);
        stats.manifest_count = apply_signed_delta(stats.manifest_count, self.manifest_count);
        stats.manifest_bytes = apply_signed_delta(stats.manifest_bytes, self.manifest_bytes);
        stats.logical_object_bytes =
            apply_signed_delta(stats.logical_object_bytes, self.logical_object_bytes);
        stats.extent_manifest_count =
            apply_signed_delta(stats.extent_manifest_count, self.extent_manifest_count);
        stats.extent_count = apply_signed_delta(stats.extent_count, self.extent_count);
        stats.staging_file_count =
            apply_signed_delta(stats.staging_file_count, self.staging_file_count);
        stats.staging_bytes = apply_signed_delta(stats.staging_bytes, self.staging_bytes);
    }

    fn is_zero(self) -> bool {
        self.blob_count == 0
            && self.blob_bytes == 0
            && self.manifest_count == 0
            && self.manifest_bytes == 0
            && self.logical_object_bytes == 0
            && self.extent_manifest_count == 0
            && self.extent_count == 0
            && self.staging_file_count == 0
            && self.staging_bytes == 0
    }

    fn add(&mut self, other: Self) {
        self.blob_count += other.blob_count;
        self.blob_bytes += other.blob_bytes;
        self.manifest_count += other.manifest_count;
        self.manifest_bytes += other.manifest_bytes;
        self.logical_object_bytes += other.logical_object_bytes;
        self.extent_manifest_count += other.extent_manifest_count;
        self.extent_count += other.extent_count;
        self.staging_file_count += other.staging_file_count;
        self.staging_bytes += other.staging_bytes;
    }
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

#[derive(Debug)]
pub struct BlobReader {
    file: fs::File,
    size_bytes: u64,
    remaining_bytes: u64,
}

impl BlobReader {
    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }
}

impl Read for BlobReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.remaining_bytes == 0 || buffer.is_empty() {
            return Ok(0);
        }
        let limit = buffer
            .len()
            .min(usize::try_from(self.remaining_bytes).unwrap_or(usize::MAX));
        let read = self.file.read(&mut buffer[..limit])?;
        self.remaining_bytes = self.remaining_bytes.saturating_sub(read as u64);
        Ok(read)
    }
}

#[derive(Debug)]
pub struct BlobRangeReader {
    file: fs::File,
    offset: u64,
    size_bytes: u64,
    remaining_bytes: u64,
}

#[derive(Debug)]
pub struct ObjectVersionReader {
    inner: ObjectVersionReaderInner,
    size_bytes: u64,
}

#[derive(Debug)]
enum ObjectVersionReaderInner {
    Blob(BlobReader),
    Range(BlobRangeReader),
    Extents(ExtentReader),
}

impl ObjectVersionReader {
    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    pub fn from_blob(reader: BlobReader) -> Self {
        Self {
            size_bytes: reader.size_bytes(),
            inner: ObjectVersionReaderInner::Blob(reader),
        }
    }

    pub fn from_blob_range(reader: BlobRangeReader) -> Self {
        let size_bytes = reader.remaining_bytes();
        Self {
            size_bytes,
            inner: ObjectVersionReaderInner::Range(reader),
        }
    }
}

impl Read for ObjectVersionReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        match &mut self.inner {
            ObjectVersionReaderInner::Blob(reader) => reader.read(buffer),
            ObjectVersionReaderInner::Range(reader) => reader.read(buffer),
            ObjectVersionReaderInner::Extents(reader) => reader.read(buffer),
        }
    }
}

#[derive(Debug)]
struct ExtentReader {
    store: LocalContentStore,
    extents: Vec<ContentExtent>,
    next_extent_index: usize,
    current: Option<BlobRangeReader>,
}

impl Read for ExtentReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        let mut total_read = 0;
        while total_read < buffer.len() {
            if self.current.is_none() {
                let Some(extent) = self.extents.get(self.next_extent_index).cloned() else {
                    break;
                };
                self.next_extent_index += 1;
                let max_bytes = usize::try_from(extent.size_bytes).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("extent is too large to read: {} bytes", extent.size_bytes),
                    )
                })?;
                self.current = Some(
                    self.store
                        .open_blob_range_stream(&extent.content_hash, extent.blob_offset, max_bytes)
                        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?,
                );
            }
            let Some(reader) = self.current.as_mut() else {
                break;
            };
            let read = reader.read(&mut buffer[total_read..])?;
            if read == 0 {
                self.current = None;
                continue;
            }
            total_read += read;
            if reader.remaining_bytes() == 0 {
                self.current = None;
            }
        }
        Ok(total_read)
    }
}

impl BlobRangeReader {
    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    pub fn remaining_bytes(&self) -> u64 {
        self.remaining_bytes
    }
}

impl Read for BlobRangeReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.remaining_bytes == 0 || buffer.is_empty() {
            return Ok(0);
        }
        let limit = buffer
            .len()
            .min(usize::try_from(self.remaining_bytes).unwrap_or(usize::MAX));
        let read = self.file.read(&mut buffer[..limit])?;
        self.remaining_bytes = self.remaining_bytes.saturating_sub(read as u64);
        Ok(read)
    }
}

#[derive(Debug)]
pub struct ChunkedBlobAssembler {
    object_id: String,
    version: String,
    expected_hash: String,
    total_size_bytes: u64,
    total_chunks: u32,
    chunk_size_bytes: usize,
    received_chunks: Vec<bool>,
    received_count: u32,
    path: PathBuf,
    file: fs::File,
    store: LocalContentStore,
    cleanup_on_drop: bool,
}

impl ChunkedBlobAssembler {
    pub fn insert_chunk(&mut self, chunk_index: u32, bytes: &[u8]) -> Result<bool> {
        self.validate_chunk(chunk_index, bytes)?;
        let offset = u64::from(chunk_index) * self.chunk_size_bytes as u64;
        if self.received_chunks[chunk_index as usize] {
            let mut existing = vec![0_u8; bytes.len()];
            self.file
                .seek(SeekFrom::Start(offset))
                .and_then(|_| self.file.read_exact(&mut existing))
                .map_err(|source| ContentStoreError::Io {
                    path: self.path.clone(),
                    source,
                })?;
            if existing != bytes {
                return Err(ContentStoreError::ConflictingChunk {
                    object_id: self.object_id.clone(),
                    version: self.version.clone(),
                    chunk_index,
                });
            }
            return Ok(self.is_complete());
        }

        self.file
            .seek(SeekFrom::Start(offset))
            .and_then(|_| self.file.write_all(bytes))
            .map_err(|source| ContentStoreError::Io {
                path: self.path.clone(),
                source,
            })?;
        self.received_chunks[chunk_index as usize] = true;
        self.received_count += 1;
        Ok(self.is_complete())
    }

    pub fn is_complete(&self) -> bool {
        self.received_count == self.total_chunks
    }

    pub fn contiguous_received_chunk_count(&self) -> u32 {
        self.received_chunks
            .iter()
            .take_while(|received| **received)
            .count()
            .try_into()
            .expect("chunk count fits in u32")
    }

    pub fn total_size_bytes(&self) -> u64 {
        self.total_size_bytes
    }

    fn validate_chunk(&self, chunk_index: u32, bytes: &[u8]) -> Result<()> {
        if chunk_index >= self.total_chunks {
            return Err(ContentStoreError::InvalidChunkedContent {
                message: format!(
                    "chunk index out of range for object {}, version {}: {chunk_index}/{}",
                    self.object_id, self.version, self.total_chunks
                ),
            });
        }
        let expected_len = self.expected_chunk_len(chunk_index)?;
        if bytes.len() as u64 != expected_len {
            return Err(ContentStoreError::InvalidChunkedContent {
                message: format!(
                    "chunk length mismatch for object {}, version {}, chunk {chunk_index}: expected {expected_len}, actual {}",
                    self.object_id,
                    self.version,
                    bytes.len()
                ),
            });
        }
        Ok(())
    }

    fn expected_chunk_len(&self, chunk_index: u32) -> Result<u64> {
        let chunk_size = self.chunk_size_bytes as u64;
        let offset = u64::from(chunk_index)
            .checked_mul(chunk_size)
            .ok_or_else(|| ContentStoreError::InvalidChunkedContent {
                message: format!(
                    "chunk offset overflow for object {}, version {}",
                    self.object_id, self.version
                ),
            })?;
        if offset >= self.total_size_bytes {
            return Err(ContentStoreError::InvalidChunkedContent {
                message: format!(
                    "chunk offset exceeds transfer size for object {}, version {}",
                    self.object_id, self.version
                ),
            });
        }
        Ok((self.total_size_bytes - offset).min(chunk_size))
    }
}

impl Drop for ChunkedBlobAssembler {
    fn drop(&mut self) {
        if self.cleanup_on_drop {
            let _ = self.store.remove_tracked_staging_file_if_exists(&self.path);
        }
    }
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
        let stats_snapshot_active = Arc::new(AtomicBool::new(
            root.join(CONTENT_STORE_STATS_FILE).is_file(),
        ));
        Ok(Self {
            root,
            quota: None,
            stats_snapshot_active,
            deferred_durable_blobs: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    pub fn with_quota(mut self, quota: ContentStoreQuota) -> Self {
        self.quota = Some(quota);
        self
    }

    pub fn without_quota(mut self) -> Self {
        self.quota = None;
        self
    }

    pub fn quota(&self) -> Option<&ContentStoreQuota> {
        self.quota.as_ref()
    }

    pub fn quota_usage(&self) -> Result<ContentStoreQuotaUsage> {
        Ok(ContentStoreQuotaUsage {
            stats: self.stats()?,
        })
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
            let blob_delta = self.write_packed_blobs_unlocked_relaxed(&mut index, &blob_writes)?;
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

    pub fn flush_deferred_durable_dirs(&self) -> Result<()> {
        let blobs = {
            let mut blobs =
                self.deferred_durable_blobs
                    .lock()
                    .map_err(|_| ContentStoreError::Io {
                        path: self.root.clone(),
                        source: io::Error::other("deferred durable blob lock is poisoned"),
                    })?;
            std::mem::take(&mut *blobs)
        };
        let mut dirs = BTreeSet::new();
        for (blob_path, dir) in blobs {
            let file = fs::File::open(&blob_path).map_err(|source| ContentStoreError::Io {
                path: blob_path.clone(),
                source,
            })?;
            file.sync_data().map_err(|source| ContentStoreError::Io {
                path: blob_path,
                source,
            })?;
            dirs.insert(dir);
        }
        for dir in dirs {
            sync_dir(&dir)?;
        }
        Ok(())
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
        reader: R,
        capture_limit_bytes: usize,
    ) -> Result<(ContentManifest, Option<Vec<u8>>)> {
        let internal_capture_limit =
            capture_limit_bytes.max(PACKED_BLOB_SMALL_LIMIT_BYTES as usize);
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

    pub fn begin_chunked_object_version(
        &self,
        object_id: &str,
        version: &str,
        expected_hash: &str,
        total_size_bytes: u64,
        total_chunks: u32,
        chunk_size_bytes: usize,
    ) -> Result<ChunkedBlobAssembler> {
        validate_chunked_object_shape(total_size_bytes, total_chunks, chunk_size_bytes)?;
        hash_hex(expected_hash)?;
        let received_len = usize::try_from(total_chunks).map_err(|_| {
            ContentStoreError::InvalidChunkedContent {
                message: format!("chunk count is too large for this platform: {total_chunks}"),
            }
        })?;
        let staging_anchor = self.root.join("staging").join("chunked-object-version");
        let path = temporary_path_for(&staging_anchor);
        self.ensure_quota_delta(ContentStoreQuotaDelta {
            staging_bytes: total_size_bytes,
            ..ContentStoreQuotaDelta::default()
        })?;
        if let Some(parent) = path.parent() {
            create_dir_all(parent.to_owned())?;
        }
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|source| ContentStoreError::Io {
                path: path.clone(),
                source,
            })?;
        file.set_len(total_size_bytes)
            .map_err(|source| ContentStoreError::Io {
                path: path.clone(),
                source,
            })?;
        if let Err(error) = self.track_staging_file_created(&path) {
            remove_file_if_exists(&path).ok();
            return Err(error);
        }
        Ok(ChunkedBlobAssembler {
            object_id: object_id.to_owned(),
            version: version.to_owned(),
            expected_hash: expected_hash.to_owned(),
            total_size_bytes,
            total_chunks,
            chunk_size_bytes,
            received_chunks: vec![false; received_len],
            received_count: 0,
            path,
            file,
            store: self.clone(),
            cleanup_on_drop: true,
        })
    }

    pub fn commit_chunked_object_version(
        &self,
        mut assembler: ChunkedBlobAssembler,
    ) -> Result<ContentManifest> {
        if !assembler.is_complete() {
            return Err(ContentStoreError::IncompleteChunkedContent {
                object_id: assembler.object_id.clone(),
                version: assembler.version.clone(),
            });
        }
        assembler
            .file
            .sync_all()
            .map_err(|source| ContentStoreError::Io {
                path: assembler.path.clone(),
                source,
            })?;
        let (actual_hash, actual_size_bytes) = file_content_hash_and_size(&assembler.path)?;
        if actual_size_bytes != assembler.total_size_bytes {
            return Err(ContentStoreError::InvalidChunkedContent {
                message: format!(
                    "assembled chunked object has wrong size for object {}, version {}: expected {}, actual {actual_size_bytes}",
                    assembler.object_id, assembler.version, assembler.total_size_bytes
                ),
            });
        }
        if actual_hash != assembler.expected_hash {
            return Err(ContentStoreError::ChunkHashMismatch {
                object_id: assembler.object_id.clone(),
                version: assembler.version.clone(),
                expected_hash: assembler.expected_hash.clone(),
                actual_hash,
            });
        }
        let manifest = self.write_object_version_from_verified_file(
            &assembler.object_id,
            &assembler.version,
            &assembler.path,
            actual_hash,
            actual_size_bytes,
        )?;
        self.remove_tracked_staging_file_if_exists(&assembler.path)?;
        assembler.cleanup_on_drop = false;
        Ok(manifest)
    }

    pub fn commit_chunked_object_version_patch(
        &self,
        mut assembler: ChunkedBlobAssembler,
        base_version: &str,
        offset: u64,
    ) -> Result<ContentManifest> {
        if !assembler.is_complete() {
            return Err(ContentStoreError::IncompleteChunkedContent {
                object_id: assembler.object_id.clone(),
                version: assembler.version.clone(),
            });
        }
        assembler
            .file
            .sync_all()
            .map_err(|source| ContentStoreError::Io {
                path: assembler.path.clone(),
                source,
            })?;
        let (actual_hash, actual_size_bytes) = file_content_hash_and_size(&assembler.path)?;
        if actual_size_bytes != assembler.total_size_bytes {
            return Err(ContentStoreError::InvalidChunkedContent {
                message: format!(
                    "assembled chunked patch has wrong size for object {}, version {}: expected {}, actual {actual_size_bytes}",
                    assembler.object_id, assembler.version, assembler.total_size_bytes
                ),
            });
        }
        if actual_hash != assembler.expected_hash {
            return Err(ContentStoreError::ChunkHashMismatch {
                object_id: assembler.object_id.clone(),
                version: assembler.version.clone(),
                expected_hash: assembler.expected_hash.clone(),
                actual_hash,
            });
        }
        let manifest = self
            .put_object_version_patch_from_file(
                &assembler.object_id,
                base_version,
                &assembler.version,
                offset,
                &assembler.path,
            )?
            .manifest;
        self.remove_tracked_staging_file_if_exists(&assembler.path)?;
        assembler.cleanup_on_drop = false;
        Ok(manifest)
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
            return Ok(BlobReader {
                file,
                size_bytes,
                remaining_bytes: size_bytes,
            });
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
            return Ok(BlobReader {
                file,
                size_bytes,
                remaining_bytes: size_bytes,
            });
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
        Ok(BlobRangeReader {
            file,
            offset: start,
            size_bytes,
            remaining_bytes,
        })
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

    pub fn read_object_version(
        &self,
        object_id: &str,
        version: &str,
    ) -> Result<(ContentManifest, Vec<u8>)> {
        let (manifest, mut reader) = self.open_object_version_stream(object_id, version)?;
        let capacity = usize::try_from(reader.size_bytes()).map_err(|_| {
            ContentStoreError::InvalidChunkedContent {
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
                return Err(ContentStoreError::InvalidChunkedContent {
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
        let reader = ObjectVersionReader {
            size_bytes: manifest.size_bytes,
            inner: ObjectVersionReaderInner::Extents(ExtentReader {
                store: self.clone(),
                extents,
                next_extent_index: 0,
                current: None,
            }),
        };
        Ok((manifest, reader))
    }

    pub fn open_object_version_range_stream(
        &self,
        object_id: &str,
        version: &str,
        offset: u64,
        max_bytes: usize,
    ) -> Result<(ContentManifest, ObjectVersionReader)> {
        let manifest = self.read_manifest(object_id, version)?;
        let start = offset.min(manifest.size_bytes);
        let max_bytes_u64 = u64::try_from(max_bytes).unwrap_or(u64::MAX);
        let end = start.saturating_add(max_bytes_u64).min(manifest.size_bytes);
        if manifest.extents.is_empty() {
            let reader = self.open_blob_range_stream(&manifest.content_hash, start, max_bytes)?;
            return Ok((manifest, ObjectVersionReader::from_blob_range(reader)));
        }

        let mut range_extents = Vec::new();
        for extent in self.logical_extents_for_manifest(&manifest)? {
            let extent_end = extent
                .logical_offset
                .checked_add(extent.size_bytes)
                .ok_or_else(|| ContentStoreError::InvalidChunkedContent {
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
            range_extents.push(ContentExtent {
                logical_offset: range_start - start,
                size_bytes: range_end - range_start,
                content_hash: extent.content_hash,
                blob_offset: extent
                    .blob_offset
                    .checked_add(extent_delta)
                    .ok_or_else(|| ContentStoreError::InvalidChunkedContent {
                        message: format!(
                            "extent blob offset overflows: object={}, version={}",
                            manifest.object_id, manifest.version
                        ),
                    })?,
            });
        }
        self.ensure_extents_readable(&range_extents)?;
        let reader = ObjectVersionReader {
            size_bytes: end.saturating_sub(start),
            inner: ObjectVersionReaderInner::Extents(ExtentReader {
                store: self.clone(),
                extents: range_extents,
                next_extent_index: 0,
                current: None,
            }),
        };
        Ok((manifest, reader))
    }

    pub fn open_object_version_blob_stream(
        &self,
        object_id: &str,
        version: &str,
    ) -> Result<(ContentManifest, BlobReader)> {
        let manifest = self.read_manifest(object_id, version)?;
        let reader = self.open_blob_stream(&manifest.content_hash)?;
        if reader.size_bytes() != manifest.size_bytes {
            return Err(ContentStoreError::InvalidChunkedContent {
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
        self.extents_available(&extents)
    }

    pub fn read_manifest(&self, object_id: &str, version: &str) -> Result<ContentManifest> {
        let path = self.manifest_path(object_id, version);
        let bytes = read_file(&path)?;
        decode_manifest(&bytes)
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

    pub fn stats(&self) -> Result<ContentStoreStats> {
        let _lock = self.lock_stats_snapshot()?;
        if let Some(stats) = self.read_stats_snapshot_unlocked()? {
            return Ok(stats);
        }
        let stats = self.scan_stats()?;
        self.write_stats_snapshot_unlocked(&stats)?;
        Ok(stats)
    }

    fn scan_stats(&self) -> Result<ContentStoreStats> {
        let (blob_count, blob_bytes) = self.scan_blob_stats()?;
        let (staging_file_count, staging_bytes) =
            recursive_file_count_and_bytes(&self.root.join("staging"))?;
        let mut stats = ContentStoreStats {
            blob_count,
            blob_bytes,
            staging_file_count,
            staging_bytes,
            ..ContentStoreStats::default()
        };
        self.collect_manifest_stats(&self.root.join("objects"), &mut stats)?;
        Ok(stats)
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
                if remove_file_if_exists_and_sync_parent(&entry.path)? {
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
        self.validate_object_version_patch_source(
            object_id,
            base_version,
            offset,
            bytes.len() as u64,
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
    ) -> Result<ContentPatchManifest> {
        let (patch_hash, patch_size) = file_content_hash_and_size(source_path)?;
        self.validate_object_version_patch_source(object_id, base_version, offset, patch_size)?;
        if patch_size == 0 {
            return self.put_object_version_patch_from_blob(
                object_id,
                base_version,
                target_version,
                offset,
                None,
                0,
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
        self.put_object_version_patch_from_blob(
            object_id,
            base_version,
            target_version,
            offset,
            Some(patch_hash),
            patch_size,
        )
    }

    fn validate_object_version_patch_source(
        &self,
        object_id: &str,
        base_version: &str,
        offset: u64,
        patch_size: u64,
    ) -> Result<()> {
        let base_manifest = self.read_manifest(object_id, base_version)?;
        let base_extents = self.logical_extents_for_manifest(&base_manifest)?;
        self.ensure_extents_readable(&base_extents)?;
        if offset > base_manifest.size_bytes {
            return Err(ContentStoreError::InvalidChunkedContent {
                message: format!(
                    "patch offset leaves sparse gap: object={object_id}, base={base_version}, offset={offset}, size={}",
                    base_manifest.size_bytes
                ),
            });
        }
        offset
            .checked_add(patch_size)
            .ok_or_else(|| ContentStoreError::InvalidChunkedContent {
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
    ) -> Result<ContentPatchManifest> {
        let base_manifest = self.read_manifest(object_id, base_version)?;
        let base_extents = self.logical_extents_for_manifest(&base_manifest)?;
        self.ensure_extents_readable(&base_extents)?;
        if offset > base_manifest.size_bytes {
            return Err(ContentStoreError::InvalidChunkedContent {
                message: format!(
                    "patch offset leaves sparse gap: object={object_id}, base={base_version}, offset={offset}, size={}",
                    base_manifest.size_bytes
                ),
            });
        }

        let patch_end =
            offset
                .checked_add(patch_size)
                .ok_or_else(|| ContentStoreError::InvalidChunkedContent {
                    message: format!(
                        "patch range overflows object size: object={object_id}, offset={offset}, bytes={patch_size}"
                    ),
                })?;
        let target_size = base_manifest.size_bytes.max(patch_end);
        let mut target_extents = Vec::new();

        for extent in base_extents {
            let extent_end = extent.logical_offset.checked_add(extent.size_bytes).ok_or_else(
                || ContentStoreError::InvalidChunkedContent {
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
        let content_hash = self.hash_extents(&target_extents)?;
        let manifest = ContentManifest {
            object_id: object_id.to_owned(),
            version: target_version.to_owned(),
            content_hash,
            size_bytes: target_size,
            extents: target_extents,
        };
        let manifest = self.write_manifest_create_new_or_existing(manifest)?;
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
                return Ok(existing);
            }
            return Err(error);
        }
        self.apply_stats_delta(ContentStoreStatsDelta::manifest_created(
            &manifest,
            manifest_bytes.len() as u64,
        ))?;
        Ok(manifest)
    }

    fn write_object_version_blob_only(
        &self,
        object_id: &str,
        version: &str,
        bytes: &[u8],
    ) -> Result<ContentManifest> {
        let trace_started = Instant::now();
        let hash_started = Instant::now();
        let hash = content_hash(bytes);
        let hash_ms = hash_started.elapsed().as_millis();
        let manifest_check_started = Instant::now();
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
            content_store_perf_trace(format_args!(
                "blob-only-write object={} version={} bytes={} existing_manifest=true hash_ms={} manifest_check_ms={} total_ms={}",
                object_id,
                version,
                bytes.len(),
                hash_ms,
                manifest_check_started.elapsed().as_millis(),
                trace_started.elapsed().as_millis()
            ));
            return Ok(existing);
        }
        let manifest_check_ms = manifest_check_started.elapsed().as_millis();

        let blob_path = self.blob_path(&hash)?;
        let manifest = ContentManifest {
            object_id: object_id.to_owned(),
            version: version.to_owned(),
            content_hash: hash,
            size_bytes: bytes.len() as u64,
            extents: Vec::new(),
        };

        let defer_blob_dir_sync = !self.tracks_stats_deltas();
        let has_blob_started = Instant::now();
        let blob_exists = if defer_blob_dir_sync {
            false
        } else {
            self.has_blob(&manifest.content_hash)?
        };
        let has_blob_ms = has_blob_started.elapsed().as_millis();
        let mut quota_ms = 0;
        let mut blob_write_ms = 0;
        if defer_blob_dir_sync {
            let blob_write_started = Instant::now();
            self.write_tracked_blob_bytes_prehashed_deferred_dir_sync(
                &manifest.content_hash,
                &blob_path,
                bytes,
            )?;
            blob_write_ms = blob_write_started.elapsed().as_millis();
        } else if !blob_exists {
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
                return Ok(existing);
            }
            return Err(error);
        }
        self.apply_stats_delta(ContentStoreStatsDelta::manifest_created(
            &manifest,
            manifest_bytes.len() as u64,
        ))?;
        Ok(manifest)
    }

    fn collect_manifest_stats(&self, path: &Path, stats: &mut ContentStoreStats) -> Result<()> {
        if !path.exists() {
            return Ok(());
        }
        for entry in read_dir(path)? {
            let entry_path = entry.path();
            if entry_path.is_dir() {
                self.collect_manifest_stats(&entry_path, stats)?;
                continue;
            }
            if entry_path.file_name().and_then(|name| name.to_str()) != Some("manifest.sobj") {
                continue;
            }
            let manifest_bytes = read_file(&entry_path)?;
            let manifest = decode_manifest(&manifest_bytes)?;
            stats.manifest_count = stats.manifest_count.saturating_add(1);
            stats.manifest_bytes = stats
                .manifest_bytes
                .saturating_add(manifest_bytes.len() as u64);
            stats.logical_object_bytes = stats
                .logical_object_bytes
                .saturating_add(manifest.size_bytes);
            if !manifest.extents.is_empty() {
                stats.extent_manifest_count = stats.extent_manifest_count.saturating_add(1);
                stats.extent_count = stats
                    .extent_count
                    .saturating_add(manifest.extents.len() as u64);
            }
        }
        Ok(())
    }

    fn scan_blob_stats(&self) -> Result<(u64, u64)> {
        let (loose_count, loose_bytes) = recursive_file_count_and_bytes(&self.root.join("blobs"))?;
        let index = self.read_packed_blob_index_unlocked()?;
        let packed_count = index.entries.len() as u64;
        let packed_bytes = index
            .entries
            .values()
            .map(|location| location.size_bytes)
            .fold(0_u64, |total, size| total.saturating_add(size));
        Ok((
            loose_count.saturating_add(packed_count),
            loose_bytes.saturating_add(packed_bytes),
        ))
    }

    fn lock_stats_snapshot(&self) -> Result<fs::File> {
        let path = self.root.join(CONTENT_STORE_STATS_LOCK_FILE);
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .map_err(|source| ContentStoreError::Io {
                path: path.clone(),
                source,
            })?;
        file.lock_exclusive()
            .map_err(|source| ContentStoreError::Io { path, source })?;
        Ok(file)
    }

    fn read_stats_snapshot_unlocked(&self) -> Result<Option<ContentStoreStats>> {
        let path = self.stats_snapshot_path();
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(source) => {
                return Err(ContentStoreError::Io {
                    path: path.clone(),
                    source,
                });
            }
        };
        let snapshot = match decode_stats_snapshot(&bytes) {
            Ok(snapshot) if snapshot.version == CONTENT_STORE_STATS_VERSION => snapshot,
            Ok(_) | Err(_) => return Ok(None),
        };
        Ok(Some(snapshot.stats))
    }

    fn write_stats_snapshot_unlocked(&self, stats: &ContentStoreStats) -> Result<()> {
        let snapshot = ContentStoreStatsSnapshot {
            version: CONTENT_STORE_STATS_VERSION,
            stats: stats.clone(),
        };
        let bytes = encode_stats_snapshot(&snapshot)?;
        write_file_atomic(&self.stats_snapshot_path(), &bytes)?;
        self.stats_snapshot_active.store(true, Ordering::Relaxed);
        Ok(())
    }

    fn replace_stats_snapshot(&self, stats: &ContentStoreStats) -> Result<()> {
        let _lock = self.lock_stats_snapshot()?;
        self.write_stats_snapshot_unlocked(stats)
    }

    fn apply_stats_delta(&self, delta: ContentStoreStatsDelta) -> Result<()> {
        if delta.is_zero() {
            return Ok(());
        }
        if !self.tracks_stats_deltas() {
            return Ok(());
        }
        let _lock = self.lock_stats_snapshot()?;
        if let Some(mut stats) = self.read_stats_snapshot_unlocked()? {
            delta.apply_to(&mut stats);
            self.write_stats_snapshot_unlocked(&stats)?;
            return Ok(());
        }
        if self.quota.is_none() {
            return Ok(());
        }
        let stats = self.scan_stats()?;
        self.write_stats_snapshot_unlocked(&stats)
    }

    fn tracks_stats_deltas(&self) -> bool {
        self.quota.is_some() || self.stats_snapshot_active.load(Ordering::Relaxed)
    }

    fn write_tracked_blob_bytes(&self, hash: &str, blob_path: &Path, bytes: &[u8]) -> Result<()> {
        if bytes.len() as u64 <= PACKED_BLOB_SMALL_LIMIT_BYTES {
            if self.write_packed_blob(hash, bytes)? {
                self.apply_stats_delta(ContentStoreStatsDelta {
                    blob_count: 1,
                    blob_bytes: bytes.len() as i128,
                    ..ContentStoreStatsDelta::default()
                })?;
            }
            if blob_path.is_file()
                && !file_content_hash(blob_path).is_ok_and(|actual| actual == hash)
            {
                remove_file_if_exists(blob_path)?;
            }
            return Ok(());
        }
        if !self.tracks_stats_deltas() {
            return write_content_addressed_blob_bytes(hash, blob_path, bytes);
        }
        let before_len = file_len_if_exists(blob_path)?;
        write_content_addressed_blob_bytes(hash, blob_path, bytes)?;
        let after_len = file_len_if_exists(blob_path)?;
        self.apply_stats_delta(ContentStoreStatsDelta::blob_file_change(
            before_len, after_len,
        ))
    }

    fn write_tracked_blob_bytes_prehashed(
        &self,
        hash: &str,
        blob_path: &Path,
        bytes: &[u8],
    ) -> Result<()> {
        if bytes.len() as u64 <= PACKED_BLOB_SMALL_LIMIT_BYTES {
            return self.write_tracked_blob_bytes(hash, blob_path, bytes);
        }
        if !self.tracks_stats_deltas() {
            return write_content_addressed_blob_bytes_prehashed_existing_parent(
                hash, blob_path, bytes,
            );
        }
        let before_len = file_len_if_exists(blob_path)?;
        write_content_addressed_blob_bytes_prehashed_existing_parent(hash, blob_path, bytes)?;
        let after_len = file_len_if_exists(blob_path)?;
        self.apply_stats_delta(ContentStoreStatsDelta::blob_file_change(
            before_len, after_len,
        ))
    }

    fn write_tracked_blob_bytes_prehashed_deferred_dir_sync(
        &self,
        hash: &str,
        blob_path: &Path,
        bytes: &[u8],
    ) -> Result<()> {
        if bytes.len() as u64 <= PACKED_BLOB_SMALL_LIMIT_BYTES {
            return self.write_tracked_blob_bytes_prehashed(hash, blob_path, bytes);
        }
        let deferred =
            write_content_addressed_blob_bytes_prehashed_defer_durability_existing_parent(
                hash, blob_path, bytes,
            )?;
        if let Some((blob_path, sync_dir)) = deferred {
            self.defer_durable_blob_sync(blob_path, sync_dir)?;
        }
        Ok(())
    }

    fn defer_durable_blob_sync(&self, blob_path: PathBuf, dir: PathBuf) -> Result<()> {
        self.deferred_durable_blobs
            .lock()
            .map_err(|_| ContentStoreError::Io {
                path: dir.clone(),
                source: io::Error::other("deferred durable blob lock is poisoned"),
            })?
            .insert(blob_path, dir);
        Ok(())
    }

    fn publish_tracked_file_as_blob(
        &self,
        hash: &str,
        source_path: &Path,
        blob_path: &Path,
        expected_hash: &str,
    ) -> Result<()> {
        let source_len = file_len_if_exists(source_path)?.unwrap_or(0);
        if source_len <= PACKED_BLOB_SMALL_LIMIT_BYTES {
            let bytes = read_file(source_path)?;
            ensure_bytes_match_hash(hash, &bytes)?;
            if hash != expected_hash {
                return Err(ContentStoreError::BlobHashMismatch {
                    expected: expected_hash.to_owned(),
                    actual: hash.to_owned(),
                });
            }
            if self.write_packed_blob(hash, &bytes)? {
                self.apply_stats_delta(ContentStoreStatsDelta {
                    blob_count: 1,
                    blob_bytes: bytes.len() as i128,
                    ..ContentStoreStatsDelta::default()
                })?;
            }
            if blob_path.is_file()
                && !file_content_hash(blob_path).is_ok_and(|actual| actual == hash)
            {
                remove_file_if_exists(blob_path)?;
            }
            return Ok(());
        }
        if !self.tracks_stats_deltas() {
            return publish_file_as_blob(source_path, blob_path, expected_hash);
        }
        let before_len = file_len_if_exists(blob_path)?;
        publish_file_as_blob(source_path, blob_path, expected_hash)?;
        let after_len = file_len_if_exists(blob_path)?;
        self.apply_stats_delta(ContentStoreStatsDelta::blob_file_change(
            before_len, after_len,
        ))
    }

    fn write_tracked_staging_bytes(&self, path: &Path, bytes: &[u8]) -> Result<()> {
        let before_len = file_len_if_exists(path)?;
        write_file_atomic(path, bytes)?;
        let after_len = file_len_if_exists(path)?;
        self.apply_stats_delta(ContentStoreStatsDelta::staging_file_change(
            before_len, after_len,
        ))
    }

    fn track_staging_file_created(&self, path: &Path) -> Result<()> {
        self.apply_stats_delta(ContentStoreStatsDelta::staging_file_change(
            None,
            file_len_if_exists(path)?,
        ))
    }

    fn remove_tracked_staging_file(&self, path: &Path) -> Result<()> {
        let before_len = file_len_if_exists(path)?;
        remove_file(path)?;
        let after_len = file_len_if_exists(path)?;
        self.apply_stats_delta(ContentStoreStatsDelta::staging_file_change(
            before_len, after_len,
        ))
    }

    fn remove_tracked_staging_file_if_exists(&self, path: &Path) -> Result<()> {
        let before_len = file_len_if_exists(path)?;
        remove_file_if_exists(path)?;
        let after_len = file_len_if_exists(path)?;
        self.apply_stats_delta(ContentStoreStatsDelta::staging_file_change(
            before_len, after_len,
        ))
    }

    fn stats_snapshot_path(&self) -> PathBuf {
        self.root.join(CONTENT_STORE_STATS_FILE)
    }

    fn lock_packed_blobs(&self) -> Result<fs::File> {
        let path = self.root.join(PACKED_BLOB_DIR).join(PACKED_BLOB_LOCK_FILE);
        if let Some(parent) = path.parent() {
            create_dir_all(parent.to_owned())?;
        }
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .map_err(|source| ContentStoreError::Io {
                path: path.clone(),
                source,
            })?;
        file.lock_exclusive()
            .map_err(|source| ContentStoreError::Io { path, source })?;
        Ok(file)
    }

    fn read_packed_blob_index_unlocked(&self) -> Result<PackedBlobIndex> {
        self.read_packed_blob_index_unlocked_with_tail_policy(false)
    }

    fn read_packed_blob_index_for_update_unlocked(&self) -> Result<PackedBlobIndex> {
        self.read_packed_blob_index_unlocked_with_tail_policy(true)
    }

    fn read_packed_blob_index_unlocked_with_tail_policy(
        &self,
        truncate_torn_tail: bool,
    ) -> Result<PackedBlobIndex> {
        let path = self.packed_blob_index_path();
        let index = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(source) if source.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(source) => {
                return Err(ContentStoreError::Io {
                    path: path.clone(),
                    source,
                });
            }
        };
        let mut index = if index.is_empty() {
            PackedBlobIndex {
                version: PACKED_BLOB_INDEX_VERSION,
                next_segment_id: 1,
                entries: BTreeMap::new(),
            }
        } else {
            let mut index = decode_packed_blob_index(&index)?;
            if index.version != PACKED_BLOB_INDEX_VERSION {
                return Err(ContentStoreError::Serde(format!(
                    "unsupported packed blob index version: {}",
                    index.version
                )));
            }
            if index.next_segment_id == 0 {
                index.next_segment_id = 1;
            }
            index
        };

        let log_path = self.packed_blob_index_log_path();
        if log_path.is_file() {
            let replayed_offset = replay_packed_blob_index_log(&log_path, &mut index)?;
            if truncate_torn_tail {
                truncate_torn_packed_blob_index_log_tail(&log_path, replayed_offset)?;
            }
        }
        if index.next_segment_id == 0 {
            index.next_segment_id = 1;
        }
        Ok(index)
    }

    fn packed_blob_location(&self, hash: &str) -> Result<Option<PackedBlobLocation>> {
        hash_hex(hash)?;
        let _lock = self.lock_packed_blobs()?;
        let index = self.read_packed_blob_index_for_update_unlocked()?;
        Ok(index.entries.get(hash).cloned())
    }

    fn write_packed_blob(&self, hash: &str, bytes: &[u8]) -> Result<bool> {
        hash_hex(hash)?;
        let _lock = self.lock_packed_blobs()?;
        let mut index = self.read_packed_blob_index_for_update_unlocked()?;
        if let Some(location) = index.entries.get(hash)
            && self.packed_blob_location_readable(location)?
        {
            return Ok(false);
        }

        let mut segment_id = if index.next_segment_id == 1 {
            "0000000000000000.pack".to_string()
        } else {
            format!("{:016x}.pack", index.next_segment_id.saturating_sub(1))
        };
        let mut segment_path = self.packed_blob_segment_path(&segment_id);
        let mut segment_len = file_len_if_exists(&segment_path)?.unwrap_or(0);
        if segment_len.saturating_add(bytes.len() as u64) > PACKED_BLOB_SEGMENT_MAX_BYTES {
            segment_id = format!("{:016x}.pack", index.next_segment_id);
            index.next_segment_id = index.next_segment_id.saturating_add(1);
            segment_path = self.packed_blob_segment_path(&segment_id);
            segment_len = 0;
        } else if index.next_segment_id == 1 {
            index.next_segment_id = 1;
        }

        if let Some(parent) = segment_path.parent() {
            create_dir_all(parent.to_owned())?;
        }
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&segment_path)
            .map_err(|source| ContentStoreError::Io {
                path: segment_path.clone(),
                source,
            })?;
        let offset = segment_len;
        file.write_all(bytes)
            .and_then(|()| file.sync_all())
            .map_err(|source| ContentStoreError::Io {
                path: segment_path.clone(),
                source,
            })?;
        if let Some(parent) = segment_path.parent() {
            sync_dir(parent)?;
        }
        let location = PackedBlobLocation {
            segment_id,
            offset,
            size_bytes: bytes.len() as u64,
        };
        index.entries.insert(hash.to_owned(), location.clone());
        self.append_packed_blob_index_log_entry_unlocked(&PackedBlobIndexLogEntry {
            version: PACKED_BLOB_INDEX_VERSION,
            content_hash: hash.to_owned(),
            location: Some(location),
            next_segment_id: index.next_segment_id,
        })?;
        Ok(true)
    }

    fn write_packed_blobs_unlocked(
        &self,
        index: &mut PackedBlobIndex,
        blobs: &[(String, &[u8])],
    ) -> Result<ContentStoreStatsDelta> {
        self.write_packed_blobs_unlocked_with_sync(index, blobs, true)
    }

    fn write_packed_blobs_unlocked_relaxed(
        &self,
        index: &mut PackedBlobIndex,
        blobs: &[(String, &[u8])],
    ) -> Result<ContentStoreStatsDelta> {
        self.write_packed_blobs_unlocked_with_sync(index, blobs, false)
    }

    fn write_packed_blobs_unlocked_with_sync(
        &self,
        index: &mut PackedBlobIndex,
        blobs: &[(String, &[u8])],
        sync_writes: bool,
    ) -> Result<ContentStoreStatsDelta> {
        if blobs.is_empty() {
            return Ok(ContentStoreStatsDelta::default());
        }

        let mut segment_files = BTreeMap::<String, fs::File>::new();
        let mut segment_lengths = BTreeMap::<String, u64>::new();
        let mut new_segment_parent_dirs = BTreeSet::<PathBuf>::new();
        let mut records = Vec::new();
        let mut stats_delta = ContentStoreStatsDelta::default();

        for (hash, bytes) in blobs {
            hash_hex(hash)?;
            if let Some(location) = index.entries.get(hash)
                && self.packed_blob_location_readable(location)?
            {
                continue;
            }

            let mut segment_id = if index.next_segment_id == 1 {
                "0000000000000000.pack".to_string()
            } else {
                format!("{:016x}.pack", index.next_segment_id.saturating_sub(1))
            };
            let mut segment_path = self.packed_blob_segment_path(&segment_id);
            let mut segment_len = match segment_lengths.get(&segment_id).copied() {
                Some(len) => len,
                None => {
                    let len = file_len_if_exists(&segment_path)?.unwrap_or(0);
                    segment_lengths.insert(segment_id.clone(), len);
                    len
                }
            };
            if segment_len.saturating_add(bytes.len() as u64) > PACKED_BLOB_SEGMENT_MAX_BYTES {
                segment_id = format!("{:016x}.pack", index.next_segment_id);
                index.next_segment_id = index.next_segment_id.saturating_add(1);
                segment_path = self.packed_blob_segment_path(&segment_id);
                segment_len = 0;
                segment_lengths.insert(segment_id.clone(), 0);
            } else if index.next_segment_id == 1 {
                index.next_segment_id = 1;
            }

            if let Some(parent) = segment_path.parent() {
                create_dir_all(parent.to_owned())?;
            }
            let existed = segment_path.exists();
            if !segment_files.contains_key(&segment_id) {
                let file = fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&segment_path)
                    .map_err(|source| ContentStoreError::Io {
                        path: segment_path.clone(),
                        source,
                    })?;
                segment_files.insert(segment_id.clone(), file);
            }
            let file = segment_files
                .get_mut(&segment_id)
                .expect("segment file was inserted");
            let offset = segment_len;
            file.write_all(bytes)
                .map_err(|source| ContentStoreError::Io {
                    path: segment_path.clone(),
                    source,
                })?;
            segment_len = segment_len.saturating_add(bytes.len() as u64);
            segment_lengths.insert(segment_id.clone(), segment_len);
            if !existed && let Some(parent) = segment_path.parent() {
                new_segment_parent_dirs.insert(parent.to_owned());
            }

            let location = PackedBlobLocation {
                segment_id: segment_id.clone(),
                offset,
                size_bytes: bytes.len() as u64,
            };
            index.entries.insert(hash.clone(), location.clone());
            records.push(PackedBlobIndexLogEntry {
                version: PACKED_BLOB_INDEX_VERSION,
                content_hash: hash.clone(),
                location: Some(location),
                next_segment_id: index.next_segment_id,
            });
            stats_delta.blob_count += 1;
            stats_delta.blob_bytes += bytes.len() as i128;
        }

        if sync_writes {
            for (segment_id, file) in segment_files {
                let path = self.packed_blob_segment_path(&segment_id);
                file.sync_all()
                    .map_err(|source| ContentStoreError::Io { path, source })?;
            }
            for parent in new_segment_parent_dirs {
                sync_dir(&parent)?;
            }
        }
        if sync_writes {
            self.append_packed_blob_index_log_entries_unlocked(&records)?;
        } else {
            self.append_packed_blob_index_log_entries_unlocked_relaxed(&records)?;
        }
        Ok(stats_delta)
    }

    fn remove_packed_blob_index_entry(&self, hash: &str) -> Result<bool> {
        hash_hex(hash)?;
        let _lock = self.lock_packed_blobs()?;
        let mut index = self.read_packed_blob_index_for_update_unlocked()?;
        let removed = index.entries.remove(hash).is_some();
        if removed {
            self.append_packed_blob_index_log_entry_unlocked(&PackedBlobIndexLogEntry {
                version: PACKED_BLOB_INDEX_VERSION,
                content_hash: hash.to_owned(),
                location: None,
                next_segment_id: index.next_segment_id,
            })?;
        }
        Ok(removed)
    }

    fn open_packed_blob_reader(
        &self,
        location: &PackedBlobLocation,
        offset: u64,
        max_bytes: usize,
    ) -> Result<BlobReader> {
        let path = self.packed_blob_segment_path(&location.segment_id);
        let mut file = fs::File::open(&path).map_err(|source| ContentStoreError::Io {
            path: path.clone(),
            source,
        })?;
        let segment_len = file
            .metadata()
            .map_err(|source| ContentStoreError::Io {
                path: path.clone(),
                source,
            })?
            .len();
        let start = offset.min(location.size_bytes);
        let physical_start = location.offset.checked_add(start).ok_or_else(|| {
            ContentStoreError::InvalidChunkedContent {
                message: format!(
                    "packed blob offset overflows: segment={}",
                    location.segment_id
                ),
            }
        })?;
        let physical_end = location
            .offset
            .checked_add(location.size_bytes)
            .ok_or_else(|| ContentStoreError::InvalidChunkedContent {
                message: format!(
                    "packed blob range overflows: segment={}",
                    location.segment_id
                ),
            })?;
        if physical_end > segment_len {
            return Err(ContentStoreError::InvalidChunkedContent {
                message: format!(
                    "packed blob range exceeds segment: segment={}, end={}, len={segment_len}",
                    location.segment_id, physical_end
                ),
            });
        }
        file.seek(SeekFrom::Start(physical_start))
            .map_err(|source| ContentStoreError::Io {
                path: path.clone(),
                source,
            })?;
        let remaining_bytes = location
            .size_bytes
            .saturating_sub(start)
            .min(u64::try_from(max_bytes).unwrap_or(u64::MAX));
        Ok(BlobReader {
            file,
            size_bytes: location.size_bytes,
            remaining_bytes,
        })
    }

    fn open_packed_blob_range_reader(
        &self,
        location: &PackedBlobLocation,
        offset: u64,
        max_bytes: usize,
    ) -> Result<BlobRangeReader> {
        let path = self.packed_blob_segment_path(&location.segment_id);
        let mut file = fs::File::open(&path).map_err(|source| ContentStoreError::Io {
            path: path.clone(),
            source,
        })?;
        let segment_len = file
            .metadata()
            .map_err(|source| ContentStoreError::Io {
                path: path.clone(),
                source,
            })?
            .len();
        let start = offset.min(location.size_bytes);
        let physical_start = location.offset.checked_add(start).ok_or_else(|| {
            ContentStoreError::InvalidChunkedContent {
                message: format!(
                    "packed blob offset overflows: segment={}",
                    location.segment_id
                ),
            }
        })?;
        let physical_end = location
            .offset
            .checked_add(location.size_bytes)
            .ok_or_else(|| ContentStoreError::InvalidChunkedContent {
                message: format!(
                    "packed blob range overflows: segment={}",
                    location.segment_id
                ),
            })?;
        if physical_end > segment_len {
            return Err(ContentStoreError::InvalidChunkedContent {
                message: format!(
                    "packed blob range exceeds segment: segment={}, end={}, len={segment_len}",
                    location.segment_id, physical_end
                ),
            });
        }
        file.seek(SeekFrom::Start(physical_start))
            .map_err(|source| ContentStoreError::Io {
                path: path.clone(),
                source,
            })?;
        let remaining_bytes = location
            .size_bytes
            .saturating_sub(start)
            .min(u64::try_from(max_bytes).unwrap_or(u64::MAX));
        Ok(BlobRangeReader {
            file,
            offset: start,
            size_bytes: location.size_bytes,
            remaining_bytes,
        })
    }

    fn packed_blob_location_readable(&self, location: &PackedBlobLocation) -> Result<bool> {
        let path = self.packed_blob_segment_path(&location.segment_id);
        let Some(segment_len) = file_len_if_exists(&path)? else {
            return Ok(false);
        };
        Ok(location
            .offset
            .checked_add(location.size_bytes)
            .is_some_and(|end| end <= segment_len))
    }

    fn packed_blob_index_path(&self) -> PathBuf {
        self.root.join(PACKED_BLOB_DIR).join(PACKED_BLOB_INDEX_FILE)
    }

    fn packed_blob_index_log_path(&self) -> PathBuf {
        self.root
            .join(PACKED_BLOB_DIR)
            .join(PACKED_BLOB_INDEX_LOG_FILE)
    }

    fn packed_blob_segment_path(&self, segment_id: &str) -> PathBuf {
        self.root
            .join(PACKED_BLOB_DIR)
            .join(PACKED_BLOB_SEGMENT_DIR)
            .join(segment_id)
    }

    fn append_packed_blob_index_log_entry_unlocked(
        &self,
        entry: &PackedBlobIndexLogEntry,
    ) -> Result<()> {
        self.append_packed_blob_index_log_entries_unlocked(std::slice::from_ref(entry))
    }

    fn append_packed_blob_index_log_entries_unlocked(
        &self,
        entries: &[PackedBlobIndexLogEntry],
    ) -> Result<()> {
        self.append_packed_blob_index_log_entries_unlocked_with_sync(entries, true)
    }

    fn append_packed_blob_index_log_entries_unlocked_relaxed(
        &self,
        entries: &[PackedBlobIndexLogEntry],
    ) -> Result<()> {
        self.append_packed_blob_index_log_entries_unlocked_with_sync(entries, false)
    }

    fn append_packed_blob_index_log_entries_unlocked_with_sync(
        &self,
        entries: &[PackedBlobIndexLogEntry],
        sync_writes: bool,
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let path = self.packed_blob_index_log_path();
        let existed = path.exists();
        if let Some(parent) = path.parent() {
            create_dir_all(parent.to_owned())?;
        }
        let mut record = Vec::new();
        for entry in entries {
            let bytes = encode_packed_blob_index_log_entry(entry)?;
            let length = u32::try_from(bytes.len()).map_err(|_| {
                ContentStoreError::Serde(format!(
                    "packed blob index entry for {} is too large: {} bytes",
                    entry.content_hash,
                    bytes.len()
                ))
            })?;
            record.extend_from_slice(&length.to_le_bytes());
            record.extend_from_slice(&bytes);
        }
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|source| ContentStoreError::Io {
                path: path.clone(),
                source,
            })?;
        file.write_all(&record)
            .map_err(|source| ContentStoreError::Io {
                path: path.clone(),
                source,
            })?;
        if sync_writes {
            file.sync_all().map_err(|source| ContentStoreError::Io {
                path: path.clone(),
                source,
            })?;
        }
        drop(file);
        if sync_writes
            && !existed
            && let Some(parent) = path.parent()
        {
            sync_dir(parent)?;
        }
        Ok(())
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
                    version: manifest.version,
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
            ContentStoreError::InvalidChunkedContent {
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
                .ok_or_else(|| ContentStoreError::InvalidChunkedContent {
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
                .ok_or_else(|| ContentStoreError::InvalidChunkedContent {
                    message: format!(
                        "extent blob offset overflows: object={}, version={}",
                        manifest.object_id, manifest.version
                    ),
                })?;
            let read_len = usize::try_from(range_end - range_start).map_err(|_| {
                ContentStoreError::InvalidChunkedContent {
                    message: format!(
                        "extent range is too large to buffer: object={}, version={}",
                        manifest.object_id, manifest.version
                    ),
                }
            })?;
            let part = self.read_blob_range(&extent.content_hash, blob_offset, read_len)?;
            if part.len() != read_len {
                return Err(ContentStoreError::InvalidChunkedContent {
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
            return Err(ContentStoreError::InvalidChunkedContent {
                message: "manifest references missing or truncated content extents".to_string(),
            });
        }
        Ok(())
    }

    fn extents_available(&self, extents: &[ContentExtent]) -> Result<bool> {
        for extent in extents {
            if !self.has_blob(&extent.content_hash)? {
                return Ok(false);
            }
            if !self.extent_range_readable(extent)? {
                return Ok(false);
            }
        }
        Ok(true)
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
        let reader = match self.open_blob_stream(&extent.content_hash) {
            Ok(reader) => reader,
            Err(ContentStoreError::Io { source, .. })
                if source.kind() == io::ErrorKind::NotFound =>
            {
                return Ok(false);
            }
            Err(error) => return Err(error),
        };
        let required_end = extent
            .blob_offset
            .checked_add(extent.size_bytes)
            .ok_or_else(|| ContentStoreError::InvalidChunkedContent {
                message: format!(
                    "extent blob range overflows for content hash {}",
                    extent.content_hash
                ),
            })?;
        Ok(required_end <= reader.size_bytes())
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
                ContentStoreError::InvalidChunkedContent {
                    message: format!(
                        "extent is too large to hash in this process: {} bytes",
                        extent.size_bytes
                    ),
                }
            })?;
            let mut reader =
                self.open_blob_range_stream(&extent.content_hash, extent.blob_offset, max_bytes)?;
            if reader.remaining_bytes() != extent.size_bytes {
                return Err(ContentStoreError::InvalidChunkedContent {
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
                return Ok(existing);
            }
            return Err(error);
        }
        self.apply_stats_delta(ContentStoreStatsDelta::manifest_created(
            &manifest,
            manifest_bytes.len() as u64,
        ))?;
        Ok(manifest)
    }

    fn ensure_quota_delta(&self, delta: ContentStoreQuotaDelta) -> Result<()> {
        let Some(quota) = &self.quota else {
            return Ok(());
        };
        let stats = self.stats()?;
        check_quota_limit(
            ContentStoreQuotaResource::BlobBytes,
            quota.max_blob_bytes,
            stats.blob_bytes,
            delta.blob_bytes,
        )?;
        check_quota_limit(
            ContentStoreQuotaResource::LogicalObjectBytes,
            quota.max_logical_object_bytes,
            stats.logical_object_bytes,
            delta.logical_object_bytes,
        )?;
        check_quota_limit(
            ContentStoreQuotaResource::ManifestBytes,
            quota.max_manifest_bytes,
            stats.manifest_bytes,
            delta.manifest_bytes,
        )?;
        check_quota_limit(
            ContentStoreQuotaResource::StagingBytes,
            quota.max_staging_bytes,
            stats.staging_bytes,
            delta.staging_bytes,
        )
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

fn encode_stats_snapshot(snapshot: &ContentStoreStatsSnapshot) -> Result<Vec<u8>> {
    sinomo_object_serde::to_bytes(snapshot, 1, LengthMode::U32)
        .map_err(|source| ContentStoreError::Serde(source.to_string()))
}

fn decode_stats_snapshot(bytes: &[u8]) -> Result<ContentStoreStatsSnapshot> {
    sinomo_object_serde::from_bytes(bytes)
        .map_err(|source| ContentStoreError::Serde(source.to_string()))
}

fn decode_packed_blob_index(bytes: &[u8]) -> Result<PackedBlobIndex> {
    sinomo_object_serde::from_bytes(bytes)
        .map_err(|source| ContentStoreError::Serde(source.to_string()))
}

fn encode_packed_blob_index_log_entry(entry: &PackedBlobIndexLogEntry) -> Result<Vec<u8>> {
    sinomo_object_serde::to_bytes(entry, 1, LengthMode::U32)
        .map_err(|source| ContentStoreError::Serde(source.to_string()))
}

fn replay_packed_blob_index_log(path: &Path, index: &mut PackedBlobIndex) -> Result<u64> {
    let mut file = fs::File::open(path).map_err(|source| ContentStoreError::Io {
        path: path.to_owned(),
        source,
    })?;
    while let Some((entry, record_end)) = read_next_packed_blob_index_log_entry(&mut file, path)? {
        if entry.version != PACKED_BLOB_INDEX_VERSION {
            return Err(ContentStoreError::Serde(format!(
                "unsupported packed blob index log version: {}",
                entry.version
            )));
        }
        if let Some(location) = entry.location {
            index.entries.insert(entry.content_hash, location);
        } else {
            index.entries.remove(&entry.content_hash);
        }
        index.next_segment_id = index.next_segment_id.max(entry.next_segment_id.max(1));
        if record_end
            != file
                .stream_position()
                .map_err(|source| ContentStoreError::Io {
                    path: path.to_owned(),
                    source,
                })?
        {
            return Err(ContentStoreError::Serde(
                "packed blob index log reader lost record alignment".to_string(),
            ));
        }
    }
    file.stream_position()
        .map_err(|source| ContentStoreError::Io {
            path: path.to_owned(),
            source,
        })
}

fn read_next_packed_blob_index_log_entry(
    file: &mut fs::File,
    path: &Path,
) -> Result<Option<(PackedBlobIndexLogEntry, u64)>> {
    let record_start = file
        .stream_position()
        .map_err(|source| ContentStoreError::Io {
            path: path.to_owned(),
            source,
        })?;
    let mut length_bytes = [0_u8; 4];
    match file.read_exact(&mut length_bytes) {
        Ok(()) => {}
        Err(source) if source.kind() == io::ErrorKind::UnexpectedEof => {
            file.seek(SeekFrom::Start(record_start))
                .map_err(|source| ContentStoreError::Io {
                    path: path.to_owned(),
                    source,
                })?;
            return Ok(None);
        }
        Err(source) => {
            return Err(ContentStoreError::Io {
                path: path.to_owned(),
                source,
            });
        }
    }
    let length = u32::from_le_bytes(length_bytes) as usize;
    let record_end = record_start
        .saturating_add(4)
        .saturating_add(u64::from(u32::from_le_bytes(length_bytes)));
    let file_len = file
        .metadata()
        .map_err(|source| ContentStoreError::Io {
            path: path.to_owned(),
            source,
        })?
        .len();
    if record_end > file_len {
        file.seek(SeekFrom::Start(record_start))
            .map_err(|source| ContentStoreError::Io {
                path: path.to_owned(),
                source,
            })?;
        return Ok(None);
    }
    let mut bytes = vec![0_u8; length];
    match file.read_exact(&mut bytes) {
        Ok(()) => {}
        Err(source) if source.kind() == io::ErrorKind::UnexpectedEof => {
            file.seek(SeekFrom::Start(record_start))
                .map_err(|source| ContentStoreError::Io {
                    path: path.to_owned(),
                    source,
                })?;
            return Ok(None);
        }
        Err(source) => {
            return Err(ContentStoreError::Io {
                path: path.to_owned(),
                source,
            });
        }
    }
    let entry = match sinomo_object_serde::from_bytes(&bytes) {
        Ok(entry) => entry,
        Err(_) if record_end >= file_len => {
            file.seek(SeekFrom::Start(record_start))
                .map_err(|source| ContentStoreError::Io {
                    path: path.to_owned(),
                    source,
                })?;
            return Ok(None);
        }
        Err(source) => return Err(ContentStoreError::Serde(source.to_string())),
    };
    Ok(Some((entry, record_end)))
}

fn truncate_torn_packed_blob_index_log_tail(path: &Path, offset: u64) -> Result<()> {
    let len = fs::metadata(path)
        .map_err(|source| ContentStoreError::Io {
            path: path.to_owned(),
            source,
        })?
        .len();
    if offset >= len {
        return Ok(());
    }
    let file = fs::OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(|source| ContentStoreError::Io {
            path: path.to_owned(),
            source,
        })?;
    file.set_len(offset)
        .and_then(|()| file.sync_all())
        .map_err(|source| ContentStoreError::Io {
            path: path.to_owned(),
            source,
        })?;
    Ok(())
}

fn file_count_delta(before_len: Option<u64>, after_len: Option<u64>) -> i128 {
    match (before_len, after_len) {
        (None, Some(_)) => 1,
        (Some(_), None) => -1,
        _ => 0,
    }
}

fn file_bytes_delta(before_len: Option<u64>, after_len: Option<u64>) -> i128 {
    i128::from(after_len.unwrap_or(0)) - i128::from(before_len.unwrap_or(0))
}

fn apply_signed_delta(value: u64, delta: i128) -> u64 {
    if delta >= 0 {
        value.saturating_add(u64::try_from(delta).unwrap_or(u64::MAX))
    } else {
        value.saturating_sub(u64::try_from(-delta).unwrap_or(u64::MAX))
    }
}

fn clip_extent(
    extent: &ContentExtent,
    logical_start: u64,
    logical_end: u64,
) -> Result<ContentExtent> {
    if logical_start < extent.logical_offset || logical_end < logical_start {
        return Err(ContentStoreError::InvalidChunkedContent {
            message: "invalid extent clip range".to_string(),
        });
    }
    let extent_end = extent
        .logical_offset
        .checked_add(extent.size_bytes)
        .ok_or_else(|| ContentStoreError::InvalidChunkedContent {
            message: "extent logical range overflow".to_string(),
        })?;
    if logical_end > extent_end {
        return Err(ContentStoreError::InvalidChunkedContent {
            message: "extent clip exceeds source extent".to_string(),
        });
    }
    let delta = logical_start - extent.logical_offset;
    Ok(ContentExtent {
        logical_offset: logical_start,
        size_bytes: logical_end - logical_start,
        content_hash: extent.content_hash.clone(),
        blob_offset: extent.blob_offset.checked_add(delta).ok_or_else(|| {
            ContentStoreError::InvalidChunkedContent {
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
            .ok_or_else(|| ContentStoreError::InvalidChunkedContent {
                message: "extent logical range overflow".to_string(),
            })?;
        let last_blob_end = last
            .blob_offset
            .checked_add(last.size_bytes)
            .ok_or_else(|| ContentStoreError::InvalidChunkedContent {
                message: "extent blob range overflow".to_string(),
            })?;
        if last.content_hash == extent.content_hash
            && last_logical_end == extent.logical_offset
            && last_blob_end == extent.blob_offset
        {
            last.size_bytes = last
                .size_bytes
                .checked_add(extent.size_bytes)
                .ok_or_else(|| ContentStoreError::InvalidChunkedContent {
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
        return Err(ContentStoreError::InvalidChunkedContent {
            message: format!("zero-sized object {object_id} {version} has non-empty extents"),
        });
    }
    let mut cursor = 0_u64;
    for extent in extents {
        hash_hex(&extent.content_hash)?;
        if extent.size_bytes == 0 {
            return Err(ContentStoreError::InvalidChunkedContent {
                message: format!("zero-sized extent in object {object_id} {version}"),
            });
        }
        if extent.logical_offset != cursor {
            return Err(ContentStoreError::InvalidChunkedContent {
                message: format!(
                    "manifest extents are not contiguous for object {object_id} {version}: expected offset {cursor}, actual {}",
                    extent.logical_offset
                ),
            });
        }
        let logical_end = extent
            .logical_offset
            .checked_add(extent.size_bytes)
            .ok_or_else(|| ContentStoreError::InvalidChunkedContent {
                message: format!("extent logical range overflow in object {object_id} {version}"),
            })?;
        extent
            .blob_offset
            .checked_add(extent.size_bytes)
            .ok_or_else(|| ContentStoreError::InvalidChunkedContent {
                message: format!("extent blob range overflow in object {object_id} {version}"),
            })?;
        cursor = logical_end;
    }
    if cursor != size_bytes {
        return Err(ContentStoreError::InvalidChunkedContent {
            message: format!(
                "manifest extents do not cover object {object_id} {version}: covered {cursor}, expected {size_bytes}"
            ),
        });
    }
    Ok(())
}

fn content_hash(bytes: &[u8]) -> String {
    format!("{HASH_PREFIX}{}", blake3::hash(bytes).to_hex())
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
    let mut file = fs::File::open(path).map_err(|source| ContentStoreError::Io {
        path: path.to_owned(),
        source,
    })?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0_u8; 64 * 1024];
    let mut size_bytes = 0u64;
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|source| ContentStoreError::Io {
                path: path.to_owned(),
                source,
            })?;
        if read == 0 {
            break;
        }
        size_bytes = size_bytes
            .checked_add(read as u64)
            .ok_or_else(|| ContentStoreError::Io {
                path: path.to_owned(),
                source: io::Error::new(io::ErrorKind::InvalidData, "content size overflow"),
            })?;
        hasher.update(&buffer[..read]);
    }
    Ok((
        format!("{HASH_PREFIX}{}", hasher.finalize().to_hex()),
        size_bytes,
    ))
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

fn validate_chunked_object_shape(
    total_size_bytes: u64,
    total_chunks: u32,
    chunk_size_bytes: usize,
) -> Result<()> {
    if total_chunks == 0 {
        return Err(ContentStoreError::InvalidChunkedContent {
            message: "chunked object transfer has no chunks".to_string(),
        });
    }
    if chunk_size_bytes == 0 {
        return Err(ContentStoreError::InvalidChunkedContent {
            message: "chunked object transfer has zero-sized chunks".to_string(),
        });
    }
    let expected_chunks = total_size_bytes.div_ceil(chunk_size_bytes as u64);
    if expected_chunks == 0 || expected_chunks > u32::MAX as u64 {
        return Err(ContentStoreError::InvalidChunkedContent {
            message: format!(
                "chunked object size cannot be represented in {chunk_size_bytes}-byte chunks"
            ),
        });
    }
    if expected_chunks != u64::from(total_chunks) {
        return Err(ContentStoreError::InvalidChunkedContent {
            message: format!(
                "chunked object chunk count does not match size: expected {expected_chunks}, actual {total_chunks}"
            ),
        });
    }
    Ok(())
}

fn create_dir_all(path: PathBuf) -> Result<()> {
    fs::create_dir_all(&path).map_err(|source| ContentStoreError::Io { path, source })
}

fn read_file(path: &Path) -> Result<Vec<u8>> {
    fs::read(path).map_err(|source| ContentStoreError::Io {
        path: path.to_owned(),
        source,
    })
}

fn read_dir(path: &Path) -> Result<Vec<fs::DirEntry>> {
    let mut entries = Vec::new();
    let reader = fs::read_dir(path).map_err(|source| ContentStoreError::Io {
        path: path.to_owned(),
        source,
    })?;
    for entry in reader {
        entries.push(entry.map_err(|source| ContentStoreError::Io {
            path: path.to_owned(),
            source,
        })?);
    }
    Ok(entries)
}

fn recursive_file_count_and_bytes(path: &Path) -> Result<(u64, u64)> {
    if !path.exists() {
        return Ok((0, 0));
    }
    let mut file_count = 0_u64;
    let mut total_bytes = 0_u64;
    for entry in read_dir(path)? {
        let entry_path = entry.path();
        if entry_path.is_dir() {
            let (child_count, child_bytes) = recursive_file_count_and_bytes(&entry_path)?;
            file_count = file_count.saturating_add(child_count);
            total_bytes = total_bytes.saturating_add(child_bytes);
            continue;
        }
        if entry_path.is_file() {
            let len = entry_path
                .metadata()
                .map_err(|source| ContentStoreError::Io {
                    path: entry_path.clone(),
                    source,
                })?
                .len();
            file_count = file_count.saturating_add(1);
            total_bytes = total_bytes.saturating_add(len);
        }
    }
    Ok((file_count, total_bytes))
}

fn projected_file_growth(path: &Path, new_len: u64) -> Result<u64> {
    let existing_len = match path.metadata() {
        Ok(metadata) => metadata.len(),
        Err(source) if source.kind() == io::ErrorKind::NotFound => 0,
        Err(source) => {
            return Err(ContentStoreError::Io {
                path: path.to_owned(),
                source,
            });
        }
    };
    Ok(new_len.saturating_sub(existing_len))
}

fn file_len_if_exists(path: &Path) -> Result<Option<u64>> {
    match path.metadata() {
        Ok(metadata) if metadata.is_file() => Ok(Some(metadata.len())),
        Ok(_) => Ok(None),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(ContentStoreError::Io {
            path: path.to_owned(),
            source,
        }),
    }
}

fn check_quota_limit(
    resource: ContentStoreQuotaResource,
    limit_bytes: Option<u64>,
    current_bytes: u64,
    additional_bytes: u64,
) -> Result<()> {
    let Some(limit_bytes) = limit_bytes else {
        return Ok(());
    };
    if current_bytes.saturating_add(additional_bytes) <= limit_bytes {
        return Ok(());
    }
    Err(ContentStoreError::QuotaExceeded {
        resource,
        limit_bytes,
        current_bytes,
        additional_bytes,
    })
}

fn capture_limit_includes_size(limit_bytes: usize, size_bytes: u64) -> bool {
    limit_bytes > 0 && size_bytes <= u64::try_from(limit_bytes).unwrap_or(u64::MAX)
}

fn remove_file(path: &Path) -> Result<()> {
    fs::remove_file(path).map_err(|source| ContentStoreError::Io {
        path: path.to_owned(),
        source,
    })
}

fn remove_file_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(ContentStoreError::Io {
            path: path.to_owned(),
            source,
        }),
    }
}

fn remove_file_if_exists_and_sync_parent(path: &Path) -> Result<bool> {
    match fs::remove_file(path) {
        Ok(()) => {
            if let Some(parent) = path.parent() {
                sync_dir(parent)?;
            }
            Ok(true)
        }
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(ContentStoreError::Io {
            path: path.to_owned(),
            source,
        }),
    }
}

fn remove_empty_dir_if_exists_and_sync_parent(path: &Path) -> Result<bool> {
    match fs::remove_dir(path) {
        Ok(()) => {
            if let Some(parent) = path.parent() {
                sync_dir(parent)?;
            }
            Ok(true)
        }
        Err(source)
            if source.kind() == io::ErrorKind::NotFound
                || source.kind() == io::ErrorKind::DirectoryNotEmpty =>
        {
            Ok(false)
        }
        Err(source) => Err(ContentStoreError::Io {
            path: path.to_owned(),
            source,
        }),
    }
}

fn file_age_nanos(path: &Path, now: SystemTime) -> Result<u64> {
    let modified = path
        .metadata()
        .and_then(|metadata| metadata.modified())
        .map_err(|source| ContentStoreError::Io {
            path: path.to_owned(),
            source,
        })?;
    Ok(now
        .duration_since(modified)
        .map(duration_nanos_saturating)
        .unwrap_or(0))
}

fn duration_nanos_saturating(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn system_time_nanos_since_epoch_saturating(time: SystemTime) -> u64 {
    time.duration_since(std::time::UNIX_EPOCH)
        .map(duration_nanos_saturating)
        .unwrap_or(0)
}

fn write_reader_to_file<R: Read>(path: &Path, reader: &mut R) -> Result<()> {
    if let Some(parent) = path.parent() {
        create_dir_all(parent.to_owned())?;
    }
    let mut file = fs::File::create(path).map_err(|source| ContentStoreError::Io {
        path: path.to_owned(),
        source,
    })?;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = reader
            .read(&mut buffer)
            .map_err(|source| ContentStoreError::Io {
                path: path.to_owned(),
                source,
            })?;
        if read == 0 {
            break;
        }
        file.write_all(&buffer[..read])
            .map_err(|source| ContentStoreError::Io {
                path: path.to_owned(),
                source,
            })?;
    }
    file.sync_all().map_err(|source| ContentStoreError::Io {
        path: path.to_owned(),
        source,
    })
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

fn write_file_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    durable_file::write_file_atomic(
        path,
        bytes,
        durable_file::DurableAtomicFileOptions::new("tmp", "tmp").sync_all(),
    )
    .map_err(content_durable_file_error)
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

fn write_content_addressed_blob_bytes_prehashed_defer_durability_existing_parent(
    hash: &str,
    blob_path: &Path,
    bytes: &[u8],
) -> Result<Option<(PathBuf, PathBuf)>> {
    let trace_started = Instant::now();
    let existing_check_started = Instant::now();
    if blob_path.is_file() {
        if file_content_hash(blob_path).is_ok_and(|actual| actual == hash) {
            content_store_perf_trace(format_args!(
                "content-addressed-blob-write hash={} bytes={} existing=true create_dir_ms=0 existing_check_ms={} total_ms={}",
                hash,
                bytes.len(),
                existing_check_started.elapsed().as_millis(),
                trace_started.elapsed().as_millis()
            ));
            return Ok(None);
        }
        remove_file_if_exists(blob_path)?;
    }
    let existing_check_ms = existing_check_started.elapsed().as_millis();

    let write_started = Instant::now();
    let mut file = match fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(blob_path)
    {
        Ok(file) => file,
        Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {
            if file_content_hash(blob_path).is_ok_and(|actual| actual == hash) {
                content_store_perf_trace(format_args!(
                    "content-addressed-blob-write hash={} bytes={} existing_race=true create_dir_ms=0 existing_check_ms={} total_ms={}",
                    hash,
                    bytes.len(),
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
    if let Err(source) = file.write_all(bytes) {
        drop(file);
        remove_file_if_exists(blob_path).ok();
        return Err(ContentStoreError::Io {
            path: blob_path.to_owned(),
            source,
        });
    }
    drop(file);
    let write_ms = write_started.elapsed().as_millis();
    let Some(sync_dir) = blob_path.parent().map(Path::to_path_buf) else {
        return Ok(None);
    };
    content_store_perf_trace(format_args!(
        "content-addressed-blob-write hash={} bytes={} existing=false create_dir_ms=0 existing_check_ms={} write_ms={} sync_file_ms=deferred sync_dir_ms=deferred total_ms={}",
        hash,
        bytes.len(),
        existing_check_ms,
        write_ms,
        trace_started.elapsed().as_millis()
    ));
    Ok(Some((blob_path.to_owned(), sync_dir)))
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

fn write_file_create_new_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        create_dir_all(parent.to_owned())?;
    }

    let tmp_path = temporary_path_for(path);
    let mut file = fs::File::create(&tmp_path).map_err(|source| ContentStoreError::Io {
        path: tmp_path.clone(),
        source,
    })?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|source| ContentStoreError::Io {
            path: tmp_path.clone(),
            source,
        })?;
    drop(file);
    if let Err(source) = fs::hard_link(&tmp_path, path) {
        let _ = fs::remove_file(&tmp_path);
        return Err(ContentStoreError::Io {
            path: path.to_owned(),
            source,
        });
    }
    fs::remove_file(&tmp_path).map_err(|source| ContentStoreError::Io {
        path: tmp_path,
        source,
    })?;
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

fn write_files_create_new_relaxed<'a, I>(files: I) -> Result<()>
where
    I: IntoIterator<Item = (PathBuf, &'a [u8])>,
{
    for (path, bytes) in files {
        if path.is_file() {
            continue;
        }
        if let Some(parent) = path.parent() {
            create_dir_all(parent.to_owned())?;
        }

        let tmp_path = temporary_path_for(&path);
        let mut file = fs::File::create(&tmp_path).map_err(|source| ContentStoreError::Io {
            path: tmp_path.clone(),
            source,
        })?;
        file.write_all(bytes)
            .map_err(|source| ContentStoreError::Io {
                path: tmp_path.clone(),
                source,
            })?;
        drop(file);
        match fs::hard_link(&tmp_path, &path) {
            Ok(()) => {}
            Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {}
            Err(source) => {
                let _ = fs::remove_file(&tmp_path);
                return Err(ContentStoreError::Io { path, source });
            }
        }
        fs::remove_file(&tmp_path).map_err(|source| ContentStoreError::Io {
            path: tmp_path,
            source,
        })?;
    }
    Ok(())
}

fn sync_dir(path: &Path) -> Result<()> {
    durable_file::sync_directory(path).map_err(content_durable_file_error)
}

fn content_durable_file_error(error: durable_file::DurableFileError) -> ContentStoreError {
    ContentStoreError::Io {
        path: PathBuf::from(error.path()),
        source: io::Error::new(io::ErrorKind::Other, error.detail().to_string()),
    }
}

fn temporary_path_for(path: &Path) -> PathBuf {
    static TMP_COUNTER: AtomicU64 = AtomicU64::new(1);
    let counter = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("tmp");
    path.with_file_name(format!(
        ".{file_name}.{}.{}.tmp",
        std::process::id(),
        counter
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn put_object_version_blob_only_large_blob_skips_manifest() {
        let root = temp_root("put_object_version_blob_only_large_blob_skips_manifest");
        let store = LocalContentStore::new(&root).expect("create store");
        let bytes = vec![0x71; PACKED_BLOB_SMALL_LIMIT_BYTES as usize + 4096];

        let manifest = store
            .put_object_version_blob_only("object-blob-only-large", "v1", &bytes)
            .expect("put blob-only large object");

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

        store
            .put_object_version("object-lazy-stats", "v2", b"lazy stats bravo")
            .expect("put second object");
        let updated = store
            .stats()
            .expect("cached stats stay current once requested");
        assert_eq!(updated.blob_count, 2);
        assert_eq!(updated.manifest_count, 2);
        assert_eq!(
            updated.logical_object_bytes,
            b"lazy stats alpha".len() as u64 + b"lazy stats bravo".len() as u64
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
    fn stats_snapshot_is_cached_and_scrub_rebuilds_drift() {
        let root = temp_root("stats_snapshot_is_cached_and_scrub_rebuilds_drift");
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

        let still_cached = store.stats().expect("cached stats");
        assert_eq!(still_cached, cached);

        let scrub = store.scrub().expect("scrub rebuilds stats");
        assert_eq!(scrub.orphan_blob_count, 1);
        assert_eq!(scrub.stats.blob_count, 2);
        assert_eq!(store.stats().expect("rebuilt stats"), scrub.stats);

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
        let error = store
            .begin_chunked_object_version(
                "object-chunk-quota",
                "v1",
                &content_hash(b"12345"),
                5,
                1,
                5,
            )
            .expect_err("staging quota should reject chunked temp allocation");
        assert!(matches!(
            error,
            ContentStoreError::QuotaExceeded {
                resource: ContentStoreQuotaResource::StagingBytes,
                ..
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
    fn chunked_object_assembler_writes_out_of_order_chunks_to_blob() {
        let root = temp_root("chunked_object_assembler_writes_out_of_order_chunks_to_blob");
        let store = LocalContentStore::new(&root).expect("create store");
        let bytes = b"chunked object bytes stored through a file-backed assembler";
        let hash = content_hash(bytes);
        let chunk_size = 11;
        let total_chunks = bytes.len().div_ceil(chunk_size) as u32;
        let mut assembler = store
            .begin_chunked_object_version(
                "object-chunked",
                "v1",
                &hash,
                bytes.len() as u64,
                total_chunks,
                chunk_size,
            )
            .expect("begin chunked object");

        assert!(
            !assembler
                .insert_chunk(1, &bytes[chunk_size..chunk_size * 2])
                .expect("second chunk")
        );
        assert!(
            !assembler
                .insert_chunk(0, &bytes[..chunk_size])
                .expect("first chunk")
        );
        assert!(
            !assembler
                .insert_chunk(1, &bytes[chunk_size..chunk_size * 2])
                .expect("duplicate second chunk")
        );
        for (chunk_index, chunk) in bytes.chunks(chunk_size).enumerate().skip(2) {
            assembler
                .insert_chunk(chunk_index as u32, chunk)
                .expect("remaining chunk");
        }
        assert!(assembler.is_complete());

        let manifest = store
            .commit_chunked_object_version(assembler)
            .expect("commit chunked object");
        assert_eq!(manifest.content_hash, hash);
        assert_eq!(manifest.size_bytes, bytes.len() as u64);
        assert_eq!(
            store.read_blob(&manifest.content_hash).expect("read blob"),
            bytes
        );
        assert!(
            std::fs::read_dir(root.join("staging"))
                .expect("staging dir")
                .next()
                .is_none(),
            "chunked assembly must clean temporary staging files"
        );

        cleanup(&root);
    }

    #[test]
    fn chunked_object_assembler_rejects_conflicting_duplicate_chunk() {
        let root = temp_root("chunked_object_assembler_rejects_conflicting_duplicate_chunk");
        let store = LocalContentStore::new(&root).expect("create store");
        let bytes = b"conflicting duplicate chunk";
        let hash = content_hash(bytes);
        let mut assembler = store
            .begin_chunked_object_version(
                "object-duplicate",
                "v1",
                &hash,
                bytes.len() as u64,
                2,
                16,
            )
            .expect("begin chunked object");
        assembler
            .insert_chunk(0, &bytes[..16])
            .expect("first chunk");
        let mut duplicate = bytes[..16].to_vec();
        duplicate[0] ^= 0xff;
        assert!(matches!(
            assembler.insert_chunk(0, &duplicate),
            Err(ContentStoreError::ConflictingChunk { chunk_index: 0, .. })
        ));

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
