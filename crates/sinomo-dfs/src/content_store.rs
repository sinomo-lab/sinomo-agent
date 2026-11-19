//! Durable local content storage for DFS blobs.

mod accounting;
mod fs_ops;
mod initialization;
#[cfg(feature = "storage-conversion")]
mod import_blob;
#[cfg(feature = "storage-conversion")]
pub use import_blob::{
    ContentImportBlobPublication, ContentImportBlobVerifier, ContentStoreVerifiedImportBlob,
};
mod preparation;
use preparation::ContentInventoryState;
mod inline_bindings;
mod readers;

pub use accounting::{
    ContentStoreQuota, ContentStoreQuotaResource, ContentStoreQuotaUsage, ContentStoreStats,
};
use accounting::{ContentStoreQuotaDelta, ContentStoreStatsDelta, ContentStoreStatsSnapshot};
use blake3::hazmat::{
    ChainingValue, HasherExt, Mode, left_subtree_len, merge_subtrees_non_root, merge_subtrees_root,
};
use fs_ops::{
    content_durable_file_error, create_dir_all, duration_nanos_saturating, file_len_if_exists,
    projected_file_growth, read_dir, read_file, recursive_file_count_and_bytes,
    remove_file_if_exists, remove_file_if_exists_and_sync_parent, sync_dir,
    system_time_nanos_since_epoch_saturating, temporary_path_for, write_file_atomic,
    write_file_create_new_atomic, write_files_create_new_relaxed, write_reader_to_file,
};
use fs2::FileExt;
#[cfg(feature = "storage-conversion")]
pub use inline_bindings::{ContentStoreStagingRow, ContentStoreStagingWriter};
use inline_bindings::{InlineReservationState, InlineStoreLease, InlineVersionState};
pub use readers::{BlobRangeReader, BlobReader, ObjectVersionReader};
use serde::{Deserialize, Serialize};
use sinomo_core::object::{LengthMode, serde_ext as sinomo_object_serde};
use sinomo_core::storage::durable_file;
use sinomo_core::storage::io_trace;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::AtomicU64;
use std::sync::{
    Arc, Mutex, OnceLock,
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
const ORPHAN_OBSERVATION_FORMAT_VERSION: u32 = 2;
const ORPHAN_OBSERVATION_DIR: &str = "orphan-observations";
const CONTENT_RESERVATION_DIR: &str = "content-reservations";
const CONTENT_RESERVATION_LOCK_FILE: &str = "write.lock";
const CONTENT_RESERVATION_BATCH_SENTINEL_FORMAT_VERSION: u32 = 4;
const CONTENT_BINDING_LAYOUT: &str = "inline-bindings-v2";
const CONTENT_RESERVATION_BATCH_SENTINEL_MARKER: &str = "sinomo-dfs-content-reservation-batches-v1";
const CONTENT_RESERVATION_BATCH_SENTINEL_SHARD: &str = "00";
const CONTENT_RESERVATION_BATCH_SENTINEL_FILE: &str = "batch-format-v1.sobj";
const CONTENT_RESERVATION_RECONCILIATION_CURSOR_FORMAT_VERSION: u32 = 1;
const CONTENT_RESERVATION_RECONCILIATION_CURSOR_FILE: &str =
    "content-reservation-reconciliation-cursor.sobj";
const MANIFEST_ABSENCE_OBSERVATION_FORMAT_VERSION: u32 = 1;
const MANIFEST_ABSENCE_OBSERVATION_DIR: &str = "manifest-absence-observations";
const VERSION_GC_DELETE_BATCH_SIZE: usize = 1024;
pub(crate) const INLINE_BLOB_SMALL_LIMIT_BYTES: u64 = 16 * 1024;

fn intrinsic_empty_content_hash() -> &'static str {
    static HASH: OnceLock<String> = OnceLock::new();
    HASH.get_or_init(|| content_hash(&[]))
}

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
    #[error(
        "content storage preparation at {root:?} remains pending after {processed_records} records"
    )]
    StoragePreparationPending {
        root: PathBuf,
        processed_records: usize,
    },
    #[error("unsupported content storage format at {root:?}; external conversion is required")]
    UnsupportedStorageFormat { root: PathBuf },
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
    synced_file: durable_file::SyncedPrivateStagedFile,
    pub content_hash: String,
    pub size_bytes: u64,
}

impl ContentStoreQuarantinedBlob {
    pub(crate) fn open_patch_reader(&self) -> Result<fs::File> {
        // Unix validates the opaque synchronized capability even when the
        // bounded read intentionally selects no payload bytes. The transport
        // also verifies the complete declared digest/length at Main ingress.
        durable_file::read_synced_private_staged_file_bounded(&self.synced_file, 0)
            .map_err(content_durable_file_error)?;
        let file =
            fs::File::open(self.synced_file.path()).map_err(|source| ContentStoreError::Io {
                path: self.synced_file.path().to_owned(),
                source,
            })?;
        if file
            .metadata()
            .map_err(|source| ContentStoreError::Io {
                path: self.synced_file.path().to_owned(),
                source,
            })?
            .len()
            != self.size_bytes
        {
            return Err(ContentStoreError::InvalidContentLayout {
                message: "quarantined patch length changed before transport".into(),
            });
        }
        Ok(file)
    }
}

impl Drop for ContentStoreQuarantinedBlob {
    fn drop(&mut self) {
        remove_file_if_exists(self.synced_file.path()).ok();
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
        io_trace::write_all(file, bytes).map_err(|source| ContentStoreError::Io {
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
        let synced_file = durable_file::sync_private_staged_file(&self.temporary_path, file)
            .map_err(content_durable_file_error)?;
        self.finished = true;
        Ok(ContentStoreQuarantinedBlob {
            store_root: self.store.root.clone(),
            synced_file,
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
    OrphanInlineBlob {
        content_hash: String,
        path: PathBuf,
        size_bytes: u64,
    },
    InvalidBlobPath {
        path: PathBuf,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ContentStoreDestructiveCatalogSeal;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentStoreGcOptions {
    pub dry_run: bool,
    pub min_orphan_age_nanos: u64,
    /// Stable generation and local observation time of the complete
    /// authoritative catalog used for this pass.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_generation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_observed_at_unix_nanos: Option<u64>,
    /// In-process capability proving that the observation was copied from a
    /// successfully validated, durably installed authoritative catalog.
    #[serde(skip)]
    destructive_catalog_seal: Option<ContentStoreDestructiveCatalogSeal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_deletions: Option<u64>,
    /// Content identities that remain reachable through metadata or another
    /// durable recovery root even when no local manifest references them.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub protected_content_hashes: BTreeSet<String>,
}

impl ContentStoreGcOptions {
    pub fn dry_run(min_orphan_age: Duration) -> Self {
        Self {
            dry_run: true,
            min_orphan_age_nanos: duration_nanos_saturating(min_orphan_age),
            catalog_generation: None,
            catalog_observed_at_unix_nanos: None,
            destructive_catalog_seal: None,
            max_deletions: None,
            protected_content_hashes: BTreeSet::new(),
        }
    }

    pub fn delete_eligible(min_orphan_age: Duration) -> Self {
        Self {
            dry_run: false,
            min_orphan_age_nanos: duration_nanos_saturating(min_orphan_age),
            catalog_generation: None,
            catalog_observed_at_unix_nanos: None,
            destructive_catalog_seal: None,
            max_deletions: None,
            protected_content_hashes: BTreeSet::new(),
        }
    }

    pub fn with_max_deletions(mut self, max_deletions: u64) -> Self {
        self.max_deletions = Some(max_deletions);
        self
    }

    /// Attach catalog identity to a non-mutating projection.
    ///
    /// This deliberately does not grant the in-process capability required by
    /// destructive collection.
    pub fn with_catalog_projection_observation(
        mut self,
        generation: impl Into<String>,
        observed_at_unix_nanos: u64,
    ) -> Self {
        self.catalog_generation = Some(generation.into());
        self.catalog_observed_at_unix_nanos = Some(observed_at_unix_nanos);
        self
    }

    pub(crate) fn with_catalog_observation(
        mut self,
        generation: impl Into<String>,
        observed_at_unix_nanos: u64,
    ) -> Self {
        self.catalog_generation = Some(generation.into());
        self.catalog_observed_at_unix_nanos = Some(observed_at_unix_nanos);
        self.destructive_catalog_seal = Some(ContentStoreDestructiveCatalogSeal);
        self
    }

    pub fn with_protected_content_hash(mut self, content_hash: impl Into<String>) -> Self {
        self.protected_content_hashes.insert(content_hash.into());
        self
    }

    pub fn with_protected_content_hashes<I, H>(mut self, content_hashes: I) -> Self
    where
        I: IntoIterator<Item = H>,
        H: Into<String>,
    {
        self.protected_content_hashes
            .extend(content_hashes.into_iter().map(Into::into));
        self
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentStoreGcReport {
    pub scrub: ContentStoreScrubReport,
    pub dry_run: bool,
    pub min_orphan_age_nanos: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_generation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_observed_at_unix_nanos: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_deletions: Option<u64>,
    pub blocked_by_scrub_issue_count: u64,
    pub eligible_orphan_count: u64,
    pub eligible_orphan_bytes: u64,
    pub deleted_orphan_count: u64,
    pub deleted_orphan_bytes: u64,
    pub retained_young_orphan_count: u64,
    pub retained_young_orphan_bytes: u64,
    /// Orphans retained because no complete local catalog observation was
    /// supplied to this pass.
    #[serde(default)]
    pub retained_missing_catalog_observation_orphan_count: u64,
    #[serde(default)]
    pub retained_missing_catalog_observation_orphan_bytes: u64,
    /// Orphans retained until a catalog generation observed after their
    /// first-absence marker is available.
    #[serde(default)]
    pub retained_same_or_stale_catalog_orphan_count: u64,
    #[serde(default)]
    pub retained_same_or_stale_catalog_orphan_bytes: u64,
    /// Orphans that had no durable first-observation marker at the start of
    /// this pass. A non-zero dry-run value tells reactive scheduling to run a
    /// write pass now instead of sleeping until `next_eligible_at_unix_nanos`.
    #[serde(default)]
    pub newly_observed_orphan_count: u64,
    #[serde(default)]
    pub newly_observed_orphan_bytes: u64,
    /// Earliest wall-clock instant at which an observed orphan reaches the
    /// configured grace period. Newly observed orphans always survive the
    /// observation pass, including when the configured duration is zero.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_eligible_at_unix_nanos: Option<u64>,
    #[serde(default)]
    pub retained_protected_orphan_count: u64,
    #[serde(default)]
    pub retained_protected_orphan_bytes: u64,
    /// Durable first-observation markers whose hash is no longer orphaned.
    /// Dry-run reports these so reactive scheduling can request a bounded write
    /// pass that resets the grace epoch before the hash can become orphaned
    /// again.
    #[serde(default)]
    pub stale_orphan_observation_count: u64,
    #[serde(default)]
    pub stale_orphan_observation_bytes: u64,
    #[serde(default)]
    pub deleted_stale_orphan_observation_count: u64,
    #[serde(default)]
    pub deleted_stale_orphan_observation_bytes: u64,
    #[serde(default)]
    pub persisted_orphan_observation_count: u64,
    #[serde(default)]
    pub persisted_orphan_observation_bytes: u64,
    #[serde(default)]
    pub retained_limit_orphan_observation_count: u64,
    #[serde(default)]
    pub retained_limit_orphan_observation_bytes: u64,
    /// Physical packed-segment reclamation derived from the durable index after
    /// logical orphan tombstones have been applied.
    /// Bounded inline generation work (copy, verification or retired files).
    #[serde(default)]
    pub pending_inline_compaction_count: u64,
    #[serde(default)]
    pub copied_inline_compaction_records: u64,
    #[serde(default)]
    pub advanced_inline_compaction_steps: u64,
    #[serde(default)]
    pub reclaimed_inline_generation_bytes: u64,
    /// Lane-grouped packed-index tombstone appends made durable by this pass.
    /// Directory fsyncs used to make relaxed orphan-marker changes durable.
    #[serde(default)]
    pub orphan_observation_directory_sync_count: u64,
    /// Blob-shard directory fsyncs used to make loose orphan unlinks durable.
    #[serde(default)]
    pub loose_blob_directory_sync_count: u64,
    /// Aggregated post-delete quota-stat snapshot writes. This is at most one
    /// for a successful collection pass.
    #[serde(default)]
    pub stats_delta_persist_count: u64,
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
    /// Additional durable absence grace after a manifest first becomes both
    /// old enough and absent from the caller's complete root set.
    #[serde(default)]
    pub min_absence_age_nanos: u64,
    /// Stable fingerprint and wall-clock generation time of the complete
    /// authoritative catalog used for this pass.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_generation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_generated_at_unix_nanos: Option<u64>,
    /// In-process capability proving that the observation was copied from a
    /// successfully validated, durably installed authoritative catalog.
    #[serde(skip)]
    destructive_catalog_seal: Option<ContentStoreDestructiveCatalogSeal>,
    pub keep_latest_versions_per_object: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_deletions: Option<u64>,
    /// Exact object/version identities that remain reachable through metadata
    /// or another durable recovery root.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub protected_versions: BTreeSet<(String, String)>,
}

impl ContentStoreVersionGcOptions {
    pub fn dry_run(min_version_age: Duration, keep_latest_versions_per_object: u64) -> Self {
        Self {
            dry_run: true,
            min_version_age_nanos: duration_nanos_saturating(min_version_age),
            min_absence_age_nanos: duration_nanos_saturating(min_version_age),
            catalog_generation: None,
            catalog_generated_at_unix_nanos: None,
            destructive_catalog_seal: None,
            keep_latest_versions_per_object,
            max_deletions: None,
            protected_versions: BTreeSet::new(),
        }
    }

    pub fn delete_eligible(
        min_version_age: Duration,
        keep_latest_versions_per_object: u64,
    ) -> Self {
        Self {
            dry_run: false,
            min_version_age_nanos: duration_nanos_saturating(min_version_age),
            min_absence_age_nanos: duration_nanos_saturating(min_version_age),
            catalog_generation: None,
            catalog_generated_at_unix_nanos: None,
            destructive_catalog_seal: None,
            keep_latest_versions_per_object,
            max_deletions: None,
            protected_versions: BTreeSet::new(),
        }
    }

    pub fn with_max_deletions(mut self, max_deletions: u64) -> Self {
        self.max_deletions = Some(max_deletions);
        self
    }

    pub fn with_min_absence_age(mut self, min_absence_age: Duration) -> Self {
        self.min_absence_age_nanos = duration_nanos_saturating(min_absence_age);
        self
    }

    /// Attach catalog identity to a non-mutating projection.
    ///
    /// This deliberately does not grant the in-process capability required by
    /// destructive collection.
    pub fn with_catalog_projection_observation(
        mut self,
        generation: impl Into<String>,
        generated_at_unix_nanos: u64,
    ) -> Self {
        self.catalog_generation = Some(generation.into());
        self.catalog_generated_at_unix_nanos = Some(generated_at_unix_nanos);
        self
    }

    pub(crate) fn with_catalog_observation(
        mut self,
        generation: impl Into<String>,
        generated_at_unix_nanos: u64,
    ) -> Self {
        self.catalog_generation = Some(generation.into());
        self.catalog_generated_at_unix_nanos = Some(generated_at_unix_nanos);
        self.destructive_catalog_seal = Some(ContentStoreDestructiveCatalogSeal);
        self
    }

    pub fn with_protected_version(
        mut self,
        object_id: impl Into<String>,
        version: impl Into<String>,
    ) -> Self {
        self.protected_versions
            .insert((object_id.into(), version.into()));
        self
    }

    pub fn with_protected_versions<I, O, V>(mut self, versions: I) -> Self
    where
        I: IntoIterator<Item = (O, V)>,
        O: Into<String>,
        V: Into<String>,
    {
        self.protected_versions.extend(
            versions
                .into_iter()
                .map(|(object_id, version)| (object_id.into(), version.into())),
        );
        self
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentStoreVersionGcReport {
    pub scrub: ContentStoreScrubReport,
    pub dry_run: bool,
    pub min_version_age_nanos: u64,
    pub min_absence_age_nanos: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_generation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_generated_at_unix_nanos: Option<u64>,
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
    /// Manifests retained because their durable first-absence grace has not
    /// elapsed, including first observations made by this pass.
    #[serde(default)]
    pub retained_absence_grace_version_count: u64,
    #[serde(default)]
    pub retained_absence_grace_logical_bytes: u64,
    #[serde(default)]
    pub newly_observed_absent_version_count: u64,
    #[serde(default)]
    pub newly_observed_absent_logical_bytes: u64,
    #[serde(default)]
    pub persisted_absence_observation_count: u64,
    #[serde(default)]
    pub retained_missing_catalog_observation_version_count: u64,
    #[serde(default)]
    pub retained_missing_catalog_observation_logical_bytes: u64,
    #[serde(default)]
    pub retained_same_or_stale_catalog_version_count: u64,
    #[serde(default)]
    pub retained_same_or_stale_catalog_logical_bytes: u64,
    /// Earliest wall-clock instant at which an unprotected, non-latest
    /// manifest reaches the configured recovery age.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_eligible_at_unix_nanos: Option<u64>,
    #[serde(default)]
    pub retained_protected_version_count: u64,
    #[serde(default)]
    pub retained_protected_logical_bytes: u64,
    pub retained_limit_version_count: u64,
    pub retained_limit_logical_bytes: u64,
    pub eligible_version_count: u64,
    pub eligible_logical_bytes: u64,
    pub eligible_manifest_bytes: u64,
    /// Old, unrooted manifests whose durable absence grace and later-catalog
    /// proof have both passed.
    #[serde(default)]
    pub deletion_ready_version_count: u64,
    #[serde(default)]
    pub deletion_ready_logical_bytes: u64,
    pub deleted_version_count: u64,
    pub deleted_logical_bytes: u64,
    pub deleted_manifest_bytes: u64,
    /// Number of bounded manifest unlink groups made durable by this pass.
    ///
    /// A pass can cover several groups with one final directory barrier. The
    /// grouping keeps unlink work bounded without repeatedly syncing the same
    /// high-cardinality object directory.
    #[serde(default)]
    pub durable_delete_batch_count: u64,
    /// Directory fsyncs used to make manifest/index unlinks and directory
    /// removals durable. This is intentionally bounded by touched directories,
    /// rather than one fsync per deleted manifest.
    #[serde(default)]
    pub durable_directory_sync_count: u64,
    /// Aggregated post-delete quota-stat snapshot writes. This is at most one
    /// for a successful collection pass.
    #[serde(default)]
    pub stats_delta_persist_count: u64,
    pub max_deletions_reached: bool,
}

/// Exact pre-catalog ownership of one content identity.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
pub struct ContentStoreReservationIdentity {
    pub object_id: String,
    pub version: String,
    pub content_hash: String,
}

/// One durable pre-catalog reservation and the time at which it became a root.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct ContentStoreReservationRecord {
    pub identity: ContentStoreReservationIdentity,
    pub reserved_at_unix_nanos: u64,
    /// Required provenance minted by the actual current content owner.
    pub causal_stamp: ContentStoreCausalStamp,
}

/// Local durable ordering; wall clocks never establish a reservation/root-cut order.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct ContentStoreCausalStamp {
    store_incarnation: String,
    catalog_scan_epoch: u64,
}

/// Minted under the reservation flock before the first primary root page.
/// Continuations and installation retain this original cut unchanged.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct ContentStoreCatalogScanCut {
    stamp: ContentStoreCausalStamp,
    started_at_unix_nanos: u64,
}

impl ContentStoreCatalogScanCut {
    pub(crate) fn started_at_unix_nanos(&self) -> u64 {
        self.started_at_unix_nanos
    }

    pub(crate) fn validate(&self) -> Result<()> {
        validate_content_causal_stamp(&self.stamp)?;
        if self.stamp.catalog_scan_epoch == 0 || self.started_at_unix_nanos == 0 {
            return Err(ContentStoreError::InvalidContentLayout {
                message: "invalid content catalog scan cut".to_owned(),
            });
        }
        Ok(())
    }
}

struct ContentReservationWriteGuard {
    _file: fs::File,
    sentinel: ContentStoreReservationBatchSentinel,
}

/// Durable roots retained by local content reservations.
#[derive(Debug, Clone, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ContentStoreReservationRoots {
    pub reservation_count: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reservations: Vec<ContentStoreReservationRecord>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub versions: BTreeSet<(String, String)>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub content_hashes: BTreeSet<String>,
}

/// One bounded page of complete-catalog reservation reconciliation.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct ContentStoreReservationReconciliationOptions {
    /// Only the validated catalog owner may attach negative-release provenance.
    #[serde(skip)]
    catalog_scan_cut: Option<ContentStoreCatalogScanCut>,
    /// Stable fingerprint of the complete catalog used for this pass.
    ///
    /// Reconciliation progress is durable only within one catalog generation.
    /// A new generation restarts scanning from the beginning.
    #[serde(default)]
    pub catalog_generation: String,
    pub catalog_generated_at_unix_nanos: u64,
    pub rejection_grace_nanos: u64,
    pub max_reservations: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<ContentStoreReservationIdentity>,
}

impl ContentStoreReservationReconciliationOptions {
    pub fn bounded(
        catalog_generated_at_unix_nanos: u64,
        rejection_grace: Duration,
        max_reservations: u64,
    ) -> Self {
        Self {
            catalog_scan_cut: None,
            catalog_generation: String::new(),
            catalog_generated_at_unix_nanos,
            rejection_grace_nanos: duration_nanos_saturating(rejection_grace),
            max_reservations: max_reservations.max(1),
            after: None,
        }
    }

    pub fn with_catalog_generation(mut self, generation: impl Into<String>) -> Self {
        self.catalog_generation = generation.into();
        self
    }

    pub(crate) fn with_catalog_scan_cut(mut self, cut: Option<ContentStoreCatalogScanCut>) -> Self {
        self.catalog_scan_cut = cut;
        self
    }

    pub fn after(mut self, identity: ContentStoreReservationIdentity) -> Self {
        self.after = Some(identity);
        self
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ContentStoreReservationReconciliationReport {
    pub reservation_count: u64,
    pub examined_reservation_count: u64,
    pub released_catalog_covered_count: u64,
    pub released_rejected_count: u64,
    pub retained_catalog_too_old_count: u64,
    pub retained_rejection_grace_count: u64,
    pub remaining_after_batch_count: u64,
    pub has_more: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<ContentStoreReservationIdentity>,
}

/// Authoritative reason for releasing a pre-catalog reservation.
///
/// Ordinary write and stage paths deliberately never call this API. A catalog
/// reconciler may release a reservation only after proving it is represented
/// by a complete authoritative root set, or after proving the operation was
/// durably rejected.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum ContentStoreReservationResolution {
    CatalogCovered,
    Rejected,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
struct ContentStoreReservationBatchSentinel {
    format_version: u32,
    marker: String,
    store_incarnation: String,
    catalog_scan_epoch: u64,
    // Required current schema gate: a future binding layout must not be ignored.
    content_binding_layout: String,
    /// Current imports may expose ready bindings before their complete inventory.
    /// None is a fresh, completely current store without an import.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    inventory: Option<ContentInventoryState>,
}

impl ContentStoreReservationBatchSentinel {
    fn new() -> Self {
        Self {
            format_version: CONTENT_RESERVATION_BATCH_SENTINEL_FORMAT_VERSION,
            marker: CONTENT_RESERVATION_BATCH_SENTINEL_MARKER.to_owned(),
            store_incarnation: sinomo_core::Id::generate().to_string(),
            catalog_scan_epoch: 0,
            content_binding_layout: CONTENT_BINDING_LAYOUT.to_owned(),
            inventory: None,
        }
    }

    fn stamp(&self) -> ContentStoreCausalStamp {
        ContentStoreCausalStamp {
            store_incarnation: self.store_incarnation.clone(),
            catalog_scan_epoch: self.catalog_scan_epoch,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
struct ContentStoreReservationReconciliationCursor {
    format_version: u32,
    catalog_generation: String,
    after: ContentStoreReservationIdentity,
}

#[derive(Debug, Clone)]
pub struct LocalContentStore {
    root: PathBuf,
    inline_store: InlineStoreLease,
    content_reservation_writer_initialized: bool,
    quota: Option<ContentStoreQuota>,
    quota_stats_initialized: Arc<AtomicBool>,
    stream_manifest_cache: Arc<Mutex<StreamManifestCache>>,
    #[cfg(test)]
    blob_stream_open_count: Arc<AtomicU64>,
    #[cfg(test)]
    stream_manifest_validation_count: Arc<AtomicU64>,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct OrphanObservation {
    format_version: u32,
    content_hash: String,
    first_observed_at_unix_nanos: u64,
    catalog_generation: String,
    catalog_observed_at_unix_nanos: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ManifestAbsenceObservation {
    format_version: u32,
    object_id: String,
    version: String,
    content_hash: String,
    first_observed_at_unix_nanos: u64,
    catalog_generation: String,
    catalog_generated_at_unix_nanos: u64,
}

#[derive(Debug, Clone)]
struct OrphanObservationGcCandidate {
    path: PathBuf,
    size_bytes: u64,
}

#[derive(Debug, Clone)]
enum OrphanBlobGcCandidate {
    Loose {
        content_hash: String,
        path: PathBuf,
        size_bytes: u64,
    },
    Inline {
        content_hash: String,
        size_bytes: u64,
    },
}

impl OrphanBlobGcCandidate {
    fn size_bytes(&self) -> u64 {
        match self {
            Self::Loose { size_bytes, .. } | Self::Inline { size_bytes, .. } => *size_bytes,
        }
    }

    fn content_hash(&self) -> &str {
        match self {
            Self::Loose { content_hash, .. } | Self::Inline { content_hash, .. } => content_hash,
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
    modified_nanos: u64,
    manifest_bytes: u64,
    logical_bytes: u64,
    extent_count: u64,
}

#[derive(Debug, Default)]
struct VersionManifestDeletionBatchResult {
    deleted_version_count: u64,
    deleted_logical_bytes: u64,
    deleted_manifest_bytes: u64,
    stats_delta: ContentStoreStatsDelta,
}

#[derive(Debug)]
struct RelaxedOrphanObservationWrite {
    observation: OrphanObservation,
    created: bool,
    size_bytes: u64,
    parent: PathBuf,
    root_created: bool,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum RelaxedDirectoryRemoval {
    Removed,
    Retained,
    Missing,
}

struct StreamedObjectVersionFile {
    path: PathBuf,
    hash: String,
    size_bytes: u64,
    captured_bytes: Option<Vec<u8>>,
}

impl LocalContentStore {
    /// Version of the sole admitted content-root representation.
    pub const FORMAT_VERSION: u32 = CONTENT_RESERVATION_BATCH_SENTINEL_FORMAT_VERSION;

    pub fn new(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        crate::storage_preparation::prepare_storage_root_before_open(
            &root,
            &crate::DfsStoragePreparationScope::Content,
        )
        .map_err(|error| match error {
            crate::MetadataBackendError::StoragePreparationPending {
                root,
                processed_records,
            } => ContentStoreError::StoragePreparationPending {
                root,
                processed_records,
            },
            crate::MetadataBackendError::Io { path, source } => {
                ContentStoreError::Io { path, source }
            }
            error => ContentStoreError::InvalidContentLayout {
                message: error.to_string(),
            },
        })?;
        Self::initialize_current(root)
    }

    // The explicit staging capability creates CURRENT output, not a cold-open
    // source. Keep one initializer and every current marker/intent/owner check.
    fn initialize_current(root: PathBuf) -> Result<Self> {
        let marker = root.join("content-reservations/00/batch-format-v1.sobj");
        let has_marker = || {
            fs::read(&marker)
                .ok()
                .and_then(|bytes| decode_content_reservation_batch_sentinel(&bytes).ok())
                .is_some_and(|sentinel| {
                    validate_content_reservation_batch_sentinel(&sentinel).is_ok()
                })
        };
        let mut has_current_marker = has_marker();
        let _initialization = if has_current_marker {
            None
        } else {
            create_dir_all(root.clone())?;
            let guard = initialization::InitializationGuard::acquire(&root)?;
            // A preceding initializer can finish while this opener waits.
            // Never publish a new intent from an earlier empty-root read.
            has_current_marker = has_marker();
            Some(guard)
        };
        if root.exists()
            && !has_current_marker
            && !initialization::pending(&root)?
            && fs::read_dir(&root)
                .map_err(|source| ContentStoreError::Io {
                    path: root.clone(),
                    source,
                })?
                .next()
                .is_some()
        {
            return Err(ContentStoreError::UnsupportedStorageFormat { root });
        }
        create_dir_all(root.clone())?;
        if !has_current_marker {
            initialization::begin(&root)?;
        }
        let mut store = Self::open_for_inspection(root);
        // Current initialization resumes under the same actual content owner.
        // Existing current stores must retain their owner file.
        let _owner =
            inline_bindings::ContentMutationGuard::acquire(&store.root, !has_current_marker)?;
        create_dir_all(store.root.join("blobs"))?;
        create_dir_all(store.root.join("objects"))?;
        create_dir_all(store.root.join("staging"))?;
        store.initialize_content_reservation_writer_state()?;
        store.initialize_inline_bindings()?;
        inline_bindings::initialize_file_reclamation_lock(&store.root)?;
        initialization::complete(&store.root)?;
        Ok(store)
    }

    /// Current initialization recognition for a converter's preparation boundary.
    /// This grants neither ownership nor permission to mutate the root.
    #[cfg(feature = "storage-conversion")]
    pub fn has_current_initialization(root: &Path) -> Result<bool> {
        initialization::pending(root)
    }

    /// Stop the real current initializer at a durable boundary for owner/restart
    /// tests. No marker, incarnation or authority is supplied by the fixture.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-support"))]
    pub fn interrupt_current_initialization_for_test(root: &Path, phase: u8) -> Result<String> {
        assert!((1..=5).contains(&phase));
        create_dir_all(root.to_owned())?;
        let _initialization = initialization::InitializationGuard::acquire(root)?;
        initialization::begin(root)?;
        let expected = initialization::marker(root)?.expect("real initialization intent");
        if phase >= 2 {
            let mut store = Self::open_for_inspection(root);
            let _owner = inline_bindings::ContentMutationGuard::acquire(root, true)?;
            create_dir_all(root.join("blobs/00"))?;
            if phase >= 3 { store.initialize_content_reservation_writer_state()?; }
            if phase >= 4 { store.initialize_inline_bindings()?; }
            if phase >= 5 { inline_bindings::initialize_file_reclamation_lock(root)?; }
        }
        Ok(expected.store_incarnation)
    }

    /// Opens a read-only view without initializing the content-store layout.
    ///
    /// This constructor is intended for advisory inspection of an existing
    /// store. Code that may write must use [`Self::new`] first.
    pub fn open_for_inspection(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        let inline_store = InlineStoreLease::new(&root);
        let root = inline_store.root().to_owned();
        Self {
            root,
            inline_store,
            content_reservation_writer_initialized: false,
            quota: None,
            // A persisted stats snapshot is derived observability state. It is
            // not trusted for quota admission until this process has rebuilt
            // it from the durable store while quota mode is active.
            quota_stats_initialized: Arc::new(AtomicBool::new(false)),
            stream_manifest_cache: Arc::new(Mutex::new(StreamManifestCache::default())),
            #[cfg(test)]
            blob_stream_open_count: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            stream_manifest_validation_count: Arc::new(AtomicU64::new(0)),
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

    fn initialize_content_reservation_writer_state(&mut self) -> Result<()> {
        let reservation_root = self.root.join(CONTENT_RESERVATION_DIR);
        let root_existed = reservation_root.is_dir();
        create_dir_all(reservation_root.clone())?;

        let lock_path = self.content_reservation_lock_path();
        let lock_existed = lock_path.is_file();
        let lock = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|source| ContentStoreError::Io {
                path: lock_path.clone(),
                source,
            })?;
        FileExt::lock_exclusive(&lock).map_err(|source| ContentStoreError::Io {
            path: lock_path,
            source,
        })?;
        if !lock_existed {
            sync_dir(&reservation_root)?;
            if !root_existed {
                sync_dir(&self.root)?;
            }
        }

        let sentinel_path = self.content_reservation_batch_sentinel_path();
        let shard_existed = sentinel_path.parent().is_some_and(Path::is_dir);
        let sentinel = initialization::marker(&self.root)?.unwrap_or_else(|| {
            let mut sentinel = ContentStoreReservationBatchSentinel::new();
            sentinel.content_binding_layout = self.inline_store.expected_layout().into();
            sentinel
        });
        self.validate_owner_sentinel(&sentinel)?;
        let bytes = encode_content_reservation_batch_sentinel(&sentinel)?;
        // The reservation lock serializes initialization. Reopening an existing
        // marker must not stage and fsync a disposable copy only to discover
        // AlreadyExists at publication. Retain atomic no-replace publication
        // for a missing marker and still validate/sync every existing marker.
        let created = if sentinel_path
            .try_exists()
            .map_err(|source| ContentStoreError::Io {
                path: sentinel_path.clone(),
                source,
            })? {
            false
        } else {
            match write_file_create_new_atomic(&sentinel_path, &bytes) {
                Ok(()) => true,
                Err(ContentStoreError::Io { source, .. })
                    if source.kind() == io::ErrorKind::AlreadyExists =>
                {
                    false
                }
                Err(error) => return Err(error),
            }
        };
        if created {
            self.sync_new_sharded_record_parents(
                &reservation_root,
                sentinel_path.parent(),
                root_existed,
                shard_existed,
            )?;
        } else {
            let existing = decode_content_reservation_batch_sentinel(&read_file(&sentinel_path)?)?;
            self.validate_owner_sentinel(&existing)?;
            if initialization::pending(&self.root)? && existing != sentinel {
                return Err(ContentStoreError::InvalidContentLayout {
                    message: "current content marker differs from initialization intent".into(),
                });
            }
        }
        // Windows rejects FlushFileBuffers for the read-only handle returned by
        // File::open, even when this process owns the file. Open the durable
        // sentinel writable so sync_all preserves the publication fence on
        // every supported platform.
        fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&sentinel_path)
            .and_then(|file| file.sync_all())
            .map_err(|source| ContentStoreError::Io {
                path: sentinel_path.clone(),
                source,
            })?;
        let sentinel_shard =
            sentinel_path
                .parent()
                .ok_or_else(|| ContentStoreError::InvalidContentLayout {
                    message: format!(
                        "content reservation batch sentinel has no parent: {}",
                        sentinel_path.display()
                    ),
                })?;
        // This explicit chain is the downgrade-safety publication fence:
        // sentinel bytes, sentinel shard, reservation root, then content root.
        sync_dir(sentinel_shard)?;
        sync_dir(&reservation_root)?;
        sync_dir(&self.root)?;
        self.content_reservation_writer_initialized = true;
        Ok(())
    }

    fn lock_content_reservations_for_write(&self) -> Result<ContentReservationWriteGuard> {
        if !self.content_reservation_writer_initialized {
            return Err(ContentStoreError::InvalidContentLayout {
                message: "content reservation writes require LocalContentStore::new".to_owned(),
            });
        }
        let path = self.content_reservation_lock_path();
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|source| ContentStoreError::Io {
                path: path.clone(),
                source,
            })?;
        FileExt::lock_exclusive(&file).map_err(|source| ContentStoreError::Io { path, source })?;
        let sentinel_path = self.content_reservation_batch_sentinel_path();
        let sentinel = decode_content_reservation_batch_sentinel(&read_file(&sentinel_path)?)?;
        self.validate_owner_sentinel(&sentinel)?;
        Ok(ContentReservationWriteGuard {
            _file: file,
            sentinel,
        })
    }

    /// A durable cross-process fence, published before any page request.
    pub(crate) fn begin_content_root_catalog_scan(&self) -> Result<ContentStoreCatalogScanCut> {
        self.begin_content_root_catalog_scan_at(system_time_nanos_since_epoch_saturating(
            SystemTime::now(),
        ))
    }

    pub(crate) fn begin_content_root_catalog_scan_at(
        &self,
        started_at_unix_nanos: u64,
    ) -> Result<ContentStoreCatalogScanCut> {
        self.require_complete_content_inventory()?;
        let _owner = self.content_mutation_guard()?;
        self.require_complete_inventory_under_owner()?;
        let mut guard = self.lock_content_reservations_for_write()?;
        guard.sentinel.catalog_scan_epoch = guard
            .sentinel
            .catalog_scan_epoch
            .checked_add(1)
            .ok_or_else(|| ContentStoreError::InvalidContentLayout {
                message: "content catalog scan epoch exhausted".to_owned(),
            })?;
        let cut = ContentStoreCatalogScanCut {
            stamp: guard.sentinel.stamp(),
            started_at_unix_nanos,
        };
        cut.validate()?;
        write_file_atomic(
            &self.content_reservation_batch_sentinel_path(),
            &encode_content_reservation_batch_sentinel(&guard.sentinel)?,
        )?;
        Ok(cut)
    }

    pub(crate) fn catalog_scan_cut_matches_store(
        &self,
        cut: &ContentStoreCatalogScanCut,
    ) -> Result<bool> {
        let _owner = self.content_mutation_guard()?;
        cut.validate()?;
        let guard = self.lock_content_reservations_for_write()?;
        Ok(
            cut.stamp.store_incarnation == guard.sentinel.store_incarnation
                && cut.stamp.catalog_scan_epoch <= guard.sentinel.catalog_scan_epoch,
        )
    }

    fn lock_content_reservations_for_read_if_present(&self) -> Result<Option<fs::File>> {
        let path = self.content_reservation_lock_path();
        let file = match fs::OpenOptions::new().read(true).open(&path) {
            Ok(file) => file,
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(source) => return Err(ContentStoreError::Io { path, source }),
        };
        FileExt::lock_shared(&file).map_err(|source| ContentStoreError::Io { path, source })?;
        Ok(Some(file))
    }

    /// Durably reserves an exact content identity before any blob or manifest
    /// becomes visible outside a quarantine file.
    pub fn ensure_content_reservation(
        &self,
        object_id: &str,
        version: &str,
        content_hash: &str,
    ) -> Result<()> {
        let _owner = self.content_mutation_guard()?;
        hash_hex(content_hash)?;
        let identity = ContentStoreReservationIdentity {
            object_id: object_id.to_owned(),
            version: version.to_owned(),
            content_hash: content_hash.to_owned(),
        };
        // Reset a previous absence epoch before publishing the new local root.
        // A crash between these operations only extends later retention.
        self.remove_manifest_absence_observation(&identity)?;
        self.persist_content_reservation(&identity)
    }

    /// Restores an existing retention root without shortening its recorded grace.
    /// The current store mints fresh causal provenance under its write owner.
    pub fn ensure_content_reservation_not_before(
        &self,
        identity: &ContentStoreReservationIdentity,
        minimum_reserved_at_unix_nanos: u64,
    ) -> Result<()> {
        self.ensure_content_reservation_not_before_for_inventory(
            identity,
            minimum_reserved_at_unix_nanos,
            None,
        )
    }

    fn ensure_content_reservation_not_before_for_inventory(
        &self,
        identity: &ContentStoreReservationIdentity,
        minimum_reserved_at_unix_nanos: u64,
        inventory: Option<&str>,
    ) -> Result<()> {
        let _owner = self.content_mutation_guard()?;
        hash_hex(&identity.content_hash)?;
        let reservation = self.lock_content_reservations_for_write()?;
        if let Some(identity) = inventory {
            preparation::require_pending_inventory(&reservation.sentinel, identity)?;
        }
        self.remove_manifest_absence_observation(identity)?;
        let previous = match self.inline_reservation_state(identity)? {
            Some(InlineReservationState::Live(record)) => record.reserved_at_unix_nanos,
            _ => 0,
        };
        let record = ContentStoreReservationRecord {
            identity: identity.clone(),
            reserved_at_unix_nanos: refreshed_reservation_timestamp(Some(
                previous.max(minimum_reserved_at_unix_nanos),
            )),
            causal_stamp: reservation.sentinel.stamp(),
        };
        self.inline_reserve_owned_identity_unlocked(&record, &reservation.sentinel)?;
        Ok(())
    }

    /// Returns every durable pre-catalog root in deterministic order.
    pub fn content_reservation_roots(&self) -> Result<ContentStoreReservationRoots> {
        let reservations = self.read_content_reservations()?;
        let versions: BTreeSet<_> = reservations
            .iter()
            .map(|reservation| {
                (
                    reservation.identity.object_id.clone(),
                    reservation.identity.version.clone(),
                )
            })
            .collect();
        let content_hashes: BTreeSet<_> = reservations
            .iter()
            .map(|reservation| reservation.identity.content_hash.clone())
            .collect();
        Ok(ContentStoreReservationRoots {
            reservation_count: reservations.len() as u64,
            reservations,
            versions,
            content_hashes,
        })
    }

    /// Releases one exact reservation after an external authoritative
    /// reconciler proves either catalog coverage or durable rejection.
    #[cfg(any(test, feature = "test-support"))]
    fn reconcile_content_reservation(
        &self,
        object_id: &str,
        version: &str,
        content_hash: &str,
        resolution: ContentStoreReservationResolution,
    ) -> Result<bool> {
        let _owner = self.content_mutation_guard()?;
        let _authoritative_resolution = resolution;
        let identity = ContentStoreReservationIdentity {
            object_id: object_id.to_owned(),
            version: version.to_owned(),
            content_hash: content_hash.to_owned(),
        };
        let _lock = self.lock_content_reservations_for_write()?;
        let inventory = self.read_content_reservation_inventory_unlocked()?;
        if inventory
            .binary_search_by(|reservation| reservation.identity.cmp(&identity))
            .is_err()
        {
            return Ok(false);
        }
        self.inline_release_reservations_unlocked(&BTreeSet::from([identity]))?;
        Ok(true)
    }

    pub(crate) fn release_all_content_reservations_for_runtime_probe(&self) -> Result<u64> {
        let _owner = self.content_mutation_guard()?;
        let _lock = self.lock_content_reservations_for_write()?;
        let inventory = self.read_content_reservation_inventory_unlocked()?;
        let resolved_identities = inventory
            .iter()
            .map(|reservation| reservation.identity.clone())
            .collect::<BTreeSet<_>>();
        let released = resolved_identities.len() as u64;
        if released != 0 {
            self.inline_release_reservations_unlocked(&resolved_identities)?;
        }
        Ok(released)
    }

    /// Test-support hook for releasing a fixture reservation.
    ///
    /// Production callers cannot assert catalog coverage directly; only the
    /// crate-internal sealed catalog lifecycle may reconcile reservations.
    #[cfg(any(test, feature = "test-support"))]
    #[doc(hidden)]
    pub fn release_content_reservation_for_test(
        &self,
        object_id: &str,
        version: &str,
        content_hash: &str,
    ) -> Result<bool> {
        self.reconcile_content_reservation(
            object_id,
            version,
            content_hash,
            ContentStoreReservationResolution::CatalogCovered,
        )
    }

    /// Reconciles a bounded page of reservations against one complete,
    /// authoritative catalog view.
    ///
    /// Exact object-version coverage releases a reservation immediately. A
    /// matching content hash owned by another version protects the deduplicated
    /// blob, but is not success evidence for this reserved publication. An
    /// absent version is rejected only when the catalog was generated after
    /// its abandonment grace elapsed. Progress is persisted by catalog
    /// generation so retained reservations at the front of the identity order
    /// cannot starve later reservations.
    pub(crate) fn reconcile_content_reservations_with_catalog(
        &self,
        catalog_versions: &BTreeSet<(String, String)>,
        _catalog_content_hashes: &BTreeSet<String>,
        options: ContentStoreReservationReconciliationOptions,
    ) -> Result<ContentStoreReservationReconciliationReport> {
        self.require_complete_content_inventory()?;
        let _owner = self.content_mutation_guard()?;
        self.require_complete_inventory_under_owner()?;
        if options.catalog_generation.is_empty() {
            return Err(ContentStoreError::InvalidContentLayout {
                message: "content reservation reconciliation requires a catalog generation"
                    .to_owned(),
            });
        }
        if options.max_reservations == 0 {
            return Err(ContentStoreError::InvalidContentLayout {
                message: "content reservation reconciliation requires a positive batch size"
                    .to_owned(),
            });
        }

        let guard = self.lock_content_reservations_for_write()?;
        let causal_cut = options.catalog_scan_cut.as_ref().filter(|cut| {
            cut.validate().is_ok()
                && cut.stamp.store_incarnation == guard.sentinel.store_incarnation
                && cut.stamp.catalog_scan_epoch <= guard.sentinel.catalog_scan_epoch
        });
        let persisted_cursor = self.read_content_reservation_reconciliation_cursor()?;
        let durable_after = match persisted_cursor {
            Some(cursor) if cursor.catalog_generation == options.catalog_generation => {
                Some(cursor.after)
            }
            Some(_) => {
                self.clear_content_reservation_reconciliation_cursor()?;
                None
            }
            None => None,
        };
        let inventory = self.read_content_reservation_inventory_unlocked()?;
        let reservations = &inventory;
        let reservation_count = reservations.len() as u64;
        let start_after = durable_after.as_ref().or(options.after.as_ref());
        let start_index = start_after
            .map(|after| reservations.partition_point(|reservation| reservation.identity <= *after))
            .unwrap_or(0);
        let max_reservations = usize::try_from(options.max_reservations).unwrap_or(usize::MAX);
        let end_index = start_index
            .saturating_add(max_reservations)
            .min(reservations.len());
        let mut report = ContentStoreReservationReconciliationReport {
            reservation_count,
            remaining_after_batch_count: reservations.len().saturating_sub(end_index) as u64,
            has_more: end_index < reservations.len(),
            ..ContentStoreReservationReconciliationReport::default()
        };
        let mut resolved_identities = BTreeSet::new();

        for reservation in &reservations[start_index..end_index] {
            report.examined_reservation_count = report.examined_reservation_count.saturating_add(1);
            let identity = &reservation.identity;
            let version_covered =
                catalog_versions.contains(&(identity.object_id.clone(), identity.version.clone()));
            let resolution = match version_covered {
                true => Some(ContentStoreReservationResolution::CatalogCovered),
                false
                    if !causal_cut.is_some_and(|cut| {
                        reservation.causal_stamp.store_incarnation == cut.stamp.store_incarnation
                            && reservation.causal_stamp.catalog_scan_epoch
                                < cut.stamp.catalog_scan_epoch
                    }) || options.catalog_generated_at_unix_nanos
                        <= reservation.reserved_at_unix_nanos =>
                {
                    report.retained_catalog_too_old_count =
                        report.retained_catalog_too_old_count.saturating_add(1);
                    None
                }
                false
                    if options.catalog_generated_at_unix_nanos
                        <= reservation
                            .reserved_at_unix_nanos
                            .saturating_add(options.rejection_grace_nanos) =>
                {
                    report.retained_rejection_grace_count =
                        report.retained_rejection_grace_count.saturating_add(1);
                    None
                }
                false => Some(ContentStoreReservationResolution::Rejected),
            };
            if let Some(resolution) = resolution {
                resolved_identities.insert(identity.clone());
                match resolution {
                    ContentStoreReservationResolution::CatalogCovered => {
                        report.released_catalog_covered_count =
                            report.released_catalog_covered_count.saturating_add(1);
                    }
                    ContentStoreReservationResolution::Rejected => {
                        report.released_rejected_count =
                            report.released_rejected_count.saturating_add(1);
                    }
                }
            }
            report.next_cursor = Some(identity.clone());
        }

        // Resolution files are durable before publishing the continuation
        // cursor. A crash can therefore only repeat work, never skip roots.
        self.inline_release_reservations_unlocked(&resolved_identities)?;
        if report.has_more {
            if let Some(next_cursor) = report.next_cursor.as_ref() {
                self.persist_content_reservation_reconciliation_cursor(
                    &options.catalog_generation,
                    next_cursor,
                )?;
            }
        } else {
            self.clear_content_reservation_reconciliation_cursor()?;
            report.next_cursor = None;
        }
        Ok(report)
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
        let file = durable_file::create_private_staged_file(&temporary_path)
            .map_err(content_durable_file_error)?;
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

    pub(crate) fn capture_quarantined_patch_reader(
        &self,
        reader: &mut dyn Read,
    ) -> Result<ContentStoreQuarantinedBlob> {
        let mut ingest = self.begin_quarantined_blob_ingest(u64::MAX, None)?;
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let read = reader
                .read(&mut buffer)
                .map_err(|source| ContentStoreError::Io {
                    path: ingest.temporary_path.clone(),
                    source,
                })?;
            if read == 0 {
                break;
            }
            ingest.write_chunk(&buffer[..read])?;
        }
        // The caller had no advance length declaration. This is the exact
        // byte count observed at EOF by the SAME quarantined ingest engine.
        ingest.expected_size_bytes = ingest.received_size_bytes;
        ingest.finish_quarantined()
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
        self.prepare_version_if_missing(object_id, version)?;
        self.bind_current_quarantined_blob(object_id, version, blob)
    }

    fn bind_current_quarantined_blob(
        &self,
        object_id: &str,
        version: &str,
        blob: ContentStoreQuarantinedBlob,
    ) -> Result<ContentManifest> {
        let _owner = self.content_mutation_guard()?;
        if blob.store_root != self.root {
            return Err(ContentStoreError::InvalidContentLayout {
                message: format!(
                    "quarantined blob belongs to {}, not content store {}",
                    blob.store_root.display(),
                    self.root.display()
                ),
            });
        }
        if blob.size_bytes <= INLINE_BLOB_SMALL_LIMIT_BYTES
            && self.inline_sentinel_unlocked()?.is_some()
            && let Some(bytes) = durable_file::read_synced_private_staged_file_bounded(
                &blob.synced_file,
                INLINE_BLOB_SMALL_LIMIT_BYTES as usize,
            )
            .map_err(content_durable_file_error)?
        {
            ensure_bytes_match_hash(&blob.content_hash, &bytes)?;
            if bytes.len() as u64 != blob.size_bytes {
                return Err(ContentStoreError::InvalidContentLayout {
                    message: "quarantine length changed before inline binding".into(),
                });
            }
            if let Some(manifest) = self.try_write_inline_binding(object_id, version, &bytes)? {
                remove_file_if_exists(blob.synced_file.path())?;
                return Ok(manifest);
            }
        }
        self.ensure_content_reservation(object_id, version, &blob.content_hash)?;
        self.publish_prehashed_object_version_file(
            object_id,
            version,
            blob.content_hash.clone(),
            blob.size_bytes,
            &blob.synced_file,
        )
    }

    /// Stores one already-materialized DFS value after validating its expected
    /// application digest. Eligible small values use the existing inline binding
    /// durability boundary without first persisting an anonymous quarantine.
    #[tracing::instrument(
        target = "sinomo.perf",
        level = "trace",
        name = "dfs.content.ingest",
        skip_all,
        fields(stage = "dfs.content.ingest")
    )]
    pub fn put_object_version_with_expected_hash(
        &self,
        object_id: &str,
        version: &str,
        expected_hash: &str,
        bytes: &[u8],
    ) -> Result<ContentManifest> {
        self.prepare_version_if_missing(object_id, version)?;
        if bytes.len() as u64 <= INLINE_BLOB_SMALL_LIMIT_BYTES
            && self.inline_sentinel_unlocked()?.is_some()
        {
            // These caller-owned bytes are already complete. Verify before any
            // reservation, manifest, or content mutation; the inline writer
            // still owns quota, tuple conflicts, GC fencing and durable append
            // certification. The public quarantined capability stays durable.
            ensure_bytes_match_hash(expected_hash, bytes)?;
            if let Some(manifest) = self.try_write_inline_binding(object_id, version, bytes)? {
                return Ok(manifest);
            }
        }
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
            .any(|(_, _, bytes)| bytes.len() as u64 > INLINE_BLOB_SMALL_LIMIT_BYTES)
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
        versions
            .into_iter()
            .map(|(object, version, bytes)| {
                self.write_object_version_blob_only(object, version, bytes)
            })
            .collect()
    }

    #[tracing::instrument(
        target = "sinomo.perf",
        level = "trace",
        name = "dfs.content.blob",
        skip_all,
        fields(stage = "dfs.content.blob")
    )]
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
        self.prepare_version_if_missing(object_id, version)?;
        if let Some(manifest) =
            self.try_publish_inline_staged_manifest(object_id, version, content_hash, size_bytes)?
        {
            return Ok(manifest);
        }
        let reader = self.open_blob_stream(content_hash)?;
        if reader.size_bytes() != size_bytes {
            return Err(ContentStoreError::InvalidContentLayout {
                message: format!(
                    "blob-only manifest size does not match blob: object={object_id}, version={version}, manifest={size_bytes}, blob={}",
                    reader.size_bytes()
                ),
            });
        }
        self.publish_existing_manifest(ContentManifest {
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
            capture_limit_bytes.max(INLINE_BLOB_SMALL_LIMIT_BYTES as usize);

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
        if streamed.size_bytes <= INLINE_BLOB_SMALL_LIMIT_BYTES {
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
        if hash == intrinsic_empty_content_hash() {
            return Ok(Vec::new());
        }
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
        if bytes.len() as u64 != reader.size_bytes() {
            return Err(ContentStoreError::InvalidContentLayout {
                message: "blob read ended before its declared size".to_string(),
            });
        }
        Ok(bytes)
    }

    pub fn read_blob_range(&self, hash: &str, offset: u64, max_bytes: usize) -> Result<Vec<u8>> {
        if hash == intrinsic_empty_content_hash() {
            return Ok(Vec::new());
        }
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

    /// Opens a blob without rehashing stored bytes. Scrub owns content verification.
    pub fn open_blob_reader(&self, hash: &str) -> Result<BlobReader> {
        self.open_blob_stream(hash)
    }

    /// Opens a blob for a single read pass, validating its stored length only.
    /// Content digests are checked by scrub, not by foreground reads.
    pub fn open_blob_stream(&self, hash: &str) -> Result<BlobReader> {
        self.prepare_blob_if_missing(hash)?;
        self.open_current_blob_stream(hash)
    }

    fn open_current_blob_stream(&self, hash: &str) -> Result<BlobReader> {
        let _owner = self.content_mutation_guard()?;
        #[cfg(test)]
        self.blob_stream_open_count.fetch_add(1, Ordering::Relaxed);
        if hash == intrinsic_empty_content_hash() {
            return Ok(BlobReader::empty().retain_store(self.inline_store.clone()));
        }
        if let Some((file, size, remaining, generation)) =
            self.open_inline_blob(hash, 0, usize::MAX)?
        {
            return Ok(BlobReader::with_remaining(file, size, remaining)
                .retain_inline_generation(generation));
        }
        if self.inline_blob_deleted(hash)? {
            return Err(ContentStoreError::Io {
                path: self.blob_path(hash)?,
                source: io::Error::new(
                    io::ErrorKind::NotFound,
                    "content hash was durably collected",
                ),
            });
        }
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
            return Ok(BlobReader::new(file, size_bytes).retain_store(self.inline_store.clone()));
        }
        Err(ContentStoreError::Io {
            path,
            source: io::Error::new(io::ErrorKind::NotFound, "blob not found"),
        })
    }

    /// Opens a bounded range of a content-addressed blob for streaming.
    ///
    /// This has the same trust model as [`Self::open_blob_stream`]: the path is
    /// derived from the validated content identity. Scrub verifies stored bytes;
    /// ordinary reads use bounded memory without a full-blob verification pass.
    pub fn open_blob_range_stream(
        &self,
        hash: &str,
        offset: u64,
        max_bytes: usize,
    ) -> Result<BlobRangeReader> {
        self.prepare_blob_if_missing(hash)?;
        self.open_current_blob_range_stream(hash, offset, max_bytes)
    }

    fn open_current_blob_range_stream(
        &self,
        hash: &str,
        offset: u64,
        max_bytes: usize,
    ) -> Result<BlobRangeReader> {
        let _owner = self.content_mutation_guard()?;
        #[cfg(test)]
        self.blob_stream_open_count.fetch_add(1, Ordering::Relaxed);
        if hash == intrinsic_empty_content_hash() {
            return Ok(BlobRangeReader::empty().retain_store(self.inline_store.clone()));
        }
        if let Some((file, size, remaining, generation)) =
            self.open_inline_blob(hash, offset, max_bytes)?
        {
            return Ok(
                BlobRangeReader::new(file, offset.min(size), size, remaining)
                    .retain_inline_generation(generation),
            );
        }
        if self.inline_blob_deleted(hash)? {
            return Err(ContentStoreError::Io {
                path: self.blob_path(hash)?,
                source: io::Error::new(
                    io::ErrorKind::NotFound,
                    "content hash was durably collected",
                ),
            });
        }
        let path = self.blob_path(hash)?;
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
        Ok(
            BlobRangeReader::new(file, start, size_bytes, remaining_bytes)
                .retain_store(self.inline_store.clone()),
        )
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
        if bytes.len() as u64 != manifest.size_bytes {
            return Err(ContentStoreError::InvalidContentLayout {
                message: "object read ended before its manifest size".to_string(),
            });
        }
        Ok((manifest, bytes))
    }

    pub fn open_object_version_stream(
        &self,
        object_id: &str,
        version: &str,
    ) -> Result<(ContentManifest, ObjectVersionReader)> {
        self.prepare_version_if_missing(object_id, version)?;
        let _owner = self.content_mutation_guard()?;
        let manifest = self.read_current_manifest(object_id, version)?;
        if manifest.extents.is_empty() {
            let reader = self.open_current_blob_stream(&manifest.content_hash)?;
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
        let snapshot =
            self.capture_read_snapshot(extents.iter().map(|extent| extent.content_hash.as_str()))?;
        for extent in &extents {
            snapshot.require_range(&extent.content_hash, extent.blob_offset, extent.size_bytes)?;
        }
        let reader = ObjectVersionReader::from_extents(snapshot, manifest.size_bytes, extents);
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
        let _owner = self.content_mutation_guard()?;
        let manifest_fields = &manifest.manifest;
        let start = offset.min(manifest_fields.size_bytes);
        let max_bytes_u64 = u64::try_from(max_bytes).unwrap_or(u64::MAX);
        let end = start
            .saturating_add(max_bytes_u64)
            .min(manifest_fields.size_bytes);
        if manifest_fields.extents.is_empty() {
            let reader =
                self.open_current_blob_range_stream(&manifest_fields.content_hash, start, max_bytes)?;
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
        let snapshot = self.capture_read_snapshot(
            manifest_fields.extents[first_extent_index..end_extent_index]
                .iter()
                .map(|extent| extent.content_hash.as_str()),
        )?;
        for extent in &manifest_fields.extents[first_extent_index..end_extent_index] {
            let extent_end = extent.logical_offset.saturating_add(extent.size_bytes);
            let range_start = start.max(extent.logical_offset);
            let range_end = end.min(extent_end);
            let blob_start = extent
                .blob_offset
                .checked_add(range_start - extent.logical_offset)
                .ok_or_else(|| ContentStoreError::InvalidContentLayout {
                    message: "extent blob offset overflows".into(),
                })?;
            snapshot.require_range(&extent.content_hash, blob_start, range_end - range_start)?;
        }
        Ok(ObjectVersionReader::from_manifest_range(
            snapshot,
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
            if manifest.content_hash == intrinsic_empty_content_hash() {
                return Ok(manifest.size_bytes == 0);
            }
            return Ok(
                self.readable_blob_size(&manifest.content_hash)? == Some(manifest.size_bytes)
            );
        }
        let extents = self.logical_extents_for_manifest(manifest)?;
        self.extents_readable(&extents)
    }

    pub fn read_manifest(&self, object_id: &str, version: &str) -> Result<ContentManifest> {
        self.prepare_version_if_missing(object_id, version)?;
        self.read_current_manifest(object_id, version)
    }

    fn read_current_manifest(&self, object_id: &str, version: &str) -> Result<ContentManifest> {
        let _owner = self.content_mutation_guard()?;
        match self.inline_version_state(object_id, version)? {
            Some(InlineVersionState::Live(entry)) => Ok(entry.manifest),
            Some(InlineVersionState::Deleted { .. }) | None => Err(ContentStoreError::Io {
                path: self.manifest_path(object_id, version),
                source: io::Error::new(io::ErrorKind::NotFound, "content version is not live"),
            }),
        }
    }

    fn read_stream_manifest(
        &self,
        object_id: &str,
        version: &str,
    ) -> Result<Arc<ValidatedContentManifest>> {
        self.prepare_version_if_missing(object_id, version)?;
        let _owner = self.content_mutation_guard()?;
        let manifest = self.read_current_manifest(object_id, version)?;
        let path = self.manifest_path(object_id, version);
        if let Some(cached) = self
            .stream_manifest_cache
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&path)
        {
            return Ok(cached);
        }
        #[cfg(test)]
        self.stream_manifest_validation_count
            .fetch_add(1, Ordering::Relaxed);
        self.cache_stream_manifest(manifest)
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
        self.prepare_content_object_inventory(object_id)?;
        let _owner = self.content_mutation_guard()?;
        let mut manifests = Vec::new();
        if let Some(view) = self.inline_binding_view()? {
            for ((object, _), state) in view.versions {
                if object == object_id
                    && let InlineVersionState::Live(entry) = state
                {
                    manifests.push(entry.manifest);
                }
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
        if let Some(current) = self.inline_manifest_for_content(object_id, content_hash, size_bytes)?
        {
            return Ok(Some(current));
        }
        self.prepare_content_object_inventory(object_id)?;
        self.inline_manifest_for_content(object_id, content_hash, size_bytes)
    }

    pub fn scrub(&self) -> Result<ContentStoreScrubReport> {
        self.scrub_with_stats_snapshot(true)
    }

    /// Verifies current catalog and content without publishing derived statistics.
    pub fn inspect_integrity(&self) -> Result<ContentStoreScrubReport> {
        self.scrub_with_stats_snapshot(false)
    }

    fn scrub_with_stats_snapshot(
        &self,
        persist_stats_snapshot: bool,
    ) -> Result<ContentStoreScrubReport> {
        self.scrub_with_stats_snapshot_and_version_entries(persist_stats_snapshot, None)
    }

    fn scrub_with_stats_snapshot_and_version_entries(
        &self,
        persist_stats_snapshot: bool,
        mut version_entries: Option<&mut BTreeMap<String, Vec<VersionManifestEntry>>>,
    ) -> Result<ContentStoreScrubReport> {
        let _owner = self.content_mutation_guard()?;
        let inline = self.inline_binding_view_verified()?;
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
        if let Some(view) = inline.as_ref() {
            for ((object, version), state) in &view.versions {
                let InlineVersionState::Live(entry) = state else {
                    continue;
                };
                let manifest = &entry.manifest;
                let encoded_bytes = encode_manifest(manifest)?.len() as u64;
                report.stats.manifest_count = report.stats.manifest_count.saturating_add(1);
                report.stats.manifest_bytes =
                    report.stats.manifest_bytes.saturating_add(encoded_bytes);
                let diagnostic_path = self
                    .root
                    .join(inline_bindings::INLINE_BINDING_DIR)
                    .join("current.sobj");
                self.scrub_manifest_references(
                    &diagnostic_path,
                    manifest,
                    &mut report,
                    &mut referenced_blobs,
                    &mut checked_blobs,
                )?;
                if let Some(entries) = version_entries.as_deref_mut() {
                    entries
                        .entry(object.clone())
                        .or_default()
                        .push(VersionManifestEntry {
                            object_id: object.clone(),
                            version: version.clone(),
                            content_hash: manifest.content_hash.clone(),
                            modified_nanos: entry.published_at_unix_nanos,
                            manifest_bytes: encoded_bytes,
                            logical_bytes: manifest.size_bytes,
                            extent_count: 0,
                        });
                }
            }
            for (hash, entry) in &view.blobs {
                if !referenced_blobs.contains(hash) {
                    report.orphan_blob_count = report.orphan_blob_count.saturating_add(1);
                    report.orphan_blob_bytes =
                        report.orphan_blob_bytes.saturating_add(entry.size_bytes);
                    report
                        .issues
                        .push(ContentStoreScrubIssue::OrphanInlineBlob {
                            content_hash: hash.clone(),
                            path: entry.path().to_owned(),
                            size_bytes: entry.size_bytes,
                        });
                }
            }
        }
        let shadowed_hashes = inline
            .as_ref()
            .map(|view| {
                view.blobs
                    .keys()
                    .cloned()
                    .chain(view.deleted_blobs.iter().cloned())
                    .collect()
            })
            .unwrap_or_default();
        self.collect_orphan_blob_issues(
            &self.root.join("blobs"),
            &referenced_blobs,
            &shadowed_hashes,
            &mut report,
        )?;
        report.referenced_blob_count = referenced_blobs.len() as u64;
        report.referenced_blob_bytes = referenced_blobs
            .iter()
            .filter_map(|hash| checked_blobs.get(hash))
            .map(|blob| blob.size_bytes)
            .fold(0_u64, |total, size| total.saturating_add(size));
        if persist_stats_snapshot {
            self.replace_stats_snapshot(&report.stats)?;
        }
        Ok(report)
    }

    pub fn collect_garbage(&self, options: ContentStoreGcOptions) -> Result<ContentStoreGcReport> {
        self.require_complete_content_inventory()?;
        let _owner = self.content_mutation_guard()?;
        self.require_complete_inventory_under_owner()?;
        let catalog_observation = options
            .catalog_generation
            .as_deref()
            .filter(|generation| !generation.is_empty())
            .zip(
                options
                    .catalog_observed_at_unix_nanos
                    .filter(|observed_at| *observed_at > 0),
            );
        if !options.dry_run
            && (catalog_observation.is_none() || options.destructive_catalog_seal.is_none())
        {
            return Err(ContentStoreError::InvalidContentLayout {
                message:
                    "destructive orphan GC requires an authorized complete local catalog observation"
                        .to_owned(),
            });
        }
        let scrub = self.scrub_with_stats_snapshot(!options.dry_run)?;
        let reservation_roots = self.content_reservation_roots()?;
        let mut protected_content_hashes = options.protected_content_hashes.clone();
        protected_content_hashes.extend(reservation_roots.content_hashes);
        let blocked_by_scrub_issue_count = scrub
            .issues
            .iter()
            .filter(|issue| {
                !matches!(
                    issue,
                    ContentStoreScrubIssue::OrphanBlob { .. }
                        | ContentStoreScrubIssue::OrphanInlineBlob { .. }
                )
            })
            .count() as u64;
        let mut report = ContentStoreGcReport {
            scrub,
            dry_run: options.dry_run,
            min_orphan_age_nanos: options.min_orphan_age_nanos,
            catalog_generation: options.catalog_generation.clone(),
            catalog_observed_at_unix_nanos: options.catalog_observed_at_unix_nanos,
            max_deletions: options.max_deletions,
            blocked_by_scrub_issue_count,
            ..ContentStoreGcReport::default()
        };
        if blocked_by_scrub_issue_count != 0 {
            return Ok(report);
        }

        let observed_at_unix_nanos = system_time_nanos_since_epoch_saturating(SystemTime::now());
        let orphan_blobs = report
            .scrub
            .issues
            .iter()
            .filter_map(|issue| match issue {
                ContentStoreScrubIssue::OrphanBlob {
                    content_hash,
                    path,
                    size_bytes,
                    ..
                } => Some(OrphanBlobGcCandidate::Loose {
                    content_hash: content_hash.clone(),
                    path: path.clone(),
                    size_bytes: *size_bytes,
                }),
                ContentStoreScrubIssue::OrphanInlineBlob {
                    content_hash,
                    size_bytes,
                    ..
                } => Some(OrphanBlobGcCandidate::Inline {
                    content_hash: content_hash.clone(),
                    size_bytes: *size_bytes,
                }),
                _ => None,
            })
            .collect::<Vec<_>>();
        let observed_orphan_hashes = orphan_blobs
            .iter()
            .filter(|orphan| !protected_content_hashes.contains(orphan.content_hash()))
            .map(|orphan| orphan.content_hash().to_owned())
            .collect::<BTreeSet<_>>();
        let max_durable_work = options.max_deletions.unwrap_or(u64::MAX);
        let mut durable_work_count = 0_u64;
        let mut stats_delta = ContentStoreStatsDelta::default();
        let mut dirty_orphan_observation_directories = BTreeSet::<PathBuf>::new();
        let mut orphan_observation_root_created = false;
        let mut dirty_loose_blob_directories = BTreeSet::<PathBuf>::new();
        let file_reclamation = if options.dry_run {
            None
        } else {
            self.try_lock_file_reclamation()?
        };
        for stale in self.orphan_observation_gc_candidates(&observed_orphan_hashes)? {
            report.stale_orphan_observation_count =
                report.stale_orphan_observation_count.saturating_add(1);
            report.stale_orphan_observation_bytes = report
                .stale_orphan_observation_bytes
                .saturating_add(stale.size_bytes);
            if durable_work_count >= max_durable_work {
                report.max_deletions_reached = true;
                report.retained_limit_orphan_observation_count = report
                    .retained_limit_orphan_observation_count
                    .saturating_add(1);
                report.retained_limit_orphan_observation_bytes = report
                    .retained_limit_orphan_observation_bytes
                    .saturating_add(stale.size_bytes);
                continue;
            }
            if options.dry_run {
                durable_work_count = durable_work_count.saturating_add(1);
                continue;
            }
            if remove_file_if_exists_without_sync(&stale.path)? {
                if let Some(parent) = stale.path.parent() {
                    dirty_orphan_observation_directories.insert(parent.to_owned());
                }
                durable_work_count = durable_work_count.saturating_add(1);
                report.deleted_stale_orphan_observation_count = report
                    .deleted_stale_orphan_observation_count
                    .saturating_add(1);
                report.deleted_stale_orphan_observation_bytes = report
                    .deleted_stale_orphan_observation_bytes
                    .saturating_add(stale.size_bytes);
            }
        }
        for orphan in orphan_blobs {
            let size_bytes = orphan.size_bytes();
            if protected_content_hashes.contains(orphan.content_hash()) {
                report.retained_protected_orphan_count =
                    report.retained_protected_orphan_count.saturating_add(1);
                report.retained_protected_orphan_bytes = report
                    .retained_protected_orphan_bytes
                    .saturating_add(size_bytes);
                continue;
            }
            let (mut observation, newly_observed) = self.read_orphan_observation_or_fresh(
                orphan.content_hash(),
                observed_at_unix_nanos,
                catalog_observation,
            )?;
            if newly_observed {
                let observation_size_bytes = self.encoded_orphan_observation_size(
                    orphan.content_hash(),
                    observed_at_unix_nanos,
                    catalog_observation,
                )?;
                report.newly_observed_orphan_count =
                    report.newly_observed_orphan_count.saturating_add(1);
                report.newly_observed_orphan_bytes = report
                    .newly_observed_orphan_bytes
                    .saturating_add(size_bytes);
                if durable_work_count >= max_durable_work {
                    report.max_deletions_reached = true;
                    report.retained_limit_orphan_observation_count = report
                        .retained_limit_orphan_observation_count
                        .saturating_add(1);
                    report.retained_limit_orphan_observation_bytes = report
                        .retained_limit_orphan_observation_bytes
                        .saturating_add(observation_size_bytes);
                } else if options.dry_run {
                    durable_work_count = durable_work_count.saturating_add(1);
                } else {
                    let (catalog_generation, catalog_observed_at_unix_nanos) =
                        catalog_observation.expect("destructive GC validated catalog observation");
                    let persisted = self.persist_orphan_observation_without_sync(
                        orphan.content_hash(),
                        observed_at_unix_nanos,
                        catalog_generation,
                        catalog_observed_at_unix_nanos,
                    )?;
                    observation = persisted.observation;
                    if persisted.created {
                        dirty_orphan_observation_directories.insert(persisted.parent);
                        orphan_observation_root_created |= persisted.root_created;
                        durable_work_count = durable_work_count.saturating_add(1);
                        report.persisted_orphan_observation_count =
                            report.persisted_orphan_observation_count.saturating_add(1);
                        report.persisted_orphan_observation_bytes = report
                            .persisted_orphan_observation_bytes
                            .saturating_add(persisted.size_bytes);
                    }
                }
            }
            let eligible_at_unix_nanos = observation
                .first_observed_at_unix_nanos
                .saturating_add(options.min_orphan_age_nanos);
            if newly_observed || observed_at_unix_nanos < eligible_at_unix_nanos {
                report.retained_young_orphan_count =
                    report.retained_young_orphan_count.saturating_add(1);
                report.retained_young_orphan_bytes = report
                    .retained_young_orphan_bytes
                    .saturating_add(size_bytes);
                report.next_eligible_at_unix_nanos = Some(
                    report
                        .next_eligible_at_unix_nanos
                        .map(|current| current.min(eligible_at_unix_nanos))
                        .unwrap_or(eligible_at_unix_nanos),
                );
                continue;
            }
            let Some((catalog_generation, catalog_observed_at_unix_nanos)) = catalog_observation
            else {
                report.retained_missing_catalog_observation_orphan_count = report
                    .retained_missing_catalog_observation_orphan_count
                    .saturating_add(1);
                report.retained_missing_catalog_observation_orphan_bytes = report
                    .retained_missing_catalog_observation_orphan_bytes
                    .saturating_add(size_bytes);
                continue;
            };
            if catalog_generation == observation.catalog_generation
                || catalog_observed_at_unix_nanos <= observation.catalog_observed_at_unix_nanos
                || catalog_observed_at_unix_nanos <= observation.first_observed_at_unix_nanos
            {
                report.retained_same_or_stale_catalog_orphan_count = report
                    .retained_same_or_stale_catalog_orphan_count
                    .saturating_add(1);
                report.retained_same_or_stale_catalog_orphan_bytes = report
                    .retained_same_or_stale_catalog_orphan_bytes
                    .saturating_add(size_bytes);
                continue;
            }

            report.eligible_orphan_count = report.eligible_orphan_count.saturating_add(1);
            report.eligible_orphan_bytes = report.eligible_orphan_bytes.saturating_add(size_bytes);
            if durable_work_count >= max_durable_work {
                report.max_deletions_reached = true;
                report.retained_limit_orphan_count =
                    report.retained_limit_orphan_count.saturating_add(1);
                report.retained_limit_orphan_bytes = report
                    .retained_limit_orphan_bytes
                    .saturating_add(size_bytes);
                continue;
            }
            if options.dry_run {
                durable_work_count = durable_work_count.saturating_add(1);
                continue;
            }
            if !matches!(&orphan, OrphanBlobGcCandidate::Inline { .. })
                && file_reclamation.is_none()
            {
                // A current lazy reader owns immutable loose-blob locators.
                // Retain their physical files until that reader drains.
                continue;
            }
            match orphan {
                OrphanBlobGcCandidate::Loose {
                    content_hash, path, ..
                } => {
                    let before_len = file_len_if_exists(&path)?;
                    if remove_file_if_exists_without_sync(&path)? {
                        if let Some(parent) = path.parent() {
                            dirty_loose_blob_directories.insert(parent.to_owned());
                        }
                        report.deleted_orphan_count = report.deleted_orphan_count.saturating_add(1);
                        report.deleted_orphan_bytes =
                            report.deleted_orphan_bytes.saturating_add(size_bytes);
                        durable_work_count = durable_work_count.saturating_add(1);
                        stats_delta.add(ContentStoreStatsDelta::blob_file_change(before_len, None));
                        if let Some(parent) =
                            self.remove_orphan_observation_without_sync(&content_hash)?
                        {
                            dirty_orphan_observation_directories.insert(parent);
                        }
                    }
                }
                OrphanBlobGcCandidate::Inline { content_hash, .. } => {
                    if self.inline_remove_blob_unlocked(&content_hash)? {
                        report.deleted_orphan_count = report.deleted_orphan_count.saturating_add(1);
                        report.deleted_orphan_bytes =
                            report.deleted_orphan_bytes.saturating_add(size_bytes);
                        durable_work_count = durable_work_count.saturating_add(1);
                        stats_delta.add(ContentStoreStatsDelta {
                            blob_count: -1,
                            blob_bytes: -i128::from(size_bytes),
                            ..ContentStoreStatsDelta::default()
                        });
                        if let Some(parent) =
                            self.remove_orphan_observation_without_sync(&content_hash)?
                        {
                            dirty_orphan_observation_directories.insert(parent);
                        }
                    }
                }
            }
        }
        for directory in dirty_loose_blob_directories {
            sync_dir(&directory)?;
            report.loose_blob_directory_sync_count =
                report.loose_blob_directory_sync_count.saturating_add(1);
        }
        report.orphan_observation_directory_sync_count = self.sync_orphan_observation_directories(
            &dirty_orphan_observation_directories,
            orphan_observation_root_created,
        )?;
        if !stats_delta.is_zero() && self.tracks_stats_deltas() {
            self.apply_stats_delta(stats_delta)?;
            report.stats_delta_persist_count = 1;
        }
        drop(file_reclamation);
        let max_compactions =
            u64::from(durable_work_count < max_durable_work && !report.max_deletions_reached);
        let inline = self.advance_inline_compaction(options.dry_run, max_compactions != 0)?;
        report.pending_inline_compaction_count = inline.pending;
        report.copied_inline_compaction_records = inline.copied_records;
        report.advanced_inline_compaction_steps = inline.advanced_steps;
        report.reclaimed_inline_generation_bytes = inline.reclaimed_bytes;
        Ok(report)
    }

    pub fn collect_stale_staging(
        &self,
        options: ContentStoreStagingGcOptions,
    ) -> Result<ContentStoreStagingGcReport> {
        let _owner = self.content_mutation_guard()?;
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
        self.require_complete_content_inventory()?;
        let _owner = self.content_mutation_guard()?;
        self.require_complete_inventory_under_owner()?;
        let destructive_catalog_observation_is_valid = options
            .catalog_generation
            .as_deref()
            .is_some_and(|generation| !generation.is_empty())
            && options
                .catalog_generated_at_unix_nanos
                .is_some_and(|observed_at| observed_at > 0);
        if !options.dry_run
            && (!destructive_catalog_observation_is_valid
                || options.destructive_catalog_seal.is_none())
        {
            return Err(ContentStoreError::InvalidContentLayout {
                message:
                    "destructive version GC requires an authorized complete local catalog observation"
                        .to_owned(),
            });
        }
        let mut versions_by_object = BTreeMap::<String, Vec<VersionManifestEntry>>::new();
        let scrub = self.scrub_with_stats_snapshot_and_version_entries(
            !options.dry_run,
            Some(&mut versions_by_object),
        )?;
        let blocked_by_scrub_issue_count = scrub
            .issues
            .iter()
            .filter(|issue| {
                !matches!(
                    issue,
                    ContentStoreScrubIssue::OrphanBlob { .. }
                        | ContentStoreScrubIssue::OrphanInlineBlob { .. }
                )
            })
            .count() as u64;
        let keep_latest_versions_per_object = options.keep_latest_versions_per_object;
        let mut report = ContentStoreVersionGcReport {
            scrub,
            dry_run: options.dry_run,
            min_version_age_nanos: options.min_version_age_nanos,
            min_absence_age_nanos: options.min_absence_age_nanos,
            catalog_generation: options.catalog_generation.clone(),
            catalog_generated_at_unix_nanos: options.catalog_generated_at_unix_nanos,
            keep_latest_versions_per_object,
            max_deletions: options.max_deletions,
            blocked_by_scrub_issue_count,
            ..ContentStoreVersionGcReport::default()
        };
        if blocked_by_scrub_issue_count != 0 {
            return Ok(report);
        }

        report.scanned_object_count = versions_by_object.len() as u64;
        report.scanned_version_count = versions_by_object
            .values()
            .map(|entries| entries.len() as u64)
            .fold(0_u64, |total, len| total.saturating_add(len));

        let reservation_roots = self.content_reservation_roots()?;
        let mut protected_versions = options.protected_versions.clone();
        protected_versions.extend(reservation_roots.versions);
        let mut protected_versions_by_object = BTreeMap::<String, BTreeSet<String>>::new();
        for (object_id, version) in protected_versions {
            protected_versions_by_object
                .entry(object_id)
                .or_default()
                .insert(version);
        }
        let now = SystemTime::now();
        let observed_at_unix_nanos = system_time_nanos_since_epoch_saturating(now);
        let catalog_observation = options
            .catalog_generation
            .as_deref()
            .filter(|generation| !generation.is_empty())
            .zip(
                options
                    .catalog_generated_at_unix_nanos
                    .filter(|generated_at| *generated_at > 0),
            );
        let keep_latest_limit =
            usize::try_from(keep_latest_versions_per_object).unwrap_or(usize::MAX);
        let max_durable_work = options.max_deletions.unwrap_or(u64::MAX);
        let mut durable_work_count = 0_u64;
        let mut deletion_batch = Vec::with_capacity(VERSION_GC_DELETE_BATCH_SIZE);
        let mut dirty_deletion_directories = BTreeSet::new();
        let mut stats_delta = ContentStoreStatsDelta::default();
        for entries in versions_by_object.values_mut() {
            entries.sort_by(|left, right| {
                right
                    .modified_nanos
                    .cmp(&left.modified_nanos)
                    .then_with(|| right.version.cmp(&left.version))
            });
            for (index, entry) in entries.iter().enumerate() {
                if protected_versions_by_object
                    .get(entry.object_id.as_str())
                    .is_some_and(|versions| versions.contains(entry.version.as_str()))
                {
                    if !options.dry_run {
                        self.remove_manifest_absence_observation(
                            &content_reservation_identity_for_entry(entry),
                        )?;
                    }
                    report.retained_protected_version_count =
                        report.retained_protected_version_count.saturating_add(1);
                    report.retained_protected_logical_bytes = report
                        .retained_protected_logical_bytes
                        .saturating_add(entry.logical_bytes);
                    continue;
                }
                if index < keep_latest_limit {
                    report.retained_latest_version_count =
                        report.retained_latest_version_count.saturating_add(1);
                    report.retained_latest_logical_bytes = report
                        .retained_latest_logical_bytes
                        .saturating_add(entry.logical_bytes);
                    continue;
                }

                let age_nanos = observed_at_unix_nanos.saturating_sub(entry.modified_nanos);
                if age_nanos < options.min_version_age_nanos {
                    report.retained_young_version_count =
                        report.retained_young_version_count.saturating_add(1);
                    report.retained_young_logical_bytes = report
                        .retained_young_logical_bytes
                        .saturating_add(entry.logical_bytes);
                    let eligible_at_unix_nanos = entry
                        .modified_nanos
                        .saturating_add(options.min_version_age_nanos);
                    report.next_eligible_at_unix_nanos = Some(
                        report
                            .next_eligible_at_unix_nanos
                            .map(|current| current.min(eligible_at_unix_nanos))
                            .unwrap_or(eligible_at_unix_nanos),
                    );
                    continue;
                }

                report.eligible_version_count = report.eligible_version_count.saturating_add(1);
                report.eligible_logical_bytes = report
                    .eligible_logical_bytes
                    .saturating_add(entry.logical_bytes);
                report.eligible_manifest_bytes = report
                    .eligible_manifest_bytes
                    .saturating_add(entry.manifest_bytes);
                let absence_observation = self.read_manifest_absence_observation(entry)?;
                if absence_observation.is_none() {
                    report.newly_observed_absent_version_count =
                        report.newly_observed_absent_version_count.saturating_add(1);
                    report.newly_observed_absent_logical_bytes = report
                        .newly_observed_absent_logical_bytes
                        .saturating_add(entry.logical_bytes);
                    report.retained_absence_grace_version_count = report
                        .retained_absence_grace_version_count
                        .saturating_add(1);
                    report.retained_absence_grace_logical_bytes = report
                        .retained_absence_grace_logical_bytes
                        .saturating_add(entry.logical_bytes);
                    let Some((catalog_generation, catalog_generated_at_unix_nanos)) =
                        catalog_observation
                    else {
                        report.retained_missing_catalog_observation_version_count = report
                            .retained_missing_catalog_observation_version_count
                            .saturating_add(1);
                        report.retained_missing_catalog_observation_logical_bytes = report
                            .retained_missing_catalog_observation_logical_bytes
                            .saturating_add(entry.logical_bytes);
                        continue;
                    };
                    let absence_eligible_at_unix_nanos =
                        observed_at_unix_nanos.saturating_add(options.min_absence_age_nanos);
                    report.next_eligible_at_unix_nanos = Some(
                        report
                            .next_eligible_at_unix_nanos
                            .map(|current| current.min(absence_eligible_at_unix_nanos))
                            .unwrap_or(absence_eligible_at_unix_nanos),
                    );
                    if durable_work_count >= max_durable_work {
                        report.max_deletions_reached = true;
                        report.retained_limit_version_count =
                            report.retained_limit_version_count.saturating_add(1);
                        report.retained_limit_logical_bytes = report
                            .retained_limit_logical_bytes
                            .saturating_add(entry.logical_bytes);
                    } else if options.dry_run {
                        durable_work_count = durable_work_count.saturating_add(1);
                    } else {
                        self.persist_manifest_absence_observation(
                            entry,
                            observed_at_unix_nanos,
                            catalog_generation,
                            catalog_generated_at_unix_nanos,
                        )?;
                        durable_work_count = durable_work_count.saturating_add(1);
                        report.persisted_absence_observation_count =
                            report.persisted_absence_observation_count.saturating_add(1);
                    }
                    continue;
                }
                let absence_observation =
                    absence_observation.expect("checked manifest absence observation");
                let Some((catalog_generation, catalog_generated_at_unix_nanos)) =
                    catalog_observation
                else {
                    report.retained_missing_catalog_observation_version_count = report
                        .retained_missing_catalog_observation_version_count
                        .saturating_add(1);
                    report.retained_missing_catalog_observation_logical_bytes = report
                        .retained_missing_catalog_observation_logical_bytes
                        .saturating_add(entry.logical_bytes);
                    continue;
                };
                if catalog_generation == absence_observation.catalog_generation
                    || catalog_generated_at_unix_nanos
                        <= absence_observation.first_observed_at_unix_nanos
                    || catalog_generated_at_unix_nanos
                        <= absence_observation.catalog_generated_at_unix_nanos
                {
                    report.retained_same_or_stale_catalog_version_count = report
                        .retained_same_or_stale_catalog_version_count
                        .saturating_add(1);
                    report.retained_same_or_stale_catalog_logical_bytes = report
                        .retained_same_or_stale_catalog_logical_bytes
                        .saturating_add(entry.logical_bytes);
                    continue;
                }
                let absence_eligible_at_unix_nanos = absence_observation
                    .first_observed_at_unix_nanos
                    .saturating_add(options.min_absence_age_nanos);
                if observed_at_unix_nanos < absence_eligible_at_unix_nanos {
                    report.retained_absence_grace_version_count = report
                        .retained_absence_grace_version_count
                        .saturating_add(1);
                    report.retained_absence_grace_logical_bytes = report
                        .retained_absence_grace_logical_bytes
                        .saturating_add(entry.logical_bytes);
                    report.next_eligible_at_unix_nanos = Some(
                        report
                            .next_eligible_at_unix_nanos
                            .map(|current| current.min(absence_eligible_at_unix_nanos))
                            .unwrap_or(absence_eligible_at_unix_nanos),
                    );
                    continue;
                }
                report.deletion_ready_version_count =
                    report.deletion_ready_version_count.saturating_add(1);
                report.deletion_ready_logical_bytes = report
                    .deletion_ready_logical_bytes
                    .saturating_add(entry.logical_bytes);

                if deletion_batch.len() >= VERSION_GC_DELETE_BATCH_SIZE {
                    self.flush_version_manifest_deletion_batch(
                        &mut deletion_batch,
                        &mut dirty_deletion_directories,
                        &mut report,
                        &mut stats_delta,
                    )?;
                }
                if durable_work_count >= max_durable_work {
                    report.max_deletions_reached = true;
                    report.retained_limit_version_count =
                        report.retained_limit_version_count.saturating_add(1);
                    report.retained_limit_logical_bytes = report
                        .retained_limit_logical_bytes
                        .saturating_add(entry.logical_bytes);
                    continue;
                }
                durable_work_count = durable_work_count.saturating_add(1);
                if options.dry_run {
                    continue;
                }
                deletion_batch.push(entry.clone());
            }
        }
        self.flush_version_manifest_deletion_batch(
            &mut deletion_batch,
            &mut dirty_deletion_directories,
            &mut report,
            &mut stats_delta,
        )?;
        for directory in dirty_deletion_directories {
            sync_dir(&directory)?;
            report.durable_directory_sync_count =
                report.durable_directory_sync_count.saturating_add(1);
        }
        if !stats_delta.is_zero() && self.tracks_stats_deltas() {
            self.apply_stats_delta(stats_delta)?;
            report.stats_delta_persist_count = 1;
        }
        Ok(report)
    }

    pub fn has_blob(&self, hash: &str) -> Result<bool> {
        let _owner = self.content_mutation_guard()?;
        if hash == intrinsic_empty_content_hash() {
            return Ok(true);
        }
        if self.inline_blob_deleted(hash)? {
            return Ok(false);
        }
        if self.inline_blob_entry(hash)?.is_some() {
            return Ok(self.open_inline_blob(hash, 0, 0)?.is_some());
        }
        let path = self.blob_path(hash)?;
        if path.is_file() {
            return Ok(file_content_hash(&path).is_ok_and(|actual| actual == hash));
        }
        Ok(false)
    }

    /// Returns whether local storage has an addressable candidate for `hash`.
    ///
    /// This is an advisory scheduling probe. It validates the hash syntax and
    /// the loose-file or packed-index bounds, but deliberately does not read or
    /// hash the blob bytes. Callers that consume the content remain responsible
    /// for end-to-end integrity validation.
    pub fn blob_storage_candidate_exists(&self, hash: &str) -> Result<bool> {
        let _owner = self.content_mutation_guard()?;
        if hash == intrinsic_empty_content_hash() {
            return Ok(true);
        }
        if self.inline_blob_deleted(hash)? {
            return Ok(false);
        }
        if let Some(entry) = self.inline_blob_entry(hash)? {
            return Ok(entry.readable_size()?.is_some());
        }
        let path = self.blob_path(hash)?;
        match fs::metadata(&path) {
            Ok(metadata) if metadata.is_file() => return Ok(true),
            Ok(_) => {}
            Err(source) if source.kind() == io::ErrorKind::NotFound => {}
            Err(source) => return Err(ContentStoreError::Io { path, source }),
        }
        Ok(false)
    }

    /// Read-only form of [`Self::blob_storage_candidate_exists`] that does not
    /// initialize the content-store directory tree.
    pub fn blob_storage_candidate_exists_at(root: &Path, hash: &str) -> Result<bool> {
        Self::open_for_inspection(root).blob_storage_candidate_exists(hash)
    }

    /// Reports explicit retirement in the current native binding ledger.
    /// Absence is not retirement, byte availability, or permission to bind.
    pub fn object_version_is_deleted(&self, object_id: &str, version: &str) -> Result<bool> {
        let _owner = self.content_mutation_guard()?;
        Ok(matches!(
            self.inline_version_state(object_id, version)?,
            Some(InlineVersionState::Deleted { .. })
        ))
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
        self.prepare_version_if_missing(object_id, version)?;
        let _owner = self.content_mutation_guard()?;
        if self.object_version_is_deleted(object_id, version)? {
            return Ok(false);
        }
        let manifest = match self.read_current_manifest(object_id, version) {
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
        self.manifest_storage_candidate_available(&manifest)
    }

    /// Checks whether a validated manifest has addressable storage of the
    /// expected size without reading or hashing its payload bytes.
    ///
    /// This is suitable for bounded anti-entropy availability scans. Content
    /// consumers and explicit scrubs still perform end-to-end hash validation.
    pub(crate) fn manifest_storage_candidate_available(
        &self,
        manifest: &ContentManifest,
    ) -> Result<bool> {
        if manifest.extents.is_empty() {
            return match self.open_current_blob_stream(&manifest.content_hash) {
                Ok(reader) => Ok(reader.size_bytes() == manifest.size_bytes),
                Err(error) if content_error_is_not_found(&error) => Ok(false),
                Err(error) => Err(error),
            };
        }
        let extents = self.logical_extents_for_manifest(manifest)?;
        self.extents_readable(&extents)
    }

    /// Test-only fault injection: remove a loose content-addressed blob.
    ///
    /// Inline blobs share segment files, so this only removes file payloads.
    /// Cluster tests use payloads above the inline threshold when they
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
        self.prepare_patch_units(object_id, base_version, target_version, base_identity)?;
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
            let (hash, size) = self.write_bytes_as_blob(object_id, target_version, bytes)?;
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
        self.prepare_patch_units(object_id, base_version, target_version, base_identity)?;
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
            let (hash, size) = self.write_bytes_as_blob(object_id, target_version, bytes)?;
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
        self.prepare_patch_units(object_id, base_version, target_version, base_identity)?;
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
        self.prepare_patch_units(object_id, base_version, target_version, base_identity)?;
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
        self.ensure_content_reservation(object_id, target_version, &patch_hash)?;
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
        match self.read_current_manifest(object_id, base_version) {
            Ok(manifest) => return Ok(manifest),
            Err(error) if !content_error_is_not_found(&error) => return Err(error),
            Err(error) if base_identity.is_none() => return Err(error),
            Err(_) => {}
        }
        let base_identity = base_identity.expect("checked above");
        hash_hex(&base_identity.content_hash)?;
        let reader = self.open_current_blob_stream(&base_identity.content_hash)?;
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

    fn patch_target_extents(
        &self,
        object_id: &str,
        base_version: &str,
        target_version: &str,
        base_manifest: &ContentManifest,
        offset: u64,
        patch_hash: &Option<String>,
        patch_size: u64,
    ) -> Result<(u64, Vec<ContentExtent>)> {
        let base_extents = self.logical_extents_for_manifest(base_manifest)?;
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
        Ok((target_size, target_extents))
    }

    pub(crate) fn replay_object_version_patch_manifest(
        &self,
        object_id: &str,
        base_version: &str,
        target_version: &str,
        offset: u64,
        patch_hash: &str,
        patch_size: u64,
        base_identity: &ContentVersionIdentity,
    ) -> Result<Option<ContentManifest>> {
        self.prepare_patch_units(object_id, base_version, target_version, Some(base_identity))?;
        let _owner = self.content_mutation_guard()?;
        let target = match self.read_current_manifest(object_id, target_version) {
            Ok(target) => target,
            Err(error) if content_error_is_not_found(&error) => return Ok(None),
            Err(error) => return Err(error),
        };
        let base = self.patch_base_manifest(object_id, base_version, Some(base_identity))?;
        let (size, extents) = self.patch_target_extents(
            object_id,
            base_version,
            target_version,
            &base,
            offset,
            &Some(patch_hash.to_owned()),
            patch_size,
        )?;
        if target.size_bytes != size || target.extents != extents {
            return Err(ContentStoreError::InvalidContentLayout {
                message: "routed patch differs from the immutable target extent layout".into(),
            });
        }
        Ok(Some(target))
    }

    pub(crate) fn bind_quarantined_patch(
        &self,
        object_id: &str,
        base_version: &str,
        target_version: &str,
        offset: u64,
        blob: ContentStoreQuarantinedBlob,
        base_identity: &ContentVersionIdentity,
    ) -> Result<ContentPatchManifest> {
        self.prepare_patch_units(object_id, base_version, target_version, Some(base_identity))?;
        let _owner = self.content_mutation_guard()?;
        if blob.store_root != self.root {
            return Err(ContentStoreError::InvalidContentLayout {
                message: "quarantined patch belongs to another content root".into(),
            });
        }
        self.validate_object_version_patch_source(
            object_id,
            base_version,
            offset,
            blob.size_bytes,
            Some(base_identity),
        )?;
        self.ensure_content_reservation(object_id, target_version, &blob.content_hash)?;
        let path = self.blob_path(&blob.content_hash)?;
        if !self.has_blob(&blob.content_hash)? {
            self.ensure_quota_delta(ContentStoreQuotaDelta {
                blob_bytes: projected_file_growth(&path, blob.size_bytes)?,
                ..ContentStoreQuotaDelta::default()
            })?;
            self.publish_prehashed_temporary_blob(&blob.synced_file, &path)?;
        }
        self.put_object_version_patch_from_blob(
            object_id,
            base_version,
            target_version,
            offset,
            Some(blob.content_hash.clone()),
            blob.size_bytes,
            Some(base_identity),
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
        let _owner = self.content_mutation_guard()?;
        if let Some(trusted_content_hash) = trusted_content_hash {
            hash_hex(trusted_content_hash)?;
        }
        let base_manifest = self.patch_base_manifest(object_id, base_version, base_identity)?;
        let (target_size, target_extents) = self.patch_target_extents(
            object_id,
            base_version,
            target_version,
            &base_manifest,
            offset,
            &patch_hash,
            patch_size,
        )?;
        for content_hash in target_extents
            .iter()
            .map(|extent| extent.content_hash.as_str())
            .collect::<BTreeSet<_>>()
        {
            self.ensure_content_reservation(object_id, target_version, content_hash)?;
        }
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
        let manifest = self.publish_current_manifest(manifest)?;
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
        self.prepare_version_if_missing(object_id, version)?;
        let _owner = self.content_mutation_guard()?;
        if let Some(manifest) = self.try_write_inline_binding(object_id, version, bytes)? {
            return Ok(manifest);
        }
        let manifest = ContentManifest {
            object_id: object_id.into(),
            version: version.into(),
            content_hash: content_hash(bytes),
            size_bytes: bytes.len() as u64,
            extents: Vec::new(),
        };
        self.inline_check_manifest_conflict(&manifest)?;
        self.ensure_content_reservation(object_id, version, &manifest.content_hash)?;
        let path = self.blob_path(&manifest.content_hash)?;
        if !self.has_blob(&manifest.content_hash)? {
            self.ensure_quota_delta(ContentStoreQuotaDelta {
                blob_bytes: projected_file_growth(&path, manifest.size_bytes)?,
                ..ContentStoreQuotaDelta::default()
            })?;
            self.write_tracked_blob_bytes(&manifest.content_hash, &path, bytes)?;
        }
        self.publish_current_manifest(manifest)
    }

    fn publish_prehashed_object_version_file(
        &self,
        object_id: &str,
        version: &str,
        hash: String,
        size_bytes: u64,
        synced_file: &durable_file::SyncedPrivateStagedFile,
    ) -> Result<ContentManifest> {
        let _owner = self.content_mutation_guard()?;
        let manifest = ContentManifest {
            object_id: object_id.into(),
            version: version.into(),
            content_hash: hash,
            size_bytes,
            extents: Vec::new(),
        };
        self.inline_check_manifest_conflict(&manifest)?;
        self.ensure_content_reservation(object_id, version, &manifest.content_hash)?;
        let path = self.blob_path(&manifest.content_hash)?;
        if !self.has_blob(&manifest.content_hash)? {
            self.ensure_quota_delta(ContentStoreQuotaDelta {
                blob_bytes: projected_file_growth(&path, size_bytes)?,
                ..ContentStoreQuotaDelta::default()
            })?;
            self.publish_prehashed_temporary_blob(synced_file, &path)?;
        } else {
            remove_file_if_exists(synced_file.path())?;
        }
        self.publish_current_manifest(manifest)
    }

    fn publish_prehashed_temporary_blob(
        &self,
        synced_file: &durable_file::SyncedPrivateStagedFile,
        blob_path: &Path,
    ) -> Result<()> {
        let _owner = self.content_mutation_guard()?;
        let temporary_path = synced_file.path();
        ensure_loose_blob_parent(blob_path)?;
        let before_len = file_len_if_exists(blob_path)?;
        match durable_file::install_synced_private_staged_file(synced_file, blob_path)
            .map_err(content_durable_file_error)
        {
            Ok(()) => {
                // The durable installer consumes the staged name and syncs the
                // destination directory on successful publication.
            }
            Err(ContentStoreError::Io { source, .. })
                if source.kind() == io::ErrorKind::AlreadyExists =>
            {
                remove_file_if_exists(temporary_path)?;
            }
            Err(error) => return Err(error),
        }
        if let Some(hex) = blob_path.file_name().and_then(|name| name.to_str()) {
            let hash = format!("{HASH_PREFIX}{hex}");
            if self.inline_blob_deleted(&hash)? {
                let bytes =
                    durable_file::read_object_file_bytes(blob_path, INLINE_BLOB_SMALL_LIMIT_BYTES)
                        .map_err(content_durable_file_error)?;
                self.try_restore_inline_blob(&hash, &bytes)?;
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
        self.prepare_version_if_missing(object_id, version)?;
        let _owner = self.content_mutation_guard()?;
        if let Some(manifest) = self.try_stage_inline_blob_only(object_id, version, bytes)? {
            return Ok(manifest);
        }
        let proposed = ContentManifest {
            object_id: object_id.to_owned(),
            version: version.to_owned(),
            content_hash: content_hash(bytes),
            size_bytes: bytes.len() as u64,
            extents: Vec::new(),
        };
        self.inline_check_manifest_conflict(&proposed)?;
        if let Some(InlineVersionState::Live(entry)) =
            self.inline_version_state(object_id, version)?
        {
            self.ensure_content_reservation(object_id, version, &proposed.content_hash)?;
            let content_valid = if entry.manifest.extents.is_empty() {
                self.has_blob(&proposed.content_hash)?
            } else {
                // This current binding owns an extent layout, not a blob at
                // its whole-file hash. Validate the exact logical stream and
                // retain the original manifest without duplicating its bytes.
                let extents = self.logical_extents_for_manifest(&entry.manifest)?;
                self.hash_extents(&extents)? == proposed.content_hash
            };
            if !content_valid {
                return Err(ContentStoreError::InvalidContentLayout {
                    message: "authoritative inline binding payload is corrupt".into(),
                });
            }
            self.clear_orphan_observations_before_manifest_publish(std::iter::once(
                &entry.manifest,
            ))?;
            return Ok(entry.manifest);
        }
        let trace_started = Instant::now();
        let manifest_check_started = Instant::now();
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
        if manifest.content_hash == intrinsic_empty_content_hash() {
            content_store_perf_trace(format_args!(
                "blob-only-write object={} version={} bytes=0 existing_manifest=false intrinsic_empty=true hash_ms={} manifest_check_ms={} total_ms={}",
                object_id,
                version,
                hash_ms,
                manifest_check_ms,
                trace_started.elapsed().as_millis()
            ));
            return Ok(manifest);
        }
        self.ensure_content_reservation(
            &manifest.object_id,
            &manifest.version,
            &manifest.content_hash,
        )?;

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
                io_trace::write_all(&mut file, chunk).map_err(|source| ContentStoreError::Io {
                    path: tmp_path.clone(),
                    source,
                })?;
            }
            io_trace::sync_all(&file).map_err(|source| ContentStoreError::Io {
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
            self.ensure_content_reservation(
                &manifest.object_id,
                &manifest.version,
                &manifest.content_hash,
            )?;

            let create_dir_started = Instant::now();
            ensure_loose_blob_parent(&blob_path)?;
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
                match durable_file::install_staged_file(&tmp_path, &blob_path, false)
                    .map_err(content_durable_file_error)
                {
                    Ok(()) => {
                        // The durable installer consumed the staged file and
                        // synced the destination directory.
                    }
                    Err(ContentStoreError::Io { source, .. })
                        if source.kind() == io::ErrorKind::AlreadyExists =>
                    {
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
                    Err(error) => return Err(error),
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
        versions
            .iter()
            .map(|(object, version, bytes)| self.write_object_version(object, version, bytes))
            .collect()
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
                io_trace::write_all(&mut file, &buffer[..read]).map_err(|source| {
                    ContentStoreError::Io {
                        path: tmp_path.clone(),
                        source,
                    }
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
        self.prepare_version_if_missing(object_id, version)?;
        let _owner = self.content_mutation_guard()?;
        let manifest = ContentManifest {
            object_id: object_id.into(),
            version: version.into(),
            content_hash: hash,
            size_bytes,
            extents: Vec::new(),
        };
        self.inline_check_manifest_conflict(&manifest)?;
        self.ensure_content_reservation(object_id, version, &manifest.content_hash)?;
        let path = self.blob_path(&manifest.content_hash)?;
        if !self.has_blob(&manifest.content_hash)? {
            self.ensure_quota_delta(ContentStoreQuotaDelta {
                blob_bytes: projected_file_growth(&path, size_bytes)?,
                ..ContentStoreQuotaDelta::default()
            })?;
            self.publish_tracked_file_as_blob(
                &manifest.content_hash,
                source_path,
                &path,
                &manifest.content_hash,
            )?;
        }
        self.publish_current_manifest(manifest)
    }

    fn scrub_manifest_references(
        &self,
        path: &Path,
        manifest: &ContentManifest,
        report: &mut ContentStoreScrubReport,
        referenced_blobs: &mut BTreeSet<String>,
        checked_blobs: &mut BTreeMap<String, ScrubbedBlob>,
    ) -> Result<()> {
        report.stats.logical_object_bytes = report
            .stats
            .logical_object_bytes
            .saturating_add(manifest.size_bytes);
        if manifest.extents.is_empty() {
            return self.scrub_blob_reference(
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
            );
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
        let mut diagnostic_path = self.blob_path(content_hash)?;
        if let Some(entry) = self.inline_blob_entry(content_hash)? {
            diagnostic_path = entry.path().to_owned();
        }
        if !checked_blobs.contains_key(content_hash) {
            let mut reader = match self.open_current_blob_stream(content_hash) {
                Ok(reader) => reader,
                Err(error) => {
                    report.issues.push(ContentStoreScrubIssue::MissingBlob {
                        object_id: object_id.to_owned(),
                        version: version.to_owned(),
                        content_hash: content_hash.to_owned(),
                        path: diagnostic_path,
                    });
                    if !content_error_is_not_found(&error) {
                        report.issues.push(ContentStoreScrubIssue::InvalidManifest {
                            path: manifest_path.to_owned(),
                            message: error.to_string(),
                        });
                    }
                    return Ok(());
                }
            };
            let mut hasher = blake3::Hasher::new();
            let mut buffer = [0_u8; 64 * 1024];
            let mut actual_size = 0_u64;
            loop {
                let read = reader
                    .read(&mut buffer)
                    .map_err(|source| ContentStoreError::Io {
                        path: diagnostic_path.clone(),
                        source,
                    })?;
                if read == 0 {
                    break;
                }
                hasher.update(&buffer[..read]);
                actual_size = actual_size.saturating_add(read as u64);
            }
            checked_blobs.insert(
                content_hash.to_owned(),
                ScrubbedBlob {
                    actual_hash: format!("{HASH_PREFIX}{}", hasher.finalize().to_hex()),
                    size_bytes: actual_size,
                },
            );
        }
        let checked = checked_blobs
            .get(content_hash)
            .expect("checked blob inserted");
        if checked.actual_hash != content_hash {
            report.issues.push(ContentStoreScrubIssue::CorruptBlob {
                object_id: object_id.to_owned(),
                version: version.to_owned(),
                content_hash: content_hash.to_owned(),
                path: diagnostic_path,
                actual_hash: checked.actual_hash.clone(),
            });
        } else if require_exact_size && checked.size_bytes != size_bytes {
            report
                .issues
                .push(ContentStoreScrubIssue::BlobSizeMismatch {
                    object_id: object_id.to_owned(),
                    version: version.to_owned(),
                    content_hash: content_hash.to_owned(),
                    path: diagnostic_path,
                    expected_size_bytes: size_bytes,
                    actual_size_bytes: checked.size_bytes,
                });
        } else if blob_offset
            .checked_add(size_bytes)
            .is_none_or(|end| end > checked.size_bytes)
        {
            report
                .issues
                .push(ContentStoreScrubIssue::BlobRangeOutOfBounds {
                    object_id: object_id.to_owned(),
                    version: version.to_owned(),
                    content_hash: content_hash.to_owned(),
                    path: diagnostic_path,
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
        shadowed_hashes: &BTreeSet<String>,
        report: &mut ContentStoreScrubReport,
    ) -> Result<()> {
        if !path.exists() {
            return Ok(());
        }
        for entry in read_dir(path)? {
            let entry_path = entry.path();
            if entry_path.is_dir() {
                self.collect_orphan_blob_issues(
                    &entry_path,
                    referenced_blobs,
                    shadowed_hashes,
                    report,
                )?;
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
            if referenced_blobs.contains(&content_hash) || shadowed_hashes.contains(&content_hash) {
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

    fn readable_blob_size(&self, hash: &str) -> Result<Option<u64>> {
        let _owner = self.content_mutation_guard()?;
        if hash == intrinsic_empty_content_hash() {
            return Ok(Some(0));
        }
        if let Some(entry) = self.inline_blob_entry(hash)? {
            return entry.readable_size();
        }
        if self.inline_blob_deleted(hash)? {
            return Ok(None);
        }
        let path = self.blob_path(hash)?;
        if let Some(size_bytes) = file_len_if_exists(&path)? {
            return Ok(Some(size_bytes));
        }
        Ok(None)
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
        let _owner = self.content_mutation_guard()?;
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

    fn write_bytes_as_blob(
        &self,
        object_id: &str,
        version: &str,
        bytes: &[u8],
    ) -> Result<(String, u64)> {
        let _owner = self.content_mutation_guard()?;
        let hash = content_hash(bytes);
        self.ensure_content_reservation(object_id, version, &hash)?;
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
                self.open_current_blob_range_stream(&extent.content_hash, extent.blob_offset, max_bytes)?;
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

    pub fn publish_existing_manifest(&self, manifest: ContentManifest) -> Result<ContentManifest> {
        self.prepare_version_if_missing(&manifest.object_id, &manifest.version)?;
        self.publish_current_manifest(manifest)
    }

    fn publish_current_manifest(&self, manifest: ContentManifest) -> Result<ContentManifest> {
        let _owner = self.content_mutation_guard()?;
        self.inline_check_manifest_conflict(&manifest)?;
        if let Some(InlineVersionState::Live(entry)) =
            self.inline_version_state(&manifest.object_id, &manifest.version)?
        {
            if manifest.content_hash != intrinsic_empty_content_hash() {
                self.ensure_content_reservation(
                    &manifest.object_id,
                    &manifest.version,
                    &manifest.content_hash,
                )?;
            }
            self.clear_orphan_observations_before_manifest_publish(std::iter::once(
                &entry.manifest,
            ))?;
            return Ok(entry.manifest);
        }
        if !self.manifest_content_available(&manifest)? {
            return Err(ContentStoreError::InvalidContentLayout {
                message: "manifest content is not durable and readable".into(),
            });
        }
        if manifest.content_hash != intrinsic_empty_content_hash() {
            self.ensure_content_reservation(
                &manifest.object_id,
                &manifest.version,
                &manifest.content_hash,
            )?;
        }
        self.ensure_quota_delta(ContentStoreQuotaDelta {
            logical_object_bytes: manifest.size_bytes,
            manifest_bytes: encode_manifest(&manifest)?.len() as u64,
            ..ContentStoreQuotaDelta::default()
        })?;
        self.clear_orphan_observations_before_manifest_publish(std::iter::once(&manifest))?;
        self.inline_manifest_published(&manifest)?;
        self.quota_stats_initialized.store(false, Ordering::Release);
        if self.quota.is_some() {
            self.replace_stats_snapshot(&self.scan_stats()?)?;
        }
        Ok(manifest)
    }

    fn flush_version_manifest_deletion_batch(
        &self,
        batch: &mut Vec<VersionManifestEntry>,
        dirty_directories: &mut BTreeSet<PathBuf>,
        report: &mut ContentStoreVersionGcReport,
        total_stats_delta: &mut ContentStoreStatsDelta,
    ) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let result = self.delete_version_manifest_batch(batch, dirty_directories)?;
        batch.clear();
        if result.deleted_version_count > 0 {
            report.durable_delete_batch_count = report.durable_delete_batch_count.saturating_add(1);
        }
        report.deleted_version_count = report
            .deleted_version_count
            .saturating_add(result.deleted_version_count);
        report.deleted_logical_bytes = report
            .deleted_logical_bytes
            .saturating_add(result.deleted_logical_bytes);
        report.deleted_manifest_bytes = report
            .deleted_manifest_bytes
            .saturating_add(result.deleted_manifest_bytes);
        total_stats_delta.add(result.stats_delta);
        Ok(())
    }

    fn delete_version_manifest_batch(
        &self,
        entries: &[VersionManifestEntry],
        dirty_directories: &mut BTreeSet<PathBuf>,
    ) -> Result<VersionManifestDeletionBatchResult> {
        let mut result = VersionManifestDeletionBatchResult::default();
        let mut removed_manifest_directories = BTreeMap::<PathBuf, PathBuf>::new();
        let mut touched_object_directories = BTreeSet::<PathBuf>::new();

        for entry in entries {
            // Forget the observation epoch before unlinking the manifest. A
            // crash between these operations only forces another complete
            // absence observation and grace period.
            self.remove_manifest_absence_observation(&content_reservation_identity_for_entry(
                entry,
            ))?;
            // The log tombstone is the authority edge for mixed stores. It
            // precedes physical cleanup so a lost unlink cannot resurrect the
            // old manifest or its content-index hardlink after reopening.
            let adopted = self.inline_delete_version_unlocked(
                &entry.object_id,
                &entry.version,
                &entry.content_hash,
            )?;
            let path = self.manifest_path(&entry.object_id, &entry.version);
            let removed = remove_file_if_exists_without_sync(&path)?;
            if !adopted && !removed {
                continue;
            }
            self.evict_stream_manifest(&path);
            if removed
                && let Some(version_dir) = path.parent()
                && let Some(object_dir) = version_dir.parent()
            {
                dirty_directories.insert(version_dir.to_owned());
                removed_manifest_directories.insert(version_dir.to_owned(), object_dir.to_owned());
                touched_object_directories.insert(object_dir.to_owned());
            }
            result.deleted_version_count = result.deleted_version_count.saturating_add(1);
            result.deleted_logical_bytes = result
                .deleted_logical_bytes
                .saturating_add(entry.logical_bytes);
            result.deleted_manifest_bytes = result
                .deleted_manifest_bytes
                .saturating_add(entry.manifest_bytes);
            result
                .stats_delta
                .add(ContentStoreStatsDelta::manifest_deleted(entry));
        }

        // If the now-empty version directory can be removed, syncing its
        // object directory makes both the manifest unlink and directory
        // removal durable. A retained version directory contains sidecars, so
        // it must itself be synced once for the manifest unlink.
        for (version_dir, object_dir) in removed_manifest_directories {
            match remove_empty_dir_if_exists_without_sync(&version_dir)? {
                RelaxedDirectoryRemoval::Removed => {
                    dirty_directories.remove(&version_dir);
                    dirty_directories.insert(object_dir);
                }
                RelaxedDirectoryRemoval::Retained => {}
                RelaxedDirectoryRemoval::Missing => {
                    dirty_directories.remove(&version_dir);
                }
            }
        }

        // Most churn is many versions of one retained object. This collapses
        // all of those per-version parent fsyncs into one object-directory
        // fsync at the final durability barrier for this collection pass.
        for object_dir in touched_object_directories {
            match remove_empty_dir_if_exists_without_sync(&object_dir)? {
                RelaxedDirectoryRemoval::Removed => {
                    dirty_directories.remove(&object_dir);
                    if let Some(parent) = object_dir.parent() {
                        dirty_directories.insert(parent.to_owned());
                    }
                }
                RelaxedDirectoryRemoval::Retained => {}
                RelaxedDirectoryRemoval::Missing => {
                    dirty_directories.remove(&object_dir);
                }
            }
        }

        Ok(result)
    }

    fn persist_content_reservation(
        &self,
        identity: &ContentStoreReservationIdentity,
    ) -> Result<()> {
        let _owner = self.content_mutation_guard()?;
        let guard = self.lock_content_reservations_for_write()?;
        self.persist_content_reservation_unlocked(identity, &guard.sentinel.stamp())
    }

    fn persist_content_reservation_unlocked(
        &self,
        identity: &ContentStoreReservationIdentity,
        stamp: &ContentStoreCausalStamp,
    ) -> Result<()> {
        let sentinel = decode_content_reservation_batch_sentinel(&read_file(
            &self.content_reservation_batch_sentinel_path(),
        )?)?;
        if sentinel.stamp() != *stamp {
            return Err(ContentStoreError::InvalidContentLayout {
                message: "reservation epoch changed under its write owner".into(),
            });
        }
        let previous = match self.inline_reservation_state(identity)? {
            Some(InlineReservationState::Live(record)) => Some(record.reserved_at_unix_nanos),
            _ => None,
        };
        let record = ContentStoreReservationRecord {
            identity: identity.clone(),
            reserved_at_unix_nanos: refreshed_reservation_timestamp(previous),
            causal_stamp: stamp.clone(),
        };
        self.inline_reserve_owned_identity_unlocked(&record, &sentinel)?;
        Ok(())
    }

    fn read_content_reservations(&self) -> Result<Vec<ContentStoreReservationRecord>> {
        let _owner = self.content_mutation_guard()?;
        let _lock = self.lock_content_reservations_for_read_if_present()?;
        self.read_content_reservation_inventory_unlocked()
    }

    fn read_content_reservation_inventory_unlocked(
        &self,
    ) -> Result<Vec<ContentStoreReservationRecord>> {
        let reservations = self
            .inline_binding_view()?
            .into_iter()
            .flat_map(|view| view.reservations.into_values())
            .filter_map(|state| match state {
                InlineReservationState::Live(record) => Some(record),
                InlineReservationState::Released => None,
            })
            .collect::<Vec<_>>();
        Ok(reservations)
    }

    fn read_content_reservation_reconciliation_cursor(
        &self,
    ) -> Result<Option<ContentStoreReservationReconciliationCursor>> {
        let path = self.content_reservation_reconciliation_cursor_path();
        let bytes = match read_file(&path) {
            Ok(bytes) => bytes,
            Err(ContentStoreError::Io { source, .. })
                if source.kind() == io::ErrorKind::NotFound =>
            {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        let cursor = decode_content_reservation_reconciliation_cursor(&bytes)?;
        validate_content_reservation_reconciliation_cursor(&cursor)?;
        Ok(Some(cursor))
    }

    fn persist_content_reservation_reconciliation_cursor(
        &self,
        catalog_generation: &str,
        after: &ContentStoreReservationIdentity,
    ) -> Result<()> {
        let cursor = ContentStoreReservationReconciliationCursor {
            format_version: CONTENT_RESERVATION_RECONCILIATION_CURSOR_FORMAT_VERSION,
            catalog_generation: catalog_generation.to_owned(),
            after: after.clone(),
        };
        let bytes = encode_content_reservation_reconciliation_cursor(&cursor)?;
        write_file_atomic(
            &self.content_reservation_reconciliation_cursor_path(),
            &bytes,
        )
    }

    fn clear_content_reservation_reconciliation_cursor(&self) -> Result<bool> {
        remove_file_if_exists_and_sync_parent(
            &self.content_reservation_reconciliation_cursor_path(),
        )
    }

    fn read_manifest_absence_observation(
        &self,
        entry: &VersionManifestEntry,
    ) -> Result<Option<ManifestAbsenceObservation>> {
        let identity = content_reservation_identity_for_entry(entry);
        let path = self.manifest_absence_observation_path(&identity);
        let bytes = match read_file(&path) {
            Ok(bytes) => bytes,
            Err(ContentStoreError::Io { source, .. })
                if source.kind() == io::ErrorKind::NotFound =>
            {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        match decode_manifest_absence_observation(&bytes).and_then(|observation| {
            validate_manifest_absence_observation(&observation, &identity)?;
            Ok(observation)
        }) {
            Ok(observation) => Ok(Some(observation)),
            Err(error) => {
                // Invalid absence state must never accelerate deletion. Reset
                // its epoch on the next write pass instead.
                content_store_perf_trace(format_args!(
                    "reset invalid manifest absence observation object={} version={} path={} error={error}",
                    entry.object_id,
                    entry.version,
                    path.display()
                ));
                Ok(None)
            }
        }
    }

    fn persist_manifest_absence_observation(
        &self,
        entry: &VersionManifestEntry,
        observed_at_unix_nanos: u64,
        catalog_generation: &str,
        catalog_generated_at_unix_nanos: u64,
    ) -> Result<ManifestAbsenceObservation> {
        let identity = content_reservation_identity_for_entry(entry);
        let path = self.manifest_absence_observation_path(&identity);
        if let Some(existing) = self.read_manifest_absence_observation(entry)? {
            return Ok(existing);
        }
        let observation = ManifestAbsenceObservation {
            format_version: MANIFEST_ABSENCE_OBSERVATION_FORMAT_VERSION,
            object_id: identity.object_id,
            version: identity.version,
            content_hash: identity.content_hash,
            first_observed_at_unix_nanos: observed_at_unix_nanos,
            catalog_generation: catalog_generation.to_owned(),
            catalog_generated_at_unix_nanos,
        };
        let bytes = encode_manifest_absence_observation(&observation)?;
        let observation_root = self.root.join(MANIFEST_ABSENCE_OBSERVATION_DIR);
        let root_existed = observation_root.is_dir();
        let shard_existed = path.parent().is_some_and(Path::is_dir);
        if path.is_file() {
            // Replacing corrupt state with a fresh epoch can only retain data
            // longer, and the atomic write makes the reset durable.
            write_file_atomic(&path, &bytes)?;
        } else {
            match write_file_create_new_atomic(&path, &bytes) {
                Ok(()) => {}
                Err(ContentStoreError::Io { source, .. })
                    if source.kind() == io::ErrorKind::AlreadyExists =>
                {
                    let existing = decode_manifest_absence_observation(&read_file(&path)?)?;
                    validate_manifest_absence_observation(
                        &existing,
                        &content_reservation_identity_for_entry(entry),
                    )?;
                    return Ok(existing);
                }
                Err(error) => return Err(error),
            }
        }
        self.sync_new_sharded_record_parents(
            &observation_root,
            path.parent(),
            root_existed,
            shard_existed,
        )?;
        Ok(observation)
    }

    fn sync_new_sharded_record_parents(
        &self,
        record_root: &Path,
        shard: Option<&Path>,
        root_existed: bool,
        shard_existed: bool,
    ) -> Result<()> {
        if !shard_existed {
            sync_dir(record_root)?;
        }
        if !root_existed {
            sync_dir(&self.root)?;
        }
        if shard.is_none() {
            return Err(ContentStoreError::InvalidContentLayout {
                message: format!(
                    "sharded content record has no parent under {}",
                    record_root.display()
                ),
            });
        }
        Ok(())
    }

    fn remove_manifest_absence_observation(
        &self,
        identity: &ContentStoreReservationIdentity,
    ) -> Result<bool> {
        remove_file_if_exists_and_sync_parent(&self.manifest_absence_observation_path(identity))
    }

    fn read_orphan_observation_or_fresh(
        &self,
        content_hash: &str,
        observed_at_unix_nanos: u64,
        catalog_observation: Option<(&str, u64)>,
    ) -> Result<(OrphanObservation, bool)> {
        let fresh_observation = || OrphanObservation {
            format_version: ORPHAN_OBSERVATION_FORMAT_VERSION,
            content_hash: content_hash.to_owned(),
            first_observed_at_unix_nanos: observed_at_unix_nanos,
            catalog_generation: catalog_observation
                .map(|(generation, _)| generation.to_owned())
                .unwrap_or_default(),
            catalog_observed_at_unix_nanos: catalog_observation
                .map(|(_, observed_at)| observed_at)
                .unwrap_or(0),
        };
        let path = self.orphan_observation_path(content_hash)?;
        let bytes = match read_file(&path) {
            Ok(bytes) => bytes,
            Err(ContentStoreError::Io { source, .. })
                if source.kind() == io::ErrorKind::NotFound =>
            {
                return Ok((fresh_observation(), true));
            }
            Err(error) => return Err(error),
        };
        match decode_orphan_observation(&bytes).and_then(|observation| {
            validate_orphan_observation(&observation, content_hash)?;
            Ok(observation)
        }) {
            Ok(observation) => Ok((observation, false)),
            Err(error) => {
                // Observation state only delays deletion. Treating a torn or
                // corrupt marker as a fresh observation resets the grace
                // period, which is conservative and lets the next write pass
                // repair relaxed batched marker writes after a crash.
                content_store_perf_trace(format_args!(
                    "reset invalid orphan observation hash={content_hash} path={} error={error}",
                    path.display()
                ));
                Ok((fresh_observation(), true))
            }
        }
    }

    fn persist_orphan_observation_without_sync(
        &self,
        content_hash: &str,
        observed_at_unix_nanos: u64,
        catalog_generation: &str,
        catalog_observed_at_unix_nanos: u64,
    ) -> Result<RelaxedOrphanObservationWrite> {
        let path = self.orphan_observation_path(content_hash)?;
        let parent = path
            .parent()
            .expect("orphan observation paths always have a shard parent")
            .to_owned();
        let root = self.root.join(ORPHAN_OBSERVATION_DIR);
        let root_created = !root.is_dir();
        let (observation, existing_is_fresh) = self.read_orphan_observation_or_fresh(
            content_hash,
            observed_at_unix_nanos,
            Some((catalog_generation, catalog_observed_at_unix_nanos)),
        )?;
        if !existing_is_fresh {
            return Ok(RelaxedOrphanObservationWrite {
                observation,
                created: false,
                size_bytes: file_len_if_exists(&path)?.unwrap_or(0),
                parent,
                root_created: false,
            });
        }

        let bytes = encode_orphan_observation(&observation)?;
        if path.is_file() {
            // A corrupt marker is not useful recovery evidence. Removing it
            // and installing a fresh timestamp can only extend retention.
            remove_file_if_exists_without_sync(&path)?;
        }
        create_dir_all(parent.clone())?;
        write_files_create_new_relaxed([(path, bytes.as_slice())])?;
        Ok(RelaxedOrphanObservationWrite {
            observation,
            created: true,
            size_bytes: bytes.len() as u64,
            parent,
            root_created,
        })
    }

    fn encoded_orphan_observation_size(
        &self,
        content_hash: &str,
        observed_at_unix_nanos: u64,
        catalog_observation: Option<(&str, u64)>,
    ) -> Result<u64> {
        let observation = OrphanObservation {
            format_version: ORPHAN_OBSERVATION_FORMAT_VERSION,
            content_hash: content_hash.to_owned(),
            first_observed_at_unix_nanos: observed_at_unix_nanos,
            catalog_generation: catalog_observation
                .map(|(generation, _)| generation.to_owned())
                .unwrap_or_default(),
            catalog_observed_at_unix_nanos: catalog_observation
                .map(|(_, observed_at)| observed_at)
                .unwrap_or(0),
        };
        Ok(u64::try_from(encode_orphan_observation(&observation)?.len()).unwrap_or(u64::MAX))
    }

    fn orphan_observation_gc_candidates(
        &self,
        observed_orphan_hashes: &BTreeSet<String>,
    ) -> Result<Vec<OrphanObservationGcCandidate>> {
        let root = self.root.join(ORPHAN_OBSERVATION_DIR);
        if !root.exists() {
            return Ok(Vec::new());
        }
        let mut candidates = Vec::new();
        let mut shard_entries = read_dir(&root)?;
        shard_entries.sort_by_key(|entry| entry.path());
        for shard_entry in shard_entries {
            let shard_path = shard_entry.path();
            if !shard_path.is_dir() {
                continue;
            }
            let mut marker_entries = read_dir(&shard_path)?;
            marker_entries.sort_by_key(|entry| entry.path());
            for marker_entry in marker_entries {
                let marker_path = marker_entry.path();
                if !marker_path.is_file() {
                    continue;
                }
                let Some(file_name) = marker_path.file_name().and_then(|name| name.to_str()) else {
                    continue;
                };
                let Some(hex) = file_name.strip_suffix(".sobj") else {
                    continue;
                };
                let content_hash = format!("{HASH_PREFIX}{hex}");
                if hash_hex(&content_hash).is_err()
                    || observed_orphan_hashes.contains(&content_hash)
                {
                    continue;
                }
                let size_bytes = marker_entry
                    .metadata()
                    .map_err(|source| ContentStoreError::Io {
                        path: marker_path.clone(),
                        source,
                    })?
                    .len();
                candidates.push(OrphanObservationGcCandidate {
                    path: marker_path,
                    size_bytes,
                });
            }
        }
        Ok(candidates)
    }

    fn remove_orphan_observation_without_sync(
        &self,
        content_hash: &str,
    ) -> Result<Option<PathBuf>> {
        let path = self.orphan_observation_path(content_hash)?;
        if remove_file_if_exists_without_sync(&path)? {
            return Ok(path.parent().map(Path::to_owned));
        }
        Ok(None)
    }

    /// Durably forgets every orphan-grace epoch covered by manifests that are
    /// about to become authoritative references.
    ///
    /// The removal is ordered before manifest publication. A crash between
    /// these two steps can only make the next orphan scan start a fresh grace
    /// period; publishing first would leave a window where a later manifest
    /// deletion could expose content to an observation epoch from before the
    /// reference existed.
    fn clear_orphan_observations_before_manifest_publish<'a, I>(&self, manifests: I) -> Result<()>
    where
        I: IntoIterator<Item = &'a ContentManifest>,
    {
        let root = self.root.join(ORPHAN_OBSERVATION_DIR);
        match fs::metadata(&root) {
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => {
                return Err(ContentStoreError::InvalidContentLayout {
                    message: format!(
                        "orphan observation root is not a directory: {}",
                        root.display()
                    ),
                });
            }
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(source) => {
                return Err(ContentStoreError::Io { path: root, source });
            }
        }

        let mut referenced_hashes = BTreeSet::<&str>::new();
        for manifest in manifests {
            if manifest.extents.is_empty() {
                referenced_hashes.insert(manifest.content_hash.as_str());
            } else {
                referenced_hashes.extend(
                    manifest
                        .extents
                        .iter()
                        .map(|extent| extent.content_hash.as_str()),
                );
            }
        }

        let mut dirty_directories = BTreeSet::new();
        for content_hash in referenced_hashes {
            if let Some(parent) = self.remove_orphan_observation_without_sync(content_hash)? {
                dirty_directories.insert(parent);
            }
        }
        self.sync_orphan_observation_directories(&dirty_directories, false)?;
        Ok(())
    }

    fn sync_orphan_observation_directories(
        &self,
        dirty_directories: &BTreeSet<PathBuf>,
        root_created: bool,
    ) -> Result<u64> {
        if dirty_directories.is_empty() && !root_created {
            return Ok(0);
        }
        let root = self.root.join(ORPHAN_OBSERVATION_DIR);
        let mut sync_count = 0_u64;
        for directory in dirty_directories {
            match remove_empty_dir_if_exists_without_sync(directory)? {
                RelaxedDirectoryRemoval::Removed | RelaxedDirectoryRemoval::Missing => {}
                RelaxedDirectoryRemoval::Retained => {
                    sync_dir(directory)?;
                    sync_count = sync_count.saturating_add(1);
                }
            }
        }
        if root.is_dir() {
            sync_dir(&root)?;
            sync_count = sync_count.saturating_add(1);
        }
        if root_created {
            sync_dir(&self.root)?;
            sync_count = sync_count.saturating_add(1);
        }
        Ok(sync_count)
    }

    fn orphan_observation_path(&self, content_hash: &str) -> Result<PathBuf> {
        let hex = hash_hex(content_hash)?;
        Ok(self
            .root
            .join(ORPHAN_OBSERVATION_DIR)
            .join(&hex[..2])
            .join(format!("{hex}.sobj")))
    }

    fn content_reservation_lock_path(&self) -> PathBuf {
        self.root
            .join(CONTENT_RESERVATION_DIR)
            .join(CONTENT_RESERVATION_LOCK_FILE)
    }

    fn content_reservation_batch_sentinel_path(&self) -> PathBuf {
        self.root
            .join(CONTENT_RESERVATION_DIR)
            .join(CONTENT_RESERVATION_BATCH_SENTINEL_SHARD)
            .join(CONTENT_RESERVATION_BATCH_SENTINEL_FILE)
    }

    fn content_reservation_reconciliation_cursor_path(&self) -> PathBuf {
        self.root
            .join(CONTENT_RESERVATION_RECONCILIATION_CURSOR_FILE)
    }

    fn manifest_absence_observation_path(
        &self,
        identity: &ContentStoreReservationIdentity,
    ) -> PathBuf {
        let digest = content_identity_digest(identity);
        self.root
            .join(MANIFEST_ABSENCE_OBSERVATION_DIR)
            .join(&digest[..2])
            .join(format!("{digest}.sobj"))
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

fn remove_file_if_exists_without_sync(path: &Path) -> Result<bool> {
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(ContentStoreError::Io {
            path: path.to_owned(),
            source,
        }),
    }
}

fn remove_empty_dir_if_exists_without_sync(path: &Path) -> Result<RelaxedDirectoryRemoval> {
    match fs::remove_dir(path) {
        Ok(()) => Ok(RelaxedDirectoryRemoval::Removed),
        Err(source) if source.kind() == io::ErrorKind::NotFound => {
            Ok(RelaxedDirectoryRemoval::Missing)
        }
        Err(source) if source.kind() == io::ErrorKind::DirectoryNotEmpty => {
            Ok(RelaxedDirectoryRemoval::Retained)
        }
        Err(source) => Err(ContentStoreError::Io {
            path: path.to_owned(),
            source,
        }),
    }
}

fn encode_manifest(manifest: &ContentManifest) -> Result<Vec<u8>> {
    sinomo_object_serde::to_bytes(manifest, 1, LengthMode::U32)
        .map_err(|source| ContentStoreError::Serde(source.to_string()))
}

fn encode_content_reservation_batch_sentinel(
    sentinel: &ContentStoreReservationBatchSentinel,
) -> Result<Vec<u8>> {
    sinomo_object_serde::to_bytes(sentinel, 1, LengthMode::U32)
        .map_err(|source| ContentStoreError::Serde(source.to_string()))
}

fn decode_content_reservation_batch_sentinel(
    bytes: &[u8],
) -> Result<ContentStoreReservationBatchSentinel> {
    sinomo_object_serde::from_bytes(bytes)
        .map_err(|source| ContentStoreError::Serde(source.to_string()))
}

fn encode_content_reservation_reconciliation_cursor(
    cursor: &ContentStoreReservationReconciliationCursor,
) -> Result<Vec<u8>> {
    sinomo_object_serde::to_bytes(cursor, 1, LengthMode::U32)
        .map_err(|source| ContentStoreError::Serde(source.to_string()))
}

fn decode_content_reservation_reconciliation_cursor(
    bytes: &[u8],
) -> Result<ContentStoreReservationReconciliationCursor> {
    sinomo_object_serde::from_bytes(bytes)
        .map_err(|source| ContentStoreError::Serde(source.to_string()))
}

fn validate_content_reservation_reconciliation_cursor(
    cursor: &ContentStoreReservationReconciliationCursor,
) -> Result<()> {
    if cursor.format_version != CONTENT_RESERVATION_RECONCILIATION_CURSOR_FORMAT_VERSION {
        return Err(ContentStoreError::InvalidContentLayout {
            message: format!(
                "unsupported content reservation reconciliation cursor format version {}",
                cursor.format_version
            ),
        });
    }
    if cursor.catalog_generation.is_empty() {
        return Err(ContentStoreError::InvalidContentLayout {
            message: "content reservation reconciliation cursor has an empty catalog generation"
                .to_owned(),
        });
    }
    hash_hex(&cursor.after.content_hash)?;
    Ok(())
}

fn validate_content_causal_stamp(stamp: &ContentStoreCausalStamp) -> Result<()> {
    if stamp.store_incarnation.parse::<sinomo_core::Id>().is_err() {
        return Err(ContentStoreError::InvalidContentLayout {
            message: "invalid content store incarnation".to_owned(),
        });
    }
    Ok(())
}

fn validate_content_reservation_batch_sentinel(
    sentinel: &ContentStoreReservationBatchSentinel,
) -> Result<()> {
    if sentinel.format_version != CONTENT_RESERVATION_BATCH_SENTINEL_FORMAT_VERSION
        || sentinel
            .inventory
            .as_ref()
            .is_some_and(|state| !state.valid_identity())
        || sentinel.marker != CONTENT_RESERVATION_BATCH_SENTINEL_MARKER
        || sentinel.content_binding_layout != CONTENT_BINDING_LAYOUT
        || sentinel
            .store_incarnation
            .parse::<sinomo_core::Id>()
            .is_err()
    {
        return Err(ContentStoreError::InvalidContentLayout {
            message: format!(
                "invalid content reservation batch sentinel: version={}, marker={:?}",
                sentinel.format_version, sentinel.marker
            ),
        });
    }
    Ok(())
}

fn content_identity_digest(identity: &ContentStoreReservationIdentity) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"sinomo-content-identity-v1");
    for field in [
        identity.object_id.as_bytes(),
        identity.version.as_bytes(),
        identity.content_hash.as_bytes(),
    ] {
        hasher.update(&(field.len() as u64).to_le_bytes());
        hasher.update(field);
    }
    hasher.finalize().to_hex().to_string()
}

fn refreshed_reservation_timestamp(previous: Option<u64>) -> u64 {
    system_time_nanos_since_epoch_saturating(SystemTime::now())
        .max(previous.unwrap_or(0).saturating_add(1))
        .max(1)
}

fn content_reservation_identity_for_entry(
    entry: &VersionManifestEntry,
) -> ContentStoreReservationIdentity {
    ContentStoreReservationIdentity {
        object_id: entry.object_id.clone(),
        version: entry.version.clone(),
        content_hash: entry.content_hash.clone(),
    }
}

fn encode_manifest_absence_observation(
    observation: &ManifestAbsenceObservation,
) -> Result<Vec<u8>> {
    sinomo_object_serde::to_bytes(observation, 1, LengthMode::U32)
        .map_err(|source| ContentStoreError::Serde(source.to_string()))
}

fn decode_manifest_absence_observation(bytes: &[u8]) -> Result<ManifestAbsenceObservation> {
    sinomo_object_serde::from_bytes(bytes)
        .map_err(|source| ContentStoreError::Serde(source.to_string()))
}

fn validate_manifest_absence_observation(
    observation: &ManifestAbsenceObservation,
    expected: &ContentStoreReservationIdentity,
) -> Result<()> {
    if observation.format_version != MANIFEST_ABSENCE_OBSERVATION_FORMAT_VERSION {
        return Err(ContentStoreError::InvalidContentLayout {
            message: format!(
                "unsupported manifest absence observation format version {}",
                observation.format_version
            ),
        });
    }
    if observation.first_observed_at_unix_nanos == 0 {
        return Err(ContentStoreError::InvalidContentLayout {
            message: "manifest absence observation has an invalid zero timestamp".to_owned(),
        });
    }
    if observation.catalog_generation.is_empty() || observation.catalog_generated_at_unix_nanos == 0
    {
        return Err(ContentStoreError::InvalidContentLayout {
            message: "manifest absence observation has an invalid catalog proof".to_owned(),
        });
    }
    let actual = ContentStoreReservationIdentity {
        object_id: observation.object_id.clone(),
        version: observation.version.clone(),
        content_hash: observation.content_hash.clone(),
    };
    hash_hex(&actual.content_hash)?;
    if &actual != expected {
        return Err(ContentStoreError::InvalidContentLayout {
            message: format!(
                "manifest absence observation identity mismatch: expected={expected:?}, actual={actual:?}"
            ),
        });
    }
    Ok(())
}

fn encode_orphan_observation(observation: &OrphanObservation) -> Result<Vec<u8>> {
    sinomo_object_serde::to_bytes(observation, 1, LengthMode::U32)
        .map_err(|source| ContentStoreError::Serde(source.to_string()))
}

fn decode_orphan_observation(bytes: &[u8]) -> Result<OrphanObservation> {
    sinomo_object_serde::from_bytes(bytes)
        .map_err(|source| ContentStoreError::Serde(source.to_string()))
}

fn validate_orphan_observation(
    observation: &OrphanObservation,
    expected_content_hash: &str,
) -> Result<()> {
    if observation.format_version != ORPHAN_OBSERVATION_FORMAT_VERSION {
        return Err(ContentStoreError::InvalidContentLayout {
            message: format!(
                "unsupported orphan observation format version {}",
                observation.format_version
            ),
        });
    }
    if observation.first_observed_at_unix_nanos == 0 {
        return Err(ContentStoreError::InvalidContentLayout {
            message: "orphan observation has an invalid zero timestamp".to_owned(),
        });
    }
    if observation.catalog_generation.is_empty() || observation.catalog_observed_at_unix_nanos == 0
    {
        return Err(ContentStoreError::InvalidContentLayout {
            message: "orphan observation has an invalid catalog proof".to_owned(),
        });
    }
    if observation.content_hash != expected_content_hash {
        return Err(ContentStoreError::InvalidContentLayout {
            message: format!(
                "orphan observation hash mismatch: expected={expected_content_hash}, actual={}",
                observation.content_hash
            ),
        });
    }
    Ok(())
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

/// Materialize only the selected loose bucket under the existing content owner.
/// Synchronize its name before file publication, including a retry after an
/// earlier directory synchronization failed.
fn ensure_loose_blob_parent(blob_path: &Path) -> Result<()> {
    if let Some(parent) = blob_path.parent() {
        create_dir_all(parent.to_owned())?;
        if let Some(blobs_root) = parent.parent() {
            sync_dir(blobs_root)?;
        }
    }
    Ok(())
}

fn publish_file_as_blob(source_path: &Path, blob_path: &Path, expected_hash: &str) -> Result<()> {
    ensure_loose_blob_parent(blob_path)?;
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

fn write_content_addressed_blob_bytes_prehashed_defer_dir_sync(
    hash: &str,
    blob_path: &Path,
    bytes: &[u8],
) -> Result<Option<PathBuf>> {
    let trace_started = Instant::now();
    let create_dir_started = Instant::now();
    ensure_loose_blob_parent(blob_path)?;
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

impl LocalContentStore {
    pub(crate) fn run_version_gc_runtime_probe(
        root: &Path,
        options: crate::DfsContentVersionGcRuntimeProbeOptions,
    ) -> std::result::Result<crate::DfsContentVersionGcRuntimeProbeReport, crate::DfsError> {
        if options.version_count < 2 {
            return Err(crate::DfsError::ContentStore {
                message: "content-version GC runtime probe requires at least two versions"
                    .to_string(),
            });
        }
        if options.write_batch_size == 0 {
            return Err(crate::DfsError::ContentStore {
                message: "content-version GC runtime probe write batch size must be positive"
                    .to_string(),
            });
        }
        if root.exists()
            && fs::read_dir(root)
                .map_err(|source| ContentStoreError::Io {
                    path: root.to_path_buf(),
                    source,
                })?
                .next()
                .is_some()
        {
            return Err(crate::DfsError::ContentStore {
                message: format!(
                    "content-version GC runtime probe requires an empty root: {}",
                    root.display()
                ),
            });
        }

        let total_started = Instant::now();
        let store = Self::new(root)?;
        let write_started = Instant::now();
        for batch_start in (0..options.version_count).step_by(options.write_batch_size) {
            let batch_end = batch_start
                .saturating_add(options.write_batch_size)
                .min(options.version_count);
            let versions = (batch_start..batch_end)
                .map(|index| format!("version_{index:06}"))
                .collect::<Vec<_>>();
            let payloads = (batch_start..batch_end)
                .map(|index| {
                    let mut payload = [0_u8; 128];
                    payload[..8].copy_from_slice(&(index as u64).to_le_bytes());
                    for (offset, byte) in payload[8..].iter_mut().enumerate() {
                        *byte = (index.wrapping_mul(131).wrapping_add(offset).wrapping_add(1)
                            as u8)
                            .rotate_left((offset % 8) as u32);
                    }
                    payload
                })
                .collect::<Vec<_>>();
            store.put_object_versions(versions.iter().zip(&payloads).map(
                |(version, payload)| {
                    (
                        "content-version-gc-runtime-probe-object",
                        version.as_str(),
                        payload.as_slice(),
                    )
                },
            ))?;
        }
        let write_elapsed = write_started.elapsed();

        let reservation_release_started = Instant::now();
        store.release_all_content_reservations_for_runtime_probe()?;
        let reservation_release_elapsed = reservation_release_started.elapsed();
        let remaining_reservations = store.content_reservation_roots()?.reservation_count;
        if remaining_reservations != 0 {
            return Err(crate::DfsError::ContentStore {
                message: format!(
                    "content-version GC runtime probe retained {remaining_reservations} fixture reservations after simulated catalog coverage"
                ),
            });
        }

        let projection_started = Instant::now();
        let projection =
            store.collect_versions(ContentStoreVersionGcOptions::dry_run(Duration::ZERO, 1))?;
        let projection_elapsed = projection_started.elapsed();
        let expected_deleted = options.version_count.saturating_sub(1) as u64;
        if projection.eligible_version_count != expected_deleted {
            return Err(crate::DfsError::ContentStore {
                message: format!(
                    "content-version GC runtime projection found {} eligible versions, expected {expected_deleted}",
                    projection.eligible_version_count
                ),
            });
        }

        let observed = store.collect_versions(
            ContentStoreVersionGcOptions::delete_eligible(Duration::ZERO, 1)
                .with_min_absence_age(Duration::ZERO)
                .with_catalog_observation("runtime-probe-catalog-generation-1", 1),
        )?;
        if observed.deleted_version_count != 0
            || observed.persisted_absence_observation_count
                != observed.newly_observed_absent_version_count
        {
            return Err(crate::DfsError::ContentStore {
                message: format!(
                    "content-version GC runtime observation was not a durable no-delete pass: {observed:?}"
                ),
            });
        }

        let gc_started = Instant::now();
        let report = store.collect_versions(
            ContentStoreVersionGcOptions::delete_eligible(Duration::ZERO, 1)
                .with_catalog_observation("runtime-probe-catalog-generation-2", u64::MAX),
        )?;
        let gc_elapsed = gc_started.elapsed();
        if report.deleted_version_count != expected_deleted {
            return Err(crate::DfsError::ContentStore {
                message: format!(
                    "content-version GC runtime probe deleted {} versions, expected {expected_deleted}",
                    report.deleted_version_count
                ),
            });
        }
        let expected_delete_batches = options
            .version_count
            .saturating_sub(1)
            .div_ceil(VERSION_GC_DELETE_BATCH_SIZE) as u64;
        if report.durable_delete_batch_count != expected_delete_batches
            || report.durable_directory_sync_count != 1
        {
            return Err(crate::DfsError::ContentStore {
                message: format!(
                    "content-version GC runtime durability amplification changed: batches={} expected={expected_delete_batches} directory_syncs={} expected=1",
                    report.durable_delete_batch_count, report.durable_directory_sync_count
                ),
            });
        }
        let retained_manifest_count = store
            .manifests_for_object("content-version-gc-runtime-probe-object")?
            .len() as u64;
        if retained_manifest_count != 1 {
            return Err(crate::DfsError::ContentStore {
                message: format!(
                    "content-version GC runtime probe retained {retained_manifest_count} manifests, expected 1"
                ),
            });
        }

        drop(store);
        let restart_started = Instant::now();
        let reopened = Self::new(root)?;
        let restart_elapsed = restart_started.elapsed();
        let reopened_manifest_count = reopened
            .manifests_for_object("content-version-gc-runtime-probe-object")?
            .len() as u64;
        if reopened_manifest_count != 1 {
            return Err(crate::DfsError::ContentStore {
                message: format!(
                    "content-version GC runtime probe reopened with {reopened_manifest_count} manifests, expected 1"
                ),
            });
        }

        Ok(crate::DfsContentVersionGcRuntimeProbeReport {
            version_count: options.version_count as u64,
            write_elapsed_ms: write_elapsed.as_millis().min(u128::from(u64::MAX)) as u64,
            reservation_release_elapsed_ms: reservation_release_elapsed
                .as_millis()
                .min(u128::from(u64::MAX)) as u64,
            projection_elapsed_ms: projection_elapsed.as_millis().min(u128::from(u64::MAX)) as u64,
            gc_elapsed_ms: gc_elapsed.as_millis().min(u128::from(u64::MAX)) as u64,
            restart_elapsed_ms: restart_elapsed.as_millis().min(u128::from(u64::MAX)) as u64,
            total_elapsed_ms: total_started
                .elapsed()
                .as_millis()
                .min(u128::from(u64::MAX)) as u64,
            deleted_version_count: report.deleted_version_count,
            durable_delete_batch_count: report.durable_delete_batch_count,
            durable_directory_sync_count: report.durable_directory_sync_count,
            retained_manifest_count,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static TEST_ORPHAN_CATALOG_OBSERVATION: AtomicU64 = AtomicU64::new(1);

    #[cfg(unix)]
    #[test]
    fn existing_current_store_open_does_not_stage_a_redundant_sentinel() {
        let root = temp_root("existing_content_sentinel_no_stage");
        let initialized = LocalContentStore::new(&root).unwrap();
        let sentinel = initialized.content_reservation_batch_sentinel_path();
        let shard = sentinel.parent().unwrap();
        let bytes = fs::read(&sentinel).unwrap();
        drop(initialized);
        // A controlled fixture timestamp exposes transient create/unlink work
        // even when the existing sentinel bytes are never replaced.
        let directory = fs::File::open(shard).unwrap();
        directory
            .set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(1))
            .unwrap();
        let before = fs::metadata(shard).unwrap().modified().unwrap();
        let reopened = LocalContentStore::new(&root).unwrap();
        assert_eq!(fs::read(&sentinel).unwrap(), bytes);
        assert_eq!(
            fs::metadata(shard).unwrap().modified().unwrap(),
            before,
            "opening an initialized store must not stage and remove an already-present sentinel"
        );
        // This remains a normal writer with its existing durability fences.
        let object = sinomo_core::Id::generate().to_string();
        let version = sinomo_core::Id::generate().to_string();
        reopened
            .put_object_version(&object, &version, b"genuine activity")
            .unwrap();
        assert_eq!(
            reopened.read_object_version(&object, &version).unwrap().1,
            b"genuine activity"
        );
        drop(reopened);
        drop(directory);
        cleanup(&root);
    }

    #[cfg(windows)]
    #[test]
    fn content_reservation_writer_syncs_existing_sentinel_on_windows() {
        let root = temp_root("content_reservation_writer_windows_sentinel_sync");
        LocalContentStore::new(&root).expect("create store and durable sentinel");
        LocalContentStore::new(&root).expect("reopen store and sync existing sentinel");
        cleanup(&root);
    }

    fn orphan_catalog_observation() -> (String, u64) {
        let sequence = TEST_ORPHAN_CATALOG_OBSERVATION.fetch_add(1, Ordering::SeqCst);
        (
            format!("test-orphan-catalog-generation-{sequence}"),
            system_time_nanos_since_epoch_saturating(SystemTime::now())
                .saturating_add(sequence.saturating_mul(1_000_000_000)),
        )
    }

    fn delete_eligible_orphan_gc_options(min_orphan_age: Duration) -> ContentStoreGcOptions {
        let (generation, observed_at_unix_nanos) = orphan_catalog_observation();
        ContentStoreGcOptions::delete_eligible(min_orphan_age)
            .with_catalog_observation(generation, observed_at_unix_nanos)
    }

    fn dry_run_orphan_gc_options(min_orphan_age: Duration) -> ContentStoreGcOptions {
        let (generation, observed_at_unix_nanos) = orphan_catalog_observation();
        ContentStoreGcOptions::dry_run(min_orphan_age)
            .with_catalog_observation(generation, observed_at_unix_nanos)
    }

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
        assert_eq!(
            store.content_reservation_roots().unwrap().reservation_count,
            0
        );

        let manifest = store
            .bind_quarantined_blob("stream-object", "v1", blob)
            .expect("bind validated blob");
        assert_eq!(manifest.content_hash, expected_hash);
        let roots = store
            .content_reservation_roots()
            .expect("read bound reservation");
        assert_eq!(roots.reservation_count, 1);
        assert_eq!(roots.reservations[0].identity.object_id, "stream-object");
        assert_eq!(roots.reservations[0].identity.version, "v1");
        assert_eq!(roots.reservations[0].identity.content_hash, expected_hash);
        let (_, stored_bytes) = store
            .read_object_version("stream-object", "v1")
            .expect("read bound object");
        assert_eq!(stored_bytes, bytes);
        drop(store);
        let reopened = LocalContentStore::new(&root).expect("reopen bound content store");
        assert_eq!(reopened.content_reservation_roots().unwrap(), roots);
        assert_eq!(
            reopened
                .read_object_version("stream-object", "v1")
                .unwrap()
                .1,
            bytes
        );

        cleanup(&root);
    }

    #[test]
    fn quarantined_stream_drop_and_incomplete_content_leave_no_binding() {
        let root = temp_root("quarantine-private-stage-cleanup");
        let store = LocalContentStore::new(&root).unwrap();
        let mut ingest = store.begin_quarantined_blob_ingest(4, None).unwrap();
        ingest.write_chunk(b"data").unwrap();
        let blob = ingest.finish_quarantined().unwrap();
        let path = blob.synced_file.path().to_owned();
        assert!(path.is_file());
        drop(blob);
        assert!(
            !path.exists(),
            "dropping the finalized capability removes unbound bytes"
        );
        let mut incomplete = store.begin_quarantined_blob_ingest(8, None).unwrap();
        let incomplete_path = incomplete.temporary_path.clone();
        incomplete.write_chunk(b"short").unwrap();
        assert!(matches!(
            incomplete.finish_quarantined(),
            Err(ContentStoreError::InvalidContentLayout { .. })
        ));
        assert!(!incomplete_path.exists());
        assert_eq!(
            store.content_reservation_roots().unwrap().reservation_count,
            0
        );
        cleanup(&root);
    }

    #[cfg(unix)]
    #[test]
    fn quarantined_stream_binding_rejects_mutation_after_sync() {
        let root = temp_root("quarantine-private-stage-mutation");
        let store = LocalContentStore::new(&root).unwrap();
        let bytes = b"original streamed content";
        let hash = content_hash(bytes);
        let mut ingest = store
            .begin_quarantined_blob_ingest(bytes.len() as u64, Some(&hash))
            .unwrap();
        ingest.write_chunk(bytes).unwrap();
        let blob = ingest.finish_quarantined().unwrap();
        let path = blob.synced_file.path().to_owned();
        fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"mutated")
            .unwrap();
        let error = store
            .bind_quarantined_blob("changed-object", "v1", blob)
            .expect_err("unsynchronized changes must not reuse the old durability capability");
        assert!(
            matches!(error, ContentStoreError::Io { source, .. } if source.kind() == io::ErrorKind::InvalidData)
        );
        assert!(!store.has_blob(&hash).unwrap());
        assert!(store.read_manifest("changed-object", "v1").is_err());
        assert!(!path.exists());
        // The bounded inline producer validates the synced private capability
        // before reserving a version. Rejected mutation owns no reservation.
        assert_eq!(
            store.content_reservation_roots().unwrap().reservation_count,
            0
        );
        cleanup(&root);
    }

    #[test]
    fn quarantined_stream_binding_resets_orphan_grace_before_manifest() {
        let root = temp_root("quarantine-private-stage-orphan-grace");
        let store = LocalContentStore::new(&root).unwrap();
        let bytes = b"previously unreferenced streamed content";
        let hash = content_hash(bytes);
        write_file_atomic(&store.blob_path(&hash).unwrap(), bytes).unwrap();
        let first = store
            .collect_garbage(delete_eligible_orphan_gc_options(Duration::ZERO))
            .unwrap();
        assert_eq!(first.newly_observed_orphan_count, 1);
        let observation = store.orphan_observation_path(&hash).unwrap();
        assert!(observation.exists());
        let mut ingest = store
            .begin_quarantined_blob_ingest(bytes.len() as u64, Some(&hash))
            .unwrap();
        ingest.write_chunk(bytes).unwrap();
        let blob = ingest.finish_quarantined().unwrap();
        store
            .bind_quarantined_blob("referenced-stream", "v1", blob)
            .unwrap();
        assert!(!observation.exists());
        drop(store);
        let reopened = LocalContentStore::new(&root).unwrap();
        assert_eq!(
            reopened
                .read_object_version("referenced-stream", "v1")
                .unwrap()
                .1,
            bytes
        );
        assert_eq!(
            reopened
                .collect_garbage(delete_eligible_orphan_gc_options(Duration::ZERO))
                .unwrap()
                .deleted_orphan_count,
            0
        );
        cleanup(&root);
    }

    #[test]
    fn current_sentinel_blocks_destructive_gc_when_malformed() {
        let root = temp_root("content_reservation_batch_sentinel");
        let store = LocalContentStore::new(&root).expect("create store");
        let sentinel_path = store.content_reservation_batch_sentinel_path();
        let sentinel_bytes = read_file(&sentinel_path).expect("read sentinel");

        let manifests = store
            .put_object_versions([
                ("sentinel-object", "v1", b"old manifest".as_slice()),
                ("sentinel-object", "v2", b"latest manifest".as_slice()),
            ])
            .expect("publish manifest batch");
        let blob_only = store
            .put_object_version_blobs_only([(
                "sentinel-blob-only",
                "v1",
                b"unpublished blob".as_slice(),
            )])
            .expect("publish blob-only batch")
            .remove(0);
        release_all_content_reservations(&store);
        let first_versions = store
            .collect_versions(
                ContentStoreVersionGcOptions::delete_eligible(Duration::ZERO, 1)
                    .with_min_absence_age(Duration::ZERO)
                    .with_catalog_observation("sentinel-version-catalog-1", u64::MAX),
            )
            .expect("observe old manifest absence");
        assert_eq!(first_versions.newly_observed_absent_version_count, 1);
        let first_orphans = store
            .collect_garbage(
                ContentStoreGcOptions::delete_eligible(Duration::ZERO)
                    .with_catalog_observation("sentinel-orphan-catalog-1", u64::MAX),
            )
            .expect("observe orphan blob");
        assert_eq!(first_orphans.newly_observed_orphan_count, 1);
        let old_identity = ContentStoreReservationIdentity {
            object_id: manifests[0].object_id.clone(),
            version: manifests[0].version.clone(),
            content_hash: manifests[0].content_hash.clone(),
        };
        let manifest_observation = store.manifest_absence_observation_path(&old_identity);
        let orphan_observation = store
            .orphan_observation_path(&blob_only.content_hash)
            .expect("orphan observation path");
        assert!(manifest_observation.is_file());
        assert!(orphan_observation.is_file());

        let malformed = ContentStoreReservationBatchSentinel {
            format_version: CONTENT_RESERVATION_BATCH_SENTINEL_FORMAT_VERSION,
            marker: "wrong-marker".to_owned(),
            ..ContentStoreReservationBatchSentinel::new()
        };
        write_file_atomic(
            &sentinel_path,
            &encode_content_reservation_batch_sentinel(&malformed)
                .expect("encode malformed sentinel"),
        )
        .expect("install malformed sentinel");

        store
            .content_reservation_roots()
            .expect_err("new reader must reject a malformed exact-path sentinel");
        store
            .collect_versions(
                ContentStoreVersionGcOptions::delete_eligible(Duration::ZERO, 1)
                    .with_min_absence_age(Duration::ZERO)
                    .with_catalog_observation("sentinel-version-catalog-2", u64::MAX),
            )
            .expect_err("version GC must fail before deleting with a malformed sentinel");
        store
            .collect_garbage(
                ContentStoreGcOptions::delete_eligible(Duration::ZERO)
                    .with_catalog_observation("sentinel-orphan-catalog-2", u64::MAX),
            )
            .expect_err("orphan GC must fail before deleting with a malformed sentinel");
        assert!(store.root.join("inline-bindings/current.sobj").is_file());
        store
            .has_blob(&blob_only.content_hash)
            .expect_err("blob reads must fail closed while the mandatory layout gate is malformed");
        store.read_manifest("sentinel-object", "v1").expect_err(
            "manifest reads must not guess legacy fallback from a malformed layout gate",
        );
        // Restore the exact known-valid gate before inspecting the preserved
        // logical contents. Allowing reads through the malformed gate would
        // bypass an unknown inline layout and could resurrect collected data.
        write_file_atomic(&sentinel_path, &sentinel_bytes)
            .expect("restore original valid sentinel");
        assert_eq!(
            store
                .read_blob(&blob_only.content_hash)
                .expect("blob after restored gate"),
            b"unpublished blob"
        );
        let (preserved, bytes) = store
            .read_object_version("sentinel-object", "v1")
            .expect("old version after restored gate");
        assert_eq!(preserved, manifests[0]);
        assert_eq!(bytes, b"old manifest");
        assert!(manifest_observation.is_file());
        assert!(orphan_observation.is_file());
        cleanup(&root);
    }

    #[test]
    fn inspection_open_is_non_creating_and_rejects_unconverted_records() {
        let root = temp_root("content-reservation-inspection");
        let missing = root.join("missing");
        let inspected = LocalContentStore::open_for_inspection(&missing);
        assert_eq!(
            inspected
                .content_reservation_roots()
                .unwrap()
                .reservation_count,
            0
        );
        assert!(!missing.exists());
        let old = root.join("old");
        fs::create_dir_all(old.join("objects/object/v1")).unwrap();
        fs::write(old.join("objects/object/v1/manifest.sobj"), b"obsolete").unwrap();
        assert!(LocalContentStore::new(&old).is_err());
        assert!(
            LocalContentStore::open_for_inspection(&old)
                .read_manifest("object", "v1")
                .is_err()
        );
        assert!(!old.join("inline-bindings").exists());
    }

    #[test]
    fn causal_catalog_cut_survives_restart_and_monotonic_renewal() {
        let root = temp_root("causal-cut-restart");
        let store = LocalContentStore::new(&root).unwrap();
        let hash = content_hash(b"bytes");
        store
            .ensure_content_reservation("object", "v1", &hash)
            .unwrap();
        let first = store.content_reservation_roots().unwrap().reservations[0].clone();
        let cut = store.begin_content_root_catalog_scan_at(200).unwrap();
        let other = LocalContentStore::new(&root).unwrap();
        other
            .ensure_content_reservation("object", "v1", &hash)
            .unwrap();
        drop(other);
        drop(store);
        let reopened = LocalContentStore::new(&root).unwrap();
        let record = reopened.content_reservation_roots().unwrap().reservations[0].clone();
        assert!(record.reserved_at_unix_nanos >= first.reserved_at_unix_nanos);
        assert_eq!(record.causal_stamp, cut.stamp);
        let report = reopened
            .reconcile_content_reservations_with_catalog(
                &BTreeSet::new(),
                &BTreeSet::new(),
                ContentStoreReservationReconciliationOptions::bounded(u64::MAX, Duration::ZERO, 10)
                    .with_catalog_generation("delayed-cut")
                    .with_catalog_scan_cut(Some(cut)),
            )
            .unwrap();
        assert_eq!(report.released_rejected_count, 0);
        assert_eq!(
            reopened
                .content_reservation_roots()
                .unwrap()
                .reservation_count,
            1
        );
        let later = reopened
            .begin_content_root_catalog_scan_at(u64::MAX)
            .unwrap();
        let report = reopened
            .reconcile_content_reservations_with_catalog(
                &BTreeSet::new(),
                &BTreeSet::new(),
                ContentStoreReservationReconciliationOptions::bounded(u64::MAX, Duration::ZERO, 10)
                    .with_catalog_generation("later-cut")
                    .with_catalog_scan_cut(Some(later)),
            )
            .unwrap();
        assert_eq!(report.released_rejected_count, 1);
        assert_eq!(
            reopened
                .content_reservation_roots()
                .unwrap()
                .reservation_count,
            0
        );
        assert_eq!(refreshed_reservation_timestamp(Some(u64::MAX)), u64::MAX);
    }

    #[test]
    fn causal_catalog_rejects_missing_and_foreign_reservation_provenance() {
        let root = temp_root("causal-invalid-provenance");
        let store = LocalContentStore::new(&root).unwrap();
        let hash = content_hash(b"bytes");
        store
            .ensure_content_reservation("object", "v1", &hash)
            .unwrap();
        let original = store.content_reservation_roots().unwrap();
        let mut missing = serde_json::to_value(&original.reservations[0]).unwrap();
        missing.as_object_mut().unwrap().remove("causal_stamp");
        assert!(
            serde_json::from_value::<ContentStoreReservationRecord>(missing).is_err(),
            "old missing-stamp data must go through migration"
        );
        let mut record = original.reservations[0].clone();
        record.causal_stamp = ContentStoreCausalStamp {
            store_incarnation: sinomo_core::Id::generate().to_string(),
            catalog_scan_epoch: 0,
        };
        let _owner = store.content_mutation_guard().unwrap();
        let reservation = store.lock_content_reservations_for_write().unwrap();
        assert!(
            store
                .inline_reserve_owned_identity_unlocked(&record, &reservation.sentinel)
                .is_err()
        );
        drop(reservation);
        drop(_owner);
        assert_eq!(store.content_reservation_roots().unwrap(), original);
    }

    #[test]
    fn causal_catalog_cut_is_store_scoped_and_unsealed_catalogs_only_release_coverage() {
        let root = temp_root("causal_cut_store_scope");
        let store = LocalContentStore::new(root.join("one")).unwrap();
        let other = LocalContentStore::new(root.join("two")).unwrap();
        store
            .ensure_content_reservation("object", "v1", &content_hash(b"root"))
            .unwrap();
        for cut in [
            None,
            Some(other.begin_content_root_catalog_scan_at(u64::MAX).unwrap()),
        ] {
            let options =
                ContentStoreReservationReconciliationOptions::bounded(u64::MAX, Duration::ZERO, 10)
                    .with_catalog_generation("foreign-or-unsealed")
                    .with_catalog_scan_cut(cut);
            let report = store
                .reconcile_content_reservations_with_catalog(
                    &BTreeSet::new(),
                    &BTreeSet::new(),
                    options,
                )
                .unwrap();
            assert_eq!(report.released_rejected_count, 0);
        }
        let report = store
            .reconcile_content_reservations_with_catalog(
                &BTreeSet::from([("object".into(), "v1".into())]),
                &BTreeSet::new(),
                ContentStoreReservationReconciliationOptions::bounded(0, Duration::ZERO, 10)
                    .with_catalog_generation("unsealed-covered"),
            )
            .unwrap();
        assert_eq!(report.released_catalog_covered_count, 1);
        cleanup(&root);
    }

    #[test]
    fn causal_catalog_revalidates_layout_and_rejects_epoch_overflow() {
        let root = temp_root("causal-sentinel-gate");
        let store = LocalContentStore::new(&root).unwrap();
        let path = store.content_reservation_batch_sentinel_path();
        let original =
            decode_content_reservation_batch_sentinel(&read_file(&path).unwrap()).unwrap();
        let mut sentinel = original.clone();
        sentinel.format_version = 1;
        write_file_atomic(
            &path,
            &encode_content_reservation_batch_sentinel(&sentinel).unwrap(),
        )
        .unwrap();
        assert!(
            store
                .ensure_content_reservation("object", "v1", &content_hash(b"root"))
                .is_err()
        );
        assert!(LocalContentStore::new(&root).is_err());
        sentinel = original;
        sentinel.catalog_scan_epoch = u64::MAX;
        write_file_atomic(
            &path,
            &encode_content_reservation_batch_sentinel(&sentinel).unwrap(),
        )
        .unwrap();
        assert!(store.begin_content_root_catalog_scan_at(1).is_err());
        assert_eq!(
            decode_content_reservation_batch_sentinel(&read_file(&path).unwrap()).unwrap(),
            sentinel
        );
        sentinel.content_binding_layout = "future-binding-log".into();
        write_file_atomic(
            &path,
            &encode_content_reservation_batch_sentinel(&sentinel).unwrap(),
        )
        .unwrap();
        assert!(
            store
                .ensure_content_reservation("object", "v1", &content_hash(b"root"))
                .is_err()
        );
        assert!(LocalContentStore::new(&root).is_err());
    }

    #[test]
    fn bounded_reconciliation_preserves_unprocessed_roots_then_releases() {
        let root = temp_root("bounded-reservation-membership");
        let store = LocalContentStore::new(&root).unwrap();
        store
            .put_object_version_blobs_only([
                ("a-covered", "v1", b"a".as_slice()),
                ("b-covered", "v1", b"b".as_slice()),
                ("c-rejected", "v1", b"c".as_slice()),
            ])
            .unwrap();
        let covered = BTreeSet::from([
            ("a-covered".into(), "v1".into()),
            ("b-covered".into(), "v1".into()),
        ]);
        let cut = store.begin_content_root_catalog_scan().unwrap();
        let partial = store
            .reconcile_content_reservations_with_catalog(
                &covered,
                &BTreeSet::new(),
                ContentStoreReservationReconciliationOptions::bounded(
                    u64::MAX,
                    Duration::from_secs(60),
                    2,
                )
                .with_catalog_generation("membership")
                .with_catalog_scan_cut(Some(cut)),
            )
            .unwrap();
        assert_eq!(partial.released_catalog_covered_count, 2);
        let roots = store.content_reservation_roots().unwrap();
        assert_eq!(roots.reservation_count, 1);
        assert_eq!(roots.reservations[0].identity.object_id, "c-rejected");
        let cut = store.begin_content_root_catalog_scan().unwrap();
        let full = store
            .reconcile_content_reservations_with_catalog(
                &covered,
                &BTreeSet::new(),
                ContentStoreReservationReconciliationOptions::bounded(u64::MAX, Duration::ZERO, 2)
                    .with_catalog_generation("membership")
                    .with_catalog_scan_cut(Some(cut)),
            )
            .unwrap();
        assert_eq!(full.released_rejected_count, 1);
        assert_eq!(
            store.content_reservation_roots().unwrap().reservation_count,
            0
        );
    }

    #[test]
    fn batch_retry_refreshes_roots_against_a_stale_catalog() {
        let root = temp_root("content_reservation_batch_retry");
        let store = LocalContentStore::new(&root).expect("create store");
        let inputs = [
            ("retry-object-a", "v1", b"retry-a".as_slice()),
            ("retry-object-b", "v1", b"retry-b".as_slice()),
            ("retry-object-c", "v1", b"retry-c".as_slice()),
        ];
        store
            .put_object_versions(inputs)
            .expect("publish initial batch");
        let initial = store
            .content_reservation_roots()
            .expect("read initial roots");
        let stale_catalog_time = initial
            .reservations
            .iter()
            .map(|reservation| reservation.reserved_at_unix_nanos)
            .max()
            .expect("initial reservation time")
            .saturating_add(1);

        store
            .put_object_versions(inputs)
            .expect("retry batch publication");
        let refreshed = store
            .content_reservation_roots()
            .expect("read refreshed roots");
        assert_eq!(refreshed.reservation_count, inputs.len() as u64);
        assert!(
            refreshed
                .reservations
                .iter()
                .all(|reservation| reservation.reserved_at_unix_nanos >= stale_catalog_time)
        );

        let stale = store
            .reconcile_content_reservations_with_catalog(
                &BTreeSet::new(),
                &BTreeSet::new(),
                ContentStoreReservationReconciliationOptions::bounded(
                    stale_catalog_time,
                    Duration::ZERO,
                    8,
                )
                .with_catalog_generation("pre-retry-catalog")
                .with_catalog_scan_cut(Some(store.begin_content_root_catalog_scan().unwrap())),
            )
            .expect("apply stale catalog");
        assert_eq!(stale.released_rejected_count, 0);
        assert_eq!(stale.retained_catalog_too_old_count, inputs.len() as u64);
        assert_eq!(
            store
                .content_reservation_roots()
                .expect("retry roots retained")
                .reservation_count,
            inputs.len() as u64
        );
        cleanup(&root);
    }

    #[test]
    fn durable_content_reservations_survive_restart_and_protect_gc_roots() {
        let root = temp_root("durable_content_reservations_survive_restart");
        let store = LocalContentStore::new(&root).expect("create store");
        let mut streamed = store
            .begin_object_version_ingest(
                "reserved-object",
                "version_1",
                b"reserved old bytes".len() as u64,
                None,
            )
            .expect("begin reserved streamed version");
        streamed
            .write_chunk(b"reserved old bytes")
            .expect("write reserved stream");
        let old = streamed
            .finish()
            .expect("publish reserved synced private stage");
        store
            .put_object_version("reserved-object", "version_2", b"reserved latest bytes")
            .expect("put reserved latest version");
        let blob_only = store
            .put_object_version_blob_only(
                "reserved-blob-only-object",
                "version_1",
                b"reserved blob-only bytes",
            )
            .expect("put reserved blob-only version");

        let roots = store
            .content_reservation_roots()
            .expect("read initial reservation roots");
        assert_eq!(roots.reservation_count, 3);
        assert!(
            roots
                .reservations
                .iter()
                .all(|reservation| reservation.reserved_at_unix_nanos > 0)
        );
        assert!(
            roots
                .versions
                .contains(&("reserved-object".to_owned(), "version_1".to_owned()))
        );
        assert!(roots.content_hashes.contains(&old.content_hash));
        assert!(roots.content_hashes.contains(&blob_only.content_hash));
        drop(store);

        let reopened = LocalContentStore::new(&root).expect("reopen store");
        let reopened_roots = reopened
            .content_reservation_roots()
            .expect("read reopened reservation roots");
        assert_eq!(reopened_roots, roots);

        for catalog_index in 0..2 {
            let versions = reopened
                .collect_versions(
                    ContentStoreVersionGcOptions::delete_eligible(Duration::ZERO, 1)
                        .with_min_absence_age(Duration::ZERO)
                        .with_catalog_observation(
                            format!("reserved-catalog-generation-{catalog_index}"),
                            catalog_index + 1,
                        ),
                )
                .expect("reserved version GC");
            assert_eq!(versions.deleted_version_count, 0);
            assert_eq!(versions.newly_observed_absent_version_count, 0);

            let blobs = reopened
                .collect_garbage(delete_eligible_orphan_gc_options(Duration::ZERO))
                .expect("reserved blob GC");
            assert_eq!(blobs.deleted_orphan_count, 0);
            assert_eq!(blobs.newly_observed_orphan_count, 0);
            assert_eq!(blobs.retained_protected_orphan_count, 1);
        }
        assert!(
            reopened
                .read_manifest("reserved-object", "version_1")
                .is_ok()
        );
        assert!(
            reopened
                .has_blob(&blob_only.content_hash)
                .expect("reserved blob-only content remains")
        );

        cleanup(&root);
    }

    #[test]
    fn reservation_only_crash_state_is_reconciled_after_restart() {
        let root = temp_root("reservation_only_crash_recovery");
        let store = LocalContentStore::new(&root).expect("create store");
        let content_hash = content_hash(b"never published");
        store
            .ensure_content_reservation("abandoned-object", "v1", &content_hash)
            .expect("persist reservation before simulated crash");
        let reserved_at_unix_nanos = store
            .content_reservation_roots()
            .expect("read reservation before restart")
            .reservations[0]
            .reserved_at_unix_nanos;
        drop(store);

        let reopened = LocalContentStore::new(&root).expect("reopen after simulated crash");
        assert_eq!(
            reopened
                .content_reservation_roots()
                .expect("read recovered reservation")
                .reservation_count,
            1
        );
        let reconciled = reopened
            .reconcile_content_reservations_with_catalog(
                &BTreeSet::new(),
                &BTreeSet::new(),
                ContentStoreReservationReconciliationOptions::bounded(
                    reserved_at_unix_nanos.saturating_add(1),
                    Duration::ZERO,
                    8,
                )
                .with_catalog_generation("catalog-generation-after-crash")
                .with_catalog_scan_cut(Some(reopened.begin_content_root_catalog_scan().unwrap())),
            )
            .expect("reject abandoned reservation from complete catalog");
        assert_eq!(reconciled.examined_reservation_count, 1);
        assert_eq!(reconciled.released_rejected_count, 1);
        assert_eq!(
            reopened
                .content_reservation_roots()
                .expect("reservation removed")
                .reservation_count,
            0
        );

        cleanup(&root);
    }

    #[test]
    fn bounded_catalog_reconciliation_covers_rejects_and_honors_grace() {
        let root = temp_root("bounded_catalog_reconciliation");
        let store = LocalContentStore::new(&root).expect("create store");
        let covered = store
            .put_object_version_blob_only("a-covered", "v1", b"covered")
            .expect("put covered reservation");
        store
            .put_object_version_blob_only("b-partial", "v1", b"partial")
            .expect("put partial reservation");
        store
            .put_object_version_blob_only("c-absent", "v1", b"absent")
            .expect("put absent reservation");
        let roots = store
            .content_reservation_roots()
            .expect("read reservations");
        let catalog_generated_at_unix_nanos = roots
            .reservations
            .iter()
            .map(|reservation| reservation.reserved_at_unix_nanos)
            .max()
            .expect("reservation timestamp")
            .saturating_add(1);
        let catalog_versions = BTreeSet::from([
            ("a-covered".to_owned(), "v1".to_owned()),
            ("b-partial".to_owned(), "v1".to_owned()),
        ]);
        let catalog_hashes = BTreeSet::from([covered.content_hash]);

        let first = store
            .reconcile_content_reservations_with_catalog(
                &catalog_versions,
                &catalog_hashes,
                ContentStoreReservationReconciliationOptions::bounded(
                    catalog_generated_at_unix_nanos,
                    Duration::from_nanos(1),
                    2,
                )
                .with_catalog_generation("catalog-generation-1")
                .with_catalog_scan_cut(Some(store.begin_content_root_catalog_scan().unwrap())),
            )
            .expect("reconcile first bounded page");
        assert_eq!(first.examined_reservation_count, 2);
        assert_eq!(first.released_catalog_covered_count, 2);
        assert!(first.has_more);
        assert!(first.next_cursor.is_some());
        assert!(
            store
                .content_reservation_reconciliation_cursor_path()
                .is_file()
        );

        let second = store
            .reconcile_content_reservations_with_catalog(
                &catalog_versions,
                &catalog_hashes,
                ContentStoreReservationReconciliationOptions::bounded(
                    catalog_generated_at_unix_nanos,
                    Duration::from_nanos(1),
                    2,
                )
                .with_catalog_generation("catalog-generation-1")
                .with_catalog_scan_cut(Some(store.begin_content_root_catalog_scan().unwrap())),
            )
            .expect("reconcile second bounded page");
        assert_eq!(second.examined_reservation_count, 1);
        assert_eq!(second.retained_rejection_grace_count, 1);
        assert!(!second.has_more);
        assert!(
            !store
                .content_reservation_reconciliation_cursor_path()
                .exists()
        );

        let final_pass = store
            .reconcile_content_reservations_with_catalog(
                &catalog_versions,
                &catalog_hashes,
                ContentStoreReservationReconciliationOptions::bounded(
                    u64::MAX,
                    Duration::from_nanos(1),
                    8,
                )
                .with_catalog_generation("catalog-generation-1")
                .with_catalog_scan_cut(Some(store.begin_content_root_catalog_scan().unwrap())),
            )
            .expect("reject abandoned reservation");
        assert_eq!(final_pass.released_rejected_count, 1);
        let remaining = store
            .content_reservation_roots()
            .expect("read remaining reservations");
        assert_eq!(remaining.reservation_count, 0);

        cleanup(&root);
    }

    #[test]
    fn catalog_hash_coverage_does_not_release_an_uncovered_version_reservation() {
        let root = temp_root("catalog_hash_does_not_cover_reserved_version");
        let store = LocalContentStore::new(&root).expect("create store");
        let reserved = store
            .put_object_version_blob_only("reserved-object", "v1", b"deduplicated bytes")
            .expect("put reserved content");
        let roots = store.content_reservation_roots().expect("read reservation");
        let reserved_at_unix_nanos = roots.reservations[0].reserved_at_unix_nanos;
        let unrelated_catalog_versions =
            BTreeSet::from([("other-object".to_owned(), "v1".to_owned())]);
        let catalog_hashes = BTreeSet::from([reserved.content_hash.clone()]);

        let hash_only = store
            .reconcile_content_reservations_with_catalog(
                &unrelated_catalog_versions,
                &catalog_hashes,
                ContentStoreReservationReconciliationOptions::bounded(
                    reserved_at_unix_nanos.saturating_add(1),
                    Duration::from_secs(60),
                    8,
                )
                .with_catalog_generation("catalog-generation-hash-only")
                .with_catalog_scan_cut(Some(store.begin_content_root_catalog_scan().unwrap())),
            )
            .expect("reconcile hash-only catalog coverage");
        assert_eq!(hash_only.examined_reservation_count, 1);
        assert_eq!(hash_only.released_catalog_covered_count, 0);
        assert_eq!(hash_only.retained_rejection_grace_count, 1);
        assert_eq!(
            store
                .content_reservation_roots()
                .expect("reservation remains")
                .reservation_count,
            1
        );

        let exact_catalog_versions =
            BTreeSet::from([("reserved-object".to_owned(), "v1".to_owned())]);
        let exact = store
            .reconcile_content_reservations_with_catalog(
                &exact_catalog_versions,
                &catalog_hashes,
                ContentStoreReservationReconciliationOptions::bounded(
                    reserved_at_unix_nanos,
                    Duration::from_secs(60),
                    8,
                )
                .with_catalog_generation("catalog-generation-exact-version")
                .with_catalog_scan_cut(Some(store.begin_content_root_catalog_scan().unwrap())),
            )
            .expect("reconcile exact version coverage");
        assert_eq!(exact.released_catalog_covered_count, 1);
        assert_eq!(
            store
                .content_reservation_roots()
                .expect("reservation released")
                .reservation_count,
            0
        );

        cleanup(&root);
    }

    #[test]
    fn existing_manifest_publication_retries_reestablish_a_reservation() {
        let root = temp_root("existing_manifest_retry_reservation");
        let store = LocalContentStore::new(&root).expect("create store");
        let bytes = b"idempotent publication";
        let manifest = store
            .put_object_version("retry-object", "v1", bytes)
            .expect("publish initial manifest");

        for retry in 0..4 {
            release_all_content_reservations(&store);
            assert_eq!(
                store
                    .content_reservation_roots()
                    .expect("reservation released before retry")
                    .reservation_count,
                0
            );
            let retried = match retry {
                0 => store
                    .put_object_version("retry-object", "v1", bytes)
                    .expect("retry ordinary publication"),
                1 => store
                    .put_object_version_blob_only("retry-object", "v1", bytes)
                    .expect("retry blob-only publication"),
                2 => {
                    store
                        .stage_bytes("retry-intent", bytes)
                        .expect("stage retry bytes");
                    store
                        .commit_staged("retry-intent", "retry-object", "v1")
                        .expect("retry staged publication")
                }
                3 => store
                    .put_blob_only_manifest(
                        "retry-object",
                        "v1",
                        &manifest.content_hash,
                        manifest.size_bytes,
                    )
                    .expect("retry blob-only manifest publication"),
                _ => unreachable!(),
            };
            assert_eq!(retried, manifest);
            let roots = store
                .content_reservation_roots()
                .expect("read retry reservation");
            assert_eq!(roots.reservation_count, 1);
            assert_eq!(roots.reservations[0].identity.object_id, "retry-object");
            assert_eq!(roots.reservations[0].identity.version, "v1");
            assert_eq!(
                roots.reservations[0].identity.content_hash,
                manifest.content_hash
            );
        }

        release_all_content_reservations(&store);
        let batch = store
            .put_object_version_blobs_only([("retry-object", "v1", bytes.as_slice())])
            .expect("retry batched blob-only publication");
        assert_eq!(batch, vec![manifest]);
        assert_eq!(
            store
                .content_reservation_roots()
                .expect("read batched retry reservation")
                .reservation_count,
            1
        );

        cleanup(&root);
    }

    #[test]
    fn reservation_reconciliation_cursor_survives_restart_and_prevents_starvation() {
        let root = temp_root("reservation_reconciliation_cursor_restart");
        let store = LocalContentStore::new(&root).expect("create store");
        for (object_id, bytes) in [
            ("a-retained", b"retained-a".as_slice()),
            ("b-retained", b"retained-b".as_slice()),
            ("y-covered", b"covered-y".as_slice()),
            ("z-covered", b"covered-z".as_slice()),
        ] {
            store
                .put_object_version_blob_only(object_id, "v1", bytes)
                .expect("put reserved content");
        }
        let roots = store
            .content_reservation_roots()
            .expect("read reservations");
        let catalog_generated_at_unix_nanos = roots
            .reservations
            .iter()
            .map(|reservation| reservation.reserved_at_unix_nanos)
            .max()
            .expect("reservation timestamp");
        let catalog_versions = BTreeSet::from([
            ("y-covered".to_owned(), "v1".to_owned()),
            ("z-covered".to_owned(), "v1".to_owned()),
        ]);
        let generation = "catalog-generation-starvation";

        let first = store
            .reconcile_content_reservations_with_catalog(
                &catalog_versions,
                &BTreeSet::new(),
                ContentStoreReservationReconciliationOptions::bounded(
                    catalog_generated_at_unix_nanos,
                    Duration::from_secs(60),
                    2,
                )
                .with_catalog_generation(generation)
                .with_catalog_scan_cut(Some(store.begin_content_root_catalog_scan().unwrap())),
            )
            .expect("reconcile retained prefix");
        assert_eq!(first.examined_reservation_count, 2);
        assert_eq!(first.retained_rejection_grace_count, 2);
        assert_eq!(first.released_catalog_covered_count, 0);
        assert_eq!(first.remaining_after_batch_count, 2);
        assert!(first.has_more);
        assert_eq!(
            first
                .next_cursor
                .as_ref()
                .map(|identity| identity.object_id.as_str()),
            Some("b-retained")
        );
        assert!(
            store
                .content_reservation_reconciliation_cursor_path()
                .is_file()
        );
        drop(store);

        let reopened = LocalContentStore::new(&root).expect("reopen store");
        let second = reopened
            .reconcile_content_reservations_with_catalog(
                &catalog_versions,
                &BTreeSet::new(),
                ContentStoreReservationReconciliationOptions::bounded(
                    catalog_generated_at_unix_nanos,
                    Duration::from_secs(60),
                    2,
                )
                .with_catalog_generation(generation)
                .with_catalog_scan_cut(Some(reopened.begin_content_root_catalog_scan().unwrap())),
            )
            .expect("reconcile suffix after restart");
        assert_eq!(second.examined_reservation_count, 2);
        assert_eq!(second.released_catalog_covered_count, 2);
        assert_eq!(second.remaining_after_batch_count, 0);
        assert!(!second.has_more);
        assert!(second.next_cursor.is_none());
        assert!(
            !reopened
                .content_reservation_reconciliation_cursor_path()
                .exists()
        );

        let after_completion = reopened
            .reconcile_content_reservations_with_catalog(
                &catalog_versions,
                &BTreeSet::new(),
                ContentStoreReservationReconciliationOptions::bounded(
                    catalog_generated_at_unix_nanos,
                    Duration::from_secs(60),
                    1,
                )
                .with_catalog_generation(generation)
                .with_catalog_scan_cut(Some(reopened.begin_content_root_catalog_scan().unwrap())),
            )
            .expect("restart completed scan from beginning");
        assert_eq!(after_completion.examined_reservation_count, 1);
        assert_eq!(after_completion.retained_rejection_grace_count, 1);
        assert_eq!(
            after_completion
                .next_cursor
                .as_ref()
                .map(|identity| identity.object_id.as_str()),
            Some("a-retained")
        );

        cleanup(&root);
    }

    #[test]
    fn reservation_reconciliation_cursor_resets_for_new_catalog_generation() {
        let root = temp_root("reservation_reconciliation_cursor_generation");
        let store = LocalContentStore::new(&root).expect("create store");
        store
            .put_object_version_blob_only("a-retained", "v1", b"retained")
            .expect("put retained reservation");
        store
            .put_object_version_blob_only("b-covered", "v1", b"covered")
            .expect("put covered reservation");
        let roots = store
            .content_reservation_roots()
            .expect("read reservations");
        let catalog_generated_at_unix_nanos = roots
            .reservations
            .iter()
            .map(|reservation| reservation.reserved_at_unix_nanos)
            .max()
            .expect("reservation timestamp");

        let first = store
            .reconcile_content_reservations_with_catalog(
                &BTreeSet::new(),
                &BTreeSet::new(),
                ContentStoreReservationReconciliationOptions::bounded(
                    catalog_generated_at_unix_nanos,
                    Duration::from_secs(60),
                    1,
                )
                .with_catalog_generation("catalog-generation-1")
                .with_catalog_scan_cut(Some(store.begin_content_root_catalog_scan().unwrap())),
            )
            .expect("start first catalog generation");
        assert_eq!(first.retained_rejection_grace_count, 1);
        assert_eq!(
            first
                .next_cursor
                .as_ref()
                .map(|identity| identity.object_id.as_str()),
            Some("a-retained")
        );

        let catalog_versions = BTreeSet::from([("b-covered".to_owned(), "v1".to_owned())]);
        let reset = store
            .reconcile_content_reservations_with_catalog(
                &catalog_versions,
                &BTreeSet::new(),
                ContentStoreReservationReconciliationOptions::bounded(
                    catalog_generated_at_unix_nanos,
                    Duration::from_secs(60),
                    1,
                )
                .with_catalog_generation("catalog-generation-2")
                .with_catalog_scan_cut(Some(store.begin_content_root_catalog_scan().unwrap())),
            )
            .expect("restart for new catalog generation");
        assert_eq!(reset.examined_reservation_count, 1);
        assert_eq!(reset.retained_rejection_grace_count, 1);
        assert_eq!(reset.released_catalog_covered_count, 0);
        assert_eq!(
            reset
                .next_cursor
                .as_ref()
                .map(|identity| identity.object_id.as_str()),
            Some("a-retained")
        );
        let persisted = store
            .read_content_reservation_reconciliation_cursor()
            .expect("read cursor")
            .expect("cursor remains");
        assert_eq!(persisted.catalog_generation, "catalog-generation-2");

        let suffix = store
            .reconcile_content_reservations_with_catalog(
                &catalog_versions,
                &BTreeSet::new(),
                ContentStoreReservationReconciliationOptions::bounded(
                    catalog_generated_at_unix_nanos,
                    Duration::from_secs(60),
                    1,
                )
                .with_catalog_generation("catalog-generation-2")
                .with_catalog_scan_cut(Some(store.begin_content_root_catalog_scan().unwrap())),
            )
            .expect("continue new catalog generation");
        assert_eq!(suffix.examined_reservation_count, 1);
        assert_eq!(suffix.released_catalog_covered_count, 1);
        assert!(!suffix.has_more);
        assert!(
            !store
                .content_reservation_reconciliation_cursor_path()
                .exists()
        );

        cleanup(&root);
    }

    #[test]
    fn manifest_first_absence_survives_restart_before_collection() {
        let root = temp_root("manifest_first_absence_survives_restart");
        let store = LocalContentStore::new(&root).expect("create store");
        let old = store
            .put_object_version("absence-object", "version_1", b"old")
            .expect("put old version");
        store
            .put_object_version("absence-object", "version_2", b"latest")
            .expect("put latest version");
        release_all_content_reservations(&store);

        store
            .collect_versions(
                ContentStoreVersionGcOptions::delete_eligible(Duration::ZERO, 1)
                    .with_min_absence_age(Duration::ZERO),
            )
            .expect_err("destructive collection without catalog authority must fail");
        assert!(
            !store
                .manifest_absence_observation_path(&ContentStoreReservationIdentity {
                    object_id: "absence-object".to_string(),
                    version: "version_1".to_string(),
                    content_hash: old.content_hash.clone(),
                })
                .exists()
        );

        let first = store
            .collect_versions(
                ContentStoreVersionGcOptions::delete_eligible(Duration::ZERO, 1)
                    .with_min_absence_age(Duration::ZERO)
                    .with_catalog_observation("catalog-generation-1", 1),
            )
            .expect("persist first manifest absence");
        assert_eq!(first.newly_observed_absent_version_count, 1);
        assert_eq!(first.persisted_absence_observation_count, 1);
        assert_eq!(first.retained_absence_grace_version_count, 1);
        assert_eq!(first.deletion_ready_version_count, 0);
        assert_eq!(first.deleted_version_count, 0);
        assert!(store.read_manifest("absence-object", "version_1").is_ok());
        drop(store);

        let reopened = LocalContentStore::new(&root).expect("reopen store");
        let stale_catalog = reopened
            .collect_versions(
                ContentStoreVersionGcOptions::delete_eligible(Duration::ZERO, 1)
                    .with_min_absence_age(Duration::ZERO)
                    .with_catalog_observation("catalog-generation-1", u64::MAX),
            )
            .expect("retain under the same catalog generation");
        assert_eq!(stale_catalog.deleted_version_count, 0);
        assert_eq!(
            stale_catalog.retained_same_or_stale_catalog_version_count,
            1
        );
        assert_eq!(stale_catalog.deletion_ready_version_count, 0);
        let second = reopened
            .collect_versions(
                ContentStoreVersionGcOptions::delete_eligible(Duration::ZERO, 1)
                    .with_min_absence_age(Duration::ZERO)
                    .with_catalog_observation("catalog-generation-2", u64::MAX),
            )
            .expect("collect after durable absence");
        assert_eq!(second.newly_observed_absent_version_count, 0);
        assert_eq!(second.deletion_ready_version_count, 1);
        assert_eq!(second.deleted_version_count, 1);
        assert!(
            reopened
                .read_manifest("absence-object", "version_1")
                .is_err()
        );
        assert!(
            reopened
                .has_blob(&old.content_hash)
                .expect("old blob remains")
        );

        let first_blob_gc = reopened
            .collect_garbage(delete_eligible_orphan_gc_options(Duration::ZERO))
            .expect("observe newly orphaned blob");
        assert_eq!(first_blob_gc.newly_observed_orphan_count, 1);
        assert_eq!(first_blob_gc.deleted_orphan_count, 0);
        let second_blob_gc = reopened
            .collect_garbage(delete_eligible_orphan_gc_options(Duration::ZERO))
            .expect("collect observed blob");
        assert_eq!(second_blob_gc.deleted_orphan_count, 1);
        assert!(!reopened.has_blob(&old.content_hash).expect("old blob gone"));

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
        assert_eq!(
            store.content_reservation_roots().unwrap().reservation_count,
            0,
            "a hash-rejected private stage must not reserve content"
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
    fn manifest_storage_candidate_probe_does_not_hash_payload_bytes() {
        let root = temp_root("manifest_storage_candidate_non_hashing");
        let store = LocalContentStore::new(&root).expect("create store");
        let bytes = vec![0x2a; INLINE_BLOB_SMALL_LIMIT_BYTES as usize + 1];
        let manifest = store
            .put_object_version("object-candidate", "v1", &bytes)
            .expect("put object version");
        let blob_path = store.blob_path(&manifest.content_hash).expect("blob path");
        std::fs::write(&blob_path, vec![0x5a; bytes.len()]).expect("corrupt same-size blob");

        assert!(
            store
                .manifest_storage_candidate_available(&manifest)
                .expect("candidate probe"),
            "the bounded availability probe should check addressability and size only"
        );
        assert!(
            store
                .manifest_content_available(&manifest)
                .expect("manifest availability"),
            "manifest availability checks length without rehashing content"
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
    fn empty_blob_only_versions_are_intrinsic_without_storage_or_reservations() {
        let root = temp_root("empty_blob_only_versions_are_intrinsic");
        let store = LocalContentStore::new(&root).expect("create store");
        let empty_hash = content_hash(&[]);

        let manifests = store
            .put_object_version_blobs_only([
                ("empty-batch-a", "v1", [].as_slice()),
                ("empty-batch-b", "v1", [].as_slice()),
            ])
            .expect("put empty blob-only batch");
        let direct = store
            .put_object_version_blob_only("empty-direct", "v1", &[])
            .expect("put direct empty blob-only version");

        assert_eq!(manifests.len(), 2);
        assert!(
            manifests
                .iter()
                .all(|manifest| manifest.content_hash == empty_hash && manifest.size_bytes == 0)
        );
        assert_eq!(direct.content_hash, empty_hash);
        assert_eq!(direct.size_bytes, 0);
        assert_eq!(store.read_blob(&empty_hash).expect("read empty blob"), b"");
        assert_eq!(
            store
                .read_blob_range(&empty_hash, 0, usize::MAX)
                .expect("read empty blob range"),
            b""
        );
        let mut reader = store
            .open_blob_stream(&empty_hash)
            .expect("open empty blob stream");
        let mut streamed = Vec::new();
        reader
            .read_to_end(&mut streamed)
            .expect("stream empty blob");
        assert_eq!(reader.size_bytes(), 0);
        assert!(streamed.is_empty());
        assert!(store.has_blob(&empty_hash).expect("check empty blob"));
        assert!(
            store
                .blob_storage_candidate_exists(&empty_hash)
                .expect("check empty storage candidate")
        );
        assert!(
            !store
                .blob_path(&empty_hash)
                .expect("empty blob path")
                .exists(),
            "canonical empty content must not require a physical blob file"
        );
        assert!(
            store
                .inline_binding_view()
                .unwrap()
                .unwrap()
                .blobs
                .is_empty()
        );
        let roots = store
            .content_reservation_roots()
            .expect("read content reservations");
        assert_eq!(roots.reservation_count, 0);
        assert!(roots.reservations.is_empty());
        assert!(roots.content_hashes.is_empty());

        cleanup(&root);
    }

    #[test]
    fn put_object_version_blobs_only_deduplicates_same_batch_inline_hash() {
        let root = temp_root("put_object_version_blobs_only_deduplicates_same_batch_inline_hash");
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
        let view = store.inline_binding_view().unwrap().unwrap();
        assert_eq!(view.blobs.len(), 1);
        assert_eq!(store.stats().unwrap().blob_count, 1);

        cleanup(&root);
    }

    #[test]
    fn no_quota_status_scan_preserves_streamed_durable_blob_only_branch() {
        let root = temp_root("no_quota_status_scan_preserves_streamed_durable_blob_only_branch");
        let store = LocalContentStore::new(&root).expect("create store");
        let bytes = vec![0x71; STREAMED_BLOB_ONLY_MIN_BYTES + 4096];

        let empty = store.stats().expect("status materializes derived snapshot");
        assert_eq!(empty, ContentStoreStats::default());
        assert!(
            !store.stats_snapshot_path().exists(),
            "a no-quota status scan must not persist derived state"
        );
        assert!(!store.tracks_stats_deltas());
        assert!(store.uses_streamed_blob_only_path(bytes.len()));

        let manifest = store
            .put_object_version_blob_only("object-blob-only-large", "v1", &bytes)
            .expect("put blob-only large object");

        assert!(
            !store.stats_snapshot_path().exists(),
            "a no-quota streamed write must not persist synchronous stats state"
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
        let bytes = vec![0x73; INLINE_BLOB_SMALL_LIMIT_BYTES as usize + 4096];
        let expected_hash = content_hash(&bytes);
        let bucket = store
            .blob_path(&expected_hash)
            .unwrap()
            .parent()
            .unwrap()
            .to_owned();
        assert!(
            !bucket.exists(),
            "new content root has not published this hash bucket"
        );

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

        assert!(bucket.is_dir());
        drop(store);
        let reopened = LocalContentStore::new(&root).expect("reopen actual current content root");
        assert_eq!(reopened.read_blob(&expected_hash).unwrap(), bytes);
        drop(reopened);
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
    fn open_blob_reader_streams_without_rehashing_bytes() {
        let root = temp_root("open_blob_reader_validates_hash_and_streams_bytes");
        let store = LocalContentStore::new(&root).expect("create store");
        let original = vec![b'a'; INLINE_BLOB_SMALL_LIMIT_BYTES as usize + 1];
        let corrupt = vec![b'b'; original.len()];
        let manifest = store
            .put_object_version("object-reader", "v1", &original)
            .expect("put object version");

        let mut reader = store
            .open_blob_reader(&manifest.content_hash)
            .expect("open reader");
        assert_eq!(reader.size_bytes(), manifest.size_bytes);
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).expect("read blob reader");
        assert_eq!(bytes, original);

        let blob_path = store.blob_path(&manifest.content_hash).expect("blob path");
        assert!(blob_path.is_file(), "normal producer selected a loose blob");
        write_file_atomic(&blob_path, &corrupt).expect("corrupt blob");
        let mut reader = store
            .open_blob_reader(&manifest.content_hash)
            .expect("read without rehashing");
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).expect("read bytes");
        assert_eq!(bytes, corrupt);
        assert!(
            store
                .scrub()
                .expect("scrub")
                .issues
                .iter()
                .any(|issue| matches!(issue, ContentStoreScrubIssue::CorruptBlob { .. }))
        );

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

        // Reads leave byte verification to scrub. Invalid identities still error.
        assert!(matches!(
            store.open_blob_stream("blake3:not-a-real-hash"),
            Err(ContentStoreError::InvalidHash(_))
        ));

        cleanup(&root);
    }

    #[test]
    fn read_missing_blob_does_not_create_storage() {
        let root = temp_root("read-missing-blob");
        let store = LocalContentStore::new(&root).expect("create store");
        let missing_hash = content_hash(b"missing read-only packed blob lookup");
        let lock_path = root.join("blob-packs/pack.lock");

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

        let manifest_validations_before = store
            .stream_manifest_validation_count
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
                .stream_manifest_validation_count
                .load(Ordering::Relaxed),
            manifest_validations_before + 1
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
                .stream_manifest_validation_count
                .load(Ordering::Relaxed),
            manifest_validations_before + 1,
            "repeated ranges should reuse the immutable validated manifest"
        );

        cleanup(&root);
    }

    #[test]
    fn no_quota_stats_are_byte_read_only_and_rescan_durable_content() {
        let root = temp_root("no_quota_stats_are_byte_read_only_and_rescan_durable_content");
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

        let initial = store.stats().expect("read derived stats");
        assert_eq!(initial.blob_count, 1);
        assert_eq!(initial.manifest_count, 1);
        assert!(
            !store.stats_snapshot_path().exists(),
            "an observational stats read must not persist a derived snapshot"
        );
        assert!(
            !root.join(CONTENT_STORE_STATS_LOCK_FILE).exists(),
            "an observational stats read must not create a snapshot lock"
        );
        assert!(
            !store.tracks_stats_deltas(),
            "derived status state must not become synchronous write authority"
        );

        store
            .put_object_version("object-lazy-stats", "v2", b"lazy stats bravo")
            .expect("put second object");
        let updated = store.stats().expect("derived stats rescan durable content");
        assert_eq!(updated.blob_count, 2);
        assert_eq!(updated.manifest_count, 2);
        assert_eq!(
            updated.logical_object_bytes,
            b"lazy stats alpha".len() as u64 + b"lazy stats bravo".len() as u64
        );
        assert!(!store.stats_snapshot_path().exists());
        assert!(!root.join(CONTENT_STORE_STATS_LOCK_FILE).exists());

        cleanup(&root);
    }

    #[test]
    fn no_quota_reopen_ignores_legacy_stale_stats_snapshot() {
        let root = temp_root("no_quota_reopen_ignores_legacy_stale_stats_snapshot");
        let store = LocalContentStore::new(&root).expect("create store");
        store
            .put_object_version("object-legacy-a", "v1", b"legacy alpha")
            .expect("put first object");
        let legacy_snapshot = ContentStoreStats {
            blob_count: 41,
            manifest_count: 43,
            ..ContentStoreStats::default()
        };
        store
            .replace_stats_snapshot(&legacy_snapshot)
            .expect("write legacy snapshot fixture");
        let stale_snapshot = read_file(&store.stats_snapshot_path()).expect("read legacy snapshot");

        let initial = store.stats().expect("ignore legacy snapshot");
        assert_eq!(initial.blob_count, 1);
        assert_eq!(initial.manifest_count, 1);
        assert_eq!(
            read_file(&store.stats_snapshot_path()).expect("read untouched legacy snapshot"),
            stale_snapshot,
            "a no-quota stats read must not rewrite legacy derived state"
        );

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
        let missing_bytes = vec![b'm'; INLINE_BLOB_SMALL_LIMIT_BYTES as usize + 1];
        let corrupt_bytes = vec![b'c'; INLINE_BLOB_SMALL_LIMIT_BYTES as usize + 2];
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
        store
            .replace_stats_snapshot(&initial)
            .expect("persist legacy pre-quota snapshot fixture");
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
    fn high_count_gc_batches_directory_syncs_and_inline_tombstones() {
        const VERSION_COUNT: usize = VERSION_GC_DELETE_BATCH_SIZE + 2;
        const HASH_SHARD: &str = "2a";
        let root = temp_root("high_count_gc_batches_durability");
        let store = LocalContentStore::new(&root)
            .expect("create store")
            .with_quota(ContentStoreQuota::no_limits());

        let mut payloads = Vec::with_capacity(VERSION_COUNT);
        for nonce in 0_u64.. {
            let mut bytes = vec![0_u8; 128];
            bytes[..8].copy_from_slice(&nonce.to_le_bytes());
            let hash = content_hash(&bytes);
            if &hash[HASH_PREFIX.len()..HASH_PREFIX.len() + 2] == HASH_SHARD {
                payloads.push(bytes);
                if payloads.len() == VERSION_COUNT {
                    break;
                }
            }
        }
        let versions = (0..VERSION_COUNT)
            .map(|index| format!("version_{index:04}"))
            .collect::<Vec<_>>();
        store
            .put_object_versions(
                versions.iter().zip(&payloads).map(|(version, bytes)| {
                    ("batched-gc-object", version.as_str(), bytes.as_slice())
                }),
            )
            .expect("write packed version churn fixture");
        release_all_content_reservations(&store);
        observe_collectible_versions(&store, 1);

        let version_gc = store
            .collect_versions(
                ContentStoreVersionGcOptions::delete_eligible(Duration::ZERO, 1)
                    .with_catalog_observation("test-catalog-generation-2", u64::MAX),
            )
            .expect("batch version GC");
        assert_eq!(version_gc.deleted_version_count, (VERSION_COUNT - 1) as u64);
        assert_eq!(version_gc.durable_delete_batch_count, 2);
        assert_eq!(
            version_gc.durable_directory_sync_count, 0,
            "inline tombstones create no loose manifest-directory unlink work"
        );
        assert_eq!(version_gc.stats_delta_persist_count, 1);
        let retired_bindings = store
            .inline_binding_view()
            .expect("actual inline tombstones")
            .unwrap();
        for version in &versions[..VERSION_COUNT - 1] {
            assert!(
                matches!(
                    retired_bindings
                        .versions
                        .get(&("batched-gc-object".to_string(), version.clone())),
                    Some(InlineVersionState::Deleted { .. })
                ),
                "every collected version has its durable tombstone"
            );
            assert!(content_error_is_not_found(
                &store
                    .read_manifest("batched-gc-object", version)
                    .unwrap_err()
            ));
        }

        let observed = store
            .collect_garbage(delete_eligible_orphan_gc_options(Duration::ZERO))
            .expect("batch orphan observations");
        assert_eq!(
            observed.persisted_orphan_observation_count,
            (VERSION_COUNT - 1) as u64
        );
        assert_eq!(
            observed.orphan_observation_directory_sync_count, 3,
            "one shard, its new root, and the store root should be synced once each"
        );

        let collected = store
            .collect_garbage(delete_eligible_orphan_gc_options(Duration::ZERO))
            .expect("batch packed orphan collection");
        assert_eq!(collected.deleted_orphan_count, (VERSION_COUNT - 1) as u64);

        assert_eq!(
            collected.orphan_observation_directory_sync_count, 1,
            "removing the now-empty marker shard only dirties its parent"
        );
        assert_eq!(collected.stats_delta_persist_count, 1);
        let usage = store.quota_usage().expect("post-GC quota usage");
        assert_eq!(usage.stats.manifest_count, 1);
        assert_eq!(usage.stats.blob_count, 1);

        drop(store);
        let reopened = LocalContentStore::new(&root).expect("reopen batched GC store");
        for version in &versions[..VERSION_COUNT - 1] {
            assert!(matches!(
                reopened
                    .inline_version_state("batched-gc-object", version)
                    .expect("reopen exact tombstone"),
                Some(InlineVersionState::Deleted { .. })
            ));
            assert!(content_error_is_not_found(
                &reopened
                    .read_manifest("batched-gc-object", version)
                    .unwrap_err()
            ));
        }
        assert_eq!(
            reopened
                .manifests_for_object("batched-gc-object")
                .expect("read retained manifests")
                .len(),
            1
        );
        assert!(
            reopened
                .scrub()
                .expect("scrub reopened batched GC store")
                .is_healthy()
        );

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
            .collect_garbage(delete_eligible_orphan_gc_options(Duration::from_secs(
                24 * 60 * 60,
            )))
            .expect("retention gc");
        assert_eq!(retained.deleted_orphan_count, 0);
        assert_eq!(retained.eligible_orphan_count, 0);
        assert_eq!(retained.retained_young_orphan_count, 2);
        assert_eq!(retained.newly_observed_orphan_count, 2);
        assert_eq!(
            retained.retained_young_orphan_bytes,
            orphan_a_bytes.len() as u64 + orphan_b_bytes.len() as u64
        );
        assert_eq!(
            retained.newly_observed_orphan_bytes,
            orphan_a_bytes.len() as u64 + orphan_b_bytes.len() as u64
        );
        assert!(retained.next_eligible_at_unix_nanos.is_some());
        assert!(store.has_blob(&orphan_a_hash).expect("orphan a retained"));
        assert!(store.has_blob(&orphan_b_hash).expect("orphan b retained"));

        let limited = store
            .collect_garbage(
                delete_eligible_orphan_gc_options(Duration::from_nanos(0)).with_max_deletions(1),
            )
            .expect("limited gc");
        assert_eq!(limited.blocked_by_scrub_issue_count, 0);
        assert_eq!(limited.newly_observed_orphan_count, 0);
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
            .collect_garbage(delete_eligible_orphan_gc_options(Duration::from_nanos(0)))
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
    fn collect_garbage_retains_protected_loose_and_inline_orphans() {
        let root = temp_root("collect_garbage_retains_protected_orphans");
        let store = LocalContentStore::new(&root).expect("create store");
        let protected_packed = store
            .put_object_version("object-protected-packed", "version_1", b"protected packed")
            .expect("put protected packed version");
        assert!(
            store
                .inline_blob_entry(&protected_packed.content_hash)
                .expect("protected packed lookup")
                .is_some(),
            "fixture must exercise packed-orphan protection"
        );
        store
            .put_object_version("object-protected-packed", "version_2", b"live packed")
            .expect("put live packed version");
        release_all_content_reservations(&store);
        observe_collectible_versions(&store, 1);
        let version_gc = store
            .collect_versions(
                ContentStoreVersionGcOptions::delete_eligible(Duration::from_nanos(0), 1)
                    .with_catalog_observation("test-catalog-generation-2", u64::MAX),
            )
            .expect("make old packed blob orphaned");
        assert_eq!(version_gc.deleted_version_count, 1);

        let protected_loose_bytes = b"protected loose orphan";
        let protected_loose_hash = content_hash(protected_loose_bytes);
        write_file_atomic(
            &store
                .blob_path(&protected_loose_hash)
                .expect("protected loose path"),
            protected_loose_bytes,
        )
        .expect("write protected loose orphan");
        let unprotected_bytes = b"unprotected loose orphan";
        let unprotected_hash = content_hash(unprotected_bytes);
        write_file_atomic(
            &store
                .blob_path(&unprotected_hash)
                .expect("unprotected loose path"),
            unprotected_bytes,
        )
        .expect("write unprotected loose orphan");

        let options = delete_eligible_orphan_gc_options(Duration::from_nanos(0))
            .with_protected_content_hash(protected_packed.content_hash.clone())
            .with_protected_content_hashes([protected_loose_hash.clone()]);
        let observed = store
            .collect_garbage(options.clone())
            .expect("observe unprotected orphan");

        assert_eq!(observed.retained_protected_orphan_count, 2);
        assert_eq!(
            observed.retained_protected_orphan_bytes,
            protected_packed.size_bytes + protected_loose_bytes.len() as u64
        );
        assert_eq!(observed.retained_young_orphan_count, 1);
        assert_eq!(observed.newly_observed_orphan_count, 1);
        assert_eq!(
            observed.newly_observed_orphan_bytes,
            unprotected_bytes.len() as u64
        );
        assert_eq!(observed.eligible_orphan_count, 0);
        assert_eq!(observed.deleted_orphan_count, 0);
        assert!(observed.next_eligible_at_unix_nanos.is_some());
        assert!(
            store
                .orphan_observation_path(&unprotected_hash)
                .expect("unprotected observation path")
                .is_file()
        );
        assert!(
            !store
                .orphan_observation_path(&protected_packed.content_hash)
                .expect("protected packed observation path")
                .exists()
        );
        assert!(
            !store
                .orphan_observation_path(&protected_loose_hash)
                .expect("protected loose observation path")
                .exists()
        );

        let report = store
            .collect_garbage(
                delete_eligible_orphan_gc_options(Duration::from_nanos(0))
                    .with_protected_content_hash(protected_packed.content_hash.clone())
                    .with_protected_content_hashes([protected_loose_hash.clone()]),
            )
            .expect("collect observed unprotected orphan");
        assert_eq!(report.retained_protected_orphan_count, 2);
        assert_eq!(report.newly_observed_orphan_count, 0);
        assert_eq!(report.eligible_orphan_count, 1);
        assert_eq!(report.deleted_orphan_count, 1);
        assert!(
            store
                .has_blob(&protected_packed.content_hash)
                .expect("protected packed retained")
        );
        assert!(
            store
                .has_blob(&protected_loose_hash)
                .expect("protected loose retained")
        );
        assert!(
            !store
                .has_blob(&unprotected_hash)
                .expect("unprotected loose collected")
        );

        cleanup(&root);
    }

    #[test]
    fn orphan_projection_distinguishes_unobserved_from_clocked_grace() {
        let root = temp_root("orphan_projection_distinguishes_observation");
        let store = LocalContentStore::new(&root).expect("create store");
        let orphan_bytes = b"reactive observation orphan";
        let orphan_hash = content_hash(orphan_bytes);
        write_file_atomic(
            &store.blob_path(&orphan_hash).expect("orphan path"),
            orphan_bytes,
        )
        .expect("write orphan");
        let min_age = Duration::from_secs(60 * 60);

        let projection = store
            .collect_garbage(dry_run_orphan_gc_options(min_age))
            .expect("project missing observation");
        assert_eq!(projection.newly_observed_orphan_count, 1);
        assert_eq!(
            projection.newly_observed_orphan_bytes,
            orphan_bytes.len() as u64
        );
        assert!(
            !store
                .orphan_observation_path(&orphan_hash)
                .expect("observation path")
                .exists()
        );

        let observation = store
            .collect_garbage(delete_eligible_orphan_gc_options(min_age))
            .expect("persist observation");
        assert_eq!(observation.newly_observed_orphan_count, 1);
        assert_eq!(observation.retained_young_orphan_count, 1);
        assert!(
            store
                .orphan_observation_path(&orphan_hash)
                .expect("persisted observation path")
                .is_file()
        );

        let clocked = store
            .collect_garbage(dry_run_orphan_gc_options(min_age))
            .expect("project persisted grace clock");
        assert_eq!(clocked.newly_observed_orphan_count, 0);
        assert_eq!(clocked.retained_young_orphan_count, 1);
        assert_eq!(
            clocked.next_eligible_at_unix_nanos,
            observation.next_eligible_at_unix_nanos
        );

        cleanup(&root);
    }

    #[test]
    fn orphan_observation_writes_respect_the_gc_work_limit() {
        let root = temp_root("orphan_observation_writes_respect_limit");
        let store = LocalContentStore::new(&root).expect("create store");
        let blobs = [
            b"bounded orphan observation a".as_slice(),
            b"bounded orphan observation b".as_slice(),
            b"bounded orphan observation c".as_slice(),
        ];
        let hashes = blobs
            .iter()
            .map(|bytes| {
                let hash = content_hash(bytes);
                write_file_atomic(&store.blob_path(&hash).expect("orphan blob path"), bytes)
                    .expect("write orphan blob");
                hash
            })
            .collect::<Vec<_>>();
        let options =
            delete_eligible_orphan_gc_options(Duration::from_secs(60 * 60)).with_max_deletions(1);

        let first = store
            .collect_garbage(options.clone())
            .expect("bounded first observation pass");
        assert_eq!(first.newly_observed_orphan_count, 3);
        assert_eq!(first.persisted_orphan_observation_count, 1);
        assert_eq!(first.retained_limit_orphan_observation_count, 2);
        assert!(first.max_deletions_reached);
        assert_eq!(
            hashes
                .iter()
                .filter(|hash| {
                    store
                        .orphan_observation_path(hash)
                        .expect("orphan observation path")
                        .is_file()
                })
                .count(),
            1,
            "one pass must persist at most max_deletions observation markers"
        );

        let second = store
            .collect_garbage(options)
            .expect("bounded second observation pass");
        assert_eq!(second.newly_observed_orphan_count, 2);
        assert_eq!(second.persisted_orphan_observation_count, 1);
        assert_eq!(second.retained_limit_orphan_observation_count, 1);
        assert_eq!(
            hashes
                .iter()
                .filter(|hash| {
                    store
                        .orphan_observation_path(hash)
                        .expect("orphan observation path")
                        .is_file()
                })
                .count(),
            2
        );

        cleanup(&root);
    }

    #[test]
    fn orphan_observation_survives_restart_before_inline_blob_collection() {
        let root = temp_root("orphan_observation_survives_restart");
        let store = LocalContentStore::new(&root).expect("create store");
        let old = store
            .put_object_version("object-observed-packed", "version_1", b"old packed bytes")
            .expect("put old packed version");
        store
            .put_object_version("object-observed-packed", "version_2", b"new packed bytes")
            .expect("put new packed version");
        assert!(
            store
                .inline_blob_entry(&old.content_hash)
                .expect("old packed lookup")
                .is_some(),
            "fixture must exercise packed-orphan observations"
        );
        release_all_content_reservations(&store);
        observe_collectible_versions(&store, 1);
        store
            .collect_versions(
                ContentStoreVersionGcOptions::delete_eligible(Duration::from_nanos(0), 1)
                    .with_catalog_observation("test-catalog-generation-2", u64::MAX),
            )
            .expect("orphan old packed version");

        let first_pass = store
            .collect_garbage(delete_eligible_orphan_gc_options(Duration::from_nanos(0)))
            .expect("persist first observation");
        assert_eq!(first_pass.retained_young_orphan_count, 1);
        assert_eq!(first_pass.newly_observed_orphan_count, 1);
        assert_eq!(first_pass.newly_observed_orphan_bytes, old.size_bytes);
        assert_eq!(first_pass.eligible_orphan_count, 0);
        assert_eq!(first_pass.deleted_orphan_count, 0);
        let marker_path = store
            .orphan_observation_path(&old.content_hash)
            .expect("orphan observation path");
        assert!(marker_path.is_file());
        drop(store);

        let reopened = LocalContentStore::new(&root).expect("reopen store");
        let second_pass = reopened
            .collect_garbage(delete_eligible_orphan_gc_options(Duration::from_nanos(0)))
            .expect("collect after restart");
        assert_eq!(second_pass.retained_young_orphan_count, 0);
        assert_eq!(second_pass.newly_observed_orphan_count, 0);
        assert_eq!(second_pass.eligible_orphan_count, 1);
        assert_eq!(second_pass.deleted_orphan_count, 1);
        assert!(
            !reopened
                .has_blob(&old.content_hash)
                .expect("packed orphan removed")
        );
        assert!(
            !marker_path.exists(),
            "successful deletion must remove its first-observed marker"
        );

        cleanup(&root);
    }

    #[test]
    fn orphan_collection_requires_a_later_catalog_generation_after_restart() {
        let root = temp_root("orphan_collection_later_catalog_after_restart");
        let store = LocalContentStore::new(&root).expect("create store");
        let bytes = b"orphan behind catalog barrier";
        let hash = content_hash(bytes);
        write_file_atomic(&store.blob_path(&hash).expect("orphan path"), bytes)
            .expect("write orphan");

        let first = store
            .collect_garbage(
                ContentStoreGcOptions::delete_eligible(Duration::ZERO)
                    .with_catalog_observation("catalog-generation-1", u64::MAX - 2),
            )
            .expect("persist first catalog-bound observation");
        assert_eq!(first.persisted_orphan_observation_count, 1);
        assert_eq!(first.deleted_orphan_count, 0);
        drop(store);

        let reopened = LocalContentStore::new(&root).expect("reopen store");
        let same_generation = reopened
            .collect_garbage(
                ContentStoreGcOptions::delete_eligible(Duration::ZERO)
                    .with_catalog_observation("catalog-generation-1", u64::MAX - 1),
            )
            .expect("retain under same catalog generation");
        assert_eq!(
            same_generation.retained_same_or_stale_catalog_orphan_count,
            1
        );
        assert_eq!(same_generation.deleted_orphan_count, 0);
        assert!(reopened.has_blob(&hash).expect("orphan retained"));

        let later_generation = reopened
            .collect_garbage(
                ContentStoreGcOptions::delete_eligible(Duration::ZERO)
                    .with_catalog_observation("catalog-generation-2", u64::MAX),
            )
            .expect("collect under later catalog generation");
        assert_eq!(later_generation.eligible_orphan_count, 1);
        assert_eq!(later_generation.deleted_orphan_count, 1);
        assert!(!reopened.has_blob(&hash).expect("orphan collected"));

        cleanup(&root);
    }

    #[test]
    fn destructive_orphan_gc_without_catalog_observation_fails_before_mutation() {
        let root = temp_root("orphan_gc_missing_catalog_fails_closed");
        let store = LocalContentStore::new(&root).expect("create store");
        let bytes = b"unproven orphan";
        let hash = content_hash(bytes);
        write_file_atomic(&store.blob_path(&hash).expect("orphan path"), bytes)
            .expect("write orphan");
        let marker_path = store
            .orphan_observation_path(&hash)
            .expect("observation path");

        let error = store
            .collect_garbage(ContentStoreGcOptions::delete_eligible(Duration::ZERO))
            .expect_err("destructive GC without a catalog must fail");
        assert!(matches!(
            error,
            ContentStoreError::InvalidContentLayout { .. }
        ));
        assert!(store.has_blob(&hash).expect("orphan retained"));
        assert!(!marker_path.exists());

        let projection = store
            .collect_garbage(ContentStoreGcOptions::dry_run(Duration::ZERO))
            .expect("dry run without catalog remains non-mutating");
        assert_eq!(projection.newly_observed_orphan_count, 1);
        assert_eq!(projection.deleted_orphan_count, 0);
        assert!(store.has_blob(&hash).expect("dry-run retained orphan"));
        assert!(!marker_path.exists());

        cleanup(&root);
    }

    #[test]
    fn serialized_catalog_observations_cannot_authorize_destructive_collectors() {
        let root = temp_root("content_gc_serialized_catalog_observation");
        let store = LocalContentStore::new(&root).expect("create store");
        let orphan_bytes = b"unproven serialized orphan";
        let orphan_hash = content_hash(orphan_bytes);
        write_file_atomic(
            &store.blob_path(&orphan_hash).expect("orphan path"),
            orphan_bytes,
        )
        .expect("write orphan");
        store
            .collect_garbage(
                ContentStoreGcOptions::delete_eligible(Duration::ZERO)
                    .with_catalog_projection_observation("public-projection-generation", u64::MAX),
            )
            .expect_err("public projection fields must not authorize orphan deletion");
        let forged_orphan_options: ContentStoreGcOptions = serde_json::from_value(
            serde_json::to_value(
                ContentStoreGcOptions::delete_eligible(Duration::ZERO)
                    .with_catalog_observation("forged-catalog-generation", u64::MAX),
            )
            .expect("serialize orphan options"),
        )
        .expect("deserialize orphan options");
        store
            .collect_garbage(forged_orphan_options)
            .expect_err("serialized catalog fields must not authorize orphan deletion");
        assert!(store.has_blob(&orphan_hash).expect("orphan retained"));

        store
            .put_object_version("serialized-version-object", "v1", b"old version")
            .expect("put old version");
        store
            .put_object_version("serialized-version-object", "v2", b"latest version")
            .expect("put latest version");
        release_all_content_reservations(&store);
        store
            .collect_versions(
                ContentStoreVersionGcOptions::delete_eligible(Duration::ZERO, 1)
                    .with_catalog_projection_observation("public-projection-generation", u64::MAX),
            )
            .expect_err("public projection fields must not authorize manifest deletion");
        let forged_version_options: ContentStoreVersionGcOptions = serde_json::from_value(
            serde_json::to_value(
                ContentStoreVersionGcOptions::delete_eligible(Duration::ZERO, 1)
                    .with_catalog_observation("forged-catalog-generation", u64::MAX),
            )
            .expect("serialize version options"),
        )
        .expect("deserialize version options");
        store
            .collect_versions(forged_version_options)
            .expect_err("serialized catalog fields must not authorize manifest deletion");
        assert_eq!(
            store
                .read_manifest("serialized-version-object", "v1")
                .expect("unqualified collector leaves the actual binding live")
                .content_hash,
            content_hash(b"old version"),
        );
        assert!(
            !store.stats_snapshot_path().exists(),
            "authorization must fail before collectors persist derived state"
        );

        cleanup(&root);
    }

    #[test]
    fn torn_orphan_observation_resets_grace_before_collection() {
        let root = temp_root("torn_orphan_observation_resets_grace");
        let store = LocalContentStore::new(&root).expect("create store");
        let bytes = b"orphan with torn observation";
        let hash = content_hash(bytes);
        write_file_atomic(&store.blob_path(&hash).expect("orphan path"), bytes)
            .expect("write orphan");

        let first = store
            .collect_garbage(delete_eligible_orphan_gc_options(Duration::ZERO))
            .expect("persist first observation");
        assert_eq!(first.persisted_orphan_observation_count, 1);
        let marker = store
            .orphan_observation_path(&hash)
            .expect("observation path");
        fs::write(&marker, b"torn").expect("corrupt observation");

        let repaired = store
            .collect_garbage(delete_eligible_orphan_gc_options(Duration::ZERO))
            .expect("repair torn observation conservatively");
        assert_eq!(repaired.newly_observed_orphan_count, 1);
        assert_eq!(repaired.persisted_orphan_observation_count, 1);
        assert_eq!(repaired.deleted_orphan_count, 0);
        assert!(
            store
                .has_blob(&hash)
                .expect("orphan retained for fresh grace")
        );
        drop(store);

        let reopened = LocalContentStore::new(&root).expect("reopen repaired store");
        let collected = reopened
            .collect_garbage(delete_eligible_orphan_gc_options(Duration::ZERO))
            .expect("collect after repaired observation restart");
        assert_eq!(collected.deleted_orphan_count, 1);
        assert!(!reopened.has_blob(&hash).expect("orphan collected"));

        cleanup(&root);
    }

    #[test]
    fn orphan_observation_is_pruned_after_content_becomes_referenced() {
        let root = temp_root("orphan_observation_pruned_when_referenced");
        let store = LocalContentStore::new(&root).expect("create store");
        let bytes = vec![b'r'; INLINE_BLOB_SMALL_LIMIT_BYTES as usize + 1];
        let hash = content_hash(&bytes);
        write_file_atomic(&store.blob_path(&hash).expect("orphan blob path"), &bytes)
            .expect("write loose orphan");

        let observed = store
            .collect_garbage(delete_eligible_orphan_gc_options(Duration::from_secs(60)))
            .expect("observe loose orphan");
        assert_eq!(observed.retained_young_orphan_count, 1);
        let marker_path = store
            .orphan_observation_path(&hash)
            .expect("orphan observation path");
        assert!(marker_path.is_file());

        let manifest = store
            .put_object_version("object-now-referenced", "version_1", &bytes)
            .expect("reference observed content");
        assert_eq!(manifest.content_hash, hash);
        assert!(
            !marker_path.exists(),
            "manifest publication must durably reset any older orphan grace epoch"
        );
        let projection = store
            .collect_garbage(dry_run_orphan_gc_options(Duration::from_nanos(0)))
            .expect("project resolved orphan observation");
        assert_eq!(projection.stale_orphan_observation_count, 0);
        assert_eq!(projection.deleted_stale_orphan_observation_count, 0);
        let report = store
            .collect_garbage(delete_eligible_orphan_gc_options(Duration::from_nanos(0)))
            .expect("prune resolved orphan observation");
        assert_eq!(report.deleted_orphan_count, 0);
        assert_eq!(report.stale_orphan_observation_count, 0);
        assert_eq!(report.deleted_stale_orphan_observation_count, 0);
        assert!(store.has_blob(&hash).expect("referenced blob retained"));

        cleanup(&root);
    }

    #[test]
    fn rereferenced_content_gets_fresh_grace_after_last_manifest_is_removed() {
        let root = temp_root("rereferenced_content_gets_fresh_grace");
        let store = LocalContentStore::new(&root).expect("create store");
        let bytes = vec![b'g'; INLINE_BLOB_SMALL_LIMIT_BYTES as usize + 1];
        let hash = content_hash(&bytes);
        write_file_atomic(&store.blob_path(&hash).expect("orphan blob path"), &bytes)
            .expect("write loose orphan");

        let first_observation = store
            .collect_garbage(delete_eligible_orphan_gc_options(Duration::ZERO))
            .expect("persist old orphan observation");
        assert_eq!(first_observation.newly_observed_orphan_count, 1);
        assert_eq!(first_observation.deleted_orphan_count, 0);
        let marker_path = store
            .orphan_observation_path(&hash)
            .expect("orphan observation path");
        assert!(marker_path.is_file());

        store
            .put_object_version("object-rereferenced", "version_1", &bytes)
            .expect("publish reference to observed content");
        assert!(
            !marker_path.exists(),
            "the pre-reference orphan epoch must be cleared at publication"
        );
        store
            .put_object_version("object-rereferenced", "version_2", b"replacement")
            .expect("publish replacement version");
        release_all_content_reservations(&store);
        observe_collectible_versions(&store, 1);
        let version_gc = store
            .collect_versions(
                ContentStoreVersionGcOptions::delete_eligible(Duration::ZERO, 1)
                    .with_catalog_observation("test-catalog-generation-2", u64::MAX),
            )
            .expect("remove the last manifest referencing old content");
        assert_eq!(version_gc.deleted_version_count, 1);

        let fresh_observation = store
            .collect_garbage(delete_eligible_orphan_gc_options(Duration::ZERO))
            .expect("observe content after reference removal");
        assert_eq!(fresh_observation.newly_observed_orphan_count, 1);
        assert_eq!(fresh_observation.persisted_orphan_observation_count, 1);
        assert_eq!(fresh_observation.deleted_orphan_count, 0);
        assert!(
            store
                .has_blob(&hash)
                .expect("freshly observed content retained"),
            "an observation from before the manifest existed must not authorize deletion"
        );

        let collected = store
            .collect_garbage(delete_eligible_orphan_gc_options(Duration::ZERO))
            .expect("collect after fresh grace pass");
        assert_eq!(collected.deleted_orphan_count, 1);
        assert!(!store.has_blob(&hash).expect("orphan collected"));

        cleanup(&root);
    }

    #[test]
    fn stale_orphan_observation_cleanup_is_projected_and_bounded() {
        let root = temp_root("stale_orphan_observation_cleanup_is_bounded");
        let store = LocalContentStore::new(&root).expect("create store");
        let bytes = [
            b"stale orphan marker a".as_slice(),
            b"stale orphan marker b".as_slice(),
            b"stale orphan marker c".as_slice(),
        ];
        let hashes = bytes
            .iter()
            .map(|blob| {
                let hash = content_hash(blob);
                write_file_atomic(&store.blob_path(&hash).expect("orphan blob path"), blob)
                    .expect("write orphan blob");
                hash
            })
            .collect::<Vec<_>>();
        for (index, blob) in bytes.iter().enumerate() {
            let manifest = store
                .put_object_version(&format!("object-referenced-{index}"), "version_1", blob)
                .expect("reference observed blob");
            assert_eq!(manifest.content_hash, hashes[index]);
        }
        // Simulate stale legacy/crash-recovery markers. Normal manifest
        // publication clears an earlier observation before it returns.
        let observed_at_unix_nanos = system_time_nanos_since_epoch_saturating(SystemTime::now());
        let mut dirty_directories = BTreeSet::new();
        let mut root_created = false;
        for hash in &hashes {
            let persisted = store
                .persist_orphan_observation_without_sync(
                    hash,
                    observed_at_unix_nanos,
                    "test-stale-observation-catalog",
                    observed_at_unix_nanos,
                )
                .expect("persist stale observation fixture");
            assert!(persisted.created);
            dirty_directories.insert(persisted.parent);
            root_created |= persisted.root_created;
        }
        store
            .sync_orphan_observation_directories(&dirty_directories, root_created)
            .expect("sync stale observation fixture");

        let projection = store
            .collect_garbage(
                dry_run_orphan_gc_options(Duration::from_nanos(0)).with_max_deletions(1),
            )
            .expect("project bounded stale-marker cleanup");
        assert_eq!(projection.stale_orphan_observation_count, 3);
        assert_eq!(projection.deleted_stale_orphan_observation_count, 0);
        assert_eq!(projection.retained_limit_orphan_observation_count, 2);
        assert!(projection.max_deletions_reached);
        assert!(hashes.iter().all(|hash| {
            store
                .orphan_observation_path(hash)
                .expect("orphan observation path")
                .is_file()
        }));

        let limited = store
            .collect_garbage(
                delete_eligible_orphan_gc_options(Duration::from_nanos(0)).with_max_deletions(1),
            )
            .expect("bounded stale-marker cleanup");
        assert_eq!(limited.stale_orphan_observation_count, 3);
        assert_eq!(limited.deleted_stale_orphan_observation_count, 1);
        assert_eq!(limited.retained_limit_orphan_observation_count, 2);
        assert_eq!(
            hashes
                .iter()
                .filter(|hash| {
                    store
                        .orphan_observation_path(hash)
                        .expect("orphan observation path")
                        .is_file()
                })
                .count(),
            2
        );

        let final_pass = store
            .collect_garbage(delete_eligible_orphan_gc_options(Duration::from_nanos(0)))
            .expect("finish stale-marker cleanup");
        assert_eq!(final_pass.deleted_stale_orphan_observation_count, 2);
        assert!(hashes.iter().all(|hash| {
            !store
                .orphan_observation_path(hash)
                .expect("orphan observation path")
                .exists()
        }));

        cleanup(&root);
    }

    #[test]
    fn collect_garbage_dry_run_and_blocks_on_non_orphan_scrub_issues() {
        let root = temp_root("collect_garbage_dry_run_blocks_on_corrupt");
        let store = LocalContentStore::new(&root).expect("create store");
        let live_bytes = vec![b'a'; INLINE_BLOB_SMALL_LIMIT_BYTES as usize + 1];
        let corrupt = vec![b'b'; live_bytes.len()];
        let live = store
            .put_object_version("object-live", "v1", &live_bytes)
            .expect("put live object");
        let orphan_bytes = b"dry run orphan blob";
        let orphan_hash = content_hash(orphan_bytes);
        write_file_atomic(
            &store.blob_path(&orphan_hash).expect("orphan path"),
            orphan_bytes,
        )
        .expect("write orphan");

        let dry_run = store
            .collect_garbage(dry_run_orphan_gc_options(Duration::from_nanos(0)))
            .expect("dry-run gc");
        assert_eq!(dry_run.blocked_by_scrub_issue_count, 0);
        assert!(dry_run.dry_run);
        assert_eq!(dry_run.retained_young_orphan_count, 1);
        assert_eq!(dry_run.newly_observed_orphan_count, 1);
        assert_eq!(
            dry_run.newly_observed_orphan_bytes,
            orphan_bytes.len() as u64
        );
        assert_eq!(dry_run.eligible_orphan_count, 0);
        assert_eq!(dry_run.deleted_orphan_count, 0);
        assert!(dry_run.next_eligible_at_unix_nanos.is_some());
        assert!(
            !store
                .orphan_observation_path(&orphan_hash)
                .expect("dry-run observation path")
                .exists(),
            "dry-run must not persist first-observed state"
        );
        assert!(
            !store.stats_snapshot_path().exists(),
            "dry-run must not persist derived stats state"
        );
        assert!(store.has_blob(&orphan_hash).expect("dry-run retained"));

        let live_path = store.blob_path(&live.content_hash).expect("live blob path");
        assert!(live_path.is_file(), "normal producer selected a loose blob");
        write_file_atomic(&live_path, &corrupt).expect("corrupt referenced blob");
        let blocked = store
            .collect_garbage(delete_eligible_orphan_gc_options(Duration::from_nanos(0)))
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
    fn collect_versions_retains_protected_old_manifest() {
        let root = temp_root("collect_versions_retains_protected_old_manifest");
        let store = LocalContentStore::new(&root).expect("create store");
        let version_1 = store
            .put_object_version(
                "object-protected-history",
                "version_1",
                b"protected history",
            )
            .expect("put protected version");
        store
            .put_object_version(
                "object-protected-history",
                "version_2",
                b"collectible history",
            )
            .expect("put collectible version");
        let version_3 = store
            .put_object_version("object-protected-history", "version_3", b"latest history")
            .expect("put latest version");

        let options = ContentStoreVersionGcOptions::delete_eligible(Duration::from_nanos(0), 1)
            .with_protected_version("object-protected-history", "version_1")
            .with_protected_versions([("object-protected-history", "version_1")])
            .with_catalog_observation("test-catalog-generation-1", 1);
        assert_eq!(options.protected_versions.len(), 1);
        release_all_content_reservations(&store);
        let observed = store
            .collect_versions(options.clone())
            .expect("observe unprotected old version");
        assert_eq!(observed.deleted_version_count, 0);
        assert_eq!(observed.newly_observed_absent_version_count, 1);
        let report = store
            .collect_versions(
                options.with_catalog_observation("test-catalog-generation-2", u64::MAX),
            )
            .expect("collect unprotected old version");

        assert_eq!(report.scanned_version_count, 3);
        assert_eq!(report.retained_protected_version_count, 1);
        assert_eq!(
            report.retained_protected_logical_bytes,
            version_1.size_bytes
        );
        assert_eq!(report.retained_latest_version_count, 1);
        assert_eq!(report.eligible_version_count, 1);
        assert_eq!(report.deleted_version_count, 1);
        let remaining_versions = store
            .manifests_for_object("object-protected-history")
            .expect("remaining protected manifests")
            .into_iter()
            .map(|manifest| manifest.version)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            remaining_versions,
            BTreeSet::from([version_1.version.clone(), version_3.version.clone()])
        );
        assert_eq!(
            store
                .read_object_version("object-protected-history", &version_1.version)
                .expect("protected old version remains")
                .1,
            b"protected history"
        );
        assert!(
            store
                .read_object_version("object-protected-history", "version_2")
                .is_err(),
            "unprotected old version should be collected"
        );

        cleanup(&root);
    }

    #[test]
    fn version_gc_deadline_excludes_latest_and_protected_manifests() {
        let root = temp_root("version_gc_deadline_excludes_roots");
        let store = LocalContentStore::new(&root).expect("create store");
        store
            .put_object_version("object-deadline", "version_1", b"protected history")
            .expect("put protected version");
        store
            .put_object_version("object-deadline", "version_2", b"young history")
            .expect("put young version");
        store
            .put_object_version("object-deadline", "version_3", b"latest history")
            .expect("put latest version");
        release_all_content_reservations(&store);

        let min_age = Duration::from_secs(60 * 60);
        let young_published = match store
            .inline_version_state("object-deadline", "version_2")
            .expect("actual binding age")
        {
            Some(InlineVersionState::Live(entry)) => entry.published_at_unix_nanos,
            _ => panic!("young version must remain live"),
        };
        let expected_deadline = young_published.saturating_add(duration_nanos_saturating(min_age));
        let report = store
            .collect_versions(
                ContentStoreVersionGcOptions::dry_run(min_age, 1)
                    .with_protected_version("object-deadline", "version_1"),
            )
            .expect("project version deadline");

        assert_eq!(report.scanned_version_count, 3);
        assert_eq!(report.retained_protected_version_count, 1);
        assert_eq!(report.retained_latest_version_count, 1);
        assert_eq!(report.retained_young_version_count, 1);
        assert_eq!(report.eligible_version_count, 0);
        assert_eq!(report.next_eligible_at_unix_nanos, Some(expected_deadline));

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
        release_all_content_reservations(&store);
        let stats_before_dry_run = fs::read(store.stats_snapshot_path()).ok();

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
            fs::read(store.stats_snapshot_path()).ok(),
            stats_before_dry_run,
            "version GC dry-run must not persist a derived stats snapshot"
        );
        assert_eq!(
            store
                .manifests_for_object("object-history")
                .expect("manifests after dry run")
                .len(),
            3
        );

        let retained = store
            .collect_versions(
                ContentStoreVersionGcOptions::delete_eligible(Duration::from_secs(24 * 60 * 60), 1)
                    .with_catalog_observation("retention-catalog-generation", 1),
            )
            .expect("retention version gc");
        assert_eq!(retained.eligible_version_count, 0);
        assert_eq!(retained.deleted_version_count, 0);
        assert_eq!(retained.retained_latest_version_count, 1);
        assert_eq!(retained.retained_young_version_count, 2);
        assert!(retained.next_eligible_at_unix_nanos.is_some());

        let observed = store
            .collect_versions(
                ContentStoreVersionGcOptions::delete_eligible(Duration::ZERO, 1)
                    .with_min_absence_age(Duration::ZERO)
                    .with_catalog_observation("test-catalog-generation-1", 1),
            )
            .expect("persist old-version absence observations");
        assert_eq!(observed.deleted_version_count, 0);
        assert_eq!(observed.newly_observed_absent_version_count, 2);

        let limited = store
            .collect_versions(
                ContentStoreVersionGcOptions::delete_eligible(Duration::from_nanos(0), 1)
                    .with_max_deletions(1)
                    .with_catalog_observation("test-catalog-generation-2", u64::MAX),
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
            .collect_versions(
                ContentStoreVersionGcOptions::delete_eligible(Duration::from_nanos(0), 1)
                    .with_catalog_observation("test-catalog-generation-2", u64::MAX),
            )
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

        let observation = store
            .collect_garbage(delete_eligible_orphan_gc_options(Duration::from_nanos(0)))
            .expect("observe orphans after version gc");
        assert_eq!(observation.retained_young_orphan_count, 2);
        assert_eq!(observation.newly_observed_orphan_count, 2);
        assert_eq!(observation.deleted_orphan_count, 0);
        assert!(observation.next_eligible_at_unix_nanos.is_some());
        let blob_gc = store
            .collect_garbage(delete_eligible_orphan_gc_options(Duration::from_nanos(0)))
            .expect("blob gc after observation");
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
        release_all_content_reservations(&store);
        observe_collectible_versions(&store, 1);

        let report = store
            .collect_versions(
                ContentStoreVersionGcOptions::delete_eligible(Duration::from_nanos(0), 1)
                    .with_catalog_observation("test-catalog-generation-2", u64::MAX),
            )
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
    fn collect_versions_blocks_on_non_orphan_scrub_issues() {
        let root = temp_root("collect_versions_blocks_on_corrupt");
        let store = LocalContentStore::new(&root).expect("create store");
        let original = vec![b'a'; INLINE_BLOB_SMALL_LIMIT_BYTES as usize + 1];
        let corrupt = vec![b'b'; original.len()];
        let version_1 = store
            .put_object_version("object-history", "version_1", &original)
            .expect("put version 1");
        store
            .put_object_version("object-history", "version_2", b"history bytes v2")
            .expect("put version 2");
        let old_path = store
            .blob_path(&version_1.content_hash)
            .expect("version 1 blob path");
        assert!(old_path.is_file(), "normal producer selected a loose blob");
        write_file_atomic(&old_path, &corrupt).expect("corrupt referenced old version");

        let blocked = store
            .collect_versions(
                ContentStoreVersionGcOptions::delete_eligible(Duration::from_nanos(0), 1)
                    .with_catalog_observation("blocked-scrub-catalog-generation", 1),
            )
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
    fn read_blob_defers_content_address_verification_to_scrub() {
        let root = temp_root("read_blob_rejects_content_address_mismatch");
        let store = LocalContentStore::new(&root).expect("create store");
        let original = vec![b'a'; INLINE_BLOB_SMALL_LIMIT_BYTES as usize + 1];
        let corrupt = vec![b'b'; original.len()];
        let manifest = store
            .put_object_version("object-a", "v1", &original)
            .expect("put object version");
        let blob_path = store.blob_path(&manifest.content_hash).expect("blob path");
        assert!(blob_path.is_file(), "normal producer selected a loose blob");
        write_file_atomic(&blob_path, &corrupt).expect("corrupt blob");

        assert_eq!(
            store.read_blob(&manifest.content_hash).expect("read bytes"),
            corrupt
        );
        assert!(
            store
                .scrub()
                .expect("scrub")
                .issues
                .iter()
                .any(|issue| matches!(issue, ContentStoreScrubIssue::CorruptBlob { .. }))
        );
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

    fn release_all_content_reservations(store: &LocalContentStore) {
        let roots = store
            .content_reservation_roots()
            .expect("read content reservations");
        for reservation in roots.reservations {
            store
                .reconcile_content_reservation(
                    &reservation.identity.object_id,
                    &reservation.identity.version,
                    &reservation.identity.content_hash,
                    ContentStoreReservationResolution::CatalogCovered,
                )
                .expect("release catalog-covered reservation");
        }
        assert_eq!(
            store
                .content_reservation_roots()
                .expect("read reconciled content reservations")
                .reservation_count,
            0
        );
    }

    fn observe_collectible_versions(store: &LocalContentStore, keep_latest: u64) {
        let observed = store
            .collect_versions(
                ContentStoreVersionGcOptions::delete_eligible(Duration::ZERO, keep_latest)
                    .with_min_absence_age(Duration::ZERO)
                    .with_catalog_observation("test-catalog-generation-1", 1),
            )
            .expect("persist manifest absence observations");
        assert_eq!(observed.deleted_version_count, 0);
        assert_eq!(
            observed.persisted_absence_observation_count,
            observed.newly_observed_absent_version_count
        );
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
