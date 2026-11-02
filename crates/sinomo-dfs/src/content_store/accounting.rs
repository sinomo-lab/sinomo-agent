use super::{
    CONTENT_STORE_STATS_FILE, CONTENT_STORE_STATS_LOCK_FILE, CONTENT_STORE_STATS_VERSION,
    ContentStoreError, LocalContentStore, Result, VersionManifestEntry, decode_stats_snapshot,
    encode_stats_snapshot, ensure_bytes_match_hash, publish_file_as_blob,
    write_content_addressed_blob_bytes, write_content_addressed_blob_bytes_prehashed,
};
use crate::content_store::INLINE_BLOB_SMALL_LIMIT_BYTES;
use crate::content_store::fs_ops::{
    file_len_if_exists, read_file, recursive_file_count_and_bytes, remove_file,
    remove_file_if_exists, write_file_atomic,
};
use crate::content_store::inline_bindings::InlineVersionState;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{self};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

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
pub(super) struct ContentStoreStatsSnapshot {
    pub(super) version: u32,
    pub(super) stats: ContentStoreStats,
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

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct ContentStoreQuotaDelta {
    pub(super) blob_bytes: u64,
    pub(super) logical_object_bytes: u64,
    pub(super) manifest_bytes: u64,
    pub(super) staging_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct ContentStoreStatsDelta {
    pub(super) blob_count: i128,
    pub(super) blob_bytes: i128,
    pub(super) manifest_count: i128,
    pub(super) manifest_bytes: i128,
    pub(super) logical_object_bytes: i128,
    pub(super) extent_manifest_count: i128,
    pub(super) extent_count: i128,
    pub(super) staging_file_count: i128,
    pub(super) staging_bytes: i128,
}

impl ContentStoreStatsDelta {
    pub(super) fn blob_file_change(before_len: Option<u64>, after_len: Option<u64>) -> Self {
        Self {
            blob_count: file_count_delta(before_len, after_len),
            blob_bytes: file_bytes_delta(before_len, after_len),
            ..Self::default()
        }
    }

    pub(super) fn staging_file_change(before_len: Option<u64>, after_len: Option<u64>) -> Self {
        Self {
            staging_file_count: file_count_delta(before_len, after_len),
            staging_bytes: file_bytes_delta(before_len, after_len),
            ..Self::default()
        }
    }

    pub(super) fn manifest_deleted(entry: &VersionManifestEntry) -> Self {
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

    pub(super) fn is_zero(self) -> bool {
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

    pub(super) fn add(&mut self, other: Self) {
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

impl LocalContentStore {
    pub fn quota_usage(&self) -> Result<ContentStoreQuotaUsage> {
        Ok(ContentStoreQuotaUsage {
            stats: self.stats()?,
        })
    }

    pub fn stats(&self) -> Result<ContentStoreStats> {
        let _owner = self.content_mutation_guard()?;
        if self.quota.is_none() {
            // Without quota, stats are rebuildable observability state. Keep
            // this read byte-read-only: status and maintenance callers must
            // not turn an otherwise idle content store into an atomic snapshot
            // writer (or even create the snapshot lock file).
            return self.scan_stats();
        }

        let _lock = self.lock_stats_snapshot()?;
        self.quota_stats_unlocked()
    }

    fn quota_stats_unlocked(&self) -> Result<ContentStoreStats> {
        if self.inline_binding_view()?.is_some() {
            let stats = self.scan_stats()?;
            self.write_stats_snapshot_unlocked(&stats)?;
            self.quota_stats_initialized.store(true, Ordering::Release);
            return Ok(stats);
        }
        if self.quota_stats_initialized.load(Ordering::Acquire)
            && let Some(stats) = self.read_stats_snapshot_unlocked()?
        {
            return Ok(stats);
        }

        // Entering quota mode never trusts a snapshot that may predate
        // no-quota writes. The first admission rebuilds authoritative usage;
        // later quota mutations update it synchronously through deltas.
        let stats = self.scan_stats()?;
        self.write_stats_snapshot_unlocked(&stats)?;
        self.quota_stats_initialized.store(true, Ordering::Release);
        Ok(stats)
    }

    pub(super) fn scan_stats(&self) -> Result<ContentStoreStats> {
        let _owner = self.content_mutation_guard()?;
        let inline = self.inline_binding_view()?;
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
        if let Some(view) = inline {
            for state in view.versions.values() {
                if let InlineVersionState::Live(entry) = state {
                    let manifest = &entry.manifest;
                    stats.manifest_count = stats.manifest_count.saturating_add(1);
                    stats.manifest_bytes = stats
                        .manifest_bytes
                        .saturating_add(super::encode_manifest(manifest)?.len() as u64);
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
            }
        }
        Ok(stats)
    }

    pub(super) fn scan_blob_stats(&self) -> Result<(u64, u64)> {
        let _owner = self.content_mutation_guard()?;
        let (loose_count, loose_bytes) = recursive_file_count_and_bytes(&self.root.join("blobs"))?;
        let mut count = loose_count;
        let mut bytes = loose_bytes;
        if let Some(view) = self.inline_binding_view()? {
            for hash in view.blobs.keys().chain(view.deleted_blobs.iter()) {
                // Usage counts selected live
                // payloads, not obsolete frame bytes or retired generations.
                // An inline hash shadows its duplicate loose copies.
                if let Some(loose_bytes) = file_len_if_exists(&self.blob_path(hash)?)? {
                    count = count.saturating_sub(1);
                    bytes = bytes.saturating_sub(loose_bytes);
                }
                if let Some(entry) = view.blobs.get(hash) {
                    count = count.saturating_add(1);
                    bytes = bytes.saturating_add(entry.size_bytes);
                }
            }
        }
        Ok((count, bytes))
    }

    fn lock_stats_snapshot(&self) -> Result<fs::File> {
        let path = self.root.join(CONTENT_STORE_STATS_LOCK_FILE);
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|source| ContentStoreError::Io {
                path: path.clone(),
                source,
            })?;
        FileExt::lock_exclusive(&file).map_err(|source| ContentStoreError::Io { path, source })?;
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
        write_file_atomic(&self.stats_snapshot_path(), &bytes)
    }

    pub(super) fn replace_stats_snapshot(&self, stats: &ContentStoreStats) -> Result<()> {
        let _owner = self.content_mutation_guard()?;
        let _lock = self.lock_stats_snapshot()?;
        self.write_stats_snapshot_unlocked(stats)?;
        if self.quota.is_some() {
            self.quota_stats_initialized.store(true, Ordering::Release);
        }
        Ok(())
    }

    pub(super) fn apply_stats_delta(&self, delta: ContentStoreStatsDelta) -> Result<()> {
        let _owner = self.content_mutation_guard()?;
        if delta.is_zero() {
            return Ok(());
        }
        if self.quota.is_none() {
            return Ok(());
        }
        let _lock = self.lock_stats_snapshot()?;
        if self.inline_binding_view()?.is_some() {
            let stats = self.scan_stats()?;
            self.write_stats_snapshot_unlocked(&stats)?;
            self.quota_stats_initialized.store(true, Ordering::Release);
            return Ok(());
        }
        if self.quota_stats_initialized.load(Ordering::Acquire)
            && let Some(mut stats) = self.read_stats_snapshot_unlocked()?
        {
            delta.apply_to(&mut stats);
            self.write_stats_snapshot_unlocked(&stats)?;
            return Ok(());
        }

        // If no authoritative in-process quota snapshot exists, the mutation
        // has already happened. Scan the resulting durable state instead of
        // applying its delta twice or trusting a stale snapshot.
        let stats = self.scan_stats()?;
        self.write_stats_snapshot_unlocked(&stats)?;
        self.quota_stats_initialized.store(true, Ordering::Release);
        Ok(())
    }

    pub(super) fn tracks_stats_deltas(&self) -> bool {
        self.quota.is_some()
    }

    pub(super) fn write_tracked_blob_bytes(
        &self,
        hash: &str,
        blob_path: &Path,
        bytes: &[u8],
    ) -> Result<()> {
        let _owner = self.content_mutation_guard()?;
        if self.try_restore_inline_blob(hash, bytes)? {
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

    pub(super) fn write_tracked_blob_bytes_prehashed(
        &self,
        hash: &str,
        blob_path: &Path,
        bytes: &[u8],
    ) -> Result<()> {
        let _owner = self.content_mutation_guard()?;
        if self.try_restore_inline_blob(hash, bytes)? {
            return Ok(());
        }
        if bytes.len() as u64 <= INLINE_BLOB_SMALL_LIMIT_BYTES {
            return self.write_tracked_blob_bytes(hash, blob_path, bytes);
        }
        if !self.tracks_stats_deltas() {
            return write_content_addressed_blob_bytes_prehashed(hash, blob_path, bytes);
        }
        let before_len = file_len_if_exists(blob_path)?;
        write_content_addressed_blob_bytes_prehashed(hash, blob_path, bytes)?;
        let after_len = file_len_if_exists(blob_path)?;
        self.apply_stats_delta(ContentStoreStatsDelta::blob_file_change(
            before_len, after_len,
        ))
    }

    pub(super) fn publish_tracked_file_as_blob(
        &self,
        hash: &str,
        source_path: &Path,
        blob_path: &Path,
        expected_hash: &str,
    ) -> Result<()> {
        let _owner = self.content_mutation_guard()?;
        let source_len = file_len_if_exists(source_path)?.unwrap_or(0);
        if source_len <= INLINE_BLOB_SMALL_LIMIT_BYTES {
            let bytes = read_file(source_path)?;
            ensure_bytes_match_hash(hash, &bytes)?;
            if hash != expected_hash {
                return Err(ContentStoreError::BlobHashMismatch {
                    expected: expected_hash.to_owned(),
                    actual: hash.to_owned(),
                });
            }
            if self.try_restore_inline_blob(hash, &bytes)? {
                return Ok(());
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

    pub(super) fn write_tracked_staging_bytes(&self, path: &Path, bytes: &[u8]) -> Result<()> {
        let before_len = file_len_if_exists(path)?;
        write_file_atomic(path, bytes)?;
        let after_len = file_len_if_exists(path)?;
        self.apply_stats_delta(ContentStoreStatsDelta::staging_file_change(
            before_len, after_len,
        ))
    }

    pub(super) fn track_staging_file_created(&self, path: &Path) -> Result<()> {
        self.apply_stats_delta(ContentStoreStatsDelta::staging_file_change(
            None,
            file_len_if_exists(path)?,
        ))
    }

    pub(super) fn remove_tracked_staging_file(&self, path: &Path) -> Result<()> {
        let before_len = file_len_if_exists(path)?;
        remove_file(path)?;
        let after_len = file_len_if_exists(path)?;
        self.apply_stats_delta(ContentStoreStatsDelta::staging_file_change(
            before_len, after_len,
        ))
    }

    pub(super) fn remove_tracked_staging_file_if_exists(&self, path: &Path) -> Result<()> {
        let before_len = file_len_if_exists(path)?;
        remove_file_if_exists(path)?;
        let after_len = file_len_if_exists(path)?;
        self.apply_stats_delta(ContentStoreStatsDelta::staging_file_change(
            before_len, after_len,
        ))
    }

    pub(super) fn stats_snapshot_path(&self) -> PathBuf {
        self.root.join(CONTENT_STORE_STATS_FILE)
    }

    pub(super) fn ensure_quota_delta(&self, delta: ContentStoreQuotaDelta) -> Result<()> {
        let _owner = self.content_mutation_guard()?;
        let Some(quota) = self.quota.as_ref() else {
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
