use super::{
    BlobRangeReader, BlobReader, ContentStoreError, ContentStoreStatsDelta, LocalContentStore,
    Result, content_durable_file_error, ensure_bytes_match_hash, hash_hex,
};
use crate::content_store::fs_ops::{
    create_dir_all, file_len_if_exists, read_dir, remove_file_if_exists_and_sync_parent, sync_dir,
    write_file_atomic,
};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sinomo_core::object::{LengthMode, serde_ext as sinomo_object_serde};
use sinomo_core::storage::durable_file;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::MutexGuard;
#[cfg(test)]
use std::time::Duration;
use std::time::{Instant, SystemTime};

pub(super) const PACKED_BLOB_INDEX_VERSION: u32 = 1;
pub(super) const PACKED_BLOB_DIR: &str = "blob-packs";
pub(super) const PACKED_BLOB_SEGMENT_DIR: &str = "segments";
pub(super) const PACKED_BLOB_INDEX_FILE: &str = "index.sobj";
pub(super) const PACKED_BLOB_INDEX_LOG_FILE: &str = "index.slog";
pub(super) const PACKED_BLOB_LOCK_FILE: &str = "pack.lock";
const PACKED_BLOB_LANE_COUNT: u8 = 8;
const PACKED_BLOB_LANE_DIR: &str = "lanes";
pub(super) const PACKED_BLOB_SMALL_LIMIT_BYTES: u64 = 16 * 1024;
pub(super) const PACKED_BLOB_SEGMENT_MAX_BYTES: u64 = 8 * 1024 * 1024;
#[cfg(test)]
const PACKED_BLOB_INDEX_CACHE_TTL: Duration = Duration::from_secs(1);
#[cfg(not(test))]
const PACKED_BLOB_INDEX_CHECKPOINT_LOG_BYTES: u64 = 8 * 1024 * 1024;
#[cfg(test)]
const PACKED_BLOB_INDEX_CHECKPOINT_LOG_BYTES: u64 = 8 * 1024;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct PackedBlobIndex {
    pub(super) version: u32,
    pub(super) next_segment_id: u64,
    #[serde(default)]
    pub(super) entries: BTreeMap<String, PackedBlobLocation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct PackedBlobLocation {
    /// Lane zero is the pre-sharding layout and remains readable after upgrade.
    #[serde(default, skip_serializing_if = "packed_blob_lane_is_zero")]
    pub(super) lane: u8,
    pub(super) segment_id: String,
    pub(super) offset: u64,
    pub(super) size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PackedBlobIndexLogEntry {
    version: u32,
    content_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    location: Option<PackedBlobLocation>,
    next_segment_id: u64,
}

#[derive(Debug, Default)]
pub(super) struct PackedBlobIndexCache {
    entries: BTreeMap<u8, PackedBlobIndexCacheEntry>,
    #[cfg(test)]
    full_load_count: u64,
    #[cfg(test)]
    tail_replay_record_count: u64,
    #[cfg(test)]
    tail_replay_bytes: u64,
    #[cfg(test)]
    checkpoint_count: u64,
}

#[derive(Debug, Clone)]
struct PackedBlobIndexCacheEntry {
    loaded_at: Instant,
    fingerprint: PackedBlobIndexCacheFingerprint,
    log_replayed_offset: u64,
    index: PackedBlobIndex,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct PackedBlobIndexCacheFingerprint {
    index: Option<PackedBlobIndexFileFingerprint>,
    log: Option<PackedBlobIndexFileFingerprint>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct PackedBlobIndexFileFingerprint {
    len: u64,
    modified: Option<SystemTime>,
    created: Option<SystemTime>,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}

pub(super) struct PackedBlobIndexUpdateGuard<'a> {
    cache: MutexGuard<'a, PackedBlobIndexCache>,
    lane: u8,
}

impl PackedBlobIndexUpdateGuard<'_> {
    fn index(&self) -> &PackedBlobIndex {
        &self
            .cache
            .entries
            .get(&self.lane)
            .expect("packed index cache is loaded before returning its guard")
            .index
    }
}

impl Deref for PackedBlobIndexUpdateGuard<'_> {
    type Target = PackedBlobIndex;

    fn deref(&self) -> &Self::Target {
        self.index()
    }
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
struct PackedBlobIndexReplayProgress {
    valid_offset: u64,
    record_count: u64,
    record_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub(super) struct PackedBlobSegmentGcReport {
    pub(super) scanned_segment_count: u64,
    pub(super) eligible_segment_count: u64,
    pub(super) eligible_reclaimable_bytes: u64,
    pub(super) compacted_segment_count: u64,
    pub(super) reclaimed_segment_bytes: u64,
    pub(super) checkpointed_index_lane_count: u64,
    pub(super) truncated_index_log_bytes: u64,
    pub(super) retained_limit_segment_count: u64,
}

#[derive(Debug, Default)]
pub(super) struct PackedBlobIndexRemovalBatch {
    pub(super) removed_hashes: BTreeSet<String>,
    pub(super) modified_lanes: BTreeSet<u8>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct PackedBlobSegmentGcCandidate {
    lane: u8,
    segment_id: String,
    size_bytes: u64,
    live_bytes: u64,
}

#[derive(Debug, Default)]
struct PackedBlobSegmentGcCandidates {
    scanned_segment_count: u64,
    eligible: Vec<PackedBlobSegmentGcCandidate>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct PackedBlobSegmentCompaction {
    reclaimed_segment_bytes: u64,
    truncated_index_log_bytes: u64,
}

#[derive(Debug)]
struct OpenedPackedBlobRange {
    file: fs::File,
    offset: u64,
    size_bytes: u64,
    remaining_bytes: u64,
}

pub(super) struct PackedBlobLockGuard {
    _file: Option<fs::File>,
    hold_started_at: Option<Instant>,
    mode: &'static str,
    lane: u8,
}

impl PackedBlobLockGuard {
    fn new(file: fs::File, hold_started_at: Option<Instant>, mode: &'static str, lane: u8) -> Self {
        Self {
            _file: Some(file),
            hold_started_at,
            mode,
            lane,
        }
    }
}

impl Drop for PackedBlobLockGuard {
    fn drop(&mut self) {
        if let Some(file) = self._file.take() {
            let _ = FileExt::unlock(&file);
        }
        if let Some(hold_started_at) = self.hold_started_at {
            crate::dfs_perf_trace(format_args!(
                "packed-blob-lock mode={} lane={} hold_us={}",
                self.mode,
                self.lane,
                hold_started_at.elapsed().as_micros()
            ));
        }
    }
}

impl LocalContentStore {
    /// Number of independently locked packed-blob append lanes.
    ///
    /// New blobs are assigned by content hash. Lane zero retains the original
    /// on-disk paths so stores written before lane sharding remain readable.
    pub fn packed_blob_lane_count() -> usize {
        usize::from(PACKED_BLOB_LANE_COUNT)
    }

    #[cfg(test)]
    pub(super) fn lock_packed_blobs(&self) -> Result<PackedBlobLockGuard> {
        self.lock_packed_blob_lane(0)
    }

    #[cfg(test)]
    pub(super) fn lock_all_packed_blob_lanes(&self) -> Result<Vec<PackedBlobLockGuard>> {
        (0..PACKED_BLOB_LANE_COUNT)
            .map(|lane| self.lock_packed_blob_lane(lane))
            .collect()
    }

    pub(super) fn lock_packed_blob_lanes_for_hashes<'a>(
        &self,
        hashes: impl IntoIterator<Item = &'a str>,
    ) -> Result<Vec<PackedBlobLockGuard>> {
        // BTreeSet order is the process-wide lock order for multi-lane batches.
        let lanes = hashes
            .into_iter()
            .map(packed_blob_lane_for_hash)
            .collect::<Result<BTreeSet<_>>>()?;
        lanes
            .into_iter()
            .map(|lane| self.lock_packed_blob_lane(lane))
            .collect()
    }

    fn lock_packed_blob_lane(&self, lane: u8) -> Result<PackedBlobLockGuard> {
        let path = self.packed_blob_lane_root(lane).join(PACKED_BLOB_LOCK_FILE);
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
        let wait_started_at = crate::dfs_perf_trace_enabled().then(Instant::now);
        FileExt::lock_exclusive(&file).map_err(|source| ContentStoreError::Io { path, source })?;
        let hold_started_at = wait_started_at.map(|wait_started_at| {
            let acquired_at = Instant::now();
            crate::dfs_perf_trace(format_args!(
                "packed-blob-lock mode=exclusive lane={lane} wait_us={}",
                acquired_at.duration_since(wait_started_at).as_micros()
            ));
            Instant::now()
        });
        Ok(PackedBlobLockGuard::new(
            file,
            hold_started_at,
            "exclusive",
            lane,
        ))
    }

    fn lock_packed_blob_lane_for_read(&self, lane: u8) -> Result<Option<PackedBlobLockGuard>> {
        let path = self.packed_blob_lane_root(lane).join(PACKED_BLOB_LOCK_FILE);
        let file = match fs::OpenOptions::new().read(true).open(&path) {
            Ok(file) => file,
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(source) => {
                return Err(ContentStoreError::Io {
                    path: path.clone(),
                    source,
                });
            }
        };
        let wait_started_at = crate::dfs_perf_trace_enabled().then(Instant::now);
        FileExt::lock_shared(&file).map_err(|source| ContentStoreError::Io { path, source })?;
        let hold_started_at = wait_started_at.map(|wait_started_at| {
            let acquired_at = Instant::now();
            crate::dfs_perf_trace(format_args!(
                "packed-blob-lock mode=shared lane={lane} wait_us={}",
                acquired_at.duration_since(wait_started_at).as_micros()
            ));
            Instant::now()
        });
        Ok(Some(PackedBlobLockGuard::new(
            file,
            hold_started_at,
            "shared",
            lane,
        )))
    }

    pub(super) fn read_packed_blob_index_unlocked(&self) -> Result<PackedBlobIndex> {
        let mut merged = PackedBlobIndex {
            version: PACKED_BLOB_INDEX_VERSION,
            next_segment_id: 1,
            entries: BTreeMap::new(),
        };
        // Load lane zero first. If an upgraded store contains the same hash in
        // both layouts, the deterministic sharded lane is authoritative.
        for lane in 0..PACKED_BLOB_LANE_COUNT {
            let index = self.read_packed_blob_index_lane_unlocked_with_tail_policy(lane, false)?;
            for (hash, location) in index.entries {
                merged.entries.insert(hash, location);
            }
        }
        Ok(merged)
    }

    fn read_packed_blob_index_lane_for_update_unlocked(
        &self,
        lane: u8,
    ) -> Result<PackedBlobIndexUpdateGuard<'_>> {
        let cache = self.refresh_packed_blob_index_lane_cache_unlocked(lane, true)?;
        Ok(PackedBlobIndexUpdateGuard { cache, lane })
    }

    fn read_packed_blob_index_lane_unlocked_with_tail_policy(
        &self,
        lane: u8,
        truncate_torn_tail: bool,
    ) -> Result<PackedBlobIndex> {
        let (index, progress) = self.load_packed_blob_index_lane_from_disk(lane)?;
        let log_path = self.packed_blob_index_log_path_for_lane(lane);
        if truncate_torn_tail && log_path.is_file() {
            truncate_torn_packed_blob_index_log_tail(&log_path, progress.valid_offset)?;
        }
        Ok(index)
    }

    fn load_packed_blob_index_lane_from_disk(
        &self,
        lane: u8,
    ) -> Result<(PackedBlobIndex, PackedBlobIndexReplayProgress)> {
        let path = self.packed_blob_index_path_for_lane(lane);
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

        let log_path = self.packed_blob_index_log_path_for_lane(lane);
        let progress = if log_path.is_file() {
            replay_packed_blob_index_log_from(&log_path, &mut index, 0)?
        } else {
            PackedBlobIndexReplayProgress::default()
        };
        if index.next_segment_id == 0 {
            index.next_segment_id = 1;
        }
        for location in index.entries.values_mut() {
            location.lane = lane;
        }
        Ok((index, progress))
    }

    fn refresh_packed_blob_index_lane_cache_unlocked(
        &self,
        lane: u8,
        truncate_torn_tail: bool,
    ) -> Result<MutexGuard<'_, PackedBlobIndexCache>> {
        let mut fingerprint = self.packed_blob_index_cache_fingerprint(lane)?;
        let mut cache = self
            .packed_index_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if let Some(entry) = cache.entries.get_mut(&lane) {
            let observed_log_len = fingerprint
                .log
                .as_ref()
                .map(|fingerprint| fingerprint.len)
                .unwrap_or(0);
            let exact_fingerprint = entry.fingerprint == fingerprint;
            let complete_log_prefix = entry.log_replayed_offset == observed_log_len;
            if exact_fingerprint && (!truncate_torn_tail || complete_log_prefix) {
                entry.loaded_at = Instant::now();
                return Ok(cache);
            }

            let can_replay_tail = entry.fingerprint.index == fingerprint.index
                && observed_log_len >= entry.log_replayed_offset;
            if can_replay_tail {
                let log_path = self.packed_blob_index_log_path_for_lane(lane);
                let progress = if observed_log_len == 0 {
                    PackedBlobIndexReplayProgress::default()
                } else {
                    replay_packed_blob_index_log_from(
                        &log_path,
                        &mut entry.index,
                        entry.log_replayed_offset,
                    )?
                };
                if truncate_torn_tail && progress.valid_offset < observed_log_len {
                    truncate_torn_packed_blob_index_log_tail(&log_path, progress.valid_offset)?;
                    fingerprint = self.packed_blob_index_cache_fingerprint(lane)?;
                }
                entry.loaded_at = Instant::now();
                entry.fingerprint = fingerprint;
                entry.log_replayed_offset = progress.valid_offset;
                #[cfg(test)]
                {
                    cache.tail_replay_record_count = cache
                        .tail_replay_record_count
                        .saturating_add(progress.record_count);
                    cache.tail_replay_bytes = cache
                        .tail_replay_bytes
                        .saturating_add(progress.record_bytes);
                }
                return Ok(cache);
            }
        }

        let (index, progress) = self.load_packed_blob_index_lane_from_disk(lane)?;
        let log_path = self.packed_blob_index_log_path_for_lane(lane);
        if truncate_torn_tail
            && fingerprint
                .log
                .as_ref()
                .is_some_and(|log| progress.valid_offset < log.len)
        {
            truncate_torn_packed_blob_index_log_tail(&log_path, progress.valid_offset)?;
            fingerprint = self.packed_blob_index_cache_fingerprint(lane)?;
        }
        cache.entries.insert(
            lane,
            PackedBlobIndexCacheEntry {
                loaded_at: Instant::now(),
                fingerprint,
                log_replayed_offset: progress.valid_offset,
                index,
            },
        );
        #[cfg(test)]
        {
            cache.full_load_count = cache.full_load_count.saturating_add(1);
            cache.tail_replay_record_count = cache
                .tail_replay_record_count
                .saturating_add(progress.record_count);
            cache.tail_replay_bytes = cache
                .tail_replay_bytes
                .saturating_add(progress.record_bytes);
        }
        Ok(cache)
    }

    #[cfg(test)]
    fn read_cached_packed_blob_location_for_lane(
        &self,
        lane: u8,
        hash: &str,
    ) -> Result<Option<PackedBlobLocation>> {
        let now = Instant::now();
        let fingerprint = self.packed_blob_index_cache_fingerprint(lane)?;
        {
            let mut cache = self
                .packed_index_cache
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(entry) = cache.entries.get_mut(&lane)
                && now.saturating_duration_since(entry.loaded_at) <= PACKED_BLOB_INDEX_CACHE_TTL
                && entry.fingerprint == fingerprint
            {
                return Ok(entry.index.entries.get(hash).cloned());
            }
        }

        let _lock = self.lock_packed_blob_lane_for_read(lane)?;
        let cache = self.refresh_packed_blob_index_lane_cache_unlocked(lane, false)?;
        let entry = cache
            .entries
            .get(&lane)
            .expect("packed index cache loaded under the shared file lock");
        Ok(entry.index.entries.get(hash).cloned())
    }

    fn packed_blob_index_cache_fingerprint(
        &self,
        lane: u8,
    ) -> Result<PackedBlobIndexCacheFingerprint> {
        Ok(PackedBlobIndexCacheFingerprint {
            index: packed_blob_index_file_fingerprint(&self.packed_blob_index_path_for_lane(lane))?,
            log: packed_blob_index_file_fingerprint(
                &self.packed_blob_index_log_path_for_lane(lane),
            )?,
        })
    }

    fn invalidate_packed_blob_index_lane_cache(&self, lane: u8) {
        let mut cache = self
            .packed_index_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        cache.entries.remove(&lane);
    }

    #[cfg(test)]
    pub(super) fn packed_blob_location(&self, hash: &str) -> Result<Option<PackedBlobLocation>> {
        let lane = packed_blob_lane_for_hash(hash)?;
        if let Some(location) = self.read_cached_packed_blob_location_for_lane(lane, hash)? {
            return Ok(Some(location));
        }
        if lane != 0 {
            // Pre-sharding stores placed every object in lane zero.
            return self.read_cached_packed_blob_location_for_lane(0, hash);
        }
        Ok(None)
    }

    pub(super) fn packed_blob_location_for_update_unlocked(
        &self,
        hash: &str,
    ) -> Result<Option<PackedBlobLocation>> {
        let lane = packed_blob_lane_for_hash(hash)?;
        let index = self.read_packed_blob_index_lane_for_update_unlocked(lane)?;
        Ok(index.entries.get(hash).cloned())
    }

    pub(super) fn write_packed_blob(&self, hash: &str, bytes: &[u8]) -> Result<bool> {
        let lane = packed_blob_lane_for_hash(hash)?;
        // Do not acquire lane zero merely to deduplicate an upgraded legacy
        // entry: that would recreate the process-wide append lock. A repeated
        // content hash may be copied once into its deterministic lane, whose
        // entry then shadows the legacy fallback.
        let _lock = self.lock_packed_blob_lane(lane)?;
        let index = self.read_packed_blob_index_lane_for_update_unlocked(lane)?;
        if let Some(location) = index.entries.get(hash)
            && self.packed_blob_location_readable(location)?
        {
            return Ok(false);
        }
        let mut next_segment_id = index.next_segment_id;
        drop(index);

        let mut segment_id = if next_segment_id == 1 {
            "0000000000000000.pack".to_string()
        } else {
            format!("{:016x}.pack", next_segment_id.saturating_sub(1))
        };
        let mut segment_path = self.packed_blob_segment_path_for_lane(lane, &segment_id);
        let mut segment_len = file_len_if_exists(&segment_path)?.unwrap_or(0);
        if segment_len.saturating_add(bytes.len() as u64) > PACKED_BLOB_SEGMENT_MAX_BYTES {
            segment_id = format!("{next_segment_id:016x}.pack");
            next_segment_id = next_segment_id.saturating_add(1);
            segment_path = self.packed_blob_segment_path_for_lane(lane, &segment_id);
            segment_len = 0;
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
            lane,
            segment_id,
            offset,
            size_bytes: bytes.len() as u64,
        };
        self.append_packed_blob_index_log_entry_unlocked(
            lane,
            &PackedBlobIndexLogEntry {
                version: PACKED_BLOB_INDEX_VERSION,
                content_hash: hash.to_owned(),
                location: Some(location),
                next_segment_id,
            },
        )?;
        Ok(true)
    }

    pub(super) fn write_packed_blobs_unlocked(
        &self,
        blobs: &[(String, &[u8])],
    ) -> Result<ContentStoreStatsDelta> {
        self.write_packed_blobs_unlocked_with_sync(blobs, true)
    }

    fn write_packed_blobs_unlocked_with_sync(
        &self,
        blobs: &[(String, &[u8])],
        sync_writes: bool,
    ) -> Result<ContentStoreStatsDelta> {
        if blobs.is_empty() {
            return Ok(ContentStoreStatsDelta::default());
        }
        let mut by_lane = BTreeMap::<u8, Vec<(String, &[u8])>>::new();
        for (hash, bytes) in blobs {
            by_lane
                .entry(packed_blob_lane_for_hash(hash)?)
                .or_default()
                .push((hash.clone(), *bytes));
        }
        let mut total = ContentStoreStatsDelta::default();
        for (lane, lane_blobs) in by_lane {
            let delta =
                self.write_packed_blob_lane_batch_unlocked(lane, &lane_blobs, sync_writes)?;
            total.blob_count += delta.blob_count;
            total.blob_bytes += delta.blob_bytes;
        }
        Ok(total)
    }

    fn write_packed_blob_lane_batch_unlocked(
        &self,
        lane: u8,
        blobs: &[(String, &[u8])],
        sync_writes: bool,
    ) -> Result<ContentStoreStatsDelta> {
        debug_assert!(!blobs.is_empty());

        struct PendingPackedSegmentWrite {
            path: PathBuf,
            bytes: Vec<u8>,
        }

        let (existing_locations, mut next_segment_id) = {
            let index = self.read_packed_blob_index_lane_for_update_unlocked(lane)?;
            let existing_locations = blobs
                .iter()
                .filter_map(|(hash, _)| {
                    index
                        .entries
                        .get(hash)
                        .cloned()
                        .map(|location| (hash.clone(), location))
                })
                .collect::<BTreeMap<_, _>>();
            (existing_locations, index.next_segment_id)
        };

        let mut segment_writes = BTreeMap::<String, PendingPackedSegmentWrite>::new();
        let mut segment_lengths = BTreeMap::<String, u64>::new();
        let mut new_segment_parent_dirs = BTreeSet::<PathBuf>::new();
        let mut pending_hashes = BTreeSet::<String>::new();
        let mut records = Vec::new();
        let mut stats_delta = ContentStoreStatsDelta::default();

        for (hash, bytes) in blobs {
            if pending_hashes.contains(hash) {
                continue;
            }
            if let Some(location) = existing_locations.get(hash)
                && self.packed_blob_location_readable(location)?
            {
                continue;
            }

            let mut segment_id = if next_segment_id == 1 {
                "0000000000000000.pack".to_string()
            } else {
                format!("{:016x}.pack", next_segment_id.saturating_sub(1))
            };
            let mut segment_path = self.packed_blob_segment_path_for_lane(lane, &segment_id);
            let mut segment_len = match segment_lengths.get(&segment_id).copied() {
                Some(len) => len,
                None => {
                    let len = file_len_if_exists(&segment_path)?.unwrap_or(0);
                    segment_lengths.insert(segment_id.clone(), len);
                    len
                }
            };
            if segment_len.saturating_add(bytes.len() as u64) > PACKED_BLOB_SEGMENT_MAX_BYTES {
                segment_id = format!("{next_segment_id:016x}.pack");
                next_segment_id = next_segment_id.saturating_add(1);
                segment_path = self.packed_blob_segment_path_for_lane(lane, &segment_id);
                segment_len = 0;
                segment_lengths.insert(segment_id.clone(), 0);
            }

            if let Some(parent) = segment_path.parent() {
                create_dir_all(parent.to_owned())?;
            }
            let existed = segment_path.exists();
            let offset = segment_len;
            segment_writes
                .entry(segment_id.clone())
                .or_insert_with(|| PendingPackedSegmentWrite {
                    path: segment_path.clone(),
                    bytes: Vec::new(),
                })
                .bytes
                .extend_from_slice(bytes);
            segment_len = segment_len.saturating_add(bytes.len() as u64);
            segment_lengths.insert(segment_id.clone(), segment_len);
            if !existed && let Some(parent) = segment_path.parent() {
                new_segment_parent_dirs.insert(parent.to_owned());
            }

            let location = PackedBlobLocation {
                lane,
                segment_id: segment_id.clone(),
                offset,
                size_bytes: bytes.len() as u64,
            };
            records.push(PackedBlobIndexLogEntry {
                version: PACKED_BLOB_INDEX_VERSION,
                content_hash: hash.clone(),
                location: Some(location),
                next_segment_id,
            });
            pending_hashes.insert(hash.clone());
            stats_delta.blob_count += 1;
            stats_delta.blob_bytes += bytes.len() as i128;
        }

        for (_, pending) in segment_writes {
            let mut file = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&pending.path)
                .map_err(|source| ContentStoreError::Io {
                    path: pending.path.clone(),
                    source,
                })?;
            file.write_all(&pending.bytes)
                .map_err(|source| ContentStoreError::Io {
                    path: pending.path.clone(),
                    source,
                })?;
            if sync_writes {
                file.sync_all().map_err(|source| ContentStoreError::Io {
                    path: pending.path.clone(),
                    source,
                })?;
            }
        }
        if sync_writes {
            for parent in new_segment_parent_dirs {
                sync_dir(&parent)?;
            }
        }
        if sync_writes {
            self.append_packed_blob_index_log_entries_unlocked(lane, &records)?;
        } else {
            self.append_packed_blob_index_log_entries_unlocked_relaxed(lane, &records)?;
        }
        Ok(stats_delta)
    }

    #[allow(dead_code)]
    pub(super) fn remove_packed_blob_index_entry(&self, hash: &str) -> Result<bool> {
        Ok(self
            .remove_packed_blob_index_entries(&[hash.to_owned()])?
            .contains(hash))
    }

    /// Durably tombstone packed hashes with one lock acquisition and one
    /// append/sync per affected lane.
    ///
    /// Lane zero remains part of every hash's removal set because it owns the
    /// pre-sharding layout. Holding all affected lane locks in numeric order
    /// preserves the global packed-index lock order used by batched writes.
    pub(super) fn remove_packed_blob_index_entries(
        &self,
        hashes: &[String],
    ) -> Result<BTreeSet<String>> {
        Ok(self
            .remove_packed_blob_index_entries_impl(hashes, true)?
            .removed_hashes)
    }

    pub(super) fn remove_packed_blob_index_entries_deferred_checkpoint(
        &self,
        hashes: &[String],
    ) -> Result<PackedBlobIndexRemovalBatch> {
        self.remove_packed_blob_index_entries_impl(hashes, false)
    }

    fn remove_packed_blob_index_entries_impl(
        &self,
        hashes: &[String],
        allow_automatic_checkpoint: bool,
    ) -> Result<PackedBlobIndexRemovalBatch> {
        let mut hashes_by_lane = BTreeMap::<u8, BTreeSet<String>>::new();
        for hash in hashes {
            let target_lane = packed_blob_lane_for_hash(hash)?;
            hashes_by_lane.entry(0).or_default().insert(hash.clone());
            if target_lane != 0 {
                hashes_by_lane
                    .entry(target_lane)
                    .or_default()
                    .insert(hash.clone());
            }
        }
        if hashes_by_lane.is_empty() {
            return Ok(PackedBlobIndexRemovalBatch::default());
        }

        let _locks = hashes_by_lane
            .keys()
            .copied()
            .map(|lane| self.lock_packed_blob_lane(lane))
            .collect::<Result<Vec<_>>>()?;
        let mut report = PackedBlobIndexRemovalBatch::default();
        for (lane, lane_hashes) in hashes_by_lane {
            let index = self.read_packed_blob_index_lane_for_update_unlocked(lane)?;
            let next_segment_id = index.next_segment_id;
            let records = lane_hashes
                .into_iter()
                .filter(|hash| index.entries.contains_key(hash))
                .map(|content_hash| PackedBlobIndexLogEntry {
                    version: PACKED_BLOB_INDEX_VERSION,
                    content_hash,
                    location: None,
                    next_segment_id,
                })
                .collect::<Vec<_>>();
            drop(index);
            if records.is_empty() {
                continue;
            }
            report
                .removed_hashes
                .extend(records.iter().map(|record| record.content_hash.clone()));
            report.modified_lanes.insert(lane);
            self.append_packed_blob_index_log_entries_unlocked_with_sync_and_checkpoint(
                lane,
                &records,
                true,
                allow_automatic_checkpoint,
            )?;
        }
        Ok(report)
    }

    pub(super) fn checkpoint_packed_blob_index_lanes_after_gc(
        &self,
        lanes: &BTreeSet<u8>,
    ) -> Result<(u64, u64)> {
        let mut checkpointed_lane_count = 0_u64;
        let mut truncated_log_bytes = 0_u64;
        for lane in lanes {
            let _lock = self.lock_packed_blob_lane(*lane)?;
            truncated_log_bytes = truncated_log_bytes
                .saturating_add(self.checkpoint_packed_blob_index_after_gc_unlocked(*lane)?);
            checkpointed_lane_count = checkpointed_lane_count.saturating_add(1);
        }
        Ok((checkpointed_lane_count, truncated_log_bytes))
    }

    /// Reclaim physical bytes left behind by durable packed-index tombstones.
    ///
    /// Logical orphan deletion deliberately publishes its index tombstone
    /// first. This pass derives obsolete segment ranges from the durable index,
    /// so a crash between those phases merely leaves reclaimable bytes for the
    /// next pass. Repacking is limited by segment count and every segment is
    /// capped by `PACKED_BLOB_SEGMENT_MAX_BYTES`.
    pub(super) fn collect_packed_blob_segment_garbage(
        &self,
        dry_run: bool,
        max_compactions: u64,
    ) -> Result<PackedBlobSegmentGcReport> {
        let candidates = self.packed_blob_segment_gc_candidates()?;
        let mut report = PackedBlobSegmentGcReport {
            scanned_segment_count: candidates.scanned_segment_count,
            eligible_segment_count: candidates.eligible.len() as u64,
            eligible_reclaimable_bytes: candidates
                .eligible
                .iter()
                .map(|candidate| candidate.size_bytes.saturating_sub(candidate.live_bytes))
                .fold(0_u64, u64::saturating_add),
            ..PackedBlobSegmentGcReport::default()
        };
        if dry_run {
            report.retained_limit_segment_count = report
                .eligible_segment_count
                .saturating_sub(max_compactions);
            return Ok(report);
        }

        for candidate in candidates
            .eligible
            .iter()
            .take(usize::try_from(max_compactions).unwrap_or(usize::MAX))
        {
            let Some(compaction) =
                self.compact_packed_blob_segment(candidate.lane, &candidate.segment_id)?
            else {
                continue;
            };
            report.compacted_segment_count = report.compacted_segment_count.saturating_add(1);
            report.reclaimed_segment_bytes = report
                .reclaimed_segment_bytes
                .saturating_add(compaction.reclaimed_segment_bytes);
            report.checkpointed_index_lane_count =
                report.checkpointed_index_lane_count.saturating_add(1);
            report.truncated_index_log_bytes = report
                .truncated_index_log_bytes
                .saturating_add(compaction.truncated_index_log_bytes);
        }
        report.retained_limit_segment_count = report
            .eligible_segment_count
            .saturating_sub(report.compacted_segment_count);
        Ok(report)
    }

    fn packed_blob_segment_gc_candidates(&self) -> Result<PackedBlobSegmentGcCandidates> {
        let mut report = PackedBlobSegmentGcCandidates::default();
        for lane in 0..PACKED_BLOB_LANE_COUNT {
            let index = self.read_packed_blob_index_lane_unlocked_with_tail_policy(lane, false)?;
            let mut live_ranges_by_segment = BTreeMap::<String, Vec<(u64, u64)>>::new();
            for location in index.entries.values() {
                let end = location
                    .offset
                    .checked_add(location.size_bytes)
                    .ok_or_else(|| ContentStoreError::InvalidContentLayout {
                        message: format!(
                            "packed blob range overflows during GC: lane={lane}, segment={}",
                            location.segment_id
                        ),
                    })?;
                live_ranges_by_segment
                    .entry(location.segment_id.clone())
                    .or_default()
                    .push((location.offset, end));
            }

            let segments_root = self
                .packed_blob_lane_root(lane)
                .join(PACKED_BLOB_SEGMENT_DIR);
            if !segments_root.exists() {
                continue;
            }
            let mut segment_entries = read_dir(&segments_root)?;
            segment_entries.sort_by_key(|entry| entry.path());
            for entry in segment_entries {
                let path = entry.path();
                if path.extension().and_then(|extension| extension.to_str()) != Some("pack") {
                    continue;
                }
                let metadata = entry.metadata().map_err(|source| ContentStoreError::Io {
                    path: path.clone(),
                    source,
                })?;
                if !metadata.is_file() {
                    continue;
                }
                report.scanned_segment_count = report.scanned_segment_count.saturating_add(1);
                let size_bytes = metadata.len();
                let Some(segment_id) = path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .map(str::to_owned)
                else {
                    continue;
                };
                let mut ranges = live_ranges_by_segment
                    .remove(&segment_id)
                    .unwrap_or_default();
                ranges.sort_unstable();
                let mut live_bytes = 0_u64;
                let mut previous_end = 0_u64;
                for (start, end) in &ranges {
                    if *end > size_bytes {
                        return Err(ContentStoreError::InvalidContentLayout {
                            message: format!(
                                "packed GC live range exceeds segment: lane={lane}, segment={segment_id}, end={end}, len={size_bytes}"
                            ),
                        });
                    }
                    if *start < previous_end {
                        return Err(ContentStoreError::InvalidContentLayout {
                            message: format!(
                                "packed GC found overlapping live ranges: lane={lane}, segment={segment_id}, start={start}, previous_end={previous_end}"
                            ),
                        });
                    }
                    live_bytes = live_bytes.saturating_add(end.saturating_sub(*start));
                    previous_end = *end;
                }
                if ranges.is_empty() || live_bytes < size_bytes {
                    report.eligible.push(PackedBlobSegmentGcCandidate {
                        lane,
                        segment_id,
                        size_bytes,
                        live_bytes,
                    });
                }
            }
        }
        Ok(report)
    }

    fn compact_packed_blob_segment(
        &self,
        lane: u8,
        segment_id: &str,
    ) -> Result<Option<PackedBlobSegmentCompaction>> {
        let _lock = self.lock_packed_blob_lane(lane)?;
        let index = self.read_packed_blob_index_lane_for_update_unlocked(lane)?;
        let mut live_entries = index
            .entries
            .iter()
            .filter(|(_, location)| location.segment_id == segment_id)
            .map(|(hash, location)| (hash.clone(), location.clone()))
            .collect::<Vec<_>>();
        let mut next_segment_id = index.next_segment_id.max(1);
        let referenced_segment_ids = index
            .entries
            .values()
            .map(|location| location.segment_id.clone())
            .collect::<BTreeSet<_>>();
        drop(index);

        let old_path = self.packed_blob_segment_path_for_lane(lane, segment_id);
        let Some(old_len) = file_len_if_exists(&old_path)? else {
            return Ok(None);
        };
        live_entries.sort_by(|left, right| {
            left.1
                .offset
                .cmp(&right.1.offset)
                .then_with(|| left.0.cmp(&right.0))
        });
        let live_len = live_entries
            .iter()
            .map(|(_, location)| location.size_bytes)
            .fold(0_u64, u64::saturating_add);
        if !live_entries.is_empty() && live_len >= old_len {
            return Ok(None);
        }
        if live_entries.is_empty() {
            let truncated_index_log_bytes =
                self.checkpoint_packed_blob_index_after_gc_unlocked(lane)?;
            return remove_file_if_exists_and_sync_parent(&old_path).map(|removed| {
                removed.then_some(PackedBlobSegmentCompaction {
                    reclaimed_segment_bytes: old_len,
                    truncated_index_log_bytes,
                })
            });
        }
        if old_len > PACKED_BLOB_SEGMENT_MAX_BYTES {
            return Err(ContentStoreError::InvalidContentLayout {
                message: format!(
                    "packed segment exceeds bounded GC size: lane={lane}, segment={segment_id}, len={old_len}, max={PACKED_BLOB_SEGMENT_MAX_BYTES}"
                ),
            });
        }

        let mut old_file = fs::File::open(&old_path).map_err(|source| ContentStoreError::Io {
            path: old_path.clone(),
            source,
        })?;
        let mut repacked = Vec::with_capacity(usize::try_from(live_len).unwrap_or(usize::MAX));
        let mut records = Vec::with_capacity(live_entries.len());
        for (hash, location) in &live_entries {
            let end = location
                .offset
                .checked_add(location.size_bytes)
                .ok_or_else(|| ContentStoreError::InvalidContentLayout {
                    message: format!(
                        "packed blob range overflows during compaction: hash={hash}, segment={segment_id}"
                    ),
                })?;
            if end > old_len {
                return Err(ContentStoreError::InvalidContentLayout {
                    message: format!(
                        "packed blob range exceeds segment during compaction: hash={hash}, segment={segment_id}, end={end}, len={old_len}"
                    ),
                });
            }
            old_file
                .seek(SeekFrom::Start(location.offset))
                .map_err(|source| ContentStoreError::Io {
                    path: old_path.clone(),
                    source,
                })?;
            let start = repacked.len();
            let new_len = start
                .checked_add(usize::try_from(location.size_bytes).map_err(|_| {
                    ContentStoreError::InvalidContentLayout {
                        message: format!(
                            "packed blob is too large to compact: hash={hash}, bytes={}",
                            location.size_bytes
                        ),
                    }
                })?)
                .ok_or_else(|| ContentStoreError::InvalidContentLayout {
                    message: "packed compaction buffer length overflow".to_string(),
                })?;
            repacked.resize(new_len, 0);
            old_file
                .read_exact(&mut repacked[start..new_len])
                .map_err(|source| ContentStoreError::Io {
                    path: old_path.clone(),
                    source,
                })?;
            ensure_bytes_match_hash(hash, &repacked[start..new_len])?;
        }

        let (new_segment_id, new_path) = loop {
            let candidate = format!("{next_segment_id:016x}.pack");
            let candidate_path = self.packed_blob_segment_path_for_lane(lane, &candidate);
            next_segment_id = next_segment_id.saturating_add(1);
            if candidate != segment_id
                && !referenced_segment_ids.contains(&candidate)
                && !candidate_path.exists()
            {
                break (candidate, candidate_path);
            }
            if next_segment_id == u64::MAX {
                return Err(ContentStoreError::InvalidContentLayout {
                    message: format!("packed segment id space exhausted for lane {lane}"),
                });
            }
        };
        write_file_atomic(&new_path, &repacked)?;

        let mut offset = 0_u64;
        for (hash, location) in live_entries {
            records.push(PackedBlobIndexLogEntry {
                version: PACKED_BLOB_INDEX_VERSION,
                content_hash: hash,
                location: Some(PackedBlobLocation {
                    lane,
                    segment_id: new_segment_id.clone(),
                    offset,
                    size_bytes: location.size_bytes,
                }),
                next_segment_id,
            });
            offset = offset.saturating_add(location.size_bytes);
        }
        // The replacement segment is durable before the new locations. The old
        // segment remains until every location record is durably appended, so a
        // torn append can only leave readable copies in both generations.
        self.append_packed_blob_index_log_entries_unlocked(lane, &records)?;
        let truncated_index_log_bytes =
            self.checkpoint_packed_blob_index_after_gc_unlocked(lane)?;
        remove_file_if_exists_and_sync_parent(&old_path)?;
        Ok(Some(PackedBlobSegmentCompaction {
            reclaimed_segment_bytes: old_len.saturating_sub(live_len),
            truncated_index_log_bytes,
        }))
    }

    fn checkpoint_packed_blob_index_after_gc_unlocked(&self, lane: u8) -> Result<u64> {
        // A preceding durable append may have attempted automatic checkpoint
        // maintenance and invalidated the process cache if that best-effort
        // checkpoint failed. Reload the authoritative snapshot-plus-log pair
        // before requiring the GC checkpoint.
        let index = self.read_packed_blob_index_lane_for_update_unlocked(lane)?;
        drop(index);
        let log_path = self.packed_blob_index_log_path_for_lane(lane);
        let before_len = file_len_if_exists(&log_path)?.unwrap_or(0);
        if !self.checkpoint_packed_blob_index_lane_unlocked(lane)? {
            return Err(ContentStoreError::InvalidContentLayout {
                message: format!("packed GC could not checkpoint the loaded index for lane {lane}"),
            });
        }
        let after_len = file_len_if_exists(&log_path)?.unwrap_or(0);
        Ok(before_len.saturating_sub(after_len))
    }

    /// Resolve and open a packed blob while holding the lane's shared lock.
    ///
    /// Segment compaction publishes replacement index locations and removes the
    /// retired segment under the exclusive form of the same lock. Keeping the
    /// shared lock through `File::open` closes the otherwise unsafe gap between
    /// returning an index location and opening its segment. The file handle is
    /// sufficient after this method returns: Unix keeps an unlinked inode alive,
    /// while platforms that forbid unlinking an open file make compaction retry.
    pub(super) fn open_packed_blob_reader(
        &self,
        hash: &str,
        offset: u64,
        max_bytes: usize,
    ) -> Result<Option<(PackedBlobLocation, BlobReader)>> {
        let Some((location, opened)) =
            self.open_packed_blob_range_file_for_hash(hash, offset, max_bytes)?
        else {
            return Ok(None);
        };
        Ok(Some((
            location,
            BlobReader::with_remaining(opened.file, opened.size_bytes, opened.remaining_bytes),
        )))
    }

    pub(super) fn open_packed_blob_range_reader(
        &self,
        hash: &str,
        offset: u64,
        max_bytes: usize,
    ) -> Result<Option<(PackedBlobLocation, BlobRangeReader)>> {
        let Some((location, opened)) =
            self.open_packed_blob_range_file_for_hash(hash, offset, max_bytes)?
        else {
            return Ok(None);
        };
        Ok(Some((
            location,
            BlobRangeReader::new(
                opened.file,
                opened.offset,
                opened.size_bytes,
                opened.remaining_bytes,
            ),
        )))
    }

    pub(super) fn packed_blob_readable_size(&self, hash: &str) -> Result<Option<u64>> {
        let Some((location, _lock)) = self.packed_blob_location_and_read_lock(hash)? else {
            return Ok(None);
        };
        if !self.packed_blob_location_readable(&location)? {
            return Ok(None);
        }
        Ok(Some(location.size_bytes))
    }

    fn open_packed_blob_range_file_for_hash(
        &self,
        hash: &str,
        offset: u64,
        max_bytes: usize,
    ) -> Result<Option<(PackedBlobLocation, OpenedPackedBlobRange)>> {
        let Some((location, _lock)) = self.packed_blob_location_and_read_lock(hash)? else {
            return Ok(None);
        };
        let opened = self.open_packed_blob_range_file(&location, offset, max_bytes)?;
        Ok(Some((location, opened)))
    }

    fn packed_blob_location_and_read_lock(
        &self,
        hash: &str,
    ) -> Result<Option<(PackedBlobLocation, Option<PackedBlobLockGuard>)>> {
        let target_lane = packed_blob_lane_for_hash(hash)?;
        for lane in [target_lane, 0] {
            let lock = self.lock_packed_blob_lane_for_read(lane)?;
            // The shared lane lock makes the snapshot-plus-log fingerprint
            // stable while the process cache is refreshed. Replaying the
            // complete lane index for every small packed read made a scrub of
            // N distinct blobs quadratic in the lane cardinality.
            let cache = self.refresh_packed_blob_index_lane_cache_unlocked(lane, false)?;
            let location = cache
                .entries
                .get(&lane)
                .and_then(|entry| entry.index.entries.get(hash))
                .cloned();
            drop(cache);
            if let Some(location) = location {
                return Ok(Some((location, lock)));
            }
            drop(lock);
            if target_lane == 0 {
                break;
            }
        }
        Ok(None)
    }

    fn open_packed_blob_range_file(
        &self,
        location: &PackedBlobLocation,
        offset: u64,
        max_bytes: usize,
    ) -> Result<OpenedPackedBlobRange> {
        let path = self.packed_blob_location_path(location);
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
            ContentStoreError::InvalidContentLayout {
                message: format!(
                    "packed blob offset overflows: segment={}",
                    location.segment_id
                ),
            }
        })?;
        let physical_end = location
            .offset
            .checked_add(location.size_bytes)
            .ok_or_else(|| ContentStoreError::InvalidContentLayout {
                message: format!(
                    "packed blob range overflows: segment={}",
                    location.segment_id
                ),
            })?;
        if physical_end > segment_len {
            return Err(ContentStoreError::InvalidContentLayout {
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
        Ok(OpenedPackedBlobRange {
            file,
            offset: start,
            size_bytes: location.size_bytes,
            remaining_bytes,
        })
    }

    pub(super) fn packed_blob_location_readable(
        &self,
        location: &PackedBlobLocation,
    ) -> Result<bool> {
        let path = self.packed_blob_location_path(location);
        let Some(segment_len) = file_len_if_exists(&path)? else {
            return Ok(false);
        };
        Ok(location
            .offset
            .checked_add(location.size_bytes)
            .is_some_and(|end| end <= segment_len))
    }

    #[cfg(test)]
    pub(super) fn packed_blob_index_path_for_hash(&self, hash: &str) -> Result<PathBuf> {
        Ok(self.packed_blob_index_path_for_lane(packed_blob_lane_for_hash(hash)?))
    }

    #[cfg(test)]
    pub(super) fn packed_blob_index_log_path_for_hash(&self, hash: &str) -> Result<PathBuf> {
        Ok(self.packed_blob_index_log_path_for_lane(packed_blob_lane_for_hash(hash)?))
    }

    pub(super) fn packed_blob_location_path(&self, location: &PackedBlobLocation) -> PathBuf {
        self.packed_blob_segment_path_for_lane(location.lane, &location.segment_id)
    }

    fn packed_blob_lane_root(&self, lane: u8) -> PathBuf {
        debug_assert!(lane < PACKED_BLOB_LANE_COUNT);
        let root = self.root.join(PACKED_BLOB_DIR);
        if lane == 0 {
            root
        } else {
            root.join(PACKED_BLOB_LANE_DIR).join(format!("{lane:02x}"))
        }
    }

    fn packed_blob_index_path_for_lane(&self, lane: u8) -> PathBuf {
        self.packed_blob_lane_root(lane)
            .join(PACKED_BLOB_INDEX_FILE)
    }

    fn packed_blob_index_log_path_for_lane(&self, lane: u8) -> PathBuf {
        self.packed_blob_lane_root(lane)
            .join(PACKED_BLOB_INDEX_LOG_FILE)
    }

    fn packed_blob_segment_path_for_lane(&self, lane: u8, segment_id: &str) -> PathBuf {
        self.packed_blob_lane_root(lane)
            .join(PACKED_BLOB_SEGMENT_DIR)
            .join(segment_id)
    }

    fn append_packed_blob_index_log_entry_unlocked(
        &self,
        lane: u8,
        entry: &PackedBlobIndexLogEntry,
    ) -> Result<()> {
        self.append_packed_blob_index_log_entries_unlocked(lane, std::slice::from_ref(entry))
    }

    fn append_packed_blob_index_log_entries_unlocked(
        &self,
        lane: u8,
        entries: &[PackedBlobIndexLogEntry],
    ) -> Result<()> {
        self.append_packed_blob_index_log_entries_unlocked_with_sync(lane, entries, true)
    }

    fn append_packed_blob_index_log_entries_unlocked_relaxed(
        &self,
        lane: u8,
        entries: &[PackedBlobIndexLogEntry],
    ) -> Result<()> {
        self.append_packed_blob_index_log_entries_unlocked_with_sync(lane, entries, false)
    }

    fn append_packed_blob_index_log_entries_unlocked_with_sync(
        &self,
        lane: u8,
        entries: &[PackedBlobIndexLogEntry],
        sync_writes: bool,
    ) -> Result<()> {
        self.append_packed_blob_index_log_entries_unlocked_with_sync_and_checkpoint(
            lane,
            entries,
            sync_writes,
            true,
        )
    }

    fn append_packed_blob_index_log_entries_unlocked_with_sync_and_checkpoint(
        &self,
        lane: u8,
        entries: &[PackedBlobIndexLogEntry],
        sync_writes: bool,
        allow_automatic_checkpoint: bool,
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let path = self.packed_blob_index_log_path_for_lane(lane);
        let append_bytes = encode_packed_blob_index_log_entries(entries)?;
        if let Some(parent) = path.parent() {
            create_dir_all(parent.to_owned())?;
        }
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|source| ContentStoreError::Io {
                path: path.clone(),
                source,
            })?;
        file.write_all(&append_bytes)
            .map_err(|source| ContentStoreError::Io {
                path: path.clone(),
                source,
            })?;
        if sync_writes {
            file.sync_all().map_err(|source| ContentStoreError::Io {
                path: path.clone(),
                source,
            })?;
            if let Some(parent) = path.parent() {
                sync_dir(parent)?;
            }
        }
        let log_offset = file
            .metadata()
            .map_err(|source| ContentStoreError::Io {
                path: path.clone(),
                source,
            })?
            .len();
        self.apply_packed_blob_index_log_entries_to_cache_unlocked(lane, entries, log_offset)?;
        if sync_writes
            && allow_automatic_checkpoint
            && let Err(error) = self.maybe_checkpoint_packed_blob_index_lane_unlocked(lane)
        {
            // The append log is already durable, so checkpoint maintenance
            // must not turn a successful content write into an ambiguous
            // failure. Drop the process cache and let the next operation
            // recover from the durable snapshot-plus-log pair.
            tracing::warn!(
                error = %error,
                path = %path.display(),
                "packed blob index checkpoint failed after a durable append"
            );
            self.invalidate_packed_blob_index_lane_cache(lane);
        }
        Ok(())
    }

    fn apply_packed_blob_index_log_entries_to_cache_unlocked(
        &self,
        lane: u8,
        entries: &[PackedBlobIndexLogEntry],
        log_offset: u64,
    ) -> Result<()> {
        let fingerprint = self.packed_blob_index_cache_fingerprint(lane)?;
        let mut cache = self
            .packed_index_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(entry) = cache.entries.get_mut(&lane) else {
            return Ok(());
        };
        for record in entries {
            apply_packed_blob_index_log_entry(&mut entry.index, record);
        }
        entry.loaded_at = Instant::now();
        entry.fingerprint = fingerprint;
        entry.log_replayed_offset = log_offset;
        Ok(())
    }

    fn maybe_checkpoint_packed_blob_index_lane_unlocked(&self, lane: u8) -> Result<bool> {
        let should_checkpoint = {
            let cache = self
                .packed_index_cache
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            cache.entries.get(&lane).is_some_and(|entry| {
                entry.log_replayed_offset >= PACKED_BLOB_INDEX_CHECKPOINT_LOG_BYTES
            })
        };
        if !should_checkpoint {
            return Ok(false);
        }
        self.checkpoint_packed_blob_index_lane_unlocked(lane)
    }

    fn checkpoint_packed_blob_index_lane_unlocked(&self, lane: u8) -> Result<bool> {
        let snapshot_bytes = {
            let cache = self
                .packed_index_cache
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let Some(entry) = cache.entries.get(&lane) else {
                return Ok(false);
            };
            encode_packed_blob_index(&entry.index)?
        };
        write_file_atomic(&self.packed_blob_index_path_for_lane(lane), &snapshot_bytes)?;

        let log_path = self.packed_blob_index_log_path_for_lane(lane);
        if log_path.is_file() {
            truncate_torn_packed_blob_index_log_tail(&log_path, 0)?;
        }
        let fingerprint = self.packed_blob_index_cache_fingerprint(lane)?;
        let mut cache = self
            .packed_index_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(entry) = cache.entries.get_mut(&lane) {
            entry.loaded_at = Instant::now();
            entry.fingerprint = fingerprint;
            entry.log_replayed_offset = 0;
        }
        #[cfg(test)]
        {
            cache.checkpoint_count = cache.checkpoint_count.saturating_add(1);
        }
        Ok(true)
    }
}

fn packed_blob_lane_for_hash(hash: &str) -> Result<u8> {
    let hex = hash_hex(hash)?;
    let prefix = u8::from_str_radix(&hex[..2], 16)
        .expect("hash_hex validates the first two lowercase hexadecimal digits");
    Ok(prefix % PACKED_BLOB_LANE_COUNT)
}

fn packed_blob_lane_is_zero(lane: &u8) -> bool {
    *lane == 0
}

fn decode_packed_blob_index(bytes: &[u8]) -> Result<PackedBlobIndex> {
    sinomo_object_serde::from_bytes(bytes)
        .map_err(|source| ContentStoreError::Serde(source.to_string()))
}

fn encode_packed_blob_index(index: &PackedBlobIndex) -> Result<Vec<u8>> {
    sinomo_object_serde::to_bytes(index, 1, LengthMode::U32)
        .map_err(|source| ContentStoreError::Serde(source.to_string()))
}

fn encode_packed_blob_index_log_entry(entry: &PackedBlobIndexLogEntry) -> Result<Vec<u8>> {
    sinomo_object_serde::to_bytes(entry, 1, LengthMode::U32)
        .map_err(|source| ContentStoreError::Serde(source.to_string()))
}

fn encode_packed_blob_index_log_entries(entries: &[PackedBlobIndexLogEntry]) -> Result<Vec<u8>> {
    let mut records = Vec::with_capacity(entries.len());
    for entry in entries {
        let bytes = encode_packed_blob_index_log_entry(entry)?;
        u32::try_from(bytes.len()).map_err(|_| {
            ContentStoreError::Serde(format!(
                "packed blob index entry for {} is too large: {} bytes",
                entry.content_hash,
                bytes.len()
            ))
        })?;
        records.push(bytes);
    }
    let append_bytes_len = records.iter().try_fold(0_usize, |total, bytes| {
        total.checked_add(std::mem::size_of::<u32>() + bytes.len())
    });
    let Some(append_bytes_len) = append_bytes_len else {
        return Err(ContentStoreError::Serde(
            "packed blob index append batch is too large".to_string(),
        ));
    };
    let mut append_bytes = Vec::with_capacity(append_bytes_len);
    for bytes in records {
        let len = u32::try_from(bytes.len()).expect("length checked before append");
        append_bytes.extend_from_slice(&len.to_le_bytes());
        append_bytes.extend_from_slice(&bytes);
    }
    Ok(append_bytes)
}

fn packed_blob_index_file_fingerprint(
    path: &Path,
) -> Result<Option<PackedBlobIndexFileFingerprint>> {
    match fs::metadata(path) {
        Ok(metadata) => {
            #[cfg(unix)]
            use std::os::unix::fs::MetadataExt;
            Ok(Some(PackedBlobIndexFileFingerprint {
                len: metadata.len(),
                modified: metadata.modified().ok(),
                created: metadata.created().ok(),
                #[cfg(unix)]
                device: metadata.dev(),
                #[cfg(unix)]
                inode: metadata.ino(),
            }))
        }
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(ContentStoreError::Io {
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn apply_packed_blob_index_log_entry(index: &mut PackedBlobIndex, entry: &PackedBlobIndexLogEntry) {
    if let Some(location) = &entry.location {
        index
            .entries
            .insert(entry.content_hash.clone(), location.clone());
    } else {
        index.entries.remove(&entry.content_hash);
    }
    index.next_segment_id = index.next_segment_id.max(entry.next_segment_id.max(1));
}

fn replay_packed_blob_index_log_from(
    path: &Path,
    index: &mut PackedBlobIndex,
    start_offset: u64,
) -> Result<PackedBlobIndexReplayProgress> {
    let mut file = fs::File::open(path).map_err(|source| ContentStoreError::Io {
        path: path.to_owned(),
        source,
    })?;
    let file_len = file
        .metadata()
        .map_err(|source| ContentStoreError::Io {
            path: path.to_owned(),
            source,
        })?
        .len();
    if start_offset > file_len {
        return Err(ContentStoreError::Serde(format!(
            "packed blob index replay offset {start_offset} exceeds log length {file_len}"
        )));
    }
    file.seek(SeekFrom::Start(start_offset))
        .map_err(|source| ContentStoreError::Io {
            path: path.to_owned(),
            source,
        })?;
    let mut progress = PackedBlobIndexReplayProgress {
        valid_offset: start_offset,
        ..PackedBlobIndexReplayProgress::default()
    };
    while let Some((entry, record_end)) = read_next_packed_blob_index_log_entry(&mut file, path)? {
        if entry.version != PACKED_BLOB_INDEX_VERSION {
            return Err(ContentStoreError::Serde(format!(
                "unsupported packed blob index log version: {}",
                entry.version
            )));
        }
        apply_packed_blob_index_log_entry(index, &entry);
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
        progress.record_count = progress.record_count.saturating_add(1);
        progress.record_bytes = progress
            .record_bytes
            .saturating_add(record_end.saturating_sub(progress.valid_offset));
        progress.valid_offset = record_end;
    }
    Ok(progress)
}

fn read_next_packed_blob_index_log_entry(
    file: &mut fs::File,
    path: &Path,
) -> Result<Option<(PackedBlobIndexLogEntry, u64)>> {
    let Some(record) = durable_file::read_length_prefixed_record(
        file,
        path,
        durable_file::DurableRecordReadOptions::new(durable_file::DurableLengthEndian::Little),
    )
    .map_err(content_durable_file_error)?
    else {
        return Ok(None);
    };
    let entry = match sinomo_object_serde::from_bytes(&record.bytes) {
        Ok(entry) => entry,
        Err(_) if record.record_end >= record.file_len => {
            file.seek(SeekFrom::Start(record.record_start))
                .map_err(|source| ContentStoreError::Io {
                    path: path.to_owned(),
                    source,
                })?;
            return Ok(None);
        }
        Err(source) => return Err(ContentStoreError::Serde(source.to_string())),
    };
    Ok(Some((entry, record.record_end)))
}

fn truncate_torn_packed_blob_index_log_tail(path: &Path, offset: u64) -> Result<()> {
    durable_file::truncate_file_and_sync(path, offset, durable_file::DurableFileSync::All)
        .map_err(content_durable_file_error)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_blob_hash(bytes: &[u8]) -> String {
        format!("blake3:{}", blake3::hash(bytes).to_hex())
    }

    fn test_blob_for_lane(lane: u8, seed: u64) -> (String, Vec<u8>) {
        assert!(lane < PACKED_BLOB_LANE_COUNT);
        for nonce in seed.. {
            let mut bytes = vec![lane; 64];
            bytes[..8].copy_from_slice(&nonce.to_le_bytes());
            let hash = test_blob_hash(&bytes);
            if packed_blob_lane_for_hash(&hash).expect("valid test hash") == lane {
                return (hash, bytes);
            }
        }
        unreachable!("u64 test nonce space must contain every packed lane")
    }

    fn packed_lane_segment_bytes(store: &LocalContentStore, lane: u8) -> u64 {
        let root = store
            .packed_blob_lane_root(lane)
            .join(PACKED_BLOB_SEGMENT_DIR);
        if !root.is_dir() {
            return 0;
        }
        fs::read_dir(root)
            .expect("read packed segment directory")
            .map(|entry| {
                entry
                    .expect("packed segment entry")
                    .metadata()
                    .expect("packed segment metadata")
                    .len()
            })
            .fold(0_u64, u64::saturating_add)
    }

    #[test]
    fn packed_index_append_batch_is_legacy_byte_identical() {
        let entries = [
            PackedBlobIndexLogEntry {
                version: PACKED_BLOB_INDEX_VERSION,
                content_hash: "11".repeat(32),
                location: Some(PackedBlobLocation {
                    lane: 0,
                    segment_id: "0000000000000000.pack".to_string(),
                    offset: 0,
                    size_bytes: 17,
                }),
                next_segment_id: 1,
            },
            PackedBlobIndexLogEntry {
                version: PACKED_BLOB_INDEX_VERSION,
                content_hash: "22".repeat(32),
                location: Some(PackedBlobLocation {
                    lane: 0,
                    segment_id: "0000000000000000.pack".to_string(),
                    offset: 17,
                    size_bytes: 29,
                }),
                next_segment_id: 1,
            },
        ];
        let mut legacy = Vec::new();
        for entry in &entries {
            let bytes = encode_packed_blob_index_log_entry(entry).expect("encode legacy record");
            let len = u32::try_from(bytes.len()).expect("test record length");
            legacy.extend_from_slice(&len.to_le_bytes());
            legacy.extend_from_slice(&bytes);
        }

        let batched = encode_packed_blob_index_log_entries(&entries).expect("encode append batch");

        assert_eq!(batched, legacy);
    }

    #[test]
    fn packed_hash_lanes_are_independent_and_legacy_lane_remains_readable() {
        let root = sinomo_testdir::TestDir::new("dfs-packed-index-lanes");
        let store = LocalContentStore::new(&root).expect("content store");
        for lane in 0..PACKED_BLOB_LANE_COUNT {
            let (hash, bytes) = test_blob_for_lane(lane, u64::from(lane) * 1_000);
            assert!(store.write_packed_blob(&hash, &bytes).expect("lane write"));
            let location = store
                .packed_blob_location(&hash)
                .expect("lane lookup")
                .expect("lane location");
            assert_eq!(location.lane, lane);
            assert!(store.packed_blob_location_path(&location).is_file());
            assert_eq!(store.read_blob(&hash).expect("lane read"), bytes);
        }

        let (legacy_hash, legacy_bytes) = test_blob_for_lane(6, 50_000);
        {
            let _lock = store.lock_packed_blobs().expect("legacy lane lock");
            let delta = store
                .write_packed_blob_lane_batch_unlocked(
                    0,
                    &[(legacy_hash.clone(), legacy_bytes.as_slice())],
                    true,
                )
                .expect("legacy-layout write");
            assert_eq!(delta.blob_count, 1);
        }
        let legacy_location = store
            .packed_blob_location(&legacy_hash)
            .expect("legacy fallback lookup")
            .expect("legacy fallback location");
        assert_eq!(legacy_location.lane, 0);
        assert_eq!(
            store.read_blob(&legacy_hash).expect("legacy fallback read"),
            legacy_bytes
        );
    }

    #[test]
    fn packed_segment_gc_reclaims_bytes_checkpoints_index_and_survives_restart() {
        const LANE: u8 = 3;
        let root = sinomo_testdir::TestDir::new("dfs-packed-segment-gc");
        let store = LocalContentStore::new(&root).expect("content store");
        let (retained_hash, retained_bytes) = test_blob_for_lane(LANE, 1);
        let (removed_hash_a, removed_bytes_a) = test_blob_for_lane(LANE, 1_000);
        let (removed_hash_b, removed_bytes_b) = test_blob_for_lane(LANE, 2_000);
        for (hash, bytes) in [
            (&retained_hash, retained_bytes.as_slice()),
            (&removed_hash_a, removed_bytes_a.as_slice()),
            (&removed_hash_b, removed_bytes_b.as_slice()),
        ] {
            assert!(
                store
                    .write_packed_blob(hash, bytes)
                    .expect("write packed blob")
            );
        }

        let old_location = store
            .packed_blob_location(&retained_hash)
            .expect("retained packed lookup")
            .expect("retained packed location");
        assert_eq!(
            packed_lane_segment_bytes(&store, LANE),
            retained_bytes.len() as u64
                + removed_bytes_a.len() as u64
                + removed_bytes_b.len() as u64
        );
        #[cfg(unix)]
        let (_, mut reader_opened_before_gc) = store
            .open_packed_blob_reader(&retained_hash, 0, usize::MAX)
            .expect("open retained packed blob")
            .expect("retained packed reader");
        assert!(
            store
                .remove_packed_blob_index_entry(&removed_hash_a)
                .expect("tombstone first packed blob")
        );
        assert!(
            store
                .remove_packed_blob_index_entry(&removed_hash_b)
                .expect("tombstone second packed blob")
        );

        let projection = store
            .collect_packed_blob_segment_garbage(true, 1)
            .expect("project packed segment GC");
        assert_eq!(projection.eligible_segment_count, 1);
        assert_eq!(
            projection.eligible_reclaimable_bytes,
            removed_bytes_a.len() as u64 + removed_bytes_b.len() as u64
        );
        assert_eq!(projection.compacted_segment_count, 0);
        assert!(
            store.packed_blob_location_path(&old_location).is_file(),
            "dry-run must not remove the source segment"
        );

        let report = store
            .collect_packed_blob_segment_garbage(false, 1)
            .expect("compact packed segment");
        assert_eq!(report.compacted_segment_count, 1);
        assert_eq!(
            report.reclaimed_segment_bytes,
            removed_bytes_a.len() as u64 + removed_bytes_b.len() as u64
        );
        assert_eq!(report.checkpointed_index_lane_count, 1);
        assert!(report.truncated_index_log_bytes > 0);
        assert_eq!(report.retained_limit_segment_count, 0);
        assert!(
            !store.packed_blob_location_path(&old_location).exists(),
            "retired segment must be removed after replacement locations are durable"
        );
        assert_eq!(
            packed_lane_segment_bytes(&store, LANE),
            retained_bytes.len() as u64
        );
        assert_eq!(
            fs::metadata(store.packed_blob_index_log_path_for_lane(LANE))
                .expect("checkpointed packed index log")
                .len(),
            0
        );

        #[cfg(unix)]
        {
            let mut bytes_from_preopened_reader = Vec::new();
            reader_opened_before_gc
                .read_to_end(&mut bytes_from_preopened_reader)
                .expect("read retained blob from pre-GC file handle");
            assert_eq!(bytes_from_preopened_reader, retained_bytes);
            drop(reader_opened_before_gc);
        }
        drop(store);

        let reopened = LocalContentStore::new(&root).expect("reopen compacted content store");
        assert_eq!(
            reopened
                .read_blob(&retained_hash)
                .expect("read retained blob after restart"),
            retained_bytes
        );
        assert!(
            !reopened
                .has_blob(&removed_hash_a)
                .expect("first hash absent")
        );
        assert!(
            !reopened
                .has_blob(&removed_hash_b)
                .expect("second hash absent")
        );
        assert_eq!(
            reopened
                .collect_packed_blob_segment_garbage(true, 1)
                .expect("project compacted store")
                .eligible_segment_count,
            0
        );
    }

    #[test]
    fn packed_batch_lane_write_does_not_wait_for_another_lane_lock() {
        let root = sinomo_testdir::TestDir::new("dfs-packed-index-independent-locks");
        let store = LocalContentStore::new(&root).expect("content store");
        let _lane_zero_lock = store.lock_packed_blob_lane(0).expect("lane zero lock");
        let (hash, bytes) = test_blob_for_lane(1, 1);
        let writer = store.clone();
        let (send, receive) = std::sync::mpsc::channel();
        let handle = std::thread::spawn(move || {
            let result = writer
                .put_object_version_blobs_only([(
                    "packed-independent-batch-object",
                    "v1",
                    bytes.as_slice(),
                )])
                .map(|manifests| {
                    manifests
                        .first()
                        .is_some_and(|manifest| manifest.content_hash == hash)
                });
            let _ = send.send(result);
        });
        assert!(
            receive
                .recv_timeout(Duration::from_secs(2))
                .expect("lane one writer must not wait for lane zero")
                .expect("lane one write")
        );
        handle.join().expect("lane writer thread");
    }

    #[test]
    fn packed_index_updates_reuse_cache_and_replay_only_external_tail() {
        const LANE: u8 = 3;
        let root = sinomo_testdir::TestDir::new("dfs-packed-index-incremental-updates");
        let store = LocalContentStore::new(&root).expect("content store");
        let mut hashes = Vec::new();
        for index in 0..16_u64 {
            let (hash, bytes) = test_blob_for_lane(LANE, index.saturating_mul(100));
            assert!(
                store
                    .write_packed_blob(&hash, &bytes)
                    .expect("write unique packed blob")
            );
            hashes.push(hash);
        }

        let local_log_offset = {
            let cache = store
                .packed_index_cache
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            assert_eq!(
                cache.full_load_count, 1,
                "same-process updates must load the snapshot and history once"
            );
            assert_eq!(
                cache.tail_replay_record_count, 0,
                "locally appended records must advance the authoritative cache directly"
            );
            let entry = cache.entries.get(&LANE).expect("cached packed lane");
            assert_eq!(entry.index.entries.len(), hashes.len());
            entry.log_replayed_offset
        };
        assert_eq!(
            local_log_offset,
            fs::metadata(store.packed_blob_index_log_path_for_lane(LANE))
                .expect("packed index log")
                .len()
        );

        let external = LocalContentStore::new(&root).expect("external writer");
        let (external_hash, external_bytes) = test_blob_for_lane(LANE, 10_000);
        assert!(
            external
                .write_packed_blob(&external_hash, &external_bytes)
                .expect("external packed write")
        );
        let (final_hash, final_bytes) = test_blob_for_lane(LANE, 20_000);
        assert!(
            store
                .write_packed_blob(&final_hash, &final_bytes)
                .expect("local write after external tail")
        );

        let cache = store
            .packed_index_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(cache.full_load_count, 1);
        assert_eq!(
            cache.tail_replay_record_count, 1,
            "the existing cache must replay only the external writer's new record"
        );
        assert!(
            cache.tail_replay_bytes
                < fs::metadata(store.packed_blob_index_log_path_for_lane(LANE))
                    .expect("updated packed index log")
                    .len(),
            "incremental synchronization must not reread the complete log"
        );
        let index = &cache.entries.get(&LANE).expect("updated packed lane").index;
        assert!(index.entries.contains_key(&external_hash));
        assert!(index.entries.contains_key(&final_hash));
    }

    #[test]
    fn packed_reads_reuse_cache_and_replay_only_external_tail() {
        const LANE: u8 = 4;
        let root = sinomo_testdir::TestDir::new("dfs-packed-index-incremental-reads");
        let writer = LocalContentStore::new(&root).expect("writer content store");
        let blobs = (0..16_u64)
            .map(|index| test_blob_for_lane(LANE, index.saturating_mul(100)))
            .collect::<Vec<_>>();
        for (hash, bytes) in &blobs {
            assert!(
                writer
                    .write_packed_blob(hash, bytes)
                    .expect("write unique packed blob")
            );
        }
        drop(writer);

        let reader = LocalContentStore::new(&root).expect("reader content store");
        for (hash, bytes) in &blobs {
            assert_eq!(reader.read_blob(hash).expect("read packed blob"), *bytes);
        }
        let (full_load_count, replay_count_before_external) = {
            let cache = reader
                .packed_index_cache
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            (cache.full_load_count, cache.tail_replay_record_count)
        };
        assert_eq!(
            full_load_count, 1,
            "repeated reads in one lane must load its index only once"
        );

        let external = LocalContentStore::new(&root).expect("external writer");
        let (external_hash, external_bytes) = test_blob_for_lane(LANE, 10_000);
        assert!(
            external
                .write_packed_blob(&external_hash, &external_bytes)
                .expect("append external packed blob")
        );
        assert_eq!(
            reader
                .read_blob(&external_hash)
                .expect("read externally appended packed blob"),
            external_bytes
        );
        let cache = reader
            .packed_index_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(
            cache.full_load_count, 1,
            "an external append must not force a complete lane reload"
        );
        assert_eq!(
            cache.tail_replay_record_count,
            replay_count_before_external.saturating_add(1),
            "the reader must replay the external writer's one-record tail"
        );
    }

    #[test]
    fn packed_index_update_truncates_torn_tail_before_appending() {
        const LANE: u8 = 2;
        let root = sinomo_testdir::TestDir::new("dfs-packed-index-torn-tail");
        let initial = LocalContentStore::new(&root).expect("initial content store");
        let (first_hash, first_bytes) = test_blob_for_lane(LANE, 1);
        let (second_hash, second_bytes) = test_blob_for_lane(LANE, 1_000);
        initial
            .write_packed_blob(&first_hash, &first_bytes)
            .expect("first packed write");
        initial
            .write_packed_blob(&second_hash, &second_bytes)
            .expect("second packed write");
        drop(initial);

        let lane_store = LocalContentStore::new(&root).expect("lane path store");
        let log_path = lane_store.packed_blob_index_log_path_for_lane(LANE);
        drop(lane_store);
        let valid_log_len = fs::metadata(&log_path).expect("valid packed log").len();
        let mut log = fs::OpenOptions::new()
            .append(true)
            .open(&log_path)
            .expect("open packed log for torn append");
        log.write_all(&128_u32.to_le_bytes())
            .and_then(|()| log.write_all(b"torn"))
            .and_then(|()| log.sync_all())
            .expect("append torn packed log tail");
        assert!(fs::metadata(&log_path).expect("torn packed log").len() > valid_log_len);

        let recovered = LocalContentStore::new(&root).expect("recovered content store");
        let (third_hash, third_bytes) = test_blob_for_lane(LANE, 2_000);
        recovered
            .write_packed_blob(&third_hash, &third_bytes)
            .expect("write after torn tail");
        {
            let cache = recovered
                .packed_index_cache
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            assert_eq!(cache.full_load_count, 1);
            assert_eq!(cache.tail_replay_record_count, 2);
            assert_eq!(
                cache
                    .entries
                    .get(&LANE)
                    .expect("recovered packed cache")
                    .index
                    .entries
                    .len(),
                3
            );
        }

        drop(recovered);
        let reopened = LocalContentStore::new(&root).expect("reopen recovered store");
        for (hash, expected) in [
            (&first_hash, first_bytes.as_slice()),
            (&second_hash, second_bytes.as_slice()),
            (&third_hash, third_bytes.as_slice()),
        ] {
            assert_eq!(
                reopened
                    .read_blob(hash)
                    .expect("read recovered packed blob"),
                expected
            );
        }
    }

    #[test]
    fn packed_index_automatically_checkpoints_a_bounded_log() {
        const LANE: u8 = 4;
        let root = sinomo_testdir::TestDir::new("dfs-packed-index-automatic-checkpoint");
        let store = LocalContentStore::new(&root).expect("content store");
        let blobs = (0..512_u64)
            .map(|index| test_blob_for_lane(LANE, index.saturating_mul(100)))
            .collect::<Vec<_>>();
        let blob_refs = blobs
            .iter()
            .map(|(hash, bytes)| (hash.clone(), bytes.as_slice()))
            .collect::<Vec<_>>();
        {
            let _locks = store
                .lock_all_packed_blob_lanes()
                .expect("packed lane write locks");
            let delta = store
                .write_packed_blobs_unlocked(&blob_refs)
                .expect("batched packed writes");
            assert_eq!(delta.blob_count, blobs.len() as i128);
        }

        assert!(store.packed_blob_index_path_for_lane(LANE).is_file());
        assert_eq!(
            fs::metadata(store.packed_blob_index_log_path_for_lane(LANE))
                .expect("checkpointed packed log")
                .len(),
            0,
            "periodic checkpointing must bound the append log"
        );
        let cache = store
            .packed_index_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(cache.checkpoint_count, 1);
        assert_eq!(
            cache
                .entries
                .get(&LANE)
                .expect("checkpointed cache")
                .index
                .entries
                .len(),
            blobs.len()
        );
    }

    #[test]
    fn packed_index_checkpoint_is_reopen_safe_with_post_checkpoint_tail() {
        const LANE: u8 = 5;
        let root = sinomo_testdir::TestDir::new("dfs-packed-index-checkpoint");
        let store = LocalContentStore::new(&root).expect("content store");
        let (retained_hash, retained_bytes) = test_blob_for_lane(LANE, 1);
        let (removed_hash, removed_bytes) = test_blob_for_lane(LANE, 1_000);
        store
            .write_packed_blob(&retained_hash, &retained_bytes)
            .expect("write retained packed blob");
        store
            .write_packed_blob(&removed_hash, &removed_bytes)
            .expect("write removable packed blob");
        store
            .remove_packed_blob_index_entry(&removed_hash)
            .expect("remove packed index entry");
        let pre_checkpoint_log = fs::read(store.packed_blob_index_log_path_for_lane(LANE))
            .expect("read pre-checkpoint packed log");

        {
            let _lock = store.lock_packed_blob_lane(LANE).expect("checkpoint lock");
            let index = store
                .read_packed_blob_index_lane_for_update_unlocked(LANE)
                .expect("load checkpoint index");
            assert!(!index.entries.contains_key(&removed_hash));
            drop(index);
            assert!(
                store
                    .checkpoint_packed_blob_index_lane_unlocked(LANE)
                    .expect("checkpoint packed index")
            );
        }
        assert!(store.packed_blob_index_path_for_lane(LANE).is_file());
        assert_eq!(
            fs::metadata(store.packed_blob_index_log_path_for_lane(LANE))
                .expect("checkpointed packed log")
                .len(),
            0
        );

        // Model a crash after the atomic snapshot rename but before log
        // truncation. Replaying the old prefix over its checkpoint must remain
        // idempotent, including the removal tombstone.
        let log_path = store.packed_blob_index_log_path_for_lane(LANE);
        let mut restored_log = fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&log_path)
            .expect("restore pre-checkpoint log");
        restored_log
            .write_all(&pre_checkpoint_log)
            .and_then(|()| restored_log.sync_all())
            .expect("sync restored pre-checkpoint log");

        let (tail_hash, tail_bytes) = test_blob_for_lane(LANE, 2_000);
        store
            .write_packed_blob(&tail_hash, &tail_bytes)
            .expect("write post-checkpoint tail");
        assert!(
            fs::metadata(store.packed_blob_index_log_path_for_lane(LANE))
                .expect("post-checkpoint packed log")
                .len()
                > 0
        );
        {
            let cache = store
                .packed_index_cache
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            assert_eq!(cache.checkpoint_count, 1);
            assert!(cache.full_load_count >= 1);
        }

        drop(store);
        let reopened = LocalContentStore::new(&root).expect("reopened checkpointed store");
        assert_eq!(
            reopened
                .read_blob(&retained_hash)
                .expect("read checkpointed blob"),
            retained_bytes
        );
        assert_eq!(
            reopened
                .read_blob(&tail_hash)
                .expect("read post-checkpoint blob"),
            tail_bytes
        );
        assert!(
            reopened
                .packed_blob_location(&removed_hash)
                .expect("removed packed lookup")
                .is_none()
        );
    }
}
