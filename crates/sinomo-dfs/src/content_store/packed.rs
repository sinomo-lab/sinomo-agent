use super::{
    BlobRangeReader, BlobReader, ContentStoreError, ContentStoreStatsDelta, LocalContentStore,
    Result, content_durable_file_error, hash_hex,
};
use crate::content_store::fs_ops::{create_dir_all, file_len_if_exists, sync_dir};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sinomo_core::object::{LengthMode, serde_ext as sinomo_object_serde};
use sinomo_core::storage::durable_file;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

pub(super) const PACKED_BLOB_INDEX_VERSION: u32 = 1;
pub(super) const PACKED_BLOB_DIR: &str = "blob-packs";
pub(super) const PACKED_BLOB_SEGMENT_DIR: &str = "segments";
pub(super) const PACKED_BLOB_INDEX_FILE: &str = "index.sobj";
pub(super) const PACKED_BLOB_INDEX_LOG_FILE: &str = "index.slog";
pub(super) const PACKED_BLOB_LOCK_FILE: &str = "pack.lock";
pub(super) const PACKED_BLOB_SMALL_LIMIT_BYTES: u64 = 16 * 1024;
pub(super) const PACKED_BLOB_SEGMENT_MAX_BYTES: u64 = 8 * 1024 * 1024;
const PACKED_BLOB_INDEX_CACHE_TTL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct PackedBlobIndex {
    pub(super) version: u32,
    pub(super) next_segment_id: u64,
    #[serde(default)]
    pub(super) entries: BTreeMap<String, PackedBlobLocation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct PackedBlobLocation {
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
    entry: Option<PackedBlobIndexCacheEntry>,
}

#[derive(Debug, Clone)]
struct PackedBlobIndexCacheEntry {
    loaded_at: Instant,
    fingerprint: PackedBlobIndexCacheFingerprint,
    index: Arc<PackedBlobIndex>,
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
}

#[derive(Debug)]
struct OpenedPackedBlobRange {
    file: fs::File,
    offset: u64,
    size_bytes: u64,
    remaining_bytes: u64,
}

impl LocalContentStore {
    pub(super) fn lock_packed_blobs(&self) -> Result<fs::File> {
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

    fn lock_packed_blobs_for_read(&self) -> Result<Option<fs::File>> {
        let path = self.root.join(PACKED_BLOB_DIR).join(PACKED_BLOB_LOCK_FILE);
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
        file.lock_shared()
            .map_err(|source| ContentStoreError::Io { path, source })?;
        Ok(Some(file))
    }

    pub(super) fn read_packed_blob_index_unlocked(&self) -> Result<PackedBlobIndex> {
        self.read_packed_blob_index_unlocked_with_tail_policy(false)
    }

    pub(super) fn read_packed_blob_index_for_update_unlocked(&self) -> Result<PackedBlobIndex> {
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

    fn read_cached_packed_blob_index_for_read(&self) -> Result<Arc<PackedBlobIndex>> {
        let now = Instant::now();
        let fingerprint = self.packed_blob_index_cache_fingerprint()?;
        {
            let cache = self
                .packed_index_cache
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(entry) = &cache.entry
                && now.saturating_duration_since(entry.loaded_at) <= PACKED_BLOB_INDEX_CACHE_TTL
                && entry.fingerprint == fingerprint
            {
                return Ok(Arc::clone(&entry.index));
            }
        }

        let _lock = self.lock_packed_blobs_for_read()?;
        let index = Arc::new(self.read_packed_blob_index_unlocked()?);
        let fingerprint = self.packed_blob_index_cache_fingerprint()?;
        let mut cache = self
            .packed_index_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        cache.entry = Some(PackedBlobIndexCacheEntry {
            loaded_at: now,
            fingerprint,
            index: Arc::clone(&index),
        });
        Ok(index)
    }

    fn packed_blob_index_cache_fingerprint(&self) -> Result<PackedBlobIndexCacheFingerprint> {
        Ok(PackedBlobIndexCacheFingerprint {
            index: packed_blob_index_file_fingerprint(&self.packed_blob_index_path())?,
            log: packed_blob_index_file_fingerprint(&self.packed_blob_index_log_path())?,
        })
    }

    fn invalidate_packed_blob_index_cache(&self) {
        let mut cache = self
            .packed_index_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        cache.entry = None;
    }

    pub(super) fn packed_blob_location(&self, hash: &str) -> Result<Option<PackedBlobLocation>> {
        hash_hex(hash)?;
        let index = self.read_cached_packed_blob_index_for_read()?;
        Ok(index.entries.get(hash).cloned())
    }

    pub(super) fn write_packed_blob(&self, hash: &str, bytes: &[u8]) -> Result<bool> {
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
        self.invalidate_packed_blob_index_cache();
        Ok(true)
    }

    pub(super) fn write_packed_blobs_unlocked(
        &self,
        index: &mut PackedBlobIndex,
        blobs: &[(String, &[u8])],
    ) -> Result<ContentStoreStatsDelta> {
        self.write_packed_blobs_unlocked_with_sync(index, blobs, true)
    }

    pub(super) fn write_packed_blobs_unlocked_relaxed(
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

        struct PendingPackedSegmentWrite {
            path: PathBuf,
            bytes: Vec<u8>,
        }

        let mut segment_writes = BTreeMap::<String, PendingPackedSegmentWrite>::new();
        let mut segment_lengths = BTreeMap::<String, u64>::new();
        let mut new_segment_parent_dirs = BTreeSet::<PathBuf>::new();
        let mut pending_hashes = BTreeSet::<String>::new();
        let mut records = Vec::new();
        let mut stats_delta = ContentStoreStatsDelta::default();

        for (hash, bytes) in blobs {
            hash_hex(hash)?;
            if let Some(location) = index.entries.get(hash)
                && (pending_hashes.contains(hash)
                    || self.packed_blob_location_readable(location)?)
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
            self.append_packed_blob_index_log_entries_unlocked(&records)?;
        } else {
            self.append_packed_blob_index_log_entries_unlocked_relaxed(&records)?;
        }
        if !records.is_empty() {
            self.invalidate_packed_blob_index_cache();
        }
        Ok(stats_delta)
    }

    pub(super) fn remove_packed_blob_index_entry(&self, hash: &str) -> Result<bool> {
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
            self.invalidate_packed_blob_index_cache();
        }
        Ok(removed)
    }

    pub(super) fn open_packed_blob_reader(
        &self,
        location: &PackedBlobLocation,
        offset: u64,
        max_bytes: usize,
    ) -> Result<BlobReader> {
        let opened = self.open_packed_blob_range_file(location, offset, max_bytes)?;
        Ok(BlobReader::with_remaining(
            opened.file,
            opened.size_bytes,
            opened.remaining_bytes,
        ))
    }

    pub(super) fn open_packed_blob_range_reader(
        &self,
        location: &PackedBlobLocation,
        offset: u64,
        max_bytes: usize,
    ) -> Result<BlobRangeReader> {
        let opened = self.open_packed_blob_range_file(location, offset, max_bytes)?;
        Ok(BlobRangeReader::new(
            opened.file,
            opened.offset,
            opened.size_bytes,
            opened.remaining_bytes,
        ))
    }

    fn open_packed_blob_range_file(
        &self,
        location: &PackedBlobLocation,
        offset: u64,
        max_bytes: usize,
    ) -> Result<OpenedPackedBlobRange> {
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
        let path = self.packed_blob_segment_path(&location.segment_id);
        let Some(segment_len) = file_len_if_exists(&path)? else {
            return Ok(false);
        };
        Ok(location
            .offset
            .checked_add(location.size_bytes)
            .is_some_and(|end| end <= segment_len))
    }

    pub(super) fn packed_blob_index_path(&self) -> PathBuf {
        self.root.join(PACKED_BLOB_DIR).join(PACKED_BLOB_INDEX_FILE)
    }

    pub(super) fn packed_blob_index_log_path(&self) -> PathBuf {
        self.root
            .join(PACKED_BLOB_DIR)
            .join(PACKED_BLOB_INDEX_LOG_FILE)
    }

    pub(super) fn packed_blob_segment_path(&self, segment_id: &str) -> PathBuf {
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
        for bytes in records {
            let len = u32::try_from(bytes.len()).expect("length checked before append");
            file.write_all(&len.to_le_bytes())
                .and_then(|()| file.write_all(&bytes))
                .map_err(|source| ContentStoreError::Io {
                    path: path.clone(),
                    source,
                })?;
        }
        if sync_writes {
            file.sync_all().map_err(|source| ContentStoreError::Io {
                path: path.clone(),
                source,
            })?;
            if let Some(parent) = path.parent() {
                sync_dir(parent)?;
            }
        }
        Ok(())
    }
}

fn decode_packed_blob_index(bytes: &[u8]) -> Result<PackedBlobIndex> {
    sinomo_object_serde::from_bytes(bytes)
        .map_err(|source| ContentStoreError::Serde(source.to_string()))
}

fn encode_packed_blob_index_log_entry(entry: &PackedBlobIndexLogEntry) -> Result<Vec<u8>> {
    sinomo_object_serde::to_bytes(entry, 1, LengthMode::U32)
        .map_err(|source| ContentStoreError::Serde(source.to_string()))
}

fn packed_blob_index_file_fingerprint(
    path: &Path,
) -> Result<Option<PackedBlobIndexFileFingerprint>> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(Some(PackedBlobIndexFileFingerprint {
            len: metadata.len(),
            modified: metadata.modified().ok(),
        })),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(ContentStoreError::Io {
            path: path.to_path_buf(),
            source,
        }),
    }
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
