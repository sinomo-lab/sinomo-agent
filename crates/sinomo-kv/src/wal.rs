use crate::internal::*;

pub(crate) const MAX_WAL_RECORD_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_OBJECT_FILE_READ_BYTES: u64 = durable_file::DEFAULT_OBJECT_FILE_MAX_READ_BYTES;
// A quorum command retains this lock while it replicates over the network. A
// concurrent local maintenance/read path must tolerate that normal WAN-sized
// critical section, but the wait remains below the outer control-plane timeout
// so genuine lock leaks still fail in bounded time.
pub(crate) const RAFT_NODE_LOCK_WAIT: Duration = Duration::from_secs(5);
pub(crate) const RAFT_NODE_LOCK_RETRY_INITIAL: Duration = Duration::from_millis(2);
pub(crate) const RAFT_NODE_LOCK_RETRY_MAX: Duration = Duration::from_millis(40);
pub(crate) static OBJECT_FILE_WRITE_SEQUENCE: AtomicU64 = AtomicU64::new(0);
pub(crate) static KV_READ_CACHE_CLOCK: AtomicU64 = AtomicU64::new(1);

/// Cache of last-written object files. The cluster directory holding these WAL /
/// snapshot files can be shared by the main and shadow daemons, so the fast-path
/// skip is guarded by a `stat`: an entry is trusted only when the file's length,
/// mtime, and inode still match what we wrote, so a write by another process is
/// detected and falls back to a real read + compare.
pub(crate) static LAST_WRITTEN_OBJECT_FILES: LazyLock<durable_file::ObjectFileWriteMarkCache> =
    LazyLock::new(|| durable_file::ObjectFileWriteMarkCache::new(FILE_CACHE_CAPACITY));

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvWalPaths {
    pub root: PathBuf,
    pub lock: PathBuf,
    pub hard_state: PathBuf,
    pub wal: PathBuf,
    pub snapshot: PathBuf,
}

impl KvWalPaths {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        Self {
            lock: root.join("node.lock"),
            hard_state: root.join("hard-state.sobj"),
            wal: root.join("wal.sobjlog"),
            snapshot: root.join("snapshot.sobj"),
            root,
        }
    }

    pub fn ensure(&self) -> Result<(), KvError> {
        fs::create_dir_all(&self.root).map_err(|source| KvError::Io {
            path: self.root.display().to_string(),
            detail: source.to_string(),
        })
    }

    pub fn has_durable_state(&self) -> Result<bool, KvError> {
        Ok(path_exists(&self.hard_state)?
            || path_exists(&self.wal)?
            || path_exists(&self.snapshot)?)
    }

    /// Reads only the persisted hard state (term/vote/leader/commit) without
    /// opening the full raft node. Opening the node replays the entire WAL — up to
    /// the retained tail, ~MBs — which is wasteful for read-only leadership/term
    /// queries that only need this tiny, atomically-rewritten file. The value is a
    /// cheap point read: it reflects the last durably persisted hard state and is
    /// never torn (writes go through a temp-file rename). Defaults when absent.
    pub fn read_persisted_hard_state(&self) -> Result<RaftHardState, KvError> {
        let identity = optional_file_identity(&self.hard_state);
        if let Some(identity) = identity
            && let Some(cached) = cache_lookup_hard_state(&self.hard_state, identity)
        {
            return Ok(cached);
        }
        #[cfg(test)]
        record_persisted_hard_state_actual_read_for_test(&self.hard_state);
        let hard_state =
            read_optional_object::<RaftHardState>(&self.hard_state)?.unwrap_or_default();
        if let Some(identity) = identity {
            cache_store_hard_state(&self.hard_state, identity, hard_state.clone());
        }
        Ok(hard_state)
    }

    pub fn write_persisted_hard_state(&self, hard_state: &RaftHardState) -> Result<(), KvError> {
        write_object_file(&self.hard_state, hard_state)?;
        if let Some(identity) = optional_file_identity(&self.hard_state) {
            cache_store_hard_state(&self.hard_state, identity, hard_state.clone());
        }
        Ok(())
    }
}

/// Process-wide memoization of parsed materialized snapshots, keyed by file path
/// and validated by file identity. Lets repeated reads of an unchanged snapshot
/// skip the disk read + deserialization entirely — the dominant idle read cost.
/// The writer updates this on every write, so intra-process reads are always
/// coherent; cross-process edits are caught by identity validation.
pub(crate) struct MaterializedSnapshotCacheEntry {
    pub(crate) identity: FileIdentity,
    pub(crate) snapshot: Arc<KvSnapshot>,
    pub(crate) last_used: u64,
}

pub(crate) struct RaftViewCacheEntry {
    pub(crate) identity: RaftViewIdentity,
    pub(crate) snapshot: Arc<KvSnapshot>,
    pub(crate) last_used: u64,
}

pub(crate) struct HardStateCacheEntry {
    pub(crate) identity: Option<FileIdentity>,
    pub(crate) hard_state: RaftHardState,
    pub(crate) last_used: u64,
}

pub(crate) static MATERIALIZED_SNAPSHOT_CACHE: OnceLock<
    Mutex<HashMap<PathBuf, MaterializedSnapshotCacheEntry>>,
> = OnceLock::new();
pub(crate) static RAFT_VIEW_CACHE: OnceLock<Mutex<HashMap<PathBuf, RaftViewCacheEntry>>> =
    OnceLock::new();
pub(crate) static HARD_STATE_CACHE: OnceLock<Mutex<HashMap<PathBuf, HardStateCacheEntry>>> =
    OnceLock::new();

pub(crate) fn materialized_snapshot_cache()
-> &'static Mutex<HashMap<PathBuf, MaterializedSnapshotCacheEntry>> {
    MATERIALIZED_SNAPSHOT_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) fn raft_view_cache() -> &'static Mutex<HashMap<PathBuf, RaftViewCacheEntry>> {
    RAFT_VIEW_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) fn hard_state_cache() -> &'static Mutex<HashMap<PathBuf, HardStateCacheEntry>> {
    HARD_STATE_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Identity used to validate a cached file parse. Every persisted object/snapshot
/// file is written via a temp file + atomic rename, so a rewrite always lands on a
/// fresh inode. Including `ino` closes the window where two writes share an mtime
/// (filesystem mtime granularity) *and* an identical length — which `(mtime, len)`
/// alone cannot distinguish, so a stale parse could otherwise be served. The
/// comparison is all-or-nothing (derived `PartialEq`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct FileIdentity {
    pub(crate) mtime: Option<SystemTime>,
    pub(crate) len: u64,
    pub(crate) ino: u64,
}

/// Point-in-time identity for a durable Raft view. The WAL is append/rewrite in
/// place, hard state and snapshots use object-file rewrites, and any of those
/// changing means a cached decoded store is no longer authoritative.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct RaftViewIdentity {
    pub(crate) hard_state: Option<FileIdentity>,
    pub(crate) wal: Option<FileIdentity>,
    pub(crate) snapshot: Option<FileIdentity>,
}

/// Upper bound on entries kept in each process-global file cache. The live set of
/// snapshot/object-file paths is tiny in normal operation; this cap only guards
/// against unbounded growth under pathological path churn. Read caches evict the
/// oldest entry so a noisy set of roots cannot repeatedly cold-start hot roots.
pub(crate) const FILE_CACHE_CAPACITY: usize = 4096;

pub(crate) fn path_exists(path: &Path) -> Result<bool, KvError> {
    #[cfg(test)]
    record_durable_state_path_exists_probe_for_test(path);
    match fs::metadata(path) {
        Ok(_) => Ok(true),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(KvError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        }),
    }
}

#[cfg(test)]
pub(crate) static DURABLE_STATE_PATH_EXISTS_PROBES_FOR_TEST: OnceLock<
    Mutex<HashMap<PathBuf, u64>>,
> = OnceLock::new();

#[cfg(test)]
pub(crate) fn durable_state_path_exists_probes_for_test() -> &'static Mutex<HashMap<PathBuf, u64>> {
    DURABLE_STATE_PATH_EXISTS_PROBES_FOR_TEST.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(test)]
pub(crate) fn record_durable_state_path_exists_probe_for_test(path: &Path) {
    if let Ok(mut probes) = durable_state_path_exists_probes_for_test().lock() {
        *probes.entry(path.to_path_buf()).or_default() += 1;
    }
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn reset_durable_state_path_exists_probes_for_test(paths: &KvWalPaths) {
    if let Ok(mut probes) = durable_state_path_exists_probes_for_test().lock() {
        probes.remove(&paths.hard_state);
        probes.remove(&paths.wal);
        probes.remove(&paths.snapshot);
    }
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn durable_state_path_exists_probe_count_for_test(paths: &KvWalPaths) -> u64 {
    durable_state_path_exists_probes_for_test()
        .lock()
        .ok()
        .map(|probes| {
            probes.get(&paths.hard_state).copied().unwrap_or_default()
                + probes.get(&paths.wal).copied().unwrap_or_default()
                + probes.get(&paths.snapshot).copied().unwrap_or_default()
        })
        .unwrap_or_default()
}

pub(crate) fn file_identity(path: &Path) -> Option<FileIdentity> {
    let metadata = fs::metadata(path).ok()?;
    Some(FileIdentity {
        mtime: metadata.modified().ok(),
        len: metadata.len(),
        ino: file_ino(&metadata),
    })
}

pub(crate) fn optional_file_identity(path: &Path) -> Option<Option<FileIdentity>> {
    match fs::metadata(path) {
        Ok(metadata) => {
            let identity = FileIdentity {
                mtime: metadata.modified().ok(),
                len: metadata.len(),
                ino: file_ino(&metadata),
            };
            identity.mtime.is_some().then_some(Some(identity))
        }
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Some(None),
        Err(_) => None,
    }
}

/// Returns `Some(None)` for an absent file, `Some(Some(identity))` for a
/// cacheable present file, and `None` for a present file whose identity cannot be
/// validated. Filesystem errors other than not-found are preserved for callers
/// that use this to decide whether durable raft state exists.
pub(crate) fn optional_file_identity_for_read(
    path: &Path,
) -> Result<Option<Option<FileIdentity>>, KvError> {
    match fs::metadata(path) {
        Ok(metadata) => {
            let identity = FileIdentity {
                mtime: metadata.modified().ok(),
                len: metadata.len(),
                ino: file_ino(&metadata),
            };
            Ok(identity.mtime.is_some().then_some(Some(identity)))
        }
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(Some(None)),
        Err(source) => Err(KvError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        }),
    }
}

pub(crate) fn raft_view_identity(paths: &KvWalPaths) -> Option<RaftViewIdentity> {
    Some(RaftViewIdentity {
        hard_state: optional_file_identity(&paths.hard_state)?,
        wal: optional_file_identity(&paths.wal)?,
        snapshot: optional_file_identity(&paths.snapshot)?,
    })
}

pub(crate) fn raft_view_identity_for_read(
    paths: &KvWalPaths,
) -> Result<(bool, Option<RaftViewIdentity>), KvError> {
    let hard_state = optional_file_identity_for_read(&paths.hard_state)?;
    let wal = optional_file_identity_for_read(&paths.wal)?;
    let snapshot = optional_file_identity_for_read(&paths.snapshot)?;
    let has_durable_state = hard_state != Some(None) || wal != Some(None) || snapshot != Some(None);
    let identity = match (hard_state, wal, snapshot) {
        (Some(hard_state), Some(wal), Some(snapshot)) => Some(RaftViewIdentity {
            hard_state,
            wal,
            snapshot,
        }),
        _ => None,
    };
    Ok((has_durable_state, identity))
}

pub(crate) fn kv_read_cache_tick() -> u64 {
    KV_READ_CACHE_CLOCK.fetch_add(1, Ordering::Relaxed)
}

pub(crate) fn evict_materialized_snapshot_cache_entry_if_needed(
    guard: &mut HashMap<PathBuf, MaterializedSnapshotCacheEntry>,
    protected: &Path,
) {
    if guard.contains_key(protected) || guard.len() < FILE_CACHE_CAPACITY {
        return;
    }
    let eviction_candidate = guard
        .iter()
        .filter(|(path, _)| path.as_path() != protected)
        .min_by_key(|(_, entry)| entry.last_used)
        .map(|(path, _)| path.clone());
    if let Some(path) = eviction_candidate {
        guard.remove(&path);
    }
}

pub(crate) fn evict_raft_view_cache_entry_if_needed(
    guard: &mut HashMap<PathBuf, RaftViewCacheEntry>,
    protected: &Path,
) {
    if guard.contains_key(protected) || guard.len() < FILE_CACHE_CAPACITY {
        return;
    }
    let eviction_candidate = guard
        .iter()
        .filter(|(path, _)| path.as_path() != protected)
        .min_by_key(|(_, entry)| entry.last_used)
        .map(|(path, _)| path.clone());
    if let Some(path) = eviction_candidate {
        guard.remove(&path);
    }
}

pub(crate) fn evict_hard_state_cache_entry_if_needed(
    guard: &mut HashMap<PathBuf, HardStateCacheEntry>,
    protected: &Path,
) {
    if guard.contains_key(protected) || guard.len() < FILE_CACHE_CAPACITY {
        return;
    }
    let eviction_candidate = guard
        .iter()
        .filter(|(path, _)| path.as_path() != protected)
        .min_by_key(|(_, entry)| entry.last_used)
        .map(|(path, _)| path.clone());
    if let Some(path) = eviction_candidate {
        guard.remove(&path);
    }
}

pub(crate) fn cache_lookup_snapshot(
    path: &Path,
    identity: FileIdentity,
) -> Option<Arc<KvSnapshot>> {
    let mut guard = materialized_snapshot_cache().lock().ok()?;
    let entry = guard.get_mut(path)?;
    // mtime is the primary key; require it to be present so we never serve a hit
    // we cannot validate, then require the full identity (mtime, len, inode).
    if identity.mtime.is_some() && entry.identity == identity {
        entry.last_used = kv_read_cache_tick();
        Some(entry.snapshot.clone())
    } else {
        None
    }
}

pub(crate) fn cache_store_snapshot(path: &Path, identity: FileIdentity, snapshot: Arc<KvSnapshot>) {
    if identity.mtime.is_none() {
        return;
    }
    if let Ok(mut guard) = materialized_snapshot_cache().lock() {
        evict_materialized_snapshot_cache_entry_if_needed(&mut guard, path);
        guard.insert(
            path.to_path_buf(),
            MaterializedSnapshotCacheEntry {
                identity,
                snapshot,
                last_used: kv_read_cache_tick(),
            },
        );
    }
}

pub(crate) fn cache_lookup_hard_state(
    path: &Path,
    identity: Option<FileIdentity>,
) -> Option<RaftHardState> {
    let mut guard = hard_state_cache().lock().ok()?;
    let entry = guard.get_mut(path)?;
    if entry.identity != identity {
        return None;
    }
    entry.last_used = kv_read_cache_tick();
    let hard_state = entry.hard_state.clone();
    drop(guard);
    // Re-stat after cloning the cached hard state so a concurrent replace cannot
    // slip between the caller's identity read and the return. If it changes after
    // this check, the read linearizes before that change.
    (optional_file_identity(path) == Some(identity)).then_some(hard_state)
}

pub(crate) fn cache_store_hard_state(
    path: &Path,
    identity: Option<FileIdentity>,
    hard_state: RaftHardState,
) {
    if let Ok(mut guard) = hard_state_cache().lock() {
        evict_hard_state_cache_entry_if_needed(&mut guard, path);
        guard.insert(
            path.to_path_buf(),
            HardStateCacheEntry {
                identity,
                hard_state,
                last_used: kv_read_cache_tick(),
            },
        );
    }
}

#[cfg(test)]
pub(crate) static PERSISTED_HARD_STATE_READS_FOR_TEST: OnceLock<Mutex<HashMap<PathBuf, u64>>> =
    OnceLock::new();

#[cfg(test)]
pub(crate) fn persisted_hard_state_reads_for_test() -> &'static Mutex<HashMap<PathBuf, u64>> {
    PERSISTED_HARD_STATE_READS_FOR_TEST.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(test)]
pub(crate) fn record_persisted_hard_state_actual_read_for_test(path: &Path) {
    if let Ok(mut reads) = persisted_hard_state_reads_for_test().lock() {
        *reads.entry(path.to_path_buf()).or_default() += 1;
    }
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn reset_persisted_hard_state_actual_reads_for_test(path: &Path) {
    if let Ok(mut reads) = persisted_hard_state_reads_for_test().lock() {
        reads.remove(path);
    }
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn persisted_hard_state_actual_reads_for_test(path: &Path) -> u64 {
    persisted_hard_state_reads_for_test()
        .lock()
        .ok()
        .and_then(|reads| reads.get(path).copied())
        .unwrap_or_default()
}

pub(crate) fn cache_lookup_raft_view(
    paths: &KvWalPaths,
    identity: RaftViewIdentity,
) -> Option<Arc<KvSnapshot>> {
    let mut guard = raft_view_cache().lock().ok()?;
    let entry = guard.get_mut(&paths.root)?;
    if entry.identity != identity {
        return None;
    }
    entry.last_used = kv_read_cache_tick();
    let snapshot = entry.snapshot.clone();
    drop(guard);
    // Re-stat after cloning the cached view so a concurrent append/rewrite cannot
    // slip between the first identity read and the return. If the files change
    // after this check, the read linearizes before that change.
    (raft_view_identity(paths) == Some(identity)).then_some(snapshot)
}

pub(crate) fn cache_store_raft_view(
    paths: &KvWalPaths,
    identity: RaftViewIdentity,
    snapshot: Arc<KvSnapshot>,
) {
    if let Ok(mut guard) = raft_view_cache().lock() {
        evict_raft_view_cache_entry_if_needed(&mut guard, &paths.root);
        guard.insert(
            paths.root.clone(),
            RaftViewCacheEntry {
                identity,
                snapshot,
                last_used: kv_read_cache_tick(),
            },
        );
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct WalRead {
    pub(crate) entries: Vec<KvLogEntry>,
    pub(crate) truncated: bool,
}

pub(crate) fn read_wal_entries(
    path: &Path,
    max_index: Option<u64>,
    retain_after_index: u64,
) -> Result<WalRead, KvError> {
    let mut metric = KvStorageMetricGuard::start("wal_read", "sinomo.kv.wal.read.us");
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            metric.succeed();
            return Ok(WalRead::default());
        }
        Err(source) => {
            return Err(KvError::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            });
        }
    };
    fs2::FileExt::lock_shared(&file).map_err(|source| KvError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    let mut entries = Vec::new();
    let mut truncated = false;
    let mut record_number = 0;
    let file_len = file
        .metadata()
        .map_err(|source| KvError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?
        .len();
    loop {
        let record_start = file.stream_position().map_err(|source| KvError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
        let record = match durable_file::read_length_prefixed_record(
            &mut file,
            path,
            durable_file::DurableRecordReadOptions::new(durable_file::DurableLengthEndian::Big)
                .max_record_bytes(MAX_WAL_RECORD_BYTES),
        ) {
            Ok(Some(record)) => record,
            Ok(None) => {
                if record_start < file_len {
                    truncated = true;
                }
                break;
            }
            Err(error) if error.detail().contains("length-prefixed record is") => {
                truncated = true;
                break;
            }
            Err(error) => return Err(kv_durable_file_error(error)),
        };
        record_number += 1;
        if record.bytes.is_empty() {
            truncated = true;
            break;
        }
        match sinomo_object_serde::from_bytes(&record.bytes) {
            Ok(entry) => {
                let entry: KvLogEntry = entry;
                if entry.index <= retain_after_index {
                    truncated = true;
                    continue;
                }
                if max_index.is_some_and(|limit| entry.index > limit) {
                    truncated = true;
                    break;
                }
                let requested_bytes = entries
                    .len()
                    .checked_add(1)
                    .and_then(|items| items.checked_mul(std::mem::size_of::<KvLogEntry>().max(1)))
                    .unwrap_or(usize::MAX);
                entries
                    .try_reserve(1)
                    .map_err(|_| KvError::ResourceExhausted {
                        operation: "read wal entries".to_string(),
                        requested_bytes,
                    })?;
                entries.push(entry);
            }
            Err(source) => {
                eprintln!(
                    "warning: truncating cluster kv WAL at unreadable binary record {} in {}: {source}",
                    record_number,
                    path.display()
                );
                truncated = true;
                break;
            }
        }
    }
    metrics::histogram!("sinomo.kv.wal.read.entries").record(entries.len() as f64);
    metric.succeed();
    Ok(WalRead { entries, truncated })
}

pub(crate) fn append_wal_entries(path: &Path, entries: &[KvLogEntry]) -> Result<(), KvError> {
    let mut metric = KvStorageMetricGuard::start("wal_append", "sinomo.kv.wal.append.us");
    if entries.is_empty() {
        metric.finish("skipped");
        return Ok(());
    }
    let mut appended_bytes = 0_u64;
    let mut file = open_wal_file_for_append(path)?;
    fs2::FileExt::lock_exclusive(&file).map_err(|source| KvError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    match encode_wal_record_payloads(entries, path) {
        Ok(records) => {
            appended_bytes = records.iter().fold(0_u64, |total, record| {
                total.saturating_add(record.len() as u64).saturating_add(4)
            });
            durable_file::append_length_prefixed_records(
                &mut file,
                path,
                records.iter().map(Vec::as_slice),
                durable_file::DurableRecordAppendOptions::new(
                    durable_file::DurableLengthEndian::Big,
                ),
            )
            .map_err(kv_durable_file_error)?;
        }
        Err(KvError::ResourceExhausted { .. }) => {
            for entry in entries {
                let record = encode_wal_record_payload(entry, path)?;
                appended_bytes = appended_bytes
                    .saturating_add(record.len() as u64)
                    .saturating_add(4);
                durable_file::append_length_prefixed_records(
                    &mut file,
                    path,
                    [record.as_slice()],
                    durable_file::DurableRecordAppendOptions::new(
                        durable_file::DurableLengthEndian::Big,
                    ),
                )
                .map_err(kv_durable_file_error)?;
            }
        }
        Err(error) => return Err(error),
    }
    file.sync_data().map_err(|source| KvError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })?;
    metrics::counter!("sinomo.kv.wal.append.bytes").increment(appended_bytes);
    metrics::histogram!("sinomo.kv.wal.append.entries").record(entries.len() as f64);
    metric.succeed();
    Ok(())
}

pub(crate) fn open_wal_file_for_append(path: &Path) -> Result<File, KvError> {
    durable_file::open_file_for_append(path)
        .map(|append| append.file)
        .map_err(kv_durable_file_error)
}

fn encode_wal_record_payloads(
    entries: &[KvLogEntry],
    path: &Path,
) -> Result<Vec<Vec<u8>>, KvError> {
    let mut records = Vec::new();
    try_reserve_kv_items(&mut records, entries.len(), "encode wal records")?;
    for entry in entries {
        records.push(encode_wal_record_payload(entry, path)?);
    }
    Ok(records)
}

fn encode_wal_record_payload(entry: &KvLogEntry, path: &Path) -> Result<Vec<u8>, KvError> {
    let mut record = Vec::new();
    let range = append_wal_record_payload(&mut record, entry, path)?;
    debug_assert_eq!(range.start, 0);
    Ok(record)
}

fn append_wal_record_payload(
    out: &mut Vec<u8>,
    entry: &KvLogEntry,
    path: &Path,
) -> Result<std::ops::Range<usize>, KvError> {
    let record =
        sinomo_object_serde::append_bytes(out, entry, KV_WAL_OBJECT_VERSION_ID, LengthMode::U32)
            .map_err(kv_object_error)?;
    u32::try_from(record.len()).map_err(|_| KvError::Serialization {
        detail: format!("wal record for {} exceeds u32 length", path.display()),
    })?;
    Ok(record)
}

pub(crate) fn encode_wal_record(entry: &KvLogEntry, path: &Path) -> Result<Vec<u8>, KvError> {
    let mut encoded = Vec::new();
    encoded
        .try_reserve_exact(4)
        .map_err(|_| KvError::ResourceExhausted {
            operation: "encode wal record length".to_string(),
            requested_bytes: 4,
        })?;
    encoded.extend_from_slice(&0_u32.to_be_bytes());
    let record = append_wal_record_payload(&mut encoded, entry, path)?;
    let record_len = u32::try_from(record.len()).map_err(|_| KvError::Serialization {
        detail: format!("wal record for {} exceeds u32 length", path.display()),
    })?;
    encoded[..4].copy_from_slice(&record_len.to_be_bytes());
    Ok(encoded)
}

pub(crate) fn write_wal_record(
    file: &mut File,
    entry: &KvLogEntry,
    path: &Path,
) -> Result<(), KvError> {
    let record = encode_wal_record_payload(entry, path)?;
    durable_file::append_length_prefixed_records(
        file,
        path,
        [record.as_slice()],
        durable_file::DurableRecordAppendOptions::new(durable_file::DurableLengthEndian::Big),
    )
    .map(|_| ())
    .map_err(kv_durable_file_error)
}

pub(crate) fn wal_file_matches_entries(
    path: &Path,
    entries: &[KvLogEntry],
) -> Result<bool, KvError> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return Ok(entries.is_empty());
        }
        Err(_) => return Ok(false),
    };
    if fs2::FileExt::lock_shared(&file).is_err() {
        return Ok(false);
    }

    for entry in entries {
        let expected = encode_wal_record(entry, path)?;
        let mut existing = Vec::new();
        existing
            .try_reserve_exact(expected.len())
            .map_err(|_| KvError::ResourceExhausted {
                operation: "compare wal records".to_string(),
                requested_bytes: expected.len(),
            })?;
        existing.resize(expected.len(), 0);
        if file.read_exact(&mut existing).is_err() || existing != expected {
            return Ok(false);
        }
    }

    let mut extra = [0_u8; 1];
    Ok(matches!(file.read(&mut extra), Ok(0)))
}

pub(crate) fn rewrite_wal_entries(path: &Path, entries: &[KvLogEntry]) -> Result<(), KvError> {
    let mut metric = KvStorageMetricGuard::start("wal_rewrite", "sinomo.kv.wal.rewrite.us");
    // Callers hold the per-node raft lock. Stage through a temp file instead of
    // truncating the live WAL so a failed rewrite leaves the previous log intact.
    if wal_file_matches_entries(path, entries)? {
        metric.finish("skipped");
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| KvError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("wal.sobjlog");
    let temp_path = parent.join(format!(
        ".{file_name}.tmp-{}-{}",
        std::process::id(),
        OBJECT_FILE_WRITE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    let write_temp_result = (|| -> Result<(), KvError> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temp_path)
            .map_err(|source| KvError::Io {
                path: temp_path.display().to_string(),
                detail: source.to_string(),
            })?;
        for entry in entries {
            write_wal_record(&mut file, entry, &temp_path)?;
        }
        file.sync_data().map_err(|source| KvError::Io {
            path: temp_path.display().to_string(),
            detail: source.to_string(),
        })?;
        Ok(())
    })();
    if let Err(error) = write_temp_result {
        let _ = fs::remove_file(&temp_path);
        return Err(error);
    }
    let result = replace_file(&temp_path, path).and_then(|()| sync_directory(parent));
    if result.is_err() {
        let _ = fs::remove_file(&temp_path);
    } else {
        let written_bytes = fs::metadata(path)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        metrics::counter!("sinomo.kv.wal.rewrite.bytes").increment(written_bytes);
        metrics::histogram!("sinomo.kv.wal.rewrite.entries").record(entries.len() as f64);
        metric.succeed();
    }
    result
}

pub(crate) fn read_optional_object<T: for<'de> Deserialize<'de>>(
    path: &Path,
) -> Result<Option<T>, KvError> {
    let Some(bytes) = read_optional_object_file_bytes(path)? else {
        return Ok(None);
    };
    decode_object_file(path, &bytes).map(Some)
}

pub(crate) fn read_object_file<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, KvError> {
    let bytes = read_object_file_bytes(path)?;
    decode_object_file(path, &bytes)
}

#[cfg(test)]
pub(crate) static REQUIRED_OBJECT_FILE_READS_FOR_TEST: OnceLock<Mutex<HashMap<PathBuf, u64>>> =
    OnceLock::new();

#[cfg(test)]
pub(crate) fn required_object_file_reads_for_test() -> &'static Mutex<HashMap<PathBuf, u64>> {
    REQUIRED_OBJECT_FILE_READS_FOR_TEST.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(test)]
pub(crate) fn record_required_object_file_read_for_test(path: &Path) {
    if let Ok(mut reads) = required_object_file_reads_for_test().lock() {
        *reads.entry(path.to_path_buf()).or_default() += 1;
    }
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn reset_required_object_file_reads_for_test(path: &Path) {
    if let Ok(mut reads) = required_object_file_reads_for_test().lock() {
        reads.remove(path);
    }
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn required_object_file_read_count_for_test(path: &Path) -> u64 {
    required_object_file_reads_for_test()
        .lock()
        .ok()
        .and_then(|reads| reads.get(path).copied())
        .unwrap_or_default()
}

pub(crate) fn decode_object_file<T: for<'de> Deserialize<'de>>(
    path: &Path,
    bytes: &[u8],
) -> Result<T, KvError> {
    sinomo_object_serde::from_bytes(&bytes).map_err(|source| match kv_object_error(source) {
        KvError::Serialization { detail } => KvError::Serialization {
            detail: format!("{}: {detail}", path.display()),
        },
        error => error,
    })
}

pub(crate) fn read_optional_object_file_bytes(path: &Path) -> Result<Option<Vec<u8>>, KvError> {
    durable_file::read_optional_object_file_bytes(path, MAX_OBJECT_FILE_READ_BYTES)
        .map_err(kv_durable_file_error)
}

pub(crate) fn read_object_file_bytes(path: &Path) -> Result<Vec<u8>, KvError> {
    #[cfg(test)]
    record_required_object_file_read_for_test(path);
    durable_file::read_object_file_bytes(path, MAX_OBJECT_FILE_READ_BYTES)
        .map_err(kv_durable_file_error)
}

pub(crate) fn write_object_file<T: Serialize>(path: &Path, value: &T) -> Result<(), KvError> {
    write_object_file_if_changed(path, value).map(|_| ())
}

pub(crate) fn write_object_file_if_changed<T: Serialize>(
    path: &Path,
    value: &T,
) -> Result<bool, KvError> {
    let content = sinomo_object_serde::to_bytes(value, KV_WAL_OBJECT_VERSION_ID, LengthMode::U32)
        .map_err(kv_object_error)?;
    write_object_file_bytes_if_changed(path, &content)
}

pub(crate) fn write_object_file_bytes_if_changed(
    path: &Path,
    content: &[u8],
) -> Result<bool, KvError> {
    durable_file::write_object_file_bytes_if_changed(
        path,
        content,
        &LAST_WRITTEN_OBJECT_FILES,
        durable_file::DurableObjectFileOptions::new("json", "tmp"),
    )
    .map(|result| result.changed)
    .map_err(kv_durable_file_error)
}

pub(crate) fn write_object_file_bytes_create_new_serialized(
    path: &Path,
    content: &[u8],
) -> Result<bool, KvError> {
    durable_file::write_object_file_bytes_create_new_serialized(
        path,
        content,
        &LAST_WRITTEN_OBJECT_FILES,
        durable_file::DurableObjectFileOptions::new("sobj", "create-new-tmp"),
        |temp_path, path| fs::hard_link(temp_path, path),
    )
    .map(|result| result.changed)
    .map_err(kv_durable_file_error)
}

pub(crate) fn replace_file(temp_path: &Path, path: &Path) -> Result<(), KvError> {
    durable_file::replace_file(temp_path, path).map_err(kv_durable_file_error)
}

pub(crate) fn kv_durable_file_error(error: durable_file::DurableFileError) -> KvError {
    match error {
        durable_file::DurableFileError::ResourceExhausted {
            operation,
            requested_bytes,
            ..
        } => KvError::ResourceExhausted {
            operation,
            requested_bytes,
        },
        error => KvError::Io {
            path: error.path().to_string(),
            detail: error.detail().to_string(),
        },
    }
}

pub(crate) fn open_node_lock(paths: &KvWalPaths) -> Result<File, KvError> {
    OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&paths.lock)
        .map_err(|source| KvError::Io {
            path: paths.lock.display().to_string(),
            detail: source.to_string(),
        })
}

pub(crate) fn lock_node_exclusive(lock: &File, path: &Path, wait: Duration) -> Result<(), KvError> {
    let deadline = Instant::now() + wait;
    let mut attempts = 0_u32;
    loop {
        match fs2::FileExt::try_lock_exclusive(lock) {
            Ok(()) => return Ok(()),
            Err(source) if file_lock_is_busy(&source) => {
                let now = Instant::now();
                if wait.is_zero() || now >= deadline {
                    return Err(KvError::Busy {
                        path: path.display().to_string(),
                    });
                }
                let delay = raft_node_lock_retry_delay(attempts)
                    .min(deadline.saturating_duration_since(now));
                thread::sleep(delay);
                attempts = attempts.saturating_add(1);
            }
            Err(source) => {
                return Err(KvError::Io {
                    path: path.display().to_string(),
                    detail: source.to_string(),
                });
            }
        }
    }
}

pub(crate) fn raft_node_lock_retry_delay(attempts: u32) -> Duration {
    let shift = attempts.min(5);
    RAFT_NODE_LOCK_RETRY_INITIAL
        .saturating_mul(2_u32.saturating_pow(shift))
        .min(RAFT_NODE_LOCK_RETRY_MAX)
}

pub(crate) fn file_lock_is_busy(source: &std::io::Error) -> bool {
    source.kind() == std::io::ErrorKind::WouldBlock
        || matches!(source.raw_os_error(), Some(32 | 33))
}

pub(crate) fn sync_directory(path: &Path) -> Result<(), KvError> {
    durable_file::sync_directory(path).map_err(kv_durable_file_error)
}
