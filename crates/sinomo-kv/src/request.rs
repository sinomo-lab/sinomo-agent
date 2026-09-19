use crate::internal::*;

pub const MAX_CLUSTER_KV_REQUEST_ATTEMPTS: usize = 4096;
pub const MAX_CLUSTER_KV_REQUEST_ATTEMPT_TOMBSTONES: usize = 4096;
pub const MAX_CLUSTER_KV_REQUEST_ATTEMPT_BYTES: usize = 8 * 1024 * 1024;

const MAX_CLUSTER_KV_REQUEST_ATTEMPT_LOG_RECORDS: usize = 4096;
const MAX_CLUSTER_KV_REQUEST_ATTEMPT_LOG_BYTES: u64 = 8 * 1024 * 1024;
pub(crate) const MAX_CLUSTER_KV_REQUEST_ATTEMPT_LOG_RECORD_BYTES: usize =
    MAX_CLUSTER_KV_REQUEST_ATTEMPT_BYTES + 64 * 1024;

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
struct KvRequestAttemptLedger {
    #[serde(default, deserialize_with = "deserialize_fallible_vec")]
    attempts: Vec<KvRequestAttemptRecord>,
    #[serde(default, deserialize_with = "deserialize_fallible_vec")]
    expired: Vec<KvRequestId>,
    /// Highest append-log sequence incorporated into this checkpoint.
    ///
    /// This makes checkpoint-then-truncate crash-safe: if the process dies
    /// after publishing the checkpoint but before truncating the old log, the
    /// replay skips records already represented by the checkpoint.
    #[serde(default)]
    log_sequence: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct KvRequestAttemptLogRecord {
    sequence: u64,
    mutation: KvRequestAttemptMutation,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
enum KvRequestAttemptMutation {
    Upsert(KvRequestAttemptRecord),
    Expire(KvRequestId),
}

#[derive(Clone, Debug)]
struct StoredRequestAttempt {
    record: KvRequestAttemptRecord,
    encoded_bytes: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct ClusterKvRequestAttemptSubmissionReceipt {
    command_digest: KvCommandDigest,
    updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Default)]
struct MaterializedRequestAttemptLedger {
    attempts: HashMap<KvRequestId, StoredRequestAttempt>,
    attempt_order: BTreeSet<(OffsetDateTime, KvRequestId)>,
    attempt_bytes: usize,
    expired: BTreeSet<KvRequestId>,
    expired_order: VecDeque<KvRequestId>,
    log_sequence: u64,
}

#[derive(Clone, Debug)]
enum PreparedRequestAttemptMutation {
    Upsert {
        record: KvRequestAttemptRecord,
        encoded_bytes: usize,
    },
    Expire(KvRequestId),
}

impl PreparedRequestAttemptMutation {
    fn persisted(&self) -> KvRequestAttemptMutation {
        match self {
            Self::Upsert { record, .. } => KvRequestAttemptMutation::Upsert(record.clone()),
            Self::Expire(request_id) => KvRequestAttemptMutation::Expire(request_id.clone()),
        }
    }
}

impl MaterializedRequestAttemptLedger {
    fn from_checkpoint(checkpoint: KvRequestAttemptLedger) -> Result<Self, KvError> {
        if checkpoint.attempts.len() > MAX_CLUSTER_KV_REQUEST_ATTEMPTS
            || checkpoint.expired.len() > MAX_CLUSTER_KV_REQUEST_ATTEMPT_TOMBSTONES
        {
            return Err(KvError::RequestRecordLimit {
                observed_records: checkpoint
                    .attempts
                    .len()
                    .saturating_add(checkpoint.expired.len()),
                max_records: MAX_CLUSTER_KV_REQUEST_ATTEMPTS
                    .saturating_add(MAX_CLUSTER_KV_REQUEST_ATTEMPT_TOMBSTONES),
            });
        }

        let mut materialized = Self {
            log_sequence: checkpoint.log_sequence,
            ..Self::default()
        };
        for record in checkpoint.attempts {
            let encoded_bytes = encode_kv_value(&record)?.len();
            materialized.insert_without_eviction(record, encoded_bytes)?;
        }
        if materialized.attempt_bytes > MAX_CLUSTER_KV_REQUEST_ATTEMPT_BYTES {
            return Err(KvError::RequestRecordBytesLimit {
                observed_bytes: materialized.attempt_bytes,
                max_bytes: MAX_CLUSTER_KV_REQUEST_ATTEMPT_BYTES,
            });
        }
        for request_id in checkpoint.expired {
            materialized.expire(request_id);
        }
        Ok(materialized)
    }

    fn checkpoint(&self) -> KvRequestAttemptLedger {
        let attempts = self
            .attempt_order
            .iter()
            .filter_map(|(_, request_id)| {
                self.attempts
                    .get(request_id)
                    .map(|stored| stored.record.clone())
            })
            .collect();
        KvRequestAttemptLedger {
            attempts,
            expired: self.expired_order.iter().cloned().collect(),
            log_sequence: self.log_sequence,
        }
    }

    fn prepare(
        &self,
        record: KvRequestAttemptRecord,
    ) -> Result<Option<PreparedRequestAttemptMutation>, KvError> {
        if self.expired.contains(&record.request_id) {
            return Err(KvError::RequestExpired {
                request_id: record.request_id,
            });
        }
        if let Some(existing) = self.attempts.get(&record.request_id) {
            if existing.record.command_digest != record.command_digest {
                return Err(KvError::RequestIdConflict {
                    request_id: record.request_id,
                });
            }
            // Once an attempt may have escaped the local pre-append boundary, a
            // later retry that is rejected locally cannot prove that the earlier
            // attempt did not commit elsewhere. Preserve uncertainty until
            // committed Raft history or an expiry tombstone resolves it.
            if matches!(
                &existing.record.state,
                KvRequestAttemptState::Submitted | KvRequestAttemptState::Unknown { .. }
            ) && matches!(
                &record.state,
                KvRequestAttemptState::Submitted
                    | KvRequestAttemptState::RejectedBeforeCommit { .. }
            ) {
                return Ok(None);
            }
        }

        if matches!(record.state, KvRequestAttemptState::Expired) {
            return Ok(Some(PreparedRequestAttemptMutation::Expire(
                record.request_id,
            )));
        }
        let encoded_bytes = encode_kv_value(&record)?.len();
        if encoded_bytes > MAX_CLUSTER_KV_REQUEST_ATTEMPT_BYTES {
            // Preserve the established outcome for an individually oversized
            // attempt without appending its multi-megabyte diagnostic detail.
            return Ok(Some(PreparedRequestAttemptMutation::Expire(
                record.request_id,
            )));
        }
        Ok(Some(PreparedRequestAttemptMutation::Upsert {
            record,
            encoded_bytes,
        }))
    }

    fn prepare_rejection_after_exact_submission(
        &self,
        record: KvRequestAttemptRecord,
        submission: &ClusterKvRequestAttemptSubmissionReceipt,
    ) -> Result<Option<PreparedRequestAttemptMutation>, KvError> {
        if self.expired.contains(&record.request_id) {
            return Err(KvError::RequestExpired {
                request_id: record.request_id,
            });
        }
        let Some(existing) = self.attempts.get(&record.request_id) else {
            return Ok(None);
        };
        if existing.record.command_digest != record.command_digest
            || existing.record.command_digest != submission.command_digest
        {
            return Err(KvError::RequestIdConflict {
                request_id: record.request_id,
            });
        }
        // Only the caller that durably created this exact Submitted record may
        // close it as a definite pre-append rejection. A retry that merely
        // observed an older Submitted/Unknown record must preserve ambiguity,
        // and a concurrent newer outcome must never be overwritten.
        if existing.record.updated_at != submission.updated_at
            || !matches!(existing.record.state, KvRequestAttemptState::Submitted)
        {
            return Ok(None);
        }
        let encoded_bytes = encode_kv_value(&record)?.len();
        if encoded_bytes > MAX_CLUSTER_KV_REQUEST_ATTEMPT_BYTES {
            return Ok(Some(PreparedRequestAttemptMutation::Expire(
                record.request_id,
            )));
        }
        Ok(Some(PreparedRequestAttemptMutation::Upsert {
            record,
            encoded_bytes,
        }))
    }

    fn apply_prepared(
        &mut self,
        mutation: PreparedRequestAttemptMutation,
        sequence: u64,
    ) -> Result<(), KvError> {
        match mutation {
            PreparedRequestAttemptMutation::Upsert {
                record,
                encoded_bytes,
            } => self.upsert(record, encoded_bytes)?,
            PreparedRequestAttemptMutation::Expire(request_id) => self.expire(request_id),
        }
        self.log_sequence = sequence;
        Ok(())
    }

    fn apply_persisted(
        &mut self,
        mutation: KvRequestAttemptMutation,
        sequence: u64,
    ) -> Result<(), KvError> {
        match mutation {
            KvRequestAttemptMutation::Upsert(record) => {
                if !self.expired.contains(&record.request_id) {
                    let encoded_bytes = encode_kv_value(&record)?.len();
                    if encoded_bytes > MAX_CLUSTER_KV_REQUEST_ATTEMPT_BYTES {
                        self.expire(record.request_id);
                    } else {
                        self.upsert(record, encoded_bytes)?;
                    }
                }
            }
            KvRequestAttemptMutation::Expire(request_id) => self.expire(request_id),
        }
        self.log_sequence = sequence;
        Ok(())
    }

    fn upsert(
        &mut self,
        record: KvRequestAttemptRecord,
        encoded_bytes: usize,
    ) -> Result<(), KvError> {
        if self.expired.contains(&record.request_id) {
            return Ok(());
        }
        self.insert_without_eviction(record, encoded_bytes)?;
        while self.attempts.len() > MAX_CLUSTER_KV_REQUEST_ATTEMPTS
            || self.attempt_bytes > MAX_CLUSTER_KV_REQUEST_ATTEMPT_BYTES
        {
            let Some((_, request_id)) = self.attempt_order.pop_first() else {
                break;
            };
            if let Some(removed) = self.attempts.remove(&request_id) {
                self.attempt_bytes = self.attempt_bytes.saturating_sub(removed.encoded_bytes);
                self.insert_expiry_tombstone(request_id);
            }
        }
        Ok(())
    }

    fn insert_without_eviction(
        &mut self,
        record: KvRequestAttemptRecord,
        encoded_bytes: usize,
    ) -> Result<(), KvError> {
        if let Some(previous) = self.attempts.remove(&record.request_id) {
            self.attempt_order
                .remove(&(previous.record.updated_at, record.request_id.clone()));
            self.attempt_bytes = self.attempt_bytes.saturating_sub(previous.encoded_bytes);
        }
        self.attempt_bytes = self
            .attempt_bytes
            .checked_add(encoded_bytes)
            .ok_or_else(|| KvError::ResourceExhausted {
                operation: "account cluster KV request attempts".to_string(),
                requested_bytes: usize::MAX,
            })?;
        self.attempt_order
            .insert((record.updated_at, record.request_id.clone()));
        self.attempts.insert(
            record.request_id.clone(),
            StoredRequestAttempt {
                record,
                encoded_bytes,
            },
        );
        Ok(())
    }

    fn expire(&mut self, request_id: KvRequestId) {
        if let Some(previous) = self.attempts.remove(&request_id) {
            self.attempt_order
                .remove(&(previous.record.updated_at, request_id.clone()));
            self.attempt_bytes = self.attempt_bytes.saturating_sub(previous.encoded_bytes);
        }
        self.insert_expiry_tombstone(request_id);
    }

    fn insert_expiry_tombstone(&mut self, request_id: KvRequestId) {
        if self.expired.insert(request_id.clone()) {
            self.expired_order.push_back(request_id);
        }
        while self.expired_order.len() > MAX_CLUSTER_KV_REQUEST_ATTEMPT_TOMBSTONES {
            if let Some(expired) = self.expired_order.pop_front() {
                self.expired.remove(&expired);
            }
        }
    }

    fn outcome(&self, request_id: &KvRequestId) -> Option<KvRequestOutcome> {
        if self.expired.contains(request_id) {
            return Some(KvRequestOutcome::Expired);
        }
        self.attempts
            .get(request_id)
            .map(|stored| match &stored.record.state {
                KvRequestAttemptState::Submitted => KvRequestOutcome::Unknown {
                    detail: "request was submitted but no durable commit outcome is known"
                        .to_string(),
                },
                KvRequestAttemptState::RejectedBeforeCommit { reason } => {
                    KvRequestOutcome::RejectedBeforeCommit {
                        reason: reason.clone(),
                    }
                }
                KvRequestAttemptState::Unknown { detail } => KvRequestOutcome::Unknown {
                    detail: detail.clone(),
                },
                KvRequestAttemptState::Expired => KvRequestOutcome::Expired,
            })
    }
}

#[derive(Clone, Debug)]
struct RequestAttemptLedgerCacheEntry {
    ledger: MaterializedRequestAttemptLedger,
    checkpoint_identity: Option<FileIdentity>,
    log_identity: Option<FileIdentity>,
    log_records: usize,
    log_bytes: u64,
    last_used: u64,
}

static REQUEST_ATTEMPT_LEDGER_CACHE: LazyLock<
    Mutex<HashMap<PathBuf, RequestAttemptLedgerCacheEntry>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn write_cached_request_attempt_checkpoint_for_test(
    paths: &ClusterKvPaths,
) -> Result<(), KvError> {
    let checkpoint = {
        let cache = REQUEST_ATTEMPT_LEDGER_CACHE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        cache
            .get(&paths.request_attempts)
            .expect("materialized request-attempt ledger")
            .ledger
            .checkpoint()
    };
    write_object_file(&paths.request_attempts, &checkpoint)
}

fn request_attempt_deadline_busy(paths: &ClusterKvPaths) -> KvError {
    KvError::Busy {
        path: paths.request_attempts_lock.display().to_string(),
    }
}

fn ensure_request_attempt_deadline(
    paths: &ClusterKvPaths,
    operation_deadline: Option<Instant>,
) -> Result<(), KvError> {
    if operation_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        return Err(request_attempt_deadline_busy(paths));
    }
    Ok(())
}

fn lock_request_attempt_ledger_before(
    paths: &ClusterKvPaths,
    operation_deadline: Option<Instant>,
) -> Result<File, KvError> {
    ensure_request_attempt_deadline(paths, operation_deadline)?;
    fs::create_dir_all(&paths.root).map_err(|source| KvError::Io {
        path: paths.root.display().to_string(),
        detail: source.to_string(),
    })?;
    ensure_request_attempt_deadline(paths, operation_deadline)?;
    let lock = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&paths.request_attempts_lock)
        .map_err(|source| KvError::Io {
            path: paths.request_attempts_lock.display().to_string(),
            detail: source.to_string(),
        })?;
    let wait = operation_deadline.map_or(CLUSTER_KV_COMMIT_SEQUENCE_LOCK_WAIT, |deadline| {
        deadline.saturating_duration_since(Instant::now())
    });
    lock_node_exclusive(&lock, &paths.request_attempts_lock, wait)?;
    // A lock release racing the timeout must not admit an already-expired
    // reconciliation operation. Dropping `lock` here releases the flock.
    ensure_request_attempt_deadline(paths, operation_deadline)?;
    Ok(lock)
}

fn lock_request_attempt_ledger(paths: &ClusterKvPaths) -> Result<File, KvError> {
    lock_request_attempt_ledger_before(paths, None)
}

#[cfg(test)]
pub(crate) fn lock_cluster_kv_request_attempt_ledger_for_test(
    paths: &ClusterKvPaths,
) -> Result<File, KvError> {
    lock_request_attempt_ledger(paths)
}

fn lock_request_attempt_cache_before(
    paths: &ClusterKvPaths,
    operation_deadline: Instant,
) -> Result<std::sync::MutexGuard<'static, HashMap<PathBuf, RequestAttemptLedgerCacheEntry>>, KvError>
{
    let mut attempts = 0_u32;
    loop {
        ensure_request_attempt_deadline(paths, Some(operation_deadline))?;
        match REQUEST_ATTEMPT_LEDGER_CACHE.try_lock() {
            Ok(cache) => {
                ensure_request_attempt_deadline(paths, Some(operation_deadline))?;
                return Ok(cache);
            }
            Err(std::sync::TryLockError::Poisoned(poisoned)) => {
                let cache = poisoned.into_inner();
                ensure_request_attempt_deadline(paths, Some(operation_deadline))?;
                return Ok(cache);
            }
            Err(std::sync::TryLockError::WouldBlock) => {
                let now = Instant::now();
                if now >= operation_deadline {
                    return Err(request_attempt_deadline_busy(paths));
                }
                thread::sleep(
                    raft_node_lock_retry_delay(attempts)
                        .min(operation_deadline.saturating_duration_since(now)),
                );
                attempts = attempts.saturating_add(1);
            }
        }
    }
}

fn evict_request_attempt_cache_entry_if_needed(
    cache: &mut HashMap<PathBuf, RequestAttemptLedgerCacheEntry>,
    protected: &Path,
) {
    if cache.contains_key(protected) || cache.len() < FILE_CACHE_CAPACITY {
        return;
    }
    let eviction_candidate = cache
        .iter()
        .filter(|(path, _)| path.as_path() != protected)
        .min_by_key(|(_, entry)| entry.last_used)
        .map(|(path, _)| path.clone());
    if let Some(path) = eviction_candidate {
        cache.remove(&path);
    }
}

fn load_request_attempt_ledger(
    paths: &ClusterKvPaths,
    operation_deadline: Option<Instant>,
) -> Result<RequestAttemptLedgerCacheEntry, KvError> {
    ensure_request_attempt_deadline(paths, operation_deadline)?;
    let checkpoint = read_optional_object::<KvRequestAttemptLedger>(&paths.request_attempts)?
        .unwrap_or_default();
    ensure_request_attempt_deadline(paths, operation_deadline)?;
    let mut ledger = MaterializedRequestAttemptLedger::from_checkpoint(checkpoint)?;
    let mut log_records = 0_usize;
    let mut log_bytes = 0_u64;
    let mut repair_log = false;

    match OpenOptions::new()
        .read(true)
        .write(true)
        .open(&paths.request_attempts_log)
    {
        Ok(mut file) => {
            log_bytes = file
                .metadata()
                .map_err(|source| KvError::Io {
                    path: paths.request_attempts_log.display().to_string(),
                    detail: source.to_string(),
                })?
                .len();
            file.seek(SeekFrom::Start(0))
                .map_err(|source| KvError::Io {
                    path: paths.request_attempts_log.display().to_string(),
                    detail: source.to_string(),
                })?;
            let mut physical_records = 0_usize;
            loop {
                ensure_request_attempt_deadline(paths, operation_deadline)?;
                let record_start = file.stream_position().map_err(|source| KvError::Io {
                    path: paths.request_attempts_log.display().to_string(),
                    detail: source.to_string(),
                })?;
                let record = match durable_file::read_length_prefixed_record(
                    &mut file,
                    &paths.request_attempts_log,
                    durable_file::DurableRecordReadOptions::new(
                        durable_file::DurableLengthEndian::Big,
                    )
                    .max_record_bytes(MAX_CLUSTER_KV_REQUEST_ATTEMPT_LOG_RECORD_BYTES),
                ) {
                    Ok(Some(record)) => record,
                    Ok(None) => {
                        if record_start < log_bytes {
                            repair_log = true;
                        }
                        break;
                    }
                    Err(durable_file::DurableFileError::RecordTooLarge { .. }) => {
                        repair_log = true;
                        break;
                    }
                    Err(error) => return Err(kv_durable_file_error(error)),
                };
                physical_records = physical_records.saturating_add(1);
                let record = match decode_kv_value::<KvRequestAttemptLogRecord>(&record.bytes) {
                    Ok(record) => record,
                    Err(_) => {
                        repair_log = true;
                        break;
                    }
                };

                if record.sequence <= ledger.log_sequence {
                    // The checkpoint landed but the preceding log truncation
                    // did not. The sequence makes this old prefix harmless.
                    repair_log = true;
                    continue;
                }
                let expected_sequence =
                    ledger
                        .log_sequence
                        .checked_add(1)
                        .ok_or_else(|| KvError::Serialization {
                            detail: "cluster KV request-attempt log sequence overflow".to_string(),
                        })?;
                if record.sequence != expected_sequence {
                    repair_log = true;
                    break;
                }
                ledger.apply_persisted(record.mutation, record.sequence)?;
                log_records = log_records.saturating_add(1);
            }
            if physical_records >= MAX_CLUSTER_KV_REQUEST_ATTEMPT_LOG_RECORDS
                || log_bytes >= MAX_CLUSTER_KV_REQUEST_ATTEMPT_LOG_BYTES
            {
                repair_log = true;
            }
            if repair_log {
                ensure_request_attempt_deadline(paths, operation_deadline)?;
                write_object_file(&paths.request_attempts, &ledger.checkpoint())?;
                ensure_request_attempt_deadline(paths, operation_deadline)?;
                durable_file::truncate_open_file_and_sync(
                    &mut file,
                    &paths.request_attempts_log,
                    0,
                    durable_file::DurableFileSync::Data,
                )
                .map_err(kv_durable_file_error)?;
                log_records = 0;
                log_bytes = 0;
            }
        }
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(KvError::Io {
                path: paths.request_attempts_log.display().to_string(),
                detail: source.to_string(),
            });
        }
    }

    Ok(RequestAttemptLedgerCacheEntry {
        ledger,
        checkpoint_identity: file_identity(&paths.request_attempts),
        log_identity: file_identity(&paths.request_attempts_log),
        log_records,
        log_bytes,
        last_used: kv_read_cache_tick(),
    })
}

fn request_attempt_cache_entry<'a>(
    paths: &ClusterKvPaths,
    cache: &'a mut HashMap<PathBuf, RequestAttemptLedgerCacheEntry>,
    operation_deadline: Option<Instant>,
) -> Result<&'a mut RequestAttemptLedgerCacheEntry, KvError> {
    ensure_request_attempt_deadline(paths, operation_deadline)?;
    let key = paths.request_attempts.clone();
    let checkpoint_identity = file_identity(&paths.request_attempts);
    let log_identity = file_identity(&paths.request_attempts_log);
    let cache_is_current = cache.get(&key).is_some_and(|entry| {
        entry.checkpoint_identity == checkpoint_identity && entry.log_identity == log_identity
    });
    if !cache_is_current {
        let entry = load_request_attempt_ledger(paths, operation_deadline)?;
        ensure_request_attempt_deadline(paths, operation_deadline)?;
        evict_request_attempt_cache_entry_if_needed(cache, &key);
        cache.insert(key.clone(), entry);
    }
    let entry = cache
        .get_mut(&key)
        .expect("request-attempt cache entry inserted above");
    ensure_request_attempt_deadline(paths, operation_deadline)?;
    entry.last_used = kv_read_cache_tick();
    Ok(entry)
}

fn append_request_attempt_log_record(
    paths: &ClusterKvPaths,
    record: &KvRequestAttemptLogRecord,
) -> Result<u64, KvError> {
    let encoded = encode_kv_value(record)?;
    if encoded.len() > MAX_CLUSTER_KV_REQUEST_ATTEMPT_LOG_RECORD_BYTES {
        return Err(KvError::RequestRecordBytesLimit {
            observed_bytes: encoded.len(),
            max_bytes: MAX_CLUSTER_KV_REQUEST_ATTEMPT_LOG_RECORD_BYTES,
        });
    }
    let mut append = durable_file::open_file_for_append(&paths.request_attempts_log)
        .map_err(kv_durable_file_error)?;
    durable_file::append_length_prefixed_records(
        &mut append.file,
        &paths.request_attempts_log,
        [encoded.as_slice()],
        durable_file::DurableRecordAppendOptions::new(durable_file::DurableLengthEndian::Big)
            .sync(durable_file::DurableFileSync::Data),
    )
    .map_err(kv_durable_file_error)
}

fn request_attempt_log_should_checkpoint(entry: &RequestAttemptLedgerCacheEntry) -> bool {
    entry.log_records >= MAX_CLUSTER_KV_REQUEST_ATTEMPT_LOG_RECORDS
        || entry.log_bytes >= MAX_CLUSTER_KV_REQUEST_ATTEMPT_LOG_BYTES
}

fn checkpoint_request_attempt_ledger(
    paths: &ClusterKvPaths,
    entry: &mut RequestAttemptLedgerCacheEntry,
) -> Result<(), KvError> {
    write_object_file(&paths.request_attempts, &entry.ledger.checkpoint())?;
    let mut append = durable_file::open_file_for_append(&paths.request_attempts_log)
        .map_err(kv_durable_file_error)?;
    durable_file::truncate_open_file_and_sync(
        &mut append.file,
        &paths.request_attempts_log,
        0,
        durable_file::DurableFileSync::Data,
    )
    .map_err(kv_durable_file_error)?;
    entry.checkpoint_identity = file_identity(&paths.request_attempts);
    entry.log_identity = file_identity(&paths.request_attempts_log);
    entry.log_records = 0;
    entry.log_bytes = 0;
    Ok(())
}

fn record_cluster_kv_request_attempt_with_deadline(
    paths: &ClusterKvPaths,
    record: KvRequestAttemptRecord,
    operation_deadline: Option<Instant>,
    expected_submission: Option<&ClusterKvRequestAttemptSubmissionReceipt>,
) -> Result<bool, KvError> {
    let _lock = lock_request_attempt_ledger_before(paths, operation_deadline)?;
    let mut cache = match operation_deadline {
        Some(deadline) => lock_request_attempt_cache_before(paths, deadline)?,
        None => REQUEST_ATTEMPT_LEDGER_CACHE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()),
    };
    let entry = request_attempt_cache_entry(paths, &mut cache, operation_deadline)?;
    if request_attempt_log_should_checkpoint(entry) {
        // Checkpoint publication and its sequence-fenced log truncation form
        // one crash-safe durability step. Admit that step only while the
        // caller still owns time, then let it finish atomically rather than
        // abandoning a published checkpoint half way through cleanup.
        ensure_request_attempt_deadline(paths, operation_deadline)?;
        checkpoint_request_attempt_ledger(paths, entry)?;
        ensure_request_attempt_deadline(paths, operation_deadline)?;
    }

    let mutation = match expected_submission {
        Some(submission) => entry
            .ledger
            .prepare_rejection_after_exact_submission(record, submission)?,
        None => entry.ledger.prepare(record)?,
    };
    let Some(mutation) = mutation else {
        return Ok(false);
    };
    // Encoding/preparation is bounded by the retained attempt-record limit,
    // but it can still consume the tail of a short request deadline. The log
    // append below is the linearization point, so refuse it after expiry.
    ensure_request_attempt_deadline(paths, operation_deadline)?;
    let sequence =
        entry
            .ledger
            .log_sequence
            .checked_add(1)
            .ok_or_else(|| KvError::Serialization {
                detail: "cluster KV request-attempt log sequence overflow".to_string(),
            })?;
    let log_record = KvRequestAttemptLogRecord {
        sequence,
        mutation: mutation.persisted(),
    };
    let written = append_request_attempt_log_record(paths, &log_record)?;
    entry.ledger.apply_prepared(mutation, sequence)?;
    entry.log_records = entry.log_records.saturating_add(1);
    entry.log_bytes = entry.log_bytes.saturating_add(written);
    entry.log_identity = file_identity(&paths.request_attempts_log);

    // Deadline-bound mutation paths have already made their record durable.
    // Do not turn optional compaction into post-linearization deadline work;
    // the next writer (or crash recovery) checkpoints this bounded log before
    // admitting another record. Legacy maintenance callers retain the eager
    // compaction behavior.
    if operation_deadline.is_none() && request_attempt_log_should_checkpoint(entry) {
        checkpoint_request_attempt_ledger(paths, entry)?;
    }
    Ok(true)
}

pub fn record_cluster_kv_request_attempt(
    paths: &ClusterKvPaths,
    record: KvRequestAttemptRecord,
) -> Result<(), KvError> {
    record_cluster_kv_request_attempt_with_deadline(paths, record, None, None).map(|_| ())
}

/// Records one request-attempt transition without waiting past the owning
/// mutation's absolute deadline for the ledger flock, cache, checkpoint, or
/// append admission. Once the length-prefixed record append is admitted, its
/// data sync completes and the function reports that durable outcome even if
/// the clock crosses the deadline during the sync itself.
pub fn record_cluster_kv_request_attempt_before(
    paths: &ClusterKvPaths,
    record: KvRequestAttemptRecord,
    operation_deadline: Instant,
) -> Result<(), KvError> {
    record_cluster_kv_request_attempt_with_deadline(paths, record, Some(operation_deadline), None)
        .map(|_| ())
}

pub(crate) fn mark_cluster_kv_request_submitted_with_receipt(
    paths: &ClusterKvPaths,
    request: &KvCommandRequest,
    operation_deadline: Option<Instant>,
) -> Result<Option<ClusterKvRequestAttemptSubmissionReceipt>, KvError> {
    let receipt = ClusterKvRequestAttemptSubmissionReceipt {
        command_digest: request.digest()?,
        updated_at: OffsetDateTime::now_utc(),
    };
    let recorded = record_cluster_kv_request_attempt_with_deadline(
        paths,
        KvRequestAttemptRecord {
            request_id: request.request_id.clone(),
            command_digest: receipt.command_digest,
            state: KvRequestAttemptState::Submitted,
            updated_at: receipt.updated_at,
        },
        operation_deadline,
        None,
    )?;
    Ok(recorded.then_some(receipt))
}

struct StagedRequestAttemptCache<'a> {
    cache: &'a mut HashMap<PathBuf, RequestAttemptLedgerCacheEntry>,
    key: PathBuf,
    durable: bool,
}

impl Drop for StagedRequestAttemptCache<'_> {
    fn drop(&mut self) {
        if !self.durable {
            self.cache.remove(&self.key);
        }
    }
}

/// Persists independently identified submissions with one ledger flush. The
/// cache mutex hides staged records until durability; any failure evicts the
/// staged view so the next caller replays the actual on-disk prefix.
pub(crate) fn mark_cluster_kv_requests_submitted_together(
    paths: &ClusterKvPaths,
    requests: &[&KvCommandRequest],
    deadline: Instant,
) -> Result<Vec<Result<Option<ClusterKvRequestAttemptSubmissionReceipt>, KvError>>, KvError> {
    if requests.is_empty() {
        return Ok(Vec::new());
    }
    let _lock = lock_request_attempt_ledger_before(paths, Some(deadline))?;
    let mut cache = lock_request_attempt_cache_before(paths, deadline)?;
    let mut staged = StagedRequestAttemptCache {
        cache: &mut cache,
        key: paths.request_attempts.clone(),
        durable: false,
    };
    let result = (|| {
        let entry = request_attempt_cache_entry(paths, staged.cache, Some(deadline))?;
        if request_attempt_log_should_checkpoint(entry) {
            checkpoint_request_attempt_ledger(paths, entry)?;
        }
        let mut receipts = Vec::with_capacity(requests.len());
        let mut encoded = Vec::with_capacity(requests.len());
        for request in requests {
            let prepared = (|| {
                let receipt = ClusterKvRequestAttemptSubmissionReceipt {
                    command_digest: request.digest()?,
                    updated_at: OffsetDateTime::now_utc(),
                };
                let record = KvRequestAttemptRecord {
                    request_id: request.request_id.clone(),
                    command_digest: receipt.command_digest,
                    state: KvRequestAttemptState::Submitted,
                    updated_at: receipt.updated_at,
                };
                let Some(mutation) = entry.ledger.prepare(record)? else {
                    return Ok(None);
                };
                let sequence = entry.ledger.log_sequence.checked_add(1).ok_or_else(|| {
                    KvError::Serialization {
                        detail: "request-attempt sequence overflow".into(),
                    }
                })?;
                let bytes = encode_kv_value(&KvRequestAttemptLogRecord {
                    sequence,
                    mutation: mutation.persisted(),
                })?;
                if bytes.len() > MAX_CLUSTER_KV_REQUEST_ATTEMPT_LOG_RECORD_BYTES {
                    return Err(KvError::RequestRecordBytesLimit {
                        observed_bytes: bytes.len(),
                        max_bytes: MAX_CLUSTER_KV_REQUEST_ATTEMPT_LOG_RECORD_BYTES,
                    });
                }
                Ok(Some((mutation, sequence, bytes, receipt)))
            })();
            match prepared {
                Ok(Some((mutation, sequence, bytes, receipt))) => {
                    entry.ledger.apply_prepared(mutation, sequence)?;
                    encoded.push(bytes);
                    receipts.push(Ok(Some(receipt)));
                }
                Ok(None) => receipts.push(Ok(None)),
                Err(error) => receipts.push(Err(error)),
            }
        }
        ensure_request_attempt_deadline(paths, Some(deadline))?;
        if !encoded.is_empty() {
            let mut append = durable_file::open_file_for_append(&paths.request_attempts_log)
                .map_err(kv_durable_file_error)?;
            let written = durable_file::append_length_prefixed_records(
                &mut append.file,
                &paths.request_attempts_log,
                encoded.iter().map(Vec::as_slice),
                durable_file::DurableRecordAppendOptions::new(
                    durable_file::DurableLengthEndian::Big,
                )
                .sync(durable_file::DurableFileSync::Data),
            )
            .map_err(kv_durable_file_error)?;
            entry.log_records = entry.log_records.saturating_add(encoded.len());
            entry.log_bytes = entry.log_bytes.saturating_add(written);
            entry.log_identity = file_identity(&paths.request_attempts_log);
        }
        Ok(receipts)
    })();
    staged.durable = result.is_ok();
    result
}

pub(crate) fn mark_cluster_kv_request_rejected_after_exact_submission(
    paths: &ClusterKvPaths,
    request: &KvCommandRequest,
    submission: &ClusterKvRequestAttemptSubmissionReceipt,
    reason: impl Into<String>,
) -> Result<bool, KvError> {
    record_cluster_kv_request_attempt_with_deadline(
        paths,
        KvRequestAttemptRecord {
            request_id: request.request_id.clone(),
            command_digest: request.digest()?,
            state: KvRequestAttemptState::RejectedBeforeCommit {
                reason: reason.into(),
            },
            updated_at: OffsetDateTime::now_utc(),
        },
        None,
        Some(submission),
    )
}

pub fn mark_cluster_kv_request_submitted(
    paths: &ClusterKvPaths,
    request: &KvCommandRequest,
) -> Result<(), KvError> {
    record_cluster_kv_request_attempt(
        paths,
        KvRequestAttemptRecord {
            request_id: request.request_id.clone(),
            command_digest: request.digest()?,
            state: KvRequestAttemptState::Submitted,
            updated_at: OffsetDateTime::now_utc(),
        },
    )
}

pub fn mark_cluster_kv_request_submitted_before(
    paths: &ClusterKvPaths,
    request: &KvCommandRequest,
    operation_deadline: Instant,
) -> Result<(), KvError> {
    record_cluster_kv_request_attempt_before(
        paths,
        KvRequestAttemptRecord {
            request_id: request.request_id.clone(),
            command_digest: request.digest()?,
            state: KvRequestAttemptState::Submitted,
            updated_at: OffsetDateTime::now_utc(),
        },
        operation_deadline,
    )
}

pub fn mark_cluster_kv_request_rejected_before_commit(
    paths: &ClusterKvPaths,
    request: &KvCommandRequest,
    reason: impl Into<String>,
) -> Result<(), KvError> {
    record_cluster_kv_request_attempt(
        paths,
        KvRequestAttemptRecord {
            request_id: request.request_id.clone(),
            command_digest: request.digest()?,
            state: KvRequestAttemptState::RejectedBeforeCommit {
                reason: reason.into(),
            },
            updated_at: OffsetDateTime::now_utc(),
        },
    )
}

pub fn mark_cluster_kv_request_rejected_before_commit_before(
    paths: &ClusterKvPaths,
    request: &KvCommandRequest,
    reason: impl Into<String>,
    operation_deadline: Instant,
) -> Result<(), KvError> {
    record_cluster_kv_request_attempt_before(
        paths,
        KvRequestAttemptRecord {
            request_id: request.request_id.clone(),
            command_digest: request.digest()?,
            state: KvRequestAttemptState::RejectedBeforeCommit {
                reason: reason.into(),
            },
            updated_at: OffsetDateTime::now_utc(),
        },
        operation_deadline,
    )
}

pub fn mark_cluster_kv_request_unknown(
    paths: &ClusterKvPaths,
    request: &KvCommandRequest,
    detail: impl Into<String>,
) -> Result<(), KvError> {
    record_cluster_kv_request_attempt(
        paths,
        KvRequestAttemptRecord {
            request_id: request.request_id.clone(),
            command_digest: request.digest()?,
            state: KvRequestAttemptState::Unknown {
                detail: detail.into(),
            },
            updated_at: OffsetDateTime::now_utc(),
        },
    )
}

pub fn mark_cluster_kv_request_unknown_before(
    paths: &ClusterKvPaths,
    request: &KvCommandRequest,
    detail: impl Into<String>,
    operation_deadline: Instant,
) -> Result<(), KvError> {
    record_cluster_kv_request_attempt_before(
        paths,
        KvRequestAttemptRecord {
            request_id: request.request_id.clone(),
            command_digest: request.digest()?,
            state: KvRequestAttemptState::Unknown {
                detail: detail.into(),
            },
            updated_at: OffsetDateTime::now_utc(),
        },
        operation_deadline,
    )
}

pub fn read_cluster_kv_request_attempt(
    paths: &ClusterKvPaths,
    request_id: &KvRequestId,
) -> Result<Option<KvRequestOutcome>, KvError> {
    let _lock = lock_request_attempt_ledger(paths)?;
    let mut cache = REQUEST_ATTEMPT_LEDGER_CACHE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    Ok(request_attempt_cache_entry(paths, &mut cache, None)?
        .ledger
        .outcome(request_id))
}

/// Deadline-aware local request-attempt lookup for end-to-end authoritative
/// operations. Unlike the legacy local maintenance API, this lookup cannot
/// inherit the 30-second ledger-lock budget after its network budget is spent.
pub fn read_cluster_kv_request_attempt_before(
    paths: &ClusterKvPaths,
    request_id: &KvRequestId,
    operation_deadline: Instant,
) -> Result<Option<KvRequestOutcome>, KvError> {
    let _lock = lock_request_attempt_ledger_before(paths, Some(operation_deadline))?;
    let mut cache = lock_request_attempt_cache_before(paths, operation_deadline)?;
    let outcome = request_attempt_cache_entry(paths, &mut cache, Some(operation_deadline))?
        .ledger
        .outcome(request_id);
    ensure_request_attempt_deadline(paths, Some(operation_deadline))?;
    Ok(outcome)
}

pub fn resolve_cluster_kv_request_outcome(
    paths: &ClusterKvPaths,
    snapshot: &KvSnapshot,
    request_id: &KvRequestId,
) -> Result<KvRequestOutcome, KvError> {
    if let Some(record) = snapshot
        .committed_requests
        .iter()
        .find(|record| &record.request_id == request_id)
    {
        return Ok(KvRequestOutcome::Committed(record.clone()));
    }
    if snapshot
        .expired_requests
        .iter()
        .any(|expired| expired == request_id)
    {
        return Ok(KvRequestOutcome::Expired);
    }
    Ok(
        read_cluster_kv_request_attempt(paths, request_id)?.unwrap_or_else(|| {
            KvRequestOutcome::Unknown {
                detail: "request id has no retained local or committed record".to_string(),
            }
        }),
    )
}
