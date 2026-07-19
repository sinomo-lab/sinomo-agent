use crate::internal::*;

pub const MAX_CLUSTER_KV_REQUEST_ATTEMPTS: usize = 4096;
pub const MAX_CLUSTER_KV_REQUEST_ATTEMPT_TOMBSTONES: usize = 4096;
pub const MAX_CLUSTER_KV_REQUEST_ATTEMPT_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
struct KvRequestAttemptLedger {
    #[serde(default, deserialize_with = "deserialize_fallible_vec")]
    attempts: Vec<KvRequestAttemptRecord>,
    #[serde(default, deserialize_with = "deserialize_fallible_vec")]
    expired: Vec<KvRequestId>,
}

fn lock_request_attempt_ledger(paths: &ClusterKvPaths) -> Result<File, KvError> {
    fs::create_dir_all(&paths.root).map_err(|source| KvError::Io {
        path: paths.root.display().to_string(),
        detail: source.to_string(),
    })?;
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
    lock_node_exclusive(
        &lock,
        &paths.request_attempts_lock,
        CLUSTER_KV_COMMIT_SEQUENCE_LOCK_WAIT,
    )?;
    Ok(lock)
}

fn read_request_attempt_ledger(paths: &ClusterKvPaths) -> Result<KvRequestAttemptLedger, KvError> {
    let ledger = read_optional_object::<KvRequestAttemptLedger>(&paths.request_attempts)?
        .unwrap_or_default();
    if ledger.attempts.len() > MAX_CLUSTER_KV_REQUEST_ATTEMPTS
        || ledger.expired.len() > MAX_CLUSTER_KV_REQUEST_ATTEMPT_TOMBSTONES
    {
        return Err(KvError::RequestRecordLimit {
            observed_records: ledger.attempts.len().saturating_add(ledger.expired.len()),
            max_records: MAX_CLUSTER_KV_REQUEST_ATTEMPTS
                .saturating_add(MAX_CLUSTER_KV_REQUEST_ATTEMPT_TOMBSTONES),
        });
    }
    let encoded_bytes = request_attempt_encoded_bytes(&ledger)?;
    if encoded_bytes > MAX_CLUSTER_KV_REQUEST_ATTEMPT_BYTES {
        return Err(KvError::RequestRecordBytesLimit {
            observed_bytes: encoded_bytes,
            max_bytes: MAX_CLUSTER_KV_REQUEST_ATTEMPT_BYTES,
        });
    }
    Ok(ledger)
}

fn request_attempt_encoded_bytes(ledger: &KvRequestAttemptLedger) -> Result<usize, KvError> {
    let mut total = 0_usize;
    for record in &ledger.attempts {
        total = total
            .checked_add(encode_kv_value(record)?.len())
            .ok_or_else(|| KvError::ResourceExhausted {
                operation: "measure cluster KV request attempts".to_string(),
                requested_bytes: usize::MAX,
            })?;
    }
    Ok(total)
}

pub fn record_cluster_kv_request_attempt(
    paths: &ClusterKvPaths,
    record: KvRequestAttemptRecord,
) -> Result<(), KvError> {
    let _lock = lock_request_attempt_ledger(paths)?;
    let mut ledger = read_request_attempt_ledger(paths)?;
    if ledger
        .expired
        .iter()
        .any(|request_id| request_id == &record.request_id)
    {
        return Err(KvError::RequestExpired {
            request_id: record.request_id,
        });
    }
    if let Some(existing) = ledger
        .attempts
        .iter()
        .find(|attempt| attempt.request_id == record.request_id)
    {
        if existing.command_digest != record.command_digest {
            return Err(KvError::RequestIdConflict {
                request_id: record.request_id,
            });
        }
        // Once an attempt may have escaped the local pre-append boundary, a
        // later retry that is rejected locally cannot prove that the earlier
        // attempt did not commit elsewhere. Preserve uncertainty until
        // committed Raft history or an expiry tombstone resolves it.
        if matches!(
            &existing.state,
            KvRequestAttemptState::Submitted | KvRequestAttemptState::Unknown { .. }
        ) && matches!(
            &record.state,
            KvRequestAttemptState::Submitted | KvRequestAttemptState::RejectedBeforeCommit { .. }
        ) {
            return Ok(());
        }
    }
    ledger
        .attempts
        .retain(|attempt| attempt.request_id != record.request_id);
    ledger.attempts.push(record);
    ledger.attempts.sort_by_key(|attempt| attempt.updated_at);
    while ledger.attempts.len() > MAX_CLUSTER_KV_REQUEST_ATTEMPTS
        || request_attempt_encoded_bytes(&ledger)? > MAX_CLUSTER_KV_REQUEST_ATTEMPT_BYTES
    {
        let expired = ledger.attempts.remove(0).request_id;
        ledger.expired.retain(|request_id| request_id != &expired);
        ledger.expired.push(expired);
    }
    if ledger.expired.len() > MAX_CLUSTER_KV_REQUEST_ATTEMPT_TOMBSTONES {
        let excess = ledger
            .expired
            .len()
            .saturating_sub(MAX_CLUSTER_KV_REQUEST_ATTEMPT_TOMBSTONES);
        ledger.expired.drain(..excess);
    }
    write_object_file(&paths.request_attempts, &ledger)
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

pub fn read_cluster_kv_request_attempt(
    paths: &ClusterKvPaths,
    request_id: &KvRequestId,
) -> Result<Option<KvRequestOutcome>, KvError> {
    let _lock = lock_request_attempt_ledger(paths)?;
    let ledger = read_request_attempt_ledger(paths)?;
    if ledger.expired.iter().any(|expired| expired == request_id) {
        return Ok(Some(KvRequestOutcome::Expired));
    }
    Ok(ledger
        .attempts
        .iter()
        .find(|attempt| &attempt.request_id == request_id)
        .map(|attempt| match &attempt.state {
            KvRequestAttemptState::Submitted => KvRequestOutcome::Unknown {
                detail: "request was submitted but no durable commit outcome is known".to_string(),
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
        }))
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
