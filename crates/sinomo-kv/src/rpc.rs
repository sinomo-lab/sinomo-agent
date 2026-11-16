use crate::internal::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterKvRpcPayload {
    pub route: &'static str,
    pub category: &'static str,
    pub body: Vec<u8>,
}

pub(crate) fn encode_cluster_kv_rpc_body<T: Serialize + ?Sized>(
    value: &T,
) -> Result<Vec<u8>, KvError> {
    sinomo_object_serde::to_bytes(value, KV_OBJECT_VERSION_ID, LengthMode::U32)
        .map_err(kv_object_error)
}

pub(crate) fn decode_cluster_kv_rpc_body<T: DeserializeOwned>(body: &[u8]) -> Result<T, KvError> {
    sinomo_object_serde::from_bytes(body).map_err(kv_object_error)
}

pub fn cluster_kv_local_snapshot_rpc_payload() -> Result<ClusterKvRpcPayload, KvError> {
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_KV_LOCAL_SNAPSHOT_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(&())?,
    })
}

pub fn decode_cluster_kv_local_snapshot_rpc_request_body(body: &[u8]) -> Result<(), KvError> {
    decode_cluster_kv_rpc_body(body)
}

pub fn cluster_kv_local_snapshot_response_rpc_payload(
    snapshot: &KvLocalRead<KvSnapshot>,
) -> Result<ClusterKvRpcPayload, KvError> {
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_KV_LOCAL_SNAPSHOT_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(snapshot)?,
    })
}

pub fn decode_cluster_kv_local_snapshot_rpc_body(
    body: &[u8],
) -> Result<KvLocalRead<KvSnapshot>, KvError> {
    let snapshot: KvLocalRead<KvSnapshot> = decode_cluster_kv_rpc_body(body)?;
    snapshot.value.validate_resource_bounds()?;
    Ok(snapshot)
}

pub fn cluster_kv_txn_rpc_payload(request: &KvTxnRequest) -> Result<ClusterKvRpcPayload, KvError> {
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_KV_TXN_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(request)?,
    })
}

pub fn decode_cluster_kv_txn_rpc_request_body(body: &[u8]) -> Result<KvTxnRequest, KvError> {
    decode_cluster_kv_rpc_body(body)
}

pub fn cluster_kv_txn_response_rpc_payload(
    response: &ClusterKvTxnCommitResult,
) -> Result<ClusterKvRpcPayload, KvError> {
    let response = ClusterKvTxnRpcResponse::from(response);
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_KV_TXN_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(&response)?,
    })
}

/// Compact transaction result transported over object RPC.
///
/// The committed KV snapshot is deliberately not part of the wire response.
/// Voters already receive the committed entry through Raft replication, while
/// a non-voter can fetch an authoritative snapshot only when it actually needs
/// to materialize the internal commit result. Keeping the snapshot out of the
/// common response makes transaction cost independent of total keyspace size.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterKvTxnRpcResponse {
    pub request_id: KvRequestId,
    /// Digest of the caller's transaction before the receiving voter adds
    /// authenticated membership/capability fence compares. `None` is accepted
    /// only for responses from a pre-field peer during a mixed-version
    /// rollout; current peers always emit `Some` and validators bind it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submitted_command_digest: Option<KvCommandDigest>,
    /// Digest of the command that actually entered the replicated log. This
    /// can include authority-fence compares added by the authenticated leader.
    pub command_digest: KvCommandDigest,
    pub txn_response: Option<KvTxnResponse>,
    pub authority: KvAuthorityProof,
    pub replayed_committed_request: bool,
}

impl From<&ClusterKvTxnCommitResult> for ClusterKvTxnRpcResponse {
    fn from(response: &ClusterKvTxnCommitResult) -> Self {
        Self {
            request_id: response.request_id.clone(),
            submitted_command_digest: Some(response.command_digest),
            command_digest: response.command_digest,
            txn_response: response.txn_response.clone(),
            authority: response.authority,
            replayed_committed_request: response.replayed_committed_request,
        }
    }
}

impl From<ClusterKvTxnCommitResult> for ClusterKvTxnRpcResponse {
    fn from(response: ClusterKvTxnCommitResult) -> Self {
        Self {
            request_id: response.request_id,
            submitted_command_digest: Some(response.command_digest),
            command_digest: response.command_digest,
            txn_response: response.txn_response,
            authority: response.authority,
            replayed_committed_request: response.replayed_committed_request,
        }
    }
}

pub fn decode_cluster_kv_txn_rpc_body(body: &[u8]) -> Result<ClusterKvTxnRpcResponse, KvError> {
    decode_cluster_kv_rpc_body(body)
}

/// Marker request for an authoritative read. The receiver must generate a
/// fresh barrier request id for every invocation; callers cannot supply or
/// reuse one and thereby replay stale authority.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterKvAuthoritativeSnapshotRequest;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterKvEntryRequest {
    pub key: String,
}

/// Requests an ordered, bounded set of exact keys from one authoritative
/// committed view. Duplicate keys are retained so callers receive exactly one
/// result per requested position.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterKvAuthoritativeEntriesRequest {
    pub keys: Vec<String>,
}

/// Wire representation for an ordered exact-key-set response. SOBJ arrays are
/// homogeneous, so a `Vec<Option<KvEntry>>` cannot directly mix present and
/// missing values. Present entries are dense and `missing` marks absent output
/// positions; an empty bitmap means every requested key was present.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterKvAuthoritativeEntriesResponse {
    pub entries: Vec<KvEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub missing: Vec<u64>,
    pub authority: KvAuthorityProof,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterKvAuthoritativeRangePrefixRequest {
    pub prefix: String,
}

/// Starts a watch after a server-generated authoritative barrier. Unlike the
/// local watch request, callers cannot choose a revision that predates the
/// barrier or reuse an old proof.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterKvAuthoritativeWatchStartRequest {
    pub prefix: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterKvRequestOutcomeRequest {
    pub request_id: KvRequestId,
}

pub fn cluster_kv_authoritative_snapshot_rpc_payload() -> Result<ClusterKvRpcPayload, KvError> {
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_KV_AUTHORITATIVE_SNAPSHOT_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(&ClusterKvAuthoritativeSnapshotRequest)?,
    })
}

pub fn decode_cluster_kv_authoritative_snapshot_rpc_request_body(
    body: &[u8],
) -> Result<ClusterKvAuthoritativeSnapshotRequest, KvError> {
    decode_cluster_kv_rpc_body(body)
}

pub fn cluster_kv_authoritative_snapshot_response_rpc_payload(
    response: &KvAuthoritativeRead<KvSnapshot>,
) -> Result<ClusterKvRpcPayload, KvError> {
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_KV_AUTHORITATIVE_SNAPSHOT_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(response)?,
    })
}

pub fn decode_cluster_kv_authoritative_snapshot_rpc_body(
    body: &[u8],
) -> Result<KvAuthoritativeRead<KvSnapshot>, KvError> {
    let response: KvAuthoritativeRead<KvSnapshot> = decode_cluster_kv_rpc_body(body)?;
    validate_cluster_kv_authoritative_snapshot_read(&response)?;
    Ok(response)
}

/// Applies the authoritative snapshot invariants after either contiguous or
/// stream-native SOBJ decoding.
pub fn validate_cluster_kv_authoritative_snapshot_read(
    response: &KvAuthoritativeRead<KvSnapshot>,
) -> Result<(), KvError> {
    response.value.validate_resource_bounds()?;
    validate_cluster_kv_current_term_authority_proof(&response.authority)?;
    if response.authority.revision != response.value.revision {
        return Err(KvError::AuthorityUnavailable {
            detail: format!(
                "authoritative proof revision {:?} does not match snapshot revision {:?}",
                response.authority.revision, response.value.revision
            ),
        });
    }
    Ok(())
}

pub fn cluster_kv_authoritative_entry_rpc_payload(
    key: impl Into<String>,
) -> Result<ClusterKvRpcPayload, KvError> {
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(&ClusterKvEntryRequest { key: key.into() })?,
    })
}

pub fn decode_cluster_kv_authoritative_entry_rpc_request_body(
    body: &[u8],
) -> Result<ClusterKvEntryRequest, KvError> {
    decode_cluster_kv_rpc_body(body)
}

pub fn cluster_kv_authoritative_entry_response_rpc_payload(
    response: &KvAuthoritativeRead<Option<KvEntry>>,
) -> Result<ClusterKvRpcPayload, KvError> {
    validate_cluster_kv_authoritative_entry_read(response)?;
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_KV_AUTHORITATIVE_ENTRY_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(response)?,
    })
}

pub fn decode_cluster_kv_authoritative_entry_rpc_body(
    body: &[u8],
) -> Result<KvAuthoritativeRead<Option<KvEntry>>, KvError> {
    let response = decode_cluster_kv_rpc_body(body)?;
    validate_cluster_kv_authoritative_entry_read(&response)?;
    Ok(response)
}

pub fn validate_cluster_kv_authoritative_entry_read(
    response: &KvAuthoritativeRead<Option<KvEntry>>,
) -> Result<(), KvError> {
    validate_cluster_kv_current_term_authority_proof(&response.authority)?;
    if let Some(entry) = &response.value
        && (entry.create_revision > entry.modify_revision
            || entry.modify_revision > response.authority.revision)
    {
        return Err(KvError::AuthorityUnavailable {
            detail: format!(
                "authoritative entry {:?} has revisions {}..{} outside proof revision {}",
                entry.key,
                entry.create_revision.0,
                entry.modify_revision.0,
                response.authority.revision.0,
            ),
        });
    }
    Ok(())
}

pub fn cluster_kv_authoritative_entries_rpc_payload(
    keys: &[String],
) -> Result<ClusterKvRpcPayload, KvError> {
    validate_cluster_kv_authoritative_entries_request_keys(keys)?;
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_KV_AUTHORITATIVE_ENTRIES_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(&ClusterKvAuthoritativeEntriesRequest {
            keys: keys.to_vec(),
        })?,
    })
}

pub fn decode_cluster_kv_authoritative_entries_rpc_request_body(
    body: &[u8],
) -> Result<ClusterKvAuthoritativeEntriesRequest, KvError> {
    let request: ClusterKvAuthoritativeEntriesRequest = decode_cluster_kv_rpc_body(body)?;
    validate_cluster_kv_authoritative_entries_request_keys(&request.keys)?;
    Ok(request)
}

pub fn cluster_kv_authoritative_entries_response_rpc_payload(
    keys: &[String],
    response: &KvAuthoritativeRead<Vec<Option<KvEntry>>>,
) -> Result<ClusterKvRpcPayload, KvError> {
    let response = pack_cluster_kv_authoritative_entries_response(keys, response)?;
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_KV_AUTHORITATIVE_ENTRIES_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(&response)?,
    })
}

pub fn decode_cluster_kv_authoritative_entries_rpc_body(
    body: &[u8],
    expected_keys: &[String],
) -> Result<KvAuthoritativeRead<Vec<Option<KvEntry>>>, KvError> {
    let response = decode_cluster_kv_rpc_body(body)?;
    unpack_cluster_kv_authoritative_entries_response(expected_keys, response)
}

pub fn pack_cluster_kv_authoritative_entries_response(
    keys: &[String],
    response: &KvAuthoritativeRead<Vec<Option<KvEntry>>>,
) -> Result<ClusterKvAuthoritativeEntriesResponse, KvError> {
    validate_cluster_kv_authoritative_entries_read(keys, response)?;
    let mut entries = Vec::new();
    entries
        .try_reserve(response.value.len())
        .map_err(|_| KvError::ResourceExhausted {
            operation: "pack authoritative exact-key response entries".to_string(),
            requested_bytes: response
                .value
                .len()
                .saturating_mul(std::mem::size_of::<KvEntry>()),
        })?;
    let has_missing = response.value.iter().any(Option::is_none);
    let mut missing = Vec::new();
    if has_missing {
        let word_count = response.value.len().div_ceil(u64::BITS as usize);
        missing
            .try_reserve(word_count)
            .map_err(|_| KvError::ResourceExhausted {
                operation: "pack authoritative exact-key response missing bitmap".to_string(),
                requested_bytes: word_count.saturating_mul(std::mem::size_of::<u64>()),
            })?;
        missing.resize(word_count, 0);
    }
    for (index, value) in response.value.iter().enumerate() {
        if let Some(entry) = value {
            entries.push(try_clone_kv_entry(
                entry,
                "pack authoritative exact-key response entry",
            )?);
        } else if has_missing {
            missing[index / u64::BITS as usize] |= 1_u64 << (index % u64::BITS as usize);
        }
    }
    Ok(ClusterKvAuthoritativeEntriesResponse {
        entries,
        missing,
        authority: response.authority,
    })
}

pub fn unpack_cluster_kv_authoritative_entries_response(
    keys: &[String],
    response: ClusterKvAuthoritativeEntriesResponse,
) -> Result<KvAuthoritativeRead<Vec<Option<KvEntry>>>, KvError> {
    validate_cluster_kv_authoritative_entries_request_keys(keys)?;
    let expected_words = keys.len().div_ceil(u64::BITS as usize);
    if !response.missing.is_empty() && response.missing.len() != expected_words {
        return Err(KvError::AuthorityUnavailable {
            detail: format!(
                "authoritative exact-key response has {} missing-bitmap words, expected {expected_words}",
                response.missing.len(),
            ),
        });
    }
    if let Some(last) = response.missing.last() {
        let used_bits = keys.len() % u64::BITS as usize;
        if used_bits != 0 && (*last >> used_bits) != 0 {
            return Err(KvError::AuthorityUnavailable {
                detail: "authoritative exact-key response sets missing bits outside the request"
                    .to_string(),
            });
        }
    }
    let missing_count = (0..keys.len())
        .filter(|index| {
            !response.missing.is_empty()
                && response.missing[*index / u64::BITS as usize]
                    & (1_u64 << (*index % u64::BITS as usize))
                    != 0
        })
        .count();
    let expected_entries = keys.len().saturating_sub(missing_count);
    if response.entries.len() != expected_entries {
        return Err(KvError::AuthorityUnavailable {
            detail: format!(
                "authoritative exact-key response has {} dense entries, expected {expected_entries}",
                response.entries.len(),
            ),
        });
    }

    let mut dense = response.entries.into_iter();
    let mut values = Vec::new();
    values
        .try_reserve(keys.len())
        .map_err(|_| KvError::ResourceExhausted {
            operation: "unpack authoritative exact-key response".to_string(),
            requested_bytes: keys
                .len()
                .saturating_mul(std::mem::size_of::<Option<KvEntry>>()),
        })?;
    for index in 0..keys.len() {
        let is_missing = !response.missing.is_empty()
            && response.missing[index / u64::BITS as usize]
                & (1_u64 << (index % u64::BITS as usize))
                != 0;
        values.push(if is_missing { None } else { dense.next() });
    }
    let read = KvAuthoritativeRead {
        value: values,
        authority: response.authority,
    };
    validate_cluster_kv_authoritative_entries_read(keys, &read)?;
    Ok(read)
}

pub fn validate_cluster_kv_authoritative_entries_request_keys(
    keys: &[String],
) -> Result<(), KvError> {
    if keys.is_empty() {
        return Err(KvError::Serialization {
            detail: "authoritative exact-key request must contain at least one key".to_string(),
        });
    }
    if keys.len() > CLUSTER_KV_AUTHORITATIVE_ENTRIES_MAX_KEYS {
        return Err(KvError::ResourceExhausted {
            operation: "authoritative exact-key request cardinality".to_string(),
            requested_bytes: keys.len(),
        });
    }
    let key_bytes = keys
        .iter()
        .fold(0_usize, |total, key| total.saturating_add(key.len()));
    if key_bytes > CLUSTER_KV_AUTHORITATIVE_ENTRIES_MAX_PAYLOAD_BYTES {
        return Err(KvError::ResourceExhausted {
            operation: "authoritative exact-key request payload".to_string(),
            requested_bytes: key_bytes,
        });
    }
    Ok(())
}

pub fn validate_cluster_kv_authoritative_entries_read(
    keys: &[String],
    response: &KvAuthoritativeRead<Vec<Option<KvEntry>>>,
) -> Result<(), KvError> {
    validate_cluster_kv_authoritative_entries_request_keys(keys)?;
    validate_cluster_kv_current_term_authority_proof(&response.authority)?;
    if response.value.len() != keys.len() {
        return Err(KvError::AuthorityUnavailable {
            detail: format!(
                "authoritative exact-key response has {} entries for {} requested keys",
                response.value.len(),
                keys.len(),
            ),
        });
    }

    let mut payload_bytes = 0_usize;
    let mut duplicate_values = BTreeMap::<&str, &Option<KvEntry>>::new();
    for (key, value) in keys.iter().zip(&response.value) {
        if let Some(entry) = value {
            if entry.key != *key {
                return Err(KvError::AuthorityUnavailable {
                    detail: format!(
                        "authoritative exact-key response returned {:?} for requested key {:?}",
                        entry.key, key,
                    ),
                });
            }
            validate_kv_value_len(entry.value.len())?;
            payload_bytes = payload_bytes.saturating_add(kv_entry_payload_bytes(entry));
            if entry.create_revision > entry.modify_revision
                || entry.modify_revision > response.authority.revision
            {
                return Err(KvError::AuthorityUnavailable {
                    detail: format!(
                        "authoritative entry {:?} has revisions {}..{} outside proof revision {}",
                        entry.key,
                        entry.create_revision.0,
                        entry.modify_revision.0,
                        response.authority.revision.0,
                    ),
                });
            }
        }
        if let Some(previous) = duplicate_values.insert(key.as_str(), value)
            && previous != value
        {
            return Err(KvError::AuthorityUnavailable {
                detail: format!(
                    "authoritative exact-key response returned inconsistent values for duplicate key {key:?}"
                ),
            });
        }
    }
    if payload_bytes > CLUSTER_KV_AUTHORITATIVE_ENTRIES_MAX_PAYLOAD_BYTES {
        return Err(KvError::ResourceExhausted {
            operation: "authoritative exact-key response payload".to_string(),
            requested_bytes: payload_bytes,
        });
    }
    Ok(())
}

/// Compatibility/test projection from an already-materialized authoritative
/// snapshot. The production exact-key route projects directly from resident
/// indexed Raft storage after one ReadIndex and must not call this helper.
pub fn project_cluster_kv_authoritative_entries(
    read: &KvAuthoritativeRead<KvSnapshot>,
    keys: &[String],
) -> Result<KvAuthoritativeRead<Vec<Option<KvEntry>>>, KvError> {
    validate_cluster_kv_authoritative_snapshot_read(read)?;
    validate_cluster_kv_authoritative_entries_request_keys(keys)?;
    let mut values = Vec::new();
    values
        .try_reserve(keys.len())
        .map_err(|_| KvError::ResourceExhausted {
            operation: "project authoritative exact-key response".to_string(),
            requested_bytes: keys
                .len()
                .saturating_mul(std::mem::size_of::<Option<KvEntry>>()),
        })?;
    for key in keys {
        values.push(
            read.value
                .get(key)
                .map(|entry| try_clone_kv_entry(entry, "project authoritative exact-key response"))
                .transpose()?,
        );
    }
    let response = KvAuthoritativeRead {
        value: values,
        authority: read.authority,
    };
    validate_cluster_kv_authoritative_entries_read(keys, &response)?;
    Ok(response)
}

pub fn cluster_kv_authoritative_range_prefix_rpc_payload(
    prefix: impl Into<String>,
) -> Result<ClusterKvRpcPayload, KvError> {
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(&ClusterKvAuthoritativeRangePrefixRequest {
            prefix: prefix.into(),
        })?,
    })
}

pub fn decode_cluster_kv_authoritative_range_prefix_rpc_request_body(
    body: &[u8],
) -> Result<ClusterKvAuthoritativeRangePrefixRequest, KvError> {
    decode_cluster_kv_rpc_body(body)
}

pub fn cluster_kv_authoritative_range_prefix_response_rpc_payload(
    response: &KvAuthoritativeRead<Vec<KvEntry>>,
) -> Result<ClusterKvRpcPayload, KvError> {
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_KV_AUTHORITATIVE_RANGE_PREFIX_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(response)?,
    })
}

pub fn decode_cluster_kv_authoritative_range_prefix_rpc_body(
    body: &[u8],
) -> Result<KvAuthoritativeRead<Vec<KvEntry>>, KvError> {
    let response: KvAuthoritativeRead<Vec<KvEntry>> = decode_cluster_kv_rpc_body(body)?;
    validate_cluster_kv_current_term_authority_proof(&response.authority)?;
    Ok(response)
}

/// Projects a bounded prefix range from the exact snapshot justified by an
/// authoritative proof. Filtering a separately reopened local view here would
/// reintroduce a stale-read race after the barrier.
pub fn project_cluster_kv_authoritative_range_prefix(
    read: &KvAuthoritativeRead<KvSnapshot>,
    prefix: &str,
) -> Result<KvAuthoritativeRead<Vec<KvEntry>>, KvError> {
    validate_cluster_kv_current_term_authority_proof(&read.authority)?;
    if read.authority.revision != read.value.revision {
        return Err(KvError::AuthorityUnavailable {
            detail: format!(
                "authoritative proof revision {:?} does not match snapshot revision {:?}",
                read.authority.revision, read.value.revision
            ),
        });
    }
    Ok(KvAuthoritativeRead {
        value: read.value.range_prefix_limited_by_payload_bytes(
            prefix,
            CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
            CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES,
        )?,
        authority: read.authority,
    })
}

/// Bootstrap/recovery reads consume ONE owned, native ReadIndex snapshot. This
/// does not join independently observed pages or reopen an observational file.
/// The complete result remains bounded by the native snapshot resource budget;
/// crossing either bound rejects the whole read rather than truncating it.
pub fn project_cluster_kv_authoritative_complete_range_prefix(
    read: &KvAuthoritativeRead<KvSnapshot>,
    prefix: &str,
) -> Result<KvAuthoritativeRead<Vec<KvEntry>>, KvError> {
    validate_cluster_kv_authoritative_snapshot_read(read)?;
    let mut previous: Option<&str> = None;
    for entry in &read.value.entries {
        if previous.is_some_and(|key| key >= entry.key.as_str())
            || entry.create_revision > entry.modify_revision
            || entry.modify_revision > read.authority.revision
        {
            return Err(KvError::AuthorityUnavailable {
                detail: "complete prefix source is not one canonical proof-bound snapshot".into(),
            });
        }
        previous = Some(&entry.key);
    }
    let projected = KvAuthoritativeRead {
        value: read.value.range_prefix_limited_by_payload_bytes(
            prefix,
            CLUSTER_KV_COMPLETE_RANGE_PREFIX_MAX_ENTRIES,
            CLUSTER_KV_COMPLETE_RANGE_PREFIX_MAX_PAYLOAD_BYTES,
        )?,
        authority: read.authority,
    };
    validate_cluster_kv_authoritative_complete_range_prefix_read(prefix, &projected)?;
    Ok(projected)
}

pub fn validate_cluster_kv_authoritative_complete_range_prefix_read(
    prefix: &str,
    read: &KvAuthoritativeRead<Vec<KvEntry>>,
) -> Result<(), KvError> {
    validate_cluster_kv_current_term_authority_proof(&read.authority)?;
    if read.value.len() > CLUSTER_KV_COMPLETE_RANGE_PREFIX_MAX_ENTRIES {
        return Err(KvError::RangeLimit {
            prefix: prefix.into(),
            max_entries: CLUSTER_KV_COMPLETE_RANGE_PREFIX_MAX_ENTRIES,
        });
    }
    let mut bytes = 0_usize;
    let mut previous: Option<&str> = None;
    for entry in &read.value {
        if entry.value.len() > MAX_KV_VALUE_BYTES {
            return Err(KvError::ValueTooLarge {
                actual_bytes: entry.value.len(),
                max_bytes: MAX_KV_VALUE_BYTES,
            });
        }
        bytes = bytes
            .saturating_add(entry.key.len())
            .saturating_add(entry.value.len());
        if bytes > CLUSTER_KV_COMPLETE_RANGE_PREFIX_MAX_PAYLOAD_BYTES {
            return Err(KvError::RangePayloadLimit {
                prefix: prefix.into(),
                observed_bytes: bytes,
                max_bytes: CLUSTER_KV_COMPLETE_RANGE_PREFIX_MAX_PAYLOAD_BYTES,
            });
        }
        if !entry.key.starts_with(prefix)
            || previous.is_some_and(|key| key >= entry.key.as_str())
            || entry.create_revision > entry.modify_revision
            || entry.modify_revision > read.authority.revision
        {
            return Err(KvError::AuthorityUnavailable {
                detail: "complete prefix entries have inconsistent scope, order, or proof revision"
                    .into(),
            });
        }
        previous = Some(&entry.key);
    }
    Ok(())
}

pub fn cluster_kv_authoritative_watch_start_rpc_payload(
    prefix: impl Into<String>,
) -> Result<ClusterKvRpcPayload, KvError> {
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_KV_AUTHORITATIVE_WATCH_START_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(&ClusterKvAuthoritativeWatchStartRequest {
            prefix: prefix.into(),
        })?,
    })
}

pub fn decode_cluster_kv_authoritative_watch_start_rpc_request_body(
    body: &[u8],
) -> Result<ClusterKvAuthoritativeWatchStartRequest, KvError> {
    decode_cluster_kv_rpc_body(body)
}

pub fn cluster_kv_authoritative_watch_start_response_rpc_payload(
    response: &KvAuthoritativeRead<ClusterKvWatchRegistration>,
) -> Result<ClusterKvRpcPayload, KvError> {
    validate_cluster_kv_authoritative_watch_start_response(response)?;
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_KV_AUTHORITATIVE_WATCH_START_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(response)?,
    })
}

pub fn decode_cluster_kv_authoritative_watch_start_rpc_body(
    body: &[u8],
) -> Result<KvAuthoritativeRead<ClusterKvWatchRegistration>, KvError> {
    let response: KvAuthoritativeRead<ClusterKvWatchRegistration> =
        decode_cluster_kv_rpc_body(body)?;
    validate_cluster_kv_authoritative_watch_start_response(&response)?;
    Ok(response)
}

pub fn validate_cluster_kv_authoritative_watch_start_response(
    response: &KvAuthoritativeRead<ClusterKvWatchRegistration>,
) -> Result<(), KvError> {
    validate_cluster_kv_current_term_authority_proof(&response.authority)?;
    let expected = response.authority.revision.next();
    if response.value.next_revision != expected {
        return Err(KvError::AuthorityUnavailable {
            detail: format!(
                "authoritative watch starts at {:?}, expected the revision after barrier {:?}",
                response.value.next_revision, response.authority.revision
            ),
        });
    }
    Ok(())
}

/// Validate the authority coordinates of a transaction result, not a read.
/// Callers must separately bind the response to the original request ID and
/// submitted command digest. Exact committed receipts authorize only that
/// original result; subsequent reads still require current-term quorum proof.
/// Membership-aware forwarding additionally validates the voter configuration.
pub fn validate_cluster_kv_txn_authority_proof(
    proof: &KvAuthorityProof,
    replayed_committed_request: bool,
    result_revision: KvRevision,
) -> Result<(), KvError> {
    if proof.term == 0 || proof.commit_index == 0 || proof.revision != result_revision {
        return Err(KvError::AuthorityUnavailable {
            detail: "transaction response carries invalid authority coordinates".into(),
        });
    }
    match (&proof.basis, replayed_committed_request) {
        (KvAuthorityBasis::ExactCommittedRequest, true) => Ok(()),
        (KvAuthorityBasis::CurrentTermQuorum { .. }, false) => {
            validate_cluster_kv_current_term_authority_proof(proof)
        }
        _ => Err(KvError::AuthorityUnavailable {
            detail: "transaction authority basis disagrees with committed replay status".into(),
        }),
    }
}

pub fn validate_cluster_kv_current_term_authority_proof(
    proof: &KvAuthorityProof,
) -> Result<(), KvError> {
    match proof.basis {
        KvAuthorityBasis::CurrentTermQuorum {
            voter_acks,
            required_quorum,
        } if proof.term > 0 && required_quorum > 0 && voter_acks >= required_quorum => Ok(()),
        _ => Err(KvError::AuthorityUnavailable {
            detail: "authoritative read response lacks a valid current-term quorum proof"
                .to_string(),
        }),
    }
}

pub fn cluster_kv_request_outcome_rpc_payload(
    request: &ClusterKvRequestOutcomeRequest,
) -> Result<ClusterKvRpcPayload, KvError> {
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_KV_REQUEST_OUTCOME_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(request)?,
    })
}

pub fn decode_cluster_kv_request_outcome_rpc_request_body(
    body: &[u8],
) -> Result<ClusterKvRequestOutcomeRequest, KvError> {
    decode_cluster_kv_rpc_body(body)
}

pub fn cluster_kv_request_outcome_response_rpc_payload(
    read: &ClusterKvRequestOutcomeRead,
) -> Result<ClusterKvRpcPayload, KvError> {
    validate_cluster_kv_request_outcome_read(read)?;
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_KV_REQUEST_OUTCOME_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(read)?,
    })
}

pub fn decode_cluster_kv_request_outcome_rpc_body(
    body: &[u8],
) -> Result<ClusterKvRequestOutcomeRead, KvError> {
    let read = decode_cluster_kv_rpc_body(body)?;
    validate_cluster_kv_request_outcome_read(&read)?;
    Ok(read)
}

pub fn validate_cluster_kv_request_outcome_read(
    read: &ClusterKvRequestOutcomeRead,
) -> Result<(), KvError> {
    validate_cluster_kv_current_term_authority_proof(&read.authority)?;
    if let KvRequestOutcome::Committed(record) = &read.outcome
        && (record.term > read.authority.term
            || record.log_index > read.authority.commit_index
            || record.revision > read.authority.revision)
    {
        return Err(KvError::AuthorityUnavailable {
            detail: format!(
                "committed request at term {}, index {}, revision {:?} is newer than outcome authority at term {}, index {}, revision {:?}",
                record.term,
                record.log_index,
                record.revision,
                read.authority.term,
                read.authority.commit_index,
                read.authority.revision
            ),
        });
    }
    Ok(())
}

pub fn cluster_kv_local_entry_rpc_payload(
    key: impl Into<String>,
) -> Result<ClusterKvRpcPayload, KvError> {
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_KV_LOCAL_ENTRY_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(&ClusterKvEntryRequest { key: key.into() })?,
    })
}

pub fn decode_cluster_kv_local_entry_rpc_request_body(
    body: &[u8],
) -> Result<ClusterKvEntryRequest, KvError> {
    decode_cluster_kv_rpc_body(body)
}

pub fn cluster_kv_local_entry_response_rpc_payload(
    entry: &KvLocalRead<Option<KvEntry>>,
) -> Result<ClusterKvRpcPayload, KvError> {
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_KV_LOCAL_ENTRY_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(entry)?,
    })
}

pub fn decode_cluster_kv_local_entry_rpc_body(
    body: &[u8],
) -> Result<KvLocalRead<Option<KvEntry>>, KvError> {
    decode_cluster_kv_rpc_body(body)
}

pub fn cluster_kv_local_range_prefix_rpc_payload(
    prefix: &str,
) -> Result<ClusterKvRpcPayload, KvError> {
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_KV_LOCAL_RANGE_PREFIX_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(prefix)?,
    })
}

pub fn cluster_kv_local_range_prefix_response_rpc_payload(
    entries: &KvLocalRead<Vec<KvEntry>>,
) -> Result<ClusterKvRpcPayload, KvError> {
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_KV_LOCAL_RANGE_PREFIX_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(entries)?,
    })
}

pub fn decode_cluster_kv_local_range_prefix_rpc_body(
    body: &[u8],
) -> Result<KvLocalRead<Vec<KvEntry>>, KvError> {
    decode_cluster_kv_rpc_body(body)
}

pub fn cluster_kv_local_watch_drain_rpc_payload(
    request: &ClusterKvWatchDrainRequest,
) -> Result<ClusterKvRpcPayload, KvError> {
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_KV_LOCAL_WATCH_DRAIN_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(request)?,
    })
}

pub fn decode_cluster_kv_local_watch_drain_rpc_body(
    body: &[u8],
) -> Result<ClusterKvWatchDrainResponse, KvError> {
    decode_cluster_kv_rpc_body(body)
}

pub fn cluster_kv_local_watch_cancel_rpc_payload(
    request: &ClusterKvWatchCancelRequest,
) -> Result<ClusterKvRpcPayload, KvError> {
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_KV_LOCAL_WATCH_CANCEL_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(request)?,
    })
}

pub fn decode_cluster_kv_local_watch_cancel_rpc_body(
    body: &[u8],
) -> Result<ClusterKvWatchCancelResponse, KvError> {
    decode_cluster_kv_rpc_body(body)
}

/// Resolves the durable local outcome of a previously accepted request id.
/// Committed Raft history is authoritative over the local attempt ledger.
pub fn handle_cluster_kv_request_outcome_rpc_payload(
    paths: &ClusterKvPaths,
    snapshot: &KvAuthoritativeRead<KvSnapshot>,
    body: &[u8],
) -> Result<ClusterKvRpcPayload, KvError> {
    let request = decode_cluster_kv_request_outcome_rpc_request_body(body)?;
    let outcome = resolve_cluster_kv_request_outcome(paths, &snapshot.value, &request.request_id)?;
    cluster_kv_request_outcome_response_rpc_payload(&ClusterKvRequestOutcomeRead {
        authority: snapshot.authority,
        outcome,
    })
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterKvWatchEventsRequest {
    pub state: ClusterBootstrapState,
    pub leader_id: NodeId,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_fallible_vec"
    )]
    pub events: Vec<KvEvent>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterKvWatchEventsResponse {
    pub accepted: bool,
    pub detail: String,
    pub stored_events: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterRaftAppendRequest {
    pub state: ClusterBootstrapState,
    pub append: RaftAppendEntriesRequest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<RaftSnapshot>,
    /// Marks the empty AppendEntries heartbeat used only to establish a
    /// linearizable ReadIndex proof.
    ///
    /// Ordinary heartbeats and replication requests deliberately leave this
    /// false. Receivers may then answer a marked same-term probe from their
    /// exact resident Raft view without entering the durable mutation queues.
    #[serde(
        default,
        skip_serializing_if = "cluster_raft_read_index_probe_is_false"
    )]
    pub read_index_probe: bool,
}

fn cluster_raft_read_index_probe_is_false(value: &bool) -> bool {
    !*value
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterRaftAppendResponse {
    pub accepted: bool,
    pub detail: String,
    pub state: ClusterBootstrapState,
    pub append: RaftAppendEntriesResponse,
    /// Set only on a rejected marked ReadIndex probe that shows the follower
    /// cannot acknowledge any later probe until the leader sends it an append.
    ///
    /// A receiver that predates the field omits it and a leader that predates
    /// it ignores it; a reason this leader does not know decodes as
    /// `Unrecognized`. In each case the rejection keeps its ordinary meaning.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_index_gap: Option<ClusterRaftReadIndexProbeGap>,
}

/// Why a follower rejected a marked ReadIndex probe in a way only an append
/// from the leader resolves. The rejection is never an acknowledgement; it is
/// the observed append gap that starts the leader's targeted recovery of this
/// peer.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ClusterRaftReadIndexProbeGap {
    /// The follower's log does not hold the probe's anchor entry at the
    /// anchor's term; the rejection's match index is its log tail.
    LogGap,
    /// The follower has no resident Raft node loaded to evaluate the probe.
    /// The probe stays a fast, non-durable rejection; the leader's ordinary
    /// append loads the node.
    ResidentCold,
    /// A reason from a newer follower that this node does not know. Only
    /// decoding produces it; nothing sends it. The leader treats it as an
    /// untyped rejection: no recovery starts and the wave is re-sent on its
    /// paced fallback.
    #[serde(other)]
    Unrecognized,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterRaftVoteRequest {
    pub state: ClusterBootstrapState,
    pub vote: RaftRequestVoteRequest,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterRaftVoteResponse {
    pub accepted: bool,
    pub detail: String,
    pub state: ClusterBootstrapState,
    pub vote: RaftRequestVoteResponse,
}

pub type ClusterKvReplicationRequest = ClusterRaftAppendRequest;
pub type ClusterKvReplicationResponse = ClusterRaftAppendResponse;
pub type ClusterKvStateAuthorityVoteRequest = ClusterRaftVoteRequest;
pub type ClusterKvStateAuthorityVoteResponse = ClusterRaftVoteResponse;

pub fn cluster_kv_watch_events_noop_response(
    request: &ClusterKvWatchEventsRequest,
) -> Option<ClusterKvWatchEventsResponse> {
    request
        .events
        .is_empty()
        .then(|| ClusterKvWatchEventsResponse {
            accepted: true,
            detail: "no watch events to fan out".to_string(),
            stored_events: 0,
        })
}

pub fn handle_cluster_kv_watch_events(
    paths: &ClusterKvPaths,
    local_state: &ClusterBootstrapState,
    request: ClusterKvWatchEventsRequest,
) -> Result<ClusterKvWatchEventsResponse, KvError> {
    if let Err(limit) = validate_cluster_kv_watch_event_fanout_batch(&request.events) {
        return Ok(ClusterKvWatchEventsResponse {
            accepted: false,
            detail: limit.detail(),
            stored_events: 0,
        });
    }
    if request.state.cluster_id != local_state.cluster_id {
        return Ok(ClusterKvWatchEventsResponse {
            accepted: false,
            detail: format!(
                "peer cluster `{}` does not match local cluster `{}`",
                request.state.cluster_id, local_state.cluster_id
            ),
            stored_events: 0,
        });
    }
    if !cluster_kv_raft_node_is_voter(local_state, &request.leader_id) {
        return Ok(ClusterKvWatchEventsResponse {
            accepted: false,
            detail: format!(
                "watch event fan-out rejected from unauthorized leader {}",
                request.leader_id
            ),
            stored_events: 0,
        });
    }
    let stored_events = append_cluster_kv_watch_events_for_active_watches_for_node(
        paths,
        local_state.local_node_id.clone(),
        &request.events,
    )?;
    Ok(ClusterKvWatchEventsResponse {
        accepted: true,
        detail: "cluster kv watch events stored".to_string(),
        stored_events,
    })
}

pub fn cluster_kv_watch_events_rpc_payload(
    request: &ClusterKvWatchEventsRequest,
) -> Result<ClusterKvRpcPayload, KvError> {
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_KV_WATCH_EVENTS_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(request)?,
    })
}

pub fn decode_cluster_kv_watch_events_rpc_request_body(
    body: &[u8],
) -> Result<ClusterKvWatchEventsRequest, KvError> {
    decode_cluster_kv_rpc_body(body)
}

pub fn cluster_kv_watch_events_response_rpc_payload(
    response: &ClusterKvWatchEventsResponse,
) -> Result<ClusterKvRpcPayload, KvError> {
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_KV_WATCH_EVENTS_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(response)?,
    })
}

pub fn decode_cluster_kv_watch_events_rpc_response_body(
    body: &[u8],
) -> Result<ClusterKvWatchEventsResponse, KvError> {
    decode_cluster_kv_rpc_body(body)
}

pub fn cluster_raft_append_rpc_payload(
    request: &ClusterRaftAppendRequest,
) -> Result<ClusterKvRpcPayload, KvError> {
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_RAFT_APPEND_ROUTE,
        category: CLUSTER_RAFT_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(request)?,
    })
}

pub fn decode_cluster_raft_append_rpc_request_body(
    body: &[u8],
) -> Result<ClusterRaftAppendRequest, KvError> {
    decode_cluster_kv_rpc_body(body)
}

pub fn cluster_raft_append_response_rpc_payload(
    response: &ClusterRaftAppendResponse,
) -> Result<ClusterKvRpcPayload, KvError> {
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_RAFT_APPEND_ROUTE,
        category: CLUSTER_RAFT_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(response)?,
    })
}

pub fn decode_cluster_raft_append_rpc_response_body(
    body: &[u8],
) -> Result<ClusterRaftAppendResponse, KvError> {
    decode_cluster_kv_rpc_body(body)
}

pub fn cluster_raft_vote_rpc_payload(
    request: &ClusterRaftVoteRequest,
) -> Result<ClusterKvRpcPayload, KvError> {
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_RAFT_VOTE_ROUTE,
        category: CLUSTER_RAFT_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(request)?,
    })
}

pub fn decode_cluster_raft_vote_rpc_request_body(
    body: &[u8],
) -> Result<ClusterRaftVoteRequest, KvError> {
    decode_cluster_kv_rpc_body(body)
}

pub fn cluster_raft_vote_response_rpc_payload(
    response: &ClusterRaftVoteResponse,
) -> Result<ClusterKvRpcPayload, KvError> {
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_RAFT_VOTE_ROUTE,
        category: CLUSTER_RAFT_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(response)?,
    })
}

pub fn decode_cluster_raft_vote_rpc_response_body(
    body: &[u8],
) -> Result<ClusterRaftVoteResponse, KvError> {
    decode_cluster_kv_rpc_body(body)
}

pub fn handle_cluster_raft_rpc_payload<A, V, E, C>(
    route: &str,
    body: &[u8],
    append_handler: A,
    vote_handler: V,
    kv_error: C,
) -> Result<Option<ClusterKvRpcPayload>, E>
where
    A: FnOnce(ClusterRaftAppendRequest) -> Result<ClusterRaftAppendResponse, E>,
    V: FnOnce(ClusterRaftVoteRequest) -> Result<ClusterRaftVoteResponse, E>,
    C: Fn(KvError) -> E,
{
    match route {
        CLUSTER_RAFT_APPEND_ROUTE => {
            let request = decode_cluster_raft_append_rpc_request_body(body).map_err(&kv_error)?;
            let response = append_handler(request)?;
            Ok(Some(
                cluster_raft_append_response_rpc_payload(&response).map_err(&kv_error)?,
            ))
        }
        CLUSTER_RAFT_VOTE_ROUTE => {
            let request = decode_cluster_raft_vote_rpc_request_body(body).map_err(&kv_error)?;
            let response = vote_handler(request)?;
            Ok(Some(
                cluster_raft_vote_response_rpc_payload(&response).map_err(&kv_error)?,
            ))
        }
        _ => Ok(None),
    }
}

pub trait ClusterKvLocalReadWatchHost {
    type Error;

    /// Wraps a KV store error in the host's error type. Replaces the old
    /// `From<KvError>` bound so host error types in other crates do not need
    /// orphan-rule-breaking `From` impls.
    fn kv_error(error: KvError) -> Self::Error;

    fn range_prefix(&mut self, prefix: &str) -> Result<KvLocalRead<Vec<KvEntry>>, Self::Error>;

    fn create_watch(
        &mut self,
        request: ClusterKvWatchPrefixRequest,
    ) -> Result<ClusterKvWatchRegistration, Self::Error>;

    fn drain_watch(
        &mut self,
        request: ClusterKvWatchDrainRequest,
    ) -> Result<ClusterKvWatchDrainResponse, Self::Error>;

    fn cancel_watch(
        &mut self,
        request: ClusterKvWatchCancelRequest,
    ) -> Result<ClusterKvWatchCancelResponse, Self::Error>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClusterKvLocalReadWatchRequest {
    RangePrefix(String),
    CreateWatch(ClusterKvWatchPrefixRequest),
    DrainWatch(ClusterKvWatchDrainRequest),
    CancelWatch(ClusterKvWatchCancelRequest),
}

pub fn handle_cluster_kv_local_read_watch_request<H>(
    request: ClusterKvLocalReadWatchRequest,
    host: &mut H,
) -> Result<ClusterKvRpcPayload, H::Error>
where
    H: ClusterKvLocalReadWatchHost,
{
    match request {
        ClusterKvLocalReadWatchRequest::RangePrefix(prefix) => {
            let result = host.range_prefix(&prefix)?;
            Ok(ClusterKvRpcPayload {
                route: CLUSTER_KV_LOCAL_RANGE_PREFIX_ROUTE,
                category: CLUSTER_KV_RPC_CATEGORY,
                body: encode_cluster_kv_rpc_body(&result).map_err(H::kv_error)?,
            })
        }
        ClusterKvLocalReadWatchRequest::CreateWatch(request) => {
            let result = host.create_watch(request)?;
            Ok(ClusterKvRpcPayload {
                route: CLUSTER_KV_LOCAL_WATCH_PREFIX_ROUTE,
                category: CLUSTER_KV_RPC_CATEGORY,
                body: encode_cluster_kv_rpc_body(&result).map_err(H::kv_error)?,
            })
        }
        ClusterKvLocalReadWatchRequest::DrainWatch(request) => {
            let result = host.drain_watch(request)?;
            Ok(ClusterKvRpcPayload {
                route: CLUSTER_KV_LOCAL_WATCH_DRAIN_ROUTE,
                category: CLUSTER_KV_RPC_CATEGORY,
                body: encode_cluster_kv_rpc_body(&result).map_err(H::kv_error)?,
            })
        }
        ClusterKvLocalReadWatchRequest::CancelWatch(request) => {
            let result = host.cancel_watch(request)?;
            Ok(ClusterKvRpcPayload {
                route: CLUSTER_KV_LOCAL_WATCH_CANCEL_ROUTE,
                category: CLUSTER_KV_RPC_CATEGORY,
                body: encode_cluster_kv_rpc_body(&result).map_err(H::kv_error)?,
            })
        }
    }
}

pub fn handle_cluster_kv_local_read_watch_rpc_payload<H>(
    route: &str,
    body: &[u8],
    host: &mut H,
) -> Result<Option<ClusterKvRpcPayload>, H::Error>
where
    H: ClusterKvLocalReadWatchHost,
{
    let request = match route {
        CLUSTER_KV_LOCAL_RANGE_PREFIX_ROUTE => ClusterKvLocalReadWatchRequest::RangePrefix(
            decode_cluster_kv_rpc_body(body).map_err(H::kv_error)?,
        ),
        CLUSTER_KV_LOCAL_WATCH_PREFIX_ROUTE => ClusterKvLocalReadWatchRequest::CreateWatch(
            decode_cluster_kv_rpc_body(body).map_err(H::kv_error)?,
        ),
        CLUSTER_KV_LOCAL_WATCH_DRAIN_ROUTE => ClusterKvLocalReadWatchRequest::DrainWatch(
            decode_cluster_kv_rpc_body(body).map_err(H::kv_error)?,
        ),
        CLUSTER_KV_LOCAL_WATCH_CANCEL_ROUTE => ClusterKvLocalReadWatchRequest::CancelWatch(
            decode_cluster_kv_rpc_body(body).map_err(H::kv_error)?,
        ),
        _ => return Ok(None),
    };
    handle_cluster_kv_local_read_watch_request(request, host).map(Some)
}
