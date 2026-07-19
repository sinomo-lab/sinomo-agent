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
    Ok(ClusterKvRpcPayload {
        route: CLUSTER_KV_TXN_ROUTE,
        category: CLUSTER_KV_RPC_CATEGORY,
        body: encode_cluster_kv_rpc_body(response)?,
    })
}

pub fn decode_cluster_kv_txn_rpc_body(body: &[u8]) -> Result<ClusterKvTxnCommitResult, KvError> {
    decode_cluster_kv_rpc_body(body)
}

/// Marker request for an authoritative read. The receiver must generate a
/// fresh barrier request id for every invocation; callers cannot supply or
/// reuse one and thereby replay stale authority.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterKvAuthoritativeSnapshotRequest;

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

fn validate_cluster_kv_authoritative_watch_start_response(
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

fn validate_cluster_kv_request_outcome_read(
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
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterRaftAppendResponse {
    pub accepted: bool,
    pub detail: String,
    pub state: ClusterBootstrapState,
    pub append: RaftAppendEntriesResponse,
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

pub fn handle_cluster_kv_local_read_watch_rpc_payload<H>(
    route: &str,
    body: &[u8],
    host: &mut H,
) -> Result<Option<ClusterKvRpcPayload>, H::Error>
where
    H: ClusterKvLocalReadWatchHost,
{
    let payload = match route {
        CLUSTER_KV_LOCAL_RANGE_PREFIX_ROUTE => {
            let prefix: String = decode_cluster_kv_rpc_body(body).map_err(H::kv_error)?;
            let result = host.range_prefix(&prefix)?;
            ClusterKvRpcPayload {
                route: CLUSTER_KV_LOCAL_RANGE_PREFIX_ROUTE,
                category: CLUSTER_KV_RPC_CATEGORY,
                body: encode_cluster_kv_rpc_body(&result).map_err(H::kv_error)?,
            }
        }
        CLUSTER_KV_LOCAL_WATCH_PREFIX_ROUTE => {
            let request: ClusterKvWatchPrefixRequest =
                decode_cluster_kv_rpc_body(body).map_err(H::kv_error)?;
            let result = host.create_watch(request)?;
            ClusterKvRpcPayload {
                route: CLUSTER_KV_LOCAL_WATCH_PREFIX_ROUTE,
                category: CLUSTER_KV_RPC_CATEGORY,
                body: encode_cluster_kv_rpc_body(&result).map_err(H::kv_error)?,
            }
        }
        CLUSTER_KV_LOCAL_WATCH_DRAIN_ROUTE => {
            let request: ClusterKvWatchDrainRequest =
                decode_cluster_kv_rpc_body(body).map_err(H::kv_error)?;
            let result = host.drain_watch(request)?;
            ClusterKvRpcPayload {
                route: CLUSTER_KV_LOCAL_WATCH_DRAIN_ROUTE,
                category: CLUSTER_KV_RPC_CATEGORY,
                body: encode_cluster_kv_rpc_body(&result).map_err(H::kv_error)?,
            }
        }
        CLUSTER_KV_LOCAL_WATCH_CANCEL_ROUTE => {
            let request: ClusterKvWatchCancelRequest =
                decode_cluster_kv_rpc_body(body).map_err(H::kv_error)?;
            let result = host.cancel_watch(request)?;
            ClusterKvRpcPayload {
                route: CLUSTER_KV_LOCAL_WATCH_CANCEL_ROUTE,
                category: CLUSTER_KV_RPC_CATEGORY,
                body: encode_cluster_kv_rpc_body(&result).map_err(H::kv_error)?,
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(payload))
}
