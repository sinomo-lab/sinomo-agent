use super::*;

pub fn encode_kv_value<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, KvError> {
    sinomo_object_serde::to_bytes(value, KV_OBJECT_VERSION_ID, LengthMode::U32).map_err(|source| {
        KvError::Serialization {
            detail: source.to_string(),
        }
    })
}

pub fn decode_kv_value<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, KvError> {
    sinomo_object_serde::from_bytes(bytes).map_err(|source| KvError::Serialization {
        detail: source.to_string(),
    })
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct KvRevision(pub u64);

impl KvRevision {
    pub const ZERO: Self = Self(0);

    pub fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct KvVersion(pub u64);

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct LeaseId(pub u64);

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct WatchId(pub u64);

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KvEntry {
    pub key: String,
    #[serde(with = "serde_bytes")]
    pub value: Vec<u8>,
    pub create_revision: KvRevision,
    pub modify_revision: KvRevision,
    pub version: KvVersion,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_id: Option<LeaseId>,
}

impl KvEntry {
    pub fn decode<T: DeserializeOwned>(&self) -> Result<T, KvError> {
        decode_kv_value(&self.value)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KvLease {
    pub lease_id: LeaseId,
    pub ttl_ticks: u64,
    pub remaining_ticks: u64,
    #[serde(with = "time::serde::rfc3339")]
    pub granted_at: OffsetDateTime,
}

impl KvLease {
    pub(crate) fn new(lease_id: LeaseId, ttl_ticks: u64) -> Self {
        Self {
            lease_id,
            ttl_ticks,
            remaining_ticks: ttl_ticks,
            granted_at: OffsetDateTime::now_utc(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum KvCompare {
    Exists {
        key: String,
    },
    Missing {
        key: String,
    },
    ValueEquals {
        key: String,
        #[serde(with = "serde_bytes")]
        value: Vec<u8>,
    },
    VersionEquals {
        key: String,
        version: KvVersion,
    },
    CreateRevisionEquals {
        key: String,
        revision: KvRevision,
    },
    ModifyRevisionEquals {
        key: String,
        revision: KvRevision,
    },
    LeaseEquals {
        key: String,
        lease_id: Option<LeaseId>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum KvOp {
    Put {
        key: String,
        #[serde(with = "serde_bytes")]
        value: Vec<u8>,
        lease_id: Option<LeaseId>,
    },
    Delete {
        key: String,
    },
    DeletePrefix {
        prefix: String,
    },
    LeaseGrant {
        lease_id: LeaseId,
        ttl_ticks: u64,
    },
    LeaseKeepAlive {
        lease_id: LeaseId,
    },
    LeaseRevoke {
        lease_id: LeaseId,
    },
}

impl KvOp {
    pub fn put_serde<T: Serialize + ?Sized>(
        key: impl Into<String>,
        value: &T,
        lease_id: Option<LeaseId>,
    ) -> Result<Self, KvError> {
        Ok(Self::Put {
            key: key.into(),
            value: encode_kv_value(value)?,
            lease_id,
        })
    }
}

pub(crate) fn validate_kv_value_len(actual_bytes: usize) -> Result<(), KvError> {
    if actual_bytes > MAX_KV_VALUE_BYTES {
        return Err(KvError::ValueTooLarge {
            actual_bytes,
            max_bytes: MAX_KV_VALUE_BYTES,
        });
    }
    Ok(())
}

pub(crate) fn validate_kv_op_payload(op: &KvOp) -> Result<(), KvError> {
    if let KvOp::Put { value, .. } = op {
        validate_kv_value_len(value.len())?;
    }
    Ok(())
}

pub(crate) fn kv_op_payload_bytes(op: &KvOp) -> usize {
    match op {
        KvOp::Put { key, value, .. } => key.len().saturating_add(value.len()),
        KvOp::Delete { key } => key.len(),
        KvOp::DeletePrefix { prefix } => prefix.len(),
        KvOp::LeaseGrant { .. } | KvOp::LeaseKeepAlive { .. } | KvOp::LeaseRevoke { .. } => 0,
    }
}

pub(crate) fn kv_compare_payload_bytes(compare: &KvCompare) -> usize {
    match compare {
        KvCompare::Exists { key }
        | KvCompare::Missing { key }
        | KvCompare::VersionEquals { key, .. }
        | KvCompare::CreateRevisionEquals { key, .. }
        | KvCompare::ModifyRevisionEquals { key, .. }
        | KvCompare::LeaseEquals { key, .. } => key.len(),
        KvCompare::ValueEquals { key, value } => key.len().saturating_add(value.len()),
    }
}

pub(crate) fn validate_kv_compare_payload(compare: &KvCompare) -> Result<(), KvError> {
    if let KvCompare::ValueEquals { value, .. } = compare {
        validate_kv_value_len(value.len())?;
    }
    Ok(())
}

pub(crate) fn validate_kv_payload_total(actual_bytes: usize) -> Result<(), KvError> {
    if actual_bytes > MAX_KV_COMMAND_PAYLOAD_BYTES {
        return Err(KvError::CommandPayloadTooLarge {
            actual_bytes,
            max_bytes: MAX_KV_COMMAND_PAYLOAD_BYTES,
        });
    }
    Ok(())
}

pub(crate) fn record_kv_mutation_events(total: &mut usize, delta: usize) -> Result<(), KvError> {
    let observed_events = total.saturating_add(delta);
    if observed_events > MAX_KV_MUTATION_EVENTS_PER_COMMAND {
        return Err(KvError::MutationEventLimit {
            observed_events,
            max_events: MAX_KV_MUTATION_EVENTS_PER_COMMAND,
        });
    }
    *total = observed_events;
    Ok(())
}

pub(crate) fn kv_entry_payload_bytes(entry: &KvEntry) -> usize {
    entry.key.len().saturating_add(entry.value.len())
}

pub(crate) fn validate_kv_txn_payload(txn: &KvTxn) -> Result<(), KvError> {
    let mut payload_bytes = 0_usize;
    for compare in &txn.compares {
        validate_kv_compare_payload(compare)?;
        payload_bytes = payload_bytes.saturating_add(kv_compare_payload_bytes(compare));
    }
    for op in txn.success.iter().chain(txn.failure.iter()) {
        validate_kv_op_payload(op)?;
        payload_bytes = payload_bytes.saturating_add(kv_op_payload_bytes(op));
    }
    validate_kv_payload_total(payload_bytes)
}

pub(crate) fn validate_kv_command_payload(command: &KvCommand) -> Result<(), KvError> {
    match command {
        KvCommand::Op(op) => {
            validate_kv_op_payload(op)?;
            validate_kv_payload_total(kv_op_payload_bytes(op))
        }
        KvCommand::Txn(txn) => validate_kv_txn_payload(txn),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KvTxn {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub compares: Vec<KvCompare>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub success: Vec<KvOp>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failure: Vec<KvOp>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KvTxnResponse {
    pub succeeded: bool,
    pub revision: KvRevision,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub responses: Vec<KvOpResponse>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum KvOpResponse {
    Put {
        key: String,
        revision: KvRevision,
    },
    Delete {
        key: String,
        deleted: bool,
    },
    DeletePrefix {
        prefix: String,
        deleted_count: usize,
    },
    LeaseGrant {
        lease_id: LeaseId,
    },
    LeaseKeepAlive {
        lease_id: LeaseId,
    },
    LeaseRevoke {
        lease_id: LeaseId,
        deleted_count: usize,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum KvEventKind {
    Put {
        #[serde(with = "serde_bytes")]
        value: Vec<u8>,
    },
    Delete,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KvEvent {
    pub revision: KvRevision,
    pub key: String,
    pub kind: KvEventKind,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KvWatch {
    pub watch_id: WatchId,
    pub prefix: String,
    pub start_revision: KvRevision,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KvSnapshot {
    pub revision: KvRevision,
    pub compacted_revision: KvRevision,
    pub entries: Vec<KvEntry>,
    pub leases: Vec<KvLease>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<KvEvent>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum KvCommand {
    Op(KvOp),
    Txn(KvTxn),
}

impl Default for KvRevision {
    fn default() -> Self {
        Self::ZERO
    }
}

impl KvOpResponse {
    pub(crate) fn revision(&self) -> KvRevision {
        match self {
            Self::Put { revision, .. } => *revision,
            Self::Delete { .. }
            | Self::DeletePrefix { .. }
            | Self::LeaseGrant { .. }
            | Self::LeaseKeepAlive { .. }
            | Self::LeaseRevoke { .. } => KvRevision::ZERO,
        }
    }
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum KvError {
    #[error("requested revision {requested:?} has been compacted at {compacted:?}")]
    Compacted {
        requested: KvRevision,
        compacted: KvRevision,
    },
    #[error("requested revision {requested:?} is ahead of current revision {current:?}")]
    RevisionAhead {
        requested: KvRevision,
        current: KvRevision,
    },
    #[error("unknown lease: {lease_id:?}")]
    UnknownLease { lease_id: LeaseId },
    #[error("invalid lease ttl: {ttl_ticks}")]
    InvalidLeaseTtl { ttl_ticks: u64 },
    #[error("too many active leases: max {max}")]
    LeaseLimit { max: usize },
    #[error(
        "transaction is too large: {compares} compares, {success_ops} success ops, {failure_ops} failure ops; max is {max_compares} compares and {max_ops_per_branch} ops per branch"
    )]
    TxnLimit {
        compares: usize,
        success_ops: usize,
        failure_ops: usize,
        max_compares: usize,
        max_ops_per_branch: usize,
    },
    #[error("unknown watch: {watch_id:?}")]
    UnknownWatch { watch_id: WatchId },
    #[error("too many active watches: max {max}")]
    WatchLimit { max: usize },
    #[error("watch id space exhausted")]
    WatchIdExhausted,
    #[error("range for prefix {prefix} exceeded max {max_entries} entries")]
    RangeLimit { prefix: String, max_entries: usize },
    #[error(
        "range for prefix {prefix} is at least {observed_bytes} payload bytes; max is {max_bytes}"
    )]
    RangePayloadLimit {
        prefix: String,
        observed_bytes: usize,
        max_bytes: usize,
    },
    #[error(
        "delete-prefix for prefix {prefix} matched at least {observed_keys} keys; max is {max_keys}"
    )]
    DeletePrefixLimit {
        prefix: String,
        observed_keys: usize,
        max_keys: usize,
    },
    #[error(
        "kv command would emit at least {observed_events} mutation events; max is {max_events}"
    )]
    MutationEventLimit {
        observed_events: usize,
        max_events: usize,
    },
    #[error("lease {lease_id:?} would have {observed_keys} attached keys; max is {max_keys}")]
    LeaseKeyLimit {
        lease_id: LeaseId,
        observed_keys: usize,
        max_keys: usize,
    },
    #[error("kv value is {actual_bytes} bytes; max is {max_bytes}")]
    ValueTooLarge {
        actual_bytes: usize,
        max_bytes: usize,
    },
    #[error("kv command payload is {actual_bytes} bytes; max is {max_bytes}")]
    CommandPayloadTooLarge {
        actual_bytes: usize,
        max_bytes: usize,
    },
    #[error("serialization error: {detail}")]
    Serialization { detail: String },
    #[error("filesystem error at {path}: {detail}")]
    Io { path: String, detail: String },
    #[error("raft node is busy at {path}")]
    Busy { path: String },
    #[error("raft cluster requires at least one voter")]
    NoVoters,
    #[error("unknown replicated node: {node_id}")]
    UnknownNode { node_id: NodeId },
    #[error("duplicate replicated node: {node_id}")]
    DuplicateNode { node_id: NodeId },
    #[error("leader is offline: {leader_id}")]
    LeaderOffline { leader_id: NodeId },
    #[error("quorum unavailable: {online_voters} voters online, {quorum} required")]
    QuorumUnavailable { online_voters: usize, quorum: usize },
    #[error("raft log gap: expected index {expected}, got {actual}")]
    LogGap { expected: u64, actual: u64 },
    #[error("conflicting raft log entry at index {index}")]
    ConflictingLogEntry { index: u64 },
    #[error("missing committed raft entry at index {index}")]
    MissingCommittedEntry { index: u64 },
    #[error(
        "cannot claim raft leadership for term {term}: local term already advanced to {current_term}"
    )]
    StaleLeadershipTerm { term: u64, current_term: u64 },
    #[error("invalid raft config transition: {detail}")]
    InvalidRaftConfigTransition { detail: String },
}
