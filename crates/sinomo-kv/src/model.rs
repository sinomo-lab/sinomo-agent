use crate::internal::*;

const KV_FALLIBLE_VEC_RESOURCE_EXHAUSTED_PREFIX: &str = "resource exhausted while allocating ";
const KV_FALLIBLE_VEC_RESOURCE_EXHAUSTED_SEPARATOR: &str = " bytes for ";

pub fn encode_kv_value<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, KvError> {
    sinomo_object_serde::to_bytes(value, KV_OBJECT_VERSION_ID, LengthMode::U32)
        .map_err(kv_object_error)
}

pub fn decode_kv_value<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, KvError> {
    sinomo_object_serde::from_bytes(bytes).map_err(kv_object_error)
}

pub(crate) fn kv_object_error(source: sinomo_object::Error) -> KvError {
    match source {
        sinomo_object::Error::ResourceExhausted {
            context,
            requested_bytes,
        } => KvError::ResourceExhausted {
            operation: context.to_string(),
            requested_bytes,
        },
        sinomo_object::Error::Serde(detail) => {
            if let Some((operation, requested_bytes)) =
                parse_kv_fallible_vec_resource_exhaustion(&detail)
            {
                KvError::ResourceExhausted {
                    operation,
                    requested_bytes,
                }
            } else {
                KvError::Serialization { detail }
            }
        }
        source => KvError::Serialization {
            detail: source.to_string(),
        },
    }
}

fn parse_kv_fallible_vec_resource_exhaustion(detail: &str) -> Option<(String, usize)> {
    let rest = detail.strip_prefix(KV_FALLIBLE_VEC_RESOURCE_EXHAUSTED_PREFIX)?;
    let (requested_bytes, operation) =
        rest.split_once(KV_FALLIBLE_VEC_RESOURCE_EXHAUSTED_SEPARATOR)?;
    Some((operation.to_string(), requested_bytes.parse().ok()?))
}

fn kv_fallible_vec_requested_bytes<T>(items: usize) -> usize {
    items
        .checked_mul(std::mem::size_of::<T>().max(1))
        .unwrap_or(usize::MAX)
}

fn kv_fallible_vec_resource_exhausted(context: &str, requested_bytes: usize) -> String {
    format!(
        "{KV_FALLIBLE_VEC_RESOURCE_EXHAUSTED_PREFIX}{requested_bytes}{KV_FALLIBLE_VEC_RESOURCE_EXHAUSTED_SEPARATOR}{context}"
    )
}

fn try_reserve_kv_vec<T, E>(vec: &mut Vec<T>, additional: usize, context: &str) -> Result<(), E>
where
    E: serde::de::Error,
{
    let requested_items = vec.len().checked_add(additional).unwrap_or(usize::MAX);
    let requested_bytes = kv_fallible_vec_requested_bytes::<T>(requested_items);
    vec.try_reserve(additional)
        .map_err(|_| E::custom(kv_fallible_vec_resource_exhausted(context, requested_bytes)))
}

pub(crate) fn try_reserve_kv_items<T>(
    vec: &mut Vec<T>,
    additional: usize,
    operation: &str,
) -> Result<(), KvError> {
    let requested_items = vec.len().checked_add(additional).unwrap_or(usize::MAX);
    let requested_bytes = kv_fallible_vec_requested_bytes::<T>(requested_items);
    vec.try_reserve(additional)
        .map_err(|_| KvError::ResourceExhausted {
            operation: operation.to_string(),
            requested_bytes,
        })
}

pub(crate) fn try_clone_kv_bytes(bytes: &[u8], operation: &str) -> Result<Vec<u8>, KvError> {
    let mut cloned = Vec::new();
    cloned
        .try_reserve(bytes.len())
        .map_err(|_| KvError::ResourceExhausted {
            operation: operation.to_string(),
            requested_bytes: bytes.len(),
        })?;
    cloned.extend_from_slice(bytes);
    Ok(cloned)
}

pub(crate) fn try_clone_kv_string(value: &str, operation: &str) -> Result<String, KvError> {
    let mut cloned = String::new();
    cloned
        .try_reserve(value.len())
        .map_err(|_| KvError::ResourceExhausted {
            operation: operation.to_string(),
            requested_bytes: value.len(),
        })?;
    cloned.push_str(value);
    Ok(cloned)
}

pub(crate) fn try_clone_kv_node_id(node_id: &NodeId, operation: &str) -> Result<NodeId, KvError> {
    let value = try_clone_kv_string(node_id.as_str(), operation)?;
    NodeId::new(value).map_err(|error| KvError::Serialization {
        detail: error.to_string(),
    })
}

pub(crate) fn try_clone_kv_event(event: &KvEvent, operation: &str) -> Result<KvEvent, KvError> {
    Ok(KvEvent {
        revision: event.revision,
        key: try_clone_kv_string(&event.key, operation)?,
        kind: match &event.kind {
            KvEventKind::Put { value } => KvEventKind::Put {
                value: try_clone_kv_bytes(value, operation)?,
            },
            KvEventKind::Delete => KvEventKind::Delete,
        },
    })
}

pub(crate) fn try_clone_kv_entry(entry: &KvEntry, operation: &str) -> Result<KvEntry, KvError> {
    Ok(KvEntry {
        key: try_clone_kv_string(&entry.key, operation)?,
        value: try_clone_kv_bytes(&entry.value, operation)?,
        create_revision: entry.create_revision,
        modify_revision: entry.modify_revision,
        version: entry.version,
        lease_id: entry.lease_id,
    })
}

pub(crate) fn try_clone_kv_compare(
    compare: &KvCompare,
    operation: &str,
) -> Result<KvCompare, KvError> {
    Ok(match compare {
        KvCompare::Exists { key } => KvCompare::Exists {
            key: try_clone_kv_string(key, operation)?,
        },
        KvCompare::Missing { key } => KvCompare::Missing {
            key: try_clone_kv_string(key, operation)?,
        },
        KvCompare::ValueEquals { key, value } => KvCompare::ValueEquals {
            key: try_clone_kv_string(key, operation)?,
            value: try_clone_kv_bytes(value, operation)?,
        },
        KvCompare::VersionEquals { key, version } => KvCompare::VersionEquals {
            key: try_clone_kv_string(key, operation)?,
            version: *version,
        },
        KvCompare::CreateRevisionEquals { key, revision } => KvCompare::CreateRevisionEquals {
            key: try_clone_kv_string(key, operation)?,
            revision: *revision,
        },
        KvCompare::ModifyRevisionEquals { key, revision } => KvCompare::ModifyRevisionEquals {
            key: try_clone_kv_string(key, operation)?,
            revision: *revision,
        },
        KvCompare::LeaseEquals { key, lease_id } => KvCompare::LeaseEquals {
            key: try_clone_kv_string(key, operation)?,
            lease_id: *lease_id,
        },
    })
}

pub(crate) fn try_clone_kv_op(op: &KvOp, operation: &str) -> Result<KvOp, KvError> {
    Ok(match op {
        KvOp::Put {
            key,
            value,
            lease_id,
        } => KvOp::Put {
            key: try_clone_kv_string(key, operation)?,
            value: try_clone_kv_bytes(value, operation)?,
            lease_id: *lease_id,
        },
        KvOp::Delete { key } => KvOp::Delete {
            key: try_clone_kv_string(key, operation)?,
        },
        KvOp::DeletePrefix { prefix } => KvOp::DeletePrefix {
            prefix: try_clone_kv_string(prefix, operation)?,
        },
        KvOp::LeaseGrant {
            lease_id,
            ttl_ticks,
        } => KvOp::LeaseGrant {
            lease_id: *lease_id,
            ttl_ticks: *ttl_ticks,
        },
        KvOp::LeaseKeepAlive { lease_id } => KvOp::LeaseKeepAlive {
            lease_id: *lease_id,
        },
        KvOp::LeaseRevoke { lease_id } => KvOp::LeaseRevoke {
            lease_id: *lease_id,
        },
    })
}

pub(crate) fn try_clone_kv_txn(txn: &KvTxn, operation: &str) -> Result<KvTxn, KvError> {
    let mut compares = Vec::new();
    try_reserve_kv_items(&mut compares, txn.compares.len(), operation)?;
    for compare in &txn.compares {
        compares.push(try_clone_kv_compare(compare, operation)?);
    }

    let mut success = Vec::new();
    try_reserve_kv_items(&mut success, txn.success.len(), operation)?;
    for op in &txn.success {
        success.push(try_clone_kv_op(op, operation)?);
    }

    let mut failure = Vec::new();
    try_reserve_kv_items(&mut failure, txn.failure.len(), operation)?;
    for op in &txn.failure {
        failure.push(try_clone_kv_op(op, operation)?);
    }

    Ok(KvTxn {
        compares,
        success,
        failure,
    })
}

pub(crate) fn try_clone_kv_command(
    command: &KvCommand,
    operation: &str,
) -> Result<KvCommand, KvError> {
    Ok(match command {
        KvCommand::Op(op) => KvCommand::Op(try_clone_kv_op(op, operation)?),
        KvCommand::Txn(txn) => KvCommand::Txn(try_clone_kv_txn(txn, operation)?),
        KvCommand::Barrier => KvCommand::Barrier,
    })
}

pub(crate) fn try_clone_kv_op_response(
    response: &KvOpResponse,
    operation: &str,
) -> Result<KvOpResponse, KvError> {
    Ok(match response {
        KvOpResponse::Put { key, revision } => KvOpResponse::Put {
            key: try_clone_kv_string(key, operation)?,
            revision: *revision,
        },
        KvOpResponse::Delete { key, deleted } => KvOpResponse::Delete {
            key: try_clone_kv_string(key, operation)?,
            deleted: *deleted,
        },
        KvOpResponse::DeletePrefix {
            prefix,
            deleted_count,
        } => KvOpResponse::DeletePrefix {
            prefix: try_clone_kv_string(prefix, operation)?,
            deleted_count: *deleted_count,
        },
        KvOpResponse::LeaseGrant { lease_id } => KvOpResponse::LeaseGrant {
            lease_id: *lease_id,
        },
        KvOpResponse::LeaseKeepAlive { lease_id } => KvOpResponse::LeaseKeepAlive {
            lease_id: *lease_id,
        },
        KvOpResponse::LeaseRevoke {
            lease_id,
            deleted_count,
        } => KvOpResponse::LeaseRevoke {
            lease_id: *lease_id,
            deleted_count: *deleted_count,
        },
    })
}

pub(crate) fn try_clone_kv_txn_response(
    response: &KvTxnResponse,
    operation: &str,
) -> Result<KvTxnResponse, KvError> {
    let mut responses = Vec::new();
    try_reserve_kv_items(&mut responses, response.responses.len(), operation)?;
    for op_response in &response.responses {
        responses.push(try_clone_kv_op_response(op_response, operation)?);
    }
    Ok(KvTxnResponse {
        succeeded: response.succeeded,
        revision: response.revision,
        responses,
    })
}

pub(crate) fn deserialize_fallible_vec<'de, D, T>(
    deserializer: D,
) -> std::result::Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    deserializer.deserialize_seq(FallibleVecVisitor {
        context: "kv vector",
        marker: std::marker::PhantomData,
    })
}

struct FallibleVecVisitor<T> {
    context: &'static str,
    marker: std::marker::PhantomData<T>,
}

impl<'de, T> serde::de::Visitor<'de> for FallibleVecVisitor<T>
where
    T: Deserialize<'de>,
{
    type Value = Vec<T>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a sequence")
    }

    fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut values = Vec::new();
        if let Some(size_hint) = seq.size_hint() {
            try_reserve_kv_vec(&mut values, size_hint, self.context)?;
        }
        while let Some(value) = seq.next_element()? {
            try_reserve_kv_vec(&mut values, 1, self.context)?;
            values.push(value);
        }
        Ok(values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct HugeSizeHintDeserializer;

    impl<'de> serde::Deserializer<'de> for HugeSizeHintDeserializer {
        type Error = sinomo_object::Error;

        fn deserialize_any<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            visitor.visit_seq(HugeSizeHintSeqAccess)
        }

        fn deserialize_seq<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
        where
            V: serde::de::Visitor<'de>,
        {
            visitor.visit_seq(HugeSizeHintSeqAccess)
        }

        serde::forward_to_deserialize_any! {
            bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string bytes
            byte_buf option unit unit_struct newtype_struct tuple tuple_struct map
            struct enum identifier ignored_any
        }
    }

    struct HugeSizeHintSeqAccess;

    impl<'de> serde::de::SeqAccess<'de> for HugeSizeHintSeqAccess {
        type Error = sinomo_object::Error;

        fn next_element_seed<T>(
            &mut self,
            _seed: T,
        ) -> std::result::Result<Option<T::Value>, Self::Error>
        where
            T: serde::de::DeserializeSeed<'de>,
        {
            Ok(None)
        }

        fn size_hint(&self) -> Option<usize> {
            Some(usize::MAX)
        }
    }

    #[test]
    fn object_resource_exhaustion_maps_to_kv_resource_exhaustion() {
        let error = kv_object_error(sinomo_object::Error::ResourceExhausted {
            context: "decode kv snapshot",
            requested_bytes: 4096,
        });

        assert_eq!(
            error,
            KvError::ResourceExhausted {
                operation: "decode kv snapshot".to_string(),
                requested_bytes: 4096,
            }
        );
    }

    #[test]
    fn fallible_vec_deserializer_rejects_huge_size_hint_before_collecting() {
        let error = deserialize_fallible_vec::<_, u8>(HugeSizeHintDeserializer)
            .expect_err("huge size hint should fail fallibly");

        assert_eq!(
            kv_object_error(error),
            KvError::ResourceExhausted {
                operation: "kv vector".to_string(),
                requested_bytes: usize::MAX,
            }
        );
    }

    #[test]
    fn kv_fallible_vec_resource_exhaustion_maps_to_kv_resource_exhaustion() {
        let error = kv_object_error(sinomo_object::Error::Serde(
            kv_fallible_vec_resource_exhausted("kv vector", 8192),
        ));

        assert_eq!(
            error,
            KvError::ResourceExhausted {
                operation: "kv vector".to_string(),
                requested_bytes: 8192,
            }
        );
    }
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

/// Stable identity for a client-visible KV request.
///
/// Callers must reuse the same id only when retrying the exact same command.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct KvRequestId(pub Id);

impl KvRequestId {
    pub fn generate() -> Self {
        Self(Id::generate())
    }

    pub fn derived(namespace: &str, value: impl AsRef<[u8]>) -> Self {
        Self(Id::derived(namespace, value))
    }

    pub fn as_id(&self) -> &Id {
        &self.0
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl std::fmt::Display for KvRequestId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct KvCommandDigest(pub [u8; 32]);

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

pub fn push_bounded_txn_success_op(ops: &mut Vec<KvOp>, op: KvOp) -> Result<(), KvError> {
    if ops.len() >= MAX_KV_TXN_OPS_PER_BRANCH {
        return Err(KvError::TxnLimit {
            compares: 0,
            success_ops: ops.len().saturating_add(1),
            failure_ops: 0,
            max_compares: MAX_KV_TXN_COMPARES,
            max_ops_per_branch: MAX_KV_TXN_OPS_PER_BRANCH,
        });
    }
    let requested_bytes = ops
        .len()
        .checked_add(1)
        .and_then(|items| items.checked_mul(std::mem::size_of::<KvOp>().max(1)))
        .unwrap_or(usize::MAX);
    ops.try_reserve(1).map_err(|_| KvError::ResourceExhausted {
        operation: "kv transaction success ops".to_string(),
        requested_bytes,
    })?;
    ops.push(op);
    Ok(())
}

pub(crate) fn validate_kv_command_payload(command: &KvCommand) -> Result<(), KvError> {
    match command {
        KvCommand::Op(op) => {
            validate_kv_op_payload(op)?;
            validate_kv_payload_total(kv_op_payload_bytes(op))
        }
        KvCommand::Txn(txn) => validate_kv_txn_payload(txn),
        KvCommand::Barrier => Ok(()),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KvTxn {
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_fallible_vec"
    )]
    pub compares: Vec<KvCompare>,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_fallible_vec"
    )]
    pub success: Vec<KvOp>,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_fallible_vec"
    )]
    pub failure: Vec<KvOp>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KvTxnRequest {
    pub request_id: KvRequestId,
    pub txn: KvTxn,
}

impl KvTxnRequest {
    pub fn new(request_id: KvRequestId, txn: KvTxn) -> Self {
        Self { request_id, txn }
    }

    pub fn generated(txn: KvTxn) -> Self {
        Self::new(KvRequestId::generate(), txn)
    }

    pub fn into_command_request(self) -> KvCommandRequest {
        KvCommandRequest::new(self.request_id, KvCommand::Txn(self.txn))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KvTxnResponse {
    pub succeeded: bool,
    pub revision: KvRevision,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_fallible_vec"
    )]
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

/// Bounded event-history projection used by local watches and committed-delta
/// fanout. Unlike [`KvSnapshot`], this view does not clone the complete
/// keyspace, leases, or request ledger.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterKvEventReadView {
    pub compacted_revision: KvRevision,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_fallible_vec"
    )]
    pub events: Vec<KvEvent>,
}

/// Final state of one key touched by a committed command. Delete events carry
/// `None`; puts carry the complete versioned entry so incremental observers do
/// not need a full snapshot to recover metadata absent from [`KvEvent`].
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterKvCommittedKeyState {
    pub key: String,
    pub entry: Option<KvEntry>,
}

/// Bounded post-commit update used by process-local observers. Its size is
/// proportional to the keys changed by one command. Cross-process consumers
/// still rely on the shared full snapshot for critical/unknown keyspaces; this
/// value is not a durable projection journal.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterKvCommittedDelta {
    pub revision: KvRevision,
    pub compacted_revision: KvRevision,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_fallible_vec"
    )]
    pub events: Vec<KvEvent>,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_fallible_vec"
    )]
    pub final_key_states: Vec<ClusterKvCommittedKeyState>,
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
    #[serde(deserialize_with = "deserialize_fallible_vec")]
    pub entries: Vec<KvEntry>,
    #[serde(deserialize_with = "deserialize_fallible_vec")]
    pub leases: Vec<KvLease>,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_fallible_vec"
    )]
    pub events: Vec<KvEvent>,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_fallible_vec"
    )]
    pub committed_requests: Vec<KvCommittedRequestRecord>,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_fallible_vec"
    )]
    pub expired_requests: Vec<KvRequestId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum KvCommand {
    Op(KvOp),
    Txn(KvTxn),
    /// A current-term no-op used to establish an authoritative read barrier.
    Barrier,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KvCommandRequest {
    pub request_id: KvRequestId,
    pub command: KvCommand,
}

impl KvCommandRequest {
    pub fn new(request_id: KvRequestId, command: KvCommand) -> Self {
        Self {
            request_id,
            command,
        }
    }

    pub fn generated(command: KvCommand) -> Self {
        Self::new(KvRequestId::generate(), command)
    }

    pub fn digest(&self) -> Result<KvCommandDigest, KvError> {
        kv_command_digest(&self.command)
    }
}

pub fn kv_command_digest(command: &KvCommand) -> Result<KvCommandDigest, KvError> {
    let bytes = encode_kv_value(command)?;
    Ok(KvCommandDigest(*blake3::hash(&bytes).as_bytes()))
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KvCommittedRequestRecord {
    pub request_id: KvRequestId,
    pub command_digest: KvCommandDigest,
    pub term: u64,
    pub log_index: u64,
    pub revision: KvRevision,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub txn_response: Option<KvTxnResponse>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum KvRequestAttemptState {
    Submitted,
    RejectedBeforeCommit { reason: String },
    Unknown { detail: String },
    Expired,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KvRequestAttemptRecord {
    pub request_id: KvRequestId,
    pub command_digest: KvCommandDigest,
    pub state: KvRequestAttemptState,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum KvRequestOutcome {
    Committed(KvCommittedRequestRecord),
    RejectedBeforeCommit { reason: String },
    Unknown { detail: String },
    Expired,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum KvAuthorityBasis {
    CurrentTermQuorum {
        voter_acks: usize,
        required_quorum: usize,
    },
    ExactCommittedRequest,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KvAuthorityProof {
    pub term: u64,
    pub commit_index: u64,
    pub revision: KvRevision,
    pub basis: KvAuthorityBasis,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KvLocalRead<T> {
    pub value: T,
    pub revision: KvRevision,
    pub observed_term: u64,
    pub applied_index: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub known_leader: Option<NodeId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KvAuthoritativeRead<T> {
    pub value: T,
    pub authority: KvAuthorityProof,
}

/// Outcome of one request after a fresh current-term quorum confirmation.
///
/// Committed history comes from the resident committed-request and expiry
/// indexes after ReadIndex (or a first-current-term barrier). Rejected and
/// unknown states may additionally reflect the querying node's durable attempt
/// ledger, but are returned only after committed history has been ruled out by
/// the attached authority proof.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterKvRequestOutcomeRead {
    pub authority: KvAuthorityProof,
    pub outcome: KvRequestOutcome,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum KvReadAuthority {
    Local,
    Authoritative,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum KvFreshness {
    Unknown,
    CurrentTermQuorumConfirmed,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum KvAuthorityFailureReason {
    LocalNodeNotVoter,
    NoQuorum,
    Timeout,
    HigherTermObserved,
    VoterRejected,
    TransportFailure,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum KvRecoveryAction {
    RetryWhenQuorumIsReachable,
    ContactKnownLeader,
    RefreshClusterState,
    RunClusterDoctor,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KvAuthorityDiagnostics {
    pub local_node_id: NodeId,
    pub local_role: ClusterMemberRole,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub known_leader: Option<NodeId>,
    pub observed_term: u64,
    pub reachable_voters: usize,
    pub required_quorum: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_successful_append_at: Option<OffsetDateTime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_snapshot_refresh_at: Option<OffsetDateTime>,
    pub authority: KvReadAuthority,
    pub revision: KvRevision,
    pub freshness: KvFreshness,
    pub suggested_recovery: KvRecoveryAction,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct KvAuthorityFailure {
    pub reason: KvAuthorityFailureReason,
    pub detail: String,
    pub diagnostics: KvAuthorityDiagnostics,
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
    #[error("imported KV snapshot conflicts with materialized revision {revision:?}")]
    ImportedSnapshotRevisionConflict { revision: KvRevision },
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
    #[error("kv snapshot contains {observed_events} retained events; max is {max_events}")]
    SnapshotEventLimit {
        observed_events: usize,
        max_events: usize,
    },
    #[error(
        "kv snapshot contains {observed_records} committed request records; max is {max_records}"
    )]
    RequestRecordLimit {
        observed_records: usize,
        max_records: usize,
    },
    #[error(
        "kv snapshot committed request records use {observed_bytes} encoded bytes; max is {max_bytes}"
    )]
    RequestRecordBytesLimit {
        observed_bytes: usize,
        max_bytes: usize,
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
    #[error("kv resource exhausted during {operation}: requested {requested_bytes} bytes")]
    ResourceExhausted {
        operation: String,
        requested_bytes: usize,
    },
    #[error("serialization error: {detail}")]
    Serialization { detail: String },
    #[error("filesystem error at {path}: {detail}")]
    Io { path: String, detail: String },
    #[error("raft node is busy at {path}")]
    Busy { path: String },
    #[error("durable raft node session is poisoned; drop and reopen the node")]
    RaftSessionPoisoned,
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
    #[error("KV request id {request_id} was reused for a different command")]
    RequestIdConflict { request_id: KvRequestId },
    #[error("KV request id {request_id} expired from idempotency retention")]
    RequestExpired { request_id: KvRequestId },
    #[error(
        "KV request {request_id} is durably committed at term {term}, log index {log_index}, revision {revision:?}, but its complete read view is unavailable: {detail}"
    )]
    CommittedViewUnavailable {
        request_id: KvRequestId,
        term: u64,
        log_index: u64,
        revision: KvRevision,
        detail: String,
    },
    #[error("cluster KV authority unavailable: {detail}")]
    AuthorityUnavailable { detail: String },
}
