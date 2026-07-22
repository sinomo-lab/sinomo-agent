use crate::prelude::*;

pub(crate) const NETWORK_FALLIBLE_VEC_RESOURCE_EXHAUSTED_PREFIX: &str =
    "network resource exhausted while allocating ";
pub(crate) const NETWORK_FALLIBLE_VEC_RESOURCE_EXHAUSTED_SEPARATOR: &str = " bytes for ";

pub(crate) fn network_fallible_vec_resource_exhausted(
    operation: &str,
    requested_bytes: usize,
) -> String {
    format!(
        "{NETWORK_FALLIBLE_VEC_RESOURCE_EXHAUSTED_PREFIX}{requested_bytes}{NETWORK_FALLIBLE_VEC_RESOURCE_EXHAUSTED_SEPARATOR}{operation}"
    )
}

fn parse_network_fallible_vec_resource_exhaustion(detail: &str) -> Option<(String, usize)> {
    let rest = detail.strip_prefix(NETWORK_FALLIBLE_VEC_RESOURCE_EXHAUSTED_PREFIX)?;
    let (requested_bytes, operation) =
        rest.split_once(NETWORK_FALLIBLE_VEC_RESOURCE_EXHAUSTED_SEPARATOR)?;
    Some((operation.to_string(), requested_bytes.parse().ok()?))
}

/// The point at which a bounded network operation ran out of time.
///
/// This is deliberately transport-oriented. Service execution deadlines and
/// domain retry state remain owned by the crate that defines the operation.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NetworkTimeoutPhase {
    Route,
    Signing,
    Connect,
    Write,
    FirstByte,
    Idle,
    Overall,
}

/// What the transport can prove about a failed request's delivery.
///
/// `PossiblyApplied` must be treated conservatively: the peer may have handled
/// the request even though its response was not observed.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NetworkDeliveryState {
    DefinitelyNotSent,
    PossiblyApplied,
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum NetworkError {
    #[error("network authorization was denied")]
    AuthDenied,
    #[error("network authorization expired")]
    AuthExpired,
    #[error("cluster has not been initialized; run `sinomo cluster init` or `sinomo cluster join`")]
    ClusterOrphan,
    #[error("unknown node: {node_id}")]
    UnknownNode { node_id: NodeId },
    #[error("unauthorized cluster node: {node_id}")]
    UnauthorizedNode { node_id: NodeId },
    #[error("frame source {source_node_id} does not match local transport node {local_node_id}")]
    FrameSourceMismatch {
        local_node_id: NodeId,
        source_node_id: NodeId,
    },
    #[error(
        "frame target {frame_target_node_id} does not match requested target {requested_target_node_id}"
    )]
    FrameTargetMismatch {
        requested_target_node_id: NodeId,
        frame_target_node_id: NodeId,
    },
    #[error("in-process mailbox for {node_id} is full: max {max_frames} frames")]
    MailboxFull { node_id: NodeId, max_frames: usize },
    #[error(
        "object frame in-flight memory unavailable: requested {frame_bytes} bytes with {in_flight_bytes} already active and {available_memory_bytes:?} bytes available"
    )]
    ObjectFrameMemoryUnavailable {
        frame_bytes: usize,
        in_flight_bytes: usize,
        available_memory_bytes: Option<usize>,
    },
    #[error("unknown object rpc route: {route}")]
    UnknownRoute { route: String },
    #[error("object rpc route {route} is saturated: max {concurrency_limit} in-flight requests")]
    ObjectRpcRouteSaturated {
        route: String,
        concurrency_limit: usize,
    },
    #[error("cluster mismatch: expected {expected}, got {actual}")]
    ClusterMismatch {
        expected: ClusterId,
        actual: ClusterId,
    },
    #[error("cluster is read-only: {detail}")]
    ClusterReadOnly { detail: String },
    #[error("authenticated cluster voter {node_id} confirmed it is not leader: {detail}")]
    VerifiedClusterNonLeader { node_id: NodeId, detail: String },
    #[error("invalid cluster command: {detail}")]
    InvalidClusterCommand { detail: String },
    #[error(
        "cluster connectivity record source mismatch: expected {expected_source_node_id}, got {record_source_node_id}"
    )]
    ClusterConnectivitySourceMismatch {
        expected_source_node_id: NodeId,
        record_source_node_id: NodeId,
    },
    #[error(
        "too many cluster connectivity records for {source_node_id}: {observed_records} records; max {max_records}"
    )]
    ClusterConnectivityRecordLimit {
        source_node_id: NodeId,
        observed_records: usize,
        max_records: usize,
    },
    #[error(
        "too many cluster connectivity snapshot records: {observed_records} records; max {max_records}"
    )]
    ClusterConnectivitySnapshotRecordLimit {
        observed_records: usize,
        max_records: usize,
    },
    #[error(
        "certificate node {certificate_node_id} does not match material node {material_node_id}"
    )]
    CertificateNodeMismatch {
        material_node_id: NodeId,
        certificate_node_id: NodeId,
    },
    #[error("certificate key does not match member key for {node_id}")]
    CertificateKeyMismatch { node_id: NodeId },
    #[error("certificate expired for {node_id}")]
    CertificateExpired { node_id: NodeId },
    #[error("certificate signature missing for {node_id}")]
    CertificateSignatureMissing { node_id: NodeId },
    #[error("network frame is {actual_bytes} bytes; max is {max_bytes}")]
    FrameTooLarge {
        actual_bytes: usize,
        max_bytes: usize,
    },
    #[error("network resource exhausted during {operation}: requested {requested_bytes} bytes")]
    ResourceExhausted {
        operation: String,
        requested_bytes: usize,
    },
    #[error("network serialization error: {detail}")]
    Serialization { detail: String },
    #[error("object RPC error at {route}: {detail}")]
    ObjectRpc { route: String, detail: String },
    #[error("cluster JSON error at {path}: {detail}")]
    Json { path: String, detail: String },
    #[error("cluster storage I/O error at {path}: {detail}")]
    StorageIo { path: String, detail: String },
    #[error(
        "network timeout during {operation} in {phase:?} after {timeout:?} ({delivery_state:?})"
    )]
    Timeout {
        operation: String,
        phase: NetworkTimeoutPhase,
        timeout: std::time::Duration,
        delivery_state: NetworkDeliveryState,
    },
    #[error(
        "network outcome is unknown during {operation} for operation id {operation_id:?}: {detail}"
    )]
    OutcomeUnknown {
        operation: String,
        operation_id: Option<String>,
        detail: String,
    },
    #[error(
        "network retries exhausted during {operation} after {attempts} attempts ({delivery_state:?}): {detail}"
    )]
    RetryExhausted {
        operation: String,
        attempts: usize,
        delivery_state: NetworkDeliveryState,
        detail: String,
    },
    #[error("network delivery failed during {operation} ({delivery_state:?}): {detail}")]
    DeliveryFailed {
        operation: String,
        delivery_state: NetworkDeliveryState,
        detail: String,
    },
    #[error("network I/O error during {operation}: {detail}")]
    Io { operation: String, detail: String },
    #[error(transparent)]
    EventStore(#[from] EventStoreError),
    #[error("cluster directory error: {detail}")]
    Directory { detail: String },
}

impl NetworkError {
    pub fn timeout(
        operation: impl Into<String>,
        phase: NetworkTimeoutPhase,
        timeout: std::time::Duration,
        delivery_state: NetworkDeliveryState,
    ) -> Self {
        Self::Timeout {
            operation: operation.into(),
            phase,
            timeout,
            delivery_state,
        }
    }

    pub fn outcome_unknown(
        operation: impl Into<String>,
        operation_id: Option<&str>,
        detail: impl Into<String>,
    ) -> Self {
        Self::OutcomeUnknown {
            operation: operation.into(),
            operation_id: operation_id.map(str::to_string),
            detail: detail.into(),
        }
    }

    pub fn delivery_failed(
        operation: impl Into<String>,
        delivery_state: NetworkDeliveryState,
        detail: impl Into<String>,
    ) -> Self {
        Self::DeliveryFailed {
            operation: operation.into(),
            delivery_state,
            detail: detail.into(),
        }
    }

    pub fn delivery_state(&self) -> Option<NetworkDeliveryState> {
        match self {
            Self::Timeout { delivery_state, .. }
            | Self::RetryExhausted { delivery_state, .. }
            | Self::DeliveryFailed { delivery_state, .. } => Some(*delivery_state),
            Self::OutcomeUnknown { .. } => Some(NetworkDeliveryState::PossiblyApplied),
            _ => None,
        }
    }

    pub fn is_timeout(&self) -> bool {
        matches!(self, Self::Timeout { .. })
    }

    /// Wraps a cluster-state/store error (KV, directory backends) for
    /// transport through the networking error type.
    pub fn directory(detail: impl std::fmt::Display) -> Self {
        Self::Directory {
            detail: detail.to_string(),
        }
    }

    pub fn object(error: sinomo_object::Error) -> Self {
        match error {
            sinomo_object::Error::ResourceExhausted {
                context,
                requested_bytes,
            } => Self::ResourceExhausted {
                operation: context.to_string(),
                requested_bytes,
            },
            sinomo_object::Error::Serde(detail) => {
                if let Some((operation, requested_bytes)) =
                    parse_network_fallible_vec_resource_exhaustion(&detail)
                {
                    Self::ResourceExhausted {
                        operation,
                        requested_bytes,
                    }
                } else {
                    Self::Serialization { detail }
                }
            }
            error => Self::Serialization {
                detail: error.to_string(),
            },
        }
    }
}
