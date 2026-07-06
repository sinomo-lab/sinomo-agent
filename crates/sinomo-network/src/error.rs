use crate::peer::RouteDeliveryId;
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
    #[error("unknown delivery: {delivery_id:?}")]
    UnknownDelivery { delivery_id: RouteDeliveryId },
    #[error("delivery ledger is full: max {max_records} records")]
    DeliveryLedgerFull { max_records: usize },
    #[error(
        "network service command queue is full during {operation}: max {max_commands} commands"
    )]
    NetworkServiceCommandQueueFull {
        operation: String,
        max_commands: usize,
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
    #[error(
        "delivery {delivery_id:?} acked by {acking_node_id}, expected {expected_target_node_id}"
    )]
    AckTargetMismatch {
        delivery_id: RouteDeliveryId,
        expected_target_node_id: NodeId,
        acking_node_id: NodeId,
    },
    #[error("delivery {delivery_id:?} acked event {acking_event_id}, expected {expected_event_id}")]
    AckEventMismatch {
        delivery_id: RouteDeliveryId,
        expected_event_id: EventId,
        acking_event_id: EventId,
    },
    #[error("cluster mismatch: expected {expected}, got {actual}")]
    ClusterMismatch {
        expected: ClusterId,
        actual: ClusterId,
    },
    #[error("cluster is read-only: {detail}")]
    ClusterReadOnly { detail: String },
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
    #[error("network I/O error during {operation}: {detail}")]
    Io { operation: String, detail: String },
    #[error(transparent)]
    EventStore(#[from] EventStoreError),
    #[error("cluster directory error: {detail}")]
    Directory { detail: String },
}

impl NetworkError {
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
