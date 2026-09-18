//! Shared DFS wire values and validation; no storage or service runtime.

use crate::dfs::*;
use serde::Deserialize;
use serde::Serialize;
use sinomo_core::node::NodeId;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsRepairAttemptRecord {
    pub intent_id: String,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub content_hash: String,
    pub missing_replica_node_id: NodeId,
    pub attempts: u32,
    pub in_flight: bool,
    pub last_attempt_unix_nanos: u64,
    pub next_retry_unix_nanos: u64,
    /// Stable request identity for an in-flight logical transfer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transfer_request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}
