//! Lightweight daemon wire API contracts.
//!
//! This crate intentionally contains serializable request/response types and
//! stable route constants, not daemon implementation logic. Runtime clients that
//! only need to talk to a daemon can depend on this crate instead of linking the
//! full daemon implementation.

mod client;
mod error;
mod idle_stats;
mod paths;
mod status;

pub use client::{
    DAEMON_API_CLUSTER_CONTROL_TIMEOUT, DAEMON_API_DEFAULT_TIMEOUT, DAEMON_API_PROBE_TIMEOUT,
    DaemonApiClient, RemoteConfigClient, send_daemon_rpc, send_daemon_rpc_async,
    send_daemon_rpc_on_runtime, send_daemon_rpc_raw, send_daemon_rpc_raw_with_timeouts,
    send_daemon_rpc_stream, send_daemon_rpc_stream_async, send_daemon_rpc_with_timeouts,
};
pub use error::DaemonApiError;
pub use idle_stats::IdleStatsSnapshot;
pub use paths::{DaemonPaths, daemon_paths_from_data_dir, shadow_daemon_paths_from_data_dir};
pub use status::{
    DaemonBuildInfo, DaemonClusterStatus, DaemonProbeStatus, DaemonRunOptions, DaemonStatus,
    SINOMO_DAEMON_DEFAULT_HOST, SINOMO_DAEMON_DEFAULT_PORT, SINOMO_SHADOW_DAEMON_DEFAULT_PORT,
    cluster_node_status_from_daemon_status, daemon_bind_address, daemon_build_info,
    daemon_connect_address, daemon_data_dir_from_paths, daemon_git_commit, daemon_hostname,
    daemon_options_from_status, daemon_package_version, daemon_status_matches_options,
    default_main_daemon_status, install_daemon_build_info, probe_daemon_status,
    probe_daemon_status_async, probe_daemon_status_at_address,
    probe_daemon_status_at_address_on_runtime, probe_daemon_status_at_address_with_timeouts_async,
    probe_daemon_status_on_runtime, probe_daemon_status_with_timeouts_async, read_daemon_status,
    write_daemon_status,
};

pub const DAEMON_RPC_CATEGORY: &str = "daemon";
pub const DAEMON_STATUS_ROUTE: &str = "/daemon/status";
pub const DAEMON_SHUTDOWN_ROUTE: &str = "/daemon/shutdown";
pub const DAEMON_CLUSTER_MAINTENANCE_REFRESH_ROUTE: &str = "/daemon/cluster/maintenance-refresh";
pub const DAEMON_OBJECT_RPC_FORWARD_ROUTE: &str = "/daemon/object-rpc/forward";
pub const DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE: &str = "/daemon/dfs/object-rpc/forward";
pub const DAEMON_DFS_BROWSER_STATUS_ROUTE: &str = "/daemon/dfs/browser/status";
pub const DAEMON_DFS_BROWSER_STAT_ROUTE: &str = "/daemon/dfs/browser/stat";
pub const DAEMON_DFS_BROWSER_LIST_ROUTE: &str = "/daemon/dfs/browser/list";
pub const DAEMON_DFS_BROWSER_READ_FILE_ROUTE: &str = "/daemon/dfs/browser/read-file";
pub const DAEMON_DFS_BROWSER_READ_OBJECT_VERSION_ROUTE: &str =
    "/daemon/dfs/browser/read-object-version";
pub const DAEMON_DFS_BROWSER_READ_FILE_PREFIX_ROUTE: &str = "/daemon/dfs/browser/read-file-prefix";
pub const DAEMON_DFS_BROWSER_READ_FILES_ROUTE: &str = "/daemon/dfs/browser/read-files";
pub const DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE: &str = "/daemon/dfs/browser/write-file";
pub const DAEMON_DFS_BROWSER_CREATE_DIR_ALL_ROUTE: &str = "/daemon/dfs/browser/create-dir-all";
pub const DAEMON_DFS_BROWSER_RENAME_ROUTE: &str = "/daemon/dfs/browser/rename";
pub const DAEMON_DFS_BROWSER_DELETE_ROUTE: &str = "/daemon/dfs/browser/delete";
pub const DAEMON_DFS_BROWSER_PIN_METADATA_ROUTE: &str = "/daemon/dfs/browser/pin-metadata";
pub const DAEMON_DFS_BROWSER_UNPIN_METADATA_ROUTE: &str = "/daemon/dfs/browser/unpin-metadata";
pub const DAEMON_DFS_BROWSER_PIN_CONTENT_ROUTE: &str = "/daemon/dfs/browser/pin-content";
pub const DAEMON_DFS_BROWSER_UNPIN_CONTENT_ROUTE: &str = "/daemon/dfs/browser/unpin-content";
pub const DAEMON_DFS_BROWSER_TRANSFER_METADATA_PRIMARY_ROUTE: &str =
    "/daemon/dfs/browser/transfer-metadata-primary";
pub const DAEMON_DFS_BROWSER_ROUTES: &[&str] = &[
    DAEMON_DFS_BROWSER_STATUS_ROUTE,
    DAEMON_DFS_BROWSER_STAT_ROUTE,
    DAEMON_DFS_BROWSER_LIST_ROUTE,
    DAEMON_DFS_BROWSER_READ_FILE_ROUTE,
    DAEMON_DFS_BROWSER_READ_OBJECT_VERSION_ROUTE,
    DAEMON_DFS_BROWSER_READ_FILE_PREFIX_ROUTE,
    DAEMON_DFS_BROWSER_READ_FILES_ROUTE,
    DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE,
    DAEMON_DFS_BROWSER_CREATE_DIR_ALL_ROUTE,
    DAEMON_DFS_BROWSER_RENAME_ROUTE,
    DAEMON_DFS_BROWSER_DELETE_ROUTE,
    DAEMON_DFS_BROWSER_PIN_METADATA_ROUTE,
    DAEMON_DFS_BROWSER_UNPIN_METADATA_ROUTE,
    DAEMON_DFS_BROWSER_PIN_CONTENT_ROUTE,
    DAEMON_DFS_BROWSER_UNPIN_CONTENT_ROUTE,
    DAEMON_DFS_BROWSER_TRANSFER_METADATA_PRIMARY_ROUTE,
];

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonObjectRpcForwardRequest {
    pub target_node_id: sinomo_core::node::NodeId,
    pub request: sinomo_core::network::ObjectRpcRequest,
    pub connect_timeout_millis: u64,
    #[serde(default)]
    pub observe_connectivity: bool,
    #[serde(default)]
    pub raw_response: bool,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsObjectRpcForwardRequest {
    pub target_node_id: sinomo_core::node::NodeId,
    pub request: sinomo_core::network::ObjectRpcRequest,
    pub connect_timeout_millis: u64,
    #[serde(default)]
    pub observe_connectivity: bool,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserStatusRequest {
    #[serde(default)]
    pub include_health: bool,
}

impl DaemonDfsBrowserStatusRequest {
    pub fn node_only() -> Self {
        Self {
            include_health: false,
        }
    }

    pub fn with_health() -> Self {
        Self {
            include_health: true,
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserStatusResponse {
    pub node_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health: Option<DaemonDfsBrowserHealthSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health_error: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserHealthSummary {
    pub file_system_count: usize,
    pub object_count: usize,
    pub version_count: usize,
    pub pending_write_count: usize,
    pub open_conflict_count: usize,
    pub pending_repair_count: usize,
    pub pending_namespace_job_count: usize,
    pub failed_namespace_job_count: usize,
    pub metadata_shard_count: usize,
    pub repair_attempt_count: usize,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DaemonDfsBrowserSortKey {
    Name,
    Kind,
    Size,
}

impl Default for DaemonDfsBrowserSortKey {
    fn default() -> Self {
        Self::Name
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DaemonDfsBrowserSortDirection {
    Ascending,
    Descending,
}

impl Default for DaemonDfsBrowserSortDirection {
    fn default() -> Self {
        Self::Ascending
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserListRequest {
    pub path: String,
    #[serde(default)]
    pub shallow: bool,
    #[serde(default)]
    pub missing_is_empty: bool,
    #[serde(default)]
    pub sort_key: DaemonDfsBrowserSortKey,
    #[serde(default)]
    pub sort_direction: DaemonDfsBrowserSortDirection,
    #[serde(default)]
    pub offset: usize,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserListResponse {
    pub node_id: String,
    pub entries: Vec<DaemonDfsBrowserEntry>,
    pub total_count: usize,
    pub offset: usize,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserEntry {
    pub name: String,
    pub path: String,
    pub kind: sinomo_dfs::DfsObjectKind,
    pub size: Option<u64>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserStatRequest {
    pub path: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserStatResponse {
    pub file_system_id: String,
    pub object_id: String,
    pub path: String,
    pub kind: sinomo_dfs::DfsObjectKind,
    pub version: String,
    pub content_hash: Option<String>,
    pub size_bytes: Option<u64>,
    pub link_count: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub accessed_at_unix_nanos: u64,
    pub modified_at_unix_nanos: u64,
    pub created_at_unix_nanos: u64,
    pub changed_at_unix_nanos: u64,
    pub policy: sinomo_core::DfsObjectPolicy,
    pub current_locations: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub local_pins: Vec<DaemonDfsBrowserPinRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health: Option<DaemonDfsBrowserObjectHealth>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_shard: Option<DaemonDfsBrowserMetadataShard>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserMetadataShard {
    pub shard_id: String,
    pub primary_node_id: String,
    pub replica_node_ids: Vec<String>,
    pub epoch: u64,
    pub local_node_id: String,
    pub local_is_primary: bool,
    pub local_is_replica: bool,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserPinRecord {
    pub pinned_by_node_id: String,
    pub mode: sinomo_dfs::DfsPinMode,
    pub created_at_unix_nanos: Option<u64>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserObjectHealth {
    pub agent_state: sinomo_dfs::DfsAgentObjectState,
    pub version_count: usize,
    pub receipt_count: usize,
    pub intent_count: usize,
    pub open_conflict_count: usize,
    pub pending_repair_count: usize,
    pub missing_replica_nodes: Vec<String>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserReadFileRequest {
    pub path: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserReadFileResponse {
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserReadObjectVersionRequest {
    pub object_id: String,
    pub version: String,
    pub content_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserReadObjectVersionResponse {
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserReadFilePrefixRequest {
    pub path: String,
    pub max_bytes: u64,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserReadFilePrefixResponse {
    pub bytes: Vec<u8>,
    pub truncated: bool,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserReadFilesRequest {
    pub paths: Vec<String>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserReadFilesResponse {
    pub files: Vec<DaemonDfsBrowserFileRead>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserFileRead {
    pub path: String,
    pub bytes: Option<Vec<u8>>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserWriteFileRequest {
    pub path: String,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserWriteFileResponse;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserCreateDirAllRequest {
    pub path: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserCreateDirAllResponse;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserRenameRequest {
    pub from: String,
    pub to: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserRenameResponse;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserDeleteRequest {
    pub path: String,
    pub directory: bool,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserDeleteResponse;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserPinMetadataRequest {
    pub path: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserPinMetadataResponse {
    pub node_id: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserUnpinMetadataRequest {
    pub path: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserUnpinMetadataResponse {
    pub removed: bool,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserPinContentRequest {
    pub path: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserPinContentResponse {
    pub node_id: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserUnpinContentRequest {
    pub path: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserUnpinContentResponse {
    pub removed: bool,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserTransferMetadataPrimaryRequest {
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_epoch: Option<u64>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserTransferMetadataPrimaryResponse {
    pub file_system_id: String,
    pub shard_id: String,
    pub previous_primary_node_id: String,
    pub new_primary_node_id: String,
    pub previous_epoch: u64,
    pub new_epoch: u64,
    pub changed: bool,
    pub metadata_shard: DaemonDfsBrowserMetadataShard,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonClusterMaintenanceRefreshRequest;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonClusterMaintenanceRefreshResponse;
