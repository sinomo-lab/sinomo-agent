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
    DAEMON_API_PROBE_TIMEOUT, DaemonApiClient, DaemonDfsPeerTransport, RemoteConfigClient,
    send_daemon_rpc, send_daemon_rpc_async, send_daemon_rpc_on_runtime, send_daemon_rpc_stream,
    send_daemon_rpc_stream_async, send_daemon_rpc_stream_from_data_dir_async,
    send_daemon_rpc_with_timeouts,
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
pub const DAEMON_LOCAL_CONNECTIVITY_RELOAD_ROUTE: &str =
    "/daemon/cluster/local-connectivity-reload";
pub const DAEMON_OBJECT_RPC_FORWARD_ROUTE: &str = "/daemon/object-rpc/forward";
pub const DAEMON_DFS_OBJECT_RPC_FORWARD_ROUTE: &str = "/daemon/dfs/object-rpc/forward";
pub const DAEMON_DFS_PLACEMENT_AUTHORITY_ADMIN_ROUTE: &str = "/daemon/dfs/placement-authority";
pub const DAEMON_EVENT_SUBSCRIBE_ROUTE: &str = "/daemon/events/subscribe";
pub const DFS_NAMESPACE_CHANGED_EVENT_KIND: &str = "dfs.namespace.changed";
pub const DFS_PLACEMENT_CHANGED_EVENT_KIND: &str = "dfs.placement.changed";
pub const DAEMON_DFS_BROWSER_STATUS_ROUTE: &str = "/daemon/dfs/browser/status";
pub const DAEMON_DFS_BROWSER_STAT_ROUTE: &str = "/daemon/dfs/browser/stat";
pub const DAEMON_DFS_BROWSER_LIST_ROUTE: &str = "/daemon/dfs/browser/list";
pub const DAEMON_DFS_BROWSER_READ_FILE_ROUTE: &str = "/daemon/dfs/browser/read-file";
pub const DAEMON_DFS_BROWSER_READ_OBJECT_VERSION_ROUTE: &str =
    "/daemon/dfs/browser/read-object-version";
pub const DAEMON_DFS_BROWSER_READ_FILE_PREFIX_ROUTE: &str = "/daemon/dfs/browser/read-file-prefix";
pub const DAEMON_DFS_BROWSER_READ_FILES_ROUTE: &str = "/daemon/dfs/browser/read-files";
pub const DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE: &str = "/daemon/dfs/browser/write-file";
pub const DAEMON_DFS_BROWSER_REPLACE_FILE_IF_VERSION_ROUTE: &str =
    "/daemon/dfs/browser/replace-file-if-version";
pub const DAEMON_DFS_BROWSER_CREATE_DIR_ALL_ROUTE: &str = "/daemon/dfs/browser/create-dir-all";
pub const DAEMON_DFS_BROWSER_RENAME_ROUTE: &str = "/daemon/dfs/browser/rename";
pub const DAEMON_DFS_BROWSER_DELETE_ROUTE: &str = "/daemon/dfs/browser/delete";
pub const DAEMON_DFS_BROWSER_PIN_METADATA_ROUTE: &str = "/daemon/dfs/browser/pin-metadata";
pub const DAEMON_DFS_BROWSER_UNPIN_METADATA_ROUTE: &str = "/daemon/dfs/browser/unpin-metadata";
pub const DAEMON_DFS_BROWSER_PIN_CONTENT_ROUTE: &str = "/daemon/dfs/browser/pin-content";
pub const DAEMON_DFS_BROWSER_UNPIN_CONTENT_ROUTE: &str = "/daemon/dfs/browser/unpin-content";
pub const DAEMON_DFS_BROWSER_TRANSFER_METADATA_PRIMARY_ROUTE: &str =
    "/daemon/dfs/browser/transfer-metadata-primary";
pub const DAEMON_DFS_BROWSER_PLACEMENT_PREVIEW_ROUTE: &str =
    "/daemon/dfs/browser/placement-preview";
/// Retained only to return an explicit rolling-upgrade error. Apply is disabled
/// on this legacy route because its request cannot carry a preview token.
pub const DAEMON_DFS_BROWSER_PLACEMENT_UPDATE_ROUTE: &str = "/daemon/dfs/browser/placement-update";
/// Fail-closed placement Apply route. A dedicated route prevents an older
/// daemon from ignoring the preview precondition during a rolling upgrade.
pub const DAEMON_DFS_BROWSER_PLACEMENT_UPDATE_V2_ROUTE: &str =
    "/daemon/dfs/browser/placement-update-v2";
pub const DAEMON_DFS_BROWSER_PLACEMENT_PROGRESS_ROUTE: &str =
    "/daemon/dfs/browser/placement-progress";
pub const DAEMON_DFS_BROWSER_PLACEMENT_ACTIVITY_ROUTE: &str =
    "/daemon/dfs/browser/placement-activity";
pub const DAEMON_DFS_BROWSER_ROUTES: &[&str] = &[
    DAEMON_DFS_BROWSER_STATUS_ROUTE,
    DAEMON_DFS_BROWSER_STAT_ROUTE,
    DAEMON_DFS_BROWSER_LIST_ROUTE,
    DAEMON_DFS_BROWSER_READ_FILE_ROUTE,
    DAEMON_DFS_BROWSER_READ_OBJECT_VERSION_ROUTE,
    DAEMON_DFS_BROWSER_READ_FILE_PREFIX_ROUTE,
    DAEMON_DFS_BROWSER_READ_FILES_ROUTE,
    DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE,
    DAEMON_DFS_BROWSER_REPLACE_FILE_IF_VERSION_ROUTE,
    DAEMON_DFS_BROWSER_CREATE_DIR_ALL_ROUTE,
    DAEMON_DFS_BROWSER_RENAME_ROUTE,
    DAEMON_DFS_BROWSER_DELETE_ROUTE,
    DAEMON_DFS_BROWSER_PIN_METADATA_ROUTE,
    DAEMON_DFS_BROWSER_UNPIN_METADATA_ROUTE,
    DAEMON_DFS_BROWSER_PIN_CONTENT_ROUTE,
    DAEMON_DFS_BROWSER_UNPIN_CONTENT_ROUTE,
    DAEMON_DFS_BROWSER_TRANSFER_METADATA_PRIMARY_ROUTE,
    DAEMON_DFS_BROWSER_PLACEMENT_PREVIEW_ROUTE,
    DAEMON_DFS_BROWSER_PLACEMENT_UPDATE_ROUTE,
    DAEMON_DFS_BROWSER_PLACEMENT_UPDATE_V2_ROUTE,
    DAEMON_DFS_BROWSER_PLACEMENT_PROGRESS_ROUTE,
    DAEMON_DFS_BROWSER_PLACEMENT_ACTIVITY_ROUTE,
];

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonEventSubscribeRequest {
    /// Event-kind prefixes to receive. An empty list subscribes to every kind.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub event_kind_prefixes: Vec<String>,
    /// DFS namespace path prefixes to receive. An empty list accepts events
    /// without path metadata as well as namespace events for every path.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub path_prefixes: Vec<String>,
}

impl DaemonEventSubscribeRequest {
    pub fn for_path_prefix(path_prefix: impl Into<String>) -> Self {
        Self {
            event_kind_prefixes: vec![DFS_NAMESPACE_CHANGED_EVENT_KIND.to_string()],
            path_prefixes: vec![path_prefix.into()],
        }
    }

    /// Subscribe to namespace and placement changes beneath one DFS path.
    pub fn for_dfs_path_prefix(path_prefix: impl Into<String>) -> Self {
        Self {
            event_kind_prefixes: vec!["dfs.".to_string()],
            path_prefixes: vec![path_prefix.into()],
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonEventSubscribeResponse {
    pub subscription_id: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DaemonEventStreamItem {
    Ready {
        subscription_id: String,
    },
    Event {
        event: sinomo_core::EventEnvelope,
    },
    /// The subscriber fell behind the daemon's bounded live buffer. Consumers
    /// must reload their durable source of truth before processing later events.
    Lagged {
        skipped: u64,
    },
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonObjectRpcForwardRequest {
    pub target_node_id: sinomo_core::node::NodeId,
    pub request: sinomo_core::network::ObjectRpcRequest,
    pub connect_timeout_millis: u64,
    /// Overrides the routed session's no-progress timeout. Missing values from
    /// older callers retain the daemon's default network timeout.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_timeout_millis: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overall_timeout_millis: Option<u64>,
    #[serde(default)]
    pub observe_connectivity: bool,
}

/// Retry-safety evidence carried only across the trusted local daemon-control
/// boundary. Remote object-RPC responses must not be allowed to assert that a
/// request was definitely not applied.
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub enum DaemonObjectRpcForwardDeliveryState {
    DefinitelyNotSent,
    PossiblyApplied,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub enum DaemonObjectRpcForwardResponse {
    Success {
        response: sinomo_core::network::ObjectRpcResponse,
    },
    NetworkError {
        detail: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        delivery_state: Option<DaemonObjectRpcForwardDeliveryState>,
    },
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
    /// Include compact, row-oriented metadata with each entry. This avoids a stat RPC per visible
    /// row while keeping filesystem adapters that need only names and kinds lightweight.
    #[serde(default)]
    pub include_metadata: bool,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<DaemonDfsBrowserEntryMetadata>,
}

/// Compact visible-version metadata suitable for one directory-list row.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserEntryMetadata {
    pub object_id: String,
    pub version: String,
    pub modified_at_unix_nanos: u64,
    pub policy: sinomo_core::DfsObjectPolicy,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub current_locations: Vec<String>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserStatRequest {
    pub path: String,
    /// Maximum age of a previously authoritative result that the resident
    /// daemon browser may reuse. Zero preserves strict DFS stat semantics.
    #[serde(default)]
    pub max_staleness_millis: u64,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placement: Option<sinomo_dfs::DfsResolvedPlacementPolicy>,
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

/// Fail-closed compare-and-swap replacement request.
///
/// This has a dedicated route so an older daemon cannot silently ignore the
/// precondition and fall back to an unconditional write during a rolling
/// upgrade.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserReplaceFileIfVersionRequest {
    pub path: String,
    pub bytes: Vec<u8>,
    pub expected_object_id: String,
    pub expected_version: String,
}

/// The exact visible version committed by a conditional replacement.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserWriteReceipt {
    pub object_id: String,
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
}

/// Typed operational result for a conditional replacement. Transport and
/// daemon-internal failures remain RPC errors; expected filesystem outcomes do
/// not get flattened into service-error strings.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub enum DaemonDfsBrowserReplaceFileIfVersionOutcome {
    Committed {
        receipt: DaemonDfsBrowserWriteReceipt,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        maintenance_warning: Option<String>,
    },
    Conflict {
        detail: String,
    },
    NotFound {
        detail: String,
    },
    Rejected {
        detail: String,
    },
}

impl DaemonDfsBrowserReplaceFileIfVersionOutcome {
    pub fn is_committed(&self) -> bool {
        matches!(self, Self::Committed { .. })
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserReplaceFileIfVersionResponse {
    pub outcome: DaemonDfsBrowserReplaceFileIfVersionOutcome,
}

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
pub struct DaemonDfsBrowserPlacementPreviewRequest {
    pub path: String,
    /// Proposed override. `None` previews removing the exact `(path, scope)`
    /// rule and inheriting from the next matching ancestor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<sinomo_dfs::DfsPlacementPolicy>,
    #[serde(default)]
    pub scope: sinomo_dfs::DfsPlacementScope,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserPlacementPreviewResponse {
    pub preview: sinomo_dfs::DfsPlacementPreview,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserPlacementUpdateRequest {
    pub path: String,
    /// `None` removes the override for exactly `(path, scope)`, restoring
    /// inheritance from the nearest matching ancestor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<sinomo_dfs::DfsPlacementPolicy>,
    #[serde(default)]
    pub scope: sinomo_dfs::DfsPlacementScope,
}

/// Apply only the exact placement plan returned by the immediately preceding
/// preview. The token is intentionally required and lives on a versioned route
/// so missing support can never degrade to an unconditional update.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserPlacementUpdateV2Request {
    pub path: String,
    /// `None` removes the override for exactly `(path, scope)`, restoring
    /// inheritance from the nearest matching ancestor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<sinomo_dfs::DfsPlacementPolicy>,
    #[serde(default)]
    pub scope: sinomo_dfs::DfsPlacementScope,
    pub expected_preview_token: sinomo_dfs::DfsPlacementPreviewToken,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserPlacementUpdateResponse {
    pub preview: sinomo_dfs::DfsPlacementPreview,
    pub reconciliation: sinomo_dfs::DfsPlacementReconciliationJob,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserPlacementProgressRequest {
    pub job_id: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserPlacementProgressResponse {
    pub reconciliation: sinomo_dfs::DfsPlacementReconciliationJob,
}

fn default_daemon_dfs_browser_placement_activity_limit() -> usize {
    sinomo_dfs::DEFAULT_DFS_PLACEMENT_RECONCILIATION_ACTIVITY_PAGE_SIZE
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserPlacementActivityRequest {
    /// Empty means every activity status.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub statuses: Vec<sinomo_dfs::DfsPlacementReconciliationActivityStatus>,
    /// Optional normalized SIFS path boundary. `/team` includes `/team/a` but
    /// not `/teammate`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<sinomo_dfs::DfsPlacementReconciliationActivityCursor>,
    #[serde(default = "default_daemon_dfs_browser_placement_activity_limit")]
    pub limit: usize,
}

impl Default for DaemonDfsBrowserPlacementActivityRequest {
    fn default() -> Self {
        Self {
            statuses: Vec::new(),
            path_prefix: None,
            cursor: None,
            limit: default_daemon_dfs_browser_placement_activity_limit(),
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserPlacementActivityResponse {
    pub page: sinomo_dfs::DfsPlacementReconciliationActivityPage,
}

/// Main-daemon-only operator surface for inspecting and recovering a stranded
/// DFS placement mutation permit. This route is intentionally separate from
/// the local-command browser API and is cataloged with admin authorization.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
#[serde(tag = "operation", rename_all = "kebab-case")]
pub enum DaemonDfsPlacementAuthorityAdminRequest {
    Inspect {
        file_system_id: sinomo_dfs::DfsFileSystemId,
    },
    RecoverRemovedNode {
        file_system_id: sinomo_dfs::DfsFileSystemId,
        expected_recovery_token: String,
        #[serde(default)]
        acknowledge_owner_stopped: bool,
    },
    BreakGlassRecover {
        file_system_id: sinomo_dfs::DfsFileSystemId,
        expected_recovery_token: String,
        acknowledgement: String,
    },
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
#[serde(tag = "operation", rename_all = "kebab-case")]
pub enum DaemonDfsPlacementAuthorityAdminResponse {
    Inspection {
        permit: Option<sinomo_dfs::DfsPlacementAuthorityPermitStatus>,
    },
    Recovery {
        outcome: sinomo_dfs::DfsPlacementAuthorityRecoveryOutcome,
    },
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonClusterMaintenanceRefreshRequest;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonClusterMaintenanceRefreshResponse;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonLocalConnectivityReloadRequest;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonLocalConnectivityReloadResponse;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dfs_subscription_covers_namespace_and_placement_events() {
        let request = DaemonEventSubscribeRequest::for_dfs_path_prefix("/users/project");
        assert_eq!(request.event_kind_prefixes, vec!["dfs."]);
        assert_eq!(request.path_prefixes, vec!["/users/project"]);
    }

    #[test]
    fn older_browser_list_requests_default_to_lightweight_entries() {
        let request: DaemonDfsBrowserListRequest = serde_json::from_value(serde_json::json!({
            "path": "/users"
        }))
        .expect("decode legacy list request");
        assert!(!request.include_metadata);
    }

    #[test]
    fn conditional_replace_has_a_dedicated_fail_closed_route() {
        assert_ne!(
            DAEMON_DFS_BROWSER_REPLACE_FILE_IF_VERSION_ROUTE,
            DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE
        );
        assert!(
            DAEMON_DFS_BROWSER_ROUTES.contains(&DAEMON_DFS_BROWSER_REPLACE_FILE_IF_VERSION_ROUTE)
        );

        let write = serde_json::to_value(DaemonDfsBrowserWriteFileRequest {
            path: "/notes.txt".to_string(),
            bytes: b"plain write".to_vec(),
        })
        .expect("serialize plain write");
        assert_eq!(write.as_object().map(|fields| fields.len()), Some(2));
        assert!(write.get("expected_object_id").is_none());
        assert!(write.get("expected_version").is_none());
    }

    #[test]
    fn placement_authority_admin_route_is_not_a_browser_local_command() {
        assert!(!DAEMON_DFS_BROWSER_ROUTES.contains(&DAEMON_DFS_PLACEMENT_AUTHORITY_ADMIN_ROUTE));
        assert_eq!(
            DAEMON_DFS_PLACEMENT_AUTHORITY_ADMIN_ROUTE,
            "/daemon/dfs/placement-authority"
        );
    }

    #[test]
    fn placement_routes_are_typed_and_inheritance_is_explicit() {
        for route in [
            DAEMON_DFS_BROWSER_PLACEMENT_PREVIEW_ROUTE,
            DAEMON_DFS_BROWSER_PLACEMENT_UPDATE_ROUTE,
            DAEMON_DFS_BROWSER_PLACEMENT_UPDATE_V2_ROUTE,
            DAEMON_DFS_BROWSER_PLACEMENT_PROGRESS_ROUTE,
            DAEMON_DFS_BROWSER_PLACEMENT_ACTIVITY_ROUTE,
        ] {
            assert!(DAEMON_DFS_BROWSER_ROUTES.contains(&route));
        }
        let request = DaemonDfsBrowserPlacementUpdateRequest {
            path: "/users/team".to_string(),
            policy: None,
            scope: sinomo_dfs::DfsPlacementScope::SelfOnly,
        };
        let encoded = serde_json::to_vec(&request).expect("encode placement update");
        let decoded: DaemonDfsBrowserPlacementUpdateRequest =
            serde_json::from_slice(&encoded).expect("decode placement update");
        assert_eq!(decoded, request);
        assert!(decoded.policy.is_none());
    }

    #[test]
    fn placement_update_v2_requires_a_preview_token_on_a_dedicated_route() {
        assert_ne!(
            DAEMON_DFS_BROWSER_PLACEMENT_UPDATE_V2_ROUTE,
            DAEMON_DFS_BROWSER_PLACEMENT_UPDATE_ROUTE
        );
        assert!(DAEMON_DFS_BROWSER_ROUTES.contains(&DAEMON_DFS_BROWSER_PLACEMENT_UPDATE_V2_ROUTE));

        let missing_token = serde_json::json!({
            "path": "/users/team",
            "scope": "self-only"
        });
        assert!(
            serde_json::from_value::<DaemonDfsBrowserPlacementUpdateV2Request>(missing_token)
                .is_err(),
            "the fail-closed route must not default a missing preview token"
        );
    }

    #[test]
    fn placement_activity_request_defaults_to_a_bounded_unfiltered_page() {
        let request: DaemonDfsBrowserPlacementActivityRequest =
            serde_json::from_value(serde_json::json!({})).expect("default activity request");
        assert!(request.statuses.is_empty());
        assert!(request.path_prefix.is_none());
        assert!(request.cursor.is_none());
        assert_eq!(
            request.limit,
            sinomo_dfs::DEFAULT_DFS_PLACEMENT_RECONCILIATION_ACTIVITY_PAGE_SIZE
        );

        let encoded = serde_json::to_value(DaemonDfsBrowserPlacementActivityRequest {
            statuses: vec![sinomo_dfs::DfsPlacementReconciliationActivityStatus::Retrying],
            path_prefix: Some("/team".to_string()),
            cursor: Some(sinomo_dfs::DfsPlacementReconciliationActivityCursor {
                created_at_unix_nanos: 42,
                job_id: "activity-job".to_string(),
            }),
            limit: 25,
        })
        .expect("encode activity request");
        let decoded: DaemonDfsBrowserPlacementActivityRequest =
            serde_json::from_value(encoded).expect("decode activity request");
        assert_eq!(decoded.limit, 25);
        assert_eq!(
            decoded.statuses,
            vec![sinomo_dfs::DfsPlacementReconciliationActivityStatus::Retrying]
        );

        let legacy_page: DaemonDfsBrowserPlacementActivityResponse =
            serde_json::from_value(serde_json::json!({"page": {"activities": []}}))
                .expect("activity completeness flags are backward-compatible defaults");
        assert!(!legacy_page.page.partial);
        assert!(!legacy_page.page.history_truncated);
    }

    #[test]
    fn conditional_replace_outcomes_preserve_receipt_and_error_kinds() {
        let committed = DaemonDfsBrowserReplaceFileIfVersionResponse {
            outcome: DaemonDfsBrowserReplaceFileIfVersionOutcome::Committed {
                receipt: DaemonDfsBrowserWriteReceipt {
                    object_id: "object-1".to_string(),
                    version: "version-2".to_string(),
                    content_hash: Some("hash-2".to_string()),
                    size_bytes: Some(7),
                },
                maintenance_warning: Some("maintenance queued".to_string()),
            },
        };
        let encoded = serde_json::to_vec(&committed).expect("encode committed outcome");
        let decoded: DaemonDfsBrowserReplaceFileIfVersionResponse =
            serde_json::from_slice(&encoded).expect("decode committed outcome");
        assert_eq!(decoded, committed);
        assert!(decoded.outcome.is_committed());

        for outcome in [
            DaemonDfsBrowserReplaceFileIfVersionOutcome::Conflict {
                detail: "stale base".to_string(),
            },
            DaemonDfsBrowserReplaceFileIfVersionOutcome::NotFound {
                detail: "removed".to_string(),
            },
            DaemonDfsBrowserReplaceFileIfVersionOutcome::Rejected {
                detail: "not writable".to_string(),
            },
        ] {
            let encoded = serde_json::to_vec(&outcome).expect("encode typed outcome");
            let decoded: DaemonDfsBrowserReplaceFileIfVersionOutcome =
                serde_json::from_slice(&encoded).expect("decode typed outcome");
            assert_eq!(decoded, outcome);
            assert!(!decoded.is_committed());
        }
    }

    #[test]
    fn daemon_api_error_preserves_possibly_applied_delivery_state() {
        let error = DaemonApiError::from(sinomo_core::network::NetworkError::timeout(
            "conditional replace response",
            sinomo_core::network::NetworkTimeoutPhase::FirstByte,
            std::time::Duration::from_secs(1),
            sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
        ));
        assert_eq!(
            error.delivery_state(),
            Some(sinomo_core::network::NetworkDeliveryState::PossiblyApplied)
        );
    }
}
