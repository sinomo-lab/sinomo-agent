//! DFS browser and placement-authority wire contracts.

pub const DFS_NAMESPACE_CHANGED_EVENT_KIND: &str = "dfs.namespace.changed";
pub const DFS_PLACEMENT_CHANGED_EVENT_KIND: &str = "dfs.placement.changed";
pub const DFS_EVENT_SCHEMA_ID: &str = "sinomo.dfs.change";
pub const DFS_EVENT_SCHEMA_VERSION: &str = "1.0.0";

pub const DAEMON_DFS_PLACEMENT_AUTHORITY_ADMIN_ROUTE: &str = "/daemon/dfs/placement-authority";
pub const DAEMON_DFS_BROWSER_STATUS_ROUTE: &str = "/daemon/dfs/browser/status";
pub const DAEMON_DFS_BROWSER_STAT_ROUTE: &str = "/daemon/dfs/browser/stat";
/// Strict namespace-entry metadata without following the final link. Older
/// servers must reject this route, rather than interpret a stat flag as absent.
pub const DAEMON_DFS_BROWSER_NAMESPACE_LSTAT_ROUTE: &str = "/daemon/dfs/browser/namespace-lstat-v1";
/// Exact scoped operations. A dedicated route makes an older provider reject
/// the no-link contract rather than silently ignore a safety option.
pub const DAEMON_DFS_BROWSER_NO_LINKS_ROUTE: &str = "/daemon/dfs/browser/no-links-v1";
pub const DAEMON_DFS_BROWSER_LIST_ROUTE: &str = "/daemon/dfs/browser/list";
pub const DAEMON_DFS_BROWSER_READ_FILE_ROUTE: &str = "/daemon/dfs/browser/read-file-v7";
pub const DAEMON_DFS_BROWSER_READ_OBJECT_RANGE_ROUTE: &str =
    "/daemon/dfs/browser/read-object-version-range-v1";
pub const DFS_BROWSER_RANGE_LIMIT: u32 = 64 * 1024;
pub const DAEMON_DFS_BROWSER_READ_OBJECT_VERSION_ROUTE: &str =
    "/daemon/dfs/browser/read-object-version";
pub const DAEMON_DFS_BROWSER_FILE_HISTORY_PIN_ROUTE: &str =
    "/daemon/dfs/browser/file-history/pin-v1";
pub const DAEMON_DFS_BROWSER_FILE_HISTORY_LIST_ROUTE: &str =
    "/daemon/dfs/browser/file-history/list-v1";
pub const DAEMON_DFS_BROWSER_FILE_HISTORY_READ_ROUTE: &str =
    "/daemon/dfs/browser/file-history/read-v1";
pub const DAEMON_DFS_BROWSER_FILE_HISTORY_RESTORE_ROUTE: &str =
    "/daemon/dfs/browser/file-history/restore-v1";
pub const DAEMON_DFS_BROWSER_FILE_HISTORY_POLICY_ROUTE: &str =
    "/daemon/dfs/browser/file-history/policy-v1";
pub const DAEMON_DFS_BROWSER_FILE_HISTORY_POLICY_UPDATE_ROUTE: &str =
    "/daemon/dfs/browser/file-history/policy-update-v1";
pub const DAEMON_DFS_BROWSER_READ_FILE_PREFIX_ROUTE: &str = "/daemon/dfs/browser/read-file-prefix";
pub const DAEMON_DFS_BROWSER_READ_FILES_ROUTE: &str = "/daemon/dfs/browser/read-files";
pub const DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE: &str = "/daemon/dfs/browser/write-file";
pub const DAEMON_DFS_BROWSER_REPLACE_FILE_IF_VERSION_ROUTE: &str =
    "/daemon/dfs/browser/replace-file-if-version";
/// Versioned route: older daemons must not ignore stable operation identity.
pub const DAEMON_DFS_BROWSER_DURABLE_REPLACE_ROUTE: &str = "/daemon/dfs/browser/durable-replace-v1";

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
/// Fail-closed placement Apply route. A dedicated route prevents an older
/// daemon from ignoring the preview precondition during a rolling upgrade.
pub const DAEMON_DFS_BROWSER_PLACEMENT_UPDATE_V2_ROUTE: &str =
    "/daemon/dfs/browser/placement-update-v2";
/// The placement Apply transaction definitely published no configuration or
/// reconciliation job. Only the rejecting boundary may emit this diagnostic;
/// transport, quorum and post-commit errors must not carry it. A caller that
/// owns its proposal may obtain a new preview before retrying the same edit.
pub const DFS_PLACEMENT_APPLY_REJECTED_PREFIX: &str =
    "SIFS placement Apply rejected before publication:";
pub const DAEMON_DFS_BROWSER_PLACEMENT_PROGRESS_ROUTE: &str =
    "/daemon/dfs/browser/placement-progress";
pub const DAEMON_DFS_BROWSER_PLACEMENT_ACTIVITY_ROUTE: &str =
    "/daemon/dfs/browser/placement-activity";
pub const DAEMON_DFS_BROWSER_ROUTES: &[&str] = &[
    super::DFS_ATTACHMENT_ROUTE,
    super::DFS_ATTACHMENT_COMPONENT_ROUTE,
    super::DFS_ATTACHMENT_UPLOAD_ROUTE,
    DAEMON_DFS_BROWSER_STATUS_ROUTE,
    DAEMON_DFS_BROWSER_STAT_ROUTE,
    DAEMON_DFS_BROWSER_NAMESPACE_LSTAT_ROUTE,
    DAEMON_DFS_BROWSER_NO_LINKS_ROUTE,
    DAEMON_DFS_BROWSER_LIST_ROUTE,
    DAEMON_DFS_BROWSER_READ_FILE_ROUTE,
    DAEMON_DFS_BROWSER_READ_OBJECT_VERSION_ROUTE,
    DAEMON_DFS_BROWSER_READ_OBJECT_RANGE_ROUTE,
    DAEMON_DFS_BROWSER_FILE_HISTORY_PIN_ROUTE,
    DAEMON_DFS_BROWSER_FILE_HISTORY_LIST_ROUTE,
    DAEMON_DFS_BROWSER_FILE_HISTORY_READ_ROUTE,
    DAEMON_DFS_BROWSER_FILE_HISTORY_RESTORE_ROUTE,
    DAEMON_DFS_BROWSER_FILE_HISTORY_POLICY_ROUTE,
    DAEMON_DFS_BROWSER_FILE_HISTORY_POLICY_UPDATE_ROUTE,
    DAEMON_DFS_BROWSER_READ_FILE_PREFIX_ROUTE,
    DAEMON_DFS_BROWSER_READ_FILES_ROUTE,
    DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE,
    DAEMON_DFS_BROWSER_REPLACE_FILE_IF_VERSION_ROUTE,
    DAEMON_DFS_BROWSER_DURABLE_REPLACE_ROUTE,
    DAEMON_DFS_BROWSER_CREATE_DIR_ALL_ROUTE,
    DAEMON_DFS_BROWSER_RENAME_ROUTE,
    DAEMON_DFS_BROWSER_DELETE_ROUTE,
    DAEMON_DFS_BROWSER_PIN_METADATA_ROUTE,
    DAEMON_DFS_BROWSER_UNPIN_METADATA_ROUTE,
    DAEMON_DFS_BROWSER_PIN_CONTENT_ROUTE,
    DAEMON_DFS_BROWSER_UNPIN_CONTENT_ROUTE,
    DAEMON_DFS_BROWSER_TRANSFER_METADATA_PRIMARY_ROUTE,
    DAEMON_DFS_BROWSER_PLACEMENT_PREVIEW_ROUTE,
    DAEMON_DFS_BROWSER_PLACEMENT_UPDATE_V2_ROUTE,
    DAEMON_DFS_BROWSER_PLACEMENT_PROGRESS_ROUTE,
    DAEMON_DFS_BROWSER_PLACEMENT_ACTIVITY_ROUTE,
];

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DaemonDfsBrowserNoLinksRequest {
    /// Bind retained source references to their authenticated cluster namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_cluster_id: Option<sinomo_core::Id>,
    pub root: String,
    pub path: String,
    pub operation: DaemonDfsBrowserNoLinksOperation,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum DaemonDfsBrowserNoLinksOperation {
    Stat,
    ListPage {
        cursor: Option<Vec<u8>>,
        max_entries: usize,
    },
    ReadRange {
        offset: u64,
        max_bytes: u64,
    },
    CreateDirAll,
    CreateNew {
        #[serde(with = "serde_bytes")]
        bytes: Vec<u8>,
    },
    ReplaceIfVersion {
        expected_object_id: String,
        expected_version: String,
        #[serde(with = "serde_bytes")]
        bytes: Vec<u8>,
    },
}

impl DaemonDfsBrowserNoLinksOperation {
    pub fn is_mutation(&self) -> bool {
        matches!(
            self,
            Self::CreateDirAll | Self::CreateNew { .. } | Self::ReplaceIfVersion { .. }
        )
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum DaemonDfsBrowserNoLinksResponse {
    Metadata {
        metadata: DaemonDfsBrowserStatResponse,
    },
    DirectoryPage {
        directory: DaemonDfsBrowserStatResponse,
        entries: Vec<DaemonDfsBrowserEntry>,
        next_cursor: Option<Vec<u8>>,
    },
    Read {
        metadata: DaemonDfsBrowserStatResponse,
        #[serde(with = "serde_bytes")]
        bytes: Vec<u8>,
        truncated: bool,
    },
    DirectoryCreated,
    Failed {
        failure: DaemonDfsBrowserNoLinksFailure,
    },
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum DaemonDfsBrowserNoLinksFailure {
    NotFound { path: String },
    AlreadyExists { path: String },
    NotDirectory { path: String },
    IsDirectory { path: String },
    InvalidPath { path: String, detail: String },
    Conflict { path: String, detail: String },
    Unavailable { path: String, detail: String },
    Unsupported { path: String, detail: String },
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
    pub pending_repair_count: usize,
    pub pending_namespace_job_count: usize,
    pub failed_namespace_job_count: usize,
    pub metadata_shard_count: usize,
    pub repair_attempt_count: usize,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum DaemonDfsBrowserSortKey {
    #[default]
    Name,
    Kind,
    Size,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum DaemonDfsBrowserSortDirection {
    #[default]
    Ascending,
    Descending,
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
    pub kind: crate::dfs::DfsObjectKind,
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
    /// Live placement state captured by the same directory refresh that returned this row.
    /// Older daemons omit it, in which case clients fall back to the copy locations above.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placement: Option<DaemonDfsBrowserEntryPlacement>,
}

/// Compact placement facts needed to render one directory row without a follow-up stat request.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserEntryPlacement {
    pub agent_state: crate::dfs::DfsAgentObjectState,
    pub desired_replicas: usize,
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
#[serde(deny_unknown_fields)]
pub struct DaemonDfsBrowserStatResponse {
    pub file_system_id: String,
    pub object_id: String,
    pub path: String,
    pub kind: crate::dfs::DfsObjectKind,
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
    pub placement: Option<crate::dfs::DfsResolvedPlacementPolicy>,
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
    pub mode: crate::dfs::DfsPinMode,
    pub created_at_unix_nanos: Option<u64>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserObjectHealth {
    pub agent_state: crate::dfs::DfsAgentObjectState,
    pub version_count: usize,
    pub receipt_count: usize,
    pub intent_count: usize,
    pub pending_repair_count: usize,
    pub missing_replica_nodes: Vec<String>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserReadFileRequest {
    pub path: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
#[serde(tag = "status", rename_all = "kebab-case", deny_unknown_fields)]
pub enum DaemonDfsBrowserReadFileResponse {
    Found {
        #[serde(with = "serde_bytes")]
        bytes: Vec<u8>,
    },
    Failed {
        failure: DaemonDfsBrowserReadFileFailure,
    },
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq, thiserror::Error)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum DaemonDfsBrowserReadFileFailure {
    #[error("SIFS path not found: {path}")]
    NotFound { path: String },
    #[error("SIFS content is unavailable at {path}: {detail}")]
    ContentUnavailable { path: String, detail: String },
    #[error("SIFS read failed at {path}: {detail}")]
    ReadFailed { path: String, detail: String },
    #[error("invalid daemon SIFS read response: {detail}")]
    InvalidResponse { detail: String },
}

/// Typed metadata lookup result.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
#[serde(tag = "status", rename_all = "kebab-case", deny_unknown_fields)]
pub enum DaemonDfsBrowserStatOutcomeResponse {
    Found(DaemonDfsBrowserStatResponse),
    Failed {
        failure: DaemonDfsBrowserReadFileFailure,
    },
}

impl DaemonDfsBrowserStatOutcomeResponse {
    pub fn found(stat: DaemonDfsBrowserStatResponse) -> Self {
        Self::Found(stat)
    }

    pub fn not_found(path: impl Into<String>) -> Self {
        Self::Failed {
            failure: DaemonDfsBrowserReadFileFailure::NotFound { path: path.into() },
        }
    }

    pub fn into_result(
        self,
    ) -> Result<DaemonDfsBrowserStatResponse, DaemonDfsBrowserReadFileFailure> {
        match self {
            Self::Found(stat) => Ok(stat),
            Self::Failed { failure } => Err(failure),
        }
    }
}

impl DaemonDfsBrowserReadFileResponse {
    pub fn found(bytes: Vec<u8>) -> Self {
        Self::Found { bytes }
    }
    pub fn not_found(path: impl Into<String>) -> Self {
        Self::Failed {
            failure: DaemonDfsBrowserReadFileFailure::NotFound { path: path.into() },
        }
    }
    pub fn content_unavailable(path: impl Into<String>, detail: impl Into<String>) -> Self {
        Self::Failed {
            failure: DaemonDfsBrowserReadFileFailure::ContentUnavailable {
                path: path.into(),
                detail: detail.into(),
            },
        }
    }
    pub fn into_result(self) -> Result<Vec<u8>, DaemonDfsBrowserReadFileFailure> {
        match self {
            Self::Found { bytes } => Ok(bytes),
            Self::Failed { failure } => Err(failure),
        }
    }
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
#[serde(tag = "status", rename_all = "kebab-case", deny_unknown_fields)]
pub enum DaemonDfsBrowserReadFilePrefixResponse {
    Found {
        #[serde(with = "serde_bytes")]
        bytes: Vec<u8>,
        truncated: bool,
    },
    Failed {
        failure: DaemonDfsBrowserReadFileFailure,
    },
}

impl DaemonDfsBrowserReadFilePrefixResponse {
    pub fn found(bytes: Vec<u8>, truncated: bool) -> Self {
        Self::Found { bytes, truncated }
    }
    pub fn not_found(path: impl Into<String>) -> Self {
        Self::Failed {
            failure: DaemonDfsBrowserReadFileFailure::NotFound { path: path.into() },
        }
    }
    pub fn content_unavailable(path: impl Into<String>, detail: impl Into<String>) -> Self {
        Self::Failed {
            failure: DaemonDfsBrowserReadFileFailure::ContentUnavailable {
                path: path.into(),
                detail: detail.into(),
            },
        }
    }
    pub fn into_result(self) -> Result<(Vec<u8>, bool), DaemonDfsBrowserReadFileFailure> {
        match self {
            Self::Found { bytes, truncated } => Ok((bytes, truncated)),
            Self::Failed { failure } => Err(failure),
        }
    }
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
#[serde(deny_unknown_fields)]
pub struct DaemonDfsBrowserFileRead {
    pub path: String,
    pub outcome: DaemonDfsBrowserReadFileResponse,
}

impl DaemonDfsBrowserFileRead {
    pub fn found(path: impl Into<String>, bytes: Vec<u8>) -> Self {
        Self {
            path: path.into(),
            outcome: DaemonDfsBrowserReadFileResponse::found(bytes),
        }
    }
    pub fn not_found(path: impl Into<String>) -> Self {
        let path = path.into();
        Self {
            path: path.clone(),
            outcome: DaemonDfsBrowserReadFileResponse::not_found(path),
        }
    }
    pub fn content_unavailable(path: impl Into<String>, detail: impl Into<String>) -> Self {
        let path = path.into();
        Self {
            path: path.clone(),
            outcome: DaemonDfsBrowserReadFileResponse::content_unavailable(path, detail),
        }
    }
    pub fn read_failed(path: impl Into<String>, detail: impl Into<String>) -> Self {
        let path = path.into();
        Self {
            path: path.clone(),
            outcome: DaemonDfsBrowserReadFileResponse::Failed {
                failure: DaemonDfsBrowserReadFileFailure::ReadFailed {
                    path,
                    detail: detail.into(),
                },
            },
        }
    }
    pub fn into_result(self) -> Result<Vec<u8>, DaemonDfsBrowserReadFileFailure> {
        self.outcome.into_result()
    }
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
    /// The replacement may or may not have committed: its metadata body is
    /// durable but its commit was not confirmed. It is not a failure; reload
    /// the file to see which version is current.
    Unknown {
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
    pub policy: Option<crate::dfs::DfsPlacementPolicy>,
    #[serde(default)]
    pub scope: crate::dfs::DfsPlacementScope,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserPlacementPreviewResponse {
    pub preview: crate::dfs::DfsPlacementPreview,
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
    pub policy: Option<crate::dfs::DfsPlacementPolicy>,
    #[serde(default)]
    pub scope: crate::dfs::DfsPlacementScope,
    pub expected_preview_token: crate::dfs::DfsPlacementPreviewToken,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserPlacementUpdateResponse {
    pub preview: crate::dfs::DfsPlacementPreview,
    pub reconciliation: crate::dfs::DfsPlacementReconciliationJob,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserPlacementProgressRequest {
    pub job_id: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserPlacementProgressResponse {
    pub reconciliation: crate::dfs::DfsPlacementReconciliationJob,
}

fn default_daemon_dfs_browser_placement_activity_limit() -> usize {
    crate::dfs::DEFAULT_DFS_PLACEMENT_RECONCILIATION_ACTIVITY_PAGE_SIZE
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserPlacementActivityRequest {
    /// Empty means every activity status.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub statuses: Vec<crate::dfs::DfsPlacementReconciliationActivityStatus>,
    /// Optional normalized SIFS path boundary. `/team` includes `/team/a` but
    /// not `/teammate`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<crate::dfs::DfsPlacementReconciliationActivityCursor>,
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
    pub page: crate::dfs::DfsPlacementReconciliationActivityPage,
}

/// Main-daemon-only operator surface for inspecting and recovering a stranded
/// DFS placement mutation permit. This route is intentionally separate from
/// the local-command browser API and is cataloged with admin authorization.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
#[serde(tag = "operation", rename_all = "kebab-case")]
pub enum DaemonDfsPlacementAuthorityAdminRequest {
    Inspect {
        file_system_id: crate::dfs::DfsFileSystemId,
    },
    RecoverRemovedNode {
        file_system_id: crate::dfs::DfsFileSystemId,
        expected_recovery_token: String,
        #[serde(default)]
        acknowledge_owner_stopped: bool,
    },
    BreakGlassRecover {
        file_system_id: crate::dfs::DfsFileSystemId,
        expected_recovery_token: String,
        acknowledgement: String,
    },
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
#[serde(tag = "operation", rename_all = "kebab-case")]
pub enum DaemonDfsPlacementAuthorityAdminResponse {
    Inspection {
        permit: Option<crate::dfs::DfsPlacementAuthorityPermitStatus>,
    },
    Recovery {
        outcome: crate::dfs::DfsPlacementAuthorityRecoveryOutcome,
    },
}

#[cfg(test)]
mod namespace_lstat_wire_tests {
    use super::*;

    #[test]
    fn versioned_namespace_lstat_round_trips_typed_absence_without_stat_flags() {
        assert_ne!(
            DAEMON_DFS_BROWSER_NAMESPACE_LSTAT_ROUTE,
            DAEMON_DFS_BROWSER_STAT_ROUTE
        );
        let request = sinomo_core::network::ObjectRpcRequest::encode_body(
            DAEMON_DFS_BROWSER_NAMESPACE_LSTAT_ROUTE,
            crate::DAEMON_RPC_CATEGORY,
            &DaemonDfsBrowserStatRequest {
                path: "/services/missing".into(),
                max_staleness_millis: 0,
            },
        )
        .unwrap();
        let decoded: DaemonDfsBrowserStatRequest = request.decode_body().unwrap();
        assert_eq!(decoded.path, "/services/missing");
        assert_eq!(decoded.max_staleness_millis, 0);
        let response = sinomo_core::network::ObjectRpcResponse::encode_body(
            DAEMON_DFS_BROWSER_NAMESPACE_LSTAT_ROUTE,
            &DaemonDfsBrowserStatOutcomeResponse::not_found(&decoded.path),
        )
        .unwrap();
        let decoded: DaemonDfsBrowserStatOutcomeResponse = response.decode_body().unwrap();
        assert!(
            matches!(decoded.into_result(), Err(DaemonDfsBrowserReadFileFailure::NotFound { path }) if path == "/services/missing")
        );
    }
}

#[cfg(test)]
mod versioned_bulk_read_wire_tests {
    use super::*;

    #[test]
    fn current_read_codec_preserves_bulk_bytes_and_typed_absence() {
        let payload = vec![23; 1024 * 1024];
        let current = DaemonDfsBrowserReadFileResponse::found(payload.clone());
        let encoded = sinomo_core::network::ObjectRpcResponse::encode_body(
            DAEMON_DFS_BROWSER_READ_FILE_ROUTE,
            &current,
        )
        .unwrap();
        assert!(encoded.body.len() < payload.len() + 256);
        assert_eq!(
            encoded
                .decode_body::<DaemonDfsBrowserReadFileResponse>()
                .unwrap()
                .into_result()
                .unwrap(),
            payload
        );
        let absent = DaemonDfsBrowserReadFileResponse::not_found("/missing");
        assert!(matches!(
            absent.into_result(),
            Err(DaemonDfsBrowserReadFileFailure::NotFound { .. })
        ));
        assert!(
            serde_json::from_value::<DaemonDfsBrowserReadFileResponse>(
                serde_json::json!({"bytes": []})
            )
            .is_err()
        );
    }
}

#[cfg(test)]
mod no_link_contract_tests {
    use super::*;

    #[test]
    fn scoped_operation_route_round_trips_exact_mutation_and_typed_absence() {
        assert_ne!(
            DAEMON_DFS_BROWSER_NO_LINKS_ROUTE,
            DAEMON_DFS_BROWSER_STAT_ROUTE
        );
        let payload = DaemonDfsBrowserNoLinksRequest {
            expected_cluster_id: Some(sinomo_core::Id::generate()),
            root: "/users/agent/home/memory".into(),
            path: "/users/agent/home/memory/note.md".into(),
            operation: DaemonDfsBrowserNoLinksOperation::CreateNew {
                bytes: vec![0, 1, 255],
            },
        };
        let rpc = sinomo_core::network::ObjectRpcRequest::encode_body(
            DAEMON_DFS_BROWSER_NO_LINKS_ROUTE,
            crate::DAEMON_RPC_CATEGORY,
            &payload,
        )
        .unwrap();
        let decoded: DaemonDfsBrowserNoLinksRequest = rpc.decode_body().unwrap();
        assert_eq!(decoded, payload);
        assert!(decoded.operation.is_mutation());
        let absent = DaemonDfsBrowserNoLinksResponse::Failed {
            failure: DaemonDfsBrowserNoLinksFailure::NotFound { path: payload.path },
        };
        let rpc = sinomo_core::network::ObjectRpcResponse::encode_body(
            DAEMON_DFS_BROWSER_NO_LINKS_ROUTE,
            &absent,
        )
        .unwrap();
        assert_eq!(
            rpc.decode_body::<DaemonDfsBrowserNoLinksResponse>()
                .unwrap(),
            absent
        );
    }
}

pub const DAEMON_DFS_BROWSER_DURABLE_REPLACE_MAX_BYTES: u64 = 32 * 1024 * 1024;

/// Durable replacement and exact-operation recovery share the same authority path.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct DaemonDfsBrowserDurableReplaceRequest {
    pub expected_node_id: String,
    pub path: String,
    pub expected_object_id: String,
    pub expected_version: String,
    pub operation_id: String,
    pub content_hash: String,
    pub size_bytes: u64,
    /// None requests reconciliation only. Some(empty) writes an empty file.
    pub bytes: Option<Vec<u8>>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum DaemonDfsBrowserDurableReplaceOutcome {
    Applied {
        operation_id: String,
        receipt: DaemonDfsBrowserWriteReceipt,
        maintenance_warning: Option<String>,
    },
    NotApplied,
    Conflict {
        detail: String,
    },
    Unknown {
        detail: String,
    },
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserReadObjectRangeRequest {
    pub object_id: String,
    pub version: String,
    pub content_hash: String,
    pub size_bytes: u64,
    pub offset: u64,
    pub length: u32,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserReadObjectRangeResponse {
    pub request: DaemonDfsBrowserReadObjectRangeRequest,
    pub bytes: Vec<u8>,
}
