//! DFS browser and placement-authority wire contracts.

pub const DAEMON_DFS_PLACEMENT_AUTHORITY_ADMIN_ROUTE: &str = "/daemon/dfs/placement-authority";
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
    pub kind: crate::DfsObjectKind,
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
    pub agent_state: crate::DfsAgentObjectState,
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
pub struct DaemonDfsBrowserStatResponse {
    pub file_system_id: String,
    pub object_id: String,
    pub path: String,
    pub kind: crate::DfsObjectKind,
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
    pub placement: Option<crate::DfsResolvedPlacementPolicy>,
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
    pub mode: crate::DfsPinMode,
    pub created_at_unix_nanos: Option<u64>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserObjectHealth {
    pub agent_state: crate::DfsAgentObjectState,
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
    /// Present only when the exact authoritative content version was read.
    ///
    /// Keeping the successful wire shape as `{ "bytes": [...] }` preserves
    /// compatibility with older clients. Failure responses omit `bytes`, so an
    /// older client fails decoding instead of mistaking unavailability for an
    /// empty file.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_present_read_file_bytes",
        deserialize_with = "deserialize_present_read_file_bytes"
    )]
    pub bytes: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<DaemonDfsBrowserReadFileFailure>,
}

fn serialize_present_read_file_bytes<S>(
    bytes: &Option<Vec<u8>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let bytes = bytes.as_ref().ok_or_else(|| {
        <S::Error as serde::ser::Error>::custom(
            "read-file bytes serializer called for an absent field",
        )
    })?;
    serde::Serialize::serialize(bytes, serializer)
}

fn deserialize_present_read_file_bytes<'de, D>(deserializer: D) -> Result<Option<Vec<u8>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    serde::Deserialize::deserialize(deserializer).map(Some)
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

/// Typed operational result for the existing DFS browser stat route.
///
/// The untagged successful variant serializes exactly as the legacy
/// [`DaemonDfsBrowserStatResponse`]. A failure omits every legacy success
/// field, so an older client fails decoding rather than inferring absence from
/// an opaque service error.
#[derive(Clone, Debug, serde::Serialize, Eq, PartialEq)]
#[serde(untagged)]
pub enum DaemonDfsBrowserStatOutcomeResponse {
    Found(DaemonDfsBrowserStatResponse),
    Failed {
        failure: DaemonDfsBrowserReadFileFailure,
    },
}

#[derive(serde::Deserialize)]
struct DaemonDfsBrowserStatFailureResponse {
    failure: DaemonDfsBrowserReadFileFailure,
}

impl<'de> serde::Deserialize<'de> for DaemonDfsBrowserStatOutcomeResponse {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = <serde_json::Value as serde::Deserialize>::deserialize(deserializer)?;
        let object = value.as_object().ok_or_else(|| {
            <D::Error as serde::de::Error>::custom("daemon SIFS stat response must be an object")
        })?;
        let has_failure = object.contains_key("failure");
        let has_success = [
            "file_system_id",
            "object_id",
            "path",
            "kind",
            "version",
            "content_hash",
            "size_bytes",
            "link_count",
            "mode",
            "uid",
            "gid",
            "accessed_at_unix_nanos",
            "modified_at_unix_nanos",
            "created_at_unix_nanos",
            "changed_at_unix_nanos",
            "policy",
            "current_locations",
            "local_pins",
            "health",
            "metadata_shard",
            "placement",
        ]
        .iter()
        .any(|field| object.contains_key(*field));

        match (has_success, has_failure) {
            (true, false) => serde_json::from_value::<DaemonDfsBrowserStatResponse>(value)
                .map(Self::Found)
                .map_err(serde::de::Error::custom),
            (false, true) => serde_json::from_value::<DaemonDfsBrowserStatFailureResponse>(value)
                .map(|response| Self::Failed {
                    failure: response.failure,
                })
                .map_err(serde::de::Error::custom),
            (true, true) => Err(<D::Error as serde::de::Error>::custom(
                "daemon SIFS stat response contains both success fields and a failure",
            )),
            (false, false) => Err(<D::Error as serde::de::Error>::custom(
                "daemon SIFS stat response contains neither success fields nor a failure",
            )),
        }
    }
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
        Self {
            bytes: Some(bytes),
            failure: None,
        }
    }

    pub fn not_found(path: impl Into<String>) -> Self {
        Self {
            bytes: None,
            failure: Some(DaemonDfsBrowserReadFileFailure::NotFound { path: path.into() }),
        }
    }

    pub fn content_unavailable(path: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            bytes: None,
            failure: Some(DaemonDfsBrowserReadFileFailure::ContentUnavailable {
                path: path.into(),
                detail: detail.into(),
            }),
        }
    }

    pub fn into_result(self) -> Result<Vec<u8>, DaemonDfsBrowserReadFileFailure> {
        match (self.bytes, self.failure) {
            (Some(bytes), None) => Ok(bytes),
            (None, Some(failure)) => Err(failure),
            (Some(_), Some(_)) => Err(DaemonDfsBrowserReadFileFailure::InvalidResponse {
                detail: "response contains both bytes and a failure".to_string(),
            }),
            (None, None) => Err(DaemonDfsBrowserReadFileFailure::InvalidResponse {
                detail: "response contains neither bytes nor a failure".to_string(),
            }),
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
pub struct DaemonDfsBrowserReadFilePrefixResponse {
    /// Successful responses preserve the legacy `{ bytes, truncated }` wire
    /// shape. Typed failures omit both legacy success fields, forcing older
    /// clients to fail decoding rather than treating unavailable content as
    /// an empty prefix.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_present_read_file_bytes",
        deserialize_with = "deserialize_present_read_file_bytes"
    )]
    pub bytes: Option<Vec<u8>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_present_prefix_truncated",
        deserialize_with = "deserialize_present_prefix_truncated"
    )]
    pub truncated: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<DaemonDfsBrowserReadFileFailure>,
}

fn serialize_present_prefix_truncated<S>(
    truncated: &Option<bool>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let truncated = truncated.ok_or_else(|| {
        <S::Error as serde::ser::Error>::custom(
            "prefix-read truncated serializer called for an absent field",
        )
    })?;
    serde::Serialize::serialize(&truncated, serializer)
}

fn deserialize_present_prefix_truncated<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    serde::Deserialize::deserialize(deserializer).map(Some)
}

impl DaemonDfsBrowserReadFilePrefixResponse {
    pub fn found(bytes: Vec<u8>, truncated: bool) -> Self {
        Self {
            bytes: Some(bytes),
            truncated: Some(truncated),
            failure: None,
        }
    }

    pub fn not_found(path: impl Into<String>) -> Self {
        Self {
            bytes: None,
            truncated: None,
            failure: Some(DaemonDfsBrowserReadFileFailure::NotFound { path: path.into() }),
        }
    }

    pub fn content_unavailable(path: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            bytes: None,
            truncated: None,
            failure: Some(DaemonDfsBrowserReadFileFailure::ContentUnavailable {
                path: path.into(),
                detail: detail.into(),
            }),
        }
    }

    pub fn into_result(self) -> Result<(Vec<u8>, bool), DaemonDfsBrowserReadFileFailure> {
        match (self.bytes, self.truncated, self.failure) {
            (Some(bytes), Some(truncated), None) => Ok((bytes, truncated)),
            (None, None, Some(failure)) => Err(failure),
            (bytes, truncated, failure) => Err(DaemonDfsBrowserReadFileFailure::InvalidResponse {
                detail: format!(
                    "inconsistent prefix-read response (bytes={}, truncated={}, typed_failure={})",
                    bytes.is_some(),
                    truncated.is_some(),
                    failure.is_some()
                ),
            }),
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
pub struct DaemonDfsBrowserFileRead {
    pub path: String,
    #[serde(default)]
    pub bytes: Option<Vec<u8>>,
    /// Legacy daemons reported every per-item failure as an untyped string.
    /// Current readers retain this field only for rolling compatibility and
    /// never infer namespace absence from it.
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<DaemonDfsBrowserReadFileFailure>,
}

impl DaemonDfsBrowserFileRead {
    pub fn found(path: impl Into<String>, bytes: Vec<u8>) -> Self {
        Self {
            path: path.into(),
            bytes: Some(bytes),
            error: None,
            failure: None,
        }
    }

    pub fn not_found(path: impl Into<String>) -> Self {
        let path = path.into();
        Self {
            path: path.clone(),
            bytes: None,
            error: None,
            failure: Some(DaemonDfsBrowserReadFileFailure::NotFound { path }),
        }
    }

    pub fn content_unavailable(path: impl Into<String>, detail: impl Into<String>) -> Self {
        let path = path.into();
        Self {
            path: path.clone(),
            bytes: None,
            error: None,
            failure: Some(DaemonDfsBrowserReadFileFailure::ContentUnavailable {
                path,
                detail: detail.into(),
            }),
        }
    }

    pub fn read_failed(path: impl Into<String>, detail: impl Into<String>) -> Self {
        let path = path.into();
        Self {
            path: path.clone(),
            bytes: None,
            error: None,
            failure: Some(DaemonDfsBrowserReadFileFailure::ReadFailed {
                path,
                detail: detail.into(),
            }),
        }
    }

    pub fn into_result(self) -> Result<Vec<u8>, DaemonDfsBrowserReadFileFailure> {
        match (self.bytes, self.error, self.failure) {
            (Some(bytes), None, None) => Ok(bytes),
            (None, None, Some(failure)) => Err(failure),
            (None, Some(error), None) => Err(DaemonDfsBrowserReadFileFailure::InvalidResponse {
                detail: format!(
                    "legacy daemon returned an unclassified per-file read failure: {error}"
                ),
            }),
            (bytes, error, failure) => Err(DaemonDfsBrowserReadFileFailure::InvalidResponse {
                detail: format!(
                    "inconsistent per-file read response (bytes={}, legacy_error={}, typed_failure={})",
                    bytes.is_some(),
                    error.is_some(),
                    failure.is_some()
                ),
            }),
        }
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
    pub policy: Option<crate::DfsPlacementPolicy>,
    #[serde(default)]
    pub scope: crate::DfsPlacementScope,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserPlacementPreviewResponse {
    pub preview: crate::DfsPlacementPreview,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserPlacementUpdateRequest {
    pub path: String,
    /// `None` removes the override for exactly `(path, scope)`, restoring
    /// inheritance from the nearest matching ancestor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<crate::DfsPlacementPolicy>,
    #[serde(default)]
    pub scope: crate::DfsPlacementScope,
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
    pub policy: Option<crate::DfsPlacementPolicy>,
    #[serde(default)]
    pub scope: crate::DfsPlacementScope,
    pub expected_preview_token: crate::DfsPlacementPreviewToken,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserPlacementUpdateResponse {
    pub preview: crate::DfsPlacementPreview,
    pub reconciliation: crate::DfsPlacementReconciliationJob,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserPlacementProgressRequest {
    pub job_id: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserPlacementProgressResponse {
    pub reconciliation: crate::DfsPlacementReconciliationJob,
}

fn default_daemon_dfs_browser_placement_activity_limit() -> usize {
    crate::DEFAULT_DFS_PLACEMENT_RECONCILIATION_ACTIVITY_PAGE_SIZE
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct DaemonDfsBrowserPlacementActivityRequest {
    /// Empty means every activity status.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub statuses: Vec<crate::DfsPlacementReconciliationActivityStatus>,
    /// Optional normalized SIFS path boundary. `/team` includes `/team/a` but
    /// not `/teammate`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<crate::DfsPlacementReconciliationActivityCursor>,
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
    pub page: crate::DfsPlacementReconciliationActivityPage,
}

/// Main-daemon-only operator surface for inspecting and recovering a stranded
/// DFS placement mutation permit. This route is intentionally separate from
/// the local-command browser API and is cataloged with admin authorization.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
#[serde(tag = "operation", rename_all = "kebab-case")]
pub enum DaemonDfsPlacementAuthorityAdminRequest {
    Inspect {
        file_system_id: crate::DfsFileSystemId,
    },
    RecoverRemovedNode {
        file_system_id: crate::DfsFileSystemId,
        expected_recovery_token: String,
        #[serde(default)]
        acknowledge_owner_stopped: bool,
    },
    BreakGlassRecover {
        file_system_id: crate::DfsFileSystemId,
        expected_recovery_token: String,
        acknowledgement: String,
    },
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
#[serde(tag = "operation", rename_all = "kebab-case")]
pub enum DaemonDfsPlacementAuthorityAdminResponse {
    Inspection {
        permit: Option<crate::DfsPlacementAuthorityPermitStatus>,
    },
    Recovery {
        outcome: crate::DfsPlacementAuthorityRecoveryOutcome,
    },
}
