//! Remote-only SIFS and DFS clients for graphical frontends.

use std::thread;
use std::time::{Duration, Instant};

use serde::Serialize;
use serde::de::DeserializeOwned;
use sinomo_runtime::network::{NetworkDeliveryState, ObjectRpcRequest};
use sinomo_runtime::{api, dfs};

const READ_RETRY_BUDGET: Duration = Duration::from_secs(3);
const READ_RETRY_INITIAL_DELAY: Duration = Duration::from_millis(25);
const READ_RETRY_MAX_DELAY: Duration = Duration::from_millis(250);

pub type SifsPlacementPolicy = dfs::DfsPlacementPolicy;
pub type SifsPlacementScope = dfs::DfsPlacementScope;
pub type SifsPlacementPreview = dfs::DfsPlacementPreview;
pub type SifsPlacementPreviewToken = dfs::DfsPlacementPreviewToken;
pub type SifsPlacementReconciliationJob = dfs::DfsPlacementReconciliationJob;
pub type SifsPlacementActivityRequest = api::DaemonDfsBrowserPlacementActivityRequest;
pub type SifsPlacementActivityPage = dfs::DfsPlacementReconciliationActivityPage;
pub type SifsReplaceFileIfVersionOutcome = api::DaemonDfsBrowserReplaceFileIfVersionOutcome;
pub type SifsWriteReceipt = api::DaemonDfsBrowserWriteReceipt;

pub type DfsFileSystemId = dfs::DfsFileSystemId;
pub type DfsObjectId = dfs::DfsObjectId;
pub type DfsVersion = dfs::DfsVersion;
pub type DfsOperatorStatus = dfs::DfsOperatorStatus;
pub type DfsOperatorObjectView = dfs::DfsOperatorObjectView;
pub type DfsOperatorInspectionCursor = dfs::DfsOperatorInspectionCursor;
pub type DfsConflictRecord = dfs::DfsConflictRecord;
pub type DfsOperatorConflictPage = dfs::DfsOperatorConflictPage;
pub type DfsOperatorConflictVersionsReport = dfs::DfsOperatorConflictVersionsReport;
pub type DfsOperatorConflictResolutionReport = dfs::DfsOperatorConflictResolutionReport;
pub type DfsOperatorUseConflictVersionReport = dfs::DfsOperatorUseConflictVersionReport;
pub type DfsOperatorRepairView = dfs::DfsOperatorRepairView;
pub type DfsOperatorRepairPage = dfs::DfsOperatorRepairPage;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SifsEntry {
    pub name: String,
    pub path: String,
    pub is_dir: bool,
    pub is_link: bool,
    pub size: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SifsListSortKey {
    #[default]
    Name,
    Kind,
    Size,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SifsListSortDirection {
    #[default]
    Ascending,
    Descending,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SifsListOptions {
    pub sort_key: SifsListSortKey,
    pub sort_direction: SifsListSortDirection,
    pub offset: usize,
    pub limit: Option<usize>,
}

impl Default for SifsListOptions {
    fn default() -> Self {
        Self {
            sort_key: SifsListSortKey::Name,
            sort_direction: SifsListSortDirection::Ascending,
            offset: 0,
            limit: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SifsEntryPage {
    pub entries: Vec<SifsEntry>,
    pub metadata: Vec<SifsEntryMetadata>,
    pub total_count: usize,
    pub offset: usize,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SifsEntryMetadata {
    pub path: String,
    pub object_id: String,
    pub version: String,
    pub modified_at_unix_nanos: u64,
    pub policy: String,
    pub current_locations: Vec<String>,
    pub placement: Option<SifsEntryPlacement>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SifsEntryPlacement {
    pub agent_state: String,
    pub desired_replicas: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SifsObjectDetails {
    pub file_system_id: String,
    pub object_id: String,
    pub path: String,
    pub kind: String,
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
    pub policy: String,
    pub current_locations: Vec<String>,
    pub local_pins: Vec<SifsPinDetails>,
    pub health: Option<SifsObjectHealth>,
    pub metadata_shard: Option<SifsMetadataShardDetails>,
    pub placement: Option<dfs::DfsResolvedPlacementPolicy>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SifsMetadataShardDetails {
    pub shard_id: String,
    pub primary_node_id: String,
    pub replica_node_ids: Vec<String>,
    pub epoch: u64,
    pub local_node_id: String,
    pub local_is_primary: bool,
    pub local_is_replica: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SifsMetadataPrimaryTransferOutcome {
    pub file_system_id: String,
    pub shard_id: String,
    pub previous_primary_node_id: String,
    pub new_primary_node_id: String,
    pub previous_epoch: u64,
    pub new_epoch: u64,
    pub changed: bool,
    pub metadata_shard: SifsMetadataShardDetails,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SifsPinDetails {
    pub pinned_by_node_id: String,
    pub mode: String,
    pub created_at_unix_nanos: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SifsObjectHealth {
    pub agent_state: String,
    pub version_count: usize,
    pub receipt_count: usize,
    pub intent_count: usize,
    pub open_conflict_count: usize,
    pub pending_repair_count: usize,
    pub missing_replica_nodes: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SifsHealthSummary {
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SifsFileRead {
    pub bytes: Vec<u8>,
    pub truncated: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SifsPlacementUpdateOutcome {
    pub preview: SifsPlacementPreview,
    pub reconciliation: SifsPlacementReconciliationJob,
}

pub struct SifsBrowser {
    address: String,
    timeout: Duration,
    runtime: Option<tokio::runtime::Handle>,
    node_id: String,
}

impl SifsBrowser {
    pub fn open_daemon(address: impl Into<String>) -> Result<Self, String> {
        Self::open_daemon_with_timeout(address, sinomo_runtime::network::DEFAULT_NETWORK_TIMEOUT)
    }

    pub fn open_daemon_with_timeout(
        address: impl Into<String>,
        timeout: Duration,
    ) -> Result<Self, String> {
        Self::open_remote(address.into(), timeout, None)
    }

    pub fn open_daemon_on_runtime(
        address: impl Into<String>,
        runtime: &tokio::runtime::Handle,
    ) -> Result<Self, String> {
        Self::open_remote(
            address.into(),
            sinomo_runtime::network::DEFAULT_NETWORK_TIMEOUT,
            Some(runtime.clone()),
        )
    }

    fn open_remote(
        address: String,
        timeout: Duration,
        runtime: Option<tokio::runtime::Handle>,
    ) -> Result<Self, String> {
        let mut client = Self {
            address,
            timeout,
            runtime,
            node_id: String::new(),
        };
        let status: api::DaemonDfsBrowserStatusResponse = client.rpc(
            api::DAEMON_DFS_BROWSER_STATUS_ROUTE,
            &api::DaemonDfsBrowserStatusRequest::node_only(),
        )?;
        client.node_id = status.node_id;
        Ok(client)
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub fn health_summary(&self) -> Result<Option<SifsHealthSummary>, String> {
        let status: api::DaemonDfsBrowserStatusResponse = self.read_rpc(
            api::DAEMON_DFS_BROWSER_STATUS_ROUTE,
            &api::DaemonDfsBrowserStatusRequest::with_health(),
        )?;
        match (status.health, status.health_error) {
            (Some(health), _) => Ok(Some(health_summary(health))),
            (None, Some(error)) => Err(error),
            (None, None) => Ok(None),
        }
    }

    pub fn list(&self, path: &str) -> Result<Vec<SifsEntry>, String> {
        self.list_inner(path, false, false)
    }

    pub fn list_if_exists(&self, path: &str) -> Result<Vec<SifsEntry>, String> {
        self.list_inner(path, false, true)
    }

    pub fn list_shallow(&self, path: &str) -> Result<Vec<SifsEntry>, String> {
        self.list_inner(path, true, false)
    }

    fn list_inner(
        &self,
        path: &str,
        shallow: bool,
        missing_is_empty: bool,
    ) -> Result<Vec<SifsEntry>, String> {
        let response: api::DaemonDfsBrowserListResponse = self.read_rpc(
            api::DAEMON_DFS_BROWSER_LIST_ROUTE,
            &api::DaemonDfsBrowserListRequest {
                path: path.to_string(),
                shallow,
                missing_is_empty,
                sort_key: api::DaemonDfsBrowserSortKey::Name,
                sort_direction: api::DaemonDfsBrowserSortDirection::Ascending,
                offset: 0,
                limit: None,
                include_metadata: false,
            },
        )?;
        Ok(response.entries.into_iter().map(entry_from_rpc).collect())
    }

    pub fn list_shallow_page(
        &self,
        path: &str,
        options: SifsListOptions,
    ) -> Result<SifsEntryPage, String> {
        let response: api::DaemonDfsBrowserListResponse = self.read_rpc(
            api::DAEMON_DFS_BROWSER_LIST_ROUTE,
            &api::DaemonDfsBrowserListRequest {
                path: path.to_string(),
                shallow: true,
                missing_is_empty: false,
                sort_key: match options.sort_key {
                    SifsListSortKey::Name => api::DaemonDfsBrowserSortKey::Name,
                    SifsListSortKey::Kind => api::DaemonDfsBrowserSortKey::Kind,
                    SifsListSortKey::Size => api::DaemonDfsBrowserSortKey::Size,
                },
                sort_direction: match options.sort_direction {
                    SifsListSortDirection::Ascending => {
                        api::DaemonDfsBrowserSortDirection::Ascending
                    }
                    SifsListSortDirection::Descending => {
                        api::DaemonDfsBrowserSortDirection::Descending
                    }
                },
                offset: options.offset,
                limit: options.limit,
                include_metadata: true,
            },
        )?;
        let mut metadata = Vec::new();
        let entries = response
            .entries
            .into_iter()
            .map(|entry| {
                if let Some(value) = entry_metadata_from_rpc(&entry) {
                    metadata.push(value);
                }
                entry_from_rpc(entry)
            })
            .collect();
        Ok(SifsEntryPage {
            entries,
            metadata,
            total_count: response.total_count,
            offset: response.offset,
            limit: response.limit,
        })
    }

    pub fn stat(&self, path: &str) -> Result<SifsObjectDetails, String> {
        let response: api::DaemonDfsBrowserStatOutcomeResponse = self.read_rpc(
            api::DAEMON_DFS_BROWSER_STAT_ROUTE,
            &api::DaemonDfsBrowserStatRequest {
                path: path.to_string(),
                max_staleness_millis: 250,
            },
        )?;
        response
            .into_result()
            .map(details_from_rpc)
            .map_err(|error| error.to_string())
    }

    pub fn read_file(&self, path: &str) -> Result<Vec<u8>, String> {
        let response: api::DaemonDfsBrowserReadFileResponse = self.read_rpc(
            api::DAEMON_DFS_BROWSER_READ_FILE_ROUTE,
            &api::DaemonDfsBrowserReadFileRequest {
                path: path.to_string(),
            },
        )?;
        response.into_result().map_err(|error| error.to_string())
    }

    pub fn read_object_version(
        &self,
        object_id: &str,
        version: &str,
        content_hash: &str,
        size_bytes: u64,
    ) -> Result<Vec<u8>, String> {
        let response: api::DaemonDfsBrowserReadObjectVersionResponse = self.read_rpc(
            api::DAEMON_DFS_BROWSER_READ_OBJECT_VERSION_ROUTE,
            &api::DaemonDfsBrowserReadObjectVersionRequest {
                object_id: object_id.to_string(),
                version: version.to_string(),
                content_hash: content_hash.to_string(),
                size_bytes: Some(size_bytes),
            },
        )?;
        Ok(response.bytes)
    }

    pub fn read_file_prefix(&self, path: &str, max_bytes: u64) -> Result<SifsFileRead, String> {
        let response: api::DaemonDfsBrowserReadFilePrefixResponse = self.read_rpc(
            api::DAEMON_DFS_BROWSER_READ_FILE_PREFIX_ROUTE,
            &api::DaemonDfsBrowserReadFilePrefixRequest {
                path: path.to_string(),
                max_bytes,
            },
        )?;
        let (bytes, truncated) = response.into_result().map_err(|error| error.to_string())?;
        Ok(SifsFileRead { bytes, truncated })
    }

    pub fn read_files(
        &self,
        paths: &[String],
    ) -> Result<Vec<(String, Result<Vec<u8>, String>)>, String> {
        let response: api::DaemonDfsBrowserReadFilesResponse = self.read_rpc(
            api::DAEMON_DFS_BROWSER_READ_FILES_ROUTE,
            &api::DaemonDfsBrowserReadFilesRequest {
                paths: paths.to_vec(),
            },
        )?;
        Ok(response
            .files
            .into_iter()
            .map(|file| {
                let path = file.path.clone();
                (path, file.into_result().map_err(|error| error.to_string()))
            })
            .collect())
    }

    pub fn write_file(&self, path: &str, bytes: &[u8]) -> Result<(), String> {
        let _: api::DaemonDfsBrowserWriteFileResponse = self.rpc(
            api::DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE,
            &api::DaemonDfsBrowserWriteFileRequest {
                path: path.to_string(),
                bytes: bytes.to_vec(),
            },
        )?;
        Ok(())
    }

    pub fn replace_file_if_version(
        &self,
        path: &str,
        expected_object_id: &str,
        expected_version: &str,
        bytes: &[u8],
    ) -> Result<SifsReplaceFileIfVersionOutcome, String> {
        let request = ObjectRpcRequest::encode_body(
            api::DAEMON_DFS_BROWSER_REPLACE_FILE_IF_VERSION_ROUTE,
            api::DAEMON_RPC_CATEGORY,
            &api::DaemonDfsBrowserReplaceFileIfVersionRequest {
                path: path.to_string(),
                bytes: bytes.to_vec(),
                expected_object_id: expected_object_id.to_string(),
                expected_version: expected_version.to_string(),
            },
        )
        .map_err(|error| error.to_string())?;
        let response = self.send(&request);
        match response {
            Ok(response) => {
                response.error_detail().map_err(|error| error.to_string())?;
                match response.decode_body::<api::DaemonDfsBrowserReplaceFileIfVersionResponse>() {
                    Ok(response) => Ok(response.outcome),
                    Err(error) => self.reconcile_replace(
                        path,
                        bytes,
                        &format!("the daemon returned an unreadable success response: {error}"),
                    ),
                }
            }
            Err(error) if error.delivery_state() == Some(NetworkDeliveryState::PossiblyApplied) => {
                self.reconcile_replace(path, bytes, &error.to_string())
            }
            Err(error) => Err(error.to_string()),
        }
    }

    fn reconcile_replace(
        &self,
        path: &str,
        requested_bytes: &[u8],
        original_error: &str,
    ) -> Result<SifsReplaceFileIfVersionOutcome, String> {
        let details = self.stat(path)?;
        let content_hash = details
            .content_hash
            .as_deref()
            .ok_or_else(|| "the current version has no content hash".to_string())?;
        let size = details
            .size_bytes
            .ok_or_else(|| "the current version has no size".to_string())?;
        let current =
            self.read_object_version(&details.object_id, &details.version, content_hash, size)?;
        if current != requested_bytes {
            return Err(format!(
                "conditional SIFS save outcome is unknown: {original_error}; the current immutable version differs from the requested contents"
            ));
        }
        Ok(SifsReplaceFileIfVersionOutcome::Committed {
            receipt: SifsWriteReceipt {
                object_id: details.object_id,
                version: details.version,
                content_hash: details.content_hash,
                size_bytes: details.size_bytes,
            },
            maintenance_warning: Some(format!(
                "the daemon response was not received, but the current immutable SIFS version was verified byte-for-byte: {original_error}"
            )),
        })
    }

    pub fn create_dir_all(&self, path: &str) -> Result<(), String> {
        let _: api::DaemonDfsBrowserCreateDirAllResponse = self.rpc(
            api::DAEMON_DFS_BROWSER_CREATE_DIR_ALL_ROUTE,
            &api::DaemonDfsBrowserCreateDirAllRequest {
                path: path.to_string(),
            },
        )?;
        Ok(())
    }

    pub fn create_file(&self, path: &str) -> Result<(), String> {
        self.write_file(path, &[])
    }

    pub fn create_dir(&self, path: &str) -> Result<(), String> {
        self.create_dir_all(path)
    }

    pub fn rename(&self, from: &str, to: &str) -> Result<(), String> {
        let _: api::DaemonDfsBrowserRenameResponse = self.rpc(
            api::DAEMON_DFS_BROWSER_RENAME_ROUTE,
            &api::DaemonDfsBrowserRenameRequest {
                from: from.to_string(),
                to: to.to_string(),
            },
        )?;
        Ok(())
    }

    pub fn delete(&self, path: &str, directory: bool) -> Result<(), String> {
        let _: api::DaemonDfsBrowserDeleteResponse = self.rpc(
            api::DAEMON_DFS_BROWSER_DELETE_ROUTE,
            &api::DaemonDfsBrowserDeleteRequest {
                path: path.to_string(),
                directory,
            },
        )?;
        Ok(())
    }

    pub fn request_maintenance_refresh(&self) -> Result<(), String> {
        let _: api::DaemonClusterMaintenanceRefreshResponse = self.rpc(
            api::DAEMON_CLUSTER_MAINTENANCE_REFRESH_ROUTE,
            &api::DaemonClusterMaintenanceRefreshRequest,
        )?;
        Ok(())
    }

    pub fn pin_content(&self, path: &str) -> Result<String, String> {
        let response: api::DaemonDfsBrowserPinContentResponse = self.rpc(
            api::DAEMON_DFS_BROWSER_PIN_CONTENT_ROUTE,
            &api::DaemonDfsBrowserPinContentRequest {
                path: path.to_string(),
            },
        )?;
        Ok(response.node_id)
    }

    pub fn unpin_content(&self, path: &str) -> Result<bool, String> {
        let response: api::DaemonDfsBrowserUnpinContentResponse = self.rpc(
            api::DAEMON_DFS_BROWSER_UNPIN_CONTENT_ROUTE,
            &api::DaemonDfsBrowserUnpinContentRequest {
                path: path.to_string(),
            },
        )?;
        Ok(response.removed)
    }

    pub fn pin_metadata(&self, path: &str) -> Result<String, String> {
        let response: api::DaemonDfsBrowserPinMetadataResponse = self.rpc(
            api::DAEMON_DFS_BROWSER_PIN_METADATA_ROUTE,
            &api::DaemonDfsBrowserPinMetadataRequest {
                path: path.to_string(),
            },
        )?;
        Ok(response.node_id)
    }

    pub fn unpin_metadata(&self, path: &str) -> Result<bool, String> {
        let response: api::DaemonDfsBrowserUnpinMetadataResponse = self.rpc(
            api::DAEMON_DFS_BROWSER_UNPIN_METADATA_ROUTE,
            &api::DaemonDfsBrowserUnpinMetadataRequest {
                path: path.to_string(),
            },
        )?;
        Ok(response.removed)
    }

    pub fn make_metadata_primary(
        &self,
        path: &str,
    ) -> Result<SifsMetadataPrimaryTransferOutcome, String> {
        let response: api::DaemonDfsBrowserTransferMetadataPrimaryResponse = self.rpc(
            api::DAEMON_DFS_BROWSER_TRANSFER_METADATA_PRIMARY_ROUTE,
            &api::DaemonDfsBrowserTransferMetadataPrimaryRequest {
                path: path.to_string(),
                expected_epoch: None,
            },
        )?;
        Ok(transfer_from_rpc(response))
    }

    pub fn preview_placement(
        &self,
        path: &str,
        policy: Option<SifsPlacementPolicy>,
        scope: SifsPlacementScope,
    ) -> Result<SifsPlacementPreview, String> {
        let response: api::DaemonDfsBrowserPlacementPreviewResponse = self.read_rpc(
            api::DAEMON_DFS_BROWSER_PLACEMENT_PREVIEW_ROUTE,
            &api::DaemonDfsBrowserPlacementPreviewRequest {
                path: path.to_string(),
                policy,
                scope,
            },
        )?;
        Ok(response.preview)
    }

    pub fn update_placement(
        &self,
        path: &str,
        policy: Option<SifsPlacementPolicy>,
        scope: SifsPlacementScope,
        expected_preview_token: SifsPlacementPreviewToken,
    ) -> Result<SifsPlacementUpdateOutcome, String> {
        let response: api::DaemonDfsBrowserPlacementUpdateResponse = self.rpc(
            api::DAEMON_DFS_BROWSER_PLACEMENT_UPDATE_V2_ROUTE,
            &api::DaemonDfsBrowserPlacementUpdateV2Request {
                path: path.to_string(),
                policy,
                scope,
                expected_preview_token,
            },
        )?;
        Ok(SifsPlacementUpdateOutcome {
            preview: response.preview,
            reconciliation: response.reconciliation,
        })
    }

    pub fn placement_progress(
        &self,
        job_id: &str,
    ) -> Result<SifsPlacementReconciliationJob, String> {
        let response: api::DaemonDfsBrowserPlacementProgressResponse = self.read_rpc(
            api::DAEMON_DFS_BROWSER_PLACEMENT_PROGRESS_ROUTE,
            &api::DaemonDfsBrowserPlacementProgressRequest {
                job_id: job_id.to_string(),
            },
        )?;
        Ok(response.reconciliation)
    }

    pub fn placement_activity(
        &self,
        request: SifsPlacementActivityRequest,
    ) -> Result<SifsPlacementActivityPage, String> {
        let response: api::DaemonDfsBrowserPlacementActivityResponse =
            self.read_rpc(api::DAEMON_DFS_BROWSER_PLACEMENT_ACTIVITY_ROUTE, &request)?;
        Ok(response.page)
    }

    pub fn parent(&self, path: &str) -> Option<String> {
        let path = normalize_path(path).ok()?;
        if path == "/" {
            return None;
        }
        let (parent, _) = path.rsplit_once('/')?;
        Some(if parent.is_empty() { "/" } else { parent }.to_string())
    }

    pub fn normalize(&self, path: &str) -> String {
        normalize_path(path).unwrap_or_else(|_| "/".to_string())
    }

    fn read_rpc<Req, Resp>(&self, route: &str, body: &Req) -> Result<Resp, String>
    where
        Req: Serialize + ?Sized,
        Resp: DeserializeOwned,
    {
        retry_read(|| self.rpc(route, body))
    }

    fn rpc<Req, Resp>(&self, route: &str, body: &Req) -> Result<Resp, String>
    where
        Req: Serialize + ?Sized,
        Resp: DeserializeOwned,
    {
        let request = ObjectRpcRequest::encode_body(route, api::DAEMON_RPC_CATEGORY, body)
            .map_err(|error| error.to_string())?;
        self.send(&request)
            .map_err(|error| error.to_string())?
            .decode_body()
            .map_err(|error| error.to_string())
    }

    fn send(
        &self,
        request: &ObjectRpcRequest,
    ) -> Result<sinomo_runtime::network::ObjectRpcResponse, api::DaemonApiError> {
        if let Some(runtime) = &self.runtime {
            api::send_daemon_rpc_on_runtime(&self.address, request, self.timeout, runtime)
        } else {
            api::send_daemon_rpc(&self.address, request, self.timeout)
        }
    }
}

#[derive(Clone)]
pub struct DfsOperatorClient {
    daemon: api::DaemonApiClient,
}

impl DfsOperatorClient {
    pub fn open_daemon_on_runtime(
        address: impl Into<String>,
        _runtime: &tokio::runtime::Handle,
    ) -> Result<Self, String> {
        let client = Self {
            daemon: api::DaemonApiClient::at_address(address),
        };
        client.status()?;
        Ok(client)
    }

    pub fn status(&self) -> Result<DfsOperatorStatus, String> {
        match self.rpc(dfs::DaemonDfsInspectionRequest::Status)? {
            dfs::DaemonDfsInspectionResponse::Status { status } => Ok(status),
            response => Err(unexpected_operator_response("status", &response)),
        }
    }

    pub fn object_by_path(
        &self,
        file_system_id: DfsFileSystemId,
        path: impl Into<String>,
    ) -> Result<Option<DfsOperatorObjectView>, String> {
        match self.rpc(dfs::DaemonDfsInspectionRequest::Path {
            file_system_id,
            path: path.into(),
        })? {
            dfs::DaemonDfsInspectionResponse::Path { object } => Ok(object),
            response => Err(unexpected_operator_response("path", &response)),
        }
    }

    pub fn object(&self, object_id: DfsObjectId) -> Result<Option<DfsOperatorObjectView>, String> {
        match self.rpc(dfs::DaemonDfsInspectionRequest::Object { object_id })? {
            dfs::DaemonDfsInspectionResponse::Object { object } => Ok(object),
            response => Err(unexpected_operator_response("object", &response)),
        }
    }

    pub fn conflicts_page(
        &self,
        file_system_id: Option<DfsFileSystemId>,
        object_id: Option<DfsObjectId>,
        limit: usize,
        cursor: Option<DfsOperatorInspectionCursor>,
    ) -> Result<DfsOperatorConflictPage, String> {
        match self.rpc(dfs::DaemonDfsInspectionRequest::Conflicts {
            file_system_id,
            object_id,
            limit: Some(limit),
            cursor,
        })? {
            dfs::DaemonDfsInspectionResponse::Conflicts {
                conflicts,
                next_cursor,
            } => Ok(DfsOperatorConflictPage {
                conflicts,
                next_cursor,
            }),
            response => Err(unexpected_operator_response("conflicts", &response)),
        }
    }

    pub fn conflict_versions(
        &self,
        object_id: DfsObjectId,
        conflict_ids: Vec<String>,
    ) -> Result<Option<DfsOperatorConflictVersionsReport>, String> {
        match self.rpc(dfs::DaemonDfsInspectionRequest::ConflictVersions {
            object_id,
            conflict_ids,
        })? {
            dfs::DaemonDfsInspectionResponse::ConflictVersions { report } => Ok(report),
            response => Err(unexpected_operator_response("conflict-versions", &response)),
        }
    }

    pub fn resolve_conflicts(
        &self,
        file_system_id: Option<DfsFileSystemId>,
        object_id: Option<DfsObjectId>,
        conflict_ids: Vec<String>,
    ) -> Result<DfsOperatorConflictResolutionReport, String> {
        match self.rpc(dfs::DaemonDfsInspectionRequest::ResolveConflicts {
            file_system_id,
            object_id,
            conflict_ids,
        })? {
            dfs::DaemonDfsInspectionResponse::ResolveConflicts { report } => Ok(report),
            response => Err(unexpected_operator_response("resolve-conflicts", &response)),
        }
    }

    pub fn use_conflict_version(
        &self,
        object_id: DfsObjectId,
        version: DfsVersion,
        conflict_ids: Vec<String>,
    ) -> Result<DfsOperatorUseConflictVersionReport, String> {
        match self.rpc(dfs::DaemonDfsInspectionRequest::UseConflictVersion {
            object_id,
            version,
            conflict_ids,
        })? {
            dfs::DaemonDfsInspectionResponse::UseConflictVersion { report } => Ok(report),
            response => Err(unexpected_operator_response(
                "use-conflict-version",
                &response,
            )),
        }
    }

    pub fn repair_page(
        &self,
        file_system_id: Option<DfsFileSystemId>,
        object_id: Option<DfsObjectId>,
        limit: usize,
        cursor: Option<DfsOperatorInspectionCursor>,
    ) -> Result<DfsOperatorRepairPage, String> {
        match self.rpc(dfs::DaemonDfsInspectionRequest::Repair {
            file_system_id,
            object_id,
            limit: Some(limit),
            cursor,
        })? {
            dfs::DaemonDfsInspectionResponse::Repair {
                repairs,
                next_cursor,
            } => Ok(DfsOperatorRepairPage {
                repairs,
                next_cursor,
            }),
            response => Err(unexpected_operator_response("repair", &response)),
        }
    }

    fn rpc(
        &self,
        body: dfs::DaemonDfsInspectionRequest,
    ) -> Result<dfs::DaemonDfsInspectionResponse, String> {
        let request = dfs::dfs_inspection_rpc_request(&body).map_err(|error| error.to_string())?;
        self.daemon
            .call_request(&request)
            .map_err(|error| error.to_string())
            .and_then(|response| {
                dfs::decode_dfs_inspection_rpc_response(&response)
                    .map_err(|error| error.to_string())
            })
    }
}

fn entry_from_rpc(entry: api::DaemonDfsBrowserEntry) -> SifsEntry {
    SifsEntry {
        name: entry.name,
        path: entry.path,
        is_dir: matches!(entry.kind, dfs::DfsObjectKind::Directory),
        is_link: matches!(entry.kind, dfs::DfsObjectKind::Symlink),
        size: entry.size,
    }
}

fn entry_metadata_from_rpc(entry: &api::DaemonDfsBrowserEntry) -> Option<SifsEntryMetadata> {
    let metadata = entry.metadata.as_ref()?;
    Some(SifsEntryMetadata {
        path: entry.path.clone(),
        object_id: metadata.object_id.clone(),
        version: metadata.version.clone(),
        modified_at_unix_nanos: metadata.modified_at_unix_nanos,
        policy: policy_label(&metadata.policy).to_string(),
        current_locations: metadata.current_locations.clone(),
        placement: metadata
            .placement
            .as_ref()
            .map(|placement| SifsEntryPlacement {
                agent_state: agent_state_label(&placement.agent_state).to_string(),
                desired_replicas: placement.desired_replicas,
            }),
    })
}

fn details_from_rpc(response: api::DaemonDfsBrowserStatResponse) -> SifsObjectDetails {
    SifsObjectDetails {
        file_system_id: response.file_system_id,
        object_id: response.object_id,
        path: response.path,
        kind: kind_label(&response.kind).to_string(),
        version: response.version,
        content_hash: response.content_hash,
        size_bytes: response.size_bytes,
        link_count: response.link_count,
        mode: response.mode,
        uid: response.uid,
        gid: response.gid,
        accessed_at_unix_nanos: response.accessed_at_unix_nanos,
        modified_at_unix_nanos: response.modified_at_unix_nanos,
        created_at_unix_nanos: response.created_at_unix_nanos,
        changed_at_unix_nanos: response.changed_at_unix_nanos,
        policy: policy_label(&response.policy).to_string(),
        current_locations: response.current_locations,
        local_pins: response.local_pins.into_iter().map(pin_from_rpc).collect(),
        health: response.health.map(health_from_rpc),
        metadata_shard: response.metadata_shard.map(shard_from_rpc),
        placement: response.placement,
    }
}

fn shard_from_rpc(shard: api::DaemonDfsBrowserMetadataShard) -> SifsMetadataShardDetails {
    SifsMetadataShardDetails {
        shard_id: shard.shard_id,
        primary_node_id: shard.primary_node_id,
        replica_node_ids: shard.replica_node_ids,
        epoch: shard.epoch,
        local_node_id: shard.local_node_id,
        local_is_primary: shard.local_is_primary,
        local_is_replica: shard.local_is_replica,
    }
}

fn transfer_from_rpc(
    response: api::DaemonDfsBrowserTransferMetadataPrimaryResponse,
) -> SifsMetadataPrimaryTransferOutcome {
    SifsMetadataPrimaryTransferOutcome {
        file_system_id: response.file_system_id,
        shard_id: response.shard_id,
        previous_primary_node_id: response.previous_primary_node_id,
        new_primary_node_id: response.new_primary_node_id,
        previous_epoch: response.previous_epoch,
        new_epoch: response.new_epoch,
        changed: response.changed,
        metadata_shard: shard_from_rpc(response.metadata_shard),
    }
}

fn pin_from_rpc(pin: api::DaemonDfsBrowserPinRecord) -> SifsPinDetails {
    SifsPinDetails {
        pinned_by_node_id: pin.pinned_by_node_id,
        mode: match pin.mode {
            dfs::DfsPinMode::Metadata => "metadata",
            dfs::DfsPinMode::Content => "content",
        }
        .to_string(),
        created_at_unix_nanos: pin.created_at_unix_nanos,
    }
}

fn health_from_rpc(health: api::DaemonDfsBrowserObjectHealth) -> SifsObjectHealth {
    SifsObjectHealth {
        agent_state: agent_state_label(&health.agent_state).to_string(),
        version_count: health.version_count,
        receipt_count: health.receipt_count,
        intent_count: health.intent_count,
        open_conflict_count: health.open_conflict_count,
        pending_repair_count: health.pending_repair_count,
        missing_replica_nodes: health.missing_replica_nodes,
    }
}

fn health_summary(health: api::DaemonDfsBrowserHealthSummary) -> SifsHealthSummary {
    SifsHealthSummary {
        file_system_count: health.file_system_count,
        object_count: health.object_count,
        version_count: health.version_count,
        pending_write_count: health.pending_write_count,
        open_conflict_count: health.open_conflict_count,
        pending_repair_count: health.pending_repair_count,
        pending_namespace_job_count: health.pending_namespace_job_count,
        failed_namespace_job_count: health.failed_namespace_job_count,
        metadata_shard_count: health.metadata_shard_count,
        repair_attempt_count: health.repair_attempt_count,
    }
}

fn kind_label(kind: &dfs::DfsObjectKind) -> &'static str {
    match kind {
        dfs::DfsObjectKind::Directory => "directory",
        dfs::DfsObjectKind::File => "file",
        dfs::DfsObjectKind::Symlink => "symlink",
        dfs::DfsObjectKind::Hardlink => "hardlink",
        dfs::DfsObjectKind::Artifact => "artifact",
        dfs::DfsObjectKind::Mount => "mount",
        dfs::DfsObjectKind::Tombstone => "tombstone",
    }
}

fn policy_label(policy: &sinomo_runtime::core::DfsObjectPolicy) -> &'static str {
    match policy {
        sinomo_runtime::core::DfsObjectPolicy::LocalOwned => "local-owned",
        sinomo_runtime::core::DfsObjectPolicy::ReplicatedConfirmed => "replicated-confirmed",
        sinomo_runtime::core::DfsObjectPolicy::ReplicatedEventual => "replicated-eventual",
        sinomo_runtime::core::DfsObjectPolicy::AppendOnly => "append-only",
        sinomo_runtime::core::DfsObjectPolicy::EphemeralLocal => "ephemeral-local",
    }
}

fn agent_state_label(state: &dfs::DfsAgentObjectState) -> &'static str {
    match state {
        dfs::DfsAgentObjectState::Current => "current",
        dfs::DfsAgentObjectState::PendingWrite => "pending-write",
        dfs::DfsAgentObjectState::PendingRepair => "pending-repair",
        dfs::DfsAgentObjectState::Conflicted => "conflicted",
        dfs::DfsAgentObjectState::MetadataOnly => "metadata-only",
    }
}

fn retry_read<T>(mut work: impl FnMut() -> Result<T, String>) -> Result<T, String> {
    let started = Instant::now();
    let mut delay = READ_RETRY_INITIAL_DELAY;
    loop {
        match work() {
            Ok(value) => return Ok(value),
            Err(error) if read_should_retry(&error) => {
                let Some(remaining) = READ_RETRY_BUDGET.checked_sub(started.elapsed()) else {
                    return Err(error);
                };
                if remaining.is_zero() {
                    return Err(error);
                }
                thread::sleep(delay.min(remaining));
                delay = (delay * 2).min(READ_RETRY_MAX_DELAY);
            }
            Err(error) => return Err(error),
        }
    }
}

fn read_should_retry(error: &str) -> bool {
    (error.contains("DFS browser service is busy") && error.contains("retry the read"))
        || (error.contains("object rpc route /daemon/dfs/browser/")
            && error.contains(" is saturated:"))
}

fn normalize_path(path: &str) -> Result<String, String> {
    let path = path.trim();
    let path = if path.is_empty() {
        "/".to_string()
    } else if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    };
    if path.bytes().any(|byte| byte == 0) {
        return Err(format!("invalid SIFS path: {path}"));
    }
    let mut parts = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            value => parts.push(value),
        }
    }
    Ok(if parts.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", parts.join("/"))
    })
}

fn unexpected_operator_response(
    expected: &str,
    response: &dfs::DaemonDfsInspectionResponse,
) -> String {
    format!("daemon returned unexpected DFS response to {expected} request: {response:?}")
}
