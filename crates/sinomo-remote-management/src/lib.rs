//! Shared routes and typed wire contracts for Sinomo remote management.
//!
//! This crate intentionally contains no transport, authorization, or host
//! implementation. Callers route these payloads through the daemon, while the
//! target node remains responsible for local permission checks.

mod cli;

pub use cli::{
    NodePermissionsCliCommandHandler, RemoteManagementCliCommandHandler, RemoteManagementCliRpc,
    RemoteManagementCliRpcFuture, node_permissions_cli_command_handler, remote_cli_command_handler,
};

use serde::{Deserialize, Serialize};
pub use sinomo_config::{
    CONFIG_CATALOG_ROUTE, CONFIG_GET_ROUTE, CONFIG_RPC_CATEGORY, CONFIG_SEARCH_ROUTE,
    CONFIG_SET_ROUTE, ConfigCatalogRequest, ConfigCatalogResponse, ConfigDefinition,
    ConfigGetRequest, ConfigGetResponse, ConfigResolution, ConfigSearchRequest,
    ConfigSearchResponse, ConfigSetRequest, ConfigSetResponse, ConfigWriteResult,
    ConfigWriteTarget,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

pub const REMOTE_MANAGEMENT_MODULE_ID: &str = "module.remote-management";
pub const REMOTE_MANAGEMENT_MODULE_OWNER: &str = "sinomo-remote-management";
pub const REMOTE_MANAGEMENT_INTERFACE_VERSION: &str = "sinomo.remote-management.v2";
pub const REMOTE_MANAGEMENT_RPC_CATEGORY: &str = "remote.management";

pub const REMOTE_MANAGEMENT_SHELL_RUN_ROUTE: &str = "/remote-management/shell/run";
pub const REMOTE_MANAGEMENT_FS_READ_ROUTE: &str = "/remote-management/fs/read";
pub const REMOTE_MANAGEMENT_FS_WRITE_ROUTE: &str = "/remote-management/fs/write";
pub const REMOTE_MANAGEMENT_FS_LIST_ROUTE: &str = "/remote-management/fs/list";
pub const REMOTE_MANAGEMENT_FS_STAT_ROUTE: &str = "/remote-management/fs/stat";
pub const REMOTE_MANAGEMENT_SYSTEM_INFO_ROUTE: &str = "/remote-management/system/info";
pub const REMOTE_MANAGEMENT_SYSTEM_OBSERVE_ROUTE: &str = "/remote-management/system/observe";

pub const REMOTE_MANAGEMENT_SHELL_RUN_OPERATION: &str = "remote.management.shell.run";
pub const REMOTE_MANAGEMENT_FS_READ_OPERATION: &str = "remote.management.fs.read";
pub const REMOTE_MANAGEMENT_FS_WRITE_OPERATION: &str = "remote.management.fs.write";
pub const REMOTE_MANAGEMENT_FS_LIST_OPERATION: &str = "remote.management.fs.list";
pub const REMOTE_MANAGEMENT_FS_STAT_OPERATION: &str = "remote.management.fs.stat";
pub const REMOTE_MANAGEMENT_SYSTEM_INFO_OPERATION: &str = "remote.management.system.info.read";
pub const REMOTE_MANAGEMENT_SYSTEM_OBSERVE_OPERATION: &str = "remote.management.system.observe";

pub const REMOTE_MANAGEMENT_OPERATIONS: &[&str] = &[
    REMOTE_MANAGEMENT_SHELL_RUN_OPERATION,
    REMOTE_MANAGEMENT_FS_READ_OPERATION,
    REMOTE_MANAGEMENT_FS_WRITE_OPERATION,
    REMOTE_MANAGEMENT_FS_LIST_OPERATION,
    REMOTE_MANAGEMENT_FS_STAT_OPERATION,
    REMOTE_MANAGEMENT_SYSTEM_INFO_OPERATION,
    REMOTE_MANAGEMENT_SYSTEM_OBSERVE_OPERATION,
];

/// Existing configuration-service routes used by remote-management clients.
/// These are aliases of the authoritative `sinomo-config` contracts, not
/// module-owned proxy routes.
pub const REMOTE_MANAGEMENT_CONFIG_ROUTES: &[&str] = &[
    CONFIG_CATALOG_ROUTE,
    CONFIG_SEARCH_ROUTE,
    CONFIG_GET_ROUTE,
    CONFIG_SET_ROUTE,
];

pub type RemoteManagementConfigCatalogRequest = ConfigCatalogRequest;
pub type RemoteManagementConfigCatalogResponse = ConfigCatalogResponse;
pub type RemoteManagementConfigSearchRequest = ConfigSearchRequest;
pub type RemoteManagementConfigSearchResponse = ConfigSearchResponse;
pub type RemoteManagementConfigGetRequest = ConfigGetRequest;
pub type RemoteManagementConfigGetResponse = ConfigGetResponse;
pub type RemoteManagementConfigSetRequest = ConfigSetRequest;
pub type RemoteManagementConfigSetResponse = ConfigSetResponse;

pub const DEFAULT_COMMAND_TIMEOUT_MS: u64 = 30_000;
pub const MAX_COMMAND_TIMEOUT_MS: u64 = 300_000;
/// Default per-stream shell capture budget. Shell execution is host-equivalent
/// after local consent, but retaining stdout and stderr in an RPC value must
/// remain an explicit memory policy rather than an accidental wire limit.
pub const DEFAULT_SHELL_CAPTURE_BYTES: usize = 256 * 1024;
pub const MAX_SHELL_CAPTURE_BYTES: usize = 4 * 1024 * 1024;
pub const DEFAULT_LIST_ENTRIES: usize = 512;
pub const MAX_LIST_ENTRIES: usize = 4096;
pub const DEFAULT_OBSERVED_PROCESSES: usize = 64;
pub const MAX_OBSERVED_PROCESSES: usize = 512;
pub const MAX_OBSERVED_RESOURCE_EXPOSURES: usize = 512;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RemoteManagementError {
    pub code: String,
    pub detail: String,
}

impl RemoteManagementError {
    pub fn new(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            detail: detail.into(),
        }
    }

    pub fn missing_payload(payload: &str) -> Self {
        Self::new(
            "missing-response-payload",
            format!("remote-management response did not contain {payload}"),
        )
    }

    pub fn code(&self) -> &str {
        &self.code
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for RemoteManagementError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.detail)
    }
}

impl std::error::Error for RemoteManagementError {}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RemoteManagementProtocolError {
    #[error("{field} must be non-empty")]
    EmptyField { field: &'static str },
    #[error("expected BLAKE3 digest must be 64 hexadecimal characters")]
    InvalidBlake3,
}

fn require_non_empty(
    value: &str,
    field: &'static str,
) -> Result<(), RemoteManagementProtocolError> {
    if value.trim().is_empty() {
        Err(RemoteManagementProtocolError::EmptyField { field })
    } else {
        Ok(())
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct RemoteManagementShellRunRequest {
    pub command: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shell: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
}

impl RemoteManagementShellRunRequest {
    pub fn new(command: impl Into<String>) -> Self {
        Self {
            command: command.into(),
            ..Self::default()
        }
    }

    pub fn with_cwd(mut self, cwd: impl Into<String>) -> Self {
        self.cwd = Some(cwd.into());
        self
    }

    pub fn with_shell(mut self, shell: impl Into<String>) -> Self {
        self.shell = Some(shell.into());
        self
    }

    pub fn with_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = Some(timeout_ms);
        self
    }

    pub fn with_max_output_bytes(mut self, max_output_bytes: usize) -> Self {
        self.max_output_bytes = Some(max_output_bytes);
        self
    }

    pub fn with_env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.insert(key.into(), value.into());
        self
    }

    pub fn effective_timeout_ms(&self) -> u64 {
        self.timeout_ms
            .unwrap_or(DEFAULT_COMMAND_TIMEOUT_MS)
            .clamp(1, MAX_COMMAND_TIMEOUT_MS)
    }

    pub fn effective_max_output_bytes(&self) -> usize {
        self.max_output_bytes
            .unwrap_or(DEFAULT_SHELL_CAPTURE_BYTES)
            .min(MAX_SHELL_CAPTURE_BYTES)
    }

    pub fn validate(&self) -> Result<(), RemoteManagementProtocolError> {
        require_non_empty(&self.command, "shell command")
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RemoteManagementShellRunResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<RemoteManagementShellOutcome>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<RemoteManagementError>,
}

impl RemoteManagementShellRunResponse {
    pub fn success(outcome: RemoteManagementShellOutcome) -> Self {
        Self {
            outcome: Some(outcome),
            error: None,
        }
    }

    pub fn failure(error: RemoteManagementError) -> Self {
        Self {
            outcome: None,
            error: Some(error),
        }
    }

    pub fn outcome(&self) -> Option<&RemoteManagementShellOutcome> {
        self.outcome.as_ref()
    }

    pub fn into_result(self) -> Result<RemoteManagementShellOutcome, RemoteManagementError> {
        match (self.outcome, self.error) {
            (_, Some(error)) => Err(error),
            (Some(outcome), None) => Ok(outcome),
            (None, None) => Err(RemoteManagementError::missing_payload("a shell outcome")),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RemoteManagementShellOutcome {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    pub duration_ms: u64,
    pub timed_out: bool,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
}

impl RemoteManagementShellOutcome {
    pub fn succeeded(&self) -> bool {
        !self.timed_out && self.exit_code == Some(0)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RemoteManagementFileReadRequest {
    pub path: String,
}

impl RemoteManagementFileReadRequest {
    pub fn new(path: impl Into<String>) -> Self {
        Self { path: path.into() }
    }

    pub fn validate(&self) -> Result<(), RemoteManagementProtocolError> {
        require_non_empty(&self.path, "file read path")
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RemoteManagementFileReadResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<RemoteManagementFileTransfer>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<RemoteManagementError>,
}

impl RemoteManagementFileReadResponse {
    pub fn success(file: RemoteManagementFileTransfer) -> Self {
        Self {
            file: Some(file),
            error: None,
        }
    }

    pub fn failure(error: RemoteManagementError) -> Self {
        Self {
            file: None,
            error: Some(error),
        }
    }

    pub fn file(&self) -> Option<&RemoteManagementFileTransfer> {
        self.file.as_ref()
    }

    pub fn into_result(self) -> Result<RemoteManagementFileTransfer, RemoteManagementError> {
        match (self.file, self.error) {
            (_, Some(error)) => Err(error),
            (Some(file), None) => Ok(file),
            (None, None) => Err(RemoteManagementError::missing_payload("file content")),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RemoteManagementFileTransfer {
    pub path: String,
    pub length: u64,
    /// Lowercase hexadecimal BLAKE3 digest of the complete attachment.
    pub blake3: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct RemoteManagementFileWriteRequest {
    pub path: String,
    pub expected_len: u64,
    pub expected_blake3: String,
    #[serde(default)]
    pub create_parent: bool,
    #[serde(default)]
    pub overwrite: bool,
}

impl RemoteManagementFileWriteRequest {
    pub fn new(
        path: impl Into<String>,
        expected_len: u64,
        expected_blake3: impl Into<String>,
    ) -> Self {
        Self {
            path: path.into(),
            expected_len,
            expected_blake3: expected_blake3.into(),
            ..Self::default()
        }
    }

    pub fn creating_parent(mut self, create_parent: bool) -> Self {
        self.create_parent = create_parent;
        self
    }

    pub fn overwriting(mut self, overwrite: bool) -> Self {
        self.overwrite = overwrite;
        self
    }

    pub fn validate(&self) -> Result<(), RemoteManagementProtocolError> {
        require_non_empty(&self.path, "file write path")?;
        let digest = self
            .expected_blake3
            .strip_prefix("blake3:")
            .unwrap_or(&self.expected_blake3);
        if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(RemoteManagementProtocolError::InvalidBlake3);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RemoteManagementFileWriteResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub written: Option<RemoteManagementFileTransfer>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<RemoteManagementError>,
}

impl RemoteManagementFileWriteResponse {
    pub fn success(written: RemoteManagementFileTransfer) -> Self {
        Self {
            written: Some(written),
            error: None,
        }
    }

    pub fn failure(error: RemoteManagementError) -> Self {
        Self {
            written: None,
            error: Some(error),
        }
    }

    pub fn written(&self) -> Option<&RemoteManagementFileTransfer> {
        self.written.as_ref()
    }

    pub fn into_result(self) -> Result<RemoteManagementFileTransfer, RemoteManagementError> {
        match (self.written, self.error) {
            (_, Some(error)) => Err(error),
            (Some(written), None) => Ok(written),
            (None, None) => Err(RemoteManagementError::missing_payload(
                "a file write outcome",
            )),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RemoteManagementFileListRequest {
    pub path: String,
    #[serde(default)]
    pub recursive: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_entries: Option<usize>,
}

impl RemoteManagementFileListRequest {
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            recursive: false,
            max_entries: None,
        }
    }

    pub fn recursively(mut self, recursive: bool) -> Self {
        self.recursive = recursive;
        self
    }

    pub fn with_max_entries(mut self, max_entries: usize) -> Self {
        self.max_entries = Some(max_entries);
        self
    }

    pub fn effective_max_entries(&self) -> usize {
        self.max_entries
            .unwrap_or(DEFAULT_LIST_ENTRIES)
            .min(MAX_LIST_ENTRIES)
    }

    pub fn validate(&self) -> Result<(), RemoteManagementProtocolError> {
        require_non_empty(&self.path, "file list path")
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RemoteManagementFileListResponse {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entries: Vec<RemoteManagementFileEntry>,
    pub truncated: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<RemoteManagementError>,
}

impl RemoteManagementFileListResponse {
    pub fn success(entries: Vec<RemoteManagementFileEntry>, truncated: bool) -> Self {
        Self {
            entries,
            truncated,
            error: None,
        }
    }

    pub fn failure(error: RemoteManagementError) -> Self {
        Self {
            entries: Vec::new(),
            truncated: false,
            error: Some(error),
        }
    }

    pub fn into_result(
        self,
    ) -> Result<(Vec<RemoteManagementFileEntry>, bool), RemoteManagementError> {
        match self.error {
            Some(error) => Err(error),
            None => Ok((self.entries, self.truncated)),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RemoteManagementFileEntry {
    pub path: String,
    pub file_type: RemoteManagementFileType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub len: Option<u64>,
    pub readonly: bool,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum RemoteManagementFileType {
    File,
    Directory,
    Symlink,
    Other,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RemoteManagementFileStatRequest {
    pub path: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub include_blake3: bool,
}

impl RemoteManagementFileStatRequest {
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            include_blake3: false,
        }
    }

    pub fn including_blake3(mut self, include_blake3: bool) -> Self {
        self.include_blake3 = include_blake3;
        self
    }

    pub fn validate(&self) -> Result<(), RemoteManagementProtocolError> {
        require_non_empty(&self.path, "file stat path")
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RemoteManagementFileStatResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stat: Option<RemoteManagementFileStat>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<RemoteManagementError>,
}

impl RemoteManagementFileStatResponse {
    pub fn success(stat: RemoteManagementFileStat) -> Self {
        Self {
            stat: Some(stat),
            error: None,
        }
    }

    pub fn failure(error: RemoteManagementError) -> Self {
        Self {
            stat: None,
            error: Some(error),
        }
    }

    pub fn stat(&self) -> Option<&RemoteManagementFileStat> {
        self.stat.as_ref()
    }

    pub fn into_result(self) -> Result<RemoteManagementFileStat, RemoteManagementError> {
        match (self.stat, self.error) {
            (_, Some(error)) => Err(error),
            (Some(stat), None) => Ok(stat),
            (None, None) => Err(RemoteManagementError::missing_payload("file metadata")),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RemoteManagementFileStat {
    pub path: String,
    pub file_type: RemoteManagementFileType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub len: Option<u64>,
    pub readonly: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modified_unix_ms: Option<u64>,
    /// Lowercase hexadecimal BLAKE3 digest when requested.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blake3: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RemoteManagementSystemInfoRequest {
    #[serde(default)]
    pub include_env: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env_names: Vec<String>,
}

impl RemoteManagementSystemInfoRequest {
    pub fn new() -> Self {
        Self {
            include_env: false,
            env_names: Vec::new(),
        }
    }

    pub fn including_env(mut self, include_env: bool) -> Self {
        self.include_env = include_env;
        self
    }

    pub fn with_env_name(mut self, name: impl Into<String>) -> Self {
        self.env_names.push(name.into());
        self
    }
}

impl Default for RemoteManagementSystemInfoRequest {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RemoteManagementSystemInfoResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub info: Option<RemoteManagementSystemInfo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<RemoteManagementError>,
}

impl RemoteManagementSystemInfoResponse {
    pub fn success(info: RemoteManagementSystemInfo) -> Self {
        Self {
            info: Some(info),
            error: None,
        }
    }

    pub fn failure(error: RemoteManagementError) -> Self {
        Self {
            info: None,
            error: Some(error),
        }
    }

    pub fn info(&self) -> Option<&RemoteManagementSystemInfo> {
        self.info.as_ref()
    }

    pub fn into_result(self) -> Result<RemoteManagementSystemInfo, RemoteManagementError> {
        match (self.info, self.error) {
            (_, Some(error)) => Err(error),
            (Some(info), None) => Ok(info),
            (None, None) => Err(RemoteManagementError::missing_payload("system information")),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RemoteManagementSystemInfo {
    pub os: String,
    pub arch: String,
    pub family: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_dir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executable: Option<String>,
    pub default_shell: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, Option<String>>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub env_redacted: BTreeSet<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct RemoteManagementSystemObserveRequest {
    #[serde(default, skip_serializing_if = "is_false")]
    pub include_processes: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_processes: Option<usize>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub include_disks: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub include_network: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub include_devices: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub include_resource_exposures: bool,
    /// Resource-exposure policy declarations injected by the authoritative
    /// receiving host after it has checked node-local policy. These are not
    /// verified device inventory. Remote callers must not be trusted to
    /// populate this field; daemon hosts overwrite it immediately before
    /// native-module invocation.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub host_authorized_resource_exposures: Vec<RemoteManagementResourceExposurePolicy>,
}

impl RemoteManagementSystemObserveRequest {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn including_processes(mut self, include_processes: bool) -> Self {
        self.include_processes = include_processes;
        self
    }

    pub fn with_max_processes(mut self, max_processes: usize) -> Self {
        self.max_processes = Some(max_processes);
        self
    }

    pub fn including_disks(mut self, include_disks: bool) -> Self {
        self.include_disks = include_disks;
        self
    }

    pub fn including_network(mut self, include_network: bool) -> Self {
        self.include_network = include_network;
        self
    }

    pub fn including_devices(mut self, include_devices: bool) -> Self {
        self.include_devices = include_devices;
        self
    }

    pub fn including_resource_exposures(mut self, include_resource_exposures: bool) -> Self {
        self.include_resource_exposures = include_resource_exposures;
        self
    }

    pub fn with_host_authorized_resource_exposures(
        mut self,
        exposures: impl IntoIterator<Item = RemoteManagementResourceExposurePolicy>,
    ) -> Self {
        self.host_authorized_resource_exposures = exposures.into_iter().collect();
        self
    }

    pub fn effective_max_processes(&self) -> usize {
        self.max_processes
            .unwrap_or(DEFAULT_OBSERVED_PROCESSES)
            .min(MAX_OBSERVED_PROCESSES)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RemoteManagementSystemObserveResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observation: Option<RemoteManagementSystemObservation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<RemoteManagementError>,
}

impl RemoteManagementSystemObserveResponse {
    pub fn success(observation: RemoteManagementSystemObservation) -> Self {
        Self {
            observation: Some(observation),
            error: None,
        }
    }

    pub fn failure(error: RemoteManagementError) -> Self {
        Self {
            observation: None,
            error: Some(error),
        }
    }

    pub fn observation(&self) -> Option<&RemoteManagementSystemObservation> {
        self.observation.as_ref()
    }

    pub fn into_result(self) -> Result<RemoteManagementSystemObservation, RemoteManagementError> {
        match (self.observation, self.error) {
            (_, Some(error)) => Err(error),
            (Some(observation), None) => Ok(observation),
            (None, None) => Err(RemoteManagementError::missing_payload(
                "a system observation",
            )),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct RemoteManagementSystemObservation {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub observed_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uptime_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logical_cpu_count: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_usage_percent: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub load_average: Option<RemoteManagementLoadAverage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<RemoteManagementMemoryObservation>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub disks: Vec<RemoteManagementDiskObservation>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub network_interfaces: Vec<RemoteManagementNetworkObservation>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub processes: Vec<RemoteManagementProcessObservation>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub processes_truncated: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub devices: Vec<RemoteManagementDeviceObservation>,
    /// Node-local policy declarations that permit named operations on a
    /// resource. A declaration does not prove that the resource currently
    /// exists or is available; actual device inventory belongs in `devices`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resource_exposure_policies: Vec<RemoteManagementResourceExposurePolicy>,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct RemoteManagementLoadAverage {
    pub one_minute: f64,
    pub five_minutes: f64,
    pub fifteen_minutes: f64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct RemoteManagementMemoryObservation {
    pub total_bytes: u64,
    pub available_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub swap_total_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub swap_free_bytes: Option<u64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct RemoteManagementDiskObservation {
    pub name: String,
    pub mount_point: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_system: Option<String>,
    pub total_bytes: u64,
    pub available_bytes: u64,
    #[serde(default, skip_serializing_if = "is_false")]
    pub removable: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct RemoteManagementNetworkObservation {
    pub name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub addresses: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub received_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transmitted_bytes: Option<u64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct RemoteManagementProcessObservation {
    pub pid: u32,
    pub name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub command: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_usage_percent: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_unix_ms: Option<u64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct RemoteManagementDeviceObservation {
    pub id: String,
    pub kind: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct RemoteManagementResourceExposurePolicy {
    pub resource_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_operations: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    fn sobj_round_trip<T>(value: &T) -> T
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        let bytes = sinomo_module_api::encode_sobj(value).expect("encode SOBJ");
        sinomo_module_api::decode_sobj(&bytes).expect("decode SOBJ")
    }

    #[test]
    fn established_routes_and_config_aliases_are_stable() {
        assert_eq!(
            REMOTE_MANAGEMENT_INTERFACE_VERSION,
            "sinomo.remote-management.v2"
        );
        assert_eq!(
            REMOTE_MANAGEMENT_SHELL_RUN_ROUTE,
            "/remote-management/shell/run"
        );
        assert_eq!(
            REMOTE_MANAGEMENT_FS_READ_ROUTE,
            "/remote-management/fs/read"
        );
        assert_eq!(
            REMOTE_MANAGEMENT_FS_WRITE_ROUTE,
            "/remote-management/fs/write"
        );
        assert_eq!(
            REMOTE_MANAGEMENT_FS_LIST_ROUTE,
            "/remote-management/fs/list"
        );
        assert_eq!(
            REMOTE_MANAGEMENT_SYSTEM_INFO_ROUTE,
            "/remote-management/system/info"
        );
        assert_eq!(CONFIG_CATALOG_ROUTE, "/config/catalog");
        assert_eq!(CONFIG_SEARCH_ROUTE, "/config/search");
        assert_eq!(CONFIG_GET_ROUTE, "/config/get");
        assert_eq!(CONFIG_SET_ROUTE, "/config/set");
        assert_eq!(
            REMOTE_MANAGEMENT_CONFIG_ROUTES,
            &[
                "/config/catalog",
                "/config/search",
                "/config/get",
                "/config/set",
            ]
        );
    }

    #[test]
    fn existing_request_payloads_round_trip_through_sobj() {
        let shell = RemoteManagementShellRunRequest::new("uname -a")
            .with_cwd("/tmp")
            .with_timeout_ms(5_000)
            .with_env("LANG", "C");
        assert_eq!(sobj_round_trip(&shell), shell);

        let read = RemoteManagementFileReadRequest::new("/tmp/data");
        assert_eq!(sobj_round_trip(&read), read);

        let write = RemoteManagementFileWriteRequest::new(
            "/tmp/data",
            0,
            blake3::hash(&[]).to_hex().to_string(),
        )
        .creating_parent(true)
        .overwriting(true);
        assert_eq!(sobj_round_trip(&write), write);

        let list = RemoteManagementFileListRequest::new("/tmp")
            .recursively(true)
            .with_max_entries(20);
        assert_eq!(sobj_round_trip(&list), list);

        let info = RemoteManagementSystemInfoRequest::new().with_env_name("PATH");
        assert_eq!(sobj_round_trip(&info), info);
    }

    #[test]
    fn file_transfer_heads_are_path_only_and_unbounded() {
        let read = RemoteManagementFileReadRequest::new("/tmp/data");
        let write = RemoteManagementFileWriteRequest::new(
            "/tmp/data",
            0,
            blake3::hash(&[]).to_hex().to_string(),
        )
        .creating_parent(true);
        read.validate().expect("read head");
        write.validate().expect("write head");

        let transfer = RemoteManagementFileTransfer {
            path: "/tmp/data".to_string(),
            length: u64::MAX,
            blake3: "00".repeat(32),
        };
        assert_eq!(sobj_round_trip(&transfer), transfer);
    }

    #[test]
    fn new_stat_and_observation_payloads_round_trip_through_sobj() {
        let stat = RemoteManagementFileStatResponse::success(RemoteManagementFileStat {
            path: "/tmp/data".to_string(),
            file_type: RemoteManagementFileType::File,
            len: Some(6),
            readonly: false,
            modified_unix_ms: Some(42),
            blake3: Some("abc123".to_string()),
        });
        assert_eq!(sobj_round_trip(&stat), stat);

        let observation =
            RemoteManagementSystemObserveResponse::success(RemoteManagementSystemObservation {
                observed_at: "2026-07-15T00:00:00Z".to_string(),
                hostname: Some("node-a".to_string()),
                uptime_seconds: Some(10),
                logical_cpu_count: Some(8),
                cpu_usage_percent: Some(12.5),
                memory: Some(RemoteManagementMemoryObservation {
                    total_bytes: 1024,
                    available_bytes: 512,
                    ..Default::default()
                }),
                resource_exposure_policies: vec![RemoteManagementResourceExposurePolicy {
                    resource_id: "camera.front".to_string(),
                    allowed_operations: vec!["observe".to_string()],
                }],
                ..Default::default()
            });
        assert_eq!(sobj_round_trip(&observation), observation);
    }

    #[test]
    fn structured_responses_preserve_remote_errors() {
        let error = RemoteManagementError::new(
            "remote-management-disabled",
            "enable remote management locally",
        );
        let response = RemoteManagementFileStatResponse::failure(error.clone());
        assert_eq!(response.into_result(), Err(error));

        let empty = RemoteManagementSystemObserveResponse {
            observation: None,
            error: None,
        };
        assert_eq!(
            empty.into_result().expect_err("missing observation").code(),
            "missing-response-payload"
        );
    }

    #[test]
    fn config_type_aliases_keep_the_authoritative_wire_contract() {
        let request = RemoteManagementConfigSetRequest {
            request_id: Some(sinomo_core::kv::KvRequestId::generate()),
            key: "sandbox.policy".to_string(),
            value: serde_json::json!({ "mode": "strict" }),
            target: ConfigWriteTarget::Local,
        };
        let round_trip: RemoteManagementConfigSetRequest = sobj_round_trip(&request);
        assert_eq!(round_trip, request);

        let catalog = RemoteManagementConfigCatalogRequest {
            owner: Some("sinomo-sandbox".to_string()),
        };
        let authoritative: ConfigCatalogRequest = catalog.clone();
        assert_eq!(authoritative, catalog);
    }

    #[test]
    fn serde_defaults_keep_new_observation_requests_bounded() {
        let request: RemoteManagementSystemObserveRequest =
            serde_json::from_str("{}").expect("default observe request");
        assert_eq!(request, RemoteManagementSystemObserveRequest::default());
        assert_eq!(
            request.effective_max_processes(),
            DEFAULT_OBSERVED_PROCESSES
        );

        let request = request.with_max_processes(usize::MAX);
        assert_eq!(request.effective_max_processes(), MAX_OBSERVED_PROCESSES);
    }
}
