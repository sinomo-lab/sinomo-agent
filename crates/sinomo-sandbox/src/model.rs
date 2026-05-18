use crate::internal::*;

#[cfg(test)]
pub(crate) const BUSYBOX_WASI_CONFIG_FRAGMENT: &str =
    include_str!("../toolchains/busybox/busybox-wasi.fragment");
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum SandboxTempPolicy {
    ClearOnSessionStart,
    RetainAcrossSessions,
}

impl Default for SandboxTempPolicy {
    fn default() -> Self {
        Self::ClearOnSessionStart
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum SandboxMountMode {
    ReadOnly,
    ReadWrite,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxMount {
    pub source_sifs_path: String,
    pub mount_path: SandboxPath,
    pub mode: SandboxMountMode,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxResourceLimits {
    pub memory_mb: u64,
    pub wall_time_ms: u64,
    #[serde(default = "SandboxResourceLimits::default_max_input_bytes")]
    pub max_input_bytes: u64,
    pub max_output_bytes: u64,
    pub max_workspace_bytes: u64,
    pub max_written_bytes_per_command: u64,
    pub max_file_count: u64,
}

pub const SANDBOX_UNLIMITED_WALL_TIME_MS: u64 = u64::MAX;

impl Default for SandboxResourceLimits {
    fn default() -> Self {
        Self {
            memory_mb: host_memory_limit_mb(),
            wall_time_ms: SANDBOX_UNLIMITED_WALL_TIME_MS,
            max_input_bytes: Self::default_max_input_bytes(),
            max_output_bytes: 16 * 1024 * 1024,
            max_workspace_bytes: 1024 * 1024 * 1024,
            max_written_bytes_per_command: 128 * 1024 * 1024,
            max_file_count: 100_000,
        }
    }
}

impl SandboxResourceLimits {
    pub fn default_max_input_bytes() -> u64 {
        16 * 1024 * 1024
    }
}

fn host_memory_limit_mb() -> u64 {
    #[cfg(target_os = "linux")]
    {
        if let Ok(meminfo) = fs::read_to_string("/proc/meminfo") {
            if let Some(kb) = meminfo.lines().find_map(|line| {
                let mut fields = line.split_whitespace();
                match (fields.next(), fields.next()) {
                    (Some("MemTotal:"), Some(value)) => value.parse::<u64>().ok(),
                    _ => None,
                }
            }) {
                return kb.saturating_add(1023) / 1024;
            }
        }
    }
    u64::MAX / (1024 * 1024)
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxInstanceManifest {
    pub sandbox_id: SandboxId,
    pub workspace_root: String,
    pub toolchain_id: SandboxToolchainId,
    pub cwd: SandboxPath,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mounts: Vec<SandboxMount>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub command_history: Vec<SandboxCommandId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_placement: Option<SandboxExecutionPlacement>,
    pub temp_policy: SandboxTempPolicy,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub temp_epoch: u64,
    pub limits: SandboxResourceLimits,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

impl SandboxInstanceManifest {
    pub fn new(
        sandbox_id: SandboxId,
        toolchain_id: SandboxToolchainId,
        now: OffsetDateTime,
    ) -> Result<Self, SandboxPathError> {
        Ok(Self {
            workspace_root: sandbox_instance_root(&sandbox_id),
            sandbox_id,
            toolchain_id,
            cwd: SandboxPath::new("/work")?,
            env: BTreeMap::new(),
            mounts: Vec::new(),
            command_history: Vec::new(),
            last_placement: None,
            temp_policy: SandboxTempPolicy::default(),
            temp_epoch: 0,
            limits: SandboxResourceLimits::default(),
            created_at: now,
            updated_at: now,
        })
    }
}

pub(crate) fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxCommandRequest {
    pub sandbox_id: SandboxId,
    pub argv: Vec<String>,
    pub cwd: SandboxPath,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdin: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    pub timeout_ms: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxResourceUsage {
    pub wall_time_ms: u64,
    pub peak_memory_bytes: u64,
    pub stdout_bytes: u64,
    pub stderr_bytes: u64,
    pub written_bytes: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxWorkspaceUsage {
    pub file_count: u64,
    pub total_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxCommandResponse {
    pub command_id: SandboxCommandId,
    pub exit_status: i32,
    pub stdout_ref: String,
    pub stderr_ref: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub changed_paths: Vec<SandboxPath>,
    pub resource_usage: SandboxResourceUsage,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum SandboxCommandRecordStatus {
    Completed,
    Failed,
    Cancelled,
    ResourceExhausted,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxCommandRecord {
    pub sandbox_id: SandboxId,
    pub command_id: SandboxCommandId,
    pub status: SandboxCommandRecordStatus,
    pub workspace_root: String,
    pub toolchain_id: SandboxToolchainId,
    pub argv: Vec<String>,
    pub cwd: SandboxPath,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    #[serde(with = "time::serde::rfc3339")]
    pub started_at: OffsetDateTime,
    #[serde(
        default,
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub completed_at: Option<OffsetDateTime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response: Option<SandboxCommandResponse>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}
