use crate::internal::*;

pub const BUSYBOX_WASI_CONFIG_FRAGMENT: &str =
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
#[serde(rename_all = "kebab-case")]
pub enum SandboxInstancePortability {
    Portable,
    NonPortable,
}

impl Default for SandboxInstancePortability {
    fn default() -> Self {
        Self::NonPortable
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxRuntimeBinding {
    pub runtime: String,
    pub node_id: NodeId,
    #[serde(default)]
    pub portability: SandboxInstancePortability,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum SandboxInstanceLifecycleStatus {
    Created,
    Running,
    Stopped,
}

impl Default for SandboxInstanceLifecycleStatus {
    fn default() -> Self {
        Self::Created
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxInstanceLifecycle {
    #[serde(default)]
    pub status: SandboxInstanceLifecycleStatus,
    #[serde(default, with = "time::serde::rfc3339::option")]
    pub started_at: Option<OffsetDateTime>,
    #[serde(default, with = "time::serde::rfc3339::option")]
    pub stopped_at: Option<OffsetDateTime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

impl Default for SandboxInstanceLifecycle {
    fn default() -> Self {
        Self {
            status: SandboxInstanceLifecycleStatus::Created,
            started_at: None,
            stopped_at: None,
            last_error: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum SandboxTrackedProcessStatus {
    Running,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxTrackedProcess {
    pub process_id: String,
    pub command: String,
    pub argv: Vec<String>,
    pub cwd: SandboxPath,
    #[serde(with = "time::serde::rfc3339")]
    pub started_at: OffsetDateTime,
    pub status: SandboxTrackedProcessStatus,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxResourceLimits {
    pub memory_mb: u64,
    pub wall_time_ms: u64,
    #[serde(default = "SandboxResourceLimits::default_size_limit")]
    pub max_input_bytes: u64,
    #[serde(default = "SandboxResourceLimits::default_size_limit")]
    pub max_output_bytes: u64,
    #[serde(default = "SandboxResourceLimits::default_size_limit")]
    pub max_workspace_bytes: u64,
    #[serde(default = "SandboxResourceLimits::default_size_limit")]
    pub max_written_bytes_per_command: u64,
    #[serde(default = "SandboxResourceLimits::default_size_limit")]
    pub max_file_count: u64,
}

pub const SANDBOX_UNLIMITED_WALL_TIME_MS: u64 = u64::MAX;
pub const SANDBOX_UNLIMITED_SIZE_LIMIT: u64 = u64::MAX;

impl Default for SandboxResourceLimits {
    fn default() -> Self {
        Self {
            memory_mb: host_memory_limit_mb(),
            wall_time_ms: SANDBOX_UNLIMITED_WALL_TIME_MS,
            max_input_bytes: SANDBOX_UNLIMITED_SIZE_LIMIT,
            max_output_bytes: SANDBOX_UNLIMITED_SIZE_LIMIT,
            max_workspace_bytes: SANDBOX_UNLIMITED_SIZE_LIMIT,
            max_written_bytes_per_command: SANDBOX_UNLIMITED_SIZE_LIMIT,
            max_file_count: SANDBOX_UNLIMITED_SIZE_LIMIT,
        }
    }
}

impl SandboxResourceLimits {
    pub fn default_size_limit() -> u64 {
        SANDBOX_UNLIMITED_SIZE_LIMIT
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
    /// Stable identity for this incarnation of a sandbox ID. A destroy followed by
    /// recreate receives a new value even when its fencing generation restarts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incarnation_id: Option<Id>,
    /// Cluster-visible fail-closed fence for explicit lifecycle operations.
    /// The stable `workspace_root` symlink is not switched until a replacement
    /// generation is complete, and sandbox APIs remain quarantined until the
    /// lifecycle operation has been finalized.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_reconciliation: Option<SandboxAuthoritativeReconciliationFence>,
    /// Monotone fencing generation advanced under the sandbox's exclusive DFS
    /// lifecycle lease.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub generation: u64,
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
    #[serde(default)]
    pub lifecycle: SandboxInstanceLifecycle,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_binding: Option<SandboxRuntimeBinding>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub active_processes: Vec<SandboxTrackedProcess>,
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
            incarnation_id: Some(Id::generate()),
            pending_reconciliation: None,
            generation: 1,
            toolchain_id,
            cwd: SandboxPath::new("/work")?,
            env: BTreeMap::new(),
            mounts: Vec::new(),
            command_history: Vec::new(),
            last_placement: None,
            lifecycle: SandboxInstanceLifecycle::default(),
            runtime_binding: None,
            active_processes: Vec::new(),
            temp_policy: SandboxTempPolicy::default(),
            temp_epoch: 0,
            limits: SandboxResourceLimits::default(),
            created_at: now,
            updated_at: now,
        })
    }

    /// Advance the authoritative sandbox generation while the caller holds the
    /// sandbox lifecycle lease.
    pub fn advance_generation(&mut self) -> Result<u64, SandboxManifestGenerationError> {
        self.generation = self.generation.checked_add(1).ok_or_else(|| {
            SandboxManifestGenerationError::Exhausted {
                sandbox_id: self.sandbox_id.clone(),
            }
        })?;
        Ok(self.generation)
    }

    /// Return the lifecycle incarnation fence. Legacy manifests did not persist
    /// an incarnation ID, so derive one from immutable creation data. A newly
    /// created manifest always carries a random persisted ID.
    pub fn effective_incarnation_id(&self) -> Id {
        self.incarnation_id.clone().unwrap_or_else(|| {
            Id::derived(
                "sinomo.sandbox.legacy-incarnation.v1",
                format!(
                    "{}:{}",
                    self.sandbox_id,
                    self.created_at.unix_timestamp_nanos()
                ),
            )
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxAuthoritativeReconciliationFence {
    pub transaction_id: Id,
    pub source_incarnation_id: Id,
    pub source_generation: u64,
    /// Previously published immutable generation target. Recovery uses it to
    /// prove that the stable symlink has either not switched or switched once.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_workspace_root: Option<String>,
    /// Fully staged replacement workspace. Present for operations which publish
    /// a new workspace generation; reboot only changes the temp-epoch pointer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_workspace_root: Option<String>,
    /// Durable DFS namespace job used by operations whose legacy tree move is
    /// intentionally resumable rather than atomic.
    #[serde(default)]
    pub operation: SandboxAuthoritativeMutationOperation,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "operation", rename_all = "kebab-case")]
pub enum SandboxAuthoritativeMutationOperation {
    /// Deserialization tombstone for pre-live-FUSE command transactions. No
    /// current API creates or recovers this operation.
    #[default]
    #[serde(rename = "reconciliation")]
    UnsupportedLegacyCommandTransaction,
    ResumeCheckpoint {
        checkpoint_id: SandboxCheckpointId,
    },
    Reboot {
        target_temp_epoch: u64,
    },
    Create,
    Destroy,
    LegacyMigration,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum SandboxManifestGenerationError {
    #[error("sandbox manifest generation exhausted for {sandbox_id}")]
    Exhausted { sandbox_id: SandboxId },
}

pub(crate) fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sandbox_incarnation_changes_on_recreate_and_legacy_fallback_is_stable() {
        let sandbox_id = SandboxId::from_legacy_named("incarnation-fence").expect("sandbox id");
        let toolchain_id =
            SandboxToolchainId::from_legacy_named("incarnation-toolchain").expect("toolchain id");
        let first = SandboxInstanceManifest::new(
            sandbox_id.clone(),
            toolchain_id.clone(),
            OffsetDateTime::UNIX_EPOCH,
        )
        .expect("first manifest");
        let second =
            SandboxInstanceManifest::new(sandbox_id, toolchain_id, OffsetDateTime::UNIX_EPOCH)
                .expect("second manifest");
        assert_ne!(
            first.effective_incarnation_id(),
            second.effective_incarnation_id(),
            "destroy and recreate must not reuse a generation-one fence"
        );

        let mut legacy = first;
        legacy.incarnation_id = None;
        assert_eq!(
            legacy.effective_incarnation_id(),
            legacy.effective_incarnation_id(),
            "legacy fallback must be deterministic for the same manifest"
        );
    }
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
    /// Bytes written when the backend can account for every writable path.
    /// [`SANDBOX_RESOURCE_USAGE_UNAVAILABLE`] means the backend deliberately
    /// exposes host paths whose writes cannot be measured completely.
    pub written_bytes: u64,
}

pub const SANDBOX_RESOURCE_USAGE_UNAVAILABLE: u64 = u64::MAX;

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
