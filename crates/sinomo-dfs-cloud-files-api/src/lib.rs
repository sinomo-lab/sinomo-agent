//! Typed SOBJ payloads for the Windows Cloud Files SIFS native module.
//!
//! The generic native module ABI lives in `sinomo-module-api`. This crate only
//! defines the backend-specific operation name and payloads used by the Windows
//! Cloud Files provider module.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

pub const SINOMO_DFS_CLOUD_FILES_MODULE_ID: &str = "module.dfs.cloud-files";
pub const SINOMO_DFS_CLOUD_FILES_INTERFACE_V1: &str = "sinomo.dfs.cloud-files.v1";
pub const SINOMO_DFS_CLOUD_FILES_INTERFACE_V2: &str = "sinomo.dfs.cloud-files.v2";
pub const SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION: &str = "sinomo.dfs.cloud-files.v3";
/// Shared bound for the canonical, plan-bearing cursor-recovery marker. Both
/// the native owner and the host fingerprint reader must admit the same bytes.
pub const CLOUD_FILES_CURSOR_RECOVERY_MARKER_MAX_BYTES: u64 = 16 * 1024 * 1024;
pub const SINOMO_DFS_CLOUD_FILES_PROVIDER_NAME: &str = "Sinomo SIFS";
pub const SINOMO_DFS_CLOUD_FILES_MOUNT_OPERATION: &str = "sifs.cloud-files.mount";
pub const SINOMO_DFS_CLOUD_FILES_UNREGISTER_OPERATION: &str = "sifs.cloud-files.unregister";
/// v3-only operation which unregisters CFAPI while retaining exact durable
/// ownership of the same mount/root state. Keeping this distinct from the
/// legacy unregister operation prevents a v2 provider from silently decoding
/// an unknown serde field and destructively releasing retained state.
pub const SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION: &str =
    "sifs.cloud-files.unregister-preserve-state";
pub const SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_CAPABILITY: &str =
    "dfs.sifs.cloud-files.unregister-preserve-state";
/// Read-only v3 inspection for the one narrowly supported legacy state shape:
/// a physically empty, unregistered sync root whose private state contains
/// valid namespace cursors and no other writable evidence.
pub const SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_INSPECT_OPERATION: &str =
    "sifs.cloud-files.recover-cursor-only-state.inspect";
/// Explicit v3 adoption of the exact snapshot returned by the inspect
/// operation. This is deliberately distinct from mount and unregister so an
/// older provider cannot deserialize it as a permissive legacy request.
pub const SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_APPLY_OPERATION: &str =
    "sifs.cloud-files.recover-cursor-only-state.apply";
pub const SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_INSPECT_CAPABILITY: &str =
    "dfs.sifs.cloud-files.recover-cursor-only-state.inspect";
pub const SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_APPLY_CAPABILITY: &str =
    "dfs.sifs.cloud-files.recover-cursor-only-state.apply";
pub const CLOUD_FILES_CURSOR_ONLY_RECOVERY_SCHEMA_VERSION: u16 = 1;
pub const CLOUD_FILES_CURSOR_ONLY_RECOVERY_TOKEN_DOMAIN: &str =
    "sinomo.dfs.cloud-files.cursor-only-recovery.v1";
pub const CLOUD_FILES_WRITABLE_SYNC_UNSUPPORTED_CODE: &str = "writable-sync-unsupported";
pub const CLOUD_FILES_WRITABLE_SYNC_CONTRACT_NOT_READY_CODE: &str =
    "writable-sync-contract-not-ready";
pub const CLOUD_FILES_READ_ONLY_ENFORCEMENT_UNAVAILABLE_CODE: &str =
    "read-only-enforcement-unavailable";
pub const CLOUD_FILES_STALE_PLACEHOLDER_IDENTITY_CODE: &str = "stale-placeholder-identity";
pub const CLOUD_FILES_PLACEHOLDER_IDENTITY_SCHEMA_VERSION: u16 = 1;
/// `CF_PLACEHOLDER_CREATE_INFO::FileIdentityLength` accepts a larger `u32`,
/// but Cloud Files only retains identities up to 4 KiB for placeholders.
pub const CLOUD_FILES_PLACEHOLDER_IDENTITY_MAX_BYTES: usize = 4096;
/// Maximum number of actionable writeback issues carried by one heartbeat.
/// Aggregate counters and `attention_overflow` preserve the omitted count.
pub const CLOUD_FILES_SYNC_ATTENTION_SUMMARY_LIMIT: usize = 16;
pub const CLOUD_FILES_SYNC_ATTENTION_BYTES_LIMIT: usize = 16 * 1024;
const CLOUD_FILES_SYNC_ATTENTION_FIELD_BYTES_LIMIT: usize = 1024;
const CLOUD_FILES_SYNC_ATTENTION_RECORD_OVERHEAD_BYTES: usize = 512;

/// Immutable SIFS identity retained by one Windows Cloud Files placeholder.
///
/// `mount` identifies the local sync-root instance and `root` identifies its
/// SIFS namespace cut. The remaining fields fence hydration to the exact
/// object version that produced the placeholder rather than whichever object
/// happens to occupy `path` when Windows asks for bytes later.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesPlaceholderIdentity {
    pub schema_version: u16,
    pub mount: String,
    pub root: String,
    pub path: String,
    pub object_id: String,
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
}

impl CloudFilesPlaceholderIdentity {
    pub fn new(
        mount: impl Into<String>,
        root: impl Into<String>,
        path: impl Into<String>,
        object_id: impl Into<String>,
        version: impl Into<String>,
        content_hash: Option<String>,
    ) -> Self {
        Self {
            schema_version: CLOUD_FILES_PLACEHOLDER_IDENTITY_SCHEMA_VERSION,
            mount: mount.into(),
            root: root.into(),
            path: path.into(),
            object_id: object_id.into(),
            version: version.into(),
            content_hash,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum CloudFilesStaleIdentityReason {
    LegacyPathOnly,
    InvalidIdentity,
    MountChanged,
    RootChanged,
    PathMissing,
    PathChanged,
    ObjectChanged,
    VersionChanged,
    ContentChanged,
}

/// Evidence returned when a placeholder can no longer be hydrated from its
/// retained immutable identity.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesStalePlaceholderIdentity {
    pub reason: CloudFilesStaleIdentityReason,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected: Option<CloudFilesPlaceholderIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed: Option<CloudFilesPlaceholderIdentity>,
    pub detail: String,
}

/// Typed failure from an exact placeholder hydration attempt.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum CloudFilesFetchDataError {
    StaleIdentity(Box<CloudFilesStalePlaceholderIdentity>),
    ContentUnavailable { path: String, detail: String },
    Backend { code: String, detail: String },
}

impl std::fmt::Display for CloudFilesFetchDataError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StaleIdentity(evidence) => write!(
                formatter,
                "{}: {}",
                CLOUD_FILES_STALE_PLACEHOLDER_IDENTITY_CODE, evidence.detail
            ),
            Self::ContentUnavailable { path, detail } => {
                write!(formatter, "content unavailable for `{path}`: {detail}")
            }
            Self::Backend { code, detail } => write!(formatter, "{code}: {detail}"),
        }
    }
}

impl std::error::Error for CloudFilesFetchDataError {}

/// Typed payloads emitted through the native-module host output callback while
/// a long-running Cloud Files mount invocation is active.
///
/// The final [`CloudFilesModuleResponse`] is not available until the provider
/// disconnects. These events let the supervising host distinguish a sync root
/// that is merely registered with Windows from one whose hydration callbacks
/// are currently connected.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum CloudFilesModuleEvent {
    MountLifecycle(CloudFilesMountLifecycleEvent),
    /// Synchronous durability handshake emitted only after the authorized
    /// recovery worker is commit-ready. The module must receive a successful
    /// host callback status before it may clear recovery markers or release
    /// local capture.
    CursorRecoveryReadyObserved(CloudFilesCursorRecoveryReadyObservedRequest),
    /// Synchronous successor emitted after the recovered worker has published
    /// Active but remains parked before ordinary capture/writeback. The host
    /// must durably persist ActiveObserved before the module may clear its last
    /// recovery marker and unpark local synchronization.
    CursorRecoveryActiveObserved(CloudFilesCursorRecoveryActiveObservedRequest),
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesCursorRecoveryReadyObservedRequest {
    pub schema_version: u16,
    pub recovery_id: String,
    pub recovery_token: String,
    pub sync_root: PathBuf,
    pub root_path: String,
    pub sync_root_file_id: u64,
    #[serde(default)]
    pub recovery_cycle: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesCursorRecoveryActiveObservedRequest {
    pub schema_version: u16,
    pub recovery_id: String,
    pub recovery_token: String,
    pub sync_root: PathBuf,
    pub root_path: String,
    pub sync_root_file_id: u64,
    #[serde(default)]
    pub recovery_cycle: u64,
    /// Domain-separated fingerprint of the exact current native recovery
    /// marker bytes. The host binds NamespaceFinalized and
    /// CompletionAuthorized as distinct durable predecessors.
    pub module_marker_fingerprint: String,
    /// Domain-separated fingerprint of the canonical ordinary Ready journal
    /// and unchanged Ready receipt expected after terminal normalization.
    pub completed_registration_artifacts_fingerprint: String,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum CloudFilesMountLifecyclePhase {
    Registered,
    Connected,
    Disconnected,
    Unregistered,
    Failed,
}

/// Current state of the optional Windows-to-SIFS local-change path.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum CloudFilesLocalChangeSyncMode {
    #[default]
    Disabled,
    Unsupported,
    ReadOnlyEnforcementUnavailable,
    /// Durable queue/manifest state is open, but workers have not yet proved
    /// their startup reconciliation and receipt capabilities.
    Starting,
    /// Capture, stable mutation, namespace feed, and placeholder workers are
    /// all ready. Only this state may precede live writable registration.
    Active,
    /// Writable synchronization was requested, but a required fail-closed
    /// contract or worker is unavailable.
    Blocked,
}

/// Origin of one actionable writable-provider issue.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum CloudFilesSyncAttentionKind {
    LocalOperationConflict,
    FailedLocalOperation,
    RemoteNamespaceConflict,
    StartupConflict,
    UnsupportedObjectKind,
    StaleOrAmbiguousReconciliation,
}

/// Non-destructive operator guidance for one actionable issue.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum CloudFilesSyncAttentionNextAction {
    ReviewAndResolve,
    RepairCauseAndRetry,
    PreserveLocalCopyAndRescan,
    ReviewUnsupportedEntry,
    WaitForAuthoritativeReconciliation,
}

/// Immutable local evidence retained by the provider. Content bytes are never
/// emitted through lifecycle events; a hash and length identify a staged blob
/// that remains owned by the durable operation queue.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesRetainedLocalEvidenceSummary {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staged_content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staged_content_size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_state: Option<String>,
}

/// Bounded, typed detail for a conflict or failed writeback operation.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesSyncAttentionSummary {
    pub kind: CloudFilesSyncAttentionKind,
    pub path: String,
    pub reason_code: String,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retained_local_evidence: Option<CloudFilesRetainedLocalEvidenceSummary>,
    pub safe_next_action: CloudFilesSyncAttentionNextAction,
}

/// Typed writeback status carried on every lifecycle transition.
///
/// The counters are zero while writeback is unsupported. Keeping them in the
/// lifecycle contract lets supervisors present queue/conflict/error state
/// without parsing human-readable details if a durable writer is added later.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesLocalChangeSyncStatus {
    pub mode: CloudFilesLocalChangeSyncMode,
    pub writable_supported: bool,
    pub local_mutations_blocked: bool,
    pub pending_operations: u64,
    pub retrying_operations: u64,
    pub conflict_count: u64,
    #[serde(default)]
    pub failed_operations: u64,
    /// Deterministically ordered actionable issues, bounded by
    /// [`CLOUD_FILES_SYNC_ATTENTION_SUMMARY_LIMIT`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attention: Vec<CloudFilesSyncAttentionSummary>,
    /// Number of additional actionable issues omitted from `attention`.
    #[serde(default)]
    pub attention_overflow: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<CloudFilesModuleError>,
}

impl CloudFilesLocalChangeSyncStatus {
    pub fn disabled() -> Self {
        Self {
            mode: CloudFilesLocalChangeSyncMode::Disabled,
            writable_supported: false,
            local_mutations_blocked: false,
            pending_operations: 0,
            retrying_operations: 0,
            conflict_count: 0,
            failed_operations: 0,
            attention: Vec::new(),
            attention_overflow: 0,
            last_error: None,
        }
    }

    pub fn unsupported(error: CloudFilesModuleError) -> Self {
        Self {
            mode: CloudFilesLocalChangeSyncMode::Unsupported,
            last_error: Some(error),
            ..Self::disabled()
        }
    }

    pub fn read_only_enforcement_unavailable(error: CloudFilesModuleError) -> Self {
        Self {
            mode: CloudFilesLocalChangeSyncMode::ReadOnlyEnforcementUnavailable,
            last_error: Some(error),
            ..Self::disabled()
        }
    }

    pub fn writable_starting(
        pending_operations: u64,
        retrying_operations: u64,
        conflict_count: u64,
        failed_operations: u64,
    ) -> Self {
        Self {
            mode: CloudFilesLocalChangeSyncMode::Starting,
            writable_supported: true,
            local_mutations_blocked: false,
            pending_operations,
            retrying_operations,
            conflict_count,
            failed_operations,
            attention: Vec::new(),
            attention_overflow: 0,
            last_error: None,
        }
    }

    pub fn writable_active(
        pending_operations: u64,
        retrying_operations: u64,
        conflict_count: u64,
        failed_operations: u64,
    ) -> Self {
        Self {
            mode: CloudFilesLocalChangeSyncMode::Active,
            ..Self::writable_starting(
                pending_operations,
                retrying_operations,
                conflict_count,
                failed_operations,
            )
        }
    }

    pub fn writable_blocked(
        pending_operations: u64,
        retrying_operations: u64,
        conflict_count: u64,
        failed_operations: u64,
        error: CloudFilesModuleError,
    ) -> Self {
        Self {
            mode: CloudFilesLocalChangeSyncMode::Blocked,
            writable_supported: false,
            local_mutations_blocked: true,
            pending_operations,
            retrying_operations,
            conflict_count,
            failed_operations,
            attention: Vec::new(),
            attention_overflow: 0,
            last_error: Some(error),
        }
    }

    pub fn with_attention(
        mut self,
        attention: Vec<CloudFilesSyncAttentionSummary>,
        total_attention: u64,
    ) -> Self {
        let mut bounded = Vec::new();
        let mut retained_bytes = 0usize;
        for mut summary in attention
            .into_iter()
            .take(CLOUD_FILES_SYNC_ATTENTION_SUMMARY_LIMIT)
        {
            bound_attention_summary_fields(&mut summary);
            let summary_bytes = attention_summary_bytes(&summary);
            if retained_bytes.saturating_add(summary_bytes) > CLOUD_FILES_SYNC_ATTENTION_BYTES_LIMIT
            {
                break;
            }
            retained_bytes = retained_bytes.saturating_add(summary_bytes);
            bounded.push(summary);
        }
        self.attention_overflow = total_attention.saturating_sub(bounded.len() as u64);
        self.attention = bounded;
        self
    }
}

fn bound_attention_summary_fields(summary: &mut CloudFilesSyncAttentionSummary) {
    truncate_string_bytes(
        &mut summary.path,
        CLOUD_FILES_SYNC_ATTENTION_FIELD_BYTES_LIMIT,
    );
    truncate_string_bytes(
        &mut summary.reason_code,
        CLOUD_FILES_SYNC_ATTENTION_FIELD_BYTES_LIMIT,
    );
    truncate_string_bytes(
        &mut summary.reason,
        CLOUD_FILES_SYNC_ATTENTION_FIELD_BYTES_LIMIT,
    );
    if let Some(evidence) = summary.retained_local_evidence.as_mut() {
        for value in [
            &mut evidence.operation_id,
            &mut evidence.evidence_id,
            &mut evidence.staged_content_hash,
            &mut evidence.local_state,
        ] {
            if let Some(value) = value.as_mut() {
                truncate_string_bytes(value, CLOUD_FILES_SYNC_ATTENTION_FIELD_BYTES_LIMIT);
            }
        }
    }
}

fn truncate_string_bytes(value: &mut String, maximum: usize) {
    if value.len() <= maximum {
        return;
    }
    let mut boundary = maximum;
    while !value.is_char_boundary(boundary) {
        boundary = boundary.saturating_sub(1);
    }
    value.truncate(boundary);
}

fn attention_summary_bytes(summary: &CloudFilesSyncAttentionSummary) -> usize {
    let mut bytes = CLOUD_FILES_SYNC_ATTENTION_RECORD_OVERHEAD_BYTES.saturating_add(
        summary
            .path
            .len()
            .saturating_add(summary.reason_code.len())
            .saturating_add(summary.reason.len()),
    );
    if let Some(evidence) = &summary.retained_local_evidence {
        bytes = bytes
            .saturating_add(evidence.operation_id.as_ref().map_or(0, String::len))
            .saturating_add(evidence.evidence_id.as_ref().map_or(0, String::len))
            .saturating_add(evidence.staged_content_hash.as_ref().map_or(0, String::len))
            .saturating_add(evidence.local_state.as_ref().map_or(0, String::len))
            .saturating_add(std::mem::size_of::<u64>());
    }
    bytes
}

impl Default for CloudFilesLocalChangeSyncStatus {
    fn default() -> Self {
        Self::disabled()
    }
}

/// One observed transition of a Cloud Files mount invocation.
///
/// `registered` and `connected` deliberately accompany `phase`: a failure may
/// happen after the persistent sync-root registration succeeded, or while a
/// live callback connection is still being torn down. A host can therefore
/// report the last known operational state without inferring it from error
/// text.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesMountLifecycleEvent {
    pub phase: CloudFilesMountLifecyclePhase,
    pub sync_root: PathBuf,
    pub root_path: String,
    pub placeholders_created: u64,
    pub registered: bool,
    pub connected: bool,
    pub interface_version: String,
    pub detail: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<CloudFilesModuleError>,
    #[serde(default)]
    pub local_change_sync: CloudFilesLocalChangeSyncStatus,
    /// True only while the writable worker thread is known to be running.
    #[serde(default)]
    pub worker_alive: bool,
    /// True only after durable startup/full reconciliation completed.
    #[serde(default)]
    pub worker_ready: bool,
    /// Monotonic per-invocation heartbeat generation. Supervisors use this to
    /// reject a stale Connected event from a dead worker.
    #[serde(default)]
    pub health_generation: u64,
}

impl CloudFilesMountLifecycleEvent {
    pub fn registered(
        sync_root: impl Into<PathBuf>,
        root_path: impl Into<String>,
        placeholders_created: u64,
    ) -> Self {
        Self::transition(
            CloudFilesMountLifecyclePhase::Registered,
            sync_root,
            root_path,
            placeholders_created,
            true,
            false,
            "Cloud Files sync root registered and placeholders prepared",
        )
    }

    pub fn connected(
        sync_root: impl Into<PathBuf>,
        root_path: impl Into<String>,
        placeholders_created: u64,
    ) -> Self {
        Self::transition(
            CloudFilesMountLifecyclePhase::Connected,
            sync_root,
            root_path,
            placeholders_created,
            true,
            true,
            "Cloud Files hydration callbacks connected",
        )
    }

    pub fn disconnected(
        sync_root: impl Into<PathBuf>,
        root_path: impl Into<String>,
        placeholders_created: u64,
    ) -> Self {
        Self::transition(
            CloudFilesMountLifecyclePhase::Disconnected,
            sync_root,
            root_path,
            placeholders_created,
            true,
            false,
            "Cloud Files hydration callbacks disconnected",
        )
    }

    pub fn unregistered(
        sync_root: impl Into<PathBuf>,
        root_path: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        Self::transition(
            CloudFilesMountLifecyclePhase::Unregistered,
            sync_root,
            root_path,
            0,
            false,
            false,
            detail,
        )
    }

    pub fn failed(
        sync_root: impl Into<PathBuf>,
        root_path: impl Into<String>,
        placeholders_created: u64,
        registered: bool,
        connected: bool,
        error: CloudFilesModuleError,
    ) -> Self {
        Self {
            phase: CloudFilesMountLifecyclePhase::Failed,
            sync_root: sync_root.into(),
            root_path: root_path.into(),
            placeholders_created,
            registered,
            connected,
            interface_version: SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION.to_string(),
            detail: error.to_string(),
            error: Some(error),
            local_change_sync: CloudFilesLocalChangeSyncStatus::disabled(),
            worker_alive: false,
            worker_ready: false,
            health_generation: 0,
        }
    }

    pub fn with_local_change_sync(
        mut self,
        local_change_sync: CloudFilesLocalChangeSyncStatus,
    ) -> Self {
        self.local_change_sync = local_change_sync;
        self
    }

    pub fn with_worker_health(
        mut self,
        worker_alive: bool,
        worker_ready: bool,
        health_generation: u64,
    ) -> Self {
        self.worker_alive = worker_alive;
        self.worker_ready = worker_ready;
        self.health_generation = health_generation;
        self
    }

    fn transition(
        phase: CloudFilesMountLifecyclePhase,
        sync_root: impl Into<PathBuf>,
        root_path: impl Into<String>,
        placeholders_created: u64,
        registered: bool,
        connected: bool,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            phase,
            sync_root: sync_root.into(),
            root_path: root_path.into(),
            placeholders_created,
            registered,
            connected,
            interface_version: SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION.to_string(),
            detail: detail.into(),
            error: None,
            local_change_sync: CloudFilesLocalChangeSyncStatus::disabled(),
            worker_alive: false,
            worker_ready: false,
            health_generation: 0,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum CloudFilesModuleRequest {
    Mount(CloudFilesMountRequest),
    Unregister(CloudFilesUnregisterRequest),
    PreserveStateUnregister(CloudFilesUnregisterRequest),
    InspectCursorOnlyRecovery(CloudFilesCursorOnlyRecoveryInspectRequest),
    ApplyCursorOnlyRecovery(CloudFilesCursorOnlyRecoveryApplyRequest),
}

/// Identity supplied by the daemon-owned supervisor to both recovery
/// operations. The module domain-separates and hashes these fields together
/// with its owner-defined raw disk inspection. In particular, changing the
/// selected provider artifact or any disabled mount setting invalidates an
/// operator's previously inspected token.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesCursorOnlyRecoveryIdentity {
    pub schema_version: u16,
    pub enabled: bool,
    pub sync_root: PathBuf,
    pub root_path: String,
    pub data_dir_root: PathBuf,
    pub provider_name: String,
    pub provider_version: String,
    pub read_only: bool,
    pub allow_other: bool,
    pub sync_local_changes: bool,
    pub prepopulate_depth: u32,
    pub connect: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_run_seconds: Option<u64>,
    pub disabled_config_fingerprint: String,
    pub provider_provenance_fingerprint: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesCursorOnlyRecoveryInspectRequest {
    pub identity: CloudFilesCursorOnlyRecoveryIdentity,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesCursorOnlyRecoveryApplyRequest {
    pub identity: CloudFilesCursorOnlyRecoveryIdentity,
    pub expected_token: String,
    pub recovery_id: String,
    /// This field must be true at the native module's final effect boundary;
    /// the CLI and supervisor checks alone are not authorization.
    pub acknowledge_authoritative_full_reconciliation: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesUnregisterRequest {
    pub sync_root: PathBuf,
    #[serde(default = "default_root_path")]
    pub root_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_dir_root: Option<PathBuf>,
    #[serde(default = "default_provider_name")]
    pub provider_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor_recovery: Option<CloudFilesCursorRecoveryContext>,
}

impl CloudFilesUnregisterRequest {
    pub fn new(sync_root: impl Into<PathBuf>) -> Self {
        Self {
            sync_root: sync_root.into(),
            root_path: default_root_path(),
            data_dir_root: None,
            provider_name: default_provider_name(),
            cursor_recovery: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesCursorRecoveryContext {
    pub recovery_id: String,
    pub recovery_token: String,
    pub recovery_cycle: u64,
    /// Opaque, domain-separated hash of the exact native
    /// `PreservedStateOwned` successor. It binds a re-enable to the native
    /// namespace resume class without exposing that private state machine at
    /// the host API boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_successor_fingerprint: Option<String>,
}

/// Durable ownership evidence for the one Cloud Files sync root supported by
/// the v1 module on a node. The module stores this under its data-dir-owned
/// runtime directory before exposing a persistent registration.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesRegistrationReceipt {
    /// Interface of the exact native provider which owns cleanup/recovery.
    pub interface_version: String,
    /// Interface encoded into the nonce-bound CFAPI registration identity.
    /// Absent legacy data defaults to `interface_version`. A v3 provider which
    /// adopts an exact v2 identity records v2 here without rewriting it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registration_identity_interface_version: Option<String>,
    pub sync_root: PathBuf,
    pub root_path: String,
    pub provider_name: String,
}

impl CloudFilesRegistrationReceipt {
    pub fn registration_identity_interface_version(&self) -> &str {
        self.registration_identity_interface_version
            .as_deref()
            .unwrap_or(&self.interface_version)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesMountRequest {
    pub sync_root: PathBuf,
    pub root_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_dir_root: Option<PathBuf>,
    #[serde(default = "default_read_only")]
    pub read_only: bool,
    #[serde(default)]
    pub sync_local_changes: bool,
    #[serde(default = "default_provider_name")]
    pub provider_name: String,
    #[serde(default = "default_provider_version")]
    pub provider_version: String,
    #[serde(default = "default_prepopulate_depth")]
    pub prepopulate_depth: u32,
    #[serde(default = "default_connect")]
    pub connect: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_run_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor_recovery: Option<CloudFilesCursorRecoveryContext>,
}

impl CloudFilesMountRequest {
    pub fn new(sync_root: impl Into<PathBuf>) -> Self {
        Self {
            sync_root: sync_root.into(),
            root_path: "/".to_string(),
            data_dir_root: None,
            read_only: default_read_only(),
            sync_local_changes: false,
            provider_name: default_provider_name(),
            provider_version: default_provider_version(),
            prepopulate_depth: default_prepopulate_depth(),
            connect: default_connect(),
            max_run_seconds: None,
            cursor_recovery: None,
        }
    }

    /// Writable Cloud Files is one explicit mode: local changes must be
    /// synchronized durably. Read-only exposure cannot be enforced for an
    /// Explorer tree owned by the same Windows principal, and writable
    /// best-effort mode would lose updates.
    pub fn validate_supported_mode(&self) -> Result<(), CloudFilesModuleError> {
        if !self.read_only && self.sync_local_changes {
            return Ok(());
        }
        Err(CloudFilesModuleError {
            code: CLOUD_FILES_WRITABLE_SYNC_UNSUPPORTED_CODE.to_string(),
            detail: "Windows Cloud Files accepts only read_only=false with sync_local_changes=true; read-only Explorer enforcement and writable best-effort exposure both fail closed"
                .to_string(),
        })
    }

    pub fn local_change_sync_status(&self) -> CloudFilesLocalChangeSyncStatus {
        match self.validate_supported_mode() {
            Ok(()) => CloudFilesLocalChangeSyncStatus::writable_starting(0, 0, 0, 0),
            Err(error) => CloudFilesLocalChangeSyncStatus::unsupported(error),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum CloudFilesModuleResponse {
    Mounted(CloudFilesMountResponse),
    Unregistered(CloudFilesUnregisterResponse),
    StatePreservingUnregistered(CloudFilesPreserveStateUnregisterResponse),
    CursorOnlyRecoveryInspected(CloudFilesCursorOnlyRecoveryInspection),
    CursorOnlyRecoveryApplied(CloudFilesCursorOnlyRecoveryApplied),
    Error(CloudFilesModuleError),
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesCursorOnlyRecoveryInspection {
    pub schema_version: u16,
    pub sync_root: PathBuf,
    pub root_path: String,
    pub sync_root_file_id: u64,
    pub cursor_count: u64,
    pub raw_state_fingerprint: String,
    pub recovery_token: String,
    pub detail: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesCursorOnlyRecoveryApplied {
    pub schema_version: u16,
    pub sync_root: PathBuf,
    pub root_path: String,
    pub sync_root_file_id: u64,
    pub cursor_count_cleared: u64,
    pub recovery_id: String,
    pub recovery_token: String,
    pub detail: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesMountResponse {
    pub sync_root: PathBuf,
    pub root_path: String,
    pub placeholders_created: u64,
    pub connected: bool,
    pub detail: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesUnregisterResponse {
    pub sync_root: PathBuf,
    pub root_path: String,
    /// False means the sync root was already absent; the operation is
    /// intentionally idempotent for restart-safe disable/path-change cleanup.
    pub was_registered: bool,
    pub detail: String,
}

/// v3-only success response for the distinct state-preserving unregister
/// operation. The typed variant itself is the retention claim: it is emitted
/// only after exact journal/file-ID verification, CFAPI absence, and durable
/// normalization to an owned-absent phase.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesPreserveStateUnregisterResponse {
    pub sync_root: PathBuf,
    pub root_path: String,
    pub was_registered: bool,
    pub outcome: CloudFilesPreserveStateUnregisterOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor_recovery: Option<CloudFilesCursorRecoveryContext>,
    pub detail: String,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum CloudFilesPreserveStateUnregisterOutcome {
    /// Exact durable state ownership remains and CFAPI absence was verified.
    Retained,
    /// No writable state could have been created, so registration artifacts
    /// were safely released instead of manufacturing ownership.
    Released,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CloudFilesModuleError {
    pub code: String,
    pub detail: String,
}

impl std::fmt::Display for CloudFilesModuleError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.code, self.detail)
    }
}

impl std::error::Error for CloudFilesModuleError {}

fn default_provider_name() -> String {
    SINOMO_DFS_CLOUD_FILES_PROVIDER_NAME.to_string()
}

fn default_root_path() -> String {
    "/".to_string()
}

fn default_provider_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

fn default_prepopulate_depth() -> u32 {
    // Activation must remain independent of the number of children below the
    // mounted root. Explorer's FETCH_PLACEHOLDERS callback pages those
    // children lazily after the provider is connected.
    0
}

fn default_read_only() -> bool {
    true
}

fn default_connect() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mount_request_defaults_remain_rolling_decodable_but_fail_closed() {
        let request = CloudFilesMountRequest::new("C:\\SinomoSIFS");

        assert_eq!(request.root_path, "/");
        assert_eq!(request.provider_name, "Sinomo SIFS");
        assert_eq!(request.prepopulate_depth, 0);
        assert!(request.connect);
        assert!(request.read_only);
        assert!(!request.sync_local_changes);
        assert_eq!(
            request
                .validate_supported_mode()
                .expect_err("same-principal read-only mode is not enforceable")
                .code,
            CLOUD_FILES_WRITABLE_SYNC_UNSUPPORTED_CODE
        );
    }

    #[test]
    fn only_durable_writable_sync_mode_is_accepted() {
        let mut writable = CloudFilesMountRequest::new("C:\\SinomoSIFS");
        writable.read_only = false;
        let error = writable
            .validate_supported_mode()
            .expect_err("writable mount without local synchronization must be rejected");
        assert_eq!(error.code, CLOUD_FILES_WRITABLE_SYNC_UNSUPPORTED_CODE);
        assert_eq!(
            writable.local_change_sync_status().mode,
            CloudFilesLocalChangeSyncMode::Unsupported
        );

        let mut local_sync = CloudFilesMountRequest::new("C:\\SinomoSIFS");
        local_sync.sync_local_changes = true;
        assert_eq!(
            local_sync
                .validate_supported_mode()
                .expect_err("local-change sync must be rejected")
                .code,
            CLOUD_FILES_WRITABLE_SYNC_UNSUPPORTED_CODE
        );

        local_sync.read_only = false;
        local_sync
            .validate_supported_mode()
            .expect("durable writable synchronization mode");
        assert_eq!(
            local_sync.local_change_sync_status().mode,
            CloudFilesLocalChangeSyncMode::Starting
        );
    }

    #[test]
    fn lifecycle_events_distinguish_persistent_registration_from_live_connection() {
        let registered =
            CloudFilesMountLifecycleEvent::registered("C:\\SinomoSIFS", "/services", 17);
        assert_eq!(registered.phase, CloudFilesMountLifecyclePhase::Registered);
        assert!(registered.registered);
        assert!(!registered.connected);
        assert_eq!(registered.placeholders_created, 17);
        assert_eq!(
            registered.local_change_sync,
            CloudFilesLocalChangeSyncStatus::disabled()
        );

        let connected = CloudFilesMountLifecycleEvent::connected("C:\\SinomoSIFS", "/services", 17);
        assert_eq!(connected.phase, CloudFilesMountLifecyclePhase::Connected);
        assert!(connected.registered);
        assert!(connected.connected);

        let disconnected =
            CloudFilesMountLifecycleEvent::disconnected("C:\\SinomoSIFS", "/services", 17);
        assert_eq!(
            disconnected.phase,
            CloudFilesMountLifecyclePhase::Disconnected
        );
        assert!(disconnected.registered);
        assert!(!disconnected.connected);
    }

    #[test]
    fn failed_lifecycle_event_retains_last_known_mount_state() {
        let error = CloudFilesModuleError {
            code: "disconnect-sync-root".to_string(),
            detail: "provider disconnect failed".to_string(),
        };
        let event = CloudFilesMountLifecycleEvent::failed(
            "C:\\SinomoSIFS",
            "/services",
            17,
            true,
            true,
            error.clone(),
        );

        assert_eq!(event.phase, CloudFilesMountLifecyclePhase::Failed);
        assert!(event.registered);
        assert!(event.connected);
        assert_eq!(event.error, Some(error));
        assert_eq!(
            event.interface_version,
            SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION
        );
    }

    #[test]
    fn unsupported_writeback_status_surfaces_empty_queue_and_typed_error() {
        let error = CloudFilesModuleError {
            code: CLOUD_FILES_WRITABLE_SYNC_UNSUPPORTED_CODE.to_string(),
            detail: "read-only only".to_string(),
        };
        let event = CloudFilesMountLifecycleEvent::failed(
            "C:\\SinomoSIFS",
            "/services",
            0,
            false,
            false,
            error.clone(),
        )
        .with_local_change_sync(CloudFilesLocalChangeSyncStatus::unsupported(error.clone()));

        assert_eq!(
            event.local_change_sync.mode,
            CloudFilesLocalChangeSyncMode::Unsupported
        );
        assert!(!event.local_change_sync.writable_supported);
        assert!(!event.local_change_sync.local_mutations_blocked);
        assert_eq!(event.local_change_sync.pending_operations, 0);
        assert_eq!(event.local_change_sync.retrying_operations, 0);
        assert_eq!(event.local_change_sync.conflict_count, 0);
        assert_eq!(event.local_change_sync.last_error, Some(error));
    }

    #[test]
    fn read_only_enforcement_failure_is_typed_separately_from_writeback() {
        let error = CloudFilesModuleError {
            code: CLOUD_FILES_READ_ONLY_ENFORCEMENT_UNAVAILABLE_CODE.to_string(),
            detail: "same Windows principal".to_string(),
        };
        let status =
            CloudFilesLocalChangeSyncStatus::read_only_enforcement_unavailable(error.clone());

        assert_eq!(
            status.mode,
            CloudFilesLocalChangeSyncMode::ReadOnlyEnforcementUnavailable
        );
        assert!(!status.writable_supported);
        assert!(!status.local_mutations_blocked);
        assert_eq!(status.last_error, Some(error));
    }

    #[test]
    fn writable_status_retains_queue_counters_and_typed_blocker() {
        let starting = CloudFilesLocalChangeSyncStatus::writable_starting(3, 2, 1, 5);
        assert_eq!(starting.mode, CloudFilesLocalChangeSyncMode::Starting);
        assert!(starting.writable_supported);
        assert_eq!(starting.pending_operations, 3);
        assert_eq!(starting.retrying_operations, 2);
        assert_eq!(starting.conflict_count, 1);
        assert_eq!(starting.failed_operations, 5);

        let active = CloudFilesLocalChangeSyncStatus::writable_active(1, 0, 4, 0);
        assert_eq!(active.mode, CloudFilesLocalChangeSyncMode::Active);
        assert!(active.writable_supported);

        let error = CloudFilesModuleError {
            code: CLOUD_FILES_WRITABLE_SYNC_CONTRACT_NOT_READY_CODE.to_string(),
            detail: "stable mkdir receipt reconciliation is unavailable".to_string(),
        };
        let blocked = CloudFilesLocalChangeSyncStatus::writable_blocked(1, 2, 4, 3, error.clone());
        assert_eq!(blocked.mode, CloudFilesLocalChangeSyncMode::Blocked);
        assert!(!blocked.writable_supported);
        assert!(blocked.local_mutations_blocked);
        assert_eq!(blocked.failed_operations, 3);
        assert_eq!(blocked.last_error, Some(error));
    }

    #[test]
    fn writable_attention_is_count_and_byte_bounded() {
        let summaries = (0..40)
            .map(|index| CloudFilesSyncAttentionSummary {
                kind: CloudFilesSyncAttentionKind::FailedLocalOperation,
                path: format!("/services/{index}/{}", "x".repeat(4_096)),
                reason_code: "failure".repeat(1_024),
                reason: "failure detail ".repeat(1_024),
                retained_local_evidence: Some(CloudFilesRetainedLocalEvidenceSummary {
                    operation_id: Some(format!("cf_{index}")),
                    evidence_id: Some(format!("receipt-{index}")),
                    staged_content_hash: Some(format!("blake3:{index}")),
                    staged_content_size_bytes: Some(index),
                    local_state: Some("retained".repeat(1_024)),
                }),
                safe_next_action: CloudFilesSyncAttentionNextAction::RepairCauseAndRetry,
            })
            .collect();

        let status = CloudFilesLocalChangeSyncStatus::writable_active(0, 0, 0, 40)
            .with_attention(summaries, 40);
        assert!(status.attention.len() <= CLOUD_FILES_SYNC_ATTENTION_SUMMARY_LIMIT);
        assert_eq!(
            status.attention_overflow,
            40 - status.attention.len() as u64
        );
        assert!(
            status
                .attention
                .iter()
                .map(attention_summary_bytes)
                .sum::<usize>()
                <= CLOUD_FILES_SYNC_ATTENTION_BYTES_LIMIT
        );
        assert!(status.attention.iter().all(|summary| {
            summary.path.len() <= CLOUD_FILES_SYNC_ATTENTION_FIELD_BYTES_LIMIT
                && summary.reason.len() <= CLOUD_FILES_SYNC_ATTENTION_FIELD_BYTES_LIMIT
        }));
    }

    #[test]
    fn writable_heartbeat_carries_liveness_readiness_and_generation() {
        let event = CloudFilesMountLifecycleEvent::connected("C:\\SinomoSIFS", "/services", 8)
            .with_local_change_sync(CloudFilesLocalChangeSyncStatus::writable_active(2, 1, 3, 4))
            .with_worker_health(true, true, 17);
        assert!(event.registered);
        assert!(event.connected);
        assert!(event.worker_alive);
        assert!(event.worker_ready);
        assert_eq!(event.health_generation, 17);
        assert_eq!(event.local_change_sync.failed_operations, 4);
    }

    #[test]
    fn unregister_contract_is_idempotent_and_has_a_terminal_lifecycle_phase() {
        let request = CloudFilesUnregisterRequest::new("C:\\SinomoSIFS");
        assert_eq!(request.root_path, "/");
        assert_eq!(request.provider_name, "Sinomo SIFS");
        assert!(request.data_dir_root.is_none());

        let event = CloudFilesMountLifecycleEvent::unregistered(
            request.sync_root.clone(),
            request.root_path.clone(),
            "already unregistered",
        );
        assert_eq!(event.phase, CloudFilesMountLifecyclePhase::Unregistered);
        assert!(!event.registered);
        assert!(!event.connected);

        let response = CloudFilesUnregisterResponse {
            sync_root: request.sync_root,
            root_path: request.root_path,
            was_registered: false,
            detail: "already unregistered".to_string(),
        };
        assert!(!response.was_registered);
    }

    #[test]
    fn preserved_unregister_is_a_distinct_v3_operation_and_typed_response() {
        assert_eq!(
            SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION,
            "sinomo.dfs.cloud-files.v3"
        );
        assert_ne!(
            SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION,
            SINOMO_DFS_CLOUD_FILES_UNREGISTER_OPERATION
        );
        let request = CloudFilesUnregisterRequest::new("C:\\SinomoSIFS");
        assert_eq!(
            CloudFilesModuleRequest::PreserveStateUnregister(request.clone()),
            CloudFilesModuleRequest::PreserveStateUnregister(request)
        );
    }

    fn cursor_only_recovery_identity() -> CloudFilesCursorOnlyRecoveryIdentity {
        CloudFilesCursorOnlyRecoveryIdentity {
            schema_version: CLOUD_FILES_CURSOR_ONLY_RECOVERY_SCHEMA_VERSION,
            enabled: false,
            sync_root: PathBuf::from(r"C:\SinomoSIFS"),
            root_path: "/services".to_string(),
            data_dir_root: PathBuf::from(r"C:\SinomoData"),
            provider_name: SINOMO_DFS_CLOUD_FILES_PROVIDER_NAME.to_string(),
            provider_version: "0.1.0".to_string(),
            read_only: false,
            allow_other: false,
            sync_local_changes: true,
            prepopulate_depth: 0,
            connect: true,
            max_run_seconds: None,
            disabled_config_fingerprint: "blake3:disabled-config".to_string(),
            provider_provenance_fingerprint: "blake3:provider".to_string(),
        }
    }

    #[test]
    fn cursor_only_recovery_uses_distinct_v3_operations_and_capabilities() {
        assert_eq!(
            SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION,
            "sinomo.dfs.cloud-files.v3"
        );
        let operations = [
            SINOMO_DFS_CLOUD_FILES_MOUNT_OPERATION,
            SINOMO_DFS_CLOUD_FILES_UNREGISTER_OPERATION,
            SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_OPERATION,
            SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_INSPECT_OPERATION,
            SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_APPLY_OPERATION,
        ];
        for (index, operation) in operations.iter().enumerate() {
            assert!(
                operations[..index].iter().all(|other| other != operation),
                "operation names must be pairwise distinct: {operation}"
            );
        }
        assert_ne!(
            SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_INSPECT_CAPABILITY,
            SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_APPLY_CAPABILITY
        );
        assert_ne!(
            SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_INSPECT_CAPABILITY,
            SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_CAPABILITY
        );
        assert_ne!(
            SINOMO_DFS_CLOUD_FILES_CURSOR_ONLY_RECOVERY_APPLY_CAPABILITY,
            SINOMO_DFS_CLOUD_FILES_PRESERVE_STATE_UNREGISTER_CAPABILITY
        );
        assert_eq!(
            CLOUD_FILES_CURSOR_ONLY_RECOVERY_TOKEN_DOMAIN,
            "sinomo.dfs.cloud-files.cursor-only-recovery.v1"
        );
        assert_eq!(CLOUD_FILES_CURSOR_ONLY_RECOVERY_SCHEMA_VERSION, 1);
    }

    #[test]
    fn cursor_only_inspect_apply_and_ready_ack_remain_exact_typed_variants() {
        let identity = cursor_only_recovery_identity();
        let inspect = CloudFilesModuleRequest::InspectCursorOnlyRecovery(
            CloudFilesCursorOnlyRecoveryInspectRequest {
                identity: identity.clone(),
            },
        );
        let apply = CloudFilesModuleRequest::ApplyCursorOnlyRecovery(
            CloudFilesCursorOnlyRecoveryApplyRequest {
                identity: identity.clone(),
                expected_token: "blake3:token".to_string(),
                recovery_id: "cursor-recovery-blake3-id".to_string(),
                acknowledge_authoritative_full_reconciliation: true,
            },
        );
        let CloudFilesModuleRequest::InspectCursorOnlyRecovery(inspect_request) = inspect else {
            panic!("inspect must retain its distinct typed request variant");
        };
        assert_eq!(inspect_request.identity, identity);
        let CloudFilesModuleRequest::ApplyCursorOnlyRecovery(apply_request) = apply else {
            panic!("apply must retain its distinct typed request variant");
        };
        assert_eq!(apply_request.identity, identity);
        assert_eq!(apply_request.expected_token, "blake3:token");
        assert_eq!(apply_request.recovery_id, "cursor-recovery-blake3-id");
        assert!(apply_request.acknowledge_authoritative_full_reconciliation);

        let event = CloudFilesModuleEvent::CursorRecoveryReadyObserved(
            CloudFilesCursorRecoveryReadyObservedRequest {
                schema_version: CLOUD_FILES_CURSOR_ONLY_RECOVERY_SCHEMA_VERSION,
                recovery_id: apply_request.recovery_id,
                recovery_token: apply_request.expected_token,
                sync_root: identity.sync_root.clone(),
                root_path: identity.root_path.clone(),
                sync_root_file_id: 42,
                recovery_cycle: 0,
            },
        );
        let CloudFilesModuleEvent::CursorRecoveryReadyObserved(ready) = event else {
            panic!("ready acknowledgement must not alias an asynchronous lifecycle event");
        };
        assert_eq!(
            ready.schema_version,
            CLOUD_FILES_CURSOR_ONLY_RECOVERY_SCHEMA_VERSION
        );
        assert_eq!(ready.sync_root, identity.sync_root);
        assert_eq!(ready.root_path, identity.root_path);
        assert_eq!(ready.sync_root_file_id, 42);

        let active = CloudFilesModuleEvent::CursorRecoveryActiveObserved(
            CloudFilesCursorRecoveryActiveObservedRequest {
                schema_version: ready.schema_version,
                recovery_id: ready.recovery_id,
                recovery_token: ready.recovery_token,
                sync_root: ready.sync_root.clone(),
                root_path: ready.root_path.clone(),
                sync_root_file_id: ready.sync_root_file_id,
                recovery_cycle: ready.recovery_cycle,
                module_marker_fingerprint: format!("blake3:{}", "1".repeat(64)),
                completed_registration_artifacts_fingerprint: format!("blake3:{}", "2".repeat(64)),
            },
        );
        let CloudFilesModuleEvent::CursorRecoveryActiveObserved(active) = active else {
            panic!("Active acknowledgement must retain its distinct typed event variant");
        };
        assert_eq!(active.sync_root, ready.sync_root);
        assert_eq!(active.root_path, ready.root_path);
        assert_eq!(active.sync_root_file_id, ready.sync_root_file_id);
        assert_eq!(active.recovery_cycle, ready.recovery_cycle);
        assert_ne!(
            active.module_marker_fingerprint,
            active.completed_registration_artifacts_fingerprint
        );
    }
}
